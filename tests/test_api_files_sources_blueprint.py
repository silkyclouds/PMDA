from __future__ import annotations

from types import SimpleNamespace
import sqlite3
import tempfile
from pathlib import Path

from flask import Flask

from pmda_api.files_sources import create_files_sources_blueprint


class FakeFilesSourcesRuntime(SimpleNamespace):
    def __init__(self, root: Path):
        super().__init__()
        self.SETTINGS_DB_FILE = root / "settings.db"
        self.STATE_DB_FILE = root / "state.db"
        self.LIBRARY_WINNER_PLACEMENT_STRATEGY = "move"
        self.applied_settings = None
        self.reconcile_requests = []
        self.started_scans = []
        self._roots = [
            {"source_id": 1, "role": "incoming", "path": "/music/Music_dump", "enabled": True, "is_winner_root": False},
            {"source_id": 2, "role": "trusted", "path": "/music/Music_matched", "enabled": True, "is_winner_root": True},
        ]
        self._init_databases()

    def _init_databases(self) -> None:
        settings = sqlite3.connect(str(self.SETTINGS_DB_FILE))
        settings.execute("CREATE TABLE settings(key TEXT PRIMARY KEY, value TEXT)")
        settings.commit()
        settings.close()
        state = sqlite3.connect(str(self.STATE_DB_FILE))
        state.executescript(
            """
            CREATE TABLE files_pending_changes(source_id INTEGER, first_seen REAL, last_seen REAL);
            INSERT INTO files_pending_changes(source_id, first_seen, last_seen) VALUES(1, 10, 15);
            INSERT INTO files_pending_changes(source_id, first_seen, last_seen) VALUES(1, 20, 25);
            INSERT INTO files_pending_changes(source_id, first_seen, last_seen) VALUES(2, 30, 35);
            """
        )
        state.commit()
        state.close()

    def _effective_files_source_rows(self, *, enabled_only: bool):
        return [r for r in self._roots if (not enabled_only or r.get("enabled"))]

    def _winner_source_row(self):
        return next((r for r in self._roots if r.get("is_winner_root")), None)

    def _get_config_from_db(self, _key: str, default):
        return default

    def _files_source_roots_replace(self, roots_payload, *, winner_source_root_id):
        self._roots = [
            {
                "source_id": int(row.get("source_id") or idx + 1),
                "role": str(row.get("role") or "incoming"),
                "path": str(row.get("path") or ""),
                "enabled": bool(row.get("enabled", True)),
                "is_winner_root": int(row.get("source_id") or idx + 1) == int(winner_source_root_id or 0),
            }
            for idx, row in enumerate(roots_payload)
        ]
        return self._roots

    def init_settings_db(self) -> None:
        return None

    def _apply_settings_in_memory(self, settings: dict) -> None:
        self.applied_settings = settings

    def _files_source_roots_fetch(self, *, enabled_only: bool):
        return [r for r in self._roots if (not enabled_only or r.get("enabled"))]

    def _pipeline_bootstrap_status(self) -> dict:
        return {"bootstrap_required": False}

    def _try_begin_scan(self, **kwargs):
        self.started_scans.append(kwargs)
        return True, {}

    def _files_watcher_status_snapshot(self) -> dict:
        return {
            "running": True,
            "enabled": True,
            "available": True,
            "degraded_mode": False,
            "dirty_count": 2,
            "dirty_count_by_root": {"/music/Music_dump": 2},
            "last_restart_duration_ms": "12",
            "roots": ["/music/Music_dump"],
        }

    def _int_or_none(self, value):
        try:
            return int(value)
        except Exception:
            return None

    def _request_files_watcher_reconcile(self, reason: str, *, force: bool) -> None:
        self.reconcile_requests.append((reason, force))


def _client(runtime: FakeFilesSourcesRuntime):
    app = Flask(__name__)
    app.register_blueprint(create_files_sources_blueprint(runtime=runtime))
    return app.test_client()


def test_files_sources_get_put_and_persist_winner_settings():
    with tempfile.TemporaryDirectory(prefix="pmda-files-sources-") as tmp:
        runtime = FakeFilesSourcesRuntime(Path(tmp))
        client = _client(runtime)

        get_resp = client.get("/api/files/sources")
        assert get_resp.status_code == 200
        assert get_resp.get_json()["winner_source_root_id"] == 2

        put_resp = client.put(
            "/api/files/sources",
            json={
                "winner_source_root_id": 10,
                "winner_placement_strategy": "copy",
                "roots": [
                    {"source_id": 9, "role": "incoming", "path": "/music/incoming", "enabled": True},
                    {"source_id": 10, "role": "trusted", "path": "/music/Music_matched", "enabled": True},
                ],
            },
        )
        assert put_resp.status_code == 200
        payload = put_resp.get_json()
        assert payload["winner_source_root_id"] == 10
        assert payload["winner_placement_strategy"] == "copy"
        assert runtime.applied_settings["LIBRARY_WORKFLOW_MODE"] == "custom"

        con = sqlite3.connect(str(runtime.SETTINGS_DB_FILE))
        rows = dict(con.execute("SELECT key, value FROM settings").fetchall())
        con.close()
        assert rows["LIBRARY_WINNER_PLACEMENT_STRATEGY"] == "copy"
        assert rows["WINNER_SOURCE_ROOT_ID"] == "10"


def test_incoming_status_rescan_and_watcher_routes():
    with tempfile.TemporaryDirectory(prefix="pmda-files-sources-") as tmp:
        runtime = FakeFilesSourcesRuntime(Path(tmp))
        client = _client(runtime)

        status = client.get("/api/incoming/status")
        assert status.status_code == 200
        payload = status.get_json()
        assert payload["pending_folders"] == 2
        assert payload["pending_by_source"] == {"1": 2}
        assert payload["incoming_source_ids"] == [1]

        rescan = client.post("/api/incoming/rescan")
        assert rescan.status_code == 200
        assert runtime.started_scans[-1]["scan_type"] == "changed_only"
        assert runtime.started_scans[-1]["source"] == "incoming_manual"

        watcher = client.get("/api/files/watcher/status")
        assert watcher.status_code == 200
        assert watcher.get_json()["last_restart_duration_ms"] == 12

        restart = client.post("/api/files/watcher/restart")
        assert restart.status_code == 200
        assert runtime.reconcile_requests == [("manual_api_restart", True)]
