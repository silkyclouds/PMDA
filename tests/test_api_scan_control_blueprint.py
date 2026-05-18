from __future__ import annotations

from types import SimpleNamespace
import sqlite3
import tempfile
import threading
import time
from pathlib import Path

from flask import Flask

from pmda_api.scan_control import create_scan_control_blueprint


class FakeScanRuntime(SimpleNamespace):
    def __init__(self, root: Path):
        super().__init__()
        self.time = time
        self.lock = threading.RLock()
        self.scan_should_stop = threading.Event()
        self.scan_is_paused = threading.Event()
        self.state = {
            "scanning": False,
            "scan_starting": False,
            "scan_finalizing": False,
            "scan_type": "full",
            "scan_resume_run_id": "resume-1",
            "scan_resume_requested_run_id": None,
            "scan_discovery_running": False,
            "run_improve_after": False,
            "duplicates": {"x": 1},
            "scan_active_artists": {"artist": {}},
            "files_watcher": {"dirty_count": 3, "dirty_count_by_root": {"a": 3}},
        }
        self.STATE_DB_FILE = root / "state.db"
        self.CACHE_DB_FILE = root / "cache.db"
        self.AI_PROVIDER = "ollama"
        self.RESOLVED_MODEL = "qwen3:4b"
        self.AI_FUNCTIONAL_ERROR_MSG = ""
        self.OPENAI_API_KEY = ""
        self.openai_client = None
        self.USE_MUSICBRAINZ = True
        self.USE_BANDCAMP = True
        self.ai_provider_ready = False
        self.started = []
        self.resume_status = []
        self.live_index_reset = False
        self.bootstrap_reset = False
        self._init_databases()

    def _init_databases(self) -> None:
        con = sqlite3.connect(str(self.STATE_DB_FILE))
        con.executescript(
            """
            CREATE TABLE duplicates_loser(id INTEGER PRIMARY KEY);
            CREATE TABLE duplicates_best(id INTEGER PRIMARY KEY);
            CREATE TABLE broken_albums(id INTEGER PRIMARY KEY);
            CREATE TABLE scan_editions(id INTEGER PRIMARY KEY);
            CREATE TABLE files_library_published_albums(id INTEGER PRIMARY KEY);
            CREATE TABLE files_pending_changes(id INTEGER PRIMARY KEY);
            CREATE TABLE settings(key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE scan_history(scan_id INTEGER PRIMARY KEY, status TEXT, end_time REAL, summary_json TEXT);
            INSERT INTO duplicates_loser DEFAULT VALUES;
            INSERT INTO duplicates_best DEFAULT VALUES;
            INSERT INTO broken_albums DEFAULT VALUES;
            INSERT INTO scan_editions DEFAULT VALUES;
            INSERT INTO files_library_published_albums DEFAULT VALUES;
            INSERT INTO files_pending_changes DEFAULT VALUES;
            INSERT INTO settings(key, value) VALUES('last_completed_scan_id', '1');
            INSERT INTO settings(key, value) VALUES('last_completed_full_scan_id', '1');
            INSERT INTO scan_history(scan_id, status, end_time, summary_json) VALUES(1, 'completed', 1, '{}');
            """
        )
        con.commit()
        con.close()
        cache = sqlite3.connect(str(self.CACHE_DB_FILE))
        cache.executescript(
            """
            CREATE TABLE audio_cache(id INTEGER PRIMARY KEY);
            CREATE TABLE musicbrainz_cache(id INTEGER PRIMARY KEY);
            CREATE TABLE musicbrainz_album_lookup(id INTEGER PRIMARY KEY);
            CREATE TABLE provider_album_lookup(id INTEGER PRIMARY KEY);
            INSERT INTO audio_cache DEFAULT VALUES;
            INSERT INTO musicbrainz_cache DEFAULT VALUES;
            INSERT INTO musicbrainz_album_lookup DEFAULT VALUES;
            INSERT INTO provider_album_lookup DEFAULT VALUES;
            """
        )
        cache.commit()
        cache.close()

    def _requires_config(self):
        return None

    def get_default_scan_type(self) -> str:
        return "full"

    def _pipeline_bootstrap_status(self) -> dict:
        return {"bootstrap_required": False, "first_full_scan_id": 1, "first_full_completed_at": 123.0}

    def _scan_autonomous_mode_effective(self) -> bool:
        return True

    def _run_preflight_checks(self):
        return True, False

    def _resolve_ai_runtime_availability(self, **_kwargs):
        return True, "ollama", "local", ""

    def _current_user_id_or_zero(self) -> int:
        return 0

    def _run_provider_preflights_parallel(self) -> dict:
        return {
            "discogs": (True, "ok"),
            "lastfm": (True, "ok"),
            "fanart": (False, "disabled"),
            "audiodb": (False, "disabled"),
            "serper": (False, "disabled"),
            "acoustid": (False, "disabled"),
        }

    def _paths_rw_status(self) -> dict:
        return {"ok": True}

    def _run_incomplete_albums_scan(self) -> None:
        self.incomplete_scan_called = True

    def _resolve_provider_for_runtime(self, *_args, **_kwargs) -> str:
        return "ollama"

    def _openai_codex_oauth_mode_enabled(self) -> bool:
        return False

    def _openai_codex_profile_present(self, _uid: int) -> bool:
        return False

    def _openai_codex_any_profile_present(self) -> bool:
        return False

    def _try_begin_scan(self, **kwargs):
        self.started.append(kwargs)
        return True, {}

    def _get_library_mode(self) -> str:
        return "files"

    def _copy_scan_discovery_runtime(self, _run_id):
        return {"updated_at": 1.0}

    def _wait_for_discovery_runtime_update(self, *_args, **_kwargs):
        return True

    def _snapshot_current_resume_state(self, reason: str):
        return {"ok": True, "rows": 7, "snapshot_kind": reason}

    def _set_resume_run_status(self, run_id, status: str) -> None:
        self.resume_status.append((run_id, status))

    def _reload_ai_config_and_reinit(self) -> None:
        self.reload_called = True

    def _ai_model_display_name(self, _provider) -> str:
        return "qwen3:4b"

    def _get_resume_run_snapshot(self, _mode: str, scan_type: str):
        return {"available": True, "run_id": "resume-1", "scan_id": 10} if scan_type == "full" else {}

    def _get_latest_resume_run_snapshot_any_signature(self, _mode: str, _scan_type: str):
        return {}

    def _reconcile_scan_move_trace_backlog(self, **_kwargs) -> int:
        return 0

    def _reset_files_live_index_for_scan(self, *, force: bool) -> None:
        self.live_index_reset = force

    def _pipeline_bootstrap_reset(self) -> None:
        self.bootstrap_reset = True


def _client(runtime: FakeScanRuntime):
    app = Flask(__name__)
    app.register_blueprint(create_scan_control_blueprint(runtime=runtime))
    return app.test_client()


def test_scan_defaults_and_preflight_use_runtime_services():
    with tempfile.TemporaryDirectory(prefix="pmda-scan-control-") as tmp:
        runtime = FakeScanRuntime(Path(tmp))
        client = _client(runtime)

        defaults = client.get("/api/scan/defaults")
        assert defaults.status_code == 200
        assert defaults.get_json()["default_scan_type"] == "full"

        preflight = client.get("/api/scan/preflight")
        assert preflight.status_code == 200
        payload = preflight.get_json()
        assert payload["musicbrainz"]["ok"] is True
        assert payload["ai"]["provider"] == "ollama"
        assert payload["bandcamp"]["ok"] is True


def test_start_pause_resume_stop_routes_preserve_scan_control_semantics():
    with tempfile.TemporaryDirectory(prefix="pmda-scan-control-") as tmp:
        runtime = FakeScanRuntime(Path(tmp))
        client = _client(runtime)

        start = client.post("/scan/start", json={"scan_type": "full"})
        assert start.status_code == 200
        assert runtime.started[-1]["source"] == "interactive"

        pause = client.post("/scan/pause")
        assert pause.status_code == 200
        assert runtime.scan_is_paused.is_set()
        assert runtime.resume_status[-1] == ("resume-1", "paused")

        resume = client.post("/scan/resume", json={})
        assert resume.status_code == 200
        assert not runtime.scan_is_paused.is_set()
        assert runtime.resume_status[-1] == ("resume-1", "running")

        stop = client.post("/scan/stop")
        assert stop.status_code == 200
        assert runtime.scan_should_stop.is_set()
        assert runtime.resume_status[-1] == ("resume-1", "stopped")


def test_clear_scan_clears_state_and_optional_caches():
    with tempfile.TemporaryDirectory(prefix="pmda-scan-control-") as tmp:
        runtime = FakeScanRuntime(Path(tmp))
        client = _client(runtime)

        resp = client.post("/api/scan/clear", json={"clear_audio_cache": True, "clear_mb_cache": True})
        assert resp.status_code == 200
        payload = resp.get_json()
        assert payload["cleared"]["duplicates_loser"] == 1
        assert payload["cleared"]["audio_cache"] == 1
        assert payload["cleared"]["musicbrainz_album_lookup"] == 1
        assert runtime.state["duplicates"] == {}
        assert runtime.state["files_watcher"]["dirty_count"] == 0
        assert runtime.live_index_reset is True
        assert runtime.bootstrap_reset is True
