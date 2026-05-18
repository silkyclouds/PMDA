from __future__ import annotations

import threading

from flask import Flask, jsonify

from pmda_api.admin_ops import create_admin_ops_blueprint


class _Runtime:
    STATE_DB_FILE = "state.db"
    CACHE_DB_FILE = "cache.db"
    SETTINGS_DB_FILE = "settings.db"

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.state = {"scanning": False}
        self.calls: list[tuple] = []
        self.restart_return = False
        self.admin_status = 200

    def _require_admin_json(self):
        if self.admin_status != 200:
            return jsonify({"error": "forbidden"}), self.admin_status
        return None

    def _parse_bool(self, value):
        if isinstance(value, bool):
            return value
        return str(value or "").strip().lower() in {"1", "true", "yes", "on"}

    def _maintenance_clear_media_cache(self):
        self.calls.append(("media_cache",))
        return {"ok": True, "cleared": 12}

    def _maintenance_reset_sqlite_db(self, db_path, reinit_fn):
        self.calls.append(("reset_sqlite", db_path))
        reinit_fn()
        return {"ok": True, "path": db_path}

    def _maintenance_clear_files_index(self):
        self.calls.append(("files_index",))
        return {"ok": True}

    def init_state_db(self):
        self.calls.append(("init_state_db",))

    def init_cache_db(self):
        self.calls.append(("init_cache_db",))

    def init_settings_db(self):
        self.calls.append(("init_settings_db",))

    def _files_cache_invalidate_all(self):
        self.calls.append(("invalidate_cache",))

    def _restart_container(self):
        self.calls.append(("restart",))
        return self.restart_return

    def _ops_snapshot_payload(self):
        self.calls.append(("snapshot",))
        return {"status": "ok", "storage": {"config": {"path": "/config"}}}

    def _ops_create_backup_bundle(self, *, include_pg_dump=True):
        self.calls.append(("backup", include_pg_dump))
        return {"status": "ok", "include_pg_dump": bool(include_pg_dump)}


def _client(runtime: _Runtime):
    app = Flask(__name__)
    app.register_blueprint(create_admin_ops_blueprint(runtime=runtime))
    return app.test_client()


def test_admin_gate_blocks_all_admin_ops_routes():
    runtime = _Runtime()
    runtime.admin_status = 403
    client = _client(runtime)

    assert client.get("/api/admin/ops/snapshot").status_code == 403
    assert client.post("/api/admin/ops/backup", json={}).status_code == 403
    assert client.post("/api/admin/maintenance/reset", json={"actions": ["media_cache"]}).status_code == 403


def test_admin_ops_snapshot_and_backup_delegate_to_runtime():
    runtime = _Runtime()
    client = _client(runtime)

    snapshot = client.get("/api/admin/ops/snapshot")
    assert snapshot.status_code == 200
    assert snapshot.get_json()["status"] == "ok"

    backup = client.post("/api/admin/ops/backup", json={"include_pg_dump": False})
    assert backup.status_code == 200
    assert backup.get_json()["include_pg_dump"] is False
    assert ("snapshot",) in runtime.calls
    assert ("backup", False) in runtime.calls


def test_maintenance_reset_validates_actions_and_running_scan():
    runtime = _Runtime()
    client = _client(runtime)

    assert client.post("/api/admin/maintenance/reset", json={}).status_code == 400
    invalid = client.post("/api/admin/maintenance/reset", json={"actions": ["unknown"]})
    assert invalid.status_code == 400
    assert "Invalid action" in invalid.get_json()["message"]

    runtime.state["scanning"] = True
    blocked = client.post("/api/admin/maintenance/reset", json={"actions": ["media_cache"]})
    assert blocked.status_code == 409
    assert blocked.get_json()["status"] == "blocked"


def test_maintenance_reset_runs_requested_actions_without_restart_when_disabled():
    runtime = _Runtime()
    client = _client(runtime)

    response = client.post(
        "/api/admin/maintenance/reset",
        json={
            "actions": ["media_cache", "state_db", "media_cache"],
            "restart": False,
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "ok"
    assert payload["actions"] == ["media_cache", "state_db"]
    assert payload["restart_initiated"] is False
    assert ("media_cache",) in runtime.calls
    assert ("reset_sqlite", "state.db") in runtime.calls
    assert ("init_state_db",) in runtime.calls
    assert ("invalidate_cache",) in runtime.calls
    assert ("restart",) not in runtime.calls


def test_maintenance_reset_requires_confirmations_for_destructive_actions():
    runtime = _Runtime()
    client = _client(runtime)

    settings = client.post("/api/admin/maintenance/reset", json={"actions": ["settings_db"], "restart": False})
    assert settings.status_code == 400
    assert "confirm_settings_db" in settings.get_json()["message"]

    files = client.post("/api/admin/maintenance/reset", json={"actions": ["files_index"], "restart": False})
    assert files.status_code == 400
    assert "confirm_files_index" in files.get_json()["message"]
