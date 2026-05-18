from __future__ import annotations

import threading

from flask import Flask

from pmda_api.publication_reconcile import create_publication_reconcile_blueprint


class _Runtime:
    lock = threading.RLock()
    state = {"files_publication_reconcile": {"status": "running", "done": 2}}

    def __init__(self):
        self.async_call = None
        self.sync_call = None

    @staticmethod
    def _get_library_mode():
        return "files"

    @staticmethod
    def _parse_int_loose(value, default=0):
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _parse_bool(value):
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def _reconcile_files_publication_from_scan_editions(self, **kwargs):
        self.sync_call = kwargs
        return {"status": "ok", "scan_ids": kwargs.get("scan_ids")}

    def _trigger_files_publication_reconcile_async(self, **kwargs):
        self.async_call = kwargs
        return True


def test_publication_reconcile_starts_async_job_with_sanitized_scan_ids():
    runtime = _Runtime()
    app = Flask(__name__)
    app.register_blueprint(create_publication_reconcile_blueprint(runtime=runtime))
    client = app.test_client()

    res = client.post(
        "/api/library/files-publication/reconcile",
        json={"scan_ids": [1, "bad", 3], "reason": "test", "rebuild_index": False},
    )
    assert res.status_code == 200
    assert res.get_json() == {"status": "started"}
    assert runtime.async_call == {"scan_ids": [1, 3], "reason": "test", "rebuild_index": False}


def test_publication_reconcile_sync_and_status_routes():
    runtime = _Runtime()
    app = Flask(__name__)
    app.register_blueprint(create_publication_reconcile_blueprint(runtime=runtime))
    client = app.test_client()

    sync = client.post("/api/library/files-publication/reconcile", json={"scan_ids": "9", "sync": True})
    assert sync.status_code == 200
    assert sync.get_json() == {"status": "ok", "scan_ids": [9]}
    assert runtime.sync_call == {"scan_ids": [9], "reason": "api_manual", "rebuild_index": True}

    status = client.get("/api/library/files-publication/reconcile/status")
    assert status.status_code == 200
    assert status.get_json() == {"status": "running", "done": 2}
