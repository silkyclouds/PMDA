from __future__ import annotations

from flask import Flask

from pmda_api.library_index_control import create_library_index_control_blueprint


class _Runtime:
    def __init__(self, *, started=True):
        self.started = started
        self.reason = None

    @staticmethod
    def _get_library_mode():
        return "files"

    def _trigger_files_index_rebuild_async(self, *, reason):
        self.reason = reason
        return self.started

    @staticmethod
    def _files_index_get_state():
        return {"running": True, "phase": "scan"}


def test_library_index_rebuild_starts_with_filesystem_reason():
    runtime = _Runtime(started=True)
    app = Flask(__name__)
    app.register_blueprint(create_library_index_control_blueprint(runtime=runtime))
    client = app.test_client()

    res = client.post("/api/library/files-index/rebuild", json={"source": "filesystem"})
    assert res.status_code == 200
    assert res.get_json() == {"status": "started"}
    assert runtime.reason == "api_rebuild_filesystem"


def test_library_index_rebuild_reports_already_running():
    app = Flask(__name__)
    app.register_blueprint(create_library_index_control_blueprint(runtime=_Runtime(started=False)))
    client = app.test_client()

    res = client.post("/api/library/files-index/rebuild", json={})
    assert res.status_code == 409
    assert res.get_json() == {"status": "already_running", "progress": {"running": True, "phase": "scan"}}
