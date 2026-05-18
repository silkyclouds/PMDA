from __future__ import annotations

import threading

from flask import Flask

from pmda_api.profile_backfill import create_profile_backfill_blueprint


class _Runtime:
    _files_profile_backfill_lock = threading.RLock()
    _files_profile_backfill_state = {"running": True, "pending_artist_profiles": 1}
    _files_profile_backfill_idle_state = {"enabled": True}

    def __init__(self):
        self.started = None

    @staticmethod
    def _files_profile_backfill_pending_work():
        return {
            "pending_artist_profiles": 2,
            "pending_album_profiles": 3,
            "eligible_album_profiles": 4,
            "pending_album_covers": 5,
        }

    @staticmethod
    def _get_library_mode():
        return "files"

    @staticmethod
    def _parse_bool(value):
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def _trigger_files_profile_backfill_async(self, *, reason, cover_only):
        self.started = {"reason": reason, "cover_only": cover_only}
        return True


def test_profile_backfill_status_merges_pending_counts():
    app = Flask(__name__)
    app.register_blueprint(create_profile_backfill_blueprint(runtime=_Runtime()))
    client = app.test_client()

    res = client.get("/api/library/files-profile-backfill/status")
    assert res.status_code == 200
    payload = res.get_json()
    assert payload["running"] is True
    assert payload["pending_artist_profiles"] == 2
    assert payload["pending_album_profiles"] == 3
    assert payload["eligible_album_profiles"] == 4
    assert payload["pending_album_covers"] == 5
    assert payload["idle_autostart"] == {"enabled": True}


def test_profile_backfill_start_and_stop_routes():
    runtime = _Runtime()
    app = Flask(__name__)
    app.register_blueprint(create_profile_backfill_blueprint(runtime=runtime))
    client = app.test_client()

    start = client.post("/api/library/files-profile-backfill/start", json={"reason": "test", "cover_only": True})
    assert start.status_code == 200
    assert start.get_json() == {"status": "started"}
    assert runtime.started == {"reason": "test", "cover_only": True}

    stop = client.post("/api/library/files-profile-backfill/stop")
    assert stop.status_code == 200
    assert stop.get_json() == {"status": "stopping"}
    assert runtime._files_profile_backfill_state["running"] is False
