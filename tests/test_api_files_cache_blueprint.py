from __future__ import annotations

import threading

from flask import Flask

from pmda_api.files_cache import create_files_cache_blueprint


class _Runtime:
    def __init__(self):
        self.lock = threading.RLock()
        self.state = {
            "scan_prescan_cache_snapshot_running": False,
            "scan_prescan_cache_snapshot_rows": 12,
            "scan_prescan_cache_snapshot_updated_at": 123.0,
        }

    @staticmethod
    def _files_cache_quality_recalc_status_unlocked():
        return {"running": False, "processed": 0}

    @staticmethod
    def _get_library_mode():
        return "files"

    @staticmethod
    def _parse_bool(value):
        return str(value).lower() in {"1", "true", "yes"}

    @staticmethod
    def _parse_int_loose(value, default=0):
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _trigger_prescan_cache_snapshot_async(*, reason):
        return True

    @staticmethod
    def _start_files_cache_quality_recalc_async(**kwargs):
        return True, {"accepted": kwargs}


def test_files_cache_blueprint_reports_status_and_starts_full_pass():
    runtime = _Runtime()
    app = Flask(__name__)
    app.register_blueprint(create_files_cache_blueprint(runtime=runtime))
    client = app.test_client()

    status = client.get("/api/files/cache/quality-recalc")
    assert status.status_code == 200
    payload = status.get_json()
    assert payload["prescan_snapshot_rows"] == 12
    assert payload["running"] is False

    start = client.post(
        "/api/files/cache/quality-recalc",
        json={"force_full_pass": True, "batch_size": 250, "source_id": 7, "limit": 9},
    )
    assert start.status_code == 200
    start_payload = start.get_json()
    assert start_payload["status"] == "started"
    assert start_payload["mode"] == "full_pass"
    assert start_payload["accepted"]["batch_size"] == 250
