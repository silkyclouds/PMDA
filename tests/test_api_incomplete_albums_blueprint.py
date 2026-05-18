from __future__ import annotations

import sqlite3
import threading

from flask import Flask

from pmda_api.incomplete_albums import create_incomplete_albums_blueprint


class _Runtime:
    def __init__(self, db_path):
        self.STATE_DB_FILE = db_path
        self.lock = threading.RLock()
        self.state = {"incomplete_scan": None}

    def _run_incomplete_albums_scan(self):
        with self.lock:
            self.state["incomplete_scan"] = {
                "running": False,
                "run_id": None,
                "progress": 0,
                "total": 0,
                "current_artist": "",
                "current_album": "",
                "count": 0,
                "error": "Manual incomplete scan is disabled; PMDA records incompletes during the scan pipeline.",
            }


def _make_app(db_path):
    app = Flask(__name__)
    app.register_blueprint(create_incomplete_albums_blueprint(runtime=_Runtime(db_path)))
    return app


def _init_db(db_path):
    con = sqlite3.connect(db_path)
    con.execute(
        """
        CREATE TABLE incomplete_album_diagnostics(
            run_id INTEGER,
            artist TEXT,
            album_id INTEGER,
            title_raw TEXT,
            folder TEXT,
            classification TEXT,
            missing_in_plex TEXT,
            missing_on_disk TEXT,
            expected_track_count INTEGER,
            actual_track_count INTEGER,
            detected_at REAL
        )
        """
    )
    con.execute(
        """
        INSERT INTO incomplete_album_diagnostics
        (run_id, artist, album_id, title_raw, folder, classification, missing_in_plex,
         missing_on_disk, expected_track_count, actual_track_count, detected_at)
        VALUES(1, 'Artist', 42, 'Album', '/music/a', 'confirmed_incomplete',
               '[1]', '[2]', 10, 8, 123.0)
        """
    )
    con.commit()
    con.close()


def test_incomplete_scan_start_is_disabled_and_progress_records_reason(tmp_path):
    db_path = tmp_path / "state.db"
    _init_db(db_path)
    client = _make_app(db_path).test_client()

    start = client.post("/api/incomplete-albums/scan")
    assert start.status_code == 400
    assert start.get_json()["started"] is False

    progress = client.get("/api/incomplete-albums/scan/progress")
    assert progress.status_code == 200
    payload = progress.get_json()
    assert payload["running"] is False
    assert "disabled" in payload["error"]


def test_incomplete_results_and_export_use_pipeline_diagnostics(tmp_path):
    db_path = tmp_path / "state.db"
    _init_db(db_path)
    client = _make_app(db_path).test_client()

    results = client.get("/api/incomplete-albums/results")
    assert results.status_code == 200
    item = results.get_json()["items"][0]
    assert item["artist"] == "Artist"
    assert item["missing_from_index"] == [1]
    assert item["missing_on_disk"] == [2]

    exported = client.get("/api/incomplete-albums/export/1")
    assert exported.status_code == 200
    assert exported.get_json()["items"][0]["album_id"] == 42
