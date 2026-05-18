from __future__ import annotations

import sqlite3

from flask import Flask

from pmda_api.scan_history import create_scan_history_blueprint


class _Runtime:
    def __init__(self, db_path):
        self.STATE_DB_FILE = db_path

    @staticmethod
    def _parse_int_loose(value, default=0):
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _scan_history_summary_with_metadata_rollup(cur, scan_id, parsed_summary):
        return parsed_summary or {}


def _init_db(path):
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE scan_history(
            scan_id INTEGER, start_time REAL, end_time REAL, duration_seconds REAL,
            albums_scanned INTEGER, duplicates_found INTEGER, artists_processed INTEGER,
            artists_total INTEGER, ai_used_count INTEGER, mb_used_count INTEGER,
            ai_enabled INTEGER, mb_enabled INTEGER, auto_move_enabled INTEGER,
            space_saved_mb REAL, albums_moved INTEGER, status TEXT,
            duplicate_groups_count INTEGER, total_duplicates_count INTEGER,
            broken_albums_count INTEGER, missing_albums_count INTEGER,
            albums_without_artist_image INTEGER, albums_without_album_image INTEGER,
            albums_without_complete_tags INTEGER, albums_without_mb_id INTEGER,
            albums_without_artist_mb_id INTEGER, ai_tokens_total INTEGER,
            ai_cost_usd_total REAL, ai_unpriced_calls INTEGER,
            ai_lifecycle_complete INTEGER, scan_type TEXT, entry_type TEXT,
            summary_json TEXT
        )
        """
    )
    cur.execute("CREATE TABLE ai_scan_cost_rollups(id INTEGER)")
    cur.execute("CREATE TABLE ai_call_usage(id INTEGER)")
    cur.execute("CREATE TABLE scan_pipeline_trace(id INTEGER)")
    cur.execute("CREATE TABLE scan_editions(id INTEGER)")
    cur.execute(
        """
        INSERT INTO scan_history VALUES (
            7, 1, 2, 1, 12, 3, 4, 5, 0, 9, 1, 1, 0, 0, 0, 'completed',
            2, 3, 1, 0, 6, 7, 8, 9, 10, 11, 0.12, 1, 1, 'full', 'scan',
            '{"strict_total_albums": 12}'
        )
        """
    )
    con.commit()
    con.close()


def test_scan_history_blueprint_lists_and_clears_history(tmp_path):
    db_path = tmp_path / "state.db"
    _init_db(db_path)
    app = Flask(__name__)
    app.register_blueprint(create_scan_history_blueprint(runtime=_Runtime(db_path)))
    client = app.test_client()

    response = client.get("/api/scan-history")
    assert response.status_code == 200
    rows = response.get_json()
    assert rows[0]["scan_id"] == 7
    assert rows[0]["summary_json"]["strict_total_albums"] == 12

    cleared = client.delete("/api/scan-history")
    assert cleared.status_code == 200
    assert cleared.get_json()["status"] == "ok"
