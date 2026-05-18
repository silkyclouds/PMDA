from __future__ import annotations

import sqlite3

from flask import Flask

from pmda_api.scan_moves import create_scan_moves_blueprint


class _Runtime:
    __file__ = "pmda.py"

    def __init__(self, db_path):
        self.STATE_DB_FILE = db_path

    @staticmethod
    def _parse_int_loose(value, default=0):
        try:
            return int(value)
        except Exception:
            return default


def test_scan_moves_audit_returns_404_when_no_scan_history(tmp_path):
    db_path = tmp_path / "state.db"
    con = sqlite3.connect(db_path)
    con.execute("CREATE TABLE scan_history(scan_id INTEGER)")
    con.commit()
    con.close()

    app = Flask(__name__)
    app.register_blueprint(create_scan_moves_blueprint(runtime=_Runtime(db_path)))
    response = app.test_client().get("/api/statistics/scan-moves-audit")

    assert response.status_code == 404
    assert response.get_json()["error"] == "No scan history found"
