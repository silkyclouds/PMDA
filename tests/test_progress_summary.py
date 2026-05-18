import json
import sqlite3

from pmda_scan.progress_summary import load_last_completed_scan_summary


def test_load_last_completed_scan_summary_normalizes_chart_fields():
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE scan_history (
            status TEXT,
            end_time TEXT,
            summary_json TEXT,
            ai_used_count INTEGER,
            ai_tokens_total INTEGER,
            ai_cost_usd_total REAL,
            ai_unpriced_calls INTEGER
        )
        """
    )
    con.execute(
        """
        INSERT INTO scan_history VALUES (
            'completed',
            '2026-05-16T09:00:00Z',
            ?,
            3,
            1200,
            0.42,
            1
        )
        """,
        (
            json.dumps(
                {
                    "albums_scanned": 10,
                    "albums_with_mb_id": 4,
                    "albums_without_mb_id": 6,
                    "scan_discogs_matched": 2,
                }
            ),
        ),
    )

    def connect_readonly(timeout=2):
        assert timeout == 2
        return con

    payload = load_last_completed_scan_summary(
        connect_readonly=connect_readonly,
        last_fix_all_total_albums=8,
        last_fix_all_by_provider={
            "lastfm": {"identified": 5},
            "bandcamp": {"identified": 6},
        },
    )

    assert payload["ai_used_count"] == 3
    assert payload["ai_tokens_total"] == 1200
    assert payload["ai_cost_usd_total"] == 0.42
    assert payload["ai_unpriced_calls"] == 1
    summary = payload["summary"]
    assert summary["mb_match"] == {"matched": 4, "total": 10}
    assert summary["discogs_match"] == {"matched": 2, "total": 10}
    assert summary["lastfm_match"] == {"matched": 5, "total": 8}
    assert summary["bandcamp_match"] == {"matched": 6, "total": 8}


def test_load_last_completed_scan_summary_handles_no_rows():
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE scan_history (
            status TEXT,
            end_time TEXT,
            summary_json TEXT,
            ai_used_count INTEGER,
            ai_tokens_total INTEGER,
            ai_cost_usd_total REAL,
            ai_unpriced_calls INTEGER
        )
        """
    )

    payload = load_last_completed_scan_summary(connect_readonly=lambda timeout=2: con)

    assert payload == {
        "summary": None,
        "ai_used_count": 0,
        "ai_tokens_total": 0,
        "ai_cost_usd_total": 0.0,
        "ai_unpriced_calls": 0,
    }
