import sqlite3

from pmda_scan.progress_ai import effective_ai_usage, load_current_scan_ai_rollup


def test_load_current_scan_ai_rollup_reads_running_scan():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """
        CREATE TABLE ai_scan_cost_rollups (
            scan_id INTEGER,
            calls_total INTEGER,
            total_tokens INTEGER,
            cost_total_microusd INTEGER,
            unpriced_calls INTEGER,
            lifecycle_complete INTEGER,
            updated_at REAL
        )
        """
    )
    con.execute(
        "INSERT INTO ai_scan_cost_rollups VALUES (7, 3, 1200, 420000, 1, 0, 123.0)"
    )

    row = load_current_scan_ai_rollup(
        scanning=True,
        scan_id=7,
        connect_readonly=lambda timeout=2: con,
    )

    assert row["calls_total"] == 3
    assert row["total_tokens"] == 1200
    assert row["cost_total_microusd"] == 420000


def test_effective_ai_usage_prefers_current_rollup_when_running():
    usage = effective_ai_usage(
        scanning=True,
        current_scan_ai_rollup={
            "calls_total": 5,
            "total_tokens": 2000,
            "cost_total_microusd": 500000,
            "unpriced_calls": 2,
        },
        scan_ai_guard_calls_used=2,
        scan_ai_used_count=1,
        last_scan_summary=None,
        last_scan_ai_used_count=0,
        last_scan_ai_tokens_total=0,
        last_scan_ai_cost_usd_total=0.0,
        last_scan_ai_unpriced_calls=0,
        microusd_to_usd=lambda value: value / 1_000_000.0,
    )

    assert usage == {
        "used_count": 5,
        "tokens_total": 2000,
        "cost_usd_total": 0.5,
        "unpriced_calls": 2,
    }


def test_effective_ai_usage_uses_last_summary_when_idle():
    usage = effective_ai_usage(
        scanning=False,
        current_scan_ai_rollup=None,
        scan_ai_guard_calls_used=0,
        scan_ai_used_count=0,
        last_scan_summary={
            "ai_calls_total": 8,
            "ai_tokens_total": 3000,
            "ai_cost_usd_total": 0.75,
            "ai_unpriced_calls": 4,
        },
        last_scan_ai_used_count=3,
        last_scan_ai_tokens_total=1000,
        last_scan_ai_cost_usd_total=0.25,
        last_scan_ai_unpriced_calls=1,
        microusd_to_usd=lambda value: value / 1_000_000.0,
    )

    assert usage == {
        "used_count": 8,
        "tokens_total": 3000,
        "cost_usd_total": 0.75,
        "unpriced_calls": 4,
    }
