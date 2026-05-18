from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from typing import Any


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return default


def _provider_identified(summary: Mapping[str, Any] | None, provider: str) -> int:
    bucket = (summary or {}).get(provider, {})
    if not isinstance(bucket, Mapping):
        return 0
    return _safe_int(bucket.get("identified"))


def load_last_completed_scan_summary(
    *,
    connect_readonly: Callable[..., Any],
    last_fix_all_total_albums: int = 0,
    last_fix_all_by_provider: Mapping[str, Any] | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Load the latest completed scan summary for the progress endpoint.

    The progress route should stay mostly as a transport layer. This helper owns
    the SQLite read and chart-friendly normalization for the last completed run.
    """

    result: dict[str, Any] = {
        "summary": None,
        "ai_used_count": 0,
        "ai_tokens_total": 0,
        "ai_cost_usd_total": 0.0,
        "ai_unpriced_calls": 0,
    }
    try:
        con = connect_readonly(timeout=2)
        try:
            cur = con.cursor()
            cur.execute(
                """
                SELECT
                    summary_json,
                    COALESCE(ai_used_count, 0),
                    COALESCE(ai_tokens_total, 0),
                    COALESCE(ai_cost_usd_total, 0.0),
                    COALESCE(ai_unpriced_calls, 0)
                FROM scan_history
                WHERE status = 'completed' AND end_time IS NOT NULL
                ORDER BY end_time DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
        finally:
            con.close()

        if not row:
            return result

        result["ai_used_count"] = _safe_int(row[1])
        result["ai_tokens_total"] = _safe_int(row[2])
        result["ai_cost_usd_total"] = _safe_float(row[3])
        result["ai_unpriced_calls"] = _safe_int(row[4])

        if not row[0]:
            return result

        summary = json.loads(row[0])
        if not isinstance(summary, dict):
            return result

        albums_with_mb = _safe_int(summary.get("albums_with_mb_id"))
        albums_without_mb = _safe_int(summary.get("albums_without_mb_id"))
        total_mb = albums_with_mb + albums_without_mb
        summary["mb_match"] = (
            {"matched": albums_with_mb, "total": total_mb}
            if total_mb
            else {"matched": 0, "total": 0}
        )

        albums_scanned = _safe_int(summary.get("albums_scanned"))
        for provider in ("discogs", "lastfm", "bandcamp"):
            scan_value = summary.get(f"scan_{provider}_matched")
            if scan_value is not None and albums_scanned:
                summary[f"{provider}_match"] = {
                    "matched": _safe_int(scan_value),
                    "total": albums_scanned,
                }
            elif last_fix_all_total_albums:
                summary[f"{provider}_match"] = {
                    "matched": _provider_identified(last_fix_all_by_provider, provider),
                    "total": _safe_int(last_fix_all_total_albums),
                }
            else:
                summary[f"{provider}_match"] = {"matched": 0, "total": 0}

        result["summary"] = summary
    except Exception as exc:
        log = logger or logging.getLogger(__name__)
        log.debug("api_progress: could not load last_scan_summary: %s", exc)
    return result
