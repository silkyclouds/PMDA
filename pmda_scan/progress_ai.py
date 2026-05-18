from __future__ import annotations

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


def load_current_scan_ai_rollup(
    *,
    scanning: bool,
    scan_id: int | None,
    connect_readonly: Callable[..., Any],
    logger: logging.Logger | None = None,
) -> Mapping[str, Any] | None:
    """Load the AI cost rollup for the currently running scan, if any."""

    if not scanning or not scan_id:
        return None
    try:
        con = connect_readonly(timeout=2)
        try:
            cur = con.cursor()
            cur.execute(
                """
                SELECT
                    calls_total,
                    total_tokens,
                    cost_total_microusd,
                    unpriced_calls,
                    lifecycle_complete,
                    updated_at
                FROM ai_scan_cost_rollups
                WHERE scan_id = ?
                LIMIT 1
                """,
                (int(scan_id),),
            )
            return cur.fetchone()
        finally:
            con.close()
    except Exception as exc:
        log = logger or logging.getLogger(__name__)
        log.debug("api_progress: could not load current_scan_ai_rollup: %s", exc)
        return None


def effective_ai_usage(
    *,
    scanning: bool,
    current_scan_ai_rollup: Mapping[str, Any] | None,
    scan_ai_guard_calls_used: int,
    scan_ai_used_count: int,
    last_scan_summary: Mapping[str, Any] | None,
    last_scan_ai_used_count: int,
    last_scan_ai_tokens_total: int,
    last_scan_ai_cost_usd_total: float,
    last_scan_ai_unpriced_calls: int,
    microusd_to_usd: Callable[[int], float],
) -> dict[str, Any]:
    """Resolve AI counters shown in the scan progress payload."""

    used_count = _safe_int(scan_ai_guard_calls_used)
    if current_scan_ai_rollup is not None:
        used_count = max(used_count, _safe_int(current_scan_ai_rollup["calls_total"]))
    if used_count <= 0:
        used_count = _safe_int(scan_ai_used_count)
    if not scanning and used_count <= 0:
        if isinstance(last_scan_summary, Mapping):
            used_count = _safe_int(
                last_scan_summary.get("ai_calls_total")
                or last_scan_summary.get("ai_used_count")
            )
        if used_count <= 0:
            used_count = _safe_int(last_scan_ai_used_count)

    tokens_total = 0
    cost_usd_total = 0.0
    unpriced_calls = 0
    if current_scan_ai_rollup is not None:
        tokens_total = _safe_int(current_scan_ai_rollup["total_tokens"])
        try:
            cost_usd_total = float(microusd_to_usd(_safe_int(current_scan_ai_rollup["cost_total_microusd"])))
        except Exception:
            cost_usd_total = 0.0
        unpriced_calls = _safe_int(current_scan_ai_rollup["unpriced_calls"])

    if isinstance(last_scan_summary, Mapping) and not scanning:
        tokens_total = _safe_int(last_scan_summary.get("ai_tokens_total") or last_scan_ai_tokens_total)
        cost_usd_total = _safe_float(last_scan_summary.get("ai_cost_usd_total") or last_scan_ai_cost_usd_total)
        unpriced_calls = _safe_int(last_scan_summary.get("ai_unpriced_calls") or last_scan_ai_unpriced_calls)

    return {
        "used_count": used_count,
        "tokens_total": tokens_total,
        "cost_usd_total": cost_usd_total,
        "unpriced_calls": unpriced_calls,
    }
