"""Library index rebuild state helpers.

The PostgreSQL rebuild itself is an effectful runtime job. This module owns the
pure state transitions used by API, MCP, scan header reporting, and tests.
"""

from __future__ import annotations

import time
from typing import Any, Optional


def merge_index_state(
    current: dict[str, Any] | None,
    updates: dict[str, Any] | None,
    *,
    now: float | None = None,
) -> dict[str, Any]:
    """Merge state updates and reset ETA/rate fields when the phase changes."""
    state = dict(current or {})
    incoming = dict(updates or {})
    timestamp = float(time.time() if now is None else now)
    current_phase = str(state.get("phase") or "").strip()
    next_phase = str(incoming.get("phase") or current_phase).strip()
    if next_phase and next_phase != current_phase:
        incoming.setdefault("phase_started_at", timestamp)
        incoming.setdefault("phase_eta_seconds", None)
        incoming.setdefault("phase_rate_per_sec", None)
        incoming.setdefault("phase_progress", None)
    incoming.setdefault("updated_at", timestamp)
    state.update(incoming)
    return state


def index_is_running(state: dict[str, Any] | None, *, phases: set[str] | None = None) -> bool:
    """Return whether an index job is running, optionally limited to phase names."""
    current = state or {}
    if not bool(current.get("running")):
        return False
    if not phases:
        return True
    phase = str(current.get("phase") or "").strip().lower()
    normalized = {str(item or "").strip().lower() for item in (phases or set()) if str(item or "").strip()}
    return phase in normalized


def progress_metrics(
    processed: Any,
    total: Any,
    *,
    started_at: Any = None,
    now: float | None = None,
) -> tuple[Optional[float], Optional[int], Optional[float]]:
    """Return progress percent, ETA seconds, and rate per second."""
    try:
        processed_n = max(0, int(processed or 0))
    except Exception:
        processed_n = 0
    try:
        total_n = max(0, int(total or 0))
    except Exception:
        total_n = 0
    if total_n <= 0:
        return None, None, None

    ratio = min(1.0, float(processed_n) / float(total_n))
    progress = round(ratio * 100.0, 2)
    try:
        started_val = float(started_at or 0.0)
    except Exception:
        started_val = 0.0
    if started_val <= 0.0 or processed_n <= 0 or processed_n >= total_n:
        return progress, None, None

    elapsed = max(0.0, float(time.time() if now is None else now) - started_val)
    if elapsed <= 0.0:
        return progress, None, None
    rate = float(processed_n) / elapsed
    if rate <= 0.0:
        return progress, None, None
    remaining = max(0, total_n - processed_n)
    eta_seconds = int(round(float(remaining) / rate)) if remaining > 0 else 0
    return progress, eta_seconds, round(rate, 2)


def status_payload(
    state: dict[str, Any] | None,
    *,
    indexed_artists: int = 0,
    indexed_albums: int = 0,
    indexed_tracks: int = 0,
    reco_embeddings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the API/MCP-safe library index status payload."""
    payload = dict(state or {"running": False, "phase": None, "error": None})
    payload["indexed_artists"] = int(indexed_artists or 0)
    payload["indexed_albums"] = int(indexed_albums or 0)
    payload["indexed_tracks"] = int(indexed_tracks or 0)
    payload["reco_embeddings"] = dict(reco_embeddings or {})
    return payload
