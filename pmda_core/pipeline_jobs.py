"""Pipeline job heartbeat helpers.

Database access stays in the runtime layer. This module owns normalization and
row-to-payload conversion so job status semantics remain consistent for UI and
MCP.
"""

from __future__ import annotations

import json
from typing import Any, Mapping


FINISHED_STATUSES = {"completed", "failed", "cancelled", "idle"}
STALE_ERROR = "Job heartbeat is stale; the worker is no longer reporting progress."


def normalize_job_type(job_type: Any) -> str:
    return str(job_type or "").strip().lower()


def normalize_scope(scope: Any) -> str:
    return str(scope or "global").strip().lower() or "global"


def normalize_status(status: Any) -> str:
    return str(status or "running").strip().lower() or "running"


def finished_at_for_status(status: Any, *, now: float, finished: bool | None = None) -> float | None:
    status_norm = normalize_status(status)
    if finished is True:
        return float(now)
    if status_norm in FINISHED_STATUSES:
        return float(now)
    return None


def item_key(job_type: Any, scope: Any) -> str:
    job = normalize_job_type(job_type)
    scope_norm = str(scope or "global").strip() or "global"
    return job if scope_norm == "global" else f"{job}:{scope_norm}"


def parse_meta(raw: Any) -> dict[str, Any]:
    try:
        meta = json.loads(raw or "{}")
        return meta if isinstance(meta, dict) else {}
    except Exception:
        return {}


def _row_get(row: Mapping[str, Any], key: str, default: Any = None) -> Any:
    getter = getattr(row, "get", None)
    if callable(getter):
        return getter(key, default)
    try:
        return row[key]
    except Exception:
        return default


def row_to_status(row: Mapping[str, Any], *, now: float, stale_after_sec: int) -> tuple[str, dict[str, Any]] | None:
    """Convert a DB row mapping into an API/MCP job status payload."""
    job_type = normalize_job_type(_row_get(row, "job_type"))
    if not job_type:
        return None
    scope = str(_row_get(row, "scope") or "global").strip() or "global"
    heartbeat_at = float(_row_get(row, "heartbeat_at") or 0.0)
    status = normalize_status(_row_get(row, "status") or "idle")
    seconds_since_heartbeat = int(max(0.0, float(now) - heartbeat_at)) if heartbeat_at else None
    stale = bool(
        status == "running"
        and seconds_since_heartbeat is not None
        and seconds_since_heartbeat > int(stale_after_sec)
    )
    if stale:
        status = "stale"
    error_text = str(_row_get(row, "error") or "")
    return item_key(job_type, scope), {
        "job_type": job_type,
        "scope": scope,
        "run_id": _row_get(row, "run_id"),
        "status": status,
        "phase": str(_row_get(row, "phase") or ""),
        "current": int(_row_get(row, "current") or 0),
        "total": int(_row_get(row, "total") or 0),
        "current_item": str(_row_get(row, "current_item") or ""),
        "message": str(_row_get(row, "message") or ""),
        "error": STALE_ERROR if stale and not error_text else error_text,
        "started_at": float(_row_get(row, "started_at") or 0.0) or None,
        "heartbeat_at": heartbeat_at or None,
        "finished_at": float(_row_get(row, "finished_at") or 0.0) or None,
        "seconds_since_heartbeat": seconds_since_heartbeat,
        "stale": stale,
        "meta": parse_meta(_row_get(row, "meta_json")),
    }


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def running_scheduler_jobs(running_meta: Mapping[Any, Any]) -> list[dict[str, Any]]:
    """Return normalized in-memory scheduler jobs for progress payloads."""

    jobs: list[dict[str, Any]] = []
    for meta in (running_meta or {}).values():
        if not isinstance(meta, Mapping):
            continue
        jobs.append(
            {
                "run_id": str(meta.get("run_id") or "").strip(),
                "job_type": normalize_job_type(meta.get("job_type")),
                "scope": normalize_scope(meta.get("scope") or "both"),
                "source": str(meta.get("source") or "").strip().lower(),
                "origin_scan_id": _int_or_none(meta.get("origin_scan_id")),
                "started_at": meta.get("started_at"),
            }
        )
    jobs.sort(key=lambda item: float(item.get("started_at") or 0.0))
    return jobs


def background_enrichment_running(
    *,
    background_jobs: list[dict[str, Any]],
    profile_backfill_state: Mapping[str, Any] | None,
    profile_jobs_active: int,
) -> bool:
    """Return whether post-publication/background enrichment is still active."""

    active_job_types = {"enrich_batch", "dedupe", "incomplete_move", "export", "player_sync"}
    return any(
        str(item.get("job_type") or "") in active_job_types
        for item in (background_jobs or [])
    ) or bool((profile_backfill_state or {}).get("running")) or bool(int(profile_jobs_active or 0) > 0)
