"""Runtime-adapter helpers for the scan progress endpoint.

These helpers keep mutable runtime access at the edge while moving repeated
progress endpoint behavior out of the historical monolith. They are deliberately
small and callback-driven so they can be tested without importing ``pmda.py``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any


def cached_provider_gateway_snapshot(cached_payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return the provider gateway fallback snapshot from a cached progress payload."""

    payload = cached_payload or {}
    try:
        inflight = int(payload.get("provider_gateway_inflight") or 0)
    except Exception:
        inflight = 0
    try:
        max_inflight = int(payload.get("provider_gateway_max_inflight_observed") or 0)
    except Exception:
        max_inflight = 0
    return {
        "providers": dict(payload.get("scan_provider_stats_live") or {}),
        "inflight": inflight,
        "max_inflight_observed": max_inflight,
    }


def normalize_provider_gateway_snapshot(snapshot: Mapping[str, Any] | None) -> dict[str, Any]:
    """Normalize provider gateway stats into lowercase provider buckets."""

    raw_providers = dict((snapshot or {}).get("providers") or {})
    providers = {
        str(provider or "").strip().lower(): dict(bucket or {})
        for provider, bucket in raw_providers.items()
        if str(provider or "").strip()
    }
    return {
        "providers": providers,
        "gateway": dict(snapshot or {}),
    }


def provider_gateway_live_stats(
    *,
    scanning: bool,
    snapshot_loader: Callable[[dict[str, Any]], Mapping[str, Any] | None],
    cached_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return live provider gateway stats, falling back to cached stats on errors."""

    fallback = cached_provider_gateway_snapshot(cached_payload)
    if not scanning:
        return {"providers": {}, "gateway": {}}
    try:
        return normalize_provider_gateway_snapshot(snapshot_loader(fallback))
    except Exception:
        return normalize_provider_gateway_snapshot(fallback)


def resume_availability_snapshot(
    *,
    scanning: bool,
    library_mode_loader: Callable[[], str],
    get_resume_run_snapshot: Callable[[str, str], Mapping[str, Any] | None],
    get_latest_resume_run_snapshot_any_signature: Callable[[str, str], Mapping[str, Any] | None],
    existing_available: bool = False,
    existing_by_scan_type: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the scan-type keyed resume availability payload."""

    collected: dict[str, dict[str, Any]] = {
        str(scan_type): dict(snapshot)
        for scan_type, snapshot in dict(existing_by_scan_type or {}).items()
        if isinstance(snapshot, Mapping) and snapshot.get("available")
    }
    if scanning:
        return {
            "resume_available": bool(existing_available or collected),
            "resume_available_by_scan_type": collected,
        }
    if not collected:
        current_mode = library_mode_loader()
        for resume_scan_type in ("full", "changed_only"):
            snap = get_resume_run_snapshot(current_mode, resume_scan_type)
            if not (isinstance(snap, Mapping) and snap.get("available")):
                snap = get_latest_resume_run_snapshot_any_signature(current_mode, resume_scan_type)
            if isinstance(snap, Mapping) and snap.get("available"):
                collected[resume_scan_type] = dict(snap)
    return {
        "resume_available": bool(existing_available or collected),
        "resume_available_by_scan_type": collected,
    }
