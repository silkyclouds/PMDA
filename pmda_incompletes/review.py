"""Global incomplete review scope helpers."""

from __future__ import annotations

from typing import Any


def normalize_review_scope(value: str | None) -> str:
    """Normalize incomplete review scope names used by UI/API/MCP."""
    raw = str(value or "global").strip().lower()
    if raw in {"scan", "last", "last_scan", "latest"}:
        return "last_scan"
    if raw in {"source", "intake"}:
        return "source"
    if raw in {"destination", "library", "matched"}:
        return "destination"
    if raw in {"resolved", "closed"}:
        return "resolved"
    if raw in {"unresolved", "open", "active"}:
        return "unresolved"
    return "global"


def latest_per_album_key(item: dict[str, Any] | None) -> tuple[str, int]:
    """Build the stable key used to collapse incomplete diagnostics globally."""
    data = item if isinstance(item, dict) else {}
    artist = str(data.get("artist") or "").strip()
    try:
        album_id = int(data.get("album_id") or 0)
    except Exception:
        album_id = 0
    return artist, album_id


def collapse_latest_per_album(items: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None) -> list[dict[str, Any]]:
    """Return only the newest diagnostic per artist/album pair."""
    latest: dict[tuple[str, int], dict[str, Any]] = {}
    for item in items or []:
        if not isinstance(item, dict):
            continue
        key = latest_per_album_key(item)
        try:
            detected_at = float(item.get("detected_at") or 0.0)
        except Exception:
            detected_at = 0.0
        previous = latest.get(key)
        try:
            previous_detected_at = float((previous or {}).get("detected_at") or 0.0)
        except Exception:
            previous_detected_at = 0.0
        if previous is None or detected_at >= previous_detected_at:
            latest[key] = dict(item)
    return list(latest.values())
