"""Global duplicate review registry helpers."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def normalize_review_scope(value: str | None) -> str:
    """Normalize duplicate review scope names used by UI/API/MCP."""
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


def duplicate_group_identity(group: dict[str, Any] | None) -> tuple[str, str]:
    """Return a stable-enough identity for merging duplicate groups."""
    data = group if isinstance(group, dict) else {}
    best = data.get("best") if isinstance(data.get("best"), dict) else {}
    album_id = _safe_int(data.get("album_id") or best.get("album_id"), 0)
    if album_id > 0:
        return "album_id", str(album_id)
    norm = str(best.get("album_norm") or data.get("album_norm") or "").strip().lower()
    if norm:
        return "album_norm", norm
    title = str(best.get("title_raw") or data.get("title_raw") or "").strip().lower()
    return "title", title


def merge_duplicate_results(*sources: dict[str, list[dict[str, Any]]] | None) -> dict[str, list[dict[str, Any]]]:
    """Merge duplicate registries without dropping historical open groups."""
    merged: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[tuple[str, str, str]] = set()
    for source in sources:
        if not isinstance(source, dict):
            continue
        for artist, groups in source.items():
            artist_key = str(artist or "").strip()
            if not artist_key:
                continue
            for group in groups or []:
                if not isinstance(group, dict):
                    continue
                kind, value = duplicate_group_identity(group)
                key = (artist_key, kind, value)
                if key in seen:
                    continue
                seen.add(key)
                merged[artist_key].append(group)
    return dict(merged)


def duplicate_registry_counts(groups: dict[str, list[dict[str, Any]]] | None) -> dict[str, int]:
    """Count reviewable duplicate groups and loser albums."""
    group_count = 0
    loser_count = 0
    for artist_groups in (groups or {}).values():
        for group in artist_groups or []:
            if not isinstance(group, dict):
                continue
            if "best" not in group or "losers" not in group:
                continue
            group_count += 1
            loser_count += len(group.get("losers") or [])
    return {"groups": group_count, "losers": loser_count}
