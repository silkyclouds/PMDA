"""Pure helpers for library browse endpoints."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BrowseCacheKeys:
    cache_key: str
    stable_cache_key: str


def live_cache_generation(runtime_state: Mapping[str, Any]) -> str:
    """Return the browse-cache generation suffix for active scan state."""

    active = bool(
        runtime_state.get("scanning")
        or runtime_state.get("scan_starting")
        or runtime_state.get("scan_finalizing")
        or runtime_state.get("scan_post_processing")
    )
    if not active:
        return "idle"
    return (
        f"{int(runtime_state.get('scan_published_albums_count') or 0)}:"
        f"{int(runtime_state.get('scan_processed_albums_count') or 0)}:"
        f"{int(runtime_state.get('scan_artists_processed') or 0)}"
    )


def browse_cache_keys(
    *,
    kind: str,
    search_query: str,
    genre: str,
    label: str,
    year: int,
    sort: str,
    limit: int,
    offset: int,
    scope_suffix: str,
    unmatched_suffix: str,
    browse_source: str,
    live_generation: str,
    user_id: int | None = None,
) -> BrowseCacheKeys:
    """Build stable/live cache keys for library browse endpoints."""

    prefix = f"library:{kind}:"
    if user_id is not None:
        prefix += f"u{int(user_id)}:"
    base = (
        f"{prefix}{str(search_query or '').lower()}:{str(genre or '').lower()}:"
        f"{str(label or '').lower()}:{int(year or 0)}:{sort}:{int(limit)}:{int(offset)}:"
        f"{scope_suffix}:{unmatched_suffix}"
    )
    return BrowseCacheKeys(
        cache_key=f"{base}:{live_generation}:{browse_source}",
        stable_cache_key=f"{base}:{browse_source}",
    )
