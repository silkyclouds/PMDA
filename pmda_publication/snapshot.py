"""Published library snapshot policy helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class PublishedScopeContext:
    """Runtime roots used to build SQL filters for published snapshot rows."""

    library_roots: tuple[str, ...] = ()
    inbox_roots: tuple[str, ...] = ()
    dupe_roots: tuple[str, ...] = ()
    use_virtual_scope: bool = False


def should_use_published_snapshot(
    *,
    scope: str | None,
    scan_running: bool = False,
    index_running: bool = False,
    index_underbuilt: bool = False,
) -> bool:
    """Return True when browse APIs should avoid touching the live rebuild."""
    scope_norm = str(scope or "library").strip().lower()
    if scope_norm not in {"", "library", "auto", "published", "snapshot"}:
        return False
    return bool(scan_running or index_running or index_underbuilt or scope_norm in {"published", "snapshot"})


def snapshot_counts_payload(
    *,
    albums: Any = 0,
    artists: Any = 0,
    tracks: Any = 0,
    source: str = "published_snapshot",
) -> dict[str, int | str]:
    """Build a normalized count payload for published browse snapshots."""
    def _int(value: Any) -> int:
        try:
            return max(0, int(value or 0))
        except Exception:
            return 0

    return {
        "source": str(source or "published_snapshot"),
        "albums": _int(albums),
        "artists": _int(artists),
        "tracks": _int(tracks),
    }


def progress_library_visibility(
    *,
    files_mode: bool,
    include_unmatched_default: bool,
    scanning: bool,
    cached_payload: dict[str, Any] | None,
    scan_processed_albums_count: int,
    total_albums: int,
    scan_published_albums_count: int,
    browse_counts: Callable[[bool], tuple[int | None, int | None]],
    effective_browse_snapshot: Callable[[bool], dict[str, Any]],
) -> dict[str, Any]:
    """Resolve library visibility counters for the scan progress endpoint."""

    result: dict[str, Any] = {
        "include_unmatched_default": bool(include_unmatched_default),
        "albums_count": None,
        "artists_count": None,
        "tracks_count": None,
        "fallback_source": None,
    }
    if not files_mode:
        return result

    try:
        if scanning:
            visible_albums, visible_artists = browse_counts(bool(include_unmatched_default))
            scan_catalog_committed = bool(
                _safe_int(scan_processed_albums_count) >= _safe_int(total_albums)
                and _safe_int(total_albums) > 0
            )
            if visible_albums is not None:
                result["albums_count"] = _safe_int(visible_albums)
            if visible_artists is not None:
                result["artists_count"] = _safe_int(visible_artists)
            if not scan_catalog_committed:
                result["albums_count"] = max(_safe_int(result["albums_count"]), _safe_int(scan_published_albums_count))
            if isinstance(cached_payload, dict):
                cached_visible_albums = _safe_int(cached_payload.get("library_visible_albums_count"))
                if result["albums_count"] in (None, 0):
                    result["albums_count"] = cached_visible_albums
                elif not scan_catalog_committed:
                    result["albums_count"] = max(_safe_int(result["albums_count"]), cached_visible_albums)
                if result["artists_count"] in (None, 0):
                    cached_artists = _safe_int(cached_payload.get("library_visible_artists_count"))
                    result["artists_count"] = cached_artists or None
                result["tracks_count"] = _safe_int(cached_payload.get("library_visible_tracks_count")) or None
                fallback_source = str(cached_payload.get("library_visible_fallback_source") or "").strip()
                result["fallback_source"] = fallback_source or None
            if _safe_int(result["albums_count"]) > 0 and not result["fallback_source"]:
                result["fallback_source"] = None if _safe_int(result["artists_count"]) > 0 else "published"
            return result

        snapshot = effective_browse_snapshot(bool(include_unmatched_default))
        result["albums_count"] = _safe_int(snapshot.get("visible_albums"))
        result["artists_count"] = _safe_int(snapshot.get("visible_artists"))
        result["tracks_count"] = _safe_int(snapshot.get("visible_tracks"))
        fallback_source = str(snapshot.get("fallback_source") or "").strip()
        result["fallback_source"] = fallback_source or None
    except Exception:
        result["albums_count"] = None
        result["artists_count"] = None
        result["tracks_count"] = None
        result["fallback_source"] = None
    return result


def should_fallback_to_published(
    snapshot: dict[str, Any] | None,
    *,
    albums: int | None = None,
    artists: int | None = None,
) -> bool:
    """Return True when the published snapshot is safer than the live browse index."""

    snap = snapshot if isinstance(snapshot, dict) else {}
    published_albums = _safe_int(snap.get("published_albums"))
    if published_albums <= 0:
        return False
    if bool(snap.get("underbuilt")):
        return True
    album_count = _safe_int(snap.get("pg_albums") if albums is None else albums)
    artist_count = _safe_int(snap.get("pg_artists") if artists is None else artists)
    return album_count <= 0 or artist_count <= 0


def browse_source_effective(
    *,
    scope: str,
    requested: str | None = None,
    snapshot: dict[str, Any] | None = None,
    scan_busy: bool = False,
    index_running: bool = False,
) -> str:
    """Resolve live vs published browse source for API routes."""

    browse_source = str(requested or "auto").strip().lower()
    if browse_source in {"live", "published"}:
        return browse_source
    snap = snapshot if isinstance(snapshot, dict) else {}
    index_state = dict(snap.get("index_state") or {})
    running = bool(index_running or index_state.get("running"))
    published_albums = _safe_int(snap.get("published_albums"))
    if published_albums > 0 and bool(snap.get("api_lightweight")):
        return "published"
    if published_albums > 0 and (scan_busy or running or bool(snap.get("underbuilt"))):
        return "published"
    return "live"


def sqlite_path_prefix_match_sql(column: str, roots: list[str] | tuple[str, ...], params: list[Any]) -> str:
    """Build a SQLite path-prefix predicate and append query parameters."""

    normalized = [str(root or "").rstrip("/") for root in roots if str(root or "").strip()]
    if not normalized:
        return "0=1"
    clauses: list[str] = []
    for root in normalized:
        clauses.append(f"({column} = ? OR {column} LIKE ?)")
        params.extend([root, f"{root}/%"])
    return "(" + " OR ".join(clauses) + ")"


def published_scope_where_sqlite(
    scope: str,
    params: list[Any],
    *,
    context: PublishedScopeContext,
) -> str:
    """Build the published-snapshot SQLite WHERE clause for a browse scope."""

    scope_norm = str(scope or "library").strip().lower() or "library"
    if scope_norm == "all":
        return "1=1"
    if scope_norm == "library":
        if context.use_virtual_scope:
            return "COALESCE(is_broken, 0) = 0"
        return sqlite_path_prefix_match_sql("folder_path", context.library_roots, params)
    if scope_norm == "dupes":
        return sqlite_path_prefix_match_sql("folder_path", context.dupe_roots, params)
    if scope_norm == "inbox":
        parts: list[str] = []
        if context.inbox_roots:
            parts.append(sqlite_path_prefix_match_sql("folder_path", context.inbox_roots, params))
        if context.use_virtual_scope:
            parts.append("NOT (COALESCE(is_broken, 0) = 0)")
        elif context.library_roots:
            parts.append(f"NOT {sqlite_path_prefix_match_sql('folder_path', context.library_roots, params)}")
        if context.dupe_roots:
            parts.append(f"NOT {sqlite_path_prefix_match_sql('folder_path', context.dupe_roots, params)}")
        parts.append("COALESCE(strict_match_verified, 0) = 0")
        return "(" + " AND ".join(parts) + ")" if parts else "1=1"
    return "1=1"


def published_album_where_sqlite(
    *,
    include_unmatched: bool,
    context: PublishedScopeContext,
    scope: str = "library",
    search_query: str = "",
    genre: str = "",
    label: str = "",
    year: int = 0,
) -> tuple[str, list[Any]]:
    """Build the published-snapshot album browse WHERE clause."""

    where_parts = ["1=1"]
    params: list[Any] = []
    if not include_unmatched:
        where_parts.append("(COALESCE(strict_match_verified, 0) = 1 OR COALESCE(mb_identified, 0) = 1)")
    where_parts.append(published_scope_where_sqlite(scope, params, context=context))
    if str(search_query or "").strip():
        like = f"%{str(search_query).strip()}%"
        where_parts.append("(album_title LIKE ? COLLATE NOCASE OR artist_name LIKE ? COLLATE NOCASE OR tracks_json LIKE ? COLLATE NOCASE)")
        params.extend([like, like, like])
    if int(year or 0) > 0:
        where_parts.append("COALESCE(year, 0) = ?")
        params.append(int(year))
    if str(label or "").strip():
        label_parts = [part.strip().lower() for part in str(label).split(",") if part.strip()]
        if label_parts:
            where_parts.append("(" + " OR ".join(["lower(trim(COALESCE(label, ''))) = ?"] * len(label_parts)) + ")")
            params.extend(label_parts)
    if str(genre or "").strip():
        genre_parts = [part.strip().lower() for part in str(genre).split(",") if part.strip()]
        if genre_parts:
            subparts: list[str] = []
            for part in genre_parts:
                subparts.append("lower(COALESCE(tags_json, '[]')) LIKE ?")
                params.append(f'%"{part}"%')
                subparts.append("lower(trim(COALESCE(genre, ''))) = ?")
                params.append(part)
            where_parts.append("(" + " OR ".join(subparts) + ")")
    return " AND ".join(where_parts), params


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except Exception:
        return 0
