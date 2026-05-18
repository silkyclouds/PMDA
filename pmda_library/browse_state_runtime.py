"""Runtime-backed library browse state helpers.

These helpers keep Albums/Artists routes responsive during scan or index
rebuilds by deciding when to trust the live PostgreSQL browse index and when to
fall back to the durable published SQLite snapshot.
"""

from __future__ import annotations

import logging
from typing import Any

from pmda_publication import snapshot as _publication_snapshot


def files_library_browse_counts_for_runtime(
    runtime: Any,
    include_unmatched: bool,
    *,
    scope: str = "library",
    acquire_timeout_sec: float = 0.20,
) -> tuple[int | None, int | None]:
    """Read browse-visible album/artist counts using the browse endpoint gates."""
    if runtime._get_library_mode() != "files":
        return (None, None)
    scope_norm = runtime._normalize_library_scope(scope, default="library")
    if include_unmatched and scope_norm in {"library", "all"}:
        fast_artists, fast_albums, _fast_tracks = runtime._files_index_read_counts_fast(
            acquire_timeout_sec=acquire_timeout_sec
        )
        if int(fast_albums or 0) > 0 or int(fast_artists or 0) > 0:
            return (int(fast_albums or 0), int(fast_artists or 0))
    if not runtime._files_pg_init_schema():
        return (None, None)
    conn = runtime._files_pg_connect(acquire_timeout_sec=acquire_timeout_sec)
    if conn is None:
        return (None, None)
    try:
        matched_where = runtime._library_albums_match_where(include_unmatched, "alb")
        scope_where = runtime._library_album_scope_where(scope, "alb")
        with conn.cursor() as cur:
            statement_timeout_ms = int(max(25, min(500, round(float(acquire_timeout_sec or 0.20) * 1000.0))))
            if statement_timeout_ms > 0:
                cur.execute(f"SET statement_timeout = {statement_timeout_ms}")
            cur.execute(
                f"""
                SELECT
                    (
                        SELECT COUNT(*)::BIGINT
                        FROM files_albums alb
                        WHERE {matched_where}
                          AND {scope_where}
                    ) AS album_count,
                    (
                        SELECT COUNT(*)::BIGINT
                        FROM files_artists a
                        WHERE EXISTS (
                            SELECT 1
                            FROM files_artist_album_links link
                            JOIN files_albums alb ON alb.id = link.album_id
                            WHERE link.artist_id = a.id
                              AND {matched_where}
                              AND {scope_where}
                        )
                    ) AS artist_count
                """
            )
            row = cur.fetchone() or (0, 0)
        return (int(row[0] or 0), int(row[1] or 0))
    except Exception:
        logging.debug("Failed to read Files browse counts", exc_info=True)
        return (None, None)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def files_scan_busy_for_runtime(runtime: Any) -> bool:
    """Return whether any scan/post-processing phase is currently active."""
    with runtime.lock:
        return bool(
            runtime.state.get("scanning")
            or runtime.state.get("scan_starting")
            or runtime.state.get("scan_finalizing")
            or runtime.state.get("scan_post_processing")
        )


def files_library_browse_snapshot_for_runtime(
    runtime: Any,
    include_unmatched: bool,
    *,
    scope: str = "library",
) -> dict[str, Any]:
    """Snapshot live and published browse counts for user-visible fallback decisions."""
    pg_albums, pg_artists = runtime._files_library_browse_counts(include_unmatched, scope=scope)
    try:
        _pg_artists_total, _pg_albums_total, pg_tracks_total = runtime._files_index_read_counts_fast(
            acquire_timeout_sec=0.20
        )
    except Exception:
        pg_tracks_total = 0
    published_albums, published_artists, published_tracks = runtime._files_library_published_browse_counts(
        include_unmatched,
        scope=scope,
    )
    index_state = runtime._files_index_get_state() or {}
    scan_busy = files_scan_busy_for_runtime(runtime)
    pg_albums_i = int(pg_albums or 0)
    pg_artists_i = int(pg_artists or 0)
    published_albums_i = int(published_albums or 0)
    published_artists_i = int(published_artists or 0)
    underbuilt = False
    if published_albums_i > 0:
        if pg_albums is None or pg_artists is None:
            underbuilt = True
        elif pg_albums_i <= 0 or pg_artists_i <= 0:
            underbuilt = True
        elif str(index_state.get("phase") or "").strip().lower() == "error" and (
            pg_albums_i < published_albums_i or pg_artists_i < published_artists_i
        ):
            underbuilt = True
        elif scan_busy and (
            pg_albums_i < max(25, int(published_albums_i * 0.55))
            or pg_artists_i < max(12, int(max(1, published_artists_i) * 0.55))
        ):
            underbuilt = True
    return {
        "scan_busy": scan_busy,
        "index_state": index_state,
        "pg_albums": pg_albums_i,
        "pg_artists": pg_artists_i,
        "pg_tracks": int(pg_tracks_total or 0),
        "published_albums": published_albums_i,
        "published_artists": published_artists_i,
        "published_tracks": int(published_tracks or 0),
        "underbuilt": bool(underbuilt),
    }


def files_library_effective_browse_snapshot_for_runtime(
    runtime: Any,
    include_unmatched: bool,
    *,
    scope: str = "library",
) -> dict[str, Any]:
    """Resolve the effective user-visible browse counts."""
    snapshot = runtime._files_library_browse_snapshot(include_unmatched, scope=scope)
    pg_albums = int(snapshot.get("pg_albums") or 0)
    pg_artists = int(snapshot.get("pg_artists") or 0)
    pg_tracks = int(snapshot.get("pg_tracks") or 0)
    published_albums = int(snapshot.get("published_albums") or 0)
    published_artists = int(snapshot.get("published_artists") or 0)
    published_tracks = int(snapshot.get("published_tracks") or 0)
    prefer_published = bool(
        published_albums > 0
        and (
            bool(snapshot.get("underbuilt"))
            or pg_albums <= 0
            or pg_artists <= 0
        )
    )
    return {
        **snapshot,
        "fallback_source": "published" if prefer_published else None,
        "visible_albums": published_albums if prefer_published else pg_albums,
        "visible_artists": published_artists if prefer_published else pg_artists,
        "visible_tracks": published_tracks if prefer_published else pg_tracks,
    }


def files_library_api_browse_snapshot_for_runtime(
    runtime: Any,
    include_unmatched: bool,
    *,
    scope: str = "library",
) -> dict[str, Any]:
    """Lightweight snapshot used by browse API routes before list queries."""
    published_albums, published_artists, published_tracks = runtime._files_library_published_browse_counts(
        include_unmatched,
        scope=scope,
    )
    index_state = runtime._files_index_get_state() or {}
    scan_busy = files_scan_busy_for_runtime(runtime)
    phase = str(index_state.get("phase") or "").strip().lower()
    index_running = bool(index_state.get("running") or runtime.files_index_lock.locked())
    return {
        "scan_busy": bool(scan_busy),
        "index_state": index_state,
        "pg_albums": 0,
        "pg_artists": 0,
        "pg_tracks": 0,
        "published_albums": int(published_albums or 0),
        "published_artists": int(published_artists or 0),
        "published_tracks": int(published_tracks or 0),
        "underbuilt": bool(int(published_albums or 0) > 0 and (index_running or phase == "error")),
        "api_lightweight": True,
    }


def files_library_browse_source_effective_for_runtime(
    runtime: Any,
    *,
    scope: str,
    requested: str | None = None,
    snapshot: dict[str, Any] | None = None,
    scan_busy: bool | None = None,
) -> str:
    """Choose live vs published browse source for the current request."""
    busy = runtime._files_scan_busy() if scan_busy is None else bool(scan_busy)
    return _publication_snapshot.browse_source_effective(
        scope=scope,
        requested=requested,
        snapshot=snapshot,
        scan_busy=busy,
        index_running=bool(runtime.files_index_lock.locked()),
    )
