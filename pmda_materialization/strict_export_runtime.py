"""Runtime-owned strict export backlog and smart-provider promotion jobs."""

from __future__ import annotations

import logging
import re
import sqlite3
import sys
import threading
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "load_scan_strict_export_items_for_runtime",
            "strict_export_backlog_summary_for_runtime",
            "smart_provider_promotion_status_for_runtime",
            "trigger_smart_provider_promotion_async_for_runtime",
            "trigger_scan_strict_match_export_async_for_runtime",
            "trigger_strict_export_backlog_async_for_runtime",
            "run_strict_export_backlog_for_runtime",
            "run_scan_strict_match_export_for_runtime",
            "_load_scan_strict_export_items",
            "_strict_export_excluded_prefixes",
            "_strict_export_backlog_summary",
            "_smart_provider_promotion_status",
            "_smart_provider_expected_id",
            "_smart_provider_fetch_payload_for_row",
            "_load_smart_provider_promotion_rows",
            "_mark_smart_provider_promoted",
            "_run_smart_provider_promotion_backlog",
            "_trigger_smart_provider_promotion_async",
            "_load_strict_export_backlog_items",
            "_run_strict_export_backlog",
            "_run_scan_strict_match_export",
            "_trigger_scan_strict_match_export_async",
            "_trigger_strict_export_backlog_async",
            "_SMART_PROVIDER_PROMOTION_LOCK",
        }:
            continue
        globals()[name] = value


def load_scan_strict_export_items_for_runtime(runtime: Any, scan_id: int) -> dict[str, list[dict]]:
    _bind_runtime(runtime)
    return _load_scan_strict_export_items(scan_id)


def strict_export_backlog_summary_for_runtime(runtime: Any) -> dict[str, Any]:
    _bind_runtime(runtime)
    return _strict_export_backlog_summary()


def smart_provider_promotion_status_for_runtime(runtime: Any) -> dict[str, Any]:
    _bind_runtime(runtime)
    return _smart_provider_promotion_status()


def trigger_smart_provider_promotion_async_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> bool:
    _bind_runtime(runtime)
    return _trigger_smart_provider_promotion_async(*args, **kwargs)


def trigger_scan_strict_match_export_async_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> bool:
    _bind_runtime(runtime)
    return _trigger_scan_strict_match_export_async(*args, **kwargs)


def trigger_strict_export_backlog_async_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> bool:
    _bind_runtime(runtime)
    return _trigger_strict_export_backlog_async(*args, **kwargs)


def run_strict_export_backlog_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
    _bind_runtime(runtime)
    return _run_strict_export_backlog(*args, **kwargs)


def run_scan_strict_match_export_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
    _bind_runtime(runtime)
    return _run_scan_strict_match_export(*args, **kwargs)


def _load_scan_strict_export_items(scan_id: int) -> dict[str, list[dict]]:
    sid = int(scan_id or 0)
    if sid <= 0:
        return {}

    def _read_rows() -> list[sqlite3.Row]:
        con = _state_connect_readonly(timeout=20.0)
        try:
            cur = con.cursor()
            if not _sqlite_table_exists(cur, "scan_pipeline_trace"):
                return []
            cur.execute(
                """
                SELECT
                    artist,
                    album_id,
                    album_title,
                    folder,
                    fmt_text,
                    metadata_source,
                    strict_match_provider,
                    strict_tracklist_score,
                    actual_track_count,
                    is_broken,
                    move_status
                FROM scan_pipeline_trace
                WHERE scan_id = ?
                  AND COALESCE(strict_match_verified, 0) = 1
                  AND COALESCE(is_broken, 0) = 0
                  AND LOWER(COALESCE(move_status, 'none')) = 'none'
                ORDER BY artist COLLATE NOCASE, album_title COLLATE NOCASE, album_id
                """,
                (sid,),
            )
            return list(cur.fetchall())
        finally:
            con.close()

    try:
        rows = _state_db_retry(_read_rows, label=f"load_scan_strict_export_items:{sid}", attempts=10)
    except Exception:
        logging.exception("[LIBRARY] Failed to load deferred strict export rows for scan_id=%s", sid)
        return {}
    grouped: dict[str, list[dict]] = defaultdict(list)
    storage_plan = _storage_materialization_plan_entries()
    for row in rows or []:
        artist_name = str(row["artist"] or "").strip() or "Unknown Artist"
        folder = str(row["folder"] or "").strip()
        if not folder:
            continue
        title = str(row["album_title"] or "").strip() or Path(folder).name
        item = {
            "artist": artist_name,
            "artist_name": artist_name,
            "album_id": int(row["album_id"] or 0),
            "source_id": int(row["album_id"] or 0),
            "title_raw": title,
            "album_title": title,
            "folder": folder,
            "fmt_text": str(row["fmt_text"] or ""),
            "format": str(row["fmt_text"] or ""),
            "metadata_source": str(row["metadata_source"] or ""),
            "primary_metadata_source": str(row["metadata_source"] or ""),
            "strict_match_verified": True,
            "strict_match_provider": str(row["strict_match_provider"] or row["metadata_source"] or ""),
            "strict_tracklist_score": float(row["strict_tracklist_score"] or 0.0),
            "actual_track_count": int(row["actual_track_count"] or 0),
            "tracks": [],
            "ordered_paths": [],
            "meta": {},
        }
        item.update(_storage_materialization_meta_for_folder(folder, plan_entries=storage_plan))
        grouped[artist_name].append(item)
    return grouped


def _strict_export_excluded_prefixes() -> tuple[str, str, str]:
    export_root = _album_folder_cache_key(Path(str(EXPORT_ROOT or "").strip() or "/music/Music_matched"))
    dupe_root = _album_folder_cache_key(Path(str(getattr(sys.modules[__name__], "DUPE_ROOT", "/dupes") or "/dupes").strip() or "/dupes"))
    incomplete_root = _album_folder_cache_key(
        Path(str(_get_config_from_db("INCOMPLETE_ALBUMS_TARGET_DIR") or "/dupes/incomplete_albums").strip() or "/dupes/incomplete_albums")
    )
    return export_root.rstrip("/"), dupe_root.rstrip("/"), incomplete_root.rstrip("/")


def _strict_export_backlog_summary() -> dict[str, Any]:
    summary: dict[str, Any] = {
        "available": False,
        "strict_total": 0,
        "strict_exported_root": 0,
        "strict_unexported_eligible": 0,
        "strict_unexported_by_provider": {},
        "strict_by_root": {},
        "provider_metadata_total": 0,
        "provider_metadata_by_source": {},
        "provider_metadata_only_total": 0,
        "provider_metadata_only_by_source": {},
        "moves_all_time": {},
    }
    try:
        export_root, dupe_root, incomplete_root = _strict_export_excluded_prefixes()
        con = _state_connect_readonly(timeout=20.0)
        con.row_factory = sqlite3.Row
        try:
            cur = con.cursor()
            if not _sqlite_table_exists(cur, "files_library_published_albums"):
                return summary | {"reason": "files_library_published_albums_missing"}
            root_case = """
                CASE
                    WHEN folder_path LIKE '/music/Music_matched/%' THEN 'matched'
                    WHEN folder_path LIKE '/music/Music_dump/%' THEN 'dump'
                    WHEN folder_path LIKE '/music/incomming/%' THEN 'incoming'
                    WHEN folder_path LIKE '/dupes/%' THEN 'dupes'
                    ELSE 'other'
                END
            """
            cur.execute(
                f"""
                SELECT {root_case} AS root, COUNT(*) AS count
                FROM files_library_published_albums
                WHERE COALESCE(strict_match_verified, 0) = 1
                  AND COALESCE(is_broken, 0) = 0
                GROUP BY root
                ORDER BY count DESC
                """
            )
            strict_by_root = {str(row["root"] or "other"): int(row["count"] or 0) for row in cur.fetchall()}
            strict_total = int(sum(strict_by_root.values()))
            cur.execute(
                """
                SELECT LOWER(COALESCE(strict_match_provider, 'unknown')) AS provider, COUNT(*) AS count
                FROM files_library_published_albums
                WHERE COALESCE(strict_match_verified, 0) = 1
                  AND COALESCE(is_broken, 0) = 0
                  AND COALESCE(folder_path, '') <> ''
                  AND folder_path NOT LIKE ? || '/%'
                  AND folder_path NOT LIKE ? || '/%'
                  AND folder_path NOT LIKE ? || '/%'
                GROUP BY LOWER(COALESCE(strict_match_provider, 'unknown'))
                ORDER BY count DESC, provider ASC
                """,
                (export_root, dupe_root, incomplete_root),
            )
            strict_unexported_by_provider = {str(row["provider"] or "unknown"): int(row["count"] or 0) for row in cur.fetchall()}
            cur.execute(
                """
                SELECT LOWER(COALESCE(primary_metadata_source, 'none')) AS provider, COUNT(*) AS count
                FROM files_library_published_albums
                WHERE COALESCE(is_broken, 0) = 0
                  AND COALESCE(primary_metadata_source, '') <> ''
                GROUP BY LOWER(COALESCE(primary_metadata_source, 'none'))
                ORDER BY count DESC, provider ASC
                """
            )
            provider_metadata_all = {str(row["provider"] or "none"): int(row["count"] or 0) for row in cur.fetchall()}
            cur.execute(
                """
                SELECT LOWER(COALESCE(primary_metadata_source, 'none')) AS provider, COUNT(*) AS count
                FROM files_library_published_albums
                WHERE COALESCE(strict_match_verified, 0) = 0
                  AND COALESCE(is_broken, 0) = 0
                  AND COALESCE(primary_metadata_source, '') <> ''
                GROUP BY LOWER(COALESCE(primary_metadata_source, 'none'))
                ORDER BY count DESC, provider ASC
                """
            )
            provider_metadata_only = {str(row["provider"] or "none"): int(row["count"] or 0) for row in cur.fetchall()}
            moves_all_time: dict[str, dict[str, int]] = {}
            if _sqlite_table_exists(cur, "scan_moves"):
                cur.execute(
                    """
                    SELECT LOWER(COALESCE(move_reason, 'unknown')) AS reason,
                           COUNT(*) AS count,
                           COALESCE(SUM(COALESCE(size_mb, 0)), 0) AS size_mb
                    FROM scan_moves
                    GROUP BY LOWER(COALESCE(move_reason, 'unknown'))
                    ORDER BY count DESC
                    """
                )
                moves_all_time = {
                    str(row["reason"] or "unknown"): {
                        "count": int(row["count"] or 0),
                        "size_mb": int(row["size_mb"] or 0),
                    }
                    for row in cur.fetchall()
                }
            summary.update(
                {
                    "available": True,
                    "strict_total": strict_total,
                    "strict_exported_root": int(strict_by_root.get("matched") or 0),
                    "strict_unexported_eligible": int(sum(strict_unexported_by_provider.values())),
                    "strict_unexported_by_provider": strict_unexported_by_provider,
                    "strict_by_root": strict_by_root,
                    "provider_metadata_total": int(sum(provider_metadata_all.values())),
                    "provider_metadata_by_source": provider_metadata_all,
                    "provider_metadata_only_total": int(sum(provider_metadata_only.values())),
                    "provider_metadata_only_by_source": provider_metadata_only,
                    "moves_all_time": moves_all_time,
                    "excluded_prefixes": {
                        "export_root": export_root,
                        "dupe_root": dupe_root,
                        "incomplete_root": incomplete_root,
                    },
                    "safety": {
                        "strict_export_safe": True,
                        "provider_metadata_only_safe_to_auto_move": False,
                        "provider_metadata_only_reason": (
                            "primary_metadata_source records metadata/enrichment provenance, not strict identity. "
                            "Only strict_match_verified rows have passed identity/tracklist checks."
                        ),
                    },
                }
            )
            return summary
        finally:
            con.close()
    except Exception:
        logging.debug("Strict export backlog summary failed", exc_info=True)
        summary["reason"] = "query_failed"
        return summary


_SMART_PROVIDER_PROMOTION_LOCK = threading.Lock()


def _smart_provider_promotion_status() -> dict[str, Any]:
    with lock:
        prog = dict(state.get("smart_provider_promotion") or {})
    if not prog:
        return {
            "running": False,
            "done": 0,
            "total": 0,
            "promoted": 0,
            "rejected": 0,
            "errors": 0,
            "reason": None,
            "error": None,
        }
    return prog


def _smart_provider_expected_id(provider: str, row: sqlite3.Row | dict[str, Any]) -> str:
    p = _normalize_identity_provider(provider)

    def _get(key: str) -> str:
        try:
            return str(row[key] or "").strip()
        except Exception:
            return ""

    if p == "musicbrainz":
        return _get("musicbrainz_release_id") or _get("musicbrainz_release_group_id")
    if p == "discogs":
        return _get("discogs_release_id")
    if p == "lastfm":
        return _get("lastfm_album_mbid")
    if p == "bandcamp":
        return _get("bandcamp_album_url")
    return ""


def _smart_provider_fetch_payload_for_row(
    provider: str,
    *,
    artist_name: str,
    album_title: str,
    row: sqlite3.Row | dict[str, Any],
) -> dict[str, Any] | None:
    p = _normalize_identity_provider(provider)
    expected_id = _smart_provider_expected_id(p, row)
    if p == "musicbrainz":
        ref = expected_id
        if not ref:
            return None
        payload = _fetch_musicbrainz_strict_payload(ref)
        return payload if isinstance(payload, dict) and (payload.get("id") or payload.get("release_group_id")) else None
    if p == "discogs":
        if expected_id:
            payload = _fetch_discogs_release_by_id(expected_id)
            if isinstance(payload, dict):
                return payload
        return fetch_provider_album_lookup_cached("discogs", artist_name, album_title, _fetch_discogs_release)
    if p == "bandcamp":
        return fetch_provider_album_lookup_cached(
            "bandcamp",
            artist_name,
            album_title,
            lambda current_artist, current_title: _fetch_bandcamp_album_info(
                current_artist,
                current_title,
                allow_web_fallback=False,
                album_url_hint=expected_id,
            ),
        )
    if p == "lastfm":
        if expected_id:
            return _fetch_lastfm_album_info(artist_name, album_title, mbid=expected_id)
        return fetch_provider_album_lookup_cached("lastfm", artist_name, album_title, _fetch_lastfm_album_info)
    if p == "itunes":
        return fetch_provider_album_lookup_cached("itunes", artist_name, album_title, _fetch_itunes_album_info)
    if p == "deezer":
        return fetch_provider_album_lookup_cached("deezer", artist_name, album_title, _fetch_deezer_album_info)
    return None


def _load_smart_provider_promotion_rows(limit: int | None = None) -> list[sqlite3.Row]:
    lim = int(limit or 0)

    def _read_rows() -> list[sqlite3.Row]:
        con = _state_connect_readonly(timeout=30.0)
        con.row_factory = sqlite3.Row
        try:
            cur = con.cursor()
            if not _sqlite_table_exists(cur, "files_library_published_albums"):
                return []
            limit_sql = "LIMIT ?" if lim > 0 else ""
            params: list[Any] = []
            if lim > 0:
                params.append(lim)
            cur.execute(
                f"""
                SELECT
                    folder_path,
                    source_id,
                    scan_id,
                    artist_name,
                    album_title,
                    primary_metadata_source,
                    musicbrainz_release_group_id,
                    musicbrainz_release_id,
                    discogs_release_id,
                    lastfm_album_mbid,
                    bandcamp_album_url,
                    primary_tags_json,
                    tracks_json,
                    strict_reject_reason,
                    updated_at
                FROM files_library_published_albums
                WHERE COALESCE(strict_match_verified, 0) = 0
                  AND COALESCE(is_broken, 0) = 0
                  AND COALESCE(folder_path, '') <> ''
                  AND LOWER(COALESCE(primary_metadata_source, '')) IN (
                      'musicbrainz', 'discogs', 'bandcamp', 'lastfm', 'itunes', 'deezer'
                  )
                ORDER BY updated_at ASC, artist_name COLLATE NOCASE, album_title COLLATE NOCASE, folder_path
                {limit_sql}
                """,
                tuple(params),
            )
            return list(cur.fetchall())
        finally:
            con.close()

    return _state_db_retry(_read_rows, label="load_smart_provider_promotion_rows", attempts=8)


def _mark_smart_provider_promoted(
    *,
    folder_path: str,
    provider: str,
    score: float,
) -> None:
    p = _normalize_identity_provider(provider)
    folder = str(folder_path or "").strip()
    if not folder or not p:
        return

    def _write_sqlite() -> None:
        con = _state_connect(timeout=30.0)
        try:
            cur = con.cursor()
            cur.execute(
                """
                UPDATE files_library_published_albums
                SET strict_match_verified = 1,
                    strict_match_provider = ?,
                    strict_reject_reason = '',
                    strict_tracklist_score = ?
                WHERE folder_path = ?
                """,
                (p, float(score or 0.0), folder),
            )
            if _sqlite_table_exists(cur, "files_album_scan_cache"):
                cur.execute(
                    """
                    UPDATE files_album_scan_cache
                    SET strict_match_verified = 1,
                        strict_match_provider = ?,
                        strict_reject_reason = '',
                        strict_tracklist_score = ?
                    WHERE folder_path = ?
                    """,
                    (p, float(score or 0.0), folder),
                )
            con.commit()
        finally:
            con.close()

    _state_db_retry(_write_sqlite, label="mark_smart_provider_promoted", attempts=8)

    try:
        with _files_pg_connection() as pg:
            if pg is None:
                return
            with pg.transaction():
                with pg.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE files_albums
                        SET strict_match_verified = TRUE,
                            strict_match_provider = %s,
                            strict_reject_reason = '',
                            strict_tracklist_score = %s,
                            updated_at = NOW()
                        WHERE folder_path = %s
                        """,
                        (p, float(score or 0.0), folder),
                    )
    except Exception:
        logging.debug("Failed to mirror smart provider promotion to PG for %s", folder, exc_info=True)


def _run_smart_provider_promotion_backlog(
    *,
    limit: int | None = None,
    export_after: bool = True,
    reason: str = "smart_provider_promotion",
) -> dict[str, Any]:
    if _get_library_mode() != "files":
        return {"ok": False, "error": "not_files_mode"}
    if not _SMART_PROVIDER_PROMOTION_LOCK.acquire(blocking=False):
        return {"ok": False, "error": "already_running"}
    try:
        rows = _load_smart_provider_promotion_rows(limit=limit)
        total = len(rows or [])
        with lock:
            state["smart_provider_promotion"] = {
                "running": True,
                "done": 0,
                "total": total,
                "promoted": 0,
                "rejected": 0,
                "errors": 0,
                "reason": str(reason or "smart_provider_promotion"),
                "current": None,
                "started_at": time.time(),
                "updated_at": time.time(),
                "finished_at": None,
                "error": None,
            }
        promoted = 0
        rejected = 0
        errors = 0
        rejected_by_reason: Counter[str] = Counter()
        promoted_by_provider: Counter[str] = Counter()
        logging.info("[LIBRARY] [↻🔄] smart provider promotion started: %d album(s)", total)
        for idx, row in enumerate(rows or [], start=1):
            folder = str(row["folder_path"] or "").strip()
            artist_name = str(row["artist_name"] or "").strip()
            album_title = str(row["album_title"] or "").strip()
            provider = _normalize_identity_provider(str(row["primary_metadata_source"] or ""))
            with lock:
                prog = dict(state.get("smart_provider_promotion") or {})
                prog.update({
                    "done": idx - 1,
                    "current": f"{artist_name} - {album_title}",
                    "updated_at": time.time(),
                })
                state["smart_provider_promotion"] = prog
            try:
                tracks = _json_loads_safe(row["tracks_json"], [])
                if not isinstance(tracks, list):
                    tracks = []
                tags = _json_loads_safe(row["primary_tags_json"], {})
                if not isinstance(tags, dict):
                    tags = {}
                payload = _smart_provider_fetch_payload_for_row(
                    provider,
                    artist_name=artist_name,
                    album_title=album_title,
                    row=row,
                )
                if not isinstance(payload, dict) or not payload:
                    rejected += 1
                    rejected_by_reason["payload_missing"] += 1
                    continue
                verdict = _strict_provider_match_100(
                    local_artist=artist_name,
                    local_title=album_title,
                    local_tracks=tracks,
                    local_tags=tags,
                    provider=provider,
                    provider_payload=payload,
                    expected_provider_id=_smart_provider_expected_id(provider, row),
                )
                if bool(verdict.get("strict_match_verified")):
                    score = float(verdict.get("strict_tracklist_score") or 0.0)
                    _mark_smart_provider_promoted(
                        folder_path=folder,
                        provider=str(verdict.get("strict_match_provider") or provider),
                        score=score,
                    )
                    promoted += 1
                    promoted_by_provider[str(verdict.get("strict_match_provider") or provider or "unknown")] += 1
                    if verdict.get("smart_match_verified"):
                        logging.info(
                            "[MATCH] Smart-promoted %s - %r via %s (%s, score=%.2f)",
                            artist_name,
                            album_title,
                            str(verdict.get("strict_match_provider") or provider),
                            str(verdict.get("smart_match_reason") or "smart"),
                            score,
                        )
                    continue
                reason_code = str(verdict.get("strict_reject_reason") or "strict_reject").strip() or "strict_reject"
                rejected += 1
                rejected_by_reason[reason_code] += 1
            except DiscogsRateLimited:
                rejected += 1
                rejected_by_reason["discogs_rate_limited"] += 1
                logging.warning("[LIBRARY] Smart provider promotion paused by Discogs rate limit")
                break
            except Exception:
                errors += 1
                rejected_by_reason["error"] += 1
                logging.debug("Smart provider promotion failed for %s", folder, exc_info=True)
            finally:
                with lock:
                    prog = dict(state.get("smart_provider_promotion") or {})
                    prog.update({
                        "done": idx,
                        "promoted": promoted,
                        "rejected": rejected,
                        "errors": errors,
                        "updated_at": time.time(),
                    })
                    state["smart_provider_promotion"] = prog
        result = {
            "ok": True,
            "total": total,
            "promoted": promoted,
            "rejected": rejected,
            "errors": errors,
            "promoted_by_provider": dict(promoted_by_provider),
            "rejected_by_reason": dict(rejected_by_reason),
        }
        with lock:
            prog = dict(state.get("smart_provider_promotion") or {})
            prog.update(result)
            prog["running"] = False
            prog["finished_at"] = time.time()
            prog["updated_at"] = time.time()
            state["smart_provider_promotion"] = prog
        logging.info(
            "[LIBRARY] [V✅] smart provider promotion finished: promoted=%d rejected=%d errors=%d",
            promoted,
            rejected,
            errors,
        )
        if export_after and promoted > 0 and PIPELINE_ENABLE_EXPORT:
            _trigger_strict_export_backlog_async(reason="smart_provider_promotion_export")
        return result
    except Exception as exc:
        with lock:
            prog = dict(state.get("smart_provider_promotion") or {})
            prog.update({
                "running": False,
                "error": str(exc),
                "finished_at": time.time(),
                "updated_at": time.time(),
            })
            state["smart_provider_promotion"] = prog
        logging.exception("[LIBRARY] Smart provider promotion failed")
        return {"ok": False, "error": str(exc)}
    finally:
        try:
            _SMART_PROVIDER_PROMOTION_LOCK.release()
        except Exception:
            pass


def _trigger_smart_provider_promotion_async(
    *,
    limit: int | None = None,
    export_after: bool = True,
    reason: str = "smart_provider_promotion",
) -> bool:
    with lock:
        prog = state.get("smart_provider_promotion") or {}
        if prog.get("running"):
            return False
    thread = threading.Thread(
        target=lambda: _run_smart_provider_promotion_backlog(
            limit=limit,
            export_after=export_after,
            reason=reason,
        ),
        name="smart-provider-promotion",
        daemon=True,
    )
    thread.start()
    return True


def _load_strict_export_backlog_items(limit: int | None = None) -> dict[str, list[dict]]:
    export_root, dupe_root, incomplete_root = _strict_export_excluded_prefixes()
    lim = int(limit or 0)

    def _read_rows() -> list[sqlite3.Row]:
        con = _state_connect_readonly(timeout=30.0)
        con.row_factory = sqlite3.Row
        try:
            cur = con.cursor()
            if not _sqlite_table_exists(cur, "files_library_published_albums"):
                return []
            limit_sql = "LIMIT ?" if lim > 0 else ""
            params: list[Any] = [export_root, dupe_root, incomplete_root]
            if lim > 0:
                params.append(lim)
            cur.execute(
                f"""
                SELECT
                    folder_path,
                    source_id,
                    scan_id,
                    artist_name,
                    album_title,
                    format,
                    strict_match_provider,
                    strict_tracklist_score,
                    primary_metadata_source,
                    musicbrainz_release_group_id,
                    musicbrainz_release_id,
                    discogs_release_id,
                    lastfm_album_mbid,
                    bandcamp_album_url,
                    track_count,
                    actual_track_count,
                    has_cover,
                    has_artist_image,
                    primary_tags_json,
                    tracks_json,
                    tags_json,
                    is_broken,
                    updated_at
                FROM files_library_published_albums
                WHERE COALESCE(strict_match_verified, 0) = 1
                  AND COALESCE(is_broken, 0) = 0
                  AND COALESCE(folder_path, '') <> ''
                  AND folder_path NOT LIKE ? || '/%'
                  AND folder_path NOT LIKE ? || '/%'
                  AND folder_path NOT LIKE ? || '/%'
                ORDER BY updated_at ASC, artist_name COLLATE NOCASE, album_title COLLATE NOCASE, folder_path
                {limit_sql}
                """,
                tuple(params),
            )
            return list(cur.fetchall())
        finally:
            con.close()

    try:
        rows = _state_db_retry(_read_rows, label="load_strict_export_backlog_items", attempts=10)
    except Exception:
        logging.exception("[LIBRARY] Failed to load strict export backlog rows")
        return {}
    grouped: dict[str, list[dict]] = defaultdict(list)
    storage_plan = _storage_materialization_plan_entries()
    for row in rows or []:
        folder = str(row["folder_path"] or "").strip()
        if not folder:
            continue
        artist_name = str(row["artist_name"] or "").strip() or "Unknown Artist"
        title = str(row["album_title"] or "").strip() or Path(folder).name
        tracks = _json_loads_safe(row["tracks_json"], [])
        if not isinstance(tracks, list):
            tracks = []
        meta = _json_loads_safe(row["primary_tags_json"], {})
        if not isinstance(meta, dict):
            meta = {}
        item = {
            "artist": artist_name,
            "artist_name": artist_name,
            "album_id": int(row["source_id"] or 0),
            "source_id": int(row["source_id"] or 0),
            "scan_id": int(row["scan_id"] or 0),
            "title_raw": title,
            "album_title": title,
            "folder": folder,
            "fmt_text": str(row["format"] or ""),
            "format": str(row["format"] or ""),
            "metadata_source": str(row["primary_metadata_source"] or ""),
            "primary_metadata_source": str(row["primary_metadata_source"] or ""),
            "strict_match_verified": True,
            "strict_match_provider": str(row["strict_match_provider"] or row["primary_metadata_source"] or ""),
            "strict_tracklist_score": float(row["strict_tracklist_score"] or 0.0),
            "actual_track_count": int(row["actual_track_count"] or row["track_count"] or 0),
            "track_count": int(row["track_count"] or row["actual_track_count"] or 0),
            "has_cover": bool(row["has_cover"]),
            "has_artist_image": bool(row["has_artist_image"]),
            "musicbrainz_id": str(row["musicbrainz_release_group_id"] or ""),
            "musicbrainz_release_group_id": str(row["musicbrainz_release_group_id"] or ""),
            "musicbrainz_release_id": str(row["musicbrainz_release_id"] or ""),
            "discogs_release_id": str(row["discogs_release_id"] or ""),
            "lastfm_album_mbid": str(row["lastfm_album_mbid"] or ""),
            "bandcamp_album_url": str(row["bandcamp_album_url"] or ""),
            "tracks": tracks,
            "ordered_paths": [],
            "meta": meta,
        }
        item.update(_storage_materialization_meta_for_folder(folder, plan_entries=storage_plan))
        grouped[artist_name].append(item)
    return grouped


def _run_strict_export_backlog(*, limit: int | None = None, reason: str = "strict_backlog_export") -> dict[str, Any]:
    if library_is_audit_mode():
        return {"ok": False, "error": "audit_mode"}
    if _get_library_mode() != "files":
        return {"ok": False, "error": "not_files_mode"}
    if not PIPELINE_ENABLE_EXPORT:
        return {"ok": False, "error": "export_disabled"}
    export_root = str(EXPORT_ROOT or "").strip()
    if not export_root:
        return {"ok": False, "error": "export_root_missing"}
    if _storage_power_saver_active() and not _storage_materialization_plan_entries():
        logging.warning(
            "[STORAGE] Refusing strict backlog export while power saver is enabled: no disk-aware materialization plan is available."
        )
        return {"ok": False, "error": "storage_plan_missing"}
    grouped = _load_strict_export_backlog_items(limit=limit)
    total_albums = sum(len(items) for items in grouped.values())
    with lock:
        state["export_progress"] = {
            "running": True,
            "tracks_done": 0,
            "total_tracks": 0,
            "albums_done": 0,
            "total_albums": total_albums,
            "error": None,
            "reason": str(reason or "strict_backlog_export"),
            "scan_id": None,
            "strict_backlog": True,
            "conflicts_held": 0,
            "moved_count": 0,
            "published_count": 0,
        }
    moved_count = 0
    published_count = 0
    conflicts_held = 0
    try:
        if total_albums <= 0:
            logging.info("[LIBRARY] [»⏭] no strict export backlog album to materialize")
            return {"ok": True, "albums": 0, "moved": 0, "published": 0, "conflicts_held": 0}
        logging.info(
            "[LIBRARY] [↻🔄] exporting strict backlog: %d album(s), strategy=%s, reason=%s",
            total_albums,
            str(EXPORT_LINK_STRATEGY or "hardlink"),
            str(reason or "strict_backlog_export"),
        )
        albums_done = 0
        current_bucket_key: tuple[int, str] | None = None
        for bucket_order, device_id, artist_name, items in _storage_ordered_materialization_groups(grouped):
            bucket_key = (int(bucket_order), str(device_id or ""))
            if bucket_key != current_bucket_key:
                current_bucket_key = bucket_key
                if device_id:
                    logging.info(
                        "[STORAGE] Deferred strict backlog export bucket start: device=%s bucket=%s albums_done=%d/%d",
                        device_id,
                        bucket_order,
                        albums_done,
                        total_albums,
                    )
                with lock:
                    prog = dict(state.get("export_progress") or {})
                    prog["storage_current_device_id"] = str(device_id or "")
                    prog["storage_bucket_order"] = int(bucket_order)
                    state["export_progress"] = prog
            before_by_album = {
                int(item.get("album_id") or _stable_album_id_for_review(item, str(item.get("folder") or ""))): str(item.get("folder") or "").strip()
                for item in items
            }
            moved_items = _move_publish_items_to_matched_library(
                artist_name,
                list(items),
                export_root=export_root,
                scan_id_override=None,
            )
            for moved_item in moved_items or []:
                album_key = int(moved_item.get("album_id") or _stable_album_id_for_review(moved_item, str(moved_item.get("folder") or "")))
                before = before_by_album.get(album_key, "")
                after = str(moved_item.get("folder") or "").strip()
                if moved_item.get("export_conflict_review"):
                    conflicts_held += 1
                if before and after and before != after:
                    moved_count += 1
            by_scan: dict[int, list[dict]] = defaultdict(list)
            for moved_item in moved_items or []:
                by_scan[_export_item_scan_id(moved_item, None)].append(moved_item)
            for sid, sid_items in by_scan.items():
                try:
                    summary = _publish_files_library_artist_live_batches(
                        artist_name,
                        list(sid_items or []),
                        scan_id=(sid if sid > 0 else None),
                        results_by_album_id={},
                    )
                    published_count += int((summary or {}).get("published") or 0)
                except Exception:
                    logging.debug("Strict backlog publication failed for artist=%s scan_id=%s", artist_name, sid, exc_info=True)
            albums_done += len(items)
            with lock:
                prog = dict(state.get("export_progress") or {})
                prog["albums_done"] = albums_done
                prog["moved_count"] = moved_count
                prog["published_count"] = published_count
                prog["conflicts_held"] = conflicts_held
                state["export_progress"] = prog
        logging.info(
            "[LIBRARY] [V✅] strict backlog export finished: moved=%d/%d, published=%d, held_conflicts=%d",
            moved_count,
            total_albums,
            published_count,
            conflicts_held,
        )
        return {
            "ok": True,
            "albums": total_albums,
            "moved": moved_count,
            "published": published_count,
            "conflicts_held": conflicts_held,
        }
    except Exception as exc:
        logging.exception("Strict backlog export failed: %s", exc)
        with lock:
            prog = dict(state.get("export_progress") or {})
            prog["error"] = str(exc)
            state["export_progress"] = prog
        return {
            "ok": False,
            "error": str(exc),
            "albums": total_albums,
            "moved": moved_count,
            "published": published_count,
            "conflicts_held": conflicts_held,
        }
    finally:
        with lock:
            prog = dict(state.get("export_progress") or {})
            prog["running"] = False
            state["export_progress"] = prog


def _run_scan_strict_match_export(scan_id: int, *, reason: str = "scan_strict_export") -> dict[str, Any]:
    """Materialize strict matches from one completed scan without rediscovering every Files root."""
    sid = int(scan_id or 0)
    if library_is_audit_mode():
        return {"ok": False, "error": "audit_mode", "scan_id": sid}
    if _get_library_mode() != "files":
        return {"ok": False, "error": "not_files_mode", "scan_id": sid}
    if not PIPELINE_ENABLE_EXPORT:
        return {"ok": False, "error": "export_disabled", "scan_id": sid}
    export_root = str(EXPORT_ROOT or "").strip()
    if not export_root:
        return {"ok": False, "error": "export_root_missing", "scan_id": sid}
    if _storage_power_saver_active() and not _storage_materialization_plan_entries():
        logging.warning(
            "[STORAGE] Refusing deferred strict scan export while power saver is enabled: no disk-aware materialization plan is available."
        )
        return {"ok": False, "error": "storage_plan_missing", "scan_id": sid}
    grouped = _load_scan_strict_export_items(sid)
    total_albums = sum(len(items) for items in grouped.values())
    with lock:
        state["export_progress"] = {
            "running": True,
            "tracks_done": 0,
            "total_tracks": 0,
            "albums_done": 0,
            "total_albums": total_albums,
            "error": None,
            "reason": str(reason or "scan_strict_export"),
            "scan_id": sid,
        }
    moved_count = 0
    published_count = 0
    try:
        if total_albums <= 0:
            logging.info("[LIBRARY] [»⏭] no deferred strict matched album to export for scan_id=%s", sid)
            return {"ok": True, "scan_id": sid, "albums": 0, "moved": 0, "published": 0}
        logging.info(
            "[LIBRARY] [↻🔄] exporting %d deferred strict matched album(s) for scan_id=%s (%s)",
            total_albums,
            sid,
            str(reason or "scan_strict_export"),
        )
        albums_done = 0
        current_bucket_key: tuple[int, str] | None = None
        for bucket_order, device_id, artist_name, items in _storage_ordered_materialization_groups(grouped):
            bucket_key = (int(bucket_order), str(device_id or ""))
            if bucket_key != current_bucket_key:
                current_bucket_key = bucket_key
                if device_id:
                    logging.info(
                        "[STORAGE] Deferred strict scan export bucket start: device=%s bucket=%s albums_done=%d/%d",
                        device_id,
                        bucket_order,
                        albums_done,
                        total_albums,
                    )
                with lock:
                    prog = dict(state.get("export_progress") or {})
                    prog["storage_current_device_id"] = str(device_id or "")
                    prog["storage_bucket_order"] = int(bucket_order)
                    state["export_progress"] = prog
            before_by_album = {
                int(item.get("album_id") or 0): str(item.get("folder") or "").strip()
                for item in items
            }
            moved_items = _move_publish_items_to_matched_library(
                artist_name,
                list(items),
                export_root=export_root,
                scan_id_override=sid,
            )
            for moved_item in moved_items or []:
                album_id = int(moved_item.get("album_id") or 0)
                before = before_by_album.get(album_id, "")
                after = str(moved_item.get("folder") or "").strip()
                if before and after and before != after:
                    moved_count += 1
            try:
                summary = _publish_files_library_artist_live_batches(
                    artist_name,
                    list(moved_items or []),
                    scan_id=sid,
                    results_by_album_id={},
                )
                published_count += int((summary or {}).get("published") or 0)
            except Exception:
                logging.debug("Deferred strict export publication failed for artist=%s scan_id=%s", artist_name, sid, exc_info=True)
            albums_done += len(items)
            with lock:
                prog = dict(state.get("export_progress") or {})
                prog["albums_done"] = albums_done
                state["export_progress"] = prog
        try:
            _sync_scan_pipeline_trace_move_rows(sid, wait_timeout_sec=5.0, poll_interval_sec=0.25)
        except Exception:
            logging.debug("Deferred strict export trace sync failed for scan_id=%s", sid, exc_info=True)
        logging.info(
            "[LIBRARY] [V✅] deferred strict export finished scan_id=%s: moved=%d/%d, published=%d",
            sid,
            moved_count,
            total_albums,
            published_count,
        )
        return {"ok": True, "scan_id": sid, "albums": total_albums, "moved": moved_count, "published": published_count}
    except Exception as exc:
        logging.exception("Deferred strict matched export failed for scan_id=%s: %s", sid, exc)
        with lock:
            prog = dict(state.get("export_progress") or {})
            prog["error"] = str(exc)
            state["export_progress"] = prog
        return {"ok": False, "scan_id": sid, "error": str(exc), "albums": total_albums, "moved": moved_count, "published": published_count}
    finally:
        with lock:
            prog = dict(state.get("export_progress") or {})
            prog["running"] = False
            state["export_progress"] = prog


def _trigger_scan_strict_match_export_async(scan_id: int, *, reason: str = "scan_strict_export") -> bool:
    sid = int(scan_id or 0)
    if sid <= 0:
        return False
    with lock:
        prog = state.get("export_progress") or {}
        if prog.get("running"):
            return False

    def _runner() -> None:
        _run_scan_strict_match_export(sid, reason=reason)

    reason_norm = re.sub(r"[^a-z0-9]+", "-", str(reason or "scan_strict_export").strip().lower()).strip("-") or "scan-strict-export"
    threading.Thread(target=_runner, daemon=True, name=f"files-export-{reason_norm}").start()
    return True


def _trigger_strict_export_backlog_async(*, limit: int | None = None, reason: str = "strict_backlog_export") -> bool:
    with lock:
        prog = state.get("export_progress") or {}
        if prog.get("running"):
            return False

    def _runner() -> None:
        _run_strict_export_backlog(limit=limit, reason=reason)

    reason_norm = re.sub(r"[^a-z0-9]+", "-", str(reason or "strict_backlog_export").strip().lower()).strip("-") or "strict-backlog-export"
    threading.Thread(target=_runner, daemon=True, name=f"files-export-{reason_norm}").start()
    return True
