"""Runtime-owned publication reconciliation from persisted scan editions."""

from __future__ import annotations

import logging
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "reconcile_files_publication_from_scan_editions_for_runtime",
            "trigger_files_publication_reconcile_async_for_runtime",
            "_reconcile_files_publication_from_scan_editions",
            "_trigger_files_publication_reconcile_async",
        }:
            continue
        globals()[name] = value


def reconcile_files_publication_from_scan_editions_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
    _bind_runtime(runtime)
    return _reconcile_files_publication_from_scan_editions(*args, **kwargs)


def trigger_files_publication_reconcile_async_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> bool:
    _bind_runtime(runtime)
    return _trigger_files_publication_reconcile_async(*args, **kwargs)


def _reconcile_files_publication_from_scan_editions(
    *,
    scan_ids: list[int] | tuple[int, ...] | set[int] | None = None,
    reason: str = "manual",
    rebuild_index: bool = True,
) -> dict[str, Any]:
    """
    Backfill files_library_published_albums from persisted scan truth.

    This repairs interrupted/resumed scans where scan_editions and the album
    scan cache were written, but the progressive publication row or its final
    moved destination never landed in the visible library snapshot.
    """
    if _get_library_mode() != "files":
        return {"status": "skipped", "reason": "not_files_mode", "published": 0}
    scan_id_values = sorted({int(_parse_int_loose(v, 0) or 0) for v in (scan_ids or []) if int(_parse_int_loose(v, 0) or 0) > 0})
    reason_norm = re.sub(r"[^a-z0-9_.:-]+", "-", str(reason or "manual").strip().lower()).strip("-") or "manual"
    if not _FILES_PUBLICATION_RECONCILE_LOCK.acquire(blocking=False):
        with lock:
            running_state = dict(state.get("files_publication_reconcile") or {})
        return {"status": "already_running", **running_state}
    stats: dict[str, Any] = {
        "status": "running",
        "reason": reason_norm,
        "scan_ids": scan_id_values,
        "candidate_rows": 0,
        "published": 0,
        "groups": 0,
        "skipped_existing": 0,
        "skipped_missing": 0,
        "skipped_moved_away": 0,
        "skipped_duplicate_loser": 0,
        "hidden_original_rows": 0,
        "started_at": time.time(),
    }
    with lock:
        state["files_publication_reconcile"] = dict(stats)
    con = None
    try:
        con = _state_connect(timeout=60)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        if not _sqlite_table_exists(cur, "scan_editions"):
            stats["status"] = "skipped"
            stats["reason"] = "scan_editions_missing"
            return stats
        published_paths = {
            str(r[0] or "").strip()
            for r in cur.execute("SELECT folder_path FROM files_library_published_albums").fetchall()
            if str(r[0] or "").strip()
        }
        published_rows_by_path = _files_publication_load_published_rows_by_folder(cur)
        move_maps = _files_publication_scan_move_maps(cur)
        duplicate_loser_keys: set[tuple[int, int]] = set()
        duplicate_loser_ids: set[int] = set()
        try:
            if _sqlite_table_exists(cur, "duplicates_loser"):
                cur.execute("PRAGMA table_info(duplicates_loser)")
                loser_cols = {str(r[1] or "").strip() for r in cur.fetchall() if len(r) > 1}
                loser_id_col = "loser_album_id" if "loser_album_id" in loser_cols else "album_id"
                scan_col = "scan_id" if "scan_id" in loser_cols else None
                cur.execute(
                    f"SELECT {scan_col if scan_col else '0'} AS scan_id, COALESCE({loser_id_col}, 0) AS album_id FROM duplicates_loser"
                )
                for row in cur.fetchall():
                    sid = int(_parse_int_loose(row["scan_id"], 0) or 0)
                    aid = int(_parse_int_loose(row["album_id"], 0) or 0)
                    if aid <= 0:
                        continue
                    duplicate_loser_ids.add(aid)
                    if sid > 0:
                        duplicate_loser_keys.add((sid, aid))
        except Exception:
            logging.debug("Failed to load duplicate loser map for publication reconciliation", exc_info=True)

        cur.execute("PRAGMA table_info(scan_editions)")
        cols = {str(r[1] or "").strip() for r in cur.fetchall() if len(r) > 1}

        def col(name: str, fallback: str = "''") -> str:
            return name if name in cols else f"{fallback} AS {name}"

        where = "WHERE COALESCE(folder, '') <> ''"
        params: list[Any] = []
        if scan_id_values:
            placeholders = ",".join("?" for _ in scan_id_values)
            where += f" AND scan_id IN ({placeholders})"
            params.extend(scan_id_values)
        cur.execute(
            f"""
            SELECT
                scan_id,
                artist,
                album_id,
                title_raw,
                folder,
                {col('fmt_text')},
                {col('br', '0')},
                {col('sr', '0')},
                {col('bd', '0')},
                {col('meta_json')},
                {col('musicbrainz_id')},
                {col('musicbrainz_release_id')},
                {col('discogs_release_id')},
                {col('lastfm_album_mbid')},
                {col('bandcamp_album_url')},
                {col('metadata_source')},
                {col('strict_match_verified', '0')},
                {col('strict_match_provider')},
                {col('strict_reject_reason')},
                {col('strict_tracklist_score', '0.0')},
                {col('is_broken', '0')},
                {col('expected_track_count', '0')},
                {col('actual_track_count', '0')},
                {col('missing_indices', "'[]'")},
                {col('has_cover', '0')},
                {col('missing_required_tags', "'[]'")}
            FROM scan_editions
            {where}
            ORDER BY scan_id DESC, artist, album_id
            """,
            tuple(params),
        )
        scan_rows = cur.fetchall()
        stats["candidate_rows"] = len(scan_rows)
        with lock:
            current_state = dict(state.get("files_publication_reconcile") or {})
            current_state.update({"candidate_rows": int(stats["candidate_rows"]), "phase": "planning"})
            state["files_publication_reconcile"] = current_state
        grouped: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
        fallback_rows: list[tuple[sqlite3.Row, str, str, str, int, int]] = []
        direct_rows: list[dict[str, Any]] = []
        seen_source_folders: set[str] = set()
        hidden_originals: set[str] = set()
        for row in scan_rows:
            sid = int(_parse_int_loose(row["scan_id"], 0) or 0)
            album_id = int(_parse_int_loose(row["album_id"], 0) or 0)
            original_raw = str(row["folder"] or "").strip()
            original_key = _album_folder_cache_key(Path(original_raw)) if original_raw else ""
            if not original_key or original_key in seen_source_folders:
                continue
            seen_source_folders.add(original_key)
            if (sid, album_id) in duplicate_loser_keys or (not scan_id_values and album_id in duplicate_loser_ids):
                stats["skipped_duplicate_loser"] += 1
                continue
            if (sid, album_id) in move_maps["suppressed_keys"] or original_key in move_maps["suppressed_sources"]:
                stats["skipped_moved_away"] += 1
                continue
            moved_dest = move_maps["matched_by_key"].get((sid, album_id)) or move_maps["matched_by_source"].get(original_key)
            if moved_dest:
                folder_key = _album_folder_cache_key(Path(str(moved_dest or "").strip()))
                live_folder = str(moved_dest or "").strip()
                if original_key and original_key != folder_key:
                    prior_row = published_rows_by_path.get(original_key)
                    remapped = _files_publication_remap_published_row(
                        prior_row or {},
                        original_key=original_key,
                        folder_key=folder_key,
                        scan_id=sid if sid > 0 else None,
                    )
                    if remapped:
                        direct_rows.append(remapped)
                        hidden_originals.add(original_key)
                        continue
                    hidden_originals.add(original_key)
            else:
                candidate = _files_publication_candidate_existing_path(original_raw)
                if candidate is None:
                    stats["skipped_missing"] += 1
                    continue
                folder_key, live_folder = candidate
            if folder_key in published_paths and original_key not in hidden_originals:
                stats["skipped_existing"] += 1
                continue
            fallback_rows.append((row, original_key, folder_key, live_folder, sid, album_id))

        stats["direct_remap_candidates"] = len(direct_rows)
        if direct_rows:
            stats["published"] += int(_upsert_files_library_published_rows(direct_rows) or 0)
            published_paths.update(str(r.get("folder_path") or "").strip() for r in direct_rows if str(r.get("folder_path") or "").strip())
            with lock:
                current_state = dict(state.get("files_publication_reconcile") or {})
                current_state.update(
                    {
                        "phase": "direct_remap",
                        "direct_remap_candidates": int(stats["direct_remap_candidates"]),
                        "published": int(stats["published"]),
                    }
                )
                state["files_publication_reconcile"] = current_state

        fallback_keys = []
        for _row, original_key, folder_key, _live_folder, _sid, _album_id in fallback_rows:
            fallback_keys.extend([original_key, folder_key])
        cache_map = _load_files_album_scan_cache_map_for_keys(
            fallback_keys,
            include_ordered_paths=True,
        )
        stats["fallback_candidates"] = len(fallback_rows)
        with lock:
            current_state = dict(state.get("files_publication_reconcile") or {})
            current_state.update({"phase": "fallback_build", "fallback_candidates": int(stats["fallback_candidates"])})
            state["files_publication_reconcile"] = current_state

        for row, original_key, folder_key, live_folder, sid, _album_id in fallback_rows:
            cache_payload = cache_map.get(original_key) or cache_map.get(folder_key) or {}
            item = _scan_edition_row_to_publication_item(
                row,
                cache_payload=cache_payload,
                folder_key=folder_key,
                live_folder=live_folder,
            )
            if not item:
                stats["skipped_missing"] += 1
                continue
            if original_key and folder_key and original_key != folder_key:
                item["ordered_paths"] = [
                    _files_publication_rewrite_path_prefix(str(path), original_key, folder_key)
                    for path in list(item.get("ordered_paths") or [])
                    if str(path or "").strip()
                ]
            artist_name = str(item.get("artist") or "Unknown Artist").strip() or "Unknown Artist"
            grouped[(sid, artist_name)].append(item)
        if hidden_originals:
            stats["hidden_original_rows"] = int(_delete_files_library_published_rows(hidden_originals) or 0)
            published_paths.difference_update(hidden_originals)
        stats["groups"] = len(grouped)
        with lock:
            state["files_publication_reconcile"] = dict(stats)
        for index, ((sid, artist_name), items) in enumerate(grouped.items(), start=1):
            with lock:
                current_state = dict(state.get("files_publication_reconcile") or {})
                current_state.update(
                    {
                        "current_artist": artist_name,
                        "groups_done": index - 1,
                        "groups_total": len(grouped),
                    }
                )
                state["files_publication_reconcile"] = current_state
            try:
                result = _publish_files_library_artist_live_batches(
                    artist_name,
                    items,
                    scan_id=sid if sid > 0 else None,
                    results_by_album_id={},
                )
                stats["published"] += int((result or {}).get("published") or 0)
            except Exception:
                logging.debug(
                    "Files publication reconciliation failed artist=%s scan_id=%s",
                    artist_name,
                    sid,
                    exc_info=True,
                )
        stats["status"] = "completed"
        stats["finished_at"] = time.time()
        logging.info(
            "Files publication reconcile complete reason=%s scans=%s candidates=%d published=%d direct_remap=%d fallback=%d existing=%d missing=%d moved_away=%d duplicate_loser=%d hidden_original_rows=%d",
            reason_norm,
            scan_id_values or "all",
            int(stats.get("candidate_rows") or 0),
            int(stats.get("published") or 0),
            int(stats.get("direct_remap_candidates") or 0),
            int(stats.get("fallback_candidates") or 0),
            int(stats.get("skipped_existing") or 0),
            int(stats.get("skipped_missing") or 0),
            int(stats.get("skipped_moved_away") or 0),
            int(stats.get("skipped_duplicate_loser") or 0),
            int(stats.get("hidden_original_rows") or 0),
        )
        for sid in scan_id_values:
            try:
                _refresh_scan_history_from_published(sid)
            except Exception:
                logging.debug("Failed to refresh scan history after publication reconcile scan_id=%s", sid, exc_info=True)
        if rebuild_index and int(stats.get("published") or 0) > 0:
            _trigger_files_index_rebuild_async(reason=f"publication_reconcile_{reason_norm}")
        return stats
    except Exception as exc:
        stats["status"] = "error"
        stats["error"] = str(exc)
        logging.exception("Files publication reconciliation failed reason=%s", reason_norm)
        return stats
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
        with lock:
            final_state = dict(stats)
            final_state.setdefault("finished_at", time.time())
            state["files_publication_reconcile"] = final_state
        _FILES_PUBLICATION_RECONCILE_LOCK.release()


def _trigger_files_publication_reconcile_async(
    *,
    scan_ids: list[int] | tuple[int, ...] | set[int] | None = None,
    reason: str = "manual",
    rebuild_index: bool = True,
) -> bool:
    if _get_library_mode() != "files":
        return False
    if _FILES_PUBLICATION_RECONCILE_LOCK.locked():
        return False
    reason_norm = re.sub(r"[^a-z0-9]+", "-", str(reason or "manual").strip().lower()).strip("-") or "manual"

    def _runner() -> None:
        _reconcile_files_publication_from_scan_editions(
            scan_ids=scan_ids,
            reason=reason_norm,
            rebuild_index=rebuild_index,
        )

    threading.Thread(target=_runner, daemon=True, name=f"files-publication-reconcile-{reason_norm}").start()
    return True
