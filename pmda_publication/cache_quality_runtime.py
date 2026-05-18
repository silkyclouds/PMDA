"""Runtime-owned files album scan cache quality recalculation."""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "files_cache_quality_recalc_status_unlocked_for_runtime",
            "recalculate_files_album_scan_cache_quality_for_runtime",
            "start_files_cache_quality_recalc_async_for_runtime",
            "_files_cache_quality_recalc_status_unlocked",
            "_recalculate_files_album_scan_cache_quality",
            "_start_files_cache_quality_recalc_async",
        }:
            continue
        globals()[name] = value


def files_cache_quality_recalc_status_unlocked_for_runtime(runtime: Any) -> dict[str, Any]:
    _bind_runtime(runtime)
    return _files_cache_quality_recalc_status_unlocked()


def recalculate_files_album_scan_cache_quality_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
    _bind_runtime(runtime)
    return _recalculate_files_album_scan_cache_quality(*args, **kwargs)


def start_files_cache_quality_recalc_async_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> tuple[bool, dict[str, Any]]:
    _bind_runtime(runtime)
    return _start_files_cache_quality_recalc_async(*args, **kwargs)


def _files_cache_quality_recalc_status_unlocked() -> dict[str, Any]:
    total = int(state.get("files_cache_quality_recalc_total") or 0)
    done = int(state.get("files_cache_quality_recalc_done") or 0)
    percent = round((done / total) * 100.0, 2) if total > 0 else 0.0
    return {
        "running": bool(state.get("files_cache_quality_recalc_running")),
        "total": total,
        "done": done,
        "percent": percent,
        "rows_upserted": int(state.get("files_cache_quality_recalc_rows_upserted") or 0),
        "errors": int(state.get("files_cache_quality_recalc_errors") or 0),
        "missing_folders": int(state.get("files_cache_quality_recalc_missing_folders") or 0),
        "no_audio": int(state.get("files_cache_quality_recalc_no_audio") or 0),
        "reason": str(state.get("files_cache_quality_recalc_reason") or ""),
        "started_at": state.get("files_cache_quality_recalc_started_at"),
        "updated_at": state.get("files_cache_quality_recalc_updated_at"),
        "finished_at": state.get("files_cache_quality_recalc_finished_at"),
    }


def _recalculate_files_album_scan_cache_quality(
    *,
    batch_size: int = 500,
    source_id: int | None = None,
    limit: int | None = None,
    reason: str = "manual",
) -> dict[str, Any]:
    """
    Recompute files_album_scan_cache quality flags from live filesystem state.
    This avoids a full scan and updates: tags completeness, identity, cover, artist image.
    """
    started_at = time.time()
    batch_size = max(50, int(batch_size or 500))
    source_id = int(source_id or 0)
    source_filter = source_id if source_id > 0 else 0
    limit_int = int(limit or 0)
    if limit_int <= 0:
        limit_int = 0

    where_sql = ""
    where_params: list[object] = []
    if source_filter > 0:
        where_sql = "WHERE COALESCE(source_id, 0) = ?"
        where_params.append(source_filter)

    total_rows = 0
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
        cur = con.cursor()
        cur.execute(f"SELECT COUNT(*) FROM files_album_scan_cache {where_sql}", tuple(where_params))
        total_rows = int((cur.fetchone() or [0])[0] or 0)
        con.close()
    except Exception:
        logging.debug("Failed to count files_album_scan_cache rows before quality recalc", exc_info=True)
    if limit_int > 0:
        total_rows = min(total_rows, limit_int) if total_rows > 0 else limit_int

    with lock:
        state["files_cache_quality_recalc_running"] = True
        state["files_cache_quality_recalc_total"] = int(total_rows)
        state["files_cache_quality_recalc_done"] = 0
        state["files_cache_quality_recalc_rows_upserted"] = 0
        state["files_cache_quality_recalc_errors"] = 0
        state["files_cache_quality_recalc_missing_folders"] = 0
        state["files_cache_quality_recalc_no_audio"] = 0
        state["files_cache_quality_recalc_started_at"] = started_at
        state["files_cache_quality_recalc_updated_at"] = started_at
        state["files_cache_quality_recalc_finished_at"] = None
        state["files_cache_quality_recalc_reason"] = reason

    select_sql = f"""
        SELECT
            rowid,
            folder_path,
            COALESCE(source_id, 0) AS source_id,
            fingerprint,
            artist_name,
            album_title,
            has_cover,
            has_artist_image,
            has_complete_tags,
            has_mbid,
            has_identity,
            identity_provider,
            strict_match_verified,
            strict_match_provider,
            strict_reject_reason,
            strict_tracklist_score,
            musicbrainz_id,
            discogs_release_id,
            lastfm_album_mbid,
            bandcamp_album_url,
            metadata_source,
            missing_required_tags,
            last_scan_id
        FROM files_album_scan_cache
        {where_sql}
          {"AND" if where_sql else "WHERE"} rowid > ?
        ORDER BY rowid
        LIMIT ?
    """

    done = 0
    rows_upserted = 0
    errors = 0
    missing_folders = 0
    no_audio = 0
    processed_rows = 0
    last_rowid = 0
    stop_requested = False

    while True:
        if stop_requested:
            break
        query_params = list(where_params)
        query_params.append(last_rowid)
        query_params.append(batch_size)
        try:
            con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
            cur = con.cursor()
            cur.execute(select_sql, tuple(query_params))
            chunk = cur.fetchall()
            con.close()
        except Exception:
            logging.exception("Failed to read files_album_scan_cache batch for quality recalc")
            errors += batch_size
            break

        if not chunk:
            break

        out_rows: list[dict] = []
        for row in chunk:
            if scan_should_stop.is_set():
                stop_requested = True
                break
            if not _scan_wait_if_paused():
                stop_requested = True
                break
            try:
                rowid = int(row[0] or 0)
            except Exception:
                rowid = 0
            if rowid > last_rowid:
                last_rowid = rowid

            processed_rows += 1
            if limit_int > 0 and processed_rows > limit_int:
                stop_requested = True
                break

            folder_key = str(row[1] or "").strip()
            if not folder_key:
                errors += 1
                done += 1
                continue

            source_id_cached = int(row[2] or 0)
            fingerprint_cached = str(row[3] or "").strip()
            artist_cached = str(row[4] or "").strip()
            album_cached = str(row[5] or "").strip()
            has_cover_cached = bool(row[6])
            has_artist_image_cached = bool(row[7])
            has_complete_tags_cached = bool(row[8])
            has_mbid_cached = bool(row[9])
            has_identity_cached = bool(row[10])
            identity_provider_cached = _normalize_identity_provider(str(row[11] or ""))
            strict_verified_cached = bool(row[12])
            strict_provider_cached = _normalize_identity_provider(str(row[13] or ""))
            strict_reason_cached = str(row[14] or "").strip()
            try:
                strict_score_cached = float(row[15] or 0.0)
            except Exception:
                strict_score_cached = 0.0
            musicbrainz_id_cached = str(row[16] or "").strip()
            discogs_cached = str(row[17] or "").strip()
            lastfm_cached = str(row[18] or "").strip()
            bandcamp_cached = str(row[19] or "").strip()
            metadata_source_cached = _normalize_identity_provider(str(row[20] or ""))
            missing_required_raw = row[21]
            last_scan_id_cached = row[22]

            try:
                missing_required_cached = json.loads(missing_required_raw or "[]")
                if not isinstance(missing_required_cached, list):
                    missing_required_cached = []
            except Exception:
                missing_required_cached = []

            cached_identity = {
                "has_mbid": has_mbid_cached,
                "musicbrainz_id": musicbrainz_id_cached,
                "has_identity": has_identity_cached,
                "identity_provider": identity_provider_cached,
                "strict_match_verified": strict_verified_cached,
                "strict_match_provider": strict_provider_cached,
                "strict_reject_reason": strict_reason_cached,
                "strict_tracklist_score": strict_score_cached,
                "discogs_release_id": discogs_cached,
                "lastfm_album_mbid": lastfm_cached,
                "bandcamp_album_url": bandcamp_cached,
                "metadata_source": metadata_source_cached,
            }

            try:
                folder_path = path_for_fs_access(Path(folder_key))
            except Exception:
                folder_path = Path(folder_key)

            if not folder_path.exists() or not folder_path.is_dir():
                missing_folders += 1
                done += 1
                continue

            ordered_paths = _files_collect_ordered_audio_paths(folder_path, None)
            tags: dict = {}
            missing_required = list(missing_required_cached)
            fingerprint = fingerprint_cached
            if not ordered_paths:
                no_audio += 1
            else:
                try:
                    tags = extract_tags(ordered_paths[0]) or {}
                except Exception:
                    tags = {}
                derived_tracks = [
                    {"title": p.stem or f"Track {i + 1}", "idx": i + 1}
                    for i, p in enumerate(ordered_paths)
                ]
                try:
                    missing_required = _check_required_tags(tags, REQUIRED_TAGS, edition={"tracks": derived_tracks})
                except Exception:
                    missing_required = []
                try:
                    computed_fp = _compute_album_fingerprint(ordered_paths)
                except Exception:
                    computed_fp = ""
                if computed_fp:
                    fingerprint = computed_fp

            has_cover_now = album_folder_has_cover(folder_path)
            has_artist_image_now = _artist_folder_has_image(folder_path.parent if folder_path.parent else folder_path)
            identity_fields = _extract_files_identity_fields(tags=tags, edition={}, cached=cached_identity)
            source_id_now = source_id_cached if source_id_cached > 0 else int(_source_id_for_path(folder_path) or 0)
            artist_name_now = (
                str(tags.get("albumartist") or tags.get("artist") or artist_cached or folder_path.parent.name.replace("_", " ")).strip()
                or "Unknown Artist"
            )
            album_title_now = (
                str(tags.get("album") or album_cached or folder_path.name.replace("_", " ")).strip()
                or "Unknown Album"
            )

            out_rows.append(
                {
                    "folder_path": folder_key,
                    "source_id": source_id_now if source_id_now > 0 else None,
                    "fingerprint": fingerprint,
                    "ordered_paths": [str(p) for p in ordered_paths if str(p or "").strip()],
                    "artist_name": artist_name_now,
                    "album_title": _sanitize_album_title_display(album_title_now),
                    "has_cover": has_cover_now,
                    "has_artist_image": has_artist_image_now,
                    "has_complete_tags": len(missing_required) == 0 if ordered_paths else bool(has_complete_tags_cached and not missing_required),
                    "has_mbid": bool(identity_fields["has_mbid"]),
                    "has_identity": bool(identity_fields["has_identity"]),
                    "identity_provider": identity_fields["identity_provider"],
                    "strict_match_verified": bool(identity_fields.get("strict_match_verified")),
                    "strict_match_provider": identity_fields.get("strict_match_provider") or "",
                    "strict_reject_reason": identity_fields.get("strict_reject_reason") or "",
                    "strict_tracklist_score": float(identity_fields.get("strict_tracklist_score") or 0.0),
                    "musicbrainz_id": identity_fields["musicbrainz_id"],
                    "discogs_release_id": identity_fields["discogs_release_id"],
                    "lastfm_album_mbid": identity_fields["lastfm_album_mbid"],
                    "bandcamp_album_url": identity_fields["bandcamp_album_url"],
                    "metadata_source": identity_fields["metadata_source"],
                    "missing_required_tags": missing_required,
                    "last_scan_id": last_scan_id_cached,
                    "updated_at": time.time(),
                }
            )
            done += 1

        if out_rows:
            try:
                _upsert_files_album_scan_cache_rows(out_rows)
                rows_upserted += len(out_rows)
            except Exception:
                logging.debug("Failed upsert during files cache quality recalc", exc_info=True)
                errors += len(out_rows)

        with lock:
            state["files_cache_quality_recalc_done"] = int(done)
            state["files_cache_quality_recalc_rows_upserted"] = int(rows_upserted)
            state["files_cache_quality_recalc_errors"] = int(errors)
            state["files_cache_quality_recalc_missing_folders"] = int(missing_folders)
            state["files_cache_quality_recalc_no_audio"] = int(no_audio)
            state["files_cache_quality_recalc_updated_at"] = time.time()

    finished_at = time.time()
    elapsed = max(0.0, finished_at - started_at)
    with lock:
        state["files_cache_quality_recalc_running"] = False
        state["files_cache_quality_recalc_done"] = int(done)
        state["files_cache_quality_recalc_rows_upserted"] = int(rows_upserted)
        state["files_cache_quality_recalc_errors"] = int(errors)
        state["files_cache_quality_recalc_missing_folders"] = int(missing_folders)
        state["files_cache_quality_recalc_no_audio"] = int(no_audio)
        state["files_cache_quality_recalc_updated_at"] = finished_at
        state["files_cache_quality_recalc_finished_at"] = finished_at

    result = {
        "ok": not stop_requested,
        "reason": reason,
        "duration_sec": round(elapsed, 2),
        "total": int(total_rows),
        "done": int(done),
        "rows_upserted": int(rows_upserted),
        "errors": int(errors),
        "missing_folders": int(missing_folders),
        "no_audio": int(no_audio),
        "stopped": bool(stop_requested),
    }
    logging.info(
        "FILES cache quality recalc (%s): done=%d/%d upserted=%d errors=%d missing_folders=%d no_audio=%d in %.2fs",
        reason,
        result["done"],
        result["total"],
        result["rows_upserted"],
        result["errors"],
        result["missing_folders"],
        result["no_audio"],
        elapsed,
    )
    return result


def _start_files_cache_quality_recalc_async(
    *,
    batch_size: int = 500,
    source_id: int | None = None,
    limit: int | None = None,
    reason: str = "manual",
) -> tuple[bool, dict[str, Any]]:
    with lock:
        if bool(state.get("files_cache_quality_recalc_running")):
            return False, _files_cache_quality_recalc_status_unlocked()
        state["files_cache_quality_recalc_running"] = True
        state["files_cache_quality_recalc_total"] = 0
        state["files_cache_quality_recalc_done"] = 0
        state["files_cache_quality_recalc_rows_upserted"] = 0
        state["files_cache_quality_recalc_errors"] = 0
        state["files_cache_quality_recalc_missing_folders"] = 0
        state["files_cache_quality_recalc_no_audio"] = 0
        state["files_cache_quality_recalc_started_at"] = time.time()
        state["files_cache_quality_recalc_updated_at"] = state["files_cache_quality_recalc_started_at"]
        state["files_cache_quality_recalc_finished_at"] = None
        state["files_cache_quality_recalc_reason"] = reason

    def _runner() -> None:
        try:
            _recalculate_files_album_scan_cache_quality(
                batch_size=batch_size,
                source_id=source_id,
                limit=limit,
                reason=reason,
            )
        except Exception:
            logging.exception("FILES cache quality recalc (%s) failed", reason)
            with lock:
                state["files_cache_quality_recalc_running"] = False
                state["files_cache_quality_recalc_errors"] = int(state.get("files_cache_quality_recalc_errors") or 0) + 1
                state["files_cache_quality_recalc_updated_at"] = time.time()
                state["files_cache_quality_recalc_finished_at"] = time.time()

    threading.Thread(
        target=_runner,
        name=f"files-cache-quality-recalc-{reason}",
        daemon=True,
    ).start()
    with lock:
        return True, _files_cache_quality_recalc_status_unlocked()
