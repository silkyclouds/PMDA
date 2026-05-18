"""Broken album diagnostics runtime extracted from the PMDA bootstrap module."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from flask import jsonify, request


_RUNTIME: Any | None = None

_EXTRACTED_NAMES = {
    "detect_broken_album",
    "_incomplete_album_disk_crosscheck",
    "_broken_album_delete_rows",
    "_broken_album_resolve_folder_snapshot",
    "_broken_album_backfill_candidates",
    "_run_broken_album_backfill",
    "_trigger_broken_album_backfill_async",
}


def _bind_runtime(runtime: Any) -> None:
    """Expose PMDA runtime globals to the extracted broken-album handlers."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "api_broken_albums",
        "api_broken_albums_for_runtime",
        "api_broken_album_detail",
        "api_broken_album_detail_for_runtime",
        "_refresh_broken_album_row",
        "refresh_broken_album_row_for_runtime",
        "_bind_runtime",
    }
    for key, value in vars(runtime).items():
        if key in _EXTRACTED_NAMES:
            if getattr(value, "__module__", "") != getattr(runtime, "__name__", ""):
                globals()[key] = value
            else:
                original = _ORIGINAL_EXTRACTED_FUNCTIONS.get(key)
                if original is not None:
                    globals()[key] = original
            continue
        own_wrapper = key.endswith("_for_runtime") and key[: -len("_for_runtime")] in _EXTRACTED_NAMES
        if key in blocked or own_wrapper:
            continue
        globals()[key] = value


def api_broken_albums_for_runtime(runtime: Any):
    _bind_runtime(runtime)
    return api_broken_albums()


def api_broken_album_detail_for_runtime(runtime: Any):
    _bind_runtime(runtime)
    return api_broken_album_detail()


def refresh_broken_album_row_for_runtime(runtime: Any, *args: Any, **kwargs: Any):
    _bind_runtime(runtime)
    return _refresh_broken_album_row(*args, **kwargs)


def detect_broken_album_for_runtime(runtime: Any, *args: Any, **kwargs: Any):
    _bind_runtime(runtime)
    return detect_broken_album(*args, **kwargs)


def _incomplete_album_disk_crosscheck_for_runtime(runtime: Any, *args: Any, **kwargs: Any):
    _bind_runtime(runtime)
    return _incomplete_album_disk_crosscheck(*args, **kwargs)


def detect_broken_album(
    db_conn,
    album_id: int,
    tracks: list[Any],
    mb_release_group_info: dict | None,
    tags: dict | None = None,
    folder_path: Path | str | None = None,
    album_title: str | None = None,
) -> tuple[bool, int | None, int, list]:
    """
    Detect whether a local album folder is obviously incomplete.
    Returns (is_broken, expected_track_count, actual_track_count, missing_indices).

    Conservative policy:
    - broken when no readable tracks exist
    - broken when local numbering has obvious internal holes
    - do not mark incomplete from provider-only shortfalls, tag totals, or trailing deficits
    """
    actual_count = len(tracks)
    if actual_count == 0:
        return True, 0, 0, []

    grouped_indices: dict[int, set[int]] = defaultdict(set)
    for track in tracks:
        try:
            disc_num = max(1, int(getattr(track, "disc", 1) or 1))
        except Exception:
            disc_num = 1
        try:
            track_num = int(getattr(track, "idx", 0) or 0)
        except Exception:
            track_num = 0
        if track_num > 0:
            grouped_indices[disc_num].add(track_num)

    if len(grouped_indices) != 1:
        return False, None, actual_count, []

    track_indices = sorted(next(iter(grouped_indices.values())))
    if len(track_indices) < 2:
        return False, None, actual_count, []

    missing_indices: list[int] = []
    for i in range(len(track_indices) - 1):
        prev_idx = int(track_indices[i] or 0)
        next_idx = int(track_indices[i + 1] or 0)
        if (next_idx - prev_idx) <= 1:
            continue
        missing_indices.extend(list(range(prev_idx + 1, next_idx)))
        if len(missing_indices) > 5000:
            missing_indices = missing_indices[:5000]
            break

    if missing_indices:
        expected_total = int(actual_count + len(missing_indices))
        return True, expected_total, actual_count, missing_indices

    return False, None, actual_count, []


def _incomplete_album_disk_crosscheck(
    db_conn,
    artist: str,
    album_id: int,
    tracks: list[Any],
    folder: Path,
    album_title_str: str,
) -> dict:
    """
    Cross-check a broken album: expected index rows vs disk files. The
    `missing_in_plex` key is retained as a legacy database column name; new API
    payloads also expose the same values as `missing_from_index`.
    """
    classifications: list[str] = []
    missing_from_index: list[int] = []  # Expected track indices with no file on disk.
    missing_on_disk: list[str] = []     # Disk filenames that do not match expected tracks.
    track_titles = [(t.idx, t.title) for t in tracks]
    expected_indices = {t.idx for t in tracks}

    if not folder.exists():
        classifications.append("DISK_MISSING")
        return {
            "classification": ",".join(classifications),
            "missing_in_plex": json.dumps(list(expected_indices)),
            "missing_on_disk": json.dumps([]),
            "track_titles": json.dumps(track_titles),
            "expected_track_count": len(tracks),
            "actual_track_count": 0,
        }

    audio_files = [p for p in folder.rglob("*") if AUDIO_RE.search(p.name)]
    disk_by_index: dict[int, Path] = {}
    disk_extra: list[str] = []
    tag_album: str | None = None
    tag_artist: str | None = None

    for p in audio_files:
        tags = extract_tags(p)
        idx = None
        if "tracknumber" in tags:
            raw = tags["tracknumber"].strip().split("/")[0].strip()
            try:
                idx = int(raw)
            except ValueError:
                pass
        if idx is not None and idx in expected_indices:
            disk_by_index[idx] = p
        else:
            disk_extra.append(p.name)
        if tag_album is None and "album" in tags:
            tag_album = (tags["album"] or "").strip()
        if tag_artist is None and "artist" in tags:
            tag_artist = (tags["artist"] or "").strip()

    for idx in expected_indices:
        if idx not in disk_by_index:
            missing_from_index.append(idx)
    if missing_from_index:
        classifications.append("DISK_MISSING")
    if disk_extra:
        classifications.append("DISK_HAS_MORE")
    if (tag_album or tag_artist) and album_title_str:
        album_norm = _normalize_identity_text_strict(album_title_str or "")
        artist_norm = _norm_artist_key(artist or "")
        tag_album_norm = _normalize_identity_text_strict(tag_album or "")
        tag_artist_norm = _norm_artist_key(tag_artist or "")
        if tag_album_norm and tag_album_norm != album_norm:
            classifications.append("DISK_HAS_TAG_SPLIT")
        elif tag_artist_norm and tag_artist_norm != artist_norm:
            classifications.append("DISK_HAS_TAG_SPLIT")

    if not classifications:
        classifications.append("DISK_MISSING")

    return {
        "classification": ",".join(classifications),
        "missing_in_plex": json.dumps(missing_from_index),
        "missing_on_disk": json.dumps(missing_on_disk[:50]),
        "track_titles": json.dumps(track_titles),
        "expected_track_count": len(tracks),
        "actual_track_count": len(audio_files),
    }


def _broken_album_delete_rows(pairs: list[tuple[str, int]] | set[tuple[str, int]]) -> int:
    targets = [
        (str(artist or "").strip(), int(album_id or 0))
        for artist, album_id in list(pairs or [])
        if str(artist or "").strip() and int(album_id or 0) > 0
    ]
    if not targets:
        return 0
    con = _state_connect(timeout=10)
    try:
        cur = con.cursor()
        cur.executemany(
            "DELETE FROM broken_albums WHERE artist = ? AND album_id = ?",
            targets,
        )
        con.commit()
        return int(cur.rowcount or 0)
    finally:
        con.close()


def _broken_album_resolve_folder_snapshot(
    *,
    folder_path: str | None,
    artist_name: str,
    album_title: str,
    metadata_source: str,
    detail_map: dict[str, Any] | None = None,
    existing_local_tracks: list[dict[str, Any]] | None = None,
    force_rescan: bool = True,
) -> tuple[Path | None, list[dict[str, Any]], bool]:
    folder_obj: Path | None = None
    folder_verified = False
    local_tracks = list(existing_local_tracks or [])
    folder_raw = str(folder_path or "").strip()
    if folder_raw:
        try:
            candidate = path_for_fs_access(Path(folder_raw))
            if candidate.exists() and candidate.is_dir():
                folder_obj = candidate
                folder_verified = True
        except Exception:
            folder_obj = None
            folder_verified = False
    if folder_obj and (force_rescan or not local_tracks):
        try:
            rescanned = _scan_move_track_entries_from_folder(
                folder_obj,
                artist_name=artist_name,
                album_title=album_title,
                metadata_source=str(metadata_source or ""),
                details=dict(detail_map or {}),
                quick=True,
            )
            if isinstance(rescanned, list):
                local_tracks = rescanned
        except Exception:
            logging.debug(
                "Broken album local snapshot refresh failed artist=%s folder=%s",
                artist_name,
                folder_raw,
                exc_info=True,
            )
    elif local_tracks:
        folder_verified = True
    return folder_obj, local_tracks, folder_verified


def _broken_album_backfill_candidates(*, limit: int = 0, full_refresh: bool = False) -> list[tuple[str, int]]:
    con = _state_connect(timeout=30)
    try:
        cur = con.cursor()
        where_parts = ["fixed_at IS NULL"]
        params: list[Any] = []
        if not full_refresh:
            where_parts.append(
                "("
                "COALESCE(classification, '') = '' OR "
                "COALESCE(reason_summary, '') = '' OR "
                "COALESCE(evidence_json, '{}') = '{}' OR "
                "COALESCE(local_tracks_json, '[]') = '[]' OR "
                "COALESCE(expected_tracks_json, '[]') = '[]'"
                ")"
            )
        sql = (
            "SELECT artist, album_id FROM broken_albums "
            f"WHERE {' AND '.join(where_parts)} "
            "ORDER BY COALESCE(detected_at, 0) DESC, artist ASC, album_id DESC"
        )
        if int(limit or 0) > 0:
            sql += " LIMIT ?"
            params.append(int(limit))
        cur.execute(sql, params)
        return [
            (str(row[0] or "").strip(), _parse_int_loose(row[1], 0))
            for row in cur.fetchall()
            if str(row[0] or "").strip() and _parse_int_loose(row[1], 0) > 0
        ]
    finally:
        con.close()


def _run_broken_album_backfill(*, reason: str = "manual", include_ai: bool = False, limit: int = 0, full_refresh: bool = False) -> None:
    candidates = _broken_album_backfill_candidates(limit=limit, full_refresh=full_refresh)
    with _broken_album_backfill_lock:
        _broken_album_backfill_state.update({
            "running": True,
            "reason": str(reason or "manual"),
            "started_at": int(time.time()),
            "finished_at": 0,
            "current": 0,
            "total": int(len(candidates)),
            "current_artist": "",
            "current_album_id": 0,
            "changed": 0,
            "errors": 0,
            "last_error": "",
            "include_ai": bool(include_ai),
            "full_refresh": bool(full_refresh),
        })
    changed = 0
    errors = 0
    try:
        for idx, (artist, album_id) in enumerate(candidates, start=1):
            with _broken_album_backfill_lock:
                if not bool(_broken_album_backfill_state.get("running")):
                    break
                _broken_album_backfill_state["current"] = int(idx)
                _broken_album_backfill_state["current_artist"] = artist
                _broken_album_backfill_state["current_album_id"] = int(album_id)
            try:
                payload = _refresh_broken_album_row(
                    artist,
                    int(album_id),
                    refresh_ai=bool(include_ai),
                    allow_provider_lookup=False,
                )
                if payload:
                    changed += 1
            except Exception as exc:
                errors += 1
                logging.warning("Broken album backfill failed artist=%s album_id=%s: %s", artist, album_id, exc)
                with _broken_album_backfill_lock:
                    _broken_album_backfill_state["errors"] = int(_broken_album_backfill_state.get("errors") or 0) + 1
                    _broken_album_backfill_state["last_error"] = str(exc)
    finally:
        with _broken_album_backfill_lock:
            _broken_album_backfill_state["running"] = False
            _broken_album_backfill_state["finished_at"] = int(time.time())
            _broken_album_backfill_state["current_artist"] = ""
            _broken_album_backfill_state["current_album_id"] = 0
            _broken_album_backfill_state["changed"] = int(changed)
            _broken_album_backfill_state["errors"] = int(errors)


def _trigger_broken_album_backfill_async(*, reason: str = "manual", include_ai: bool = False, limit: int = 0, full_refresh: bool = False) -> bool:
    with _broken_album_backfill_lock:
        if bool(_broken_album_backfill_state.get("running")):
            return False
        _broken_album_backfill_state["running"] = True
    threading.Thread(
        target=_run_broken_album_backfill,
        kwargs={
            "reason": str(reason or "manual"),
            "include_ai": bool(include_ai),
            "limit": int(limit or 0),
            "full_refresh": bool(full_refresh),
        },
        daemon=True,
        name="broken-album-backfill",
    ).start()
    return True


def api_broken_albums():
    """Return broken albums from PMDA's files-mode diagnostics table."""
    _reload_section_ids_from_db()
    import json
    refresh_local_snapshots = bool(_parse_bool(request.args.get("refresh")))
    con = _state_connect(timeout=30)
    cur = con.cursor()
    cur.execute("""
        SELECT artist, album_id, expected_track_count, actual_track_count,
               missing_indices, musicbrainz_release_group_id, detected_at, sent_to_lidarr, COALESCE(review_status, ''),
               COALESCE(album_title, ''), COALESCE(folder_path, ''), COALESCE(metadata_source, ''),
               COALESCE(strict_match_provider, ''), COALESCE(strict_reject_reason, ''), COALESCE(reason_summary, ''),
               COALESCE(classification, ''), COALESCE(classification_confidence, 0.0),
               COALESCE(classification_source, ''), COALESCE(quarantine_eligible, 0),
               COALESCE(evidence_json, '{}'), COALESCE(ai_verdict_json, '{}'),
               COALESCE(local_tracks_json, '[]'), COALESCE(expected_tracks_json, '[]'),
               COALESCE(missing_required_tags_json, '[]')
        FROM broken_albums
        ORDER BY detected_at DESC
    """)
    rows = cur.fetchall()
    if _get_library_mode() == "files":
        album_ids = [int(row[1]) for row in rows if _parse_int_loose(row[1], 0) > 0]

        def _load_latest_album_meta(
            table: str,
            title_col: str,
            folder_col: str,
            provider_col: str,
            reject_col: str,
            strict_provider_col: str | None = None,
            pipeline_col: str | None = None,
        ) -> dict[int, dict]:
            meta: dict[int, dict] = {}
            if not album_ids:
                return meta
            chunk_size = 400
            for start in range(0, len(album_ids), chunk_size):
                chunk = album_ids[start:start + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                strict_provider_sql = f"COALESCE({strict_provider_col}, '')" if strict_provider_col else "''"
                pipeline_sql = f"COALESCE({pipeline_col}, '')" if pipeline_col else "''"
                cur.execute(
                    f"""
                    SELECT src.album_id,
                           COALESCE(src.{title_col}, ''),
                           COALESCE(src.{folder_col}, ''),
                           COALESCE(src.{provider_col}, ''),
                           COALESCE(src.{reject_col}, ''),
                           {strict_provider_sql},
                           {pipeline_sql}
                    FROM {table} src
                    JOIN (
                        SELECT album_id, MAX(rowid) AS max_rowid
                        FROM {table}
                        WHERE album_id IN ({placeholders})
                        GROUP BY album_id
                    ) latest ON latest.max_rowid = src.rowid
                    """,
                    chunk,
                )
                for album_id, title, folder, provider, reject_reason, strict_provider, pipeline_status in cur.fetchall():
                    meta[int(album_id)] = {
                        "album_title": str(title or "").strip(),
                        "folder": str(folder or "").strip(),
                        "provider": str(provider or "").strip(),
                        "strict_reject_reason": str(reject_reason or "").strip(),
                        "strict_match_provider": str(strict_provider or "").strip(),
                        "pipeline_status": str(pipeline_status or "").strip(),
                    }
            return meta

        trace_meta = _load_latest_album_meta(
            "scan_pipeline_trace",
            title_col="album_title",
            folder_col="folder",
            provider_col="decision_provider",
            reject_col="decision_reason",
            strict_provider_col="strict_match_provider",
            pipeline_col="pipeline_status",
        )
        edition_meta = _load_latest_album_meta(
            "scan_editions",
            title_col="title_raw",
            folder_col="folder",
            provider_col="metadata_source",
            reject_col="strict_reject_reason",
            strict_provider_col="strict_match_provider",
        )
        con.close()

        pg_meta: dict[int, dict] = {}
        base_url = request.url_root.rstrip("/")
        pg_conn = _files_pg_connect(acquire_timeout_sec=0.75)
        if pg_conn is not None and album_ids:
            try:
                with pg_conn.cursor() as pg_cur:
                    pg_cur.execute(
                        """
                        SELECT
                            alb.id,
                            COALESCE(alb.title, '') AS title,
                            COALESCE(alb.folder_path, '') AS folder_path,
                            alb.has_cover,
                            COALESCE(alb.cover_path, '') AS cover_path,
                            COALESCE(alb.metadata_source, '') AS metadata_source,
                            COALESCE(alb.strict_match_provider, '') AS strict_match_provider,
                            COALESCE(alb.strict_reject_reason, '') AS strict_reject_reason
                        FROM files_albums alb
                        WHERE alb.id = ANY(%s)
                        """,
                        (album_ids,),
                    )
                    for album_id, title, folder_path, has_cover, cover_path_raw, metadata_source, strict_match_provider, strict_reject_reason in pg_cur.fetchall():
                        has_cover_effective, _effective_cover_path = _resolve_files_album_cover_asset(
                            album_id=int(album_id or 0),
                            cover_path_raw=str(cover_path_raw or "").strip(),
                            folder_path_raw=str(folder_path or "").strip(),
                            has_cover=bool(has_cover),
                            persist=True,
                        )
                        pg_meta[int(album_id)] = {
                            "album_title": str(title or "").strip(),
                            "folder": str(folder_path or "").strip(),
                            "thumb_url": f"{base_url}/api/library/files/album/{int(album_id)}/cover?size=320" if has_cover_effective else None,
                            "metadata_source": _normalize_identity_provider(str(metadata_source or "")) or None,
                            "strict_match_provider": _normalize_identity_provider(str(strict_match_provider or "")) or None,
                            "strict_reject_reason": str(strict_reject_reason or "").strip() or None,
                        }
            finally:
                try:
                    pg_conn.close()
                except Exception:
                    pass

        broken_albums = []
        stale_targets: set[tuple[str, int]] = set()
        for row in rows:
            artist = str(row[0] or "").strip()
            album_id = int(row[1] or 0)
            missing_indices = json.loads(row[4]) if row[4] else []
            trace = trace_meta.get(album_id) or {}
            edition = edition_meta.get(album_id) or {}
            published = pg_meta.get(album_id) or {}
            stored_title = str(row[9] or "").strip()
            stored_folder = str(row[10] or "").strip()
            stored_metadata_source = _normalize_identity_provider(str(row[11] or "")) or None
            stored_strict_provider = _normalize_identity_provider(str(row[12] or "")) or None
            stored_strict_reason = str(row[13] or "").strip() or None
            classification = str(row[15] or "").strip() or None
            classification_confidence = float(row[16] or 0.0)
            classification_source = str(row[17] or "").strip() or None
            quarantine_eligible = bool(int(row[18] or 0))
            try:
                evidence_json = json.loads(str(row[19] or "{}"))
                if not isinstance(evidence_json, dict):
                    evidence_json = {}
            except Exception:
                evidence_json = {}
            try:
                ai_verdict_json = json.loads(str(row[20] or "{}"))
                if not isinstance(ai_verdict_json, dict):
                    ai_verdict_json = {}
            except Exception:
                ai_verdict_json = {}
            try:
                local_tracks_json = json.loads(str(row[21] or "[]"))
                if not isinstance(local_tracks_json, list):
                    local_tracks_json = []
            except Exception:
                local_tracks_json = []
            try:
                expected_tracks_json = json.loads(str(row[22] or "[]"))
                if not isinstance(expected_tracks_json, list):
                    expected_tracks_json = []
            except Exception:
                expected_tracks_json = []
            try:
                missing_required_tags_json = json.loads(str(row[23] or "[]"))
                if not isinstance(missing_required_tags_json, list):
                    missing_required_tags_json = []
            except Exception:
                missing_required_tags_json = []
            anchor_title, anchor_folder = _broken_album_snapshot_anchor(
                stored_title,
                stored_folder,
                trace_title=trace.get("album_title"),
                trace_folder=trace.get("folder"),
                edition_title=edition.get("album_title"),
                edition_folder=edition.get("folder"),
            )
            if trace and not _broken_album_meta_matches_snapshot(
                anchor_title,
                anchor_folder,
                trace.get("album_title"),
                trace.get("folder"),
            ):
                trace = {}
            if edition and not _broken_album_meta_matches_snapshot(
                anchor_title,
                anchor_folder,
                edition.get("album_title"),
                edition.get("folder"),
            ):
                edition = {}
            if published and not _broken_album_meta_matches_snapshot(
                anchor_title,
                anchor_folder,
                published.get("album_title"),
                published.get("folder"),
            ):
                published = {}
            folder_path = str(
                anchor_folder
                or published.get("folder")
                or trace.get("folder")
                or edition.get("folder")
                or ""
            ).strip()
            title = (
                anchor_title
                or str(published.get("album_title") or "").strip()
                or str(trace.get("album_title") or "").strip()
                or str(edition.get("album_title") or "").strip()
                or (Path(folder_path).name.strip() if folder_path else "")
                or f"Album {album_id}"
            )
            detail_map = {
                "expected_track_count": row[2],
                "actual_track_count": row[3],
                "missing_indices": missing_indices,
                "strict_match_provider": (
                    stored_strict_provider
                    or published.get("strict_match_provider")
                    or _normalize_identity_provider(str(trace.get("strict_match_provider") or edition.get("strict_match_provider") or ""))
                    or None
                ),
                "strict_reject_reason": (
                    stored_strict_reason
                    or published.get("strict_reject_reason")
                    or str(edition.get("strict_reject_reason") or "").strip()
                    or ""
                ),
                "musicbrainz_release_group_id": str(row[5] or "").strip(),
                "missing_required_tags": list(missing_required_tags_json or []),
            }
            _, local_tracks_json, local_tracks_verified = _broken_album_resolve_folder_snapshot(
                folder_path=folder_path,
                artist_name=artist,
                album_title=title,
                metadata_source=str(
                    stored_metadata_source
                    or published.get("metadata_source")
                    or _normalize_identity_provider(str(edition.get("provider") or trace.get("provider") or ""))
                    or ""
                ),
                detail_map=detail_map,
                existing_local_tracks=local_tracks_json,
                force_rescan=refresh_local_snapshots,
            )
            payload_for_assessment = {
                "album_id": album_id,
                "album_title": title,
                "strict_reject_reason": (
                    stored_strict_reason
                    or published.get("strict_reject_reason")
                    or str(edition.get("strict_reject_reason") or "").strip()
                    or ""
                ),
                "expected_track_count": row[2],
                "actual_track_count": row[3],
                "missing_indices": missing_indices,
                "missing_required_tags": missing_required_tags_json,
                "local_tracks": local_tracks_json,
                "expected_tracks": expected_tracks_json,
                "_local_tracks_verified": bool(local_tracks_verified),
                "metadata_source": (
                    stored_metadata_source
                    or published.get("metadata_source")
                    or _normalize_identity_provider(str(edition.get("provider") or trace.get("provider") or ""))
                    or None
                ),
                "strict_match_provider": (
                    stored_strict_provider
                    or published.get("strict_match_provider")
                    or _normalize_identity_provider(str(trace.get("strict_match_provider") or edition.get("strict_match_provider") or ""))
                    or None
                ),
            }
            assessment = _build_incomplete_assessment_from_payload(payload_for_assessment)
            if not bool(assessment.get("mark_broken")):
                stale_targets.add((artist, int(album_id)))
                continue
            broken_albums.append({
                "artist": artist,
                "album_id": album_id,
                "album_title": title,
                "expected_track_count": int(payload_for_assessment.get("expected_track_count") or 0),
                "actual_track_count": int(payload_for_assessment.get("actual_track_count") or 0),
                "missing_indices": list(payload_for_assessment.get("missing_indices") or []),
                "musicbrainz_release_group_id": row[5],
                "detected_at": row[6],
                "sent_to_lidarr": bool(row[7]) if row[7] is not None else False,
                "sent_to_external_recovery": bool(row[7]) if row[7] is not None else False,
                "review_status": str(row[8] or "").strip() or None,
                "folder_path": folder_path or None,
                "thumb_url": published.get("thumb_url"),
                "metadata_source": (
                    stored_metadata_source
                    or published.get("metadata_source")
                    or _normalize_identity_provider(str(edition.get("provider") or trace.get("provider") or ""))
                    or None
                ),
                "strict_match_provider": (
                    stored_strict_provider
                    or published.get("strict_match_provider")
                    or _normalize_identity_provider(str(trace.get("strict_match_provider") or edition.get("strict_match_provider") or ""))
                    or None
                ),
                "strict_reject_reason": (
                    stored_strict_reason
                    or published.get("strict_reject_reason")
                    or str(edition.get("strict_reject_reason") or "").strip()
                    or None
                ),
                "classification": str(assessment.get("verdict") or classification or "").strip() or None,
                "classification_confidence": float(assessment.get("confidence") or classification_confidence or 0.0),
                "classification_source": str(assessment.get("source") or classification_source or "").strip() or None,
                "quarantine_eligible": bool(assessment.get("quarantine_eligible")) if assessment else quarantine_eligible,
                "evidence": evidence_json,
                "ai_verdict": ai_verdict_json,
                "reason_summary": str(assessment.get("summary") or row[14] or "").strip() or None,
                "pipeline_status": str(trace.get("pipeline_status") or "incomplete").strip() or "incomplete",
                "recoverable": bool(str(row[5] or "").strip()),
            })
        if stale_targets:
            try:
                _broken_album_delete_rows(stale_targets)
            except Exception:
                logging.debug("Failed to purge stale broken album rows", exc_info=True)
        return jsonify(broken_albums)

    con.close()

    # PMDA no longer supports Plex-source broken album browsing. In files mode
    # the function returns above; this non-files fallback stays non-destructive
    # and intentionally avoids opening any external player database.
    return jsonify([])


def _refresh_broken_album_row(
    artist: str,
    album_id: int,
    *,
    refresh_ai: bool = False,
    allow_provider_lookup: bool = False,
) -> dict[str, Any] | None:
    con = _state_connect(timeout=30)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT artist, album_id, expected_track_count, actual_track_count,
                   missing_indices, musicbrainz_release_group_id, detected_at,
                   sent_to_lidarr, COALESCE(review_status, '') AS review_status,
                   COALESCE(album_title, '') AS album_title, COALESCE(folder_path, '') AS folder_path, COALESCE(metadata_source, '') AS metadata_source,
                   COALESCE(strict_match_provider, '') AS strict_match_provider, COALESCE(strict_reject_reason, '') AS strict_reject_reason,
                   COALESCE(provider_refs_json, '{}') AS provider_refs_json, COALESCE(reason_summary, '') AS reason_summary,
                   COALESCE(local_tracks_json, '[]') AS local_tracks_json, COALESCE(expected_tracks_json, '[]') AS expected_tracks_json,
                   COALESCE(missing_required_tags_json, '[]') AS missing_required_tags_json,
                   COALESCE(classification, '') AS classification, COALESCE(classification_confidence, 0.0) AS classification_confidence,
                   COALESCE(classification_source, '') AS classification_source, COALESCE(quarantine_eligible, 0) AS quarantine_eligible,
                   COALESCE(evidence_json, '{}') AS evidence_json, COALESCE(ai_verdict_json, '{}') AS ai_verdict_json
            FROM broken_albums
            WHERE artist = ? AND album_id = ?
            LIMIT 1
            """,
            (artist, int(album_id)),
        )
        row = cur.fetchone()
    finally:
        con.close()
    if not row:
        return None

    def _row_json(key: str, default: Any) -> Any:
        raw_value = row[key]
        if raw_value in (None, ""):
            return default
        try:
            parsed = json.loads(str(raw_value))
        except Exception:
            return default
        return parsed

    try:
        missing_indices = json.loads(str(row["missing_indices"] or "[]"))
        if not isinstance(missing_indices, list):
            missing_indices = []
    except Exception:
        missing_indices = []

    provider_refs = _row_json("provider_refs_json", {})
    if not isinstance(provider_refs, dict):
        provider_refs = {}
    local_tracks = _row_json("local_tracks_json", [])
    if not isinstance(local_tracks, list):
        local_tracks = []
    expected_tracks = _row_json("expected_tracks_json", [])
    if not isinstance(expected_tracks, list):
        expected_tracks = []
    missing_required_tags = _row_json("missing_required_tags_json", [])
    if not isinstance(missing_required_tags, list):
        missing_required_tags = []
    ai_verdict = _row_json("ai_verdict_json", {})
    if not isinstance(ai_verdict, dict):
        ai_verdict = {}

    folder_path = str(row["folder_path"] or "").strip()
    album_title = _broken_album_display_title(
        str(row["album_title"] or "").strip(),
        folder_path,
        int(album_id),
    )
    metadata_source = _normalize_identity_provider(str(row["metadata_source"] or "")) or None
    strict_match_provider = _normalize_identity_provider(str(row["strict_match_provider"] or "")) or None
    strict_reject_reason = str(row["strict_reject_reason"] or "").strip()
    expected_track_count = _parse_int_loose(row["expected_track_count"], 0)
    actual_track_count = _parse_int_loose(row["actual_track_count"], 0)

    detail_map = {
        "expected_track_count": expected_track_count,
        "actual_track_count": actual_track_count,
        "missing_indices": missing_indices,
        "strict_match_provider": strict_match_provider,
        "strict_reject_reason": strict_reject_reason,
        "musicbrainz_release_group_id": str(row["musicbrainz_release_group_id"] or "").strip() or str(provider_refs.get("musicbrainz_release_id") or "").strip(),
        "discogs_release_id": str(provider_refs.get("discogs_release_id") or "").strip(),
        "lastfm_album_mbid": str(provider_refs.get("lastfm_album_mbid") or "").strip(),
        "bandcamp_album_url": str(provider_refs.get("bandcamp_album_url") or "").strip(),
        "missing_required_tags": list(missing_required_tags or []),
    }

    folder_obj, local_tracks, local_tracks_verified = _broken_album_resolve_folder_snapshot(
        folder_path=folder_path,
        artist_name=artist,
        album_title=album_title,
        metadata_source=str(metadata_source or strict_match_provider or ""),
        detail_map=detail_map,
        existing_local_tracks=local_tracks,
        force_rescan=True,
    )
    if not expected_tracks:
        expected_tracks = _scan_move_expected_tracks(
            folder=folder_obj,
            artist_name=artist,
            album_title=album_title,
            metadata_source=str(metadata_source or strict_match_provider or ""),
            details=detail_map,
            cache_only=True,
        )
    if allow_provider_lookup and not expected_tracks and folder_obj:
        logging.debug(
            "Broken album refresh skipped live provider reference lookup artist=%s album_id=%s",
            artist,
            int(album_id),
        )

    payload = {
        "artist": artist,
        "album_id": int(album_id),
        "album_title": album_title,
        "detected_at": float(row["detected_at"] or 0),
        "folder_path": folder_path or None,
        "thumb_url": None,
        "metadata_source": metadata_source,
        "strict_match_provider": strict_match_provider,
        "strict_reject_reason": strict_reject_reason or None,
        "expected_track_count": int(expected_track_count or 0),
        "actual_track_count": int(actual_track_count or len(local_tracks or [])),
        "missing_indices": missing_indices,
        "missing_required_tags": list(missing_required_tags or []),
        "musicbrainz_release_group_id": str(row["musicbrainz_release_group_id"] or "").strip() or None,
        "provider_refs": provider_refs,
        "pipeline_status": "incomplete",
        "timeline": [],
        "meta_summary": {},
        "reason_summary": str(row["reason_summary"] or "").strip(),
        "classification": str(row["classification"] or "").strip() or None,
        "classification_confidence": float(row["classification_confidence"] or 0.0),
        "classification_source": str(row["classification_source"] or "").strip() or None,
        "quarantine_eligible": bool(int(row["quarantine_eligible"] or 0)),
        "evidence": {},
        "ai_verdict": ai_verdict,
        "local_tracks": local_tracks,
        "expected_tracks": expected_tracks,
        "_local_tracks_verified": bool(local_tracks_verified),
        "recoverable": bool(str(row["musicbrainz_release_group_id"] or "").strip()),
        "sent_to_lidarr": bool(row["sent_to_lidarr"]) if row["sent_to_lidarr"] is not None else False,
        "sent_to_external_recovery": bool(row["sent_to_lidarr"]) if row["sent_to_lidarr"] is not None else False,
        "review_status": str(row["review_status"] or "").strip() or "pending",
    }
    assessment = _build_incomplete_assessment_from_payload(payload)
    payload["reason_summary"] = str(payload.get("reason_summary") or assessment.get("summary") or _broken_album_reason_summary(
        expected_track_count=int(payload.get("expected_track_count") or expected_track_count or 0),
        actual_track_count=int(payload.get("actual_track_count") or 0),
        missing_indices=list(payload.get("missing_indices") or missing_indices or []),
        missing_required_tags=list(missing_required_tags or []),
        strict_reject_reason=str(payload.get("strict_reject_reason") or strict_reject_reason or ""),
    ))
    payload["classification"] = str(assessment.get("verdict") or "") or None
    payload["classification_confidence"] = float(assessment.get("confidence") or 0.0)
    payload["classification_source"] = str(assessment.get("source") or "deterministic")
    payload["quarantine_eligible"] = bool(assessment.get("quarantine_eligible"))
    payload["evidence"] = dict(assessment or {})
    if refresh_ai:
        try:
            payload["ai_verdict"] = _run_incomplete_ai_shadow_verdict(payload, force=True)
        except Exception:
            logging.debug("Failed to compute broken album AI shadow verdict artist=%s album_id=%s", artist, album_id, exc_info=True)

    if not bool(assessment.get("mark_broken")) and not bool(assessment.get("needs_manual_review")):
        con = _state_connect(timeout=10)
        try:
            cur = con.cursor()
            cur.execute(
                "DELETE FROM broken_albums WHERE artist = ? AND album_id = ?",
                (artist, int(album_id)),
            )
            con.commit()
        finally:
            con.close()
        payload["removed_from_broken_albums"] = True
        return payload

    con = _state_connect(timeout=10)
    try:
        cur = con.cursor()
        cur.execute(
            """
            UPDATE broken_albums
               SET expected_track_count = ?,
                   actual_track_count = ?,
                   missing_indices = ?,
                   album_title = ?,
                   folder_path = ?,
                   metadata_source = ?,
                   strict_match_provider = ?,
                   strict_reject_reason = ?,
                   provider_refs_json = ?,
                   reason_summary = ?,
                   local_tracks_json = ?,
                   expected_tracks_json = ?,
                   missing_required_tags_json = ?,
                   evidence_json = ?,
                   ai_verdict_json = ?,
                   classification = ?,
                   classification_confidence = ?,
                   classification_source = ?,
                   quarantine_eligible = ?
             WHERE artist = ? AND album_id = ?
            """,
            (
                int(payload.get("expected_track_count") or 0),
                int(payload.get("actual_track_count") or 0),
                json.dumps(payload.get("missing_indices") or [], default=str),
                str(album_title or ""),
                str(folder_path or ""),
                str(metadata_source or ""),
                str(strict_match_provider or ""),
                str(payload.get("strict_reject_reason") or strict_reject_reason or ""),
                json.dumps(payload.get("provider_refs") or {}, default=str),
                str(payload.get("reason_summary") or ""),
                json.dumps(payload.get("local_tracks") or [], default=str),
                json.dumps(payload.get("expected_tracks") or [], default=str),
                json.dumps(payload.get("missing_required_tags") or [], default=str),
                json.dumps(payload.get("evidence") or {}, default=str),
                json.dumps(payload.get("ai_verdict") or {}, default=str),
                str(payload.get("classification") or ""),
                float(payload.get("classification_confidence") or 0.0),
                str(payload.get("classification_source") or ""),
                1 if bool(payload.get("quarantine_eligible")) else 0,
                artist,
                int(album_id),
            ),
        )
        con.commit()
    finally:
        con.close()
    return payload


def api_broken_album_detail():
    artist = str(request.args.get("artist") or "").strip()
    album_id = _parse_int_loose(request.args.get("album_id"), 0)
    if not artist or album_id <= 0:
        return jsonify({"error": "artist and album_id are required"}), 400

    con = _state_connect(timeout=30)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT artist, album_id, expected_track_count, actual_track_count,
                   missing_indices, musicbrainz_release_group_id, detected_at,
                   sent_to_lidarr,
                   COALESCE(review_status, '') AS review_status,
                   COALESCE(album_title, '') AS album_title,
                   COALESCE(folder_path, '') AS folder_path,
                   COALESCE(metadata_source, '') AS metadata_source,
                   COALESCE(strict_match_provider, '') AS strict_match_provider,
                   COALESCE(strict_reject_reason, '') AS strict_reject_reason,
                   COALESCE(provider_refs_json, '{}') AS provider_refs_json,
                   COALESCE(reason_summary, '') AS reason_summary,
                   COALESCE(local_tracks_json, '[]') AS local_tracks_json,
                   COALESCE(expected_tracks_json, '[]') AS expected_tracks_json,
                   COALESCE(missing_required_tags_json, '[]') AS missing_required_tags_json,
                   COALESCE(classification, '') AS classification,
                   COALESCE(classification_confidence, 0.0) AS classification_confidence,
                   COALESCE(classification_source, '') AS classification_source,
                   COALESCE(quarantine_eligible, 0) AS quarantine_eligible,
                   COALESCE(evidence_json, '{}') AS evidence_json,
                   COALESCE(ai_verdict_json, '{}') AS ai_verdict_json
            FROM broken_albums
            WHERE artist = ? AND album_id = ?
            LIMIT 1
            """,
            (artist, int(album_id)),
        )
        broken_row = cur.fetchone()
        if not broken_row:
            return jsonify({"error": "broken album not found"}), 404

        cur.execute(
            """
            SELECT *
            FROM scan_pipeline_trace
            WHERE album_id = ?
            ORDER BY updated_at DESC, rowid DESC
            LIMIT 1
            """,
            (int(album_id),),
        )
        trace_row = cur.fetchone()
        trace = _scan_pipeline_trace_row_to_api(trace_row) if trace_row else {}

        cur.execute(
            """
            SELECT title_raw, folder, metadata_source, strict_match_provider, strict_reject_reason
            FROM scan_editions
            WHERE album_id = ?
            ORDER BY rowid DESC
            LIMIT 1
            """,
            (int(album_id),),
        )
        edition_row = cur.fetchone()
    finally:
        con.close()

    published: dict[str, Any] = {}
    base_url = request.url_root.rstrip("/")
    pg_conn = _files_pg_connect(acquire_timeout_sec=0.75)
    if pg_conn is not None:
        try:
            with pg_conn.cursor() as pg_cur:
                pg_cur.execute(
                    """
                    SELECT
                        alb.id,
                        COALESCE(alb.title, '') AS title,
                        COALESCE(alb.folder_path, '') AS folder_path,
                        alb.has_cover,
                        COALESCE(alb.cover_path, '') AS cover_path,
                        COALESCE(alb.metadata_source, '') AS metadata_source,
                        COALESCE(alb.strict_match_provider, '') AS strict_match_provider,
                        COALESCE(alb.strict_reject_reason, '') AS strict_reject_reason
                    FROM files_albums alb
                    WHERE alb.id = %s
                    LIMIT 1
                    """,
                    (int(album_id),),
                )
                row = pg_cur.fetchone()
                if row:
                    has_cover_effective, _effective_cover_path = _resolve_files_album_cover_asset(
                        album_id=int(album_id or 0),
                        cover_path_raw=str(row[4] or "").strip(),
                        folder_path_raw=str(row[2] or "").strip(),
                        has_cover=bool(row[3]),
                        persist=True,
                    )
                    published = {
                        "album_title": str(row[1] or "").strip(),
                        "folder_path": str(row[2] or "").strip(),
                        "thumb_url": f"{base_url}/api/library/files/album/{int(album_id)}/cover?size=320" if has_cover_effective else None,
                        "metadata_source": _normalize_identity_provider(str(row[5] or "")) or None,
                        "strict_match_provider": _normalize_identity_provider(str(row[6] or "")) or None,
                        "strict_reject_reason": str(row[7] or "").strip() or None,
                    }
        finally:
            try:
                pg_conn.close()
            except Exception:
                pass

    broken_keys = set(broken_row.keys()) if hasattr(broken_row, "keys") else set()

    def _broken_value(key: str, default: Any = None) -> Any:
        try:
            if key in broken_keys:
                return broken_row[key]
        except Exception:
            pass
        return default

    missing_indices_raw = _broken_value("missing_indices", "[]")
    try:
        missing_indices = json.loads(str(missing_indices_raw or "[]"))
        if not isinstance(missing_indices, list):
            missing_indices = []
    except Exception:
        missing_indices = []

    def _broken_json_list(key: str, default: Any) -> Any:
        raw_value = _broken_value(key, None)
        if raw_value in (None, ""):
            return default
        try:
            parsed = json.loads(str(raw_value))
        except Exception:
            return default
        return parsed

    stored_title = str(_broken_value("album_title", "") or "").strip()
    stored_folder = str(_broken_value("folder_path", "") or "").strip()
    stored_metadata_source = _normalize_identity_provider(str(_broken_value("metadata_source", "") or "")) or None
    stored_strict_provider = _normalize_identity_provider(str(_broken_value("strict_match_provider", "") or "")) or None
    stored_strict_reason = str(_broken_value("strict_reject_reason", "") or "").strip() or None
    stored_provider_refs = _broken_json_list("provider_refs_json", {})
    if not isinstance(stored_provider_refs, dict):
        stored_provider_refs = {}
    stored_reason_summary = str(_broken_value("reason_summary", "") or "").strip()
    stored_local_tracks = _broken_json_list("local_tracks_json", [])
    if not isinstance(stored_local_tracks, list):
        stored_local_tracks = []
    stored_expected_tracks = _broken_json_list("expected_tracks_json", [])
    if not isinstance(stored_expected_tracks, list):
        stored_expected_tracks = []
    stored_missing_required_tags = _broken_json_list("missing_required_tags_json", [])
    if not isinstance(stored_missing_required_tags, list):
        stored_missing_required_tags = []
    stored_evidence_json = _broken_json_list("evidence_json", {})
    if not isinstance(stored_evidence_json, dict):
        stored_evidence_json = {}
    stored_ai_verdict_json = _broken_json_list("ai_verdict_json", {})
    if not isinstance(stored_ai_verdict_json, dict):
        stored_ai_verdict_json = {}
    anchor_title, anchor_folder = _broken_album_snapshot_anchor(
        stored_title,
        stored_folder,
        trace_title=trace.get("album_title"),
        trace_folder=trace.get("folder"),
        edition_title=(str(edition_row["title_raw"] or "").strip() if edition_row else ""),
        edition_folder=(str(edition_row["folder"] or "").strip() if edition_row else ""),
    )

    if trace and not _broken_album_meta_matches_snapshot(
        anchor_title,
        anchor_folder,
        trace.get("album_title"),
        trace.get("folder"),
    ):
        trace = {}
    if edition_row and not _broken_album_meta_matches_snapshot(
        anchor_title,
        anchor_folder,
        str(edition_row["title_raw"] or "").strip(),
        str(edition_row["folder"] or "").strip(),
    ):
        edition_row = None
    if published and not _broken_album_meta_matches_snapshot(
        anchor_title,
        anchor_folder,
        published.get("album_title"),
        published.get("folder_path"),
    ):
        published = {}

    folder_path = str(
        anchor_folder
        or published.get("folder_path")
        or trace.get("folder")
        or (str(edition_row["folder"] or "").strip() if edition_row else "")
        or ""
    ).strip()
    album_title = _broken_album_display_title(
        anchor_title
        or str(published.get("album_title") or "").strip()
        or str(trace.get("album_title") or "").strip()
        or (str(edition_row["title_raw"] or "").strip() if edition_row else ""),
        folder_path,
        int(album_id),
    )
    metadata_source = (
        stored_metadata_source
        or published.get("metadata_source")
        or trace.get("metadata_source")
        or (str(edition_row["metadata_source"] or "").strip() if edition_row else "")
        or trace.get("decision_provider")
        or ""
    )
    strict_match_provider = (
        stored_strict_provider
        or published.get("strict_match_provider")
        or trace.get("strict_match_provider")
        or (str(edition_row["strict_match_provider"] or "").strip() if edition_row else "")
        or ""
    )
    strict_reject_reason = (
        stored_strict_reason
        or published.get("strict_reject_reason")
        or trace.get("strict_reject_reason")
        or (str(edition_row["strict_reject_reason"] or "").strip() if edition_row else "")
        or ""
    )
    expected_track_count = _parse_int_loose(broken_row["expected_track_count"], 0)
    actual_track_count = _parse_int_loose(broken_row["actual_track_count"], 0)
    detail_map = {
        "expected_track_count": expected_track_count,
        "actual_track_count": actual_track_count,
        "missing_indices": missing_indices,
        "strict_match_provider": strict_match_provider,
        "strict_reject_reason": strict_reject_reason,
        "musicbrainz_release_group_id": str(_broken_value("musicbrainz_release_group_id", "") or "").strip() or str(stored_provider_refs.get("musicbrainz_release_id") or trace.get("provider_refs", {}).get("musicbrainz_release_id") or "").strip(),
        "discogs_release_id": str(stored_provider_refs.get("discogs_release_id") or trace.get("provider_refs", {}).get("discogs_release_id") or "").strip(),
        "lastfm_album_mbid": str(stored_provider_refs.get("lastfm_album_mbid") or trace.get("provider_refs", {}).get("lastfm_album_mbid") or "").strip(),
        "bandcamp_album_url": str(stored_provider_refs.get("bandcamp_album_url") or trace.get("provider_refs", {}).get("bandcamp_album_url") or "").strip(),
        "missing_required_tags": list(stored_missing_required_tags or trace.get("missing_required_tags") or []),
    }

    local_tracks = list(stored_local_tracks or [])
    folder_obj, local_tracks, local_tracks_verified = _broken_album_resolve_folder_snapshot(
        folder_path=folder_path,
        artist_name=artist,
        album_title=album_title,
        metadata_source=str(metadata_source or strict_match_provider or ""),
        detail_map=detail_map,
        existing_local_tracks=local_tracks,
        force_rescan=True,
    )
    expected_tracks = list(stored_expected_tracks or [])
    if not expected_tracks:
        expected_tracks = _scan_move_expected_tracks(
            folder=folder_obj,
            artist_name=artist,
            album_title=album_title,
            metadata_source=str(metadata_source or strict_match_provider or ""),
            details=detail_map,
            cache_only=True,
        )
    payload = {
        "artist": artist,
        "album_id": int(album_id),
        "album_title": album_title,
        "detected_at": float(_broken_value("detected_at", 0) or 0),
        "folder_path": folder_path or None,
        "thumb_url": published.get("thumb_url"),
        "metadata_source": _normalize_identity_provider(str(metadata_source or "")) or None,
        "strict_match_provider": _normalize_identity_provider(str(strict_match_provider or "")) or None,
        "strict_reject_reason": strict_reject_reason or None,
        "expected_track_count": int(expected_track_count or 0),
        "actual_track_count": int(actual_track_count or len(local_tracks or [])),
        "missing_indices": missing_indices,
        "missing_required_tags": list(stored_missing_required_tags or trace.get("missing_required_tags") or []),
        "musicbrainz_release_group_id": str(_broken_value("musicbrainz_release_group_id", "") or "").strip() or None,
        "provider_refs": stored_provider_refs or trace.get("provider_refs") or {},
        "pipeline_status": str(trace.get("pipeline_status") or "incomplete"),
        "timeline": trace.get("timeline") or [],
        "meta_summary": trace.get("meta_summary") or {},
        "reason_summary": stored_reason_summary or _broken_album_reason_summary(
            expected_track_count=int(expected_track_count or 0),
            actual_track_count=int(actual_track_count or len(local_tracks or [])),
            missing_indices=missing_indices,
            missing_required_tags=list(stored_missing_required_tags or trace.get("missing_required_tags") or []),
            strict_reject_reason=str(strict_reject_reason or ""),
        ),
        "classification": str(_broken_value("classification", "") or "").strip() or None,
        "classification_confidence": float(_broken_value("classification_confidence", 0.0) or 0.0),
        "classification_source": str(_broken_value("classification_source", "") or "").strip() or None,
        "quarantine_eligible": bool(int(_broken_value("quarantine_eligible", 0) or 0)),
        "evidence": dict(stored_evidence_json or {}),
        "ai_verdict": dict(stored_ai_verdict_json or {}),
        "local_tracks": local_tracks,
        "expected_tracks": expected_tracks,
        "_local_tracks_verified": bool(local_tracks_verified),
        "recoverable": bool(str(_broken_value("musicbrainz_release_group_id", "") or "").strip()),
        "sent_to_lidarr": bool(_broken_value("sent_to_lidarr", 0)) if _broken_value("sent_to_lidarr", 0) is not None else False,
        "sent_to_external_recovery": bool(_broken_value("sent_to_lidarr", 0)) if _broken_value("sent_to_lidarr", 0) is not None else False,
        "review_status": str(_broken_value("review_status", "") or "").strip() or "pending",
    }
    assessment = _build_incomplete_assessment_from_payload(payload)
    payload["reason_summary"] = str(payload.get("reason_summary") or assessment.get("summary") or "")
    payload["classification"] = str(assessment.get("verdict") or payload.get("classification") or "").strip() or None
    payload["classification_confidence"] = float(assessment.get("confidence") or payload.get("classification_confidence") or 0.0)
    payload["classification_source"] = str(assessment.get("source") or payload.get("classification_source") or "").strip() or None
    payload["quarantine_eligible"] = bool(assessment.get("quarantine_eligible")) if assessment else bool(payload.get("quarantine_eligible"))
    if not bool(assessment.get("mark_broken")):
        try:
            _broken_album_delete_rows([(artist, int(album_id))])
        except Exception:
            logging.debug("Failed to purge stale broken album detail row artist=%s album_id=%s", artist, album_id, exc_info=True)
        payload["removed_from_broken_albums"] = True
        return jsonify(payload)
    refresh_ai = str(request.args.get("refresh_ai") or "").strip().lower() in {"1", "true", "yes", "on"}
    if refresh_ai:
        try:
            queued, queue_status = _trigger_incomplete_ai_review_async(artist, int(album_id))
            queued_at = float(time.time())
            ai_verdict = dict(payload.get("ai_verdict") or {})
            if not ai_verdict or queued:
                ai_verdict = {
                    "status": "queued" if queued else queue_status,
                    "provider": "ollama",
                    "shadow_mode": True,
                    "prompt_version": _INCOMPLETE_AI_PROMPT_VERSION,
                    "deterministic_verdict": str(payload.get("classification") or "").strip(),
                    "deterministic_confidence": float(payload.get("classification_confidence") or 0.0),
                    "created_at": queued_at,
                }
            payload["ai_verdict"] = ai_verdict
        except Exception:
            logging.debug("Failed to enqueue broken album AI shadow verdict artist=%s album_id=%s", artist, album_id, exc_info=True)
    try:
        con = _state_connect(timeout=10)
        cur = con.cursor()
        cur.execute(
            """
            UPDATE broken_albums
               SET expected_track_count = ?,
                   actual_track_count = ?,
                   missing_indices = ?,
                   album_title = ?,
                   folder_path = ?,
                   metadata_source = ?,
                   strict_match_provider = ?,
                   strict_reject_reason = ?,
                   provider_refs_json = ?,
                   reason_summary = ?,
                   local_tracks_json = ?,
                   expected_tracks_json = ?,
                   missing_required_tags_json = ?,
                   evidence_json = ?,
                   ai_verdict_json = ?,
                   classification = ?,
                   classification_confidence = ?,
                   classification_source = ?,
                   quarantine_eligible = ?
             WHERE artist = ? AND album_id = ?
            """,
            (
                int(payload.get("expected_track_count") or 0),
                int(payload.get("actual_track_count") or 0),
                json.dumps(payload.get("missing_indices") or [], default=str),
                str(album_title or ""),
                str(folder_path or ""),
                str(_normalize_identity_provider(str(metadata_source or "")) or ""),
                str(_normalize_identity_provider(str(strict_match_provider or "")) or ""),
                str(payload.get("strict_reject_reason") or strict_reject_reason or ""),
                json.dumps(payload.get("provider_refs") or {}, default=str),
                str(payload.get("reason_summary") or ""),
                json.dumps(payload.get("local_tracks") or [], default=str),
                json.dumps(payload.get("expected_tracks") or [], default=str),
                json.dumps(payload.get("missing_required_tags") or [], default=str),
                json.dumps(payload.get("evidence") or {}, default=str),
                json.dumps(payload.get("ai_verdict") or {}, default=str),
                str(payload.get("classification") or ""),
                float(payload.get("classification_confidence") or 0.0),
                str(payload.get("classification_source") or ""),
                1 if bool(payload.get("quarantine_eligible")) else 0,
                artist,
                int(album_id),
            ),
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to persist broken album detail snapshot artist=%s album_id=%s", artist, album_id, exc_info=True)
    return jsonify(payload)


_ORIGINAL_EXTRACTED_FUNCTIONS = {
    "_broken_album_delete_rows": _broken_album_delete_rows,
    "_broken_album_resolve_folder_snapshot": _broken_album_resolve_folder_snapshot,
    "_broken_album_backfill_candidates": _broken_album_backfill_candidates,
    "_run_broken_album_backfill": _run_broken_album_backfill,
    "_trigger_broken_album_backfill_async": _trigger_broken_album_backfill_async,
}


def _broken_album_delete_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _broken_album_delete_rows(*args, **kwargs)


def _broken_album_resolve_folder_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _broken_album_resolve_folder_snapshot(*args, **kwargs)


def _broken_album_backfill_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _broken_album_backfill_candidates(*args, **kwargs)


def _run_broken_album_backfill_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_broken_album_backfill(*args, **kwargs)


def _trigger_broken_album_backfill_async_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _trigger_broken_album_backfill_async(*args, **kwargs)
