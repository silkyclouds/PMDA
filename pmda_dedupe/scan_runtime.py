"""Runtime-backed duplicate scan workers.

This module contains the heavy duplicate scanning implementations extracted
from ``pmda.py``. The public boundary still accepts the live PMDA runtime
module until the scan state and persistence dependencies are decomposed into
explicit services.
"""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one duplicate scan call."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {"scan_artist_duplicates", "scan_duplicates"}
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def scan_artist_duplicates_for_runtime(runtime: Any, args: Any):
    """Run ``scan_artist_duplicates`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _scan_artist_duplicates_impl(args)

def scan_duplicates_for_runtime(runtime: Any, editions: Any, artist: Any):
    """Run ``scan_duplicates`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _scan_duplicates_impl(editions, artist)


def _scan_artist_duplicates_impl(args):
    """
    ThreadPool worker: scan one artist for duplicate albums.
    args: (artist_id, artist_name) or (artist_id, artist_name, album_ids).
    When album_ids is provided (e.g. merged from multiple Plex artist entries with same name),
    use it so duplicates across folders (e.g. Ochre vs Ochre2) are detected.
    Returns (artist_name, list_of_groups, album_count, stats_dict).
    stats_dict contains: {"ai_used": count, "mb_used": count, "timing": {...}}
    """
    artist_id = args[0]
    artist_name = args[1]
    album_ids = args[2] if len(args) >= 3 else None
    artist_start_time = time.perf_counter()
    timing_stats = {
        "db_query_time": 0.0,
        "audio_analysis_time": 0.0,
        "mb_lookup_time": 0.0,
        "ai_processing_time": 0.0,
        "total_time": 0.0,
    }
    try:
        if scan_should_stop.is_set():
            return (artist_name, [], 0, {"ai_used": 0, "mb_used": 0, "timing": timing_stats}, [])
        while scan_is_paused.is_set() and not scan_should_stop.is_set():
            time.sleep(0.5)
        logging.info("Processing artist: %s", artist_name)
        mode = _get_library_mode()
        db_conn = None
        prebuilt_editions = None

        if mode == "files":
            # Build editions from state populated by _build_scan_plan (files backend).
            with lock:
                files_editions = state.get("files_editions_by_album_id") or {}
            if album_ids is None:
                album_ids = []
            editions_for_artist = []
            for idx, aid in enumerate(album_ids):
                fe = files_editions.get(aid)
                if not fe:
                    continue
                if bool(fe.get("_resume_stub")) or not fe.get("tracks") or not fe.get("ordered_paths"):
                    hydrated = _hydrate_resume_files_edition(fe)
                    if not hydrated:
                        logging.info(
                            "Files resume hydration skipped album_id=%s artist=%s folder=%s",
                            aid,
                            artist_name,
                            str(fe.get("folder") or ""),
                        )
                        continue
                    fe = hydrated
                    with lock:
                        latest_files_editions = state.get("files_editions_by_album_id") or {}
                        latest_files_editions[aid] = hydrated
                folder = fe.get("folder")
                if not folder:
                    continue
                folder_for_io = path_for_fs_access(Path(str(fe.get("storage_access_path") or folder)))
                with lock:
                    if artist_name in state.get("scan_active_artists", {}):
                        state["scan_active_artists"][artist_name]["albums_processed"] = idx
                        state["scan_active_artists"][artist_name]["current_album"] = {
                            "album_id": aid,
                            "album_title": fe.get("album_title") or f"Album {aid}",
                            "status": "analyzing_format",
                            "status_details": "",
                            "step_summary": "Running FFprobe…",
                            "step_response": "",
                        }
                fmt_score_val, br, sr, bd, audio_cache_hit = analyse_format(folder_for_io)
                tr = fe.get("tracks") or []
                meta_tags = fe.get("tags") or {}
                title_raw = _sanitize_album_title_display(fe.get("album_title") or Path(folder).name.replace("_", " "))
                normalize_parenthetical = bool(_parse_bool(_get_config_from_db("NORMALIZE_PARENTHETICAL_FOR_DEDUPE") or "true"))
                album_norm_value = fe.get("album_norm") or norm_album_for_dedup(title_raw, normalize_parenthetical)
                plex_norm_value = album_norm_value
                # Files mode: deterministic broken detection from track indices.
                # For local files, *any* gap in a sane numbering sequence is a strong signal of missing tracks.
                is_broken = False
                expected_track_count = None
                actual_track_count = len(tr)
                missing_indices: list[int] = []
                try:
                    idxs = [int(getattr(t, "idx", 0) or 0) for t in (tr or []) if int(getattr(t, "idx", 0) or 0) > 0]
                    idxs = sorted(set(idxs))
                    if idxs and actual_track_count >= 4:
                        max_idx = max(idxs)
                        coverage = (actual_track_count / max_idx) if max_idx else 1.0
                        # Skip when numbering is obviously corrupt (prevents huge false positives).
                        if not (max_idx > max(120, actual_track_count * 3) and coverage < 0.5):
                            # Leading gap
                            if idxs[0] > 1:
                                missing_indices.extend(list(range(1, int(idxs[0]))))
                            # Internal gaps
                            for a, b in zip(idxs, idxs[1:]):
                                if int(b) - int(a) > 1:
                                    missing_indices.extend(list(range(int(a) + 1, int(b))))
                            if missing_indices:
                                is_broken = True
                                expected_track_count = max_idx
                                if len(missing_indices) > 5000:
                                    missing_indices = missing_indices[:5000]
                except Exception:
                    is_broken = False
                    expected_track_count = None
                    missing_indices = []
                editions_for_artist.append({
                    "album_id": aid,
                    "title_raw": title_raw,
                    "album_norm": album_norm_value,
                    "plex_norm": plex_norm_value,
                    "artist": artist_name,
                    "folder": folder,
                    "tracks": tr,
                    "file_count": fe.get("file_count") or len(tr),
                    "sig": signature(tr),
                    "titles": {t.title for t in tr},
                    "dur": sum(t.dur for t in tr),
                    "fmt_score": fmt_score_val,
                    "br": br,
                    "sr": sr,
                    "bd": bd,
                    "discs": len({t.disc for t in tr}) if tr else 1,
                    "meta": meta_tags,
                    "invalid": False,
                    "title_source": "tag:album",
                    "plex_title": title_raw,
                    "audio_cache_hit": audio_cache_hit,
                    "ordered_paths": fe.get("ordered_paths") or [],
                    "canonical_ordered_paths": fe.get("canonical_ordered_paths") or [],
                    "storage_access_path": str(fe.get("storage_access_path") or ""),
                    "storage_provider": str(fe.get("storage_provider") or ""),
                    "storage_device_id": str(fe.get("storage_device_id") or ""),
                    "storage_device_label": str(fe.get("storage_device_label") or ""),
                    "storage_bucket_order": int(fe.get("storage_bucket_order") or 0),
                    "storage_rel_path": str(fe.get("storage_rel_path") or ""),
                    "fingerprint": fe.get("fingerprint"),
                    "skip_heavy_processing": bool(fe.get("skip_heavy_processing")),
                    "has_cover": bool(fe.get("has_cover")),
                    "has_artist_image": bool(fe.get("has_artist_image")),
                    "missing_required_tags": list(fe.get("missing_required_tags") or []),
                    "has_mbid": bool(fe.get("has_mbid")),
                    "musicbrainz_id": (fe.get("musicbrainz_id") or "").strip(),
                    "is_broken": bool(is_broken),
                    "expected_track_count": expected_track_count,
                    "actual_track_count": int(actual_track_count),
                    "missing_indices": missing_indices,
                    # Provider identity (used by Dupe Detection v2 hard grouping)
                    "discogs_release_id": str(fe.get("discogs_release_id") or meta_tags.get("discogs_release_id") or "").strip(),
                    "lastfm_album_mbid": str(fe.get("lastfm_album_mbid") or meta_tags.get("lastfm_album_mbid") or "").strip(),
                    "bandcamp_album_url": str(fe.get("bandcamp_album_url") or meta_tags.get("bandcamp_album_url") or "").strip(),
                    "metadata_source": str(fe.get("metadata_source") or fe.get("identity_provider") or meta_tags.get("primary_metadata_source") or "").strip(),
                })
            prebuilt_editions = editions_for_artist
            # Changed-only scans only build editions for changed folders. For dupe detection we still
            # need to compare new/changed albums against the existing local catalog, otherwise we
            # miss dupes introduced in later runs. We pull lightweight context editions from the
            # published cache in state.db (no heavy provider work).
            try:
                with lock:
                    scan_type_hint = str(state.get("scan_type") or "full").strip().lower()
            except Exception:
                scan_type_hint = "full"
            if scan_type_hint == "changed_only" and editions_for_artist:
                try:
                    existing_folders: set[str] = set()
                    for e in editions_for_artist:
                        try:
                            existing_folders.add(str(Path(e.get("folder")).resolve()))
                        except Exception:
                            existing_folders.add(str(e.get("folder") or ""))
                    _ctx_artists, ctx_albums, _ctx_count = _load_files_library_published_payload_for_artist(artist_name)
                    # Allocate stable per-run ids for context editions (avoid collisions with scan-local ids)
                    next_ctx_id = (max([int(x) for x in album_ids if x is not None] or [0]) + 1) if album_ids else (max([int(e.get("album_id") or 0) for e in editions_for_artist] or [0]) + 1)
                    added = 0
                    for alb in (ctx_albums or []):
                        folder_path = str(alb.get("folder_path") or "").strip()
                        if not folder_path:
                            continue
                        try:
                            folder_obj = Path(folder_path)
                        except Exception:
                            continue
                        try:
                            folder_key = str(folder_obj.resolve())
                        except Exception:
                            folder_key = str(folder_obj)
                        if not folder_key or folder_key in existing_folders:
                            continue
                        # Convert published track dicts into Track tuples expected by dupe logic.
                        tracks_in: list = list(alb.get("tracks") or [])
                        tracks: list[Track] = []
                        br_guess = 0
                        sr_guess = 0
                        bd_guess = 0
                        for i, t in enumerate(tracks_in):
                            if not isinstance(t, dict):
                                continue
                            title = (t.get("title") or "").strip() or f"Track {i + 1}"
                            disc_num = _parse_int_loose(t.get("disc_num") or t.get("disc") or 1, 1) or 1
                            track_num = _parse_int_loose(t.get("track_num") or t.get("idx") or t.get("track") or (i + 1), i + 1) or (i + 1)
                            dur_sec = _parse_int_loose(t.get("duration_sec") or 0, 0) or 0
                            tracks.append(Track(title=title, idx=track_num, disc=disc_num, dur=int(dur_sec) * 1000))
                            br_guess = max(br_guess, int(_parse_int_loose(t.get("bitrate") or 0, 0) or 0))
                            sr_guess = max(sr_guess, int(_parse_int_loose(t.get("sample_rate") or 0, 0) or 0))
                            bd_guess = max(bd_guess, int(_parse_int_loose(t.get("bit_depth") or 0, 0) or 0))

                        meta_tags = {}
                        try:
                            meta_tags = json.loads(alb.get("primary_tags_json") or "{}") if alb.get("primary_tags_json") else {}
                            if not isinstance(meta_tags, dict):
                                meta_tags = {}
                        except Exception:
                            meta_tags = {}
                        try:
                            missing_required = json.loads(alb.get("missing_required_tags_json") or "[]") if alb.get("missing_required_tags_json") else []
                            if not isinstance(missing_required, list):
                                missing_required = []
                        except Exception:
                            missing_required = []

                        normalize_parenthetical = bool(_parse_bool(_get_config_from_db("NORMALIZE_PARENTHETICAL_FOR_DEDUPE") or "true"))
                        title_raw = (alb.get("title") or "").strip() or folder_obj.name.replace("_", " ").strip() or "Unknown Album"
                        album_norm_value = norm_album_for_dedup(title_raw, normalize_parenthetical)
                        fmt_txt = (alb.get("format") or "").strip().upper()
                        fmt_score_val = score_format(fmt_txt.lower()) if fmt_txt else 0

                        editions_for_artist.append(
                            {
                                "album_id": next_ctx_id,
                                "title_raw": title_raw,
                                "album_norm": album_norm_value,
                                "plex_norm": album_norm_value,
                                "artist": artist_name,
                                "folder": folder_obj,
                                "tracks": tracks,
                                "file_count": int(alb.get("track_count") or len(tracks) or 0),
                                "sig": signature(tracks),
                                "titles": {t.title for t in tracks},
                                "dur": sum(t.dur for t in tracks),
                                "fmt_score": fmt_score_val,
                                "br": br_guess,
                                "sr": sr_guess,
                                "bd": bd_guess,
                                "discs": len({t.disc for t in tracks}) if tracks else 1,
                                "meta": meta_tags,
                                "invalid": False,
                                "title_source": "published_cache",
                                "plex_title": title_raw,
                                "audio_cache_hit": True,
                                "ordered_paths": [Path(str(t.get("file_path"))) for t in tracks_in if isinstance(t, dict) and t.get("file_path")] if tracks_in else [],
                                "fingerprint": (alb.get("fingerprint") or "").strip() if isinstance(alb.get("fingerprint"), str) else alb.get("fingerprint"),
                                "skip_heavy_processing": True,
                                "has_cover": bool(alb.get("has_cover")),
                                "has_artist_image": bool(alb.get("has_artist_image")),
                                "missing_required_tags": missing_required,
                                "has_mbid": bool(alb.get("mb_identified")),
                                "musicbrainz_id": str(alb.get("musicbrainz_release_group_id") or "").strip(),
                                "discogs_release_id": str(alb.get("discogs_release_id") or "").strip(),
                                "lastfm_album_mbid": str(alb.get("lastfm_album_mbid") or "").strip(),
                                "bandcamp_album_url": str(alb.get("bandcamp_album_url") or "").strip(),
                                "metadata_source": str(alb.get("metadata_source") or "").strip(),
                                # Marker so scan stats can exclude this if needed.
                                "context_only": True,
                            }
                        )
                        existing_folders.add(folder_key)
                        next_ctx_id += 1
                        added += 1
                        if added >= 220:
                            break
                    if added:
                        prebuilt_editions = editions_for_artist
                        logging.debug("Files changed-only: added %d context edition(s) from published cache for artist %s", added, artist_name)
                except Exception:
                    logging.debug("Files changed-only: failed to add published context editions for artist %s", artist_name, exc_info=True)
        if album_ids is None and db_conn is not None:
            logging.warning(
                "[Artist %s (ID %s)] Legacy source database duplicate scan is disabled in files mode.",
                artist_name,
                artist_id,
            )
            album_ids = []
        if album_ids is None:
            album_ids = []
        logging.debug("[Artist %s (ID %s)] Album list for scan: %d albums", artist_name, artist_id, len(album_ids))

        # Update total_albums in active tracking
        with lock:
            if artist_name in state.get("scan_active_artists", {}):
                state["scan_active_artists"][artist_name]["total_albums"] = len(album_ids)

        groups = []
        stats = {"ai_used": 0, "mb_used": 0, "timing": {}}
        all_editions_for_stats = []
        if album_ids or prebuilt_editions:
            groups, stats, all_editions_for_stats = _scan_duplicates_impl(
                db_conn, artist_name, album_ids or [], prebuilt_editions=prebuilt_editions
            )
            # Merge timing stats
            if "timing" in stats:
                timing_stats.update(stats["timing"])
        if db_conn is not None:
            db_conn.close()

        timing_stats["total_time"] = time.perf_counter() - artist_start_time
        stats["timing"] = timing_stats

        logging.debug(
            "scan_artist_duplicates(): done Artist %s (ID %s) – %d groups, %d albums, AI=%d, MB=%d, "
            "timing: total=%.2fs, db=%.2fs, audio=%.2fs, mb=%.2fs, ai=%.2fs",
            artist_name, artist_id, len(groups), len(album_ids),
            stats.get("ai_used", 0), stats.get("mb_used", 0),
            timing_stats["total_time"], timing_stats["db_query_time"],
            timing_stats["audio_analysis_time"], timing_stats["mb_lookup_time"],
            timing_stats["ai_processing_time"]
        )
        return (artist_name, groups, len(album_ids), stats, all_editions_for_stats)
    except Exception as e:
        logging.error("Unexpected error scanning artist %s: %s", artist_name, e, exc_info=True)
        # On error, return no groups and zero albums so scan can continue
        timing_stats["total_time"] = time.perf_counter() - artist_start_time
        return (artist_name, [], 0, {"ai_used": 0, "mb_used": 0, "timing": timing_stats}, [])


def _scan_duplicates_impl(
    db_conn, artist: str, album_ids: List[int], *, prebuilt_editions: list | None = None
) -> tuple[List[dict], dict, list]:
    global no_file_streak_global, popup_displayed, gui
    scan_start_time = time.perf_counter()
    logging.debug("[Artist %s] Starting duplicate scan for album IDs: %s", artist, album_ids)
    logging.debug("Verbose SKIP_FOLDERS: %s", SKIP_FOLDERS)
    skip_count = 0
    editions = []
    total_albums = len(album_ids)
    processed_albums = 0
    PROGRESS_STATE["total"] = total_albums
    # Track folders and all album_ids pointing to each (for same-folder duplicate detection)
    seen_folders: dict[str, list[int]] = {}  # folder_path_resolved -> [album_id, ...]
    # Performance timing
    audio_analysis_time = 0.0
    mb_lookup_time = 0.0
    ai_processing_time = 0.0
    if prebuilt_editions is None:
        logging.warning(
            "[Artist %s] Legacy source-database duplicate scan is disabled; "
            "Files-mode prebuilt editions are required.",
            artist,
        )
        return (
            [],
            {
                "ai_used": 0,
                "mb_used": 0,
                "timing": {
                    "audio_analysis_time": 0.0,
                    "mb_lookup_time": 0.0,
                    "ai_processing_time": 0.0,
                    "total_time": time.perf_counter() - scan_start_time,
                },
            },
            [],
        )

    if prebuilt_editions is not None:
        # Files backend: use editions built by scan_artist_duplicates from state["files_editions_by_album_id"].
        editions = prebuilt_editions
        for e in editions:
            folder = e.get("folder")
            if folder is not None:
                try:
                    folder_str_resolved = str(Path(folder).resolve())
                    seen_folders.setdefault(folder_str_resolved, []).append(e["album_id"])
                except (OSError, RuntimeError):
                    seen_folders.setdefault(str(folder), []).append(e["album_id"])
        # Continue to MB enrichment and grouping below (skip the Plex for-loop).
    else:
        for aid in album_ids:
            processed_albums += 1
            PROGRESS_STATE["current"] = processed_albums
            # Periodic progress update every 100 albums
            if processed_albums % 100 == 0:
                logging.info("[Artist %s] processed %d/%d albums (skipped %d so far)", artist, processed_albums, total_albums, skip_count)
            try:
                if scan_should_stop.is_set():
                    break
                while scan_is_paused.is_set() and not scan_should_stop.is_set():
                    time.sleep(0.5)

                # Update current album tracking and albums_processed so UI shows progress during long artist scan
                with lock:
                    if artist in state.get("scan_active_artists", {}):
                        state["scan_active_artists"][artist]["albums_processed"] = processed_albums
                        album_title_str = album_title(db_conn, aid) or f"Album {aid}"
                        state["scan_active_artists"][artist]["current_album"] = {
                            "album_id": aid,
                            "album_title": album_title_str,
                            "status": "fetching_tracks",
                            "status_details": "",
                            "step_summary": "",
                            "step_response": ""
                        }

                tr = get_tracks(db_conn, aid)
                if not tr:
                    continue

                # Update: analyzing format
                with lock:
                    if artist in state.get("scan_active_artists", {}):
                        state["scan_active_artists"][artist]["current_album"]["status"] = "analyzing_format"
                        state["scan_active_artists"][artist]["current_album"]["status_details"] = "analyzing audio format"
                        state["scan_active_artists"][artist]["current_album"]["step_summary"] = "Running FFprobe…"

                folder = first_part_path(db_conn, aid)
                if not folder:
                    continue
                # Skip albums in configured skip folders (path-aware)
                logging.debug("Checking album %s at folder %s against skip prefixes %s", aid, folder, SKIP_FOLDERS)
                folder_resolved = Path(folder).resolve()
                folder_str_resolved = str(folder_resolved)

                # Track same-folder duplicates: multiple Plex album entries pointing to the same folder
                if folder_str_resolved in seen_folders:
                    seen_folders[folder_str_resolved].append(aid)
                    logging.warning(
                        "[Artist %s] Album ID %d points to the same folder as album ID(s) %s: %s. "
                        "Same-folder duplicate (Plex metadata). Will report as duplicate group.",
                        artist, aid, seen_folders[folder_str_resolved], folder_str_resolved
                    )
                    skip_count += 1
                    continue

                # First time we see this folder: record and process
                seen_folders[folder_str_resolved] = [aid]

                if SKIP_FOLDERS and any(folder_resolved.is_relative_to(Path(s).resolve()) for s in SKIP_FOLDERS):
                    skip_count += 1
                    logging.info("Skipping album %s since folder %s matches skip prefixes %s", aid, folder_resolved, SKIP_FOLDERS)
                    continue
                # count audio files once – we re‑use it later
                file_count = sum(1 for f in folder.rglob("*") if AUDIO_RE.search(f.name))

                # consider edition invalid when technical data are all zero OR no files found

                # ─── audio‑format inspection ──────────────────────────────────────
                audio_start = time.perf_counter()
                fmt_score, br, sr, bd, audio_cache_hit = analyse_format(folder)
                audio_analysis_time += time.perf_counter() - audio_start

                # --- metadata tags (first track only) -----------------------------
                first_audio = next((p for p in folder.rglob("*") if AUDIO_RE.search(p.name)), None)
                meta_tags = extract_tags(first_audio) if first_audio else {}

                # Mark as invalid if file_count == 0 OR all tech data are zero
                is_invalid = (file_count == 0) or (br == 0 and sr == 0 and bd == 0)

                # --- Quick retry before purging to avoid false negatives -------------
                if is_invalid:
                    time.sleep(0.5)
                    fmt_score_retry, br_retry, sr_retry, bd_retry, audio_cache_hit_retry = analyse_format(folder)
                    file_count_retry = file_count or sum(1 for f in folder.rglob("*") if AUDIO_RE.search(f.name))
                    if (file_count_retry == 0) or (br_retry == 0 and sr_retry == 0 and bd_retry == 0):
                        _purge_invalid_edition({
                            "folder":   folder,
                            "artist":   artist,
                            "title_raw": album_title(db_conn, aid),
                            "album_id": aid
                        })
                        continue            # do NOT add to the editions list
                    else:
                        fmt_score, br, sr, bd, audio_cache_hit = fmt_score_retry, br_retry, sr_retry, bd_retry, audio_cache_hit_retry
                        is_invalid = False

                plex_title = album_title(db_conn, aid)
                title_raw, title_source = derive_album_title(plex_title, meta_tags, folder, aid)
                normalize_parenthetical = bool(_parse_bool(_get_config_from_db("NORMALIZE_PARENTHETICAL_FOR_DEDUPE") or "true"))
                album_norm_value = norm_album_for_dedup(title_raw, normalize_parenthetical)

                # Update: album title + FFprobe result (low-level summary for UI)
                with lock:
                    if artist in state.get("scan_active_artists", {}) and state["scan_active_artists"][artist].get("current_album", {}).get("album_id") == aid:
                        state["scan_active_artists"][artist]["current_album"]["album_title"] = title_raw or plex_title or f"Album {aid}"
                        fmt_ext = first_audio.suffix.upper().lstrip(".") if first_audio else "?"
                        br_k = (br // 1000) if br >= 1000 else br
                        state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                            f"FFprobe: {fmt_ext} · {br_k} kbps · {sr} Hz · {bd}-bit"
                            + (" (cached)" if audio_cache_hit else "")
                        )
                        state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                            f"FFprobe: format {fmt_ext}, {br_k} kbps, {sr} Hz, {bd}-bit"
                            + (" (from cache)" if audio_cache_hit else "")
                        )
                        state["scan_format_done_count"] = state.get("scan_format_done_count", 0) + 1
                        state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1

                # Plex-normalized title: same key as get_duplicate_groups_from_library so scan groups match library
                plex_norm_value = norm_album_for_dedup(plex_title or "", normalize_parenthetical) if plex_title else album_norm_value
                editions.append({
                    'album_id':  aid,
                    'title_raw': title_raw,
                    'album_norm': album_norm_value,
                    'plex_norm': plex_norm_value,  # For grouping: align with library (norm_album(plex title))
                    'artist':    artist,
                    'folder':    folder,
                    'tracks':    tr,
                    'file_count': file_count,
                    'sig':       signature(tr),
                    'titles':    {t.title for t in tr},
                    'dur':       sum(t.dur for t in tr),
                    'fmt_score': fmt_score,
                    'br':        br,
                    'sr':        sr,
                    'bd':        bd,
                    'discs':     len({t.disc for t in tr}),
                    'meta':      meta_tags,
                    'invalid':   False,
                    'title_source': title_source,
                    'plex_title': plex_title or "",
                    'audio_cache_hit': audio_cache_hit  # Track if this album used cache
                })

                # Store AcousticID fingerprints in cache during scan when USE_ACOUSTID is on
                if getattr(sys.modules[__name__], "USE_ACOUSTID", False):
                    try:
                        _store_acoustid_fingerprints_for_folder(path_for_fs_access(folder))
                    except Exception:
                        pass

                # Mark album as done if it's not part of any duplicate group (single edition)
                # This will be updated later if it becomes part of a group
                with lock:
                    if artist in state.get("scan_active_artists", {}) and state["scan_active_artists"][artist].get("current_album", {}).get("album_id") == aid:
                        # Don't mark as done yet - wait to see if it's part of a group
                        pass
            except Exception as e:
                logging.error("Error processing album %s for artist %s: %s", aid, artist, e, exc_info=True)
                # Mark as done even on error
                with lock:
                    if artist in state.get("scan_active_artists", {}) and state["scan_active_artists"][artist].get("current_album", {}).get("album_id") == aid:
                        state["scan_active_artists"][artist]["current_album"]["status"] = "done"
                        state["scan_active_artists"][artist]["current_album"]["status_details"] = ""
                        state["scan_active_artists"][artist]["current_album"]["step_summary"] = ""
                        state["scan_active_artists"][artist]["current_album"]["step_response"] = ""
                continue

    logging.debug("[Artist %s] Computed stats for %d valid editions: %s", artist, len(editions), [e['album_id'] for e in editions])

    if not USE_MUSICBRAINZ:
        logging.debug("[Artist %s] Skipping MusicBrainz enrichment (USE_MUSICBRAINZ=False).", artist)
    else:
        # ─── MusicBrainz enrichment & Box Set handling ─────────────────────────────
        mb_start = time.perf_counter()
        # Per-album MB lookup only (no batch fetch). Batch fetch caused queue pile-up and 300s
        # timeouts when multiple scan threads each submitted a long-running fetch_rg_* job.
        artist_mb_rg_index = None
        if MB_DISABLE_CACHE:
            log_mb(
                "Artist %s: FULL MusicBrainz rescan – ignoring existing MBID tags and album lookup cache for this run",
                artist,
            )
        # Enrich using any available MusicBrainz ID tags (in priority order)
        id_tags = [
            'musicbrainz_releasegroupid',
            'musicbrainz_releaseid',
            'musicbrainz_originalreleaseid',
            'musicbrainz_albumid'
        ]
        def _log_release_match_outcome(edition: dict, *, reason: str | None = None, context: str | None = None) -> None:
            """Single closing log per release: trusted match ✅ or rejected/no-match ❌."""
            try:
                title = str(
                    edition.get("title_raw")
                    or edition.get("plex_title")
                    or edition.get("album_norm")
                    or "Unknown Album"
                )
                album_id = edition.get("album_id")
                strict_ok = bool(edition.get("strict_match_verified"))
                strict_provider = str(edition.get("strict_match_provider") or "").strip()
                strict_reason = str(edition.get("strict_reject_reason") or "").strip()
                hint = edition.get("_lookup_identity_hint") if isinstance(edition.get("_lookup_identity_hint"), dict) else {}
                hint_artist = str((hint or {}).get("artist") or "").strip()
                hint_album = str((hint or {}).get("album") or "").strip()
                hint_conf = int((hint or {}).get("confidence") or 0) if hint else 0
                hint_suffix = ""
                if hint_artist and hint_album and hint_conf > 0:
                    hint_suffix = f'; ai-hint="{hint_artist} - {hint_album}" ({hint_conf}%)'
                context_suffix = f" [{context}]" if context else ""
                provider_labels = {
                    "musicbrainz": "MusicBrainz",
                    "discogs": "Discogs",
                    "lastfm": "Last.fm",
                    "bandcamp": "Bandcamp",
                }
                if strict_ok:
                    provider = _normalize_identity_provider(strict_provider)
                    provider_id = ""
                    if provider == "musicbrainz":
                        provider_id = str(edition.get("musicbrainz_id") or "").strip()
                    elif provider == "discogs":
                        provider_id = str(edition.get("discogs_release_id") or "").strip()
                    elif provider == "lastfm":
                        provider_id = str(edition.get("lastfm_album_mbid") or "").strip()
                    elif provider == "bandcamp":
                        provider_id = str(edition.get("bandcamp_album_url") or "").strip()
                    provider_label = provider_labels.get(provider, "provider")
                    provider_id_snippet = (
                        provider_id[:80] + "…"
                        if provider_id and len(provider_id) > 80
                        else provider_id
                    )
                    id_suffix = f" (id={provider_id_snippet})" if provider_id_snippet else ""
                    log_match(
                        "Album %s – \"%s\" (album_id=%s): trusted via %s%s%s%s",
                        artist,
                        title,
                        album_id,
                        provider_label,
                        id_suffix,
                        hint_suffix,
                        context_suffix,
                    )
                    return
                soft_ok = bool(edition.get("has_identity"))
                if soft_ok:
                    provider = _normalize_identity_provider(
                        str(
                            edition.get("identity_provider")
                            or edition.get("primary_metadata_source")
                            or edition.get("metadata_source")
                            or ""
                        )
                    )
                    provider_id = ""
                    if provider == "musicbrainz":
                        provider_id = str(edition.get("musicbrainz_id") or "").strip()
                    elif provider == "discogs":
                        provider_id = str(edition.get("discogs_release_id") or "").strip()
                    elif provider == "lastfm":
                        provider_id = str(edition.get("lastfm_album_mbid") or "").strip()
                    elif provider == "bandcamp":
                        provider_id = str(edition.get("bandcamp_album_url") or "").strip()
                    if not provider_id:
                        provider_id = str(
                            edition.get("musicbrainz_id")
                            or edition.get("discogs_release_id")
                            or edition.get("lastfm_album_mbid")
                            or edition.get("bandcamp_album_url")
                            or ""
                        ).strip()
                    provider_label = provider_labels.get(provider, "provider")
                    provider_id_snippet = (
                        provider_id[:80] + "…"
                        if provider_id and len(provider_id) > 80
                        else provider_id
                    )
                    id_suffix = f" (id={provider_id_snippet})" if provider_id_snippet else ""
                    strict_suffix = f"; strict={strict_reason}" if strict_reason else ""
                    log_soft(
                        "Album %s – \"%s\" (album_id=%s): probable via %s%s%s%s%s",
                        artist,
                        title,
                        album_id,
                        provider_label,
                        id_suffix,
                        strict_suffix,
                        hint_suffix,
                        context_suffix,
                    )
                    return
                reject_reason = str(
                    reason
                    or strict_reason
                    or edition.get("_match_reject_reason")
                    or "strict_match_failed"
                ).strip()
                log_miss(
                    "Album %s – \"%s\" (album_id=%s): no trusted provider match (%s)%s%s; see [Providers] for strict/soft details",
                    artist,
                    title,
                    album_id,
                    reject_reason,
                    hint_suffix,
                    context_suffix,
                )
            except Exception:
                logging.debug("[Artist %s] release match outcome log failed", artist, exc_info=True)
        total_matching_albums = len(editions)
        for match_idx, e in enumerate(editions, start=1):
            _ai_usage_set_album_context(
                album_id=_int_or_none(e.get("album_id")),
                album_artist=str(artist or "").strip(),
                album_title=str(e.get("title_raw") or e.get("plex_title") or e.get("album_norm") or "").strip(),
            )
            # Set once per edition so all branches can use it (used before the later "album_norm = e['album_norm']")
            album_norm = e.get("album_norm") or e.get("title_raw") or ""
            tracks_edition = {t for t in (getattr(x, "title", None) for x in e.get("tracks", [])) if t}
            # Update current_album to this edition so UI shows MusicBrainz step for every album (not only the last one)
            with lock:
                if artist in state.get("scan_active_artists", {}):
                    state["scan_active_artists"][artist]["albums_processed"] = max(0, match_idx - 1)
                    state["scan_active_artists"][artist]["current_album"] = {
                        "album_id": e["album_id"],
                        "album_title": e.get("title_raw") or e.get("plex_title") or f"Album {e['album_id']}",
                        "album_index": match_idx,
                        "album_total": total_matching_albums,
                        "status": "fetching_mb_id",
                        "status_details": "fetching MusicBrainz ID",
                        "step_summary": "Looking up release group from tags…",
                        "step_response": "",
                        "status_updated_at": time.time(),
                    }
            if e.get("skip_heavy_processing"):
                mbid_quick = (e.get("musicbrainz_id") or _extract_musicbrainz_id_from_meta(e.get("meta") or {}) or "").strip()
                if mbid_quick:
                    e["musicbrainz_id"] = mbid_quick
                    e["musicbrainz_type"] = "release-group"
                e["rg_info_source"] = "incremental_skip"
                e["is_broken"] = False
                with lock:
                    if artist in state.get("scan_active_artists", {}) and e["album_id"] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                        state["scan_active_artists"][artist]["current_album"]["status"] = "searching_mb"
                        state["scan_active_artists"][artist]["current_album"]["status_details"] = "unchanged and complete (fast-skip)"
                        state["scan_active_artists"][artist]["current_album"]["step_summary"] = "Skipped heavy MB/provider lookup"
                        state["scan_active_artists"][artist]["current_album"]["step_response"] = "Incremental fast-skip: unchanged album already complete."
                    state["scan_mb_done_count"] = state.get("scan_mb_done_count", 0) + 1
                    state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1
                _log_release_match_outcome(e, context="incremental fast-skip")
                continue
            skip_live_mb = getattr(sys.modules[__name__], "SKIP_MB_FOR_LIVE_ALBUMS", True) and _is_likely_live_album(
                e.get("folder"), e.get("title_raw") or e.get("plex_title")
            )
            if skip_live_mb:
                rg_info = None
                with lock:
                    if artist in state.get("scan_active_artists", {}) and e["album_id"] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                        state["scan_active_artists"][artist]["current_album"]["status"] = "searching_mb"
                        state["scan_active_artists"][artist]["current_album"]["step_summary"] = "Skipped (live album; MB disabled for live albums)"
                        state["scan_active_artists"][artist]["current_album"]["step_response"] = "Skipped: live album; MusicBrainz lookup disabled. Fallback providers (Discogs/Last.fm/Bandcamp) will still be tried."
                logging.info(
                    "[Artist %s] Edition %s \"%s\": skipped MB (live album, SKIP_MB_FOR_LIVE_ALBUMS=true)",
                    artist, e.get("album_id"), e.get("title_raw") or e.get("plex_title") or album_norm,
                )
            else:
                meta = e.get('meta', {})
                rg_info = None
                mbid_found = None
                mbid_type = None
                tag_used = None

                # AcousticID first (for all albums when enabled, unless album already has MBID in tags and we skip to save API)
                existing_rgid_tag = meta.get("musicbrainz_releasegroupid")
                run_acoustid_first = (
                    USE_ACOUSTID and ACOUSTID_API_KEY
                    and (USE_ACOUSTID_WHEN_TAGGED or not existing_rgid_tag)
                )
                if not run_acoustid_first and USE_ACOUSTID and ACOUSTID_API_KEY and existing_rgid_tag:
                    logging.debug(
                        "[Artist %s] Edition %s: skip AcousticID (album has MBID in tags, USE_ACOUSTID_WHEN_TAGGED=false)",
                        artist, e.get("album_id"),
                    )
                if run_acoustid_first:
                    folder_ac = e.get("folder")
                    if folder_ac and Path(folder_ac).exists():
                        acoustid_rg_info, match_verified_by_ai = _identify_album_by_acoustic_id(
                            Path(folder_ac), artist or "Unknown", e.get("album_norm") or e.get("title_raw") or "Unknown"
                        )
                        if acoustid_rg_info:
                            e["acoustid_rg_info"] = acoustid_rg_info
                            e["acoustid_rg_id"] = acoustid_rg_info.get("id")
                            e["match_verified_by_ai"] = bool(match_verified_by_ai)
                            with lock:
                                if artist in state.get("scan_active_artists", {}) and e["album_id"] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                    state["scan_active_artists"][artist]["current_album"]["status_details"] = "AcousticID: identified (will verify with providers)"
                                    state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                        "AcousticID: release group %s" % (e["acoustid_rg_id"] or "")
                                    )
                            logging.debug("[Artist %s] Edition %s AcousticID first: rg_id=%s", artist, e["album_id"], e.get("acoustid_rg_id"))

                # Already identified: album has release-group ID in tags — use cache only, no API
                existing_rgid = meta.get('musicbrainz_releasegroupid')
                if existing_rgid and not MB_DISABLE_CACHE:
                    rg_info = get_cached_mb_info(existing_rgid)
                    mb_cache_hit = rg_info is not None
                    mbid_found = existing_rgid
                    mbid_type = 'release-group'
                    tag_used = 'musicbrainz_releasegroupid'
                    e['mb_cache_hit'] = mb_cache_hit
                    e['rg_info_source'] = tag_used
                    if rg_info:
                        e['rg_info'] = rg_info
                    e['musicbrainz_id'] = mbid_found
                    e['musicbrainz_type'] = mbid_type
                    with lock:
                        if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                            cache_text = " (cached)" if mb_cache_hit else " (from tags, no API)"
                            state["scan_active_artists"][artist]["current_album"]["status"] = "searching_mb"
                            state["scan_active_artists"][artist]["current_album"]["status_details"] = f"MusicBrainz ID from tags{cache_text}"
                            rg_title = (rg_info.get("title") or "") if isinstance(rg_info, dict) else ""
                            state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                f"MusicBrainz: release group \"{rg_title}\" (id: {mbid_found}){cache_text}"
                            )
                            state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                                f"MusicBrainz: from tags. Release group \"{rg_title}\" (id: {mbid_found}){cache_text}"
                            )
                    album_title_log = e.get("title_raw") or e.get("plex_title") or album_norm
                    # AcousticID verification: if we have AcousticID result and it disagrees with tags, prefer AcousticID
                    if e.get("acoustid_rg_id") and rg_info and rg_info.get("id") != e["acoustid_rg_id"]:
                        log_mb(
                            "Album %s – \"%s\": tags say RG %s but AcousticID says %s; using AcousticID",
                            artist, album_title_log, rg_info.get("id"), e["acoustid_rg_id"],
                        )
                        rg_info = e["acoustid_rg_info"]
                        e["rg_info"] = rg_info
                        e["musicbrainz_id"] = e["acoustid_rg_id"]
                        e["rg_info_source"] = "acoustid_verify"
                        mbid_found = e["acoustid_rg_id"]
                        with lock:
                            if artist in state.get("scan_active_artists", {}) and e["album_id"] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                state["scan_active_artists"][artist]["current_album"]["status_details"] = "AcousticID verified (overrode tags)"
                                state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                    "AcousticID: release group %s (overrode tags)" % e["acoustid_rg_id"]
                                )

                    # High-level log per album when MBID comes directly from tags
                    if rg_info:
                        log_mb(
                            "Album %s – \"%s\": MusicBrainz release-group %s from tags (%s)%s",
                            artist,
                            album_title_log,
                            mbid_found,
                            tag_used,
                            " (cached)" if mb_cache_hit else "",
                        )
                    logging.debug("[Artist %s] Edition %s already has MBID in tags, cache_hit=%s", artist, e['album_id'], mb_cache_hit)
                else:
                    # No release-group ID in tags (or full rescan requested): try other ID tags (release ID etc.) or search
                    if not MB_DISABLE_CACHE:
                        for tag in id_tags:
                            mbid = meta.get(tag)
                            if not mbid:
                                continue
                            try:
                                mb_cache_hit = False
                                if tag == 'musicbrainz_releasegroupid':
                                    rg_info, mb_cache_hit = fetch_mb_release_group_info(mbid)
                                    mbid_found = mbid
                                    mbid_type = 'release-group'
                                else:
                                    def _fetch_release():
                                        return musicbrainzngs.get_release_by_id(mbid, includes=["release-groups"])["release"]
                                    if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
                                        rel = get_mb_queue().submit(f"rel_{mbid}", _fetch_release)
                                    else:
                                        rel = _fetch_release()
                                    rgid = rel['release-group']['id']
                                    rg_info, mb_cache_hit = fetch_mb_release_group_info(rgid)
                                    mbid_found = rgid
                                    mbid_type = 'release-group'
                                e['mb_cache_hit'] = mb_cache_hit
                                tag_used = tag
                                with lock:
                                    if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                        cache_text = " (cached)" if mb_cache_hit else ""
                                        state["scan_active_artists"][artist]["current_album"]["status"] = "searching_mb"
                                        state["scan_active_artists"][artist]["current_album"]["status_details"] = f"MusicBrainz ID fetched{cache_text}"
                                        rg_title = (rg_info.get("title") or "") if isinstance(rg_info, dict) else ""
                                        state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                            f"MusicBrainz: release group \"{rg_title}\" (id: {mbid_found}){cache_text}"
                                        )
                                        state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                                            f"MusicBrainz: from tags ({tag}). Release group \"{rg_title}\" (id: {mbid_found}){cache_text}"
                                        )
                                album_title_log = e.get("title_raw") or e.get("plex_title") or album_norm
                                if rg_info:
                                    log_mb(
                                        "Album %s – \"%s\": MusicBrainz release-group %s fetched via tag %s%s",
                                        artist,
                                        album_title_log,
                                        mbid_found,
                                        tag,
                                        " (cached)" if mb_cache_hit else "",
                                    )
                                logging.debug("[Artist %s] Edition %s RG info (via %s %s): %s", artist, e['album_id'], tag, mbid, rg_info)
                                break
                            except Exception as exc:
                                logging.debug("[Artist %s] MusicBrainz lookup failed for %s (%s): %s", artist, tag, mbid, exc)
                        if rg_info:
                            e['rg_info_source'] = tag_used
                            e['rg_info'] = rg_info
                            if mbid_found:
                                e['musicbrainz_id'] = mbid_found
                                e['musicbrainz_type'] = mbid_type

                # Fallback: search by metadata if no ID tag yielded results
                album_norm = e['album_norm']
                tracks = {t.title for t in e['tracks']}
                artist_norm_key = _norm_artist_key(artist)
                if not rg_info:
                    # Check cache for "artist+album -> MBID".
                    cached_mbid = cached_rg_info = None
                    if not MB_DISABLE_CACHE:
                        cached_mbid, cached_rg_info = get_cached_mb_album_lookup(artist_norm_key, album_norm)
                    cached_not_found = bool(cached_mbid == "")
                    if cached_mbid and cached_mbid != "":
                        # Cached: found MBID for this artist+album — use it, no API search
                        rg_info = cached_rg_info or get_cached_mb_info(cached_mbid)
                        if rg_info:
                            e['rg_info_source'] = 'fallback'
                            e['rg_info'] = rg_info
                            e['musicbrainz_id'] = cached_mbid
                            e['musicbrainz_type'] = 'release-group'
                            e['mb_cache_hit'] = True
                            with lock:
                                if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                    rg_title = (rg_info.get("title") or "") if isinstance(rg_info, dict) else ""
                                    state["scan_active_artists"][artist]["current_album"]["status_details"] = "MusicBrainz found (cached)"
                                    state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                        f"MusicBrainz: found \"{rg_title}\" (id: {cached_mbid}) (cached)"
                                    )
                                    state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                                        f"MusicBrainz: found release group \"{rg_title}\" (id: {cached_mbid}) (cached)"
                                    )
                            album_title_log = e.get("title_raw") or e.get("plex_title") or album_norm
                            log_mb(
                                "Album %s – \"%s\": MusicBrainz release-group %s from album lookup cache",
                                artist,
                                album_title_log,
                                cached_mbid,
                            )
                            logging.debug("[Artist %s] Edition %s MBID from album lookup cache: %s", artist, e['album_id'], cached_mbid)
                    elif cached_not_found and not MB_RETRY_NOT_FOUND:
                        e["mb_cache_hit"] = True
                        e["rg_info_source"] = "mb_not_found_cached"
                        with lock:
                            if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                state["scan_active_artists"][artist]["current_album"]["status_details"] = "MusicBrainz skipped (cached not found)"
                                state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                    "MusicBrainz: cached not-found (provider fallback first)"
                                )
                                state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                                    "MusicBrainz: cached not-found, skipping re-query and using provider fallback."
                                )
                        album_title_log = e.get("title_raw") or e.get("plex_title") or album_norm
                        log_mb(
                            "Album %s – \"%s\": skipping MusicBrainz re-query (cached not-found, MB_RETRY_NOT_FOUND=false)",
                            artist,
                            album_title_log,
                        )
                    if not rg_info and not (cached_not_found and not MB_RETRY_NOT_FOUND):
                        # Not in cache (or cache intentionally bypassed): try pre-fetched artist index first, then search
                        with lock:
                            if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                state["scan_active_artists"][artist]["current_album"]["status"] = "searching_mb"
                                state["scan_active_artists"][artist]["current_album"]["status_details"] = "searching MusicBrainz"
                                state["scan_active_artists"][artist]["current_album"]["step_summary"] = "Searching by artist + album name…"
                                state["scan_active_artists"][artist]["current_album"]["step_response"] = "MusicBrainz: querying by artist + album name…"
                        title_raw_mb = e.get("title_raw") or e.get("plex_title") or album_norm
                        album_folder_arg = e.get("folder")
                        lookup_artist_for_search = str(artist or "").strip()
                        lookup_title_for_search = str(title_raw_mb or album_norm or "").strip()
                        # No-tags / weak-tags path: infer likely artist+album from local filenames/folder context.
                        # This is intentionally conservative (high confidence threshold) to avoid false positives.
                        ai_identity_hint = {}
                        if not _ai_scan_lifecycle_phase_active():
                            ai_identity_hint = _infer_identity_from_local_context_ai(
                                local_artist=artist,
                                local_album=lookup_title_for_search,
                                folder_path=album_folder_arg,
                                track_titles=list(tracks_edition) if tracks_edition else list(tracks or []),
                                file_paths=list(e.get("ordered_paths") or []),
                                local_tags=e.get("meta") if isinstance(e.get("meta"), dict) else {},
                                missing_required_tags=list(e.get("missing_required_tags") or []),
                            )
                        if isinstance(ai_identity_hint, dict) and ai_identity_hint:
                            hinted_artist = str(ai_identity_hint.get("artist") or "").strip()
                            hinted_album = str(ai_identity_hint.get("album") or "").strip()
                            hint_conf = int(ai_identity_hint.get("confidence") or 0)
                            if hinted_artist and hinted_album and hint_conf >= 65:
                                lookup_artist_for_search = hinted_artist
                                lookup_title_for_search = hinted_album
                                e["_lookup_artist_name"] = hinted_artist
                                e["_lookup_album_title"] = hinted_album
                                e["_lookup_identity_hint"] = ai_identity_hint
                                _apply_resolved_identity_to_edition(
                                    e,
                                    default_artist=artist,
                                    default_title=title_raw_mb or album_norm,
                                    folder_name=(Path(album_folder_arg).name if album_folder_arg else ""),
                                )
                                log_mb(
                                    "Album %s – \"%s\": AI local-context identity hint => artist=%r album=%r (confidence=%d)",
                                    artist,
                                    title_raw_mb or album_norm,
                                    hinted_artist,
                                    hinted_album,
                                    hint_conf,
                                )
                        rg_info = None
                        match_verified_by_ai = False
                        if artist_mb_rg_index is not None:
                            candidates = _match_album_norm_to_mb_index(album_norm, artist_mb_rg_index)
                            if candidates:
                                letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                log_mb(
                                    "%s – %r: %d candidate(s) from artist index",
                                    artist,
                                    title_raw_mb or album_norm or "?",
                                    len(candidates),
                                )
                                for i, rg in enumerate(candidates[:20]):
                                    log_mb(
                                        "Artist-index candidate %s: %s (id=%s)",
                                        letters[i],
                                        (rg.get("title") or "?"),
                                        rg.get("id") or "?",
                                    )
                                if len(candidates) > 20:
                                    log_mb("... and %d more artist-index candidate(s)", len(candidates) - 20)
                                chosen_rg_id = None
                                if len(candidates) == 1:
                                    chosen_rg_id = candidates[0].get("id")
                                elif len(candidates) > 1 and getattr(sys.modules[__name__], "ai_provider_ready", False):
                                    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                    choices = [f"{letters[i]}) {rg.get('title', '?')} (id={rg.get('id', '')})" for i, rg in enumerate(candidates[:20])]
                                    track_list_str = ", ".join((list(tracks)[:30] if tracks else [])) if tracks else "(none)"
                                    if tracks and len(tracks) > 30:
                                        track_list_str += ", ..."
                                    prompt = (
                                        f"Our album: artist={artist!r}, title_raw={title_raw_mb or album_norm!r}, normalized={album_norm!r}. "
                                        f"Our track titles: [{track_list_str}].\n\n"
                                        "MusicBrainz candidates (id + title only):\n" + "\n".join(choices) + "\n\n"
                                        "Which candidate matches our album? Reply with only the letter (A, B, ...) or the MBID (UUID) or NONE if no match. "
                                        "Optionally end with (confidence: N) where N is 0-100."
                                    )
                                    system_msg = "You reply with a single letter (A, B, C, ...) or an MBID (UUID) or the word NONE. Optionally end with (confidence: N). No other explanation."
                                    try:
                                        provider = getattr(sys.modules[__name__], "AI_PROVIDER", "openai")
                                        model = getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4o-mini")
                                        reply = _call_ai_provider_bounded(
                                            provider=provider,
                                            model=model,
                                            system_msg=system_msg,
                                            user_msg=prompt,
                                            max_tokens=40,
                                            analysis_type="mb_artist_index_choice",
                                            timeout_sec=AI_SCAN_HARD_TIMEOUT_SEC,
                                            log_prefix="[MusicBrainz Index AI]",
                                        )
                                        reply_clean, ai_confidence = parse_ai_confidence((reply or "").strip())
                                        if ai_confidence is not None:
                                            logging.info("[Artist %s] MusicBrainz index AI choice confidence: %d", artist, ai_confidence)
                                        reply = reply_clean.upper()
                                        if reply and reply != "NONE":
                                            letter = reply[:1]
                                            idx = letters.find(letter)
                                            if 0 <= idx < len(candidates):
                                                chosen_rg_id = candidates[idx].get("id")
                                            if not chosen_rg_id:
                                                mbid_match = re.search(r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}", reply)
                                                if mbid_match:
                                                    chosen_rg_id = mbid_match.group(0)
                                        if chosen_rg_id:
                                            match_verified_by_ai = True
                                    except Exception:
                                        chosen_rg_id = candidates[0].get("id") if candidates else None
                                else:
                                    chosen_rg_id = candidates[0].get("id") if candidates else None
                                if chosen_rg_id:
                                    try:
                                        rg_info, _ = fetch_mb_release_group_info(chosen_rg_id)
                                    except Exception as _err:
                                        logging.debug("[Artist %s] fetch_mb_release_group_info failed for %s (from index): %s", artist, chosen_rg_id, _err)
                                        rg_info = None
                        if not rg_info:
                            # Fast-path: if we already have a cached strict Bandcamp identity with perfect tracklist,
                            # skip expensive MB search for this album and rely on provider arbitration below.
                            if bool(getattr(sys.modules[__name__], "MB_FAST_FALLBACK_MODE", True)) and not SCAN_DISABLE_CACHE:
                                try:
                                    bandcamp_status, bandcamp_cached = get_cached_provider_album_lookup(
                                        "bandcamp",
                                        artist,
                                        title_raw_mb or album_norm,
                                    )
                                except Exception:
                                    bandcamp_status, bandcamp_cached = (None, None)
                                if bandcamp_status == "found" and isinstance(bandcamp_cached, dict):
                                    provider_payloads_prefetched = {
                                        "discogs": None,
                                        "lastfm": None,
                                        "bandcamp": bandcamp_cached,
                                        "extra_sources": [],
                                    }
                                    local_titles_prefetch = list(tracks_edition) if tracks_edition else list(tracks or [])
                                    prefetched_arbitration = _arbitrate_provider_identity(
                                        artist_name=artist,
                                        album_title=title_raw_mb or album_norm,
                                        local_track_titles=local_titles_prefetch,
                                        provider_payloads=provider_payloads_prefetched,
                                        local_tags=e.get("tags") if isinstance(e.get("tags"), dict) else {},
                                        local_paths=list(e.get("ordered_paths") or []),
                                    )
                                    if prefetched_arbitration and str(prefetched_arbitration.get("provider") or "").strip().lower() == "bandcamp":
                                        track_score_prefetch = float(prefetched_arbitration.get("track_score") or 0.0)
                                        title_score_prefetch = float(prefetched_arbitration.get("title_score") or 0.0)
                                        artist_score_prefetch = float(prefetched_arbitration.get("artist_score") or 0.0)
                                        if (
                                            track_score_prefetch >= 0.999
                                            and title_score_prefetch >= 0.999
                                            and artist_score_prefetch >= 0.999
                                        ):
                                            e["_provider_payloads_prefetched"] = provider_payloads_prefetched
                                            e["_provider_arbitration_prefetched"] = prefetched_arbitration
                                            e["_provider_fastpath_reason"] = "bandcamp_cached_strict_1.00"
                                            log_mb(
                                                "Album %s – \"%s\": skipping MusicBrainz search (cached strict Bandcamp match, tracklist=1.00)",
                                                artist,
                                                title_raw_mb or album_norm,
                                            )

                            fastpath_reason = e.pop("_provider_fastpath_reason", None)
                            if not fastpath_reason:
                                rg_info, match_verified_by_ai = search_mb_release_group_by_metadata(
                                    lookup_artist_for_search,
                                    lookup_title_for_search or album_norm,
                                    tracks,
                                    title_raw=lookup_title_for_search or title_raw_mb,
                                    album_folder=album_folder_arg,
                                    local_tags=e.get("tags") if isinstance(e.get("tags"), dict) else {},
                                    local_paths=list(e.get("ordered_paths") or []),
                                    scan_inline=True,
                                )
                                if (
                                    not rg_info
                                    and not bool((_classical_context_for_edition(e) or {}).get("is_classical"))
                                    and bool(getattr(sys.modules[__name__], "PROVIDER_IDENTITY_USE_AI", True))
                                    and not _scan_inline_matching_active()
                                ):
                                    retry_hint = {}
                                    if not _ai_scan_lifecycle_phase_active():
                                        retry_hint = _infer_identity_from_local_context_ai(
                                            local_artist=artist,
                                            local_album=lookup_title_for_search or album_norm,
                                            folder_path=album_folder_arg,
                                            track_titles=list(tracks_edition) if tracks_edition else list(tracks or []),
                                            file_paths=list(e.get("ordered_paths") or []),
                                            local_tags=e.get("meta") if isinstance(e.get("meta"), dict) else {},
                                            missing_required_tags=list(e.get("missing_required_tags") or []),
                                            force_try=True,
                                        )
                                    if isinstance(retry_hint, dict) and retry_hint:
                                        retry_artist = str(retry_hint.get("artist") or "").strip()
                                        retry_album = str(retry_hint.get("album") or "").strip()
                                        try:
                                            retry_conf = int(float(retry_hint.get("confidence") or 0))
                                        except Exception:
                                            retry_conf = 0
                                        retry_differs = bool(
                                            retry_artist
                                            and retry_album
                                            and (
                                                _normalize_identity_text_strict(retry_artist) != _normalize_identity_text_strict(lookup_artist_for_search)
                                                or _normalize_identity_text_strict(retry_album) != _normalize_identity_text_strict(lookup_title_for_search or album_norm)
                                            )
                                        )
                                        if retry_differs and retry_conf >= 85:
                                            e["_lookup_artist_name"] = retry_artist
                                            e["_lookup_album_title"] = retry_album
                                            e["_lookup_identity_hint"] = retry_hint
                                            _apply_resolved_identity_to_edition(
                                                e,
                                                default_artist=artist,
                                                default_title=title_raw_mb or album_norm,
                                                folder_name=(Path(album_folder_arg).name if album_folder_arg else ""),
                                            )
                                            log_mb(
                                                "Album %s – \"%s\": retrying MusicBrainz search with local-context hint artist=%r album=%r (confidence=%d)",
                                                artist,
                                                title_raw_mb or album_norm,
                                                retry_artist,
                                                retry_album,
                                                retry_conf,
                                            )
                                            rg_info, match_verified_by_ai = search_mb_release_group_by_metadata(
                                                retry_artist,
                                                retry_album,
                                                tracks,
                                                title_raw=retry_album,
                                                album_folder=album_folder_arg,
                                                local_tags=e.get("tags") if isinstance(e.get("tags"), dict) else {},
                                                local_paths=list(e.get("ordered_paths") or []),
                                                scan_inline=True,
                                            )
                        if rg_info and rg_info.get("track_titles") and tracks:
                            track_min = getattr(sys.modules[__name__], "TRACKLIST_MATCH_MIN", 0.8)
                            score = _crosscheck_tracklist(list(tracks), rg_info["track_titles"])
                            if score < track_min:
                                logging.info("[Artist %s] Edition %s: MB candidate rejected (tracklist match %.2f < %.2f)", artist, e.get("album_id"), score, track_min)
                                e["_match_reject_reason"] = (
                                    f"MusicBrainz candidate rejected (tracklist {score:.2f} < {track_min:.2f})"
                                )
                                rg_info = None
                        if rg_info:
                            set_cached_mb_album_lookup(artist_norm_key, album_norm, rg_info.get('id') or "", rg_info)
                        else:
                            set_cached_mb_album_lookup(artist_norm_key, album_norm, "", None)
                        if rg_info:
                            e['rg_info_source'] = 'fallback'
                            e['rg_info'] = rg_info
                            e['match_verified_by_ai'] = bool(match_verified_by_ai)
                            if isinstance(rg_info, dict) and 'id' in rg_info:
                                e['musicbrainz_id'] = rg_info['id']
                                e['musicbrainz_type'] = 'release-group'
                                mbid = rg_info['id']
                                cached_mb = get_cached_mb_info(mbid)
                                e['mb_cache_hit'] = cached_mb is not None
                                with lock:
                                    if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                        cache_text = " (cached)" if cached_mb else ""
                                        ai_text = " (match verified by AI)" if match_verified_by_ai else ""
                                        state["scan_active_artists"][artist]["current_album"]["status_details"] = f"MusicBrainz found{cache_text}{ai_text}"
                                        rg_title = (rg_info.get("title") or "") if isinstance(rg_info, dict) else ""
                                        state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                            f"MusicBrainz: found \"{rg_title}\" (id: {mbid}){cache_text}{ai_text}"
                                        )
                                        state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                                            f"MusicBrainz: found release group \"{rg_title}\" (id: {mbid}){cache_text}{ai_text}"
                                        )
                            album_title_log = e.get("title_raw") or e.get("plex_title") or album_norm
                            log_mb(
                                "Album %s – \"%s\": MusicBrainz search/fallback matched release-group %s%s",
                                artist,
                                album_title_log,
                                e.get("musicbrainz_id", "?"),
                                " (verified by AI)" if match_verified_by_ai else "",
                            )
                            logging.debug("[Artist %s] Edition %s RG info (search fallback)%s: %s", artist, e['album_id'], " (verified by AI)" if match_verified_by_ai else "", rg_info)
                        else:
                            logging.debug("[Artist %s] No RG info found via search for '%s'", artist, album_norm)
                            e['mb_cache_hit'] = False
                            with lock:
                                if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                    state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                        f"MusicBrainz: no release group found for \"{album_norm}\""
                                    )
                                    state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                                        f"MusicBrainz: no release group found for \"{album_norm}\""
                                    )
                            # Use stored AcousticID result when no RG from search (AcousticID was run first for all albums)
                            if not rg_info and e.get("acoustid_rg_info"):
                                rg_info = e["acoustid_rg_info"]
                                e["rg_info"] = rg_info
                                e["musicbrainz_id"] = e["acoustid_rg_id"]
                                e["rg_info_source"] = "acoustid"
                                rg_title = (rg_info.get("title") or "") if isinstance(rg_info, dict) else ""
                                mbid = rg_info.get("id", "?")
                                with lock:
                                    if artist in state.get("scan_active_artists", {}) and e["album_id"] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                        state["scan_active_artists"][artist]["current_album"]["status_details"] = "AcousticID: identified"
                                        state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                            f"AcousticID: identified as \"{rg_title}\" (id: {mbid})"
                                        )
                                        state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                                            f"AcousticID: identified as release group \"{rg_title}\" (id: {mbid})"
                                        )
                                log_mb("Album %s – folder: AcousticID matched release-group %s (%s)", artist, mbid, rg_title)

                # AcousticID verification: if we have RG from search/other but it disagrees with AcousticID, prefer AcousticID
                if e.get("acoustid_rg_id") and e.get("rg_info") and e["rg_info"].get("id") != e["acoustid_rg_id"]:
                    log_mb(
                        "Album %s – \"%s\": provider said RG %s but AcousticID says %s; using AcousticID",
                        artist, e.get("title_raw") or e.get("plex_title") or album_norm, e["rg_info"].get("id"), e["acoustid_rg_id"],
                    )
                    e["rg_info"] = e["acoustid_rg_info"]
                    e["musicbrainz_id"] = e["acoustid_rg_id"]
                    e["rg_info_source"] = "acoustid_verify"
                    with lock:
                        if artist in state.get("scan_active_artists", {}) and e["album_id"] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                            state["scan_active_artists"][artist]["current_album"]["status_details"] = "AcousticID verified (overrode provider)"
                            state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                "AcousticID: release group %s (overrode provider)" % e["acoustid_rg_id"]
                            )
                # LIVE_ALBUMS_MB_STRICT: for detected live albums, only keep MB assign if RG is type Live
                if getattr(sys.modules[__name__], "LIVE_ALBUMS_MB_STRICT", False) and e.get("rg_info") and _is_likely_live_album(e.get("folder"), e.get("title_raw") or e.get("plex_title")):
                    sec = (e["rg_info"].get("secondary_types") or []) if isinstance(e["rg_info"], dict) else []
                    if "Live" not in sec:
                        e["rg_info"] = None
                        e["musicbrainz_id"] = None
                        rg_info = None
                        e.pop("rg_info_source", None)
                        logging.info("[Artist %s] Edition %s: cleared MB assign (live album but RG is not type Live)", artist, e.get("album_id"))

            # Fallback when MusicBrainz found nothing: try Discogs, Bandcamp, then Last.fm for metadata
            # (and optionally MB ID from Last.fm as the last resort).
            title_raw_local = e.get("title_raw") or e.get("plex_title") or album_norm
            use_hint_for_provider_lookup = _identity_hint_safe_for_provider_lookup(
                e,
                default_artist=str(artist or "").strip(),
                default_title=str(title_raw_local or "").strip(),
            )
            lookup_artist_for_provider = (
                str(e.get("_lookup_artist_name") or artist or "").strip()
                if use_hint_for_provider_lookup
                else str(artist or "").strip()
            ) or str(artist or "").strip()
            title_raw = (
                str(e.get("_lookup_album_title") or title_raw_local or "").strip()
                if use_hint_for_provider_lookup
                else str(title_raw_local or "").strip()
            ) or str(title_raw_local or "")
            if (lookup_artist_for_provider != str(artist or "").strip()) or (title_raw != str(title_raw_local or "").strip()):
                logging.info(
                    "[Providers Lookup] local=%r - %r | using hint=%r - %r",
                    artist,
                    title_raw_local,
                    lookup_artist_for_provider,
                    title_raw,
                )
            if not rg_info:
                fallback_sources = []
                provider_payloads = e.pop("_provider_payloads_prefetched", None)
                scan_inline_provider_budget = bool(
                    _scan_inline_matching_active() or _ai_scan_lifecycle_phase_active()
                )
                if scan_inline_provider_budget:
                    provider_payloads = _provider_payloads_fetch_bounded_for_scan(
                        lookup_artist_for_provider,
                        title_raw,
                        existing=provider_payloads if isinstance(provider_payloads, dict) else None,
                    )
                else:
                    provider_payloads = _default_scan_provider_payloads(provider_payloads if isinstance(provider_payloads, dict) else None)

                def _ensure_provider_payload(provider_key: str) -> None:
                    """Fetch a provider payload (cached) only when needed."""
                    try:
                        if isinstance(provider_payloads.get(provider_key), dict):
                            return
                        if provider_key == "lastfm" and USE_LASTFM:
                            provider_payloads["lastfm"] = fetch_provider_album_lookup_cached(
                                "lastfm",
                                lookup_artist_for_provider,
                                title_raw,
                                _fetch_lastfm_album_info,
                            )
                        elif provider_key == "discogs" and USE_DISCOGS:
                            provider_payloads["discogs"] = fetch_provider_album_lookup_cached(
                                "discogs",
                                lookup_artist_for_provider,
                                title_raw,
                                _fetch_discogs_release,
                            )
                        elif provider_key == "bandcamp" and USE_BANDCAMP:
                            provider_payloads["bandcamp"] = fetch_provider_album_lookup_cached(
                                "bandcamp",
                                lookup_artist_for_provider,
                                title_raw,
                                _fetch_bandcamp_album_info,
                            )
                        elif provider_key == "itunes" and USE_ITUNES:
                            provider_payloads["itunes"] = fetch_provider_album_lookup_cached(
                                "itunes",
                                lookup_artist_for_provider,
                                title_raw,
                                _fetch_itunes_album_info,
                            )
                        elif provider_key == "deezer" and USE_DEEZER:
                            provider_payloads["deezer"] = fetch_provider_album_lookup_cached(
                                "deezer",
                                lookup_artist_for_provider,
                                title_raw,
                                _fetch_deezer_album_info,
                            )
                        elif provider_key == "spotify" and USE_SPOTIFY:
                            provider_payloads["spotify"] = fetch_provider_album_lookup_cached(
                                "spotify",
                                lookup_artist_for_provider,
                                title_raw,
                                _fetch_spotify_album_info,
                            )
                        elif provider_key == "qobuz" and USE_QOBUZ:
                            provider_payloads["qobuz"] = fetch_provider_album_lookup_cached(
                                "qobuz",
                                lookup_artist_for_provider,
                                title_raw,
                                _fetch_qobuz_album_info,
                            )
                        elif provider_key == "tidal" and USE_TIDAL:
                            provider_payloads["tidal"] = fetch_provider_album_lookup_cached(
                                "tidal",
                                lookup_artist_for_provider,
                                title_raw,
                                _fetch_tidal_album_info,
                            )
                        elif provider_key == "audiodb" and THEAUDIODB_API_KEY:
                            provider_payloads["audiodb"] = fetch_provider_album_lookup_cached(
                                "audiodb",
                                lookup_artist_for_provider,
                                title_raw,
                                _fetch_audiodb_album_info,
                            )
                    except DiscogsRateLimited:
                        # Rate limiting is handled globally; treat as "no payload" for this edition.
                        provider_payloads[provider_key] = None
                    except Exception as provider_exc:
                        logging.debug(
                            "[Artist %s] provider %s fetch failed for %s: %s",
                            lookup_artist_for_provider or artist,
                            provider_key,
                            title_raw,
                            provider_exc,
                        )
                        provider_payloads[provider_key] = None

                # In the live scan, keep provider fallback time-bounded. The bounded
                # parallel helper already attempted the fanout above and cached timeout
                # sentinels for slow providers, so do not re-serialize cold fetches here.
                if not scan_inline_provider_budget:
                    # Fetch providers in deterministic priority:
                    # MB -> Discogs -> iTunes -> Deezer -> Spotify -> Qobuz -> TIDAL -> TheAudioDB -> Bandcamp -> Last.fm.
                    # We avoid short-circuiting on Last.fm MBID before arbitration so Discogs/Bandcamp
                    # can win when they provide a stricter identity.
                    for provider_key in _SCAN_MATCH_PROVIDER_KEYS:
                        _ensure_provider_payload(provider_key)

                discogs_info = provider_payloads.get("discogs")
                if isinstance(discogs_info, dict):
                    e["fallback_discogs"] = discogs_info
                    fallback_sources.append("Discogs")

                bandcamp_info = provider_payloads.get("bandcamp")
                if isinstance(bandcamp_info, dict):
                    e["fallback_bandcamp"] = bandcamp_info
                    fallback_sources.append("Bandcamp")

                itunes_info = provider_payloads.get("itunes")
                if isinstance(itunes_info, dict):
                    e["fallback_itunes"] = itunes_info
                    fallback_sources.append("iTunes / Apple Music")

                deezer_info = provider_payloads.get("deezer")
                if isinstance(deezer_info, dict):
                    e["fallback_deezer"] = deezer_info
                    fallback_sources.append("Deezer")

                spotify_info = provider_payloads.get("spotify")
                if isinstance(spotify_info, dict):
                    e["fallback_spotify"] = spotify_info
                    fallback_sources.append("Spotify")

                qobuz_info = provider_payloads.get("qobuz")
                if isinstance(qobuz_info, dict):
                    e["fallback_qobuz"] = qobuz_info
                    fallback_sources.append("Qobuz")

                tidal_info = provider_payloads.get("tidal")
                if isinstance(tidal_info, dict):
                    e["fallback_tidal"] = tidal_info
                    fallback_sources.append("TIDAL")

                audiodb_info = provider_payloads.get("audiodb")
                if isinstance(audiodb_info, dict):
                    e["fallback_audiodb"] = audiodb_info
                    fallback_sources.append("TheAudioDB")

                lastfm_info = provider_payloads.get("lastfm")
                if isinstance(lastfm_info, dict):
                    e["fallback_lastfm"] = lastfm_info
                    fallback_sources.append("Last.fm")

                if fallback_sources and artist in state.get("scan_active_artists", {}) and e["album_id"] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                    with lock:
                        cur_resp = state["scan_active_artists"][artist]["current_album"].get("step_response", "") or ""
                        base_resp = cur_resp.split("; fallback:", 1)[0]
                        state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                            base_resp
                            + ("; fallback: " + ", ".join(fallback_sources) if fallback_sources else "")
                        )

                # Strict identity arbitration across providers when MB is unavailable.
                if not rg_info:
                    local_titles = list(tracks_edition) if tracks_edition else []
                    arbitration_prefetched = e.pop("_provider_arbitration_prefetched", None)
                    if isinstance(arbitration_prefetched, dict):
                        arbitration = arbitration_prefetched
                    else:
                        arbitration = _arbitrate_provider_identity(
                            artist_name=lookup_artist_for_provider,
                            album_title=title_raw,
                            local_track_titles=local_titles,
                            provider_payloads=provider_payloads,
                            local_tags=e.get("tags") if isinstance(e.get("tags"), dict) else {},
                            local_paths=list(e.get("ordered_paths") or []),
                            log_negative=False,
                            log_skips=False,
                        )
                        # If arbitration fails with only Last.fm (no tracklist), progressively fetch other providers.
                        if not arbitration and not scan_inline_provider_budget:
                            if USE_DISCOGS and not isinstance(provider_payloads.get("discogs"), dict):
                                _ensure_provider_payload("discogs")
                                discogs_info = provider_payloads.get("discogs")
                                if isinstance(discogs_info, dict):
                                    e["fallback_discogs"] = discogs_info
                                    if "Discogs" not in fallback_sources:
                                        fallback_sources.append("Discogs")
                                arbitration = _arbitrate_provider_identity(
                                    artist_name=lookup_artist_for_provider,
                                    album_title=title_raw,
                                    local_track_titles=local_titles,
                                    provider_payloads=provider_payloads,
                                    local_tags=e.get("tags") if isinstance(e.get("tags"), dict) else {},
                                    local_paths=list(e.get("ordered_paths") or []),
                                    log_negative=False,
                                    log_skips=False,
                                )
                            if (not arbitration) and USE_ITUNES and not isinstance(provider_payloads.get("itunes"), dict):
                                _ensure_provider_payload("itunes")
                                itunes_info = provider_payloads.get("itunes")
                                if isinstance(itunes_info, dict):
                                    e["fallback_itunes"] = itunes_info
                                    if "iTunes / Apple Music" not in fallback_sources:
                                        fallback_sources.append("iTunes / Apple Music")
                                arbitration = _arbitrate_provider_identity(
                                    artist_name=lookup_artist_for_provider,
                                    album_title=title_raw,
                                    local_track_titles=local_titles,
                                    provider_payloads=provider_payloads,
                                    local_tags=e.get("tags") if isinstance(e.get("tags"), dict) else {},
                                    local_paths=list(e.get("ordered_paths") or []),
                                    log_negative=False,
                                    log_skips=False,
                                )
                            if (not arbitration) and USE_DEEZER and not isinstance(provider_payloads.get("deezer"), dict):
                                _ensure_provider_payload("deezer")
                                deezer_info = provider_payloads.get("deezer")
                                if isinstance(deezer_info, dict):
                                    e["fallback_deezer"] = deezer_info
                                    if "Deezer" not in fallback_sources:
                                        fallback_sources.append("Deezer")
                                arbitration = _arbitrate_provider_identity(
                                    artist_name=lookup_artist_for_provider,
                                    album_title=title_raw,
                                    local_track_titles=local_titles,
                                    provider_payloads=provider_payloads,
                                    local_tags=e.get("tags") if isinstance(e.get("tags"), dict) else {},
                                    local_paths=list(e.get("ordered_paths") or []),
                                    log_negative=False,
                                    log_skips=False,
                                )
                            if (not arbitration) and USE_BANDCAMP and not isinstance(provider_payloads.get("bandcamp"), dict):
                                _ensure_provider_payload("bandcamp")
                                bandcamp_info = provider_payloads.get("bandcamp")
                                if isinstance(bandcamp_info, dict):
                                    e["fallback_bandcamp"] = bandcamp_info
                                    if "Bandcamp" not in fallback_sources:
                                        fallback_sources.append("Bandcamp")
                                arbitration = _arbitrate_provider_identity(
                                    artist_name=lookup_artist_for_provider,
                                    album_title=title_raw,
                                    local_track_titles=local_titles,
                                    provider_payloads=provider_payloads,
                                    local_tags=e.get("tags") if isinstance(e.get("tags"), dict) else {},
                                    local_paths=list(e.get("ordered_paths") or []),
                                    log_negative=False,
                                    log_skips=False,
                                )
                            for _provider_key, _enabled, _label, _fallback_key in (
                                ("spotify", USE_SPOTIFY, "Spotify", "fallback_spotify"),
                                ("qobuz", USE_QOBUZ, "Qobuz", "fallback_qobuz"),
                                ("tidal", USE_TIDAL, "TIDAL", "fallback_tidal"),
                                ("audiodb", bool(THEAUDIODB_API_KEY), "TheAudioDB", "fallback_audiodb"),
                            ):
                                if arbitration or (not _enabled) or isinstance(provider_payloads.get(_provider_key), dict):
                                    continue
                                _ensure_provider_payload(_provider_key)
                                _payload = provider_payloads.get(_provider_key)
                                if isinstance(_payload, dict):
                                    e[_fallback_key] = _payload
                                    if _label not in fallback_sources:
                                        fallback_sources.append(_label)
                                arbitration = _arbitrate_provider_identity(
                                    artist_name=lookup_artist_for_provider,
                                    album_title=title_raw,
                                    local_track_titles=local_titles,
                                    provider_payloads=provider_payloads,
                                    local_tags=e.get("tags") if isinstance(e.get("tags"), dict) else {},
                                    local_paths=list(e.get("ordered_paths") or []),
                                    log_negative=False,
                                    log_skips=False,
                                )
                            # Refresh UI step response to reflect newly used fallback sources.
                            if fallback_sources and artist in state.get("scan_active_artists", {}) and e["album_id"] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                with lock:
                                    cur_resp = state["scan_active_artists"][artist]["current_album"].get("step_response", "") or ""
                                    base_resp = cur_resp.split("; fallback:", 1)[0]
                                    state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                                        base_resp
                                        + ("; fallback: " + ", ".join(fallback_sources) if fallback_sources else "")
                                    )
                            if not arbitration and provider_payloads:
                                arbitration = _arbitrate_provider_identity(
                                    artist_name=lookup_artist_for_provider,
                                    album_title=title_raw,
                                    local_track_titles=local_titles,
                                    provider_payloads=provider_payloads,
                                    local_tags=e.get("tags") if isinstance(e.get("tags"), dict) else {},
                                    local_paths=list(e.get("ordered_paths") or []),
                                    log_negative=True,
                                    log_skips=True,
                                )
                    if arbitration:
                        chosen_provider = str(arbitration.get("provider") or "").strip().lower()
                        chosen_payload = arbitration.get("payload") if isinstance(arbitration.get("payload"), dict) else {}
                        e["_strict_provider_payload"] = chosen_payload
                        e["_strict_provider"] = chosen_provider
                        e["provider_identity_soft_match"] = bool(arbitration.get("soft_match_verified"))
                        e["provider_identity_soft_reason"] = str(arbitration.get("soft_match_reason") or "").strip()
                        e["provider_identity_soft_match_provider"] = chosen_provider
                        e["provider_identity_confidence"] = float(arbitration.get("confidence") or 0.0)
                        e["provider_identity_confidence_tier"] = str(arbitration.get("confidence_tier") or "").strip()
                        e["provider_identity_confidence_reason"] = str(arbitration.get("confidence_reason") or "").strip()
                        e["provider_identity_match_explanation"] = arbitration.get("match_explanation") if isinstance(arbitration.get("match_explanation"), dict) else {}
                        e["primary_metadata_source"] = chosen_provider
                        e["metadata_source"] = chosen_provider
                        if e.get("provider_identity_soft_match"):
                            e["has_identity"] = True
                            e["identity_provider"] = chosen_provider
                        if not isinstance(e.get("meta"), dict):
                            e["meta"] = {}
                        e["meta"]["primary_metadata_source"] = chosen_provider
                        e["meta"][PMDA_MATCH_PROVIDER_TAG] = chosen_provider
                        if chosen_provider == "discogs":
                            e["discogs_release_id"] = str(
                                chosen_payload.get("release_id") or chosen_payload.get("master_id") or e.get("discogs_release_id") or ""
                            ).strip()
                            if e.get("discogs_release_id"):
                                e["meta"]["discogs_release_id"] = e["discogs_release_id"]
                        elif chosen_provider == "lastfm":
                            e["lastfm_album_mbid"] = str(
                                chosen_payload.get("mbid") or e.get("lastfm_album_mbid") or ""
                            ).strip()
                            if e.get("lastfm_album_mbid"):
                                e["meta"]["lastfm_album_mbid"] = e["lastfm_album_mbid"]
                        elif chosen_provider == "bandcamp":
                            e["bandcamp_album_url"] = str(
                                chosen_payload.get("album_url") or chosen_payload.get("url") or e.get("bandcamp_album_url") or ""
                            ).strip()
                            if e.get("bandcamp_album_url"):
                                e["meta"]["bandcamp_album_url"] = e["bandcamp_album_url"]
                        elif chosen_provider == "itunes":
                            e["itunes_collection_id"] = str(
                                chosen_payload.get("collection_id") or chosen_payload.get("id") or e.get("itunes_collection_id") or ""
                            ).strip()
                            if e.get("itunes_collection_id"):
                                e["meta"]["itunes_collection_id"] = e["itunes_collection_id"]
                        elif chosen_provider == "deezer":
                            e["deezer_album_id"] = str(
                                chosen_payload.get("album_id") or chosen_payload.get("id") or e.get("deezer_album_id") or ""
                            ).strip()
                            if e.get("deezer_album_id"):
                                e["meta"]["deezer_album_id"] = e["deezer_album_id"]
                        elif chosen_provider == "spotify":
                            e["spotify_album_id"] = str(
                                chosen_payload.get("album_id") or chosen_payload.get("id") or e.get("spotify_album_id") or ""
                            ).strip()
                            if e.get("spotify_album_id"):
                                e["meta"]["spotify_album_id"] = e["spotify_album_id"]
                        elif chosen_provider == "qobuz":
                            e["qobuz_album_id"] = str(
                                chosen_payload.get("album_id") or chosen_payload.get("id") or e.get("qobuz_album_id") or ""
                            ).strip()
                            if e.get("qobuz_album_id"):
                                e["meta"]["qobuz_album_id"] = e["qobuz_album_id"]
                        elif chosen_provider == "tidal":
                            e["tidal_album_id"] = str(
                                chosen_payload.get("album_id") or chosen_payload.get("id") or e.get("tidal_album_id") or ""
                            ).strip()
                            if e.get("tidal_album_id"):
                                e["meta"]["tidal_album_id"] = e["tidal_album_id"]
                        elif chosen_provider == "audiodb":
                            e["audiodb_album_id"] = str(
                                chosen_payload.get("album_id") or chosen_payload.get("idAlbum") or e.get("audiodb_album_id") or ""
                            ).strip()
                            if e.get("audiodb_album_id"):
                                e["meta"]["audiodb_album_id"] = e["audiodb_album_id"]

                        provider_tracklist = chosen_payload.get("tracklist") if isinstance(chosen_payload, dict) else None
                        # Track-count hint for incomplete detection when MB isn't available.
                        # This allows detecting tail-truncated albums even when there are no index gaps.
                        if isinstance(provider_tracklist, list) and provider_tracklist:
                            try:
                                e["_expected_track_count"] = int(len(provider_tracklist))
                            except Exception:
                                pass
                        if local_titles and isinstance(provider_tracklist, list) and provider_tracklist:
                            track_min = getattr(sys.modules[__name__], "TRACKLIST_MATCH_MIN", 0.8)
                            score = _crosscheck_tracklist(local_titles, provider_tracklist)
                            if score >= track_min:
                                e["mb_submission_payload"] = _prepare_mb_submission_payload(
                                    chosen_payload.get("artist_name") or chosen_payload.get("artist") or artist,
                                    chosen_payload.get("title") or chosen_payload.get("album") or "",
                                    chosen_payload.get("year") or "",
                                    provider_tracklist,
                                    chosen_provider,
                                )
                        logging.info(
                            "[Artist %s] Edition %s: provider identity accepted via %s (confidence=%.2f, source=%s, title=%.2f artist=%.2f track=%.2f)",
                            artist,
                            e.get("album_id"),
                            chosen_provider,
                            float(arbitration.get("confidence") or 0.0),
                            arbitration.get("confidence_source") or "heuristic",
                            float(arbitration.get("title_score") or 0.0),
                            float(arbitration.get("artist_score") or 0.0),
                            float(arbitration.get("track_score") or 0.0),
                        )
                    else:
                        e["_match_reject_reason"] = "provider arbitration rejected all candidates"
                    # Last resort in provider chain: if arbitration still produced no match,
                    # allow a strict Last.fm MBID lift.
                    if not rg_info:
                        lastfm_info = provider_payloads.get("lastfm")
                        if isinstance(lastfm_info, dict):
                            lfm_mbid = (lastfm_info.get("mbid") or "").strip()
                            if lfm_mbid:
                                try:
                                    rg_info, _ = fetch_mb_release_group_info(lfm_mbid)
                                    if rg_info:
                                        strict_ok_src, strict_reason_src = _strict_identity_match_details(
                                            local_artist=lookup_artist_for_provider,
                                            local_title=title_raw,
                                            candidate_artist=lastfm_info.get("artist") or lastfm_info.get("artist_name") or "",
                                            candidate_title=lastfm_info.get("title") or lastfm_info.get("album") or "",
                                        )
                                        strict_ok_mb, strict_reason_mb = _strict_identity_match_details(
                                            local_artist=lookup_artist_for_provider,
                                            local_title=title_raw,
                                            candidate_artist=_extract_mb_artist_names(rg_info),
                                            candidate_title=rg_info.get("title") or "",
                                        )
                                        if not strict_ok_src or not strict_ok_mb:
                                            e["_match_reject_reason"] = (
                                                f"Last.fm MBID rejected (source={strict_reason_src}; mb={strict_reason_mb})"
                                            )
                                            log_mb(
                                                "Album %s – \"%s\": Last.fm MBID %s rejected (source=%s; mb=%s)",
                                                artist,
                                                title_raw,
                                                lfm_mbid,
                                                strict_reason_src,
                                                strict_reason_mb,
                                            )
                                            raise RuntimeError("strict identity mismatch")
                                        e["rg_info"] = rg_info
                                        e["musicbrainz_id"] = lfm_mbid
                                        e["rg_info_source"] = "lastfm_fallback"
                                        e["primary_metadata_source"] = "lastfm"
                                        e["lastfm_album_mbid"] = lfm_mbid
                                        if not isinstance(e.get("meta"), dict):
                                            e["meta"] = {}
                                        e["meta"]["primary_metadata_source"] = "lastfm"
                                        e["meta"][PMDA_MATCH_PROVIDER_TAG] = "lastfm"
                                        e["meta"]["lastfm_album_mbid"] = lfm_mbid
                                        with lock:
                                            if artist in state.get("scan_active_artists", {}) and e["album_id"] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                                state["scan_active_artists"][artist]["current_album"]["status_details"] = "MusicBrainz ID from Last.fm"
                                                state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                                    f"Last.fm: MusicBrainz release group (id: {lfm_mbid})"
                                                )
                                        logging.debug(
                                            "[Artist %s] Edition %s MB ID from Last.fm mbid (last resort): %s",
                                            artist,
                                            e["album_id"],
                                            lfm_mbid,
                                        )
                                except RuntimeError:
                                    pass
                                except Exception:
                                    pass
            use_hint_for_strict_validation = _identity_hint_safe_for_provider_lookup(
                e,
                default_artist=str(artist or "").strip(),
                default_title=str(title_raw or "").strip(),
            )
            strict_ref_artist = (
                str(e.get("_lookup_artist_name") or artist or "").strip()
                if use_hint_for_strict_validation
                else str(artist or "").strip()
            ) or str(artist or "").strip()
            strict_ref_title = (
                str(e.get("_lookup_album_title") or title_raw or "").strip()
                if use_hint_for_strict_validation
                else str(title_raw or "").strip()
            ) or str(title_raw or "")
            strict_verdict = _strict_validate_edition_match(
                artist_name=strict_ref_artist,
                album_title=strict_ref_title,
                edition=e,
            )
            e["strict_match_verified"] = bool(strict_verdict.get("strict_match_verified"))
            e["strict_match_provider"] = str(strict_verdict.get("strict_match_provider") or "").strip()
            e["strict_reject_reason"] = str(strict_verdict.get("strict_reject_reason") or "").strip()
            e["strict_tracklist_score"] = float(strict_verdict.get("strict_tracklist_score") or 0.0)
            if e.get("strict_match_verified"):
                strict_provider = str(e.get("strict_match_provider") or "").strip()
                verified_artist_name = str(strict_verdict.get("strict_artist_name") or "").strip()
                verified_album_title = str(strict_verdict.get("strict_album_title") or "").strip()
                if verified_artist_name:
                    e["_verified_artist_name"] = verified_artist_name
                if verified_album_title:
                    e["_verified_album_title"] = _sanitize_album_title_display(verified_album_title)
                if strict_provider:
                    e["primary_metadata_source"] = strict_provider
                    e["metadata_source"] = strict_provider
                    e["has_identity"] = True
                    e["identity_provider"] = strict_provider
                    if not isinstance(e.get("meta"), dict):
                        e["meta"] = {}
                    e["meta"]["primary_metadata_source"] = strict_provider
                    e["meta"][PMDA_MATCH_PROVIDER_TAG] = strict_provider
                    _scan_record_provider_match(strict_provider)
            else:
                soft_provider = _normalize_identity_provider(
                    str(
                        e.get("identity_provider")
                        or e.get("primary_metadata_source")
                        or e.get("metadata_source")
                        or ""
                    )
                )
                has_soft_identity = _edition_soft_identity_survives_strict_reject(e)
                if has_soft_identity:
                    e["has_identity"] = True
                    if soft_provider:
                        e["identity_provider"] = soft_provider
                        e["metadata_source"] = soft_provider
                        if not e.get("primary_metadata_source"):
                            e["primary_metadata_source"] = soft_provider
                    if not isinstance(e.get("meta"), dict):
                        e["meta"] = {}
                    if soft_provider:
                        e["meta"]["primary_metadata_source"] = soft_provider
                        e["meta"][PMDA_MATCH_PROVIDER_TAG] = soft_provider
                    _scan_record_provider_match(soft_provider)
                    e["_match_reject_reason"] = (
                        e.get("strict_reject_reason")
                        or e.get("_match_reject_reason")
                        or "strict_match_failed"
                    )
                else:
                    e["_match_reject_reason"] = e.get("strict_reject_reason") or e.get("_match_reject_reason") or "strict_match_failed"
                    _strict_clear_identity_on_reject(e)
                with lock:
                    if artist in state.get("scan_active_artists", {}) and e["album_id"] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                        if has_soft_identity:
                            state["scan_active_artists"][artist]["current_album"]["status_details"] = "Soft identity kept (strict mutation blocked)"
                            state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                f"Soft match kept; strict gate={e.get('strict_reject_reason') or 'strict_match_failed'}"
                            )
                        else:
                            state["scan_active_artists"][artist]["current_album"]["status_details"] = "Strict match rejected"
                            state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                f"Rejected: {e.get('strict_reject_reason') or 'strict_match_failed'}"
                            )
            # Increment MB-done count for this edition (whether we found rg_info or not)
            with lock:
                state["scan_mb_done_count"] = state.get("scan_mb_done_count", 0) + 1
                state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1
                if artist in state.get("scan_active_artists", {}):
                    state["scan_active_artists"][artist]["albums_processed"] = max(
                        int(state["scan_active_artists"][artist].get("albums_processed") or 0),
                        match_idx,
                    )
                    current_album_live = state["scan_active_artists"][artist].get("current_album")
                    if isinstance(current_album_live, dict) and int(current_album_live.get("album_id") or 0) == int(e.get("album_id") or 0):
                        current_album_live["album_index"] = match_idx
                        current_album_live["album_total"] = total_matching_albums
                        current_album_live["status_updated_at"] = time.time()
            # Also store MBID from rg_info if not already set
            if e.get('rg_info') and 'musicbrainz_id' not in e and isinstance(e['rg_info'], dict) and 'id' in e['rg_info']:
                e['musicbrainz_id'] = e['rg_info']['id']
                e['musicbrainz_type'] = 'release-group'

            # Detect broken album (missing tracks).
            # Prefer MB track_count when present; otherwise fall back to provider tracklist size when available.
            mb_hint = e.get("rg_info") if isinstance(e.get("rg_info"), dict) else None
            if not mb_hint:
                try:
                    exp = int(e.get("_expected_track_count") or 0)
                except Exception:
                    exp = 0
                if exp > 0:
                    mb_hint = {"track_count": exp, "source": "provider_tracklist"}
            first_track_tags = None
            try:
                folder_for_tags = Path(str(e.get("folder") or ""))
                first_audio = next((p for p in folder_for_tags.rglob("*") if AUDIO_RE.search(p.name)), None)
                if first_audio and first_audio.exists():
                    first_track_tags = extract_tags(first_audio) or {}
            except Exception:
                first_track_tags = None
            is_broken, expected_count, actual_count, missing_indices = detect_broken_album(
                db_conn,
                e["album_id"],
                e["tracks"],
                mb_hint,
                tags=first_track_tags,
            )
            strict_reject_reason = str(e.get("strict_reject_reason") or "").strip().lower()
            assessment = _build_incomplete_assessment(
                edition=e,
                tags=first_track_tags,
                mb_hint=mb_hint,
                is_broken_detected=bool(is_broken),
                expected_track_count=_parse_int_loose(expected_count, 0),
                actual_track_count=int(actual_count or 0),
                missing_indices=list(missing_indices or []),
                strict_reject_reason=str(strict_reject_reason or ""),
            )
            e["_incomplete_assessment"] = dict(assessment or {})
            e["_incomplete_review_candidate"] = bool((assessment or {}).get("needs_manual_review"))
            e["is_broken"] = bool((assessment or {}).get("mark_broken"))
            e["expected_track_count"] = int((assessment or {}).get("expected_track_count") or 0)
            e["actual_track_count"] = int((assessment or {}).get("actual_track_count") or 0)
            e["missing_indices"] = list((assessment or {}).get("missing_indices") or [])
            if e["is_broken"]:
                logging.warning(
                    "[Artist %s] Album %s (%s) is broken: %d tracks found, expected %s, gaps: %s",
                    artist,
                    e["album_id"],
                    e.get("title_raw", ""),
                    e.get("actual_track_count", actual_count),
                    e.get("expected_track_count") or "unknown",
                    e.get("missing_indices") or [],
                )
            _log_release_match_outcome(e)
        _ai_usage_set_album_context(album_id=None, album_artist="", album_title="")
        mb_lookup_time = time.perf_counter() - mb_start
        # --- MusicBrainz enrichment summary ---
        direct = sum(1 for e in editions if 'rg_info' in e and e.get('rg_info_source') in id_tags)
        fallback = sum(1 for e in editions if 'rg_info' in e and e.get('rg_info_source') == 'fallback')
        incremental_skipped = sum(1 for e in editions if e.get("rg_info_source") == "incremental_skip")
        missing = sum(1 for e in editions if 'rg_info' not in e)
        log_mb(
            "[Artist %s] enrichment summary: direct=%d, fallback=%d, fast-skip=%d, missing=%d "
            "(elapsed %.2fs)",
            artist,
            direct,
            fallback,
            incremental_skipped,
            missing,
            mb_lookup_time,
        )
        _mark_classical_sibling_incompletes(editions, artist_name=artist)

    # Detect and collapse Box Set discs (skip as duplicates)
    from collections import defaultdict
    box_set_groups = defaultdict(list)
    for e in editions:
        sec_types = e.get('rg_info', {}).get('secondary_types', [])
        if 'Box Set' in sec_types:
            parent_folder = e['folder'].parent
            box_set_groups[parent_folder].append(e)

    if box_set_groups:
        for parent_folder, items in box_set_groups.items():
            log_mb(
                "Box Set detected at %s with %d disc(s) – skipping duplicate detection for these discs.",
                parent_folder,
                len(items),
            )
        # Exclude all Box Set disc folders from further duplicate grouping
        editions = [e for e in editions if e['folder'].parent not in box_set_groups]
    # --- NO FILES HANDLING ---
    if editions:
        # Reset streak on success
        no_file_streak_global = 0
        ok_msg = (
            f"[Artist {artist}] FOUND {len(editions)} valid file editions on filesystem "
            f"for {len(album_ids)} albums. PATH_MAP and volume bindings appear correct!"
        )
        log_path(ok_msg)
    else:
        # No valid editions found
        no_file_streak_global += 1
        if skip_count == len(album_ids):
            logging.info(f"[Artist {artist}] All {skip_count} albums skipped due to SKIP_FOLDERS {SKIP_FOLDERS}")
            return [], {"ai_used": 0, "mb_used": 0}, []
        else:
            logger = logging.getLogger()
            logger.error(f"[Artist {artist}] FOUND 0 valid file editions on filesystem! Checked SKIP_FOLDERS: {SKIP_FOLDERS}")
            notify_discord = globals().get("notify_discord", None)
            if notify_discord:
                notify_discord(f"No files found for {artist}.")
            global popup_displayed
            if no_file_streak_global >= NO_FILE_THRESHOLD:
                if not popup_displayed:
                    gui.display_popup(
                        f"PMDA didn't find any files for {NO_FILE_THRESHOLD} artists in a row. "
                        "Aborting scan. Files appear unreachable from inside the container; "
                        "please check your volume bindings."
                    )
                    popup_displayed = True
                scan_should_stop.set()
                return [], {"ai_used": 0, "mb_used": 0}, []
            # Below threshold, do not show repeated popups -- let scan continue or fail silently
            return [], {"ai_used": 0, "mb_used": 0}, []
    for e in editions:
        logging.debug(
            f"[Artist {artist}] Edition {e['album_id']}: "
            f"norm='{e['album_norm']}', tracks={len(e['tracks'])}, dur_ms={e['dur']}, "
            f"files={e['file_count']}, fmt_score={e['fmt_score']}, "
            f"br={e['br']}, sr={e['sr']}, bd={e['bd']}"
        )
    # Map resolved folder path -> edition (for same-folder duplicate groups later)
    folder_to_edition: dict[str, dict] = {}
    for e in editions:
        try:
            k = str(Path(e["folder"]).resolve())
        except Exception:
            k = str(e["folder"])
        folder_to_edition[k] = e
    # album_id -> title (for same-folder loser labels when db_conn is None, e.g. Files backend)
    album_id_to_title: dict[int, str] = {
        e["album_id"]: (e.get("title_raw") or e.get("plex_title") or f"Album {e['album_id']}")
        for e in editions
    }
    # --- Dupe Detection v2 grouping: provider IDs + signatures + loose title + similarity ---
    from collections import defaultdict

    # In changed-only scans, we may inject context-only editions from the published cache
    # to detect dupes against older albums. Those context editions should not affect scan
    # health stats nor be persisted into scan_editions (they use run-local album_id).
    all_editions_for_stats = [e for e in editions if not bool(e.get("context_only"))]

    dupe_report: dict = {
        "version": 2,
        "groups_total": 0,
        "groups_needs_ai": 0,
        "groups_by_signal": {},
        "rejected_by_reason": {},
    }

    def _dr_inc(bucket: str, key: str, n: int = 1) -> None:
        if not key:
            return
        d = dupe_report.get(bucket)
        if not isinstance(d, dict):
            d = {}
            dupe_report[bucket] = d
        k = str(key)
        d[k] = int(d.get(k) or 0) + int(n or 0)

    def _folder_key(e: dict) -> str:
        folder = e.get("folder")
        if not folder:
            return ""
        try:
            return str(Path(folder).resolve())
        except Exception:
            return str(folder)

    def _all_same_folder(ed_list: list[dict]) -> bool:
        keys = {_folder_key(e) for e in ed_list if e.get("folder")}
        keys = {k for k in keys if k}
        return len(keys) == 1 and bool(keys)

    def _ensure_track_set(e: dict) -> set[str]:
        ts = e.get("_dupe_track_title_set")
        if isinstance(ts, set):
            return ts
        ts = _dupe_track_title_set(e.get("tracks") or [])
        e["_dupe_track_title_set"] = ts
        return ts

    def _is_classical(ed_list: list[dict]) -> bool:
        for e in ed_list:
            ctx = _classical_context_for_edition(e)
            if bool(ctx.get("is_classical")):
                return True
        return False

    def _split_classical(ed_list: list[dict]) -> list[list[dict]]:
        return _classical_cluster_same_recording(ed_list)

    def _group_pair_metrics(ed_list: list[dict]) -> tuple[float, float]:
        # Return (max_jaccard, min_track_ratio) across all pairs in this group.
        n = len(ed_list)
        if n < 2:
            return (1.0, 1.0)
        sets = [_ensure_track_set(e) for e in ed_list]
        tracks = [e.get("tracks") or [] for e in ed_list]
        max_j = 0.0
        min_ratio = 1.0
        for i in range(n):
            for j in range(i + 1, n):
                ratio = _dupe_track_count_ratio(tracks[i], tracks[j])
                if ratio < min_ratio:
                    min_ratio = ratio
                jac = _dupe_jaccard(sets[i], sets[j])
                if jac > max_j:
                    max_j = jac
        return (max_j, min_ratio)

    def _maybe_audio_overlap_max(ed_list: list[dict]) -> float:
        # Expensive: compute max chromaprint overlap across any pair in group.
        n = len(ed_list)
        if n < 2:
            return 0.0
        fps: list[set[str]] = []
        for e in ed_list:
            try:
                fps.append(_dupe_audio_fp_set_for_edition(e))
            except Exception:
                fps.append(set())
        max_ov = 0.0
        for i in range(n):
            for j in range(i + 1, n):
                a = fps[i]
                b = fps[j]
                if not a or not b:
                    continue
                ov = (len(a & b) / max(len(a), len(b))) if max(len(a), len(b)) else 0.0
                if ov > max_ov:
                    max_ov = ov
        return max_ov

    def _live_safety_skip(ed_list: list[dict]) -> bool:
        if LIVE_DEDUPE_MODE != "safe" or len(ed_list) < 2:
            return False
        live_flags: list[bool] = []
        track_sets: list[set[str]] = []
        track_counts: list[int] = []
        for e in ed_list:
            meta = e.get("meta", {}) or {}
            title_lower = (e.get("plex_title") or e.get("title_raw") or "").lower()
            sec_types = (meta.get("musicbrainz_secondarytypes") or "").lower()
            is_live = (
                "live" in sec_types
                or " live " in f" {title_lower} "
                or " live at " in title_lower
                or " live in " in title_lower
            )
            live_flags.append(is_live)
            ts = _ensure_track_set(e)
            if ts:
                track_sets.append(ts)
            track_counts.append(len(e.get("tracks") or []))
        if not any(live_flags):
            return False
        if not all(live_flags):
            _dr_inc("rejected_by_reason", "mixed_live_nonlive")
            return True
        if len(track_sets) >= 2:
            union = set().union(*track_sets)
            inter = set.intersection(*track_sets) if track_sets else set()
            jaccard = (len(inter) / len(union)) if union else 1.0
        else:
            jaccard = 1.0
        min_tracks = min(track_counts) if track_counts else 0
        max_tracks = max(track_counts) if track_counts else 0
        ratio = (min_tracks / max_tracks) if max_tracks else 1.0
        if jaccard < 0.8 or ratio < 0.75:
            _dr_inc("rejected_by_reason", "live_low_similarity")
            return True
        return False

    # Precompute v2 identity + loose title norms for grouping.
    for e in editions:
        title_src = (e.get("title_raw") or e.get("plex_title") or e.get("album_norm") or "").strip()
        e["_dupe_title_norm_loose"] = norm_album_for_dedup_loose(title_src)
        e["_dupe_edition_tokens"] = _dupe_extract_edition_tokens(title_src)
        e["_dupe_mb_rg"] = _dupe_get_mb_release_group_id(e)
        e["_dupe_mb_rel"] = _dupe_get_mb_release_id(e)
        e["_dupe_discogs"] = _dupe_get_discogs_id(e)
        e["_dupe_lastfm"] = _dupe_get_lastfm_mbid(e)
        e["_dupe_bandcamp"] = _dupe_get_bandcamp_url(e)

    out: list[dict] = []
    used_ids: set[int] = set()
    feedback_pairs = _dupe_load_feedback_pairs_for_artist(artist)

    # Track statistics
    mb_used_count = 0  # Editions enriched with provider identity
    ai_used_count = 0  # Groups where a previous AI decision was reused immediately
    audio_cache_hits = sum(1 for e in editions if e.get("audio_cache_hit", False))
    audio_cache_misses = len(editions) - audio_cache_hits
    mb_cache_hits = 0
    mb_cache_misses = 0

    if USE_MUSICBRAINZ:
        mb_used_count = sum(
            1
            for e in editions
            if e.get("musicbrainz_id") or (isinstance(e.get("rg_info"), dict) and e["rg_info"].get("id"))
        )
        mb_cache_hits = sum(1 for e in editions if (("rg_info" in e) and bool(e.get("mb_cache_hit", False))))
        mb_cache_misses = sum(1 for e in editions if (("rg_info" in e) and (not bool(e.get("mb_cache_hit", False)))))

    def _edition_track_count_for_dupe(e: dict) -> int:
        tracks = e.get("tracks") or []
        if isinstance(tracks, list) and tracks:
            return int(len(tracks))
        for key in ("track_count", "actual_track_count", "expected_track_count"):
            try:
                parsed = int(e.get(key) or 0)
            except Exception:
                parsed = 0
            if parsed > 0:
                return parsed
        return 0

    def _edition_total_duration_for_dupe(e: dict) -> int:
        try:
            parsed = int(e.get("dur") or 0)
        except Exception:
            parsed = 0
        if parsed > 0:
            return parsed
        total = 0
        for tr in (e.get("tracks") or []):
            try:
                total += int(getattr(tr, "dur", 0) or (tr.get("dur") if isinstance(tr, dict) else 0) or 0)
            except Exception:
                continue
        return max(0, int(total))

    def _group_has_exact_provider_trackcount_signal(group: list[dict]) -> bool:
        if len(group or []) < 2:
            return False
        shared_provider_id = False
        provider_sets = [
            {e.get("_dupe_mb_rg") for e in group if e.get("_dupe_mb_rg")},
            {e.get("_dupe_mb_rel") for e in group if e.get("_dupe_mb_rel")},
            {e.get("_dupe_discogs") for e in group if e.get("_dupe_discogs")},
            {e.get("_dupe_lastfm") for e in group if e.get("_dupe_lastfm")},
            {e.get("_dupe_bandcamp") for e in group if e.get("_dupe_bandcamp")},
        ]
        for values in provider_sets:
            if len(values) == 1:
                shared_provider_id = True
                break
        if not shared_provider_id:
            return False
        counts = {_edition_track_count_for_dupe(e) for e in group if _edition_track_count_for_dupe(e) > 0}
        if len(counts) != 1:
            return False
        durations = [_edition_total_duration_for_dupe(e) for e in group if _edition_total_duration_for_dupe(e) > 0]
        if len(durations) >= 2:
            lo = min(durations)
            hi = max(durations)
            if lo <= 0 or ((hi - lo) / float(hi)) > 0.03:
                return False
        return True

    _CLASSICAL_SAME_RECORDING_REASONS = {
        "same_recording_exact_structure",
        "same_recording_missing_performance_context",
        "same_recording_subset_structure",
        "same_recording_similarity",
    }

    def _expand_classical_same_recording_group(group: list[dict], best: dict | None) -> list[dict]:
        if not group or not isinstance(best, dict) or not _is_classical(group):
            return list(group or [])
        expanded = list(group)
        present_ids = {
            _parse_int_loose((e or {}).get("album_id"), 0)
            for e in expanded
            if isinstance(e, dict)
        }
        present_ids.discard(0)
        best_ctx = _classical_context_for_edition(best)
        best_count = int(best_ctx.get("track_count") or 0) or _edition_track_count_for_dupe(best)
        added = 0
        for candidate in editions:
            if not isinstance(candidate, dict):
                continue
            aid = _parse_int_loose(candidate.get("album_id"), 0)
            if aid <= 0 or aid in present_ids or aid in used_ids:
                continue
            same_recording, same_reason = _classical_same_recording_pair_details(best, candidate)
            if (not same_recording) or same_reason not in _CLASSICAL_SAME_RECORDING_REASONS:
                continue
            cand_ctx = _classical_context_for_edition(candidate)
            cand_count = int(cand_ctx.get("track_count") or 0) or _edition_track_count_for_dupe(candidate)
            if best_count > 0 and cand_count > 0 and cand_count < best_count and (best_count - cand_count) <= 2:
                if not bool(candidate.get("is_broken")):
                    candidate["is_broken"] = True
                    candidate["expected_track_count"] = best_count
                    candidate["actual_track_count"] = cand_count
                    candidate["missing_indices"] = _edition_missing_indices_exact(candidate, best_count, cand_count)
                    candidate["_classical_sibling_incomplete"] = True
                    logging.warning(
                        "[Artist %s] Album %s (%s) forced incomplete from classical winner family: actual=%s expected=%s missing=%s",
                        artist,
                        candidate.get("album_id"),
                        candidate.get("title_raw") or candidate.get("plex_title") or "",
                        cand_count,
                        best_count,
                        list(candidate.get("missing_indices") or [])[:24],
                    )
            expanded.append(candidate)
            present_ids.add(aid)
            added += 1
        if added > 0:
            logging.info(
                "[Artist %s] classical same-recording expansion: winner=%s added=%s sibling(s)",
                artist,
                best.get("album_id"),
                added,
            )
        return expanded

    def _append_group(
        ed_list: list[dict],
        *,
        fuzzy: bool,
        signal: str,
        evidence: list[str] | None = None,
    ) -> None:
        nonlocal ai_used_count
        if not ed_list or len(ed_list) < 2:
            return
        # Never regroup already-used editions.
        ed_list = [e for e in ed_list if e.get("album_id") not in used_ids]
        if len(ed_list) < 2:
            return
        if _all_same_folder(ed_list):
            _dr_inc("rejected_by_reason", "same_folder")
            return
        if _dupe_is_multidisc_sibling_group(ed_list):
            _dr_inc("rejected_by_reason", "multidisc_siblings")
            return
        if _live_safety_skip(ed_list):
            return

        # Classical safety: split by year + first-track duration threshold.
        subgroups = _split_classical(ed_list) if _is_classical(ed_list) else [ed_list]

        for sg in subgroups:
            if not sg or len(sg) < 2:
                continue
            sg = [e for e in sg if e.get("album_id") not in used_ids]
            if len(sg) < 2:
                continue
            if _is_classical(sg):
                _mark_classical_sibling_incompletes(sg, artist_name=artist)

            if (
                signal not in {"provider_id", "track_sig", "audio_fp", "user_label"}
                and not editions_share_confident_signal(sg)
                and not (_is_classical(sg) and _classical_group_is_same_recording_confident(sg))
            ):
                _dr_inc("rejected_by_reason", "low_confidence")
                continue

            chips = list(evidence or [])
            max_jac, min_ratio = _group_pair_metrics(sg)
            chips.append(f"TRACKS_JACCARD_MAX:{max_jac:.2f}")
            chips.append(f"TRACKS_RATIO_MIN:{min_ratio:.2f}")

            tok_union: set[str] = set()
            for e in sg:
                toks = e.get("_dupe_edition_tokens") or []
                if isinstance(toks, list):
                    for t in toks:
                        if t:
                            tok_union.add(str(t))
            if tok_union:
                chips.append("TOKENS:" + ",".join(sorted(tok_union))[:200])

            no_move = False
            manual_review = False

            # Feedback loop: if the user explicitly marked any pair as NOT a dupe, never auto-move.
            if feedback_pairs:
                try:
                    folders = [_folder_key(e) for e in sg]
                except Exception:
                    folders = []
                user_dupe = False
                user_not_dupe = False
                for i in range(len(folders)):
                    for j in range(i + 1, len(folders)):
                        key = _dupe_feedback_pair_key(folders[i], folders[j])
                        lab = feedback_pairs.get(key)
                        if lab == "dupe":
                            user_dupe = True
                        elif lab in {"not_dupe", "notdupe", "no"}:
                            user_not_dupe = True
                if user_dupe:
                    chips.append("USER_LABEL:DUPE")
                if user_not_dupe:
                    chips.append("USER_LABEL:NOT_DUPE")
                    manual_review = True
                    no_move = True
                    _dr_inc("rejected_by_reason", "user_not_dupe_conflict")
            if signal == "track_sig" and _dupe_track_sig_title_conflict(sg):
                no_move = True
                manual_review = True
                chips.append("TRACK_SIG_TITLE_DIVERGENCE")
                _dr_inc("rejected_by_reason", "track_sig_title_divergence")

            if signal == "provider_id":
                provider_title_conflict = _dupe_provider_id_title_conflict(
                    sg,
                    max_jaccard=max_jac,
                    min_track_ratio=min_ratio,
                )
                if provider_title_conflict:
                    no_move = True
                    manual_review = True
                    chips.append("PROVIDER_ID_TITLE_DIVERGENCE")
                    _dr_inc("rejected_by_reason", "provider_id_title_divergence")
                # Provider IDs are strong, but wrong tags can collide. If coherence is extremely low,
                # keep for review but do not auto-move.
                if (not provider_title_conflict) and max_jac < 0.22 and min_ratio < 0.35:
                    exact_provider_trackcount_safe = _group_has_exact_provider_trackcount_signal(sg)
                    audio_ov = 0.0
                    try:
                        audio_ov = _maybe_audio_overlap_max(sg)
                    except Exception:
                        audio_ov = 0.0
                    if audio_ov:
                        chips.append(f"AUDIO_FP_MAX:{audio_ov:.2f}")
                    if exact_provider_trackcount_safe:
                        chips.append("PROVIDER_ID_EXACT_TRACKCOUNT")
                    elif audio_ov < 0.87:
                        no_move = True
                        manual_review = True
                        chips.append("COHERENCE_LOW")
                        _dr_inc("rejected_by_reason", "provider_low_coherence")

            best = choose_best(sg, defer_ai=True)
            if best is None:
                out.append(
                    {
                        "artist": artist,
                        "album_id": None,
                        "editions": sg,
                        "fuzzy": bool(fuzzy),
                        "needs_ai": True,
                        "dupe_signal": signal,
                        "dupe_evidence": chips,
                        "no_move": bool(no_move),
                        "manual_review": bool(manual_review),
                    }
                )
                used_ids.update(e.get("album_id") for e in sg if e.get("album_id") is not None)
                dupe_report["groups_total"] = int(dupe_report.get("groups_total") or 0) + 1
                dupe_report["groups_needs_ai"] = int(dupe_report.get("groups_needs_ai") or 0) + 1
                _dr_inc("groups_by_signal", signal)
                continue

            original_len = len(sg)
            if _is_classical(sg):
                sg = _expand_classical_same_recording_group(sg, best)
                refined_best = choose_best(sg, defer_ai=True)
                if refined_best is not None:
                    best = refined_best

            # Never keep broken editions as duplicate losers: they belong to incomplete handling.
            losers = [
                e
                for e in sg
                if e.get("album_id") != best.get("album_id") and not bool(e.get("is_broken", False))
            ]
            if not losers:
                used_ids.update(e.get("album_id") for e in sg if e.get("album_id") is not None)
                continue

            if _is_classical(sg) and len(sg) > original_len:
                chips.append(f"CLASSICAL_SATELLITES:{len(sg) - original_len}")
            best["dupe_evidence"] = chips
            if best.get("used_ai", False):
                ai_used_count += 1

            out.append(
                {
                    "artist": artist,
                    "album_id": best.get("album_id"),
                    "best": best,
                    "losers": losers,
                    "fuzzy": bool(fuzzy),
                    "needs_ai": False,
                    "dupe_signal": signal,
                    "dupe_evidence": chips,
                    "no_move": bool(no_move),
                    "manual_review": bool(manual_review),
                }
            )
            used_ids.update(e.get("album_id") for e in sg if e.get("album_id") is not None)
            dupe_report["groups_total"] = int(dupe_report.get("groups_total") or 0) + 1
            _dr_inc("groups_by_signal", signal)

    # Phase 0: user feedback labels (force dupe grouping).
    if feedback_pairs and len(editions) >= 2:
        try:
            n = len(editions)
            parent = list(range(n))
            rank = [0] * n

            def _find_fb(i: int) -> int:
                while parent[i] != i:
                    parent[i] = parent[parent[i]]
                    i = parent[i]
                return i

            def _union_fb(i: int, j: int) -> None:
                ri = _find_fb(i)
                rj = _find_fb(j)
                if ri == rj:
                    return
                if rank[ri] < rank[rj]:
                    parent[ri] = rj
                elif rank[ri] > rank[rj]:
                    parent[rj] = ri
                else:
                    parent[rj] = ri
                    rank[ri] += 1

            folder_to_idxs: dict[str, list[int]] = defaultdict(list)
            for i, e in enumerate(editions):
                fk = _folder_key(e)
                if fk:
                    folder_to_idxs[fk].append(i)

            for (fa, fb), lab in (feedback_pairs or {}).items():
                if str(lab or "").strip().lower() != "dupe":
                    continue
                a = (fa or "").strip()
                b = (fb or "").strip()
                if not a or not b:
                    continue
                for ia in folder_to_idxs.get(a, [])[:5]:
                    for ib in folder_to_idxs.get(b, [])[:5]:
                        _union_fb(ia, ib)

            comps: dict[int, list[int]] = defaultdict(list)
            for i in range(n):
                comps[_find_fb(i)].append(i)
            for idxs in comps.values():
                if len(idxs) < 2:
                    continue
                grp = [editions[i] for i in idxs]
                _append_group(grp, fuzzy=False, signal="user_label", evidence=["USER_DUPE_LABEL"])
        except Exception:
            logging.debug("[Artist %s] user-label dupe grouping failed", artist, exc_info=True)

    # Phase 2: group by provider IDs (high precision).
    if len(editions) >= 2:
        n = len(editions)
        parent = list(range(n))
        rank = [0] * n

        def _find(i: int) -> int:
            while parent[i] != i:
                parent[i] = parent[parent[i]]
                i = parent[i]
            return i

        def _union(i: int, j: int) -> None:
            ri = _find(i)
            rj = _find(j)
            if ri == rj:
                return
            if rank[ri] < rank[rj]:
                parent[ri] = rj
            elif rank[ri] > rank[rj]:
                parent[rj] = ri
            else:
                parent[rj] = ri
                rank[ri] += 1

        id_to_idxs: dict[str, list[int]] = defaultdict(list)
        for i, e in enumerate(editions):
            mb_rg = (e.get("_dupe_mb_rg") or "").strip()
            if mb_rg:
                id_to_idxs[f"mb_rg:{mb_rg}"].append(i)
            mb_rel = (e.get("_dupe_mb_rel") or "").strip()
            if mb_rel:
                id_to_idxs[f"mb_rel:{mb_rel}"].append(i)
            discogs = (e.get("_dupe_discogs") or "").strip()
            if discogs:
                id_to_idxs[f"discogs:{discogs}"].append(i)
            lastfm = (e.get("_dupe_lastfm") or "").strip()
            if lastfm:
                id_to_idxs[f"lastfm:{lastfm}"].append(i)
            bandcamp = (e.get("_dupe_bandcamp") or "").strip()
            if bandcamp:
                id_to_idxs[f"bandcamp:{bandcamp}"].append(i)

        for _k, idxs in id_to_idxs.items():
            if len(idxs) < 2:
                continue
            base = idxs[0]
            for j in idxs[1:]:
                _union(base, j)

        comps: dict[int, list[int]] = defaultdict(list)
        for i in range(n):
            comps[_find(i)].append(i)

        for idxs in comps.values():
            if len(idxs) < 2:
                continue
            grp = [editions[i] for i in idxs]
            # Broken editions are handled by the incomplete pipeline and must not seed provider-id dupe groups.
            grp = [e for e in grp if not bool(e.get("is_broken", False))]
            if len(grp) < 2:
                continue

            candidate_groups: list[list[dict]] = []
            if len(grp) > 10:
                by_loose: dict[str, list[dict]] = defaultdict(list)
                for e in grp:
                    by_loose[str(e.get("_dupe_title_norm_loose") or "")].append(e)
                for _lk, sub in by_loose.items():
                    if len(sub) < 2:
                        continue
                    for c in _dupe_split_editions_by_similarity(
                        sub,
                        min_jaccard=0.82,
                        min_ratio=0.75,
                        allow_audio_fp=True,
                    ):
                        if len(c) >= 2:
                            candidate_groups.append(c)
            else:
                for c in _dupe_split_editions_by_similarity(
                    grp,
                    min_jaccard=0.82,
                    min_ratio=0.75,
                    allow_audio_fp=True,
                ):
                    if len(c) >= 2:
                        candidate_groups.append(c)

            if not candidate_groups and _group_has_exact_provider_trackcount_signal(grp):
                candidate_groups.append(grp)

            for sub in candidate_groups:
                mb_rg_ids = {e.get("_dupe_mb_rg") for e in sub if e.get("_dupe_mb_rg")}
                discogs_ids = {e.get("_dupe_discogs") for e in sub if e.get("_dupe_discogs")}
                lastfm_ids = {e.get("_dupe_lastfm") for e in sub if e.get("_dupe_lastfm")}
                bandcamp_urls = {e.get("_dupe_bandcamp") for e in sub if e.get("_dupe_bandcamp")}
                evidence: list[str] = []
                if len(mb_rg_ids) == 1:
                    evidence.append(f"MB_RG:{next(iter(mb_rg_ids))}")
                if len(discogs_ids) == 1:
                    evidence.append(f"DISCOGS:{next(iter(discogs_ids))}")
                if len(lastfm_ids) == 1:
                    evidence.append(f"LASTFM_MBID:{next(iter(lastfm_ids))}")
                if len(bandcamp_urls) == 1:
                    evidence.append("BANDCAMP:" + str(next(iter(bandcamp_urls)))[:120])
                if _group_has_exact_provider_trackcount_signal(sub):
                    evidence.append("PROVIDER_ID_EXACT_TRACKCOUNT")
                _append_group(sub, fuzzy=False, signal="provider_id", evidence=evidence)

    # Phase 3: exact grouping by track signature (strong evidence; catches bad titles).
    remaining = [e for e in editions if e.get("album_id") not in used_ids]
    sig_groups: dict[tuple, list[dict]] = defaultdict(list)
    for e in remaining:
        sig = e.get("sig")
        if sig:
            sig_groups[sig].append(e)
    for ed_list in sig_groups.values():
        if len(ed_list) < 2:
            continue
        _append_group(ed_list, fuzzy=False, signal="track_sig", evidence=["SIG_MATCH"])

    # Phase 3.5: exact grouping by audio fingerprint signature (cached-only, high precision).
    remaining = [e for e in editions if e.get("album_id") not in used_ids]
    audio_groups: dict[str, list[dict]] = defaultdict(list)
    for e in remaining:
        try:
            sig = _dupe_audio_sig_for_edition(e, max_tracks=10, min_fps=3, compute_missing=False)
        except Exception:
            sig = ""
        if sig:
            audio_groups[sig].append(e)
    for sig, ed_list in audio_groups.items():
        if len(ed_list) < 2:
            continue
        _append_group(ed_list, fuzzy=False, signal="audio_fp", evidence=[f"AUDIO_SIG:{sig[:12]}"])

    # Phase 3.75: strict canonical identity grouping.
    remaining = [e for e in editions if e.get("album_id") not in used_ids]
    strict_identity_groups: dict[str, list[dict]] = defaultdict(list)
    for e in remaining:
        strict_key = _strict_album_identity_key_for_edition(
            e,
            default_artist=str(artist or "").strip(),
            default_title=str(e.get("title_raw") or e.get("album_norm") or "").strip(),
        )
        if strict_key:
            strict_identity_groups[strict_key].append(e)
    for strict_key, ed_list in strict_identity_groups.items():
        if len(ed_list) < 2:
            continue
        candidate_groups = _dupe_split_editions_by_similarity(
            ed_list,
            min_jaccard=0.82,
            min_ratio=0.75,
            allow_audio_fp=True,
        )
        if not candidate_groups and _group_has_exact_provider_trackcount_signal(ed_list):
            candidate_groups = [ed_list]
        for candidate in candidate_groups:
            if len(candidate) < 2:
                continue
            _append_group(candidate, fuzzy=False, signal="strict_identity", evidence=[f"STRICT_IDENTITY:{strict_key}"])

    # Phase 1/3: loose title grouping + similarity-based splitting.
    remaining = [e for e in editions if e.get("album_id") not in used_ids]
    loose_groups: dict[str, list[dict]] = defaultdict(list)
    for e in remaining:
        key = (e.get("_dupe_title_norm_loose") or "").strip() or (e.get("album_norm") or "").strip()
        loose_groups[key].append(e)
    for key, ed_list in loose_groups.items():
        if len(ed_list) < 2:
            continue
        clusters = _dupe_split_editions_by_similarity(ed_list, min_jaccard=0.82, min_ratio=0.75, allow_audio_fp=True)
        for c in clusters:
            if len(c) < 2:
                continue
            _append_group(c, fuzzy=True, signal="title_loose", evidence=[f"TITLE_LOOSE:{key}"])

    # Fallback: strict title grouping for remaining editions.
    remaining = [e for e in editions if e.get("album_id") not in used_ids]
    strict_groups: dict[str, list[dict]] = defaultdict(list)
    for e in remaining:
        group_key = (e.get("plex_norm") or e.get("album_norm") or "").strip()
        if not group_key or group_key.startswith("__untitled__"):
            group_key = (e.get("album_norm") or "").strip()
        strict_groups[group_key].append(e)
    for key, ed_list in strict_groups.items():
        if len(ed_list) < 2:
            continue
        _append_group(ed_list, fuzzy=True, signal="title_strict", evidence=[f"TITLE_STRICT:{key}"])

    # --- Same-folder duplicate groups: multiple Plex album entries pointing to one folder ---
    for folder_str, album_ids in seen_folders.items():
        if len(album_ids) < 2:
            continue
        best_edition = folder_to_edition.get(folder_str)
        if not best_edition:
            continue
        losers = []
        for aid in album_ids:
            if aid == best_edition["album_id"]:
                continue
            pt = album_id_to_title.get(aid, f"Album {aid}") if db_conn is None else (album_title(db_conn, aid) or f"Album {aid}")
            losers.append({
                "album_id": aid,
                "title_raw": pt,
                "folder": best_edition["folder"],
                "meta": {},
                "plex_title": pt,
                "br": 0,
                "sr": 0,
                "bd": 0,
            })
        if not losers:
            continue
        chips = ["SAME_FOLDER_DUPLICATE"]
        try:
            best_edition["dupe_evidence"] = chips
        except Exception:
            pass
        logging.info(
            "[Artist %s] Same-folder duplicate group: '%s' has %d Plex entries (best=%s, losers=%s)",
            artist, best_edition.get("title_raw", ""), len(album_ids),
            best_edition["album_id"], [l["album_id"] for l in losers]
        )
        out.append({
            "artist": artist,
            "album_id": best_edition["album_id"],
            "best": best_edition,
            "losers": losers,
            "fuzzy": False,
            "needs_ai": False,
            "dupe_signal": "same_folder",
            "dupe_evidence": chips,
            # Same-folder groups are Plex metadata duplicates; they require manual cleanup, not moves.
            "no_move": True,
            "manual_review": True,
            "same_folder": True,
        })
        dupe_report["groups_total"] = int(dupe_report.get("groups_total") or 0) + 1
        _dr_inc("groups_by_signal", "same_folder")

    logging.info(
        "[Artist %s] dupe_v2: groups=%d (needs_ai=%d) signals=%s rejected=%s",
        artist,
        int(dupe_report.get("groups_total") or 0),
        int(dupe_report.get("groups_needs_ai") or 0),
        dupe_report.get("groups_by_signal") or {},
        dupe_report.get("rejected_by_reason") or {},
    )
    # Keep groups that have losers (resolved) or need AI (will be resolved in batch).
    # Do not drop needs_ai groups: they have "editions" but no "losers" yet.
    out = [g for g in out if g.get("losers") or g.get("needs_ai")]

    # Calculate total scan time
    scan_total_time = time.perf_counter() - scan_start_time

    # Compile stats with timing (use all_editions_for_stats for per-edition counts)
    stats_editions = list(all_editions_for_stats)
    audio_cache_hits_stats = sum(1 for e in stats_editions if e.get("audio_cache_hit", False))
    audio_cache_misses_stats = max(0, len(stats_editions) - audio_cache_hits_stats)
    mb_cache_hits_stats = sum(
        1 for e in stats_editions if ("rg_info" in e) and bool(e.get("mb_cache_hit", False))
    )
    mb_cache_misses_stats = sum(
        1 for e in stats_editions if ("rg_info" in e) and (not bool(e.get("mb_cache_hit", False)))
    )
    strict_matched_stats = sum(1 for e in stats_editions if bool(e.get("strict_match_verified")))
    mb_used_stats = strict_matched_stats
    mb_verified_by_ai = sum(1 for e in stats_editions if e.get("match_verified_by_ai"))
    duplicate_groups_count = len(out)
    total_duplicates_count = sum(len(g.get("losers", [])) for g in out)
    broken_albums_count = sum(1 for e in stats_editions if e.get("is_broken", False))
    albums_without_mb_id = 0
    albums_without_artist_mb_id = 0
    albums_without_complete_tags = 0
    albums_without_artist_image = 0
    albums_without_album_image = 0

    for e in stats_editions:
        meta = e.get("meta", {}) or {}
        if not bool(e.get("strict_match_verified")):
            albums_without_mb_id += 1
        if not meta.get("musicbrainz_albumartistid") and not meta.get("musicbrainz_artistid"):
            albums_without_artist_mb_id += 1

        # REQUIRED_TAGS from settings = single source of truth
        missing_required = []
        try:
            missing_required = _check_required_tags(meta, REQUIRED_TAGS, edition=e)
        except Exception:
            missing_required = []
        if missing_required:
            albums_without_complete_tags += 1

        # Album cover (prefer cached boolean when available).
        has_cover = None
        if "has_cover" in e:
            has_cover = bool(e.get("has_cover"))
        else:
            folder = e.get("folder")
            folder_path = None
            if folder:
                folder_path = folder if isinstance(folder, Path) else Path(str(folder))
            if folder_path and folder_path.exists():
                cover_patterns = ["cover.*", "folder.*", "album.*", "artwork.*", "front.*"]
                for pattern in cover_patterns:
                    try:
                        matches = list(folder_path.glob(pattern))
                    except Exception:
                        matches = []
                    image_matches = [f for f in matches if f.suffix.lower() in [".jpg", ".jpeg", ".png", ".webp", ".gif"]]
                    if image_matches:
                        has_cover = True
                        break
                if has_cover is None:
                    has_cover = False
        if has_cover is False:
            albums_without_album_image += 1

        # Artist image is tracked downstream (per-artist), but keep the counter for completeness.
        if "has_artist_image" in e and (not bool(e.get("has_artist_image"))):
            albums_without_artist_image += 1

    stats = {
        "ai_used": ai_used_count,
        "mb_used": mb_used_stats,
        "mb_verified_by_ai": mb_verified_by_ai,
        "audio_cache_hits": audio_cache_hits_stats,
        "audio_cache_misses": audio_cache_misses_stats,
        "mb_cache_hits": mb_cache_hits_stats,
        "mb_cache_misses": mb_cache_misses_stats,
        "duplicate_groups_count": duplicate_groups_count,
        "total_duplicates_count": total_duplicates_count,
        "broken_albums_count": broken_albums_count,
        "albums_without_mb_id": albums_without_mb_id,
        "albums_without_artist_mb_id": albums_without_artist_mb_id,
        "albums_without_complete_tags": albums_without_complete_tags,
        "albums_without_album_image": albums_without_album_image,
        "albums_without_artist_image": albums_without_artist_image,
        "dupe_report": dupe_report,
        "timing": {
            "audio_analysis_time": audio_analysis_time,
            "mb_lookup_time": mb_lookup_time,
            "ai_processing_time": ai_processing_time,
            "total_time": scan_total_time,
        },
    }

    # Mark all remaining albums as done (those not in any duplicate group)
    with lock:
        if artist in state.get("scan_active_artists", {}):
            current_album = state["scan_active_artists"][artist].get("current_album", {})
            current_album_id = current_album.get("album_id")
            # If current album is not in used_ids, it means it's a single edition (no duplicates)
            # Mark it as done
            if current_album_id and current_album_id not in used_ids:
                state["scan_active_artists"][artist]["current_album"]["status"] = "done"
                state["scan_active_artists"][artist]["current_album"]["status_details"] = ""
                state["scan_active_artists"][artist]["current_album"]["step_summary"] = ""
                state["scan_active_artists"][artist]["current_album"]["step_response"] = ""

    # Store broken/review albums in database
    import json
    tracked_incomplete_album_ids: set[int] = set()
    broken_album_rows: list[tuple[Any, ...]] = []
    for e in all_editions_for_stats:
        assessment = dict(e.get("_incomplete_assessment") or {})
        is_broken_entry = bool(e.get("is_broken", False))
        review_candidate = bool(e.get("_incomplete_review_candidate"))
        if is_broken_entry or review_candidate:
            tracked_incomplete_album_ids.add(int(e.get("album_id") or 0))
            missing_required_tags = []
            try:
                missing_required_tags = _check_required_tags(e.get("meta", {}) or {}, REQUIRED_TAGS, edition=e)
            except Exception:
                missing_required_tags = []
            missing_indices_json = json.dumps(e.get('missing_indices', []), default=str)
            folder_path = str(e.get("folder") or "").strip()
            folder_obj: Path | None = None
            if folder_path:
                try:
                    candidate = path_for_fs_access(Path(folder_path))
                    if candidate.exists() and candidate.is_dir():
                        folder_obj = candidate
                except Exception:
                    folder_obj = None
            metadata_source_value = _normalize_identity_provider(
                str(
                    e.get("primary_metadata_source")
                    or e.get("metadata_source")
                    or (e.get("meta", {}) or {}).get("primary_metadata_source")
                    or ""
                )
            )
            strict_provider_value = _normalize_identity_provider(str(e.get("strict_match_provider") or ""))
            detail_map = {
                "expected_track_count": _parse_int_loose(e.get("expected_track_count"), 0),
                "actual_track_count": _parse_int_loose(e.get("actual_track_count"), len(e.get("tracks", []))),
                "missing_indices": list(e.get("missing_indices") or []),
                "strict_match_provider": strict_provider_value,
                "strict_reject_reason": str(e.get("strict_reject_reason") or "").strip(),
                "musicbrainz_release_group_id": str(e.get("musicbrainz_id") or "").strip(),
                "discogs_release_id": str(e.get("discogs_release_id") or "").strip(),
                "lastfm_album_mbid": str(e.get("lastfm_album_mbid") or "").strip(),
                "bandcamp_album_url": str(e.get("bandcamp_album_url") or "").strip(),
                "missing_required_tags": list(missing_required_tags or []),
            }
            local_tracks = _scan_move_track_entries_from_folder(
                folder_obj,
                artist_name=str(artist or ""),
                album_title=str(e.get("title_raw") or e.get("album_title") or ""),
                metadata_source=str(metadata_source_value or strict_provider_value or ""),
                details=detail_map,
                quick=True,
            ) if folder_obj else []
            expected_tracks = _scan_move_expected_tracks(
                folder=folder_obj,
                artist_name=str(artist or ""),
                album_title=str(e.get("title_raw") or e.get("album_title") or ""),
                metadata_source=str(metadata_source_value or strict_provider_value or ""),
                details=detail_map,
                cache_only=True,
            )
            provider_refs = {
                "musicbrainz_release_id": str(e.get("musicbrainz_id") or "").strip(),
                "discogs_release_id": str(e.get("discogs_release_id") or "").strip(),
                "lastfm_album_mbid": str(e.get("lastfm_album_mbid") or "").strip(),
                "bandcamp_album_url": str(e.get("bandcamp_album_url") or "").strip(),
            }
            reason_summary = str((assessment or {}).get("summary") or "").strip() or _broken_album_reason_summary(
                expected_track_count=_parse_int_loose(e.get("expected_track_count"), 0),
                actual_track_count=_parse_int_loose(e.get("actual_track_count"), len(local_tracks or e.get("tracks", []))),
                missing_indices=list(e.get("missing_indices") or []),
                missing_required_tags=list(missing_required_tags or []),
                strict_reject_reason=str(e.get("strict_reject_reason") or "").strip(),
            )
            broken_album_rows.append((
                artist,
                e['album_id'],
                e.get('expected_track_count'),
                e.get('actual_track_count', len(e.get('tracks', []))),
                missing_indices_json,
                e.get('musicbrainz_id'),
                time.time(),
                str(e.get("title_raw") or e.get("album_title") or "").strip(),
                folder_path,
                str(metadata_source_value or ""),
                str(strict_provider_value or ""),
                str(e.get("strict_reject_reason") or "").strip(),
                json.dumps(provider_refs, default=str),
                reason_summary,
                json.dumps(local_tracks, default=str),
                json.dumps(expected_tracks, default=str),
                json.dumps(list(missing_required_tags or []), default=str),
                str((assessment or {}).get("verdict") or ("confirmed_incomplete" if is_broken_entry else "insufficient_evidence_manual_review")),
                float((assessment or {}).get("confidence") or 0.0),
                str((assessment or {}).get("source") or "deterministic"),
                1 if bool((assessment or {}).get("quarantine_eligible")) else 0,
                json.dumps(assessment or {}, default=str),
            ))

    def _write_broken_album_rows() -> None:
        con = _state_connect()
        try:
            cur = con.cursor()
            for row_values in broken_album_rows:
                cur.execute("""
                INSERT INTO broken_albums
                (
                    artist, album_id, expected_track_count, actual_track_count, missing_indices,
                    musicbrainz_release_group_id, detected_at, album_title, folder_path, metadata_source,
                    strict_match_provider, strict_reject_reason, provider_refs_json, reason_summary,
                    local_tracks_json, expected_tracks_json, missing_required_tags_json,
                    classification, classification_confidence, classification_source,
                    quarantine_eligible, evidence_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(artist, album_id) DO UPDATE SET
                    expected_track_count = excluded.expected_track_count,
                    actual_track_count = excluded.actual_track_count,
                    missing_indices = excluded.missing_indices,
                    musicbrainz_release_group_id = excluded.musicbrainz_release_group_id,
                    detected_at = excluded.detected_at,
                    album_title = excluded.album_title,
                    folder_path = excluded.folder_path,
                    metadata_source = excluded.metadata_source,
                    strict_match_provider = excluded.strict_match_provider,
                    strict_reject_reason = excluded.strict_reject_reason,
                    provider_refs_json = excluded.provider_refs_json,
                    reason_summary = excluded.reason_summary,
                    local_tracks_json = excluded.local_tracks_json,
                    expected_tracks_json = excluded.expected_tracks_json,
                    missing_required_tags_json = excluded.missing_required_tags_json,
                    classification = excluded.classification,
                    classification_confidence = excluded.classification_confidence,
                    classification_source = excluded.classification_source,
                    quarantine_eligible = excluded.quarantine_eligible,
                    evidence_json = excluded.evidence_json
                """, row_values)
            # Do not clear other rows for this artist here. In disk-aware or
            # resumed scans an artist can be processed as a partial bucket; a
            # per-artist DELETE would hide historical open incompletes from
            # other disks/runs. Stale rows are revalidated and purged lazily by
            # /api/broken-albums when their current folder no longer qualifies.
            con.commit()
        finally:
            con.close()

    if artist or broken_album_rows:
        _state_db_write_retry(
            _write_broken_album_rows,
            label=f"scan_duplicates.broken_albums:{artist}",
            attempts=12,
        )

    return out, stats, all_editions_for_stats
