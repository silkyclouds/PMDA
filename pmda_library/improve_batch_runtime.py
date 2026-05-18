"""Batch album improvement and scan-inline enrichment runtime helpers.

The functions in this module are extracted from the historical PMDA bootstrap.
They operate against the live PMDA runtime through explicit binding so existing
compatibility tests can still patch runtime symbols on ``pmda``.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, List, Optional

_EXTRACTED_NAMES = {
    "_improve_one_album_item",
    "_build_improve_items_from_editions",
    "_run_improve_all_albums_global",
    "_run_scan_profile_enrichment_inline",
    "_run_improve_all_albums",
    "_mb_missing_release_group_ids_cache",
}


def _bind_runtime(runtime: Any) -> None:
    """Bind live PMDA runtime globals while preserving extracted originals."""
    for name, value in vars(runtime).items():
        if name in _EXTRACTED_NAMES:
            if getattr(value, "__module__", "") != getattr(runtime, "__name__", ""):
                globals()[name] = value
            else:
                original = _ORIGINAL_EXTRACTED_FUNCTIONS.get(name)
                if original is not None:
                    globals()[name] = original
            continue
        own_wrapper = name.endswith("_for_runtime") and name[: -len("_for_runtime")] in _EXTRACTED_NAMES
        if name == "_bind_runtime" or own_wrapper:
            continue
        globals()[name] = value

def _improve_one_album_item(item: dict) -> tuple:
    """Run improve on one album (own DB connection). Returns (index, item, result, steps) for state update."""
    idx = item.get("_idx", 0)
    album_id = item.get("album_id")
    artist_name = item.get("artist", "Unknown")
    album_title = item.get("album_title", f"Album {album_id}")
    audit_enabled = bool(item.get("_audit_enabled"))
    run_kind = str(item.get("_run_kind") or "manual").strip() or "manual"
    strict_ok, strict_reason = _strict_mutation_allowed(item)
    if not strict_ok:
        reason = strict_reason or "strict_match_missing"
        result = {
            "steps": [f"Mutation blocked: {reason}"],
            "summary": f"No mutation applied (strict gate): {reason}.",
            "tags_updated": False,
            "cover_saved": False,
            "provider_used": None,
            "files_updated": 0,
            "pmda_matched": False,
            "pmda_cover": False,
            "pmda_artist_image": False,
            "pmda_complete": False,
            "strict_match_verified": False,
            "strict_match_provider": "",
            "strict_reject_reason": reason,
            "strict_tracklist_score": 0.0,
            "mutation_blocked": True,
            "mutation_blocked_reason": reason,
        }
        steps = [{"label": result["steps"][0], "success": False}]
        if audit_enabled:
            _record_files_match_audit_album(
                album_id=int(album_id or 0),
                artist_name=str(artist_name or ""),
                album_title=str(album_title or ""),
                run_kind=run_kind,
                status="blocked",
                result=result,
                steps=[result["steps"][0]],
            )
        return (idx, album_id, album_title, artist_name, result, steps)
    folder_path_raw = (item.get("folder") or "").strip() if isinstance(item.get("folder"), str) else ""
    # Files mode: improve by folder path from scan_editions (album_id is not a Plex metadata id).
    if folder_path_raw:
        result = _improve_folder_by_path(Path(folder_path_raw))
        result.setdefault("strict_match_verified", bool(item.get("strict_match_verified")))
        result.setdefault("strict_match_provider", item.get("strict_match_provider") or "")
        result.setdefault("strict_reject_reason", item.get("strict_reject_reason") or "")
        result.setdefault("strict_tracklist_score", float(item.get("strict_tracklist_score") or 0.0))
        result.setdefault("mutation_blocked", False)
        result.setdefault("mutation_blocked_reason", "")
        steps_raw = result.get("steps", [])
        steps = [{"label": s if isinstance(s, str) else s.get("label", str(s)), "success": True} for s in steps_raw]
        if audit_enabled:
            _record_files_match_audit_album(
                album_id=int(album_id or 0),
                artist_name=str(artist_name or ""),
                album_title=str(album_title or ""),
                run_kind=run_kind,
                status="completed",
                result=result,
                steps=[str(s or "").strip() for s in steps_raw if str(s or "").strip()],
            )
        return (idx, album_id, album_title, artist_name, result, steps)
    reason = "missing_folder_path"
    result = {
        "steps": [f"Mutation blocked: {reason}"],
        "summary": "No mutation applied because the files album folder path is missing.",
        "tags_updated": False,
        "cover_saved": False,
        "provider_used": None,
        "files_updated": 0,
        "pmda_matched": False,
        "pmda_cover": False,
        "pmda_artist_image": False,
        "pmda_complete": False,
        "strict_match_verified": False,
        "strict_match_provider": "",
        "strict_reject_reason": reason,
        "strict_tracklist_score": 0.0,
        "mutation_blocked": True,
        "mutation_blocked_reason": reason,
    }
    steps = [{"label": result["steps"][0], "success": False}]
    if audit_enabled:
        _record_files_match_audit_album(
            album_id=int(album_id or 0),
            artist_name=str(artist_name or ""),
            album_title=str(album_title or ""),
            run_kind=run_kind,
            status="blocked",
            result=result,
            steps=[result["steps"][0]],
        )
    return (idx, album_id, album_title, artist_name, result, steps)


def _build_improve_items_from_editions(
    artist_name: str,
    editions: list[dict],
    groups: list[dict] | None = None,
) -> list[dict]:
    """
    Convert scan editions for one artist into improve-album items, deduplicated by album_id.
    Used to stream post-processing artist-by-artist in Files mode.
    """
    items: list[dict] = []
    root_dirs = _files_root_dir_strings()
    seen_album_ids: set[int] = set()
    skip_album_ids: set[int] = set()
    for group in groups or []:
        if not isinstance(group, dict):
            continue
        for loser in list(group.get("losers") or []):
            try:
                loser_album_id = int((loser or {}).get("album_id") or 0)
            except Exception:
                loser_album_id = 0
            if loser_album_id > 0:
                skip_album_ids.add(loser_album_id)
    for e in editions or []:
        folder_name = ""
        try:
            folder_name = Path(str(e.get("folder") or "")).name
        except Exception:
            folder_name = ""
        artist_resolved, title_resolved = _apply_resolved_identity_to_edition(
            e,
            default_artist=str(artist_name or ""),
            default_title=str(e.get("title_raw") or e.get("album_title") or ""),
            folder_name=folder_name,
        )
        try:
            album_id = int(e.get("album_id") or 0)
        except Exception:
            album_id = 0
        if album_id <= 0 or album_id in seen_album_ids or album_id in skip_album_ids:
            continue
        seen_album_ids.add(album_id)
        folder_raw = e.get("folder")
        folder_str = str(folder_raw).strip() if folder_raw is not None else ""
        if bool(e.get("is_broken")):
            continue
        mbid = (
            (e.get("musicbrainz_id") or "")
            or ((e.get("meta") or {}).get("musicbrainz_releasegroupid") or "")
            or ((e.get("meta") or {}).get("musicbrainz_id") or "")
        )
        mbid = str(mbid or "").strip()
        title = (title_resolved or e.get("title_raw") or e.get("album_norm") or f"Album {album_id}")
        # Snapshot pre-fix health so post-processing can update live counters by delta.
        folder_path: Optional[Path] = None
        if folder_str:
            try:
                folder_path = path_for_fs_access(Path(folder_str))
            except Exception:
                folder_path = None
        if folder_path is None or (not folder_path.exists()) or (not folder_path.is_dir()):
            continue
        meta_snapshot = dict(e.get("meta") or {})
        # Required tags check needs tracks; derive them if absent (so "tracks" requirement is meaningful).
        edition_for_required = dict(e)
        if not (edition_for_required.get("tracks") or []):
            ordered_paths = [Path(p) for p in (e.get("ordered_paths") or []) if str(p).strip()]
            if folder_path is not None and not ordered_paths:
                try:
                    ordered_paths = _files_collect_ordered_audio_paths(folder_path, [])
                except Exception:
                    ordered_paths = []
            derived_tracks = [
                {"title": p.stem or f"Track {i + 1}", "idx": i + 1}
                for i, p in enumerate(ordered_paths)
            ]
            edition_for_required["tracks"] = derived_tracks
        try:
            pre_missing_required = _check_required_tags(meta_snapshot, REQUIRED_TAGS, edition=edition_for_required)
        except Exception:
            pre_missing_required = []
        pre_has_cover = False
        pre_has_artist_image = False
        if folder_path is not None:
            try:
                pre_has_cover = bool(album_folder_has_cover(folder_path))
            except Exception:
                pre_has_cover = False
            try:
                artist_folder = _files_guess_artist_folder(folder_path, artist_name, root_dirs=root_dirs)
                pre_has_artist_image = bool(_artist_folder_has_image(artist_folder)) if artist_folder else False
            except Exception:
                pre_has_artist_image = False
        pre_has_mb_id = bool(e.get("strict_match_verified"))
        pre_has_artist_mb_id = bool(
            meta_snapshot.get("musicbrainz_albumartistid")
            or meta_snapshot.get("musicbrainz_artistid")
            or meta_snapshot.get("musicbrainz_albumartist_id")
            or meta_snapshot.get("musicbrainz_artist_id")
        )
        items.append(
            {
                "artist": artist_resolved or (artist_name or "").strip() or "Unknown Artist",
                "artist_name": artist_resolved,
                "album_id": album_id,
                "album_title": str(title or "").strip() or f"Album {album_id}",
                "title_raw": str(title or "").strip() or f"Album {album_id}",
                "musicbrainz_id": mbid,
                "folder": folder_str,
                # Keep scan edition context so post-process cache refresh can preserve
                # required-tags health (notably "tracks") for incremental changed-only scans.
                "tracks": list(e.get("tracks") or []),
                "meta": dict(e.get("meta") or {}),
                "ordered_paths": list(e.get("ordered_paths") or []),
                "fingerprint": e.get("fingerprint") or "",
                "primary_metadata_source": e.get("primary_metadata_source") or e.get("metadata_source") or "",
                "metadata_source": e.get("metadata_source") or e.get("primary_metadata_source") or "",
                "discogs_release_id": e.get("discogs_release_id") or "",
                "lastfm_album_mbid": e.get("lastfm_album_mbid") or "",
                "bandcamp_album_url": e.get("bandcamp_album_url") or "",
                "strict_match_verified": bool(e.get("strict_match_verified")),
                "strict_match_provider": e.get("strict_match_provider") or "",
                "strict_reject_reason": e.get("strict_reject_reason") or "",
                "strict_tracklist_score": float(e.get("strict_tracklist_score") or 0.0),
                "is_broken": bool(e.get("is_broken")),
                "expected_track_count": e.get("expected_track_count"),
                "actual_track_count": e.get("actual_track_count") or len(e.get("tracks") or []),
                "missing_indices": list(e.get("missing_indices") or []),
                "_lookup_artist_name": e.get("_lookup_artist_name") or "",
                "_lookup_album_title": e.get("_lookup_album_title") or "",
                "_lookup_identity_hint": e.get("_lookup_identity_hint") or {},
                "fmt_text": str(e.get("fmt_text") or get_primary_format(folder_path) or ""),
                "br": e.get("br") or 0,
                "sr": e.get("sr") or 0,
                "bd": e.get("bd") or 0,
                "pre_missing_required_tags": list(pre_missing_required or []),
                "pre_has_cover": bool(pre_has_cover),
                "pre_has_artist_image": bool(pre_has_artist_image),
                "pre_has_mb_id": bool(pre_has_mb_id),
                "pre_has_artist_mb_id": bool(pre_has_artist_mb_id),
            }
        )
    return items


def _run_improve_all_albums_global(best_albums_list: List[dict]):
    """Background worker: improve each 'best' album from duplicate groups (global fix-all). Saves last_fix_all_by_provider for summary/chart."""
    total = len(best_albums_list)
    workers = max(1, min(8, getattr(sys.modules[__name__], "IMPROVE_ALL_WORKERS", 1)))
    logging.info("Improve-all (Magic): started for %d album(s) – tags, covers, artist images (workers=%d)", total, workers)
    albums_improved = 0
    tags_updated_count = 0
    covers_downloaded = 0
    # PMDA-level stats for this improve-all run (used later in summary_json)
    pmda_albums_processed = 0
    pmda_albums_complete = 0
    pmda_albums_with_cover = 0
    pmda_albums_with_artist_image = 0
    album_log = []
    providers = ["musicbrainz", "discogs", "lastfm", "bandcamp"]
    by_provider = {p: {"identified": 0, "covers": 0, "tags": 0} for p in providers}
    # Inject index for ordering
    items_with_idx = [{**item, "_idx": i} for i, item in enumerate(best_albums_list)]
    with lock:
        state["improve_all"] = {
            "running": True,
            "global": True,
            "artist_id": None,
            "current": 0,
            "total": total,
            "current_album_id": None,
            "current_album": None,
            "current_artist": None,
            "current_provider": "musicbrainz",
            "provider_status": {p: "pending" for p in providers},
            "log": [],
            "result": None,
            "error": None,
        }
    try:
        if workers <= 1:
            # Sequential (original flow)
            for i, item in enumerate(items_with_idx):
                idx, album_id, album_title, artist_name, result, steps = _improve_one_album_item(item)
                prov = result.get("provider_used") or "musicbrainz"
                if prov in by_provider:
                    if result.get("tags_updated") or result.get("cover_saved"):
                        by_provider[prov]["identified"] += 1
                    if result.get("cover_saved"):
                        by_provider[prov]["covers"] += 1
                    if result.get("tags_updated"):
                        by_provider[prov]["tags"] += 1
                if result.get("tags_updated"):
                    tags_updated_count += 1
                if result.get("cover_saved"):
                    covers_downloaded += 1
                if result.get("tags_updated") or result.get("cover_saved"):
                    albums_improved += 1
                # PMDA stats from this album (only when we have PMDA flags)
                if result.get("pmda_matched") or result.get("pmda_cover") or result.get("pmda_artist_image"):
                    pmda_albums_processed += 1
                if result.get("pmda_cover"):
                    pmda_albums_with_cover += 1
                if result.get("pmda_artist_image"):
                    pmda_albums_with_artist_image += 1
                if result.get("pmda_complete"):
                    pmda_albums_complete += 1
                album_log.append({
                    "album_id": album_id,
                    "title": album_title,
                    "artist": artist_name,
                    "summary": result.get("summary", ""),
                    "steps": steps,
                })
                with lock:
                    if state.get("improve_all") and state["improve_all"].get("running"):
                        state["improve_all"]["current"] = idx + 1
                        state["improve_all"]["current_album_id"] = album_id
                        state["improve_all"]["current_album"] = album_title
                        state["improve_all"]["current_artist"] = artist_name
                        state["improve_all"]["log"] = list(album_log)
                        state["improve_all"]["current_steps"] = steps
                        state["improve_all"]["provider_status"] = {p: "ok" for p in providers}
        else:
            # Parallel: run up to `workers` albums at a time (MB queue still serializes MB calls)
            completed_with_idx: List[tuple] = []  # (idx, album_id, album_title, artist_name, result, steps)
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_item = {executor.submit(_improve_one_album_item, it): it for it in items_with_idx}
                for future in as_completed(future_to_item):
                    try:
                        idx, album_id, album_title, artist_name, result, steps = future.result()
                        completed_with_idx.append((idx, album_id, album_title, artist_name, result, steps))
                        prov = result.get("provider_used") or "musicbrainz"
                        if prov in by_provider:
                            if result.get("tags_updated") or result.get("cover_saved"):
                                by_provider[prov]["identified"] += 1
                            if result.get("cover_saved"):
                                by_provider[prov]["covers"] += 1
                            if result.get("tags_updated"):
                                by_provider[prov]["tags"] += 1
                        if result.get("tags_updated"):
                            tags_updated_count += 1
                        if result.get("cover_saved"):
                            covers_downloaded += 1
                        if result.get("tags_updated") or result.get("cover_saved"):
                            albums_improved += 1
                        if result.get("pmda_matched") or result.get("pmda_cover") or result.get("pmda_artist_image"):
                            pmda_albums_processed += 1
                        if result.get("pmda_cover"):
                            pmda_albums_with_cover += 1
                        if result.get("pmda_artist_image"):
                            pmda_albums_with_artist_image += 1
                        if result.get("pmda_complete"):
                            pmda_albums_complete += 1
                        album_log.append({
                            "album_id": album_id,
                            "title": album_title,
                            "artist": artist_name,
                            "summary": result.get("summary", ""),
                            "steps": steps,
                        })
                        with lock:
                            if state.get("improve_all") and state["improve_all"].get("running"):
                                state["improve_all"]["current"] = len(album_log)
                                state["improve_all"]["current_album_id"] = album_id
                                state["improve_all"]["current_album"] = album_title
                                state["improve_all"]["current_artist"] = artist_name
                                state["improve_all"]["log"] = list(album_log)
                                state["improve_all"]["current_steps"] = steps
                                state["improve_all"]["provider_status"] = {p: "ok" for p in providers}
                    except Exception as e:
                        logging.exception("improve-all worker failed: %s", e)
            # Sort album_log by original index for consistent final report
            idx_to_entry = {t[0]: {"album_id": t[1], "title": t[2], "artist": t[3], "summary": t[4].get("summary", ""), "steps": t[5]} for t in completed_with_idx}
            album_log = [idx_to_entry[i] for i in range(total) if i in idx_to_entry]
        with lock:
            if state.get("improve_all"):
                state["improve_all"]["running"] = False
                state["improve_all"]["result"] = {
                    "message": f"Processed {total} album(s). Tags updated on {tags_updated_count} album(s), {covers_downloaded} cover(s) saved.",
                    "albums_processed": total,
                    "albums_improved": albums_improved,
                    "covers_downloaded": covers_downloaded,
                    "tags_updated": tags_updated_count,
                    "by_provider": by_provider,
                    "album_log": album_log,
                    "pmda_albums_processed": pmda_albums_processed,
                    "pmda_albums_complete": pmda_albums_complete,
                    "pmda_albums_with_cover": pmda_albums_with_cover,
                    "pmda_albums_with_artist_image": pmda_albums_with_artist_image,
                }
                # Also expose PMDA stats at scan level so they can be included in summary_json
                state["scan_pmda_albums_processed"] = pmda_albums_processed
                state["scan_pmda_albums_complete"] = pmda_albums_complete
                state["scan_pmda_albums_with_cover"] = pmda_albums_with_cover
                state["scan_pmda_albums_with_artist_image"] = pmda_albums_with_artist_image
            state["last_fix_all_by_provider"] = by_provider
            state["last_fix_all_total_albums"] = total
            msg = state["improve_all"]["result"].get("message", "")
            logging.info("Improve-all (Magic): finished. %s", msg)
        # Auto-export: rebuild Files export library after Magic when enabled
        try:
            if _get_library_mode() == "files" and getattr(sys.modules[__name__], "AUTO_EXPORT_LIBRARY", False) and not library_is_audit_mode():
                started = _trigger_export_library_async(reason="magic_auto_export")
                if started:
                    logging.info("Auto-export queued after Magic run (AUTO_EXPORT_LIBRARY=True).")
                else:
                    logging.info("Auto-export already running after Magic run; queue skipped.")
        except Exception as e:
            logging.exception("Auto-export library after Magic failed: %s", e)
        if _get_library_mode() == "files":
            _trigger_files_index_rebuild_async(reason="improve_all_completed")
    except Exception as e:
        logging.exception("improve-all (global) failed: %s", e)
        with lock:
            if state.get("improve_all"):
                state["improve_all"]["running"] = False
                state["improve_all"]["error"] = str(e)


def _run_scan_profile_enrichment_inline(best_albums_list: list[dict], *, reason: str = "scan_inline") -> dict[str, int]:
    """
    Scan-only profile enrichment (artist bios + album reviews) executed inside the scan
    when background scheduler jobs are disabled. This keeps the pipeline deterministic:
    one scan run does all metadata processing.
    """
    if _get_library_mode() != "files":
        return {"artists_total": 0, "artists_done": 0, "artists_failed": 0, "albums_targeted": 0}
    grouped: dict[str, dict[str, Any]] = {}
    for row in (best_albums_list or []):
        if not isinstance(row, dict):
            continue
        resolved = dict(row)
        try:
            _apply_resolved_identity_to_edition(resolved)
        except Exception:
            pass
        artist_name = str(
            resolved.get("artist")
            or resolved.get("_lookup_artist_name")
            or row.get("artist")
            or ""
        ).strip()
        album_title = str(
            resolved.get("album_title")
            or resolved.get("title_raw")
            or resolved.get("_lookup_album_title")
            or row.get("album_title")
            or row.get("title_raw")
            or ""
        ).strip()
        if _identity_text_is_generic(artist_name):
            artist_name = str(resolved.get("_lookup_artist_name") or "").strip() or artist_name
        if not album_title:
            album_title = str(resolved.get("_lookup_album_title") or "").strip()
        if not artist_name or not album_title:
            continue
        artist_norm = _norm_artist_key(artist_name)
        if not artist_norm:
            continue
        title_norm = norm_album_for_dedup(album_title, normalize_parenthetical=True)
        if not title_norm:
            title_norm = norm_album(album_title)
        bucket = grouped.setdefault(
            artist_norm,
            {
                "artist_name": artist_name,
                "albums": [],
                "_seen_norms": set(),
            },
        )
        bucket["artist_name"] = _choose_preferred_identity_display(
            str(bucket.get("artist_name") or ""),
            artist_name,
        )
        seen_norms: set[str] = bucket.get("_seen_norms") or set()
        if title_norm in seen_norms:
            continue
        seen_norms.add(title_norm)
        bucket["_seen_norms"] = seen_norms
        bucket["albums"].append((album_title, title_norm))

    artists_total = len(grouped)
    artists_done = 0
    artists_failed = 0
    albums_targeted = sum(len(v.get("albums") or []) for v in grouped.values())
    if artists_total <= 0:
        return {
            "artists_total": 0,
            "artists_done": 0,
            "artists_failed": 0,
            "albums_targeted": 0,
        }

    logging.info(
        "Pipeline step profile-enrich-inline: start (artists=%d, albums=%d, reason=%s)",
        artists_total,
        albums_targeted,
        reason,
    )
    with lock:
        state["scan_profile_enrich_total"] = int(artists_total)
        state["scan_profile_enrich_done"] = 0
        state["scan_profile_enrich_current_artist"] = None
        state["scan_profile_enrich_running"] = True
        state["scan_profile_enrich_started_at"] = time.time()

    try:
        for artist_norm, payload in grouped.items():
            artist_name = str(payload.get("artist_name") or "").strip()
            albums = list(payload.get("albums") or [])
            with lock:
                state["scan_profile_enrich_current_artist"] = artist_name
            if not artist_name or not albums:
                continue
            job_key = f"{reason}:{artist_norm}:{int(time.time() * 1000)}"
            try:
                _run_files_profile_enrichment_job(
                    job_key=job_key,
                    artist_name=artist_name,
                    artist_norm=artist_norm,
                    albums=albums,
                    skip_album_profiles=False,
                    allow_soft_profiles=False,
                    fast_mode=False,
                )
                artists_done += 1
            except Exception:
                artists_failed += 1
                logging.debug("Inline profile enrichment failed for %s", artist_name, exc_info=True)
            finally:
                with lock:
                    state["scan_profile_enrich_done"] = int(artists_done + artists_failed)
                    state["scan_profile_enrich_updated_at"] = time.time()
    finally:
        with lock:
            state["scan_profile_enrich_running"] = False
            state["scan_profile_enrich_current_artist"] = None
            state["scan_profile_enrich_finished_at"] = time.time()

    logging.info(
        "Pipeline step profile-enrich-inline: done (artists_done=%d, artists_failed=%d, albums=%d)",
        artists_done,
        artists_failed,
        albums_targeted,
    )
    return {
        "artists_total": int(artists_total),
        "artists_done": int(artists_done),
        "artists_failed": int(artists_failed),
        "albums_targeted": int(albums_targeted),
    }


def _run_improve_all_albums(artist_id: int, album_ids: list, album_titles: dict):
    """Compatibility stub for the removed Plex-source artist improve worker."""
    total = len(album_ids or [])
    with lock:
        state["improve_all"] = {
            "running": False,
            "global": False,
            "artist_id": artist_id,
            "current": 0,
            "total": total,
            "current_album_id": None,
            "current_album": None,
            "current_artist": None,
            "current_provider": None,
            "provider_status": {},
            "log": [],
            "result": None,
            "error": "Plex source improvement is disabled. Use the files library improve workflow.",
        }


def _mb_missing_release_group_ids_cache() -> set[str]:
    mod = sys.modules[__name__]
    cache = getattr(mod, "_MB_MISSING_RELEASE_GROUP_IDS", None)
    if not isinstance(cache, set):
        cache = set()
        setattr(mod, "_MB_MISSING_RELEASE_GROUP_IDS", cache)
    return cache


def _improve_one_album_item_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _improve_one_album_item(*args, **kwargs)


def _build_improve_items_from_editions_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_improve_items_from_editions(*args, **kwargs)


def _run_improve_all_albums_global_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_improve_all_albums_global(*args, **kwargs)


def _run_scan_profile_enrichment_inline_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_scan_profile_enrichment_inline(*args, **kwargs)


def _run_improve_all_albums_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_improve_all_albums(*args, **kwargs)


def _mb_missing_release_group_ids_cache_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mb_missing_release_group_ids_cache(*args, **kwargs)


_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}
