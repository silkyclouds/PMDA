"""Runtime-bound duplicate review and manual dedupe action handlers."""
from __future__ import annotations

from typing import Any
import logging
import sqlite3
import threading
from pathlib import Path

_RUNTIME: Any | None = None
_EXTRACTED_NAMES = {
    "api_dedupe",
    "details",
    "_normalize_edition_as_best",
    "_run_dedupe_artist_one",
    "dedupe_artist",
    "_merge_bonus_tracks_for_group",
    "dedupe_move_track",
    "_dedupe_all_impl",
    "api_dedupe_all",
    "dedupe_all",
    "dedupe_merge_and_dedupe",
    "dedupe_selected",
}
_ORIGINAL_EXTRACTED_FUNCTIONS: dict[str, Any] = {}


def _bind_runtime(runtime: Any) -> None:
    global _RUNTIME
    _RUNTIME = runtime
    for name, value in vars(runtime).items():
        if name in _EXTRACTED_NAMES:
            original = _ORIGINAL_EXTRACTED_FUNCTIONS.get(name)
            if original is not None:
                globals()[name] = original
            continue
        own_wrapper = name.endswith("_for_runtime") and name[: -len("_for_runtime")] in _EXTRACTED_NAMES
        if name == "_bind_runtime" or own_wrapper:
            continue
        globals()[name] = value

def api_dedupe():
    with lock:
        deduping = state["deduping"]
        progress = state["dedupe_progress"]
        total = state["dedupe_total"]
        start_time = state.get("dedupe_start_time")
        saved_this_run = state.get("dedupe_saved_this_run", 0)
        current_group = state.get("dedupe_current_group")
        last_write = state.get("dedupe_last_write")

    percent = round(100 * progress / total, 1) if total else 0
    eta_seconds = None
    if start_time and total and progress > 0:
        elapsed = time.time() - start_time
        avg_per_group = elapsed / progress
        remaining = total - progress
        eta_seconds = max(0, int(remaining * avg_per_group))

    return jsonify(
        deduping=deduping,
        progress=progress,
        total=total,
        saved=get_stat("space_saved"),
        saved_this_run=saved_this_run,
        moved=get_stat("removed_dupes"),
        percent=percent,
        eta_seconds=eta_seconds,
        current_group=current_group,
        last_write=last_write,
    )


def details(artist, album_id):
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required", "requiresConfig": True}), 410
    art = artist.replace("_", " ").strip()
    g = _find_duplicate_group_by_artist_album(art, album_id, allow_library_build=True)
    if not (g and g.get("best") and g.get("losers")):
        return jsonify({}), 404

    editions = [g["best"]] + g["losers"]
    artist_rating_key = None
    best_track_titles: set[str] = set()
    try:
        best_folder = path_for_fs_access(Path(str(g.get("best", {}).get("folder") or "")))
        best_fallback_tracks = _duplicate_tracks_from_folder(best_folder, g.get("best") or {})
        best_track_titles = {
            str(t.get("title") or t.get("name") or "").strip().lower()
            for t in best_fallback_tracks
            if str(t.get("title") or t.get("name") or "").strip()
        }
    except Exception:
        best_track_titles = set()

    out = []
    rationale = g["best"].get("rationale", "")
    for i, e in enumerate(editions):
        folder_path = path_for_fs_access(Path(e["folder"])) if e.get("folder") else None
        is_best = i == 0
        if is_best:
            size_mb = safe_folder_size(folder_path) // (1024 * 1024) if folder_path else 0
        else:
            size_mb = e.get("size", 0) or (safe_folder_size(folder_path) // (1024 * 1024) if folder_path else 0)
        size_bytes = size_mb * (1024 * 1024)

        track_list = []
        if folder_path and folder_path.exists() and folder_path.is_dir():
            try:
                track_list = _duplicate_tracks_from_folder(folder_path, e)
                if not is_best and best_track_titles:
                    for t in track_list:
                        title_norm = str(t.get("title") or t.get("name") or "").strip().lower()
                        t["is_bonus"] = bool(title_norm and title_norm not in best_track_titles)
            except Exception:
                track_list = []

        br_out = (e.get("br", 0) // 1000) if isinstance(e.get("br"), int) else (e.get("br") or 0)
        sr_out = e.get("sr", 0) or 0
        bd_out = e.get("bd", 0) or 0
        if (br_out == 0 or sr_out == 0 or bd_out == 0) and track_list:
            br_from_tracks = next((t.get("bitrate") for t in track_list if t.get("bitrate")), None)
            if br_from_tracks is not None and br_out == 0:
                br_out = br_from_tracks if br_from_tracks < 100000 else br_from_tracks // 1000
        if (br_out == 0 or sr_out == 0 or bd_out == 0) and folder_path:
            try:
                _fmt_score, br_bps, sr_hz, bd_val, _ = analyse_format(folder_path)
                if br_out == 0 and br_bps:
                    br_out = br_bps // 1000 if br_bps >= 1000 else br_bps
                if sr_out == 0 and sr_hz:
                    sr_out = sr_hz
                if bd_out == 0 and bd_val:
                    bd_out = bd_val
            except Exception:
                pass

        thumb_data = _duplicate_cover_data_for_edition(e)
        try:
            thumb_url = _duplicate_album_thumb_url(int(e.get("album_id") or 0), folder_path)
        except Exception:
            thumb_url = ""
        path_str = str(folder_path) if folder_path is not None else ""
        out.append({
            "thumb_data": thumb_data,
            "thumb_url": thumb_url,
            "title_raw": e.get("title_raw") or "",
            "size": size_bytes,
            "fmt": e.get("fmt_text", e.get("fmt", "")),
            "br": br_out,
            "sr": sr_out,
            "bd": bd_out,
            "path": path_str,
            "folder": path_str,
            "album_id": e["album_id"],
            "track_count": len(track_list),
            "tracks": track_list,
            "musicbrainz_id": e.get("musicbrainz_id"),
            "match_verified_by_ai": bool(e.get("match_verified_by_ai", False)),
        })
    return jsonify(
        artist=art,
        album=g["best"]["title_raw"],
        artist_id=artist_rating_key,
        editions=out,
        rationale=rationale,
        merge_list=g["best"].get("merge_list", []),
    )


def _normalize_edition_as_best(edition: dict, artist: str) -> dict:
    """Ensure an edition has the keys expected for group['best']."""
    e = dict(edition)
    if "title_raw" not in e or e["title_raw"] is None:
        folder_name = Path(str(e.get("folder") or "")).name
        e["title_raw"] = folder_name or ""
    e.setdefault("album_norm", (e.get("title_raw") or "").lower())
    e.setdefault("fmt_text", e.get("fmt", ""))
    e.setdefault("br", 0)
    e.setdefault("sr", 0)
    e.setdefault("bd", 0)
    e.setdefault("rationale", "")
    e.setdefault("merge_list", [])
    e.setdefault("used_ai", False)
    e.setdefault("meta", {})
    e.setdefault("dur", 0)
    e.setdefault("discs", 1)
    return e


def _run_dedupe_artist_one(art: str, album_id: int, keep_edition_album_id: Optional[int], group_copy: dict) -> None:
    """Run dedupe for one group in a background thread. Updates state and DB."""
    with lock:
        state["deduping"] = True
        state["dedupe_progress"] = 0
        state["dedupe_total"] = 1
    try:
        moved_list = perform_dedupe(group_copy, manual_override=True)
        removed_count = len(moved_list)
        total_mb = sum(item["size"] for item in moved_list)
        increment_stat("removed_dupes", removed_count)
        increment_stat("space_saved", total_mb)
        logging.debug(f"dedupe_artist(): removed {removed_count} dupes, freed {total_mb} MB")

        with lock:
            groups = state["duplicates"].get(art, [])
            groups[:] = [gr for gr in groups if not _group_contains_album_id(gr, album_id)]
            if not groups:
                state["duplicates"].pop(art, None)
            con = sqlite3.connect(str(STATE_DB_FILE))
            cur = con.cursor()
            cur.execute("DELETE FROM duplicates_best WHERE artist = ? AND album_id = ?", (art, group_copy.get("album_id")))
            cur.execute("DELETE FROM duplicates_loser WHERE artist = ? AND album_id = ?", (art, group_copy.get("album_id")))
            con.commit()
            con.close()
            sid = state.get("scan_id")
            state["dedupe_progress"] = 1
            state["deduping"] = False
        if sid is not None:
            update_dedupe_scan_summary(sid, total_mb, removed_count)
    except Exception as e:
        logging.exception("dedupe_artist background: %s", e)
        with lock:
            state["deduping"] = False
            state["dedupe_progress"] = 0
            state["dedupe_total"] = 0


def dedupe_artist(artist):
    r = _requires_config()
    if r is not None:
        return r
    ensure_dedupe_scan_id()
    art = artist.replace("_", " ")
    data = request.get_json() or {}
    raw = data.get("album_id")
    album_id = int(raw) if raw is not None else None
    keep_edition_album_id = data.get("keep_edition_album_id")
    if keep_edition_album_id is not None:
        keep_edition_album_id = int(keep_edition_album_id)

    g = _find_duplicate_group_by_artist_album(art, album_id, allow_library_build=True)
    if g is None:
        return jsonify({"error": "Group not found"}), 404
    if keep_edition_album_id is not None:
        editions = [g.get("best", {})] + list(g.get("losers") or [])
        kept = None
        losers = []
        for e in editions:
            aid = int(e.get("album_id") or 0)
            if aid == int(keep_edition_album_id):
                kept = e
            elif aid:
                losers.append(e)
        if kept is None or not losers:
            return jsonify({"error": "Invalid keep_edition_album_id or no editions to remove"}), 400
        group_copy = {
            "artist": art,
            "album_id": int(kept.get("album_id") or g.get("album_id") or album_id),
            "best": _normalize_edition_as_best(kept, art),
            "losers": losers,
            "dupe_signal": g.get("dupe_signal"),
            "no_move": g.get("no_move"),
            "manual_review": g.get("manual_review"),
            "same_folder": g.get("same_folder"),
        }
        _record_ai_override_event(
            domain="dedupe",
            target_key=f"{art}|{int(group_copy.get('album_id') or album_id or 0)}",
            action="manual_keep_selection",
            details={"artist": art, "keep_edition_album_id": int(keep_edition_album_id)},
        )
    else:
        import copy
        group_copy = copy.deepcopy(g)
        _record_ai_override_event(
            domain="dedupe",
            target_key=f"{art}|{int(group_copy.get('album_id') or album_id or 0)}",
            action="manual_dedupe_run",
            details={"artist": art},
        )
    if bool(group_copy.get("same_folder")):
        return jsonify({"error": "Same-folder duplicates are metadata-only entries; nothing to move on disk."}), 400

    threading.Thread(target=_run_dedupe_artist_one, args=(art, album_id, keep_edition_album_id, group_copy), daemon=True).start()
    return jsonify(status="started", message="Deduplication started", moved=[]), 202


def _merge_bonus_tracks_for_group(g: dict) -> None:
    """Move bonus tracks from loser edition folders into the kept files-mode edition folder."""
    merge_list = g["best"].get("merge_list") or []
    if not merge_list:
        return
    merge_set = {(t.strip().lower()): t.strip() for t in merge_list}
    best_folder = path_for_fs_access(Path(g["best"]["folder"]))
    for loser in g["losers"]:
        source_folder = path_for_fs_access(Path(loser["folder"]))
        tracks_iter = _duplicate_tracks_from_folder(source_folder, loser)
        for t in tracks_iter:
            title = (t.get("title") or t.get("name") or "").strip()
            if not title or title.lower() not in merge_set:
                continue
            raw_path = t.get("path")
            if not raw_path:
                continue
            track_path = Path(raw_path)
            try:
                src_resolved = path_for_fs_access(track_path).resolve()
                base_resolved = source_folder.resolve()
            except Exception:
                continue
            if not src_resolved.is_file():
                continue
            if src_resolved.suffix.lower() not in _MOVE_TRACK_EXTENSIONS:
                continue
            try:
                if not src_resolved.is_relative_to(base_resolved):
                    continue
            except AttributeError:
                if not str(src_resolved).startswith(str(base_resolved)):
                    continue
            dest_file = best_folder / src_resolved.name
            if dest_file.exists():
                stem, suf = dest_file.stem, dest_file.suffix
                n = 1
                while dest_file.exists():
                    dest_file = best_folder / f"{stem} ({n}){suf}"
                    n += 1
            try:
                safe_move(str(src_resolved), str(dest_file))
                logging.info("merge_bonus: moved %s -> %s", src_resolved.name, best_folder)
            except Exception as e:
                logging.warning("merge_bonus: failed %s -> %s: %s", src_resolved, dest_file, e)


def dedupe_move_track(artist):
    """
    Move a single bonus track file from one edition folder to the kept edition folder.
    Body: { "album_id": "<group id>", "source_index": int, "track_path": str, "target_index": int }.
    """
    r = _requires_config()
    if r is not None:
        return r
    art = artist.replace("_", " ")
    data = request.get_json() or {}
    raw_album_id = data.get("album_id")
    album_id = int(raw_album_id) if raw_album_id is not None else None
    source_index = data.get("source_index")
    target_index = data.get("target_index")
    track_path_raw = data.get("track_path")

    if album_id is None or source_index is None or target_index is None or not track_path_raw:
        return jsonify(success=False, message="Missing album_id, source_index, target_index or track_path"), 400

    try:
        source_index = int(source_index)
        target_index = int(target_index)
    except (TypeError, ValueError):
        return jsonify(success=False, message="source_index and target_index must be integers"), 400

    with lock:
        groups = state["duplicates"].get(art)
        if groups is None:
            groups = load_scan_from_db().get(art, [])
        g = next((gr for gr in groups if gr["album_id"] == album_id), None)

    if g is None:
        return jsonify(success=False, message="Duplicate group not found"), 404

    editions = [g["best"]] + g["losers"]
    if source_index < 0 or source_index >= len(editions) or target_index < 0 or target_index >= len(editions):
        return jsonify(success=False, message="Invalid source_index or target_index"), 400
    if source_index == target_index:
        return jsonify(success=False, message="Source and target editions must differ"), 400

    source_folder = path_for_fs_access(Path(editions[source_index]["folder"]))
    target_folder = path_for_fs_access(Path(editions[target_index]["folder"]))

    track_path = Path(track_path_raw)
    try:
        src_resolved = track_path.resolve()
        base_resolved = source_folder.resolve()
    except Exception as e:
        return jsonify(success=False, message=f"Invalid path: {e}"), 400

    if not src_resolved.is_file():
        return jsonify(success=False, message="track_path is not a file"), 400
    if src_resolved.suffix.lower() not in _MOVE_TRACK_EXTENSIONS:
        return jsonify(success=False, message="File type not allowed for move"), 400
    try:
        if not src_resolved.is_relative_to(base_resolved):
            return jsonify(success=False, message="Track must be inside the source edition folder"), 400
    except AttributeError:
        if not str(src_resolved).startswith(str(base_resolved)):
            return jsonify(success=False, message="Track must be inside the source edition folder"), 400

    dest_file = target_folder / src_resolved.name
    if dest_file.exists():
        stem, suf = dest_file.stem, dest_file.suffix
        n = 1
        while dest_file.exists():
            dest_file = target_folder / f"{stem} ({n}){suf}"
            n += 1

    try:
        safe_move(str(src_resolved), str(dest_file))
    except Exception as e:
        logging.exception("move-track: move failed %s → %s", src_resolved, dest_file)
        return jsonify(success=False, message=str(e)), 500

    return jsonify(success=True, message="Track moved to kept edition", dest=str(dest_file)), 200


def _dedupe_all_impl():
    """Shared logic for POST /dedupe/all and POST /api/dedupe/all."""
    r = _requires_config()
    if r is not None:
        return r
    if library_is_audit_mode():
        admin_gate = _require_admin_json()
        if admin_gate is not None:
            return admin_gate
    with lock:
        all_groups = [g for lst in state["duplicates"].values() for g in lst]
        if not state["duplicates"]:
            state["duplicates"] = load_scan_from_db()
        all_groups = [g for lst in state["duplicates"].values() for g in lst]
    if not all_groups:
        return "", 204
    threading.Thread(target=background_dedupe, args=(all_groups,), daemon=True).start()
    return "", 204


def api_dedupe_all():
    return _dedupe_all_impl()


def dedupe_all():
    return _dedupe_all_impl()


def dedupe_merge_and_dedupe():
    """
    First merge bonus tracks (from merge_list) into the kept edition for every group
    that has extra tracks, then run full dedupe (move loser folders, update Plex).
    """
    r = _requires_config()
    if r is not None:
        return r
    if library_is_audit_mode():
        admin_gate = _require_admin_json()
        if admin_gate is not None:
            return admin_gate
    with lock:
        if not state["duplicates"]:
            state["duplicates"] = load_scan_from_db()
        all_groups = [g for lst in state["duplicates"].values() for g in lst]

    for g in all_groups:
        if g["best"].get("merge_list"):
            try:
                _merge_bonus_tracks_for_group(g)
            except Exception as e:
                logging.warning("merge_and_dedupe: merge_bonus failed for %s: %s", g.get("artist"), e)

    threading.Thread(target=background_dedupe, args=(all_groups,), daemon=True).start()
    return "", 204


def dedupe_selected():
    r = _requires_config()
    if r is not None:
        return r
    if library_is_audit_mode():
        admin_gate = _require_admin_json()
        if admin_gate is not None:
            return admin_gate
    ensure_dedupe_scan_id()
    data = request.get_json() or {}
    selected = data.get("selected", [])
    moved_list: List[Dict] = []
    total_moved = 0
    removed_count = 0
    artists_to_refresh = set()

    for sel in selected:
        try:
            art_key, aid_str = sel.split("||", 1)
            art = art_key.replace("_", " ").strip()
            album_id = int(aid_str)
        except Exception:
            continue
        g = _find_duplicate_group_by_artist_album(art, album_id, allow_library_build=True)
        if not g:
            logging.debug("dedupe_selected(): group not found for %s", sel)
            continue
        if bool(g.get("same_folder")):
            logging.debug("dedupe_selected(): skipping same-folder group for %s", sel)
            continue
        logging.debug("dedupe_selected(): processing group for artist '%s', album_id=%s", art, album_id)
        moved = perform_dedupe(g, manual_override=True)
        moved_list.extend(moved)
        total_moved += sum(item["size"] for item in moved)
        removed_count += len(moved)
        artists_to_refresh.add(art)
        best_album_id = int(g.get("album_id") or g.get("best", {}).get("album_id") or 0)
        loser_album_ids = [int((e or {}).get("album_id") or 0) for e in (g.get("losers") or [])]
        loser_album_ids = [aid for aid in loser_album_ids if aid]
        if best_album_id:
            _remove_dedupe_group_from_db(art, best_album_id, loser_album_ids)
        with lock:
            groups = state["duplicates"].get(art, [])
            groups[:] = [gr for gr in groups if not _group_contains_album_id(gr, album_id)]
            if not groups and art in state["duplicates"]:
                del state["duplicates"][art]

    increment_stat("removed_dupes", removed_count)
    increment_stat("space_saved", total_moved)
    logging.debug(f"dedupe_selected(): removed {removed_count} dupes, freed {total_moved} MB")

    with lock:
        sid = state.get("scan_id")
    if sid is not None:
        update_dedupe_scan_summary(sid, total_moved, removed_count)

    return jsonify(moved=moved_list), 200


def api_dedupe_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_dedupe(*args, **kwargs)

def details_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return details(*args, **kwargs)

def _normalize_edition_as_best_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _normalize_edition_as_best(*args, **kwargs)

def _run_dedupe_artist_one_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_dedupe_artist_one(*args, **kwargs)

def dedupe_artist_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return dedupe_artist(*args, **kwargs)

def _merge_bonus_tracks_for_group_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _merge_bonus_tracks_for_group(*args, **kwargs)

def dedupe_move_track_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return dedupe_move_track(*args, **kwargs)

def _dedupe_all_impl_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _dedupe_all_impl(*args, **kwargs)

def api_dedupe_all_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_dedupe_all(*args, **kwargs)

def dedupe_all_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return dedupe_all(*args, **kwargs)

def dedupe_merge_and_dedupe_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return dedupe_merge_and_dedupe(*args, **kwargs)

def dedupe_selected_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return dedupe_selected(*args, **kwargs)


_ORIGINAL_EXTRACTED_FUNCTIONS.update({
    "api_dedupe": api_dedupe,
    "details": details,
    "_normalize_edition_as_best": _normalize_edition_as_best,
    "_run_dedupe_artist_one": _run_dedupe_artist_one,
    "dedupe_artist": dedupe_artist,
    "_merge_bonus_tracks_for_group": _merge_bonus_tracks_for_group,
    "dedupe_move_track": dedupe_move_track,
    "_dedupe_all_impl": _dedupe_all_impl,
    "api_dedupe_all": api_dedupe_all,
    "dedupe_all": dedupe_all,
    "dedupe_merge_and_dedupe": dedupe_merge_and_dedupe,
    "dedupe_selected": dedupe_selected,
})
