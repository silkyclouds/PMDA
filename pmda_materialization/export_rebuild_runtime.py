"""Runtime-owned full export-library materialization job."""

from __future__ import annotations

import logging
import re
import sys
import threading
from collections import defaultdict
from pathlib import Path
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "run_export_library_for_runtime",
            "trigger_export_library_async_for_runtime",
            "_run_export_library",
            "_trigger_export_library_async",
        }:
            continue
        globals()[name] = value


def run_export_library_for_runtime(runtime: Any) -> None:
    _bind_runtime(runtime)
    return _run_export_library()


def trigger_export_library_async_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> bool:
    _bind_runtime(runtime)
    return _trigger_export_library_async(*args, **kwargs)


def _run_export_library() -> None:
    """Background worker: build export library from Files editions by moving strict matched album folders."""
    if library_is_audit_mode():
        with lock:
            state["export_progress"] = {"running": False, "error": "Audit mode: export is disabled", "tracks_done": 0, "total_tracks": 0, "albums_done": 0, "total_albums": 0}
        return
    if _storage_power_saver_active():
        with lock:
            state["export_progress"] = {
                "running": False,
                "error": "Storage power saver is enabled; use disk-aware strict materialization instead of global export.",
                "tracks_done": 0,
                "total_tracks": 0,
                "albums_done": 0,
                "total_albums": 0,
            }
        logging.info(
            "[STORAGE] Refusing global export-library rebuild: disk-aware power saver is enabled."
        )
        return
    export_root = (EXPORT_ROOT or "").strip()
    if not export_root:
        with lock:
            state["export_progress"] = {"running": False, "error": "EXPORT_ROOT is not configured", "tracks_done": 0, "total_tracks": 0, "albums_done": 0, "total_albums": 0}
        return
    with lock:
        state["export_progress"] = {"running": True, "tracks_done": 0, "total_tracks": 0, "albums_done": 0, "total_albums": 0, "error": None}
    try:
        _, _, editions_by_id = _build_files_editions(scan_type="full", respect_scan_controls=False)
        export_editions: dict[int, dict] = {}
        skipped_broken = 0
        skipped_quarantine = 0
        skipped_empty = 0
        included_auto = 0
        held_for_review = 0
        tier_counts: dict[str, int] = defaultdict(int)
        dupe_root_cfg = str(getattr(sys.modules[__name__], "DUPE_ROOT", "/dupes") or "/dupes").strip() or "/dupes"
        incomplete_cfg = str(_get_config_from_db("INCOMPLETE_ALBUMS_TARGET_DIR") or "/dupes/incomplete_albums").strip()
        dupe_root = path_for_fs_access(Path(dupe_root_cfg))
        incomplete_root = path_for_fs_access(Path(incomplete_cfg))

        def _path_under(path_obj: Path, root_obj: Path) -> bool:
            try:
                return path_obj.resolve().is_relative_to(root_obj.resolve())
            except Exception:
                return False

        for album_id, edition in (editions_by_id or {}).items():
            if bool(edition.get("is_broken")):
                skipped_broken += 1
                continue
            ordered_paths = edition.get("ordered_paths") or []
            if not ordered_paths:
                skipped_empty += 1
                continue
            folder_raw = str(edition.get("folder") or "").strip()
            if folder_raw:
                folder_path = path_for_fs_access(Path(folder_raw))
                if _path_under(folder_path, dupe_root) or _path_under(folder_path, incomplete_root):
                    skipped_quarantine += 1
                    continue
            confidence_policy = _materialization_confidence_policy(edition)
            tier = str(confidence_policy.get("tier") or "unresolved")
            tier_counts[tier] += 1
            edition["materialization_confidence_tier"] = tier
            edition["materialization_confidence"] = float(confidence_policy.get("confidence") or 0.0)
            edition["materialization_decision_reason"] = str(confidence_policy.get("reason") or "")
            if not bool(confidence_policy.get("auto_materialize")):
                held_for_review += 1
                continue
            export_editions[album_id] = edition
            included_auto += 1
        logging.info(
            "[LIBRARY] [↻🔄] export selection: %d auto-materializable album(s), held_for_review=%d, tiers=%s, skipped [broken=%d, quarantine=%d, empty=%d]",
            len(export_editions),
            held_for_review,
            dict(sorted(tier_counts.items())),
            skipped_broken,
            skipped_quarantine,
            skipped_empty,
        )
        items_by_artist: dict[str, list[dict]] = defaultdict(list)
        total_tracks = 0
        total_albums = 0
        for edition in export_editions.values():
            artist_name = str(edition.get("artist_name") or edition.get("artist") or "").strip() or "Unknown Artist"
            built_items = _build_improve_items_from_editions(artist_name, [edition], None)
            if not built_items:
                continue
            items_by_artist[artist_name].extend(built_items)
            total_albums += len(built_items)
            for built in built_items:
                total_tracks += len(built.get("ordered_paths") or [])
        with lock:
            state["export_progress"]["total_tracks"] = total_tracks
            state["export_progress"]["total_albums"] = total_albums
            state["export_progress"]["confidence_tiers"] = dict(sorted(tier_counts.items()))
            state["export_progress"]["auto_materialize_albums"] = int(included_auto)
            state["export_progress"]["review_needed_albums"] = int(held_for_review)
            state["export_progress"]["skipped_broken"] = int(skipped_broken)
            state["export_progress"]["skipped_quarantine"] = int(skipped_quarantine)
            state["export_progress"]["skipped_empty"] = int(skipped_empty)
        tracks_done = 0
        albums_done = 0
        for artist_name, items in items_by_artist.items():
            moved_items = _move_publish_items_to_matched_library(
                artist_name,
                list(items),
                export_root=export_root,
            )
            try:
                _publish_files_library_artist_live_batches(
                    artist_name,
                    list(moved_items or []),
                    scan_id=None,
                    results_by_album_id={},
                )
            except Exception:
                logging.debug("Export library: failed to republish moved items for artist=%s", artist_name, exc_info=True)
            for moved_item in moved_items:
                tracks_done += len(moved_item.get("ordered_paths") or [])
                albums_done += 1
                with lock:
                    state["export_progress"]["tracks_done"] = tracks_done
                    state["export_progress"]["albums_done"] = albums_done
        with lock:
            state["export_progress"]["running"] = False
            state["export_progress"]["error"] = None
        _trigger_files_index_rebuild_async(reason="export_library_complete")
    except Exception as e:
        logging.exception("Export library failed: %s", e)
        with lock:
            state["export_progress"]["running"] = False
            state["export_progress"]["error"] = str(e)


def _trigger_export_library_async(reason: str = "manual") -> bool:
    """Queue a Files export-library rebuild without blocking the caller."""
    if library_is_audit_mode():
        return False
    if _get_library_mode() != "files":
        return False
    if _storage_should_defer_live_library_materialization():
        logging.info(
            "[STORAGE] Skipping async export-library queue (%s): disk-aware power-saver is active during scan.",
            str(reason or "manual").strip() or "manual",
        )
        return False
    if _storage_power_saver_active():
        logging.info(
            "[STORAGE] Skipping async export-library queue (%s): global materialization would wake the full array; use strict disk-aware export.",
            str(reason or "manual").strip() or "manual",
        )
        return False
    with lock:
        prog = state.get("export_progress") or {}
        if prog.get("running"):
            return False
    reason_norm = re.sub(r"[^a-z0-9]+", "-", str(reason or "manual").strip().lower()).strip("-") or "manual"

    def _runner() -> None:
        try:
            _run_export_library()
        except Exception:
            logging.exception("Async export library failed (%s)", reason_norm)

    threading.Thread(target=_runner, daemon=True, name=f"files-export-{reason_norm}").start()
    return True
