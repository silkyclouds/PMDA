"""Runtime-owned duplicate move background worker."""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any, List


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "background_dedupe_for_runtime",
            "background_dedupe",
        }:
            continue
        globals()[name] = value


def background_dedupe_for_runtime(runtime: Any, all_groups: List[dict]):
    _bind_runtime(runtime)
    return background_dedupe(all_groups)


def background_dedupe(all_groups: List[dict]):
    """
    Processes deduplication of all groups in a background thread.
    Updates stats in DB and in-memory state.
    Sets dedupe_current_group so the UI can show artist, album, winner, losers, destination.
    """
    ensure_dedupe_scan_id()
    dupe_root = getattr(sys.modules[__name__], "DUPE_ROOT", Path("/dupes"))
    with lock:
        state.update(
            deduping=True,
            dedupe_progress=0,
            dedupe_total=len(all_groups),
            dedupe_start_time=time.time(),
            dedupe_saved_this_run=0,
            scan_dupe_moved_count=0,
            scan_dupe_moved_mb=0,
            dedupe_current_group=None,
            dedupe_last_write=None,
        )

    total_moved = 0
    removed_count = 0
    artists_to_refresh = set()

    # Never move a folder that is any group's best (avoids moving both editions when duplicate groups slip through)
    best_folders = set()
    for g in all_groups:
        best = g.get("best")
        if best and best.get("folder"):
            p = path_for_fs_access(Path(best["folder"]))
            if p:
                best_folders.add(str(p))

    for g in all_groups:
        best = g.get("best", {})
        losers = g.get("losers", [])
        artist = g["artist"]
        album_title = best.get("title_raw", "")
        num_dupes = 1 + len(losers)
        # Ensure folder values are str for JSON (edition dict may store Path)
        current_group = {
            "artist": artist,
            "album": album_title,
            "num_dupes": num_dupes,
            "winner": {
                "title_raw": best.get("title_raw", ""),
                "album_id": best.get("album_id"),
                "folder": str(best.get("folder") or ""),
            },
            "losers": [
                {"title_raw": e.get("title_raw", ""), "album_id": e.get("album_id"), "folder": str(e.get("folder") or "")}
                for e in losers
            ],
            "destination": str(dupe_root),
            "status": "moving",
        }
        with lock:
            state["dedupe_current_group"] = current_group

        moved = perform_dedupe(g, best_folders=best_folders)
        removed_count += len(moved)
        group_saved = sum(item["size"] for item in moved)
        total_moved += group_saved
        artists_to_refresh.add(g["artist"])

        with lock:
            state["dedupe_progress"] += 1
            state["dedupe_saved_this_run"] = state.get("dedupe_saved_this_run", 0) + group_saved
            state["scan_dupe_moved_count"] = removed_count
            state["scan_dupe_moved_mb"] = total_moved
            state["dedupe_current_group"] = None
            logging.debug(f"background_dedupe(): processed group for '{artist}|{album_title}', dedupe_progress={state['dedupe_progress']}/{state['dedupe_total']}")
            # Remove this group from in-memory state so the list shrinks on next /api/duplicates
            # Only remove if still present (same ref can appear twice from AI merge, avoid ValueError)
            if artist in state["duplicates"]:
                lst = state["duplicates"][artist]
                if g in lst:
                    lst.remove(g)
                if not state["duplicates"][artist]:
                    del state["duplicates"][artist]
        # Remove from DB so /api/duplicates (and reload) shows shrinking list
        best_album_id = best.get("album_id")
        loser_album_ids = [e.get("album_id") for e in losers if e.get("album_id") is not None]
        if best_album_id is not None:
            _remove_dedupe_group_from_db(artist, best_album_id, loser_album_ids)

    # Update stats in DB
    increment_stat("space_saved", total_moved)
    increment_stat("removed_dupes", removed_count)
    notify_discord(
        f"🟢 Deduplication finished: {removed_count} duplicate folders moved, "
        f"{total_moved}  MB reclaimed."
    )
    logging.debug(f"background_dedupe(): updated stats: space_saved += {total_moved}, removed_dupes += {removed_count}")

    with lock:
        scan_id = state.get("scan_id")
        state["deduping"] = False
        state["dedupe_current_group"] = None
        state["dedupe_last_write"] = None
        state["dedupe_start_time"] = None
        state["dedupe_saved_this_run"] = 0
        # For "Last scan summary": dupes moved and space saved in this run (when auto-move was used)
        state["last_dedupe_moved_count"] = removed_count
        state["last_dedupe_saved_mb"] = total_moved
        state["scan_dupe_moved_count"] = removed_count
        state["scan_dupe_moved_mb"] = total_moved
    if scan_id is not None:
        update_dedupe_scan_summary(scan_id, total_moved, removed_count)
    logging.debug("background_dedupe(): deduping completed")
