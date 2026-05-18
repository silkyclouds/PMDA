"""Runtime-owned duplicate move and filesystem placement helpers."""

from __future__ import annotations

import logging
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, List, Optional


_LOCAL_NAMES = {
    "fetch_cover_as_base64",
    "_files_forget_album_folder_global",
    "_duplicate_tracks_from_folder",
    "_duplicate_cover_data_for_edition",
    "_next_available_folder_path",
    "_hardlink_tree",
    "_place_folder_with_strategy",
    "_winner_destination_for_folder",
    "_normalize_winner_folder_to_canonical_root",
    "perform_dedupe",
    "_bind_runtime",
    "fetch_cover_as_base64_for_runtime",
    "files_forget_album_folder_global_for_runtime",
    "duplicate_tracks_from_folder_for_runtime",
    "duplicate_cover_data_for_edition_for_runtime",
    "next_available_folder_path_for_runtime",
    "hardlink_tree_for_runtime",
    "place_folder_with_strategy_for_runtime",
    "winner_destination_for_folder_for_runtime",
    "normalize_winner_folder_to_canonical_root_for_runtime",
    "perform_dedupe_for_runtime",
}


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in _LOCAL_NAMES:
            continue
        globals()[name] = value


def fetch_cover_as_base64_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Optional[str]:
    _bind_runtime(runtime)
    return fetch_cover_as_base64(*args, **kwargs)


def files_forget_album_folder_global_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> bool:
    _bind_runtime(runtime)
    return _files_forget_album_folder_global(*args, **kwargs)


def duplicate_tracks_from_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> list[dict]:
    _bind_runtime(runtime)
    return _duplicate_tracks_from_folder(*args, **kwargs)


def duplicate_cover_data_for_edition_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Optional[str]:
    _bind_runtime(runtime)
    return _duplicate_cover_data_for_edition(*args, **kwargs)


def next_available_folder_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Path:
    _bind_runtime(runtime)
    return _next_available_folder_path(*args, **kwargs)


def hardlink_tree_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> None:
    _bind_runtime(runtime)
    return _hardlink_tree(*args, **kwargs)


def place_folder_with_strategy_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Path:
    _bind_runtime(runtime)
    return _place_folder_with_strategy(*args, **kwargs)


def winner_destination_for_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Optional[Path]:
    _bind_runtime(runtime)
    return _winner_destination_for_folder(*args, **kwargs)


def normalize_winner_folder_to_canonical_root_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Optional[Path]:
    _bind_runtime(runtime)
    return _normalize_winner_folder_to_canonical_root(*args, **kwargs)


def perform_dedupe_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> List[dict]:
    _bind_runtime(runtime)
    return perform_dedupe(*args, **kwargs)


def fetch_cover_as_base64(album_id: int) -> Optional[str]:
    """
    Legacy placeholder kept for callers that still pass backend album ids.
    Files mode resolves duplicate covers from local folders instead.
    """
    return None


def _files_forget_album_folder_global(folder: Path | str) -> bool:
    """
    Remove one moved folder from Files-mode caches and live PostgreSQL index.
    Returns True when at least one row/cache entry was removed.
    """
    try:
        if _get_library_mode() != "files":
            return False
    except Exception:
        return False

    changed = False
    try:
        key = _album_folder_cache_key(folder)
    except Exception:
        key = str(folder)

    # 1) SQLite scan/published caches
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.execute("DELETE FROM files_album_scan_cache WHERE folder_path = ?", (key,))
        changed = changed or int(cur.rowcount or 0) > 0
        cur.execute("DELETE FROM files_library_published_albums WHERE folder_path = ?", (key,))
        changed = changed or int(cur.rowcount or 0) > 0
        con.commit()
        con.close()
    except Exception:
        logging.debug("Files cache/published cleanup failed for %s", key, exc_info=True)

    # 2) PostgreSQL live index
    candidates: list[str] = []
    for raw in (folder, key):
        txt = str(raw or "").strip()
        if txt:
            candidates.append(txt)
            try:
                candidates.append(str(path_for_fs_access(Path(txt)).resolve()))
            except Exception:
                pass
    candidate_paths = sorted({c for c in candidates if c})
    if candidate_paths:
        conn_pg = _files_pg_connect()
        if conn_pg is not None:
            try:
                with conn_pg.transaction():
                    with conn_pg.cursor() as cur_pg:
                        cur_pg.execute(
                            "SELECT id, artist_id FROM files_albums WHERE folder_path = ANY(%s)",
                            (candidate_paths,),
                        )
                        rows = cur_pg.fetchall() or []
                        if rows:
                            album_ids = [int(r[0]) for r in rows if int(r[0] or 0) > 0]
                            artist_ids = sorted({int(r[1]) for r in rows if int(r[1] or 0) > 0})
                            if album_ids:
                                cur_pg.execute("DELETE FROM files_albums WHERE id = ANY(%s)", (album_ids,))
                                changed = changed or int(cur_pg.rowcount or 0) > 0
                            if artist_ids:
                                cur_pg.execute(
                                    """
                                    UPDATE files_artists a
                                    SET album_count = s.album_count,
                                        track_count = s.track_count,
                                        broken_albums_count = s.broken_albums_count,
                                        updated_at = NOW()
                                    FROM (
                                        SELECT
                                            artist_id,
                                            COUNT(*) AS album_count,
                                            COALESCE(SUM(track_count), 0) AS track_count,
                                            COALESCE(SUM(CASE WHEN is_broken THEN 1 ELSE 0 END), 0) AS broken_albums_count
                                        FROM files_albums
                                        WHERE artist_id = ANY(%s)
                                        GROUP BY artist_id
                                    ) s
                                    WHERE a.id = s.artist_id
                                    """,
                                    (artist_ids,),
                                )
                                cur_pg.execute(
                                    """
                                    UPDATE files_artists a
                                    SET album_count = 0,
                                        track_count = 0,
                                        broken_albums_count = 0,
                                        updated_at = NOW()
                                    WHERE a.id = ANY(%s)
                                      AND NOT EXISTS (
                                          SELECT 1
                                          FROM files_albums alb
                                          WHERE alb.artist_id = a.id
                                      )
                                    """,
                                    (artist_ids,),
                                )
            except Exception:
                logging.debug("Files PG cleanup failed for folder=%s", key, exc_info=True)
            finally:
                try:
                    conn_pg.close()
                except Exception:
                    pass

    if changed:
        _files_cache_invalidate_all()
    return changed


def _duplicate_tracks_from_folder(folder: Path, edition_payload: dict | None = None) -> list[dict]:
    """
    Build a best-effort track list from folder audio files for duplicate detail modal.
    Used when DB-backed track fetch is unavailable for an edition.
    """
    if not folder or not folder.exists() or not folder.is_dir():
        return []
    item = edition_payload if isinstance(edition_payload, dict) else {}
    try:
        entries = _files_build_track_entries_from_item(item, folder)
    except Exception:
        entries = []
    out: list[dict] = []
    for i, tr in enumerate(entries):
        title = str(tr.get("title") or "").strip() or f"Track {i + 1}"
        dur_sec = int(tr.get("duration_sec") or 0)
        raw_br = int(tr.get("bitrate") or 0)
        br_kbps = raw_br if 0 < raw_br < 100000 else (raw_br // 1000 if raw_br >= 100000 else None)
        out.append(
            {
                "idx": int(tr.get("track_num") or (i + 1)),
                "title": title,
                "name": title,
                "dur": max(0, dur_sec) * 1000,
                "duration": max(0, dur_sec),
                "format": str(tr.get("format") or "").strip() or None,
                "bitrate": br_kbps,
                "is_bonus": False,
                "path": str(tr.get("file_path") or "").strip() or None,
            }
        )
    return out


def _duplicate_cover_data_for_edition(edition_payload: dict) -> Optional[str]:
    """
    Resolve duplicate edition cover as data URI:
    1) local cover file / embedded cover from folder
    """
    folder_path = None
    try:
        folder_raw = str((edition_payload or {}).get("folder") or "").strip()
        if folder_raw:
            folder_path = path_for_fs_access(Path(folder_raw))
    except Exception:
        folder_path = None
    if folder_path and folder_path.exists() and folder_path.is_dir():
        local_cover = _get_local_cover_data_uri_for_vision(folder_path)
        if local_cover:
            return local_cover
    try:
        aid = int((edition_payload or {}).get("album_id") or 0)
    except Exception:
        aid = 0
    if aid > 0:
        return fetch_cover_as_base64(aid)
    return None


def _next_available_folder_path(base: Path) -> Path:
    if not base.exists():
        return base
    parent = base.parent
    stem = base.name
    idx = 1
    while True:
        candidate = parent / f"{stem} ({idx})"
        if not candidate.exists():
            return candidate
        idx += 1


def _hardlink_tree(src: Path, dst: Path) -> None:
    """Create a directory mirror with hardlinked files."""
    dst.mkdir(parents=True, exist_ok=False)
    for entry in src.rglob("*"):
        rel = entry.relative_to(src)
        target = dst / rel
        if entry.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        if not entry.is_file():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(str(entry), str(target))


def _place_folder_with_strategy(src: Path, dst: Path, strategy: str) -> Path:
    mode = str(strategy or "move").strip().lower()
    if mode not in {"move", "hardlink", "symlink", "copy"}:
        mode = "move"
    target = _next_available_folder_path(dst)
    target.parent.mkdir(parents=True, exist_ok=True)
    if mode == "move":
        safe_move(str(src), str(target))
        return target
    if mode == "copy":
        shutil.copytree(str(src), str(target))
        return target
    if mode == "symlink":
        os.symlink(str(src), str(target), target_is_directory=True)
        return target
    # hardlink mode
    try:
        _hardlink_tree(src, target)
    except Exception as exc:
        logging.warning("Winner placement hardlink failed (%s), falling back to copy for %s", exc, src)
        shutil.copytree(str(src), str(target))
    return target


def _winner_destination_for_folder(best_folder: Path) -> Optional[Path]:
    winner_row = _winner_source_row()
    if not winner_row:
        return None
    winner_root_raw = _normalize_root_path(winner_row.get("path"))
    if not winner_root_raw:
        return None
    winner_root = path_for_fs_access(Path(winner_root_raw))
    src_folder = path_for_fs_access(Path(best_folder))
    if not src_folder.exists() or not src_folder.is_dir():
        return None
    try:
        src_resolved = src_folder.resolve()
    except Exception:
        src_resolved = src_folder
    try:
        winner_resolved = winner_root.resolve()
    except Exception:
        winner_resolved = winner_root
    try:
        src_resolved.relative_to(winner_resolved)
        return None
    except Exception:
        pass
    rel: Optional[Path] = None
    current_row = _source_row_for_path(src_resolved, enabled_only=False)
    if current_row:
        current_root_raw = _normalize_root_path(current_row.get("path"))
        if current_root_raw:
            current_root = path_for_fs_access(Path(current_root_raw))
            try:
                rel = src_resolved.relative_to(current_root.resolve())
            except Exception:
                rel = None
    if rel is None:
        rel_guess = relative_path_under_known_roots(src_resolved)
        if rel_guess is not None:
            rel = rel_guess
    if rel is None or str(rel).strip() == "":
        rel = Path(src_resolved.name)
    return winner_root / rel


def _normalize_winner_folder_to_canonical_root(group: dict) -> Optional[Path]:
    """
    Ensure dedupe winner lives under the configured winner source root.
    Returns the new folder path when moved/linked/copied, or None when unchanged.
    """
    best = group.get("best") if isinstance(group, dict) else None
    if not isinstance(best, dict):
        return None
    best_folder_raw = str(best.get("folder") or "").strip()
    if not best_folder_raw:
        return None
    best_folder = path_for_fs_access(Path(best_folder_raw))
    if not best_folder.exists() or not best_folder.is_dir():
        return None
    destination = _winner_destination_for_folder(best_folder)
    if destination is None:
        return None
    strategy = str(getattr(sys.modules[__name__], "LIBRARY_WINNER_PLACEMENT_STRATEGY", "move") or "move").strip().lower()
    if strategy not in {"move", "hardlink", "symlink", "copy"}:
        strategy = "move"
    try:
        _files_watcher_suppress_folder(best_folder, seconds=180.0, reason="pmda_winner_normalize")
    except Exception:
        pass
    moved_to = _place_folder_with_strategy(best_folder, destination, strategy)
    try:
        _files_watcher_suppress_folder(moved_to, seconds=180.0, reason="pmda_winner_normalize")
    except Exception:
        pass
    if strategy == "move":
        _files_forget_album_folder_global(best_folder)
    best["folder"] = str(moved_to)
    try:
        group["best"] = best
    except Exception:
        pass
    logging.info(
        "Winner normalized to canonical source root via %s: %s -> %s",
        strategy,
        best_folder,
        moved_to,
    )
    return moved_to


def perform_dedupe(group: dict, best_folders: set = None, manual_override: bool = False) -> List[dict]:
    """
    Move each "loser" folder out to DUPE_ROOT and return a list of dicts describing each moved item.
    Cover is fetched after moves so the first group does not block on artwork retrieval.
    If best_folders is provided, never move a loser whose folder is in that set (keeps one edition per album).
    """
    moved_items: List[dict] = []
    artist = group["artist"]
    best_title = group["best"]["title_raw"]
    if (not manual_override) and (
        bool(group.get("no_move")) or bool(group.get("manual_review")) or bool(group.get("same_folder"))
    ):
        logging.info(
            "perform_dedupe(): skipped group for artist=%s album=%s (auto dedupe guard: no_move/manual_review/same_folder)",
            artist,
            best_title,
        )
        return moved_items
    if best_folders is None:
        best_folders = set()
    best_folder = None
    try:
        best_folder = path_for_fs_access(Path(group.get("best", {}).get("folder")))
    except Exception:
        best_folder = None

    num_losers = len(group["losers"])
    for idx, loser in enumerate(group["losers"], 1):
        src_folder = Path(loser["folder"])
        # Never move a folder that is any group's best (safeguard when duplicate groups exist)
        src_resolved = path_for_fs_access(src_folder)
        if best_folder and src_resolved and str(src_resolved) == str(best_folder):
            logging.warning("perform_dedupe(): skipping loser (same folder as best) – %s", src_folder)
            continue
        if best_folders and src_resolved and str(src_resolved) in best_folders:
            logging.warning("perform_dedupe(): skipping loser (folder is another group's best) – %s", src_folder)
            continue
        # Skip if the source folder is absent (e.g. already moved or path mapping issue)
        if not src_folder.exists():
            logging.warning(f"perform_dedupe(): source folder missing – {src_folder}; skipping.")
            continue
        base_dst = build_dupe_destination(
            src_folder,
            artist_hint=str(artist or ""),
            album_hint=str(loser.get("title_raw") or src_folder.name or ""),
        )
        dst = base_dst
        counter = 1
        while dst.exists():
            candidate = base_dst.parent / f"{base_dst.name} ({counter})"
            if not candidate.exists():
                dst = candidate
                break
            counter += 1
        dst.parent.mkdir(parents=True, exist_ok=True)

        logging.info("Moving dupe %s/%s: %s  →  %s", idx, num_losers, src_folder, dst)
        logging.debug("perform_dedupe(): moving %s → %s", src_folder, dst)
        try:
            try:
                _files_watcher_suppress_folder(src_folder, seconds=180.0, reason="pmda_move_dedupe")
            except Exception:
                pass
            safe_move(str(src_folder), str(dst))
            try:
                _files_watcher_suppress_folder(dst, seconds=180.0, reason="pmda_move_dedupe")
            except Exception:
                pass
            # Keep Files-mode browsing consistent: the loser is now outside FILES_ROOTS.
            _files_forget_album_folder_global(src_folder)
            with lock:
                state["dedupe_last_write"] = {"path": str(dst), "at": time.time()}
            logging.info("Moved to /dupes: %s", dst)
        except Exception as move_err:
            logging.error("perform_dedupe(): move failed for %s → %s – %s",
                          src_folder, dst, move_err)
            continue

        # warn if something prevented full deletion (e.g. Thumbs.db)
        if src_folder.exists():
            logging.warning("perform_dedupe(): %s was not fully removed (left‑over non‑audio files?)", src_folder)
            notify_discord(f"⚠ Folder **{src_folder.name}** could not be fully removed (non‑audio files locked?). Check manually.")

        size_mb = folder_size(dst) // (1024 * 1024)
        fmt_text = loser.get("fmt_text", loser.get("fmt", ""))
        br_kbps = loser["br"] // 1000
        sr = loser["sr"]
        bd = loser["bd"]

        # Record move in scan_moves table
        moved_at = time.time()
        scan_id = None
        with lock:
            scan_id = state.get("scan_id")

        if scan_id:
            try:
                winner = group.get("best") or {}
                winner_album_id = int(winner.get("album_id") or 0)
                winner_title = str(winner.get("title_raw") or winner.get("album_norm") or "")
                winner_path = str(winner.get("folder") or "")
                decision_provider = _normalize_identity_provider(
                    str(
                        winner.get("strict_match_provider")
                        or (winner.get("meta") or {}).get("primary_metadata_source")
                        or ""
                    )
                )
                decision_reason = str(group.get("dupe_signal") or "").strip() or str(winner.get("rationale") or "").strip()
                decision_confidence = float(winner.get("strict_tracklist_score") or 0.0)
                if bool(winner.get("strict_match_verified")):
                    decision_confidence = max(decision_confidence, 1.0)

                def _write_move() -> None:
                    con = _state_connect()
                    try:
                        cur = con.cursor()
                        _insert_scan_move_row(
                            cur,
                            scan_id=int(scan_id),
                            artist=str(artist or ""),
                            album_id=int(loser_id or 0),
                            original_path=str(src_folder),
                            moved_to_path=str(dst),
                            size_mb=int(size_mb or 0),
                            moved_at=moved_at,
                            album_title=str(loser.get("title_raw") or best_title or ""),
                            fmt_text=str(fmt_text or ""),
                            move_reason="dedupe",
                            winner_album_id=winner_album_id or None,
                            winner_title=winner_title,
                            winner_path=winner_path,
                            decision_source="pipeline_dedupe",
                            decision_provider=decision_provider,
                            decision_reason=decision_reason,
                            decision_confidence=decision_confidence,
                            details={
                                "kind": "dedupe",
                                "winner": {
                                    "album_id": winner_album_id,
                                    "title": winner_title,
                                    "folder": winner_path,
                                    "fmt_text": str(winner.get("fmt_text") or ""),
                                },
                                "moved": {
                                    "album_id": int(loser_id or 0),
                                    "title": str(loser.get("title_raw") or best_title or ""),
                                    "folder": str(src_folder),
                                    "fmt_text": str(fmt_text or ""),
                                },
                                "analysis": {
                                    "dupe_signal": str(group.get("dupe_signal") or ""),
                                    "no_move": bool(group.get("no_move")),
                                    "manual_review": bool(group.get("manual_review")),
                                    "same_folder": bool(group.get("same_folder")),
                                    "rationale": str(winner.get("rationale") or ""),
                                    "strict_match_verified": bool(winner.get("strict_match_verified")),
                                    "strict_match_provider": str(winner.get("strict_match_provider") or ""),
                                    "strict_reject_reason": str(winner.get("strict_reject_reason") or ""),
                                    "strict_tracklist_score": float(winner.get("strict_tracklist_score") or 0.0),
                                    "match_verified_by_ai": bool(winner.get("match_verified_by_ai")),
                                    "dupe_evidence": list(winner.get("dupe_evidence") or []),
                                },
                            },
                        )
                        con.commit()
                    finally:
                        con.close()

                _state_db_write_retry(_write_move, label=f"perform_dedupe.scan_moves:{scan_id}:{loser_id}", attempts=12)
            except Exception as e:
                logging.warning(f"perform_dedupe(): failed to record move in scan_moves: {e}")

        moved_items.append({
            "artist":    artist,
            "title_raw": best_title,
            "size":      size_mb,
            "fmt":       fmt_text,
            "br":        br_kbps,
            "sr":        sr,
            "bd":        bd,
            "thumb_data": None
        })

    # Fetch cover after moves so we do not block the first group on Plex API (fixes stuck 1/N dedupe).
    try:
        _normalize_winner_folder_to_canonical_root(group)
    except Exception:
        logging.debug("Winner canonical placement failed for group %s", group.get("artist"), exc_info=True)

    # Fetch cover after moves so we do not block the first group on Plex API (fixes stuck 1/N dedupe).
    cover_data = fetch_cover_as_base64(group["best"]["album_id"]) if group.get("best", {}).get("album_id") else None
    for m in moved_items:
        m["thumb_data"] = cover_data

    return moved_items


# ────────────────────────── UI card helper ──────────────────────────
