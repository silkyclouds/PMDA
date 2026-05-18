"""Runtime-owned incomplete album quarantine moves."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "auto_move_incomplete_albums_for_scan_for_runtime",
            "_auto_move_incomplete_albums_for_scan",
        }:
            continue
        globals()[name] = value


def auto_move_incomplete_albums_for_scan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> dict:
    _bind_runtime(runtime)
    return _auto_move_incomplete_albums_for_scan(*args, **kwargs)


def _auto_move_incomplete_albums_for_scan(
    scan_id: int | None,
    editions_by_artist: dict[str, list[dict]] | None,
) -> dict:
    """
    Move broken/incomplete album folders to INCOMPLETE_ALBUMS_TARGET_DIR.
    Returns {'moved': int, 'size_mb': int, 'errors': int}.
    """
    if library_is_audit_mode():
        return {"moved": 0, "size_mb": 0, "errors": 0}
    result = {"moved": 0, "size_mb": 0, "errors": 0}
    if not scan_id or not editions_by_artist:
        return result

    target_dir = str(_get_config_from_db("INCOMPLETE_ALBUMS_TARGET_DIR") or "/dupes/incomplete_albums").strip()
    target_root = path_for_fs_access(Path(target_dir))
    try:
        target_root.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logging.warning("Auto-move incomplete: cannot create target dir %s: %s", target_root, e)
        result["errors"] += 1
        return result

    candidates: dict[str, dict] = {}
    for artist_name, editions in (editions_by_artist or {}).items():
        for e in (editions or []):
            assessment = dict(e.get("_incomplete_assessment") or {})
            if not bool((assessment or {}).get("quarantine_eligible")):
                continue
            folder = e.get("folder")
            if not folder:
                continue
            src_folder = path_for_fs_access(Path(str(folder)))
            key = str(src_folder)
            if key in candidates:
                continue
            missing_required_raw = e.get("missing_required_tags")
            if isinstance(missing_required_raw, list):
                missing_required_tags = missing_required_raw
            elif isinstance(missing_required_raw, tuple):
                missing_required_tags = list(missing_required_raw)
            else:
                missing_required_tags = []
            candidates[key] = {
                "artist": str(artist_name or ""),
                "album_id": int(e.get("album_id") or 0),
                "title_raw": str(e.get("title_raw") or ""),
                "src": src_folder,
                "fmt_text": str(e.get("fmt_text") or get_primary_format(src_folder) or ""),
                "expected_track_count": int(e.get("expected_track_count") or 0),
                "actual_track_count": int(e.get("actual_track_count") or len(e.get("tracks", [])) or 0),
                "missing_indices": list(e.get("missing_indices") or []),
                "missing_required_tags": missing_required_tags,
                "strict_match_provider": _normalize_identity_provider(str(e.get("strict_match_provider") or "")),
                "strict_reject_reason": str(e.get("strict_reject_reason") or ""),
                "strict_tracklist_score": float(e.get("strict_tracklist_score") or 0.0),
                "classification": str((assessment or {}).get("verdict") or ""),
                "classification_confidence": float((assessment or {}).get("confidence") or 0.0),
                "classification_source": str((assessment or {}).get("source") or "deterministic"),
                "evidence_json": dict(assessment or {}),
            }

    if not candidates:
        return result

    with lock:
        state["scan_incomplete_move_running"] = True
        state["scan_incomplete_move_done"] = 0
        state["scan_incomplete_move_total"] = int(len(candidates))
        state["scan_incomplete_move_current_album"] = None

    try:
        for item in candidates.values():
            src_folder = item["src"]
            if not src_folder.exists():
                with lock:
                    state["scan_incomplete_move_done"] = int(state.get("scan_incomplete_move_done") or 0) + 1
                continue
            # Keep quarantine tree aligned with library layout: letter/artist/album.
            letter, artist_dir, album_dir = _quarantine_artist_album_parts(
                src_folder,
                artist_hint=str(item.get("artist") or ""),
                album_hint=str(item.get("title_raw") or src_folder.name or ""),
            )
            with lock:
                state["scan_incomplete_move_current_album"] = {
                    "artist": str(item.get("artist") or ""),
                    "album": str(item.get("title_raw") or src_folder.name or ""),
                    "path": str(src_folder),
                }
            dst = target_root / letter / artist_dir / album_dir
            counter = 1
            while dst.exists():
                dst = target_root / letter / artist_dir / f"{album_dir} ({counter})"
                counter += 1
            try:
                dst.parent.mkdir(parents=True, exist_ok=True)
                size_mb = int(folder_size(src_folder) // (1024 * 1024))
                try:
                    _files_watcher_suppress_folder(src_folder, seconds=180.0, reason="pmda_move_incomplete")
                except Exception:
                    pass
                safe_move(str(src_folder), str(dst))
                try:
                    _files_watcher_suppress_folder(dst, seconds=180.0, reason="pmda_move_incomplete")
                except Exception:
                    pass
                _files_forget_album_folder_global(src_folder)
                result["moved"] += 1
                result["size_mb"] += size_mb
                moved_at = time.time()
                decision_reason = (
                    str(item.get("strict_reject_reason") or "").strip()
                    or "broken_album_missing_tracks"
                )

                def _write_move() -> None:
                    con = _state_connect()
                    try:
                        cur = con.cursor()
                        _insert_scan_move_row(
                            cur,
                            scan_id=int(scan_id),
                            artist=str(item.get("artist") or ""),
                            album_id=int(item.get("album_id") or 0),
                            original_path=str(src_folder),
                            moved_to_path=str(dst),
                            size_mb=int(size_mb or 0),
                            moved_at=moved_at,
                            album_title=str(item.get("title_raw") or ""),
                            fmt_text=str(item.get("fmt_text") or ""),
                            move_reason="incomplete",
                            decision_source="pipeline_incomplete_move",
                            decision_provider=str(item.get("strict_match_provider") or ""),
                            decision_reason=decision_reason,
                            decision_confidence=float(item.get("strict_tracklist_score") or 0.0),
                            materialization_strategy="move",
                            arbitration_result="incomplete_quarantine",
                            details={
                                "kind": "incomplete",
                                "classification": "broken_album",
                                "expected_track_count": int(item.get("expected_track_count") or 0),
                                "actual_track_count": int(item.get("actual_track_count") or 0),
                                "missing_indices": list(item.get("missing_indices") or []),
                                "missing_required_tags": list(item.get("missing_required_tags") or []),
                                "strict_match_provider": str(item.get("strict_match_provider") or ""),
                                "strict_reject_reason": str(item.get("strict_reject_reason") or ""),
                                "strict_tracklist_score": float(item.get("strict_tracklist_score") or 0.0),
                                "classification": str(item.get("classification") or ""),
                                "classification_confidence": float(item.get("classification_confidence") or 0.0),
                                "classification_source": str(item.get("classification_source") or "deterministic"),
                                "evidence": dict(item.get("evidence_json") or {}),
                            },
                        )
                        con.commit()
                    finally:
                        con.close()

                _state_db_write_retry(
                    _write_move,
                    label=f"move_incomplete.scan_moves:{scan_id}:{item.get('album_id')}",
                    attempts=12,
                )
                logging.info(
                    "[INCOMPLETE] [X❌] quarantined incomplete album: %s -> %s",
                    src_folder,
                    dst,
                )
            except Exception as move_err:
                result["errors"] += 1
                logging.warning(
                    "[INCOMPLETE] [X❌] failed to quarantine incomplete album: %s -> %s: %s",
                    src_folder,
                    dst,
                    move_err,
                )
            finally:
                with lock:
                    state["scan_incomplete_move_done"] = int(state.get("scan_incomplete_move_done") or 0) + 1
    finally:
        with lock:
            state["scan_incomplete_move_running"] = False
            state["scan_incomplete_move_current_album"] = None
    return result
