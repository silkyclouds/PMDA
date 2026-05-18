"""Checkpointable filesystem audio walker used by Files discovery."""

from __future__ import annotations

import copy
import os
import threading
import time
from pathlib import Path
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "iter_audio_files_under_roots_checkpointed_for_runtime",
            "_iter_audio_files_under_roots_checkpointed",
        }:
            continue
        globals()[name] = value


def iter_audio_files_under_roots_checkpointed_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> tuple[list[Path], set[str]]:
    _bind_runtime(runtime)
    return _iter_audio_files_under_roots_checkpointed(*args, **kwargs)


def _iter_audio_files_under_roots_checkpointed(
    roots: list[str],
    *,
    run_id: str | None,
    progress_cb=None,
    progress_every: int = 250,
    heartbeat_seconds: float = 10.0,
    stop_event: threading.Event | None = None,
    pause_event: threading.Event | None = None,
    resume_snapshot: dict[str, Any] | None = None,
    checkpoint_cb=None,
    dir_skip_lookup=None,
    dir_skip_resolver=None,
) -> tuple[list[Path], set[str]]:
    roots_list = [str(r) for r in (roots or []) if r]
    roots_total = len(roots_list)
    if roots_total <= 0:
        return []

    progress_every = max(1, int(progress_every)) if progress_every else 0
    heartbeat_seconds = float(heartbeat_seconds or 0.0)
    progress_enabled = callable(progress_cb)
    checkpoint_enabled = callable(checkpoint_cb)

    results_by_idx: dict[int, list[Path]] = {}
    cached_album_folders: set[str] = set(str(p or "").strip() for p in ((resume_snapshot or {}).get("cached_album_folders") or []) if str(p or "").strip())
    shared_entries_scanned = 0
    shared_files_found = 0
    shared_roots_done = 0
    resume_stage = str((resume_snapshot or {}).get("stage") or "").strip().lower()
    current_resume_root_index = _parse_int_loose((resume_snapshot or {}).get("current_root_index"), None)
    current_resume_stack = copy.deepcopy((resume_snapshot or {}).get("current_stack") or [])
    current_resume_entries = int((resume_snapshot or {}).get("current_root_entries_scanned") or 0)
    current_resume_audio_found = int((resume_snapshot or {}).get("current_root_audio_found") or 0)
    current_resume_files = [Path(str(p)) for p in ((resume_snapshot or {}).get("current_root_files") or []) if p]

    if isinstance(resume_snapshot, dict):
        shared_entries_scanned = int(resume_snapshot.get("shared_entries_scanned") or resume_snapshot.get("entries_scanned") or 0)
        shared_files_found = int(resume_snapshot.get("shared_files_found") or resume_snapshot.get("files_found") or 0)
        shared_roots_done = int(resume_snapshot.get("shared_roots_done") or 0)
        for key, paths in ((resume_snapshot.get("results_by_root") or {}).items() if isinstance(resume_snapshot.get("results_by_root"), dict) else []):
            idx = _parse_int_loose(key, None)
            if idx is None:
                continue
            results_by_idx[int(idx)] = [Path(str(p)) for p in (paths or []) if p]
        if current_resume_root_index is not None and current_resume_files:
            results_by_idx[int(current_resume_root_index)] = list(current_resume_files)

    def _normalize_stack_frames(frames: list[Any]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for frame in frames or []:
            if isinstance(frame, dict):
                dir_path = str(frame.get("dir") or "").strip()
                pending = []
                for item in (frame.get("pending") or []):
                    if not isinstance(item, dict):
                        continue
                    pending.append(
                        {
                            "path": str(item.get("path") or "").strip(),
                            "is_dir": bool(item.get("is_dir")),
                            "is_audio": bool(item.get("is_audio")),
                        }
                    )
                if dir_path:
                    out.append({"dir": dir_path, "pending": pending})
            else:
                dir_path = str(frame or "").strip()
                if dir_path:
                    out.append({"dir": dir_path, "pending": None})
        return out

    def _publish_checkpoint(
        *,
        root_index: int | None,
        root_path: str | None,
        stack_frames: list[dict[str, Any]] | None,
        root_entries_scanned: int,
        root_audio_found: int,
        current_root_files: list[Path] | None,
        stage: str,
        paused_ack: bool = False,
    ) -> None:
        if not checkpoint_enabled:
            return
        try:
            checkpoint_cb(
                {
                    "run_id": str(run_id or "").strip(),
                    "stage": str(stage or "filesystem"),
                    "roots": list(roots_list),
                    "roots_total": roots_total,
                    "results_by_root": results_by_idx,
                    "current_root_index": root_index,
                    "current_root_path": str(root_path or "").strip() or None,
                    "current_stack": _normalize_stack_frames(stack_frames or []),
                    "current_root_entries_scanned": int(root_entries_scanned or 0),
                    "current_root_audio_found": int(root_audio_found or 0),
                    "current_root_files": list(current_root_files or []),
                    "shared_entries_scanned": int(shared_entries_scanned or 0),
                    "shared_files_found": int(shared_files_found or 0),
                    "shared_roots_done": int(shared_roots_done or 0),
                    "cached_album_folders": sorted(cached_album_folders),
                    "paused_ack": bool(paused_ack),
                    "updated_at": time.time(),
                }
            )
        except Exception:
            pass

    def _emit_progress(
        *,
        root: str,
        root_entries_scanned: int,
        delta_entries: int = 0,
        delta_files: int = 0,
        done_root: bool = False,
    ) -> None:
        nonlocal shared_entries_scanned, shared_files_found, shared_roots_done
        if delta_entries:
            shared_entries_scanned += max(0, int(delta_entries))
        if delta_files:
            shared_files_found += max(0, int(delta_files))
        if done_root:
            shared_roots_done += 1
        if not progress_enabled:
            return
        payload = {
            "root": root,
            "roots_done": shared_roots_done,
            "roots_total": roots_total,
            "files_found": shared_files_found,
            "entries_scanned": shared_entries_scanned,
            "root_entries_scanned": max(0, int(root_entries_scanned)),
            "done": shared_roots_done >= roots_total,
        }
        try:
            progress_cb(payload)
        except Exception:
            pass

    start_index = max(0, min(shared_roots_done, roots_total))
    if resume_stage == "filesystem" and current_resume_root_index is not None:
        start_index = max(0, min(int(current_resume_root_index), roots_total - 1))

    for idx in range(start_index, roots_total):
        root_path = roots_list[idx]
        base = Path(root_path)
        root_entries = 0
        root_audio_found = 0
        pending_entries = 0
        pending_files = 0
        last_heartbeat = time.monotonic()
        local_out = list(results_by_idx.get(idx) or [])
        stack_frames: list[dict[str, Any]] = [{"dir": str(base), "pending": None}]
        if resume_stage == "filesystem" and current_resume_root_index == idx:
            stack_frames = _normalize_stack_frames(current_resume_stack) or [{"dir": str(base), "pending": None}]
            root_entries = int(current_resume_entries or 0)
            root_audio_found = int(current_resume_audio_found or len(local_out))
            if not local_out:
                local_out = list(current_resume_files or [])

        def _load_dir_entries(dir_path: str) -> list[dict[str, Any]]:
            items: list[dict[str, Any]] = []
            try:
                with os.scandir(dir_path) as it:
                    for entry in it:
                        try:
                            is_dir = bool(entry.is_dir(follow_symlinks=False))
                            is_audio = bool(not is_dir and entry.is_file(follow_symlinks=False) and AUDIO_RE.search(entry.name))
                            items.append(
                                {
                                    "path": str(entry.path),
                                    "is_dir": is_dir,
                                    "is_audio": is_audio,
                                }
                            )
                        except (OSError, PermissionError):
                            continue
            except (FileNotFoundError, NotADirectoryError, PermissionError, OSError):
                return []
            return items

        def _flush(*, done_root: bool = False, force: bool = False, paused_ack: bool = False) -> None:
            nonlocal pending_entries, pending_files, last_heartbeat
            if force or done_root or pending_entries > 0 or pending_files > 0:
                _emit_progress(
                    root=str(base),
                    root_entries_scanned=root_entries,
                    delta_entries=pending_entries,
                    delta_files=pending_files,
                    done_root=done_root,
                )
                pending_entries = 0
                pending_files = 0
                last_heartbeat = time.monotonic()
            _publish_checkpoint(
                root_index=idx,
                root_path=str(base),
                stack_frames=stack_frames,
                root_entries_scanned=root_entries,
                root_audio_found=root_audio_found,
                current_root_files=local_out,
                stage="filesystem",
                paused_ack=paused_ack,
            )

        if not base.exists():
            _flush(done_root=True, force=True)
            results_by_idx[idx] = local_out
            current_resume_root_index = None
            current_resume_stack = []
            continue

        interrupted = False
        while stack_frames:
            if stop_event is not None and stop_event.is_set():
                interrupted = True
                break
            if pause_event is not None and pause_event.is_set():
                _flush(force=True, paused_ack=True)
                while pause_event.is_set() and not (stop_event is not None and stop_event.is_set()):
                    time.sleep(0.2)
                if stop_event is not None and stop_event.is_set():
                    interrupted = True
                    break
            frame = stack_frames.pop()
            dir_path = str(frame.get("dir") or "").strip()
            pending = frame.get("pending")
            if pending is None:
                pending = _load_dir_entries(dir_path)
            if not pending:
                continue
            item = dict(pending.pop() or {})
            if pending:
                stack_frames.append({"dir": dir_path, "pending": pending})
            root_entries += 1
            pending_entries += 1
            if bool(item.get("is_dir")):
                child_dir = str(item.get("path") or "").strip()
                skipped_payload = None
                if child_dir and callable(dir_skip_lookup) and callable(dir_skip_resolver):
                    try:
                        skip_meta = dir_skip_lookup(child_dir, str(base))
                    except Exception:
                        skip_meta = None
                    if isinstance(skip_meta, dict) and skip_meta:
                        try:
                            skipped_payload = dir_skip_resolver(child_dir, skip_meta)
                        except Exception:
                            skipped_payload = None
                if isinstance(skipped_payload, dict) and skipped_payload.get("file_paths"):
                    file_paths = [Path(str(p)) for p in (skipped_payload.get("file_paths") or []) if str(p or "").strip()]
                    local_out.extend(file_paths)
                    added_files = int(skipped_payload.get("audio_count") or len(file_paths))
                    added_entries = int(skipped_payload.get("entry_estimate") or 0)
                    root_audio_found += max(0, added_files)
                    pending_files += max(0, added_files)
                    if added_entries > 0:
                        root_entries += added_entries
                        pending_entries += added_entries
                    for folder_key in (skipped_payload.get("album_folders") or []):
                        folder_clean = str(folder_key or "").strip()
                        if folder_clean:
                            cached_album_folders.add(folder_clean)
                    if progress_every > 0 and (root_audio_found % progress_every == 0):
                        _flush()
                else:
                    stack_frames.append({"dir": child_dir, "pending": None})
            elif bool(item.get("is_audio")):
                local_out.append(Path(str(item.get("path") or "")))
                root_audio_found += 1
                pending_files += 1
                if progress_every > 0 and (root_audio_found % progress_every == 0):
                    _flush()
            if heartbeat_seconds > 0:
                now = time.monotonic()
                if (now - last_heartbeat) >= heartbeat_seconds:
                    _flush()

        results_by_idx[idx] = local_out
        if interrupted:
            _flush(force=True, paused_ack=bool(pause_event is not None and pause_event.is_set()))
            break
        _flush(done_root=True, force=True)
        current_resume_root_index = None
        current_resume_stack = []
        current_resume_entries = 0
        current_resume_audio_found = 0

    merged: list[Path] = []
    seen_paths: set[str] = set()
    for idx in range(roots_total):
        for path in results_by_idx.get(idx, []):
            sp = str(path)
            if sp in seen_paths:
                continue
            seen_paths.add(sp)
            merged.append(path)
    return merged, cached_album_folders
