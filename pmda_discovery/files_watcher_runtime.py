"""Filesystem watcher runtime helpers for changed-only scans."""

from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any


def record_files_pending_change_for_runtime(
    runtime: Any,
    folder_path: str,
    reason: str,
    *,
    source_id: int | None = None,
    event_kind: str | None = None,
    event_path: str | None = None,
) -> None:
    folder_key = (folder_path or "").strip()
    if not folder_key:
        return
    now = time.time()
    source_id_int = int(source_id or 0) if source_id is not None else 0
    if source_id_int <= 0:
        source_id_int = int(runtime._source_id_for_path(folder_key) or 0)
    try:
        con = sqlite3.connect(str(runtime.STATE_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO files_pending_changes(folder_path, source_id, event_kind, event_path, reason, first_seen, last_seen, event_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(folder_path) DO UPDATE SET
                source_id = COALESCE(excluded.source_id, files_pending_changes.source_id),
                event_kind = COALESCE(excluded.event_kind, files_pending_changes.event_kind),
                event_path = COALESCE(excluded.event_path, files_pending_changes.event_path),
                reason = excluded.reason,
                last_seen = excluded.last_seen,
                event_count = files_pending_changes.event_count + 1
            """,
            (
                folder_key,
                source_id_int if source_id_int > 0 else None,
                str(event_kind or reason or "").strip()[:64] or None,
                str(event_path or folder_key or "").strip()[:2048] or None,
                (reason or "").strip()[:64],
                now,
                now,
            ),
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to record pending files change for %s", folder_key, exc_info=True)

    with runtime.lock:
        fw = dict(runtime.state.get("files_watcher") or {})
        fw["dirty_count"] = int(fw.get("dirty_count") or 0) + 1
        fw["last_event_at"] = now
        fw["last_event_path"] = folder_key
        root_counts = dict(fw.get("dirty_count_by_root") or {})
        root_key = str(source_id_int if source_id_int > 0 else "unknown")
        root_counts[root_key] = int(root_counts.get(root_key) or 0) + 1
        fw["dirty_count_by_root"] = root_counts
        runtime.state["files_watcher"] = fw


def files_watcher_suppress_folder_for_runtime(
    runtime: Any,
    folder: Path | str,
    *,
    seconds: float = 90.0,
    reason: str = "pmda_write",
) -> None:
    try:
        key = runtime._album_folder_cache_key(folder)
    except Exception:
        key = str(folder or "")
    key = (key or "").strip()
    if not key:
        return
    until = time.time() + max(5.0, float(seconds or 90.0))
    with runtime._FILES_WATCHER_SUPPRESS_LOCK:
        runtime._FILES_WATCHER_SUPPRESS_UNTIL[key] = max(runtime._FILES_WATCHER_SUPPRESS_UNTIL.get(key, 0.0), until)


def files_watcher_is_suppressed_for_runtime(runtime: Any, folder_key: str) -> bool:
    key = (folder_key or "").strip()
    if not key:
        return False
    now = time.time()
    with runtime._FILES_WATCHER_SUPPRESS_LOCK:
        if runtime._FILES_WATCHER_SUPPRESS_UNTIL:
            expired = [k for k, v in runtime._FILES_WATCHER_SUPPRESS_UNTIL.items() if float(v or 0.0) <= now]
            for k in expired[:5000]:
                runtime._FILES_WATCHER_SUPPRESS_UNTIL.pop(k, None)
        until = float(runtime._FILES_WATCHER_SUPPRESS_UNTIL.get(key, 0.0) or 0.0)
    return bool(until and until > now)


def files_watcher_should_ignore_folder_key_for_runtime(runtime: Any, folder_key: str) -> bool:
    p = (folder_key or "").strip()
    if not p:
        return True
    low = p.lower()
    if low.startswith("/config/") or low == "/config":
        return True
    if low.startswith("/dupes/") or low == "/dupes":
        return True
    try:
        target_dir = str(runtime._get_config_from_db("INCOMPLETE_ALBUMS_TARGET_DIR") or "/dupes/incomplete_albums").strip()
        if target_dir:
            target_norm = str(runtime.path_for_fs_access(Path(target_dir))).strip().lower()
            if target_norm and low.startswith(target_norm.lower().rstrip("/") + "/"):
                return True
    except Exception:
        pass
    return False


def list_files_pending_changes_for_runtime(runtime: Any, limit: int = 10000) -> list[dict]:
    out: list[dict] = []
    try:
        con = sqlite3.connect(str(runtime.STATE_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.execute(
            """
            SELECT folder_path, source_id, event_kind, event_path, reason, first_seen, last_seen, event_count
            FROM files_pending_changes
            ORDER BY last_seen DESC
            LIMIT ?
            """,
            (max(1, int(limit or 10000)),),
        )
        for row in cur.fetchall():
            folder_path = (row[0] or "").strip()
            if not folder_path:
                continue
            out.append(
                {
                    "folder_path": folder_path,
                    "source_id": int(row[1] or 0) if row[1] is not None else None,
                    "event_kind": str(row[2] or "").strip(),
                    "event_path": str(row[3] or "").strip(),
                    "reason": (row[4] or "").strip(),
                    "first_seen": float(row[5] or 0),
                    "last_seen": float(row[6] or 0),
                    "event_count": int(row[7] or 0),
                }
            )
        con.close()
    except Exception:
        logging.debug("Failed to list files_pending_changes", exc_info=True)
    return out


def clear_files_pending_changes_for_runtime(runtime: Any, folder_paths: list[str]) -> int:
    cleaned = [str(p or "").strip() for p in (folder_paths or []) if str(p or "").strip()]
    if not cleaned:
        return 0
    removed = 0
    try:
        con = sqlite3.connect(str(runtime.STATE_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.executemany("DELETE FROM files_pending_changes WHERE folder_path = ?", [(p,) for p in cleaned])
        removed = int(cur.rowcount or 0)
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to clear files_pending_changes", exc_info=True)
    return removed


def update_files_watcher_state_for_runtime(
    runtime: Any,
    *,
    running: bool,
    roots: list[str] | None = None,
    reason: str | None = None,
) -> None:
    with runtime.lock:
        fw = dict(runtime.state.get("files_watcher") or {})
        fw["running"] = bool(running)
        if roots is not None:
            fw["roots"] = [str(r) for r in roots]
        if reason is not None:
            fw["reason"] = str(reason).strip()
        runtime.state["files_watcher"] = fw


def update_files_watcher_runtime_for_runtime(runtime: Any, **fields: Any) -> None:
    with runtime.lock:
        fw = dict(runtime.state.get("files_watcher") or {})
        for key, value in fields.items():
            fw[str(key)] = value
        runtime.state["files_watcher"] = fw


def files_watcher_available_for_runtime(runtime: Any) -> bool:
    return bool(runtime.Observer is not None and runtime.FileSystemEventHandler is not None)


def files_watcher_status_snapshot_for_runtime(runtime: Any) -> dict[str, Any]:
    with runtime.lock:
        fw = dict(runtime.state.get("files_watcher") or {})
    enabled = bool(runtime.PMDA_FILES_WATCHER_ENABLED)
    available = bool(files_watcher_available_for_runtime(runtime))
    running = bool(fw.get("running"))
    degraded = bool(enabled and ((not available) or (not running)))
    return {
        "running": running,
        "enabled": enabled,
        "available": available,
        "degraded_mode": degraded,
        "reason": str(fw.get("reason") or ""),
        "dirty_count": int(fw.get("dirty_count") or 0),
        "dirty_count_by_root": dict(fw.get("dirty_count_by_root") or {}),
        "last_event_at": fw.get("last_event_at"),
        "last_event_path": fw.get("last_event_path"),
        "restart_in_progress": bool(fw.get("restart_in_progress")),
        "last_restart_started_at": fw.get("last_restart_started_at"),
        "last_restart_ended_at": fw.get("last_restart_ended_at"),
        "last_restart_duration_ms": runtime._int_or_none(fw.get("last_restart_duration_ms")),
        "consecutive_failures": int(fw.get("consecutive_failures") or 0),
        "last_error": str(fw.get("last_error") or ""),
        "roots": list(fw.get("roots") or []),
    }


def _make_files_watcher_handler(runtime: Any):
    if runtime.FileSystemEventHandler is None:
        return None

    class FilesWatcherHandler(runtime.FileSystemEventHandler):
        def __init__(self):
            super().__init__()
            self._last_log_ts = 0.0

        def _handle(self, path: str, reason: str) -> None:
            try:
                with runtime.lock:
                    if bool(runtime.state.get("scanning")) or bool(runtime.state.get("scan_finalizing")):
                        return
            except Exception:
                pass
            folders = runtime._resolve_album_folders_from_event_path(path)
            if not folders:
                return
            kept = 0
            for folder_key in folders:
                if files_watcher_should_ignore_folder_key_for_runtime(runtime, folder_key):
                    continue
                if files_watcher_is_suppressed_for_runtime(runtime, folder_key):
                    continue
                record_files_pending_change_for_runtime(
                    runtime,
                    folder_key,
                    reason,
                    source_id=runtime._source_id_for_path(folder_key),
                    event_kind=reason,
                    event_path=path,
                )
                kept += 1
            now = time.time()
            if (now - self._last_log_ts) >= max(1.0, runtime.PMDA_FILES_WATCHER_LOG_COOLDOWN_SEC):
                self._last_log_ts = now
                if kept:
                    logging.info("[FILES watcher] queued %d changed album folder(s) (%s)", kept, reason)

        def on_created(self, event):
            self._handle(getattr(event, "src_path", ""), "created")

        def on_modified(self, event):
            self._handle(getattr(event, "src_path", ""), "modified")

        def on_moved(self, event):
            self._handle(getattr(event, "src_path", ""), "moved")
            self._handle(getattr(event, "dest_path", ""), "moved")

        def on_deleted(self, event):
            self._handle(getattr(event, "src_path", ""), "deleted")

    return FilesWatcherHandler


def stop_files_watcher_for_runtime(runtime: Any) -> None:
    with runtime._files_watcher_lock:
        obs = runtime._files_watcher_observer
        runtime._files_watcher_observer = None
    if obs is None:
        update_files_watcher_state_for_runtime(runtime, running=False, reason="stopped")
        return
    try:
        obs.stop()
        obs.join(timeout=3)
    except Exception:
        logging.debug("Failed to stop files watcher cleanly", exc_info=True)
    update_files_watcher_state_for_runtime(runtime, running=False, reason="stopped")


def restart_files_watcher_if_needed_for_runtime(runtime: Any) -> bool:
    active_roots = runtime._effective_files_roots(enabled_only=True)
    if not runtime.PMDA_FILES_WATCHER_ENABLED:
        stop_files_watcher_for_runtime(runtime)
        update_files_watcher_state_for_runtime(runtime, running=False, roots=list(active_roots), reason="disabled_by_setting")
        return False
    if runtime._get_library_mode() != "files":
        stop_files_watcher_for_runtime(runtime)
        update_files_watcher_state_for_runtime(runtime, running=False, roots=list(active_roots), reason="disabled_non_files_mode")
        return False
    if not active_roots:
        stop_files_watcher_for_runtime(runtime)
        update_files_watcher_state_for_runtime(runtime, running=False, roots=[], reason="disabled_no_roots")
        return False
    if not files_watcher_available_for_runtime(runtime):
        logging.info("FILES watcher unavailable (watchdog not installed); changed-only uses discovery fallback.")
        update_files_watcher_state_for_runtime(runtime, running=False, roots=list(active_roots), reason="watchdog_unavailable")
        return False

    stop_files_watcher_for_runtime(runtime)
    obs = runtime.Observer()
    handler_cls = _make_files_watcher_handler(runtime)
    if handler_cls is None:
        update_files_watcher_state_for_runtime(runtime, running=False, roots=list(active_roots), reason="watchdog_unavailable")
        return False
    handler = handler_cls()
    valid_roots: list[str] = []
    for root in active_roots:
        if not root:
            continue
        p = Path(root)
        if not p.exists() or not p.is_dir():
            continue
        try:
            obs.schedule(handler, str(p), recursive=True)
            valid_roots.append(str(p))
        except Exception:
            logging.debug("Failed to watch root %s", p, exc_info=True)
    if not valid_roots:
        update_files_watcher_state_for_runtime(runtime, running=False, roots=list(active_roots), reason="no_valid_roots")
        return False
    try:
        obs.start()
    except Exception:
        logging.warning("Unable to start files watcher; falling back to discovery scan.")
        update_files_watcher_state_for_runtime(runtime, running=False, roots=list(active_roots), reason="observer_start_failed")
        return False

    with runtime._files_watcher_lock:
        runtime._files_watcher_observer = obs
    update_files_watcher_state_for_runtime(runtime, running=True, roots=valid_roots, reason="running")
    logging.info("FILES watcher started on %d root(s): %s", len(valid_roots), ", ".join(valid_roots))
    runtime._start_auto_changed_only_scan_scheduler()
    return True


def files_watcher_reconcile_attempt_for_runtime(runtime: Any, reason: str) -> bool:
    started = time.time()
    with runtime._files_watcher_restart_lock:
        update_files_watcher_runtime_for_runtime(
            runtime,
            restart_in_progress=True,
            last_restart_started_at=started,
        )
        ok = False
        err_msg = ""
        try:
            ok = bool(restart_files_watcher_if_needed_for_runtime(runtime))
        except Exception as e:
            err_msg = str(e).strip()
            logging.exception("FILES watcher reconcile failed (%s)", reason)
            ok = False
        ended = time.time()
        snap = files_watcher_status_snapshot_for_runtime(runtime)
        running_now = bool(snap.get("running"))
        reason_now = str(snap.get("reason") or "").strip().lower()
        benign_reasons = {
            "disabled_by_setting",
            "disabled_non_files_mode",
            "disabled_no_roots",
            "watchdog_unavailable",
        }
        success = bool(ok or running_now or reason_now in benign_reasons)
        failures_now = int(snap.get("consecutive_failures") or 0)
        if success:
            update_files_watcher_runtime_for_runtime(
                runtime,
                restart_in_progress=False,
                last_restart_ended_at=ended,
                last_restart_duration_ms=int(max(0.0, (ended - started) * 1000.0)),
                consecutive_failures=0,
                last_error="",
            )
            return True
        next_failures = max(0, failures_now) + 1
        update_files_watcher_runtime_for_runtime(
            runtime,
            restart_in_progress=False,
            last_restart_ended_at=ended,
            last_restart_duration_ms=int(max(0.0, (ended - started) * 1000.0)),
            consecutive_failures=next_failures,
            last_error=(err_msg or reason_now or "watcher_restart_failed")[:300],
        )
        return False


def files_watcher_manager_loop_for_runtime(runtime: Any) -> None:
    logging.info("FILES watcher manager loop started.")
    while not runtime._files_watcher_manager_stop_event.is_set():
        runtime._files_watcher_manager_event.wait(timeout=1.0)
        runtime._files_watcher_manager_event.clear()
        if runtime._files_watcher_manager_stop_event.is_set():
            break
        while not runtime._files_watcher_manager_stop_event.is_set():
            with runtime._files_watcher_manager_lock:
                now = time.time()
                due_retry = bool(runtime._files_watcher_retry_pending and now >= float(runtime._files_watcher_next_retry_at or 0.0))
                requested = bool(runtime._files_watcher_reconcile_requested)
                reason = str(runtime._files_watcher_reconcile_reason or "reconcile").strip() or "reconcile"
                if not requested and not due_retry:
                    break
                runtime._files_watcher_reconcile_requested = False
            ok = files_watcher_reconcile_attempt_for_runtime(runtime, reason)
            if ok:
                with runtime._files_watcher_manager_lock:
                    runtime._files_watcher_retry_pending = False
                    runtime._files_watcher_next_retry_at = 0.0
                    runtime._files_watcher_backoff_step = 0
                continue
            with runtime._files_watcher_manager_lock:
                step = min(int(runtime._files_watcher_backoff_step or 0), len(runtime._FILES_WATCHER_RESTART_BACKOFF_SEC) - 1)
                wait_sec = float(runtime._FILES_WATCHER_RESTART_BACKOFF_SEC[step])
                runtime._files_watcher_backoff_step = min(step + 1, len(runtime._FILES_WATCHER_RESTART_BACKOFF_SEC) - 1)
                runtime._files_watcher_retry_pending = True
                runtime._files_watcher_next_retry_at = time.time() + wait_sec
            logging.warning("FILES watcher manager retry scheduled in %.0fs (%s).", wait_sec, reason)
            break
    logging.info("FILES watcher manager loop stopped.")


def start_files_watcher_manager_if_needed_for_runtime(runtime: Any) -> None:
    with runtime._files_watcher_manager_lock:
        thread = runtime._files_watcher_manager_thread
        if thread is not None and thread.is_alive():
            return
        runtime._files_watcher_manager_stop_event.clear()
        t = runtime.threading.Thread(
            target=lambda: files_watcher_manager_loop_for_runtime(runtime),
            daemon=True,
            name="files-watcher-manager",
        )
        runtime._files_watcher_manager_thread = t
        t.start()


def stop_files_watcher_manager_for_runtime(runtime: Any) -> None:
    runtime._files_watcher_manager_stop_event.set()
    runtime._files_watcher_manager_event.set()
    with runtime._files_watcher_manager_lock:
        thread = runtime._files_watcher_manager_thread
    if thread is not None and thread.is_alive():
        thread.join(timeout=5.0)


def request_files_watcher_reconcile_for_runtime(runtime: Any, reason: str, *, force: bool = False) -> None:
    start_files_watcher_manager_if_needed_for_runtime(runtime)
    with runtime._files_watcher_manager_lock:
        runtime._files_watcher_reconcile_requested = True
        runtime._files_watcher_reconcile_reason = str(reason or "reconcile").strip() or "reconcile"
        if force:
            runtime._files_watcher_retry_pending = False
            runtime._files_watcher_next_retry_at = 0.0
            runtime._files_watcher_backoff_step = 0
    runtime._files_watcher_manager_event.set()
