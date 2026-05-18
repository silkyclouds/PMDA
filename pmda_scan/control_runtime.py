"""Runtime-owned scan start/control helpers."""

from __future__ import annotations

import logging
import sys
import time
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "requires_config_for_runtime",
            "active_scan_info_locked_for_runtime",
            "try_begin_scan_for_runtime",
            "start_background_scan_for_runtime",
            "run_preflight_checks_for_runtime",
            "_requires_config",
            "_active_scan_info_locked",
            "_try_begin_scan",
            "start_background_scan",
            "_run_preflight_checks",
        }:
            continue
        globals()[name] = value


def requires_config_for_runtime(runtime: Any):
    _bind_runtime(runtime)
    return _requires_config()


def active_scan_info_locked_for_runtime(runtime: Any) -> dict[str, Any]:
    _bind_runtime(runtime)
    return _active_scan_info_locked()


def try_begin_scan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> tuple[bool, dict[str, Any]]:
    _bind_runtime(runtime)
    return _try_begin_scan(*args, **kwargs)


def start_background_scan_for_runtime(runtime: Any) -> tuple[bool, dict[str, Any]]:
    _bind_runtime(runtime)
    return start_background_scan()


def run_preflight_checks_for_runtime(runtime: Any):
    _bind_runtime(runtime)
    return _run_preflight_checks()


def _requires_config():
    """Return 503 response when required backend config is missing for current mode."""
    _reload_library_mode_and_files_roots_from_db()
    if not _effective_files_roots(enabled_only=True):
        payload = {"error": "No source folders configured. Add your music folders in Settings.", "requiresConfig": True}
        if has_request_context():
            return jsonify(payload), 503
        return payload
    return None


def _active_scan_info_locked() -> dict[str, Any]:
    active_scan_type = str(state.get("scan_type") or "full").strip().lower() or "full"
    started_at = state.get("scan_start_time") or state.get("scan_start_requested_at")
    return {
        "active_scan_type": active_scan_type,
        "started_at": float(started_at) if started_at is not None else None,
    }


def _try_begin_scan(
    *,
    scan_type: str,
    source: str,
    run_improve_after: bool,
    scheduler_run_id: str | None,
) -> tuple[bool, dict[str, Any]]:
    scan_type_norm = str(scan_type or "full").strip().lower()
    if scan_type_norm not in {"full", "changed_only"}:
        scan_type_norm = "full"
    source_norm = str(source or "interactive").strip().lower() or "interactive"
    requested_at = time.time()
    scan_should_stop.clear()
    scan_is_paused.clear()
    with lock:
        if bool(state.get("scanning")) or bool(state.get("scan_finalizing")) or bool(state.get("scan_starting")):
            active = _active_scan_info_locked()
            return False, {
                "status": "blocked",
                "reason": "scan_already_running",
                "active_scan_type": active.get("active_scan_type"),
                "started_at": active.get("started_at"),
            }
        state["scan_starting"] = True
        state["scan_start_requested_at"] = requested_at
        state["run_improve_after"] = bool(run_improve_after)
        state["scan_type"] = scan_type_norm
        state["scan_auto_trigger"] = source_norm
        state["scan_scheduler_run_id"] = str(scheduler_run_id or "").strip() or None
        state["scan_ai_enabled"] = bool(ai_provider_ready)
        state["scan_mb_enabled"] = USE_MUSICBRAINZ
        state["scan_progress"] = 0
        state["scan_total"] = 0
        state["scan_step_progress"] = 0
        state["scan_step_total"] = 0
    try:
        logging.debug("start_scan(): launching background_scan() thread")
        start_scan_thread(background_scan, scan_type=scan_type_norm)
    except Exception as e:
        with lock:
            state["scan_starting"] = False
            state["scan_start_requested_at"] = None
        return False, {
            "status": "blocked",
            "reason": "scan_start_failed",
            "message": str(e) or "Unable to start scan thread",
        }
    if _get_library_mode() == "files":
        _request_files_watcher_reconcile(f"scan_start_{scan_type_norm}")
    return True, {
        "status": "started",
        "scan_type": scan_type_norm,
        "run_improve_after": bool(run_improve_after),
    }


def start_background_scan() -> tuple[bool, dict[str, Any]]:
    with lock:
        scan_type = str(state.get("scan_type") or "full")
        source = str(state.get("scan_auto_trigger") or "interactive")
        run_improve_after = bool(state.get("run_improve_after", False))
        run_id = str(state.get("scan_scheduler_run_id") or "").strip() or None
    return _try_begin_scan(
        scan_type=scan_type,
        source=source,
        run_improve_after=run_improve_after,
        scheduler_run_id=run_id,
    )

def _run_preflight_checks():
    """Run MusicBrainz and AI connectivity checks. Returns (mb_ok: bool, ai_ok: bool).
    For AI, ai_ok reflects ai_provider_ready (functional check is done by _reload_ai_config_and_reinit)."""
    mb_ok = False
    if USE_MUSICBRAINZ:
        try:
            test_mbid = "9162580e-5df4-32de-80cc-f45a8d8a9b1d"
            musicbrainzngs.get_release_group_by_id(test_mbid, includes=[])
            mb_ok = True
        except Exception:
            pass
    ai_ok = bool(ai_provider_ready)
    return mb_ok, ai_ok
