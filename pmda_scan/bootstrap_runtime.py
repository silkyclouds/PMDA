"""Pipeline bootstrap state helpers extracted from the PMDA bootstrap module."""

from __future__ import annotations

import sys
from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind live PMDA globals for bootstrap-state operations."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "_pipeline_bootstrap_status_for_runtime",
        "_has_completed_full_scan_for_runtime",
        "_pipeline_bootstrap_refresh_from_history_for_runtime",
        "_pipeline_bootstrap_mark_full_completed_for_runtime",
        "_pipeline_bootstrap_reset_for_runtime",
        "_auto_changed_only_mode_effective_for_runtime",
        "_scan_autonomous_mode_effective_for_runtime",
        "get_default_scan_type_for_runtime",
        "_bind_runtime",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})

def _pipeline_bootstrap_status_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_pipeline_bootstrap_status`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _pipeline_bootstrap_status_impl(*args, **kwargs)

def _has_completed_full_scan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_has_completed_full_scan`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _has_completed_full_scan_impl(*args, **kwargs)

def _pipeline_bootstrap_refresh_from_history_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_pipeline_bootstrap_refresh_from_history`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _pipeline_bootstrap_refresh_from_history_impl(*args, **kwargs)

def _pipeline_bootstrap_mark_full_completed_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_pipeline_bootstrap_mark_full_completed`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _pipeline_bootstrap_mark_full_completed_impl(*args, **kwargs)

def _pipeline_bootstrap_reset_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_pipeline_bootstrap_reset`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _pipeline_bootstrap_reset_impl(*args, **kwargs)

def _auto_changed_only_mode_effective_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_auto_changed_only_mode_effective`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _auto_changed_only_mode_effective_impl(*args, **kwargs)

def _scan_autonomous_mode_effective_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_scan_autonomous_mode_effective`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _scan_autonomous_mode_effective_impl(*args, **kwargs)

def get_default_scan_type_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``get_default_scan_type`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return get_default_scan_type_impl(*args, **kwargs)

def _pipeline_bootstrap_status_impl(
    *,
    timeout: float = 15.0,
    prefer_cached_on_failure: bool = True,
) -> dict[str, Any]:
    mod = sys.modules[__name__]
    cache_key = "_PIPELINE_BOOTSTRAP_STATUS_CACHE"

    def _sqlite_ts_to_float(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            try:
                return float(value)
            except Exception:
                return None
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            return float(raw)
        except Exception:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                return datetime.strptime(raw, fmt).timestamp()
            except Exception:
                continue
        return None

    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=max(0.01, float(timeout)))
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute(
            """
            SELECT bootstrap_required, autonomous_mode, first_full_scan_id, first_full_completed_at, updated_at
            FROM pipeline_bootstrap_state
            WHERE id = 1
            LIMIT 1
            """
        )
        row = cur.fetchone()
        con.close()
    except Exception:
        cached = getattr(mod, cache_key, None)
        if prefer_cached_on_failure and isinstance(cached, dict):
            return dict(cached)
        row = None
    if not row:
        return {
            "bootstrap_required": True,
            "autonomous_mode": False,
            "first_full_scan_id": None,
            "first_full_completed_at": None,
            "updated_at": None,
        }
    payload = {
        "bootstrap_required": bool(row["bootstrap_required"]),
        "autonomous_mode": bool(row["autonomous_mode"]),
        "first_full_scan_id": int(row["first_full_scan_id"]) if row["first_full_scan_id"] is not None else None,
        "first_full_completed_at": _sqlite_ts_to_float(row["first_full_completed_at"]),
        "updated_at": _sqlite_ts_to_float(row["updated_at"]),
    }
    setattr(mod, cache_key, dict(payload))
    return payload

def _has_completed_full_scan_impl() -> bool:
    status = _pipeline_bootstrap_status()
    if not bool(status.get("bootstrap_required")):
        return True
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.execute(
            """
            SELECT scan_id, end_time
            FROM scan_history
            WHERE status = 'completed' AND COALESCE(scan_type, 'full') = 'full'
            ORDER BY end_time DESC, scan_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        con.close()
        return bool(row)
    except Exception:
        return False

def _pipeline_bootstrap_refresh_from_history_impl() -> None:
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT bootstrap_required FROM pipeline_bootstrap_state WHERE id = 1")
        row = cur.fetchone()
        current_required = bool(row["bootstrap_required"]) if row else True
        cur.execute(
            """
            SELECT scan_id, end_time
            FROM scan_history
            WHERE status = 'completed' AND COALESCE(scan_type, 'full') = 'full'
            ORDER BY end_time ASC, scan_id ASC
            LIMIT 1
            """
        )
        first_full = cur.fetchone()
        now = time.time()
        if first_full:
            first_id = int(first_full["scan_id"] or 0)
            first_at = float(first_full["end_time"] or now)
            cur.execute(
                """
                INSERT INTO pipeline_bootstrap_state
                (id, bootstrap_required, autonomous_mode, first_full_scan_id, first_full_completed_at, updated_at)
                VALUES (1, 0, 1, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    bootstrap_required = 0,
                    autonomous_mode = 1,
                    first_full_scan_id = COALESCE(pipeline_bootstrap_state.first_full_scan_id, excluded.first_full_scan_id),
                    first_full_completed_at = COALESCE(pipeline_bootstrap_state.first_full_completed_at, excluded.first_full_completed_at),
                    updated_at = excluded.updated_at
                """,
                (first_id, first_at, now),
            )
        elif current_required is False:
            cur.execute(
                """
                UPDATE pipeline_bootstrap_state
                SET bootstrap_required = 1,
                    autonomous_mode = 0,
                    first_full_scan_id = NULL,
                    first_full_completed_at = NULL,
                    updated_at = ?
                WHERE id = 1
                """,
                (now,),
            )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to refresh pipeline bootstrap state", exc_info=True)

def _pipeline_bootstrap_mark_full_completed_impl(scan_id: int, completed_at: float | None = None) -> None:
    sid = int(scan_id or 0)
    if sid <= 0:
        return
    now = float(completed_at or time.time())
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO pipeline_bootstrap_state
            (id, bootstrap_required, autonomous_mode, first_full_scan_id, first_full_completed_at, updated_at)
            VALUES (1, 0, 1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                bootstrap_required = 0,
                autonomous_mode = 1,
                first_full_scan_id = COALESCE(pipeline_bootstrap_state.first_full_scan_id, excluded.first_full_scan_id),
                first_full_completed_at = COALESCE(pipeline_bootstrap_state.first_full_completed_at, excluded.first_full_completed_at),
                updated_at = excluded.updated_at
            """,
            (sid, now, now),
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to mark pipeline bootstrap full completion", exc_info=True)

def _pipeline_bootstrap_reset_impl() -> None:
    now = time.time()
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO pipeline_bootstrap_state
            (id, bootstrap_required, autonomous_mode, first_full_scan_id, first_full_completed_at, updated_at)
            VALUES (1, 1, 0, NULL, NULL, ?)
            ON CONFLICT(id) DO UPDATE SET
                bootstrap_required = 1,
                autonomous_mode = 0,
                first_full_scan_id = NULL,
                first_full_completed_at = NULL,
                updated_at = excluded.updated_at
            """,
            (now,),
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to reset pipeline bootstrap state", exc_info=True)

def _auto_changed_only_mode_effective_impl() -> bool:
    """Whether watcher-driven changed-only scans are currently allowed."""
    try:
        if not PMDA_AUTO_CHANGED_ONLY_SCAN:
            return False
        if not PMDA_FILES_WATCHER_ENABLED:
            return False
        if bool(_scheduler_paused):
            return False
        if _get_library_mode() != "files":
            return False
    except Exception:
        return False
    return True

def _scan_autonomous_mode_effective_impl() -> bool:
    bootstrap = _pipeline_bootstrap_status()
    return bool(bootstrap.get("autonomous_mode")) and _auto_changed_only_mode_effective()

def get_default_scan_type_impl() -> str:
    if bool(_pipeline_bootstrap_status().get("bootstrap_required")):
        return "full"
    return "changed_only" if _auto_changed_only_mode_effective() else "full"
