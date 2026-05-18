"""Runtime-owned final scan summary emission.

This module keeps run-summary counters and final notification side effects out
of the PMDA bootstrap while preserving the existing ``emit_final_summary`` call
surface used by ``atexit`` and internal shutdown paths.
"""

from __future__ import annotations

import logging
from typing import Any

_RUNTIME: Any | None = None
_RUN_START_TS: float = 0.0
_RUN_BASELINE: dict[str, int] = {
    "removed_dupes": 0,
    "space_saved": 0,
    "best_rows": 0,
    "loser_rows": 0,
}
_SUMMARY_EMITTED = False


def _bind_runtime(runtime: Any) -> None:
    global _RUNTIME
    _RUNTIME = runtime


def _runtime_module() -> Any:
    if _RUNTIME is None:
        raise RuntimeError("Scan summary runtime is not bound")
    return _RUNTIME


def _count_rows(table: str) -> int:
    runtime = _runtime_module()
    con = runtime.sqlite3.connect(str(runtime.STATE_DB_FILE))
    try:
        cur = con.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return int((cur.fetchone() or [0])[0] or 0)
    finally:
        con.close()


def initialize_run_summary_for_runtime(runtime: Any) -> None:
    """Capture process-start counters used by the final summary delta."""
    global _RUN_START_TS, _RUN_BASELINE
    _bind_runtime(runtime)
    _RUN_START_TS = runtime.time.time()
    _RUN_BASELINE = {
        "removed_dupes": int(runtime.get_stat("removed_dupes") or 0),
        "space_saved": int(runtime.get_stat("space_saved") or 0),
        "best_rows": 0,
        "loser_rows": 0,
    }
    try:
        _RUN_BASELINE["best_rows"] = _count_rows("duplicates_best")
        _RUN_BASELINE["loser_rows"] = _count_rows("duplicates_loser")
    except Exception:
        pass


def _library_counts() -> tuple[int, int]:
    runtime = _runtime_module()
    if runtime._get_library_mode() != "files":
        return 0, 0
    try:
        conn = runtime._files_pg_connect(acquire_timeout_sec=0.35)
        if conn is None:
            return 0, 0
        try:
            with conn.cursor() as cur:
                with runtime._files_pg_statement_timeout(cur, 1200):
                    cur.execute(
                        """
                        SELECT
                            (SELECT COUNT(*) FROM files_artists),
                            (SELECT COUNT(*) FROM files_albums)
                        """
                    )
                    artists, albums = cur.fetchone() or (0, 0)
                    return int(artists or 0), int(albums or 0)
        finally:
            conn.close()
    except Exception:
        return 0, 0


def emit_final_summary_for_runtime(runtime: Any, reason: str = "normal") -> None:
    """Emit a one-shot final summary and optional Discord notification."""
    global _SUMMARY_EMITTED
    _bind_runtime(runtime)
    if _SUMMARY_EMITTED:
        return
    for handler in logging.getLogger().handlers:
        stream = getattr(handler, "stream", None)
        if stream is not None and bool(getattr(stream, "closed", False)):
            _SUMMARY_EMITTED = True
            return

    duration = max(0, int(runtime.time.time() - _RUN_START_TS))
    removed = max(0, int(runtime.get_stat("removed_dupes") or 0) - _RUN_BASELINE["removed_dupes"])
    saved_mb = max(0, int(runtime.get_stat("space_saved") or 0) - _RUN_BASELINE["space_saved"])
    try:
        new_groups = max(0, _count_rows("duplicates_best") - _RUN_BASELINE.get("best_rows", 0))
        new_losers = max(0, _count_rows("duplicates_loser") - _RUN_BASELINE.get("loser_rows", 0))
    except Exception:
        new_groups = new_losers = 0

    total_artists, total_albums = _library_counts()

    bar = "-" * 85
    logging.info("\n%s", bar)
    logging.info("FINAL SUMMARY")
    logging.info("Total artists           : %s", f"{total_artists:,}" if total_artists else "n/a")
    logging.info("Total albums            : %s", f"{total_albums:,}" if total_albums else "n/a")
    logging.info("Albums with dupes       : %s", f"{new_groups:,}")
    logging.info("Folders moved           : %s", f"{removed:,}")
    logging.info("Total space reclaimed   : %s MB", f"{saved_mb:,}")
    logging.info("Duration                : %s s", f"{duration:,}")
    logging.info("%s\n", bar)

    if runtime.DISCORD_WEBHOOK:
        fields = [
            {"name": "Artists", "value": (f"{total_artists:,}" if total_artists else "n/a"), "inline": True},
            {"name": "Albums", "value": (f"{total_albums:,}" if total_albums else "n/a"), "inline": True},
            {"name": "Groups", "value": f"{new_groups:,}", "inline": True},
            {"name": "Removed", "value": f"{removed:,}", "inline": True},
            {"name": "Reclaimed", "value": f"{saved_mb:,} MB", "inline": True},
            {"name": "Duration", "value": f"{duration:,} s", "inline": True},
        ]
        runtime.notify_discord_embed("PMDA - Final summary", "Run completed.", fields=fields)

    _SUMMARY_EMITTED = True
