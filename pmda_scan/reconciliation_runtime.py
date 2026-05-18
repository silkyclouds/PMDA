"""Runtime-owned scan track reconciliation counters."""

from __future__ import annotations

import logging
import sqlite3
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "compute_scan_track_reconciliation_for_runtime",
            "_compute_scan_track_reconciliation",
        }:
            continue
        globals()[name] = value


def compute_scan_track_reconciliation_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> dict[str, int]:
    _bind_runtime(runtime)
    return _compute_scan_track_reconciliation(*args, **kwargs)


def _compute_scan_track_reconciliation(scan_id: int | None, detected_total: int) -> dict[str, int]:
    """
    Compute run-level track reconciliation counters:
    detected = library_kept + moved_dupes + moved_incomplete + unaccounted
    """
    detected = max(0, int(detected_total or 0))
    out = {
        "detected_total": detected,
        "library_kept": 0,
        "moved_dupes": 0,
        "moved_incomplete": 0,
        "unaccounted": detected,
    }
    if not scan_id:
        return out

    moved_dupes = 0
    moved_incomplete = 0
    con = None
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
        cur = con.cursor()
        cur.execute("PRAGMA table_info(scan_moves)")
        move_cols = [r[1] for r in cur.fetchall()]
        has_reason = "move_reason" in move_cols
        if has_reason:
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(
                        CASE
                            WHEN LOWER(COALESCE(m.move_reason, 'dedupe')) = 'dedupe'
                            THEN COALESCE(e.actual_track_count, 0)
                            ELSE 0
                        END
                    ), 0),
                    COALESCE(SUM(
                        CASE
                            WHEN LOWER(COALESCE(m.move_reason, 'dedupe')) = 'incomplete'
                            THEN COALESCE(e.actual_track_count, 0)
                            ELSE 0
                        END
                    ), 0)
                FROM scan_moves m
                LEFT JOIN scan_editions e
                  ON e.scan_id = m.scan_id
                 AND e.album_id = m.album_id
                WHERE m.scan_id = ?
                  AND COALESCE(m.restored, 0) = 0
                """,
                (int(scan_id),),
            )
            row = cur.fetchone() or (0, 0)
            moved_dupes = int(row[0] or 0)
            moved_incomplete = int(row[1] or 0)
        else:
            cur.execute(
                """
                SELECT COALESCE(SUM(COALESCE(e.actual_track_count, 0)), 0)
                FROM scan_moves m
                LEFT JOIN scan_editions e
                  ON e.scan_id = m.scan_id
                 AND e.album_id = m.album_id
                WHERE m.scan_id = ?
                  AND COALESCE(m.restored, 0) = 0
                """,
                (int(scan_id),),
            )
            row = cur.fetchone() or (0,)
            moved_dupes = int(row[0] or 0)
    except Exception:
        logging.debug("Track reconciliation: could not compute moved track counts", exc_info=True)
    finally:
        try:
            con.close()
        except Exception:
            pass

    library_kept = 0
    if _get_library_mode() == "files":
        try:
            _artists, _albums, tracks = _files_index_read_counts()
            library_kept = int(tracks or 0)
        except Exception:
            logging.debug("Track reconciliation: could not read files_tracks count", exc_info=True)

    out["library_kept"] = max(0, int(library_kept))
    out["moved_dupes"] = max(0, int(moved_dupes))
    out["moved_incomplete"] = max(0, int(moved_incomplete))
    out["unaccounted"] = max(
        0,
        out["detected_total"] - out["library_kept"] - out["moved_dupes"] - out["moved_incomplete"],
    )
    return out
