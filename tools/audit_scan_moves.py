#!/usr/bin/env python3
"""Audit scan moves against strict match verdicts.

Usage:
  python3 scripts/audit_scan_moves.py --db /path/to/state.db --scan-id 1
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any


def _norm_path(value: str | None) -> str:
    return (value or "").strip().rstrip("/").lower()


def _row_as_dict(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def _load_latest_scan_id(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(scan_id) FROM scan_history").fetchone()
    if not row or row[0] is None:
        raise RuntimeError("No scan_history rows found in the selected database.")
    return int(row[0])


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    names: set[str] = set()
    for row in rows:
        # pragma tuple: cid, name, type, notnull, dflt_value, pk
        if isinstance(row, dict):
            names.add(str(row.get("name") or ""))
        else:
            names.add(str(row[1]))
    return {n for n in names if n}


def audit_scan(db_path: Path, scan_id: int) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = _row_as_dict

    moves = conn.execute(
        """
        SELECT move_id, move_reason, original_path, moved_to_path, size_mb
        FROM scan_moves
        WHERE scan_id = ?
        ORDER BY move_id
        """,
        (scan_id,),
    ).fetchall()
    edition_cols = _table_columns(conn, "scan_editions")
    has_strict_cols = {"strict_match_verified", "strict_match_provider", "strict_reject_reason"}.issubset(edition_cols)

    if has_strict_cols:
        editions = conn.execute(
            """
            SELECT folder, strict_match_verified, strict_match_provider, strict_reject_reason
            FROM scan_editions
            WHERE scan_id = ?
            """,
            (scan_id,),
        ).fetchall()
    else:
        editions = conn.execute(
            """
            SELECT folder
            FROM scan_editions
            WHERE scan_id = ?
            """,
            (scan_id,),
        ).fetchall()

    by_folder: dict[str, dict[str, Any]] = {}
    for row in editions:
        folder = _norm_path(str(row.get("folder") or ""))
        if folder:
            if has_strict_cols:
                by_folder[folder] = row
            else:
                by_folder[folder] = {
                    "folder": row.get("folder"),
                    "strict_match_verified": None,
                    "strict_match_provider": "",
                    "strict_reject_reason": "strict_columns_missing_in_scan_editions",
                }

    counters = Counter()
    provider_counts = Counter()
    missing_examples: list[str] = []
    strict_no_examples: list[dict[str, Any]] = []

    for move in moves:
        reason = str(move.get("move_reason") or "unknown")
        original_path = str(move.get("original_path") or "")
        counters["moves_total"] += 1
        counters[f"{reason}_moves"] += 1

        edition = by_folder.get(_norm_path(original_path))
        if edition is None:
            counters[f"{reason}_edition_missing"] += 1
            if len(missing_examples) < 20:
                missing_examples.append(original_path)
            continue

        strict_verified = int(edition.get("strict_match_verified") or 0)
        if strict_verified == 1:
            counters[f"{reason}_strict_yes"] += 1
            provider = str(edition.get("strict_match_provider") or "unknown").strip() or "unknown"
            provider_counts[provider] += 1
        else:
            counters[f"{reason}_strict_no"] += 1
            if len(strict_no_examples) < 20:
                strict_no_examples.append(
                    {
                        "original_path": original_path,
                        "provider": str(edition.get("strict_match_provider") or ""),
                        "reject_reason": str(edition.get("strict_reject_reason") or ""),
                    }
                )

    report = {
        "scan_id": scan_id,
        "db_path": str(db_path),
        "strict_columns_present": has_strict_cols,
        "summary": dict(counters),
        "strict_provider_counts": dict(provider_counts),
        "examples": {
            "edition_missing_sample": missing_examples,
            "strict_no_sample": strict_no_examples,
        },
    }
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit scan moves vs strict match verdicts.")
    parser.add_argument(
        "--db",
        default="/config/state.db",
        help="Path to state.db (default: /config/state.db).",
    )
    parser.add_argument(
        "--scan-id",
        type=int,
        default=None,
        help="Scan ID to audit (default: latest scan).",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        scan_id = int(args.scan_id) if args.scan_id is not None else _load_latest_scan_id(conn)
    finally:
        conn.close()

    report = audit_scan(db_path, scan_id)
    if args.pretty:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
