"""Metadata enrichment queue persistence helpers."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from typing import Any


ConnectionFactory = Callable[..., Any]


def enqueue_metadata_job(
    connect: ConnectionFactory,
    album_manifest: dict[str, Any],
    *,
    provider_hints: dict[str, Any] | None = None,
    cache_keys: list[str] | None = None,
    priority: int = 50,
    queue_name: str = "metadata",
    scope: str = "album",
    run_id: str = "",
    scan_id: int | None = None,
    job_id: str | None = None,
    now: float | None = None,
) -> str:
    """Insert one queued metadata job and return its job id."""
    new_job_id = str(job_id or uuid.uuid4().hex)
    ts = float(time.time() if now is None else now)
    con = connect(timeout=10)
    try:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO metadata_jobs (
                job_id, status, priority, queue_name, scope,
                album_manifest_json, provider_hints_json, cache_keys_json,
                run_id, scan_id, created_at, updated_at
            ) VALUES (?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_job_id,
                int(max(0, min(1000, int(priority or 50)))),
                str(queue_name or "metadata"),
                str(scope or "album"),
                json.dumps(album_manifest or {}, ensure_ascii=False),
                json.dumps(provider_hints or {}, ensure_ascii=False),
                json.dumps(list(cache_keys or []), ensure_ascii=False),
                str(run_id or ""),
                int(scan_id or 0) if scan_id else None,
                ts,
                ts,
            ),
        )
        con.commit()
    finally:
        con.close()
    return new_job_id


def metadata_jobs_summary(connect: ConnectionFactory) -> dict[str, Any]:
    """Return lightweight queue counts for the operator jobs panel."""
    con = connect(timeout=10)
    out = {
        "queued": 0,
        "running": 0,
        "completed": 0,
        "failed": 0,
        "total": 0,
        "oldest_queued_at": 0.0,
    }
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT status, COUNT(*) AS c
            FROM metadata_jobs
            GROUP BY status
            """
        )
        for status_raw, count_raw in cur.fetchall():
            status = str(status_raw or "").strip().lower()
            count = int(count_raw or 0)
            out[status] = count
            out["total"] += count
        cur.execute(
            """
            SELECT MIN(created_at)
            FROM metadata_jobs
            WHERE status = 'queued'
            """
        )
        row = cur.fetchone()
        out["oldest_queued_at"] = float(row[0] or 0.0) if row else 0.0
    finally:
        con.close()
    return out
