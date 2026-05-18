"""Durable pipeline job heartbeat persistence."""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind live PMDA globals for pipeline job persistence."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        '_pipeline_job_update_for_runtime',
        '_pipeline_job_snapshot_for_runtime',
        "_bind_runtime",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def _pipeline_job_update_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_pipeline_job_update`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _pipeline_job_update_impl(*args, **kwargs)

def _pipeline_job_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_pipeline_job_snapshot`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _pipeline_job_snapshot_impl(*args, **kwargs)


def _pipeline_job_update_impl(
    job_type: str,
    *,
    scope: str = "global",
    status: str = "running",
    phase: str = "",
    current: int | None = None,
    total: int | None = None,
    current_item: str = "",
    message: str = "",
    error: str = "",
    run_id: str | int | None = None,
    meta: dict[str, Any] | None = None,
    finished: bool | None = None,
) -> None:
    """
    Persist a durable job heartbeat.

    This is intentionally small and SQLite-backed: it gives the UI/MCP a stable
    source of truth for long-running phases without forcing a full pipeline
    rewrite in one risky change.
    """
    jt = _pipeline_jobs_core.normalize_job_type(job_type)
    if not jt:
        return
    scope_norm = _pipeline_jobs_core.normalize_scope(scope)
    now = time.time()
    status_norm = _pipeline_jobs_core.normalize_status(status)
    finished_at = _pipeline_jobs_core.finished_at_for_status(status_norm, now=now, finished=finished)
    try:
        def _write() -> None:
            con = _state_connect(timeout=10)
            try:
                cur = con.cursor()
                cur.execute(
                    """
                    INSERT INTO pipeline_jobs (
                        job_type, scope, run_id, status, phase, current, total,
                        current_item, message, error, started_at, heartbeat_at,
                        finished_at, meta_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(job_type, scope) DO UPDATE SET
                        run_id = COALESCE(excluded.run_id, pipeline_jobs.run_id),
                        status = excluded.status,
                        phase = excluded.phase,
                        current = excluded.current,
                        total = excluded.total,
                        current_item = excluded.current_item,
                        message = excluded.message,
                        error = excluded.error,
                        started_at = CASE
                            WHEN excluded.status = 'running'
                              AND pipeline_jobs.status IN ('completed', 'failed', 'cancelled', 'idle')
                            THEN excluded.started_at
                            WHEN pipeline_jobs.started_at IS NULL
                            THEN excluded.started_at
                            ELSE pipeline_jobs.started_at
                        END,
                        heartbeat_at = excluded.heartbeat_at,
                        finished_at = excluded.finished_at,
                        meta_json = excluded.meta_json
                    """,
                    (
                        jt,
                        scope_norm,
                        str(run_id) if run_id is not None else None,
                        status_norm,
                        str(phase or ""),
                        max(0, int(current or 0)),
                        max(0, int(total or 0)),
                        str(current_item or "")[:1000],
                        str(message or "")[:2000],
                        str(error or "")[:2000],
                        now,
                        now,
                        finished_at,
                        json.dumps(meta or {}, ensure_ascii=False, default=str),
                    ),
                )
                con.commit()
            finally:
                con.close()

        _state_db_write_retry(_write, label=f"pipeline_jobs:{jt}:{scope_norm}", attempts=4)
    except Exception:
        logging.debug("Failed to persist pipeline job heartbeat job=%s scope=%s", jt, scope_norm, exc_info=True)


def _pipeline_job_snapshot_impl() -> dict[str, dict[str, Any]]:
    try:
        con = _state_connect(timeout=5)
        con.row_factory = sqlite3.Row
        try:
            cur = con.cursor()
            if not _sqlite_table_exists(cur, "pipeline_jobs"):
                return {}
            cur.execute(
                """
                SELECT job_type, scope, run_id, status, phase, current, total,
                       current_item, message, error, started_at, heartbeat_at,
                       finished_at, meta_json
                FROM pipeline_jobs
                ORDER BY job_type ASC, scope ASC
                """
            )
            rows = cur.fetchall()
        finally:
            con.close()
    except Exception:
        return {}
    now = time.time()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        converted = _pipeline_jobs_core.row_to_status(
            row,
            now=now,
            stale_after_sec=PIPELINE_JOB_STALE_AFTER_SEC,
        )
        if converted is None:
            continue
        item_key, payload = converted
        out[item_key] = payload
    return out
