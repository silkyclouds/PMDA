"""Runtime-owned durable storage bucket updates."""

from __future__ import annotations

import logging
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "update_scan_storage_bucket_row_for_runtime",
            "_update_scan_storage_bucket_row",
        }:
            continue
        globals()[name] = value


def update_scan_storage_bucket_row_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> None:
    _bind_runtime(runtime)
    return _update_scan_storage_bucket_row(*args, **kwargs)


def _update_scan_storage_bucket_row(
    run_id: str | None,
    meta: dict[str, Any] | None,
    *,
    status: str | None = None,
    albums_done: int | None = None,
    started_at: float | None = None,
    finished_at: float | None = None,
    message: str | None = None,
) -> None:
    run_id_text = str(run_id or "").strip()
    if not run_id_text or not meta:
        return
    try:
        bucket_order = int(meta.get("storage_bucket_order") or meta.get("bucket_order") or 0)
    except Exception:
        bucket_order = 0
    device_id = str(meta.get("storage_device_id") or "").strip()
    if not device_id:
        return
    assignments: list[str] = []
    params: list[Any] = []
    if status is not None:
        assignments.append("status = ?")
        params.append(str(status or ""))
    if albums_done is not None:
        assignments.append("albums_done = ?")
        params.append(max(0, int(albums_done or 0)))
    if started_at is not None:
        assignments.append("started_at = COALESCE(started_at, ?)")
        params.append(float(started_at))
    if finished_at is not None:
        assignments.append("finished_at = ?")
        params.append(float(finished_at))
    if message is not None:
        assignments.append("message = ?")
        params.append(str(message or "")[:1000])
    if not assignments:
        return
    con = None
    try:
        con = _state_connect(timeout=5)
        con.execute(
            f"""
            UPDATE scan_storage_buckets
            SET {", ".join(assignments)}
            WHERE run_id = ? AND bucket_order = ? AND storage_device_id = ?
            """,
            tuple(params + [run_id_text, bucket_order, device_id]),
        )
        con.commit()
    except Exception:
        logging.debug("Storage bucket DB update failed for run_id=%s device=%s", run_id_text, device_id, exc_info=True)
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
    return None
