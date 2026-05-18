"""Runtime adapter for operator-facing PMDA job status snapshots."""

from __future__ import annotations

import logging
import time
from typing import Any

from pmda_core import job_status as job_status_core


def pmda_jobs_status_snapshot_for_runtime(runtime: Any) -> dict[str, Any]:
    """Aggregate durable/runtime job state for the UI and MCP."""
    now = time.time()
    with runtime.lock:
        state_snapshot = dict(runtime.state or {})

    files_index = runtime._files_index_get_state()
    with runtime._files_profile_backfill_lock:
        profile_backfill = dict(runtime._files_profile_backfill_state or {})

    metadata_summary: dict[str, Any] = {}
    try:
        metadata_summary = runtime._metadata_jobs_summary()
    except Exception:
        logging.debug("Failed to collect metadata job summary", exc_info=True)

    runtime_snapshot: dict[str, Any] = {}
    try:
        runtime_snapshot = runtime._managed_runtime_status_snapshot(include_candidates=False)
    except Exception:
        logging.debug("Failed to collect managed runtime status", exc_info=True)

    latest_runtime_actions: list[dict[str, Any]] = []
    for bundle_type in (runtime._MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, runtime._MANAGED_RUNTIME_OLLAMA_BUNDLE):
        try:
            action = runtime._managed_runtime_get_latest_action(bundle_type)
            if action:
                latest_runtime_actions.append(action)
        except Exception:
            logging.debug("Failed to read latest managed runtime action for %s", bundle_type, exc_info=True)

    try:
        published_rows = int(runtime._files_library_published_row_count() or 0)
    except Exception:
        published_rows = 0

    storage_current = runtime._mcp_storage_current()
    return job_status_core.build_jobs_status_snapshot(
        now=now,
        state=state_snapshot,
        files_index=files_index,
        profile_backfill=profile_backfill,
        metadata_summary=metadata_summary,
        runtime_snapshot=runtime_snapshot,
        latest_runtime_actions=latest_runtime_actions,
        published_rows=published_rows,
        media_cache_root=str(runtime.MEDIA_CACHE_ROOT or ""),
        storage_current=storage_current,
        allow_plex_db_in_files_mode=bool(runtime._ALLOW_PLEX_DB_IN_FILES_MODE),
    )
