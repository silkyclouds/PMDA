"""Operator-facing PMDA job status payloads.

This module is intentionally pure: it receives snapshots gathered by the
runtime layer and returns the normalized status shape consumed by UI and MCP.
Database access, locks, and live service calls stay outside this file.
"""

from __future__ import annotations

from typing import Any, Optional


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return int(default)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except Exception:
        return float(default)


def percent(done: Any, total: Any) -> Optional[float]:
    try:
        total_n = float(total or 0)
        done_n = float(done or 0)
    except Exception:
        return None
    if total_n <= 0:
        return None
    return round(max(0.0, min(100.0, (done_n / total_n) * 100.0)), 2)


def job_record(
    job_id: str,
    *,
    job_type: str,
    status: str,
    running: bool = False,
    phase: str = "",
    label: str = "",
    done: Any = 0,
    total: Any = 0,
    unit: str = "items",
    heartbeat_at: Any = None,
    last_item: str = "",
    eta_seconds: Any = None,
    blockers: list[str] | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    done_i = max(0, _to_int(done))
    total_i = max(0, _to_int(total))
    return {
        "job_id": str(job_id or ""),
        "job_type": str(job_type or ""),
        "status": str(status or ("running" if running else "idle")),
        "running": bool(running),
        "phase": str(phase or ""),
        "label": str(label or ""),
        "done": done_i,
        "total": total_i,
        "unit": str(unit or "items"),
        "percent": percent(done_i, total_i),
        "heartbeat_at": float(heartbeat_at) if heartbeat_at not in (None, "") else None,
        "last_item": str(last_item or ""),
        "eta_seconds": _to_int(eta_seconds) if eta_seconds not in (None, "") else None,
        "blockers": list(blockers or []),
        "details": dict(details or {}),
    }


def build_jobs_status_snapshot(
    *,
    now: float,
    state: dict[str, Any] | None,
    files_index: dict[str, Any] | None,
    profile_backfill: dict[str, Any] | None,
    metadata_summary: dict[str, Any] | None,
    runtime_snapshot: dict[str, Any] | None,
    latest_runtime_actions: list[dict[str, Any]] | None,
    published_rows: int,
    media_cache_root: str,
    storage_current: dict[str, Any] | None,
    allow_plex_db_in_files_mode: bool,
) -> dict[str, Any]:
    """Build the single operator-facing job truth for UI and MCP."""
    st = dict(state or {})
    files = dict(files_index or {})
    profile = dict(profile_backfill or {})
    metadata = dict(metadata_summary or {})
    runtime = dict(runtime_snapshot or {})
    runtime_actions = list(latest_runtime_actions or [])
    storage = dict(storage_current or {})

    scan_running = bool(st.get("scanning") or st.get("scan_starting") or st.get("scan_finalizing"))
    scan_starting = bool(st.get("scan_starting"))
    scan_finalizing = bool(st.get("scan_finalizing"))
    scan_phase = str(st.get("scan_phase") or st.get("scan_discovery_stage") or st.get("current_step") or "").strip()
    scan_label = str(
        st.get("scan_finalizing_label")
        or st.get("scan_discovery_label")
        or st.get("scan_current_focus")
        or st.get("current_step")
        or ""
    ).strip()
    scan_done = _to_int(
        st.get("scan_finalizing_item_done")
        or st.get("scan_processed_albums_count")
        or st.get("scan_mb_done_count")
        or st.get("scan_format_done_count")
        or 0
    )
    scan_total = _to_int(
        st.get("scan_finalizing_item_total")
        or st.get("scan_total_albums")
        or st.get("detected_albums_total")
        or 0
    )
    scan_last_item = str(st.get("scan_current_focus") or st.get("scan_current_album") or st.get("scan_current_artist") or "").strip()
    scan_heartbeat = st.get("scan_last_progress_at") or st.get("scan_finalizing_updated_at") or st.get("scan_start_time")
    scan_resume_run_id = str(st.get("scan_resume_run_id") or st.get("scan_resume_requested_run_id") or "").strip()
    scan_blockers = list(st.get("scan_blockers") or [])
    scan_published = _to_int(st.get("scan_published_albums_count"))
    scan_processed = _to_int(st.get("scan_processed_albums_count"))
    export_progress = dict(st.get("export_progress") or {})
    incomplete_move_running = bool(st.get("scan_incomplete_move_running"))
    deduping = bool(st.get("deduping"))
    scan_post_processing = bool(st.get("scan_post_processing"))
    scan_profile_enrich_running = bool(st.get("scan_profile_enrich_running"))
    reco_state = dict(st.get("files_reco_embeddings") or {})
    dupe_moved_count = _to_int(st.get("scan_dupe_moved_count"))
    dupe_moved_mb = _to_int(st.get("scan_dupe_moved_mb"))
    incomplete_moved_count = _to_int(st.get("scan_incomplete_moved_count"))
    incomplete_moved_mb = _to_int(st.get("scan_incomplete_moved_mb"))

    jobs: dict[str, dict[str, Any]] = {}
    jobs["scan"] = job_record(
        "scan",
        job_type="scan",
        status="starting" if scan_starting else "finalizing" if scan_finalizing else "running" if scan_running else "idle",
        running=scan_running,
        phase=scan_phase,
        label=scan_label,
        done=scan_done,
        total=scan_total,
        unit="albums" if not scan_finalizing else "items",
        heartbeat_at=scan_heartbeat,
        last_item=scan_last_item,
        blockers=[str(item) for item in scan_blockers if str(item or "").strip()],
        details={"resume_run_id": scan_resume_run_id or None},
    )

    publication_running = bool(scan_running and scan_processed > 0 and scan_published < scan_processed)
    published_rows_i = _to_int(published_rows)
    jobs["publication"] = job_record(
        "publication",
        job_type="publication",
        status="running" if publication_running else "done" if published_rows_i > 0 else "idle",
        running=publication_running,
        phase="publishing_rows" if publication_running else "published_snapshot",
        label="Publishing visible library rows" if publication_running else "Published library snapshot",
        done=max(scan_published, published_rows_i if not publication_running else 0),
        total=max(scan_processed, published_rows_i),
        unit="albums",
        heartbeat_at=now,
        details={"published_rows": published_rows_i, "scan_published_rows": scan_published},
    )

    export_running = bool(export_progress.get("running"))
    materialization_running = bool(export_running or incomplete_move_running or deduping)
    materialization_phase = "export" if export_running else "incomplete_move" if incomplete_move_running else "dedupe_move" if deduping else "idle"
    jobs["materialization"] = job_record(
        "materialization",
        job_type="materialization",
        status="running" if materialization_running else "idle",
        running=materialization_running,
        phase=materialization_phase,
        label="Moving/copying audited filesystem outputs" if materialization_running else "No materialization job active",
        done=_to_int(export_progress.get("albums_done")),
        total=_to_int(export_progress.get("total_albums")),
        unit="albums",
        heartbeat_at=now,
        details={
            "export_progress": export_progress,
            "duplicate_moved_count": dupe_moved_count,
            "duplicate_moved_mb": dupe_moved_mb,
            "incomplete_moved_count": incomplete_moved_count,
            "incomplete_moved_mb": incomplete_moved_mb,
        },
    )

    index_done = _to_int(files.get("phase_item_done") or files.get("current"))
    index_total = _to_int(files.get("phase_item_total") or files.get("total"))
    files_running = bool(files.get("running"))
    files_phase = str(files.get("phase") or "")
    jobs["library_index"] = job_record(
        "library_index",
        job_type="library_index",
        status="running" if files_running else str(files.get("phase") or "idle"),
        running=files_running,
        phase=files_phase,
        label=str(files.get("phase_message") or ""),
        done=index_done,
        total=index_total,
        unit=str(files.get("phase_item_label") or "items"),
        heartbeat_at=files.get("updated_at"),
        last_item=str(files.get("current_folder") or files.get("current_artist") or ""),
        eta_seconds=files.get("phase_eta_seconds"),
        details=files,
    )

    media_running = bool(files_running and files_phase == "media_cache")
    jobs["media_cache"] = job_record(
        "media_cache",
        job_type="media_cache",
        status="running" if media_running else "background",
        running=media_running,
        phase="media_cache" if media_running else "cache_on_demand",
        label="Preparing artwork/media cache" if media_running else "Media cache is post-publication/on-demand",
        done=index_done if media_running else 0,
        total=index_total if media_running else 0,
        unit="items",
        heartbeat_at=files.get("updated_at") if media_running else None,
        details={"root": str(media_cache_root or "")},
    )

    profile_running = bool(profile.get("running") or scan_profile_enrich_running or scan_post_processing)
    profile_phase = str(profile.get("phase") or ("scan_post_processing" if scan_post_processing else ""))
    profile_label = str(profile.get("phase_label") or ("Artist/profile enrichment" if profile_running else ""))
    jobs["profile_backfill"] = job_record(
        "profile_backfill",
        job_type="profile_backfill",
        status="running" if profile_running else "idle",
        running=profile_running,
        phase=profile_phase,
        label=profile_label,
        done=_to_int(profile.get("current") or profile.get("phase_current")),
        total=_to_int(profile.get("total") or profile.get("phase_total")),
        unit="artists",
        heartbeat_at=profile.get("last_probe_at") or profile.get("started_at"),
        last_item=str(profile.get("current_artist") or ""),
        details=profile,
    )

    embeddings_running = bool(reco_state.get("running"))
    jobs["embeddings"] = job_record(
        "embeddings",
        job_type="embeddings",
        status="running" if embeddings_running else str(reco_state.get("phase") or "idle"),
        running=embeddings_running,
        phase=str(reco_state.get("phase") or ""),
        label="Recommendation embeddings",
        done=_to_int(reco_state.get("embeddings_done") or reco_state.get("tracks_done")),
        total=_to_int(reco_state.get("tracks_total")),
        unit="tracks",
        heartbeat_at=reco_state.get("updated_at") or reco_state.get("started_at"),
        eta_seconds=reco_state.get("eta_seconds"),
        details=reco_state,
    )

    metadata_running = _to_int(metadata.get("running") or metadata.get("in_progress")) > 0
    jobs["metadata_workers"] = job_record(
        "metadata_workers",
        job_type="metadata_workers",
        status="running" if metadata_running else "idle",
        running=metadata_running,
        phase="metadata_jobs",
        label="Queued metadata worker jobs",
        done=_to_int(metadata.get("completed")),
        total=_to_int(metadata.get("total")),
        unit="jobs",
        heartbeat_at=now,
        details=metadata,
    )

    runtime_running = any(str(action.get("status") or "").lower() in {"running", "pending"} for action in runtime_actions)
    runtime_heartbeat = max([_to_float(action.get("updated_at")) for action in runtime_actions] or [0.0]) or None
    jobs["runtime_repair"] = job_record(
        "runtime_repair",
        job_type="runtime_repair",
        status="running" if runtime_running else "idle",
        running=runtime_running,
        phase="managed_runtime",
        label="MusicBrainz/Ollama runtime maintenance",
        done=0,
        total=0,
        unit="actions",
        heartbeat_at=runtime_heartbeat,
        details={"latest_actions": runtime_actions, "managed_runtime": runtime},
    )

    storage_running = bool(storage.get("enabled") and storage.get("active_devices"))
    jobs["storage"] = job_record(
        "storage",
        job_type="storage",
        status="running" if storage_running else "idle",
        running=storage_running,
        phase=str(storage.get("current_device_id") or ""),
        label="Disk-aware storage bucket",
        done=_to_int(storage.get("bucket_done")),
        total=_to_int(storage.get("bucket_total")),
        unit="albums",
        heartbeat_at=storage.get("bucket_started_at") or storage.get("started_at"),
        last_item=str(storage.get("current_device_label") or storage.get("current_device_id") or ""),
        details=storage,
    )

    return {
        "generated_at": now,
        "jobs": jobs,
        "running": [job_id for job_id, row in jobs.items() if bool(row.get("running"))],
        "post_publication_jobs": ["media_cache", "profile_backfill", "embeddings"],
        "notes": {
            "publication_blocks_browse": False,
            "post_publication_blocks_scan_completion": False,
            "legacy_player_db_access": False,
            "files_mode_opens_plex_db": bool(allow_plex_db_in_files_mode),
        },
    }
