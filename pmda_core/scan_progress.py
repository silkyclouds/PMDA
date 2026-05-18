"""Pure scan progress helpers.

The HTTP layer still assembles the full progress payload, but the calculations
that decide phase, ETA, and active worker summaries live here so they can be
tested without importing the monolithic runtime state.
"""

from __future__ import annotations

import time
from typing import Any


def safe_int(value: Any, default: int = 0) -> int:
    """Best-effort integer coercion for mutable runtime state values."""
    try:
        return int(value or 0)
    except Exception:
        return int(default)


def cache_state_key(runtime_state: dict[str, Any]) -> tuple[Any, ...]:
    """Build the coarse progress-cache key from mutable scan runtime state."""

    try:
        raw_active = runtime_state.get("scan_active_artists") or {}
    except Exception:
        raw_active = {}
    active_count = 0
    for key, info in dict(raw_active).items():
        if str(key).startswith("_") or not isinstance(info, dict):
            continue
        active_count += 1
    export_progress = runtime_state.get("export_progress") or {}
    return (
        bool(runtime_state.get("scanning") or runtime_state.get("scan_starting")),
        bool(runtime_state.get("scan_starting")),
        safe_int(runtime_state.get("scan_id")),
        str(runtime_state.get("scan_type") or ""),
        safe_int(runtime_state.get("scan_artists_processed")),
        safe_int(runtime_state.get("scan_artists_total")),
        safe_int(runtime_state.get("scan_total_albums")),
        safe_int(runtime_state.get("scan_processed_albums_count")),
        safe_int(runtime_state.get("scan_format_done_count")),
        safe_int(runtime_state.get("scan_mb_done_count")),
        safe_int(runtime_state.get("scan_published_albums_count")),
        bool(runtime_state.get("scan_discovery_running")),
        str(runtime_state.get("scan_discovery_stage") or ""),
        bool(runtime_state.get("scan_prescan_cache_snapshot_running")),
        safe_int(runtime_state.get("scan_prescan_cache_snapshot_rows")),
        bool(runtime_state.get("scan_published_catchup_running")),
        safe_int(runtime_state.get("scan_published_catchup_done")),
        bool(runtime_state.get("scan_run_scope_preparing")),
        safe_int(runtime_state.get("scan_run_scope_done")),
        bool(runtime_state.get("scan_incomplete_move_running")),
        safe_int(runtime_state.get("scan_incomplete_move_done")),
        bool(runtime_state.get("deduping")),
        safe_int(runtime_state.get("dedupe_progress")),
        bool(export_progress.get("running")),
        safe_int(export_progress.get("albums_done")),
        bool(runtime_state.get("scan_profile_enrich_running")),
        safe_int(runtime_state.get("scan_profile_enrich_done")),
        bool(runtime_state.get("scan_post_processing")),
        safe_int(runtime_state.get("scan_post_done")),
        bool(runtime_state.get("scan_finalizing")),
        str(runtime_state.get("scan_finalizing_stage") or ""),
        str(runtime_state.get("scan_finalizing_label") or ""),
        safe_int(runtime_state.get("scan_finalizing_done")),
        safe_int(runtime_state.get("scan_finalizing_total")),
        safe_int(runtime_state.get("scan_finalizing_item_done")),
        safe_int(runtime_state.get("scan_finalizing_item_total")),
        str(runtime_state.get("scan_finalizing_item_label") or ""),
        active_count,
        int(len(runtime_state.get("scan_steps_log") or [])),
        bool(runtime_state.get("storage_power_saver_enabled")),
        str(runtime_state.get("storage_current_device_id") or ""),
        str(runtime_state.get("storage_current_device_label") or ""),
        safe_int(runtime_state.get("storage_bucket_done")),
        safe_int(runtime_state.get("storage_bucket_total")),
        safe_int(runtime_state.get("storage_buckets_done")),
        safe_int(runtime_state.get("storage_buckets_total")),
        str(runtime_state.get("storage_validation_error") or ""),
    )


def percent(done: Any, total: Any) -> float:
    """Return a bounded percentage rounded for API payloads."""
    done_n = max(0, safe_int(done))
    total_n = max(0, safe_int(total))
    if total_n <= 0:
        return 0.0
    return round((float(min(done_n, total_n)) / float(total_n)) * 100.0, 2)


def elapsed_seconds(scanning: bool, scan_start_time: Any, *, now: float | None = None) -> int | None:
    """Return elapsed seconds for a running scan, or None when unavailable."""
    if not scanning or not scan_start_time:
        return None
    try:
        current = float(time.time() if now is None else now)
        return int(max(0.0, current - float(scan_start_time)))
    except Exception:
        return None


def rate_eta(progress: Any, total: Any, elapsed: Any) -> tuple[float | None, int | None]:
    """Return phase rate and ETA from progress counters."""
    progress_n = safe_int(progress)
    total_n = safe_int(total)
    elapsed_n = safe_int(elapsed)
    if elapsed_n <= 0 or progress_n <= 0 or total_n <= 0:
        return None, None
    try:
        phase_rate = float(progress_n) / float(elapsed_n)
    except Exception:
        return None, None
    if phase_rate <= 0:
        return phase_rate, None
    if total_n <= progress_n:
        return phase_rate, None
    return phase_rate, int(max(0, total_n - progress_n) / phase_rate)


def count_progress_snapshot(
    *,
    done: Any,
    total: Any,
    running: bool = False,
    started_at: Any = None,
    updated_at: Any = None,
    stall_after_seconds: float | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Return bounded count, percent, ETA, and stale heartbeat status."""

    done_n = safe_int(done)
    total_n = safe_int(total)
    if total_n > 0:
        done_n = max(0, min(done_n, total_n))
    else:
        done_n = max(0, done_n)

    now_ts = float(time.time() if now is None else now)
    eta_seconds = None
    if running and started_at and done_n > 0 and total_n > 0:
        try:
            elapsed = max(0.0, now_ts - float(started_at))
        except Exception:
            elapsed = 0.0
        if elapsed > 0:
            rate = float(done_n) / elapsed
            if rate > 0:
                eta_seconds = int(max(0, total_n - done_n) / rate)

    stalled = False
    if running and updated_at and stall_after_seconds is not None:
        try:
            stalled = (now_ts - float(updated_at)) >= float(stall_after_seconds)
        except Exception:
            stalled = False

    return {
        "done": int(done_n),
        "total": int(total_n),
        "percent": percent(done_n, total_n) if total_n > 0 else 0.0,
        "eta_seconds": eta_seconds,
        "stalled": bool(stalled),
    }


def pre_scan_current_step(
    *,
    scan_resume_run_id: Any = None,
    scan_discovery_stage: Any = None,
    prescan_snapshot_active: bool = False,
    published_catchup_active: bool = False,
) -> str:
    """Choose the user-visible pre-scan step label."""
    if prescan_snapshot_active:
        return "snapshotting_prescan_cache"
    if published_catchup_active:
        return "rehydrating_library_index"
    discovery_stage = str(scan_discovery_stage or "").strip().lower()
    if discovery_stage == "filesystem":
        return "discovering_filesystem"
    if discovery_stage == "album_candidates":
        return "building_album_candidates"
    return "restoring_resume_plan" if scan_resume_run_id else "preparing_prescan"


def pre_work_stage_progress(
    *,
    progress: Any,
    total: Any,
    phase_progress: Any = 0.0,
    current_step: str | None = None,
    eta_seconds: Any = None,
    run_scope_preparing: bool = False,
    run_scope_done: Any = 0,
    run_scope_total: Any = 0,
    run_scope_percent: Any = 0.0,
    run_scope_eta_seconds: Any = None,
    pre_scan_active: bool = False,
    scan_resume_run_id: Any = None,
    scan_discovery_stage: Any = None,
    prescan_snapshot_active: bool = False,
    published_catchup_active: bool = False,
    prescan_snapshot_done: Any = 0,
    prescan_snapshot_total: Any = 0,
    detected_albums_total: Any = 0,
    total_albums: Any = 0,
    published_catchup_done: Any = 0,
    published_catchup_total: Any = 0,
    discovery_roots_done: Any = 0,
    discovery_roots_total: Any = 0,
    discovery_started_at: Any = None,
    preplan_done: Any = 0,
    preplan_total: Any = 0,
    preplan_percent: Any = 0.0,
    now: float | None = None,
) -> dict[str, Any]:
    """Return stage counters for pre-worker scan phases.

    These phases are easy to duplicate incorrectly in the HTTP layer because
    they use different units: artists for resume-scope work, roots for raw
    filesystem discovery, and albums for candidate building.
    """

    stage_done = safe_int(progress)
    stage_total = safe_int(total)
    stage_percent = float(phase_progress or 0.0)
    stage_unit = "steps"
    effective_progress = stage_done
    next_step = current_step
    next_eta = eta_seconds
    handled = False

    if run_scope_preparing and safe_int(run_scope_total) > 0:
        stage_done = safe_int(run_scope_done)
        stage_total = safe_int(run_scope_total)
        stage_percent = float(run_scope_percent or percent(stage_done, stage_total))
        stage_unit = "artists"
        effective_progress = stage_done
        next_eta = run_scope_eta_seconds if run_scope_eta_seconds is not None else next_eta
        next_step = next_step or "restoring_resume_plan"
        handled = True
    elif pre_scan_active:
        next_step = pre_scan_current_step(
            scan_resume_run_id=scan_resume_run_id,
            scan_discovery_stage=scan_discovery_stage,
            prescan_snapshot_active=prescan_snapshot_active,
            published_catchup_active=published_catchup_active,
        )
        if prescan_snapshot_active:
            stage_done = safe_int(prescan_snapshot_done)
            stage_total = safe_int(prescan_snapshot_total) or safe_int(detected_albums_total) or safe_int(total_albums)
            stage_unit = "albums"
        elif published_catchup_active:
            stage_done = safe_int(published_catchup_done)
            stage_total = safe_int(published_catchup_total)
            stage_unit = "artists"
        elif str(scan_discovery_stage or "").strip().lower() == "filesystem":
            stage_done = safe_int(discovery_roots_done)
            stage_total = safe_int(discovery_roots_total)
            stage_unit = "roots"
            if stage_total > 0 and 0 < stage_done < stage_total and discovery_started_at:
                try:
                    elapsed = max(0.0, float((time.time() if now is None else now) - float(discovery_started_at)))
                except Exception:
                    elapsed = 0.0
                if elapsed > 0:
                    rate = float(stage_done) / elapsed
                    if rate > 0:
                        next_eta = int(max(0, stage_total - stage_done) / rate)
        else:
            stage_done = safe_int(preplan_done)
            stage_total = safe_int(preplan_total)
            stage_percent = float(preplan_percent or 0.0)
            stage_unit = "albums"
        effective_progress = stage_done
        handled = True

    stage_done = max(0, int(stage_done or 0))
    stage_total = max(0, int(stage_total or 0))
    if stage_total > 0:
        stage_done = min(stage_done, stage_total)
        if not (pre_scan_active and not prescan_snapshot_active and not published_catchup_active and str(scan_discovery_stage or "").strip().lower() != "filesystem"):
            stage_percent = percent(stage_done, stage_total)
    else:
        stage_percent = 0.0 if handled else float(stage_percent or 0.0)

    return {
        "handled": handled,
        "progress": stage_done,
        "total": stage_total,
        "effective_progress": effective_progress,
        "phase_progress": stage_percent,
        "stage_progress_done": stage_done,
        "stage_progress_total": stage_total,
        "stage_progress_percent": stage_percent,
        "stage_progress_unit": stage_unit,
        "current_step": next_step,
        "eta_seconds": next_eta,
    }


def active_artists_snapshot(raw_active_artists: dict[str, Any] | None) -> dict[str, Any]:
    """Normalize active artist worker state into API-ready counters."""
    items: list[dict[str, Any]] = []
    started_count = 0
    album_progress = 0
    current_step: str | None = None
    for name, info in dict(raw_active_artists or {}).items():
        if str(name).startswith("_") or not isinstance(info, dict):
            continue
        total_artist_albums = safe_int(info.get("total_albums"))
        albums_done = safe_int(info.get("albums_processed"))
        raw_current_album = info.get("current_album")
        current_album = raw_current_album if isinstance(raw_current_album, dict) else None
        if current_step is None and current_album:
            step_value = str(current_album.get("status") or "").strip()
            if step_value and step_value != "done":
                current_step = step_value
        if albums_done > 0 or current_album:
            started_count += 1
        if total_artist_albums > 0:
            album_progress += max(0, min(albums_done, total_artist_albums))
        else:
            album_progress += max(0, albums_done)
        items.append(
            {
                "artist_name": str(name or ""),
                "total_albums": total_artist_albums,
                "albums_processed": albums_done,
                "current_album": current_album,
            }
        )
    return {
        "items": items,
        "started_count": started_count,
        "album_progress": album_progress,
        "current_step": current_step,
    }


def hot_phase(
    *,
    scan_starting: bool = False,
    scan_run_scope_preparing: bool = False,
    scan_discovery_running: bool = False,
    scan_prescan_cache_snapshot_running: bool = False,
    scan_published_catchup_running: bool = False,
    scan_incomplete_move_running: bool = False,
    deduping: bool = False,
    scan_finalizing: bool = False,
    export_running: bool = False,
    scan_profile_enrich_running: bool = False,
    scan_post_processing: bool = False,
    scan_mb_done_count: Any = 0,
    scan_format_done_count: Any = 0,
    current_step: str | None = None,
    scan_resume_run_id: Any = None,
    scan_discovery_stage: Any = None,
) -> tuple[str, str | None]:
    """Choose the coarse scan phase for the hot `/api/progress` path."""
    phase = "pre_scan"
    next_step = current_step
    if scan_starting:
        return phase, next_step
    if scan_run_scope_preparing:
        return "preparing_run_scope", next_step or "restoring_resume_plan"
    if scan_discovery_running or scan_prescan_cache_snapshot_running or scan_published_catchup_running:
        return (
            "pre_scan",
            pre_scan_current_step(
                scan_resume_run_id=scan_resume_run_id,
                scan_discovery_stage=scan_discovery_stage,
                prescan_snapshot_active=scan_prescan_cache_snapshot_running,
                published_catchup_active=scan_published_catchup_running,
            ),
        )
    if scan_incomplete_move_running:
        return "incomplete_move", next_step
    if deduping:
        return "moving_dupes", next_step
    if scan_finalizing:
        return "finalizing", next_step
    if export_running:
        return "export", next_step
    if scan_profile_enrich_running:
        return "profile_enrichment", next_step
    if scan_post_processing:
        return "post_processing", next_step
    if safe_int(scan_mb_done_count) > 0 or next_step in {"fetching_mb_id", "searching_mb"}:
        return "identification_tags", next_step
    if safe_int(scan_format_done_count) > 0 or next_step == "analyzing_format":
        return "format_analysis", next_step
    return phase, next_step


def phase_labels(
    phase: str,
    current_step: str | None,
    *,
    scan_finalizing_label: str | None = None,
    scan_finalizing_item_label: str | None = None,
) -> tuple[str, str]:
    """Return stable user-facing labels for the current scan phase."""

    phase_key = str(phase or "").strip()
    step_key = str(current_step or "").strip()
    if phase_key == "pre_scan":
        pipeline_label = "Preparing the scan"
        stage_by_step = {
            "starting_scan": "Initializing scan runtime",
            "snapshotting_prescan_cache": "Snapshotting resume cache",
            "rehydrating_library_index": "Rehydrating library index",
            "discovering_filesystem": "Discovering monitored folders",
            "building_album_candidates": "Building album candidates",
            "restoring_resume_plan": "Restoring resume plan",
        }
        return pipeline_label, stage_by_step.get(step_key, "Preparing scan scope")
    if phase_key == "preparing_run_scope":
        return "Preparing effective scope", "Calculating run scope"
    if phase_key == "format_analysis":
        return "Format analysis", "Reading formats and audio traits"
    if phase_key == "identification_tags":
        return "Matching albums and verifying tags", "Querying MusicBrainz and providers"
    if phase_key == "ia_analysis":
        return "AI analysis", "Resolving ambiguous candidates"
    if phase_key == "incomplete_move":
        return "Quarantining incompletes", "Moving incomplete albums"
    if phase_key == "moving_dupes":
        return "Moving duplicate losers", "Moving duplicate losers"
    if phase_key == "export":
        return "Building your clean library", "Materializing promoted albums"
    if phase_key in {"post_processing", "profile_enrichment", "background_enrichment"}:
        return "Applying final metadata and artwork", "Applying final metadata and artwork"
    if phase_key == "finalizing":
        stage_label = str(scan_finalizing_label or "").strip() or "Writing final summaries"
        item_label = str(scan_finalizing_item_label or "").strip()
        if item_label:
            stage_label = f"{stage_label}: {item_label}"
        return "Finalizing scan results", stage_label
    return "Scanning library", "Working"


def progress_mode(phase: str) -> str:
    """Return the frontend display mode for a scan phase."""

    phase_key = str(phase or "").strip()
    if phase_key in {"pre_scan", "preparing_run_scope"}:
        return "preparing"
    if phase_key in {"finalizing", "post_processing", "profile_enrichment", "background_enrichment"}:
        return "finalizing"
    return "stage_active"


def eta_confidence(
    *,
    scanning: bool,
    eta_seconds: Any,
    phase: str,
    elapsed_seconds: Any,
    stage_progress_done: Any,
) -> str:
    """Return a conservative confidence level for ETA display."""

    phase_key = str(phase or "").strip()
    if not scanning or eta_seconds is None:
        return "low"
    if phase_key in {"pre_scan", "preparing_run_scope"}:
        return "low"
    if safe_int(elapsed_seconds) < 15 * 60 or safe_int(stage_progress_done) < 100:
        return "medium"
    return "high"


def runtime_seconds_effective(elapsed_seconds_value: Any, last_scan_summary: dict[str, Any] | None) -> int | None:
    """Return live elapsed seconds, falling back to the last settled scan."""

    if elapsed_seconds_value is not None:
        try:
            return int(elapsed_seconds_value)
        except Exception:
            return None
    if not isinstance(last_scan_summary, dict):
        return None
    try:
        return int(last_scan_summary.get("duration_seconds") or 0)
    except Exception:
        return None


def library_ready(
    *,
    scanning: bool,
    visible_published_albums_count: Any = 0,
    visible_published_artists_count: Any = 0,
    scan_published_albums_count: Any = 0,
    has_completed_full_scan: bool = False,
    last_scan_summary: dict[str, Any] | None = None,
    cached_library_ready: bool = False,
) -> bool:
    """Return whether the UI can treat the published library as usable."""

    if scanning:
        return False
    if safe_int(visible_published_albums_count) > 0 or safe_int(visible_published_artists_count) > 0:
        return True
    if cached_library_ready:
        return True
    if not has_completed_full_scan:
        return False
    if safe_int(scan_published_albums_count) > 0:
        return True
    if isinstance(last_scan_summary, dict) and safe_int(last_scan_summary.get("albums_scanned")) > 0:
        return True
    return False


def worker_stage_progress(
    *,
    phase: str | None,
    default_done: Any = 0,
    default_total: Any = 0,
    default_unit: str = "steps",
    scan_incomplete_move_done: Any = 0,
    scan_incomplete_move_total: Any = 0,
    format_done_count: Any = 0,
    mb_done_count: Any = 0,
    scan_processed_albums_count: Any = 0,
    active_album_progress: Any = 0,
    total_albums: Any = 0,
    detected_albums_total: Any = 0,
    scan_ai_batch_processed: Any = 0,
    scan_ai_batch_total: Any = 0,
    dedupe_progress: Any = 0,
    dedupe_total: Any = 0,
    export_albums_done: Any = 0,
    export_albums_total: Any = 0,
    scan_post_done: Any = 0,
    scan_post_total: Any = 0,
    scan_profile_enrich_running: bool = False,
    scan_profile_enrich_done: Any = 0,
    scan_profile_enrich_total: Any = 0,
    profile_backfill_state: dict[str, Any] | None = None,
    scan_finalizing_item_done: Any = 0,
    scan_finalizing_item_total: Any = 0,
    scan_finalizing_done: Any = 0,
    scan_finalizing_total: Any = 0,
) -> dict[str, Any]:
    """Return stage counters for the worker/post-worker scan phase."""

    phase_key = str(phase or "").strip()
    done = safe_int(default_done)
    total = safe_int(default_total)
    unit = str(default_unit or "steps")
    handled = True

    if phase_key == "incomplete_move":
        done = safe_int(scan_incomplete_move_done)
        total = safe_int(scan_incomplete_move_total)
        unit = "albums"
    elif phase_key == "format_analysis":
        done = max(safe_int(format_done_count), safe_int(scan_processed_albums_count) + safe_int(active_album_progress))
        total = safe_int(total_albums) or safe_int(detected_albums_total)
        unit = "albums"
    elif phase_key == "identification_tags":
        done = max(safe_int(mb_done_count), safe_int(scan_processed_albums_count) + safe_int(active_album_progress))
        total = safe_int(total_albums) or safe_int(detected_albums_total)
        unit = "albums"
    elif phase_key == "ia_analysis":
        done = safe_int(scan_ai_batch_processed)
        total = safe_int(scan_ai_batch_total)
        unit = "groups"
    elif phase_key == "moving_dupes":
        done = safe_int(dedupe_progress)
        total = safe_int(dedupe_total)
        unit = "groups"
    elif phase_key == "export":
        done = safe_int(export_albums_done)
        total = safe_int(export_albums_total)
        unit = "albums"
    elif phase_key in {"post_processing", "profile_enrichment"}:
        if scan_profile_enrich_running and safe_int(scan_profile_enrich_total) > 0:
            done = safe_int(scan_profile_enrich_done)
            total = safe_int(scan_profile_enrich_total)
            unit = "artists"
        else:
            done = safe_int(scan_post_done)
            total = safe_int(scan_post_total)
            unit = "albums"
    elif phase_key == "background_enrichment":
        backfill = profile_backfill_state or {}
        if backfill.get("running") and safe_int(backfill.get("total")) > 0:
            done = safe_int(backfill.get("current"))
            total = safe_int(backfill.get("total"))
            unit = "artists"
        else:
            done = 0
            total = 0
            unit = "jobs"
    elif phase_key == "finalizing":
        if safe_int(scan_finalizing_item_total) > 0:
            done = safe_int(scan_finalizing_item_done)
            total = safe_int(scan_finalizing_item_total)
            unit = "items"
        else:
            done = safe_int(scan_finalizing_done)
            total = safe_int(scan_finalizing_total)
            unit = "tasks"
    else:
        handled = False

    done = max(0, int(done or 0))
    total = max(0, int(total or 0))
    if total > 0:
        done = min(done, total)
    return {
        "handled": handled,
        "stage_progress_done": done,
        "stage_progress_total": total,
        "stage_progress_unit": unit,
    }


def effective_inflight_progress(
    *,
    scanning: bool,
    run_scope_preparing: bool,
    pre_scan_active: bool,
    phase: str | None,
    scan_processed_albums_count: Any,
    artists_processed: Any,
    active_album_progress: Any = 0,
    active_artists_started: Any = 0,
    stage_progress_total: Any = 0,
    total_albums: Any = 0,
    detected_albums_total: Any = 0,
    artists_total: Any = 0,
    count_active_during_prework: bool = False,
) -> dict[str, int]:
    """Return effective artist/album counters including in-flight workers."""

    album_effective = safe_int(scan_processed_albums_count)
    artist_effective = safe_int(artists_processed)
    phase_key = str(phase or "").strip()
    artist_total = safe_int(artists_total)
    album_total = safe_int(total_albums)

    if scanning and not run_scope_preparing and not pre_scan_active:
        if phase_key in {"format_analysis", "identification_tags", "ia_analysis", "export", "post_processing"}:
            album_scope_total = safe_int(stage_progress_total) or album_total or safe_int(detected_albums_total)
            in_flight_album_progress = safe_int(scan_processed_albums_count) + safe_int(active_album_progress)
            if album_scope_total > 0:
                in_flight_album_progress = max(0, min(in_flight_album_progress, album_scope_total))
            album_effective = max(album_effective, in_flight_album_progress)
        if phase_key in {"format_analysis", "identification_tags", "ia_analysis"}:
            in_flight_artist_progress = safe_int(artists_processed) + safe_int(active_artists_started)
            if artist_total > 0:
                in_flight_artist_progress = max(0, min(in_flight_artist_progress, artist_total))
            artist_effective = max(artist_effective, in_flight_artist_progress)
    elif scanning and count_active_during_prework and artist_total > 0:
        artist_effective = max(artist_effective, safe_int(artists_processed) + safe_int(active_artists_started))
        if album_total > 0:
            album_effective = max(album_effective, safe_int(scan_processed_albums_count) + safe_int(active_album_progress))

    return {
        "scan_processed_albums_effective": int(album_effective),
        "artists_processed_effective": int(artist_effective),
    }


def stage_rate_eta(
    *,
    scanning: bool,
    elapsed_seconds_value: Any,
    stage_progress_done: Any,
    stage_progress_total: Any,
    phase: str | None = None,
    current_phase_rate: Any = None,
    current_eta_seconds: Any = None,
    update_eta_for_all_phases: bool = True,
    replace_outlier_eta_for_album_phases: bool = False,
) -> dict[str, Any]:
    """Return a stage throughput and ETA update without touching runtime state."""

    phase_key = str(phase or "").strip()
    elapsed = safe_int(elapsed_seconds_value)
    done = safe_int(stage_progress_done)
    total = safe_int(stage_progress_total)
    phase_rate = current_phase_rate
    eta_seconds = current_eta_seconds
    if not (scanning and elapsed > 0 and total > 0 and done > 0):
        return {"phase_rate": phase_rate, "eta_seconds": eta_seconds}

    try:
        stage_rate_per_sec = float(done) / float(elapsed)
    except Exception:
        stage_rate_per_sec = 0.0
    if stage_rate_per_sec <= 0:
        return {"phase_rate": phase_rate, "eta_seconds": eta_seconds}

    try:
        current_rate = float(phase_rate or 0.0)
    except Exception:
        current_rate = 0.0
    if current_rate <= 0:
        phase_rate = stage_rate_per_sec
    elif phase_key in {"format_analysis", "identification_tags"}:
        phase_rate = max(current_rate, stage_rate_per_sec)

    remaining = max(0, total - done)
    stage_eta_seconds = int(remaining / stage_rate_per_sec) if remaining > 0 else 0
    album_phase = phase_key in {"format_analysis", "identification_tags"}
    should_update_eta = bool(update_eta_for_all_phases or album_phase)
    if should_update_eta:
        replace_existing = eta_seconds is None
        if replace_outlier_eta_for_album_phases and album_phase:
            try:
                current_eta = int(eta_seconds or 0)
            except Exception:
                current_eta = 0
            replace_existing = (
                replace_existing
                or current_eta <= 0
                or (stage_eta_seconds > 0 and current_eta > max(3600, stage_eta_seconds * 5))
            )
        if replace_existing:
            eta_seconds = stage_eta_seconds

    return {"phase_rate": phase_rate, "eta_seconds": eta_seconds}


def overall_progress(
    *,
    scanning: bool,
    run_scope_preparing: bool,
    pre_scan_active: bool,
    progress: Any = 0,
    total: Any = 0,
    phase_progress: Any = 0.0,
    scan_profile_enrich_running: bool = False,
    scan_profile_enrich_done: Any = 0,
    scan_profile_enrich_total: Any = 0,
    scan_post_processing: bool = False,
    improve_all_running: bool = False,
    scan_post_done: Any = 0,
    scan_post_total: Any = 0,
    artists_processed: Any = 0,
    artists_total: Any = 0,
    artists_processed_effective: Any = 0,
) -> dict[str, Any]:
    """Return global progress counters for the scan header."""

    done = safe_int(progress)
    total_n = safe_int(total)
    percent_value = float(phase_progress or 0.0)
    artist_total = safe_int(artists_total)
    artist_done = safe_int(artists_processed)
    post_total = safe_int(scan_post_total)
    post_done = safe_int(scan_post_done)

    if scanning and not run_scope_preparing and not pre_scan_active:
        if scan_profile_enrich_running and safe_int(scan_profile_enrich_total) > 0:
            done = safe_int(scan_profile_enrich_done)
            total_n = safe_int(scan_profile_enrich_total)
        elif (scan_post_processing or improve_all_running or post_total > 0) and (artist_total > 0 or post_total > 0):
            done = int(max(0, min(artist_done, artist_total)) + max(0, min(post_done, post_total)))
            total_n = int(max(0, artist_total) + max(0, post_total))
        elif artist_total > 0:
            done = int(max(0, min(artist_done, artist_total)))
            total_n = int(artist_total)
        elif total_n > 0:
            done = safe_int(progress)
        percent_value = percent(done, total_n) if total_n > 0 else 0.0
    elif scanning and artist_total > 0:
        done = max(safe_int(artists_processed_effective), done)
        total_n = max(artist_total, total_n)
        percent_value = percent(done, total_n) if total_n > 0 else 0.0

    return {
        "overall_progress_done": int(done),
        "overall_progress_total": int(total_n),
        "overall_progress_percent": float(percent_value or 0.0),
    }


def refine_post_work_phase(
    phase: str,
    *,
    scanning: bool,
    scan_starting: bool,
    run_scope_preparing: bool,
    pre_scan_active: bool,
    scan_discovery_running: bool,
    scan_incomplete_move_running: bool,
    export_running: bool,
    deduping: bool,
    background_enrichment_running: bool,
    scan_processed_albums_count: Any,
    total_albums: Any,
    artists_processed: Any,
    artists_total: Any,
    require_positive_album_total_for_background: bool = False,
    require_positive_totals_for_finalizing: bool = False,
) -> str:
    """Switch to background/finalizing phases after measurable primary work ends."""

    idle_primary_work = bool(
        scanning
        and not scan_starting
        and not run_scope_preparing
        and not pre_scan_active
        and not scan_discovery_running
        and not scan_incomplete_move_running
        and not export_running
        and not deduping
    )
    if not idle_primary_work:
        return phase

    processed = safe_int(scan_processed_albums_count)
    album_total = safe_int(total_albums)
    artist_done = safe_int(artists_processed)
    artist_total = safe_int(artists_total)

    album_complete_for_background = processed >= album_total
    if require_positive_album_total_for_background:
        album_complete_for_background = album_complete_for_background and album_total > 0
    if background_enrichment_running and album_complete_for_background:
        return "background_enrichment"

    album_complete_for_finalizing = processed >= album_total
    artist_complete_for_finalizing = artist_done >= artist_total
    if require_positive_totals_for_finalizing:
        album_complete_for_finalizing = album_complete_for_finalizing and album_total > 0
        artist_complete_for_finalizing = artist_complete_for_finalizing and artist_total > 0
    if album_complete_for_finalizing and artist_complete_for_finalizing:
        return "finalizing"
    return phase


def run_scope_payload_state(
    *,
    phase: str | None,
    run_scope_preparing: bool,
    run_scope_stage: Any = "idle",
    run_scope_done: Any = 0,
    run_scope_total: Any = 0,
    run_scope_percent: Any = 0.0,
    run_scope_eta_seconds: Any = None,
    run_scope_stalled: bool = False,
) -> dict[str, Any]:
    """Return run-scope fields ready for the progress payload.

    The UI should only see run-scope counters while the current phase is
    actually preparing a scope. Stale counters from a previous phase otherwise
    make the header look stuck.
    """

    active = bool(str(phase or "").strip() == "preparing_run_scope" or run_scope_preparing)
    if not active:
        return {
            "active": False,
            "run_scope_stage": "idle",
            "run_scope_done": 0,
            "run_scope_total": 0,
            "run_scope_percent": 0.0,
            "run_scope_eta_seconds": None,
            "run_scope_stalled": False,
        }
    return {
        "active": True,
        "run_scope_stage": str(run_scope_stage or "idle"),
        "run_scope_done": safe_int(run_scope_done),
        "run_scope_total": safe_int(run_scope_total),
        "run_scope_percent": float(run_scope_percent or 0.0),
        "run_scope_eta_seconds": run_scope_eta_seconds,
        "run_scope_stalled": bool(run_scope_stalled),
    }


def post_processing_payload_state(
    *,
    phase: str | None,
    scan_post_processing: bool,
    scan_post_done: Any = 0,
    scan_post_total: Any = 0,
    scan_post_current_artist: Any = None,
    scan_post_current_album: Any = None,
    scanning: bool = False,
    run_scope_preparing: bool = False,
    pre_scan_active: bool = False,
    artists_processed: Any = 0,
    artists_total: Any = 0,
    overall_progress_done: Any = 0,
    overall_progress_total: Any = 0,
    overall_progress_percent: Any = 0.0,
) -> dict[str, Any]:
    """Return post-processing fields ready for the progress payload.

    Post-processing counters are reset outside post/enrichment phases. When a
    scan is still active, global progress falls back to artist progress so the
    frontend does not keep showing stale post-processing totals.
    """

    post_done = safe_int(scan_post_done)
    post_total = safe_int(scan_post_total)
    phase_key = str(phase or "").strip()
    active = bool(
        phase_key in {"post_processing", "profile_enrichment", "background_enrichment"}
        or (scan_post_processing and post_total > 0 and post_done < post_total)
    )
    payload = {
        "active": active,
        "scan_post_processing": bool(scan_post_processing),
        "scan_post_done": post_done,
        "scan_post_total": post_total,
        "scan_post_current_artist": scan_post_current_artist,
        "scan_post_current_album": scan_post_current_album,
        "overall_progress_done": safe_int(overall_progress_done),
        "overall_progress_total": safe_int(overall_progress_total),
        "overall_progress_percent": float(overall_progress_percent or 0.0),
    }
    if active:
        return payload

    payload.update(
        {
            "scan_post_processing": False,
            "scan_post_done": 0,
            "scan_post_total": 0,
            "scan_post_current_artist": None,
            "scan_post_current_album": None,
        }
    )
    artist_total = safe_int(artists_total)
    if scanning and not run_scope_preparing and not pre_scan_active and artist_total > 0:
        artist_done = int(max(0, min(safe_int(artists_processed), artist_total)))
        payload.update(
            {
                "overall_progress_done": artist_done,
                "overall_progress_total": artist_total,
                "overall_progress_percent": percent(artist_done, artist_total),
            }
        )
    return payload


def fallback_phase(
    *,
    scanning: bool,
    scan_starting: bool = False,
    run_scope_preparing: bool = False,
    pre_scan_active: bool = False,
    scan_incomplete_move_running: bool = False,
    deduping: bool = False,
    finalizing: bool = False,
    export_running: bool = False,
    scan_profile_enrich_running: bool = False,
    scan_post_processing: bool = False,
    current_step: str | None = None,
    raw_active_artists: dict[str, Any] | None = None,
    scan_resume_run_id: Any = None,
    scan_discovery_stage: Any = None,
    prescan_snapshot_active: bool = False,
    published_catchup_active: bool = False,
) -> dict[str, Any]:
    """Choose the phase for the lock-contention progress fallback path."""

    active_artists = dict(raw_active_artists or {})
    primary_worker_phase = None
    if current_step in ("comparing_versions", "detecting_best") or "_ai_batch" in active_artists:
        primary_worker_phase = "ia_analysis"
    elif current_step in ("fetching_mb_id", "searching_mb"):
        primary_worker_phase = "identification_tags"
    elif current_step == "analyzing_format":
        primary_worker_phase = "format_analysis"

    next_step = current_step
    clear_active_artists = False
    if not scanning:
        phase = None
        next_step = None
        clear_active_artists = True
    elif scan_starting:
        phase = "pre_scan"
        next_step = "starting_scan"
    elif run_scope_preparing:
        phase = "preparing_run_scope"
    elif pre_scan_active:
        phase = "pre_scan"
        next_step = pre_scan_current_step(
            scan_resume_run_id=scan_resume_run_id,
            scan_discovery_stage=scan_discovery_stage,
            prescan_snapshot_active=prescan_snapshot_active,
            published_catchup_active=published_catchup_active,
        )
    elif scan_incomplete_move_running:
        phase = "incomplete_move"
    elif deduping:
        phase = "moving_dupes"
    elif finalizing:
        phase = "finalizing"
    elif primary_worker_phase:
        phase = primary_worker_phase
    elif export_running:
        phase = "export"
    elif scan_profile_enrich_running:
        phase = "profile_enrichment"
    elif scan_post_processing and next_step in (None, "", "done") and "_ai_batch" not in active_artists:
        phase = "post_processing"
    else:
        phase = "format_analysis"

    return {
        "phase": phase,
        "current_step": next_step,
        "clear_active_artists": clear_active_artists,
    }


def scanning_hot_payload(
    *,
    runtime_state: dict[str, Any],
    cached_payload: dict[str, Any] | None = None,
    scan_paused: bool = False,
    scan_threads: int = 0,
    scan_progress_log_tail: int = 64,
    provider_matches_so_far: dict[str, int] | None = None,
    mcp_status: dict[str, Any] | None = None,
    storage_progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the fast progress payload used while a scan is running."""

    payload = dict(cached_payload) if isinstance(cached_payload, dict) else {}
    state = runtime_state
    scan_starting = bool(state.get("scan_starting"))
    scanning = bool(state.get("scanning") or scan_starting or state.get("scan_finalizing"))
    status = "paused" if (scanning and scan_paused) else ("running" if scanning else "stopped")
    scan_type = str(state.get("scan_type") or payload.get("scan_type") or "full")
    scan_id_current = safe_int(state.get("scan_id") or payload.get("scan_id")) or None
    scan_resume_run_id = state.get("scan_resume_run_id") or state.get("scan_resume_requested_run_id") or payload.get("scan_resume_run_id")

    elapsed = elapsed_seconds(scanning, state.get("scan_start_time"))
    progress = safe_int(state.get("scan_step_progress"))
    total = safe_int(state.get("scan_step_total") or state.get("scan_total"))
    total_albums = safe_int(
        state.get("scan_total_albums")
        or state.get("scan_detected_albums_total")
        or payload.get("total_albums")
    )
    artists_total = safe_int(
        state.get("scan_artists_total")
        or state.get("scan_detected_artists_total")
        or payload.get("artists_total")
    )
    artists_processed = safe_int(state.get("scan_artists_processed"))
    scan_processed_albums_count = safe_int(state.get("scan_processed_albums_count"))

    scan_finalizing_done = safe_int(state.get("scan_finalizing_done"))
    scan_finalizing_total = safe_int(state.get("scan_finalizing_total"))
    scan_finalizing_item_done = safe_int(state.get("scan_finalizing_item_done"))
    scan_finalizing_item_total = safe_int(state.get("scan_finalizing_item_total"))

    active_snapshot = active_artists_snapshot(dict(state.get("scan_active_artists") or {}))
    active_artists_list: list[dict[str, Any]] = list(active_snapshot.get("items") or [])
    active_artists_started = safe_int(active_snapshot.get("started_count"))
    active_album_progress = safe_int(active_snapshot.get("album_progress"))
    current_step = active_snapshot.get("current_step")

    active_artists_count = int(len(active_artists_list))
    artists_processed_effective = int(max(artists_processed, active_artists_started))
    scan_processed_albums_effective = int(max(scan_processed_albums_count, active_album_progress))

    phase_rate, eta_seconds = rate_eta(progress, total, elapsed)
    stage_progress_done = int(progress or 0)
    stage_progress_total = int(total or 0)
    stage_progress_unit = "steps"
    stage_progress_percent = percent(stage_progress_done, stage_progress_total)
    overall_progress_done = int(scan_processed_albums_effective or stage_progress_done or 0)
    overall_progress_total = int(total_albums or stage_progress_total or 0)
    overall_progress_percent = percent(overall_progress_done, overall_progress_total) if overall_progress_total > 0 else stage_progress_percent

    phase, current_step = hot_phase(
        scan_starting=scan_starting,
        scan_run_scope_preparing=bool(state.get("scan_run_scope_preparing")),
        scan_discovery_running=bool(state.get("scan_discovery_running")),
        scan_prescan_cache_snapshot_running=bool(state.get("scan_prescan_cache_snapshot_running")),
        scan_published_catchup_running=bool(state.get("scan_published_catchup_running")),
        scan_incomplete_move_running=bool(state.get("scan_incomplete_move_running")),
        deduping=bool(state.get("deduping")),
        scan_finalizing=bool(state.get("scan_finalizing")),
        export_running=bool((state.get("export_progress") or {}).get("running")),
        scan_profile_enrich_running=bool(state.get("scan_profile_enrich_running")),
        scan_post_processing=bool(state.get("scan_post_processing")),
        scan_mb_done_count=state.get("scan_mb_done_count"),
        scan_format_done_count=state.get("scan_format_done_count"),
        current_step=current_step,
        scan_resume_run_id=scan_resume_run_id,
        scan_discovery_stage=state.get("scan_discovery_stage"),
    )

    stage_progress_unit = "albums" if phase in {"format_analysis", "identification_tags", "ia_analysis"} else "steps"
    if phase == "finalizing":
        if scan_finalizing_item_total > 0:
            stage_progress_done = max(0, scan_finalizing_item_done)
            stage_progress_total = max(0, scan_finalizing_item_total)
            stage_progress_unit = "items"
        else:
            stage_progress_done = max(0, scan_finalizing_done)
            stage_progress_total = max(0, scan_finalizing_total)
            stage_progress_unit = "tasks"
    stage_progress_percent = percent(stage_progress_done, stage_progress_total)
    overall_progress_done = int(scan_processed_albums_effective or stage_progress_done or 0)
    overall_progress_total = int(total_albums or stage_progress_total or 0)
    overall_progress_percent = percent(overall_progress_done, overall_progress_total) if overall_progress_total > 0 else stage_progress_percent

    raw_scan_steps_log = list(state.get("scan_steps_log") or [])
    scan_steps_log_total = int(len(raw_scan_steps_log))
    scan_steps_log = (
        raw_scan_steps_log[-scan_progress_log_tail:]
        if scan_steps_log_total > scan_progress_log_tail
        else raw_scan_steps_log
    )

    provider_matches = dict(provider_matches_so_far or {})
    matches_so_far = int(sum(safe_int(value) for value in provider_matches.values()))
    scan_published_albums_count = safe_int(state.get("scan_published_albums_count"))
    visible_artists_count = safe_int(payload.get("library_visible_artists_count"))

    payload.update(
        {
            "scanning": scanning,
            "scan_starting": scan_starting,
            "status": status,
            "scan_id": scan_id_current,
            "scan_type": scan_type,
            "scan_resume_run_id": scan_resume_run_id,
            "phase": phase,
            "current_step": current_step,
            "progress": stage_progress_done,
            "total": stage_progress_total,
            "effective_progress": stage_progress_done,
            "stage_progress_done": stage_progress_done,
            "stage_progress_total": stage_progress_total,
            "stage_progress_percent": stage_progress_percent,
            "stage_progress_unit": stage_progress_unit,
            "overall_progress_done": overall_progress_done,
            "overall_progress_total": overall_progress_total,
            "overall_progress_percent": overall_progress_percent,
            "phase_progress": stage_progress_percent,
            "phase_rate": phase_rate,
            "eta_seconds": eta_seconds,
            "elapsed_seconds": elapsed,
            "scan_runtime_sec": elapsed,
            "threads_in_use": int(scan_threads or 0),
            "active_artists": active_artists_list,
            "active_artists_count": active_artists_count,
            "artists_processed": artists_processed,
            "artists_processed_effective": artists_processed_effective,
            "artists_total": artists_total,
            "total_albums": total_albums,
            "scan_processed_albums_count": scan_processed_albums_count,
            "scan_processed_albums_effective": scan_processed_albums_effective,
            "mb_done_count": safe_int(state.get("scan_mb_done_count")),
            "format_done_count": safe_int(state.get("scan_format_done_count")),
            "matches_so_far": matches_so_far,
            "provider_matches_so_far": provider_matches,
            "scan_provider_stats_live": {},
            "scan_steps_log": scan_steps_log,
            "scan_steps_log_total": scan_steps_log_total,
            "scan_published_albums_count": scan_published_albums_count,
            "library_visible_albums_count": int(max(scan_published_albums_count, safe_int(payload.get("library_visible_albums_count")))),
            "library_visible_artists_count": visible_artists_count,
            "library_visible_tracks_count": safe_int(payload.get("library_visible_tracks_count")),
            "library_visible_fallback_source": payload.get("library_visible_fallback_source"),
            "background_enrichment_running": bool(state.get("scan_profile_enrich_running") or state.get("scan_post_processing")),
            "scan_profile_enrich_running": bool(state.get("scan_profile_enrich_running")),
            "post_processing": bool(state.get("scan_post_processing")),
            "scan_incomplete_moved_count": safe_int(state.get("scan_incomplete_moved_count")),
            "duplicate_losers_so_far": safe_int(state.get("scan_total_duplicates_count")),
            "incomplete_albums_so_far": safe_int(state.get("scan_broken_albums_count")),
            "exports_so_far": int(scan_published_albums_count),
            "bootstrap_required": False,
            "autonomous_mode": False,
            "has_completed_full_scan": True,
            "default_scan_type": "full",
            "last_scan_summary": None,
            "mcp": dict(mcp_status or {}),
            "paths_status": payload.get("paths_status") or {},
            "stale": False,
            "lock_contention": False,
        }
    )
    payload.update(dict(storage_progress or {}))
    return payload
