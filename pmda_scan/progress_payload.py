"""Runtime-backed scan progress payload builders.

This module is an intermediate extraction from the historical PMDA monolith.
It deliberately accepts the live PMDA runtime module at the public boundary so
large progress payload construction can move out of ``pmda.py`` before the
state object is fully decomposed.
"""

from __future__ import annotations

import logging
import time
from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind the current PMDA runtime module for this request."""
    global _RUNTIME
    _RUNTIME = runtime
    # Do not import the pmda.py wrapper back into this module, otherwise the
    # extracted payload builder recursively calls itself through the wrapper.
    globals().update({key: value for key, value in vars(runtime).items() if key != "api_progress"})


def _runtime_module() -> Any:
    if _RUNTIME is None:
        raise RuntimeError("PMDA progress runtime is not bound")
    return _RUNTIME


def api_progress_for_runtime(runtime: Any):
    """Build the Flask response for /api/progress using the live runtime."""
    _bind_runtime(runtime)
    return api_progress()




def _api_progress_cache_state_key() -> tuple[Any, ...]:
    return _scan_progress_core.cache_state_key(state)


_SCAN_PROGRESS_LOG_TAIL = 64

def _api_progress_scanning_hot_payload(
    *,
    cached_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    provider_matches_so_far = _normalize_scan_provider_matches(
        dict(state.get("scan_provider_matches") or {}),
        legacy_discogs=int(state.get("scan_discogs_matched") or 0),
        legacy_lastfm=int(state.get("scan_lastfm_matched") or 0),
        legacy_bandcamp=int(state.get("scan_bandcamp_matched") or 0),
    )
    return _scan_progress_core.scanning_hot_payload(
        runtime_state=state,
        cached_payload=cached_payload,
        scan_paused=bool(scan_is_paused.is_set()),
        scan_threads=int(SCAN_THREADS or 0),
        scan_progress_log_tail=_SCAN_PROGRESS_LOG_TAIL,
        provider_matches_so_far=provider_matches_so_far,
        mcp_status=_mcp_status_summary(include_audit=False),
        storage_progress=_storage_progress_payload(),
    )


def _api_progress_lock_fallback_payload(
    *,
    bootstrap_status: dict[str, Any],
    default_scan_type: str,
    cached_payload: dict[str, Any] | None = None,
    mark_stale: bool = True,
    mark_lock_contention: bool = True,
    allow_db_helpers: bool = True,
) -> dict[str, Any]:
    """
    Fast scan-progress payload that only relies on in-memory state plus a few
    best-effort non-blocking snapshots. This keeps `/api/progress` responsive
    even while SQLite is hot during a running scan.
    """
    payload: dict[str, Any] = {}
    scanning = bool(state.get("scanning") or state.get("scan_starting") or state.get("scan_finalizing"))
    scan_starting = bool(state.get("scan_starting"))
    status = "paused" if (scanning and scan_is_paused.is_set()) else ("running" if scanning else "stopped")
    scan_type = str(state.get("scan_type") or payload.get("scan_type") or "full")
    scan_id_current = int(state.get("scan_id") or payload.get("scan_id") or 0) or None
    scan_resume_run_id = (
        state.get("scan_resume_run_id")
        or state.get("scan_resume_requested_run_id")
        or payload.get("scan_resume_run_id")
    )

    scan_start_time = state.get("scan_start_time")
    elapsed_seconds: int | None = None
    if scanning and scan_start_time:
        try:
            elapsed_seconds = int(max(0.0, time.time() - float(scan_start_time)))
        except Exception:
            elapsed_seconds = None

    progress = int(state.get("scan_step_progress") or payload.get("progress") or 0)
    total = int(state.get("scan_step_total") or payload.get("total") or 0)
    effective_progress = int(state.get("scan_progress") or payload.get("effective_progress") or 0)
    threads_in_use = SCAN_THREADS

    raw_active_artists = dict(state.get("scan_active_artists") or {})
    active_snapshot = _scan_progress_core.active_artists_snapshot(raw_active_artists)
    active_artists_list = list(active_snapshot.get("items") or [])
    active_artists_started = int(active_snapshot.get("started_count") or 0)
    active_album_progress = int(active_snapshot.get("album_progress") or 0)
    current_step = active_snapshot.get("current_step")

    artists_processed = int(state.get("scan_artists_processed") or payload.get("artists_processed") or 0)
    artists_total = int(state.get("scan_artists_total") or payload.get("artists_total") or 0)
    detected_artists_total = int(state.get("scan_detected_artists_total") or payload.get("detected_artists_total") or 0)
    detected_albums_total = int(state.get("scan_detected_albums_total") or payload.get("detected_albums_total") or 0)
    resume_skipped_artists = int(state.get("scan_resume_skipped_artists") or payload.get("resume_skipped_artists") or 0)
    resume_skipped_albums = int(state.get("scan_resume_skipped_albums") or payload.get("resume_skipped_albums") or 0)
    total_albums = int(
        state.get("scan_total_albums")
        or detected_albums_total
        or payload.get("total_albums")
        or 0
    )
    scan_processed_albums_count = int(state.get("scan_processed_albums_count") or payload.get("scan_processed_albums_count") or 0)
    scan_finalizing_stage = str(state.get("scan_finalizing_stage") or payload.get("scan_finalizing_stage") or "")
    scan_finalizing_label = str(state.get("scan_finalizing_label") or payload.get("scan_finalizing_label") or "")
    scan_finalizing_done = int(state.get("scan_finalizing_done") or payload.get("scan_finalizing_done") or 0)
    scan_finalizing_total = int(state.get("scan_finalizing_total") or payload.get("scan_finalizing_total") or 0)
    scan_finalizing_item_done = int(state.get("scan_finalizing_item_done") or payload.get("scan_finalizing_item_done") or 0)
    scan_finalizing_item_total = int(state.get("scan_finalizing_item_total") or payload.get("scan_finalizing_item_total") or 0)
    scan_finalizing_item_label = str(state.get("scan_finalizing_item_label") or payload.get("scan_finalizing_item_label") or "")
    scan_finalizing_updated_at = state.get("scan_finalizing_updated_at") or payload.get("scan_finalizing_updated_at")
    scan_processed_albums_effective = int(scan_processed_albums_count or 0)
    artists_processed_effective = int(artists_processed or 0)

    run_scope_preparing = bool(state.get("scan_run_scope_preparing"))
    run_scope_stage = str(state.get("scan_run_scope_stage") or "idle")
    run_scope_done = int(state.get("scan_run_scope_done") or 0)
    run_scope_total = int(state.get("scan_run_scope_total") or 0)
    run_scope_artists_included = int(state.get("scan_run_scope_artists_included") or 0)
    run_scope_albums_included = int(state.get("scan_run_scope_albums_included") or 0)
    run_scope_started_at = state.get("scan_run_scope_started_at")
    run_scope_updated_at = state.get("scan_run_scope_updated_at")
    run_scope_snapshot = _scan_progress_core.count_progress_snapshot(
        done=run_scope_done,
        total=run_scope_total,
        running=run_scope_preparing,
        started_at=run_scope_started_at,
        updated_at=run_scope_updated_at,
        stall_after_seconds=30.0,
    )
    run_scope_done = int(run_scope_snapshot.get("done") or 0)
    run_scope_total = int(run_scope_snapshot.get("total") or 0)
    run_scope_percent = float(run_scope_snapshot.get("percent") or 0.0)
    run_scope_eta_seconds = run_scope_snapshot.get("eta_seconds")
    run_scope_stalled = bool(run_scope_snapshot.get("stalled"))

    scan_prescan_cache_snapshot_running = bool(state.get("scan_prescan_cache_snapshot_running"))
    scan_prescan_cache_snapshot_done = bool(state.get("scan_prescan_cache_snapshot_done"))
    scan_prescan_cache_snapshot_rows = int(state.get("scan_prescan_cache_snapshot_rows") or 0)
    scan_prescan_cache_snapshot_total = int(state.get("scan_prescan_cache_snapshot_total") or 0)
    scan_prescan_cache_snapshot_updated_at = state.get("scan_prescan_cache_snapshot_updated_at")

    scan_published_catchup_running = bool(state.get("scan_published_catchup_running"))
    scan_published_catchup_reason = str(state.get("scan_published_catchup_reason") or "")
    scan_published_catchup_done = int(state.get("scan_published_catchup_done") or 0)
    scan_published_catchup_total = int(state.get("scan_published_catchup_total") or 0)
    scan_published_catchup_ok = int(state.get("scan_published_catchup_ok") or 0)
    scan_published_catchup_failed = int(state.get("scan_published_catchup_failed") or 0)
    scan_published_catchup_current_artist = state.get("scan_published_catchup_current_artist")
    scan_published_catchup_started_at = state.get("scan_published_catchup_started_at")
    scan_published_catchup_updated_at = state.get("scan_published_catchup_updated_at")
    scan_published_catchup_finished_at = state.get("scan_published_catchup_finished_at")

    scan_discovery_running = bool(state.get("scan_discovery_running"))
    scan_discovery_current_root = state.get("scan_discovery_current_root")
    scan_discovery_roots_done = int(state.get("scan_discovery_roots_done") or 0)
    scan_discovery_roots_total = int(state.get("scan_discovery_roots_total") or 0)
    scan_discovery_files_found = int(state.get("scan_discovery_files_found") or 0)
    scan_discovery_folders_found = int(state.get("scan_discovery_folders_found") or 0)
    scan_discovery_albums_found = int(state.get("scan_discovery_albums_found") or 0)
    scan_discovery_artists_found = int(state.get("scan_discovery_artists_found") or 0)
    scan_discovery_stage = str(state.get("scan_discovery_stage") or "")
    scan_discovery_entries_scanned = int(state.get("scan_discovery_entries_scanned") or 0)
    scan_discovery_root_entries_scanned = int(state.get("scan_discovery_root_entries_scanned") or 0)
    scan_discovery_folders_done = int(state.get("scan_discovery_folders_done") or 0)
    scan_discovery_folders_total = int(state.get("scan_discovery_folders_total") or 0)
    scan_discovery_albums_done = int(state.get("scan_discovery_albums_done") or 0)
    scan_discovery_albums_total = int(state.get("scan_discovery_albums_total") or 0)
    scan_discovery_started_at = state.get("scan_discovery_started_at")
    scan_discovery_updated_at = state.get("scan_discovery_updated_at")
    preplan_total = int(scan_discovery_albums_total or scan_discovery_folders_total or 0)
    preplan_done = int(scan_discovery_albums_done or scan_discovery_folders_done or 0)
    if preplan_total > 0:
        preplan_done = max(0, min(preplan_done, preplan_total))
    preplan_percent = round((float(preplan_done) / float(preplan_total)) * 100.0, 2) if preplan_total > 0 else 0.0
    preplan_label = f"{preplan_done}/{preplan_total}" if preplan_total > 0 else f"{preplan_done}/?"

    files_cache_quality_recalc_running = bool(state.get("files_cache_quality_recalc_running"))
    files_cache_quality_recalc_total = int(state.get("files_cache_quality_recalc_total") or 0)
    files_cache_quality_recalc_done = int(state.get("files_cache_quality_recalc_done") or 0)
    files_cache_quality_recalc_rows_upserted = int(state.get("files_cache_quality_recalc_rows_upserted") or 0)
    files_cache_quality_recalc_errors = int(state.get("files_cache_quality_recalc_errors") or 0)
    files_cache_quality_recalc_missing_folders = int(state.get("files_cache_quality_recalc_missing_folders") or 0)
    files_cache_quality_recalc_no_audio = int(state.get("files_cache_quality_recalc_no_audio") or 0)
    files_cache_quality_recalc_reason = str(state.get("files_cache_quality_recalc_reason") or "")
    files_cache_quality_recalc_started_at = state.get("files_cache_quality_recalc_started_at")
    files_cache_quality_recalc_updated_at = state.get("files_cache_quality_recalc_updated_at")
    files_cache_quality_recalc_finished_at = state.get("files_cache_quality_recalc_finished_at")

    ai_used_count = int(state.get("scan_ai_used_count") or 0)
    mb_used_count = int(state.get("scan_mb_used_count") or 0)
    ai_enabled = bool(state.get("scan_ai_enabled"))
    scan_ai_guard_calls_used = int(state.get("scan_ai_guard_calls_used") or 0)
    scan_ai_guard_calls_blocked = int(state.get("scan_ai_guard_calls_blocked") or 0)
    scan_ai_guard_last_reason = str(state.get("scan_ai_guard_last_reason") or "")
    scan_ai_guard_last_block_at = state.get("scan_ai_guard_last_block_at")

    scan_profile_enrich_running = bool(state.get("scan_profile_enrich_running"))
    scan_profile_enrich_total = int(state.get("scan_profile_enrich_total") or 0)
    scan_profile_enrich_done = int(state.get("scan_profile_enrich_done") or 0)
    scan_profile_enrich_current_artist = state.get("scan_profile_enrich_current_artist")
    scan_profile_enrich_started_at = state.get("scan_profile_enrich_started_at")
    scan_profile_enrich_updated_at = state.get("scan_profile_enrich_updated_at")
    enrich_snapshot = _scan_progress_core.count_progress_snapshot(
        done=scan_profile_enrich_done,
        total=scan_profile_enrich_total,
        running=scan_profile_enrich_running,
        started_at=scan_profile_enrich_started_at,
    )
    scan_profile_enrich_done = int(enrich_snapshot.get("done") or 0)
    scan_profile_enrich_total = int(enrich_snapshot.get("total") or 0)
    scan_profile_enrich_percent = float(enrich_snapshot.get("percent") or 0.0)
    scan_profile_enrich_eta_seconds = enrich_snapshot.get("eta_seconds")

    improve_all_state = dict(state.get("improve_all") or {})
    improve_all_running = bool(improve_all_state.get("running"))
    scan_post_processing = bool(state.get("scan_post_processing") or improve_all_running)
    scan_post_total = int(state.get("scan_post_total") or 0)
    scan_post_done = int(state.get("scan_post_done") or 0)
    scan_post_current_artist = state.get("scan_post_current_artist")
    scan_post_current_album = state.get("scan_post_current_album")
    if improve_all_running:
        scan_post_total = max(scan_post_total, int(improve_all_state.get("total") or 0))
        scan_post_done = max(scan_post_done, int(improve_all_state.get("current") or 0))
        if not scan_post_current_artist:
            scan_post_current_artist = improve_all_state.get("current_artist")
        if not scan_post_current_album:
            scan_post_current_album = improve_all_state.get("current_album")

    finalizing = bool(state.get("scan_finalizing"))
    deduping = bool(state.get("deduping"))
    dedupe_progress = int(state.get("dedupe_progress") or 0)
    dedupe_total = int(state.get("dedupe_total") or 0)
    dedupe_current_group = state.get("dedupe_current_group")
    scan_ai_batch_total = int(state.get("scan_ai_batch_total") or 0)
    scan_ai_batch_processed = int(state.get("scan_ai_batch_processed") or 0)
    scan_ai_current_label = state.get("scan_ai_current_label")

    scan_incomplete_moved_count = int(state.get("scan_incomplete_moved_count") or 0)
    scan_incomplete_moved_mb = int(state.get("scan_incomplete_moved_mb") or 0)
    scan_incomplete_move_running = bool(state.get("scan_incomplete_move_running"))
    scan_incomplete_move_done = int(state.get("scan_incomplete_move_done") or 0)
    scan_incomplete_move_total = int(state.get("scan_incomplete_move_total") or 0)
    scan_incomplete_move_current_album = state.get("scan_incomplete_move_current_album")

    export_progress = dict(state.get("export_progress") or {})
    export_running = bool(export_progress.get("running"))
    export_albums_done = int(export_progress.get("albums_done") or 0)
    export_albums_total = int(export_progress.get("total_albums") or 0)
    export_tracks_done = int(export_progress.get("tracks_done") or 0)
    export_tracks_total = int(export_progress.get("total_tracks") or 0)

    scan_tracks_detected_total = int(state.get("scan_tracks_detected_total") or 0)
    scan_tracks_library_kept = int(state.get("scan_tracks_library_kept") or 0)
    scan_tracks_moved_dupes = int(state.get("scan_tracks_moved_dupes") or 0)
    scan_tracks_moved_incomplete = int(state.get("scan_tracks_moved_incomplete") or 0)
    scan_tracks_unaccounted = int(state.get("scan_tracks_unaccounted") or 0)
    scan_dupe_moved_count = int(state.get("scan_dupe_moved_count") or state.get("last_dedupe_moved_count") or 0)
    scan_dupe_moved_mb = int(state.get("scan_dupe_moved_mb") or state.get("last_dedupe_saved_mb") or state.get("dedupe_saved_this_run") or 0)
    scan_published_albums_count = int(state.get("scan_published_albums_count") or 0)
    scan_postprocessed_albums_count = int(state.get("scan_postprocessed_albums_count") or 0)
    scan_player_sync_target = state.get("scan_player_sync_target")
    scan_player_sync_ok = state.get("scan_player_sync_ok")
    scan_player_sync_message = state.get("scan_player_sync_message")
    scan_pipeline_flags = dict(state.get("scan_pipeline_flags") or {})
    scan_pipeline_async = bool(state.get("scan_pipeline_async"))
    scan_pipeline_sync_target = state.get("scan_pipeline_sync_target")
    auto_move_enabled = bool(
        (scan_pipeline_flags or {}).get(
            "dedupe",
            getattr(_runtime_module(), "AUTO_MOVE_DUPES", False),
        )
    )

    mb_enabled = bool(state.get("scan_mb_enabled"))
    audio_cache_hits = int(state.get("scan_audio_cache_hits") or 0)
    audio_cache_misses = int(state.get("scan_audio_cache_misses") or 0)
    mb_cache_hits = int(state.get("scan_mb_cache_hits") or 0)
    mb_cache_misses = int(state.get("scan_mb_cache_misses") or 0)
    duplicate_groups_count = int(state.get("scan_duplicate_groups_count") or 0)
    total_duplicates_count = int(state.get("scan_total_duplicates_count") or 0)
    broken_albums_count = int(state.get("scan_broken_albums_count") or 0)
    missing_albums_count = int(state.get("scan_missing_albums_count") or 0)
    albums_without_artist_image = int(state.get("scan_albums_without_artist_image") or 0)
    albums_without_album_image = int(state.get("scan_albums_without_album_image") or 0)
    albums_without_complete_tags = int(state.get("scan_albums_without_complete_tags") or 0)
    albums_without_mb_id = int(state.get("scan_albums_without_mb_id") or 0)
    albums_without_artist_mb_id = int(state.get("scan_albums_without_artist_mb_id") or 0)
    format_done_count = int(state.get("scan_format_done_count") or 0)
    mb_done_count = int(state.get("scan_mb_done_count") or 0)
    scan_discogs_matched = int(state.get("scan_discogs_matched") or 0)
    scan_lastfm_matched = int(state.get("scan_lastfm_matched") or 0)
    scan_bandcamp_matched = int(state.get("scan_bandcamp_matched") or 0)
    scan_provider_matches_raw = dict(state.get("scan_provider_matches") or {})
    raw_scan_steps_log = list(state.get("scan_steps_log") or [])
    scan_steps_log_total = int(len(raw_scan_steps_log))
    if scan_steps_log_total > _SCAN_PROGRESS_LOG_TAIL:
        scan_steps_log = raw_scan_steps_log[-_SCAN_PROGRESS_LOG_TAIL:]
    else:
        scan_steps_log = raw_scan_steps_log
    files_watcher_state = dict(state.get("files_watcher") or {})

    prescan_snapshot_active = bool(
        scanning
        and scan_prescan_cache_snapshot_running
        and int(scan_prescan_cache_snapshot_total or detected_albums_total or total_albums or 0) > 0
    )
    published_catchup_active = bool(
        scanning
        and scan_published_catchup_running
        and int(scan_published_catchup_total or 0) > 0
    )
    pre_scan_active = bool(
        scanning
        and not run_scope_preparing
        and (
            published_catchup_active
            or prescan_snapshot_active
            or scan_discovery_running
            or preplan_total > 0
        )
        and (
            not scan_incomplete_move_running
            and not deduping
            and not export_running
            and not finalizing
            and not scan_profile_enrich_running
            and not scan_post_processing
        )
    )

    phase_payload = _scan_progress_core.fallback_phase(
        scanning=bool(scanning), scan_starting=bool(scan_starting), run_scope_preparing=bool(run_scope_preparing),
        pre_scan_active=bool(pre_scan_active), scan_incomplete_move_running=bool(scan_incomplete_move_running),
        deduping=bool(deduping), finalizing=bool(finalizing), export_running=bool(export_running),
        scan_profile_enrich_running=bool(scan_profile_enrich_running), scan_post_processing=bool(scan_post_processing),
        current_step=current_step, raw_active_artists=raw_active_artists,
        scan_resume_run_id=scan_resume_run_id, scan_discovery_stage=scan_discovery_stage,
        prescan_snapshot_active=bool(prescan_snapshot_active), published_catchup_active=bool(published_catchup_active),
    )
    phase = phase_payload.get("phase")
    current_step = phase_payload.get("current_step")
    if phase_payload.get("clear_active_artists"):
        active_artists_list = []

    include_unmatched_default = False
    if _get_library_mode() == "files":
        try:
            include_unmatched_default = _library_include_unmatched_effective()
        except Exception:
            include_unmatched_default = bool(getattr(_runtime_module(), "LIBRARY_INCLUDE_UNMATCHED", True))
    fallback_visibility_payload = _publication_snapshot.progress_library_visibility(
        files_mode=_get_library_mode() == "files",
        include_unmatched_default=bool(include_unmatched_default),
        scanning=bool(scanning),
        cached_payload=cached_payload if isinstance(cached_payload, dict) else None,
        scan_processed_albums_count=int(scan_processed_albums_count or 0),
        total_albums=int(total_albums or 0),
        scan_published_albums_count=int(scan_published_albums_count or 0),
        browse_counts=lambda include_unmatched: _files_library_browse_counts(
            include_unmatched,
            scope="library",
            acquire_timeout_sec=0.05,
        ) if allow_db_helpers else (None, None),
        effective_browse_snapshot=lambda include_unmatched: {},
    )
    browse_visible_albums_count = fallback_visibility_payload.get("albums_count")
    browse_visible_artists_count = fallback_visibility_payload.get("artists_count")
    browse_visible_tracks_count = fallback_visibility_payload.get("tracks_count")
    browse_visible_fallback_source = fallback_visibility_payload.get("fallback_source")
    if browse_visible_albums_count is None:
        browse_visible_albums_count = int(scan_published_albums_count or 0)
    if browse_visible_artists_count is None:
        browse_visible_artists_count = int((cached_payload or {}).get("library_visible_artists_count") or 0)
    if browse_visible_tracks_count is None:
        browse_visible_tracks_count = int((cached_payload or {}).get("library_visible_tracks_count") or 0)

    background_jobs = list((cached_payload or {}).get("background_jobs") or [])
    if _lock_try_acquire_nonblocking(_scheduler_lock):
        try:
            background_jobs = _pipeline_jobs_core.running_scheduler_jobs(_scheduler_running_meta)
        except Exception:
            background_jobs = list((cached_payload or {}).get("background_jobs") or [])
        finally:
            try:
                _scheduler_lock.release()
            except Exception:
                pass
    profile_backfill_state = dict((cached_payload or {}).get("profile_backfill") or {})
    if _lock_try_acquire_nonblocking(_files_profile_backfill_lock):
        try:
            profile_backfill_state = dict(_files_profile_backfill_state or {})
        except Exception:
            profile_backfill_state = dict((cached_payload or {}).get("profile_backfill") or {})
        finally:
            try:
                _files_profile_backfill_lock.release()
            except Exception:
                pass
    try:
        profile_jobs_active = int(len(_files_profile_jobs_active)) if _lock_try_acquire_nonblocking(_files_profile_jobs_lock) else 0
    except Exception:
        profile_jobs_active = 0
    finally:
        try:
            _files_profile_jobs_lock.release()
        except Exception:
            pass
    background_enrichment_running = _pipeline_jobs_core.background_enrichment_running(
        background_jobs=background_jobs,
        profile_backfill_state=profile_backfill_state,
        profile_jobs_active=profile_jobs_active,
    )

    phase = _scan_progress_core.refine_post_work_phase(
        phase,
        scanning=bool(scanning),
        scan_starting=bool(scan_starting),
        run_scope_preparing=bool(run_scope_preparing),
        pre_scan_active=bool(pre_scan_active),
        scan_discovery_running=bool(scan_discovery_running),
        scan_incomplete_move_running=bool(scan_incomplete_move_running),
        export_running=bool(export_running),
        deduping=bool(deduping),
        background_enrichment_running=bool(background_enrichment_running),
        scan_processed_albums_count=scan_processed_albums_count,
        total_albums=total_albums,
        artists_processed=artists_processed,
        artists_total=artists_total,
        require_positive_album_total_for_background=True,
        require_positive_totals_for_finalizing=True,
    )

    pre_work_stage = _scan_progress_core.pre_work_stage_progress(
        progress=progress,
        total=total,
        phase_progress=0.0,
        current_step=current_step,
        eta_seconds=None,
        run_scope_preparing=run_scope_preparing,
        run_scope_done=run_scope_done,
        run_scope_total=run_scope_total,
        run_scope_percent=run_scope_percent,
        run_scope_eta_seconds=run_scope_eta_seconds,
        pre_scan_active=pre_scan_active,
        scan_resume_run_id=scan_resume_run_id,
        scan_discovery_stage=scan_discovery_stage,
        prescan_snapshot_active=prescan_snapshot_active,
        published_catchup_active=published_catchup_active,
        prescan_snapshot_done=scan_prescan_cache_snapshot_rows,
        prescan_snapshot_total=scan_prescan_cache_snapshot_total,
        detected_albums_total=detected_albums_total,
        total_albums=total_albums,
        published_catchup_done=scan_published_catchup_done,
        published_catchup_total=scan_published_catchup_total,
        discovery_roots_done=scan_discovery_roots_done,
        discovery_roots_total=scan_discovery_roots_total,
        discovery_started_at=scan_discovery_started_at,
        preplan_done=preplan_done,
        preplan_total=preplan_total,
        preplan_percent=preplan_percent,
    )
    stage_progress_done = int(pre_work_stage.get("stage_progress_done") or 0)
    stage_progress_total = int(pre_work_stage.get("stage_progress_total") or 0)
    stage_progress_unit = str(pre_work_stage.get("stage_progress_unit") or "steps")
    if pre_work_stage.get("handled"):
        effective_progress = int(pre_work_stage.get("effective_progress") or 0)
        phase_progress = float(pre_work_stage.get("phase_progress") or 0.0)
        eta_seconds = pre_work_stage.get("eta_seconds")
        if pre_work_stage.get("current_step"):
            current_step = str(pre_work_stage.get("current_step") or "")
    else:
        worker_stage = _scan_progress_core.worker_stage_progress(
            phase=phase,
            default_done=stage_progress_done,
            default_total=stage_progress_total,
            default_unit=stage_progress_unit,
            scan_incomplete_move_done=scan_incomplete_move_done,
            scan_incomplete_move_total=scan_incomplete_move_total,
            format_done_count=format_done_count,
            mb_done_count=mb_done_count,
            scan_processed_albums_count=scan_processed_albums_count,
            active_album_progress=active_album_progress,
            total_albums=total_albums,
            detected_albums_total=detected_albums_total,
            scan_ai_batch_processed=scan_ai_batch_processed,
            scan_ai_batch_total=scan_ai_batch_total,
            dedupe_progress=dedupe_progress,
            dedupe_total=dedupe_total,
            export_albums_done=export_albums_done,
            export_albums_total=export_albums_total,
            scan_post_done=scan_post_done,
            scan_post_total=scan_post_total,
            scan_profile_enrich_running=scan_profile_enrich_running,
            scan_profile_enrich_done=scan_profile_enrich_done,
            scan_profile_enrich_total=scan_profile_enrich_total,
            profile_backfill_state=profile_backfill_state,
            scan_finalizing_item_done=scan_finalizing_item_done,
            scan_finalizing_item_total=scan_finalizing_item_total,
            scan_finalizing_done=scan_finalizing_done,
            scan_finalizing_total=scan_finalizing_total,
        )
        if worker_stage.get("handled"):
            stage_progress_done = int(worker_stage.get("stage_progress_done") or 0)
            stage_progress_total = int(worker_stage.get("stage_progress_total") or 0)
            stage_progress_unit = str(worker_stage.get("stage_progress_unit") or stage_progress_unit)

    effective_inflight = _scan_progress_core.effective_inflight_progress(
        scanning=bool(scanning),
        run_scope_preparing=bool(run_scope_preparing),
        pre_scan_active=bool(pre_scan_active),
        phase=phase,
        scan_processed_albums_count=scan_processed_albums_count,
        artists_processed=artists_processed,
        active_album_progress=active_album_progress,
        active_artists_started=active_artists_started,
        stage_progress_total=stage_progress_total,
        total_albums=total_albums,
        detected_albums_total=detected_albums_total,
        artists_total=artists_total,
        count_active_during_prework=True,
    )
    scan_processed_albums_effective = max(
        scan_processed_albums_effective,
        int(effective_inflight.get("scan_processed_albums_effective") or 0),
    )
    artists_processed_effective = max(
        artists_processed_effective,
        int(effective_inflight.get("artists_processed_effective") or 0),
    )

    stage_progress_done = max(0, int(stage_progress_done or 0))
    stage_progress_total = max(0, int(stage_progress_total or 0))
    if stage_progress_total > 0:
        stage_progress_done = min(stage_progress_done, stage_progress_total)
    stage_progress_percent = (
        round((float(stage_progress_done) / float(stage_progress_total)) * 100.0, 2)
        if stage_progress_total > 0
        else 0.0
    )
    phase_progress = float(stage_progress_percent or 0.0)

    eta_seconds = pre_work_stage.get("eta_seconds") if pre_work_stage.get("handled") else None
    phase_rate = None
    stage_rate_update = _scan_progress_core.stage_rate_eta(
        scanning=bool(scanning),
        elapsed_seconds_value=elapsed_seconds,
        stage_progress_done=stage_progress_done,
        stage_progress_total=stage_progress_total,
        phase=phase,
        current_phase_rate=phase_rate,
        current_eta_seconds=eta_seconds,
    )
    phase_rate = stage_rate_update.get("phase_rate")
    eta_seconds = stage_rate_update.get("eta_seconds")
    if run_scope_preparing and run_scope_eta_seconds is not None:
        eta_seconds = run_scope_eta_seconds
    if scan_profile_enrich_running and scan_profile_enrich_eta_seconds is not None:
        eta_seconds = scan_profile_enrich_eta_seconds

    overall = _scan_progress_core.overall_progress(
        scanning=bool(scanning),
        run_scope_preparing=bool(run_scope_preparing),
        pre_scan_active=bool(pre_scan_active),
        progress=progress,
        total=total,
        phase_progress=phase_progress,
        scan_profile_enrich_running=scan_profile_enrich_running,
        scan_profile_enrich_done=scan_profile_enrich_done,
        scan_profile_enrich_total=scan_profile_enrich_total,
        scan_post_processing=scan_post_processing,
        improve_all_running=improve_all_running,
        scan_post_done=scan_post_done,
        scan_post_total=scan_post_total,
        artists_processed=artists_processed,
        artists_total=artists_total,
        artists_processed_effective=artists_processed_effective,
    )
    overall_progress_done = int(overall.get("overall_progress_done") or 0)
    overall_progress_total = int(overall.get("overall_progress_total") or 0)
    overall_progress_percent = float(overall.get("overall_progress_percent") or 0.0)

    pipeline_step_human_label, current_stage_human_label = _scan_progress_core.phase_labels(
        phase,
        current_step,
        scan_finalizing_label=scan_finalizing_label,
        scan_finalizing_item_label=scan_finalizing_item_label,
    )
    scan_progress_mode = _scan_progress_core.progress_mode(phase)
    scan_eta_confidence = _scan_progress_core.eta_confidence(
        scanning=scanning,
        eta_seconds=eta_seconds,
        phase=phase,
        elapsed_seconds=elapsed_seconds,
        stage_progress_done=stage_progress_done,
    )

    provider_matches_so_far = _normalize_scan_provider_matches(
        scan_provider_matches_raw,
        legacy_discogs=scan_discogs_matched,
        legacy_lastfm=scan_lastfm_matched,
        legacy_bandcamp=scan_bandcamp_matched,
    )
    matches_so_far = int(sum(int(v or 0) for v in provider_matches_so_far.values()))
    provider_live_stats = _progress_runtime_core.provider_gateway_live_stats(
        scanning=bool(scanning),
        cached_payload=cached_payload,
        snapshot_loader=lambda fallback: _provider_gateway_stats_snapshot_best_effort(fallback),
    )
    scan_provider_stats_live = dict(provider_live_stats.get("providers") or {})
    gateway_stats_live = dict(provider_live_stats.get("gateway") or {})

    visible_published_albums_count = int(browse_visible_albums_count or 0)
    exports_so_far = int(max(int(scan_published_albums_count or 0), int(export_albums_done or 0), visible_published_albums_count))
    incomplete_albums_so_far = int(broken_albums_count or 0)
    duplicate_losers_so_far = int(total_duplicates_count or 0)
    active_artists_count = int(len(active_artists_list))
    library_ready = _scan_progress_core.library_ready(
        scanning=bool(scanning),
        visible_published_albums_count=visible_published_albums_count,
        visible_published_artists_count=browse_visible_artists_count,
        cached_library_ready=bool(payload.get("library_ready")),
    )

    resume_snapshot = _progress_runtime_core.resume_availability_snapshot(
        scanning=bool(scanning),
        library_mode_loader=_get_library_mode,
        get_resume_run_snapshot=_get_resume_run_snapshot,
        get_latest_resume_run_snapshot_any_signature=_get_latest_resume_run_snapshot_any_signature,
        existing_available=bool(payload.get("resume_available") or False),
        existing_by_scan_type=payload.get("resume_available_by_scan_type") or {},
    )
    resume_available = bool(resume_snapshot.get("resume_available"))
    resume_available_by_scan_type = dict(resume_snapshot.get("resume_available_by_scan_type") or {})

    autonomous_mode_effective = bool(bootstrap_status.get("autonomous_mode")) and _auto_changed_only_mode_effective()

    payload.update(
        {
            "scan_id": scan_id_current,
            "scanning": scanning,
            "scan_starting": scan_starting,
            "progress": progress,
            "total": total,
            "effective_progress": effective_progress,
            "status": status,
            "stale": bool(mark_stale),
            "lock_contention": bool(mark_lock_contention),
            "library_ready": library_ready,
            "background_enrichment_running": background_enrichment_running,
            "background_jobs": background_jobs,
            "profile_backfill": profile_backfill_state,
            "resume_available": resume_available,
            "resume_available_by_scan_type": resume_available_by_scan_type,
            "phase": phase,
            "current_step": current_step,
            "scan_progress_mode": scan_progress_mode,
            "scan_eta_confidence": scan_eta_confidence,
            "pipeline_step_human_label": pipeline_step_human_label,
            "current_stage_human_label": current_stage_human_label,
            "ai_provider": str(payload.get("ai_provider") or (AI_PROVIDER or "")),
            "ai_model": str(payload.get("ai_model") or _ai_model_display_name(AI_PROVIDER or "")),
            "artists_processed": artists_processed,
            "artists_total": artists_total,
            "detected_artists_total": detected_artists_total,
            "detected_albums_total": detected_albums_total,
            "resume_skipped_artists": resume_skipped_artists,
            "resume_skipped_albums": resume_skipped_albums,
            "scan_run_scope_preparing": run_scope_preparing,
            "scan_run_scope_stage": run_scope_stage,
            "scan_run_scope_done": run_scope_done,
            "scan_run_scope_total": run_scope_total,
            "scan_run_scope_percent": run_scope_percent,
            "scan_run_scope_eta_seconds": run_scope_eta_seconds,
            "scan_run_scope_stalled": run_scope_stalled,
            "scan_run_scope_artists_included": run_scope_artists_included,
            "scan_run_scope_albums_included": run_scope_albums_included,
            "scan_prescan_cache_snapshot_running": scan_prescan_cache_snapshot_running,
            "scan_prescan_cache_snapshot_done": scan_prescan_cache_snapshot_done,
            "scan_prescan_cache_snapshot_rows": scan_prescan_cache_snapshot_rows,
            "scan_prescan_cache_snapshot_total": scan_prescan_cache_snapshot_total,
            "scan_prescan_cache_snapshot_updated_at": scan_prescan_cache_snapshot_updated_at,
            "scan_published_catchup_running": scan_published_catchup_running,
            "scan_published_catchup_reason": scan_published_catchup_reason,
            "scan_published_catchup_done": scan_published_catchup_done,
            "scan_published_catchup_total": scan_published_catchup_total,
            "scan_published_catchup_ok": scan_published_catchup_ok,
            "scan_published_catchup_failed": scan_published_catchup_failed,
            "scan_published_catchup_current_artist": scan_published_catchup_current_artist,
            "scan_published_catchup_started_at": scan_published_catchup_started_at,
            "scan_published_catchup_updated_at": scan_published_catchup_updated_at,
            "scan_published_catchup_finished_at": scan_published_catchup_finished_at,
            "files_cache_quality_recalc_running": files_cache_quality_recalc_running,
            "files_cache_quality_recalc_total": files_cache_quality_recalc_total,
            "files_cache_quality_recalc_done": files_cache_quality_recalc_done,
            "files_cache_quality_recalc_rows_upserted": files_cache_quality_recalc_rows_upserted,
            "files_cache_quality_recalc_errors": files_cache_quality_recalc_errors,
            "files_cache_quality_recalc_missing_folders": files_cache_quality_recalc_missing_folders,
            "files_cache_quality_recalc_no_audio": files_cache_quality_recalc_no_audio,
            "files_cache_quality_recalc_reason": files_cache_quality_recalc_reason,
            "files_cache_quality_recalc_started_at": files_cache_quality_recalc_started_at,
            "files_cache_quality_recalc_updated_at": files_cache_quality_recalc_updated_at,
            "files_cache_quality_recalc_finished_at": files_cache_quality_recalc_finished_at,
            "ai_used_count": ai_used_count,
            "ai_tokens_total": int(payload.get("ai_tokens_total") or 0),
            "ai_cost_usd_total": float(payload.get("ai_cost_usd_total") or 0.0),
            "ai_unpriced_calls": int(payload.get("ai_unpriced_calls") or 0),
            "mb_used_count": mb_used_count,
            "ai_enabled": ai_enabled,
            "scan_ai_guard_calls_used": scan_ai_guard_calls_used,
            "scan_ai_guard_calls_blocked": scan_ai_guard_calls_blocked,
            "scan_ai_guard_last_reason": scan_ai_guard_last_reason,
            "scan_ai_guard_last_block_at": scan_ai_guard_last_block_at,
            "scan_profile_enrich_running": scan_profile_enrich_running,
            "scan_profile_enrich_total": scan_profile_enrich_total,
            "scan_profile_enrich_done": scan_profile_enrich_done,
            "scan_profile_enrich_percent": scan_profile_enrich_percent,
            "scan_profile_enrich_eta_seconds": scan_profile_enrich_eta_seconds,
            "scan_profile_enrich_current_artist": scan_profile_enrich_current_artist,
            "scan_profile_enrich_started_at": scan_profile_enrich_started_at,
            "scan_profile_enrich_updated_at": scan_profile_enrich_updated_at,
            "mb_enabled": mb_enabled,
            "audio_cache_hits": audio_cache_hits,
            "audio_cache_misses": audio_cache_misses,
            "mb_cache_hits": mb_cache_hits,
            "mb_cache_misses": mb_cache_misses,
            "duplicate_groups_count": duplicate_groups_count,
            "total_duplicates_count": total_duplicates_count,
            "broken_albums_count": broken_albums_count,
            "missing_albums_count": missing_albums_count,
            "albums_without_artist_image": albums_without_artist_image,
            "albums_without_album_image": albums_without_album_image,
            "albums_without_complete_tags": albums_without_complete_tags,
            "albums_without_mb_id": albums_without_mb_id,
            "albums_without_artist_mb_id": albums_without_artist_mb_id,
            "format_done_count": format_done_count,
            "mb_done_count": mb_done_count,
            "mb_retry_not_found": getattr(_runtime_module(), "MB_RETRY_NOT_FOUND", False),
            "eta_seconds": eta_seconds,
            "threads_in_use": threads_in_use,
            "active_artists": active_artists_list,
            "active_artists_count": active_artists_count,
            "last_scan_summary": None,
            "scan_steps_log": scan_steps_log,
            "scan_steps_log_total": scan_steps_log_total,
            "scan_type": scan_type,
            "scan_resume_run_id": scan_resume_run_id,
            "finalizing": finalizing,
            "scan_finalizing_stage": scan_finalizing_stage,
            "scan_finalizing_label": scan_finalizing_label,
            "scan_finalizing_done": scan_finalizing_done,
            "scan_finalizing_total": scan_finalizing_total,
            "scan_finalizing_item_done": scan_finalizing_item_done,
            "scan_finalizing_item_total": scan_finalizing_item_total,
            "scan_finalizing_item_label": scan_finalizing_item_label,
            "scan_finalizing_updated_at": scan_finalizing_updated_at,
            "deduping": deduping,
            "dedupe_progress": dedupe_progress,
            "dedupe_total": dedupe_total,
            "dedupe_current_group": dedupe_current_group,
            "auto_move_enabled": auto_move_enabled,
            "paths_status": (cached_payload or {}).get("paths_status") or {"music_rw": True, "dupes_rw": True},
            "scan_ai_batch_total": scan_ai_batch_total,
            "scan_ai_batch_processed": scan_ai_batch_processed,
            "scan_ai_current_label": scan_ai_current_label,
            "total_albums": total_albums,
            "post_processing": scan_post_processing,
            "post_processing_done": scan_post_done,
            "post_processing_total": scan_post_total,
            "post_processing_current_artist": scan_post_current_artist,
            "post_processing_current_album": scan_post_current_album,
            "scan_discovery_running": scan_discovery_running,
            "scan_discovery_current_root": scan_discovery_current_root,
            "scan_discovery_roots_done": scan_discovery_roots_done,
            "scan_discovery_roots_total": scan_discovery_roots_total,
            "scan_discovery_files_found": scan_discovery_files_found,
            "scan_discovery_folders_found": scan_discovery_folders_found,
            "scan_discovery_albums_found": scan_discovery_albums_found,
            "scan_discovery_artists_found": scan_discovery_artists_found,
            "scan_discovery_stage": scan_discovery_stage,
            "scan_discovery_entries_scanned": scan_discovery_entries_scanned,
            "scan_discovery_root_entries_scanned": scan_discovery_root_entries_scanned,
            "scan_discovery_folders_done": scan_discovery_folders_done,
            "scan_discovery_folders_total": scan_discovery_folders_total,
            "scan_discovery_albums_done": scan_discovery_albums_done,
            "scan_discovery_albums_total": scan_discovery_albums_total,
            "scan_discovery_started_at": scan_discovery_started_at,
            "scan_discovery_updated_at": scan_discovery_updated_at,
            "scan_preplan_done": preplan_done,
            "scan_preplan_total": preplan_total,
            "scan_preplan_percent": preplan_percent,
            "scan_preplan_label": preplan_label,
            "files_watcher_running": bool(files_watcher_state.get("running")),
            "files_watcher_roots": list(files_watcher_state.get("roots") or []),
            "files_watcher_dirty_count": int(files_watcher_state.get("dirty_count") or 0),
            "files_watcher_last_event_at": files_watcher_state.get("last_event_at"),
            "files_watcher_last_event_path": files_watcher_state.get("last_event_path"),
            "scan_discogs_matched": scan_discogs_matched,
            "scan_lastfm_matched": scan_lastfm_matched,
            "scan_bandcamp_matched": scan_bandcamp_matched,
            "provider_matches_so_far": provider_matches_so_far,
            "scan_provider_stats_live": scan_provider_stats_live,
            "provider_gateway_inflight": int((gateway_stats_live or {}).get("inflight") or 0) if scanning else 0,
            "provider_gateway_max_inflight_observed": int((gateway_stats_live or {}).get("max_inflight_observed") or 0) if scanning else 0,
            "matches_so_far": matches_so_far,
            "exports_so_far": exports_so_far,
            "incomplete_albums_so_far": incomplete_albums_so_far,
            "duplicate_losers_so_far": duplicate_losers_so_far,
            "scan_start_time": scan_start_time,
            "scan_pipeline_flags": scan_pipeline_flags,
            "scan_pipeline_async": scan_pipeline_async,
            "scan_pipeline_sync_target": scan_pipeline_sync_target,
            "scan_incomplete_moved_count": scan_incomplete_moved_count,
            "scan_incomplete_moved_mb": scan_incomplete_moved_mb,
            "scan_dupe_moved_count": scan_dupe_moved_count,
            "scan_dupe_moved_mb": scan_dupe_moved_mb,
            "scan_incomplete_move_running": scan_incomplete_move_running,
            "scan_incomplete_move_done": scan_incomplete_move_done,
            "scan_incomplete_move_total": scan_incomplete_move_total,
            "scan_incomplete_move_current_album": scan_incomplete_move_current_album,
            "export_progress": export_progress,
            "export_running": export_running,
            "export_albums_done": export_albums_done,
            "export_albums_total": export_albums_total,
            "export_tracks_done": export_tracks_done,
            "export_tracks_total": export_tracks_total,
            "scan_tracks_detected_total": scan_tracks_detected_total,
            "scan_tracks_library_kept": scan_tracks_library_kept,
            "scan_tracks_moved_dupes": scan_tracks_moved_dupes,
            "scan_tracks_moved_incomplete": scan_tracks_moved_incomplete,
            "scan_tracks_unaccounted": scan_tracks_unaccounted,
            "scan_processed_albums_count": scan_processed_albums_count,
            "scan_processed_albums_effective": scan_processed_albums_effective,
            "scan_published_albums_count": visible_published_albums_count,
            "scan_published_album_rows_count": scan_published_albums_count,
            "scan_postprocessed_albums_count": scan_postprocessed_albums_count,
            "artists_processed_effective": artists_processed_effective,
            "scan_player_sync_target": scan_player_sync_target,
            "scan_player_sync_ok": scan_player_sync_ok,
            "scan_player_sync_message": scan_player_sync_message,
            "bootstrap_required": bool(bootstrap_status.get("bootstrap_required")),
            "autonomous_mode": autonomous_mode_effective,
            "has_completed_full_scan": not bool(bootstrap_status.get("bootstrap_required")),
            "default_scan_type": str(default_scan_type or "full"),
            "library_visible_albums_count": browse_visible_albums_count,
            "library_visible_artists_count": browse_visible_artists_count,
            "library_visible_tracks_count": browse_visible_tracks_count,
            "library_visible_fallback_source": browse_visible_fallback_source,
            "library_include_unmatched_default": bool(include_unmatched_default),
            "elapsed_seconds": elapsed_seconds,
            "scan_runtime_sec": elapsed_seconds,
            "phase_rate": phase_rate,
            "phase_progress": phase_progress,
            "stage_progress_done": stage_progress_done,
            "stage_progress_total": stage_progress_total,
            "stage_progress_percent": stage_progress_percent,
            "stage_progress_unit": stage_progress_unit,
            "overall_progress_done": overall_progress_done,
            "overall_progress_total": overall_progress_total,
            "overall_progress_percent": overall_progress_percent,
        }
    )
    payload.update(_storage_progress_payload())
    return payload


def api_progress():
    now_ts = time.time()
    mod = _runtime_module()
    cache_obj = getattr(mod, "_API_PROGRESS_CACHE", None)
    cached_payload = None
    cache_state_key = None
    current_state_key = _api_progress_cache_state_key()
    if isinstance(cache_obj, dict):
        cached_payload = cache_obj.get("payload")
        cache_state_key = cache_obj.get("state_key")
        try:
            cached_ts = float(cache_obj.get("ts") or 0.0)
        except Exception:
            cached_ts = 0.0
        if isinstance(cached_payload, dict) and cache_state_key == current_state_key:
            age = now_ts - cached_ts
            # Coalesce bursts of concurrent polling requests from multiple UI components.
            if 0.0 <= age < 0.75:
                return jsonify(cached_payload)
            # When the shared state lock is currently contended, serve a recent stale snapshot
            # instead of piling up blocked requests in the browser.
            if _rlock_locked_compat(lock) and 0.0 <= age < 15.0:
                stale_payload = dict(cached_payload)
                stale_payload["stale"] = True
                return jsonify(stale_payload)

    scanning_hint = bool(state.get("scanning") or state.get("scan_starting") or state.get("scan_finalizing"))
    if scanning_hint:
        bootstrap_status_cache = getattr(mod, "_PIPELINE_BOOTSTRAP_STATUS_CACHE", None)
        if isinstance(bootstrap_status_cache, dict):
            bootstrap_status = dict(bootstrap_status_cache)
        else:
            bootstrap_status = {
                "bootstrap_required": False,
                "autonomous_mode": False,
                "first_full_scan_id": None,
                "first_full_completed_at": None,
                "updated_at": None,
            }
        default_scan_type = str(
            state.get("scan_type")
            or ((cached_payload or {}).get("default_scan_type") if isinstance(cached_payload, dict) else None)
            or "full"
        )
        lock_contended = bool(_rlock_locked_compat(lock))
        fast_payload = _api_progress_lock_fallback_payload(
            bootstrap_status=bootstrap_status,
            default_scan_type=default_scan_type,
            cached_payload=cached_payload if isinstance(cached_payload, dict) else None,
            mark_stale=lock_contended,
            mark_lock_contention=lock_contended,
            allow_db_helpers=False,
        )
        setattr(mod, "_API_PROGRESS_CACHE", {"ts": time.time(), "payload": fast_payload, "state_key": current_state_key})
        return jsonify(fast_payload)

    bootstrap_status = _pipeline_bootstrap_status(timeout=0.05, prefer_cached_on_failure=True)
    default_scan_type = get_default_scan_type()
    has_completed_full_scan = not bool(bootstrap_status.get("bootstrap_required"))

    pre_acquired = lock.acquire(timeout=0.05)
    if not pre_acquired:
        if isinstance(cached_payload, dict):
            stale_payload = dict(cached_payload)
            stale_payload["stale"] = True
            stale_payload["lock_contention"] = True
            return jsonify(stale_payload)
        fallback_payload = _api_progress_lock_fallback_payload(
            bootstrap_status=bootstrap_status,
            default_scan_type=default_scan_type,
            cached_payload=cached_payload if isinstance(cached_payload, dict) else None,
        )
        setattr(mod, "_API_PROGRESS_CACHE", {"ts": time.time(), "payload": fallback_payload, "state_key": current_state_key})
        return jsonify(fallback_payload)

    try:
        # Do NOT set scanning=False here when progress >= total. The scan thread still runs
        # the AI batch and finally block (save_scan_to_db, scan_history) after progress hits
        # 100%; only that thread must set scanning=False so the UI does not show "finished"
        # while the scan is still running.
        scan_starting = bool(state.get("scan_starting"))
        scanning = bool(state["scanning"] or scan_starting or state.get("scan_finalizing"))
        status = "paused" if (scanning and scan_is_paused.is_set()) else ("running" if scanning else "stopped")
        # Step-based progress for bar: progress/total = steps done / step total (3*albums+2 or +3)
        progress = state.get("scan_step_progress", 0)
        total = state.get("scan_step_total", 0) or state["scan_total"]
        format_done_count = state.get("scan_format_done_count", 0)
        mb_done_count = state.get("scan_mb_done_count", 0)
        scan_steps_log = list(state.get("scan_steps_log") or [])
        scan_steps_log_total = int(len(scan_steps_log))
        if scan_steps_log_total > _SCAN_PROGRESS_LOG_TAIL:
            scan_steps_log = scan_steps_log[-_SCAN_PROGRESS_LOG_TAIL:]
        current_scan_type = (state.get("scan_type") or "full")
        scan_resume_run_id = (
            state.get("scan_resume_run_id")
            or state.get("scan_resume_requested_run_id")
        )

        # Keep scan_progress for effective_progress / legacy; bar uses step progress
        active_artists_dict = state.get("scan_active_artists", {})
        effective_progress = state.get("scan_progress", 0)

        # ETA from step progress
        threads_in_use = SCAN_THREADS
        elapsed_seconds = _scan_progress_core.elapsed_seconds(bool(scanning), state.get("scan_start_time"))
        phase_rate, eta_seconds = _scan_progress_core.rate_eta(progress, total, elapsed_seconds)
        phase_progress = _scan_progress_core.percent(progress, total)

        active_snapshot = _scan_progress_core.active_artists_snapshot(active_artists_dict)
        active_artists_list = list(active_snapshot.get("items") or [])
        active_artists_started = int(active_snapshot.get("started_count") or 0)
        active_album_progress = int(active_snapshot.get("album_progress") or 0)
        current_step = active_snapshot.get("current_step")

        # Copy all state values we need while still in the lock
        artists_processed = state.get("scan_artists_processed", 0)
        artists_total = state.get("scan_artists_total", 0)
        detected_artists_total = state.get("scan_detected_artists_total", 0)
        detected_albums_total = state.get("scan_detected_albums_total", 0)
        resume_skipped_artists = state.get("scan_resume_skipped_artists", 0)
        resume_skipped_albums = state.get("scan_resume_skipped_albums", 0)
        run_scope_preparing = bool(state.get("scan_run_scope_preparing"))
        run_scope_stage = str(state.get("scan_run_scope_stage") or "idle")
        run_scope_done = int(state.get("scan_run_scope_done") or 0)
        run_scope_total = int(state.get("scan_run_scope_total") or 0)
        run_scope_artists_included = int(state.get("scan_run_scope_artists_included") or 0)
        run_scope_albums_included = int(state.get("scan_run_scope_albums_included") or 0)
        run_scope_started_at = state.get("scan_run_scope_started_at")
        run_scope_updated_at = state.get("scan_run_scope_updated_at")
        scan_prescan_cache_snapshot_running = bool(state.get("scan_prescan_cache_snapshot_running"))
        scan_prescan_cache_snapshot_done = bool(state.get("scan_prescan_cache_snapshot_done"))
        scan_prescan_cache_snapshot_rows = int(state.get("scan_prescan_cache_snapshot_rows") or 0)
        scan_prescan_cache_snapshot_total = int(state.get("scan_prescan_cache_snapshot_total") or 0)
        scan_prescan_cache_snapshot_updated_at = state.get("scan_prescan_cache_snapshot_updated_at")
        scan_published_catchup_running = bool(state.get("scan_published_catchup_running"))
        scan_published_catchup_reason = str(state.get("scan_published_catchup_reason") or "")
        scan_published_catchup_done = int(state.get("scan_published_catchup_done") or 0)
        scan_published_catchup_total = int(state.get("scan_published_catchup_total") or 0)
        scan_published_catchup_ok = int(state.get("scan_published_catchup_ok") or 0)
        scan_published_catchup_failed = int(state.get("scan_published_catchup_failed") or 0)
        scan_published_catchup_current_artist = state.get("scan_published_catchup_current_artist")
        scan_published_catchup_started_at = state.get("scan_published_catchup_started_at")
        scan_published_catchup_updated_at = state.get("scan_published_catchup_updated_at")
        scan_published_catchup_finished_at = state.get("scan_published_catchup_finished_at")
        files_cache_quality_recalc_running = bool(state.get("files_cache_quality_recalc_running"))
        files_cache_quality_recalc_total = int(state.get("files_cache_quality_recalc_total") or 0)
        files_cache_quality_recalc_done = int(state.get("files_cache_quality_recalc_done") or 0)
        files_cache_quality_recalc_rows_upserted = int(state.get("files_cache_quality_recalc_rows_upserted") or 0)
        files_cache_quality_recalc_errors = int(state.get("files_cache_quality_recalc_errors") or 0)
        files_cache_quality_recalc_missing_folders = int(state.get("files_cache_quality_recalc_missing_folders") or 0)
        files_cache_quality_recalc_no_audio = int(state.get("files_cache_quality_recalc_no_audio") or 0)
        files_cache_quality_recalc_reason = str(state.get("files_cache_quality_recalc_reason") or "")
        files_cache_quality_recalc_started_at = state.get("files_cache_quality_recalc_started_at")
        files_cache_quality_recalc_updated_at = state.get("files_cache_quality_recalc_updated_at")
        files_cache_quality_recalc_finished_at = state.get("files_cache_quality_recalc_finished_at")
        ai_used_count = state.get("scan_ai_used_count", 0)
        mb_used_count = state.get("scan_mb_used_count", 0)
        ai_enabled = state.get("scan_ai_enabled", False)
        scan_ai_guard_calls_used = int(state.get("scan_ai_guard_calls_used") or 0)
        scan_ai_guard_calls_blocked = int(state.get("scan_ai_guard_calls_blocked") or 0)
        scan_ai_guard_last_reason = str(state.get("scan_ai_guard_last_reason") or "")
        scan_ai_guard_last_block_at = state.get("scan_ai_guard_last_block_at")
        scan_profile_enrich_running = bool(state.get("scan_profile_enrich_running"))
        scan_profile_enrich_total = int(state.get("scan_profile_enrich_total") or 0)
        scan_profile_enrich_done = int(state.get("scan_profile_enrich_done") or 0)
        scan_profile_enrich_current_artist = state.get("scan_profile_enrich_current_artist")
        scan_profile_enrich_started_at = state.get("scan_profile_enrich_started_at")
        scan_profile_enrich_updated_at = state.get("scan_profile_enrich_updated_at")
        mb_enabled = state.get("scan_mb_enabled", False)
        audio_cache_hits = state.get("scan_audio_cache_hits", 0)
        audio_cache_misses = state.get("scan_audio_cache_misses", 0)
        mb_cache_hits = state.get("scan_mb_cache_hits", 0)
        mb_cache_misses = state.get("scan_mb_cache_misses", 0)
        duplicate_groups_count = state.get("scan_duplicate_groups_count", 0)
        total_duplicates_count = state.get("scan_total_duplicates_count", 0)
        broken_albums_count = state.get("scan_broken_albums_count", 0)
        missing_albums_count = state.get("scan_missing_albums_count", 0)
        albums_without_artist_image = state.get("scan_albums_without_artist_image", 0)
        albums_without_album_image = state.get("scan_albums_without_album_image", 0)
        albums_without_complete_tags = state.get("scan_albums_without_complete_tags", 0)
        albums_without_mb_id = state.get("scan_albums_without_mb_id", 0)
        albums_without_artist_mb_id = state.get("scan_albums_without_artist_mb_id", 0)
        format_done_count = state.get("scan_format_done_count", 0)
        mb_done_count = state.get("scan_mb_done_count", 0)
        scan_ai_batch_total = state.get("scan_ai_batch_total", 0)
        scan_ai_batch_processed = state.get("scan_ai_batch_processed", 0)
        scan_ai_current_label = state.get("scan_ai_current_label")
        last_fix_all_by_provider = state.get("last_fix_all_by_provider")
        last_fix_all_total_albums = state.get("last_fix_all_total_albums", 0)
        total_albums = state.get("scan_total_albums", 0)
        improve_all_state = state.get("improve_all") or {}
        improve_all_running = bool(improve_all_state.get("running"))
        scan_post_processing = bool(state.get("scan_post_processing") or improve_all_running)
        scan_post_total = int(state.get("scan_post_total") or 0)
        scan_post_done = int(state.get("scan_post_done") or 0)
        scan_post_current_artist = state.get("scan_post_current_artist")
        scan_post_current_album = state.get("scan_post_current_album")
        scan_discovery_running = bool(state.get("scan_discovery_running"))
        scan_discovery_current_root = state.get("scan_discovery_current_root")
        scan_discovery_roots_done = int(state.get("scan_discovery_roots_done") or 0)
        scan_discovery_roots_total = int(state.get("scan_discovery_roots_total") or 0)
        scan_discovery_files_found = int(state.get("scan_discovery_files_found") or 0)
        scan_discovery_folders_found = int(state.get("scan_discovery_folders_found") or 0)
        scan_discovery_albums_found = int(state.get("scan_discovery_albums_found") or 0)
        scan_discovery_artists_found = int(state.get("scan_discovery_artists_found") or 0)
        scan_discovery_stage = str(state.get("scan_discovery_stage") or "")
        scan_discovery_entries_scanned = int(state.get("scan_discovery_entries_scanned") or 0)
        scan_discovery_root_entries_scanned = int(state.get("scan_discovery_root_entries_scanned") or 0)
        scan_discovery_folders_done = int(state.get("scan_discovery_folders_done") or 0)
        scan_discovery_folders_total = int(state.get("scan_discovery_folders_total") or 0)
        scan_discovery_albums_done = int(state.get("scan_discovery_albums_done") or 0)
        scan_discovery_albums_total = int(state.get("scan_discovery_albums_total") or 0)
        scan_discovery_started_at = state.get("scan_discovery_started_at")
        scan_discovery_updated_at = state.get("scan_discovery_updated_at")
        files_watcher_state = dict(state.get("files_watcher") or {})
        scan_discogs_matched = int(state.get("scan_discogs_matched") or 0)
        scan_lastfm_matched = int(state.get("scan_lastfm_matched") or 0)
        scan_bandcamp_matched = int(state.get("scan_bandcamp_matched") or 0)
        scan_provider_matches_raw = dict(state.get("scan_provider_matches") or {})
        scan_start_time = state.get("scan_start_time")
        scan_pipeline_flags = dict(state.get("scan_pipeline_flags") or {})
        scan_pipeline_async = bool(state.get("scan_pipeline_async"))
        scan_pipeline_sync_target = state.get("scan_pipeline_sync_target")
        scan_incomplete_moved_count = int(state.get("scan_incomplete_moved_count") or 0)
        scan_incomplete_moved_mb = int(state.get("scan_incomplete_moved_mb") or 0)
        scan_incomplete_move_running = bool(state.get("scan_incomplete_move_running"))
        scan_incomplete_move_done = int(state.get("scan_incomplete_move_done") or 0)
        scan_incomplete_move_total = int(state.get("scan_incomplete_move_total") or 0)
        scan_incomplete_move_current_album = state.get("scan_incomplete_move_current_album")
        export_progress = dict(state.get("export_progress") or {})
        export_running = bool(export_progress.get("running"))
        export_albums_done = int(export_progress.get("albums_done") or 0)
        export_albums_total = int(export_progress.get("total_albums") or 0)
        export_tracks_done = int(export_progress.get("tracks_done") or 0)
        export_tracks_total = int(export_progress.get("total_tracks") or 0)
        scan_tracks_detected_total = int(state.get("scan_tracks_detected_total") or 0)
        scan_tracks_library_kept = int(state.get("scan_tracks_library_kept") or 0)
        scan_tracks_moved_dupes = int(state.get("scan_tracks_moved_dupes") or 0)
        scan_tracks_moved_incomplete = int(state.get("scan_tracks_moved_incomplete") or 0)
        scan_tracks_unaccounted = int(state.get("scan_tracks_unaccounted") or 0)
        scan_dupe_moved_count = int(state.get("scan_dupe_moved_count") or state.get("last_dedupe_moved_count") or 0)
        scan_dupe_moved_mb = int(state.get("scan_dupe_moved_mb") or state.get("last_dedupe_saved_mb") or state.get("dedupe_saved_this_run") or 0)
        scan_processed_albums_count = int(state.get("scan_processed_albums_count") or 0)
        scan_published_albums_count = int(state.get("scan_published_albums_count") or 0)
        scan_postprocessed_albums_count = int(state.get("scan_postprocessed_albums_count") or 0)
        scan_player_sync_target = state.get("scan_player_sync_target")
        scan_player_sync_ok = state.get("scan_player_sync_ok")
        scan_player_sync_message = state.get("scan_player_sync_message")
        if improve_all_running:
            scan_post_total = max(scan_post_total, int(improve_all_state.get("total") or 0))
            scan_post_done = max(scan_post_done, int(improve_all_state.get("current") or 0))
            if not scan_post_current_artist:
                scan_post_current_artist = improve_all_state.get("current_artist")
            if not scan_post_current_album:
                scan_post_current_album = improve_all_state.get("current_album")

        scan_id_current = _int_or_none(state.get("scan_id"))
        finalizing = state.get("scan_finalizing", False)
        scan_finalizing_stage = str(state.get("scan_finalizing_stage") or "")
        scan_finalizing_label = str(state.get("scan_finalizing_label") or "")
        scan_finalizing_done = int(state.get("scan_finalizing_done") or 0)
        scan_finalizing_total = int(state.get("scan_finalizing_total") or 0)
        scan_finalizing_item_done = int(state.get("scan_finalizing_item_done") or 0)
        scan_finalizing_item_total = int(state.get("scan_finalizing_item_total") or 0)
        scan_finalizing_item_label = str(state.get("scan_finalizing_item_label") or "")
        scan_finalizing_updated_at = state.get("scan_finalizing_updated_at")
        deduping = state.get("deduping", False)
        dedupe_progress = state.get("dedupe_progress", 0)
        dedupe_total = state.get("dedupe_total", 0)
        dedupe_current_group = state.get("dedupe_current_group")
        auto_move_enabled = bool(
            (scan_pipeline_flags or {}).get(
                "dedupe",
                getattr(_runtime_module(), "AUTO_MOVE_DUPES", False),
            )
        )
        preplan_snapshot = _scan_progress_core.count_progress_snapshot(
            done=int(scan_discovery_albums_done or scan_discovery_folders_done or 0),
            total=int(scan_discovery_albums_total or scan_discovery_folders_total or 0),
        )
        preplan_done = int(preplan_snapshot.get("done") or 0)
        preplan_total = int(preplan_snapshot.get("total") or 0)
        preplan_percent = float(preplan_snapshot.get("percent") or 0.0)
        preplan_label = f"{preplan_done}/{preplan_total}" if preplan_total > 0 else f"{preplan_done}/?"
        run_scope_snapshot = _scan_progress_core.count_progress_snapshot(
            done=run_scope_done,
            total=run_scope_total,
            running=run_scope_preparing,
            started_at=run_scope_started_at,
            updated_at=run_scope_updated_at,
            stall_after_seconds=30.0,
        )
        run_scope_done = int(run_scope_snapshot.get("done") or 0)
        run_scope_total = int(run_scope_snapshot.get("total") or 0)
        run_scope_percent = float(run_scope_snapshot.get("percent") or 0.0)
        run_scope_eta_seconds = run_scope_snapshot.get("eta_seconds")
        run_scope_stalled = bool(run_scope_snapshot.get("stalled"))
        prescan_snapshot_active = bool(
            scanning
            and bool(scan_prescan_cache_snapshot_running)
            and int(scan_prescan_cache_snapshot_total or detected_albums_total or total_albums or 0) > 0
        )
        published_catchup_active = bool(
            scanning
            and bool(scan_published_catchup_running)
            and int(scan_published_catchup_total or 0) > 0
        )
        pre_scan_active = bool(
            scanning
            and not run_scope_preparing
            and (
                published_catchup_active
                or prescan_snapshot_active
                or scan_discovery_running
                or preplan_total > 0
            )
            and (
                not bool(scan_incomplete_move_running)
                and not bool(deduping)
                and not bool(export_running)
                and not bool(finalizing)
                and not bool(scan_profile_enrich_running)
                and not bool(scan_post_processing)
                and not bool(current_step)
            )
        )
        # Phase: derive from current_step and flags for UI (format_analysis | identification_tags | ia_analysis | finalizing | moving_dupes)
        enrich_snapshot = _scan_progress_core.count_progress_snapshot(
            done=scan_profile_enrich_done,
            total=scan_profile_enrich_total,
            running=scan_profile_enrich_running,
            started_at=scan_profile_enrich_started_at,
        )
        scan_profile_enrich_done = int(enrich_snapshot.get("done") or 0)
        scan_profile_enrich_total = int(enrich_snapshot.get("total") or 0)
        scan_profile_enrich_percent = float(enrich_snapshot.get("percent") or 0.0)
        scan_profile_enrich_eta_seconds = enrich_snapshot.get("eta_seconds")
        primary_worker_phase = None
        if current_step in ("comparing_versions", "detecting_best") or "_ai_batch" in active_artists_dict:
            primary_worker_phase = "ia_analysis"
        elif current_step in ("fetching_mb_id", "searching_mb"):
            primary_worker_phase = "identification_tags"
        elif current_step == "analyzing_format":
            primary_worker_phase = "format_analysis"

        if not scanning:
            phase = None
            current_step = None
            active_artists_list = []
        elif scan_starting:
            phase = "pre_scan"
            current_step = "starting_scan"
        elif run_scope_preparing:
            phase = "preparing_run_scope"
        elif pre_scan_active:
            phase = "pre_scan"
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
        elif scan_post_processing and current_step in (None, "", "done") and "_ai_batch" not in active_artists_dict:
            phase = "post_processing"
        else:
            phase = "format_analysis"
        pre_work_stage = _scan_progress_core.pre_work_stage_progress(
            progress=progress,
            total=total,
            phase_progress=phase_progress,
            current_step=current_step,
            eta_seconds=eta_seconds,
            run_scope_preparing=run_scope_preparing,
            run_scope_done=run_scope_done,
            run_scope_total=run_scope_total,
            run_scope_percent=run_scope_percent,
            run_scope_eta_seconds=run_scope_eta_seconds,
            pre_scan_active=pre_scan_active,
            scan_resume_run_id=scan_resume_run_id,
            scan_discovery_stage=scan_discovery_stage,
            prescan_snapshot_active=prescan_snapshot_active,
            published_catchup_active=published_catchup_active,
            prescan_snapshot_done=scan_prescan_cache_snapshot_rows,
            prescan_snapshot_total=scan_prescan_cache_snapshot_total,
            detected_albums_total=detected_albums_total,
            total_albums=total_albums,
            published_catchup_done=scan_published_catchup_done,
            published_catchup_total=scan_published_catchup_total,
            discovery_roots_done=scan_discovery_roots_done,
            discovery_roots_total=scan_discovery_roots_total,
            discovery_started_at=scan_discovery_started_at,
            preplan_done=preplan_done,
            preplan_total=preplan_total,
            preplan_percent=preplan_percent,
        )
        stage_progress_done = int(pre_work_stage.get("stage_progress_done") or 0)
        stage_progress_total = int(pre_work_stage.get("stage_progress_total") or 0)
        stage_progress_percent = float(pre_work_stage.get("stage_progress_percent") or 0.0)
        stage_progress_unit = str(pre_work_stage.get("stage_progress_unit") or "steps")
        if pre_work_stage.get("handled"):
            progress = int(pre_work_stage.get("progress") or 0)
            total = int(pre_work_stage.get("total") or 0)
            effective_progress = int(pre_work_stage.get("effective_progress") or 0)
            phase_progress = float(pre_work_stage.get("phase_progress") or 0.0)
            eta_seconds = pre_work_stage.get("eta_seconds")
            if pre_work_stage.get("current_step"):
                current_step = str(pre_work_stage.get("current_step") or "")
        if scan_profile_enrich_running and scan_profile_enrich_total > 0:
            phase_progress = scan_profile_enrich_percent
            stage_progress_done = int(scan_profile_enrich_done or 0)
            stage_progress_total = int(scan_profile_enrich_total or 0)
            stage_progress_percent = float(scan_profile_enrich_percent or 0.0)
            stage_progress_unit = "artists"
            if scan_profile_enrich_eta_seconds is not None:
                eta_seconds = scan_profile_enrich_eta_seconds
            current_step = "enriching_profiles"

        if scanning and not run_scope_preparing and not pre_scan_active:
            worker_stage = _scan_progress_core.worker_stage_progress(
                phase=phase,
                default_done=stage_progress_done,
                default_total=stage_progress_total,
                default_unit=stage_progress_unit,
                scan_incomplete_move_done=scan_incomplete_move_done,
                scan_incomplete_move_total=scan_incomplete_move_total,
                format_done_count=format_done_count,
                mb_done_count=mb_done_count,
                scan_processed_albums_count=scan_processed_albums_count,
                active_album_progress=active_album_progress,
                total_albums=total_albums,
                detected_albums_total=detected_albums_total,
                scan_ai_batch_processed=scan_ai_batch_processed,
                scan_ai_batch_total=scan_ai_batch_total,
                dedupe_progress=dedupe_progress,
                dedupe_total=dedupe_total,
                export_albums_done=export_albums_done,
                export_albums_total=export_albums_total,
                scan_post_done=scan_post_done,
                scan_post_total=scan_post_total,
                scan_profile_enrich_running=scan_profile_enrich_running,
                scan_profile_enrich_done=scan_profile_enrich_done,
                scan_profile_enrich_total=scan_profile_enrich_total,
                profile_backfill_state=profile_backfill_state,
                scan_finalizing_item_done=scan_finalizing_item_done,
                scan_finalizing_item_total=scan_finalizing_item_total,
                scan_finalizing_done=scan_finalizing_done,
                scan_finalizing_total=scan_finalizing_total,
            )
            if worker_stage.get("handled"):
                stage_progress_done = int(worker_stage.get("stage_progress_done") or 0)
                stage_progress_total = int(worker_stage.get("stage_progress_total") or 0)
                stage_progress_unit = str(worker_stage.get("stage_progress_unit") or stage_progress_unit)

        if stage_progress_total > 0:
            stage_progress_done = max(0, min(stage_progress_done, stage_progress_total))
            stage_progress_percent = max(
                0.0,
                min(100.0, round((float(stage_progress_done) / float(stage_progress_total)) * 100.0, 2)),
            )
            phase_progress = stage_progress_percent
        elif phase in {"finalizing", "background_enrichment"}:
            stage_progress_percent = 0.0
            phase_progress = stage_progress_percent

        scan_processed_albums_effective = int(scan_processed_albums_count or 0)
        artists_processed_effective = int(artists_processed or 0)
        effective_inflight = _scan_progress_core.effective_inflight_progress(
            scanning=bool(scanning),
            run_scope_preparing=bool(run_scope_preparing),
            pre_scan_active=bool(pre_scan_active),
            phase=phase,
            scan_processed_albums_count=scan_processed_albums_count,
            artists_processed=artists_processed,
            active_album_progress=active_album_progress,
            active_artists_started=active_artists_started,
            stage_progress_total=stage_progress_total,
            total_albums=total_albums,
            detected_albums_total=detected_albums_total,
            artists_total=artists_total,
        )
        scan_processed_albums_effective = max(
            scan_processed_albums_effective,
            int(effective_inflight.get("scan_processed_albums_effective") or 0),
        )
        artists_processed_effective = max(
            artists_processed_effective,
            int(effective_inflight.get("artists_processed_effective") or 0),
        )

        stage_rate_update = _scan_progress_core.stage_rate_eta(
            scanning=bool(scanning and not run_scope_preparing and not pre_scan_active),
            elapsed_seconds_value=elapsed_seconds,
            stage_progress_done=stage_progress_done,
            stage_progress_total=stage_progress_total,
            phase=phase,
            current_phase_rate=phase_rate,
            current_eta_seconds=eta_seconds,
            update_eta_for_all_phases=False,
            replace_outlier_eta_for_album_phases=True,
        )
        phase_rate = stage_rate_update.get("phase_rate")
        eta_seconds = stage_rate_update.get("eta_seconds")

        overall = _scan_progress_core.overall_progress(
            scanning=bool(scanning),
            run_scope_preparing=bool(run_scope_preparing),
            pre_scan_active=bool(pre_scan_active),
            progress=progress,
            total=total,
            phase_progress=phase_progress,
            scan_profile_enrich_running=scan_profile_enrich_running,
            scan_profile_enrich_done=scan_profile_enrich_done,
            scan_profile_enrich_total=scan_profile_enrich_total,
            scan_post_processing=scan_post_processing,
            improve_all_running=improve_all_running,
            scan_post_done=scan_post_done,
            scan_post_total=scan_post_total,
            artists_processed=artists_processed,
            artists_total=artists_total,
            artists_processed_effective=artists_processed_effective,
        )
        overall_progress_done = int(overall.get("overall_progress_done") or 0)
        overall_progress_total = int(overall.get("overall_progress_total") or 0)
        overall_progress_percent = float(overall.get("overall_progress_percent") or 0.0)

    finally:
        try:
            lock.release()
        except Exception:
            pass

    # AI provider/model for display (read outside lock)
    ai_provider_display = AI_PROVIDER or ""
    ai_model_display = _ai_model_display_name(ai_provider_display)

    # When not scanning, attach last completed scan summary for "Scan complete - Summary" UI.
    last_scan_summary = None
    last_scan_ai_used_count = 0
    last_scan_ai_tokens_total = 0
    last_scan_ai_cost_usd_total = 0.0
    last_scan_ai_unpriced_calls = 0
    if not scanning:
        last_summary_payload = _progress_summary_core.load_last_completed_scan_summary(
            connect_readonly=_state_connect_readonly,
            last_fix_all_total_albums=int(last_fix_all_total_albums or 0),
            last_fix_all_by_provider=last_fix_all_by_provider if isinstance(last_fix_all_by_provider, dict) else None,
            logger=logging.getLogger(__name__),
        )
        last_scan_summary = last_summary_payload.get("summary")
        last_scan_ai_used_count = int(last_summary_payload.get("ai_used_count") or 0)
        last_scan_ai_tokens_total = int(last_summary_payload.get("ai_tokens_total") or 0)
        last_scan_ai_cost_usd_total = float(last_summary_payload.get("ai_cost_usd_total") or 0.0)
        last_scan_ai_unpriced_calls = int(last_summary_payload.get("ai_unpriced_calls") or 0)

    include_unmatched_default = False
    browse_visible_albums_count = None
    browse_visible_artists_count = None
    browse_visible_tracks_count = None
    browse_visible_fallback_source = None
    if _get_library_mode() == "files":
        try:
            include_unmatched_default = _library_include_unmatched_effective()
        except Exception:
            include_unmatched_default = bool(getattr(_runtime_module(), "LIBRARY_INCLUDE_UNMATCHED", True))
        visibility_payload = _publication_snapshot.progress_library_visibility(
            files_mode=True,
            include_unmatched_default=bool(include_unmatched_default),
            scanning=bool(scanning),
            cached_payload=cached_payload if isinstance(cached_payload, dict) else None,
            scan_processed_albums_count=int(scan_processed_albums_count or 0),
            total_albums=int(total_albums or 0),
            scan_published_albums_count=int(scan_published_albums_count or 0),
            browse_counts=lambda include_unmatched: _files_library_browse_counts(
                include_unmatched,
                scope="library",
                acquire_timeout_sec=0.12,
            ),
            effective_browse_snapshot=lambda include_unmatched: _files_library_effective_browse_snapshot(
                include_unmatched,
                scope="library",
            ),
        )
        browse_visible_albums_count = visibility_payload.get("albums_count")
        browse_visible_artists_count = visibility_payload.get("artists_count")
        browse_visible_tracks_count = visibility_payload.get("tracks_count")
        browse_visible_fallback_source = visibility_payload.get("fallback_source")

    current_scan_ai_rollup = _progress_ai_core.load_current_scan_ai_rollup(
        scanning=bool(scanning),
        scan_id=int(scan_id_current) if scan_id_current else None,
        connect_readonly=_state_connect_readonly,
        logger=logging.getLogger(__name__),
    )
    ai_usage_payload = _progress_ai_core.effective_ai_usage(
        scanning=bool(scanning),
        current_scan_ai_rollup=current_scan_ai_rollup,
        scan_ai_guard_calls_used=int(scan_ai_guard_calls_used or 0),
        scan_ai_used_count=int(ai_used_count or 0),
        last_scan_summary=last_scan_summary if isinstance(last_scan_summary, dict) else None,
        last_scan_ai_used_count=int(last_scan_ai_used_count or 0),
        last_scan_ai_tokens_total=int(last_scan_ai_tokens_total or 0),
        last_scan_ai_cost_usd_total=float(last_scan_ai_cost_usd_total or 0.0),
        last_scan_ai_unpriced_calls=int(last_scan_ai_unpriced_calls or 0),
        microusd_to_usd=_microusd_to_usd,
    )
    ai_used_count_effective = int(ai_usage_payload.get("used_count") or 0)
    ai_tokens_total_effective = int(ai_usage_payload.get("tokens_total") or 0)
    ai_cost_usd_total_effective = float(ai_usage_payload.get("cost_usd_total") or 0.0)
    ai_unpriced_calls_effective = int(ai_usage_payload.get("unpriced_calls") or 0)

    try:
        with _scheduler_lock:
            background_jobs = _pipeline_jobs_core.running_scheduler_jobs(_scheduler_running_meta)
    except Exception:
        background_jobs = []
    with _files_profile_backfill_lock:
        profile_backfill_state = dict(_files_profile_backfill_state or {})
    try:
        with _files_profile_jobs_lock:
            profile_jobs_active = int(len(_files_profile_jobs_active))
    except Exception:
        profile_jobs_active = 0
    background_enrichment_running = _pipeline_jobs_core.background_enrichment_running(
        background_jobs=background_jobs,
        profile_backfill_state=profile_backfill_state,
        profile_jobs_active=profile_jobs_active,
    )
    visible_published_albums_count = (
        int(browse_visible_albums_count or 0)
        if browse_visible_albums_count is not None
        else int(scan_published_albums_count or 0)
    )
    total_artists = int(artists_total or 0)
    scan_runtime_sec_effective = _scan_progress_core.runtime_seconds_effective(
        elapsed_seconds,
        last_scan_summary if isinstance(last_scan_summary, dict) else None,
    )
    library_ready = _scan_progress_core.library_ready(
        scanning=bool(scanning),
        visible_published_albums_count=visible_published_albums_count,
        visible_published_artists_count=browse_visible_artists_count,
        scan_published_albums_count=scan_published_albums_count,
        has_completed_full_scan=bool(has_completed_full_scan),
        last_scan_summary=last_scan_summary if isinstance(last_scan_summary, dict) else None,
    )
    phase = _scan_progress_core.refine_post_work_phase(
        phase,
        scanning=bool(scanning),
        scan_starting=bool(scan_starting),
        run_scope_preparing=bool(run_scope_preparing),
        pre_scan_active=bool(pre_scan_active),
        scan_discovery_running=bool(scan_discovery_running),
        scan_incomplete_move_running=bool(scan_incomplete_move_running),
        export_running=bool(export_running),
        deduping=bool(deduping),
        background_enrichment_running=bool(background_enrichment_running),
        scan_processed_albums_count=scan_processed_albums_count,
        total_albums=total_albums,
        artists_processed=artists_processed,
        artists_total=total_artists,
    )

    run_scope_payload = _scan_progress_core.run_scope_payload_state(
        phase=phase, run_scope_preparing=bool(run_scope_preparing),
        run_scope_stage=run_scope_stage, run_scope_done=run_scope_done, run_scope_total=run_scope_total,
        run_scope_percent=run_scope_percent, run_scope_eta_seconds=run_scope_eta_seconds, run_scope_stalled=run_scope_stalled,
    )
    run_scope_stage = str(run_scope_payload.get("run_scope_stage") or "idle")
    run_scope_done, run_scope_total = int(run_scope_payload.get("run_scope_done") or 0), int(run_scope_payload.get("run_scope_total") or 0)
    run_scope_percent = float(run_scope_payload.get("run_scope_percent") or 0.0)
    run_scope_eta_seconds, run_scope_stalled = run_scope_payload.get("run_scope_eta_seconds"), bool(run_scope_payload.get("run_scope_stalled"))

    post_processing_payload = _scan_progress_core.post_processing_payload_state(
        phase=phase, scan_post_processing=bool(scan_post_processing),
        scan_post_done=scan_post_done, scan_post_total=scan_post_total,
        scan_post_current_artist=scan_post_current_artist, scan_post_current_album=scan_post_current_album,
        scanning=bool(scanning), run_scope_preparing=bool(run_scope_preparing), pre_scan_active=bool(pre_scan_active),
        artists_processed=artists_processed, artists_total=artists_total,
        overall_progress_done=overall_progress_done, overall_progress_total=overall_progress_total,
        overall_progress_percent=overall_progress_percent,
    )
    scan_post_processing = bool(post_processing_payload.get("scan_post_processing"))
    scan_post_done, scan_post_total = int(post_processing_payload.get("scan_post_done") or 0), int(post_processing_payload.get("scan_post_total") or 0)
    scan_post_current_artist, scan_post_current_album = post_processing_payload.get("scan_post_current_artist"), post_processing_payload.get("scan_post_current_album")
    overall_progress_done, overall_progress_total = int(post_processing_payload.get("overall_progress_done") or 0), int(post_processing_payload.get("overall_progress_total") or 0)
    overall_progress_percent = float(post_processing_payload.get("overall_progress_percent") or 0.0)

    resume_snapshot = _progress_runtime_core.resume_availability_snapshot(
        scanning=bool(scanning),
        library_mode_loader=_get_library_mode,
        get_resume_run_snapshot=_get_resume_run_snapshot,
        get_latest_resume_run_snapshot_any_signature=_get_latest_resume_run_snapshot_any_signature,
    )
    resume_available = bool(resume_snapshot.get("resume_available"))
    resume_available_by_scan_type = dict(resume_snapshot.get("resume_available_by_scan_type") or {})

    provider_matches_so_far = _normalize_scan_provider_matches(
        scan_provider_matches_raw,
        legacy_discogs=scan_discogs_matched,
        legacy_lastfm=scan_lastfm_matched,
        legacy_bandcamp=scan_bandcamp_matched,
    )
    provider_live_stats = _progress_runtime_core.provider_gateway_live_stats(
        scanning=bool(scanning),
        snapshot_loader=lambda _fallback: _provider_gateway_stats_snapshot(),
    )
    scan_provider_stats_live = dict(provider_live_stats.get("providers") or {})
    matches_so_far = int(sum(int(v or 0) for v in provider_matches_so_far.values()))
    exports_so_far = int(max(int(scan_published_albums_count or 0), int(export_albums_done or 0), int(visible_published_albums_count or 0)))
    incomplete_albums_so_far = int(broken_albums_count or 0)
    duplicate_losers_so_far = int(total_duplicates_count or 0)
    active_artists_count = int(len(active_artists_list))

    pipeline_step_human_label, current_stage_human_label = _scan_progress_core.phase_labels(
        phase,
        current_step,
        scan_finalizing_label=scan_finalizing_label,
        scan_finalizing_item_label=scan_finalizing_item_label,
    )
    scan_progress_mode = _scan_progress_core.progress_mode(phase)
    scan_eta_confidence = _scan_progress_core.eta_confidence(
        scanning=scanning,
        eta_seconds=eta_seconds,
        phase=phase,
        elapsed_seconds=elapsed_seconds,
        stage_progress_done=stage_progress_done,
    )

    payload = {
        "scan_id": int(state.get("scan_id") or 0) if state.get("scan_id") else None,
        "scanning": scanning,
        "scan_starting": scan_starting,
        "progress": progress,
        "total": total,
        "effective_progress": effective_progress,
        "status": status,
        "library_ready": library_ready,
        "background_enrichment_running": background_enrichment_running,
        "background_jobs": background_jobs,
        "profile_backfill": profile_backfill_state,
        "resume_available": resume_available,
        "resume_available_by_scan_type": resume_available_by_scan_type,
        "phase": phase,
        "current_step": current_step,
        "scan_progress_mode": scan_progress_mode,
        "scan_eta_confidence": scan_eta_confidence,
        "pipeline_step_human_label": pipeline_step_human_label,
        "current_stage_human_label": current_stage_human_label,
        "ai_provider": ai_provider_display,
        "ai_model": ai_model_display,
        # Scan details
        "artists_processed": artists_processed,
        "artists_total": artists_total,
        "detected_artists_total": detected_artists_total,
        "detected_albums_total": detected_albums_total,
        "resume_skipped_artists": resume_skipped_artists,
        "resume_skipped_albums": resume_skipped_albums,
        "scan_run_scope_preparing": run_scope_preparing,
        "scan_run_scope_stage": run_scope_stage,
        "scan_run_scope_done": run_scope_done,
        "scan_run_scope_total": run_scope_total,
        "scan_run_scope_percent": run_scope_percent,
        "scan_run_scope_eta_seconds": run_scope_eta_seconds,
        "scan_run_scope_stalled": run_scope_stalled,
        "scan_run_scope_artists_included": run_scope_artists_included,
        "scan_run_scope_albums_included": run_scope_albums_included,
        "scan_prescan_cache_snapshot_running": scan_prescan_cache_snapshot_running,
        "scan_prescan_cache_snapshot_done": scan_prescan_cache_snapshot_done,
        "scan_prescan_cache_snapshot_rows": scan_prescan_cache_snapshot_rows,
        "scan_prescan_cache_snapshot_total": scan_prescan_cache_snapshot_total,
        "scan_prescan_cache_snapshot_updated_at": scan_prescan_cache_snapshot_updated_at,
        "scan_published_catchup_running": scan_published_catchup_running,
        "scan_published_catchup_reason": scan_published_catchup_reason,
        "scan_published_catchup_done": scan_published_catchup_done,
        "scan_published_catchup_total": scan_published_catchup_total,
        "scan_published_catchup_ok": scan_published_catchup_ok,
        "scan_published_catchup_failed": scan_published_catchup_failed,
        "scan_published_catchup_current_artist": scan_published_catchup_current_artist,
        "scan_published_catchup_started_at": scan_published_catchup_started_at,
        "scan_published_catchup_updated_at": scan_published_catchup_updated_at,
        "scan_published_catchup_finished_at": scan_published_catchup_finished_at,
        "files_cache_quality_recalc_running": files_cache_quality_recalc_running,
        "files_cache_quality_recalc_total": files_cache_quality_recalc_total,
        "files_cache_quality_recalc_done": files_cache_quality_recalc_done,
        "files_cache_quality_recalc_rows_upserted": files_cache_quality_recalc_rows_upserted,
        "files_cache_quality_recalc_errors": files_cache_quality_recalc_errors,
        "files_cache_quality_recalc_missing_folders": files_cache_quality_recalc_missing_folders,
        "files_cache_quality_recalc_no_audio": files_cache_quality_recalc_no_audio,
        "files_cache_quality_recalc_reason": files_cache_quality_recalc_reason,
        "files_cache_quality_recalc_started_at": files_cache_quality_recalc_started_at,
        "files_cache_quality_recalc_updated_at": files_cache_quality_recalc_updated_at,
        "files_cache_quality_recalc_finished_at": files_cache_quality_recalc_finished_at,
        "ai_used_count": ai_used_count_effective,
        "ai_tokens_total": ai_tokens_total_effective,
        "ai_cost_usd_total": ai_cost_usd_total_effective,
        "ai_unpriced_calls": ai_unpriced_calls_effective,
        "mb_used_count": mb_used_count,
        "ai_enabled": ai_enabled,
        "scan_ai_guard_calls_used": scan_ai_guard_calls_used,
        "scan_ai_guard_calls_blocked": scan_ai_guard_calls_blocked,
        "scan_ai_guard_last_reason": scan_ai_guard_last_reason,
        "scan_ai_guard_last_block_at": scan_ai_guard_last_block_at,
        "scan_profile_enrich_running": scan_profile_enrich_running,
        "scan_profile_enrich_total": scan_profile_enrich_total,
        "scan_profile_enrich_done": scan_profile_enrich_done,
        "scan_profile_enrich_percent": scan_profile_enrich_percent,
        "scan_profile_enrich_eta_seconds": scan_profile_enrich_eta_seconds,
        "scan_profile_enrich_current_artist": scan_profile_enrich_current_artist,
        "scan_profile_enrich_started_at": scan_profile_enrich_started_at,
        "scan_profile_enrich_updated_at": scan_profile_enrich_updated_at,
        "mb_enabled": mb_enabled,
        # Cache statistics
        "audio_cache_hits": audio_cache_hits,
        "audio_cache_misses": audio_cache_misses,
        "mb_cache_hits": mb_cache_hits,
        "mb_cache_misses": mb_cache_misses,
        # Detailed statistics
        "duplicate_groups_count": duplicate_groups_count,
        "total_duplicates_count": total_duplicates_count,
        "broken_albums_count": broken_albums_count,
        "missing_albums_count": missing_albums_count,
        "albums_without_artist_image": albums_without_artist_image,
        "albums_without_album_image": albums_without_album_image,
        "albums_without_complete_tags": albums_without_complete_tags,
        "albums_without_mb_id": albums_without_mb_id,
        "albums_without_artist_mb_id": albums_without_artist_mb_id,
        "format_done_count": format_done_count,
        "mb_done_count": mb_done_count,
        # Settings visible in scanner (e.g. link to configure)
        "mb_retry_not_found": getattr(_runtime_module(), "MB_RETRY_NOT_FOUND", False),
        # ETA
        "eta_seconds": eta_seconds,
        "threads_in_use": threads_in_use,
        "active_artists": active_artists_list,
        "last_scan_summary": None if scanning else last_scan_summary,
        "scan_steps_log": scan_steps_log,
        "scan_steps_log_total": scan_steps_log_total,
        "scan_type": current_scan_type,
        "scan_resume_run_id": scan_resume_run_id,
        "finalizing": finalizing,
        "scan_finalizing_stage": scan_finalizing_stage,
        "scan_finalizing_label": scan_finalizing_label,
        "scan_finalizing_done": scan_finalizing_done,
        "scan_finalizing_total": scan_finalizing_total,
        "scan_finalizing_item_done": scan_finalizing_item_done,
        "scan_finalizing_item_total": scan_finalizing_item_total,
        "scan_finalizing_item_label": scan_finalizing_item_label,
        "scan_finalizing_updated_at": scan_finalizing_updated_at,
        "deduping": deduping,
        "dedupe_progress": dedupe_progress,
        "dedupe_total": dedupe_total,
        "dedupe_current_group": dedupe_current_group,
        "auto_move_enabled": auto_move_enabled,
        "paths_status": _paths_rw_status(),
        # IA analysis step: current group label and N/M progress
        "scan_ai_batch_total": scan_ai_batch_total,
        "scan_ai_batch_processed": scan_ai_batch_processed,
        "scan_ai_current_label": scan_ai_current_label,
        "total_albums": total_albums,
        "post_processing": scan_post_processing,
        "post_processing_done": scan_post_done,
        "post_processing_total": scan_post_total,
        "post_processing_current_artist": scan_post_current_artist,
        "post_processing_current_album": scan_post_current_album,
        "scan_discovery_running": scan_discovery_running,
        "scan_discovery_current_root": scan_discovery_current_root,
        "scan_discovery_roots_done": scan_discovery_roots_done,
        "scan_discovery_roots_total": scan_discovery_roots_total,
        "scan_discovery_files_found": scan_discovery_files_found,
        "scan_discovery_folders_found": scan_discovery_folders_found,
        "scan_discovery_albums_found": scan_discovery_albums_found,
        "scan_discovery_artists_found": scan_discovery_artists_found,
        "scan_discovery_stage": scan_discovery_stage,
        "scan_discovery_entries_scanned": scan_discovery_entries_scanned,
        "scan_discovery_root_entries_scanned": scan_discovery_root_entries_scanned,
        "scan_discovery_folders_done": scan_discovery_folders_done,
        "scan_discovery_folders_total": scan_discovery_folders_total,
        "scan_discovery_albums_done": scan_discovery_albums_done,
        "scan_discovery_albums_total": scan_discovery_albums_total,
        "scan_discovery_started_at": scan_discovery_started_at,
        "scan_discovery_updated_at": scan_discovery_updated_at,
        "scan_preplan_done": preplan_done,
        "scan_preplan_total": preplan_total,
        "scan_preplan_percent": preplan_percent,
        "scan_preplan_label": preplan_label,
        "files_watcher_running": bool(files_watcher_state.get("running")),
        "files_watcher_roots": list(files_watcher_state.get("roots") or []),
        "files_watcher_dirty_count": int(files_watcher_state.get("dirty_count") or 0),
        "files_watcher_last_event_at": files_watcher_state.get("last_event_at"),
        "files_watcher_last_event_path": files_watcher_state.get("last_event_path"),
        "scan_discogs_matched": scan_discogs_matched,
        "scan_lastfm_matched": scan_lastfm_matched,
        "scan_bandcamp_matched": scan_bandcamp_matched,
        "provider_matches_so_far": provider_matches_so_far,
        "scan_provider_stats_live": scan_provider_stats_live,
        "matches_so_far": matches_so_far,
        "exports_so_far": exports_so_far,
        "incomplete_albums_so_far": incomplete_albums_so_far,
        "duplicate_losers_so_far": duplicate_losers_so_far,
        "active_artists_count": active_artists_count,
        "scan_start_time": scan_start_time,
        "scan_pipeline_flags": scan_pipeline_flags,
        "scan_pipeline_async": scan_pipeline_async,
        "scan_pipeline_sync_target": scan_pipeline_sync_target,
        "scan_incomplete_moved_count": scan_incomplete_moved_count,
        "scan_incomplete_moved_mb": scan_incomplete_moved_mb,
        "scan_dupe_moved_count": scan_dupe_moved_count,
        "scan_dupe_moved_mb": scan_dupe_moved_mb,
        "scan_incomplete_move_running": scan_incomplete_move_running,
        "scan_incomplete_move_done": scan_incomplete_move_done,
        "scan_incomplete_move_total": scan_incomplete_move_total,
        "scan_incomplete_move_current_album": scan_incomplete_move_current_album,
        "export_progress": export_progress,
        "export_running": export_running,
        "export_albums_done": export_albums_done,
        "export_albums_total": export_albums_total,
        "export_tracks_done": export_tracks_done,
        "export_tracks_total": export_tracks_total,
        "scan_tracks_detected_total": scan_tracks_detected_total,
        "scan_tracks_library_kept": scan_tracks_library_kept,
        "scan_tracks_moved_dupes": scan_tracks_moved_dupes,
        "scan_tracks_moved_incomplete": scan_tracks_moved_incomplete,
        "scan_tracks_unaccounted": scan_tracks_unaccounted,
        "scan_processed_albums_count": scan_processed_albums_count,
        "scan_processed_albums_effective": scan_processed_albums_effective,
        "scan_published_albums_count": visible_published_albums_count,
        "scan_published_album_rows_count": scan_published_albums_count,
        "scan_postprocessed_albums_count": scan_postprocessed_albums_count,
        "artists_processed_effective": artists_processed_effective,
        "scan_player_sync_target": scan_player_sync_target,
        "scan_player_sync_ok": scan_player_sync_ok,
        "scan_player_sync_message": scan_player_sync_message,
        "bootstrap_required": bool(bootstrap_status.get("bootstrap_required")),
        "autonomous_mode": _scan_autonomous_mode_effective(),
        "has_completed_full_scan": bool(has_completed_full_scan),
        "default_scan_type": str(default_scan_type or "full"),
        "library_visible_albums_count": browse_visible_albums_count,
        "library_visible_artists_count": browse_visible_artists_count,
        "library_visible_tracks_count": browse_visible_tracks_count,
        "library_visible_fallback_source": browse_visible_fallback_source,
        "library_include_unmatched_default": bool(include_unmatched_default),
        "elapsed_seconds": elapsed_seconds,
        "scan_runtime_sec": scan_runtime_sec_effective,
        "phase_rate": phase_rate,
        "phase_progress": phase_progress,
        "stage_progress_done": stage_progress_done,
        "stage_progress_total": stage_progress_total,
        "stage_progress_percent": stage_progress_percent,
        "stage_progress_unit": stage_progress_unit,
        "overall_progress_done": overall_progress_done,
        "overall_progress_total": overall_progress_total,
        "overall_progress_percent": overall_progress_percent,
        "mcp": _mcp_status_summary(include_audit=False),
    }
    payload.update(_storage_progress_payload())
    setattr(mod, "_API_PROGRESS_CACHE", {"ts": time.time(), "payload": payload, "state_key": current_state_key})
    return jsonify(payload)
