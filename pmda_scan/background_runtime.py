"""Runtime-backed background scan orchestration.

This module contains the main scan orchestration body extracted from ``pmda.py``.
It still binds the live PMDA runtime at the boundary while scan phases are split
into explicit services. Runtime global writes are mirrored back to the bound PMDA
module to preserve existing behavior during the transition.
"""

from __future__ import annotations

import sys
from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one background scan call."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {"background_scan"}
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def _runtime_module() -> Any:
    """Return the bound PMDA runtime module when available."""
    return _RUNTIME if _RUNTIME is not None else sys.modules[__name__]


def _set_runtime_attr(name: str, value: Any) -> None:
    """Mirror transitional global writes back to the bound PMDA runtime."""
    runtime = _runtime_module()
    try:
        setattr(runtime, name, value)
    except Exception:
        pass
    globals()[name] = value


def background_scan_for_runtime(runtime: Any) -> None:
    """Run the PMDA background scan using the live runtime module."""
    _bind_runtime(runtime)
    return _background_scan_impl()


def _background_scan_impl():
    """
    Scan the entire library in parallel, persist results to SQLite,
    and update the in‑memory `state` for the Web UI.

    The function is now exception‑safe: no single worker failure will abort
    the whole scan, and `state["scanning"]` is **always** cleared even when
    an unexpected error occurs, so the front‑end never hangs in "running".
    """
    global ai_provider_ready, AI_FUNCTIONAL_ERROR_MSG
    # Reload library backend settings (mode + files roots) and Plex selectors/path map
    # so scan always uses the latest saved sources from Settings.
    try:
        _reload_library_mode_and_files_roots_from_db()
        _reload_musicbrainz_settings_from_db()
    except Exception:
        logging.exception("background_scan(): failed to reload runtime settings")
        with lock:
            state["scanning"] = False
            state["scan_starting"] = False
            state["scan_start_requested_at"] = None
            state["scan_auto_trigger"] = None
            state["scan_scheduler_run_id"] = None
        return
    mode = _get_library_mode()
    if mode == "plex":
        _reload_section_ids_from_db()
        _reload_path_map_from_db()
    if mode == "plex" and not SECTION_IDS:
        logging.warning("background_scan(): SECTION_IDS is empty after reload; aborting scan (Plex mode)")
        with lock:
            state["scanning"] = False
            state["scan_starting"] = False
            state["scan_start_requested_at"] = None
            state["scan_auto_trigger"] = None
            state["scan_scheduler_run_id"] = None
        return
    if mode == "plex" and not PATH_MAP:
        logging.warning("background_scan(): PATH_MAP is empty after reload; albums will not resolve to container paths. Run Detect & verify in Settings.")
    if mode == "plex":
        logging.debug(f"background_scan(): SECTION_IDS=%s, PATH_MAP keys=%s, opening Plex DB at {PLEX_DB_FILE}", SECTION_IDS, list(PATH_MAP.keys()))
    else:
        logging.debug("background_scan(): FILES mode active; skipping Plex PATH_MAP reload/checks.")
    scan_perf_start = time.perf_counter()
    all_results: Dict[str, List[dict]] = {}  # Always defined so finally can persist
    all_editions_by_artist: Dict[str, List[dict]] = {}  # For scan_editions (Library, Tag Fixer)
    scan_incremental_queue = None
    scan_incremental_writer_thread = None
    files_live_index_last_trigger = 0.0
    files_live_index_interval_sec = 45.0
    scan_status = "failed"
    scan_type = "full"
    resume_run_id = None
    scan_post_queue = None
    scan_post_worker_thread = None
    run_improve_after_requested = False
    pipeline_async_enabled = False
    pipeline_flags_requested = {
        "match_fix": False,
        "dedupe": False,
        "incomplete_move": False,
        "export": False,
        "player_sync": False,
        "sync_target": "none",
    }
    pipeline_flags = {
        "match_fix": False,
        "dedupe": False,
        "incomplete_move": False,
        "export": False,
        "player_sync": False,
        "sync_target": "none",
    }
    scan_auto_trigger = "interactive"
    scan_scheduler_run_id = None
    scan_task_event_id = 0
    scan_task_run_id = ""
    scan_task_job_type = "scan_full"
    scan_task_scope = "full"
    scan_task_source = "interactive"
    scan_task_summary: dict[str, Any] = {}
    scan_task_message = ""
    scan_task_error = ""
    scan_stream_post_by_artist = False
    streamed_post_process_done = False
    scan_post_queue_shutdown_requested = False
    files_live_index_precleared = False
    preserve_live_index_for_run = False
    with lock:
        run_improve_after_requested = bool(state.get("run_improve_after", False))
        scan_type = (state.get("scan_type") or "full").strip().lower()
        scan_auto_trigger = str(state.get("scan_auto_trigger") or "interactive").strip().lower() or "interactive"
        scan_scheduler_run_id = state.get("scan_scheduler_run_id")
    if scan_type not in {"full", "changed_only"}:
        scan_type = "full"
    pipeline_flags_requested = _pipeline_flags_for_scan(scan_type, run_improve_after_requested)
    pipeline_async_enabled = _scan_orchestrator_core.pipeline_async_enabled(scan_type, PIPELINE_POST_SCAN_ASYNC)
    pipeline_flags = _pipeline_inline_flags(
        pipeline_flags_requested,
        pipeline_async_enabled=bool(pipeline_async_enabled),
    )
    scan_task_job_type = "scan_changed" if scan_type == "changed_only" else "scan_full"
    scan_task_scope = "new" if scan_type == "changed_only" else "full"
    scan_task_source = scan_auto_trigger
    logging.info(
        "[Scan Pipeline] Starting %s scan from %s. Async post-scan=%s. Requested steps: %s. Running inline now: %s.",
        scan_type,
        scan_task_source,
        "yes" if bool(pipeline_async_enabled) else "no",
        _summarize_pipeline_flags_for_log(pipeline_flags_requested),
        _summarize_pipeline_flags_for_log(pipeline_flags),
    )
    _log_scan_web_search_backend_status_once()
    if scan_task_source in SCHEDULER_MANAGED_SCAN_SOURCES and scan_scheduler_run_id:
        scan_task_run_id = str(scan_scheduler_run_id)
    else:
        scan_task_run_id = str(uuid.uuid4())
        scan_task_event_id = _task_event_start(
            run_id=scan_task_run_id,
            job_type=scan_task_job_type,
            scope=scan_task_scope,
            source=scan_task_source,
            message=f"{scan_task_job_type} started",
        )
    _pipeline_job_update(
        "scan",
        status="running",
        phase="starting",
        message=f"{scan_task_job_type} started",
        run_id=scan_task_run_id,
        meta={"scan_type": scan_type, "source": scan_task_source},
    )

    # Mark scan as running immediately so UI can show early source-discovery activity.
    with lock:
        requested_resume_run_id = str(state.get("scan_resume_requested_run_id") or "").strip() or None
        preserve_live_index_for_run = _files_should_preserve_live_index_for_scan(
            scan_type,
            requested_resume_run_id,
        )
        state["scan_starting"] = False
        state["scanning"] = True
        state["scan_resume_run_id"] = requested_resume_run_id
        state["scan_preserve_live_index"] = bool(preserve_live_index_for_run)
        state["scan_type"] = scan_type
        state["scan_auto_trigger"] = scan_auto_trigger
        state["scan_scheduler_run_id"] = scan_scheduler_run_id
        state["scan_progress"] = 0
        state["scan_total"] = 0
        state["scan_step_progress"] = 0
        state["scan_step_total"] = 0
        state["scan_artists_processed"] = 0
        state["scan_artists_total"] = 0
        state["scan_resume_plan_restored"] = False
        state["scan_detected_artists_total"] = 0
        state["scan_detected_albums_total"] = 0
        state["scan_resume_skipped_artists"] = 0
        state["scan_resume_skipped_albums"] = 0
        state["scan_run_scope_preparing"] = False
        state["scan_run_scope_stage"] = "idle"
        state["scan_run_scope_done"] = 0
        state["scan_run_scope_total"] = 0
        state["scan_run_scope_artists_included"] = 0
        state["scan_run_scope_albums_included"] = 0
        state["scan_processed_albums_count"] = 0
        state["scan_published_albums_count"] = 0
        state["scan_postprocessed_albums_count"] = 0
        state["scan_run_scope_started_at"] = None
        state["scan_run_scope_updated_at"] = None
        state["scan_prescan_cache_snapshot_running"] = False
        state["scan_prescan_cache_snapshot_done"] = False
        state["scan_prescan_cache_snapshot_rows"] = 0
        state["scan_prescan_cache_snapshot_total"] = 0
        state["scan_prescan_cache_snapshot_updated_at"] = None
        state["scan_published_catchup_running"] = False
        state["scan_published_catchup_reason"] = None
        state["scan_published_catchup_done"] = 0
        state["scan_published_catchup_total"] = 0
        state["scan_published_catchup_ok"] = 0
        state["scan_published_catchup_failed"] = 0
        state["scan_published_catchup_current_artist"] = None
        state["scan_published_catchup_started_at"] = None
        state["scan_published_catchup_updated_at"] = None
        state["scan_published_catchup_finished_at"] = None
        state["scan_active_artists"] = {}
        state["scan_finalizing"] = False
        state["scan_discovery_running"] = (_get_library_mode() == "files")
        state["scan_discovery_current_root"] = None
        state["scan_discovery_roots_done"] = 0
        state["scan_discovery_roots_total"] = 0
        state["scan_discovery_files_found"] = 0
        state["scan_discovery_folders_found"] = 0
        state["scan_discovery_albums_found"] = 0
        state["scan_discovery_artists_found"] = 0
        state["scan_discovery_stage"] = "filesystem" if (_get_library_mode() == "files") else "idle"
        state["scan_discovery_entries_scanned"] = 0
        state["scan_discovery_root_entries_scanned"] = 0
        state["scan_discovery_folders_done"] = 0
        state["scan_discovery_folders_total"] = 0
        state["scan_discovery_albums_done"] = 0
        state["scan_discovery_albums_total"] = 0
        state["scan_discovery_started_at"] = time.time() if (_get_library_mode() == "files") else None
        state["scan_discovery_updated_at"] = time.time() if (_get_library_mode() == "files") else None
        state["scan_tracks_detected_total"] = 0
        state["scan_tracks_library_kept"] = 0
        state["scan_tracks_moved_dupes"] = 0
        state["scan_tracks_moved_incomplete"] = 0
        state["scan_tracks_unaccounted"] = 0
        state["scan_dupe_moved_count"] = 0
        state["scan_dupe_moved_mb"] = 0
        state["scan_pipeline_flags"] = dict(pipeline_flags_requested)
        state["scan_pipeline_async"] = bool(pipeline_async_enabled)
        state["scan_pipeline_sync_target"] = str(pipeline_flags.get("sync_target") or "none")

    scan_id = None
    try:
        live_index_preserved_for_run = bool(requested_resume_run_id or preserve_live_index_for_run)
        if _get_library_mode() == "files" and scan_type == "full":
            if requested_resume_run_id:
                log_scan(
                    "Files full scan: explicit resume_run_id %s requested, keeping current live library index.",
                    requested_resume_run_id,
                )
            elif preserve_live_index_for_run:
                log_scan("Files full scan: unfinished resume run detected, keeping current live library index.")
            else:
                _reset_files_live_index_for_scan()
                files_live_index_precleared = True
            if bool(requested_resume_run_id or preserve_live_index_for_run):
                try:
                    snapshot = _files_library_browse_snapshot(True, scope="all")
                    if bool(snapshot.get("underbuilt")) and int(snapshot.get("published_albums") or 0) > 0:
                        _enqueue_files_index_published_catchup(reason="scan_resume")
                except Exception:
                    logging.debug("Failed to enqueue Files published catchup on scan resume", exc_info=True)

        # Log cache behavior for this run so logs show whether existing cache is being used
        if SCAN_DISABLE_CACHE:
            log_scan(
                "Background scan started with SCAN_DISABLE_CACHE=True – ignoring audio and metadata caches for this run"
            )
        else:
            log_scan(
                "Background scan started with SCAN_DISABLE_CACHE=False – using audio and metadata caches when available"
            )

        # 1) Build the scan plan from the active library backend.
        artists_merged, total_albums = _build_scan_plan(scan_type=scan_type)
        detected_artists_total = len(artists_merged)
        detected_albums_total = total_albums
        detected_tracks_total = 0
        total_artists = detected_artists_total
        files_editions_for_resume = {}
        resume_plan_restored = False
        if _get_library_mode() == "files":
            with lock:
                files_editions_for_resume = dict(state.get("files_editions_by_album_id") or {})
                detected_tracks_total = int(state.get("scan_discovery_files_found") or 0)
                resume_plan_restored = bool(state.get("scan_resume_plan_restored"))
        with lock:
            # Surface run-scope counts as soon as discovery is done (before resume filtering completes).
            state["scan_detected_artists_total"] = int(detected_artists_total or 0)
            state["scan_detected_albums_total"] = int(detected_albums_total or 0)
            state["scan_tracks_detected_total"] = int(detected_tracks_total or 0)
            state["scan_artists_total"] = int(detected_artists_total or 0)
            state["scan_total_albums"] = int(detected_albums_total or 0)
            state["scan_run_scope_preparing"] = bool(detected_artists_total > 0)
            state["scan_run_scope_stage"] = "signatures" if detected_artists_total > 0 else "done"
            state["scan_run_scope_done"] = 0
            state["scan_run_scope_total"] = int(detected_artists_total or 0)
            state["scan_run_scope_artists_included"] = 0
            state["scan_run_scope_albums_included"] = 0
            state["scan_run_scope_started_at"] = time.time() if detected_artists_total > 0 else None
            state["scan_run_scope_updated_at"] = time.time() if detected_artists_total > 0 else None
        if _get_library_mode() == "files":
            with lock:
                restored_resume_run_id = str(state.get("scan_resume_run_id") or "").strip() or None
            if _files_should_snapshot_prescan_cache_for_run(
                requested_resume_run_id=requested_resume_run_id,
                current_resume_run_id=restored_resume_run_id,
            ):
                # Persist pre-scan findings immediately so next runs can reuse them even if this run pauses/stops.
                _trigger_prescan_cache_snapshot_async(reason="post_prescan_before_resume")
            else:
                log_scan(
                    "FILES prescan cache snapshot skipped: restored resume plan %s already provides persisted pre-scan state.",
                    restored_resume_run_id or requested_resume_run_id or "",
                )

        def _on_run_scope_progress(**payload: object) -> None:
            with lock:
                state["scan_run_scope_stage"] = str(payload.get("stage") or state.get("scan_run_scope_stage") or "signatures")
                state["scan_run_scope_done"] = int(payload.get("done") or 0)
                state["scan_run_scope_total"] = int(payload.get("total") or 0)
                state["scan_run_scope_artists_included"] = int(payload.get("included_artists") or 0)
                state["scan_run_scope_albums_included"] = int(payload.get("included_albums") or 0)
                state["scan_resume_skipped_artists"] = int(payload.get("skipped_artists") or 0)
                state["scan_resume_skipped_albums"] = int(payload.get("skipped_albums") or 0)
                if not state.get("scan_run_scope_started_at"):
                    state["scan_run_scope_started_at"] = time.time()
                state["scan_run_scope_updated_at"] = time.time()
                if state["scan_run_scope_stage"] == "done":
                    state["scan_run_scope_preparing"] = False

        with lock:
            resume_run_id_current = str(state.get("scan_resume_run_id") or "").strip() or None
        resume_run_id, artists_merged, resume_skipped_artists, resume_skipped_albums = _prepare_resume_scan_artists(
            _get_library_mode(),
            scan_type,
            artists_merged,
            files_editions_by_album_id=files_editions_for_resume,
            resume_run_id_override=(resume_run_id_current or requested_resume_run_id),
            progress_cb=_on_run_scope_progress,
            pause_event=scan_is_paused,
            force_include_plan_rows=bool(resume_plan_restored),
        )
        total_artists = len(artists_merged)
        total_albums = sum(len(ids) for _a, _n, ids in artists_merged)
        with lock:
            state["scan_resume_run_id"] = resume_run_id
            state["scan_preserve_live_index"] = bool(preserve_live_index_for_run)
            state["scan_resume_requested_run_id"] = None
            state["scan_detected_artists_total"] = int(detected_artists_total or 0)
            state["scan_detected_albums_total"] = int(detected_albums_total or 0)
            state["scan_resume_skipped_artists"] = int(resume_skipped_artists or 0)
            state["scan_resume_skipped_albums"] = int(resume_skipped_albums or 0)
            state["scan_tracks_detected_total"] = int(detected_tracks_total or 0)
            state["scan_run_scope_preparing"] = False
            state["scan_run_scope_stage"] = "done"
            state["scan_run_scope_done"] = int(detected_artists_total or 0)
            state["scan_run_scope_total"] = int(detected_artists_total or 0)
            state["scan_run_scope_artists_included"] = int(total_artists or 0)
            state["scan_run_scope_albums_included"] = int(total_albums or 0)
            state["scan_run_scope_updated_at"] = time.time()
        if resume_skipped_artists:
            log_scan(
                "Resume: skipped %d already-done artist(s), %d album(s) unchanged since last interrupted run.",
                resume_skipped_artists,
                resume_skipped_albums,
            )
        if _get_library_mode() == "files":
            if scan_type == "full" and resume_skipped_artists == 0 and not files_live_index_precleared and not live_index_preserved_for_run:
                _reset_files_live_index_for_scan()
            files_live_index_last_trigger = time.time()
            if _trigger_files_index_rebuild_async(reason="scan_started"):
                files_live_index_last_trigger = time.time()
                logging.info("Files library live sync: initial index rebuild started")
        log_scan(
            "SCAN [%s] %d artist(s), %d album(s)%s",
            scan_type,
            total_artists,
            total_albums,
            f" – Section ID(s): {SECTION_IDS}" if _get_library_mode() == "plex" else " (Files backend)",
        )

        # --- Discord: announce scan start ---
        notify_discord_embed(
            title="🔄 PMDA scan started",
            description=(
                f"Scanning {len(artists_merged)} artists / {total_albums} albums… "
                "Buckle up!"
            )
        )

        logging.debug(
            f"background_scan(): {len(artists_merged)} artists (merged by name), {total_albums} albums total"
        )

        # Reload AI config from DB and run functional check (probe/ladder for OpenAI)
        _reload_ai_config_and_reinit()
        logging.debug("Scan: AI reload/reinit done. ai_provider_ready=%s", ai_provider_ready)

        try:
            uid = _current_user_id_or_zero()
            effective_scan_provider = _resolve_provider_for_runtime(
                str(AI_PROVIDER or "openai"),
                "provider_identity",
                user_id=uid,
            )
            if effective_scan_provider == "openai-codex" and not ai_provider_ready:
                codex_ready, codex_reason = _wait_for_codex_runtime_ready_for_scan(uid, timeout_sec=45.0)
                if codex_ready:
                    _set_runtime_attr("ai_provider_ready", True)
                    ai_provider_ready = True
                    _set_runtime_attr("AI_FUNCTIONAL_ERROR_MSG", None)
                    AI_FUNCTIONAL_ERROR_MSG = None
                    logging.info("background_scan(): OpenAI Codex OAuth runtime became ready before scan processing started")
                else:
                    _set_runtime_attr("AI_FUNCTIONAL_ERROR_MSG", codex_reason or "OpenAI Codex OAuth runtime is unavailable")
                    AI_FUNCTIONAL_ERROR_MSG = codex_reason or "OpenAI Codex OAuth runtime is unavailable"
                    logging.warning("background_scan(): OpenAI Codex OAuth runtime still unavailable after wait: %s", AI_FUNCTIONAL_ERROR_MSG)
        except Exception as e:
            logging.warning("background_scan(): Codex runtime readiness wait failed: %s", e)

        if not ai_provider_ready:
            # Scans must still run without AI: the pipeline can rely on deterministic/provider signals.
            logging.warning(
                "background_scan(): AI not configured or functional check failed; continuing scan without AI."
            )
            with lock:
                state["scan_ai_preflight_error"] = getattr(_runtime_module(), "AI_FUNCTIONAL_ERROR_MSG", None) or "AI not ready; scan will run without AI."

        # Reset live state. Step-based progress: total_steps = 3*albums + 2 (+1 if auto-move).
        scan_start_epoch = time.time()
        with lock:
            state.update(scanning=True, scan_progress=0, scan_total=total_albums + 2)
            state["scan_type"] = scan_type
            state["scan_auto_trigger"] = scan_auto_trigger
            state["scan_scheduler_run_id"] = scan_scheduler_run_id
            state["scan_resume_run_id"] = resume_run_id
            state["scan_total_albums"] = total_albums
            state["scan_detected_artists_total"] = int(detected_artists_total or 0)
            state["scan_detected_albums_total"] = int(detected_albums_total or 0)
            state["scan_resume_skipped_artists"] = int(resume_skipped_artists or 0)
            state["scan_resume_skipped_albums"] = int(resume_skipped_albums or 0)
            state["scan_run_scope_preparing"] = False
            state["scan_run_scope_stage"] = "done"
            state["scan_run_scope_done"] = int(detected_artists_total or 0)
            state["scan_run_scope_total"] = int(detected_artists_total or 0)
            state["scan_run_scope_artists_included"] = int(total_artists or 0)
            state["scan_run_scope_albums_included"] = int(total_albums or 0)
            state["scan_processed_albums_count"] = 0
            state["scan_published_albums_count"] = 0
            state["scan_postprocessed_albums_count"] = 0
            state["scan_run_scope_started_at"] = state.get("scan_run_scope_started_at") or time.time()
            state["scan_run_scope_updated_at"] = time.time()
            state["scan_tracks_detected_total"] = int(detected_tracks_total or 0)
            state["scan_tracks_library_kept"] = 0
            state["scan_tracks_moved_dupes"] = 0
            state["scan_tracks_moved_incomplete"] = 0
            state["scan_tracks_unaccounted"] = 0
            state["scan_dupe_moved_count"] = 0
            state["scan_dupe_moved_mb"] = 0
            state["scan_step_progress"] = 0
            # scan_step_total set after _reload_auto_move_from_db() so AUTO_MOVE_DUPES is current
            state["duplicates"].clear()
            # Initialize scan details tracking
            state["scan_artists_processed"] = 0
            state["scan_artists_total"] = total_artists
            state["scan_ai_used_count"] = 0
            state["scan_mb_used_count"] = 0
            # Real AI usage counters (calls), independent from the legacy ai_used_count (dupe groups).
            state["scan_ai_calls_total"] = 0
            state["scan_ai_calls_provider_identity"] = 0
            state["scan_ai_calls_mb_verify"] = 0
            state["scan_ai_calls_web_mbid"] = 0
            state["scan_ai_calls_vision"] = 0
            state["scan_ai_enabled"] = ai_provider_ready
            state["scan_ai_guard_calls_used"] = 0
            state["scan_ai_guard_calls_blocked"] = 0
            state["scan_ai_guard_last_reason"] = ""
            state["scan_ai_guard_last_block_at"] = None
            state["scan_mb_enabled"] = USE_MUSICBRAINZ
            # Initialize ETA tracking
            state["scan_start_time"] = scan_start_epoch
            state["scan_last_update_time"] = scan_start_epoch
            state["scan_last_progress"] = 0
            state["scan_format_done_count"] = 0
            state["scan_mb_done_count"] = 0
            state["scan_active_artists"] = {}
            # Initialize cache tracking
            state["scan_audio_cache_hits"] = 0
            state["scan_audio_cache_misses"] = 0
            state["scan_mb_cache_hits"] = 0
            state["scan_mb_cache_misses"] = 0
            # Initialize detailed statistics tracking
            state["scan_duplicate_groups_count"] = 0
            state["scan_total_duplicates_count"] = 0
            state["scan_broken_albums_count"] = 0
            state["scan_missing_albums_count"] = 0
            state["scan_albums_without_artist_image"] = 0
            state["scan_albums_without_album_image"] = 0
            state["scan_albums_without_complete_tags"] = 0
            state["scan_albums_without_mb_id"] = 0
            state["scan_albums_without_artist_mb_id"] = 0
            state["scan_mb_verified_by_ai_count"] = 0
            state["scan_discogs_matched"] = 0
            state["scan_lastfm_matched"] = 0
            state["scan_bandcamp_matched"] = 0
            state["scan_provider_matches"] = {key: 0 for key in _scan_provider_match_keys()}
            # PMDA-level per-scan stats (albums processed/complete, with cover/artist image)
            state["scan_pmda_albums_processed"] = 0
            state["scan_pmda_albums_complete"] = 0
            state["scan_pmda_albums_with_cover"] = 0
            state["scan_pmda_albums_with_artist_image"] = 0
            state["scan_incomplete_moved_count"] = 0
            state["scan_incomplete_moved_mb"] = 0
            state["scan_incomplete_move_running"] = False
            state["scan_incomplete_move_done"] = 0
            state["scan_incomplete_move_total"] = 0
            state["scan_incomplete_move_current_album"] = None
            state["scan_player_sync_target"] = None
            state["scan_player_sync_ok"] = None
            state["scan_player_sync_message"] = ""
            state["scan_ai_errors"] = []
            state["scan_steps_log"] = []  # Per-step log for "what was done" (append after each artist)
            # Aggregate explainable dupe detection stats across the scan (Dupe Detection v2).
            state["scan_dupe_report"] = {
                "version": 2,
                "groups_total": 0,
                "groups_needs_ai": 0,
                "groups_by_signal": {},
                "rejected_by_reason": {},
            }
            state["scan_post_processing"] = False
            state["scan_post_total"] = 0
            state["scan_post_done"] = 0
            state["scan_post_current_artist"] = None
            state["scan_post_current_album"] = None
            state["scan_discovery_running"] = False
            state["scan_discovery_current_root"] = None
            state["scan_discovery_roots_done"] = 0
            state["scan_discovery_roots_total"] = 0
            state["scan_discovery_files_found"] = 0
            state["scan_discovery_folders_found"] = 0
            state["scan_discovery_albums_found"] = 0
            state["scan_discovery_artists_found"] = 0
            state["scan_discovery_stage"] = "idle"
            state["scan_discovery_entries_scanned"] = 0
            state["scan_discovery_root_entries_scanned"] = 0
            state["scan_discovery_folders_done"] = 0
            state["scan_discovery_folders_total"] = 0
            state["scan_discovery_albums_done"] = 0
            state["scan_discovery_albums_total"] = 0
            state["scan_discovery_started_at"] = None
            state["scan_discovery_updated_at"] = time.time()
            # Preflight: store MB and AI connection status for end-of-scan summary
            mb_ok, ai_ok = _run_preflight_checks()
            state["scan_mb_connection_ok"] = mb_ok
            state["scan_ai_connection_ok"] = ai_ok

        if _get_library_mode() == "files" and resume_run_id:
            if resume_plan_restored:
                log_scan(
                    "FILES resume plan snapshot reuse: skipping initial plan rewrite for run %s.",
                    resume_run_id,
                )
            else:
                try:
                    with lock:
                        current_files_editions = dict(state.get("files_editions_by_album_id") or {})
                    _persist_resume_files_plan(
                        resume_run_id,
                        artists_merged,
                        current_files_editions,
                        detected_artists_total=int(detected_artists_total or 0),
                        detected_albums_total=int(detected_albums_total or 0),
                        detected_tracks_total=int(detected_tracks_total or 0),
                    )
                except Exception:
                    logging.debug("Initial resume files plan persist failed for run_id=%s", resume_run_id, exc_info=True)

        # Reload AUTO_MOVE_DUPES from DB so scan uses current setting (UI may have toggled it)
        _reload_auto_move_from_db()
        pipeline_flags_requested = _pipeline_flags_for_scan(scan_type, run_improve_after_requested)
        pipeline_flags = _pipeline_inline_flags(
            pipeline_flags_requested,
            pipeline_async_enabled=bool(pipeline_async_enabled),
        )
        # Files changed-only scans can legitimately produce 0 albums to process (e.g. watcher noise,
        # or a folder that is unchanged+healthy and gets fast-skipped). In that case, skip expensive
        # pipeline steps (export rebuild, player sync, etc.) so the auto background scan stays fast.
        if str(scan_type or "").strip().lower() == "changed_only" and int(total_albums or 0) == 0:
            pipeline_flags_requested = dict(pipeline_flags_requested)
            pipeline_flags = dict(pipeline_flags)
            pipeline_flags_requested.update(
                match_fix=False,
                dedupe=False,
                incomplete_move=False,
                export=False,
                player_sync=False,
                sync_target="none",
            )
            pipeline_flags.update(
                match_fix=False,
                dedupe=False,
                incomplete_move=False,
                export=False,
                player_sync=False,
                sync_target="none",
            )
        with lock:
            extra_steps = 0
            if pipeline_flags.get("dedupe"):
                extra_steps += 1
            if pipeline_flags.get("incomplete_move"):
                extra_steps += 1
            if pipeline_flags.get("export"):
                extra_steps += 1
            if pipeline_flags.get("player_sync"):
                extra_steps += 1
            # Step-based progress: 3 steps per album + AI step + finalize + optional pipeline extras.
            state["scan_step_total"] = 3 * total_albums + 2 + extra_steps
            state["scan_pipeline_flags"] = dict(pipeline_flags_requested)
            state["scan_pipeline_async"] = bool(pipeline_async_enabled)
            state["scan_pipeline_sync_target"] = str(pipeline_flags.get("sync_target") or "none")

        # Create scan history entry
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        cur.execute("PRAGMA table_info(scan_history)")
        scan_cols = [r[1] for r in cur.fetchall()]
        if "entry_type" in scan_cols:
            cur.execute("""
                INSERT INTO scan_history
                (start_time, scan_type, albums_scanned, artists_total, ai_enabled, mb_enabled, auto_move_enabled, status,
                 duplicate_groups_count, total_duplicates_count, broken_albums_count, missing_albums_count,
                 albums_without_artist_image, albums_without_album_image, albums_without_complete_tags,
                 albums_without_mb_id, albums_without_artist_mb_id, entry_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'scan')
            """, (
                scan_start_epoch,
                str(scan_type or "full"),
                total_albums,
                total_artists,
                1 if ai_provider_ready else 0,
                1 if USE_MUSICBRAINZ else 0,
                1 if bool(getattr(_runtime_module(), "AUTO_MOVE_DUPES", False)) else 0,
                'running',
                0, 0, 0, 0, 0, 0, 0, 0, 0  # Initialize all detailed stats to 0
            ))
        else:
            cur.execute("""
                INSERT INTO scan_history
                (start_time, scan_type, albums_scanned, artists_total, ai_enabled, mb_enabled, auto_move_enabled, status,
                 duplicate_groups_count, total_duplicates_count, broken_albums_count, missing_albums_count,
                 albums_without_artist_image, albums_without_album_image, albums_without_complete_tags,
                 albums_without_mb_id, albums_without_artist_mb_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                scan_start_epoch,
                str(scan_type or "full"),
                total_albums,
                total_artists,
                1 if ai_provider_ready else 0,
                1 if USE_MUSICBRAINZ else 0,
                1 if bool(getattr(_runtime_module(), "AUTO_MOVE_DUPES", False)) else 0,
                'running',
                0, 0, 0, 0, 0, 0, 0, 0, 0  # Initialize all detailed stats to 0
            ))
        scan_id = cur.lastrowid
        con.commit()
        con.close()

        # Store scan_id in state for linking moves
        with lock:
            state["scan_id"] = scan_id
        _ai_guard_reset_scan(int(scan_id))
        _set_resume_run_status(resume_run_id, "running", scan_id=scan_id)

        # Clear scan_editions for this scan_id so only the latest run's data is stored
        con = sqlite3.connect(str(STATE_DB_FILE))
        con.execute("DELETE FROM scan_editions WHERE scan_id = ?", (scan_id,))
        con.execute("DELETE FROM scan_pipeline_trace WHERE scan_id = ?", (scan_id,))
        con.commit()
        con.close()

        clear_db_on_new_scan()  # clear only live memory; duplicate review registry stays global

        # Background writer for incremental persist (duplicates + scan_editions + scan_history)
        scan_incremental_queue = Queue()
        scan_incremental_writer_thread = None

        def _scan_incremental_writer():
            while True:
                item = scan_incremental_queue.get()
                if item is None:
                    break
                sid, aname, grps, eds = item
                try:
                    save_scan_artist_to_db(aname, grps)
                    save_scan_editions_artist_to_db(sid, aname, eds)
                    save_scan_pipeline_trace_artist_to_db(sid, aname, eds, grps)
                    logging.debug(
                        "Incremental persist: %s (%d groups, %d editions)",
                        aname, len(grps), len(eds),
                    )
                    with lock:
                        update_scan_history_incremental(
                            sid,
                            artists_processed=state.get("scan_artists_processed", 0),
                            duplicates_found=sum(len(g) for g in state["duplicates"].values()),
                            duplicate_groups_count=state.get("scan_duplicate_groups_count", 0),
                            total_duplicates_count=state.get("scan_total_duplicates_count", 0),
                            broken_albums_count=state.get("scan_broken_albums_count", 0),
                            missing_albums_count=state.get("scan_missing_albums_count", 0),
                            albums_without_artist_image=state.get("scan_albums_without_artist_image", 0),
                            albums_without_album_image=state.get("scan_albums_without_album_image", 0),
                            albums_without_complete_tags=state.get("scan_albums_without_complete_tags", 0),
                            albums_without_mb_id=state.get("scan_albums_without_mb_id", 0),
                            albums_without_artist_mb_id=state.get("scan_albums_without_artist_mb_id", 0),
                        )
                except Exception as e:
                    logging.warning("Incremental scan persist failed for artist %s: %s", aname, e)

        scan_incremental_writer_thread = threading.Thread(target=_scan_incremental_writer, daemon=True)
        scan_incremental_writer_thread.start()

        should_request_improve = bool(pipeline_flags.get("match_fix"))
        if should_request_improve and _get_library_mode() == "files":
            # Files mode: run metadata fix/covers artist-by-artist during the scan so
            # Library updates are progressive instead of "all at end".
            scan_stream_post_by_artist = True
            streamed_post_process_done = True
            scan_post_queue = Queue()
            defer_live_library_materialization = _storage_should_defer_live_library_materialization()
            if defer_live_library_materialization:
                logging.info(
                    "[STORAGE] Deferring matched-library filesystem materialization until after scan completion "
                    "because disk-aware power-saver is active."
                )

            def _scan_postprocess_worker():
                nonlocal defer_live_library_materialization
                providers = ["musicbrainz", "discogs", "lastfm", "bandcamp"]
                by_provider = {p: {"identified": 0, "covers": 0, "tags": 0} for p in providers}
                post_done = 0
                root_dirs = _files_root_dir_strings()
                def _update_edition_after_fix(artist_name: str, item: dict, result: dict) -> None:
                    folder_raw = (item.get("folder") or "").strip()
                    if not folder_raw:
                        return
                    try:
                        folder_path = Path(folder_raw)
                    except Exception:
                        return
                    ordered_paths = _files_collect_ordered_audio_paths(folder_path, item.get("ordered_paths") or [])
                    # Refresh tags from disk for accuracy.
                    tags = dict(item.get("meta") or {})
                    try:
                        if ordered_paths:
                            live_tags = extract_tags(ordered_paths[0]) or {}
                            if live_tags:
                                tags.update(live_tags)
                    except Exception:
                        pass

                    # Snapshot pre-fix health from the publish item so we can update scan counters by delta.
                    pre_missing_required = item.get("pre_missing_required_tags")
                    pre_has_cover = item.get("pre_has_cover")
                    pre_has_artist_image = item.get("pre_has_artist_image")
                    pre_has_mb_id = item.get("pre_has_mb_id")
                    pre_has_artist_mb_id = item.get("pre_has_artist_mb_id")

                    # Compute post-fix health.
                    edition_for_required: dict = {"tracks": list(item.get("tracks") or [])}
                    if not (edition_for_required.get("tracks") or []):
                        derived_tracks = [
                            {"title": p.stem or f"Track {i + 1}", "idx": i + 1}
                            for i, p in enumerate(ordered_paths)
                        ]
                        edition_for_required["tracks"] = derived_tracks
                    try:
                        missing_required_new = _check_required_tags(tags, REQUIRED_TAGS, edition=edition_for_required)
                    except Exception:
                        missing_required_new = []
                    try:
                        has_cover_new = bool(album_folder_has_cover(folder_path))
                    except Exception:
                        has_cover_new = False
                    try:
                        artist_folder = _files_guess_artist_folder(folder_path, artist_name, root_dirs=root_dirs)
                        local_artist_img = _first_artist_image_path(artist_folder) if artist_folder else None
                        if local_artist_img and local_artist_img.is_file():
                            has_artist_image_new = True
                        else:
                            effective_artist_img = _files_effective_artist_image_path(
                                folder_path,
                                artist_name,
                                _norm_artist_key(artist_name),
                            )
                            has_artist_image_new = bool(effective_artist_img and effective_artist_img.is_file())
                    except Exception:
                        has_artist_image_new = False
                    has_mb_id_new = bool(
                        result.get("strict_match_verified")
                        if ("strict_match_verified" in result)
                        else item.get("strict_match_verified")
                    )
                    has_artist_mb_id_new = bool(
                        tags.get("musicbrainz_albumartistid")
                        or tags.get("musicbrainz_artistid")
                        or tags.get("musicbrainz_albumartist_id")
                        or tags.get("musicbrainz_artist_id")
                    )
                    with lock:
                        editions = all_editions_by_artist.get(artist_name) or []
                        for e in editions:
                            if int(e.get("album_id") or 0) != int(item.get("album_id") or 0) and str(e.get("folder") or "") != folder_raw:
                                continue
                            e["meta"] = tags
                            if ordered_paths:
                                e["ordered_paths"] = [str(p) for p in ordered_paths]
                            if result.get("musicbrainz_id"):
                                e["musicbrainz_id"] = result.get("musicbrainz_id")
                            if result.get("provider_used"):
                                e["primary_metadata_source"] = result.get("provider_used")
                                e["metadata_source"] = result.get("provider_used")
                            if result.get("discogs_release_id"):
                                e["discogs_release_id"] = result.get("discogs_release_id")
                            if result.get("lastfm_album_mbid"):
                                e["lastfm_album_mbid"] = result.get("lastfm_album_mbid")
                            if result.get("bandcamp_album_url"):
                                e["bandcamp_album_url"] = result.get("bandcamp_album_url")
                            if "strict_match_verified" in result:
                                e["strict_match_verified"] = bool(result.get("strict_match_verified"))
                            if "strict_match_provider" in result:
                                e["strict_match_provider"] = _normalize_identity_provider(
                                    str(result.get("strict_match_provider") or "")
                                )
                            if "strict_reject_reason" in result:
                                e["strict_reject_reason"] = str(result.get("strict_reject_reason") or "").strip()
                            if "strict_tracklist_score" in result:
                                try:
                                    e["strict_tracklist_score"] = float(result.get("strict_tracklist_score") or 0.0)
                                except Exception:
                                    e["strict_tracklist_score"] = 0.0
                            _apply_resolved_identity_to_edition(
                                e,
                                default_artist=str(artist_name or ""),
                                default_title=str(item.get("album_title") or item.get("title_raw") or ""),
                                folder_name=folder_path.name,
                            )
                            e["missing_required_tags"] = list(missing_required_new or [])
                            e["has_cover"] = bool(has_cover_new)
                            e["has_artist_image"] = bool(has_artist_image_new)

                            # Delta-adjust scan health counters so live stats reflect post-fix state.
                            try:
                                if pre_missing_required is not None:
                                    old_missing = bool(pre_missing_required)
                                    new_missing = bool(missing_required_new)
                                    if old_missing != new_missing:
                                        state["scan_albums_without_complete_tags"] = max(
                                            0,
                                            int(state.get("scan_albums_without_complete_tags", 0)) + (-1 if old_missing else 1),
                                        )
                                if pre_has_cover is not None:
                                    old_without = not bool(pre_has_cover)
                                    new_without = not bool(has_cover_new)
                                    if old_without != new_without:
                                        state["scan_albums_without_album_image"] = max(
                                            0,
                                            int(state.get("scan_albums_without_album_image", 0)) + (-1 if old_without else 1),
                                        )
                                if pre_has_artist_image is not None:
                                    old_without = not bool(pre_has_artist_image)
                                    new_without = not bool(has_artist_image_new)
                                    if old_without != new_without:
                                        state["scan_albums_without_artist_image"] = max(
                                            0,
                                            int(state.get("scan_albums_without_artist_image", 0)) + (-1 if old_without else 1),
                                        )
                                if pre_has_mb_id is not None:
                                    old_without = not bool(pre_has_mb_id)
                                    new_without = not bool(has_mb_id_new)
                                    if old_without != new_without:
                                        state["scan_albums_without_mb_id"] = max(
                                            0,
                                            int(state.get("scan_albums_without_mb_id", 0)) + (-1 if old_without else 1),
                                        )
                                if pre_has_artist_mb_id is not None:
                                    old_without = not bool(pre_has_artist_mb_id)
                                    new_without = not bool(has_artist_mb_id_new)
                                    if old_without != new_without:
                                        state["scan_albums_without_artist_mb_id"] = max(
                                            0,
                                            int(state.get("scan_albums_without_artist_mb_id", 0)) + (-1 if old_without else 1),
                                        )
                            except Exception:
                                logging.debug("Post-process counter delta update failed", exc_info=True)
                            break
                    _apply_resolved_identity_to_edition(
                        item,
                        default_artist=str(artist_name or ""),
                        default_title=str(item.get("album_title") or item.get("title_raw") or ""),
                        folder_name=folder_path.name,
                    )
                    item["meta"] = tags
                with lock:
                    state["scan_post_processing"] = True
                    state["scan_post_total"] = total_albums
                    state["scan_post_done"] = 0
                    state["scan_post_current_artist"] = None
                    state["scan_post_current_album"] = None
                while True:
                    payload = scan_post_queue.get()
                    if payload is None:
                        scan_post_queue.task_done()
                        break
                    artist_name_for_batch, items = payload
                    items, _filtered_missing = _filter_existing_files_album_items(
                        items,
                        context="scan_postprocess_worker",
                        artist_name=artist_name_for_batch,
                    )
                    batch_results: dict[int, dict] = {}
                    try:
                        for item in items:
                            if scan_should_stop.is_set():
                                break
                            try:
                                _idx, album_id, album_title, artist_name, result, _steps = _improve_one_album_item(item)
                            except Exception as post_err:
                                logging.warning("Post-process worker failed for artist %s item: %s", artist_name_for_batch, post_err)
                                continue
                            try:
                                _update_edition_after_fix(artist_name, item, result or {})
                            except Exception:
                                logging.debug("Post-process edition refresh failed for %s", artist_name, exc_info=True)
                            try:
                                batch_results[int(album_id)] = dict(result or {})
                            except Exception:
                                pass

                            prov = result.get("provider_used") or "musicbrainz"
                            if prov in by_provider:
                                if result.get("tags_updated") or result.get("cover_saved"):
                                    by_provider[prov]["identified"] += 1
                                if result.get("cover_saved"):
                                    by_provider[prov]["covers"] += 1
                                if result.get("tags_updated"):
                                    by_provider[prov]["tags"] += 1

                            post_done += 1
                            with lock:
                                state["scan_post_done"] = post_done
                                state["scan_postprocessed_albums_count"] = int(state.get("scan_postprocessed_albums_count") or 0) + 1
                                state["scan_post_current_artist"] = artist_name
                                state["scan_post_current_album"] = album_title
                                if result.get("pmda_matched") or result.get("pmda_cover") or result.get("pmda_artist_image"):
                                    state["scan_pmda_albums_processed"] = state.get("scan_pmda_albums_processed", 0) + 1
                                if result.get("pmda_cover"):
                                    state["scan_pmda_albums_with_cover"] = state.get("scan_pmda_albums_with_cover", 0) + 1
                                if result.get("pmda_artist_image"):
                                    state["scan_pmda_albums_with_artist_image"] = state.get("scan_pmda_albums_with_artist_image", 0) + 1
                                if result.get("pmda_complete"):
                                    state["scan_pmda_albums_complete"] = state.get("scan_pmda_albums_complete", 0) + 1
                                step_log = state.get("scan_steps_log") or []
                                step_log.append(
                                    f"[post] {artist_name} — {album_title}: "
                                    f"{'tags' if result.get('tags_updated') else 'no-tags'} / "
                                    f"{'cover' if result.get('cover_saved') else 'no-cover'}"
                                )
                                if len(step_log) > 200:
                                    step_log = step_log[-200:]
                                state["scan_steps_log"] = step_log
                            if _get_library_mode() == "files":
                                _refresh_files_album_scan_cache_from_editions(
                                    [
                                        {
                                            "folder": item.get("folder"),
                                            "artist": artist_name,
                                            "artist_name": artist_name,
                                            "title_raw": album_title,
                                            "album_title": album_title,
                                            "musicbrainz_id": item.get("musicbrainz_id") or result.get("musicbrainz_id"),
                                            "meta": item.get("meta") or {},
                                            "tracks": item.get("tracks") or [],
                                            "ordered_paths": item.get("ordered_paths") or [],
                                            "fingerprint": item.get("fingerprint") or "",
                                            "provider_used": result.get("provider_used"),
                                            "metadata_source": (
                                                result.get("provider_used")
                                                or result.get("pmda_match_provider")
                                                or item.get("metadata_source")
                                                or item.get("primary_metadata_source")
                                                or ""
                                            ),
                                            "identity_provider": (
                                                result.get("provider_used")
                                                or result.get("pmda_match_provider")
                                                or item.get("metadata_source")
                                                or item.get("primary_metadata_source")
                                                or ""
                                            ),
                                            "discogs_release_id": (
                                                result.get("discogs_release_id")
                                                or item.get("discogs_release_id")
                                                or ""
                                            ),
                                            "lastfm_album_mbid": (
                                                result.get("lastfm_album_mbid")
                                                or item.get("lastfm_album_mbid")
                                                or ""
                                            ),
                                            "bandcamp_album_url": (
                                                result.get("bandcamp_album_url")
                                                or item.get("bandcamp_album_url")
                                                or ""
                                            ),
                                        }
                                    ],
                                    scan_id=scan_id,
                                )
                    finally:
                        if _get_library_mode() == "files":
                            if not _storage_should_defer_live_library_materialization():
                                try:
                                    items = _move_publish_items_to_matched_library(
                                        artist_name_for_batch,
                                        items,
                                    )
                                except Exception:
                                    logging.exception(
                                        "Matched-library move failed for artist %s in post worker",
                                        artist_name_for_batch,
                                    )
                            elif not defer_live_library_materialization:
                                defer_live_library_materialization = True
                                logging.info(
                                    "[STORAGE] Deferring matched-library filesystem materialization until after scan completion "
                                    "because disk-aware power-saver became active mid-scan."
                                )
                            try:
                                def _on_publish_batch(*, inserted: int, batch_index: int, total_batches: int, batch_size: int) -> None:
                                    with lock:
                                        state["scan_published_albums_count"] = int(state.get("scan_published_albums_count") or 0) + int(inserted or 0)
                                        if int(total_batches or 0) > 1:
                                            step_log = state.get("scan_steps_log") or []
                                            step_log.append(
                                                f"[publish] {artist_name_for_batch}: batch {batch_index}/{total_batches} published {int(inserted or 0)} album(s)"
                                            )
                                            if len(step_log) > 200:
                                                step_log = step_log[-200:]
                                            state["scan_steps_log"] = step_log

                                publish_summary = _publish_files_library_artist_live_batches(
                                    artist_name_for_batch,
                                    items,
                                    scan_id=scan_id,
                                    results_by_album_id=batch_results,
                                    on_batch=_on_publish_batch,
                                )
                                published_now = int((publish_summary or {}).get("published") or 0)
                            except Exception:
                                logging.debug(
                                    "Files publication failed for artist %s in post worker",
                                    artist_name_for_batch,
                                    exc_info=True,
                                )
                            rebuild_reason = f"scan_artist_ready_{artist_name_for_batch}"
                            try:
                                res = _rebuild_files_library_index_for_artist(
                                    artist_name_for_batch,
                                    reason=rebuild_reason,
                                    wait_if_running=False,
                                )
                                if not res.get("ok"):
                                    logging.debug(
                                        "Files artist index sync returned non-ok for %s in post worker: %s",
                                        artist_name_for_batch,
                                        res.get("error") or res,
                                    )
                            except Exception:
                                logging.debug(
                                    "Files artist index sync failed for %s",
                                    artist_name_for_batch,
                                    exc_info=True,
                                )
                        scan_post_queue.task_done()

                with lock:
                    state["scan_post_processing"] = False
                    state["scan_post_current_artist"] = None
                    state["scan_post_current_album"] = None
                    state["last_fix_all_by_provider"] = by_provider
                    state["last_fix_all_total_albums"] = post_done

            scan_post_worker_thread = threading.Thread(target=_scan_postprocess_worker, daemon=True)
            scan_post_worker_thread.start()

        scan_heartbeat_interval_sec = 8.0
        scan_heartbeat_idle_interval_sec = 60.0
        last_scan_heartbeat_at = 0.0
        last_scan_heartbeat_signature: tuple[Any, ...] | None = None
        last_scan_heartbeat_throughput_per_hour = 0.0
        last_scan_progress_changed_at = 0.0

        def _format_scan_idle_duration(seconds: float) -> str:
            total = max(0, int(seconds or 0))
            hours, rem = divmod(total, 3600)
            minutes, secs = divmod(rem, 60)
            if hours > 0:
                return f"{hours}h {minutes:02d}m"
            if minutes > 0:
                return f"{minutes}m {secs:02d}s"
            return f"{secs}s"

        def _emit_scan_progress_heartbeat(*, reason: str, force: bool = False) -> None:
            nonlocal last_scan_heartbeat_at
            nonlocal last_scan_heartbeat_signature
            nonlocal last_scan_heartbeat_throughput_per_hour
            nonlocal last_scan_progress_changed_at
            now_mono = time.monotonic()
            if not force and (now_mono - last_scan_heartbeat_at) < scan_heartbeat_interval_sec:
                return
            with lock:
                active_snapshot = dict(state.get("scan_active_artists") or {})
                artists_done = int(state.get("scan_artists_processed") or 0)
                artists_total_live = int(state.get("scan_artists_total") or total_artists or 0)
                albums_done = int(state.get("scan_processed_albums_count") or 0)
                albums_total_live = int(state.get("scan_total_albums") or total_albums or 0)
                mb_done_live = int(state.get("scan_mb_done_count") or 0)
                provider_matches_live = dict(state.get("scan_provider_matches") or {})
            active_preview: list[str] = []
            active_blockers: list[str] = []
            partial_done = 0
            active_started = 0
            for artist_name_live, info_live in list(active_snapshot.items())[:4]:
                if str(artist_name_live or "").startswith("_") or not isinstance(info_live, dict):
                    continue
                current_album_live = info_live.get("current_album") if isinstance(info_live, dict) else None
                albums_processed_live = max(0, int(info_live.get("albums_processed") or 0))
                total_artist_albums_live = max(albums_processed_live, int(info_live.get("total_albums") or 0))
                current_album_index_live = 0
                current_album_total_live = total_artist_albums_live
                if isinstance(current_album_live, dict):
                    try:
                        current_album_index_live = max(0, int(current_album_live.get("album_index") or 0))
                    except Exception:
                        current_album_index_live = 0
                    try:
                        current_album_total_live = max(current_album_total_live, int(current_album_live.get("album_total") or 0))
                    except Exception:
                        current_album_total_live = total_artist_albums_live
                preview_done = albums_processed_live
                if current_album_index_live > 0:
                    preview_done = max(preview_done, max(0, current_album_index_live - 1))
                if albums_processed_live > 0 or current_album_live:
                    active_started += 1
                active_preview.append(f"{artist_name_live} {preview_done}/{current_album_total_live}")
                partial_done += preview_done
                if isinstance(current_album_live, dict):
                    detail_text = str(
                        current_album_live.get("step_response")
                        or current_album_live.get("step_summary")
                        or current_album_live.get("status_details")
                        or current_album_live.get("status")
                        or ""
                    ).strip()
                    if detail_text:
                        detail_text = re.sub(r"\s+", " ", detail_text)
                        active_blockers.append(
                            f"{artist_name_live} {max(1, current_album_index_live or (preview_done + 1))}/{max(1, current_album_total_live)}: {detail_text[:140]}"
                        )
            artists_advanced = max(artists_done, min(artists_total_live or max(artists_done + active_started, 0), artists_done + active_started))
            albums_advanced = max(albums_done, min(albums_total_live or max(albums_done + partial_done, 0), albums_done + partial_done))
            provider_summary_parts = [
                f"{provider}={int(provider_matches_live.get(provider) or 0)}"
                for provider in _scan_provider_match_keys(tuple(provider_matches_live.keys()))
                if int(provider_matches_live.get(provider) or 0) > 0
            ]
            throughput_album_proxy = max(0, albums_advanced)
            elapsed_live = max(0.0, time.time() - float(scan_start_epoch or time.time()))
            signature = (
                int(artists_done),
                int(artists_advanced),
                int(albums_done),
                int(albums_advanced),
                int(mb_done_live),
                tuple(sorted((str(k), int(v or 0)) for k, v in provider_matches_live.items() if int(v or 0) > 0)),
                tuple(active_preview),
            )
            progress_changed = signature != last_scan_heartbeat_signature
            if not progress_changed and not force and (now_mono - last_scan_heartbeat_at) < scan_heartbeat_idle_interval_sec:
                return
            if progress_changed:
                throughput_per_hour = ((throughput_album_proxy * 3600.0) / elapsed_live) if elapsed_live > 0 and throughput_album_proxy > 0 else 0.0
                last_scan_heartbeat_throughput_per_hour = throughput_per_hour
                last_scan_progress_changed_at = now_mono
            else:
                throughput_per_hour = float(last_scan_heartbeat_throughput_per_hour or 0.0)
            if last_scan_progress_changed_at <= 0.0:
                last_scan_progress_changed_at = now_mono
            idle_for_sec = max(0.0, now_mono - float(last_scan_progress_changed_at or now_mono))
            log_scan(
                "Heartbeat (%s): artists %d committed / %d advanced / %d total, albums %d committed / %d advanced / %d total, mb %d/%d, active=%d%s, throughput %.1f/h%s%s%s",
                reason,
                artists_done,
                artists_advanced,
                artists_total_live,
                albums_done,
                albums_advanced,
                albums_total_live,
                mb_done_live,
                albums_total_live,
                len([k for k in active_snapshot.keys() if not str(k or '').startswith('_')]),
                f" [{'; '.join(active_preview)}]" if active_preview else "",
                throughput_per_hour,
                f", matches {', '.join(provider_summary_parts)}" if provider_summary_parts else "",
                "" if progress_changed or force else ", idle " + _format_scan_idle_duration(idle_for_sec),
                "" if progress_changed or force or not active_blockers else ", blockers " + " | ".join(active_blockers[:2]),
            )
            last_scan_heartbeat_signature = signature
            last_scan_heartbeat_at = now_mono

        futures: set[concurrent.futures.Future] = set()
        import concurrent.futures
        future_to_albums: dict[concurrent.futures.Future, int] = {}
        future_to_artist: dict[concurrent.futures.Future, str] = {}
        future_to_album_ids: dict[concurrent.futures.Future, list[int]] = {}
        future_to_storage_bucket: dict[concurrent.futures.Future, dict[str, Any]] = {}

        def _scan_row_storage_meta(artist_row: tuple[int, str, list[int]]) -> dict[str, Any]:
            try:
                _primary_id, _artist_name, album_ids_list = artist_row
                if not album_ids_list:
                    return {}
                with lock:
                    fe = dict((state.get("files_editions_by_album_id") or {}).get(int(album_ids_list[0])) or {})
                provider = str(fe.get("storage_provider") or "").strip()
                device_id = str(fe.get("storage_device_id") or "").strip()
                if not provider or not device_id:
                    return {}
                return {
                    "storage_provider": provider,
                    "storage_device_id": device_id,
                    "storage_device_label": str(fe.get("storage_device_label") or device_id).strip() or device_id,
                    "storage_bucket_order": int(fe.get("storage_bucket_order") or 0),
                    "albums_total": len(album_ids_list),
                }
            except Exception:
                return {}

        storage_scheduler_enabled = bool(_get_library_mode() == "files" and state.get("storage_power_saver_enabled") and state.get("storage_scan_plan"))

        def _storage_bucket_key(meta: dict[str, Any] | None) -> tuple[int, str] | None:
            if not storage_scheduler_enabled or not meta:
                return None
            return (int(meta.get("storage_bucket_order") or 0), str(meta.get("storage_device_id") or ""))

        def _storage_mark_bucket_active(meta: dict[str, Any] | None) -> None:
            if not storage_scheduler_enabled or not meta:
                return
            now_ts = time.time()
            with lock:
                current = state.get("storage_current_bucket") if isinstance(state.get("storage_current_bucket"), dict) else {}
                current_key = (
                    int((current or {}).get("storage_bucket_order") or -1),
                    str((current or {}).get("storage_device_id") or ""),
                )
                next_key = (int(meta.get("storage_bucket_order") or 0), str(meta.get("storage_device_id") or ""))
                if current_key == next_key:
                    return
                plan = list(state.get("storage_scan_plan") or [])
                total_for_bucket = 0
                for item in plan:
                    if int(item.get("storage_bucket_order") or 0) == next_key[0] and str(item.get("storage_device_id") or "") == next_key[1]:
                        item["status"] = "running"
                        item["started_at"] = item.get("started_at") or now_ts
                        total_for_bucket += int(item.get("albums_total") or 0)
                    elif str(item.get("status") or "") == "running":
                        item["status"] = "done"
                        item["finished_at"] = item.get("finished_at") or now_ts
                state["storage_scan_plan"] = plan
                state["storage_current_bucket"] = dict(meta)
                state["storage_current_device_id"] = str(meta.get("storage_device_id") or "")
                state["storage_current_device_label"] = str(meta.get("storage_device_label") or meta.get("storage_device_id") or "")
                state["storage_bucket_done"] = 0
                state["storage_bucket_total"] = int(total_for_bucket or meta.get("albums_total") or 0)
                state["storage_bucket_started_at"] = now_ts
                state["storage_active_devices"] = 1
                state["storage_estimated_watts_saved"] = _storage_estimated_watts_saved(1, int(state.get("storage_devices_total") or 0))
            log_scan(
                "[STORAGE] Bucket start: %s (%s), albums=%d, active disks 1/%d.",
                str(meta.get("storage_device_label") or meta.get("storage_device_id") or ""),
                str(meta.get("storage_device_id") or ""),
                int(total_for_bucket or meta.get("albums_total") or 0),
                int(state.get("storage_devices_total") or 0),
            )
            _update_scan_storage_bucket_row(
                resume_run_id,
                meta,
                status="running",
                started_at=now_ts,
                message="bucket active",
            )

        def _storage_mark_bucket_progress(meta: dict[str, Any] | None, album_count: int) -> None:
            if not storage_scheduler_enabled or not meta:
                return
            updated_done: int | None = None
            with lock:
                key = (int(meta.get("storage_bucket_order") or 0), str(meta.get("storage_device_id") or ""))
                if state.get("storage_current_bucket"):
                    state["storage_bucket_done"] = int(state.get("storage_bucket_done") or 0) + int(album_count or 0)
                plan = list(state.get("storage_scan_plan") or [])
                for item in plan:
                    if int(item.get("storage_bucket_order") or 0) == key[0] and str(item.get("storage_device_id") or "") == key[1]:
                        item["albums_done"] = min(int(item.get("albums_total") or 0), int(item.get("albums_done") or 0) + int(album_count or 0))
                        item["status"] = "running"
                        updated_done = int(item.get("albums_done") or 0)
                        break
                state["storage_scan_plan"] = plan
            if updated_done is not None:
                _update_scan_storage_bucket_row(
                    resume_run_id,
                    meta,
                    status="running",
                    albums_done=updated_done,
                )

        def _storage_mark_current_bucket_done() -> None:
            if not storage_scheduler_enabled:
                return
            now_ts = time.time()
            with lock:
                current = dict(state.get("storage_current_bucket") or {})
                if not current:
                    return
                key = (int(current.get("storage_bucket_order") or 0), str(current.get("storage_device_id") or ""))
                done = int(state.get("storage_bucket_done") or 0)
                total = int(state.get("storage_bucket_total") or 0)
                plan = list(state.get("storage_scan_plan") or [])
                for item in plan:
                    if int(item.get("storage_bucket_order") or 0) == key[0] and str(item.get("storage_device_id") or "") == key[1]:
                        item["albums_done"] = max(int(item.get("albums_done") or 0), done)
                        item["finished_at"] = now_ts
                        item["status"] = "done"
                        break
                history = list(state.get("storage_bucket_history") or [])
                history.append(
                    {
                        **current,
                        "albums_done": done,
                        "albums_total": total,
                        "started_at": state.get("storage_bucket_started_at"),
                        "finished_at": now_ts,
                    }
                )
                state["storage_bucket_history"] = history[-100:]
                state["storage_scan_plan"] = plan
                state["storage_buckets_done"] = min(int(state.get("storage_buckets_total") or 0), int(state.get("storage_buckets_done") or 0) + 1)
                state["storage_current_bucket"] = None
                state["storage_current_device_id"] = None
                state["storage_current_device_label"] = None
                state["storage_bucket_done"] = 0
                state["storage_bucket_total"] = 0
                bucket_started_at = state.get("storage_bucket_started_at")
                watts_saved = float(state.get("storage_estimated_watts_saved") or 0.0)
            try:
                bucket_elapsed = max(0.0, float(now_ts) - float(bucket_started_at or now_ts))
            except Exception:
                bucket_elapsed = 0.0
            cost_saved = _storage_estimated_cost_saved_eur(watts_saved, bucket_elapsed)
            log_scan(
                "[STORAGE] Bucket end: %s (%s), albums %d/%d, active %.1f min, estimated avoided load %.1f W / €%.4f.",
                str(current.get("storage_device_label") or current.get("storage_device_id") or ""),
                str(current.get("storage_device_id") or ""),
                done,
                total,
                bucket_elapsed / 60.0,
                watts_saved,
                cost_saved,
            )
            _update_scan_storage_bucket_row(
                resume_run_id,
                current,
                status="done",
                albums_done=done,
                finished_at=now_ts,
                message="bucket completed",
            )

        def _submit_scan_artist(
            executor: ThreadPoolExecutor,
            artist_row: tuple[int, str, list[int]],
        ) -> concurrent.futures.Future:
            primary_id, artist_name, album_ids_list = artist_row
            album_cnt = len(album_ids_list)
            with lock:
                state["scan_active_artists"][artist_name] = {
                    "start_time": time.time(),
                    "total_albums": album_cnt,
                    "albums_processed": 0,
                }
            fut = executor.submit(scan_artist_duplicates, (primary_id, artist_name, album_ids_list))
            future_to_albums[fut] = album_cnt
            future_to_artist[fut] = artist_name
            future_to_album_ids[fut] = list(album_ids_list or [])
            future_to_storage_bucket[fut] = _scan_row_storage_meta(artist_row)
            return fut

        with ThreadPoolExecutor(max_workers=SCAN_THREADS) as executor:
            max_inflight = max(1, int(SCAN_THREADS or 1))
            artist_queue = list(artists_merged)
            artist_index = 0
            current_storage_bucket_key: tuple[int, str] | None = None

            def _fill_worker_slots() -> None:
                nonlocal artist_index, current_storage_bucket_key
                if artist_index >= len(artist_queue):
                    return
                if storage_scheduler_enabled and current_storage_bucket_key is None:
                    meta = _scan_row_storage_meta(artist_queue[artist_index])
                    current_storage_bucket_key = _storage_bucket_key(meta)
                    _storage_mark_bucket_active(meta)
                while len(futures) < max_inflight and artist_index < len(artist_queue):
                    row = artist_queue[artist_index]
                    meta = _scan_row_storage_meta(row)
                    row_key = _storage_bucket_key(meta)
                    if storage_scheduler_enabled and current_storage_bucket_key is not None and row_key != current_storage_bucket_key:
                        break
                    futures.add(_submit_scan_artist(executor, row))
                    artist_index += 1

            _fill_worker_slots()

            if futures:
                log_scan(
                    "FILES worker queue primed: %d/%d artist worker slot(s) submitted immediately.",
                    len(futures),
                    max_inflight,
                )
                _emit_scan_progress_heartbeat(reason="queue_primed", force=True)

            artists_processed = 0
            while futures:
                done, _pending = concurrent.futures.wait(
                    futures,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                    timeout=5.0,
                )
                if not done:
                    _emit_scan_progress_heartbeat(reason="waiting_on_workers")
                    continue
                # Allow stop/pause mid‑scan
                if scan_should_stop.is_set():
                    break
                for future in done:
                    futures.discard(future)
                    album_cnt = future_to_albums.pop(future, 0)
                    artist_name = future_to_artist.pop(future, "<unknown>")
                    artist_album_ids = list(future_to_album_ids.pop(future, []) or [])
                    storage_bucket_meta = dict(future_to_storage_bucket.pop(future, {}) or {})
                    stats = {"ai_used": 0, "mb_used": 0}
                    artist_failed = False
                    artist_error = None
                    try:
                        result = future.result()
                        if len(result) == 5:
                            artist_name, groups, _, stats, all_editions = result
                            all_editions_by_artist.setdefault(artist_name, []).extend(list(all_editions or []))
                        elif len(result) == 4:
                            artist_name, groups, _, stats = result
                            all_editions_by_artist.setdefault(artist_name, [])
                        else:
                            # Backward compatibility: old format without stats
                            artist_name, groups, _ = result
                            stats = {"ai_used": 0, "mb_used": 0}
                            all_editions_by_artist.setdefault(artist_name, [])
                    except Exception as e:
                        logging.exception("Worker crash for artist %s: %s", artist_name, e)
                        worker_errors.put((artist_name, str(e)))
                        artist_failed = True
                        artist_error = str(e)
                        groups = []
                        stats = {"ai_used": 0, "mb_used": 0}
                        all_editions_by_artist.setdefault(artist_name, [])
                    finally:
                        with lock:
                            state["scan_progress"] += album_cnt
                            state["scan_step_progress"] = state.get("scan_step_progress", 0) + album_cnt  # compare step: 1 per album
                            state["scan_artists_processed"] += 1
                            if not artist_failed:
                                state["scan_processed_albums_count"] = int(state.get("scan_processed_albums_count") or 0) + int(album_cnt or 0)
                            state["scan_ai_used_count"] += stats.get("ai_used", 0)
                            state["scan_mb_used_count"] += stats.get("mb_used", 0)
                            state["scan_audio_cache_hits"] += stats.get("audio_cache_hits", 0)
                            state["scan_audio_cache_misses"] += stats.get("audio_cache_misses", 0)
                            state["scan_mb_cache_hits"] += stats.get("mb_cache_hits", 0)
                            state["scan_mb_cache_misses"] += stats.get("mb_cache_misses", 0)
                            # Aggregate detailed statistics
                            state["scan_duplicate_groups_count"] += stats.get("duplicate_groups_count", 0)
                            state["scan_total_duplicates_count"] += stats.get("total_duplicates_count", 0)
                            state["scan_broken_albums_count"] += stats.get("broken_albums_count", 0)
                            state["scan_albums_without_mb_id"] += stats.get("albums_without_mb_id", 0)
                            state["scan_albums_without_artist_mb_id"] += stats.get("albums_without_artist_mb_id", 0)
                            state["scan_mb_verified_by_ai_count"] += stats.get("mb_verified_by_ai", 0)
                            state["scan_albums_without_complete_tags"] += stats.get("albums_without_complete_tags", 0)
                            state["scan_albums_without_album_image"] += stats.get("albums_without_album_image", 0)
                            state["scan_albums_without_artist_image"] += stats.get("albums_without_artist_image", 0)
                            # Merge dupe_report (per-artist) into a scan-level report for comparisons between builds.
                            dr = stats.get("dupe_report")
                            if isinstance(dr, dict):
                                agg = state.get("scan_dupe_report")
                                if not isinstance(agg, dict):
                                    agg = {
                                        "version": 2,
                                        "groups_total": 0,
                                        "groups_needs_ai": 0,
                                        "groups_by_signal": {},
                                        "rejected_by_reason": {},
                                    }
                                try:
                                    agg["version"] = int(agg.get("version") or dr.get("version") or 2)
                                except Exception:
                                    agg["version"] = 2
                                agg["groups_total"] = int(agg.get("groups_total") or 0) + int(dr.get("groups_total") or 0)
                                agg["groups_needs_ai"] = int(agg.get("groups_needs_ai") or 0) + int(dr.get("groups_needs_ai") or 0)
                                for bucket in ("groups_by_signal", "rejected_by_reason"):
                                    dst = agg.get(bucket)
                                    if not isinstance(dst, dict):
                                        dst = {}
                                        agg[bucket] = dst
                                    src = dr.get(bucket) or {}
                                    if isinstance(src, dict):
                                        for k, v in src.items():
                                            key = str(k)
                                            try:
                                                dst[key] = int(dst.get(key) or 0) + int(v or 0)
                                            except Exception:
                                                continue
                                state["scan_dupe_report"] = agg
                            # Remove artist from active tracking when done
                            if artist_name in state.get("scan_active_artists", {}):
                                del state["scan_active_artists"][artist_name]
                            # Append one line to steps log for this artist (bounded to avoid unbounded growth)
                            step_log = state.get("scan_steps_log") or []
                            n_albums = album_cnt
                            n_grps = stats.get("duplicate_groups_count", 0)
                            n_broken = stats.get("broken_albums_count", 0)
                            n_mb = max(0, n_albums - stats.get("albums_without_mb_id", 0))
                            step_log.append(
                                f"{artist_name}: {n_albums} albums, strict matched {n_mb}, broken {n_broken}, duplicate groups {n_grps}"
                            )
                            # Keep only the latest 200 entries so JSON payloads and DB rows stay small
                            if len(step_log) > 200:
                                step_log = step_log[-200:]
                            state["scan_steps_log"] = step_log
                            if groups:
                                all_results.setdefault(artist_name, []).extend(list(groups or []))
                                state["duplicates"][artist_name] = all_results.get(artist_name, [])
                            # Enqueue for incremental persist (duplicates + scan_editions + scan_history)
                            if scan_incremental_queue is not None:
                                scan_incremental_queue.put(
                                    (scan_id, artist_name, groups, all_editions_by_artist.get(artist_name, []))
                                )
                        _storage_mark_bucket_progress(storage_bucket_meta, album_cnt)
                        publish_items: list[dict] = []
                        try:
                            if _get_library_mode() == "files":
                                _refresh_files_album_scan_cache_from_editions(
                                    all_editions_by_artist.get(artist_name, []),
                                    scan_id=scan_id,
                                )
                                publish_items = _build_improve_items_from_editions(
                                    artist_name,
                                    all_editions_by_artist.get(artist_name, []),
                                    groups,
                                )
                            _set_resume_artist_status(
                                resume_run_id,
                                artist_name,
                                "failed" if artist_failed else "done",
                                error=artist_error,
                            )
                            if _get_library_mode() == "files" and not artist_failed:
                                if artist_album_ids:
                                    with lock:
                                        files_editions_live = state.get("files_editions_by_album_id") or {}
                                        for _album_id_done in artist_album_ids:
                                            files_editions_live.pop(_album_id_done, None)
                                if storage_scheduler_enabled:
                                    _prune_resume_files_plan_albums(resume_run_id, artist_album_ids)
                                else:
                                    _prune_resume_files_plan_artist(resume_run_id, artist_name)
                            if scan_stream_post_by_artist and scan_post_queue is not None:
                                if publish_items:
                                    scan_post_queue.put((artist_name, publish_items))
                            elif _get_library_mode() == "files" and publish_items:
                                if not _storage_should_defer_live_library_materialization():
                                    try:
                                        publish_items = _move_publish_items_to_matched_library(
                                            artist_name,
                                            publish_items,
                                        )
                                    except Exception:
                                        logging.exception(
                                            "Matched-library move failed for artist %s",
                                            artist_name,
                                        )
                                try:
                                    def _on_publish_batch(*, inserted: int, batch_index: int, total_batches: int, batch_size: int) -> None:
                                        with lock:
                                            state["scan_published_albums_count"] = int(
                                                state.get("scan_published_albums_count") or 0
                                            ) + int(inserted or 0)
                                            if int(total_batches or 0) > 1:
                                                step_log = state.get("scan_steps_log") or []
                                                step_log.append(
                                                    f"[publish] {artist_name}: batch {batch_index}/{total_batches} published {int(inserted or 0)} album(s)"
                                                )
                                                if len(step_log) > 200:
                                                    step_log = step_log[-200:]
                                                state["scan_steps_log"] = step_log

                                    publish_summary = _publish_files_library_artist_live_batches(
                                        artist_name,
                                        publish_items,
                                        scan_id=scan_id,
                                        results_by_album_id={},
                                        on_batch=_on_publish_batch,
                                    )
                                    published_now = int((publish_summary or {}).get("published") or 0)
                                except Exception:
                                    logging.debug("Files publication failed for artist %s", artist_name, exc_info=True)
                                rebuild_reason = f"scan_artist_ready_{artist_name}"
                                try:
                                    res = _rebuild_files_library_index_for_artist(
                                        artist_name,
                                        reason=rebuild_reason,
                                        wait_if_running=False,
                                    )
                                    if not res.get("ok"):
                                        logging.debug(
                                            "Files artist index sync returned non-ok for %s: %s",
                                            artist_name,
                                            res.get("error") or res,
                                        )
                                except Exception:
                                    logging.debug("Files artist index sync failed for %s", artist_name, exc_info=True)
                        except Exception as artist_followup_err:
                            logging.exception(
                                "[Scan Pipeline] artist follow-up failed for %s: %s",
                                artist_name,
                                artist_followup_err,
                            )
                            try:
                                worker_errors.put((artist_name, f"followup: {artist_followup_err}"))
                            except Exception:
                                pass
                        finally:
                            artists_processed += 1
                            if _get_library_mode() == "files":
                                now_ts = time.time()
                                if (now_ts - files_live_index_last_trigger) >= files_live_index_interval_sec:
                                    files_live_index_last_trigger = now_ts
                                    if _trigger_files_index_rebuild_async(reason=f"scan_live_sync_{artists_processed}"):
                                        logging.debug(
                                            "Files library live sync: triggered rebuild after %d/%d artist(s)",
                                            artists_processed,
                                            total_artists,
                                        )
                            # Log scan progress every 10 artists or if debug/verbose, using a tree-style line
                            if artists_processed % 10 == 0 or logging.getLogger().isEnabledFor(logging.DEBUG):
                                log_scan(
                                    "├─ Artist %s (%d/%d processed, %d duplicate group(s))",
                                    artist_name,
                                    artists_processed,
                                    total_artists,
                                    n_grps,
                                )
                            _emit_scan_progress_heartbeat(reason="artist_completed")
                            if not scan_should_stop.is_set():
                                if storage_scheduler_enabled and not futures:
                                    _storage_mark_current_bucket_done()
                                    current_storage_bucket_key = None
                                _fill_worker_slots()
            if storage_scheduler_enabled and not futures:
                _storage_mark_current_bucket_done()

        # Reconcile duplicate groups across artist buckets after identity normalization.
        # This catches cases discovered under separate raw artist names during pre-scan
        # (for example "sigur ros" vs "Sigur Rós") before auto-move dedupe runs.
        try:
            all_results, all_editions_by_artist, cross_bucket_metrics = _reconcile_scan_duplicates_across_artist_buckets(
                all_results,
                all_editions_by_artist,
            )
            if int(cross_bucket_metrics.get("reconciled_buckets") or 0) > 0:
                with lock:
                    state["duplicates"] = all_results
                    state["scan_cross_bucket_dupe_buckets"] = int(cross_bucket_metrics.get("reconciled_buckets") or 0)
                    state["scan_cross_bucket_dupe_groups"] = int(cross_bucket_metrics.get("groups_found") or 0)
        except Exception:
            logging.debug("Cross-bucket duplicate reconciliation failed", exc_info=True)

        # Collect all groups requiring AI processing (must be kept in scan_duplicates output, not filtered by "losers")
        ai_groups_to_process = []
        ai_group_positions = {}  # Track position of each AI group for replacement (key = artist + sorted edition ids)
        for artist_name, groups in all_results.items():
            for i, group in enumerate(groups):
                if group.get("needs_ai", False):
                    ai_groups_to_process.append(group)
                    # Normalize to sorted ints so key matches when merging ai_result (same type/order)
                    key = (artist_name, tuple(sorted(int(e['album_id']) for e in group.get("editions", []))))
                    ai_group_positions[key] = (artist_name, i)
        log_ai(
            "├─ [AI] Duplicate selection: %d group(s) requiring AI from %d artist(s) with duplicates",
            len(ai_groups_to_process),
            sum(1 for a, grps in all_results.items() if any(g.get("needs_ai") for g in grps)),
        )

        # Process AI groups in parallel batch
        if ai_groups_to_process and ai_provider_ready:
            log_ai(
                "│  ├─ Processing %d group(s) requiring AI in parallel batch (max %d concurrent)…",
                len(ai_groups_to_process),
                AI_BATCH_SIZE,
            )
            with lock:
                state["scan_active_artists"]["_ai_batch"] = {
                    "start_time": time.time(),
                    "total_groups": len(ai_groups_to_process),
                    "groups_processed": 0
                }
                state["scan_ai_batch_total"] = len(ai_groups_to_process)
                state["scan_ai_batch_processed"] = 0
                state["scan_ai_current_label"] = None

            ai_results = process_ai_groups_batch(ai_groups_to_process, max_workers=AI_BATCH_SIZE)
            ai_success_count = _dupe_ai_used_results_count(ai_results)
            log_ai(
                "│  └─ AI batch returned %d result(s) for %d group(s); used_ai=%d heuristic_fallback=%d",
                len(ai_results),
                len(ai_groups_to_process),
                ai_success_count,
                max(0, len(ai_results) - ai_success_count),
            )

            # Update all_results with AI-processed groups; ensure used_ai/ai_provider/ai_model so Unduper shows "AI"
            mod = _runtime_module()
            for ai_result in ai_results:
                try:
                    artist_name = ai_result.get("artist")
                    best = ai_result.get("best")
                    losers = ai_result.get("losers")
                    if not artist_name or best is None or not isinstance(losers, list):
                        continue
                    best.setdefault("used_ai", True)
                    if not best.get("ai_provider"):
                        best["ai_provider"] = getattr(mod, "AI_PROVIDER", None) or ""
                    if not best.get("ai_model"):
                        best["ai_model"] = getattr(mod, "RESOLVED_MODEL", None) or getattr(mod, "OPENAI_MODEL", None) or ""
                    # Normalize to sorted ints to match ai_group_positions key
                    result_edition_ids = tuple(sorted(int(e.get("album_id") or 0) for e in [best] + losers))
                    key = (artist_name, result_edition_ids)

                    if key in ai_group_positions:
                        target_artist, target_index = ai_group_positions[key]
                        if target_artist in all_results and target_index < len(all_results[target_artist]):
                            all_results[target_artist][target_index] = ai_result
                        else:
                            if target_artist not in all_results:
                                all_results[target_artist] = []
                            all_results[target_artist].append(ai_result)
                    else:
                        if artist_name not in all_results:
                            all_results[artist_name] = []
                        all_results[artist_name].append(ai_result)
                except Exception as merge_err:
                    logging.warning("Skipping malformed AI result: %s", merge_err)

            # Update state (start_time was stored with time.time(), so use time.time() for duration)
            _ai_batch = state.get("scan_active_artists", {}).get("_ai_batch", {})
            ai_batch_start = _ai_batch.get("start_time", time.time())
            ai_batch_time = time.time() - ai_batch_start
            with lock:
                if "_ai_batch" in state.get("scan_active_artists", {}):
                    del state["scan_active_artists"]["_ai_batch"]
                state.pop("scan_ai_batch_total", None)
                state.pop("scan_ai_batch_processed", None)
                state.pop("scan_ai_current_label", None)
                state["scan_ai_used_count"] += ai_success_count
                # Update duplicates in state
                state["duplicates"] = all_results
                # Progress: step 2/3 done (albums + AI batch); bar shows e.g. 39/40 until finalize
                state["scan_progress"] = state["scan_total"] - 1
                state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1  # AI batch step done
            logging.info(
                f"AI batch processing completed: {len(ai_results)}/{len(ai_groups_to_process)} groups processed successfully "
                f"in {ai_batch_time:.2f}s (avg {ai_batch_time/max(len(ai_groups_to_process), 1):.2f}s per group)"
            )
        else:
            # No AI batch run (no groups or no provider): still count the AI step so progress reaches step_total
            with lock:
                state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1

        # Final pass: any group still with needs_ai and no best/losers -> run AI (no heuristic fallback)
        fallback_count = 0
        for artist_name, groups in list(all_results.items()):
            for i, g in enumerate(groups):
                if g.get("needs_ai", False) and "best" not in g and "editions" in g:
                    editions = g["editions"]
                    if len(editions) >= 2:
                        best = choose_best(editions, defer_ai=False)
                        if best:
                            losers = [e for e in editions if e["album_id"] != best["album_id"]]
                            all_results[artist_name][i] = {
                                "artist": artist_name,
                                "album_id": best["album_id"],
                                "best": best,
                                "losers": losers,
                                "fuzzy": g.get("fuzzy", False),
                                "needs_ai": False,
                            }
                            fallback_count += 1
        if fallback_count:
            fallback_ai_count = 0
            for groups in list(all_results.values()):
                for g in groups:
                    if isinstance(g, dict):
                        best = g.get("best")
                        if isinstance(best, dict) and bool(best.get("used_ai")):
                            fallback_ai_count += 1
            logging.info(
                "Final pass: resolved %d pending group(s); used_ai=%d heuristic_fallback=%d.",
                fallback_count,
                fallback_ai_count,
                max(0, fallback_count - fallback_ai_count),
            )
            with lock:
                state["scan_ai_used_count"] = state.get("scan_ai_used_count", 0) + fallback_ai_count
                state["duplicates"] = all_results

        # Recompute dupe counts from final all_results (after AI merge + final pass),
        # so scan_history stats match what actually happened (even when groups were unresolved during per-artist threads).
        try:
            final_groups = 0
            final_losers = 0
            final_needs_ai = 0
            for _a, _grps in (all_results or {}).items():
                for _g in (_grps or []):
                    if isinstance(_g, dict) and _g.get("needs_ai", False):
                        final_needs_ai += 1
                    if not isinstance(_g, dict):
                        continue
                    best = _g.get("best")
                    losers = _g.get("losers")
                    if best is not None and isinstance(losers, list):
                        final_groups += 1
                        final_losers += len(losers or [])
            with lock:
                state["scan_duplicate_groups_count"] = int(final_groups)
                state["scan_total_duplicates_count"] = int(final_losers)
                state["scan_dupe_groups_needs_ai"] = int(final_needs_ai)
        except Exception:
            logging.debug("Final dupe count recompute failed", exc_info=True)

        # Calculate missing albums count (compare Plex albums with MusicBrainz)
        # This is a simplified version - for now, we'll set it to 0 as calculating it requires
        # MusicBrainz API calls for each artist which could be slow during scan
        # This can be implemented later as a post-scan analysis or background task
        missing_albums_total = 0
        with lock:
            state["scan_missing_albums_count"] = missing_albums_total

        # Drain incremental writer queue so all artist data is persisted before final save_scan_to_db
        if scan_incremental_writer_thread is not None and scan_incremental_queue is not None:
            scan_incremental_queue.put(None)
            scan_incremental_writer_thread.join(timeout=120)

        # Drain post-processing queue (Files mode streamed improve) before finalizing.
        # This is bounded: optional enrichment must never keep the scan in a fake
        # "almost done" state forever.
        if scan_post_worker_thread is not None and scan_post_queue is not None:
            scan_post_queue.put(None)
            scan_post_queue_shutdown_requested = True
            drain_timeout_sec = max(
                1,
                _parse_int_loose(os.getenv("PMDA_SCAN_POSTPROCESS_DRAIN_TIMEOUT_SEC"), 900),
            )
            drained = _wait_queue_idle(
                scan_post_queue,
                max_wait_sec=drain_timeout_sec,
                label="scan post-process queue",
                worker_thread=scan_post_worker_thread,
                poll_sec=0.25,
            )
            scan_post_worker_thread.join(timeout=60 if drained else 1)

        # Persist is done in finally so we always save (even on stop/exception); auto-move runs synchronously in finally after save.

    finally:
        # "Finalizing": persist to DB and update scan_history before marking scan done.
        # UI shows "Finalizing" until this is complete; only then scanning=False and stats appear.
        finalizing_steps: list[tuple[str, str]] = [
            ("persist_duplicates", "Saving duplicate decisions"),
            ("persist_editions", "Saving scan editions"),
            ("mark_incomplete", "Preparing incomplete quarantine"),
            ("move_incomplete", "Moving incomplete albums"),
            ("move_dupes", "Moving duplicate losers"),
            ("export", "Settling library export"),
            ("player_sync", "Syncing external player"),
            ("pipeline_trace", "Saving pipeline trace"),
            ("track_reconciliation", "Reconciling track outcomes"),
            ("scan_summary", "Writing scan summary"),
            ("resume_status", "Closing resume state"),
            ("post_scan", "Scheduling post-scan jobs"),
        ]
        finalizing_index = {key: idx for idx, (key, _label) in enumerate(finalizing_steps)}

        def _set_finalizing_step(
            key: str,
            label: str | None = None,
            *,
            item_done: int = 0,
            item_total: int = 0,
            item_label: str = "",
        ) -> None:
            idx = int(finalizing_index.get(str(key), 0))
            resolved_label = str(label or finalizing_steps[idx][1]).strip()
            with lock:
                state["scan_finalizing_stage"] = str(key or "")
                state["scan_finalizing_label"] = resolved_label
                state["scan_finalizing_done"] = idx
                state["scan_finalizing_total"] = len(finalizing_steps)
                state["scan_finalizing_item_done"] = max(0, int(item_done or 0))
                state["scan_finalizing_item_total"] = max(0, int(item_total or 0))
                state["scan_finalizing_item_label"] = str(item_label or "")
                state["scan_finalizing_updated_at"] = time.time()
            logging.info(
                "[SCAN] Finalizing %d/%d: %s",
                idx + 1,
                len(finalizing_steps),
                resolved_label,
            )
            job_type_for_step = {
                "persist_duplicates": "publication",
                "persist_editions": "publication",
                "mark_incomplete": "publication",
                "move_incomplete": "materialization",
                "move_dupes": "materialization",
                "export": "materialization",
                "player_sync": "materialization",
                "pipeline_trace": "publication",
                "track_reconciliation": "publication",
                "scan_summary": "scan",
                "resume_status": "scan",
                "post_scan": "scan",
            }.get(str(key or ""), "scan")
            _pipeline_job_update(
                job_type_for_step,
                status="running",
                phase=str(key or ""),
                current=max(0, int(item_done or idx)),
                total=max(0, int(item_total or len(finalizing_steps))),
                current_item=str(item_label or ""),
                message=resolved_label,
                run_id=scan_task_run_id or scan_id,
            )

        def _set_finalizing_item(done: int, total: int, label: str = "") -> None:
            with lock:
                state["scan_finalizing_item_done"] = max(0, int(done or 0))
                state["scan_finalizing_item_total"] = max(0, int(total or 0))
                state["scan_finalizing_item_label"] = str(label or "")
                state["scan_finalizing_updated_at"] = time.time()

        _set_finalizing_step("persist_duplicates")
        with lock:
            state["scan_finalizing"] = True
            state["scan_finalizing_stage"] = "persist_duplicates"
            state["scan_finalizing_label"] = "Saving duplicate decisions"
            state["scan_finalizing_done"] = 0
            state["scan_finalizing_total"] = len(finalizing_steps)
            state["scan_finalizing_item_done"] = 0
            state["scan_finalizing_item_total"] = 0
            state["scan_finalizing_item_label"] = ""
            state["scan_finalizing_updated_at"] = time.time()
            state["last_dedupe_moved_count"] = 0
            state["last_dedupe_saved_mb"] = 0
            # Progress: ensure we're at step 2/3 (e.g. 39/40) during finalize; if there was no AI batch we were still at 38
            state["scan_progress"] = max(state["scan_progress"], state["scan_total"] - 1)
            state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1  # finalize step started
        if (
            scan_post_worker_thread is not None
            and scan_post_queue is not None
            and scan_post_worker_thread.is_alive()
            and not scan_post_queue_shutdown_requested
        ):
            # If we arrived here through an exception path, stop and drain safely.
            try:
                scan_post_queue.put(None)
                scan_post_queue_shutdown_requested = True
                drain_timeout_sec = max(
                    1,
                    _parse_int_loose(os.getenv("PMDA_SCAN_POSTPROCESS_DRAIN_TIMEOUT_SEC"), 900),
                )
                drained = _wait_queue_idle(
                    scan_post_queue,
                    max_wait_sec=drain_timeout_sec,
                    label="scan post-process queue shutdown",
                    worker_thread=scan_post_worker_thread,
                    poll_sec=0.25,
                )
                scan_post_worker_thread.join(timeout=60 if drained else 1)
            except Exception:
                logging.debug("Post-process queue shutdown in finally failed", exc_info=True)
        try:
            _set_finalizing_step("persist_duplicates")
            save_scan_to_db(all_results)
            with lock:
                state["duplicates"] = load_scan_from_db()
        except Exception as e:
            logging.warning("save_scan_to_db in finally failed: %s", e)
        try:
            _set_finalizing_step("persist_editions")
            _scan_id = state.get("scan_id")
            if _scan_id and all_editions_by_artist is not None:
                save_scan_editions_to_db(
                    _scan_id,
                    all_editions_by_artist,
                    progress_callback=_set_finalizing_item,
                )
        except Exception as e:
            logging.warning("save_scan_editions_to_db in finally failed: %s", e)
        # Mark clearly truncated duplicate losers as "broken" before incomplete move.
        # Safety is enforced inside _mark_broken_from_dupe_groups via exact provider identity overlap.
        try:
            _set_finalizing_step("mark_incomplete")
            marked_from_dupes = _mark_broken_from_dupe_groups(
                all_results,
                all_editions_by_artist,
                ratio_threshold=0.90,
                require_exact_identity=True,
            )
            if marked_from_dupes:
                logging.info(
                    "Pipeline step incomplete-move prep: marked %d truncated duplicate loser(s) as broken (exact provider identity)",
                    int(marked_from_dupes),
                )
        except Exception:
            logging.exception("Pipeline step incomplete-move prep failed")

        # Pipeline step: move incomplete albums to configured quarantine folder.
        # Run this *before* dedupe so "broken" variants don't get moved as dupes.
        incomplete_move_result = {"moved": 0, "size_mb": 0, "errors": 0}
        _set_finalizing_step("move_incomplete")
        if pipeline_flags.get("incomplete_move"):
            logging.info("Pipeline step incomplete-move: scanning broken albums from current run...")
            try:
                incomplete_move_result = _auto_move_incomplete_albums_for_scan(
                    int(state.get("scan_id") or 0),
                    all_editions_by_artist,
                )
                logging.info(
                    "Pipeline step incomplete-move: moved %d album(s), %d MB, errors=%d",
                    int(incomplete_move_result.get("moved") or 0),
                    int(incomplete_move_result.get("size_mb") or 0),
                    int(incomplete_move_result.get("errors") or 0),
                )
            except Exception:
                logging.exception("Pipeline step incomplete-move failed")
            with lock:
                state["scan_incomplete_moved_count"] = int(incomplete_move_result.get("moved") or 0)
                state["scan_incomplete_moved_mb"] = int(incomplete_move_result.get("size_mb") or 0)
                state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1

        # Pipeline step: dedupe (synchronous so scan summary includes moved counts).
        # Guarded by AUTO_MOVE_DUPES: when disabled, we must never move anything automatically.
        # Magic mode implies automatic dedupe moves (users expect "fully automatic" behavior).
        _set_finalizing_step("move_dupes")
        auto_move_dup = bool(getattr(_runtime_module(), "AUTO_MOVE_DUPES", False) or getattr(_runtime_module(), "MAGIC_MODE", False))
        if pipeline_flags.get("dedupe") and all_results and auto_move_dup:
            flat_groups = [g for groups in all_results.values() for g in groups]
            # One group per unique (artist, set of editions) so we never move both editions of the same album
            seen_group_keys = set()
            deduped_flat = []
            for g in flat_groups:
                # Safety: never auto-move groups explicitly marked as no-move/manual-review.
                if g.get("no_move") or g.get("manual_review") or g.get("same_folder"):
                    continue
                best = g.get("best")
                losers = g.get("losers", [])
                if not best or not losers:
                    continue
                # Never auto-move broken/incomplete editions as dupes; those belong in the incomplete quarantine step.
                filtered_losers = []
                for e in losers:
                    try:
                        if e.get("is_broken", False):
                            continue
                    except Exception:
                        pass
                    try:
                        folder = path_for_fs_access(Path(str(e.get("folder") or "")))
                    except Exception:
                        folder = None
                    if not folder or (not folder.exists()):
                        continue
                    filtered_losers.append(e)
                if not filtered_losers:
                    continue
                g2 = dict(g)
                g2["losers"] = filtered_losers
                edition_ids = [int(best.get("album_id") or 0)]
                for e in filtered_losers:
                    edition_ids.append(int(e.get("album_id") or 0))
                key = (g2.get("artist") or "", tuple(sorted(edition_ids)))
                if key in seen_group_keys:
                    continue
                seen_group_keys.add(key)
                deduped_flat.append(g2)
            flat_groups = deduped_flat
            if flat_groups:
                logging.info("Pipeline step dedupe: running automatic deduplication (%d group(s))...", len(flat_groups))
                try:
                    background_dedupe(flat_groups)
                except Exception as e:
                    logging.warning("background_dedupe in finally failed: %s", e)
                    with lock:
                        state["deduping"] = False
                        state["dedupe_progress"] = 0
                        state["dedupe_total"] = 0
                        state["dedupe_start_time"] = None
                        state["dedupe_current_group"] = None
                        state["dedupe_last_write"] = None
                        state["dedupe_saved_this_run"] = 0
                logging.info("Pipeline step dedupe: done")
            with lock:
                state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1
        elif pipeline_flags.get("dedupe") and all_results and (not auto_move_dup):
            logging.info("Pipeline step dedupe: AUTO_MOVE_DUPES is disabled; skipping automatic deduplication")
            with lock:
                state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1

        # Pipeline step: export library (Files mode only).
        _set_finalizing_step("export")
        export_deferred_for_scan = False
        if pipeline_flags.get("export"):
            if _get_library_mode() == "files":
                if _storage_should_defer_live_library_materialization():
                    export_deferred_for_scan = True
                    logging.info(
                        "Pipeline step export: deferred until scan completion because disk-aware power-saver is active."
                    )
                else:
                    try:
                        started = _trigger_export_library_async(reason=f"scan_{int(_parse_int_loose(scan_id, 0) or 0)}_pipeline_export")
                        if started:
                            logging.info("Pipeline step export: queued Files export library rebuild in background.")
                        else:
                            logging.info("Pipeline step export: export already running; background queue skipped.")
                    except Exception:
                        logging.exception("Pipeline step export queue failed")
            else:
                logging.info("Pipeline step export skipped: not in Files mode")
            with lock:
                state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1

        # Pipeline step: sync external player library.
        _set_finalizing_step("player_sync")
        sync_result_ok = None
        sync_result_msg = ""
        sync_target = str(pipeline_flags.get("sync_target") or "none")
        if pipeline_flags.get("player_sync"):
            logging.info("Pipeline step player-sync: target=%s", sync_target)
            try:
                sync_result_ok, sync_result_msg = _trigger_player_refresh_by_target(sync_target)
                if sync_result_ok:
                    logging.info("Pipeline step player-sync: %s", sync_result_msg)
                else:
                    logging.warning("Pipeline step player-sync failed: %s", sync_result_msg)
            except Exception:
                sync_result_ok = False
                sync_result_msg = "Unexpected error while triggering player refresh"
                logging.exception("Pipeline step player-sync failed")
            with lock:
                state["scan_player_sync_target"] = sync_target
                state["scan_player_sync_ok"] = bool(sync_result_ok)
                state["scan_player_sync_message"] = sync_result_msg
                state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1
        try:
            _set_finalizing_step("pipeline_trace")
            _scan_id = state.get("scan_id")
            if _scan_id and all_editions_by_artist is not None:
                save_scan_pipeline_trace_to_db(
                    int(_scan_id),
                    all_editions_by_artist,
                    all_results,
                    progress_callback=_set_finalizing_item,
                )
        except Exception as e:
            logging.warning("save_scan_pipeline_trace_to_db in finally failed: %s", e)
        _set_finalizing_step("track_reconciliation")
        end_time = time.time()
        scan_id = None
        detected_tracks_total = 0
        with lock:
            state["scan_progress"] = state["scan_total"]  # force 100 % before stopping
            state["scan_step_progress"] = state.get("scan_step_total", state["scan_step_progress"])  # ensure 100% for steps
            scan_id = state.get("scan_id")
            scan_start_epoch = state.get("scan_start_time") or end_time
            detected_tracks_total = int(
                state.get("scan_tracks_detected_total")
                or state.get("scan_discovery_files_found")
                or 0
            )
        track_reconciliation = _compute_scan_track_reconciliation(
            int(scan_id or 0),
            detected_tracks_total,
        )
        with lock:
            state["scan_tracks_detected_total"] = int(track_reconciliation.get("detected_total") or 0)
            state["scan_tracks_library_kept"] = int(track_reconciliation.get("library_kept") or 0)
            state["scan_tracks_moved_dupes"] = int(track_reconciliation.get("moved_dupes") or 0)
            state["scan_tracks_moved_incomplete"] = int(track_reconciliation.get("moved_incomplete") or 0)
            state["scan_tracks_unaccounted"] = int(track_reconciliation.get("unaccounted") or 0)

        # Update scan history entry (summary_json etc.); only then mark scan done
        _set_finalizing_step("scan_summary")
        if scan_id:
            con = sqlite3.connect(str(STATE_DB_FILE))
            cur = con.cursor()
            duration = int(end_time - scan_start_epoch) if scan_start_epoch else None
            with lock:
                duplicates_found = sum(len(groups) for groups in all_results.values())
                artists_processed = state.get("scan_artists_processed", 0)
                ai_used_count = state.get("scan_ai_used_count", 0)
                mb_used_count = state.get("scan_mb_used_count", 0)
                space_saved = get_stat("space_saved")
                # Get detailed statistics
                duplicate_groups_count = state.get("scan_duplicate_groups_count", 0)
                total_duplicates_count = state.get("scan_total_duplicates_count", 0)
                broken_albums_count = state.get("scan_broken_albums_count", 0)
                missing_albums_count = state.get("scan_missing_albums_count", 0)  # Will be calculated separately
                albums_scanned = state.get("scan_total_albums", state.get("scan_total", 0))
                albums_without_artist_image = state.get("scan_albums_without_artist_image", 0)
                albums_without_album_image = state.get("scan_albums_without_album_image", 0)
                albums_without_complete_tags = state.get("scan_albums_without_complete_tags", 0)
                albums_without_mb_id = state.get("scan_albums_without_mb_id", 0)
                albums_without_artist_mb_id = state.get("scan_albums_without_artist_mb_id", 0)
                # When auto-move ran this scan (in finally), these are set by background_dedupe
                dupes_moved_this_scan = state.get("last_dedupe_moved_count", 0)
                space_saved_mb_this_scan = state.get("last_dedupe_saved_mb", 0)
                albums_moved_this_scan = state.get("last_dedupe_moved_count", 0)
                incomplete_moved_this_scan = state.get("scan_incomplete_moved_count", 0)
                incomplete_moved_mb_this_scan = state.get("scan_incomplete_moved_mb", 0)
                scan_tracks_detected_total = int(state.get("scan_tracks_detected_total") or 0)
                scan_tracks_library_kept = int(state.get("scan_tracks_library_kept") or 0)
                scan_tracks_moved_dupes = int(state.get("scan_tracks_moved_dupes") or 0)
                scan_tracks_moved_incomplete = int(state.get("scan_tracks_moved_incomplete") or 0)
                scan_tracks_unaccounted = int(state.get("scan_tracks_unaccounted") or 0)
                player_sync_target = state.get("scan_player_sync_target")
                player_sync_ok = state.get("scan_player_sync_ok")
                player_sync_message = state.get("scan_player_sync_message")

            scan_status = 'cancelled' if scan_should_stop.is_set() else 'completed'
            scan_task_message = (
                "Scan cancelled"
                if scan_status == "cancelled"
                else f"Scan finished: {int(albums_scanned or 0)} album(s), {int(duplicate_groups_count or 0)} duplicate group(s)"
            )
            scan_task_summary["status"] = scan_status
            # Build summary_json for end-of-scan summary (FFmpeg formats, MB, AI)
            mb_conn_ok = state.get("scan_mb_connection_ok", False)
            ai_conn_ok = state.get("scan_ai_connection_ok", False)
            mb_done = state.get("scan_mb_done_count", 0)
            mb_used = state.get("scan_mb_used_count", 0)
            ai_groups = state.get("scan_ai_used_count", 0)
            mb_verified_by_ai = state.get("scan_mb_verified_by_ai_count", 0)
            ai_calls_total = int(state.get("scan_ai_calls_total") or 0)
            ai_calls_provider_identity = int(state.get("scan_ai_calls_provider_identity") or 0)
            ai_calls_mb_verify = int(state.get("scan_ai_calls_mb_verify") or 0)
            ai_calls_web_mbid = int(state.get("scan_ai_calls_web_mbid") or 0)
            ai_calls_vision = int(state.get("scan_ai_calls_vision") or 0)
            ai_guard_calls_used = int(state.get("scan_ai_guard_calls_used") or 0)
            ai_guard_calls_blocked = int(state.get("scan_ai_guard_calls_blocked") or 0)
            ai_guard_last_reason = str(state.get("scan_ai_guard_last_reason") or "")
            ai_guard_last_block_at = state.get("scan_ai_guard_last_block_at")
            ai_tokens_total = 0
            ai_cost_usd_total = 0.0
            ai_unpriced_calls = 0
            try:
                _ai_usage_wait_for_idle(max_wait_sec=2.0)
                ai_cost_summary = _ai_scan_cost_summary(
                    int(scan_id),
                    include_lifecycle=False,
                    group_by="analysis_type",
                )
                ai_totals = ai_cost_summary.get("totals") if isinstance(ai_cost_summary, dict) else {}
                if isinstance(ai_totals, dict):
                    ai_tokens_total = int(ai_totals.get("total_tokens") or 0)
                    ai_cost_usd_total = float(ai_totals.get("cost_usd") or 0.0)
                    ai_unpriced_calls = int(ai_totals.get("unpriced_calls") or 0)
            except Exception:
                logging.debug("Failed to read AI cost summary at scan end", exc_info=True)
            cur.execute(
                "SELECT fmt_text, COUNT(*) FROM scan_editions WHERE scan_id = ? GROUP BY fmt_text",
                (scan_id,),
            )
            ffmpeg_formats = {row[0] or "?": row[1] for row in cur.fetchall()}
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_albums,
                    SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 1 THEN 1 ELSE 0 END) AS strict_matched_albums
                FROM scan_editions
                WHERE scan_id = ?
                """,
                (scan_id,),
            )
            strict_row = cur.fetchone() or (0, 0)
            strict_total_albums = int(strict_row[0] or 0)
            strict_matched_albums = int(strict_row[1] or 0)
            strict_unmatched_albums = max(0, strict_total_albums - strict_matched_albums)
            mb_used = strict_matched_albums
            cur.execute(
                """
                SELECT COALESCE(strict_match_provider, ''), COUNT(*)
                FROM scan_editions
                WHERE scan_id = ?
                  AND COALESCE(strict_match_verified, 0) = 1
                GROUP BY COALESCE(strict_match_provider, '')
                """,
                (scan_id,),
            )
            strict_provider_counts = {
                _normalize_identity_provider(str(row[0] or "")): int(row[1] or 0)
                for row in cur.fetchall()
            }
            provider_no_tracklist_rollup = _scan_provider_no_tracklist_rollup(cur, int(scan_id))
            metadata_rollup = _scan_history_metadata_rollup(cur, int(scan_id))
            strict_total_albums = int(metadata_rollup.get("strict_total_albums") or strict_total_albums or 0)
            strict_matched_albums = int(metadata_rollup.get("strict_matched_albums") or strict_matched_albums or 0)
            strict_unmatched_albums = int(metadata_rollup.get("strict_unmatched_albums") or strict_unmatched_albums or 0)
            strict_provider_counts = dict(metadata_rollup.get("strict_provider_counts") or strict_provider_counts or {})
            strict_provider_total = int(metadata_rollup.get("strict_provider_total") or sum(int(v or 0) for v in strict_provider_counts.values()))
            musicbrainz_identity_hits = int(metadata_rollup.get("musicbrainz_identity_hits") or 0)
            musicbrainz_identity_verified = int(metadata_rollup.get("musicbrainz_identity_verified") or 0)
            musicbrainz_strict_wins = int(metadata_rollup.get("musicbrainz_strict_wins") or 0)
            musicbrainz_identity_non_wins = int(metadata_rollup.get("musicbrainz_identity_non_wins") or 0)
            mb_used = strict_matched_albums
            ai_errors_raw = state.get("scan_ai_errors", [])
            # Deduplicate by message, keep last occurrence; limit to 20 for summary
            # Exclude recovered "AI index out of range; clamped" so UI does not show them as errors
            seen_msg = set()
            ai_errors_dedup = []
            for entry in reversed(ai_errors_raw):
                msg = entry.get("message", "")
                if not msg or msg in seen_msg:
                    continue
                if "AI index out of range" in msg and "clamped" in msg:
                    continue
                seen_msg.add(msg)
                ai_errors_dedup.append(entry)
                if len(ai_errors_dedup) >= 20:
                    break
            ai_errors_dedup.reverse()
            ai_errors_total = len(ai_errors_dedup)
            # Duplicate decision stats from save_scan_to_db (fallback to counters when missing)
            duplicate_groups_saved = int(state.get("scan_duplicate_groups_saved", duplicate_groups_count))
            duplicate_groups_ai_saved = int(state.get("scan_duplicate_groups_ai_saved", 0))
            duplicate_groups_skipped = int(state.get("scan_duplicate_groups_skipped", 0))
            # Heuristic approximation of AI error recovery vs unresolved:
            # - groups recovered: min(errors, AI-decided groups)
            # - unresolved: remaining errors beyond recovered
            ai_failed_then_recovered = min(ai_errors_total, duplicate_groups_ai_saved)
            ai_failed_unresolved = max(ai_errors_total - ai_failed_then_recovered, 0)
            # Last-scan-only stats for "Last scan summary" UI (only this scan's numbers)
            artists_total = state.get("scan_artists_total", 0)
            scan_discogs_matched = int(strict_provider_counts.get("discogs", 0))
            scan_lastfm_matched = int(strict_provider_counts.get("lastfm", 0))
            scan_bandcamp_matched = int(strict_provider_counts.get("bandcamp", 0))
            scan_provider_matches = {
                key: int(strict_provider_counts.get(key) or 0)
                for key in _scan_provider_match_keys(tuple(strict_provider_counts.keys()))
            }
            audio_hits = state.get("scan_audio_cache_hits", 0)
            audio_misses = state.get("scan_audio_cache_misses", 0)
            mb_hits = state.get("scan_mb_cache_hits", 0)
            mb_misses = state.get("scan_mb_cache_misses", 0)
            albums_with_mb = strict_matched_albums
            albums_without_mb = strict_unmatched_albums
            # Lossy vs lossless from ffmpeg_formats (lossless: FLAC, ALAC, WAV, AIFF, etc.)
            lossless_keys = {"FLAC", "ALAC", "WAV", "AIFF", "APE", "WV", "TAK"}
            lossless_count = sum(c for fmt, c in ffmpeg_formats.items() if (fmt or "").upper().strip() in lossless_keys)
            lossy_count = max(0, sum(ffmpeg_formats.values()) - lossless_count)
            # Cover provenance during scan (optional, may be missing on older runs)
            cover_from_mb = state.get("scan_cover_from_mb", 0)
            cover_from_discogs = state.get("scan_cover_from_discogs", 0)
            cover_from_lastfm = state.get("scan_cover_from_lastfm", 0)
            cover_from_bandcamp = state.get("scan_cover_from_bandcamp", 0)
            cover_from_web = state.get("scan_cover_from_web", 0)
            # PMDA album-level stats from improve-all (may be zero when Magic mode not run)
            pmda_albums_processed = state.get("scan_pmda_albums_processed", 0)
            pmda_albums_complete = state.get("scan_pmda_albums_complete", 0)
            pmda_albums_with_cover = state.get("scan_pmda_albums_with_cover", 0)
            pmda_albums_with_artist_image = state.get("scan_pmda_albums_with_artist_image", 0)

            summary = {
                "ffmpeg_formats": ffmpeg_formats,
                "mb_connection_ok": mb_conn_ok,
                "mb_albums_verified": mb_done,
                "mb_albums_identified": mb_used,
                "ai_connection_ok": ai_conn_ok,
                "ai_groups_count": ai_groups,
                "mb_verified_by_ai": mb_verified_by_ai,
                "ai_calls_total": ai_calls_total,
                "ai_calls_provider_identity": ai_calls_provider_identity,
                "ai_calls_mb_verify": ai_calls_mb_verify,
                "ai_calls_web_mbid": ai_calls_web_mbid,
                "ai_calls_vision": ai_calls_vision,
                "ai_guard_calls_used": ai_guard_calls_used,
                "ai_guard_calls_blocked": ai_guard_calls_blocked,
                "ai_guard_last_reason": ai_guard_last_reason,
                "ai_guard_last_block_at": ai_guard_last_block_at,
                "ai_tokens_total": ai_tokens_total,
                "ai_cost_usd_total": ai_cost_usd_total,
                "ai_unpriced_calls": ai_unpriced_calls,
                "ai_lifecycle_complete": bool(scan_status == "completed" and not pipeline_async_enabled),
                "ai_errors": ai_errors_dedup,
                # Duplicate decision stats
                "duplicate_groups_total": duplicate_groups_count,
                "duplicate_groups_saved": duplicate_groups_saved,
                "duplicate_groups_ai_decided": duplicate_groups_ai_saved,
                "duplicate_groups_skipped": duplicate_groups_skipped,
                "duplicate_groups_ai_failed_total": ai_errors_total,
                "duplicate_groups_ai_failed_then_recovered": ai_failed_then_recovered,
                "duplicate_groups_ai_failed_unresolved": ai_failed_unresolved,
                # Last-scan-only stats for "Last scan summary" UI
                "duration_seconds": duration,
                "artists_total": artists_total,
                "albums_scanned": albums_scanned,
                "duplicate_groups_count": duplicate_groups_count,
                "total_duplicates_count": total_duplicates_count,
                "broken_albums_count": broken_albums_count,
                "missing_albums_count": missing_albums_count,
                "albums_without_artist_image": albums_without_artist_image,
                "albums_without_album_image": albums_without_album_image,
                "albums_without_complete_tags": albums_without_complete_tags,
                "albums_without_mb_id": albums_without_mb_id,
                "albums_without_artist_mb_id": albums_without_artist_mb_id,
                "audio_cache_hits": audio_hits,
                "audio_cache_misses": audio_misses,
                "mb_cache_hits": mb_hits,
                "mb_cache_misses": mb_misses,
                "lossy_count": lossy_count,
                "lossless_count": lossless_count,
                "strict_total_albums": strict_total_albums,
                "strict_matched_albums": strict_matched_albums,
                "strict_unmatched_albums": strict_unmatched_albums,
                "strict_provider_counts": strict_provider_counts,
                "strict_provider_total": strict_provider_total,
                "musicbrainz_identity_hits": musicbrainz_identity_hits,
                "musicbrainz_identity_verified": musicbrainz_identity_verified,
                "musicbrainz_strict_wins": musicbrainz_strict_wins,
                "musicbrainz_identity_non_wins": musicbrainz_identity_non_wins,
                "musicbrainz_ids_captured": int(metadata_rollup.get("musicbrainz_ids_captured") or musicbrainz_identity_hits or 0),
                "musicbrainz_outcome_counts": dict(metadata_rollup.get("musicbrainz_outcome_counts") or {}),
                "musicbrainz_non_win_by_winner": dict(metadata_rollup.get("musicbrainz_non_win_by_winner") or {}),
                "musicbrainz_non_win_by_reason": dict(metadata_rollup.get("musicbrainz_non_win_by_reason") or {}),
                "albums_with_mb_id": albums_with_mb,
                "albums_without_mb_id": albums_without_mb,
                # When auto-move ran this scan
                "dupes_moved_this_scan": dupes_moved_this_scan,
                "space_saved_mb_this_scan": space_saved_mb_this_scan,
                "incomplete_moved_this_scan": incomplete_moved_this_scan,
                "incomplete_moved_mb_this_scan": incomplete_moved_mb_this_scan,
                "scan_tracks_detected_total": scan_tracks_detected_total,
                "scan_tracks_library_kept": scan_tracks_library_kept,
                "scan_tracks_moved_dupes": scan_tracks_moved_dupes,
                "scan_tracks_moved_incomplete": scan_tracks_moved_incomplete,
                "scan_tracks_unaccounted": scan_tracks_unaccounted,
                "player_sync_target": player_sync_target,
                "player_sync_ok": player_sync_ok,
                "player_sync_message": player_sync_message,
                # Fallback sources during scan (when MusicBrainz found nothing)
                "scan_discogs_matched": scan_discogs_matched,
                "scan_lastfm_matched": scan_lastfm_matched,
                "scan_bandcamp_matched": scan_bandcamp_matched,
                "scan_provider_matches": scan_provider_matches,
                # Cover provenance (when Improve Album / fallbacks fetched covers)
                "cover_from_mb": cover_from_mb,
                "cover_from_discogs": cover_from_discogs,
                "cover_from_lastfm": cover_from_lastfm,
                "cover_from_bandcamp": cover_from_bandcamp,
                "cover_from_web": cover_from_web,
                # PMDA tags-based album health (albums touched by PMDA during this run)
                "pmda_albums_processed": pmda_albums_processed,
                "pmda_albums_complete": pmda_albums_complete,
                "pmda_albums_with_cover": pmda_albums_with_cover,
                "pmda_albums_with_artist_image": pmda_albums_with_artist_image,
                "provider_no_tracklist": provider_no_tracklist_rollup,
                "provider_no_tracklist_total": int(provider_no_tracklist_rollup.get("total") or 0),
                "provider_no_tracklist_by_provider": dict(provider_no_tracklist_rollup.get("by_provider") or {}),
                "provider_no_tracklist_by_cause": dict(provider_no_tracklist_rollup.get("by_cause") or {}),
            }
            summary["dupe_report"] = state.get("scan_dupe_report") or {}
            # Build human-readable list of steps executed (for History > Scan Details)
            steps_executed = []
            steps_executed.append("1. Format analysis (FFprobe): all albums analyzed for format/bitrate/sample rate.")
            if USE_MUSICBRAINZ:
                steps_executed.append(
                    f"2. MusicBrainz lookup: {mb_done} release groups processed. "
                    f"Re-check not found: {'yes' if getattr(_runtime_module(), 'MB_RETRY_NOT_FOUND', False) else 'no'}."
                )
            else:
                steps_executed.append("2. MusicBrainz lookup: disabled.")
            steps_executed.append(
                f"3. AI to choose among MB candidates: {'enabled, ' + str(mb_verified_by_ai) + ' matches chosen' if getattr(_runtime_module(), 'USE_AI_FOR_MB_MATCH', False) else 'disabled'}."
            )
            steps_executed.append(
                f"4. AI to verify MB match: {'enabled, ' + str(mb_verified_by_ai) + ' verified' if getattr(_runtime_module(), 'USE_AI_FOR_MB_VERIFY', False) else 'disabled'}."
            )
            steps_executed.append(
                f"5. AI vision for cover comparison: {'enabled' if getattr(_runtime_module(), 'USE_AI_VISION_FOR_COVER', False) else 'disabled'}."
            )
            steps_executed.append(
                f"6. Web search (Serper) for MusicBrainz: {'enabled' if getattr(_runtime_module(), 'USE_WEB_SEARCH_FOR_MB', False) else 'disabled'}."
            )
            steps_executed.append(
                f"7. Discogs fallback: {'enabled, ' + str(scan_discogs_matched) + ' matched' if getattr(_runtime_module(), 'USE_DISCOGS', False) else 'disabled'}."
            )
            steps_executed.append(
                f"8. Bandcamp fallback: {'enabled, ' + str(scan_bandcamp_matched) + ' matched' if getattr(_runtime_module(), 'USE_BANDCAMP', False) else 'disabled'}."
            )
            steps_executed.append(
                f"9. Last.fm fallback: {'enabled, ' + str(scan_lastfm_matched) + ' matched' if getattr(_runtime_module(), 'USE_LASTFM', False) else 'disabled'}."
            )
            steps_executed.append(
                f"10. Strict match gate: {strict_matched_albums} / {strict_total_albums} release(s) verified at 100% (artist+album+track count+track titles exact)."
            )
            # REQUIRED_TAGS from settings = single source of truth
            req_tags = getattr(_runtime_module(), "REQUIRED_TAGS", ["artist", "album", "genre", "year"])
            tags_str = ", ".join((t or "").strip() or "?" for t in req_tags)
            steps_executed.append(
                f"11. Incomplete album definition (required tags: {tags_str}): {albums_without_complete_tags} without complete tags, {broken_albums_count} broken (missing tracks)."
            )
            steps_executed.append(
                f"12. Duplicate detection: {duplicate_groups_count} groups, {total_duplicates_count} total duplicate editions."
            )
            steps_executed.append(
                "13. Pipeline steps: "
                f"mode={'queued/hybrid' if pipeline_async_enabled else 'inline'}, "
                f"match_fix={'on' if pipeline_flags_requested.get('match_fix') else 'off'}, "
                f"dedupe={'on' if pipeline_flags_requested.get('dedupe') else 'off'}, "
                f"incomplete_move={'on' if pipeline_flags_requested.get('incomplete_move') else 'off'}, "
                f"export={'on' if pipeline_flags_requested.get('export') else 'off'}, "
                f"player_sync={'on' if pipeline_flags_requested.get('player_sync') else 'off'}"
                + (f" (target={pipeline_flags_requested.get('sync_target')})" if pipeline_flags_requested.get("player_sync") else "")
            )
            summary["steps_executed"] = steps_executed
            summary["scan_steps_log"] = state.get("scan_steps_log") or []
            scan_task_summary = {
                "scan_id": int(scan_id or 0),
                "artists_total": int(artists_total or 0),
                "artists_processed": int(artists_processed or 0),
                "albums_scanned": int(albums_scanned or 0),
                "duplicate_groups_count": int(duplicate_groups_count or 0),
                "total_duplicates_count": int(total_duplicates_count or 0),
                "dupes_moved_this_scan": int(dupes_moved_this_scan or 0),
                "incomplete_moved_this_scan": int(incomplete_moved_this_scan or 0),
                "duration_seconds": int(duration or 0),
            }
            if pipeline_async_enabled:
                scan_task_summary["post_scan_mode"] = "queued"

            # Emit a compact end-of-scan stats block in logs for quick troubleshooting.
            def _pct(n: int, d: int) -> str:
                return "n/a" if d <= 0 else f"{(100.0 * float(n) / float(d)):.1f}%"

            albums_with_complete_tags = max(0, albums_scanned - albums_without_complete_tags)
            albums_with_cover = max(0, albums_scanned - albums_without_album_image)
            albums_with_artist_image = max(0, albums_scanned - albums_without_artist_image)
            bar = "─" * 85
            logging.info("%s", bar)
            logging.info("SCAN SUMMARY [scan_id=%s, status=%s]", scan_id, scan_status)
            logging.info("Artists processed        : %s / %s", artists_processed, artists_total)
            logging.info("Albums scanned           : %s", albums_scanned)
            logging.info(
                "Albums with all tags     : %s / %s (%s)",
                albums_with_complete_tags, albums_scanned, _pct(albums_with_complete_tags, albums_scanned)
            )
            logging.info(
                "Albums with cover art    : %s / %s (%s)",
                albums_with_cover, albums_scanned, _pct(albums_with_cover, albums_scanned)
            )
            logging.info(
                "Albums with artist image : %s / %s (%s)",
                albums_with_artist_image, albums_scanned, _pct(albums_with_artist_image, albums_scanned)
            )
            logging.info(
                "Strict match coverage    : %s / %s (%s)",
                albums_with_mb, albums_scanned, _pct(albums_with_mb, albums_scanned)
            )
            logging.info(
                "Fallback matches         : Discogs=%s Last.fm=%s Bandcamp=%s",
                scan_discogs_matched, scan_lastfm_matched, scan_bandcamp_matched
            )
            if int(provider_no_tracklist_rollup.get("total") or 0) > 0:
                by_cause = dict(provider_no_tracklist_rollup.get("by_cause") or {})
                by_provider = dict(provider_no_tracklist_rollup.get("by_provider") or {})
                logging.info(
                    "Provider tracklist misses: total=%s api/parser=%s edition=%s absence=%s",
                    int(provider_no_tracklist_rollup.get("total") or 0),
                    int(by_cause.get("api_or_parser") or 0),
                    int(by_cause.get("edition") or 0),
                    int(by_cause.get("absence_real") or 0),
                )
                logging.info(
                    "Provider tracklist by src: MB=%s Discogs=%s Last.fm=%s Bandcamp=%s Multiple=%s None=%s",
                    int(by_provider.get("musicbrainz") or 0),
                    int(by_provider.get("discogs") or 0),
                    int(by_provider.get("lastfm") or 0),
                    int(by_provider.get("bandcamp") or 0),
                    int(by_provider.get("multiple") or 0),
                    int(by_provider.get("none") or 0),
                )
            logging.info(
                "Duplicates               : groups=%s editions=%s moved=%s",
                duplicate_groups_count, total_duplicates_count, albums_moved_this_scan
            )
            logging.info(
                "Incomplete moved         : %s album(s), %s MB",
                incomplete_moved_this_scan,
                incomplete_moved_mb_this_scan,
            )
            if player_sync_target:
                logging.info(
                    "Player sync             : target=%s status=%s",
                    player_sync_target,
                    "ok" if player_sync_ok else f"failed ({player_sync_message or 'unknown'})",
                )
            logging.info("Space saved (this scan)  : %s MB", space_saved_mb_this_scan)
            logging.info(
                "Cover sources            : MB=%s Discogs=%s Last.fm=%s Bandcamp=%s Web=%s",
                cover_from_mb, cover_from_discogs, cover_from_lastfm, cover_from_bandcamp, cover_from_web
            )
            logging.info(
                "Cache hit/miss           : audio=%s/%s MB=%s/%s",
                audio_hits, audio_misses, mb_hits, mb_misses
            )
            logging.info("Duration                 : %ss", duration if duration is not None else "n/a")
            logging.info("%s", bar)

            summary_json_str = json.dumps(summary)
            cur.execute("""
                UPDATE scan_history
                SET end_time = ?,
                    duration_seconds = ?,
                    duplicates_found = ?,
                    artists_processed = ?,
                    ai_used_count = ?,
                    mb_used_count = ?,
                    space_saved_mb = ?,
                    albums_moved = ?,
                    status = ?,
                    duplicate_groups_count = ?,
                    total_duplicates_count = ?,
                    broken_albums_count = ?,
                    missing_albums_count = ?,
                    albums_without_artist_image = ?,
                    albums_without_album_image = ?,
                    albums_without_complete_tags = ?,
                    albums_without_mb_id = ?,
                    albums_without_artist_mb_id = ?,
                    ai_tokens_total = ?,
                    ai_cost_usd_total = ?,
                    ai_unpriced_calls = ?,
                    summary_json = ?
                WHERE scan_id = ?
            """, (
                end_time,
                duration,
                duplicates_found,
                artists_processed,
                ai_used_count,
                mb_used_count,
                space_saved_mb_this_scan,
                albums_moved_this_scan,
                scan_status,
                duplicate_groups_count,
                total_duplicates_count,
                broken_albums_count,
                missing_albums_count,
                albums_without_artist_image,
                albums_without_album_image,
                albums_without_complete_tags,
                albums_without_mb_id,
                albums_without_artist_mb_id,
                ai_tokens_total,
                ai_cost_usd_total,
                ai_unpriced_calls,
                summary_json_str,
                scan_id
            ))
            if scan_status == 'completed':
                cur.execute(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES ('last_completed_scan_id', ?)",
                    (str(scan_id),),
                )
                if str(scan_type or "").strip().lower() == "full":
                    cur.execute(
                        "INSERT OR REPLACE INTO settings (key, value) VALUES ('last_completed_full_scan_id', ?)",
                        (str(scan_id),),
                    )
            con.commit()
            con.close()
            if scan_status == "completed" and str(scan_type or "").strip().lower() == "full":
                _pipeline_bootstrap_mark_full_completed(int(scan_id), completed_at=end_time)
            try:
                con_roll = _state_connect(timeout=10)
                cur_roll = con_roll.cursor()
                _ai_refresh_rollup_for_scan(cur_roll, int(scan_id))
                con_roll.commit()
                con_roll.close()
            except Exception:
                logging.debug("Failed to refresh AI rollup after scan completion", exc_info=True)
            _set_resume_run_status(
                resume_run_id,
                "completed" if scan_status == "completed" else ("cancelled" if scan_status == "cancelled" else "failed"),
                scan_id=scan_id,
            )

        if not scan_id:
            _set_finalizing_step("resume_status")
            if not scan_task_message:
                scan_task_message = "Scan finished without scan history row"
            scan_task_summary = scan_task_summary or {
                "scan_id": None,
                "status": scan_status,
            }
            _set_resume_run_status(
                resume_run_id,
                "completed" if scan_status == "completed" else ("cancelled" if scan_status == "cancelled" else "failed"),
                scan_id=None,
            )

        _set_finalizing_step("post_scan")
        if _get_library_mode() == "files" and scan_status == "completed":
            # In practice, the watcher queue may be refilled during a scan by PMDA's own writes
            # (or by atime/metadata updates on some filesystems). For robustness, clear the
            # whole pending-changes table after any successful scan.
            cleared = 0
            try:
                con = sqlite3.connect(str(STATE_DB_FILE), timeout=10)
                cur = con.cursor()
                cur.execute("DELETE FROM files_pending_changes")
                cleared = int(cur.rowcount or 0)
                con.commit()
                con.close()
            except Exception:
                logging.debug("Failed to clear files_pending_changes after scan", exc_info=True)
                cleared = 0
            logging.info(
                "FILES %s scan: cleared %d pending change row(s) after successful run.",
                scan_type,
                cleared,
            )
            with lock:
                fw = dict(state.get("files_watcher") or {})
                fw["dirty_count"] = 0
                fw["dirty_count_by_root"] = {}
                state["files_watcher"] = fw
        with lock:
            state["scan_dirty_folders_pending_clear"] = []

        with lock:
            state["scan_finalizing"] = False
            state["scan_finalizing_stage"] = "idle"
            state["scan_finalizing_label"] = ""
            state["scan_finalizing_done"] = len(finalizing_steps)
            state["scan_finalizing_total"] = len(finalizing_steps)
            state["scan_finalizing_item_done"] = 0
            state["scan_finalizing_item_total"] = 0
            state["scan_finalizing_item_label"] = ""
            state["scan_finalizing_updated_at"] = time.time()
            run_improve_after = state.pop("run_improve_after", False)
        final_job_status = "completed" if scan_status == "completed" else ("cancelled" if scan_status == "cancelled" else "failed")
        final_job_error = "" if final_job_status == "completed" else (scan_task_error or scan_task_message or scan_status)
        _pipeline_job_update(
            "publication",
            status=final_job_status,
            phase="done",
            message="Publication phase settled",
            error=final_job_error,
            run_id=scan_task_run_id or scan_id,
            finished=True,
        )
        _pipeline_job_update(
            "materialization",
            status=final_job_status,
            phase="done",
            message="Materialization phase settled",
            error=final_job_error,
            run_id=scan_task_run_id or scan_id,
            finished=True,
        )
        _pipeline_job_update(
            "scan",
            status=final_job_status,
            phase="done",
            message="Scan pipeline settled",
            error=final_job_error,
            run_id=scan_task_run_id or scan_id,
            finished=True,
            meta={"scan_id": int(scan_id or 0), "scan_type": scan_type, "status": scan_status},
        )
        # Magic / run-improve-after: run improve-all only for a completed scan.
        should_request_improve = bool((not pipeline_async_enabled) and (run_improve_after or pipeline_flags.get("match_fix")))
        if should_request_improve and scan_status != "completed":
            logging.info(
                "Skipping post-scan improve-all because current scan status is '%s' (only 'completed' is eligible).",
                scan_status,
            )
        elif should_request_improve and scan_status == "completed" and streamed_post_process_done:
            logging.info("Post-processing was streamed artist-by-artist during scan (Files mode).")

        if should_request_improve and scan_status == "completed" and not streamed_post_process_done:
            best_albums = []
            seen_ids = set()
            seen_group_keys = set()  # One best per (artist, set of edition ids) when from duplicate groups
            if all_results:
                for artist_name, groups in all_results.items():
                    for g in groups:
                        best = g.get("best")
                        losers = g.get("losers", [])
                        if not best:
                            continue
                        edition_ids = [int(best.get("album_id") or 0)]
                        for e in losers:
                            edition_ids.append(int(e.get("album_id") or 0))
                        key = (artist_name or "", tuple(sorted(edition_ids)))
                        if key in seen_group_keys:
                            continue
                        seen_group_keys.add(key)
                        if best.get("album_id") in seen_ids:
                            continue
                        seen_ids.add(best["album_id"])
                        mbid = (best.get("musicbrainz_id") or (best.get("meta") or {}).get("musicbrainz_releasegroupid") or (best.get("meta") or {}).get("musicbrainz_id") or "")
                        mbid = (mbid if isinstance(mbid, str) else str(mbid)).strip()
                        best_albums.append({
                            "album_id": best["album_id"],
                            "artist": artist_name,
                            "title_raw": best.get("title_raw", ""),
                            "album_title": best.get("title_raw") or best.get("album_norm") or f"Album {best['album_id']}",
                            "musicbrainz_id": mbid or "",
                            "folder": str(best.get("folder") or "").strip(),
                            "strict_match_verified": bool(best.get("strict_match_verified")),
                            "strict_match_provider": best.get("strict_match_provider") or "",
                            "strict_reject_reason": best.get("strict_reject_reason") or "",
                            "strict_tracklist_score": float(best.get("strict_tracklist_score") or 0.0),
                        })
            # Include albums from the CURRENT scan only (never from previous scans).
            scan_id_for_improve = scan_id
            if scan_id_for_improve is not None:
                try:
                    con = sqlite3.connect(str(STATE_DB_FILE))
                    cur = con.cursor()
                    # Include all albums from the current scan so improve-all can enrich tags/covers
                    # even when there is no MusicBrainz ID yet (e.g. Bandcamp/Last.fm-only matches, or new REQUIRED_TAGS like "genre").
                    cur.execute(
                        """
                        SELECT artist, album_id, title_raw, musicbrainz_id, folder,
                               strict_match_verified, strict_match_provider, strict_reject_reason, strict_tracklist_score
                        FROM scan_editions
                        WHERE scan_id = ?
                        """,
                        (scan_id_for_improve,),
                    )
                    for row in cur.fetchall():
                        artist_name = row[0]
                        album_id = row[1]
                        title_raw = row[2] or ""
                        mbid = (row[3] or "").strip()
                        folder = row[4] or ""
                        strict_match_verified = bool(row[5])
                        strict_match_provider = str(row[6] or "").strip()
                        strict_reject_reason = str(row[7] or "").strip()
                        try:
                            strict_tracklist_score = float(row[8] or 0.0)
                        except Exception:
                            strict_tracklist_score = 0.0
                        if album_id in seen_ids:
                            continue
                        seen_ids.add(album_id)
                        best_albums.append({
                            "artist": artist_name,
                            "album_id": album_id,
                            "title_raw": (title_raw or "").strip(),
                            "album_title": (title_raw or "").strip() or f"Album {album_id}",
                            "musicbrainz_id": mbid or "",
                            "folder": (folder or "").strip(),
                            "strict_match_verified": strict_match_verified,
                            "strict_match_provider": strict_match_provider,
                            "strict_reject_reason": strict_reject_reason,
                            "strict_tracklist_score": strict_tracklist_score,
                        })
                    con.close()
                except Exception as e:
                    logging.debug("Post-scan improve: could not load current scan_editions for extra albums: %s", e)
            current_mode = _get_library_mode()
            # Fallback to duplicates_best only in Plex mode.
            if not best_albums and current_mode != "files":
                try:
                    con = sqlite3.connect(str(STATE_DB_FILE))
                    cur = con.cursor()
                    cur.execute(
                        "SELECT artist, album_id, title_raw, album_norm, folder, meta_json FROM duplicates_best"
                    )
                    for row in cur.fetchall():
                        artist_name, album_id, title_raw, album_norm, folder, meta_json = row[0], row[1], row[2] or "", row[3] or "", row[4] or "", row[5] or ""
                        if album_id in seen_ids:
                            continue
                        seen_ids.add(album_id)
                        mbid = None
                        if meta_json:
                            try:
                                meta = json.loads(meta_json)
                                mbid = meta.get("musicbrainz_releasegroupid") or meta.get("musicbrainz_id")
                            except Exception:
                                pass
                        best_albums.append({
                            "artist": artist_name,
                            "album_id": album_id,
                            "title_raw": (title_raw or "").strip(),
                            "album_title": (title_raw or "").strip() or (album_norm or "").strip() or f"Album {album_id}",
                            "musicbrainz_id": mbid or "",
                            "folder": (folder or "").strip(),
                        })
                    con.close()
                    if best_albums:
                        logging.debug("Post-scan improve: built best_albums from duplicates_best (%d albums)", len(best_albums))
                except Exception as e:
                    logging.debug("Post-scan improve: could not load duplicates_best fallback: %s", e)
            if best_albums:
                if MAGIC_MODE:
                    logging.info("Magic mode: dedupe done, starting improve-all (%d albums) – tags, covers, artist images", len(best_albums))
                else:
                    logging.info("Run improve after scan: starting improve-all for %d albums – tags, covers, artist images", len(best_albums))
                with lock:
                    state["scan_post_processing"] = True
                    state["scan_post_total"] = len(best_albums)
                    state["scan_post_done"] = 0
                    state["scan_post_current_artist"] = None
                    state["scan_post_current_album"] = None
                _run_improve_all_albums_global(best_albums)
                if _get_library_mode() == "files":
                    _refresh_files_album_scan_cache_from_editions(best_albums, scan_id=scan_id)
            else:
                if MAGIC_MODE:
                    logging.info("Magic mode: dedupe done; no albums to improve from current scan.")
                else:
                    logging.info("Run improve after scan: no albums to improve from current scan.")
        trigger_auto_export_after_scan = False
        if (
            scan_status == "completed"
            and bool(locals().get("export_deferred_for_scan"))
            and _get_library_mode() == "files"
            and not library_is_audit_mode()
        ):
            logging.info("Deferred pipeline export will be queued after scan state is settled.")
            trigger_auto_export_after_scan = True
        if scan_stream_post_by_artist and scan_status == "completed":
            try:
                if _get_library_mode() == "files" and getattr(_runtime_module(), "AUTO_EXPORT_LIBRARY", False) and not library_is_audit_mode():
                    logging.info("Auto-export queued after streamed post-processing.")
                    trigger_auto_export_after_scan = True
            except Exception as e:
                logging.exception("Auto-export library after streamed post-processing failed: %s", e)
        if scan_status == "completed" and _get_library_mode() == "files":
            try:
                _refresh_scan_history_from_published(scan_id)
                logging.info(
                    "Final files index sync deferred to async rebuild for scan_id=%s",
                    scan_id,
                )
            except Exception:
                logging.debug("Final published scan refresh failed for scan_id=%s", scan_id, exc_info=True)
        with lock:
            state["scan_post_processing"] = False
            state["scan_post_current_artist"] = None
            state["scan_post_current_album"] = None
            state["scan_incomplete_move_running"] = False
            state["scan_incomplete_move_done"] = 0
            state["scan_incomplete_move_total"] = 0
            state["scan_incomplete_move_current_album"] = None
            state["scan_discovery_running"] = False
            state["scan_discovery_current_root"] = None
            state["scan_discovery_stage"] = "idle"
            state["scan_discovery_updated_at"] = time.time()
            state["scan_run_scope_preparing"] = False
            state["scan_run_scope_stage"] = "idle"
            state["scan_run_scope_done"] = 0
            state["scan_run_scope_total"] = 0
            state["scan_run_scope_artists_included"] = 0
            state["scan_run_scope_albums_included"] = 0
            state["scan_run_scope_started_at"] = None
            state["scan_run_scope_updated_at"] = None
            state["scan_prescan_cache_snapshot_running"] = False
            state["scan_prescan_cache_snapshot_done"] = False
            state["scan_prescan_cache_snapshot_rows"] = 0
            state["scan_prescan_cache_snapshot_total"] = 0
            state["scan_prescan_cache_snapshot_updated_at"] = None
            state["scan_published_catchup_running"] = False
            state["scan_published_catchup_reason"] = None
            state["scan_published_catchup_done"] = 0
            state["scan_published_catchup_total"] = 0
            state["scan_published_catchup_ok"] = 0
            state["scan_published_catchup_failed"] = 0
            state["scan_published_catchup_current_artist"] = None
            state["scan_published_catchup_started_at"] = None
            state["scan_published_catchup_updated_at"] = None
            state["scan_published_catchup_finished_at"] = None
            state["scan_resume_run_id"] = None
            state["scan_resume_requested_run_id"] = None
            state["scan_preserve_live_index"] = False
            state["scan_starting"] = False
            state["scan_start_requested_at"] = None
            state["scan_auto_trigger"] = None
            state["scan_scheduler_run_id"] = None
            state["scan_active_artists"] = {}
            state["scanning"] = False
        if scan_status == "completed" and _get_library_mode() == "files":
            try:
                _refresh_scan_history_from_published(scan_id)
                logging.info(
                    "Final published scan refresh complete for scan_id=%s after scan state settled",
                    scan_id,
                )
            except Exception:
                logging.debug(
                    "Final settled-state scan refresh failed for scan_id=%s",
                    scan_id,
                    exc_info=True,
                )
        if trigger_auto_export_after_scan:
            try:
                started = _trigger_scan_strict_match_export_async(
                    int(_parse_int_loose(scan_id, 0) or 0),
                    reason=f"scan_{int(_parse_int_loose(scan_id, 0) or 0)}_deferred_strict_export",
                )
                if started:
                    logging.info("Deferred strict-match export started in background after scan completion.")
                else:
                    logging.info("Deferred strict-match export already running or unavailable; background trigger skipped.")
            except Exception:
                logging.exception("Failed to queue deferred strict-match export after scan completion")
        if pipeline_async_enabled and scan_status == "completed":
            try:
                include_enrich = bool(run_improve_after_requested or pipeline_flags_requested.get("match_fix"))
                enabled_chain_jobs: set[str] = set()
                if include_enrich:
                    enabled_chain_jobs.add("enrich_batch")
                files_mode = _get_library_mode() == "files"
                if (not files_mode) and bool(pipeline_flags_requested.get("incomplete_move")):
                    enabled_chain_jobs.add("incomplete_move")
                if (not files_mode) and bool(pipeline_flags_requested.get("dedupe")):
                    enabled_chain_jobs.add("dedupe")
                if bool(pipeline_flags_requested.get("export")):
                    enabled_chain_jobs.add("export")
                if bool(pipeline_flags_requested.get("player_sync")):
                    enabled_chain_jobs.add("player_sync")
                logging.info(
                    "[Scan Pipeline] queueing post-scan chain scan_id=%s include_enrich=%s jobs=%s",
                    _int_or_none(scan_id),
                    include_enrich,
                    ",".join(sorted(enabled_chain_jobs)) or "-",
                )
                _scheduler_chain_post_scan(
                    scan_type,
                    origin_scan_id=_int_or_none(scan_id),
                    include_enrich=include_enrich,
                    enabled_jobs=enabled_chain_jobs,
                )
            except Exception:
                logging.exception("Post-scan scheduler chain failed")
        logging.debug("background_scan(): finished (flag cleared)")
        duration = time.perf_counter() - scan_perf_start
        groups_found = sum(len(v) for v in all_results.values()) if 'all_results' in locals() else 0
        removed_dupes = get_stat("removed_dupes")
        space_saved   = get_stat("space_saved")
        total_artists = len(artists_merged) if 'artists_merged' in locals() else 0
        err_count = worker_errors.qsize()
        if err_count:
            errs = []
            while not worker_errors.empty():
                errs.append(worker_errors.get())
            err_file = CONFIG_DIR / f"scan_errors_{int(time.time())}.log"
            with err_file.open("w", encoding="utf-8") as fh:
                for art, msg in errs:
                    fh.write(f"{art}: {msg}\n")
            logging.warning("⚠️  %d worker errors – details in %s", err_count, err_file)
            notify_discord(
                f"⚠️  PMDA scan finished with {err_count} errors. "
                f"See {err_file.name} for details."
            )
        notify_discord(
            "🟢 PMDA scan completed in "
            f"{duration:.1f}s\n"
            f"Artists: {total_artists}\n"
            f"Albums: {total_albums if 'total_albums' in locals() else 0}\n"
            f"Duplicate groups found: {groups_found}\n"
            f"Duplicates removed so far: {removed_dupes}\n"
            f"Space saved: {space_saved}  MB"
        )
        if scan_task_event_id:
            task_status = "completed" if scan_status == "completed" else ("skipped" if scan_status == "cancelled" else "failed")
            if not scan_task_message:
                scan_task_message = (
                    "Scan completed"
                    if task_status == "completed"
                    else ("Scan cancelled" if task_status == "skipped" else "Scan failed")
                )
            if task_status == "completed" and pipeline_async_enabled:
                scan_task_message = f"{scan_task_message}. Library ready; background enrichment jobs queued."
            if task_status != "completed" and not scan_task_error:
                scan_task_error = scan_task_message
            try:
                _task_event_finish(
                    scan_task_event_id,
                    status=task_status,
                    message=scan_task_message,
                    metrics=scan_task_summary,
                    summary=scan_task_summary,
                    error=scan_task_error,
                )
            except Exception:
                logging.debug("Failed to finalize scan task event", exc_info=True)
        try:
            _ai_guard_reset_scan(int(scan_id or 0))
        except Exception:
            logging.debug("Failed to reset AI guard runtime for scan_id=%s", scan_id, exc_info=True)
        if _get_library_mode() == "files":
            # First repair any persisted scan rows that did not land in the progressive
            # publication cache, then rebuild the PostgreSQL browsing index from that
            # repaired cache. This covers resumed/interrupted scans and post-export moves.
            started_reconcile = _trigger_files_publication_reconcile_async(
                reason="scan_completed",
                rebuild_index=True,
            )
            if not started_reconcile:
                _trigger_files_index_rebuild_async(reason="scan_completed")
