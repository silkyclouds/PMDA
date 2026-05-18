from pmda_core import scan_progress


def test_cache_state_key_tracks_hot_progress_dimensions():
    key = scan_progress.cache_state_key(
        {
            "scanning": True,
            "scan_id": "12",
            "scan_type": "full",
            "scan_artists_processed": 3,
            "scan_active_artists": {
                "_ai_batch": {"total_albums": 1},
                "Autechre": {"total_albums": 2},
                "Bad": "ignored",
            },
            "export_progress": {"running": True, "albums_done": 9},
            "storage_power_saver_enabled": True,
            "storage_current_device_id": "disk12",
            "storage_bucket_done": 4,
            "storage_bucket_total": 10,
        }
    )

    assert key[0] is True
    assert key[2] == 12
    assert key[3] == "full"
    assert 1 in key  # active non-internal artist count
    assert "disk12" in key


def test_pre_scan_current_step_prioritizes_snapshot_and_catchup():
    assert (
        scan_progress.pre_scan_current_step(
            scan_discovery_stage="album_candidates",
            prescan_snapshot_active=True,
        )
        == "snapshotting_prescan_cache"
    )
    assert (
        scan_progress.pre_scan_current_step(
            scan_discovery_stage="filesystem",
            published_catchup_active=True,
        )
        == "rehydrating_library_index"
    )


def test_pre_scan_current_step_uses_resume_and_discovery_stage():
    assert scan_progress.pre_scan_current_step(scan_discovery_stage="filesystem") == "discovering_filesystem"
    assert scan_progress.pre_scan_current_step(scan_discovery_stage="album_candidates") == "building_album_candidates"
    assert scan_progress.pre_scan_current_step(scan_resume_run_id="run-1") == "restoring_resume_plan"
    assert scan_progress.pre_scan_current_step() == "preparing_prescan"


def test_pre_work_stage_progress_handles_run_scope():
    stage = scan_progress.pre_work_stage_progress(
        progress=5,
        total=100,
        run_scope_preparing=True,
        run_scope_done=12,
        run_scope_total=20,
        run_scope_eta_seconds=90,
    )

    assert stage["handled"] is True
    assert stage["progress"] == 12
    assert stage["total"] == 20
    assert stage["stage_progress_unit"] == "artists"
    assert stage["phase_progress"] == 60.0
    assert stage["current_step"] == "restoring_resume_plan"
    assert stage["eta_seconds"] == 90


def test_pre_work_stage_progress_handles_discovery_filesystem_eta():
    stage = scan_progress.pre_work_stage_progress(
        progress=0,
        total=0,
        pre_scan_active=True,
        scan_discovery_stage="filesystem",
        discovery_roots_done=2,
        discovery_roots_total=10,
        discovery_started_at=100.0,
        now=120.0,
    )

    assert stage["handled"] is True
    assert stage["current_step"] == "discovering_filesystem"
    assert stage["stage_progress_unit"] == "roots"
    assert stage["stage_progress_percent"] == 20.0
    assert stage["eta_seconds"] == 80


def test_pre_work_stage_progress_handles_album_candidate_building():
    stage = scan_progress.pre_work_stage_progress(
        progress=0,
        total=0,
        pre_scan_active=True,
        scan_discovery_stage="album_candidates",
        preplan_done=25,
        preplan_total=100,
        preplan_percent=25.0,
    )

    assert stage["handled"] is True
    assert stage["current_step"] == "building_album_candidates"
    assert stage["stage_progress_unit"] == "albums"
    assert stage["stage_progress_done"] == 25
    assert stage["stage_progress_total"] == 100


def test_active_artists_snapshot_skips_internal_entries_and_counts_progress():
    snapshot = scan_progress.active_artists_snapshot(
        {
            "_ai_batch": {"total_albums": 99, "albums_processed": 99},
            "Orbital": {
                "total_albums": 4,
                "albums_processed": 2,
                "current_album": {"album_title": "In Sides", "status": "searching_mb"},
            },
            "Autechre": {
                "total_albums": 0,
                "albums_processed": 3,
                "current_album": {"album_title": "Tri Repetae", "status": "done"},
            },
            "Bad": "ignored",
        }
    )
    assert snapshot["started_count"] == 2
    assert snapshot["album_progress"] == 5
    assert snapshot["current_step"] == "searching_mb"
    assert [item["artist_name"] for item in snapshot["items"]] == ["Orbital", "Autechre"]


def test_rate_eta_and_percent_are_bounded():
    assert scan_progress.percent(25, 100) == 25.0
    assert scan_progress.percent(125, 100) == 100.0
    assert scan_progress.percent(1, 0) == 0.0
    rate, eta = scan_progress.rate_eta(25, 100, 10)
    assert rate == 2.5
    assert eta == 30
    assert scan_progress.rate_eta(0, 100, 10) == (None, None)


def test_count_progress_snapshot_bounds_eta_and_stalled_state():
    snapshot = scan_progress.count_progress_snapshot(
        done=125,
        total=100,
        running=True,
        started_at=100.0,
        updated_at=110.0,
        stall_after_seconds=5.0,
        now=120.0,
    )

    assert snapshot == {
        "done": 100,
        "total": 100,
        "percent": 100.0,
        "eta_seconds": 0,
        "stalled": True,
    }


def test_count_progress_snapshot_handles_missing_totals():
    snapshot = scan_progress.count_progress_snapshot(done=7, total=0, running=True, now=120.0)

    assert snapshot["done"] == 7
    assert snapshot["total"] == 0
    assert snapshot["percent"] == 0.0
    assert snapshot["eta_seconds"] is None
    assert snapshot["stalled"] is False


def test_hot_phase_priorities_match_scan_runtime():
    phase, step = scan_progress.hot_phase(scan_run_scope_preparing=True)
    assert phase == "preparing_run_scope"
    assert step == "restoring_resume_plan"

    phase, step = scan_progress.hot_phase(
        scan_discovery_running=True,
        scan_discovery_stage="album_candidates",
        scan_resume_run_id="resume-1",
    )
    assert phase == "pre_scan"
    assert step == "building_album_candidates"

    phase, step = scan_progress.hot_phase(
        scan_mb_done_count=2,
        current_step="searching_mb",
    )
    assert phase == "identification_tags"
    assert step == "searching_mb"

    phase, step = scan_progress.hot_phase(
        scan_profile_enrich_running=True,
        scan_mb_done_count=50,
    )
    assert phase == "profile_enrichment"
    assert step is None


def test_phase_labels_and_modes_are_centralized():
    assert scan_progress.phase_labels("pre_scan", "discovering_filesystem") == (
        "Preparing the scan",
        "Discovering monitored folders",
    )
    assert scan_progress.phase_labels(
        "finalizing",
        None,
        scan_finalizing_label="Writing final summaries",
        scan_finalizing_item_label="scan 42",
    ) == ("Finalizing scan results", "Writing final summaries: scan 42")
    assert scan_progress.phase_labels("identification_tags", "searching_mb") == (
        "Matching albums and verifying tags",
        "Querying MusicBrainz and providers",
    )
    assert scan_progress.progress_mode("pre_scan") == "preparing"
    assert scan_progress.progress_mode("profile_enrichment") == "finalizing"
    assert scan_progress.progress_mode("identification_tags") == "stage_active"


def test_eta_confidence_is_conservative():
    assert scan_progress.eta_confidence(
        scanning=False,
        eta_seconds=10,
        phase="identification_tags",
        elapsed_seconds=3600,
        stage_progress_done=1000,
    ) == "low"
    assert scan_progress.eta_confidence(
        scanning=True,
        eta_seconds=10,
        phase="pre_scan",
        elapsed_seconds=3600,
        stage_progress_done=1000,
    ) == "low"
    assert scan_progress.eta_confidence(
        scanning=True,
        eta_seconds=10,
        phase="identification_tags",
        elapsed_seconds=60,
        stage_progress_done=1000,
    ) == "medium"
    assert scan_progress.eta_confidence(
        scanning=True,
        eta_seconds=10,
        phase="identification_tags",
        elapsed_seconds=3600,
        stage_progress_done=1000,
    ) == "high"


def test_runtime_seconds_effective_prefers_live_elapsed_then_summary():
    assert scan_progress.runtime_seconds_effective(42, {"duration_seconds": 99}) == 42
    assert scan_progress.runtime_seconds_effective(None, {"duration_seconds": "99"}) == 99
    assert scan_progress.runtime_seconds_effective(None, None) is None


def test_library_ready_requires_idle_visible_or_completed_scan():
    assert scan_progress.library_ready(scanning=True, visible_published_albums_count=10) is False
    assert scan_progress.library_ready(scanning=False, visible_published_albums_count=10) is True
    assert scan_progress.library_ready(scanning=False, visible_published_artists_count=2) is True
    assert scan_progress.library_ready(scanning=False, cached_library_ready=True) is True
    assert (
        scan_progress.library_ready(
            scanning=False,
            has_completed_full_scan=True,
            last_scan_summary={"albums_scanned": 5},
        )
        is True
    )
    assert scan_progress.library_ready(scanning=False) is False


def test_worker_stage_progress_identification_uses_inflight_album_progress():
    stage = scan_progress.worker_stage_progress(
        phase="identification_tags",
        mb_done_count=10,
        scan_processed_albums_count=12,
        active_album_progress=3,
        total_albums=100,
    )

    assert stage == {
        "handled": True,
        "stage_progress_done": 15,
        "stage_progress_total": 100,
        "stage_progress_unit": "albums",
    }


def test_worker_stage_progress_background_uses_profile_backfill():
    stage = scan_progress.worker_stage_progress(
        phase="background_enrichment",
        profile_backfill_state={"running": True, "current": 4, "total": 10},
    )

    assert stage["stage_progress_done"] == 4
    assert stage["stage_progress_total"] == 10
    assert stage["stage_progress_unit"] == "artists"


def test_worker_stage_progress_finalizing_prefers_item_progress():
    stage = scan_progress.worker_stage_progress(
        phase="finalizing",
        scan_finalizing_done=1,
        scan_finalizing_total=3,
        scan_finalizing_item_done=8,
        scan_finalizing_item_total=12,
    )

    assert stage["stage_progress_done"] == 8
    assert stage["stage_progress_total"] == 12
    assert stage["stage_progress_unit"] == "items"


def test_effective_inflight_progress_counts_active_album_and_artist_work():
    effective = scan_progress.effective_inflight_progress(
        scanning=True,
        run_scope_preparing=False,
        pre_scan_active=False,
        phase="identification_tags",
        scan_processed_albums_count=12,
        artists_processed=4,
        active_album_progress=3,
        active_artists_started=2,
        stage_progress_total=20,
        artists_total=10,
    )

    assert effective["scan_processed_albums_effective"] == 15
    assert effective["artists_processed_effective"] == 6


def test_effective_inflight_progress_can_count_active_prework_for_fallback():
    effective = scan_progress.effective_inflight_progress(
        scanning=True,
        run_scope_preparing=True,
        pre_scan_active=False,
        phase="preparing_run_scope",
        scan_processed_albums_count=1,
        artists_processed=2,
        active_album_progress=4,
        active_artists_started=3,
        total_albums=20,
        artists_total=10,
        count_active_during_prework=True,
    )

    assert effective["scan_processed_albums_effective"] == 5
    assert effective["artists_processed_effective"] == 5


def test_stage_rate_eta_updates_eta_from_stage_progress():
    update = scan_progress.stage_rate_eta(
        scanning=True,
        elapsed_seconds_value=10,
        stage_progress_done=25,
        stage_progress_total=100,
    )

    assert update["phase_rate"] == 2.5
    assert update["eta_seconds"] == 30


def test_stage_rate_eta_replaces_outlier_album_eta_only_when_requested():
    update = scan_progress.stage_rate_eta(
        scanning=True,
        elapsed_seconds_value=10,
        stage_progress_done=25,
        stage_progress_total=100,
        phase="identification_tags",
        current_phase_rate=1.0,
        current_eta_seconds=999999,
        update_eta_for_all_phases=False,
        replace_outlier_eta_for_album_phases=True,
    )

    assert update["phase_rate"] == 2.5
    assert update["eta_seconds"] == 30

    unchanged = scan_progress.stage_rate_eta(
        scanning=True,
        elapsed_seconds_value=10,
        stage_progress_done=25,
        stage_progress_total=100,
        phase="export",
        current_eta_seconds=999999,
        update_eta_for_all_phases=False,
        replace_outlier_eta_for_album_phases=True,
    )
    assert unchanged["eta_seconds"] == 999999


def test_overall_progress_prefers_profile_enrichment_when_active():
    progress = scan_progress.overall_progress(
        scanning=True,
        run_scope_preparing=False,
        pre_scan_active=False,
        progress=10,
        total=100,
        phase_progress=10.0,
        scan_profile_enrich_running=True,
        scan_profile_enrich_done=4,
        scan_profile_enrich_total=8,
    )

    assert progress["overall_progress_done"] == 4
    assert progress["overall_progress_total"] == 8
    assert progress["overall_progress_percent"] == 50.0


def test_overall_progress_combines_artist_and_post_processing_work():
    progress = scan_progress.overall_progress(
        scanning=True,
        run_scope_preparing=False,
        pre_scan_active=False,
        scan_post_processing=True,
        scan_post_done=5,
        scan_post_total=10,
        artists_processed=7,
        artists_total=10,
    )

    assert progress["overall_progress_done"] == 12
    assert progress["overall_progress_total"] == 20
    assert progress["overall_progress_percent"] == 60.0


def test_overall_progress_uses_effective_artists_during_prework():
    progress = scan_progress.overall_progress(
        scanning=True,
        run_scope_preparing=True,
        pre_scan_active=False,
        progress=1,
        total=4,
        phase_progress=25.0,
        artists_total=10,
        artists_processed_effective=3,
    )

    assert progress["overall_progress_done"] == 3
    assert progress["overall_progress_total"] == 10
    assert progress["overall_progress_percent"] == 30.0


def test_refine_post_work_phase_preserves_active_primary_work():
    assert (
        scan_progress.refine_post_work_phase(
            "identification_tags",
            scanning=True,
            scan_starting=False,
            run_scope_preparing=False,
            pre_scan_active=False,
            scan_discovery_running=True,
            scan_incomplete_move_running=False,
            export_running=False,
            deduping=False,
            background_enrichment_running=True,
            scan_processed_albums_count=100,
            total_albums=100,
            artists_processed=10,
            artists_total=10,
        )
        == "identification_tags"
    )


def test_refine_post_work_phase_detects_background_and_finalizing():
    common = {
        "scanning": True,
        "scan_starting": False,
        "run_scope_preparing": False,
        "pre_scan_active": False,
        "scan_discovery_running": False,
        "scan_incomplete_move_running": False,
        "export_running": False,
        "deduping": False,
        "scan_processed_albums_count": 100,
        "total_albums": 100,
        "artists_processed": 10,
        "artists_total": 10,
    }
    assert (
        scan_progress.refine_post_work_phase(
            "identification_tags",
            background_enrichment_running=True,
            **common,
        )
        == "background_enrichment"
    )
    assert (
        scan_progress.refine_post_work_phase(
            "identification_tags",
            background_enrichment_running=False,
            **common,
        )
        == "finalizing"
    )


def test_run_scope_payload_state_resets_stale_scope_when_phase_moves_on():
    payload = scan_progress.run_scope_payload_state(
        phase="identification_tags",
        run_scope_preparing=False,
        run_scope_stage="restoring",
        run_scope_done=10,
        run_scope_total=20,
        run_scope_percent=50.0,
        run_scope_eta_seconds=99,
        run_scope_stalled=True,
    )

    assert payload == {
        "active": False,
        "run_scope_stage": "idle",
        "run_scope_done": 0,
        "run_scope_total": 0,
        "run_scope_percent": 0.0,
        "run_scope_eta_seconds": None,
        "run_scope_stalled": False,
    }


def test_run_scope_payload_state_preserves_active_scope():
    payload = scan_progress.run_scope_payload_state(
        phase="preparing_run_scope",
        run_scope_preparing=True,
        run_scope_stage="restoring",
        run_scope_done="7",
        run_scope_total="14",
        run_scope_percent=50.0,
        run_scope_eta_seconds=30,
        run_scope_stalled=True,
    )

    assert payload["active"] is True
    assert payload["run_scope_stage"] == "restoring"
    assert payload["run_scope_done"] == 7
    assert payload["run_scope_total"] == 14
    assert payload["run_scope_percent"] == 50.0
    assert payload["run_scope_eta_seconds"] == 30
    assert payload["run_scope_stalled"] is True


def test_post_processing_payload_state_resets_stale_post_counters_and_falls_back_to_artists():
    payload = scan_progress.post_processing_payload_state(
        phase="identification_tags",
        scan_post_processing=False,
        scan_post_done=12,
        scan_post_total=20,
        scan_post_current_artist="AFX",
        scan_post_current_album="SAW",
        scanning=True,
        run_scope_preparing=False,
        pre_scan_active=False,
        artists_processed=7,
        artists_total=10,
        overall_progress_done=12,
        overall_progress_total=30,
        overall_progress_percent=40.0,
    )

    assert payload["active"] is False
    assert payload["scan_post_processing"] is False
    assert payload["scan_post_done"] == 0
    assert payload["scan_post_total"] == 0
    assert payload["scan_post_current_artist"] is None
    assert payload["scan_post_current_album"] is None
    assert payload["overall_progress_done"] == 7
    assert payload["overall_progress_total"] == 10
    assert payload["overall_progress_percent"] == 70.0


def test_post_processing_payload_state_preserves_active_post_counters():
    payload = scan_progress.post_processing_payload_state(
        phase="post_processing",
        scan_post_processing=True,
        scan_post_done=3,
        scan_post_total=10,
        scan_post_current_artist="Autechre",
        scan_post_current_album="Amber",
        scanning=True,
        overall_progress_done=5,
        overall_progress_total=20,
        overall_progress_percent=25.0,
    )

    assert payload["active"] is True
    assert payload["scan_post_processing"] is True
    assert payload["scan_post_done"] == 3
    assert payload["scan_post_total"] == 10
    assert payload["scan_post_current_artist"] == "Autechre"
    assert payload["scan_post_current_album"] == "Amber"
    assert payload["overall_progress_percent"] == 25.0


def test_fallback_phase_clears_active_artists_when_scan_is_idle():
    payload = scan_progress.fallback_phase(scanning=False, current_step="searching_mb")

    assert payload == {
        "phase": None,
        "current_step": None,
        "clear_active_artists": True,
    }


def test_fallback_phase_prioritizes_pre_scan_and_primary_worker_state():
    assert scan_progress.fallback_phase(scanning=True, scan_starting=True)["phase"] == "pre_scan"
    pre_scan = scan_progress.fallback_phase(
        scanning=True,
        pre_scan_active=True,
        scan_discovery_stage="album_candidates",
    )
    assert pre_scan["phase"] == "pre_scan"
    assert pre_scan["current_step"] == "building_album_candidates"

    assert (
        scan_progress.fallback_phase(
            scanning=True,
            current_step="searching_mb",
            raw_active_artists={"AFX": {}},
        )["phase"]
        == "identification_tags"
    )
    assert (
        scan_progress.fallback_phase(
            scanning=True,
            current_step="done",
            raw_active_artists={"_ai_batch": {}},
            scan_post_processing=True,
        )["phase"]
        == "ia_analysis"
    )


def test_fallback_phase_uses_post_processing_when_no_primary_worker_is_active():
    payload = scan_progress.fallback_phase(
        scanning=True,
        current_step="done",
        scan_post_processing=True,
        raw_active_artists={"AFX": {}},
    )

    assert payload["phase"] == "post_processing"
    assert payload["current_step"] == "done"
    assert payload["clear_active_artists"] is False


def test_scanning_hot_payload_summarizes_runtime_state():
    payload = scan_progress.scanning_hot_payload(
        runtime_state={
            "scanning": True,
            "scan_id": 7,
            "scan_type": "full",
            "scan_start_time": 1000.0,
            "scan_step_progress": 25,
            "scan_step_total": 100,
            "scan_total_albums": 80,
            "scan_artists_total": 12,
            "scan_artists_processed": 2,
            "scan_processed_albums_count": 10,
            "scan_mb_done_count": 3,
            "scan_active_artists": {
                "AFX": {
                    "total_albums": 4,
                    "albums_processed": 2,
                    "current_album": {"album_title": "Selected Ambient Works", "status": "searching_mb"},
                }
            },
            "scan_provider_matches": {"musicbrainz": 2},
            "scan_published_albums_count": 5,
            "scan_steps_log": list(range(80)),
        },
        cached_payload={"library_visible_albums_count": 4, "library_visible_artists_count": 3},
        scan_paused=False,
        scan_threads=8,
        scan_progress_log_tail=10,
        provider_matches_so_far={"musicbrainz": 2, "discogs": 1},
        mcp_status={"enabled": True},
        storage_progress={"storage_current_device_id": "disk1"},
    )

    assert payload["status"] == "running"
    assert payload["scan_id"] == 7
    assert payload["phase"] == "identification_tags"
    assert payload["current_step"] == "searching_mb"
    assert payload["threads_in_use"] == 8
    assert payload["matches_so_far"] == 3
    assert payload["library_visible_albums_count"] == 5
    assert payload["scan_steps_log"] == list(range(70, 80))
    assert payload["mcp"] == {"enabled": True}
    assert payload["storage_current_device_id"] == "disk1"
