from pmda_core import scan_orchestrator


def test_summarize_pipeline_flags_is_stable_for_logs():
    assert scan_orchestrator.summarize_pipeline_flags(None) == "none"
    assert (
        scan_orchestrator.summarize_pipeline_flags(
            {
                "export": True,
                "match_fix": False,
                "sync_target": "plex",
                "custom": "x",
            }
        )
        == "match_fix=no, export=yes, sync_target=plex, custom='x'"
    )


def test_resolve_pipeline_flags_disables_destructive_work_in_audit_mode():
    flags = scan_orchestrator.resolve_pipeline_flags(
        "full",
        True,
        pipeline_enable_match_fix=True,
        pipeline_enable_dedupe=True,
        pipeline_enable_incomplete_move=True,
        pipeline_enable_export=True,
        pipeline_enable_player_sync=True,
        auto_move_dupes=True,
        magic_mode=False,
        audit_mode=True,
        sync_target="plex",
    )
    assert flags == {
        "match_fix": True,
        "dedupe": False,
        "incomplete_move": False,
        "export": False,
        "player_sync": False,
        "sync_target": "plex",
    }


def test_resolve_pipeline_flags_requires_auto_move_for_dedupe_and_target_for_player_sync():
    flags = scan_orchestrator.resolve_pipeline_flags(
        "full",
        False,
        pipeline_enable_match_fix=True,
        pipeline_enable_dedupe=True,
        pipeline_enable_incomplete_move=True,
        pipeline_enable_export=True,
        pipeline_enable_player_sync=True,
        auto_move_dupes=False,
        magic_mode=False,
        audit_mode=False,
        sync_target="none",
    )
    assert flags["match_fix"] is True
    assert flags["dedupe"] is False
    assert flags["incomplete_move"] is True
    assert flags["export"] is True
    assert flags["player_sync"] is False


def test_inline_pipeline_flags_defer_heavy_post_scan_jobs_only():
    requested = {
        "match_fix": True,
        "dedupe": True,
        "incomplete_move": True,
        "export": True,
        "player_sync": True,
        "sync_target": "jellyfin",
    }
    inline = scan_orchestrator.inline_pipeline_flags(requested, pipeline_async_enabled=True)
    assert inline["match_fix"] is False
    assert inline["dedupe"] is True
    assert inline["incomplete_move"] is True
    assert inline["export"] is False
    assert inline["player_sync"] is False
    assert inline["sync_target"] == "jellyfin"


def test_scan_lifecycle_helpers_read_plain_state_snapshots():
    assert scan_orchestrator.scan_pipeline_active_from_state({"scan_profile_enrich_running": True}) is True
    assert scan_orchestrator.scan_pipeline_active_from_state({"scan_discovery_running": True}) is False
    assert scan_orchestrator.scan_inline_matching_active_from_state({"scan_discovery_running": True}) is True
    assert scan_orchestrator.scan_inline_matching_active_from_state({"scan_profile_enrich_running": True}) is False


def test_pipeline_async_enabled_only_for_content_scans():
    assert scan_orchestrator.pipeline_async_enabled("full", True) is True
    assert scan_orchestrator.pipeline_async_enabled("changed_only", True) is True
    assert scan_orchestrator.pipeline_async_enabled("metadata_only", True) is True
    assert scan_orchestrator.pipeline_async_enabled("full", False) is False
