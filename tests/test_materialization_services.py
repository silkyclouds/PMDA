from pmda_materialization import audit, policy


def test_materialization_policy_direct_import_allows_strict_provider():
    result = policy.confidence_policy(
        {
            "strict_match_verified": True,
            "strict_match_provider": "discogs",
            "strict_tracklist_score": 0.96,
            "discogs_release_id": "123",
        }
    )

    assert result["tier"] == "strong_provider"
    assert result["auto_materialize"] is True
    assert result["provider"] == "discogs"


def test_materialization_audit_direct_import_keeps_move_reason_stable():
    payload = audit.build_move_payload(
        scan_id=1,
        artist="Autechre",
        album_id=2,
        original_path="/music/in",
        moved_to_path="/music/out",
        size_mb=3,
        moved_at=4.5,
        move_reason="Matched_Export",
    )

    assert payload["move_reason"] == "matched_export"
    assert payload["source_path"] == "/music/in"
    assert payload["destination_path"] == "/music/out"
