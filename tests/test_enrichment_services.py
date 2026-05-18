from pmda_enrichment import profiles, status


def test_profile_priority_flags_keep_cover_only_visual_stage():
    flags = profiles.priority_flags(priority_mode="all", skip_album_profiles=False, cover_only=True)

    assert flags == {
        "priority_mode": "p0",
        "run_visual_stage": True,
        "run_artist_profile_stage": False,
        "run_album_profile_stage": False,
    }
    assert profiles.backfill_stage_specs(cover_only=True) == [("p0", "Visual assets")]


def test_album_profile_fetch_strength_prefers_strict_then_provider_hints():
    assert profiles.album_profile_fetch_strength(strict_verified=True) == 3
    assert profiles.album_profile_fetch_strength(metadata_source="last.fm") == 2
    assert profiles.album_profile_fetch_strength(bandcamp_album_url="https://artist.bandcamp.com/album/demo") == 2
    assert profiles.album_profile_fetch_strength() == 0


def test_live_status_context_marks_background_enrichment_without_blocking_publication():
    ctx = status.live_status_context(
        scan_busy=False,
        scan_profile_enrich_running=True,
        source_is_published=False,
    )

    assert ctx == {
        "scan_busy": False,
        "background_enrichment_running": True,
        "publication_state": "enriching",
        "source_is_published": False,
    }


def test_live_status_context_preserves_published_snapshot_state():
    ctx = status.live_status_context(
        source_is_published=True,
        scan_busy=True,
        profile_jobs_active=True,
    )

    assert ctx["background_enrichment_running"] is True
    assert ctx["publication_state"] == "published"
    assert ctx["source_is_published"] is True


def test_album_status_fields_are_post_publication_non_blocking():
    fields = status.album_status_fields(
        status_context={"background_enrichment_running": True},
        has_cover=False,
        has_artist_image=True,
        has_profile=False,
    )

    assert fields["publication_state"] == "enriching"
    assert fields["cover_state"] == "enriching"
    assert fields["artist_media_state"] == "ready"
    assert fields["profile_state"] == "enriching"


def test_artist_status_fields_preserve_published_snapshot_state():
    fields = status.artist_status_fields(
        status_context={"source_is_published": True, "background_enrichment_running": True},
        has_image=False,
        has_profile=False,
        has_fallback_thumb=True,
    )

    assert fields["publication_state"] == "published"
    assert fields["cover_state"] == "fallback"
