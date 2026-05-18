from pmda_core import provider_matching


def test_candidate_match_classification_strict_mb_is_auto_safe():
    payload = provider_matching.candidate_match_classification(
        {"strict_match_verified": True, "title_score": 0.8},
        provider="musicbrainz",
        confidence=0.5,
    )
    assert payload["tier"] == "strict_mb"
    assert payload["confidence"] == 0.99
    assert payload["safe_for_auto_materialization"] is True
    assert payload["title_score"] == 0.8


def test_candidate_match_classification_ai_review_is_not_auto_safe():
    payload = provider_matching.candidate_match_classification(
        {"confidence": 0.2, "strict_reason": "ambiguous"},
        provider="discogs",
        confidence=0.2,
        ai_selected=True,
    )
    assert payload["tier"] == "ai_review"
    assert payload["confidence"] == 0.6
    assert payload["safe_for_auto_materialization"] is False


def test_candidate_match_classification_soft_provider_keeps_reason():
    payload = provider_matching.candidate_match_classification(
        {"track_count_ratio": 0.88, "local_track_count": 8, "provider_track_count": 9},
        provider="lastfm",
        confidence=0.78,
        soft_identity_ok=True,
        soft_identity_reason="lastfm_album_page",
    )
    assert payload["tier"] == "soft_provider"
    assert payload["reason"] == "lastfm_album_page"
    assert payload["safe_for_auto_materialization"] is False
    assert payload["track_count_ratio"] == 0.88


def test_candidate_match_classification_unresolved_keeps_reject_reason():
    payload = provider_matching.candidate_match_classification(
        {"strict_reject_reason": "album_mismatch"},
        provider="bandcamp",
        confidence=0.1,
        soft_identity_ok=False,
        soft_identity_reason="provider_no_tracklist",
    )
    assert payload["tier"] == "unresolved"
    assert payload["reason"] == "provider_no_tracklist"
    assert payload["strict_reject_reason"] == "album_mismatch"


def test_normalize_scan_provider_matches_orders_and_aliases():
    payload = provider_matching.normalize_scan_provider_matches(
        {"last.fm": "2", "apple music": 3, "weird": 1}
    )

    assert list(payload)[:5] == ["musicbrainz", "discogs", "lastfm", "bandcamp", "itunes"]
    assert payload["lastfm"] == 2
    assert payload["itunes"] == 3
    assert payload["weird"] == 1


def test_normalize_scan_provider_matches_uses_legacy_when_raw_empty():
    payload = provider_matching.normalize_scan_provider_matches(
        {},
        legacy_discogs=4,
        legacy_lastfm=5,
        legacy_bandcamp=6,
    )

    assert payload["discogs"] == 4
    assert payload["lastfm"] == 5
    assert payload["bandcamp"] == 6
