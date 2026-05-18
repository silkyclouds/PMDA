from pmda_core import materialization_policy


def test_normalize_identity_provider_handles_aliases():
    assert materialization_policy.normalize_identity_provider("MBID") == "musicbrainz"
    assert materialization_policy.normalize_identity_provider("Last.FM") == "lastfm"
    assert materialization_policy.normalize_identity_provider("apple music") == "itunes"
    assert materialization_policy.normalize_identity_provider("custom") == "custom"


def test_confidence_policy_allows_strict_musicbrainz():
    policy = materialization_policy.confidence_policy(
        {
            "strict_match_verified": True,
            "strict_match_provider": "musicbrainz",
            "strict_tracklist_score": 0.91,
            "musicbrainz_release_group_id": "rg-1",
        }
    )
    assert policy["tier"] == "strict_mb"
    assert policy["auto_materialize"] is True
    assert policy["confidence"] == 0.99
    assert policy["reason"] == "strict_identity_verified"


def test_confidence_policy_allows_strong_non_mb_provider_when_strict_verified():
    policy = materialization_policy.confidence_policy(
        {
            "strict_match_verified": True,
            "strict_match_provider": "discogs",
            "strict_tracklist_score": 0.97,
            "discogs_release_id": "123",
        }
    )
    assert policy["tier"] == "strong_provider"
    assert policy["auto_materialize"] is True
    assert policy["confidence"] == 0.97
    assert policy["provider"] == "discogs"


def test_confidence_policy_holds_provider_id_without_strict_tracklist_for_review():
    policy = materialization_policy.confidence_policy(
        {
            "metadata_source": "bandcamp",
            "bandcamp_album_url": "https://example.bandcamp.com/album/demo",
        }
    )
    assert policy["tier"] == "soft_provider"
    assert policy["auto_materialize"] is False
    assert policy["confidence"] == 0.72
    assert policy["provider"] == "bandcamp"


def test_confidence_policy_separates_ai_review_from_unresolved():
    ai_policy = materialization_policy.confidence_policy({"ai_used": True})
    assert ai_policy["tier"] == "ai_review"
    assert ai_policy["auto_materialize"] is False

    unresolved = materialization_policy.confidence_policy({})
    assert unresolved["tier"] == "unresolved"
    assert unresolved["confidence"] == 0.0
