from pmda_matching.arbitration import has_auto_materialization_candidate, summarize_candidate_tiers
from pmda_matching.confidence import (
    AI_REVIEW,
    STRICT_MB,
    STRONG_PROVIDER,
    candidate_match_classification,
    is_auto_materialization_safe,
)


def test_confidence_normalizes_provider_aliases():
    payload = candidate_match_classification(
        {"strict_match_verified": True},
        provider="mbid",
        confidence=0.1,
    )

    assert payload["provider"] == "musicbrainz"
    assert payload["tier"] == STRICT_MB
    assert payload["confidence"] == 0.99
    assert is_auto_materialization_safe(payload) is True


def test_confidence_marks_strict_non_mb_provider_as_move_safe():
    payload = candidate_match_classification(
        {"strict_match_verified": True},
        provider="apple music",
        confidence=0.2,
    )

    assert payload["provider"] == "itunes"
    assert payload["tier"] == STRONG_PROVIDER
    assert is_auto_materialization_safe(payload) is True


def test_confidence_never_auto_moves_ai_review():
    payload = candidate_match_classification(
        {"strict_reason": "ambiguous"},
        provider="discogs",
        confidence=0.2,
        ai_selected=True,
    )

    assert payload["tier"] == AI_REVIEW
    assert is_auto_materialization_safe(payload) is False


def test_arbitration_summarizes_annotated_candidate_tiers():
    candidates = [
        {"confidence_tier": "strict_mb", "match_explanation": {"tier": "strict_mb", "safe_for_auto_materialization": True}},
        {"match_explanation": {"tier": "soft_provider"}},
        {"confidence_tier": "unresolved"},
    ]

    assert summarize_candidate_tiers(candidates) == {
        "strict_mb": 1,
        "soft_provider": 1,
        "unresolved": 1,
    }
    assert has_auto_materialization_candidate(candidates) is True
