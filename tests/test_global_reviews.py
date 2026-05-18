from pmda_dedupe import review as dedupe_review
from pmda_incompletes import review as incomplete_review


def test_duplicate_review_merge_keeps_global_open_groups():
    persisted = {"Autechre": [{"album_id": 1, "best": {"album_id": 1}, "losers": [{"album_id": 2}]}]}
    live = {"Autechre": [{"album_id": 1, "best": {"album_id": 1}, "losers": [{"album_id": 3}]}]}
    trace = {"Autechre": [{"album_norm": "amber", "best": {"album_norm": "amber"}, "losers": [{"album_id": 4}]}]}

    merged = dedupe_review.merge_duplicate_results(persisted, trace, live)

    assert len(merged["Autechre"]) == 2
    assert dedupe_review.duplicate_registry_counts(merged) == {"groups": 2, "losers": 2}


def test_review_scope_normalization_is_shared_language():
    assert dedupe_review.normalize_review_scope("latest") == "last_scan"
    assert dedupe_review.normalize_review_scope("open") == "unresolved"
    assert incomplete_review.normalize_review_scope("matched") == "destination"


def test_incomplete_review_collapse_keeps_latest_per_album():
    items = [
        {"artist": "Biosphere", "album_id": 1, "detected_at": 1, "classification": "old"},
        {"artist": "Biosphere", "album_id": 1, "detected_at": 2, "classification": "new"},
        {"artist": "Biosphere", "album_id": 2, "detected_at": 1, "classification": "other"},
    ]

    collapsed = sorted(incomplete_review.collapse_latest_per_album(items), key=lambda item: item["album_id"])

    assert [item["classification"] for item in collapsed] == ["new", "other"]
