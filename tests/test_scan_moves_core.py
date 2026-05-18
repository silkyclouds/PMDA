import json

from pmda_core import scan_moves


def test_build_move_payload_normalizes_audited_fields():
    payload = scan_moves.build_move_payload(
        scan_id=7,
        artist="Orbital",
        album_id=42,
        original_path="/music/source",
        moved_to_path="/music/dest",
        size_mb=123,
        moved_at=10.5,
        move_reason="Matched_Export",
        decision_confidence=0.97,
        materialization_strategy="HardLink",
        arbitration_result="Promoted",
        details={"tier": "strict_mb"},
    )
    assert payload["move_reason"] == "matched_export"
    assert payload["source_path"] == "/music/source"
    assert payload["destination_path"] == "/music/dest"
    assert payload["materialization_strategy"] == "hardlink"
    assert payload["arbitration_result"] == "promoted"
    assert json.loads(payload["details_json"]) == {"tier": "strict_mb"}


def test_ordered_insert_columns_is_schema_compatible_and_stable():
    cols = {
        "album_id",
        "scan_id",
        "artist",
        "moved_at",
        "original_path",
        "moved_to_path",
        "size_mb",
        "details_json",
        "move_reason",
        "unknown",
    }
    assert scan_moves.ordered_insert_columns(cols) == [
        "scan_id",
        "artist",
        "album_id",
        "original_path",
        "moved_to_path",
        "size_mb",
        "moved_at",
        "move_reason",
        "details_json",
    ]


def test_ordered_insert_columns_returns_empty_for_missing_schema():
    assert scan_moves.ordered_insert_columns(set()) == []
