"""scan_moves audit helpers."""

from __future__ import annotations

import json
from typing import Any


MANDATORY_COLUMNS = ("scan_id", "artist", "album_id", "original_path", "moved_to_path", "size_mb", "moved_at")
OPTIONAL_COLUMNS = (
    "album_title",
    "fmt_text",
    "move_reason",
    "winner_album_id",
    "winner_title",
    "winner_path",
    "decision_source",
    "decision_provider",
    "decision_reason",
    "decision_confidence",
    "source_path",
    "destination_path",
    "materialization_strategy",
    "arbitration_result",
    "details_json",
)


def build_move_payload(
    *,
    scan_id: int,
    artist: str,
    album_id: int,
    original_path: str,
    moved_to_path: str,
    size_mb: int,
    moved_at: float,
    album_title: str = "",
    fmt_text: str = "",
    move_reason: str = "dedupe",
    winner_album_id: int | None = None,
    winner_title: str = "",
    winner_path: str = "",
    decision_source: str = "",
    decision_provider: str = "",
    decision_reason: str = "",
    decision_confidence: float | None = None,
    source_path: str = "",
    destination_path: str = "",
    materialization_strategy: str = "",
    arbitration_result: str = "",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a normalized scan_moves row payload."""
    return {
        "scan_id": int(scan_id),
        "artist": str(artist or ""),
        "album_id": int(album_id or 0),
        "original_path": str(original_path or ""),
        "moved_to_path": str(moved_to_path or ""),
        "size_mb": int(size_mb or 0),
        "moved_at": float(moved_at),
        "album_title": str(album_title or ""),
        "fmt_text": str(fmt_text or ""),
        "move_reason": str(move_reason or "dedupe").strip().lower() or "dedupe",
        "winner_album_id": int(winner_album_id) if winner_album_id is not None else None,
        "winner_title": str(winner_title or ""),
        "winner_path": str(winner_path or ""),
        "decision_source": str(decision_source or ""),
        "decision_provider": str(decision_provider or ""),
        "decision_reason": str(decision_reason or ""),
        "decision_confidence": float(decision_confidence) if decision_confidence is not None else None,
        "source_path": str(source_path or original_path or ""),
        "destination_path": str(destination_path or moved_to_path or ""),
        "materialization_strategy": str(materialization_strategy or "").strip().lower(),
        "arbitration_result": str(arbitration_result or "").strip().lower(),
        "details_json": json.dumps(details or {}, ensure_ascii=False, default=str),
    }


def ordered_insert_columns(cols_present: set[str] | list[str] | tuple[str, ...]) -> list[str]:
    """Return supported scan_moves columns in stable insert order."""
    present = {str(col) for col in (cols_present or [])}
    ordered: list[str] = []
    for key in MANDATORY_COLUMNS:
        if key in present:
            ordered.append(key)
    for key in OPTIONAL_COLUMNS:
        if key in present:
            ordered.append(key)
    return ordered
