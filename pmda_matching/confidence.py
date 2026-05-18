"""Provider match confidence tiers.

This module is intentionally pure. It classifies provider evidence for display,
MCP analytics, and materialization policy without touching the database or the
filesystem.
"""

from __future__ import annotations

from typing import Any


STRICT_MB = "strict_mb"
STRONG_PROVIDER = "strong_provider"
SOFT_PROVIDER = "soft_provider"
AI_REVIEW = "ai_review"
UNRESOLVED = "unresolved"

AUTO_MATERIALIZATION_TIERS = frozenset({STRICT_MB, STRONG_PROVIDER})


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except Exception:
        return int(default)


def normalize_provider_id(value: str | None) -> str:
    """Normalize provider aliases used by match confidence payloads."""
    raw = (value or "").strip().lower()
    if raw in {"mb", "mbid", "musicbrainz", "musicbrainz_rg", "musicbrainz_release_group"}:
        return "musicbrainz"
    if raw in {"apple", "applemusic", "apple_music", "apple music", "itune", "itunes"}:
        return "itunes"
    if raw in {"last.fm", "last_fm", "lastfm"}:
        return "lastfm"
    if raw in {"theaudiodb", "audio_db", "audiodb"}:
        return "audiodb"
    return raw


def is_auto_materialization_safe(classification: dict[str, Any] | None) -> bool:
    """Return True only when a classification allows automatic file moves."""
    payload = classification if isinstance(classification, dict) else {}
    tier = str(payload.get("tier") or "").strip().lower()
    return bool(payload.get("safe_for_auto_materialization")) and tier in AUTO_MATERIALIZATION_TIERS


def candidate_match_classification(
    candidate: dict[str, Any] | None,
    *,
    provider: str,
    confidence: float,
    ai_selected: bool = False,
    soft_identity_ok: bool = False,
    soft_identity_reason: str = "",
) -> dict[str, Any]:
    """Build the descriptive confidence tier for a provider candidate."""
    c = candidate if isinstance(candidate, dict) else {}
    confidence = max(0.0, min(1.0, float(confidence or 0.0)))
    provider_norm = normalize_provider_id(provider)
    if bool(c.get("strict_match_verified")):
        tier = STRICT_MB if provider_norm == "musicbrainz" else STRONG_PROVIDER
        reason = "strict_identity_verified"
        safe_for_auto_materialization = True
        confidence = max(confidence, 0.99 if tier == STRICT_MB else 0.95)
    elif ai_selected:
        tier = AI_REVIEW
        reason = "ai_assisted_identity_requires_human_review"
        safe_for_auto_materialization = False
        confidence = max(confidence, 0.60)
    elif soft_identity_ok:
        tier = SOFT_PROVIDER
        reason = soft_identity_reason or "soft_identity_ok"
        safe_for_auto_materialization = False
    else:
        tier = UNRESOLVED
        reason = soft_identity_reason or str(c.get("strict_reject_reason") or c.get("strict_reason") or "unresolved")
        safe_for_auto_materialization = False
    return {
        "provider": provider_norm,
        "tier": tier,
        "confidence": round(float(confidence), 4),
        "reason": reason,
        "safe_for_auto_materialization": bool(safe_for_auto_materialization),
        "title_score": round(_safe_float(c.get("title_score")), 4),
        "artist_score": round(_safe_float(c.get("artist_score")), 4),
        "track_score": round(_safe_float(c.get("track_score")), 4),
        "track_count_ratio": round(_safe_float(c.get("track_count_ratio")), 4),
        "local_track_count": _safe_int(c.get("local_track_count")),
        "provider_track_count": _safe_int(c.get("provider_track_count")),
        "strict_reject_reason": str(c.get("strict_reject_reason") or c.get("strict_reason") or "").strip(),
    }
