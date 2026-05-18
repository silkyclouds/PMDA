"""Materialization confidence policy.

Browse visibility can accept useful but incomplete provider metadata. Filesystem
mutation is stricter: PMDA only materializes automatically when identity proof is
strong enough to avoid destructive misclassification.
"""

from __future__ import annotations

from typing import Any


def normalize_identity_provider(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    if raw in {"mb", "mbid", "musicbrainz", "musicbrainz_rg", "musicbrainz_release_group"}:
        return "musicbrainz"
    if raw in {"discogs", "discog"}:
        return "discogs"
    if raw in {"itunes", "itune", "apple", "applemusic", "apple_music", "apple music"}:
        return "itunes"
    if raw in {"deezer"}:
        return "deezer"
    if raw in {"spotify"}:
        return "spotify"
    if raw in {"qobuz"}:
        return "qobuz"
    if raw in {"tidal"}:
        return "tidal"
    if raw in {"lastfm", "last.fm", "last_fm"}:
        return "lastfm"
    if raw in {"bandcamp"}:
        return "bandcamp"
    if raw in {"audiodb", "theaudiodb", "audio_db"}:
        return "audiodb"
    return raw


def has_trusted_album_identity(
    *,
    musicbrainz_id: str | None = None,
    discogs_release_id: str | None = None,
    lastfm_album_mbid: str | None = None,
    bandcamp_album_url: str | None = None,
) -> bool:
    """Return True when a trusted provider identifier exists."""
    return bool(
        str(musicbrainz_id or "").strip()
        or str(discogs_release_id or "").strip()
        or str(lastfm_album_mbid or "").strip()
        or str(bandcamp_album_url or "").strip()
    )


def confidence_policy(item: dict[str, Any] | None) -> dict[str, Any]:
    """Classify whether an album may be materialized automatically."""
    data = item if isinstance(item, dict) else {}
    provider = normalize_identity_provider(
        str(
            data.get("strict_match_provider")
            or data.get("identity_provider")
            or data.get("metadata_source")
            or data.get("primary_metadata_source")
            or ""
        )
    )
    musicbrainz_id = str(
        data.get("musicbrainz_id")
        or data.get("musicbrainz_release_group_id")
        or data.get("musicbrainz_release_id")
        or ""
    ).strip()
    discogs_release_id = str(data.get("discogs_release_id") or "").strip()
    lastfm_album_mbid = str(data.get("lastfm_album_mbid") or "").strip()
    bandcamp_album_url = str(data.get("bandcamp_album_url") or "").strip()
    has_trusted_id = has_trusted_album_identity(
        musicbrainz_id=musicbrainz_id,
        discogs_release_id=discogs_release_id,
        lastfm_album_mbid=lastfm_album_mbid,
        bandcamp_album_url=bandcamp_album_url,
    )
    try:
        strict_score = float(data.get("strict_tracklist_score") or 0.0)
    except Exception:
        strict_score = 0.0
    strict_verified = bool(data.get("strict_match_verified"))
    ai_hint = data.get("_lookup_identity_hint") or data.get("ai_identity_hint") or {}
    ai_used = bool(data.get("ai_used") or (isinstance(ai_hint, dict) and ai_hint))

    if strict_verified:
        tier = "strict_mb" if (provider == "musicbrainz" or musicbrainz_id) else "strong_provider"
        confidence = max(strict_score, 0.99 if tier == "strict_mb" else 0.95)
        return {
            "tier": tier,
            "auto_materialize": True,
            "confidence": float(max(0.0, min(1.0, confidence))),
            "provider": provider or ("musicbrainz" if musicbrainz_id else ""),
            "reason": "strict_identity_verified",
        }
    if has_trusted_id:
        return {
            "tier": "soft_provider",
            "auto_materialize": False,
            "confidence": 0.72,
            "provider": provider
            or (
                "musicbrainz"
                if musicbrainz_id
                else "discogs"
                if discogs_release_id
                else "lastfm"
                if lastfm_album_mbid
                else "bandcamp"
                if bandcamp_album_url
                else ""
            ),
            "reason": "trusted_provider_id_without_strict_tracklist",
        }
    if ai_used:
        return {
            "tier": "ai_review",
            "auto_materialize": False,
            "confidence": 0.60,
            "provider": provider,
            "reason": "ai_assisted_identity_requires_human_review",
        }
    return {
        "tier": "unresolved",
        "auto_materialize": False,
        "confidence": 0.0,
        "provider": provider,
        "reason": "no_trusted_identity",
    }
