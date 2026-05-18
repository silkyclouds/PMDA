"""Backward-compatible provider matching helpers.

New matching code should import from :mod:`pmda_matching`. This module remains
for older call sites and tests while the monolith is split domain by domain.
"""

from __future__ import annotations

from typing import Any

from pmda_core.materialization_policy import normalize_identity_provider
from pmda_matching.confidence import candidate_match_classification

SCAN_PROVIDER_MATCH_ORDER = (
    "musicbrainz",
    "discogs",
    "lastfm",
    "bandcamp",
    "itunes",
    "deezer",
    "spotify",
    "qobuz",
    "tidal",
    "audiodb",
    "acoustid",
    "fanart",
    "serper",
)


def scan_provider_match_keys(extra_keys: list[str] | tuple[str, ...] | None = None) -> list[str]:
    """Return provider stat keys in stable display order."""

    ordered: list[str] = []
    seen: set[str] = set()
    for raw in list(SCAN_PROVIDER_MATCH_ORDER) + list(extra_keys or []):
        key = normalize_identity_provider(str(raw or "").strip()) or str(raw or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return ordered


def normalize_scan_provider_matches(
    raw_matches: dict[str, Any] | None,
    *,
    legacy_discogs: int = 0,
    legacy_lastfm: int = 0,
    legacy_bandcamp: int = 0,
) -> dict[str, int]:
    """Normalize provider match counters with legacy fallback counters."""

    normalized: dict[str, int] = {}
    raw_present = False
    for provider_raw, count_raw in dict(raw_matches or {}).items():
        provider = normalize_identity_provider(str(provider_raw or "").strip()) or str(provider_raw or "").strip().lower()
        if not provider:
            continue
        try:
            normalized[provider] = int(normalized.get(provider) or 0) + int(count_raw or 0)
            raw_present = True
        except Exception:
            continue
    if not raw_present:
        normalized["discogs"] = max(int(normalized.get("discogs") or 0), int(legacy_discogs or 0))
        normalized["lastfm"] = max(int(normalized.get("lastfm") or 0), int(legacy_lastfm or 0))
        normalized["bandcamp"] = max(int(normalized.get("bandcamp") or 0), int(legacy_bandcamp or 0))
    return {key: int(normalized.get(key) or 0) for key in scan_provider_match_keys(tuple(normalized.keys()))}


__all__ = [
    "SCAN_PROVIDER_MATCH_ORDER",
    "candidate_match_classification",
    "normalize_scan_provider_matches",
    "scan_provider_match_keys",
]
