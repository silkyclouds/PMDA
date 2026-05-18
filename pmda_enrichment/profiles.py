"""Artist and album profile enrichment policy helpers."""

from __future__ import annotations

from typing import Any

from pmda_materialization.policy import normalize_identity_provider


def album_profile_fetch_strength(
    *,
    strict_verified: bool = False,
    metadata_source: str = "",
    mbid: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
) -> int:
    """Return how strongly an album is backed for profile/review fetching."""
    if bool(strict_verified):
        return 3
    provider_hint = normalize_identity_provider(str(metadata_source or "").strip())
    if provider_hint in {"musicbrainz", "discogs", "lastfm", "bandcamp"}:
        return 2
    if any(
        (
            str(mbid or "").strip(),
            str(discogs_release_id or "").strip(),
            str(lastfm_album_mbid or "").strip(),
            str(bandcamp_album_url or "").strip(),
        )
    ):
        return 2
    return 0


def priority_flags(
    *,
    priority_mode: str = "all",
    skip_album_profiles: bool = False,
    cover_only: bool = False,
) -> dict[str, Any]:
    """Normalize profile backfill priority flags."""
    mode = str(priority_mode or "all").strip().lower() or "all"
    if mode not in {"all", "p0", "p1", "p2"}:
        mode = "all"
    cover_only_flag = bool(cover_only)
    if cover_only_flag:
        mode = "p0"
    return {
        "priority_mode": mode,
        "run_visual_stage": bool(mode in {"all", "p0"}),
        "run_artist_profile_stage": bool((not cover_only_flag) and mode in {"all", "p1"}),
        "run_album_profile_stage": bool((not cover_only_flag) and (not bool(skip_album_profiles)) and mode in {"all", "p2"}),
    }


def backfill_stage_specs(*, cover_only: bool = False) -> list[tuple[str, str]]:
    """Return the ordered profile backfill stages."""
    if bool(cover_only):
        return [("p0", "Visual assets")]
    return [
        ("p0", "Visual assets"),
        ("p1", "Artist profiles"),
        ("p2", "Album profiles"),
    ]
