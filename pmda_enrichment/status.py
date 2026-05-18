"""Library enrichment status helpers."""

from __future__ import annotations

from typing import Any


def enrichment_state(*, has_value: bool, active: bool, eligible: bool = True) -> str:
    """Return ready/enriching/missing for one enrichment facet."""
    if bool(has_value):
        return "ready"
    if bool(eligible) and bool(active):
        return "enriching"
    return "missing"


def live_status_context(
    *,
    source_is_published: bool = False,
    scan_busy: bool = False,
    scan_profile_enrich_running: bool = False,
    scan_post_processing: bool = False,
    profile_backfill_running: bool = False,
    profile_jobs_active: bool = False,
) -> dict[str, Any]:
    """Build the common browse-card enrichment context."""

    background_enrichment_running = bool(
        scan_busy
        or scan_profile_enrich_running
        or scan_post_processing
        or profile_backfill_running
        or profile_jobs_active
    )
    publication_state = (
        "published"
        if bool(source_is_published)
        else ("enriching" if background_enrichment_running else "ready")
    )
    return {
        "scan_busy": bool(scan_busy),
        "background_enrichment_running": bool(background_enrichment_running),
        "publication_state": publication_state,
        "source_is_published": bool(source_is_published),
    }


def artist_status_fields(
    *,
    status_context: dict[str, Any] | None,
    has_image: bool,
    has_profile: bool,
    has_fallback_thumb: bool = False,
) -> dict[str, str]:
    """Build status labels for artist cards."""
    ctx = status_context if isinstance(status_context, dict) else {}
    active = bool(ctx.get("background_enrichment_running"))
    artist_media_state = enrichment_state(has_value=bool(has_image), active=active, eligible=True)
    profile_state = enrichment_state(has_value=bool(has_profile), active=active, eligible=True)
    if bool(has_image):
        cover_state = "ready"
    elif bool(has_fallback_thumb):
        cover_state = "fallback"
    elif active:
        cover_state = "enriching"
    else:
        cover_state = "missing"
    publication_state = "published" if bool(ctx.get("source_is_published")) else (
        "enriching" if "enriching" in {artist_media_state, profile_state} else "ready"
    )
    return {
        "publication_state": publication_state,
        "cover_state": cover_state,
        "artist_media_state": artist_media_state,
        "profile_state": profile_state,
    }


def album_status_fields(
    *,
    status_context: dict[str, Any] | None,
    has_cover: bool,
    has_artist_image: bool,
    has_profile: bool,
    cover_eligible: bool = True,
    artist_media_eligible: bool = True,
    profile_eligible: bool = True,
) -> dict[str, str]:
    """Build status labels for album cards."""
    ctx = status_context if isinstance(status_context, dict) else {}
    active = bool(ctx.get("background_enrichment_running"))
    cover_state = enrichment_state(has_value=bool(has_cover), active=active, eligible=bool(cover_eligible))
    artist_media_state = enrichment_state(
        has_value=bool(has_artist_image),
        active=active,
        eligible=bool(artist_media_eligible),
    )
    profile_state = enrichment_state(has_value=bool(has_profile), active=active, eligible=bool(profile_eligible))
    publication_state = "published" if bool(ctx.get("source_is_published")) else (
        "enriching" if "enriching" in {cover_state, artist_media_state, profile_state} else "ready"
    )
    return {
        "publication_state": publication_state,
        "cover_state": cover_state,
        "artist_media_state": artist_media_state,
        "profile_state": profile_state,
    }
