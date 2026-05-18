"""Runtime-bound MusicBrainz client configuration helpers."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlparse


def musicbrainz_target_settings_for_runtime(runtime: Any, *, probe_health: bool = True) -> dict[str, Any]:
    """Return the effective MusicBrainz API target, including mirror fallback state."""
    raw_base = str(getattr(runtime, "MUSICBRAINZ_BASE_URL", "") or "").strip()
    mirror_enabled = bool(getattr(runtime, "MUSICBRAINZ_MIRROR_ENABLED", False))
    configured_use_mirror = bool(mirror_enabled and raw_base)
    use_mirror = bool(configured_use_mirror)
    fallback_reason = ""
    mirror_health: dict[str, Any] = {}
    if configured_use_mirror and probe_health:
        try:
            health = runtime._managed_runtime_health_check_musicbrainz(raw_base)
        except Exception as exc:
            health = {"available": False, "message": str(exc or "MusicBrainz mirror health probe failed")}
        mirror_health = dict(health or {})
        if not bool((health or {}).get("available")):
            use_mirror = False
            fallback_reason = str((health or {}).get("message") or "MusicBrainz mirror is unavailable")
            raw_base = "https://musicbrainz.org"
    if not raw_base:
        raw_base = "https://musicbrainz.org"
    parsed = urlparse(raw_base if "://" in raw_base else f"https://{raw_base}")
    scheme = parsed.scheme or "https"
    hostname = parsed.netloc or parsed.path or "musicbrainz.org"
    use_https = scheme.lower() != "http"
    normalized = f"{'https' if use_https else 'http'}://{hostname}"
    return {
        "enabled": bool(use_mirror),
        "configured_enabled": bool(configured_use_mirror),
        "base_url": normalized,
        "hostname": hostname,
        "use_https": use_https,
        "mirror_name": str(getattr(runtime, "MUSICBRAINZ_MIRROR_NAME", "") or "").strip(),
        "fallback_to_public": bool(configured_use_mirror and not use_mirror),
        "fallback_reason": fallback_reason,
        "mirror_health": mirror_health,
    }


def configure_musicbrainz_client_for_runtime(runtime: Any) -> None:
    """Configure the musicbrainzngs client from the live PMDA runtime settings."""
    musicbrainzngs = runtime.musicbrainzngs
    email = str(getattr(runtime, "MUSICBRAINZ_EMAIL", "") or "").strip() or "pmda@example.com"
    musicbrainzngs.set_useragent("PMDA", "0.6.6", email)
    target = musicbrainz_target_settings_for_runtime(runtime)
    try:
        musicbrainzngs.set_hostname(target["hostname"], use_https=bool(target["use_https"]))
    except TypeError:
        try:
            musicbrainzngs.set_hostname(target["hostname"])
        except Exception:
            pass
    except Exception:
        logging.debug("MusicBrainz hostname configuration failed", exc_info=True)
    logging.debug(
        "MusicBrainz client configured email=%s base_url=%s mirror=%s",
        email,
        target["base_url"],
        bool(target["enabled"]),
    )
