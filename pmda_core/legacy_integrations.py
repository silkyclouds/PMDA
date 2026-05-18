"""Compatibility helpers for removed external acquisition integrations.

PMDA no longer drives Lidarr or Autobrr. Old API routes still exist so clients
receive a deterministic 410 instead of a 404, and old database columns are kept
for migrations/history. All behavior here is deliberately non-networked.
"""

from __future__ import annotations

import logging
from typing import Any


LIDARR_DISABLED_MESSAGE = "Lidarr integration is currently disabled"
AUTOBRR_DISABLED_MESSAGE = "Autobrr integration is currently disabled"


def lidarr_feature_enabled() -> bool:
    return False


def autobrr_feature_enabled() -> bool:
    return False


def disabled_lidarr_payload(*, started: bool | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"message": LIDARR_DISABLED_MESSAGE}
    if started is not None:
        payload = {"error": LIDARR_DISABLED_MESSAGE, "started": bool(started)}
    return payload


def disabled_autobrr_payload() -> dict[str, Any]:
    return {"message": AUTOBRR_DISABLED_MESSAGE}


def ignore_album_acquisition(artist_name: str, album_id: int, musicbrainz_release_group_id: str, album_title: str) -> bool:
    logging.debug(
        "Ignoring removed Lidarr album acquisition request artist=%r album_id=%r mbid=%r title=%r",
        artist_name,
        album_id,
        musicbrainz_release_group_id,
        album_title,
    )
    return False


def ignore_artist_acquisition(artist_id: int, artist_name: str, artist_mbid: str | None = None) -> bool:
    logging.debug(
        "Ignoring removed Lidarr artist acquisition request artist_id=%r artist=%r mbid=%r",
        artist_id,
        artist_name,
        artist_mbid,
    )
    return False


def ignore_autobrr_filter(artist_names: list[str], quality_preferences: dict | None = None) -> bool:
    logging.debug(
        "Ignoring removed Autobrr filter request artists=%r quality=%r",
        artist_names,
        quality_preferences,
    )
    return False
