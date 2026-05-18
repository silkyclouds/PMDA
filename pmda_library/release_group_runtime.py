"""MusicBrainz release-group detail helpers for library routes."""

from __future__ import annotations

import logging
from typing import Any

from flask import jsonify


def api_library_release_group_labels_for_runtime(runtime: Any, mbid: str):
    """Return labels from the first release attached to a MusicBrainz release-group."""
    if not bool(getattr(runtime, "USE_MUSICBRAINZ", False)):
        return jsonify({"error": "MusicBrainz not enabled"}), 400
    mbid = str(mbid or "").strip()
    if not mbid or len(mbid) != 36:
        return jsonify({"error": "Invalid release-group MBID"}), 400
    musicbrainzngs = runtime.musicbrainzngs
    try:
        if bool(getattr(runtime, "MB_QUEUE_ENABLED", False)) and bool(getattr(runtime, "USE_MUSICBRAINZ", False)):
            rg_data = runtime.get_mb_queue().submit(
                f"rg_labels_{mbid}",
                lambda: musicbrainzngs.get_release_group_by_id(
                    mbid,
                    includes=["releases", "artist-credits"],
                ),
            )
        else:
            rg_data = musicbrainzngs.get_release_group_by_id(
                mbid,
                includes=["releases", "artist-credits"],
            )
        rg = rg_data.get("release-group", {})
        releases = rg.get("release-list") or []
        if not releases:
            return jsonify({"labels": []})
        first_release_id = releases[0].get("id")
        if not first_release_id:
            return jsonify({"labels": []})
        if bool(getattr(runtime, "MB_QUEUE_ENABLED", False)) and bool(getattr(runtime, "USE_MUSICBRAINZ", False)):
            rel_data = runtime.get_mb_queue().submit(
                f"rel_labels_{first_release_id}",
                lambda: musicbrainzngs.get_release_by_id(first_release_id, includes=["labels"]),
            )
        else:
            rel_data = musicbrainzngs.get_release_by_id(first_release_id, includes=["labels"])
        release = rel_data.get("release", {})
        label_list = release.get("label-list") or []
        labels = []
        for item in label_list:
            label = item.get("label", {}) if isinstance(item.get("label"), dict) else {}
            name = label.get("name") or item.get("name") or ""
            label_id = label.get("id") or item.get("id") or ""
            if name or label_id:
                labels.append({"name": name, "id": label_id})
        return jsonify({"labels": labels})
    except musicbrainzngs.WebServiceError as exc:
        if "404" in str(exc):
            return jsonify({"error": "Release group not found"}), 404
        raise
    except Exception as exc:
        logging.exception("Failed to get labels for release-group %s", mbid)
        return jsonify({"error": str(exc)}), 500
