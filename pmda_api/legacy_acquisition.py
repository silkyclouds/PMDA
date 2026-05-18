"""Compatibility API routes for removed external acquisition workflows."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from flask import Blueprint, jsonify

from pmda_core.legacy_integrations import disabled_autobrr_payload, disabled_lidarr_payload


def create_legacy_acquisition_blueprint(
    *,
    set_lidarr_progress: Callable[[list[tuple[Any, ...]]], None] | None = None,
) -> Blueprint:
    """Create disabled Lidarr/Autobrr compatibility routes.

    The old endpoints remain so stale clients fail explicitly with HTTP 410.
    No route in this blueprint performs network acquisition work.
    """

    blueprint = Blueprint("pmda_legacy_acquisition", __name__)

    @blueprint.post("/api/lidarr/add-album", endpoint="api_lidarr_add_album")
    def api_lidarr_add_album():
        return jsonify({"success": False, **disabled_lidarr_payload()}), 410

    @blueprint.post("/api/lidarr/add-incomplete-albums", endpoint="api_lidarr_add_incomplete_albums")
    def api_lidarr_add_incomplete_albums():
        if set_lidarr_progress is not None:
            set_lidarr_progress([])
        return jsonify(disabled_lidarr_payload(started=False)), 410

    @blueprint.get(
        "/api/lidarr/add-incomplete-albums/progress",
        endpoint="api_lidarr_add_incomplete_albums_progress",
    )
    def api_lidarr_add_incomplete_albums_progress():
        return jsonify({"running": False, "finished": False, "disabled": True}), 410

    @blueprint.post("/api/lidarr/add-artist", endpoint="api_lidarr_add_artist")
    def api_lidarr_add_artist():
        return jsonify({"success": False, **disabled_lidarr_payload()}), 410

    @blueprint.post("/api/autobrr/create-filter", endpoint="api_autobrr_create_filter")
    def api_autobrr_create_filter():
        return jsonify({"success": False, **disabled_autobrr_payload()}), 410

    @blueprint.post("/api/lidarr/test", endpoint="api_lidarr_test")
    def api_lidarr_test():
        return jsonify({"success": False, **disabled_lidarr_payload()}), 410

    @blueprint.post("/api/autobrr/test", endpoint="api_autobrr_test")
    def api_autobrr_test():
        return jsonify({"success": False, **disabled_autobrr_payload()}), 410

    return blueprint
