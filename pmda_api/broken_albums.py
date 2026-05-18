"""Broken/incomplete album review API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint


def create_broken_albums_blueprint(*, runtime: Any) -> Blueprint:
    """Create broken-album routes while their heavy payload builders are extracted."""

    blueprint = Blueprint("pmda_broken_albums", __name__)

    @blueprint.get("/api/broken-albums", endpoint="api_broken_albums")
    def api_broken_albums():
        return runtime.api_broken_albums()

    @blueprint.get("/api/broken-albums/detail", endpoint="api_broken_album_detail")
    def api_broken_album_detail():
        return runtime.api_broken_album_detail()

    return blueprint
