"""Library browse API routes.

This blueprint keeps the public browse URLs stable while the historical browse
handlers are extracted from the monolith in smaller, testable steps.
"""

from __future__ import annotations

from typing import Any

from flask import Blueprint


def create_library_browse_blueprint(*, runtime: Any) -> Blueprint:
    """Create album and artist browse routes."""

    blueprint = Blueprint("pmda_library_browse", __name__)

    @blueprint.get("/api/library/artists", endpoint="api_library_artists")
    def api_library_artists():
        return runtime.api_library_artists()

    @blueprint.get("/api/library/albums", endpoint="api_library_albums")
    def api_library_albums():
        return runtime.api_library_albums()

    return blueprint
