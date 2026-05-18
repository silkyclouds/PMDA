"""Operator tools API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint


def create_tools_blueprint(*, runtime: Any) -> Blueprint:
    """Create tools routes while keeping public URLs stable."""

    blueprint = Blueprint("pmda_tools", __name__)

    @blueprint.get("/api/tools/trash-releases", endpoint="api_tools_trash_releases")
    def api_tools_trash_releases():
        return runtime.api_tools_trash_releases()

    @blueprint.post("/api/tools/trash-releases/action", endpoint="api_tools_trash_releases_action")
    def api_tools_trash_releases_action():
        return runtime.api_tools_trash_releases_action()

    return blueprint
