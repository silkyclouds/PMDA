"""Scan progress API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint


def create_progress_blueprint(*, runtime: Any) -> Blueprint:
    """Create scan progress routes while the payload builder is extracted incrementally."""

    blueprint = Blueprint("pmda_progress", __name__)

    @blueprint.get("/api/progress", endpoint="api_progress")
    @blueprint.get("/api/scan/progress", endpoint="api_scan_progress")
    def api_progress():
        return runtime.api_progress()

    return blueprint
