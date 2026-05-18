"""Settings and configuration API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint


def create_settings_config_blueprint(*, runtime: Any) -> Blueprint:
    """Create configuration and AI preference routes."""

    blueprint = Blueprint("pmda_settings_config", __name__)

    @blueprint.get("/api/config", endpoint="api_config_get")
    def api_config_get():
        return runtime.api_config_get()

    @blueprint.put("/api/config", endpoint="api_config_put")
    def api_config_put():
        return runtime.api_config_put()

    @blueprint.get("/api/ai/providers/preferences", endpoint="api_ai_provider_preferences_get")
    def api_ai_provider_preferences_get():
        return runtime.api_ai_provider_preferences_get()

    @blueprint.put("/api/ai/providers/preferences", endpoint="api_ai_provider_preferences_put")
    def api_ai_provider_preferences_put():
        return runtime.api_ai_provider_preferences_put()

    return blueprint
