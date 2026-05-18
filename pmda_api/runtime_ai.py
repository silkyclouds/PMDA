"""AI provider and managed runtime API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint


def create_runtime_ai_blueprint(*, runtime: Any) -> Blueprint:
    """Create routes for AI provider checks, OAuth, models, and local runtimes."""

    blueprint = Blueprint("pmda_runtime_ai", __name__)

    @blueprint.post("/api/openai/check", endpoint="api_openai_check")
    def api_openai_check():
        return runtime.api_openai_check()

    @blueprint.get("/api/lastfm/auth/status", endpoint="api_lastfm_auth_status")
    def api_lastfm_auth_status():
        return runtime.api_lastfm_auth_status()

    @blueprint.post("/api/lastfm/auth/start", endpoint="api_lastfm_auth_start")
    def api_lastfm_auth_start():
        return runtime.api_lastfm_auth_start()

    @blueprint.get("/api/lastfm/auth/callback", endpoint="api_lastfm_auth_callback")
    def api_lastfm_auth_callback():
        return runtime.api_lastfm_auth_callback()

    @blueprint.post("/api/lastfm/auth/complete", endpoint="api_lastfm_auth_complete")
    def api_lastfm_auth_complete():
        return runtime.api_lastfm_auth_complete()

    @blueprint.post("/api/lastfm/auth/disconnect", endpoint="api_lastfm_auth_disconnect")
    def api_lastfm_auth_disconnect():
        return runtime.api_lastfm_auth_disconnect()

    @blueprint.post("/api/ai/providers/openai-codex/oauth/device/start", endpoint="api_openai_codex_oauth_device_start")
    def api_openai_codex_oauth_device_start():
        return runtime.api_openai_codex_oauth_device_start()

    @blueprint.post("/api/ai/providers/openai-codex/oauth/device/poll", endpoint="api_openai_codex_oauth_device_poll")
    def api_openai_codex_oauth_device_poll():
        return runtime.api_openai_codex_oauth_device_poll()

    @blueprint.get("/api/ai/providers/openai-codex/oauth/status", endpoint="api_openai_codex_oauth_status")
    def api_openai_codex_oauth_status():
        return runtime.api_openai_codex_oauth_status()

    @blueprint.post("/api/ai/providers/openai-codex/oauth/disconnect", endpoint="api_openai_codex_oauth_disconnect")
    def api_openai_codex_oauth_disconnect():
        return runtime.api_openai_codex_oauth_disconnect()

    @blueprint.post("/api/openai/oauth/device/start", endpoint="api_openai_oauth_device_start")
    def api_openai_oauth_device_start():
        return runtime.api_openai_oauth_device_start()

    @blueprint.post("/api/openai/oauth/device/poll", endpoint="api_openai_oauth_device_poll")
    def api_openai_oauth_device_poll():
        return runtime.api_openai_oauth_device_poll()

    @blueprint.get("/api/openai/oauth/status", endpoint="api_openai_oauth_status_legacy")
    def api_openai_oauth_status_legacy():
        return runtime.api_openai_oauth_status_legacy()

    @blueprint.post("/api/openai/oauth/disconnect", endpoint="api_openai_oauth_disconnect_legacy")
    def api_openai_oauth_disconnect_legacy():
        return runtime.api_openai_oauth_disconnect_legacy()

    @blueprint.route("/api/musicbrainz/test", methods=["GET", "POST"], endpoint="api_musicbrainz_test")
    def api_musicbrainz_test():
        return runtime.api_musicbrainz_test()

    @blueprint.route("/api/openai/models", methods=["GET", "POST"], endpoint="api_openai_models")
    def api_openai_models():
        return runtime.api_openai_models()

    @blueprint.post("/api/anthropic/models", endpoint="api_anthropic_models")
    def api_anthropic_models():
        return runtime.api_anthropic_models()

    @blueprint.post("/api/google/models", endpoint="api_google_models")
    def api_google_models():
        return runtime.api_google_models()

    @blueprint.post("/api/ollama/models", endpoint="api_ollama_models")
    def api_ollama_models():
        return runtime.api_ollama_models()

    @blueprint.get("/api/ollama/discover", endpoint="api_ollama_discover")
    def api_ollama_discover():
        return runtime.api_ollama_discover()

    @blueprint.get("/api/ollama/pull/status", endpoint="api_ollama_pull_status")
    def api_ollama_pull_status():
        return runtime.api_ollama_pull_status()

    @blueprint.post("/api/ollama/pull", endpoint="api_ollama_pull")
    def api_ollama_pull():
        return runtime.api_ollama_pull()

    @blueprint.get("/api/runtime/managed/status", endpoint="api_runtime_managed_status")
    def api_runtime_managed_status():
        return runtime.api_runtime_managed_status()

    @blueprint.get("/api/runtime/managed/logs", endpoint="api_runtime_managed_logs")
    def api_runtime_managed_logs():
        return runtime.api_runtime_managed_logs()

    @blueprint.post("/api/runtime/managed/bootstrap", endpoint="api_runtime_managed_bootstrap")
    def api_runtime_managed_bootstrap():
        return runtime.api_runtime_managed_bootstrap()

    @blueprint.post("/api/runtime/managed/adopt", endpoint="api_runtime_managed_adopt")
    def api_runtime_managed_adopt():
        return runtime.api_runtime_managed_adopt()

    @blueprint.post("/api/runtime/managed/action", endpoint="api_runtime_managed_action")
    def api_runtime_managed_action():
        return runtime.api_runtime_managed_action()

    @blueprint.post("/api/ai/models", endpoint="api_ai_models")
    def api_ai_models():
        return runtime.api_ai_models()

    return blueprint
