"""In-UI assistant API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint


def create_assistant_blueprint(*, runtime: Any) -> Blueprint:
    """Create assistant routes while keeping public URLs stable."""

    blueprint = Blueprint("pmda_assistant", __name__)

    @blueprint.get("/api/assistant/status", endpoint="api_assistant_status")
    def api_assistant_status():
        return runtime.api_assistant_status()

    @blueprint.get("/api/assistant/session/<session_id>", endpoint="api_assistant_get_session")
    def api_assistant_get_session(session_id: str):
        return runtime.api_assistant_get_session(session_id)

    @blueprint.post("/api/assistant/chat", endpoint="api_assistant_chat")
    def api_assistant_chat():
        return runtime.api_assistant_chat()

    return blueprint
