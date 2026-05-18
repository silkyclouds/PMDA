"""Post-publication player control API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint, jsonify, request


def create_player_blueprint(*, runtime: Any) -> Blueprint:
    """Create routes for checking and refreshing external player targets."""

    blueprint = Blueprint("pmda_player", __name__)

    @blueprint.post("/api/player/check", endpoint="api_player_check")
    def api_player_check():
        """Test connectivity for the requested post-publication player refresh target."""

        data = request.get_json(silent=True) or {}
        target = runtime._effective_player_target(data.get("target"))
        if target == "none":
            return jsonify({"success": False, "target": "none", "message": "No player target selected"}), 400
        if target == "plex":
            ok, msg = runtime._do_plex_check(
                (data.get("PLEX_HOST") or "").strip() or getattr(runtime, "PLEX_HOST", ""),
                (data.get("PLEX_TOKEN") or "").strip() or getattr(runtime, "PLEX_TOKEN", ""),
            )
            return jsonify({"success": ok, "target": "plex", "message": msg}), (200 if ok else 400)
        if target == "jellyfin":
            ok, msg = runtime._do_jellyfin_check(
                (data.get("JELLYFIN_URL") or "").strip() or getattr(runtime, "JELLYFIN_URL", ""),
                (data.get("JELLYFIN_API_KEY") or "").strip() or getattr(runtime, "JELLYFIN_API_KEY", ""),
            )
            return jsonify({"success": ok, "target": "jellyfin", "message": msg}), (200 if ok else 400)
        if target == "navidrome":
            ok, msg = runtime._do_navidrome_check(
                (data.get("NAVIDROME_URL") or "").strip() or getattr(runtime, "NAVIDROME_URL", ""),
                (data.get("NAVIDROME_USERNAME") or "").strip() or getattr(runtime, "NAVIDROME_USERNAME", ""),
                (data.get("NAVIDROME_PASSWORD") or "").strip() or getattr(runtime, "NAVIDROME_PASSWORD", ""),
                (data.get("NAVIDROME_API_KEY") or "").strip() or getattr(runtime, "NAVIDROME_API_KEY", ""),
            )
            return jsonify({"success": ok, "target": "navidrome", "message": msg}), (200 if ok else 400)
        return jsonify({"success": False, "target": target, "message": "Unknown target"}), 400

    @blueprint.post("/api/player/refresh", endpoint="api_player_refresh")
    def api_player_refresh():
        """Trigger a media-library refresh on the selected external player target."""

        data = request.get_json(silent=True) or {}
        target = runtime._effective_player_target(data.get("target"))
        ok, msg = runtime._trigger_player_refresh_by_target(target)
        return jsonify({"success": ok, "target": target, "message": msg}), (200 if ok else 400)

    return blueprint
