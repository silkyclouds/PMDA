"""Files profile backfill API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint, jsonify, request


def create_profile_backfill_blueprint(*, runtime: Any) -> Blueprint:
    """Create routes for profile/artwork backfill control."""

    blueprint = Blueprint("pmda_profile_backfill", __name__)

    @blueprint.get("/api/library/files-profile-backfill/status", endpoint="api_library_files_profile_backfill_status")
    def api_library_files_profile_backfill_status():
        with runtime._files_profile_backfill_lock:
            st = dict(runtime._files_profile_backfill_state or {})
        if not st:
            st = {"running": False}
        pending = runtime._files_profile_backfill_pending_work()
        st["pending_artist_profiles"] = int(pending.get("pending_artist_profiles") or st.get("pending_artist_profiles") or 0)
        st["pending_album_profiles"] = int(pending.get("pending_album_profiles") or st.get("pending_album_profiles") or 0)
        st["eligible_album_profiles"] = int(pending.get("eligible_album_profiles") or st.get("eligible_album_profiles") or 0)
        st["pending_album_covers"] = int(pending.get("pending_album_covers") or st.get("pending_album_covers") or 0)
        st["idle_autostart"] = dict(runtime._files_profile_backfill_idle_state or {})
        return jsonify(st)

    @blueprint.post("/api/library/files-profile-backfill/start", endpoint="api_library_files_profile_backfill_start")
    def api_library_files_profile_backfill_start():
        if runtime._get_library_mode() != "files":
            return jsonify({"status": "error", "message": "Backfill is only available in Files library mode"}), 400
        payload = request.get_json(silent=True) or {}
        reason = str(payload.get("reason") or "api_manual")
        cover_only = bool(runtime._parse_bool(payload.get("cover_only")))
        if runtime._trigger_files_profile_backfill_async(reason=reason, cover_only=cover_only):
            return jsonify({"status": "started"})
        with runtime._files_profile_backfill_lock:
            st = dict(runtime._files_profile_backfill_state or {})
        return jsonify({"status": "already_running", "progress": st}), 409

    @blueprint.post("/api/library/files-profile-backfill/stop", endpoint="api_library_files_profile_backfill_stop")
    def api_library_files_profile_backfill_stop():
        with runtime._files_profile_backfill_lock:
            runtime._files_profile_backfill_state["running"] = False
        return jsonify({"status": "stopping"})

    return blueprint
