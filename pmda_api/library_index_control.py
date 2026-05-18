"""Files library index control API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint, jsonify, request


def create_library_index_control_blueprint(*, runtime: Any) -> Blueprint:
    """Create routes that control files-library index jobs."""

    blueprint = Blueprint("pmda_library_index_control", __name__)

    @blueprint.post("/api/library/files-index/rebuild", endpoint="api_library_files_index_rebuild")
    def api_library_files_index_rebuild():
        if runtime._get_library_mode() != "files":
            return jsonify({"status": "error", "message": "Files index rebuild is only available in Files library mode"}), 400
        payload = request.get_json(silent=True) or {}
        source = str(payload.get("source") or request.args.get("source") or "").strip().lower()
        reason = "api_rebuild_filesystem" if source in {"filesystem", "filesystem_roots", "roots"} else "api_rebuild"
        if runtime._trigger_files_index_rebuild_async(reason=reason):
            return jsonify({"status": "started"})
        st = runtime._files_index_get_state()
        return jsonify({"status": "already_running", "progress": st}), 409

    return blueprint
