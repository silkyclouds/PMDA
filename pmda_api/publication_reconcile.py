"""Files publication reconcile API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint, jsonify, request


def create_publication_reconcile_blueprint(*, runtime: Any) -> Blueprint:
    """Create routes for publication reconcile jobs."""

    blueprint = Blueprint("pmda_publication_reconcile", __name__)

    @blueprint.post("/api/library/files-publication/reconcile", endpoint="api_library_files_publication_reconcile")
    def api_library_files_publication_reconcile():
        if runtime._get_library_mode() != "files":
            return jsonify({"status": "error", "message": "Files publication reconcile is only available in Files library mode"}), 400
        payload = request.get_json(silent=True) or {}
        raw_scan_ids = payload.get("scan_ids")
        scan_ids: list[int] | None = None
        if isinstance(raw_scan_ids, list):
            scan_ids = [
                int(runtime._parse_int_loose(value, 0) or 0)
                for value in raw_scan_ids
                if int(runtime._parse_int_loose(value, 0) or 0) > 0
            ]
        elif raw_scan_ids not in (None, ""):
            scan_id_value = int(runtime._parse_int_loose(raw_scan_ids, 0) or 0)
            scan_ids = [scan_id_value] if scan_id_value > 0 else None
        reason = str(payload.get("reason") or "api_manual")
        rebuild_index = bool(runtime._parse_bool(payload.get("rebuild_index", True)))
        if runtime._parse_bool(payload.get("sync")):
            result = runtime._reconcile_files_publication_from_scan_editions(
                scan_ids=scan_ids,
                reason=reason,
                rebuild_index=rebuild_index,
            )
            status_code = 200 if result.get("status") != "already_running" else 409
            return jsonify(result), status_code
        started = runtime._trigger_files_publication_reconcile_async(
            scan_ids=scan_ids,
            reason=reason,
            rebuild_index=rebuild_index,
        )
        if started:
            return jsonify({"status": "started"})
        with runtime.lock:
            st = dict(runtime.state.get("files_publication_reconcile") or {})
        return jsonify({"status": "already_running", "progress": st}), 409

    @blueprint.get("/api/library/files-publication/reconcile/status", endpoint="api_library_files_publication_reconcile_status")
    def api_library_files_publication_reconcile_status():
        with runtime.lock:
            st = dict(runtime.state.get("files_publication_reconcile") or {})
        if not st:
            st = {"status": "idle"}
        return jsonify(st)

    return blueprint
