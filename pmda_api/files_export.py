"""Files-library export and promotion API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint, jsonify, request


def create_files_export_blueprint(*, runtime: Any) -> Blueprint:
    """Create Files export/materialization routes while preserving legacy URLs."""

    blueprint = Blueprint("pmda_files_export", __name__)

    def _files_mode_error(message: str):
        return jsonify({"status": "error", "message": message}), 400

    def _export_already_running(message: str):
        return jsonify({"status": "already_running", "message": message}), 409

    def _export_progress_running() -> bool:
        with runtime.lock:
            prog = runtime.state.get("export_progress") or {}
            return bool(prog.get("running"))

    def _latest_completed_scan_id() -> int:
        try:
            con = runtime._state_connect_readonly(timeout=10.0)
            try:
                cur = con.cursor()
                cur.execute(
                    """
                    SELECT scan_id
                    FROM scan_history
                    WHERE status = 'completed'
                    ORDER BY start_time DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                return int(row["scan_id"] or 0) if row else 0
            finally:
                con.close()
        except Exception:
            return 0

    @blueprint.post("/api/files/export/rebuild", endpoint="api_files_export_rebuild")
    def api_files_export_rebuild():
        """Start rebuilding the exported library in the background."""
        if runtime._get_library_mode() != "files":
            return _files_mode_error("Export is only available in Files library mode")
        if _export_progress_running():
            return _export_already_running("Export already in progress")
        started = runtime._trigger_export_library_async(reason="api_export_rebuild")
        if not started:
            return _export_already_running("Export already in progress")
        return jsonify({"status": "started"})

    @blueprint.post("/api/files/export/reconcile", endpoint="api_files_export_reconcile")
    def api_files_export_reconcile():
        """Re-materialize the clean library using the currently selected strategy."""
        if runtime._get_library_mode() != "files":
            return _files_mode_error("Export reconcile is only available in Files library mode")
        started = runtime._trigger_export_library_async(reason="materialization_reconcile")
        if not started:
            return _export_already_running("Export or reconcile already in progress")
        return jsonify({"status": "started"})

    @blueprint.post("/api/files/export/scan-strict", endpoint="api_files_export_scan_strict")
    def api_files_export_scan_strict():
        """Materialize strict matches from a completed scan without rediscovering the full library."""
        if runtime._get_library_mode() != "files":
            return _files_mode_error("Scan strict export is only available in Files library mode")
        payload = request.get_json(silent=True) or {}
        scan_id_raw = payload.get("scan_id") or request.args.get("scan_id") or 0
        scan_id = int(runtime._parse_int_loose(scan_id_raw, 0) or 0)
        if scan_id <= 0:
            scan_id = _latest_completed_scan_id()
        if scan_id <= 0:
            return jsonify({"status": "error", "message": "No completed scan_id available"}), 400
        if _export_progress_running():
            return _export_already_running("Export already in progress")
        started = runtime._trigger_scan_strict_match_export_async(
            scan_id,
            reason=f"api_scan_{scan_id}_strict_export",
        )
        if not started:
            return _export_already_running("Export already in progress or scan_id unavailable")
        return jsonify({"status": "started", "scan_id": scan_id})

    @blueprint.post("/api/files/export/strict-backlog", endpoint="api_files_export_strict_backlog")
    def api_files_export_strict_backlog():
        """Materialize all strict verified Files albums that are still outside the export root."""
        if runtime._get_library_mode() != "files":
            return _files_mode_error("Strict backlog export is only available in Files library mode")
        payload = request.get_json(silent=True) or {}
        limit = int(runtime._parse_int_loose(payload.get("limit") or request.args.get("limit"), 0) or 0)
        if _export_progress_running():
            return _export_already_running("Export already in progress")
        started = runtime._trigger_strict_export_backlog_async(
            limit=(limit if limit > 0 else None),
            reason="api_strict_backlog_export",
        )
        if not started:
            return _export_already_running("Export already in progress")
        summary = runtime._strict_export_backlog_summary()
        return jsonify(
            {
                "status": "started",
                "limit": limit if limit > 0 else None,
                "backlog": summary,
            }
        )

    @blueprint.post("/api/files/match/smart-promote", endpoint="api_files_match_smart_promote")
    def api_files_match_smart_promote():
        """Promote provider-only albums that now pass smart move-safe identity checks."""
        if runtime._get_library_mode() != "files":
            return _files_mode_error("Smart provider promotion is only available in Files library mode")
        payload = request.get_json(silent=True) or {}
        limit = int(runtime._parse_int_loose(payload.get("limit") or request.args.get("limit"), 0) or 0)
        export_after_raw = payload.get("export_after")
        if export_after_raw is None:
            export_after_raw = request.args.get("export_after")
        export_after = True if export_after_raw is None else runtime._parse_bool(export_after_raw)
        started = runtime._trigger_smart_provider_promotion_async(
            limit=(limit if limit > 0 else None),
            export_after=export_after,
            reason="api_smart_provider_promotion",
        )
        if not started:
            return jsonify({"status": "already_running", "progress": runtime._smart_provider_promotion_status()}), 409
        return jsonify(
            {
                "status": "started",
                "limit": limit if limit > 0 else None,
                "export_after": bool(export_after),
                "progress": runtime._smart_provider_promotion_status(),
            }
        )

    @blueprint.get("/api/files/match/smart-promote/status", endpoint="api_files_match_smart_promote_status")
    def api_files_match_smart_promote_status():
        return jsonify(runtime._smart_provider_promotion_status())

    @blueprint.get("/api/files/export/status", endpoint="api_files_export_status")
    def api_files_export_status():
        """Return current export progress."""
        with runtime.lock:
            prog = runtime.state.get("export_progress")
        if not prog:
            return jsonify(
                {
                    "running": False,
                    "tracks_done": 0,
                    "total_tracks": 0,
                    "albums_done": 0,
                    "total_albums": 0,
                    "error": None,
                }
            )
        return jsonify(
            {
                "running": prog.get("running", False),
                "tracks_done": prog.get("tracks_done", 0),
                "total_tracks": prog.get("total_tracks", 0),
                "albums_done": prog.get("albums_done", 0),
                "total_albums": prog.get("total_albums", 0),
                "error": prog.get("error"),
                "reason": prog.get("reason"),
                "strict_backlog": prog.get("strict_backlog", False),
                "moved_count": prog.get("moved_count", 0),
                "published_count": prog.get("published_count", 0),
                "conflicts_held": prog.get("conflicts_held", 0),
                "smart_provider_promotion": runtime._smart_provider_promotion_status(),
            }
        )

    return blueprint
