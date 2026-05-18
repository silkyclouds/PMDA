"""Files cache maintenance API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint, jsonify, request


def create_files_cache_blueprint(*, runtime: Any) -> Blueprint:
    """Create files cache maintenance endpoints."""

    blueprint = Blueprint("pmda_files_cache", __name__)

    @blueprint.get("/api/files/cache/quality-recalc", endpoint="api_files_cache_quality_recalc_status")
    def api_files_cache_quality_recalc_status():
        with runtime.lock:
            status_payload = runtime._files_cache_quality_recalc_status_unlocked()
            status_payload["prescan_snapshot_running"] = bool(runtime.state.get("scan_prescan_cache_snapshot_running"))
            status_payload["prescan_snapshot_rows"] = int(runtime.state.get("scan_prescan_cache_snapshot_rows") or 0)
            status_payload["prescan_snapshot_updated_at"] = runtime.state.get("scan_prescan_cache_snapshot_updated_at")
        return jsonify(status_payload)

    @blueprint.post("/api/files/cache/quality-recalc", endpoint="api_files_cache_quality_recalc_start")
    def api_files_cache_quality_recalc_start():
        if runtime._get_library_mode() != "files":
            return jsonify({"status": "blocked", "reason": "files_mode_only"}), 409

        data = request.get_json(silent=True) or {}
        prefer_prescan_snapshot = runtime._parse_bool(data.get("prefer_prescan_snapshot", True))
        force_full_pass = runtime._parse_bool(data.get("force_full_pass", False))
        batch_size = runtime._parse_int_loose(data.get("batch_size"), 500) or 500
        batch_size = max(100, min(int(batch_size), 5000))
        source_id_raw = runtime._parse_int_loose(data.get("source_id"), 0) or 0
        source_id = int(source_id_raw) if int(source_id_raw) > 0 else None
        limit_raw = runtime._parse_int_loose(data.get("limit"), 0) or 0
        limit = int(limit_raw) if int(limit_raw) > 0 else None

        with runtime.lock:
            if bool(runtime.state.get("files_cache_quality_recalc_running")):
                return jsonify({"status": "running", **runtime._files_cache_quality_recalc_status_unlocked()}), 202
            scan_running = bool(runtime.state.get("scanning"))
            files_map = runtime.state.get("files_editions_by_album_id") or {}
            has_prescan_map = isinstance(files_map, dict) and bool(files_map)

        if prefer_prescan_snapshot and not force_full_pass and scan_running and has_prescan_map:
            triggered = runtime._trigger_prescan_cache_snapshot_async(reason="manual_quality_recalc")
            if triggered:
                return jsonify(
                    {
                        "status": "started",
                        "mode": "prescan_snapshot",
                        "message": "Using current pre-scan payload to refresh files cache quality flags.",
                    }
                )

        if scan_running and not force_full_pass:
            return (
                jsonify(
                    {
                        "status": "blocked",
                        "reason": "scan_running",
                        "message": "A scan is running. Pause it to snapshot pre-scan data, or retry with force_full_pass=true.",
                    }
                ),
                409,
            )

        started, payload = runtime._start_files_cache_quality_recalc_async(
            batch_size=batch_size,
            source_id=source_id,
            limit=limit,
            reason="manual_api",
        )
        if not started:
            return jsonify({"status": "running", **payload}), 202
        return jsonify({"status": "started", "mode": "full_pass", **payload})

    return blueprint
