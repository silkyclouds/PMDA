"""Administrative maintenance and operations API routes."""

from __future__ import annotations

import logging
from typing import Any

from flask import Blueprint, jsonify, request


def _normalize_actions(raw_actions: Any) -> list[str]:
    if isinstance(raw_actions, str):
        actions = [raw_actions]
    elif isinstance(raw_actions, list):
        actions = raw_actions
    else:
        actions = []

    normalized: list[str] = []
    for item in actions:
        key = str(item or "").strip().lower()
        if key and key not in normalized:
            normalized.append(key)
    return normalized


def create_admin_ops_blueprint(*, runtime: Any) -> Blueprint:
    """Create admin maintenance and backup routes while keeping paths stable."""

    blueprint = Blueprint("pmda_admin_ops", __name__)

    @blueprint.post("/api/admin/maintenance/reset", endpoint="api_admin_maintenance_reset")
    def api_admin_maintenance_reset():
        admin_gate = runtime._require_admin_json()
        if admin_gate is not None:
            return admin_gate

        data = request.get_json(silent=True) or {}
        normalized_actions = _normalize_actions(data.get("actions"))

        allowed = {"media_cache", "state_db", "cache_db", "settings_db", "files_index"}
        invalid = [action for action in normalized_actions if action not in allowed]
        if invalid:
            return jsonify({"status": "error", "message": f"Invalid action(s): {', '.join(invalid)}"}), 400
        if not normalized_actions:
            return jsonify({"status": "error", "message": "No maintenance action requested"}), 400

        if "settings_db" in normalized_actions and not bool(runtime._parse_bool(data.get("confirm_settings_db", False))):
            return jsonify(
                {
                    "status": "error",
                    "message": "settings_db reset requires confirm_settings_db=true",
                }
            ), 400
        if "files_index" in normalized_actions and not bool(runtime._parse_bool(data.get("confirm_files_index", False))):
            return jsonify(
                {
                    "status": "error",
                    "message": "files_index reset requires confirm_files_index=true",
                }
            ), 400

        with runtime.lock:
            scan_running = bool(runtime.state.get("scanning"))
        if scan_running:
            return jsonify(
                {
                    "status": "blocked",
                    "message": "A scan is currently running. Stop the scan before maintenance reset.",
                }
            ), 409

        results: dict[str, Any] = {}
        warnings: list[str] = []
        errors: list[str] = []

        for action in normalized_actions:
            try:
                if action == "media_cache":
                    result = runtime._maintenance_clear_media_cache()
                elif action == "state_db":
                    result = runtime._maintenance_reset_sqlite_db(runtime.STATE_DB_FILE, runtime.init_state_db)
                elif action == "cache_db":
                    result = runtime._maintenance_reset_sqlite_db(runtime.CACHE_DB_FILE, runtime.init_cache_db)
                elif action == "settings_db":
                    result = runtime._maintenance_reset_sqlite_db(runtime.SETTINGS_DB_FILE, runtime.init_settings_db)
                elif action == "files_index":
                    result = runtime._maintenance_clear_files_index()
                else:
                    result = {"ok": False, "error": f"Unsupported action: {action}"}
            except Exception as exc:
                logging.exception("Maintenance action '%s' failed: %s", action, exc)
                result = {"ok": False, "error": str(exc)}
            results[action] = result
            if not bool(result.get("ok", False)):
                msg = str(result.get("error") or f"{action} failed").strip()
                if msg:
                    errors.append(f"{action}: {msg}")

        runtime._files_cache_invalidate_all()
        restart_requested = bool(runtime._parse_bool(data.get("restart", True)))
        restart_initiated = False
        if restart_requested:
            restart_initiated = bool(runtime._restart_container())
            if not restart_initiated:
                warnings.append("Container restart could not be initiated automatically.")

        status = "ok" if not errors else "partial"
        message = "Maintenance actions completed."
        if errors:
            message = "Maintenance actions completed with errors."
        if restart_requested and restart_initiated:
            message += " PMDA restart initiated."

        return jsonify(
            {
                "status": status,
                "message": message,
                "actions": normalized_actions,
                "results": results,
                "warnings": warnings,
                "errors": errors,
                "restart_initiated": restart_initiated,
            }
        )

    @blueprint.get("/api/admin/ops/snapshot", endpoint="api_admin_ops_snapshot")
    def api_admin_ops_snapshot():
        admin_gate = runtime._require_admin_json()
        if admin_gate is not None:
            return admin_gate
        return jsonify(runtime._ops_snapshot_payload())

    @blueprint.post("/api/admin/ops/backup", endpoint="api_admin_ops_backup")
    def api_admin_ops_backup():
        admin_gate = runtime._require_admin_json()
        if admin_gate is not None:
            return admin_gate
        data = request.get_json(silent=True) or {}
        include_pg_dump = bool(runtime._parse_bool(data.get("include_pg_dump", True)))
        result = runtime._ops_create_backup_bundle(include_pg_dump=include_pg_dump)
        code = 200 if str(result.get("status") or "") in {"ok", "partial"} else 500
        return jsonify(result), code

    return blueprint
