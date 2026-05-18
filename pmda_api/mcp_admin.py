"""MCP administration and local tool proxy API routes."""

from __future__ import annotations

import logging
import time
from typing import Any

from flask import Blueprint, g, jsonify, request


def create_mcp_admin_blueprint(*, runtime: Any) -> Blueprint:
    """Create MCP admin/token routes and the authenticated MCP tool endpoint."""

    blueprint = Blueprint("pmda_mcp_admin", __name__)

    @blueprint.get("/api/admin/mcp/status", endpoint="api_admin_mcp_status")
    def api_admin_mcp_status():
        try:
            limit = max(1, min(100, int(request.args.get("audit_limit") or 20)))
        except Exception:
            limit = 20
        return jsonify(runtime._mcp_status_summary(include_audit=True, audit_limit=limit))

    @blueprint.get("/api/admin/mcp/audit", endpoint="api_admin_mcp_audit")
    def api_admin_mcp_audit():
        try:
            limit = max(1, min(200, int(request.args.get("limit") or 50)))
        except Exception:
            limit = 50
        return jsonify({"items": runtime._mcp_latest_audit(limit), "limit": limit})

    @blueprint.post("/api/admin/mcp/token/rotate", endpoint="api_admin_mcp_token_rotate")
    def api_admin_mcp_token_rotate():
        data = request.get_json(silent=True) or {}
        scopes = runtime._mcp_normalize_scopes(data.get("scopes") or list(runtime.MCP_DEFAULT_SCOPES))
        raw_token, token = runtime._mcp_generate_token(scopes)
        return jsonify(
            {
                "status": "ok",
                "token": raw_token,
                "token_display_once": True,
                "active_token": token,
                "mcp": runtime._mcp_status_summary(include_audit=True),
                "message": "Copy this token now. PMDA will not show it again.",
            }
        )

    @blueprint.post("/api/admin/mcp/token/revoke", endpoint="api_admin_mcp_token_revoke")
    def api_admin_mcp_token_revoke():
        revoked = runtime._mcp_revoke_active_tokens()
        return jsonify({"status": "ok", "revoked": revoked, "mcp": runtime._mcp_status_summary(include_audit=True)})

    @blueprint.post("/api/mcp/tool", endpoint="api_mcp_tool_call")
    def api_mcp_tool_call():
        if not runtime._mcp_enabled():
            return jsonify({"error": "mcp_disabled", "code": "mcp_disabled"}), 403
        token = dict(getattr(g, "mcp_token", {}) or {})
        if not token:
            return jsonify({"error": "mcp_token_required", "code": "mcp_token_required"}), 401

        data = request.get_json(silent=True) or {}
        tool = str(data.get("tool") or data.get("name") or "").strip()
        args = data.get("args")
        if args is None:
            args = data.get("arguments")
        if not isinstance(args, dict):
            args = {}
        if not tool:
            return jsonify({"error": "tool_required", "code": "tool_required"}), 400

        start = time.time()
        try:
            result = runtime._mcp_dispatch_tool(tool, args, token)
            duration_ms = int(max(0.0, (time.time() - start) * 1000.0))
            runtime._mcp_audit(tool, "ok", "", args, duration_ms)
            return jsonify({"ok": True, "tool": tool, "result": result})
        except PermissionError as exc:
            duration_ms = int(max(0.0, (time.time() - start) * 1000.0))
            message = str(exc or "scope_denied")
            runtime._mcp_audit(tool, "denied", message, args, duration_ms)
            return jsonify({"ok": False, "error": "scope_denied", "code": message}), 403
        except KeyError as exc:
            duration_ms = int(max(0.0, (time.time() - start) * 1000.0))
            message = str(exc or "unknown_tool").strip("'")
            runtime._mcp_audit(tool, "unknown_tool", message, args, duration_ms)
            return jsonify({"ok": False, "error": "unknown_tool", "code": message}), 404
        except ValueError as exc:
            duration_ms = int(max(0.0, (time.time() - start) * 1000.0))
            message = str(exc or "bad_request")
            runtime._mcp_audit(tool, "bad_request", message, args, duration_ms)
            return jsonify({"ok": False, "error": "bad_request", "message": message}), 400
        except Exception as exc:
            duration_ms = int(max(0.0, (time.time() - start) * 1000.0))
            message = str(exc or "tool_failed")
            runtime._mcp_audit(tool, "error", message, args, duration_ms)
            logging.exception("MCP tool failed: %s", tool)
            return jsonify({"ok": False, "error": "tool_failed", "message": message}), 500

    return blueprint
