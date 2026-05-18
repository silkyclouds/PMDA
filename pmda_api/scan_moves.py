"""Scan move audit API routes."""

from __future__ import annotations

from pathlib import Path
import json
import sqlite3
import subprocess
import sys
from typing import Any

from flask import Blueprint, jsonify, request


def create_scan_moves_blueprint(*, runtime: Any) -> Blueprint:
    """Create scan-move audit endpoints."""

    blueprint = Blueprint("pmda_scan_moves", __name__)

    @blueprint.get("/api/statistics/scan-moves-audit", endpoint="api_statistics_scan_moves_audit")
    def api_statistics_scan_moves_audit():
        scan_id_raw = runtime._parse_int_loose(request.args.get("scan_id"), 0)
        scan_id = int(scan_id_raw or 0)
        try:
            if scan_id <= 0:
                con = sqlite3.connect(str(runtime.STATE_DB_FILE))
                cur = con.cursor()
                cur.execute("SELECT MAX(scan_id) FROM scan_history")
                row = cur.fetchone()
                con.close()
                scan_id = int((row or [0])[0] or 0)
        except Exception as exc:
            return jsonify({"error": f"Could not resolve scan_id: {exc}"}), 500
        if scan_id <= 0:
            return jsonify({"error": "No scan history found"}), 404

        script_path = Path(str(getattr(runtime, "__file__", "pmda.py"))).resolve().parent / "tools" / "audit_scan_moves.py"
        if not script_path.exists():
            return jsonify({"error": f"Audit script not found: {script_path}"}), 404

        try:
            proc = subprocess.run(
                [
                    sys.executable,
                    str(script_path),
                    "--db",
                    str(runtime.STATE_DB_FILE),
                    "--scan-id",
                    str(scan_id),
                ],
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
        except Exception as exc:
            return jsonify({"error": f"Audit script execution failed: {exc}"}), 500

        if proc.returncode != 0:
            return (
                jsonify(
                    {
                        "error": "Audit script failed",
                        "scan_id": scan_id,
                        "exit_code": int(proc.returncode),
                        "stderr": (proc.stderr or "").strip()[:4000],
                    }
                ),
                500,
            )
        try:
            report = json.loads((proc.stdout or "").strip() or "{}")
        except Exception as exc:
            return (
                jsonify(
                    {
                        "error": f"Audit script returned invalid JSON: {exc}",
                        "scan_id": scan_id,
                        "stdout": (proc.stdout or "").strip()[:4000],
                    }
                ),
                500,
            )
        return jsonify(report)

    @blueprint.get("/api/scan-history/<int:scan_id>/moves", endpoint="api_scan_history_moves")
    def api_scan_history_moves(scan_id: int):
        return runtime.api_scan_history_moves(scan_id)

    @blueprint.get("/api/scan-history/<int:scan_id>/moves/summary", endpoint="api_scan_history_moves_summary")
    def api_scan_history_moves_summary(scan_id: int):
        return runtime.api_scan_history_moves_summary(scan_id)

    @blueprint.get("/api/scan-move/<int:move_id>/artwork", endpoint="api_scan_move_artwork")
    def api_scan_move_artwork(move_id: int):
        return runtime.api_scan_move_artwork(move_id)

    @blueprint.get("/api/scan-move/<int:move_id>/detail", endpoint="api_scan_move_detail")
    def api_scan_move_detail(move_id: int):
        return runtime.api_scan_move_detail(move_id)

    @blueprint.post("/api/scan-history/<int:scan_id>/restore", endpoint="api_scan_history_restore")
    def api_scan_history_restore(scan_id: int):
        return runtime.api_scan_history_restore(scan_id)

    @blueprint.post("/api/scan-history/<int:scan_id>/dedupe", endpoint="api_scan_history_dedupe")
    def api_scan_history_dedupe(scan_id: int):
        return runtime.api_scan_history_dedupe(scan_id)

    return blueprint
