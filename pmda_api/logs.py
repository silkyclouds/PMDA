"""Backend log API routes."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
import time
from typing import Any

from flask import Blueprint, Response, jsonify, request


def create_logs_blueprint(
    *,
    get_log_file: Callable[[], str],
    parse_bool: Callable[[Any], bool],
    recent_log_tail_entries: Callable[..., list[dict[str, Any]]],
    tail_log_entries: Callable[..., list[dict[str, Any]]],
    tail_log_lines: Callable[..., list[str]],
) -> Blueprint:
    """Create the logs blueprint while keeping legacy endpoint names stable."""

    blueprint = Blueprint("pmda_logs", __name__)

    @blueprint.get("/api/logs/tail", endpoint="api_logs_tail")
    def api_logs_tail():
        """Return recent backend logs for Scan page power-user panel."""

        try:
            lines = int(request.args.get("lines", 180))
        except Exception:
            lines = 180
        lines = max(20, min(lines, 1200))
        scan_mode = parse_bool(request.args.get("scan_mode", "true"))
        log_path = Path(str(get_log_file() or "")).expanduser()
        live_entries = recent_log_tail_entries(lines=lines, scan_mode=scan_mode)
        live_lines = [str(entry.get("raw") or "") for entry in live_entries]
        if live_entries:
            return jsonify(
                path=f"live://process-buffer ({log_path})",
                lines=live_lines,
                entries=live_entries,
            )
        return jsonify(
            path=str(log_path),
            lines=tail_log_lines(log_path, lines=lines),
            entries=tail_log_entries(log_path, lines=lines),
        )

    @blueprint.get("/api/logs/download", endpoint="api_logs_download")
    def api_logs_download():
        """Download backend logs as a text file."""

        try:
            lines = int(request.args.get("lines", 20000))
        except Exception:
            lines = 20000
        lines = max(200, min(lines, 50000))
        log_path = Path(str(get_log_file() or "")).expanduser()
        out_lines = tail_log_lines(log_path, lines=lines, max_bytes=10 * 1024 * 1024)
        payload = ("\n".join(out_lines) + "\n") if out_lines else ""
        ts = time.strftime("%Y%m%d-%H%M%S")
        fname = f"pmda-log-{ts}.log"
        return Response(
            payload,
            mimetype="text/plain",
            headers={"Content-Disposition": f'attachment; filename="{fname}"'},
        )

    return blueprint
