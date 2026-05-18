"""Integrated frontend and SPA fallback routes."""

from __future__ import annotations

import os
import re
from typing import Any

from flask import Blueprint, jsonify, redirect, request, send_from_directory


def create_frontend_blueprint(*, runtime: Any, frontend_dist: str) -> Blueprint:
    """Create static frontend routes served by the PMDA container."""

    blueprint = Blueprint("pmda_frontend", __name__)

    def _ui_build_payload():
        """Return the current Vite asset paths from dist/index.html."""

        index_path = os.path.join(frontend_dist, "index.html")
        try:
            with open(index_path, "r", encoding="utf-8") as handle:
                html = handle.read()
        except Exception as exc:
            return {"ok": False, "error": f"Failed to read index.html: {exc}"}

        js_matches = re.findall(r'src="(/assets/[^"]+\.js)"', html)
        css_matches = re.findall(r'href="(/assets/[^"]+\.css)"', html)
        asset_js = next((path for path in js_matches if "/assets/index-" in path), js_matches[0] if js_matches else None)
        asset_css = next((path for path in css_matches if "/assets/index-" in path), css_matches[0] if css_matches else None)

        try:
            index_mtime = os.path.getmtime(index_path)
        except Exception:
            index_mtime = None

        return {
            "ok": True,
            "index_mtime": index_mtime,
            "asset_js": asset_js,
            "asset_css": asset_css,
        }

    def _send_index_no_cache():
        resp = send_from_directory(frontend_dist, "index.html")
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    def _redirect_prefixed_runtime_path(target: str):
        query = request.query_string.decode("utf-8", errors="ignore")
        location = target if not query else f"{target}?{query}"
        return redirect(location, code=307)

    @blueprint.get("/api/ui/build", endpoint="api_ui_build")
    def api_ui_build():
        payload = _ui_build_payload()
        resp = jsonify(payload)
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    @blueprint.get("/", endpoint="serve_index")
    def serve_index():
        return _send_index_no_cache()

    @blueprint.get("/assets/<path:path>", endpoint="serve_assets")
    def serve_assets(path: str):
        return send_from_directory(os.path.join(frontend_dist, "assets"), path)

    @blueprint.route("/PMDA/api/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    @blueprint.route("/pmda/api/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    def serve_prefixed_api(path: str):
        return _redirect_prefixed_runtime_path(f"/api/{path}")

    @blueprint.route("/PMDA/scan/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    @blueprint.route("/pmda/scan/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    def serve_prefixed_scan_api(path: str):
        return _redirect_prefixed_runtime_path(f"/scan/{path}")

    @blueprint.route("/PMDA/dedupe/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    @blueprint.route("/pmda/dedupe/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    def serve_prefixed_dedupe_api(path: str):
        return _redirect_prefixed_runtime_path(f"/dedupe/{path}")

    @blueprint.route("/PMDA/details/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    @blueprint.route("/pmda/details/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    def serve_prefixed_details_api(path: str):
        return _redirect_prefixed_runtime_path(f"/details/{path}")

    @blueprint.get("/PMDA/assets/<path:path>")
    @blueprint.get("/pmda/assets/<path:path>")
    def serve_prefixed_assets(path: str):
        return send_from_directory(os.path.join(frontend_dist, "assets"), path)

    @blueprint.get("/PMDA")
    @blueprint.get("/PMDA/")
    @blueprint.get("/pmda")
    @blueprint.get("/pmda/")
    def serve_prefixed_index():
        return _send_index_no_cache()

    @blueprint.get("/PMDA/<path:path>")
    @blueprint.get("/pmda/<path:path>")
    def serve_prefixed_spa_fallback(path: str):
        path_obj = os.path.join(frontend_dist, path)
        if os.path.isfile(path_obj):
            return send_from_directory(frontend_dist, path)
        return _send_index_no_cache()

    @blueprint.get("/<path:path>")
    def serve_spa_fallback(path: str):
        """Serve static files from dist, or index.html for client-side routing."""

        if request.path.startswith(("/api/", "/scan/", "/dedupe/", "/details/")):
            return jsonify(error="Not found"), 404
        path_obj = os.path.join(frontend_dist, path)
        if os.path.isfile(path_obj):
            return send_from_directory(frontend_dist, path)
        return _send_index_no_cache()

    return blueprint
