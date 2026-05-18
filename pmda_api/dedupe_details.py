"""Duplicate-review detail routes.

These routes are intentionally files-mode only. Historical PMDA builds used
Plex metadata tables as a scan source fallback here; current PMDA treats Plex
only as an optional post-publication player refresh target.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from flask import Blueprint, jsonify, request


def create_dedupe_details_blueprint(*, runtime: Any) -> Blueprint:
    """Create duplicate detail and manual purge routes."""

    blueprint = Blueprint("pmda_dedupe_details", __name__)

    @blueprint.get("/api/edition_details", endpoint="edition_details")
    def edition_details():
        try:
            album_id = int(request.args["album_id"])
            folder = Path(request.args["folder"])
        except Exception:
            return jsonify({"error": "album_id and folder query parameters are required"}), 400

        if runtime._get_library_mode() != "files":
            return (
                jsonify(
                    {
                        "error": "edition_details_files_only",
                        "message": "Edition details are available only in files mode; Plex DB source access is disabled.",
                    }
                ),
                410,
            )

        folder_access = runtime.path_for_fs_access(folder)
        resolved_album_id = runtime._files_album_id_for_folder(folder_access) or album_id
        track_list: list[dict[str, Any]] = []
        if int(resolved_album_id or 0) > 0:
            conn = runtime._files_pg_connect(acquire_timeout_sec=0.75)
            if conn is not None:
                try:
                    with conn.cursor() as cur:
                        with runtime._files_pg_statement_timeout(cur, 2000):
                            cur.execute(
                                """
                                SELECT track_num, title, COALESCE(duration_sec, 0)
                                FROM files_tracks
                                WHERE album_id = %s
                                ORDER BY disc_num ASC, track_num ASC, id ASC
                                """,
                                (int(resolved_album_id),),
                            )
                            track_list = [
                                {
                                    "idx": int(row[0] or 0),
                                    "title": str(row[1] or "").strip(),
                                    "dur": int(row[2] or 0) * 1000,
                                }
                                for row in cur.fetchall()
                            ]
                finally:
                    conn.close()

        if not track_list and folder_access.exists() and folder_access.is_dir():
            track_list = [
                {
                    "idx": int(track.get("idx") or track.get("index") or 0),
                    "title": str(track.get("title") or track.get("name") or "").strip(),
                    "dur": int(track.get("dur") or 0),
                }
                for track in runtime._duplicate_tracks_from_folder(folder_access, {"folder": str(folder_access)})
            ]

        fmt_score, bitrate, sample_rate, bit_depth, _ = runtime.analyse_format(folder_access)
        return jsonify({"tracks": track_list, "info": (fmt_score, bitrate, sample_rate, bit_depth)})

    @blueprint.post("/api/dedupe_manual", endpoint="dedupe_manual")
    def dedupe_manual():
        required = runtime._requires_config()
        if required is not None:
            return required
        if runtime.library_is_audit_mode():
            admin_gate = runtime._require_admin_json()
            if admin_gate is not None:
                return admin_gate

        req = request.get_json(force=True)
        if not isinstance(req, list):
            return jsonify({"error": "Expected a list of duplicate editions"}), 400
        for item in req:
            runtime._purge_invalid_edition(
                {
                    "folder": item["folder"],
                    "artist": "",
                    "title_raw": "",
                    "album_id": int(item["album_id"]),
                }
            )
        return jsonify({"status": "ok"})

    @blueprint.get("/api/duplicates", endpoint="api_duplicates")
    def api_duplicates():
        """Return duplicate-group cards for the Web UI."""
        files_mode = runtime._get_library_mode() == "files"
        if not runtime.PLEX_CONFIGURED and not files_mode:
            resp = jsonify([])
            resp.headers["X-PMDA-Requires-Config"] = "true"
            return resp
        source = request.args.get("source", "all").strip().lower()
        include_library_groups = source != "scan"
        if include_library_groups:
            registry = runtime._global_duplicate_review_registry(include_live=True)
            merged_groups = dict(registry.get("groups") or {})
            with runtime.lock:
                scanning_now = bool(runtime.state.get("scanning"))
                runtime.state["duplicates"] = merged_groups
            cards = runtime._build_card_list(merged_groups)
        else:
            cards = []
        with runtime.lock:
            scanning_now = bool(runtime.state.get("scanning"))
            # During a scan, workers publish duplicate groups in memory before the
            # incremental DB writer catches up. Do not clobber that live state with
            # an empty/lagging DB snapshot.
            if not include_library_groups and (
                (not scanning_now and not runtime.state["duplicates"])
                or (scanning_now and not runtime.state.get("duplicates"))
            ):
                if not runtime.state.get("_api_duplicates_load_logged") and not scanning_now:
                    logging.debug("api_duplicates(): loading scan results from DB into memory")
                    runtime.state["_api_duplicates_load_logged"] = True
                runtime.state["duplicates"] = runtime.load_scan_from_db()
            if not include_library_groups:
                cards = runtime._build_card_list(runtime.state["duplicates"])
            if not include_library_groups:
                return jsonify(cards)
        return jsonify(cards)

    @blueprint.get("/api/dedupe", endpoint="api_dedupe")
    def api_dedupe():
        return runtime.api_dedupe()

    @blueprint.get("/details/<artist>/<int:album_id>", endpoint="details")
    def details(artist: str, album_id: int):
        return runtime.details(artist, album_id)

    @blueprint.post("/dedupe/artist/<artist>", endpoint="dedupe_artist")
    def dedupe_artist(artist: str):
        return runtime.dedupe_artist(artist)

    @blueprint.post("/dedupe/move-track/<artist>", endpoint="dedupe_move_track")
    def dedupe_move_track(artist: str):
        return runtime.dedupe_move_track(artist)

    @blueprint.post("/api/dedupe/all", endpoint="api_dedupe_all")
    def api_dedupe_all():
        return runtime.api_dedupe_all()

    @blueprint.post("/dedupe/all", endpoint="dedupe_all")
    def dedupe_all():
        return runtime.dedupe_all()

    @blueprint.post("/dedupe/merge-and-dedupe", endpoint="dedupe_merge_and_dedupe")
    def dedupe_merge_and_dedupe():
        return runtime.dedupe_merge_and_dedupe()

    @blueprint.post("/dedupe/selected", endpoint="dedupe_selected")
    def dedupe_selected():
        return runtime.dedupe_selected()

    return blueprint
