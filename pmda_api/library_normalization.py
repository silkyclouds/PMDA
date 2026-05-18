"""Album folder-name normalization API routes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from flask import Blueprint, jsonify, request


def create_library_normalization_blueprint(*, runtime: Any) -> Blueprint:
    """Create routes that inspect and normalize album folder names."""

    blueprint = Blueprint("pmda_library_normalization", __name__)

    @blueprint.get(
        "/api/library/albums-with-parenthetical-names",
        endpoint="api_library_albums_with_parenthetical_names",
    )
    def api_library_albums_with_parenthetical_names():
        """Return album folders with removable parenthetical suffixes."""

        if runtime._get_library_mode() != "files":
            return jsonify({"albums": [], "error": "Files mode required"}), 400
        ok, err = runtime._ensure_files_index_ready()
        if not ok:
            return jsonify({"albums": [], "error": err or "Files index unavailable"}), 503
        conn = runtime._files_pg_connect()
        if conn is None:
            return jsonify({"albums": [], "error": "PostgreSQL unavailable"}), 503
        try:
            limit = max(1, min(1000, int(request.args.get("limit", 250))))
        except (TypeError, ValueError):
            limit = 250
        try:
            offset = max(0, int(request.args.get("offset", 0)))
        except (TypeError, ValueError):
            offset = 0
        suffix_pattern = r"\s*\([^)]*\)\s*$"
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT alb.id, alb.title, alb.folder_path, art.name
                    FROM files_albums alb
                    JOIN files_artists art ON art.id = alb.artist_id
                    WHERE regexp_replace(alb.folder_path, '^.*/', '') ~ %s
                    ORDER BY art.name, alb.title
                    LIMIT %s OFFSET %s
                    """,
                    (suffix_pattern, limit + 1, offset),
                )
                rows = cur.fetchall()
            out = []
            has_more = len(rows) > limit
            for album_id, title, folder_path_raw, artist_name in rows[:limit]:
                if not folder_path_raw:
                    continue
                # Listing candidates must not touch the filesystem: doing so can
                # wake large HDD arrays just to render an admin helper page.
                folder = Path(folder_path_raw)
                current_name = folder.name
                proposed_name = runtime.strip_parenthetical_suffixes(current_name)
                if not proposed_name or proposed_name == current_name:
                    continue
                proposed_path = folder.parent / proposed_name
                out.append(
                    {
                        "album_id": int(album_id),
                        "artist": artist_name or "",
                        "title": title or current_name,
                        "current_path": str(folder),
                        "proposed_path": str(proposed_path),
                        "current_name": current_name,
                        "proposed_name": proposed_name,
                    }
                )
            return jsonify({"albums": out, "limit": limit, "offset": offset, "has_more": has_more})
        finally:
            conn.close()

    @blueprint.post("/api/library/normalize-album-names", endpoint="api_library_normalize_album_names")
    def api_library_normalize_album_names():
        """Rename album folders by removing removable parenthetical suffixes."""

        if runtime._get_library_mode() != "files":
            return jsonify({"error": "Files mode required"}), 400
        data = request.get_json(silent=True) or {}
        album_ids = data.get("album_ids")
        if album_ids is not None and not isinstance(album_ids, list):
            return jsonify({"error": "album_ids must be an array"}), 400
        ok, err = runtime._ensure_files_index_ready()
        if not ok:
            return jsonify({"error": err or "Files index unavailable"}), 503
        conn = runtime._files_pg_connect(acquire_timeout_sec=0.75)
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                if album_ids:
                    placeholders = ",".join(["%s"] * len(album_ids))
                    cur.execute(
                        f"""
                        SELECT id, folder_path
                        FROM files_albums
                        WHERE id IN ({placeholders})
                        """,
                        tuple(int(x) for x in album_ids),
                    )
                else:
                    cur.execute("SELECT id, folder_path FROM files_albums")
                rows = cur.fetchall()
        finally:
            conn.close()

        renamed = []
        errors = []
        for album_id, folder_path_raw in rows:
            if not folder_path_raw:
                continue
            folder = runtime.path_for_fs_access(Path(folder_path_raw))
            if not folder.exists():
                errors.append({"album_id": int(album_id), "message": "Folder not found"})
                continue
            current_name = folder.name
            proposed_name = runtime.strip_parenthetical_suffixes(current_name)
            if not proposed_name or proposed_name == current_name:
                continue
            proposed_path = folder.parent / proposed_name
            if proposed_path.exists():
                errors.append({"album_id": int(album_id), "path": str(proposed_path), "message": "Target path already exists"})
                continue
            try:
                folder.rename(proposed_path)
                renamed.append({"album_id": int(album_id), "from": str(folder), "to": str(proposed_path)})
            except OSError as exc:
                errors.append({"album_id": int(album_id), "message": str(exc)})
        if renamed:
            runtime._trigger_files_index_rebuild_async(reason="normalize_album_names")
        return jsonify({"renamed": renamed, "errors": errors})

    return blueprint
