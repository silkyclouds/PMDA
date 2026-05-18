"""User likes, ratings and listening-note API routes."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from flask import Blueprint, jsonify, request


def create_user_feedback_blueprint(*, runtime: Any) -> Blueprint:
    """Create user feedback routes for files-mode library entities."""

    blueprint = Blueprint("pmda_user_feedback", __name__)

    @blueprint.get("/api/library/likes", endpoint="api_library_likes_get")
    def api_library_likes_get():
        """Return like state for one or more entities."""

        if runtime._get_library_mode() != "files":
            return jsonify({"items": [], "error": "Files mode required"}), 400
        ok, err = runtime._ensure_files_index_ready()
        if not ok:
            return jsonify({"items": [], "error": err or "Files index unavailable"}), 503
        uid = runtime._current_user_id_or_zero()
        if uid <= 0:
            return jsonify({"items": [], "error": "Authentication required"}), 401
        entity_type = (request.args.get("entity_type") or "").strip().lower()
        if not runtime._social_entity_type_allowed(entity_type):
            return jsonify({"items": [], "error": "unsupported entity_type"}), 400
        ids_raw = str(request.args.get("ids") or "").strip()
        keys_raw = str(request.args.get("keys") or "").strip()
        limit = max(1, min(1000, runtime._parse_int_loose(request.args.get("limit"), 250)))

        ids: list[int] = []
        if ids_raw:
            for part in ids_raw.split(","):
                part = part.strip()
                if not part:
                    continue
                try:
                    n = int(part)
                except ValueError:
                    continue
                if n > 0:
                    ids.append(n)
                if len(ids) >= 1000:
                    break
        keys: list[str] = []
        if keys_raw:
            for part in keys_raw.split(","):
                key = runtime._social_entity_key_norm(entity_type, part)
                if not key:
                    continue
                keys.append(key)
                if len(keys) >= 1000:
                    break

        conn = runtime._files_pg_connect()
        if conn is None:
            return jsonify({"items": [], "error": "PostgreSQL unavailable"}), 503
        try:
            if entity_type == "track":
                try:
                    runtime._lastfm_sync_loved_tracks_to_pmda(uid, force=False)
                except Exception:
                    logging.debug("Last.fm loved-track sync during likes_get failed", exc_info=True)
            with conn.cursor() as cur:
                if ids:
                    cur.execute(
                        """
                        SELECT entity_id, entity_key, liked, EXTRACT(EPOCH FROM updated_at)::BIGINT
                        FROM files_user_entity_likes
                        WHERE user_id = %s
                          AND entity_type = %s
                          AND entity_id = ANY(%s)
                        """,
                        (int(uid), entity_type, ids),
                    )
                elif keys:
                    cur.execute(
                        """
                        SELECT entity_id, entity_key, liked, EXTRACT(EPOCH FROM updated_at)::BIGINT
                        FROM files_user_entity_likes
                        WHERE user_id = %s
                          AND entity_type = %s
                          AND entity_key = ANY(%s)
                        """,
                        (int(uid), entity_type, keys),
                    )
                else:
                    cur.execute(
                        """
                        SELECT entity_id, entity_key, liked, EXTRACT(EPOCH FROM updated_at)::BIGINT
                        FROM files_user_entity_likes
                        WHERE user_id = %s
                          AND entity_type = %s
                          AND liked = TRUE
                        ORDER BY updated_at DESC, entity_id DESC
                        LIMIT %s
                        """,
                        (int(uid), entity_type, int(limit)),
                    )
                rows = cur.fetchall()
            items = [
                {
                    "entity_id": int(r[0] or 0),
                    "entity_key": str(r[1] or "").strip() or None,
                    "liked": bool(r[2]),
                    "updated_at": int(r[3] or 0),
                }
                for r in rows
                if int(r[0] or 0) > 0 or str(r[1] or "").strip()
            ]
            return jsonify({"entity_type": entity_type, "items": items})
        finally:
            conn.close()

    @blueprint.put("/api/library/likes", endpoint="api_library_likes_put")
    def api_library_likes_put():
        """Set like state for a single entity."""

        if runtime._get_library_mode() != "files":
            return jsonify({"error": "Files mode required"}), 400
        ok, err = runtime._ensure_files_index_ready()
        if not ok:
            return jsonify({"error": err or "Files index unavailable"}), 503
        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            data = {}
        uid = runtime._current_user_id_or_zero()
        if uid <= 0:
            return jsonify({"error": "Authentication required"}), 401
        entity_type = str(data.get("entity_type") or "").strip().lower()
        if not runtime._social_entity_type_allowed(entity_type):
            return jsonify({"error": "unsupported entity_type"}), 400
        entity_id = runtime._parse_int_loose(data.get("entity_id"), 0)
        entity_key = runtime._social_entity_key_norm(entity_type, data.get("entity_key"))
        if entity_id <= 0 and not entity_key:
            return jsonify({"error": "entity_id or entity_key is required"}), 400
        liked = bool(runtime._parse_bool(data.get("liked") if data.get("liked") is not None else True))
        source = str(data.get("source") or "ui").strip()[:64] or "ui"

        conn = runtime._files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO files_user_entity_likes(user_id, entity_type, entity_id, entity_key, liked, source, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (user_id, entity_type, entity_id, entity_key) DO UPDATE SET
                            liked = EXCLUDED.liked,
                            source = EXCLUDED.source,
                            updated_at = NOW()
                        """,
                        (int(uid), entity_type, int(entity_id), entity_key, bool(liked), source),
                    )
            if entity_type == "track" and int(entity_id or 0) > 0:
                try:
                    threading.Thread(
                        target=runtime._lastfm_set_track_love,
                        args=(int(entity_id), bool(liked)),
                        name=f"lastfm-track-love-{entity_id}",
                        daemon=True,
                    ).start()
                except Exception:
                    logging.debug("Failed to enqueue Last.fm track love sync", exc_info=True)
            return jsonify(
                {
                    "entity_type": entity_type,
                    "entity_id": int(entity_id),
                    "entity_key": entity_key or None,
                    "liked": bool(liked),
                    "updated_at": int(time.time()),
                }
            )
        finally:
            conn.close()

    @blueprint.get("/api/library/album/<int:album_id>/rating", endpoint="api_library_album_rating_get")
    def api_library_album_rating_get(album_id: int):
        return _album_feedback_get(album_id)

    @blueprint.put("/api/library/album/<int:album_id>/rating", endpoint="api_library_album_rating_put")
    def api_library_album_rating_put(album_id: int):
        return _album_feedback_put(album_id, rating_only=True)

    @blueprint.get("/api/library/album/<int:album_id>/review", endpoint="api_library_album_review_get")
    def api_library_album_review_get(album_id: int):
        return _album_feedback_get(album_id)

    @blueprint.put("/api/library/album/<int:album_id>/review", endpoint="api_library_album_review_put")
    def api_library_album_review_put(album_id: int):
        return _album_feedback_put(album_id, rating_only=False)

    def _album_feedback_get(album_id: int):
        if runtime._get_library_mode() != "files":
            return jsonify({"error": "Files mode required"}), 400
        ok, err = runtime._ensure_files_index_ready()
        if not ok:
            return jsonify({"error": err or "Files index unavailable"}), 503
        uid = runtime._current_user_id_or_zero()
        if uid <= 0:
            return jsonify({"error": "Authentication required"}), 401
        album_id = int(album_id or 0)
        if album_id <= 0:
            return jsonify({"error": "Invalid album id"}), 400
        conn = runtime._files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                rating, review_text, updated_at = runtime._files_user_album_feedback_row(cur, int(uid), int(album_id))
            return jsonify(
                {
                    "album_id": int(album_id),
                    "user_id": int(uid),
                    "rating": max(1, min(5, rating)) if rating > 0 else None,
                    "review_text": review_text or None,
                    "updated_at": updated_at or None,
                }
            )
        finally:
            conn.close()

    def _album_feedback_put(album_id: int, *, rating_only: bool):
        if runtime._get_library_mode() != "files":
            return jsonify({"error": "Files mode required"}), 400
        ok, err = runtime._ensure_files_index_ready()
        if not ok:
            return jsonify({"error": err or "Files index unavailable"}), 503
        uid = runtime._current_user_id_or_zero()
        if uid <= 0:
            return jsonify({"error": "Authentication required"}), 401
        album_id = int(album_id or 0)
        if album_id <= 0:
            return jsonify({"error": "Invalid album id"}), 400
        data = request.get_json(silent=True) or {}
        source = str(data.get("source") or "ui").strip()[:64] or "ui"
        if rating_only:
            raw_rating = data.get("rating")
            try:
                rating = int(raw_rating) if raw_rating is not None and str(raw_rating).strip() != "" else 0
            except Exception:
                return jsonify({"error": "rating must be an integer between 1 and 5, or 0 to clear"}), 400
            if rating < 0 or rating > 5:
                return jsonify({"error": "rating must be between 1 and 5, or 0 to clear"}), 400
            review_text = None
        else:
            rating = None
            review_text = runtime._normalize_user_album_review_text(data.get("review_text"))

        conn = runtime._files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM files_albums WHERE id = %s LIMIT 1", (int(album_id),))
                    if not cur.fetchone():
                        return jsonify({"error": "Album not found"}), 404
                    current_rating, current_review_text, _current_updated_at = runtime._files_user_album_feedback_row(
                        cur,
                        int(uid),
                        int(album_id),
                    )
                    if rating_only:
                        next_rating, next_review_text, delete_row = runtime._merge_user_album_feedback(
                            current_rating,
                            current_review_text,
                            rating=int(rating or 0),
                        )
                    else:
                        next_rating, next_review_text, delete_row = runtime._merge_user_album_feedback(
                            current_rating,
                            current_review_text,
                            review_text=review_text,
                        )
                    if delete_row:
                        cur.execute(
                            "DELETE FROM files_user_album_ratings WHERE user_id = %s AND album_id = %s",
                            (int(uid), int(album_id)),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO files_user_album_ratings(user_id, album_id, rating, review_text, source, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                            ON CONFLICT (user_id, album_id) DO UPDATE SET
                                rating = EXCLUDED.rating,
                                review_text = EXCLUDED.review_text,
                                source = EXCLUDED.source,
                                updated_at = NOW()
                            """,
                            (int(uid), int(album_id), int(next_rating), next_review_text, source),
                        )
            runtime._files_cache_invalidate_all()
            return jsonify(
                {
                    "album_id": int(album_id),
                    "user_id": int(uid),
                    "rating": int(next_rating) if next_rating > 0 else None,
                    "review_text": next_review_text or None,
                    "updated_at": int(time.time()),
                }
            )
        finally:
            conn.close()

    return blueprint
