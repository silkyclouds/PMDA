"""Runtime-owned personal library, recommendations, playlist, and playback handlers."""
from __future__ import annotations

import json
import re
import time
from typing import Any

from flask import jsonify, request

_EXTRACTED_NAMES = {
    '_social_entity_type_allowed',
    '_social_entity_key_norm',
    '_social_notification_insert',
    '_social_recommendation_payload',
    'api_library_social_users',
    'api_library_social_context',
    'api_library_recommendations',
    'api_library_recommendation_like',
    'api_library_notifications',
    'api_library_notifications_mark_read',
    'api_library_playlists',
    'api_library_playlists_create',
    'api_library_playlist_detail',
    'api_library_playlist_delete',
    'api_library_playlist_items_add',
    'api_library_playlist_item_delete',
    'api_library_playlist_reorder',
    'api_library_reco_event',
    'api_library_playback_event',
    'api_library_reco_for_you',
    '_social_build_entity_snapshot',
    'api_library_share',
}

_SOCIAL_ENTITY_TYPES = {"artist", "album", "track", "label", "genre", "playlist"}


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in _EXTRACTED_NAMES:
            if getattr(value, "__module__", "") != getattr(runtime, "__name__", ""):
                globals()[name] = value
            else:
                original = _ORIGINAL_EXTRACTED_FUNCTIONS.get(name)
                if original is not None:
                    globals()[name] = original
            continue
        if name == "_bind_runtime" or name.endswith("_for_runtime"):
            continue
        globals()[name] = value


def _social_entity_type_allowed(entity_type: str) -> bool:
    return str(entity_type or "").strip().lower() in _SOCIAL_ENTITY_TYPES


def _social_entity_key_norm(entity_type: str, entity_key: str) -> str:
    et = str(entity_type or "").strip().lower()
    raw = re.sub(r"\s+", " ", str(entity_key or "").strip())
    if et in {"label", "genre"}:
        return raw
    if et == "playlist":
        return raw[:160]
    return raw


def _social_notification_insert(
    cur,
    *,
    user_id: int,
    actor_user_id: int | None,
    actor_username: str,
    kind: str,
    title: str,
    body: str,
    entity_type: str = "",
    entity_id: int = 0,
    entity_key: str = "",
    recommendation_id: int | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    cur.execute(
        """
        INSERT INTO files_user_notifications(
            user_id, actor_user_id, actor_username, kind, title, body,
            entity_type, entity_id, entity_key, recommendation_id, payload_json,
            is_read, created_at, read_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, NOW(), NULL)
        """,
        (
            int(user_id),
            int(actor_user_id or 0) if actor_user_id else None,
            str(actor_username or "").strip(),
            str(kind or "").strip()[:64],
            str(title or "").strip()[:200],
            str(body or "").strip()[:1200],
            str(entity_type or "").strip()[:32],
            int(entity_id or 0),
            str(entity_key or "").strip()[:240],
            int(recommendation_id or 0) if recommendation_id else None,
            json.dumps(payload or {}, ensure_ascii=False),
        ),
    )


def _social_recommendation_payload(row) -> dict[str, Any]:
    return {
        "recommendation_id": int(row[0] or 0),
        "sender_user_id": int(row[1] or 0),
        "sender_username": str(row[2] or "").strip(),
        "recipient_user_id": int(row[3] or 0),
        "recipient_username": str(row[4] or "").strip(),
        "entity_type": str(row[5] or "").strip(),
        "entity_id": int(row[6] or 0),
        "entity_key": str(row[7] or "").strip(),
        "entity_label": str(row[8] or "").strip(),
        "entity_subtitle": str(row[9] or "").strip(),
        "entity_href": str(row[10] or "").strip(),
        "entity_thumb": str(row[11] or "").strip() or None,
        "entity_meta": json.loads(str(row[12] or "{}") or "{}") if str(row[12] or "").strip() else {},
        "message": str(row[13] or "").strip() or None,
        "parent_recommendation_id": int(row[14] or 0) or None,
        "liked_by_recipient": bool(row[15]),
        "created_at": int(_dt_to_epoch(row[16])) if row[16] else 0,
        "read_at": int(_dt_to_epoch(row[17])) if row[17] else None,
        "status": str(row[18] or "sent").strip() or "sent",
    }


def api_library_social_users():
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required", "users": []}), 401
    mode = str(request.args.get("mode") or "shares").strip().lower()
    require_accept_shares = mode == "shares"
    require_public_likes = mode == "liked"
    require_public_recommendations = mode == "recommendations"
    return jsonify(
        {
            "users": _auth_active_users_list(
                exclude_user_id=uid,
                require_accept_shares=require_accept_shares,
                require_public_likes=require_public_likes,
                require_public_recommendations=require_public_recommendations,
            )
        }
    )


def api_library_social_context():
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required"}), 401
    entity_type = str(request.args.get("entity_type") or "").strip().lower()
    if not _social_entity_type_allowed(entity_type):
        return jsonify({"error": "unsupported entity_type"}), 400
    entity_id = _parse_int_loose(request.args.get("entity_id"), 0)
    entity_key = _social_entity_key_norm(entity_type, request.args.get("entity_key"))
    if entity_id <= 0 and not entity_key:
        return jsonify({"error": "entity_id or entity_key is required"}), 400
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        liked_public_users = {
            int(item.get("id") or 0): item
            for item in _auth_active_users_list(exclude_user_id=0, require_public_likes=True)
        }
        reco_public_users = {
            int(item.get("id") or 0): item
            for item in _auth_active_users_list(exclude_user_id=0, require_public_recommendations=True)
        }
        payload: dict[str, Any] = {"liked_by": [], "recommended_by": []}
        with conn.cursor() as cur:
            if entity_id > 0:
                cur.execute(
                    """
                    SELECT DISTINCT user_id
                    FROM files_user_entity_likes
                    WHERE entity_type = %s AND entity_id = %s AND liked = TRUE
                    ORDER BY user_id ASC
                    """,
                    (entity_type, int(entity_id)),
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT user_id
                    FROM files_user_entity_likes
                    WHERE entity_type = %s AND entity_key = %s AND liked = TRUE
                    ORDER BY user_id ASC
                    """,
                    (entity_type, entity_key),
                )
            liked_ids = [int(row[0] or 0) for row in cur.fetchall() if int(row[0] or 0) > 0]
            payload["liked_by"] = [liked_public_users[user_id] for user_id in liked_ids if user_id in liked_public_users]

            if entity_id > 0:
                cur.execute(
                    """
                    SELECT DISTINCT sender_user_id
                    FROM files_social_recommendations
                    WHERE entity_type = %s AND entity_id = %s
                    ORDER BY sender_user_id ASC
                    """,
                    (entity_type, int(entity_id)),
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT sender_user_id
                    FROM files_social_recommendations
                    WHERE entity_type = %s AND entity_key = %s
                    ORDER BY sender_user_id ASC
                    """,
                    (entity_type, entity_key),
                )
            reco_ids = [int(row[0] or 0) for row in cur.fetchall() if int(row[0] or 0) > 0]
            payload["recommended_by"] = [reco_public_users[user_id] for user_id in reco_ids if user_id in reco_public_users]
        return jsonify(payload)
    finally:
        conn.close()


def api_library_recommendations():
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required"}), 401
    target_uid, target_user, scope_err = _auth_resolve_public_user_scope(
        request.args.get("user_id"),
        current_user_id=uid,
        visibility_key="share_recommendations_public",
    )
    if scope_err:
        return jsonify({"error": scope_err[0]}), int(scope_err[1])
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        payload = {"owner": target_user or _auth_user_snapshot(target_uid), "received": [], "sent": [], "unread_count": 0}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, sender_user_id, sender_username, recipient_user_id, recipient_username,
                       entity_type, entity_id, entity_key, entity_label, entity_subtitle, entity_href, entity_thumb,
                       COALESCE(entity_meta_json, '{}'), COALESCE(message, ''), parent_recommendation_id,
                       liked_by_recipient, created_at, read_at, COALESCE(status, 'sent')
                FROM files_social_recommendations
                WHERE recipient_user_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 200
                """,
                (int(target_uid),),
            )
            payload["received"] = [_social_recommendation_payload(row) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT id, sender_user_id, sender_username, recipient_user_id, recipient_username,
                       entity_type, entity_id, entity_key, entity_label, entity_subtitle, entity_href, entity_thumb,
                       COALESCE(entity_meta_json, '{}'), COALESCE(message, ''), parent_recommendation_id,
                       liked_by_recipient, created_at, read_at, COALESCE(status, 'sent')
                FROM files_social_recommendations
                WHERE sender_user_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 200
                """,
                (int(target_uid),),
            )
            payload["sent"] = [_social_recommendation_payload(row) for row in cur.fetchall()]
            if int(target_uid) == int(uid):
                cur.execute(
                    "SELECT COUNT(*) FROM files_user_notifications WHERE user_id = %s AND is_read = FALSE",
                    (int(uid),),
                )
                payload["unread_count"] = int((cur.fetchone() or [0])[0] or 0)
        return jsonify(payload)
    finally:
        conn.close()

def api_library_recommendation_like(recommendation_id: int):
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required"}), 401
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, sender_user_id, sender_username, recipient_user_id, entity_type, entity_id, entity_key, entity_label, liked_by_recipient
                    FROM files_social_recommendations
                    WHERE id = %s AND recipient_user_id = %s
                    LIMIT 1
                    """,
                    (int(recommendation_id), int(uid)),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Recommendation not found"}), 404
                if bool(row[8]):
                    return jsonify({"ok": True, "recommendation_id": int(recommendation_id), "liked_by_recipient": True})
                cur.execute(
                    """
                    UPDATE files_social_recommendations
                    SET liked_by_recipient = TRUE, liked_at = NOW(), read_at = COALESCE(read_at, NOW()), status = 'liked'
                    WHERE id = %s
                    """,
                    (int(recommendation_id),),
                )
                actor = _auth_user_snapshot(uid)
                _social_notification_insert(
                    cur,
                    user_id=int(row[1] or 0),
                    actor_user_id=uid,
                    actor_username=str(actor.get("username") or "").strip(),
                    kind="recommendation_liked",
                    title="Recommendation liked",
                    body=f"{actor.get('username') or 'Someone'} liked your recommendation for {str(row[7] or '').strip() or 'an item'}",
                    entity_type=str(row[4] or "").strip(),
                    entity_id=int(row[5] or 0),
                    entity_key=str(row[6] or "").strip(),
                    recommendation_id=int(recommendation_id),
                    payload={"label": str(row[7] or "").strip()},
                )
        return jsonify({"ok": True, "recommendation_id": int(recommendation_id), "liked_by_recipient": True})
    finally:
        conn.close()

def api_library_notifications():
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required", "notifications": []}), 401
    limit = max(1, min(200, _parse_int_loose(request.args.get("limit"), 50)))
    unread_only = bool(_parse_bool(request.args.get("unread_only")))
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable", "notifications": []}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, actor_user_id, COALESCE(actor_username, ''), kind, title, body,
                       entity_type, entity_id, entity_key, recommendation_id, COALESCE(payload_json, '{{}}'),
                       is_read, created_at, read_at
                FROM files_user_notifications
                WHERE user_id = %s
                  {'AND is_read = FALSE' if unread_only else ''}
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (int(uid), int(limit)),
            )
            notifications = [
                {
                    "notification_id": int(row[0] or 0),
                    "actor_user_id": int(row[1] or 0) or None,
                    "actor_username": str(row[2] or "").strip() or None,
                    "kind": str(row[3] or "").strip(),
                    "title": str(row[4] or "").strip(),
                    "body": str(row[5] or "").strip(),
                    "entity_type": str(row[6] or "").strip() or None,
                    "entity_id": int(row[7] or 0) or None,
                    "entity_key": str(row[8] or "").strip() or None,
                    "recommendation_id": int(row[9] or 0) or None,
                    "payload": json.loads(str(row[10] or "{}") or "{}") if str(row[10] or "").strip() else {},
                    "is_read": bool(row[11]),
                    "created_at": int(_dt_to_epoch(row[12])) if row[12] else 0,
                    "read_at": int(_dt_to_epoch(row[13])) if row[13] else None,
                }
                for row in cur.fetchall()
            ]
        return jsonify({"notifications": notifications})
    finally:
        conn.close()

def api_library_notifications_mark_read(notification_id: int):
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required"}), 401
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE files_user_notifications
                    SET is_read = TRUE, read_at = COALESCE(read_at, NOW())
                    WHERE id = %s AND user_id = %s
                    """,
                    (int(notification_id), int(uid)),
                )
                if cur.rowcount <= 0:
                    return jsonify({"error": "Notification not found"}), 404
        return jsonify({"ok": True, "notification_id": int(notification_id)})
    finally:
        conn.close()

def api_library_playlists():
    """List local playlists (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"playlists": [], "error": "Files mode required"}), 400
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"playlists": [], "error": "Authentication required"}), 401
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"playlists": [], "error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    pl.id,
                    pl.name,
                    COALESCE(pl.description, '') AS description,
                    pl.updated_at,
                    COUNT(it.id) AS item_count
                FROM files_playlists pl
                LEFT JOIN files_playlist_items it ON it.playlist_id = pl.id
                WHERE pl.user_id = %s
                GROUP BY pl.id
                ORDER BY pl.updated_at DESC, pl.id DESC
                """
                ,
                (int(uid),),
            )
            rows = cur.fetchall()
        playlists = [
            {
                "playlist_id": int(r[0]),
                "name": r[1] or "",
                "description": r[2] or "",
                "item_count": int(r[4] or 0),
                "updated_at": int(_dt_to_epoch(r[3])) if r[3] else 0,
            }
            for r in rows
        ]
        return jsonify({"playlists": playlists})
    finally:
        conn.close()

def api_library_playlists_create():
    """Create a local playlist (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required"}), 401
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        data = {}
    name = str(data.get("name") or "").strip()
    description = str(data.get("description") or "").strip() or None
    if not name:
        return jsonify({"error": "name is required"}), 400
    if len(name) > 160:
        return jsonify({"error": "name too long"}), 400
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO files_playlists(user_id, name, description, created_at, updated_at) VALUES (%s, %s, %s, NOW(), NOW()) RETURNING id",
                    (int(uid), name, description),
                )
                pid = int((cur.fetchone() or [0])[0] or 0)
        return jsonify({"playlist_id": pid, "name": name, "description": description or "", "item_count": 0, "updated_at": int(time.time())})
    finally:
        conn.close()

def api_library_playlist_detail(playlist_id: int):
    """Return playlist details and items (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required"}), 401
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, COALESCE(description, ''), updated_at FROM files_playlists WHERE id = %s AND user_id = %s",
                (int(playlist_id), int(uid)),
            )
            pl = cur.fetchone()
            if not pl:
                return jsonify({"error": "Playlist not found"}), 404
            cur.execute(
                """
                SELECT
                    it.id AS item_id,
                    it.position,
                    it.added_at,
                    tr.id AS track_id,
                    tr.title AS track_title,
                    tr.duration_sec,
                    tr.track_num,
                    tr.disc_num,
                    alb.id AS album_id,
                    alb.title AS album_title,
                    alb.has_cover,
                    art.id AS artist_id,
                    art.name AS artist_name
                FROM files_playlist_items it
                JOIN files_tracks tr ON tr.id = it.track_id
                JOIN files_albums alb ON alb.id = tr.album_id
                JOIN files_artists art ON art.id = alb.artist_id
                WHERE it.playlist_id = %s
                ORDER BY it.position ASC, it.id ASC
                """,
                (int(playlist_id),),
            )
            rows = cur.fetchall()
        items: list[dict] = []
        for r in rows:
            item_id = int(r[0] or 0)
            track_id = int(r[3] or 0)
            album_id = int(r[8] or 0)
            has_cover = bool(r[10])
            items.append(
                {
                    "item_id": item_id,
                    "position": int(r[1] or 0),
                    "added_at": int(_dt_to_epoch(r[2])) if r[2] else 0,
                    "track": {
                        "track_id": track_id,
                        "title": r[4] or "",
                        "artist_id": int(r[11] or 0),
                        "artist_name": r[12] or "",
                        "album_id": album_id,
                        "album_title": r[9] or "",
                        "duration_sec": int(r[5] or 0),
                        "track_num": int(r[6] or 0),
                        "disc_num": int(r[7] or 0),
                        "thumb": _browser_api_url(f"/api/library/files/album/{album_id}/cover?size=96") if has_cover else None,
                        "file_url": _browser_api_url(f"/api/library/track/{track_id}/stream"),
                    },
                }
            )
        return jsonify(
            {
                "playlist_id": int(pl[0]),
                "name": pl[1] or "",
                "description": pl[2] or "",
                "updated_at": int(_dt_to_epoch(pl[3])) if pl[3] else 0,
                "items": items,
            }
        )
    finally:
        conn.close()

def api_library_playlist_delete(playlist_id: int):
    """Delete a playlist (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required"}), 401
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("DELETE FROM files_playlists WHERE id = %s AND user_id = %s", (int(playlist_id), int(uid)))
                if cur.rowcount <= 0:
                    return jsonify({"error": "Playlist not found"}), 404
        return jsonify({"ok": True, "playlist_id": int(playlist_id)})
    finally:
        conn.close()

def api_library_playlist_items_add(playlist_id: int):
    """Append tracks (or an album) to a playlist (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required"}), 401
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        data = {}
    track_ids_raw = data.get("track_ids")
    track_id_single = _parse_int_loose(data.get("track_id"), 0)
    album_id = _parse_int_loose(data.get("album_id"), 0)

    track_ids: list[int] = []
    if isinstance(track_ids_raw, list):
        for x in track_ids_raw:
            tid = _parse_int_loose(x, 0)
            if tid > 0:
                track_ids.append(int(tid))
    if track_id_single > 0:
        track_ids.append(int(track_id_single))

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM files_playlists WHERE id = %s AND user_id = %s", (int(playlist_id), int(uid)))
                if not cur.fetchone():
                    return jsonify({"error": "Playlist not found"}), 404

                # Expand album -> tracks (ordered).
                if album_id > 0:
                    cur.execute(
                        """
                        SELECT id
                        FROM files_tracks
                        WHERE album_id = %s
                        ORDER BY disc_num ASC, track_num ASC, id ASC
                        """,
                        (int(album_id),),
                    )
                    for (tid,) in cur.fetchall():
                        tid_int = int(tid or 0)
                        if tid_int > 0:
                            track_ids.append(tid_int)

                # Deduplicate while preserving order.
                seen = set()
                track_ids = [t for t in track_ids if not (t in seen or seen.add(t))]
                if not track_ids:
                    return jsonify({"error": "track_ids (or album_id) required"}), 400

                cur.execute("SELECT COALESCE(MAX(position), -1) FROM files_playlist_items WHERE playlist_id = %s", (int(playlist_id),))
                max_pos = int((cur.fetchone() or [-1])[0] or -1)
                pos = max_pos + 1
                inserted = 0
                for tid in track_ids:
                    cur.execute(
                        """
                        INSERT INTO files_playlist_items(playlist_id, track_id, position, added_at)
                        VALUES (%s, %s, %s, NOW())
                        """,
                        (int(playlist_id), int(tid), int(pos)),
                    )
                    inserted += 1
                    pos += 1
                cur.execute("UPDATE files_playlists SET updated_at = NOW() WHERE id = %s", (int(playlist_id),))
        return jsonify({"ok": True, "playlist_id": int(playlist_id), "inserted": int(inserted)})
    finally:
        conn.close()

def api_library_playlist_item_delete(playlist_id: int, item_id: int):
    """Remove a single playlist item (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required"}), 401
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM files_playlists WHERE id = %s AND user_id = %s", (int(playlist_id), int(uid)))
                if not cur.fetchone():
                    return jsonify({"error": "Playlist not found"}), 404
                cur.execute(
                    "DELETE FROM files_playlist_items WHERE id = %s AND playlist_id = %s",
                    (int(item_id), int(playlist_id)),
                )
                if cur.rowcount <= 0:
                    return jsonify({"error": "Item not found"}), 404
                cur.execute("UPDATE files_playlists SET updated_at = NOW() WHERE id = %s", (int(playlist_id),))
        return jsonify({"ok": True, "playlist_id": int(playlist_id), "item_id": int(item_id)})
    finally:
        conn.close()

def api_library_playlist_reorder(playlist_id: int):
    """Reorder playlist items by item_id list (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required"}), 401
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        data = {}
    item_ids_raw = data.get("item_ids")
    if not isinstance(item_ids_raw, list) or not item_ids_raw:
        return jsonify({"error": "item_ids is required"}), 400
    item_ids: list[int] = []
    for x in item_ids_raw:
        iid = _parse_int_loose(x, 0)
        if iid > 0:
            item_ids.append(int(iid))
    if not item_ids:
        return jsonify({"error": "item_ids is required"}), 400

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM files_playlists WHERE id = %s AND user_id = %s", (int(playlist_id), int(uid)))
                if not cur.fetchone():
                    return jsonify({"error": "Playlist not found"}), 404
                for idx, iid in enumerate(item_ids):
                    cur.execute(
                        "UPDATE files_playlist_items SET position = %s WHERE id = %s AND playlist_id = %s",
                        (int(idx), int(iid), int(playlist_id)),
                    )
                cur.execute("UPDATE files_playlists SET updated_at = NOW() WHERE id = %s", (int(playlist_id),))
        return jsonify({"ok": True, "playlist_id": int(playlist_id), "count": len(item_ids)})
    finally:
        conn.close()

def api_library_reco_event():
    """Record a playback/session event for recommendation ranking (Files mode)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    data = request.get_json() or {}
    session_id = str(data.get("session_id") or "").strip()
    track_id = _parse_int_loose(data.get("track_id"), 0)
    event_type = str(data.get("event_type") or "").strip().lower()
    played_seconds = _parse_int_loose(data.get("played_seconds"), 0)
    if not session_id:
        return jsonify({"error": "session_id is required"}), 400
    if not re.match(r"^[a-zA-Z0-9._:-]{6,128}$", session_id):
        return jsonify({"error": "Invalid session_id format"}), 400
    if track_id <= 0:
        return jsonify({"error": "track_id is required"}), 400
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.transaction():
            success, message = _reco_record_event(conn, session_id, track_id, event_type, played_seconds)
        if not success:
            status = 404 if "track not found" in message else 400
            return jsonify({"error": message}), status
        return jsonify({"ok": True, "session_id": session_id, "track_id": track_id, "event_type": event_type})
    finally:
        conn.close()

def api_library_playback_event():
    """Record a listening event (Files mode). Used for user listening statistics charts."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        data = {}
    track_id = _parse_int_loose(data.get("track_id"), 0)
    event_type = str(data.get("event_type") or "").strip().lower() or "play_partial"
    played_seconds = _parse_int_loose(data.get("played_seconds"), 0)
    user_id = _current_user_id_or_zero()
    if user_id <= 0:
        return jsonify({"error": "Authentication required"}), 401
    if track_id <= 0:
        return jsonify({"error": "track_id is required"}), 400
    if played_seconds < 0:
        played_seconds = 0
    if event_type not in {"play_complete", "play_partial", "skip", "stop", "play_start"}:
        event_type = "play_partial"

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT duration_sec FROM files_tracks WHERE id = %s", (int(track_id),))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "track not found"}), 404
            dur = int(row[0] or 0)
        if dur > 0:
            played_seconds = max(0, min(int(played_seconds), int(dur)))
        else:
            played_seconds = max(0, min(int(played_seconds), 60 * 60 * 8))

        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO files_playback_events(user_id, track_id, event_type, played_seconds, created_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    """,
                    (int(user_id), int(track_id), str(event_type), int(played_seconds)),
                )
        try:
            if event_type in {"play_start", "play_complete", "play_partial", "stop"}:
                threading.Thread(
                    target=_lastfm_handle_playback_event_async,
                    args=(int(track_id), str(event_type), int(played_seconds)),
                    name=f"lastfm-playback-{track_id}",
                    daemon=True,
                ).start()
        except Exception:
            logging.debug("Failed to enqueue Last.fm playback hook", exc_info=True)
        return jsonify({"ok": True, "track_id": int(track_id), "event_type": event_type, "played_seconds": int(played_seconds)})
    finally:
        conn.close()

def api_library_reco_for_you():
    """Return personalized track recommendations for the active session (Files mode)."""
    if _get_library_mode() != "files":
        return jsonify({"session_id": "", "tracks": []})
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"session_id": "", "tracks": [], "error": err or "Files index unavailable"}), 503

    session_id = str(request.args.get("session_id") or "").strip()
    limit = max(1, min(120, _parse_int_loose(request.args.get("limit"), 12)))
    offset = max(0, _parse_int_loose(request.args.get("offset"), 0))
    exclude_track_id = _parse_int_loose(request.args.get("exclude_track_id"), 0)
    if session_id and not re.match(r"^[a-zA-Z0-9._:-]{6,128}$", session_id):
        return jsonify({"error": "Invalid session_id format"}), 400

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"session_id": session_id, "tracks": [], "error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            if session_id:
                cur.execute("SELECT COALESCE(MAX(id), 0) FROM files_reco_events WHERE session_id = %s", (session_id,))
            else:
                cur.execute("SELECT COALESCE(MAX(id), 0) FROM files_reco_events")
            token = int((cur.fetchone() or [0])[0] or 0)
        cache_key = f"library:reco:for_you:{session_id or 'global'}:{limit}:{offset}:{exclude_track_id}:{token}"
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)

        profile = _reco_build_session_profile(conn, session_id) if session_id else {"has_data": False}
        candidate_limit = max(220, min(4000, (offset + limit) * 90))
        candidates = _reco_fetch_candidates(conn, profile, candidate_limit)
        if exclude_track_id > 0:
            candidates = [c for c in candidates if int(c.get("track_id") or 0) != int(exclude_track_id)]
        total = len(candidates)
        ranked_all = _reco_rank_candidates(profile, candidates, max(1, min(4000, offset + limit)))
        ranked = ranked_all[offset: offset + limit]
        tracks = []
        for c in ranked:
            track_id = int(c.get("track_id") or 0)
            album_id = int(c.get("album_id") or 0)
            artist_id = int(c.get("artist_id") or 0)
            if track_id <= 0:
                continue
            tracks.append(
                {
                    "track_id": track_id,
                    "title": c.get("title") or "",
                    "artist_id": artist_id,
                    "artist_name": c.get("artist_name") or "",
                    "album_id": album_id,
                    "album_title": c.get("album_title") or "",
                    "duration_sec": int(c.get("duration_sec") or 0),
                    "track_num": int(c.get("track_num") or 0),
                    "score": round(float(c.get("score") or 0.0), 4),
                    "reasons": c.get("reasons") or [],
                    "thumb": _browser_api_url(f"/api/library/files/album/{album_id}/cover?size=96") if bool(c.get("has_cover")) else None,
                    "file_url": _browser_api_url(f"/api/library/track/{track_id}/stream"),
                }
            )
        payload = {
            "session_id": session_id,
            "total": int(total),
            "limit": int(limit),
            "offset": int(offset),
            "tracks": tracks,
            "session_event_count": int(profile.get("session_event_count") or 0),
            "algorithm": RECO_EMBED_SOURCE,
        }
        _files_cache_set_json(cache_key, payload, ttl=20)
        return jsonify(payload)
    finally:
        conn.close()

def _social_build_entity_snapshot(
    conn,
    *,
    entity_type: str,
    entity_id: int = 0,
    entity_key: str = "",
    owner_user_id: int = 0,
) -> dict[str, Any] | None:
    base_url = request.url_root.rstrip("/")
    et = str(entity_type or "").strip().lower()
    ek = _social_entity_key_norm(et, entity_key)
    with conn.cursor() as cur:
        if et == "album" and int(entity_id or 0) > 0:
            cur.execute(
                """
                SELECT alb.id, alb.title, ar.name, alb.year, alb.has_cover,
                       COALESCE(alb.label, ''), COALESCE(alb.genre, ''), COALESCE(alb.tags_json, '[]')
                FROM files_albums alb
                JOIN files_artists ar ON ar.id = alb.artist_id
                WHERE alb.id = %s
                LIMIT 1
                """,
                (int(entity_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            aid = int(row[0] or 0)
            return {
                "entity_type": et,
                "entity_id": aid,
                "entity_key": "",
                "label": str(row[1] or "").strip(),
                "subtitle": str(row[2] or "").strip(),
                "href": f"/library/album/{aid}",
                "thumb": f"{base_url}/api/library/files/album/{aid}/cover?size=320" if bool(row[4]) else None,
                "meta": {
                    "year": int(row[3] or 0) or None,
                    "label": str(row[5] or "").strip() or None,
                    "genre": str(row[6] or "").strip() or None,
                    "tags_json": str(row[7] or "[]"),
                },
            }
        if et == "artist" and int(entity_id or 0) > 0:
            cur.execute(
                """
                SELECT id, name, album_count, has_image
                FROM files_artists
                WHERE id = %s
                LIMIT 1
                """,
                (int(entity_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            aid = int(row[0] or 0)
            return {
                "entity_type": et,
                "entity_id": aid,
                "entity_key": "",
                "label": str(row[1] or "").strip(),
                "subtitle": f"{int(row[2] or 0)} album(s)",
                "href": f"/library/artist/{aid}",
                "thumb": _artist_image_asset_url(base_url, aid, size=320) or None,
                "meta": {"album_count": int(row[2] or 0)},
            }
        if et == "track" and int(entity_id or 0) > 0:
            cur.execute(
                """
                SELECT tr.id, tr.title, ar.name, alb.id, alb.title, alb.has_cover
                FROM files_tracks tr
                JOIN files_albums alb ON alb.id = tr.album_id
                JOIN files_artists ar ON ar.id = alb.artist_id
                WHERE tr.id = %s
                LIMIT 1
                """,
                (int(entity_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            tid = int(row[0] or 0)
            album_id = int(row[3] or 0)
            return {
                "entity_type": et,
                "entity_id": tid,
                "entity_key": "",
                "label": str(row[1] or "").strip(),
                "subtitle": f"{str(row[2] or '').strip()} · {str(row[4] or '').strip()}",
                "href": f"/library/album/{album_id}",
                "thumb": f"{base_url}/api/library/files/album/{album_id}/cover?size=320" if bool(row[5]) else None,
                "meta": {"album_id": album_id},
            }
        if et == "playlist" and int(entity_id or 0) > 0:
            cur.execute(
                """
                SELECT pl.id, pl.name, COALESCE(pl.description, ''), COUNT(it.id)
                FROM files_playlists pl
                LEFT JOIN files_playlist_items it ON it.playlist_id = pl.id
                WHERE pl.id = %s AND pl.user_id = %s
                GROUP BY pl.id
                LIMIT 1
                """,
                (int(entity_id), int(owner_user_id or 0)),
            )
            row = cur.fetchone()
            if not row:
                return None
            pid = int(row[0] or 0)
            return {
                "entity_type": et,
                "entity_id": pid,
                "entity_key": "",
                "label": str(row[1] or "").strip(),
                "subtitle": str(row[2] or "").strip() or f"{int(row[3] or 0)} track(s)",
                "href": f"/library/playlists/{pid}",
                "thumb": None,
                "meta": {"item_count": int(row[3] or 0)},
            }
        if et == "label" and ek:
            return {
                "entity_type": et,
                "entity_id": 0,
                "entity_key": ek,
                "label": ek,
                "subtitle": "Label",
                "href": f"/library/label/{quote(ek)}",
                "thumb": None,
                "meta": {},
            }
        if et == "genre" and ek:
            return {
                "entity_type": et,
                "entity_id": 0,
                "entity_key": ek,
                "label": ek,
                "subtitle": "Genre",
                "href": f"/library/genre/{quote(ek)}",
                "thumb": None,
                "meta": {},
            }
    return None

def api_library_share():
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required"}), 401
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        data = {}
    entity_type = str(data.get("entity_type") or "").strip().lower()
    if not _social_entity_type_allowed(entity_type):
        return jsonify({"error": "unsupported entity_type"}), 400
    entity_id = _parse_int_loose(data.get("entity_id"), 0)
    entity_key = _social_entity_key_norm(entity_type, data.get("entity_key"))
    recipient_ids_raw = data.get("recipient_user_ids")
    if not isinstance(recipient_ids_raw, list):
        recipient_ids_raw = []
    recipient_ids = [int(x) for x in recipient_ids_raw if _parse_int_loose(x, 0) > 0 and int(x) != int(uid)]
    if not recipient_ids:
        return jsonify({"error": "recipient_user_ids is required"}), 400
    message = str(data.get("message") or "").strip()[:1200]
    parent_recommendation_id = _parse_int_loose(data.get("parent_recommendation_id"), 0)
    sender = _auth_user_snapshot(uid)
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        snapshot = _social_build_entity_snapshot(
            conn,
            entity_type=entity_type,
            entity_id=entity_id,
            entity_key=entity_key,
            owner_user_id=uid,
        )
        if not snapshot:
            return jsonify({"error": "Entity not found"}), 404
        inserted_ids: list[int] = []
        with conn.transaction():
            with conn.cursor() as cur:
                valid_recipients = {
                    int(item.get("id") or 0): item
                    for item in _auth_active_users_list(exclude_user_id=uid, require_accept_shares=True)
                    if int(item.get("id") or 0) in recipient_ids
                }
                if not valid_recipients:
                    return jsonify({"error": "No valid recipients"}), 400
                for rid, recipient in valid_recipients.items():
                    cur.execute(
                        """
                        INSERT INTO files_social_recommendations(
                            sender_user_id, sender_username, recipient_user_id, recipient_username,
                            entity_type, entity_id, entity_key, entity_label, entity_subtitle, entity_href, entity_thumb,
                            entity_meta_json, message, parent_recommendation_id, liked_by_recipient,
                            created_at, read_at, status
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, NOW(), NULL, 'sent')
                        RETURNING id
                        """,
                        (
                            int(uid),
                            str(sender.get("username") or "").strip(),
                            int(rid),
                            str(recipient.get("username") or "").strip(),
                            snapshot["entity_type"],
                            int(snapshot.get("entity_id") or 0),
                            str(snapshot.get("entity_key") or ""),
                            str(snapshot.get("label") or "")[:240],
                            str(snapshot.get("subtitle") or "")[:240],
                            str(snapshot.get("href") or "")[:512],
                            str(snapshot.get("thumb") or "")[:1024],
                            json.dumps(snapshot.get("meta") or {}, ensure_ascii=False),
                            message or None,
                            int(parent_recommendation_id or 0) if parent_recommendation_id > 0 else None,
                        ),
                    )
                    rec_id = int((cur.fetchone() or [0])[0] or 0)
                    inserted_ids.append(rec_id)
                    body = f"{sender.get('username') or 'Someone'} recommended {snapshot.get('label') or 'something'}"
                    if message:
                        body = f"{body}: {message}"
                    _social_notification_insert(
                        cur,
                        user_id=rid,
                        actor_user_id=uid,
                        actor_username=str(sender.get("username") or "").strip(),
                        kind="recommendation_received",
                        title="New recommendation",
                        body=body,
                        entity_type=snapshot["entity_type"],
                        entity_id=int(snapshot.get("entity_id") or 0),
                        entity_key=str(snapshot.get("entity_key") or ""),
                        recommendation_id=rec_id,
                        payload={"href": snapshot.get("href")},
                    )
        return jsonify({"ok": True, "count": len(inserted_ids), "recommendation_ids": inserted_ids})
    except Exception as exc:
        logging.exception(
            "[Share] failed entity_type=%s entity_id=%s entity_key=%r uid=%s recipients=%s: %s",
            entity_type,
            int(entity_id or 0),
            entity_key,
            int(uid or 0),
            recipient_ids,
            exc,
        )
        return jsonify({"error": "Could not send recommendation"}), 500
    finally:
        conn.close()

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}

def _social_entity_type_allowed_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _social_entity_type_allowed(*args, **kwargs)

def _social_entity_key_norm_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _social_entity_key_norm(*args, **kwargs)

def _social_notification_insert_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _social_notification_insert(*args, **kwargs)

def _social_recommendation_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _social_recommendation_payload(*args, **kwargs)

def api_library_social_users_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_social_users(*args, **kwargs)

def api_library_social_context_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_social_context(*args, **kwargs)

def api_library_recommendations_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_recommendations(*args, **kwargs)

def api_library_recommendation_like_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_recommendation_like(*args, **kwargs)

def api_library_notifications_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_notifications(*args, **kwargs)

def api_library_notifications_mark_read_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_notifications_mark_read(*args, **kwargs)

def api_library_playlists_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_playlists(*args, **kwargs)

def api_library_playlists_create_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_playlists_create(*args, **kwargs)

def api_library_playlist_detail_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_playlist_detail(*args, **kwargs)

def api_library_playlist_delete_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_playlist_delete(*args, **kwargs)

def api_library_playlist_items_add_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_playlist_items_add(*args, **kwargs)

def api_library_playlist_item_delete_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_playlist_item_delete(*args, **kwargs)

def api_library_playlist_reorder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_playlist_reorder(*args, **kwargs)

def api_library_reco_event_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_reco_event(*args, **kwargs)

def api_library_playback_event_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_playback_event(*args, **kwargs)

def api_library_reco_for_you_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_reco_for_you(*args, **kwargs)

def api_library_share_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_share(*args, **kwargs)
