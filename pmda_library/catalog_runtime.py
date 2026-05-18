"""Library catalog/search runtime extracted from the PMDA bootstrap module."""

from __future__ import annotations

import json
import re
import time
from typing import Any
from urllib.parse import quote

from flask import jsonify, request


_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Expose PMDA runtime globals to the extracted catalog handlers."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "api_library_search_suggest",
        "api_library_search_suggest_for_runtime",
        "api_library_entity_discover",
        "api_library_entity_discover_for_runtime",
        "api_library_artists_suggest",
        "api_library_artists_suggest_for_runtime",
        "api_library_genre_labels",
        "api_library_genre_labels_for_runtime",
        "_bind_runtime",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def api_library_search_suggest_for_runtime(runtime: Any):
    _bind_runtime(runtime)
    return api_library_search_suggest()


def api_library_entity_discover_for_runtime(runtime: Any):
    _bind_runtime(runtime)
    return api_library_entity_discover()

def api_library_artists_suggest_for_runtime(runtime: Any):
    _bind_runtime(runtime)
    return api_library_artists_suggest()

def api_library_genre_labels_for_runtime(runtime: Any, genre: Any):
    _bind_runtime(runtime)
    return api_library_genre_labels(genre)

def api_library_artists_suggest():
    """Ultra-fast artist suggestions for typeahead search."""
    query = (request.args.get("q") or "").strip()
    limit = max(1, min(50, _parse_int_loose(request.args.get("limit"), 12)))
    if not query:
        return jsonify({"query": "", "artists": []})
    if _get_library_mode() != "files":
        return jsonify({"query": query, "artists": []})
    cache_key = f"library:artists:suggest:{query.lower()}:{limit}"
    cached = _files_cache_get_json(cache_key)
    if cached is not None:
        return jsonify(cached)
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"query": query, "artists": [], "error": err or "Files index unavailable"}), 503
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"query": query, "artists": [], "error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            like = f"%{query}%"
            query_norm = _norm_artist_key(query)
            query_signature = _classical_person_signature_key(query)
            try:
                cur.execute(
                    """
                    SELECT
                        a.id,
                        a.name,
                        COALESCE(a.entity_kind, 'artist') AS entity_kind,
                        COALESCE(a.roles_json, '[]') AS roles_json,
                        (
                            SELECT COUNT(DISTINCT link_cnt.album_id)
                            FROM files_artist_album_links link_cnt
                            JOIN files_albums alb_cnt ON alb_cnt.id = link_cnt.album_id
                            WHERE link_cnt.artist_id = a.id
                        ) AS album_count,
                        a.broken_albums_count,
                        (""" + _artist_has_true_image_sql("a", "ext") + """) AS has_image,
                        GREATEST(similarity(a.name, %s), similarity(COALESCE(a.canonical_name, ''), %s)) AS score,
                        CASE
                            WHEN lower(a.name) LIKE lower(%s) || '%%'
                              OR lower(COALESCE(a.canonical_name, '')) LIKE lower(%s) || '%%'
                            THEN 0 ELSE 1
                        END AS prefix_rank
                    FROM files_artists a
                    LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                    WHERE (
                        a.name ILIKE %s
                        OR COALESCE(a.canonical_name, '') ILIKE %s
                        OR (%s <> '' AND COALESCE(a.canonical_name_norm, '') = %s)
                        OR COALESCE(a.aliases_json, '[]') ILIKE %s
                        OR EXISTS (
                            SELECT 1
                            FROM files_artist_aliases alias
                            WHERE alias.artist_id = a.id
                              AND (
                                  alias.alias ILIKE %s
                                  OR (%s <> '' AND alias.alias_norm = %s)
                                  OR (%s <> '' AND alias.alias_signature = %s)
                              )
                        )
                    )
                      AND COALESCE(a.entity_kind, 'artist') IN ('artist', 'composer')
                    ORDER BY prefix_rank ASC, score DESC, album_count DESC, a.name ASC
                    LIMIT %s
                    """,
                    (query, query, query, query, like, like, query_norm, query_norm, like, query_norm, query_norm, query_signature, query_signature, limit),
                )
            except Exception:
                cur.execute(
                    """
                    SELECT
                        a.id,
                        a.name,
                        COALESCE(a.entity_kind, 'artist') AS entity_kind,
                        COALESCE(a.roles_json, '[]') AS roles_json,
                        (
                            SELECT COUNT(DISTINCT link_cnt.album_id)
                            FROM files_artist_album_links link_cnt
                            JOIN files_albums alb_cnt ON alb_cnt.id = link_cnt.album_id
                            WHERE link_cnt.artist_id = a.id
                        ) AS album_count,
                        a.broken_albums_count,
                        (""" + _artist_has_true_image_sql("a", "ext") + """) AS has_image
                    FROM files_artists a
                    LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                    WHERE (
                        a.name ILIKE %s
                        OR COALESCE(a.canonical_name, '') ILIKE %s
                        OR (%s <> '' AND COALESCE(a.canonical_name_norm, '') = %s)
                        OR COALESCE(a.aliases_json, '[]') ILIKE %s
                        OR EXISTS (
                            SELECT 1
                            FROM files_artist_aliases alias
                            WHERE alias.artist_id = a.id
                              AND (
                                  alias.alias ILIKE %s
                                  OR (%s <> '' AND alias.alias_norm = %s)
                                  OR (%s <> '' AND alias.alias_signature = %s)
                              )
                        )
                    )
                      AND COALESCE(a.entity_kind, 'artist') IN ('artist', 'composer')
                    ORDER BY album_count DESC, a.name ASC
                    LIMIT %s
                    """,
                    (like, like, query_norm, query_norm, like, like, query_norm, query_norm, query_signature, query_signature, limit),
                )
            rows = cur.fetchall()
        base_url = request.url_root.rstrip("/")
        payload = {
            "query": query,
            "artists": [
                {
                    "artist_id": int(r[0]),
                    "artist_name": r[1] or "",
                    "entity_kind": str(r[2] or "artist"),
                    "roles": _safe_json_load(r[3] or "[]", fallback=[]),
                    "album_count": int(r[4] or 0),
                    "broken_albums_count": int(r[5] or 0),
                    "artist_thumb": _artist_image_asset_url(base_url, int(r[0]), size=96) or None,
                }
                for r in rows
            ],
        }
        _files_cache_set_json(cache_key, payload, ttl=20)
        return jsonify(payload)
    finally:
        conn.close()

def api_library_genre_labels(genre: str):
    """Return labels publishing a given genre (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"genre": genre or "", "album_count": 0, "labels": [], "error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"genre": genre or "", "album_count": 0, "labels": [], "error": err or "Files index unavailable"}), 503
    g = (genre or "").strip()
    if not g:
        return jsonify({"genre": "", "album_count": 0, "labels": [], "error": "Invalid genre"}), 400
    limit = max(1, min(200, _parse_int_loose(request.args.get("limit"), 80)))
    refresh = bool(_parse_bool(request.args.get("refresh")))
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    album_match_sql = (
        f"({_library_albums_match_where(include_unmatched, 'alb')})"
        f" AND ({_library_album_scope_where(scope, 'alb')})"
    )
    cache_key = (
        f"library:genre:labels:{g.lower()}:{limit}:"
        f"{_library_cache_scope_suffix(scope)}:{_library_cache_unmatched_suffix(include_unmatched)}"
    )
    if not refresh:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"genre": g, "album_count": 0, "labels": [], "error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH matched_albums AS (
                    SELECT DISTINCT alb.id AS album_id
                    FROM files_albums alb
                    WHERE EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS gg(value)
                        WHERE lower(trim(gg.value)) = lower(%s)
                    )
                      AND """ + album_match_sql + """
                    UNION
                    SELECT DISTINCT alb.id AS album_id
                    FROM files_albums alb
                    WHERE COALESCE(alb.tags_json, '[]') = '[]'
                      AND lower(trim(COALESCE(alb.genre, ''))) = lower(%s)
                      AND COALESCE(trim(alb.genre), '') <> ''
                      AND """ + album_match_sql + """
                )
                SELECT COUNT(*) FROM matched_albums
                """,
                (g, g),
            )
            album_count = int((cur.fetchone() or [0])[0] or 0)

            cur.execute(
                """
                WITH matched_albums AS (
                    SELECT DISTINCT alb.id AS album_id
                    FROM files_albums alb
                    WHERE EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS gg(value)
                        WHERE lower(trim(gg.value)) = lower(%s)
                    )
                      AND """ + album_match_sql + """
                    UNION
                    SELECT DISTINCT alb.id AS album_id
                    FROM files_albums alb
                    WHERE COALESCE(alb.tags_json, '[]') = '[]'
                      AND lower(trim(COALESCE(alb.genre, ''))) = lower(%s)
                      AND COALESCE(trim(alb.genre), '') <> ''
                      AND """ + album_match_sql + """
                ),
                label_tokens AS (
                    SELECT TRIM(COALESCE(alb.label, '')) AS label_disp
                    FROM files_albums alb
                    JOIN matched_albums m ON m.album_id = alb.id
                    WHERE COALESCE(TRIM(alb.label), '') <> ''
                ),
                norm_tokens AS (
                    SELECT LOWER(label_disp) AS label_norm, label_disp
                    FROM label_tokens
                    WHERE COALESCE(label_disp, '') <> ''
                ),
                counts AS (
                    SELECT label_norm, COUNT(*) AS c
                    FROM norm_tokens
                    GROUP BY label_norm
                ),
                best_disp AS (
                    SELECT
                        label_norm,
                        label_disp,
                        ROW_NUMBER() OVER (
                            PARTITION BY label_norm
                            ORDER BY COUNT(*) DESC, LENGTH(label_disp) DESC, label_disp ASC
                        ) AS rn
                    FROM norm_tokens
                    GROUP BY label_norm, label_disp
                )
                SELECT b.label_disp AS label, c.c
                FROM counts c
                JOIN best_disp b ON b.label_norm = c.label_norm AND b.rn = 1
                ORDER BY c.c DESC, label ASC
                LIMIT %s
                """,
                (g, g, int(limit)),
            )
            rows = cur.fetchall()

        labels = [{"label": (r[0] or "").strip(), "count": int(r[1] or 0)} for r in rows if str(r[0] or "").strip()]
        payload = {"genre": g, "album_count": int(album_count), "labels": labels}
        _files_cache_set_json(cache_key, payload, ttl=60)
        return jsonify(payload)
    finally:
        conn.close()

def api_library_search_suggest():
    """Unified typeahead search across artists, classical entities, albums, tracks and genres (Files mode)."""
    query = (request.args.get("q") or "").strip()
    limit = max(1, min(40, _parse_int_loose(request.args.get("limit"), 12)))
    album_match_sql = "1=1"
    artist_match_sql = "1=1"
    artist_count_match_sql = "1=1"
    if not query:
        return jsonify({"query": "", "items": []})
    if _get_library_mode() != "files":
        return jsonify({"query": query, "items": []})

    cache_key = f"library:search:suggest:{query.lower()}:{limit}:browseall"
    cached = _files_cache_get_json(cache_key)
    if cached is not None:
        return jsonify(cached)

    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"query": query, "items": [], "error": err or "Files index unavailable"}), 503
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"query": query, "items": [], "error": "PostgreSQL unavailable"}), 503

    like = f"%{query}%"
    query_norm = _norm_artist_key(query)
    query_signature = _classical_person_signature_key(query)
    per_kind = max(8, min(120, limit * 4))
    base_url = request.url_root.rstrip("/")
    merged: list[dict] = []
    try:
        with conn.cursor() as cur:
            # Artists
            try:
                cur.execute(
                    """
                    SELECT
                        a.id,
                        a.name,
                        COALESCE(a.entity_kind, 'artist') AS entity_kind,
                        COALESCE(a.roles_json, '[]') AS roles_json,
                        (
                            SELECT COUNT(DISTINCT link_cnt.album_id)
                            FROM files_artist_album_links link_cnt
                            JOIN files_albums alb_cnt ON alb_cnt.id = link_cnt.album_id
                            WHERE link_cnt.artist_id = a.id
                              AND """ + artist_count_match_sql + """
                        ) AS album_count,
                        (""" + _artist_has_true_image_sql("a", "ext") + """) AS has_image,
                        GREATEST(similarity(a.name, %s), similarity(COALESCE(a.canonical_name, ''), %s)) AS score,
                        CASE
                            WHEN lower(a.name) LIKE lower(%s) || '%%'
                              OR lower(COALESCE(a.canonical_name, '')) LIKE lower(%s) || '%%'
                            THEN 0 ELSE 1
                        END AS prefix_rank
                    FROM files_artists a
                    LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                    WHERE (
                        a.name ILIKE %s
                        OR COALESCE(a.canonical_name, '') ILIKE %s
                        OR (%s <> '' AND COALESCE(a.canonical_name_norm, '') = %s)
                        OR COALESCE(a.aliases_json, '[]') ILIKE %s
                        OR EXISTS (
                            SELECT 1
                            FROM files_artist_aliases alias
                            WHERE alias.artist_id = a.id
                              AND (
                                  alias.alias ILIKE %s
                                  OR (%s <> '' AND alias.alias_norm = %s)
                                  OR (%s <> '' AND alias.alias_signature = %s)
                              )
                        )
                    )
                      AND EXISTS (
                        SELECT 1
                        FROM files_artist_album_links link2
                        JOIN files_albums alb2 ON alb2.id = link2.album_id
                        WHERE link2.artist_id = a.id
                          AND """ + artist_match_sql + """
                      )
                    ORDER BY prefix_rank ASC, score DESC, album_count DESC, a.name ASC
                    LIMIT %s
                    """,
                    (query, query, query, query, like, like, query_norm, query_norm, like, like, query_norm, query_norm, query_signature, query_signature, per_kind),
                )
            except Exception:
                cur.execute(
                    """
                    SELECT
                        a.id,
                        a.name,
                        COALESCE(a.entity_kind, 'artist') AS entity_kind,
                        COALESCE(a.roles_json, '[]') AS roles_json,
                        (
                            SELECT COUNT(DISTINCT link_cnt.album_id)
                            FROM files_artist_album_links link_cnt
                            JOIN files_albums alb_cnt ON alb_cnt.id = link_cnt.album_id
                            WHERE link_cnt.artist_id = a.id
                              AND """ + artist_count_match_sql + """
                        ) AS album_count,
                        (""" + _artist_has_true_image_sql("a", "ext") + """) AS has_image,
                        0.0 AS score,
                        1 AS prefix_rank
                    FROM files_artists a
                    LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                    WHERE (
                        a.name ILIKE %s
                        OR COALESCE(a.canonical_name, '') ILIKE %s
                        OR (%s <> '' AND COALESCE(a.canonical_name_norm, '') = %s)
                        OR COALESCE(a.aliases_json, '[]') ILIKE %s
                        OR EXISTS (
                            SELECT 1
                            FROM files_artist_aliases alias
                            WHERE alias.artist_id = a.id
                              AND (
                                  alias.alias ILIKE %s
                                  OR (%s <> '' AND alias.alias_norm = %s)
                                  OR (%s <> '' AND alias.alias_signature = %s)
                              )
                        )
                    )
                      AND EXISTS (
                        SELECT 1
                        FROM files_artist_album_links link2
                        JOIN files_albums alb2 ON alb2.id = link2.album_id
                        WHERE link2.artist_id = a.id
                          AND """ + artist_match_sql + """
                      )
                    ORDER BY album_count DESC, a.name ASC
                    LIMIT %s
                    """,
                    (like, like, query_norm, query_norm, like, like, query_norm, query_norm, query_signature, query_signature, per_kind),
                )
            for row in cur.fetchall():
                artist_id = int(row[0] or 0)
                entity_kind = str(row[2] or "artist").strip().lower() or "artist"
                roles = _safe_json_load(row[3] or "[]", fallback=[])
                label = entity_kind.replace("_", " ").title()
                merged.append(
                    {
                        "type": entity_kind,
                        "artist_id": artist_id,
                        "title": row[1] or "",
                        "subtitle": f"{label} · {int(row[4] or 0)} album(s)",
                        "roles": roles if isinstance(roles, list) else [],
                        "thumb": _artist_image_asset_url(base_url, artist_id, size=96) if artist_id > 0 else None,
                        "_score": float(row[6] or 0.0),
                        "_prefix": int(row[7] or 1),
                        "_rank": 0,
                    }
                )

            # Classical entities (composer / conductor / orchestra)
            classical_fetch_limit = max(72, min(360, per_kind * 18))
            classical_like = like
            classical_query_norm = _classical_norm_text(query)
            classical_merged: dict[tuple[str, str], dict[str, Any]] = {}
            if classical_query_norm:
                cur.execute(
                    """
                    SELECT
                        alb.id,
                        alb.title,
                        ar.id AS artist_id,
                        ar.name AS artist_name,
                        COALESCE(alb.year, 0) AS year,
                        alb.has_cover,
                        COALESCE(alb.tags_json, '{}') AS tags_json
                    FROM files_albums alb
                    JOIN files_artists ar ON ar.id = alb.artist_id
                    WHERE COALESCE(alb.tags_json, '') <> ''
                      AND COALESCE(alb.tags_json, '') NOT IN ('[]', '{}')
                      AND alb.primary_tags_json ILIKE %s
                      AND """
                    + album_match_sql
                    + """
                    ORDER BY alb.track_count DESC, alb.title ASC
                    LIMIT %s
                    """,
                    (classical_like, int(classical_fetch_limit)),
                )
                role_labels = {
                    "composer": "Composer",
                    "conductor": "Conductor",
                    "orchestra": "Orchestra",
                }
                for row in cur.fetchall():
                    album_id = int(row[0] or 0)
                    album_title = str(row[1] or "").strip()
                    artist_id = int(row[2] or 0)
                    artist_name = str(row[3] or "").strip()
                    album_year = int(row[4] or 0)
                    album_has_cover = bool(row[5])
                    try:
                        tags_map = json.loads(row[6] or "{}") if row[6] else {}
                    except Exception:
                        tags_map = {}
                    if not isinstance(tags_map, dict):
                        continue
                    classical_payload = _classical_display_payload(
                        tags_map,
                        fallback_title=album_title,
                        fallback_artist=artist_name,
                    )
                    if not isinstance(classical_payload, dict):
                        continue
                    for entity_type, values in (
                        ("composer", classical_payload.get("composer") or []),
                        ("conductor", classical_payload.get("conductor") or []),
                        ("orchestra", classical_payload.get("orchestra") or []),
                        ("ensemble", classical_payload.get("ensemble") or []),
                    ):
                        for value in values:
                            display_name = re.sub(r"\s+", " ", str(value or "").strip())
                            display_norm = _classical_norm_text(display_name)
                            if not display_norm or classical_query_norm not in display_norm:
                                continue
                            key = (entity_type, display_norm)
                            entry = classical_merged.get(key)
                            if entry is None:
                                entry = {
                                    "type": entity_type,
                                    "title": display_name,
                                    "label": role_labels.get(entity_type, "Classical"),
                                    "search_query": display_name,
                                    "album_ids": set(),
                                    "representative_album_id": album_id if album_id > 0 else None,
                                    "representative_artist_id": artist_id if artist_id > 0 else None,
                                    "representative_artist_name": artist_name,
                                    "representative_year": album_year if album_year > 0 else None,
                                    "representative_has_cover": bool(album_has_cover),
                                    "_prefix": 0 if display_norm.startswith(classical_query_norm) else 1,
                                    "_score": 0.0,
                                }
                                classical_merged[key] = entry
                            entry["album_ids"].add(album_id)
                            current_has_cover = bool(entry.get("representative_has_cover"))
                            if album_has_cover and not current_has_cover:
                                entry["representative_album_id"] = album_id if album_id > 0 else entry.get("representative_album_id")
                                entry["representative_artist_id"] = artist_id if artist_id > 0 else entry.get("representative_artist_id")
                                entry["representative_artist_name"] = artist_name or entry.get("representative_artist_name")
                                entry["representative_year"] = album_year if album_year > 0 else entry.get("representative_year")
                                entry["representative_has_cover"] = True
                if classical_merged:
                    entity_norms = [key[1] for key in classical_merged.keys()]
                    artist_by_norm: dict[str, int] = {}
                    external_image_norms: set[str] = set()
                    if entity_norms:
                        cur.execute(
                            "SELECT id, name_norm FROM files_artists WHERE name_norm = ANY(%s)",
                            (entity_norms,),
                        )
                        for artist_id, name_norm in cur.fetchall():
                            norm_key = str(name_norm or "").strip()
                            if norm_key:
                                artist_by_norm[norm_key] = int(artist_id or 0)
                        cur.execute(
                            "SELECT name_norm FROM files_external_artist_images WHERE name_norm = ANY(%s) AND COALESCE(image_path, '') <> ''",
                            (entity_norms,),
                        )
                        for (name_norm,) in cur.fetchall():
                            norm_key = str(name_norm or "").strip()
                            if norm_key:
                                external_image_norms.add(norm_key)
                    for (entity_type, entity_norm), entry in classical_merged.items():
                        album_count = len(entry.get("album_ids") or set())
                        direct_artist_id = int(artist_by_norm.get(entity_norm) or 0)
                        rep_album_id = int(entry.get("representative_album_id") or 0)
                        thumb = None
                        if direct_artist_id > 0:
                            thumb = f"{base_url}/api/library/files/artist/{direct_artist_id}/image?size=96"
                        elif entity_norm in external_image_norms:
                            thumb = f"{base_url}/api/library/external/artist-image/{quote(entity_norm, safe='')}?size=96"
                        elif rep_album_id > 0 and bool(entry.get('representative_has_cover')):
                            thumb = f"{base_url}/api/library/files/album/{rep_album_id}/cover?size=96"
                        merged.append(
                            {
                                "type": entity_type,
                                "artist_id": direct_artist_id if direct_artist_id > 0 else None,
                                "album_id": rep_album_id if rep_album_id > 0 else None,
                                "title": str(entry.get("title") or ""),
                                "subtitle": f"{album_count} album(s)",
                                "thumb": thumb,
                                "search_query": str(entry.get("search_query") or entry.get("title") or ""),
                                "_score": float(album_count) + (3.0 if direct_artist_id > 0 else 0.0),
                                "_prefix": int(entry.get("_prefix") or 1),
                                "_rank": 1,
                            }
                        )

            # Albums
            try:
                cur.execute(
                    """
                    SELECT
                        alb.id,
                        alb.title,
                        ar.id AS artist_id,
                        ar.name AS artist_name,
                        COALESCE(alb.year, 0) AS year,
                        alb.has_cover,
                        (similarity(alb.title, %s) * 0.78 + similarity(ar.name, %s) * 0.22) AS score,
                        CASE
                            WHEN lower(alb.title) LIKE lower(%s) || '%%' THEN 0
                            WHEN lower(ar.name) LIKE lower(%s) || '%%' THEN 1
                            ELSE 2
                        END AS prefix_rank
                    FROM files_albums alb
                    JOIN files_artists ar ON ar.id = alb.artist_id
                    WHERE (alb.title ILIKE %s OR ar.name ILIKE %s)
                      AND """ + album_match_sql + """
                    ORDER BY prefix_rank ASC, score DESC, alb.track_count DESC, alb.title ASC
                    LIMIT %s
                    """,
                    (query, query, query, query, like, like, per_kind),
                )
            except Exception:
                cur.execute(
                    """
                    SELECT
                        alb.id,
                        alb.title,
                        ar.id AS artist_id,
                        ar.name AS artist_name,
                        COALESCE(alb.year, 0) AS year,
                        alb.has_cover,
                        0.0 AS score,
                        2 AS prefix_rank
                    FROM files_albums alb
                    JOIN files_artists ar ON ar.id = alb.artist_id
                    WHERE (alb.title ILIKE %s OR ar.name ILIKE %s)
                      AND """ + album_match_sql + """
                    ORDER BY alb.track_count DESC, alb.title ASC
                    LIMIT %s
                    """,
                    (like, like, per_kind),
                )
            for row in cur.fetchall():
                album_id = int(row[0] or 0)
                artist_id = int(row[2] or 0)
                year = int(row[4] or 0)
                merged.append(
                    {
                        "type": "album",
                        "album_id": album_id,
                        "artist_id": artist_id,
                        "title": row[1] or "",
                        "subtitle": f"{row[3] or ''}{' · ' + str(year) if year > 0 else ''}",
                        "thumb": f"{base_url}/api/library/files/album/{album_id}/cover?size=96" if bool(row[5]) else None,
                        "_score": float(row[6] or 0.0),
                        "_prefix": int(row[7] or 2),
                        "_rank": 2,
                    }
                )

            # Tracks
            try:
                cur.execute(
                    """
                    SELECT
                        tr.id,
                        tr.title,
                        tr.duration_sec,
                        tr.track_num,
                        alb.id AS album_id,
                        alb.title AS album_title,
                        ar.id AS artist_id,
                        ar.name AS artist_name,
                        alb.has_cover,
                        COALESCE(st.play_count, 0) AS play_count,
                        (
                            similarity(tr.title, %s) * 0.70 +
                            similarity(alb.title, %s) * 0.15 +
                            similarity(ar.name, %s) * 0.15
                        ) AS score,
                        CASE
                            WHEN lower(tr.title) LIKE lower(%s) || '%%' THEN 0
                            WHEN lower(alb.title) LIKE lower(%s) || '%%' THEN 1
                            WHEN lower(ar.name) LIKE lower(%s) || '%%' THEN 2
                            ELSE 3
                        END AS prefix_rank
                    FROM files_tracks tr
                    JOIN files_albums alb ON alb.id = tr.album_id
                    JOIN files_artists ar ON ar.id = alb.artist_id
                    LEFT JOIN files_reco_track_stats st ON st.track_id = tr.id
                    WHERE (tr.title ILIKE %s OR alb.title ILIKE %s OR ar.name ILIKE %s)
                      AND """ + album_match_sql + """
                    ORDER BY prefix_rank ASC, score DESC, play_count DESC, tr.id DESC
                    LIMIT %s
                    """,
                    (query, query, query, query, query, query, like, like, like, per_kind),
                )
            except Exception:
                cur.execute(
                    """
                    SELECT
                        tr.id,
                        tr.title,
                        tr.duration_sec,
                        tr.track_num,
                        alb.id AS album_id,
                        alb.title AS album_title,
                        ar.id AS artist_id,
                        ar.name AS artist_name,
                        alb.has_cover,
                        0 AS play_count,
                        0.0 AS score,
                        3 AS prefix_rank
                    FROM files_tracks tr
                    JOIN files_albums alb ON alb.id = tr.album_id
                    JOIN files_artists ar ON ar.id = alb.artist_id
                    WHERE (tr.title ILIKE %s OR alb.title ILIKE %s OR ar.name ILIKE %s)
                      AND """ + album_match_sql + """
                    ORDER BY tr.id DESC
                    LIMIT %s
                    """,
                    (like, like, like, per_kind),
                )
            for row in cur.fetchall():
                track_id = int(row[0] or 0)
                album_id = int(row[4] or 0)
                artist_id = int(row[6] or 0)
                merged.append(
                    {
                        "type": "track",
                        "track_id": track_id,
                        "album_id": album_id,
                        "artist_id": artist_id,
                        "title": row[1] or "",
                        "subtitle": f"{row[7] or ''} · {row[5] or ''}",
                        "duration_sec": int(row[2] or 0),
                        "track_num": int(row[3] or 0),
                        "thumb": f"{base_url}/api/library/files/album/{album_id}/cover?size=96" if bool(row[8]) else None,
                        "_score": float(row[10] or 0.0),
                        "_prefix": int(row[11] or 3),
                        "_rank": 3,
                    }
                )

            # Genres
            cur.execute(
                """
                WITH genre_tokens AS (
                    SELECT
                        alb.id AS album_id,
                        TRIM(g.value) AS genre_disp
                    FROM files_albums alb
                    CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                    WHERE COALESCE(TRIM(g.value), '') <> ''
                      AND """
                + album_match_sql
                + """
                    UNION ALL
                    SELECT
                        alb.id AS album_id,
                        TRIM(alb.genre) AS genre_disp
                    FROM files_albums alb
                    WHERE COALESCE(TRIM(alb.genre), '') <> ''
                      AND COALESCE(alb.tags_json, '[]') = '[]'
                      AND """
                + album_match_sql
                + """
                ),
                norm_tokens AS (
                    SELECT
                        album_id,
                        LOWER(genre_disp) AS genre_norm,
                        genre_disp
                    FROM genre_tokens
                    WHERE COALESCE(genre_disp, '') <> ''
                ),
                counts AS (
                    SELECT genre_norm, COUNT(DISTINCT album_id) AS c
                    FROM norm_tokens
                    GROUP BY genre_norm
                ),
                best_disp AS (
                    SELECT
                        genre_norm,
                        genre_disp,
                        ROW_NUMBER() OVER (
                            PARTITION BY genre_norm
                            ORDER BY COUNT(DISTINCT album_id) DESC, LENGTH(genre_disp) DESC, genre_disp ASC
                        ) AS rn
                    FROM norm_tokens
                    GROUP BY genre_norm, genre_disp
                )
                SELECT
                    b.genre_disp AS genre,
                    c.c,
                    CASE WHEN lower(b.genre_disp) LIKE lower(%s) || '%%' THEN 0 ELSE 1 END AS prefix_rank
                FROM counts c
                JOIN best_disp b ON b.genre_norm = c.genre_norm AND b.rn = 1
                WHERE b.genre_disp ILIKE %s
                ORDER BY prefix_rank ASC, c.c DESC, genre ASC
                LIMIT %s
                """,
                (query, like, int(per_kind)),
            )
            for row in cur.fetchall():
                merged.append(
                    {
                        "type": "genre",
                        "title": row[0] or "",
                        "subtitle": f"{int(row[1] or 0)} album(s)",
                        "_score": float(row[1] or 0.0),
                        "_prefix": int(row[2] or 1),
                        "_rank": 4,
                    }
                )

        merged.sort(
            key=lambda x: (
                int(x.get("_prefix", 9)),
                -float(x.get("_score", 0.0)),
                int(x.get("_rank", 9)),
                str(x.get("title", "")).lower(),
            )
        )
        def _dedupe_key(item: dict[str, Any]) -> tuple[Any, ...]:
            item_type = str(item.get("type") or "").strip().lower()
            if item_type in {"artist", "composer", "conductor", "orchestra", "ensemble", "soloist", "performer"}:
                return ("entity", item_type, _norm_artist_key(str(item.get("title") or "")))
            if item_type == "album":
                return ("album", int(item.get("album_id") or 0))
            if item_type == "track":
                return ("track", int(item.get("track_id") or 0))
            if item_type == "genre":
                return ("genre", str(item.get("title") or "").strip().lower())
            return (item_type, str(item.get("title") or "").strip().lower())

        items = []
        seen_keys: set[tuple[Any, ...]] = set()
        for item in merged:
            key = _dedupe_key(item)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            clean = dict(item)
            clean.pop("_score", None)
            clean.pop("_prefix", None)
            clean.pop("_rank", None)
            items.append(clean)
            if len(items) >= limit:
                break
        payload = {"query": query, "items": items}
        _files_cache_set_json(cache_key, payload, ttl=20)
        return jsonify(payload)
    finally:
        conn.close()


def api_library_entity_discover():
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    if not _auth_user_can_use_ai(_current_user_or_empty()):
        return jsonify({"error": "AI access is disabled for this user"}), 403
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        data = {}
    entity_type = str(data.get("entity_type") or "").strip().lower()
    if entity_type not in {"artist", "album", "label"}:
        return jsonify({"error": "unsupported entity_type"}), 400
    entity_id = _parse_int_loose(data.get("entity_id"), 0)
    artist_id = _parse_int_loose(data.get("artist_id"), 0) or (entity_id if entity_type == "artist" else 0)
    album_id = _parse_int_loose(data.get("album_id"), 0) or (entity_id if entity_type == "album" else 0)
    label_value = str(data.get("label") or "").strip()
    base_url = request.url_root.rstrip("/")
    uid = _current_user_id_or_zero()
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        entity_label = ""
        query_terms = ""
        context_lines: list[str] = []
        sections: list[dict[str, Any]] = []

        with conn.cursor() as cur:
            if entity_type == "artist":
                if artist_id <= 0:
                    return jsonify({"error": "artist_id is required"}), 400
                cur.execute(
                    """
                    SELECT id, name, album_count, track_count, COALESCE(name_norm, '')
                    FROM files_artists
                    WHERE id = %s
                    """,
                    (int(artist_id),),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Artist not found"}), 404
                artist_id = int(row[0] or 0)
                entity_label = str(row[1] or "").strip()
                artist_norm = str(row[4] or "").strip()
                context_lines.append(f"Artist: {entity_label}")
                context_lines.append(f"Local albums: {int(row[2] or 0)}")
                context_lines.append(f"Local tracks: {int(row[3] or 0)}")
                query_terms = f"{entity_label} similar artists last.fm bandcamp"

                cur.execute(
                    """
                    WITH artist_albums AS (
                        SELECT DISTINCT album_id
                        FROM files_artist_album_links
                        WHERE artist_id = %s
                    )
                    SELECT alb.id, alb.title, COALESCE(alb.year, 0), alb.track_count, COALESCE(alb.format, ''), alb.is_lossless
                    FROM artist_albums aa
                    JOIN files_albums alb ON alb.id = aa.album_id
                    ORDER BY COALESCE(alb.year, 0) DESC, alb.title ASC
                    LIMIT 12
                    """,
                    (artist_id,),
                )
                local_album_links = [
                    _entity_discover_make_internal_link(
                        entity_type="album",
                        entity_id=int(aid or 0),
                        label=str(title or "").strip(),
                        base_url=base_url,
                        subtitle=" · ".join(
                            part for part in [
                                str(int(year or 0)) if int(year or 0) > 0 else "",
                                f"{int(track_count or 0)} tracks" if int(track_count or 0) > 0 else "",
                                (str(fmt or "").strip() or "") + (" lossless" if bool(is_lossless) and str(fmt or "").strip() else (" lossy" if str(fmt or "").strip() else "")),
                            ] if part
                        ),
                    )
                    for aid, title, year, track_count, fmt, is_lossless in cur.fetchall()
                    if int(aid or 0) > 0 and str(title or "").strip()
                ]
                if local_album_links:
                    sections.append({
                        "key": "local_albums",
                        "title": "In your library",
                        "reason": "Start from the albums you already own.",
                        "links": _entity_discover_dedup_links(local_album_links, limit=12),
                    })

                cur.execute(
                    """
                    SELECT COALESCE(similar_json, '[]')
                    FROM files_artist_profiles
                    WHERE name_norm = %s
                    LIMIT 1
                    """,
                    (artist_norm,),
                )
                similar_row = cur.fetchone()
                similar_payload = []
                try:
                    similar_payload = json.loads(str((similar_row[0] if similar_row else "[]") or "[]"))
                except Exception:
                    similar_payload = []
                similar_names: list[str] = []
                for item in similar_payload if isinstance(similar_payload, list) else []:
                    if isinstance(item, dict):
                        nm = str(item.get("name") or "").strip()
                    else:
                        nm = str(item or "").strip()
                    if nm:
                        similar_names.append(nm)
                library_similar_links: list[dict[str, Any]] = []
                for nm in similar_names[:12]:
                    cur.execute(
                        """
                        SELECT id, name
                        FROM files_artists
                        WHERE name_norm = %s
                        LIMIT 1
                        """,
                        (_norm_artist_key(nm),),
                    )
                    match = cur.fetchone()
                    if not match:
                        continue
                    library_similar_links.append(
                        _entity_discover_make_internal_link(
                            entity_type="artist",
                            entity_id=int(match[0] or 0),
                            label=str(match[1] or "").strip(),
                            base_url=base_url,
                            subtitle="Also present in your library",
                            provider="lastfm",
                        )
                    )
                if library_similar_links:
                    sections.append({
                        "key": "similar_in_library",
                        "title": "Also in your library",
                        "reason": "Artists already present locally and adjacent to this artist.",
                        "links": _entity_discover_dedup_links(library_similar_links, limit=10),
                    })

            elif entity_type == "album":
                if album_id <= 0:
                    return jsonify({"error": "album_id is required"}), 400
                cur.execute(
                    """
                    SELECT alb.id, alb.title, COALESCE(alb.title_norm, ''), COALESCE(alb.genre, ''), COALESCE(alb.label, ''),
                           alb.artist_id, ar.name, COALESCE(ar.name_norm, ''), COALESCE(alb.year, 0)
                    FROM files_albums alb
                    JOIN files_artists ar ON ar.id = alb.artist_id
                    WHERE alb.id = %s
                    """,
                    (int(album_id),),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Album not found"}), 404
                album_id = int(row[0] or 0)
                entity_label = str(row[1] or "").strip()
                title_norm = str(row[2] or "").strip()
                genre_text = str(row[3] or "").strip()
                label_text = str(row[4] or "").strip()
                artist_id = int(row[5] or 0)
                artist_name = str(row[6] or "").strip()
                artist_norm = str(row[7] or "").strip()
                year = int(row[8] or 0)
                context_lines.extend([
                    f"Album: {entity_label}",
                    f"Artist: {artist_name}",
                    f"Year: {year}" if year > 0 else "",
                    f"Genres: {genre_text}" if genre_text else "",
                    f"Label: {label_text}" if label_text else "",
                ])
                context_lines = [line for line in context_lines if line]
                query_terms = f"{artist_name} {entity_label} similar albums review last.fm bandcamp"

                cur.execute(
                    """
                    WITH artist_albums AS (
                        SELECT DISTINCT album_id
                        FROM files_artist_album_links
                        WHERE artist_id = %s
                    )
                    SELECT alb.id, alb.title, COALESCE(alb.year, 0), alb.track_count, COALESCE(alb.format, ''), alb.is_lossless
                    FROM artist_albums aa
                    JOIN files_albums alb ON alb.id = aa.album_id
                    WHERE alb.id <> %s
                    ORDER BY COALESCE(year, 0) DESC, title ASC
                    LIMIT 8
                    """,
                    (artist_id, album_id),
                )
                artist_album_links = [
                    _entity_discover_make_internal_link(
                        entity_type="album",
                        entity_id=int(aid or 0),
                        label=str(title or "").strip(),
                        base_url=base_url,
                        subtitle=" · ".join(
                            part for part in [
                                str(int(year2 or 0)) if int(year2 or 0) > 0 else "",
                                f"{int(track_count or 0)} tracks" if int(track_count or 0) > 0 else "",
                                str(fmt or "").strip(),
                            ] if part
                        ),
                    )
                    for aid, title, year2, track_count, fmt, _lossless in cur.fetchall()
                    if int(aid or 0) > 0 and str(title or "").strip()
                ]
                if artist_album_links:
                    sections.append({
                        "key": "more_from_artist",
                        "title": f"More from {artist_name}",
                        "reason": "The closest next step is usually another album by the same artist.",
                        "links": _entity_discover_dedup_links(artist_album_links, limit=8),
                    })

                genre_values = _split_genre_values(genre_text)[:4]
                same_vibe_links: list[dict[str, Any]] = []
                if genre_values:
                    cur.execute(
                        """
                        SELECT alb.id, alb.title, ar.id, ar.name, COALESCE(alb.year, 0), COALESCE(alb.format, ''), alb.is_lossless
                        FROM files_albums alb
                        JOIN files_artists ar ON ar.id = alb.artist_id
                        WHERE alb.id <> %s
                          AND ar.id <> %s
                          AND (
                            lower(COALESCE(alb.genre, '')) LIKE ANY(%s)
                            OR EXISTS (
                              SELECT 1
                              FROM jsonb_array_elements_text(
                                CASE
                                  WHEN trim(COALESCE(alb.tags_json, '')) = '' THEN '[]'::jsonb
                                  ELSE alb.tags_json::jsonb
                                END
                              ) AS t(tag)
                              WHERE lower(t.tag) = ANY(%s)
                            )
                          )
                        ORDER BY COALESCE(alb.updated_at, alb.created_at) DESC, alb.id DESC
                        LIMIT 10
                        """,
                        (
                            album_id,
                            artist_id,
                            [f"%{g.lower()}%" for g in genre_values],
                            [g.lower() for g in genre_values],
                        ),
                    )
                    same_vibe_links = [
                        _entity_discover_make_internal_link(
                            entity_type="album",
                            entity_id=int(aid or 0),
                            label=str(title or "").strip(),
                            base_url=base_url,
                            subtitle=" · ".join(
                                part for part in [
                                    str(an or "").strip(),
                                    str(int(year2 or 0)) if int(year2 or 0) > 0 else "",
                                    str(fmt or "").strip(),
                                ] if part
                            ),
                        )
                        for aid, title, _arid, an, year2, fmt, _lossless in cur.fetchall()
                        if int(aid or 0) > 0 and str(title or "").strip()
                    ]
                if same_vibe_links:
                    sections.append({
                        "key": "same_vibe_library",
                        "title": "Same vibe in your library",
                        "reason": "Albums nearby by genre and listening context.",
                        "links": _entity_discover_dedup_links(same_vibe_links, limit=8),
                    })

            else:
                if not label_value:
                    return jsonify({"error": "label is required"}), 400
                entity_label = label_value
                query_terms = f"{entity_label} record label artists discogs bandcamp"
                context_lines.append(f"Label: {entity_label}")
                cur.execute(
                    """
                    SELECT alb.id, alb.title, ar.id, ar.name, COALESCE(alb.year, 0), alb.track_count, COALESCE(alb.format, ''), alb.is_lossless
                    FROM files_albums alb
                    JOIN files_artists ar ON ar.id = alb.artist_id
                    WHERE lower(trim(COALESCE(alb.label, ''))) = lower(trim(%s))
                    ORDER BY COALESCE(alb.updated_at, alb.created_at) DESC, alb.id DESC
                    LIMIT 12
                    """,
                    (label_value,),
                )
                label_release_links: list[dict[str, Any]] = []
                label_artist_links: list[dict[str, Any]] = []
                seen_artist_ids: set[int] = set()
                for aid, title, arid, aname, year2, track_count, fmt, _lossless in cur.fetchall():
                    if int(aid or 0) > 0 and str(title or "").strip():
                        label_release_links.append(
                            _entity_discover_make_internal_link(
                                entity_type="album",
                                entity_id=int(aid or 0),
                                label=str(title or "").strip(),
                                base_url=base_url,
                                subtitle=" · ".join(
                                    part for part in [
                                        str(aname or "").strip(),
                                        str(int(year2 or 0)) if int(year2 or 0) > 0 else "",
                                        f"{int(track_count or 0)} tracks" if int(track_count or 0) > 0 else "",
                                        str(fmt or "").strip(),
                                    ] if part
                                ),
                            )
                        )
                    if int(arid or 0) > 0 and int(arid or 0) not in seen_artist_ids:
                        seen_artist_ids.add(int(arid or 0))
                        label_artist_links.append(
                            _entity_discover_make_internal_link(
                                entity_type="artist",
                                entity_id=int(arid or 0),
                                label=str(aname or "").strip(),
                                base_url=base_url,
                                subtitle=f"On {entity_label}",
                            )
                        )
                if label_release_links:
                    sections.append({
                        "key": "releases_on_label",
                        "title": "Releases on this label",
                        "reason": "Start with albums already indexed locally for this label.",
                        "links": _entity_discover_dedup_links(label_release_links, limit=10),
                    })
                if label_artist_links:
                    sections.append({
                        "key": "artists_on_label",
                        "title": "Artists on this label",
                        "reason": "These artists are already represented in your library.",
                        "links": _entity_discover_dedup_links(label_artist_links, limit=10),
                    })

        web_results: list[dict[str, Any]] = []
        if query_terms:
            try:
                web_results = _web_search_serper(query_terms, num=6, allow_ai_fallback=True) or []
            except Exception:
                web_results = []
        external_links = _assistant_links_from_web_results(web_results)
        if external_links:
            sections.append({
                "key": "on_the_web",
                "title": "Go deeper on the web",
                "reason": "Useful references and next-listen paths outside your current library.",
                "links": _entity_discover_dedup_links(external_links, limit=8),
            })

        sections = [sec for sec in sections if len(sec.get("links") or []) > 0]
        summary = _entity_discover_fallback_summary(entity_type=entity_type, entity_label=entity_label, sections=sections)
        ai_summary, provider_used, model_used, ai_used = _entity_discover_ai_summary(
            entity_type=entity_type,
            entity_label=entity_label,
            context_lines=context_lines,
            sections=sections,
            user_id=uid,
        )
        if ai_summary:
            summary = ai_summary
        return jsonify(
            {
                "entity_type": entity_type,
                "entity_label": entity_label,
                "generated_at": int(time.time()),
                "summary": summary,
                "sections": sections,
                "provider": provider_used or None,
                "model": model_used or None,
                "ai_used": bool(ai_used),
                "fallback_used": not bool(ai_used),
            }
        )
    finally:
        conn.close()
