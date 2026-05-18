"""Runtime-owned library catalog summary and personal statistics handlers."""
from __future__ import annotations

import json
import math
import time
from typing import Any

from flask import jsonify, request

_EXTRACTED_NAMES = {
    'api_library_digest',
    'api_library_top_artists',
    'api_library_recent_artists',
    'api_library_facets',
    'api_library_genres_suggest',
    'api_library_labels_suggest',
    'api_library_genres',
    'api_library_labels',
    'api_library_recently_played_albums',
    'api_library_liked_summary',
    'api_library_playback_stats',
    'api_library_missing_tags',
}


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

def api_library_digest():
    """
    Library Digest feed (Files mode): recent albums that *have* a review snippet (short_description).

    Important behavior:
    - Albums without a review snippet are omitted (no placeholder).
    - Optionally triggers background enrichment for missing snippets on the returned recent albums.
    """
    if _get_library_mode() != "files":
        return jsonify({"albums": [], "limit": 0, "generated_at": int(time.time()), "enrichment": {"triggered": False, "missing_total": 0, "available_total": 0, "error": "Files mode required"}}), 400

    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"albums": [], "limit": 0, "generated_at": int(time.time()), "enrichment": {"triggered": False, "missing_total": 0, "available_total": 0, "error": err or "Files index unavailable"}}), 503

    limit = max(1, min(36, _parse_int_loose(request.args.get("limit"), 12)))
    trigger = str(request.args.get("trigger", "1")).strip().lower() in {"1", "true", "yes"}
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    album_match_sql = (
        f"({_library_albums_match_where(include_unmatched, 'alb')})"
        f" AND ({_library_album_scope_where(scope, 'alb')})"
    )
    candidate_limit = max(int(limit), min(1000, int(limit) * 12))

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"albums": [], "limit": limit, "generated_at": int(time.time()), "enrichment": {"triggered": False, "missing_total": 0, "available_total": 0, "error": "PostgreSQL unavailable"}}), 503

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    alb.id,
                    alb.title,
                    alb.title_norm,
                    COALESCE(alb.year, 0) AS year,
                    COALESCE(alb.genre, '') AS genre,
                    COALESCE(alb.label, '') AS label,
                    COALESCE(alb.tags_json, '[]') AS tags_json,
                    alb.track_count,
                    COALESCE(alb.format, '') AS format,
                    alb.is_lossless,
                    alb.has_cover,
                    alb.mb_identified,
                    COALESCE(alb.primary_tags_json, '{}') AS primary_tags_json,
                    COALESCE(
                        (
                            SELECT link.artist_id
                            FROM files_artist_album_links link
                            JOIN files_artists comp ON comp.id = link.artist_id
                            WHERE link.album_id = alb.id
                              AND COALESCE(comp.entity_kind, 'artist') = 'composer'
                            ORDER BY CASE WHEN link.is_primary THEN 0 ELSE 1 END ASC, link.artist_id ASC
                            LIMIT 1
                        ),
                        ar.id
                    ) AS artist_id,
                    ar.name AS artist_name,
                    ar.name_norm AS artist_norm,
                    COALESCE(
                        (
                            SELECT json_agg(names.name ORDER BY names.sort_key, names.name)
                            FROM (
                                SELECT DISTINCT
                                    COALESCE(NULLIF(comp.canonical_name, ''), comp.name) AS name,
                                    CASE WHEN link_comp.is_primary THEN 0 ELSE 1 END AS sort_key
                                FROM files_artist_album_links link_comp
                                JOIN files_artists comp ON comp.id = link_comp.artist_id
                                WHERE link_comp.album_id = alb.id
                                  AND COALESCE(comp.entity_kind, 'artist') = 'composer'
                            ) AS names
                        )::text,
                        '[]'
                    ) AS composer_names_json,
                    COALESCE(pr.short_description, '') AS short_description,
                    COALESCE(pr.source, '') AS profile_source
                FROM files_albums alb
                JOIN files_artists ar ON ar.id = alb.artist_id
                LEFT JOIN files_album_profiles pr
                       ON pr.artist_norm = ar.name_norm
                      AND pr.title_norm = alb.title_norm
                WHERE """ + album_match_sql + """
                ORDER BY
                    CASE WHEN COALESCE(pr.short_description, '') <> '' THEN 0 ELSE 1 END ASC,
                    COALESCE(pr.updated_at, alb.created_at) DESC,
                    alb.id DESC
                LIMIT %s
                """,
                (int(candidate_limit),),
            )
            rows = cur.fetchall()

        base_url = request.url_root.rstrip("/")
        albums_with_reviews: list[dict] = []
        missing_by_artist: dict[str, dict] = {}
        missing_total = 0

        for (
            album_id,
            title,
            title_norm,
            year,
            genre,
            label,
            tags_json,
            track_count,
            fmt,
            is_lossless,
            has_cover,
            mb_identified,
            primary_tags_json_raw,
            artist_id,
            artist_name,
            artist_norm,
            composer_names_json,
            short_desc,
            profile_source,
        ) in rows:
            aid = int(album_id or 0)
            arid = int(artist_id or 0)
            if aid <= 0 or arid <= 0:
                continue

            # Parsed list of genres for UI badges (multi-genre albums).
            genres_list: list[str] = []
            try:
                tags_list = json.loads(tags_json) if tags_json else []
                if isinstance(tags_list, list):
                    for t in tags_list:
                        v = str(t or "").strip()
                        if v:
                            genres_list.append(v)
            except Exception:
                genres_list = []
            if not genres_list:
                try:
                    genres_list = _split_genre_values(genre or "")
                except Exception:
                    genres_list = []
            if genres_list:
                seen = set()
                deduped = []
                for g in genres_list:
                    gg = re.sub(r"\s+", " ", (g or "").strip())
                    if not gg:
                        continue
                    key = gg.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    deduped.append(gg)
                genres_list = deduped[:20]
            primary_tags_map = _safe_json_load(primary_tags_json_raw, fallback={})
            if not isinstance(primary_tags_map, dict):
                primary_tags_map = {}
            classical_payload = _classical_display_payload(
                primary_tags_map,
                fallback_title=str(title or ""),
                fallback_artist=str(artist_name or ""),
            )
            composer_names = _safe_json_load(composer_names_json, fallback=[])
            if not isinstance(composer_names, list):
                composer_names = []
            classical_payload = _files_apply_canonical_composers_to_classical_payload(
                classical_payload,
                composer_names,
            )

            short_desc_clean = (short_desc or "").strip()
            prof_source_clean = (profile_source or "").strip() or None

            if not short_desc_clean:
                missing_total += 1
                an = str(artist_norm or "").strip()
                tn = str(title_norm or "").strip()
                if an and tn:
                    slot = missing_by_artist.setdefault(
                        an,
                        {
                            "artist_name": str(artist_name or "").strip(),
                            "artist_norm": an,
                            "albums": [],
                        },
                    )
                    slot["albums"].append((str(title or "").strip(), tn))
                continue

            thumb = f"{base_url}/api/library/files/album/{aid}/cover?size=512" if bool(has_cover) else None
            albums_with_reviews.append(
                {
                    "album_id": aid,
                    "title": title or "",
                    "year": int(year or 0) or None,
                    "genre": (genre or "").strip() or None,
                    "genres": genres_list,
                    "label": (label or "").strip() or None,
                    "track_count": int(track_count or 0),
                    "format": (fmt or "").strip() or None,
                    "is_lossless": bool(is_lossless),
                    "mb_identified": bool(mb_identified),
                    "thumb": thumb,
                    "artist_id": arid,
                    "artist_name": _files_album_display_artist_name(
                        artist_name=str(artist_name or ""),
                        classical_payload=classical_payload,
                    ),
                    "short_description": short_desc_clean,
                    "profile_source": prof_source_clean,
                }
            )

        triggered = False
        if trigger and missing_by_artist:
            # Keep this bounded: we only want to backfill "recent digest" coverage, not the full library.
            max_artists = 10
            max_albums_per_artist = 24
            for idx, (_artist_norm, info) in enumerate(list(missing_by_artist.items())[:max_artists]):
                try:
                    artist_name = str(info.get("artist_name") or "").strip()
                    artist_norm = str(info.get("artist_norm") or "").strip()
                    albums = list(info.get("albums") or [])[:max_albums_per_artist]
                    if artist_name and artist_norm and albums:
                        ok_enq = _enqueue_files_profile_enrichment(
                            artist_name,
                            artist_norm,
                            albums,
                            priority_mode="p2",
                        )
                        triggered = triggered or bool(ok_enq)
                except Exception:
                    continue

        with _files_profile_jobs_lock:
            active_jobs = int(len(_files_profile_jobs_active))
        with _files_profile_backfill_lock:
            backfill_state = dict(_files_profile_backfill_state or {})

        payload = {
            "limit": int(limit),
            "generated_at": int(time.time()),
            "albums": albums_with_reviews[: int(limit)],
            "enrichment": {
                "triggered": bool(triggered),
                "missing_total": int(missing_total),
                "available_total": int(len(albums_with_reviews)),
                "active_jobs": active_jobs,
                "profile_backfill": backfill_state,
            },
        }
        return jsonify(payload)
    finally:
        conn.close()

def api_library_top_artists():
    """Return top listened artists (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"artists": [], "error": "Files mode required"}), 400
    limit = max(1, min(200, _parse_int_loose(request.args.get("limit"), 18)))
    offset = max(0, _parse_int_loose(request.args.get("offset"), 0))
    days = max(0, min(3650, _parse_int_loose(request.args.get("days"), 0)))
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    album_match_sql = _library_albums_match_where(include_unmatched, "alb")
    album_count_sql = _library_albums_match_where(include_unmatched, "alb_cnt")
    album_scope_sql = _library_album_scope_where(scope, "alb")
    album_count_scope_sql = _library_album_scope_where(scope, "alb_cnt")

    cache_key = (
        f"library:top_artists:{limit}:{offset}:{days}:"
        f"{_library_cache_scope_suffix(scope)}:{_library_cache_unmatched_suffix(include_unmatched)}"
    )
    cached = _files_cache_get_json(cache_key)
    if cached is not None:
        return jsonify(cached)
    _files_index_maybe_enqueue_published_catchup(
        include_unmatched=include_unmatched,
        scope=scope,
        reason=f"api_library_top_artists_{scope}",
    )
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"artists": [], "error": err or "Files index unavailable"}), 503

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"artists": [], "error": "PostgreSQL unavailable"}), 503
    try:
        base_url = request.url_root.rstrip("/")
        total = 0
        with conn.cursor() as cur:
            if days > 0:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT ar.id
                        FROM files_reco_events e
                        JOIN files_tracks tr ON tr.id = e.track_id
                        JOIN files_albums alb ON alb.id = tr.album_id
                        JOIN (SELECT DISTINCT artist_id, album_id FROM files_artist_album_links) link ON link.album_id = alb.id
                        JOIN files_artists ar ON ar.id = link.artist_id
                        WHERE e.created_at >= (NOW() - (%s || ' days')::INTERVAL)
                          AND """
                    + album_match_sql
                    + """
                          AND """
                    + album_scope_sql
                    + """
                        GROUP BY ar.id
                    ) AS ranked
                    """,
                    (int(days),),
                )
                total = int((cur.fetchone() or [0])[0] or 0)
                cur.execute(
                    """
                    SELECT
                        ar.id,
                        ar.name,
                        (
                            SELECT COUNT(DISTINCT link_cnt.album_id)
                            FROM files_artist_album_links link_cnt
                            JOIN files_albums alb_cnt ON alb_cnt.id = link_cnt.album_id
                            WHERE link_cnt.artist_id = ar.id
                              AND """ + album_count_sql + """
                              AND """ + album_count_scope_sql + """
                        ) AS album_count,
                        COALESCE(BOOL_OR(""" + _artist_has_true_image_sql("ar", "ext") + """), FALSE) AS has_image,
                        COALESCE(SUM(CASE WHEN e.event_type IN ('play_complete', 'like') THEN 1 ELSE 0 END), 0) AS completion_count,
                        COALESCE(SUM(CASE WHEN e.event_type IN ('play_start', 'play_partial', 'play_complete', 'like') THEN 1 ELSE 0 END), 0) AS play_events
                    FROM files_reco_events e
                    JOIN files_tracks tr ON tr.id = e.track_id
                    JOIN files_albums alb ON alb.id = tr.album_id
                    JOIN (SELECT DISTINCT artist_id, album_id FROM files_artist_album_links) link ON link.album_id = alb.id
                    JOIN files_artists ar ON ar.id = link.artist_id
                    LEFT JOIN files_external_artist_images ext ON ext.name_norm = ar.name_norm
                    WHERE e.created_at >= (NOW() - (%s || ' days')::INTERVAL)
                      AND """ + album_match_sql + """
                      AND """ + album_scope_sql + """
                    GROUP BY ar.id, ar.name
                    ORDER BY completion_count DESC, play_events DESC, album_count DESC, ar.name ASC
                    LIMIT %s OFFSET %s
                    """,
                    (int(days), int(limit), int(offset)),
                )
            else:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT ar.id
                        FROM files_reco_track_stats st
                        JOIN files_tracks tr ON tr.id = st.track_id
                        JOIN files_albums alb ON alb.id = tr.album_id
                        JOIN (SELECT DISTINCT artist_id, album_id FROM files_artist_album_links) link ON link.album_id = alb.id
                        JOIN files_artists ar ON ar.id = link.artist_id
                        WHERE """
                    + album_match_sql
                    + """
                        AND """
                    + album_scope_sql
                    + """
                        GROUP BY ar.id
                    ) AS ranked
                    """
                )
                total = int((cur.fetchone() or [0])[0] or 0)
                cur.execute(
                    """
                    SELECT
                        ar.id,
                        ar.name,
                        (
                            SELECT COUNT(DISTINCT link_cnt.album_id)
                            FROM files_artist_album_links link_cnt
                            JOIN files_albums alb_cnt ON alb_cnt.id = link_cnt.album_id
                            WHERE link_cnt.artist_id = ar.id
                              AND """ + album_count_sql + """
                              AND """ + album_count_scope_sql + """
                        ) AS album_count,
                        COALESCE(BOOL_OR(""" + _artist_has_true_image_sql("ar", "ext") + """), FALSE) AS has_image,
                        COALESCE(SUM(st.completion_count), 0) AS completion_count,
                        COALESCE(SUM(st.play_count), 0) AS play_count
                    FROM files_reco_track_stats st
                    JOIN files_tracks tr ON tr.id = st.track_id
                    JOIN files_albums alb ON alb.id = tr.album_id
                    JOIN (SELECT DISTINCT artist_id, album_id FROM files_artist_album_links) link ON link.album_id = alb.id
                    JOIN files_artists ar ON ar.id = link.artist_id
                    LEFT JOIN files_external_artist_images ext ON ext.name_norm = ar.name_norm
                    WHERE """ + album_match_sql + """
                      AND """ + album_scope_sql + """
                    GROUP BY ar.id, ar.name
                    ORDER BY completion_count DESC, play_count DESC, album_count DESC, ar.name ASC
                    LIMIT %s OFFSET %s
                    """,
                    (int(limit), int(offset)),
                )
            rows = cur.fetchall()

        artists = []
        for artist_id, name, album_count, has_image, completion_count, play_count in rows:
            aid = int(artist_id or 0)
            if aid <= 0:
                continue
            thumb = f"{base_url}/api/library/files/artist/{aid}/image?size=192" if bool(has_image) else None
            artists.append(
                {
                    "artist_id": aid,
                    "artist_name": name or "",
                    "album_count": int(album_count or 0),
                    "completion_count": int(completion_count or 0),
                    "play_count": int(play_count or 0),
                    "thumb": thumb,
                }
            )

        payload = {"artists": artists, "total": int(total), "limit": int(limit), "offset": int(offset), "days": int(days)}
        _files_cache_set_json(cache_key, payload, ttl=30)
        return jsonify(payload)
    finally:
        conn.close()

def api_library_recent_artists():
    """Return recently added artists based on latest album import date."""
    if _get_library_mode() != "files":
        return jsonify({"artists": [], "error": "Files mode required"}), 400
    limit = max(1, min(60, _parse_int_loose(request.args.get("limit"), 18)))
    offset = max(0, _parse_int_loose(request.args.get("offset"), 0))
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    album_match_sql = _library_albums_match_where(include_unmatched, "alb")
    album_scope_sql = _library_album_scope_where(scope, "alb")
    cache_key = (
        f"library:artists:recent:{limit}:{offset}:"
        f"{_library_cache_scope_suffix(scope)}:{_library_cache_unmatched_suffix(include_unmatched)}"
    )
    cached = _files_cache_get_json(cache_key)
    if cached is not None:
        return jsonify(cached)
    _files_index_maybe_enqueue_published_catchup(
        include_unmatched=include_unmatched,
        scope=scope,
        reason=f"api_library_recent_artists_{scope}",
    )
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"artists": [], "error": err or "Files index unavailable"}), 503

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"artists": [], "error": "PostgreSQL unavailable"}), 503
    try:
        base_url = request.url_root.rstrip("/")
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ar.id,
                    ar.name,
                    COALESCE(BOOL_OR(""" + _artist_has_true_image_sql("ar", "ext") + """), FALSE) AS has_image,
                    COUNT(DISTINCT aa.album_id)::BIGINT AS album_count,
                    EXTRACT(EPOCH FROM MAX(alb.created_at))::BIGINT AS last_added_at
                FROM (SELECT DISTINCT artist_id, album_id FROM files_artist_album_links) aa
                JOIN files_albums alb ON alb.id = aa.album_id
                JOIN files_artists ar ON ar.id = aa.artist_id
                LEFT JOIN files_external_artist_images ext ON ext.name_norm = ar.name_norm
                WHERE """
                + album_match_sql
                + """
                  AND """
                + album_scope_sql
                + """
                GROUP BY ar.id, ar.name
                ORDER BY MAX(alb.created_at) DESC, ar.name ASC
                LIMIT %s OFFSET %s
                """,
                (int(limit), int(offset)),
            )
            rows = cur.fetchall()

        artists = []
        for artist_id, name, _has_image, album_count, last_added_at in rows:
            aid = int(artist_id or 0)
            if aid <= 0:
                continue
            artists.append(
                {
                    "artist_id": aid,
                    "artist_name": name or "",
                    "album_count": int(album_count or 0),
                    "thumb": _artist_image_asset_url(base_url, aid, size=192) or None,
                    "last_added_at": int(last_added_at or 0),
                }
            )

        payload = {"artists": artists, "limit": int(limit), "offset": int(offset)}
        _files_cache_set_json(cache_key, payload, ttl=30)
        return jsonify(payload)
    finally:
        conn.close()


def api_library_facets():
    """Return library facets (genres/labels/years) for discovery chips (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"genres": [], "labels": [], "years": [], "error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"genres": [], "labels": [], "years": [], "error": err or "Files index unavailable"}), 503
    limit_genres = max(1, min(80, _parse_int_loose(request.args.get("limit_genres"), 24)))
    limit_labels = max(1, min(80, _parse_int_loose(request.args.get("limit_labels"), 24)))
    limit_years = max(1, min(200, _parse_int_loose(request.args.get("limit_years"), 50)))
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    album_match_sql = (
        f"({_library_albums_match_where(include_unmatched, 'alb')})"
        f" AND ({_library_album_scope_where(scope, 'alb')})"
    )

    cache_key = (
        f"library:facets:{limit_genres}:{limit_labels}:{limit_years}:"
        f"{_library_cache_scope_suffix(scope)}:{_library_cache_unmatched_suffix(include_unmatched)}"
    )
    cached = _files_cache_get_json(cache_key)
    if cached is not None:
        return jsonify(cached)

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"genres": [], "labels": [], "years": [], "error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH genre_tokens AS (
                    -- Primary source: tags_json array (multi-genre support)
                    SELECT
                        alb.id AS album_id,
                        TRIM(g.value) AS genre_disp
                    FROM files_albums alb
                    CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                    WHERE COALESCE(TRIM(g.value), '') <> ''
                      AND """ + album_match_sql + """
                    UNION ALL
                    -- Fallback: legacy single-genre column when tags_json is empty
                    SELECT
                        alb.id AS album_id,
                        TRIM(alb.genre) AS genre_disp
                    FROM files_albums alb
                    WHERE COALESCE(TRIM(alb.genre), '') <> ''
                      AND """ + album_match_sql + """
                      AND COALESCE(alb.tags_json, '[]') = '[]'
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
                SELECT b.genre_disp AS genre, c.c
                FROM counts c
                JOIN best_disp b ON b.genre_norm = c.genre_norm AND b.rn = 1
                ORDER BY c.c DESC, genre ASC
                LIMIT %s
                """,
                (int(limit_genres),),
            )
            genres = [{"value": (r[0] or "").strip(), "count": int(r[1] or 0)} for r in cur.fetchall() if str(r[0] or "").strip()]

            cur.execute(
                """
                WITH label_tokens AS (
                    SELECT TRIM(COALESCE(alb.label, '')) AS label_disp
                    FROM files_albums alb
                    WHERE COALESCE(TRIM(alb.label), '') <> ''
                      AND """ + album_match_sql + """
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
                (int(limit_labels),),
            )
            labels = [{"value": (r[0] or "").strip(), "count": int(r[1] or 0)} for r in cur.fetchall() if str(r[0] or "").strip()]

            cur.execute(
                """
                SELECT year, COUNT(*) AS c
                FROM files_albums alb
                WHERE year IS NOT NULL AND year > 0
                  AND """ + album_match_sql + """
                GROUP BY year
                ORDER BY year DESC
                LIMIT %s
                """,
                (int(limit_years),),
            )
            years = [{"value": int(r[0] or 0), "count": int(r[1] or 0)} for r in cur.fetchall() if int(r[0] or 0) > 0]

        payload = {"genres": genres, "labels": labels, "years": years}
        _files_cache_set_json(cache_key, payload, ttl=60)
        return jsonify(payload)
    finally:
        conn.close()

def api_library_genres_suggest():
    """Suggest genres with album counts (Files mode only). Used for advanced filtering UI."""
    if _get_library_mode() != "files":
        return jsonify({"query": "", "genres": [], "error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"query": "", "genres": [], "error": err or "Files index unavailable"}), 503

    query = (request.args.get("q") or "").strip()
    label = (request.args.get("label") or "").strip()
    year = _parse_int_loose(request.args.get("year"), 0)
    limit = max(1, min(80, _parse_int_loose(request.args.get("limit"), 16)))
    refresh = bool(_parse_bool(request.args.get("refresh")))
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    album_match_sql = (
        f"({_library_albums_match_where(include_unmatched, 'alb')})"
        f" AND ({_library_album_scope_where(scope, 'alb')})"
    )

    cache_key = (
        f"library:genres:suggest:{query.lower()}:{label.lower()}:{int(year or 0)}:{limit}:"
        f"{_library_cache_scope_suffix(scope)}:{_library_cache_unmatched_suffix(include_unmatched)}"
    )
    if not refresh:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"query": query, "genres": [], "error": "PostgreSQL unavailable"}), 503
    try:
        album_filters = ["1=1", album_match_sql]
        album_params: list = []
        if year and int(year) > 0:
            album_filters.append("COALESCE(alb.year, 0) = %s")
            album_params.append(int(year))
        if label:
            parts = [p.strip() for p in str(label).split(",") if p.strip()]
            if parts:
                album_filters.append("lower(trim(COALESCE(alb.label, ''))) = ANY(%s)")
                album_params.append([p.lower() for p in parts])

        like = f"%{query}%"
        with conn.cursor() as cur:
            if query:
                cur.execute(
                    f"""
                    WITH genre_tokens AS (
                        SELECT
                            alb.id AS album_id,
                            TRIM(g.value) AS genre_disp
                        FROM files_albums alb
                        CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                        WHERE COALESCE(TRIM(g.value), '') <> ''
                          AND {" AND ".join(album_filters)}
                        UNION ALL
                        SELECT
                            alb.id AS album_id,
                            TRIM(alb.genre) AS genre_disp
                        FROM files_albums alb
                        WHERE COALESCE(TRIM(alb.genre), '') <> ''
                          AND {" AND ".join(album_filters)}
                          AND COALESCE(alb.tags_json, '[]') = '[]'
                    ),
                    norm_tokens AS (
                        SELECT
                            album_id,
                            LOWER(genre_disp) AS genre_norm,
                            genre_disp
                        FROM genre_tokens
                        WHERE COALESCE(genre_disp, '') <> ''
                          AND genre_disp ILIKE %s
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
                    SELECT b.genre_disp AS genre, c.c
                    FROM counts c
                    JOIN best_disp b ON b.genre_norm = c.genre_norm AND b.rn = 1
                    ORDER BY c.c DESC, genre ASC
                    LIMIT %s
                    """,
                    (*album_params, like, int(limit)),
                )
            else:
                cur.execute(
                    f"""
                    WITH genre_tokens AS (
                        SELECT
                            alb.id AS album_id,
                            TRIM(g.value) AS genre_disp
                        FROM files_albums alb
                        CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                        WHERE COALESCE(TRIM(g.value), '') <> ''
                          AND {" AND ".join(album_filters)}
                        UNION ALL
                        SELECT
                            alb.id AS album_id,
                            TRIM(alb.genre) AS genre_disp
                        FROM files_albums alb
                        WHERE COALESCE(TRIM(alb.genre), '') <> ''
                          AND {" AND ".join(album_filters)}
                          AND COALESCE(alb.tags_json, '[]') = '[]'
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
                    SELECT b.genre_disp AS genre, c.c
                    FROM counts c
                    JOIN best_disp b ON b.genre_norm = c.genre_norm AND b.rn = 1
                    ORDER BY c.c DESC, genre ASC
                    LIMIT %s
                    """,
                    (*album_params, int(limit)),
                )
            rows = cur.fetchall()

        genres = [{"value": (r[0] or "").strip(), "count": int(r[1] or 0)} for r in rows if str(r[0] or "").strip()]
        payload = {"query": query, "genres": genres}
        _files_cache_set_json(cache_key, payload, ttl=30)
        return jsonify(payload)
    finally:
        conn.close()

def api_library_labels_suggest():
    """Suggest labels with album counts (Files mode only). Used for advanced filtering UI."""
    if _get_library_mode() != "files":
        return jsonify({"query": "", "labels": [], "error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"query": "", "labels": [], "error": err or "Files index unavailable"}), 503

    query = (request.args.get("q") or "").strip()
    genre = (request.args.get("genre") or "").strip()
    year = _parse_int_loose(request.args.get("year"), 0)
    limit = max(1, min(80, _parse_int_loose(request.args.get("limit"), 16)))
    refresh = bool(_parse_bool(request.args.get("refresh")))
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    album_match_sql = (
        f"({_library_albums_match_where(include_unmatched, 'alb')})"
        f" AND ({_library_album_scope_where(scope, 'alb')})"
    )

    cache_key = (
        f"library:labels:suggest:{query.lower()}:{genre.lower()}:{int(year or 0)}:{limit}:"
        f"{_library_cache_scope_suffix(scope)}:{_library_cache_unmatched_suffix(include_unmatched)}"
    )
    if not refresh:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"query": query, "labels": [], "error": "PostgreSQL unavailable"}), 503
    try:
        album_filters = ["1=1", album_match_sql]
        album_params: list = []
        if year and int(year) > 0:
            album_filters.append("COALESCE(alb.year, 0) = %s")
            album_params.append(int(year))
        if genre:
            parts = [p.strip() for p in str(genre).split(",") if p.strip()]
            if parts:
                norms = [p.lower() for p in parts]
                album_filters.append(
                    """
                    (
                        EXISTS (
                            SELECT 1
                            FROM jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                            WHERE lower(trim(g.value)) = ANY(%s)
                        )
                        OR (
                            COALESCE(alb.tags_json, '[]') = '[]'
                            AND lower(trim(COALESCE(alb.genre, ''))) = ANY(%s)
                        )
                    )
                    """
                )
                album_params.append(norms)
                album_params.append(norms)

        like = f"%{query}%"
        with conn.cursor() as cur:
            if query:
                cur.execute(
                    f"""
                    WITH label_tokens AS (
                        SELECT TRIM(COALESCE(alb.label, '')) AS label_disp
                        FROM files_albums alb
                        WHERE COALESCE(TRIM(alb.label), '') <> ''
                          AND {" AND ".join(album_filters)}
                          AND TRIM(COALESCE(alb.label, '')) ILIKE %s
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
                    (*album_params, like, int(limit)),
                )
            else:
                cur.execute(
                    f"""
                    WITH label_tokens AS (
                        SELECT TRIM(COALESCE(alb.label, '')) AS label_disp
                        FROM files_albums alb
                        WHERE COALESCE(TRIM(alb.label), '') <> ''
                          AND {" AND ".join(album_filters)}
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
                    (*album_params, int(limit)),
                )
            rows = cur.fetchall()

        labels = [{"value": (r[0] or "").strip(), "count": int(r[1] or 0)} for r in rows if str(r[0] or "").strip()]
        payload = {"query": query, "labels": labels}
        _files_cache_set_json(cache_key, payload, ttl=30)
        return jsonify(payload)
    finally:
        conn.close()

def api_library_genres():
    """List genres with album counts (Files mode only). Supports search + pagination and optional label/year filtering."""
    if _get_library_mode() != "files":
        return jsonify({"genres": [], "total": 0, "limit": 0, "offset": 0, "error": "Files mode required"}), 400

    search = (request.args.get("search") or request.args.get("q") or "").strip()
    label = (request.args.get("label") or "").strip()
    year = _parse_int_loose(request.args.get("year"), 0)
    limit = max(1, min(200, _parse_int_loose(request.args.get("limit"), 80)))
    offset = max(0, _parse_int_loose(request.args.get("offset"), 0))
    refresh = bool(_parse_bool(request.args.get("refresh")))
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    album_match_sql = (
        f"({_library_albums_match_where(include_unmatched, 'alb')})"
        f" AND ({_library_album_scope_where(scope, 'alb')})"
    )

    cache_key = (
        f"library:genres:list:{search.lower()}:{label.lower()}:{int(year or 0)}:{limit}:{offset}:"
        f"{_library_cache_scope_suffix(scope)}:{_library_cache_unmatched_suffix(include_unmatched)}"
    )
    if not refresh:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)
    ok, err = _ensure_files_index_ready()
    if not ok:
        payload = _files_library_published_genres(
            include_unmatched=include_unmatched,
            scope=scope,
            search=search,
            label=label,
            year=int(year or 0),
            limit=int(limit),
            offset=int(offset),
        )
        payload["error"] = err or "Files index unavailable"
        return jsonify(payload)
    with lock:
        scan_busy = bool(state.get("scanning") or state.get("scan_starting") or state.get("scan_finalizing") or state.get("scan_post_processing"))
    if files_index_lock.locked():
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            payload = dict(cached)
            payload["stale"] = True
            return jsonify(payload)
        if scan_busy:
            payload = _files_library_published_genres(
                include_unmatched=include_unmatched,
                scope=scope,
                search=search,
                label=label,
                year=int(year or 0),
                limit=int(limit),
                offset=int(offset),
            )
            _files_cache_set_json(cache_key, payload, ttl=20)
            return jsonify(payload)

    album_filters = ["1=1", album_match_sql]
    album_params: list = []
    if year and int(year) > 0:
        album_filters.append("COALESCE(alb.year, 0) = %s")
        album_params.append(int(year))
    if label:
        parts = [p.strip() for p in str(label).split(",") if p.strip()]
        if parts:
            album_filters.append("lower(trim(COALESCE(alb.label, ''))) = ANY(%s)")
            album_params.append([p.lower() for p in parts])

    conn = _files_pg_connect()
    if conn is None:
        payload = _files_library_published_genres(
            include_unmatched=include_unmatched,
            scope=scope,
            search=search,
            label=label,
            year=int(year or 0),
            limit=int(limit),
            offset=int(offset),
        )
        payload["error"] = "PostgreSQL unavailable"
        return jsonify(payload)
    try:
        like = f"%{search}%"
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH genre_tokens AS (
                    SELECT
                        alb.id AS album_id,
                        BTRIM(split.value, ' []{{}}()''"') AS genre_disp
                    FROM files_albums alb
                    CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                    CROSS JOIN LATERAL regexp_split_to_table(TRIM(g.value), E'\\s*[;,/|]\\s*|\\s*,\\s*') AS split(value)
                    WHERE COALESCE(TRIM(split.value), '') <> ''
                      AND {" AND ".join(album_filters)}
                    UNION ALL
                    SELECT
                        alb.id AS album_id,
                        BTRIM(split.value, ' []{{}}()''"') AS genre_disp
                    FROM files_albums alb
                    CROSS JOIN LATERAL regexp_split_to_table(TRIM(alb.genre), E'\\s*[;,/|]\\s*|\\s*,\\s*') AS split(value)
                    WHERE COALESCE(TRIM(alb.genre), '') <> ''
                      AND COALESCE(TRIM(split.value), '') <> ''
                      AND {" AND ".join(album_filters)}
                      AND COALESCE(alb.tags_json, '[]') = '[]'
                ),
                norm_tokens AS (
                    SELECT
                        album_id,
                        LOWER(genre_disp) AS genre_norm,
                        genre_disp
                    FROM genre_tokens
                    WHERE COALESCE(genre_disp, '') <> ''
                      AND (%s = '' OR genre_disp ILIKE %s)
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
                SELECT COUNT(*) FROM counts
                """,
                (*album_params, search, like),
            )
            total = int((cur.fetchone() or [0])[0] or 0)

            cur.execute(
                f"""
                WITH genre_tokens AS (
                    SELECT
                        alb.id AS album_id,
                        BTRIM(split.value, ' []{{}}()''"') AS genre_disp
                    FROM files_albums alb
                    CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                    CROSS JOIN LATERAL regexp_split_to_table(TRIM(g.value), E'\\s*[;,/|]\\s*|\\s*,\\s*') AS split(value)
                    WHERE COALESCE(TRIM(split.value), '') <> ''
                      AND {" AND ".join(album_filters)}
                    UNION ALL
                    SELECT
                        alb.id AS album_id,
                        BTRIM(split.value, ' []{{}}()''"') AS genre_disp
                    FROM files_albums alb
                    CROSS JOIN LATERAL regexp_split_to_table(TRIM(alb.genre), E'\\s*[;,/|]\\s*|\\s*,\\s*') AS split(value)
                    WHERE COALESCE(TRIM(alb.genre), '') <> ''
                      AND COALESCE(TRIM(split.value), '') <> ''
                      AND {" AND ".join(album_filters)}
                      AND COALESCE(alb.tags_json, '[]') = '[]'
                ),
                norm_tokens AS (
                    SELECT
                        album_id,
                        LOWER(genre_disp) AS genre_norm,
                        genre_disp
                    FROM genre_tokens
                    WHERE COALESCE(genre_disp, '') <> ''
                      AND (%s = '' OR genre_disp ILIKE %s)
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
                SELECT b.genre_disp AS genre, c.c
                FROM counts c
                JOIN best_disp b ON b.genre_norm = c.genre_norm AND b.rn = 1
                ORDER BY c.c DESC, genre ASC
                LIMIT %s OFFSET %s
                """,
                (*album_params, search, like, int(limit), int(offset)),
            )
            rows = cur.fetchall()

        genres = [{"value": (r[0] or "").strip(), "count": int(r[1] or 0)} for r in rows if str(r[0] or "").strip()]
        payload = {"genres": genres, "total": total, "limit": int(limit), "offset": int(offset)}
        _files_cache_set_json(cache_key, payload, ttl=45)
        return jsonify(payload)
    finally:
        conn.close()

def api_library_labels():
    """List labels with album counts (Files mode only). Supports search + pagination and optional genre/year filtering."""
    if _get_library_mode() != "files":
        return jsonify({"labels": [], "total": 0, "limit": 0, "offset": 0, "error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"labels": [], "total": 0, "limit": 0, "offset": 0, "error": err or "Files index unavailable"}), 503

    search = (request.args.get("search") or request.args.get("q") or "").strip()
    genre = (request.args.get("genre") or "").strip()
    year = _parse_int_loose(request.args.get("year"), 0)
    limit = max(1, min(200, _parse_int_loose(request.args.get("limit"), 80)))
    offset = max(0, _parse_int_loose(request.args.get("offset"), 0))
    refresh = bool(_parse_bool(request.args.get("refresh")))
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    album_match_sql = (
        f"({_library_albums_match_where(include_unmatched, 'alb')})"
        f" AND ({_library_album_scope_where(scope, 'alb')})"
    )

    cache_key = (
        f"library:labels:list:{search.lower()}:{genre.lower()}:{int(year or 0)}:{limit}:{offset}:"
        f"{_library_cache_scope_suffix(scope)}:{_library_cache_unmatched_suffix(include_unmatched)}"
    )
    if not refresh:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)
    with lock:
        scan_busy = bool(state.get("scanning") or state.get("scan_starting") or state.get("scan_finalizing") or state.get("scan_post_processing"))
    if scan_busy and not refresh:
        payload = _files_library_published_labels(
            include_unmatched=include_unmatched,
            scope=scope,
            search=search,
            genre=genre,
            year=year,
            limit=limit,
            offset=offset,
        )
        payload["stale"] = True
        payload["fallback_source"] = "published"
        return jsonify(payload)
    if files_index_lock.locked():
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            payload = dict(cached)
            payload["stale"] = True
            return jsonify(payload)
        if scan_busy:
            payload = _files_library_published_labels(
                include_unmatched=include_unmatched,
                scope=scope,
                search=search,
                genre=genre,
                year=year,
                limit=limit,
                offset=offset,
            )
            return jsonify(payload)

    album_filters = ["1=1", album_match_sql]
    album_params: list = []
    if year and int(year) > 0:
        album_filters.append("COALESCE(alb.year, 0) = %s")
        album_params.append(int(year))
    if genre:
        parts = [p.strip() for p in str(genre).split(",") if p.strip()]
        if parts:
            norms = [p.lower() for p in parts]
            album_filters.append(
                """
                (
                    EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                        WHERE lower(trim(g.value)) = ANY(%s)
                    )
                    OR (
                        COALESCE(alb.tags_json, '[]') = '[]'
                        AND lower(trim(COALESCE(alb.genre, ''))) = ANY(%s)
                    )
                )
                """
            )
            album_params.append(norms)
            album_params.append(norms)

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"labels": [], "total": 0, "limit": limit, "offset": offset, "error": "PostgreSQL unavailable"}), 503
    try:
        like = f"%{search}%"
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH label_tokens AS (
                    SELECT TRIM(COALESCE(alb.label, '')) AS label_disp
                    FROM files_albums alb
                    WHERE COALESCE(TRIM(alb.label), '') <> ''
                      AND {" AND ".join(album_filters)}
                      AND (%s = '' OR TRIM(COALESCE(alb.label, '')) ILIKE %s)
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
                )
                SELECT COUNT(*) FROM counts
                """,
                (*album_params, search, like),
            )
            total = int((cur.fetchone() or [0])[0] or 0)

            cur.execute(
                f"""
                WITH album_rows AS (
                    SELECT
                        alb.id AS album_id,
                        TRIM(COALESCE(alb.label, '')) AS label_disp,
                        LOWER(TRIM(COALESCE(alb.label, ''))) AS label_norm,
                        COALESCE(alb.has_cover, FALSE) AS has_cover,
                        COALESCE(alb.updated_at, alb.created_at, NOW()) AS recency
                    FROM files_albums alb
                    WHERE COALESCE(TRIM(alb.label), '') <> ''
                      AND {" AND ".join(album_filters)}
                      AND (%s = '' OR TRIM(COALESCE(alb.label, '')) ILIKE %s)
                ),
                counts AS (
                    SELECT label_norm, COUNT(*) AS c
                    FROM album_rows
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
                    FROM album_rows
                    GROUP BY label_norm, label_disp
                ),
                cover_pick AS (
                    SELECT
                        label_norm,
                        album_id,
                        has_cover,
                        ROW_NUMBER() OVER (
                            PARTITION BY label_norm
                            ORDER BY has_cover DESC, recency DESC, album_id DESC
                        ) AS rn
                    FROM album_rows
                )
                SELECT b.label_disp AS label, c.c, cp.album_id, cp.has_cover
                FROM counts c
                JOIN best_disp b ON b.label_norm = c.label_norm AND b.rn = 1
                LEFT JOIN cover_pick cp ON cp.label_norm = c.label_norm AND cp.rn = 1
                ORDER BY c.c DESC, label ASC
                LIMIT %s OFFSET %s
                """,
                (*album_params, search, like, int(limit), int(offset)),
            )
            rows = cur.fetchall()

        base_url = request.url_root.rstrip("/")
        label_norms = [_norm_label_key(str((r[0] or "")).strip()) for r in rows if str((r[0] or "")).strip()]
        ext_logo_map = _files_get_external_label_images(conn, [n for n in label_norms if n]) if label_norms else {}
        labels = []
        logos_to_warm: list[tuple[str, str, str, str]] = []
        for r in rows:
            label_value = str((r[0] or "")).strip()
            if not label_value:
                continue
            count = int(r[1] or 0)
            album_id = int(r[2] or 0)
            has_cover = bool(r[3]) if len(r) > 3 else False
            label_norm = _norm_label_key(label_value)
            cached_logo = ext_logo_map.get(label_norm) or {}
            has_logo = bool(str(cached_logo.get("image_path") or "").strip())
            thumb = _label_logo_asset_url(base_url, label_value, size=224) if has_logo else None
            if not thumb and album_id > 0 and has_cover:
                thumb = f"{base_url}/api/library/files/album/{album_id}/cover?size=128"
            labels.append({"value": label_value, "count": count, "thumb": thumb})
            if has_logo or len(logos_to_warm) >= 10 or not label_norm:
                continue
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            COALESCE(alb.bandcamp_album_url, ''),
                            COALESCE(alb.title, ''),
                            COALESCE(ar.name, '')
                        FROM files_albums alb
                        JOIN files_artist_album_links link ON link.album_id = alb.id
                        JOIN files_artists ar ON ar.id = link.artist_id
                        WHERE LOWER(TRIM(COALESCE(alb.label, ''))) = LOWER(TRIM(%s))
                          AND COALESCE(alb.bandcamp_album_url, '') <> ''
                        ORDER BY
                            CASE WHEN COALESCE(alb.strict_match_provider, '') = 'bandcamp' THEN 0 ELSE 1 END,
                            alb.updated_at DESC,
                            alb.id DESC
                        LIMIT 1
                        """,
                        (label_value,),
                    )
                    bandcamp_row = cur.fetchone()
                if bandcamp_row:
                    logos_to_warm.append(
                        (
                            label_value,
                            str(bandcamp_row[2] or "").strip(),
                            str(bandcamp_row[1] or "").strip(),
                            str(bandcamp_row[0] or "").strip(),
                        )
                    )
            except Exception:
                continue

        for label_name, artist_name, album_title, bandcamp_url in logos_to_warm:
            if not artist_name or not album_title or not bandcamp_url:
                continue
            try:
                with conn.transaction():
                    _files_prewarm_label_logo_from_bandcamp(
                        conn,
                        label_name=label_name,
                        artist_name=artist_name,
                        album_title=album_title,
                        bandcamp_album_url=bandcamp_url,
                    )
            except Exception:
                continue
        payload = {"labels": labels, "total": total, "limit": int(limit), "offset": int(offset)}
        _files_cache_set_json(cache_key, payload, ttl=45)
        return jsonify(payload)
    finally:
        conn.close()

def api_library_recently_played_albums():
    """Return recently played albums based on listening telemetry (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"days": 0, "limit": 0, "generated_at": int(time.time()), "albums": [], "error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"days": 0, "limit": 0, "generated_at": int(time.time()), "albums": [], "error": err or "Files index unavailable"}), 503

    days = max(7, min(365, _parse_int_loose(request.args.get("days"), 90)))
    limit = max(1, min(200, _parse_int_loose(request.args.get("limit"), 18)))
    offset = max(0, _parse_int_loose(request.args.get("offset"), 0))
    refresh = bool(_parse_bool(request.args.get("refresh")))
    user_id = _current_user_id_or_zero()
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    album_match_sql = (
        f"({_library_albums_match_where(include_unmatched, 'alb')})"
        f" AND ({_library_album_scope_where(scope, 'alb')})"
    )

    cache_key = (
        f"library:recently_played_albums:{user_id}:{days}:{limit}:{offset}:"
        f"{_library_cache_scope_suffix(scope)}:{_library_cache_unmatched_suffix(include_unmatched)}"
    )
    if not refresh:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)

    scan_busy = _files_scan_busy()
    conn = _files_pg_connect(acquire_timeout_sec=0.75)
    if conn is None:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            payload = dict(cached)
            payload["stale"] = True
            return jsonify(payload)
        status_code = 503 if not scan_busy else 200
        return jsonify({"days": days, "limit": limit, "generated_at": int(time.time()), "albums": [], "source": "playback", "stale": True, "error": None if scan_busy else "PostgreSQL unavailable"}), status_code
    try:
        base_url = request.url_root.rstrip("/")
        total = 0
        with conn.cursor() as cur:
            # Prefer explicit playback telemetry; fall back to reco telemetry when empty.
            with _files_pg_statement_timeout(cur, 2200):
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM files_playback_events
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s || ' days')::interval
                      AND played_seconds >= 12
                    """,
                    (int(user_id), int(days)),
                )
                playback_count = int((cur.fetchone() or [0])[0] or 0)
            use_reco = playback_count <= 0
            ev_table = "files_reco_events" if use_reco else "files_playback_events"
            ev_user_filter = "1=1" if use_reco else "e.user_id = %s"
            ev_played_filter = "TRUE" if use_reco else "COALESCE(e.played_seconds, 0) >= 12"
            count_params = [int(days)]
            rows_params = [int(days), int(limit), int(offset)]
            if not use_reco:
                count_params.insert(0, int(user_id))
                rows_params.insert(0, int(user_id))

            with _files_pg_statement_timeout(cur, 2500):
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM (
                        SELECT t.album_id
                        FROM {ev_table} e
                        JOIN files_tracks t ON t.id = e.track_id
                        JOIN files_albums alb ON alb.id = t.album_id
                        WHERE {ev_user_filter}
                          AND {album_match_sql}
                          AND e.created_at >= NOW() - (%s || ' days')::interval
                          AND {ev_played_filter}
                        GROUP BY t.album_id
                    ) AS ranked
                    """,
                    tuple(count_params),
                )
                total = int((cur.fetchone() or [0])[0] or 0)

                cur.execute(
                    f"""
                    SELECT
                        t.album_id,
                        MAX(e.created_at) AS last_played_at
                    FROM {ev_table} e
                    JOIN files_tracks t ON t.id = e.track_id
                    JOIN files_albums alb ON alb.id = t.album_id
                    WHERE {ev_user_filter}
                      AND {album_match_sql}
                      AND e.created_at >= NOW() - (%s || ' days')::interval
                      AND {ev_played_filter}
                    GROUP BY t.album_id
                    ORDER BY last_played_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(rows_params),
                )
                rows = cur.fetchall()

            album_ids: list[int] = []
            last_played: dict[int, int] = {}
            for album_id, ts in rows:
                try:
                    aid = int(album_id or 0)
                except Exception:
                    aid = 0
                if aid <= 0:
                    continue
                album_ids.append(aid)
                last_played[aid] = int(_dt_to_epoch(ts)) if ts else 0

            albums_out: list[dict] = []
            if album_ids:
                cur.execute(
                    """
                    WITH ids AS (
                        SELECT album_id, ord
                        FROM unnest(%s::bigint[]) WITH ORDINALITY AS u(album_id, ord)
                    )
                    SELECT
                        alb.id,
                        alb.title,
                        COALESCE(alb.year, 0) AS year,
                        COALESCE(alb.genre, '') AS genre,
                        COALESCE(alb.label, '') AS label,
                        COALESCE(alb.tags_json, '[]') AS tags_json,
                        alb.track_count,
                        COALESCE(alb.format, '') AS format,
                        alb.is_lossless,
                        alb.has_cover,
                        alb.mb_identified,
                        ar.id AS artist_id,
                        ar.name AS artist_name,
                        COALESCE(pr.short_description, '') AS short_description,
                        COALESCE(pr.source, '') AS profile_source,
                        ids.ord
                    FROM ids
                    JOIN files_albums alb ON alb.id = ids.album_id
                    JOIN files_artists ar ON ar.id = alb.artist_id
                    LEFT JOIN files_album_profiles pr
                           ON pr.artist_norm = ar.name_norm
                          AND pr.title_norm = alb.title_norm
                    WHERE """ + album_match_sql + """
                    ORDER BY ids.ord ASC
                    """,
                    (album_ids,),
                )
                alb_rows = cur.fetchall()
                for album_id, title, year, genre, label, tags_json, track_count, fmt, is_lossless, has_cover, mb_identified, artist_id, artist_name, short_desc, profile_source, _ord in alb_rows:
                    aid = int(album_id or 0)
                    arid = int(artist_id or 0)
                    thumb = f"{base_url}/api/library/files/album/{aid}/cover?size=512" if bool(has_cover) else None
                    short_desc_clean = (short_desc or "").strip()
                    genres_list: list[str] = []
                    try:
                        tags_list = json.loads(tags_json) if tags_json else []
                        if isinstance(tags_list, list):
                            for t in tags_list:
                                v = str(t or "").strip()
                                if v:
                                    genres_list.append(v)
                    except Exception:
                        genres_list = []
                    if not genres_list:
                        try:
                            genres_list = _split_genre_values(genre or "")
                        except Exception:
                            genres_list = []
                    if genres_list:
                        seen = set()
                        deduped = []
                        for g in genres_list:
                            gg = re.sub(r"\\s+", " ", (g or "").strip())
                            if not gg:
                                continue
                            key = gg.lower()
                            if key in seen:
                                continue
                            seen.add(key)
                            deduped.append(gg)
                        genres_list = deduped[:20]
                    albums_out.append(
                        {
                            "album_id": aid,
                            "title": title or "",
                            "year": int(year or 0) or None,
                            "genre": (genre or "").strip() or None,
                            "genres": genres_list,
                            "label": (label or "").strip() or None,
                            "track_count": int(track_count or 0),
                            "format": (fmt or "").strip() or None,
                            "is_lossless": bool(is_lossless),
                            "mb_identified": bool(mb_identified),
                            "thumb": thumb,
                            "artist_id": arid,
                            "artist_name": artist_name or "",
                            "short_description": short_desc_clean or None,
                            "profile_source": (profile_source or "").strip() or None,
                            "last_played_at": int(last_played.get(aid) or 0),
                        }
                    )

        payload = {
            "days": int(days),
            "total": int(total),
            "limit": int(limit),
            "offset": int(offset),
            "generated_at": int(time.time()),
            "source": "reco" if bool(use_reco) else "playback",
            "albums": albums_out,
        }
        _files_cache_set_json(cache_key, payload, ttl=30)
        return jsonify(payload)
    except Exception as e:
        logging.exception("recently_played_albums failed: %s", e)
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            payload = dict(cached)
            payload["stale"] = True
            return jsonify(payload)
        return jsonify({
            "days": int(days),
            "total": 0,
            "limit": int(limit),
            "offset": int(offset),
            "generated_at": int(time.time()),
            "source": "playback",
            "albums": [],
            "stale": True,
            "error": None if (scan_busy or _files_pg_is_statement_timeout_error(e)) else "Recently played temporarily unavailable",
        })
    finally:
        conn.close()

def api_library_liked_summary():
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    uid = _current_user_id_or_zero()
    if uid <= 0:
        return jsonify({"error": "Authentication required"}), 401
    target_uid, target_user, scope_err = _auth_resolve_public_user_scope(
        request.args.get("user_id"),
        current_user_id=uid,
        visibility_key="share_liked_public",
    )
    if scope_err:
        return jsonify({"error": scope_err[0]}), int(scope_err[1])
    base_url = request.url_root.rstrip("/")
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        payload: dict[str, Any] = {
            "owner": target_user or _auth_user_snapshot(target_uid),
            "tracks": [],
            "albums": [],
            "artists": [],
            "labels": [],
            "recommended_albums": [],
        }
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    tr.id,
                    tr.title,
                    ar.id,
                    ar.name,
                    alb.id,
                    alb.title,
                    tr.duration_sec,
                    tr.track_num,
                    tr.disc_num,
                    alb.has_cover,
                    EXTRACT(EPOCH FROM l.updated_at)::BIGINT
                FROM files_user_entity_likes l
                JOIN files_tracks tr ON tr.id = l.entity_id
                JOIN files_albums alb ON alb.id = tr.album_id
                JOIN files_artists ar ON ar.id = alb.artist_id
                WHERE l.user_id = %s AND l.entity_type = 'track' AND l.liked = TRUE
                ORDER BY l.updated_at DESC, tr.id DESC
                LIMIT 96
                """,
                (int(target_uid),),
            )
            for row in cur.fetchall():
                track_id = int(row[0] or 0)
                album_id = int(row[4] or 0)
                payload["tracks"].append(
                    {
                        "track_id": track_id,
                        "title": str(row[1] or "").strip(),
                        "artist_id": int(row[2] or 0),
                        "artist_name": str(row[3] or "").strip(),
                        "album_id": album_id,
                        "album_title": str(row[5] or "").strip(),
                        "duration_sec": int(row[6] or 0),
                        "track_num": int(row[7] or 0),
                        "disc_num": int(row[8] or 0),
                        "thumb": f"{base_url}/api/library/files/album/{album_id}/cover?size=320" if bool(row[9]) and album_id > 0 else None,
                        "updated_at": int(row[10] or 0),
                    }
                )
            cur.execute(
                """
                SELECT alb.id, alb.title, ar.id, ar.name, alb.year, alb.has_cover,
                       COALESCE(alb.label, ''), COALESCE(alb.genre, ''), COALESCE(alb.tags_json, '[]'),
                       alb.track_count, COALESCE(alb.format, ''), alb.is_lossless,
                       ur.rating, pr.public_rating, COALESCE(pr.public_rating_votes, 0), pr.heat_score
                FROM files_user_entity_likes l
                JOIN files_albums alb ON alb.id = l.entity_id
                JOIN files_artists ar ON ar.id = alb.artist_id
                LEFT JOIN files_user_album_ratings ur ON ur.user_id = %s AND ur.album_id = alb.id
                LEFT JOIN files_album_profiles pr
                  ON pr.artist_norm = ar.name_norm
                 AND pr.title_norm = alb.title_norm
                WHERE l.user_id = %s AND l.entity_type = 'album' AND l.liked = TRUE
                ORDER BY l.updated_at DESC, alb.id DESC
                LIMIT 48
                """,
                (int(target_uid), int(target_uid)),
            )
            for row in cur.fetchall():
                aid = int(row[0] or 0)
                payload["albums"].append(
                    {
                        "album_id": aid,
                        "title": str(row[1] or "").strip(),
                        "artist_id": int(row[2] or 0),
                        "artist_name": str(row[3] or "").strip(),
                        "year": int(row[4] or 0) or None,
                        "thumb": f"{base_url}/api/library/files/album/{aid}/cover?size=512" if bool(row[5]) else None,
                        "label": str(row[6] or "").strip() or None,
                        "genre": str(row[7] or "").strip() or None,
                        "genres": json.loads(str(row[8] or "[]") or "[]") if str(row[8] or "").strip() else [],
                        "track_count": int(row[9] or 0),
                        "format": str(row[10] or "").strip() or None,
                        "is_lossless": bool(row[11]),
                        "user_rating": float(row[12]) if row[12] is not None else None,
                        "public_rating": float(row[13]) if row[13] is not None else None,
                        "public_rating_votes": int(row[14] or 0) if row[14] is not None else None,
                        "heat_score": float(row[15]) if row[15] is not None else None,
                    }
                )
            cur.execute(
                """
                SELECT a.id, a.name, a.album_count, a.has_image
                FROM files_user_entity_likes l
                JOIN files_artists a ON a.id = l.entity_id
                WHERE l.user_id = %s AND l.entity_type = 'artist' AND l.liked = TRUE
                ORDER BY l.updated_at DESC, a.id DESC
                LIMIT 48
                """,
                (int(target_uid),),
            )
            for row in cur.fetchall():
                aid = int(row[0] or 0)
                payload["artists"].append(
                    {
                        "artist_id": aid,
                        "artist_name": str(row[1] or "").strip(),
                        "album_count": int(row[2] or 0),
                        "thumb": _artist_image_asset_url(base_url, aid, size=320) or None,
                    }
                )
            cur.execute(
                """
                SELECT entity_key, EXTRACT(EPOCH FROM updated_at)::BIGINT
                FROM files_user_entity_likes
                WHERE user_id = %s AND entity_type = 'label' AND liked = TRUE AND COALESCE(entity_key, '') <> ''
                ORDER BY updated_at DESC, entity_key ASC
                LIMIT 48
                """,
                (int(target_uid),),
            )
            for row in cur.fetchall():
                payload["labels"].append(
                    {
                        "label": str(row[0] or "").strip(),
                        "updated_at": int(row[1] or 0),
                    }
                )
            cur.execute(
                """
                WITH liked_artists AS (
                    SELECT entity_id
                    FROM files_user_entity_likes
                    WHERE user_id = %s AND entity_type = 'artist' AND liked = TRUE
                    UNION
                    SELECT DISTINCT alb.artist_id
                    FROM files_user_entity_likes ltr
                    JOIN files_tracks tr ON tr.id = ltr.entity_id
                    JOIN files_albums alb ON alb.id = tr.album_id
                    WHERE ltr.user_id = %s AND ltr.entity_type = 'track' AND ltr.liked = TRUE
                ),
                liked_albums AS (
                    SELECT entity_id
                    FROM files_user_entity_likes
                    WHERE user_id = %s AND entity_type = 'album' AND liked = TRUE
                    UNION
                    SELECT DISTINCT tr.album_id
                    FROM files_user_entity_likes ltr
                    JOIN files_tracks tr ON tr.id = ltr.entity_id
                    WHERE ltr.user_id = %s AND ltr.entity_type = 'track' AND ltr.liked = TRUE
                )
                SELECT alb.id, alb.title, ar.id, ar.name, alb.year, alb.has_cover,
                       COALESCE(alb.label, ''), COALESCE(alb.genre, ''), COALESCE(alb.tags_json, '[]'),
                       alb.track_count, COALESCE(alb.format, ''), alb.is_lossless,
                       ur.rating, pr.public_rating, COALESCE(pr.public_rating_votes, 0), pr.heat_score
                FROM files_albums alb
                JOIN files_artists ar ON ar.id = alb.artist_id
                LEFT JOIN files_user_album_ratings ur ON ur.user_id = %s AND ur.album_id = alb.id
                LEFT JOIN files_album_profiles pr
                  ON pr.artist_norm = ar.name_norm
                 AND pr.title_norm = alb.title_norm
                WHERE (
                    ar.id IN (SELECT entity_id FROM liked_artists)
                    OR lower(trim(COALESCE(alb.label, ''))) IN (
                        SELECT lower(trim(entity_key))
                        FROM files_user_entity_likes
                        WHERE user_id = %s AND entity_type = 'label' AND liked = TRUE
                    )
                )
                  AND alb.id NOT IN (SELECT entity_id FROM liked_albums)
                ORDER BY COALESCE(ur.rating, 0) DESC,
                         COALESCE(pr.public_rating, 0) DESC,
                         COALESCE(pr.heat_score, 0) DESC,
                         alb.updated_at DESC
                LIMIT 24
                """,
                (int(target_uid), int(target_uid), int(target_uid), int(target_uid), int(target_uid), int(target_uid)),
            )
            seen_album_ids: set[int] = set()
            for row in cur.fetchall():
                aid = int(row[0] or 0)
                if aid <= 0 or aid in seen_album_ids:
                    continue
                seen_album_ids.add(aid)
                payload["recommended_albums"].append(
                    {
                        "album_id": aid,
                        "title": str(row[1] or "").strip(),
                        "artist_id": int(row[2] or 0),
                        "artist_name": str(row[3] or "").strip(),
                        "year": int(row[4] or 0) or None,
                        "thumb": f"{base_url}/api/library/files/album/{aid}/cover?size=512" if bool(row[5]) else None,
                        "label": str(row[6] or "").strip() or None,
                        "genre": str(row[7] or "").strip() or None,
                        "genres": json.loads(str(row[8] or "[]") or "[]") if str(row[8] or "").strip() else [],
                        "track_count": int(row[9] or 0),
                        "format": str(row[10] or "").strip() or None,
                        "is_lossless": bool(row[11]),
                        "user_rating": float(row[12]) if row[12] is not None else None,
                        "public_rating": float(row[13]) if row[13] is not None else None,
                        "public_rating_votes": int(row[14] or 0) if row[14] is not None else None,
                        "heat_score": float(row[15]) if row[15] is not None else None,
                    }
                )
        return jsonify(payload)
    finally:
        conn.close()

def api_library_playback_stats():
    """Return listening statistics for charts (Files mode)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    days = max(1, min(365, _parse_int_loose(request.args.get("days"), 30)))
    user_id = _current_user_id_or_zero()
    if user_id <= 0:
        return jsonify({"error": "Authentication required"}), 401

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH filtered AS (
                    SELECT track_id, event_type, played_seconds, created_at
                    FROM files_playback_events
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s || ' days')::interval
                )
                SELECT
                    COALESCE(SUM(played_seconds), 0) AS total_seconds,
                    COUNT(*) AS events,
                    COUNT(DISTINCT track_id) AS distinct_tracks
                FROM filtered
                """,
                (int(user_id), int(days)),
            )
            row = cur.fetchone() or [0, 0, 0]
            total_seconds = int(row[0] or 0)
            events = int(row[1] or 0)
            distinct_tracks = int(row[2] or 0)

            cur.execute(
                """
                WITH filtered AS (
                    SELECT track_id, event_type, played_seconds, created_at
                    FROM files_playback_events
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s || ' days')::interval
                )
                SELECT
                    ar.id AS artist_id,
                    ar.name AS artist_name,
                    COALESCE(SUM(f.played_seconds), 0) AS seconds,
                    COUNT(*) AS plays
                FROM filtered f
                JOIN files_tracks tr ON tr.id = f.track_id
                JOIN files_albums alb ON alb.id = tr.album_id
                JOIN files_artists ar ON ar.id = alb.artist_id
                GROUP BY ar.id, ar.name
                ORDER BY seconds DESC, plays DESC, artist_name ASC
                LIMIT 10
                """
            , (int(user_id), int(days)))
            top_artists = [
                {"artist_id": int(r[0] or 0), "artist_name": r[1] or "", "seconds": int(r[2] or 0), "plays": int(r[3] or 0)}
                for r in cur.fetchall()
                if int(r[0] or 0) > 0
            ]

            cur.execute(
                """
                WITH filtered AS (
                    SELECT track_id, event_type, played_seconds, created_at
                    FROM files_playback_events
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s || ' days')::interval
                )
                SELECT
                    tr.id AS track_id,
                    tr.title AS track_title,
                    ar.id AS artist_id,
                    ar.name AS artist_name,
                    alb.id AS album_id,
                    alb.title AS album_title,
                    COALESCE(SUM(f.played_seconds), 0) AS seconds,
                    COUNT(*) AS plays
                FROM filtered f
                JOIN files_tracks tr ON tr.id = f.track_id
                JOIN files_albums alb ON alb.id = tr.album_id
                JOIN files_artists ar ON ar.id = alb.artist_id
                GROUP BY tr.id, tr.title, ar.id, ar.name, alb.id, alb.title
                ORDER BY seconds DESC, plays DESC, track_title ASC
                LIMIT 10
                """
            , (int(user_id), int(days)))
            top_tracks = [
                {
                    "track_id": int(r[0] or 0),
                    "track_title": r[1] or "",
                    "artist_id": int(r[2] or 0),
                    "artist_name": r[3] or "",
                    "album_id": int(r[4] or 0),
                    "album_title": r[5] or "",
                    "seconds": int(r[6] or 0),
                    "plays": int(r[7] or 0),
                }
                for r in cur.fetchall()
                if int(r[0] or 0) > 0
            ]

            # Genres: split seconds across tags_json elements to avoid inflating totals.
            cur.execute(
                """
                WITH filtered AS (
                    SELECT track_id, played_seconds, created_at
                    FROM files_playback_events
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s || ' days')::interval
                ),
                joined AS (
                    SELECT
                        f.played_seconds,
                        COALESCE(NULLIF(alb.tags_json, ''), '[]')::jsonb AS tags
                    FROM filtered f
                    JOIN files_tracks tr ON tr.id = f.track_id
                    JOIN files_albums alb ON alb.id = tr.album_id
                ),
                expanded AS (
                    SELECT
                        lower(trim(g.value)) AS genre,
                        (joined.played_seconds::double precision / GREATEST(1, jsonb_array_length(joined.tags))) AS sec_share
                    FROM joined
                    JOIN LATERAL jsonb_array_elements_text(joined.tags) AS g(value) ON TRUE
                    WHERE COALESCE(trim(g.value), '') <> ''
                )
                SELECT genre, ROUND(SUM(sec_share))::BIGINT AS seconds
                FROM expanded
                GROUP BY genre
                ORDER BY seconds DESC, genre ASC
                LIMIT 10
                """
            , (int(user_id), int(days)))
            top_genres = [{"genre": str(r[0] or ""), "seconds": int(r[1] or 0)} for r in cur.fetchall() if str(r[0] or "").strip()]

            cur.execute(
                """
                WITH filtered AS (
                    SELECT track_id, event_type, played_seconds, created_at
                    FROM files_playback_events
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s || ' days')::interval
                )
                SELECT to_char(date_trunc('day', created_at), 'YYYY-MM-DD') AS day,
                       COALESCE(SUM(played_seconds), 0) AS seconds,
                       COUNT(*) AS plays
                FROM filtered
                GROUP BY day
                ORDER BY day ASC
                """
            , (int(user_id), int(days)))
            daily = [{"day": str(r[0] or ""), "seconds": int(r[1] or 0), "plays": int(r[2] or 0)} for r in cur.fetchall() if str(r[0] or "").strip()]

            cur.execute(
                """
                WITH filtered AS (
                    SELECT event_type, played_seconds, created_at
                    FROM files_playback_events
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s || ' days')::interval
                )
                SELECT event_type, COUNT(*) AS c
                FROM filtered
                GROUP BY event_type
                ORDER BY c DESC, event_type ASC
                """
            , (int(user_id), int(days)))
            event_types = [{"event_type": str(r[0] or ""), "count": int(r[1] or 0)} for r in cur.fetchall() if str(r[0] or "").strip()]

            cur.execute(
                """
                WITH filtered AS (
                    SELECT played_seconds, created_at
                    FROM files_playback_events
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s || ' days')::interval
                )
                SELECT EXTRACT(HOUR FROM created_at)::INT AS hour, COALESCE(SUM(played_seconds), 0) AS seconds
                FROM filtered
                GROUP BY hour
                ORDER BY hour ASC
                """
            , (int(user_id), int(days)))
            hours = [{"hour": int(r[0] or 0), "seconds": int(r[1] or 0)} for r in cur.fetchall()]

        return jsonify(
            {
                "days": int(days),
                "total_seconds": total_seconds,
                "events": events,
                "distinct_tracks": distinct_tracks,
                "top_artists": top_artists,
                "top_tracks": top_tracks,
                "top_genres": top_genres,
                "daily": daily,
                "event_types": event_types,
                "hours": hours,
            }
        )
    finally:
        conn.close()

def api_library_missing_tags():
    """Return albums with missing MusicBrainz or required tags."""
    if _get_library_mode() == "files":
        ok, err = _ensure_files_index_ready()
        if not ok:
            return jsonify({"albums": [], "error": err or "Files index unavailable"}), 503
        conn = _files_pg_connect()
        if conn is None:
            return jsonify({"albums": [], "error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT a.name, alb.id, alb.title, alb.missing_required_tags_json
                    FROM files_albums alb
                    JOIN files_artists a ON a.id = alb.artist_id
                    WHERE alb.missing_required_tags_json IS NOT NULL
                      AND alb.missing_required_tags_json <> ''
                      AND alb.missing_required_tags_json <> '[]'
                    ORDER BY a.name, alb.title
                    """
                )
                rows = cur.fetchall()
            out = []
            for artist_name, album_id, album_title, missing_json in rows:
                try:
                    missing_tags = json.loads(missing_json) if missing_json else []
                except (TypeError, ValueError):
                    missing_tags = []
                if not missing_tags:
                    continue
                out.append({
                    "artist_name": artist_name or "",
                    "album_id": int(album_id),
                    "album_title": album_title or "",
                    "missing_tags": missing_tags,
                })
            return jsonify({"albums": out})
        finally:
            conn.close()

    _reload_section_ids_from_db()
    if not PLEX_CONFIGURED:
        return jsonify({"albums": []})
    if not SECTION_IDS:
        return jsonify({"albums": []})

    scan_id = get_last_completed_scan_id()
    if scan_id:
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        cur.execute(
            """
            SELECT artist, album_id, title_raw, missing_required_tags
            FROM scan_editions
            WHERE scan_id = ? AND missing_required_tags IS NOT NULL AND missing_required_tags != '' AND missing_required_tags != '[]'
            ORDER BY artist, title_raw
            """,
            (scan_id,),
        )
        rows = cur.fetchall()
        con.close()
        if rows:
            results = []
            for artist_name, album_id, title_raw, missing_required_tags in rows:
                try:
                    missing_tags = json.loads(missing_required_tags) if isinstance(missing_required_tags, str) else (missing_required_tags or [])
                except (json.JSONDecodeError, TypeError):
                    missing_tags = []
                if missing_tags:
                    results.append({
                        "artist_name": artist_name or "",
                        "album_id": album_id,
                        "album_title": (title_raw or "").strip() or "",
                        "missing_tags": missing_tags,
                    })
            return jsonify({"albums": results})
        return jsonify({"albums": []})

    return jsonify({"albums": []})


_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}

def api_library_digest_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_digest(*args, **kwargs)

def api_library_top_artists_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_top_artists(*args, **kwargs)

def api_library_recent_artists_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_recent_artists(*args, **kwargs)

def api_library_facets_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_facets(*args, **kwargs)

def api_library_genres_suggest_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_genres_suggest(*args, **kwargs)

def api_library_labels_suggest_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_labels_suggest(*args, **kwargs)

def api_library_genres_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_genres(*args, **kwargs)

def api_library_labels_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_labels(*args, **kwargs)

def api_library_recently_played_albums_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_recently_played_albums(*args, **kwargs)

def api_library_liked_summary_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_liked_summary(*args, **kwargs)

def api_library_playback_stats_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_playback_stats(*args, **kwargs)

def api_library_missing_tags_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_missing_tags(*args, **kwargs)
