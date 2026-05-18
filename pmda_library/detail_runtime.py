"""Runtime-backed library detail and match handlers.

The public Flask routes still live in ``pmda_api``. This module holds the heavy
request implementations that used to live in ``pmda.py`` while the monolith is
reduced toward bootstrap/wiring only.
"""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one detail request."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "api_library_genre_profile",
        "api_library_label_profile",
        "api_library_artist_detail",
        "api_library_artist_summary_ai",
        "api_library_album_detail",
        "api_library_artist_similar",
        "api_library_artist_summary",
        "api_library_artist_concerts",
        "_bandsintown_block_state",
        "_bandsintown_mark_blocked",
        "_bandsintown_fetch_upcoming_events",
        "_concerts_fetch_upcoming_events",
        "_osm_geocode_place",
        "_songkick_search_artist",
        "_songkick_fetch_upcoming_events",
        "api_library_album_match_detail",
        "api_library_artist_match_detail",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def api_library_genre_profile_for_runtime(runtime: Any, genre: Any):
    """Run ``api_library_genre_profile`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _api_library_genre_profile_impl(genre)

def api_library_label_profile_for_runtime(runtime: Any, label: Any):
    """Run ``api_library_label_profile`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _api_library_label_profile_impl(label)

def api_library_artist_detail_for_runtime(runtime: Any, artist_id: Any):
    """Run ``api_library_artist_detail`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _api_library_artist_detail_impl(artist_id)

def api_library_artist_summary_ai_for_runtime(runtime: Any, artist_id: Any):
    """Run ``api_library_artist_summary_ai`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _api_library_artist_summary_ai_impl(artist_id)

def api_library_artist_summary_for_runtime(runtime: Any, artist_id: Any):
    """Run ``api_library_artist_summary`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return api_library_artist_summary(artist_id)

def api_library_album_detail_for_runtime(runtime: Any, album_id: Any):
    """Run ``api_library_album_detail`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _api_library_album_detail_impl(album_id)

def api_library_artist_similar_for_runtime(runtime: Any, artist_id: Any):
    """Run ``api_library_artist_similar`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _api_library_artist_similar_impl(artist_id)

def api_library_album_match_detail_for_runtime(runtime: Any, album_id: Any):
    """Run ``api_library_album_match_detail`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _api_library_album_match_detail_impl(album_id)

def api_library_artist_match_detail_for_runtime(runtime: Any, artist_id: Any):
    """Run ``api_library_artist_match_detail`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _api_library_artist_match_detail_impl(artist_id)

def api_library_artist_concerts_for_runtime(runtime: Any, artist_id: Any):
    """Run ``api_library_artist_concerts`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return api_library_artist_concerts(artist_id)


def _api_library_genre_profile_impl(genre: str):
    """Return profile data for a genre: description + notable artists (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"genre": genre or "", "error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"genre": genre or "", "error": err or "Files index unavailable"}), 503
    g = (genre or "").strip()
    if not g:
        return jsonify({"genre": "", "error": "Invalid genre"}), 400
    limit_artists = max(1, min(120, _parse_int_loose(request.args.get("limit_artists"), 24)))
    refresh = bool(_parse_bool(request.args.get("refresh")))
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    album_match_sql = (
        f"({_library_albums_match_where(include_unmatched, 'alb')})"
        f" AND ({_library_album_scope_where(scope, 'alb')})"
    )

    cache_key = (
        f"library:genre:profile:{g.lower()}:{limit_artists}:"
        f"{_library_cache_scope_suffix(scope)}:{_library_cache_unmatched_suffix(include_unmatched)}"
    )
    if not refresh:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"genre": g, "error": "PostgreSQL unavailable"}), 503
    try:
        base_url = request.url_root.rstrip("/")
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
                      AND """
                + album_match_sql
                + """
                    UNION
                    SELECT DISTINCT alb.id AS album_id
                    FROM files_albums alb
                    WHERE COALESCE(alb.tags_json, '[]') = '[]'
                      AND lower(trim(COALESCE(alb.genre, ''))) = lower(%s)
                      AND COALESCE(trim(alb.genre), '') <> ''
                      AND """
                + album_match_sql
                + """
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
                      AND """
                + album_match_sql
                + """
                    UNION
                    SELECT DISTINCT alb.id AS album_id
                    FROM files_albums alb
                    WHERE COALESCE(alb.tags_json, '[]') = '[]'
                      AND lower(trim(COALESCE(alb.genre, ''))) = lower(%s)
                      AND COALESCE(trim(alb.genre), '') <> ''
                      AND """
                + album_match_sql
                + """
                )
                SELECT
                    ar.id,
                    ar.name,
                    COUNT(*)::BIGINT AS release_count,
                    COALESCE(BOOL_OR(""" + _artist_has_true_image_sql("ar", "ext") + """), FALSE) AS has_image
                FROM matched_albums m
                JOIN files_albums alb ON alb.id = m.album_id
                JOIN files_artists ar ON ar.id = alb.artist_id
                LEFT JOIN files_external_artist_images ext ON ext.name_norm = ar.name_norm
                GROUP BY ar.id, ar.name
                ORDER BY release_count DESC, ar.name ASC
                LIMIT %s
                """,
                (g, g, int(limit_artists)),
            )
            rows = cur.fetchall()

        top_artists = []
        for artist_id, artist_name, release_count, has_image in rows:
            aid = int(artist_id or 0)
            if aid <= 0:
                continue
            top_artists.append(
                {
                    "artist_id": aid,
                    "artist_name": artist_name or "",
                    "album_count": int(release_count or 0),
                    "thumb": _artist_image_asset_url(base_url, aid, size=192) or None,
                }
            )

        wiki_extract = ""
        wiki_url = ""
        wiki_desc = ""
        for cand in (f"{g} music", f"{g} (music)", g):
            try:
                ex, url, desc = _fetch_wikipedia_intro_extract(cand, lang="en")
            except Exception:
                ex, url, desc = "", "", ""
            if ex:
                wiki_extract, wiki_url, wiki_desc = ex, url, desc
                break

        payload = {
            "genre": g,
            "album_count": int(album_count),
            "description": wiki_extract or "",
            "wiki_url": wiki_url or "",
            "wiki_description": wiki_desc or "",
            "top_artists": top_artists,
            "source": "wikipedia" if wiki_extract else "",
        }
        _files_cache_set_json(cache_key, payload, ttl=60 * 60 * 12)
        return jsonify(payload)
    finally:
        conn.close()


def _api_library_label_profile_impl(label: str):
    """Return profile data for a label: artists, genres, sub-label hints and owner hints."""
    if _get_library_mode() != "files":
        return jsonify({"label": label or "", "error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"label": label or "", "error": err or "Files index unavailable"}), 503
    raw_label = (label or "").strip()
    if not raw_label:
        return jsonify({"label": "", "error": "Invalid label"}), 400
    limit_artists = max(1, min(120, _parse_int_loose(request.args.get("limit_artists"), 24)))
    limit_genres = max(1, min(120, _parse_int_loose(request.args.get("limit_genres"), 24)))
    refresh = bool(_parse_bool(request.args.get("refresh")))
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    album_match_sql = (
        f"({_library_albums_match_where(include_unmatched, 'alb')})"
        f" AND ({_library_album_scope_where(scope, 'alb')})"
    )

    cache_key = (
        f"library:label:profile:{raw_label.lower()}:{limit_artists}:{limit_genres}:"
        f"{_library_cache_scope_suffix(scope)}:{_library_cache_unmatched_suffix(include_unmatched)}"
    )
    if not refresh:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"label": raw_label, "error": "PostgreSQL unavailable"}), 503
    try:
        base_url = request.url_root.rstrip("/")
        label_norm = _norm_label_key(raw_label)
        logo_url = ""
        logo_provider = ""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM files_albums alb
                WHERE lower(trim(COALESCE(alb.label, ''))) = lower(%s)
                  AND """
                + album_match_sql,
                (raw_label,),
            )
            album_count = int((cur.fetchone() or [0])[0] or 0)

            cur.execute(
                """
                SELECT
                    ar.id,
                    ar.name,
                    COUNT(*)::BIGINT AS release_count,
                    COALESCE(BOOL_OR(""" + _artist_has_true_image_sql("ar", "ext") + """), FALSE) AS has_image
                FROM files_albums alb
                JOIN files_artists ar ON ar.id = alb.artist_id
                LEFT JOIN files_external_artist_images ext ON ext.name_norm = ar.name_norm
                WHERE lower(trim(COALESCE(alb.label, ''))) = lower(%s)
                  AND """
                + album_match_sql
                + """
                GROUP BY ar.id, ar.name
                ORDER BY release_count DESC, ar.name ASC
                LIMIT %s
                """,
                (raw_label, int(limit_artists)),
            )
            artist_rows = cur.fetchall()

            cur.execute(
                """
                WITH matched AS (
                    SELECT
                        alb.id AS album_id,
                        COALESCE(alb.tags_json, '[]')::jsonb AS tags_json,
                        COALESCE(alb.genre, '') AS genre
                    FROM files_albums alb
                    WHERE lower(trim(COALESCE(alb.label, ''))) = lower(%s)
                      AND """
                + album_match_sql
                + """
                ),
                genre_tokens AS (
                    SELECT
                        m.album_id,
                        TRIM(g.value) AS genre_disp
                    FROM matched m
                    CROSS JOIN LATERAL jsonb_array_elements_text(m.tags_json) AS g(value)
                    WHERE COALESCE(TRIM(g.value), '') <> ''
                    UNION ALL
                    SELECT
                        m.album_id,
                        TRIM(m.genre) AS genre_disp
                    FROM matched m
                    WHERE COALESCE(TRIM(m.genre), '') <> ''
                      AND COALESCE(m.tags_json, '[]'::jsonb) = '[]'::jsonb
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
                (raw_label, int(limit_genres)),
            )
            genre_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    COALESCE(alb.bandcamp_album_url, ''),
                    COALESCE(alb.title, ''),
                    COALESCE(ar.name, '')
                FROM files_albums alb
                JOIN files_artists ar ON ar.id = alb.artist_id
                WHERE lower(trim(COALESCE(alb.label, ''))) = lower(%s)
                  AND """
                + album_match_sql
                + """
                  AND COALESCE(alb.bandcamp_album_url, '') <> ''
                ORDER BY
                    CASE WHEN COALESCE(alb.strict_match_provider, '') = 'bandcamp' THEN 0 ELSE 1 END,
                    alb.updated_at DESC,
                    alb.id DESC
                LIMIT 8
                """,
                (raw_label,),
            )
            label_bandcamp_rows = cur.fetchall()

        influential_artists = []
        for artist_id, artist_name, release_count, has_image in artist_rows:
            aid = int(artist_id or 0)
            if aid <= 0:
                continue
            influential_artists.append(
                {
                    "artist_id": aid,
                    "artist_name": artist_name or "",
                    "album_count": int(release_count or 0),
                    "thumb": _artist_image_asset_url(base_url, aid, size=192) or None,
                }
            )
        genres = [{"genre": str(r[0] or "").strip(), "count": int(r[1] or 0)} for r in genre_rows if str(r[0] or "").strip()]

        # Optional metadata enrichments (best-effort): Wikipedia + Discogs label graph.
        wiki_extract = ""
        wiki_url = ""
        wiki_desc = ""
        for cand in (f"{raw_label} (record label)", f"{raw_label} label", raw_label):
            try:
                ex, url, desc = _fetch_wikipedia_intro_extract(cand, lang="en")
            except Exception:
                ex, url, desc = "", "", ""
            if ex:
                wiki_extract, wiki_url, wiki_desc = ex, url, desc
                break

        owner = ""
        sub_labels: list[str] = []
        discogs_profile = ""
        discogs_url = ""
        try:
            if USE_DISCOGS:
                d = _discogs_client()
                if d is not None:
                    results = d.search(raw_label, type="label")
                    page = _discogs_call("label search page=1", lambda: results.page(1))
                    picked = None
                    for it in page or []:
                        data = getattr(it, "data", None)
                        if not isinstance(data, dict):
                            continue
                        lab_name = str(data.get("title") or data.get("name") or "").strip()
                        if not lab_name:
                            continue
                        score = _provider_identity_text_score(raw_label, lab_name)
                        if score >= 0.78:
                            picked = data
                            break
                    if isinstance(picked, dict):
                        lid = _parse_int_loose(picked.get("id"), 0)
                        if lid > 0:
                            ldata = _discogs_call("label data", lambda: d.label(int(lid)).data)
                            if isinstance(ldata, dict):
                                parent = ldata.get("parent_label")
                                if isinstance(parent, dict):
                                    owner = str(parent.get("name") or "").strip()
                                elif parent:
                                    owner = str(parent).strip()
                                sub_raw = ldata.get("sublabels") or []
                                if isinstance(sub_raw, list):
                                    for s in sub_raw:
                                        if isinstance(s, dict):
                                            nm = str(s.get("name") or "").strip()
                                        else:
                                            nm = str(s or "").strip()
                                        if nm:
                                            sub_labels.append(nm)
                                discogs_profile = _strip_html_text(str(ldata.get("profile") or "")).strip()
                                urls = ldata.get("urls") or []
                                if isinstance(urls, list):
                                    for u in urls:
                                        uu = str(u or "").strip()
                                        if uu:
                                            discogs_url = uu
                                            break
        except DiscogsRateLimited:
            pass
        except Exception:
            logging.debug("Label profile enrichment failed for '%s'", raw_label, exc_info=True)

        try:
            cached_label = _files_get_external_label_images(conn, [label_norm]).get(label_norm) or {}
        except Exception:
            cached_label = {}
        cached_label_path = str(cached_label.get("image_path") or "").strip()
        if cached_label_path:
            logo_url = f"{base_url}/api/library/external/label-image/{quote(label_norm, safe='')}?size=256"
            logo_provider = str(cached_label.get("provider") or "").strip().lower()
        if (refresh or not logo_url or not wiki_extract) and label_bandcamp_rows:
            for album_url, album_title, artist_name in label_bandcamp_rows:
                try:
                    bandcamp_payload = _fetch_bandcamp_album_info(
                        str(artist_name or "").strip(),
                        str(album_title or "").strip(),
                        allow_web_fallback=False,
                        album_url_hint=str(album_url or "").strip(),
                    ) or {}
                except Exception:
                    bandcamp_payload = {}
                if not isinstance(bandcamp_payload, dict) or not bandcamp_payload:
                    continue
                owner_name = str(
                    bandcamp_payload.get("page_owner_name")
                    or bandcamp_payload.get("label_name")
                    or ""
                ).strip()
                if owner_name and _provider_identity_text_score(raw_label, owner_name) < 0.78:
                    continue
                owner_bio = str(bandcamp_payload.get("page_owner_bio") or "").strip()
                owner_image_url = str(bandcamp_payload.get("page_owner_image_url") or "").strip()
                if not wiki_extract and owner_bio:
                    wiki_extract = owner_bio
                if owner_image_url:
                    try:
                        cached_logo = _files_cache_external_label_image(
                            conn,
                            label_name=raw_label,
                            provider="bandcamp",
                            image_url=owner_image_url,
                        )
                    except Exception:
                        cached_logo = None
                    if cached_logo:
                        logo_url = f"{base_url}/api/library/external/label-image/{quote(label_norm, safe='')}?size=256"
                        logo_provider = "bandcamp"
                if wiki_extract or logo_url:
                    break

        # Simple owner fallback from profile text if parent_label is missing.
        if (not owner) and discogs_profile:
            m = re.search(r"(?:founded by|owner(?:ed)? by)\s+([^.;\n]{3,120})", discogs_profile, flags=re.IGNORECASE)
            if m:
                owner = m.group(1).strip()

        payload = {
            "label": raw_label,
            "album_count": int(album_count),
            "description": wiki_extract or "",
            "wiki_url": wiki_url or "",
            "wiki_description": wiki_desc or "",
            "owner": owner or "",
            "sub_labels": _dedupe_keep_order(sub_labels)[:60],
            "influential_artists": influential_artists,
            "genres": genres,
            "discogs_profile": discogs_profile or "",
            "discogs_url": discogs_url or "",
            "logo_url": logo_url or "",
            "logo_provider": logo_provider or "",
        }
        _files_cache_set_json(cache_key, payload, ttl=60 * 60 * 12)
        return jsonify(payload)
    finally:
        conn.close()


def _api_library_artist_detail_impl(artist_id):
    """Return detailed information about an artist including all albums with images and types."""
    if _get_library_mode() == "files":
        include_unmatched = _library_include_unmatched_effective()
        album_match_sql = _library_albums_match_where(include_unmatched, "alb")
        cache_key = f"library:artist:v4:{artist_id}:{_library_cache_unmatched_suffix(include_unmatched)}"
        refresh = bool(_parse_bool(request.args.get("refresh")))
        if not refresh:
            cached = _files_cache_get_json(cache_key)
            if cached is not None:
                return jsonify(cached)
        ok, err = _ensure_files_index_ready()
        if not ok:
            return jsonify({"error": err or "Files index unavailable"}), 503
        conn = _files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, name, name_norm, has_image, image_path, COALESCE(entity_kind, 'artist'), COALESCE(roles_json, '[]'), created_at, updated_at FROM files_artists WHERE id = %s",
                    (artist_id,),
                )
                artist_row = cur.fetchone()
                if not artist_row:
                    return jsonify({"error": "Artist not found"}), 404
                artist_name = artist_row[1] or ""
                artist_norm = str(artist_row[2] or "").strip() or _norm_artist_key(artist_name)
                has_artist_image = bool(artist_row[3])
                artist_image_path = (artist_row[4] or "").strip()
                artist_entity_kind = str(artist_row[5] or "artist").strip() or "artist"
                artist_roles = _safe_json_load(artist_row[6] or "[]", fallback=[])
                artist_created_epoch = int(_dt_to_epoch(artist_row[7])) if len(artist_row) > 7 else 0
                artist_updated_epoch = int(_dt_to_epoch(artist_row[8])) if len(artist_row) > 8 else 0
                cur.execute(
                    """
                    WITH artist_albums AS (
                        SELECT
                            link.album_id,
                            array_to_string(array_agg(DISTINCT link.role), '|') AS artist_roles,
                            bool_or(link.is_primary) AS artist_is_primary
                        FROM files_artist_album_links link
                        JOIN files_albums alb ON alb.id = link.album_id
                        WHERE link.artist_id = %s
                          AND """ + album_match_sql + """
                        GROUP BY link.album_id
                    )
                    SELECT
                        id, title, title_norm, year, date_text, COALESCE(genre, '') AS genre, COALESCE(label, '') AS label, COALESCE(tags_json, '[]') AS tags_json,
                        track_count, is_broken, format, is_lossless, COALESCE(sample_rate, 0) AS sample_rate, COALESCE(bit_depth, 0) AS bit_depth,
                        has_cover, COALESCE(cover_path, ''), COALESCE(folder_path, ''), mb_identified, musicbrainz_release_group_id,
                        discogs_release_id, lastfm_album_mbid, bandcamp_album_url, metadata_source, COALESCE(strict_match_provider, '') AS strict_match_provider, COALESCE(primary_tags_json, '{}') AS primary_tags_json,
                        expected_track_count, actual_track_count, missing_indices_json, COALESCE(aa.artist_roles, '') AS artist_roles, COALESCE(aa.artist_is_primary, FALSE) AS artist_is_primary,
                        COUNT(*) OVER (PARTITION BY title_norm) AS dup_count
                    FROM files_albums alb
                    JOIN artist_albums aa ON aa.album_id = alb.id
                    ORDER BY COALESCE(year, 0) DESC, title ASC
                    """,
                    (artist_id,),
                )
                rows = cur.fetchall()

            composer_names_by_album = _files_album_linked_composer_names_map(
                conn,
                [int(r[0] or 0) for r in rows if int(r[0] or 0) > 0],
            )

            _files_ensure_local_artist_profile(
                conn,
                artist_id=int(artist_id),
                artist_name=artist_name,
                artist_norm=artist_norm,
                entity_kind=artist_entity_kind,
                roles_json=json.dumps(artist_roles if isinstance(artist_roles, list) else []),
            )
            artist_profile = _files_get_artist_profile_cached(artist_name, artist_norm)
            title_norms = [str(r[2] or "") for r in rows if str(r[2] or "").strip()]
            album_profile_map = _files_get_album_profiles_cached(artist_norm, title_norms)
            profile_enriching = _files_profile_job_is_active(artist_norm)
            user_rating_map: dict[int, int] = {}
            user_id = int(_current_user_id_or_zero())
            if user_id > 0 and rows:
                album_ids = [int(r[0] or 0) for r in rows if int(r[0] or 0) > 0]
                if album_ids:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT album_id, rating
                            FROM files_user_album_ratings
                            WHERE user_id = %s
                              AND album_id = ANY(%s)
                            """,
                            (user_id, album_ids),
                        )
                        for aid, rating in cur.fetchall():
                            parsed_rating = int(rating or 0)
                            if int(aid or 0) > 0 and parsed_rating > 0:
                                user_rating_map[int(aid)] = max(1, min(5, parsed_rating))

            albums = []
            stats_duplicates = 0
            stats_no_cover = 0
            stats_mb = 0
            stats_broken = 0
            for row in rows:
                album_id = int(row[0])
                title_norm = str(row[2] or "")
                genre_raw = (row[5] or "").strip()
                label_raw = (row[6] or "").strip()
                tags_json_raw = row[7] or "[]"
                track_count = int(row[8] or 0)
                is_broken = bool(row[9])
                fmt = (row[10] or "").strip() or None
                is_lossless = bool(row[11])
                sample_rate = int(row[12] or 0)
                bit_depth = int(row[13] or 0)
                has_cover = bool(row[14])
                cover_path_raw = (row[15] or "").strip()
                folder_path_raw = (row[16] or "").strip()
                mb_identified = bool(row[17])
                mbid = (row[18] or "").strip() or None
                discogs_release_id = (row[19] or "").strip() or None
                lastfm_album_mbid = (row[20] or "").strip() or None
                bandcamp_album_url = (row[21] or "").strip() or None
                metadata_source = (row[22] or "").strip() or None
                strict_match_provider = _normalize_identity_provider(str(row[23] or "")) or None
                primary_tags_json_raw = row[24] or "{}"
                expected_track_count = row[25]
                actual_track_count = row[26]
                missing_indices_raw = row[27] or "[]"
                artist_roles_raw = str(row[28] or "").strip()
                artist_is_primary = bool(row[29])
                dup_count = int(row[30] or 0)
                album_profile = album_profile_map.get(title_norm, {}) if title_norm else {}
                genres_list = _merge_album_genre_lists(tags_json_raw, album_profile.get("tags"), genre_raw or "")
                artist_roles_for_album = [
                    value
                    for value in sorted(
                        {str(item or "").strip().lower() for item in artist_roles_raw.split("|") if str(item or "").strip()},
                        key=lambda role: (_FILES_BROWSE_ROLE_PRIORITY.get(role, 99), role),
                    )
                    if value
                ]
                appears_on = bool(
                    (not artist_is_primary)
                    and any(role in {"featured", "appearance"} for role in artist_roles_for_album)
                    and "artist" not in artist_roles_for_album
                )

                has_cover_effective, _effective_cover_path = _resolve_files_album_cover_asset(
                    album_id=int(album_id or 0),
                    cover_path_raw=str(cover_path_raw or "").strip(),
                    folder_path_raw=str(folder_path_raw or "").strip(),
                    has_cover=bool(has_cover),
                    persist=True,
                )

                if dup_count > 1:
                    stats_duplicates += 1
                if not has_cover_effective:
                    stats_no_cover += 1
                if mb_identified:
                    stats_mb += 1
                if is_broken:
                    stats_broken += 1

                album_type = "Album"
                if track_count <= 3:
                    album_type = "Single"
                elif track_count <= 6:
                    album_type = "EP"

                try:
                    missing_indices = json.loads(missing_indices_raw) if missing_indices_raw else []
                except (TypeError, ValueError):
                    missing_indices = []
                primary_tags_map = _safe_json_load(primary_tags_json_raw, fallback={})
                if not isinstance(primary_tags_map, dict):
                    primary_tags_map = {}
                classical_payload = _classical_display_payload(
                    primary_tags_map,
                    fallback_title=str(row[1] or ""),
                    fallback_artist=str(artist_name or ""),
                )
                classical_payload = _files_apply_canonical_composers_to_classical_payload(
                    classical_payload,
                    composer_names_by_album.get(album_id) or [],
                )

                broken_detail = None
                if is_broken:
                    broken_detail = {
                        "expected_track_count": int(expected_track_count or track_count),
                        "actual_track_count": int(actual_track_count or track_count),
                        "missing_indices": missing_indices if isinstance(missing_indices, list) else [],
                    }

                thumb_url_files = f"{request.url_root.rstrip('/')}/api/library/files/album/{album_id}/cover" if has_cover_effective else None
                can_improve = (not is_lossless) or (not has_cover_effective) or (not mb_identified) or is_broken
                albums.append({
                    "album_id": album_id,
                    "title": row[1] or "",
                    "year": row[3],
                    "date": row[4] or "",
                    "track_count": track_count,
                    "is_broken": is_broken,
                    "thumb": f"{thumb_url_files}?size=320" if thumb_url_files else None,
                    "type": album_type,
                    "format": fmt,
                    "is_lossless": is_lossless,
                    "sample_rate": sample_rate or None,
                    "bit_depth": bit_depth or None,
                    "thumb_empty": not has_cover_effective,
                    "mb_identified": mb_identified,
                    "musicbrainz_release_group_id": mbid,
                    "discogs_release_id": discogs_release_id,
                    "lastfm_album_mbid": lastfm_album_mbid,
                    "bandcamp_album_url": bandcamp_album_url,
                    "metadata_source": metadata_source,
                    "strict_match_provider": strict_match_provider,
                    "in_duplicate_group": dup_count > 1,
                    "can_improve": can_improve,
                    "broken_detail": broken_detail,
                    "genre": "; ".join(genres_list) if genres_list else (genre_raw or None),
                    "genres": genres_list,
                    "label": label_raw or None,
                    "classical": classical_payload,
                    "description": album_profile.get("description"),
                    "short_description": album_profile.get("short_description"),
                    "description_source": album_profile.get("source"),
                    "public_rating": float(album_profile.get("public_rating")) if album_profile.get("public_rating") is not None else None,
                    "public_rating_votes": int(album_profile.get("public_rating_votes") or 0),
                    "public_rating_source": str(album_profile.get("public_rating_source") or "").strip() or None,
                    "heat_score": float(album_profile.get("heat_score")) if album_profile.get("heat_score") is not None else None,
                    "heat_label": None,
                    "user_rating": user_rating_map.get(album_id),
                    "artist_roles": artist_roles_for_album,
                    "artist_is_primary": bool(artist_is_primary),
                    "appears_on": appears_on,
                })

            base_url = request.url_root.rstrip("/")
            # Patch similar artists with local IDs + images (local first, then cached external).
            try:
                if isinstance(artist_profile, dict):
                    sim = artist_profile.get("similar_artists")
                    if isinstance(sim, list) and sim:
                        artist_profile = dict(artist_profile)
                        artist_profile["similar_artists"] = _files_attach_similar_artist_refs(conn, sim, base_url)
                        # If images are still missing (e.g. provider only returned placeholders),
                        # warm external image cache in the background so the grid becomes pretty.
                        missing_names: list[str] = []
                        for it in artist_profile.get("similar_artists") or []:
                            if not isinstance(it, dict):
                                continue
                            nm = str(it.get("name") or "").strip()
                            if not nm:
                                continue
                            if not str(it.get("image_url") or "").strip():
                                missing_names.append(nm)
                        if missing_names:
                            _enqueue_files_similar_images_warm(artist_norm, missing_names[:12], force=True)
            except Exception:
                pass
            needs_profile_refresh = _artist_profile_payload_requires_refresh(
                artist_profile if isinstance(artist_profile, dict) else None,
                entity_kind=artist_entity_kind,
                role_hints=artist_roles if isinstance(artist_roles, list) else [],
            )
            needs_artist_image_refresh = True
            artist_thumb_version = int(artist_updated_epoch or 0)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            COALESCE(ext.image_path, ''),
                            COALESCE(ext.artist_name, ''),
                            COALESCE(ext.provider, ''),
                            COALESCE(ext.image_url, '')
                        FROM files_artists a
                        LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                        WHERE a.id = %s
                        LIMIT 1
                        """,
                        (int(artist_id),),
                    )
                    media_row = cur.fetchone()
                ext_image_path = str((media_row[0] if media_row else "") or "").strip()
                ext_artist_name = " ".join(str((media_row[1] if media_row else "") or "").split()).strip()
                ext_provider = str((media_row[2] if media_row else "") or "").strip().lower()
                ext_image_url = str((media_row[3] if media_row else "") or "").strip()
                artist_image_path, ext_image_path, ext_valid_exact = _files_reconcile_artist_image_cache_state(
                    conn,
                    artist_name=artist_name,
                    artist_norm=artist_norm,
                    entity_kind=artist_entity_kind,
                    role_hints=artist_roles if isinstance(artist_roles, list) else [],
                    local_image_path=artist_image_path,
                    ext_image_path=ext_image_path,
                    ext_artist_name=ext_artist_name,
                    ext_provider=ext_provider,
                    ext_image_url=ext_image_url,
                )
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            ({_artist_has_true_image_sql("a", "ext")}) AS has_image,
                            GREATEST(
                                EXTRACT(EPOCH FROM COALESCE(a.updated_at, NOW())),
                                EXTRACT(EPOCH FROM COALESCE(ext.updated_at, a.updated_at, NOW()))
                            )::bigint AS image_version
                        FROM files_artists a
                        LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                        WHERE a.id = %s
                        LIMIT 1
                        """,
                        (int(artist_id),),
                    )
                    effective_media_row = cur.fetchone()
                artist_has_image_effective = bool((effective_media_row[0] if effective_media_row else False) or False)
                artist_thumb_version = int((effective_media_row[1] if effective_media_row else 0) or artist_updated_epoch or 0)
                if not artist_has_image_effective:
                    artist_has_image_effective = _artist_effective_image_present(
                        artist_name=artist_name,
                        entity_kind=artist_entity_kind,
                        role_hints=artist_roles if isinstance(artist_roles, list) else [],
                        local_image_path=artist_image_path,
                        ext_image_path=ext_image_path,
                        ext_artist_name=ext_artist_name,
                        ext_provider=ext_provider,
                        ext_image_url=ext_image_url,
                    )
                needs_artist_image_refresh = not artist_has_image_effective
            except Exception:
                needs_artist_image_refresh = True
                artist_has_image_effective = _artist_effective_image_present(
                    artist_name=artist_name,
                    entity_kind=artist_entity_kind,
                    role_hints=artist_roles if isinstance(artist_roles, list) else [],
                    local_image_path=artist_image_path,
                )
            artist_thumb = (
                _artist_image_asset_url(
                    base_url,
                    artist_id,
                    size=320,
                    version=artist_thumb_version or None,
                )
                if artist_has_image_effective
                else None
            )
            if needs_profile_refresh or needs_artist_image_refresh:
                profile_enriching = _enqueue_files_profile_enrichment(
                    artist_name,
                    artist_norm,
                    [
                        (str(album.get("title") or "").strip(), norm_album(str(album.get("title") or "").strip()))
                        for album in albums[:120]
                        if str(album.get("title") or "").strip()
                    ],
                    force=True,
                    fast_mode=False,
                ) or profile_enriching
            payload = {
                "artist_id": artist_id,
                "artist_name": artist_name,
                "entity_kind": artist_entity_kind,
                "roles": artist_roles if isinstance(artist_roles, list) else [],
                "created_at": artist_created_epoch or None,
                "updated_at": artist_updated_epoch or None,
                "artist_thumb": artist_thumb,
                "artist_has_image": bool(artist_has_image_effective),
                "artist_profile": artist_profile,
                "profile_enriching": profile_enriching,
                "albums": albums,
                "total_albums": len(albums),
                "stats": {
                    "duplicates": stats_duplicates,
                    "no_cover": stats_no_cover,
                    "mb_identified": stats_mb,
                    "broken": stats_broken,
                },
            }
            _files_cache_set_json(cache_key, payload, ttl=30)
            return jsonify(payload)
        finally:
            conn.close()
    return jsonify({"error": "Files mode required"}), 400


def _api_library_artist_summary_ai_impl(artist_id: int):
    """Generate/refresh an AI summary (100-200 words) for an artist and persist it in PostgreSQL."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Artist summary endpoint is available in Files mode only"}), 400
    if not _auth_user_can_use_ai(_current_user_or_empty()):
        return jsonify({"error": "AI access is disabled for this user"}), 403
    ai_ok, _provider_effective, _auth_mode, ai_reason = _resolve_ai_runtime_availability(
        analysis_type="assistant_chat",
        requested_provider="openai",
        user_id=_current_user_id_or_zero(),
    )
    if not ai_ok:
        msg = ai_reason or getattr(sys.modules[__name__], "AI_FUNCTIONAL_ERROR_MSG", None) or "AI is not configured"
        logging.warning(
            "[ArtistFacts] AI unavailable for artist_id=%s user_id=%s provider=%s auth=%s reason=%s",
            int(artist_id or 0),
            _current_user_id_or_zero(),
            str(_provider_effective or ""),
            str(_auth_mode or ""),
            str(msg or ""),
        )
        return jsonify({"error": msg}), 503

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        body = {}
    lang = str(body.get("lang") or "").strip().lower() or _assistant_preferred_lang()
    if lang not in {"en", "fr"}:
        lang = "en"

    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503

    try:
        # Ensure we have the latest local context docs.
        context_info = _assistant_ingest_artist_rag(conn, int(artist_id))
        if not context_info:
            return jsonify({"error": "Artist not found"}), 404
        artist_name = context_info.get("artist_name") or ""

        # Collect context: original bio (if any) + local snapshot.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT doc_type, source, content
                FROM assistant_docs
                WHERE entity_type = 'artist'
                  AND entity_id = %s
                  AND doc_type IN ('artist_profile_bio', 'artist_profile_short', 'artist_library_snapshot')
                ORDER BY updated_at DESC
                """,
                (int(artist_id),),
            )
            rows = cur.fetchall()
        doc_map: dict[str, dict] = {}
        for dt, src, content in rows:
            key = str(dt or "").strip().lower()
            if not key or key in doc_map:
                continue
            doc_map[key] = {"source": (src or "").strip(), "content": (content or "").strip()}

        original_text = (doc_map.get("artist_profile_bio") or {}).get("content") or (doc_map.get("artist_profile_short") or {}).get("content") or ""
        original_source = (doc_map.get("artist_profile_bio") or {}).get("source") or (doc_map.get("artist_profile_short") or {}).get("source") or ""
        snapshot_text = (doc_map.get("artist_library_snapshot") or {}).get("content") or ""

        # External context for "real" summaries when local/provider bios are missing.
        wiki_text = ""
        wiki_source = ""
        wiki_url = ""
        wiki_lang = ""
        serper_snippets_text = ""
        concerts_text = ""
        concerts_provider_used = ""

        try:
            if _is_garbage_bio(original_text):
                original_text = ""
        except Exception:
            pass

        if not original_text.strip():
            try:
                wiki_info = _fetch_wikipedia_artist_bio_best(artist_name)
                if isinstance(wiki_info, dict) and str(wiki_info.get("bio") or "").strip():
                    wiki_text = str(wiki_info.get("bio") or "").strip()
                    wiki_source = str(wiki_info.get("source") or "").strip() or "wikipedia"
                    wiki_url = str(wiki_info.get("url") or "").strip()
                    wiki_lang = str(wiki_info.get("lang") or "").strip()
                    _assistant_upsert_doc(
                        conn,
                        entity_type="artist",
                        entity_id=int(artist_id),
                        doc_type="artist_external_wikipedia_intro",
                        source=wiki_source,
                        title=str(artist_name),
                        url=wiki_url,
                        lang=wiki_lang,
                        content=wiki_text,
                    )
            except Exception:
                pass

            # Serper web snippets (optional, best-effort).
            try:
                hits = _serper_web_search(f"{artist_name} musician", num=5)
                lines: list[str] = []
                for h in hits[:5]:
                    if not isinstance(h, dict):
                        continue
                    title = str(h.get("title") or "").strip()
                    link = str(h.get("link") or "").strip()
                    snippet = str(h.get("snippet") or "").strip()
                    if not (title or snippet):
                        continue
                    chunk = f"- {title} | {snippet}"
                    if link:
                        chunk += f" | {link}"
                    lines.append(chunk)
                serper_snippets_text = "\n".join(lines).strip()
                if serper_snippets_text:
                    source_name = str((hits[0].get("source") if hits and isinstance(hits[0], dict) else "") or "web_search").strip() or "web_search"
                    provider_name = "openai" if "openai" in source_name.lower() else ("serper" if "serper" in source_name.lower() else "web")
                    _assistant_upsert_doc(
                        conn,
                        entity_type="artist",
                        entity_id=int(artist_id),
                        doc_type="artist_external_web_snippets",
                        source=source_name,
                        provider=provider_name,
                        title=str(artist_name),
                        content=serper_snippets_text,
                    )
            except Exception:
                pass

            # Upcoming concerts (cached; refresh if missing).
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT provider, events_json, source_url, updated_at
                        FROM files_artist_concerts
                        WHERE artist_id = %s
                        """,
                        (int(artist_id),),
                    )
                    crow = cur.fetchone()
                events: list[dict] = []
                provider = "bandsintown"
                source_url = ""
                if crow:
                    provider = str(crow[0] or "").strip() or provider
                    source_url = str(crow[2] or "").strip()
                    try:
                        events = json.loads(crow[1] or "[]") if crow[1] else []
                    except Exception:
                        events = []
                # If missing, fetch once using the shared provider selector.
                if not events:
                    provider, events, fetched_source_url = _concerts_fetch_upcoming_events(artist_name, "auto")
                    if fetched_source_url:
                        source_url = fetched_source_url
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO files_artist_concerts(artist_id, provider, events_json, source_url, updated_at)
                                VALUES (%s, %s, %s, %s, NOW())
                                ON CONFLICT (artist_id) DO UPDATE SET
                                    provider = EXCLUDED.provider,
                                    events_json = EXCLUDED.events_json,
                                    source_url = EXCLUDED.source_url,
                                    updated_at = NOW()
                                """,
                                (int(artist_id), provider, json.dumps(events, ensure_ascii=False), source_url or None),
                            )
                if isinstance(events, list) and events:
                    lines = []
                    for ev in events[:8]:
                        if not isinstance(ev, dict):
                            continue
                        dt = str(ev.get("datetime") or "").strip()
                        venue = ev.get("venue") if isinstance(ev.get("venue"), dict) else {}
                        city = str((venue or {}).get("city") or "").strip()
                        country = str((venue or {}).get("country") or "").strip()
                        vname = str((venue or {}).get("name") or "").strip()
                        where = ", ".join([x for x in [city, country] if x])
                        parts = [p for p in [dt[:10] if dt else "", where, vname] if p]
                        if parts:
                            lines.append("- " + " · ".join(parts))
                    concerts_text = "\n".join(lines).strip()
                    concerts_provider_used = str(provider or "").strip().lower()
                    if concerts_text:
                        _assistant_upsert_doc(
                            conn,
                            entity_type="artist",
                            entity_id=int(artist_id),
                            doc_type="artist_concerts_upcoming",
                            source=str(provider or "bandsintown"),
                            title=str(artist_name),
                            url=str(source_url or ""),
                            content=concerts_text,
                        )
            except Exception:
                pass

        # AI prompt: produce a clean artist description (no "AI" mentions, no local file/format talk).
        lang_hint = "French" if lang == "fr" else "English"
        system_msg = (
            "You are PMDA Intelligence, a meticulous music librarian.\n"
            "Rules:\n"
            "- Use ONLY the provided context.\n"
            "- Do not invent facts.\n"
            "- Output must be plain text (no markdown).\n"
            f"- Write in {lang_hint}.\n"
            "- Length: 100 to 200 words.\n"
            "- Focus on the artist (bio, style, era, notable works, labels, collaborations, scene).\n"
            "- Do NOT mention audio formats, file quality, local file paths, IDs, or anything about the user's library.\n"
            "- If the context is insufficient, keep it generic and short rather than guessing.\n"
        )
        external_blocks: list[str] = []
        if wiki_text:
            external_blocks.append(f"Wikipedia intro (source={wiki_source}, lang={wiki_lang}, url={wiki_url or 'n/a'}):\n{wiki_text}")
        if serper_snippets_text:
            external_blocks.append(f"Web snippets (Serper):\n{serper_snippets_text}")
        if concerts_text:
            external_blocks.append(f"Upcoming concerts:\n{concerts_text}")
        external_context = "\n\n".join([b for b in external_blocks if b]).strip()
        external_section = ""
        if external_context:
            external_section = "External sources:\n" + external_context + "\n\n"
        user_msg = (
            f"Artist: {artist_name}\n\n"
            f"Original bio (source={original_source or 'unknown'}):\n{original_text or '(none)'}\n\n"
            f"{external_section}"
            "Task: Write the best possible artist description for a music library UI."
        )

        provider = "openai"
        model = getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4o-mini")
        ai_text = call_ai_provider_longform(
            provider,
            model,
            system_msg,
            user_msg,
            max_tokens=520,
            analysis_type="assistant_chat",
        )
        ai_text = (ai_text or "").strip()
        if not ai_text:
            return jsonify({"error": "AI returned empty summary"}), 502

        # Persist as assistant doc so it can be chunked and re-used by the chat RAG.
        sources_used: list[str] = []
        try:
            if original_text.strip() and (original_source or "").strip():
                sources_used.append(str(original_source).strip())
            if wiki_text:
                sources_used.append("wikipedia")
            if serper_snippets_text:
                sources_used.append("web")
            if concerts_text:
                sources_used.append(concerts_provider_used or "concerts")
        except Exception:
            sources_used = []
        # Deduplicate while preserving order.
        sources_used = list(dict.fromkeys([s for s in sources_used if (s or "").strip()]))[:8]
        source_label = ", ".join(sources_used) if sources_used else "web"
        _assistant_upsert_doc(
            conn,
            entity_type="artist",
            entity_id=int(artist_id),
            doc_type="artist_summary_ai",
            source=source_label,
            provider=str(provider),
            model=str(model),
            title=str(artist_name),
            lang=lang,
            content=ai_text,
        )
        _files_cache_invalidate_all()

        return jsonify(
            {
                "artist_id": int(artist_id),
                "artist_name": artist_name,
                "ai": {
                    "text": ai_text,
                    "source": source_label,
                    "provider": str(provider),
                    "model": str(model),
                    "lang": lang,
                    "updated_at": int(time.time()),
                },
            }
        )
    finally:
        conn.close()


def _api_library_album_detail_impl(album_id: int):
    """Return album details + tracklist (for album page). Files mode only."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    album_id = int(album_id or 0)
    if album_id <= 0:
        return jsonify({"error": "Invalid album id"}), 400
    user_id = _current_user_id_or_zero()

    cache_key = f"library:album:v5:u{user_id}:{album_id}"
    refresh = bool(_parse_bool(request.args.get("refresh")))
    if not refresh:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    alb.id,
                    alb.title,
                    alb.title_norm,
                    alb.strict_match_verified,
                    EXTRACT(EPOCH FROM alb.created_at)::BIGINT AS created_at,
                    EXTRACT(EPOCH FROM alb.updated_at)::BIGINT AS updated_at,
                    COALESCE(alb.year, 0) AS year,
                    COALESCE(alb.date_text, '') AS date_text,
                    COALESCE(alb.genre, '') AS genre,
                    COALESCE(alb.label, '') AS label,
                    COALESCE(alb.format, '') AS format,
                    alb.is_lossless,
                    COALESCE(alb.sample_rate, 0) AS sample_rate,
                    COALESCE(alb.bit_depth, 0) AS bit_depth,
                    COALESCE(alb.track_count, 0) AS track_count,
                    COALESCE(alb.total_duration_sec, 0) AS total_duration_sec,
                    alb.has_cover,
                    COALESCE(alb.cover_path, '') AS cover_path,
                    COALESCE(alb.folder_path, '') AS folder_path,
                    COALESCE(alb.musicbrainz_release_group_id, '') AS musicbrainz_release_group_id,
                    COALESCE(alb.discogs_release_id, '') AS discogs_release_id,
                    COALESCE(alb.lastfm_album_mbid, '') AS lastfm_album_mbid,
                    COALESCE(alb.bandcamp_album_url, '') AS bandcamp_album_url,
                    COALESCE(alb.metadata_source, '') AS metadata_source,
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
                        art.id
                    ) AS artist_id,
                    COALESCE(art.name, '') AS artist_name,
                    COALESCE(art.name_norm, '') AS artist_norm,
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
                    ) AS composer_names_json
                FROM files_albums alb
                JOIN files_artists art ON art.id = alb.artist_id
                WHERE alb.id = %s
                """,
                (album_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Album not found"}), 404

            (
                _aid,
                album_title,
                title_norm,
                strict_match_verified,
                album_created_at,
                album_updated_at,
                year,
                date_text,
                genre,
                label,
                fmt,
                is_lossless,
                album_sample_rate,
                album_bit_depth,
                track_count,
                total_duration_sec,
                has_cover,
                cover_path_raw,
                folder_path_raw,
                musicbrainz_release_group_id,
                discogs_release_id,
                lastfm_album_mbid,
                bandcamp_album_url,
                metadata_source,
                primary_tags_json,
                artist_id,
                artist_name,
                artist_name_norm,
                composer_names_json,
            ) = row

            # Album profile (review/description) is keyed by (artist_norm, title_norm).
            artist_norm = str(artist_name_norm or "").strip() or _norm_artist_key(artist_name)
            title_norm = str(title_norm or "").strip()
            has_identity_hint = bool(
                bool(strict_match_verified)
                or str(metadata_source or "").strip()
                or str(musicbrainz_release_group_id or "").strip()
                or str(discogs_release_id or "").strip()
                or str(lastfm_album_mbid or "").strip()
                or str(bandcamp_album_url or "").strip()
            )
            allow_profile_for_album = bool(strict_match_verified or has_identity_hint)
            prof = {}
            if allow_profile_for_album and artist_norm and title_norm:
                try:
                    norm_variants = _profile_title_norm_variants(title_norm, album_title)
                    placeholders = ",".join(["%s"] * len(norm_variants))
                    cur.execute(
                        f"""
                        SELECT
                            description,
                            short_description,
                            source,
                            COALESCE(tags_json, '[]'),
                            updated_at,
                            title_norm,
                            public_rating,
                            COALESCE(public_rating_votes, 0),
                            COALESCE(public_rating_source, ''),
                            COALESCE(discogs_have_count, 0),
                            COALESCE(discogs_want_count, 0),
                            COALESCE(bandcamp_supporter_count, 0),
                            COALESCE(bandcamp_supporter_comments_json, '[]'),
                            COALESCE(lastfm_scrobbles, 0),
                            COALESCE(lastfm_listeners, 0),
                            heat_score,
                            COALESCE(heat_label, '')
                        FROM files_album_profiles
                        WHERE artist_norm = %s
                          AND title_norm IN ({placeholders})
                        ORDER BY CASE WHEN title_norm = %s THEN 0 ELSE 1 END, updated_at DESC
                        LIMIT 1
                        """,
                        [artist_norm, *norm_variants, title_norm],
                    )
                    prow = cur.fetchone()
                    if prow:
                        prof = {
                            "description": (prow[0] or "").strip(),
                            "short_description": (prow[1] or "").strip(),
                            "source": (prow[2] or "").strip(),
                            "tags_json": str(prow[3] or "[]"),
                            "updated_at": int(_dt_to_epoch(prow[4])) if prow[4] else 0,
                            "title_norm": str(prow[5] or "").strip(),
                            "public_rating": float(prow[6]) if prow[6] is not None else None,
                            "public_rating_votes": int(prow[7] or 0),
                            "public_rating_source": str(prow[8] or "").strip() or None,
                            "discogs_have_count": int(prow[9] or 0),
                            "discogs_want_count": int(prow[10] or 0),
                            "bandcamp_supporter_count": int(prow[11] or 0),
                            "bandcamp_supporter_comments": _normalize_bandcamp_supporter_comments(_safe_json_load(prow[12], fallback=[])),
                            "lastfm_scrobbles": int(prow[13] or 0),
                            "lastfm_listeners": int(prow[14] or 0),
                            "heat_score": float(prow[15]) if prow[15] is not None else None,
                            "heat_label": None,
                        }
                except Exception:
                    prof = {}
            user_rating = None
            user_review_text = ""
            user_review_updated_at = None
            if user_id > 0:
                try:
                    parsed_rating, parsed_review_text, parsed_updated_at = _files_user_album_feedback_row(
                        cur,
                        int(user_id),
                        int(album_id),
                    )
                    if parsed_rating > 0:
                        user_rating = parsed_rating
                    user_review_text = parsed_review_text
                    user_review_updated_at = parsed_updated_at
                except Exception:
                    user_rating = None
                    user_review_text = ""
                    user_review_updated_at = None

            # Keep this endpoint low-latency:
            # do not perform network/provider/AI review lookup synchronously here.

            # Tracks (detailed)
            cur.execute(
                """
                SELECT
                    id,
                    COALESCE(title, '') AS title,
                    COALESCE(disc_num, 0) AS disc_num,
                    COALESCE(track_num, 0) AS track_num,
                    COALESCE(duration_sec, 0) AS duration_sec,
                    COALESCE(format, '') AS format,
                    COALESCE(bitrate, 0) AS bitrate,
                    COALESCE(sample_rate, 0) AS sample_rate,
                    COALESCE(bit_depth, 0) AS bit_depth,
                    COALESCE(file_size_bytes, 0) AS file_size_bytes,
                    COALESCE(file_path, '') AS file_path
                FROM files_tracks
                WHERE album_id = %s
                ORDER BY disc_num ASC, track_num ASC, id ASC
                """,
                (album_id,),
            )
            track_rows = cur.fetchall()

        has_cover_effective, cover_path_effective = _resolve_files_album_cover_asset(
            album_id=int(album_id or 0),
            cover_path_raw=str(cover_path_raw or "").strip(),
            folder_path_raw=str(folder_path_raw or "").strip(),
            has_cover=bool(has_cover),
            persist=True,
        )

        if allow_profile_for_album and artist_norm and title_norm and (
            (not _album_profile_has_payload(prof)) or (not _album_profile_has_text(prof)) or (not has_cover_effective)
        ):
            _enqueue_files_profile_enrichment(
                artist_name,
                artist_norm,
                [(str(album_title or "").strip(), title_norm)],
                force=True,
                fast_mode=False,
            )

        # Keep this endpoint fast: background enrichment handles missing durations/embedded cover.
        enrich_rows = [(int(r[0] or 0), int(r[4] or 0), str(r[10] or "")) for r in track_rows]
        _schedule_album_detail_enrichment(
            int(album_id),
            rows=enrich_rows,
            has_cover=bool(has_cover_effective),
            cover_path_raw=cover_path_effective,
        )

        raw_tracks: list[dict[str, Any]] = []
        primary_tags = _safe_json_load(primary_tags_json, fallback={})
        if not isinstance(primary_tags, dict):
            primary_tags = {}
        authoritative_mb_rg = str(musicbrainz_release_group_id or "").strip()
        if authoritative_mb_rg:
            tagged_mb_rg = str(
                primary_tags.get("musicbrainz_releasegroupid")
                or primary_tags.get("musicbrainz_release_group_id")
                or ""
            ).strip()
            if tagged_mb_rg and tagged_mb_rg != authoritative_mb_rg:
                primary_tags.pop("musicbrainz_releaseid", None)
                primary_tags.pop("musicbrainz_release_id", None)
                primary_tags.pop("musicbrainz_albumid", None)
                primary_tags.pop("musicbrainz_id", None)
            primary_tags["musicbrainz_releasegroupid"] = authoritative_mb_rg
        authoritative_discogs = str(discogs_release_id or "").strip()
        if authoritative_discogs:
            primary_tags["discogs_release_id"] = authoritative_discogs
        authoritative_lastfm = str(lastfm_album_mbid or "").strip()
        if authoritative_lastfm:
            primary_tags["lastfm_album_mbid"] = authoritative_lastfm
        authoritative_bandcamp = str(bandcamp_album_url or "").strip()
        if authoritative_bandcamp:
            primary_tags["bandcamp_album_url"] = authoritative_bandcamp
        edition_payload = dict(primary_tags)
        edition_payload.setdefault("musicbrainz_id", str(musicbrainz_release_group_id or "").strip())
        edition_payload.setdefault("discogs_release_id", str(discogs_release_id or "").strip())
        edition_payload.setdefault("lastfm_album_mbid", str(lastfm_album_mbid or "").strip())
        edition_payload.setdefault("bandcamp_album_url", str(bandcamp_album_url or "").strip())
        edition_payload.setdefault("metadata_source", str(metadata_source or "").strip())
        for idx, r in enumerate(track_rows, start=1):
            tid = int(r[0] or 0)
            raw_title = str(r[1] or "").strip()
            disc_num = int(r[2] or 0) or 1
            track_num = int(r[3] or 0) or idx
            file_path = str(r[10] or "").strip()
            display = _track_display_fields_from_sources(
                raw_title=raw_title,
                file_path=file_path,
                fallback_disc=disc_num,
                fallback_track=track_num,
            )
            title = str(display.get("display_title") or raw_title or f"Track {idx}").strip()
            disc_num = int(display.get("display_disc_num") or disc_num or 1)
            track_num = int(display.get("display_track_num") or track_num or idx)
            disc_label = str(display.get("display_disc_label") or "").strip()
            dur = int(r[4] or 0)
            t_fmt = (r[5] or "").strip()
            bitrate = int(r[6] or 0)
            sample_rate = int(r[7] or 0)
            bit_depth = int(r[8] or 0)
            size_bytes = int(r[9] or 0)

            feat = ""
            try:
                m = re.search(r"\\b(?:feat\\.?|ft\\.?|featuring)\\s+([^\\)\\]\\-]+)", title, flags=re.IGNORECASE)
                if m:
                    feat = str(m.group(1) or "").strip()
            except Exception:
                feat = ""

            raw_tracks.append(
                {
                    "track_id": tid,
                    "title": title,
                    "disc_num": disc_num,
                    "track_num": track_num,
                    "disc_label": disc_label,
                    "duration_sec": dur,
                    "format": t_fmt,
                    "bitrate": bitrate,
                    "sample_rate": sample_rate,
                    "bit_depth": bit_depth,
                    "file_size_bytes": size_bytes,
                    "file_path": file_path,
                    "featured": feat,
                    "file_url": _browser_api_url(f"/api/library/track/{tid}/stream") if tid > 0 else "",
                }
            )
        tracks = _display_tracks_with_provider_overlay(
            raw_tracks,
            artist_name=str(artist_name or ""),
            album_title=str(album_title or ""),
            metadata_source=str(metadata_source or ""),
            musicbrainz_release_group_id=str(musicbrainz_release_group_id or ""),
            discogs_release_id=str(discogs_release_id or ""),
            lastfm_album_mbid=str(lastfm_album_mbid or ""),
            bandcamp_album_url=str(bandcamp_album_url or ""),
            edition_payload=edition_payload,
            cache_only=True,
        )

        # Best-effort total duration (keep existing album value when present).
        try:
            total_duration_sec = int(total_duration_sec or 0)
        except Exception:
            total_duration_sec = 0
        if total_duration_sec <= 0 and tracks:
            total_duration_sec = sum(int(t.get("duration_sec") or 0) for t in tracks)
        album_sample_rate = int(album_sample_rate or 0)
        album_bit_depth = int(album_bit_depth or 0)
        if (album_sample_rate <= 0 or album_bit_depth <= 0) and tracks:
            derived_sample_rate, derived_bit_depth = _representative_album_audio_profile(tracks)
            if album_sample_rate <= 0:
                album_sample_rate = int(derived_sample_rate or 0)
            if album_bit_depth <= 0:
                album_bit_depth = int(derived_bit_depth or 0)

        genres_list = _merge_album_genre_lists(
            genre or "",
            str(prof.get("tags_json") or "[]"),
        )

        # Always expose the cover endpoint; it can still resolve folder/embedded art lazily.
        cover_url = _browser_api_url(f"/api/library/files/album/{album_id}/cover?size=640")
        metadata_source_norm = _normalize_identity_provider(str(metadata_source or ""))
        metadata_ref = ""
        if metadata_source_norm == "musicbrainz":
            metadata_ref = str(musicbrainz_release_group_id or "").strip()
        elif metadata_source_norm == "discogs":
            metadata_ref = str(discogs_release_id or "").strip()
        elif metadata_source_norm == "lastfm":
            metadata_ref = str(lastfm_album_mbid or "").strip()
        elif metadata_source_norm == "bandcamp":
            metadata_ref = str(bandcamp_album_url or "").strip()
        metadata_source_url = _provider_reference_link(
            provider=metadata_source_norm,
            ref=metadata_ref,
            artist_name=str(artist_name or ""),
            album_title=str(album_title or ""),
        ) if metadata_source_norm else None
        classical_payload = _classical_display_payload(
            primary_tags,
            fallback_title=str(album_title or ""),
            fallback_artist=str(artist_name or ""),
        )
        composer_names = _safe_json_load(composer_names_json, fallback=[])
        if not isinstance(composer_names, list):
            composer_names = []
        classical_payload = _files_apply_canonical_composers_to_classical_payload(
            classical_payload,
            composer_names,
        )
        artist_display_name = _files_album_display_artist_name(
            artist_name=str(artist_name or ""),
            classical_payload=classical_payload,
            limit=3,
        )

        payload = {
            "album_id": int(album_id),
            "title": (album_title or "").strip(),
            "year": int(year or 0) if int(year or 0) > 0 else None,
            "created_at": int(album_created_at or 0) or None,
            "updated_at": int(album_updated_at or 0) or None,
            "date_text": (date_text or "").strip(),
            "genre": "; ".join(genres_list) if genres_list else (genre or "").strip(),
            "genres": genres_list,
            "label": (label or "").strip(),
            "format": (fmt or "").strip(),
            "is_lossless": bool(is_lossless),
            "sample_rate": album_sample_rate or None,
            "bit_depth": album_bit_depth or None,
            "track_count": int(track_count or 0),
            "total_duration_sec": int(total_duration_sec or 0),
            "has_cover": bool(has_cover_effective),
            "cover_url": cover_url,
            "bandcamp_album_url": (bandcamp_album_url or "").strip() or None,
            "metadata_source": (metadata_source_norm or "").strip() or None,
            "metadata_source_url": metadata_source_url,
            "artist_id": int(artist_id or 0),
            "artist_name": artist_display_name,
            "classical": classical_payload,
            "review": {
                "description": str(prof.get("description") or "").strip(),
                "short_description": str(prof.get("short_description") or "").strip(),
                "source": str(prof.get("source") or "").strip(),
                "updated_at": int(prof.get("updated_at") or 0),
            },
            "ratings": {
                "user_rating": int(user_rating or 0) if user_rating else None,
                "user_review_text": user_review_text or None,
                "user_review_updated_at": int(user_review_updated_at or 0) or None,
                "public_rating": float(prof.get("public_rating")) if prof.get("public_rating") is not None else None,
                "public_rating_votes": int(prof.get("public_rating_votes") or 0),
                "public_rating_source": str(prof.get("public_rating_source") or "").strip() or None,
                "heat_score": float(prof.get("heat_score")) if prof.get("heat_score") is not None else None,
                "heat_label": None,
                "signals": {
                    "discogs_have_count": int(prof.get("discogs_have_count") or 0),
                    "discogs_want_count": int(prof.get("discogs_want_count") or 0),
                    "bandcamp_supporter_count": int(prof.get("bandcamp_supporter_count") or 0),
                    "bandcamp_supporter_comments": _normalize_bandcamp_supporter_comments(prof.get("bandcamp_supporter_comments")),
                    "lastfm_scrobbles": int(prof.get("lastfm_scrobbles") or 0),
                    "lastfm_listeners": int(prof.get("lastfm_listeners") or 0),
                },
            },
            "tracks": tracks,
        }
        missing_duration = any(int(t.get("duration_sec") or 0) <= 0 for t in tracks)
        payload_ttl = 20 if missing_duration else 300
        _files_cache_set_json(cache_key, payload, ttl=payload_ttl)
        return jsonify(payload)
    finally:
        conn.close()


def _api_library_artist_similar_impl(artist_id):
    """Get similar artists for a given artist (providers + local genre fallback)."""
    if _get_library_mode() == "files":
        ok, err = _ensure_files_index_ready()
        if not ok:
            return jsonify({"error": err or "Files index unavailable"}), 503
        conn = _files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, name, name_norm FROM files_artists WHERE id = %s", (int(artist_id),))
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Artist not found"}), 404
                artist_name = (row[1] or "").strip()
                artist_norm = (row[2] or "").strip() or _norm_artist_key(artist_name)

                cur.execute(
                    """
                    SELECT COALESCE(similar_json, ''), updated_at
                    FROM files_artist_profiles
                    WHERE name_norm = %s
                    """,
                    (artist_norm,),
                )
                prof_row = cur.fetchone()

            base_url = request.url_root.rstrip("/")

            # 1) Prefer already-enriched similar artists from cached profiles.
            if prof_row and str(prof_row[0] or "").strip():
                try:
                    stored = json.loads(prof_row[0] or "[]")
                except Exception:
                    stored = []
                if isinstance(stored, list) and stored:
                    patched = _files_attach_similar_artist_refs(conn, stored, base_url)
                    # Warm missing images asynchronously (keeps API fast; UI can refetch).
                    try:
                        missing = [
                            str(it.get("name") or "").strip()
                            for it in (patched or [])
                            if isinstance(it, dict)
                            and str(it.get("name") or "").strip()
                            and not str(it.get("image_url") or "").strip()
                        ]
                        if missing:
                            _enqueue_files_similar_images_warm(artist_norm, missing[:12])
                    except Exception:
                        pass
                    return jsonify(
                        {
                            "artist_id": int(artist_id),
                            "source": "profile",
                            "similar_artists": patched[:20],
                        }
                    )

            # 2) Try Last.fm (best UX: includes similar artists + images).
            lastfm_info = _fetch_lastfm_artist_info(artist_name) or {}
            lf_sim = lastfm_info.get("similar") or []
            if isinstance(lf_sim, list) and lf_sim:
                # Best-effort: persist similar list (do not overwrite a good bio).
                try:
                    existing = _files_get_artist_profile_cached(artist_name, artist_norm) or {}
                    next_profile = dict(existing) if isinstance(existing, dict) else {}
                    if not next_profile.get("bio") and str(lastfm_info.get("bio") or "").strip():
                        next_profile["bio"] = str(lastfm_info.get("bio") or "").strip()
                    if not next_profile.get("short_bio") and str(lastfm_info.get("short_bio") or "").strip():
                        next_profile["short_bio"] = str(lastfm_info.get("short_bio") or "").strip()
                    if lastfm_info.get("tags"):
                        next_profile["tags"] = lastfm_info.get("tags") or []
                    next_profile["similar"] = lf_sim
                    if not (next_profile.get("source") or "").strip():
                        next_profile["source"] = "lastfm"
                    with conn.transaction():
                        _files_upsert_artist_profile(conn, artist_norm, artist_name, next_profile)
                except Exception:
                    pass
                # Best-effort: cache similar artists images so UI stays pretty offline.
                try:
                    for sim in lf_sim[:12]:
                        if not isinstance(sim, dict):
                            continue
                        sname = (sim.get("name") or "").strip()
                        surl = (sim.get("image_url") or "").strip()
                        if not sname or not surl:
                            continue
                        if _is_probably_placeholder_artist_image_url(surl):
                            continue
                        with conn.transaction():
                            _files_cache_external_artist_image(
                                conn,
                                artist_name=sname,
                                artist_norm=_files_resolve_artist_cache_name_norm(
                                    conn,
                                    artist_name=sname,
                                ),
                                provider="lastfm",
                                image_url=surl,
                                max_px=640,
                            )
                except Exception:
                    pass
                patched = _files_attach_similar_artist_refs(conn, lf_sim, base_url)
                # Warm missing images asynchronously (Last.fm similar sometimes lacks usable images).
                try:
                    missing = [
                        str(it.get("name") or "").strip()
                        for it in (patched or [])
                        if isinstance(it, dict)
                        and str(it.get("name") or "").strip()
                        and not str(it.get("image_url") or "").strip()
                    ]
                    if missing:
                        _enqueue_files_similar_images_warm(artist_norm, missing[:12])
                except Exception:
                    pass
                return jsonify(
                    {
                        "artist_id": int(artist_id),
                        "source": "lastfm",
                        "similar_artists": patched[:20],
                    }
                )

            # 3) Try MusicBrainz, but only when identity match is sane (avoid nonsense).
            mbid = ""
            mb_sim: list[dict] = []
            if USE_MUSICBRAINZ:
                try:
                    search_result = musicbrainzngs.search_artists(artist=artist_name, limit=3)
                    artist_list = search_result.get("artist-list") or []
                    for cand in artist_list[:3]:
                        if not isinstance(cand, dict):
                            continue
                        cand_name = (cand.get("name") or "").strip()
                        if cand_name and _provider_identity_text_score(artist_name, cand_name) >= 0.78:
                            mbid = (cand.get("id") or "").strip()
                            break
                except Exception:
                    mbid = ""
                if mbid:
                    try:
                        mb_sim = get_similar_artists_mb(mbid) or []
                    except Exception:
                        mb_sim = []
            if isinstance(mb_sim, list) and mb_sim:
                patched = _files_attach_similar_artist_refs(conn, mb_sim, base_url)
                return jsonify(
                    {
                        "artist_id": int(artist_id),
                        "artist_mbid": mbid,
                        "source": "musicbrainz",
                        "similar_artists": patched[:20],
                    }
                )

            # 4) Local fallback: overlap inferred genres in the local DB.
            genre_sim = _files_similar_artists_by_genre(conn, int(artist_id), limit=20) or []
            if genre_sim:
                genre_sim = _files_attach_similar_artist_refs(conn, genre_sim, base_url)
            return jsonify(
                {
                    "artist_id": int(artist_id),
                    "source": "genre" if genre_sim else "none",
                    "similar_artists": (genre_sim[:20] if isinstance(genre_sim, list) else []),
                }
            )
        finally:
            conn.close()
    return jsonify({"error": "Files mode required"}), 400


def _api_library_album_match_detail_impl(album_id: int):
    """Explain where album match/metadata/artwork came from + manual audit history."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    album_id = int(album_id or 0)
    if album_id <= 0:
        return jsonify({"error": "Invalid album id"}), 400
    # Keep modal fast by default: do not perform live provider lookups unless explicitly requested.
    live_crosscheck = str(request.args.get("crosscheck", "0")).strip().lower() in {"1", "true", "yes", "on"}

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    alb.id,
                    alb.title,
                    alb.title_norm,
                    COALESCE(alb.folder_path, ''),
                    COALESCE(alb.year, 0),
                    COALESCE(alb.track_count, 0),
                    alb.strict_match_verified,
                    COALESCE(alb.strict_match_provider, ''),
                    COALESCE(alb.strict_reject_reason, ''),
                    COALESCE(alb.strict_tracklist_score, 0.0),
                    COALESCE(alb.musicbrainz_release_group_id, ''),
                    COALESCE(alb.musicbrainz_release_id, ''),
                    COALESCE(alb.discogs_release_id, ''),
                    COALESCE(alb.lastfm_album_mbid, ''),
                    COALESCE(alb.bandcamp_album_url, ''),
                    COALESCE(alb.metadata_source, ''),
                    alb.has_cover,
                    COALESCE(alb.cover_path, ''),
                    COALESCE(alb.primary_tags_json, '{}'),
                    EXTRACT(EPOCH FROM alb.updated_at)::BIGINT,
                    art.id,
                    COALESCE(art.name, ''),
                    COALESCE(art.name_norm, ''),
                    art.has_image,
                    COALESCE(art.image_path, ''),
                    EXTRACT(EPOCH FROM art.updated_at)::BIGINT
                FROM files_albums alb
                JOIN files_artists art ON art.id = alb.artist_id
                WHERE alb.id = %s
                LIMIT 1
                """,
                (album_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Album not found"}), 404

            (
                _aid,
                album_title,
                title_norm,
                folder_path,
                year,
                track_count,
                strict_verified,
                strict_provider_raw,
                strict_reason,
                strict_score,
                mbid,
                musicbrainz_release_id,
                discogs_release_id,
                lastfm_album_mbid,
                bandcamp_album_url,
                metadata_source_raw,
                has_cover,
                cover_path_raw,
                primary_tags_json,
                album_updated_at,
                artist_id,
                artist_name,
                artist_norm,
                artist_has_image,
                artist_image_path,
                artist_updated_at,
            ) = row

            cur.execute(
                """
                SELECT COALESCE(title, '')
                FROM files_tracks
                WHERE album_id = %s
                ORDER BY disc_num ASC, track_num ASC, id ASC
                """,
                (album_id,),
            )
            local_track_titles = [str(r[0] or "").strip() for r in cur.fetchall() if str(r[0] or "").strip()]

            cur.execute(
                """
                SELECT COALESCE(source, ''), EXTRACT(EPOCH FROM updated_at)::BIGINT
                FROM files_album_profiles
                WHERE artist_norm = %s AND title_norm = %s
                LIMIT 1
                """,
                (artist_norm, title_norm),
            )
            profile_row = cur.fetchone()

            cur.execute(
                """
                SELECT COALESCE(source, ''), EXTRACT(EPOCH FROM updated_at)::BIGINT
                FROM files_artist_profiles
                WHERE name_norm = %s
                LIMIT 1
                """,
                (artist_norm,),
            )
            artist_profile_row = cur.fetchone()

            cur.execute(
                """
                SELECT COALESCE(provider, ''), COALESCE(image_url, ''), COALESCE(image_path, ''), EXTRACT(EPOCH FROM updated_at)::BIGINT
                FROM files_external_artist_images
                WHERE name_norm = %s
                LIMIT 1
                """,
                (artist_norm,),
            )
            ext_artist_row = cur.fetchone()

            cur.execute(
                """
                SELECT
                    id,
                    COALESCE(run_kind, ''),
                    COALESCE(status, ''),
                    COALESCE(match_type, ''),
                    confidence,
                    ai_used,
                    ai_confidence,
                    COALESCE(provider_used, ''),
                    COALESCE(summary, ''),
                    COALESCE(details_json, '{}'),
                    EXTRACT(EPOCH FROM created_at)::BIGINT
                FROM files_match_audit
                WHERE (folder_path IS NOT NULL AND folder_path <> '' AND folder_path = %s)
                   OR album_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 20
                """,
                (str(folder_path or "").strip(), album_id),
            )
            audit_rows = cur.fetchall()

        tags_map = _safe_json_load(primary_tags_json, fallback={})
        if not isinstance(tags_map, dict):
            tags_map = {}
        pmda_id = str(tags_map.get(PMDA_ID_TAG) or "").strip() or None

        strict_provider = _normalize_identity_provider(str(strict_provider_raw or ""))
        metadata_source = _normalize_identity_provider(str(metadata_source_raw or ""))
        has_identity_hint = bool(
            strict_verified
            or metadata_source
            or str(mbid or "").strip()
            or str(discogs_release_id or "").strip()
            or str(lastfm_album_mbid or "").strip()
            or str(bandcamp_album_url or "").strip()
        )
        match_type = _match_type_from_flags(
            strict_match_verified=bool(strict_verified),
            has_identity_hint=has_identity_hint,
        )
        confidence = None
        try:
            strict_score_f = float(strict_score or 0.0)
            if strict_score_f > 0.0:
                confidence = max(0.0, min(1.0, strict_score_f))
        except Exception:
            confidence = None
        if confidence is None and bool(strict_verified):
            confidence = 1.0

        selected_provider = (
            strict_provider
            or metadata_source
            or ("musicbrainz" if str(mbid or "").strip() else "")
            or ("discogs" if str(discogs_release_id or "").strip() else "")
            or ("lastfm" if str(lastfm_album_mbid or "").strip() else "")
            or ("bandcamp" if str(bandcamp_album_url or "").strip() else "")
        )
        selected_provider = _normalize_identity_provider(selected_provider)

        links = _album_match_links(
            mbid=str(mbid or "").strip(),
            musicbrainz_release_id=str(musicbrainz_release_id or "").strip(),
            discogs_release_id=str(discogs_release_id or "").strip(),
            lastfm_album_mbid=str(lastfm_album_mbid or "").strip(),
            bandcamp_album_url=str(bandcamp_album_url or "").strip(),
            artist_name=str(artist_name or ""),
            album_title=str(album_title or ""),
        )

        cover_origin = "none"
        cover_path = str(cover_path_raw or "").strip()
        if bool(has_cover):
            if cover_path:
                try:
                    cp = path_for_fs_access(Path(cover_path))
                    if cp.exists() and cp.is_file():
                        cover_origin = "media_cache" if _is_media_cache_file(cp, kind="album") else "local_file"
                    else:
                        cover_origin = "virtual"
                except Exception:
                    cover_origin = "virtual"
            else:
                cover_origin = "virtual"

        artist_image_provider = ""
        artist_image_source_url = None
        artist_image_mode = "none"
        if bool(artist_has_image) and str(artist_image_path or "").strip():
            artist_image_mode = "local"
            artist_image_provider = "local"
        elif ext_artist_row:
            ext_provider = _normalize_identity_provider(str(ext_artist_row[0] or ""))
            ext_image_url = str(ext_artist_row[1] or "").strip()
            if ext_provider:
                artist_image_provider = ext_provider
            if ext_image_url:
                artist_image_source_url = ext_image_url
            artist_image_mode = "external_cached"

        audits = [_serialize_match_audit_row(r) for r in audit_rows]
        latest_audit = audits[0] if audits else None
        attempts = []
        latest_result = {}
        if latest_audit:
            det = latest_audit.get("details") or {}
            if isinstance(det, dict):
                raw_attempts = det.get("provider_attempts")
                if isinstance(raw_attempts, list):
                    attempts = [a for a in raw_attempts if isinstance(a, dict)]
                latest_result = det.get("result") if isinstance(det.get("result"), dict) else {}

        crosscheck: list[dict[str, Any]] = []
        if live_crosscheck:
            try:
                crosscheck = _build_album_provider_crosscheck(
                    artist_name=str(artist_name or ""),
                    album_title=str(album_title or ""),
                    local_track_titles=list(local_track_titles or []),
                    selected_provider=selected_provider,
                    known_mbid=str(mbid or "").strip(),
                    known_discogs_release_id=str(discogs_release_id or "").strip(),
                    known_lastfm_album_mbid=str(lastfm_album_mbid or "").strip(),
                    known_bandcamp_album_url=str(bandcamp_album_url or "").strip(),
                )
            except Exception:
                crosscheck = []

        if not attempts:
            attempts = []
            if crosscheck:
                for row in crosscheck:
                    if not isinstance(row, dict):
                        continue
                    attempted = bool(row.get("attempted"))
                    selected = bool(row.get("selected"))
                    if not (attempted or selected):
                        continue
                    attempts.append(
                        {
                            "provider": _normalize_identity_provider(str(row.get("provider") or "")),
                            "label": _match_provider_label(str(row.get("provider") or "")),
                            "attempted": attempted,
                            "selected": selected,
                            "id": str(row.get("provider_id") or "").strip() or None,
                            "url": str(row.get("source_url") or "").strip() or None,
                            "notes": [],
                        }
                    )
            else:
                provider_refs = {
                    "musicbrainz": str(mbid or "").strip(),
                    "discogs": str(discogs_release_id or "").strip(),
                    "lastfm": str(lastfm_album_mbid or "").strip(),
                    "bandcamp": str(bandcamp_album_url or "").strip(),
                }
                for provider in _MATCH_PROVIDER_ORDER:
                    ref = provider_refs.get(provider, "")
                    selected = bool(provider == selected_provider)
                    attempted = bool(ref) or selected
                    if not attempted:
                        continue
                    attempts.append(
                        {
                            "provider": provider,
                            "label": _match_provider_label(provider),
                            "attempted": True,
                            "selected": selected,
                            "id": ref or None,
                            "url": _provider_reference_link(
                                provider=provider,
                                ref=ref,
                                artist_name=str(artist_name or ""),
                                album_title=str(album_title or ""),
                            ),
                            "notes": [],
                        }
                    )

        pmda_matched = bool(has_identity_hint)
        pmda_cover = bool(has_cover)
        pmda_artist_image = bool(artist_has_image) or bool(ext_artist_row)
        pmda_complete = bool(pmda_matched and pmda_cover and pmda_artist_image)
        pmda_match_provider = selected_provider
        pmda_cover_provider = _normalize_identity_provider(str(latest_result.get("pmda_cover_provider") or ""))
        if not pmda_cover_provider:
            if cover_origin in {"media_cache", "local_file"}:
                pmda_cover_provider = "local"
            elif selected_provider and pmda_cover:
                pmda_cover_provider = selected_provider
        pmda_artist_provider = _normalize_identity_provider(str(latest_result.get("pmda_artist_provider") or ""))
        if not pmda_artist_provider:
            pmda_artist_provider = _normalize_identity_provider(artist_image_provider) or None

        ai_used_manual = bool(latest_audit.get("ai_used")) if isinstance(latest_audit, dict) else False
        ai_conf_manual = (
            int(latest_audit.get("ai_confidence"))
            if isinstance(latest_audit, dict) and latest_audit.get("ai_confidence") is not None
            else None
        )
        ai_used_crosscheck = any(bool(r.get("ai_used")) for r in (crosscheck or []) if isinstance(r, dict))
        ai_used = bool(ai_used_manual or ai_used_crosscheck)
        ai_source = None
        if ai_used_manual:
            ai_source = "manual_history"
        elif ai_used_crosscheck:
            ai_source = "provider_crosscheck"

        links_map: dict[str, dict[str, Any]] = {}
        for link in links:
            if not isinstance(link, dict):
                continue
            href = str(link.get("url") or "").strip()
            if not href:
                continue
            links_map[href] = {
                "provider": _normalize_identity_provider(str(link.get("provider") or "")),
                "label": str(link.get("label") or "").strip() or href,
                "url": href,
                "release_title": str(link.get("release_title") or "").strip() or None,
                "release_artist": str(link.get("release_artist") or "").strip() or None,
                "release_year": int(link.get("release_year")) if str(link.get("release_year") or "").strip().isdigit() else None,
                "provider_ref": str(link.get("provider_ref") or "").strip() or None,
            }
        for row in crosscheck:
            if not isinstance(row, dict):
                continue
            href = str(row.get("source_url") or "").strip()
            if not href:
                continue
            p = _normalize_identity_provider(str(row.get("provider") or ""))
            title_val = str(row.get("title") or "").strip() or None
            artist_val = str(row.get("artist") or "").strip() or None
            year_val: int | None = None
            try:
                year_raw = int(row.get("year") or 0)
                if year_raw > 1800:
                    year_val = year_raw
            except Exception:
                year_val = None
            provider_ref = str(row.get("provider_id") or "").strip() or None
            if href in links_map:
                existing = links_map.get(href) or {}
                if title_val and not str(existing.get("release_title") or "").strip():
                    existing["release_title"] = title_val
                if artist_val and not str(existing.get("release_artist") or "").strip():
                    existing["release_artist"] = artist_val
                if year_val and not existing.get("release_year"):
                    existing["release_year"] = year_val
                if provider_ref and not str(existing.get("provider_ref") or "").strip():
                    existing["provider_ref"] = provider_ref
                if p and not str(existing.get("provider") or "").strip():
                    existing["provider"] = p
                links_map[href] = existing
                continue
            links_map[href] = {
                "provider": p,
                "label": f"{_match_provider_label(p)} source",
                "url": href,
                "release_title": title_val,
                "release_artist": artist_val,
                "release_year": year_val,
                "provider_ref": provider_ref,
            }
        links = list(links_map.values())

        alt_covers: list[dict[str, Any]] = []
        seen_cover_urls: set[str] = set()
        for row in crosscheck:
            if not isinstance(row, dict):
                continue
            curl = str(row.get("cover_url") or "").strip()
            if not curl or curl in seen_cover_urls:
                continue
            seen_cover_urls.add(curl)
            p = _normalize_identity_provider(str(row.get("provider") or ""))
            alt_covers.append(
                {
                    "provider": p,
                    "label": _match_provider_label(p),
                    "cover_url": curl,
                    "source_url": str(row.get("source_url") or "").strip() or None,
                    "selected": bool(row.get("selected")) or (p == pmda_cover_provider),
                }
            )

        versions_info = {
            "has_alternatives": False,
            "provider": None,
            "count": 0,
            "source_url": None,
            "items": [],
        }
        for row in crosscheck:
            if not isinstance(row, dict):
                continue
            versions = row.get("versions") if isinstance(row.get("versions"), list) else []
            versions_count = int(row.get("versions_count") or 0)
            if versions_count <= 0 and versions:
                versions_count = int(len(versions))
            if versions_count > 1 or len(versions) > 1:
                versions_info = {
                    "has_alternatives": True,
                    "provider": _normalize_identity_provider(str(row.get("provider") or "")) or None,
                    "count": int(versions_count),
                    "source_url": str(row.get("source_url") or "").strip() or None,
                    "items": versions[:20],
                }
                break

        profile_allowed = bool(
            bool(strict_verified)
            or (bool(getattr(sys.modules[__name__], "USE_AI_FOR_SOFT_MATCH_PROFILES", False)) and has_identity_hint)
        )
        profile_source = str(profile_row[0] or "").strip() if (profile_row and profile_allowed) else ""
        profile_updated = int(profile_row[1] or 0) if (profile_row and profile_allowed) else 0
        artist_profile_source = str(artist_profile_row[0] or "").strip() if artist_profile_row else ""
        artist_profile_updated = int(artist_profile_row[1] or 0) if artist_profile_row else 0

        payload = {
            "album_id": int(album_id),
            "album_title": str(album_title or "").strip(),
            "artist_id": int(artist_id or 0),
            "artist_name": str(artist_name or "").strip(),
            "year": int(year or 0) if int(year or 0) > 0 else None,
            "track_count": int(track_count or 0),
            "match_type": match_type,
            "confidence": confidence,
            "decision": {
                "strict_match_verified": bool(strict_verified),
                "strict_match_provider": strict_provider or None,
                "strict_reject_reason": str(strict_reason or "").strip() or None,
                "strict_tracklist_score": float(strict_score or 0.0),
                "selected_provider": selected_provider or None,
                "metadata_source": metadata_source or None,
            },
            "pmda": {
                "id": pmda_id,
                "matched": bool(pmda_matched),
                "cover": bool(pmda_cover),
                "artist_image": bool(pmda_artist_image),
                "complete": bool(pmda_complete),
                "match_provider": pmda_match_provider or None,
                "cover_provider": pmda_cover_provider or None,
                "artist_provider": pmda_artist_provider or None,
            },
            "ai": {
                "used": bool(ai_used),
                "confidence": ai_conf_manual,
                "source": ai_source,
            },
            "identity": {
                "musicbrainz_release_group_id": str(mbid or "").strip() or None,
                "discogs_release_id": str(discogs_release_id or "").strip() or None,
                "lastfm_album_mbid": str(lastfm_album_mbid or "").strip() or None,
                "bandcamp_album_url": str(bandcamp_album_url or "").strip() or None,
            },
            "cover": {
                "has_cover": bool(has_cover),
                "provider": pmda_cover_provider or None,
                "origin": cover_origin,
                "path": cover_path or None,
                "url": f"{request.url_root.rstrip('/')}/api/library/files/album/{album_id}/cover?size=640",
            },
            "artist_image": {
                "has_image": bool(artist_has_image) or bool(ext_artist_row),
                "provider": _normalize_identity_provider(artist_image_provider) or None,
                "mode": artist_image_mode,
                "source_url": artist_image_source_url,
                "url": (
                    f"{request.url_root.rstrip('/')}/api/library/files/artist/{int(artist_id)}/image?size=320"
                    if (bool(artist_has_image) or bool(ext_artist_row))
                    else None
                ),
            },
            "description": {
                "album_profile_source": profile_source or None,
                "album_profile_updated_at": profile_updated or None,
                "artist_profile_source": artist_profile_source or None,
                "artist_profile_updated_at": artist_profile_updated or None,
                "soft_match_ai_auto_enabled": bool(getattr(sys.modules[__name__], "USE_AI_FOR_SOFT_MATCH_PROFILES", False)),
            },
            "providers_order": list(_MATCH_PROVIDER_ORDER),
            "provider_crosscheck_live": bool(live_crosscheck),
            "provider_attempts": attempts,
            "provider_crosscheck": crosscheck,
            "links": links,
            "alternative_covers": alt_covers,
            "versions": versions_info,
            "latest_manual_run": latest_audit,
            "history": audits,
            "updated_at": int(album_updated_at or 0) or int(artist_updated_at or 0),
        }
        return jsonify(payload)
    finally:
        conn.close()


def _api_library_artist_match_detail_impl(artist_id: int):
    """Explain artist-level metadata sources + album-level match rollup."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    artist_id = int(artist_id or 0)
    if artist_id <= 0:
        return jsonify({"error": "Invalid artist id"}), 400
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    COALESCE(name, ''),
                    COALESCE(name_norm, ''),
                    has_image,
                    COALESCE(image_path, ''),
                    COALESCE(album_count, 0),
                    EXTRACT(EPOCH FROM updated_at)::BIGINT
                FROM files_artists
                WHERE id = %s
                LIMIT 1
                """,
                (artist_id,),
            )
            arow = cur.fetchone()
            if not arow:
                return jsonify({"error": "Artist not found"}), 404
            artist_name = str(arow[1] or "")
            artist_norm = str(arow[2] or "")

            cur.execute(
                """
                SELECT COALESCE(source, ''), EXTRACT(EPOCH FROM updated_at)::BIGINT
                FROM files_artist_profiles
                WHERE name_norm = %s
                LIMIT 1
                """,
                (artist_norm,),
            )
            profile_row = cur.fetchone()

            cur.execute(
                """
                SELECT COALESCE(provider, ''), COALESCE(image_url, ''), COALESCE(image_path, ''), EXTRACT(EPOCH FROM updated_at)::BIGINT
                FROM files_external_artist_images
                WHERE name_norm = %s
                LIMIT 1
                """,
                (artist_norm,),
            )
            ext_artist_row = cur.fetchone()

            cur.execute(
                """
                SELECT
                    id,
                    COALESCE(title, ''),
                    COALESCE(year, 0),
                    strict_match_verified,
                    COALESCE(strict_match_provider, ''),
                    COALESCE(strict_reject_reason, ''),
                    COALESCE(strict_tracklist_score, 0.0),
                    COALESCE(musicbrainz_release_group_id, ''),
                    COALESCE(discogs_release_id, ''),
                    COALESCE(lastfm_album_mbid, ''),
                    COALESCE(bandcamp_album_url, ''),
                    COALESCE(metadata_source, ''),
                    has_cover,
                    COALESCE(cover_path, ''),
                    COALESCE(primary_tags_json, '{}'),
                    EXTRACT(EPOCH FROM updated_at)::BIGINT
                FROM files_albums
                WHERE id IN (
                    SELECT DISTINCT album_id
                    FROM files_artist_album_links
                    WHERE artist_id = %s
                )
                ORDER BY COALESCE(year, 0) DESC, title ASC
                """,
                (artist_id,),
            )
            album_rows = cur.fetchall()

            album_ids = [int(r[0] or 0) for r in album_rows if int(r[0] or 0) > 0]
            audit_rows: list[tuple[Any, ...]] = []
            if album_ids:
                placeholders = ",".join(["%s"] * len(album_ids))
                cur.execute(
                    f"""
                    SELECT
                        id,
                        COALESCE(run_kind, ''),
                        COALESCE(status, ''),
                        COALESCE(match_type, ''),
                        confidence,
                        ai_used,
                        ai_confidence,
                        COALESCE(provider_used, ''),
                        COALESCE(summary, ''),
                        COALESCE(details_json, '{{}}'),
                        EXTRACT(EPOCH FROM created_at)::BIGINT,
                        COALESCE(album_id, 0)
                    FROM files_match_audit
                    WHERE album_id IN ({placeholders})
                    ORDER BY created_at DESC, id DESC
                    LIMIT 120
                    """,
                    tuple(album_ids),
                )
                audit_rows = cur.fetchall()

        album_reports: list[dict[str, Any]] = []
        matched_count = 0
        soft_count = 0
        no_count = 0
        for row in album_rows:
            (
                album_id_row,
                album_title,
                year,
                strict_verified,
                strict_provider_raw,
                strict_reason,
                strict_score,
                mbid,
                discogs_release_id,
                lastfm_album_mbid,
                bandcamp_album_url,
                metadata_source_raw,
                has_cover,
                cover_path_raw,
                primary_tags_json,
                updated_at,
            ) = row
            strict_provider = _normalize_identity_provider(str(strict_provider_raw or ""))
            metadata_source = _normalize_identity_provider(str(metadata_source_raw or ""))
            has_identity_hint = bool(
                strict_verified
                or metadata_source
                or str(mbid or "").strip()
                or str(discogs_release_id or "").strip()
                or str(lastfm_album_mbid or "").strip()
                or str(bandcamp_album_url or "").strip()
            )
            match_type = _match_type_from_flags(
                strict_match_verified=bool(strict_verified),
                has_identity_hint=has_identity_hint,
            )
            if match_type == "MATCH":
                matched_count += 1
            elif match_type == "SOFT_MATCH":
                soft_count += 1
            else:
                no_count += 1
            confidence = None
            try:
                score_f = float(strict_score or 0.0)
                if score_f > 0.0:
                    confidence = max(0.0, min(1.0, score_f))
            except Exception:
                confidence = None
            if confidence is None and bool(strict_verified):
                confidence = 1.0
            selected_provider = (
                strict_provider
                or metadata_source
                or ("musicbrainz" if str(mbid or "").strip() else "")
                or ("discogs" if str(discogs_release_id or "").strip() else "")
                or ("lastfm" if str(lastfm_album_mbid or "").strip() else "")
                or ("bandcamp" if str(bandcamp_album_url or "").strip() else "")
            )
            links = _album_match_links(
                mbid=str(mbid or "").strip(),
                discogs_release_id=str(discogs_release_id or "").strip(),
                lastfm_album_mbid=str(lastfm_album_mbid or "").strip(),
                bandcamp_album_url=str(bandcamp_album_url or "").strip(),
                artist_name=artist_name,
                album_title=str(album_title or ""),
            )
            cover_origin = "none"
            cover_path = str(cover_path_raw or "").strip()
            if bool(has_cover):
                if cover_path:
                    try:
                        cp = path_for_fs_access(Path(cover_path))
                        if cp.exists() and cp.is_file():
                            cover_origin = "media_cache" if _is_media_cache_file(cp, kind="album") else "local_file"
                        else:
                            cover_origin = "virtual"
                    except Exception:
                        cover_origin = "virtual"
                else:
                    cover_origin = "virtual"
            album_reports.append(
                {
                    "album_id": int(album_id_row or 0),
                    "album_title": str(album_title or "").strip(),
                    "year": int(year or 0) if int(year or 0) > 0 else None,
                    "match_type": match_type,
                    "confidence": confidence,
                    "selected_provider": _normalize_identity_provider(selected_provider) or None,
                    "strict_match_verified": bool(strict_verified),
                    "strict_match_provider": strict_provider or None,
                    "strict_reject_reason": str(strict_reason or "").strip() or None,
                    "strict_tracklist_score": float(strict_score or 0.0),
                    "metadata_source": metadata_source or None,
                    "identity": {
                        "musicbrainz_release_group_id": str(mbid or "").strip() or None,
                        "discogs_release_id": str(discogs_release_id or "").strip() or None,
                        "lastfm_album_mbid": str(lastfm_album_mbid or "").strip() or None,
                        "bandcamp_album_url": str(bandcamp_album_url or "").strip() or None,
                    },
                    "cover": {
                        "has_cover": bool(has_cover),
                        "origin": cover_origin,
                        "url": f"{request.url_root.rstrip('/')}/api/library/files/album/{int(album_id_row)}/cover?size=320",
                    },
                    "links": links,
                    "updated_at": int(updated_at or 0),
                }
            )

        audits = []
        for r in audit_rows:
            obj = _serialize_match_audit_row(r[:11])
            obj["album_id"] = int(r[11] or 0)
            audits.append(obj)

        artist_image_mode = "none"
        artist_image_provider = None
        artist_image_source_url = None
        has_visual = bool(arow[3]) or bool(ext_artist_row)
        if bool(arow[3]) and str(arow[4] or "").strip():
            artist_image_mode = "local"
            artist_image_provider = "local"
        elif ext_artist_row:
            artist_image_mode = "external_cached"
            artist_image_provider = _normalize_identity_provider(str(ext_artist_row[0] or "")) or None
            artist_image_source_url = str(ext_artist_row[1] or "").strip() or None
        else:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1
                        FROM files_artist_album_links link
                        JOIN files_albums alb ON alb.id = link.album_id
                        WHERE link.artist_id = %s
                          AND COALESCE(alb.has_cover, FALSE) = TRUE
                        LIMIT 1
                        """,
                        (artist_id,),
                    )
                    has_visual = bool(cur.fetchone())
                if has_visual:
                    artist_image_mode = "album_cover_fallback"
                    artist_image_provider = "local"
            except Exception:
                has_visual = bool(arow[3]) or bool(ext_artist_row)

        payload = {
            "artist_id": int(arow[0] or 0),
            "artist_name": artist_name,
            "album_count": int(arow[5] or 0),
            "artist_profile_source": (str(profile_row[0] or "").strip() if profile_row else "") or None,
            "artist_profile_updated_at": int(profile_row[1] or 0) if profile_row else None,
            "artist_image": {
                "has_image": bool(has_visual),
                "mode": artist_image_mode,
                "provider": artist_image_provider,
                "source_url": artist_image_source_url,
                "url": (
                    f"{request.url_root.rstrip('/')}/api/library/files/artist/{artist_id}/image?size=320"
                    if bool(has_visual)
                    else None
                ),
            },
            "summary": {
                "matched": matched_count,
                "soft_matched": soft_count,
                "not_matched": no_count,
                "total": len(album_reports),
            },
            "albums": album_reports,
            "history": audits,
            "updated_at": int(arow[6] or 0),
        }
        return jsonify(payload)
    finally:
        conn.close()

def api_library_artist_summary(artist_id: int):
    """Return original vs AI summary for an artist (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Artist summary endpoint is available in Files mode only"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, name_norm, COALESCE(entity_kind, 'artist'), COALESCE(roles_json, '[]') FROM files_artists WHERE id = %s", (int(artist_id),))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Artist not found"}), 404
            artist_name = (row[1] or "").strip()
            artist_norm = (row[2] or "").strip() or _norm_artist_key(artist_name)
            _files_ensure_local_artist_profile(
                conn,
                artist_id=int(artist_id),
                artist_name=artist_name,
                artist_norm=artist_norm,
                entity_kind=str(row[3] or "artist").strip() or "artist",
                roles_json=str(row[4] or "[]"),
            )

            cur.execute(
                """
                SELECT bio, short_bio, source, updated_at
                FROM files_artist_profiles
                WHERE name_norm = %s
                """,
                (artist_norm,),
            )
            prof_row = cur.fetchone()
            original_text = ""
            original_source = ""
            original_updated_at = 0
            if prof_row:
                # Prefer long-form bio, fall back to short.
                original_text = (prof_row[0] or "").strip() or (prof_row[1] or "").strip()
                original_source = (prof_row[2] or "").strip()
                original_updated_at = int(_dt_to_epoch(prof_row[3])) if prof_row[3] else 0
            # Hide garbage bios (e.g. "Read more on Last.fm") so UI can fall back to AI.
            if _is_garbage_bio(original_text):
                original_text = ""

            cur.execute(
                """
                SELECT content, source, provider, model, lang, updated_at
                FROM assistant_docs
                WHERE entity_type = 'artist'
                  AND entity_id = %s
                  AND doc_type = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (int(artist_id), "artist_summary_ai"),
            )
            ai_row = cur.fetchone()
            ai_text = ""
            ai_source = ""
            ai_provider = ""
            ai_model = ""
            ai_lang = ""
            ai_updated_at = 0
            if ai_row:
                ai_text = (ai_row[0] or "").strip()
                ai_source = (ai_row[1] or "").strip()
                ai_provider = (ai_row[2] or "").strip()
                ai_model = (ai_row[3] or "").strip()
                ai_lang = (ai_row[4] or "").strip()
                ai_updated_at = int(_dt_to_epoch(ai_row[5])) if ai_row[5] else 0
                # Back-compat: older rows used internal markers like "ai_generated".
                if ai_source.lower() in {"ai_generated", "ai"}:
                    ai_source = "web"

        return jsonify(
            {
                "artist_id": int(artist_id),
                "artist_name": artist_name,
                "original": {
                    "text": original_text,
                    "source": original_source,
                    "updated_at": original_updated_at,
                },
                "ai": {
                    "text": ai_text,
                    "source": ai_source,
                    "provider": ai_provider,
                    "model": ai_model,
                    "lang": ai_lang,
                    "updated_at": ai_updated_at,
                },
            }
        )
    finally:
        conn.close()

def _bandsintown_block_state() -> tuple[bool, float, str]:
    now = float(time.time())
    with _BANDSINTOWN_BLOCK_LOCK:
        until_ts = float(_BANDSINTOWN_BLOCK_UNTIL_TS or 0.0)
        reason = str(_BANDSINTOWN_BLOCK_REASON or "")
    return (until_ts > now, until_ts, reason)

def _bandsintown_mark_blocked(*, ttl_sec: int, reason: str) -> None:
    global _BANDSINTOWN_BLOCK_UNTIL_TS, _BANDSINTOWN_BLOCK_REASON, _BANDSINTOWN_BLOCK_LOGGED_UNTIL_TS
    now = float(time.time())
    until_ts = now + max(60, int(ttl_sec or 0))
    msg = str(reason or "temporarily disabled").strip() or "temporarily disabled"
    with _BANDSINTOWN_BLOCK_LOCK:
        _BANDSINTOWN_BLOCK_UNTIL_TS = until_ts
        _BANDSINTOWN_BLOCK_REASON = msg
        already_logged = float(_BANDSINTOWN_BLOCK_LOGGED_UNTIL_TS or 0.0) >= until_ts - 1.0
        if not already_logged:
            _BANDSINTOWN_BLOCK_LOGGED_UNTIL_TS = until_ts
    if not already_logged:
        logging.warning(
            "[Concerts] Bandsintown temporarily disabled for %.1fh (%s)",
            max(0.0, (until_ts - now) / 3600.0),
            msg,
        )

def _bandsintown_fetch_upcoming_events(artist_name: str, app_id: str, *, force: bool = False) -> list[dict]:
    """
    Fetch upcoming events for an artist from Bandsintown.
    Notes:
    - Bandsintown uses `app_id` (string) for identification; it is not a secret.
    - We normalize the payload for UI stability.
    """
    name = (artist_name or "").strip()
    if not name:
        return []
    blocked, until_ts, reason = _bandsintown_block_state()
    if blocked and not force:
        logging.debug(
            "[Concerts] Bandsintown skipped (temporarily disabled until %s; reason=%s) artist=%s",
            datetime.fromtimestamp(until_ts).strftime("%Y-%m-%d %H:%M:%S"),
            reason or "n/a",
            name,
        )
        return []
    encoded = quote(name, safe="")
    url = f"https://rest.bandsintown.com/artists/{encoded}/events"
    try:
        r = requests.get(
            url,
            params={"app_id": (app_id or "pmda"), "date": "upcoming"},
            headers={"User-Agent": "PMDA/0.7.5"},
            timeout=12,
        )
        if r.status_code == 404:
            return []
        # Bandsintown frequently returns 403 from some hosting environments.
        # Keep it non-fatal so we can fall back to Songkick scraping.
        if r.status_code == 403:
            body = ""
            try:
                body = str((r.text or "")[:240]).strip()
            except Exception:
                body = ""
            _bandsintown_mark_blocked(
                ttl_sec=_BANDSINTOWN_FORBIDDEN_TTL_SEC,
                reason=f"HTTP 403 explicit deny{': ' + body if body else ''}",
            )
            logging.info("[Concerts] Bandsintown forbidden (403) for artist=%s", name)
            return []
        if r.status_code == 429:
            _bandsintown_mark_blocked(ttl_sec=_BANDSINTOWN_RATE_LIMIT_TTL_SEC, reason="HTTP 429 rate limited")
            return []
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            return []
        out: list[dict] = []
        for ev in data:
            if not isinstance(ev, dict):
                continue
            dt = str(ev.get("datetime") or "").strip()
            venue = ev.get("venue") if isinstance(ev.get("venue"), dict) else {}
            offers = ev.get("offers") if isinstance(ev.get("offers"), list) else []
            ticket_url = str(ev.get("url") or "").strip()
            if not ticket_url and offers:
                for off in offers:
                    if isinstance(off, dict) and str(off.get("url") or "").strip():
                        ticket_url = str(off.get("url") or "").strip()
                        break
            out.append(
                {
                    "provider": "bandsintown",
                    "id": str(ev.get("id") or ""),
                    "datetime": dt,
                    "title": str(ev.get("title") or "").strip(),
                    "url": ticket_url,
                    "lineup": ev.get("lineup") if isinstance(ev.get("lineup"), list) else [],
                    "venue": {
                        "name": str(venue.get("name") or "").strip(),
                        "city": str(venue.get("city") or "").strip(),
                        "region": str(venue.get("region") or "").strip(),
                        "country": str(venue.get("country") or "").strip(),
                        "latitude": str(venue.get("latitude") or "").strip(),
                        "longitude": str(venue.get("longitude") or "").strip(),
                    },
                }
            )
        return out
    except Exception:
        return []

def _concerts_fetch_upcoming_events(artist_name: str, preferred_provider: str = "auto") -> tuple[str, list[dict], str | None]:
    """
    Centralized concert provider selection.
    Strategy:
    - `songkick`: Songkick only
    - `bandsintown`: Bandsintown only (unless temporarily blocked)
    - `auto`: Songkick only. Bandsintown is no longer part of the default path.
    """
    provider = str(preferred_provider or "auto").strip().lower() or "auto"
    name = (artist_name or "").strip()
    if not name:
        return ("songkick" if provider == "auto" else provider, [], None)
    if provider == "songkick":
        events, source_url = _songkick_fetch_upcoming_events(name)
        return ("songkick", events, source_url)
    if provider == "bandsintown":
        app_id = str(_get_config_from_db("BANDSINTOWN_APP_ID", "") or "").strip() or "pmda"
        events = _bandsintown_fetch_upcoming_events(name, app_id)
        source_url = f"https://www.bandsintown.com/a/{quote(name, safe='')}" if name else None
        return ("bandsintown", events, source_url)
    events, source_url = _songkick_fetch_upcoming_events(name)
    return ("songkick", events, source_url)

def _osm_geocode_place(query: str) -> tuple[str, str]:
    """Best-effort geocode using Nominatim (OpenStreetMap). Returns (lat, lon) strings or ('','')."""
    q = (query or "").strip()
    if not q:
        return ("", "")
    key = q.lower()
    now = float(time.time())
    try:
        cached = _GEO_OSM_CACHE.get(key)
        if cached and (now - float(cached[0] or 0.0)) < _GEO_OSM_CACHE_TTL_SEC:
            return (str(cached[1] or ""), str(cached[2] or ""))
    except Exception:
        pass
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": q, "format": "jsonv2", "limit": 1, "addressdetails": 0},
            headers={"User-Agent": "PMDA/0.7.5 (concert-map)"},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or not data:
            return ("", "")
        item = data[0] if isinstance(data[0], dict) else {}
        lat = str(item.get("lat") or "").strip()
        lon = str(item.get("lon") or "").strip()
        if lat and lon:
            _GEO_OSM_CACHE[key] = (now, lat, lon)
            return (lat, lon)
    except Exception:
        return ("", "")
    return ("", "")

def _songkick_search_artist(artist_name: str) -> dict | None:
    """Best-effort Songkick artist search (HTML). Returns {name, href, id, upcoming_events}."""
    name = (artist_name or "").strip()
    if not name:
        return None
    try:
        r = requests.get(
            "https://www.songkick.com/search",
            params={"query": name, "type": "artists"},
            headers={"User-Agent": "Mozilla/5.0 (PMDA)"},
            timeout=12,
        )
        r.raise_for_status()
        html_text = r.text or ""
    except Exception:
        return None

    def _norm(s: str) -> str:
        s = (s or "").strip().lower()
        s = re.sub(r"[^a-z0-9]+", " ", s)
        return re.sub(r"\s+", " ", s).strip()

    qn = _norm(name)
    blocks = re.findall(r'<li class="artist">.*?</li>', html_text, flags=re.S | re.I)
    best: dict | None = None
    best_score = -1.0
    for b in blocks[:50]:
        href_m = re.search(r'href="(/artists/[^"]+)"', b, flags=re.I)
        name_m = re.search(r"<strong>(.*?)</strong>", b, flags=re.I | re.S)
        if not href_m or not name_m:
            continue
        href = str(href_m.group(1) or "").strip()
        cand_name = html.unescape(str(name_m.group(1) or "")).strip()
        if not href or not cand_name:
            continue
        upcoming = 0
        um = re.search(r"(\d+)\s+upcoming\s+events", b, flags=re.I)
        if um:
            try:
                upcoming = int(um.group(1) or 0)
            except Exception:
                upcoming = 0
        cn = _norm(cand_name)
        # Prefer exact normalized matches, otherwise best similarity.
        score = 0.0
        if cn == qn:
            score = 1000.0 + float(upcoming)
        else:
            try:
                import difflib

                ratio = difflib.SequenceMatcher(None, cn, qn).ratio()
                score = ratio * 100.0 + min(50.0, float(upcoming) / 2.0)
            except Exception:
                score = float(upcoming)
        if score > best_score:
            best_score = score
            best = {
                "name": cand_name,
                "href": href,
                "id": _parse_int_loose(re.search(r"/artists/(\d+)", href).group(1) if re.search(r"/artists/(\d+)", href) else 0),
                "upcoming_events": upcoming,
            }
    return best

def _songkick_fetch_upcoming_events(artist_name: str) -> tuple[list[dict], str | None]:
    """Fetch upcoming events for an artist from Songkick (scrape HTML). Returns (events, source_url)."""
    name = (artist_name or "").strip()
    if not name:
        return ([], None)
    hit = _songkick_search_artist(name)
    if not hit or not str(hit.get("href") or "").strip():
        return ([], None)
    href = str(hit.get("href") or "").strip()
    source_url = f"https://www.songkick.com{href}"
    try:
        r = requests.get(source_url, headers={"User-Agent": "Mozilla/5.0 (PMDA)"}, timeout=12)
        r.raise_for_status()
        page = r.text or ""
    except Exception:
        return ([], source_url)

    low = page.lower()
    start = low.find('id="coming-up"')
    end = low.find('id="past-events"', start + 1) if start != -1 else -1
    scope = page[start:end] if start != -1 and end != -1 else page

    items = re.findall(r'<li[^>]*class="event-listing-item[^"]*"[^>]*>.*?</li>', scope, flags=re.S | re.I)
    out: list[dict] = []
    for it in items[:80]:
        try:
            dt_m = re.search(r'<time[^>]*datetime="([^"]+)"', it, flags=re.I)
            href_m = re.search(r'href="(/concerts/[^"]+)"', it, flags=re.I)
            loc_m = re.search(r'<div[^>]*class="primary-detail"[^>]*>(.*?)</div>', it, flags=re.I | re.S)
            venue_m = re.search(r'<div[^>]*class="secondary-detail"[^>]*>(.*?)</div>', it, flags=re.I | re.S)
            if not href_m:
                continue
            event_href = str(href_m.group(1) or "").strip()
            event_url = f"https://www.songkick.com{event_href}"
            ev_id_m = re.search(r"/concerts/(\d+)", event_href)
            ev_id = str(ev_id_m.group(1) or "") if ev_id_m else ""
            dt = str(dt_m.group(1) or "").strip() if dt_m else ""
            location = html.unescape(re.sub(r"<[^>]+>", " ", (loc_m.group(1) if loc_m else "")).strip())
            location = re.sub(r"\s+", " ", location).strip()
            venue = html.unescape(re.sub(r"<[^>]+>", " ", (venue_m.group(1) if venue_m else "")).strip())
            venue = re.sub(r"\s+", " ", venue).strip()

            city = location
            region = ""
            country = ""
            parts = [p.strip() for p in location.split(",") if p.strip()]
            if len(parts) >= 3:
                city, region, country = parts[0], parts[1], parts[2]
            elif len(parts) == 2:
                city, country = parts[0], parts[1]

            out.append(
                {
                    "provider": "songkick",
                    "id": ev_id,
                    "datetime": dt,
                    "title": name,
                    "url": event_url,
                    "lineup": [name],
                    "venue": {
                        "name": venue,
                        "city": city,
                        "region": region,
                        "country": country,
                        "latitude": "",
                        "longitude": "",
                    },
                }
            )
        except Exception:
            continue

    # Best-effort geocoding so the UI can render a small map with pins.
    # We intentionally geocode only city/region/country (not the venue name) to reduce noise.
    try:
        # Deduplicate queries and cap requests so this endpoint remains responsive.
        q_to_latlon: dict[str, tuple[str, str]] = {}
        queries: list[str] = []
        for ev in out[:32]:
            v = ev.get("venue") if isinstance(ev.get("venue"), dict) else {}
            if not isinstance(v, dict):
                continue
            if str(v.get("latitude") or "").strip() and str(v.get("longitude") or "").strip():
                continue
            city = str(v.get("city") or "").strip()
            region = str(v.get("region") or "").strip()
            country = str(v.get("country") or "").strip()
            if not (city and country):
                continue
            q = ", ".join([p for p in [city, region, country] if p]).strip()
            if not q or q in q_to_latlon:
                continue
            queries.append(q)
            if len(queries) >= 8:
                break
        for q in queries:
            lat, lon = _osm_geocode_place(q)
            if lat and lon:
                q_to_latlon[q] = (lat, lon)
        if q_to_latlon:
            for ev in out:
                v = ev.get("venue") if isinstance(ev.get("venue"), dict) else {}
                if not isinstance(v, dict):
                    continue
                if str(v.get("latitude") or "").strip() and str(v.get("longitude") or "").strip():
                    continue
                city = str(v.get("city") or "").strip()
                region = str(v.get("region") or "").strip()
                country = str(v.get("country") or "").strip()
                q = ", ".join([p for p in [city, region, country] if p]).strip()
                if q in q_to_latlon:
                    v["latitude"], v["longitude"] = q_to_latlon[q]
    except Exception:
        pass
    return (out, source_url)

def api_library_artist_concerts(artist_id: int):
    """Return cached upcoming concerts for an artist (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Artist concerts endpoint is available in Files mode only"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503

    refresh = str(request.args.get("refresh") or "").strip().lower() in {"1", "true", "yes"}
    provider = str(request.args.get("provider") or "auto").strip().lower() or "auto"
    ttl_sec = max(600, min(24 * 3600, _parse_int_loose(request.args.get("ttl_sec"), 6 * 3600)))

    if provider not in {"auto", "bandsintown", "songkick"}:
        return jsonify({"error": f"Unsupported provider: {provider}"}), 400

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM files_artists WHERE id = %s", (int(artist_id),))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Artist not found"}), 404
            artist_name = (row[1] or "").strip()

            cur.execute(
                """
                SELECT provider, events_json, source_url, updated_at
                FROM files_artist_concerts
                WHERE artist_id = %s
                """,
                (int(artist_id),),
            )
            cached = cur.fetchone()

        now_epoch = int(time.time())
        if cached and not refresh:
            cached_provider = str(cached[0] or "").strip() or provider
            cached_events_json = str(cached[1] or "[]")
            cached_source_url = str(cached[2] or "").strip() or None
            cached_updated_epoch = int(_dt_to_epoch(cached[3])) if cached[3] else 0
            if cached_updated_epoch and (now_epoch - cached_updated_epoch) < ttl_sec:
                try:
                    events = json.loads(cached_events_json) if cached_events_json else []
                except (TypeError, ValueError):
                    events = []
                if not isinstance(events, list):
                    events = []
                return jsonify(
                    {
                        "artist_id": int(artist_id),
                        "artist_name": artist_name,
                        "provider": cached_provider,
                        "events": events,
                        "source_url": cached_source_url,
                        "updated_at": cached_updated_epoch,
                        "cached": True,
                    }
                )

        # Refresh from provider.
        provider_used, events, source_url = _concerts_fetch_upcoming_events(artist_name, provider)

        events_json = json.dumps(events, ensure_ascii=False)
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO files_artist_concerts(artist_id, provider, events_json, source_url, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (artist_id) DO UPDATE SET
                        provider = EXCLUDED.provider,
                        events_json = EXCLUDED.events_json,
                        source_url = EXCLUDED.source_url,
                        updated_at = NOW()
                    """,
                    (int(artist_id), provider_used, events_json, source_url),
                )

        return jsonify(
            {
                "artist_id": int(artist_id),
                "artist_name": artist_name,
                "provider": provider_used,
                "events": events,
                "source_url": source_url,
                "updated_at": int(time.time()),
                "cached": False,
            }
        )
    finally:
        conn.close()
