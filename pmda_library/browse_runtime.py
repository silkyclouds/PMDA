"""Runtime-backed library browse handlers.

This module contains the heavy browse/discover implementations that used to
live in ``pmda.py``. It accepts the live PMDA runtime module at the public
boundary while the remaining global state is progressively decomposed into
explicit service objects.
"""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one browse request."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "api_library_discover",
        "api_library_artists",
        "api_library_albums",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def api_library_discover_for_runtime(runtime: Any):
    """Run ``api_library_discover`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _api_library_discover_impl()

def api_library_artists_for_runtime(runtime: Any):
    """Run ``api_library_artists`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _api_library_artists_impl()

def api_library_albums_for_runtime(runtime: Any):
    """Run ``api_library_albums`` using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _api_library_albums_impl()


def _api_library_discover_impl():
    """Personalized discovery feed (Files mode only).

    Returns a set of album carousels driven by listening telemetry:
    - genres you listen to
    - your top artists
    - similar artists (from cached artist profiles)
    - labels you tend to play
    """
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400

    days = max(7, min(365, _parse_int_loose(request.args.get("days"), 90)))
    limit = max(6, min(36, _parse_int_loose(request.args.get("limit"), 18)))
    refresh = bool(_parse_bool(request.args.get("refresh")))
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    matched_where = (
        f"({_library_albums_match_where(include_unmatched, 'alb')})"
        f" AND ({_library_album_scope_where(scope, 'alb')})"
    )
    user_id = _current_user_id_or_zero()

    cache_key = (
        f"library:discover:u{user_id}:{days}:{limit}:"
        f"{_library_cache_scope_suffix(scope)}:{_library_cache_unmatched_suffix(include_unmatched)}"
    )
    scan_busy = _files_scan_busy()
    if not refresh:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)

    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    if scan_busy:
        snapshot = _files_index_maybe_enqueue_published_catchup(
            include_unmatched=include_unmatched,
            scope=scope,
            reason=f"api_library_discover_{scope}",
        )
        if bool(snapshot.get("underbuilt")) and int(snapshot.get("published_albums") or 0) > 0:
            payload = _files_library_discover_scan_safe_payload(
                include_unmatched=include_unmatched,
                scope=scope,
                limit=int(limit),
                days=int(days),
            )
            _files_cache_set_json(cache_key, payload, ttl=45)
            return jsonify(payload)

    conn = _files_pg_connect(acquire_timeout_sec=0.75)
    if conn is None:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            payload = dict(cached)
            payload["stale"] = True
            return jsonify(payload)
        if scan_busy:
            return jsonify(
                _files_library_discover_scan_safe_payload(
                    include_unmatched=include_unmatched,
                    scope=scope,
                    limit=int(limit),
                    days=int(days),
                )
            )
        return jsonify({"error": "PostgreSQL unavailable"}), 503

    def _split_genre_string(raw: str) -> list[str]:
        parts = []
        try:
            parts = _split_genre_values(raw or "")
        except Exception:
            parts = []
        out: list[str] = []
        seen: set[str] = set()
        for p in parts:
            v = re.sub(r"\s+", " ", str(p or "").strip())
            if not v:
                continue
            k = v.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(v)
        return out

    def _album_rows_to_payload(rows: list[tuple]) -> list[dict]:
        base_url = request.url_root.rstrip("/")
        albums_out: list[dict] = []
        for (
            album_id,
            title,
            year,
            genre,
            label,
            tags_json,
            track_count,
            fmt,
            is_lossless,
            sample_rate,
            bit_depth,
            has_cover,
            _cover_path,
            _folder_path,
            mb_identified,
            artist_id,
            artist_name,
            short_desc,
            profile_source,
            profile_tags_json,
            public_rating,
            public_rating_votes,
            public_rating_source,
            heat_score,
            heat_label,
            user_rating,
        ) in rows:
            aid = int(album_id or 0)
            arid = int(artist_id or 0)
            thumb = f"{base_url}/api/library/files/album/{aid}/cover?size=512" if bool(has_cover) else None
            short_desc_clean = (short_desc or "").strip()
            genres_list = _merge_album_genre_lists(tags_json, profile_tags_json, genre or "")
            albums_out.append(
                {
                    "album_id": aid,
                    "title": title or "",
                    "year": int(year or 0) or None,
                    "genre": "; ".join(genres_list) if genres_list else ((genre or "").strip() or None),
                    "genres": genres_list,
                    "label": (label or "").strip() or None,
                    "track_count": int(track_count or 0),
                    "format": (fmt or "").strip() or None,
                    "is_lossless": bool(is_lossless),
                    "sample_rate": int(sample_rate or 0) or None,
                    "bit_depth": int(bit_depth or 0) or None,
                    "mb_identified": bool(mb_identified),
                    "thumb": thumb,
                    "artist_id": arid,
                    "artist_name": artist_name or "",
                    "short_description": short_desc_clean or None,
                    "profile_source": (profile_source or "").strip() or None,
                    "public_rating": float(public_rating) if public_rating is not None else None,
                    "public_rating_votes": int(public_rating_votes or 0),
                    "public_rating_source": (public_rating_source or "").strip() or None,
                    "heat_score": float(heat_score) if heat_score is not None else None,
                    "heat_label": None,
                    "user_rating": int(user_rating or 0) if int(user_rating or 0) > 0 else None,
                }
            )
        return albums_out

    discover_album_select_sql = """
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
                    COALESCE(alb.sample_rate, 0) AS sample_rate,
                    COALESCE(alb.bit_depth, 0) AS bit_depth,
                    alb.has_cover,
                    COALESCE(alb.cover_path, '') AS cover_path,
                    COALESCE(alb.folder_path, '') AS folder_path,
                    alb.mb_identified,
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
                    COALESCE(pr.short_description, '') AS short_description,
                    COALESCE(pr.source, '') AS profile_source,
                    COALESCE(pr.tags_json, '[]') AS profile_tags_json,
                    pr.public_rating,
                    COALESCE(pr.public_rating_votes, 0) AS public_rating_votes,
                    COALESCE(pr.public_rating_source, '') AS public_rating_source,
                    pr.heat_score,
                    COALESCE(pr.heat_label, '') AS heat_label,
                    COALESCE(ur.rating, 0) AS user_rating
                FROM files_albums alb
                JOIN files_artists ar ON ar.id = alb.artist_id
                LEFT JOIN files_album_profiles pr
                       ON pr.artist_norm = ar.name_norm
                      AND pr.title_norm = alb.title_norm
                LEFT JOIN files_user_album_ratings ur
                       ON ur.album_id = alb.id
                      AND ur.user_id = %s
    """

    def _fetch_random_albums(where_sql: str, params: list, n: int, exclude_album_ids: set[int]) -> list[dict]:
        if n <= 0:
            return []
        where_all = f"({matched_where}) AND ({where_sql})"
        with conn.cursor() as cur:
            cur.execute(
                f"""
                {discover_album_select_sql}
                WHERE {where_all}
                ORDER BY random()
                LIMIT %s
                """,
                [int(user_id), *params, int(n)],
            )
            rows = cur.fetchall()
        albums = _album_rows_to_payload(rows)
        # Defensive: ensure no duplicates in caller exclude set.
        out = []
        for a in albums:
            aid = int(a.get("album_id") or 0)
            if aid <= 0:
                continue
            if aid in exclude_album_ids:
                continue
            out.append(a)
            exclude_album_ids.add(aid)
        return out

    def _fetch_ranked_albums(where_sql: str, params: list, n: int, exclude_album_ids: set[int], *, order_sql: str) -> list[dict]:
        if n <= 0:
            return []
        where_all = f"({matched_where}) AND ({where_sql})"
        with conn.cursor() as cur:
            cur.execute(
                f"""
                {discover_album_select_sql}
                WHERE {where_all}
                {order_sql}
                LIMIT %s
                """,
                [int(user_id), *params, int(n)],
            )
            rows = cur.fetchall()
        albums = _album_rows_to_payload(rows)
        out: list[dict] = []
        for a in albums:
            aid = int(a.get("album_id") or 0)
            if aid <= 0 or aid in exclude_album_ids:
                continue
            out.append(a)
            exclude_album_ids.add(aid)
        return out

    try:
        generated_at = int(time.time())
        with conn.cursor() as cur:
            # Prefer explicit playback telemetry; fall back to recommendation telemetry when empty.
            with _files_pg_statement_timeout(cur, 2500):
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM files_playback_events
                    WHERE user_id = 1
                      AND created_at >= NOW() - (%s || ' days')::interval
                      AND played_seconds >= 12
                    """,
                    (int(days),),
                )
                playback_count = int((cur.fetchone() or [0])[0] or 0)
            use_reco = playback_count <= 0
            # NOTE: files_reco_events has no user_id (single-user), but does store played_seconds + created_at.
            ev_table = "files_reco_events" if use_reco else "files_playback_events"
            ev_user_filter = "1=1" if use_reco else "e.user_id = 1"

            # Recently played albums (to reduce repetition).
            cur.execute(
                f"""
                SELECT t.album_id
                FROM {ev_table} e
                JOIN files_tracks t ON t.id = e.track_id
                JOIN files_albums alb ON alb.id = t.album_id
                WHERE {ev_user_filter}
                  AND {matched_where}
                  AND e.created_at >= NOW() - (%s || ' days')::interval
                  AND COALESCE(e.played_seconds, 0) >= 12
                ORDER BY e.created_at DESC
                LIMIT 600
                """,
                (int(days),),
            )
            recent_album_rows = cur.fetchall()

            cur.execute(
                f"""
                SELECT
                    a.id AS artist_id,
                    a.name AS artist_name,
                    a.name_norm AS artist_norm,
                    SUM(e.played_seconds) AS sec
                FROM {ev_table} e
                JOIN files_tracks t ON t.id = e.track_id
                JOIN files_albums alb ON alb.id = t.album_id
                JOIN files_artists a ON a.id = alb.artist_id
                WHERE {ev_user_filter}
                  AND {matched_where}
                  AND e.created_at >= NOW() - (%s || ' days')::interval
                  AND COALESCE(e.played_seconds, 0) >= 12
                GROUP BY a.id, a.name, a.name_norm
                ORDER BY sec DESC, a.name ASC
                LIMIT 10
                """,
                (int(days),),
            )
            top_artist_rows = cur.fetchall()

            cur.execute(
                f"""
                SELECT
                    lower(trim(COALESCE(alb.label, ''))) AS label_norm,
                    MIN(trim(COALESCE(alb.label, ''))) AS label,
                    SUM(e.played_seconds) AS sec
                FROM {ev_table} e
                JOIN files_tracks t ON t.id = e.track_id
                JOIN files_albums alb ON alb.id = t.album_id
                WHERE {ev_user_filter}
                  AND {matched_where}
                  AND e.created_at >= NOW() - (%s || ' days')::interval
                  AND COALESCE(e.played_seconds, 0) >= 12
                  AND COALESCE(trim(alb.label), '') <> ''
                GROUP BY lower(trim(COALESCE(alb.label, '')))
                ORDER BY sec DESC, label ASC
                LIMIT 10
                """,
                (int(days),),
            )
            top_label_rows = cur.fetchall()

            # Album-level rows to compute genre preference (multi-genre albums: split seconds across tags).
            cur.execute(
                f"""
                SELECT
                    alb.id AS album_id,
                    COALESCE(alb.tags_json, '[]') AS tags_json,
                    COALESCE(alb.genre, '') AS genre,
                    SUM(e.played_seconds) AS sec
                FROM {ev_table} e
                JOIN files_tracks t ON t.id = e.track_id
                JOIN files_albums alb ON alb.id = t.album_id
                WHERE {ev_user_filter}
                  AND {matched_where}
                  AND e.created_at >= NOW() - (%s || ' days')::interval
                  AND COALESCE(e.played_seconds, 0) >= 12
                GROUP BY alb.id, alb.tags_json, alb.genre
                ORDER BY sec DESC
                LIMIT 1200
                """,
                (int(days),),
            )
            album_genre_rows = cur.fetchall()

            cur.execute(
                f"""
                SELECT
                    COALESCE(alb.year, 0) AS year,
                    SUM(e.played_seconds) AS sec
                FROM {ev_table} e
                JOIN files_tracks t ON t.id = e.track_id
                JOIN files_albums alb ON alb.id = t.album_id
                WHERE {ev_user_filter}
                  AND {matched_where}
                  AND e.created_at >= NOW() - (%s || ' days')::interval
                  AND COALESCE(e.played_seconds, 0) >= 12
                  AND alb.year IS NOT NULL AND alb.year > 0
                GROUP BY COALESCE(alb.year, 0)
                ORDER BY sec DESC, year DESC
                LIMIT 10
                """,
                (int(days),),
            )
            year_rows = cur.fetchall()

            # Fallback seeds: liked artists (when listening telemetry is empty).
            cur.execute(
                """
                SELECT l.entity_id, a.name, a.name_norm, EXTRACT(EPOCH FROM l.updated_at)::BIGINT AS ts
                FROM files_user_entity_likes l
                JOIN files_artists a ON a.id = l.entity_id
                WHERE l.user_id = %s
                  AND l.entity_type = 'artist'
                  AND l.liked = TRUE
                ORDER BY l.updated_at DESC, l.entity_id DESC
                LIMIT 10
                """,
                (int(_current_user_id_or_zero() or 1),),
            )
            liked_artist_rows = cur.fetchall()

        recent_album_ids: set[int] = set()
        for r in recent_album_rows:
            try:
                aid = int(r[0] or 0)
            except Exception:
                aid = 0
            if aid > 0:
                recent_album_ids.add(aid)

        top_artists = [
            {"artist_id": int(r[0] or 0), "artist_name": (r[1] or ""), "artist_norm": (r[2] or ""), "seconds": int(r[3] or 0)}
            for r in top_artist_rows
            if int(r[0] or 0) > 0
        ]
        liked_artists = [
            {"artist_id": int(r[0] or 0), "artist_name": (r[1] or ""), "artist_norm": (r[2] or ""), "updated_at": int(r[3] or 0)}
            for r in liked_artist_rows
            if int(r[0] or 0) > 0
        ]
        top_labels = [
            {"label": (r[1] or "").strip(), "seconds": int(r[2] or 0)}
            for r in top_label_rows
            if str(r[1] or "").strip()
        ]

        # Genre preference map.
        genre_seconds: dict[str, float] = {}
        for album_id, tags_json, genre_raw, sec in album_genre_rows:
            seconds = float(sec or 0)
            if seconds <= 0:
                continue
            tags_list: list[str] = []
            try:
                t = json.loads(tags_json or "[]") if tags_json else []
                if isinstance(t, list):
                    tags_list = [re.sub(r"\s+", " ", str(x or "").strip()) for x in t]
                    tags_list = [x for x in tags_list if x]
            except Exception:
                tags_list = []
            if not tags_list:
                tags_list = _split_genre_string(genre_raw or "")
            tags_list = [x for x in tags_list if x]
            if not tags_list:
                continue
            share = seconds / max(1.0, float(len(tags_list)))
            for g in tags_list[:12]:
                k = g.lower()
                genre_seconds[k] = genre_seconds.get(k, 0.0) + share

        top_genres = sorted(genre_seconds.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
        top_genre_names = [g for g, _ in top_genres if g]
        top_years = sorted([(int(y or 0), float(sec or 0.0)) for y, sec in (year_rows or []) if int(y or 0) > 0], key=lambda kv: (-kv[1], -kv[0]))
        top_year = int(top_years[0][0]) if top_years else 0

        # Build carousels (avoid duplicates across sections).
        used_album_ids: set[int] = set(recent_album_ids)
        sections: list[dict] = []

        # 1) Genres you listen to.
        if top_genre_names:
            g = top_genre_names[0]
            albums = _fetch_random_albums(
                """
                NOT (alb.id = ANY(%s))
                AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS gg(value)
                    WHERE lower(trim(gg.value)) = %s
                )
                """,
                [list(used_album_ids), str(g)],
                limit,
                used_album_ids,
            )
            if albums:
                sections.append(
                    {
                        "key": "genre",
                        "title": f"More in {g}",
                        "reason": f"Because you listen to {g}.",
                        "seed": {"genre": g},
                        "albums": albums,
                    }
                )

        # 2) Your top artists (or liked artists).
        seed_artists = [a for a in top_artists[:4]]
        if not seed_artists and liked_artists:
            seed_artists = [{"artist_id": a["artist_id"], "artist_name": a["artist_name"], "artist_norm": a["artist_norm"], "seconds": 0} for a in liked_artists[:3]]
        if seed_artists:
            artist_ids = [int(a["artist_id"]) for a in seed_artists if int(a.get("artist_id") or 0) > 0]
            if artist_ids:
                label_artist = seed_artists[0]["artist_name"]
                albums = _fetch_random_albums(
                    "NOT (alb.id = ANY(%s)) AND alb.artist_id = ANY(%s)",
                    [list(used_album_ids), artist_ids],
                    limit,
                    used_album_ids,
                )
                if albums:
                    sections.append(
                        {
                            "key": "artists",
                            "title": "Because you play these artists",
                            "reason": f"Because you often listen to {label_artist}.",
                            "seed": {"artist_ids": artist_ids},
                            "albums": albums,
                        }
                    )

        # 3) Similar artists (from cached profile of your top artist).
        if top_artists:
            seed = top_artists[0]
            artist_norm = (seed.get("artist_norm") or "").strip()
            artist_name = (seed.get("artist_name") or "").strip()
            base_url = request.url_root.rstrip("/")
            sim_ids: list[int] = []
            try:
                prof = _files_get_artist_profile_cached(artist_name, artist_norm)
                sim = prof.get("similar_artists") if isinstance(prof, dict) else []
                if isinstance(sim, list) and sim:
                    sim = _files_attach_similar_artist_refs(conn, sim, base_url)
                    for it in sim:
                        if not isinstance(it, dict):
                            continue
                        sid = _parse_int_loose(it.get("artist_id"), 0)
                        if sid > 0:
                            sim_ids.append(int(sid))
                # De-dupe while preserving order.
                seen = set()
                sim_ids = [x for x in sim_ids if not (x in seen or seen.add(x))]
                sim_ids = sim_ids[:20]
            except Exception:
                sim_ids = []
            if sim_ids:
                albums = _fetch_random_albums(
                    "NOT (alb.id = ANY(%s)) AND alb.artist_id = ANY(%s)",
                    [list(used_album_ids), sim_ids],
                    limit,
                    used_album_ids,
                )
                if albums:
                    sections.append(
                        {
                            "key": "similar",
                            "title": "From similar artists",
                            "reason": f"Artists similar to {artist_name}.",
                            "seed": {"artist_id": int(seed.get("artist_id") or 0), "similar_artist_ids": sim_ids[:10]},
                            "albums": albums,
                        }
                    )

        # 4) Labels you tend to play.
        if top_labels:
            label = (top_labels[0].get("label") or "").strip()
            if label:
                albums = _fetch_random_albums(
                    "NOT (alb.id = ANY(%s)) AND lower(trim(COALESCE(alb.label, ''))) = lower(%s)",
                    [list(used_album_ids), label],
                    limit,
                    used_album_ids,
                )
                if albums:
                    sections.append(
                        {
                            "key": "labels",
                            "title": f"More from {label}",
                            "reason": f"Because you often play releases on {label}.",
                            "seed": {"label": label},
                            "albums": albums,
                        }
                    )

        # 5) Most played year.
        if top_year and int(top_year) > 0:
            albums = _fetch_random_albums(
                "NOT (alb.id = ANY(%s)) AND COALESCE(alb.year, 0) = %s",
                [list(used_album_ids), int(top_year)],
                limit,
                used_album_ids,
            )
            if albums:
                sections.append(
                    {
                        "key": "year",
                        "title": f"Most played in {int(top_year)}",
                        "reason": f"Because you often listen to music from {int(top_year)}.",
                        "seed": {"year": int(top_year)},
                        "albums": albums,
                    }
                )

        # Fallback: no listening telemetry yet (or empty library signals). Show random picks so Discover isn't empty.
        if not sections:
            albums = _fetch_random_albums(
                "NOT (alb.id = ANY(%s))",
                [list(used_album_ids)],
                limit,
                used_album_ids,
            )
            if albums:
                sections.append(
                    {
                        "key": "random",
                        "title": "Random picks",
                        "reason": "Start listening to personalize Discover. For now, here are random albums from your library.",
                        "albums": albums,
                    }
                )

        payload = {"days": days, "limit": limit, "generated_at": generated_at, "sections": sections}
        _files_cache_set_json(cache_key, payload, ttl=30)
        return jsonify(payload)
    except Exception as e:
        logging.warning("library discover query falling back during scan: %s", e)
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            payload = dict(cached)
            payload["stale"] = True
            return jsonify(payload)
        if scan_busy or _files_pg_is_statement_timeout_error(e):
            return jsonify(
                _files_library_discover_scan_safe_payload(
                    include_unmatched=include_unmatched,
                    scope=scope,
                    limit=int(limit),
                    days=int(days),
                )
            )
        raise
    finally:
        conn.close()


def _api_library_artists_impl():
    """Return list of artists with statistics. Supports search and pagination.
    Always restricted to SECTION_IDS (selected libraries) — CROSS_LIBRARY_DEDUPE only affects duplicate detection, not which artists are listed.
    """
    if _get_library_mode() == "files":
        search_query = request.args.get("search", "").strip()
        genre = (request.args.get("genre") or "").strip()
        label = (request.args.get("label") or "").strip()
        year = _parse_int_loose(request.args.get("year"), 0)
        sort = (request.args.get("sort") or ("relevance" if search_query else "recent")).strip().lower()
        include_unmatched = _library_include_unmatched_effective()
        scope = _library_scope_effective()
        browse_source_requested = _files_library_browse_source_requested()
        browse_album_match_sql = _library_albums_match_where(include_unmatched, "alb")
        browse_album_scope_sql = _library_album_scope_where(scope, "alb")
        browse_album_last_match_sql = _library_albums_match_where(include_unmatched, "alb_last")
        browse_album_last_scope_sql = _library_album_scope_where(scope, "alb_last")
        search_album_match_sql = _library_albums_match_where(include_unmatched, "alb2")
        search_album_scope_sql = _library_album_scope_where(scope, "alb2")
        album_count_match_sql = _library_albums_match_where(include_unmatched, "alb_cnt")
        album_count_scope_sql = _library_album_scope_where(scope, "alb_cnt")
        limit = max(1, min(500, _parse_int_loose(request.args.get("limit"), 100)))
        offset = max(0, _parse_int_loose(request.args.get("offset"), 0))
        with lock:
            live_cache_generation = _library_browse_core.live_cache_generation(state)
        refresh = bool(_parse_bool(request.args.get("refresh")))
        scan_busy = _files_scan_busy()
        snapshot: dict[str, Any] | None = None
        snapshot = _files_index_maybe_enqueue_published_catchup(
            include_unmatched=include_unmatched,
            scope=scope,
            reason=f"api_library_artists_{scope}",
        )
        browse_source = _files_library_browse_source_effective(
            scope=scope,
            requested=browse_source_requested,
            snapshot=snapshot,
            scan_busy=scan_busy,
        )
        if (
            browse_source_requested == "auto"
            and int(snapshot.get("published_albums") or 0) > 0
            and not bool(snapshot.get("api_lightweight"))
        ):
            # Keep the published snapshot as an automatic fallback only when the live
            # browse index is busy or underbuilt. Idle live queries must remain the
            # default so tests and operators can inspect exact PG-side artist state.
            browse_source = "published"
        prefer_published = browse_source == "published"
        cache_keys = _library_browse_core.browse_cache_keys(
            kind="artists",
            search_query=search_query,
            genre=genre,
            label=label,
            year=int(year or 0),
            sort=sort,
            limit=int(limit),
            offset=int(offset),
            scope_suffix=_library_cache_scope_suffix(scope),
            unmatched_suffix=_library_cache_unmatched_suffix(include_unmatched),
            live_generation=live_cache_generation,
            browse_source=browse_source,
        )
        cache_key = cache_keys.cache_key
        stable_cache_key = cache_keys.stable_cache_key
        if not refresh:
            cached = _files_cache_get_json(cache_key)
            if cached is not None:
                cached_source = str(cached.get("fallback_source") or "").strip().lower()
                if prefer_published:
                    if cached_source == "published":
                        return jsonify(cached)
                elif cached_source != "published":
                    return jsonify(cached)
            if not prefer_published:
                cached = _files_cache_get_json(stable_cache_key)
                if cached is not None:
                    if str(cached.get("fallback_source") or "").strip().lower() == "published":
                        cached = None
                if cached is not None:
                    payload = dict(cached)
                    payload["stale"] = True
                    payload["fallback_source"] = "stable_cache"
                    return jsonify(payload)
        if snapshot is None:
            snapshot = _files_index_maybe_enqueue_published_catchup(
                include_unmatched=include_unmatched,
                scope=scope,
                reason=f"api_library_artists_{scope}",
            )
            browse_source = _files_library_browse_source_effective(
                scope=scope,
                requested=browse_source_requested,
                snapshot=snapshot,
                scan_busy=scan_busy,
            )
            prefer_published = browse_source == "published"
        if prefer_published:
            payload = _files_library_published_artists(
                include_unmatched=include_unmatched,
                scope=scope,
                search_query=search_query,
                genre=genre,
                label=label,
                year=int(year or 0),
                sort=sort,
                limit=int(limit),
                offset=int(offset),
            )
            payload["browse_source"] = browse_source
            _files_cache_set_json(cache_key, payload, ttl=15)
            return jsonify(payload)
        if not refresh and files_index_lock.locked():
            cached = _files_cache_get_json(cache_key)
            if cached is not None:
                if str(cached.get("fallback_source") or "").strip().lower() == "published":
                    cached = None
            if cached is not None:
                payload = dict(cached)
                payload["stale"] = True
                return jsonify(payload)
            cached = _files_cache_get_json(stable_cache_key)
            if cached is not None:
                if str(cached.get("fallback_source") or "").strip().lower() == "published":
                    cached = None
            if cached is not None:
                payload = dict(cached)
                payload["stale"] = True
                payload["fallback_source"] = "stable_cache"
                return jsonify(payload)
        ok, err = _ensure_files_index_ready()
        if not ok:
            return jsonify({"error": err or "Files index unavailable"}), 503
        conn = _files_pg_connect(acquire_timeout_sec=0.75)
        if conn is None:
            cached = _files_cache_get_json(stable_cache_key)
            if cached is not None:
                if str(cached.get("fallback_source") or "").strip().lower() == "published":
                    cached = None
            if cached is not None:
                payload = dict(cached)
                payload["stale"] = True
                payload["fallback_source"] = "stable_cache"
                return jsonify(payload)
            if int(snapshot.get("published_albums") or 0) > 0:
                return jsonify(
                    _files_library_published_artists(
                        include_unmatched=include_unmatched,
                        scope=scope,
                        search_query=search_query,
                        genre=genre,
                        label=label,
                        year=int(year or 0),
                        sort=sort,
                        limit=int(limit),
                        offset=int(offset),
                    )
                )
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            status_ctx = _files_library_live_status_context(source_is_published=False)
            with conn.cursor() as cur:
                where_parts = ["1=1"]
                params: list = []
                where_parts.append("COALESCE(a.entity_kind, 'artist') IN ('artist', 'composer')")
                where_parts.append(
                    f"""
                    EXISTS (
                        SELECT 1
                        FROM files_artist_album_links link
                        JOIN files_albums alb ON alb.id = link.album_id
                        WHERE link.artist_id = a.id
                          AND {browse_album_match_sql}
                          AND {browse_album_scope_sql}
                    )
                    """
                )

                if search_query:
                    like = f"%{search_query}%"
                    search_norm = _norm_artist_key(search_query)
                    search_signature = _classical_person_signature_key(search_query)
                    search_album_match = search_album_match_sql
                    where_parts.append(
                        f"""
                        (
                            a.name ILIKE %s
                            OR COALESCE(a.canonical_name, '') ILIKE %s
                            OR (%s <> '' AND COALESCE(a.canonical_name_norm, '') = %s)
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
                            OR EXISTS (
                                SELECT 1
                                FROM files_artist_album_links link2
                                JOIN files_albums alb2 ON alb2.id = link2.album_id
                                WHERE link2.artist_id = a.id
                                  AND {search_album_match}
                                  AND {search_album_scope_sql}
                                  AND (
                                      alb2.title ILIKE %s
                                      OR EXISTS (
                                          SELECT 1
                                          FROM files_tracks tr2
                                          WHERE tr2.album_id = alb2.id
                                            AND tr2.title ILIKE %s
                                      )
                                  )
                            )
                        )
                        """
                    )
                    params.extend([like, like, search_norm, search_norm, like, search_norm, search_norm, search_signature, search_signature, like, like])

                if year and int(year) > 0:
                    where_parts.append(
                        """
                        EXISTS (
                            SELECT 1
                            FROM files_artist_album_links link
                            JOIN files_albums alb ON alb.id = link.album_id
                            WHERE link.artist_id = a.id
                              AND """ + browse_album_match_sql + """
                              AND """ + browse_album_scope_sql + """
                              AND COALESCE(alb.year, 0) = %s
                        )
                        """
                    )
                    params.append(int(year))

                if label:
                    parts = [p.strip() for p in str(label).split(",") if p.strip()]
                    if parts:
                        where_parts.append(
                            """
                            EXISTS (
                                SELECT 1
                            FROM files_artist_album_links link
                            JOIN files_albums alb ON alb.id = link.album_id
                            WHERE link.artist_id = a.id
                                  AND """ + browse_album_match_sql + """
                                  AND """ + browse_album_scope_sql + """
                                  AND lower(trim(COALESCE(alb.label, ''))) = ANY(%s)
                            )
                            """
                        )
                        params.append([p.lower() for p in parts])

                if genre:
                    parts = [p.strip() for p in str(genre).split(",") if p.strip()]
                    if parts:
                        norms = [p.lower() for p in parts]
                        where_parts.append(
                            """
                            EXISTS (
                                SELECT 1
                            FROM files_artist_album_links link
                            JOIN files_albums alb ON alb.id = link.album_id
                            WHERE link.artist_id = a.id
                                  AND """ + browse_album_match_sql + """
                                  AND """ + browse_album_scope_sql + """
                                  AND (
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
                            )
                            """
                        )
                        params.append(norms)
                        params.append(norms)

                where_sql = " AND ".join(where_parts)

                unfiltered_browse = not (search_query or genre or label or int(year or 0) > 0)
                snapshot_total = int((snapshot or {}).get("pg_artists") or 0)
                if unfiltered_browse and snapshot_total > 0:
                    total = snapshot_total
                else:
                    with _files_pg_statement_timeout(cur, 2200):
                        cur.execute(
                            f"""
                            SELECT COUNT(*)
                            FROM files_artists a
                            WHERE {where_sql}
                            """,
                            params,
                        )
                        total = int((cur.fetchone() or [0])[0] or 0)

                artist_has_image_sql = _artist_has_true_image_sql("a", "ext")

                if search_query:
                    if sort == "alpha":
                        search_order_sql = "ORDER BY a.name ASC, album_count DESC, last_added_at DESC NULLS LAST, a.id ASC"
                    elif sort == "recent":
                        search_order_sql = "ORDER BY last_added_at DESC NULLS LAST, album_count DESC, a.name ASC, a.id ASC"
                    elif sort == "albums":
                        search_order_sql = "ORDER BY album_count DESC, last_added_at DESC NULLS LAST, a.name ASC, a.id ASC"
                    else:
                        search_order_sql = "ORDER BY prefix_rank ASC, score DESC, album_count DESC, last_added_at DESC NULLS LAST, a.name ASC"
                    try:
                        with _files_pg_statement_timeout(cur, 3500):
                            cur.execute(
                                f"""
                            SELECT
                                a.id,
                                a.name,
                                COALESCE(a.canonical_name, '') AS canonical_name,
                                COALESCE(a.entity_kind, 'artist') AS entity_kind,
                                COALESCE(a.roles_json, '[]') AS roles_json,
                                COALESCE(a.aliases_json, '[]') AS aliases_json,
                                (
                                    SELECT COUNT(DISTINCT link_cnt.album_id)
                                    FROM files_artist_album_links link_cnt
                                    JOIN files_albums alb_cnt ON alb_cnt.id = link_cnt.album_id
                                    WHERE link_cnt.artist_id = a.id
                                      AND {album_count_match_sql}
                                      AND {album_count_scope_sql}
                                ) AS album_count,
                                a.broken_albums_count,
                                ({artist_has_image_sql}) AS has_image,
                                GREATEST(
                                    EXTRACT(EPOCH FROM COALESCE(a.updated_at, NOW())),
                                    EXTRACT(EPOCH FROM COALESCE(ext.updated_at, a.updated_at, NOW()))
                                )::bigint AS image_version,
                                COALESCE(a.image_path, '') AS local_image_path,
                                COALESCE(ext.image_path, '') AS ext_image_path,
                                COALESCE(ext.artist_name, '') AS ext_artist_name,
                                COALESCE(ext.provider, '') AS ext_provider,
                                COALESCE(ext.image_url, '') AS ext_image_url,
                                GREATEST(similarity(a.name, %s), similarity(COALESCE(a.canonical_name, ''), %s)) AS score,
                                CASE
                                    WHEN lower(a.name) LIKE lower(%s) || '%%'
                                      OR lower(COALESCE(a.canonical_name, '')) LIKE lower(%s) || '%%'
                                    THEN 0 ELSE 1
                                END AS prefix_rank,
                                (
                                    SELECT EXTRACT(EPOCH FROM MAX(alb_last.created_at))::BIGINT
                                    FROM files_artist_album_links link_last
                                    JOIN files_albums alb_last ON alb_last.id = link_last.album_id
                                    WHERE link_last.artist_id = a.id
                                      AND {browse_album_last_match_sql}
                                      AND {browse_album_last_scope_sql}
                                ) AS last_added_at
                            FROM files_artists a
                            LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                            WHERE {where_sql}
                            {search_order_sql}
                            LIMIT %s OFFSET %s
                            """,
                                [search_query, search_query, search_query, search_query, *params, int(limit), int(offset)],
                            )
                    except Exception:
                        if sort == "alpha":
                            fallback_search_order_sql = "ORDER BY a.name ASC, album_count DESC, last_added_at DESC NULLS LAST, a.id ASC"
                        elif sort == "recent":
                            fallback_search_order_sql = "ORDER BY last_added_at DESC NULLS LAST, album_count DESC, a.name ASC, a.id ASC"
                        else:
                            fallback_search_order_sql = "ORDER BY album_count DESC, last_added_at DESC NULLS LAST, a.name ASC"
                        with _files_pg_statement_timeout(cur, 3500):
                            cur.execute(
                                f"""
                            SELECT
                                a.id,
                                a.name,
                                COALESCE(a.canonical_name, '') AS canonical_name,
                                COALESCE(a.entity_kind, 'artist') AS entity_kind,
                                COALESCE(a.roles_json, '[]') AS roles_json,
                                COALESCE(a.aliases_json, '[]') AS aliases_json,
                                (
                                    SELECT COUNT(DISTINCT link_cnt.album_id)
                                    FROM files_artist_album_links link_cnt
                                    JOIN files_albums alb_cnt ON alb_cnt.id = link_cnt.album_id
                                    WHERE link_cnt.artist_id = a.id
                                      AND {album_count_match_sql}
                                      AND {album_count_scope_sql}
                                ) AS album_count,
                                a.broken_albums_count,
                                ({artist_has_image_sql}) AS has_image
                                ,
                                GREATEST(
                                    EXTRACT(EPOCH FROM COALESCE(a.updated_at, NOW())),
                                    EXTRACT(EPOCH FROM COALESCE(ext.updated_at, a.updated_at, NOW()))
                                )::bigint AS image_version,
                                COALESCE(a.image_path, '') AS local_image_path,
                                COALESCE(ext.image_path, '') AS ext_image_path,
                                COALESCE(ext.artist_name, '') AS ext_artist_name,
                                COALESCE(ext.provider, '') AS ext_provider,
                                COALESCE(ext.image_url, '') AS ext_image_url,
                                (
                                    SELECT EXTRACT(EPOCH FROM MAX(alb_last.created_at))::BIGINT
                                    FROM files_artist_album_links link_last
                                    JOIN files_albums alb_last ON alb_last.id = link_last.album_id
                                    WHERE link_last.artist_id = a.id
                                      AND {browse_album_last_match_sql}
                                      AND {browse_album_last_scope_sql}
                                ) AS last_added_at
                            FROM files_artists a
                            LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                            WHERE {where_sql}
                            {fallback_search_order_sql}
                            LIMIT %s OFFSET %s
                            """,
                                [*params, int(limit), int(offset)],
                            )
                else:
                    if sort == "alpha":
                        browse_order_sql = "ORDER BY a.name ASC, album_count DESC, last_added_at DESC NULLS LAST, a.id ASC"
                    elif sort == "albums":
                        browse_order_sql = "ORDER BY album_count DESC, last_added_at DESC NULLS LAST, a.name ASC"
                    else:
                        browse_order_sql = "ORDER BY last_added_at DESC NULLS LAST, album_count DESC, a.name ASC, a.id ASC"
                    with _files_pg_statement_timeout(cur, 3500):
                        cur.execute(
                            f"""
                        SELECT
                            a.id,
                            a.name,
                            COALESCE(a.canonical_name, '') AS canonical_name,
                            COALESCE(a.entity_kind, 'artist') AS entity_kind,
                            COALESCE(a.roles_json, '[]') AS roles_json,
                            COALESCE(a.aliases_json, '[]') AS aliases_json,
                            (
                                SELECT COUNT(DISTINCT link_cnt.album_id)
                                FROM files_artist_album_links link_cnt
                                JOIN files_albums alb_cnt ON alb_cnt.id = link_cnt.album_id
                                WHERE link_cnt.artist_id = a.id
                                  AND {album_count_match_sql}
                                  AND {album_count_scope_sql}
                            ) AS album_count,
                            a.broken_albums_count,
                            ({artist_has_image_sql}) AS has_image
                            ,
                            GREATEST(
                                EXTRACT(EPOCH FROM COALESCE(a.updated_at, NOW())),
                                EXTRACT(EPOCH FROM COALESCE(ext.updated_at, a.updated_at, NOW()))
                            )::bigint AS image_version,
                            COALESCE(a.image_path, '') AS local_image_path,
                            COALESCE(ext.image_path, '') AS ext_image_path,
                            COALESCE(ext.artist_name, '') AS ext_artist_name,
                            COALESCE(ext.provider, '') AS ext_provider,
                            COALESCE(ext.image_url, '') AS ext_image_url,
                            (
                                SELECT EXTRACT(EPOCH FROM MAX(alb_last.created_at))::BIGINT
                                FROM files_artist_album_links link_last
                                JOIN files_albums alb_last ON alb_last.id = link_last.album_id
                                WHERE link_last.artist_id = a.id
                                  AND {browse_album_last_match_sql}
                                  AND {browse_album_last_scope_sql}
                            ) AS last_added_at
                        FROM files_artists a
                        LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                        WHERE {where_sql}
                        {browse_order_sql}
                        LIMIT %s OFFSET %s
                        """,
                            [*params, int(limit), int(offset)],
                        )
                rows = cur.fetchall()
                artist_fallback_thumb_by_id: dict[int, str] = {}
                artist_profile_meta_by_id: dict[int, tuple[bool, str]] = {}
                page_artist_ids = [int(row[0] or 0) for row in rows if int(row[0] or 0) > 0]
                if page_artist_ids:
                    try:
                        with _files_pg_statement_timeout(cur, 3500):
                            cur.execute(
                                f"""
                                SELECT DISTINCT ON (link.artist_id)
                                    link.artist_id,
                                    alb_last.id AS album_id,
                                    COALESCE(alb_last.cover_path, '') AS cover_path,
                                    COALESCE(alb_last.folder_path, '') AS folder_path,
                                    COALESCE(alb_last.has_cover, FALSE) AS has_cover
                                FROM files_artist_album_links link
                                JOIN files_albums alb_last ON alb_last.id = link.album_id
                                WHERE link.artist_id = ANY(%s)
                                  AND {browse_album_last_match_sql}
                                  AND {browse_album_last_scope_sql}
                                ORDER BY link.artist_id, link.is_primary DESC, alb_last.created_at DESC NULLS LAST, alb_last.year DESC NULLS LAST, alb_last.id DESC
                                """,
                                (page_artist_ids,),
                            )
                            fallback_rows = cur.fetchall()
                        for fallback_row in fallback_rows:
                            fallback_artist_id = int(fallback_row[0] or 0)
                            fallback_album_id = int(fallback_row[1] or 0)
                            if fallback_artist_id <= 0 or fallback_album_id <= 0:
                                continue
                            has_cover_effective, _effective_cover_path = _resolve_files_album_cover_asset(
                                album_id=fallback_album_id,
                                cover_path_raw=str(fallback_row[2] or "").strip(),
                                folder_path_raw=str(fallback_row[3] or "").strip(),
                                has_cover=bool(fallback_row[4]),
                                persist=False,
                            )
                            if not has_cover_effective:
                                continue
                            artist_fallback_thumb_by_id[fallback_artist_id] = (
                                f"{request.url_root.rstrip('/')}/api/library/files/album/{fallback_album_id}/cover?size=512"
                            )
                    except Exception:
                        artist_fallback_thumb_by_id = {}
                    try:
                        with _files_pg_statement_timeout(cur, 3500):
                            cur.execute(
                                """
                                SELECT
                                    a.id,
                                    CASE
                                        WHEN BTRIM(COALESCE(prof.bio, '')) <> '' OR BTRIM(COALESCE(prof.short_bio, '')) <> '' THEN TRUE
                                        ELSE FALSE
                                    END AS has_profile,
                                    COALESCE(prof.source, '') AS profile_source
                                FROM files_artists a
                                LEFT JOIN files_artist_profiles prof ON prof.name_norm = a.name_norm
                                WHERE a.id = ANY(%s)
                                """,
                                (page_artist_ids,),
                            )
                            artist_profile_meta_by_id = {
                                int(profile_row[0] or 0): (bool(profile_row[1]), str(profile_row[2] or "").strip())
                                for profile_row in (cur.fetchall() or [])
                                if int(profile_row[0] or 0) > 0
                            }
                    except Exception:
                        artist_profile_meta_by_id = {}
            base_url = request.url_root.rstrip("/")
            artists_payload = []
            artists_missing_images: list[tuple[str, str]] = []
            for r in rows:
                artist_id = int(r[0] or 0)
                current_name = str(r[1] or "")
                canonical_name = str(r[2] or "")
                entity_kind = str(r[3] or "artist")
                roles_json = r[4] or "[]"
                aliases_json = r[5] or "[]"
                has_image = bool(r[8]) if len(r) > 8 else False
                image_version = int(r[9] or 0) if len(r) > 9 else 0
                local_image_path = str(r[10] or "") if len(r) > 10 else ""
                ext_image_path = str(r[11] or "") if len(r) > 11 else ""
                ext_artist_name = str(r[12] or "") if len(r) > 12 else ""
                ext_provider = str(r[13] or "") if len(r) > 13 else ""
                ext_image_url = str(r[14] or "") if len(r) > 14 else ""
                display_name = _library_artist_display_name(
                    current_name=current_name,
                    canonical_name=canonical_name,
                    entity_kind=entity_kind,
                    roles_json=roles_json,
                    aliases_json=aliases_json,
                )
                artist_norm = str(_norm_artist_key(canonical_name or current_name or display_name) or "").strip()
                roles_list = _safe_json_load(roles_json, fallback=[])
                if not has_image:
                    has_image = _artist_effective_image_present(
                        artist_name=display_name,
                        entity_kind=entity_kind,
                        role_hints=roles_list if isinstance(roles_list, list) else [],
                        local_image_path=local_image_path,
                        ext_image_path=ext_image_path,
                        ext_artist_name=ext_artist_name,
                        ext_provider=ext_provider,
                        ext_image_url=ext_image_url,
                    )
                has_profile, profile_source = artist_profile_meta_by_id.get(artist_id, (False, ""))
                if (not has_image) and artist_norm and display_name:
                    artists_missing_images.append((display_name, artist_norm))
                has_fallback_thumb = bool(artist_fallback_thumb_by_id.get(artist_id))
                status_fields = _files_library_artist_status_fields(
                    status_context=status_ctx,
                    has_image=bool(has_image),
                    has_profile=bool(has_profile),
                    has_fallback_thumb=bool(has_fallback_thumb),
                )
                artists_payload.append(
                    {
                        "artist_thumb_version": image_version,
                        "artist_id": artist_id,
                        "artist_name": display_name,
                        "entity_kind": entity_kind,
                        "roles": roles_list,
                        "album_count": int(r[6] or 0),
                        "broken_albums_count": int(r[7] or 0),
                        "artist_has_image": bool(has_image),
                        "artist_profile_source": profile_source or None,
                        "artist_fallback_thumb": artist_fallback_thumb_by_id.get(artist_id) or None,
                        "artist_thumb": (
                            _artist_image_asset_url(
                                base_url,
                                artist_id,
                                size=512,
                                version=image_version,
                            )
                            if has_image
                            else None
                        ),
                        **status_fields,
                    }
                )

            if not _files_index_is_running():
                targeted_refresh_limit = min((96 if refresh else 64), max(24, int(limit or 0)))
                for artist_name, artist_norm in artists_missing_images[:targeted_refresh_limit]:
                    try:
                        _enqueue_files_profile_enrichment(
                            artist_name=artist_name,
                            artist_norm=artist_norm,
                            albums=[],
                            skip_album_profiles=True,
                            fast_mode=False,
                            force=True,
                            priority_mode="p0",
                        )
                    except Exception:
                        continue

            payload = {
                "artists": artists_payload,
                "total": total,
                "limit": limit,
                "offset": offset,
                "has_more": _pagination_core.page_has_more(total=total, offset=offset, returned=len(rows)),
                "publication_state": str(status_ctx.get("publication_state") or "ready"),
                "background_enrichment_running": bool(status_ctx.get("background_enrichment_running")),
                "browse_source": browse_source,
            }
            if int(total or 0) <= 0 and _files_library_should_fallback_to_published(snapshot, albums=0, artists=0):
                status_ctx = _files_library_live_status_context(source_is_published=True)
                payload = _files_library_published_artists(
                    include_unmatched=include_unmatched,
                    scope=scope,
                    search_query=search_query,
                    genre=genre,
                    label=label,
                    year=int(year or 0),
                    sort=sort,
                    limit=int(limit),
                    offset=int(offset),
                    status_context=status_ctx,
                )
                payload["fallback_source"] = "published"
                payload["browse_source"] = "published"
            _files_cache_set_json(cache_key, payload, ttl=30)
            if str(payload.get("fallback_source") or "").strip().lower() != "published":
                _files_cache_set_json(stable_cache_key, payload, ttl=300)
            return jsonify(payload)
        except Exception as e:
            logging.warning("library artists query falling back during scan: %s", e)
            cached = _files_cache_get_json(stable_cache_key)
            if cached is not None:
                if str(cached.get("fallback_source") or "").strip().lower() == "published":
                    cached = None
            if cached is not None:
                payload = dict(cached)
                payload["stale"] = True
                payload["fallback_source"] = "stable_cache"
                return jsonify(payload)
            if scan_busy or _files_pg_is_statement_timeout_error(e):
                return jsonify(
                    _files_library_published_artists(
                        include_unmatched=include_unmatched,
                        scope=scope,
                        search_query=search_query,
                        genre=genre,
                        label=label,
                        year=int(year or 0),
                        sort=sort,
                        limit=int(limit),
                        offset=int(offset),
                    )
                )
            raise
        finally:
            conn.close()
    return jsonify({"error": "Files mode required"}), 400


def _api_library_albums_impl():
    """List albums (Files mode). Used for Roon-like album grid and carousels."""
    if _get_library_mode() != "files":
        return jsonify({"albums": [], "total": 0, "limit": 0, "offset": 0, "error": "Files mode required"}), 400

    search_query = (request.args.get("search") or "").strip()
    genre = (request.args.get("genre") or "").strip()
    label = (request.args.get("label") or "").strip()
    year = _parse_int_loose(request.args.get("year"), 0)
    include_unmatched = _library_include_unmatched_effective()
    scope = _library_scope_effective()
    browse_source_requested = _files_library_browse_source_requested()
    album_match_sql = _library_albums_match_where(include_unmatched, "alb")
    album_scope_sql = _library_album_scope_where(scope, "alb")
    sort = (request.args.get("sort") or "recent").strip().lower()
    user_id = _current_user_id_or_zero()
    limit = max(1, min(240, _parse_int_loose(request.args.get("limit"), 80)))
    offset = max(0, _parse_int_loose(request.args.get("offset"), 0))
    with lock:
        live_cache_generation = _library_browse_core.live_cache_generation(state)

    force_refresh = bool(_parse_bool(request.args.get("refresh")))
    scan_busy = _files_scan_busy()
    snapshot: dict[str, Any] | None = _files_index_maybe_enqueue_published_catchup(
        include_unmatched=include_unmatched,
        scope=scope,
        reason=f"api_library_albums_{scope}",
    )
    browse_source = _files_library_browse_source_effective(
        scope=scope,
        requested=browse_source_requested,
        snapshot=snapshot,
        scan_busy=scan_busy,
    )
    if browse_source_requested == "auto" and int((snapshot or {}).get("published_albums") or 0) > 0:
        # The published SQLite snapshot is the UI browse contract. The live PG
        # index can be richer, but album grids must not depend on it during or
        # after large rebuilds because a single slow aggregate can stall the UI.
        browse_source = "published"
    prefer_published = browse_source == "published"
    cache_keys = _library_browse_core.browse_cache_keys(
        kind="albums",
        user_id=int(user_id),
        search_query=search_query,
        genre=genre,
        label=label,
        year=int(year or 0),
        sort=sort,
        limit=int(limit),
        offset=int(offset),
        scope_suffix=_library_cache_scope_suffix(scope),
        unmatched_suffix=_library_cache_unmatched_suffix(include_unmatched),
        live_generation=live_cache_generation,
        browse_source=browse_source,
    )
    cache_key = cache_keys.cache_key
    stable_cache_key = cache_keys.stable_cache_key
    if not force_refresh:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            cached_source = str(cached.get("fallback_source") or "").strip().lower()
            if prefer_published:
                if cached_source == "published":
                    return jsonify(cached)
            elif cached_source != "published":
                return jsonify(cached)
        if not prefer_published:
            cached = _files_cache_get_json(stable_cache_key)
            if cached is not None:
                if str(cached.get("fallback_source") or "").strip().lower() == "published":
                    cached = None
            if cached is not None:
                payload = dict(cached)
                payload["stale"] = True
                payload["fallback_source"] = "stable_cache"
                return jsonify(payload)
    if snapshot is None:
        snapshot = _files_index_maybe_enqueue_published_catchup(
            include_unmatched=include_unmatched,
            scope=scope,
            reason=f"api_library_albums_{scope}",
        )
        browse_source = _files_library_browse_source_effective(
            scope=scope,
            requested=browse_source_requested,
            snapshot=snapshot,
            scan_busy=scan_busy,
        )
        prefer_published = browse_source == "published"
    if prefer_published:
        status_ctx = _files_library_live_status_context(source_is_published=True)
        payload = _files_library_published_albums(
            include_unmatched=include_unmatched,
            scope=scope,
            search_query=search_query,
            genre=genre,
            label=label,
            year=int(year or 0),
            sort=sort,
            limit=int(limit),
            offset=int(offset),
            status_context=status_ctx,
            allow_live_resolution=True,
        )
        payload["browse_source"] = browse_source
        _files_cache_set_json(cache_key, payload, ttl=15)
        return jsonify(payload)
    if not force_refresh and files_index_lock.locked():
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            if str(cached.get("fallback_source") or "").strip().lower() == "published":
                cached = None
        if cached is not None:
            payload = dict(cached)
            payload["stale"] = True
            return jsonify(payload)
        cached = _files_cache_get_json(stable_cache_key)
        if cached is not None:
            if str(cached.get("fallback_source") or "").strip().lower() == "published":
                cached = None
        if cached is not None:
            payload = dict(cached)
            payload["stale"] = True
            payload["fallback_source"] = "stable_cache"
            return jsonify(payload)

    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"albums": [], "total": 0, "limit": limit, "offset": offset, "error": err or "Files index unavailable"}), 503
    conn = _files_pg_connect(acquire_timeout_sec=0.75)
    if conn is None:
        cached = _files_cache_get_json(cache_key)
        if cached is not None:
            if str(cached.get("fallback_source") or "").strip().lower() == "published":
                cached = None
        if cached is not None:
            payload = dict(cached)
            payload["stale"] = True
            return jsonify(payload)
        cached = _files_cache_get_json(stable_cache_key)
        if cached is not None:
            payload = dict(cached)
            payload["stale"] = True
            payload["fallback_source"] = "stable_cache"
            return jsonify(payload)
        if int(snapshot.get("published_albums") or 0) > 0:
            return jsonify(
                _files_library_published_albums(
                    include_unmatched=include_unmatched,
                    scope=scope,
                    search_query=search_query,
                    genre=genre,
                    label=label,
                    year=int(year or 0),
                    sort=sort,
                    limit=int(limit),
                    offset=int(offset),
                    allow_live_resolution=False,
                )
            )
        return jsonify({"albums": [], "total": 0, "limit": limit, "offset": offset, "error": "PostgreSQL unavailable", "stale": True}), 503

    try:
        status_ctx = _files_library_live_status_context(source_is_published=False)
        where_parts = ["1=1", album_match_sql, album_scope_sql]
        params: list = []
        unfiltered_browse = not (search_query or genre or label or int(year or 0) > 0)
        if search_query:
            where_parts.append(
                """
                (
                    alb.title ILIKE %s
                    OR ar.name ILIKE %s
                    OR EXISTS (
                        SELECT 1
                        FROM files_tracks tr
                        WHERE tr.album_id = alb.id
                          AND tr.title ILIKE %s
                    )
                )
                """
            )
            like = f"%{search_query}%"
            params.extend([like, like, like])
        if year and int(year) > 0:
            where_parts.append("alb.year = %s")
            params.append(int(year))
        if genre:
            parts = [p.strip() for p in str(genre).split(",") if p.strip()]
            if parts:
                # Multi-genre support: album tags_json stores the full list (e.g. ["ambient","electronic"]).
                # We match case-insensitively against any token in that array.
                where_parts.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                        WHERE lower(trim(g.value)) = ANY(%s)
                    )
                    """
                )
                params.append([p.lower() for p in parts])
        if label:
            parts = [p.strip() for p in str(label).split(",") if p.strip()]
            if parts:
                where_parts.append("lower(COALESCE(alb.label, '')) = ANY(%s)")
                params.append([p.lower() for p in parts])

        if sort == "year_desc":
            order_sql = "ORDER BY COALESCE(alb.year, 0) DESC, alb.title ASC, alb.id DESC"
        elif sort == "alpha":
            order_sql = "ORDER BY alb.title ASC, alb.id DESC"
        elif sort == "artist":
            order_sql = "ORDER BY ar.name ASC, COALESCE(alb.year, 0) DESC, alb.title ASC, alb.id DESC"
        elif sort == "user_rating":
            order_sql = "ORDER BY COALESCE(ur.rating, 0) DESC, COALESCE(pr.public_rating, 0) DESC, COALESCE(pr.heat_score, 0) DESC, alb.updated_at DESC, alb.id DESC"
        elif sort == "public_rating":
            order_sql = "ORDER BY COALESCE(pr.public_rating, 0) DESC, COALESCE(pr.public_rating_votes, 0) DESC, COALESCE(pr.heat_score, 0) DESC, alb.updated_at DESC, alb.id DESC"
        elif sort == "heat":
            order_sql = "ORDER BY COALESCE(pr.heat_score, 0) DESC, COALESCE(pr.public_rating, 0) DESC, COALESCE(pr.public_rating_votes, 0) DESC, alb.updated_at DESC, alb.id DESC"
        else:
            # recent (default): created/updated activity during index build
            order_sql = "ORDER BY alb.created_at DESC, alb.id DESC"

        with conn.cursor() as cur:
            snapshot_total = int((snapshot or {}).get("pg_albums") or 0)
            if unfiltered_browse and snapshot_total > 0:
                total = snapshot_total
            else:
                with _files_pg_statement_timeout(cur, 2200):
                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM files_albums alb
                        LEFT JOIN files_artists ar ON ar.id = alb.artist_id
                        WHERE {" AND ".join(where_parts)}
                        """,
                        params,
                    )
                    total = int((cur.fetchone() or [0])[0] or 0)

            with _files_pg_statement_timeout(cur, 3500):
                cur.execute(
                    f"""
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
                    COALESCE(alb.sample_rate, 0) AS sample_rate,
                    COALESCE(alb.bit_depth, 0) AS bit_depth,
                    alb.has_cover,
                    COALESCE(alb.cover_path, '') AS cover_path,
                    COALESCE(alb.folder_path, '') AS folder_path,
                    COALESCE(alb.musicbrainz_release_group_id, '') AS musicbrainz_release_group_id,
                    COALESCE(alb.discogs_release_id, '') AS discogs_release_id,
                    COALESCE(alb.lastfm_album_mbid, '') AS lastfm_album_mbid,
                    COALESCE(alb.bandcamp_album_url, '') AS bandcamp_album_url,
                    COALESCE(alb.metadata_source, '') AS metadata_source,
                    COALESCE(alb.strict_match_provider, '') AS strict_match_provider,
                    COALESCE(alb.primary_tags_json, '{{}}') AS primary_tags_json,
                    alb.mb_identified,
                    ar.id AS artist_id,
                    ar.name AS artist_name,
                    COALESCE(
                        (
                            SELECT json_agg(display_name ORDER BY is_primary DESC, display_name ASC)
                            FROM (
                                SELECT
                                    COALESCE(NULLIF(BTRIM(comp.canonical_name), ''), NULLIF(BTRIM(comp.name), ''), '') AS display_name,
                                    COALESCE(link.is_primary, FALSE) AS is_primary
                                FROM files_artist_album_links link
                                JOIN files_artists comp ON comp.id = link.artist_id
                                WHERE link.album_id = alb.id
                                  AND COALESCE(comp.entity_kind, 'artist') = 'composer'
                            ) composer_names
                        )::text,
                        '[]'
                    ) AS composer_names_json,
                    COALESCE(pr.short_description, '') AS short_description,
                    COALESCE(pr.source, '') AS profile_source,
                    COALESCE(pr.tags_json, '[]') AS profile_tags_json,
                    pr.public_rating,
                    COALESCE(pr.public_rating_votes, 0) AS public_rating_votes,
                    COALESCE(pr.public_rating_source, '') AS public_rating_source,
                    pr.heat_score,
                    COALESCE(pr.heat_label, '') AS heat_label,
                    COALESCE(ur.rating, 0) AS user_rating
                FROM files_albums alb
                LEFT JOIN files_artists ar ON ar.id = alb.artist_id
                LEFT JOIN files_album_profiles pr
                       ON pr.artist_norm = COALESCE(ar.name_norm, '')
                      AND pr.title_norm = alb.title_norm
                LEFT JOIN files_user_album_ratings ur
                       ON ur.album_id = alb.id
                      AND ur.user_id = %s
                WHERE {" AND ".join(where_parts)}
                {order_sql}
                LIMIT %s OFFSET %s
                """,
                    [int(user_id), *params, int(limit), int(offset)],
                )
            rows = cur.fetchall()
            artist_has_image_by_id: dict[int, bool] = {}
            page_artist_ids = sorted({int(row[22] or 0) for row in rows if int(row[22] or 0) > 0})
            if page_artist_ids:
                try:
                    with _files_pg_statement_timeout(cur, 3500):
                        cur.execute(
                            f"""
                            SELECT
                                a.id,
                                COALESCE(({_artist_has_true_image_sql("a", "ext")}), FALSE) AS has_image
                            FROM files_artists a
                            LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                            WHERE a.id = ANY(%s)
                            """,
                            (page_artist_ids,),
                        )
                        artist_has_image_by_id = {
                            int(image_row[0] or 0): bool(image_row[1])
                            for image_row in (cur.fetchall() or [])
                            if int(image_row[0] or 0) > 0
                        }
                except Exception:
                    artist_has_image_by_id = {}

        base_url = request.url_root.rstrip("/")
        albums = []
        for album_id, title, year, genre, label, tags_json, track_count, fmt, is_lossless, sample_rate, bit_depth, has_cover, cover_path_raw, folder_path_raw, musicbrainz_release_group_id, discogs_release_id, lastfm_album_mbid, bandcamp_album_url, metadata_source, strict_match_provider, primary_tags_json_raw, mb_identified, artist_id, artist_name, composer_names_json, short_desc, profile_source, profile_tags_json, public_rating, public_rating_votes, public_rating_source, heat_score, heat_label, user_rating in rows:
            aid = int(album_id or 0)
            arid = int(artist_id or 0)
            # Album list endpoints must stay index-only. Verifying/extracting cover
            # assets here can walk album folders or read audio files for every grid
            # item; the cover image endpoint does that lazily when the browser
            # actually requests a thumbnail.
            has_cover_effective = bool(has_cover) or bool(str(cover_path_raw or "").strip())
            thumb = f"{base_url}/api/library/files/album/{aid}/cover?size=512" if has_cover_effective else None
            short_desc_clean = (short_desc or "").strip()
            # Parsed list of genres for UI badges (multi-genre albums).
            genres_list = _merge_album_genre_lists(tags_json, profile_tags_json, genre or "")
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
            has_profile = bool(short_desc_clean or str(profile_source or "").strip())
            enrichment_eligible = bool(
                mb_identified
                or str(metadata_source or "").strip()
                or str(strict_match_provider or "").strip()
                or str(musicbrainz_release_group_id or "").strip()
                or str(discogs_release_id or "").strip()
                or str(lastfm_album_mbid or "").strip()
                or str(bandcamp_album_url or "").strip()
            )
            status_fields = _files_library_album_status_fields(
                status_context=status_ctx,
                has_cover=bool(has_cover_effective),
                has_artist_image=bool(artist_has_image_by_id.get(arid)),
                has_profile=bool(has_profile),
                cover_eligible=bool(enrichment_eligible),
                artist_media_eligible=bool(arid or str(artist_name or "").strip()),
                profile_eligible=bool(enrichment_eligible),
            )
            albums.append(
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
                    "sample_rate": int(sample_rate or 0) or None,
                    "bit_depth": int(bit_depth or 0) or None,
                    "mb_identified": bool(mb_identified),
                    "musicbrainz_release_group_id": str(musicbrainz_release_group_id or "").strip() or None,
                    "discogs_release_id": str(discogs_release_id or "").strip() or None,
                    "lastfm_album_mbid": str(lastfm_album_mbid or "").strip() or None,
                    "bandcamp_album_url": str(bandcamp_album_url or "").strip() or None,
                    "metadata_source": _normalize_identity_provider(str(metadata_source or "")) or None,
                    "strict_match_provider": _normalize_identity_provider(str(strict_match_provider or "")) or None,
                    "has_cover": has_cover_effective,
                    "thumb": thumb,
                    "artist_id": arid,
                    "artist_name": _files_album_display_artist_name(
                        artist_name=str(artist_name or ""),
                        classical_payload=classical_payload,
                    ),
                    "short_description": short_desc_clean or None,
                    "profile_source": (profile_source or "").strip() or None,
                    "public_rating": float(public_rating) if public_rating is not None else None,
                    "public_rating_votes": int(public_rating_votes or 0),
                    "public_rating_source": (public_rating_source or "").strip() or None,
                    "heat_score": float(heat_score) if heat_score is not None else None,
                    "heat_label": None,
                    "user_rating": int(user_rating or 0) if int(user_rating or 0) > 0 else None,
                    **status_fields,
                }
            )

        payload = {
            "albums": albums,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": _pagination_core.page_has_more(total=total, offset=offset, returned=len(rows)),
            "scope": scope,
            "publication_state": str(status_ctx.get("publication_state") or "ready"),
            "background_enrichment_running": bool(status_ctx.get("background_enrichment_running")),
            "browse_source": browse_source,
        }
        if int(total or 0) <= 0 and _files_library_should_fallback_to_published(snapshot, albums=0, artists=0):
            status_ctx = _files_library_live_status_context(source_is_published=True)
            payload = _files_library_published_albums(
                include_unmatched=include_unmatched,
                scope=scope,
                search_query=search_query,
                genre=genre,
                label=label,
                year=int(year or 0),
                sort=sort,
                limit=int(limit),
                offset=int(offset),
                status_context=status_ctx,
                allow_live_resolution=False,
            )
            payload["fallback_source"] = "published"
            payload["browse_source"] = "published"
        _files_cache_set_json(cache_key, payload, ttl=20)
        if str(payload.get("fallback_source") or "").strip().lower() != "published":
            _files_cache_set_json(stable_cache_key, payload, ttl=300)
        return jsonify(payload)
    except Exception as e:
        logging.warning("library albums query falling back during scan: %s", e)
        cached = _files_cache_get_json(stable_cache_key)
        if cached is not None:
            if str(cached.get("fallback_source") or "").strip().lower() == "published":
                cached = None
        if cached is not None:
            payload = dict(cached)
            payload["stale"] = True
            payload["fallback_source"] = "stable_cache"
            return jsonify(payload)
        if scan_busy or _files_pg_is_statement_timeout_error(e):
            return jsonify(
                _files_library_published_albums(
                    include_unmatched=include_unmatched,
                    scope=scope,
                    search_query=search_query,
                    genre=genre,
                    label=label,
                    year=int(year or 0),
                    sort=sort,
                    limit=int(limit),
                    offset=int(offset),
                    allow_live_resolution=False,
                )
            )
        raise
    finally:
        conn.close()
