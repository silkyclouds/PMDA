"""Recommendation and discovery scoring runtime helpers for PMDA."""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None

_EXTRACTED_NAMES = {
    '_rebuild_files_reco_embeddings',
    '_reco_event_weight',
    '_reco_build_track_embeddings',
    '_reco_build_track_embeddings_chunked',
    '_reco_upsert_track_embeddings_for_album_ids',
    '_entity_discover_ai_summary',
    '_reco_genre_tokens',
    '_reco_fetch_embeddings_map',
    '_reco_build_session_profile',
    '_reco_fetch_candidates',
    '_reco_rank_candidates',
    '_reco_record_event',
    '_files_backfill_trusted_match_flags',
    '_files_similar_artists_by_genre',
}


def _bind_runtime(runtime: Any) -> None:
    global _RUNTIME
    _RUNTIME = runtime
    for name, value in vars(runtime).items():
        if name in _EXTRACTED_NAMES:
            if getattr(value, "__module__", "") != getattr(runtime, "__name__", ""):
                globals()[name] = value
            else:
                original = _ORIGINAL_EXTRACTED_FUNCTIONS.get(name)
                if original is not None:
                    globals()[name] = original
            continue
        own_wrapper = name.endswith("_for_runtime") and name[: -len("_for_runtime")] in _EXTRACTED_NAMES
        if name == "_bind_runtime" or own_wrapper:
            continue
        globals()[name] = value


def _runtime_module() -> Any:
    if _RUNTIME is None:
        raise RuntimeError("Recommendation runtime is not bound")
    return _RUNTIME


def _rebuild_files_reco_embeddings(reason: str = "manual", wait_if_running: bool = False) -> dict:
    if _get_library_mode() != "files":
        return {"ok": False, "error": "LIBRARY_MODE is not 'files'"}
    if not _files_pg_init_schema():
        return {"ok": False, "error": "PostgreSQL schema unavailable"}

    if files_index_lock.locked():
        if not wait_if_running:
            return {"ok": False, "running": True, "error": "Files index rebuild already running"}
        logging.info("Files reco embedding rebuild waiting for Files index publication to finish")
        files_index_lock.acquire()
        files_index_lock.release()

    acquired = _FILES_RECO_EMBED_BUILD_LOCK.acquire(blocking=wait_if_running)
    if not acquired:
        return {"ok": False, "running": True, "error": "Files reco embedding rebuild already running"}
    try:
        started_at = time.time()
        tracks_total, embeddings_before = _files_index_read_track_and_embedding_counts()
        _files_reco_embeddings_set_state(
            running=True,
            reason=str(reason or "manual"),
            started_at=started_at,
            finished_at=None,
            tracks_total=int(tracks_total or 0),
            embeddings_before=int(embeddings_before or 0),
            embeddings_done=0,
            progress=0.0,
            error=None,
        )
        conn = _files_pg_connect()
        if conn is None:
            raise RuntimeError("PostgreSQL connection unavailable during embedding rebuild")
        try:
            def _progress(done: int, _last_track_id: int) -> None:
                total = max(0, int(tracks_total or 0))
                progress = round(min(100.0, (float(done) / float(total)) * 100.0), 2) if total else None
                _files_reco_embeddings_set_state(
                    running=True,
                    embeddings_done=int(done or 0),
                    tracks_total=total,
                    progress=progress,
                )

            embedding_count = _reco_build_track_embeddings_chunked(conn, progress_cb=_progress)
            with conn.transaction():
                with conn.cursor() as cur:
                    _files_index_write_meta(cur, "track_embeddings", str(embedding_count))
                    _files_index_write_meta(cur, "track_embeddings_source", RECO_EMBED_SOURCE)
                    _files_index_write_meta(cur, "track_embeddings_reason", reason)
                    _files_index_write_meta(cur, "track_embeddings_ts", str(int(time.time())))
        finally:
            conn.close()
        _files_cache_invalidate_all()
        artists, albums, tracks = _files_index_read_counts()
        _files_reco_embeddings_set_state(
            running=False,
            finished_at=time.time(),
            tracks_total=int(tracks or 0),
            embeddings_done=int(embedding_count or 0),
            progress=100.0,
            artists=artists,
            albums=albums,
            tracks=tracks,
            error=None,
        )
        elapsed = round(time.time() - started_at, 2)
        logging.info(
            "Files reco embeddings rebuilt (%s): %d embedding(s) in %.2fs",
            reason,
            embedding_count,
            elapsed,
        )
        return {
            "ok": True,
            "track_embeddings": embedding_count,
            "duration_sec": elapsed,
        }
    except Exception as e:
        logging.exception("Files reco embedding rebuild failed: %s", e)
        _files_reco_embeddings_set_state(
            running=False,
            finished_at=time.time(),
            error=str(e),
        )
        return {"ok": False, "error": str(e)}
    finally:
        _FILES_RECO_EMBED_BUILD_LOCK.release()


def _reco_event_weight(event_type: str, played_seconds: int) -> float:
    et = (event_type or "").strip().lower()
    played = max(0, int(played_seconds or 0))
    if et == "like":
        return 1.8
    if et == "dislike":
        return -2.0
    if et == "play_complete":
        return 1.0
    if et == "play_partial":
        return 0.45
    if et == "play_start":
        return 0.25 if played >= 15 else 0.1
    if et == "skip":
        return -0.9
    if et == "stop":
        return 0.12 if played >= 20 else -0.25
    return 0.0


def _reco_build_track_embeddings(conn) -> int:
    """Rebuild deterministic per-track embeddings used for recommendation ranking."""
    inserted = 0
    with conn.cursor() as write_cur:
        write_cur.execute("DELETE FROM files_track_embeddings")
    with conn.cursor() as read_cur, conn.cursor() as write_cur:
        read_cur.execute(
            """
            SELECT
                tr.id,
                tr.title,
                COALESCE(alb.title, ''),
                COALESCE(alb.genre, ''),
                COALESCE(ar.name, ''),
                COALESCE(alb.year::text, ''),
                COALESCE(tr.format, ''),
                COALESCE(alb.tags_json, '[]')
            FROM files_tracks tr
            JOIN files_albums alb ON alb.id = tr.album_id
            JOIN files_artists ar ON ar.id = alb.artist_id
            ORDER BY tr.id ASC
            """
        )
        while True:
            rows = read_cur.fetchmany(2000)
            if not rows:
                break
            batch: list[tuple] = []
            for row in rows:
                track_id = int(row[0])
                title = row[1] or ""
                album_title = row[2] or ""
                genre = row[3] or ""
                artist = row[4] or ""
                year_text = row[5] or ""
                fmt = row[6] or ""
                tags_json = row[7] or "[]"
                try:
                    tags_list = json.loads(tags_json) if tags_json else []
                except Exception:
                    tags_list = []
                tags_part = " ".join(str(t or "").strip() for t in tags_list[:12] if str(t or "").strip())
                text = " ".join(
                    p for p in [artist, album_title, title, genre, tags_part, year_text, fmt] if p
                )
                vec, norm = _build_hashed_embedding(text, RECO_EMBED_DIM)
                if norm <= 0:
                    continue
                batch.append((track_id, json.dumps(vec, separators=(",", ":")), float(norm), RECO_EMBED_SOURCE))
            if batch:
                write_cur.executemany(
                    """
                    INSERT INTO files_track_embeddings(track_id, embed_json, norm, source, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (track_id) DO UPDATE
                    SET embed_json = EXCLUDED.embed_json,
                        norm = EXCLUDED.norm,
                        source = EXCLUDED.source,
                        updated_at = NOW()
                    """,
                    batch,
                )
                inserted += len(batch)
    return inserted


def _reco_build_track_embeddings_chunked(
    conn,
    *,
    batch_size: int = 2000,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> int:
    """Rebuild deterministic embeddings in short transactions so browsing stays usable."""
    inserted = 0
    last_track_id = 0
    batch_n = max(250, int(batch_size or 2000))
    with conn.transaction():
        with conn.cursor() as write_cur:
            write_cur.execute("DELETE FROM files_track_embeddings")

    while True:
        with conn.transaction():
            with conn.cursor() as read_cur:
                read_cur.execute(
                    """
                    SELECT
                        tr.id,
                        tr.title,
                        COALESCE(alb.title, ''),
                        COALESCE(alb.genre, ''),
                        COALESCE(ar.name, ''),
                        COALESCE(alb.year::text, ''),
                        COALESCE(tr.format, ''),
                        COALESCE(alb.tags_json, '[]')
                    FROM files_tracks tr
                    JOIN files_albums alb ON alb.id = tr.album_id
                    JOIN files_artists ar ON ar.id = alb.artist_id
                    WHERE tr.id > %s
                    ORDER BY tr.id ASC
                    LIMIT %s
                    """,
                    (last_track_id, batch_n),
                )
                rows = read_cur.fetchall() or []
        if not rows:
            break

        batch: list[tuple] = []
        for row in rows:
            track_id = int(row[0])
            last_track_id = max(last_track_id, track_id)
            title = row[1] or ""
            album_title = row[2] or ""
            genre = row[3] or ""
            artist = row[4] or ""
            year_text = row[5] or ""
            fmt = row[6] or ""
            tags_json = row[7] or "[]"
            try:
                tags_list = json.loads(tags_json) if tags_json else []
            except Exception:
                tags_list = []
            tags_part = " ".join(str(t or "").strip() for t in tags_list[:12] if str(t or "").strip())
            text = " ".join(p for p in [artist, album_title, title, genre, tags_part, year_text, fmt] if p)
            vec, norm = _build_hashed_embedding(text, RECO_EMBED_DIM)
            if norm <= 0:
                continue
            batch.append((track_id, json.dumps(vec, separators=(",", ":")), float(norm), RECO_EMBED_SOURCE))

        if batch:
            with conn.transaction():
                with conn.cursor() as write_cur:
                    write_cur.executemany(
                        """
                        INSERT INTO files_track_embeddings(track_id, embed_json, norm, source, updated_at)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON CONFLICT (track_id) DO UPDATE
                        SET embed_json = EXCLUDED.embed_json,
                            norm = EXCLUDED.norm,
                            source = EXCLUDED.source,
                            updated_at = NOW()
                        """,
                        batch,
                    )
            inserted += len(batch)
            if progress_cb:
                try:
                    progress_cb(inserted, last_track_id)
                except Exception:
                    pass
    return inserted


def _reco_upsert_track_embeddings_for_album_ids(conn, album_ids: list[int]) -> int:
    """
    Rebuild deterministic embeddings only for tracks belonging to the provided album IDs.
    Used by granular Files index upserts to avoid full table rebuild each time.
    """
    cleaned_ids = sorted({int(a) for a in (album_ids or []) if int(a) > 0})
    if not cleaned_ids:
        return 0
    inserted = 0
    with conn.cursor() as write_cur:
        write_cur.execute(
            """
            DELETE FROM files_track_embeddings
            WHERE track_id IN (
                SELECT id FROM files_tracks WHERE album_id = ANY(%s)
            )
            """,
            (cleaned_ids,),
        )
    with conn.cursor() as read_cur, conn.cursor() as write_cur:
        read_cur.execute(
            """
            SELECT
                tr.id,
                tr.title,
                COALESCE(alb.title, ''),
                COALESCE(alb.genre, ''),
                COALESCE(ar.name, ''),
                COALESCE(alb.year::text, ''),
                COALESCE(tr.format, ''),
                COALESCE(alb.tags_json, '[]')
            FROM files_tracks tr
            JOIN files_albums alb ON alb.id = tr.album_id
            JOIN files_artists ar ON ar.id = alb.artist_id
            WHERE tr.album_id = ANY(%s)
            ORDER BY tr.id ASC
            """,
            (cleaned_ids,),
        )
        while True:
            rows = read_cur.fetchmany(2000)
            if not rows:
                break
            batch: list[tuple] = []
            for row in rows:
                track_id = int(row[0])
                title = row[1] or ""
                album_title = row[2] or ""
                genre = row[3] or ""
                artist = row[4] or ""
                year_text = row[5] or ""
                fmt = row[6] or ""
                tags_json = row[7] or "[]"
                try:
                    tags_list = json.loads(tags_json) if tags_json else []
                except Exception:
                    tags_list = []
                tags_part = " ".join(str(t or "").strip() for t in tags_list[:12] if str(t or "").strip())
                text = " ".join(p for p in [artist, album_title, title, genre, tags_part, year_text, fmt] if p)
                vec, norm = _build_hashed_embedding(text, RECO_EMBED_DIM)
                if norm <= 0:
                    continue
                batch.append((track_id, json.dumps(vec, separators=(",", ":")), float(norm), RECO_EMBED_SOURCE))
            if batch:
                write_cur.executemany(
                    """
                    INSERT INTO files_track_embeddings(track_id, embed_json, norm, source, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (track_id) DO UPDATE
                    SET embed_json = EXCLUDED.embed_json,
                        norm = EXCLUDED.norm,
                        source = EXCLUDED.source,
                        updated_at = NOW()
                    """,
                    batch,
                )
                inserted += len(batch)
    return inserted


def _entity_discover_ai_summary(
    *,
    entity_type: str,
    entity_label: str,
    context_lines: list[str],
    sections: list[dict[str, Any]],
    user_id: int = 0,
) -> tuple[str, str, str, bool]:
    runtime = _assistant_runtime_status(user_id=user_id)
    if not bool(runtime.get("ai_ready")):
        return ("", str(runtime.get("ai_provider") or ""), str(runtime.get("ai_model") or ""), False)
    provider = str(runtime.get("ai_provider") or getattr(_runtime_module(), "AI_PROVIDER", "openai")).strip() or "openai"
    model = str(runtime.get("ai_model") or getattr(_runtime_module(), "RESOLVED_MODEL", "") or getattr(_runtime_module(), "OPENAI_MODEL", "")).strip()
    system_msg = (
        "You are PMDA Intelligence. Write concise music-discovery guidance for a local music-library UI.\n"
        "Output ONLY a JSON object with keys:\n"
        "- summary: string\n"
        "- section_reasons: object mapping section key -> short reason string\n"
        "Rules:\n"
        "- Use only the provided local-library context and web findings.\n"
        "- Prefer recommending items already present in the local library.\n"
        "- Mention external links only as discovery extensions when they are not already local.\n"
        "- Keep summary under 140 words.\n"
    )
    payload = {
        "entity_type": entity_type,
        "entity_label": entity_label,
        "context": context_lines[:20],
        "sections": [
            {
                "key": str(sec.get("key") or ""),
                "title": str(sec.get("title") or ""),
                "links": [
                    {
                        "label": str(link.get("label") or ""),
                        "subtitle": str(link.get("subtitle") or ""),
                        "kind": str(link.get("kind") or ""),
                    }
                    for link in (sec.get("links") or [])[:8]
                ],
            }
            for sec in sections[:6]
        ],
    }
    try:
        raw = call_ai_provider_longform(
            provider,
            model,
            system_msg,
            json.dumps(payload, ensure_ascii=False),
            max_tokens=500,
            analysis_type="other",
        )
        obj = _assistant_extract_json_obj(raw)
        if not isinstance(obj, dict):
            return ("", provider, model, False)
        summary = str(obj.get("summary") or "").strip()
        section_reasons = obj.get("section_reasons") if isinstance(obj.get("section_reasons"), dict) else {}
        if summary:
            for sec in sections:
                key = str(sec.get("key") or "").strip()
                if key and str(section_reasons.get(key) or "").strip():
                    sec["reason"] = str(section_reasons.get(key) or "").strip()
            return (summary, provider, model, True)
        return ("", provider, model, False)
    except Exception as exc:
        logging.warning(
            "Entity discover AI summary failed entity_type=%s entity_label=%s provider=%s model=%s: %s",
            entity_type,
            entity_label,
            provider,
            model,
            exc,
        )
        return ("", provider, model, False)


def _reco_genre_tokens(raw_genre: str) -> list[str]:
    tokens = []
    for part in re.split(r"[;,/|]+", (raw_genre or "").lower()):
        txt = re.sub(r"\s+", " ", (part or "").strip())
        if txt:
            tokens.append(txt)
    return tokens[:12]


def _reco_fetch_embeddings_map(conn, track_ids: list[int]) -> dict[int, list[float]]:
    if not track_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT track_id, embed_json
            FROM files_track_embeddings
            WHERE track_id = ANY(%s)
            """,
            (track_ids,),
        )
        rows = cur.fetchall()
    out: dict[int, list[float]] = {}
    for track_id, embed_json in rows:
        out[int(track_id)] = _load_embedding_json(embed_json or "")
    return out


def _reco_build_session_profile(conn, session_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                e.id,
                e.event_type,
                e.track_id,
                e.played_seconds,
                EXTRACT(EPOCH FROM (NOW() - e.created_at))::DOUBLE PRECISION AS age_sec,
                COALESCE(e.artist_id, 0),
                COALESCE(e.album_id, 0),
                COALESCE(alb.genre, '')
            FROM files_reco_events e
            LEFT JOIN files_albums alb ON alb.id = e.album_id
            WHERE e.session_id = %s
            ORDER BY e.created_at DESC
            LIMIT 400
            """,
            (session_id,),
        )
        rows = cur.fetchall()
    profile = {
        "has_data": bool(rows),
        "recent_track_ids": [],
        "negative_track_ids": set(),
        "artist_weights": defaultdict(float),
        "genre_weights": defaultdict(float),
        "track_weights": defaultdict(float),
        "session_event_count": len(rows),
    }
    if not rows:
        profile["centroid"] = []
        return profile

    recent_limit = 40
    seen_recent = set()
    for idx, row in enumerate(rows):
        _event_id = int(row[0] or 0)
        event_type = str(row[1] or "").strip().lower()
        track_id = int(row[2] or 0)
        played_seconds = int(row[3] or 0)
        age_sec = max(0.0, float(row[4] or 0.0))
        artist_id = int(row[5] or 0)
        album_genre = str(row[7] or "")
        decay = math.exp(-(age_sec / (72.0 * 3600.0)))
        weight = _reco_event_weight(event_type, played_seconds) * decay

        if track_id > 0 and track_id not in seen_recent and len(profile["recent_track_ids"]) < recent_limit:
            profile["recent_track_ids"].append(track_id)
            seen_recent.add(track_id)
        if track_id > 0 and weight < -0.4:
            profile["negative_track_ids"].add(track_id)
        if track_id > 0 and abs(weight) > 0.01:
            profile["track_weights"][track_id] += weight
        if artist_id > 0 and abs(weight) > 0.01:
            profile["artist_weights"][artist_id] += weight
        for gt in _reco_genre_tokens(album_genre):
            profile["genre_weights"][gt] += weight

        # Extra recency emphasis on first events in the list.
        if idx < 6 and track_id > 0:
            profile["track_weights"][track_id] += 0.12

    # Normalize artist and genre affinity to approximately [-1, 1].
    for key in ("artist_weights", "genre_weights"):
        mapping = profile[key]
        if not mapping:
            continue
        max_abs = max(abs(v) for v in mapping.values()) or 1.0
        for mk in list(mapping.keys()):
            mapping[mk] = float(mapping[mk]) / float(max_abs)

    positive_track_ids = [int(tid) for tid, w in profile["track_weights"].items() if w > 0.03]
    emb_map = _reco_fetch_embeddings_map(conn, positive_track_ids)
    centroid = [0.0] * RECO_EMBED_DIM
    total_w = 0.0
    for tid, weight in profile["track_weights"].items():
        if weight <= 0.03:
            continue
        emb = emb_map.get(int(tid)) or []
        if not emb:
            continue
        w = float(weight)
        for i in range(min(RECO_EMBED_DIM, len(emb))):
            centroid[i] += emb[i] * w
        total_w += w
    norm = math.sqrt(sum(v * v for v in centroid))
    if norm > 0:
        inv = 1.0 / norm
        centroid = [v * inv for v in centroid]
    profile["centroid"] = centroid
    profile["positive_track_ids"] = positive_track_ids

    # Keep top affinities for SQL filtering.
    sorted_artists = sorted(
        ((int(k), float(v)) for k, v in profile["artist_weights"].items() if v > 0.08),
        key=lambda x: x[1],
        reverse=True,
    )
    sorted_genres = sorted(
        ((str(k), float(v)) for k, v in profile["genre_weights"].items() if v > 0.08),
        key=lambda x: x[1],
        reverse=True,
    )
    profile["top_artist_ids"] = [aid for aid, _ in sorted_artists[:12]]
    profile["top_genres"] = [g for g, _ in sorted_genres[:10]]
    return profile


def _reco_fetch_candidates(conn, profile: dict, limit: int) -> list[dict]:
    limit = max(100, min(5000, int(limit or 500)))
    recent_track_ids = [int(x) for x in (profile.get("recent_track_ids") or []) if int(x) > 0]
    top_artist_ids = [int(x) for x in (profile.get("top_artist_ids") or []) if int(x) > 0]
    top_genres = [str(x or "").strip() for x in (profile.get("top_genres") or []) if str(x or "").strip()]

    where_parts = ["1=1"]
    params: list = []
    if recent_track_ids:
        where_parts.append("t.id <> ALL(%s)")
        params.append(recent_track_ids[:120])

    pref_parts = []
    if top_artist_ids:
        pref_parts.append("ar.id = ANY(%s)")
        params.append(top_artist_ids[:24])
    if top_genres:
        for g in top_genres[:8]:
            pref_parts.append("alb.genre ILIKE %s")
            params.append(f"%{g}%")
    if pref_parts:
        where_parts.append("(" + " OR ".join(pref_parts) + ")")

    sql = f"""
        SELECT
            t.id,
            t.title,
            t.duration_sec,
            t.track_num,
            alb.id AS album_id,
            alb.title AS album_title,
            COALESCE(alb.genre, '') AS album_genre,
            COALESCE(alb.year, 0) AS album_year,
            ar.id AS artist_id,
            ar.name AS artist_name,
            alb.has_cover,
            COALESCE(st.play_count, 0) AS play_count,
            COALESCE(st.completion_count, 0) AS completion_count,
            COALESCE(st.skip_count, 0) AS skip_count,
            COALESCE(emb.embed_json, '[]') AS embed_json
        FROM files_tracks t
        JOIN files_albums alb ON alb.id = t.album_id
        JOIN files_artists ar ON ar.id = alb.artist_id
        LEFT JOIN files_reco_track_stats st ON st.track_id = t.id
        LEFT JOIN files_track_embeddings emb ON emb.track_id = t.id
        WHERE {" AND ".join(where_parts)}
        ORDER BY COALESCE(st.play_count, 0) DESC, COALESCE(alb.year, 0) DESC, t.id DESC
        LIMIT %s
    """
    params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    # If strict preferences yielded too few tracks, widen the net.
    if len(rows) < min(30, limit // 4):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    t.id,
                    t.title,
                    t.duration_sec,
                    t.track_num,
                    alb.id AS album_id,
                    alb.title AS album_title,
                    COALESCE(alb.genre, '') AS album_genre,
                    COALESCE(alb.year, 0) AS album_year,
                    ar.id AS artist_id,
                    ar.name AS artist_name,
                    alb.has_cover,
                    COALESCE(st.play_count, 0) AS play_count,
                    COALESCE(st.completion_count, 0) AS completion_count,
                    COALESCE(st.skip_count, 0) AS skip_count,
                    COALESCE(emb.embed_json, '[]') AS embed_json
                FROM files_tracks t
                JOIN files_albums alb ON alb.id = t.album_id
                JOIN files_artists ar ON ar.id = alb.artist_id
                LEFT JOIN files_reco_track_stats st ON st.track_id = t.id
                LEFT JOIN files_track_embeddings emb ON emb.track_id = t.id
                ORDER BY COALESCE(st.play_count, 0) DESC, COALESCE(alb.year, 0) DESC, t.id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

    out = []
    for row in rows:
        out.append(
            {
                "track_id": int(row[0]),
                "title": row[1] or "",
                "duration_sec": int(row[2] or 0),
                "track_num": int(row[3] or 0),
                "album_id": int(row[4] or 0),
                "album_title": row[5] or "",
                "album_genre": row[6] or "",
                "album_year": int(row[7] or 0),
                "artist_id": int(row[8] or 0),
                "artist_name": row[9] or "",
                "has_cover": bool(row[10]),
                "play_count": int(row[11] or 0),
                "completion_count": int(row[12] or 0),
                "skip_count": int(row[13] or 0),
                "embedding": _load_embedding_json(row[14] or "[]"),
            }
        )
    return out


def _reco_rank_candidates(profile: dict, candidates: list[dict], limit: int) -> list[dict]:
    limit = max(1, min(100, int(limit or 20)))
    if not candidates:
        return []
    centroid = profile.get("centroid") or []
    artist_weights = profile.get("artist_weights") or {}
    genre_weights = profile.get("genre_weights") or {}
    negative_track_ids = set(profile.get("negative_track_ids") or set())
    recent_track_ids = set(profile.get("recent_track_ids") or [])

    max_play_count = max((int(c.get("play_count") or 0) for c in candidates), default=0)
    log_den = math.log1p(max_play_count) if max_play_count > 0 else 1.0
    year_values = [int(c.get("album_year") or 0) for c in candidates if int(c.get("album_year") or 0) > 0]
    max_year = max(year_values) if year_values else 0
    min_year = min(year_values) if year_values else 0
    year_span = max(1, max_year - min_year) if max_year > 0 else 1

    scored: list[dict] = []
    for c in candidates:
        track_id = int(c.get("track_id") or 0)
        artist_id = int(c.get("artist_id") or 0)
        play_count = int(c.get("play_count") or 0)
        completion_count = int(c.get("completion_count") or 0)
        skip_count = int(c.get("skip_count") or 0)

        emb = c.get("embedding") or []
        emb_score = _vec_cosine(centroid, emb) if centroid and emb else 0.0
        artist_affinity = float(artist_weights.get(artist_id, 0.0))
        genre_affinity = 0.0
        for gt in _reco_genre_tokens(c.get("album_genre") or ""):
            genre_affinity += float(genre_weights.get(gt, 0.0))
        genre_affinity = max(-1.0, min(1.0, genre_affinity))

        pop_score = (math.log1p(play_count) / log_den) if play_count > 0 and log_den > 0 else 0.0
        completion_rate = (float(completion_count) / float(play_count)) if play_count > 0 else 0.0
        skip_rate = float(skip_count) / float(max(1, play_count + skip_count))

        year_val = int(c.get("album_year") or 0)
        recency = ((year_val - min_year) / year_span) if year_val > 0 and max_year > min_year else 0.0

        score = (
            0.58 * emb_score
            + 0.18 * artist_affinity
            + 0.12 * genre_affinity
            + 0.06 * pop_score
            + 0.03 * completion_rate
            + 0.03 * recency
            - 0.20 * skip_rate
        )
        if track_id in recent_track_ids:
            score -= 0.95
        if track_id in negative_track_ids:
            score -= 1.25

        reasons: list[str] = []
        if emb_score >= 0.40:
            reasons.append("embedding match")
        if artist_affinity >= 0.20:
            reasons.append("artist affinity")
        if genre_affinity >= 0.20:
            reasons.append("genre affinity")
        if pop_score >= 0.55:
            reasons.append("popular")
        c["score"] = float(score)
        c["reasons"] = reasons[:3]
        scored.append(c)

    scored.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)

    # Simple diversity pass to avoid too many tracks from the same album/artist.
    selected: list[dict] = []
    album_seen: dict[int, int] = defaultdict(int)
    artist_seen: dict[int, int] = defaultdict(int)
    for cand in scored:
        album_id = int(cand.get("album_id") or 0)
        artist_id = int(cand.get("artist_id") or 0)
        adjusted = float(cand.get("score") or 0.0)
        adjusted -= 0.10 * float(album_seen.get(album_id, 0))
        adjusted -= 0.05 * float(artist_seen.get(artist_id, 0))
        cand["score"] = adjusted
        if adjusted < -0.9:
            continue
        selected.append(cand)
        album_seen[album_id] += 1
        artist_seen[artist_id] += 1
        if len(selected) >= limit:
            break

    selected.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    return selected[:limit]


def _reco_record_event(conn, session_id: str, track_id: int, event_type: str, played_seconds: int = 0) -> tuple[bool, str]:
    et = str(event_type or "").strip().lower()
    allowed = {"play_start", "play_partial", "play_complete", "skip", "stop", "like", "dislike"}
    if et not in allowed:
        return False, "unsupported event_type"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.id, t.album_id, alb.artist_id
            FROM files_tracks t
            JOIN files_albums alb ON alb.id = t.album_id
            WHERE t.id = %s
            """,
            (int(track_id or 0),),
        )
        row = cur.fetchone()
        if not row:
            return False, "track not found"
        resolved_track_id = int(row[0] or 0)
        album_id = int(row[1] or 0)
        artist_id = int(row[2] or 0)

        cur.execute(
            """
            INSERT INTO files_reco_events(session_id, track_id, album_id, artist_id, event_type, played_seconds, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """,
            (session_id, resolved_track_id, album_id, artist_id, et, max(0, int(played_seconds or 0))),
        )

        play_inc = 1 if et in {"play_complete", "like"} else 0
        completion_inc = 1 if et in {"play_complete", "like"} else 0
        partial_inc = 1 if et in {"play_partial", "play_start", "stop"} else 0
        skip_inc = 1 if et in {"skip", "dislike"} else 0
        cur.execute(
            """
            INSERT INTO files_reco_track_stats(
                track_id, play_count, completion_count, partial_count, skip_count, last_event_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (track_id) DO UPDATE
            SET play_count = files_reco_track_stats.play_count + EXCLUDED.play_count,
                completion_count = files_reco_track_stats.completion_count + EXCLUDED.completion_count,
                partial_count = files_reco_track_stats.partial_count + EXCLUDED.partial_count,
                skip_count = files_reco_track_stats.skip_count + EXCLUDED.skip_count,
                last_event_at = NOW(),
                updated_at = NOW()
            """,
            (resolved_track_id, play_inc, completion_inc, partial_inc, skip_inc),
        )
        cur.execute(
            """
            INSERT INTO files_reco_sessions(session_id, last_event_at, created_at, total_events)
            VALUES (%s, NOW(), NOW(), 1)
            ON CONFLICT (session_id) DO UPDATE
            SET last_event_at = NOW(),
                total_events = files_reco_sessions.total_events + 1
            """,
            (session_id,),
        )
    return True, "ok"


def _files_backfill_trusted_match_flags() -> int:
    """Backfill files_albums legacy flags for strict identity and non-blocking genre completeness."""
    conn = _files_pg_connect()
    if conn is None:
        return 0
    changed = 0
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                expr = """
                    (
                        COALESCE(strict_match_verified, FALSE) = TRUE
                        OR COALESCE(NULLIF(TRIM(musicbrainz_release_group_id), ''), '') <> ''
                        OR COALESCE(NULLIF(TRIM(discogs_release_id), ''), '') <> ''
                        OR COALESCE(NULLIF(TRIM(lastfm_album_mbid), ''), '') <> ''
                        OR COALESCE(NULLIF(TRIM(bandcamp_album_url), ''), '') <> ''
                    )
                """
                cur.execute(
                    f"""
                    UPDATE files_albums
                    SET mb_identified = CASE WHEN {expr} THEN TRUE ELSE FALSE END
                    WHERE mb_identified IS DISTINCT FROM CASE WHEN {expr} THEN TRUE ELSE FALSE END
                    """
                )
                changed = int(cur.rowcount or 0)
                # Legacy rows may still mark only "genre" as missing; genre is non-blocking now.
                cur.execute(
                    """
                    SELECT id, COALESCE(missing_required_tags_json, '[]')
                    FROM files_albums
                    WHERE COALESCE(missing_required_tags_json, '') <> ''
                      AND missing_required_tags_json <> '[]'
                    """
                )
                tag_updates: list[tuple[str, int]] = []
                for album_id, raw_missing in cur.fetchall():
                    try:
                        parsed = json.loads(raw_missing) if raw_missing else []
                    except Exception:
                        parsed = []
                    if not isinstance(parsed, list):
                        parsed = []
                    filtered = [m for m in parsed if str(m or "").strip().lower() != "genre"]
                    if filtered != parsed:
                        tag_updates.append((json.dumps(filtered), int(album_id)))
                if tag_updates:
                    cur.executemany(
                        "UPDATE files_albums SET missing_required_tags_json = %s WHERE id = %s",
                        tag_updates,
                    )
                    changed += len(tag_updates)
        if changed > 0:
            logging.info("Files index: backfilled trusted identity/completeness flags on %d album row(s).", changed)
    except Exception:
        logging.debug("Failed to backfill trusted match flags", exc_info=True)
    finally:
        conn.close()


def _files_similar_artists_by_genre(conn, artist_id: int, *, limit: int = 20) -> list[dict]:
    """
    Local fallback: suggest similar artists from the local library by overlapping inferred genres.
    This is a safe fallback when provider similar artists are missing or unreliable.
    """
    artist_id = int(artist_id or 0)
    limit = max(1, min(40, int(limit or 20)))
    if artist_id <= 0:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT LOWER(TRIM(g.value)) AS g, COUNT(DISTINCT alb.id) AS c
                FROM files_artist_album_links link
                JOIN files_albums alb ON alb.id = link.album_id
                CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                WHERE link.artist_id = %s
                  AND COALESCE(TRIM(g.value), '') <> ''
                GROUP BY LOWER(TRIM(g.value))
                ORDER BY COUNT(DISTINCT alb.id) DESC, g ASC
                LIMIT 12
                """,
                (artist_id,),
            )
            target_genres = [str(r[0] or "").strip() for r in cur.fetchall() if str(r[0] or "").strip()]
        if not target_genres:
            return []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    a.id,
                    a.name,
                    COUNT(DISTINCT LOWER(TRIM(g.value))) AS score,
                    ARRAY_AGG(DISTINCT LOWER(TRIM(g.value))) AS matched_genres
                FROM files_artists a
                JOIN files_artist_album_links link ON link.artist_id = a.id
                JOIN files_albums alb ON alb.id = link.album_id
                CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                WHERE a.id <> %s
                  AND COALESCE(TRIM(g.value), '') <> ''
                  AND LOWER(TRIM(g.value)) = ANY(%s)
                GROUP BY a.id, a.name, a.album_count
                ORDER BY score DESC, a.album_count DESC, a.name ASC
                LIMIT %s
                """,
                (artist_id, target_genres, limit),
            )
            rows = cur.fetchall()
        out: list[dict] = []
        for aid, name, score, matched_genres in rows:
            nm = str(name or "").strip()
            if not nm:
                continue
            mg: list[str] = []
            if isinstance(matched_genres, list):
                for g in matched_genres:
                    gg = str(g or "").strip()
                    if gg:
                        mg.append(gg)
            label = f"Genre: {', '.join(mg[:2])}" if mg else "Genre match"
            out.append(
                {
                    "name": nm,
                    "artist_id": int(aid or 0),
                    "type": label,
                    "score": int(score or 0),
                }
            )
        return out[:limit]
    except Exception:
        return []


_ORIGINAL_EXTRACTED_FUNCTIONS = {
    '_rebuild_files_reco_embeddings': _rebuild_files_reco_embeddings,
    '_reco_event_weight': _reco_event_weight,
    '_reco_build_track_embeddings': _reco_build_track_embeddings,
    '_reco_build_track_embeddings_chunked': _reco_build_track_embeddings_chunked,
    '_reco_upsert_track_embeddings_for_album_ids': _reco_upsert_track_embeddings_for_album_ids,
    '_entity_discover_ai_summary': _entity_discover_ai_summary,
    '_reco_genre_tokens': _reco_genre_tokens,
    '_reco_fetch_embeddings_map': _reco_fetch_embeddings_map,
    '_reco_build_session_profile': _reco_build_session_profile,
    '_reco_fetch_candidates': _reco_fetch_candidates,
    '_reco_rank_candidates': _reco_rank_candidates,
    '_reco_record_event': _reco_record_event,
    '_files_backfill_trusted_match_flags': _files_backfill_trusted_match_flags,
    '_files_similar_artists_by_genre': _files_similar_artists_by_genre,
}

def _rebuild_files_reco_embeddings_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _rebuild_files_reco_embeddings(*args, **kwargs)

def _reco_event_weight_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _reco_event_weight(*args, **kwargs)

def _reco_build_track_embeddings_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _reco_build_track_embeddings(*args, **kwargs)

def _reco_build_track_embeddings_chunked_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _reco_build_track_embeddings_chunked(*args, **kwargs)

def _reco_upsert_track_embeddings_for_album_ids_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _reco_upsert_track_embeddings_for_album_ids(*args, **kwargs)

def _entity_discover_ai_summary_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _entity_discover_ai_summary(*args, **kwargs)

def _reco_genre_tokens_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _reco_genre_tokens(*args, **kwargs)

def _reco_fetch_embeddings_map_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _reco_fetch_embeddings_map(*args, **kwargs)

def _reco_build_session_profile_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _reco_build_session_profile(*args, **kwargs)

def _reco_fetch_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _reco_fetch_candidates(*args, **kwargs)

def _reco_rank_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _reco_rank_candidates(*args, **kwargs)

def _reco_record_event_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _reco_record_event(*args, **kwargs)

def _files_backfill_trusted_match_flags_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_backfill_trusted_match_flags(*args, **kwargs)

def _files_similar_artists_by_genre_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_similar_artists_by_genre(*args, **kwargs)
