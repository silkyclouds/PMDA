"""Runtime-backed assistant RAG ingestion.

Assistant RAG ingestion writes PMDA library and artist facts into assistant_docs
and assistant_doc_chunks. Keeping this outside the bootstrap isolates database
writes and embedding generation while preserving the existing assistant API.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from typing import Any

_RUNTIME: Any | None = None
_ASSISTANT_LIBRARY_SNAPSHOT_REFRESH_SEC = 15 * 60


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one assistant RAG ingestion call."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "_assistant_text_hash",
        "_assistant_chunk_text",
        "_assistant_upsert_doc",
        "_assistant_ingest_library_rag",
        "_assistant_ingest_artist_rag",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def assistant_upsert_doc_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> int | None:
    _bind_runtime(runtime)
    return _assistant_upsert_doc(*args, **kwargs)


def assistant_ingest_library_rag_for_runtime(runtime: Any, conn: Any) -> dict:
    _bind_runtime(runtime)
    return _assistant_ingest_library_rag(conn)


def assistant_ingest_artist_rag_for_runtime(runtime: Any, conn: Any, artist_id: int) -> dict:
    _bind_runtime(runtime)
    return _assistant_ingest_artist_rag(conn, artist_id)


def _assistant_text_hash(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8", errors="ignore")).hexdigest()


def _assistant_chunk_text(text: str, max_chars: int = 900) -> list[str]:
    """Chunk text roughly by paragraphs, capped by max_chars (ASCII-friendly)."""
    raw = (text or "").strip()
    if not raw:
        return []
    paras = [p.strip() for p in re.split(r"\n\s*\n+", raw) if p and p.strip()]
    chunks: list[str] = []
    buf = ""
    for p in paras:
        if not buf:
            buf = p
            continue
        if len(buf) + 2 + len(p) <= max_chars:
            buf = f"{buf}\n\n{p}"
            continue
        chunks.append(buf)
        buf = p
    if buf:
        chunks.append(buf)

    # Hard split any oversize chunk.
    out: list[str] = []
    for c in chunks:
        if len(c) <= max_chars:
            out.append(c)
            continue
        start = 0
        while start < len(c):
            out.append(c[start : start + max_chars].strip())
            start += max_chars
    return [c for c in out if c]


def _assistant_upsert_doc(
    conn,
    *,
    entity_type: str,
    entity_id: int,
    doc_type: str,
    source: str,
    provider: str = "",
    model: str = "",
    title: str = "",
    url: str = "",
    lang: str = "",
    content: str,
) -> int | None:
    """Upsert a document and (re)build its chunk embeddings when content changes."""
    et = (entity_type or "").strip().lower()
    dt = (doc_type or "").strip().lower()
    src = (source or "").strip().lower() or "unknown"
    if not et or int(entity_id or 0) <= 0 or not dt:
        return None
    body = (content or "").strip()
    if not body:
        return None
    h = _assistant_text_hash(body)
    title = (title or "").strip()
    url = (url or "").strip()
    lang = (lang or "").strip()
    provider = (provider or "").strip()
    model = (model or "").strip()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, content_hash
            FROM assistant_docs
            WHERE entity_type = %s AND entity_id = %s AND doc_type = %s AND source = %s
            """,
            (et, int(entity_id), dt, src),
        )
        row = cur.fetchone()
        if row:
            doc_id = int(row[0])
            prev_hash = str(row[1] or "")
            if prev_hash == h:
                return doc_id
        else:
            doc_id = 0
            prev_hash = ""

    # Upsert document.
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO assistant_docs(
                    entity_type, entity_id, doc_type, source, provider, model, title, url, lang, content, content_hash, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (entity_type, entity_id, doc_type, source) DO UPDATE
                SET provider = EXCLUDED.provider,
                    model = EXCLUDED.model,
                    title = EXCLUDED.title,
                    url = EXCLUDED.url,
                    lang = EXCLUDED.lang,
                    content = EXCLUDED.content,
                    content_hash = EXCLUDED.content_hash,
                    updated_at = NOW()
                RETURNING id
                """,
                (et, int(entity_id), dt, src, provider, model, title, url, lang, body, h),
            )
            doc_id = int((cur.fetchone() or [0])[0] or 0)
            if doc_id <= 0:
                return None
            if prev_hash and prev_hash == h:
                return doc_id

            # Rebuild chunks only when content changed.
            cur.execute("DELETE FROM assistant_doc_chunks WHERE doc_id = %s", (doc_id,))
            chunks = _assistant_chunk_text(body, max_chars=900)
            if not chunks:
                return doc_id
            rows = []
            for idx, chunk in enumerate(chunks):
                vec, norm = _build_hashed_embedding(chunk, RECO_EMBED_DIM)
                rows.append((doc_id, idx, chunk, json.dumps(vec), float(norm or 1.0)))
            cur.executemany(
                """
                INSERT INTO assistant_doc_chunks(doc_id, chunk_index, content, embed_json, norm, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                """,
                rows,
            )
    return doc_id




def _assistant_ingest_library_rag(conn) -> dict:
    """
    Ensure RAG docs exist for library-level questions (counts/facets).

    Keeps a lightweight "library snapshot" document in Postgres so the assistant can answer
    collection-wide questions without guessing.
    """
    # Throttle rebuilds; this runs in the request path.
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM files_index_meta WHERE key = %s", ("assistant_library_snapshot_ts",))
            row = cur.fetchone()
        last_ts = float(row[0] or 0) if row and row[0] else 0.0
    except Exception:
        last_ts = 0.0
    if last_ts > 0 and (time.time() - last_ts) < _ASSISTANT_LIBRARY_SNAPSHOT_REFRESH_SEC:
        return {"skipped": True}

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM files_artists")
            artists = int((cur.fetchone() or [0])[0] or 0)
            cur.execute("SELECT COUNT(*) FROM files_albums")
            albums = int((cur.fetchone() or [0])[0] or 0)
            cur.execute("SELECT COUNT(*) FROM files_tracks")
            tracks = int((cur.fetchone() or [0])[0] or 0)

            # Top genres (multi-genre tags_json first, then legacy alb.genre).
            cur.execute(
                """
                WITH genre_tokens AS (
                    SELECT
                        alb.id AS album_id,
                        LOWER(TRIM(g.value)) AS genre
                    FROM files_albums alb
                    CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                    WHERE COALESCE(TRIM(g.value), '') <> ''
                    UNION ALL
                    SELECT
                        alb.id AS album_id,
                        LOWER(TRIM(alb.genre)) AS genre
                    FROM files_albums alb
                    WHERE COALESCE(TRIM(alb.genre), '') <> ''
                      AND COALESCE(alb.tags_json, '[]') = '[]'
                )
                SELECT genre, COUNT(DISTINCT album_id) AS c
                FROM genre_tokens
                WHERE COALESCE(genre, '') <> ''
                GROUP BY genre
                ORDER BY c DESC, genre ASC
                LIMIT 20
                """
            )
            genre_rows = cur.fetchall()

            # Top labels.
            cur.execute(
                """
                SELECT TRIM(label) AS label, COUNT(*) AS c
                FROM files_albums
                WHERE COALESCE(TRIM(label), '') <> ''
                GROUP BY TRIM(label)
                ORDER BY c DESC, label ASC
                LIMIT 20
                """
            )
            label_rows = cur.fetchall()

            # Recently indexed albums (acts like "recently added").
            cur.execute(
                """
                SELECT
                    alb.id,
                    alb.title,
                    COALESCE(alb.year, 0) AS year,
                    ar.id AS artist_id,
                    ar.name AS artist_name
                FROM files_albums alb
                JOIN files_artists ar ON ar.id = alb.artist_id
                ORDER BY alb.created_at DESC, alb.id DESC
                LIMIT 20
                """
            )
            recent_rows = cur.fetchall()
    except Exception:
        logging.debug("Assistant library snapshot build failed", exc_info=True)
        return {}

    lines: list[str] = []
    lines.append("PMDA Library snapshot (local files)")
    lines.append(f"Artists: {artists}")
    lines.append(f"Albums: {albums}")
    lines.append(f"Tracks: {tracks}")

    if genre_rows:
        lines.append("")
        lines.append("Top genres (by #albums):")
        for g, c in genre_rows[:15]:
            gg = str(g or "").strip()
            if not gg:
                continue
            lines.append(f"- {gg}: {int(c or 0)}")

    if label_rows:
        lines.append("")
        lines.append("Top labels (by #albums):")
        for lab, c in label_rows[:12]:
            ll = str(lab or "").strip()
            if not ll:
                continue
            lines.append(f"- {ll}: {int(c or 0)}")

    if recent_rows:
        lines.append("")
        lines.append("Recently added albums (most recent first):")
        for aid, title, year, artist_id, artist_name in recent_rows[:15]:
            y = int(year or 0)
            year_txt = str(y) if y > 0 else "—"
            t = str(title or "").strip()
            a = str(artist_name or "").strip()
            if not (t and a):
                continue
            lines.append(f"- {a} · {year_txt} · {t} · album_id={int(aid or 0)} · artist_id={int(artist_id or 0)}")

    # Upsert the snapshot doc for RAG.
    try:
        _assistant_upsert_doc(
            conn,
            entity_type="library",
            entity_id=1,
            doc_type="library_snapshot",
            source="pmda_db",
            title="PMDA Library",
            content="\n".join(lines),
        )
        with conn.transaction():
            with conn.cursor() as cur:
                _files_index_write_meta(cur, "assistant_library_snapshot_ts", str(int(time.time())))
    except Exception:
        logging.debug("Assistant library snapshot upsert failed", exc_info=True)

    return {"artists": artists, "albums": albums, "tracks": tracks}


def _assistant_ingest_artist_rag(conn, artist_id: int) -> dict:
    """Ensure RAG docs exist for an artist. Returns minimal context info for citations."""
    artist_id = int(artist_id or 0)
    if artist_id <= 0:
        return {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, name_norm FROM files_artists WHERE id = %s", (artist_id,))
        row = cur.fetchone()
        if not row:
            return {}
        artist_name = (row[1] or "").strip()
        artist_norm = (row[2] or "").strip() or _norm_artist_key(artist_name)

        cur.execute(
            """
            SELECT bio, short_bio, tags_json, similar_json, source, updated_at
            FROM files_artist_profiles
            WHERE name_norm = %s
            """,
            (artist_norm,),
        )
        prof_row = cur.fetchone()
        bio = (prof_row[0] or "").strip() if prof_row else ""
        short_bio = (prof_row[1] or "").strip() if prof_row else ""
        tags_json = (prof_row[2] or "").strip() if prof_row else ""
        similar_json = (prof_row[3] or "").strip() if prof_row else ""
        prof_source = (prof_row[4] or "").strip() if prof_row else ""
        prof_updated_at = prof_row[5] if prof_row else None

        try:
            tags = json.loads(tags_json) if tags_json else []
        except Exception:
            tags = []
        if not isinstance(tags, list):
            tags = []
        try:
            similar = json.loads(similar_json) if similar_json else []
        except Exception:
            similar = []
        if not isinstance(similar, list):
            similar = []

        cur.execute(
            """
            WITH artist_albums AS (
                SELECT DISTINCT album_id
                FROM files_artist_album_links
                WHERE artist_id = %s
            )
            SELECT alb.id, alb.title, alb.title_norm, COALESCE(alb.year, 0) AS year, alb.track_count, COALESCE(alb.format, ''), alb.is_lossless, alb.has_cover
            FROM artist_albums aa
            JOIN files_albums alb ON alb.id = aa.album_id
            ORDER BY COALESCE(year, 0) DESC, title ASC
            LIMIT 160
            """,
            (artist_id,),
        )
        album_rows = cur.fetchall()

        # Local genre cues to disambiguate common-name artists (e.g. Last.fm "multiple artists using this name").
        try:
            cur.execute(
                """
                WITH genre_tokens AS (
                    SELECT
                        LOWER(TRIM(g.value)) AS genre
                    FROM files_artist_album_links link
                    JOIN files_albums alb ON alb.id = link.album_id
                    CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                    WHERE link.artist_id = %s
                      AND COALESCE(TRIM(g.value), '') <> ''
                    UNION ALL
                    SELECT
                        LOWER(TRIM(alb.genre)) AS genre
                    FROM files_artist_album_links link
                    JOIN files_albums alb ON alb.id = link.album_id
                    WHERE link.artist_id = %s
                      AND COALESCE(TRIM(alb.genre), '') <> ''
                      AND COALESCE(alb.tags_json, '[]') = '[]'
                )
                SELECT genre, COUNT(*) AS c
                FROM genre_tokens
                WHERE COALESCE(genre, '') <> ''
                GROUP BY genre
                ORDER BY c DESC, genre ASC
                LIMIT 12
                """,
                (artist_id, artist_id),
            )
            genre_rows = cur.fetchall()
        except Exception:
            genre_rows = []

    if bio:
        _assistant_upsert_doc(
            conn,
            entity_type="artist",
            entity_id=artist_id,
            doc_type="artist_profile_bio",
            source=prof_source or "unknown",
            title=artist_name,
            content=bio,
        )
    if short_bio and (not bio or len(bio) < 140):
        _assistant_upsert_doc(
            conn,
            entity_type="artist",
            entity_id=artist_id,
            doc_type="artist_profile_short",
            source=prof_source or "unknown",
            title=artist_name,
            content=short_bio,
        )

    # Always ingest a local library snapshot for factual "what do I own" questions.
    lines = []
    lines.append(f"Artist: {artist_name}")
    lines.append(f"Local albums: {len(album_rows)}")
    try:
        if genre_rows:
            parts = []
            for g, c in genre_rows[:12]:
                gg = str(g or "").strip()
                if not gg:
                    continue
                try:
                    cc = int(c or 0)
                except Exception:
                    cc = 0
                parts.append(f"{gg} ({cc})" if cc > 1 else gg)
            if parts:
                lines.append("Local genres: " + ", ".join(parts[:12]))
    except Exception:
        pass
    for aid, title, title_norm, year, track_count, fmt, is_lossless, has_cover in album_rows[:160]:
        yr = int(year or 0)
        year_txt = str(yr) if yr > 0 else "—"
        fmt_txt = (fmt or "").strip().upper() or "—"
        loss = "lossless" if bool(is_lossless) else "lossy"
        cover = "cover" if bool(has_cover) else "no_cover"
        lines.append(f"- {year_txt} · {title} ({int(track_count or 0)} tracks) · {fmt_txt} · {loss} · {cover} · album_id={int(aid)}")
    _assistant_upsert_doc(
        conn,
        entity_type="artist",
        entity_id=artist_id,
        doc_type="artist_library_snapshot",
        source="pmda_db",
        title=artist_name,
        content="\n".join(lines),
    )

    # Ingest tags + similar artists for conversational recommendations.
    try:
        if tags:
            tag_txt = ", ".join([str(t or "").strip() for t in tags if str(t or "").strip()][:30])
            if tag_txt:
                _assistant_upsert_doc(
                    conn,
                    entity_type="artist",
                    entity_id=artist_id,
                    doc_type="artist_tags",
                    source=prof_source or "unknown",
                    title=artist_name,
                    content=f"Tags: {tag_txt}",
                )
    except Exception:
        pass

    try:
        if similar:
            s_lines = ["Similar artists (from metadata providers):"]
            for s in similar[:40]:
                if not isinstance(s, dict):
                    continue
                nm = (s.get("name") or "").strip()
                if not nm:
                    continue
                typ = (s.get("type") or "").strip()
                if typ:
                    s_lines.append(f"- {nm} ({typ})")
                else:
                    s_lines.append(f"- {nm}")
            if len(s_lines) > 1:
                _assistant_upsert_doc(
                    conn,
                    entity_type="artist",
                    entity_id=artist_id,
                    doc_type="artist_similar_artists",
                    source=prof_source or "unknown",
                    title=artist_name,
                    content="\n".join(s_lines),
                )
    except Exception:
        pass

    # Ingest cached album snippets/reviews (local-only "journal" use-case).
    try:
        title_norms = [str(r[2] or "").strip() for r in album_rows if str(r[2] or "").strip()]
        title_norms = list(dict.fromkeys(title_norms))[:160]
        prof_map: dict[str, dict] = {}
        if title_norms:
            placeholders = ",".join(["%s"] * len(title_norms))
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT title_norm, short_description, source
                    FROM files_album_profiles
                    WHERE artist_norm = %s AND title_norm IN ({placeholders})
                    """,
                    [artist_norm, *title_norms],
                )
                for tn, sd, src in cur.fetchall():
                    key = str(tn or "").strip()
                    if not key:
                        continue
                    prof_map[key] = {"short_description": (sd or "").strip(), "source": (src or "").strip()}

        review_lines: list[str] = []
        for aid, title, title_norm, year, track_count, fmt, is_lossless, has_cover in album_rows[:120]:
            tn = str(title_norm or "").strip()
            if not tn:
                continue
            p = prof_map.get(tn) or {}
            sd = (p.get("short_description") or "").strip()
            if not sd:
                continue
            src = (p.get("source") or "").strip() or "unknown"
            yr = int(year or 0)
            year_txt = str(yr) if yr > 0 else "—"
            review_lines.append(f"- {year_txt} · {title}: {sd} (source={src})")
            if len(review_lines) >= 80:
                break
        if review_lines:
            _assistant_upsert_doc(
                conn,
                entity_type="artist",
                entity_id=artist_id,
                doc_type="artist_album_snippets",
                source="pmda_db",
                title=artist_name,
                content="Album snippets:\n" + "\n".join(review_lines),
            )
    except Exception:
        pass

    # Ingest upcoming concerts if cached (or recently refreshed by the artist page).
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT provider, events_json, source_url, updated_at
                FROM files_artist_concerts
                WHERE artist_id = %s
                """,
                (artist_id,),
            )
            crow = cur.fetchone()
        if crow:
            provider = (crow[0] or "").strip().lower() or "bandsintown"
            source_url = (crow[2] or "").strip()
            try:
                events = json.loads(crow[1] or "[]") if crow[1] else []
            except Exception:
                events = []
            if not isinstance(events, list):
                events = []
            if events:
                c_lines = []
                for ev in events[:30]:
                    if not isinstance(ev, dict):
                        continue
                    dt = (ev.get("datetime") or ev.get("date") or "").strip()
                    venue = ev.get("venue") if isinstance(ev.get("venue"), dict) else {}
                    vname = (venue.get("name") or "").strip() if isinstance(venue, dict) else ""
                    city = (venue.get("city") or "").strip() if isinstance(venue, dict) else ""
                    country = (venue.get("country") or "").strip() if isinstance(venue, dict) else ""
                    where = ", ".join([x for x in [city, country] if x])
                    url = (ev.get("url") or "").strip()
                    line = " · ".join([x for x in [dt, vname, where, url] if x])
                    if line:
                        c_lines.append(f"- {line}")
                if c_lines:
                    _assistant_upsert_doc(
                        conn,
                        entity_type="artist",
                        entity_id=artist_id,
                        doc_type="artist_concerts_upcoming",
                        source=provider,
                        title=artist_name,
                        url=source_url,
                        content="Upcoming concerts:\n" + "\n".join(c_lines),
                    )
    except Exception:
        pass

    # Ingest extracted facts (AKA/groups/labels/collabs) when available.
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT facts_json, source, provider, model, updated_at
                FROM assistant_entity_facts
                WHERE entity_type = 'artist' AND entity_id = %s
                """,
                (artist_id,),
            )
            frow = cur.fetchone()
        if frow:
            try:
                facts = json.loads(frow[0] or "{}") if frow[0] else {}
            except Exception:
                facts = {}
            if not isinstance(facts, dict):
                facts = {}
            src = (frow[1] or "").strip() or "facts"
            provider = (frow[2] or "").strip()
            model = (frow[3] or "").strip()
            lines = []
            for key, label in (
                ("aka", "AKA"),
                ("aliases", "Aliases"),
                ("member_of", "Groups"),
                ("collaborated_with", "Collaborations"),
                ("labels", "Labels"),
                ("notable_cities", "Cities"),
            ):
                val = facts.get(key)
                if not isinstance(val, list):
                    continue
                clean = [str(x or "").strip() for x in val if str(x or "").strip()]
                if not clean:
                    continue
                lines.append(f"{label}: {', '.join(clean[:30])}")
            if lines:
                _assistant_upsert_doc(
                    conn,
                    entity_type="artist",
                    entity_id=artist_id,
                    doc_type="artist_facts_extracted",
                    source=src,
                    provider=provider,
                    model=model,
                    title=artist_name,
                    content="\n".join(lines),
                )
    except Exception:
        pass

    return {
        "artist_id": artist_id,
        "artist_name": artist_name,
        "artist_norm": artist_norm,
        "profile_source": prof_source,
        "profile_updated_at": int(_dt_to_epoch(prof_updated_at)) if prof_updated_at else 0,
    }
