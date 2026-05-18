"""Runtime-owned assistant chat, DB tools, and SQL-agent handlers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_assistant_runtime_status',
    '_assistant_find_artist_ids_for_query',
    '_assistant_fetch_session_messages',
    '_assistant_retrieve_chunks',
    '_assistant_maybe_gc',
    '_assistant_ensure_session',
    '_assistant_insert_message',
    '_assistant_build_prompt',
    '_assistant_links_from_citations',
    '_assistant_links_from_web_results',
    '_assistant_should_include_web_discovery',
    '_assistant_simplify_for_intent',
    '_assistant_lang_for_message',
    '_assistant_detect_tool_intent',
    '_assistant_should_force_llm_rag',
    '_assistant_tool_library_counts',
    '_assistant_tool_library_top_genres',
    '_assistant_tool_library_top_labels',
    '_assistant_tool_library_top_artists',
    '_assistant_tool_artist_list_albums',
    '_assistant_extract_requested_count',
    '_assistant_find_genre_for_query',
    '_assistant_playlist_candidate_tracks',
    '_assistant_playlist_title',
    '_assistant_create_playlist_from_query',
    '_assistant_recommend_albums_from_query',
    '_assistant_tool_artist_concerts',
    '_assistant_tool_artist_similar',
    '_assistant_try_handle_tool_query',
    '_assistant_should_try_sql_agent',
    '_assistant_extract_json_obj',
    '_assistant_validate_readonly_sql',
    '_assistant_sql_agent_generate_query',
    '_assistant_sql_agent_execute',
    '_assistant_sql_agent_format_result',
    '_assistant_sql_agent_links_from_result',
    '_assistant_try_handle_sql_agent_query',
    'api_assistant_status',
    'api_assistant_get_session',
    'api_assistant_chat',
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
        own_wrapper = name.endswith("_for_runtime") and name[: -len("_for_runtime")] in _EXTRACTED_NAMES
        if name == "_bind_runtime" or own_wrapper:
            continue
        globals()[name] = value

def _assistant_runtime_status(
    *,
    user_id: int | None = None,
    system_msg: str = "",
    user_msg: str = "",
) -> dict[str, Any]:
    uid = _current_user_id_or_zero() if user_id is None else max(0, int(user_id or 0))
    requested_provider = str(getattr(sys.modules[__name__], "AI_PROVIDER", "ollama") or "ollama").strip() or "ollama"
    ai_ready, provider_effective, auth_mode, ai_error = _resolve_ai_runtime_availability(
        analysis_type="assistant_chat",
        requested_provider=requested_provider,
        user_id=uid,
    )
    provider_name = str(provider_effective or requested_provider or "ollama").strip() or "ollama"
    model_seed = _ai_model_display_name(provider_name)
    endpoint_kind = "longform" if (str(system_msg or "").strip() or str(user_msg or "").strip()) else "text"
    model_name = _resolve_model_for_runtime(
        provider_name,
        str(model_seed or "").strip(),
        endpoint_kind=endpoint_kind,
        analysis_type="assistant_chat",
        system_msg=system_msg,
        user_msg=user_msg,
    )
    provider_norm = provider_name.lower()
    if provider_norm == "openai-codex" and not model_name:
        model_name = "codex"
    available_models: list[str] = []
    if provider_norm == "ollama":
        url = str(getattr(sys.modules[__name__], "OLLAMA_URL", "") or "").strip()
        if not url:
            ai_ready = False
            ai_error = "Ollama URL is not configured"
        else:
            cached_models = {
                str(v or "").strip().lower()
                for v in _ollama_available_models_cached()
                if str(v or "").strip()
            }
            if cached_models:
                available_models = sorted(cached_models)
            else:
                health = _managed_runtime_health_check_ollama(url)
                available_models = [
                    str(v or "").strip()
                    for v in (health.get("models") or [])
                    if str(v or "").strip()
                ]
                if not bool(health.get("available")):
                    ai_ready = False
                    ai_error = str(health.get("message") or "").strip() or "Ollama runtime is unreachable"
                elif not available_models:
                    ai_ready = False
                    ai_error = "Ollama is reachable, but no models are installed yet"
                cached_models = {str(v or "").strip().lower() for v in available_models}
            if bool(ai_ready) and str(model_name or "").strip():
                target_model = str(model_name or "").strip().lower()
                if cached_models and target_model not in cached_models:
                    ai_ready = False
                    ai_error = f"Ollama is reachable, but required model {model_name} is not installed"
    return {
        "ai_ready": bool(ai_ready),
        "ai_provider": provider_name,
        "ai_auth_mode": str(auth_mode or ""),
        "ai_model": model_name,
        "ai_error": str(ai_error or "").strip() if not bool(ai_ready) else None,
        "available_models": available_models,
    }


def _assistant_find_artist_ids_for_query(conn, query: str, limit: int = 3) -> list[int]:
    q = (query or "").strip()
    if not q:
        return []
    q_norm = _assistant_simplify_for_intent(q)
    limit = max(1, min(10, int(limit or 3)))
    with conn.cursor() as cur:
        # 1) Strong heuristic: if the message contains an artist name verbatim, prefer that.
        # This is robust for natural language queries like "Quels albums de Rod Stewart...".
        cur.execute(
            """
            SELECT id
            FROM files_artists
            WHERE length(name) >= 3
              AND position(lower(name) in lower(%s)) > 0
            ORDER BY length(name) DESC, album_count DESC, name ASC
            LIMIT %s
            """,
            (q, limit),
        )
        rows = cur.fetchall()
        if rows:
            return [int(r[0] or 0) for r in rows if int(r[0] or 0) > 0]

        if q_norm:
            cur.execute(
                """
                SELECT id
                FROM files_artists
                WHERE length(name_norm) >= 3
                  AND position(name_norm in %s) > 0
                ORDER BY length(name_norm) DESC, album_count DESC, name ASC
                LIMIT %s
                """,
                (q_norm, limit),
            )
            rows = cur.fetchall()
            if rows:
                return [int(r[0] or 0) for r in rows if int(r[0] or 0) > 0]

        # 2) Fallback: similarity / ILIKE when pg_trgm is available.
        like = f"%{q}%"
        like_norm = f"%{q_norm}%" if q_norm else ""
        try:
            cur.execute(
                """
                SELECT id
                FROM files_artists
                WHERE name ILIKE %s
                ORDER BY similarity(name, %s) DESC, album_count DESC, name ASC
                LIMIT %s
                """,
                (like, q, limit),
            )
        except Exception:
            cur.execute(
                """
                SELECT id
                FROM files_artists
                WHERE name ILIKE %s
                ORDER BY album_count DESC, name ASC
                LIMIT %s
                """,
                (like, limit),
            )
        rows = cur.fetchall()
        if (not rows) and like_norm:
            try:
                cur.execute(
                    """
                    SELECT id
                    FROM files_artists
                    WHERE name_norm ILIKE %s
                    ORDER BY similarity(name_norm, %s) DESC, album_count DESC, name ASC
                    LIMIT %s
                    """,
                    (like_norm, q_norm, limit),
                )
            except Exception:
                cur.execute(
                    """
                    SELECT id
                    FROM files_artists
                    WHERE name_norm ILIKE %s
                    ORDER BY album_count DESC, name ASC
                    LIMIT %s
                    """,
                    (like_norm, limit),
                )
            rows = cur.fetchall()
    return [int(r[0] or 0) for r in rows if int(r[0] or 0) > 0]


def _assistant_fetch_session_messages(conn, session_id: str, limit: int = 12) -> list[dict]:
    session_id = (session_id or "").strip()
    if not session_id:
        return []
    limit = max(1, min(50, int(limit or 12)))
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT role, content
            FROM assistant_messages
            WHERE session_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (session_id, limit),
        )
        rows = cur.fetchall()
    out = []
    for role, content in reversed(rows):
        r = (role or "").strip().lower()
        if r not in {"user", "assistant", "system"}:
            continue
        out.append({"role": r, "content": (content or "").strip()})
    return out


def _assistant_retrieve_chunks(conn, query: str, *, artist_id: int | None = None, artist_ids: list[int] | None = None, k: int = 8) -> dict:
    """Return top-k chunks with citation metadata (artist + library snapshot)."""
    query = (query or "").strip()
    k = max(1, min(16, int(k or 8)))
    artist_id = int(artist_id or 0) if artist_id else 0
    artist_ids_list = [int(x or 0) for x in (artist_ids or []) if int(x or 0) > 0]
    if artist_id > 0 and artist_id not in artist_ids_list:
        artist_ids_list.insert(0, artist_id)
    if not query:
        return {"chunks": [], "citations": []}

    doc_ids: list[int] = []
    # Always include library snapshot docs so collection-wide questions have grounding.
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM assistant_docs
                WHERE entity_type = 'library' AND entity_id = 1
                ORDER BY updated_at DESC
                """,
            )
            doc_ids.extend([int(r[0] or 0) for r in cur.fetchall() if int(r[0] or 0) > 0])
    except Exception:
        pass

    if artist_ids_list:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM assistant_docs
                    WHERE entity_type = 'artist' AND entity_id = ANY(%s)
                    ORDER BY updated_at DESC
                    """,
                    (artist_ids_list,),
                )
                doc_ids.extend([int(r[0] or 0) for r in cur.fetchall() if int(r[0] or 0) > 0])
        except Exception:
            pass
    # Deduplicate while preserving order.
    doc_ids = list(dict.fromkeys([int(x) for x in doc_ids if int(x) > 0]))
    if not doc_ids:
        return {"chunks": [], "citations": []}

    q_vec, _q_norm = _build_hashed_embedding(query, RECO_EMBED_DIM)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                c.id,
                c.content,
                c.embed_json,
                d.entity_type,
                d.entity_id,
                d.doc_type,
                d.source,
                COALESCE(d.title, '')
            FROM assistant_doc_chunks c
            JOIN assistant_docs d ON d.id = c.doc_id
            WHERE c.doc_id = ANY(%s)
            """,
            (doc_ids,),
        )
        rows = cur.fetchall()

    scored = []
    artist_snapshot_item: dict | None = None
    library_snapshot_item: dict | None = None
    for chunk_id, content, embed_json, et, eid, doc_type, source, title in rows:
        emb = _load_embedding_json(embed_json or "[]")
        sim = _vec_cosine(q_vec, emb) if emb else 0.0
        txt = (content or "").strip()
        # Small lexical boost for exact substring match.
        if query.lower() in txt.lower():
            sim += 0.08
        item = (
            {
                "chunk_id": int(chunk_id or 0),
                "score": float(sim),
                "text": txt,
                "citation": {
                    "entity_type": str(et or ""),
                    "entity_id": int(eid or 0),
                    "doc_type": str(doc_type or ""),
                    "source": str(source or ""),
                    "title": str(title or ""),
                },
            }
        )
        scored.append(item)
        # Always keep snapshot chunks so "what do I own" questions are answerable without relying on
        # embedding similarity (artist inventory + collection snapshot).
        dt = str(doc_type or "").strip().lower()
        if dt == "artist_library_snapshot" and artist_snapshot_item is None:
            artist_snapshot_item = item
        if dt == "library_snapshot" and library_snapshot_item is None:
            library_snapshot_item = item
    scored.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    top = scored[: max(k, 1)]

    # Ensure snapshots are present (artist snapshot first, then library snapshot).
    merged = list(top)
    for snap in [artist_snapshot_item, library_snapshot_item]:
        if snap is None:
            continue
        snap_dt = (snap.get("citation") or {}).get("doc_type")
        if snap_dt and any((it.get("citation") or {}).get("doc_type") == snap_dt for it in merged):
            continue
        merged = [snap, *merged]
    if merged != top:
        # Put snapshots first so the model sees local inventory before external bios.
        seen = set()
        deduped = []
        for it in merged:
            cid = int(it.get("chunk_id") or 0)
            if cid and cid in seen:
                continue
            if cid:
                seen.add(cid)
            deduped.append(it)
        top = deduped[: min(14, max(k, 1) + 4)]
    citations = []
    for item in top:
        c = dict(item.get("citation") or {})
        c["chunk_id"] = int(item.get("chunk_id") or 0)
        c["score"] = round(float(item.get("score") or 0.0), 4)
        c["snippet"] = _truncate_text(item.get("text") or "", max_chars=240)
        citations.append(c)
    return {"chunks": top, "citations": citations}


def _assistant_maybe_gc(conn) -> None:
    """Daily GC for assistant sessions/messages to prevent unbounded growth."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM files_index_meta WHERE key = %s", ("assistant_gc_ts",))
            row = cur.fetchone()
        last_ts = float(row[0] or 0) if row and row[0] else 0.0
    except Exception:
        last_ts = 0.0
    if last_ts > 0 and (time.time() - last_ts) < _ASSISTANT_GC_INTERVAL_SEC:
        return
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM assistant_sessions WHERE updated_at < NOW() - INTERVAL '{int(_ASSISTANT_SESSION_MAX_AGE_DAYS)} days'"
                )
                # Hard cap: keep most recent sessions.
                cur.execute(
                    """
                    SELECT session_id
                    FROM assistant_sessions
                    ORDER BY updated_at DESC
                    OFFSET %s
                    """,
                    (int(_ASSISTANT_SESSION_HARD_CAP),),
                )
                extra = [str(r[0] or "").strip() for r in cur.fetchall() if str(r[0] or "").strip()]
                if extra:
                    cur.execute("DELETE FROM assistant_sessions WHERE session_id = ANY(%s)", (extra,))
                _files_index_write_meta(cur, "assistant_gc_ts", str(int(time.time())))
    except Exception:
        # Never block UI/API on GC.
        logging.debug("Assistant GC failed", exc_info=True)


def _assistant_ensure_session(conn, session_id: str | None) -> str:
    sid = (session_id or "").strip()
    if not sid:
        sid = str(uuid.uuid4())
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SELECT session_id FROM assistant_sessions WHERE session_id = %s", (sid,))
            if cur.fetchone():
                cur.execute("UPDATE assistant_sessions SET updated_at = NOW() WHERE session_id = %s", (sid,))
                return sid
            cur.execute(
                """
                INSERT INTO assistant_sessions(session_id, created_at, updated_at)
                VALUES (%s, NOW(), NOW())
                """,
                (sid,),
            )
    return sid


def _assistant_insert_message(conn, *, session_id: str, role: str, content: str, context: dict, metadata: dict) -> dict:
    sid = (session_id or "").strip()
    role_norm = (role or "").strip().lower()
    if role_norm not in {"user", "assistant", "system"}:
        role_norm = "user"
    ctx_json = json.dumps(context or {}, ensure_ascii=True)
    meta_json = json.dumps(metadata or {}, ensure_ascii=True)
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO assistant_messages(session_id, role, content, context_json, metadata_json, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                RETURNING id, EXTRACT(EPOCH FROM created_at)::BIGINT
                """,
                (sid, role_norm, (content or "").strip(), ctx_json, meta_json),
            )
            row = cur.fetchone() or [0, 0]
            msg_id = int(row[0] or 0)
            created_at = int(row[1] or 0)
            cur.execute("UPDATE assistant_sessions SET updated_at = NOW() WHERE session_id = %s", (sid,))
    # Include context/metadata in the returned payload so the UI can render citations/links immediately
    # without requiring a separate history fetch.
    return {
        "id": msg_id,
        "role": role_norm,
        "content": (content or "").strip(),
        "created_at": created_at,
        "context": context if isinstance(context, dict) else {},
        "metadata": metadata if isinstance(metadata, dict) else {},
    }


def _assistant_build_prompt(
    *,
    user_message: str,
    retrieved: dict,
    history: list[dict],
    context_info: dict,
    web_results: list[dict[str, Any]] | None = None,
) -> tuple[str, str]:
    system_msg = (
        "You are PMDA Intelligence, an audiophile-focused music librarian for a LOCAL library.\n"
        "Rules:\n"
        "- Use only the provided context excerpts. Do not invent facts.\n"
        "- If the answer is not in context, say so and suggest a concrete next step (refresh profile, rescan, etc.).\n"
        "- If a current artist context is provided, focus only on that artist (do not enumerate other artists with the same name unless the user explicitly asks).\n"
        "- If a provider bio mentions multiple artists with the same name, DO NOT repeat the disambiguation; pick the variant that matches the local PMDA cues (local genres, local releases, known collaborators) and ignore the others.\n"
        "- When sources conflict about identity, prefer pmda_db excerpts as the identity anchor.\n"
        "- Answer in the same language as the user.\n"
        "- Prefer structured output (short sections / bullets) when helpful.\n"
    )
    ctx_lines = []
    chunks = retrieved.get("chunks") or []
    for i, item in enumerate(chunks[:12], start=1):
        cit = (item.get("citation") or {})
        label = f"C{i}"
        src = f"{cit.get('source') or 'unknown'}"
        dtype = f"{cit.get('doc_type') or 'doc'}"
        title = f"{cit.get('title') or ''}".strip()
        header = f"[{label}] ({dtype}, source={src}{', title='+title if title else ''})"
        ctx_lines.append(header)
        ctx_lines.append(item.get("text") or "")
        ctx_lines.append("")
    ctx_block = "\n".join(ctx_lines).strip()

    hist_lines = []
    for m in history[-12:]:
        role = (m.get("role") or "").strip().lower()
        content = (m.get("content") or "").strip()
        if not role or not content:
            continue
        prefix = "User" if role == "user" else ("Assistant" if role == "assistant" else "System")
        hist_lines.append(f"{prefix}: {content}")
    hist_block = "\n".join(hist_lines).strip()

    focus = ""
    artist_names: list[str] = []
    if isinstance(context_info.get("artist_names"), list):
        artist_names = [str(x or "").strip() for x in context_info.get("artist_names") if str(x or "").strip()]
    elif context_info.get("artist_name"):
        artist_names = [str(context_info.get("artist_name") or "").strip()]
    if artist_names:
        if len(artist_names) == 1:
            focus = f"Context: Current artist = {artist_names[0]}\n"
        else:
            focus = f"Context: Focus artists = {', '.join(artist_names[:6])}\n"

    web_block = ""
    if isinstance(web_results, list) and web_results:
        web_lines = []
        for idx, row in enumerate(web_results[:6], start=1):
            if not isinstance(row, dict):
                continue
            title = str(row.get("title") or "").strip()
            link = str(row.get("link") or "").strip()
            snippet = str(row.get("snippet") or "").strip()
            source = str(row.get("source") or "").strip() or "web"
            if not title and not snippet:
                continue
            web_lines.append(f"[W{idx}] ({source}{', title=' + title if title else ''}{', link=' + link if link else ''})")
            if snippet:
                web_lines.append(snippet)
            web_lines.append("")
        web_block = "\n".join(web_lines).strip()

    user_prompt = (
        f"{focus}"
        f"User question:\n{(user_message or '').strip()}\n\n"
        f"Conversation (most recent last):\n{hist_block if hist_block else '(none)'}\n\n"
        f"Context excerpts:\n{ctx_block if ctx_block else '(none)'}\n\n"
        f"Optional web discovery snippets:\n{web_block if web_block else '(none)'}\n"
    )
    return system_msg, user_prompt


def _assistant_links_from_citations(*, citations: list[dict], base_url: str) -> list[dict]:
    base = (base_url or "").rstrip("/")
    out: list[dict] = []
    seen: set[str] = set()

    def _push(link: dict) -> None:
        href = str(link.get("href") or "").strip()
        label = str(link.get("label") or "").strip()
        if not href or not label:
            return
        key = f"{href}||{label}"
        if key in seen:
            return
        seen.add(key)
        out.append(link)

    for cit in citations or []:
        if not isinstance(cit, dict):
            continue
        entity_type = str(cit.get("entity_type") or "").strip().lower()
        entity_id = int(cit.get("entity_id") or 0)
        title = str(cit.get("title") or "").strip()
        if entity_type == "artist" and entity_id > 0:
            _push(
                {
                    "kind": "internal",
                    "label": title or f"Artist #{entity_id}",
                    "href": f"/library/artist/{entity_id}",
                    "entity_type": "artist",
                    "entity_id": entity_id,
                    "thumb": f"{base}/api/library/files/artist/{entity_id}/image?size=96",
                }
            )
        elif entity_type == "album" and entity_id > 0:
            _push(
                {
                    "kind": "internal",
                    "label": title or f"Album #{entity_id}",
                    "href": f"/library/album/{entity_id}",
                    "entity_type": "album",
                    "entity_id": entity_id,
                    "thumb": f"{base}/api/library/files/album/{entity_id}/cover?size=96",
                }
            )
        elif entity_type == "playlist" and entity_id > 0:
            _push(
                {
                    "kind": "internal",
                    "label": title or f"Playlist #{entity_id}",
                    "href": f"/library/playlists/{entity_id}",
                    "entity_type": "playlist",
                    "entity_id": entity_id,
                    "thumb": None,
                }
            )
        elif entity_type == "label" and title:
            _push(
                {
                    "kind": "internal",
                    "label": title,
                    "href": f"/library/label/{quote(title)}",
                    "entity_type": "label",
                    "entity_id": 0,
                    "thumb": None,
                }
            )
        elif entity_type == "genre" and title:
            _push(
                {
                    "kind": "internal",
                    "label": title,
                    "href": f"/library/genre/{quote(title)}",
                    "entity_type": "genre",
                    "entity_id": 0,
                    "thumb": None,
                }
            )
    return out[:18]


def _assistant_links_from_web_results(web_results: list[dict[str, Any]]) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for row in (web_results or [])[:8]:
        if not isinstance(row, dict):
            continue
        href = str(row.get("link") or "").strip()
        label = str(row.get("title") or "").strip()
        if not href or not label:
            continue
        key = f"{href}||{label}"
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "kind": "external",
                "label": label,
                "href": href,
                "entity_type": "external",
                "entity_id": 0,
                "thumb": None,
            }
        )
    return out[:8]


def _assistant_should_include_web_discovery(user_message: str, *, retrieved_chunks_count: int) -> bool:
    s = _assistant_simplify_for_intent(user_message)
    if not s:
        return False
    if retrieved_chunks_count <= 1:
        return True
    return any(
        tok in s
        for tok in [
            "qui est",
            "who is",
            "parle moi de",
            "parle de",
            "raconte",
            "biographie",
            "biography",
            "recommend",
            "recommande",
            "recommandation",
            "discover",
            "decouvrir",
            "decouverte",
            "similar",
            "similaire",
        ]
    )


def _assistant_simplify_for_intent(text: str) -> str:
    """Lowercase, strip accents, collapse whitespace (good enough for intent heuristics)."""
    t = (text or "").strip().lower()
    if not t:
        return ""
    try:
        t = unicodedata.normalize("NFKD", t)
        t = "".join(ch for ch in t if not unicodedata.combining(ch))
    except Exception:
        pass
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _assistant_lang_for_message(user_message: str) -> str:
    s = _assistant_simplify_for_intent(user_message)
    if any(tok in s for tok in ["combien", "quel", "quelle", "quels", "bibliotheque", "bibliotheque", "morceau", "artiste", "album", "collection"]):
        return "fr"
    if any(tok in s for tok in ["how many", "library", "collection", "artist", "album", "track", "genre"]):
        return "en"
    try:
        return _assistant_preferred_lang()
    except Exception:
        return "en"


def _assistant_detect_tool_intent(user_message: str, *, context_artist_id: int) -> str | None:
    s = _assistant_simplify_for_intent(user_message)
    if not s:
        return None
    complex_prompt = any(
        tok in s
        for tok in [
            "qui est",
            "who is",
            "tell me about",
            "parle moi de",
            "parle de",
            "raconte",
            "biographie",
            "biography",
            "compare",
            "comparaison",
            "versus",
            "vs",
            "analyse",
            "why",
            "pourquoi",
        ]
    )

    if any(
        tok in s
        for tok in [
            "recommend",
            "recommendation",
            "recommande",
            "recommandation",
            "devrais je ecouter",
            "que dois je ecouter",
            "what should i listen",
            "what should i hear",
            "suggest me",
            "suggestions",
        ]
    ):
        return "library_recommend_albums"

    if ("playlist" in s or "mix" in s or "mixtape" in s) and any(
        tok in s
        for tok in ["create", "build", "make", "generate", "fais", "cree", "creer", "genere", "fabrique", "compose"]
    ):
        return "library_create_playlist"

    wants_count = any(tok in s for tok in ["combien", "nombre", "how many", "count"])
    if wants_count:
        if "artiste" in s or "artist" in s:
            return "library_count_artists"
        if "album" in s or "albums" in s:
            return "library_count_albums"
        if any(tok in s for tok in ["morceau", "track", "tracks", "titre", "song", "songs"]):
            return "library_count_tracks"

    # "Top" / "most present" / "most represented"
    if ("genre" in s) and any(tok in s for tok in ["plus present", "plus frequent", "most common", "most frequent", "most present"]):
        return "library_top_genres"
    if ("label" in s or "labels" in s) and any(tok in s for tok in ["plus present", "plus frequent", "most common", "most frequent", "most present", "top", "principaux", "main labels"]):
        return "library_top_labels"
    if any(tok in s for tok in ["artistes les plus presents", "artistes les plus presents", "most represented artists", "top artists", "artistes les plus representes"]):
        return "library_top_artists"

    if context_artist_id > 0:
        if "concert" in s or "concerts" in s or "tour" in s or "shows" in s or "dates" in s:
            if complex_prompt:
                return None
            return "artist_concerts"
        if ("artiste similaire" in s) or ("artistes similaires" in s) or ("similar artists" in s) or ("similaires" in s and "artiste" in s):
            if complex_prompt:
                return None
            return "artist_similar_artists"
        if ("album" in s or "albums" in s) and any(tok in s for tok in ["liste", "list", "quels", "which", "dispose", "dans ma collection", "local"]):
            if complex_prompt:
                return None
            return "artist_list_albums"

    return None


def _assistant_should_force_llm_rag(user_message: str) -> bool:
    s = _assistant_simplify_for_intent(user_message)
    if not s:
        return False
    rich_tokens = [
        "qui est",
        "who is",
        "tell me about",
        "parle moi de",
        "parle de",
        "raconte",
        "biographie",
        "biography",
        "compare",
        "comparaison",
        "versus",
        "vs",
        "analyse",
        "why",
        "pourquoi",
        "background",
        "story",
        "influence",
        "scene",
    ]
    if any(tok in s for tok in rich_tokens):
        return True
    # Rich hybrid prompts like "recommend X and explain why" should not be reduced to DB-only tools.
    if any(tok in s for tok in ["recommend", "recommande", "recommandation", "suggest", "devrais je ecouter"]):
        if any(tok in s for tok in ["because", "pourquoi", "why", "explique", "explain", "compare", "parce que"]):
            return True
    return False


def _assistant_tool_library_counts(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM files_artists")
        artists = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("SELECT COUNT(*) FROM files_albums")
        albums = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("SELECT COUNT(*) FROM files_tracks")
        tracks = int((cur.fetchone() or [0])[0] or 0)
    return {"artists": artists, "albums": albums, "tracks": tracks}


def _assistant_tool_library_top_genres(conn, *, limit: int = 10) -> list[tuple[str, int]]:
    limit = max(1, min(50, int(limit or 10)))
    with conn.cursor() as cur:
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
            LIMIT %s
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
    out: list[tuple[str, int]] = []
    for g, c in rows:
        gg = str(g or "").strip()
        if not gg:
            continue
        out.append((gg, int(c or 0)))
    return out


def _assistant_tool_library_top_labels(conn, *, limit: int = 10) -> list[tuple[str, int]]:
    limit = max(1, min(50, int(limit or 10)))
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH label_tokens AS (
                SELECT TRIM(COALESCE(alb.label, '')) AS label
                FROM files_albums alb
                WHERE COALESCE(TRIM(alb.label), '') <> ''
            )
            SELECT label, COUNT(*) AS c
            FROM label_tokens
            GROUP BY label
            ORDER BY c DESC, label ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
    out: list[tuple[str, int]] = []
    for label, c in rows:
        lab = str(label or "").strip()
        if not lab:
            continue
        out.append((lab, int(c or 0)))
    return out


def _assistant_tool_library_top_artists(conn, *, base_url: str, limit: int = 12) -> list[dict]:
    limit = max(1, min(40, int(limit or 12)))
    base = (base_url or "").rstrip("/")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                a.id,
                a.name,
                a.name_norm,
                a.album_count,
                a.track_count,
                (""" + _artist_has_true_image_sql("a", "ext") + """) AS has_image
            FROM files_artists a
            LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
            ORDER BY a.album_count DESC, a.track_count DESC, a.name ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
    out: list[dict] = []
    for aid, name, name_norm, album_count, track_count, has_image in rows:
        artist_id = int(aid or 0)
        nm = str(name or "").strip()
        if artist_id <= 0 or not nm:
            continue
        thumb = None
        if bool(has_image):
            thumb = f"{base}/api/library/files/artist/{artist_id}/image?size=192"
        out.append(
            {
                "artist_id": artist_id,
                "artist_name": nm,
                "name_norm": str(name_norm or "").strip(),
                "album_count": int(album_count or 0),
                "track_count": int(track_count or 0),
                "thumb": thumb,
            }
        )
    return out


def _assistant_tool_artist_list_albums(conn, *, artist_id: int, limit: int = 80) -> tuple[str, list[dict]]:
    artist_id = int(artist_id or 0)
    limit = max(1, min(200, int(limit or 80)))
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM files_artists WHERE id = %s", (artist_id,))
        row = cur.fetchone()
        artist_name = str((row[0] if row else "") or "").strip()
        cur.execute(
            """
            WITH artist_albums AS (
                SELECT DISTINCT album_id
                FROM files_artist_album_links
                WHERE artist_id = %s
            )
            SELECT alb.id, alb.title, COALESCE(alb.year, 0) AS year, alb.track_count, COALESCE(alb.format, '') AS fmt, alb.is_lossless, alb.has_cover
            FROM artist_albums aa
            JOIN files_albums alb ON alb.id = aa.album_id
            ORDER BY COALESCE(year, 0) DESC, title ASC
            LIMIT %s
            """,
            (artist_id, int(limit)),
        )
        rows = cur.fetchall()
    albums: list[dict] = []
    for aid, title, year, track_count, fmt, is_lossless, has_cover in rows:
        albums.append(
            {
                "album_id": int(aid or 0),
                "title": str(title or "").strip(),
                "year": int(year or 0),
                "track_count": int(track_count or 0),
                "format": str(fmt or "").strip(),
                "is_lossless": bool(is_lossless),
                "has_cover": bool(has_cover),
            }
        )
    return artist_name, albums


def _assistant_extract_requested_count(user_message: str, *, default: int = 10, minimum: int = 1, maximum: int = 30) -> int:
    text = str(user_message or "").strip()
    if not text:
        return max(minimum, min(maximum, int(default or minimum)))
    m = re.search(r"\b([1-9][0-9]?)\b", text)
    if not m:
        return max(minimum, min(maximum, int(default or minimum)))
    try:
        value = int(m.group(1))
    except Exception:
        value = int(default or minimum)
    return max(minimum, min(maximum, value))


def _assistant_find_genre_for_query(conn, query: str) -> str:
    s = _assistant_simplify_for_intent(query)
    if not s:
        return ""
    candidates = _assistant_tool_library_top_genres(conn, limit=120)
    best = ""
    best_len = 0
    for genre, _count in candidates:
        g = _assistant_simplify_for_intent(genre)
        if not g:
            continue
        if re.search(rf"(^| )({re.escape(g)})( |$)", s) and len(g) > best_len:
            best = str(genre or "").strip()
            best_len = len(g)
    return best


def _assistant_playlist_candidate_tracks(
    conn,
    *,
    user_id: int,
    count: int,
    artist_id: int = 0,
    genre: str = "",
) -> list[dict]:
    count = max(1, min(30, int(count or 10)))
    uid = max(0, int(user_id or 0))
    artist_id = max(0, int(artist_id or 0))
    genre_norm = _assistant_simplify_for_intent(genre)
    with conn.cursor() as cur:
        play_sql = """
            LEFT JOIN (
                SELECT track_id, COUNT(*)::BIGINT AS play_count
                FROM files_playback_events
                WHERE user_id = %s
                GROUP BY track_id
            ) pev ON pev.track_id = tr.id
        """
        profile_sql = """
            LEFT JOIN files_album_profiles pr
              ON pr.artist_norm = art.name_norm
             AND pr.title_norm = alb.title_norm
        """
        select_sql = """
            SELECT
                tr.id,
                COALESCE(NULLIF(TRIM(tr.title), ''), CONCAT('Track ', tr.id::TEXT)) AS track_title,
                COALESCE(tr.duration_sec, 0) AS duration_sec,
                COALESCE(tr.disc_num, 0) AS disc_num,
                COALESCE(tr.track_num, 0) AS track_num,
                alb.id AS album_id,
                alb.title AS album_title,
                COALESCE(alb.year, 0) AS album_year,
                alb.has_cover,
                art.id AS artist_id,
                art.name AS artist_name,
                COALESCE(pev.play_count, 0) AS play_count,
                COALESCE(pr.public_rating, 0) AS public_rating,
                COALESCE(pr.heat_score, 0) AS heat_score
            FROM files_tracks tr
            JOIN files_albums alb ON alb.id = tr.album_id
            JOIN files_artists art ON art.id = alb.artist_id
        """
        order_sql = """
            ORDER BY
                COALESCE(pev.play_count, 0) DESC,
                COALESCE(pr.public_rating, 0) DESC,
                COALESCE(pr.heat_score, 0) DESC,
                COALESCE(alb.year, 0) DESC,
                art.name ASC,
                alb.title ASC,
                COALESCE(tr.disc_num, 0) ASC,
                COALESCE(tr.track_num, 0) ASC,
                tr.id ASC
            LIMIT %s
        """
        params: list[Any] = [uid]
        extra_join_sql = ""
        sql = select_sql
        if artist_id > 0:
            extra_join_sql = """
                JOIN (SELECT DISTINCT artist_id, album_id FROM files_artist_album_links) link_filter
                  ON link_filter.album_id = alb.id
            """
        sql += extra_join_sql + play_sql + profile_sql
        if artist_id > 0:
            sql += " WHERE link_filter.artist_id = %s "
            params.append(artist_id)
        elif genre_norm:
            sql += """
                WHERE (
                    LOWER(COALESCE(alb.genre, '')) = %s
                    OR EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                        WHERE LOWER(TRIM(g.value)) = %s
                    )
                )
            """
            params.extend([genre_norm, genre_norm])
        sql += order_sql
        params.append(count)
        cur.execute(sql, params)
        rows = cur.fetchall()

        if not rows and uid > 0:
            cur.execute(
                """
                SELECT
                    tr.id,
                    COALESCE(NULLIF(TRIM(tr.title), ''), CONCAT('Track ', tr.id::TEXT)) AS track_title,
                    COALESCE(tr.duration_sec, 0) AS duration_sec,
                    COALESCE(tr.disc_num, 0) AS disc_num,
                    COALESCE(tr.track_num, 0) AS track_num,
                    alb.id AS album_id,
                    alb.title AS album_title,
                    COALESCE(alb.year, 0) AS album_year,
                    alb.has_cover,
                    art.id AS artist_id,
                    art.name AS artist_name,
                    COALESCE(pev.play_count, 0) AS play_count,
                    COALESCE(pr.public_rating, 0) AS public_rating,
                    COALESCE(pr.heat_score, 0) AS heat_score
                FROM files_tracks tr
                JOIN files_albums alb ON alb.id = tr.album_id
                JOIN files_artists art ON art.id = alb.artist_id
                LEFT JOIN (
                    SELECT track_id, COUNT(*)::BIGINT AS play_count
                    FROM files_playback_events
                    WHERE user_id = %s
                    GROUP BY track_id
                ) pev ON pev.track_id = tr.id
                LEFT JOIN files_album_profiles pr
                  ON pr.artist_norm = art.name_norm
                 AND pr.title_norm = alb.title_norm
                ORDER BY
                    COALESCE(pev.play_count, 0) DESC,
                    COALESCE(pr.public_rating, 0) DESC,
                    COALESCE(pr.heat_score, 0) DESC,
                    COALESCE(alb.year, 0) DESC,
                    art.name ASC,
                    alb.title ASC,
                    COALESCE(tr.disc_num, 0) ASC,
                    COALESCE(tr.track_num, 0) ASC,
                    tr.id ASC
                LIMIT %s
                """,
                (uid, count),
            )
            rows = cur.fetchall()

    out: list[dict] = []
    for row in rows:
        out.append(
            {
                "track_id": int(row[0] or 0),
                "track_title": str(row[1] or "").strip(),
                "duration_sec": int(row[2] or 0),
                "disc_num": int(row[3] or 0),
                "track_num": int(row[4] or 0),
                "album_id": int(row[5] or 0),
                "album_title": str(row[6] or "").strip(),
                "album_year": int(row[7] or 0),
                "has_cover": bool(row[8]),
                "artist_id": int(row[9] or 0),
                "artist_name": str(row[10] or "").strip(),
                "play_count": int(row[11] or 0),
                "public_rating": float(row[12] or 0.0),
                "heat_score": float(row[13] or 0.0),
            }
        )
    return out[:count]


def _assistant_playlist_title(
    *,
    query: str,
    artist_name: str = "",
    genre: str = "",
) -> str:
    if artist_name:
        return f"{artist_name} • PMDA picks"
    if genre:
        return f"{genre.title()} • PMDA picks"
    raw = str(query or "").strip()
    quoted = re.search(r"[\"“”']([^\"“”']{3,80})[\"“”']", raw)
    if quoted:
        return quoted.group(1).strip()
    return f"PMDA picks • {datetime.now().strftime('%m/%d %H:%M')}"


def _assistant_create_playlist_from_query(
    conn,
    *,
    user_id: int,
    user_message: str,
    context_artist_id: int,
    base_url: str,
) -> dict:
    uid = max(0, int(user_id or 0))
    if uid <= 0:
        return {"handled": False}
    count = _assistant_extract_requested_count(user_message, default=10, minimum=3, maximum=30)
    artist_id = max(0, int(context_artist_id or 0))
    artist_name = ""
    if artist_id <= 0:
        inferred = _assistant_find_artist_ids_for_query(conn, user_message, limit=1)
        if inferred:
            artist_id = int(inferred[0] or 0)
    if artist_id > 0:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM files_artists WHERE id = %s", (artist_id,))
            row = cur.fetchone()
            artist_name = str((row[0] if row else "") or "").strip()
    genre = ""
    if artist_id <= 0:
        genre = _assistant_find_genre_for_query(conn, user_message)

    tracks = _assistant_playlist_candidate_tracks(conn, user_id=uid, count=count, artist_id=artist_id, genre=genre)
    if not tracks:
        return {"handled": False}

    title = _assistant_playlist_title(query=user_message, artist_name=artist_name, genre=genre)
    description_parts = ["Generated by PMDA Intelligence"]
    if artist_name:
        description_parts.append(f"focus={artist_name}")
    elif genre:
        description_parts.append(f"genre={genre}")
    description = " • ".join(description_parts)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO files_playlists(user_id, name, description, created_at, updated_at) VALUES (%s, %s, %s, NOW(), NOW()) RETURNING id",
                (uid, title[:160], description[:400] or None),
            )
            playlist_id = int((cur.fetchone() or [0])[0] or 0)
            for pos, item in enumerate(tracks):
                cur.execute(
                    """
                    INSERT INTO files_playlist_items(playlist_id, track_id, position, added_at)
                    VALUES (%s, %s, %s, NOW())
                    """,
                    (playlist_id, int(item.get("track_id") or 0), int(pos)),
                )

    lang = _assistant_lang_for_message(user_message)
    if lang == "fr":
        header = f"Playlist creee: {title} ({len(tracks)} morceaux)"
    else:
        header = f"Playlist created: {title} ({len(tracks)} tracks)"
    lines = [header]
    for item in tracks[: min(12, len(tracks))]:
        lines.append(
            f"- {item.get('artist_name') or ''} — {item.get('track_title') or ''} ({item.get('album_title') or ''})"
        )
    links: list[dict] = [
        {
            "kind": "internal",
            "label": title,
            "href": f"/library/playlists/{playlist_id}",
            "entity_type": "playlist",
            "entity_id": int(playlist_id),
            "thumb": str(tracks[0].get("has_cover") or False)
            and f"{(base_url or '').rstrip('/')}/api/library/files/album/{int(tracks[0].get('album_id') or 0)}/cover?size=96"
            or None,
        }
    ]
    seen_albums: set[int] = set()
    for item in tracks:
        album_id = int(item.get("album_id") or 0)
        if album_id <= 0 or album_id in seen_albums:
            continue
        seen_albums.add(album_id)
        links.append(
            {
                "kind": "internal",
                "label": str(item.get("album_title") or "").strip() or f"Album #{album_id}",
                "href": f"/library/album/{album_id}",
                "entity_type": "album",
                "entity_id": album_id,
                "thumb": bool(item.get("has_cover"))
                and f"{(base_url or '').rstrip('/')}/api/library/files/album/{album_id}/cover?size=96"
                or None,
            }
        )
        if len(links) >= 10:
            break
    citations = [
        {
            "entity_type": "playlist",
            "entity_id": int(playlist_id),
            "doc_type": "playlist_builder",
            "source": "pmda_db",
            "title": title,
            "chunk_id": -1,
            "score": 1.0,
            "snippet": _truncate_text("\n".join(lines), max_chars=240),
        }
    ]
    return {
        "handled": True,
        "tool": "playlist_builder_v1",
        "assistant_text": "\n".join(lines).strip(),
        "citations": citations,
        "links": links[:12],
        "ts": int(time.time()),
    }


def _assistant_recommend_albums_from_query(
    conn,
    *,
    user_message: str,
    context_artist_id: int,
    base_url: str,
) -> dict:
    count = _assistant_extract_requested_count(user_message, default=5, minimum=3, maximum=12)
    artist_ids = _assistant_find_artist_ids_for_query(conn, user_message, limit=4)
    if int(context_artist_id or 0) > 0 and int(context_artist_id) not in artist_ids:
        artist_ids.insert(0, int(context_artist_id))
    artist_ids = [aid for aid in artist_ids if aid > 0][:4]
    genre = _assistant_find_genre_for_query(conn, user_message)
    lang = _assistant_lang_for_message(user_message)
    base = (base_url or "").rstrip("/")
    params: list[Any] = []
    where_clauses: list[str] = []
    extra_join_sql = ""
    if artist_ids:
        extra_join_sql = """
            JOIN (
                SELECT DISTINCT artist_id, album_id
                FROM files_artist_album_links
            ) artist_filter ON artist_filter.album_id = alb.id
        """
        where_clauses.append("artist_filter.artist_id = ANY(%s)")
        params.append(artist_ids)
    elif genre:
        genre_norm = _assistant_simplify_for_intent(genre)
        where_clauses.append(
            """
            (
                LOWER(COALESCE(alb.genre, '')) = %s
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements_text(COALESCE(alb.tags_json, '[]')::jsonb) AS g(value)
                    WHERE LOWER(TRIM(g.value)) = %s
                )
            )
            """
        )
        params.extend([genre_norm, genre_norm])
    sql = """
        SELECT
            alb.id,
            alb.title,
            COALESCE(alb.year, 0) AS year,
            art.id AS artist_id,
            art.name AS artist_name,
            alb.has_cover,
            COALESCE(pr.public_rating, 0) AS public_rating,
            COALESCE(pr.heat_score, 0) AS heat_score,
            COALESCE(pr.public_rating_votes, 0) AS rating_votes
        FROM files_albums alb
        JOIN files_artists art ON art.id = alb.artist_id
        LEFT JOIN files_album_profiles pr
          ON pr.artist_norm = art.name_norm
         AND pr.title_norm = alb.title_norm
    """
    sql += extra_join_sql
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses) + " "
    sql += """
        ORDER BY
            COALESCE(pr.public_rating, 0) DESC,
            COALESCE(pr.heat_score, 0) DESC,
            COALESCE(pr.public_rating_votes, 0) DESC,
            COALESCE(alb.year, 0) DESC,
            art.name ASC,
            alb.title ASC
        LIMIT %s
    """
    params.append(int(count))
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    if not rows and artist_ids:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    alb.id,
                    alb.title,
                    COALESCE(alb.year, 0) AS year,
                    art.id AS artist_id,
                    art.name AS artist_name,
                    alb.has_cover,
                    COALESCE(pr.public_rating, 0) AS public_rating,
                    COALESCE(pr.heat_score, 0) AS heat_score,
                    COALESCE(pr.public_rating_votes, 0) AS rating_votes
                FROM files_albums alb
                JOIN files_artists art ON art.id = alb.artist_id
                LEFT JOIN files_album_profiles pr
                  ON pr.artist_norm = art.name_norm
                 AND pr.title_norm = alb.title_norm
                ORDER BY
                    COALESCE(pr.public_rating, 0) DESC,
                    COALESCE(pr.heat_score, 0) DESC,
                    COALESCE(pr.public_rating_votes, 0) DESC,
                    COALESCE(alb.year, 0) DESC,
                    art.name ASC,
                    alb.title ASC
                LIMIT %s
                """,
                (int(count),),
            )
            rows = cur.fetchall()
    if not rows:
        return {"handled": False}

    focus_labels: list[str] = []
    if artist_ids:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM files_artists WHERE id = ANY(%s)", (artist_ids,))
            id_to_name = {int(r[0] or 0): str(r[1] or "").strip() for r in cur.fetchall()}
        focus_labels = [id_to_name.get(aid, "") for aid in artist_ids if id_to_name.get(aid, "")]
    if not focus_labels and genre:
        focus_labels = [genre]
    if lang == "fr":
        heading = "Voici ce que je te conseillerais dans ta bibliotheque"
        if focus_labels:
            heading += f" autour de {', '.join(focus_labels)}"
        heading += " :"
    else:
        heading = "Here is what I would recommend from your library"
        if focus_labels:
            heading += f" around {', '.join(focus_labels)}"
        heading += ":"
    lines = [heading]
    links: list[dict[str, Any]] = []
    citations: list[dict[str, Any]] = []
    for row in rows:
        album_id = int(row[0] or 0)
        title = str(row[1] or "").strip() or f"Album #{album_id}"
        year = int(row[2] or 0)
        artist_id = int(row[3] or 0)
        artist_name = str(row[4] or "").strip()
        has_cover = bool(row[5])
        public_rating = float(row[6] or 0.0)
        heat_score = float(row[7] or 0.0)
        rating_votes = int(row[8] or 0)
        rating_txt = f"{public_rating:.1f}" if public_rating > 0 else "—"
        if lang == "fr":
            lines.append(f"- {artist_name} — {title} ({year if year > 0 else '—'}) · note publique {rating_txt} · score {heat_score:.0f}")
        else:
            lines.append(f"- {artist_name} — {title} ({year if year > 0 else '—'}) · public rating {rating_txt} · heat {heat_score:.0f}")
        links.append(
            {
                "kind": "internal",
                "label": title,
                "href": f"/library/album/{album_id}",
                "entity_type": "album",
                "entity_id": album_id,
                "thumb": has_cover and f"{base}/api/library/files/album/{album_id}/cover?size=96" or None,
            }
        )
        if artist_id > 0:
            links.append(
                {
                    "kind": "internal",
                    "label": artist_name,
                    "href": f"/library/artist/{artist_id}",
                    "entity_type": "artist",
                    "entity_id": artist_id,
                    "thumb": f"{base}/api/library/files/artist/{artist_id}/image?size=192",
                }
            )
        citations.append(
            {
                "entity_type": "album",
                "entity_id": album_id,
                "doc_type": "album_recommendation_tool",
                "source": "pmda_db",
                "title": title,
                "chunk_id": -1,
                "score": 1.0,
                "snippet": f"{artist_name} — {title} · public_rating={public_rating:.1f} · votes={rating_votes} · heat={heat_score:.0f}",
            }
        )
    return {
        "handled": True,
        "tool": "library_recommend_albums",
        "assistant_text": "\n".join(lines).strip(),
        "citations": citations[: min(len(citations), 8)],
        "links": links[:16],
        "ts": int(time.time()),
    }


def _assistant_tool_artist_concerts(conn, *, artist_id: int, limit: int = 12) -> tuple[str, str, list[dict]]:
    artist_id = int(artist_id or 0)
    limit = max(1, min(30, int(limit or 12)))
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM files_artists WHERE id = %s", (artist_id,))
        row = cur.fetchone()
        artist_name = str((row[0] if row else "") or "").strip()
        cur.execute(
            """
            SELECT provider, events_json, source_url
            FROM files_artist_concerts
            WHERE artist_id = %s
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (artist_id,),
        )
        crow = cur.fetchone()
    if not crow:
        return artist_name, "", []
    provider = str(crow[0] or "").strip()
    source_url = str(crow[2] or "").strip()
    try:
        events = json.loads(crow[1] or "[]") if crow[1] else []
    except Exception:
        events = []
    out: list[dict] = []
    if isinstance(events, list):
        for it in events[:limit]:
            if not isinstance(it, dict):
                continue
            title = str(it.get("title") or "").strip()
            city = str(it.get("city") or "").strip()
            country = str(it.get("country") or "").strip()
            venue = str(it.get("venue") or "").strip()
            when = str(it.get("date") or it.get("datetime") or "").strip()
            href = str(it.get("url") or source_url or "").strip()
            out.append(
                {
                    "title": title,
                    "city": city,
                    "country": country,
                    "venue": venue,
                    "when": when,
                    "href": href,
                }
            )
    return artist_name, provider, out


def _assistant_tool_artist_similar(conn, *, artist_id: int, base_url: str, limit: int = 18) -> tuple[str, str, list[dict]]:
    """Return (artist_name, source, similar_items) with local ids + image_url when possible."""
    artist_id = int(artist_id or 0)
    limit = max(1, min(40, int(limit or 18)))
    base = (base_url or "").rstrip("/")
    with conn.cursor() as cur:
        cur.execute("SELECT name, name_norm FROM files_artists WHERE id = %s", (artist_id,))
        row = cur.fetchone()
        artist_name = str((row[0] if row else "") or "").strip()
        artist_norm = str((row[1] if row else "") or "").strip()
        cur.execute(
            """
            SELECT similar_json, source
            FROM files_artist_profiles
            WHERE name_norm = %s
            """,
            (artist_norm,),
        )
        prow = cur.fetchone()
    source = ""
    similar: list[dict] = []
    if prow:
        source = str(prow[1] or "").strip()
        try:
            sim = json.loads(prow[0] or "[]") if prow[0] else []
        except Exception:
            sim = []
        if isinstance(sim, list):
            for it in sim:
                if isinstance(it, dict) and str(it.get("name") or "").strip():
                    similar.append(dict(it))

    if not similar:
        # Local fallback: overlap genres from the library (works for Bandcamp-only libraries).
        similar = _files_similar_artists_by_genre(conn, artist_id, limit=limit) or []
        source = "pmda_db_genre"

    patched = _files_attach_similar_artist_refs(conn, similar[:limit], base)
    return artist_name, (source or "").strip(), (patched or [])[:limit]


def _assistant_try_handle_tool_query(conn, *, user_message: str, context_artist_id: int, base_url: str) -> dict:
    """
    Fast path: answer common DB-grounded questions without calling the LLM.

    Returns {handled: bool, assistant_text, citations, links, tool}.
    """
    intent = _assistant_detect_tool_intent(user_message, context_artist_id=context_artist_id)
    if not intent:
        return {"handled": False}

    lang = _assistant_lang_for_message(user_message)
    now_ts = int(time.time())

    def _cit(entity_type: str, entity_id: int, title: str, snippet: str, doc_type: str = "sql_tool") -> list[dict]:
        return [
            {
                "entity_type": entity_type,
                "entity_id": int(entity_id or 0),
                "doc_type": doc_type,
                "source": "pmda_db",
                "title": title,
                "chunk_id": -1,
                "score": 1.0,
                "snippet": _truncate_text(snippet or "", max_chars=240),
            }
        ]

    if intent == "library_create_playlist":
        return _assistant_create_playlist_from_query(
            conn,
            user_id=_current_user_id_or_zero(),
            user_message=user_message,
            context_artist_id=int(context_artist_id or 0),
            base_url=base_url,
        )

    if intent == "library_recommend_albums":
        return _assistant_recommend_albums_from_query(
            conn,
            user_message=user_message,
            context_artist_id=int(context_artist_id or 0),
            base_url=base_url,
        )

    if intent in {"library_count_artists", "library_count_albums", "library_count_tracks"}:
        counts = _assistant_tool_library_counts(conn)
        if lang == "fr":
            text = (
                "Stats de ta bibliotheque locale (PMDA):\n"
                f"- Artistes: {counts['artists']}\n"
                f"- Albums: {counts['albums']}\n"
                f"- Morceaux: {counts['tracks']}\n"
            )
        else:
            text = (
                "Local library stats (PMDA):\n"
                f"- Artists: {counts['artists']}\n"
                f"- Albums: {counts['albums']}\n"
                f"- Tracks: {counts['tracks']}\n"
            )
        return {
            "handled": True,
            "tool": intent,
            "assistant_text": text.strip(),
            "citations": _cit("library", 1, "Library stats", f"artists={counts['artists']}, albums={counts['albums']}, tracks={counts['tracks']}"),
            "links": [],
            "ts": now_ts,
        }

    if intent == "library_top_genres":
        top = _assistant_tool_library_top_genres(conn, limit=12)
        if not top:
            text = "Je n'ai trouve aucun genre exploitable dans ta bibliotheque." if lang == "fr" else "I could not find any usable genres in your library."
            return {"handled": True, "tool": intent, "assistant_text": text, "citations": _cit("library", 1, "Library genres", "no genres found"), "links": [], "ts": now_ts}
        if lang == "fr":
            lines = ["Genres les plus presents (par #albums):"]
        else:
            lines = ["Most present genres (by #albums):"]
        links: list[dict] = []
        for g, c in top[:12]:
            lines.append(f"- {g}: {c}")
            links.append(
                {
                    "kind": "internal",
                    "label": g,
                    "href": f"/library/genre/{quote(g)}",
                    "entity_type": "genre",
                    "entity_id": 0,
                    "thumb": None,
                }
            )
        snippet = ", ".join([f"{g}={c}" for g, c in top[:6]])
        return {"handled": True, "tool": intent, "assistant_text": "\n".join(lines), "citations": _cit("library", 1, "Library genres", snippet), "links": links[:18], "ts": now_ts}

    if intent == "library_top_labels":
        top = _assistant_tool_library_top_labels(conn, limit=12)
        if not top:
            text = "Je n'ai trouve aucun label exploitable dans ta bibliotheque." if lang == "fr" else "I could not find any usable labels in your library."
            return {"handled": True, "tool": intent, "assistant_text": text, "citations": _cit("library", 1, "Library labels", "no labels found"), "links": [], "ts": now_ts}
        lines = ["Labels les plus presents (par #albums):"] if lang == "fr" else ["Most present labels (by #albums):"]
        links: list[dict] = []
        for label, count in top[:12]:
            lines.append(f"- {label}: {count}")
            links.append(
                {
                    "kind": "internal",
                    "label": label,
                    "href": f"/library/label/{quote(label)}",
                    "entity_type": "label",
                    "entity_id": 0,
                    "thumb": None,
                }
            )
        snippet = ", ".join([f"{label}={count}" for label, count in top[:6]])
        return {"handled": True, "tool": intent, "assistant_text": "\n".join(lines), "citations": _cit("library", 1, "Library labels", snippet), "links": links[:18], "ts": now_ts}

    if intent == "library_top_artists":
        items = _assistant_tool_library_top_artists(conn, base_url=base_url, limit=16)
        if not items:
            text = "Je n'ai trouve aucun artiste dans la base." if lang == "fr" else "I couldn't find any artists in the database."
            return {"handled": True, "tool": intent, "assistant_text": text, "citations": _cit("library", 1, "Library artists", "no artists found"), "links": [], "ts": now_ts}
        if lang == "fr":
            lines = ["Artistes les plus presents (par #albums):"]
        else:
            lines = ["Most represented artists (by #albums):"]
        links: list[dict] = []
        for it in items[:16]:
            nm = str(it.get("artist_name") or "").strip()
            aid = int(it.get("artist_id") or 0)
            ac = int(it.get("album_count") or 0)
            if not nm or aid <= 0:
                continue
            lines.append(f"- {nm}: {ac} albums")
            links.append(
                {
                    "kind": "internal",
                    "label": nm,
                    "href": f"/library/artist/{aid}",
                    "entity_type": "artist",
                    "entity_id": aid,
                    "thumb": it.get("thumb"),
                }
            )
        snippet = "; ".join([f"{it.get('artist_name')}={it.get('album_count')}" for it in items[:6]])
        return {"handled": True, "tool": intent, "assistant_text": "\n".join(lines), "citations": _cit("library", 1, "Top artists", snippet), "links": links[:24], "ts": now_ts}

    if intent == "artist_list_albums" and int(context_artist_id or 0) > 0:
        artist_name, albums = _assistant_tool_artist_list_albums(conn, artist_id=int(context_artist_id), limit=120)
        if lang == "fr":
            lines = [f"Albums locaux pour {artist_name or f'Artist #{int(context_artist_id)}'} ({len(albums)}):"]
        else:
            lines = [f"Local albums for {artist_name or f'Artist #{int(context_artist_id)}'} ({len(albums)}):"]
        links: list[dict] = []
        for a in albums[:120]:
            yr = int(a.get("year") or 0)
            year_txt = str(yr) if yr > 0 else "—"
            fmt = str(a.get("format") or "").strip().upper() or "—"
            loss = "lossless" if bool(a.get("is_lossless")) else "lossy"
            lines.append(f"- {year_txt} · {a.get('title') or ''} ({int(a.get('track_count') or 0)} tracks) · {fmt} · {loss} · album_id={int(a.get('album_id') or 0)}")
            album_id = int(a.get("album_id") or 0)
            if album_id > 0:
                links.append(
                    {
                        "kind": "internal",
                        "label": str(a.get("title") or "").strip() or f"Album #{album_id}",
                        "href": f"/library/album/{album_id}",
                        "entity_type": "album",
                        "entity_id": album_id,
                        "thumb": bool(a.get("has_cover"))
                        and f"{base_url.rstrip('/')}/api/library/files/album/{album_id}/cover?size=96"
                        or None,
                    }
                )
        snippet = ", ".join([str(a.get("title") or "") for a in albums[:6] if str(a.get("title") or "").strip()])
        return {"handled": True, "tool": intent, "assistant_text": "\n".join(lines).strip(), "citations": _cit("artist", int(context_artist_id), artist_name or "Artist albums", snippet), "links": links[:24], "ts": now_ts}

    if intent == "artist_similar_artists" and int(context_artist_id or 0) > 0:
        artist_name, source, sim = _assistant_tool_artist_similar(conn, artist_id=int(context_artist_id), base_url=base_url, limit=20)
        if lang == "fr":
            lines = [f"Artistes similaires a {artist_name or f'Artist #{int(context_artist_id)}'} (source: {source or 'unknown'}):"]
        else:
            lines = [f"Similar artists to {artist_name or f'Artist #{int(context_artist_id)}'} (source: {source or 'unknown'}):"]
        links: list[dict] = []
        for it in (sim or [])[:20]:
            nm = str(it.get("name") or "").strip()
            if not nm:
                continue
            local_id = int(it.get("artist_id") or 0)
            typ = str(it.get("type") or "").strip()
            label = f"{nm} ({typ})" if typ else nm
            if local_id > 0:
                lines.append(f"- {label} (dans ta librairie)")
                links.append(
                    {
                        "kind": "internal",
                        "label": nm,
                        "href": f"/library/artist/{local_id}",
                        "entity_type": "artist",
                        "entity_id": local_id,
                        "thumb": it.get("image_url"),
                    }
                )
            else:
                lines.append(f"- {label}")
                # Generic external lookup (non-local artists).
                links.append(
                    {
                        "kind": "external",
                        "label": nm,
                        "href": f"https://bandcamp.com/search?q={quote(nm)}",
                        "entity_type": "artist",
                        "entity_id": 0,
                        "thumb": it.get("image_url"),
                    }
                )
        snippet = ", ".join([str(it.get("name") or "") for it in (sim or [])[:8] if str(it.get("name") or "").strip()])
        return {"handled": True, "tool": intent, "assistant_text": "\n".join(lines).strip(), "citations": _cit("artist", int(context_artist_id), artist_name or "Similar artists", snippet, doc_type="artist_similar_tool"), "links": links[:28], "ts": now_ts}

    if intent == "artist_concerts" and int(context_artist_id or 0) > 0:
        artist_name, source, events = _assistant_tool_artist_concerts(conn, artist_id=int(context_artist_id), limit=10)
        if not events:
            text = (
                f"Je n'ai trouve aucun concert a venir en cache pour {artist_name or f'Artist #{int(context_artist_id)}'}."
                if lang == "fr"
                else f"I couldn't find any cached upcoming concerts for {artist_name or f'Artist #{int(context_artist_id)}'}."
            )
            return {"handled": True, "tool": intent, "assistant_text": text, "citations": _cit("artist", int(context_artist_id), artist_name or "Artist concerts", "no cached concerts"), "links": [], "ts": now_ts}
        lines = (
            [f"Concerts a venir pour {artist_name or f'Artist #{int(context_artist_id)}'} (source: {source or 'unknown'}):"]
            if lang == "fr"
            else [f"Upcoming concerts for {artist_name or f'Artist #{int(context_artist_id)}'} (source: {source or 'unknown'}):"]
        )
        links: list[dict] = []
        for ev in events[:10]:
            city = str(ev.get("city") or "").strip()
            country = str(ev.get("country") or "").strip()
            venue = str(ev.get("venue") or "").strip()
            when = str(ev.get("when") or "").strip()
            location = ", ".join([part for part in [city, country] if part])
            fragments = [frag for frag in [when, venue, location] if frag]
            lines.append(f"- {' · '.join(fragments) if fragments else (ev.get('title') or 'Concert')}")
            href = str(ev.get("href") or "").strip()
            if href.startswith(("http://", "https://")):
                links.append(
                    {
                        "kind": "external",
                        "label": venue or location or when or (ev.get("title") or "Concert"),
                        "href": href,
                        "entity_type": "concert",
                        "entity_id": 0,
                        "thumb": None,
                    }
                )
        snippet = "; ".join([" / ".join([str(ev.get("when") or "").strip(), str(ev.get("venue") or "").strip(), str(ev.get("city") or "").strip()]).strip(" /") for ev in events[:4]])
        return {"handled": True, "tool": intent, "assistant_text": "\n".join(lines).strip(), "citations": _cit("artist", int(context_artist_id), artist_name or "Artist concerts", snippet or "cached concerts", doc_type="artist_concerts_tool"), "links": links[:10], "ts": now_ts}

    return {"handled": False}


def _assistant_should_try_sql_agent(user_message: str) -> bool:
    """
    Return True when the user's question looks like a library/stats query where we should
    attempt a DB-backed SQL answer (instead of biography/interpretation questions).
    """
    s = _assistant_simplify_for_intent(user_message)
    if not s:
        return False

    # Avoid running the SQL agent for biography-like prompts; those should use RAG docs.
    if any(
        tok in s
        for tok in [
            "qui est",
            "who is",
            "biographie",
            "biography",
            "resume",
            "resumer",
            "summarize",
            "tell me about",
            "dis m en plus",
            "raconte",
            "parle moi de",
            "parle de",
            "compare",
            "comparaison",
            "versus",
            "vs",
            "recommend",
            "recommande",
            "recommandation",
            "discover",
            "decouvrir",
            "decouverte",
        ]
    ):
        return False

    # Strong signals that the question is about the user's local collection / statistics.
    strong = [
        "ma collection", "ma bibliotheque", "bibliotheque", "collection", "my library", "library",
        "dans ma", "dans mon", "j ai", "ai je", "do i have", "owned",
        "combien", "nombre", "how many", "count",
        "stat", "stats", "statistique", "statistiques",
        "ecoute", "ecoutes", "listened", "played", "playback", "plays", "lecture",
        "like", "dislike", "favori", "favorite", "playlist",
    ]
    if any(tok in s for tok in strong):
        return True

    # Weaker signals: entity words + possessive context.
    if any(tok in s for tok in ["artiste", "artist", "album", "morceau", "track", "genre", "label", "annee", "year"]):
        if any(tok in s for tok in ["ma", "mon", "mes", "my", "dans", "collection", "bibliotheque", "library"]):
            return True

    return False


def _assistant_extract_json_obj(text: str) -> dict:
    """Best-effort JSON object extractor for LLM outputs (no exceptions)."""
    raw = (text or "").strip()
    if not raw:
        return {}

    # Strip common fenced formats.
    raw = re.sub(r"^\s*```(?:json)?\s*", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"\s*```\s*$", "", raw).strip()

    def _try_parse_obj(s: str) -> dict:
        s = (s or "").strip()
        if not s:
            return {}
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            pass
        # Some models still output Python-like dicts (single quotes). literal_eval is safe for literals.
        try:
            py = s
            py = re.sub(r"\bnull\b", "None", py, flags=re.IGNORECASE)
            py = re.sub(r"\btrue\b", "True", py, flags=re.IGNORECASE)
            py = re.sub(r"\bfalse\b", "False", py, flags=re.IGNORECASE)
            obj = ast.literal_eval(py)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    # If the output is already a clean JSON object, parse directly.
    direct = _try_parse_obj(raw)
    if direct:
        return direct

    # Otherwise, scan for the first balanced {...} object (ignoring braces in strings).
    start = None
    depth = 0
    in_str = False
    str_ch = ""
    esc = False
    for i, ch in enumerate(raw):
        if in_str:
            if esc:
                esc = False
                continue
            if ch == "\\":
                esc = True
                continue
            if ch == str_ch:
                in_str = False
                str_ch = ""
            continue
        if ch in ("\"", "'"):
            in_str = True
            str_ch = ch
            continue
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
            continue
        if ch == "}" and depth > 0:
            depth -= 1
            if depth == 0 and start is not None:
                cand = raw[start : i + 1]
                obj = _try_parse_obj(cand)
                if obj:
                    return obj
                start = None
    return {}


def _assistant_validate_readonly_sql(sql: str) -> tuple[bool, str]:
    """Reject obviously unsafe SQL. Only allow SELECT/CTE SELECT statements."""
    s = (sql or "").strip()
    if not s:
        return (False, "empty_sql")
    # Allow a single trailing semicolon, but reject any other semicolons.
    while s.endswith(";"):
        s = s[:-1].rstrip()
    if ";" in s:
        return (False, "multiple_statements_forbidden")
    low = s.lower()
    if not re.match(r"^(select|with)\b", low.strip()):
        return (False, "only_select_queries_allowed")
    # Disallow common mutating / privileged keywords.
    banned = [
        "insert", "update", "delete", "drop", "alter", "truncate", "create",
        "grant", "revoke", "copy", "call", "do", "execute", "vacuum",
        "analyze", "refresh", "listen", "notify",
    ]
    for kw in banned:
        if re.search(rf"\b{kw}\b", low):
            return (False, f"forbidden_keyword:{kw}")
    if re.search(r"\bpg_sleep\b", low):
        return (False, "forbidden_function:pg_sleep")
    return (True, "ok")


def _assistant_sql_agent_generate_query(
    *,
    user_message: str,
    context_artist_id: int,
    context_artist_name: str,
    provider: str,
    model: str,
    error_hint: str = "",
) -> dict:
    """
    Ask the LLM for a single read-only SQL query + params (positional).
    Returns dict with keys: sql, params (best-effort).
    """
    schema = (
        "files_artists(id, name, name_norm, album_count, track_count, broken_albums_count, has_image, image_path, created_at, updated_at)\n"
        "files_albums(id, artist_id, title, title_norm, folder_path, year, date_text, genre, label, tags_json, format, is_lossless, has_cover, cover_path, "
        "mb_identified, musicbrainz_release_group_id, discogs_release_id, lastfm_album_mbid, bandcamp_album_url, metadata_source, track_count, total_duration_sec, "
        "is_broken, expected_track_count, actual_track_count, missing_indices_json, missing_required_tags_json, primary_tags_json, created_at, updated_at)\n"
        "files_tracks(id, album_id, file_path, title, disc_num, track_num, duration_sec, format, bitrate, sample_rate, bit_depth, file_size_bytes, created_at, updated_at)\n"
        "files_playback_events(user_id, track_id, event_type, played_seconds, created_at)\n"
        "files_user_entity_likes(user_id, entity_type, entity_id, entity_key, liked, source, created_at, updated_at)\n"
        "files_playlists(id, user_id, name, description, created_at, updated_at)\n"
        "files_social_recommendations(id, sender_user_id, recipient_user_id, entity_type, entity_id, entity_key, entity_label, entity_subtitle, entity_href, message, liked_by_recipient, created_at, read_at, status)\n"
        "files_reco_events(session_id, track_id, album_id, artist_id, event_type, played_seconds, created_at)\n"
        "files_artist_profiles(name_norm, artist_name, bio, short_bio, tags_json, similar_json, source, updated_at)\n"
        "files_album_profiles(artist_norm, title_norm, album_title, description, short_description, tags_json, source, updated_at)\n"
    )
    system_msg = (
        "You are a PostgreSQL query generator for PMDA (a local music library app).\n"
        "Output ONLY a JSON object, no extra text.\n"
        "JSON keys:\n"
        "- sql: string (a single read-only query; SELECT or WITH ... SELECT)\n"
        "- params: array (positional parameters for psycopg, use %s placeholders)\n"
        "- title: short string describing what the query answers\n"
        "Rules:\n"
        "- No semicolons. No multiple statements.\n"
        "- Never use INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/TRUNCATE/COPY/CALL/DO.\n"
        "- Prefer parameterized values (%s) for user-provided strings.\n"
        "- For non-aggregate queries, include LIMIT <= 100.\n"
        "- If the user asks about their listening stats, use files_playback_events filtered by the current authenticated user.\n"
        "Formatting rules for downstream UI:\n"
        "- If returning artists, include columns: artist_id, artist_name.\n"
        "- If returning labels, include a column named: label.\n"
        "- If returning albums, include columns: artist_id, artist_name, album_id, album_title (and bandcamp_album_url when available).\n"
        "Schema:\n"
        f"{schema}"
        "Join hints:\n"
        "- files_tracks.album_id -> files_albums.id\n"
        "- files_albums.artist_id -> files_artists.id\n"
    )
    ctx_line = ""
    if int(context_artist_id or 0) > 0 and (context_artist_name or "").strip():
        ctx_line = f"Current context artist: id={int(context_artist_id)} name={context_artist_name}\n"
    user_msg = (
        f"{ctx_line}"
        "User question:\n"
        f"{(user_message or '').strip()}\n"
    )
    if (error_hint or "").strip():
        user_msg = (
            f"{user_msg}\n"
            "Previous error (fix your SQL accordingly):\n"
            f"{_truncate_text(str(error_hint), max_chars=800)}\n"
        )

    out = ""
    provider_lower = (provider or "").strip().lower()
    if provider_lower == "openai" and openai_client:
        # Prefer JSON mode for reliability; fall back to normal call on older models.
        direct_started_at = time.time()
        direct_response_obj: Any = None
        direct_status = "failed"
        direct_error = ""
        param_style = getattr(sys.modules[__name__], "RESOLVED_PARAM_STYLE", "mct")
        _kwargs = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "timeout": _openai_request_timeout_seconds(),
        }
        if param_style == "mct":
            _kwargs["max_completion_tokens"] = 420
        else:
            _kwargs["max_tokens"] = 420
        try:
            resp = openai_client.chat.completions.create(**_kwargs)
            direct_response_obj = resp
            direct_status = "completed"
            out = (resp.choices[0].message.content or "").strip()
        except Exception as e:
            direct_error = str(e)
            # Retry with other token parameter style when OpenAI errors on one of them.
            err_msg = str(e).lower()
            try:
                if "unsupported_parameter" in err_msg or "400" in err_msg:
                    if "max_completion_tokens" in err_msg and ("max_tokens" in err_msg or "use" in err_msg):
                        _kwargs.pop("max_completion_tokens", None)
                        _kwargs["max_tokens"] = 420
                        resp = openai_client.chat.completions.create(**_kwargs)
                        direct_response_obj = resp
                        direct_status = "completed"
                        direct_error = ""
                        out = (resp.choices[0].message.content or "").strip()
                    elif "max_tokens" in err_msg and "max_completion_tokens" in err_msg:
                        _kwargs.pop("max_tokens", None)
                        _kwargs["max_completion_tokens"] = 420
                        resp = openai_client.chat.completions.create(**_kwargs)
                        direct_response_obj = resp
                        direct_status = "completed"
                        direct_error = ""
                        out = (resp.choices[0].message.content or "").strip()
            except Exception:
                out = ""
            if not out:
                logging.debug("Assistant SQL agent JSON-mode call failed, falling back: %s", str(e)[:240])
                out = call_ai_provider_longform(
                    provider,
                    model,
                    system_msg,
                    user_msg,
                    max_tokens=420,
                    analysis_type="assistant_chat",
                )
        finally:
            recorder = globals().get("record_ai_usage")
            if callable(recorder):
                recorder(
                    provider="openai",
                    model=str(model or ""),
                    endpoint_kind="longform",
                    analysis_type="assistant_chat",
                    started_at=direct_started_at,
                    status=direct_status,
                    response_obj=direct_response_obj,
                    image_inputs=0,
                    error=direct_error,
                    metadata={"feature": "assistant_sql_agent_json_mode", "max_tokens": 420},
                )
    else:
        out = call_ai_provider_longform(
            provider,
            model,
            system_msg,
            user_msg,
            max_tokens=420,
            analysis_type="assistant_chat",
        )

    obj = _assistant_extract_json_obj(out)
    if not obj:
        logging.debug("Assistant SQL agent produced no JSON plan. out=%s", _truncate_text(out, max_chars=600))
        return {}
    if "params" in obj and not isinstance(obj.get("params"), list):
        obj["params"] = []
    return obj


def _assistant_sql_agent_execute(conn, *, sql: str, params: list, max_rows: int = 100) -> tuple[list[str], list[tuple]]:
    """Execute SQL with a statement timeout. Returns (columns, rows)."""
    max_rows = max(1, min(500, int(max_rows or 100)))
    params = params if isinstance(params, list) else []
    with conn.cursor() as cur:
        # Keep requests responsive; reset after executing.
        try:
            cur.execute("SET statement_timeout TO '5000ms'")
        except Exception:
            pass
        try:
            cur.execute(sql, params)
            cols = [str(d.name or "") for d in (cur.description or []) if getattr(d, "name", None)]
            rows = cur.fetchmany(max_rows)
            return (cols, rows)
        finally:
            try:
                cur.execute("RESET statement_timeout")
            except Exception:
                pass


def _assistant_sql_agent_format_result(*, lang: str, title: str, cols: list[str], rows: list[tuple]) -> str:
    """Format SQL rows into a chat-friendly answer (deterministic; no hallucination)."""
    title = (title or "").strip() or ("Resultats (PMDA)" if lang == "fr" else "Results (PMDA)")
    if not rows:
        return f"{title}:\n- Aucun resultat." if lang == "fr" else f"{title}:\n- No results."

    if len(rows) == 1 and len(cols) == 1:
        val = rows[0][0]
        return f"{title}: {val}"

    # Special-case common 2-col shapes: (name, count)
    if len(cols) == 2:
        lines = [f"{title}:"]
        for r in rows[:20]:
            a = str(r[0] if len(r) > 0 else "").strip()
            b = r[1] if len(r) > 1 else ""
            if not a:
                continue
            lines.append(f"- {a}: {b}")
        return "\n".join(lines).strip()

    # Generic row formatter.
    lines = [f"{title}:"]
    for r in rows[:20]:
        parts = []
        for idx, col in enumerate(cols[:8]):
            try:
                v = r[idx]
            except Exception:
                v = None
            if v is None or v == "":
                continue
            parts.append(f"{col}={v}")
        if parts:
            lines.append(f"- " + " · ".join(parts))
    return "\n".join(lines).strip()


def _assistant_sql_agent_links_from_result(*, cols: list[str], rows: list[tuple], base_url: str) -> list[dict]:
    """Create clickable links (internal + external) from common result shapes."""
    base = (base_url or "").rstrip("/")
    idx = {str(c or "").strip().lower(): i for i, c in enumerate(cols or []) if str(c or "").strip()}

    links: list[dict] = []
    seen: set[str] = set()

    def _add(link: dict) -> None:
        href = str(link.get("href") or "").strip()
        label = str(link.get("label") or "").strip()
        if not href or not label:
            return
        key = f"{href}||{label}"
        if key in seen:
            return
        seen.add(key)
        links.append(link)

    for r in (rows or [])[:60]:
        # Artist link
        if "artist_id" in idx:
            try:
                aid = int(r[idx["artist_id"]] or 0)
            except Exception:
                aid = 0
            if aid > 0:
                nm = ""
                if "artist_name" in idx:
                    try:
                        nm = str(r[idx["artist_name"]] or "").strip()
                    except Exception:
                        nm = ""
                if not nm:
                    nm = f"Artist #{aid}"
                _add(
                    {
                        "kind": "internal",
                        "label": nm,
                        "href": f"/library/artist/{aid}",
                        "entity_type": "artist",
                        "entity_id": aid,
                        "thumb": f"{base}/api/library/files/artist/{aid}/image?size=96",
                    }
                )

        # Label link
        if "label" in idx:
            try:
                lab = str(r[idx["label"]] or "").strip()
            except Exception:
                lab = ""
            if lab:
                _add(
                    {
                        "kind": "internal",
                        "label": lab,
                        "href": f"/library/label/{quote(lab)}",
                        "entity_type": "label",
                        "entity_id": 0,
                        "thumb": None,
                    }
                )

        # Bandcamp album link when present (nice "internet link" without needing extra crawling).
        if "bandcamp_album_url" in idx:
            try:
                url = str(r[idx["bandcamp_album_url"]] or "").strip()
            except Exception:
                url = ""
            if url and url.startswith(("http://", "https://")):
                title = ""
                for key in ("album_title", "title"):
                    if key in idx:
                        try:
                            title = str(r[idx[key]] or "").strip()
                        except Exception:
                            title = ""
                        if title:
                            break
                if not title:
                    title = "Bandcamp"
                _add(
                    {
                        "kind": "external",
                        "label": title,
                        "href": url,
                        "entity_type": "album",
                        "entity_id": 0,
                        "thumb": None,
                    }
                )

    return links[:18]


def _assistant_try_handle_sql_agent_query(
    conn,
    *,
    user_message: str,
    context_artist_id: int,
    base_url: str,
    provider: str = "",
    model: str = "",
) -> dict:
    """
    Slow path (LLM-assisted): generate a safe SELECT query, execute, and format the result.
    Returns {handled: bool, assistant_text, citations, links, tool}.
    """
    if not _assistant_should_try_sql_agent(user_message):
        return {"handled": False}

    # Context artist name (optional).
    context_artist_id = int(context_artist_id or 0)
    context_artist_name = ""
    if context_artist_id > 0:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM files_artists WHERE id = %s", (context_artist_id,))
                row = cur.fetchone()
            context_artist_name = str((row[0] if row else "") or "").strip()
        except Exception:
            context_artist_name = ""

    provider = str(provider or "").strip()
    model = str(model or "").strip()
    if not provider or not model:
        runtime_status = _assistant_runtime_status(user_id=_current_user_id_or_zero(), user_msg=user_message)
        provider = provider or str(runtime_status.get("ai_provider") or getattr(sys.modules[__name__], "AI_PROVIDER", "openai"))
        model = model or str(runtime_status.get("ai_model") or _ai_model_display_name(provider))
    lang = _assistant_lang_for_message(user_message)

    last_err = ""
    plan: dict = {}
    sql = ""
    params: list = []
    title = ""
    cols: list[str] = []
    rows: list[tuple] = []

    for attempt in range(2):
        plan = _assistant_sql_agent_generate_query(
            user_message=user_message,
            context_artist_id=context_artist_id,
            context_artist_name=context_artist_name,
            provider=provider,
            model=model,
            error_hint=last_err if attempt > 0 else "",
        )
        sql = str(plan.get("sql") or "").strip()
        params = plan.get("params") if isinstance(plan.get("params"), list) else []
        title = str(plan.get("title") or "").strip()

        # Strip trailing semicolons (we still forbid multi-statement SQL).
        while sql.endswith(";"):
            sql = sql[:-1].rstrip()

        ok, reason = _assistant_validate_readonly_sql(sql)
        if not ok:
            logging.debug("Assistant SQL agent rejected query (%s): %s", reason, sql[:240])
            last_err = f"validation_failed:{reason}"
            continue

        # Add a hard LIMIT if the model forgot one (safe default).
        if not re.search(r"\blimit\b", sql, flags=re.IGNORECASE):
            sql = f"{sql.rstrip()} LIMIT 100"

        try:
            cols, rows = _assistant_sql_agent_execute(conn, sql=sql, params=params, max_rows=100)
            break
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)[:400]}"
            logging.debug("Assistant SQL agent execute failed (attempt %s): %s", attempt + 1, last_err)
            cols, rows = ([], [])
            continue

    if not cols and not rows:
        return {"handled": False}

    assistant_text = _assistant_sql_agent_format_result(lang=lang, title=title, cols=cols, rows=rows)
    links = _assistant_sql_agent_links_from_result(cols=cols, rows=rows, base_url=base_url)

    # Attach a minimal citation so the UI can show "pmda_db" provenance.
    snippet = _truncate_text(assistant_text, max_chars=240)
    citations = [
        {
            "entity_type": "library" if context_artist_id <= 0 else "artist",
            "entity_id": 1 if context_artist_id <= 0 else int(context_artist_id),
            "doc_type": "sql_agent",
            "source": "pmda_db",
            "title": title or ("SQL result" if lang != "fr" else "Resultat SQL"),
            "chunk_id": -1,
            "score": 1.0,
            "snippet": snippet,
        }
    ]

    return {
        "handled": True,
        "tool": "sql_agent_v1",
        "assistant_text": assistant_text,
        "citations": citations,
        "links": links,
        "ts": int(time.time()),
    }


def api_assistant_status():
    """Lightweight health/status endpoint for the in-UI assistant."""
    postgres_ready = False
    if _get_library_mode() == "files":
        postgres_ready = bool(_files_pg_init_schema())
    runtime_status = _assistant_runtime_status()
    payload = {
        "library_mode": _get_library_mode(),
        "ai_provider": str(runtime_status.get("ai_provider") or getattr(sys.modules[__name__], "AI_PROVIDER", "openai")),
        "ai_model": str(runtime_status.get("ai_model") or getattr(sys.modules[__name__], "OPENAI_MODEL", "")),
        "ai_auth_mode": str(runtime_status.get("ai_auth_mode") or ""),
        "ai_ready": bool(runtime_status.get("ai_ready")),
        "ai_error": runtime_status.get("ai_error"),
        "postgres_ready": postgres_ready,
        "db_tools_ready": postgres_ready and _get_library_mode() == "files",
        "pg_host": PMDA_PG_HOST,
        "pg_port": PMDA_PG_PORT,
        "pg_db": PMDA_PG_DB,
        "pg_user": PMDA_PG_USER,
        "config_dir": str(CONFIG_DIR),
        "config_sources": {k: ENV_SOURCES.get(k, "") for k in ("AI_PROVIDER", "OPENAI_API_KEY", "OPENAI_MODEL", "FILES_ROOTS", "LIBRARY_MODE")},
    }
    return jsonify(payload)


def api_assistant_get_session(session_id: str):
    if _get_library_mode() != "files":
        return jsonify({"error": "Assistant is available in Files mode only"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        limit = max(1, min(400, _parse_int_loose(request.args.get("limit"), 120)))
        with conn.cursor() as cur:
            cur.execute("SELECT session_id FROM assistant_sessions WHERE session_id = %s", ((session_id or "").strip(),))
            if not cur.fetchone():
                return jsonify({"session_id": (session_id or "").strip(), "messages": []})
            cur.execute(
                """
                SELECT id, role, content, context_json, metadata_json, EXTRACT(EPOCH FROM created_at)::BIGINT
                FROM assistant_messages
                WHERE session_id = %s
                ORDER BY created_at ASC
                LIMIT %s
                """,
                ((session_id or "").strip(), int(limit)),
            )
            rows = cur.fetchall()
        messages = []
        for mid, role, content, ctx_json, meta_json, created_at in rows:
            try:
                ctx = json.loads(ctx_json or "{}") if ctx_json else {}
            except Exception:
                ctx = {}
            try:
                meta = json.loads(meta_json or "{}") if meta_json else {}
            except Exception:
                meta = {}
            messages.append(
                {
                    "id": int(mid or 0),
                    "role": (role or "").strip().lower(),
                    "content": (content or "").strip(),
                    "created_at": int(created_at or 0),
                    "context": ctx if isinstance(ctx, dict) else {},
                    "metadata": meta if isinstance(meta, dict) else {},
                }
            )
        return jsonify({"session_id": (session_id or "").strip(), "messages": messages})
    finally:
        conn.close()


def api_assistant_chat():
    """Chat endpoint: uses Postgres-backed RAG (artist profiles + local library snapshot)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Assistant is available in Files mode only"}), 400
    if not _auth_user_can_use_ai(_current_user_or_empty()):
        return jsonify({"error": "AI access is disabled for this user"}), 403
    data = request.get_json() or {}
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400
    user_message = str(data.get("message") or "").strip()
    if not user_message:
        return jsonify({"error": "message is required"}), 400
    session_id = str(data.get("session_id") or "").strip()
    ctx = data.get("context") or {}
    if not isinstance(ctx, dict):
        ctx = {}

    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503

    try:
        _assistant_maybe_gc(conn)
        session_id = _assistant_ensure_session(conn, session_id)

        # Conversation history before we insert current user message.
        history = _assistant_fetch_session_messages(conn, session_id, limit=10)

        # Ensure a library snapshot doc exists so collection-wide questions have grounding.
        try:
            _assistant_ingest_library_rag(conn)
        except Exception:
            pass

        context_artist_id = _parse_int_loose(ctx.get("artist_id"), 0)
        context_artist_ids: list[int] = []
        context_inferred = False
        context_info: dict = {}

        if context_artist_id and int(context_artist_id) > 0:
            context_artist_id = int(context_artist_id)
            context_artist_ids = [context_artist_id]
            context_info = _assistant_ingest_artist_rag(conn, context_artist_id)
            context_info["artist_names"] = [str(context_info.get("artist_name") or "").strip()] if str(context_info.get("artist_name") or "").strip() else []
        else:
            # Best-effort inference for "Ask PMDA" from anywhere.
            inferred_ids = _assistant_find_artist_ids_for_query(conn, user_message, limit=4)
            if inferred_ids:
                context_inferred = True
                context_artist_id = int(inferred_ids[0] or 0)
                context_artist_ids = [int(x or 0) for x in inferred_ids if int(x or 0) > 0][:4]
                ctx = dict(ctx)
                ctx["artist_id"] = int(context_artist_id)
                ctx["context_inferred"] = True
                first_context = _assistant_ingest_artist_rag(conn, int(context_artist_id))
                artist_names: list[str] = []
                if str(first_context.get("artist_name") or "").strip():
                    artist_names.append(str(first_context.get("artist_name") or "").strip())
                for extra_artist_id in context_artist_ids[1:]:
                    try:
                        extra_context = _assistant_ingest_artist_rag(conn, int(extra_artist_id))
                        extra_name = str(extra_context.get("artist_name") or "").strip()
                        if extra_name and extra_name not in artist_names:
                            artist_names.append(extra_name)
                    except Exception:
                        continue
                context_info = dict(first_context or {})
                context_info["artist_names"] = artist_names

        # Persist the user message.
        user_msg_row = _assistant_insert_message(
            conn,
            session_id=session_id,
            role="user",
            content=user_message,
            context=ctx,
            metadata={},
        )

        runtime_status = _assistant_runtime_status(user_id=_current_user_id_or_zero(), user_msg=user_message)
        ai_ready = bool(runtime_status.get("ai_ready"))
        ai_error = str(runtime_status.get("ai_error") or "").strip() or "AI is not configured"
        force_llm_rag = bool(ai_ready) and _assistant_should_force_llm_rag(user_message)

        # Fast path: answer common DB-grounded questions without paying LLM tokens.
        tool = {"handled": False}
        if not force_llm_rag:
            try:
                tool = _assistant_try_handle_tool_query(
                    conn,
                    user_message=user_message,
                    context_artist_id=int(context_artist_id or 0),
                    base_url=request.url_root.rstrip("/"),
                )
            except Exception:
                tool = {"handled": False}

        if tool.get("handled"):
            assistant_text = str(tool.get("assistant_text") or "").strip()
            citations = tool.get("citations") or []
            links = tool.get("links") or []
            assistant_meta = {
                "provider": "pmda_db",
                "model": str(tool.get("tool") or "sql_tool_v1"),
                "context_inferred": bool(context_inferred),
                "citations": citations,
                "links": links,
                "tool": str(tool.get("tool") or ""),
            }
            assistant_msg_row = _assistant_insert_message(
                conn,
                session_id=session_id,
                role="assistant",
                content=assistant_text,
                context=ctx,
                metadata=assistant_meta,
            )
            return jsonify(
                {
                    "session_id": session_id,
                    "user_message": user_msg_row,
                    "assistant_message": assistant_msg_row,
                    "citations": citations,
                }
            )

        if not ai_ready:
            return jsonify({"error": ai_error, "db_tools_only": True}), 503

        # Slow path: LLM-assisted SQL query over the library DB (read-only).
        sql_tool = {"handled": False}
        if not force_llm_rag:
            try:
                sql_tool = _assistant_try_handle_sql_agent_query(
                    conn,
                    user_message=user_message,
                    context_artist_id=int(context_artist_id or 0),
                    base_url=request.url_root.rstrip("/"),
                    provider=str(runtime_status.get("ai_provider") or ""),
                    model=str(runtime_status.get("ai_model") or ""),
                )
            except Exception:
                sql_tool = {"handled": False}

        if sql_tool.get("handled"):
            assistant_text = str(sql_tool.get("assistant_text") or "").strip()
            citations = sql_tool.get("citations") or []
            links = sql_tool.get("links") or []
            assistant_meta = {
                "provider": "pmda_db",
                "model": str(sql_tool.get("tool") or "sql_agent_v1"),
                "context_inferred": bool(context_inferred),
                "citations": citations,
                "links": links,
                "tool": str(sql_tool.get("tool") or ""),
            }
            assistant_msg_row = _assistant_insert_message(
                conn,
                session_id=session_id,
                role="assistant",
                content=assistant_text,
                context=ctx,
                metadata=assistant_meta,
            )
            return jsonify(
                {
                    "session_id": session_id,
                    "user_message": user_msg_row,
                    "assistant_message": assistant_msg_row,
                    "citations": citations,
                }
            )

        retrieved = _assistant_retrieve_chunks(
            conn,
            user_message,
            artist_id=int(context_artist_id or 0),
            artist_ids=context_artist_ids,
            k=8,
        )
        web_results: list[dict[str, Any]] = []
        if _assistant_should_include_web_discovery(user_message, retrieved_chunks_count=len(retrieved.get("chunks") or [])):
            try:
                discovery_query = str(user_message or "").strip()
                artist_names = [str(x or "").strip() for x in (context_info.get("artist_names") or []) if str(x or "").strip()]
                if artist_names:
                    discovery_query = f"{discovery_query} {' '.join(artist_names[:2])}".strip()
                web_results = _web_search_serper(discovery_query, num=5, allow_ai_fallback=True) or []
            except Exception:
                web_results = []
        system_msg, prompt = _assistant_build_prompt(
            user_message=user_message,
            retrieved=retrieved,
            history=history,
            context_info=context_info,
            web_results=web_results,
        )

        llm_runtime = _assistant_runtime_status(
            user_id=_current_user_id_or_zero(),
            system_msg=system_msg,
            user_msg=prompt,
        )
        if not bool(llm_runtime.get("ai_ready")):
            return jsonify({"error": str(llm_runtime.get("ai_error") or "").strip() or "AI is not configured", "db_tools_only": True}), 503
        provider = str(llm_runtime.get("ai_provider") or runtime_status.get("ai_provider") or getattr(sys.modules[__name__], "AI_PROVIDER", "openai"))
        model = str(llm_runtime.get("ai_model") or runtime_status.get("ai_model") or _ai_model_display_name(provider))
        assistant_text = call_ai_provider_longform(
            provider,
            model,
            system_msg,
            prompt,
            max_tokens=900,
            analysis_type="assistant_chat",
        )

        assistant_links = _assistant_links_from_citations(
            citations=retrieved.get("citations") or [],
            base_url=request.url_root.rstrip("/"),
        )
        assistant_links.extend(_assistant_links_from_web_results(web_results))
        assistant_meta = {
            "provider": provider,
            "model": model,
            "context_inferred": bool(context_inferred),
            "citations": retrieved.get("citations") or [],
            "links": assistant_links[:18],
        }
        assistant_msg_row = _assistant_insert_message(
            conn,
            session_id=session_id,
            role="assistant",
            content=assistant_text,
            context=ctx,
            metadata=assistant_meta,
        )

        return jsonify(
            {
                "session_id": session_id,
                "user_message": user_msg_row,
                "assistant_message": assistant_msg_row,
                "citations": retrieved.get("citations") or [],
            }
        )
    finally:
        conn.close()


_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}

def _assistant_runtime_status_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_runtime_status(*args, **kwargs)

def _assistant_find_artist_ids_for_query_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_find_artist_ids_for_query(*args, **kwargs)

def _assistant_fetch_session_messages_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_fetch_session_messages(*args, **kwargs)

def _assistant_retrieve_chunks_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_retrieve_chunks(*args, **kwargs)

def _assistant_maybe_gc_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_maybe_gc(*args, **kwargs)

def _assistant_ensure_session_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_ensure_session(*args, **kwargs)

def _assistant_insert_message_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_insert_message(*args, **kwargs)

def _assistant_build_prompt_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_build_prompt(*args, **kwargs)

def _assistant_links_from_citations_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_links_from_citations(*args, **kwargs)

def _assistant_links_from_web_results_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_links_from_web_results(*args, **kwargs)

def _assistant_should_include_web_discovery_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_should_include_web_discovery(*args, **kwargs)

def _assistant_simplify_for_intent_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_simplify_for_intent(*args, **kwargs)

def _assistant_lang_for_message_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_lang_for_message(*args, **kwargs)

def _assistant_detect_tool_intent_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_detect_tool_intent(*args, **kwargs)

def _assistant_should_force_llm_rag_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_should_force_llm_rag(*args, **kwargs)

def _assistant_tool_library_counts_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_tool_library_counts(*args, **kwargs)

def _assistant_tool_library_top_genres_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_tool_library_top_genres(*args, **kwargs)

def _assistant_tool_library_top_labels_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_tool_library_top_labels(*args, **kwargs)

def _assistant_tool_library_top_artists_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_tool_library_top_artists(*args, **kwargs)

def _assistant_tool_artist_list_albums_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_tool_artist_list_albums(*args, **kwargs)

def _assistant_extract_requested_count_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_extract_requested_count(*args, **kwargs)

def _assistant_find_genre_for_query_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_find_genre_for_query(*args, **kwargs)

def _assistant_playlist_candidate_tracks_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_playlist_candidate_tracks(*args, **kwargs)

def _assistant_playlist_title_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_playlist_title(*args, **kwargs)

def _assistant_create_playlist_from_query_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_create_playlist_from_query(*args, **kwargs)

def _assistant_recommend_albums_from_query_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_recommend_albums_from_query(*args, **kwargs)

def _assistant_tool_artist_concerts_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_tool_artist_concerts(*args, **kwargs)

def _assistant_tool_artist_similar_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_tool_artist_similar(*args, **kwargs)

def _assistant_try_handle_tool_query_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_try_handle_tool_query(*args, **kwargs)

def _assistant_should_try_sql_agent_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_should_try_sql_agent(*args, **kwargs)

def _assistant_extract_json_obj_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_extract_json_obj(*args, **kwargs)

def _assistant_validate_readonly_sql_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_validate_readonly_sql(*args, **kwargs)

def _assistant_sql_agent_generate_query_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_sql_agent_generate_query(*args, **kwargs)

def _assistant_sql_agent_execute_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_sql_agent_execute(*args, **kwargs)

def _assistant_sql_agent_format_result_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_sql_agent_format_result(*args, **kwargs)

def _assistant_sql_agent_links_from_result_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_sql_agent_links_from_result(*args, **kwargs)

def _assistant_try_handle_sql_agent_query_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_try_handle_sql_agent_query(*args, **kwargs)

def api_assistant_status_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_assistant_status(*args, **kwargs)

def api_assistant_get_session_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_assistant_get_session(*args, **kwargs)

def api_assistant_chat_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_assistant_chat(*args, **kwargs)
