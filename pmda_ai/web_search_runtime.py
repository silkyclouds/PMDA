"""Runtime-owned web search, AI search fallback, and result-normalization helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_extract_text_from_openai_response',
    '_assistant_extract_json_array',
    '_assistant_extract_json_object',
    '_normalize_web_results',
    '_ai_web_search_cache_key',
    '_ai_web_search_cache_get',
    '_ai_web_search_cache_lookup',
    '_ai_web_search_cache_set',
    '_ai_query_cache_get',
    '_ai_query_cache_set',
    '_ai_web_search_run_key',
    '_ai_web_search_mark_run_query_seen',
    '_ai_web_search_budget_allows',
    '_ai_web_search_available',
    '_ollama_web_search_enabled',
    '_ollama_web_search_context_lines',
    '_ollama_web_search_should_retry_complex',
    '_ollama_web_search_allowed_domains',
    '_ollama_web_search_prompt',
    '_ollama_web_search_response_schema',
    '_ollama_chat_json',
    '_duckduckgo_html_search_http',
    '_ollama_web_search_seed_hits',
    '_ollama_web_search_enrich_seed_rows',
    '_review_hit_domain',
    '_review_domain_matches',
    '_review_row_text',
    '_review_hit_has_signal',
    '_review_hit_is_metadata_only',
    '_review_filter_primary_hits',
    '_ollama_web_search_parse_rows',
    '_ollama_web_search',
    '_is_openai_web_search_unsupported_error',
    '_openai_web_search_model_candidates',
    '_openai_web_search_fallback',
    '_web_search_ai_fallback_enabled',
    '_web_search_provider_order',
    '_web_search_serper_http',
    '_web_search_serper',
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

def _extract_text_from_openai_response(resp: Any) -> str:
    """Best-effort text extractor compatible with OpenAI responses/chat SDK payloads."""
    try:
        txt = str(getattr(resp, "output_text", "") or "").strip()
        if txt:
            return txt
    except Exception:
        pass
    try:
        if isinstance(resp, dict):
            txt = str(resp.get("output_text") or "").strip()
            if txt:
                return txt
    except Exception:
        pass
    parts: list[str] = []
    try:
        output = getattr(resp, "output", None)
        if output is None and isinstance(resp, dict):
            output = resp.get("output")
        if isinstance(output, list):
            for item in output:
                content = getattr(item, "content", None)
                if content is None and isinstance(item, dict):
                    content = item.get("content")
                if not isinstance(content, list):
                    continue
                for block in content:
                    btype = str(getattr(block, "type", "") or (block.get("type") if isinstance(block, dict) else "")).strip().lower()
                    if btype not in {"output_text", "text"}:
                        continue
                    text_val = getattr(block, "text", None)
                    if text_val is None and isinstance(block, dict):
                        text_val = block.get("text")
                    if isinstance(text_val, str) and text_val.strip():
                        parts.append(text_val.strip())
    except Exception:
        pass
    return "\n".join(parts).strip()


def _assistant_extract_json_array(text: str) -> list[dict]:
    """Best-effort JSON array extractor for LLM outputs. Returns [] on parse failure."""
    raw = (text or "").strip()
    if not raw:
        return []
    raw = re.sub(r"^\s*```(?:json)?\s*", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"\s*```\s*$", "", raw).strip()

    def _try_parse_array(payload: str) -> list[dict]:
        s = (payload or "").strip()
        if not s:
            return []
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [x for x in arr if isinstance(x, dict)]
        except Exception:
            pass
        return []

    direct = _try_parse_array(raw)
    if direct:
        return direct

    start = raw.find("[")
    end = raw.rfind("]")
    if start >= 0 and end > start:
        return _try_parse_array(raw[start : end + 1])
    return []


def _assistant_extract_json_object(text: str) -> dict[str, Any]:
    """Best-effort JSON object extractor for LLM outputs. Returns {} on parse failure."""
    raw = (text or "").strip()
    if not raw:
        return {}
    raw = re.sub(r"^\s*```(?:json)?\s*", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"\s*```\s*$", "", raw).strip()

    def _try_parse_object(payload: str) -> dict[str, Any]:
        s = (payload or "").strip()
        if not s:
            return {}
        try:
            obj = json.loads(s)
            if isinstance(obj, dict):
                return {str(k): v for k, v in obj.items()}
        except Exception:
            pass
        return {}

    direct = _try_parse_object(raw)
    if direct:
        return direct

    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        return _try_parse_object(raw[start : end + 1])
    return {}


def _normalize_web_results(rows: list[dict], *, source: str, max_items: int = 10) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "").strip()
        link = str(row.get("link") or row.get("url") or "").strip()
        snippet = str(row.get("snippet") or row.get("summary") or row.get("description") or "").strip()
        if not (title or link or snippet):
            continue
        key = link.lower() if link else f"{title.lower()}|{snippet.lower()[:140]}"
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "title": title,
                "link": link,
                "snippet": snippet,
                "source": source,
            }
        )
        if len(out) >= max(1, int(max_items or 10)):
            break
    return out


def _ai_web_search_cache_key(query: str, num: int) -> str:
    q = re.sub(r"\s+", " ", str(query or "").strip().lower())
    return f"{q}|{max(1, min(20, int(num or 10)))}"


def _ai_web_search_cache_get(query: str, num: int) -> list[dict]:
    found, rows = _ai_web_search_cache_lookup(query, num)
    return rows if found else []


def _ai_web_search_cache_lookup(query: str, num: int) -> tuple[bool, list[dict]]:
    key = _ai_web_search_cache_key(query, num)
    now_ts = time.time()
    with _ai_web_search_cache_lock:
        row = _ai_web_search_cache.get(key)
        if not row:
            return (False, [])
        status = str(row.get("status") or "hit").strip().lower()
        ttl_default = (
            getattr(
                sys.modules[__name__],
                "AI_WEB_SEARCH_CACHE_NEG_TTL_SEC",
                AI_WEB_SEARCH_CACHE_NEG_TTL_SEC,
            )
            if status in {"miss", "error", "empty"}
            else getattr(
                sys.modules[__name__],
                "AI_WEB_SEARCH_CACHE_TTL_SEC",
                AI_WEB_SEARCH_CACHE_TTL_SEC,
            )
        )
        ttl = max(60, int(ttl_default or 60))
        created_at = float(row.get("created_at") or 0.0)
        if created_at <= 0 or (now_ts - created_at) > ttl:
            _ai_web_search_cache.pop(key, None)
            return (False, [])
        rows = row.get("results")
        if isinstance(rows, list):
            return (True, [dict(r) for r in rows if isinstance(r, dict)])
    return (False, [])


def _ai_web_search_cache_set(query: str, num: int, rows: list[dict], *, status: str = "") -> None:
    key = _ai_web_search_cache_key(query, num)
    max_entries = max(
        100,
        int(
            getattr(
                sys.modules[__name__],
                "AI_WEB_SEARCH_CACHE_MAX_ENTRIES",
                AI_WEB_SEARCH_CACHE_MAX_ENTRIES,
            )
            or AI_WEB_SEARCH_CACHE_MAX_ENTRIES
        ),
    )
    payload = [dict(r) for r in (rows or []) if isinstance(r, dict)]
    status_norm = str(status or "").strip().lower()
    if status_norm not in {"hit", "miss", "error"}:
        status_norm = "hit" if payload else "miss"
    with _ai_web_search_cache_lock:
        _ai_web_search_cache[key] = {"created_at": time.time(), "results": payload, "status": status_norm}
        if len(_ai_web_search_cache) > max_entries:
            overflow = max(1, len(_ai_web_search_cache) - max_entries)
            oldest = sorted(
                _ai_web_search_cache.items(),
                key=lambda item: float((item[1] or {}).get("created_at") or 0.0),
            )[:overflow]
            for old_key, _ in oldest:
                _ai_web_search_cache.pop(old_key, None)


def _ai_query_cache_get(scope: str, query: str, num: int) -> tuple[bool, list[dict], str]:
    cache_key = _ai_web_search_cache_key(query, num)
    now_ts = time.time()
    con = None
    try:
        con = _state_connect(timeout=5)
        cur = con.cursor()
        cur.execute(
            """
            SELECT status, results_json, expires_at
            FROM ai_query_cache
            WHERE cache_key = ? AND scope = ?
            LIMIT 1
            """,
            (cache_key, str(scope or "web_search")),
        )
        row = cur.fetchone()
        if not row:
            return (False, [], "")
        expires_at = float(row["expires_at"] or 0.0)
        if expires_at > 0 and expires_at < now_ts:
            cur.execute("DELETE FROM ai_query_cache WHERE cache_key = ? AND scope = ?", (cache_key, str(scope or "web_search")))
            con.commit()
            return (False, [], "")
        status = str(row["status"] or "miss").strip().lower()
        raw = row["results_json"]
        parsed: list[dict] = []
        try:
            data = json.loads(raw or "[]")
            if isinstance(data, list):
                parsed = [dict(x) for x in data if isinstance(x, dict)]
        except Exception:
            parsed = []
        try:
            cur.execute(
                "UPDATE ai_query_cache SET hit_count = COALESCE(hit_count, 0) + 1, updated_at = ? WHERE cache_key = ? AND scope = ?",
                (now_ts, cache_key, str(scope or "web_search")),
            )
            con.commit()
        except Exception:
            pass
        return (True, parsed, status)
    except Exception:
        return (False, [], "")
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass


def _ai_query_cache_set(scope: str, query: str, num: int, rows: list[dict], *, status: str = "hit", source: str = "") -> None:
    cache_key = _ai_web_search_cache_key(query, num)
    now_ts = time.time()
    status_norm = str(status or "").strip().lower()
    if status_norm not in {"hit", "miss", "error"}:
        status_norm = "hit" if rows else "miss"
    ttl_sec = (
        int(getattr(sys.modules[__name__], "AI_WEB_SEARCH_CACHE_NEG_TTL_SEC", AI_WEB_SEARCH_CACHE_NEG_TTL_SEC) or AI_WEB_SEARCH_CACHE_NEG_TTL_SEC)
        if status_norm in {"miss", "error"}
        else int(getattr(sys.modules[__name__], "AI_WEB_SEARCH_CACHE_TTL_SEC", AI_WEB_SEARCH_CACHE_TTL_SEC) or AI_WEB_SEARCH_CACHE_TTL_SEC)
    )
    expires_at = now_ts + max(60, ttl_sec)
    payload = _json_dumps_safe([dict(r) for r in (rows or []) if isinstance(r, dict)])
    con = None
    try:
        con = _state_connect(timeout=5)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO ai_query_cache
              (cache_key, scope, query_text, status, source, results_json, hit_count, created_at, updated_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
              scope = excluded.scope,
              query_text = excluded.query_text,
              status = excluded.status,
              source = excluded.source,
              results_json = excluded.results_json,
              updated_at = excluded.updated_at,
              expires_at = excluded.expires_at
            """,
            (
                cache_key,
                str(scope or "web_search"),
                str(query or "").strip(),
                status_norm,
                str(source or "").strip(),
                payload,
                now_ts,
                now_ts,
                expires_at,
            ),
        )
        con.commit()
    except Exception:
        pass
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass


def _ai_web_search_run_key() -> str:
    try:
        ctx = _ai_infer_runtime_context()
    except Exception:
        ctx = {}
    scan_id = _int_or_none(ctx.get("scan_id"))
    origin_scan_id = _int_or_none(ctx.get("origin_scan_id"))
    run_id = str(ctx.get("run_id") or "").strip()
    if scan_id and scan_id > 0:
        return f"scan:{int(scan_id)}"
    if origin_scan_id and origin_scan_id > 0:
        return f"scan:{int(origin_scan_id)}"
    if run_id:
        return f"run:{run_id}"
    return ""


def _ai_web_search_mark_run_query_seen(query: str, num: int) -> bool:
    run_key = _ai_web_search_run_key()
    if not run_key:
        return True
    query_key = _ai_web_search_cache_key(query, num)
    with _ai_web_search_run_seen_lock:
        seen = _ai_web_search_run_seen.setdefault(run_key, set())
        if query_key in seen:
            return False
        seen.add(query_key)
        if len(_ai_web_search_run_seen) > 128:
            # Keep memory bounded for long-lived workers.
            oldest_key = next(iter(_ai_web_search_run_seen.keys()), "")
            if oldest_key:
                _ai_web_search_run_seen.pop(oldest_key, None)
    return True


def _ai_web_search_budget_allows() -> tuple[bool, str]:
    return (True, "")


def _ai_web_search_available(*, user_id: int | None = None) -> bool:
    if not bool(getattr(sys.modules[__name__], "USE_AI_WEB_SEARCH_FALLBACK", False)):
        return False
    ok, provider_effective, _auth_mode, _reason = _resolve_ai_runtime_availability(
        analysis_type="web_search",
        requested_provider="openai",
        user_id=user_id,
    )
    if not ok:
        return False
    return str(provider_effective or "").strip().lower() in {"openai", "openai-api", "openai-codex"}


def _ollama_web_search_enabled(*, allow_ai_fallback: bool = True) -> bool:
    if not allow_ai_fallback:
        return False
    provider = _normalize_web_search_provider(str(getattr(sys.modules[__name__], "WEB_SEARCH_PROVIDER", "auto") or "auto"))
    if not bool(getattr(sys.modules[__name__], "USE_AI_WEB_SEARCH_FALLBACK", False)):
        if provider not in {"ollama", "ai_only"}:
            return False
    if not _ollama_service_configured():
        return False
    return True


def _ollama_web_search_context_lines(context: dict[str, Any] | None) -> list[str]:
    if not isinstance(context, dict):
        return []
    lines: list[str] = []
    for key in (
        "artist",
        "album",
        "album_title",
        "year",
        "metadata_source",
        "musicbrainz_release_group_id",
        "discogs_release_id",
        "lastfm_album_mbid",
        "bandcamp_album_url",
        "strict_match_verified",
        "query_kind",
        "local_track_preview",
    ):
        value = context.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        lines.append(f"- {key}: {text}")
    return lines


def _ollama_web_search_should_retry_complex(
    *,
    accepted_rows: list[dict[str, Any]],
    confidence: float | None,
    raw_payload: dict[str, Any] | None,
) -> bool:
    conf = float(confidence or 0.0)
    if not accepted_rows:
        return True
    if conf < _OLLAMA_WEB_SEARCH_MIN_CONFIDENCE:
        return True
    rejected = raw_payload.get("sources") if isinstance(raw_payload, dict) else []
    if isinstance(rejected, list):
        for row in rejected[:6]:
            if not isinstance(row, dict):
                continue
            accepted = row.get("accepted")
            if accepted is True:
                continue
            reason = str(row.get("reason") or "").strip().lower()
            if any(tok in reason for tok in ("different artist", "wrong album", "same title", "ambiguous", "conflict")):
                return True
    return False


def _ollama_web_search_allowed_domains(context: dict[str, Any] | None = None) -> tuple[str, ...]:
    query_kind = str((context or {}).get("query_kind") or "").strip().lower()
    if query_kind == "album_review":
        return ()
    return _OLLAMA_WEB_SEARCH_ALLOWED_DOMAINS


def _ollama_web_search_prompt(
    query: str,
    *,
    max_items: int,
    context: dict[str, Any] | None = None,
    seed_rows: list[dict[str, Any]] | None = None,
) -> str:
    context_lines = _ollama_web_search_context_lines(context)
    context_block = "\n".join(context_lines) if context_lines else "- none"
    query_kind = str((context or {}).get("query_kind") or "").strip().lower()
    search_rows = seed_rows or []
    search_lines: list[str] = []
    for idx, row in enumerate(search_rows[: max(1, int(max_items or 10) * 2)], start=1):
        if not isinstance(row, dict):
            continue
        search_lines.append(f"[{idx}] title: {str(row.get('title') or '').strip()}")
        search_lines.append(f"[{idx}] url: {str(row.get('link') or row.get('url') or '').strip()}")
        search_lines.append(f"[{idx}] snippet: {str(row.get('snippet') or '').strip()}")
        page_title = str(row.get("page_title") or "").strip()
        page_excerpt = str(row.get("page_excerpt") or "").strip()
        fetch_error = str(row.get("fetch_error") or "").strip()
        if page_title:
            search_lines.append(f"[{idx}] page_title: {page_title}")
        if page_excerpt:
            search_lines.append(f"[{idx}] page_excerpt: {page_excerpt}")
        if fetch_error:
            search_lines.append(f"[{idx}] fetch_error: {fetch_error}")
    search_block = "\n".join(search_lines) if search_lines else "- none"
    if query_kind == "album_review":
        task_block = (
            "You are validating web search results that were already retrieved for a music album review query.\n"
            "Keep the original result order.\n"
            "Accept only pages that look like real reviews of the correct music album by the correct artist.\n"
            "Reject metadata pages, discographies, store pages, lyrics, forums, tracklists, and wrong artist/album matches.\n"
        )
    else:
        domains = ", ".join(_ollama_web_search_allowed_domains(context)[:8])
        task_block = (
            "You are validating web search results that were already retrieved for a music query.\n"
            f"Prefer music-relevant results when helpful: {domains}.\n"
            "Reject wrong artist/album/title matches.\n"
        )
    return (
        task_block
        + "Use only the provided search results.\n"
        + "Do not invent URLs.\n"
        + "When you accept a source, copy its title, link, and snippet from the candidate list.\n"
        + "Return JSON only:\n"
        + '{"query":"...","verdict":"strong|weak|none","confidence":0.0,"decision_reason":"...",'
        + '"sources":[{"title":"...","link":"https://...","snippet":"...","accepted":true,"reason":"..."}]}\n'
        + f"Keep at most {max_items} sources.\n"
        + f"Query: {query}\n"
        + f"Search results:\n{search_block}\n"
        + f"Context:\n{context_block}\n"
    )


def _ollama_web_search_response_schema(max_items: int) -> dict[str, Any]:
    item_schema = {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "link": {"type": "string"},
            "snippet": {"type": "string"},
            "accepted": {"type": "boolean"},
            "reason": {"type": "string"},
        },
        "required": ["title", "link", "snippet", "accepted", "reason"],
    }
    return {
        "type": "object",
        "properties": {
            "query": {"type": "string"},
            "verdict": {"type": "string", "enum": ["strong", "weak", "none"]},
            "confidence": {"type": "number"},
            "decision_reason": {"type": "string"},
            "sources": {
                "type": "array",
                "items": item_schema,
                "maxItems": max(1, int(max_items or 10)),
            },
        },
        "required": ["query", "verdict", "confidence", "decision_reason", "sources"],
    }


def _ollama_chat_json(
    *,
    model_name: str,
    system_msg: str,
    user_msg: str,
    timeout_sec: int,
    max_tokens: int | None = None,
    format_schema: dict[str, Any] | str = "json",
    analysis_type: str = "",
    keep_warm: bool = True,
) -> dict[str, Any]:
    host = str(getattr(sys.modules[__name__], "OLLAMA_URL", "") or "").strip().rstrip("/")
    if not host:
        raise RuntimeError("Ollama URL not configured")
    model = str(model_name or "").strip()
    if keep_warm and model:
        _ollama_prewarm_model(model, analysis_type=analysis_type, force=False)
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": str(system_msg or "").strip()},
            {"role": "user", "content": str(user_msg or "").strip()},
        ],
        "think": False,
        "stream": False,
        "keep_alive": _ollama_keep_alive_for_analysis(
            analysis_type=analysis_type,
            model_name=model,
        ),
        "options": {
            "temperature": 0,
            "num_predict": max(64, min(512, int(max_tokens or _OLLAMA_WEB_SEARCH_MAX_OUTPUT_TOKENS))),
        },
    }
    if format_schema:
        payload["format"] = format_schema
    resp = requests.post(
        f"{host}/api/chat",
        json=payload,
        timeout=max(10, int(timeout_sec)),
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Ollama API error: {resp.status_code} - {str(resp.text or '').strip()[:240]}")
    out = resp.json()
    if not isinstance(out, dict):
        raise RuntimeError("Ollama API returned a non-object response")
    return out


def _duckduckgo_html_search_http(query: str, num: int = 10) -> tuple[str, list[dict]]:
    q = str(query or "").strip()
    if not q:
        return ("miss", [])
    limit = max(1, min(20, int(num or 10)))
    try:
        resp = requests.post(
            "https://html.duckduckgo.com/html/",
            data={"q": q},
            headers={"User-Agent": "Mozilla/5.0 (PMDA web search)"},
            timeout=max(4, min(int(_DUCKDUCKGO_SEARCH_TIMEOUT_SEC), 20)),
        )
        if resp.status_code != 200:
            logging.warning("[DuckDuckGo] HTTP %s for query=%r", resp.status_code, q)
            return ("error", [])
        text = str(resp.text or "")
        pattern = re.compile(
            r'(?is)<a[^>]+class="result__a"[^>]+href="(?P<link>[^"]+)"[^>]*>(?P<title>.*?)</a>(?P<body>.*?)(?=<a[^>]+class="result__a"|<div[^>]+class="nav-link"|$)'
        )
        rows: list[dict[str, Any]] = []
        for match in pattern.finditer(text):
            link = html.unescape(str(match.group("link") or "").strip())
            title = _strip_html_text(html.unescape(str(match.group("title") or "").strip()))
            body = str(match.group("body") or "")
            snippet_match = re.search(r'(?is)<(?:a|div)[^>]+class="result__snippet"[^>]*>(.*?)</(?:a|div)>', body)
            snippet = _strip_html_text(html.unescape(str(snippet_match.group(1) or "").strip())) if snippet_match else ""
            if link.startswith("//"):
                link = f"https:{link}"
            redirect_match = re.search(r"[?&]uddg=([^&]+)", link)
            if redirect_match:
                try:
                    link = unquote(str(redirect_match.group(1) or "").strip())
                except Exception:
                    pass
            rows.append({"title": title, "link": link, "snippet": snippet})
            if len(rows) >= limit:
                break
        normalized = _normalize_web_results(rows, source="duckduckgo", max_items=limit)
        return ("hit" if normalized else "miss", normalized[:limit])
    except Exception as exc:
        logging.debug("[DuckDuckGo] Request failed for query=%r: %s", q, exc)
        return ("error", [])


def _ollama_web_search_seed_hits(
    query: str,
    *,
    max_items: int,
    context: dict[str, Any] | None = None,
) -> tuple[str, str, list[dict[str, Any]]]:
    q = str(query or "").strip()
    if not q:
        return ("none", "miss", [])
    limit = max(1, min(20, max(int(max_items or 10), _OLLAMA_WEB_SEARCH_SEED_MAX_RESULTS)))
    serper_ok, _serper_msg = _serper_runtime_status()
    if serper_ok:
        status, rows = _web_search_serper_http(q, num=limit)
        if rows:
            return ("serper", status, rows[:limit])
    status, rows = _duckduckgo_html_search_http(q, num=limit)
    return ("duckduckgo", status, rows[:limit])


def _ollama_web_search_enrich_seed_rows(
    rows: list[dict[str, Any]],
    *,
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return [dict(row) for row in (rows or []) if isinstance(row, dict)]


def _review_hit_domain(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return ""
    link = str(row.get("link") or row.get("url") or "").strip()
    if not link:
        return ""
    try:
        host = urlparse(link).netloc.lower().strip()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _review_domain_matches(host: str, domain: str) -> bool:
    h = str(host or "").strip().lower()
    d = str(domain or "").strip().lower()
    if not h or not d:
        return False
    return h == d or h.endswith(f".{d}")


def _review_row_text(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return ""
    title = str(row.get("title") or "").strip()
    snippet = str(row.get("snippet") or "").strip()
    link = str(row.get("link") or row.get("url") or "").strip()
    return " ".join(part for part in (title, snippet, link) if part).strip().lower()


def _review_hit_has_signal(row: dict[str, Any] | None) -> bool:
    text = _review_row_text(row)
    if not text:
        return False
    if any(marker in text for marker in _REVIEW_SIGNAL_MARKERS):
        return True
    snippet = str((row or {}).get("snippet") or "").strip()
    title = str((row or {}).get("title") or "").strip().lower()
    return bool(len(snippet) >= 100 and "review" in title and not any(marker in text for marker in _REVIEW_METADATA_ONLY_MARKERS))


def _review_hit_is_metadata_only(row: dict[str, Any] | None) -> bool:
    text = _review_row_text(row)
    if not text:
        return False
    return any(marker in text for marker in _REVIEW_METADATA_ONLY_MARKERS) and not _review_hit_has_signal(row)


def _review_filter_primary_hits(
    artist: str,
    album: str,
    hits: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in hits or []:
        if not isinstance(row, dict):
            continue
        row_copy = dict(row)
        identity_blob = f"{str(row_copy.get('title') or '').strip()} {str(row_copy.get('snippet') or '').strip()}".strip()
        artist_hit = _text_mentions_identity_phrase(artist, identity_blob)
        album_hit = _text_mentions_identity_phrase(album, identity_blob)
        if not (artist_hit or album_hit):
            row_copy["reason"] = "identity_mismatch"
            rejected.append(row_copy)
            continue
        if _review_hit_is_metadata_only(row_copy):
            row_copy["reason"] = "metadata_only_review_source"
            rejected.append(row_copy)
            continue
        if not _review_hit_has_signal(row_copy):
            row_copy["reason"] = "no_review_signal"
            rejected.append(row_copy)
            continue
        accepted.append(row_copy)
    return accepted, rejected


def _ollama_web_search_parse_rows(
    payload: dict[str, Any],
    *,
    max_items: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], float | None, str]:
    confidence = _safe_bounded_float(payload.get("confidence"), minimum=0.0, maximum=1.0)
    decision_reason = str(payload.get("decision_reason") or payload.get("short_reason") or payload.get("reason") or "").strip()
    raw_sources = payload.get("sources")
    if not isinstance(raw_sources, list):
        raw_sources = payload.get("results")
    if not isinstance(raw_sources, list):
        raw_sources = payload.get("top_sources")
    if not isinstance(raw_sources, list):
        album_reviews = payload.get("album_reviews")
        if isinstance(album_reviews, list):
            raw_sources = []
            for item in album_reviews[:max_items]:
                if not isinstance(item, dict):
                    continue
                raw_sources.append(
                    {
                        "title": str(item.get("title") or "").strip(),
                        "snippet": str(item.get("review_text") or item.get("summary") or item.get("description") or "").strip(),
                        "accepted": True,
                        "reason": "model_summary_without_links",
                    }
                )
            if raw_sources and confidence is None:
                confidence = 0.56
            if raw_sources and not decision_reason:
                decision_reason = "model returned review summaries without direct source links"
    normalized_input: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for item in raw_sources or []:
        if isinstance(item, str):
            row = {"title": "", "link": item, "snippet": "", "accepted": True, "reason": ""}
        elif isinstance(item, dict):
            row = {
                "title": str(item.get("title") or "").strip(),
                "link": str(item.get("link") or item.get("url") or "").strip(),
                "snippet": str(item.get("snippet") or item.get("summary") or item.get("description") or "").strip(),
                "accepted": item.get("accepted"),
                "reason": str(item.get("reason") or "").strip(),
            }
        else:
            continue
        link = str(row.get("link") or "").strip()
        reason_tag = str(row.get("reason") or "").strip()
        allow_linkless_summary = bool(reason_tag == "model_summary_without_links" and str(row.get("snippet") or "").strip())
        if not allow_linkless_summary and not (link.startswith("http://") or link.startswith("https://")):
            row["accepted"] = False
            row["reason"] = str(row.get("reason") or "invalid_or_missing_url").strip()
        accepted = row.get("accepted")
        if accepted is None:
            accepted = not bool(str(row.get("reason") or "").strip())
            row["accepted"] = accepted
        if accepted:
            normalized_input.append(row)
        else:
            rejected.append(row)
    accepted_rows = _normalize_web_results(normalized_input, source="ollama_web_search", max_items=max_items)
    return accepted_rows, rejected[:max_items], confidence, decision_reason


def _ollama_web_search(
    query: str,
    num: int = 10,
    *,
    reason: str = "",
    context: dict[str, Any] | None = None,
    analysis_type: str = "web_search",
) -> tuple[str, list[dict], dict[str, Any]]:
    q = str(query or "").strip()
    if not q:
        return ("miss", [], {"reason": "empty_query"})
    if not _ollama_web_search_enabled(allow_ai_fallback=True):
        return ("miss", [], {"reason": "ollama_web_search_disabled"})
    base_model = _ollama_model_configured()
    complex_model = _ollama_complex_model_configured()
    model_sequence = [base_model]
    if complex_model and complex_model != base_model and _ollama_model_available(complex_model):
        model_sequence.append(complex_model)
    max_items = max(1, min(10, int(num or 10)))
    reason_tag = str(reason or "").strip() or "ollama_web_search"
    last_meta: dict[str, Any] = {"reason": reason_tag}
    query_kind = str((context or {}).get("query_kind") or "").strip().lower()
    seed_source, seed_status, seed_rows = _ollama_web_search_seed_hits(
        q,
        max_items=max_items,
        context=context,
    )
    if not seed_rows:
        return (
            "miss",
            [],
            {
                "reason": reason_tag,
                "seed_source": seed_source,
                "seed_status": seed_status,
            },
        )
    if query_kind == "album_review":
        logging.info(
            "[WEB][OLLAMA] review_seed query=%r seed=%s count=%d reason=direct_review_validation",
            q,
            seed_source,
            min(len(seed_rows), max_items),
        )
        return (
            "hit",
            seed_rows[:max_items],
            {
                "reason": reason_tag,
                "seed_source": seed_source,
                "seed_status": seed_status,
                "seed_count": len(seed_rows),
                "fallback": "direct_seed_rows",
            },
        )
    enriched_seed_rows = _ollama_web_search_enrich_seed_rows(seed_rows, context=context)
    prompt = _ollama_web_search_prompt(
        q,
        max_items=max_items,
        context=context,
        seed_rows=enriched_seed_rows,
    )
    system_msg = (
        "You return structured JSON only.\n"
        "Use only the provided search results.\n"
        "Do not invent URLs.\n"
        "Keep accepted=true only for results that plausibly match the query and context.\n"
    )
    for idx, model_name in enumerate(model_sequence):
        timeout_sec = _OLLAMA_WEB_SEARCH_COMPLEX_TIMEOUT_SEC if idx > 0 else _OLLAMA_WEB_SEARCH_TIMEOUT_SEC
        started_at = time.time()
        status = "failed"
        error_msg = ""
        response_obj: dict[str, Any] | None = None
        payload_obj: dict[str, Any] | None = None
        try:
            response_obj = _ollama_chat_json(
                model_name=model_name,
                system_msg=system_msg,
                user_msg=prompt,
                timeout_sec=max(10, int(timeout_sec)),
                format_schema=_ollama_web_search_response_schema(max_items),
                analysis_type="web_search_review",
            )
            raw_content = str(((response_obj.get("message") or {}) if isinstance(response_obj, dict) else {}).get("content") or "").strip()
            payload_obj = _assistant_extract_json_object(raw_content)
            if not payload_obj:
                raise RuntimeError("Ollama web search returned no JSON object")
            accepted_rows, rejected_rows, confidence, decision_reason = _ollama_web_search_parse_rows(
                payload_obj,
                max_items=max_items,
            )
            meta = {
                "model": model_name,
                "confidence": confidence,
                "decision_reason": decision_reason,
                "reason": reason_tag,
                "rejected_count": len(rejected_rows),
                "verdict": str(payload_obj.get("verdict") or "").strip().lower(),
                "seed_source": seed_source,
                "seed_count": len(seed_rows),
            }
            for row in accepted_rows[:max_items]:
                logging.info(
                    "[WEB][OLLAMA] source_accept query=%r model=%s seed=%s confidence=%.2f link=%s reason=%s",
                    q,
                    model_name,
                    seed_source,
                    float(confidence or 0.0),
                    str(row.get("link") or "").strip(),
                    decision_reason or "accepted",
                )
            for row in rejected_rows[:max_items]:
                logging.info(
                    "[WEB][OLLAMA] source_reject query=%r model=%s seed=%s confidence=%.2f link=%s reason=%s",
                    q,
                    model_name,
                    seed_source,
                    float(confidence or 0.0),
                    str(row.get("link") or "").strip(),
                    str(row.get("reason") or "").strip() or "rejected_by_model",
                )
            status = "completed"
            last_meta = dict(meta)
            if callable(globals().get("record_ai_usage")):
                globals()["record_ai_usage"](
                    provider="ollama",
                    model=model_name,
                    endpoint_kind="web_search",
                    analysis_type=analysis_type,
                    started_at=started_at,
                    status=status,
                    response_obj=payload_obj,
                    image_inputs=0,
                    error="",
                    metadata={
                        "query_hash": hashlib.sha1(q.encode("utf-8", errors="ignore")).hexdigest(),
                        "query_len": len(q),
                        "accepted_count": len(accepted_rows),
                        "rejected_count": len(rejected_rows),
                        "confidence": float(confidence or 0.0),
                        "decision_reason": decision_reason,
                        "reason": reason_tag,
                        "seed_source": seed_source,
                        "seed_count": len(seed_rows),
                    },
                )
            should_retry_complex = idx < (len(model_sequence) - 1) and _ollama_web_search_should_retry_complex(
                accepted_rows=accepted_rows,
                confidence=confidence,
                raw_payload=payload_obj,
            )
            if should_retry_complex:
                logging.info(
                    "[WEB][OLLAMA] escalation query=%r from=%s to=%s seed=%s accepted=%d confidence=%.2f reason=%s",
                    q,
                    model_name,
                    model_sequence[idx + 1],
                    seed_source,
                    len(accepted_rows),
                    float(confidence or 0.0),
                    decision_reason or "weak_or_empty_result",
                )
                continue
            if accepted_rows and float(confidence or 0.0) >= _OLLAMA_WEB_SEARCH_MIN_CONFIDENCE:
                logging.info(
                    "[WEB][OLLAMA] decision query=%r model=%s seed=%s accepted=%d confidence=%.2f reason=%s",
                    q,
                    model_name,
                    seed_source,
                    len(accepted_rows),
                    float(confidence or 0.0),
                    decision_reason or "accepted",
                )
                return ("hit", accepted_rows[:max_items], meta)
            if accepted_rows:
                logging.info(
                    "[WEB][OLLAMA] decision query=%r model=%s seed=%s accepted=%d confidence=%.2f reason=%s",
                    q,
                    model_name,
                    seed_source,
                    len(accepted_rows),
                    float(confidence or 0.0),
                    decision_reason or "accepted_with_low_confidence",
                )
                return ("hit", accepted_rows[:max_items], meta)
        except Exception as exc:
            error_msg = str(exc or "").strip()
            last_meta = {
                "model": model_name,
                "reason": reason_tag,
                "error": error_msg,
                "seed_source": seed_source,
                "seed_status": seed_status,
                "seed_count": len(seed_rows),
            }
            logging.warning(
                "[WEB][OLLAMA] failed query=%r model=%s seed=%s error=%s",
                q,
                model_name,
                seed_source,
                error_msg,
            )
            if callable(globals().get("record_ai_usage")):
                globals()["record_ai_usage"](
                    provider="ollama",
                    model=model_name,
                    endpoint_kind="web_search",
                    analysis_type=analysis_type,
                    started_at=started_at,
                    status=status,
                    response_obj=payload_obj,
                    image_inputs=0,
                    error=error_msg,
                    metadata={
                        "query_hash": hashlib.sha1(q.encode("utf-8", errors="ignore")).hexdigest(),
                        "query_len": len(q),
                        "reason": reason_tag,
                        "seed_source": seed_source,
                        "seed_status": seed_status,
                        "seed_count": len(seed_rows),
                    },
                )
            if idx < (len(model_sequence) - 1):
                logging.info(
                    "[WEB][OLLAMA] escalation query=%r from=%s to=%s seed=%s reason=%s",
                    q,
                    model_name,
                    model_sequence[idx + 1],
                    seed_source,
                    error_msg or "base_model_failed",
                )
                continue
        break
    if query_kind == "album_review" and seed_rows:
        logging.info(
            "[WEB][OLLAMA] returning raw review candidates query=%r seed=%s count=%d because structured selection failed",
            q,
            seed_source,
            min(len(seed_rows), max_items),
        )
        last_meta = {
            "reason": reason_tag,
            "seed_source": seed_source,
            "seed_status": seed_status,
            "seed_count": len(seed_rows),
            "fallback": "raw_seed_rows",
        }
        return ("hit", seed_rows[:max_items], last_meta)
    return ("miss", [], last_meta)


def _is_openai_web_search_unsupported_error(message: str) -> bool:
    text = str(message or "").strip().lower()
    if not text:
        return False
    markers = (
        "web search unavailable",
        "web_search",
        "web-search",
        "does not support tools",
        "unsupported parameter: 'tools'",
        "tool is not supported",
        "unsupported parameter: 'temperature'",
        "parameter 'temperature' is not supported",
    )
    return any(m in text for m in markers)


def _openai_web_search_model_candidates(primary_model: str) -> list[str]:
    candidates: list[str] = []

    def _add(model_name: str) -> None:
        name = str(model_name or "").strip()
        if not name:
            return
        if name not in candidates:
            candidates.append(name)

    # Optional explicit model just for web-search fallback.
    try:
        _add(str(_get_config_from_db("OPENAI_WEB_SEARCH_MODEL", "") or ""))
    except Exception:
        pass
    _add(primary_model)
    # Known models that generally expose web search tooling.
    _add("gpt-4o-mini")
    _add("gpt-4.1-mini")
    _add("gpt-4o")
    return candidates


def _openai_web_search_fallback(query: str, num: int = 10, *, reason: str = "") -> list[dict]:
    """
    Fallback web search using OpenAI native web-search tool when Serper is unavailable.
    Returns list of {"title","link","snippet","source"}.
    """
    if _scan_ai_policy_for_runtime() == "local_only":
        return []
    if not _web_search_ai_fallback_enabled(allow_ai_fallback=True):
        return []
    user_id = _current_user_id_or_zero()
    if not _ai_web_search_available(user_id=user_id):
        return []
    q = str(query or "").strip()
    if not q:
        return []
    provider_for_usage = _resolve_provider_for_runtime(
        requested_provider="openai",
        analysis_type="web_search",
        user_id=user_id,
    )
    provider_lower = str(provider_for_usage or "").strip().lower()
    if provider_lower not in {"openai", "openai-api", "openai-codex"}:
        return []
    if provider_lower == "openai-codex":
        started_at = time.time()
        response_obj: Any = None
        status = "failed"
        error_msg = ""
        auth_mode_for_usage = "oauth"
        guard_allowed = True
        guard_reason = ""
        guard_meta: dict[str, Any] = {}
        max_items = max(1, int(num or 10))
        prompt = (
            f"Search the web for: {q}\n"
            f"Return ONLY a JSON array (max {max_items} items).\n"
            "Each item must have: title, link, snippet.\n"
            "Use direct source URLs and concise snippets."
        )
        try:
            guard_allowed, guard_reason, guard_meta = _ai_guardrail_precheck(
                provider=provider_for_usage,
                model="codex",
                endpoint_kind="web_search",
                analysis_type="web_search",
                requested_tokens=int(max(120, 80 + (max_items * 60))),
            )
            if not guard_allowed:
                raise RuntimeError(f"AI guardrail blocked call: {guard_reason}")
            raw_text, response_obj = _run_openai_codex_exec(
                system_msg="Use live web search and return only the requested JSON.",
                user_msg=prompt,
                analysis_type="web_search",
                request_timeout_sec=_openai_request_timeout_seconds(),
                web_search=True,
            )
            rows = _assistant_extract_json_array(raw_text)
            normalized = _normalize_web_results(rows, source=str(provider_for_usage or "openai-codex"), max_items=max_items)
            status = "completed"
            if normalized:
                logging.info(
                    "[WEB][AI] Codex web-search fallback used (%s, provider=%s, auth=%s) query=%r results=%d",
                    str(reason or "codex_web_search"),
                    provider_for_usage,
                    auth_mode_for_usage,
                    q,
                    len(normalized),
                )
                return normalized[:max_items]
        except Exception as e:
            error_msg = str(e)
            logging.warning(
                "[WEB][AI] Codex web-search fallback failed (%s) query=%r: %s",
                str(reason or "codex_web_search"),
                q,
                e,
            )
        finally:
            recorder = globals().get("record_ai_usage")
            if callable(recorder):
                recorder(
                    provider=provider_for_usage,
                    model="codex",
                    endpoint_kind="web_search",
                    analysis_type="web_search",
                    started_at=started_at,
                    status=status,
                    response_obj=response_obj,
                    image_inputs=1 if response_obj is not None else 0,
                    error=error_msg,
                    metadata={
                        "query_hash": hashlib.sha1(q.encode("utf-8", errors="ignore")).hexdigest(),
                        "query_len": len(q),
                        "reason": str(reason or "codex_web_search"),
                        "max_items": int(max_items),
                        "auth_mode": auth_mode_for_usage,
                        "guardrail_blocked": bool(not guard_allowed),
                        "guardrail_reason": guard_reason if not guard_allowed else "",
                        **(guard_meta or {}),
                    },
                )
        return []
    client_to_use, auth_mode_for_usage, openai_runtime_reason = _resolve_openai_client_for_runtime(provider_for_usage, user_id)
    if not client_to_use:
        if openai_runtime_reason:
            logging.warning(
                "[WEB][AI] OpenAI web-search fallback unavailable (%s, provider=%s): %s",
                usage_reason,
                provider_for_usage,
                openai_runtime_reason,
            )
        return []
    base_model = (
        getattr(sys.modules[__name__], "RESOLVED_MODEL", None)
        or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4o-mini")
    )
    model_candidates = _openai_web_search_model_candidates(str(base_model or ""))
    model_used = str(model_candidates[0] if model_candidates else (base_model or "gpt-4o-mini"))
    prompt = (
        f"Search the web for: {q}\n"
        f"Return ONLY a JSON array (max {max(1, int(num or 10))} items).\n"
        "Each item must have: title, link, snippet.\n"
        "Use direct source URLs and concise snippets."
    )
    started_at = time.time()
    response_obj: Any = None
    status = "failed"
    error_msg = ""
    auth_mode_for_usage = _provider_auth_mode(provider_for_usage)
    guard_allowed = True
    guard_reason = ""
    guard_meta: dict[str, Any] = {}
    usage_reason = str(reason or "serper_unavailable").strip() or "serper_unavailable"
    allow_call, deny_reason = _ai_web_search_budget_allows()
    if not allow_call:
        logging.warning(
            "[WEB][AI] OpenAI web-search skipped (%s) query=%r",
            deny_reason,
            q,
        )
        return []
    max_items = max(1, int(num or 10))
    max_output_tokens = max(
        120,
        min(
            int(getattr(sys.modules[__name__], "AI_WEB_SEARCH_MAX_OUTPUT_TOKENS", AI_WEB_SEARCH_MAX_OUTPUT_TOKENS) or AI_WEB_SEARCH_MAX_OUTPUT_TOKENS),
            int(80 + (max_items * 60)),
        ),
    )
    try:
        for idx, model_try in enumerate(model_candidates):
            model_used = str(model_try or "").strip() or model_used
            guard_allowed, guard_reason, guard_meta = _ai_guardrail_precheck(
                provider=provider_for_usage,
                model=model_used,
                endpoint_kind="web_search",
                analysis_type="web_search",
                requested_tokens=int(max_output_tokens),
            )
            if not guard_allowed:
                raise RuntimeError(f"AI guardrail blocked call: {guard_reason}")
            req = {
                "model": model_used,
                "tools": [{"type": "web_search_preview"}],
                "input": prompt,
                "max_output_tokens": max_output_tokens,
                "timeout": _openai_request_timeout_seconds(),
            }
            try:
                resp = client_to_use.responses.create(**req)
            except Exception as call_err:
                call_error = str(call_err or "").strip()
                if idx < (len(model_candidates) - 1) and _is_openai_web_search_unsupported_error(call_error):
                    logging.info(
                        "[WEB][AI] web-search tool unsupported on model=%s (provider=%s auth=%s); retrying with next candidate",
                        model_used,
                        provider_for_usage,
                        auth_mode_for_usage,
                    )
                    continue
                raise

            response_obj = resp
            raw_text = _extract_text_from_openai_response(resp)
            rows = _assistant_extract_json_array(raw_text)
            normalized = _normalize_web_results(rows, source=str(provider_for_usage or "openai-api"), max_items=max(1, int(num or 10)))
            status = "completed"
            if normalized:
                logging.info(
                    "[WEB][AI] OpenAI web-search fallback used (%s, provider=%s, auth=%s, model=%s) query=%r results=%d",
                    usage_reason,
                    provider_for_usage,
                    auth_mode_for_usage,
                    model_used,
                    q,
                    len(normalized),
                )
                return normalized[: max(1, int(num or 10))]
    except Exception as e:
        error_msg = str(e)
        if provider_lower in {"openai", "openai-api"} and _openai_error_allows_codex_fallback(e):
            try:
                uid = _current_user_id_or_zero()
                if _openai_codex_runtime_available(uid, require_token=True):
                    logging.warning(
                        "[WEB][AI] OpenAI API web-search failed; retrying with Codex OAuth (%s) query=%r: %s",
                        usage_reason,
                        q,
                        e,
                    )
                    raw_text, response_obj = _run_openai_codex_exec(
                        system_msg="Use live web search and return only the requested JSON.",
                        user_msg=prompt,
                        analysis_type="web_search",
                        request_timeout_sec=_openai_request_timeout_seconds(),
                        web_search=True,
                    )
                    rows = _assistant_extract_json_array(raw_text)
                    normalized = _normalize_web_results(rows, source=str(provider_for_usage or "openai-codex"), max_items=max_items)
                    provider_for_usage = "openai-codex"
                    auth_mode_for_usage = "oauth"
                    if normalized:
                        error_msg = ""
                        status = "completed"
                        logging.info(
                            "[WEB][AI] Codex web-search fallback used after API failure (%s) query=%r results=%d",
                            usage_reason,
                            q,
                            len(normalized),
                        )
                        return normalized[:max_items]
            except Exception as codex_exc:
                error_msg = f"{error_msg} | codex fallback: {codex_exc}"
        logging.warning(
            "[WEB][AI] OpenAI web-search fallback failed (%s) query=%r: %s",
            usage_reason,
            q,
            e,
        )
    finally:
        recorder = globals().get("record_ai_usage")
        if callable(recorder):
            web_search_tool_calls = 1 if response_obj is not None else 0
            recorder(
                provider=provider_for_usage,
                model=model_used,
                endpoint_kind="web_search",
                analysis_type="web_search",
                started_at=started_at,
                status=status,
                response_obj=response_obj,
                image_inputs=web_search_tool_calls,
                error=error_msg,
                metadata={
                    "query_hash": hashlib.sha1(q.encode("utf-8", errors="ignore")).hexdigest(),
                    "query_len": len(q),
                    "reason": usage_reason,
                    "max_items": int(max_items),
                    "max_output_tokens": int(max_output_tokens),
                    "web_search_tool_calls": int(web_search_tool_calls),
                    "auth_mode": auth_mode_for_usage,
                    "guardrail_blocked": bool(not guard_allowed),
                    "guardrail_reason": guard_reason if not guard_allowed else "",
                    **(guard_meta or {}),
                },
            )
    return []


def _web_search_ai_fallback_enabled(*, allow_ai_fallback: bool = True) -> bool:
    if not allow_ai_fallback:
        return False
    if _scan_ai_policy_for_runtime() == "local_only":
        return False
    if not bool(getattr(sys.modules[__name__], "USE_AI_WEB_SEARCH_FALLBACK", False)):
        provider = _normalize_web_search_provider(str(getattr(sys.modules[__name__], "WEB_SEARCH_PROVIDER", "auto") or "auto"))
        if provider not in {"ai_only"}:
            return False
    if _scan_ai_policy_for_runtime() == "paid_only":
        return True
    usage_level = _normalize_ai_usage_level(str(getattr(sys.modules[__name__], "AI_USAGE_LEVEL", "auto") or "auto"))
    provider = _normalize_web_search_provider(str(getattr(sys.modules[__name__], "WEB_SEARCH_PROVIDER", "auto") or "auto"))
    if provider in {"ollama", "ai_only"}:
        return False
    return usage_level == "aggressive"


def _web_search_provider_order() -> list[str]:
    provider = _normalize_web_search_provider(str(getattr(sys.modules[__name__], "WEB_SEARCH_PROVIDER", "auto") or "auto"))
    policy = _scan_ai_policy_for_runtime()
    if provider == "disabled":
        return []
    if provider in {"ai_only", "ollama"}:
        return []
    serper_ok, _serper_msg = _serper_runtime_status()
    if provider == "serper":
        if policy == "local_only":
            return []
        return ["serper"] if serper_ok else []
    if policy == "paid_only":
        return ["serper"] if serper_ok else []
    order: list[str] = []
    for source in _web_search_local_chain():
        if source == "serper" and policy != "local_only" and serper_ok:
            order.append("serper")
    return order


def _web_search_serper_http(query: str, num: int = 10) -> tuple[str, list[dict]]:
    q = str(query or "").strip()
    if not q:
        return ("miss", [])
    key = getattr(sys.modules[__name__], "SERPER_API_KEY", "") or ""
    if not key.strip():
        return ("miss", [])
    limit = max(1, min(20, int(num or 10)))
    try:
        resp = requests.post(
            "https://google.serper.dev/search",
            json={"q": q, "num": limit},
            headers={"X-API-KEY": key, "Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            msg = _serper_response_message(resp)
            msg_lower = msg.lower()
            if "not enough credits" in msg_lower:
                logging.warning("[Serper] credits exhausted for query=%r", q)
            else:
                logging.warning("[Serper] HTTP %s for query=%r: %s", resp.status_code, q, msg[:200])
            return ("error", [])
        data = resp.json()
        organic = data.get("organic") or []
        out = _normalize_web_results(
            [
                {
                    "title": item.get("title") if isinstance(item, dict) else "",
                    "link": item.get("link") if isinstance(item, dict) else "",
                    "snippet": item.get("snippet") if isinstance(item, dict) else "",
                }
                for item in organic
            ],
            source="serper",
            max_items=limit,
        )
        return ("hit" if out else "miss", out[:limit])
    except Exception as e:
        logging.debug("[Serper] Request failed for query=%r: %s", q, e)
        return ("error", [])


def _web_search_serper(
    query: str,
    num: int = 10,
    *,
    allow_ai_fallback: bool = True,
    context: dict[str, Any] | None = None,
    analysis_type: str = "web_search",
) -> List[dict]:
    """
    Run web search with provider search first, then local/premium AI fallback:
    1) Serper when configured and allowed by policy
    2) Ollama web search fallback
    3) Paid OpenAI web-search fallback when explicitly allowed
    Returns list of {"title", "link", "snippet"}.
    """
    q = str(query or "").strip()
    if not q:
        return []
    limit = max(1, min(20, int(num or 10)))
    scope = "web_search"
    inmem_found, inmem_rows = _ai_web_search_cache_lookup(q, limit)
    if inmem_found:
        return inmem_rows[:limit]
    persisted_found, persisted_rows, persisted_status = _ai_query_cache_get(scope, q, limit)
    if persisted_found:
        _ai_web_search_cache_set(q, limit, persisted_rows, status=persisted_status)
        return persisted_rows[:limit]
    first_run_seen = _ai_web_search_mark_run_query_seen(q, limit)
    if not first_run_seen:
        # Query already attempted during this run: avoid duplicate network calls.
        return []

    last_status = "miss"
    last_source = "none"
    for source in _web_search_provider_order():
        if source == "serper":
            status, rows = _web_search_serper_http(q, num=limit)
        else:
            continue
        last_status = status
        last_source = source
        if rows:
            logging.info(
                "[WEB] query=%r provider=%s results=%d top=%s",
                q,
                source,
                len(rows),
                _web_results_log_summary(rows),
            )
            _ai_web_search_cache_set(q, limit, rows[:limit], status="hit")
            _ai_query_cache_set(scope, q, limit, rows[:limit], status="hit", source=source)
            return rows[:limit]
    if _ollama_web_search_enabled(allow_ai_fallback=allow_ai_fallback):
        ollama_status, ollama_rows, ollama_meta = _ollama_web_search(
            q,
            num=limit,
            reason=f"{last_source}_{last_status}",
            context=context,
            analysis_type=analysis_type,
        )
        last_status = ollama_status
        last_source = "ollama_web_search"
        if ollama_rows:
            logging.info(
                "[WEB] query=%r provider=%s results=%d confidence=%.2f top=%s",
                q,
                "ollama_web_search",
                len(ollama_rows),
                float(ollama_meta.get("confidence") or 0.0),
                _web_results_log_summary(ollama_rows),
            )
            _ai_web_search_cache_set(q, limit, ollama_rows[:limit], status="hit")
            _ai_query_cache_set(scope, q, limit, ollama_rows[:limit], status="hit", source="ollama_web_search")
            return ollama_rows[:limit]
    if _web_search_ai_fallback_enabled(allow_ai_fallback=allow_ai_fallback):
        fallback_rows = _openai_web_search_fallback(q, num=limit, reason=f"{last_source}_{last_status}")
        if fallback_rows:
            logging.info(
                "[WEB] query=%r provider=%s results=%d top=%s",
                q,
                "openai_web_search",
                len(fallback_rows),
                _web_results_log_summary(fallback_rows),
            )
            _ai_web_search_cache_set(q, limit, fallback_rows[:limit], status="hit")
            _ai_query_cache_set(scope, q, limit, fallback_rows[:limit], status="hit", source="openai_web_search")
            return fallback_rows[:limit]
    logging.info(
        "[WEB] query=%r results=0 providers=%s last_status=%s",
        q,
        ",".join(_web_search_provider_order() + (["ollama_web_search"] if _ollama_web_search_enabled(allow_ai_fallback=allow_ai_fallback) else [])) or "none",
        last_status,
    )
    _ai_web_search_cache_set(q, limit, [], status=last_status)
    _ai_query_cache_set(scope, q, limit, [], status=last_status, source=last_source)
    return []


_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}

def _extract_text_from_openai_response_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _extract_text_from_openai_response(*args, **kwargs)

def _assistant_extract_json_array_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_extract_json_array(*args, **kwargs)

def _assistant_extract_json_object_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _assistant_extract_json_object(*args, **kwargs)

def _normalize_web_results_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _normalize_web_results(*args, **kwargs)

def _ai_web_search_cache_key_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_web_search_cache_key(*args, **kwargs)

def _ai_web_search_cache_get_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_web_search_cache_get(*args, **kwargs)

def _ai_web_search_cache_lookup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_web_search_cache_lookup(*args, **kwargs)

def _ai_web_search_cache_set_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_web_search_cache_set(*args, **kwargs)

def _ai_query_cache_get_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_query_cache_get(*args, **kwargs)

def _ai_query_cache_set_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_query_cache_set(*args, **kwargs)

def _ai_web_search_run_key_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_web_search_run_key(*args, **kwargs)

def _ai_web_search_mark_run_query_seen_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_web_search_mark_run_query_seen(*args, **kwargs)

def _ai_web_search_budget_allows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_web_search_budget_allows(*args, **kwargs)

def _ai_web_search_available_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_web_search_available(*args, **kwargs)

def _ollama_web_search_enabled_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_web_search_enabled(*args, **kwargs)

def _ollama_web_search_context_lines_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_web_search_context_lines(*args, **kwargs)

def _ollama_web_search_should_retry_complex_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_web_search_should_retry_complex(*args, **kwargs)

def _ollama_web_search_allowed_domains_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_web_search_allowed_domains(*args, **kwargs)

def _ollama_web_search_prompt_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_web_search_prompt(*args, **kwargs)

def _ollama_web_search_response_schema_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_web_search_response_schema(*args, **kwargs)

def _ollama_chat_json_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_chat_json(*args, **kwargs)

def _duckduckgo_html_search_http_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _duckduckgo_html_search_http(*args, **kwargs)

def _ollama_web_search_seed_hits_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_web_search_seed_hits(*args, **kwargs)

def _ollama_web_search_enrich_seed_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_web_search_enrich_seed_rows(*args, **kwargs)

def _review_hit_domain_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_hit_domain(*args, **kwargs)

def _review_domain_matches_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_domain_matches(*args, **kwargs)

def _review_row_text_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_row_text(*args, **kwargs)

def _review_hit_has_signal_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_hit_has_signal(*args, **kwargs)

def _review_hit_is_metadata_only_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_hit_is_metadata_only(*args, **kwargs)

def _review_filter_primary_hits_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_filter_primary_hits(*args, **kwargs)

def _ollama_web_search_parse_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_web_search_parse_rows(*args, **kwargs)

def _ollama_web_search_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_web_search(*args, **kwargs)

def _is_openai_web_search_unsupported_error_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _is_openai_web_search_unsupported_error(*args, **kwargs)

def _openai_web_search_model_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _openai_web_search_model_candidates(*args, **kwargs)

def _openai_web_search_fallback_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _openai_web_search_fallback(*args, **kwargs)

def _web_search_ai_fallback_enabled_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _web_search_ai_fallback_enabled(*args, **kwargs)

def _web_search_provider_order_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _web_search_provider_order(*args, **kwargs)

def _web_search_serper_http_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _web_search_serper_http(*args, **kwargs)

def _web_search_serper_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _web_search_serper(*args, **kwargs)
