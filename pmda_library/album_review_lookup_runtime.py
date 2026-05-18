"""Runtime-bound album review lookup and validation helpers."""
from __future__ import annotations

from typing import Any

_REVIEW_PAGE_FETCH_TIMEOUT_SEC = 8.0
_REVIEW_PAGE_EXCERPT_MAX_CHARS = 1400
_REVIEW_MAX_CANDIDATE_PAGES = 6
_REVIEW_MAX_CANDIDATE_PROBES = 12
_REVIEW_BODY_MIN_PARAGRAPH_CHARS = 80
_REVIEW_BODY_MAX_PARAGRAPHS = 6

_REVIEW_PARAGRAPH_PREFIX_PATTERNS = (
    r"^(?:save this story(?:\s+save story)?(?:\s+save this story)?\s*)+",
    r"^(?:share this story\s+)+",
    r"^(?:newsletter\s+search(?:\s+search)?(?:\s+news)?(?:\s+reviews)?(?:\s+best new music)?(?:\s+features)?(?:\s+lists)?(?:\s+columns)?(?:\s+video)?(?:\s+open navigation menu)?(?:\s+menu)?(?:\s+search)?\s*)+",
)

_REVIEW_PARAGRAPH_TRIM_MARKERS = (
    "most read",
    "all rights reserved",
    "affiliate partnerships",
    "copyright",
    "newsletter",
    "open navigation menu",
    "save this story",
    "related articles",
    "more from",
)

_REVIEW_PARAGRAPH_DROP_PATTERNS = (
    r"\bnewsletter\b",
    r"\bopen navigation menu\b",
    r"\ball rights reserved\b",
    r"\baffiliate partnerships\b",
    r"\bpitchfork may earn\b",
    r"\bconde nast\b",
    r"\bhas been writing for\b",
    r"\bsign up\b",
    r"\bprivacy policy\b",
    r"\bterms of use\b",
)

_EXTRACTED_NAMES = {
    '_review_lookup_query_plan',
    '_review_candidate_signal_blob',
    '_review_candidate_has_obvious_match',
    '_review_score_hits',
    '_review_lines_from_scored',
    '_review_lookup_collect_hits',
    '_review_page_meta_content',
    '_review_clean_paragraph_text',
    '_review_extract_paragraphs_from_html_block',
    '_review_extract_body_excerpt',
    '_review_fetch_page_context',
    '_review_prepare_candidates',
    '_review_candidates_need_broader_retry',
    '_review_validate_candidates_with_ai',
    '_review_search_source_from_hits',
    '_review_summary_fallback_from_hits',
    '_review_ai_provider_source',
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

def _review_lookup_query_plan(artist: str, album: str) -> tuple[str, list[str], int]:
    artist_query = re.sub(r"\s+", " ", str(artist or "").replace("…", "...")).strip()
    album_query = re.sub(r"\s+", " ", _sanitize_album_title_display(album)).strip()
    artist_relaxed = _normalize_identity_text_strict(artist_query).replace(" and ", " ").strip()
    album_relaxed = _normalize_identity_album_strict(album_query).replace(" and ", " ").strip()
    relaxed_review = f"{artist_relaxed} {album_relaxed} review".strip() if artist_relaxed and album_relaxed else ""
    relaxed_album_review = f"{artist_relaxed} {album_relaxed} album review".strip() if artist_relaxed and album_relaxed else ""
    relaxed_retrospective = f"{artist_relaxed} {album_relaxed} retrospective".strip() if artist_relaxed and album_relaxed else ""
    relaxed_reissue_review = f"{artist_relaxed} {album_relaxed} reissue review".strip() if artist_relaxed and album_relaxed else ""
    exact_album_review = f"\"{artist_query}\" \"{album_query}\" album review"
    exact_review = f"\"{artist_query}\" \"{album_query}\" review"
    primary = relaxed_review or relaxed_album_review or exact_review or exact_album_review
    expansions: list[str] = []

    def _add(query_text: str) -> None:
        q = re.sub(r"\s+", " ", str(query_text or "").strip())
        if not q or q == primary or q in expansions:
            return
        expansions.append(q)

    _add(relaxed_album_review)
    _add(relaxed_retrospective)
    _add(relaxed_reissue_review)
    _add(exact_review)
    _add(exact_album_review)
    return primary, expansions[:5], 5

def _review_candidate_signal_blob(candidate: dict[str, Any] | None) -> str:
    if not isinstance(candidate, dict):
        return ""
    parts = [
        str(candidate.get("title") or "").strip(),
        str(candidate.get("page_title") or "").strip(),
        str(candidate.get("snippet") or "").strip(),
        str(candidate.get("page_url") or candidate.get("link") or "").strip(),
    ]
    return " ".join(part for part in parts if part).strip()

def _review_candidate_has_obvious_match(artist: str, album: str, candidate: dict[str, Any] | None) -> bool:
    if not isinstance(candidate, dict):
        return False
    if str(candidate.get("fetch_error") or "").strip():
        return False
    if not str(candidate.get("page_excerpt") or "").strip():
        return False
    blob = _review_candidate_signal_blob(candidate)
    if not blob:
        return False
    if not _text_mentions_identity_phrase(artist, blob):
        return False
    if not _text_mentions_identity_phrase(album, blob):
        return False
    blob_norm = _normalize_identity_text_strict(blob)
    review_markers = (
        " album review ",
        " review ",
        " reviews ",
        " critic review ",
        " critique ",
        "/review/",
        "critic-reviews",
    )
    return any(marker.strip() in blob_norm or marker in blob.lower() for marker in review_markers)

def _review_score_hits(artist: str, album: str, hits: list[dict[str, Any]]) -> list[tuple[int, dict[str, Any]]]:
    scored_rows: list[tuple[int, dict[str, Any]]] = []
    for row in (hits or [])[:24]:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "").strip()
        link = str(row.get("link") or "").strip()
        snippet = str(row.get("snippet") or "").strip()
        if not (title or snippet):
            continue
        filtered_rows, _rejected_rows = _review_filter_primary_hits(artist, album, [row])
        if not filtered_rows:
            continue
        score = 0
        if _text_mentions_identity_phrase(artist, f"{title} {snippet}".strip()):
            score += 2
        if _text_mentions_identity_phrase(album, f"{title} {snippet}".strip()):
            score += 2
        if _review_hit_has_signal(row):
            score += 2
        if len(snippet) >= 80:
            score += 1
        scored_rows.append((score, row))
    scored_rows.sort(key=lambda item: item[0], reverse=True)
    return scored_rows

def _review_lines_from_scored(scored_rows: list[tuple[int, dict[str, Any]]]) -> list[str]:
    lines: list[str] = []
    for score, row in scored_rows:
        if score < 2:
            continue
        title = str(row.get("title") or "").strip()
        link = str(row.get("link") or "").strip()
        snippet = str(row.get("snippet") or "").strip()
        chunk = f"- {title} | {snippet}".strip(" |")
        if link:
            chunk += f" | {link}"
        lines.append(chunk)
        if len(lines) >= 6:
            break
    return lines

def _review_lookup_collect_hits(
    artist: str,
    album: str,
    *,
    query_batch_size: int,
    max_hits: int = 24,
    search_context: dict[str, Any] | None = None,
    continue_after_hit: bool = False,
) -> list[dict[str, Any]]:
    primary_query, expansion_queries, _ = _review_lookup_query_plan(artist, album)
    hits: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    def _add_batch(batch: list[dict[str, Any]]) -> None:
        nonlocal hits
        for row in (batch or [])[:query_batch_size]:
            if not isinstance(row, dict):
                continue
            link = str(row.get("link") or "").strip()
            title = str(row.get("title") or "").strip()
            snippet = str(row.get("snippet") or "").strip()
            key = link.lower() if link else f"{title.lower()}|{snippet.lower()[:120]}"
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            hits.append(row)
            if len(hits) >= max_hits:
                break

    for q in [primary_query] + expansion_queries:
        ddg_status, ddg_rows = _duckduckgo_html_search_http(q, num=query_batch_size)
        if ddg_rows:
            logging.info(
                "[Review Search] query=%r provider=duckduckgo results=%d top=%s",
                q,
                len(ddg_rows),
                _web_results_log_summary(ddg_rows),
            )
            _add_batch(ddg_rows)
        elif ddg_status == "error":
            logging.info("[Review Search] query=%r provider=duckduckgo results=0 status=error", q)
        if hits and not continue_after_hit:
            break
        _add_batch(
            _web_search_serper(
                q,
                num=query_batch_size,
                context=search_context,
                analysis_type="album_review_lookup",
            )
            or []
        )
        if hits and not continue_after_hit:
            break
    return hits[:max_hits]

def _review_page_meta_content(html_text: str, *names: str) -> str:
    if not html_text:
        return ""
    for name in names:
        safe_name = re.escape(str(name or "").strip())
        patterns = (
            rf'<meta[^>]+property=["\']{safe_name}["\'][^>]+content=["\']([^"\']+)["\']',
            rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']{safe_name}["\']',
            rf'<meta[^>]+name=["\']{safe_name}["\'][^>]+content=["\']([^"\']+)["\']',
            rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']{safe_name}["\']',
        )
        for pattern in patterns:
            match = re.search(pattern, html_text, flags=re.IGNORECASE)
            if match:
                value = _strip_html_text(html.unescape(str(match.group(1) or "").strip()))
                if value:
                    return value
    return ""

def _review_clean_paragraph_text(text: str) -> str:
    cleaned = _strip_html_text(html.unescape(str(text or "").strip()))
    if not cleaned:
        return ""
    for pattern in _REVIEW_PARAGRAPH_PREFIX_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip(" .:-\u2026")
    lowered = cleaned.lower()
    for marker in _REVIEW_PARAGRAPH_TRIM_MARKERS:
        idx = lowered.find(marker)
        if idx < 0:
            continue
        if idx <= 80:
            return ""
        cleaned = cleaned[:idx].strip(" .:-\u2026")
        lowered = cleaned.lower()
    if len(cleaned) < _REVIEW_BODY_MIN_PARAGRAPH_CHARS:
        return ""
    for pattern in _REVIEW_PARAGRAPH_DROP_PATTERNS:
        if re.search(pattern, lowered, flags=re.IGNORECASE):
            return ""
    return cleaned

def _review_extract_paragraphs_from_html_block(html_block: str) -> list[str]:
    if not html_block:
        return []
    paragraphs: list[str] = []
    seen: set[str] = set()
    for chunk in re.findall(r"(?is)<p[^>]*>(.*?)</p>", html_block):
        cleaned = _review_clean_paragraph_text(chunk)
        if not cleaned:
            continue
        key = _normalize_identity_text_strict(cleaned)
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        paragraphs.append(cleaned)
    return paragraphs

def _review_extract_body_excerpt(raw_html: str, *, meta_desc: str = "") -> str:
    if not raw_html:
        return ""
    html_no_scripts = re.sub(r"(?is)<(script|style|noscript|svg)[^>]*>.*?</\\1>", " ", raw_html)
    candidate_blocks = re.findall(r"(?is)<article[^>]*>(.*?)</article>", html_no_scripts)
    candidate_blocks.extend(re.findall(r"(?is)<main[^>]*>(.*?)</main>", html_no_scripts))
    if not candidate_blocks:
        candidate_blocks = [html_no_scripts]
    best_paragraphs: list[str] = []
    for block in candidate_blocks:
        paragraphs = _review_extract_paragraphs_from_html_block(block)
        if len(paragraphs) > len(best_paragraphs):
            best_paragraphs = paragraphs
    if not best_paragraphs and html_no_scripts:
        best_paragraphs = _review_extract_paragraphs_from_html_block(html_no_scripts)
    body_text = " ".join(best_paragraphs[:_REVIEW_BODY_MAX_PARAGRAPHS]).strip()
    if not body_text:
        body_text = _strip_html_text(html.unescape(meta_desc or "")).strip()
    if len(body_text) > _REVIEW_PAGE_EXCERPT_MAX_CHARS:
        body_text = body_text[:_REVIEW_PAGE_EXCERPT_MAX_CHARS].rsplit(" ", 1)[0].strip()
    return body_text

def _review_fetch_page_context(url: str) -> dict[str, str]:
    target_url = str(url or "").strip()
    if not target_url.startswith(("http://", "https://")):
        return {}
    try:
        resp = requests.get(
            target_url,
            headers={"User-Agent": "Mozilla/5.0 (PMDA review fetch)"},
            timeout=max(3, min(int(_REVIEW_PAGE_FETCH_TIMEOUT_SEC), 20)),
            allow_redirects=True,
        )
    except Exception as exc:
        return {"page_url": target_url, "fetch_error": str(exc or "").strip() or "request_failed"}
    final_url = str(resp.url or target_url).strip()
    if resp.status_code != 200:
        return {"page_url": final_url, "fetch_error": f"http_{resp.status_code}"}
    content_type = str((resp.headers.get("content-type") or "").split(";", 1)[0]).strip().lower()
    if "html" not in content_type and not content_type.startswith("text/"):
        return {"page_url": final_url, "fetch_error": f"unsupported_content_type:{content_type or 'unknown'}"}
    raw_html = str(resp.text or "")
    if not raw_html:
        return {"page_url": final_url, "fetch_error": "empty_body"}
    page_title = ""
    title_match = re.search(r"(?is)<title[^>]*>(.*?)</title>", raw_html)
    if title_match:
        page_title = _strip_html_text(html.unescape(str(title_match.group(1) or "").strip()))
    meta_desc = _review_page_meta_content(raw_html, "og:description", "twitter:description", "description")
    excerpt = _review_extract_body_excerpt(raw_html, meta_desc=meta_desc)
    out = {
        "page_url": final_url,
        "page_title": page_title,
        "page_excerpt": excerpt,
    }
    if not excerpt:
        out["fetch_error"] = "empty_excerpt"
    return out

def _review_prepare_candidates(
    hits: list[dict[str, Any]],
    *,
    max_candidates: int = _REVIEW_MAX_CANDIDATE_PAGES,
    max_probes: int = _REVIEW_MAX_CANDIDATE_PROBES,
) -> list[dict[str, Any]]:
    candidates_ok: list[dict[str, Any]] = []
    candidates_blocked: list[dict[str, Any]] = []
    probe_limit = max(1, int(max_probes or _REVIEW_MAX_CANDIDATE_PROBES))
    keep_limit = max(1, int(max_candidates or _REVIEW_MAX_CANDIDATE_PAGES))
    for idx, row in enumerate((hits or [])[:probe_limit], start=1):
        if not isinstance(row, dict):
            continue
        link = str(row.get("link") or "").strip()
        if not link:
            continue
        page_context = _review_fetch_page_context(link)
        candidate = dict(row)
        candidate["position"] = idx
        candidate["page_title"] = str(page_context.get("page_title") or "").strip()
        candidate["page_excerpt"] = str(page_context.get("page_excerpt") or "").strip()
        candidate["page_url"] = str(page_context.get("page_url") or link).strip()
        candidate["fetch_error"] = str(page_context.get("fetch_error") or "").strip()
        if candidate["fetch_error"] or not candidate["page_excerpt"]:
            candidates_blocked.append(candidate)
        else:
            candidates_ok.append(candidate)
            if len(candidates_ok) >= keep_limit:
                break
    candidates = candidates_ok[:keep_limit]
    if len(candidates) < keep_limit:
        candidates.extend(candidates_blocked[: keep_limit - len(candidates)])
    return candidates

def _review_candidates_need_broader_retry(candidates: list[dict[str, Any]]) -> bool:
    candidate_rows = [row for row in (candidates or []) if isinstance(row, dict)]
    if not candidate_rows:
        return False
    usable = sum(
        1
        for row in candidate_rows
        if str(row.get("page_excerpt") or "").strip() and not str(row.get("fetch_error") or "").strip()
    )
    blocked = sum(1 for row in candidate_rows if str(row.get("fetch_error") or "").strip())
    if usable <= 0:
        return True
    return blocked >= max(2, len(candidate_rows) - 1)

def _review_validate_candidates_with_ai(
    artist: str,
    album: str,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    if not artist or not album or not candidates:
        return {}
    requested_provider = str(getattr(sys.modules[__name__], "AI_PROVIDER", "ollama") or "ollama").strip() or "ollama"
    ai_ok, provider_effective, auth_mode, ai_reason = _resolve_ai_runtime_availability(
        analysis_type="album_review_validate",
        requested_provider=requested_provider,
        user_id=_current_user_id_or_zero(),
    )
    if not ai_ok:
        logging.info(
            "[Review] Cannot validate a review for %r - %r because AI is unavailable: %s",
            artist,
            album,
            ai_reason or "provider_unavailable",
        )
        return {}
    provider_norm = str(provider_effective or requested_provider).strip().lower()
    if provider_norm == "ollama":
        base_model = _ollama_model_configured()
        model_sequence = [base_model]
        complex_model = _ollama_complex_model_configured()
        if complex_model and complex_model != base_model and _ollama_model_available(complex_model):
            model_sequence.append(complex_model)
    elif provider_norm in {"openai", "openai-api", "openai-codex"}:
        model_sequence = [
            str(
                getattr(sys.modules[__name__], "RESOLVED_MODEL", None)
                or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4o-mini")
                or "gpt-4o-mini"
            ).strip()
        ]
    else:
        model_sequence = [str(getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or "").strip() or "gpt-4o-mini"]
    review_schema = {
        "type": "object",
        "properties": {
            "accepted": {"type": "boolean"},
            "confidence": {"type": "integer"},
            "reason": {"type": "string"},
        },
        "required": ["accepted", "confidence", "reason"],
    }
    system_msg = (
        "You validate one web page for a music album review.\n"
        "Accept when the title, page title, URL, and snippet clearly indicate a real review of the correct music album by the correct artist.\n"
        "Do not reject a candidate just because it compares the target album to older albums or mentions other album titles.\n"
        "Reject metadata pages, discographies, wikis, stores, marketplaces, lyrics, forums, news blurbs, and wrong artist or album matches.\n"
        "Use only the provided candidate data.\n"
        "Return ONLY JSON with keys: accepted, confidence, reason.\n"
        "accepted must be boolean. confidence must be integer 0..100. reason must be a short string.\n"
    )
    last_confidence = 0
    last_reason = ""
    candidate_list = [row for row in candidates[:_REVIEW_MAX_CANDIDATE_PAGES] if isinstance(row, dict)]
    for candidate in candidate_list:
        selected_index = max(1, int(candidate.get("position") or 0))
        candidate_url = str(candidate.get("page_url") or candidate.get("link") or "").strip()
        if _review_candidate_has_obvious_match(artist, album, candidate):
            logging.info(
                "[Review] Accepted obvious candidate #%d for %r - %r (reason=strong_title_match, url=%s).",
                selected_index,
                artist,
                album,
                candidate_url,
            )
            return {
                "confidence": 95,
                "reason": "strong_title_match",
                "selected_index": selected_index,
                "selected": candidate,
                "provider_effective": provider_effective,
                "auth_mode": auth_mode,
                "model": "",
            }
        candidate_reason = ""
        candidate_confidence = 0
        for idx, model in enumerate([m for m in model_sequence if str(m or "").strip()]):
            user_msg = (
                f"Target artist: {artist}\n"
                f"Target album: {album}\n"
                f"Candidate position: {selected_index}\n"
                f"Search title: {str(candidate.get('title') or '').strip()}\n"
                f"URL: {candidate_url}\n"
                f"Search snippet: {str(candidate.get('snippet') or '').strip()}\n"
                f"Page title: {str(candidate.get('page_title') or '').strip()}\n"
                f"Fetch error: {str(candidate.get('fetch_error') or '').strip()}\n"
            )
            try:
                if provider_norm == "ollama":
                    response_obj = _ollama_chat_json(
                        model_name=model,
                        system_msg=system_msg,
                        user_msg=user_msg,
                        timeout_sec=45 if idx > 0 else 30,
                        format_schema=review_schema,
                        analysis_type="album_review_validate",
                    )
                    ai_out = str(((response_obj.get("message") or {}) if isinstance(response_obj, dict) else {}).get("content") or "").strip()
                else:
                    ai_out = call_ai_provider(
                        requested_provider,
                        model,
                        system_msg,
                        user_msg,
                        max_tokens=320,
                        analysis_type="album_review_validate",
                        request_timeout_sec=45 if idx > 0 else 30,
                    )
            except Exception as exc:
                logging.info(
                    "[Review] Candidate #%d for %r - %r could not be validated with %s: %s",
                    selected_index,
                    artist,
                    album,
                    model,
                    exc,
                )
                if idx < (len(model_sequence) - 1):
                    logging.info(
                        "[Review] Candidate #%d for %r - %r needs a second opinion; retrying with %s.",
                        selected_index,
                        artist,
                        album,
                        model_sequence[idx + 1],
                    )
                    continue
                candidate_reason = "validation_error"
                break
            payload = _assistant_extract_json_object(str(ai_out or ""))
            if not payload:
                if idx < (len(model_sequence) - 1):
                    logging.info(
                        "[Review] Candidate #%d for %r - %r returned no JSON with %s; retrying with %s.",
                        selected_index,
                        artist,
                        album,
                        model,
                        model_sequence[idx + 1],
                    )
                    continue
                candidate_reason = "empty_json"
                break
            accepted = bool(payload.get("accepted"))
            try:
                confidence = max(0, min(100, int(payload.get("confidence") or 0)))
            except Exception:
                confidence = 0
            reason = str(payload.get("reason") or "").strip() or "no_reason"
            if accepted and confidence < 60 and _review_candidate_has_obvious_match(artist, album, candidate):
                confidence = 85
                reason = reason or "strong_title_match"
            candidate_confidence = confidence
            candidate_reason = reason
            weak_verdict = confidence < 70
            if accepted and confidence >= 60:
                logging.info(
                    "[Review] Accepted candidate #%d for %r - %r (confidence=%d, provider=%s, auth=%s, model=%s, reason=%s, url=%s).",
                    selected_index,
                    artist,
                    album,
                    confidence,
                    provider_effective,
                    auth_mode,
                    model,
                    reason or "validated",
                    candidate_url,
                )
                return {
                    "confidence": confidence,
                    "reason": reason,
                    "selected_index": selected_index,
                    "selected": candidate,
                    "provider_effective": provider_effective,
                    "auth_mode": auth_mode,
                    "model": model,
                }
            if weak_verdict and idx < (len(model_sequence) - 1):
                logging.info(
                    "[Review] Candidate #%d for %r - %r looks inconclusive with %s (confidence=%d, reason=%s); retrying with %s.",
                    selected_index,
                    artist,
                    album,
                    model,
                    confidence,
                    reason,
                    model_sequence[idx + 1],
                )
                continue
            logging.info(
                "[Review] Rejected candidate #%d for %r - %r (confidence=%d, model=%s, reason=%s, url=%s).",
                selected_index,
                artist,
                album,
                confidence,
                model,
                reason,
                candidate_url,
            )
            break
        last_confidence = candidate_confidence
        last_reason = candidate_reason or "no_valid_review"
    if candidate_list:
        logging.info(
            "[Review] No trustworthy review found for %r - %r after checking %d candidate page(s). Last reason=%s confidence=%d.",
            artist,
            album,
            len(candidate_list),
            last_reason or "no_valid_review",
            last_confidence,
        )
    return {}

def _review_search_source_from_hits(hits: list[dict[str, Any]]) -> str:
    hit_sources = {
        str(row.get("source") or "").strip().lower()
        for row in (hits or [])
        if isinstance(row, dict) and str(row.get("source") or "").strip()
    }
    if "duckduckgo" in hit_sources:
        return "duckduckgo"
    if "ollama_web_search" in hit_sources or "ollama" in hit_sources:
        return "ollama"
    if "openai-codex" in hit_sources:
        return "openai-codex"
    if "openai-api" in hit_sources:
        return "openai-api"
    if "openai_web_search" in hit_sources:
        try:
            _ai_ok, provider_effective, auth_mode, _reason = _resolve_ai_runtime_availability(
                analysis_type="web_search",
                requested_provider="openai",
                user_id=_current_user_id_or_zero(),
            )
            if str(provider_effective or "").strip():
                return _review_ai_provider_source(provider_effective, auth_mode, "openai_web_search")
        except Exception:
            pass
        return "openai_web_search"
    return "serper"

def _review_summary_fallback_from_hits(artist: str, album: str, hits: list[dict[str, Any]]) -> str:
    filtered_hits, _rejected = _review_filter_primary_hits(artist, album, hits)
    merged = " ".join(
        _strip_html_text(str(row.get("snippet") or "").strip())
        for row in filtered_hits[:6]
        if isinstance(row, dict) and str(row.get("snippet") or "").strip()
    ).strip()
    return _truncate_text(merged, max_chars=900) if merged else ""

def _review_ai_provider_source(provider_effective: str, auth_mode: str, fallback_source: str) -> str:
    provider_norm = str(provider_effective or "").strip().lower()
    auth_norm = str(auth_mode or "").strip().lower()
    if provider_norm in {"openai", "openai-api", "openai-codex"}:
        return "openai-codex" if auth_norm == "oauth" else "openai-api"
    if provider_norm == "ollama":
        return "ollama"
    if provider_norm:
        return provider_norm
    return str(fallback_source or "").strip() or "openai-api"
_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}

def _review_lookup_query_plan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_lookup_query_plan(*args, **kwargs)

def _review_candidate_signal_blob_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_candidate_signal_blob(*args, **kwargs)

def _review_candidate_has_obvious_match_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_candidate_has_obvious_match(*args, **kwargs)

def _review_score_hits_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_score_hits(*args, **kwargs)

def _review_lines_from_scored_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_lines_from_scored(*args, **kwargs)

def _review_lookup_collect_hits_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_lookup_collect_hits(*args, **kwargs)

def _review_page_meta_content_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_page_meta_content(*args, **kwargs)

def _review_clean_paragraph_text_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_clean_paragraph_text(*args, **kwargs)

def _review_extract_paragraphs_from_html_block_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_extract_paragraphs_from_html_block(*args, **kwargs)

def _review_extract_body_excerpt_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_extract_body_excerpt(*args, **kwargs)

def _review_fetch_page_context_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_fetch_page_context(*args, **kwargs)

def _review_prepare_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_prepare_candidates(*args, **kwargs)

def _review_candidates_need_broader_retry_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_candidates_need_broader_retry(*args, **kwargs)

def _review_validate_candidates_with_ai_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_validate_candidates_with_ai(*args, **kwargs)

def _review_search_source_from_hits_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_search_source_from_hits(*args, **kwargs)

def _review_summary_fallback_from_hits_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_summary_fallback_from_hits(*args, **kwargs)

def _review_ai_provider_source_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _review_ai_provider_source(*args, **kwargs)
