"""Bandcamp provider runtime extracted from the PMDA bootstrap module."""

from __future__ import annotations

import html
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urlparse

import requests
from requests import exceptions as requests_exceptions


_RUNTIME: Any | None = None
_SYNC_GLOBALS = ("_last_bandcamp_request",)


def _bind_runtime(runtime: Any) -> None:
    """Expose PMDA runtime globals to the extracted Bandcamp provider."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "_fetch_bandcamp_album_info",
        "fetch_bandcamp_album_info_for_runtime",
        "_bind_runtime",
        "_sync_runtime_globals",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def _sync_runtime_globals() -> None:
    if _RUNTIME is None:
        return
    for key in _SYNC_GLOBALS:
        if key in globals():
            try:
                setattr(_RUNTIME, key, globals()[key])
            except Exception:
                logging.debug("Failed to sync Bandcamp runtime global %s", key, exc_info=True)


def fetch_bandcamp_album_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any):
    _bind_runtime(runtime)
    result = _fetch_bandcamp_album_info(*args, **kwargs)
    _sync_runtime_globals()
    return result


def _fetch_bandcamp_album_info(
    artist_name: str,
    album_title: str,
    *,
    allow_web_fallback: bool | None = None,
    album_url_hint: str = "",
) -> Optional[dict]:
    """
    Search Bandcamp for an album by artist + title. Returns dict with title, artist_name, year, cover_url, tracklist, tags, or None.
    Uses scraping; no public API. Rate-limited (5s between calls). User-Agent identifies PMDA.
    Use at your own risk regarding Bandcamp ToS.
    """
    if not USE_BANDCAMP:
        return None
    global _last_bandcamp_request
    if not PROVIDER_GATEWAY_ENABLED:
        with _bandcamp_lock:
            now = time.time()
            wait = 5.0 - (now - _last_bandcamp_request)
            if wait > 0:
                time.sleep(wait)
            _last_bandcamp_request = time.time()
    headers = {
        "User-Agent": "PMDA/1.0 (metadata fallback; https://github.com/silkyclouds/PMDA)",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def _bandcamp_http_get(url: str, *, context: str, allow_redirects: bool = True) -> Optional[requests.Response]:
        last_error: Exception | None = None
        scan_active = bool(_scan_pipeline_active())
        timeout = (5, 12) if scan_active else (6, 25)
        max_attempts = 1 if scan_active else 2
        for attempt in range(max_attempts):
            try:
                resp = _provider_gateway_http_get(
                    "bandcamp",
                    url,
                    context=f"bandcamp {context}",
                    headers=headers,
                    timeout=timeout,
                    allow_redirects=allow_redirects,
                    cache_ttl_sec=60 * 60 * 12,
                )
                if resp.status_code == 429:
                    retry_after = 3.0
                    try:
                        retry_after = max(1.0, float(resp.headers.get("Retry-After") or 3.0))
                    except Exception:
                        retry_after = 3.0
                    if attempt < (max_attempts - 1):
                        time.sleep(min(retry_after, 8.0))
                        continue
                return resp
            except requests_exceptions.Timeout as e:
                last_error = e
                if attempt < (max_attempts - 1):
                    time.sleep(1.5)
                    continue
            except requests_exceptions.RequestException as e:
                last_error = e
                if _provider_is_name_resolution_failure(e):
                    break
                if attempt < (max_attempts - 1):
                    time.sleep(1.0)
                    continue
                break
        if _provider_is_name_resolution_failure(last_error):
            raise last_error
        if last_error is not None:
            raise last_error
        return None

    def _split_bandcamp_keywords(value: str) -> list[str]:
        raw = (value or "").strip()
        if not raw:
            return []
        # Bandcamp JSON-LD keywords is sometimes a comma-separated string.
        # Split conservatively and keep insertion order.
        parts: list[str] = []
        for part in re.split(r"[,\n\r]+", raw):
            txt = re.sub(r"\s+", " ", (part or "").strip())
            if txt:
                parts.append(txt)
        return parts

    def _bandcamp_first_nonempty(mapping: dict[str, Any], keys: tuple[str, ...]) -> str:
        for key in keys:
            value = mapping.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def _normalize_supporter_url(value: str) -> str:
        txt = str(value or "").strip()
        if not txt:
            return ""
        if txt.startswith("//"):
            return f"https:{txt}"
        if txt.startswith("/"):
            return f"https://bandcamp.com{txt}"
        return txt

    def _extract_bandcamp_collectors_blob(raw_page: str) -> dict[str, Any]:
        blob_match = re.search(
            r'<div[^>]+id="collectors-data"[^>]+data-blob=(?:"([^"]+)"|\'([^\']+)\')',
            raw_page,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not blob_match:
            return {}
        blob_raw = blob_match.group(1) or blob_match.group(2) or ""
        try:
            blob = html.unescape(html.unescape(blob_raw))
            parsed = json.loads(blob)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _normalize_bandcamp_supporter_comment_items(raw_items: Any, limit: int = 12) -> list[dict[str, str]]:
        if not isinstance(raw_items, list):
            return []
        out: list[dict[str, str]] = []
        seen: set[str] = set()
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            nested_fan = item.get("fan") if isinstance(item.get("fan"), dict) else {}
            nested_user = item.get("user") if isinstance(item.get("user"), dict) else {}
            nested_data = item.get("fan_data") if isinstance(item.get("fan_data"), dict) else {}
            author = (
                _bandcamp_first_nonempty(item, ("name", "fan_name", "username", "display_name", "comment_author", "author"))
                or _bandcamp_first_nonempty(nested_fan, ("name", "fan_name", "username", "display_name"))
                or _bandcamp_first_nonempty(nested_user, ("name", "username", "display_name"))
                or _bandcamp_first_nonempty(nested_data, ("name", "username", "display_name"))
            )
            text = (
                _bandcamp_first_nonempty(item, ("why", "text", "review", "comment", "body", "contents", "blurb"))
                or _bandcamp_first_nonempty(nested_fan, ("why", "text", "review", "comment", "body"))
                or _bandcamp_first_nonempty(nested_user, ("why", "text", "review", "comment", "body"))
            )
            author = _strip_html_text(author)
            text = _strip_html_text(text)
            if not text:
                continue
            key = f"{author.lower()}|{text.lower()}"
            if key in seen:
                continue
            seen.add(key)
            comment: dict[str, str] = {
                "text": _truncate_text(text, max_chars=600),
            }
            if author:
                comment["author"] = author
            supporter_url = (
                _normalize_supporter_url(_bandcamp_first_nonempty(item, ("fan_url", "url", "collection_url", "permalink")))
                or _normalize_supporter_url(_bandcamp_first_nonempty(nested_fan, ("url", "fan_url", "collection_url")))
                or _normalize_supporter_url(_bandcamp_first_nonempty(nested_user, ("url", "fan_url")))
            )
            if supporter_url:
                comment["url"] = supporter_url
            avatar_url = (
                _normalize_supporter_url(_bandcamp_first_nonempty(item, ("thumb_url", "image_url", "avatar_url")))
                or _normalize_supporter_url(_bandcamp_first_nonempty(nested_fan, ("thumb_url", "image_url", "avatar_url")))
                or _normalize_supporter_url(_bandcamp_first_nonempty(nested_user, ("thumb_url", "image_url", "avatar_url")))
            )
            if avatar_url:
                comment["avatar_url"] = avatar_url
            out.append(comment)
            if len(out) >= max(1, int(limit or 12)):
                break
        return out

    def _parse_bandcamp_page_owner(page_html: str, album_url: str) -> dict[str, str]:
        owner_name = ""
        owner_location = ""
        owner_bio = ""
        owner_image_url = ""
        try:
            owner_match = re.search(
                r'<p id="band-name-location".*?</p>',
                page_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if owner_match:
                owner_block = owner_match.group(0)
                name_match = re.search(r'<span class="title">([^<]+)</span>', owner_block, flags=re.IGNORECASE)
                location_match = re.search(r'<span class="location[^"]*">([^<]+)</span>', owner_block, flags=re.IGNORECASE)
                if name_match:
                    owner_name = _strip_html_text(name_match.group(1))
                if location_match:
                    owner_location = _strip_html_text(location_match.group(1))
            bio_match = re.search(
                r'<p id="bio-text">(.+?)</p>',
                page_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if bio_match:
                owner_bio = _strip_html_text(bio_match.group(1))
            img_match = re.search(
                r'<img(?=[^>]*\bclass="[^"]*\bband-photo\b[^"]*")(?=[^>]*\bsrc="([^"]+)")[^>]*>',
                page_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if img_match:
                owner_image_url = _bandcamp_preferred_image_url(_normalize_supporter_url(img_match.group(1)))
            if not owner_image_url:
                popup_match = re.search(
                    r'<a[^>]+class="popupImage"[^>]+href="([^"]+)"',
                    page_html,
                    flags=re.IGNORECASE | re.DOTALL,
                )
                if popup_match:
                    owner_image_url = _bandcamp_preferred_image_url(_normalize_supporter_url(popup_match.group(1)))
        except Exception:
            pass
        owner_url = ""
        try:
            parsed = urlparse(album_url)
            if parsed.scheme and parsed.netloc:
                owner_url = f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            owner_url = ""
        return {
            "page_owner_name": owner_name,
            "page_owner_location": owner_location,
            "page_owner_bio": owner_bio,
            "page_owner_image_url": owner_image_url,
            "page_owner_url": owner_url,
        }

    def _parse_album_page(album_url: str) -> Optional[dict]:
        album_resp = _bandcamp_http_get(album_url, context="album_page")
        if album_resp is None:
            return None
        if album_resp.status_code != 200:
            return None
        page = album_resp.text
        owner_payload = _parse_bandcamp_page_owner(page, album_url)

        def _clean_bandcamp_description(raw: str) -> str:
            txt = html.unescape(html.unescape(str(raw or "")))
            txt = re.sub(r"<[^>]+>", " ", txt)
            txt = txt.replace("\r", "\n")
            lines: list[str] = []
            for line in txt.splitlines():
                ln = re.sub(r"\s+", " ", line).strip()
                if not ln:
                    continue
                # Drop plain numbered tracklist rows to keep the review body readable.
                if re.match(r"^\d+\.\s+\S", ln):
                    continue
                lines.append(ln)
            cleaned = "\n".join(lines).strip()
            cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
            return cleaned

        title = None
        cover_url = None
        cover_candidates: List[str] = []
        tralbum_data: dict[str, Any] = {}
        m_tralbum = re.search(r'data-tralbum="([^"]+)"', page, flags=re.IGNORECASE)
        if m_tralbum:
            try:
                blob = html.unescape(m_tralbum.group(1))
                parsed = json.loads(blob)
                if isinstance(parsed, dict):
                    tralbum_data = parsed
            except Exception:
                tralbum_data = {}
        og_title = re.search(r'<meta\s+property="og:title"\s+content="([^"]+)"', page)
        if og_title:
            title = html.unescape(og_title.group(1)).strip()
        og_image = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', page)
        if og_image:
            cover_url = og_image.group(1).strip()
            cover_candidates.append(cover_url)
        page_cover_matches = re.findall(
            r"(https?://f\d+\.bcbits\.com/img/[ab]\d+_[0-9]+\.[a-zA-Z0-9]+(?:\?[^\"]*)?)",
            page,
        )
        if page_cover_matches:
            cover_candidates.extend(page_cover_matches)
        art_id_match = re.search(r'"art_id"\s*:\s*(\d+)', page)
        if art_id_match:
            art_id = art_id_match.group(1)
            cover_candidates.append(f"https://f4.bcbits.com/img/a{art_id}_0.jpg")
        cover_candidates = _dedupe_keep_order(cover_candidates)
        if cover_candidates:
            prioritized = []
            for u in cover_candidates:
                m_size = re.search(r"_([0-9]+)\.[a-zA-Z0-9]+(?:\?|$)", u)
                if m_size:
                    val = int(m_size.group(1))
                    score = 10_000_000 if val == 0 else val
                else:
                    score = 0
                prioritized.append((score, u))
            prioritized.sort(key=lambda x: x[0], reverse=True)
            cover_url = prioritized[0][1]

        current_payload = tralbum_data.get("current") if isinstance(tralbum_data.get("current"), dict) else {}
        title = str(current_payload.get("title") or title or "").strip() or title
        artist_str = str(current_payload.get("artist") or artist_name or "").strip() or artist_name
        if title and " by " in title:
            # Bandcamp uses "Album, by Artist". Album titles can themselves contain "by",
            # so split on the *last* occurrence to avoid mis-parsing titles like "Destroyed by Fire".
            parts = title.rsplit(" by ", 1)
            title = parts[0].strip()
            if len(parts) > 1:
                artist_str = parts[1].strip()
        if not title:
            title = album_title

        year = ""
        year_m = re.search(r'released\s+(\w+\s+\d{1,2},?\s+\d{4})', page)
        if year_m:
            year = year_m.group(1)
        if not year:
            year = str(
                current_payload.get("publish_date")
                or current_payload.get("release_date")
                or current_payload.get("new_date")
                or ""
            ).strip()

        description = ""
        if isinstance(current_payload, dict):
            for key in ("about", "description", "album_description", "albumDescription", "credits"):
                raw_desc = current_payload.get(key)
                if isinstance(raw_desc, str) and raw_desc.strip():
                    description = _clean_bandcamp_description(raw_desc)
                    break
        if not description:
            desc_m = re.search(
                r'<meta\s+name="description"\s+content="([^"]+)"',
                page,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if desc_m:
                description = _clean_bandcamp_description(desc_m.group(1))

        tracklist = []
        track_title_spans = re.findall(r'class="track-title"[^>]*>([^<]+)</span>', page)
        if track_title_spans:
            tracklist = [html.unescape(t).strip() for t in track_title_spans if t.strip()]
        if not tracklist:
            track_title_data = re.findall(r'data-item-title="([^"]+)"', page)
            if track_title_data:
                tracklist = [html.unescape(t).strip() for t in track_title_data if t.strip()]

        tags: List[str] = []
        try:
            # Some Bandcamp pages do not use rel="tag" anymore. Keep both patterns.
            tag_matches = re.findall(r'rel=[\'"]tag[\'"][^>]*>([^<]+)</a>', page, flags=re.IGNORECASE)
            if not tag_matches:
                tag_matches = re.findall(
                    r'<a[^>]*class=[\'"][^\'"]*\btag\b[^\'"]*[\'"][^>]*>([^<]+)</a>',
                    page,
                    flags=re.IGNORECASE,
                )
            tags = [html.unescape(t).strip() for t in tag_matches if isinstance(t, str) and t.strip()]
            if not tags:
                jsonld_blocks = re.findall(
                    r'<script[^>]+type="application/ld\+json"[^>]*>\s*(.*?)\s*</script>',
                    page,
                    flags=re.IGNORECASE | re.DOTALL,
                )
                for block in jsonld_blocks:
                    try:
                        data = json.loads(block)
                    except Exception:
                        continue
                    # JSON-LD can be a dict or a list of dicts.
                    objects = []
                    if isinstance(data, dict):
                        objects = [data]
                    elif isinstance(data, list):
                        objects = [o for o in data if isinstance(o, dict)]
                    for obj in objects:
                        keywords = obj.get("keywords")
                        if isinstance(keywords, list):
                            tags.extend([str(x).strip() for x in keywords if str(x).strip()])
                        elif isinstance(keywords, str):
                            tags.extend(_split_bandcamp_keywords(keywords))
                    if tags:
                        break
            if not tags:
                # Some pages embed JSON in the data-tralbum attribute.
                if tralbum_data:
                    raw_tags = (
                        tralbum_data.get("tags")
                        or (tralbum_data.get("current") or {}).get("tags")
                        or (tralbum_data.get("album") or {}).get("tags")
                    )
                    if isinstance(raw_tags, list):
                        tags.extend([str(x).strip() for x in raw_tags if str(x).strip()])
                    elif isinstance(raw_tags, str):
                        tags.extend(_split_bandcamp_keywords(raw_tags))
            tags = _dedupe_keep_order([t for t in tags if isinstance(t, str) and t.strip()])
        except Exception:
            tags = []
        supporter_count = 0
        supporter_comments: list[dict[str, str]] = []
        try:
            collectors_data = _extract_bandcamp_collectors_blob(page)
            raw_review_items: list[Any] = []
            if isinstance(collectors_data, dict):
                for key in (
                    "shown_reviews",
                    "reviews",
                    "review_items",
                    "reviewItems",
                    "fan_reviews",
                    "fanReviews",
                    "supporter_reviews",
                    "supporterReviews",
                    "fan_comments",
                    "fanComments",
                ):
                    val = collectors_data.get(key)
                    if isinstance(val, list):
                        raw_review_items.extend(val)
            supporter_comments = _normalize_bandcamp_supporter_comment_items(raw_review_items)
            support_line = re.search(
                r"supported by\s+(\d[\d,]*)\s+fans?\s+who\s+also\s+own",
                page,
                flags=re.IGNORECASE,
            )
            if support_line:
                supporter_count = _safe_nonneg_int(support_line.group(1))
            if supporter_count <= 0:
                shown_thumbs_block = re.search(
                    r'&quot;shown_thumbs&quot;:\[(.*?)\](?:,&quot;more_thumbs_available&quot;|,&quot;reviews&quot;)',
                    page,
                    flags=re.IGNORECASE | re.DOTALL,
                )
                if shown_thumbs_block:
                    supporter_count = len(re.findall(r"&quot;fan_id&quot;:", shown_thumbs_block.group(1)))
            if supporter_count <= 0:
                sponsor_block = re.search(r'"sponsor"\s*:\s*\[(.*?)\]\s*,\s*"numTracks"', page, flags=re.IGNORECASE | re.DOTALL)
                if sponsor_block:
                    supporter_count = len(re.findall(r'"@type"\s*:\s*"Person"', sponsor_block.group(1), flags=re.IGNORECASE))
            if supporter_count <= 0 and supporter_comments:
                supporter_count = len(supporter_comments)
        except Exception:
            supporter_count = 0
            supporter_comments = []

        payload = {
            "title": title,
            "artist_name": artist_str,
            "year": year,
            "cover_url": cover_url,
            "cover_candidates": cover_candidates,
            "tracklist": tracklist,
            "tags": tags,
            "description": description,
            "album_url": album_url,
            "bandcamp_supporter_count": supporter_count,
            "bandcamp_supporter_comments": supporter_comments,
        }
        payload.update(owner_payload)
        return payload

    try:
        if allow_web_fallback is None:
            allow_web_fallback = not _scan_inline_matching_active() and not _ai_scan_lifecycle_phase_active()
        hint_url = str(album_url_hint or "").strip().split("?", 1)[0].split("#", 1)[0].strip()
        if hint_url and "bandcamp.com/album/" in hint_url.lower():
            hinted = _parse_album_page(hint_url)
            if hinted:
                strict_ok, _strict_reason = _strict_identity_match_details(
                    local_artist=artist_name,
                    local_title=album_title,
                    candidate_artist=hinted.get("artist_name") or "",
                    candidate_title=hinted.get("title") or "",
                )
                if strict_ok:
                    return hinted
                title_score = _provider_identity_album_score(
                    album_title,
                    str(hinted.get("title") or ""),
                    artist_hints=[artist_name, str(hinted.get("artist_name") or "")],
                )
                artist_score = _provider_identity_artist_score(
                    artist_name,
                    str(hinted.get("artist_name") or ""),
                )
                if title_score >= 0.86 and artist_score >= 0.74:
                    return hinted
        q = quote_plus(f"{artist_name} {album_title}".strip())
        search_url = f"https://bandcamp.com/search?q={q}&item_type=a"
        search_page = ""
        try:
            resp = _bandcamp_http_get(search_url, context="search")
            if resp is not None and resp.status_code == 200:
                search_page = resp.text
        except Exception as e:
            logging.debug("[Bandcamp] direct search failed for %r / %r: %s", artist_name, album_title, e)

        artist_norm_compact = _normalize_identity_text_strict(artist_name).replace(" ", "")
        album_norm_compact = _normalize_identity_album_strict(album_title).replace(" ", "")

        candidates: List[Tuple[float, str]] = []
        def _score_bandcamp_candidate(url: str, idx: int = 0, *, base_score: float = 0.0) -> float:
            score = float(base_score)
            slug = ""
            try:
                slug = re.sub(r"^https?://", "", url).split("/album/", 1)[1].split("?", 1)[0].split("#", 1)[0]
            except Exception:
                slug = ""
            slug_norm = _normalize_identity_album_strict(slug).replace(" ", "")
            if album_norm_compact and slug_norm:
                if album_norm_compact == slug_norm:
                    score += 2.0
                elif album_norm_compact in slug_norm or slug_norm in album_norm_compact:
                    score += 1.0

            host = re.sub(r"^https?://", "", url).split("/")[0]
            host_artist = host.split(".")[0]
            host_artist_norm = _normalize_identity_text_strict(host_artist).replace(" ", "")
            if artist_norm_compact and host_artist_norm:
                if artist_norm_compact == host_artist_norm:
                    score += 1.5
                elif artist_norm_compact in host_artist_norm or host_artist_norm in artist_norm_compact:
                    score += 0.75
            score += max(0.0, 0.25 - (idx * 0.01))
            return score

        if search_page:
            matches = re.findall(
                r'href="(https?://[^"]*bandcamp\.com/album/([^"?#]+))',
                search_page,
                flags=re.IGNORECASE,
            )
            for idx, (full_url, _slug) in enumerate(matches):
                url = full_url.split("?")[0].split("#")[0].strip()
                if not url:
                    continue
                candidates.append((_score_bandcamp_candidate(url, idx), url))

        if not candidates and not bool(allow_web_fallback):
            return None

        if not candidates and bool(allow_web_fallback):
            query = f'site:bandcamp.com/album "{artist_name}" "{album_title}"'
            web_results = _web_search_serper(
                query,
                num=8,
                context={
                    "query_kind": "bandcamp_lookup",
                    "artist": artist_name,
                    "album": album_title,
                },
                analysis_type="bandcamp_lookup",
            )
            if web_results:
                logging.info(
                    "[Bandcamp] direct search unavailable for %r / %r; using web fallback (%d result(s))",
                    artist_name,
                    album_title,
                    len(web_results),
                )
                for idx, item in enumerate(web_results):
                    url = str((item or {}).get("link") or "").split("?", 1)[0].split("#", 1)[0].strip()
                    if not url or "bandcamp.com/album/" not in url.lower():
                        continue
                    candidates.append((_score_bandcamp_candidate(url, idx, base_score=0.5), url))

        if not candidates:
            return None

        ranked: Dict[str, float] = {}
        for score, url in candidates:
            ranked[url] = max(score, ranked.get(url, float("-inf")))
        ranked_urls = [u for u, _s in sorted(ranked.items(), key=lambda item: item[1], reverse=True)]
        ranked_scores = [float(s) for _u, s in sorted(ranked.items(), key=lambda item: item[1], reverse=True)]

        candidate_limit = _bandcamp_lookup_candidate_cap(ranked_scores)

        strict_payload: Optional[dict] = None
        best_payload: Optional[dict] = None
        best_score = -1.0
        for idx, album_url in enumerate(ranked_urls[:max(1, candidate_limit)]):
            payload = _parse_album_page(album_url)
            if not payload:
                continue
            strict_ok, _strict_reason = _strict_identity_match_details(
                local_artist=artist_name,
                local_title=album_title,
                candidate_artist=payload.get("artist_name") or "",
                candidate_title=payload.get("title") or "",
            )
            if strict_ok:
                strict_payload = payload
                break
            title_score = _provider_identity_text_score(album_title, str(payload.get("title") or ""))
            artist_score = _provider_identity_text_score(artist_name, str(payload.get("artist_name") or ""))
            combined = (title_score * 0.6) + (artist_score * 0.4)
            if combined > best_score:
                best_score = combined
                best_payload = payload
            if idx == 0 and combined >= 0.96 and title_score >= 0.95 and artist_score >= 0.90:
                break

        if strict_payload is not None:
            return strict_payload
        return best_payload
    except Exception as e:
        logging.warning("Bandcamp fetch failed for %s / %s: %s", artist_name, album_title, e)
        return None
