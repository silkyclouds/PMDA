"""Wikipedia/Wikidata helper functions used by artist profile enrichment."""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote, unquote, urlparse

import requests

_USER_AGENT = "PMDA/0.7.5 (self-hosted music library; https://github.com/silkyclouds/PMDA)"


def commons_file_path_url(filename: str) -> str:
    clean = str(filename or "").strip()
    if not clean:
        return ""
    return f"https://commons.wikimedia.org/wiki/Special:FilePath/{quote(clean, safe='')}"


def fetch_wikidata_media_url(entity_id: str, preferred_props: tuple[str, ...] = ("P18", "P154")) -> str:
    qid = str(entity_id or "").strip()
    if not qid:
        return ""
    headers = {"User-Agent": _USER_AGENT}
    try:
        resp = requests.get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbgetentities",
                "ids": qid,
                "props": "claims",
                "format": "json",
            },
            headers=headers,
            timeout=10,
        )
        if resp.status_code != 200:
            return ""
        data = resp.json() if resp.content else {}
        entity = (((data or {}).get("entities") or {}).get(qid) or {}) if isinstance(data, dict) else {}
        claims = entity.get("claims") or {}
        for prop in preferred_props:
            claim_list = claims.get(prop) or []
            if not isinstance(claim_list, list):
                continue
            for claim in claim_list:
                if not isinstance(claim, dict):
                    continue
                datavalue = (((claim.get("mainsnak") or {}).get("datavalue") or {}).get("value"))
                if isinstance(datavalue, str) and datavalue.strip():
                    return commons_file_path_url(datavalue.strip())
        return ""
    except Exception:
        return ""


def fetch_wikipedia_page_metadata(title: str, lang: str = "en", thumb_px: int = 640) -> dict[str, str]:
    t = (title or "").strip()
    l = (lang or "en").strip().lower() or "en"
    thumb_px = max(64, min(1600, int(thumb_px or 640)))
    if not t:
        return {}
    api_url = f"https://{l}.wikipedia.org/w/api.php"
    headers = {"User-Agent": _USER_AGENT}
    try:
        resp = requests.get(
            api_url,
            params={
                "action": "query",
                "prop": "pageimages|pageprops|info",
                "piprop": "thumbnail|original",
                "pithumbsize": thumb_px,
                "inprop": "url",
                "titles": t,
                "redirects": 1,
                "format": "json",
                "utf8": 1,
            },
            headers=headers,
            timeout=10,
        )
        if resp.status_code != 200:
            return {}
        data = resp.json()
        pages = ((data.get("query") or {}).get("pages") or {}) if isinstance(data, dict) else {}
        for _pid, page in (pages or {}).items():
            if not isinstance(page, dict):
                continue
            original = page.get("original") or {}
            thumb = page.get("thumbnail") or {}
            pageprops = page.get("pageprops") or {}
            return {
                "fullurl": str(page.get("fullurl") or "").strip(),
                "original": str((original.get("source") if isinstance(original, dict) else "") or "").strip(),
                "thumbnail": str((thumb.get("source") if isinstance(thumb, dict) else "") or "").strip(),
                "wikibase_item": str((pageprops.get("wikibase_item") if isinstance(pageprops, dict) else "") or "").strip(),
            }
        return {}
    except Exception:
        return {}


def wikipedia_title_from_fullurl(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        path = unquote(parsed.path or "")
        if "/wiki/" in path:
            return " ".join(path.split("/wiki/", 1)[1].replace("_", " ").split()).strip()
    except Exception:
        return ""
    return ""


def fetch_wikipedia_pageimage(title: str, lang: str = "en", thumb_px: int = 640) -> str:
    """
    Return a Wikipedia lead thumbnail/original URL for a page title, or "" on failure.
    Falls back to Wikidata media claims and finally article-level og:image.
    """
    meta = fetch_wikipedia_page_metadata(title, lang=lang, thumb_px=thumb_px)
    if not meta:
        return ""
    for key in ("original", "thumbnail"):
        src = str(meta.get(key) or "").strip()
        if src:
            return src
    wikidata_url = fetch_wikidata_media_url(str(meta.get("wikibase_item") or "").strip())
    if wikidata_url:
        return wikidata_url
    page_url = str(meta.get("fullurl") or "").strip()
    if page_url:
        headers = {"User-Agent": _USER_AGENT}
        try:
            html_resp = requests.get(page_url, headers=headers, timeout=10, allow_redirects=True)
            if html_resp.status_code == 200 and "text/html" in (html_resp.headers.get("content-type") or "").lower():
                match = re.search(
                    r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
                    html_resp.text,
                    re.IGNORECASE,
                )
                if not match:
                    match = re.search(
                        r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:image["\']',
                        html_resp.text,
                        re.IGNORECASE,
                    )
                if match:
                    src = match.group(1).strip()
                    if src:
                        return src
        except Exception:
            pass
    return ""


def fetch_wikipedia_intro_extract(title: str, lang: str = "en") -> tuple[str, str, str]:
    """
    Return (extract, page_url, description) for a Wikipedia page title, or ("", "", "") on failure.
    Uses the MediaWiki API with plaintext extracts.
    """
    t = (title or "").strip()
    l = (lang or "en").strip().lower() or "en"
    if not t:
        return "", "", ""
    api_url = f"https://{l}.wikipedia.org/w/api.php"
    headers = {"User-Agent": _USER_AGENT}
    try:
        resp = requests.get(
            api_url,
            params={
                "action": "query",
                "prop": "extracts|description",
                "explaintext": 1,
                "exintro": 1,
                "redirects": 1,
                "titles": t,
                "format": "json",
                "utf8": 1,
            },
            headers=headers,
            timeout=10,
        )
        if resp.status_code != 200:
            return "", "", ""
        data = resp.json()
        pages = ((data.get("query") or {}).get("pages") or {}) if isinstance(data, dict) else {}
        extract = ""
        description = ""
        canonical_title = t
        for _pid, page in (pages or {}).items():
            if not isinstance(page, dict):
                continue
            canonical_title = (page.get("title") or canonical_title).strip()
            extract = (page.get("extract") or "").strip()
            description = (page.get("description") or "").strip()
            break
        extract = re.sub(r"\s+", " ", extract).strip()
        if not extract:
            return "", "", ""
        low = extract.lower()
        if "may refer to" in low and len(extract) < 220:
            return "", "", ""
        if "disambiguation page" in (description or "").lower():
            return "", "", ""
        page_url = f"https://{l}.wikipedia.org/wiki/{quote(canonical_title.replace(' ', '_'), safe='')}"
        return extract, page_url, description
    except Exception:
        return "", "", ""


def artist_cached_image_provider_is_provider_first(
    *,
    provider: str = "",
    image_url: str = "",
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> bool:
    provider_low = str(provider or "").strip().lower()
    url_low = str(image_url or "").strip().lower()
    if not provider_low:
        return False
    if provider_low in {"bandcamp", "discogs", "lastfm", "fanart", "audiodb"}:
        return True
    if provider_low in {"musicbrainz", "musicbrainz_url"}:
        if url_low and any(tok in url_low for tok in ("wikimedia.org", "wikipedia.org", "wikidata.org")):
            return False
        return True
    return False


def dedupe_keep_order(items: list[str] | tuple[str, ...]) -> list[str]:
    out: list[str] = []
    seen = set()
    for item in items:
        key = (item or "").strip()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out
