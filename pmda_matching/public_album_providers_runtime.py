"""Runtime-owned public album provider page parsing and lookup helpers."""
from __future__ import annotations

import html
import json
import logging
import re
from typing import Any, Optional
from urllib.parse import quote, urlparse

_EXTRACTED_NAMES = {
    '_provider_album_search_candidate_score',
    '_public_album_provider_headers',
    '_extract_json_ld_nodes',
    '_json_ld_type_matches',
    '_json_ld_music_album_node',
    '_json_ld_artist_name',
    '_json_ld_tracklist',
    '_provider_album_meta_fallback',
    '_parse_public_album_page_payload',
    '_spotify_album_page_urls',
    '_qobuz_album_page_urls',
    '_tidal_album_page_urls',
    '_itunes_cover_url_candidates',
    '_fetch_itunes_album_info',
    '_fetch_deezer_album_info',
    '_fetch_spotify_album_info',
    '_fetch_qobuz_album_info',
    '_fetch_tidal_album_info',
    '_fetch_audiodb_album_info',
    '_fetch_lastfm_album_info',
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

def _provider_album_search_candidate_score(
    local_artist: str,
    local_title: str,
    candidate_artist: str,
    candidate_title: str,
) -> float:
    title_score = float(
        _provider_identity_album_score(
            local_title,
            candidate_title,
            artist_hints=[local_artist, candidate_artist],
        )
    )
    artist_score = float(_provider_identity_text_score(local_artist, candidate_artist))
    score = (title_score * 0.6) + (artist_score * 0.4)
    if title_score >= 0.98 and artist_score >= 0.98:
        score += 0.08
    elif title_score >= 0.92 and artist_score >= 0.88:
        score += 0.04
    return max(0.0, min(1.0, score))

def _public_album_provider_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (PMDA metadata lookup)",
        "Accept-Language": "en-US,en;q=0.9",
    }

def _extract_json_ld_nodes(raw_html: str) -> list[dict[str, Any]]:
    if not raw_html:
        return []
    out: list[dict[str, Any]] = []

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            out.append(node)
            for child in node.values():
                _walk(child)
        elif isinstance(node, list):
            for child in node:
                _walk(child)

    for match in re.finditer(
        r'(?is)<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        raw_html,
    ):
        blob = html.unescape(str(match.group(1) or "").strip())
        if not blob:
            continue
        try:
            parsed = json.loads(blob)
        except Exception:
            continue
        _walk(parsed)
    return out

def _json_ld_type_matches(node: dict[str, Any], target: str) -> bool:
    if not isinstance(node, dict):
        return False
    expected = str(target or "").strip().lower()
    if not expected:
        return False
    raw_type = node.get("@type")
    values = raw_type if isinstance(raw_type, list) else [raw_type]
    for value in values:
        if str(value or "").strip().lower() == expected:
            return True
    return False

def _json_ld_music_album_node(raw_html: str) -> dict[str, Any] | None:
    for node in _extract_json_ld_nodes(raw_html):
        if _json_ld_type_matches(node, "MusicAlbum"):
            return node
    return None

def _json_ld_artist_name(node: Any) -> str:
    if isinstance(node, str):
        return str(node).strip()
    if isinstance(node, dict):
        return str(node.get("name") or node.get("@id") or "").strip()
    if isinstance(node, list):
        for item in node:
            name = _json_ld_artist_name(item)
            if name:
                return name
    return ""

def _json_ld_tracklist(node: dict[str, Any] | None) -> list[str]:
    if not isinstance(node, dict):
        return []
    raw_tracks = (
        node.get("track")
        or node.get("tracks")
        or node.get("itemListElement")
        or []
    )
    if isinstance(raw_tracks, dict):
        raw_tracks = [raw_tracks]
    out: list[str] = []
    for item in raw_tracks if isinstance(raw_tracks, list) else []:
        if isinstance(item, str):
            title = item.strip()
        elif isinstance(item, dict):
            inner = item.get("item") if isinstance(item.get("item"), dict) else item
            title = str(
                inner.get("name")
                or item.get("name")
                or inner.get("title")
                or item.get("title")
                or ""
            ).strip()
        else:
            title = ""
        if title:
            out.append(title)
    return out

def _provider_album_meta_fallback(provider: str, og_title: str, og_desc: str) -> tuple[str, str]:
    p = _normalize_identity_provider(provider)
    title = ""
    artist = ""
    if p == "spotify":
        match = re.match(r"^(.*?)\s*-\s*Album by\s*(.*?)\s*\|\s*Spotify\s*$", og_title, flags=re.IGNORECASE)
        if match:
            title = str(match.group(1) or "").strip()
            artist = str(match.group(2) or "").strip()
    elif p == "qobuz":
        match = re.match(r"^(.*?),\s*(.*?)\s*-\s*Qobuz\s*$", og_title, flags=re.IGNORECASE)
        if match:
            title = str(match.group(1) or "").strip()
            artist = str(match.group(2) or "").strip()
    elif p == "tidal":
        match = re.match(r"^(.*?)\s*-\s*(.*?)\s*\|\s*TIDAL\s*$", og_title, flags=re.IGNORECASE)
        if match:
            artist = str(match.group(1) or "").strip()
            title = str(match.group(2) or "").strip()
    if (not title or not artist) and og_desc:
        desc_match = re.search(r"album\s*[·|-]\s*(.*?)\s*[·|-]\s*(\d{4})?", og_desc, flags=re.IGNORECASE)
        if desc_match and not artist:
            artist = str(desc_match.group(1) or "").strip()
    return title, artist

def _parse_public_album_page_payload(
    provider: str,
    *,
    raw_html: str,
    page_url: str,
    fallback_artist: str,
    fallback_title: str,
    album_id: str = "",
) -> dict[str, Any] | None:
    if not raw_html:
        return None
    og_title = _review_page_meta_content(raw_html, "og:title", "twitter:title", "title")
    og_desc = _review_page_meta_content(raw_html, "og:description", "twitter:description", "description")
    og_image = _review_page_meta_content(raw_html, "og:image", "twitter:image")
    music_album = _json_ld_music_album_node(raw_html) or {}
    title = str(music_album.get("name") or "").strip()
    artist_name = _json_ld_artist_name(music_album.get("byArtist") or music_album.get("artist"))
    if not title or not artist_name:
        parsed_title, parsed_artist = _provider_album_meta_fallback(provider, og_title, og_desc)
        title = title or parsed_title
        artist_name = artist_name or parsed_artist
    if not title:
        title = str(fallback_title or "").strip()
    if not artist_name:
        artist_name = str(fallback_artist or "").strip()
    if not title or not artist_name:
        return None
    release_date = str(
        music_album.get("datePublished")
        or music_album.get("dateCreated")
        or music_album.get("releaseDate")
        or ""
    ).strip()
    payload: dict[str, Any] = {
        "title": title,
        "artist_name": artist_name,
        "artist": artist_name,
        "year": release_date[:4] if release_date[:4].isdigit() else release_date,
        "cover_url": str(og_image or music_album.get("image") or "").strip(),
        "tracklist": _json_ld_tracklist(music_album),
        "url": str(page_url or "").strip(),
        "identity_scope": f"{_normalize_identity_provider(provider) or provider}_album",
    }
    if album_id:
        payload["album_id"] = str(album_id or "").strip()
    return payload

def _spotify_album_page_urls(raw_html: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for match in re.finditer(r'(?i)(?:href=|https://open\.spotify\.com)/album/([A-Za-z0-9]+)', raw_html or ""):
        album_id = str(match.group(1) or "").strip()
        if not album_id or album_id in seen:
            continue
        seen.add(album_id)
        out.append((album_id, f"https://open.spotify.com/album/{album_id}"))
    return out

def _qobuz_album_page_urls(raw_html: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for match in re.finditer(r'(?i)(/[^"\'>\s]*/album/[^"\'>\s]+)', raw_html or ""):
        rel = str(match.group(1) or "").strip()
        if not rel or "/album/" not in rel:
            continue
        if rel in seen:
            continue
        seen.add(rel)
        out.append((rel.rsplit("/", 1)[-1], f"https://www.qobuz.com{rel}"))
    return out

def _tidal_album_page_urls(raw_html: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for match in re.finditer(r'(?i)(?:/browse)?/album/(\d+)', raw_html or ""):
        album_id = str(match.group(1) or "").strip()
        if not album_id or album_id in seen:
            continue
        seen.add(album_id)
        out.append((album_id, f"https://tidal.com/browse/album/{album_id}"))
    return out

def _itunes_cover_url_candidates(url: str) -> list[str]:
    raw = str(url or "").strip()
    if not raw:
        return []
    candidates = [raw]
    upgraded = re.sub(r"/\d+x\d+(?:bb)?[-\w]*\.(jpg|jpeg|png|webp)", r"/1200x1200bb.\1", raw, flags=re.IGNORECASE)
    if upgraded != raw:
        candidates.append(upgraded)
    upgraded = re.sub(r"/\d+x\d+(?:bb)?", "/1200x1200bb", raw, flags=re.IGNORECASE)
    if upgraded != raw:
        candidates.append(upgraded)
    return _dedupe_keep_order(candidates)

def _fetch_itunes_album_info(artist_name: str, album_title: str) -> Optional[dict]:
    if not USE_ITUNES:
        return None
    query = " ".join(part for part in [str(artist_name or "").strip(), str(album_title or "").strip()] if part).strip()
    if not query:
        return None
    try:
        search_resp = _provider_gateway_http_get(
            "itunes",
            "https://itunes.apple.com/search",
            params={
                "term": query,
                "entity": "album",
                "limit": 12,
                "country": "us",
            },
            timeout=(5, 15),
            context="itunes album search",
            cache_ttl_sec=60 * 60 * 12,
        )
        if search_resp.status_code != 200:
            return None
        search_data = search_resp.json() or {}
        results = search_data.get("results") if isinstance(search_data, dict) else []
        if not isinstance(results, list):
            return None
        best_item: dict[str, Any] | None = None
        best_score = 0.0
        for item in results[:12]:
            if not isinstance(item, dict):
                continue
            candidate_title = str(item.get("collectionName") or item.get("trackName") or item.get("title") or "").strip()
            candidate_artist = str(item.get("artistName") or item.get("artist") or "").strip()
            if not candidate_title or not candidate_artist:
                continue
            score = _provider_album_search_candidate_score(artist_name, album_title, candidate_artist, candidate_title)
            if score > best_score:
                best_score = score
                best_item = item
        if not isinstance(best_item, dict) or best_score < 0.64:
            return None
        collection_id = str(best_item.get("collectionId") or "").strip()
        tracklist: list[str] = []
        if collection_id:
            lookup_resp = _provider_gateway_http_get(
                "itunes",
                "https://itunes.apple.com/lookup",
                params={"id": collection_id, "entity": "song", "country": "us"},
                timeout=(5, 15),
                context="itunes album lookup",
                cache_ttl_sec=60 * 60 * 12,
            )
            if lookup_resp.status_code == 200:
                lookup_data = lookup_resp.json() or {}
                lookup_results = lookup_data.get("results") if isinstance(lookup_data, dict) else []
                if isinstance(lookup_results, list):
                    songs: list[tuple[int, int, int, str]] = []
                    for idx, row in enumerate(lookup_results):
                        if not isinstance(row, dict):
                            continue
                        if str(row.get("wrapperType") or "").strip().lower() != "track":
                            continue
                        title = str(row.get("trackName") or row.get("name") or "").strip()
                        if not title:
                            continue
                        disc_num = _parse_int_loose(row.get("discNumber"), 1) or 1
                        track_num = _parse_int_loose(row.get("trackNumber"), idx + 1) or (idx + 1)
                        songs.append((disc_num, track_num, idx, title))
                    songs.sort(key=lambda item: (item[0], item[1], item[2]))
                    tracklist = [item[3] for item in songs]
        cover_url = str(
            best_item.get("artworkUrl100")
            or best_item.get("artworkUrl60")
            or best_item.get("artworkUrl30")
            or ""
        ).strip()
        release_date = str(best_item.get("releaseDate") or "").strip()
        return {
            "title": str(best_item.get("collectionName") or best_item.get("trackName") or album_title or "").strip(),
            "artist_name": str(best_item.get("artistName") or artist_name or "").strip(),
            "artist": str(best_item.get("artistName") or artist_name or "").strip(),
            "year": release_date[:4] if release_date[:4].isdigit() else release_date,
            "cover_url": cover_url,
            "cover_candidates": _itunes_cover_url_candidates(cover_url),
            "tracklist": tracklist,
            "collection_id": collection_id,
            "url": str(best_item.get("collectionViewUrl") or best_item.get("artistViewUrl") or "").strip(),
            "label": str(best_item.get("copyright") or best_item.get("collectionCensoredName") or "").strip() or None,
            "identity_scope": "itunes_collection",
        }
    except Exception as e:
        logging.warning("iTunes fetch failed for %s / %s: %s", artist_name, album_title, e)
        return None

def _fetch_deezer_album_info(artist_name: str, album_title: str) -> Optional[dict]:
    if not USE_DEEZER:
        return None
    query = f'artist:"{str(artist_name or "").strip()}" album:"{str(album_title or "").strip()}"'.strip()
    if not query:
        return None
    try:
        search_resp = _provider_gateway_http_get(
            "deezer",
            "https://api.deezer.com/search/album",
            params={"q": query, "limit": 12},
            timeout=(5, 15),
            context="deezer album search",
            cache_ttl_sec=60 * 60 * 12,
        )
        if search_resp.status_code != 200:
            return None
        search_data = search_resp.json() or {}
        results = search_data.get("data") if isinstance(search_data, dict) else []
        if not isinstance(results, list):
            return None
        best_item: dict[str, Any] | None = None
        best_score = 0.0
        for item in results[:12]:
            if not isinstance(item, dict):
                continue
            candidate_title = str(item.get("title") or "").strip()
            artist_block = item.get("artist") if isinstance(item.get("artist"), dict) else {}
            candidate_artist = str(artist_block.get("name") or item.get("artist_name") or "").strip()
            if not candidate_title or not candidate_artist:
                continue
            score = _provider_album_search_candidate_score(artist_name, album_title, candidate_artist, candidate_title)
            if score > best_score:
                best_score = score
                best_item = item
        if not isinstance(best_item, dict) or best_score < 0.64:
            return None
        album_id = str(best_item.get("id") or "").strip()
        detail = best_item
        if album_id:
            detail_resp = _provider_gateway_http_get(
                "deezer",
                f"https://api.deezer.com/album/{quote(album_id, safe='')}",
                timeout=(5, 15),
                context="deezer album lookup",
                cache_ttl_sec=60 * 60 * 12,
            )
            if detail_resp.status_code == 200:
                detail_json = detail_resp.json() or {}
                if isinstance(detail_json, dict) and not detail_json.get("error"):
                    detail = detail_json
        artist_block = detail.get("artist") if isinstance(detail.get("artist"), dict) else {}
        tracklist: list[str] = []
        tracks_block = detail.get("tracks") if isinstance(detail.get("tracks"), dict) else {}
        track_rows = tracks_block.get("data") if isinstance(tracks_block, dict) else []
        if isinstance(track_rows, list):
            items: list[tuple[int, int, int, str]] = []
            for idx, row in enumerate(track_rows):
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title") or row.get("title_short") or "").strip()
                if not title:
                    continue
                disc_num = _parse_int_loose(row.get("disk_number"), 1) or 1
                track_num = _parse_int_loose(row.get("track_position"), idx + 1) or (idx + 1)
                items.append((disc_num, track_num, idx, title))
            items.sort(key=lambda item: (item[0], item[1], item[2]))
            tracklist = [item[3] for item in items]
        genres_block = detail.get("genres") if isinstance(detail.get("genres"), dict) else {}
        genre_rows = genres_block.get("data") if isinstance(genres_block, dict) else []
        genre_names = [
            str(item.get("name") or "").strip()
            for item in (genre_rows or [])
            if isinstance(item, dict) and str(item.get("name") or "").strip()
        ]
        release_date = str(detail.get("release_date") or best_item.get("release_date") or "").strip()
        cover_url = str(
            detail.get("cover_xl")
            or detail.get("cover_big")
            or detail.get("cover_medium")
            or detail.get("cover")
            or best_item.get("cover_xl")
            or best_item.get("cover_big")
            or best_item.get("cover_medium")
            or best_item.get("cover")
            or ""
        ).strip()
        return {
            "title": str(detail.get("title") or best_item.get("title") or album_title or "").strip(),
            "artist_name": str(artist_block.get("name") or artist_name or "").strip(),
            "artist": str(artist_block.get("name") or artist_name or "").strip(),
            "year": release_date[:4] if release_date[:4].isdigit() else release_date,
            "cover_url": cover_url,
            "tracklist": tracklist,
            "album_id": album_id,
            "url": str(detail.get("link") or best_item.get("link") or "").strip(),
            "label": str(detail.get("label") or "").strip() or None,
            "tags": genre_names,
            "identity_scope": "deezer_album",
        }
    except Exception as e:
        logging.warning("Deezer fetch failed for %s / %s: %s", artist_name, album_title, e)
        return None

def _fetch_spotify_album_info(artist_name: str, album_title: str) -> Optional[dict]:
    if not USE_SPOTIFY:
        return None
    query = " ".join(part for part in [str(artist_name or "").strip(), str(album_title or "").strip()] if part).strip()
    if not query:
        return None
    try:
        search_resp = _provider_gateway_http_get(
            "spotify",
            f"https://open.spotify.com/search/{quote(query, safe='')}/albums",
            headers=_public_album_provider_headers(),
            timeout=(5, 15),
            context="spotify album search",
            cache_ttl_sec=60 * 60 * 12,
        )
        if search_resp.status_code != 200 or not search_resp.text:
            return None
        best_payload: dict[str, Any] | None = None
        best_score = 0.0
        for album_id, album_url in _spotify_album_page_urls(search_resp.text)[:5]:
            try:
                detail_resp = _provider_gateway_http_get(
                    "spotify",
                    album_url,
                    headers=_public_album_provider_headers(),
                    timeout=(5, 15),
                    context="spotify album detail",
                    cache_ttl_sec=60 * 60 * 12,
                )
            except Exception:
                continue
            if detail_resp.status_code != 200 or not detail_resp.text:
                continue
            payload = _parse_public_album_page_payload(
                "spotify",
                raw_html=detail_resp.text,
                page_url=str(detail_resp.url or album_url),
                fallback_artist=artist_name,
                fallback_title=album_title,
                album_id=album_id,
            )
            if not isinstance(payload, dict):
                continue
            score = _provider_album_search_candidate_score(
                artist_name,
                album_title,
                str(payload.get("artist_name") or ""),
                str(payload.get("title") or ""),
            )
            if score > best_score:
                best_score = score
                best_payload = payload
        if not isinstance(best_payload, dict) or best_score < 0.64:
            return None
        return best_payload
    except Exception as e:
        logging.warning("Spotify fetch failed for %s / %s: %s", artist_name, album_title, e)
        return None

def _fetch_qobuz_album_info(artist_name: str, album_title: str) -> Optional[dict]:
    if not USE_QOBUZ:
        return None
    query = " ".join(part for part in [str(artist_name or "").strip(), str(album_title or "").strip()] if part).strip()
    if not query:
        return None
    try:
        search_resp = None
        search_endpoints = [
            (
                f"https://www.qobuz.com/us-en/search/albums/{quote(query, safe='')}",
                None,
                "qobuz album search us-en",
            ),
            (
                "https://www.qobuz.com/search",
                {"query": query},
                "qobuz album search legacy",
            ),
        ]
        for search_url, search_params, search_context in search_endpoints:
            resp = _provider_gateway_http_get(
                "qobuz",
                search_url,
                params=search_params,
                headers=_public_album_provider_headers(),
                timeout=(5, 15),
                context=search_context,
                cache_ttl_sec=60 * 60 * 12,
            )
            if resp.status_code == 200 and resp.text:
                search_resp = resp
                break
        if search_resp is None:
            return None
        best_payload: dict[str, Any] | None = None
        best_score = 0.0
        for album_id, album_url in _qobuz_album_page_urls(search_resp.text)[:5]:
            try:
                detail_resp = _provider_gateway_http_get(
                    "qobuz",
                    album_url,
                    headers=_public_album_provider_headers(),
                    timeout=(5, 15),
                    context="qobuz album detail",
                    cache_ttl_sec=60 * 60 * 12,
                )
            except Exception:
                continue
            if detail_resp.status_code != 200 or not detail_resp.text:
                continue
            payload = _parse_public_album_page_payload(
                "qobuz",
                raw_html=detail_resp.text,
                page_url=str(detail_resp.url or album_url),
                fallback_artist=artist_name,
                fallback_title=album_title,
                album_id=album_id,
            )
            if not isinstance(payload, dict):
                continue
            score = _provider_album_search_candidate_score(
                artist_name,
                album_title,
                str(payload.get("artist_name") or ""),
                str(payload.get("title") or ""),
            )
            if score > best_score:
                best_score = score
                best_payload = payload
        if not isinstance(best_payload, dict) or best_score < 0.64:
            return None
        return best_payload
    except Exception as e:
        logging.warning("Qobuz fetch failed for %s / %s: %s", artist_name, album_title, e)
        return None

def _fetch_tidal_album_info(artist_name: str, album_title: str) -> Optional[dict]:
    if not USE_TIDAL:
        return None
    query = " ".join(part for part in [str(artist_name or "").strip(), str(album_title or "").strip()] if part).strip()
    if not query:
        return None
    try:
        search_resp = _provider_gateway_http_get(
            "tidal",
            "https://tidal.com/search",
            params={"q": query},
            headers=_public_album_provider_headers(),
            timeout=(5, 15),
            context="tidal album search",
            cache_ttl_sec=60 * 60 * 12,
        )
        if search_resp.status_code != 200 or not search_resp.text:
            return None
        best_payload: dict[str, Any] | None = None
        best_score = 0.0
        for album_id, album_url in _tidal_album_page_urls(search_resp.text)[:3]:
            try:
                detail_resp = _provider_gateway_http_get(
                    "tidal",
                    album_url,
                    headers=_public_album_provider_headers(),
                    timeout=(5, 15),
                    context="tidal album detail",
                    cache_ttl_sec=60 * 60 * 12,
                )
            except Exception:
                continue
            if detail_resp.status_code != 200 or not detail_resp.text:
                continue
            payload = _parse_public_album_page_payload(
                "tidal",
                raw_html=detail_resp.text,
                page_url=str(detail_resp.url or album_url),
                fallback_artist=artist_name,
                fallback_title=album_title,
                album_id=album_id,
            )
            if not isinstance(payload, dict):
                continue
            score = _provider_album_search_candidate_score(
                artist_name,
                album_title,
                str(payload.get("artist_name") or ""),
                str(payload.get("title") or ""),
            )
            if score > best_score:
                best_score = score
                best_payload = payload
        if not isinstance(best_payload, dict) or best_score < 0.64:
            return None
        return best_payload
    except Exception as e:
        logging.warning("TIDAL fetch failed for %s / %s: %s", artist_name, album_title, e)
        return None

def _fetch_audiodb_album_info(artist_name: str, album_title: str) -> Optional[dict]:
    api_key = (getattr(sys.modules[__name__], "THEAUDIODB_API_KEY", "") or "").strip()
    if not api_key:
        return None
    artist_query = str(artist_name or "").strip()
    title_query = str(album_title or "").strip()
    if not artist_query or not title_query:
        return None
    try:
        resp = _provider_gateway_http_get(
            "audiodb",
            f"https://www.theaudiodb.com/api/v1/json/{quote(api_key, safe='')}/searchalbum.php",
            params={"s": artist_query, "a": title_query},
            headers=_public_album_provider_headers(),
            timeout=(5, 15),
            context="audiodb album search",
            cache_ttl_sec=60 * 60 * 12,
        )
        if resp.status_code != 200:
            return None
        data = resp.json() or {}
        albums = data.get("album") if isinstance(data, dict) else []
        if not isinstance(albums, list):
            return None
        best_item: dict[str, Any] | None = None
        best_score = 0.0
        for item in albums[:10]:
            if not isinstance(item, dict):
                continue
            candidate_title = str(item.get("strAlbum") or "").strip()
            candidate_artist = str(item.get("strArtist") or "").strip()
            if not candidate_title or not candidate_artist:
                continue
            score = _provider_album_search_candidate_score(artist_query, title_query, candidate_artist, candidate_title)
            if score > best_score:
                best_score = score
                best_item = item
        if not isinstance(best_item, dict) or best_score < 0.64:
            return None
        year = str(best_item.get("intYearReleased") or best_item.get("strReleased") or "").strip()
        album_id = str(best_item.get("idAlbum") or "").strip()
        return {
            "title": str(best_item.get("strAlbum") or title_query).strip(),
            "artist_name": str(best_item.get("strArtist") or artist_query).strip(),
            "artist": str(best_item.get("strArtist") or artist_query).strip(),
            "year": year[:4] if year[:4].isdigit() else year,
            "cover_url": str(best_item.get("strAlbumThumb") or best_item.get("strAlbumThumbHQ") or "").strip(),
            "album_id": album_id,
            "url": _provider_reference_link(provider="audiodb", ref=album_id, artist_name=artist_query, album_title=title_query) or "",
            "label": str(best_item.get("strLabel") or "").strip() or None,
            "tags": [
                str(value or "").strip()
                for value in (
                    best_item.get("strGenre"),
                    best_item.get("strStyle"),
                    best_item.get("strMood"),
                )
                if str(value or "").strip()
            ],
            "identity_scope": "audiodb_album",
        }
    except Exception as e:
        logging.debug("TheAudioDB album fetch failed for %s / %s: %s", artist_name, album_title, e)
        return None

def _fetch_lastfm_album_info(artist_name: str, album_title: str, mbid: Optional[str] = None) -> Optional[dict]:
    """
    Call Last.fm album.getInfo. Returns dict with cover_url, toptags (list of str), title, artist, or None.
    """
    if not USE_LASTFM:
        return None
    api_key = (getattr(sys.modules[__name__], "LASTFM_API_KEY", "") or "").strip()
    if not api_key:
        return None
    api_root = "https://ws.audioscrobbler.com/2.0/"

    def _lastfm_get_json(params: dict) -> Optional[dict]:
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                resp = _provider_gateway_http_get(
                    "lastfm",
                    api_root,
                    params=params,
                    timeout=(5, 15),
                    context=f"lastfm album {params.get('method')}",
                    cache_ttl_sec=60 * 60 * 6,
                )
                if resp.status_code == 429:
                    retry_after = 2.0
                    try:
                        retry_after = max(1.0, float(resp.headers.get("Retry-After") or 2.0))
                    except Exception:
                        retry_after = 2.0
                    if attempt == 0:
                        time.sleep(min(retry_after, 6.0))
                        continue
                if resp.status_code != 200:
                    return None
                return resp.json()
            except requests.exceptions.RequestException as e:
                last_error = e
                if _provider_is_name_resolution_failure(e):
                    break
                if attempt == 0:
                    time.sleep(0.75)
                    continue
        # Stdlib fallback can succeed when requests/urllib3 trips over transient socket routing issues.
        if _provider_is_name_resolution_failure(last_error):
            raise last_error
        try:
            from urllib.parse import urlencode
            from urllib.request import urlopen

            url = f"{api_root}?{urlencode(params)}"
            with urlopen(url, timeout=15) as resp:
                raw = resp.read()
            if not raw:
                return None
            return json.loads(raw.decode("utf-8", errors="replace"))
        except Exception as e:
            if last_error is not None:
                raise last_error
            raise e

    try:
        params = {"method": "album.getInfo", "artist": artist_name, "album": album_title, "api_key": api_key, "format": "json"}
        if mbid:
            params["mbid"] = mbid
        data = _lastfm_get_json(params)
        if not data:
            return None
        if data.get("error") and data.get("error") != 0:
            search_params = {"method": "album.search", "album": f"{artist_name} {album_title}".strip(), "api_key": api_key, "format": "json"}
            search_data = _lastfm_get_json(search_params)
            if search_data:
                matches = (search_data.get("results") or {}).get("albummatches") or {}
                album_list = matches.get("album") or []
                if isinstance(album_list, dict):
                    album_list = [album_list]
                if album_list:
                    picked = _lastfm_pick_search_candidate(artist_name, album_title, album_list)
                    if picked is None:
                        return None
                    search_artist, search_album = picked
                    params2 = {"method": "album.getInfo", "artist": search_artist, "album": search_album, "api_key": api_key, "format": "json"}
                    data2 = _lastfm_get_json(params2)
                    if data2:
                        data = data2
                        if not (data.get("error") and data.get("error") != 0):
                            album_title = search_album
                            artist_name = search_artist
            if data.get("error") and data.get("error") != 0:
                return None
        album = data.get("album") or {}
        images = album.get("image") or []
        cover_url = None
        size_rank = {
            "small": 1,
            "medium": 2,
            "large": 3,
            "extralarge": 4,
            "mega": 5,
        }
        best_rank = -1
        for img in images:
            if not isinstance(img, dict):
                continue
            url = (img.get("#text") or "").strip()
            if not url:
                continue
            rank = size_rank.get((img.get("size") or "").strip().lower(), 0)
            if rank >= best_rank:
                best_rank = rank
                cover_url = url
        if not cover_url and images:
            try:
                cover_url = (images[-1].get("#text") or "").strip() if isinstance(images[-1], dict) else None
            except Exception:
                cover_url = None
        toptags = []
        for t in (album.get("toptags", {}).get("tag") or []):
            name = t.get("name") if isinstance(t, dict) else str(t)
            if name:
                toptags.append(name)
        wiki = album.get("wiki") or {}
        wiki_summary = wiki.get("summary") if isinstance(wiki, dict) else ""
        wiki_content = wiki.get("content") if isinstance(wiki, dict) else ""
        mbid = (album.get("mbid") or "").strip()
        artist_val = album.get("artist")
        if isinstance(artist_val, dict):
            artist_value = (
                str(artist_val.get("name") or artist_val.get("#text") or "").strip()
                or artist_name
            )
        else:
            artist_value = str(artist_val or artist_name or "").strip() or artist_name
        tracklist: list[str] = []
        try:
            tracks_block = album.get("tracks") or {}
            track_nodes = tracks_block.get("track") if isinstance(tracks_block, dict) else []
            if isinstance(track_nodes, dict):
                track_nodes = [track_nodes]
            for tr in track_nodes or []:
                if isinstance(tr, dict):
                    tname = str(tr.get("name") or tr.get("title") or "").strip()
                else:
                    tname = str(tr or "").strip()
                if tname:
                    tracklist.append(tname)
        except Exception:
            tracklist = []
        playcount = _safe_nonneg_int(album.get("playcount"))
        listeners = _safe_nonneg_int(album.get("listeners"))
        return {
            "cover_url": cover_url,
            "toptags": toptags,
            "title": album.get("name") or album_title,
            "artist": artist_value,
            "mbid": mbid,
            "url": str(album.get("url") or "").strip(),
            "wiki_summary": wiki_summary or "",
            "wiki_content": wiki_content or "",
            "tracklist": tracklist,
            "lastfm_scrobbles": playcount,
            "lastfm_listeners": listeners,
        }
    except Exception as e:
        logging.warning("Last.fm fetch failed for %s / %s: %s", artist_name, album_title, e)
        return None

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}

def _provider_album_search_candidate_score_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_album_search_candidate_score(*args, **kwargs)

def _public_album_provider_headers_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _public_album_provider_headers(*args, **kwargs)

def _extract_json_ld_nodes_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _extract_json_ld_nodes(*args, **kwargs)

def _json_ld_type_matches_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _json_ld_type_matches(*args, **kwargs)

def _json_ld_music_album_node_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _json_ld_music_album_node(*args, **kwargs)

def _json_ld_artist_name_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _json_ld_artist_name(*args, **kwargs)

def _json_ld_tracklist_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _json_ld_tracklist(*args, **kwargs)

def _provider_album_meta_fallback_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_album_meta_fallback(*args, **kwargs)

def _parse_public_album_page_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _parse_public_album_page_payload(*args, **kwargs)

def _spotify_album_page_urls_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _spotify_album_page_urls(*args, **kwargs)

def _qobuz_album_page_urls_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _qobuz_album_page_urls(*args, **kwargs)

def _tidal_album_page_urls_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _tidal_album_page_urls(*args, **kwargs)

def _itunes_cover_url_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _itunes_cover_url_candidates(*args, **kwargs)

def _fetch_itunes_album_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_itunes_album_info(*args, **kwargs)

def _fetch_deezer_album_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_deezer_album_info(*args, **kwargs)

def _fetch_spotify_album_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_spotify_album_info(*args, **kwargs)

def _fetch_qobuz_album_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_qobuz_album_info(*args, **kwargs)

def _fetch_tidal_album_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_tidal_album_info(*args, **kwargs)

def _fetch_audiodb_album_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_audiodb_album_info(*args, **kwargs)

def _fetch_lastfm_album_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_lastfm_album_info(*args, **kwargs)
