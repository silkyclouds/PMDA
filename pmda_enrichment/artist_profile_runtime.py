"""Runtime-owned artist profile and artist image enrichment handlers."""
from __future__ import annotations

from typing import Any

from pmda_enrichment.wikipedia_runtime import (
    artist_cached_image_provider_is_provider_first as _artist_cached_image_provider_is_provider_first,
    commons_file_path_url as _commons_file_path_url,
    dedupe_keep_order as _dedupe_keep_order,
    fetch_wikidata_media_url as _fetch_wikidata_media_url,
    fetch_wikipedia_intro_extract as _fetch_wikipedia_intro_extract,
    fetch_wikipedia_page_metadata as _fetch_wikipedia_page_metadata,
    fetch_wikipedia_pageimage as _fetch_wikipedia_pageimage,
    wikipedia_title_from_fullurl as _wikipedia_title_from_fullurl,
)

_EXTRACTED_NAMES = {
    '_fetch_discogs_artist_profile_info',
    '_fetch_bandcamp_artist_profile_hint',
    '_fetch_musicbrainz_artist_profile_info',
    '_files_try_artist_image_refresh',
    '_is_relevant_artist_profile_text',
    '_artist_profile_text_looks_music_related',
    '_artist_profile_text_looks_biographical',
    '_fetch_lastfm_artist_info',
    '_fetch_wikimedia_commons_artist_image',
    '_resolve_authoritative_artist_image_url',
    '_artist_profile_search_queries',
    '_fetch_wikipedia_artist_bio',
    '_artist_profile_payload_requires_refresh',
    '_build_single_artist_profile_payload',
    '_build_artist_profile_payload',
    '_merge_artist_profile_tags',
    'api_library_artist_profile',
    'api_library_artist_ai_enrich',
    'api_library_artist_facts',
    'api_library_artist_facts_extract',
    'api_library_files_artist_image',
    'api_library_external_artist_image',
    'get_artist_images_mb',
    'get_similar_artists_mb',
    '_is_probably_placeholder_artist_image_url',
    '_is_suspicious_external_artist_image_url',
    '_artist_image_provider_allowed_for_entity',
    '_is_usable_artist_image_bytes',
    '_is_usable_artist_image_path',
    '_is_artist_image_distinct_from_local_covers',
    '_fetch_and_save_artist_image_mb',
    '_fetch_artist_image_lastfm',
    '_fetch_artist_image_discogs',
    '_fetch_artist_image_fanart',
    '_fetch_artist_image_audiodb',
    '_extract_artist_mbid_from_mb_payload',
    '_resolve_artist_mbid_for_fanart',
    '_artist_image_search_queries',
    '_artist_image_lookup_candidates',
    '_artist_image_alias_candidate_is_compatible',
    '_artist_image_exact_name_match',
    '_fetch_wikipedia_artist_bio_best',
    '_artist_image_url_looks_relevant',
    '_artist_image_result_looks_relevant',
    '_fetch_artist_image_web',
    '_fetch_and_save_artist_image',
    'api_library_artist_images',
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

def _fetch_discogs_artist_profile_info(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    alias_candidates: list[str] | tuple[str, ...] | None = None,
) -> Optional[dict[str, Any]]:
    """Return exact-match Discogs artist profile data when available."""
    if not USE_DISCOGS:
        return None
    name = " ".join(str(artist_name or "").split()).strip()
    if not name:
        return None
    try:
        d = _get_discogs_client()
        if d is None:
            return None
        results = d.search(name, type="artist")
        page = _discogs_call("artist profile search page=1", lambda: results.page(1))
        if not page:
            return None
        for artist in page[:10]:
            artist_data = getattr(artist, "data", None)
            cand_name = ""
            if isinstance(artist_data, dict):
                cand_name = str(artist_data.get("title") or artist_data.get("name") or "").strip()
            if not cand_name:
                cand_name = str(getattr(artist, "name", "") or "").strip()
            if not _artist_image_exact_name_match(
                name,
                cand_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                alias_candidates=alias_candidates,
            ):
                continue
            artist_id = getattr(artist, "id", None) or (artist_data.get("id") if isinstance(artist_data, dict) else None)
            if not artist_id:
                continue
            full_id = int(getattr(artist_id, "id", artist_id))
            full_data = _discogs_call(f"artist {full_id} data", lambda aid=full_id: d.artist(aid).data)
            if not isinstance(full_data, dict):
                continue
            profile_text = _strip_html_text(str(full_data.get("profile") or "").strip())
            if profile_text and not _artist_profile_text_matches_any_identity(
                name,
                f"{cand_name}\n{profile_text}",
                entity_kind=entity_kind,
                role_hints=role_hints,
                candidate_names=list(alias_candidates or []),
            ):
                profile_text = ""
            image_url = ""
            images = full_data.get("images") or []
            if isinstance(images, list):
                for item in images:
                    if not isinstance(item, dict):
                        continue
                    cand_url = str(item.get("uri150") or item.get("uri") or item.get("resource_url") or "").strip()
                    if cand_url and not _is_probably_placeholder_artist_image_url(cand_url):
                        image_url = cand_url
                        break
            artist_url = ""
            urls = full_data.get("urls") or []
            if isinstance(urls, list):
                for value in urls:
                    clean = str(value or "").strip()
                    if clean:
                        artist_url = clean
                        break
            payload = {
                "bio": profile_text,
                "short_bio": _truncate_text(profile_text, max_chars=460) if profile_text else "",
                "image_url": image_url,
                "matched_name": cand_name,
                "url": artist_url,
                "source": "discogs",
            }
            if any(str(payload.get(key) or "").strip() for key in ("bio", "short_bio", "image_url", "url")):
                return payload
        return None
    except DiscogsRateLimited:
        return None
    except Exception:
        logging.debug("Discogs artist profile fetch failed for %s", name, exc_info=True)
        return None

def _fetch_bandcamp_artist_profile_hint(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    alias_candidates: list[str] | tuple[str, ...] | None = None,
    limit: int = 6,
) -> Optional[dict[str, Any]]:
    """Use matched Bandcamp album pages to extract exact owner bio/image for the artist."""
    name = " ".join(str(artist_name or "").split()).strip()
    artist_norm = _norm_artist_key(name)
    if not name or not artist_norm or _get_library_mode() != "files":
        return None
    album_hints: list[tuple[str, str]] = []
    with _files_pg_connection() as conn:
        if conn is None:
            return None
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COALESCE(alb.bandcamp_album_url, ''),
                        COALESCE(alb.title, '')
                    FROM files_albums alb
                    JOIN files_artist_album_links link ON link.album_id = alb.id
                    JOIN files_artists art ON art.id = link.artist_id
                    WHERE art.name_norm = %s
                      AND COALESCE(alb.bandcamp_album_url, '') <> ''
                    ORDER BY
                        CASE WHEN COALESCE(alb.strict_match_provider, '') = 'bandcamp' THEN 0 ELSE 1 END,
                        alb.updated_at DESC,
                        alb.id DESC
                    LIMIT %s
                    """,
                    (artist_norm, max(1, min(int(limit or 6), 10))),
                )
                album_hints = [
                    (str(url or "").strip(), str(title or "").strip())
                    for url, title in cur.fetchall()
                    if str(url or "").strip()
                ]
        except Exception:
            album_hints = []
    if not album_hints:
        return None
    lookup_names = _artist_image_lookup_candidates(
        name,
        list(alias_candidates or []),
        entity_kind=entity_kind,
        role_hints=role_hints,
        limit=10,
    ) or [name]
    for album_url_hint, album_title_hint in album_hints:
        try:
            bandcamp_payload = _fetch_bandcamp_album_info(
                name,
                album_title_hint,
                allow_web_fallback=False,
                album_url_hint=album_url_hint,
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
        if not owner_name or not _artist_image_exact_name_match(
            name,
            owner_name,
            entity_kind=entity_kind,
            role_hints=role_hints,
            alias_candidates=lookup_names,
        ):
            continue
        owner_bio = _strip_html_text(str(bandcamp_payload.get("page_owner_bio") or "").strip())
        if owner_bio and not _artist_profile_text_matches_any_identity(
            name,
            f"{owner_name}\n{owner_bio}",
            entity_kind=entity_kind,
            role_hints=role_hints,
            candidate_names=lookup_names,
        ):
            owner_bio = ""
        payload = {
            "bio": owner_bio,
            "short_bio": _truncate_text(owner_bio, max_chars=460) if owner_bio else "",
            "image_url": _bandcamp_preferred_image_url(str(bandcamp_payload.get("page_owner_image_url") or "").strip()),
            "matched_name": owner_name,
            "url": str(bandcamp_payload.get("page_owner_url") or "").strip(),
            "source": "bandcamp",
        }
        if any(str(payload.get(key) or "").strip() for key in ("bio", "short_bio", "image_url", "url")):
            return payload
    return None

def _fetch_musicbrainz_artist_profile_info(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    alias_candidates: list[str] | tuple[str, ...] | None = None,
    mb_identity: dict[str, Any] | None = None,
) -> Optional[dict[str, Any]]:
    """Return exact-match MusicBrainz profile hints, tags, similar artists and authoritative image when available."""
    if not USE_MUSICBRAINZ:
        return None
    name = " ".join(str(artist_name or "").split()).strip()
    if not name:
        return None
    identity = dict(mb_identity or {})
    if not identity:
        try:
            identity = _musicbrainz_artist_identity_lookup(
                name,
                entity_kind=entity_kind,
                role_hints=role_hints,
            ) or {}
        except Exception:
            identity = {}
    artist_mbid = str(identity.get("mbid") or "").strip()
    matched_name = " ".join(str(identity.get("name") or name).split()).strip() or name
    lookup_names = _artist_image_lookup_candidates(
        name,
        [
            matched_name,
            str(identity.get("sort_name") or "").strip(),
            *[str(alias or "").strip() for alias in (identity.get("aliases") or [])],
            *[str(alias or "").strip() for alias in (alias_candidates or [])],
        ],
        entity_kind=entity_kind,
        role_hints=role_hints,
        limit=12,
    )
    if artist_mbid and not _artist_image_exact_name_match(
        name,
        matched_name,
        entity_kind=entity_kind,
        role_hints=role_hints,
        alias_candidates=lookup_names,
    ):
        return None
    artist_data = {}
    if artist_mbid:
        try:
            full = musicbrainzngs.get_artist_by_id(
                artist_mbid,
                includes=["aliases", "url-rels", "tags", "annotation", "artist-rels"],
            )
            artist_data = (full or {}).get("artist") or {}
        except Exception:
            artist_data = {}
    if not isinstance(artist_data, dict):
        artist_data = {}
    full_name = " ".join(str(artist_data.get("name") or matched_name).split()).strip() or matched_name
    if artist_mbid and not _artist_image_exact_name_match(
        name,
        full_name,
        entity_kind=entity_kind,
        role_hints=role_hints,
        alias_candidates=lookup_names,
    ):
        return None
    tag_names: list[str] = []
    for tag in (artist_data.get("tag-list") or []):
        if not isinstance(tag, dict):
            continue
        tag_name = str(tag.get("name") or "").strip()
        if tag_name:
            tag_names.append(tag_name)
    tag_names = _dedupe_keep_order(tag_names)[:20]

    annotation = _strip_html_text(str(artist_data.get("annotation") or "").strip())
    disambiguation = _strip_html_text(str(artist_data.get("disambiguation") or "").strip())
    bio_text = ""
    if annotation and _artist_profile_text_matches_any_identity(
        name,
        annotation,
        entity_kind=entity_kind,
        role_hints=role_hints,
        candidate_names=lookup_names,
    ):
        bio_text = annotation
    elif disambiguation:
        bio_text = disambiguation

    url_values = [
        str(url or "").strip()
        for url in (identity.get("urls") or [])
        if str(url or "").strip()
    ]
    url_relations = artist_data.get("url-relation-list") or []
    if isinstance(url_relations, list):
        for relation in url_relations:
            if not isinstance(relation, dict):
                continue
            target = str(relation.get("target") or "").strip()
            if target:
                url_values.append(target)
    url_values = _dedupe_keep_order(url_values)[:20]

    summary_parts = [
        str(part or "").strip()
        for part in [
            disambiguation,
            annotation,
            ", ".join(tag_names[:8]) if tag_names else "",
        ]
        if str(part or "").strip()
    ]
    image_url = ""
    for target in url_values:
        resolved = _resolve_authoritative_artist_image_url(
            target,
            artist_name=name,
            entity_kind=entity_kind,
            role_hints=role_hints,
            page_title=full_name,
            page_summary="\n".join(summary_parts),
        )
        if resolved:
            image_url = resolved
            break
    try:
        similar = get_similar_artists_mb(artist_mbid) if artist_mbid else []
    except Exception:
        similar = []
    payload = {
        "bio": bio_text,
        "short_bio": _truncate_text(bio_text, max_chars=460) if bio_text else "",
        "image_url": image_url,
        "matched_name": full_name,
        "url": url_values[0] if url_values else "",
        "source": "musicbrainz",
        "tags": tag_names,
        "similar": similar[:20] if isinstance(similar, list) else [],
        "mbid": artist_mbid,
    }
    if any(
        str(payload.get(key) or "").strip()
        for key in ("bio", "short_bio", "image_url", "url", "mbid")
    ) or bool(payload.get("tags")) or bool(payload.get("similar")):
        return payload
    return None

def _files_try_artist_image_refresh(
    *,
    artist_name: str,
    artist_norm: str,
    entity_kind: str,
    role_hints: list[str] | tuple[str, ...] | None,
    lastfm_info: dict | None = None,
    wiki_info: dict | None = None,
    mb_identity: dict | None = None,
    fast_mode: bool = False,
) -> bool:
    return _profile_runtime.files_try_artist_image_refresh_for_runtime(
        sys.modules[__name__],
        artist_name=artist_name,
        artist_norm=artist_norm,
        entity_kind=entity_kind,
        role_hints=role_hints,
        lastfm_info=lastfm_info,
        wiki_info=wiki_info,
        mb_identity=mb_identity,
        fast_mode=fast_mode,
    )

def _is_relevant_artist_profile_text(artist_name: str, text: str) -> bool:
    return _text_mentions_identity_phrase(str(artist_name or ""), str(text or ""))

def _artist_profile_text_looks_music_related(
    text: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> bool:
    body = " ".join(str(text or "").split()).strip().lower()
    if not body:
        return False
    general_terms = {
        "album",
        "artist",
        "band",
        "composer",
        "conductor",
        "dj",
        "drummer",
        "electronic musician",
        "ensemble",
        "guitarist",
        "label",
        "musician",
        "orchestra",
        "performer",
        "pianist",
        "producer",
        "project",
        "rapper",
        "recording",
        "singer",
        "songwriter",
        "techno",
        "violinist",
        "vocalist",
    }
    role_terms = {
        str(role or "").strip().lower()
        for role in (role_hints or [])
        if str(role or "").strip()
    }
    kind = str(entity_kind or "").strip().lower()
    expected_terms = set(general_terms)
    if kind:
        expected_terms.add(kind.replace("_", " "))
    expected_terms.update(role_terms)
    return any(term in body for term in expected_terms if term)

def _artist_profile_text_looks_biographical(
    artist_name: str,
    text: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    candidate_names: list[str] | tuple[str, ...] | None = None,
) -> bool:
    body = " ".join(str(text or "").split()).strip()
    if not body:
        return False
    body_low = body.lower()
    if not _artist_profile_text_matches_any_identity(
        artist_name,
        body,
        entity_kind=entity_kind,
        role_hints=role_hints,
        candidate_names=candidate_names,
    ):
        return False
    if not _artist_profile_text_looks_music_related(
        body,
        entity_kind=entity_kind,
        role_hints=role_hints,
    ):
        return False

    identity_names = [str(artist_name or "").strip(), *(list(candidate_names or []))]
    identity_names = [n for n in identity_names if n]
    identity_names.sort(key=len, reverse=True)
    bad_patterns = [
        r"\brefers to\b",
        r"\bis a type of\b",
        r"\bis the term for\b",
        r"\bis the name for\b",
        r"\bis an audio signal\b",
        r"\bis a random signal\b",
        r"\bis random noise\b",
        r"\bis white noise\b",
        r"\bis noise\b",
        r"\bis a sound\b",
        r"\bis the soundtrack to\b",
        r"\bis the title of\b",
        r"\bis a kind of\b",
        r"\bis a form of\b",
    ]
    for name in identity_names[:4]:
        name_pat = re.escape(name.lower())
        prefix = rf"^(?:the\s+)?{name_pat}\s+"
        if any(re.search(prefix + pat, body_low) for pat in bad_patterns):
            return False

    positive_markers = (
        "artist",
        "musician",
        "band",
        "group",
        "project",
        "producer",
        "dj",
        "composer",
        "conductor",
        "orchestra",
        "ensemble",
        "choir",
        "singer",
        "songwriter",
        "rapper",
        "formed",
        "founded",
        "born",
        "based in",
        "released",
        "debut",
        "discography",
        "record label",
    )
    return any(token in body_low for token in positive_markers)

def _fetch_lastfm_artist_info(artist_name: str) -> Optional[dict]:
    """
    Call Last.fm artist.getInfo. Returns dict with bio/tags/similar artists or None.
    """
    if not USE_LASTFM:
        return None

    def _fetch_lastfm_artist_info_html_fallback(name: str) -> Optional[dict]:
        query_name = " ".join(str(name or "").split()).strip()
        if not query_name:
            return None
        try:
            url = f"https://www.last.fm/music/{quote(query_name, safe='')}"
            resp = _provider_gateway_http_get(
                "lastfm",
                url,
                timeout=10,
                headers={
                    "User-Agent": "PMDA/0.7.5 (self-hosted music library; https://github.com/silkyclouds/PMDA)",
                },
                context="lastfm artist html fallback",
                cache_ttl_sec=60 * 60 * 6,
            )
            if resp.status_code != 200 or not resp.text:
                return None
            text = resp.text
            title_match = re.search(r"<title[^>]*>(.*?)</title>", text, re.IGNORECASE | re.DOTALL)
            og_title_match = re.search(
                r'<meta\s+property=["\']og:title["\']\s+content=["\']([^"\']+)["\']',
                text,
                re.IGNORECASE,
            )
            og_desc_match = re.search(
                r'<meta\s+property=["\']og:description["\']\s+content=["\']([^"\']+)["\']',
                text,
                re.IGNORECASE,
            )
            og_image_match = re.search(
                r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
                text,
                re.IGNORECASE,
            )
            matched_title = html.unescape(
                str((og_title_match.group(1) if og_title_match else "") or (title_match.group(1) if title_match else "")).strip()
            )
            matched_name = re.sub(
                r"\s+music,\s+videos,\s+stats,\s+and\s+photos\s*\|\s*Last\.fm\s*$",
                "",
                matched_title,
                flags=re.IGNORECASE,
            ).strip()
            if not matched_name:
                matched_name = query_name
            if _norm_artist_key(matched_name) != _norm_artist_key(query_name):
                return None
            image_url = html.unescape(str(og_image_match.group(1) if og_image_match else "").strip())
            if image_url and _is_probably_placeholder_artist_image_url(image_url):
                image_url = ""
            desc = _cleanup_lastfm_bio_text(html.unescape(str(og_desc_match.group(1) if og_desc_match else "").strip()))
            if _is_garbage_bio(desc):
                desc = ""
            return {
                "bio": desc,
                "short_bio": _truncate_text(desc, max_chars=460) if desc else "",
                "tags": [],
                "similar": [],
                "image_url": image_url,
                "mbid": "",
                "matched_name": matched_name,
                "source": "lastfm",
            }
        except Exception:
            logging.debug("Last.fm artist HTML fallback failed for %s", name, exc_info=True)
            return None

    api_key = (getattr(sys.modules[__name__], "LASTFM_API_KEY", "") or "").strip()
    if not api_key:
        return _fetch_lastfm_artist_info_html_fallback(artist_name)
    try:
        def _pick_best_lastfm_image_url(images) -> str:
            if isinstance(images, dict):
                images = [images]
            if not isinstance(images, list):
                return ""
            rank = {
                "mega": 60,
                "extralarge": 50,
                "large": 40,
                "medium": 30,
                "small": 20,
                "": 10,
            }
            best_url = ""
            best_rank = -1
            for im in images:
                if not isinstance(im, dict):
                    continue
                url = (im.get("#text") or im.get("text") or "").strip()
                if not url:
                    continue
                # Last.fm frequently returns a "missing image" placeholder that looks like a music note.
                # We treat those as absent so the UI can fall back to Wikipedia or local/DB images.
                try:
                    if _is_probably_placeholder_artist_image_url(url):
                        continue
                except Exception:
                    pass
                r = rank.get(str(im.get("size") or "").strip().lower(), 0)
                if r > best_rank:
                    best_rank = r
                    best_url = url
            return best_url

        data = None
        artist = {}
        returned_name = ""
        # Avoid Last.fm autocorrect mapping obscure artists to wrong entities.
        for autocorrect in (0, 1):
            params = {"method": "artist.getInfo", "artist": artist_name, "api_key": api_key, "format": "json", "autocorrect": autocorrect}
            resp = _provider_gateway_http_get(
                "lastfm",
                "https://ws.audioscrobbler.com/2.0/",
                params=params,
                timeout=10,
                context=f"lastfm artist api autocorrect={autocorrect}",
                cache_ttl_sec=60 * 60 * 6,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            if (data or {}).get("error") and (data or {}).get("error") != 0:
                continue
            artist = (data or {}).get("artist") or {}
            returned_name = (artist.get("name") or "").strip()
            if returned_name:
                score = _provider_identity_text_score(artist_name or "", returned_name)
                if score < 0.78:
                    # Try without/with autocorrect; otherwise reject to prevent nonsense bios/similar.
                    continue
            break
        if not artist:
            return _fetch_lastfm_artist_info_html_fallback(artist_name)
        bio = artist.get("bio") or {}
        summary = _cleanup_lastfm_bio_text((bio.get("summary") if isinstance(bio, dict) else "") or "")
        content = _cleanup_lastfm_bio_text((bio.get("content") if isinstance(bio, dict) else "") or "")
        tags_raw = (artist.get("tags") or {}).get("tag") or []
        if isinstance(tags_raw, dict):
            tags_raw = [tags_raw]
        tags = []
        for t in tags_raw:
            name = (t.get("name") if isinstance(t, dict) else str(t) or "").strip()
            if name:
                tags.append(name)
        similar_raw = (artist.get("similar") or {}).get("artist") or []
        if isinstance(similar_raw, dict):
            similar_raw = [similar_raw]
        similar = []
        for item in similar_raw:
            if not isinstance(item, dict):
                continue
            name = (item.get("name") or "").strip()
            mbid = (item.get("mbid") or "").strip()
            if not name:
                continue
            sim_img = _pick_best_lastfm_image_url(item.get("image"))
            entry = {"name": name, "mbid": mbid, "type": "Last.fm"}
            if sim_img:
                entry["image_url"] = sim_img
            similar.append(entry)
        artist_image_url = _pick_best_lastfm_image_url(artist.get("image"))
        artist_mbid = str(artist.get("mbid") or "").strip()
        best_bio = content or summary
        if _is_garbage_bio(best_bio):
            best_bio = ""
        best_short = summary or content
        best_short = "" if _is_garbage_bio(best_short) else best_short
        if (not artist_image_url) or (not best_bio) or (not best_short):
            fallback = _fetch_lastfm_artist_info_html_fallback(returned_name or artist_name) or {}
            fallback_name = str(fallback.get("matched_name") or "").strip()
            if fallback_name and _norm_artist_key(fallback_name) == _norm_artist_key(returned_name or artist_name):
                if not artist_image_url:
                    artist_image_url = str(fallback.get("image_url") or "").strip()
                if not best_bio:
                    best_bio = str(fallback.get("bio") or "").strip()
                if not best_short:
                    best_short = str(fallback.get("short_bio") or "").strip()
        return {
            "bio": best_bio,
            "short_bio": _truncate_text(best_short, max_chars=460) if best_short else "",
            "tags": _dedupe_keep_order(tags)[:20],
            "similar": similar[:20],
            "image_url": artist_image_url or "",
            "mbid": artist_mbid,
            "matched_name": returned_name,
            "source": "lastfm",
        }
    except Exception as e:
        logging.debug("Last.fm artist profile fetch failed for %s: %s", artist_name, e)
        return _fetch_lastfm_artist_info_html_fallback(artist_name)

def _fetch_wikimedia_commons_artist_image(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    candidate_names: list[str] | tuple[str, ...] | None = None,
    limit: int = 8,
) -> str:
    primary_name = " ".join(str(artist_name or "").split()).strip()
    if not primary_name:
        return ""
    primary_lookup_name = _artist_identity_primary_lookup_name(
        primary_name,
        entity_kind=entity_kind,
        role_hints=role_hints,
        candidate_names=candidate_names,
    )
    headers = {"User-Agent": "PMDA/0.7.5 (self-hosted music library; https://github.com/silkyclouds/PMDA)"}
    lookup_names = _artist_image_lookup_candidates(
        primary_lookup_name,
        [primary_name, *(list(candidate_names or []))],
        entity_kind=entity_kind,
        role_hints=role_hints,
        limit=12,
    ) or [primary_lookup_name or primary_name]
    for lookup_name in lookup_names[:6]:
        queries = _artist_image_search_queries(
            lookup_name,
            entity_kind=entity_kind,
            role_hints=role_hints,
        )
        for query in queries[:4]:
            try:
                resp = requests.get(
                    "https://commons.wikimedia.org/w/api.php",
                    params={
                        "action": "query",
                        "list": "search",
                        "srsearch": query,
                        "srnamespace": 6,
                        "srlimit": max(3, min(int(limit or 8), 12)),
                        "format": "json",
                        "utf8": 1,
                    },
                    headers=headers,
                    timeout=10,
                )
            except Exception:
                continue
            if resp.status_code != 200:
                continue
            data = resp.json() if resp.content else {}
            results = ((data.get("query") or {}).get("search") or []) if isinstance(data, dict) else []
            for item in results:
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title") or "").strip()
                if not title.lower().startswith("file:"):
                    continue
                snippet = _strip_html_text(str(item.get("snippet") or "")).strip()
                page_url = f"https://commons.wikimedia.org/wiki/{quote(title.replace(' ', '_'), safe=':/')}"
                if not _artist_image_result_looks_relevant(
                    lookup_name,
                    {"title": title, "snippet": snippet, "link": page_url},
                    entity_kind=entity_kind,
                    role_hints=role_hints,
                    candidate_names=lookup_names,
                ):
                    continue
                file_name = title.split(":", 1)[1].strip()
                if re.search(r"\.(pdf|djvu|svg)$", file_name, flags=re.IGNORECASE):
                    continue
                file_url = _commons_file_path_url(file_name)
                if not file_url:
                    continue
                if _artist_image_url_looks_relevant(
                    file_url,
                    artist_name=lookup_name,
                    entity_kind=entity_kind,
                    role_hints=role_hints,
                    page_title=title,
                    page_summary=snippet,
                ):
                    return file_url
    return ""

def _resolve_authoritative_artist_image_url(
    target_url: str,
    *,
    artist_name: str,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    page_title: str = "",
    page_summary: str = "",
    timeout: int = 8,
) -> str:
    url = str(target_url or "").strip()
    if not url:
        return ""
    parsed = urlparse(url)
    host = (parsed.netloc or "").strip().lower()
    path = unquote(parsed.path or "")
    wiki_lang = ""
    if host.endswith(".wikipedia.org"):
        wiki_lang = host.split(".")[0]
        title = ""
        if "/wiki/" in path:
            title = path.split("/wiki/", 1)[1].replace("_", " ").strip()
        if title:
            img = _fetch_wikipedia_pageimage(title, lang=wiki_lang or "en", thumb_px=960)
            if img and _artist_image_url_looks_relevant(
                img,
                artist_name=artist_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                page_title=page_title or title,
                page_summary=page_summary,
            ):
                return img
    if host == "commons.wikimedia.org" and "/wiki/" in path:
        title = path.split("/wiki/", 1)[1].replace("_", " ").strip()
        title_clean = _artist_image_page_identity_candidate(title)
        if title.lower().startswith("file:"):
            file_name = title.split(":", 1)[1].strip()
            img = _commons_file_path_url(file_name)
            if img and _artist_image_url_looks_relevant(
                img,
                artist_name=artist_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                page_title=title_clean or title,
                page_summary=page_summary,
            ):
                return img
    if host.endswith(".wikidata.org") or host == "wikidata.org":
        parts = [part for part in path.split("/") if part]
        qid = parts[-1] if parts else ""
        img = _fetch_wikidata_media_url(qid, preferred_props=("P18",))
        if img and _artist_image_url_looks_relevant(
            img,
            artist_name=artist_name,
            entity_kind=entity_kind,
            role_hints=role_hints,
            page_title=page_title,
            page_summary=page_summary,
        ):
            return img
    resolved = _resolve_remote_image_or_og_url(url, timeout=timeout)
    if resolved and _artist_image_url_looks_relevant(
        resolved,
        artist_name=artist_name,
        entity_kind=entity_kind,
        role_hints=role_hints,
        page_title=page_title,
        page_summary=page_summary,
    ):
        return resolved
    return ""

def _artist_profile_search_queries(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> list[str]:
    name = " ".join((artist_name or "").split()).strip()
    if not name:
        return []
    kind = str(entity_kind or "").strip().lower()
    roles = {str(role or "").strip().lower() for role in (role_hints or []) if str(role or "").strip()}
    qualifiers: list[str]
    if kind in {"composer"} or "composer" in roles:
        qualifiers = ["composer", "classical composer", "composer portrait", "musician", "wikipedia"]
    elif kind in {"conductor"} or "conductor" in roles:
        qualifiers = ["conductor", "maestro", "conductor portrait", "musician", "wikipedia"]
    elif kind in {"orchestra"} or "orchestra" in roles:
        qualifiers = ["orchestra", "philharmonic orchestra", "symphony orchestra", "orchestra musicians", "official photo", "press photo", "wikipedia"]
    elif kind in {"ensemble", "choir", "chorus"} or roles.intersection({"ensemble", "choir", "chorus"}):
        qualifiers = ["ensemble", "music ensemble", "official photo", "ensemble musicians", "press photo", "choir", "chorus", "wikipedia"]
    elif kind in {"performer"} or roles.intersection({"soloist", "performer"}):
        qualifiers = ["musician", "performer", "soloist", "portrait", "wikipedia"]
    else:
        qualifiers = ["musician", "band", "artist", "portrait"]
    queries = [f"{name} {qualifier}" for qualifier in qualifiers]
    queries.append(name)
    out: list[str] = []
    seen: set[str] = set()
    for query in queries:
        clean = " ".join(str(query or "").split()).strip()
        key = clean.lower()
        if clean and key not in seen:
            seen.add(key)
            out.append(clean)
    return out

def _fetch_wikipedia_artist_bio(
    artist_name: str,
    lang: str = "en",
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    candidate_names: list[str] | tuple[str, ...] | None = None,
) -> Optional[dict]:
    """
    Best-effort Wikipedia intro for an artist name.
    Returns {"bio","short_bio","source","url","lang"} or None.
    """
    name = (artist_name or "").strip()
    l = (lang or "en").strip().lower() or "en"
    if not name:
        return None
    lookup_names = _artist_identity_lookup_names(
        name,
        entity_kind=entity_kind,
        role_hints=role_hints,
        candidate_names=candidate_names,
        limit=12,
    )
    primary_lookup_name = str(lookup_names[0] or name).strip()
    ranked_lookup_names = sorted(
        lookup_names,
        key=lambda value: (
            0 if _norm_artist_key(str(value or "")) == _norm_artist_key(primary_lookup_name) else 1,
            -_identity_display_quality_score(str(value or ""))[0],
            -_classical_person_alias_signature(str(value or "")).get("token_count", 0),
            -len(str(value or "")),
            str(value or "").lower(),
        ),
    )
    api_url = f"https://{l}.wikipedia.org/w/api.php"
    headers = {"User-Agent": "PMDA/0.7.5 (self-hosted music library; https://github.com/silkyclouds/PMDA)"}
    try:
        def _title_is_identity_compatible(title: str) -> bool:
            base_title = re.sub(r"\s*\([^)]*\)\s*$", "", str(title or "").strip()).strip()
            if not base_title:
                return False
            for candidate in lookup_names:
                if _artist_image_alias_candidate_is_compatible(
                    candidate,
                    base_title,
                    entity_kind=entity_kind,
                    role_hints=role_hints,
                ):
                    return True
            return _provider_identity_text_score(name, base_title) >= 0.9

        def _looks_music_related(extract: str, description: str = "", title: str = "") -> bool:
            low = (extract or "").lower()
            if not low:
                return False
            desc_low = (description or "").strip().lower()
            title_low = (title or "").strip().lower()
            # If Wikipedia explicitly describes the page as "Earth pigment ..." etc, reject immediately.
            if desc_low:
                hard_bad = (
                    "disambiguation page",
                    "earth pigment",
                    "pigment",
                    "clay",
                    "oxide",
                    "mineral",
                    "paint",
                    "soil",
                    "ferric",
                    "hematite",
                    "kaolin",
                )
                if any(tok in desc_low for tok in hard_bad):
                    return False
                # Positive hints in the short description are very reliable.
                hard_good = (
                    "musician",
                    "electronic musician",
                    "singer",
                    "singer-songwriter",
                    "songwriter",
                    "composer",
                    "producer",
                    "dj",
                    "rapper",
                    "band",
                    "music group",
                    "conductor",
                    "maestro",
                    "orchestra",
                    "philharmonic",
                    "symphony orchestra",
                    "ensemble",
                    "choir",
                    "chorus",
                    # French variants (when using frwiki).
                    "musicien",
                    "groupe",
                    "chanteur",
                    "chanteuse",
                    "auteur-compositeur",
                    "compositeur",
                    "producteur",
                    "rappeur",
                    "chef d'orchestre",
                    "orchestre",
                    "ensemble",
                    "choeur",
                    "chœur",
                )
                if any(tok in desc_low for tok in hard_good):
                    return True

            # Title disambiguators are also a strong signal.
            if re.search(r"\((band|musician|singer|rapper|dj|music group|groupe|musicien)\)", title_low):
                return True

            good = (
                "musician", "band", "singer", "songwriter", "composer", "producer", "dj",
                "album", "record", "label", "single", "track", "genre", "formed", "born",
                "conductor", "maestro", "orchestra", "philharmonic", "symphony", "ensemble", "choir", "chorus",
                # French variants
                "musicien", "groupe", "chanteur", "chanteuse", "album", "label", "dj",
                "chef d'orchestre", "orchestre", "ensemble", "choeur", "chœur",
            )
            bad = (
                "pigment", "clay", "oxide", "mineral", "earth pigment", "paint", "soil",
                "ferric", "hematite", "kaolin",
                # French variants
                "argile", "oxyde", "mineral", "pigment",
            )
            score = 0
            for kw in good:
                if kw in low:
                    score += 1
            for kw in bad:
                if kw in low:
                    score -= 2
            return score >= 1
        def _bio_matches_identity(value: str) -> bool:
            return _artist_profile_text_matches_any_identity(
                name,
                value,
                entity_kind=entity_kind,
                role_hints=role_hints,
                candidate_names=ranked_lookup_names,
            )

        for exact_title in ranked_lookup_names[:8]:
            extract, page_url, desc = _fetch_wikipedia_intro_extract(exact_title, lang=l)
            if not extract:
                continue
            page_meta = _fetch_wikipedia_page_metadata(exact_title, lang=l, thumb_px=720)
            canonical_title = _wikipedia_title_from_fullurl(str(page_meta.get("fullurl") or page_url)) or exact_title
            if not _title_is_identity_compatible(canonical_title):
                continue
            if not _looks_music_related(extract, description=desc, title=canonical_title):
                continue
            if not _bio_matches_identity(f"{canonical_title}\n{extract}"):
                continue
            img_url = _fetch_wikipedia_pageimage(exact_title, lang=l, thumb_px=720) or ""
            if img_url and not _artist_image_url_looks_relevant(
                img_url,
                artist_name=name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                page_title=canonical_title,
                page_summary=f"{desc}\n{extract}",
            ):
                img_url = ""
            short = _truncate_text(extract, max_chars=460)
            return {
                "bio": extract,
                "short_bio": short,
                "source": f"wikipedia:{l}",
                "url": page_meta.get("fullurl") or page_url,
                "lang": l,
                "page_title": canonical_title,
                "page_description": desc,
                "image_url": img_url,
            }

        for lookup_name in ranked_lookup_names[:6]:
            # Use role-aware queries to avoid common false positives and to find the right entity kind.
            for q in _artist_profile_search_queries(lookup_name, entity_kind=entity_kind, role_hints=role_hints):
                resp = requests.get(
                    api_url,
                    params={
                        "action": "query",
                    "list": "search",
                    "srsearch": q,
                    "srlimit": 5,
                    "format": "json",
                    "utf8": 1,
                },
                headers=headers,
                timeout=10,
            )
                if resp.status_code != 200:
                    continue
                data = resp.json()
                results = ((data.get("query") or {}).get("search") or []) if isinstance(data, dict) else []
                if not isinstance(results, list) or not results:
                    continue
                # Try a few candidates to dodge disambiguation pages and non-music pages.
                for item in results[:5]:
                    if not isinstance(item, dict):
                        continue
                    title = (item.get("title") or "").strip()
                    if not title:
                        continue
                    title_base = re.sub(r"\s*\([^)]*\)\s*$", "", title).strip()
                    name_score = max(
                        _provider_identity_text_score(name, title_base or title),
                        _provider_identity_text_score(lookup_name, title_base or title),
                    )
                    name_key = _norm_artist_key(lookup_name)
                    title_key = _norm_artist_key(title)
                    name_in_title = bool(name_key and title_key and name_key in title_key)
                    if not _title_is_identity_compatible(title) and name_score < 0.58 and not name_in_title:
                        continue
                    if not _title_is_identity_compatible(title):
                        continue
                    extract, page_url, desc = _fetch_wikipedia_intro_extract(title, lang=l)
                    if not extract or not _looks_music_related(extract, description=desc, title=title):
                        continue
                    if not _bio_matches_identity(f"{title}\n{extract}"):
                        continue
                    img_url = _fetch_wikipedia_pageimage(title, lang=l, thumb_px=720) or ""
                    if img_url and not _artist_image_url_looks_relevant(
                        img_url,
                        artist_name=name,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                        page_title=title,
                        page_summary=f"{desc}\n{extract}",
                    ):
                        img_url = ""
                    short = _truncate_text(extract, max_chars=460)
                    return {
                        "bio": extract,
                        "short_bio": short,
                        "source": f"wikipedia:{l}",
                        "url": page_url,
                        "lang": l,
                        "page_title": title,
                        "page_description": desc,
                        "image_url": img_url,
                    }
    except Exception:
        return None
    return None

def _artist_profile_payload_requires_refresh(
    profile: dict | None,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> bool:
    if not isinstance(profile, dict):
        return True
    source = str(profile.get("source") or "").strip().lower()
    bio_text = str(profile.get("bio") or profile.get("short_bio") or "").strip()
    if not bio_text or _is_garbage_bio(bio_text):
        return True
    if source in {"", "pmda-local"}:
        return True
    if source.startswith("wikipedia"):
        return True
    if source.startswith("composite:"):
        source_low = source.lower()
        has_exact_provider = any(
            token in source_low for token in ("bandcamp", "discogs", "musicbrainz", "lastfm")
        )
        if ("wikipedia" in source_low) and (not has_exact_provider):
            return True
    return False

def _build_single_artist_profile_payload(
    artist_query: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> tuple[dict, dict, dict]:
    q = " ".join((artist_query or "").split()).strip()
    if not q:
        return ({"bio": "", "short_bio": "", "tags": [], "similar": [], "source": ""}, {}, {})

    mb_identity: dict[str, Any] = {}
    try:
        if _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints) or str(entity_kind or "").strip().lower() in {"ensemble", "orchestra", "choir", "chorus", "conductor", "composer"}:
            mb_identity = _musicbrainz_artist_identity_lookup(
                q,
                entity_kind=entity_kind,
                role_hints=role_hints,
            ) or {}
    except Exception:
        mb_identity = {}
    wiki_candidate_names = _artist_image_lookup_candidates(
        q,
        [
            str(mb_identity.get("name") or "").strip(),
            str(mb_identity.get("sort_name") or "").strip(),
            *[str(alias or "").strip() for alias in (mb_identity.get("aliases") or [])],
        ],
        entity_kind=entity_kind,
        role_hints=role_hints,
        limit=12,
    )
    lastfm_info = _fetch_lastfm_artist_info(q) or {}
    wiki_info = _fetch_wikipedia_artist_bio_best(
        q,
        entity_kind=entity_kind,
        role_hints=role_hints,
        candidate_names=wiki_candidate_names,
    )
    bandcamp_info = _fetch_bandcamp_artist_profile_hint(
        q,
        entity_kind=entity_kind,
        role_hints=role_hints,
        alias_candidates=wiki_candidate_names,
    ) or {}
    discogs_info = _fetch_discogs_artist_profile_info(
        q,
        entity_kind=entity_kind,
        role_hints=role_hints,
        alias_candidates=wiki_candidate_names,
    ) or {}
    musicbrainz_info = _fetch_musicbrainz_artist_profile_info(
        q,
        entity_kind=entity_kind,
        role_hints=role_hints,
        alias_candidates=wiki_candidate_names,
        mb_identity=mb_identity,
    ) or {}

    # Guard: reject artist bios that do not actually mention the requested artist identity.
    if isinstance(lastfm_info, dict):
        lf_bio = str(lastfm_info.get("bio") or lastfm_info.get("short_bio") or "").strip()
        if lf_bio and (
            not _artist_profile_text_matches_any_identity(
                q,
                lf_bio,
                entity_kind=entity_kind,
                role_hints=role_hints,
                candidate_names=wiki_candidate_names,
            )
        ):
            lastfm_info = {
                **lastfm_info,
                "bio": "",
                "short_bio": "",
            }
        elif lf_bio and (
            not _artist_profile_text_looks_biographical(
                q,
                lf_bio,
                entity_kind=entity_kind,
                role_hints=role_hints,
                candidate_names=wiki_candidate_names,
            )
        ):
            lastfm_info = {
                **lastfm_info,
                "bio": "",
                "short_bio": "",
            }
    if isinstance(wiki_info, dict):
        wk_bio = str(wiki_info.get("bio") or wiki_info.get("short_bio") or "").strip()
        if wk_bio and (
            not _artist_profile_text_matches_any_identity(
                q,
                wk_bio,
                entity_kind=entity_kind,
                role_hints=role_hints,
                candidate_names=wiki_candidate_names,
            )
        ):
            wiki_info = {}
        elif wk_bio and (
            not _artist_profile_text_looks_biographical(
                q,
                wk_bio,
                entity_kind=entity_kind,
                role_hints=role_hints,
                candidate_names=wiki_candidate_names,
            )
        ):
            wiki_info = {}
    for provider_info in (bandcamp_info, discogs_info):
        if not isinstance(provider_info, dict):
            continue
        prov_bio = str(provider_info.get("bio") or provider_info.get("short_bio") or "").strip()
        if prov_bio and (
            not _artist_profile_text_matches_any_identity(
                q,
                prov_bio,
                entity_kind=entity_kind,
                role_hints=role_hints,
                candidate_names=wiki_candidate_names,
            )
        ):
            provider_info["bio"] = ""
            provider_info["short_bio"] = ""
        elif prov_bio and (
            not _artist_profile_text_looks_biographical(
                q,
                prov_bio,
                entity_kind=entity_kind,
                role_hints=role_hints,
                candidate_names=wiki_candidate_names,
            )
        ):
            provider_info["bio"] = ""
            provider_info["short_bio"] = ""

    # Merge exact-provider hints instead of treating Last.fm as the only source of truth.
    merged_tags = _merge_artist_profile_tags(
        lastfm_info.get("tags") or [],
        musicbrainz_info.get("tags") or [],
        bandcamp_info.get("tags") or [],
        discogs_info.get("tags") or [],
    )
    merged_similar = _merge_similar_artist_candidates(
        lastfm_info.get("similar") or [],
        musicbrainz_info.get("similar") or [],
        bandcamp_info.get("similar") or [],
        discogs_info.get("similar") or [],
    )

    profile = {
        "bio": "",
        "short_bio": "",
        "tags": merged_tags,
        "similar": merged_similar,
        "source": "",
    }

    def _bio_candidate_score(source_name: str, bio: str, short_bio: str) -> int:
        source_norm = str(source_name or "").strip().lower()
        long_text = str(bio or "").strip()
        short_text = str(short_bio or "").strip()
        if not long_text and not short_text:
            return -1
        if source_norm.startswith("wikipedia"):
            effective = long_text or short_text
            if not _artist_profile_text_looks_music_related(
                effective,
                entity_kind=entity_kind,
                role_hints=role_hints,
            ):
                return -1
            if _is_acceptable_original_bio(long_text):
                return 80 + _word_count(long_text)
            if short_text and not _is_garbage_bio(short_text):
                return 40 + _word_count(short_text)
            return -1
        effective = long_text or short_text
        if _is_garbage_bio(effective):
            return -1
        base = {
            "bandcamp": 340,
            "discogs": 260,
            "musicbrainz": 220,
            "lastfm": 180,
        }.get(source_norm, 120)
        return base + _word_count(effective)

    provider_candidates = [
        {
            "source": str((wiki_info or {}).get("source") or "wikipedia").strip(),
            "bio": str((wiki_info or {}).get("bio") or "").strip(),
            "short_bio": str((wiki_info or {}).get("short_bio") or "").strip(),
        },
        {
            "source": str((bandcamp_info or {}).get("source") or "bandcamp").strip(),
            "bio": str((bandcamp_info or {}).get("bio") or "").strip(),
            "short_bio": str((bandcamp_info or {}).get("short_bio") or "").strip(),
        },
        {
            "source": str((discogs_info or {}).get("source") or "discogs").strip(),
            "bio": str((discogs_info or {}).get("bio") or "").strip(),
            "short_bio": str((discogs_info or {}).get("short_bio") or "").strip(),
        },
        {
            "source": str((musicbrainz_info or {}).get("source") or "musicbrainz").strip(),
            "bio": str((musicbrainz_info or {}).get("bio") or "").strip(),
            "short_bio": str((musicbrainz_info or {}).get("short_bio") or "").strip(),
        },
        {
            "source": str((lastfm_info or {}).get("source") or "lastfm").strip(),
            "bio": str((lastfm_info or {}).get("bio") or "").strip(),
            "short_bio": str((lastfm_info or {}).get("short_bio") or "").strip(),
        },
    ]
    best_profile_candidate = max(
        provider_candidates,
        key=lambda item: _bio_candidate_score(
            str(item.get("source") or ""),
            str(item.get("bio") or ""),
            str(item.get("short_bio") or ""),
        ),
        default=None,
    )
    best_score = (
        _bio_candidate_score(
            str(best_profile_candidate.get("source") or ""),
            str(best_profile_candidate.get("bio") or ""),
            str(best_profile_candidate.get("short_bio") or ""),
        )
        if isinstance(best_profile_candidate, dict)
        else -1
    )
    if isinstance(best_profile_candidate, dict) and best_score >= 0:
        profile["bio"] = str(best_profile_candidate.get("bio") or "").strip()
        profile["short_bio"] = str(best_profile_candidate.get("short_bio") or "").strip()
        profile["source"] = str(best_profile_candidate.get("source") or "").strip()
        if _is_garbage_bio(profile.get("bio") or ""):
            profile["bio"] = ""
        if _is_garbage_bio(profile.get("short_bio") or ""):
            profile["short_bio"] = ""

    return profile, lastfm_info, wiki_info

def _build_artist_profile_payload(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> tuple[dict, dict, dict, list[str]]:
    """
    Build artist profile data for either a single artist or a multi-credit string.
    Returns: (profile, lastfm_info_for_images, wiki_info_for_images, split_entities).
    """
    entities = _split_artist_entities_for_profiles(artist_name)
    if len(entities) <= 1:
        profile, lastfm_info, wiki_info = _build_single_artist_profile_payload(
            artist_name,
            entity_kind=entity_kind,
            role_hints=role_hints,
        )
        return profile, lastfm_info, wiki_info, entities

    section_rows: list[dict] = []
    merged_tags: list[str] = []
    merged_similar: list[dict] = []
    similar_seen: set[str] = set()
    source_tokens: list[str] = []
    missing_entities: list[str] = []
    primary_lastfm: dict = {}
    primary_wiki: dict = {}

    for entity in entities:
        profile, lastfm_info, wiki_info = _build_single_artist_profile_payload(
            entity,
            entity_kind=entity_kind,
            role_hints=role_hints,
        )
        source = str(profile.get("source") or "").strip()
        if source and source not in source_tokens:
            source_tokens.append(source)

        if not primary_lastfm and isinstance(lastfm_info, dict):
            if any(str(lastfm_info.get(k) or "").strip() for k in ("bio", "short_bio", "image_url", "mbid")):
                primary_lastfm = lastfm_info
        if not primary_wiki and isinstance(wiki_info, dict):
            if any(str(wiki_info.get(k) or "").strip() for k in ("bio", "short_bio", "image_url", "url")):
                primary_wiki = wiki_info

        bio = str(profile.get("bio") or "").strip()
        short_bio = str(profile.get("short_bio") or "").strip()
        if bio or short_bio:
            section_rows.append(
                {
                    "name": entity,
                    "bio": bio,
                    "short_bio": short_bio,
                    "source": source,
                }
            )
        else:
            missing_entities.append(entity)

        for t in (profile.get("tags") or []):
            tag = str(t or "").strip()
            if tag:
                merged_tags.append(tag)

        for s in (profile.get("similar") or []):
            if not isinstance(s, dict):
                continue
            sname = str(s.get("name") or "").strip()
            skey = _norm_artist_key(sname)
            if not skey or skey in similar_seen:
                continue
            similar_seen.add(skey)
            merged_similar.append(dict(s))

    if not section_rows:
        # Fallback to single-string enrichment when split entities yielded nothing useful.
        profile, lastfm_info, wiki_info = _build_single_artist_profile_payload(
            artist_name,
            entity_kind=entity_kind,
            role_hints=role_hints,
        )
        return profile, (primary_lastfm or lastfm_info), (primary_wiki or wiki_info), entities

    bio_blocks: list[str] = []
    short_blocks: list[str] = []
    for row in section_rows:
        nm = str(row.get("name") or "").strip()
        src = str(row.get("source") or "").strip()
        long_txt = str(row.get("bio") or "").strip()
        short_txt = str(row.get("short_bio") or "").strip()
        if not long_txt:
            long_txt = short_txt
        if not short_txt:
            short_txt = _truncate_text(long_txt, max_chars=220)
        heading = f"{nm} ({src})" if src else nm
        block = f"{heading}\n{long_txt}".strip()
        if block:
            bio_blocks.append(block)
        if short_txt:
            short_blocks.append(f"{nm}: {short_txt}")
    for missing in missing_entities[:4]:
        bio_blocks.append(
            f"{missing}\nNo reliable artist biography found from configured providers for this credit."
        )
        short_blocks.append(f"{missing}: no reliable biography found")

    merged_profile = {
        "bio": "\n\n".join(bio_blocks).strip(),
        "short_bio": _truncate_text(" | ".join(short_blocks), max_chars=1200),
        "tags": _dedupe_keep_order(merged_tags)[:24],
        "similar": merged_similar[:30],
        "source": f"composite:{'+'.join(source_tokens)}" if source_tokens else "composite",
    }
    return merged_profile, (primary_lastfm or {}), (primary_wiki or {}), entities

def _merge_artist_profile_tags(*tag_sources: Any, limit: int = 20) -> list[str]:
    merged: list[str] = []
    for source in tag_sources:
        if not isinstance(source, (list, tuple)):
            continue
        merged.extend(str(item or "").strip() for item in source)
    return _dedupe_keep_order(merged)[: max(1, min(int(limit or 20), 40))]

def api_library_artist_profile(artist_id: int):
    """Return cached artist profile (bio/tags/similar) and trigger async enrichment when missing/stale."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Artist profile endpoint is available in Files mode only"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, name_norm, COALESCE(entity_kind, 'artist'), COALESCE(roles_json, '[]')
                FROM files_artists
                WHERE id = %s
                """,
                (artist_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Artist not found"}), 404
            artist_name = row[1] or ""
            artist_norm = str(row[2] or "").strip() or _norm_artist_key(artist_name)
            artist_entity_kind = str(row[3] or "artist").strip() or "artist"
            artist_roles = str(row[4] or "[]")
            cur.execute(
                """
                WITH artist_albums AS (
                    SELECT DISTINCT album_id
                    FROM files_artist_album_links
                    WHERE artist_id = %s
                )
                SELECT alb.title, alb.title_norm
                FROM artist_albums aa
                JOIN files_albums alb ON alb.id = aa.album_id
                ORDER BY COALESCE(year, 0) DESC, title ASC
                LIMIT 180
                """,
                (artist_id,),
            )
            albums = [(str(r[0] or ""), str(r[1] or "")) for r in cur.fetchall()]
        _files_ensure_local_artist_profile(
            conn,
            artist_id=int(artist_id),
            artist_name=artist_name,
            artist_norm=artist_norm,
            entity_kind=artist_entity_kind,
            roles_json=artist_roles,
        )
        profile = _files_get_artist_profile_cached(artist_name, artist_norm)
        force_refresh = str(request.args.get("refresh", "")).strip().lower() in {"1", "true", "yes"}
        enriching = _files_profile_job_is_active(artist_norm)
        needs_profile_refresh = _artist_profile_payload_requires_refresh(
            profile if isinstance(profile, dict) else None,
            entity_kind=artist_entity_kind,
            role_hints=_artist_role_hints_from_roles_json(artist_roles),
        )
        has_any_artist_image = False
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(a.image_path, ''), COALESCE(ext.image_path, '')
                    FROM files_artists a
                    LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                    WHERE a.id = %s
                    LIMIT 1
                    """,
                    (int(artist_id),),
                )
                media_row = cur.fetchone()
            for raw in (media_row or []):
                txt = str(raw or "").strip()
                if not txt:
                    continue
                try:
                    p = path_for_fs_access(Path(txt))
                except Exception:
                    p = Path(txt)
                if p.exists() and p.is_file():
                    has_any_artist_image = True
                    break
        except Exception:
            has_any_artist_image = False
        if force_refresh or needs_profile_refresh or not has_any_artist_image:
            enriching = _enqueue_files_profile_enrichment(
                artist_name,
                artist_norm,
                albums,
                force=True,
                fast_mode=False,
            ) or enriching

        # Attach local IDs + images to similar artists.
        base_url = request.url_root.rstrip("/")
        try:
            if isinstance(profile, dict):
                sim = profile.get("similar_artists")
                if isinstance(sim, list) and sim:
                    profile = dict(profile)
                    profile["similar_artists"] = _files_attach_similar_artist_refs(conn, sim, base_url)
                    missing_names = [
                        str(item.get("name") or "").strip()
                        for item in (profile.get("similar_artists") or [])
                        if isinstance(item, dict) and str(item.get("name") or "").strip() and not str(item.get("image_url") or "").strip()
                    ]
                    if missing_names:
                        _enqueue_files_similar_images_warm(artist_norm, missing_names[:12], force=True)
        except Exception:
            pass

        album_profiles = _files_get_album_profiles_cached(artist_norm, [norm for _, norm in albums if norm])
        return jsonify(
            {
                "artist_id": artist_id,
                "artist_name": artist_name,
                "artist_norm": artist_norm,
                "profile": profile,
                "album_profiles": album_profiles,
                "enriching": enriching,
            }
        )
    finally:
        conn.close()

def api_library_artist_ai_enrich(artist_id: int):
    """Trigger artist-level AI enrichment with soft-match safety."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, name_norm
                FROM files_artists
                WHERE id = %s
                """,
                (int(artist_id),),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Artist not found"}), 404
            artist_name = str(row[1] or "").strip()
            artist_norm = str(row[2] or "").strip() or _norm_artist_key(artist_name)
            cur.execute(
                """
                WITH artist_albums AS (
                    SELECT DISTINCT album_id
                    FROM files_artist_album_links
                    WHERE artist_id = %s
                )
                SELECT alb.id, alb.title, alb.title_norm
                FROM artist_albums aa
                JOIN files_albums alb ON alb.id = aa.album_id
                ORDER BY COALESCE(year, 0) DESC, title ASC
                LIMIT 300
                """,
                (int(artist_id),),
            )
            rows = cur.fetchall()
        albums = [(str(r[1] or ""), str(r[2] or "")) for r in rows if str(r[2] or "").strip()]
        started = _enqueue_files_profile_enrichment(
            artist_name,
            artist_norm,
            albums,
            allow_soft_profiles=True,
        )
        return jsonify(
            {
                "started": bool(started),
                "artist_id": int(artist_id),
                "artist_name": artist_name,
                "albums_total": int(len(rows)),
                "profiles_targeted": int(len(albums)),
                "mode": "ai_enrich_soft_safe",
            }
        )
    finally:
        conn.close()

def api_library_artist_facts(artist_id: int):
    """Return extracted structured facts for an artist (Files mode only)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Artist facts endpoint is available in Files mode only"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM files_artists WHERE id = %s", (int(artist_id),))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Artist not found"}), 404
            artist_name = (row[0] or "").strip()

            cur.execute(
                """
                SELECT facts_json, evidence_json, source, provider, model, updated_at
                FROM assistant_entity_facts
                WHERE entity_type = 'artist' AND entity_id = %s
                """,
                (int(artist_id),),
            )
            frow = cur.fetchone()
        if not frow:
            return jsonify(
                {
                    "artist_id": int(artist_id),
                    "artist_name": artist_name,
                    "facts": {},
                    "evidence": [],
                    "source": "",
                    "provider": "",
                    "model": "",
                    "updated_at": 0,
                }
            )
        try:
            facts = json.loads(frow[0] or "{}") if frow[0] else {}
        except (TypeError, ValueError):
            facts = {}
        try:
            evidence = json.loads(frow[1] or "[]") if frow[1] else []
        except (TypeError, ValueError):
            evidence = []
        if not isinstance(facts, dict):
            facts = {}
        if not isinstance(evidence, list):
            evidence = []
        return jsonify(
            {
                "artist_id": int(artist_id),
                "artist_name": artist_name,
                "facts": facts,
                "evidence": evidence,
                "source": str(frow[2] or "").strip(),
                "provider": str(frow[3] or "").strip(),
                "model": str(frow[4] or "").strip(),
                "updated_at": int(_dt_to_epoch(frow[5])) if frow[5] else 0,
            }
        )
    finally:
        conn.close()

def api_library_artist_facts_extract(artist_id: int):
    """Extract artist facts via AI (stored in PostgreSQL)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Artist facts endpoint is available in Files mode only"}), 400
    ai_ok, _provider_effective, _auth_mode, ai_reason = _resolve_ai_runtime_availability(
        analysis_type="assistant_chat",
        requested_provider="openai",
        user_id=_current_user_id_or_zero(),
    )
    if not ai_ok:
        msg = ai_reason or getattr(sys.modules[__name__], "AI_FUNCTIONAL_ERROR_MSG", None) or "AI is not configured"
        logging.warning(
            "[ArtistFacts] AI unavailable for facts/extract artist_id=%s user_id=%s provider=%s auth=%s reason=%s",
            int(artist_id or 0),
            _current_user_id_or_zero(),
            str(_provider_effective or ""),
            str(_auth_mode or ""),
            str(msg or ""),
        )
        return jsonify({"error": msg}), 503
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        context_info = _assistant_ingest_artist_rag(conn, int(artist_id))
        if not context_info:
            return jsonify({"error": "Artist not found"}), 404
        artist_name = (context_info.get("artist_name") or "").strip()

        # Pull best-effort context for grounded extraction.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT doc_type, source, title, url, content
                FROM assistant_docs
                WHERE entity_type = 'artist'
                  AND entity_id = %s
                  AND doc_type IN (
                    'artist_profile_bio',
                    'artist_profile_short',
                    'artist_summary_ai',
                    'artist_external_wikipedia_intro',
                    'artist_external_web_snippets'
                  )
                ORDER BY updated_at DESC
                """,
                (int(artist_id),),
            )
            rows = cur.fetchall()

        ctx_parts: list[str] = []
        evidence: list[dict] = []
        for dt, src, title, url, content in rows:
            dt_s = str(dt or "").strip()
            src_s = str(src or "").strip()
            title_s = str(title or "").strip()
            url_s = str(url or "").strip()
            text = (content or "").strip()
            if not text:
                continue
            # Keep context bounded to avoid runaway tokens.
            excerpt = text[:2400]
            ctx_parts.append(f"[{dt_s} source={src_s} title={title_s} url={url_s}]\n{excerpt}")
        ctx = "\n\n".join(ctx_parts) if ctx_parts else "(no context)"

        system_msg = (
            "You are PMDA Intelligence.\n"
            "Task: extract structured artist facts from the given context.\n"
            "Rules:\n"
            "- Use ONLY the provided context.\n"
            "- Do NOT invent facts.\n"
            "- Output must be STRICT JSON only.\n"
            "- If unknown, use empty arrays/empty strings.\n"
        )
        user_msg = (
            f"Artist: {artist_name}\n\n"
            "Return JSON with exactly these keys:\n"
            "{\n"
            '  "facts": {\n'
            '    "aka": [string],\n'
            '    "aliases": [string],\n'
            '    "member_of": [string],\n'
            '    "collaborated_with": [string],\n'
            '    "labels": [string],\n'
            '    "notable_cities": [string]\n'
            "  },\n"
            '  "evidence": [ { "fact_path": string, "excerpt": string, "source": string } ]\n'
            "}\n\n"
            "Context:\n"
            f"{ctx}\n"
        )

        provider = "openai"
        model = getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4o-mini")
        raw = call_ai_provider_longform(
            provider,
            model,
            system_msg,
            user_msg,
            max_tokens=800,
            analysis_type="assistant_chat",
        )
        raw = (raw or "").strip()
        if not raw:
            return jsonify({"error": "AI returned empty payload"}), 502
        parsed = None
        try:
            parsed = json.loads(raw)
        except Exception:
            # Attempt to salvage JSON from surrounding text.
            try:
                start = raw.find("{")
                end = raw.rfind("}")
                if start != -1 and end != -1 and end > start:
                    parsed = json.loads(raw[start : end + 1])
            except Exception:
                parsed = None
        if not isinstance(parsed, dict):
            return jsonify({"error": "AI returned invalid JSON"}), 502
        facts = parsed.get("facts") if isinstance(parsed.get("facts"), dict) else {}
        evidence = parsed.get("evidence") if isinstance(parsed.get("evidence"), list) else []

        facts_json = json.dumps(facts or {}, ensure_ascii=False)
        evidence_json = json.dumps(evidence or [], ensure_ascii=False)

        # For UI: keep a human-friendly "source" label (avoid exposing AI/provider internals).
        sources_used: list[str] = []
        try:
            for dt, src, _title, _url, _content in rows:
                dt_s = str(dt or "").strip().lower()
                src_s = str(src or "").strip()
                if dt_s == "artist_external_wikipedia_intro":
                    sources_used.append("wikipedia")
                elif dt_s == "artist_external_web_snippets":
                    sources_used.append("web")
                elif src_s:
                    sources_used.append(src_s)
        except Exception:
            sources_used = []
        sources_used = list(dict.fromkeys([s for s in sources_used if (s or "").strip()]))[:8]
        source_label = ", ".join(sources_used) if sources_used else "web"

        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO assistant_entity_facts(entity_type, entity_id, facts_json, evidence_json, source, provider, model, updated_at)
                    VALUES ('artist', %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (entity_type, entity_id) DO UPDATE SET
                        facts_json = EXCLUDED.facts_json,
                        evidence_json = EXCLUDED.evidence_json,
                        source = EXCLUDED.source,
                        provider = EXCLUDED.provider,
                        model = EXCLUDED.model,
                        updated_at = NOW()
                    """,
                    (int(artist_id), facts_json, evidence_json, source_label, str(provider), str(model)),
                )

        return jsonify(
            {
                "artist_id": int(artist_id),
                "artist_name": artist_name,
                "facts": facts,
                "evidence": evidence,
                "source": source_label,
                "provider": str(provider),
                "model": str(model),
                "updated_at": int(time.time()),
            }
        )
    finally:
        conn.close()

def api_library_files_artist_image(artist_id):
    """Serve artist image from files-library index (files mode)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    size = max(64, min(2048, _parse_int_loose(request.args.get("size"), 320)))
    version_hint = max(0, int(_parse_int_loose(request.args.get("v"), 0) or 0))
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    lookup_key = (
        f"artwork:artist:{int(artist_id)}:v{version_hint}"
        if version_hint > 0
        else f"artwork:artist:{int(artist_id)}"
    )
    lookup = _files_cache_get_json(lookup_key)
    name_norm = ""
    artist_name = ""
    entity_kind = "artist"
    roles_json = "[]"
    img_raw = ""
    ext_raw = ""
    ext_artist_name = ""
    ext_provider = ""
    ext_image_url = ""
    no_image_cached = False

    if isinstance(lookup, dict):
        name_norm = str(lookup.get("name_norm") or "").strip()
        artist_name = str(lookup.get("artist_name") or "").strip()
        entity_kind = str(lookup.get("entity_kind") or "artist").strip() or "artist"
        roles_json = str(lookup.get("roles_json") or "[]")
        img_raw = str(lookup.get("image_path") or "").strip()
        ext_raw = str(lookup.get("ext_image_path") or "").strip()
        ext_provider = str(lookup.get("ext_provider") or "").strip().lower()
        ext_image_url = str(lookup.get("ext_image_url") or "").strip()
        no_image_cached = bool(lookup.get("no_image"))
    if (
        (not name_norm)
        or (not artist_name)
        or ("ext_image_path" not in (lookup or {}) if isinstance(lookup, dict) else True)
        or ("ext_artist_name" not in (lookup or {}) if isinstance(lookup, dict) else True)
    ):
        conn = _files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        a.name_norm,
                        COALESCE(a.image_path, ''),
                        COALESCE(a.name, ''),
                        COALESCE(a.entity_kind, 'artist'),
                        COALESCE(a.roles_json, '[]'),
                        COALESCE(ext.image_path, ''),
                        COALESCE(ext.artist_name, ''),
                        COALESCE(ext.provider, ''),
                        COALESCE(ext.image_url, '')
                    FROM files_artists a
                    LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                    WHERE a.id = %s
                    LIMIT 1
                    """,
                    (artist_id,),
                )
                row = cur.fetchone()
            if row:
                name_norm = str(row[0] or "").strip()
                img_raw = str(row[1] or "").strip() or img_raw
                artist_name = str(row[2] or "").strip()
                entity_kind = str(row[3] or "artist").strip() or "artist"
                roles_json = str(row[4] or "[]")
                ext_raw = str(row[5] or "").strip()
                ext_artist_name = str(row[6] or "").strip()
                ext_provider = str(row[7] or "").strip().lower()
                ext_image_url = str(row[8] or "").strip()
            initial_no_image = not bool(img_raw) and not bool(ext_raw)
            _files_cache_set_json(
                lookup_key,
                {
                "name_norm": name_norm,
                "image_path": img_raw,
                "artist_name": artist_name,
                "entity_kind": entity_kind,
                "roles_json": roles_json,
                "ext_image_path": ext_raw,
                "ext_artist_name": ext_artist_name,
                "ext_provider": ext_provider,
                "ext_image_url": ext_image_url,
                "no_image": initial_no_image,
            },
                ttl=60 if initial_no_image else 3600,
            )
        finally:
            conn.close()

    role_hints = _artist_role_hints_from_roles_json(roles_json or "[]")
    if not ext_artist_name and isinstance(lookup, dict):
        ext_artist_name = str(lookup.get("ext_artist_name") or "").strip()
    ext_requires_refresh = False
    if name_norm and artist_name:
        conn = _files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.transaction():
                img_raw, ext_raw, ext_valid_exact = _files_reconcile_artist_image_cache_state(
                    conn,
                    artist_name=artist_name,
                    artist_norm=name_norm,
                    entity_kind=entity_kind,
                    role_hints=role_hints,
                    local_image_path=img_raw,
                    ext_image_path=ext_raw,
                    ext_artist_name=ext_artist_name,
                    ext_provider=ext_provider,
                    ext_image_url=ext_image_url,
                )
            ext_requires_refresh = bool(ext_raw and not ext_valid_exact)
        finally:
            conn.close()
        _files_cache_set_json(
            lookup_key,
            {
                "name_norm": name_norm,
                "image_path": img_raw,
                "artist_name": artist_name,
                "entity_kind": entity_kind,
                "roles_json": roles_json,
                "ext_image_path": ext_raw,
                "ext_artist_name": ext_artist_name,
                "ext_provider": ext_provider,
                "ext_image_url": ext_image_url,
                "no_image": not bool(img_raw or ext_raw),
            },
            ttl=60 if not (img_raw or ext_raw) else 3600,
        )
        if not (img_raw or ext_raw):
            no_image_cached = True

    if img_raw:
        img_path = path_for_fs_access(Path(img_raw))
        if img_path.exists() and img_path.is_file() and _is_media_cache_file(img_path, kind="artist"):
            cached = _ensure_cached_image_for_path(img_path, kind="artist", max_px=size)
            to_send = cached or img_path
            return _serve_image_file_cached(to_send, max_age=0, revalidate=True)
        elif img_path.exists() and img_path.is_file():
            cached = _ensure_cached_image_for_path(img_path, kind="artist", max_px=size)
            to_send = cached or img_path
            return _serve_image_file_cached(to_send, max_age=0, revalidate=True)
        else:
            logging.debug(
                "cache_miss_no_runtime_fallback: artist_id=%s image_path=%s",
                int(artist_id),
                str(img_path),
            )

    if name_norm:
        ext_key = f"artwork:artist:ext:{name_norm}"
        ext_lookup = _files_cache_get_json(ext_key)
        if isinstance(ext_lookup, dict):
            ext_raw = str(ext_lookup.get("image_path") or "").strip() or ext_raw
            ext_provider = str(ext_lookup.get("provider") or "").strip().lower() or ext_provider
            ext_image_url = str(ext_lookup.get("image_url") or "").strip() or ext_image_url
        if not ext_raw or not ext_provider:
            conn = _files_pg_connect()
            if conn is None:
                return jsonify({"error": "PostgreSQL unavailable"}), 503
            try:
                try:
                    _files_relink_external_artist_images_for_artist(
                        conn,
                        artist_name=artist_name,
                        artist_norm=name_norm,
                        alias_candidates=[],
                    )
                except Exception:
                    logging.debug("Artist image endpoint relink-by-name failed for %s", artist_name, exc_info=True)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COALESCE(image_path, ''), COALESCE(provider, ''), COALESCE(image_url, '')
                        FROM files_external_artist_images
                        WHERE name_norm = %s
                        LIMIT 1
                        """,
                        (name_norm,),
                    )
                    erow = cur.fetchone()
                ext_raw = str((erow[0] if erow else "") or "").strip()
                ext_provider = str((erow[1] if erow else "") or "").strip().lower()
                ext_image_url = str((erow[2] if erow else "") or "").strip()
                ext_no_image = not bool(ext_raw)
                _files_cache_set_json(
                    ext_key,
                    {
                        "image_path": ext_raw,
                        "provider": ext_provider,
                        "image_url": ext_image_url,
                        "no_image": ext_no_image,
                    },
                    ttl=60 if ext_no_image else 3600,
                )
            finally:
                conn.close()
        if ext_raw:
            ext_path = path_for_fs_access(Path(ext_raw))
            if ext_path.exists() and ext_path.is_file() and _is_media_cache_file(ext_path, kind="artist"):
                cached = _ensure_cached_image_for_path(ext_path, kind="artist", max_px=size)
                to_send = cached or ext_path
                _files_cache_set_json(
                    lookup_key,
                    {
                        "name_norm": name_norm,
                        "image_path": img_raw,
                        "artist_name": artist_name,
                        "entity_kind": entity_kind,
                        "roles_json": roles_json,
                        "ext_image_path": str(ext_path),
                        "ext_artist_name": ext_artist_name or artist_name,
                        "ext_provider": ext_provider,
                        "ext_image_url": ext_image_url,
                        "no_image": False,
                    },
                    ttl=3600,
                )
                return _serve_image_file_cached(to_send, max_age=0, revalidate=True)
            logging.debug(
                "cache_miss_no_runtime_fallback: artist_id=%s external_image_path=%s",
                int(artist_id),
                str(ext_path),
            )

    if no_image_cached:
        if artist_name and name_norm:
            _enqueue_files_profile_enrichment(
                artist_name,
                name_norm,
                [],
                force=True,
                fast_mode=False,
                priority_mode="p0",
            )
        return _transparent_png_response(max_age=0, revalidate=True)
    if artist_name and name_norm:
        _enqueue_files_profile_enrichment(
            artist_name,
            name_norm,
            [],
            force=True,
            fast_mode=False,
            priority_mode="p0",
        )
    _files_cache_set_json(
        lookup_key,
        {
            "name_norm": name_norm,
            "image_path": "",
            "artist_name": artist_name,
            "entity_kind": entity_kind,
            "roles_json": roles_json,
            "ext_image_path": ext_raw,
            "ext_artist_name": ext_artist_name,
            "ext_provider": ext_provider,
            "ext_image_url": ext_image_url,
            "no_image": True,
        },
        ttl=60,
    )
    return _transparent_png_response(max_age=0, revalidate=True)

def api_library_external_artist_image(name_norm: str):
    """Serve cached external artist images (not necessarily in local library)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    size = max(64, min(2048, _parse_int_loose(request.args.get("size"), 320)))
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    key = _norm_artist_key(str(name_norm or ""))
    if not key:
        return jsonify({"error": "Invalid name"}), 400
    display_name = str(request.args.get("name") or "").strip()
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        ext_row = _files_get_external_artist_images(conn, [key]).get(key) or {}
        img_raw = str(ext_row.get("image_path") or "").strip()
        if not img_raw:
            artist_name = display_name
            entity_kind = "artist"
            role_hints: list[str] = []
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            COALESCE(name, ''),
                            COALESCE(entity_kind, 'artist'),
                            COALESCE(roles_json, '[]')
                        FROM files_artists
                        WHERE name_norm = %s
                        LIMIT 1
                        """,
                        (key,),
                    )
                    row = cur.fetchone()
                if row:
                    artist_name = str(row[0] or "").strip() or artist_name
                    entity_kind = str(row[1] or "artist").strip() or "artist"
                    role_hints = _artist_role_hints_from_roles_json(row[2] or "[]")
            except Exception:
                artist_name = display_name
                entity_kind = "artist"
                role_hints = []
            if artist_name:
                try:
                    _files_try_artist_image_refresh(
                        artist_name=artist_name,
                        artist_norm=key,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                        fast_mode=False,
                    )
                    ext_row = _files_get_external_artist_images(conn, [key]).get(key) or {}
                    img_raw = str(ext_row.get("image_path") or "").strip()
                except Exception:
                    img_raw = ""
            if display_name:
                _enqueue_files_similar_images_warm(key, [display_name], force=True)
            if not img_raw:
                remote_url = str(ext_row.get("image_url") or "").strip()
                if remote_url:
                    try:
                        return redirect(remote_url, code=302)
                    except Exception:
                        pass
            return _transparent_png_response(max_age=0, revalidate=True)
        p = Path(img_raw)
        if not p.exists() or not p.is_file() or not _is_media_cache_file(p, kind="artist"):
            if display_name:
                _enqueue_files_similar_images_warm(key, [display_name], force=True)
            return _transparent_png_response(max_age=0, revalidate=True)
        cached = _ensure_cached_image_for_path(p, kind="artist", max_px=size)
        to_send = cached or p
        return _serve_image_file_cached(to_send, max_age=0, revalidate=True)
    finally:
        conn.close()

def get_artist_images_mb(artist_mbid: str) -> List[str]:
    """Get artist images from MusicBrainz/Wikimedia."""
    if not USE_MUSICBRAINZ:
        return []

    try:
        result = musicbrainzngs.get_artist_by_id(
            artist_mbid,
            includes=["url-rels"]
        )
        image_urls = []
        artist_data = result.get("artist", {})
        url_relations = artist_data.get("url-relation-list", [])

        for url_rel in url_relations:
            target = str(url_rel.get("target") or "").strip()
            target_low = target.lower()
            if (
                "wikimedia" in target_low
                or "commons.wikimedia" in target_low
                or "wikipedia.org" in target_low
            ):
                image_urls.append(target)

        return image_urls
    except Exception as e:
        logging.error("Failed to get artist images for MBID %s: %s", artist_mbid, e)
        return []

def _is_probably_placeholder_artist_image_url(url: str) -> bool:
    low = (url or "").strip().lower()
    if not low:
        return True
    # Known Last.fm "missing artist image" hashes frequently returned for unknown artists.
    placeholder_tokens = (
        "2a96cbd8b46e442fc41c2b86b821562f",
        "4128a6eb29f94943c9d206c08e625904",
        "c6f59c1e5e7240a4c0d427abd71f3dbb",
        # Generic placeholder patterns from various CDNs/search results.
        "placeholder-artist",
        "/placeholders/",
        "default_avatar",
        "default-avatar",
        "noimage",
        "no-image",
        "blank.jpg",
        "spacer.gif",
        "transparent.png",
    )
    if any(tok in low for tok in placeholder_tokens):
        return True
    if "default" in low and ("last.fm" in low or "lastfm" in low):
        return True
    # Common known placeholder endpoints.
    if "static-images.merchbar.com" in low and "/placeholders/" in low:
        return True
    if "s0.wp.com/i/blank.jpg" in low:
        return True
    return False

def _is_suspicious_external_artist_image_url(url: str) -> bool:
    low = (url or "").strip().lower()
    if not low:
        return False
    suspicious_tokens = (
        "soundtrack",
        "coverartarchive",
        "album-cover",
        "albumcover",
        "album_art",
        "cover-art",
        "record-cover",
        "recording-cover",
        "facebook_share",
        "allmusic_facebook_share",
        "sharecard",
        "share-card",
        "logo",
        "signet",
        "%28album%29",
        "%28ep%29",
        "/album/",
        "/release/",
        "/cover/",
        "/covers/",
        ".svg",
    )
    if any(tok in low for tok in suspicious_tokens):
        return True
    if "i.scdn.co/image/" in low and "ab67616d" in low:
        return True
    return False

def _artist_image_provider_allowed_for_entity(
    provider: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> bool:
    provider_low = str(provider or "").strip().lower()
    if not provider_low:
        return False
    return provider_low in {
        "audiodb",
        "bandcamp",
        "discogs",
        "fanart",
        "lastfm",
        "musicbrainz",
        "musicbrainz_url",
        "wikipedia",
    }

def _is_usable_artist_image_bytes(raw: bytes, *, min_dim: int = 220, min_bytes: int = 8192) -> bool:
    if not raw or len(raw) < min_bytes:
        return False
    try:
        from PIL import Image
        from io import BytesIO

        with Image.open(BytesIO(raw)) as img:
            w, h = img.size
            if w < min_dim or h < min_dim:
                return False
            # Reject quasi-flat placeholders (very low color diversity).
            thumb = img.convert("RGB").resize((32, 32), Image.Resampling.BILINEAR)
            colors = thumb.getcolors(maxcolors=4096)
            if colors is not None and len(colors) < 20:
                return False
    except Exception:
        # If we cannot inspect image content, keep previous permissive behavior.
        return True
    return True

def _is_usable_artist_image_path(path: Path, *, min_dim: int = 220, min_bytes: int = 8192) -> bool:
    """
    Validate an on-disk cached artist image without loading it through the "download" path.
    Used to decide whether an existing cache entry is good enough to keep.
    """
    try:
        if not path or not path.is_file():
            return False
        st = path.stat()
        if int(st.st_size or 0) < int(min_bytes):
            return False
    except Exception:
        return False
    try:
        from PIL import Image

        with Image.open(path) as img:
            w, h = img.size
            if w < min_dim or h < min_dim:
                return False
            thumb = img.convert("RGB").resize((32, 32), Image.Resampling.BILINEAR)
            colors = thumb.getcolors(maxcolors=4096)
            if colors is not None and len(colors) < 20:
                return False
    except Exception:
        # If we cannot inspect image content, keep permissive behavior.
        return True
    return True

def _is_artist_image_distinct_from_local_covers(artist_folder: Path, candidate_raw: bytes) -> bool:
    """
    Reject candidate artist images that are near-identical to local album covers for the same artist.
    """
    if not artist_folder or not artist_folder.is_dir():
        return True
    cand_hash = _image_ahash_hex(candidate_raw)
    if not cand_hash:
        return True
    compared = 0
    try:
        for child in sorted(artist_folder.iterdir(), key=lambda p: str(p)):
            if not child.is_dir():
                continue
            for pat in ("cover.*", "folder.*", "album.*", "artwork.*", "front.*"):
                for cover in child.glob(pat):
                    if cover.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp"}:
                        continue
                    try:
                        raw = cover.read_bytes()
                    except Exception:
                        continue
                    cover_hash = _image_ahash_hex(raw)
                    if not cover_hash:
                        continue
                    dist = _hamming_hex(cand_hash, cover_hash)
                    compared += 1
                    if dist <= 4:
                        logging.debug(
                            "Artist image candidate rejected: too similar to album cover (%s, distance=%s)",
                            cover,
                            dist,
                        )
                        return False
                    if compared >= 12:
                        return True
    except Exception:
        return True
    return True

def _fetch_and_save_artist_image_mb(artist_mbid: str, artist_folder: Path) -> bool:
    """Fetch first artist image from MusicBrainz/Wikimedia and save to artist_folder/artist.jpg. Returns True if saved."""
    urls = get_artist_images_mb(artist_mbid)
    for url in urls:
        try:
            resp = requests.get(url, timeout=10, allow_redirects=True)
            if resp.status_code != 200:
                continue
            ct = resp.headers.get("content-type", "").lower()
            if "image/" in ct:
                if not _is_usable_artist_image_bytes(resp.content):
                    continue
                if not _is_artist_image_distinct_from_local_covers(artist_folder, resp.content):
                    continue
                ext = ".jpg"
                if "png" in ct:
                    ext = ".png"
                out = artist_folder / f"artist{ext}"
                out.write_bytes(resp.content)
                logging.info("Saved artist image from MusicBrainz to %s", out)
                return True
            # If HTML (e.g. Commons page), try og:image
            if "text/html" in ct:
                m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', resp.text)
                if m:
                    img_url = m.group(1).strip()
                    img_resp = requests.get(img_url, timeout=10, allow_redirects=True)
                    if img_resp.status_code == 200 and "image/" in img_resp.headers.get("content-type", "").lower():
                        if not _is_usable_artist_image_bytes(img_resp.content):
                            continue
                        if not _is_artist_image_distinct_from_local_covers(artist_folder, img_resp.content):
                            continue
                        (artist_folder / "artist.jpg").write_bytes(img_resp.content)
                        logging.info("Saved artist image from Wikimedia to %s", artist_folder / "artist.jpg")
                        return True
        except Exception as e:
            logging.warning("Artist image fetch (MB) failed for %s: %s", url, e)
    return False

def _fetch_artist_image_lastfm(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    alias_candidates: list[str] | tuple[str, ...] | None = None,
) -> Optional[str]:
    """Get largest artist image URL from Last.fm artist.getInfo. Returns URL or None."""
    try:
        info = _fetch_lastfm_artist_info(artist_name) or {}
        matched_name = str(info.get("matched_name") or artist_name).strip()
        if not _artist_image_exact_name_match(
            artist_name,
            matched_name,
            entity_kind=entity_kind,
            role_hints=role_hints,
            alias_candidates=alias_candidates,
        ):
            return None
        url = str(info.get("image_url") or "").strip()
        if not url or _is_probably_placeholder_artist_image_url(url):
            return None
        return url
    except Exception as e:
        logging.warning("Last.fm artist.getInfo failed for %s: %s", artist_name, e)
        return None

def _fetch_artist_image_discogs(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    alias_candidates: list[str] | tuple[str, ...] | None = None,
) -> Optional[str]:
    """Get artist image URL from Discogs (search artist, first result). Returns URL or None."""
    if not USE_DISCOGS:
        return None
    try:
        d = _get_discogs_client()
        if d is None:
            return None
        results = d.search(artist_name, type="artist")
        page = _discogs_call("artist search page=1", lambda: results.page(1))
        if not page:
            return None
        for artist in page[:10]:
            artist_data = getattr(artist, "data", None)
            cand_name = ""
            if isinstance(artist_data, dict):
                cand_name = str(artist_data.get("title") or artist_data.get("name") or "").strip()
            if not cand_name:
                cand_name = str(getattr(artist, "name", "") or "").strip()
            if not _artist_image_exact_name_match(
                artist_name,
                cand_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                alias_candidates=alias_candidates,
            ):
                continue
            artist_id = getattr(artist, "id", None) or (artist_data.get("id") if isinstance(artist_data, dict) else None)
            if not artist_id:
                continue
            full_id = int(getattr(artist_id, "id", artist_id))
            full_data = _discogs_call(f"artist {full_id} data", lambda aid=full_id: d.artist(aid).data)
            if isinstance(full_data, dict):
                images = full_data.get("images") or []
                if isinstance(images, list) and images:
                    img = images[0] if isinstance(images[0], dict) else None
                    if img:
                        return (img.get("uri") or img.get("resource_url") or "").strip() or None
        return None
    except DiscogsRateLimited:
        # Already logged + backed off in _discogs_call.
        return None
    except Exception as e:
        logging.debug("Discogs artist image fetch failed for %s: %s", artist_name, e)
        return None

def _fetch_artist_image_fanart(artist_mbid: str) -> Optional[str]:
    """
    Fetch an artist image from fanart.tv using a MusicBrainz artist MBID.
    Returns a direct image URL (usually `artistthumb`) or None.
    """
    api_key = (getattr(sys.modules[__name__], "FANART_API_KEY", "") or "").strip()
    mbid = str(artist_mbid or "").strip()
    if not api_key or not mbid or len(mbid) != 36:
        return None
    try:
        resp = requests.get(
            f"https://webservice.fanart.tv/v3/music/{mbid}",
            params={"api_key": api_key},
            timeout=10,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None
        data = resp.json() if resp.content else {}
        if not isinstance(data, dict):
            return None

        def _pick_best(list_val: object) -> str:
            if not isinstance(list_val, list):
                return ""
            best_url = ""
            best_likes = -1
            for it in list_val:
                if not isinstance(it, dict):
                    continue
                url = str(it.get("url") or "").strip()
                if not url:
                    continue
                try:
                    if _is_probably_placeholder_artist_image_url(url):
                        continue
                except Exception:
                    pass
                likes = _parse_int_loose(it.get("likes"), 0)
                if likes > best_likes:
                    best_likes = likes
                    best_url = url
            return best_url

        # Prefer an artist thumb (portrait/headshot), fall back to background art when needed.
        url = _pick_best(data.get("artistthumb"))
        if url:
            return url
        url = _pick_best(data.get("artistbackground"))
        if url:
            return url
        return None
    except Exception as e:
        logging.debug("Fanart.tv artist image fetch failed for MBID %s: %s", mbid, e)
        return None

def _extract_artist_mbid_from_mb_payload(payload: dict | None) -> str:
    """Best-effort extraction of the first artist MBID from MB release or release-group payloads."""
    if not isinstance(payload, dict):
        return ""
    for item in (payload.get("artist-credit") or []):
        if not isinstance(item, dict):
            continue
        artist = item.get("artist") or {}
        if isinstance(artist, dict):
            aid = str(artist.get("id") or "").strip()
            if aid:
                return aid
    return ""

def _resolve_artist_mbid_for_fanart(
    *,
    artist_name: str,
    artist_mbid: str | None = None,
    musicbrainz_id: str | None = None,
    discogs_release_id: str | None = None,
    lastfm_album_mbid: str | None = None,
    bandcamp_album_url: str | None = None,
) -> str:
    """
    Resolve an artist MBID for Fanart.tv.

    Fanart requires an artist MBID, so when a trusted album identity exists but
    the artist MBID is missing, resolve it from MB release/release-group payloads
    or Last.fm artist info.
    """
    mbid = str(artist_mbid or "").strip()
    if len(mbid) == 36:
        return mbid

    trusted = _has_trusted_album_identity(
        musicbrainz_id=musicbrainz_id,
        discogs_release_id=discogs_release_id,
        lastfm_album_mbid=lastfm_album_mbid,
        bandcamp_album_url=bandcamp_album_url,
    )
    if not trusted:
        return ""

    release_like_ids: list[str] = []
    for candidate in (musicbrainz_id, lastfm_album_mbid):
        cid = str(candidate or "").strip()
        if cid and cid not in release_like_ids:
            release_like_ids.append(cid)

    for rel_id in release_like_ids:
        if len(rel_id) != 36 or not USE_MUSICBRAINZ:
            continue
        try:
            release_data = musicbrainzngs.get_release_by_id(rel_id, includes=["artists"])["release"]
            aid = _extract_artist_mbid_from_mb_payload(release_data)
            if aid:
                return aid
        except Exception:
            pass
        try:
            rg_data = musicbrainzngs.get_release_group_by_id(rel_id, includes=["artist-credits"])["release-group"]
            aid = _extract_artist_mbid_from_mb_payload(rg_data)
            if aid:
                return aid
        except Exception:
            pass

    if USE_LASTFM:
        try:
            lf = _fetch_lastfm_artist_info(artist_name) or {}
            aid = str(lf.get("mbid") or "").strip()
            if aid and len(aid) == 36:
                return aid
        except Exception:
            pass

    return ""

def _fetch_artist_image_audiodb(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    alias_candidates: list[str] | tuple[str, ...] | None = None,
) -> Optional[str]:
    """
    Fetch an artist image from TheAudioDB by name search.
    Returns a direct image URL (`strArtistThumb` preferred) or None.
    """
    api_key = (getattr(sys.modules[__name__], "THEAUDIODB_API_KEY", "") or "").strip()
    name = str(artist_name or "").strip()
    if not api_key or not name:
        return None
    try:
        resp = requests.get(
            f"https://www.theaudiodb.com/api/v1/json/{api_key}/search.php",
            params={"s": name},
            timeout=10,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None
        data = resp.json() if resp.content else {}
        if not isinstance(data, dict):
            return None
        artists = data.get("artists") or []
        if not isinstance(artists, list) or not artists:
            return None
        best_url = ""
        best_score = 0.0
        for a in artists[:10]:
            if not isinstance(a, dict):
                continue
            cand_name = str(a.get("strArtist") or "").strip()
            if not cand_name:
                continue
            if not _artist_image_exact_name_match(
                name,
                cand_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                alias_candidates=alias_candidates,
            ):
                continue
            score = _provider_identity_text_score(name, cand_name)
            if score < 0.78:
                continue
            url = (
                str(a.get("strArtistThumb") or "").strip()
                or str(a.get("strArtistFanart") or "").strip()
                or str(a.get("strArtistFanart2") or "").strip()
            )
            if not url:
                continue
            try:
                if _is_probably_placeholder_artist_image_url(url):
                    continue
            except Exception:
                pass
            if score > best_score:
                best_score = score
                best_url = url
        return best_url or None
    except Exception as e:
        logging.debug("TheAudioDB artist image fetch failed for %s: %s", name, e)
        return None

def _artist_image_search_queries(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> list[str]:
    name = str(artist_name or "").strip()
    if not name:
        return []
    kind = str(entity_kind or "").strip().lower()
    role_norms = {
        str(role or "").strip().lower()
        for role in (role_hints or [])
        if str(role or "").strip()
    }
    qualifiers: list[str] = []
    if kind in {"composer"} or "composer" in role_norms:
        qualifiers = ["composer portrait", "composer photo", "composer official portrait", "musician portrait", "wikipedia"]
    elif kind in {"conductor"} or "conductor" in role_norms:
        qualifiers = ["conductor portrait", "conductor photo", "conductor official portrait", "official biography photo", "wikipedia"]
    elif kind in {"orchestra"} or "orchestra" in role_norms:
        qualifiers = ["orchestra official photo", "official orchestra photo", "orchestra musicians photo", "orchestra press photo", "symphony orchestra photo", "wikipedia"]
    elif kind in {"ensemble", "choir", "chorus"} or role_norms.intersection({"ensemble", "choir", "chorus"}):
        qualifiers = ["ensemble official photo", "official ensemble photo", "ensemble musicians photo", "ensemble press photo", "group photo", "wikipedia"]
    elif kind in {"band"} or "band" in role_norms:
        qualifiers = ["band photo", "band promo photo", "group photo"]
    else:
        qualifiers = ["musician photo", "artist photo", "portrait"]
    queries = [f"{name} {qualifier}" for qualifier in qualifiers]
    for base in _artist_profile_search_queries(name, entity_kind=entity_kind, role_hints=role_hints):
        if base and base not in queries:
            queries.append(base)
    queries.append(name)
    out: list[str] = []
    seen: set[str] = set()
    for query in queries:
        clean = re.sub(r"\s+", " ", str(query or "").strip())
        key = clean.lower()
        if clean and key not in seen:
            seen.add(key)
            out.append(clean)
    return out

def _artist_image_lookup_candidates(
    artist_name: str,
    candidates: list[str] | tuple[str, ...],
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    limit: int = 12,
) -> list[str]:
    primary = " ".join(str(artist_name or "").split()).strip()
    primary_norm = _norm_artist_key(primary)
    classical_like = bool(
        _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints)
        or str(entity_kind or "").strip().lower() in {"ensemble", "conductor", "composer", "orchestra", "choir", "chorus"}
        or bool({str(role or "").strip().lower() for role in (role_hints or []) if str(role or "").strip()}.intersection({"orchestra", "ensemble", "choir", "chorus"}))
    )
    out: list[str] = []
    seen: set[str] = set()
    for raw in [primary, *(candidates or [])]:
        clean = " ".join(str(raw or "").split()).strip()
        norm = _norm_artist_key(clean)
        if not clean or not norm or norm in seen:
            continue
        if norm != primary_norm and classical_like:
            token_count = len([tok for tok in re.findall(r"[a-z0-9]+", norm) if tok])
            if len(norm) < 6 or token_count < 2:
                continue
            if not _artist_image_alias_candidate_is_compatible(
                primary,
                clean,
                entity_kind=entity_kind,
                role_hints=role_hints,
            ):
                continue
        seen.add(norm)
        out.append(clean)
        if len(out) >= max(2, int(limit or 12)):
            break
    return out

def _artist_image_alias_candidate_is_compatible(
    artist_name: str,
    candidate_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> bool:
    primary = " ".join(str(artist_name or "").split()).strip()
    candidate = " ".join(str(candidate_name or "").split()).strip()
    if not primary or not candidate:
        return False
    primary_norm = _norm_artist_key(primary)
    candidate_norm = _norm_artist_key(candidate)
    if not primary_norm or not candidate_norm:
        return False
    if primary_norm == candidate_norm:
        return True
    if _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints):
        if _classical_person_names_equivalent(primary, candidate):
            return True
        primary_sig = _classical_person_alias_signature(primary)
        candidate_sig = _classical_person_alias_signature(candidate)
        if primary_sig and candidate_sig:
            if str(primary_sig.get("surname") or "").strip() != str(candidate_sig.get("surname") or "").strip():
                return False
            primary_long = {str(tok or "").strip() for tok in (primary_sig.get("long_givens") or set()) if str(tok or "").strip()}
            candidate_long = {str(tok or "").strip() for tok in (candidate_sig.get("long_givens") or set()) if str(tok or "").strip()}
            if primary_long and candidate_long and primary_long.intersection(candidate_long):
                return True
            primary_initials = {str(tok or "").strip() for tok in (primary_sig.get("initials") or set()) if str(tok or "").strip()}
            candidate_initials = {str(tok or "").strip() for tok in (candidate_sig.get("initials") or set()) if str(tok or "").strip()}
            if primary_initials and candidate_initials and primary_initials.intersection(candidate_initials):
                return True
            if primary_long and candidate_long and (primary_long <= candidate_long or candidate_long <= primary_long):
                return True
            return False
        if primary_sig:
            surname = str(primary_sig.get("surname") or "").strip()
            candidate_tokens = {tok for tok in re.findall(r"[a-z0-9]+", candidate_norm) if tok}
            if surname and surname not in candidate_tokens:
                return False
            primary_long = {str(tok or "").strip() for tok in (primary_sig.get("long_givens") or set()) if str(tok or "").strip()}
            if primary_long and primary_long.intersection(candidate_tokens):
                return True
            primary_initials = {str(tok or "").strip() for tok in (primary_sig.get("initials") or set()) if str(tok or "").strip()}
            candidate_initials = {tok[:1] for tok in candidate_tokens if tok}
            if primary_initials and candidate_initials and primary_initials.intersection(candidate_initials):
                return True
            return candidate_norm == primary_norm
        return _provider_identity_text_score(primary, candidate) >= 0.9
    primary_tokens = _artist_identity_distinctive_tokens(primary, entity_kind=entity_kind, role_hints=role_hints)
    candidate_tokens = _artist_identity_distinctive_tokens(candidate, entity_kind=entity_kind, role_hints=role_hints)
    overlap = primary_tokens.intersection(candidate_tokens)
    ensemble_like = bool(
        str(entity_kind or "").strip().lower() in {"orchestra", "ensemble", "choir", "chorus"}
        or bool({str(role or "").strip().lower() for role in (role_hints or []) if str(role or "").strip()}.intersection({"orchestra", "ensemble", "choir", "chorus"}))
    )
    if ensemble_like:
        if len(overlap) >= 2:
            return True
        if overlap and _provider_identity_text_score(primary, candidate) >= 0.86:
            return True
        return _provider_identity_text_score(primary, candidate) >= 0.9
    if primary_tokens and candidate_tokens and overlap:
        return True
    return _provider_identity_text_score(primary, candidate) >= 0.8

def _artist_image_exact_name_match(
    artist_name: str,
    candidate_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    alias_candidates: list[str] | tuple[str, ...] | None = None,
) -> bool:
    primary = " ".join(str(artist_name or "").split()).strip()
    candidate = " ".join(str(candidate_name or "").split()).strip()
    if not primary or not candidate:
        return False
    candidate_norm = _norm_artist_key(candidate)
    if not candidate_norm:
        return False
    accepted_names: list[str] = [primary]
    accepted_names.extend([str(value or "").strip() for value in (alias_candidates or []) if str(value or "").strip()])
    for accepted in accepted_names:
        accepted_norm = _norm_artist_key(accepted)
        if not accepted_norm:
            continue
        if accepted_norm == candidate_norm:
            return True
        if _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints):
            try:
                if _classical_person_names_equivalent(accepted, candidate):
                    return True
            except Exception:
                pass
    return False

def _fetch_wikipedia_artist_bio_best(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    candidate_names: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    for lang in _artist_wikipedia_lang_candidates(entity_kind=entity_kind, role_hints=role_hints):
        found = _fetch_wikipedia_artist_bio(
            artist_name,
            lang=lang,
            entity_kind=entity_kind,
            role_hints=role_hints,
            candidate_names=candidate_names,
        )
        if isinstance(found, dict) and found:
            return found
    return {}

def _artist_image_url_looks_relevant(
    url: str,
    *,
    artist_name: str,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    page_title: str = "",
    page_summary: str = "",
) -> bool:
    target_url = str(url or "").strip()
    if not target_url:
        return False
    try:
        if _is_probably_placeholder_artist_image_url(target_url) or _is_suspicious_external_artist_image_url(target_url):
            return False
    except Exception:
        return False
    parsed = urlparse(target_url)
    host = (parsed.netloc or "").strip().lower()
    wiki_like = any(host == domain or host.endswith(f".{domain}") for domain in ("wikipedia.org", "wikimedia.org"))
    decoded_path = unquote(parsed.path or "")
    basename = Path(decoded_path).stem
    basename_norm = _norm_artist_key(re.sub(r"[_-]+", " ", basename))
    basename_tokens = {tok for tok in re.findall(r"[a-z0-9]+", basename_norm) if tok}
    page_title_clean = _artist_image_page_identity_candidate(page_title)
    basename_candidate = _artist_image_page_identity_candidate(basename)
    distinctive_tokens = _artist_identity_distinctive_tokens(
        artist_name,
        entity_kind=entity_kind,
        role_hints=role_hints,
    )
    overlap = distinctive_tokens.intersection(basename_tokens)
    context_blob = "\n".join(
        part.strip()
        for part in (page_title, page_summary)
        if str(part or "").strip()
    )
    context_relevant = _text_mentions_identity_phrase(str(artist_name or "").strip(), context_blob)
    building_like = bool(basename_tokens.intersection(_ARTIST_IMAGE_BUILDING_TOKENS))
    kind = str(entity_kind or "").strip().lower()
    roles = {str(role or "").strip().lower() for role in (role_hints or []) if str(role or "").strip()}
    person_like = _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints) or kind in {"composer", "conductor"}
    ensemble_like = kind in {"orchestra", "ensemble", "choir", "chorus"} or roles.intersection({"orchestra", "ensemble", "choir", "chorus"})
    classical_like = _artist_entity_is_classical_like(entity_kind=entity_kind, role_hints=role_hints)
    albumish_tokens = {"album", "cover", "soundtrack", "single", "release", "recording"}
    event_like = bool(basename_tokens.intersection(_ARTIST_IMAGE_EVENT_TOKENS))
    taxonomy_like = bool(
        basename_tokens.intersection(
            {
                "wasp",
                "bee",
                "hornet",
                "moth",
                "beetle",
                "insect",
                "species",
                "genus",
                "spider",
                "fly",
                "bug",
            }
        )
    )
    trusted_domains = ("fanart.tv", "theaudiodb.com", "last.fm", "discogs.com", "wikimedia.org", "wikipedia.org", "musicbrainz.org")
    trusted_host = bool(
        host
        and any(host == domain or host.endswith(f".{domain}") for domain in trusted_domains)
    )
    if basename_tokens.intersection(albumish_tokens):
        return False
    if taxonomy_like:
        return False
    if "logo" in basename_tokens or "signet" in basename_tokens or "svg" in basename_tokens:
        return False
    if host == "cf.allmusic.com" and "facebook_share" in target_url.lower():
        return False
    if host == "i.scdn.co":
        low_url = target_url.lower()
        if "ab67616d" in low_url:
            return False
        if ensemble_like:
            return False
        if person_like and "ab676161" in low_url:
            return True
    if classical_like and not wiki_like:
        return False
    if not wiki_like:
        if ensemble_like and building_like:
            return False
        if building_like and not overlap and not context_relevant:
            return False
        if person_like:
            if basename_candidate and _artist_image_alias_candidate_is_compatible(
                artist_name,
                basename_candidate,
                entity_kind=entity_kind,
                role_hints=role_hints,
            ):
                return True
            if (
                page_title_clean
                and trusted_host
                and _artist_image_alias_candidate_is_compatible(
                    artist_name,
                    page_title_clean,
                    entity_kind=entity_kind,
                    role_hints=role_hints,
                )
            ):
                return True
            person_context = context_blob.lower()
            musician_terms = ("composer", "conductor", "musician", "pianist", "violinist", "maestro", "artist", "portrait")
            if (
                page_title_clean
                and _artist_image_alias_candidate_is_compatible(
                    artist_name,
                    page_title_clean,
                    entity_kind=entity_kind,
                    role_hints=role_hints,
                )
                and context_relevant
                and trusted_host
                and overlap
                and any(term in person_context for term in musician_terms)
            ):
                return True
            return False
        if ensemble_like:
            if event_like or building_like:
                return False
            if basename_candidate and _artist_image_alias_candidate_is_compatible(
                artist_name,
                basename_candidate,
                entity_kind=entity_kind,
                role_hints=role_hints,
            ):
                return True
            return False
        if host and not trusted_host:
            if not overlap and not context_relevant:
                return False
        return True
    if person_like:
        if page_title_clean and _artist_image_alias_candidate_is_compatible(
            artist_name,
            page_title_clean,
                entity_kind=entity_kind,
                role_hints=role_hints,
            ):
            return True
        if basename_candidate and _artist_image_alias_candidate_is_compatible(
            artist_name,
            basename_candidate,
            entity_kind=entity_kind,
            role_hints=role_hints,
        ):
            return True
        return False
    if ensemble_like:
        if page_title_clean and _artist_image_alias_candidate_is_compatible(
            artist_name,
            page_title_clean,
            entity_kind=entity_kind,
            role_hints=role_hints,
        ):
            return True
        if basename_candidate and _artist_image_alias_candidate_is_compatible(
            artist_name,
            basename_candidate,
            entity_kind=entity_kind,
            role_hints=role_hints,
        ):
            return True
        return False
    if overlap:
        return True
    if building_like and not context_relevant:
        return False
    return context_relevant

def _artist_image_result_looks_relevant(
    artist_name: str,
    result: dict[str, Any] | None,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    candidate_names: list[str] | tuple[str, ...] | None = None,
) -> bool:
    if not isinstance(result, dict):
        return False
    title = str(result.get("title") or "").strip()
    snippet = str(result.get("snippet") or "").strip()
    link = str(result.get("link") or "").strip()
    merged = f"{title} {snippet} {link}".lower()
    host = (urlparse(link).netloc or "").strip().lower()
    if not _artist_profile_text_matches_any_identity(
        str(artist_name or "").strip(),
        f"{title} {snippet}".strip(),
        entity_kind=entity_kind,
        role_hints=role_hints,
        candidate_names=candidate_names,
    ):
        return False
    reject_tokens = (
        "discogs release",
        "release page",
        "album review",
        "vinyl",
        "cd ",
        "bandcamp album",
        "track listing",
        "tracklist",
        "cover art",
        "album cover",
        "recording cover",
        "official logo",
        "brand identity",
        "visual identity",
        "monogram",
    )
    if any(tok in merged for tok in reject_tokens):
        return False
    if " logo" in f" {merged}" or ".svg" in merged or "/logo" in merged:
        return False
    kind = str(entity_kind or "").strip().lower()
    role_norms = {
        str(role or "").strip().lower()
        for role in (role_hints or [])
        if str(role or "").strip()
    }
    person_like = _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints) or kind in {"composer", "conductor"}
    ensemble_like = kind in {"orchestra", "ensemble", "choir", "chorus"} or role_norms.intersection({"orchestra", "ensemble", "choir", "chorus"})
    trusted_domains = ("wikipedia.org", "wikimedia.org", "wikidata.org", "musicbrainz.org", "last.fm", "discogs.com", "theaudiodb.com")
    if ensemble_like:
        if any(tok in merged for tok in ("skyline", "skyscraper", "empire state", "observation deck", "real estate", "concert hall", "concertgebouw", "concert", "konzert", "festival", "album", "release", "recording", "tracklist", "vinyl", "cd ")):
            return False
        if title and not _artist_image_alias_candidate_is_compatible(
            artist_name,
            re.sub(r"\s*\([^)]*\)\s*$", "", title).strip(),
            entity_kind=entity_kind,
            role_hints=role_hints,
        ):
            return False
        if any(host == domain or host.endswith(f".{domain}") for domain in trusted_domains):
            return True
        return any(tok in merged for tok in ("orchestra", "philharmonic", "symphony", "ensemble", "choir", "chorus", "group"))
    if person_like:
        title_base = re.sub(r"\s*\([^)]*\)\s*$", "", title).strip()
        if title_base and not _artist_image_alias_candidate_is_compatible(
            artist_name,
            title_base,
            entity_kind=entity_kind,
            role_hints=role_hints,
        ):
            return False
        if any(tok in merged for tok in ("statue", "monument", "sculpture", "stamp", "painting", "engraving", "book cover", "djvu", "pdf", "wasp", "bee", "hornet", "moth", "beetle", "insect", "spider", "species", "genus")):
            return False
        if any(host == domain or host.endswith(f".{domain}") for domain in trusted_domains):
            return True
        return any(tok in merged for tok in ("composer", "conductor", "maestro", "musician", "pianist", "violinist", "portrait", "artist"))
    return True

def _fetch_artist_image_web(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    candidate_names: list[str] | tuple[str, ...] | None = None,
    allow_ai_fallback: bool = True,
) -> Optional[str]:
    """
    Fallback: try to find an artist image via web search (IA-first, Serper as backup) + OpenGraph image.
    Returns image URL or None.
    """
    kind = str(entity_kind or "").strip().lower()
    role_norms = {
        str(role or "").strip().lower()
        for role in (role_hints or [])
        if str(role or "").strip()
    }
    lookup_names = _artist_identity_lookup_names(
        artist_name,
        entity_kind=entity_kind,
        role_hints=role_hints,
        candidate_names=candidate_names,
        limit=12,
    )
    strict_identity = bool(
        kind in {"composer", "conductor", "orchestra", "ensemble", "choir", "chorus"}
        or role_norms.intersection({"composer", "conductor", "orchestra", "ensemble", "choir", "chorus"})
    )
    for lookup_name in lookup_names[:6]:
        queries = _artist_image_search_queries(
            lookup_name,
            entity_kind=entity_kind,
            role_hints=role_hints,
        )
        for query in queries[:4]:
            results = _serper_web_search(query, num=6, allow_ai_fallback=allow_ai_fallback)
            if not results:
                continue
            for item in results:
                if not _artist_image_result_looks_relevant(
                    lookup_name,
                    item,
                    entity_kind=entity_kind,
                    role_hints=role_hints,
                    candidate_names=lookup_names,
                ):
                    continue
                title = str(item.get("title") or "").strip()
                snippet = str(item.get("snippet") or "").strip()
                link = item.get("link") or ""
                if not link:
                    continue
                resolved_authoritative = _resolve_authoritative_artist_image_url(
                    str(link),
                    artist_name=lookup_name,
                    entity_kind=entity_kind,
                    role_hints=role_hints,
                    page_title=title,
                    page_summary=snippet,
                    timeout=8,
                )
                if resolved_authoritative:
                    return resolved_authoritative
                if strict_identity:
                    continue
                try:
                    resp = requests.get(link, timeout=8, allow_redirects=True)
                except Exception as e:
                    logging.debug("[ArtistWebImage] Failed to fetch %s: %s", link, e)
                    continue
                if resp.status_code != 200:
                    continue
                ct = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
                # Direct image URL
                if ct.startswith("image/"):
                    if _artist_image_url_looks_relevant(
                        link,
                        artist_name=lookup_name,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                        page_title=title,
                        page_summary=snippet,
                    ):
                        return link
                    continue
                # HTML page – try og:image
                if "text/html" in ct and resp.text:
                    try:
                        m = re.search(
                            r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
                            resp.text,
                            re.IGNORECASE,
                        )
                        if not m:
                            m = re.search(
                                r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:image["\']',
                                resp.text,
                                re.IGNORECASE,
                            )
                        if not m:
                            continue
                        img_url = m.group(1).strip()
                        if img_url:
                            resolved = _resolve_authoritative_artist_image_url(
                                img_url,
                                artist_name=lookup_name,
                                entity_kind=entity_kind,
                                role_hints=role_hints,
                                page_title=title,
                                page_summary=snippet,
                                timeout=8,
                            )
                            if resolved:
                                return resolved
                    except Exception as e:
                        logging.debug("[ArtistWebImage] Failed to extract og:image from %s: %s", link, e)
                        continue
    return None

def _fetch_and_save_artist_image(
    artist_name: str,
    artist_folder: Path,
    artist_mbid: Optional[str] = None,
    identity_fields: Optional[dict] = None,
) -> Optional[str]:
    """
    Fetch artist image from MB, then Last.fm, then Discogs and save to artist_folder (artist.jpg).
    Skips if artist_folder already has a dedicated artist image.
    Returns provider string on success ("musicbrainz", "lastfm", "discogs"), or None on failure.
    """
    if not artist_folder or not artist_folder.is_dir():
        return None
    if _artist_folder_has_image(artist_folder):
        return None
    fanart_key = (getattr(sys.modules[__name__], "FANART_API_KEY", "") or "").strip()
    identity_fields = dict(identity_fields or {})
    entity_kind = str(identity_fields.get("entity_kind") or "").strip().lower()
    role_hints = _artist_role_hints_from_roles_json(identity_fields.get("roles_json") or identity_fields.get("role_hints") or [])
    candidate_names = [
        value
        for value in (identity_fields.get("artist_aliases") or [])
        if str(value or "").strip()
    ]
    classical_like = _artist_entity_is_classical_like(entity_kind=entity_kind, role_hints=role_hints)
    ensemble_like = bool(
        entity_kind in {"ensemble", "orchestra", "choir", "chorus"}
        or bool(set(role_hints).intersection({"orchestra", "ensemble", "choir", "chorus"}))
    )
    mb_identity: dict[str, Any] = {}
    if classical_like:
        try:
            mb_identity = _musicbrainz_artist_identity_lookup(
                artist_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
            ) or {}
        except Exception:
            mb_identity = {}
        if mb_identity:
            for key in ("name", "sort_name"):
                alias_txt = " ".join(str(mb_identity.get(key) or "").split()).strip()
                if alias_txt:
                    candidate_names.append(alias_txt)
            for alias in (mb_identity.get("aliases") or []):
                alias_txt = " ".join(str(alias or "").split()).strip()
                if alias_txt:
                    candidate_names.append(alias_txt)

    def _download_and_save(url: str, provider: str, *, page_title: str = "", page_summary: str = "") -> Optional[str]:
        try:
            if not _artist_image_url_looks_relevant(
                url,
                artist_name=artist_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                page_title=page_title,
                page_summary=page_summary,
            ):
                return None
            resp = requests.get(url, timeout=10, allow_redirects=True)
            if resp.status_code != 200 or not resp.content:
                return None
            ct = resp.headers.get("content-type", "").lower()
            if "image/" not in ct:
                return None
            if not _is_usable_artist_image_bytes(resp.content):
                return None
            if not _is_artist_image_distinct_from_local_covers(artist_folder, resp.content):
                logging.debug("Skipped %s artist image for '%s': too similar to local cover", provider, artist_name)
                return None
            ext = ".jpg"
            if "png" in ct:
                ext = ".png"
            elif "webp" in ct:
                ext = ".webp"
            out = artist_folder / f"artist{ext}"
            out.write_bytes(resp.content)
            logging.info("Saved artist image from %s to %s", provider, out)
            return provider
        except Exception as e:
            logging.warning("Artist image download (%s) failed: %s", provider, e)
            return None

    if classical_like:
        mb_title = str(mb_identity.get("name") or artist_name).strip()
        mb_summary = "\n".join(str(alias or "").strip() for alias in (mb_identity.get("aliases") or []) if str(alias or "").strip())
        if not ensemble_like:
            for mb_url in [str(url or "").strip() for url in (mb_identity.get("urls") or []) if str(url or "").strip()][:10]:
                resolved_mb_url = _resolve_authoritative_artist_image_url(
                    mb_url,
                    artist_name=artist_name,
                    entity_kind=entity_kind,
                    role_hints=role_hints,
                    page_title=mb_title,
                    page_summary=mb_summary,
                )
                if resolved_mb_url:
                    saved = _download_and_save(resolved_mb_url, "musicbrainz_url", page_title=mb_title, page_summary=mb_summary)
                    if saved:
                        return saved
        if ensemble_like:
            web_url = _fetch_artist_image_web(
                artist_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                candidate_names=candidate_names,
                allow_ai_fallback=False,
            )
            if web_url and not _is_probably_placeholder_artist_image_url(web_url):
                saved = _download_and_save(web_url, "web_authoritative", page_title=str(mb_identity.get("name") or artist_name), page_summary=mb_summary)
                if saved:
                    return saved
        commons_url = _fetch_wikimedia_commons_artist_image(
            artist_name,
            entity_kind=entity_kind,
            role_hints=role_hints,
            candidate_names=candidate_names,
        )
        if commons_url:
            saved = _download_and_save(commons_url, "commons", page_title=str(mb_identity.get("name") or artist_name), page_summary=mb_summary)
            if saved:
                return saved
    if not USE_MUSICBRAINZ and not USE_LASTFM and not USE_DISCOGS and not fanart_key:
        return None
    mb_artist_for_fanart = _resolve_artist_mbid_for_fanart(
        artist_name=artist_name,
        artist_mbid=artist_mbid,
        musicbrainz_id=identity_fields.get("musicbrainz_release_group_id") or identity_fields.get("musicbrainz_id"),
        discogs_release_id=identity_fields.get("discogs_release_id"),
        lastfm_album_mbid=identity_fields.get("lastfm_album_mbid"),
        bandcamp_album_url=identity_fields.get("bandcamp_album_url"),
    )

    # Try MusicBrainz first (if we have an artist MBID)
    if mb_artist_for_fanart and USE_MUSICBRAINZ:
        if _fetch_and_save_artist_image_mb(mb_artist_for_fanart, artist_folder):
            return "musicbrainz"
    # Fanart.tv (MBID-based) – optional but often high-quality.
    if mb_artist_for_fanart:
        url = _fetch_artist_image_fanart(mb_artist_for_fanart)
        if url:
            saved = _download_and_save(url, "fanart")
            if saved:
                return saved
    # Last.fm
    if USE_LASTFM:
        url = _fetch_artist_image_lastfm(artist_name)
        if url and not _is_probably_placeholder_artist_image_url(url):
            saved = _download_and_save(url, "lastfm")
            if saved:
                return saved
    # TheAudioDB (name-based) – optional.
    url = _fetch_artist_image_audiodb(artist_name)
    if url:
        saved = _download_and_save(url, "audiodb")
        if saved:
            return saved
    # Discogs
    if USE_DISCOGS:
        url = _fetch_artist_image_discogs(artist_name)
        if url:
            saved = _download_and_save(url, "discogs")
            if saved:
                return saved
    # Web search fallback (Serper + OpenGraph) when other providers did not yield an image
    web_url = _fetch_artist_image_web(artist_name)
    if web_url and not _is_probably_placeholder_artist_image_url(web_url):
        saved = _download_and_save(web_url, "web")
        if saved:
            return saved
    return None

def api_library_artist_images(artist_id):
    """Get artist images from MusicBrainz/Wikimedia."""
    if _get_library_mode() == "files":
        if not USE_MUSICBRAINZ:
            return jsonify({"error": "MusicBrainz not enabled"}), 400
        ok, err = _ensure_files_index_ready()
        if not ok:
            return jsonify({"error": err or "Files index unavailable"}), 503
        conn = _files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM files_artists WHERE id = %s", (artist_id,))
                row = cur.fetchone()
            if not row:
                return jsonify({"error": "Artist not found"}), 404
            artist_name = row[0] or ""
        finally:
            conn.close()
        mbid = None
        try:
            search_result = musicbrainzngs.search_artists(artist=artist_name, limit=1)
            if search_result.get("artist-list"):
                mbid = search_result["artist-list"][0]["id"]
        except Exception as e:
            logging.warning("Failed to search MusicBrainz for artist '%s': %s", artist_name, e)
            return jsonify({"error": "Could not find MusicBrainz ID for artist"}), 404
        if not mbid:
            return jsonify({"error": "Could not find MusicBrainz ID for artist"}), 404
        image_urls = get_artist_images_mb(mbid)
        return jsonify({"artist_mbid": mbid, "images": image_urls})

    return jsonify({"error": "Artist image lookup is only available from the files library"}), 400


def get_similar_artists_mb(artist_mbid: str) -> list[dict[str, Any]]:
    """Get similar artists from MusicBrainz using relations and tags."""
    if not USE_MUSICBRAINZ:
        return []

    similar = []

    try:
        result = musicbrainzngs.get_artist_by_id(
            artist_mbid,
            includes=["artist-rels", "tags"],
        )
        artist_data = result.get("artist", {})
        relations = artist_data.get("artist-relation-list", [])

        for rel in relations:
            rel_type = rel.get("type", "")
            if rel_type not in {"similar to", "influenced by", "collaboration", "member of", "founded"}:
                continue
            target_artist = rel.get("artist", {})
            if target_artist:
                similar.append({
                    "name": target_artist.get("name", ""),
                    "mbid": target_artist.get("id", ""),
                    "type": rel_type,
                })

        tags = artist_data.get("tag-list", [])
        if tags:
            top_tags = sorted(tags, key=lambda t: int(t.get("count", 0)), reverse=True)[:3]
            for tag_info in top_tags:
                tag_name = tag_info.get("name", "")
                if not tag_name:
                    continue
                try:
                    search_result = musicbrainzngs.search_artists(tag=tag_name, limit=10)
                    artist_list = search_result.get("artist-list", [])
                    for artist in artist_list:
                        if artist.get("id") == artist_mbid:
                            continue
                        if not any(s.get("mbid") == artist.get("id") for s in similar):
                            similar.append({
                                "name": artist.get("name", ""),
                                "mbid": artist.get("id", ""),
                                "type": f"tag: {tag_name}",
                            })
                        if len(similar) >= 20:
                            break
                    if len(similar) >= 20:
                        break
                except Exception:
                    continue

        seen = set()
        unique_similar = []
        for item in similar:
            mbid = item.get("mbid")
            if mbid in seen:
                continue
            seen.add(mbid)
            unique_similar.append(item)
            if len(unique_similar) >= 15:
                break

        return unique_similar
    except Exception as exc:
        logging.error("Failed to get similar artists for MBID %s: %s", artist_mbid, exc)
        return []

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}


def _fetch_discogs_artist_profile_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_discogs_artist_profile_info(*args, **kwargs)

def _fetch_bandcamp_artist_profile_hint_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_bandcamp_artist_profile_hint(*args, **kwargs)

def _fetch_musicbrainz_artist_profile_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_musicbrainz_artist_profile_info(*args, **kwargs)

def _files_try_artist_image_refresh_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_try_artist_image_refresh(*args, **kwargs)

def _is_relevant_artist_profile_text_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _is_relevant_artist_profile_text(*args, **kwargs)

def _artist_profile_text_looks_music_related_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_profile_text_looks_music_related(*args, **kwargs)

def _artist_profile_text_looks_biographical_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_profile_text_looks_biographical(*args, **kwargs)

def _fetch_lastfm_artist_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_lastfm_artist_info(*args, **kwargs)

def _fetch_wikimedia_commons_artist_image_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_wikimedia_commons_artist_image(*args, **kwargs)

def _resolve_authoritative_artist_image_url_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _resolve_authoritative_artist_image_url(*args, **kwargs)

def _artist_profile_search_queries_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_profile_search_queries(*args, **kwargs)

def _fetch_wikipedia_artist_bio_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_wikipedia_artist_bio(*args, **kwargs)

def _artist_profile_payload_requires_refresh_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_profile_payload_requires_refresh(*args, **kwargs)

def _build_single_artist_profile_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_single_artist_profile_payload(*args, **kwargs)

def _build_artist_profile_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_artist_profile_payload(*args, **kwargs)

def _merge_artist_profile_tags_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _merge_artist_profile_tags(*args, **kwargs)

def api_library_artist_profile_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_artist_profile(*args, **kwargs)

def api_library_artist_ai_enrich_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_artist_ai_enrich(*args, **kwargs)

def api_library_artist_facts_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_artist_facts(*args, **kwargs)

def api_library_artist_facts_extract_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_artist_facts_extract(*args, **kwargs)

def api_library_files_artist_image_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_files_artist_image(*args, **kwargs)

def api_library_external_artist_image_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_external_artist_image(*args, **kwargs)

def get_artist_images_mb_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return get_artist_images_mb(*args, **kwargs)

def get_similar_artists_mb_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return get_similar_artists_mb(*args, **kwargs)

def _is_probably_placeholder_artist_image_url_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _is_probably_placeholder_artist_image_url(*args, **kwargs)

def _is_suspicious_external_artist_image_url_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _is_suspicious_external_artist_image_url(*args, **kwargs)

def _artist_image_provider_allowed_for_entity_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_image_provider_allowed_for_entity(*args, **kwargs)

def _is_usable_artist_image_bytes_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _is_usable_artist_image_bytes(*args, **kwargs)

def _is_usable_artist_image_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _is_usable_artist_image_path(*args, **kwargs)

def _is_artist_image_distinct_from_local_covers_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _is_artist_image_distinct_from_local_covers(*args, **kwargs)

def _fetch_and_save_artist_image_mb_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_and_save_artist_image_mb(*args, **kwargs)

def _fetch_artist_image_lastfm_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_artist_image_lastfm(*args, **kwargs)

def _fetch_artist_image_discogs_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_artist_image_discogs(*args, **kwargs)

def _fetch_artist_image_fanart_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_artist_image_fanart(*args, **kwargs)

def _extract_artist_mbid_from_mb_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _extract_artist_mbid_from_mb_payload(*args, **kwargs)

def _resolve_artist_mbid_for_fanart_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _resolve_artist_mbid_for_fanart(*args, **kwargs)

def _fetch_artist_image_audiodb_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_artist_image_audiodb(*args, **kwargs)

def _artist_image_search_queries_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_image_search_queries(*args, **kwargs)

def _artist_image_lookup_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_image_lookup_candidates(*args, **kwargs)

def _artist_image_alias_candidate_is_compatible_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_image_alias_candidate_is_compatible(*args, **kwargs)

def _artist_image_exact_name_match_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_image_exact_name_match(*args, **kwargs)

def _fetch_wikipedia_artist_bio_best_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_wikipedia_artist_bio_best(*args, **kwargs)

def _artist_image_url_looks_relevant_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_image_url_looks_relevant(*args, **kwargs)

def _artist_image_result_looks_relevant_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_image_result_looks_relevant(*args, **kwargs)

def _fetch_artist_image_web_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_artist_image_web(*args, **kwargs)

def _fetch_and_save_artist_image_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_and_save_artist_image(*args, **kwargs)

def api_library_artist_images_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_artist_images(*args, **kwargs)
