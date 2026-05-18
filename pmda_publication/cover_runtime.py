"""Runtime-bound publication cover and authoritative tag helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_authoritative_primary_tags_for_publication',
    '_publication_cover_needs_provider_refresh',
    '_cover_provider_from_primary_tags_blob',
    '_publication_cover_identity_ok',
    '_authoritative_publication_cover',
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

def _authoritative_primary_tags_for_publication(
    *,
    tags: dict,
    artist_resolved: str,
    album_resolved: str,
    year: int | None,
    genre: str,
    label: str,
    metadata_source: str,
    musicbrainz_release_group_id: str,
    musicbrainz_release_id: str,
    discogs_release_id: str,
    lastfm_album_mbid: str,
    bandcamp_album_url: str,
    strict_match_verified: bool = False,
    cover_provider: str = "",
) -> dict:
    out = dict(tags or {})
    artist_txt = str(artist_resolved or "").strip() or "Unknown Artist"
    album_txt = _sanitize_album_title_display(str(album_resolved or "").strip() or "Unknown Album")
    if artist_txt:
        out["artist"] = artist_txt
        out["album_artist"] = artist_txt
        out["albumartist"] = artist_txt
        out["artists"] = artist_txt
        out["artist credit"] = artist_txt
    if album_txt:
        out["album"] = album_txt
        out["toal"] = album_txt
        out["original album"] = album_txt
    if year:
        out.setdefault("date", str(year))
        out["album_year"] = str(year)
    if genre:
        out["genre"] = genre
    if label:
        out["label"] = label
        out.setdefault("publisher", label)
    provider_txt = _normalize_identity_provider(str(metadata_source or ""))
    if provider_txt:
        out["primary_metadata_source"] = provider_txt
        if bool(strict_match_verified):
            out[PMDA_MATCH_PROVIDER_TAG] = provider_txt
            out["pmda_matched"] = "true"
        else:
            out.pop(PMDA_MATCH_PROVIDER_TAG, None)
            out.pop("pmda_matched", None)
    if musicbrainz_release_group_id:
        out["musicbrainz_releasegroupid"] = musicbrainz_release_group_id
        out["musicbrainz_release_group_id"] = musicbrainz_release_group_id
    if musicbrainz_release_id:
        out["musicbrainz_releaseid"] = musicbrainz_release_id
        out["musicbrainz_release_id"] = musicbrainz_release_id
        out["musicbrainz_albumid"] = musicbrainz_release_id
        out["musicbrainz_album_id"] = musicbrainz_release_id
    if discogs_release_id:
        out["discogs_release_id"] = discogs_release_id
    if lastfm_album_mbid:
        out["lastfm_album_mbid"] = lastfm_album_mbid
    if bandcamp_album_url:
        out["bandcamp_album_url"] = bandcamp_album_url
    cover_provider_txt = _normalize_identity_provider(str(cover_provider or ""))
    if cover_provider_txt and cover_provider_txt not in {"unknown", "local"}:
        out[PMDA_COVER_PROVIDER_TAG] = cover_provider_txt
        out["pmda_cover"] = "true"
    return out

def _publication_cover_needs_provider_refresh(tags: dict, *, artist_resolved: str, album_resolved: str) -> bool:
    tag_artist = str(_pick_album_artist_from_tag_dicts([tags], default="") or "").strip()
    tag_album = _sanitize_album_title_display(str(_pick_album_title_from_tag_dicts([tags], fallback="") or "").strip())
    artist_norm = _normalize_identity_text_strict(artist_resolved)
    album_norm = _normalize_identity_text_strict(album_resolved)
    tag_artist_norm = _normalize_identity_text_strict(tag_artist)
    tag_album_norm = _normalize_identity_text_strict(tag_album)
    if artist_norm and tag_artist_norm and artist_norm != tag_artist_norm:
        return True
    if album_norm and tag_album_norm and album_norm != tag_album_norm:
        return True
    return False

def _cover_provider_from_primary_tags_blob(primary_tags_json: Any) -> str:
    tags_blob = primary_tags_json
    if isinstance(tags_blob, str):
        tags_blob = _safe_json_load(tags_blob, fallback={})
    if not isinstance(tags_blob, dict):
        return ""
    return _normalize_identity_provider(
        str(
            tags_blob.get(PMDA_COVER_PROVIDER_TAG)
            or tags_blob.get("pmda_cover_provider")
            or ""
        )
    )

def _publication_cover_identity_ok(
    provider_name: str,
    payload: dict[str, Any] | None,
    *,
    artist_name: str,
    album_title: str,
) -> bool:
    provider_norm = _normalize_identity_provider(provider_name)
    if provider_norm == "musicbrainz":
        return True
    payload_dict = payload if isinstance(payload, dict) else {}
    candidate_artist = _provider_payload_artist(provider_norm, payload_dict)
    candidate_title = _provider_payload_title(provider_norm, payload_dict)
    if not candidate_artist or not candidate_title:
        return False
    ok, _reason = _strict_identity_match_details(
        local_artist=artist_name,
        local_title=album_title,
        candidate_artist=candidate_artist,
        candidate_title=candidate_title,
    )
    return bool(ok)

def _authoritative_publication_cover(
    *,
    folder: Path,
    item: dict,
    result: dict,
    tags: dict,
    artist_resolved: str,
    album_resolved: str,
    strict_match_verified: bool,
    strict_match_provider: str,
    metadata_source: str,
    musicbrainz_release_group_id: str,
    musicbrainz_release_id: str,
    discogs_release_id: str,
    lastfm_album_mbid: str,
    bandcamp_album_url: str,
    current_cover_path: str = "",
    current_cover_provider: str = "",
) -> tuple[str, bool, str]:
    if bool(item.get("_publication_reconcile_fast_publish")):
        current_cover_raw = str(current_cover_path or item.get("cover_path") or "").strip()
        current_cover_provider_norm = _normalize_identity_provider(str(current_cover_provider or ""))
        return (
            current_cover_raw,
            bool(current_cover_raw),
            current_cover_provider_norm if current_cover_raw else "",
        )
    local_cover = _first_cover_path(folder)
    local_cover_ok = bool(local_cover and local_cover.is_file())
    if not bool(strict_match_verified):
        return (str(local_cover) if local_cover_ok else "", bool(local_cover_ok), "local" if local_cover_ok else "")
    provider_seed = _normalize_identity_provider(
        str(
            strict_match_provider
            or metadata_source
            or result.get("provider_used")
            or result.get("pmda_match_provider")
            or item.get("primary_metadata_source")
            or item.get("metadata_source")
            or ""
        )
    )
    preferred_cover_order = ["bandcamp", "itunes", "deezer", "spotify", "qobuz", "audiodb", "lastfm", "musicbrainz", "discogs", "tidal"]
    provider_chain: list[str] = []
    if provider_seed:
        provider_chain.append(provider_seed)
    provider_chain.extend(preferred_cover_order)
    seen: set[str] = set()
    provider_chain = [p for p in provider_chain if p and not (p in seen or seen.add(p))]
    edition_payload = dict(item or {})
    if musicbrainz_release_group_id:
        edition_payload["musicbrainz_id"] = musicbrainz_release_group_id
        edition_payload["musicbrainz_release_group_id"] = musicbrainz_release_group_id
    if musicbrainz_release_id:
        edition_payload["musicbrainz_release_id"] = musicbrainz_release_id
        edition_payload["musicbrainz_albumid"] = musicbrainz_release_id
    if discogs_release_id:
        edition_payload["discogs_release_id"] = discogs_release_id
    if lastfm_album_mbid:
        edition_payload["lastfm_album_mbid"] = lastfm_album_mbid
    if bandcamp_album_url:
        edition_payload["bandcamp_album_url"] = bandcamp_album_url
    if metadata_source:
        edition_payload["primary_metadata_source"] = metadata_source
    trusted_identity = _has_trusted_album_identity(
        musicbrainz_id=musicbrainz_release_group_id or musicbrainz_release_id,
        discogs_release_id=discogs_release_id,
        lastfm_album_mbid=lastfm_album_mbid,
        bandcamp_album_url=bandcamp_album_url,
    ) or bool(
        str(edition_payload.get("itunes_collection_id") or "").strip()
        or str(edition_payload.get("deezer_album_id") or "").strip()
        or str(edition_payload.get("spotify_album_id") or "").strip()
        or str(edition_payload.get("qobuz_album_id") or "").strip()
        or str(edition_payload.get("tidal_album_id") or "").strip()
        or str(edition_payload.get("audiodb_album_id") or "").strip()
        or provider_seed in {"itunes", "deezer", "spotify", "qobuz", "tidal", "audiodb"}
    )
    if not trusted_identity:
        return (str(local_cover) if local_cover_ok else "", bool(local_cover_ok), "local" if local_cover_ok else "")

    exact_provider_ids: dict[str, str] = {}
    for provider in ("musicbrainz", "discogs", "itunes", "deezer", "spotify", "qobuz", "tidal", "audiodb", "bandcamp", "lastfm"):
        try:
            expected_id = _strict_expected_provider_id(provider, edition_payload)
        except Exception:
            expected_id = ""
        if expected_id:
            exact_provider_ids[provider] = expected_id

    exact_cover_provider_lock = ""
    metadata_provider_norm = _normalize_identity_provider(str(metadata_source or ""))
    if metadata_provider_norm in exact_provider_ids:
        exact_cover_provider_lock = metadata_provider_norm
    elif metadata_provider_norm == "musicbrainz" and musicbrainz_release_id:
        exact_cover_provider_lock = "musicbrainz"
    elif provider_seed in exact_provider_ids:
        exact_cover_provider_lock = provider_seed

    # If we do not have an exact provider identity, keep a usable local cover.
    # Once an exact identity exists, prefer authoritative external art over any
    # embedded/local image because legacy files can carry stale or incorrect art.
    if (not exact_provider_ids) and local_cover_ok and not _publication_cover_needs_provider_refresh(
        tags,
        artist_resolved=artist_resolved,
        album_resolved=album_resolved,
    ):
        return (str(local_cover), True, "local")

    if exact_cover_provider_lock:
        provider_chain = [exact_cover_provider_lock] + [provider for provider in provider_chain if provider != exact_cover_provider_lock]
    elif exact_provider_ids:
        if musicbrainz_release_id or discogs_release_id:
            exact_first = [provider for provider in ("musicbrainz", "discogs") if provider in provider_chain]
            provider_chain = exact_first + [provider for provider in provider_chain if provider not in exact_first]
        else:
            exact_first = [provider for provider in preferred_cover_order if provider in exact_provider_ids]
            provider_chain = exact_first + [provider for provider in provider_chain if provider not in exact_first]

    current_cover_provider_norm = _normalize_identity_provider(str(current_cover_provider or ""))
    current_cover_cached: Optional[Path] = None
    current_cover_raw = str(current_cover_path or "").strip()
    if current_cover_raw:
        try:
            cand = path_for_fs_access(Path(current_cover_raw))
            if cand.exists() and cand.is_file():
                current_cover_cached = cand
        except Exception:
            current_cover_cached = None
    if current_cover_cached and current_cover_provider_norm and current_cover_provider_norm not in {"local", "unknown"} and not exact_provider_ids:
        if current_cover_provider_norm in {
            provider_seed,
            _normalize_identity_provider(str(metadata_source or "")),
        }:
            return (str(current_cover_cached), True, current_cover_provider_norm)

    for provider in provider_chain:
        try:
            payload = _strict_payload_for_provider(
                provider,
                artist_name=artist_resolved,
                album_title=album_resolved,
                edition=edition_payload,
            )
            payload_dict = payload if isinstance(payload, dict) else {}
            if provider != "musicbrainz" and not _publication_cover_identity_ok(
                provider,
                payload_dict,
                artist_name=artist_resolved,
                album_title=album_resolved,
            ):
                continue
            cover_url = _provider_cover_url_from_payload(provider, payload_dict)
            if not cover_url:
                continue
            downloaded = _download_best_cover_image(
                _match_provider_label(provider),
                cover_url,
                cover_candidates=payload_dict.get("cover_candidates") or [],
                timeout=14,
            )
            if not downloaded:
                continue
            content, mime, _used_url = downloaded
            provider_ref = (
                _provider_id_for_strict(provider, payload_dict)
                or _strict_expected_provider_id(provider, edition_payload)
                or f"{_norm_artist_key(artist_resolved)}::{norm_album_for_dedup(album_resolved, normalize_parenthetical=True)}"
            )
            cached = _ensure_cached_image_from_bytes(
                content,
                mime,
                kind="album",
                cache_key_hint=f"publish:{provider}:{provider_ref}",
                max_px=_MEDIA_CACHE_MASTER_PX,
            )
            if cached and cached.exists() and cached.is_file():
                return (str(cached), True, provider)
        except DiscogsRateLimited:
            continue
        except Exception:
            logging.debug(
                "Publication authoritative cover fetch failed provider=%s artist=%s album=%s",
                provider,
                artist_resolved,
                album_resolved,
                exc_info=True,
            )
            continue
    return (str(local_cover) if local_cover_ok else "", bool(local_cover_ok), "local" if local_cover_ok else "")

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}

def _authoritative_primary_tags_for_publication_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _authoritative_primary_tags_for_publication(*args, **kwargs)

def _publication_cover_needs_provider_refresh_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _publication_cover_needs_provider_refresh(*args, **kwargs)

def _cover_provider_from_primary_tags_blob_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _cover_provider_from_primary_tags_blob(*args, **kwargs)

def _publication_cover_identity_ok_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _publication_cover_identity_ok(*args, **kwargs)

def _authoritative_publication_cover_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _authoritative_publication_cover(*args, **kwargs)
