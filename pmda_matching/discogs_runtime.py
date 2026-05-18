"""Runtime-owned Discogs lookup, throttling, and release-fetch helpers."""
from __future__ import annotations

import logging
import random
import time
from typing import Any, Optional

_EXTRACTED_NAMES = {
    '_fetch_discogs_release_by_id',
    '_provider_error_text',
    '_provider_is_name_resolution_failure',
    '_discogs_min_interval_sec',
    '_discogs_throttle',
    '_discogs_penalize',
    '_get_discogs_client',
    '_get_or_create_discogs_client',
    '_discogs_api_get_json',
    '_discogs_hydrate_release_or_master_data',
    '_discogs_call',
    '_run_discogs_preflight',
    '_discogs_lookup_candidate_cap',
    '_discogs_search_identity_from_data',
    '_discogs_search_candidate_score',
    '_fetch_discogs_release',
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

def _fetch_discogs_release_by_id(discogs_id: str) -> Optional[dict]:
    """Fetch Discogs release payload from a known release/master id."""
    if not USE_DISCOGS:
        return None
    sid = str(discogs_id or "").strip()
    if not sid:
        return None
    try:
        iid = int(float(sid))
    except Exception:
        return None
    d = _get_discogs_client()
    if d is None:
        return None
    try:
        rel_data = _discogs_call(
            f"release {iid} data",
            lambda rid=iid: d.release(int(rid)).data,
        )
        rel_data = _discogs_hydrate_release_or_master_data("release", rel_data, iid)
        if isinstance(rel_data, dict) and (rel_data.get("id") or iid):
            return _strict_discogs_payload_from_release_data(rel_data)
    except DiscogsRateLimited:
        raise
    except Exception:
        pass
    try:
        master_data = _discogs_call(
            f"master {iid} data",
            lambda mid=iid: d.master(int(mid)).data,
        )
        master_data = _discogs_hydrate_release_or_master_data("master", master_data, iid)
        if isinstance(master_data, dict):
            main_release = master_data.get("main_release")
            if isinstance(main_release, dict):
                main_release = main_release.get("id")
            try:
                rid = int(float(str(main_release or "").strip()))
            except Exception:
                rid = 0
            if rid > 0:
                rel_data = _discogs_call(
                    f"release {rid} data",
                    lambda relid=rid: d.release(int(relid)).data,
                )
                rel_data = _discogs_hydrate_release_or_master_data("release", rel_data, rid)
                if isinstance(rel_data, dict) and (rel_data.get("id") or rid):
                    payload = _strict_discogs_payload_from_release_data(rel_data)
                    payload["master_id"] = str(master_data.get("id") or payload.get("master_id") or "").strip()
                    return payload
    except DiscogsRateLimited:
        raise
    except Exception:
        pass
    return None

def _provider_error_text(exc: Exception | None) -> str:
    if exc is None:
        return ""
    parts = [str(exc or "").strip()]
    cur = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        cause = getattr(cur, "__cause__", None)
        if cause is None:
            cause = getattr(cur, "__context__", None)
        cur = cause if isinstance(cause, Exception) else None
        if cur is not None:
            txt = str(cur or "").strip()
            if txt:
                parts.append(txt)
    return " | ".join(part for part in parts if part)

def _provider_is_name_resolution_failure(exc: Exception | None) -> bool:
    msg = _provider_error_text(exc).lower()
    if not msg:
        return False
    return any(
        token in msg
        for token in (
            "name resolution",
            "failed to resolve",
            "temporary failure in name resolution",
            "nodename nor servname provided",
            "[errno -3]",
            "getaddrinfo failed",
        )
    )

def _discogs_min_interval_sec() -> float:
    """
    Minimum delay between Discogs API requests across the whole process.
    Not exposed in the UI; can be overridden via env/DB for debugging.
    """
    try:
        override = getattr(sys.modules[__name__], "DISCOGS_MIN_INTERVAL_SEC", None)
    except Exception:
        override = None
    try:
        if override is not None and str(override).strip() != "":
            interval = float(override)
        else:
            rpm = float(_discogs_effective_rpm())
            interval = (60.0 / rpm) if rpm > 0.0 else 1.5
    except Exception:
        interval = 1.5
    return max(0.2, min(interval, 15.0))

def _discogs_throttle() -> None:
    """Reserve a global Discogs request slot and sleep until it is due."""
    global _discogs_next_allowed_at
    interval = _discogs_min_interval_sec()
    with _discogs_lock:
        now = time.monotonic()
        scheduled = max(float(_discogs_next_allowed_at or 0.0), now)
        _discogs_next_allowed_at = scheduled + interval
    wait = scheduled - now
    if wait > 0:
        time.sleep(wait)

def _discogs_penalize(seconds: float) -> None:
    """Push the next allowed time into the future (used after 429 backoff)."""
    global _discogs_next_allowed_at
    try:
        sec = float(seconds or 0.0)
    except Exception:
        sec = 0.0
    if sec <= 0:
        return
    with _discogs_lock:
        now = time.monotonic()
        _discogs_next_allowed_at = max(float(_discogs_next_allowed_at or 0.0), now + sec)

def _get_discogs_client():
    """Return a cached Discogs client (token-scoped), or None when not configured."""
    token = (getattr(sys.modules[__name__], "DISCOGS_USER_TOKEN", "") or "").strip()
    if not token:
        return None
    global _discogs_client, _discogs_client_token
    with _discogs_lock:
        if _discogs_client is None or _discogs_client_token != token:
            import discogs_client
            _discogs_client = discogs_client.Client("PMDA/0.6.6", user_token=token)
            _discogs_client_token = token
        return _discogs_client

def _get_or_create_discogs_client():
    """Backward-compatible alias for older Discogs call sites."""
    return _get_discogs_client()

def _discogs_api_get_json(url: str, *, desc: str = "discogs api", attempts: int = 2) -> Optional[dict]:
    token = (getattr(sys.modules[__name__], "DISCOGS_USER_TOKEN", "") or "").strip()
    target = str(url or "").strip()
    if not token or not target:
        return None
    last_status = 0
    last_error: Exception | None = None
    for attempt in range(1, max(1, int(attempts or 1)) + 1):
        _discogs_throttle()
        try:
            resp = requests.get(
                target,
                headers={
                    "Authorization": f"Discogs token={token}",
                    "User-Agent": "PMDA/0.6.6",
                    "Accept": "application/json",
                },
                timeout=15,
            )
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if _provider_is_name_resolution_failure(exc):
                logging.warning("[Discogs] network resolve failure during %s: %s", desc, exc)
                return None
            if attempt < attempts:
                time.sleep(min(1.5, 0.5 * attempt))
                continue
            return None
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(min(1.5, 0.5 * attempt))
                continue
            return None
        last_status = int(resp.status_code or 0)
        if resp.status_code == 429:
            _runtime_auto_tune_note_discogs_rate_limited(desc)
            backoff = min(120.0, 3.0 * (2.0 ** min(max(0, attempt - 1), 6)))
            _discogs_penalize(backoff)
            if attempt >= attempts:
                raise DiscogsRateLimited(f"Discogs rate limited during {desc}: HTTP 429")
            continue
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except Exception:
            continue
        if isinstance(data, dict):
            return data
    if last_status == 429:
        raise DiscogsRateLimited(f"Discogs rate limited during {desc}: HTTP 429")
    if _provider_is_name_resolution_failure(last_error):
        logging.warning("[Discogs] network resolve failure during %s: %s", desc, last_error)
    return None

def _discogs_hydrate_release_or_master_data(kind: str, data: dict | None, fallback_id: int = 0) -> dict:
    payload = dict(data or {}) if isinstance(data, dict) else {}
    kind_norm = str(kind or "release").strip().lower() or "release"
    title = str(payload.get("title") or "").strip()
    artists = payload.get("artists")
    images = payload.get("images")
    tracklist = payload.get("tracklist")
    looks_sparse = (
        len(payload.keys()) <= 2
        or (not title)
        or not isinstance(artists, list)
        or not isinstance(images, list)
        or not isinstance(tracklist, list)
    )
    if not looks_sparse:
        return payload
    resource_url = str(payload.get("resource_url") or "").strip()
    fallback_url = ""
    if fallback_id > 0:
        endpoint = "masters" if kind_norm == "master" else "releases"
        fallback_url = f"https://api.discogs.com/{endpoint}/{int(fallback_id)}"
    for candidate_url in [resource_url, fallback_url]:
        full = _discogs_api_get_json(candidate_url, desc=f"{kind_norm} hydrate {fallback_id or payload.get('id') or ''}")
        if isinstance(full, dict) and full:
            merged = dict(payload)
            merged.update(full)
            return merged
    return payload

def _discogs_call(desc: str, fn, attempts: int = 2):
    """
    Execute a Discogs API call under a global throttle, with 429 backoff.
    The *fn* must be the code path that actually triggers HTTP (e.g. `.page()`, `.data`).
    """
    global _discogs_429_streak
    try:
        from discogs_client.exceptions import HTTPError
    except Exception:
        HTTPError = Exception  # type: ignore
    last_exc = None
    for attempt in range(1, max(1, int(attempts or 1)) + 1):
        _discogs_throttle()
        started = time.time()
        try:
            result = fn()
            with _discogs_lock:
                _discogs_429_streak = 0
            _provider_gateway_record_result(
                "discogs",
                status_code=200,
                latency_ms=int(max(0.0, (time.time() - started) * 1000.0)),
                context=desc,
            )
            return result
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            _provider_gateway_record_result(
                "discogs",
                latency_ms=int(max(0.0, (time.time() - started) * 1000.0)),
                error=exc,
                context=desc,
            )
            fail_fast = _provider_is_name_resolution_failure(exc)
            if fail_fast or attempt >= attempts:
                raise ProviderTransientError(
                    f"Discogs network failure during {desc}: {exc}"
                ) from exc
            time.sleep(min(1.5, 0.5 * attempt))
            continue
        except HTTPError as he:  # type: ignore[misc]
            last_exc = he
            if getattr(he, "status_code", None) == 429:
                _runtime_auto_tune_note_discogs_rate_limited(desc)
                _provider_gateway_record_result(
                    "discogs",
                    status_code=429,
                    latency_ms=int(max(0.0, (time.time() - started) * 1000.0)),
                    error=he,
                    context=desc,
                )
                with _discogs_lock:
                    _discogs_429_streak = int(_discogs_429_streak or 0) + 1
                    streak = int(_discogs_429_streak or 0)
                # Exponential backoff with jitter, capped.
                base = 3.0
                backoff = min(120.0, base * (2.0 ** min(max(0, streak - 1), 6)))
                backoff *= 0.8 + (random.random() * 0.4)
                _discogs_penalize(backoff)
                logging.warning(
                    "[Discogs] 429 rate limited during %s; backing off %.1fs (attempt %d/%d)",
                    (desc or "call"),
                    backoff,
                    attempt,
                    attempts,
                )
                if attempt >= attempts:
                    raise DiscogsRateLimited(f"Discogs rate limited during {desc}: {he}") from he
                continue
            _provider_gateway_record_result(
                "discogs",
                status_code=int(getattr(he, "status_code", 0) or 0),
                latency_ms=int(max(0.0, (time.time() - started) * 1000.0)),
                error=he,
                context=desc,
            )
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"Discogs call failed: {desc}")

def _run_discogs_preflight() -> tuple[bool, str]:
    """Test Discogs API connectivity. Returns (ok, message)."""
    if not USE_DISCOGS or not (getattr(sys.modules[__name__], "DISCOGS_USER_TOKEN", "") or "").strip():
        return False, "Disabled (no token)"
    try:
        d = _get_discogs_client()
        if d is None:
            return False, "Disabled (no token)"
        _discogs_call("preflight identity", lambda: d.identity(), attempts=1)
        return True, "Discogs reachable"
    except DiscogsRateLimited as e:
        return False, f"Discogs rate limited: {e}"
    except Exception as e:
        return False, f"Discogs unreachable: {e}"

def _discogs_lookup_candidate_cap(best_score: float) -> int:
    score = float(best_score or 0.0)
    if score >= 0.98:
        return 3
    if score >= 0.93:
        return 4
    if score >= 0.86:
        return 5
    return 6

def _discogs_search_identity_from_data(
    data: dict[str, Any] | None,
    *,
    fallback_artist: str = "",
) -> tuple[str, str]:
    payload = data if isinstance(data, dict) else {}
    raw_title = str(payload.get("title") or "").strip()
    candidate_artist = str(fallback_artist or "").strip()
    candidate_title = raw_title
    artists = payload.get("artists")
    if isinstance(artists, list):
        artist_names: list[str] = []
        for item in artists:
            if isinstance(item, dict):
                name = str(item.get("name") or "").strip()
            else:
                name = str(item or "").strip()
            if name:
                artist_names.append(name)
        if artist_names:
            candidate_artist = ", ".join(artist_names)
    if " - " in raw_title:
        left, right = raw_title.split(" - ", 1)
        left = left.strip()
        right = right.strip()
        if left and right:
            if candidate_artist:
                artist_score = _provider_identity_artist_score(candidate_artist, left)
                if artist_score >= 0.72:
                    candidate_artist = left
                    candidate_title = right
            else:
                candidate_artist = left
                candidate_title = right
    return (str(candidate_artist or "").strip(), str(candidate_title or raw_title or "").strip())

def _discogs_search_candidate_score(
    local_artist: str,
    local_album: str,
    *,
    data: dict[str, Any] | None,
    item_type: str,
    index: int = 0,
) -> float:
    candidate_artist, candidate_title = _discogs_search_identity_from_data(
        data,
        fallback_artist=local_artist,
    )
    title_score = _provider_identity_album_score(
        local_album,
        candidate_title,
        artist_hints=[local_artist, candidate_artist],
    )
    artist_score = _provider_identity_artist_score(local_artist, candidate_artist or local_artist)
    score = (title_score * 0.65) + (artist_score * 0.35)
    if _normalize_identity_album_strict(local_album) == _normalize_identity_album_strict(candidate_title):
        score += 0.12
    if candidate_artist and _normalize_identity_text_strict(local_artist) == _normalize_identity_text_strict(candidate_artist):
        score += 0.08
    if str(item_type or "").strip().lower() == "release":
        score += 0.04
    score -= min(0.12, max(0, int(index or 0)) * 0.01)
    return round(score, 4)

def _fetch_discogs_release(artist_name: str, album_title: str) -> Optional[dict]:
    """
    Search Discogs for a release by artist + album. Returns dict with title, year, cover_url, artist_name, or None.
    Requires USE_DISCOGS and DISCOGS_USER_TOKEN.
    Search can return Masters first; d.release(master_id) would 404. We collect candidate release IDs (from
    type=release or from master.main_release). main_release is a Release object in discogs_client, so we must
    use its .id for d.release(). We try each candidate until one succeeds.
    """
    if not USE_DISCOGS:
        return None
    d = _get_discogs_client()
    if d is None:
        return None

    def _int_id(value) -> Optional[int]:
        if value is None:
            return None
        try:
            if isinstance(value, int):
                return int(value)
            return int(getattr(value, "id", value))
        except Exception:
            return None

    candidate_meta: dict[int, dict[str, Any]] = {}

    def _remember_candidate_meta(candidate_id: int, data: dict[str, Any] | None) -> None:
        if candidate_id <= 0 or not isinstance(data, dict):
            return
        meta = candidate_meta.setdefault(candidate_id, {})
        cover_image = str(data.get("cover_image") or "").strip()
        thumb = str(data.get("thumb") or "").strip()
        master_id = str(data.get("master_id") or "").strip()
        resource_url = str(data.get("resource_url") or "").strip()
        title = str(data.get("title") or "").strip()
        if cover_image and not meta.get("cover_image"):
            meta["cover_image"] = cover_image
        if thumb and not meta.get("thumb"):
            meta["thumb"] = thumb
        if master_id and not meta.get("master_id"):
            meta["master_id"] = master_id
        if resource_url and not meta.get("resource_url"):
            meta["resource_url"] = resource_url
        if title and not meta.get("title"):
            meta["title"] = title

    def _page_to_candidate_ids(page_list: list) -> list[tuple[float, int]]:
        out: list[tuple[float, int]] = []
        for idx, item in enumerate(page_list or []):
            data = getattr(item, "data", None)
            if isinstance(data, dict):
                item_id = data.get("id", None) if "id" in data else getattr(item, "id", None)
                item_type = (data.get("type") or getattr(item, "type", None) or "release")
            else:
                item_id = getattr(item, "id", None)
                item_type = getattr(item, "type", None) or "release"
            search_score = _discogs_search_candidate_score(
                artist_name,
                album_title,
                data=data if isinstance(data, dict) else None,
                item_type=str(item_type or "release"),
                index=idx,
            )
            iid = _int_id(item_id)
            if iid is None:
                continue
            if item_type == "release":
                _remember_candidate_meta(iid, data if isinstance(data, dict) else None)
                out.append((search_score, iid))
                continue
            if item_type != "master":
                continue
            # Try to resolve master -> main release without extra API calls when possible.
            main_release = None
            if isinstance(data, dict):
                main_release = data.get("main_release") or data.get("main_release_id")
                if isinstance(main_release, dict):
                    main_release = main_release.get("id")
            rid = _int_id(main_release)
            if rid is not None:
                _remember_candidate_meta(rid, data if isinstance(data, dict) else None)
                out.append((max(0.0, search_score - 0.02), rid))
                continue
            if search_score < 0.72:
                continue
            try:
                master_data = _discogs_call(f"master {iid} data", lambda mid=iid: d.master(mid).data)
                master_data = _discogs_hydrate_release_or_master_data("master", master_data, iid)
                main_release = master_data.get("main_release") if isinstance(master_data, dict) else None
                if isinstance(main_release, dict):
                    main_release = main_release.get("id")
                rid = _int_id(main_release)
                if rid is not None:
                    _remember_candidate_meta(rid, data if isinstance(data, dict) else None)
                    out.append((max(0.0, search_score - 0.03), rid))
            except DiscogsRateLimited:
                raise
            except Exception:
                continue
        return out

    album_norm = norm_album(album_title) or album_title
    # Discogs client may trigger HTTP on search/page/data. Keep every HTTP trigger behind _discogs_call.
    results = d.search(album_norm, artist=artist_name, type="release")
    page = _discogs_call("search release page=1", lambda: results.page(1))
    candidate_pairs = _page_to_candidate_ids(page)

    if not candidate_pairs:
        combined = f"{artist_name} {album_title}".strip()
        if combined:
            results = d.search(combined, type="release")
            page = _discogs_call("search combined page=1", lambda: results.page(1))
            candidate_pairs = _page_to_candidate_ids(page)

    ranked_candidates: dict[int, float] = {}
    for score, release_id in candidate_pairs:
        ranked_candidates[release_id] = max(float(score or 0.0), float(ranked_candidates.get(release_id, float("-inf"))))
    ranked_ids = [rid for rid, _score in sorted(ranked_candidates.items(), key=lambda item: item[1], reverse=True)]
    max_candidates = _discogs_lookup_candidate_cap(max(ranked_candidates.values()) if ranked_candidates else 0.0)
    for release_id in ranked_ids[:max_candidates]:
        try:
            rel_data = _discogs_call(
                f"release {release_id} data",
                lambda rid=release_id: d.release(int(rid)).data,
            )
            rel_data = _discogs_hydrate_release_or_master_data("release", rel_data, int(release_id))
        except DiscogsRateLimited:
            raise
        except Exception as e:
            # Skip invalid ids (Discogs sometimes returns masters/releases that don't resolve).
            if getattr(e, "status_code", None) == 404:
                continue
            raise
        if not isinstance(rel_data, dict):
            continue

        title = str(rel_data.get("title") or album_title or "").strip() or str(album_title or "").strip()
        year_val = rel_data.get("year")
        year = str(year_val).strip() if year_val else ""

        search_meta = candidate_meta.get(int(release_id), {}) if candidate_meta else {}
        cover_url = None
        images = rel_data.get("images") or []
        if isinstance(images, list) and images:
            img0 = images[0] if isinstance(images[0], dict) else None
            if img0:
                cover_url = (img0.get("uri") or img0.get("resource_url") or "").strip() or None
        if not cover_url:
            cover_url = (
                str(search_meta.get("cover_image") or "").strip()
                or str(search_meta.get("thumb") or "").strip()
                or None
            )

        artist_str = (artist_name or "").strip()
        artists = rel_data.get("artists") or []
        if isinstance(artists, list) and artists:
            a0 = artists[0] if isinstance(artists[0], dict) else None
            if a0 and a0.get("name"):
                artist_str = str(a0.get("name") or "").strip() or artist_str

        tracklist: list[str] = []
        for tr in rel_data.get("tracklist") or []:
            if not isinstance(tr, dict):
                continue
            t_title = tr.get("title")
            if t_title:
                tracklist.append(str(t_title))
        labels: list[str] = []
        catalog_numbers: list[str] = []
        for label_info in rel_data.get("labels") or []:
            if not isinstance(label_info, dict):
                continue
            label_name = str(label_info.get("name") or "").strip()
            if label_name:
                labels.append(label_name)
            catno = str(label_info.get("catno") or "").strip()
            if catno:
                catalog_numbers.append(catno)
        notes = rel_data.get("notes")
        notes_text = ""
        if isinstance(notes, str):
            notes_text = _strip_html_text(notes.strip())
        elif isinstance(notes, list):
            parts = [_strip_html_text(str(x or "").strip()) for x in notes if str(x or "").strip()]
            if parts:
                notes_text = _truncate_text(" ".join(parts), max_chars=2400)
        community = rel_data.get("community") if isinstance(rel_data.get("community"), dict) else {}
        rating_block = community.get("rating") if isinstance(community.get("rating"), dict) else {}
        public_rating = _safe_bounded_float(rating_block.get("average"))
        public_rating_votes = _safe_nonneg_int(rating_block.get("count"))
        discogs_have_count = _safe_nonneg_int(community.get("have"))
        discogs_want_count = _safe_nonneg_int(community.get("want"))

        release_id_str = str(rel_data.get("id") or release_id or "").strip()
        master_id = str(rel_data.get("master_id") or "").strip()
        if not master_id:
            master_val = rel_data.get("master")
            if isinstance(master_val, dict):
                master_id = str(master_val.get("id") or "").strip()
        if not master_id:
            master_id = str(search_meta.get("master_id") or "").strip()

        return {
            "title": title,
            "year": year,
            "cover_url": cover_url,
            "artist_name": artist_str,
            "tracklist": tracklist,
            "label": _dedupe_keep_order(labels),
            "catalog_number": _dedupe_keep_order(catalog_numbers),
            "notes": notes_text,
            "release_id": release_id_str,
            "master_id": master_id,
            "public_rating": public_rating,
            "public_rating_votes": public_rating_votes,
            "public_rating_source": "discogs",
            "discogs_have_count": discogs_have_count,
            "discogs_want_count": discogs_want_count,
            "identity_scope": "discogs_release",
        }
    return None

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}

def _fetch_discogs_release_by_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_discogs_release_by_id(*args, **kwargs)

def _provider_error_text_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_error_text(*args, **kwargs)

def _provider_is_name_resolution_failure_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_is_name_resolution_failure(*args, **kwargs)

def _discogs_min_interval_sec_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _discogs_min_interval_sec(*args, **kwargs)

def _discogs_throttle_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _discogs_throttle(*args, **kwargs)

def _discogs_penalize_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _discogs_penalize(*args, **kwargs)

def _get_discogs_client_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _get_discogs_client(*args, **kwargs)

def _get_or_create_discogs_client_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _get_or_create_discogs_client(*args, **kwargs)

def _discogs_api_get_json_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _discogs_api_get_json(*args, **kwargs)

def _discogs_hydrate_release_or_master_data_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _discogs_hydrate_release_or_master_data(*args, **kwargs)

def _discogs_call_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _discogs_call(*args, **kwargs)

def _run_discogs_preflight_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_discogs_preflight(*args, **kwargs)

def _discogs_lookup_candidate_cap_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _discogs_lookup_candidate_cap(*args, **kwargs)

def _discogs_search_identity_from_data_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _discogs_search_identity_from_data(*args, **kwargs)

def _discogs_search_candidate_score_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _discogs_search_candidate_score(*args, **kwargs)

def _fetch_discogs_release_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_discogs_release(*args, **kwargs)
