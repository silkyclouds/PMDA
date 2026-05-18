"""Runtime-backed MusicBrainz release-group matching.

This module contains the heavy MusicBrainz release-group search and arbitration
implementation extracted from ``pmda.py``. It still binds the live runtime module
at the boundary while provider clients, caches, and scan policy are progressively
split into explicit services.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Optional

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one MusicBrainz search call."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {"search_mb_release_group_by_metadata"}
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def _runtime_module() -> Any:
    """Return the live PMDA runtime module when bound, else this module."""
    return _RUNTIME if _RUNTIME is not None else sys.modules[__name__]


def search_mb_release_group_by_metadata_for_runtime(
    runtime: Any,
    artist: str,
    album_norm: str,
    tracks: set[str],
    title_raw: Optional[str] = None,
    album_folder: Optional[Path] = None,
    local_tags: Optional[dict] = None,
    local_paths: Optional[list[Any]] = None,
    scan_inline: bool | None = None,
) -> tuple[dict | None, bool]:
    """Search MusicBrainz release groups using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _search_mb_release_group_by_metadata_impl(
        artist,
        album_norm,
        tracks,
        title_raw=title_raw,
        album_folder=album_folder,
        local_tags=local_tags,
        local_paths=local_paths,
        scan_inline=scan_inline,
    )


def _search_mb_release_group_by_metadata_impl(
    artist: str,
    album_norm: str,
    tracks: set[str],
    title_raw: Optional[str] = None,
    album_folder: Optional[Path] = None,
    local_tags: Optional[dict] = None,
    local_paths: Optional[list[Any]] = None,
    scan_inline: bool | None = None,
) -> tuple[dict | None, bool]:
    """
    Fallback search on MusicBrainz by artist name, normalized album title, and optional track titles.
    Tries: (1) search with strict=True, (2) search with strict=False, (3) browse by artist and match title,
    (4) if still no candidates and title_raw differs from album_norm, retry search/browse with title_raw (e.g. "Isolette").
    If multiple candidates and USE_AI_FOR_MB_MATCH, uses AI to pick best match.
    When USE_AI_VISION_FOR_COVER and album_folder are set, compares local cover to Cover Art Archive (vision).
    When 0 candidates or AI returns NONE, can use Bandcamp + web search (Serper) + AI to suggest MBID.
    Returns (release-group info dict or None, verified_by_ai: bool). verified_by_ai is True when match was chosen by USE_AI_FOR_MB_VERIFY.
    """
    if album_folder is not None and not isinstance(album_folder, Path):
        album_folder = Path(album_folder) if album_folder else None
    mb_search_started = time.perf_counter()
    mb_budget_sec = max(0, int(getattr(_runtime_module(), "MB_SEARCH_ALBUM_TIMEOUT_SEC", 0) or 0))
    candidate_fetch_limit = max(0, int(getattr(_runtime_module(), "MB_CANDIDATE_FETCH_LIMIT", 0) or 0))
    tracklist_fetch_limit = max(0, int(getattr(_runtime_module(), "MB_TRACKLIST_FETCH_LIMIT", 0) or 0))
    fast_fallback_mode = bool(getattr(_runtime_module(), "MB_FAST_FALLBACK_MODE", False))
    scan_inline_mode = _scan_inline_matching_active() if scan_inline is None else bool(scan_inline)
    allow_web_fallback = bool(
        getattr(_runtime_module(), "USE_WEB_SEARCH_FOR_MB", False)
    ) and not scan_inline_mode
    provider_fallback_cache: dict | None = None
    use_ai_for_mb_match = bool(getattr(_runtime_module(), "USE_AI_FOR_MB_MATCH", False))
    use_ai_for_mb_verify = bool(getattr(_runtime_module(), "USE_AI_FOR_MB_VERIFY", False))
    use_ai_for_mb = bool(use_ai_for_mb_match or use_ai_for_mb_verify)
    search_local_tags = local_tags if isinstance(local_tags, dict) else {}
    search_local_paths = list(local_paths or [])
    search_local_context = _classical_identity_context(
        local_artist=artist,
        local_title=title_raw or album_norm or "",
        local_tracks=list(tracks or []),
        local_tags=search_local_tags,
        local_paths=search_local_paths,
    )
    if bool(search_local_context.get("is_classical")) and use_ai_for_mb:
        logging.info(
            "[MusicBrainz] %s – %r: classical context detected; using deterministic-only MB arbitration (AI disambiguation disabled)",
            artist,
            title_raw or album_norm or "?",
        )
        use_ai_for_mb_match = False
        use_ai_for_mb_verify = False
        use_ai_for_mb = False

    def _mb_budget_exceeded() -> bool:
        if mb_budget_sec <= 0:
            return False
        return (time.perf_counter() - mb_search_started) >= mb_budget_sec

    def _provider_fallbacks() -> dict:
        nonlocal provider_fallback_cache
        if provider_fallback_cache is None:
            provider_fallback_cache = _fetch_album_provider_fallbacks_parallel(
                artist,
                (title_raw or album_norm or "").strip(),
                scan_inline=scan_inline_mode,
            )
        return provider_fallback_cache

    def _fetch_rg_details(rg_id: str):
        """Fetch release group details by ID. On 404, try treating rg_id as a release ID and resolve to release-group."""
        try:
            info = musicbrainzngs.get_release_group_by_id(
                rg_id, includes=["releases", "artist-credits"]
            )["release-group"]
            return info
        except musicbrainzngs.WebServiceError as e:
            if "404" not in str(e):
                raise
            # API returned 404: rg_id may be a release ID (search/browse can occasionally return release refs)
            # use_queue=False to avoid deadlock (we are already inside the MB queue worker)
            resolved = resolve_mbid_to_release_group(rg_id, "musicbrainz_releaseid", use_queue=False)
            if resolved and resolved != rg_id:
                info = musicbrainzngs.get_release_group_by_id(
                    resolved, includes=["releases", "artist-credits"]
                )["release-group"]
                return info
            raise

    seen_ids: set[str] = set()
    candidates: List[dict] = []

    def _collect_candidates(search_results: List[dict]) -> None:
        for rg in search_results:
            rg_id = rg.get("id")
            if rg_id and rg_id not in seen_ids:
                seen_ids.add(rg_id)
                candidates.append(rg)

    def _run_search_and_browse(release_query: str) -> None:
        if _mb_budget_exceeded():
            return
        if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
            try:
                _collect_candidates(get_mb_queue().submit(f"search_{artist}_{release_query}_1", lambda: _search_mb_rg_candidates(artist, release_query, True)))
            except Exception as e:
                logging.debug("[MusicBrainz Search] strict query failed for %r - %r: %s", artist, release_query, e)
            if not _mb_budget_exceeded():
                try:
                    _collect_candidates(get_mb_queue().submit(f"search_{artist}_{release_query}_0", lambda: _search_mb_rg_candidates(artist, release_query, False)))
                except Exception as e:
                    logging.debug("[MusicBrainz Search] relaxed query failed for %r - %r: %s", artist, release_query, e)
        else:
            try:
                _collect_candidates(_search_mb_rg_candidates(artist, release_query, True))
            except Exception as e:
                logging.debug("[MusicBrainz Search] strict query failed for %r - %r: %s", artist, release_query, e)
            if not _mb_budget_exceeded():
                try:
                    _collect_candidates(_search_mb_rg_candidates(artist, release_query, False))
                except Exception as e:
                    logging.debug("[MusicBrainz Search] relaxed query failed for %r - %r: %s", artist, release_query, e)
        if not candidates and not _mb_budget_exceeded():
            browse_list = _browse_mb_rg_by_artist(artist, release_query)
            _collect_candidates(browse_list)

    try:
        # 1–3) Search (strict, non-strict) and browse by artist with normalized title
        _run_search_and_browse(album_norm)

        # 4) If still no candidates, try with raw title (e.g. "Isolette") — MusicBrainz may match exact casing
        if (not candidates) and (not _mb_budget_exceeded()) and title_raw and (title_raw.strip() != album_norm):
            raw_clean = title_raw.strip()
            if raw_clean:
                _run_search_and_browse(raw_clean)

        raw_candidate_count = len(candidates)
        if candidates:
            prefiltered_candidates = _prefilter_mb_release_group_candidates(
                artist_name=artist,
                album_title=title_raw or album_norm or "",
                candidates=candidates,
            )
            if prefiltered_candidates:
                if len(prefiltered_candidates) != raw_candidate_count:
                    logging.info(
                        "[MusicBrainz] %s – %r: prefiltered %d/%d candidate(s) before detailed fetch",
                        artist,
                        title_raw or album_norm or "?",
                        len(prefiltered_candidates),
                        raw_candidate_count,
                    )
                candidates = prefiltered_candidates
            else:
                logging.info(
                    "[MusicBrainz] %s – %r: no title-relevant candidate remained after prefilter (%d raw candidate(s)); switching to provider fallback flow",
                    artist,
                    title_raw or album_norm or "?",
                    raw_candidate_count,
                )
                candidates = []
        if candidates and bool(search_local_context.get("is_classical")):
            def _search_candidate_rank(item: dict) -> tuple[float, float, float]:
                title_score = _provider_identity_text_score(
                    title_raw or album_norm or "",
                    str(item.get("title") or ""),
                )
                candidate_artists = _extract_mb_artist_names(item)
                artist_score = max(
                    (_provider_identity_text_score(artist, name) for name in candidate_artists),
                    default=0.0,
                )
                year_score = 0.0
                local_year = _mb_extract_year(search_local_context.get("year"))
                candidate_year = _mb_extract_year(item.get("first-release-date"))
                if local_year and candidate_year:
                    try:
                        diff = abs(int(local_year) - int(candidate_year))
                    except Exception:
                        diff = 99
                    if diff == 0:
                        year_score = 1.0
                    elif diff <= 1:
                        year_score = 0.75
                    elif diff <= 5:
                        year_score = 0.35
                return (title_score, artist_score, year_score)

            candidates = sorted(candidates, key=_search_candidate_rank, reverse=True)

        if candidates:
            letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            display_limit = _MB_SEARCH_LOG_CANDIDATE_LIMIT
            log_mb(
                "%s – %r: %d candidate(s) from search",
                artist,
                title_raw or album_norm or "?",
                len(candidates),
            )
            for i, rg in enumerate(candidates[:display_limit]):
                log_mb(
                    "Candidate %s: %s (id=%s)",
                    letters[i],
                    (rg.get("title") or "?"),
                    rg.get("id") or "?",
                )
            if len(candidates) > display_limit:
                log_mb("... and %d more candidate(s)", len(candidates) - display_limit)

        all_fetched: List[tuple] = []
        # Intentionally avoid an "AI-first" pick here. We fetch a limited number of candidates and
        # only use AI later if disambiguation is genuinely ambiguous.

        if candidate_fetch_limit > 0 and len(candidates) > candidate_fetch_limit:
            logging.info(
                "[MusicBrainz] %s – %r: %d candidate(s); limiting detailed fetch to top %d for speed",
                artist,
                title_raw or album_norm or "?",
                len(candidates),
                candidate_fetch_limit,
            )

        effective_candidate_fetch_limit = candidate_fetch_limit
        effective_tracklist_fetch_limit = tracklist_fetch_limit
        if bool(search_local_context.get("is_classical")):
            classical_fetch_cap = 5
            classical_tracklist_cap = 3
            if effective_candidate_fetch_limit > 0:
                effective_candidate_fetch_limit = min(effective_candidate_fetch_limit, classical_fetch_cap)
            else:
                effective_candidate_fetch_limit = min(len(candidates), classical_fetch_cap)
            if effective_tracklist_fetch_limit > 0:
                effective_tracklist_fetch_limit = min(effective_tracklist_fetch_limit, classical_tracklist_cap)
            else:
                effective_tracklist_fetch_limit = min(effective_candidate_fetch_limit, classical_tracklist_cap)
            if len(candidates) > effective_candidate_fetch_limit:
                logging.info(
                    "[MusicBrainz] %s – %r: classical context detected; limiting detailed fetch to top %d/%d candidate(s)",
                    artist,
                    title_raw or album_norm or "?",
                    effective_candidate_fetch_limit,
                    len(candidates),
                )
        candidates_to_fetch = candidates[:effective_candidate_fetch_limit] if effective_candidate_fetch_limit > 0 else candidates
        for idx, rg in enumerate(candidates_to_fetch):
            if _mb_budget_exceeded():
                logging.info(
                    "[MusicBrainz] %s – %r: MB budget reached after %.1fs (budget=%ds); stopping detailed candidate fetch and using fallback flow",
                    artist,
                    title_raw or album_norm or "?",
                    time.perf_counter() - mb_search_started,
                    mb_budget_sec,
                )
                break
            try:
                rg_id = rg['id']
                if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
                    info = get_mb_queue().submit(f"rg_{rg_id}", lambda rid=rg_id: _fetch_rg_details(rid))
                else:
                    info = _fetch_rg_details(rg_id)
                mb_track_count = _mb_track_count_from_rg_info(info)
                formats = set()
                for release in info.get('release-list', []):
                    for medium in release.get('medium-list', []):
                        fmt = medium.get('format')
                        qty = medium.get('track-count', 1)
                        if fmt:
                            formats.add(f"{qty}×{fmt}")
                rg_id_final = info.get('id', rg['id'])
                mb_title = str(info.get("title") or rg.get("title") or "").strip()
                mb_artists = _extract_mb_artist_names(info) or _extract_mb_artist_names(rg)
                mb_provider_payload = {
                    "id": rg_id_final,
                    "title": mb_title,
                    "mb_artist_names": mb_artists,
                    "tracklist": list(info.get("track_titles") or []),
                    "year": _mb_extract_year(info.get("first-release-date") or rg.get("first-release-date") or ""),
                }
                strict_ok, strict_reason = _strict_identity_match_details(
                    local_artist=artist,
                    local_title=title_raw or album_norm,
                    candidate_artist=mb_artists,
                    candidate_title=mb_title,
                    local_tracks=list(tracks or []),
                    local_tags=search_local_tags,
                    local_paths=search_local_paths,
                    provider="musicbrainz",
                    provider_payload=mb_provider_payload,
                    local_context=search_local_context,
                )
                if not strict_ok:
                    _log_mb_candidate_rejection(
                        artist,
                        title_raw or album_norm,
                        rg_id_final,
                        strict_reason,
                    )
                    continue
                result_dict = {
                    'primary_type': info.get('primary-type', ''),
                    'secondary_types': info.get('secondary-types', []),
                    'format_summary': ', '.join(sorted(formats)),
                    'id': rg_id_final,
                    'track_count': mb_track_count,
                    # Phase 4: Cover Art Archive URL for optional vision comparison
                    'cover_url': f"https://coverartarchive.org/release-group/{rg_id_final}/front",
                    'mb_title': mb_title,
                    'mb_artist_names': mb_artists,
                }
                # Optionally fetch recording-level track titles for deterministic tracklist crosschecks
                # (no LLM cost, but this endpoint is heavier; controlled by MB_TRACKLIST_FETCH_LIMIT).
                if info.get('release-list') and tracks and (effective_tracklist_fetch_limit <= 0 or idx < effective_tracklist_fetch_limit):
                    first_release_id = info['release-list'][0].get('id')
                    if first_release_id:
                        try:
                            def _fetch_release_recordings(rel_id: str):
                                return musicbrainzngs.get_release_by_id(rel_id, includes=["recordings"])
                            if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
                                rel_resp = get_mb_queue().submit(f"rel_rec_{first_release_id}", lambda rid=first_release_id: _fetch_release_recordings(rid))
                            else:
                                rel_resp = _fetch_release_recordings(first_release_id)
                            result_dict['track_titles'] = _extract_track_titles_from_mb_release(rel_resp)
                            mb_provider_payload["tracklist"] = list(result_dict.get("track_titles") or [])
                        except musicbrainzngs.WebServiceError:
                            pass
                all_fetched.append((rg, result_dict))
            except musicbrainzngs.WebServiceError:
                continue

        if all_fetched:
            letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            display_limit = _MB_SEARCH_LOG_CANDIDATE_LIMIT
            log_mb(
                "%s – %r: fetched details for %d candidate(s)",
                artist,
                title_raw or album_norm or "?",
                len(all_fetched),
            )
            for i, r in enumerate(all_fetched[:display_limit]):
                log_mb(
                    "Fetched %s: %s | %s | %d tracks | %s",
                    letters[i],
                    (r[0].get("title") or "?"),
                    r[1].get("id") or "?",
                    r[1].get("track_count", 0),
                    (r[1].get("format_summary") or "")[:40],
                )
            if len(all_fetched) > display_limit:
                log_mb("... and %d more fetched candidate(s)", len(all_fetched) - display_limit)

        if not all_fetched:
            title_for_zero = (title_raw or album_norm or "").strip()
            if fast_fallback_mode:
                if _mb_budget_exceeded():
                    reason = f"budget exceeded (budget={mb_budget_sec}s)"
                else:
                    reason = "no MB candidates/details"
                logging.info(
                    "[MusicBrainz] %s – %r: fast fallback mode -> skipping web+AI MBID hunt (%s)",
                    artist,
                    title_for_zero or album_norm or "?",
                    reason,
                )
                return (None, False)

            provider_info = _provider_fallbacks() if title_for_zero else {}
            provider_sources = list(provider_info.get("extra_sources") or [])
            if provider_sources:
                log_provider("%s – %r: no usable MusicBrainz match yet; fallback sources ready:", artist, title_for_zero or "?")
                for src in provider_sources:
                    src_name = src.get("source", "?")
                    src_title = src.get("title") or src.get("album") or "?"
                    src_artist = src.get("artist") or src.get("artist_name") or "?"
                    log_provider("%s: %r by %s", src_name, src_title, src_artist)

            # Never run the slow AI MBID inference path during scan/discovery.
            # Inline provider matching should stop at direct providers and leave
            # speculative web+AI MBID repair to manual/post-scan flows.
            if scan_inline_mode:
                return (None, False)

            if (not use_ai_for_mb) or (not getattr(_runtime_module(), "ai_provider_ready", False)):
                return (None, False)

            # Optional slow path: try web + AI to infer MBID even when MB has no usable candidate.
            bandcamp_text = ""
            bc_info = provider_info.get("bandcamp") if provider_info else None
            if bc_info:
                bandcamp_text = f"Bandcamp: title={bc_info.get('title')}, artist={bc_info.get('artist_name')}"

            # IMPORTANT: web+AI MBID hunting is high-risk (false positives like Wikipedia disambiguations).
            # Only attempt it when we have at least one grounding signal from providers (Discogs/Last.fm/Bandcamp).
            has_grounding = bool(provider_sources) or bool(bandcamp_text)
            web_snippets = ""
            web_results = []
            if has_grounding and allow_web_fallback and title_for_zero:
                q = f"{artist} {title_for_zero} album"
                web_results = _web_search_serper(q, num=10)
                if web_results:
                    parts = [
                        f"Web {i+1}) {r.get('title')} — {r.get('snippet')} ({r.get('link')})"
                        for i, r in enumerate(web_results[:10])
                    ]
                    web_snippets = "\n".join(parts)

            if bandcamp_text or web_snippets:
                prompt = f"Our album: artist={artist}, title={title_for_zero}. No usable MusicBrainz candidate.\n"
                if bandcamp_text:
                    prompt += bandcamp_text + "\n"
                if web_snippets:
                    prompt += "Web results:\n" + web_snippets + "\n"
                prompt += "Suggest a MusicBrainz release group ID (MBID, UUID format) if you can identify it from the above, or reply exactly NONE. Optionally end with (confidence: N) where N is 0-100."
                try:
                    provider = getattr(_runtime_module(), "AI_PROVIDER", "openai")
                    model = getattr(_runtime_module(), "RESOLVED_MODEL", None) or getattr(_runtime_module(), "OPENAI_MODEL", "gpt-4o-mini")
                    reply = _call_ai_provider_bounded(
                        provider=provider,
                        model=model,
                        system_msg="You reply with a single MBID (UUID) or the word NONE. Optionally end with (confidence: N).",
                        user_msg=prompt,
                        max_tokens=70,
                        analysis_type="web_mbid_inference",
                        timeout_sec=min(
                            float(AI_SCAN_HARD_TIMEOUT_SEC or 120.0),
                            _FILES_SCAN_WEB_MBID_AI_TIMEOUT_SEC if _scan_pipeline_active() else 45.0,
                        ),
                        log_prefix="[MusicBrainz Web+AI]",
                    )
                    reply_clean, ai_confidence = parse_ai_confidence((reply or "").strip())
                    if ai_confidence is not None:
                        logging.info("[MusicBrainz] Bandcamp/web AI MBID suggestion confidence: %d", ai_confidence)
                    reply = reply_clean.upper()
                    if reply and reply != "NONE":
                        mbid_match = re.search(r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}", reply)
                        if mbid_match:
                            suggested_mbid = mbid_match.group(0)
                            try:
                                info = _fetch_rg_details(suggested_mbid)
                                mb_track_count = _mb_track_count_from_rg_info(info)
                                formats = set()
                                for release in info.get("release-list", []):
                                    for medium in release.get("medium-list", []):
                                        fmt = medium.get("format")
                                        qty = medium.get("track-count", 1)
                                        if fmt:
                                            formats.add(f"{qty}×{fmt}")
                                rg_id_final = info.get("id", suggested_mbid)
                                mb_title = str(info.get("title") or "").strip()
                                mb_artists = _extract_mb_artist_names(info)
                                mb_provider_payload = {
                                    "id": rg_id_final,
                                    "title": mb_title,
                                    "mb_artist_names": mb_artists,
                                    "year": _mb_extract_year(info.get("first-release-date") or ""),
                                }
                                strict_ok, strict_reason = _strict_identity_match_details(
                                    local_artist=artist,
                                    local_title=title_for_zero or album_norm,
                                    candidate_artist=mb_artists,
                                    candidate_title=mb_title,
                                    local_tracks=list(tracks or []),
                                    local_tags=search_local_tags,
                                    local_paths=search_local_paths,
                                    provider="musicbrainz",
                                    provider_payload=mb_provider_payload,
                                    local_context=search_local_context,
                                )
                                if not strict_ok:
                                    log_mb(
                                        "Album %s – \"%s\": web+AI suggested MBID %s rejected (%s)",
                                        artist,
                                        title_for_zero or album_norm,
                                        rg_id_final,
                                        strict_reason,
                                    )
                                    raise RuntimeError("strict identity mismatch")
                                result_dict = {
                                    "primary_type": info.get("primary-type", ""),
                                    "secondary_types": info.get("secondary-types", []),
                                    "format_summary": ", ".join(sorted(formats)),
                                    "id": rg_id_final,
                                    "track_count": mb_track_count,
                                    "cover_url": f"https://coverartarchive.org/release-group/{rg_id_final}/front",
                                    "mb_title": mb_title,
                                    "mb_artist_names": mb_artists,
                                }
                                set_cached_mb_info(rg_id_final, result_dict)
                                logging.info("[MusicBrainz] MBID suggested by AI from Bandcamp/web for artist=%r album=%r: %s", artist, album_norm, rg_id_final)
                                return (result_dict, True)
                            except RuntimeError:
                                pass
                            except musicbrainzngs.WebServiceError:
                                logging.debug("[MusicBrainz] AI-suggested MBID %s invalid or 404", suggested_mbid)
                except Exception as e:
                    logging.debug("[MusicBrainz] Bandcamp/web AI suggestion failed: %s", e)
            return (None, False)

        matching: List[tuple] = [
            x for x in all_fetched
            if not tracks or abs(len(tracks) - x[1].get('track_count', 0)) <= 1
        ]

        # Deterministic fast-path: if track titles were fetched for *all* candidates and exactly one is a perfect
        # tracklist match, accept it without AI (secure + cheaper). This only triggers when we have enough local
        # signal (track titles) and enough remote signal (track titles for every candidate).
        if tracks and len(matching) >= 2:
            try:
                local_titles = list(tracks)
                scored: list[tuple[float, dict]] = []
                missing_titles = 0
                for _rg, info in matching:
                    cand_titles = info.get("track_titles") if isinstance(info, dict) else None
                    if not cand_titles:
                        missing_titles += 1
                        continue
                    score = float(_crosscheck_tracklist_perfect(local_titles, list(cand_titles)))
                    scored.append((score, info))
                if scored and missing_titles == 0:
                    scored.sort(key=lambda x: -x[0])
                    best_score = float(scored[0][0])
                    best_count = sum(1 for s, _ in scored if abs(float(s) - best_score) < 1e-9)
                    if best_count == 1 and best_score >= 0.999:
                        picked = scored[0][1]
                        set_cached_mb_info(picked['id'], picked)
                        log_mb(
                            "Album %s – \"%s\": accepted MB candidate %s (perfect tracklist match; skipped AI)",
                            artist,
                            title_raw or album_norm,
                            picked.get("id") or "?",
                        )
                        return (picked, False)
            except Exception:
                pass

        if use_ai_for_mb_verify and len(matching) >= 2:
            # AI verify is expensive (prompt can include tracklists). Only use it when there is real ambiguity.
            title_for_strict = (title_raw or album_norm or "").strip()
            strict_matches: list[tuple] = []
            if title_for_strict:
                for rg, info in all_fetched[:12]:
                    cand_title = str(info.get("mb_title") or rg.get("title") or "").strip()
                    cand_artists = info.get("mb_artist_names") or _extract_mb_artist_names(rg)
                    ok, _reason = _strict_identity_match_details(
                        local_artist=artist,
                        local_title=title_for_strict,
                        candidate_artist=cand_artists,
                        candidate_title=cand_title,
                        local_tracks=list(tracks or []),
                        local_tags=search_local_tags,
                        local_paths=search_local_paths,
                        provider="musicbrainz",
                        provider_payload=info if isinstance(info, dict) else {},
                        local_context=search_local_context,
                    )
                    if ok:
                        strict_matches.append((rg, info))
            if len(strict_matches) == 1:
                # Unique strict match: no need to pay for an LLM disambiguation call.
                picked = strict_matches[0][1]
                set_cached_mb_info(picked['id'], picked)
                log_mb(
                    "Album %s – \"%s\": accepted MB candidate %s (strict identity unique; skipped AI verify)",
                    artist,
                    title_for_strict or album_norm,
                    picked.get("id") or "?",
                )
                return (picked, False)

            # Phase 3: optional Discogs/Last.fm/Bandcamp for AI disambiguation (only when we actually call AI).
            extra_sources: List[dict] = []
            title_for_fetch = title_for_strict
            if title_for_fetch:
                extra_sources = list((_provider_fallbacks() or {}).get("extra_sources") or [])
            if extra_sources:
                log_provider("%s – %r: extra sources for AI disambiguation:", artist, title_for_fetch or "?")
                for s in extra_sources:
                    src = s.get("source", "?")
                    title = s.get("title") or s.get("album") or "?"
                    artist_val = s.get("artist") or s.get("artist_name") or "?"
                    log_provider("%s: %r by %s", src, title, artist_val)

            chosen, _ = ai_verify_mb_match(
                artist, title_raw, album_norm,
                list(tracks) if tracks else None,
                len(tracks) if tracks else 0,
                all_fetched,
                has_cover=False,
                extra_sources=extra_sources or None,
            )
            if chosen:
                chosen_title = str(chosen[1].get("mb_title") or chosen[0].get("title") or "").strip()
                chosen_artists = chosen[1].get("mb_artist_names") or _extract_mb_artist_names(chosen[0])
                strict_ok, strict_reason = _strict_identity_match_details(
                    local_artist=artist,
                    local_title=title_raw or album_norm,
                    candidate_artist=chosen_artists,
                    candidate_title=chosen_title,
                    local_tracks=list(tracks or []),
                    local_tags=search_local_tags,
                    local_paths=search_local_paths,
                    provider="musicbrainz",
                    provider_payload=chosen[1] if isinstance(chosen[1], dict) else {},
                    local_context=search_local_context,
                )
                if not strict_ok:
                    log_mb(
                        "Album %s – \"%s\": AI-verified candidate rejected (%s)",
                        artist,
                        title_raw or album_norm,
                        strict_reason,
                    )
                    chosen = None
            if chosen:
                # Partie 1: Optional vision check (local cover vs Cover Art Archive)
                # Vision cover comparison is expensive and can reject correct matches; keep it off by default.
                # We already guard identity via strict checks + tracklist evidence above.
                use_vision = False
                if use_vision:
                    folder_path = Path(album_folder) if not isinstance(album_folder, Path) else album_folder
                    local_data_uri = _get_local_cover_data_uri_for_vision(folder_path)
                    mb_cover_url = chosen[1].get("cover_url")
                    if local_data_uri and mb_cover_url:
                        try:
                            vision_model = getattr(_runtime_module(), "RESOLVED_MODEL", None) or getattr(_runtime_module(), "OPENAI_MODEL", "gpt-4o-mini")
                            sys_msg = "You reply with exactly Yes or No. If No, add in parentheses the reason, e.g. (artist photo) or (different cover). Optionally end with (confidence: N) where N is 0-100."
                            user_msg = (
                                "Image 1 is the local album cover. Image 2 is the MusicBrainz Cover Art Archive cover. "
                                "Do they represent the same album cover? Reply: Yes or No. "
                                "If No, add in parentheses why (e.g. 'No (artist photo)' or 'No (different album cover)'). "
                                "Optionally end with (confidence: N)."
                            )
                            log_cov(
                                "Vision: comparing local cover vs CAA for artist=%r album=%r (model=%s)",
                                artist,
                                album_norm or title_raw or "",
                                vision_model,
                            )
                            resp = call_ai_provider_vision(
                                getattr(_runtime_module(), "AI_PROVIDER", "openai"),
                                vision_model,
                                sys_msg,
                                user_msg,
                                image_urls=[mb_cover_url],
                                image_base64=[{"type": "image_url", "image_url": {"url": local_data_uri}}],
                                max_tokens=20,
                                analysis_type="cover_vision_verify",
                            )
                            verdict_clean, vision_confidence = parse_ai_confidence((resp or "").strip())
                            if vision_confidence is not None:
                                logging.info("[MusicBrainz Vision] Cover comparison confidence: %d", vision_confidence)
                            verdict = (verdict_clean or "").strip().upper()
                            # Log comparison result clearly: successful (same cover) or rejected (with reason if present)
                            if verdict and "YES" in verdict:
                                log_cov(
                                    "Vision comparison result: accepted — same album cover (artist=%r album=%r)%s",
                                    artist, album_norm or title_raw or "",
                                    " confidence=%d" % vision_confidence if vision_confidence is not None else "",
                                )
                            else:
                                reason = (verdict_clean or "").strip()
                                if not reason:
                                    reason = "no verdict"
                                log_cov(
                                    "Vision comparison result: rejected — %s (artist=%r album=%r)%s",
                                    reason, artist, album_norm or title_raw or "",
                                    " confidence=%d" % vision_confidence if vision_confidence is not None else "",
                                )
                            if verdict and "NO" in verdict:
                                chosen = None
                                log_cov(
                                    "Cover mismatch: rejecting AI-chosen MB match for artist=%r album=%r based on vision",
                                    artist,
                                    album_norm or title_raw or "",
                                )
                        except Exception as e:
                            logging.debug("[MusicBrainz Vision] Vision check failed: %s", e)
                if chosen:
                    set_cached_mb_info(chosen[1]['id'], chosen[1])
                    logging.info("[MusicBrainz Verify] Match verified by AI for artist=%r album=%r", artist, album_norm)
                    return (chosen[1], True)
            # Partie 2 Cas 2: AI said NONE but we have candidates; try web search + one more AI call
            if (
                chosen is None
                and all_fetched
                and allow_web_fallback
                and (not fast_fallback_mode)
                and (not _mb_budget_exceeded())
            ):
                title_for_web = (title_raw or album_norm or "").strip()
                if title_for_web:
                    q = f"{artist} {title_for_web} album"
                    web_results = _web_search_serper(q, num=10)
                    if web_results:
                        log_provider("%s – %r: web search (AI said NONE, retry with web):", artist, title_for_web)
                        log_provider("Query: %s — %d result(s)", q, len(web_results))
                        for i, r in enumerate(web_results[:8]):
                            title_preview = (r.get("title") or "?")[:70]
                            snippet_preview = str((r.get("snippet") or "")[:100]).replace("\n", " ")
                            log_provider("Result %d: %s", i + 1, title_preview)
                            if r.get("snippet"):
                                log_provider("Snippet %d: %s", i + 1, snippet_preview)
                        if len(web_results) > 8:
                            log_provider("... and %d more result(s)", len(web_results) - 8)
                        web_snippets = "\n".join([f"Web {i+1}) {r.get('title')} — {r.get('snippet')} ({r.get('link')})" for i, r in enumerate(web_results[:10])])
                        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                        choices = [f"{letters[i]}: {all_fetched[i][0].get('title', '?')} (MBID: {all_fetched[i][1].get('id')})" for i in range(min(len(all_fetched), 10))]
                        prompt = f"Artist: {artist}. Album: {title_for_web}. We had MusicBrainz candidates but none matched. Web results:\n{web_snippets}\n\nCandidates: " + " | ".join(choices)
                        prompt += "\nReply with the letter (A/B/...) of the correct candidate, or an MBID (UUID) if you find one in web results, or NONE. Optionally end with (confidence: N) where N is 0-100."
                        try:
                            provider = getattr(_runtime_module(), "AI_PROVIDER", "openai")
                            model = getattr(_runtime_module(), "RESOLVED_MODEL", None) or getattr(_runtime_module(), "OPENAI_MODEL", "gpt-4o-mini")
                            reply = _call_ai_provider_bounded(
                                provider=provider,
                                model=model,
                                system_msg="You reply with a single letter, or an MBID (UUID), or NONE. Optionally end with (confidence: N).",
                                user_msg=prompt,
                                max_tokens=70,
                                analysis_type="mb_retry_disambiguation",
                                timeout_sec=AI_SCAN_HARD_TIMEOUT_SEC,
                                log_prefix="[MusicBrainz Retry]",
                            )
                            reply_clean, ai_confidence = parse_ai_confidence((reply or "").strip())
                            if ai_confidence is not None:
                                logging.info("[MusicBrainz Verify] Web+AI choice confidence: %d", ai_confidence)
                            reply = reply_clean.upper()
                            if reply and reply != "NONE":
                                letter = reply[:1]
                                idx = letters.find(letter)
                                if 0 <= idx < len(all_fetched):
                                    picked = all_fetched[idx][1]
                                    strict_ok, strict_reason = _strict_identity_match_details(
                                        local_artist=artist,
                                        local_title=title_for_web or album_norm,
                                        candidate_artist=picked.get("mb_artist_names") or _extract_mb_artist_names(all_fetched[idx][0]),
                                        candidate_title=picked.get("mb_title") or all_fetched[idx][0].get("title") or "",
                                        local_tracks=list(tracks or []),
                                        local_tags=search_local_tags,
                                        local_paths=search_local_paths,
                                        provider="musicbrainz",
                                        provider_payload=picked if isinstance(picked, dict) else {},
                                        local_context=search_local_context,
                                    )
                                    if strict_ok:
                                        set_cached_mb_info(picked['id'], picked)
                                        logging.info("[MusicBrainz Verify] Web+AI chose candidate %s for artist=%r album=%r", letter, artist, album_norm)
                                        return (picked, True)
                                    log_mb(
                                        "Album %s – \"%s\": web+AI candidate %s rejected (%s)",
                                        artist,
                                        title_for_web or album_norm,
                                        picked.get("id") or "?",
                                        strict_reason,
                                    )
                                mbid_match = re.search(r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}", reply)
                                if mbid_match:
                                    suggested_mbid = mbid_match.group(0)
                                    try:
                                        info = _fetch_rg_details(suggested_mbid)
                                        mb_track_count = _mb_track_count_from_rg_info(info)
                                        formats = set()
                                        for release in info.get("release-list", []):
                                            for medium in release.get("medium-list", []):
                                                fmt, qty = medium.get("format"), medium.get("track-count", 1)
                                                if fmt:
                                                    formats.add(f"{qty}×{fmt}")
                                        rg_id_final = info.get("id", suggested_mbid)
                                        mb_title = str(info.get("title") or "").strip()
                                        mb_artists = _extract_mb_artist_names(info)
                                        mb_provider_payload = {
                                            "id": rg_id_final,
                                            "title": mb_title,
                                            "mb_artist_names": mb_artists,
                                            "year": _mb_extract_year(info.get("first-release-date") or ""),
                                        }
                                        strict_ok, strict_reason = _strict_identity_match_details(
                                            local_artist=artist,
                                            local_title=title_for_web or album_norm,
                                            candidate_artist=mb_artists,
                                            candidate_title=mb_title,
                                            local_tracks=list(tracks or []),
                                            local_tags=search_local_tags,
                                            local_paths=search_local_paths,
                                            provider="musicbrainz",
                                            provider_payload=mb_provider_payload,
                                            local_context=search_local_context,
                                        )
                                        if not strict_ok:
                                            log_mb(
                                                "Album %s – \"%s\": web+AI MBID %s rejected (%s)",
                                                artist,
                                                title_for_web or album_norm,
                                                rg_id_final,
                                                strict_reason,
                                            )
                                            raise RuntimeError("strict identity mismatch")
                                        result_dict = {
                                            "primary_type": info.get("primary-type", ""),
                                            "secondary_types": info.get("secondary-types", []),
                                            "format_summary": ", ".join(sorted(formats)),
                                            "id": rg_id_final,
                                            "track_count": mb_track_count,
                                            "cover_url": f"https://coverartarchive.org/release-group/{rg_id_final}/front",
                                            "mb_title": mb_title,
                                            "mb_artist_names": mb_artists,
                                        }
                                        set_cached_mb_info(rg_id_final, result_dict)
                                        logging.info("[MusicBrainz] Web+AI suggested MBID %s for artist=%r album=%r", rg_id_final, artist, album_norm)
                                        return (result_dict, True)
                                    except RuntimeError:
                                        pass
                                    except musicbrainzngs.WebServiceError:
                                        pass
                        except Exception as e:
                            logging.debug("[MusicBrainz] Web+AI second call failed: %s", e)
            if not matching:
                if candidates:
                    detail = ", ".join(f"{rg.get('title', '?')} ({rg.get('id', '')})" for rg in candidates[:10])
                    if len(candidates) > 10:
                        detail += f" ... and {len(candidates) - 10} more"
                    logging.debug(
                        "[MusicBrainz Search] artist=%r release=%r: AI said NONE or failed, no track-count match: %s",
                        artist, album_norm, detail,
                    )
                return (None, False)

        if not matching:
            if candidates:
                detail = ", ".join(f"{rg.get('title', '?')} ({rg.get('id', '')})" for rg in candidates[:10])
                if len(candidates) > 10:
                    detail += f" ... and {len(candidates) - 10} more"
                logging.debug(
                    "[MusicBrainz Search] artist=%r release=%r: %d candidate(s) but none matched (track count or fetch failed): %s",
                    artist, album_norm, len(candidates), detail,
                )
            return (None, False)
        if len(matching) == 1:
            set_cached_mb_info(matching[0][1]['id'], matching[0][1])
            log_mb(
                "Album %s – \"%s\": accepted MB candidate %s (strict identity ok; single track-count match)",
                artist,
                title_raw or album_norm,
                matching[0][1].get("id") or "?",
            )
            return (matching[0][1], False)

        if len(matching) >= 2:
            # Try deterministic provider evidence (no LLM cost) before asking AI.
            title_for_fetch = (title_raw or album_norm or "").strip()
            extra_sources: List[dict] = []
            if title_for_fetch:
                try:
                    extra_sources = list((_provider_fallbacks() or {}).get("extra_sources") or [])
                except Exception:
                    extra_sources = []
            if extra_sources:
                votes = [0] * len(matching)
                for i, (rg, info) in enumerate(matching[:12]):
                    cand_title = str(info.get("mb_title") or rg.get("title") or "").strip()
                    cand_artists = info.get("mb_artist_names") or _extract_mb_artist_names(rg)
                    for s in extra_sources:
                        src_title = str(s.get("title") or s.get("album") or "").strip()
                        src_artist = str(s.get("artist") or s.get("artist_name") or "").strip()
                        if not src_title or not src_artist:
                            continue
                        ok, _reason = _strict_identity_match_details(
                            local_artist=src_artist,
                            local_title=src_title,
                            candidate_artist=cand_artists,
                            candidate_title=cand_title,
                        )
                        if ok:
                            votes[i] += 1
                max_votes = max(votes) if votes else 0
                if max_votes > 0 and votes.count(max_votes) == 1:
                    best_idx = votes.index(max_votes)
                    picked = matching[best_idx][1]
                    set_cached_mb_info(picked['id'], picked)
                    log_mb(
                        "Album %s – \"%s\": accepted MB candidate %s (provider strict match; skipped AI)",
                        artist,
                        title_raw or album_norm,
                        picked.get("id") or "?",
                    )
                    return (picked, False)

            # Still ambiguous: ask AI to pick among matching titles only (cheap).
            if use_ai_for_mb_match and getattr(_runtime_module(), "ai_provider_ready", False):
                letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                choices: list[str] = []
                for i, m in enumerate(matching[:10]):
                    rg, info = m
                    mid = str(info.get("id") or rg.get("id") or "").strip()
                    choices.append(f"{letters[i]}: {rg.get('title', 'Unknown')} (id={mid})")
                prompt = (
                    f"Artist: {artist}. Album: {title_raw or album_norm}. "
                    "Which MusicBrainz release-group is the same release? "
                    "Reply with only the letter (A/B/...) or NONE. Optionally end with (confidence: N).\n"
                    + "\n".join(choices)
                )
                if extra_sources:
                    other_lines = []
                    for s in extra_sources:
                        src = s.get("source", "?")
                        t = s.get("title") or s.get("album") or "?"
                        a = s.get("artist") or s.get("artist_name") or "?"
                        other_lines.append(f"  {src}: {t!r} (artist: {a!r})")
                    prompt += "\n\nOther sources:\n" + "\n".join(other_lines)
                try:
                    provider = getattr(_runtime_module(), "AI_PROVIDER", "openai")
                    model = getattr(_runtime_module(), "RESOLVED_MODEL", None) or getattr(_runtime_module(), "OPENAI_MODEL", "gpt-4o-mini")
                    tiebreak_timeout = float(
                        getattr(
                            _runtime_module(),
                            "MB_AI_TIEBREAK_TIMEOUT_SEC",
                            MB_AI_TIEBREAK_TIMEOUT_SEC,
                        )
                        or MB_AI_TIEBREAK_TIMEOUT_SEC
                    )
                    logging.info(
                        "[MusicBrainz Search] AI tie-break start (candidates=%d, timeout=%.1fs)",
                        len(matching),
                        tiebreak_timeout,
                    )
                    reply = _call_ai_provider_bounded(
                        provider=provider,
                        model=model,
                        system_msg="Reply with a single letter (A/B/...) or NONE. Optionally end with (confidence: N).",
                        user_msg=prompt,
                        max_tokens=30,
                        analysis_type="mb_candidate_tiebreak",
                        timeout_sec=tiebreak_timeout,
                        log_prefix="[MusicBrainz Search]",
                    )
                    reply_clean, ai_confidence = parse_ai_confidence((reply or "").strip())
                    if ai_confidence is not None:
                        logging.info("[MusicBrainz Search] AI pick among matching candidates confidence: %d", ai_confidence)
                    letter = (reply_clean or "").strip().upper()[:1]
                    idx = letters.find(letter)
                    if 0 <= idx < len(matching):
                        picked = matching[idx][1]
                        # Optional vision check (OpenAI only): local cover vs CAA.
                        # Vision cover comparison is expensive and can reject correct matches; keep it off by default.
                        use_vision = False
                        if use_vision:
                            try:
                                folder_path = Path(album_folder) if not isinstance(album_folder, Path) else album_folder
                                local_data_uri = _get_local_cover_data_uri_for_vision(folder_path)
                                mb_cover_url = picked.get("cover_url")
                                if local_data_uri and mb_cover_url:
                                    sys_msg = "You reply with exactly Yes or No. If No, add in parentheses the reason. Optionally end with (confidence: N)."
                                    user_msg = (
                                        "Image 1 is the local album cover. Image 2 is the MusicBrainz Cover Art Archive cover. "
                                        "Do they represent the same album cover? Reply: Yes or No."
                                    )
                                    resp = call_ai_provider_vision(
                                        provider,
                                        model,
                                        sys_msg,
                                        user_msg,
                                        image_urls=[mb_cover_url],
                                        image_base64=[{"type": "image_url", "image_url": {"url": local_data_uri}}],
                                        max_tokens=20,
                                        analysis_type="cover_vision_verify",
                                    )
                                    verdict_clean, _vision_conf = parse_ai_confidence((resp or "").strip())
                                    verdict = (verdict_clean or "").strip().upper()
                                    if verdict and "NO" in verdict:
                                        log_cov(
                                            "Cover mismatch: rejecting AI-chosen MB match for artist=%r album=%r based on vision",
                                            artist,
                                            album_norm or title_raw or "",
                                        )
                                        return (None, False)
                            except Exception as e:
                                logging.debug("[MusicBrainz Vision] Vision check failed: %s", e)

                        set_cached_mb_info(picked['id'], picked)
                        log_mb(
                            "Album %s – \"%s\": accepted MB candidate %s (strict identity ok; AI tie-break among matches)",
                            artist,
                            title_raw or album_norm,
                            picked.get("id") or "?",
                        )
                        return (picked, True)
                except Exception as e:
                    logging.debug("[MusicBrainz Search] AI pick failed: %s", e)

            # Ambiguous and no deterministic winner: do not guess.
            log_mb(
                "Album %s – \"%s\": multiple MusicBrainz candidates remain (strict identity ok) but no reliable disambiguation; leaving unmatched",
                artist,
                title_raw or album_norm,
            )
            return (None, False)
    except Exception as e:
        logging.debug("[MusicBrainz Search Groups] failed for '%s' / '%s': %s", artist, album_norm, e)
    return (None, False)

# Additional MusicBrainz helpers extracted from pmda.py.

def resolve_mbid_to_release_group_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return resolve_mbid_to_release_group_impl(*args, **kwargs)

def fetch_mb_release_group_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return fetch_mb_release_group_info_impl(*args, **kwargs)

def _extract_track_titles_from_mb_release_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _extract_track_titles_from_mb_release_impl(*args, **kwargs)

def _mb_track_count_from_rg_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mb_track_count_from_rg_info_impl(*args, **kwargs)

def _is_likely_live_album_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _is_likely_live_album_impl(*args, **kwargs)

def _crosscheck_tracklist_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _crosscheck_tracklist_impl(*args, **kwargs)

def _crosscheck_tracklist_perfect_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _crosscheck_tracklist_perfect_impl(*args, **kwargs)

def _prepare_mb_submission_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _prepare_mb_submission_payload_impl(*args, **kwargs)

def _search_mb_rg_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _search_mb_rg_candidates_impl(*args, **kwargs)

def _prefilter_mb_release_group_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _prefilter_mb_release_group_candidates_impl(*args, **kwargs)

def fetch_all_mb_release_groups_for_artist_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return fetch_all_mb_release_groups_for_artist_impl(*args, **kwargs)

def _build_mb_rg_index_for_artist_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_mb_rg_index_for_artist_impl(*args, **kwargs)

def _match_album_norm_to_mb_index_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _match_album_norm_to_mb_index_impl(*args, **kwargs)

def _browse_mb_rg_by_artist_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _browse_mb_rg_by_artist_impl(*args, **kwargs)

def resolve_mbid_to_release_group_impl(mbid: str, tag_source: str = "", use_queue: bool = True) -> Optional[str]:
    """
    Return a MusicBrainz release-group ID for use with get_release_group_by_id / fetch_mb_release_group_info.
    - If tag_source is 'musicbrainz_releasegroupid', mbid is already a release-group ID: return it.
    - If tag_source is 'musicbrainz_releaseid' or 'musicbrainz_albumid' (or empty/unknown), mbid is a release ID:
      fetch the release, extract release-group id, return it. On API error, log and return None.
    - use_queue: if False, call API directly (required when already inside MB queue worker to avoid deadlock).
    """
    if not mbid or not mbid.strip():
        return None
    mbid = mbid.strip()
    if tag_source == "musicbrainz_releasegroupid":
        try:
            def _fetch_release_group_probe():
                return musicbrainzngs.get_release_group_by_id(mbid, includes=[])["release-group"]
            if use_queue and MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
                get_mb_queue().submit(f"rg_probe_{mbid}", _fetch_release_group_probe)
            else:
                _fetch_release_group_probe()
            return mbid
        except Exception as e:
            err_text = str(e or "").lower()
            # Some files incorrectly store a release ID in the release-group tag.
            # If MB says the "release-group" does not exist, fall through and try
            # to resolve it as a release ID instead of trusting the malformed tag.
            if "404" not in err_text and "not found" not in err_text:
                logging.debug("resolve_mbid_to_release_group: release-group probe failed for mbid=%s: %s", mbid, e)
                return mbid
    # Release ID (musicbrainz_releaseid, musicbrainz_albumid) or unknown: resolve via get_release_by_id
    # MusicBrainz API expects "release-groups" (plural) for release lookup includes
    try:
        def _fetch_release():
            return musicbrainzngs.get_release_by_id(mbid, includes=["release-groups"])["release"]
        if use_queue and MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
            rel = get_mb_queue().submit(f"rel_{mbid}", _fetch_release)
        else:
            rel = _fetch_release()
        rg = rel.get("release-group")
        if rg and isinstance(rg.get("id"), str):
            return rg["id"]
        return None
    except Exception as e:
        logging.debug("resolve_mbid_to_release_group: failed for mbid=%s tag_source=%s: %s", mbid, tag_source, e)
        return None

def fetch_mb_release_group_info_impl(mbid: str) -> tuple[dict, bool]:
    """
    Fetch primary type, secondary-types, and optional format summary from MusicBrainz release-group.
    Uses musicbrainzngs for proper rate-limiting. Only inc=releases is used (inc=media is not valid
    for release-group and causes 400); format_summary may be empty unless releases include media.
    Returns (info_dict, cache_hit) where cache_hit is True if found in cache.
    """
    # Attempt to reuse cached MusicBrainz release-group info
    cached = get_cached_mb_info(mbid)
    if cached:
        logging.debug("[MusicBrainz RG Info] using cached info for MBID %s", mbid)
        return cached, True  # True = cache hit

    # Use queue for rate-limited API call.
    # Note: for release-group, include "releases" and "artist-credits". Avoid "media" here (use release endpoint instead).
    def _fetch():
        try:
            result = musicbrainzngs.get_release_group_by_id(
                mbid,
                includes=["releases", "artist-credits"]
            )["release-group"]
            return result
        except musicbrainzngs.WebServiceError as e:
            error_msg = str(e)
            if "503" in error_msg or "rate" in error_msg.lower():
                logging.warning("[MusicBrainz] Rate limited for MBID %s, will retry after delay", mbid)
                time.sleep(1.5)
                try:
                    result = musicbrainzngs.get_release_group_by_id(mbid, includes=["releases", "artist-credits"])["release-group"]
                    return result
                except musicbrainzngs.WebServiceError as e2:
                    raise RuntimeError(f"MusicBrainz lookup failed for {mbid} after retry: {e2}") from None
            if "404" in error_msg:
                # mbid may be a release ID; resolve to release-group and retry
                resolved = resolve_mbid_to_release_group(mbid, "")
                if resolved and resolved != mbid:
                    result = musicbrainzngs.get_release_group_by_id(
                        resolved, includes=["releases", "artist-credits"]
                    )["release-group"]
                    return result
            raise RuntimeError(f"MusicBrainz lookup failed for {mbid}: {e}") from None

    try:
        if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
            result = get_mb_queue().submit(f"rg_{mbid}", _fetch)
        else:
            result = _fetch()
    except Exception as e:
        raise

    primary = result.get("primary-type", "")
    secondary = result.get("secondary-types", [])
    formats = set()

    # Each release may have multiple medium entries
    for release in result.get("releases", []):
        for medium in release.get("media", []):
            fmt = medium.get("format")
            qty = medium.get("track-count") or medium.get("position") or medium.get("discs-count") or medium.get("count")
            # Some media entries include 'track-count' and 'format'
            if fmt:
                # quantity fallback: if medium["discs"] is present
                if isinstance(medium.get("format"), str):
                    quantity = medium.get("track-count", 1)
                else:
                    quantity = 1
                formats.add(f"{quantity}×{fmt}")

    format_summary = ", ".join(sorted(formats))
    logging.debug("[MusicBrainz RG Info] raw response for MBID %s: %s", mbid, result)
    logging.debug("[MusicBrainz RG Info] parsed primary_type=%s, secondary_types=%s, format_summary=%s", primary, secondary, format_summary)
    info = {
        "id": mbid,  # Include the MBID in the info dict
        "title": result.get("title", ""),
        # Keep artist names so strict identity checks can validate MBIDs sourced from providers (e.g. Last.fm).
        "mb_artist_names": _extract_mb_artist_names(result),
        "primary_type": primary,
        "secondary_types": secondary,
        "format_summary": format_summary
    }
    # Cache the lookup result
    set_cached_mb_info(mbid, info)
    return info, False  # False = cache miss

def _extract_track_titles_from_mb_release_impl(release_response: dict) -> List[str]:
    """Extract track titles from a MusicBrainz release response (get_release_by_id with includes=['recordings'])."""
    titles: List[str] = []
    try:
        release = release_response.get("release") if isinstance(release_response.get("release"), dict) else release_response
        for medium in release.get("medium-list", []):
            for track in medium.get("track-list", []):
                rec = track.get("recording") or {}
                t = rec.get("title") if isinstance(rec, dict) else None
                if t:
                    titles.append(str(t))
    except Exception:
        pass
    return titles

def _mb_track_count_from_rg_info_impl(info: dict) -> int:
    """
    Return total track count from release-group info (release-list / medium-list / track-count).
    When the API returns no medium-list (track count 0) but we have release-list, fetch the first
    release with includes=['recordings'] to get the real count (release-group lookup often omits media).
    Tolerates both "release-list"/"medium-list" and "releases"/"media" key names (musicbrainzngs/API variants).
    """
    releases = info.get("release-list") or info.get("releases") or []
    count = 0
    for release in releases:
        media = release.get("medium-list") or release.get("media") or []
        for medium in media:
            count += int(medium.get("track-count", 0) or 0)
    if count > 0:
        return count
    if not releases:
        return 0
    first_id = releases[0].get("id")
    if not first_id:
        return 0
    try:
        rel_resp = musicbrainzngs.get_release_by_id(first_id, includes=["recordings"])
        release = rel_resp.get("release") if isinstance(rel_resp.get("release"), dict) else rel_resp
        n = 0
        for medium in release.get("medium-list") or release.get("media") or []:
            track_list = medium.get("track-list") or medium.get("tracks") or []
            if track_list:
                n += len(track_list)
            else:
                n += int(medium.get("track-count", 0) or 0)
        return n
    except Exception:
        return 0

def _is_likely_live_album_impl(folder: Optional[Path], title: Optional[str]) -> bool:
    """
    Return True if the album is likely a live recording based on folder path and title.
    Used to skip MB assignment (SKIP_MB_FOR_LIVE_ALBUMS) or only accept MB release-groups
    with secondary type Live (LIVE_ALBUMS_MB_STRICT).
    """
    combined = " ".join(
        filter(None, [str(folder) if folder else "", (title or "").strip()])
    ).lower()
    if not combined:
        return False
    live_phrases = [
        " live at ",
        " live in ",
        " (live)",
        " (other live)",
        " (concert)",
        " (bootleg)",
        " live)",
    ]
    if any(p in combined for p in live_phrases):
        return True
    if combined.rstrip().endswith(" live"):
        return True
    return False

def _crosscheck_tracklist_impl(local_titles: List[str], release_titles: List[str]) -> float:
    """
    Compare local track titles with release tracklist. Returns a score in [0, 1]:
    fraction of local titles that match at least one release title (normalized).
    Used to only assign a release when confidence is above TRACKLIST_MATCH_MIN.
    """
    if not local_titles:
        return 1.0

    def _norm(s: str) -> str:
        return (s or "").lower().strip()[:200]

    local_n = [_norm(t) for t in local_titles if (t or "").strip()]
    release_n = [_norm(t) for t in release_titles if (t or "").strip()]
    if not local_n:
        return 1.0

    matches = 0
    for ln in local_n:
        for rn in release_n:
            if ln == rn:
                matches += 1
                break
            if len(ln) > 2 and len(rn) > 2 and (ln in rn or rn in ln):
                matches += 1
                break
    return matches / len(local_n)

def _crosscheck_tracklist_perfect_impl(local_titles: List[str], release_titles: List[str]) -> float:
    """
    More conservative tracklist match: normalize titles and require exact equality (no substring).
    This is intended for "certain match" fast-paths where we want to avoid false positives.
    """
    if not local_titles:
        return 1.0

    def _norm(s: str) -> str:
        raw = (s or "").strip().lower()
        if not raw:
            return ""
        had_remaster = bool(re.search(r"\bremaster", raw))
        # Drop leading index patterns.
        raw = re.sub(r"^\s*\d+\s*[-.)]\s*", "", raw)
        raw = raw.replace("&", " and ")
        # Strip punctuation, keep letters/numbers/spaces.
        raw = re.sub(r"[^\w\s]+", " ", raw)
        raw = " ".join(raw.split()).strip()
        if not raw:
            return ""
        if had_remaster:
            tokens = raw.split()
            tokens = [t for t in tokens if not re.fullmatch(r"remaster(?:ed|ing)?", t)]
            # Common tag noise: "... 2011 Remaster" -> keep the song title, drop trailing year.
            if len(tokens) >= 2 and re.fullmatch(r"(19|20)\d{2}", tokens[-1] or ""):
                tokens = tokens[:-1]
            raw = " ".join(tokens).strip()
        return raw[:240]

    local_n = [_norm(t) for t in local_titles if (t or "").strip()]
    release_n = [_norm(t) for t in release_titles if (t or "").strip()]
    if not local_n:
        return 1.0
    release_set = {t for t in release_n if t}
    if not release_set:
        return 0.0
    matches = 0
    for ln in local_n:
        if ln and ln in release_set:
            matches += 1
    return matches / len(local_n)

def _prepare_mb_submission_payload_impl(artist: str, title: str, date: str, tracklist: List[str], source: str = "discogs") -> dict:
    """
    Build a payload suitable for preparing a MusicBrainz submission (manual or via MB edit API).
    Used when we have a high-confidence match from Discogs/Bandcamp but no MB release-group.
    Returns dict with artist, title, date, tracks (list of {position, title}), source.
    """
    tracks = [{"position": i + 1, "title": t} for i, t in enumerate(tracklist or [])]
    return {
        "artist": artist or "",
        "title": title or "",
        "date": (date or "").strip()[:10],
        "tracks": tracks,
        "source": source,
    }

def _search_mb_rg_candidates_impl(artist: str, release_query: str, strict: bool) -> List[dict]:
    """Run MusicBrainz search_release_groups; return list of release-group dicts (no details).
    release_query can be normalized title or raw title (e.g. 'Isolette') for better API match."""
    result = musicbrainzngs.search_release_groups(
        artist=artist,
        release=release_query,
        limit=50,
        strict=strict
    )
    logging.debug("[MusicBrainz Search] artist=%r release=%r strict=%s -> %d results", artist, release_query, strict, len(result.get('release-group-list', [])))
    return result.get('release-group-list', [])

def _prefilter_mb_release_group_candidates_impl(
    artist_name: str,
    album_title: str,
    candidates: List[dict],
) -> List[dict]:
    """
    Reduce obviously irrelevant MusicBrainz release-group candidates before fetching details.
    This is not a hard cap: we keep every candidate that still looks title-relevant, and we
    only skip candidates that are clearly unrelated to the requested album title.
    """
    if not candidates:
        return []
    local_title_raw = str(album_title or "").strip()
    local_title_strict = _normalize_identity_album_strict(local_title_raw)
    local_title_loose = norm_album(local_title_raw or "")
    local_artist_raw = str(artist_name or "").strip()

    exact: list[dict] = []
    contains: list[dict] = []
    fuzzy: list[dict] = []
    fallback: list[dict] = []

    for rg in candidates:
        if not isinstance(rg, dict):
            continue
        cand = dict(rg)
        cand_title = str(cand.get("title") or "").strip()
        cand_title_strict = _normalize_identity_album_strict(cand_title)
        cand_title_loose = norm_album(cand_title or "")
        title_score = float(_provider_identity_text_score(local_title_raw, cand_title))
        artist_names = _extract_mb_artist_names(cand)
        artist_score = 0.0
        for nm in artist_names:
            artist_score = max(artist_score, float(_provider_identity_text_score(local_artist_raw, nm)))
        cand["_pmda_prefilter_title_score"] = title_score
        cand["_pmda_prefilter_artist_score"] = artist_score
        cand["_pmda_prefilter_exact"] = False
        cand["_pmda_prefilter_bucket"] = "fallback"

        exact_match = bool(local_title_strict and cand_title_strict and cand_title_strict == local_title_strict)
        contains_match = bool(
            not exact_match
            and local_title_loose
            and cand_title_loose
            and (local_title_loose in cand_title_loose or cand_title_loose in local_title_loose)
        )
        fuzzy_match = bool(
            not exact_match
            and not contains_match
            and (
                title_score >= 0.78
                or (title_score >= 0.68 and artist_score >= 0.80)
            )
        )

        if exact_match:
            cand["_pmda_prefilter_exact"] = True
            cand["_pmda_prefilter_bucket"] = "exact"
            exact.append(cand)
        elif contains_match:
            cand["_pmda_prefilter_bucket"] = "contains"
            contains.append(cand)
        elif fuzzy_match:
            cand["_pmda_prefilter_bucket"] = "fuzzy"
            fuzzy.append(cand)
        else:
            fallback.append(cand)

    sort_key = lambda item: (
        1 if bool(item.get("_pmda_prefilter_exact")) else 0,
        float(item.get("_pmda_prefilter_title_score") or 0.0),
        float(item.get("_pmda_prefilter_artist_score") or 0.0),
    )
    exact.sort(key=sort_key, reverse=True)
    contains.sort(key=sort_key, reverse=True)
    fuzzy.sort(key=sort_key, reverse=True)
    fallback.sort(key=sort_key, reverse=True)

    if exact:
        # Keep exact-title candidates and a tiny amount of related variants for legitimate editions.
        return exact + contains[:2]
    if contains:
        return contains + fuzzy[:2]
    if fuzzy:
        return fuzzy
    return []

def fetch_all_mb_release_groups_for_artist_impl(artist_name: str) -> List[dict]:
    """
    Fetch all release-groups for an artist from MusicBrainz (paginated browse).
    Used to build a per-artist index so we avoid one search+browse per album.
    Returns list of dicts with 'id' and 'title'; cap at 100 pages (10k RGs) for huge artists.
    """
    if not USE_MUSICBRAINZ:
        return []

    def _do_fetch() -> List[dict]:
        try:
            search_result = musicbrainzngs.search_artists(artist=artist_name, limit=1)
            artist_list = search_result.get("artist-list", [])
            if not artist_list:
                return []
            artist_mbid = artist_list[0]["id"]
            all_rgs: List[dict] = []
            offset = 0
            limit = 100
            max_pages = 100
            time.sleep(1.0)  # Rate limit: 1 req/s (avoid 2nd request right after search_artists)
            for _ in range(max_pages):
                result = musicbrainzngs.browse_release_groups(artist=artist_mbid, limit=limit, offset=offset)
                rg_list = result.get("release-group-list", [])
                for rg in rg_list:
                    title = (rg.get("title") or "").strip()
                    if title:
                        all_rgs.append({"id": rg.get("id"), "title": title})
                if len(rg_list) < limit:
                    break
                offset += limit
                time.sleep(1.0)
            logging.debug("[MusicBrainz] fetch_all_mb_release_groups_for_artist %r -> %d RGs", artist_name, len(all_rgs))
            return all_rgs
        except Exception as e:
            logging.debug("[MusicBrainz] fetch_all_mb_release_groups_for_artist failed for %r: %s", artist_name, e)
            return []

    if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
        safe_key = re.sub(r"[^a-zA-Z0-9_-]", "_", artist_name[:60])
        return get_mb_queue().submit(f"fetch_rg_{safe_key}", _do_fetch)
    return _do_fetch()

def _build_mb_rg_index_for_artist_impl(all_rgs: List[dict]) -> dict:
    """Build norm_title -> [rg_dict, ...] for matching album_norm against pre-fetched RGs."""
    index: dict = {}
    for rg in all_rgs:
        title = rg.get("title") or ""
        if not title:
            continue
        key = norm_album(title)
        index.setdefault(key, []).append(rg)
    return index

def _match_album_norm_to_mb_index_impl(album_norm: str, index: dict) -> List[dict]:
    """Return list of RG dicts (id, title) that match album_norm: exact then substring."""
    if not index or not (album_norm or "").strip():
        return []
    album_norm = (album_norm or "").strip().lower()
    exact = index.get(album_norm)
    if exact:
        return list(exact)
    candidates = []
    for key, rgs in index.items():
        if not key:
            continue
        if album_norm in key or key in album_norm:
            candidates.extend(rgs)
    return candidates

def _browse_mb_rg_by_artist_impl(artist: str, album_norm: str) -> List[dict]:
    """Get release-group candidates by browsing artist's release groups; filter by title match."""
    if not USE_MUSICBRAINZ:
        return []
    try:
        search_result = musicbrainzngs.search_artists(artist=artist, limit=1)
        artist_list = search_result.get("artist-list", [])
        if not artist_list:
            return []
        artist_mbid = artist_list[0]["id"]
        artist_data = musicbrainzngs.get_artist_by_id(artist_mbid, includes=["release-groups"])
        rg_list = artist_data.get("artist", {}).get("release-group-list", [])
        candidates = []
        for rg in rg_list:
            title = (rg.get("title") or "").strip()
            if not title:
                continue
            rg_norm = norm_album(title)
            if rg_norm == album_norm or album_norm in rg_norm or rg_norm in album_norm:
                candidates.append(rg)
        logging.debug("[MusicBrainz Browse] artist=%s album_norm=%s -> %d title-matched release groups", artist, album_norm, len(candidates))
        return candidates
    except Exception as e:
        logging.debug("[MusicBrainz Browse] failed for '%s' / '%s': %s", artist, album_norm, e)
        return []
