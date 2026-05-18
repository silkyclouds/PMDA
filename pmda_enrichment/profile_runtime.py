"""Runtime-backed profile enrichment jobs.

This module contains the effectful artist image refresh, per-artist profile
enrichment, and global profile backfill implementations extracted from
``pmda.py``. It binds the live PMDA runtime at the boundary while storage,
provider, and persistence dependencies are progressively split into services.
"""

from __future__ import annotations

from typing import Any, Optional

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one profile enrichment call."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "_files_try_artist_image_refresh",
        "_run_files_profile_enrichment_job",
        "_run_files_profile_backfill",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def files_try_artist_image_refresh_for_runtime(runtime: Any, **kwargs: Any) -> bool:
    """Refresh one artist image using the live PMDA runtime."""
    _bind_runtime(runtime)
    return bool(_files_try_artist_image_refresh_impl(**kwargs))


def run_files_profile_enrichment_job_for_runtime(runtime: Any, **kwargs: Any) -> None:
    """Run one profile enrichment job using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _run_files_profile_enrichment_job_impl(**kwargs)


def run_files_profile_backfill_for_runtime(
    runtime: Any,
    *,
    reason: str = "manual",
    sleep_sec: float = 0.30,
    cover_only: bool = False,
) -> None:
    """Run the profile backfill job using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _run_files_profile_backfill_impl(
        reason=reason,
        sleep_sec=sleep_sec,
        cover_only=cover_only,
    )


def _files_try_artist_image_refresh_impl(
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
    role_hints = list(role_hints or [])
    entity_kind = str(entity_kind or "").strip() or "artist"
    artist_name = str(artist_name or "").strip()
    artist_norm = str(artist_norm or "").strip()
    if not artist_name or not artist_norm:
        return False

    local_row = None
    ext_row: dict[str, Any] = {}
    alias_candidates: list[str] = []
    with _files_pg_connection() as conn:
        if conn is not None:
            try:
                _files_relink_external_artist_images_for_artist(
                    conn,
                    artist_name=artist_name,
                    artist_norm=artist_norm,
                    alias_candidates=alias_candidates,
                )
            except Exception:
                logging.debug("Artist image relink-by-name failed for %s", artist_name, exc_info=True)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT has_image, COALESCE(image_path, '') FROM files_artists WHERE name_norm = %s LIMIT 1",
                        (artist_norm,),
                    )
                    local_row = cur.fetchone()
            except Exception:
                local_row = None
            try:
                ext_row = _files_get_external_artist_images(conn, [artist_norm]).get(artist_norm) or {}
            except Exception:
                ext_row = {}
            try:
                alias_candidates = _files_get_artist_alias_candidates(
                    conn,
                    artist_norm=artist_norm,
                    artist_name=artist_name,
                    limit=8,
                )
            except Exception:
                alias_candidates = []
    ext_path = str(ext_row.get("image_path") or "").strip()
    ext_provider = str(ext_row.get("provider") or "").strip().lower()
    ext_url = str(ext_row.get("image_url") or "").strip()
    ext_artist_name = " ".join(str(ext_row.get("artist_name") or "").split()).strip()
    if ext_row and (not ext_path) and (not ext_url):
        try:
            with _files_pg_connection() as conn:
                if conn is not None:
                    with conn.transaction():
                        _files_clear_external_artist_image_cache(
                            conn,
                            name_norm=artist_norm,
                            image_path="",
                            clear_files_artist_row=False,
                        )
            ext_row = {}
            ext_provider = ""
            ext_artist_name = ""
        except Exception:
            pass
    ext_exact_match = bool(
        ext_artist_name
        and _artist_image_exact_name_match(
            artist_name,
            ext_artist_name,
            entity_kind=entity_kind,
            role_hints=role_hints,
            alias_candidates=alias_candidates,
        )
    )
    local_has_image = bool(local_row[0]) if local_row else False
    local_image_path = str((local_row[1] if local_row else "") or "").strip()
    local_is_mirrored_cache = _artist_image_path_is_mirrored_media_cache(local_image_path)
    prefer_provider_override = bool(
        ext_path
        and ext_exact_match
        and not _artist_cached_image_provider_is_provider_first(
            provider=ext_provider,
            image_url=ext_url,
            entity_kind=entity_kind,
            role_hints=role_hints,
        )
    )

    if ext_path and (not ext_exact_match):
        try:
            with _files_pg_connection() as conn:
                if conn is not None:
                    with conn.transaction():
                        _files_clear_external_artist_image_cache(
                            conn,
                            name_norm=artist_norm,
                            image_path=ext_path,
                            clear_files_artist_row=True,
                        )
            ext_row = {}
            ext_path = ""
            ext_url = ""
            ext_provider = ""
        except Exception:
            pass

    if local_has_image and local_image_path:
        try:
            local_path_obj = Path(local_image_path)
            if local_path_obj.exists():
                if local_is_mirrored_cache and prefer_provider_override:
                    pass
                elif ext_path and local_image_path == ext_path and not ext_exact_match:
                    pass
                else:
                    return True
        except Exception:
            pass

    if ext_path and not bool(ext_row.get("stale")):
        try:
            if _is_usable_artist_image_path(Path(ext_path)) and not _artist_external_image_requires_authoritative_refresh(
                provider=ext_provider,
                image_url=ext_url,
                entity_kind=entity_kind,
                role_hints=role_hints,
            ) and (not prefer_provider_override):
                return True
        except Exception:
            pass

    lastfm_info = dict(lastfm_info or {})
    wiki_info = dict(wiki_info or {})
    mb_identity = dict(mb_identity or {})
    if not lastfm_info and not wiki_info:
        try:
            _profile, seed_lastfm, seed_wiki, _entities = _build_artist_profile_payload(
                artist_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
            )
            lastfm_info = dict(seed_lastfm or {})
            wiki_info = dict(seed_wiki or {})
        except Exception:
            lastfm_info = {}
            wiki_info = {}

    classical_like_entity = _artist_entity_is_classical_like(
        entity_kind=entity_kind,
        role_hints=role_hints,
    )
    ensemble_like_entity = bool(
        str(entity_kind or "").strip().lower() in {"ensemble", "orchestra", "choir", "chorus"}
        or bool(set(role_hints).intersection({"orchestra", "ensemble", "choir", "chorus"}))
    )
    artist_lookup_candidates = _artist_image_lookup_candidates(
        artist_name,
        alias_candidates,
        entity_kind=entity_kind,
        role_hints=role_hints,
        limit=8,
    )
    if not artist_lookup_candidates:
        artist_lookup_candidates = [artist_name]
    bandcamp_info: dict[str, Any] = {}
    discogs_info: dict[str, Any] = {}
    try:
        if not classical_like_entity:
            bandcamp_info = _fetch_bandcamp_artist_profile_hint(
                artist_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                alias_candidates=artist_lookup_candidates,
            ) or {}
    except Exception:
        bandcamp_info = {}
    try:
        if not classical_like_entity:
            discogs_info = _fetch_discogs_artist_profile_info(
                artist_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                alias_candidates=artist_lookup_candidates,
            ) or {}
    except Exception:
        discogs_info = {}
    musicbrainz_info: dict[str, Any] = {}
    try:
        musicbrainz_info = _fetch_musicbrainz_artist_profile_info(
            artist_name,
            entity_kind=entity_kind,
            role_hints=role_hints,
            alias_candidates=artist_lookup_candidates,
            mb_identity=mb_identity,
        ) or {}
    except Exception:
        musicbrainz_info = {}

    if not mb_identity and isinstance(musicbrainz_info, dict):
        mbid_seed = str(musicbrainz_info.get("mbid") or "").strip()
        matched_name_seed = str(musicbrainz_info.get("matched_name") or "").strip()
        if mbid_seed or matched_name_seed:
            mb_identity = {
                "mbid": mbid_seed,
                "name": matched_name_seed,
            }

    candidates: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    def _append_candidate(provider: str, url: str, *, title: str = "", summary: str = "") -> None:
        clean_url = str(url or "").strip()
        prov = str(provider or "").strip().lower()
        if not clean_url or not prov or clean_url in seen_urls:
            return
        if not _artist_image_provider_allowed_for_entity(
            prov,
            entity_kind=entity_kind,
            role_hints=role_hints,
        ):
            return
        seen_urls.add(clean_url)
        candidates.append(
            {
                "provider": prov,
                "url": clean_url,
                "title": str(title or "").strip(),
                "summary": str(summary or "").strip(),
            }
        )

    lf_mbid = str(lastfm_info.get("mbid") or mb_identity.get("mbid") or "").strip()
    mb_img = str(musicbrainz_info.get("image_url") or "").strip()
    mb_matched_name = str(musicbrainz_info.get("matched_name") or mb_identity.get("name") or artist_name).strip()
    if (
        mb_img
        and _artist_image_exact_name_match(
            artist_name,
            mb_matched_name,
            entity_kind=entity_kind,
            role_hints=role_hints,
            alias_candidates=artist_lookup_candidates,
        )
    ):
        _append_candidate(
            "musicbrainz",
            mb_img,
            title=mb_matched_name,
            summary="\n".join(
                part.strip()
                for part in (
                    str(musicbrainz_info.get("bio") or "").strip(),
                    ", ".join(musicbrainz_info.get("tags") or []),
                )
                if part and str(part).strip()
            ),
        )
    wiki_img = str(wiki_info.get("image_url") or "").strip()
    wiki_title = str(
        wiki_info.get("page_title")
        or wiki_info.get("matched_name")
        or wiki_info.get("title")
        or artist_name
    ).strip()
    wiki_summary = "\n".join(
        part.strip()
        for part in (
            str(wiki_info.get("page_description") or "").strip(),
            str(wiki_info.get("bio") or wiki_info.get("short_bio") or "").strip(),
        )
        if part and str(part).strip()
    )
    if (
        wiki_img
        and _artist_image_exact_name_match(
            artist_name,
            wiki_title,
            entity_kind=entity_kind,
            role_hints=role_hints,
            alias_candidates=artist_lookup_candidates,
        )
    ):
        _append_candidate(
            "wikipedia",
            wiki_img,
            title=wiki_title,
            summary=wiki_summary,
        )
    if lf_mbid and not classical_like_entity:
        try:
            fanart_url = (_fetch_artist_image_fanart(lf_mbid) or "").strip()
        except Exception:
            fanart_url = ""
        if fanart_url:
            _append_candidate("fanart", fanart_url, title=str(mb_identity.get("name") or artist_name))

    if not classical_like_entity:
        bc_img = str(bandcamp_info.get("image_url") or "").strip()
        bc_name = str(bandcamp_info.get("matched_name") or "").strip()
        if (
            bc_img
            and bc_name
            and _artist_image_exact_name_match(
                artist_name,
                bc_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                alias_candidates=artist_lookup_candidates,
            )
        ):
            _append_candidate(
                "bandcamp",
                bc_img,
                title=bc_name,
                summary=str(bandcamp_info.get("bio") or bandcamp_info.get("short_bio") or "").strip(),
            )

    if not classical_like_entity:
        lf_img = str(lastfm_info.get("image_url") or "").strip()
        lf_matched_name = str(lastfm_info.get("matched_name") or mb_identity.get("name") or artist_name).strip()
        if (
            lf_img
            and not _is_probably_placeholder_artist_image_url(lf_img)
            and _artist_image_exact_name_match(
                artist_name,
                lf_matched_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                alias_candidates=artist_lookup_candidates,
            )
        ):
            _append_candidate(
                "lastfm",
                lf_img,
                title=lf_matched_name,
                summary=str(lastfm_info.get("bio") or lastfm_info.get("short_bio") or "").strip(),
            )

    if not classical_like_entity:
        discogs_img = str(discogs_info.get("image_url") or "").strip()
        discogs_name = str(discogs_info.get("matched_name") or artist_name).strip()
        if (
            discogs_img
            and _artist_image_exact_name_match(
                artist_name,
                discogs_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
                alias_candidates=artist_lookup_candidates,
            )
        ):
            _append_candidate(
                "discogs",
                discogs_img,
                title=discogs_name,
                summary=str(discogs_info.get("bio") or discogs_info.get("short_bio") or "").strip(),
            )

    if not classical_like_entity:
        lookup_limit = 2 if bool(fast_mode) else 4
        for fetch_provider, fetcher in (
            ("audiodb", _fetch_artist_image_audiodb),
        ):
            for lookup_name in artist_lookup_candidates[:lookup_limit]:
                try:
                    fetched = (
                        fetcher(
                            lookup_name,
                            entity_kind=entity_kind,
                            role_hints=role_hints,
                            alias_candidates=artist_lookup_candidates,
                        )
                        or ""
                    ).strip()
                except Exception:
                    fetched = ""
                if fetched:
                    _append_candidate(fetch_provider, fetched, title=str(mb_identity.get("name") or artist_name))
                    break

    preferred_order = ["bandcamp", "discogs", "lastfm", "fanart", "musicbrainz", "audiodb", "wikipedia"]
    ordered_candidates: list[dict[str, str]] = []
    for provider in preferred_order:
        ordered_candidates.extend([item for item in candidates if str(item.get("provider") or "").strip().lower() == provider])

    for item in ordered_candidates:
        try:
            with _files_pg_connection() as conn:
                if conn is None:
                    continue
                with conn.transaction():
                    outp = _files_cache_external_artist_image(
                        conn,
                        artist_name=artist_name,
                        artist_norm=artist_norm,
                        provider=str(item.get("provider") or "").strip(),
                        image_url=str(item.get("url") or "").strip(),
                        max_px=640,
                        force_replace=bool(prefer_provider_override),
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                        alias_candidates=artist_lookup_candidates,
                        page_title=str(item.get("title") or "").strip(),
                        page_summary=str(item.get("summary") or "").strip(),
                    )
            if outp:
                logging.info(
                    "[Artist Image] %s (%s): accepted %s",
                    artist_name,
                    entity_kind,
                    str(item.get("provider") or "").strip(),
                )
                return True
        except Exception:
            continue
    if local_has_image and local_image_path:
        try:
            local_path_obj = Path(local_image_path)
            if local_path_obj.exists():
                return True
        except Exception:
            pass
    if ext_path and ext_exact_match:
        try:
            ext_path_obj = Path(ext_path)
            if ext_path_obj.exists() and _is_usable_artist_image_path(ext_path_obj):
                return True
        except Exception:
            pass
    logging.info(
        "[Artist Image] %s (%s): no verified candidate%s",
        artist_name,
        entity_kind,
        " [fast-mode]" if bool(fast_mode) else "",
    )
    return False

def _run_files_profile_enrichment_job_impl(
    *,
    job_key: str,
    artist_name: str,
    artist_norm: str,
    albums: list[tuple[str, str]],
    skip_album_profiles: bool = False,
    allow_soft_profiles: Optional[bool] = None,
    fast_mode: bool = False,
    cover_only: bool = False,
    priority_mode: str = "all",
) -> None:
    started_at = time.time()
    artist_profile_refreshed = False
    artist_image_refreshed = False
    album_profiles_saved = 0
    album_profiles_targeted = 0
    allow_soft_profiles = True if allow_soft_profiles is None else bool(allow_soft_profiles)
    try:
        priority_flags = _files_profile_enrichment_priority_flags(
            priority_mode=priority_mode,
            skip_album_profiles=skip_album_profiles,
            cover_only=cover_only,
        )
        priority_mode = str(priority_flags.get("priority_mode") or "all")
        run_visual_stage = bool(priority_flags.get("run_visual_stage"))
        run_artist_profile_stage = bool(priority_flags.get("run_artist_profile_stage"))
        run_album_profile_stage = bool(priority_flags.get("run_album_profile_stage"))
        storage_scope_gate = _storage_profile_enrichment_scope_for_artist(
            artist_norm,
            albums,
        )
        if not bool(storage_scope_gate.get("allowed")):
            logging.info(
                "[STORAGE] Files profile enrichment skipped artist=%r reason=%s allowed=%s matched=%s",
                artist_name,
                str(storage_scope_gate.get("reason") or "blocked"),
                ",".join(list(storage_scope_gate.get("allowed_device_ids") or [])) or "none",
                ",".join(list(storage_scope_gate.get("matched_device_ids") or [])) or "none",
            )
            return
        logging.info(
            "[Profile Enrich] start artist=%r priority=%s fast_mode=%s skip_album_profiles=%s cover_only=%s albums=%d",
            artist_name,
            priority_mode,
            bool(fast_mode),
            bool(skip_album_profiles),
            bool(cover_only),
            len(albums or []),
        )
        fast_mode = bool(fast_mode)
        cover_only = bool(cover_only)
        role_hints: list[str] = []
        entity_kind = "artist"
        artist_id = 0
        with _files_pg_connection() as conn:
            if conn is not None:
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT id, COALESCE(entity_kind, 'artist'), COALESCE(roles_json, '[]')
                            FROM files_artists
                            WHERE name_norm = %s
                            LIMIT 1
                            """,
                            (artist_norm,),
                        )
                        entity_row = cur.fetchone()
                    if entity_row:
                        artist_id = int(entity_row[0] or 0)
                        entity_kind = str(entity_row[1] or "artist").strip() or "artist"
                        role_hints = _artist_role_hints_from_roles_json(entity_row[2] or "[]")
                except Exception:
                    role_hints = []
                    entity_kind = "artist"
                    artist_id = 0

        mb_identity: dict[str, Any] = {}
        lastfm_info = None
        wiki_info = None
        if run_visual_stage or run_artist_profile_stage:
            if artist_id > 0 and (
                _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints)
                or entity_kind in {"ensemble", "composer", "conductor"}
                or bool(set(role_hints).intersection({"orchestra", "ensemble", "choir", "chorus"}))
            ):
                try:
                    mb_identity = _musicbrainz_artist_identity_lookup(
                        artist_name,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                    )
                    mb_aliases = list(mb_identity.get("aliases") or [])
                    if mb_identity:
                        with _files_pg_connection() as conn:
                            if conn is not None:
                                with conn.transaction():
                                    _files_upsert_artist_canonical_identity(
                                        conn,
                                        artist_id=artist_id,
                                        artist_norm=artist_norm,
                                        artist_name=artist_name,
                                        canonical_name=str(mb_identity.get("name") or artist_name),
                                        canonical_mbid=str(mb_identity.get("mbid") or ""),
                                        aliases=mb_aliases,
                                        entity_kind=entity_kind,
                                        roles_json=role_hints,
                                    )
                                _files_sync_artist_aliases(conn, artist_norms=[artist_norm])
                                _files_merge_duplicate_person_artists(conn)
                except Exception:
                    mb_identity = {}

            if run_artist_profile_stage:
                existing_profile = _files_get_artist_profile_cached(artist_name, artist_norm)
                existing_source = str((existing_profile.get("source") if isinstance(existing_profile, dict) else "") or "").strip().lower()
                composite_entities = _split_artist_entities_for_profiles(artist_name)
                is_multi_credit_artist = len(composite_entities) > 1
                should_refresh_artist = (
                    _artist_profile_payload_requires_refresh(
                        existing_profile if isinstance(existing_profile, dict) else None,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                    )
                    or bool(existing_profile.get("stale"))
                    or (is_multi_credit_artist and not existing_source.startswith("composite:"))
                )

                artist_profile = None
                if should_refresh_artist:
                    artist_profile, lastfm_info, wiki_info, _profile_entities = _build_artist_profile_payload(
                        artist_name,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                    )

                    if (not fast_mode) and (not (artist_profile.get("similar") or [])) and USE_MUSICBRAINZ:
                        mbid = ""
                        mb_queries = [artist_name, *composite_entities]
                        seen_mb_queries: set[str] = set()
                        for mb_query in mb_queries:
                            q = str(mb_query or "").strip()
                            if not q:
                                continue
                            q_key = _norm_artist_key(q)
                            if not q_key or q_key in seen_mb_queries:
                                continue
                            seen_mb_queries.add(q_key)
                            try:
                                search_result = musicbrainzngs.search_artists(artist=q, limit=3)
                                artist_list = search_result.get("artist-list") or []
                                if artist_list and isinstance(artist_list[0], dict):
                                    cand_name = (artist_list[0].get("name") or "").strip()
                                    if _provider_identity_text_score(q, cand_name) >= 0.78:
                                        mbid = (artist_list[0].get("id") or "").strip()
                            except Exception:
                                mbid = ""
                            if mbid:
                                break
                        if mbid:
                            try:
                                mb_similar = get_similar_artists_mb(mbid)
                                if mb_similar:
                                    artist_profile["similar"] = mb_similar[:20]
                                    if not (artist_profile.get("source") or "").strip():
                                        artist_profile["source"] = "musicbrainz"
                            except Exception:
                                pass

                    with _files_pg_connection() as conn:
                        if conn is not None:
                            with conn.transaction():
                                _files_upsert_artist_profile(conn, artist_norm, artist_name, artist_profile)
                            artist_profile_refreshed = True

            if run_visual_stage:
                try:
                    artist_image_refreshed = bool(
                        _files_try_artist_image_refresh_impl(
                            artist_name=artist_name,
                            artist_norm=artist_norm,
                            entity_kind=entity_kind,
                            role_hints=role_hints,
                            lastfm_info=lastfm_info,
                            wiki_info=wiki_info,
                            mb_identity=mb_identity,
                            fast_mode=fast_mode,
                        )
                    ) or artist_image_refreshed
                except Exception:
                    logging.debug("Fast artist image refresh failed for %s", artist_name, exc_info=True)

        def _cover_provider_from_primary_tags(primary_tags_json: Any) -> str:
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

        def _refresh_album_cover_from_identity(
            *,
            album_id: int,
            album_title_db: str,
            metadata_source: str,
            strict_verified: bool,
            mbid: str,
            musicbrainz_release_id: str,
            discogs_release_id: str,
            lastfm_album_mbid: str,
            bandcamp_album_url: str,
            cover_path_raw: str,
            primary_tags_json: Any,
        ) -> bool:
            current_cover_provider = _cover_provider_from_primary_tags(primary_tags_json)
            cover_raw = str(cover_path_raw or "").strip()
            edition_payload = {
                "musicbrainz_id": str(mbid or musicbrainz_release_id or "").strip(),
                "musicbrainz_release_group_id": str(mbid or "").strip(),
                "musicbrainz_release_id": str(musicbrainz_release_id or "").strip(),
                "musicbrainz_albumid": str(musicbrainz_release_id or "").strip(),
                "discogs_release_id": str(discogs_release_id or "").strip(),
                "lastfm_album_mbid": str(lastfm_album_mbid or "").strip(),
                "bandcamp_album_url": str(bandcamp_album_url or "").strip(),
                "primary_metadata_source": str(metadata_source or "").strip(),
            }

            exact_provider_ids: dict[str, str] = {}
            for provider_name in ("musicbrainz", "discogs", "itunes", "deezer", "spotify", "qobuz", "bandcamp", "lastfm", "audiodb", "tidal"):
                try:
                    expected_id = _strict_expected_provider_id(provider_name, edition_payload)
                except Exception:
                    expected_id = ""
                if expected_id:
                    exact_provider_ids[provider_name] = expected_id

            cover_candidates_default = [
                str(metadata_source or "").strip(),
                "bandcamp",
                "itunes",
                "deezer",
                "spotify",
                "qobuz",
                "audiodb",
                "lastfm",
                "musicbrainz",
                "discogs",
                "tidal",
            ]

            provider_chain: list[str] = []
            for provider_name in cover_candidates_default:
                provider_norm = _normalize_identity_provider(provider_name)
                if provider_norm and provider_norm not in provider_chain:
                    provider_chain.append(provider_norm)

            exact_cover_provider_lock = ""
            metadata_provider_norm = _normalize_identity_provider(str(metadata_source or ""))
            if metadata_provider_norm in exact_provider_ids:
                exact_cover_provider_lock = metadata_provider_norm
            elif metadata_provider_norm == "musicbrainz" and str(mbid or "").strip():
                exact_cover_provider_lock = "musicbrainz"
            else:
                for provider_name in provider_chain:
                    if provider_name in exact_provider_ids:
                        exact_cover_provider_lock = provider_name
                        break

            if exact_cover_provider_lock:
                provider_chain = [exact_cover_provider_lock, *[provider_name for provider_name in provider_chain if provider_name != exact_cover_provider_lock]]
            elif exact_provider_ids:
                exact_order = [provider_name for provider_name in ("musicbrainz", "discogs", "bandcamp", "itunes", "deezer", "spotify", "qobuz", "lastfm", "audiodb", "tidal") if provider_name in exact_provider_ids]
                provider_chain = [*exact_order, *[provider_name for provider_name in provider_chain if provider_name not in exact_order]]

            current_cover_provider_norm = _normalize_identity_provider(current_cover_provider) if current_cover_provider else ""
            if current_cover_provider_norm and current_cover_provider_norm not in {"local", "unknown"} and cover_raw:
                if exact_cover_provider_lock:
                    if current_cover_provider_norm == exact_cover_provider_lock:
                        return False
                elif exact_provider_ids:
                    preferred_provider = provider_chain[0] if provider_chain else ""
                    if current_cover_provider_norm == preferred_provider:
                        return False
                else:
                    return False

            def _identity_ok(provider_name: str, payload: dict[str, Any] | None) -> bool:
                if provider_name == "musicbrainz":
                    return True
                payload_dict = payload if isinstance(payload, dict) else {}
                candidate_artist = _provider_payload_artist(provider_name, payload_dict)
                candidate_title = _provider_payload_title(provider_name, payload_dict)
                if not candidate_artist or not candidate_title:
                    return False
                ok, _reason = _strict_identity_match_details(
                    local_artist=artist_name,
                    local_title=album_title_db,
                    candidate_artist=candidate_artist,
                    candidate_title=candidate_title,
                )
                return bool(ok)

            refreshed_provider = ""
            refreshed_source_url = ""
            refreshed_cached: Optional[Path] = None

            for provider_name in provider_chain:
                try:
                    payload: dict[str, Any] | None = None
                    downloaded: tuple[bytes, str, str] | None = None
                    if provider_name == "musicbrainz":
                        mb_release_group_id = str(mbid or "").strip()
                        mb_release_id = str(musicbrainz_release_id or "").strip()
                        if not (mb_release_group_id or mb_release_id):
                            continue
                        downloaded = _download_cover_art_archive_front(
                            release_id=mb_release_id,
                            release_group_id=mb_release_group_id,
                            timeout_sec=8.0,
                        )
                        refreshed_source_url = _provider_reference_link(
                            provider="musicbrainz",
                            ref=mb_release_id or mb_release_group_id,
                            artist_name=artist_name,
                            album_title=album_title_db,
                        )
                    elif provider_name == "discogs":
                        discogs_ref = str(discogs_release_id or "").strip()
                        if discogs_ref:
                            payload = _fetch_discogs_release_by_id(discogs_ref)
                        else:
                            payload = fetch_provider_album_lookup_cached(
                                "discogs",
                                artist_name,
                                album_title_db,
                                _fetch_discogs_release,
                            )
                            if isinstance(payload, dict):
                                discogs_ref = str(payload.get("release_id") or "").strip()
                        if not isinstance(payload, dict) or not _identity_ok(provider_name, payload):
                            continue
                        cover_url = _provider_cover_url_from_payload(provider_name, payload)
                        if not cover_url:
                            continue
                        downloaded = _download_best_cover_image("Discogs", cover_url, timeout=14)
                        refreshed_source_url = _provider_reference_link(
                            provider="discogs",
                            ref=discogs_ref,
                            artist_name=artist_name,
                            album_title=album_title_db,
                        )
                    elif provider_name == "bandcamp":
                        bandcamp_hint = str(bandcamp_album_url or "").strip()
                        payload = fetch_provider_album_lookup_cached(
                            "bandcamp",
                            artist_name,
                            album_title_db,
                            lambda current_artist, current_title: _fetch_bandcamp_album_info(
                                current_artist,
                                current_title,
                                allow_web_fallback=False,
                                album_url_hint=bandcamp_hint,
                            ),
                        )
                        if not isinstance(payload, dict) or not _identity_ok(provider_name, payload):
                            continue
                        cover_url = _provider_cover_url_from_payload(provider_name, payload)
                        if not cover_url:
                            continue
                        downloaded = _download_best_cover_image(
                            "Bandcamp",
                            cover_url,
                            cover_candidates=payload.get("cover_candidates") or [],
                            headers={"User-Agent": "PMDA/1.0 (profile enrichment)"},
                            timeout=14,
                        )
                        refreshed_source_url = str(
                            payload.get("album_url")
                            or payload.get("url")
                            or bandcamp_album_url
                            or ""
                        ).strip()
                    elif provider_name == "lastfm":
                        lastfm_mbid = str(lastfm_album_mbid or "").strip()
                        if lastfm_mbid:
                            payload = _fetch_lastfm_album_info(
                                artist_name,
                                album_title_db,
                                mbid=lastfm_mbid,
                            )
                        else:
                            payload = fetch_provider_album_lookup_cached(
                                "lastfm",
                                artist_name,
                                album_title_db,
                                _fetch_lastfm_album_info,
                            )
                        if not isinstance(payload, dict) or not _identity_ok(provider_name, payload):
                            continue
                        cover_url = _provider_cover_url_from_payload(provider_name, payload)
                        if not cover_url:
                            continue
                        downloaded = _download_best_cover_image("Last.fm", cover_url, timeout=14)
                        refreshed_source_url = _provider_reference_link(
                            provider="lastfm",
                            ref=str(lastfm_album_mbid or payload.get("mbid") or "").strip(),
                            artist_name=artist_name,
                            album_title=album_title_db,
                        )
                    elif provider_name == "itunes":
                        payload = fetch_provider_album_lookup_cached(
                            "itunes",
                            artist_name,
                            album_title_db,
                            _fetch_itunes_album_info,
                        )
                        if not isinstance(payload, dict) or not _identity_ok(provider_name, payload):
                            continue
                        cover_url = _provider_cover_url_from_payload(provider_name, payload)
                        if not cover_url:
                            continue
                        downloaded = _download_best_cover_image("iTunes / Apple Music", cover_url, timeout=14)
                        refreshed_source_url = _provider_reference_link(
                            provider="itunes",
                            ref=str(payload.get("album_id") or payload.get("collection_id") or "").strip(),
                            artist_name=artist_name,
                            album_title=album_title_db,
                        )
                    elif provider_name == "deezer":
                        payload = fetch_provider_album_lookup_cached(
                            "deezer",
                            artist_name,
                            album_title_db,
                            _fetch_deezer_album_info,
                        )
                        if not isinstance(payload, dict) or not _identity_ok(provider_name, payload):
                            continue
                        cover_url = _provider_cover_url_from_payload(provider_name, payload)
                        if not cover_url:
                            continue
                        downloaded = _download_best_cover_image("Deezer", cover_url, timeout=14)
                        refreshed_source_url = _provider_reference_link(
                            provider="deezer",
                            ref=str(payload.get("album_id") or "").strip(),
                            artist_name=artist_name,
                            album_title=album_title_db,
                        )
                    elif provider_name == "spotify":
                        payload = fetch_provider_album_lookup_cached(
                            "spotify",
                            artist_name,
                            album_title_db,
                            _fetch_spotify_album_info,
                        )
                        if not isinstance(payload, dict) or not _identity_ok(provider_name, payload):
                            continue
                        cover_url = _provider_cover_url_from_payload(provider_name, payload)
                        if not cover_url:
                            continue
                        downloaded = _download_best_cover_image("Spotify", cover_url, timeout=14)
                        refreshed_source_url = _provider_reference_link(
                            provider="spotify",
                            ref=str(payload.get("album_id") or "").strip(),
                            artist_name=artist_name,
                            album_title=album_title_db,
                        )
                    elif provider_name == "qobuz":
                        payload = fetch_provider_album_lookup_cached(
                            "qobuz",
                            artist_name,
                            album_title_db,
                            _fetch_qobuz_album_info,
                        )
                        if not isinstance(payload, dict) or not _identity_ok(provider_name, payload):
                            continue
                        cover_url = _provider_cover_url_from_payload(provider_name, payload)
                        if not cover_url:
                            continue
                        downloaded = _download_best_cover_image("Qobuz", cover_url, timeout=14)
                        refreshed_source_url = _provider_reference_link(
                            provider="qobuz",
                            ref=str(payload.get("album_id") or "").strip(),
                            artist_name=artist_name,
                            album_title=album_title_db,
                        )
                    elif provider_name == "audiodb":
                        payload = fetch_provider_album_lookup_cached(
                            "audiodb",
                            artist_name,
                            album_title_db,
                            _fetch_audiodb_album_info,
                        )
                        if not isinstance(payload, dict) or not _identity_ok(provider_name, payload):
                            continue
                        cover_url = _provider_cover_url_from_payload(provider_name, payload)
                        if not cover_url:
                            continue
                        downloaded = _download_best_cover_image("TheAudioDB", cover_url, timeout=14)
                        refreshed_source_url = _provider_reference_link(
                            provider="audiodb",
                            ref=str(payload.get("album_id") or "").strip(),
                            artist_name=artist_name,
                            album_title=album_title_db,
                        )
                    elif provider_name == "tidal":
                        payload = fetch_provider_album_lookup_cached(
                            "tidal",
                            artist_name,
                            album_title_db,
                            _fetch_tidal_album_info,
                        )
                        if not isinstance(payload, dict) or not _identity_ok(provider_name, payload):
                            continue
                        cover_url = _provider_cover_url_from_payload(provider_name, payload)
                        if not cover_url:
                            continue
                        downloaded = _download_best_cover_image("TIDAL", cover_url, timeout=14)
                        refreshed_source_url = _provider_reference_link(
                            provider="tidal",
                            ref=str(payload.get("album_id") or "").strip(),
                            artist_name=artist_name,
                            album_title=album_title_db,
                        )
                    if not downloaded:
                        continue
                    raw, mime, used_url = downloaded
                    cached = _ensure_cached_image_from_bytes(
                        raw,
                        mime,
                        kind="album",
                        cache_key_hint=f"profile-cover-refresh:{album_id}:{provider_name}:{used_url}",
                        max_px=_MEDIA_CACHE_MASTER_PX,
                    )
                    if not cached or (not cached.exists()) or (not cached.is_file()):
                        continue
                    wrote = False
                    with _files_pg_connection() as write_conn:
                        if write_conn is not None:
                            with write_conn.transaction():
                                with write_conn.cursor() as cur:
                                    cur.execute(
                                        """
                                        UPDATE files_albums
                                        SET has_cover = TRUE,
                                            cover_path = %s,
                                            discogs_release_id = COALESCE(NULLIF(%s, ''), discogs_release_id),
                                            updated_at = NOW()
                                        WHERE id = %s
                                        """,
                                        (
                                            str(cached),
                                            (
                                                str(payload.get("release_id") or "").strip()
                                                if provider_name == "discogs" and isinstance(payload, dict)
                                                else ""
                                            ),
                                            int(album_id),
                                        ),
                                    )
                            wrote = True
                    if not wrote:
                        continue
                    refreshed_provider = provider_name
                    refreshed_cached = cached
                    if not refreshed_source_url:
                        refreshed_source_url = used_url
                    break
                except DiscogsRateLimited:
                    continue
                except Exception:
                    continue

            if not refreshed_provider or refreshed_cached is None:
                return False

            _record_files_match_audit_album(
                album_id=album_id,
                artist_name=str(artist_name or "").strip() or "Unknown Artist",
                album_title=str(album_title_db or "").strip() or f"Album {album_id}",
                run_kind="profile_cover_refresh",
                status="completed",
                result={
                    "summary": f"Refreshed cover from {_match_provider_label(refreshed_provider)} after identity verification.",
                    "provider_used": metadata_source or refreshed_provider,
                    "cover_saved": True,
                    "pmda_matched": bool(strict_verified or metadata_source or mbid or discogs_release_id or lastfm_album_mbid or bandcamp_album_url),
                    "pmda_cover": True,
                    "pmda_artist_image": False,
                    "pmda_complete": False,
                    "pmda_match_provider": metadata_source or None,
                    "pmda_cover_provider": refreshed_provider,
                    "pmda_artist_provider": None,
                    "strict_match_verified": bool(strict_verified),
                    "strict_match_provider": metadata_source or None,
                    "strict_reject_reason": "",
                    "strict_tracklist_score": 1.0 if strict_verified else 0.0,
                },
                steps=[
                    f"Album identity verified for {artist_name} / {album_title_db}",
                    f"Replaced local/unknown cover with provider cover from {_match_provider_label(refreshed_provider)}",
                    f"Cached path: {str(refreshed_cached)}",
                    f"Source: {refreshed_source_url}",
                ],
            )
            _files_cache_invalidate_all()
            logging.info(
                "Files profile enrichment refreshed album cover album_id=%s provider=%s artist=%s album=%s",
                int(album_id),
                refreshed_provider,
                str(artist_name or "").strip() or "Unknown Artist",
                str(album_title_db or "").strip() or "Unknown Album",
            )
            return True

        if run_visual_stage or run_album_profile_stage:
            album_pairs = [(str(title or ""), str(norm or "")) for title, norm in albums if str(norm or "").strip()]
            if album_pairs:
                norm_profile_flags: dict[str, bool] = {}
                norm_cover_flags: dict[str, bool] = {}
                norm_strict_flags: dict[str, bool] = {}
                album_state_by_norm: dict[str, dict[str, Any]] = {}
                existing: dict[str, Any] = {}
                warmed_label_norms: set[str] = set()
                with _files_pg_connection() as conn:
                    if conn is not None:
                        with conn.cursor() as cur:
                            placeholders = ",".join(["%s"] * len(album_pairs))
                            norms = [norm for _, norm in album_pairs]
                            cur.execute(
                                f"""
                                SELECT
                                  alb.id,
                                  COALESCE(alb.title, ''),
                                  alb.title_norm,
                                  alb.strict_match_verified,
                                  COALESCE(alb.metadata_source, ''),
                                  COALESCE(alb.musicbrainz_release_group_id, ''),
                                  COALESCE(alb.musicbrainz_release_id, ''),
                                  COALESCE(alb.discogs_release_id, ''),
                                  COALESCE(alb.lastfm_album_mbid, ''),
                                  COALESCE(alb.bandcamp_album_url, ''),
                                  COALESCE(alb.label, ''),
                                  COALESCE(alb.cover_path, ''),
                                  COALESCE(alb.primary_tags_json, '{{}}')
                                FROM files_albums alb
                                JOIN files_artists ar ON ar.id = alb.artist_id
                                WHERE ar.name_norm = %s
                                  AND alb.title_norm IN ({placeholders})
                                """,
                                [artist_norm, *norms],
                            )
                            for (
                                album_id,
                                album_title_db,
                                raw_norm,
                                strict_verified,
                                metadata_source,
                                mbid,
                                musicbrainz_release_id,
                                discogs_release_id,
                                lastfm_album_mbid,
                                bandcamp_album_url,
                                label_name,
                                cover_path_raw,
                                primary_tags_json,
                            ) in cur.fetchall():
                                norm_key = str(raw_norm or "").strip()
                                if not norm_key:
                                    continue
                                strict_ok = bool(strict_verified)
                                can_fetch_profile = _files_album_profile_fetch_allowed(
                                    strict_verified=strict_ok,
                                    metadata_source=str(metadata_source or "").strip(),
                                    mbid=str(mbid or "").strip(),
                                    discogs_release_id=str(discogs_release_id or "").strip(),
                                    lastfm_album_mbid=str(lastfm_album_mbid or "").strip(),
                                    bandcamp_album_url=str(bandcamp_album_url or "").strip(),
                                )
                                fetch_strength = _files_album_profile_fetch_strength(
                                    strict_verified=strict_ok,
                                    metadata_source=str(metadata_source or "").strip(),
                                    mbid=str(mbid or "").strip(),
                                    discogs_release_id=str(discogs_release_id or "").strip(),
                                    lastfm_album_mbid=str(lastfm_album_mbid or "").strip(),
                                    bandcamp_album_url=str(bandcamp_album_url or "").strip(),
                                )
                                can_refresh_cover = _files_album_cover_refresh_allowed(
                                    strict_verified=strict_ok,
                                    metadata_source=str(metadata_source or "").strip(),
                                    mbid=str(mbid or "").strip(),
                                    discogs_release_id=str(discogs_release_id or "").strip(),
                                    lastfm_album_mbid=str(lastfm_album_mbid or "").strip(),
                                    bandcamp_album_url=str(bandcamp_album_url or "").strip(),
                                )
                                album_state_by_norm[norm_key] = {
                                    "album_id": int(album_id or 0),
                                    "album_title": str(album_title_db or "").strip(),
                                    "strict_verified": strict_ok,
                                    "metadata_source": str(metadata_source or "").strip(),
                                    "mbid": str(mbid or "").strip(),
                                    "musicbrainz_release_id": str(musicbrainz_release_id or "").strip(),
                                    "discogs_release_id": str(discogs_release_id or "").strip(),
                                    "lastfm_album_mbid": str(lastfm_album_mbid or "").strip(),
                                    "bandcamp_album_url": str(bandcamp_album_url or "").strip(),
                                    "label": str(label_name or "").strip(),
                                    "cover_path_raw": str(cover_path_raw or "").strip(),
                                    "primary_tags_json": primary_tags_json,
                                    "fetch_strength": int(fetch_strength),
                                }
                                if can_refresh_cover:
                                    norm_cover_flags[norm_key] = True
                                if fetch_strength <= 0:
                                    if not can_refresh_cover:
                                        continue
                                elif (not strict_ok) and (not allow_soft_profiles):
                                    if not can_refresh_cover:
                                        continue
                                elif can_fetch_profile:
                                    norm_profile_flags[norm_key] = True
                                    if strict_ok:
                                        norm_strict_flags[norm_key] = True
                                    if not strict_ok:
                                        logging.info(
                                            "[Profile Enrich] allow soft album profile artist=%s album=%s provider=%s strict=%s",
                                            str(artist_name or "").strip() or "Unknown Artist",
                                            str(album_title_db or "").strip() or "Unknown Album",
                                            str(metadata_source or "").strip() or "unknown",
                                            bool(strict_ok),
                                        )
                                elif not can_refresh_cover and any(
                                    (
                                        str(metadata_source or "").strip(),
                                        str(mbid or "").strip(),
                                        str(discogs_release_id or "").strip(),
                                        str(lastfm_album_mbid or "").strip(),
                                        str(bandcamp_album_url or "").strip(),
                                    )
                                ):
                                    logging.info(
                                        "[Profile Enrich] skip album profile artist=%s album=%s provider=%s reason=no_identity_hint",
                                        str(artist_name or "").strip() or "Unknown Artist",
                                        str(album_title_db or "").strip() or "Unknown Album",
                                        str(metadata_source or "").strip() or "unknown",
                                    )
                            cur.execute(
                                f"""
                                SELECT title_norm, updated_at
                                FROM files_album_profiles
                                WHERE artist_norm = %s AND title_norm IN ({placeholders})
                                """,
                                [artist_norm, *norms],
                            )
                            existing = {str(r[0] or ""): r[1] for r in cur.fetchall()}

                if run_visual_stage:
                    for _title, norm in album_pairs:
                        state = album_state_by_norm.get(norm)
                        if not state or not norm_cover_flags.get(norm):
                            continue
                        try:
                            _refresh_album_cover_from_identity(
                                album_id=int(state.get("album_id") or 0),
                                album_title_db=str(state.get("album_title") or _title or "").strip(),
                                metadata_source=str(state.get("metadata_source") or "").strip(),
                                strict_verified=bool(state.get("strict_verified")),
                                mbid=str(state.get("mbid") or "").strip(),
                                musicbrainz_release_id=str(state.get("musicbrainz_release_id") or "").strip(),
                                discogs_release_id=str(state.get("discogs_release_id") or "").strip(),
                                lastfm_album_mbid=str(state.get("lastfm_album_mbid") or "").strip(),
                                bandcamp_album_url=str(state.get("bandcamp_album_url") or "").strip(),
                                cover_path_raw=str(state.get("cover_path_raw") or "").strip(),
                                primary_tags_json=state.get("primary_tags_json"),
                            )
                        except Exception:
                            logging.debug(
                                "Files profile enrichment cover refresh failed artist=%s album=%s",
                                str(artist_name or "").strip() or "Unknown Artist",
                                str(state.get("album_title") or _title or "").strip() or "Unknown Album",
                                exc_info=True,
                            )

                if run_album_profile_stage:
                    to_fetch: list[tuple[str, str]] = []
                    for title, norm in album_pairs:
                        if not norm_profile_flags.get(norm):
                            continue
                        if norm not in existing or _is_profile_stale(existing.get(norm)):
                            to_fetch.append((title, norm))
                    to_fetch.sort(
                        key=lambda item: (
                            -int((album_state_by_norm.get(item[1]) or {}).get("fetch_strength") or 0),
                            0 if bool((album_state_by_norm.get(item[1]) or {}).get("strict_verified")) else 1,
                            str((album_state_by_norm.get(item[1]) or {}).get("album_title") or item[0] or "").lower(),
                        )
                    )
                    batch_web_profiles_by_norm: dict[str, dict[str, str]] = {}
                    for title, norm in to_fetch[:120]:
                        state = album_state_by_norm.get(norm) or {}
                        review_artist = str(artist_name or "").strip()
                        review_title = str(title or "").strip()
                        if state:
                            try:
                                review_artist, review_title, _review_identity_provider = _resolve_album_review_identity_from_provider_hints(
                                    review_artist,
                                    review_title,
                                    metadata_source=str(state.get("metadata_source") or "").strip(),
                                    mbid=str(state.get("mbid") or "").strip(),
                                    discogs_release_id=str(state.get("discogs_release_id") or "").strip(),
                                    lastfm_album_mbid=str(state.get("lastfm_album_mbid") or "").strip(),
                                    bandcamp_album_url=str(state.get("bandcamp_album_url") or "").strip(),
                                )
                            except Exception:
                                review_artist = str(artist_name or "").strip()
                                review_title = str(title or "").strip()
                        profile = _fetch_best_album_profile(
                            review_artist,
                            review_title,
                            allow_web_ai=False,
                            allow_short_title_fallback=bool(norm_strict_flags.get(norm, False)),
                            precomputed_web_profile=batch_web_profiles_by_norm.get(norm),
                            metadata_source=str(state.get("metadata_source") or "").strip(),
                            mbid=str(state.get("mbid") or "").strip(),
                            discogs_release_id=str(state.get("discogs_release_id") or "").strip(),
                            lastfm_album_mbid=str(state.get("lastfm_album_mbid") or "").strip(),
                            bandcamp_album_url=str(state.get("bandcamp_album_url") or "").strip(),
                            strict_match_verified=bool(norm_strict_flags.get(norm, False)),
                        ) or {}
                        if not isinstance(profile, dict):
                            continue
                        with _files_pg_connection() as write_conn:
                            if write_conn is None:
                                continue
                            with write_conn.transaction():
                                if _album_profile_has_payload(profile):
                                    _files_upsert_album_profile(write_conn, artist_norm, norm, title, profile)
                                    album_profiles_saved += 1
                                state = album_state_by_norm.get(norm) or {}
                                label_name = str(state.get("label") or "").strip()
                                label_norm = _norm_label_key(label_name)
                                bandcamp_album_url = str(state.get("bandcamp_album_url") or "").strip()
                                if label_norm and bandcamp_album_url and label_norm not in warmed_label_norms:
                                    if _files_prewarm_label_logo_from_bandcamp(
                                        write_conn,
                                        label_name=label_name,
                                        artist_name=str(artist_name or "").strip(),
                                        album_title=str(state.get("album_title") or title or "").strip(),
                                        bandcamp_album_url=bandcamp_album_url,
                                    ):
                                        warmed_label_norms.add(label_norm)
                    album_profiles_targeted = max(album_profiles_targeted, len(to_fetch))

        _files_cache_invalidate_all()
    except Exception as e:
        logging.debug("Artist profile enrichment failed for %s: %s", artist_name, e)
    finally:
        logging.info(
            "[Profile Enrich] done artist=%r priority=%s elapsed=%.2fs artist_profile=%s artist_image=%s album_profiles=%d/%d",
            artist_name,
            priority_mode,
            max(0.0, time.time() - started_at),
            artist_profile_refreshed,
            artist_image_refreshed,
            int(album_profiles_saved or 0),
            int(album_profiles_targeted or 0),
        )
        with _files_profile_jobs_lock:
            _files_profile_jobs_active.discard(job_key)
            _files_profile_jobs_last_ts[str(artist_norm or "").strip()] = time.time()

def _run_files_profile_backfill_impl(*, reason: str = "manual", sleep_sec: float = 0.30, cover_only: bool = False) -> None:
    """
    Background job: progressively enrich all artists with canonical bios/images and
    album reviews after the main scan has already published the library.
    This is intentionally throttled to avoid provider rate limits.
    """
    sleep_sec = max(0.0, min(2.0, float(sleep_sec or 0.0)))
    storage_scope = _storage_profile_backfill_scope()
    storage_scope_active = bool(storage_scope.get("enabled") and storage_scope.get("scan_active"))
    storage_scope_signature = _storage_profile_backfill_scope_signature(storage_scope)
    storage_allowed_device_ids = set(storage_scope_signature)
    storage_plan_entries = list(storage_scope.get("plan_entries") or [])
    restart_for_scope_change = False
    total_stage_items = 0
    global_index = 0
    stop_requested = False
    pending: dict[str, int] = {}
    if storage_scope_active:
        scope_labels = list(storage_scope.get("allowed_device_labels") or [])
        scope_summary = ", ".join([str(label or "").strip() for label in scope_labels if str(label or "").strip()]) or ", ".join(storage_scope_signature)
        logging.info(
            "[STORAGE] Files profile backfill scoped to device(s): %s (scan active, max_active=%d).",
            scope_summary or "none",
            int(storage_scope.get("max_active_devices") or 1),
        )
    try:
        conn = _files_pg_connect()
        if conn is None:
            return
        try:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            to_regclass('public.files_artists'),
                            to_regclass('public.files_albums'),
                            to_regclass('public.files_artist_album_links')
                        """
                    )
                    row = cur.fetchone() or ()
                if len(row) < 3 or any(not value for value in row[:3]):
                    logging.debug("Skipping files profile backfill: files_* tables are not ready yet")
                    return
            except Exception:
                logging.debug("Skipping files profile backfill: failed to probe files_* tables", exc_info=True)
                return
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            DELETE FROM files_external_artist_images
                            WHERE COALESCE(image_path, '') = ''
                              AND COALESCE(image_url, '') = ''
                            """
                        )
                        empty_rows_deleted = int(cur.rowcount or 0)
                    _files_purge_orphan_mirrored_artist_images(conn, limit=5000)
                if empty_rows_deleted > 0:
                    logging.info(
                        "[Artist Image] purged %d empty external artist image row(s) before profile backfill",
                        empty_rows_deleted,
                    )
            except Exception:
                logging.debug("Artist image orphan purge failed before profile backfill", exc_info=True)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        a.id,
                        a.name,
                        a.name_norm,
                        COALESCE(a.entity_kind, 'artist'),
                        COALESCE(a.roles_json, '[]'),
                        COALESCE(prof.source, ''),
                        COALESCE(prof.bio, ''),
                        COALESCE(prof.short_bio, ''),
                        COALESCE(ext.image_path, ''),
                        COALESCE(ext.provider, ''),
                        COALESCE(ext.image_url, ''),
                        COALESCE(a.has_image, FALSE),
                        COALESCE(a.album_count, 0),
                        COALESCE(SUM(CASE WHEN COALESCE(alb.strict_match_verified, FALSE) THEN 1 ELSE 0 END), 0) AS strict_album_count,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN COALESCE(alb.strict_match_verified, FALSE) THEN 0
                                    WHEN COALESCE(alb.metadata_source, '') IN ('musicbrainz', 'discogs', 'lastfm', 'bandcamp') THEN 1
                                    WHEN COALESCE(alb.musicbrainz_release_group_id, '') <> '' THEN 1
                                    WHEN COALESCE(alb.discogs_release_id, '') <> '' THEN 1
                                    WHEN COALESCE(alb.lastfm_album_mbid, '') <> '' THEN 1
                                    WHEN COALESCE(alb.bandcamp_album_url, '') <> '' THEN 1
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS hinted_album_count
                    FROM files_artists a
                    LEFT JOIN files_artist_profiles prof ON prof.name_norm = a.name_norm
                    LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                    LEFT JOIN files_artist_album_links link ON link.artist_id = a.id
                    LEFT JOIN files_albums alb ON alb.id = link.album_id
                    GROUP BY
                        a.id,
                        a.name,
                        a.name_norm,
                        a.entity_kind,
                        a.roles_json,
                        prof.source,
                        prof.bio,
                        prof.short_bio,
                        ext.image_path,
                        ext.provider,
                        ext.image_url,
                        a.has_image,
                        a.album_count
                    ORDER BY a.name ASC
                    """
                )
                artist_rows = cur.fetchall()
                artists_scored: list[tuple[int, int, int, int, str, int]] = []
                for row in artist_rows:
                    artist_id = int(row[0] or 0)
                    artist_name = str(row[1] or "").strip()
                    artist_norm = str(row[2] or "").strip()
                    entity_kind = str(row[3] or "artist").strip() or "artist"
                    roles_json = str(row[4] or "[]")
                    profile_source = str(row[5] or "").strip()
                    profile_bio = str(row[6] or "").strip()
                    profile_short = str(row[7] or "").strip()
                    ext_image_path = str(row[8] or "").strip()
                    ext_provider = str(row[9] or "").strip().lower()
                    ext_image_url = str(row[10] or "").strip()
                    has_image = bool(row[11])
                    album_count = int(row[12] or 0)
                    strict_album_count = int(row[13] or 0)
                    hinted_album_count = int(row[14] or 0)
                    if not artist_id or not artist_name or not artist_norm:
                        continue
                    role_hints = _artist_role_hints_from_roles_json(roles_json)
                    profile_payload = {
                        "source": profile_source,
                        "bio": profile_bio,
                        "short_bio": profile_short,
                    }
                    needs_profile = _artist_profile_payload_requires_refresh(
                        profile_payload,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                    )
                    needs_image = (not has_image and not ext_image_path) or _artist_external_image_requires_authoritative_refresh(
                        provider=ext_provider,
                        image_url=ext_image_url,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                    )
                    priority = 0 if (needs_profile or needs_image) else 1
                    artists_scored.append(
                        (
                            priority,
                            -strict_album_count,
                            -hinted_album_count,
                            -album_count,
                            artist_name.lower(),
                            artist_id,
                        )
                    )
                artist_order = [
                    artist_id
                    for _priority, _neg_strict_count, _neg_hinted_count, _neg_album_count, _sort_name, artist_id in sorted(artists_scored)
                ]
                artist_map = {
                    int(row[0] or 0): (int(row[0] or 0), str(row[1] or "").strip(), str(row[2] or "").strip())
                    for row in artist_rows
                    if int(row[0] or 0) > 0 and str(row[1] or "").strip() and str(row[2] or "").strip()
                }
                artists = [artist_map[artist_id] for artist_id in artist_order if artist_id in artist_map]
                cur.execute(
                    """
                    SELECT
                        link.artist_id,
                        COALESCE(alb.title, ''),
                        COALESCE(alb.title_norm, ''),
                        COALESCE(alb.folder_path, '')
                    FROM files_artist_album_links link
                    JOIN files_albums alb ON alb.id = link.album_id
                    ORDER BY link.artist_id ASC, alb.title ASC
                    """
                )
                album_rows = cur.fetchall()
        finally:
            conn.close()

        albums_by_artist_id: dict[int, list[tuple[str, str]]] = defaultdict(list)
        seen_pairs_by_artist_id: dict[int, set[str]] = defaultdict(set)
        allowed_artist_ids: set[int] = set()
        for artist_id, album_title, album_norm, folder_path_raw in album_rows:
            aid = int(artist_id or 0)
            title_txt = str(album_title or "").strip()
            norm_txt = str(album_norm or "").strip()
            if aid <= 0 or not title_txt or not norm_txt:
                continue
            if storage_scope_active:
                entry = _storage_plan_entry_for_canonical_path(folder_path_raw, storage_plan_entries)
                device_id = str((entry or {}).get("storage_device_id") or "").strip()
                if not device_id or device_id not in storage_allowed_device_ids:
                    continue
                allowed_artist_ids.add(aid)
            if norm_txt in seen_pairs_by_artist_id[aid]:
                continue
            seen_pairs_by_artist_id[aid].add(norm_txt)
            albums_by_artist_id[aid].append((title_txt, norm_txt))
        if storage_scope_active:
            artists = [artist_row for artist_row in artists if int(artist_row[0] or 0) in allowed_artist_ids]

        stage_specs = _files_profile_backfill_stage_specs(cover_only=cover_only)
        stage_runs: list[tuple[str, str, list[tuple[int, str, str]]]] = []
        for stage_key, stage_label in stage_specs:
            stage_artists = list(artists)
            if stage_key == "p2":
                stage_artists = [
                    artist_row
                    for artist_row in stage_artists
                    if albums_by_artist_id.get(int(artist_row[0] or 0))
                ]
            stage_runs.append((stage_key, stage_label, stage_artists))
        total_stage_items = int(sum(len(stage_artists) for _stage_key, _stage_label, stage_artists in stage_runs))

        pending = _files_profile_backfill_pending_work()
        with _files_profile_backfill_lock:
            _files_profile_backfill_state["running"] = True
            _files_profile_backfill_state["reason"] = str(reason or "manual")
            _files_profile_backfill_state["started_at"] = int(time.time())
            _files_profile_backfill_state["finished_at"] = 0
            _files_profile_backfill_state["cover_only"] = bool(cover_only)
            _files_profile_backfill_state["current"] = 0
            _files_profile_backfill_state["total"] = int(total_stage_items)
            _files_profile_backfill_state["current_artist"] = ""
            _files_profile_backfill_state["errors"] = 0
            _files_profile_backfill_state["pending_artist_profiles"] = int(pending.get("pending_artist_profiles") or 0)
            _files_profile_backfill_state["pending_album_profiles"] = int(pending.get("pending_album_profiles") or 0)
            _files_profile_backfill_state["eligible_album_profiles"] = int(pending.get("eligible_album_profiles") or 0)
            _files_profile_backfill_state["pending_album_covers"] = int(pending.get("pending_album_covers") or 0)
            _files_profile_backfill_state["last_probe_at"] = int(time.time())
            _files_profile_backfill_state["phase"] = ""
            _files_profile_backfill_state["phase_label"] = ""
            _files_profile_backfill_state["phase_index"] = 0
            _files_profile_backfill_state["phase_count"] = int(len(stage_runs))
            _files_profile_backfill_state["phase_current"] = 0
            _files_profile_backfill_state["phase_total"] = 0
            _files_profile_backfill_state["storage_scope_enabled"] = bool(storage_scope_active)
            _files_profile_backfill_state["storage_scope_mode"] = str(storage_scope.get("mode") or "")
            _files_profile_backfill_state["storage_scope_devices"] = list(storage_scope.get("allowed_device_ids") or [])
        _pipeline_job_update(
            "profile_backfill",
            status="running",
            phase="starting",
            current=0,
            total=int(total_stage_items),
            current_item="",
            message="Starting files profile backfill",
            run_id=str(reason or "manual"),
            meta={
                "reason": str(reason or "manual"),
                "cover_only": bool(cover_only),
                "storage_scope_enabled": bool(storage_scope_active),
                "storage_devices": list(storage_scope.get("allowed_device_ids") or []),
            },
        )

        global_index = 0
        stop_requested = False
        for stage_index, (stage_key, stage_label, stage_artists) in enumerate(stage_runs, start=1):
            stage_total = int(len(stage_artists))
            with _files_profile_backfill_lock:
                if not bool(_files_profile_backfill_state.get("running")):
                    stop_requested = True
                    break
                _files_profile_backfill_state["phase"] = str(stage_key or "")
                _files_profile_backfill_state["phase_label"] = str(stage_label or "")
                _files_profile_backfill_state["phase_index"] = int(stage_index)
                _files_profile_backfill_state["phase_count"] = int(len(stage_runs))
                _files_profile_backfill_state["phase_current"] = 0
                _files_profile_backfill_state["phase_total"] = int(stage_total)
                _files_profile_backfill_state["current_artist"] = ""
            _pipeline_job_update(
                "profile_backfill",
                status="running",
                phase=str(stage_key or ""),
                current=int(global_index),
                total=int(total_stage_items),
                current_item="",
                message=str(stage_label or "Profile backfill"),
                run_id=str(reason or "manual"),
                meta={
                    "phase_index": int(stage_index),
                    "phase_count": int(len(stage_runs)),
                    "phase_total": int(stage_total),
                    "cover_only": bool(cover_only),
                    "storage_scope_enabled": bool(storage_scope_active),
                },
            )

            for stage_pos, (artist_id, artist_name, artist_norm) in enumerate(stage_artists, start=1):
                if not artist_norm or not artist_name:
                    continue
                current_scope = _storage_profile_backfill_scope()
                current_scope_active = bool(current_scope.get("enabled") and current_scope.get("scan_active"))
                current_scope_signature = _storage_profile_backfill_scope_signature(current_scope)
                if current_scope_active:
                    if (not storage_scope_active) or current_scope_signature != storage_scope_signature:
                        logging.info(
                            "[STORAGE] Files profile backfill scope changed from %s to %s; stopping current pass to avoid waking stale disks.",
                            ", ".join(storage_scope_signature) or "none",
                            ", ".join(current_scope_signature) or "none",
                        )
                        restart_for_scope_change = bool(current_scope_signature)
                        stop_requested = True
                        break
                elif storage_scope_active:
                    logging.info(
                        "[STORAGE] Files profile backfill scope became unbounded while power-saver scan moved on; stopping current pass.",
                    )
                    restart_for_scope_change = False
                    stop_requested = True
                    break
                with _files_profile_backfill_lock:
                    if not bool(_files_profile_backfill_state.get("running")):
                        stop_requested = True
                        break
                    global_index += 1
                    _files_profile_backfill_state["current"] = int(global_index)
                    _files_profile_backfill_state["current_artist"] = artist_name
                    _files_profile_backfill_state["phase_current"] = int(stage_pos)
                if global_index == 1 or (global_index % 25) == 0 or global_index == total_stage_items:
                    _pipeline_job_update(
                        "profile_backfill",
                        status="running",
                        phase=str(stage_key or ""),
                        current=int(global_index),
                        total=int(total_stage_items),
                        current_item=str(artist_name or ""),
                        message=f"{stage_label}: {global_index:,} / {total_stage_items:,} artist tasks",
                        run_id=str(reason or "manual"),
                        meta={
                            "phase_current": int(stage_pos),
                            "phase_total": int(stage_total),
                            "cover_only": bool(cover_only),
                            "storage_scope_enabled": bool(storage_scope_active),
                        },
                    )
                # Avoid duplicate work when a user is browsing the same artist.
                with _files_profile_jobs_lock:
                    if artist_norm in _files_profile_jobs_active:
                        continue
                    _files_profile_jobs_active.add(artist_norm)
                try:
                    _run_files_profile_enrichment_job_impl(
                        job_key=artist_norm,
                        artist_name=artist_name,
                        artist_norm=artist_norm,
                        albums=list(albums_by_artist_id.get(int(artist_id or 0)) or []),
                        skip_album_profiles=False,
                        allow_soft_profiles=False,
                        fast_mode=False,
                        cover_only=bool(cover_only) and stage_key == "p0",
                        priority_mode=stage_key,
                    )
                except Exception:
                    with _files_profile_backfill_lock:
                        _files_profile_backfill_state["errors"] = int(_files_profile_backfill_state.get("errors") or 0) + 1
                if sleep_sec:
                    time.sleep(sleep_sec)
            if stop_requested:
                break
    finally:
        pending = _files_profile_backfill_pending_work()
        with _files_profile_backfill_lock:
            _files_profile_backfill_state["running"] = False
            _files_profile_backfill_state["finished_at"] = int(time.time())
            _files_profile_backfill_state["cover_only"] = False
            _files_profile_backfill_state["current_artist"] = ""
            _files_profile_backfill_state["pending_artist_profiles"] = int(pending.get("pending_artist_profiles") or 0)
            _files_profile_backfill_state["pending_album_profiles"] = int(pending.get("pending_album_profiles") or 0)
            _files_profile_backfill_state["eligible_album_profiles"] = int(pending.get("eligible_album_profiles") or 0)
            _files_profile_backfill_state["pending_album_covers"] = int(pending.get("pending_album_covers") or 0)
            _files_profile_backfill_state["last_probe_at"] = int(time.time())
            _files_profile_backfill_state["phase"] = ""
            _files_profile_backfill_state["phase_label"] = ""
            _files_profile_backfill_state["phase_index"] = 0
            _files_profile_backfill_state["phase_count"] = 0
            _files_profile_backfill_state["phase_current"] = 0
            _files_profile_backfill_state["phase_total"] = 0
            _files_profile_backfill_state["storage_scope_enabled"] = False
            _files_profile_backfill_state["storage_scope_mode"] = ""
            _files_profile_backfill_state["storage_scope_devices"] = []
        _pipeline_job_update(
            "profile_backfill",
            status="cancelled" if stop_requested else "completed",
            phase="stopped" if stop_requested else "done",
            current=int(global_index),
            total=int(total_stage_items),
            current_item="",
            message="Files profile backfill stopped" if stop_requested else "Files profile backfill complete",
            run_id=str(reason or "manual"),
            meta={
                "reason": str(reason or "manual"),
                "errors": int(_files_profile_backfill_state.get("errors") or 0),
                "pending_artist_profiles": int(pending.get("pending_artist_profiles") or 0),
                "pending_album_profiles": int(pending.get("pending_album_profiles") or 0),
                "pending_album_covers": int(pending.get("pending_album_covers") or 0),
            },
            finished=True,
        )
    if restart_for_scope_change:
        try:
            if _trigger_files_profile_backfill_async(reason="storage_scope_changed", cover_only=cover_only):
                logging.info("[STORAGE] Files profile backfill restarted for the new active disk scope.")
        except Exception:
            logging.debug("Failed to restart files profile backfill after storage scope change", exc_info=True)
