"""Runtime-backed similar artist image warming jobs.

The similar-image warmer is effectful: it reads artist rows, calls external
providers, writes cached images, and invalidates the file library cache. Keep it
outside the Flask bootstrap while it still binds to the live PMDA runtime during
this migration phase.
"""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one similar-image warm job."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "_run_files_similar_images_warm_job",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def run_files_similar_images_warm_job_for_runtime(runtime: Any, **kwargs: Any) -> None:
    """Run the similar artist image warming job using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _run_files_similar_images_warm_job_impl(**kwargs)


def _run_files_similar_images_warm_job_impl(*, job_key: str, artist_norm: str, names: list[str]) -> None:
    """
    Best-effort background job: cache external images for a list of artist names.
    Intended to make "Similar artists" grids look good even when providers return placeholders.
    """
    try:
        conn = _files_pg_connect()
        if conn is None:
            return
        try:
            # Deduplicate names while preserving order.
            uniq: list[str] = []
            seen: set[str] = set()
            for n in (names or [])[:24]:
                s = str(n or "").strip()
                if not s:
                    continue
                key = _norm_artist_key(s)
                if not key or key in seen:
                    continue
                seen.add(key)
                uniq.append(s)

            if not uniq:
                return

            for sname in uniq[:12]:
                key = _norm_artist_key(sname)
                if not key:
                    continue
                entity_kind = ""
                role_hints: list[str] = []
                lookup_names: list[str] = []
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT COALESCE(entity_kind, ''), COALESCE(roles_json, '[]') FROM files_artists WHERE name_norm = %s",
                            (key,),
                        )
                        erow = cur.fetchone()
                    if erow:
                        entity_kind = str(erow[0] or "").strip()
                        role_hints = _artist_role_hints_from_roles_json(erow[1] or "[]")
                except Exception:
                    entity_kind = ""
                    role_hints = []
                try:
                    lookup_names = _artist_image_lookup_candidates(
                        sname,
                        _files_get_artist_alias_candidates(
                            conn,
                            artist_norm=key,
                            artist_name=sname,
                            limit=12,
                        ),
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                        limit=12,
                    )
                except Exception:
                    lookup_names = [sname]
                if not lookup_names:
                    lookup_names = [sname]
                classical_like_entity = _artist_entity_is_classical_like(
                    entity_kind=entity_kind,
                    role_hints=role_hints,
                )

                # Skip if local library already has a real on-disk artist image.
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT has_image FROM files_artists WHERE name_norm = %s", (key,))
                        r = cur.fetchone()
                    if r and bool(r[0]):
                        continue
                except Exception:
                    pass

                # Skip if an external cached image already exists, is fresh, and still matches the artist exactly.
                try:
                    ext = _files_get_external_artist_images(conn, [key]).get(key) or {}
                    if ext and not bool(ext.get("stale")) and str(ext.get("image_path") or "").strip():
                        ext_artist_name = " ".join(str(ext.get("artist_name") or "").split()).strip()
                        if ext_artist_name and _artist_image_exact_name_match(
                            sname,
                            ext_artist_name,
                            entity_kind=entity_kind,
                            role_hints=role_hints,
                            alias_candidates=lookup_names,
                        ):
                            continue
                        with conn.transaction():
                            _files_clear_external_artist_image_cache(
                                conn,
                                name_norm=key,
                                image_path=str(ext.get("image_path") or "").strip(),
                                clear_files_artist_row=True,
                            )
                except Exception:
                    pass

                # 0) MusicBrainz exact identity -> authoritative linked image.
                try:
                    mb_info = _fetch_musicbrainz_artist_profile_info(
                        sname,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                        alias_candidates=lookup_names,
                    ) or {}
                except Exception:
                    mb_info = {}
                mb_img = str(mb_info.get("image_url") or "").strip()
                mb_name = str(mb_info.get("matched_name") or sname).strip()
                if (
                    mb_img
                    and _artist_image_exact_name_match(
                        sname,
                        mb_name,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                        alias_candidates=lookup_names,
                    )
                ):
                    try:
                        with conn.transaction():
                            outp = _files_cache_external_artist_image(
                                conn,
                                artist_name=sname,
                                artist_norm=key,
                                provider="musicbrainz",
                                image_url=mb_img,
                                max_px=640,
                                entity_kind=entity_kind,
                                role_hints=role_hints,
                                alias_candidates=lookup_names,
                                page_title=mb_name,
                                page_summary=str(mb_info.get("bio") or mb_info.get("short_bio") or "").strip(),
                            )
                        if outp:
                            continue
                    except Exception:
                        pass

                # 0a) Exact Wikipedia page image from the already validated artist page.
                try:
                    wk_info = _fetch_wikipedia_artist_bio_best(
                        sname,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                        candidate_names=list(lookup_names or []),
                    ) or {}
                except Exception:
                    wk_info = {}
                wk_url = str(wk_info.get("image_url") or "").strip()
                wk_name = str(
                    wk_info.get("page_title")
                    or wk_info.get("matched_name")
                    or sname
                ).strip()
                if (
                    wk_url
                    and _artist_image_exact_name_match(
                        sname,
                        wk_name,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                        alias_candidates=lookup_names,
                    )
                ):
                    try:
                        with conn.transaction():
                            outp = _files_cache_external_artist_image(
                                conn,
                                artist_name=sname,
                                artist_norm=key,
                                provider="wikipedia",
                                image_url=wk_url,
                                max_px=640,
                                entity_kind=entity_kind,
                                role_hints=role_hints,
                                alias_candidates=lookup_names,
                                page_title=wk_name,
                                page_summary="\n".join(
                                    part.strip()
                                    for part in (
                                        str(wk_info.get("page_description") or "").strip(),
                                        str(wk_info.get("bio") or wk_info.get("short_bio") or "").strip(),
                                    )
                                    if part and str(part).strip()
                                ),
                            )
                        if outp:
                            continue
                    except Exception:
                        pass

                # 0b) Bandcamp exact owner/profile image.
                try:
                    bc_info = _fetch_bandcamp_artist_profile_hint(
                        sname,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                        alias_candidates=lookup_names,
                    ) or {}
                except Exception:
                    bc_info = {}
                bc_url = str(bc_info.get("image_url") or "").strip()
                bc_name = str(bc_info.get("matched_name") or sname).strip()
                if (
                    bc_url
                    and _artist_image_exact_name_match(
                        sname,
                        bc_name,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                        alias_candidates=lookup_names,
                    )
                ):
                    try:
                        with conn.transaction():
                            outp = _files_cache_external_artist_image(
                                conn,
                                artist_name=sname,
                                artist_norm=key,
                                provider="bandcamp",
                                image_url=bc_url,
                                max_px=640,
                                entity_kind=entity_kind,
                                role_hints=role_hints,
                                alias_candidates=lookup_names,
                                page_title=bc_name,
                                page_summary=str(bc_info.get("bio") or bc_info.get("short_bio") or "").strip(),
                            )
                        if outp:
                            continue
                    except Exception:
                        pass

                # 1) Last.fm artist.getInfo (best coverage when configured).
                img_url = ""
                lf_mbid = ""
                try:
                    lf = _fetch_lastfm_artist_info(sname) or {}
                    img_url = str(lf.get("image_url") or "").strip()
                    lf_mbid = str(lf.get("mbid") or "").strip()
                    lf_matched_name = str(lf.get("matched_name") or sname).strip()
                except Exception:
                    img_url = ""
                    lf_mbid = ""
                    lf_matched_name = sname

                # 1a) Fanart.tv (MBID-based) – prefer when available (often higher quality than placeholders).
                if lf_mbid:
                    try:
                        f_url = (_fetch_artist_image_fanart(lf_mbid) or "").strip()
                    except Exception:
                        f_url = ""
                    if f_url and not classical_like_entity:
                        try:
                            with conn.transaction():
                                outp = _files_cache_external_artist_image(
                                    conn,
                                    artist_name=sname,
                                    artist_norm=key,
                                    provider="fanart",
                                    image_url=f_url,
                                    max_px=640,
                                    entity_kind=entity_kind,
                                    role_hints=role_hints,
                                    alias_candidates=lookup_names,
                                )
                            if outp:
                                continue
                        except Exception:
                            pass

                # 1b) Last.fm image (fallback).
                if (
                    img_url
                    and not _is_probably_placeholder_artist_image_url(img_url)
                    and not classical_like_entity
                    and _artist_image_exact_name_match(
                        sname,
                        lf_matched_name,
                        entity_kind=entity_kind,
                        role_hints=role_hints,
                        alias_candidates=lookup_names,
                    )
                ):
                    try:
                        with conn.transaction():
                            outp = _files_cache_external_artist_image(
                                conn,
                                artist_name=sname,
                                artist_norm=key,
                                provider="lastfm",
                                image_url=img_url,
                                max_px=640,
                                entity_kind=entity_kind,
                                role_hints=role_hints,
                                alias_candidates=lookup_names,
                            )
                        if outp:
                            continue
                    except Exception:
                        pass

                # 2) TheAudioDB (name-based) – optional.
                try:
                    aurl = (
                        _fetch_artist_image_audiodb(
                            sname,
                            entity_kind=entity_kind,
                            role_hints=role_hints,
                            alias_candidates=lookup_names,
                        )
                        or ""
                    ).strip()
                except Exception:
                    aurl = ""
                if aurl and not classical_like_entity:
                    try:
                        with conn.transaction():
                            outp = _files_cache_external_artist_image(
                                conn,
                                artist_name=sname,
                                artist_norm=key,
                                provider="audiodb",
                                image_url=aurl,
                                max_px=640,
                                entity_kind=entity_kind,
                                role_hints=role_hints,
                                alias_candidates=lookup_names,
                            )
                        if outp:
                            continue
                    except Exception:
                        pass

                # 3) Discogs artist image.
                try:
                    durl = (
                        _fetch_artist_image_discogs(
                            sname,
                            entity_kind=entity_kind,
                            role_hints=role_hints,
                            alias_candidates=lookup_names,
                        )
                        or ""
                    ).strip()
                except Exception:
                    durl = ""
                if durl and not classical_like_entity:
                    try:
                        with conn.transaction():
                            outp = _files_cache_external_artist_image(
                                conn,
                                artist_name=sname,
                                artist_norm=key,
                                provider="discogs",
                                image_url=durl,
                                max_px=640,
                                entity_kind=entity_kind,
                                role_hints=role_hints,
                                alias_candidates=lookup_names,
                            )
                        if outp:
                            continue
                    except Exception:
                        pass


                time.sleep(0.12)
        finally:
            try:
                conn.close()
            except Exception:
                pass
            _files_cache_invalidate_all()
    except Exception:
        pass
    finally:
        with _files_similar_images_jobs_lock:
            _files_similar_images_jobs_active.discard(job_key)
