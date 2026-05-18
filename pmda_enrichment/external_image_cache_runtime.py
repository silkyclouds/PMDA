"""Runtime-bound external image cache helpers for labels and artists."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_norm_label_key',
    '_is_external_artist_image_stale',
    '_is_external_label_image_stale',
    '_files_get_external_label_images',
    '_files_upsert_external_label_image',
    '_files_cache_external_label_image',
    '_files_prewarm_label_logo_from_bandcamp',
    '_files_get_external_artist_images',
    '_files_upsert_external_artist_image',
    '_files_resolve_artist_cache_name_norm',
    '_files_clear_external_artist_image_cache',
    '_artist_image_path_is_mirrored_media_cache',
    '_artist_external_cached_image_is_valid_exact',
    '_artist_effective_image_present',
    '_files_reconcile_artist_image_cache_state',
    '_files_purge_orphan_mirrored_artist_images',
    '_files_artist_reference_folder',
    '_files_cache_external_artist_image',
    '_files_attach_similar_artist_refs',
    '_files_promote_artist_alias_cache',
    '_files_refresh_artist_media_map_from_conn',
    '_artist_entity_is_classical_like',
    '_artist_wikipedia_lang_candidates',
    '_artist_external_image_requires_authoritative_refresh',
    '_artist_external_image_requires_authoritative_refresh_sql',
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

def _norm_label_key(value: str | None) -> str:
    txt = re.sub(r"\s+", " ", str(value or "").strip()).strip().lower()
    return txt[:240]

def _is_external_artist_image_stale(updated_at) -> bool:
    ts = _dt_to_epoch(updated_at)
    if ts <= 0:
        return True
    return (time.time() - ts) > _FILES_EXTERNAL_ARTIST_IMAGE_MAX_AGE_SEC

def _is_external_label_image_stale(updated_at) -> bool:
    ts = _dt_to_epoch(updated_at)
    if ts <= 0:
        return True
    return (time.time() - ts) > _FILES_EXTERNAL_LABEL_IMAGE_MAX_AGE_SEC

def _files_get_external_label_images(conn, label_norms: list[str]) -> dict[str, dict]:
    norms = [str(n or "").strip() for n in (label_norms or []) if str(n or "").strip()]
    if not norms:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT label_norm, label_name, provider, COALESCE(image_path, ''), COALESCE(image_url, ''), updated_at
                FROM files_external_label_images
                WHERE label_norm = ANY(%s)
                """,
                (norms,),
            )
            rows = cur.fetchall()
        out: dict[str, dict] = {}
        for n, label_name, provider, image_path, image_url, updated_at in rows:
            key = str(n or "").strip()
            if not key:
                continue
            img_path = (image_path or "").strip() or None
            img_url = (image_url or "").strip() or None
            stale = _is_external_label_image_stale(updated_at)
            if img_path:
                try:
                    p = Path(img_path)
                    if (not p.exists()) or (not p.is_file()):
                        img_path = None
                        stale = True
                except Exception:
                    img_path = None
                    stale = True
            out[key] = {
                "label_name": str(label_name or "").strip(),
                "provider": str(provider or "").strip().lower(),
                "image_path": img_path,
                "image_url": img_url,
                "updated_at": int(_dt_to_epoch(updated_at)) if updated_at else 0,
                "stale": stale,
            }
        return out
    except Exception:
        return {}

def _files_upsert_external_label_image(
    conn,
    *,
    label_norm: str,
    label_name: str,
    provider: str,
    image_path: Optional[str],
    image_url: Optional[str],
) -> None:
    key = _norm_label_key(label_norm or label_name)
    if not key:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO files_external_label_images(label_norm, label_name, provider, image_path, image_url, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (label_norm) DO UPDATE SET
                label_name = EXCLUDED.label_name,
                provider = EXCLUDED.provider,
                image_path = EXCLUDED.image_path,
                image_url = EXCLUDED.image_url,
                updated_at = NOW()
            """,
            (
                key,
                str(label_name or "").strip() or str(label_norm or "").strip(),
                str(provider or "").strip().lower() or "bandcamp",
                (image_path or "").strip() if image_path else None,
                (image_url or "").strip() if image_url else None,
            ),
        )

def _files_cache_external_label_image(
    conn,
    *,
    label_name: str,
    provider: str,
    image_url: str,
    max_px: int = 640,
) -> Optional[str]:
    name = str(label_name or "").strip()
    key = _norm_label_key(name)
    img_url = str(image_url or "").strip()
    if not key or not img_url:
        return None
    existing = _files_get_external_label_images(conn, [key]).get(key) or {}
    if existing and not bool(existing.get("stale")):
        p = str(existing.get("image_path") or "").strip()
        if p:
            try:
                pp = Path(p)
                if pp.exists() and pp.is_file():
                    return str(pp)
            except Exception:
                pass
    dl = _download_best_cover_image(provider, img_url, cover_candidates=[img_url], timeout=12)
    if not dl:
        return None
    raw, mime, url_used = dl
    cached = _ensure_cached_image_from_bytes(
        raw,
        mime,
        kind="label",
        cache_key_hint=f"external-label-{key}",
        max_px=max_px,
    )
    if not cached:
        return None
    _files_upsert_external_label_image(
        conn,
        label_norm=key,
        label_name=name,
        provider=provider,
        image_path=str(cached),
        image_url=url_used,
    )
    return str(cached)

def _files_prewarm_label_logo_from_bandcamp(
    conn,
    *,
    label_name: str,
    artist_name: str,
    album_title: str,
    bandcamp_album_url: str,
) -> bool:
    label_txt = str(label_name or "").strip()
    album_url = str(bandcamp_album_url or "").strip()
    if not label_txt or not album_url:
        return False
    label_norm = _norm_label_key(label_txt)
    if not label_norm:
        return False
    try:
        cached = _files_get_external_label_images(conn, [label_norm]).get(label_norm) or {}
    except Exception:
        cached = {}
    if cached and not bool(cached.get("stale")):
        image_path = str(cached.get("image_path") or "").strip()
        if image_path:
            try:
                p = Path(image_path)
                if p.exists() and p.is_file():
                    return True
            except Exception:
                pass
    try:
        bandcamp_payload = _fetch_bandcamp_album_info(
            str(artist_name or "").strip(),
            str(album_title or "").strip(),
            allow_web_fallback=False,
            album_url_hint=album_url,
        ) or {}
    except Exception:
        bandcamp_payload = {}
    if not isinstance(bandcamp_payload, dict) or not bandcamp_payload:
        return False
    owner_name = str(
        bandcamp_payload.get("page_owner_name")
        or bandcamp_payload.get("label_name")
        or ""
    ).strip()
    if owner_name and _provider_identity_text_score(label_txt, owner_name) < 0.78:
        return False
    owner_image_url = str(bandcamp_payload.get("page_owner_image_url") or "").strip()
    if not owner_image_url:
        return False
    try:
        cached_logo = _files_cache_external_label_image(
            conn,
            label_name=label_txt,
            provider="bandcamp",
            image_url=owner_image_url,
        )
    except Exception:
        cached_logo = None
    return bool(cached_logo)

def _files_get_external_artist_images(conn, name_norms: list[str]) -> dict[str, dict]:
    """Return map name_norm -> {image_path,image_url,provider,updated_at,stale}."""
    norms = [str(n or "").strip() for n in (name_norms or []) if str(n or "").strip()]
    if not norms:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name_norm, artist_name, provider, COALESCE(image_path, ''), COALESCE(image_url, ''), updated_at
                FROM files_external_artist_images
                WHERE name_norm = ANY(%s)
                """,
                (norms,),
            )
            rows = cur.fetchall()
        out: dict[str, dict] = {}
        for n, artist_name, provider, image_path, image_url, updated_at in rows:
            key = str(n or "").strip()
            if not key:
                continue
            provider_norm = str(provider or "").strip().lower()
            img_path = (image_path or "").strip() or None
            img_url = (image_url or "").strip() or None
            stale = _is_external_artist_image_stale(updated_at)
            invalidate_path = False
            invalidate_url = False

            if provider_norm in {"web", "web_authoritative"}:
                img_path = None
                img_url = None
                stale = True
                invalidate_path = True
                invalidate_url = True

            # Never serve known placeholder images (e.g. Last.fm "music note" missing-avatar).
            try:
                if img_url and _is_probably_placeholder_artist_image_url(img_url):
                    img_path = None
                    img_url = None
                    stale = True
                    invalidate_path = True
                    invalidate_url = True
            except Exception:
                pass
            try:
                if img_url and _is_suspicious_external_artist_image_url(img_url):
                    img_path = None
                    img_url = None
                    stale = True
                    invalidate_path = True
                    invalidate_url = True
            except Exception:
                pass

            # If the cached file disappeared, treat as stale so a refresh can fix it.
            if img_path:
                try:
                    p = Path(img_path)
                    if not p.exists():
                        img_path = None
                        stale = True
                        invalidate_path = True
                    else:
                        try:
                            if not _is_usable_artist_image_path(p, min_dim=150, min_bytes=3000):
                                img_path = None
                                stale = True
                                invalidate_path = True
                        except Exception:
                            pass
                except Exception:
                    img_path = None
                    stale = True
                    invalidate_path = True
            if invalidate_path or invalidate_url:
                try:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM files_external_artist_images WHERE name_norm = %s", (key,))
                except Exception:
                    pass
            if not img_path and not img_url:
                try:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM files_external_artist_images WHERE name_norm = %s", (key,))
                except Exception:
                    pass
                continue
            out[key] = {
                "artist_name": artist_name or "",
                "provider": provider_norm,
                "image_path": img_path,
                "image_url": img_url,
                "updated_at": int(_dt_to_epoch(updated_at)) if updated_at else 0,
                "stale": stale,
            }
        return out
    except Exception:
        return {}

def _files_upsert_external_artist_image(
    conn,
    *,
    name_norm: str,
    artist_name: str,
    provider: str,
    image_path: Optional[str],
    image_url: Optional[str],
) -> None:
    key = str(name_norm or "").strip()
    if not key:
        return
    clean_image_path = (image_path or "").strip() if image_path else ""
    clean_image_url = (image_url or "").strip() if image_url else ""
    if not clean_image_path and not clean_image_url:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM files_external_artist_images WHERE name_norm = %s", (key,))
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO files_external_artist_images(name_norm, artist_name, provider, image_path, image_url, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (name_norm) DO UPDATE SET
                artist_name = EXCLUDED.artist_name,
                provider = EXCLUDED.provider,
                image_path = EXCLUDED.image_path,
                image_url = EXCLUDED.image_url,
                updated_at = NOW()
            """,
            (
                key,
                (artist_name or "").strip(),
                (provider or "").strip().lower() or "lastfm",
                clean_image_path or None,
                clean_image_url or None,
            ),
        )
        # Keep files_artists in sync so list endpoints relying on files_artists.has_image
        # show thumbnails immediately without requiring a full index rebuild.
        try:
            img_path = clean_image_path
            if img_path:
                cur.execute("SELECT COALESCE(image_path, '') FROM files_artists WHERE name_norm = %s", (key,))
                row = cur.fetchone()
                existing_raw = str((row[0] if row else "") or "").strip()
                override = False
                if not existing_raw:
                    override = True
                else:
                    try:
                        existing_path = path_for_fs_access(Path(existing_raw))
                    except Exception:
                        existing_path = Path(existing_raw)
                    try:
                        if not existing_path.exists():
                            override = True
                    except Exception:
                        override = True
                    # Never keep a FILES_ROOT-level "artist.jpg" as the canonical artist image: it's almost certainly
                    # a flat-library artifact that would be incorrectly shared by many artists.
                    try:
                        if not override and existing_path.name.lower() in {n.lower() for n in _ARTIST_IMAGE_NAMES}:
                            if _files_is_files_root_dir(existing_path.parent):
                                override = True
                    except Exception:
                        pass
                if override:
                    cur.execute(
                        """
                        UPDATE files_artists
                        SET has_image = TRUE,
                            image_path = %s,
                            updated_at = NOW()
                        WHERE name_norm = %s
                        """,
                        (img_path, key),
                    )
                else:
                    cur.execute(
                        "UPDATE files_artists SET has_image = TRUE, updated_at = NOW() WHERE name_norm = %s",
                        (key,),
                    )
        except Exception:
            pass

def _files_resolve_artist_cache_name_norm(
    conn,
    *,
    artist_name: str,
    artist_norm: str = "",
    alias_candidates: list[str] | tuple[str, ...] | None = None,
) -> str:
    base_name = " ".join(str(artist_name or "").split()).strip()
    provided_norm = str(artist_norm or "").strip()
    candidate_norms: list[str] = []
    seen: set[str] = set()

    def _push_norm(value: str, *, pre_normalized: bool = False) -> None:
        clean = " ".join(str(value or "").split()).strip()
        if not clean:
            return
        key = clean if pre_normalized else _norm_artist_key(clean)
        key = str(key or "").strip()
        if not key or key in seen:
            return
        seen.add(key)
        candidate_norms.append(key)

    _push_norm(provided_norm, pre_normalized=True)
    _push_norm(base_name)
    for alias in (alias_candidates or []):
        _push_norm(str(alias or "").strip())

    if not candidate_norms or conn is None:
        return provided_norm or (_norm_artist_key(base_name) or "")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name_norm
                FROM files_artists
                WHERE name_norm = ANY(%s)
                ORDER BY
                    CASE WHEN name_norm = %s THEN 0 ELSE 1 END,
                    id ASC
                LIMIT 1
                """,
                (candidate_norms, provided_norm or candidate_norms[0]),
            )
            row = cur.fetchone()
            if row and str(row[0] or "").strip():
                return str(row[0] or "").strip()

            cur.execute(
                """
                SELECT name_norm
                FROM files_artists
                WHERE COALESCE(canonical_name_norm, '') = ANY(%s)
                ORDER BY
                    CASE WHEN COALESCE(canonical_name_norm, '') = %s THEN 0 ELSE 1 END,
                    id ASC
                LIMIT 1
                """,
                (candidate_norms, provided_norm or candidate_norms[0]),
            )
            row = cur.fetchone()
            if row and str(row[0] or "").strip():
                return str(row[0] or "").strip()

            cur.execute(
                """
                SELECT artist_name_norm
                FROM files_artist_aliases
                WHERE alias_norm = ANY(%s)
                ORDER BY
                    CASE WHEN alias_norm = %s THEN 0 ELSE 1 END,
                    is_canonical DESC,
                    updated_at DESC NULLS LAST,
                    alias ASC
                LIMIT 1
                """,
                (candidate_norms, provided_norm or candidate_norms[0]),
            )
            row = cur.fetchone()
            if row and str(row[0] or "").strip():
                return str(row[0] or "").strip()
    except Exception:
        logging.debug("Artist image cache norm resolution failed for %s", base_name or provided_norm, exc_info=True)

    return provided_norm or (_norm_artist_key(base_name) or "")

def _files_clear_external_artist_image_cache(
    conn,
    *,
    name_norm: str,
    image_path: str = "",
    clear_files_artist_row: bool = True,
) -> None:
    key = str(name_norm or "").strip()
    if not key or conn is None:
        return
    cached_path = str(image_path or "").strip()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM files_external_artist_images WHERE name_norm = %s", (key,))
        if clear_files_artist_row:
            if cached_path:
                cur.execute(
                    """
                    UPDATE files_artists
                    SET has_image = FALSE,
                        image_path = NULL,
                        updated_at = NOW()
                    WHERE name_norm = %s
                      AND (
                          COALESCE(image_path, '') = %s
                          OR REPLACE(COALESCE(image_path, ''), '\\', '/') LIKE '%%/media_cache/artist/%%'
                      )
                    """,
                    (key, cached_path),
                )
            else:
                cur.execute(
                    """
                    UPDATE files_artists
                    SET has_image = FALSE,
                        image_path = NULL,
                        updated_at = NOW()
                    WHERE name_norm = %s
                      AND (
                          COALESCE(image_path, '') = ''
                          OR REPLACE(COALESCE(image_path, ''), '\\', '/') LIKE '%%/media_cache/artist/%%'
                      )
                    """,
                    (key,),
                )

def _artist_image_path_is_mirrored_media_cache(raw_path: str | Path | None) -> bool:
    raw = str(raw_path or "").strip()
    if not raw:
        return False
    try:
        return _is_media_cache_file(path_for_fs_access(Path(raw)), kind="artist")
    except Exception:
        try:
            return _is_media_cache_file(Path(raw), kind="artist")
        except Exception:
            return False

def _artist_external_cached_image_is_valid_exact(
    *,
    artist_name: str,
    entity_kind: str,
    role_hints: list[str] | tuple[str, ...] | None,
    alias_candidates: list[str] | tuple[str, ...] | None = None,
    ext_artist_name: str = "",
    ext_image_path: str = "",
    ext_provider: str = "",
    ext_image_url: str = "",
) -> bool:
    ext_artist = " ".join(str(ext_artist_name or "").split()).strip()
    if not ext_artist:
        return False
    if not _artist_image_provider_allowed_for_entity(
        str(ext_provider or "").strip(),
        entity_kind=entity_kind,
        role_hints=role_hints,
    ):
        return False
    if not _artist_image_exact_name_match(
        artist_name,
        ext_artist,
        entity_kind=entity_kind,
        role_hints=role_hints,
        alias_candidates=alias_candidates,
    ):
        return False
    ext_path = _existing_file_path(str(ext_image_path or "").strip())
    if not ext_path or not ext_path.is_file():
        return False
    try:
        if not _is_media_cache_file(ext_path, kind="artist"):
            return False
    except Exception:
        return False
    provider_norm = str(ext_provider or "").strip().lower()
    image_url = str(ext_image_url or "").strip()
    if _artist_external_image_requires_authoritative_refresh(
        provider=provider_norm,
        image_url=image_url,
        entity_kind=entity_kind,
        role_hints=role_hints,
    ):
        return False
    try:
        if not _is_usable_artist_image_path(ext_path, min_dim=150, min_bytes=3000):
            return False
    except Exception:
        pass
    # Revalidation of an already-cached exact provider image must not depend on the
    # CDN/file URL containing artist-name hints. The exact artist-name match was already
    # enforced when the cache row was accepted, and trusted providers regularly serve
    # opaque URLs (Bandcamp, Discogs, Last.fm, Wikimedia).
    return True

def _artist_effective_image_present(
    *,
    artist_name: str,
    entity_kind: str,
    role_hints: list[str] | tuple[str, ...] | None,
    local_image_path: str = "",
    ext_image_path: str = "",
    ext_artist_name: str = "",
    ext_provider: str = "",
    ext_image_url: str = "",
    alias_candidates: list[str] | tuple[str, ...] | None = None,
) -> bool:
    local_path = _existing_file_path(str(local_image_path or "").strip())
    if local_path and local_path.is_file():
        try:
            if not _artist_image_path_is_mirrored_media_cache(local_path):
                return True
        except Exception:
            return True
    return _artist_external_cached_image_is_valid_exact(
        artist_name=artist_name,
        entity_kind=entity_kind,
        role_hints=role_hints,
        alias_candidates=alias_candidates,
        ext_artist_name=ext_artist_name,
        ext_image_path=ext_image_path,
        ext_provider=ext_provider,
        ext_image_url=ext_image_url,
    )

def _files_reconcile_artist_image_cache_state(
    conn,
    *,
    artist_name: str,
    artist_norm: str,
    entity_kind: str,
    role_hints: list[str] | tuple[str, ...] | None,
    local_image_path: str = "",
    ext_image_path: str = "",
    ext_artist_name: str = "",
    ext_provider: str = "",
    ext_image_url: str = "",
    alias_candidates: list[str] | tuple[str, ...] | None = None,
) -> tuple[str, str, bool]:
    local_raw = str(local_image_path or "").strip()
    ext_raw = str(ext_image_path or "").strip()
    ext_valid_exact = _artist_external_cached_image_is_valid_exact(
        artist_name=artist_name,
        entity_kind=entity_kind,
        role_hints=role_hints,
        alias_candidates=alias_candidates,
        ext_artist_name=ext_artist_name,
        ext_image_path=ext_raw,
        ext_provider=ext_provider,
        ext_image_url=ext_image_url,
    )
    local_is_mirrored_cache = _artist_image_path_is_mirrored_media_cache(local_raw)

    if local_is_mirrored_cache:
        if ext_valid_exact and ext_raw:
            if not _paths_refer_to_same_file(local_raw, ext_raw):
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE files_artists
                        SET has_image = TRUE,
                            image_path = %s,
                            updated_at = NOW()
                        WHERE name_norm = %s
                        """,
                        (ext_raw, artist_norm),
                    )
                local_raw = ext_raw
        else:
            if ext_raw:
                _files_clear_external_artist_image_cache(
                    conn,
                    name_norm=artist_norm,
                    image_path=ext_raw or local_raw,
                    clear_files_artist_row=True,
                )
                ext_raw = ""
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE files_artists
                        SET has_image = FALSE,
                            image_path = NULL,
                            updated_at = NOW()
                        WHERE name_norm = %s
                        """,
                        (artist_norm,),
                    )
            local_raw = ""
    elif ext_raw and not ext_valid_exact:
        _files_clear_external_artist_image_cache(
            conn,
            name_norm=artist_norm,
            image_path=ext_raw,
            clear_files_artist_row=False,
        )
        ext_raw = ""

    return local_raw, ext_raw, ext_valid_exact

def _files_purge_orphan_mirrored_artist_images(conn, *, limit: int | None = None) -> int:
    if conn is None:
        return 0
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT
                    a.name_norm,
                    COALESCE(a.name, ''),
                    COALESCE(a.entity_kind, 'artist'),
                    COALESCE(a.roles_json, '[]'),
                    COALESCE(a.image_path, ''),
                    COALESCE(ext.image_path, ''),
                    COALESCE(ext.artist_name, ''),
                    COALESCE(ext.provider, ''),
                    COALESCE(ext.image_url, '')
                FROM files_artists a
                LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                WHERE a.has_image = TRUE
                  AND REPLACE(COALESCE(a.image_path, ''), '\\', '/') LIKE '%%/media_cache/artist/%%'
                ORDER BY a.updated_at DESC NULLS LAST, a.id ASC
            """
            if limit is not None and int(limit or 0) > 0:
                sql += " LIMIT %s"
                cur.execute(sql, (int(limit),))
            else:
                cur.execute(sql)
            rows = cur.fetchall()
    except Exception:
        return 0

    cleaned = 0
    for name_norm, artist_name, entity_kind, roles_json, local_path, ext_path, ext_artist_name, ext_provider, ext_image_url in rows:
        key = str(name_norm or "").strip()
        display_name = str(artist_name or "").strip()
        if not key or not display_name:
            continue
        role_hints = _artist_role_hints_from_roles_json(roles_json or "[]")
        try:
            before_local = str(local_path or "").strip()
            before_ext = str(ext_path or "").strip()
            new_local, new_ext, _ext_valid_exact = _files_reconcile_artist_image_cache_state(
                conn,
                artist_name=display_name,
                artist_norm=key,
                entity_kind=str(entity_kind or "artist"),
                role_hints=role_hints,
                local_image_path=before_local,
                ext_image_path=before_ext,
                ext_artist_name=str(ext_artist_name or "").strip(),
                ext_provider=str(ext_provider or "").strip(),
                ext_image_url=str(ext_image_url or "").strip(),
            )
            if str(new_local or "").strip() != before_local or str(new_ext or "").strip() != before_ext:
                cleaned += 1
        except Exception:
            continue
    if cleaned:
        logging.info("[Artist Image] purged %d orphan mirrored artist image row(s)", int(cleaned))
    return int(cleaned)

def _files_artist_reference_folder(
    conn,
    *,
    artist_norm: str,
    artist_name: str = "",
) -> Optional[Path]:
    key = str(artist_norm or "").strip() or _norm_artist_key(artist_name)
    if not key or conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(alb.folder_path, '')
                FROM files_artists a
                JOIN files_artist_album_links link ON link.artist_id = a.id
                JOIN files_albums alb ON alb.id = link.album_id
                WHERE a.name_norm = %s
                  AND COALESCE(alb.folder_path, '') <> ''
                ORDER BY link.is_primary DESC, alb.year DESC NULLS LAST, alb.id ASC
                LIMIT 1
                """,
                (key,),
            )
            row = cur.fetchone()
    except Exception:
        return None
    folder_raw = str((row[0] if row else "") or "").strip()
    if not folder_raw:
        return None
    try:
        album_folder = path_for_fs_access(Path(folder_raw))
    except Exception:
        album_folder = Path(folder_raw)
    if not album_folder.exists() or not album_folder.is_dir():
        return None
    try:
        artist_folder = _files_guess_artist_folder(album_folder, artist_name or Path(folder_raw).parent.name)
    except Exception:
        artist_folder = album_folder.parent if album_folder.parent else None
    if artist_folder and artist_folder.exists() and artist_folder.is_dir():
        return artist_folder
    return None

def _files_cache_external_artist_image(
    conn,
    *,
    artist_name: str,
    artist_norm: str = "",
    provider: str,
    image_url: str,
    max_px: int = 640,
    force_replace: bool = False,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    alias_candidates: list[str] | tuple[str, ...] | None = None,
    page_title: str = "",
    page_summary: str = "",
) -> Optional[str]:
    """
    Download + store an external artist image into the media cache and upsert the DB row.
    Returns the cached file path string, or None.
    """
    name = (artist_name or "").strip()
    if not name:
        return None
    source_key = _norm_artist_key(name)
    key = _files_resolve_artist_cache_name_norm(
        conn,
        artist_name=name,
        artist_norm=artist_norm or source_key,
        alias_candidates=alias_candidates,
    )
    if not key:
        key = source_key
    img_url = (image_url or "").strip()
    if not img_url:
        return None
    try:
        if _is_probably_placeholder_artist_image_url(img_url):
            return None
        if not _artist_image_provider_allowed_for_entity(
            provider,
            entity_kind=entity_kind,
            role_hints=role_hints,
        ):
            return None
        if _artist_external_image_requires_authoritative_refresh(
            provider=provider,
            image_url=img_url,
            entity_kind=entity_kind,
            role_hints=role_hints,
        ):
            return None
        if not _artist_image_url_looks_relevant(
            img_url,
            artist_name=name,
            entity_kind=entity_kind,
            role_hints=role_hints,
            page_title=page_title,
            page_summary=page_summary,
        ):
            return None
    except Exception:
        pass

    exact_page_title = bool(
        str(page_title or "").strip()
        and _artist_image_exact_name_match(
            name,
            str(page_title or "").strip(),
            entity_kind=entity_kind,
            role_hints=role_hints,
            alias_candidates=alias_candidates,
        )
    )
    min_image_dim = 160 if exact_page_title else 220
    min_image_bytes = 4096 if exact_page_title else 8192

    # Skip refresh if a recent cached image exists and the file is still present.
    existing_keys = [value for value in dict.fromkeys([key, source_key]) if value]
    existing_map = _files_get_external_artist_images(conn, existing_keys)
    existing = existing_map.get(key) or {}
    legacy_existing = existing_map.get(source_key) or {}
    if (not existing) and legacy_existing and key and source_key and key != source_key:
        try:
            _files_upsert_external_artist_image(
                conn,
                name_norm=key,
                artist_name=" ".join(str(legacy_existing.get("artist_name") or name).split()).strip() or name,
                provider=str(legacy_existing.get("provider") or provider or "").strip(),
                image_path=str(legacy_existing.get("image_path") or "").strip() or None,
                image_url=str(legacy_existing.get("image_url") or "").strip() or None,
            )
            _files_clear_external_artist_image_cache(
                conn,
                name_norm=source_key,
                image_path=str(legacy_existing.get("image_path") or "").strip(),
                clear_files_artist_row=False,
            )
            existing = _files_get_external_artist_images(conn, [key]).get(key) or {}
        except Exception:
            existing = legacy_existing
    existing_artist_name = " ".join(str(existing.get("artist_name") or "").split()).strip()
    existing_exact_match = bool(
        existing_artist_name
        and _artist_image_exact_name_match(
            name,
            existing_artist_name,
            entity_kind=entity_kind,
            role_hints=role_hints,
        )
    )
    if existing and (not existing_exact_match):
        try:
            _files_clear_external_artist_image_cache(
                conn,
                name_norm=key,
                image_path=str(existing.get("image_path") or "").strip(),
                clear_files_artist_row=True,
            )
        except Exception:
            pass
        existing = {}
    if existing and not bool(existing.get("stale")) and not bool(force_replace):
        p = (existing.get("image_path") or "").strip()
        if p:
            try:
                pp = Path(p)
                if pp.exists():
                    # If the cached file is tiny/low-res/placeholder-ish, force a refresh.
                    try:
                        if not _is_usable_artist_image_path(pp, min_dim=min_image_dim, min_bytes=min_image_bytes):
                            p = ""
                        else:
                            # Keep files_artists in sync even when we reuse an existing cached file.
                            try:
                                _files_upsert_external_artist_image(
                                    conn,
                                    name_norm=key,
                                    artist_name=name,
                                    provider=str(existing.get("provider") or provider),
                                    image_path=str(pp),
                                    image_url=str(existing.get("image_url") or img_url),
                                )
                            except Exception:
                                pass
                            return str(pp)
                    except Exception:
                        return str(pp)
            except Exception:
                pass

    dl = _download_best_cover_image(provider, img_url, cover_candidates=[img_url], timeout=12)
    if not dl:
        return None
    raw, mime, url_used = dl
    try:
        if _artist_external_image_requires_authoritative_refresh(
            provider=provider,
            image_url=url_used,
            entity_kind=entity_kind,
            role_hints=role_hints,
        ):
            return None
        if not _artist_image_url_looks_relevant(
            url_used,
            artist_name=name,
            entity_kind=entity_kind,
            role_hints=role_hints,
            page_title=page_title,
            page_summary=page_summary,
        ):
            return None
        if not _is_usable_artist_image_bytes(raw, min_dim=min_image_dim, min_bytes=min_image_bytes):
            return None
        reference_folder = _files_artist_reference_folder(
            conn,
            artist_norm=key,
            artist_name=name,
        )
        if reference_folder and not _is_artist_image_distinct_from_local_covers(reference_folder, raw):
            return None
    except Exception:
        # Keep permissive behavior if inspection fails unexpectedly.
        pass
    cached = _ensure_cached_image_from_bytes(
        raw,
        mime,
        kind="artist",
        cache_key_hint=f"external-artist-{key}",
        max_px=max_px,
    )
    if not cached:
        return None
    cached_path = str(cached)
    _files_upsert_external_artist_image(
        conn,
        name_norm=key,
        artist_name=name,
        provider=provider,
        image_path=cached_path,
        image_url=url_used,
    )
    return cached_path

def _files_attach_similar_artist_refs(conn, similar_artists: list, base_url: str) -> list:
    """
    Attach `artist_id` and `image_url` (prefer local, then cached external, then remote)
    to similar artist entries.
    """
    if not isinstance(similar_artists, list) or not similar_artists:
        return []
    base = (base_url or "").rstrip("/")
    # Build normalized name keys.
    norms: list[str] = []
    for entry in similar_artists:
        if not isinstance(entry, dict):
            continue
        n = _norm_artist_key(str(entry.get("name") or ""))
        if n:
            norms.append(n)
    norms = list(dict.fromkeys(norms))
    if not norms:
        return similar_artists

    local_map: dict[str, dict] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name_norm, has_image
                FROM files_artists
                WHERE name_norm = ANY(%s)
                """,
                (norms,),
            )
            for aid, nn, has_img in cur.fetchall():
                key = str(nn or "").strip()
                if not key:
                    continue
                local_map[key] = {"artist_id": int(aid or 0), "has_image": bool(has_img)}
    except Exception:
        local_map = {}

    out: list[dict] = []
    for entry in similar_artists:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "").strip()
        if not name:
            continue
        key = _norm_artist_key(name)
        patched = dict(entry)
        # Local link + image.
        local = local_map.get(key)
        if local and int(local.get("artist_id") or 0) > 0:
            patched["artist_id"] = int(local["artist_id"])
            patched["image_url"] = _artist_image_asset_url(base, int(local["artist_id"]), size=320)
        elif key:
            fallback_remote = str(patched.get("image_url") or "").strip()
            if fallback_remote:
                patched["image_url"] = fallback_remote
                patched["image_cached_url"] = f"{base}/api/library/external/artist-image/{quote(key, safe='')}?size=320&name={quote(name, safe='')}"
            else:
                patched["image_url"] = f"{base}/api/library/external/artist-image/{quote(key, safe='')}?size=320&name={quote(name, safe='')}"
        out.append(patched)
    return out

def _files_promote_artist_alias_cache(conn, artists_map: dict[str, dict[str, Any]]) -> None:
    if not artists_map:
        return
    for artist_norm, payload in artists_map.items():
        canonical_norm = str(artist_norm or "").strip()
        if not canonical_norm:
            continue
        aliases_raw = _safe_json_load((payload or {}).get("aliases_json") or "[]", fallback=[])
        alias_norms = [
            _norm_artist_key(alias)
            for alias in (aliases_raw if isinstance(aliases_raw, list) else [])
            if _norm_artist_key(alias)
        ]
        alias_norms = [value for value in alias_norms if value and value != canonical_norm]
        if not alias_norms:
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(bio, ''), COALESCE(short_bio, ''), COALESCE(tags_json, '[]'), COALESCE(similar_json, '[]'), COALESCE(source, ''), updated_at
                FROM files_artist_profiles
                WHERE name_norm = %s
                LIMIT 1
                """,
                (canonical_norm,),
            )
            current_profile_row = cur.fetchone()
            current_bio = str((current_profile_row[0] if current_profile_row else "") or "").strip()
            current_short = str((current_profile_row[1] if current_profile_row else "") or "").strip()
            needs_profile = _is_garbage_bio(current_bio) and _is_garbage_bio(current_short)
            if needs_profile:
                cur.execute(
                    """
                    SELECT name_norm, artist_name, bio, short_bio, tags_json, similar_json, source, updated_at
                    FROM files_artist_profiles
                    WHERE name_norm = ANY(%s)
                    ORDER BY updated_at DESC NULLS LAST
                    """,
                    (alias_norms,),
                )
                best_profile: Optional[tuple[Any, ...]] = None
                best_score = -1
                for row in cur.fetchall():
                    bio = str(row[2] or "").strip()
                    short_bio = str(row[3] or "").strip()
                    score = 0
                    if bio and not _is_garbage_bio(bio):
                        score += max(10, _word_count(bio))
                    if short_bio and not _is_garbage_bio(short_bio):
                        score += max(6, _word_count(short_bio))
                    if score > best_score:
                        best_profile = row
                        best_score = score
                if best_profile and best_score > 0:
                    _files_upsert_artist_profile(
                        conn,
                        canonical_norm,
                        str((payload or {}).get("name") or best_profile[1] or "").strip(),
                        {
                            "bio": str(best_profile[2] or "").strip(),
                            "short_bio": str(best_profile[3] or "").strip(),
                            "tags": _safe_json_load(best_profile[4] or "[]", fallback=[]),
                            "similar": _safe_json_load(best_profile[5] or "[]", fallback=[]),
                            "source": str(best_profile[6] or "").strip(),
                        },
                    )

            cur.execute(
                """
                SELECT COALESCE(image_path, ''), COALESCE(image_url, ''), COALESCE(provider, ''), updated_at
                FROM files_external_artist_images
                WHERE name_norm = %s
                LIMIT 1
                """,
                (canonical_norm,),
            )
            current_ext_row = cur.fetchone()
            current_ext_path = str((current_ext_row[0] if current_ext_row else "") or "").strip()
            current_has_ext = bool(current_ext_path and _existing_file_path(current_ext_path))
            if not current_has_ext:
                cur.execute(
                    """
                    SELECT name_norm, artist_name, provider, COALESCE(image_path, ''), COALESCE(image_url, ''), updated_at
                    FROM files_external_artist_images
                    WHERE name_norm = ANY(%s)
                    ORDER BY updated_at DESC NULLS LAST
                    """,
                    (alias_norms,),
                )
                for alias_row in cur.fetchall():
                    candidate_path = str(alias_row[3] or "").strip()
                    if candidate_path and _existing_file_path(candidate_path):
                        _files_upsert_external_artist_image(
                            conn,
                            name_norm=canonical_norm,
                            artist_name=str((payload or {}).get("name") or alias_row[1] or "").strip(),
                            provider=str(alias_row[2] or "").strip(),
                            image_path=candidate_path,
                            image_url=str(alias_row[4] or "").strip() or None,
                        )
                        break

def _files_refresh_artist_media_map_from_conn(conn, artists_map: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    if conn is None or not artists_map:
        return artists_map or {}
    norms = [str(norm or "").strip() for norm in artists_map.keys() if str(norm or "").strip()]
    if not norms:
        return artists_map
    try:
        artist_rows: dict[str, dict[str, Any]] = {}
        ext_rows: dict[str, str] = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    name_norm,
                    COALESCE(name, ''),
                    has_image,
                    COALESCE(image_path, ''),
                    COALESCE(canonical_name, ''),
                    COALESCE(canonical_name_norm, ''),
                    COALESCE(canonical_mbid, ''),
                    COALESCE(aliases_json, '[]')
                FROM files_artists
                WHERE name_norm = ANY(%s)
                """,
                (norms,),
            )
            for name_norm, name, has_image, image_path, canonical_name, canonical_name_norm, canonical_mbid, aliases_json in cur.fetchall():
                artist_rows[str(name_norm or "").strip()] = {
                    "name": str(name or "").strip(),
                    "has_image": bool(has_image),
                    "image_path": str(image_path or "").strip(),
                    "canonical_name": str(canonical_name or "").strip(),
                    "canonical_name_norm": str(canonical_name_norm or "").strip(),
                    "canonical_mbid": str(canonical_mbid or "").strip(),
                    "aliases_json": aliases_json or "[]",
                }
            cur.execute(
                """
                SELECT name_norm, COALESCE(image_path, '')
                FROM files_external_artist_images
                WHERE name_norm = ANY(%s)
                """,
                (norms,),
            )
            for name_norm, image_path in cur.fetchall():
                ext_rows[str(name_norm or "").strip()] = str(image_path or "").strip()
        refreshed: dict[str, dict[str, Any]] = {}
        for norm, payload in artists_map.items():
            key = str(norm or "").strip()
            current = dict(payload or {})
            row = artist_rows.get(key)
            if row:
                current["name"] = _choose_preferred_identity_display(str(current.get("name") or ""), str(row.get("name") or ""))
                current["has_image"] = bool(row.get("has_image"))
                current["image_path"] = str(row.get("image_path") or "").strip()
                current["canonical_name"] = str(row.get("canonical_name") or "").strip() or current.get("name") or ""
                current["canonical_name_norm"] = str(row.get("canonical_name_norm") or "").strip() or key
                current["canonical_mbid"] = str(row.get("canonical_mbid") or "").strip()
                current["aliases_json"] = row.get("aliases_json") or current.get("aliases_json") or "[]"
            if (not current.get("has_image")) and ext_rows.get(key):
                current["has_image"] = True
                current["image_path"] = ext_rows.get(key) or ""
            refreshed[key] = current
        return refreshed
    except Exception:
        return artists_map


_ARTIST_CLASSICAL_ENTITY_KINDS = {"composer", "conductor", "orchestra", "ensemble", "choir", "chorus"}
_ARTIST_CLASSICAL_ROLE_HINTS = {"composer", "conductor", "orchestra", "ensemble", "choir", "chorus"}
_ARTIST_WEAK_CLASSICAL_IMAGE_PROVIDERS = {"web", "discogs", "lastfm", "fanart", "audiodb", "musicbrainz"}
_ARTIST_CLASSICAL_WIKIPEDIA_LANGS = ("en", "de", "fr", "it", "es", "pl", "cs")


def _artist_entity_is_classical_like(
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> bool:
    kind = str(entity_kind or "").strip().lower()
    roles = {
        str(role or "").strip().lower()
        for role in (role_hints or [])
        if str(role or "").strip()
    }
    return bool(
        _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints)
        or kind in _ARTIST_CLASSICAL_ENTITY_KINDS
        or bool(roles.intersection(_ARTIST_CLASSICAL_ROLE_HINTS))
    )


def _artist_wikipedia_lang_candidates(
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> tuple[str, ...]:
    if _artist_entity_is_classical_like(entity_kind=entity_kind, role_hints=role_hints):
        return _ARTIST_CLASSICAL_WIKIPEDIA_LANGS
    return ("en", "fr")


def _artist_external_image_requires_authoritative_refresh(
    *,
    provider: str = "",
    image_url: str = "",
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> bool:
    provider_low = str(provider or "").strip().lower()
    url_low = str(image_url or "").strip().lower()
    try:
        if url_low and (
            _is_probably_placeholder_artist_image_url(url_low)
            or _is_suspicious_external_artist_image_url(url_low)
        ):
            return True
    except Exception:
        return True
    if provider_low in {"musicbrainz", "musicbrainz_url"} and url_low and (
        "wikimedia.org" in url_low or "wikipedia.org" in url_low or "wikidata.org" in url_low
    ):
        return True
    classical_like = _artist_entity_is_classical_like(entity_kind=entity_kind, role_hints=role_hints)
    if not classical_like:
        return False
    roles = {
        str(role or "").strip().lower()
        for role in (role_hints or [])
        if str(role or "").strip()
    }
    ensemble_like = bool(
        str(entity_kind or "").strip().lower() in {"orchestra", "ensemble", "choir", "chorus"}
        or roles.intersection({"orchestra", "ensemble", "choir", "chorus"})
    )
    if provider_low in _ARTIST_WEAK_CLASSICAL_IMAGE_PROVIDERS:
        return True
    if provider_low == "web_authoritative" and url_low and "wikimedia.org" not in url_low and "wikipedia.org" not in url_low:
        return True
    if provider_low == "musicbrainz_url":
        if ensemble_like and url_low and "i.scdn.co/image/" in url_low:
            return True
        return False
    if ensemble_like and url_low and "i.scdn.co/image/" in url_low:
        return True
    if provider_low == "wikipedia" and url_low and "wikimedia.org" not in url_low and "wikipedia.org" not in url_low:
        return True
    return False


def _artist_external_image_requires_authoritative_refresh_sql(
    artist_alias: str = "a",
    ext_alias: str = "ext",
) -> str:
    kind_expr = f"LOWER(COALESCE({artist_alias}.entity_kind, 'artist'))"
    roles_expr = f"LOWER(COALESCE({artist_alias}.roles_json::text, '[]'))"
    provider_expr = f"LOWER(COALESCE({ext_alias}.provider, ''))"
    url_expr = f"LOWER(COALESCE({ext_alias}.image_url, ''))"
    classical_expr = (
        f"({kind_expr} IN ('composer','conductor','orchestra','ensemble','choir','chorus')"
        f" OR {roles_expr} LIKE '%composer%'"
        f" OR {roles_expr} LIKE '%conductor%'"
        f" OR {roles_expr} LIKE '%orchestra%'"
        f" OR {roles_expr} LIKE '%ensemble%'"
        f" OR {roles_expr} LIKE '%choir%'"
        f" OR {roles_expr} LIKE '%chorus%')"
    )
    placeholder_expr = (
        f"({url_expr} LIKE '%2a96cbd8b46e442fc41c2b86b821562f%'"
        f" OR {url_expr} LIKE '%4128a6eb29f94943c9d206c08e625904%'"
        f" OR {url_expr} LIKE '%c6f59c1e5e7240a4c0d427abd71f3dbb%'"
        f" OR {url_expr} LIKE '%placeholder-artist%'"
        f" OR {url_expr} LIKE '%/placeholders/%'"
        f" OR {url_expr} LIKE '%default_avatar%'"
        f" OR {url_expr} LIKE '%default-avatar%'"
        f" OR {url_expr} LIKE '%noimage%'"
        f" OR {url_expr} LIKE '%no-image%'"
        f" OR {url_expr} LIKE '%blank.jpg%'"
        f" OR {url_expr} LIKE '%spacer.gif%'"
        f" OR {url_expr} LIKE '%transparent.png%'"
        f" OR ({url_expr} LIKE '%default%' AND ({url_expr} LIKE '%last.fm%' OR {url_expr} LIKE '%lastfm%'))"
        f" OR ({url_expr} LIKE '%static-images.merchbar.com%' AND {url_expr} LIKE '%/placeholders/%')"
        f" OR {url_expr} LIKE '%s0.wp.com/i/blank.jpg%')"
    )
    suspicious_expr = (
        f"({url_expr} LIKE '%soundtrack%'"
        f" OR {url_expr} LIKE '%coverartarchive%'"
        f" OR {url_expr} LIKE '%album-cover%'"
        f" OR {url_expr} LIKE '%albumcover%'"
        f" OR {url_expr} LIKE '%album_art%'"
        f" OR {url_expr} LIKE '%cover-art%'"
        f" OR {url_expr} LIKE '%record-cover%'"
        f" OR {url_expr} LIKE '%recording-cover%'"
        f" OR {url_expr} LIKE '%facebook_share%'"
        f" OR {url_expr} LIKE '%allmusic_facebook_share%'"
        f" OR {url_expr} LIKE '%sharecard%'"
        f" OR {url_expr} LIKE '%share-card%'"
        f" OR {url_expr} LIKE '%logo%'"
        f" OR {url_expr} LIKE '%signet%'"
        f" OR {url_expr} LIKE '%/album/%'"
        f" OR {url_expr} LIKE '%/release/%'"
        f" OR {url_expr} LIKE '%/cover/%'"
        f" OR {url_expr} LIKE '%/covers/%'"
        f" OR {url_expr} LIKE '%.svg%'"
        f" OR (({provider_expr} IN ('musicbrainz','musicbrainz_url'))"
        f"     AND ({url_expr} LIKE '%wikimedia.org%' OR {url_expr} LIKE '%wikipedia.org%' OR {url_expr} LIKE '%wikidata.org%'))"
        f" OR ({url_expr} LIKE '%i.scdn.co/image/%' AND {url_expr} LIKE '%ab67616d%'))"
    )
    weak_classical_expr = (
        f"({classical_expr} AND ("
        f"{provider_expr} IN ('web','discogs','lastfm','fanart','audiodb','musicbrainz')"
        f" OR ({provider_expr} = 'wikipedia' AND {url_expr} <> '' AND {url_expr} NOT LIKE '%wikimedia.org%' AND {url_expr} NOT LIKE '%wikipedia.org%')"
        f" OR ({provider_expr} = 'musicbrainz_url' AND ({kind_expr} IN ('orchestra','ensemble','choir','chorus')"
        f" OR {roles_expr} LIKE '%orchestra%'"
        f" OR {roles_expr} LIKE '%ensemble%'"
        f" OR {roles_expr} LIKE '%choir%'"
        f" OR {roles_expr} LIKE '%chorus%'))"
        f" OR (({kind_expr} IN ('orchestra','ensemble','choir','chorus')"
        f" OR {roles_expr} LIKE '%orchestra%'"
        f" OR {roles_expr} LIKE '%ensemble%'"
        f" OR {roles_expr} LIKE '%choir%'"
        f" OR {roles_expr} LIKE '%chorus%') AND {url_expr} LIKE '%i.scdn.co/image/%')"
        f" OR {suspicious_expr}"
        f"))"
    )
    sql = f"({placeholder_expr} OR {weak_classical_expr})"
    return sql.replace("%", "%%")

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}

def _norm_label_key_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _norm_label_key(*args, **kwargs)

def _is_external_artist_image_stale_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _is_external_artist_image_stale(*args, **kwargs)

def _is_external_label_image_stale_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _is_external_label_image_stale(*args, **kwargs)

def _files_get_external_label_images_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_get_external_label_images(*args, **kwargs)

def _files_upsert_external_label_image_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_upsert_external_label_image(*args, **kwargs)

def _files_cache_external_label_image_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_cache_external_label_image(*args, **kwargs)

def _files_prewarm_label_logo_from_bandcamp_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_prewarm_label_logo_from_bandcamp(*args, **kwargs)

def _files_get_external_artist_images_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_get_external_artist_images(*args, **kwargs)

def _files_upsert_external_artist_image_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_upsert_external_artist_image(*args, **kwargs)

def _files_resolve_artist_cache_name_norm_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_resolve_artist_cache_name_norm(*args, **kwargs)

def _files_clear_external_artist_image_cache_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_clear_external_artist_image_cache(*args, **kwargs)

def _artist_image_path_is_mirrored_media_cache_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_image_path_is_mirrored_media_cache(*args, **kwargs)

def _artist_external_cached_image_is_valid_exact_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_external_cached_image_is_valid_exact(*args, **kwargs)

def _artist_effective_image_present_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_effective_image_present(*args, **kwargs)

def _files_reconcile_artist_image_cache_state_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_reconcile_artist_image_cache_state(*args, **kwargs)

def _files_purge_orphan_mirrored_artist_images_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_purge_orphan_mirrored_artist_images(*args, **kwargs)

def _files_artist_reference_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_artist_reference_folder(*args, **kwargs)

def _files_cache_external_artist_image_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_cache_external_artist_image(*args, **kwargs)

def _files_attach_similar_artist_refs_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_attach_similar_artist_refs(*args, **kwargs)

def _files_promote_artist_alias_cache_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_promote_artist_alias_cache(*args, **kwargs)

def _files_refresh_artist_media_map_from_conn_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_refresh_artist_media_map_from_conn(*args, **kwargs)

def _artist_entity_is_classical_like_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_entity_is_classical_like(*args, **kwargs)

def _artist_wikipedia_lang_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_wikipedia_lang_candidates(*args, **kwargs)

def _artist_external_image_requires_authoritative_refresh_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_external_image_requires_authoritative_refresh(*args, **kwargs)

def _artist_external_image_requires_authoritative_refresh_sql_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artist_external_image_requires_authoritative_refresh_sql(*args, **kwargs)
