"""Artist maintenance helpers used by the files publication schema."""

from __future__ import annotations

import logging
from pathlib import Path


def migrate_external_artist_images_norm_keys(
    cur,
    *,
    norm_artist_key,
    path_size,
    is_probably_placeholder_artist_image_url,
    logger=None,
) -> None:
    logger = logger or logging
    """
    Keep files_external_artist_images.name_norm aligned with the current artist normalization used by
    files_artists.name_norm so joins work and cached images survive normalization upgrades.
    """
    try:
        cur.execute("SELECT COALESCE(value, '') FROM files_index_meta WHERE key = 'external_artist_images_norm' LIMIT 1")
        row = cur.fetchone()
        if row and str(row[0] or "").strip() == "strict_v2":
            return
    except Exception:
        # If meta read fails, still attempt best-effort migration.
        pass

    try:
        cur.execute(
            """
            SELECT name_norm, artist_name, provider, COALESCE(image_path, ''), COALESCE(image_url, '')
            FROM files_external_artist_images
            """
        )
        rows = cur.fetchall()
    except Exception:
        rows = []
    if not rows:
        try:
            cur.execute(
                """
                INSERT INTO files_index_meta(key, value, updated_at)
                VALUES ('external_artist_images_norm', 'strict_v2', NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """
            )
        except Exception:
            pass
        return

    def path_size(p: str) -> int:
        try:
            pp = Path((p or "").strip())
            if pp.is_file():
                return int(pp.stat().st_size or 0)
        except Exception:
            return 0
        return 0

    migrated = 0
    for old_norm, artist_name, provider, image_path, image_url in rows:
        old_key = str(old_norm or "").strip()
        name = str(artist_name or "").strip()
        if not old_key or not name:
            continue
        new_key = norm_artist_key(name) or old_key
        if not new_key or new_key == old_key:
            continue

        cand_path = str(image_path or "").strip()
        cand_url = str(image_url or "").strip()
        cand_size = path_size(cand_path)

        # Decide whether to override an existing strict-key row.
        try:
            cur.execute(
                "SELECT COALESCE(image_path, ''), COALESCE(image_url, ''), COALESCE(provider, '') FROM files_external_artist_images WHERE name_norm = %s",
                (new_key,),
            )
            ex = cur.fetchone()
        except Exception:
            ex = None
        override = True
        if ex:
            ex_path = str(ex[0] or "").strip()
            ex_url = str(ex[1] or "").strip()
            ex_provider = str(ex[2] or "").strip()
            ex_size = path_size(ex_path)
            # Prefer the larger on-disk cached image; also replace tiny placeholders.
            if ex_size >= 8192 and (ex_size >= cand_size or cand_size <= 0):
                override = False
            if ex_size < 8192 and cand_size >= 8192:
                override = True
            # If existing URL is a known placeholder but candidate is not, override.
            try:
                if ex_url and is_probably_placeholder_artist_image_url(ex_url) and cand_url and not is_probably_placeholder_artist_image_url(cand_url):
                    override = True
                elif ex_url and is_probably_placeholder_artist_image_url(ex_url) and not cand_url:
                    override = False
            except Exception:
                pass
            # If existing row is empty but candidate has a usable file, override.
            if (not ex_path and cand_path) or (ex_size <= 0 and cand_size > 0):
                override = True
            # Prefer existing non-empty data when candidate is empty.
            if ex_path and not cand_path and ex_size > 0:
                override = False
            if ex_url and not cand_url and not cand_path:
                override = False
            # Preserve existing provider if we decide not to override.
            if not override:
                provider = ex_provider or provider

        if override:
            try:
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
                        new_key,
                        name,
                        (provider or "").strip().lower() or "lastfm",
                        cand_path or None,
                        cand_url or None,
                    ),
                )
            except Exception:
                pass

        # Delete the legacy row regardless; the normalized-key row will be reused by joins.
        try:
            cur.execute("DELETE FROM files_external_artist_images WHERE name_norm = %s", (old_key,))
            migrated += 1
        except Exception:
            pass

    try:
        cur.execute(
            """
            INSERT INTO files_index_meta(key, value, updated_at)
            VALUES ('external_artist_images_norm', 'strict_v2', NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
            """
        )
    except Exception:
        pass
    if migrated:
        logger.info("Migrated %d external artist image cache keys to strict normalization", migrated)



def backfill_artist_canonical_fields(
    conn,
    *,
    norm_artist_key,
    safe_json_load,
    artist_role_hints_from_roles_json,
    artist_is_person_like,
    select_classical_person_display_name,
    files_merge_artist_alias_values,
    files_sync_artist_aliases,
    logger=None,
) -> None:
    logger = logger or logging
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(value, '') FROM files_index_meta WHERE key = 'artist_canonical_schema' LIMIT 1")
            row = cur.fetchone()
            if row and str(row[0] or "").strip() == "v8":
                return
            cur.execute(
                """
                SELECT id,
                       COALESCE(name, ''),
                       COALESCE(name_norm, ''),
                       COALESCE(canonical_name, ''),
                       COALESCE(canonical_name_norm, ''),
                       COALESCE(entity_kind, 'artist'),
                       COALESCE(roles_json, '[]'),
                       COALESCE(aliases_json, '[]')
                FROM files_artists
                """
            )
            rows = cur.fetchall()
            updates: list[tuple[str, str, str, int]] = []
            touched_norms: list[str] = []
            for artist_id, name, name_norm, canonical_name, canonical_name_norm, entity_kind, roles_json, aliases_json in rows:
                current_name = " ".join(str(name or "").split()).strip()
                current_norm = str(name_norm or "").strip()
                current_canonical = " ".join(str(canonical_name or "").split()).strip()
                current_canonical_norm = str(canonical_name_norm or "").strip()
                roles = artist_role_hints_from_roles_json(roles_json)
                aliases = safe_json_load(aliases_json or "[]", fallback=[])
                merged_aliases = files_merge_artist_alias_values([current_name, current_canonical], aliases)
                if artist_is_person_like(entity_kind=str(entity_kind or "artist"), role_hints=roles):
                    preferred = select_classical_person_display_name(
                        current_name=current_name,
                        primary_name=current_canonical or current_name,
                        aliases=merged_aliases,
                    )
                    display_name = preferred or current_name or current_canonical
                    canonical = preferred or current_canonical or current_name
                else:
                    display_name = current_name
                    canonical = current_canonical or current_name
                canonical = " ".join(str(canonical or "").split()).strip() or current_name
                display_name = " ".join(str(display_name or canonical or current_name).split()).strip() or current_name
                canonical_norm = norm_artist_key(canonical) or current_norm
                if (
                    display_name != current_name
                    or canonical != current_canonical
                    or canonical_norm != current_canonical_norm
                ):
                    updates.append((display_name, canonical, canonical_norm, int(artist_id or 0)))
                    if current_norm:
                        touched_norms.append(current_norm)
            if updates:
                cur.executemany(
                    """
                    UPDATE files_artists
                    SET name = %s,
                        canonical_name = %s,
                        canonical_name_norm = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    updates,
                )
            if touched_norms:
                files_sync_artist_aliases(conn, artist_norms=[norm for norm in dict.fromkeys(touched_norms) if norm])
            cur.execute(
                """
                INSERT INTO files_index_meta(key, value, updated_at)
                VALUES ('artist_canonical_schema', 'v8', NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """
            )
    except Exception:
        logger.debug("Artist canonical field backfill failed", exc_info=True)



def purge_weak_classical_artist_images(
    conn,
    *,
    config_dir,
    media_cache_root,
    artist_role_hints_from_roles_json,
    artist_entity_is_classical_like,
    artist_external_image_requires_authoritative_refresh,
    artist_image_url_looks_relevant,
    files_artist_reference_folder,
    is_artist_image_distinct_from_local_covers,
    is_usable_artist_image_path,
    paths_refer_to_same_file,
    logger=None,
) -> None:
    logger = logger or logging
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(value, '') FROM files_index_meta WHERE key = 'artist_image_policy_schema' LIMIT 1")
            row = cur.fetchone()
            if row and str(row[0] or "").strip() == "v9":
                return
            try:
                media_cache_root = path_for_fs_access(Path((media_cache_root or "").strip() or str(config_dir / "media_cache"))).resolve()
            except Exception:
                media_cache_root = Path((media_cache_root or "").strip() or str(config_dir / "media_cache"))
            cur.execute(
                """
                SELECT
                    a.name_norm,
                    COALESCE(a.name, ''),
                    COALESCE(a.canonical_name, ''),
                    COALESCE(a.entity_kind, 'artist'),
                    COALESCE(a.roles_json, '[]'),
                    COALESCE(a.image_path, ''),
                    COALESCE(ext.image_path, ''),
                    COALESCE(ext.provider, ''),
                    COALESCE(ext.image_url, '')
                FROM files_artists a
                LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                """
            )
            weak_rows = cur.fetchall()
            for name_norm, artist_name, canonical_name, entity_kind, roles_json, artist_image_path, ext_image_path, provider, image_url in weak_rows:
                key = str(name_norm or "").strip()
                if not key:
                    continue
                display_name = " ".join(str(canonical_name or artist_name or "").split()).strip()
                role_hints = artist_role_hints_from_roles_json(roles_json)
                ext_path = str(ext_image_path or "").strip()
                art_path = str(artist_image_path or "").strip()
                classical_like = artist_entity_is_classical_like(
                    entity_kind=str(entity_kind or ""),
                    role_hints=role_hints,
                )
                invalidate = artist_external_image_requires_authoritative_refresh(
                    provider=str(provider or "").strip().lower(),
                    image_url=str(image_url or "").strip(),
                    entity_kind=str(entity_kind or ""),
                    role_hints=role_hints,
                )
                if not invalidate and display_name and str(image_url or "").strip():
                    invalidate = not artist_image_url_looks_relevant(
                        str(image_url or "").strip(),
                        artist_name=display_name,
                        entity_kind=str(entity_kind or ""),
                        role_hints=role_hints,
                    )
                if not invalidate and ext_path:
                    try:
                        invalidate = not is_usable_artist_image_path(Path(ext_path))
                    except Exception:
                        invalidate = True
                if not invalidate:
                    reference_folder = files_artist_reference_folder(
                        conn,
                        artist_norm=key,
                        artist_name=display_name,
                    )
                    try:
                        ref_path = Path(ext_path) if ext_path else Path(art_path) if art_path else None
                    except Exception:
                        ref_path = None
                    if reference_folder and ref_path and ref_path.is_file():
                        try:
                            candidate_raw = ref_path.read_bytes()
                        except Exception:
                            candidate_raw = b""
                        if candidate_raw and not is_artist_image_distinct_from_local_covers(reference_folder, candidate_raw):
                            invalidate = True
                if not invalidate:
                    continue
                mirrored = bool(ext_path and art_path and paths_refer_to_same_file(ext_path, art_path))
                art_under_cache = False
                if art_path:
                    try:
                        art_under_cache = path_for_fs_access(Path(art_path)).resolve().is_relative_to(media_cache_root)
                    except Exception:
                        art_under_cache = False
                if not invalidate and classical_like and art_under_cache and not ext_path:
                    invalidate = True
                cur.execute("DELETE FROM files_external_artist_images WHERE name_norm = %s", (key,))
                if mirrored or art_under_cache or not art_path:
                    for raw_path in [ext_path, art_path]:
                        try:
                            if raw_path:
                                resolved = path_for_fs_access(Path(raw_path)).resolve()
                                if resolved.is_relative_to(media_cache_root) and resolved.exists():
                                    resolved.unlink()
                        except Exception:
                            pass
                    cur.execute(
                        """
                        UPDATE files_artists
                        SET has_image = FALSE,
                            image_path = NULL,
                            updated_at = NOW()
                        WHERE name_norm = %s
                        """,
                        (key,),
                    )
            cur.execute(
                """
                INSERT INTO files_index_meta(key, value, updated_at)
                VALUES ('artist_image_policy_schema', 'v9', NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """
            )
    except Exception:
        logger.debug("Artist image policy backfill failed", exc_info=True)



def relink_external_artist_images_to_canonical_norm(
    conn,
    *,
    files_resolve_artist_cache_name_norm,
    files_upsert_external_artist_image,
    logger=None,
) -> None:
    logger = logger or logging
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(value, '') FROM files_index_meta WHERE key = 'external_artist_images_artist_link_schema' LIMIT 1")
            row = cur.fetchone()
            if row and str(row[0] or "").strip() == "v2":
                return
            cur.execute(
                """
                SELECT
                    ext.name_norm,
                    COALESCE(ext.artist_name, ''),
                    COALESCE(ext.provider, ''),
                    COALESCE(ext.image_path, ''),
                    COALESCE(ext.image_url, '')
                FROM files_external_artist_images ext
                LEFT JOIN files_artists art ON art.name_norm = ext.name_norm
                WHERE art.id IS NULL
                """
            )
            rows = cur.fetchall()
    except Exception:
        logger.debug("External artist image relink preflight failed", exc_info=True)
        return

    moved = 0
    for old_norm, artist_name, provider, image_path, image_url in rows:
        old_key = str(old_norm or "").strip()
        display_name = " ".join(str(artist_name or "").split()).strip()
        if not old_key or not display_name:
            continue
        new_key = files_resolve_artist_cache_name_norm(
            conn,
            artist_name=display_name,
            artist_norm=old_key,
        )
        new_key = str(new_key or "").strip()
        if not new_key or new_key == old_key:
            continue
        try:
            files_upsert_external_artist_image(
                conn,
                name_norm=new_key,
                artist_name=display_name,
                provider=str(provider or "").strip().lower() or "lastfm",
                image_path=str(image_path or "").strip() or None,
                image_url=str(image_url or "").strip() or None,
            )
            with conn.cursor() as cur:
                cur.execute("DELETE FROM files_external_artist_images WHERE name_norm = %s", (old_key,))
            moved += 1
        except Exception:
            logger.debug(
                "External artist image relink failed old=%s new=%s artist=%s",
                old_key,
                new_key,
                display_name,
                exc_info=True,
            )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO files_index_meta(key, value, updated_at)
                VALUES ('external_artist_images_artist_link_schema', 'v2', NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """
            )
    except Exception:
        logger.debug("External artist image relink schema stamp failed", exc_info=True)
    if moved:
        logger.info("Relinked %d external artist image row(s) to canonical artist norms.", int(moved))



def backfill_artist_alias_table(
    conn,
    *,
    files_sync_artist_aliases,
    logger=None,
) -> None:
    logger = logger or logging
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(value, '') FROM files_index_meta WHERE key = 'artist_aliases_schema' LIMIT 1")
            row = cur.fetchone()
            if row and str(row[0] or "").strip() == "v7":
                return
            cur.execute("SELECT name_norm FROM files_artists")
            norms = [str(value[0] or "").strip() for value in cur.fetchall() if str(value[0] or "").strip()]
        if not norms:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO files_index_meta(key, value, updated_at)
                    VALUES ('artist_aliases_schema', 'v7', NOW())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                    """
                )
            return
        for start in range(0, len(norms), 1000):
            files_sync_artist_aliases(conn, artist_norms=norms[start : start + 1000])
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO files_index_meta(key, value, updated_at)
                VALUES ('artist_aliases_schema', 'v7', NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """
            )
    except Exception:
        logger.debug("Artist alias table backfill failed", exc_info=True)
