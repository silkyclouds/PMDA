"""Runtime-bound Files artist browse entity publication helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_files_backfill_artist_browse_entities_from_existing_index',
    '_files_resolve_artist_norm_map',
    '_files_remap_resolved_artist_norms',
    '_files_apply_canonical_artist_resolution',
    '_files_split_artist_credit_entities',
    '_files_collect_nonclassical_album_artist_entities',
    '_files_collect_track_contributor_entities',
    '_files_extract_browse_entities_for_album',
    '_build_files_browse_artist_entities',
    '_ensure_files_album_primary_links',
    '_dedupe_files_artist_album_link_rows',
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

def _files_backfill_artist_browse_entities_from_existing_index() -> dict[str, int]:
    """
    Upgrade path for already-published libraries:
    rebuild browseable artist/composer/conductor/orchestra entities + link table
    from existing files_albums/files_artists rows without requiring a fresh scan.
    """
    conn = _files_pg_connect()
    if conn is None:
        return {"artists": 0, "albums": 0, "links": 0}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM files_albums")
            album_count = int((cur.fetchone() or [0])[0] or 0)
            if album_count <= 0:
                return {"artists": 0, "albums": 0, "links": 0}
            cur.execute("SELECT COUNT(*) FROM files_artist_album_links")
            existing_links = int((cur.fetchone() or [0])[0] or 0)
            if existing_links > 0:
                return {"artists": 0, "albums": album_count, "links": existing_links}
            cur.execute(
                """
                SELECT id, name, name_norm, COALESCE(has_image, FALSE), COALESCE(image_path, '')
                FROM files_artists
                """
            )
            artist_rows = cur.fetchall()
            cur.execute(
                """
                SELECT
                    alb.id,
                    alb.folder_path,
                    alb.title,
                    alb.track_count,
                    alb.is_broken,
                    COALESCE(alb.primary_tags_json, '{}'),
                    art.name,
                    art.name_norm
                FROM files_albums alb
                JOIN files_artists art ON art.id = alb.artist_id
                ORDER BY alb.id ASC
                """
            )
            album_rows = cur.fetchall()
            cur.execute(
                """
                SELECT album_id, COALESCE(primary_tags_json, '{}')
                FROM files_tracks
                WHERE COALESCE(NULLIF(TRIM(primary_tags_json), ''), '{}') <> '{}'
                ORDER BY album_id ASC, disc_num ASC, track_num ASC, id ASC
                """
            )
            track_tag_rows = cur.fetchall()

        artists_seed: dict[str, dict[str, Any]] = {}
        for _artist_id, name, name_norm, has_image, image_path in artist_rows:
            norm = str(name_norm or "").strip() or _norm_artist_key(str(name or "").strip())
            if not norm:
                continue
            artists_seed[norm] = {
                "name": str(name or "").strip(),
                "has_image": bool(has_image),
                "image_path": str(image_path or "").strip(),
            }

        albums_payload: list[dict[str, Any]] = []
        album_id_by_folder: dict[str, int] = {}
        track_tags_by_album_id: dict[int, list[str]] = defaultdict(list)
        for album_id, primary_tags_json in track_tag_rows:
            key = int(album_id or 0)
            if key <= 0:
                continue
            raw = str(primary_tags_json or "").strip()
            if raw:
                track_tags_by_album_id[key].append(raw)
        for album_id, folder_path, title, track_count, is_broken, primary_tags_json, artist_name, artist_norm in album_rows:
            folder = str(folder_path or "").strip()
            norm = str(artist_norm or "").strip() or _norm_artist_key(str(artist_name or "").strip())
            if not folder or not norm:
                continue
            album_id_by_folder[folder] = int(album_id or 0)
            albums_payload.append(
                {
                    "folder_path": folder,
                    "artist_norm": norm,
                    "artist_name": str(artist_name or "").strip(),
                    "title": str(title or "").strip(),
                    "track_count": int(track_count or 0),
                    "is_broken": bool(is_broken),
                    "primary_tags_json": str(primary_tags_json or "{}"),
                    "track_primary_tags_jsons": track_tags_by_album_id.get(int(album_id or 0), []),
                }
            )

        if not albums_payload:
            return {"artists": 0, "albums": album_count, "links": 0}

        artists_map, album_links_by_folder = _build_files_browse_artist_entities(artists_seed, albums_payload)
        if not artists_map or not album_links_by_folder:
            return {"artists": 0, "albums": album_count, "links": 0}

        inserted_links = 0
        with conn.transaction():
            with conn.cursor() as cur:
                for artist_norm, data in artists_map.items():
                    cur.execute(
                        """
                        INSERT INTO files_artists (name, name_norm, canonical_name, canonical_name_norm, canonical_mbid, entity_kind, roles_json, aliases_json, has_image, image_path, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (name_norm) DO UPDATE
                        SET name = EXCLUDED.name,
                            canonical_name = EXCLUDED.canonical_name,
                            canonical_name_norm = EXCLUDED.canonical_name_norm,
                            canonical_mbid = CASE
                                WHEN COALESCE(NULLIF(TRIM(EXCLUDED.canonical_mbid), ''), '') <> '' THEN EXCLUDED.canonical_mbid
                                ELSE files_artists.canonical_mbid
                            END,
                            entity_kind = EXCLUDED.entity_kind,
                            roles_json = EXCLUDED.roles_json,
                            aliases_json = EXCLUDED.aliases_json,
                            has_image = EXCLUDED.has_image,
                            image_path = CASE
                                WHEN COALESCE(NULLIF(TRIM(EXCLUDED.image_path), ''), '') <> '' THEN EXCLUDED.image_path
                                ELSE files_artists.image_path
                            END,
                            updated_at = NOW()
                        """,
                        (
                            str(data.get("name") or artist_norm).strip(),
                            artist_norm,
                            str(data.get("canonical_name") or data.get("name") or artist_norm).strip(),
                            str(data.get("canonical_name_norm") or artist_norm).strip(),
                            str(data.get("canonical_mbid") or "").strip() or None,
                            str(data.get("entity_kind") or "artist").strip() or "artist",
                            str(data.get("roles_json") or "[]"),
                            str(data.get("aliases_json") or "[]"),
                            bool(data.get("has_image")),
                            str(data.get("image_path") or "").strip() or None,
                        ),
                    )
                _files_sync_artist_aliases(conn, artists_map=artists_map)
                _files_merge_duplicate_person_artists(conn)
                artists_map, album_links_by_folder, _resolved_artist_norm_map = _files_apply_canonical_artist_resolution(
                    conn,
                    artists_map,
                    albums_payload=albums_payload,
                    album_links_by_folder=album_links_by_folder,
                )
                for album in albums_payload:
                    folder_key = str(album.get("folder_path") or "").strip()
                    primary_link = next(
                        (link for link in (album_links_by_folder.get(folder_key) or []) if bool(link.get("is_primary"))),
                        None,
                    )
                    artist_norm = str((primary_link or {}).get("artist_norm") or "").strip()
                    if artist_norm:
                        album["artist_norm"] = artist_norm

                cur.execute(
                    "SELECT id, name_norm FROM files_artists WHERE name_norm = ANY(%s)",
                    (list(artists_map.keys()),),
                )
                artist_id_by_norm = {
                    str(name_norm or "").strip(): int(artist_id or 0)
                    for artist_id, name_norm in cur.fetchall()
                    if str(name_norm or "").strip() and int(artist_id or 0) > 0
                }

                cur.execute("DELETE FROM files_artist_album_links")
                primary_album_updates: list[tuple[int, int]] = []
                link_rows: list[tuple[int, int, str, bool]] = []
                for folder_path, links in album_links_by_folder.items():
                    album_id = int(album_id_by_folder.get(folder_path) or 0)
                    if album_id <= 0:
                        continue
                    primary_link = next((item for item in links if bool(item.get("is_primary"))), None)
                    if primary_link:
                        primary_id = int(artist_id_by_norm.get(str(primary_link.get("artist_norm") or "").strip()) or 0)
                        if primary_id > 0:
                            primary_album_updates.append((primary_id, album_id))
                    for link in links:
                        artist_id = int(artist_id_by_norm.get(str(link.get("artist_norm") or "").strip()) or 0)
                        if artist_id <= 0:
                            continue
                        role = str(link.get("role") or "artist").strip().lower() or "artist"
                        link_rows.append((artist_id, album_id, role, bool(link.get("is_primary"))))
                link_rows = _dedupe_files_artist_album_link_rows(link_rows)
                if link_rows:
                    cur.executemany(
                        """
                        INSERT INTO files_artist_album_links (artist_id, album_id, role, is_primary, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (artist_id, album_id, role) DO UPDATE
                        SET is_primary = EXCLUDED.is_primary,
                            updated_at = NOW()
                        """,
                        link_rows,
                    )
                    inserted_links = len(link_rows)
                if primary_album_updates:
                    cur.executemany(
                        "UPDATE files_albums SET artist_id = %s, updated_at = NOW() WHERE id = %s",
                        primary_album_updates,
                    )
                cur.execute("UPDATE files_artists SET album_count = 0, track_count = 0, broken_albums_count = 0")
                cur.execute(
                    """
                    WITH artist_rollup AS (
                        SELECT
                            link.artist_id AS artist_id,
                            COUNT(DISTINCT alb.id) AS album_count,
                            COUNT(tr.id) AS track_count,
                            COUNT(DISTINCT CASE WHEN alb.is_broken THEN alb.id END) AS broken_albums_count
                        FROM files_artist_album_links link
                        JOIN files_albums alb ON alb.id = link.album_id
                        LEFT JOIN files_tracks tr ON tr.album_id = alb.id
                        GROUP BY link.artist_id
                    )
                    UPDATE files_artists a
                    SET album_count = COALESCE(r.album_count, 0),
                        track_count = COALESCE(r.track_count, 0),
                        broken_albums_count = COALESCE(r.broken_albums_count, 0),
                        updated_at = NOW()
                    FROM artist_rollup r
                    WHERE a.id = r.artist_id
                    """
                )

        try:
            _files_promote_artist_alias_cache(conn, artists_map)
        except Exception:
            logging.debug("Artist alias cache promotion failed during browse backfill", exc_info=True)
        try:
            _files_enrich_artists_blocking(artists_map)
        except Exception:
            logging.debug("Artist enrichment failed during browse backfill", exc_info=True)
        try:
            refreshed = _files_refresh_artist_media_map_from_db(artists_map)
            _files_precache_artist_media(refreshed)
        except Exception:
            logging.debug("Artist media precache failed during browse backfill", exc_info=True)
        _files_cache_invalidate_all()
        logging.info(
            "Files artist browse entities backfilled from existing index: artists=%d albums=%d links=%d",
            len(artists_map),
            len(albums_payload),
            inserted_links,
        )
        return {"artists": len(artists_map), "albums": len(albums_payload), "links": inserted_links}
    except Exception:
        logging.exception("Files artist browse entity backfill failed")
        return {"artists": 0, "albums": 0, "links": 0}
    finally:
        conn.close()
    return changed

def _files_resolve_artist_norm_map(
    conn,
    norms: list[str] | tuple[str, ...] | set[str] | None,
) -> dict[str, str]:
    requested = [str(norm or "").strip() for norm in (norms or []) if str(norm or "").strip()]
    requested = list(dict.fromkeys(requested))
    if conn is None or not requested:
        return {norm: norm for norm in requested}
    resolved: dict[str, str] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name_norm FROM files_artists WHERE name_norm = ANY(%s)",
                (requested,),
            )
            for (name_norm,) in cur.fetchall():
                key = str(name_norm or "").strip()
                if key:
                    resolved[key] = key
            missing = [norm for norm in requested if norm not in resolved]
            if missing:
                cur.execute(
                    """
                    SELECT alias.alias_norm, alias.artist_name_norm
                    FROM files_artist_aliases alias
                    JOIN files_artists artist ON artist.name_norm = alias.artist_name_norm
                    WHERE alias.alias_norm = ANY(%s)
                    ORDER BY alias.is_canonical DESC, alias.source ASC, alias.artist_name_norm ASC
                    """,
                    (missing,),
                )
                for alias_norm, artist_name_norm in cur.fetchall():
                    alias_key = str(alias_norm or "").strip()
                    target_norm = str(artist_name_norm or "").strip()
                    if alias_key and target_norm and alias_key not in resolved:
                        resolved[alias_key] = target_norm
    except Exception:
        logging.debug("Artist norm alias resolution failed", exc_info=True)
    for norm in requested:
        resolved.setdefault(norm, norm)
    return resolved

def _files_remap_resolved_artist_norms(
    artists_map: dict[str, dict[str, Any]],
    *,
    resolved_norm_map: dict[str, str] | None = None,
    albums_payload: list[dict[str, Any]] | None = None,
    album_links_by_folder: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    mapping = {
        str(key or "").strip(): str(value or "").strip()
        for key, value in dict(resolved_norm_map or {}).items()
        if str(key or "").strip() and str(value or "").strip()
    }
    if not artists_map:
        return {}, dict(album_links_by_folder or {})
    if not mapping:
        mapping = {
            str(norm or "").strip(): str(norm or "").strip()
            for norm in artists_map.keys()
            if str(norm or "").strip()
        }

    def _merge_artist_payload(base: dict[str, Any], incoming: dict[str, Any], target_norm: str) -> dict[str, Any]:
        merged = dict(base or {})
        current = dict(incoming or {})
        base_roles = _artist_role_hints_from_roles_json(merged.get("roles_json") or "[]")
        current_roles = _artist_role_hints_from_roles_json(current.get("roles_json") or "[]")
        merged_roles = sorted(
            {str(role or "").strip().lower() for role in [*base_roles, *current_roles] if str(role or "").strip()},
            key=lambda role: (_FILES_BROWSE_ROLE_PRIORITY.get(role, 99), role),
        )
        existing_entity_kind = str(merged.get("entity_kind") or "artist").strip() or "artist"
        incoming_entity_kind = str(current.get("entity_kind") or "artist").strip() or "artist"
        person_like = _artist_is_person_like(entity_kind=existing_entity_kind, role_hints=merged_roles) or _artist_is_person_like(entity_kind=incoming_entity_kind, role_hints=merged_roles)
        merged["entity_kind"] = (
            _files_best_person_entity_kind([existing_entity_kind, incoming_entity_kind], merged_roles)
            if person_like
            else (existing_entity_kind or incoming_entity_kind or "artist")
        )
        merged["roles_json"] = json.dumps(merged_roles, ensure_ascii=False)
        merged_name = str(merged.get("name") or "").strip()
        incoming_name = str(current.get("name") or "").strip()
        merged_canonical = str(merged.get("canonical_name") or merged_name).strip()
        incoming_canonical = str(current.get("canonical_name") or incoming_name).strip()
        if person_like:
            preferred_name = _choose_preferred_person_identity_name(merged_name, incoming_name)
            preferred_canonical = _choose_preferred_person_identity_name(merged_canonical or preferred_name, incoming_canonical or incoming_name)
            alias_values = _files_merge_artist_alias_values(
                merged.get("aliases_json") or "[]",
                current.get("aliases_json") or "[]",
                [merged_name, incoming_name, preferred_canonical],
            )
            alias_values = _collapse_classical_person_aliases(alias_values)
            display_name = _select_classical_person_display_name(
                current_name=preferred_name,
                primary_name=preferred_canonical,
                aliases=alias_values,
            ) or preferred_canonical or preferred_name
            merged["name"] = display_name
            merged["canonical_name"] = preferred_canonical or display_name
            merged["aliases_json"] = json.dumps(alias_values[:12], ensure_ascii=False)
        else:
            merged["name"] = _choose_preferred_identity_display(merged_name, incoming_name) or incoming_name or merged_name
            merged["canonical_name"] = _choose_preferred_identity_display(merged_canonical, incoming_canonical) or incoming_canonical or merged_canonical or merged["name"]
            alias_values = _files_merge_artist_alias_values(
                merged.get("aliases_json") or "[]",
                current.get("aliases_json") or "[]",
                [merged_name, incoming_name, merged.get("canonical_name") or ""],
            )
            merged["aliases_json"] = json.dumps(alias_values[:12], ensure_ascii=False)
        merged["canonical_name_norm"] = str(current.get("canonical_name_norm") or merged.get("canonical_name_norm") or target_norm).strip() or target_norm
        merged["canonical_mbid"] = str(merged.get("canonical_mbid") or current.get("canonical_mbid") or "").strip()
        if bool(current.get("has_image")) and str(current.get("image_path") or "").strip():
            if not (bool(merged.get("has_image")) and str(merged.get("image_path") or "").strip()):
                merged["has_image"] = True
                merged["image_path"] = str(current.get("image_path") or "").strip()
        return merged

    remapped_artists: dict[str, dict[str, Any]] = {}
    for old_norm, payload in artists_map.items():
        source_norm = str(old_norm or "").strip()
        target_norm = mapping.get(source_norm, source_norm)
        if not target_norm:
            continue
        current_payload = dict(payload or {})
        current_payload["canonical_name_norm"] = str(current_payload.get("canonical_name_norm") or target_norm).strip() or target_norm
        existing_payload = remapped_artists.get(target_norm)
        if existing_payload is None:
            remapped_artists[target_norm] = current_payload
        else:
            remapped_artists[target_norm] = _merge_artist_payload(existing_payload, current_payload, target_norm)

    remapped_links: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for folder_path, links in dict(album_links_by_folder or {}).items():
        seen_links: set[tuple[str, str, bool]] = set()
        for link in links or []:
            raw_norm = str((link or {}).get("artist_norm") or "").strip()
            resolved_norm = mapping.get(raw_norm, raw_norm)
            role = str((link or {}).get("role") or "").strip().lower() or "artist"
            is_primary = bool((link or {}).get("is_primary"))
            key = (resolved_norm, role, is_primary)
            if not resolved_norm or key in seen_links:
                continue
            seen_links.add(key)
            remapped_links[str(folder_path or "").strip()].append(
                {
                    "artist_norm": resolved_norm,
                    "role": role,
                    "is_primary": is_primary,
                }
            )

    for album in albums_payload or []:
        raw_norm = str((album or {}).get("artist_norm") or "").strip()
        if not raw_norm:
            continue
        album["artist_norm"] = mapping.get(raw_norm, raw_norm)

    return remapped_artists, dict(remapped_links)

def _files_apply_canonical_artist_resolution(
    conn,
    artists_map: dict[str, dict[str, Any]],
    *,
    albums_payload: list[dict[str, Any]] | None = None,
    album_links_by_folder: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, str]]:
    norms: set[str] = {
        str(norm or "").strip()
        for norm in (artists_map or {}).keys()
        if str(norm or "").strip()
    }
    for album in albums_payload or []:
        artist_norm = str((album or {}).get("artist_norm") or "").strip()
        if artist_norm:
            norms.add(artist_norm)
    for links in dict(album_links_by_folder or {}).values():
        for link in links or []:
            artist_norm = str((link or {}).get("artist_norm") or "").strip()
            if artist_norm:
                norms.add(artist_norm)
    resolved_norm_map = _files_resolve_artist_norm_map(conn, norms)
    remapped_artists, remapped_links = _files_remap_resolved_artist_norms(
        artists_map or {},
        resolved_norm_map=resolved_norm_map,
        albums_payload=albums_payload,
        album_links_by_folder=album_links_by_folder,
    )
    refreshed_artists = _files_refresh_artist_media_map_from_conn(conn, remapped_artists)
    return refreshed_artists, remapped_links, resolved_norm_map

def _files_split_artist_credit_entities(raw_credit: str) -> tuple[list[str], list[str]]:
    clean = re.sub(r"\s+", " ", str(raw_credit or "").strip())
    if not clean:
        return ([], [])
    base_credit = _strip_identity_artist_feature_clause(clean)
    has_feature_clause = _normalize_identity_text_strict(base_credit) != _normalize_identity_text_strict(clean)
    primary_entities = _split_artist_entities_for_profiles(base_credit if has_feature_clause else clean)
    if not primary_entities and base_credit:
        primary_entities = [base_credit]
    if not has_feature_clause:
        return (primary_entities, [])
    all_entities = _split_artist_entities_for_profiles(clean)
    primary_norms = {
        _norm_artist_key(value)
        for value in primary_entities
        if _norm_artist_key(value)
    }
    featured_entities = [
        value
        for value in all_entities
        if _norm_artist_key(value) and _norm_artist_key(value) not in primary_norms
    ]
    return (primary_entities, featured_entities)

def _files_collect_nonclassical_album_artist_entities(album: dict[str, Any], artist_name: str) -> list[dict[str, Any]]:
    clean_name = re.sub(r"\s+", " ", str(artist_name or "").strip())
    if not clean_name or _identity_text_is_generic(clean_name):
        return []

    entities: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def _push(name: str, role: str, *, is_primary: bool = False) -> None:
        display = re.sub(r"\s+", " ", str(name or "").strip()).strip(" -–—")
        role_key = str(role or "").strip().lower() or "artist"
        norm = _norm_artist_key(display)
        if not display or not norm:
            return
        key = (norm, role_key)
        if key in seen:
            return
        seen.add(key)
        entities.append({"name": display, "role": role_key, "is_primary": bool(is_primary)})

    primary_entities, featured_entities = _files_split_artist_credit_entities(clean_name)
    if not primary_entities:
        primary_entities = [clean_name]
    for idx, value in enumerate(primary_entities):
        _push(value, "artist", is_primary=(idx == 0))
    for value in featured_entities:
        _push(value, "featured", is_primary=False)
    return entities

def _files_collect_track_contributor_entities(
    album: dict[str, Any],
    *,
    primary_artist_names: list[str] | tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    primary_names = [str(value or "").strip() for value in (primary_artist_names or []) if str(value or "").strip()]
    primary_norms = {
        _norm_artist_key(value)
        for value in primary_names
        if _norm_artist_key(value) and not _identity_text_is_generic(value)
    }
    album_primary_is_generic = not primary_norms
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def _push(name: str, role: str) -> None:
        display = re.sub(r"\s+", " ", str(name or "").strip()).strip(" -–—")
        role_key = str(role or "").strip().lower() or "appearance"
        norm = _norm_artist_key(display)
        if not display or not norm or _identity_text_is_generic(display):
            return
        key = (norm, role_key)
        if key in seen:
            return
        seen.add(key)
        out.append({"name": display, "role": role_key, "is_primary": False})

    for tags in _files_album_track_tag_dicts(album):
        track_artist = _normalize_meta_text(
            tags.get("artist")
            or tags.get("albumartist")
            or tags.get("album_artist")
            or ""
        )
        if not track_artist:
            continue
        base_entities, featured_entities = _files_split_artist_credit_entities(track_artist)
        base_norms = {
            _norm_artist_key(value)
            for value in base_entities
            if _norm_artist_key(value) and not _identity_text_is_generic(value)
        }
        track_compilation = str(tags.get("compilation") or "").strip().lower() in {"1", "true", "yes"}
        disjoint_from_primary = bool(base_norms) and bool(primary_norms) and base_norms.isdisjoint(primary_norms)
        should_appear = album_primary_is_generic or track_compilation or disjoint_from_primary
        if should_appear:
            for value in base_entities:
                _push(value, "appearance")
        for value in featured_entities:
            if _norm_artist_key(value) not in primary_norms:
                _push(value, "featured")
    return out

def _files_extract_browse_entities_for_album(
    album: dict[str, Any],
    artists_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    artist_norm = str(album.get("artist_norm") or "").strip()
    artist_name = str(((artists_map.get(artist_norm) or {}).get("name") if artist_norm else "") or "").strip()
    source_image_path = str(((artists_map.get(artist_norm) or {}).get("image_path") if artist_norm else "") or "").strip()
    source_has_image = bool((artists_map.get(artist_norm) or {}).get("has_image")) if artist_norm else False
    entities: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def _append_entity(name: str, role: str, *, is_primary: bool = False, image_path: str = "", has_image: bool = False) -> None:
        clean = re.sub(r"\s+", " ", str(name or "").strip()).strip(" -–—")
        role_key = str(role or "").strip().lower() or "artist"
        if not clean:
            return
        norm = _norm_artist_key(clean)
        if not norm:
            return
        key = (norm, role_key)
        if key in seen:
            return
        seen.add(key)
        artist_row = artists_map.get(norm) or {}
        entities.append(
            {
                "name": clean,
                "norm": norm,
                "role": role_key,
                "is_primary": bool(is_primary),
                "has_image": bool(has_image),
                "image_path": str(image_path or "").strip(),
                "canonical_name": str(artist_row.get("canonical_name") or artist_row.get("name") or clean).strip(),
                "canonical_norm": str(artist_row.get("canonical_name_norm") or norm).strip(),
                "canonical_mbid": str(artist_row.get("canonical_mbid") or "").strip(),
            }
        )

    classical_payload = _files_collect_album_classical_payload(album, fallback_artist=artist_name)
    composer_values = _files_classical_composer_values(classical_payload)
    classical_like = bool(classical_payload) or _files_album_is_classical_like_for_browse(
        album,
        fallback_artist=artist_name,
    )

    if artist_name and not composer_values and not classical_like:
        nonclassical_entities = _files_collect_nonclassical_album_artist_entities(album, artist_name)
        for idx, entity in enumerate(nonclassical_entities):
            role = str(entity.get("role") or "artist").strip().lower() or "artist"
            is_primary = bool(entity.get("is_primary")) or (idx == 0 and role == "artist")
            use_source_media = bool(is_primary and role == "artist")
            _append_entity(
                str(entity.get("name") or "").strip(),
                role,
                is_primary=is_primary,
                image_path=source_image_path if use_source_media else "",
                has_image=source_has_image if use_source_media else False,
            )
        primary_credit_names = [
            str(entity.get("name") or "").strip()
            for entity in nonclassical_entities
            if str(entity.get("role") or "").strip().lower() == "artist"
        ]
        for entity in _files_collect_track_contributor_entities(album, primary_artist_names=primary_credit_names):
            _append_entity(
                str(entity.get("name") or "").strip(),
                str(entity.get("role") or "appearance").strip().lower() or "appearance",
                is_primary=False,
            )

    if not isinstance(classical_payload, dict):
        return entities

    if composer_values:
        for idx, value in enumerate(composer_values):
            _append_entity(value, "composer", is_primary=(idx == 0))
    return entities

def _build_files_browse_artist_entities(
    artists_map: dict[str, dict[str, Any]],
    albums_payload: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    raw_entities: list[dict[str, Any]] = []
    for album in albums_payload:
        for entity in _files_extract_browse_entities_for_album(album, artists_map):
            item = dict(entity)
            item["folder_path"] = str(album.get("folder_path") or "").strip()
            item["track_count"] = int(album.get("track_count") or 0)
            item["is_broken"] = bool(album.get("is_broken"))
            raw_entities.append(item)

    person_alias_targets: list[dict[str, Any]] = sorted(
        [item for item in raw_entities if str(item.get("role") or "").strip().lower() in _FILES_BROWSE_PERSON_ROLES],
        key=lambda entry: (
            -_identity_display_quality_score(str(entry.get("name") or ""))[0],
            -_classical_person_alias_signature(str(entry.get("name") or "")).get("token_count", 0),
            -len(str(entry.get("name") or "")),
            str(entry.get("name") or "").lower(),
        ),
    )
    canonical_norm_by_raw_norm: dict[str, str] = {}
    canonical_name_by_norm: dict[str, str] = {}
    canonical_norm_by_mbid: dict[str, str] = {}

    def _person_alias_bucket_key(name: str, fallback_norm: str = "") -> str:
        sig = _classical_person_alias_signature(name)
        surname = str(sig.get("surname") or "").strip()
        if surname:
            return surname
        tokens = [tok for tok in re.findall(r"[a-z0-9]+", _classical_norm_text(name)) if tok]
        if tokens:
            return tokens[-1]
        return str(fallback_norm or "").strip()

    canonical_norms_by_alias_bucket: dict[str, list[str]] = defaultdict(list)
    for entry in person_alias_targets:
        raw_name = str(entry.get("name") or "").strip()
        raw_norm = str(entry.get("norm") or "").strip()
        canonical_mbid = str(entry.get("canonical_mbid") or "").strip()
        canonical_name_hint = str(entry.get("canonical_name") or raw_name).strip()
        canonical_norm_hint = str(entry.get("canonical_norm") or raw_norm).strip()
        if not raw_norm:
            continue
        if canonical_mbid:
            matched_norm = canonical_norm_by_mbid.get(canonical_mbid) or canonical_norm_hint or raw_norm
            current_name = canonical_name_by_norm.get(matched_norm, canonical_name_hint or raw_name)
            preferred = _choose_preferred_person_identity_name(current_name, canonical_name_hint or raw_name)
            canonical_name_by_norm[matched_norm] = preferred
            canonical_norm_by_raw_norm[raw_norm] = matched_norm
            canonical_norm_by_mbid[canonical_mbid] = matched_norm
            bucket_key = _person_alias_bucket_key(preferred or canonical_name_hint or raw_name, matched_norm)
            if bucket_key and matched_norm not in canonical_norms_by_alias_bucket[bucket_key]:
                canonical_norms_by_alias_bucket[bucket_key].append(matched_norm)
            continue
        matched_norm = None
        bucket_key = _person_alias_bucket_key(raw_name, raw_norm)
        candidate_norms = canonical_norms_by_alias_bucket.get(bucket_key, []) if bucket_key else []
        for candidate_norm in candidate_norms:
            candidate_name = canonical_name_by_norm.get(candidate_norm, "")
            if _classical_person_names_equivalent(raw_name, candidate_name):
                matched_norm = candidate_norm
                preferred = _choose_preferred_person_identity_name(candidate_name, raw_name)
                if preferred != candidate_name:
                    canonical_name_by_norm[candidate_norm] = preferred
                break
        if matched_norm is None:
            canonical_name_by_norm[raw_norm] = raw_name
            matched_norm = raw_norm
            if bucket_key and matched_norm not in canonical_norms_by_alias_bucket[bucket_key]:
                canonical_norms_by_alias_bucket[bucket_key].append(matched_norm)
        canonical_norm_by_raw_norm[raw_norm] = matched_norm

    surname_targets: dict[str, list[str]] = defaultdict(list)
    for candidate_norm, candidate_name in canonical_name_by_norm.items():
        sig = _classical_person_alias_signature(candidate_name)
        surname = str(sig.get("surname") or "").strip()
        if surname:
            surname_targets[surname].append(candidate_norm)
    for entry in person_alias_targets:
        raw_name = str(entry.get("name") or "").strip()
        raw_norm = str(entry.get("norm") or "").strip()
        if not raw_norm:
            continue
        tokens = [tok for tok in re.findall(r"[a-z0-9]+", _classical_norm_text(raw_name)) if tok]
        if len(tokens) != 1:
            continue
        candidates = [cand for cand in surname_targets.get(tokens[0], []) if cand != raw_norm]
        if not candidates:
            continue
        candidate_names = [str(canonical_name_by_norm.get(cand) or cand).strip() for cand in candidates]
        if len(candidates) == 1 or all(
            _classical_person_names_equivalent(left_name, right_name)
            for idx, left_name in enumerate(candidate_names)
            for right_name in candidate_names[idx + 1 :]
            if left_name and right_name
        ):
            best_candidate = max(
                candidates,
                key=lambda cand: _classical_person_display_preference_score(
                    str(canonical_name_by_norm.get(cand) or cand).strip()
                ),
            )
            canonical_norm_by_raw_norm[raw_norm] = best_candidate

    entity_map: dict[str, dict[str, Any]] = {}
    album_links_by_folder: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in raw_entities:
        role = str(entry.get("role") or "").strip().lower() or "artist"
        raw_norm = str(entry.get("norm") or "").strip()
        canonical_norm = raw_norm
        if role in _FILES_BROWSE_PERSON_ROLES:
            canonical_norm = canonical_norm_by_raw_norm.get(raw_norm, raw_norm)
        canonical_name = canonical_name_by_norm.get(canonical_norm, str(entry.get("name") or "").strip())
        if not canonical_norm or not canonical_name:
            continue
        bucket = entity_map.setdefault(
            canonical_norm,
            {
                "name": canonical_name,
                "canonical_name_hint": canonical_name,
                "has_image": False,
                "image_path": "",
                "roles": set(),
                "aliases": set(),
                "album_ids": set(),
                "track_total": 0,
                "broken_count": 0,
            },
        )
        if role in _FILES_BROWSE_PERSON_ROLES:
            bucket["name"] = _choose_preferred_person_identity_name(str(bucket.get("name") or ""), canonical_name)
            bucket["canonical_name_hint"] = _choose_preferred_person_identity_name(
                str(bucket.get("canonical_name_hint") or ""),
                canonical_name,
            )
        else:
            bucket["name"] = _choose_preferred_identity_display(str(bucket.get("name") or ""), canonical_name)
            bucket["canonical_name_hint"] = _choose_preferred_identity_display(
                str(bucket.get("canonical_name_hint") or ""),
                canonical_name,
            )
        bucket["roles"].add(role)
        bucket["aliases"].add(str(entry.get("name") or "").strip())
        bucket["aliases"].add(canonical_name)
        if bool(entry.get("has_image")) and str(entry.get("image_path") or "").strip():
            if not bucket.get("has_image"):
                bucket["has_image"] = True
                bucket["image_path"] = str(entry.get("image_path") or "").strip()
        folder_path = str(entry.get("folder_path") or "").strip()
        if folder_path:
            album_links_by_folder[folder_path].append(
                {
                    "artist_norm": canonical_norm,
                    "role": role,
                    "is_primary": bool(entry.get("is_primary")),
                }
            )

    for canonical_norm, bucket in entity_map.items():
        role_values = sorted(
            set(bucket.get("roles") or set()),
            key=lambda role: (_FILES_BROWSE_ROLE_PRIORITY.get(role, 99), role),
        )
        bucket["entity_kind"] = _files_browse_entity_kind_from_roles(set(role_values))
        bucket["roles_json"] = json.dumps(
            role_values,
            ensure_ascii=False,
        )
        alias_values = [
            value
            for value in sorted(
                {str(v or "").strip() for v in (bucket.get("aliases") or set()) if str(v or "").strip()},
                key=lambda value: (-_identity_display_quality_score(value)[0], -len(value), value.lower()),
            )
            if _norm_artist_key(value) != canonical_norm
        ]
        if _artist_is_person_like(entity_kind=str(bucket.get("entity_kind") or ""), role_hints=role_values):
            alias_values = _collapse_classical_person_aliases(alias_values)
            bucket["name"] = _select_classical_person_display_name(
                current_name=str(bucket.get("name") or "").strip(),
                primary_name=str(bucket.get("canonical_name_hint") or bucket.get("name") or "").strip(),
                aliases=alias_values,
            ) or str(bucket.get("name") or "").strip()
        else:
            bucket["name"] = _choose_preferred_identity_display(
                str(bucket.get("name") or "").strip(),
                str(bucket.get("canonical_name_hint") or bucket.get("name") or "").strip(),
            )
        bucket["aliases_json"] = json.dumps(alias_values[:12], ensure_ascii=False)
        bucket.pop("canonical_name_hint", None)
    return entity_map, dict(album_links_by_folder)

def _ensure_files_album_primary_links(
    artists_map: dict[str, dict[str, Any]],
    albums_payload: list[dict[str, Any]],
    album_links_by_folder: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]], int]:
    """
    Keep publication rebuilds lossless.

    Browse-entity extraction can legitimately fail to derive display entities for
    noisy/generic albums. That must not make a published album disappear from the
    visible library: fall back to the persisted album artist and add a primary
    artist link if needed.
    """
    fixed = 0

    def _ensure_artist(norm: str, name: str, source: dict[str, Any] | None = None) -> None:
        norm = str(norm or "").strip()
        if not norm:
            return
        clean_name = re.sub(r"\s+", " ", str(name or "").strip()) or norm or "Unknown Artist"
        source = source or {}
        current = artists_map.get(norm)
        if current is None:
            artists_map[norm] = {
                "name": clean_name,
                "canonical_name": str(source.get("canonical_name") or clean_name),
                "canonical_name_norm": str(source.get("canonical_name_norm") or norm),
                "canonical_mbid": str(source.get("canonical_mbid") or ""),
                "entity_kind": str(source.get("entity_kind") or "artist"),
                "roles_json": str(source.get("roles_json") or json.dumps(["artist"], ensure_ascii=False)),
                "aliases_json": str(source.get("aliases_json") or "[]"),
                "has_image": bool(source.get("has_image")),
                "image_path": str(source.get("image_path") or "").strip(),
            }
            return
        current["name"] = _choose_preferred_identity_display(str(current.get("name") or ""), clean_name)
        current.setdefault("canonical_name", current.get("name") or clean_name)
        current.setdefault("canonical_name_norm", norm)
        current.setdefault("canonical_mbid", "")
        current.setdefault("entity_kind", "artist")
        current.setdefault("roles_json", json.dumps(["artist"], ensure_ascii=False))
        current.setdefault("aliases_json", "[]")
        current.setdefault("has_image", False)
        current.setdefault("image_path", "")

    for album in albums_payload:
        folder_key = str(album.get("folder_path") or "").strip()
        if not folder_key:
            continue
        fallback_name = re.sub(
            r"\s+",
            " ",
            str(album.get("artist_name") or album.get("artist") or "").strip(),
        ) or "Unknown Artist"
        fallback_norm = (
            str(album.get("artist_norm") or "").strip()
            or _norm_artist_key(fallback_name)
            or "unknown artist"
        )
        album["artist_norm"] = fallback_norm
        album["artist_name"] = fallback_name
        source_artist = artists_map.get(fallback_norm) or {}
        _ensure_artist(fallback_norm, fallback_name, source_artist)

        links = album_links_by_folder.setdefault(folder_key, [])
        for link in links:
            link_norm = str((link or {}).get("artist_norm") or "").strip()
            if not link_norm:
                continue
            link_name = str((artists_map.get(link_norm) or {}).get("name") or link_norm).strip()
            _ensure_artist(link_norm, link_name, artists_map.get(link_norm) or {})

        primary_link = next(
            (
                link
                for link in links
                if bool((link or {}).get("is_primary"))
                and str((link or {}).get("artist_norm") or "").strip() in artists_map
            ),
            None,
        )
        if primary_link is None:
            links.insert(0, {"artist_norm": fallback_norm, "role": "artist", "is_primary": True})
            fixed += 1
    return artists_map, dict(album_links_by_folder), fixed

def _dedupe_files_artist_album_link_rows(
    rows: list[tuple[int, int, str, bool]] | tuple[tuple[int, int, str, bool], ...] | None,
) -> list[tuple[int, int, str, bool]]:
    """
    PostgreSQL ON CONFLICT cannot update the same target row twice in one batch.
    Canonical artist resolution can collapse several generated links to the same
    (artist, album, role), so normalize and merge them before executemany().
    """
    deduped: dict[tuple[int, int, str], bool] = {}
    for raw in rows or []:
        try:
            artist_id = int(raw[0] or 0)
            album_id = int(raw[1] or 0)
        except Exception:
            continue
        if artist_id <= 0 or album_id <= 0:
            continue
        role = str(raw[2] if len(raw) > 2 else "artist").strip().lower() or "artist"
        key = (artist_id, album_id, role)
        deduped[key] = bool(deduped.get(key)) or bool(raw[3] if len(raw) > 3 else False)
    return [(artist_id, album_id, role, is_primary) for (artist_id, album_id, role), is_primary in deduped.items()]

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}

def _files_backfill_artist_browse_entities_from_existing_index_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_backfill_artist_browse_entities_from_existing_index(*args, **kwargs)

def _files_resolve_artist_norm_map_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_resolve_artist_norm_map(*args, **kwargs)

def _files_remap_resolved_artist_norms_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_remap_resolved_artist_norms(*args, **kwargs)

def _files_apply_canonical_artist_resolution_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_apply_canonical_artist_resolution(*args, **kwargs)

def _files_split_artist_credit_entities_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_split_artist_credit_entities(*args, **kwargs)

def _files_collect_nonclassical_album_artist_entities_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_collect_nonclassical_album_artist_entities(*args, **kwargs)

def _files_collect_track_contributor_entities_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_collect_track_contributor_entities(*args, **kwargs)

def _files_extract_browse_entities_for_album_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_extract_browse_entities_for_album(*args, **kwargs)

def _build_files_browse_artist_entities_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_files_browse_artist_entities(*args, **kwargs)

def _ensure_files_album_primary_links_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ensure_files_album_primary_links(*args, **kwargs)

def _dedupe_files_artist_album_link_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _dedupe_files_artist_album_link_rows(*args, **kwargs)
