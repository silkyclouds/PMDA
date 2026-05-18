"""Runtime-backed Files library index rebuild jobs.

This module owns the heavy PostgreSQL/filesystem publication index rebuild
implementations extracted from ``pmda.py``. It accepts the live PMDA runtime at
the boundary while the remaining DB/session dependencies are split into explicit
services.
"""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one library index rebuild call."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "_rebuild_files_library_index_for_artist",
        "_rebuild_files_library_index",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def rebuild_files_library_index_for_artist_for_runtime(
    runtime: Any,
    artist_hint: str,
    *,
    reason: str = "manual_artist_upsert",
    wait_if_running: bool = False,
):
    """Run a single-artist Files index refresh using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _rebuild_files_library_index_for_artist_impl(
        artist_hint,
        reason=reason,
        wait_if_running=wait_if_running,
    )


def rebuild_files_library_index_for_runtime(
    runtime: Any,
    reason: str = "manual",
    wait_if_running: bool = False,
):
    """Run a full Files index rebuild using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _rebuild_files_library_index_impl(
        reason=reason,
        wait_if_running=wait_if_running,
    )


def _rebuild_files_library_index_for_artist_impl(
    artist_hint: str,
    *,
    reason: str = "manual_artist_upsert",
    wait_if_running: bool = False,
) -> dict:
    """
    Granular Files index rebuild for one artist from published rows.
    If no published rows exist for that artist, perform targeted cleanup only.
    """
    if _get_library_mode() != "files":
        return {"ok": False, "error": "LIBRARY_MODE is not 'files'"}
    if not FILES_ROOTS:
        return {"ok": False, "error": "FILES_ROOTS is empty"}
    if not _files_pg_init_schema():
        return {"ok": False, "error": "PostgreSQL schema unavailable"}

    artist_name = str(artist_hint or "").strip()
    if not artist_name:
        return {"ok": False, "error": "artist_hint is empty"}

    acquired = files_index_lock.acquire(blocking=wait_if_running)
    if not acquired:
        return {"ok": False, "running": True, "error": "Files index rebuild already running"}
    try:
        started_at = time.time()
        reason_norm = str(reason or "").strip()
        scan_critical_rebuild = _scan_pipeline_active() or reason_norm.startswith("scan_artist_ready_")
        background_published_catchup = reason_norm.startswith("published_catchup_")
        lightweight_background_upsert = bool(background_published_catchup)
        status_visible = not background_published_catchup
        if status_visible:
            _files_index_set_state(
                running=True,
                started_at=started_at,
                finished_at=None,
                phase="writing",
                phase_message=f"Refreshing artist index for {artist_name}",
                current_folder=f"artist:{artist_name}",
                error=None,
            )

        artists_map, albums_payload, payload_count = _load_files_library_published_payload_for_artist(artist_name)
        if payload_count <= 0:
            removed_artists = 0
            conn = _files_pg_connect()
            if conn is not None:
                try:
                    artist_norm = _norm_artist_key(artist_name)
                    artist_norm_alt = norm_album(artist_name or "")
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT id
                                FROM files_artists
                                WHERE name_norm = %s
                                   OR name_norm = %s
                                   OR lower(name) = lower(%s)
                                """,
                                (artist_norm, artist_norm_alt or artist_norm, artist_name),
                            )
                            ids = [int(r[0]) for r in cur.fetchall() if r and r[0]]
                            if ids:
                                cur.execute("DELETE FROM files_artists WHERE id = ANY(%s)", (ids,))
                                removed_artists = len(ids)
                            cur.execute(
                                """
                                DELETE FROM files_artists
                                WHERE id NOT IN (SELECT DISTINCT artist_id FROM files_artist_album_links)
                                """
                            )
                            _files_index_write_meta(cur, "last_reason", reason)
                            _files_index_write_meta(cur, "last_build_ts", str(int(time.time())))
                            _files_index_write_meta(cur, "source", "published_rows_artist_cleanup")
                finally:
                    conn.close()
            _files_cache_invalidate_all()
            artists_count, albums_count, tracks_count = _files_index_read_counts()
            if status_visible:
                _files_index_set_state(
                    running=False,
                    finished_at=time.time(),
                    phase="done",
                    phase_message="Library artist refresh complete",
                    current_folder=None,
                    artists=artists_count,
                    albums=albums_count,
                    tracks=tracks_count,
                    error=None,
                )
            logging.info(
                "Files library index artist cleanup (%s, artist=%s): no published rows, removed %d artist row(s).",
                reason,
                artist_name,
                removed_artists,
            )
            return {
                "ok": True,
                "artists": artists_count,
                "albums": albums_count,
                "tracks": tracks_count,
                "artist_removed": removed_artists,
                "source": "published_rows_artist_cleanup",
            }

        _apply_genre_defaults_to_albums_payload(albums_payload)
        artists_map, album_links_by_folder = _build_files_browse_artist_entities(artists_map, albums_payload)
        artists_map, album_links_by_folder, _repaired_primary_links = _ensure_files_album_primary_links(
            artists_map,
            albums_payload,
            album_links_by_folder,
        )
        for album in albums_payload:
            folder_key = str(album.get("folder_path") or "").strip()
            primary_link = next(
                (link for link in (album_links_by_folder.get(folder_key) or []) if bool(link.get("is_primary"))),
                None,
            )
            if primary_link and str(primary_link.get("artist_norm") or "").strip():
                album["artist_norm"] = str(primary_link.get("artist_norm") or "").strip()

        if status_visible:
            _files_index_set_state(
                phase="media_prepare",
                phase_message="Preparing artist artwork and media",
            )
        if lightweight_background_upsert:
            covers_promoted = 0
            artists_promoted = 0
        else:
            covers_promoted, artists_promoted = _promote_files_media_paths_to_cache(artists_map, albums_payload)

        base_artists_map = copy.deepcopy(artists_map)
        base_albums_payload = copy.deepcopy(albums_payload)
        base_album_links_by_folder = copy.deepcopy(album_links_by_folder)
        resolved_artist_norm_map: dict[str, str] = {}
        total_tracks = 0
        embeddings_upserted = 0
        last_db_error: Optional[Exception] = None

        def _write_artist_payload_once(
            attempt_artists_map: dict[str, dict[str, Any]],
            attempt_albums_payload: list[dict[str, Any]],
            attempt_album_links_by_folder: dict[str, list[dict[str, Any]]],
        ) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, str], int, int]:
            total_tracks_local = 0
            embeddings_upserted_local = 0
            resolved_artist_norm_map_local: dict[str, str] = {}
            conn = _files_pg_connect()
            if conn is None:
                raise RuntimeError("PostgreSQL connection unavailable during granular rebuild")
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        artist_rows = [
                            (
                                data["name"],
                                norm,
                                str(data.get("canonical_name") or data["name"]),
                                str(data.get("canonical_name_norm") or norm),
                                str(data.get("canonical_mbid") or ""),
                                str(data.get("entity_kind") or "artist"),
                                str(data.get("roles_json") or "[]"),
                                str(data.get("aliases_json") or "[]"),
                                bool(data.get("has_image")),
                                data.get("image_path") or "",
                            )
                            for norm, data in attempt_artists_map.items()
                        ]
                        if artist_rows:
                            cur.executemany(
                                """
                                INSERT INTO files_artists (name, name_norm, canonical_name, canonical_name_norm, canonical_mbid, entity_kind, roles_json, aliases_json, has_image, image_path, created_at, updated_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                                ON CONFLICT (name_norm) DO UPDATE SET
                                    name = EXCLUDED.name,
                                    canonical_name = EXCLUDED.canonical_name,
                                    canonical_name_norm = EXCLUDED.canonical_name_norm,
                                    canonical_mbid = CASE
                                        WHEN EXCLUDED.canonical_mbid IS NULL OR EXCLUDED.canonical_mbid = '' THEN files_artists.canonical_mbid
                                        ELSE EXCLUDED.canonical_mbid
                                    END,
                                    entity_kind = EXCLUDED.entity_kind,
                                    roles_json = EXCLUDED.roles_json,
                                    aliases_json = EXCLUDED.aliases_json,
                                    has_image = EXCLUDED.has_image,
                                    image_path = CASE
                                        WHEN EXCLUDED.image_path IS NULL OR EXCLUDED.image_path = '' THEN files_artists.image_path
                                        ELSE EXCLUDED.image_path
                                    END,
                                    updated_at = NOW()
                                """,
                                artist_rows,
                            )
                        _files_sync_artist_aliases(conn, artists_map=attempt_artists_map)
                        _files_merge_duplicate_person_artists(conn)
                        attempt_artists_map, attempt_album_links_by_folder, resolved_artist_norm_map_local = _files_apply_canonical_artist_resolution(
                            conn,
                            attempt_artists_map,
                            albums_payload=attempt_albums_payload,
                            album_links_by_folder=attempt_album_links_by_folder,
                        )
                        _files_promote_artist_alias_cache(conn, attempt_artists_map)
                        for album in attempt_albums_payload:
                            folder_key = str(album.get("folder_path") or "").strip()
                            primary_link = next(
                                (link for link in (attempt_album_links_by_folder.get(folder_key) or []) if bool(link.get("is_primary"))),
                                None,
                            )
                            artist_norm = str((primary_link or {}).get("artist_norm") or "").strip()
                            if artist_norm:
                                album["artist_norm"] = artist_norm
                        artist_norms = [norm for norm in attempt_artists_map.keys()]
                        cur.execute(
                            "SELECT id, name_norm FROM files_artists WHERE name_norm = ANY(%s)",
                            (artist_norms,),
                        )
                        artist_id_by_norm = {str(r[1]): int(r[0]) for r in cur.fetchall()}
                        target_artist_ids = sorted({aid for aid in artist_id_by_norm.values() if aid > 0})
                        if target_artist_ids:
                            cur.execute(
                                """
                                DELETE FROM files_albums
                                WHERE id IN (
                                    SELECT DISTINCT album_id
                                    FROM files_artist_album_links
                                    WHERE artist_id = ANY(%s)
                                )
                                """,
                                (target_artist_ids,),
                            )

                        album_rows = []
                        for album in attempt_albums_payload:
                            artist_id = artist_id_by_norm.get(album["artist_norm"])
                            if not artist_id:
                                continue
                            album_sample_rate, album_bit_depth = _representative_album_audio_profile(album.get("tracks") or [])
                            album_rows.append(
                                (
                                    artist_id,
                                    album["title"],
                                    album["title_norm"],
                                    album["folder_path"],
                                    album["year"],
                                    album["date_text"],
                                    album["genre"],
                                    album.get("label") or "",
                                    album["tags_json"],
                                    album["format"],
                                    bool(album["is_lossless"]),
                                    album_sample_rate,
                                    album_bit_depth,
                                    bool(album["has_cover"]),
                                    album["cover_path"],
                                    bool(album["mb_identified"]),
                                    bool(album.get("strict_match_verified")),
                                    _normalize_identity_provider(str(album.get("strict_match_provider") or "")),
                                    str(album.get("strict_reject_reason") or "").strip(),
                                    float(album.get("strict_tracklist_score") or 0.0),
                                    album["musicbrainz_release_group_id"],
                                    album.get("musicbrainz_release_id") or "",
                                    album.get("discogs_release_id") or "",
                                    album.get("lastfm_album_mbid") or "",
                                    album.get("bandcamp_album_url") or "",
                                    _normalize_identity_provider(str(album.get("metadata_source") or "")),
                                    album["track_count"],
                                    album["total_duration_sec"],
                                    bool(album["is_broken"]),
                                    album["expected_track_count"],
                                    album["actual_track_count"],
                                    album["missing_indices_json"],
                                    album["missing_required_tags_json"],
                                    album["primary_tags_json"],
                                )
                            )
                        if album_rows:
                            cur.executemany(
                                """
                                INSERT INTO files_albums (
                                    artist_id, title, title_norm, folder_path, year, date_text, genre, label, tags_json,
                                    format, is_lossless, sample_rate, bit_depth, has_cover, cover_path, mb_identified,
                                    strict_match_verified, strict_match_provider, strict_reject_reason, strict_tracklist_score,
                                    musicbrainz_release_group_id, musicbrainz_release_id,
                                    discogs_release_id, lastfm_album_mbid, bandcamp_album_url, metadata_source,
                                    track_count, total_duration_sec, is_broken, expected_track_count, actual_track_count,
                                    missing_indices_json, missing_required_tags_json, primary_tags_json,
                                    created_at, updated_at
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s,
                                    NOW(), NOW()
                                )
                                ON CONFLICT (folder_path) DO UPDATE SET
                                    artist_id = EXCLUDED.artist_id,
                                    title = EXCLUDED.title,
                                    title_norm = EXCLUDED.title_norm,
                                    year = EXCLUDED.year,
                                    date_text = EXCLUDED.date_text,
                                    genre = EXCLUDED.genre,
                                    label = EXCLUDED.label,
                                    tags_json = EXCLUDED.tags_json,
                                    format = EXCLUDED.format,
                                    is_lossless = EXCLUDED.is_lossless,
                                    sample_rate = EXCLUDED.sample_rate,
                                    bit_depth = EXCLUDED.bit_depth,
                                    has_cover = EXCLUDED.has_cover,
                                    cover_path = EXCLUDED.cover_path,
                                    mb_identified = EXCLUDED.mb_identified,
                                    strict_match_verified = EXCLUDED.strict_match_verified,
                                    strict_match_provider = EXCLUDED.strict_match_provider,
                                    strict_reject_reason = EXCLUDED.strict_reject_reason,
                                    strict_tracklist_score = EXCLUDED.strict_tracklist_score,
                                    musicbrainz_release_group_id = EXCLUDED.musicbrainz_release_group_id,
                                    musicbrainz_release_id = EXCLUDED.musicbrainz_release_id,
                                    discogs_release_id = EXCLUDED.discogs_release_id,
                                    lastfm_album_mbid = EXCLUDED.lastfm_album_mbid,
                                    bandcamp_album_url = EXCLUDED.bandcamp_album_url,
                                    metadata_source = EXCLUDED.metadata_source,
                                    track_count = EXCLUDED.track_count,
                                    total_duration_sec = EXCLUDED.total_duration_sec,
                                    is_broken = EXCLUDED.is_broken,
                                    expected_track_count = EXCLUDED.expected_track_count,
                                    actual_track_count = EXCLUDED.actual_track_count,
                                    missing_indices_json = EXCLUDED.missing_indices_json,
                                    missing_required_tags_json = EXCLUDED.missing_required_tags_json,
                                    primary_tags_json = EXCLUDED.primary_tags_json,
                                    updated_at = NOW()
                                """,
                                album_rows,
                            )

                        folder_paths = [str(a.get("folder_path") or "") for a in attempt_albums_payload if str(a.get("folder_path") or "")]
                        cur.execute(
                            "SELECT id, folder_path FROM files_albums WHERE folder_path = ANY(%s)",
                            (folder_paths,),
                        )
                        album_id_by_folder = {str(r[1]): int(r[0]) for r in cur.fetchall()}
                        album_ids_written = sorted({aid for aid in album_id_by_folder.values() if aid > 0})
                        if album_ids_written:
                            cur.execute("DELETE FROM files_artist_album_links WHERE album_id = ANY(%s)", (album_ids_written,))
                        link_rows: list[tuple[int, int, str, bool]] = []
                        for album in attempt_albums_payload:
                            album_id = album_id_by_folder.get(str(album.get("folder_path") or "").strip())
                            if not album_id:
                                continue
                            for link in (attempt_album_links_by_folder.get(str(album.get("folder_path") or "").strip()) or []):
                                link_artist_id = artist_id_by_norm.get(str(link.get("artist_norm") or "").strip())
                                if not link_artist_id:
                                    continue
                                role = str(link.get("role") or "artist").strip().lower() or "artist"
                                link_rows.append((int(link_artist_id), int(album_id), role, bool(link.get("is_primary"))))
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

                        for album in attempt_albums_payload:
                            album_id = album_id_by_folder.get(str(album.get("folder_path") or ""))
                            if not album_id:
                                continue
                            track_rows = []
                            for t in (album.get("tracks") or []):
                                file_path = str(t.get("file_path") or "").strip()
                                if not file_path:
                                    continue
                                disc_num = max(1, _clamp_int(t.get("disc_num"), 1, _PG_INT4_MIN, _PG_INT4_MAX))
                                track_num = max(0, _clamp_int(t.get("track_num"), 0, _PG_INT4_MIN, _PG_INT4_MAX))
                                duration_sec = max(0, _clamp_int(t.get("duration_sec"), 0, _PG_INT4_MIN, _PG_INT4_MAX))
                                bitrate = max(0, _clamp_int(t.get("bitrate"), 0, _PG_INT4_MIN, _PG_INT4_MAX))
                                sample_rate = max(0, _clamp_int(t.get("sample_rate"), 0, _PG_INT4_MIN, _PG_INT4_MAX))
                                bit_depth = max(0, _clamp_int(t.get("bit_depth"), 0, _PG_INT4_MIN, _PG_INT4_MAX))
                                file_size_bytes = max(0, _clamp_int(t.get("file_size_bytes"), 0, _PG_INT8_MIN, _PG_INT8_MAX))
                                track_rows.append(
                                    (
                                        album_id,
                                        file_path,
                                        str(t.get("title") or ""),
                                        disc_num,
                                        track_num,
                                        duration_sec,
                                        str(t.get("format") or ""),
                                        bitrate,
                                        sample_rate,
                                        bit_depth,
                                        file_size_bytes,
                                        str(t.get("primary_tags_json") or "{}"),
                                    )
                                )
                            if not track_rows:
                                continue
                            cur.executemany(
                                """
                                INSERT INTO files_tracks (
                                    album_id, file_path, title, disc_num, track_num, duration_sec, format,
                                    bitrate, sample_rate, bit_depth, file_size_bytes, primary_tags_json, created_at, updated_at
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, NOW(), NOW()
                                )
                                ON CONFLICT (file_path) DO UPDATE SET
                                    album_id = EXCLUDED.album_id,
                                    title = EXCLUDED.title,
                                    disc_num = EXCLUDED.disc_num,
                                    track_num = EXCLUDED.track_num,
                                    duration_sec = EXCLUDED.duration_sec,
                                    format = EXCLUDED.format,
                                    bitrate = EXCLUDED.bitrate,
                                    sample_rate = EXCLUDED.sample_rate,
                                    bit_depth = EXCLUDED.bit_depth,
                                    file_size_bytes = EXCLUDED.file_size_bytes,
                                    primary_tags_json = EXCLUDED.primary_tags_json,
                                    updated_at = NOW()
                                """,
                                track_rows,
                            )
                            total_tracks_local += len(track_rows)

                        if target_artist_ids:
                            cur.execute(
                                """
                                UPDATE files_artists a
                                SET album_count = COALESCE(s.album_count, 0),
                                    track_count = COALESCE(s.track_count, 0),
                                    broken_albums_count = COALESCE(s.broken_albums_count, 0),
                                    updated_at = NOW()
                                FROM (
                                    SELECT
                                        artist_id,
                                        COUNT(*) AS album_count,
                                        COALESCE(SUM(track_count), 0) AS track_count,
                                        COALESCE(SUM(CASE WHEN is_broken THEN 1 ELSE 0 END), 0) AS broken_albums_count
                                    FROM (
                                        SELECT DISTINCT
                                            link.artist_id,
                                            link.album_id,
                                            alb.track_count,
                                            alb.is_broken
                                        FROM files_artist_album_links link
                                        JOIN files_albums alb ON alb.id = link.album_id
                                        WHERE link.artist_id = ANY(%s)
                                    ) artist_album_rollup
                                    GROUP BY artist_id
                                ) s
                                WHERE a.id = s.artist_id
                                """,
                                (target_artist_ids,),
                            )
                            cur.execute(
                                """
                                DELETE FROM files_artists
                                WHERE id = ANY(%s)
                                  AND id NOT IN (SELECT DISTINCT artist_id FROM files_artist_album_links)
                                """,
                                (target_artist_ids,),
                            )
                        if lightweight_background_upsert:
                            embeddings_upserted_local = 0
                        else:
                            embeddings_upserted_local = _reco_upsert_track_embeddings_for_album_ids(conn, album_ids_written)
                        _files_index_write_meta(cur, "last_reason", reason)
                        _files_index_write_meta(cur, "last_build_ts", str(int(time.time())))
                        _files_index_write_meta(cur, "source", "published_rows_artist_upsert")
                        if not lightweight_background_upsert:
                            _files_index_write_meta(cur, "track_embeddings_source", RECO_EMBED_SOURCE)
            finally:
                conn.close()
            return (
                attempt_artists_map,
                attempt_albums_payload,
                attempt_album_links_by_folder,
                resolved_artist_norm_map_local,
                total_tracks_local,
                embeddings_upserted_local,
            )

        for attempt in range(1, 3):
            attempt_artists_map = base_artists_map if attempt == 1 else copy.deepcopy(base_artists_map)
            attempt_albums_payload = base_albums_payload if attempt == 1 else copy.deepcopy(base_albums_payload)
            attempt_album_links_by_folder = base_album_links_by_folder if attempt == 1 else copy.deepcopy(base_album_links_by_folder)
            try:
                (
                    artists_map,
                    albums_payload,
                    album_links_by_folder,
                    resolved_artist_norm_map,
                    total_tracks,
                    embeddings_upserted,
                ) = _write_artist_payload_once(
                    attempt_artists_map,
                    attempt_albums_payload,
                    attempt_album_links_by_folder,
                )
                last_db_error = None
                break
            except Exception as exc:
                last_db_error = exc
                retryable = _files_pg_is_connection_dropped_error(exc)
                if retryable and attempt < 2:
                    logging.warning(
                        "[Files Index] !⚠ Artist upsert lost PostgreSQL connection artist=%r reason=%s attempt=%d/2; retrying once: %s",
                        artist_name,
                        reason,
                        attempt,
                        exc,
                    )
                    _files_pg_reap_stale_connections(closed_only=False, log_reason="artist-upsert-retry")
                    time.sleep(0.25)
                    continue
                raise
        if last_db_error is not None:
            raise last_db_error

        if status_visible:
            _files_index_set_state(
                phase="artist_enrichment",
                phase_message=f"Refreshing artist page for {artist_name}",
                current_artist=None,
                current_folder=None,
                phase_item_done=0,
                phase_item_total=0,
                phase_item_label="artist pages",
            )
        if scan_critical_rebuild:
            primary_norm = _norm_artist_key(artist_name) or norm_album(artist_name or "")
            primary_norm = str(resolved_artist_norm_map.get(primary_norm, primary_norm)).strip() or primary_norm
            payload = artists_map.get(primary_norm) if primary_norm else None
            target_name = str((payload or {}).get("name") or artist_name or "").strip()
            if primary_norm and target_name:
                enrich_albums = [
                    (
                        str(album.get("title") or "").strip(),
                        str(album.get("title_norm") or "").strip(),
                    )
                    for album in (albums_payload or [])
                    if str(album.get("artist_norm") or "").strip() == primary_norm
                    and str(album.get("title_norm") or "").strip()
                ]
                spawned = _enqueue_files_profile_enrichment(
                    artist_name=target_name,
                    artist_norm=primary_norm,
                    albums=enrich_albums,
                    allow_soft_profiles=True,
                    skip_album_profiles=False,
                    fast_mode=False,
                    force=True,
                )
                logging.info(
                    "[Scan Pipeline] deferred artist enrichment to background artist=%r reason=%s spawned=%s",
                    target_name,
                    reason,
                    bool(spawned),
                )
        elif not lightweight_background_upsert:
            _files_enrich_artists_blocking(artists_map)
        artists_map = _files_refresh_artist_media_map_from_db(artists_map)
        if status_visible:
            _files_index_set_state(
                phase="media_cache",
                phase_message="Finalizing artist media cache",
                current_artist=None,
                current_folder=None,
                phase_item_done=None,
                phase_item_total=None,
                phase_item_label=None,
            )
        if lightweight_background_upsert:
            covers_cached = 0
            artists_cached = 0
        else:
            covers_cached, artists_cached = _precache_files_media_assets(
                artists_map,
                albums_payload,
                include_album_covers=True,
                include_artist_images=not scan_critical_rebuild,
            )
        _files_cache_invalidate_all()
        artists_count, albums_count, tracks_count = _files_index_read_counts()
        _files_index_sync_live_counts_meta(
            reason=reason,
            source="published_rows_artist_upsert",
            counts=(artists_count, albums_count, tracks_count),
        )
        _tracks_count, embeddings_count = _files_index_read_track_and_embedding_counts()
        elapsed = round(time.time() - started_at, 2)
        if status_visible:
            _files_index_set_state(
                running=False,
                finished_at=time.time(),
                phase="done",
                phase_message="Library artist refresh complete",
                current_folder=None,
                artists=artists_count,
                albums=albums_count,
                tracks=tracks_count,
                error=None,
            )
        logging.info(
            "Files library index upserted (%s, artist=%s): %d album(s), %d track row(s), %d embedding(s) in %.2fs (promoted covers=%d, promoted artist images=%d, cached covers=%d, artist images=%d)",
            reason,
            artist_name,
            len(albums_payload),
            total_tracks,
            embeddings_upserted,
            elapsed,
            covers_promoted,
            artists_promoted,
            covers_cached,
            artists_cached,
        )
        return {
            "ok": True,
            "artists": artists_count,
            "albums": albums_count,
            "tracks": tracks_count,
            "track_embeddings": embeddings_count,
            "cached_covers": covers_cached,
            "cached_artist_images": artists_cached,
            "duration_sec": elapsed,
            "source": "published_rows_artist_upsert",
        }
    except Exception as e:
        logging.exception("Files index artist upsert failed: %s", e)
        if "status_visible" not in locals() or status_visible:
            _files_index_set_state(
                running=False,
                finished_at=time.time(),
                phase="error",
                current_folder=None,
                error=str(e),
            )
        return {"ok": False, "error": str(e)}
    finally:
        files_index_lock.release()

def _rebuild_files_library_index_impl(reason: str = "manual", wait_if_running: bool = False) -> dict:
    if _get_library_mode() != "files":
        return {"ok": False, "error": "LIBRARY_MODE is not 'files'"}
    if not FILES_ROOTS:
        return {"ok": False, "error": "FILES_ROOTS is empty"}
    if not _files_pg_init_schema():
        return {"ok": False, "error": "PostgreSQL schema unavailable"}

    embed_build_lock = globals().get("_FILES_RECO_EMBED_BUILD_LOCK")
    if embed_build_lock is not None and embed_build_lock.locked():
        if not wait_if_running:
            return {"ok": False, "running": True, "error": "Files reco embedding rebuild already running"}
        logging.info("Files library index rebuild waiting for reco embedding backfill to finish")
        embed_build_lock.acquire()
        embed_build_lock.release()

    acquired = files_index_lock.acquire(blocking=wait_if_running)
    if not acquired:
        return {"ok": False, "running": True, "error": "Files index rebuild already running"}
    try:
        started_at = time.time()
        _pipeline_job_update(
            "library_index",
            status="running",
            phase="discovering",
            current=0,
            total=0,
            current_item="",
            message="Starting files library index rebuild",
            run_id=str(reason or "manual"),
            meta={"reason": str(reason or "manual")},
        )
        _files_index_set_state(
            running=True,
            started_at=started_at,
            finished_at=None,
            updated_at=started_at,
            phase="discovering",
            phase_started_at=started_at,
            phase_message="Scanning library folders",
            phase_progress=None,
            phase_eta_seconds=None,
            phase_rate_per_sec=None,
            current_folder=None,
            folders_processed=0,
            total_folders=0,
            collapsed_groups=0,
            entries_scanned=0,
            discovered_audio_files=0,
            artists=0,
            albums=0,
            tracks=0,
            error=None,
        )

        artists_map: dict[str, dict] = {}
        albums_payload: list[dict] = []
        reco_embeddings_count = 0
        published_artists, published_albums, published_count = _load_files_library_published_payload()
        with lock:
            files_scan_running = bool(state.get("scanning")) and _get_library_mode() == "files"
        (
            use_published_payload,
            payload_source,
            force_filesystem_source,
            force_reason,
        ) = _files_index_payload_source_decision(
            reason,
            published_count=published_count,
            files_scan_running=files_scan_running,
        )
        logging.info(
            "Files library index rebuild starting (%s): source=%s roots=%s force_filesystem=%s force_reason=%s published_rows=%s scan_running=%s",
            reason,
            payload_source,
            list(FILES_ROOTS or []),
            bool(force_filesystem_source),
            force_reason or "-",
            int(published_count or 0),
            bool(files_scan_running),
        )

        if use_published_payload:
            artists_map = published_artists
            albums_payload = published_albums
            _pipeline_job_update(
                "library_index",
                status="running",
                phase="published_rows",
                current=max(published_count, 0),
                total=max(published_count, 0),
                current_item="published_scan_rows",
                message="Reading published scan rows",
                run_id=str(reason or "manual"),
                meta={"reason": str(reason or "manual"), "source": payload_source},
            )
            _files_index_set_state(
                total_folders=max(published_count, 0),
                folders_processed=max(published_count, 0),
                phase="parsing",
                phase_message="Reading published scan rows",
                phase_progress=100.0,
                phase_eta_seconds=0,
                phase_rate_per_sec=None,
                current_folder="published_scan_rows",
                tracks=max(published_count, 0),
            )
        else:
            last_group_log = {"ts": 0.0, "audio": 0, "folders": 0}

            def _on_group_progress(payload: dict[str, Any]) -> None:
                try:
                    _files_index_set_state(
                        phase="discovering",
                        phase_message=(
                            f"Discovered {int(payload.get('folders_found') or 0):,} folder groups "
                            f"and {int(payload.get('files_found') or 0):,} audio files"
                        ),
                        current_folder=str(payload.get("root") or ""),
                        folders_processed=int(payload.get("folders_found") or 0),
                        total_folders=int(payload.get("folders_found") or 0),
                        entries_scanned=int(payload.get("entries_scanned") or 0),
                        discovered_audio_files=int(payload.get("files_found") or 0),
                        tracks=int(payload.get("files_found") or 0),
                        phase_progress=None,
                        phase_eta_seconds=None,
                        phase_rate_per_sec=None,
                    )
                except Exception:
                    pass
                now = time.time()
                audio_found = int(payload.get("files_found") or 0)
                folders_found = int(payload.get("folders_found") or 0)
                should_log = (
                    last_group_log["ts"] == 0.0
                    or (now - float(last_group_log["ts"] or 0.0)) >= 15.0
                    or (audio_found - int(last_group_log["audio"] or 0)) >= 50000
                    or (folders_found - int(last_group_log["folders"] or 0)) >= 5000
                )
                if should_log:
                    _pipeline_job_update(
                        "library_index",
                        status="running",
                        phase="discovering",
                        current=folders_found,
                        total=folders_found,
                        current_item=str(payload.get("root") or ""),
                        message=(
                            f"Discovered {folders_found:,} folder groups "
                            f"and {audio_found:,} audio files"
                        ),
                        run_id=str(reason or "manual"),
                        meta={
                            "reason": str(reason or "manual"),
                            "source": payload_source,
                            "entries_scanned": int(payload.get("entries_scanned") or 0),
                            "roots_done": int(payload.get("roots_done") or 0),
                            "roots_total": int(payload.get("roots_total") or len(FILES_ROOTS or [])),
                        },
                    )
                    logging.info(
                        "Files library index discovering source=%s root=%s roots=%d/%d visited=%d audio=%d folders=%d",
                        payload_source,
                        str(payload.get("root") or ""),
                        int(payload.get("roots_done") or 0),
                        int(payload.get("roots_total") or len(FILES_ROOTS or [])),
                        int(payload.get("entries_scanned") or 0),
                        audio_found,
                        folders_found,
                    )
                    last_group_log["ts"] = now
                    last_group_log["audio"] = audio_found
                    last_group_log["folders"] = folders_found

            by_folder = _group_audio_files_by_folder_under_roots(
                FILES_ROOTS,
                progress_cb=_on_group_progress,
                progress_every=500,
                heartbeat_seconds=10.0,
            )
            discovered_audio_files = sum(len(paths or []) for paths in by_folder.values())
            _files_index_set_state(
                phase="collapsing",
                phase_message=f"Grouping release folders from {len(by_folder):,} discovered album folder groups",
                current_folder="collapsing_release_segments",
                total_folders=len(by_folder),
                folders_processed=0,
                collapsed_groups=0,
                discovered_folder_groups=len(by_folder),
                collapse_parent_folders_processed=0,
                collapse_parent_folders_total=0,
                phase_item_done=0,
                phase_item_total=0,
                phase_item_label="parent folders checked",
                discovered_audio_files=discovered_audio_files,
                tracks=discovered_audio_files,
                phase_progress=0.0,
                phase_eta_seconds=None,
                phase_rate_per_sec=None,
            )
            logging.info(
                "Files library index collapsing %d discovered folder group(s) for source=%s",
                len(by_folder),
                payload_source,
            )
            collapse_started_at = time.time()
            collapse_last_state = {"ts": 0.0, "processed": 0, "collapsed": 0}

            def _on_collapse_progress(payload: dict[str, Any]) -> None:
                now = time.time()
                parent_raw = str(payload.get("parent") or "").strip()
                if not parent_raw and (now - float(collapse_last_state["ts"] or 0.0)) < 1.0:
                    return
                parents_processed = int(payload.get("parents_processed") or 0)
                parents_total = int(payload.get("parents_total") or len(by_folder))
                collapsed_groups = int(payload.get("collapsed_groups") or 0)
                phase_progress, phase_eta_seconds, phase_rate_per_sec = _files_index_progress_metrics(
                    parents_processed,
                    parents_total,
                    started_at=collapse_started_at,
                )
                collapse_elapsed = max(0.0, now - collapse_started_at)
                if parents_processed < 1000 or collapse_elapsed < 60.0:
                    phase_eta_seconds = None
                    phase_rate_per_sec = None
                try:
                    _files_index_set_state(
                        phase="collapsing",
                        phase_message=(
                            f"Checked {parents_processed:,} / {parents_total:,} parent folders for multi-disc grouping "
                            f"· {len(by_folder):,} discovered album folder groups · merged {collapsed_groups:,}"
                        ),
                        current_folder=parent_raw or "collapsing_release_segments",
                        folders_processed=len(by_folder),
                        total_folders=len(by_folder),
                        collapsed_groups=collapsed_groups,
                        discovered_folder_groups=len(by_folder),
                        collapse_parent_folders_processed=parents_processed,
                        collapse_parent_folders_total=parents_total,
                        phase_item_done=parents_processed,
                        phase_item_total=parents_total,
                        phase_item_label="parent folders checked",
                        tracks=discovered_audio_files,
                        phase_progress=phase_progress,
                        phase_eta_seconds=phase_eta_seconds,
                        phase_rate_per_sec=phase_rate_per_sec,
                    )
                except Exception:
                    pass
                should_log = (
                    collapse_last_state["ts"] == 0.0
                    or (now - float(collapse_last_state["ts"] or 0.0)) >= 15.0
                    or parents_processed == parents_total
                    or (parents_processed - int(collapse_last_state["processed"] or 0)) >= 5000
                    or (collapsed_groups - int(collapse_last_state["collapsed"] or 0)) >= 200
                )
                if should_log:
                    _pipeline_job_update(
                        "library_index",
                        status="running",
                        phase="collapsing",
                        current=parents_processed,
                        total=parents_total,
                        current_item=parent_raw or "collapsing_release_segments",
                        message=f"Checked {parents_processed:,} / {parents_total:,} parent folders",
                        run_id=str(reason or "manual"),
                        meta={
                            "reason": str(reason or "manual"),
                            "source": payload_source,
                            "collapsed_groups": collapsed_groups,
                        },
                    )
                    eta_label = (
                        f"{int(phase_eta_seconds // 60)}m {int(phase_eta_seconds % 60)}s"
                        if isinstance(phase_eta_seconds, int) and phase_eta_seconds > 0
                        else "warming_up"
                    )
                    logging.info(
                        "Files library index collapsing source=%s checked=%d/%d (%.2f%%) merged=%d rate=%.2f groups/s eta=%s current=%s",
                        payload_source,
                        parents_processed,
                        parents_total,
                        float(phase_progress or 0.0),
                        collapsed_groups,
                        float(phase_rate_per_sec or 0.0),
                        eta_label,
                        parent_raw or "collapsing_release_segments",
                    )
                    collapse_last_state["ts"] = now
                    collapse_last_state["processed"] = parents_processed
                    collapse_last_state["collapsed"] = collapsed_groups

            by_folder = _collapse_nested_album_folder_groups(
                by_folder,
                root_dirs=_files_root_dir_strings(),
                progress_cb=_on_collapse_progress,
            )
            folders = sorted(by_folder.items(), key=lambda x: str(x[0]).lower())
            logging.info(
                "Files library index discovered %d folder(s) under %d root(s) for source=%s",
                len(folders),
                len(FILES_ROOTS or []),
                payload_source,
            )
            parsing_started_at = time.time()
            _files_index_set_state(
                total_folders=len(folders),
                folders_processed=0,
                phase="parsing",
                phase_message=f"Reading 0 / {len(folders):,} normalized album folders",
                phase_progress=0.0,
                phase_eta_seconds=None,
                phase_rate_per_sec=None,
            )
            root_dirs = _files_root_dir_strings()

            for idx, (folder, files) in enumerate(folders, start=1):
                phase_progress, phase_eta_seconds, phase_rate_per_sec = _files_index_progress_metrics(
                    idx,
                    len(folders),
                    started_at=parsing_started_at,
                )
                _files_index_set_state(
                    folders_processed=idx,
                    current_folder=str(folder),
                    phase_message=f"Reading {idx:,} / {len(folders):,} normalized album folders",
                    phase_progress=phase_progress,
                    phase_eta_seconds=phase_eta_seconds,
                    phase_rate_per_sec=phase_rate_per_sec,
                )
                if idx == 1 or idx == len(folders) or (idx % 250) == 0:
                    _pipeline_job_update(
                        "library_index",
                        status="running",
                        phase="parsing",
                        current=idx,
                        total=len(folders),
                        current_item=str(folder),
                        message=f"Reading {idx:,} / {len(folders):,} normalized album folders",
                        run_id=str(reason or "manual"),
                        meta={"reason": str(reason or "manual"), "source": payload_source},
                    )
                    logging.info(
                        "Files library index parsing %d/%d folder(s) for source=%s: %s",
                        idx,
                        len(folders),
                        payload_source,
                        str(folder),
                    )
                if not files:
                    continue

                track_entries: list[dict] = []
                tag_dicts: list[dict] = []
                raw_genres: list[str] = []
                fmt_counts: dict[str, int] = defaultdict(int)
                first_tags: dict = {}
                total_duration_sec = 0

                for p in files:
                    tags = extract_tags(p) or {}
                    tag_dicts.append(tags)
                    if not first_tags:
                        first_tags = tags
                    title = (
                        (tags.get("title") or tags.get("name") or p.stem or "").strip()
                        or p.stem
                        or "Unknown Track"
                    )
                    disc_num = _parse_int_loose(tags.get("disc") or tags.get("discnumber"), 1) or 1
                    track_num = _parse_int_loose(tags.get("track") or tags.get("tracknumber"), 0)
                    duration_sec = int(max(0.0, _parse_duration_seconds_loose(tags.get("duration"), 0.0)))
                    if duration_sec <= 0:
                        duration_sec = int(max(0, _run_ffprobe_duration_sec(str(p))))
                    bitrate = _parse_int_loose(tags.get("bitrate") or tags.get("bit_rate"), 0)
                    sample_rate = _parse_int_loose(tags.get("sample_rate") or tags.get("samplerate"), 0)
                    bit_depth = _parse_int_loose(tags.get("bit_depth") or tags.get("bits_per_sample"), 0)
                    fmt = (p.suffix.lower().lstrip(".") or "UNKNOWN").upper()
                    fmt_counts[fmt] += 1
                    total_duration_sec += duration_sec
                    raw_genres.extend(_split_genre_values(tags.get("genre") or ""))
                    try:
                        file_size = int(p.stat().st_size)
                    except OSError:
                        file_size = 0
                    track_entries.append(
                        {
                            "file_path": str(p),
                            "title": title,
                            "disc_num": disc_num,
                            "track_num": track_num,
                            "duration_sec": duration_sec,
                            "format": fmt,
                            "bitrate": bitrate,
                            "sample_rate": sample_rate,
                            "bit_depth": bit_depth,
                            "file_size_bytes": file_size,
                            "primary_tags_json": json.dumps(tags or {}, default=str),
                        }
                    )

                track_entries.sort(key=lambda t: (t["disc_num"], t["track_num"], t["file_path"]))
                if not track_entries:
                    continue

                artist_name = _pick_album_artist_from_tag_dicts(tag_dicts, default="Unknown Artist")
                album_title = _pick_album_title_from_tag_dicts(
                    tag_dicts,
                    fallback=folder.name.replace("_", " ").strip() or "Unknown Album",
                )
                if _folder_has_release_segment_children(folder, files):
                    inferred_artist_name, inferred_album_title = _infer_artist_album_from_folder(folder, list(files or []))
                    if inferred_artist_name and (
                        (artist_name or "").strip().lower() in {"unknown", "unknown artist", "various", "various artists"}
                        or not _identity_artist_fallback_is_usable(artist_name)
                    ):
                        artist_name = inferred_artist_name
                    if inferred_album_title:
                        album_title = _sanitize_album_title_display(inferred_album_title)
                elif (
                    not _identity_artist_fallback_is_usable(artist_name)
                    or not bool(_normalize_meta_text(first_tags.get("album")))
                    or album_title == _sanitize_album_title_display(folder.name.replace("_", " ").strip() or "Unknown Album")
                ):
                    try:
                        inferred_artist_name, inferred_album_title = _infer_artist_album_from_folder(folder, list(files or []))
                    except Exception:
                        inferred_artist_name, inferred_album_title = ("", "")
                    if _identity_artist_fallback_is_usable(inferred_artist_name):
                        artist_name = inferred_artist_name
                    if inferred_album_title and (
                        not bool(_normalize_meta_text(first_tags.get("album")))
                        or album_title == _sanitize_album_title_display(folder.name.replace("_", " ").strip() or "Unknown Album")
                    ):
                        album_title = _sanitize_album_title_display(inferred_album_title)
                label = _pick_album_label_from_tag_dicts(tag_dicts)
                artist_norm = _norm_artist_key(artist_name) or "unknown artist"
                title_norm = norm_album_for_dedup(album_title, normalize_parenthetical=True)
                date_text = (first_tags.get("date") or first_tags.get("year") or "").strip()
                year = _parse_int_loose((date_text[:4] if date_text else first_tags.get("year")), 0) or None
                dominant_format = max(fmt_counts.items(), key=lambda x: x[1])[0] if fmt_counts else "UNKNOWN"
                is_lossless = dominant_format in _LOSSLESS_FORMATS
                cover_path = _first_cover_path(folder)
                has_cover = bool(cover_path and cover_path.is_file()) or album_folder_has_cover(folder)
                artist_folder = _files_guess_artist_folder(folder, artist_name, root_dirs=root_dirs)
                artist_image_path = _first_artist_image_path(artist_folder) if artist_folder else None
                artist_has_image = bool(artist_image_path and artist_image_path.is_file())
                if artist_norm not in artists_map:
                    artists_map[artist_norm] = {
                        "name": artist_name,
                        "image_path": str(artist_image_path) if artist_image_path else None,
                        "has_image": artist_has_image,
                    }
                else:
                    artists_map[artist_norm]["name"] = _choose_preferred_identity_display(
                        str(artists_map[artist_norm].get("name") or ""),
                        artist_name,
                    )
                    if artist_has_image and not artists_map[artist_norm].get("has_image"):
                        artists_map[artist_norm]["image_path"] = str(artist_image_path)
                        artists_map[artist_norm]["has_image"] = True

                indices = [t["track_num"] for t in track_entries if t["track_num"] > 0]
                actual_track_count = len(track_entries)
                is_broken = False
                expected_track_count = None
                missing_indices: list[int] = []
                if indices:
                    # Track numbers can be garbage (e.g. "745" on a 15-track album). In that case
                    # gaps-based "broken" detection produces massive false positives and UI junk.
                    max_idx = max(indices)
                    coverage = (actual_track_count / max_idx) if max_idx else 1.0
                    if max_idx > max(120, actual_track_count * 3) and coverage < 0.5:
                        is_broken = False
                        expected_track_count = None
                        missing_indices = []
                    else:
                        is_broken, _actual_count_from_indices, gaps = _detect_gaps_in_indices(indices)
                        if is_broken and _classical_gap_anomaly_should_be_ignored(
                            first_tags,
                            actual_count=actual_track_count,
                            max_idx=max_idx,
                            gaps=gaps,
                        ):
                            is_broken = False
                            gaps = []
                        if is_broken:
                            expected_track_count = max_idx
                            for start_i, end_i in gaps:
                                # Defensive cap to avoid pathological arrays.
                                if (end_i - start_i) > 2000:
                                    continue
                                missing_indices.extend(list(range(start_i + 1, end_i)))
                                if len(missing_indices) > 5000:
                                    missing_indices = missing_indices[:5000]
                                    break

                inferred_genre = _infer_genre_from_bandcamp_tags(raw_genres) if raw_genres else None
                identity_fields = _extract_files_identity_fields(tags=first_tags, edition={}, cached={})
                mbid = identity_fields["musicbrainz_id"]
                missing_required = _check_required_tags(
                    first_tags or {},
                    REQUIRED_TAGS,
                    edition={"tracks": [{"title": t.get("title"), "index": t.get("track_num")} for t in track_entries]},
                )

                albums_payload.append(
                    {
                        "artist_norm": artist_norm,
                        "artist_name": artist_name,
                        "title": album_title,
                        "title_norm": title_norm,
                        "folder_path": str(folder),
                        "year": year,
                        "date_text": date_text[:32] if date_text else "",
                        "genre": inferred_genre or "",
                        "label": (label or "").strip(),
                        "tags_json": json.dumps(raw_genres[:20]),
                        "format": dominant_format,
                        "is_lossless": bool(is_lossless),
                        "has_cover": bool(has_cover),
                        "cover_path": str(cover_path) if cover_path else "",
                        "mb_identified": bool(identity_fields["has_identity"]),
                        "strict_match_verified": bool(identity_fields.get("strict_match_verified")),
                        "strict_match_provider": identity_fields.get("strict_match_provider") or "",
                        "strict_reject_reason": identity_fields.get("strict_reject_reason") or "",
                        "strict_tracklist_score": float(identity_fields.get("strict_tracklist_score") or 0.0),
                        "musicbrainz_release_group_id": mbid,
                        "track_count": actual_track_count,
                        "total_duration_sec": total_duration_sec,
                        "is_broken": bool(is_broken),
                        "expected_track_count": expected_track_count,
                        "actual_track_count": actual_track_count,
                        "missing_indices_json": json.dumps(missing_indices),
                        "missing_required_tags_json": json.dumps(missing_required),
                        "primary_tags_json": json.dumps(first_tags or {}),
                        "tracks": track_entries,
                        "discogs_release_id": identity_fields["discogs_release_id"],
                        "lastfm_album_mbid": identity_fields["lastfm_album_mbid"],
                        "bandcamp_album_url": identity_fields["bandcamp_album_url"],
                        "metadata_source": identity_fields["identity_provider"] or identity_fields["metadata_source"],
                    }
                )

        # Fill missing genres from dominant artist genres (when available), and keep
        # missing-required tags aligned with non-blocking genre behavior.
        _apply_genre_defaults_to_albums_payload(albums_payload)
        _pipeline_job_update(
            "library_index",
            status="running",
            phase="entity_resolution",
            current=len(albums_payload),
            total=len(albums_payload),
            current_item="artist_album_links",
            message=f"Building artist links for {len(albums_payload):,} published albums",
            run_id=str(reason or "manual"),
            meta={"reason": str(reason or "manual"), "source": payload_source, "artists": len(artists_map)},
        )
        _files_index_set_state(
            phase="entity_resolution",
            phase_message=f"Building artist links for {len(albums_payload):,} published albums",
            phase_progress=72.0,
            phase_eta_seconds=None,
            phase_rate_per_sec=None,
            artists=len(artists_map),
            albums=len(albums_payload),
            tracks=sum(int(album.get("track_count") or 0) for album in albums_payload),
        )
        artists_map, album_links_by_folder = _build_files_browse_artist_entities(artists_map, albums_payload)
        artists_map, album_links_by_folder, repaired_primary_links = _ensure_files_album_primary_links(
            artists_map,
            albums_payload,
            album_links_by_folder,
        )
        if repaired_primary_links:
            logging.info(
                "Files library index repaired %d published album(s) with missing primary artist links",
                repaired_primary_links,
            )
        for album in albums_payload:
            folder_key = str(album.get("folder_path") or "").strip()
            primary_link = next((link for link in (album_links_by_folder.get(folder_key) or []) if bool(link.get("is_primary"))), None)
            if primary_link and str(primary_link.get("artist_norm") or "").strip():
                album["artist_norm"] = str(primary_link.get("artist_norm") or "").strip()

        _files_index_set_state(
            phase="media_prepare",
            phase_message="Preparing artwork and cached media",
            phase_progress=76.0,
            phase_eta_seconds=None,
            phase_rate_per_sec=None,
        )
        _pipeline_job_update(
            "media_cache",
            status="running",
            phase="media_prepare",
            current=0,
            total=len(albums_payload),
            current_item="artwork",
            message="Preparing artwork and cached media",
            run_id=str(reason or "manual"),
            meta={"source": payload_source, "blocking": False},
        )
        storage_media_scope = _storage_background_filesystem_scope()
        filesystem_media_allowed = payload_source != "published_rows"
        if filesystem_media_allowed and bool(storage_media_scope.get("enabled")) and not bool(storage_media_scope.get("scan_active")):
            filesystem_media_allowed = False
            logging.info(
                "[STORAGE] Files library index deferring filesystem media prepare for reason=%s: "
                "power saver is enabled and no bounded disk bucket is active.",
                reason,
            )
        blocking_media_prepare = bool(
            filesystem_media_allowed
            and (
                _FILES_BLOCKING_MEDIA_PREP_MAX_ALBUMS <= 0
                or len(albums_payload or []) <= _FILES_BLOCKING_MEDIA_PREP_MAX_ALBUMS
            )
        )
        if filesystem_media_allowed and not blocking_media_prepare:
            logging.info(
                "Files library index using non-blocking media prepare for %d album(s): "
                "skipping embedded cover extraction and new cache generation before DB swap "
                "(threshold=%d)",
                len(albums_payload or []),
                _FILES_BLOCKING_MEDIA_PREP_MAX_ALBUMS,
            )
            _files_index_set_state(
                phase_message=(
                    f"Publishing metadata first; deferring artwork cache for {len(albums_payload or []):,} albums"
                ),
                phase_item_done=0,
                phase_item_total=len(albums_payload or []),
                phase_item_label="albums prepared",
            )
        covers_promoted, artists_promoted = _promote_files_media_paths_to_cache(
            artists_map,
            albums_payload,
            filesystem_allowed=filesystem_media_allowed,
            allow_embedded_cover_extract=blocking_media_prepare,
            generate_missing_cache=blocking_media_prepare,
        )

        _files_index_set_state(
            phase="writing",
            phase_message="Updating the library database",
            phase_progress=86.0,
            phase_eta_seconds=None,
            phase_rate_per_sec=None,
            artists=len(artists_map),
            albums=len(albums_payload),
        )
        _pipeline_job_update(
            "library_index",
            status="running",
            phase="writing",
            current=0,
            total=len(albums_payload),
            current_item="files_albums",
            message="Updating the library database",
            run_id=str(reason or "manual"),
            meta={"reason": str(reason or "manual"), "source": payload_source, "artists": len(artists_map)},
        )

        def _write_full_index_once(
            conn: Any,
            *,
            attempt_artists_map: dict[str, dict[str, Any]],
            attempt_albums_payload: list[dict[str, Any]],
            attempt_album_links_by_folder: dict[str, list[dict[str, Any]]],
        ) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], dict[str, list[dict[str, Any]]], int]:
            total_tracks = 0
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout = 0")
                    cur.execute("SET LOCAL idle_in_transaction_session_timeout = 0")
                    _files_reset_rebuild_tables(cur)
                    ext_artist_images: dict[str, str] = {}
                    try:
                        norms = list(attempt_artists_map.keys())
                        if norms:
                            cur.execute(
                                """
                                SELECT name_norm, COALESCE(image_path, '')
                                FROM files_external_artist_images
                                WHERE name_norm = ANY(%s)
                                """,
                                (norms,),
                            )
                            for n, p in cur.fetchall():
                                nkey = str(n or "").strip()
                                pval = str(p or "").strip()
                                if nkey and pval:
                                    ext_artist_images[nkey] = pval
                    except Exception:
                        ext_artist_images = {}
                    artist_rows = [
                        (
                            data["name"],
                            norm,
                            str(data.get("canonical_name") or data["name"]),
                            str(data.get("canonical_name_norm") or norm),
                            str(data.get("canonical_mbid") or ""),
                            str(data.get("entity_kind") or "artist"),
                            str(data.get("roles_json") or "[]"),
                            str(data.get("aliases_json") or "[]"),
                            bool(data["has_image"]) or bool(ext_artist_images.get(norm)),
                            (data.get("image_path") or "").strip() or ext_artist_images.get(norm) or "",
                        )
                        for norm, data in attempt_artists_map.items()
                    ]
                    if artist_rows:
                        cur.executemany(
                            """
                            INSERT INTO files_artists (name, name_norm, canonical_name, canonical_name_norm, canonical_mbid, entity_kind, roles_json, aliases_json, has_image, image_path, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                            """,
                            artist_rows,
                        )
                    fast_published_rebuild = bool(
                        payload_source == "published_rows"
                        and len(attempt_albums_payload or []) >= int(os.getenv("FILES_FAST_PUBLISHED_REBUILD_MIN_ALBUMS", "10000") or 10000)
                    )
                    if fast_published_rebuild:
                        logging.info(
                            "Files library index fast published rebuild: deferring artist alias sync and person merge for %d album(s)",
                            len(attempt_albums_payload or []),
                        )
                    else:
                        _files_sync_artist_aliases(conn, artists_map=attempt_artists_map)
                        _files_merge_duplicate_person_artists(conn)
                        attempt_artists_map, attempt_album_links_by_folder, _resolved_artist_norm_map = _files_apply_canonical_artist_resolution(
                            conn,
                            attempt_artists_map,
                            albums_payload=attempt_albums_payload,
                            album_links_by_folder=attempt_album_links_by_folder,
                        )
                    for album in attempt_albums_payload:
                        folder_key = str(album.get("folder_path") or "").strip()
                        primary_link = next(
                            (link for link in (attempt_album_links_by_folder.get(folder_key) or []) if bool(link.get("is_primary"))),
                            None,
                        )
                        artist_norm = str((primary_link or {}).get("artist_norm") or "").strip()
                        if artist_norm:
                            album["artist_norm"] = artist_norm
                    cur.execute("SELECT id, name_norm FROM files_artists")
                    artist_id_by_norm = {str(r[1]): int(r[0]) for r in cur.fetchall()}

                    album_rows = []
                    for album in attempt_albums_payload:
                        artist_id = artist_id_by_norm.get(album["artist_norm"])
                        if not artist_id:
                            continue
                        album_sample_rate, album_bit_depth = _representative_album_audio_profile(album.get("tracks") or [])
                        album_rows.append(
                            (
                                artist_id,
                                album["title"],
                                album["title_norm"],
                                album["folder_path"],
                                album["year"],
                                album["date_text"],
                                album["genre"],
                                album.get("label") or "",
                                album["tags_json"],
                                album["format"],
                                bool(album["is_lossless"]),
                                album_sample_rate,
                                album_bit_depth,
                                bool(album["has_cover"]),
                                album["cover_path"],
                                bool(album["mb_identified"]),
                                bool(album.get("strict_match_verified")),
                                _normalize_identity_provider(str(album.get("strict_match_provider") or "")),
                                str(album.get("strict_reject_reason") or "").strip(),
                                float(album.get("strict_tracklist_score") or 0.0),
                                album["musicbrainz_release_group_id"],
                                album.get("musicbrainz_release_id") or "",
                                album.get("discogs_release_id") or "",
                                album.get("lastfm_album_mbid") or "",
                                album.get("bandcamp_album_url") or "",
                                album.get("metadata_source") or "",
                                album["track_count"],
                                album["total_duration_sec"],
                                bool(album["is_broken"]),
                                album["expected_track_count"],
                                album["actual_track_count"],
                                album["missing_indices_json"],
                                album["missing_required_tags_json"],
                                album["primary_tags_json"],
                            )
                        )
                    if album_rows:
                        cur.executemany(
                            """
                            INSERT INTO files_albums (
                                artist_id, title, title_norm, folder_path, year, date_text, genre, label, tags_json,
                                format, is_lossless, sample_rate, bit_depth, has_cover, cover_path, mb_identified,
                                strict_match_verified, strict_match_provider, strict_reject_reason, strict_tracklist_score,
                                musicbrainz_release_group_id, musicbrainz_release_id,
                                discogs_release_id, lastfm_album_mbid, bandcamp_album_url, metadata_source,
                                track_count, total_duration_sec, is_broken, expected_track_count, actual_track_count,
                                missing_indices_json, missing_required_tags_json, primary_tags_json,
                                created_at, updated_at
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s,
                                %s, %s, %s,
                                NOW(), NOW()
                            )
                            """,
                            album_rows,
                        )
                    cur.execute("SELECT id, folder_path FROM files_albums")
                    album_id_by_folder = {str(r[1]): int(r[0]) for r in cur.fetchall()}
                    link_rows: list[tuple[int, int, str, bool]] = []
                    for album in attempt_albums_payload:
                        album_id = album_id_by_folder.get(str(album.get("folder_path") or ""))
                        if not album_id:
                            continue
                        for link in (attempt_album_links_by_folder.get(str(album.get("folder_path") or "").strip()) or []):
                            artist_id = artist_id_by_norm.get(str(link.get("artist_norm") or "").strip())
                            if not artist_id:
                                continue
                            role = str(link.get("role") or "artist").strip().lower() or "artist"
                            link_rows.append((artist_id, int(album_id), role, bool(link.get("is_primary"))))
                    link_rows = _dedupe_files_artist_album_link_rows(link_rows)
                    if link_rows:
                        cur.executemany(
                            """
                            INSERT INTO files_artist_album_links(artist_id, album_id, role, is_primary, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, NOW(), NOW())
                            ON CONFLICT (artist_id, album_id, role) DO UPDATE
                            SET is_primary = EXCLUDED.is_primary,
                                updated_at = NOW()
                            """,
                            link_rows,
                        )

                    for album in attempt_albums_payload:
                        album_id = album_id_by_folder.get(album["folder_path"])
                        if not album_id:
                            continue
                        track_by_path: dict[str, tuple] = {}
                        for t in album["tracks"]:
                            fp = str(t.get("file_path") or "").strip()
                            if not fp:
                                continue
                            if fp in track_by_path:
                                continue
                            disc_num = max(1, _clamp_int(t.get("disc_num"), 1, _PG_INT4_MIN, _PG_INT4_MAX))
                            track_num = max(0, _clamp_int(t.get("track_num"), 0, _PG_INT4_MIN, _PG_INT4_MAX))
                            duration_sec = max(0, _clamp_int(t.get("duration_sec"), 0, _PG_INT4_MIN, _PG_INT4_MAX))
                            bitrate = max(0, _clamp_int(t.get("bitrate"), 0, _PG_INT4_MIN, _PG_INT4_MAX))
                            sample_rate = max(0, _clamp_int(t.get("sample_rate"), 0, _PG_INT4_MIN, _PG_INT4_MAX))
                            bit_depth = max(0, _clamp_int(t.get("bit_depth"), 0, _PG_INT4_MIN, _PG_INT4_MAX))
                            file_size_bytes = max(0, _clamp_int(t.get("file_size_bytes"), 0, _PG_INT8_MIN, _PG_INT8_MAX))
                            track_by_path[fp] = (
                                album_id,
                                fp,
                                t.get("title") or "",
                                disc_num,
                                track_num,
                                duration_sec,
                                t.get("format") or "",
                                bitrate,
                                sample_rate,
                                bit_depth,
                                file_size_bytes,
                                str(t.get("primary_tags_json") or "{}"),
                            )
                        track_rows = list(track_by_path.values())
                        if track_rows:
                            cur.executemany(
                                """
                                INSERT INTO files_tracks (
                                    album_id, file_path, title, disc_num, track_num, duration_sec, format,
                                    bitrate, sample_rate, bit_depth, file_size_bytes, primary_tags_json, created_at, updated_at
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, NOW(), NOW()
                                )
                                ON CONFLICT (file_path) DO UPDATE
                                SET album_id = EXCLUDED.album_id,
                                    title = EXCLUDED.title,
                                    disc_num = EXCLUDED.disc_num,
                                    track_num = EXCLUDED.track_num,
                                    duration_sec = EXCLUDED.duration_sec,
                                    format = EXCLUDED.format,
                                    bitrate = EXCLUDED.bitrate,
                                    sample_rate = EXCLUDED.sample_rate,
                                    bit_depth = EXCLUDED.bit_depth,
                                    file_size_bytes = EXCLUDED.file_size_bytes,
                                    primary_tags_json = EXCLUDED.primary_tags_json,
                                    updated_at = NOW()
                                """,
                                track_rows,
                            )
                            total_tracks += len(track_rows)

                    cur.execute("""
                        UPDATE files_artists a
                        SET album_count = s.album_count,
                            track_count = s.track_count,
                            broken_albums_count = s.broken_albums_count,
                            updated_at = NOW()
                        FROM (
                            SELECT
                                artist_id,
                                COUNT(*) AS album_count,
                                COALESCE(SUM(track_count), 0) AS track_count,
                                COALESCE(SUM(CASE WHEN is_broken THEN 1 ELSE 0 END), 0) AS broken_albums_count
                            FROM (
                                SELECT DISTINCT
                                    link.artist_id,
                                    link.album_id,
                                    alb.track_count,
                                    alb.is_broken
                                FROM files_artist_album_links link
                                JOIN files_albums alb ON alb.id = link.album_id
                            ) artist_album_rollup
                            GROUP BY artist_id
                        ) s
                        WHERE a.id = s.artist_id
                    """)
                    _files_index_write_meta(cur, "last_reason", reason)
                    _files_index_write_meta(cur, "last_build_ts", str(int(time.time())))
                    cur.execute("SELECT COUNT(*) FROM files_artists")
                    db_artists_count = int((cur.fetchone() or [0])[0] or 0)
                    cur.execute("SELECT COUNT(*) FROM files_albums")
                    db_albums_count = int((cur.fetchone() or [0])[0] or 0)
                    cur.execute("SELECT COUNT(*) FROM files_tracks")
                    db_tracks_count = int((cur.fetchone() or [0])[0] or 0)
                    _files_index_write_meta(cur, "artists", str(db_artists_count))
                    _files_index_write_meta(cur, "albums", str(db_albums_count))
                    _files_index_write_meta(cur, "tracks", str(db_tracks_count))
                    _files_index_write_meta(cur, "source", payload_source)
                    _files_index_write_meta(cur, "full_source", payload_source)
                    _files_index_write_meta(cur, "full_reason", reason)
                    _files_index_write_meta(cur, "full_build_ts", str(int(time.time())))
            return attempt_artists_map, attempt_albums_payload, attempt_album_links_by_folder, total_tracks

        write_error: Exception | None = None
        total_tracks = 0
        for attempt in (1, 2):
            conn = _files_pg_connect()
            if conn is None:
                raise RuntimeError("PostgreSQL connection unavailable during rebuild")
            try:
                attempt_artists_map = copy.deepcopy(artists_map)
                attempt_albums_payload = copy.deepcopy(albums_payload)
                attempt_album_links_by_folder = copy.deepcopy(album_links_by_folder)
                artists_map, albums_payload, album_links_by_folder, total_tracks = _write_full_index_once(
                    conn,
                    attempt_artists_map=attempt_artists_map,
                    attempt_albums_payload=attempt_albums_payload,
                    attempt_album_links_by_folder=attempt_album_links_by_folder,
                )
                write_error = None
                break
            except Exception as exc:
                write_error = exc
                if attempt < 2 and _files_pg_is_connection_dropped_error(exc):
                    logging.warning(
                        "[Files Index] !⚠ Full rebuild lost PostgreSQL connection reason=%s attempt=%d/2; retrying once: %s",
                        reason,
                        attempt,
                        _files_pg_error_text(exc),
                    )
                    _files_pg_reap_stale_connections(log_reason="files-rebuild-retry")
                    time.sleep(0.25)
                    continue
                raise
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        if write_error is not None:
            raise write_error

        promote_conn = _files_pg_connect()
        if promote_conn is not None:
            try:
                with promote_conn.transaction():
                    _files_promote_artist_alias_cache(promote_conn, artists_map)
            except Exception:
                logging.debug("Artist alias cache promotion failed after files rebuild", exc_info=True)
            finally:
                try:
                    promote_conn.close()
                except Exception:
                    pass

        _tracks_count_for_embeddings, reco_embeddings_count = _files_index_read_track_and_embedding_counts()
        sync_embeddings_now = bool(
            _FILES_BLOCKING_RECO_EMBED_MAX_TRACKS > 0
            and int(_tracks_count_for_embeddings or 0) <= _FILES_BLOCKING_RECO_EMBED_MAX_TRACKS
        )
        if sync_embeddings_now:
            _files_index_set_state(
                phase="embeddings",
                phase_message="Refreshing recommendations index",
                phase_progress=92.0,
                phase_eta_seconds=None,
                phase_rate_per_sec=None,
            )
            _pipeline_job_update(
                "embeddings",
                status="running",
                phase="rebuild",
                current=0,
                total=int(_tracks_count_for_embeddings or 0),
                current_item="track_embeddings",
                message="Refreshing recommendations index",
                run_id=str(reason or "manual"),
                meta={"source": payload_source},
            )
            embed_conn = _files_pg_connect()
            if embed_conn is None:
                raise RuntimeError("PostgreSQL connection unavailable during embedding rebuild")
            try:
                reco_embeddings_count = _reco_build_track_embeddings_chunked(embed_conn)
                with embed_conn.transaction():
                    with embed_conn.cursor() as cur:
                        _files_index_write_meta(cur, "track_embeddings", str(reco_embeddings_count))
                        _files_index_write_meta(cur, "track_embeddings_source", RECO_EMBED_SOURCE)
                        _files_index_write_meta(cur, "track_embeddings_reason", f"after_files_index_rebuild:{reason}")
                        _files_index_write_meta(cur, "track_embeddings_ts", str(int(time.time())))
            finally:
                embed_conn.close()
        else:
            queued_embeddings = _enqueue_files_reco_embedding_backfill(
                reason=f"after_files_index_rebuild:{reason}"
            )
            _pipeline_job_update(
                "embeddings",
                status="running" if queued_embeddings else "idle",
                phase="queued" if queued_embeddings else "deferred",
                current=int(reco_embeddings_count or 0),
                total=int(_tracks_count_for_embeddings or 0),
                current_item="track_embeddings",
                message="Recommendation embedding backfill queued" if queued_embeddings else "Recommendation embedding backfill deferred",
                run_id=str(reason or "manual"),
                meta={"source": payload_source, "queued": bool(queued_embeddings)},
            )
            logging.info(
                "Files library index published before reco embeddings; queued_background_embeddings=%s tracks=%d current_embeddings=%d threshold=%d",
                bool(queued_embeddings),
                int(_tracks_count_for_embeddings or 0),
                int(reco_embeddings_count or 0),
                int(_FILES_BLOCKING_RECO_EMBED_MAX_TRACKS or 0),
            )

        _files_index_set_state(
            phase="artist_enrichment",
            phase_message="Deferring artist pages to post-publish backfill",
            phase_progress=96.0,
            phase_eta_seconds=None,
            phase_rate_per_sec=None,
            current_artist=None,
            current_folder=None,
            phase_item_done=0,
            phase_item_total=0,
            phase_item_label="artist pages",
        )
        logging.info("Files library index deferring blocking artist enrichment until after publication")
        _files_index_set_state(
            phase="media_cache",
            phase_message="Finalizing media cache",
            phase_progress=99.0,
            phase_eta_seconds=None,
            phase_rate_per_sec=None,
            current_artist=None,
            current_folder=None,
            phase_item_done=None,
            phase_item_total=None,
            phase_item_label=None,
        )
        if filesystem_media_allowed and blocking_media_prepare:
            covers_cached, artists_cached = _precache_files_media_assets(
                artists_map,
                albums_payload,
                cache_only=False,
            )
            _pipeline_job_update(
                "media_cache",
                status="completed",
                phase="done",
                current=len(albums_payload),
                total=len(albums_payload),
                current_item="artwork",
                message="Media cache finalized",
                run_id=str(reason or "manual"),
                meta={"covers_cached": covers_cached, "artist_images_cached": artists_cached},
                finished=True,
            )
        else:
            covers_cached = 0
            artists_cached = 0
            _pipeline_job_update(
                "media_cache",
                status="completed",
                phase="deferred",
                current=0,
                total=len(albums_payload),
                current_item="artwork",
                message="Blocking media cache deferred after publication",
                run_id=str(reason or "manual"),
                meta={"source": payload_source, "blocking": False},
                finished=True,
            )
            if filesystem_media_allowed:
                logging.info(
                    "Files library index skipped blocking media-cache precache for large filesystem rebuild (%d album(s), threshold=%d)",
                    len(albums_payload),
                    _FILES_BLOCKING_MEDIA_PREP_MAX_ALBUMS,
                )
            else:
                logging.info(
                    "Files library index skipped blocking media-cache precache for published-row rebuild (%d album(s))",
                    len(albums_payload),
                )
        _files_cache_invalidate_all()
        artists_count, albums_count, tracks_count = _files_index_read_counts()
        elapsed = round(time.time() - started_at, 2)
        _files_index_set_state(
            running=False,
            finished_at=time.time(),
            phase="done",
            phase_message="Library rebuild complete",
            phase_progress=100.0,
            phase_eta_seconds=0,
            phase_rate_per_sec=None,
            current_folder=None,
            artists=artists_count,
            albums=albums_count,
            tracks=tracks_count,
            error=None,
        )
        logging.info(
            "Files library index rebuilt (%s, source=%s): %d artist(s), %d album(s), %d track(s), %d embedding(s) in %.2fs (promoted covers=%d, promoted artist images=%d, cached covers=%d, artist images=%d)",
            reason,
            payload_source,
            artists_count,
            albums_count,
            tracks_count,
            reco_embeddings_count,
            elapsed,
            covers_promoted,
            artists_promoted,
            covers_cached,
            artists_cached,
        )
        _pipeline_job_update(
            "library_index",
            status="completed",
            phase="done",
            current=int(albums_count or 0),
            total=int(albums_count or 0),
            current_item="",
            message="Files library index rebuild complete",
            run_id=str(reason or "manual"),
            meta={
                "reason": str(reason or "manual"),
                "source": payload_source,
                "artists": int(artists_count or 0),
                "albums": int(albums_count or 0),
                "tracks": int(tracks_count or 0),
                "track_embeddings": int(reco_embeddings_count or 0),
                "duration_sec": elapsed,
            },
            finished=True,
        )
        if sync_embeddings_now:
            _pipeline_job_update(
                "embeddings",
                status="completed",
                phase="done",
                current=int(reco_embeddings_count or 0),
                total=int(_tracks_count_for_embeddings or 0),
                current_item="track_embeddings",
                message="Recommendation embeddings refreshed",
                run_id=str(reason or "manual"),
                meta={"source": payload_source, "track_embeddings": int(reco_embeddings_count or 0)},
                finished=True,
            )
        # Post-build: backfill artist profiles (bios/tags/similar) and cache external images,
        # so browsing shows rich pages (no placeholders) without waiting for first click.
        try:
            if bool(_FILES_PROFILE_BACKFILL_ON_REBUILD):
                spawned = _trigger_files_profile_backfill_async(reason=f"after_files_index_rebuild:{reason}")
                logging.info(
                    "Files library index queued post-publish artist backfill spawned=%s reason=%s",
                    bool(spawned),
                    reason,
                )
        except Exception:
            logging.debug("Failed to queue post-publish files profile backfill", exc_info=True)
        return {
            "ok": True,
            "artists": artists_count,
            "albums": albums_count,
            "tracks": tracks_count,
            "track_embeddings": reco_embeddings_count,
            "cached_covers": covers_cached,
            "cached_artist_images": artists_cached,
            "duration_sec": elapsed,
            "source": payload_source,
        }
    except Exception as e:
        logging.exception("Files index rebuild failed: %s", e)
        _pipeline_job_update(
            "library_index",
            status="failed",
            phase="error",
            current=0,
            total=0,
            current_item="",
            message="Files library index rebuild failed",
            error=str(e),
            run_id=str(reason or "manual"),
            meta={"reason": str(reason or "manual")},
            finished=True,
        )
        _files_index_set_state(
            running=False,
            finished_at=time.time(),
            phase="error",
            phase_message=str(e),
            phase_progress=None,
            phase_eta_seconds=None,
            phase_rate_per_sec=None,
            current_folder=None,
            error=str(e),
        )
        return {"ok": False, "error": str(e)}
    finally:
        files_index_lock.release()
