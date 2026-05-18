"""Runtime-owned album media, tags, reviews, and cover handlers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    'api_library_album_tracks',
    '_files_fix_missing_album_track_durations',
    '_run_album_detail_enrichment',
    '_schedule_album_detail_enrichment',
    '_normalize_user_album_review_text',
    'api_library_album_download',
    'api_library_files_album_cover',
    'api_library_external_label_image',
    'api_library_album_tags',
    'api_library_album_tracks_detail',
    '_provider_cover_url_from_payload',
    '_cover_art_archive_front_urls',
    '_cover_art_archive_front_urls_for_identity',
    '_download_cover_art_archive_front',
    '_fetch_album_review_web_ai',
    '_fetch_album_review_web_ai_batch',
    '_album_review_search_context',
    '_resolve_album_review_identity_from_provider_hints',
    '_fetch_album_profile_from_provider_fallback',
    '_album_profile_provider_candidate',
    '_choose_best_album_profile_provider_candidate',
    '_fetch_best_album_profile',
    '_write_pmda_album_tags',
    '_apply_artist_album_tags_to_audio',
    '_clean_track_title_from_text',
    '_strip_album_artist_prefixes_from_track_title',
    '_track_display_fields_from_sources',
    '_provider_track_titles_cached',
    '_display_tracks_with_provider_overlay',
    'api_library_album_review_generate',
    'api_library_album_select_cover',
    'api_musicbrainz_fix_album_tags',
    '_album_artwork_gallery_cache_key',
    '_cover_art_archive_gallery_items',
    '_discogs_gallery_items',
    '_album_artwork_gallery_manifest',
    'api_library_album_artwork_gallery',
    'api_library_files_album_artwork_item',
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

def api_library_album_tracks(album_id):
    """Return track list for an album for playback (track_id, title, duration, file_url)."""
    if _get_library_mode() == "files":
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
                    SELECT alb.title, art.name, alb.has_cover
                    FROM files_albums alb
                    JOIN files_artists art ON art.id = alb.artist_id
                    WHERE alb.id = %s
                    """,
                    (album_id,),
                )
                album_row = cur.fetchone()
                if not album_row:
                    return jsonify({"error": "Album not found"}), 404
                album_title = album_row[0] or ""
                artist_name = album_row[1] or ""
                has_cover = bool(album_row[2])
                cur.execute(
                    """
                    SELECT id, title, duration_sec, track_num, file_path
                    FROM files_tracks
                    WHERE album_id = %s
                    ORDER BY disc_num ASC, track_num ASC, id ASC
                    """,
                    (album_id,),
                )
                rows = cur.fetchall()
            _schedule_album_detail_enrichment(
                int(album_id),
                rows=[(int(r[0] or 0), int(r[2] or 0), str(r[4] or "")) for r in rows],
                has_cover=bool(has_cover),
                cover_path_raw="",
            )
            tracks = [
                {
                    "track_id": int(r[0]),
                    "title": r[1] or "",
                    "artist": artist_name,
                    "album": album_title,
                    "duration": int(r[2] or 0),
                    "index": int(r[3] or 0),
                    "file_url": _browser_api_url(f"/api/library/track/{int(r[0])}/stream"),
                }
                for r in rows
            ]
            album_thumb = _browser_api_url(f"/api/library/files/album/{album_id}/cover?size=320")
            return jsonify({"tracks": tracks, "album_thumb": album_thumb})
        finally:
            conn.close()

    return jsonify({"error": "Album track lookup is only available from the files library"}), 400


def _files_fix_missing_album_track_durations(conn, *, album_id: int, rows: list[tuple[int, int, str]]) -> dict[int, int]:
    """
    Best-effort: ensure duration_sec is populated for the given album's tracks.
    Input rows: (track_id, duration_sec, file_path).
    Returns: {track_id: computed_duration_sec} overrides for tracks that were fixed in this request.
    """
    album_id = int(album_id or 0)
    if album_id <= 0:
        return {}
    duration_overrides: dict[int, int] = {}
    try:
        missing: list[tuple[int, Path]] = []
        for tid, dur, fpath in (rows or []):
            tid = int(tid or 0)
            if tid <= 0:
                continue
            if int(dur or 0) > 0:
                continue
            raw_path = str(fpath or "").strip()
            if not raw_path:
                continue
            p = path_for_fs_access(Path(raw_path))
            if not p.exists() or not p.is_file():
                continue
            missing.append((tid, p))
        if not missing:
            return {}

        pool = get_ffprobe_pool()
        futures = {pool.submit(_run_ffprobe_duration_sec, str(p)): tid for tid, p in missing[:96]}
        updates: list[tuple[int, int]] = []
        for fut in as_completed(futures):
            tid = int(futures.get(fut) or 0)
            try:
                dur_sec = int(fut.result() or 0)
            except Exception:
                dur_sec = 0
            if tid > 0 and dur_sec > 0:
                duration_overrides[tid] = dur_sec
                updates.append((dur_sec, tid))
        if not updates:
            return duration_overrides

        with conn.transaction():
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE files_tracks SET duration_sec = %s, updated_at = NOW() WHERE id = %s",
                    updates,
                )
                # Keep album total_duration_sec consistent (best-effort).
                cur.execute(
                    """
                    UPDATE files_albums
                    SET total_duration_sec = COALESCE((
                        SELECT SUM(t.duration_sec)
                        FROM files_tracks t
                        WHERE t.album_id = %s
                    ), 0)
                    WHERE id = %s
                    """,
                    (int(album_id), int(album_id)),
                )
        _files_cache_invalidate_all()
        return duration_overrides
    except Exception:
        return {}


def _run_album_detail_enrichment(album_id: int, *, rows: list[tuple[int, int, str]], has_cover: bool, cover_path_raw: str) -> None:
    """
    Background enrichment for album detail:
    - fill missing track durations via ffprobe
    - try embedded cover extraction if album has no persisted cover
    """
    album_id = int(album_id or 0)
    if album_id <= 0:
        return
    conn = _files_pg_connect()
    if conn is None:
        return
    try:
        _files_fix_missing_album_track_durations(conn, album_id=album_id, rows=rows)
        if bool(has_cover) or str(cover_path_raw or "").strip():
            return
        # Embedded cover fallback: first readable track only.
        first_track = None
        for _tid, _dur, raw_path in (rows or []):
            p = path_for_fs_access(Path(str(raw_path or "")))
            if p.exists() and p.is_file():
                first_track = p
                break
        if first_track is None:
            return
        embedded = _extract_embedded_cover_from_audio(first_track)
        if not embedded:
            return
        raw, mime = embedded
        master_cached = _ensure_cached_image_from_bytes(
            raw,
            mime,
            kind="embedded",
            cache_key_hint=f"album-{album_id}-master",
            max_px=1600,
        )
        if not master_cached or not master_cached.exists():
            return
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE files_albums
                    SET has_cover = TRUE,
                        cover_path = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (str(master_cached), int(album_id)),
                )
        _files_cache_invalidate_all()
    except Exception:
        logging.debug("Album detail enrichment failed for album_id=%s", album_id, exc_info=True)
    finally:
        conn.close()
        with _ALBUM_DETAIL_ENRICH_LOCK:
            _ALBUM_DETAIL_ENRICH_INFLIGHT.discard(album_id)
            _ALBUM_DETAIL_ENRICH_LAST_TS[album_id] = time.time()


def _schedule_album_detail_enrichment(album_id: int, *, rows: list[tuple[int, int, str]], has_cover: bool, cover_path_raw: str) -> None:
    album_id = int(album_id or 0)
    if album_id <= 0:
        return
    need_duration = any(int(dur or 0) <= 0 for _tid, dur, _fpath in (rows or []))
    need_cover = not bool(has_cover or str(cover_path_raw or "").strip())
    if not (need_duration or need_cover):
        return
    now = time.time()
    with _ALBUM_DETAIL_ENRICH_LOCK:
        if album_id in _ALBUM_DETAIL_ENRICH_INFLIGHT:
            return
        last_ts = float(_ALBUM_DETAIL_ENRICH_LAST_TS.get(album_id) or 0.0)
        if (now - last_ts) < float(_ALBUM_DETAIL_ENRICH_COOLDOWN_SEC):
            return
        _ALBUM_DETAIL_ENRICH_INFLIGHT.add(album_id)
    threading.Thread(
        target=_run_album_detail_enrichment,
        kwargs={
            "album_id": album_id,
            "rows": list(rows or []),
            "has_cover": bool(has_cover),
            "cover_path_raw": str(cover_path_raw or ""),
        },
        daemon=True,
        name=f"album-enrich-{album_id}",
    ).start()

def _normalize_user_album_review_text(value: Any, *, max_chars: int | None = None) -> str:
    if max_chars is None:
        max_chars = int(_USER_ALBUM_REVIEW_MAX_CHARS)
    text = str(value or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [str(line or "").rstrip() for line in text.split("\n")]
    normalized = "\n".join(lines).strip()
    if len(normalized) > int(max_chars):
        normalized = normalized[: int(max_chars)].rstrip()
    return normalized


def api_library_album_download(album_id: int):
    """Download a files-mode album folder as ZIP. Access is gated by auth/RBAC."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503

    album_id = int(album_id or 0)
    if album_id <= 0:
        return jsonify({"error": "Invalid album id"}), 400

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(alb.folder_path, '') AS folder_path,
                    COALESCE(alb.title, '') AS album_title,
                    COALESCE(art.name, '') AS artist_name
                FROM files_albums alb
                JOIN files_artists art ON art.id = alb.artist_id
                WHERE alb.id = %s
                LIMIT 1
                """,
                (album_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row or not str(row[0] or "").strip():
        return jsonify({"error": "Album not found"}), 404

    folder_path = path_for_fs_access(Path(str(row[0] or "").strip()))
    if not folder_path.exists() or not folder_path.is_dir():
        return jsonify({"error": "Album folder not found"}), 404

    artist_name = _sanitize_path_component(str(row[2] or "").strip() or f"artist-{album_id}")
    album_title = _sanitize_path_component(str(row[1] or "").strip() or f"album-{album_id}")
    archive_label = f"{artist_name} - {album_title}".strip(" -") or f"album-{album_id}"

    tmp_dir = tempfile.mkdtemp(prefix=f"pmda-album-{album_id}-")
    archive_base = os.path.join(tmp_dir, archive_label)
    archive_path = shutil.make_archive(archive_base, "zip", root_dir=str(folder_path))

    @after_this_request
    def _cleanup_archive(response):
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        return response

    return send_file(
        archive_path,
        as_attachment=True,
        download_name=os.path.basename(archive_path),
        mimetype="application/zip",
        conditional=True,
    )


def api_library_files_album_cover(album_id):
    """Serve album cover from files-library index (files mode)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    size = max(64, min(2048, _parse_int_loose(request.args.get("size"), 320)))
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    lookup_key = f"artwork:album:{int(album_id)}"
    lookup = _files_cache_get_json(lookup_key)
    cover_raw = ""
    folder_raw = ""
    no_cover_cached = False

    if isinstance(lookup, dict):
        cover_raw = str(lookup.get("cover_path") or "").strip()
        folder_raw = str(lookup.get("folder_path") or "").strip()
        no_cover_cached = bool(lookup.get("no_cover"))
    else:
        conn = _files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT cover_path, folder_path, has_cover FROM files_albums WHERE id = %s", (album_id,))
                row = cur.fetchone()
            if row:
                cover_raw = str(row[0] or "").strip()
                folder_raw = str(row[1] or "").strip()
                no_cover_cached = not bool(row[2])
            _files_cache_set_json(
                lookup_key,
                {
                    "cover_path": cover_raw,
                    "folder_path": folder_raw,
                    "no_cover": bool(no_cover_cached),
                },
                ttl=3600,
            )
        finally:
            conn.close()

    has_cover_effective, effective_cover_path = _resolve_files_album_cover_asset(
        album_id=int(album_id),
        cover_path_raw=cover_raw,
        folder_path_raw=folder_raw,
        has_cover=not bool(no_cover_cached),
        lookup_key=lookup_key,
        persist=True,
    )
    if has_cover_effective and effective_cover_path:
        try:
            cover_path = path_for_fs_access(Path(effective_cover_path))
        except Exception:
            cover_path = Path(effective_cover_path)
        if cover_path.exists() and cover_path.is_file():
            cached = _ensure_cached_image_for_path(cover_path, kind="album", max_px=size)
            return _serve_image_file_cached(cached or cover_path, max_age=0, revalidate=True)

    _files_cache_set_json(
        lookup_key,
        {
            "cover_path": "",
            "folder_path": folder_raw,
            "no_cover": True,
        },
        ttl=3600 if no_cover_cached else 900,
    )
    return _transparent_png_response(max_age=0, revalidate=True)


def api_library_external_label_image(label_norm: str):
    """Serve cached external label images."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    size = max(64, min(2048, _parse_int_loose(request.args.get("size"), 320)))
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    key = _norm_label_key(str(label_norm or ""))
    if not key:
        return jsonify({"error": "Invalid label"}), 400
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        ext_row = _files_get_external_label_images(conn, [key]).get(key) or {}
        img_raw = str(ext_row.get("image_path") or "").strip()
        if not img_raw:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COALESCE(alb.bandcamp_album_url, ''), COALESCE(alb.title, ''), COALESCE(ar.name, '')
                        FROM files_albums alb
                        JOIN files_artists ar ON ar.id = alb.artist_id
                        WHERE lower(trim(COALESCE(alb.label, ''))) = %s
                          AND COALESCE(alb.bandcamp_album_url, '') <> ''
                        ORDER BY
                            CASE WHEN COALESCE(alb.strict_match_provider, '') = 'bandcamp' THEN 0 ELSE 1 END,
                            alb.updated_at DESC,
                            alb.id DESC
                        LIMIT 6
                        """,
                        (key,),
                    )
                    warm_rows = cur.fetchall()
            except Exception:
                warm_rows = []
            for album_url, album_title, artist_name in warm_rows:
                try:
                    if _files_prewarm_label_logo_from_bandcamp(
                        conn,
                        label_name=str(label_norm or ""),
                        artist_name=str(artist_name or "").strip(),
                        album_title=str(album_title or "").strip(),
                        bandcamp_album_url=str(album_url or "").strip(),
                    ):
                        ext_row = _files_get_external_label_images(conn, [key]).get(key) or {}
                        img_raw = str(ext_row.get("image_path") or "").strip()
                        if img_raw:
                            break
                except Exception:
                    continue
            if not img_raw:
                return _transparent_png_response(max_age=0, revalidate=True)
        p = Path(img_raw)
        if not p.exists() or not p.is_file() or not _is_media_cache_file(p, kind="label"):
            return _transparent_png_response(max_age=0, revalidate=True)
        cached = _ensure_cached_image_for_path(p, kind="label", max_px=size)
        to_send = cached or p
        return _serve_image_file_cached(to_send, max_age=0, revalidate=True)
    finally:
        conn.close()


def api_library_album_tags(album_id):
    """Get current tags and MusicBrainz info for an album."""
    if _get_library_mode() == "files":
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
                    SELECT
                        alb.id,
                        alb.title,
                        art.name,
                        alb.folder_path,
                        alb.primary_tags_json,
                        alb.musicbrainz_release_group_id,
                        alb.has_cover
                    FROM files_albums alb
                    JOIN files_artists art ON art.id = alb.artist_id
                    WHERE alb.id = %s
                    """,
                    (album_id,),
                )
                row = cur.fetchone()
            if not row:
                return jsonify({"error": "Album not found"}), 404
            folder_path = path_for_fs_access(Path(row[3]))
            current_tags = {}
            try:
                current_tags = json.loads(row[4]) if row[4] else {}
            except (TypeError, ValueError):
                current_tags = {}
            if not current_tags:
                first_audio = next((p for p in folder_path.rglob("*") if AUDIO_RE.search(p.name)), None)
                current_tags = extract_tags(first_audio) if first_audio else {}
            thumb_url_files = (
                f"{request.url_root.rstrip('/')}/api/library/files/album/{album_id}/cover"
                if bool(row[6]) else None
            )
            return jsonify({
                "album_id": int(row[0]),
                "album_title": row[1] or "",
                "artist_name": row[2] or "",
                "folder": str(folder_path),
                "current_tags": current_tags or {},
                "musicbrainz_id": (row[5] or "").strip(),
                "mb_info": None,
                "thumb_url": thumb_url_files,
            })
        finally:
            conn.close()

    return jsonify({"error": "Album tag inspection is only available from the files library"}), 400

def api_library_album_tracks_detail(album_id: int):
    """
    Detailed per-track inspector for one album (files mode only).
    Returns file path + technical audio fields + full container tags (ffprobe).
    """
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503

    album_id = int(album_id or 0)
    if album_id <= 0:
        return jsonify({"error": "Invalid album id"}), 400
    include_tags = bool(_parse_bool(request.args.get("include_tags", "1")))

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(alb.title, ''),
                    COALESCE(art.name, ''),
                    COALESCE(alb.primary_tags_json, '{}'),
                    COALESCE(alb.metadata_source, ''),
                    COALESCE(alb.strict_match_provider, ''),
                    COALESCE(alb.musicbrainz_release_group_id, ''),
                    COALESCE(alb.discogs_release_id, ''),
                    COALESCE(alb.lastfm_album_mbid, ''),
                    COALESCE(alb.bandcamp_album_url, '')
                FROM files_albums alb
                JOIN files_artists art ON art.id = alb.artist_id
                WHERE alb.id = %s
                LIMIT 1
                """,
                (album_id,),
            )
            album_row = cur.fetchone()
            if not album_row:
                return jsonify({"error": "Album not found"}), 404
            album_title = str(album_row[0] or "").strip()
            artist_name = str(album_row[1] or "").strip()
            album_primary_tags_json = str(album_row[2] or "{}")
            metadata_source = str(album_row[3] or "").strip()
            strict_match_provider = str(album_row[4] or "").strip()
            authoritative_mb_rg = str(album_row[5] or "").strip()
            authoritative_discogs = str(album_row[6] or "").strip()
            authoritative_lastfm = str(album_row[7] or "").strip()
            authoritative_bandcamp = str(album_row[8] or "").strip()

            cur.execute(
                """
                SELECT
                    id,
                    COALESCE(title, '') AS title,
                    COALESCE(disc_num, 0) AS disc_num,
                    COALESCE(track_num, 0) AS track_num,
                    COALESCE(duration_sec, 0) AS duration_sec,
                    COALESCE(format, '') AS format,
                    COALESCE(bitrate, 0) AS bitrate,
                    COALESCE(sample_rate, 0) AS sample_rate,
                    COALESCE(bit_depth, 0) AS bit_depth,
                    COALESCE(file_size_bytes, 0) AS file_size_bytes,
                    COALESCE(file_path, '') AS file_path,
                    COALESCE(primary_tags_json, '{}') AS primary_tags_json
                FROM files_tracks
                WHERE album_id = %s
                ORDER BY disc_num ASC, track_num ASC, id ASC
                """,
                (album_id,),
            )
            rows = cur.fetchall()

        album_primary_tags = _safe_json_load(album_primary_tags_json, fallback={})
        if not isinstance(album_primary_tags, dict):
            album_primary_tags = {}
        if authoritative_mb_rg:
            album_primary_tags["musicbrainz_releasegroupid"] = authoritative_mb_rg
            album_primary_tags["musicbrainz_release_group_id"] = authoritative_mb_rg
            album_primary_tags["musicbrainz_id"] = authoritative_mb_rg
        if authoritative_discogs:
            album_primary_tags["discogs_release_id"] = authoritative_discogs
        if authoritative_lastfm:
            album_primary_tags["lastfm_album_mbid"] = authoritative_lastfm
        if authoritative_bandcamp:
            album_primary_tags["bandcamp_album_url"] = authoritative_bandcamp
        if metadata_source:
            album_primary_tags["metadata_source"] = metadata_source
        if strict_match_provider:
            album_primary_tags["strict_match_provider"] = strict_match_provider
        if album_primary_tags.get(PMDA_ID_TAG):
            album_primary_tags["pmda_id"] = str(album_primary_tags.get(PMDA_ID_TAG) or "").strip()

        def _normalize_tag_map(raw_map: Any) -> dict[str, str]:
            out: dict[str, str] = {}
            if not isinstance(raw_map, dict):
                return out
            for key, value in raw_map.items():
                kk = str(key or "").strip().lower()
                if not kk:
                    continue
                vv = str(value or "").strip()
                if not vv:
                    continue
                out[kk] = vv
            return out

        album_detail_tags = _normalize_tag_map(album_primary_tags)

        def _extract_one_track(row: tuple[Any, ...]) -> dict[str, Any]:
            tid = int(row[0] or 0)
            file_path_raw = str(row[10] or "").strip()
            track_primary_tags = _normalize_tag_map(_safe_json_load(row[11], fallback={}))
            merged_tags = {}
            merged_tags.update(album_detail_tags)
            merged_tags.update(track_primary_tags)
            out: dict[str, Any] = {
                "track_id": tid,
                "title": str(row[1] or "").strip(),
                "disc_num": int(row[2] or 0),
                "track_num": int(row[3] or 0),
                "duration_sec": int(row[4] or 0),
                "format": str(row[5] or "").strip(),
                "bitrate": int(row[6] or 0),
                "sample_rate": int(row[7] or 0),
                "bit_depth": int(row[8] or 0),
                "file_size_bytes": int(row[9] or 0),
                "file_path": file_path_raw,
                "tags": merged_tags,
                "tags_error": None,
            }
            if not include_tags:
                return out
            if not file_path_raw:
                out["tags_error"] = "missing_file_path"
                return out
            try:
                p = path_for_fs_access(Path(file_path_raw))
                if not p.exists() or not p.is_file():
                    out["tags_error"] = "file_not_found"
                    return out
                tags_raw = extract_tags(p) or {}
                tags_clean: dict[str, str] = {}
                for k, v in (tags_raw or {}).items():
                    kk = str(k or "").strip().lower()
                    if not kk:
                        continue
                    vv = str(v or "").strip()
                    if not vv:
                        continue
                    tags_clean[kk] = vv
                merged_tags.update(tags_clean)
                out["tags"] = merged_tags
            except Exception:
                out["tags_error"] = "tag_read_error"
            return out

        details: list[dict[str, Any]] = []
        if include_tags and len(rows) >= 6:
            workers = max(2, min(8, int(len(rows))))
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="track-detail") as pool:
                fut_to_idx = {pool.submit(_extract_one_track, row): idx for idx, row in enumerate(rows)}
                ordered: list[dict[str, Any] | None] = [None] * len(rows)
                for fut in as_completed(fut_to_idx):
                    idx = int(fut_to_idx.get(fut) or 0)
                    try:
                        ordered[idx] = fut.result()
                    except Exception:
                        ordered[idx] = _extract_one_track(rows[idx])
                details = [d for d in ordered if isinstance(d, dict)]
        else:
            details = [_extract_one_track(r) for r in rows]

        return jsonify(
            {
                "album_id": int(album_id),
                "artist_name": artist_name,
                "album_title": album_title,
                "tracks": details,
            }
        )
    finally:
        conn.close()

def _provider_cover_url_from_payload(provider: str, payload: dict | None) -> str:
    p = _normalize_identity_provider(provider)
    data = payload if isinstance(payload, dict) else {}
    if p == "musicbrainz":
        cover_url = str(data.get("cover_url") or "").strip()
        if cover_url:
            return cover_url
        payload_id = str(data.get("id") or data.get("musicbrainz_id") or "").strip()
        release_id = str(
            data.get("musicbrainz_release_id")
            or data.get("release_id")
            or ""
        ).strip()
        release_group_id = str(
            data.get("release_group_id")
            or data.get("musicbrainz_release_group_id")
            or ""
        ).strip()
        ref_url = str(data.get("url") or "").strip().lower()
        if not release_id and payload_id and "/release/" in ref_url:
            release_id = payload_id
        if not release_group_id and payload_id and "/release-group/" in ref_url:
            release_group_id = payload_id
        if release_id:
            return f"https://coverartarchive.org/release/{quote(release_id, safe='')}/front"
        if release_group_id:
            return f"https://coverartarchive.org/release-group/{quote(release_group_id, safe='')}/front"
        return ""
    return str(data.get("cover_url") or "").strip()

def _cover_art_archive_front_urls(mbid: str | None) -> list[str]:
    raw = str(mbid or "").strip()
    if not raw or not re.fullmatch(r"[0-9a-fA-F-]{36}", raw):
        return []
    quoted = quote(raw, safe="")
    return [
        f"https://coverartarchive.org/release-group/{quoted}/front",
        f"https://coverartarchive.org/release/{quoted}/front",
    ]

def _cover_art_archive_front_urls_for_identity(
    *,
    release_id: str | None = None,
    release_group_id: str | None = None,
) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for kind, raw in (
        ("release", str(release_id or "").strip()),
        ("release-group", str(release_group_id or "").strip()),
    ):
        if not raw or not re.fullmatch(r"[0-9a-fA-F-]{36}", raw):
            continue
        url = f"https://coverartarchive.org/{kind}/{quote(raw, safe='')}/front"
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls

def _download_cover_art_archive_front(
    mbid: str | None = None,
    *,
    release_id: str | None = None,
    release_group_id: str | None = None,
    timeout_sec: float = 5.0,
) -> tuple[bytes, str, str] | None:
    cover_urls = _cover_art_archive_front_urls_for_identity(
        release_id=release_id,
        release_group_id=release_group_id,
    )
    if not cover_urls:
        cover_urls = _cover_art_archive_front_urls(mbid)
    for cover_url in cover_urls:
        try:
            cover_resp = requests.get(cover_url, timeout=timeout_sec, allow_redirects=True)
        except Exception:
            continue
        if cover_resp.status_code != 200:
            continue
        content = cover_resp.content
        mime = (cover_resp.headers.get("content-type") or "").split(";")[0].strip() or "image/jpeg"
        if not mime.startswith("image/"):
            mime = "image/jpeg"
        return (content, mime, cover_url)
    return None

def _fetch_album_review_web_ai(
    artist_name: str,
    album_title: str,
    *,
    allow_short_title_fallback: bool = False,
    search_context: dict[str, Any] | None = None,
) -> dict[str, str]:
    """
    Best-effort web review fallback:
    - Single query first, expand only on failure.
    - Optional AI summary (single album).
    Returns {"description","short_description","source"} or {}.
    """
    artist = str(artist_name or "").strip()
    album = str(album_title or "").strip()
    if not artist or not album:
        return {}
    _primary_query, _expansion_queries, query_batch_size = _review_lookup_query_plan(artist, album)
    hits = _review_lookup_collect_hits(
        artist,
        album,
        query_batch_size=query_batch_size,
        max_hits=24,
        search_context=search_context,
    )
    if not hits:
        return {}
    search_source = _review_search_source_from_hits(hits)
    source = search_source
    candidates = _review_prepare_candidates(hits)
    if not candidates:
        return {}
    validated = _review_validate_candidates_with_ai(artist, album, candidates)
    if not validated and _review_candidates_need_broader_retry(candidates):
        logging.info(
            "[Review] Initial candidates for %r - %r were blocked or inconclusive; retrying broader search.",
            artist,
            album,
        )
        retry_hits = _review_lookup_collect_hits(
            artist,
            album,
            query_batch_size=query_batch_size,
            max_hits=36,
            search_context=search_context,
            continue_after_hit=True,
        )
        if retry_hits:
            source = _review_search_source_from_hits(retry_hits)
            retry_candidates = _review_prepare_candidates(retry_hits)
            if retry_candidates:
                candidates = retry_candidates
                validated = _review_validate_candidates_with_ai(artist, album, retry_candidates)
    if not validated:
        return {}
    selected = (validated.get("selected") or {}) if isinstance(validated.get("selected"), dict) else {}
    description = _strip_html_text(str(selected.get("page_excerpt") or "").strip())
    if not description:
        return {}
    provider_effective = str(validated.get("provider_effective") or "").strip()
    auth_mode = str(validated.get("auth_mode") or "").strip()
    if provider_effective:
        source = _review_ai_provider_source(provider_effective, auth_mode, search_source)
    short_description = _strip_html_text(str(selected.get("snippet") or "").strip())
    if not short_description:
        short_description = _truncate_text(description, max_chars=320)
    relevance_probe = " ".join(
        part
        for part in (
            description,
            str(selected.get("title") or "").strip(),
            str(selected.get("snippet") or "").strip(),
            str(selected.get("page_title") or "").strip(),
        )
        if str(part or "").strip()
    ).strip()
    if not _is_relevant_album_profile_text(
        artist,
        album,
        relevance_probe or description,
        allow_short_album_fallback=bool(allow_short_title_fallback),
    ):
        return {}
    return {
        "description": description,
        "short_description": _truncate_text(short_description or description, max_chars=320),
        "source": source,
        "source_url": str(selected.get("page_url") or "").strip(),
    }

def _fetch_album_review_web_ai_batch(
    artist_name: str,
    requests_payload: list[dict[str, Any]],
) -> dict[str, dict[str, str]]:
    """
    Multi-album review lookup with grouped AI summarization (single call per chunk).
    Returns map keyed by title_norm.
    """
    artist = str(artist_name or "").strip()
    if not artist or not isinstance(requests_payload, list):
        return {}

    out: dict[str, dict[str, str]] = {}
    for raw in requests_payload[:180]:
        if not isinstance(raw, dict):
            continue
        entry_artist = str(raw.get("artist_name") or artist).strip() or artist
        album = str(raw.get("album_title") or "").strip()
        title_norm = str(raw.get("title_norm") or norm_album(album)).strip()
        if not album or not title_norm:
            continue
        search_context = _album_review_search_context(
            artist=entry_artist,
            album=album,
            metadata_source=str(raw.get("metadata_source") or "").strip(),
            mbid=str(raw.get("mbid") or "").strip(),
            discogs_release_id=str(raw.get("discogs_release_id") or "").strip(),
            lastfm_album_mbid=str(raw.get("lastfm_album_mbid") or "").strip(),
            bandcamp_album_url=str(raw.get("bandcamp_album_url") or "").strip(),
            strict_match_verified=bool(raw.get("strict_match_verified")),
        )
        payload = _fetch_album_review_web_ai(
            entry_artist,
            album,
            allow_short_title_fallback=bool(raw.get("allow_short_title_fallback")),
            search_context=search_context,
        )
        if payload:
            out[title_norm] = payload
    return out

def _album_review_search_context(
    *,
    artist: str,
    album: str,
    metadata_source: str = "",
    mbid: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
    strict_match_verified: bool = False,
) -> dict[str, Any]:
    return {
        "query_kind": "album_review",
        "artist": str(artist or "").strip(),
        "album": str(album or "").strip(),
        "metadata_source": str(metadata_source or "").strip(),
        "musicbrainz_release_group_id": str(mbid or "").strip(),
        "discogs_release_id": str(discogs_release_id or "").strip(),
        "lastfm_album_mbid": str(lastfm_album_mbid or "").strip(),
        "bandcamp_album_url": str(bandcamp_album_url or "").strip(),
        "strict_match_verified": bool(strict_match_verified),
    }

def _resolve_album_review_identity_from_provider_hints(
    artist_name: str,
    album_title: str,
    *,
    metadata_source: str = "",
    mbid: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
) -> tuple[str, str, str]:
    """
    Resolve a safer review-search identity from trusted provider IDs when local tags are generic.
    Returns: (artist_name, album_title, provider_used)
    """
    local_artist = str(artist_name or "").strip()
    local_album = str(album_title or "").strip()
    preferred = _normalize_identity_provider(str(metadata_source or ""))

    provider_chain: list[str] = []
    for p in (preferred, "musicbrainz", "discogs", "lastfm", "bandcamp"):
        p2 = _normalize_identity_provider(p)
        if p2 and p2 not in provider_chain:
            provider_chain.append(p2)

    provider_payloads: dict[str, dict[str, Any]] = {}
    if str(mbid or "").strip():
        try:
            payload = _fetch_musicbrainz_strict_payload(str(mbid or "").strip())
            if isinstance(payload, dict):
                provider_payloads["musicbrainz"] = payload
        except Exception:
            pass
    if str(discogs_release_id or "").strip():
        try:
            payload = _fetch_discogs_release_by_id(str(discogs_release_id or "").strip())
            if isinstance(payload, dict):
                provider_payloads["discogs"] = payload
        except Exception:
            pass
    if str(lastfm_album_mbid or "").strip():
        try:
            payload = _fetch_lastfm_album_info(local_artist, local_album, mbid=str(lastfm_album_mbid or "").strip())
            if isinstance(payload, dict):
                provider_payloads["lastfm"] = payload
        except Exception:
            pass
    if str(bandcamp_album_url or "").strip():
        try:
            payload = _fetch_bandcamp_album_info(local_artist, local_album, album_url_hint=str(bandcamp_album_url or "").strip())
            if isinstance(payload, dict):
                provider_payloads["bandcamp"] = payload
        except Exception:
            pass

    local_generic = _identity_text_is_generic(local_artist) or _identity_text_is_generic(local_album)
    best_artist = local_artist
    best_album = local_album
    best_provider = ""
    best_score = -1.0

    for provider in provider_chain:
        payload = provider_payloads.get(provider)
        if not isinstance(payload, dict):
            continue
        cand_artist = _strip_html_text(str(_provider_payload_artist(provider, payload) or "").strip())
        cand_album = _strip_html_text(str(_provider_payload_title(provider, payload) or "").strip())
        if not cand_artist or not cand_album:
            continue
        if _identity_text_is_generic(cand_artist) or _identity_text_is_generic(cand_album):
            continue

        title_score = float(_provider_identity_text_score(local_album, cand_album))
        artist_score = float(_provider_identity_text_score(local_artist, cand_artist))
        score = (title_score * 0.55) + (artist_score * 0.45)
        if provider == preferred:
            score += 0.08
        if local_generic:
            score += 0.25

        if score > best_score:
            best_score = score
            best_artist = cand_artist
            best_album = cand_album
            best_provider = provider

    # Keep local identity unless it is generic, or provider identity is clearly stronger.
    if best_provider:
        if local_generic or best_score >= 0.70:
            return best_artist, best_album, best_provider
    return local_artist, local_album, ""

def _fetch_album_profile_from_provider_fallback(
    artist_name: str,
    album_title: str,
    *,
    metadata_source: str = "",
    mbid: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
    allow_short_title_fallback: bool = False,
) -> dict[str, Any]:
    """
    Provider-only profile fallback used when web search returns nothing.
    Goal: still produce a useful review snippet from trusted provider pages.
    """
    artist = str(artist_name or "").strip()
    album = str(album_title or "").strip()
    if not artist or not album:
        return {}

    preferred = _normalize_identity_provider(metadata_source)
    chain = [preferred, "lastfm", "bandcamp", "discogs", "musicbrainz"]
    seen: set[str] = set()
    providers: list[str] = []
    for p in chain:
        p2 = _normalize_identity_provider(p)
        if not p2 or p2 in seen:
            continue
        seen.add(p2)
        providers.append(p2)

    for p in providers:
        try:
            if p == "lastfm":
                info = _fetch_lastfm_album_info(artist, album, mbid=(lastfm_album_mbid or None)) or {}
                desc = _strip_html_text(
                    str(info.get("wiki_content") or info.get("wiki_summary") or "").strip()
                )
                if not desc:
                    continue
                if not _is_relevant_album_profile_text(
                    artist,
                    album,
                    desc,
                    allow_short_album_fallback=bool(allow_short_title_fallback),
                ):
                    continue
                tags = info.get("toptags") if isinstance(info.get("toptags"), list) else []
                return {
                    "description": desc,
                    "short_description": _truncate_text(
                        str(info.get("wiki_summary") or desc).strip(),
                        max_chars=320,
                    ),
                    "tags": tags[:20],
                    "source": "lastfm",
                }

            if p == "bandcamp":
                info = _fetch_bandcamp_album_info(
                    artist,
                    album,
                    allow_web_fallback=False,
                    album_url_hint=str(bandcamp_album_url or "").strip(),
                ) or {}
                desc = _strip_html_text(str(info.get("description") or "").strip())
                if not desc:
                    # Keep a tiny fallback marker from known album URL when available.
                    if str(bandcamp_album_url or "").strip():
                        desc = ""
                if not desc:
                    continue
                if not _is_relevant_album_profile_text(
                    artist,
                    album,
                    desc,
                    allow_short_album_fallback=bool(allow_short_title_fallback),
                ):
                    continue
                tags = info.get("tags") if isinstance(info.get("tags"), list) else []
                return {
                    "description": desc,
                    "short_description": _truncate_text(desc, max_chars=320),
                    "tags": _dedupe_keep_order([str(t or "").strip() for t in tags if str(t or "").strip()])[:20],
                    "source": "bandcamp",
                }

            if p == "discogs":
                discogs_payload = None
                did = str(discogs_release_id or "").strip()
                if did:
                    try:
                        discogs_payload = _fetch_discogs_release_by_id(did)
                    except DiscogsRateLimited:
                        discogs_payload = None
                if not isinstance(discogs_payload, dict):
                    try:
                        discogs_payload = _fetch_discogs_release(artist, album)
                    except DiscogsRateLimited:
                        discogs_payload = None
                desc = _discogs_release_notes_text(discogs_payload)
                if not desc:
                    continue
                if not _is_relevant_album_profile_text(
                    artist,
                    album,
                    desc,
                    allow_short_album_fallback=bool(allow_short_title_fallback),
                ):
                    continue
                return {
                    "description": desc,
                    "short_description": _truncate_text(desc, max_chars=320),
                    "tags": [],
                    "source": "discogs",
                }

            if p == "musicbrainz":
                rgid = str(mbid or "").strip()
                if not rgid:
                    continue
                payload = _fetch_musicbrainz_strict_payload(rgid)
                if not isinstance(payload, dict):
                    continue
                annotation = _strip_html_text(str(payload.get("annotation") or "").strip())
                if not annotation:
                    continue
                if not _is_relevant_album_profile_text(
                    artist,
                    album,
                    annotation,
                    allow_short_album_fallback=bool(allow_short_title_fallback),
                ):
                    continue
                return {
                    "description": annotation,
                    "short_description": _truncate_text(annotation, max_chars=320),
                    "tags": [],
                    "source": "musicbrainz",
                }
        except Exception:
            continue
    return {}

def _album_profile_provider_candidate(
    *,
    artist_name: str,
    album_title: str,
    source: str,
    description: Any,
    short_description: Any = "",
    tags: Any = None,
    allow_short_title_fallback: bool = False,
) -> dict[str, Any]:
    source_norm = _normalize_identity_provider(source)
    desc = _strip_html_text(str(description or "").strip())
    if not desc:
        return {}
    if not _is_relevant_album_profile_text(
        artist_name,
        album_title,
        desc,
        allow_short_album_fallback=bool(allow_short_title_fallback),
    ):
        return {}
    summary = _strip_html_text(str(short_description or "").strip()) or desc
    if not summary:
        summary = desc
    clean_tags = _dedupe_keep_order(
        [str(tag or "").strip() for tag in (tags or []) if str(tag or "").strip()]
    )[:20]
    base_score = {
        "bandcamp": 40.0,
        "lastfm": 34.0,
        "discogs": 22.0,
        "musicbrainz": 18.0,
    }.get(source_norm, 0.0)
    richness_score = min(len(desc), 2400) / 100.0
    paragraph_score = min(6.0, float(max(0, desc.count("\n"))))
    return {
        "description": desc,
        "short_description": _truncate_text(summary, max_chars=320),
        "tags": clean_tags,
        "source": source_norm,
        "_score": base_score + richness_score + paragraph_score,
    }

def _choose_best_album_profile_provider_candidate(
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    usable = [dict(candidate) for candidate in candidates if isinstance(candidate, dict) and str(candidate.get("description") or "").strip()]
    if not usable:
        return {}
    usable.sort(
        key=lambda candidate: (
            float(candidate.get("_score") or 0.0),
            len(str(candidate.get("description") or "")),
            len(str(candidate.get("short_description") or "")),
        ),
        reverse=True,
    )
    best = usable[0]
    best.pop("_score", None)
    return best

def _fetch_best_album_profile(
    artist_name: str,
    album_title: str,
    *,
    allow_web_ai: bool = True,
    allow_short_title_fallback: bool = False,
    precomputed_web_profile: Optional[dict[str, Any]] = None,
    metadata_source: str = "",
    mbid: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
    strict_match_verified: bool = False,
) -> dict[str, Any]:
    """
    Best-effort album profile for one album.
    Automatic scan/backfill persists the richest relevant provider prose available.
    Community pulse still aggregates from all providers even when prose comes from one source.
    """
    artist = str(artist_name or "").strip()
    album = str(album_title or "").strip()
    if not artist or not album:
        return {}

    info = _fetch_lastfm_album_info(artist, album, mbid=(lastfm_album_mbid or None)) or {}
    direct_bandcamp_info: dict[str, Any] = {}
    if str(bandcamp_album_url or "").strip():
        try:
            direct_bandcamp_info = _fetch_bandcamp_album_info(
                artist,
                album,
                allow_web_fallback=False,
                album_url_hint=str(bandcamp_album_url or "").strip(),
            ) or {}
        except Exception:
            direct_bandcamp_info = {}
    try:
        fetched = _fetch_album_provider_fallbacks_parallel(artist, album) or {}
    except Exception:
        fetched = {}
    discogs_info = fetched.get("discogs") if isinstance(fetched.get("discogs"), dict) else {}
    bandcamp_info = direct_bandcamp_info if isinstance(direct_bandcamp_info, dict) and direct_bandcamp_info else {}
    if not bandcamp_info:
        bandcamp_info = fetched.get("bandcamp") if isinstance(fetched.get("bandcamp"), dict) else {}
    lastfm_info = fetched.get("lastfm") if isinstance(fetched.get("lastfm"), dict) else {}
    metrics = _merge_album_public_metrics(info, lastfm_info, discogs_info, bandcamp_info)
    provider_tags = _dedupe_keep_order(
        [
            str(tag or "").strip()
            for tag in (
                list(info.get("toptags") or [])
                + list(lastfm_info.get("toptags") or [])
                + list(bandcamp_info.get("tags") or [])
            )
            if str(tag or "").strip()
        ]
    )[:20]

    web_profile = {}
    if bool(allow_web_ai):
        if isinstance(precomputed_web_profile, dict) and (
            str(precomputed_web_profile.get("description") or "").strip()
            or str(precomputed_web_profile.get("short_description") or "").strip()
        ):
            web_profile = dict(precomputed_web_profile)
        else:
            web_profile = _fetch_album_review_web_ai(
                artist,
                album,
                allow_short_title_fallback=bool(allow_short_title_fallback),
                search_context={
                    "query_kind": "album_review",
                    "artist": artist,
                    "album": album,
                    "metadata_source": str(metadata_source or "").strip(),
                    "musicbrainz_release_group_id": str(mbid or "").strip(),
                    "discogs_release_id": str(discogs_release_id or "").strip(),
                    "lastfm_album_mbid": str(lastfm_album_mbid or "").strip(),
                    "bandcamp_album_url": str(bandcamp_album_url or "").strip(),
                    "strict_match_verified": bool(strict_match_verified),
                },
            )
    if web_profile:
        desc = str(web_profile.get("description") or "").strip()
        short_desc = str(web_profile.get("short_description") or "").strip()
        if not _is_relevant_album_profile_text(
            artist,
            album,
            desc or short_desc,
            allow_short_album_fallback=bool(allow_short_title_fallback),
        ):
            return {}
        return {
            "description": desc,
            "short_description": short_desc,
            "tags": _dedupe_keep_order(
                provider_tags
                + [str(tag or "").strip() for tag in (web_profile.get("tags") or []) if str(tag or "").strip()]
            )[:20],
            "source": str(web_profile.get("source") or "ollama").strip() or "ollama",
            **metrics,
        }
    provider_candidates: list[dict[str, Any]] = []
    provider_candidates.append(
        _album_profile_provider_candidate(
            artist_name=artist,
            album_title=album,
            source="lastfm",
            description=(
                info.get("wiki_content")
                or info.get("wiki_summary")
                or lastfm_info.get("wiki_content")
                or lastfm_info.get("wiki_summary")
                or ""
            ),
            short_description=(info.get("wiki_summary") or lastfm_info.get("wiki_summary") or ""),
            tags=list(info.get("toptags") or []) + list(lastfm_info.get("toptags") or []),
            allow_short_title_fallback=bool(allow_short_title_fallback),
        )
    )
    provider_candidates.append(
        _album_profile_provider_candidate(
            artist_name=artist,
            album_title=album,
            source="bandcamp",
            description=bandcamp_info.get("description") or "",
            short_description=bandcamp_info.get("description") or "",
            tags=bandcamp_info.get("tags") or [],
            allow_short_title_fallback=bool(allow_short_title_fallback),
        )
    )
    provider_candidates.append(
        _album_profile_provider_candidate(
            artist_name=artist,
            album_title=album,
            source="discogs",
            description=_discogs_release_notes_text(discogs_info),
            short_description=_discogs_release_notes_text(discogs_info),
            tags=[],
            allow_short_title_fallback=bool(allow_short_title_fallback),
        )
    )
    provider_desc = _choose_best_album_profile_provider_candidate(provider_candidates)
    if not provider_desc:
        provider_desc = _fetch_album_profile_from_provider_fallback(
            artist,
            album,
            metadata_source=metadata_source,
            mbid=mbid,
            discogs_release_id=discogs_release_id,
            lastfm_album_mbid=lastfm_album_mbid,
            bandcamp_album_url=bandcamp_album_url,
            allow_short_title_fallback=bool(allow_short_title_fallback),
        )
    if provider_desc:
        merged_tags = _dedupe_keep_order(
            provider_tags
            + [str(tag or "").strip() for tag in (provider_desc.get("tags") or []) if str(tag or "").strip()]
        )[:20]
        return {
            "description": str(provider_desc.get("description") or "").strip(),
            "short_description": str(provider_desc.get("short_description") or "").strip(),
            "tags": merged_tags,
            "source": str(provider_desc.get("source") or "").strip(),
            **metrics,
        }
    payload = {
        "description": "",
        "short_description": "",
        "tags": provider_tags,
        "source": "",
        **metrics,
    }
    return payload if _album_profile_has_payload(payload) else {}

def _write_pmda_album_tags(
    folder: Path,
    audio_files: list[Path],
    *,
    pmda_id: Optional[str],
    match_provider: Optional[str],
    cover_provider: Optional[str],
    artist_provider: Optional[str],
    matched: bool,
    cover: bool,
    artist_image: bool,
    complete: bool,
    tag_write_mode: Optional[str] = None,
) -> None:
    """
    Write PMDA_* album-level tags to all audio files in an album folder.
    Called at the end of improve-album once we know what was actually applied.
    """
    if not audio_files:
        return
    # Writing tags triggers filesystem events; suppress watcher-triggered rescans for a short period.
    try:
        _files_watcher_suppress_folder(folder, seconds=120.0, reason="pmda_tag_write")
    except Exception:
        pass
    try:
        from mutagen import File as MutagenFile  # type: ignore
    except Exception:
        return
    pmda_id_val = (pmda_id or "").strip() or str(uuid.uuid4())
    matched_str = "true" if matched else "false"
    cover_str = "true" if cover else "false"
    artist_str = "true" if artist_image else "false"
    complete_str = "true" if complete else "false"
    match_provider_val = (match_provider or "").strip()
    cover_provider_val = (cover_provider or "").strip()
    artist_provider_val = (artist_provider or "").strip()
    mode = str(tag_write_mode or _files_tag_write_mode() or "full").strip().lower()
    for p in audio_files:
        try:
            audio = MutagenFile(str(p))
            if audio is None:
                continue
            _set_pmda_tag(audio, "PMDA_ID", pmda_id_val)
            if mode != "pmda_id_only":
                _set_pmda_tag(audio, "PMDA_MATCHED", matched_str)
                if match_provider_val:
                    _set_pmda_tag(audio, "PMDA_MATCH_PROVIDER", match_provider_val)
                _set_pmda_tag(audio, "PMDA_COVER", cover_str)
                if cover_provider_val:
                    _set_pmda_tag(audio, "PMDA_COVER_PROVIDER", cover_provider_val)
                _set_pmda_tag(audio, "PMDA_ARTIST_IMAGE", artist_str)
                if artist_provider_val:
                    _set_pmda_tag(audio, "PMDA_ARTIST_PROVIDER", artist_provider_val)
                _set_pmda_tag(audio, "PMDA_COMPLETE", complete_str)
            audio.save()
        except Exception as e:
            logging.warning("PMDA tag write failed for %s: %s", p, e)

def _apply_artist_album_tags_to_audio(
    audio,
    *,
    album_artist: str,
    track_artist: str,
    album_title: str,
    year_str: str | None,
    genre_str: str | None = None,
) -> None:
    """
    Central helper to write basic ARTIST/ALBUM (and album artist when supported) on a Mutagen audio object.
    This keeps album artist consistent across all tracks so PMDA does not accidentally split albums.
    """
    mode = _normalize_artist_credit_mode(globals().get("ARTIST_CREDIT_MODE", ARTIST_CREDIT_MODE))
    # For now, track_artist is already chosen by the caller based on mode.
    aa = (album_artist or track_artist or "").strip()
    ta = (track_artist or album_artist or "").strip()
    title = (album_title or "").strip()
    year = (year_str or "").strip() if year_str else ""
    genre = (genre_str or "").strip() if genre_str else ""

    # ID3 / MP3
    try:
        from mutagen.id3 import ID3, TPE1, TPE2, TALB, TDRC, TCON, TXXX  # type: ignore
        from mutagen.mp3 import MP3  # type: ignore
    except Exception:  # pragma: no cover - optional dependency
        ID3 = MP3 = None  # type: ignore[assignment]

    if ID3 is not None and isinstance(audio, (MP3, ID3)):
        if audio.tags is None:
            audio.add_tags()
        # Track artist
        if ta:
            audio.tags.add(TPE1(encoding=3, text=ta))
        # Album artist (TPE2)
        if aa:
            audio.tags.add(TPE2(encoding=3, text=aa))
        # Album title / year / genre
        if title:
            audio.tags.add(TALB(encoding=3, text=title))
        if year:
            audio.tags.add(TDRC(encoding=3, text=year))
        if genre:
            audio.tags.add(TCON(encoding=3, text=genre))
        # Simple featuring storage in a TXXX frame when mode is not album_artist_strict
        if mode != "album_artist_strict":
            # Caller can optionally add a TXXX later using _split_main_and_featuring; we keep helper minimal for now.
            pass
        return

    # FLAC / VorbisComment
    try:
        from mutagen.flac import FLAC  # type: ignore
    except Exception:  # pragma: no cover
        FLAC = None  # type: ignore[assignment]

    if FLAC is not None and isinstance(audio, FLAC):
        if ta:
            audio["ARTIST"] = ta
        if title:
            audio["ALBUM"] = title
        if aa:
            audio["ALBUMARTIST"] = aa
        if year:
            audio["DATE"] = year
        if genre:
            audio["GENRE"] = genre
        return

    # MP4 / M4A
    try:
        from mutagen.mp4 import MP4  # type: ignore
    except Exception:  # pragma: no cover
        MP4 = None  # type: ignore[assignment]

    if MP4 is not None and isinstance(audio, MP4):
        if ta:
            audio["\xa9ART"] = [ta]
        if title:
            audio["\xa9alb"] = [title]
        if year:
            audio["\xa9day"] = [year]
        if genre:
            audio["\xa9gen"] = [genre]
        # Some players use aART for album artist; set it when available
        if aa:
            try:
                audio["aART"] = [aa]
            except Exception:
                # aART may not be supported by all taggers; ignore silently
                pass
        return

def api_library_album_review_generate(album_id: int):
    """Manually generate/persist one album review profile (web+AI first, then provider fallback)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    if not _auth_user_can_use_ai(_current_user_or_empty()):
        return jsonify({"error": "AI access is disabled for this user"}), 403
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    album_id = int(album_id or 0)
    if album_id <= 0:
        return jsonify({"error": "Invalid album id"}), 400

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    alb.id,
                    COALESCE(alb.title, ''),
                    COALESCE(alb.title_norm, ''),
                    alb.strict_match_verified,
                    COALESCE(alb.metadata_source, ''),
                    COALESCE(alb.musicbrainz_release_group_id, ''),
                    COALESCE(alb.discogs_release_id, ''),
                    COALESCE(alb.lastfm_album_mbid, ''),
                    COALESCE(alb.bandcamp_album_url, ''),
                    art.id,
                    COALESCE(art.name, ''),
                    COALESCE(art.name_norm, '')
                FROM files_albums alb
                JOIN files_artists art ON art.id = alb.artist_id
                WHERE alb.id = %s
                LIMIT 1
                """,
                (album_id,),
            )
            row = cur.fetchone()
        if not row:
            return jsonify({"error": "Album not found"}), 404
        (
            _aid,
            album_title,
            title_norm,
            strict_verified,
            metadata_source,
            mbid,
            discogs_release_id,
            lastfm_album_mbid,
            bandcamp_album_url,
            artist_id,
            artist_name,
            artist_norm,
        ) = row
        album_title = str(album_title or "").strip()
        title_norm = str(title_norm or "").strip()
        artist_name = str(artist_name or "").strip()
        artist_norm = str(artist_norm or "").strip() or _norm_artist_key(artist_name)
        strict_verified = bool(strict_verified)
        has_identity_hint = bool(
            strict_verified
            or str(metadata_source or "").strip()
            or str(mbid or "").strip()
            or str(discogs_release_id or "").strip()
            or str(lastfm_album_mbid or "").strip()
            or str(bandcamp_album_url or "").strip()
        )
        if not artist_name or not album_title or not title_norm:
            return jsonify({"error": "Album identity incomplete"}), 409
        if not (strict_verified or has_identity_hint):
            return jsonify({"error": "Album identity not trusted enough for review generation"}), 409

        review_artist = artist_name
        review_album = album_title
        review_identity_provider = ""
        if has_identity_hint:
            review_artist, review_album, review_identity_provider = _resolve_album_review_identity_from_provider_hints(
                artist_name,
                album_title,
                metadata_source=str(metadata_source or "").strip(),
                mbid=str(mbid or "").strip(),
                discogs_release_id=str(discogs_release_id or "").strip(),
                lastfm_album_mbid=str(lastfm_album_mbid or "").strip(),
                bandcamp_album_url=str(bandcamp_album_url or "").strip(),
            )
            if review_identity_provider and (
                _norm_artist_key(review_artist) != _norm_artist_key(artist_name)
                or norm_album(review_album) != norm_album(album_title)
            ):
                logging.info(
                    "[Review] Album %s: using provider identity hint for review lookup (%s): %r - %r (local=%r - %r)",
                    album_id,
                    review_identity_provider,
                    review_artist,
                    review_album,
                    artist_name,
                    album_title,
                )

        review_timeout = max(
            4.0,
            float(
                getattr(
                    sys.modules[__name__],
                    "AI_REVIEW_FETCH_TIMEOUT_SEC",
                    AI_REVIEW_FETCH_TIMEOUT_SEC,
                )
                or AI_REVIEW_FETCH_TIMEOUT_SEC
            ),
        )
        search_context = _album_review_search_context(
            artist=review_artist,
            album=review_album,
            metadata_source=str(metadata_source or "").strip(),
            mbid=str(mbid or "").strip(),
            discogs_release_id=str(discogs_release_id or "").strip(),
            lastfm_album_mbid=str(lastfm_album_mbid or "").strip(),
            bandcamp_album_url=str(bandcamp_album_url or "").strip(),
            strict_match_verified=bool(strict_verified),
        )
        review_started = time.perf_counter()
        profile = _fetch_album_review_web_ai(
            review_artist,
            review_album,
            allow_short_title_fallback=bool(strict_verified or has_identity_hint),
            search_context=search_context,
        ) or {}
        logging.info(
            "[Review] album=%s web+ai finished in %.2fs",
            int(album_id or 0),
            time.perf_counter() - review_started,
        )
        if not isinstance(profile, dict):
            profile = {}
        has_profile_text = bool(
            str(profile.get("description") or "").strip()
            or str(profile.get("short_description") or "").strip()
        )
        if not has_profile_text:
            ai_web_ok, _ai_web_provider, _ai_web_auth, ai_web_reason = _resolve_ai_runtime_availability(
                analysis_type="web_search",
                requested_provider="openai",
                user_id=_current_user_id_or_zero(),
            )
            local_ai_web_possible = bool(_ollama_web_search_enabled())
            ai_web_fallback_possible = bool(
                bool(ai_web_ok)
                and str(_ai_web_provider or "").strip().lower() in {"openai", "openai-api", "openai-codex"}
                and _web_search_ai_fallback_enabled()
            )
            serper_ok, serper_msg = _run_serper_preflight()
            if not (serper_ok or local_ai_web_possible or ai_web_fallback_possible):
                detail_parts: list[str] = []
                if str(ai_web_reason or "").strip():
                    detail_parts.append(f"openai_web={str(ai_web_reason).strip()}")
                if str(serper_msg or "").strip():
                    detail_parts.append(f"serper={str(serper_msg).strip()}")
                if not local_ai_web_possible:
                    detail_parts.append("ollama_web=disabled_or_unavailable")
                detail = " ; ".join(detail_parts) if detail_parts else "no web provider available"
                logging.warning(
                    "[Review] Album %s web search unavailable for review generation: %s",
                    int(album_id or 0),
                    detail,
                )
                return jsonify({"error": f"No relevant review found (web search unavailable: {detail})"}), 404
            return jsonify({"error": "No relevant review found"}), 404

        with conn.transaction():
            _files_upsert_album_profile(conn, artist_norm, title_norm, album_title, profile)
        _files_cache_invalidate_all()
        source = str(profile.get("source") or "").strip() or "unknown"
        return jsonify(
            {
                "ok": True,
                "album_id": int(album_id),
                "artist_id": int(artist_id or 0),
                "source": source,
                "used_ai": ("ai" in source.lower()),
                "updated_at": int(time.time()),
            }
        )
    finally:
        conn.close()

def api_library_album_select_cover(album_id: int):
    """Manual cover override for one album (files mode)."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    album_id = int(album_id or 0)
    if album_id <= 0:
        return jsonify({"error": "Invalid album id"}), 400

    body = request.get_json(silent=True) or {}
    cover_url = str(body.get("cover_url") or "").strip()
    source_url = str(body.get("source_url") or "").strip()
    provider = _normalize_identity_provider(str(body.get("provider") or "")) or "manual"
    if not cover_url:
        return jsonify({"error": "Missing cover_url"}), 400
    if not _is_safe_public_http_url(cover_url):
        return jsonify({"error": "Invalid cover_url"}), 400
    if source_url and (not _is_safe_public_http_url(source_url)):
        source_url = ""

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    alb.id,
                    COALESCE(alb.title, ''),
                    COALESCE(art.name, ''),
                    alb.strict_match_verified,
                    COALESCE(alb.strict_match_provider, ''),
                    COALESCE(alb.strict_reject_reason, ''),
                    COALESCE(alb.strict_tracklist_score, 0.0),
                    COALESCE(alb.musicbrainz_release_group_id, ''),
                    COALESCE(alb.discogs_release_id, ''),
                    COALESCE(alb.lastfm_album_mbid, ''),
                    COALESCE(alb.bandcamp_album_url, ''),
                    COALESCE(alb.metadata_source, '')
                FROM files_albums alb
                JOIN files_artists art ON art.id = alb.artist_id
                WHERE alb.id = %s
                LIMIT 1
                """,
                (album_id,),
            )
            row = cur.fetchone()
        if not row:
            return jsonify({"error": "Album not found"}), 404

        (
            _aid,
            album_title,
            artist_name,
            strict_verified,
            strict_provider_raw,
            strict_reason,
            strict_track_score,
            mbid,
            discogs_release_id,
            lastfm_album_mbid,
            bandcamp_album_url,
            metadata_source_raw,
        ) = row
        strict_provider = _normalize_identity_provider(str(strict_provider_raw or ""))
        metadata_source = _normalize_identity_provider(str(metadata_source_raw or ""))
        selected_provider = (
            strict_provider
            or metadata_source
            or ("musicbrainz" if str(mbid or "").strip() else "")
            or ("discogs" if str(discogs_release_id or "").strip() else "")
            or ("lastfm" if str(lastfm_album_mbid or "").strip() else "")
            or ("bandcamp" if str(bandcamp_album_url or "").strip() else "")
        )
        selected_provider = _normalize_identity_provider(selected_provider)
        has_identity_hint = bool(
            bool(strict_verified)
            or selected_provider
            or str(mbid or "").strip()
            or str(discogs_release_id or "").strip()
            or str(lastfm_album_mbid or "").strip()
            or str(bandcamp_album_url or "").strip()
        )

        dl = _download_best_cover_image(
            provider,
            cover_url,
            cover_candidates=[cover_url],
            timeout=14,
        )
        if not dl:
            return jsonify({"error": "Unable to fetch cover image"}), 424
        raw, mime, used_url = dl
        cached = _ensure_cached_image_from_bytes(
            raw,
            mime,
            kind="album",
            cache_key_hint=f"manual-cover:{album_id}:{provider}:{used_url}",
            max_px=_MEDIA_CACHE_MASTER_PX,
        )
        if not cached or (not cached.exists()) or (not cached.is_file()):
            return jsonify({"error": "Unable to cache selected cover"}), 500

        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE files_albums
                    SET has_cover = TRUE,
                        cover_path = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (str(cached), int(album_id)),
                )
    finally:
        conn.close()

    _files_cache_invalidate_all()
    steps = [
        f"Manual cover selection applied from provider={provider}",
        f"Requested URL: {cover_url}",
        f"Resolved URL: {used_url}",
        f"Cached path: {str(cached)}",
    ]
    if source_url:
        steps.append(f"Source page: {source_url}")
    audit_result = {
        "summary": f"Manual cover selected from {_match_provider_label(provider)}.",
        "provider_used": selected_provider or provider,
        "tags_updated": False,
        "cover_saved": True,
        "pmda_matched": bool(has_identity_hint),
        "pmda_cover": True,
        "pmda_artist_image": False,
        "pmda_complete": False,
        "pmda_match_provider": selected_provider or None,
        "pmda_cover_provider": provider,
        "pmda_artist_provider": None,
        "strict_match_verified": bool(strict_verified),
        "strict_match_provider": strict_provider,
        "strict_reject_reason": str(strict_reason or "").strip(),
        "strict_tracklist_score": float(strict_track_score or 0.0),
        "mutation_blocked": False,
        "mutation_blocked_reason": "",
    }
    _record_files_match_audit_album(
        album_id=album_id,
        artist_name=str(artist_name or "").strip() or "Unknown Artist",
        album_title=str(album_title or "").strip() or f"Album {album_id}",
        run_kind="manual_cover",
        status="completed",
        result=audit_result,
        steps=steps,
    )

    return jsonify(
        {
            "ok": True,
            "album_id": int(album_id),
            "provider": provider,
            "cover_url": used_url,
            "cover_path": str(cached),
            "source_url": source_url or None,
        }
    )

def api_musicbrainz_fix_album_tags():
    """Fix tags for a single album using MusicBrainz data."""
    if _get_library_mode() == "files":
        data = request.get_json() or {}
        album_id = data.get("album_id")
        tags_to_apply = data.get("tags", {}) or {}
        if not album_id:
            return jsonify({"error": "Missing album_id"}), 400
        try:
            album_id = int(album_id)
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid album_id"}), 400
        ok, err = _ensure_files_index_ready()
        if not ok:
            return jsonify({"error": err or "Files index unavailable"}), 503
        conn = _files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT folder_path FROM files_albums WHERE id = %s", (album_id,))
                row = cur.fetchone()
            if not row or not (row[0] or "").strip():
                return jsonify({"error": "Album not found"}), 404
            folder_path = path_for_fs_access(Path(row[0]))
        finally:
            conn.close()
        if not folder_path.exists() or not folder_path.is_dir():
            return jsonify({"error": "Album folder not found"}), 404

        album_artist = (tags_to_apply.get("albumartist") or tags_to_apply.get("artist") or "").strip()
        track_artist = (tags_to_apply.get("artist") or tags_to_apply.get("albumartist") or "").strip()
        album_title = (tags_to_apply.get("album") or "").strip()
        year_val = (tags_to_apply.get("year") or tags_to_apply.get("date") or "").strip()
        if year_val and len(year_val) >= 4:
            year_val = year_val[:4]
        genre_val = (tags_to_apply.get("genre") or "").strip()
        from mutagen import File as MutagenFile

        audio_files = [p for p in folder_path.rglob("*") if AUDIO_RE.search(p.name)]
        updated = 0
        errors = []
        for p in audio_files:
            try:
                audio = MutagenFile(str(p))
                if audio is None:
                    continue
                _apply_artist_album_tags_to_audio(
                    audio,
                    album_artist=album_artist,
                    track_artist=track_artist or album_artist,
                    album_title=album_title,
                    year_str=year_val,
                    genre_str=genre_val or None,
                )
                audio.save()
                updated += 1
            except Exception as e:
                errors.append(f"{p.name}: {e}")
        _trigger_files_index_rebuild_async(reason="manual_tag_fix")
        return jsonify({
            "success": True,
            "message": f"Updated tags on {updated} file(s).",
            "files_updated": updated,
            "errors": errors[:20],
        })

    return jsonify({"error": "Album tag writing is only available from the files library"}), 400

def _album_artwork_gallery_cache_key(album_id: int) -> str:
    return f"artwork_gallery:album:{int(album_id)}"
def _cover_art_archive_gallery_items(release_id: str, release_group_id: str = "") -> list[dict[str, Any]]:
    rel_id = str(release_id or "").strip()
    rg_id = str(release_group_id or "").strip()
    urls: list[tuple[str, str]] = []
    if rel_id and re.fullmatch(r"[0-9a-fA-F-]{36}", rel_id):
        urls.append((f"https://coverartarchive.org/release/{quote(rel_id, safe='')}", "musicbrainz"))
    if not urls and rg_id and re.fullmatch(r"[0-9a-fA-F-]{36}", rg_id):
        urls.append((f"https://coverartarchive.org/release-group/{quote(rg_id, safe='')}", "musicbrainz"))
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for url, provider in urls:
        try:
            resp = requests.get(url, timeout=12)
            if resp.status_code != 200:
                continue
            payload = resp.json() if "json" in (resp.headers.get("content-type") or "").lower() else {}
        except Exception:
            continue
        images = payload.get("images") if isinstance(payload, dict) else []
        if not isinstance(images, list):
            continue
        for idx, image in enumerate(images[:12], start=1):
            if not isinstance(image, dict):
                continue
            image_url = str(
                ((image.get("thumbnails") or {}).get("1200"))
                or ((image.get("thumbnails") or {}).get("large"))
                or image.get("image")
                or ""
            ).strip()
            if not image_url or image_url in seen:
                continue
            seen.add(image_url)
            types = [str(t or "").strip().lower() for t in list(image.get("types") or []) if str(t or "").strip()]
            slot = "front" if bool(image.get("front")) or "front" in types else "other"
            if bool(image.get("back")) or "back" in types:
                slot = "back"
            elif any(t in {"booklet", "booklet page", "booklet-page"} for t in types):
                slot = "booklet"
            elif any(t in {"medium", "disc"} for t in types):
                slot = "disc"
            out.append(
                {
                    "provider": provider,
                    "slot": slot,
                    "label": _artwork_slot_label(slot),
                    "source_url": image_url,
                    "types": types,
                    "selected": slot == "front",
                }
            )
    return out
def _discogs_gallery_items(discogs_release_id: str) -> list[dict[str, Any]]:
    payload = _fetch_discogs_release_by_id(discogs_release_id)
    data = payload if isinstance(payload, dict) else {}
    raw_images = data.get("images") if isinstance(data.get("images"), list) else []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for idx, image in enumerate(raw_images[:12], start=1):
        if not isinstance(image, dict):
            continue
        image_url = str(image.get("uri") or image.get("resource_url") or "").strip()
        if not image_url or image_url in seen:
            continue
        seen.add(image_url)
        image_type = str(image.get("type") or "").strip().lower()
        slot = "front" if image_type == "primary" else "other"
        out.append(
            {
                "provider": "discogs",
                "slot": slot,
                "label": _artwork_slot_label(slot),
                "source_url": image_url,
                "types": [image_type] if image_type else [],
                "selected": slot == "front" and idx == 1,
            }
        )
    cover_url = str(data.get("cover_url") or "").strip()
    if cover_url and cover_url not in seen:
        out.insert(
            0,
            {
                "provider": "discogs",
                "slot": "front",
                "label": _artwork_slot_label("front"),
                "source_url": cover_url,
                "types": ["primary"],
                "selected": True,
            },
        )
    return out
def _album_artwork_gallery_manifest(
    *,
    album_id: int,
    folder_path_raw: str,
    cover_path_raw: str,
    musicbrainz_release_id: str,
    musicbrainz_release_group_id: str,
    discogs_release_id: str,
) -> dict[str, Any]:
    _ensure_media_cache_dirs()
    folder = path_for_fs_access(Path(folder_path_raw)) if str(folder_path_raw or "").strip() else None
    cover_path = path_for_fs_access(Path(cover_path_raw)) if str(cover_path_raw or "").strip() else None
    items: list[dict[str, Any]] = []
    seen_file_keys: set[str] = set()
    seen_source_urls: set[str] = set()

    def _push_local(path: Path, slot: str, *, origin: str = "local_file", selected: bool = False, source_name: str = "") -> None:
        if not path or not path.exists() or not path.is_file():
            return
        cache_path = _ensure_cached_image_for_path(path, kind="album", max_px=_MEDIA_CACHE_MASTER_PX) or path
        cache_key = hashlib.sha1(f"{origin}|{slot}|{cache_path}".encode("utf-8", errors="ignore")).hexdigest()
        if cache_key in seen_file_keys:
            return
        seen_file_keys.add(cache_key)
        items.append(
            {
                "id": cache_key,
                "slot": slot,
                "label": _artwork_slot_label(slot),
                "origin": origin,
                "provider": "local",
                "selected": bool(selected),
                "cache_path": str(cache_path),
                "source_name": source_name or path.name,
            }
        )

    def _push_bytes(raw: bytes, mime: str, slot: str, *, origin: str, source_name: str = "", source_url: str | None = None, provider: str = "local", selected: bool = False) -> None:
        cache_path = _ensure_cached_image_from_bytes(
            raw,
            mime,
            kind="album",
            cache_key_hint=f"gallery:{album_id}:{origin}:{slot}:{source_name}:{source_url or ''}",
            max_px=_MEDIA_CACHE_MASTER_PX,
        )
        if cache_path is None:
            return
        cache_key = hashlib.sha1(f"{origin}|{slot}|{cache_path}".encode("utf-8", errors="ignore")).hexdigest()
        if cache_key in seen_file_keys:
            return
        seen_file_keys.add(cache_key)
        items.append(
            {
                "id": cache_key,
                "slot": slot,
                "label": _artwork_slot_label(slot),
                "origin": origin,
                "provider": provider,
                "selected": bool(selected),
                "cache_path": str(cache_path),
                "source_name": source_name or origin,
                "source_url": source_url,
            }
        )

    if cover_path and cover_path.exists() and cover_path.is_file():
        _push_local(cover_path, "front", selected=True, source_name=cover_path.name)
    if folder and folder.exists() and folder.is_dir():
        for path, slot in _collect_folder_artwork_files(folder, max_items=10):
            selected = bool(cover_path and path.resolve() == cover_path.resolve())
            _push_local(path, slot, selected=selected, source_name=path.name)
        for raw, mime, slot, source_name, desc in _extract_embedded_artworks_from_folder(folder, max_audio_files=8, max_items=8):
            _push_bytes(
                raw,
                mime,
                slot,
                origin="embedded",
                source_name=f"{source_name}:{desc or slot}",
                provider="embedded",
                selected=slot == "front" and not any(bool(item.get("selected")) for item in items),
            )

    remote_items = _cover_art_archive_gallery_items(musicbrainz_release_id, musicbrainz_release_group_id)
    if not remote_items and discogs_release_id:
        remote_items = _discogs_gallery_items(discogs_release_id)
    for idx, entry in enumerate(remote_items[:10], start=1):
        source_url = str(entry.get("source_url") or "").strip()
        if not source_url or source_url in seen_source_urls:
            continue
        seen_source_urls.add(source_url)
        try:
            resp = requests.get(source_url, timeout=12, allow_redirects=True)
            if resp.status_code != 200 or not resp.content:
                continue
            mime = (resp.headers.get("content-type") or "").split(";", 1)[0].strip() or "image/jpeg"
        except Exception:
            continue
        _push_bytes(
            resp.content,
            mime,
            str(entry.get("slot") or "other"),
            origin="provider",
            source_name=f"{entry.get('provider') or 'provider'}-{idx}",
            source_url=source_url,
            provider=str(entry.get("provider") or "provider"),
            selected=bool(entry.get("selected")),
        )

    items.sort(
        key=lambda item: (
            _artwork_slot_sort_key(str(item.get("slot") or "other")),
            0 if bool(item.get("selected")) else 1,
            str(item.get("origin") or ""),
            str(item.get("source_name") or ""),
        )
    )
    if items and not any(bool(item.get("selected")) for item in items):
        items[0]["selected"] = True
    return {
        "album_id": int(album_id),
        "items": items,
        "updated_at": int(time.time()),
    }
def api_library_album_artwork_gallery(album_id: int):
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    album_id = int(album_id or 0)
    if album_id <= 0:
        return jsonify({"error": "Invalid album id"}), 400
    cache_key = _album_artwork_gallery_cache_key(album_id)
    cached = _files_cache_get_json(cache_key)
    if not isinstance(cached, dict):
        conn = _files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COALESCE(folder_path, ''),
                        COALESCE(cover_path, ''),
                        COALESCE(musicbrainz_release_id, ''),
                        COALESCE(musicbrainz_release_group_id, ''),
                        COALESCE(discogs_release_id, ''),
                        COALESCE(title, '')
                    FROM files_albums
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (album_id,),
                )
                row = cur.fetchone()
        finally:
            conn.close()
        if not row:
            return jsonify({"error": "Album not found"}), 404
        cached = _album_artwork_gallery_manifest(
            album_id=album_id,
            folder_path_raw=str(row[0] or "").strip(),
            cover_path_raw=str(row[1] or "").strip(),
            musicbrainz_release_id=str(row[2] or "").strip(),
            musicbrainz_release_group_id=str(row[3] or "").strip(),
            discogs_release_id=str(row[4] or "").strip(),
        )
        cached["title"] = str(row[5] or "").strip()
        _files_cache_set_json(cache_key, cached, ttl=21600)
    items_payload: list[dict[str, Any]] = []
    for item in list(cached.get("items") or []):
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id") or "").strip()
        if not item_id:
            continue
        items_payload.append(
            {
                "id": item_id,
                "slot": str(item.get("slot") or "other"),
                "label": str(item.get("label") or _artwork_slot_label(str(item.get("slot") or "other"))),
                "origin": str(item.get("origin") or ""),
                "provider": str(item.get("provider") or ""),
                "selected": bool(item.get("selected")),
                "source_name": str(item.get("source_name") or "").strip() or None,
                "source_url": str(item.get("source_url") or "").strip() or None,
                "image_url": f"{request.url_root.rstrip('/')}/api/library/files/album/{album_id}/artwork/{quote(item_id, safe='')}?size=1600",
                "thumb_url": f"{request.url_root.rstrip('/')}/api/library/files/album/{album_id}/artwork/{quote(item_id, safe='')}?size=320",
            }
        )
    return jsonify(
        {
            "album_id": album_id,
            "title": str(cached.get("title") or "").strip() or None,
            "items": items_payload,
            "updated_at": int(cached.get("updated_at") or 0) or None,
        }
    )
def api_library_files_album_artwork_item(album_id, artwork_id):
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    size = max(64, min(2048, _parse_int_loose(request.args.get("size"), 640)))
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    cache_key = _album_artwork_gallery_cache_key(int(album_id))
    manifest = _files_cache_get_json(cache_key)
    if not isinstance(manifest, dict):
        conn = _files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COALESCE(folder_path, ''),
                        COALESCE(cover_path, ''),
                        COALESCE(musicbrainz_release_id, ''),
                        COALESCE(musicbrainz_release_group_id, ''),
                        COALESCE(discogs_release_id, ''),
                        COALESCE(title, '')
                    FROM files_albums
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(album_id),),
                )
                row = cur.fetchone()
        finally:
            conn.close()
        if row:
            manifest = _album_artwork_gallery_manifest(
                album_id=int(album_id),
                folder_path_raw=str(row[0] or "").strip(),
                cover_path_raw=str(row[1] or "").strip(),
                musicbrainz_release_id=str(row[2] or "").strip(),
                musicbrainz_release_group_id=str(row[3] or "").strip(),
                discogs_release_id=str(row[4] or "").strip(),
            )
            manifest["title"] = str(row[5] or "").strip()
            _files_cache_set_json(cache_key, manifest, ttl=21600)
    if not isinstance(manifest, dict):
        return _transparent_png_response(max_age=0, revalidate=True)
    wanted = str(artwork_id or "").strip()
    for item in list(manifest.get("items") or []):
        if not isinstance(item, dict) or str(item.get("id") or "").strip() != wanted:
            continue
        cache_raw = str(item.get("cache_path") or "").strip()
        if not cache_raw:
            break
        try:
            image_path = path_for_fs_access(Path(cache_raw))
        except Exception:
            image_path = Path(cache_raw)
        if image_path.exists() and image_path.is_file():
            cached = _ensure_cached_image_for_path(image_path, kind="album", max_px=size)
            return _serve_image_file_cached(cached or image_path, max_age=86400, revalidate=False)
        break
    return _transparent_png_response(max_age=0, revalidate=True)


def _clean_track_title_from_text(text: str, fallback_index: int) -> str:
    raw = str(text or "").strip()
    if not raw:
        return f"Track {max(1, int(fallback_index or 1))}"

    def _trim_title_separators(value: str) -> str:
        return str(value or "").strip(" -_")

    cleaned = re.sub(
        r"^\s*[^-]+?\s*-\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\s*[-_. ]*",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    if cleaned != raw:
        cleaned = _trim_title_separators(cleaned)
        return cleaned or raw or f"Track {fallback_index}"
    cleaned = re.sub(
        r"^\s*[^-]+?\s*-\s*\d{1,3}\s*[-_. ]*",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    if cleaned != raw:
        cleaned = _trim_title_separators(cleaned)
        return cleaned or raw or f"Track {fallback_index}"
    cleaned = re.sub(
        r"^\s*[^-]+?\s*-\s*[^-]+?\s*-\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\s*[-_. ]*",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    if cleaned != raw:
        cleaned = _trim_title_separators(cleaned)
        return cleaned or raw or f"Track {fallback_index}"
    cleaned = re.sub(
        r"^\s*[^-]+?\s*-\s*[^-]+?\s*-\s*\d{1,3}\s*[-_. ]*",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    if cleaned != raw:
        cleaned = _trim_title_separators(cleaned)
        return cleaned or raw or f"Track {fallback_index}"
    cleaned = re.sub(
        r"^\s*[^-]+?\s*-\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\s*[-_. ]*",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    if cleaned != raw:
        cleaned = _trim_title_separators(cleaned)
        return cleaned or raw or f"Track {fallback_index}"
    cleaned = re.sub(r"^\s*(?:cd|disc)\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\s*[-_. ]*", "", raw, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*\d{1,2}\s*[-_.]\s*\d{1,3}\s*[-_. ]*", "", cleaned)
    cleaned = re.sub(r"^\s*[A-Z]\s*(?:[-_. ]?\s*\d{1,3})\s*[-_. ]*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*\d{1,3}\s*[-_. ]*", "", cleaned)
    cleaned = _trim_title_separators(cleaned)
    return cleaned or raw or f"Track {fallback_index}"


def _strip_album_artist_prefixes_from_track_title(
    text: str,
    *,
    album_hint: str = "",
    artist_hint: str = "",
) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    cleaned = raw
    for prefix in (album_hint, artist_hint):
        prefix_txt = str(prefix or "").strip()
        if not prefix_txt:
            continue
        cleaned = re.sub(
            rf"^\s*{re.escape(prefix_txt)}\s*[-–—_. ]+\s*(?:(?:cd|disc)\s*)?\d{{1,2}}\s*[-–—_. ]+\s*\d{{1,3}}\s*[-–—_. ]*",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            rf"^\s*{re.escape(prefix_txt)}\s*[-–—_. ]+\s*\d{{1,3}}\s*[-–—_. ]*",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
    cleaned = cleaned.strip(" -_")
    return cleaned or raw


def _track_display_fields_from_sources(
    *,
    raw_title: str,
    file_path: str,
    fallback_disc: int,
    fallback_track: int,
    album_hint: str = "",
    artist_hint: str = "",
) -> dict[str, Any]:
    disc_num = max(1, int(fallback_disc or 1))
    track_num = max(1, int(fallback_track or 1))
    title_out = str(raw_title or "").strip()
    candidates: list[str] = []
    for source in (raw_title, Path(file_path).stem if str(file_path or "").strip() else ""):
        candidates.extend(_track_text_candidates(source))
    seen: set[str] = set()
    uniq_candidates: list[str] = []
    for cand in candidates:
        norm = cand.strip().lower()
        if not norm or norm in seen:
            continue
        seen.add(norm)
        uniq_candidates.append(cand)
    for cand in uniq_candidates:
        parsed_disc, parsed_track = _infer_disc_track_from_text(cand, track_num)
        looks_structured = bool(
            re.match(
                r"^\s*(?:[^-]+?\s*-\s*){0,2}(?:cd|disc\s*)?\d{1,2}\s*[-_. ]\s*\d{1,3}\b",
                cand,
                flags=re.IGNORECASE,
            )
            or re.match(r"^\s*[A-Z]\s*(?:[-_. ]?\s*\d{1,3})\b", cand, flags=re.IGNORECASE)
        )
        if looks_structured or (parsed_disc != 1 or parsed_track != track_num):
            disc_num = max(1, int(parsed_disc or disc_num))
            track_num = max(1, int(parsed_track or track_num))
            break
    for cand in uniq_candidates:
        cleaned = _clean_track_title_from_text(cand, track_num)
        cleaned = _strip_album_artist_prefixes_from_track_title(
            cleaned,
            album_hint=album_hint,
            artist_hint=artist_hint,
        )
        if cleaned and _norm_track_title_strict(cleaned):
            title_out = cleaned
            break
    title_out = str(title_out or "").strip() or f"Track {track_num}"
    disc_label = ""
    for cand in uniq_candidates:
        cand_label = _disc_label_from_text(cand, disc_num)
        if cand_label:
            disc_label = cand_label
            break
    if not disc_label and disc_num > 1:
        disc_label = f"Disc {disc_num}"
    return {
        "display_title": title_out,
        "display_disc_num": disc_num,
        "display_track_num": track_num,
        "display_disc_label": disc_label,
    }


def _provider_track_titles_cached(
    *,
    artist_name: str,
    album_title: str,
    metadata_source: str,
    musicbrainz_release_group_id: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
    edition_payload: dict | None = None,
    cache_only: bool = False,
) -> list[str]:
    provider = _normalize_identity_provider(metadata_source)
    if not provider:
        return []
    edition = dict(edition_payload or {})
    edition.setdefault("musicbrainz_id", (musicbrainz_release_group_id or "").strip())
    edition.setdefault("discogs_release_id", (discogs_release_id or "").strip())
    edition.setdefault("lastfm_album_mbid", (lastfm_album_mbid or "").strip())
    edition.setdefault("bandcamp_album_url", (bandcamp_album_url or "").strip())
    edition.setdefault("primary_metadata_source", provider)
    ref = (
        _strict_expected_provider_id(provider, edition)
        or (discogs_release_id or "").strip()
        or (lastfm_album_mbid or "").strip()
        or (bandcamp_album_url or "").strip()
        or f"{_norm_artist_key(artist_name)}::{norm_album_for_dedup(album_title, normalize_parenthetical=True)}"
    )
    cache_key = f"provider:tracklist:{provider}:{hashlib.sha1(ref.encode('utf-8', errors='ignore')).hexdigest()}"
    cached = _files_cache_get_json(cache_key)
    if isinstance(cached, dict) and isinstance(cached.get("tracklist"), list):
        return [str(t or "").strip() for t in cached.get("tracklist") or [] if str(t or "").strip()]
    if cache_only:
        return []
    try:
        payload = _strict_payload_for_provider(
            provider,
            artist_name=str(artist_name or ""),
            album_title=str(album_title or ""),
            edition=edition,
        )
        tracklist = _provider_track_titles_for_strict(provider, payload or {})
        _files_cache_set_json(cache_key, {"tracklist": tracklist}, ttl=86400 * 7)
        return tracklist
    except DiscogsRateLimited:
        raise
    except Exception as exc:
        logging.warning(
            "Provider tracklist overlay failed for provider=%s artist=%s album=%s ref=%s: %s",
            provider,
            str(artist_name or "").strip() or "Unknown Artist",
            str(album_title or "").strip() or "Unknown Album",
            ref,
            exc,
        )
        return []


def _display_tracks_with_provider_overlay(
    rows: list[dict[str, Any]],
    *,
    artist_name: str,
    album_title: str,
    metadata_source: str,
    musicbrainz_release_group_id: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
    edition_payload: dict | None = None,
    cache_only: bool = False,
) -> list[dict[str, Any]]:
    out = [dict(r or {}) for r in (rows or [])]
    out.sort(
        key=lambda t: (
            int(t.get("disc_num") or 1),
            int(t.get("track_num") or 0),
            int(t.get("track_id") or 0),
            str(t.get("file_path") or ""),
        )
    )
    provider_track_titles = _provider_track_titles_cached(
        artist_name=artist_name,
        album_title=album_title,
        metadata_source=metadata_source,
        musicbrainz_release_group_id=musicbrainz_release_group_id,
        discogs_release_id=discogs_release_id,
        lastfm_album_mbid=lastfm_album_mbid,
        bandcamp_album_url=bandcamp_album_url,
        edition_payload=edition_payload,
        cache_only=cache_only,
    )
    if provider_track_titles and len(provider_track_titles) == len(out):
        for idx, title in enumerate(provider_track_titles):
            cleaned = str(title or "").strip()
            if not cleaned:
                continue
            out[idx]["title"] = cleaned
            out[idx]["provider_title_used"] = True
    max_disc = max((int(t.get("disc_num") or 1) for t in out), default=1)
    for track in out:
        track["disc_label"] = str(track.get("disc_label") or "").strip() or (
            f"Disc {int(track.get('disc_num') or 1)}" if max_disc > 1 else ""
        )
    return out

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}


def api_library_album_tracks_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_album_tracks(*args, **kwargs)

def _files_fix_missing_album_track_durations_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_fix_missing_album_track_durations(*args, **kwargs)

def _run_album_detail_enrichment_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_album_detail_enrichment(*args, **kwargs)

def _schedule_album_detail_enrichment_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _schedule_album_detail_enrichment(*args, **kwargs)

def _normalize_user_album_review_text_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _normalize_user_album_review_text(*args, **kwargs)

def api_library_album_download_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_album_download(*args, **kwargs)

def api_library_files_album_cover_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_files_album_cover(*args, **kwargs)

def api_library_external_label_image_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_external_label_image(*args, **kwargs)

def api_library_album_tags_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_album_tags(*args, **kwargs)

def api_library_album_tracks_detail_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_album_tracks_detail(*args, **kwargs)

def _provider_cover_url_from_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_cover_url_from_payload(*args, **kwargs)

def _cover_art_archive_front_urls_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _cover_art_archive_front_urls(*args, **kwargs)

def _cover_art_archive_front_urls_for_identity_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _cover_art_archive_front_urls_for_identity(*args, **kwargs)

def _download_cover_art_archive_front_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _download_cover_art_archive_front(*args, **kwargs)

def _fetch_album_review_web_ai_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_album_review_web_ai(*args, **kwargs)

def _fetch_album_review_web_ai_batch_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_album_review_web_ai_batch(*args, **kwargs)

def _album_review_search_context_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _album_review_search_context(*args, **kwargs)

def _resolve_album_review_identity_from_provider_hints_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _resolve_album_review_identity_from_provider_hints(*args, **kwargs)

def _fetch_album_profile_from_provider_fallback_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_album_profile_from_provider_fallback(*args, **kwargs)

def _album_profile_provider_candidate_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _album_profile_provider_candidate(*args, **kwargs)

def _choose_best_album_profile_provider_candidate_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _choose_best_album_profile_provider_candidate(*args, **kwargs)

def _fetch_best_album_profile_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_best_album_profile(*args, **kwargs)

def _write_pmda_album_tags_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _write_pmda_album_tags(*args, **kwargs)

def _apply_artist_album_tags_to_audio_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _apply_artist_album_tags_to_audio(*args, **kwargs)

def _clean_track_title_from_text_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _clean_track_title_from_text(*args, **kwargs)

def _strip_album_artist_prefixes_from_track_title_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strip_album_artist_prefixes_from_track_title(*args, **kwargs)

def _track_display_fields_from_sources_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _track_display_fields_from_sources(*args, **kwargs)

def _provider_track_titles_cached_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_track_titles_cached(*args, **kwargs)

def _display_tracks_with_provider_overlay_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _display_tracks_with_provider_overlay(*args, **kwargs)

def api_library_album_review_generate_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_album_review_generate(*args, **kwargs)

def api_library_album_select_cover_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_album_select_cover(*args, **kwargs)

def api_musicbrainz_fix_album_tags_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_musicbrainz_fix_album_tags(*args, **kwargs)
def _album_artwork_gallery_cache_key_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _album_artwork_gallery_cache_key(*args, **kwargs)

def _cover_art_archive_gallery_items_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _cover_art_archive_gallery_items(*args, **kwargs)

def _discogs_gallery_items_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _discogs_gallery_items(*args, **kwargs)

def _album_artwork_gallery_manifest_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _album_artwork_gallery_manifest(*args, **kwargs)

def api_library_album_artwork_gallery_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_album_artwork_gallery(*args, **kwargs)

def api_library_files_album_artwork_item_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_library_files_album_artwork_item(*args, **kwargs)
