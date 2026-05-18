"""Runtime-owned published snapshot browse helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_files_library_published_row_count',
    '_files_published_artist_names',
    '_enqueue_files_index_published_catchup',
    '_files_library_published_browse_counts',
    '_files_library_browse_source_requested',
    '_files_library_published_artists',
    '_files_library_published_albums',
    '_files_library_published_genres',
    '_files_library_published_labels',
    '_files_index_maybe_enqueue_published_catchup',
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

def _files_library_published_row_count() -> int:
    try:
        con = _state_browse_readonly_connect(timeout_sec=0.35)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM files_library_published_albums")
        value = int((cur.fetchone() or [0])[0] or 0)
        con.close()
        return value
    except Exception:
        return 0

def _files_published_artist_names(limit: int = 0) -> list[str]:
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
        cur = con.cursor()
        sql = """
            SELECT artist_name, MAX(updated_at) AS latest_ts, COUNT(*) AS album_count
            FROM files_library_published_albums
            WHERE COALESCE(artist_name, '') <> ''
            GROUP BY artist_norm, artist_name
            ORDER BY latest_ts DESC, album_count DESC, lower(artist_name) ASC
        """
        params: list[Any] = []
        if int(limit or 0) > 0:
            sql += " LIMIT ?"
            params.append(int(limit))
        cur.execute(sql, tuple(params))
        rows = cur.fetchall() or []
        con.close()
    except Exception:
        logging.debug("Failed to enumerate published artist names for Files PG catchup", exc_info=True)
        return []
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        name = str((row[0] if row else "") or "").strip()
        norm = _norm_artist_key(name)
        if not name or not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(name)
    return out

def _enqueue_files_index_published_catchup(reason: str = "published_rows") -> bool:
    global _FILES_INDEX_PUBLISHED_CATCHUP_RUNNING
    if files_index_lock.locked():
        logging.info(
            "[Files Index] ·ℹ Published catchup deferred reason=%s because a full index rebuild is active",
            str(reason or "published_rows"),
        )
        return False
    with _FILES_INDEX_PUBLISHED_CATCHUP_LOCK:
        if _FILES_INDEX_PUBLISHED_CATCHUP_RUNNING:
            return False
        _FILES_INDEX_PUBLISHED_CATCHUP_RUNNING = True

    def _runner() -> None:
        global _FILES_INDEX_PUBLISHED_CATCHUP_RUNNING
        processed = 0
        ok_count = 0
        failed = 0
        started_at = time.time()
        try:
            artist_names = _files_published_artist_names()
            with lock:
                state["scan_published_catchup_running"] = True
                state["scan_published_catchup_reason"] = str(reason or "published_rows")
                state["scan_published_catchup_done"] = 0
                state["scan_published_catchup_total"] = int(len(artist_names))
                state["scan_published_catchup_ok"] = 0
                state["scan_published_catchup_failed"] = 0
                state["scan_published_catchup_current_artist"] = None
                state["scan_published_catchup_started_at"] = started_at
                state["scan_published_catchup_updated_at"] = started_at
                state["scan_published_catchup_finished_at"] = None
            logging.info(
                "[Files Index] ↻🔄 Starting published catchup reason=%s artists=%d",
                str(reason or "published_rows"),
                len(artist_names),
            )
            for artist_name in artist_names:
                if files_index_lock.locked():
                    logging.info(
                        "[Files Index] ·ℹ Published catchup paused reason=%s processed=%d/%d because a full index rebuild started",
                        str(reason or "published_rows"),
                        processed,
                        len(artist_names),
                    )
                    break
                with lock:
                    state["scan_published_catchup_current_artist"] = str(artist_name or "").strip() or None
                    state["scan_published_catchup_updated_at"] = time.time()
                try:
                    result = _rebuild_files_library_index_for_artist(
                        artist_name,
                        reason=f"published_catchup_{reason}",
                        wait_if_running=False,
                    )
                    processed += 1
                    if bool(result.get("ok")):
                        ok_count += 1
                    else:
                        failed += 1
                    with lock:
                        state["scan_published_catchup_done"] = int(processed)
                        state["scan_published_catchup_ok"] = int(ok_count)
                        state["scan_published_catchup_failed"] = int(failed)
                        state["scan_published_catchup_updated_at"] = time.time()
                    if processed % 25 == 0:
                        logging.info(
                            "[Files Index] ·ℹ Published catchup progress processed=%d ok=%d failed=%d",
                            processed,
                            ok_count,
                            failed,
                        )
                except Exception:
                    processed += 1
                    failed += 1
                    with lock:
                        state["scan_published_catchup_done"] = int(processed)
                        state["scan_published_catchup_ok"] = int(ok_count)
                        state["scan_published_catchup_failed"] = int(failed)
                        state["scan_published_catchup_updated_at"] = time.time()
                    logging.debug("Published catchup artist sync failed for %s", artist_name, exc_info=True)
            logging.info(
                "[Files Index] V✅ Published catchup finished reason=%s processed=%d ok=%d failed=%d elapsed=%.2fs",
                str(reason or "published_rows"),
                processed,
                ok_count,
                failed,
                time.time() - started_at,
            )
        finally:
            finished_at = time.time()
            with lock:
                state["scan_published_catchup_running"] = False
                state["scan_published_catchup_done"] = int(processed)
                state["scan_published_catchup_total"] = max(
                    int(state.get("scan_published_catchup_total") or 0),
                    int(processed),
                )
                state["scan_published_catchup_ok"] = int(ok_count)
                state["scan_published_catchup_failed"] = int(failed)
                state["scan_published_catchup_current_artist"] = None
                state["scan_published_catchup_updated_at"] = finished_at
                state["scan_published_catchup_finished_at"] = finished_at
            with _FILES_INDEX_PUBLISHED_CATCHUP_LOCK:
                _FILES_INDEX_PUBLISHED_CATCHUP_RUNNING = False

    threading.Thread(
        target=_runner,
        daemon=True,
        name="files-index-published-catchup",
    ).start()
    return True

def _files_library_published_browse_counts(
    include_unmatched: bool,
    *,
    scope: str = "library",
) -> tuple[int, int, int]:
    """
    Fallback browse counts from progressive publication rows in state.db.
    This keeps the UI readable while PG live index is rebuilding.
    """
    where_sql, params = _files_library_published_album_where_sqlite(
        include_unmatched=include_unmatched,
        scope=scope,
    )
    con = _state_browse_readonly_connect(timeout_sec=0.35)
    try:
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT
                COUNT(*) AS albums,
                COUNT(DISTINCT lower(trim(COALESCE(NULLIF(artist_norm, ''), artist_name)))) AS artists,
                COALESCE(SUM(COALESCE(track_count, 0)), 0) AS tracks
            FROM files_library_published_albums
            WHERE {where_sql}
            """,
            tuple(params),
        )
        row = cur.fetchone() or (0, 0, 0)
        return (int(row[0] or 0), int(row[1] or 0), int(row[2] or 0))
    except Exception:
        logging.debug("Failed to read published browse counts", exc_info=True)
        return (0, 0, 0)
    finally:
        try:
            con.close()
        except Exception:
            pass

def _files_library_browse_source_requested() -> str:
    raw = str(request.args.get("browse_source") or "").strip().lower()
    if raw in {"live", "published"}:
        return raw
    return "auto"

def _files_library_published_artists(
    *,
    include_unmatched: bool,
    scope: str = "library",
    search_query: str = "",
    genre: str = "",
    label: str = "",
    year: int = 0,
    sort: str = "recent",
    limit: int = 100,
    offset: int = 0,
    status_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    status_ctx = status_context if isinstance(status_context, dict) else _files_library_live_status_context(source_is_published=True)
    where_sql, params = _files_library_published_album_where_sqlite(
        include_unmatched=include_unmatched,
        scope=scope,
        search_query=search_query,
        genre=genre,
        label=label,
        year=year,
    )
    if sort == "alpha":
        order_sql = "ORDER BY lower(artist_name) ASC, album_count DESC, last_added_at DESC, artist_norm ASC"
    elif sort == "albums":
        order_sql = "ORDER BY album_count DESC, last_added_at DESC, lower(artist_name) ASC, artist_norm ASC"
    else:
        order_sql = "ORDER BY last_added_at DESC, album_count DESC, lower(artist_name) ASC, artist_norm ASC"
    con = _state_browse_readonly_connect(timeout_sec=0.75)
    try:
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT artist_norm
                FROM files_library_published_albums
                WHERE {where_sql}
                GROUP BY artist_norm
            ) artists
            """,
            tuple(params),
        )
        total = int((cur.fetchone() or [0])[0] or 0)
        cur.execute(
            f"""
            SELECT
                artist_norm,
                MIN(COALESCE(NULLIF(trim(artist_name), ''), 'Unknown Artist')) AS artist_name,
                COUNT(*) AS album_count,
                COALESCE(SUM(CASE WHEN COALESCE(is_broken, 0) <> 0 THEN 1 ELSE 0 END), 0) AS broken_albums_count,
                MAX(COALESCE(has_artist_image, 0)) AS has_artist_image,
                MAX(COALESCE(updated_at, 0)) AS last_added_at
            FROM files_library_published_albums
            WHERE {where_sql}
            GROUP BY artist_norm
            {order_sql}
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), int(offset)),
        )
        rows = cur.fetchall()
    except Exception:
        logging.debug("Failed to build published artists fallback", exc_info=True)
        return {"artists": [], "total": 0, "limit": int(limit), "offset": int(offset), "has_more": False, "stale": True, "fallback_source": "published"}
    finally:
        try:
            con.close()
        except Exception:
            pass

    base_url = request.url_root.rstrip("/")
    artist_ids = _files_library_resolve_artist_ids_by_norms([str(row[0] or "").strip() for row in rows])
    artists_payload: list[dict[str, Any]] = []
    for artist_norm, artist_name, album_count, broken_albums_count, has_artist_image, last_added_at in rows:
        norm_key = str(artist_norm or "").strip()
        artist_id = int(artist_ids.get(norm_key) or 0)
        has_image = bool(has_artist_image)
        image_version = int(float(last_added_at or 0.0) or 0)
        status_fields = _files_library_artist_status_fields(
            status_context=status_ctx,
            has_image=has_image,
            has_profile=False,
            has_fallback_thumb=False,
        )
        artists_payload.append(
            {
                "artist_id": artist_id,
                "artist_name": str(artist_name or "").strip() or "Unknown Artist",
                "album_count": int(album_count or 0),
                "broken_albums_count": int(broken_albums_count or 0),
                "artist_has_image": has_image,
                "artist_profile_source": None,
                "artist_thumb": (
                    _artist_image_asset_url(base_url, artist_id, size=512, version=image_version)
                    if has_image and artist_id > 0
                    else None
                ),
                **status_fields,
            }
        )
    return {
        "artists": artists_payload,
        "total": int(total),
        "limit": int(limit),
        "offset": int(offset),
        "has_more": _pagination_core.page_has_more(total=total, offset=offset, returned=len(rows)),
        "publication_state": str(status_ctx.get("publication_state") or "published"),
        "background_enrichment_running": bool(status_ctx.get("background_enrichment_running")),
        "stale": True,
        "fallback_source": "published",
    }

def _files_library_published_albums(
    *,
    include_unmatched: bool,
    scope: str = "library",
    search_query: str = "",
    genre: str = "",
    label: str = "",
    year: int = 0,
    sort: str = "recent",
    limit: int = 80,
    offset: int = 0,
    status_context: dict[str, Any] | None = None,
    allow_live_resolution: bool = True,
) -> dict[str, Any]:
    status_ctx = status_context if isinstance(status_context, dict) else _files_library_live_status_context(source_is_published=True)
    where_sql, params = _files_library_published_album_where_sqlite(
        include_unmatched=include_unmatched,
        scope=scope,
        search_query=search_query,
        genre=genre,
        label=label,
        year=year,
    )
    sort_norm = str(sort or "recent").strip().lower()
    if sort_norm == "alpha":
        order_sql = "ORDER BY lower(album_title) ASC, lower(artist_name) ASC, COALESCE(updated_at, 0) DESC"
    elif sort_norm == "artist":
        order_sql = "ORDER BY lower(artist_name) ASC, COALESCE(year, 0) DESC, lower(album_title) ASC"
    elif sort_norm == "year_desc":
        order_sql = "ORDER BY COALESCE(year, 0) DESC, lower(album_title) ASC, COALESCE(updated_at, 0) DESC"
    else:
        order_sql = "ORDER BY COALESCE(updated_at, 0) DESC, lower(artist_name) ASC, lower(album_title) ASC"
    con = _state_browse_readonly_connect(timeout_sec=0.75)
    try:
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM files_library_published_albums
            WHERE {where_sql}
            """,
            tuple(params),
        )
        total = int((cur.fetchone() or [0])[0] or 0)
        cur.execute(
            f"""
            SELECT
                folder_path,
                COALESCE(source_id, 0),
                artist_name,
                artist_norm,
                album_title,
                COALESCE(year, 0),
                COALESCE(genre, ''),
                COALESCE(label, ''),
                COALESCE(tags_json, '[]'),
                COALESCE(format, ''),
                COALESCE(is_lossless, 0),
                COALESCE(has_cover, 0),
                COALESCE(has_artist_image, 0),
                COALESCE(mb_identified, 0),
                COALESCE(strict_match_verified, 0),
                COALESCE(strict_match_provider, ''),
                COALESCE(musicbrainz_release_group_id, ''),
                COALESCE(discogs_release_id, ''),
                COALESCE(lastfm_album_mbid, ''),
                COALESCE(bandcamp_album_url, ''),
                COALESCE(primary_metadata_source, ''),
                COALESCE(track_count, 0),
                COALESCE(primary_tags_json, '{{}}'),
                COALESCE(updated_at, 0)
            FROM files_library_published_albums
            WHERE {where_sql}
            {order_sql}
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), int(offset)),
        )
        rows = cur.fetchall()
    except Exception:
        logging.debug("Failed to build published albums fallback", exc_info=True)
        return {"albums": [], "total": 0, "limit": int(limit), "offset": int(offset), "has_more": False, "scope": scope, "stale": True, "fallback_source": "published"}
    finally:
        try:
            con.close()
        except Exception:
            pass

    # Published snapshot rows may carry a legacy source/root id in source_id. Resolve
    # the real live album id by canonical folder path whenever the PG index is ready.
    folder_paths_for_album_id_resolution = [
        str(row[0] or "").strip()
        for row in rows
        if str(row[0] or "").strip()
    ]
    folder_to_album_id: dict[str, int] = {}
    album_id_resolution_ready = False
    if bool(allow_live_resolution) and folder_paths_for_album_id_resolution:
        try:
            folder_to_album_id = _files_library_resolve_album_ids_by_folder_paths(folder_paths_for_album_id_resolution)
            album_id_resolution_ready = bool(folder_to_album_id)
        except Exception:
            logging.debug("Failed to resolve published album ids by folder path", exc_info=True)
    artist_ids = _files_library_resolve_artist_ids_by_norms([str(row[3] or "").strip() for row in rows])
    page_album_ids = [
        int(folder_to_album_id.get(str(row[0] or "").strip()) or 0) or int(row[1] or 0)
        for row in rows
    ]
    live_cover_flags = {}
    if bool(allow_live_resolution) and bool(album_id_resolution_ready) and page_album_ids:
        try:
            live_cover_flags = _files_library_resolve_album_cover_flags_by_ids(page_album_ids)
        except Exception:
            logging.debug("Failed to resolve live cover flags for published albums", exc_info=True)
    base_url = request.url_root.rstrip("/")
    albums_payload: list[dict[str, Any]] = []
    for (
        folder_path,
        source_id,
        artist_name,
        artist_norm,
        album_title,
        album_year,
        album_genre,
        album_label,
        tags_json,
        album_format,
        is_lossless,
        has_cover,
        has_artist_image,
        mb_identified,
        strict_match_verified,
        strict_match_provider,
        musicbrainz_release_group_id,
        discogs_release_id,
        lastfm_album_mbid,
        bandcamp_album_url,
        primary_metadata_source,
        track_count,
        primary_tags_json,
        _updated_at,
    ) in rows:
        folder_key = str(folder_path or "").strip()
        artist_norm_key = str(artist_norm or "").strip()
        album_id = int(folder_to_album_id.get(folder_key) or 0) or int(source_id or 0)
        artist_id = int(artist_ids.get(artist_norm_key) or 0)
        has_cover_effective = bool(live_cover_flags.get(album_id, bool(has_cover))) if album_id > 0 else bool(has_cover)
        genres_list = _merge_album_genre_lists(tags_json, "[]", str(album_genre or ""))
        primary_tags = _safe_json_load(primary_tags_json, fallback={})
        if not isinstance(primary_tags, dict):
            primary_tags = {}
        classical_payload = _classical_display_payload(
            primary_tags,
            fallback_title=str(album_title or ""),
            fallback_artist=str(artist_name or ""),
        )
        profile_eligible = bool(
            mb_identified
            or str(primary_metadata_source or "").strip()
            or str(strict_match_provider or "").strip()
            or str(musicbrainz_release_group_id or "").strip()
            or str(discogs_release_id or "").strip()
            or str(lastfm_album_mbid or "").strip()
            or str(bandcamp_album_url or "").strip()
        )
        status_fields = _files_library_album_status_fields(
            status_context=status_ctx,
            has_cover=has_cover_effective,
            has_artist_image=bool(has_artist_image),
            has_profile=False,
            cover_eligible=bool(profile_eligible),
            artist_media_eligible=True,
            profile_eligible=bool(profile_eligible),
        )
        albums_payload.append(
            {
                "album_id": album_id,
                "title": str(album_title or "").strip() or "Unknown Album",
                "year": int(album_year or 0) or None,
                "genre": str(album_genre or "").strip() or None,
                "genres": genres_list,
                "label": str(album_label or "").strip() or None,
                "track_count": int(track_count or 0),
                "format": str(album_format or "").strip() or None,
                "is_lossless": bool(is_lossless),
                "sample_rate": None,
                "bit_depth": None,
                "mb_identified": bool(mb_identified or strict_match_verified),
                "musicbrainz_release_group_id": str(musicbrainz_release_group_id or "").strip() or None,
                "discogs_release_id": str(discogs_release_id or "").strip() or None,
                "lastfm_album_mbid": str(lastfm_album_mbid or "").strip() or None,
                "bandcamp_album_url": str(bandcamp_album_url or "").strip() or None,
                "metadata_source": _normalize_identity_provider(str(primary_metadata_source or "")) or None,
                "strict_match_provider": _normalize_identity_provider(str(strict_match_provider or "")) or None,
                "thumb": (
                    f"{base_url}/api/library/files/album/{album_id}/cover?size=512"
                    if has_cover_effective and album_id > 0
                    else None
                ),
                "artist_id": artist_id,
                "artist_name": _files_album_display_artist_name(
                    artist_name=str(artist_name or "").strip(),
                    classical_payload=classical_payload,
                ),
                "short_description": None,
                "profile_source": None,
                "user_rating": None,
                "public_rating": None,
                "public_rating_votes": 0,
                "public_rating_source": None,
                "heat_score": None,
                "classical": classical_payload,
                **status_fields,
            }
        )
    return {
        "albums": albums_payload,
        "total": int(total),
        "limit": int(limit),
        "offset": int(offset),
        "has_more": _pagination_core.page_has_more(total=total, offset=offset, returned=len(rows)),
        "scope": scope,
        "publication_state": str(status_ctx.get("publication_state") or "published"),
        "background_enrichment_running": bool(status_ctx.get("background_enrichment_running")),
        "stale": True,
        "fallback_source": "published",
    }

def _files_library_published_genres(
    *,
    include_unmatched: bool,
    scope: str = "library",
    search: str = "",
    label: str = "",
    year: int = 0,
    limit: int = 80,
    offset: int = 0,
) -> dict[str, Any]:
    """Fallback genre list from files_library_published_albums while the PG index is rebuilding."""
    where_sql, params = _files_library_published_album_where_sqlite(
        include_unmatched=include_unmatched,
        scope=scope,
        search_query="",
        label=label,
        year=year,
    )
    needle = str(search or "").strip().lower()
    con = _state_browse_readonly_connect(timeout_sec=0.75)
    try:
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT COALESCE(tags_json, '[]'), COALESCE(genre, '')
            FROM files_library_published_albums
            WHERE {where_sql}
            """,
            tuple(params),
        )
        counts: dict[str, dict[str, Any]] = {}
        for tags_json_raw, genre_raw in cur.fetchall():
            per_album: dict[str, str] = {}
            try:
                parsed_tags = json.loads(str(tags_json_raw or "[]"))
            except Exception:
                parsed_tags = []
            if isinstance(parsed_tags, list):
                for raw_value in parsed_tags:
                    for split_value in _split_genre_values(str(raw_value or "")):
                        value = str(split_value or "").strip()
                        if not value:
                            continue
                        if any(sep in value for sep in (";", ",", "[", "]", "{", "}")):
                            continue
                        norm = value.lower()
                        if needle and needle not in norm:
                            continue
                        per_album.setdefault(norm, value)
            if not per_album:
                for split_value in _split_genre_values(str(genre_raw or "")):
                    legacy = str(split_value or "").strip()
                    if not legacy:
                        continue
                    if any(sep in legacy for sep in (";", ",", "[", "]", "{", "}")):
                        continue
                    norm = legacy.lower()
                    if not needle or needle in norm:
                        per_album.setdefault(norm, legacy)
            for norm, display in per_album.items():
                bucket = counts.setdefault(norm, {"value": display, "count": 0})
                bucket["count"] = int(bucket.get("count") or 0) + 1
                current_display = str(bucket.get("value") or "")
                if len(display) > len(current_display) or (len(display) == len(current_display) and display < current_display):
                    bucket["value"] = display

        ordered = sorted(
            (bucket for bucket in counts.values() if str(bucket.get("value") or "").strip()),
            key=lambda item: (-int(item.get("count") or 0), str(item.get("value") or "").lower()),
        )
        total = len(ordered)
        sliced = ordered[int(max(0, offset)): int(max(0, offset)) + int(max(1, limit))]
        return {
            "genres": [{"value": str(item["value"]).strip(), "count": int(item["count"] or 0)} for item in sliced],
            "total": int(total),
            "limit": int(limit),
            "offset": int(offset),
            "stale": True,
            "fallback_source": "published",
        }
    except Exception:
        logging.debug("Failed to build published genre fallback", exc_info=True)
        return {"genres": [], "total": 0, "limit": int(limit), "offset": int(offset), "stale": True}
    finally:
        try:
            con.close()
        except Exception:
            pass

def _files_library_published_labels(
    *,
    include_unmatched: bool,
    scope: str = "library",
    search: str = "",
    genre: str = "",
    year: int = 0,
    limit: int = 80,
    offset: int = 0,
) -> dict[str, Any]:
    """Fallback label list from files_library_published_albums while the PG index is rebuilding."""
    where_sql, params = _files_library_published_album_where_sqlite(
        include_unmatched=include_unmatched,
        scope=scope,
        search_query="",
        genre=genre,
        label="",
        year=year,
    )
    search_value = str(search or "").strip()
    search_norm = search_value.lower()
    con = _state_browse_readonly_connect(timeout_sec=0.75)
    try:
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT
                TRIM(COALESCE(label, '')) AS label_disp,
                folder_path,
                COALESCE(has_cover, 0) AS has_cover,
                COALESCE(updated_at, 0) AS updated_at
            FROM files_library_published_albums
            WHERE {where_sql}
              AND COALESCE(TRIM(label), '') <> ''
            """,
            tuple(params),
        )
        buckets: dict[str, dict[str, Any]] = {}
        for label_disp_raw, folder_path_raw, has_cover_raw, updated_at_raw in cur.fetchall():
            label_disp = str(label_disp_raw or "").strip()
            if not label_disp:
                continue
            label_norm = label_disp.lower()
            if search_norm and search_norm not in label_norm:
                continue
            bucket = buckets.setdefault(
                label_norm,
                {
                    "value": label_disp,
                    "count": 0,
                    "folder_path": "",
                    "has_cover": False,
                    "updated_at": 0.0,
                },
            )
            bucket["count"] = int(bucket.get("count") or 0) + 1
            current_value = str(bucket.get("value") or "")
            if len(label_disp) > len(current_value) or (len(label_disp) == len(current_value) and label_disp < current_value):
                bucket["value"] = label_disp
            folder_path = str(folder_path_raw or "").strip()
            has_cover = bool(has_cover_raw)
            updated_at = float(updated_at_raw or 0.0)
            current_has_cover = bool(bucket.get("has_cover"))
            current_updated_at = float(bucket.get("updated_at") or 0.0)
            if has_cover and (not current_has_cover or updated_at >= current_updated_at):
                bucket["folder_path"] = folder_path
                bucket["has_cover"] = True
                bucket["updated_at"] = updated_at
            elif not current_has_cover and updated_at >= current_updated_at:
                bucket["folder_path"] = folder_path
                bucket["updated_at"] = updated_at

        ordered = sorted(
            (bucket for bucket in buckets.values() if str(bucket.get("value") or "").strip()),
            key=lambda item: (-int(item.get("count") or 0), str(item.get("value") or "").lower()),
        )
        total = len(ordered)
        sliced = ordered[int(max(0, offset)): int(max(0, offset)) + int(max(1, limit))]
        folder_paths = [str(item.get("folder_path") or "").strip() for item in sliced if str(item.get("folder_path") or "").strip()]
        folder_to_album_id = _files_library_resolve_album_ids_by_folder_paths(folder_paths)
        base_url = request.url_root.rstrip("/")
        labels = []
        for item in sliced:
            folder_path = str(item.get("folder_path") or "").strip()
            album_id = int(folder_to_album_id.get(folder_path) or 0)
            thumb = (
                f"{base_url}/api/library/files/album/{album_id}/cover?size=128"
                if bool(item.get("has_cover")) and album_id > 0
                else None
            )
            labels.append(
                {
                    "value": str(item.get("value") or "").strip(),
                    "count": int(item.get("count") or 0),
                    "thumb": thumb,
                }
            )
        return {
            "labels": labels,
            "total": int(total),
            "limit": int(limit),
            "offset": int(offset),
            "stale": True,
            "fallback_source": "published",
        }
    except Exception:
        logging.debug("Failed to build published label fallback", exc_info=True)
        return {"labels": [], "total": 0, "limit": int(limit), "offset": int(offset), "stale": True, "fallback_source": "published"}
    finally:
        try:
            con.close()
        except Exception:
            pass

def _files_index_maybe_enqueue_published_catchup(
    *,
    include_unmatched: bool,
    scope: str = "library",
    reason: str = "published_rows",
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reason_norm = str(reason or "").strip().lower()
    # Browse/API request paths must stay read-only and fast. A normal Albums/Artists
    # page render must not enqueue a catchup that walks tens of thousands of artists,
    # otherwise a stale/underbuilt snapshot turns into user-visible timeouts.
    if reason_norm.startswith("api_"):
        snap = snapshot if isinstance(snapshot, dict) else _files_library_api_browse_snapshot(include_unmatched, scope=scope)
        return snap
    snap = snapshot if isinstance(snapshot, dict) else _files_library_browse_snapshot(include_unmatched, scope=scope)
    with lock:
        scan_busy = bool(
            state.get("scanning")
            or state.get("scan_starting")
            or state.get("scan_finalizing")
            or state.get("scan_post_processing")
        )
    if scan_busy or files_index_lock.locked():
        return snap
    if bool(snap.get("underbuilt")) and int(snap.get("published_albums") or 0) > 0:
        try:
            _enqueue_files_index_published_catchup(reason=reason)
        except Exception:
            logging.debug("Failed to enqueue Files published catchup reason=%s", reason, exc_info=True)
    return snap

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}

def _files_library_published_row_count_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_library_published_row_count(*args, **kwargs)

def _files_published_artist_names_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_published_artist_names(*args, **kwargs)

def _enqueue_files_index_published_catchup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _enqueue_files_index_published_catchup(*args, **kwargs)

def _files_library_published_browse_counts_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_library_published_browse_counts(*args, **kwargs)

def _files_library_browse_source_requested_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_library_browse_source_requested(*args, **kwargs)

def _files_library_published_artists_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_library_published_artists(*args, **kwargs)

def _files_library_published_albums_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_library_published_albums(*args, **kwargs)

def _files_library_published_genres_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_library_published_genres(*args, **kwargs)

def _files_library_published_labels_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_library_published_labels(*args, **kwargs)

def _files_index_maybe_enqueue_published_catchup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_index_maybe_enqueue_published_catchup(*args, **kwargs)
