"""Runtime-owned scan resume, scan-plan, and scan-cache helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_mcp_scan_resume_state',
    '_reconcile_scan_duplicates_across_artist_buckets',
    'save_scan_editions_to_db',
    'save_scan_editions_artist_to_db',
    '_refresh_scan_history_from_published',
    'load_scan_from_db',
    '_load_files_album_scan_cache_map',
    '_upsert_files_album_scan_cache_rows',
    '_snapshot_files_album_scan_cache_from_prescan',
    '_refresh_files_album_scan_cache_from_editions',
    '_load_files_album_scan_cache_map_for_keys',
    '_resume_files_plan_row_tuple',
    '_persist_resume_files_plan',
    '_upsert_resume_files_plan_partial',
    '_prune_resume_files_plan_artist',
    '_prune_resume_files_plan_albums',
    '_ensure_resume_run_started',
    '_persist_resume_discovery_snapshot',
    '_persist_resume_discovery_progress_only',
    '_load_resume_discovery_snapshot_by_run_id',
    '_snapshot_current_resume_discovery',
    '_snapshot_current_resume_state',
    '_restore_resume_files_plan_from_run_row',
    '_load_resume_files_plan_by_run_id',
    '_load_resume_files_plan_partial_by_run_id',
    '_load_resume_files_plan',
    '_snapshot_current_resume_files_plan',
    '_hydrate_resume_files_edition',
    '_prepare_resume_scan_artists',
    '_has_unfinished_resume_run',
    '_get_resume_run_snapshot',
    '_get_resume_run_snapshot_by_run_id',
    '_get_latest_resume_run_snapshot_any_signature',
    '_get_startup_resume_snapshot',
    '_maybe_resume_interrupted_scan_on_startup',
    '_set_resume_artist_status',
    '_set_resume_run_status',
    '_build_scan_plan',
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

def _mcp_scan_resume_state(args: dict[str, Any]) -> dict[str, Any]:
    args = args if isinstance(args, dict) else {}
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=15)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        if not _sqlite_table_exists(cur, "scan_resume_runs"):
            return {"available": False, "reason": "scan_resume_runs_missing"}
        run_id = str(args.get("run_id") or "").strip()
        if not run_id:
            with lock:
                run_id = str(state.get("scan_resume_run_id") or state.get("scan_resume_requested_run_id") or "").strip()
        if not run_id:
            row = cur.execute(
                "SELECT * FROM scan_resume_runs ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
        else:
            row = cur.execute("SELECT * FROM scan_resume_runs WHERE run_id = ?", (run_id,)).fetchone()
        if not row:
            return {"available": True, "run": None, "artist_status_counts": {}, "plan": {}}
        run = dict(row)
        run_id = str(run.get("run_id") or "")
        artist_status_counts: dict[str, int] = {}
        if _sqlite_table_exists(cur, "scan_resume_artists"):
            cur.execute(
                """
                SELECT LOWER(COALESCE(status, 'pending')) AS status, COUNT(*) AS count
                FROM scan_resume_artists
                WHERE run_id = ?
                GROUP BY LOWER(COALESCE(status, 'pending'))
                """,
                (run_id,),
            )
            artist_status_counts = {str(r["status"] or "pending"): int(r["count"] or 0) for r in cur.fetchall()}
        plan = {}
        if _sqlite_table_exists(cur, "scan_resume_files_plan"):
            cur.execute(
                """
                SELECT COUNT(*) AS albums,
                       COUNT(DISTINCT artist_name) AS artists,
                       COALESCE(SUM(file_count), 0) AS files,
                       COALESCE(SUM(CASE WHEN skip_heavy_processing = 1 THEN 1 ELSE 0 END), 0) AS skip_heavy_processing,
                       COALESCE(SUM(CASE WHEN strict_match_verified = 1 THEN 1 ELSE 0 END), 0) AS strict_match_verified,
                       COALESCE(SUM(CASE WHEN has_identity = 1 THEN 1 ELSE 0 END), 0) AS has_identity
                FROM scan_resume_files_plan
                WHERE run_id = ?
                """,
                (run_id,),
            )
            plan = dict(cur.fetchone() or {})
        discovery = {}
        if _sqlite_table_exists(cur, "scan_resume_discovery_files"):
            cur.execute(
                """
                SELECT COUNT(*) AS files, COUNT(DISTINCT root_index) AS roots
                FROM scan_resume_discovery_files
                WHERE run_id = ?
                """,
                (run_id,),
            )
            discovery = dict(cur.fetchone() or {})
        return {
            "available": True,
            "run": run,
            "artist_status_counts": artist_status_counts,
            "plan": plan,
            "discovery": discovery,
        }
    finally:
        con.close()

def _reconcile_scan_duplicates_across_artist_buckets(
    all_results: dict[str, list[dict]] | None,
    all_editions_by_artist: dict[str, list[dict]] | None,
) -> tuple[dict[str, list[dict]], dict[str, list[dict]], dict[str, int]]:
    """
    Re-run duplicate grouping on canonical artist buckets when scan discovery split the same
    artist across multiple raw buckets (e.g. "sigur ros" vs "Sigur Rós").

    This keeps dedupe moves correct even when local tags/folders are inconsistent during discovery.
    """
    results_map: dict[str, list[dict]] = {
        str(k): list(v or [])
        for k, v in (all_results or {}).items()
    }
    editions_map: dict[str, list[dict]] = {
        str(k): [dict(e or {}) for e in (v or []) if isinstance(e, dict)]
        for k, v in (all_editions_by_artist or {}).items()
    }
    metrics = {
        "candidate_buckets": 0,
        "reconciled_buckets": 0,
        "groups_found": 0,
    }
    if not editions_map:
        return results_map, editions_map, metrics

    canonical: dict[str, dict[str, Any]] = {}
    for source_artist, editions in editions_map.items():
        source_artist_name = str(source_artist or "").strip() or "Unknown Artist"
        for edition in editions:
            resolved = dict(edition or {})
            folder_name = ""
            try:
                folder_name = Path(str(resolved.get("folder") or "")).name
            except Exception:
                folder_name = ""
            artist_final, _album_final = _apply_resolved_identity_to_edition(
                resolved,
                default_artist=source_artist_name,
                default_title=str(resolved.get("title_raw") or resolved.get("album_title") or ""),
                folder_name=folder_name,
            )
            canon_norm = _norm_artist_key(artist_final) or _norm_artist_key(source_artist_name) or "unknown artist"
            bucket = canonical.setdefault(
                canon_norm,
                {
                    "display_artist": "",
                    "source_artists": set(),
                    "resolved_variants": set(),
                    "album_ids": set(),
                    "editions": [],
                },
            )
            bucket["display_artist"] = _choose_preferred_identity_display(
                str(bucket.get("display_artist") or ""),
                artist_final or source_artist_name,
            )
            bucket["source_artists"].add(source_artist_name)
            bucket["resolved_variants"].add(str(artist_final or source_artist_name).strip())
            aid = _parse_int_loose(resolved.get("album_id"), 0)
            if aid > 0:
                bucket["album_ids"].add(int(aid))
            bucket["editions"].append(resolved)

    for payload in canonical.values():
        editions = list(payload.get("editions") or [])
        if len(editions) < 2:
            continue
        source_artists = {str(x or "").strip() for x in (payload.get("source_artists") or set()) if str(x or "").strip()}
        resolved_variants = {str(x or "").strip() for x in (payload.get("resolved_variants") or set()) if str(x or "").strip()}
        if len(source_artists) <= 1 and len(resolved_variants) <= 1:
            continue
        metrics["candidate_buckets"] += 1
        display_artist = str(payload.get("display_artist") or "").strip() or next(iter(source_artists), "Unknown Artist")
        bucket_album_ids = {int(x) for x in (payload.get("album_ids") or set()) if int(x or 0) > 0}
        try:
            groups, _stats, reconciled_editions = scan_duplicates(
                None,
                display_artist,
                [],
                prebuilt_editions=editions,
            )
        except Exception:
            logging.debug(
                "Cross-bucket duplicate reconciliation failed for artist=%s",
                display_artist,
                exc_info=True,
            )
            continue

        for source_artist, source_groups in list(results_map.items()):
            filtered_groups: list[dict] = []
            for group in source_groups or []:
                if _duplicate_group_album_ids(group) & bucket_album_ids:
                    continue
                filtered_groups.append(group)
            if filtered_groups:
                results_map[source_artist] = filtered_groups
            else:
                results_map.pop(source_artist, None)

        for source_artist, source_editions in list(editions_map.items()):
            filtered_editions: list[dict] = []
            for edition in source_editions or []:
                aid = _parse_int_loose((edition or {}).get("album_id"), 0)
                if aid > 0 and int(aid) in bucket_album_ids:
                    continue
                filtered_editions.append(edition)
            if filtered_editions:
                editions_map[source_artist] = filtered_editions
            else:
                editions_map.pop(source_artist, None)

        if reconciled_editions:
            editions_map[display_artist] = [dict(e or {}) for e in reconciled_editions if isinstance(e, dict)]
        if groups:
            results_map[display_artist] = list(groups)
        else:
            results_map.pop(display_artist, None)

        metrics["reconciled_buckets"] += 1
        metrics["groups_found"] += len(groups or [])
        logging.info(
            "Cross-bucket duplicate reconciliation: artist=%s source_buckets=%d editions=%d groups=%d",
            display_artist,
            len(source_artists),
            len(editions),
            len(groups or []),
        )

    return results_map, editions_map, metrics

def save_scan_editions_to_db(
    scan_id: int,
    all_editions_by_artist: Dict[str, List[dict]],
    progress_callback=None,
):
    """
    Persist per-edition scan data to scan_editions for Library and Tag Fixer to use.
    Call after a scan completes (or is stopped) so last_completed_scan_id can be used to read from this table.
    """
    import json
    mode = _get_library_mode()
    cache_map = _load_files_album_scan_cache_map() if mode == "files" else {}
    total_rows_expected = 0
    try:
        total_rows_expected = sum(len(items or []) for items in (all_editions_by_artist or {}).values())
    except Exception:
        total_rows_expected = 0
    if progress_callback:
        try:
            progress_callback(0, int(total_rows_expected or 0), "Preparing scan edition rows")
        except Exception:
            pass
    con = _state_connect(timeout=30)
    cur = con.cursor()
    cur.execute("DELETE FROM scan_editions WHERE scan_id = ?", (scan_id,))
    row_count = 0
    for artist, editions_list in all_editions_by_artist.items():
        for e in editions_list:
            folder = e.get("folder")
            meta = e.get("meta", {})
            folder_name = ""
            if folder:
                try:
                    folder_name = (Path(folder).name or "").strip()
                except Exception:
                    folder_name = ""
            artist_resolved, title_resolved = _apply_resolved_identity_to_edition(
                e,
                default_artist=str(artist or ""),
                default_title=str(e.get("title_raw") or e.get("album_title") or ""),
                folder_name=folder_name,
            )
            has_cover = 1 if _edition_cached_has_cover(e, meta) else 0
            ordered_paths: list[Any] = []
            if mode == "files":
                ordered_paths = list(e.get("ordered_paths") or e.get("canonical_ordered_paths") or [])
            # missing_required_tags (REQUIRED_TAGS from Settings = source of truth)
            edition_for_required = e
            if mode == "files" and not (edition_for_required.get("tracks") or []):
                derived_tracks = [
                    {"title": p.stem or f"Track {i + 1}", "idx": i + 1}
                    for i, p in enumerate(ordered_paths)
                ]
                edition_for_required = dict(e)
                edition_for_required["tracks"] = derived_tracks
            missing_required = _check_required_tags(meta, REQUIRED_TAGS, edition=edition_for_required)
            missing_required_json = json.dumps(missing_required) if missing_required else None
            folder_str = str(folder) if folder else ""
            fmt_text = _edition_cached_format_text(e, meta)
            if mode == "files":
                folder_key = ""
                try:
                    if folder:
                        folder_key = _album_folder_cache_key(Path(folder))
                except Exception:
                    folder_key = ""
                cached = cache_map.get(folder_key) or {}
                identity_fields = _extract_files_identity_fields(tags=meta, edition=e, cached=cached)
                mbid = identity_fields["musicbrainz_id"]
                discogs_release_id = identity_fields["discogs_release_id"]
                lastfm_album_mbid = identity_fields["lastfm_album_mbid"]
                bandcamp_album_url = identity_fields["bandcamp_album_url"]
                metadata_source = identity_fields["metadata_source"]
                musicbrainz_release_id = str(identity_fields.get("musicbrainz_release_id") or "").strip()
            else:
                mbid = (e.get("musicbrainz_id") or meta.get("musicbrainz_releasegroupid") or meta.get("musicbrainz_id") or "")
                mbid = (mbid.strip() if isinstance(mbid, str) else str(mbid or "").strip()) or ""
                musicbrainz_release_id = str(
                    e.get("musicbrainz_release_id")
                    or meta.get("musicbrainz_releaseid")
                    or meta.get("musicbrainz_release_id")
                    or meta.get("musicbrainz_albumid")
                    or ""
                ).strip()
                discogs_release_id = str(
                    e.get("discogs_release_id")
                    or meta.get("discogs_release_id")
                    or ""
                ).strip()
                lastfm_album_mbid = str(
                    e.get("lastfm_album_mbid")
                    or meta.get("lastfm_album_mbid")
                    or ""
                ).strip()
                bandcamp_album_url = str(
                    e.get("bandcamp_album_url")
                    or meta.get("bandcamp_album_url")
                    or ""
                ).strip()
                metadata_source = _normalize_identity_provider(
                    str(
                        e.get("primary_metadata_source")
                        or e.get("metadata_source")
                        or meta.get("primary_metadata_source")
                        or meta.get(PMDA_MATCH_PROVIDER_TAG)
                        or ""
                    )
                )
            strict_match_verified = 1 if bool(e.get("strict_match_verified")) else 0
            strict_match_provider = _normalize_identity_provider(str(e.get("strict_match_provider") or ""))
            strict_reject_reason = str(e.get("strict_reject_reason") or "").strip()
            strict_tracklist_score = float(e.get("strict_tracklist_score") or 0.0)
            cur.execute("""
                INSERT OR REPLACE INTO scan_editions
                (scan_id, artist, album_id, title_raw, folder, fmt_text, br, sr, bd, meta_json, musicbrainz_id, musicbrainz_release_id,
                 is_broken, expected_track_count, actual_track_count, missing_indices, has_cover, missing_required_tags,
                 discogs_release_id, lastfm_album_mbid, bandcamp_album_url, metadata_source,
                 strict_match_verified, strict_match_provider, strict_reject_reason, strict_tracklist_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                scan_id,
                artist_resolved,
                e.get("album_id"),
                title_resolved,
                folder_str,
                fmt_text,
                e.get("br") or 0,
                e.get("sr") or 0,
                e.get("bd") or 0,
                json.dumps(meta, default=str),
                mbid,
                musicbrainz_release_id,
                1 if e.get("is_broken") else 0,
                e.get("expected_track_count"),
                e.get("actual_track_count") or len(e.get("tracks", [])),
                json.dumps(e.get("missing_indices", [])),
                has_cover,
                missing_required_json,
                discogs_release_id,
                lastfm_album_mbid,
                bandcamp_album_url,
                metadata_source,
                strict_match_verified,
                strict_match_provider,
                strict_reject_reason,
                strict_tracklist_score,
            ))
            row_count += 1
            if progress_callback and (row_count == 1 or row_count % 250 == 0):
                try:
                    progress_callback(int(row_count), int(total_rows_expected or 0), f"Saving scan edition rows ({row_count:,})")
                except Exception:
                    pass
    con.commit()
    con.close()
    if progress_callback:
        try:
            progress_callback(int(row_count), int(total_rows_expected or row_count or 0), f"Saved {row_count:,} scan edition rows")
        except Exception:
            pass
    logging.debug("save_scan_editions_to_db: scan_id=%s, %d edition rows", scan_id, row_count)

def save_scan_editions_artist_to_db(scan_id: int, artist_name: str, editions_list: List[dict]) -> int:
    """
    Insert one artist's editions into scan_editions (no DELETE). Returns row count inserted.
    """
    rows_to_insert: list[tuple[Any, ...]] = []
    for e in editions_list:
        folder = e.get("folder")
        meta = dict(e.get("meta", {}))
        folder_name = ""
        if folder:
            try:
                folder_name = (Path(folder).name or "").strip()
            except Exception:
                folder_name = ""
        artist_resolved, title_resolved = _apply_resolved_identity_to_edition(
            e,
            default_artist=str(artist_name or ""),
            default_title=str(e.get("title_raw") or e.get("album_title") or ""),
            folder_name=folder_name,
        )
        if e.get("primary_metadata_source"):
            meta["primary_metadata_source"] = e["primary_metadata_source"]
        if e.get("mb_submission_payload"):
            meta["mb_submission_payload"] = e["mb_submission_payload"]
        has_cover = 1 if _edition_cached_has_cover(e, meta) else 0
        missing_required = _check_required_tags(meta, REQUIRED_TAGS, edition=e)
        try:
            missing_required_json = json.dumps(missing_required, default=str) if missing_required else None
        except (TypeError, ValueError):
            missing_required_json = None
        folder_str = str(folder) if folder else ""
        fmt_text = _edition_cached_format_text(e, meta)
        try:
            meta_json_str = json.dumps(meta, default=str)
        except (TypeError, ValueError):
            meta_json_str = "{}"
        mbid = (e.get("musicbrainz_id") or meta.get("musicbrainz_releasegroupid") or meta.get("musicbrainz_id") or "")
        mbid = (mbid.strip() if isinstance(mbid, str) else str(mbid or "").strip()) or ""
        musicbrainz_release_id = str(
            e.get("musicbrainz_release_id")
            or meta.get("musicbrainz_releaseid")
            or meta.get("musicbrainz_release_id")
            or meta.get("musicbrainz_albumid")
            or ""
        ).strip()
        discogs_release_id = str(
            e.get("discogs_release_id")
            or meta.get("discogs_release_id")
            or ""
        ).strip()
        lastfm_album_mbid = str(
            e.get("lastfm_album_mbid")
            or meta.get("lastfm_album_mbid")
            or ""
        ).strip()
        bandcamp_album_url = str(
            e.get("bandcamp_album_url")
            or meta.get("bandcamp_album_url")
            or ""
        ).strip()
        metadata_source = _normalize_identity_provider(
            str(
                e.get("primary_metadata_source")
                or e.get("metadata_source")
                or meta.get("primary_metadata_source")
                or meta.get(PMDA_MATCH_PROVIDER_TAG)
                or ""
            )
        )
        strict_match_verified = 1 if bool(e.get("strict_match_verified")) else 0
        strict_match_provider = _normalize_identity_provider(str(e.get("strict_match_provider") or ""))
        strict_reject_reason = str(e.get("strict_reject_reason") or "").strip()
        strict_tracklist_score = float(e.get("strict_tracklist_score") or 0.0)
        rows_to_insert.append((
            scan_id,
            artist_resolved,
            e.get("album_id"),
            title_resolved,
            folder_str,
            fmt_text,
            e.get("br") or 0,
            e.get("sr") or 0,
            e.get("bd") or 0,
            meta_json_str,
            mbid,
            musicbrainz_release_id,
            1 if e.get("is_broken") else 0,
            e.get("expected_track_count"),
            e.get("actual_track_count") or len(e.get("tracks", [])),
            json.dumps(e.get("missing_indices", [])),
            has_cover,
            missing_required_json,
            discogs_release_id,
            lastfm_album_mbid,
            bandcamp_album_url,
            metadata_source,
            strict_match_verified,
            strict_match_provider,
            strict_reject_reason,
            strict_tracklist_score,
        ))

    def _write() -> int:
        con = _state_connect()
        cur = con.cursor()
        try:
            cur.executemany("""
            INSERT OR REPLACE INTO scan_editions
            (scan_id, artist, album_id, title_raw, folder, fmt_text, br, sr, bd, meta_json, musicbrainz_id, musicbrainz_release_id,
             is_broken, expected_track_count, actual_track_count, missing_indices, has_cover, missing_required_tags,
             discogs_release_id, lastfm_album_mbid, bandcamp_album_url, metadata_source,
             strict_match_verified, strict_match_provider, strict_reject_reason, strict_tracklist_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows_to_insert)
            con.commit()
            return len(rows_to_insert)
        finally:
            con.close()

    return _state_db_write_retry(_write, label=f"save_scan_editions_artist_to_db:{artist_name}", attempts=12)

def _refresh_scan_history_from_published(scan_id: int | None) -> None:
    sid = _parse_int_loose(scan_id, 0)
    if sid <= 0:
        return
    try:
        con = _state_connect(timeout=20)
        cur = con.cursor()
        cur.execute(
            """
            SELECT
                artist_norm,
                COALESCE(has_cover, 0),
                COALESCE(cover_path, ''),
                COALESCE(has_artist_image, 0),
                COALESCE(artist_image_path, ''),
                COALESCE(missing_required_tags_json, '[]'),
                COALESCE(strict_match_verified, 0),
                COALESCE(musicbrainz_release_group_id, ''),
                COALESCE(primary_tags_json, '{}')
            FROM files_library_published_albums
            WHERE scan_id = ?
            """,
            (sid,),
        )
        rows = cur.fetchall()
        if not rows:
            con.close()
            return

        without_cover = 0
        without_artist_image = 0
        without_complete_tags = 0
        without_mb_id = 0
        pmda_albums_processed = 0
        pmda_albums_complete = 0
        pmda_albums_with_cover = 0
        pmda_albums_with_artist_image = 0
        published_artists: set[str] = set()
        for row in rows:
            artist_norm = str(row[0] or "").strip()
            has_cover = bool(row[1]) or bool(str(row[2] or "").strip())
            has_artist_image = bool(row[3]) or bool(str(row[4] or "").strip())
            try:
                missing_required = json.loads(row[5] or "[]")
                missing_required = missing_required if isinstance(missing_required, list) else []
            except Exception:
                missing_required = []
            has_mb_id = bool(row[6]) or bool(str(row[7] or "").strip())
            try:
                primary_tags = json.loads(row[8] or "{}")
                primary_tags = primary_tags if isinstance(primary_tags, dict) else {}
            except Exception:
                primary_tags = {}
            pmda_matched = _pmda_bool_from_str(primary_tags.get(PMDA_MATCHED_TAG, ""))
            pmda_cover = _pmda_bool_from_str(primary_tags.get(PMDA_COVER_TAG, "")) or has_cover
            pmda_artist_image = _pmda_bool_from_str(primary_tags.get(PMDA_ARTIST_IMAGE_TAG, "")) or has_artist_image
            pmda_complete = _pmda_bool_from_str(primary_tags.get(PMDA_COMPLETE_TAG, ""))
            if artist_norm:
                published_artists.add(artist_norm)
            if not has_cover:
                without_cover += 1
            if not has_artist_image:
                without_artist_image += 1
            if missing_required:
                without_complete_tags += 1
            if not has_mb_id:
                without_mb_id += 1
            if pmda_matched or pmda_cover or pmda_artist_image:
                pmda_albums_processed += 1
            if pmda_cover:
                pmda_albums_with_cover += 1
            if pmda_artist_image:
                pmda_albums_with_artist_image += 1
            if pmda_complete or (pmda_matched and pmda_cover and pmda_artist_image and not missing_required):
                pmda_albums_complete += 1

        cur.execute("SELECT summary_json FROM scan_history WHERE scan_id = ?", (sid,))
        row = cur.fetchone()
        summary: dict[str, Any] = {}
        if row and row[0]:
            try:
                parsed = json.loads(row[0])
                if isinstance(parsed, dict):
                    summary = parsed
            except Exception:
                summary = {}

        _ai_refresh_rollup_for_scan(cur, sid)
        cur.execute(
            """
            SELECT
                COALESCE(start_time, 0),
                COALESCE(ai_tokens_total, 0),
                COALESCE(ai_cost_usd_total, 0),
                COALESCE(ai_unpriced_calls, 0),
                COALESCE(ai_lifecycle_complete, 0)
            FROM scan_history
            WHERE scan_id = ?
            """,
            (sid,),
        )
        ai_row = cur.fetchone() or (0, 0, 0.0, 0, 0)
        start_time = float(ai_row[0] or 0.0)
        lifecycle_complete = bool(ai_row[4])
        end_time = None
        duration_seconds = None
        if lifecycle_complete and start_time > 0:
            end_time = time.time()
            duration_seconds = int(max(0.0, end_time - start_time))
        summary["albums_without_album_image"] = int(without_cover)
        summary["albums_without_artist_image"] = int(without_artist_image)
        summary["albums_without_complete_tags"] = int(without_complete_tags)
        summary["albums_without_mb_id"] = int(without_mb_id)
        summary["published_albums"] = int(len(rows))
        summary["published_artists"] = int(len(published_artists))
        summary["pmda_albums_processed"] = int(pmda_albums_processed)
        summary["pmda_albums_complete"] = int(pmda_albums_complete)
        summary["pmda_albums_with_cover"] = int(pmda_albums_with_cover)
        summary["pmda_albums_with_artist_image"] = int(pmda_albums_with_artist_image)
        summary["ai_tokens_total"] = int(ai_row[1] or 0)
        summary["ai_cost_usd_total"] = float(ai_row[2] or 0.0)
        summary["ai_unpriced_calls"] = int(ai_row[3] or 0)
        summary["ai_lifecycle_complete"] = lifecycle_complete
        summary["ai_calls_total"] = int(_parse_int_loose(summary.get("ai_calls_total"), 0) or 0)
        try:
            summary["ai_calls_total"] = int(
                _sqlite_scalar(
                    cur,
                    "SELECT COUNT(*) FROM ai_call_usage WHERE scan_id = ? OR origin_scan_id = ?",
                    (sid, sid),
                )
                or 0
            )
        except Exception:
            pass
        summary["summary_refreshed_at"] = time.time()
        cur.execute(
            """
            UPDATE scan_history
            SET albums_without_artist_image = ?,
                albums_without_album_image = ?,
                albums_without_complete_tags = ?,
                albums_without_mb_id = ?,
                artists_processed = CASE
                    WHEN COALESCE(artists_processed, 0) < ? THEN ?
                    ELSE artists_processed
                END,
                end_time = COALESCE(?, end_time),
                duration_seconds = COALESCE(?, duration_seconds),
                summary_json = ?
            WHERE scan_id = ?
            """,
            (
                int(without_artist_image),
                int(without_cover),
                int(without_complete_tags),
                int(without_mb_id),
                int(len(published_artists)),
                int(len(published_artists)),
                end_time,
                duration_seconds,
                json.dumps(summary),
                sid,
            ),
        )
        con.commit()
        con.close()
        with lock:
            if int(state.get("scan_id") or 0) == sid:
                state["scan_published_albums_count"] = int(len(rows))
    except Exception:
        logging.debug("Failed to refresh scan_history from published rows for scan_id=%s", scan_id, exc_info=True)

def load_scan_from_db() -> Dict[str, List[dict]]:
    """
    Read the most-recent duplicate-scan from STATE_DB_FILE and rebuild the
    in-memory structure used by the Web UI.

    Returns
    -------
    dict
        { artist_name : [ group_dict, ... ] }
    """
    import json
    try:
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()

        # ---- 1) Best editions -------------------------------------------------
        cur.execute("PRAGMA table_info(duplicates_best)")
        best_cols = {r[1] for r in cur.fetchall()}
        has_match_verified = "match_verified_by_ai" in best_cols
        has_evidence_json = "evidence_json" in best_cols
        has_dupe_signal = "dupe_signal" in best_cols
        has_no_move = "no_move" in best_cols
        has_manual_review = "manual_review" in best_cols
        has_same_folder = "same_folder" in best_cols
        cur.execute(
            """
            SELECT artist, album_id, title_raw, album_norm, folder,
                   fmt_text, br, sr, bd, dur, discs, rationale, merge_list, ai_used, meta_json,
                   ai_provider, ai_model
            """ + (", evidence_json" if has_evidence_json else "") + """
                   , size_mb, track_count
            """ + (", match_verified_by_ai" if has_match_verified else "") + """
            """ + (", dupe_signal" if has_dupe_signal else "") + """
            """ + (", no_move" if has_no_move else "") + """
            """ + (", manual_review" if has_manual_review else "") + """
            """ + (", same_folder" if has_same_folder else "") + """
            FROM   duplicates_best
            """
        )
        best_rows = cur.fetchall()

        # ---- 2) Loser editions -----------------------------------------------
        cur.execute("PRAGMA table_info(duplicates_loser)")
        loser_cols = {r[1] for r in cur.fetchall()}
        has_loser_album_id = "loser_album_id" in loser_cols
        if has_loser_album_id:
            cur.execute(
                """
                SELECT artist, album_id, loser_album_id, folder, fmt_text, br, sr, bd, size_mb
                FROM   duplicates_loser
                """
            )
        else:
            cur.execute(
                """
                SELECT artist, album_id, folder, fmt_text, br, sr, bd, size_mb
                FROM   duplicates_loser
                """
            )
        loser_rows = cur.fetchall()
        con.close()
    except sqlite3.OperationalError as e:
        # If the user wiped state.db while the app is running, tables may be missing.
        # Recreate schema and return an empty result instead of 500.
        if "no such table" in str(e).lower():
            logging.warning("load_scan_from_db: missing table in state.db (%s); reinitializing DB and returning empty scan.", e)
            try:
                init_state_db()
            except Exception:
                logging.exception("init_state_db failed while handling missing tables in load_scan_from_db")
            return {}
        raise

    # Map losers by (artist, album_id) for quick lookup. album_id in table = best (group key).
    # loser_album_id is the legacy edition identifier retained for compatibility.
    loser_map: Dict[tuple, List[dict]] = defaultdict(list)
    for row in loser_rows:
        if has_loser_album_id:
            artist, aid, loser_aid, folder, fmt, br, sr, bd, size_mb = row[:9]
            edition_album_id = loser_aid if loser_aid is not None else aid
        else:
            artist, aid, folder, fmt, br, sr, bd, size_mb = row[:8]
            edition_album_id = aid
        loser_map[(artist, aid)].append(
            {
                "folder": Path(folder) if folder else None,
                "fmt": fmt,
                "br": br or 0,
                "sr": sr or 0,
                "bd": bd or 0,
                "size": size_mb,
                "album_id": edition_album_id,
                "artist": artist,
                "title_raw": None,
            }
        )

    results: Dict[str, List[dict]] = defaultdict(list)

    for row in best_rows:
        (artist, aid, title_raw, album_norm, folder, fmt_txt, br, sr, bd, dur, discs,
         rationale, merge_list_json, ai_used, meta_json) = row[:15]
        idx = 15
        ai_provider = (row[idx] or "") if len(row) > idx else ""
        idx += 1
        ai_model = (row[idx] or "") if len(row) > idx else ""
        idx += 1
        evidence_raw = None
        if has_evidence_json:
            evidence_raw = row[idx] if len(row) > idx else None
            idx += 1
        size_mb = row[idx] if len(row) > idx else None
        idx += 1
        track_count = row[idx] if len(row) > idx else None
        idx += 1
        match_verified_by_ai = bool(row[idx]) if has_match_verified and len(row) > idx else False
        idx += 1 if has_match_verified else 0
        dupe_signal = str(row[idx] or "") if has_dupe_signal and len(row) > idx else ""
        idx += 1 if has_dupe_signal else 0
        no_move = bool(row[idx]) if has_no_move and len(row) > idx else False
        idx += 1 if has_no_move else 0
        manual_review = bool(row[idx]) if has_manual_review and len(row) > idx else False
        idx += 1 if has_manual_review else 0
        same_folder = bool(row[idx]) if has_same_folder and len(row) > idx else False
        try:
            dupe_evidence = json.loads(evidence_raw) if evidence_raw else []
            if not isinstance(dupe_evidence, list):
                dupe_evidence = []
        except Exception:
            dupe_evidence = []

        best_entry = {
            "album_id": aid,
            "title_raw": title_raw,
            "album_norm": album_norm,
            "folder": Path(folder),
            "fmt_text": fmt_txt,
            "br": br,
            "sr": sr,
            "bd": bd,
            "dur": dur,
            "discs": discs,
            "rationale": rationale,
            "merge_list": json.loads(merge_list_json) if merge_list_json else [],
            "used_ai": bool(ai_used),
            "meta": json.loads(meta_json or "{}"),
            "ai_provider": ai_provider,
            "ai_model": ai_model,
            "dupe_evidence": dupe_evidence,
            "size_mb": size_mb,
            "track_count": track_count,
            "match_verified_by_ai": match_verified_by_ai,
        }

        losers = loser_map.get((artist, aid), [])

        # Some legacy loser rows still need the readable title. PMDA is files-only now,
        # so fall back to the group title instead of opening an external source DB.
        for l in losers:
            if l["title_raw"] is None:
                l["title_raw"] = str(title_raw or album_norm or "")

        results[artist].append(
            {
                "artist": artist,
                "album_id": aid,
                "best": best_entry,
                "losers": losers,
                "dupe_signal": dupe_signal,
                "no_move": bool(no_move),
                "manual_review": bool(manual_review),
                "same_folder": bool(same_folder),
            }
        )

    return results

def _load_files_album_scan_cache_map(
    *,
    folder_keys: list[str] | None = None,
    include_ordered_paths: bool = False,
) -> dict[str, dict]:
    """Load files album cache rows keyed by folder path."""
    out: dict[str, dict] = {}
    try:
        con = _state_connect_readonly(timeout=20)
        cur = con.cursor()
        cur.execute("PRAGMA table_info(files_album_scan_cache)")
        cols = {r[1] for r in cur.fetchall()}
        has_mbid_col = "musicbrainz_id" in cols
        has_identity_col = "has_identity" in cols
        has_identity_provider_col = "identity_provider" in cols
        has_discogs_col = "discogs_release_id" in cols
        has_lastfm_col = "lastfm_album_mbid" in cols
        has_bandcamp_col = "bandcamp_album_url" in cols
        has_metadata_source_col = "metadata_source" in cols
        has_mb_release_id_col = "musicbrainz_release_id" in cols
        has_strict_verified_col = "strict_match_verified" in cols
        has_strict_provider_col = "strict_match_provider" in cols
        has_strict_reason_col = "strict_reject_reason" in cols
        has_strict_score_col = "strict_tracklist_score" in cols
        has_ordered_paths_col = "ordered_paths_json" in cols

        where_sql = ""
        query_params: list[object] = []
        if folder_keys:
            cleaned_keys = [str(k or "").strip() for k in folder_keys if str(k or "").strip()]
            if cleaned_keys:
                placeholders = ",".join("?" for _ in cleaned_keys)
                where_sql = f" WHERE folder_path IN ({placeholders})"
                query_params.extend(cleaned_keys)

        cur.execute(
            f"""
            SELECT
                folder_path,
                fingerprint,
                {'ordered_paths_json' if has_ordered_paths_col else "'[]'"} AS ordered_paths_json,
                has_cover,
                has_artist_image,
                has_complete_tags,
                has_mbid,
                {'musicbrainz_id' if has_mbid_col else "''"} AS musicbrainz_id,
                {'musicbrainz_release_id' if has_mb_release_id_col else "''"} AS musicbrainz_release_id,
                {'has_identity' if has_identity_col else '0'} AS has_identity,
                {'identity_provider' if has_identity_provider_col else "''"} AS identity_provider,
                {'discogs_release_id' if has_discogs_col else "''"} AS discogs_release_id,
                {'lastfm_album_mbid' if has_lastfm_col else "''"} AS lastfm_album_mbid,
                {'bandcamp_album_url' if has_bandcamp_col else "''"} AS bandcamp_album_url,
                {'metadata_source' if has_metadata_source_col else "''"} AS metadata_source,
                {'strict_match_verified' if has_strict_verified_col else '0'} AS strict_match_verified,
                {'strict_match_provider' if has_strict_provider_col else "''"} AS strict_match_provider,
                {'strict_reject_reason' if has_strict_reason_col else "''"} AS strict_reject_reason,
                {'strict_tracklist_score' if has_strict_score_col else '0'} AS strict_tracklist_score,
                missing_required_tags,
                updated_at,
                artist_name,
                album_title,
                COALESCE(source_id, 0) AS source_id
            FROM files_album_scan_cache
            {where_sql}
            """,
            tuple(query_params),
        )
        for row in cur.fetchall():
            folder_path = row[0] or ""
            if not folder_path:
                continue
            ordered_paths: list[str] = []
            if include_ordered_paths:
                try:
                    ordered_paths = json.loads(row[2] or "[]")
                    if not isinstance(ordered_paths, list):
                        ordered_paths = []
                except Exception:
                    ordered_paths = []
            try:
                missing_required = json.loads(row[19] or "[]")
                if not isinstance(missing_required, list):
                    missing_required = []
            except Exception:
                missing_required = []
            identity_provider = _normalize_identity_provider(str(row[10] or ""))
            metadata_source = _normalize_identity_provider(str(row[14] or ""))
            strict_match_verified = bool(row[15])
            strict_match_provider = _normalize_identity_provider(str(row[16] or ""))
            strict_reject_reason = str(row[17] or "").strip()
            try:
                strict_tracklist_score = float(row[18] or 0.0)
            except Exception:
                strict_tracklist_score = 0.0
            has_identity = bool(row[9]) or bool(strict_match_verified)
            payload = {
                "fingerprint": row[1] or "",
                "has_cover": bool(row[3]),
                "has_artist_image": bool(row[4]),
                "has_complete_tags": bool(row[5]),
                "has_mbid": bool(row[6]),
                "musicbrainz_id": (row[7] or "").strip(),
                "musicbrainz_release_id": (row[8] or "").strip(),
                "has_identity": has_identity,
                "identity_provider": strict_match_provider or identity_provider,
                "discogs_release_id": (row[11] or "").strip(),
                "lastfm_album_mbid": (row[12] or "").strip(),
                "bandcamp_album_url": (row[13] or "").strip(),
                "metadata_source": metadata_source,
                "strict_match_verified": strict_match_verified,
                "strict_match_provider": strict_match_provider,
                "strict_reject_reason": strict_reject_reason,
                "strict_tracklist_score": strict_tracklist_score,
                "missing_required_tags": missing_required,
                "updated_at": float(row[20] or 0),
                "artist_name": row[21] or "",
                "album_title": row[22] or "",
                "source_id": int(row[23] or 0) if len(row) > 23 else 0,
            }
            if include_ordered_paths:
                payload["ordered_paths"] = [str(p) for p in ordered_paths if str(p or "").strip()]
            out[folder_path] = payload
        con.close()
    except Exception:
        logging.debug("Failed to load files album scan cache", exc_info=True)
    return out

def _upsert_files_album_scan_cache_rows(rows: list[dict]) -> None:
    """Upsert rows into files_album_scan_cache."""
    if not rows:
        return
    try:
        con = _state_connect(timeout=30)
        cur = con.cursor()
        cur.executemany(
            """
            INSERT INTO files_album_scan_cache
            (folder_path, source_id, fingerprint, ordered_paths_json, artist_name, album_title,
             has_cover, has_artist_image, has_complete_tags, has_mbid, has_identity,
             identity_provider, strict_match_verified, strict_match_provider, strict_reject_reason, strict_tracklist_score,
             musicbrainz_id, musicbrainz_release_id, discogs_release_id, lastfm_album_mbid,
             bandcamp_album_url, metadata_source,
             missing_required_tags, last_scan_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(folder_path) DO UPDATE SET
              source_id=excluded.source_id,
              fingerprint=excluded.fingerprint,
              ordered_paths_json=excluded.ordered_paths_json,
              artist_name=excluded.artist_name,
              album_title=excluded.album_title,
              has_cover=excluded.has_cover,
              has_artist_image=excluded.has_artist_image,
              has_complete_tags=excluded.has_complete_tags,
              has_mbid=excluded.has_mbid,
              has_identity=excluded.has_identity,
              identity_provider=excluded.identity_provider,
              strict_match_verified=excluded.strict_match_verified,
              strict_match_provider=excluded.strict_match_provider,
              strict_reject_reason=excluded.strict_reject_reason,
              strict_tracklist_score=excluded.strict_tracklist_score,
              musicbrainz_id=excluded.musicbrainz_id,
              musicbrainz_release_id=excluded.musicbrainz_release_id,
              discogs_release_id=excluded.discogs_release_id,
              lastfm_album_mbid=excluded.lastfm_album_mbid,
              bandcamp_album_url=excluded.bandcamp_album_url,
              metadata_source=excluded.metadata_source,
              missing_required_tags=excluded.missing_required_tags,
              last_scan_id=excluded.last_scan_id,
              updated_at=excluded.updated_at
            """,
            [
                (
                    r.get("folder_path") or "",
                    int(r.get("source_id") or 0) if int(r.get("source_id") or 0) > 0 else None,
                    r.get("fingerprint") or "",
                    json.dumps([str(p) for p in (r.get("ordered_paths") or []) if str(p or "").strip()], ensure_ascii=False),
                    r.get("artist_name") or "",
                    r.get("album_title") or "",
                    1 if r.get("has_cover") else 0,
                    1 if r.get("has_artist_image") else 0,
                    1 if r.get("has_complete_tags") else 0,
                    1 if r.get("has_mbid") else 0,
                    1 if r.get("has_identity") else 0,
                    _normalize_identity_provider(str(r.get("identity_provider") or "")),
                    1 if r.get("strict_match_verified") else 0,
                    _normalize_identity_provider(str(r.get("strict_match_provider") or "")),
                    str(r.get("strict_reject_reason") or "").strip(),
                    float(r.get("strict_tracklist_score") or 0.0),
                    r.get("musicbrainz_id") or "",
                    r.get("musicbrainz_release_id") or "",
                    r.get("discogs_release_id") or "",
                    r.get("lastfm_album_mbid") or "",
                    r.get("bandcamp_album_url") or "",
                    _normalize_identity_provider(str(r.get("metadata_source") or "")),
                    json.dumps(r.get("missing_required_tags") or []),
                    r.get("last_scan_id"),
                    float(r.get("updated_at") or time.time()),
                )
                for r in rows
                if (r.get("folder_path") or "").strip()
            ],
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to upsert files album scan cache rows", exc_info=True)

def _snapshot_files_album_scan_cache_from_prescan(
    files_editions_by_album_id: dict[int, dict],
    *,
    scan_id: int | None = None,
    reason: str = "prescan",
    batch_size: int = 2000,
    pause_event: threading.Event | None = None,
    respect_pause: bool = False,
) -> dict:
    """
    Persist current pre-scan album metadata to files_album_scan_cache in batches.
    Designed for fast resume across restarts/cancellations without waiting for full scan completion.
    """
    started_at = time.time()
    if not files_editions_by_album_id:
        return {"ok": True, "rows_upserted": 0, "reason": reason, "duration_sec": 0.0}

    ids = list(files_editions_by_album_id.keys())
    total = len(ids)
    batch_size = max(200, int(batch_size or 2000))
    rows_upserted = 0
    rows_buffer: list[dict] = []
    now_ts = time.time()

    for idx, aid in enumerate(ids, start=1):
        if scan_should_stop.is_set():
            break
        if respect_pause and pause_event is not None:
            while pause_event.is_set() and not scan_should_stop.is_set():
                time.sleep(0.2)
        item = files_editions_by_album_id.get(aid) or {}
        row = _build_files_cache_row_from_prescan_item(item, scan_id=scan_id, now_ts=now_ts)
        if row:
            rows_buffer.append(row)
        if len(rows_buffer) >= batch_size:
            _upsert_files_album_scan_cache_rows(rows_buffer)
            rows_upserted += len(rows_buffer)
            rows_buffer = []
            with lock:
                if bool(state.get("scan_prescan_cache_snapshot_running")):
                    state["scan_prescan_cache_snapshot_rows"] = int(rows_upserted)
                    state["scan_prescan_cache_snapshot_total"] = int(total)
                    state["scan_prescan_cache_snapshot_updated_at"] = time.time()
        if idx % 10000 == 0:
            logging.info(
                "FILES cache snapshot (%s): %d/%d album(s) prepared, %d row(s) upserted",
                reason,
                idx,
                total,
                rows_upserted,
            )

    if rows_buffer:
        _upsert_files_album_scan_cache_rows(rows_buffer)
        rows_upserted += len(rows_buffer)
        with lock:
            if bool(state.get("scan_prescan_cache_snapshot_running")):
                state["scan_prescan_cache_snapshot_rows"] = int(rows_upserted)
                state["scan_prescan_cache_snapshot_total"] = int(total)
                state["scan_prescan_cache_snapshot_updated_at"] = time.time()

    elapsed = round(time.time() - started_at, 2)
    logging.info(
        "FILES cache snapshot (%s): upserted %d row(s) from %d pre-scan album(s) in %.2fs",
        reason,
        rows_upserted,
        total,
        elapsed,
    )
    return {"ok": True, "rows_upserted": rows_upserted, "reason": reason, "duration_sec": elapsed}


def _refresh_files_album_scan_cache_from_editions(editions: list[dict], scan_id: int | None = None) -> None:
    """
    Refresh incremental files cache from a set of scanned editions.
    Called after each artist to keep changed-only scans accurate.
    """
    if not editions:
        return
    rows: list[dict] = []
    now = time.time()
    pg_conn = None
    try:
        pg_conn = _files_pg_connect()
    except Exception:
        pg_conn = None
    try:
        for e in editions:
            folder_raw = e.get("folder")
            if not folder_raw:
                continue
            canonical_folder = Path(str(folder_raw))
            folder = path_for_fs_access(Path(str(e.get("storage_access_path") or folder_raw)))
            if not folder or not folder.exists():
                continue
            folder_key = _album_folder_cache_key(canonical_folder)
            ordered_paths = [Path(p) for p in (e.get("ordered_paths") or []) if Path(p).exists()]
            if not ordered_paths:
                try:
                    ordered_paths = sorted(
                        [p for p in folder.rglob("*") if p.is_file() and AUDIO_RE.search(p.name)],
                        key=lambda x: str(x),
                    )
                except Exception:
                    ordered_paths = []
            computed_fingerprint = _compute_album_fingerprint(ordered_paths)
            fingerprint = computed_fingerprint or (e.get("fingerprint") or "").strip()
            tags = dict(e.get("meta") or {})
            if ordered_paths:
                try:
                    live_tags = extract_tags(ordered_paths[0]) or {}
                    if live_tags:
                        tags.update(live_tags)
                except Exception:
                    if not tags:
                        tags = {}
            edition_for_required = e
            if not (edition_for_required.get("tracks") or []):
                derived_tracks = [
                    {"title": p.stem or f"Track {i + 1}", "idx": i + 1}
                    for i, p in enumerate(ordered_paths)
                ]
                edition_for_required = dict(e)
                edition_for_required["tracks"] = derived_tracks
            missing_required = _check_required_tags(tags, REQUIRED_TAGS, edition=edition_for_required)
            has_cover = album_folder_has_cover(folder)
            artist_resolved, title_resolved = _resolve_edition_display_identity(
                e,
                default_artist=str(e.get("artist") or e.get("artist_name") or folder.parent.name.replace("_", " ") or ""),
                default_title=str(e.get("title_raw") or e.get("album_title") or folder.name or ""),
                folder_name=folder.name,
            )
            artist_norm = _norm_artist_key(artist_resolved) or _norm_artist_key(str(e.get("artist") or ""))
            artist_image_path = _files_effective_artist_image_path(
                folder,
                artist_resolved,
                artist_norm or "",
                conn=pg_conn,
            )
            has_artist_image = bool(artist_image_path and artist_image_path.is_file())
            identity_fields = _extract_files_identity_fields(tags=tags, edition=e, cached={})
            mbid = identity_fields["musicbrainz_id"]
            source_id = _parse_int_loose(e.get("source_id"), 0) or 0
            if source_id <= 0:
                source_id = int(_source_id_for_path(folder) or 0)
            rows.append(
                {
                    "folder_path": folder_key,
                    "source_id": source_id if source_id > 0 else None,
                    "fingerprint": fingerprint,
                    "ordered_paths": [
                        str(p)
                        for p in ((e.get("canonical_ordered_paths") or ordered_paths or []))
                        if str(p or "").strip()
                    ],
                    "artist_name": artist_resolved,
                    "album_title": title_resolved or folder.name,
                    "has_cover": has_cover,
                    "has_artist_image": has_artist_image,
                    "has_complete_tags": len(missing_required) == 0,
                    "has_mbid": bool(identity_fields["has_mbid"]),
                    "has_identity": bool(identity_fields["has_identity"]),
                    "identity_provider": identity_fields["identity_provider"],
                    "strict_match_verified": bool(identity_fields.get("strict_match_verified")),
                    "strict_match_provider": identity_fields.get("strict_match_provider") or "",
                    "strict_reject_reason": identity_fields.get("strict_reject_reason") or "",
                    "strict_tracklist_score": float(identity_fields.get("strict_tracklist_score") or 0.0),
                    "musicbrainz_id": mbid,
                    "discogs_release_id": identity_fields["discogs_release_id"],
                    "lastfm_album_mbid": identity_fields["lastfm_album_mbid"],
                    "bandcamp_album_url": identity_fields["bandcamp_album_url"],
                    "metadata_source": identity_fields["metadata_source"],
                    "missing_required_tags": missing_required,
                    "last_scan_id": scan_id,
                    "updated_at": now,
                }
            )
    finally:
        try:
            if pg_conn is not None:
                pg_conn.close()
        except Exception:
            pass
    _upsert_files_album_scan_cache_rows(rows)

def _load_files_album_scan_cache_map_for_keys(
    folder_keys: list[str],
    *,
    include_ordered_paths: bool = False,
    batch_size: int = 500,
) -> dict[str, dict]:
    keys = list(dict.fromkeys(str(k or "").strip() for k in (folder_keys or []) if str(k or "").strip()))
    if not keys:
        return {}
    out: dict[str, dict] = {}
    size = max(100, int(batch_size or 500))
    for idx in range(0, len(keys), size):
        out.update(
            _load_files_album_scan_cache_map(
                folder_keys=keys[idx: idx + size],
                include_ordered_paths=include_ordered_paths,
            )
        )
    return out


def _resume_files_plan_row_tuple(
    run_id: str,
    album_id: int,
    artist_name: str,
    artist_order: int,
    album_order: int,
    fe: dict[str, Any] | None,
) -> tuple[Any, ...] | None:
    fe = dict(fe or {})
    folder_raw = str(fe.get("folder") or "").strip()
    if not folder_raw:
        return None
    return (
        str(run_id),
        int(album_id),
        str(artist_name or "").strip() or "Unknown Artist",
        int(artist_order or 0),
        int(album_order or 0),
        str(fe.get("album_title") or fe.get("title_raw") or "").strip(),
        str(fe.get("album_norm") or "").strip(),
        folder_raw,
        str(fe.get("fingerprint") or "").strip(),
        int(fe.get("file_count") or 0),
        _parse_int_loose(fe.get("source_id"), 0),
        1 if bool(fe.get("has_cover")) else 0,
        1 if bool(fe.get("has_artist_image")) else 0,
        1 if bool(fe.get("has_mbid")) else 0,
        1 if bool(fe.get("has_identity")) else 0,
        str(fe.get("identity_provider") or "").strip(),
        1 if bool(fe.get("strict_match_verified")) else 0,
        str(fe.get("strict_match_provider") or "").strip(),
        str(fe.get("strict_reject_reason") or "").strip(),
        float(fe.get("strict_tracklist_score") or 0.0),
        str(fe.get("musicbrainz_id") or "").strip(),
        str(fe.get("discogs_release_id") or "").strip(),
        str(fe.get("lastfm_album_mbid") or "").strip(),
        str(fe.get("bandcamp_album_url") or "").strip(),
        str(fe.get("metadata_source") or "").strip(),
        json.dumps(list(fe.get("missing_required_tags") or [])),
        1 if bool(fe.get("skip_heavy_processing")) else 0,
        str(fe.get("_lookup_artist_name") or "").strip(),
        str(fe.get("_lookup_album_title") or "").strip(),
        str(fe.get("storage_provider") or "").strip(),
        str(fe.get("storage_device_id") or "").strip(),
        str(fe.get("storage_device_label") or "").strip(),
        int(fe.get("storage_bucket_order") or 0),
        str(fe.get("storage_rel_path") or "").strip(),
        str(fe.get("storage_access_path") or "").strip(),
    )

def _persist_resume_files_plan(
    run_id: str | None,
    artists_merged: list[tuple[int, str, list[int]]],
    files_editions_by_album_id: dict[int, dict] | None,
    *,
    detected_artists_total: int | None = None,
    detected_albums_total: int | None = None,
    detected_tracks_total: int | None = None,
) -> int:
    """Persist the remaining Files scan plan so stop/restart can resume without full rediscovery."""
    if not run_id or not artists_merged or not files_editions_by_album_id:
        return 0
    rows: list[tuple[Any, ...]] = []
    for artist_order, (_artist_id, artist_name, album_ids) in enumerate(artists_merged, start=1):
        for album_order, aid in enumerate(album_ids, start=1):
            row = _resume_files_plan_row_tuple(
                str(run_id),
                int(aid),
                str(artist_name or "").strip() or "Unknown Artist",
                int(artist_order),
                int(album_order),
                files_editions_by_album_id.get(aid) or {},
            )
            if row is not None:
                rows.append(row)
    if not rows:
        return 0
    now = time.time()
    con = _state_connect(timeout=60)
    cur = con.cursor()
    cur.execute("DELETE FROM scan_resume_files_plan WHERE run_id = ?", (str(run_id),))
    cur.executemany(
        """
        INSERT INTO scan_resume_files_plan (
            run_id, album_id, artist_name, artist_order, album_order, album_title, album_norm,
            folder_path, fingerprint, file_count, source_id, has_cover, has_artist_image, has_mbid,
            has_identity, identity_provider, strict_match_verified, strict_match_provider,
            strict_reject_reason, strict_tracklist_score, musicbrainz_id, discogs_release_id,
            lastfm_album_mbid, bandcamp_album_url, metadata_source, missing_required_tags_json,
            skip_heavy_processing, lookup_artist_name, lookup_album_title,
            storage_provider, storage_device_id, storage_device_label, storage_bucket_order,
            storage_rel_path, storage_access_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    bucket_map: dict[tuple[int, str, str], dict[str, Any]] = {}
    for row in rows:
        provider = str(row[29] or "").strip()
        device_id = str(row[30] or "").strip()
        if not provider or not device_id:
            continue
        bucket_order = int(row[32] or 0)
        folder_path = str(row[7] or "").strip()
        access_path = str(row[34] or "").strip()
        canonical_root = str(Path(folder_path).parent if folder_path else "")
        access_root = str(Path(access_path).parent if access_path else "")
        key = (bucket_order, device_id, canonical_root)
        item = bucket_map.setdefault(
            key,
            {
                "bucket_order": bucket_order,
                "storage_provider": provider,
                "storage_device_id": device_id,
                "storage_device_label": str(row[31] or device_id).strip() or device_id,
                "canonical_root": canonical_root,
                "access_root": access_root,
                "albums_total": 0,
            },
        )
        item["albums_total"] = int(item.get("albums_total") or 0) + 1
    cur.execute("DELETE FROM scan_storage_buckets WHERE run_id = ?", (str(run_id),))
    if bucket_map:
        cur.executemany(
            """
            INSERT OR REPLACE INTO scan_storage_buckets
            (run_id, bucket_order, storage_provider, storage_device_id, storage_device_label,
             canonical_root, access_root, albums_total, albums_done, started_at, finished_at, status, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, 'pending', '')
            """,
            [
                (
                    str(run_id),
                    int(item.get("bucket_order") or 0),
                    str(item.get("storage_provider") or ""),
                    str(item.get("storage_device_id") or ""),
                    str(item.get("storage_device_label") or ""),
                    str(item.get("canonical_root") or ""),
                    str(item.get("access_root") or ""),
                    int(item.get("albums_total") or 0),
                )
                for item in sorted(bucket_map.values(), key=lambda x: (int(x.get("bucket_order") or 0), str(x.get("storage_device_id") or "")))
            ],
        )
    cur.execute(
        """
        UPDATE scan_resume_runs
        SET updated_at = ?,
            detected_artists_total = ?,
            detected_albums_total = ?,
            detected_tracks_total = ?,
            plan_snapshot_ready = 1
        WHERE run_id = ?
        """,
        (
            now,
            int(detected_artists_total if detected_artists_total is not None else len(artists_merged)),
            int(detected_albums_total if detected_albums_total is not None else sum(len(ids) for _a, _n, ids in artists_merged)),
            int(detected_tracks_total or 0),
            str(run_id),
        ),
    )
    con.commit()
    con.close()
    return len(rows)

def _upsert_resume_files_plan_partial(
    run_id: str | None,
    album_ids: list[int] | tuple[int, ...] | set[int],
    files_editions_by_album_id: dict[int, dict] | None,
    *,
    detected_artists_total: int | None = None,
    detected_albums_total: int | None = None,
    detected_tracks_total: int | None = None,
) -> int:
    run_id = str(run_id or "").strip()
    if not run_id or not album_ids or not files_editions_by_album_id:
        return 0
    rows: list[tuple[Any, ...]] = []
    for aid in sorted({int(aid) for aid in album_ids if _parse_int_loose(aid, 0)}):
        fe = files_editions_by_album_id.get(int(aid)) or {}
        artist_name = str(fe.get("artist_name") or fe.get("artist") or "").strip() or "Unknown Artist"
        row = _resume_files_plan_row_tuple(run_id, int(aid), artist_name, 0, 0, fe)
        if row is not None:
            rows.append(row)
    if not rows:
        return 0
    now = time.time()
    con = _state_connect(timeout=60)
    cur = con.cursor()
    cur.executemany(
        """
        INSERT OR REPLACE INTO scan_resume_files_plan (
            run_id, album_id, artist_name, artist_order, album_order, album_title, album_norm,
            folder_path, fingerprint, file_count, source_id, has_cover, has_artist_image, has_mbid,
            has_identity, identity_provider, strict_match_verified, strict_match_provider,
            strict_reject_reason, strict_tracklist_score, musicbrainz_id, discogs_release_id,
            lastfm_album_mbid, bandcamp_album_url, metadata_source, missing_required_tags_json,
            skip_heavy_processing, lookup_artist_name, lookup_album_title,
            storage_provider, storage_device_id, storage_device_label, storage_bucket_order,
            storage_rel_path, storage_access_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    cur.execute(
        """
        UPDATE scan_resume_runs
        SET updated_at = ?,
            detected_artists_total = ?,
            detected_albums_total = ?,
            detected_tracks_total = ?
        WHERE run_id = ?
        """,
        (
            now,
            int(detected_artists_total or 0),
            int(detected_albums_total or 0),
            int(detected_tracks_total or 0),
            run_id,
        ),
    )
    con.commit()
    con.close()
    return len(rows)

def _prune_resume_files_plan_artist(run_id: str | None, artist_name: str) -> None:
    if not run_id or not artist_name:
        return
    try:
        con = _state_connect(timeout=15)
        cur = con.cursor()
        cur.execute(
            "DELETE FROM scan_resume_files_plan WHERE run_id = ? AND artist_name = ?",
            (str(run_id), str(artist_name)),
        )
        cur.execute(
            "UPDATE scan_resume_runs SET updated_at = ? WHERE run_id = ?",
            (time.time(), str(run_id)),
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to prune resume files plan for artist=%s run_id=%s", artist_name, run_id, exc_info=True)

def _prune_resume_files_plan_albums(run_id: str | None, album_ids: list[int] | tuple[int, ...] | set[int]) -> None:
    if not run_id or not album_ids:
        return
    ids = [int(aid) for aid in album_ids if _parse_int_loose(aid, 0)]
    if not ids:
        return
    try:
        con = _state_connect(timeout=15)
        cur = con.cursor()
        cur.executemany(
            "DELETE FROM scan_resume_files_plan WHERE run_id = ? AND album_id = ?",
            [(str(run_id), int(aid)) for aid in ids],
        )
        cur.execute(
            "UPDATE scan_resume_runs SET updated_at = ? WHERE run_id = ?",
            (time.time(), str(run_id)),
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to prune resume files plan albums for run_id=%s", run_id, exc_info=True)

def _ensure_resume_run_started(
    mode: str,
    scan_type: str,
    *,
    requested_run_id: str | None = None,
) -> str | None:
    if mode != "files":
        return None
    now = time.time()
    run_id: str | None = None
    source_signature = _compute_scan_source_signature(mode, scan_type)
    requested_run_id = str(requested_run_id or "").strip() or None
    try:
        con = _state_connect(timeout=30)
        cur = con.cursor()
        row = None
        if requested_run_id:
            cur.execute(
                """
                SELECT run_id
                FROM scan_resume_runs
                WHERE run_id = ?
                  AND mode = ? AND scan_type = ?
                  AND COALESCE(status, '') != 'completed'
                LIMIT 1
                """,
                (requested_run_id, mode, scan_type),
            )
            row = cur.fetchone()
        if not row:
            cur.execute(
                """
                SELECT run_id
                FROM scan_resume_runs
                WHERE source_signature = ? AND mode = ? AND scan_type = ?
                  AND COALESCE(status, '') != 'completed'
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (source_signature, mode, scan_type),
            )
            row = cur.fetchone()
        if row:
            run_id = str(row["run_id"] if isinstance(row, sqlite3.Row) else row[0] or "").strip() or None
            if run_id:
                cur.execute(
                    """
                    UPDATE scan_resume_runs
                    SET status = 'running', updated_at = ?
                    WHERE run_id = ?
                    """,
                    (now, run_id),
                )
        if not run_id:
            run_id = uuid.uuid4().hex
            cur.execute(
                """
                INSERT INTO scan_resume_runs
                (run_id, created_at, updated_at, mode, scan_type, source_signature, status, scan_id)
                VALUES (?, ?, ?, ?, ?, ?, 'running', NULL)
                """,
                (run_id, now, now, mode, scan_type, source_signature),
            )
        con.commit()
        con.close()
        return run_id
    except Exception:
        logging.debug("Failed to seed resume run for mode=%s scan_type=%s", mode, scan_type, exc_info=True)
        return requested_run_id

def _persist_resume_discovery_snapshot(
    run_id: str | None,
    snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    run_id = str(run_id or "").strip()
    if not run_id or not isinstance(snapshot, dict):
        return {"ok": False, "rows": 0, "run_id": run_id}
    roots = [str(r) for r in (snapshot.get("roots") or []) if r]
    stage = str(snapshot.get("stage") or "").strip() or "filesystem"
    results_by_root_raw = snapshot.get("results_by_root") or {}
    current_root_index = _parse_int_loose(snapshot.get("current_root_index"), 0)
    current_root_files_raw = snapshot.get("current_root_files") or []
    rows: list[tuple[str, int, str]] = []
    seen_paths: set[str] = set()
    for key, paths in (results_by_root_raw.items() if isinstance(results_by_root_raw, dict) else []):
        root_index = _parse_int_loose(key, 0) or 0
        for path in paths or []:
            sp = str(path or "").strip()
            if not sp or sp in seen_paths:
                continue
            seen_paths.add(sp)
            rows.append((run_id, root_index, sp))
    if current_root_index is not None:
        for path in current_root_files_raw or []:
            sp = str(path or "").strip()
            if not sp or sp in seen_paths:
                continue
            seen_paths.add(sp)
            rows.append((run_id, int(current_root_index), sp))
    state_json = {
        "stage": stage,
        "roots": roots,
        "current_root_index": _parse_int_loose(snapshot.get("current_root_index"), None),
        "current_root_path": str(snapshot.get("current_root_path") or "").strip() or None,
        "current_stack": copy.deepcopy(snapshot.get("current_stack") or []),
        "current_root_entries_scanned": int(snapshot.get("current_root_entries_scanned") or 0),
        "current_root_audio_found": int(snapshot.get("current_root_audio_found") or 0),
        "shared_entries_scanned": int(snapshot.get("shared_entries_scanned") or 0),
        "shared_files_found": int(snapshot.get("shared_files_found") or 0),
        "shared_roots_done": int(snapshot.get("shared_roots_done") or 0),
        "roots_total": int(snapshot.get("roots_total") or len(roots)),
        "entries_scanned": int(snapshot.get("shared_entries_scanned") or 0),
        "files_found": int(snapshot.get("shared_files_found") or 0),
        "albums_found": int(snapshot.get("albums_found") or 0),
        "artists_found": int(snapshot.get("artists_found") or 0),
        "folders_found": int(snapshot.get("folders_found") or 0),
        "folders_done": int(snapshot.get("folders_done") or 0),
        "folders_total": int(snapshot.get("folders_total") or 0),
        "cached_album_folders": [str(p) for p in (snapshot.get("cached_album_folders") or []) if str(p or "").strip()],
        "updated_at": time.time(),
    }
    try:
        con = _state_connect(timeout=60)
        cur = con.cursor()
        cur.execute("DELETE FROM scan_resume_discovery_files WHERE run_id = ?", (run_id,))
        if rows:
            cur.executemany(
                """
                INSERT OR REPLACE INTO scan_resume_discovery_files (run_id, root_index, file_path)
                VALUES (?, ?, ?)
                """,
                rows,
            )
        cur.execute(
            """
            UPDATE scan_resume_runs
            SET updated_at = ?,
                detected_artists_total = ?,
                detected_albums_total = ?,
                detected_tracks_total = ?,
                discovery_snapshot_ready = 1,
                discovery_stage = ?,
                discovery_state_json = ?
            WHERE run_id = ?
            """,
            (
                time.time(),
                int(state_json.get("artists_found") or 0),
                int(state_json.get("albums_found") or 0),
                int(state_json.get("files_found") or 0),
                stage,
                _json_dumps_safe(state_json),
                run_id,
            ),
        )
        con.commit()
        con.close()
        return {"ok": True, "rows": len(rows), "run_id": run_id, "stage": stage}
    except Exception:
        logging.debug("Failed to persist discovery snapshot for run_id=%s", run_id, exc_info=True)
        return {"ok": False, "rows": 0, "run_id": run_id}

def _persist_resume_discovery_progress_only(
    run_id: str | None,
    snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    run_id = str(run_id or "").strip()
    if not run_id or not isinstance(snapshot, dict):
        return {"ok": False, "rows": 0, "run_id": run_id}
    roots = [str(r) for r in (snapshot.get("roots") or []) if r]
    stage = str(snapshot.get("stage") or "").strip() or "filesystem"
    state_json = {
        "stage": stage,
        "roots": roots,
        "current_root_index": _parse_int_loose(snapshot.get("current_root_index"), None),
        "current_root_path": str(snapshot.get("current_root_path") or "").strip() or None,
        "current_stack": copy.deepcopy(snapshot.get("current_stack") or []),
        "current_root_entries_scanned": int(snapshot.get("current_root_entries_scanned") or 0),
        "current_root_audio_found": int(snapshot.get("current_root_audio_found") or 0),
        "shared_entries_scanned": int(snapshot.get("shared_entries_scanned") or 0),
        "shared_files_found": int(snapshot.get("shared_files_found") or 0),
        "shared_roots_done": int(snapshot.get("shared_roots_done") or 0),
        "roots_total": int(snapshot.get("roots_total") or len(roots)),
        "entries_scanned": int(snapshot.get("shared_entries_scanned") or 0),
        "files_found": int(snapshot.get("shared_files_found") or 0),
        "albums_found": int(snapshot.get("albums_found") or 0),
        "artists_found": int(snapshot.get("artists_found") or 0),
        "folders_found": int(snapshot.get("folders_found") or 0),
        "folders_done": int(snapshot.get("folders_done") or 0),
        "folders_total": int(snapshot.get("folders_total") or 0),
        "cached_album_folders": [str(p) for p in (snapshot.get("cached_album_folders") or []) if str(p or "").strip()],
        "updated_at": time.time(),
    }
    try:
        con = _state_connect(timeout=60)
        cur = con.cursor()
        cur.execute(
            """
            UPDATE scan_resume_runs
            SET updated_at = ?,
                detected_artists_total = ?,
                detected_albums_total = ?,
                detected_tracks_total = ?,
                discovery_snapshot_ready = 1,
                discovery_stage = ?,
                discovery_state_json = ?
            WHERE run_id = ?
            """,
            (
                time.time(),
                int(state_json.get("artists_found") or 0),
                int(state_json.get("albums_found") or 0),
                int(state_json.get("files_found") or 0),
                stage,
                _json_dumps_safe(state_json),
                run_id,
            ),
        )
        con.commit()
        con.close()
        return {"ok": True, "rows": 0, "run_id": run_id, "stage": stage}
    except Exception:
        logging.debug("Failed to persist discovery progress-only snapshot for run_id=%s", run_id, exc_info=True)
        return {"ok": False, "rows": 0, "run_id": run_id}

def _load_resume_discovery_snapshot_by_run_id(run_id: str | None) -> dict[str, Any] | None:
    run_id = str(run_id or "").strip()
    if not run_id:
        return None
    try:
        con = _state_connect_readonly(timeout=20)
        cur = con.cursor()
        cur.execute(
            """
            SELECT discovery_stage, discovery_state_json
            FROM scan_resume_runs
            WHERE run_id = ?
              AND COALESCE(status, '') != 'completed'
              AND COALESCE(discovery_snapshot_ready, 0) = 1
            LIMIT 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        if not row:
            con.close()
            return None
        try:
            state_json = json.loads(row["discovery_state_json"] or "{}")
            if not isinstance(state_json, dict):
                state_json = {}
        except Exception:
            state_json = {}
        cur.execute(
            """
            SELECT root_index, file_path
            FROM scan_resume_discovery_files
            WHERE run_id = ?
            ORDER BY root_index ASC, file_path ASC
            """,
            (run_id,),
        )
        files_by_root: dict[int, list[str]] = defaultdict(list)
        for file_row in cur.fetchall():
            root_index = _parse_int_loose(file_row["root_index"] if isinstance(file_row, sqlite3.Row) else file_row[0], 0) or 0
            file_path = str(file_row["file_path"] if isinstance(file_row, sqlite3.Row) else file_row[1] or "").strip()
            if file_path:
                files_by_root[root_index].append(file_path)
        con.close()
        current_root_index = _parse_int_loose(state_json.get("current_root_index"), None)
        current_root_files = list(files_by_root.get(int(current_root_index), [])) if current_root_index is not None else []
        return {
            "run_id": run_id,
            "stage": str(row["discovery_stage"] if isinstance(row, sqlite3.Row) else row[0] or "").strip() or str(state_json.get("stage") or "filesystem"),
            "roots": [str(r) for r in (state_json.get("roots") or []) if r],
            "current_root_index": current_root_index,
            "current_root_path": str(state_json.get("current_root_path") or "").strip() or None,
            "current_stack": copy.deepcopy(state_json.get("current_stack") or []),
            "current_root_entries_scanned": int(state_json.get("current_root_entries_scanned") or 0),
            "current_root_audio_found": int(state_json.get("current_root_audio_found") or len(current_root_files)),
            "shared_entries_scanned": int(state_json.get("shared_entries_scanned") or 0),
            "shared_files_found": int(state_json.get("shared_files_found") or 0),
            "shared_roots_done": int(state_json.get("shared_roots_done") or 0),
            "roots_total": int(state_json.get("roots_total") or 0),
            "entries_scanned": int(state_json.get("entries_scanned") or 0),
            "files_found": int(state_json.get("files_found") or 0),
            "albums_found": int(state_json.get("albums_found") or 0),
            "artists_found": int(state_json.get("artists_found") or 0),
            "folders_found": int(state_json.get("folders_found") or 0),
            "folders_done": int(state_json.get("folders_done") or 0),
            "folders_total": int(state_json.get("folders_total") or 0),
            "cached_album_folders": [str(p) for p in (state_json.get("cached_album_folders") or []) if str(p or "").strip()],
            "results_by_root": {int(idx): list(paths) for idx, paths in files_by_root.items()},
            "current_root_files": current_root_files,
            "updated_at": float(state_json.get("updated_at") or 0.0),
        }
    except Exception:
        logging.debug("Failed to restore discovery snapshot for run_id=%s", run_id, exc_info=True)
        return None

def _snapshot_current_resume_discovery(reason: str = "") -> dict[str, Any]:
    with lock:
        run_id = (
            str(state.get("scan_resume_run_id") or "").strip()
            or str(state.get("scan_resume_requested_run_id") or "").strip()
            or None
        )
    runtime_snapshot = _copy_scan_discovery_runtime(run_id)
    result = _persist_resume_discovery_snapshot(run_id, runtime_snapshot)
    if result.get("ok"):
        logging.info(
            "[Resume] persisted discovery snapshot (%s) for run_id=%s (%d file row(s))",
            str(runtime_snapshot.get("stage") if isinstance(runtime_snapshot, dict) else "" or "filesystem"),
            run_id,
            int(result.get("rows") or 0),
        )
    return result

def _snapshot_current_resume_state(reason: str = "") -> dict[str, Any]:
    with lock:
        mode = _get_library_mode()
        run_id = (
            str(state.get("scan_resume_run_id") or "").strip()
            or str(state.get("scan_resume_requested_run_id") or "").strip()
            or None
        )
        has_plan = bool(mode == "files" and (state.get("files_editions_by_album_id") or {}))
        discovery_running = bool(mode == "files" and state.get("scan_discovery_running"))
    if has_plan:
        result = _snapshot_current_resume_files_plan(reason)
        result["snapshot_kind"] = "plan"
        return result
    if discovery_running or isinstance(_copy_scan_discovery_runtime(run_id), dict):
        result = _snapshot_current_resume_discovery(reason)
        result["snapshot_kind"] = "discovery"
        return result
    return {"ok": False, "rows": 0, "run_id": run_id, "snapshot_kind": "none"}

def _restore_resume_files_plan_from_run_row(
    con: sqlite3.Connection,
    run_row: sqlite3.Row | tuple | None,
) -> dict[str, Any] | None:
    if not run_row:
        return None
    run_id = str(run_row["run_id"] if isinstance(run_row, sqlite3.Row) else (run_row[0] if len(run_row) > 0 else "") or "").strip()
    if not run_id:
        return None
    cur = con.cursor()
    cur.execute(
        """
        SELECT album_id, artist_name, album_title, album_norm, folder_path, fingerprint, file_count,
               source_id, has_cover, has_artist_image, has_mbid, has_identity, identity_provider,
               strict_match_verified, strict_match_provider, strict_reject_reason, strict_tracklist_score,
               musicbrainz_id, discogs_release_id, lastfm_album_mbid, bandcamp_album_url, metadata_source,
               missing_required_tags_json, skip_heavy_processing, lookup_artist_name, lookup_album_title,
               storage_provider, storage_device_id, storage_device_label, storage_bucket_order,
               storage_rel_path, storage_access_path
        FROM scan_resume_files_plan
        WHERE run_id = ?
        ORDER BY artist_order, album_order
        """,
        (run_id,),
    )
    rows = cur.fetchall()
    if not rows:
        return None
    files_editions_by_album_id: dict[int, dict] = {}
    artist_to_album_ids: dict[str, list[int]] = {}
    restore_started_at = time.perf_counter()
    for row in rows:
        album_id = int(row["album_id"] or 0)
        if album_id <= 0:
            continue
        artist_name = str(row["artist_name"] or "").strip() or "Unknown Artist"
        folder_path = str(row["folder_path"] or "").strip()
        if not folder_path:
            continue
        fingerprint = str(row["fingerprint"] or "").strip()
        # Resume-plan restoration must stay metadata-only.
        # Do not touch the live filesystem here: the heavy hydration step runs
        # lazily when a worker actually picks the album back up.
        folder_obj = Path(folder_path)
        folder_key = folder_path
        files_editions_by_album_id[album_id] = {
            "folder": folder_obj,
            "artist": artist_name,
            "artist_name": artist_name,
            "title_raw": str(row["album_title"] or "").strip(),
            "album_title": str(row["album_title"] or "").strip(),
            "album_norm": str(row["album_norm"] or "").strip(),
            "fingerprint": fingerprint,
            "folder_key": folder_key,
            "file_count": int(row["file_count"] or 0),
            "source_id": _parse_int_loose(row["source_id"], 0) or None,
            "has_cover": bool(row["has_cover"]),
            "has_artist_image": bool(row["has_artist_image"]),
            "has_mbid": bool(row["has_mbid"]),
            "has_identity": bool(row["has_identity"]),
            "identity_provider": str(row["identity_provider"] or "").strip(),
            "strict_match_verified": bool(row["strict_match_verified"]),
            "strict_match_provider": str(row["strict_match_provider"] or "").strip(),
            "strict_reject_reason": str(row["strict_reject_reason"] or "").strip(),
            "strict_tracklist_score": float(row["strict_tracklist_score"] or 0.0),
            "musicbrainz_id": str(row["musicbrainz_id"] or "").strip(),
            "discogs_release_id": str(row["discogs_release_id"] or "").strip(),
            "lastfm_album_mbid": str(row["lastfm_album_mbid"] or "").strip(),
            "bandcamp_album_url": str(row["bandcamp_album_url"] or "").strip(),
            "metadata_source": str(row["metadata_source"] or "").strip(),
            "missing_required_tags": [],
            "skip_heavy_processing": bool(row["skip_heavy_processing"]),
            "_lookup_artist_name": str(row["lookup_artist_name"] or "").strip(),
            "_lookup_album_title": str(row["lookup_album_title"] or "").strip(),
            "storage_provider": str(row["storage_provider"] or "").strip(),
            "storage_device_id": str(row["storage_device_id"] or "").strip(),
            "storage_device_label": str(row["storage_device_label"] or "").strip(),
            "storage_bucket_order": int(row["storage_bucket_order"] or 0),
            "storage_rel_path": str(row["storage_rel_path"] or "").strip(),
            "storage_access_path": str(row["storage_access_path"] or "").strip(),
            "_resume_stub": True,
            "resume_sig_part": f"{folder_key}|{fingerprint}",
        }
        artist_to_album_ids.setdefault(artist_name, []).append(album_id)
    artists_merged = sorted(
        [(0, name, ids) for name, ids in artist_to_album_ids.items() if ids],
        key=lambda row: (
            int((files_editions_by_album_id.get(int(row[2][0])) or {}).get("storage_bucket_order") or 0) if row[2] else 0,
            str((files_editions_by_album_id.get(int(row[2][0])) or {}).get("storage_device_id") or "") if row[2] else "",
            str(row[1] or "").lower(),
        ),
    )
    if not artists_merged:
        return None
    restore_elapsed = time.perf_counter() - restore_started_at
    if restore_elapsed >= 1.0:
        logging.info(
            "[Resume] restored files plan row set for run_id=%s in %.2fs (%d row(s), %d artist bucket(s)).",
            run_id,
            restore_elapsed,
            len(rows),
            len(artists_merged),
        )
    return {
        "run_id": run_id,
        "status": str(run_row["status"] if isinstance(run_row, sqlite3.Row) else (run_row[1] if len(run_row) > 1 else "") or "").strip().lower() or "running",
        "scan_id": _parse_int_loose(run_row["scan_id"] if isinstance(run_row, sqlite3.Row) else (run_row[2] if len(run_row) > 2 else None), 0) or None,
        "detected_artists_total": int((run_row["detected_artists_total"] if isinstance(run_row, sqlite3.Row) else (run_row[3] if len(run_row) > 3 else 0)) or len(artists_merged)),
        "detected_albums_total": int((run_row["detected_albums_total"] if isinstance(run_row, sqlite3.Row) else (run_row[4] if len(run_row) > 4 else 0)) or sum(len(ids) for _a, _n, ids in artists_merged)),
        "detected_tracks_total": int((run_row["detected_tracks_total"] if isinstance(run_row, sqlite3.Row) else (run_row[5] if len(run_row) > 5 else 0)) or 0),
        "artists_merged": artists_merged,
        "total_albums": sum(len(ids) for _a, _n, ids in artists_merged),
        "files_editions_by_album_id": files_editions_by_album_id,
    }

def _load_resume_files_plan_by_run_id(run_id: str | None) -> dict[str, Any] | None:
    run_id = str(run_id or "").strip()
    if not run_id:
        return None
    try:
        con = _state_connect(timeout=30)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute(
            """
            SELECT run_id, status, scan_id, detected_artists_total, detected_albums_total, detected_tracks_total
            FROM scan_resume_runs
            WHERE run_id = ?
              AND COALESCE(status, '') != 'completed'
              AND COALESCE(plan_snapshot_ready, 0) = 1
            LIMIT 1
            """,
            (run_id,),
        )
        restored = _restore_resume_files_plan_from_run_row(con, cur.fetchone())
        con.close()
        return restored
    except Exception:
        logging.debug("Failed to restore resume files plan for run_id=%s", run_id, exc_info=True)
        return None

def _load_resume_files_plan_partial_by_run_id(run_id: str | None) -> dict[str, Any] | None:
    run_id = str(run_id or "").strip()
    if not run_id:
        return None
    try:
        con = _state_connect(timeout=30)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute(
            """
            SELECT run_id, status, scan_id, detected_artists_total, detected_albums_total, detected_tracks_total
            FROM scan_resume_runs
            WHERE run_id = ?
              AND COALESCE(status, '') != 'completed'
            LIMIT 1
            """,
            (run_id,),
        )
        restored = _restore_resume_files_plan_from_run_row(con, cur.fetchone())
        con.close()
        return restored
    except Exception:
        logging.debug("Failed to restore partial resume files plan for run_id=%s", run_id, exc_info=True)
        return None

def _load_resume_files_plan(mode: str, scan_type: str) -> dict[str, Any] | None:
    if mode != "files":
        return None
    source_signature = _compute_scan_source_signature(mode, scan_type)
    try:
        con = _state_connect(timeout=30)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute(
            """
            SELECT run_id, status, scan_id, detected_artists_total, detected_albums_total, detected_tracks_total
            FROM scan_resume_runs
            WHERE source_signature = ? AND mode = ? AND scan_type = ?
              AND COALESCE(status, '') != 'completed'
              AND COALESCE(plan_snapshot_ready, 0) = 1
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (source_signature, mode, scan_type),
        )
        restored = _restore_resume_files_plan_from_run_row(con, cur.fetchone())
        con.close()
        return restored
    except Exception:
        logging.debug("Failed to restore resume files plan for scan_type=%s", scan_type, exc_info=True)
        return None

def _snapshot_current_resume_files_plan(reason: str = "") -> dict[str, Any]:
    with lock:
        run_id = (
            str(state.get("scan_resume_run_id") or "").strip()
            or str(state.get("scan_resume_requested_run_id") or "").strip()
            or None
        )
        files_editions = dict(state.get("files_editions_by_album_id") or {})
        detected_artists_total = int(state.get("scan_detected_artists_total") or 0)
        detected_albums_total = int(state.get("scan_detected_albums_total") or 0)
        detected_tracks_total = int(state.get("scan_tracks_detected_total") or 0)
    if not run_id or not files_editions:
        return {"ok": False, "rows": 0, "run_id": run_id}
    artist_to_album_ids: dict[str, list[int]] = defaultdict(list)
    for album_id, fe in files_editions.items():
        artist_name = str((fe or {}).get("artist_name") or (fe or {}).get("artist") or "").strip() or "Unknown Artist"
        try:
            artist_to_album_ids[artist_name].append(int(album_id))
        except Exception:
            continue
    artists_merged = [(0, artist_name, album_ids) for artist_name, album_ids in sorted(artist_to_album_ids.items(), key=lambda x: x[0].lower())]
    rows = _persist_resume_files_plan(
        run_id,
        artists_merged,
        files_editions,
        detected_artists_total=detected_artists_total,
        detected_albums_total=detected_albums_total,
        detected_tracks_total=detected_tracks_total,
    )
    if rows:
        logging.info("[Resume] persisted %d remaining Files album(s) for run_id=%s (%s)", rows, run_id, str(reason or "snapshot"))
    return {"ok": bool(rows), "rows": int(rows), "run_id": run_id}

def _hydrate_resume_files_edition(fe: dict[str, Any]) -> dict[str, Any] | None:
    folder_raw = fe.get("folder")
    if not folder_raw:
        return None
    canonical_folder = Path(str(folder_raw))
    access_raw = str(fe.get("storage_access_path") or "").strip()
    try:
        folder_for_io = path_for_fs_access(Path(access_raw)) if access_raw else path_for_fs_access(canonical_folder)
    except Exception:
        folder_for_io = Path(access_raw or str(folder_raw))
    folder = canonical_folder if access_raw else folder_for_io
    ordered_paths = _files_collect_ordered_audio_paths(folder_for_io, fe.get("ordered_paths") or [])
    if not ordered_paths:
        return None
    canonical_ordered_paths: list[Path] = []
    if access_raw:
        for p in ordered_paths:
            try:
                rel = Path(p).relative_to(folder_for_io)
                canonical_ordered_paths.append(canonical_folder / rel)
            except Exception:
                canonical_ordered_paths.append(Path(p))
    else:
        canonical_ordered_paths = list(ordered_paths)
    first_tags = extract_tags(ordered_paths[0]) or {}
    artist_name = str(fe.get("artist_name") or fe.get("artist") or "").strip() or "Unknown Artist"
    album_title_tag = str(fe.get("album_title") or fe.get("title_raw") or "").strip() or _sanitize_album_title_display(folder.name.replace("_", " "))
    inferred_artist_name, inferred_album_title = ("", "")
    try:
        inferred_artist_name, inferred_album_title = _infer_artist_album_from_folder(folder, ordered_paths)
    except Exception:
        pass
    if (
        (
            (artist_name or "").strip().lower() in {"unknown", "unknown artist", "various", "various artists"}
            or not _identity_artist_fallback_is_usable(artist_name)
        )
        and inferred_artist_name
    ):
        artist_name = inferred_artist_name
    if not album_title_tag and inferred_album_title:
        album_title_tag = inferred_album_title
    album_title_tag = _sanitize_album_title_display(album_title_tag)
    tracks: list[Track] = []
    first_disc, first_trk = _parse_disc_track_loose(first_tags, fallback_disc=1, fallback_track=1)
    first_title = str(first_tags.get("title") or first_tags.get("name") or "").strip()
    for i, p in enumerate(ordered_paths):
        disc, trk = _infer_disc_track_from_text(p.stem, i + 1)
        title = re.sub(r"[_]+", " ", p.stem).strip() or f"Track {i + 1}"
        if i == 0:
            disc = first_disc or disc
            trk = first_trk or trk
            title = first_title or title
        tracks.append(Track(title=(title or "").strip(), idx=max(1, int(trk or 1)), disc=max(1, int(disc or 1)), dur=0))
    normalize_parenthetical = bool(_parse_bool(_get_config_from_db("NORMALIZE_PARENTHETICAL_FOR_DEDUPE") or "true"))
    album_norm = norm_album_for_dedup(album_title_tag, normalize_parenthetical)
    fingerprint = str(fe.get("fingerprint") or "").strip() or _compute_album_fingerprint(ordered_paths)
    cached = {
        "fingerprint": fingerprint,
        "has_cover": bool(fe.get("has_cover")),
        "has_artist_image": bool(fe.get("has_artist_image")),
        "has_mbid": bool(fe.get("has_mbid")),
        "has_identity": bool(fe.get("has_identity")),
        "identity_provider": str(fe.get("identity_provider") or "").strip(),
        "strict_match_verified": bool(fe.get("strict_match_verified")),
        "strict_match_provider": str(fe.get("strict_match_provider") or "").strip(),
        "strict_reject_reason": str(fe.get("strict_reject_reason") or "").strip(),
        "strict_tracklist_score": float(fe.get("strict_tracklist_score") or 0.0),
        "musicbrainz_id": str(fe.get("musicbrainz_id") or "").strip(),
        "discogs_release_id": str(fe.get("discogs_release_id") or "").strip(),
        "lastfm_album_mbid": str(fe.get("lastfm_album_mbid") or "").strip(),
        "bandcamp_album_url": str(fe.get("bandcamp_album_url") or "").strip(),
        "metadata_source": str(fe.get("metadata_source") or "").strip(),
    }
    identity_now = _extract_files_identity_fields(tags=first_tags, edition={}, cached=cached)
    hydrated = dict(fe)
    hydrated.update(
        {
            "folder": folder,
            "artist": artist_name,
            "artist_name": artist_name,
            "title_raw": album_title_tag,
            "album_title": album_title_tag,
            "album_norm": album_norm,
            "tracks": tracks,
            "format": str(fe.get("format") or "").strip() or (ordered_paths[0].suffix.lower().lstrip(".") if ordered_paths else "").upper(),
            "tags": first_tags,
            "confidence_score": float(fe.get("confidence_score") or 0.8),
            "file_count": int(fe.get("file_count") or len(ordered_paths)),
            "ordered_paths": ordered_paths,
            "fingerprint": fingerprint,
            "folder_key": _album_folder_cache_key(folder),
            "source_id": _parse_int_loose(fe.get("source_id"), 0) or None,
            "missing_required_tags": _check_required_tags(first_tags, REQUIRED_TAGS, edition={"tracks": tracks}),
            "has_cover": bool(album_folder_has_cover(folder_for_io)),
            "has_artist_image": bool(_artist_folder_has_image(folder_for_io.parent if folder_for_io.parent else folder_for_io)),
            "has_mbid": bool(identity_now["has_mbid"]),
            "has_identity": bool(identity_now["has_identity"]),
            "identity_provider": identity_now["identity_provider"],
            "strict_match_verified": bool(identity_now.get("strict_match_verified")),
            "strict_match_provider": identity_now.get("strict_match_provider") or "",
            "strict_reject_reason": identity_now.get("strict_reject_reason") or "",
            "strict_tracklist_score": float(identity_now.get("strict_tracklist_score") or 0.0),
            "musicbrainz_id": identity_now["musicbrainz_id"],
            "discogs_release_id": identity_now["discogs_release_id"],
            "lastfm_album_mbid": identity_now["lastfm_album_mbid"],
            "bandcamp_album_url": identity_now["bandcamp_album_url"],
            "metadata_source": identity_now["metadata_source"],
            "canonical_ordered_paths": canonical_ordered_paths,
            "storage_access_path": access_raw,
            "_resume_stub": False,
        }
    )
    return hydrated

def _prepare_resume_scan_artists(
    mode: str,
    scan_type: str,
    artists_merged: list[tuple[int, str, list[int]]],
    files_editions_by_album_id: dict[int, dict] | None = None,
    resume_run_id_override: str | None = None,
    progress_cb=None,
    pause_event: threading.Event | None = None,
    force_include_plan_rows: bool = False,
) -> tuple[str, list[tuple[int, str, list[int]]], int, int]:
    """
    Create/reuse a persistent resume run and return artists that still need processing.
    Returns: (run_id, artists_to_scan, skipped_artists, skipped_albums).
    """
    now = time.time()
    source_signature = _compute_scan_source_signature(mode, scan_type)
    total_artists = len(artists_merged)
    progress_emit_every_artists = 250
    progress_emit_every_sec = 0.25
    progress_last_idx_by_stage: dict[str, int] = {}
    progress_last_ts_by_stage: dict[str, float] = {}

    def _emit_progress(
        *,
        stage: str,
        done: int,
        included_artists: int,
        included_albums: int,
        skipped_artists: int,
        skipped_albums: int,
        force: bool = False,
    ) -> None:
        if not progress_cb:
            return
        now_mono = time.monotonic()
        prev_idx = int(progress_last_idx_by_stage.get(stage, 0))
        prev_ts = float(progress_last_ts_by_stage.get(stage, 0.0))
        should_emit = bool(force or done <= 1 or done >= total_artists)
        if not should_emit:
            if (done - prev_idx) >= progress_emit_every_artists:
                should_emit = True
            elif (now_mono - prev_ts) >= progress_emit_every_sec:
                should_emit = True
        if not should_emit:
            return
        progress_last_idx_by_stage[stage] = int(done)
        progress_last_ts_by_stage[stage] = now_mono
        try:
            progress_cb(
                stage=stage,
                done=done,
                total=total_artists,
                included_artists=included_artists,
                included_albums=included_albums,
                skipped_artists=skipped_artists,
                skipped_albums=skipped_albums,
            )
        except Exception:
            pass

    def _wait_pause() -> bool:
        if pause_event is None:
            return not scan_should_stop.is_set()
        while pause_event.is_set() and not scan_should_stop.is_set():
            time.sleep(0.2)
        return not scan_should_stop.is_set()

    files_signature_part_by_album_id: dict[int, str] = {}
    if mode == "files":
        files_map = files_editions_by_album_id or {}
        seen_album_ids: set[int] = set()
        ordered_album_ids: list[int] = []
        for _artist_id, _artist_name, album_ids in artists_merged:
            if not _wait_pause():
                break
            for aid in album_ids:
                if aid in seen_album_ids:
                    continue
                seen_album_ids.add(aid)
                ordered_album_ids.append(aid)
        for aid in ordered_album_ids:
            fe = files_map.get(aid) or {}
            part = fe.get("resume_sig_part")
            if not part:
                folder = fe.get("folder")
                folder_key = _album_folder_cache_key(folder) if folder else str(aid)
                fp = (fe.get("fingerprint") or "").strip()
                part = f"{folder_key}|{fp}"
                if isinstance(fe, dict):
                    fe["resume_sig_part"] = part
            files_signature_part_by_album_id[aid] = str(part)

    artists_with_signature: list[tuple[int, str, list[int], str]] = []
    for idx, (artist_id, artist_name, album_ids) in enumerate(artists_merged, start=1):
        if not _wait_pause():
            break
        sig = _compute_artist_signature(
            mode,
            artist_name,
            album_ids,
            files_editions_by_album_id=files_editions_by_album_id,
            files_signature_part_by_album_id=files_signature_part_by_album_id,
        )
        artists_with_signature.append((artist_id, artist_name, album_ids, sig))
        _emit_progress(
            stage="signatures",
            done=idx,
            included_artists=0,
            included_albums=0,
            skipped_artists=0,
            skipped_albums=0,
        )

    run_id: str | None = None
    artists_to_scan: list[tuple[int, str, list[int]]] = []
    skipped_artists = 0
    skipped_albums = 0
    included_albums = 0

    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    cur = con.cursor()
    prev = None
    resume_run_id_override = str(resume_run_id_override or "").strip()
    if resume_run_id_override:
        cur.execute(
            """
            SELECT run_id, status
            FROM scan_resume_runs
            WHERE run_id = ?
              AND mode = ? AND scan_type = ?
              AND COALESCE(status, '') != 'completed'
            LIMIT 1
            """,
            (resume_run_id_override, mode, scan_type),
        )
        prev = cur.fetchone()
    if not prev:
        cur.execute(
            """
            SELECT run_id, status
            FROM scan_resume_runs
            WHERE source_signature = ? AND mode = ? AND scan_type = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (source_signature, mode, scan_type),
        )
        prev = cur.fetchone()
    if prev and (prev[1] or "").strip().lower() != "completed":
        run_id = prev[0]
        cur.execute(
            "SELECT artist_name, artist_signature, status FROM scan_resume_artists WHERE run_id = ?",
            (run_id,),
        )
        prev_artists = {
            (r[0] or ""): {
                "artist_signature": (r[1] or ""),
                "status": (r[2] or "pending").strip().lower(),
            }
            for r in cur.fetchall()
        }
        pending_rows: list[tuple[str, str, str, int, float]] = []
        for idx, (artist_id, artist_name, album_ids, sig) in enumerate(artists_with_signature, start=1):
            if not _wait_pause():
                break
            prev_row = prev_artists.get(artist_name)
            is_done_same_signature = bool(
                prev_row
                and prev_row.get("status") == "done"
                and prev_row.get("artist_signature") == sig
            )
            if is_done_same_signature and not bool(force_include_plan_rows):
                skipped_artists += 1
                skipped_albums += len(album_ids)
            else:
                artists_to_scan.append((artist_id, artist_name, album_ids))
                included_albums += len(album_ids)
                pending_rows.append((run_id, artist_name, sig, len(album_ids), now))
                if len(pending_rows) >= 1000:
                    cur.executemany(
                        """
                        INSERT INTO scan_resume_artists
                        (run_id, artist_name, artist_signature, status, album_count, updated_at, error)
                        VALUES (?, ?, ?, 'pending', ?, ?, NULL)
                        ON CONFLICT(run_id, artist_name) DO UPDATE SET
                          artist_signature=excluded.artist_signature,
                          status='pending',
                          album_count=excluded.album_count,
                          updated_at=excluded.updated_at,
                          error=NULL
                        """,
                        pending_rows,
                    )
                    pending_rows = []
            _emit_progress(
                stage="resume_compare",
                done=idx,
                included_artists=len(artists_to_scan),
                included_albums=included_albums,
                skipped_artists=skipped_artists,
                skipped_albums=skipped_albums,
            )
        if pending_rows:
            cur.executemany(
                """
                INSERT INTO scan_resume_artists
                (run_id, artist_name, artist_signature, status, album_count, updated_at, error)
                VALUES (?, ?, ?, 'pending', ?, ?, NULL)
                ON CONFLICT(run_id, artist_name) DO UPDATE SET
                  artist_signature=excluded.artist_signature,
                  status='pending',
                  album_count=excluded.album_count,
                  updated_at=excluded.updated_at,
                  error=NULL
                """,
                pending_rows,
            )
        cur.execute(
            "UPDATE scan_resume_runs SET updated_at = ?, status = 'running' WHERE run_id = ?",
            (now, run_id),
        )
    else:
        run_id = uuid.uuid4().hex
        cur.execute(
            """
            INSERT INTO scan_resume_runs
            (run_id, created_at, updated_at, mode, scan_type, source_signature, status, scan_id)
            VALUES (?, ?, ?, ?, ?, ?, 'running', NULL)
            """,
            (run_id, now, now, mode, scan_type, source_signature),
        )
        pending_rows: list[tuple[str, str, str, int, float]] = []
        for idx, (artist_id, artist_name, album_ids, sig) in enumerate(artists_with_signature, start=1):
            if not _wait_pause():
                break
            artists_to_scan.append((artist_id, artist_name, album_ids))
            included_albums += len(album_ids)
            pending_rows.append((run_id, artist_name, sig, len(album_ids), now))
            if len(pending_rows) >= 1000:
                cur.executemany(
                    """
                    INSERT INTO scan_resume_artists
                    (run_id, artist_name, artist_signature, status, album_count, updated_at, error)
                    VALUES (?, ?, ?, 'pending', ?, ?, NULL)
                    """,
                    pending_rows,
                )
                pending_rows = []
            _emit_progress(
                stage="resume_seed",
                done=idx,
                included_artists=len(artists_to_scan),
                included_albums=included_albums,
                skipped_artists=0,
                skipped_albums=0,
            )
        if pending_rows:
            cur.executemany(
                """
                INSERT INTO scan_resume_artists
                (run_id, artist_name, artist_signature, status, album_count, updated_at, error)
                VALUES (?, ?, ?, 'pending', ?, ?, NULL)
                """,
                pending_rows,
            )
    con.commit()
    con.close()
    _emit_progress(
        stage="done",
        done=total_artists,
        included_artists=len(artists_to_scan),
        included_albums=included_albums,
        skipped_artists=skipped_artists,
        skipped_albums=skipped_albums,
        force=True,
    )
    return run_id, artists_to_scan, skipped_artists, skipped_albums

def _has_unfinished_resume_run(mode: str, scan_type: str) -> bool:
    """
    Return True if there is a non-completed resume run for the current source signature.
    Used to avoid clearing progressive Files index when the user is resuming an interrupted scan.
    """
    source_signature = _compute_scan_source_signature(mode, scan_type)
    try:
        con = _state_connect_readonly(timeout=15)
        cur = con.cursor()
        cur.execute(
            """
            SELECT status
            FROM scan_resume_runs
            WHERE source_signature = ? AND mode = ? AND scan_type = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (source_signature, mode, scan_type),
        )
        row = cur.fetchone()
        con.close()
        if not row:
            return False
        return (row[0] or "").strip().lower() != "completed"
    except Exception:
        logging.debug("Failed to check unfinished resume runs", exc_info=True)
        return False

def _get_resume_run_snapshot(mode: str, scan_type: str) -> dict[str, Any] | None:
    """
    Return the latest unfinished resume run for the current source signature with
    counts describing what remains to process.
    """
    source_signature = _compute_scan_source_signature(mode, scan_type)
    try:
        con = _state_connect_readonly(timeout=15)
        cur = con.cursor()
        cur.execute(
            """
            SELECT run_id, status, created_at, updated_at, scan_id,
                   COALESCE(detected_artists_total, 0),
                   COALESCE(detected_albums_total, 0),
                   COALESCE(detected_tracks_total, 0),
                   COALESCE(plan_snapshot_ready, 0),
                   COALESCE(discovery_snapshot_ready, 0),
                   COALESCE(discovery_stage, ''),
                   COALESCE(discovery_state_json, '{}')
            FROM scan_resume_runs
            WHERE source_signature = ? AND mode = ? AND scan_type = ?
              AND COALESCE(status, '') != 'completed'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (source_signature, mode, scan_type),
        )
        row = cur.fetchone()
        if not row:
            con.close()
            return None
        run_id = str(row[0] or "")
        run_status = str(row[1] or "").strip().lower() or "pending"
        created_at = float(row[2] or 0.0) if row[2] is not None else None
        updated_at = float(row[3] or 0.0) if row[3] is not None else None
        scan_id = int(row[4] or 0) if row[4] is not None else None
        detected_artists_total = int(row[5] or 0)
        detected_albums_total = int(row[6] or 0)
        detected_tracks_total = int(row[7] or 0)
        plan_snapshot_ready = bool(row[8])
        discovery_snapshot_ready = bool(row[9])
        discovery_stage = str(row[10] or "").strip()
        try:
            discovery_state = json.loads(row[11] or "{}")
            if not isinstance(discovery_state, dict):
                discovery_state = {}
        except Exception:
            discovery_state = {}
        cur.execute(
            """
            SELECT
              COUNT(*) AS total_artists,
              COALESCE(SUM(album_count), 0) AS total_albums,
              COALESCE(SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END), 0) AS done_artists,
              COALESCE(SUM(CASE WHEN status = 'done' THEN album_count ELSE 0 END), 0) AS done_albums,
              COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0) AS pending_artists,
              COALESCE(SUM(CASE WHEN status = 'pending' THEN album_count ELSE 0 END), 0) AS pending_albums,
              COALESCE(SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END), 0) AS running_artists,
              COALESCE(SUM(CASE WHEN status = 'running' THEN album_count ELSE 0 END), 0) AS running_albums,
              COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed_artists,
              COALESCE(SUM(CASE WHEN status = 'failed' THEN album_count ELSE 0 END), 0) AS failed_albums
            FROM scan_resume_artists
            WHERE run_id = ?
            """,
            (run_id,),
        )
        counts = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        con.close()
        total_artists = int(counts[0] or 0)
        total_albums = int(counts[1] or 0)
        done_artists = int(counts[2] or 0)
        done_albums = int(counts[3] or 0)
        pending_artists = int(counts[4] or 0)
        pending_albums = int(counts[5] or 0)
        running_artists = int(counts[6] or 0)
        running_albums = int(counts[7] or 0)
        failed_artists = int(counts[8] or 0)
        failed_albums = int(counts[9] or 0)
        remaining_artists = max(0, pending_artists + running_artists + failed_artists)
        remaining_albums = max(0, pending_albums + running_albums + failed_albums)
        if plan_snapshot_ready and remaining_artists <= 0:
            try:
                con2 = _state_connect_readonly(timeout=15)
                cur2 = con2.cursor()
                cur2.execute(
                    "SELECT COUNT(*), COALESCE(COUNT(DISTINCT artist_name), 0) FROM scan_resume_files_plan WHERE run_id = ?",
                    (run_id,),
                )
                plan_counts = cur2.fetchone() or (0, 0)
                con2.close()
                remaining_albums = max(remaining_albums, int(plan_counts[0] or 0))
                remaining_artists = max(remaining_artists, int(plan_counts[1] or 0))
            except Exception:
                logging.debug("Failed to count resume files plan rows for run_id=%s", run_id, exc_info=True)
        if discovery_snapshot_ready and remaining_artists <= 0 and remaining_albums <= 0:
            remaining_artists = max(remaining_artists, int(discovery_state.get("artists_found") or detected_artists_total or 0))
            remaining_albums = max(
                remaining_albums,
                int(
                    discovery_state.get("albums_found")
                    or discovery_state.get("folders_found")
                    or detected_albums_total
                    or 0
                ),
            )
        return {
            "available": bool(
                run_id
                and (
                    remaining_artists > 0
                    or remaining_albums > 0
                    or (
                        discovery_snapshot_ready
                        and bool(
                            int(discovery_state.get("entries_scanned") or 0)
                            or int(discovery_state.get("files_found") or 0)
                        )
                    )
                )
            ),
            "run_id": run_id,
            "status": run_status,
            "scan_type": scan_type,
            "created_at": created_at,
            "updated_at": updated_at,
            "scan_id": scan_id,
            "total_artists": total_artists,
            "total_albums": total_albums,
            "done_artists": done_artists,
            "done_albums": done_albums,
            "remaining_artists": remaining_artists,
            "remaining_albums": remaining_albums,
            "pending_artists": pending_artists,
            "pending_albums": pending_albums,
            "running_artists": running_artists,
            "running_albums": running_albums,
            "failed_artists": failed_artists,
            "failed_albums": failed_albums,
            "detected_artists_total": detected_artists_total,
            "detected_albums_total": detected_albums_total,
            "detected_tracks_total": detected_tracks_total,
            "plan_snapshot_ready": bool(plan_snapshot_ready),
            "discovery_snapshot_ready": bool(discovery_snapshot_ready),
            "discovery_stage": discovery_stage,
            "discovery_entries_scanned": int(discovery_state.get("entries_scanned") or 0),
            "discovery_files_found": int(discovery_state.get("files_found") or 0),
        }
    except Exception:
        logging.debug("Failed to read resume snapshot for scan_type=%s", scan_type, exc_info=True)
        return None

def _get_resume_run_snapshot_by_run_id(run_id: str | None) -> dict[str, Any] | None:
    run_id = str(run_id or "").strip()
    if not run_id:
        return None
    try:
        con = _state_connect_readonly(timeout=15)
        cur = con.cursor()
        cur.execute(
            """
            SELECT run_id, status, created_at, updated_at, scan_id,
                   COALESCE(detected_artists_total, 0),
                   COALESCE(detected_albums_total, 0),
                   COALESCE(detected_tracks_total, 0),
                   COALESCE(plan_snapshot_ready, 0),
                   COALESCE(discovery_snapshot_ready, 0),
                   COALESCE(discovery_stage, ''),
                   COALESCE(discovery_state_json, '{}'),
                   mode,
                   scan_type
            FROM scan_resume_runs
            WHERE run_id = ?
              AND COALESCE(status, '') != 'completed'
            LIMIT 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        con.close()
        if not row:
            return None
        snap = _get_resume_run_snapshot(str(row[12] or "files"), str(row[13] or "full"))
        if snap and str(snap.get("run_id") or "").strip() == run_id:
            return snap
        # Signature may have changed since the run was created; compute directly from run_id.
        con = _state_connect_readonly(timeout=15)
        cur = con.cursor()
        cur.execute(
            """
            SELECT
              COUNT(*) AS total_artists,
              COALESCE(SUM(album_count), 0) AS total_albums,
              COALESCE(SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END), 0) AS done_artists,
              COALESCE(SUM(CASE WHEN status = 'done' THEN album_count ELSE 0 END), 0) AS done_albums,
              COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0) AS pending_artists,
              COALESCE(SUM(CASE WHEN status = 'pending' THEN album_count ELSE 0 END), 0) AS pending_albums,
              COALESCE(SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END), 0) AS running_artists,
              COALESCE(SUM(CASE WHEN status = 'running' THEN album_count ELSE 0 END), 0) AS running_albums,
              COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed_artists,
              COALESCE(SUM(CASE WHEN status = 'failed' THEN album_count ELSE 0 END), 0) AS failed_albums
            FROM scan_resume_artists
            WHERE run_id = ?
            """,
            (run_id,),
        )
        counts = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        remaining_artists = max(0, int(counts[4] or 0) + int(counts[6] or 0) + int(counts[8] or 0))
        remaining_albums = max(0, int(counts[5] or 0) + int(counts[7] or 0) + int(counts[9] or 0))
        if bool(row[8]) and remaining_artists <= 0:
            cur.execute(
                "SELECT COUNT(*), COALESCE(COUNT(DISTINCT artist_name), 0) FROM scan_resume_files_plan WHERE run_id = ?",
                (run_id,),
            )
            plan_counts = cur.fetchone() or (0, 0)
            remaining_albums = max(remaining_albums, int(plan_counts[0] or 0))
            remaining_artists = max(remaining_artists, int(plan_counts[1] or 0))
        try:
            discovery_state = json.loads(row[11] or "{}")
            if not isinstance(discovery_state, dict):
                discovery_state = {}
        except Exception:
            discovery_state = {}
        if bool(row[9]) and remaining_artists <= 0 and remaining_albums <= 0:
            remaining_artists = max(remaining_artists, int(discovery_state.get("artists_found") or row[5] or 0))
            remaining_albums = max(
                remaining_albums,
                int(discovery_state.get("albums_found") or discovery_state.get("folders_found") or row[6] or 0),
            )
        con.close()
        return {
            "available": bool(
                run_id
                and (
                    remaining_artists > 0
                    or remaining_albums > 0
                    or (
                        bool(row[9])
                        and bool(
                            int(discovery_state.get("entries_scanned") or 0)
                            or int(discovery_state.get("files_found") or 0)
                        )
                    )
                )
            ),
            "run_id": run_id,
            "status": str(row[1] or "").strip().lower() or "pending",
            "scan_type": str(row[13] or "full"),
            "created_at": float(row[2] or 0.0) if row[2] is not None else None,
            "updated_at": float(row[3] or 0.0) if row[3] is not None else None,
            "scan_id": int(row[4] or 0) if row[4] is not None else None,
            "total_artists": int(counts[0] or 0),
            "total_albums": int(counts[1] or 0),
            "done_artists": int(counts[2] or 0),
            "done_albums": int(counts[3] or 0),
            "remaining_artists": remaining_artists,
            "remaining_albums": remaining_albums,
            "pending_artists": int(counts[4] or 0),
            "pending_albums": int(counts[5] or 0),
            "running_artists": int(counts[6] or 0),
            "running_albums": int(counts[7] or 0),
            "failed_artists": int(counts[8] or 0),
            "failed_albums": int(counts[9] or 0),
            "detected_artists_total": int(row[5] or 0),
            "detected_albums_total": int(row[6] or 0),
            "detected_tracks_total": int(row[7] or 0),
            "plan_snapshot_ready": bool(row[8]),
            "discovery_snapshot_ready": bool(row[9]),
            "discovery_stage": str(row[10] or "").strip(),
            "discovery_entries_scanned": int(discovery_state.get("entries_scanned") or 0),
            "discovery_files_found": int(discovery_state.get("files_found") or 0),
            "signature_match": False,
        }
    except Exception:
        logging.debug("Failed to read resume snapshot for run_id=%s", run_id, exc_info=True)
        return None

def _get_latest_resume_run_snapshot_any_signature(mode: str, scan_type: str) -> dict[str, Any] | None:
    try:
        con = _state_connect_readonly(timeout=15)
        cur = con.cursor()
        cur.execute(
            """
            SELECT run_id
            FROM scan_resume_runs
            WHERE mode = ? AND scan_type = ?
              AND COALESCE(status, '') != 'completed'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (mode, scan_type),
        )
        row = cur.fetchone()
        con.close()
        if not row:
            return None
        return _get_resume_run_snapshot_by_run_id(str(row[0] or ""))
    except Exception:
        logging.debug("Failed to read latest resume snapshot for mode=%s scan_type=%s", mode, scan_type, exc_info=True)
        return None

def _get_startup_resume_snapshot(mode: str) -> dict[str, Any] | None:
    mode_norm = str(mode or "files").strip().lower() or "files"
    try:
        con = _state_connect_readonly(timeout=15)
        cur = con.cursor()
        cur.execute(
            """
            SELECT run_id
            FROM scan_resume_runs
            WHERE mode = ?
              AND COALESCE(status, '') = 'running'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (mode_norm,),
        )
        row = cur.fetchone()
        con.close()
        if not row:
            return None
        snap = _get_resume_run_snapshot_by_run_id(str(row[0] or "").strip())
        if isinstance(snap, dict) and snap.get("available"):
            return snap
    except Exception:
        logging.debug("Failed to read startup resume snapshot for mode=%s", mode_norm, exc_info=True)
    return None

def _maybe_resume_interrupted_scan_on_startup(delay_seconds: float = 8.0) -> None:
    def _runner() -> None:
        if delay_seconds > 0:
            time.sleep(float(delay_seconds))
        mode = _get_library_mode()
        if mode != "files":
            return
        snapshot = _get_startup_resume_snapshot(mode)
        if not snapshot:
            return
        resume_run_id = str(snapshot.get("run_id") or "").strip() or None
        scan_type = str(snapshot.get("scan_type") or "full").strip().lower() or "full"
        if not resume_run_id:
            return
        with lock:
            if bool(state.get("scanning")) or bool(state.get("scan_finalizing")) or bool(state.get("scan_starting")):
                return
            state["scan_resume_requested_run_id"] = resume_run_id
            run_improve_after = bool(state.get("run_improve_after", False))
        try:
            reconciled = _reconcile_scan_move_trace_backlog(reason="startup_resume")
            if reconciled:
                logging.info(
                    "[Trace] V✅ Startup resume reconciled %s move trace row(s) before restarting run_id=%s",
                    int(reconciled),
                    resume_run_id,
                )
        except Exception:
            logging.warning("[Trace] X❌ Startup resume move trace reconcile failed", exc_info=True)
        ok, meta = _try_begin_scan(
            scan_type=scan_type,
            source="startup_resume",
            run_improve_after=run_improve_after,
            scheduler_run_id=None,
        )
        if not ok:
            with lock:
                if str(state.get("scan_resume_requested_run_id") or "").strip() == resume_run_id:
                    state["scan_resume_requested_run_id"] = None
            logging.warning(
                "Startup resume skipped for run %s (%s): %s",
                resume_run_id,
                scan_type,
                str(meta.get("reason") or "scan_start_failed"),
            )
            return
        _set_resume_run_status(resume_run_id, "running")
        logging.info(
            "Startup resume started %s scan from unfinished run %s.",
            scan_type,
            resume_run_id,
        )

    threading.Thread(target=_runner, daemon=True, name="startup-resume-scan").start()

def _set_resume_artist_status(run_id: str | None, artist_name: str, status: str, error: str | None = None) -> None:
    """Update one artist status for a resume run."""
    if not run_id or not artist_name:
        return
    now = time.time()
    try:
        con = _state_connect(timeout=15)
        cur = con.cursor()
        cur.execute(
            """
            UPDATE scan_resume_artists
            SET status = ?, updated_at = ?, error = ?
            WHERE run_id = ? AND artist_name = ?
            """,
            ((status or "pending").strip().lower(), now, error, run_id, artist_name),
        )
        cur.execute(
            "UPDATE scan_resume_runs SET updated_at = ? WHERE run_id = ?",
            (now, run_id),
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to update resume artist status for %s", artist_name, exc_info=True)

def _set_resume_run_status(run_id: str | None, status: str, scan_id: int | None = None) -> None:
    """Finalize resume run status."""
    if not run_id:
        return
    now = time.time()
    try:
        con = _state_connect(timeout=15)
        cur = con.cursor()
        cur.execute(
            """
            UPDATE scan_resume_runs
            SET status = ?, updated_at = ?, scan_id = COALESCE(?, scan_id)
            WHERE run_id = ?
            """,
            ((status or "failed").strip().lower(), now, scan_id, run_id),
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to finalize resume run %s", run_id, exc_info=True)

def _build_scan_plan(scan_type: str = "full") -> tuple[list[tuple[int, str, list[int]]], int]:
    """
    Build the list of artists/albums to scan and return (artists_merged, total_albums).
    PMDA now supports files mode only for scan sources.
    """
    mode = _get_library_mode()
    scan_type = (scan_type or "full").strip().lower()
    if mode == "files":
        active_roots = _effective_files_scan_roots(enabled_only=True)
        if not active_roots:
            raise RuntimeError("FILES scan roots are empty – configure at least one incoming/intake folder for files library mode.")
        log_scan("FILES mode scan roots: %s", ", ".join(str(r) for r in active_roots))
        with lock:
            state["scan_resume_plan_restored"] = False
        with lock:
            requested_resume_run_id = str(state.get("scan_resume_requested_run_id") or "").strip() or None
        restored_plan = None
        if requested_resume_run_id:
            restored_plan = _load_resume_files_plan_by_run_id(requested_resume_run_id)
        if not restored_plan:
            restored_plan = _load_resume_files_plan("files", scan_type)
        if restored_plan:
            artists_merged = list(restored_plan.get("artists_merged") or [])
            total_albums = int(restored_plan.get("total_albums") or 0)
            files_editions_by_album_id = dict(restored_plan.get("files_editions_by_album_id") or {})
            storage_plan = _storage_plan_summary_from_files_editions(files_editions_by_album_id)
            storage_devices_total = len({str(item.get("storage_device_id") or "") for item in storage_plan if str(item.get("storage_device_id") or "")})
            with lock:
                state["files_editions_by_album_id"] = files_editions_by_album_id
                state["scan_resume_run_id"] = str(restored_plan.get("run_id") or "").strip() or None
                state["scan_resume_plan_restored"] = True
                state["scan_detected_artists_total"] = int(restored_plan.get("detected_artists_total") or len(artists_merged))
                state["scan_detected_albums_total"] = int(restored_plan.get("detected_albums_total") or total_albums)
                state["scan_tracks_detected_total"] = int(restored_plan.get("detected_tracks_total") or 0)
                state["scan_discovery_running"] = False
                state["scan_discovery_stage"] = "ready"
                state["scan_discovery_current_root"] = None
                state["scan_discovery_updated_at"] = time.time()
                if storage_plan:
                    state["storage_power_saver_enabled"] = True
                    state["storage_provider"] = "unraid"
                    state["storage_scan_plan"] = storage_plan
                    state["storage_bucket_history"] = []
                    state["storage_active_devices"] = 1
                    state["storage_devices_total"] = storage_devices_total
                    state["storage_current_device_id"] = None
                    state["storage_current_device_label"] = None
                    state["storage_bucket_done"] = 0
                    state["storage_bucket_total"] = 0
                    state["storage_buckets_done"] = 0
                    state["storage_buckets_total"] = len(storage_plan)
                    state["storage_estimated_watts_saved"] = _storage_estimated_watts_saved(1, storage_devices_total)
                    state["storage_validation_error"] = ""
            log_scan(
                "FILES mode restored resume plan %s: %d remaining artist(s), %d remaining album(s).",
                str(restored_plan.get("run_id") or ""),
                len(artists_merged),
                total_albums,
            )
            if storage_plan:
                log_scan(
                    "[STORAGE] Restored disk-aware resume plan: %d album(s) across %d bucket(s), active disks 1/%d.",
                    total_albums,
                    len(storage_plan),
                    storage_devices_total,
                )
            return artists_merged, total_albums
        artists_merged, total_albums, files_editions_by_album_id = _build_files_editions(scan_type=scan_type)
        with lock:
            state["files_editions_by_album_id"] = files_editions_by_album_id
        return artists_merged, total_albums

    raise RuntimeError("Only files library mode is supported for scan planning.")

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}

def _mcp_scan_resume_state_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scan_resume_state(*args, **kwargs)

def _reconcile_scan_duplicates_across_artist_buckets_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _reconcile_scan_duplicates_across_artist_buckets(*args, **kwargs)

def save_scan_editions_to_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return save_scan_editions_to_db(*args, **kwargs)

def save_scan_editions_artist_to_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return save_scan_editions_artist_to_db(*args, **kwargs)

def _refresh_scan_history_from_published_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _refresh_scan_history_from_published(*args, **kwargs)

def load_scan_from_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return load_scan_from_db(*args, **kwargs)

def _load_files_album_scan_cache_map_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _load_files_album_scan_cache_map(*args, **kwargs)

def _upsert_files_album_scan_cache_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _upsert_files_album_scan_cache_rows(*args, **kwargs)

def _snapshot_files_album_scan_cache_from_prescan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _snapshot_files_album_scan_cache_from_prescan(*args, **kwargs)


def _refresh_files_album_scan_cache_from_editions_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _refresh_files_album_scan_cache_from_editions(*args, **kwargs)

def _load_files_album_scan_cache_map_for_keys_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _load_files_album_scan_cache_map_for_keys(*args, **kwargs)


def _resume_files_plan_row_tuple_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _resume_files_plan_row_tuple(*args, **kwargs)

def _persist_resume_files_plan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _persist_resume_files_plan(*args, **kwargs)

def _upsert_resume_files_plan_partial_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _upsert_resume_files_plan_partial(*args, **kwargs)

def _prune_resume_files_plan_artist_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _prune_resume_files_plan_artist(*args, **kwargs)

def _prune_resume_files_plan_albums_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _prune_resume_files_plan_albums(*args, **kwargs)

def _ensure_resume_run_started_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ensure_resume_run_started(*args, **kwargs)

def _persist_resume_discovery_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _persist_resume_discovery_snapshot(*args, **kwargs)

def _persist_resume_discovery_progress_only_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _persist_resume_discovery_progress_only(*args, **kwargs)

def _load_resume_discovery_snapshot_by_run_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _load_resume_discovery_snapshot_by_run_id(*args, **kwargs)

def _snapshot_current_resume_discovery_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _snapshot_current_resume_discovery(*args, **kwargs)

def _snapshot_current_resume_state_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _snapshot_current_resume_state(*args, **kwargs)

def _restore_resume_files_plan_from_run_row_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _restore_resume_files_plan_from_run_row(*args, **kwargs)

def _load_resume_files_plan_by_run_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _load_resume_files_plan_by_run_id(*args, **kwargs)

def _load_resume_files_plan_partial_by_run_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _load_resume_files_plan_partial_by_run_id(*args, **kwargs)

def _load_resume_files_plan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _load_resume_files_plan(*args, **kwargs)

def _snapshot_current_resume_files_plan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _snapshot_current_resume_files_plan(*args, **kwargs)

def _hydrate_resume_files_edition_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _hydrate_resume_files_edition(*args, **kwargs)

def _prepare_resume_scan_artists_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _prepare_resume_scan_artists(*args, **kwargs)

def _has_unfinished_resume_run_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _has_unfinished_resume_run(*args, **kwargs)

def _get_resume_run_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _get_resume_run_snapshot(*args, **kwargs)

def _get_resume_run_snapshot_by_run_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _get_resume_run_snapshot_by_run_id(*args, **kwargs)

def _get_latest_resume_run_snapshot_any_signature_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _get_latest_resume_run_snapshot_any_signature(*args, **kwargs)

def _get_startup_resume_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _get_startup_resume_snapshot(*args, **kwargs)

def _maybe_resume_interrupted_scan_on_startup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _maybe_resume_interrupted_scan_on_startup(*args, **kwargs)

def _set_resume_artist_status_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _set_resume_artist_status(*args, **kwargs)

def _set_resume_run_status_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _set_resume_run_status(*args, **kwargs)

def _build_scan_plan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_scan_plan(*args, **kwargs)
