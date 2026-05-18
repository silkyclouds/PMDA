"""Runtime-owned scan persistence and pre-scan cache helpers.

This module owns duplicate review persistence, provider-no-tracklist rollups,
scan duplicate recovery from pipeline traces, and pre-scan album/dir cache
snapshots extracted from ``pmda.py``. It binds the live PMDA runtime at the
call boundary to preserve existing scan behavior while keeping write-heavy
logic outside the bootstrap module.
"""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None
_EXTRACTED_NAMES = {
    '_delete_duplicate_group_rows',
    'save_scan_artist_to_db',
    '_scan_provider_no_tracklist_rollup',
    'update_scan_history_incremental',
    'save_scan_to_db',
    '_load_duplicate_groups_from_pipeline_trace',
    '_relative_depth_under_root',
    '_compute_dir_scan_fingerprint',
    '_load_files_dir_scan_cache_map',
    '_upsert_files_dir_scan_cache_rows',
    '_build_files_cache_row_from_prescan_item',
    '_files_should_snapshot_prescan_cache_for_run',
    '_snapshot_files_dir_scan_cache_from_prescan',
    '_trigger_prescan_cache_snapshot_async',
}


def _bind_runtime(runtime: Any) -> None:
    global _RUNTIME
    _RUNTIME = runtime
    for name, value in vars(runtime).items():
        if name in _EXTRACTED_NAMES:
            continue
        if name == "_bind_runtime" or (name.endswith("_for_runtime") and name[: -len("_for_runtime")] in _EXTRACTED_NAMES):
            continue
        globals()[name] = value


def _runtime_module() -> Any:
    if _RUNTIME is None:
        raise RuntimeError("Scan persistence runtime is not bound")
    return _RUNTIME

def delete_duplicate_group_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _delete_duplicate_group_rows(*args, **kwargs)

def save_scan_artist_to_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return save_scan_artist_to_db(*args, **kwargs)

def scan_provider_no_tracklist_rollup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_provider_no_tracklist_rollup(*args, **kwargs)

def update_scan_history_incremental_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return update_scan_history_incremental(*args, **kwargs)

def save_scan_to_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return save_scan_to_db(*args, **kwargs)

def load_duplicate_groups_from_pipeline_trace_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _load_duplicate_groups_from_pipeline_trace(*args, **kwargs)

def relative_depth_under_root_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _relative_depth_under_root(*args, **kwargs)

def compute_dir_scan_fingerprint_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _compute_dir_scan_fingerprint(*args, **kwargs)

def load_files_dir_scan_cache_map_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _load_files_dir_scan_cache_map(*args, **kwargs)

def upsert_files_dir_scan_cache_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _upsert_files_dir_scan_cache_rows(*args, **kwargs)

def build_files_cache_row_from_prescan_item_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_files_cache_row_from_prescan_item(*args, **kwargs)

def files_should_snapshot_prescan_cache_for_run_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_should_snapshot_prescan_cache_for_run(*args, **kwargs)

def snapshot_files_dir_scan_cache_from_prescan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _snapshot_files_dir_scan_cache_from_prescan(*args, **kwargs)

def trigger_prescan_cache_snapshot_async_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _trigger_prescan_cache_snapshot_async(*args, **kwargs)


def _delete_duplicate_group_rows(
    cur: sqlite3.Cursor,
    *,
    artist: str,
    album_id: int | None = None,
    album_norm: str | None = None,
) -> None:
    """Delete one logical duplicate group before refreshing it, not the whole registry."""
    artist_key = str(artist or "").strip()
    if not artist_key:
        return
    clauses: list[str] = []
    params: list[Any] = [artist_key]
    parsed_album_id = _parse_int_loose(album_id, 0)
    if parsed_album_id > 0:
        clauses.append("album_id = ?")
        params.append(parsed_album_id)
    norm = str(album_norm or "").strip()
    if norm:
        clauses.append("COALESCE(album_norm, '') = ?")
        params.append(norm)
    if not clauses:
        return
    where_sql = " OR ".join(clauses)
    cur.execute(
        f"SELECT album_id FROM duplicates_best WHERE artist = ? AND ({where_sql})",
        tuple(params),
    )
    group_ids = sorted({_parse_int_loose(row[0], 0) for row in cur.fetchall() if _parse_int_loose(row[0], 0) > 0})
    for group_id in group_ids:
        cur.execute("DELETE FROM duplicates_loser WHERE artist = ? AND album_id = ?", (artist_key, group_id))
    cur.execute(
        f"DELETE FROM duplicates_best WHERE artist = ? AND ({where_sql})",
        tuple(params),
    )


def save_scan_artist_to_db(artist_name: str, groups: List[dict]) -> int:
    """
    Insert one artist's duplicate groups into duplicates_best and duplicates_loser.
    Skips groups without best/losers (e.g. needs_ai not yet processed). Returns count of groups saved.
    """
    def _write() -> int:
        con = _state_connect()
        cur = con.cursor()
        saved_count = 0
        try:
            for g in groups:
                if "best" not in g or "losers" not in g:
                    continue
                saved_count += 1
                best = g["best"]
                best_folder_path = path_for_fs_access(Path(str(best.get("storage_access_path") or best.get("folder")))) if best.get("folder") else None
                best_size_mb = (safe_folder_size(best_folder_path) // (1024 * 1024)) if best_folder_path else 0
                best_track_count = len(best.get("tracks", []))
                used_ai = bool(best.get("used_ai", False))
                ai_provider = best.get("ai_provider") or ""
                ai_model = best.get("ai_model") or ""
                if used_ai and (not ai_provider or not ai_model):
                    mod = sys.modules[__name__]
                    ai_provider = ai_provider or (getattr(mod, "AI_PROVIDER", None) or "")
                    ai_model = ai_model or (getattr(mod, "RESOLVED_MODEL", None) or getattr(mod, "OPENAI_MODEL", None) or "")
                try:
                    evidence_json = json.dumps(best.get("dupe_evidence", []))
                except Exception:
                    evidence_json = "[]"
                _delete_duplicate_group_rows(
                    cur,
                    artist=str(artist_name or ""),
                    album_id=_parse_int_loose(best.get("album_id"), 0),
                    album_norm=str(best.get("album_norm") or "").strip(),
                )
                cur.execute("""
                      INSERT OR REPLACE INTO duplicates_best
                        (artist, album_id, title_raw, album_norm, folder,
                         fmt_text, br, sr, bd, dur, discs, rationale, merge_list, ai_used, meta_json, ai_provider, ai_model, evidence_json, size_mb, track_count, match_verified_by_ai,
                         dupe_signal, no_move, manual_review, same_folder)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                  """, (
                      artist_name,
                      best["album_id"],
                      best["title_raw"],
                      best["album_norm"],
                      str(best["folder"]),
                      get_primary_format(best_folder_path or Path(best["folder"])),
                      best["br"],
                      best["sr"],
                      best["bd"],
                      best["dur"],
                      best["discs"],
                      best.get("rationale", ""),
                      json.dumps(best.get("merge_list", [])),
                      int(used_ai),
                      json.dumps(best.get("meta", {})),
                      ai_provider,
                      ai_model,
                      evidence_json,
                      best_size_mb,
                      best_track_count,
                      1 if best.get("match_verified_by_ai") else 0,
                      str(g.get("dupe_signal") or ""),
                      1 if bool(g.get("no_move")) else 0,
                      1 if bool(g.get("manual_review")) else 0,
                      1 if bool(g.get("same_folder")) else 0,
                  ))
                for e in g["losers"]:
                    size_mb = folder_size(e["folder"]) // (1024 * 1024)
                    cur.execute("""
                        INSERT INTO duplicates_loser
                          (artist, album_id, loser_album_id, folder, fmt_text, br, sr, bd, size_mb)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        artist_name,
                        best["album_id"],
                        e.get("album_id"),
                        str(e["folder"]),
                        get_primary_format(e["folder"]),
                        e["br"],
                        e["sr"],
                        e["bd"],
                        size_mb,
                    ))
            con.commit()
            return saved_count
        finally:
            con.close()

    return _state_db_write_retry(_write, label=f"save_scan_artist_to_db:{artist_name}", attempts=12)


def _scan_provider_no_tracklist_rollup(cur, scan_id: int) -> dict[str, Any]:
    providers = ("musicbrainz", "discogs", "lastfm", "bandcamp")
    details_by_provider: dict[str, dict[str, int]] = {
        key: {
            "total": 0,
            "api_or_parser": 0,
            "edition": 0,
            "absence_real": 0,
            "full_album": 0,
        }
        for key in (*providers, "multiple", "none")
    }
    out: dict[str, Any] = {
        "total": 0,
        "by_provider": {key: 0 for key in (*providers, "multiple", "none")},
        "by_cause": {
            "api_or_parser": 0,
            "edition": 0,
            "absence_real": 0,
        },
        "details_by_provider": details_by_provider,
    }
    try:
        cur.execute(
            """
            SELECT
                COALESCE(strict_match_provider, ''),
                COALESCE(metadata_source, ''),
                COALESCE(strict_reject_reason, ''),
                COALESCE(musicbrainz_release_id, ''),
                COALESCE(musicbrainz_id, ''),
                COALESCE(discogs_release_id, ''),
                COALESCE(lastfm_album_mbid, ''),
                COALESCE(bandcamp_album_url, '')
            FROM scan_editions
            WHERE scan_id = ?
              AND LOWER(COALESCE(strict_reject_reason, '')) LIKE 'provider_no_tracklist%%'
            """,
            (int(scan_id),),
        )
        rows = cur.fetchall() or []
    except Exception:
        logging.debug("Failed to build provider_no_tracklist rollup for scan_id=%s", scan_id, exc_info=True)
        return out

    for row in rows:
        strict_provider = _normalize_identity_provider(str(row[0] or ""))
        metadata_source = _normalize_identity_provider(str(row[1] or ""))
        reason = str(row[2] or "").strip().lower()
        provider_signals: list[str] = []

        def _add_signal(name: str) -> None:
            key = _normalize_identity_provider(name)
            if key in providers and key not in provider_signals:
                provider_signals.append(key)

        _add_signal(strict_provider)
        _add_signal(metadata_source)
        if str(row[3] or "").strip() or str(row[4] or "").strip():
            _add_signal("musicbrainz")
        if str(row[5] or "").strip():
            _add_signal("discogs")
        if str(row[6] or "").strip():
            _add_signal("lastfm")
        if str(row[7] or "").strip():
            _add_signal("bandcamp")

        if strict_provider in providers:
            provider_bucket = strict_provider
        elif metadata_source in providers:
            provider_bucket = metadata_source
        elif len(provider_signals) == 1:
            provider_bucket = provider_signals[0]
        elif len(provider_signals) > 1:
            provider_bucket = "multiple"
        else:
            provider_bucket = "none"

        if provider_bucket == "none":
            cause = "absence_real"
        elif reason == "provider_no_tracklist_full_album" or len(provider_signals) > 1:
            cause = "edition"
        else:
            cause = "api_or_parser"

        out["total"] += 1
        out["by_provider"][provider_bucket] += 1
        out["by_cause"][cause] += 1
        details_by_provider[provider_bucket]["total"] += 1
        details_by_provider[provider_bucket][cause] += 1
        if reason == "provider_no_tracklist_full_album":
            details_by_provider[provider_bucket]["full_album"] += 1

    return out


def update_scan_history_incremental(
    scan_id: int,
    artists_processed: int,
    duplicates_found: int,
    duplicate_groups_count: int,
    total_duplicates_count: int,
    broken_albums_count: int,
    missing_albums_count: int = 0,
    albums_without_artist_image: int = 0,
    albums_without_album_image: int = 0,
    albums_without_complete_tags: int = 0,
    albums_without_mb_id: int = 0,
    albums_without_artist_mb_id: int = 0,
) -> None:
    """
    Update the running scan_history row with current counters so UI can show partial progress.
    Only updates rows with status = 'running'.
    """
    try:
        def _write() -> None:
            con = _state_connect()
            try:
                cur = con.cursor()
                cur.execute(
                    """
                    UPDATE scan_history
                    SET artists_processed = ?,
                        duplicates_found = ?,
                        duplicate_groups_count = ?,
                        total_duplicates_count = ?,
                        broken_albums_count = ?,
                        missing_albums_count = ?,
                        albums_without_artist_image = ?,
                        albums_without_album_image = ?,
                        albums_without_complete_tags = ?,
                        albums_without_mb_id = ?,
                        albums_without_artist_mb_id = ?
                    WHERE scan_id = ? AND status = 'running'
                    """,
                    (
                        artists_processed,
                        duplicates_found,
                        duplicate_groups_count,
                        total_duplicates_count,
                        broken_albums_count,
                        missing_albums_count,
                        albums_without_artist_image,
                        albums_without_album_image,
                        albums_without_complete_tags,
                        albums_without_mb_id,
                        albums_without_artist_mb_id,
                        scan_id,
                    ),
                )
                con.commit()
            finally:
                con.close()

        _state_db_write_retry(_write, label=f"update_scan_history_incremental:{scan_id}", attempts=8)
    except Exception as e:
        logging.debug("update_scan_history_incremental failed: %s", e)


def save_scan_to_db(scan_results: Dict[str, List[dict]]):
    """
    Persist duplicate groups into the global open-review registry.

    Important: this is intentionally not a per-scan snapshot. PMDA must keep
    historical unresolved duplicate groups visible across scans until the user
    resolves them.
    """
    import sqlite3, json

    # (Removed: filtering of invalid editions; already purged upstream)
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()

    saved_count = 0
    skipped_count = 0
    saved_with_ai = 0
    for artist, groups in scan_results.items():
        for g in groups:
            if "best" not in g or "losers" not in g:
                skipped_count += 1
                logging.debug("save_scan_to_db: skipping group without best/losers (artist=%s)", artist)
                continue
            saved_count += 1
            best = g["best"]
            if best.get("used_ai"):
                saved_with_ai += 1
            # Persist size_mb and track_count so Unduper shows them after reload
            best_folder_path = path_for_fs_access(Path(best["folder"])) if best.get("folder") else None
            best_size_mb = (safe_folder_size(best_folder_path) // (1024 * 1024)) if best_folder_path else 0
            best_track_count = len(best.get("tracks", []))
            # When used_ai, ensure ai_provider/ai_model are set (e.g. from cache they may be empty)
            used_ai = bool(best.get("used_ai", False))
            ai_provider = best.get("ai_provider") or ""
            ai_model = best.get("ai_model") or ""
            if used_ai and (not ai_provider or not ai_model):
                mod = sys.modules[__name__]
                ai_provider = ai_provider or (getattr(mod, "AI_PROVIDER", None) or "")
                ai_model = ai_model or (getattr(mod, "RESOLVED_MODEL", None) or getattr(mod, "OPENAI_MODEL", None) or "")
            try:
                evidence_json = json.dumps(best.get("dupe_evidence", []))
            except Exception:
                evidence_json = "[]"
            _delete_duplicate_group_rows(
                cur,
                artist=str(artist or ""),
                album_id=_parse_int_loose(best.get("album_id"), 0),
                album_norm=str(best.get("album_norm") or "").strip(),
            )
            # Best edition
            cur.execute("""
                  INSERT OR REPLACE INTO duplicates_best
                    (artist, album_id, title_raw, album_norm, folder,
                     fmt_text, br, sr, bd, dur, discs, rationale, merge_list, ai_used, meta_json, ai_provider, ai_model, evidence_json, size_mb, track_count, match_verified_by_ai,
                     dupe_signal, no_move, manual_review, same_folder)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              """, (
                  artist,
                  best['album_id'],
                  best['title_raw'],
                  best['album_norm'],
                  str(best['folder']),
                  get_primary_format(Path(best['folder'])),
                  best['br'],
                  best['sr'],
                  best['bd'],
                  best['dur'],
                  best['discs'],
                  best.get('rationale', ''),
                  json.dumps(best.get('merge_list', [])),
                  int(used_ai),
                  json.dumps(best.get('meta', {})),
                  ai_provider,
                  ai_model,
                  evidence_json,
                  best_size_mb,
                  best_track_count,
                  1 if best.get('match_verified_by_ai') else 0,
                  str(g.get("dupe_signal") or ""),
                  1 if bool(g.get("no_move")) else 0,
                  1 if bool(g.get("manual_review")) else 0,
                  1 if bool(g.get("same_folder")) else 0,
              ))

            # All "loser" editions
            for e in g['losers']:
                size_mb = folder_size(e['folder']) // (1024 * 1024)
                cur.execute("""
                    INSERT INTO duplicates_loser
                      (artist, album_id, loser_album_id, folder, fmt_text, br, sr, bd, size_mb)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    artist,
                    best['album_id'],
                    e.get('album_id'),
                    str(e['folder']),
                    get_primary_format(e['folder']),
                    e['br'],
                    e['sr'],
                    e['bd'],
                    size_mb
                ))

    # 3) Commit & close
    con.commit()
    con.close()
    if saved_count or skipped_count:
        logging.info(
            "save_scan_to_db: saved %d group(s) (%d with AI), skipped %d without best/losers",
            saved_count, saved_with_ai, skipped_count,
        )
        # Expose duplicate decision stats for summary_json
        try:
            with lock:
                state["scan_duplicate_groups_saved"] = int(saved_count)
                state["scan_duplicate_groups_ai_saved"] = int(saved_with_ai)
                state["scan_duplicate_groups_skipped"] = int(skipped_count)
        except Exception:
            # Telemetry only; never break scan on state update failure
            pass


def _load_duplicate_groups_from_pipeline_trace(limit_groups: int = 5000) -> dict[str, list[dict]]:
    """Best-effort recovery of duplicate review groups from historical pipeline trace rows."""
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=10)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        if not _sqlite_table_exists(cur, "scan_pipeline_trace"):
            con.close()
            return {}
        cur.execute(
            """
            SELECT t.*
            FROM scan_pipeline_trace t
            JOIN (
                SELECT artist, album_id, MAX(rowid) AS rowid
                FROM scan_pipeline_trace
                WHERE LOWER(COALESCE(dupe_role, 'none')) IN ('winner', 'loser', 'candidate')
                GROUP BY artist, album_id
            ) latest ON latest.rowid = t.rowid
            WHERE LOWER(COALESCE(t.dupe_role, 'none')) IN ('winner', 'loser', 'candidate')
            ORDER BY COALESCE(t.updated_at, 0) DESC, t.artist COLLATE NOCASE ASC
            LIMIT ?
            """,
            (max(100, int(limit_groups or 5000)) * 4,),
        )
        rows = cur.fetchall()
        con.close()
    except Exception:
        logging.debug("Failed to load duplicate groups from scan_pipeline_trace", exc_info=True)
        return {}

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        artist = str(row["artist"] or "").strip()
        if not artist:
            continue
        album_id = _parse_int_loose(row["album_id"], 0)
        if album_id <= 0:
            continue
        title = str(row["album_title"] or row["winner_title"] or "").strip() or f"Album {album_id}"
        album_norm = norm_album_for_dedup(title, normalize_parenthetical=True) or title.lower()
        winner_album_id = _parse_int_loose(row["winner_album_id"], 0)
        winner_title = str(row["winner_title"] or "").strip()
        group_key = str(winner_album_id or "") if winner_album_id > 0 else (norm_album_for_dedup(winner_title or title, normalize_parenthetical=True) or album_norm)
        key = (artist, group_key)
        role = str(row["dupe_role"] or "candidate").strip().lower()
        folder = Path(str(row["folder"] or ""))
        entry = {
            "album_id": album_id,
            "title_raw": title,
            "album_norm": album_norm,
            "folder": folder,
            "fmt_text": str(row["fmt_text"] or ""),
            "fmt": str(row["fmt_text"] or ""),
            "br": 0,
            "sr": 0,
            "bd": 0,
            "dur": 0,
            "discs": 1,
            "tracks": [],
            "meta": {},
            "used_ai": bool(_parse_int_loose(row["ai_used"], 0)),
            "ai_provider": str(row["ai_provider"] or ""),
            "ai_model": str(row["ai_model"] or ""),
            "track_count": _parse_int_loose(row["actual_track_count"], 0),
            "size_mb": 0,
        }
        bucket = grouped.setdefault(
            key,
            {
                "artist": artist,
                "best": None,
                "losers": [],
                "candidates": [],
                "dupe_signal": str(row["dupe_signal"] or "pipeline_trace").strip() or "pipeline_trace",
                "no_move": bool(_parse_int_loose(row["no_move"], 0)),
                "manual_review": bool(_parse_int_loose(row["manual_review"], 0)),
                "same_folder": bool(_parse_int_loose(row["same_folder"], 0)),
            },
        )
        if role == "winner" or (winner_album_id > 0 and album_id == winner_album_id):
            bucket["best"] = entry
        elif role == "loser":
            bucket["losers"].append(entry)
        else:
            bucket["candidates"].append(entry)

    out: dict[str, list[dict]] = defaultdict(list)
    emitted = 0
    for (_artist, _group_key), bucket in grouped.items():
        if emitted >= max(100, int(limit_groups or 5000)):
            break
        best = bucket.get("best")
        candidates = list(bucket.get("candidates") or [])
        losers = list(bucket.get("losers") or [])
        if best is None and candidates:
            best = candidates.pop(0)
            losers.extend(candidates)
        if best is None or not losers:
            continue
        artist = str(bucket.get("artist") or "").strip()
        out[artist].append(
            {
                "artist": artist,
                "album_id": best.get("album_id"),
                "best": best,
                "losers": losers,
                "dupe_signal": str(bucket.get("dupe_signal") or "pipeline_trace"),
                "no_move": bool(bucket.get("no_move")),
                "manual_review": bool(bucket.get("manual_review")),
                "same_folder": bool(bucket.get("same_folder")),
            }
        )
        emitted += 1
    return dict(out)


def _relative_depth_under_root(path_like: Path | str | None, root_like: Path | str | None) -> int | None:
    try:
        path_obj = path_for_fs_access(Path(path_like)) if path_like else None
        root_obj = path_for_fs_access(Path(root_like)) if root_like else None
    except Exception:
        return None
    if path_obj is None or root_obj is None:
        return None
    try:
        rel = path_obj.resolve().relative_to(root_obj.resolve())
    except Exception:
        try:
            rel = path_obj.relative_to(root_obj)
        except Exception:
            return None
    parts = [part for part in rel.parts if part not in {"", "."}]
    return len(parts)


def _compute_dir_scan_fingerprint(dir_path: Path | str) -> tuple[str, int]:
    """
    Lightweight subtree gate based on immediate directory entries only.
    This is intentionally cheap and optimized for append-only bucket layouts
    such as Music_dump/month/day trees.
    """
    try:
        dir_obj = path_for_fs_access(Path(dir_path))
    except Exception:
        dir_obj = Path(dir_path)
    h = hashlib.blake2b(digest_size=20)
    count = 0
    items: list[tuple[str, bool, int, int]] = []
    try:
        with os.scandir(dir_obj) as it:
            for entry in it:
                try:
                    is_dir = bool(entry.is_dir(follow_symlinks=False))
                    is_file = bool(entry.is_file(follow_symlinks=False))
                    if not is_dir and not is_file:
                        continue
                    st = entry.stat(follow_symlinks=False)
                    size = int(st.st_size) if is_file else 0
                    mtime_ns = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))
                    items.append((entry.name, is_dir, size, mtime_ns))
                except (OSError, PermissionError):
                    continue
    except (FileNotFoundError, NotADirectoryError, PermissionError, OSError):
        return "", 0
    for name, is_dir, size, mtime_ns in sorted(items, key=lambda x: x[0].lower()):
        h.update(name.encode("utf-8", "replace"))
        h.update(b"|d|" if is_dir else b"|f|")
        h.update(str(size).encode("ascii"))
        h.update(b"|")
        h.update(str(mtime_ns).encode("ascii"))
        h.update(b"\n")
        count += 1
    return h.hexdigest(), count


def _load_files_dir_scan_cache_map() -> dict[str, dict]:
    out: dict[str, dict] = {}
    try:
        con = _state_connect_readonly(timeout=20)
        cur = con.cursor()
        cur.execute(
            """
            SELECT dir_path, COALESCE(source_id, 0) AS source_id, root_path, relative_depth,
                   fingerprint, subtree_audio_count, subtree_album_count, subtree_entry_estimate,
                   album_folders_json, updated_at
            FROM files_dir_scan_cache
            """
        )
        for row in cur.fetchall():
            dir_path = str(row["dir_path"] or "").strip()
            if not dir_path:
                continue
            try:
                album_folders = json.loads(row["album_folders_json"] or "[]")
                if not isinstance(album_folders, list):
                    album_folders = []
            except Exception:
                album_folders = []
            out[dir_path] = {
                "source_id": int(row["source_id"] or 0),
                "root_path": str(row["root_path"] or "").strip(),
                "relative_depth": int(row["relative_depth"] or 0),
                "fingerprint": str(row["fingerprint"] or "").strip(),
                "subtree_audio_count": int(row["subtree_audio_count"] or 0),
                "subtree_album_count": int(row["subtree_album_count"] or 0),
                "subtree_entry_estimate": int(row["subtree_entry_estimate"] or 0),
                "album_folders": [str(p) for p in album_folders if str(p or "").strip()],
                "updated_at": float(row["updated_at"] or 0.0),
            }
        con.close()
    except Exception:
        logging.debug("Failed to load files dir scan cache", exc_info=True)
    return out


def _upsert_files_dir_scan_cache_rows(rows: list[dict]) -> None:
    if not rows:
        return
    try:
        con = _state_connect(timeout=30)
        cur = con.cursor()
        cur.executemany(
            """
            INSERT INTO files_dir_scan_cache
            (dir_path, source_id, root_path, relative_depth, fingerprint,
             subtree_audio_count, subtree_album_count, subtree_entry_estimate,
             album_folders_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dir_path) DO UPDATE SET
              source_id=excluded.source_id,
              root_path=excluded.root_path,
              relative_depth=excluded.relative_depth,
              fingerprint=excluded.fingerprint,
              subtree_audio_count=excluded.subtree_audio_count,
              subtree_album_count=excluded.subtree_album_count,
              subtree_entry_estimate=excluded.subtree_entry_estimate,
              album_folders_json=excluded.album_folders_json,
              updated_at=excluded.updated_at
            """,
            [
                (
                    str(r.get("dir_path") or "").strip(),
                    int(r.get("source_id") or 0) if int(r.get("source_id") or 0) > 0 else None,
                    str(r.get("root_path") or "").strip() or None,
                    int(r.get("relative_depth") or 0),
                    str(r.get("fingerprint") or "").strip(),
                    int(r.get("subtree_audio_count") or 0),
                    int(r.get("subtree_album_count") or 0),
                    int(r.get("subtree_entry_estimate") or 0),
                    json.dumps([str(p) for p in (r.get("album_folders") or []) if str(p or "").strip()], ensure_ascii=False),
                    float(r.get("updated_at") or time.time()),
                )
                for r in rows
                if str(r.get("dir_path") or "").strip()
            ],
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to upsert files dir scan cache rows", exc_info=True)


def _build_files_cache_row_from_prescan_item(item: dict, *, scan_id: int | None = None, now_ts: float | None = None) -> dict | None:
    """Create one files_album_scan_cache row from a pre-scan edition payload (no filesystem re-read)."""
    folder_raw = item.get("folder")
    if not folder_raw:
        return None
    resume_stub = bool(item.get("_resume_stub"))
    folder_key_raw = str(folder_raw or "").strip()
    if resume_stub:
        folder_key = folder_key_raw
    elif str(item.get("storage_access_path") or "").strip():
        folder_key = _album_folder_cache_key(folder_key_raw)
    else:
        try:
            folder_path = path_for_fs_access(Path(folder_raw))
            folder_key = _album_folder_cache_key(folder_path)
        except Exception:
            folder_key = _album_folder_cache_key(folder_key_raw)
    if not folder_key:
        return None

    tags = dict(item.get("tags") or item.get("meta") or {})
    missing_required = item.get("missing_required_tags")
    if not isinstance(missing_required, list):
        if resume_stub:
            missing_required = []
        else:
            try:
                missing_required = _check_required_tags(tags, REQUIRED_TAGS, edition=item)
            except Exception:
                missing_required = []
    identity_fields = _extract_files_identity_fields(tags=tags, edition=item, cached=item)
    source_id = _parse_int_loose(item.get("source_id"), 0) or 0
    if source_id <= 0 and not resume_stub:
        try:
            source_id = int(_source_id_for_path(folder_key) or 0)
        except Exception:
            source_id = 0

    return {
        "folder_path": folder_key,
        "source_id": source_id if source_id > 0 else None,
        "fingerprint": str(item.get("fingerprint") or "").strip(),
        "ordered_paths": [
            str(p)
            for p in ((item.get("canonical_ordered_paths") or item.get("ordered_paths") or []))
            if str(p or "").strip()
        ],
        "artist_name": item.get("artist_name") or item.get("artist") or "",
        "album_title": item.get("album_title") or item.get("title_raw") or "",
        "has_cover": bool(item.get("has_cover")),
        "has_artist_image": bool(item.get("has_artist_image")),
        "has_complete_tags": len(missing_required) == 0,
        "has_mbid": bool(identity_fields["has_mbid"]),
        "has_identity": bool(identity_fields["has_identity"]),
        "identity_provider": identity_fields["identity_provider"],
        "strict_match_verified": bool(identity_fields.get("strict_match_verified")),
        "strict_match_provider": identity_fields.get("strict_match_provider") or "",
        "strict_reject_reason": identity_fields.get("strict_reject_reason") or "",
        "strict_tracklist_score": float(identity_fields.get("strict_tracklist_score") or 0.0),
        "musicbrainz_id": identity_fields["musicbrainz_id"],
        "musicbrainz_release_id": identity_fields.get("musicbrainz_release_id") or "",
        "discogs_release_id": identity_fields["discogs_release_id"],
        "lastfm_album_mbid": identity_fields["lastfm_album_mbid"],
        "bandcamp_album_url": identity_fields["bandcamp_album_url"],
        "metadata_source": identity_fields["metadata_source"],
        "missing_required_tags": missing_required,
        "last_scan_id": scan_id,
        "updated_at": float(now_ts or time.time()),
    }


def _files_should_snapshot_prescan_cache_for_run(
    *,
    requested_resume_run_id: str | None = None,
    current_resume_run_id: str | None = None,
) -> bool:
    requested = str(requested_resume_run_id or "").strip()
    current = str(current_resume_run_id or "").strip()
    return not bool(requested or current)


def _snapshot_files_dir_scan_cache_from_prescan(
    files_editions_by_album_id: dict[int, dict],
    *,
    roots: list[str] | None = None,
    reason: str = "prescan",
    batch_size: int = 500,
) -> dict:
    """
    Persist subtree cache rows so later scans can skip unchanged day/month buckets
    instead of re-walking every historical album folder.
    """
    started_at = time.time()
    if not files_editions_by_album_id:
        return {"ok": True, "rows_upserted": 0, "reason": reason, "duration_sec": 0.0}

    root_paths = [path_for_fs_access(Path(r)) for r in (roots or _effective_files_roots(enabled_only=True)) if r]
    if not root_paths:
        return {"ok": True, "rows_upserted": 0, "reason": reason, "duration_sec": 0.0}

    dir_to_album_folders: dict[str, set[str]] = defaultdict(set)
    dir_to_audio_count: dict[str, int] = defaultdict(int)
    dir_to_source_id: dict[str, int] = {}
    dir_to_root_path: dict[str, str] = {}
    dir_to_depth: dict[str, int] = {}

    for item in files_editions_by_album_id.values():
        folder_raw = item.get("folder")
        if not folder_raw:
            continue
        try:
            folder_path = path_for_fs_access(Path(folder_raw))
            folder_key = _album_folder_cache_key(folder_path)
        except Exception:
            continue
        if not folder_key:
            continue
        ordered_paths = [str(p) for p in (item.get("ordered_paths") or []) if str(p or "").strip()]
        source_row = _source_row_for_path(folder_path, enabled_only=True)
        source_id = _parse_int_loose(item.get("source_id"), 0) or int((source_row or {}).get("source_id") or 0)
        root_path = None
        root_raw = _normalize_root_path((source_row or {}).get("path"))
        if root_raw:
            root_path = path_for_fs_access(Path(root_raw))
        else:
            best_root = None
            best_len = -1
            for candidate_root in root_paths:
                candidate_str = str(candidate_root)
                if str(folder_path) == candidate_str or str(folder_path).startswith(candidate_str.rstrip("/") + "/"):
                    if len(candidate_str) > best_len:
                        best_root = candidate_root
                        best_len = len(candidate_str)
            root_path = best_root
        if root_path is None:
            continue
        current = folder_path
        while True:
            depth = _relative_depth_under_root(current, root_path)
            if depth is None or depth < PMDA_FILES_DIR_CACHE_MIN_SKIP_DEPTH:
                break
            dir_key = _album_folder_cache_key(current)
            dir_to_album_folders[dir_key].add(folder_key)
            dir_to_audio_count[dir_key] += len(ordered_paths)
            if source_id > 0:
                dir_to_source_id[dir_key] = source_id
            dir_to_root_path[dir_key] = str(root_path)
            dir_to_depth[dir_key] = int(depth)
            parent = current.parent
            if parent == current:
                break
            current = parent

    if not dir_to_album_folders:
        return {"ok": True, "rows_upserted": 0, "reason": reason, "duration_sec": round(time.time() - started_at, 2)}

    rows_buffer: list[dict] = []
    rows_upserted = 0
    now_ts = time.time()
    for dir_key, folder_keys in sorted(dir_to_album_folders.items()):
        dir_path = path_for_fs_access(Path(dir_key))
        if not dir_path.exists() or not dir_path.is_dir():
            continue
        fingerprint, _entry_count = _compute_dir_scan_fingerprint(dir_path)
        if not fingerprint:
            continue
        folder_list = sorted(folder_keys)
        if len(folder_list) < PMDA_FILES_DIR_CACHE_MIN_ALBUMS:
            continue
        rows_buffer.append(
            {
                "dir_path": dir_key,
                "source_id": dir_to_source_id.get(dir_key) or None,
                "root_path": dir_to_root_path.get(dir_key) or "",
                "relative_depth": int(dir_to_depth.get(dir_key) or 0),
                "fingerprint": fingerprint,
                "subtree_audio_count": int(dir_to_audio_count.get(dir_key) or 0),
                "subtree_album_count": len(folder_list),
                "subtree_entry_estimate": int(dir_to_audio_count.get(dir_key) or 0) + len(folder_list),
                "album_folders": folder_list,
                "updated_at": now_ts,
            }
        )
        if len(rows_buffer) >= max(100, int(batch_size or 500)):
            _upsert_files_dir_scan_cache_rows(rows_buffer)
            rows_upserted += len(rows_buffer)
            rows_buffer = []
    if rows_buffer:
        _upsert_files_dir_scan_cache_rows(rows_buffer)
        rows_upserted += len(rows_buffer)

    elapsed = round(time.time() - started_at, 2)
    logging.info(
        "FILES dir-cache snapshot (%s): upserted %d row(s) from %d cached subtree(s) in %.2fs",
        reason,
        rows_upserted,
        len(dir_to_album_folders),
        elapsed,
    )
    return {"ok": True, "rows_upserted": rows_upserted, "reason": reason, "duration_sec": elapsed}


def _trigger_prescan_cache_snapshot_async(*, reason: str = "prescan", scan_id: int | None = None) -> bool:
    """Start one async snapshot job from current files_editions_by_album_id (if available)."""
    if files_cache_snapshot_lock.locked():
        return False
    with lock:
        if state.get("scan_prescan_cache_snapshot_running"):
            return False
        files_map = state.get("files_editions_by_album_id") or {}
        if not isinstance(files_map, dict) or not files_map:
            return False
        state["scan_prescan_cache_snapshot_running"] = True
        state["scan_prescan_cache_snapshot_done"] = False
        state["scan_prescan_cache_snapshot_rows"] = 0
        state["scan_prescan_cache_snapshot_total"] = int(len(files_map))
        state["scan_prescan_cache_snapshot_updated_at"] = time.time()

    def _runner():
        rows = 0
        try:
            with files_cache_snapshot_lock:
                with lock:
                    files_map_live = state.get("files_editions_by_album_id") or {}
                    if not isinstance(files_map_live, dict):
                        files_map_live = {}
                    active_roots_live = list(_effective_files_roots(enabled_only=True))
                    state["scan_prescan_cache_snapshot_total"] = int(len(files_map_live))
                result = _snapshot_files_album_scan_cache_from_prescan(
                    files_map_live,
                    scan_id=scan_id,
                    reason=reason,
                    batch_size=2500,
                    pause_event=scan_is_paused,
                    respect_pause=False,
                )
                rows = int(result.get("rows_upserted") or 0)
                dir_result = _snapshot_files_dir_scan_cache_from_prescan(
                    files_map_live,
                    roots=active_roots_live,
                    reason=reason,
                    batch_size=750,
                )
                rows += int(dir_result.get("rows_upserted") or 0)
        except Exception:
            logging.exception("FILES cache snapshot (%s) failed", reason)
        finally:
            with lock:
                state["scan_prescan_cache_snapshot_running"] = False
                state["scan_prescan_cache_snapshot_done"] = True
                state["scan_prescan_cache_snapshot_rows"] = int(rows)
                if not int(state.get("scan_prescan_cache_snapshot_total") or 0):
                    state["scan_prescan_cache_snapshot_total"] = int(rows)
                state["scan_prescan_cache_snapshot_updated_at"] = time.time()

    threading.Thread(target=_runner, name=f"files-cache-snapshot-{reason}", daemon=True).start()
    return True
