"""Runtime-bound cache telemetry helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_read_media_cache_usage',
    '_read_sqlite_cache_metrics',
    '_read_state_cache_metrics',
    '_read_settings_db_metrics',
    '_read_redis_cache_metrics',
    '_read_pg_cache_metrics',
    '_collect_cache_control_metrics',
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

def _read_media_cache_usage(max_files: int | None = None) -> dict:
    root = _media_cache_root_dir()
    album_root = root / "album"
    artist_root = root / "artist"
    walk_limit = int(max_files if max_files is not None else PMDA_CACHE_TELEMETRY_MAX_WALK_FILES)
    walk_limit = max(100, walk_limit)
    total = _scan_dir_usage(root, max_files=walk_limit)
    album = _scan_dir_usage(album_root, max_files=walk_limit)
    artist = _scan_dir_usage(artist_root, max_files=walk_limit)
    with _ARTWORK_RAM_CACHE_LOCK:
        ram_entries = int(len(_ARTWORK_RAM_CACHE))
        ram_bytes = int(_ARTWORK_RAM_CACHE_BYTES)
    return {
        "root": str(root),
        "total": total,
        "album": album,
        "artist": artist,
        "ram_cache": {
            "enabled": bool(_ARTWORK_RAM_CACHE_MAX_BYTES > 0),
            "entries": ram_entries,
            "used_bytes": ram_bytes,
            "max_bytes": int(_ARTWORK_RAM_CACHE_MAX_BYTES),
            "item_max_bytes": int(_ARTWORK_RAM_CACHE_MAX_ITEM_BYTES),
            "ttl_sec": int(ARTWORK_RAM_CACHE_TTL_SEC),
        },
    }

def _read_sqlite_cache_metrics() -> dict:
    cache_db_wal = Path(str(CACHE_DB_FILE) + "-wal")
    cache_db_shm = Path(str(CACHE_DB_FILE) + "-shm")
    out = {
        "db_path": str(CACHE_DB_FILE),
        "db_bytes": _path_size_bytes(CACHE_DB_FILE),
        "wal_bytes": _path_size_bytes(cache_db_wal),
        "shm_bytes": _path_size_bytes(cache_db_shm),
        "audio_cache_rows": 0,
        "musicbrainz_cache_rows": 0,
        "musicbrainz_album_lookup_rows": 0,
        "musicbrainz_album_lookup_not_found_rows": 0,
        "provider_album_lookup_rows": 0,
        "provider_album_lookup_not_found_rows": 0,
    }
    if not CACHE_DB_FILE.exists():
        return out
    try:
        con = sqlite3.connect(str(CACHE_DB_FILE), timeout=5)
        cur = con.cursor()
        if _sqlite_table_exists(cur, "audio_cache"):
            out["audio_cache_rows"] = _sqlite_scalar(cur, "SELECT COUNT(*) FROM audio_cache")
        if _sqlite_table_exists(cur, "musicbrainz_cache"):
            out["musicbrainz_cache_rows"] = _sqlite_scalar(cur, "SELECT COUNT(*) FROM musicbrainz_cache")
        if _sqlite_table_exists(cur, "musicbrainz_album_lookup"):
            out["musicbrainz_album_lookup_rows"] = _sqlite_scalar(cur, "SELECT COUNT(*) FROM musicbrainz_album_lookup")
            out["musicbrainz_album_lookup_not_found_rows"] = _sqlite_scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM musicbrainz_album_lookup
                WHERE mbid IS NULL OR TRIM(COALESCE(mbid, '')) = ''
                """,
            )
        if _sqlite_table_exists(cur, "provider_album_lookup"):
            out["provider_album_lookup_rows"] = _sqlite_scalar(cur, "SELECT COUNT(*) FROM provider_album_lookup")
            out["provider_album_lookup_not_found_rows"] = _sqlite_scalar(
                cur,
                "SELECT COUNT(*) FROM provider_album_lookup WHERE status = 'not_found'",
            )
        con.close()
    except Exception:
        logging.debug("Failed reading cache.db telemetry", exc_info=True)
    return out

def _read_state_cache_metrics() -> dict:
    state_db_wal = Path(str(STATE_DB_FILE) + "-wal")
    state_db_shm = Path(str(STATE_DB_FILE) + "-shm")
    out = {
        "db_path": str(STATE_DB_FILE),
        "db_bytes": _path_size_bytes(STATE_DB_FILE),
        "wal_bytes": _path_size_bytes(state_db_wal),
        "shm_bytes": _path_size_bytes(state_db_shm),
        "files_album_scan_cache_rows": 0,
        "files_album_scan_cache_healthy_rows": 0,
        "files_pending_changes_rows": 0,
        "files_library_published_rows": 0,
        "scan_resume_pending_artists_rows": 0,
    }
    if not STATE_DB_FILE.exists():
        return out
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
        cur = con.cursor()
        if _sqlite_table_exists(cur, "files_album_scan_cache"):
            out["files_album_scan_cache_rows"] = _sqlite_scalar(cur, "SELECT COUNT(*) FROM files_album_scan_cache")
            out["files_album_scan_cache_healthy_rows"] = _sqlite_scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM files_album_scan_cache
                WHERE has_cover = 1
                  AND has_artist_image = 1
                  AND has_complete_tags = 1
                  AND has_identity = 1
                  AND (
                        missing_required_tags IS NULL
                        OR TRIM(missing_required_tags) = ''
                        OR TRIM(missing_required_tags) = '[]'
                  )
                """,
            )
        if _sqlite_table_exists(cur, "files_pending_changes"):
            out["files_pending_changes_rows"] = _sqlite_scalar(cur, "SELECT COUNT(*) FROM files_pending_changes")
        if _sqlite_table_exists(cur, "files_library_published_albums"):
            out["files_library_published_rows"] = _sqlite_scalar(cur, "SELECT COUNT(*) FROM files_library_published_albums")
        if _sqlite_table_exists(cur, "scan_resume_artists"):
            out["scan_resume_pending_artists_rows"] = _sqlite_scalar(
                cur,
                "SELECT COUNT(*) FROM scan_resume_artists WHERE status IN ('pending', 'running', 'failed')",
            )
        con.close()
    except Exception:
        logging.debug("Failed reading state.db telemetry", exc_info=True)
    return out

def _read_settings_db_metrics() -> dict:
    settings_db_wal = Path(str(SETTINGS_DB_FILE) + "-wal")
    settings_db_shm = Path(str(SETTINGS_DB_FILE) + "-shm")
    out = {
        "db_path": str(SETTINGS_DB_FILE),
        "db_bytes": _path_size_bytes(SETTINGS_DB_FILE),
        "wal_bytes": _path_size_bytes(settings_db_wal),
        "shm_bytes": _path_size_bytes(settings_db_shm),
        "rows": 0,
    }
    if not SETTINGS_DB_FILE.exists():
        return out
    try:
        con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
        cur = con.cursor()
        if _sqlite_table_exists(cur, "settings"):
            out["rows"] = _sqlite_scalar(cur, "SELECT COUNT(*) FROM settings")
        con.close()
    except Exception:
        logging.debug("Failed reading settings.db telemetry", exc_info=True)
    return out

def _read_redis_cache_metrics() -> dict:
    out = {
        "available": False,
        "host": PMDA_REDIS_HOST,
        "port": PMDA_REDIS_PORT,
        "db": PMDA_REDIS_DB,
        "mode": "local" if bool(_FILES_CACHE_LOCAL_ENABLED) else "none",
        "reason": "",
        "last_error": str(_FILES_REDIS_LAST_ERROR or ""),
        "last_ok_ts": float(_FILES_REDIS_LAST_OK_TS or 0.0),
        "local_cache_enabled": bool(_FILES_CACHE_LOCAL_ENABLED),
        "local_cache_keys": 0,
        "db_keys": 0,
        "pmda_prefix_keys": 0,
        "pmda_prefix_scan_truncated": False,
        "used_memory_bytes": 0,
        "used_memory_peak_bytes": 0,
        "maxmemory_bytes": 0,
        "evicted_keys": 0,
        "keyspace_hits": 0,
        "keyspace_misses": 0,
        "keyspace_hit_rate_pct": None,
        "ops_per_sec": 0,
        "connected_clients": 0,
        "redis_process_id": 0,
        "redis_process_cpu_pct": None,
        "idle_cpu_drift": {
            "suspected": False,
            "streak": 0,
            "last_warn_ts": 0.0,
            "reason": "",
        },
    }
    try:
        with _FILES_LOCAL_CACHE_LOCK:
            out["local_cache_keys"] = int(len(_FILES_LOCAL_CACHE))
    except Exception:
        out["local_cache_keys"] = 0
    cli = _files_redis_client()
    if cli is None:
        if not bool(_FILES_CACHE_LOCAL_ENABLED):
            out["mode"] = "none"
            out["reason"] = "redis_unavailable_no_local_cache"
        elif redis_lib is None:
            out["mode"] = "local"
            out["reason"] = "redis_library_not_installed"
        elif str(PMDA_REDIS_HOST or "").strip() in {"", "127.0.0.1", "localhost"}:
            out["mode"] = "local"
            out["reason"] = "redis_unavailable_fallback_local_cache"
        else:
            out["mode"] = "local"
            out["reason"] = "redis_connection_failed_fallback_local_cache"
        return out
    out["available"] = True
    out["mode"] = "redis"
    out["reason"] = ""
    try:
        out["db_keys"] = _safe_int(cli.dbsize(), 0)
    except Exception:
        pass
    try:
        info_memory = cli.info("memory") or {}
        out["used_memory_bytes"] = _safe_int(info_memory.get("used_memory"), 0)
        out["used_memory_peak_bytes"] = _safe_int(info_memory.get("used_memory_peak"), 0)
        out["maxmemory_bytes"] = _safe_int(info_memory.get("maxmemory"), 0)
    except Exception:
        pass
    try:
        info_stats = cli.info("stats") or {}
        hits = _safe_int(info_stats.get("keyspace_hits"), 0)
        misses = _safe_int(info_stats.get("keyspace_misses"), 0)
        out["ops_per_sec"] = _safe_int(info_stats.get("instantaneous_ops_per_sec"), 0)
        out["keyspace_hits"] = hits
        out["keyspace_misses"] = misses
        out["evicted_keys"] = _safe_int(info_stats.get("evicted_keys"), 0)
        total = hits + misses
        if total > 0:
            out["keyspace_hit_rate_pct"] = round((hits / total) * 100.0, 2)
    except Exception:
        pass
    try:
        info_clients = cli.info("clients") or {}
        out["connected_clients"] = _safe_int(info_clients.get("connected_clients"), 0)
    except Exception:
        pass
    redis_pid = 0
    try:
        info_server = cli.info("server") or {}
        redis_pid = _safe_int(info_server.get("process_id"), 0)
        out["redis_process_id"] = int(redis_pid)
    except Exception:
        redis_pid = 0
    if redis_pid > 0:
        cpu_pct = _sample_process_cpu_pct(
            f"redis:{PMDA_REDIS_HOST}:{PMDA_REDIS_PORT}:{PMDA_REDIS_DB}",
            int(redis_pid),
        )
        if cpu_pct is not None:
            out["redis_process_cpu_pct"] = float(cpu_pct)
    try:
        max_scan = max(1000, int(PMDA_CACHE_TELEMETRY_MAX_REDIS_SCAN_KEYS or 200000))
        k = 0
        truncated = False
        for _ in cli.scan_iter(f"{FILES_CACHE_PREFIX}*"):
            k += 1
            if k >= max_scan:
                truncated = True
                break
        out["pmda_prefix_keys"] = int(k)
        out["pmda_prefix_scan_truncated"] = bool(truncated)
    except Exception:
        pass
    try:
        cpu_pct = float(out.get("redis_process_cpu_pct") or 0.0)
        ops_per_sec = int(out.get("ops_per_sec") or 0)
        db_keys = int(out.get("db_keys") or 0)
        drift_reason = (
            f"cpu={cpu_pct:.1f}% ops={ops_per_sec}/s db_keys={db_keys}"
            if cpu_pct > 0
            else ""
        )
        global _REDIS_IDLE_DRIFT_STREAK, _REDIS_IDLE_LAST_WARN_TS
        if (
            cpu_pct >= float(PMDA_REDIS_IDLE_CPU_WARN_PCT)
            and ops_per_sec <= int(PMDA_REDIS_IDLE_OPS_MAX)
            and db_keys <= int(PMDA_REDIS_IDLE_KEYS_MAX)
        ):
            _REDIS_IDLE_DRIFT_STREAK = int(_REDIS_IDLE_DRIFT_STREAK) + 1
        else:
            _REDIS_IDLE_DRIFT_STREAK = 0
            drift_reason = ""
        now_ts = time.time()
        suspected = int(_REDIS_IDLE_DRIFT_STREAK) >= int(PMDA_REDIS_IDLE_CONSECUTIVE)
        out["idle_cpu_drift"] = {
            "suspected": bool(suspected),
            "streak": int(_REDIS_IDLE_DRIFT_STREAK),
            "last_warn_ts": float(_REDIS_IDLE_LAST_WARN_TS or 0.0),
            "reason": str(drift_reason or ""),
        }
        if suspected and (now_ts - float(_REDIS_IDLE_LAST_WARN_TS or 0.0)) >= float(PMDA_REDIS_IDLE_WARN_COOLDOWN_SEC):
            _REDIS_IDLE_LAST_WARN_TS = float(now_ts)
            out["idle_cpu_drift"]["last_warn_ts"] = float(_REDIS_IDLE_LAST_WARN_TS)
            logging.warning(
                "[Redis] suspicious idle CPU drift detected (%s) host=%s:%s db=%s maxmemory=%sMB",
                drift_reason or "n/a",
                PMDA_REDIS_HOST,
                PMDA_REDIS_PORT,
                PMDA_REDIS_DB,
                int((out.get("maxmemory_bytes") or 0) / (1024 * 1024)),
            )
    except Exception:
        pass
    return out

def _read_pg_cache_metrics() -> dict:
    out = {
        "available": False,
        "mode": "none",
        "reason": "",
        "last_error": str(_FILES_PG_LAST_ERROR or ""),
        "last_ok_ts": float(_FILES_PG_LAST_OK_TS or 0.0),
        "db_size_bytes": 0,
        "db_cache_hit_rate_pct": None,
        "numbackends": 0,
        "table_estimated_rows": {},
        "table_total_bytes": {},
    }
    if _get_library_mode() != "files":
        out["mode"] = "disabled"
        out["reason"] = "library_mode_not_files"
        return out
    if psycopg is None:
        out["mode"] = "none"
        out["reason"] = "psycopg_not_installed"
        return out
    if not _files_pg_init_schema():
        out["mode"] = "none"
        out["reason"] = str(_FILES_PG_LAST_ERROR or "pg_init_schema_failed")
        return out
    conn = _files_pg_connect()
    if conn is None:
        out["mode"] = "none"
        out["reason"] = str(_FILES_PG_LAST_ERROR or "pg_connection_failed")
        return out
    out["available"] = True
    out["mode"] = "postgres"
    out["reason"] = ""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_database_size(current_database())")
            out["db_size_bytes"] = _safe_int((cur.fetchone() or [0])[0], 0)
            cur.execute(
                """
                SELECT numbackends, blks_hit, blks_read
                FROM pg_stat_database
                WHERE datname = current_database()
                """
            )
            row = cur.fetchone()
            if row:
                numbackends = _safe_int(row[0], 0)
                blks_hit = _safe_int(row[1], 0)
                blks_read = _safe_int(row[2], 0)
                out["numbackends"] = numbackends
                total_blks = blks_hit + blks_read
                if total_blks > 0:
                    out["db_cache_hit_rate_pct"] = round((blks_hit / total_blks) * 100.0, 2)
            table_names = [
                "files_artists",
                "files_albums",
                "files_tracks",
                "files_track_embeddings",
                "files_reco_events",
                "files_reco_sessions",
            ]
            cur.execute(
                """
                SELECT c.relname,
                       GREATEST(COALESCE(c.reltuples, 0), 0)::BIGINT AS est_rows,
                       pg_total_relation_size(c.oid) AS total_bytes
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname = ANY(%s)
                """,
                (table_names,),
            )
            row_map = {}
            size_map = {}
            for relname, est_rows, total_bytes in cur.fetchall():
                row_map[str(relname)] = _safe_int(est_rows, 0)
                size_map[str(relname)] = _safe_int(total_bytes, 0)
            out["table_estimated_rows"] = row_map
            out["table_total_bytes"] = size_map
    except Exception:
        logging.debug("Failed reading PostgreSQL telemetry", exc_info=True)
    finally:
        conn.close()
    return out

def _collect_cache_control_metrics(force: bool = False) -> dict:
    now = time.time()
    ttl = max(2.0, float(PMDA_CACHE_TELEMETRY_TTL_SEC or 15.0))
    with _CACHE_TELEMETRY_LOCK:
        cached_payload = _CACHE_TELEMETRY_SNAPSHOT.get("payload")
        cached_ts = float(_CACHE_TELEMETRY_SNAPSHOT.get("ts") or 0.0)
        if not force and cached_payload is not None and (now - cached_ts) < ttl:
            return dict(cached_payload)

    with lock:
        files_watcher_state = dict(state.get("files_watcher") or {})
        scan_audio_hits = _safe_int(state.get("scan_audio_cache_hits"), 0)
        scan_audio_misses = _safe_int(state.get("scan_audio_cache_misses"), 0)
        scan_mb_hits = _safe_int(state.get("scan_mb_cache_hits"), 0)
        scan_mb_misses = _safe_int(state.get("scan_mb_cache_misses"), 0)
        files_cache_quality_recalc_state = {
            "running": bool(state.get("files_cache_quality_recalc_running")),
            "total": _safe_int(state.get("files_cache_quality_recalc_total"), 0),
            "done": _safe_int(state.get("files_cache_quality_recalc_done"), 0),
            "rows_upserted": _safe_int(state.get("files_cache_quality_recalc_rows_upserted"), 0),
            "errors": _safe_int(state.get("files_cache_quality_recalc_errors"), 0),
            "missing_folders": _safe_int(state.get("files_cache_quality_recalc_missing_folders"), 0),
            "no_audio": _safe_int(state.get("files_cache_quality_recalc_no_audio"), 0),
            "reason": str(state.get("files_cache_quality_recalc_reason") or ""),
            "started_at": state.get("files_cache_quality_recalc_started_at"),
            "updated_at": state.get("files_cache_quality_recalc_updated_at"),
            "finished_at": state.get("files_cache_quality_recalc_finished_at"),
        }

    payload = {
        "generated_at": int(now),
        "cache_policies": {
            "scan_disable_cache": bool(SCAN_DISABLE_CACHE),
            "mb_disable_cache": bool(MB_DISABLE_CACHE),
        },
        "runtime": {
            "library_mode": _get_library_mode(),
            "process_rss_bytes": _current_process_rss_bytes(),
            "container_memory": _read_container_memory_stats(),
        },
        "redis": _read_redis_cache_metrics(),
        "postgres": _read_pg_cache_metrics(),
        "sqlite_cache_db": _read_sqlite_cache_metrics(),
        "sqlite_state_db": _read_state_cache_metrics(),
        "files_cache_quality_recalc": files_cache_quality_recalc_state,
        "media_cache": _read_media_cache_usage(),
        "scan_cache_counters_live": {
            "audio_hits": scan_audio_hits,
            "audio_misses": scan_audio_misses,
            "mb_hits": scan_mb_hits,
            "mb_misses": scan_mb_misses,
        },
        "files_watcher": {
            "running": bool(files_watcher_state.get("running")),
            "roots": list(files_watcher_state.get("roots") or []),
            "dirty_count": _safe_int(files_watcher_state.get("dirty_count"), 0),
            "dirty_count_by_root": dict(files_watcher_state.get("dirty_count_by_root") or {}),
            "last_event_at": files_watcher_state.get("last_event_at"),
            "last_event_path": files_watcher_state.get("last_event_path"),
            "enabled": bool(PMDA_FILES_WATCHER_ENABLED),
            "available": bool(_files_watcher_available()),
            "reason": str(files_watcher_state.get("reason") or ""),
            "degraded_mode": bool(
                bool(PMDA_FILES_WATCHER_ENABLED)
                and (
                    not bool(_files_watcher_available())
                    or not bool(files_watcher_state.get("running"))
                )
            ),
            "restart_in_progress": bool(files_watcher_state.get("restart_in_progress")),
            "last_restart_started_at": files_watcher_state.get("last_restart_started_at"),
            "last_restart_ended_at": files_watcher_state.get("last_restart_ended_at"),
            "last_restart_duration_ms": _int_or_none(files_watcher_state.get("last_restart_duration_ms")),
            "consecutive_failures": _safe_int(files_watcher_state.get("consecutive_failures"), 0),
            "last_error": str(files_watcher_state.get("last_error") or ""),
        },
    }

    with _CACHE_TELEMETRY_LOCK:
        _CACHE_TELEMETRY_SNAPSHOT["ts"] = now
        _CACHE_TELEMETRY_SNAPSHOT["payload"] = dict(payload)
    return payload

def _read_media_cache_usage_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _read_media_cache_usage(*args, **kwargs)

def _read_sqlite_cache_metrics_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _read_sqlite_cache_metrics(*args, **kwargs)

def _read_state_cache_metrics_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _read_state_cache_metrics(*args, **kwargs)

def _read_settings_db_metrics_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _read_settings_db_metrics(*args, **kwargs)

def _read_redis_cache_metrics_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _read_redis_cache_metrics(*args, **kwargs)

def _read_pg_cache_metrics_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _read_pg_cache_metrics(*args, **kwargs)

def _collect_cache_control_metrics_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _collect_cache_control_metrics(*args, **kwargs)

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}
