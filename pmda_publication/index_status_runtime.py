"""Files library index readiness helpers extracted from the PMDA bootstrap."""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind live PMDA globals for files-index readiness checks."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "_ensure_files_index_ready_for_runtime",
        "_ensure_files_index_ready_impl",
        "_bind_runtime",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def _ensure_files_index_ready_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_ensure_files_index_ready`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _ensure_files_index_ready_impl(*args, **kwargs)


def _ensure_files_index_ready_impl() -> tuple[bool, str | None]:
    if _get_library_mode() != "files":
        return False, "LIBRARY_MODE is not 'files'"
    if not FILES_ROOTS:
        return False, "FILES_ROOTS is empty"
    if not _files_pg_init_schema():
        return False, "PostgreSQL is unavailable"
    with lock:
        files_scan_running = bool(state.get("scanning")) and _get_library_mode() == "files"
    artists, albums, tracks = _files_index_read_counts()
    if files_scan_running and (albums == 0 or tracks == 0):
        # During a running Files scan, the live index may be intentionally empty
        # before first artist publication. Do not force a full bootstrap rebuild.
        return True, None
    if albums > 0 and tracks > 0:
        full_source_before_sync = _files_index_read_meta_value("full_source") or _files_index_read_meta_value("source")
        _files_index_sync_live_counts_meta(
            reason="startup_live_counts_reconcile",
            source="existing_pg_index",
            counts=(artists, albums, tracks),
        )
        published_rows = _files_library_published_row_count()
        if published_rows > albums + max(100, int(max(albums, 1) * 0.02)):
            if not files_index_lock.locked():
                logging.info(
                    "Files PG index is behind published scan rows (%d indexed albums / %d published rows). Scheduling published-row rebuild.",
                    albums,
                    published_rows,
                )
                _trigger_files_index_rebuild_async(reason="startup_published_rows_catchup")
            return True, None
        needs_mirror_rebuild, mirror_source = _files_index_needs_mirror_filesystem_rebuild(full_source_before_sync)
        if needs_mirror_rebuild:
            force_startup_mirror_rebuild = _parse_bool(
                os.getenv("PMDA_FORCE_MIRROR_TRUSTED_STARTUP_REBUILD", "false")
            )
            if force_startup_mirror_rebuild and not files_index_lock.locked():
                logging.info(
                    "Files PG index last full source is %s in mirror workflow. Scheduling authoritative filesystem rebuild from trusted library roots.",
                    mirror_source or "unknown",
                )
                _trigger_files_index_rebuild_async(reason="startup_published_rows_catchup")
            else:
                try:
                    mod = sys.modules[__name__]
                    now = time.time()
                    last_notice_at = float(getattr(mod, "_FILES_MIRROR_EXISTING_INDEX_NOTICE_AT", 0.0) or 0.0)
                    if now - last_notice_at >= 600:
                        setattr(mod, "_FILES_MIRROR_EXISTING_INDEX_NOTICE_AT", now)
                        logging.info(
                            "Files PG index last full source is %s in mirror workflow. Keeping existing index at startup; run a manual filesystem rebuild if root-level reconciliation is required.",
                            mirror_source or "unknown",
                        )
                except Exception:
                    pass
            return True, None
        try:
            mod = sys.modules[__name__]
            if not bool(getattr(mod, "_FILES_MATCH_FLAG_MIGRATED", False)):
                changed = _files_backfill_trusted_match_flags()
                setattr(mod, "_FILES_MATCH_FLAG_MIGRATED", True)
                if changed > 0:
                    _files_cache_invalidate_all()
        except Exception:
            pass
        try:
            mod = sys.modules[__name__]
            if not bool(getattr(mod, "_FILES_BROWSE_ENTITY_BACKFILLED", False)):
                backfill = _files_backfill_artist_browse_entities_from_existing_index()
                if int(backfill.get("links") or 0) > 0:
                    artists, albums, tracks = _files_index_read_counts()
                setattr(mod, "_FILES_BROWSE_ENTITY_BACKFILLED", True)
        except Exception:
            pass
        track_count, embedding_count = _files_index_read_track_and_embedding_counts()
        min_expected = max(1, int(track_count * 0.85)) if track_count > 0 else 0
        if track_count > 0 and embedding_count < min_expected:
            if not files_index_lock.locked():
                logging.info(
                    "Files reco embeddings below threshold (%d/%d). Scheduling async embeddings backfill...",
                    embedding_count,
                    track_count,
                )
                _enqueue_files_reco_embedding_backfill(reason="auto_backfill_missing_embeddings")
        return True, None
    bootstrap_required = bool(
        _pipeline_bootstrap_status(timeout=0.10, prefer_cached_on_failure=True).get("bootstrap_required")
    )
    if bootstrap_required and (albums == 0 or tracks == 0):
        _files_index_set_state(
            running=False,
            started_at=None,
            finished_at=time.time(),
            phase="idle",
            current_folder=None,
            folders_processed=0,
            total_folders=0,
            artists=0,
            albums=0,
            tracks=0,
            error=None,
        )
        mod = sys.modules[__name__]
        bootstrap_log_lock = getattr(mod, "_FILES_INDEX_BOOTSTRAP_SKIP_LOG_LOCK", None)
        if bootstrap_log_lock is None:
            bootstrap_log_lock = threading.Lock()
            setattr(mod, "_FILES_INDEX_BOOTSTRAP_SKIP_LOG_LOCK", bootstrap_log_lock)
        with bootstrap_log_lock:
            if not bool(getattr(mod, "_FILES_INDEX_BOOTSTRAP_SKIP_LOGGED", False)):
                setattr(mod, "_FILES_INDEX_BOOTSTRAP_SKIP_LOGGED", True)
                logging.info("Files index stays empty until the first full scan finishes (bootstrap_required=1).")
        return True, None
    result = _rebuild_files_library_index(reason="auto_bootstrap", wait_if_running=True)
    if not result.get("ok"):
        return False, str(result.get("error") or "Files index build failed")
    return True, None
