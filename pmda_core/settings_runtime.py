"""Settings/configuration runtime extracted from the PMDA bootstrap module."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

from flask import jsonify, request


_RUNTIME: Any | None = None
_SYNC_GLOBALS = (
    'ACOUSTID_API_KEY',
    'AI_CALL_COOLDOWN_SEC',
    'AI_CONFIDENCE_MIN',
    'AI_GLOBAL_MAX_CALLS_PER_DAY',
    'AI_GLOBAL_MAX_CALLS_PER_MINUTE',
    'AI_MAX_CALLS_PER_SCAN',
    'AI_USAGE_LEVEL',
    'ARTIST_CREDIT_MODE',
    'ARTWORK_RAM_CACHE_AUTO',
    'ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC',
    'ARTWORK_RAM_CACHE_AUTO_MAX_MB',
    'AUTO_EXPORT_LIBRARY',
    'AUTO_FIX_BROKEN_ALBUMS',
    'AUTO_MOVE_DUPES',
    'AUTO_TUNE_ENABLED',
    'AUTO_TUNE_INTERVAL_SEC',
    'AUTO_TUNE_MB_MIRROR_MAX_RPS',
    'AUTO_TUNE_MB_MIRROR_MIN_RPS',
    'AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP',
    'AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN',
    'BACKUP_BEFORE_FIX',
    'BROKEN_ALBUM_CONSECUTIVE_THRESHOLD',
    'BROKEN_ALBUM_PERCENTAGE_THRESHOLD',
    'CLASSICAL_NAME_PREFERENCE',
    'CROSSCHECK_SAMPLES',
    'CROSS_LIBRARY_DEDUPE',
    'DISABLE_PATH_CROSSCHECK',
    'DISCORD_WEBHOOK',
    'DUPE_ROOT',
    'EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER',
    'EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER',
    'EXPORT_LINK_STRATEGY',
    'EXPORT_NAMING_TEMPLATE',
    'EXPORT_ROOT',
    'FFPROBE_POOL_SIZE',
    'FILES_ROOTS',
    'FMT_SCORE',
    'FORMAT_PREFERENCE',
    'IMPROVE_ALL_WORKERS',
    'JELLYFIN_API_KEY',
    'JELLYFIN_URL',
    'LIBRARY_INCLUDE_UNMATCHED',
    'LIBRARY_MODE',
    'LIBRARY_WINNER_PLACEMENT_STRATEGY',
    'LIVE_DEDUPE_MODE',
    'LOG_FILE',
    'LOG_LEVEL',
    'MAGIC_MODE',
    'MATCH_COVER_OCR_MODE',
    'MB_CANDIDATE_FETCH_LIMIT',
    'MB_DISABLE_CACHE',
    'MB_FAST_FALLBACK_MODE',
    'MB_MIRROR_QUEUE_RPS',
    'MB_MIRROR_QUEUE_WORKERS',
    'MB_PUBLIC_QUEUE_RPS',
    'MB_RETRY_NOT_FOUND',
    'MB_SEARCH_ALBUM_TIMEOUT_SEC',
    'MB_TRACKLIST_FETCH_LIMIT',
    'MEDIA_CACHE_ROOT',
    'MUSICBRAINZ_BASE_URL',
    'MUSICBRAINZ_EMAIL',
    'MUSICBRAINZ_MIRROR_ENABLED',
    'MUSICBRAINZ_MIRROR_NAME',
    'NAVIDROME_API_KEY',
    'NAVIDROME_PASSWORD',
    'NAVIDROME_URL',
    'NAVIDROME_USERNAME',
    'OPENAI_ENABLE_API_KEY_MODE',
    'OPENAI_ENABLE_CODEX_OAUTH_MODE',
    'OPENAI_VISION_MODEL',
    'PATH_MAP',
    'PIPELINE_PLAYER_TARGET',
    'PLEX_CONFIGURED',
    'PLEX_HOST',
    'PLEX_TOKEN',
    'PROVIDER_CACHE_ERROR_TTL_SEC',
    'PROVIDER_CACHE_FOUND_TTL_SEC',
    'PROVIDER_CACHE_NOT_FOUND_TTL_SEC',
    'PROVIDER_GATEWAY_BANDCAMP_RPM',
    'PROVIDER_GATEWAY_CACHE_ENABLED',
    'PROVIDER_GATEWAY_DISCOGS_RPM',
    'PROVIDER_GATEWAY_ENABLED',
    'PROVIDER_GATEWAY_LASTFM_RPM',
    'PROVIDER_GATEWAY_MAX_INFLIGHT',
    'PROVIDER_IDENTITY_MIN_SCORE',
    'PROVIDER_IDENTITY_SCORE_MARGIN',
    'PROVIDER_IDENTITY_STRICT',
    'PROVIDER_IDENTITY_USE_AI',
    'REPROCESS_INCOMPLETE_ALBUMS',
    'REQUIRED_TAGS',
    'SCAN_AI_POLICY',
    'SCAN_DISABLE_CACHE',
    'SCAN_PAID_PROVIDER_ORDER',
    'SCAN_THREADS',
    'SCHEDULER_ALLOW_NON_SCAN_JOBS',
    'SERPER_API_KEY',
    'SKIP_FOLDERS',
    'STORAGE_MAX_ACTIVE_DEVICES',
    'STORAGE_POWER_SAVER_ENABLED',
    'STORAGE_PROVIDER',
    'STORAGE_SPINDOWN_POLICY',
    'UNRAID_CONTAINER_SHARE_ROOT',
    'UNRAID_HOST_MNT_ROOT',
    'UNRAID_USER_SHARE_HOST_ROOT',
    'USE_ACOUSTID',
    'USE_ACOUSTID_WHEN_TAGGED',
    'USE_AI_FOR_DEDUPE',
    'USE_AI_FOR_MB_MATCH',
    'USE_AI_FOR_MB_VERIFY',
    'USE_AI_FOR_SOFT_MATCH_PROFILES',
    'USE_AI_VISION_BEFORE_COVER_INJECT',
    'USE_AI_VISION_FOR_COVER',
    'USE_AI_WEB_SEARCH_FALLBACK',
    'USE_MUSICBRAINZ',
    'USE_WEB_SEARCH_FOR_MB',
    'WEB_SEARCH_LOCAL_ORDER',
    'WEB_SEARCH_PROVIDER',
    'WINNER_SOURCE_ROOT_ID',
    '_ffprobe_pool',
    '_mb_queue',
)


def _runtime_module() -> Any:
    return _RUNTIME if _RUNTIME is not None else sys.modules[__name__]


def _bind_runtime(runtime: Any) -> None:
    """Expose PMDA runtime globals to extracted settings handlers."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "api_config_get",
        "api_config_get_for_runtime",
        "api_config_put",
        "api_config_put_for_runtime",
        "_apply_settings_in_memory",
        "apply_settings_in_memory_for_runtime",
        "_bind_runtime",
        "_runtime_module",
        "_reload_auto_move_from_db",
        "_reload_auto_move_from_db_for_runtime",
        "_reload_musicbrainz_settings_from_db",
        "_reload_musicbrainz_settings_from_db_for_runtime",
        "_reload_section_ids_from_db",
        "_reload_section_ids_from_db_for_runtime",
        "_reload_path_map_from_db",
        "_reload_path_map_from_db_for_runtime",
        "_reload_library_mode_and_files_roots_from_db",
        "_reload_library_mode_and_files_roots_from_db_for_runtime",
        "_sync_runtime_globals",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def _sync_runtime_globals() -> None:
    if _RUNTIME is None:
        return
    for key in _SYNC_GLOBALS:
        if key in globals():
            try:
                setattr(_RUNTIME, key, globals()[key])
            except Exception:
                logging.debug("Failed to sync setting runtime global %s", key, exc_info=True)


def api_config_get_for_runtime(runtime: Any):
    _bind_runtime(runtime)
    return api_config_get()


def apply_settings_in_memory_for_runtime(runtime: Any, updates: dict):
    _bind_runtime(runtime)
    result = _apply_settings_in_memory(updates)
    _sync_runtime_globals()
    return result


def api_config_put_for_runtime(runtime: Any):
    _bind_runtime(runtime)
    result = api_config_put()
    _sync_runtime_globals()
    return result

def _reload_auto_move_from_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any):
    _bind_runtime(runtime)
    result = _reload_auto_move_from_db(*args, **kwargs)
    _sync_runtime_globals()
    return result

def _reload_musicbrainz_settings_from_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any):
    _bind_runtime(runtime)
    result = _reload_musicbrainz_settings_from_db(*args, **kwargs)
    _sync_runtime_globals()
    return result

def _reload_section_ids_from_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any):
    _bind_runtime(runtime)
    result = _reload_section_ids_from_db(*args, **kwargs)
    _sync_runtime_globals()
    return result

def _reload_path_map_from_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any):
    _bind_runtime(runtime)
    result = _reload_path_map_from_db(*args, **kwargs)
    _sync_runtime_globals()
    return result

def _reload_library_mode_and_files_roots_from_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any):
    _bind_runtime(runtime)
    result = _reload_library_mode_and_files_roots_from_db(*args, **kwargs)
    _sync_runtime_globals()
    return result


def _reload_auto_move_from_db():
    """Reload AUTO_MOVE_DUPES from SQLite so the current scan uses the value saved in Settings (UI)."""
    global AUTO_MOVE_DUPES
    val = _get_config_from_db("AUTO_MOVE_DUPES")
    if val is not None:
        AUTO_MOVE_DUPES = bool(_parse_bool(val))
        logging.debug("AUTO_MOVE_DUPES reloaded from DB: %s", AUTO_MOVE_DUPES)


def _reload_musicbrainz_settings_from_db():
    """Reload MusicBrainz-related settings from SQLite so scans use the latest UI values without restart."""
    global USE_MUSICBRAINZ
    global MB_PUBLIC_QUEUE_RPS, MB_MIRROR_QUEUE_RPS, MB_MIRROR_QUEUE_WORKERS
    global MB_SEARCH_ALBUM_TIMEOUT_SEC, MB_CANDIDATE_FETCH_LIMIT, MB_TRACKLIST_FETCH_LIMIT, MB_FAST_FALLBACK_MODE
    global MATCH_COVER_OCR_MODE
    global MB_QUEUE_ENABLED
    global _mb_queue

    mod = sys.modules[__name__]
    changed: dict[str, object] = {}

    def _reload_bool(key: str, current: bool) -> tuple[bool, bool]:
        raw = _get_config_from_db(key)
        if raw is None:
            return current, False
        new_val = bool(_parse_bool(raw))
        return new_val, new_val != current

    def _reload_int(key: str, current: int, *, min_v: int, max_v: int) -> tuple[int, bool]:
        raw = _get_config_from_db(key)
        if raw is None:
            return current, False
        try:
            new_val = int(str(raw).strip())
        except Exception:
            return current, False
        new_val = max(min_v, min(max_v, new_val))
        return new_val, new_val != current

    def _reload_float(key: str, current: float, *, min_v: float, max_v: float) -> tuple[float, bool]:
        raw = _get_config_from_db(key)
        if raw is None:
            return current, False
        try:
            new_val = float(str(raw).strip())
        except Exception:
            return current, False
        new_val = max(min_v, min(max_v, new_val))
        return new_val, new_val != current

    use_mb, use_mb_changed = _reload_bool("USE_MUSICBRAINZ", bool(USE_MUSICBRAINZ))
    if use_mb_changed:
        USE_MUSICBRAINZ = bool(use_mb)
        mod.merged["USE_MUSICBRAINZ"] = USE_MUSICBRAINZ
        changed["USE_MUSICBRAINZ"] = USE_MUSICBRAINZ

    mb_queue_enabled, mb_queue_changed = _reload_bool("MB_QUEUE_ENABLED", bool(MB_QUEUE_ENABLED))
    if mb_queue_changed:
        MB_QUEUE_ENABLED = bool(mb_queue_enabled)
        mod.merged["MB_QUEUE_ENABLED"] = MB_QUEUE_ENABLED
        changed["MB_QUEUE_ENABLED"] = MB_QUEUE_ENABLED
        # Update live queue instance if already created.
        try:
            if _mb_queue is not None:
                _mb_queue.enabled = bool(MB_QUEUE_ENABLED and USE_MUSICBRAINZ)
        except Exception:
            pass

    public_rps, public_rps_changed = _reload_float("MB_PUBLIC_QUEUE_RPS", float(MB_PUBLIC_QUEUE_RPS), min_v=0.1, max_v=100.0)
    if public_rps_changed:
        MB_PUBLIC_QUEUE_RPS = float(public_rps)
        mod.merged["MB_PUBLIC_QUEUE_RPS"] = MB_PUBLIC_QUEUE_RPS
        changed["MB_PUBLIC_QUEUE_RPS"] = MB_PUBLIC_QUEUE_RPS

    mirror_rps, mirror_rps_changed = _reload_float("MB_MIRROR_QUEUE_RPS", float(MB_MIRROR_QUEUE_RPS), min_v=1.0, max_v=100.0)
    if mirror_rps_changed:
        MB_MIRROR_QUEUE_RPS = float(mirror_rps)
        mod.merged["MB_MIRROR_QUEUE_RPS"] = MB_MIRROR_QUEUE_RPS
        changed["MB_MIRROR_QUEUE_RPS"] = MB_MIRROR_QUEUE_RPS

    mirror_workers, mirror_workers_changed = _reload_int(
        "MB_MIRROR_QUEUE_WORKERS",
        int(MB_MIRROR_QUEUE_WORKERS),
        min_v=1,
        max_v=32,
    )
    if mirror_workers_changed:
        MB_MIRROR_QUEUE_WORKERS = int(mirror_workers)
        mod.merged["MB_MIRROR_QUEUE_WORKERS"] = MB_MIRROR_QUEUE_WORKERS
        changed["MB_MIRROR_QUEUE_WORKERS"] = MB_MIRROR_QUEUE_WORKERS

    if public_rps_changed or mirror_rps_changed or mirror_workers_changed:
        try:
            if _mb_queue is not None:
                _mb_queue.shutdown()
                _mb_queue = None
        except Exception:
            pass

    timeout_sec, timeout_changed = _reload_int(
        "MB_SEARCH_ALBUM_TIMEOUT_SEC",
        int(MB_SEARCH_ALBUM_TIMEOUT_SEC),
        min_v=0,
        max_v=3600,
    )
    if timeout_changed:
        MB_SEARCH_ALBUM_TIMEOUT_SEC = int(timeout_sec)
        mod.merged["MB_SEARCH_ALBUM_TIMEOUT_SEC"] = MB_SEARCH_ALBUM_TIMEOUT_SEC
        changed["MB_SEARCH_ALBUM_TIMEOUT_SEC"] = MB_SEARCH_ALBUM_TIMEOUT_SEC

    cand_limit, cand_changed = _reload_int(
        "MB_CANDIDATE_FETCH_LIMIT",
        int(MB_CANDIDATE_FETCH_LIMIT),
        min_v=0,
        max_v=100,
    )
    if cand_changed:
        MB_CANDIDATE_FETCH_LIMIT = int(cand_limit)
        mod.merged["MB_CANDIDATE_FETCH_LIMIT"] = MB_CANDIDATE_FETCH_LIMIT
        changed["MB_CANDIDATE_FETCH_LIMIT"] = MB_CANDIDATE_FETCH_LIMIT

    track_limit, track_changed = _reload_int(
        "MB_TRACKLIST_FETCH_LIMIT",
        int(MB_TRACKLIST_FETCH_LIMIT),
        min_v=0,
        max_v=100,
    )
    if track_changed:
        MB_TRACKLIST_FETCH_LIMIT = int(track_limit)
        mod.merged["MB_TRACKLIST_FETCH_LIMIT"] = MB_TRACKLIST_FETCH_LIMIT
        changed["MB_TRACKLIST_FETCH_LIMIT"] = MB_TRACKLIST_FETCH_LIMIT

    fast_fallback, fast_changed = _reload_bool("MB_FAST_FALLBACK_MODE", bool(MB_FAST_FALLBACK_MODE))
    if fast_changed:
        MB_FAST_FALLBACK_MODE = bool(fast_fallback)
        mod.merged["MB_FAST_FALLBACK_MODE"] = MB_FAST_FALLBACK_MODE
        changed["MB_FAST_FALLBACK_MODE"] = MB_FAST_FALLBACK_MODE

    raw_ocr_mode = _get_config_from_db("MATCH_COVER_OCR_MODE")
    if raw_ocr_mode is not None:
        new_ocr_mode = _normalize_match_cover_ocr_mode(raw_ocr_mode)
        if new_ocr_mode != MATCH_COVER_OCR_MODE:
            MATCH_COVER_OCR_MODE = new_ocr_mode
            mod.merged["MATCH_COVER_OCR_MODE"] = MATCH_COVER_OCR_MODE
            changed["MATCH_COVER_OCR_MODE"] = MATCH_COVER_OCR_MODE

    if changed:
        summary = ", ".join([f"{k}={v}" for k, v in changed.items()])
        logging.info("MusicBrainz settings reloaded from SQLite: %s", summary)


def _reload_section_ids_from_db():
    """Reload SECTION_IDS from SQLite so library APIs use latest saved selection."""
    global SECTION_IDS, SECTION_ID
    section_ids_str = _get_config_from_db("SECTION_IDS")
    if not section_ids_str:
        return
    raw = str(section_ids_str).strip()
    if not raw:
        return
    try:
        if raw.startswith("["):
            SECTION_IDS = [int(x) for x in json.loads(raw)]
        else:
            SECTION_IDS = [int(x.strip()) for x in raw.split(",") if x.strip()]
        SECTION_ID = SECTION_IDS[0] if SECTION_IDS else 0
    except Exception:
        pass


def _reload_path_map_from_db():
    """Reload PATH_MAP from SQLite so scan/dedupe use latest saved bindings (e.g. after Detect & verify)."""
    global PATH_MAP
    path_map_val = _get_config_from_db("PATH_MAP")
    if path_map_val is None:
        return
    parsed = _parse_path_map(path_map_val)
    if parsed:
        PATH_MAP = parsed
        logging.info("PATH_MAP reloaded from SQLite at scan start (%d entries)", len(PATH_MAP))


def _reload_library_mode_and_files_roots_from_db():
    """
    Reload LIBRARY_MODE and FILES_ROOTS from SQLite so each scan uses the
    latest source selection saved in Settings.
    """
    global LIBRARY_MODE, FILES_ROOTS, WINNER_SOURCE_ROOT_ID
    global STORAGE_POWER_SAVER_ENABLED, STORAGE_PROVIDER, UNRAID_HOST_MNT_ROOT, UNRAID_USER_SHARE_HOST_ROOT
    global UNRAID_CONTAINER_SHARE_ROOT, STORAGE_MAX_ACTIVE_DEVICES, STORAGE_SPINDOWN_POLICY
    mod = sys.modules[__name__]

    mode_raw = _get_config_from_db("LIBRARY_MODE")
    if mode_raw is not None:
        mode = str(mode_raw).strip().lower()
        if mode == "files":
            LIBRARY_MODE = "files"
            mod.merged["LIBRARY_MODE"] = "files"
        else:
            LIBRARY_MODE = "files"
            mod.merged["LIBRARY_MODE"] = "files"

    roots_from_sources = _effective_files_roots(enabled_only=True)
    if roots_from_sources:
        FILES_ROOTS = list(roots_from_sources)
        mod.merged["FILES_ROOTS"] = FILES_ROOTS
    else:
        roots_raw = _get_config_from_db("FILES_ROOTS")
        if roots_raw is not None:
            FILES_ROOTS = _parse_files_roots(roots_raw)
            mod.merged["FILES_ROOTS"] = FILES_ROOTS

    winner_row = _winner_source_row()
    if winner_row:
        WINNER_SOURCE_ROOT_ID = str(int(winner_row.get("source_id") or 0))
        mod.merged["WINNER_SOURCE_ROOT_ID"] = WINNER_SOURCE_ROOT_ID

    storage_snapshot = _settings_db_read_all()
    if storage_snapshot:
        STORAGE_POWER_SAVER_ENABLED = bool(_parse_bool(storage_snapshot.get("STORAGE_POWER_SAVER_ENABLED", STORAGE_POWER_SAVER_ENABLED)))
        STORAGE_PROVIDER = str(storage_snapshot.get("STORAGE_PROVIDER", STORAGE_PROVIDER) or "unraid").strip().lower() or "unraid"
        if STORAGE_PROVIDER not in {"unraid"}:
            STORAGE_PROVIDER = "unraid"
        UNRAID_HOST_MNT_ROOT = _storage_clean_path(storage_snapshot.get("UNRAID_HOST_MNT_ROOT", UNRAID_HOST_MNT_ROOT), "/host_mnt")
        UNRAID_USER_SHARE_HOST_ROOT = _storage_clean_path(storage_snapshot.get("UNRAID_USER_SHARE_HOST_ROOT", UNRAID_USER_SHARE_HOST_ROOT), "/host_mnt/user/MURRAY/Music")
        UNRAID_CONTAINER_SHARE_ROOT = _storage_clean_path(storage_snapshot.get("UNRAID_CONTAINER_SHARE_ROOT", UNRAID_CONTAINER_SHARE_ROOT), "/music")
        STORAGE_MAX_ACTIVE_DEVICES = int(max(1, min(64, _parse_int(storage_snapshot.get("STORAGE_MAX_ACTIVE_DEVICES", STORAGE_MAX_ACTIVE_DEVICES), 1) or 1)))
        STORAGE_SPINDOWN_POLICY = str(storage_snapshot.get("STORAGE_SPINDOWN_POLICY", STORAGE_SPINDOWN_POLICY) or "none").strip().lower() or "none"
        if STORAGE_SPINDOWN_POLICY not in {"none"}:
            STORAGE_SPINDOWN_POLICY = "none"
        mod.merged.update(
            {
                "STORAGE_POWER_SAVER_ENABLED": STORAGE_POWER_SAVER_ENABLED,
                "STORAGE_PROVIDER": STORAGE_PROVIDER,
                "UNRAID_HOST_MNT_ROOT": UNRAID_HOST_MNT_ROOT,
                "UNRAID_USER_SHARE_HOST_ROOT": UNRAID_USER_SHARE_HOST_ROOT,
                "UNRAID_CONTAINER_SHARE_ROOT": UNRAID_CONTAINER_SHARE_ROOT,
                "STORAGE_MAX_ACTIVE_DEVICES": STORAGE_MAX_ACTIVE_DEVICES,
                "STORAGE_SPINDOWN_POLICY": STORAGE_SPINDOWN_POLICY,
            }
        )

    try:
        scan_roots = _effective_files_scan_roots(enabled_only=True)
    except Exception:
        scan_roots = list(FILES_ROOTS or [])
    logging.info(
        "Files mode roots reloaded: index_roots=%d scan_roots=%d scan=%s storage_power_saver=%s provider=%s",
        len(FILES_ROOTS or []),
        len(scan_roots or []),
        scan_roots or [],
        STORAGE_POWER_SAVER_ENABLED,
        STORAGE_PROVIDER,
    )



def api_config_get():
    """Return current effective configuration for the Web UI.
    Loads from SQLite (settings table) first, then falls back to runtime variables.
    SQLite is the single source of truth for all saved configuration.
    """
    # Load settings table once to keep /api/config responsive under concurrent traffic.
    settings_snapshot = _settings_db_read_all()

    # Load from SQLite first (source of truth), fallback to runtime variables
    def get_setting(key: str, runtime_value, default=""):
        if key in settings_snapshot:
            return settings_snapshot.get(key)
        return runtime_value if runtime_value is not None else default

    def get_setting_bool(key: str, runtime_value):
        """Return config value as a real boolean for JSON (so frontend toggles work)."""
        raw = get_setting(key, runtime_value)
        return bool(_parse_bool(raw)) if raw not in (None, "") else bool(_parse_bool(str(runtime_value)))

    path_map = getattr(_runtime_module(), "PATH_MAP", {})
    section_ids = getattr(_runtime_module(), "SECTION_IDS", [])
    skip_folders = getattr(_runtime_module(), "SKIP_FOLDERS", [])

    # Check if settings exist in DB (wizard was completed)
    has_settings = bool(settings_snapshot)

    # Load SECTION_IDS from SQLite if available (stored as comma-separated "1,5" or legacy JSON "[1,5]")
    section_ids_str = settings_snapshot.get("SECTION_IDS")
    if section_ids_str:
        raw = str(section_ids_str).strip()
        try:
            if raw.startswith("["):
                section_ids = [int(x) for x in json.loads(raw)]
            else:
                section_ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
        except Exception:
            pass

    # Load PATH_MAP from SQLite if available
    path_map_str = settings_snapshot.get("PATH_MAP")
    if path_map_str:
        try:
            path_map = json.loads(path_map_str) if isinstance(path_map_str, str) else path_map_str
        except Exception:
            pass

    files_roots_effective = _parse_files_roots(get_setting("FILES_ROOTS", ",".join(FILES_ROOTS) if isinstance(FILES_ROOTS, list) else (FILES_ROOTS or "")))
    workflow_state = _library_workflow_state(settings_snapshot)
    workflow_scope_roots = _library_workflow_scope_roots(settings_snapshot)
    configured_flag = bool(_parse_bool(settings_snapshot.get("configured"))) if settings_snapshot else False
    configured = configured_flag or has_settings or bool(files_roots_effective) or _has_settings_in_db()

    def _is_set(val) -> bool:
        return bool(str(val or "").strip())

    # Fetch effective secret values (from DB/runtime) once so *_SET flags are accurate.
    openai_key_eff = get_setting("OPENAI_API_KEY", OPENAI_API_KEY)
    openai_oauth_refresh_eff = get_setting("OPENAI_OAUTH_REFRESH_TOKEN", "")
    anthropic_key_eff = get_setting("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY)
    google_key_eff = get_setting("GOOGLE_API_KEY", GOOGLE_API_KEY)
    discogs_token_eff = get_setting("DISCOGS_USER_TOKEN", DISCOGS_USER_TOKEN)
    lastfm_key_eff = get_setting("LASTFM_API_KEY", LASTFM_API_KEY)
    lastfm_secret_eff = get_setting("LASTFM_API_SECRET", LASTFM_API_SECRET)
    lastfm_session_name_eff = str(_get_config_from_db(_LASTFM_SESSION_NAME_SETTING, "") or "").strip()
    lastfm_session_connected_eff = bool(_lastfm_session_key())
    lastfm_pending_eff = bool(_lastfm_pending_token())
    fanart_key_eff = get_setting("FANART_API_KEY", FANART_API_KEY)
    theaudiodb_key_eff = get_setting("THEAUDIODB_API_KEY", THEAUDIODB_API_KEY)
    serper_key_eff = get_setting("SERPER_API_KEY", SERPER_API_KEY)
    acoustid_key_eff = get_setting("ACOUSTID_API_KEY", ACOUSTID_API_KEY)
    mb_replication_token_eff = get_setting("MUSICBRAINZ_REPLICATION_TOKEN", "")
    plex_token_eff = get_setting("PLEX_TOKEN", PLEX_TOKEN)
    jellyfin_key_eff = get_setting("JELLYFIN_API_KEY", JELLYFIN_API_KEY)
    discord_webhook_eff = get_setting("DISCORD_WEBHOOK", DISCORD_WEBHOOK)
    navidrome_pass_eff = get_setting("NAVIDROME_PASSWORD", NAVIDROME_PASSWORD)
    navidrome_key_eff = get_setting("NAVIDROME_API_KEY", NAVIDROME_API_KEY)
    user_id_eff = _current_user_id_or_zero()
    provider_prefs_eff = _get_ai_provider_preferences(user_id_eff)
    # Keep /api/config fast: do not trigger token refresh/validation here.
    # Detailed runtime readiness is exposed via /api/ai/providers/openai-codex/oauth/status.
    codex_connected_eff = _openai_codex_connected(user_id_eff, require_token=False)
    openai_effective_interactive = select_provider_id(
        context="interactive",
        preferred=str(provider_prefs_eff.get("interactive_provider_id") or "openai-codex"),
        codex_connected=codex_connected_eff,
        openai_api_enabled=bool(get_setting_bool("OPENAI_ENABLE_API_KEY_MODE", OPENAI_ENABLE_API_KEY_MODE)),
        openai_codex_enabled=bool(get_setting_bool("OPENAI_ENABLE_CODEX_OAUTH_MODE", OPENAI_ENABLE_CODEX_OAUTH_MODE)),
    )
    openai_effective_batch = select_provider_id(
        context="batch",
        preferred=str(provider_prefs_eff.get("batch_provider_id") or "openai-codex"),
        codex_connected=codex_connected_eff,
        openai_api_enabled=bool(get_setting_bool("OPENAI_ENABLE_API_KEY_MODE", OPENAI_ENABLE_API_KEY_MODE)),
        openai_codex_enabled=bool(get_setting_bool("OPENAI_ENABLE_CODEX_OAUTH_MODE", OPENAI_ENABLE_CODEX_OAUTH_MODE)),
    )

    payload = {
        "configured": configured,
        "PATH_MAP": path_map,
        "DUPE_ROOT": str(DUPE_ROOT),
        "PMDA_CONFIG_DIR": str(CONFIG_DIR),
        "MUSIC_PARENT_PATH": get_setting("MUSIC_PARENT_PATH", merged.get("MUSIC_PARENT_PATH", "")),
        "SCAN_THREADS": get_setting("SCAN_THREADS", SCAN_THREADS if isinstance(SCAN_THREADS, int) else "auto"),
        "FFPROBE_POOL_SIZE": get_setting("FFPROBE_POOL_SIZE", FFPROBE_POOL_SIZE),
        "IMPROVE_ALL_WORKERS": get_setting("IMPROVE_ALL_WORKERS", IMPROVE_ALL_WORKERS),
        "SKIP_FOLDERS": get_setting("SKIP_FOLDERS", ",".join(skip_folders) if isinstance(skip_folders, list) else (skip_folders or "")),
        "CROSS_LIBRARY_DEDUPE": get_setting_bool("CROSS_LIBRARY_DEDUPE", CROSS_LIBRARY_DEDUPE),
        "CROSSCHECK_SAMPLES": get_setting("CROSSCHECK_SAMPLES", CROSSCHECK_SAMPLES),
        "FORMAT_PREFERENCE": _parse_format_preference(get_setting("FORMAT_PREFERENCE", FORMAT_PREFERENCE)),
        "AI_PROVIDER": get_setting("AI_PROVIDER", AI_PROVIDER),
        "AI_USAGE_LEVEL": _normalize_ai_usage_level(get_setting("AI_USAGE_LEVEL", AI_USAGE_LEVEL)),
        "SCAN_AI_POLICY": _normalize_scan_ai_policy(get_setting("SCAN_AI_POLICY", SCAN_AI_POLICY)),
        "SCAN_PAID_PROVIDER_ORDER": ",".join(
            _normalize_ordered_values(
                get_setting("SCAN_PAID_PROVIDER_ORDER", SCAN_PAID_PROVIDER_ORDER),
                allowed=("openai-api", "openai-codex", "anthropic", "google"),
                default=("openai-api", "openai-codex", "anthropic", "google"),
            )
        ),
        "WEB_SEARCH_LOCAL_ORDER": ",".join(
            _normalize_ordered_values(
                get_setting("WEB_SEARCH_LOCAL_ORDER", WEB_SEARCH_LOCAL_ORDER),
                allowed=("serper",),
                default=("serper",),
            )
        ),
        "OPENAI_API_KEY": str(openai_key_eff or ""),
        "OPENAI_API_KEY_SET": _is_set(openai_key_eff),
        "OPENAI_ENABLE_API_KEY_MODE": get_setting_bool("OPENAI_ENABLE_API_KEY_MODE", OPENAI_ENABLE_API_KEY_MODE),
        "OPENAI_ENABLE_CODEX_OAUTH_MODE": get_setting_bool("OPENAI_ENABLE_CODEX_OAUTH_MODE", OPENAI_ENABLE_CODEX_OAUTH_MODE),
        "OPENAI_OAUTH_REFRESH_TOKEN_SET": _is_set(openai_oauth_refresh_eff),
        "OPENAI_AUTH_MODE": (
            "oauth_api_key"
            if (_is_set(openai_oauth_refresh_eff) and _is_set(openai_key_eff))
            else ("oauth_connected_no_api_key" if _is_set(openai_oauth_refresh_eff) else ("api_key" if _is_set(openai_key_eff) else "none"))
        ),
        "OPENAI_CODEX_OAUTH_CONNECTED": bool(codex_connected_eff),
        "OPENAI_PROVIDER_PREF_INTERACTIVE": str(provider_prefs_eff.get("interactive_provider_id") or "openai-codex"),
        "OPENAI_PROVIDER_PREF_BATCH": str(provider_prefs_eff.get("batch_provider_id") or "openai-codex"),
        "OPENAI_PROVIDER_PREF_WEB_SEARCH": str(provider_prefs_eff.get("web_search_provider_id") or "openai-codex"),
        "OPENAI_PROVIDER_EFFECTIVE_INTERACTIVE": _normalize_provider_id(openai_effective_interactive, fallback="openai-api"),
        "OPENAI_PROVIDER_EFFECTIVE_BATCH": _normalize_provider_id(openai_effective_batch, fallback="openai-api"),
        "SCAN_AI_EFFECTIVE_BATCH": _resolve_provider_for_runtime("openai", "scan_pipeline"),
        "SCAN_AI_EFFECTIVE_WEB_SEARCH": (
            "provider"
            if bool(_web_search_provider_order())
            else ("local_ai" if _ollama_web_search_enabled(allow_ai_fallback=True) else ("paid_ai" if _web_search_ai_fallback_enabled(allow_ai_fallback=True) else "disabled"))
        ),
        "OPENAI_MODEL": get_setting("OPENAI_MODEL", OPENAI_MODEL),
        "OPENAI_MODEL_FALLBACKS": get_setting("OPENAI_MODEL_FALLBACKS", merged.get("OPENAI_MODEL_FALLBACKS", "")),
        "ANTHROPIC_API_KEY": str(anthropic_key_eff or ""),
        "ANTHROPIC_API_KEY_SET": _is_set(anthropic_key_eff),
        "GOOGLE_API_KEY": str(google_key_eff or ""),
        "GOOGLE_API_KEY_SET": _is_set(google_key_eff),
        "OLLAMA_URL": get_setting("OLLAMA_URL", OLLAMA_URL),
        "OLLAMA_MODEL": get_setting("OLLAMA_MODEL", OLLAMA_MODEL),
        "OLLAMA_COMPLEX_MODEL": get_setting("OLLAMA_COMPLEX_MODEL", getattr(_runtime_module(), "OLLAMA_COMPLEX_MODEL", "qwen3:14b")),
        "OLLAMA_RUNTIME_MODE": get_setting("OLLAMA_RUNTIME_MODE", str(merged.get("OLLAMA_RUNTIME_MODE", "") or "")),
        "SCAN_AI_LOCAL_BULK_MODEL": _ollama_model_configured(),
        "SCAN_AI_LOCAL_HARD_MODEL": _ollama_complex_model_configured(),
        "SCAN_AI_LOCAL_HARD_AVAILABLE": _ollama_model_available(_ollama_complex_model_configured()),
        "USE_MUSICBRAINZ": True,
        "MUSICBRAINZ_EMAIL": get_setting("MUSICBRAINZ_EMAIL", MUSICBRAINZ_EMAIL),
        "MUSICBRAINZ_MIRROR_ENABLED": get_setting_bool("MUSICBRAINZ_MIRROR_ENABLED", MUSICBRAINZ_MIRROR_ENABLED),
        "MUSICBRAINZ_BASE_URL": get_setting("MUSICBRAINZ_BASE_URL", MUSICBRAINZ_BASE_URL),
        "MUSICBRAINZ_MIRROR_NAME": get_setting("MUSICBRAINZ_MIRROR_NAME", MUSICBRAINZ_MIRROR_NAME),
        "MUSICBRAINZ_RUNTIME_MODE": get_setting("MUSICBRAINZ_RUNTIME_MODE", str(merged.get("MUSICBRAINZ_RUNTIME_MODE", "") or "")),
        "MUSICBRAINZ_REPLICATION_TOKEN": str(mb_replication_token_eff or ""),
        "MUSICBRAINZ_REPLICATION_TOKEN_SET": _is_set(mb_replication_token_eff),
        "MUSICBRAINZ_EFFECTIVE_BASE_URL": _musicbrainz_target_settings()["base_url"],
        "MANAGED_RUNTIME_CONFIG_ROOT": get_setting("MANAGED_RUNTIME_CONFIG_ROOT", str(merged.get("MANAGED_RUNTIME_CONFIG_ROOT", "") or "")),
        "MANAGED_RUNTIME_DATA_ROOT": get_setting("MANAGED_RUNTIME_DATA_ROOT", str(merged.get("MANAGED_RUNTIME_DATA_ROOT", "") or "")),
        "MANAGED_MUSICBRAINZ_INSTALL_ROOT": get_setting("MANAGED_MUSICBRAINZ_INSTALL_ROOT", str(merged.get("MANAGED_MUSICBRAINZ_INSTALL_ROOT", "") or "")),
        "MANAGED_MUSICBRAINZ_UPDATE_ENABLED": get_setting_bool("MANAGED_MUSICBRAINZ_UPDATE_ENABLED", True),
        "MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS": max(
            1,
            min(
                24 * 30,
                int(get_setting("MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS", 24 * 7) or (24 * 7)),
            ),
        ),
        "MB_PUBLIC_QUEUE_RPS": float(get_setting("MB_PUBLIC_QUEUE_RPS", MB_PUBLIC_QUEUE_RPS) or MB_PUBLIC_QUEUE_RPS),
        "MB_MIRROR_QUEUE_RPS": float(get_setting("MB_MIRROR_QUEUE_RPS", MB_MIRROR_QUEUE_RPS) or MB_MIRROR_QUEUE_RPS),
        "MB_MIRROR_QUEUE_WORKERS": max(
            1,
            min(
                32,
                int(get_setting("MB_MIRROR_QUEUE_WORKERS", MB_MIRROR_QUEUE_WORKERS) or MB_MIRROR_QUEUE_WORKERS),
            ),
        ),
        "MB_RETRY_NOT_FOUND": get_setting_bool("MB_RETRY_NOT_FOUND", MB_RETRY_NOT_FOUND),
        "MB_SEARCH_ALBUM_TIMEOUT_SEC": max(
            0,
            min(
                3600,
                int(get_setting("MB_SEARCH_ALBUM_TIMEOUT_SEC", MB_SEARCH_ALBUM_TIMEOUT_SEC) or 0),
            ),
        ),
        "MB_CANDIDATE_FETCH_LIMIT": max(
            0,
            min(
                100,
                int(get_setting("MB_CANDIDATE_FETCH_LIMIT", MB_CANDIDATE_FETCH_LIMIT) or 0),
            ),
        ),
        "MB_TRACKLIST_FETCH_LIMIT": max(
            0,
            min(
                100,
                int(get_setting("MB_TRACKLIST_FETCH_LIMIT", MB_TRACKLIST_FETCH_LIMIT) or 0),
            ),
        ),
        "MB_FAST_FALLBACK_MODE": get_setting_bool("MB_FAST_FALLBACK_MODE", MB_FAST_FALLBACK_MODE),
        "PROVIDER_IDENTITY_STRICT": get_setting_bool("PROVIDER_IDENTITY_STRICT", PROVIDER_IDENTITY_STRICT),
        "PROVIDER_IDENTITY_USE_AI": get_setting_bool("PROVIDER_IDENTITY_USE_AI", PROVIDER_IDENTITY_USE_AI),
        "MATCH_COVER_OCR_MODE": _normalize_match_cover_ocr_mode(get_setting("MATCH_COVER_OCR_MODE", MATCH_COVER_OCR_MODE)),
        "PROVIDER_IDENTITY_MIN_SCORE": get_setting("PROVIDER_IDENTITY_MIN_SCORE", PROVIDER_IDENTITY_MIN_SCORE),
        "PROVIDER_IDENTITY_SCORE_MARGIN": get_setting("PROVIDER_IDENTITY_SCORE_MARGIN", PROVIDER_IDENTITY_SCORE_MARGIN),
        "PROVIDER_CACHE_FOUND_TTL_SEC": get_setting("PROVIDER_CACHE_FOUND_TTL_SEC", PROVIDER_CACHE_FOUND_TTL_SEC),
        "PROVIDER_CACHE_NOT_FOUND_TTL_SEC": get_setting("PROVIDER_CACHE_NOT_FOUND_TTL_SEC", PROVIDER_CACHE_NOT_FOUND_TTL_SEC),
        "PROVIDER_CACHE_ERROR_TTL_SEC": get_setting("PROVIDER_CACHE_ERROR_TTL_SEC", PROVIDER_CACHE_ERROR_TTL_SEC),
        "PROVIDER_GATEWAY_ENABLED": get_setting_bool("PROVIDER_GATEWAY_ENABLED", PROVIDER_GATEWAY_ENABLED),
        "PROVIDER_GATEWAY_CACHE_ENABLED": get_setting_bool("PROVIDER_GATEWAY_CACHE_ENABLED", PROVIDER_GATEWAY_CACHE_ENABLED),
        "PROVIDER_GATEWAY_MAX_INFLIGHT": get_setting("PROVIDER_GATEWAY_MAX_INFLIGHT", PROVIDER_GATEWAY_MAX_INFLIGHT),
        "PROVIDER_GATEWAY_DISCOGS_RPM": get_setting("PROVIDER_GATEWAY_DISCOGS_RPM", PROVIDER_GATEWAY_DISCOGS_RPM),
        "PROVIDER_GATEWAY_ITUNES_RPM": get_setting("PROVIDER_GATEWAY_ITUNES_RPM", PROVIDER_GATEWAY_ITUNES_RPM),
        "PROVIDER_GATEWAY_DEEZER_RPM": get_setting("PROVIDER_GATEWAY_DEEZER_RPM", PROVIDER_GATEWAY_DEEZER_RPM),
        "PROVIDER_GATEWAY_SPOTIFY_RPM": get_setting("PROVIDER_GATEWAY_SPOTIFY_RPM", PROVIDER_GATEWAY_SPOTIFY_RPM),
        "PROVIDER_GATEWAY_QOBUZ_RPM": get_setting("PROVIDER_GATEWAY_QOBUZ_RPM", PROVIDER_GATEWAY_QOBUZ_RPM),
        "PROVIDER_GATEWAY_TIDAL_RPM": get_setting("PROVIDER_GATEWAY_TIDAL_RPM", PROVIDER_GATEWAY_TIDAL_RPM),
        "PROVIDER_GATEWAY_LASTFM_RPM": get_setting("PROVIDER_GATEWAY_LASTFM_RPM", PROVIDER_GATEWAY_LASTFM_RPM),
        "PROVIDER_GATEWAY_AUDIODB_RPM": get_setting("PROVIDER_GATEWAY_AUDIODB_RPM", PROVIDER_GATEWAY_AUDIODB_RPM),
        "PROVIDER_GATEWAY_BANDCAMP_RPM": get_setting("PROVIDER_GATEWAY_BANDCAMP_RPM", PROVIDER_GATEWAY_BANDCAMP_RPM),
        "AUTO_TUNE_ENABLED": get_setting_bool("AUTO_TUNE_ENABLED", AUTO_TUNE_ENABLED),
        "AUTO_TUNE_INTERVAL_SEC": get_setting("AUTO_TUNE_INTERVAL_SEC", AUTO_TUNE_INTERVAL_SEC),
        "AUTO_TUNE_MB_MIRROR_MIN_RPS": get_setting("AUTO_TUNE_MB_MIRROR_MIN_RPS", AUTO_TUNE_MB_MIRROR_MIN_RPS),
        "AUTO_TUNE_MB_MIRROR_MAX_RPS": get_setting("AUTO_TUNE_MB_MIRROR_MAX_RPS", AUTO_TUNE_MB_MIRROR_MAX_RPS),
        "AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN": get_setting("AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN", AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN),
        "AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP": get_setting("AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP", AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP),
        "USE_AI_FOR_MB_MATCH": bool(USE_AI_FOR_MB_MATCH),
        "USE_AI_FOR_MB_VERIFY": bool(USE_AI_FOR_MB_VERIFY),
        "USE_AI_FOR_DEDUPE": bool(USE_AI_FOR_DEDUPE),
        "USE_AI_FOR_SOFT_MATCH_PROFILES": get_setting_bool("USE_AI_FOR_SOFT_MATCH_PROFILES", USE_AI_FOR_SOFT_MATCH_PROFILES),
        "USE_AI_VISION_FOR_COVER": bool(USE_AI_VISION_FOR_COVER),
        "AI_CONFIDENCE_MIN": max(0, min(100, int(get_setting("AI_CONFIDENCE_MIN", AI_CONFIDENCE_MIN) or 50))),
        "OPENAI_VISION_MODEL": get_setting("OPENAI_VISION_MODEL", OPENAI_VISION_MODEL),
        "USE_AI_VISION_BEFORE_COVER_INJECT": bool(USE_AI_VISION_BEFORE_COVER_INJECT),
        "USE_WEB_SEARCH_FOR_MB": bool(USE_WEB_SEARCH_FOR_MB),
        "WEB_SEARCH_PROVIDER": _normalize_web_search_provider(get_setting("WEB_SEARCH_PROVIDER", WEB_SEARCH_PROVIDER)),
        "USE_AI_WEB_SEARCH_FALLBACK": get_setting_bool("USE_AI_WEB_SEARCH_FALLBACK", USE_AI_WEB_SEARCH_FALLBACK),
        "SCHEDULER_ALLOW_NON_SCAN_JOBS": get_setting_bool("SCHEDULER_ALLOW_NON_SCAN_JOBS", SCHEDULER_ALLOW_NON_SCAN_JOBS),
        "AI_MAX_CALLS_PER_SCAN": 0,
        "AI_CALL_COOLDOWN_SEC": 0.0,
        "AI_GLOBAL_MAX_CALLS_PER_MINUTE": 0,
        "AI_GLOBAL_MAX_CALLS_PER_DAY": 0,
        "SERPER_API_KEY": str(serper_key_eff or ""),
        "SERPER_API_KEY_SET": _is_set(serper_key_eff),
        "USE_ACOUSTID": True,
        "ACOUSTID_API_KEY": str(acoustid_key_eff or ""),
        "ACOUSTID_API_KEY_SET": _is_set(acoustid_key_eff),
        "USE_ACOUSTID_WHEN_TAGGED": False,
        "AUTO_FIX_BROKEN_ALBUMS": get_setting_bool("AUTO_FIX_BROKEN_ALBUMS", AUTO_FIX_BROKEN_ALBUMS),
        "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD": get_setting("BROKEN_ALBUM_CONSECUTIVE_THRESHOLD", BROKEN_ALBUM_CONSECUTIVE_THRESHOLD),
        "BROKEN_ALBUM_PERCENTAGE_THRESHOLD": get_setting("BROKEN_ALBUM_PERCENTAGE_THRESHOLD", BROKEN_ALBUM_PERCENTAGE_THRESHOLD),
        "METADATA_QUEUE_ENABLED": get_setting_bool("METADATA_QUEUE_ENABLED", METADATA_QUEUE_ENABLED),
        "METADATA_WORKER_MODE": get_setting("METADATA_WORKER_MODE", METADATA_WORKER_MODE),
        "METADATA_WORKER_COUNT": _metadata_worker_ui_value(get_setting("METADATA_WORKER_COUNT", METADATA_WORKER_COUNT)),
        "METADATA_JOB_BATCH_SIZE": _metadata_job_batch_ui_value(get_setting("METADATA_JOB_BATCH_SIZE", METADATA_JOB_BATCH_SIZE)),
        "REQUIRED_TAGS": ["artist", "album", "genre", "year", "tracks"],
        "DISCORD_WEBHOOK": str(discord_webhook_eff or ""),
        "DISCORD_WEBHOOK_SET": _is_set(discord_webhook_eff),
        "LOG_LEVEL": get_setting("LOG_LEVEL", LOG_LEVEL),
        "LOG_FILE": get_setting("LOG_FILE", LOG_FILE),
        "AUTO_MOVE_DUPES": get_setting_bool("AUTO_MOVE_DUPES", AUTO_MOVE_DUPES),
        "PIPELINE_ENABLE_MATCH_FIX": get_setting_bool("PIPELINE_ENABLE_MATCH_FIX", PIPELINE_ENABLE_MATCH_FIX),
        "PIPELINE_ENABLE_DEDUPE": get_setting_bool("PIPELINE_ENABLE_DEDUPE", PIPELINE_ENABLE_DEDUPE),
        "PIPELINE_ENABLE_INCOMPLETE_MOVE": get_setting_bool("PIPELINE_ENABLE_INCOMPLETE_MOVE", PIPELINE_ENABLE_INCOMPLETE_MOVE),
        "PIPELINE_ENABLE_EXPORT": get_setting_bool("PIPELINE_ENABLE_EXPORT", PIPELINE_ENABLE_EXPORT),
        "PIPELINE_ENABLE_PLAYER_SYNC": get_setting_bool("PIPELINE_ENABLE_PLAYER_SYNC", PIPELINE_ENABLE_PLAYER_SYNC),
        "PIPELINE_PLAYER_TARGET": _normalize_player_target(get_setting("PIPELINE_PLAYER_TARGET", PIPELINE_PLAYER_TARGET)),
        "PLEX_HOST": get_setting("PLEX_HOST", PLEX_HOST),
        "PLEX_TOKEN_SET": _is_set(plex_token_eff),
        "LIDARR_FEATURE_ENABLED": _lidarr_feature_enabled(),
        "AUTOBRR_FEATURE_ENABLED": _autobrr_feature_enabled(),
        "PIPELINE_POST_SCAN_ASYNC": get_setting_bool("PIPELINE_POST_SCAN_ASYNC", PIPELINE_POST_SCAN_ASYNC),
        "NORMALIZE_PARENTHETICAL_FOR_DEDUPE": get_setting_bool("NORMALIZE_PARENTHETICAL_FOR_DEDUPE", True),
        "BACKUP_BEFORE_FIX": get_setting_bool("BACKUP_BEFORE_FIX", BACKUP_BEFORE_FIX),
        "MAGIC_MODE": get_setting_bool("MAGIC_MODE", MAGIC_MODE),
        "REPROCESS_INCOMPLETE_ALBUMS": get_setting_bool("REPROCESS_INCOMPLETE_ALBUMS", REPROCESS_INCOMPLETE_ALBUMS),
        "MCP_ENABLED": get_setting_bool("MCP_ENABLED", False),
        "TASK_NOTIFICATIONS_ENABLED": get_setting_bool("TASK_NOTIFICATIONS_ENABLED", TASK_NOTIFICATIONS_ENABLED),
        "TASK_NOTIFICATIONS_SUCCESS": get_setting_bool("TASK_NOTIFICATIONS_SUCCESS", TASK_NOTIFICATIONS_SUCCESS),
        "TASK_NOTIFICATIONS_FAILURE": get_setting_bool("TASK_NOTIFICATIONS_FAILURE", TASK_NOTIFICATIONS_FAILURE),
        "TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN": get_setting_bool("TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN", TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN),
        "TASK_NOTIFICATIONS_COOLDOWN_SEC": int(get_setting("TASK_NOTIFICATIONS_COOLDOWN_SEC", TASK_NOTIFICATIONS_COOLDOWN_SEC) or TASK_NOTIFICATIONS_COOLDOWN_SEC),
        "TASK_NOTIFY_SCAN_CHANGED": get_setting_bool("TASK_NOTIFY_SCAN_CHANGED", TASK_NOTIFY_SCAN_CHANGED),
        "TASK_NOTIFY_SCAN_FULL": get_setting_bool("TASK_NOTIFY_SCAN_FULL", TASK_NOTIFY_SCAN_FULL),
        "TASK_NOTIFY_ENRICH_BATCH": get_setting_bool("TASK_NOTIFY_ENRICH_BATCH", TASK_NOTIFY_ENRICH_BATCH),
        "TASK_NOTIFY_DEDUPE": get_setting_bool("TASK_NOTIFY_DEDUPE", TASK_NOTIFY_DEDUPE),
        "TASK_NOTIFY_INCOMPLETE_MOVE": get_setting_bool("TASK_NOTIFY_INCOMPLETE_MOVE", TASK_NOTIFY_INCOMPLETE_MOVE),
        "TASK_NOTIFY_EXPORT": get_setting_bool("TASK_NOTIFY_EXPORT", TASK_NOTIFY_EXPORT),
        "TASK_NOTIFY_PLAYER_SYNC": get_setting_bool("TASK_NOTIFY_PLAYER_SYNC", TASK_NOTIFY_PLAYER_SYNC),
        "SCHEDULER_PAUSED": bool(_scheduler_paused),
        "ARTIST_CREDIT_MODE": get_setting("ARTIST_CREDIT_MODE", ARTIST_CREDIT_MODE),
        "CLASSICAL_NAME_PREFERENCE": get_setting("CLASSICAL_NAME_PREFERENCE", CLASSICAL_NAME_PREFERENCE),
        "LIVE_DEDUPE_MODE": get_setting("LIVE_DEDUPE_MODE", LIVE_DEDUPE_MODE),
        "SCAN_DISABLE_CACHE": get_setting_bool("SCAN_DISABLE_CACHE", SCAN_DISABLE_CACHE),
        "DISABLE_PATH_CROSSCHECK": get_setting_bool("DISABLE_PATH_CROSSCHECK", DISABLE_PATH_CROSSCHECK),
        "USE_DISCOGS": get_setting_bool("USE_DISCOGS", USE_DISCOGS),
        "DISCOGS_USER_TOKEN": str(discogs_token_eff or ""),
        "DISCOGS_USER_TOKEN_SET": _is_set(discogs_token_eff),
        "USE_ITUNES": get_setting_bool("USE_ITUNES", USE_ITUNES),
        "USE_DEEZER": get_setting_bool("USE_DEEZER", USE_DEEZER),
        "USE_SPOTIFY": get_setting_bool("USE_SPOTIFY", USE_SPOTIFY),
        "USE_QOBUZ": get_setting_bool("USE_QOBUZ", USE_QOBUZ),
        "USE_TIDAL": get_setting_bool("USE_TIDAL", USE_TIDAL),
        "USE_LASTFM": get_setting_bool("USE_LASTFM", USE_LASTFM),
        "LASTFM_API_KEY": str(lastfm_key_eff or ""),
        "LASTFM_API_KEY_SET": _is_set(lastfm_key_eff),
        "LASTFM_API_SECRET": str(lastfm_secret_eff or ""),
        "LASTFM_API_SECRET_SET": _is_set(lastfm_secret_eff),
        "LASTFM_SCROBBLE_ENABLED": get_setting_bool("LASTFM_SCROBBLE_ENABLED", getattr(_runtime_module(), "LASTFM_SCROBBLE_ENABLED", False)),
        "LASTFM_NOW_PLAYING_ENABLED": get_setting_bool("LASTFM_NOW_PLAYING_ENABLED", getattr(_runtime_module(), "LASTFM_NOW_PLAYING_ENABLED", False)),
        "LASTFM_SCROBBLE_CONNECTED": bool(lastfm_session_connected_eff),
        "LASTFM_SCROBBLE_USER": str(lastfm_session_name_eff or ""),
        "LASTFM_SCROBBLE_PENDING": bool(lastfm_pending_eff and not lastfm_session_connected_eff),
        "FANART_API_KEY": str(fanart_key_eff or ""),
        "FANART_API_KEY_SET": _is_set(fanart_key_eff),
        "THEAUDIODB_API_KEY": str(theaudiodb_key_eff or ""),
        "THEAUDIODB_API_KEY_SET": _is_set(theaudiodb_key_eff),
        "USE_BANDCAMP": get_setting_bool("USE_BANDCAMP", USE_BANDCAMP),
        "JELLYFIN_URL": get_setting("JELLYFIN_URL", JELLYFIN_URL),
        "JELLYFIN_API_KEY": str(jellyfin_key_eff or ""),
        "JELLYFIN_API_KEY_SET": _is_set(jellyfin_key_eff),
        "NAVIDROME_URL": get_setting("NAVIDROME_URL", NAVIDROME_URL),
        "NAVIDROME_USERNAME": get_setting("NAVIDROME_USERNAME", NAVIDROME_USERNAME),
        "NAVIDROME_PASSWORD": str(navidrome_pass_eff or ""),
        "NAVIDROME_PASSWORD_SET": _is_set(navidrome_pass_eff),
        "NAVIDROME_API_KEY": str(navidrome_key_eff or ""),
        "NAVIDROME_API_KEY_SET": _is_set(navidrome_key_eff),
        "SKIP_MB_FOR_LIVE_ALBUMS": True,
        "TRACKLIST_MATCH_MIN": "0.9",
        "LIVE_ALBUMS_MB_STRICT": False,
        "INCOMPLETE_ALBUMS_TARGET_DIR": get_setting("INCOMPLETE_ALBUMS_TARGET_DIR", "/dupes/incomplete_albums"),
        # Library backend and file-library settings
        "LIBRARY_MODE": "files",
        "LIBRARY_INCLUDE_UNMATCHED": get_setting_bool("LIBRARY_INCLUDE_UNMATCHED", LIBRARY_INCLUDE_UNMATCHED),
        "FILES_ROOTS": ", ".join(files_roots_effective),
        "STORAGE_POWER_SAVER_ENABLED": get_setting_bool("STORAGE_POWER_SAVER_ENABLED", STORAGE_POWER_SAVER_ENABLED),
        "STORAGE_PROVIDER": str(get_setting("STORAGE_PROVIDER", STORAGE_PROVIDER) or "unraid").strip().lower() or "unraid",
        "UNRAID_HOST_MNT_ROOT": _storage_clean_path(get_setting("UNRAID_HOST_MNT_ROOT", UNRAID_HOST_MNT_ROOT), "/host_mnt"),
        "UNRAID_USER_SHARE_HOST_ROOT": _storage_clean_path(get_setting("UNRAID_USER_SHARE_HOST_ROOT", UNRAID_USER_SHARE_HOST_ROOT), "/host_mnt/user/MURRAY/Music"),
        "UNRAID_CONTAINER_SHARE_ROOT": _storage_clean_path(get_setting("UNRAID_CONTAINER_SHARE_ROOT", UNRAID_CONTAINER_SHARE_ROOT), "/music"),
        "STORAGE_MAX_ACTIVE_DEVICES": int(max(1, min(64, _parse_int(get_setting("STORAGE_MAX_ACTIVE_DEVICES", STORAGE_MAX_ACTIVE_DEVICES), 1) or 1))),
        "STORAGE_SPINDOWN_POLICY": str(get_setting("STORAGE_SPINDOWN_POLICY", STORAGE_SPINDOWN_POLICY) or "none").strip().lower() or "none",
        "LIBRARY_WORKFLOW_MODE": workflow_state["mode"],
        "LIBRARY_SERVING_ROOT": workflow_state["serving_root"],
        "LIBRARY_INTAKE_ROOTS": _workflow_serialized_path_list(workflow_state["intake_roots"]),
        "LIBRARY_SOURCE_ROOTS": _workflow_serialized_path_list(workflow_state["source_roots"]),
        "LIBRARY_DUPES_ROOT": workflow_state["dupes_root"],
        "LIBRARY_INCOMPLETE_ROOT": workflow_state["incomplete_root"],
        "LIBRARY_MATERIALIZATION_MODE": workflow_state["materialization_mode"],
        "LIBRARY_INCLUDE_FORMAT_IN_FOLDER": bool(workflow_state["include_format"]),
        "LIBRARY_INCLUDE_TYPE_IN_FOLDER": bool(workflow_state["include_type"]),
        "FILES_TAG_WRITE_MODE": str(get_setting("FILES_TAG_WRITE_MODE", FILES_TAG_WRITE_MODE) or "full").strip().lower(),
        "LIBRARY_HAS_INTAKE": bool(workflow_state["has_intake"]),
        "LIBRARY_VISIBLE_SCOPES": list(workflow_state["visible_scopes"] or ["library", "inbox", "dupes"]),
        "LIBRARY_EFFECTIVE_SCAN_ROOTS": _workflow_serialized_path_list(workflow_scope_roots.get("scan_roots") or []),
        "LIBRARY_EFFECTIVE_LIBRARY_ROOTS": _workflow_serialized_path_list(workflow_scope_roots.get("library_roots") or []),
        "LIBRARY_EFFECTIVE_INBOX_ROOTS": _workflow_serialized_path_list(workflow_scope_roots.get("inbox_roots") or []),
        "LIBRARY_WINNER_PLACEMENT_STRATEGY": str(
            get_setting("LIBRARY_WINNER_PLACEMENT_STRATEGY", LIBRARY_WINNER_PLACEMENT_STRATEGY or "move")
            or "move"
        ).strip().lower(),
        "WINNER_SOURCE_ROOT_ID": str(
            get_setting("WINNER_SOURCE_ROOT_ID", WINNER_SOURCE_ROOT_ID or "")
            or ""
        ).strip(),
        "EXPORT_ROOT": get_setting("EXPORT_ROOT", EXPORT_ROOT),
        "EXPORT_NAMING_TEMPLATE": get_setting("EXPORT_NAMING_TEMPLATE", EXPORT_NAMING_TEMPLATE),
        "EXPORT_LINK_STRATEGY": get_setting("EXPORT_LINK_STRATEGY", EXPORT_LINK_STRATEGY),
        "EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER": get_setting_bool(
            "EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER",
            EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER,
        ),
        "EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER": get_setting_bool(
            "EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER",
            EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER,
        ),
        "MEDIA_CACHE_ROOT": get_setting("MEDIA_CACHE_ROOT", MEDIA_CACHE_ROOT),
        "ARTWORK_RAM_CACHE_MB": int(get_setting("ARTWORK_RAM_CACHE_MB", ARTWORK_RAM_CACHE_MB) or ARTWORK_RAM_CACHE_MB),
        "ARTWORK_RAM_CACHE_TTL_SEC": int(get_setting("ARTWORK_RAM_CACHE_TTL_SEC", ARTWORK_RAM_CACHE_TTL_SEC) or ARTWORK_RAM_CACHE_TTL_SEC),
        "ARTWORK_RAM_CACHE_MAX_ITEM_MB": int(get_setting("ARTWORK_RAM_CACHE_MAX_ITEM_MB", ARTWORK_RAM_CACHE_MAX_ITEM_MB) or ARTWORK_RAM_CACHE_MAX_ITEM_MB),
        "ARTWORK_RAM_CACHE_AUTO": get_setting_bool("ARTWORK_RAM_CACHE_AUTO", ARTWORK_RAM_CACHE_AUTO),
        "ARTWORK_RAM_CACHE_AUTO_MAX_MB": int(get_setting("ARTWORK_RAM_CACHE_AUTO_MAX_MB", ARTWORK_RAM_CACHE_AUTO_MAX_MB) or ARTWORK_RAM_CACHE_AUTO_MAX_MB),
        "ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC": int(get_setting("ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC", ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC) or ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC),
        "AUTO_EXPORT_LIBRARY": get_setting_bool("AUTO_EXPORT_LIBRARY", AUTO_EXPORT_LIBRARY),
        "paths_status": _paths_rw_status(),
        "container_mounts": _container_mounts_status(),
        # Concert discovery (UI filtering)
        "CONCERTS_FILTER_ENABLED": get_setting_bool("CONCERTS_FILTER_ENABLED", False),
        "CONCERTS_HOME_LAT": str(get_setting("CONCERTS_HOME_LAT", "") or "").strip(),
        "CONCERTS_HOME_LON": str(get_setting("CONCERTS_HOME_LON", "") or "").strip(),
        "CONCERTS_RADIUS_KM": str(get_setting("CONCERTS_RADIUS_KM", "150") or "").strip() or "150",
    }

    current_user = dict(getattr(g, "current_user", {}) or {})
    if not bool(current_user.get("is_admin")):
        # Non-admin users only need a very small read-only subset for Library UX.
        public_payload = {
            "configured": configured,
            "LIBRARY_MODE": payload.get("LIBRARY_MODE", "files"),
            "LIBRARY_INCLUDE_UNMATCHED": bool(payload.get("LIBRARY_INCLUDE_UNMATCHED", True)),
            "LIBRARY_WORKFLOW_MODE": payload.get("LIBRARY_WORKFLOW_MODE", "managed"),
            "LIBRARY_EFFECTIVE_LIBRARY_ROOTS": payload.get("LIBRARY_EFFECTIVE_LIBRARY_ROOTS", ""),
            "LIBRARY_EFFECTIVE_INBOX_ROOTS": payload.get("LIBRARY_EFFECTIVE_INBOX_ROOTS", ""),
            "LIBRARY_EFFECTIVE_SCAN_ROOTS": payload.get("LIBRARY_EFFECTIVE_SCAN_ROOTS", ""),
            "CONCERTS_FILTER_ENABLED": bool(payload.get("CONCERTS_FILTER_ENABLED", False)),
            "CONCERTS_HOME_LAT": str(payload.get("CONCERTS_HOME_LAT", "") or "").strip(),
            "CONCERTS_HOME_LON": str(payload.get("CONCERTS_HOME_LON", "") or "").strip(),
            "CONCERTS_RADIUS_KM": str(payload.get("CONCERTS_RADIUS_KM", "150") or "").strip() or "150",
        }
        return jsonify(public_payload)

    return jsonify(payload)


def _apply_settings_in_memory(updates: dict):
    """Apply saved settings to in-memory globals so they take effect without restart."""
    global PLEX_CONFIGURED  # declared once so it can be set in any of the Plex blocks below
    global _mb_queue, MB_PUBLIC_QUEUE_RPS, MB_MIRROR_QUEUE_RPS, MB_MIRROR_QUEUE_WORKERS
    mod = _runtime_module()
    updates, disabled_external_keys = _config_core.filter_disabled_external_updates(updates)
    if disabled_external_keys:
        logging.info(
            "Ignoring disabled external integration setting update(s): %s",
            ", ".join(sorted(disabled_external_keys)),
        )
        PLEX_CONFIGURED = False
    ai_keys = {"AI_PROVIDER", "OPENAI_API_KEY", "OPENAI_MODEL", "OPENAI_MODEL_FALLBACKS",
               "OPENAI_ENABLE_API_KEY_MODE", "OPENAI_ENABLE_CODEX_OAUTH_MODE",
               "ANTHROPIC_API_KEY", "GOOGLE_API_KEY", "OLLAMA_URL"}
    need_ai_reinit = bool(ai_keys & set(updates.keys()))

    if "MUSICBRAINZ_EMAIL" in updates:
        global MUSICBRAINZ_EMAIL
        MUSICBRAINZ_EMAIL = str(updates["MUSICBRAINZ_EMAIL"] or "")
        _configure_musicbrainz_client()
        logging.info("MusicBrainz client updated")
    if "MUSICBRAINZ_MIRROR_ENABLED" in updates:
        global MUSICBRAINZ_MIRROR_ENABLED
        MUSICBRAINZ_MIRROR_ENABLED = bool(_parse_bool(updates["MUSICBRAINZ_MIRROR_ENABLED"]))
        merged["MUSICBRAINZ_MIRROR_ENABLED"] = MUSICBRAINZ_MIRROR_ENABLED
        _configure_musicbrainz_client()
    if "MUSICBRAINZ_BASE_URL" in updates:
        global MUSICBRAINZ_BASE_URL
        MUSICBRAINZ_BASE_URL = str(updates["MUSICBRAINZ_BASE_URL"] or "").strip()
        merged["MUSICBRAINZ_BASE_URL"] = MUSICBRAINZ_BASE_URL
        _configure_musicbrainz_client()
    if "MUSICBRAINZ_MIRROR_NAME" in updates:
        global MUSICBRAINZ_MIRROR_NAME
        MUSICBRAINZ_MIRROR_NAME = str(updates["MUSICBRAINZ_MIRROR_NAME"] or "").strip()
        merged["MUSICBRAINZ_MIRROR_NAME"] = MUSICBRAINZ_MIRROR_NAME
    if "MUSICBRAINZ_RUNTIME_MODE" in updates:
        merged["MUSICBRAINZ_RUNTIME_MODE"] = str(updates["MUSICBRAINZ_RUNTIME_MODE"] or "").strip()
    if "MANAGED_RUNTIME_CONFIG_ROOT" in updates:
        merged["MANAGED_RUNTIME_CONFIG_ROOT"] = str(updates["MANAGED_RUNTIME_CONFIG_ROOT"] or "").strip()
    if "MANAGED_RUNTIME_DATA_ROOT" in updates:
        merged["MANAGED_RUNTIME_DATA_ROOT"] = str(updates["MANAGED_RUNTIME_DATA_ROOT"] or "").strip()
    if "MANAGED_MUSICBRAINZ_INSTALL_ROOT" in updates:
        merged["MANAGED_MUSICBRAINZ_INSTALL_ROOT"] = str(updates["MANAGED_MUSICBRAINZ_INSTALL_ROOT"] or "").strip()
    if "MANAGED_MUSICBRAINZ_UPDATE_ENABLED" in updates:
        merged["MANAGED_MUSICBRAINZ_UPDATE_ENABLED"] = bool(_parse_bool(updates["MANAGED_MUSICBRAINZ_UPDATE_ENABLED"]))
    if "MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS" in updates:
        try:
            merged["MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS"] = max(
                1,
                min(24 * 30, int(updates["MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS"])),
            )
        except (TypeError, ValueError):
            pass
    if "MB_PUBLIC_QUEUE_RPS" in updates:
        try:
            MB_PUBLIC_QUEUE_RPS = max(0.1, float(updates["MB_PUBLIC_QUEUE_RPS"] or 1.0))
            merged["MB_PUBLIC_QUEUE_RPS"] = MB_PUBLIC_QUEUE_RPS
        except (TypeError, ValueError):
            pass
        try:
            if _mb_queue is not None:
                _mb_queue.shutdown()
                _mb_queue = None
        except Exception:
            pass
    if "MB_MIRROR_QUEUE_RPS" in updates:
        try:
            MB_MIRROR_QUEUE_RPS = max(1.0, float(updates["MB_MIRROR_QUEUE_RPS"] or 12.0))
            merged["MB_MIRROR_QUEUE_RPS"] = MB_MIRROR_QUEUE_RPS
        except (TypeError, ValueError):
            pass
        try:
            if _mb_queue is not None:
                _mb_queue.shutdown()
                _mb_queue = None
        except Exception:
            pass
    if "MB_MIRROR_QUEUE_WORKERS" in updates:
        try:
            MB_MIRROR_QUEUE_WORKERS = max(1, min(32, int(updates["MB_MIRROR_QUEUE_WORKERS"] or 4)))
            merged["MB_MIRROR_QUEUE_WORKERS"] = MB_MIRROR_QUEUE_WORKERS
        except (TypeError, ValueError):
            pass
        try:
            if _mb_queue is not None:
                _mb_queue.shutdown()
                _mb_queue = None
        except Exception:
            pass
    if "USE_MUSICBRAINZ" in updates:
        global USE_MUSICBRAINZ
        USE_MUSICBRAINZ = bool(_parse_bool(updates["USE_MUSICBRAINZ"]))
    if "PLEX_HOST" in updates:
        global PLEX_HOST
        PLEX_HOST = str(updates["PLEX_HOST"] or "").strip().rstrip("/")
        merged["PLEX_HOST"] = PLEX_HOST
    if "PLEX_TOKEN" in updates:
        global PLEX_TOKEN
        PLEX_TOKEN = str(updates["PLEX_TOKEN"] or "").strip()
        merged["PLEX_TOKEN"] = PLEX_TOKEN
    if "MB_RETRY_NOT_FOUND" in updates:
        global MB_RETRY_NOT_FOUND
        MB_RETRY_NOT_FOUND = bool(_parse_bool(updates["MB_RETRY_NOT_FOUND"]))
    if "MB_SEARCH_ALBUM_TIMEOUT_SEC" in updates:
        global MB_SEARCH_ALBUM_TIMEOUT_SEC
        try:
            MB_SEARCH_ALBUM_TIMEOUT_SEC = max(0, min(3600, int(updates["MB_SEARCH_ALBUM_TIMEOUT_SEC"])))
        except (TypeError, ValueError):
            pass
    if "MB_CANDIDATE_FETCH_LIMIT" in updates:
        global MB_CANDIDATE_FETCH_LIMIT
        try:
            MB_CANDIDATE_FETCH_LIMIT = max(0, min(100, int(updates["MB_CANDIDATE_FETCH_LIMIT"])))
        except (TypeError, ValueError):
            pass
    if "MB_TRACKLIST_FETCH_LIMIT" in updates:
        global MB_TRACKLIST_FETCH_LIMIT
        try:
            MB_TRACKLIST_FETCH_LIMIT = max(0, min(100, int(updates["MB_TRACKLIST_FETCH_LIMIT"])))
        except (TypeError, ValueError):
            pass
    if "MB_FAST_FALLBACK_MODE" in updates:
        global MB_FAST_FALLBACK_MODE
        MB_FAST_FALLBACK_MODE = bool(_parse_bool(updates["MB_FAST_FALLBACK_MODE"]))
    if "PROVIDER_IDENTITY_STRICT" in updates:
        global PROVIDER_IDENTITY_STRICT
        PROVIDER_IDENTITY_STRICT = bool(_parse_bool(updates["PROVIDER_IDENTITY_STRICT"]))
    if "PROVIDER_IDENTITY_USE_AI" in updates:
        global PROVIDER_IDENTITY_USE_AI
        PROVIDER_IDENTITY_USE_AI = bool(_parse_bool(updates["PROVIDER_IDENTITY_USE_AI"]))
    if "MATCH_COVER_OCR_MODE" in updates:
        global MATCH_COVER_OCR_MODE
        MATCH_COVER_OCR_MODE = _normalize_match_cover_ocr_mode(updates["MATCH_COVER_OCR_MODE"])
        mod.merged["MATCH_COVER_OCR_MODE"] = MATCH_COVER_OCR_MODE
    if "PROVIDER_IDENTITY_MIN_SCORE" in updates:
        global PROVIDER_IDENTITY_MIN_SCORE
        try:
            PROVIDER_IDENTITY_MIN_SCORE = max(0.0, min(1.0, float(updates["PROVIDER_IDENTITY_MIN_SCORE"])))
        except (TypeError, ValueError):
            pass
    if "PROVIDER_IDENTITY_SCORE_MARGIN" in updates:
        global PROVIDER_IDENTITY_SCORE_MARGIN
        try:
            PROVIDER_IDENTITY_SCORE_MARGIN = max(0.0, min(1.0, float(updates["PROVIDER_IDENTITY_SCORE_MARGIN"])))
        except (TypeError, ValueError):
            pass
    if "PROVIDER_CACHE_FOUND_TTL_SEC" in updates:
        global PROVIDER_CACHE_FOUND_TTL_SEC
        try:
            PROVIDER_CACHE_FOUND_TTL_SEC = max(60, min(60 * 60 * 24 * 365, int(updates["PROVIDER_CACHE_FOUND_TTL_SEC"])))
        except (TypeError, ValueError):
            pass
    if "PROVIDER_CACHE_NOT_FOUND_TTL_SEC" in updates:
        global PROVIDER_CACHE_NOT_FOUND_TTL_SEC
        try:
            PROVIDER_CACHE_NOT_FOUND_TTL_SEC = max(60, min(60 * 60 * 24 * 30, int(updates["PROVIDER_CACHE_NOT_FOUND_TTL_SEC"])))
        except (TypeError, ValueError):
            pass
    if "PROVIDER_CACHE_ERROR_TTL_SEC" in updates:
        global PROVIDER_CACHE_ERROR_TTL_SEC
        try:
            PROVIDER_CACHE_ERROR_TTL_SEC = max(30, min(60 * 60 * 24, int(updates["PROVIDER_CACHE_ERROR_TTL_SEC"])))
        except (TypeError, ValueError):
            pass
    if "PROVIDER_GATEWAY_ENABLED" in updates:
        global PROVIDER_GATEWAY_ENABLED
        PROVIDER_GATEWAY_ENABLED = bool(_parse_bool(updates["PROVIDER_GATEWAY_ENABLED"]))
        merged["PROVIDER_GATEWAY_ENABLED"] = PROVIDER_GATEWAY_ENABLED
        _provider_gateway_reconfigure()
    if "PROVIDER_GATEWAY_CACHE_ENABLED" in updates:
        global PROVIDER_GATEWAY_CACHE_ENABLED
        PROVIDER_GATEWAY_CACHE_ENABLED = bool(_parse_bool(updates["PROVIDER_GATEWAY_CACHE_ENABLED"]))
        merged["PROVIDER_GATEWAY_CACHE_ENABLED"] = PROVIDER_GATEWAY_CACHE_ENABLED
    if "PROVIDER_GATEWAY_MAX_INFLIGHT" in updates:
        global PROVIDER_GATEWAY_MAX_INFLIGHT
        try:
            PROVIDER_GATEWAY_MAX_INFLIGHT = max(1, min(256, int(updates["PROVIDER_GATEWAY_MAX_INFLIGHT"])))
        except (TypeError, ValueError):
            pass
        merged["PROVIDER_GATEWAY_MAX_INFLIGHT"] = PROVIDER_GATEWAY_MAX_INFLIGHT
        _provider_gateway_reconfigure()
    if "PROVIDER_GATEWAY_DISCOGS_RPM" in updates:
        global PROVIDER_GATEWAY_DISCOGS_RPM
        try:
            PROVIDER_GATEWAY_DISCOGS_RPM = max(1, min(600, int(updates["PROVIDER_GATEWAY_DISCOGS_RPM"])))
        except (TypeError, ValueError):
            pass
        merged["PROVIDER_GATEWAY_DISCOGS_RPM"] = PROVIDER_GATEWAY_DISCOGS_RPM
    if "PROVIDER_GATEWAY_LASTFM_RPM" in updates:
        global PROVIDER_GATEWAY_LASTFM_RPM
        try:
            PROVIDER_GATEWAY_LASTFM_RPM = max(1, min(6000, int(updates["PROVIDER_GATEWAY_LASTFM_RPM"])))
        except (TypeError, ValueError):
            pass
        merged["PROVIDER_GATEWAY_LASTFM_RPM"] = PROVIDER_GATEWAY_LASTFM_RPM
    if "PROVIDER_GATEWAY_BANDCAMP_RPM" in updates:
        global PROVIDER_GATEWAY_BANDCAMP_RPM
        try:
            PROVIDER_GATEWAY_BANDCAMP_RPM = max(1, min(600, int(updates["PROVIDER_GATEWAY_BANDCAMP_RPM"])))
        except (TypeError, ValueError):
            pass
        merged["PROVIDER_GATEWAY_BANDCAMP_RPM"] = PROVIDER_GATEWAY_BANDCAMP_RPM
    if "AUTO_TUNE_ENABLED" in updates:
        global AUTO_TUNE_ENABLED
        AUTO_TUNE_ENABLED = bool(_parse_bool(updates["AUTO_TUNE_ENABLED"]))
        merged["AUTO_TUNE_ENABLED"] = AUTO_TUNE_ENABLED
        _start_runtime_auto_tune_worker()
    if "AUTO_TUNE_INTERVAL_SEC" in updates:
        global AUTO_TUNE_INTERVAL_SEC
        try:
            AUTO_TUNE_INTERVAL_SEC = max(15, min(900, int(updates["AUTO_TUNE_INTERVAL_SEC"])))
        except (TypeError, ValueError):
            pass
        merged["AUTO_TUNE_INTERVAL_SEC"] = AUTO_TUNE_INTERVAL_SEC
        _start_runtime_auto_tune_worker()
    if "AUTO_TUNE_MB_MIRROR_MIN_RPS" in updates:
        global AUTO_TUNE_MB_MIRROR_MIN_RPS
        try:
            AUTO_TUNE_MB_MIRROR_MIN_RPS = max(1.0, min(100.0, float(updates["AUTO_TUNE_MB_MIRROR_MIN_RPS"])))
        except (TypeError, ValueError):
            pass
        merged["AUTO_TUNE_MB_MIRROR_MIN_RPS"] = AUTO_TUNE_MB_MIRROR_MIN_RPS
    if "AUTO_TUNE_MB_MIRROR_MAX_RPS" in updates:
        global AUTO_TUNE_MB_MIRROR_MAX_RPS
        try:
            AUTO_TUNE_MB_MIRROR_MAX_RPS = max(1.0, min(100.0, float(updates["AUTO_TUNE_MB_MIRROR_MAX_RPS"])))
        except (TypeError, ValueError):
            pass
        merged["AUTO_TUNE_MB_MIRROR_MAX_RPS"] = AUTO_TUNE_MB_MIRROR_MAX_RPS
    if "AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN" in updates:
        global AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN
        try:
            AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN = max(1, min(256, int(updates["AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN"])))
        except (TypeError, ValueError):
            pass
        merged["AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN"] = AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN
    if "AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP" in updates:
        global AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP
        try:
            AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP = max(1, min(256, int(updates["AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP"])))
        except (TypeError, ValueError):
            pass
        merged["AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP"] = AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP
    if "SCAN_DISABLE_CACHE" in updates:
        global SCAN_DISABLE_CACHE
        SCAN_DISABLE_CACHE = bool(_parse_bool(updates["SCAN_DISABLE_CACHE"]))
        logging.info("SCAN_DISABLE_CACHE updated in memory: %s", SCAN_DISABLE_CACHE)
    # Recompute MB_DISABLE_CACHE so that SCAN_DISABLE_CACHE also forces full MB lookups.
    if "MB_DISABLE_CACHE" in updates or "SCAN_DISABLE_CACHE" in updates:
        global MB_DISABLE_CACHE
        current_mb_disable = MB_DISABLE_CACHE
        if "MB_DISABLE_CACHE" in updates:
            current_mb_disable = bool(_parse_bool(updates["MB_DISABLE_CACHE"]))
        MB_DISABLE_CACHE = bool(current_mb_disable or SCAN_DISABLE_CACHE)
        logging.info(
            "MB_DISABLE_CACHE updated in memory (effective): MB_DISABLE_CACHE=%s, SCAN_DISABLE_CACHE=%s",
            MB_DISABLE_CACHE,
            SCAN_DISABLE_CACHE,
        )
    if "USE_AI_FOR_MB_MATCH" in updates:
        global USE_AI_FOR_MB_MATCH
        USE_AI_FOR_MB_MATCH = bool(_parse_bool(updates["USE_AI_FOR_MB_MATCH"]))
    if "USE_AI_FOR_MB_VERIFY" in updates:
        global USE_AI_FOR_MB_VERIFY
        USE_AI_FOR_MB_VERIFY = bool(_parse_bool(updates["USE_AI_FOR_MB_VERIFY"]))
    if "USE_AI_FOR_DEDUPE" in updates:
        global USE_AI_FOR_DEDUPE
        USE_AI_FOR_DEDUPE = bool(_parse_bool(updates["USE_AI_FOR_DEDUPE"]))
    if "USE_AI_FOR_SOFT_MATCH_PROFILES" in updates:
        global USE_AI_FOR_SOFT_MATCH_PROFILES
        USE_AI_FOR_SOFT_MATCH_PROFILES = bool(_parse_bool(updates["USE_AI_FOR_SOFT_MATCH_PROFILES"]))
    if "USE_AI_VISION_FOR_COVER" in updates:
        global USE_AI_VISION_FOR_COVER
        USE_AI_VISION_FOR_COVER = bool(_parse_bool(updates["USE_AI_VISION_FOR_COVER"]))
    if "AI_CONFIDENCE_MIN" in updates:
        global AI_CONFIDENCE_MIN
        try:
            v = updates["AI_CONFIDENCE_MIN"]
            AI_CONFIDENCE_MIN = max(0, min(100, int(v))) if v is not None and str(v).strip().isdigit() else 50
        except (TypeError, ValueError):
            AI_CONFIDENCE_MIN = 50
    if "OPENAI_VISION_MODEL" in updates:
        global OPENAI_VISION_MODEL
        OPENAI_VISION_MODEL = str(updates.get("OPENAI_VISION_MODEL") or "").strip()
    if "USE_AI_VISION_BEFORE_COVER_INJECT" in updates:
        global USE_AI_VISION_BEFORE_COVER_INJECT
        USE_AI_VISION_BEFORE_COVER_INJECT = bool(_parse_bool(updates["USE_AI_VISION_BEFORE_COVER_INJECT"]))
    if "USE_WEB_SEARCH_FOR_MB" in updates:
        global USE_WEB_SEARCH_FOR_MB
        USE_WEB_SEARCH_FOR_MB = bool(_parse_bool(updates["USE_WEB_SEARCH_FOR_MB"]))
    if "WEB_SEARCH_PROVIDER" in updates:
        global WEB_SEARCH_PROVIDER
        WEB_SEARCH_PROVIDER = _normalize_web_search_provider(updates["WEB_SEARCH_PROVIDER"])
    if "USE_AI_WEB_SEARCH_FALLBACK" in updates:
        global USE_AI_WEB_SEARCH_FALLBACK
        USE_AI_WEB_SEARCH_FALLBACK = bool(_parse_bool(updates["USE_AI_WEB_SEARCH_FALLBACK"]))
    if "SCHEDULER_ALLOW_NON_SCAN_JOBS" in updates:
        global SCHEDULER_ALLOW_NON_SCAN_JOBS
        SCHEDULER_ALLOW_NON_SCAN_JOBS = bool(_parse_bool(updates["SCHEDULER_ALLOW_NON_SCAN_JOBS"]))
    if "AI_MAX_CALLS_PER_SCAN" in updates:
        global AI_MAX_CALLS_PER_SCAN
        AI_MAX_CALLS_PER_SCAN = 0
    if "AI_CALL_COOLDOWN_SEC" in updates:
        global AI_CALL_COOLDOWN_SEC
        AI_CALL_COOLDOWN_SEC = 0.0
    if "AI_GLOBAL_MAX_CALLS_PER_MINUTE" in updates:
        global AI_GLOBAL_MAX_CALLS_PER_MINUTE
        AI_GLOBAL_MAX_CALLS_PER_MINUTE = 0
    if "AI_GLOBAL_MAX_CALLS_PER_DAY" in updates:
        global AI_GLOBAL_MAX_CALLS_PER_DAY
        AI_GLOBAL_MAX_CALLS_PER_DAY = 0
    if "AI_USAGE_LEVEL" in updates:
        global AI_USAGE_LEVEL
        AI_USAGE_LEVEL = _normalize_ai_usage_level(str(updates.get("AI_USAGE_LEVEL") or "auto"))
        _apply_ai_usage_level(AI_USAGE_LEVEL)
        logging.info("AI_USAGE_LEVEL updated in memory: %s", AI_USAGE_LEVEL)
    if "SCAN_AI_POLICY" in updates:
        global SCAN_AI_POLICY
        SCAN_AI_POLICY = _normalize_scan_ai_policy(str(updates.get("SCAN_AI_POLICY") or "local_only"))
        merged["SCAN_AI_POLICY"] = SCAN_AI_POLICY
        logging.info("SCAN_AI_POLICY updated in memory: %s", SCAN_AI_POLICY)
    if "SCAN_PAID_PROVIDER_ORDER" in updates:
        global SCAN_PAID_PROVIDER_ORDER
        SCAN_PAID_PROVIDER_ORDER = ",".join(
            _normalize_ordered_values(
                updates.get("SCAN_PAID_PROVIDER_ORDER"),
                allowed=("openai-api", "openai-codex", "anthropic", "google"),
                default=("openai-api", "openai-codex", "anthropic", "google"),
            )
        )
        merged["SCAN_PAID_PROVIDER_ORDER"] = SCAN_PAID_PROVIDER_ORDER
        logging.info("SCAN_PAID_PROVIDER_ORDER updated in memory: %s", SCAN_PAID_PROVIDER_ORDER)
    if "WEB_SEARCH_LOCAL_ORDER" in updates:
        global WEB_SEARCH_LOCAL_ORDER
        WEB_SEARCH_LOCAL_ORDER = ",".join(
            _normalize_ordered_values(
                updates.get("WEB_SEARCH_LOCAL_ORDER"),
                allowed=("serper",),
                default=("serper",),
            )
        )
        merged["WEB_SEARCH_LOCAL_ORDER"] = WEB_SEARCH_LOCAL_ORDER
        logging.info("WEB_SEARCH_LOCAL_ORDER updated in memory: %s", WEB_SEARCH_LOCAL_ORDER)
    if "SERPER_API_KEY" in updates:
        global SERPER_API_KEY
        v = str(updates.get("SERPER_API_KEY") or "").strip()
        if not _is_masked_secret_placeholder(v):
            SERPER_API_KEY = v
    if "USE_ACOUSTID" in updates:
        global USE_ACOUSTID
        USE_ACOUSTID = bool(_parse_bool(updates["USE_ACOUSTID"]))
    if "ACOUSTID_API_KEY" in updates:
        global ACOUSTID_API_KEY
        v = str(updates.get("ACOUSTID_API_KEY") or "").strip()
        if not _is_masked_secret_placeholder(v):
            ACOUSTID_API_KEY = v
    if "USE_ACOUSTID_WHEN_TAGGED" in updates:
        global USE_ACOUSTID_WHEN_TAGGED
        USE_ACOUSTID_WHEN_TAGGED = bool(_parse_bool(updates["USE_ACOUSTID_WHEN_TAGGED"]))
    if "ARTIST_CREDIT_MODE" in updates:
        global ARTIST_CREDIT_MODE
        ARTIST_CREDIT_MODE = str(updates.get("ARTIST_CREDIT_MODE") or "album_artist_strict").strip().lower()
        logging.info("ARTIST_CREDIT_MODE updated in memory: %s", ARTIST_CREDIT_MODE)
    if "CLASSICAL_NAME_PREFERENCE" in updates:
        global CLASSICAL_NAME_PREFERENCE
        CLASSICAL_NAME_PREFERENCE = _normalize_classical_name_preference(updates.get("CLASSICAL_NAME_PREFERENCE"))
        merged["CLASSICAL_NAME_PREFERENCE"] = CLASSICAL_NAME_PREFERENCE
        logging.info("CLASSICAL_NAME_PREFERENCE updated in memory: %s", CLASSICAL_NAME_PREFERENCE)
    if "LIVE_DEDUPE_MODE" in updates:
        global LIVE_DEDUPE_MODE
        LIVE_DEDUPE_MODE = str(updates.get("LIVE_DEDUPE_MODE") or "safe").strip().lower()
        logging.info("LIVE_DEDUPE_MODE updated in memory: %s", LIVE_DEDUPE_MODE)
    if "SCAN_THREADS" in updates:
        global SCAN_THREADS
        v = updates["SCAN_THREADS"]
        if isinstance(v, str) and str(v).strip().lower() == "auto":
            SCAN_THREADS = max(1, os.cpu_count() or 4)
        else:
            try:
                SCAN_THREADS = max(1, int(v))
            except (ValueError, TypeError):
                pass
        logging.info("SCAN_THREADS updated in memory: %s", SCAN_THREADS)
    if "FFPROBE_POOL_SIZE" in updates:
        global FFPROBE_POOL_SIZE, _ffprobe_pool
        try:
            FFPROBE_POOL_SIZE = max(1, min(64, int(updates["FFPROBE_POOL_SIZE"])))
            _ffprobe_pool = None  # Next scan will create pool with new size
        except (ValueError, TypeError):
            pass
        logging.info("FFPROBE_POOL_SIZE updated in memory: %s", FFPROBE_POOL_SIZE)
    # Library backend & file-library settings
    if "LIBRARY_MODE" in updates:
        mode = str(updates["LIBRARY_MODE"] or "files").strip().lower()
        if mode != "files":
            logging.warning("Ignoring unsupported LIBRARY_MODE '%s' (PMDA is files-only in this build)", mode)
        else:
            global LIBRARY_MODE
            LIBRARY_MODE = mode
            mod.merged["LIBRARY_MODE"] = mode
            logging.info("LIBRARY_MODE updated in memory: %s", mode)
            if mode == "files":
                _trigger_files_index_rebuild_async(reason="settings_library_mode_files")
            _request_files_watcher_reconcile("settings_library_mode")
    if "LIBRARY_INCLUDE_UNMATCHED" in updates:
        global LIBRARY_INCLUDE_UNMATCHED
        LIBRARY_INCLUDE_UNMATCHED = bool(_parse_bool(updates["LIBRARY_INCLUDE_UNMATCHED"]))
        mod.merged["LIBRARY_INCLUDE_UNMATCHED"] = LIBRARY_INCLUDE_UNMATCHED
        logging.info("LIBRARY_INCLUDE_UNMATCHED updated in memory: %s", LIBRARY_INCLUDE_UNMATCHED)
    if "FILES_ROOTS" in updates:
        roots = _parse_files_roots(updates["FILES_ROOTS"])
        global FILES_ROOTS
        FILES_ROOTS = roots
        mod.merged["FILES_ROOTS"] = roots
        logging.info("FILES_ROOTS updated in memory: %s", FILES_ROOTS)
        if _get_library_mode() == "files":
            _trigger_files_index_rebuild_async(reason="settings_files_roots")
        _request_files_watcher_reconcile("settings_files_roots")
    storage_updates = _config_core.normalize_storage_power_saver_settings(updates)
    if storage_updates:
        global STORAGE_POWER_SAVER_ENABLED, STORAGE_PROVIDER, UNRAID_HOST_MNT_ROOT, UNRAID_USER_SHARE_HOST_ROOT
        global UNRAID_CONTAINER_SHARE_ROOT, STORAGE_MAX_ACTIVE_DEVICES, STORAGE_SPINDOWN_POLICY
        if "STORAGE_POWER_SAVER_ENABLED" in storage_updates:
            STORAGE_POWER_SAVER_ENABLED = bool(storage_updates["STORAGE_POWER_SAVER_ENABLED"])
            mod.merged["STORAGE_POWER_SAVER_ENABLED"] = STORAGE_POWER_SAVER_ENABLED
        if "STORAGE_PROVIDER" in storage_updates:
            STORAGE_PROVIDER = str(storage_updates["STORAGE_PROVIDER"])
            mod.merged["STORAGE_PROVIDER"] = STORAGE_PROVIDER
        if "UNRAID_HOST_MNT_ROOT" in storage_updates:
            UNRAID_HOST_MNT_ROOT = str(storage_updates["UNRAID_HOST_MNT_ROOT"])
            mod.merged["UNRAID_HOST_MNT_ROOT"] = UNRAID_HOST_MNT_ROOT
        if "UNRAID_USER_SHARE_HOST_ROOT" in storage_updates:
            UNRAID_USER_SHARE_HOST_ROOT = str(storage_updates["UNRAID_USER_SHARE_HOST_ROOT"])
            mod.merged["UNRAID_USER_SHARE_HOST_ROOT"] = UNRAID_USER_SHARE_HOST_ROOT
        if "UNRAID_CONTAINER_SHARE_ROOT" in storage_updates:
            UNRAID_CONTAINER_SHARE_ROOT = str(storage_updates["UNRAID_CONTAINER_SHARE_ROOT"])
            mod.merged["UNRAID_CONTAINER_SHARE_ROOT"] = UNRAID_CONTAINER_SHARE_ROOT
        if "STORAGE_MAX_ACTIVE_DEVICES" in storage_updates:
            STORAGE_MAX_ACTIVE_DEVICES = int(storage_updates["STORAGE_MAX_ACTIVE_DEVICES"])
            mod.merged["STORAGE_MAX_ACTIVE_DEVICES"] = STORAGE_MAX_ACTIVE_DEVICES
        if "STORAGE_SPINDOWN_POLICY" in storage_updates:
            STORAGE_SPINDOWN_POLICY = str(storage_updates["STORAGE_SPINDOWN_POLICY"])
            mod.merged["STORAGE_SPINDOWN_POLICY"] = STORAGE_SPINDOWN_POLICY
        logging.info(
            "[STORAGE] power saver settings updated: enabled=%s provider=%s host=%s user_share=%s container=%s max_active=%s",
            STORAGE_POWER_SAVER_ENABLED,
            STORAGE_PROVIDER,
            UNRAID_HOST_MNT_ROOT,
            UNRAID_USER_SHARE_HOST_ROOT,
            UNRAID_CONTAINER_SHARE_ROOT,
            STORAGE_MAX_ACTIVE_DEVICES,
        )
    if "LIBRARY_WINNER_PLACEMENT_STRATEGY" in updates:
        global LIBRARY_WINNER_PLACEMENT_STRATEGY
        strat = str(updates["LIBRARY_WINNER_PLACEMENT_STRATEGY"] or "move").strip().lower()
        if strat not in {"move", "hardlink", "symlink", "copy"}:
            strat = "move"
        LIBRARY_WINNER_PLACEMENT_STRATEGY = strat
        mod.merged["LIBRARY_WINNER_PLACEMENT_STRATEGY"] = strat
        logging.info("LIBRARY_WINNER_PLACEMENT_STRATEGY updated in memory: %s", strat)
    if "WINNER_SOURCE_ROOT_ID" in updates:
        global WINNER_SOURCE_ROOT_ID
        try:
            WINNER_SOURCE_ROOT_ID = str(max(0, int(updates["WINNER_SOURCE_ROOT_ID"] or 0)))
        except Exception:
            WINNER_SOURCE_ROOT_ID = ""
        mod.merged["WINNER_SOURCE_ROOT_ID"] = WINNER_SOURCE_ROOT_ID
        logging.info("WINNER_SOURCE_ROOT_ID updated in memory: %s", WINNER_SOURCE_ROOT_ID or "none")
    if "EXPORT_ROOT" in updates:
        root = str(updates["EXPORT_ROOT"] or "").strip()
        global EXPORT_ROOT
        EXPORT_ROOT = root
        mod.merged["EXPORT_ROOT"] = root
        logging.info("EXPORT_ROOT updated in memory: %s", EXPORT_ROOT)
    if "EXPORT_NAMING_TEMPLATE" in updates:
        tpl = str(updates["EXPORT_NAMING_TEMPLATE"] or "").strip()
        global EXPORT_NAMING_TEMPLATE
        EXPORT_NAMING_TEMPLATE = tpl
        mod.merged["EXPORT_NAMING_TEMPLATE"] = tpl
        logging.info("EXPORT_NAMING_TEMPLATE updated in memory")
    if "EXPORT_LINK_STRATEGY" in updates:
        strat = str(updates["EXPORT_LINK_STRATEGY"] or "hardlink").strip().lower()
        if strat not in {"hardlink", "symlink", "copy", "move"}:
            logging.warning("Ignoring invalid EXPORT_LINK_STRATEGY '%s' (expected hardlink/symlink/copy/move)", strat)
        else:
            global EXPORT_LINK_STRATEGY
            EXPORT_LINK_STRATEGY = strat
            mod.merged["EXPORT_LINK_STRATEGY"] = strat
            logging.info("EXPORT_LINK_STRATEGY updated in memory: %s", EXPORT_LINK_STRATEGY)
    if "EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER" in updates:
        global EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER
        EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER = bool(_parse_bool(updates["EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER"]))
        mod.merged["EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER"] = EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER
        logging.info(
            "EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER updated in memory: %s",
            EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER,
        )
    if "EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER" in updates:
        global EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER
        EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER = bool(_parse_bool(updates["EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER"]))
        mod.merged["EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER"] = EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER
        logging.info(
            "EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER updated in memory: %s",
            EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER,
        )
    if "MEDIA_CACHE_ROOT" in updates:
        root = str(updates["MEDIA_CACHE_ROOT"] or "").strip() or str(CONFIG_DIR / "media_cache")
        global MEDIA_CACHE_ROOT
        MEDIA_CACHE_ROOT = root
        mod.merged["MEDIA_CACHE_ROOT"] = root
        try:
            (Path(MEDIA_CACHE_ROOT) / "album").mkdir(parents=True, exist_ok=True)
            (Path(MEDIA_CACHE_ROOT) / "artist").mkdir(parents=True, exist_ok=True)
            logging.info("MEDIA_CACHE_ROOT updated in memory: %s", MEDIA_CACHE_ROOT)
        except Exception as e:
            logging.warning("Could not initialize MEDIA_CACHE_ROOT=%s: %s", MEDIA_CACHE_ROOT, e)
    if any(
        k in updates
        for k in (
            "ARTWORK_RAM_CACHE_MB",
            "ARTWORK_RAM_CACHE_TTL_SEC",
            "ARTWORK_RAM_CACHE_MAX_ITEM_MB",
            "ARTWORK_RAM_CACHE_AUTO",
            "ARTWORK_RAM_CACHE_AUTO_MAX_MB",
            "ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC",
        )
    ):
        next_mb: Optional[int] = None
        next_ttl: Optional[int] = None
        next_item_mb: Optional[int] = None
        global ARTWORK_RAM_CACHE_AUTO, ARTWORK_RAM_CACHE_AUTO_MAX_MB, ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC
        if "ARTWORK_RAM_CACHE_MB" in updates:
            try:
                next_mb = max(0, min(65536, int(updates["ARTWORK_RAM_CACHE_MB"])))
            except (ValueError, TypeError):
                next_mb = ARTWORK_RAM_CACHE_MB
            mod.merged["ARTWORK_RAM_CACHE_MB"] = next_mb
        if "ARTWORK_RAM_CACHE_TTL_SEC" in updates:
            try:
                next_ttl = max(60, min(60 * 60 * 24 * 30, int(updates["ARTWORK_RAM_CACHE_TTL_SEC"])))
            except (ValueError, TypeError):
                next_ttl = ARTWORK_RAM_CACHE_TTL_SEC
            mod.merged["ARTWORK_RAM_CACHE_TTL_SEC"] = next_ttl
        if "ARTWORK_RAM_CACHE_MAX_ITEM_MB" in updates:
            try:
                next_item_mb = max(1, min(64, int(updates["ARTWORK_RAM_CACHE_MAX_ITEM_MB"])))
            except (ValueError, TypeError):
                next_item_mb = ARTWORK_RAM_CACHE_MAX_ITEM_MB
            mod.merged["ARTWORK_RAM_CACHE_MAX_ITEM_MB"] = next_item_mb
        if "ARTWORK_RAM_CACHE_AUTO" in updates:
            ARTWORK_RAM_CACHE_AUTO = bool(_parse_bool(updates["ARTWORK_RAM_CACHE_AUTO"]))
            mod.merged["ARTWORK_RAM_CACHE_AUTO"] = ARTWORK_RAM_CACHE_AUTO
        if "ARTWORK_RAM_CACHE_AUTO_MAX_MB" in updates:
            try:
                ARTWORK_RAM_CACHE_AUTO_MAX_MB = max(0, min(65536, int(updates["ARTWORK_RAM_CACHE_AUTO_MAX_MB"])))
            except (ValueError, TypeError):
                pass
            mod.merged["ARTWORK_RAM_CACHE_AUTO_MAX_MB"] = ARTWORK_RAM_CACHE_AUTO_MAX_MB
        if "ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC" in updates:
            try:
                ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC = max(30, min(3600, int(updates["ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC"])))
            except (ValueError, TypeError):
                pass
            mod.merged["ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC"] = ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC
        _reconfigure_artwork_ram_cache(cache_mb=next_mb, ttl_sec=next_ttl, item_mb=next_item_mb)
        _start_artwork_ram_cache_auto_worker()
        if ARTWORK_RAM_CACHE_AUTO:
            applied = _apply_auto_artwork_ram_target(force=True)
            logging.info(
                "Artwork RAM auto-tune applied: %dMB (cap=%s interval=%ss)",
                applied,
                _artwork_auto_cap_log_label(),
                ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC,
            )
        logging.info(
            "Artwork RAM cache updated in memory: max=%dMB ttl=%ds item_max=%dMB auto=%s cap=%s interval=%ss",
            ARTWORK_RAM_CACHE_MB,
            ARTWORK_RAM_CACHE_TTL_SEC,
            ARTWORK_RAM_CACHE_MAX_ITEM_MB,
            ARTWORK_RAM_CACHE_AUTO,
            _artwork_auto_cap_log_label(),
            ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC,
        )
    if "IMPROVE_ALL_WORKERS" in updates:
        global IMPROVE_ALL_WORKERS
        try:
            IMPROVE_ALL_WORKERS = max(1, min(8, int(updates["IMPROVE_ALL_WORKERS"])))
        except (ValueError, TypeError):
            pass
        logging.info("IMPROVE_ALL_WORKERS updated in memory: %s", IMPROVE_ALL_WORKERS)
    if "LOG_LEVEL" in updates:
        global LOG_LEVEL
        LOG_LEVEL = str(updates["LOG_LEVEL"] or "INFO").upper()
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    if "DISCORD_WEBHOOK" in updates:
        v = str(updates["DISCORD_WEBHOOK"] or "").strip()
        if not _is_masked_secret_placeholder(v):
            global DISCORD_WEBHOOK
            DISCORD_WEBHOOK = v
    if "SKIP_FOLDERS" in updates:
        global SKIP_FOLDERS
        v = updates["SKIP_FOLDERS"]
        if isinstance(v, list):
            SKIP_FOLDERS = [str(p).strip() for p in v if str(p).strip()]
        else:
            SKIP_FOLDERS = [p.strip() for p in str(v or "").split(",") if p.strip()]
    if "REQUIRED_TAGS" in updates:
        global REQUIRED_TAGS
        REQUIRED_TAGS = _parse_required_tags(updates["REQUIRED_TAGS"]) if updates["REQUIRED_TAGS"] else []
    if "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD" in updates:
        global BROKEN_ALBUM_CONSECUTIVE_THRESHOLD
        try:
            BROKEN_ALBUM_CONSECUTIVE_THRESHOLD = int(updates["BROKEN_ALBUM_CONSECUTIVE_THRESHOLD"])
        except (ValueError, TypeError):
            pass
    if "BROKEN_ALBUM_PERCENTAGE_THRESHOLD" in updates:
        global BROKEN_ALBUM_PERCENTAGE_THRESHOLD
        try:
            BROKEN_ALBUM_PERCENTAGE_THRESHOLD = float(updates["BROKEN_ALBUM_PERCENTAGE_THRESHOLD"])
        except (ValueError, TypeError):
            pass
    if "AUTO_MOVE_DUPES" in updates:
        global AUTO_MOVE_DUPES
        AUTO_MOVE_DUPES = bool(_parse_bool(updates["AUTO_MOVE_DUPES"]))
    for _pipeline_key, _pipeline_value in _config_core.normalize_pipeline_bool_settings(updates).items():
        setattr(mod, _pipeline_key, _pipeline_value)
        mod.merged[_pipeline_key] = _pipeline_value
    if "PIPELINE_PLAYER_TARGET" in updates:
        global PIPELINE_PLAYER_TARGET
        PIPELINE_PLAYER_TARGET = _normalize_player_target(updates.get("PIPELINE_PLAYER_TARGET"))
        mod.merged["PIPELINE_PLAYER_TARGET"] = PIPELINE_PLAYER_TARGET
        logging.info("PIPELINE_PLAYER_TARGET updated in memory: %s", PIPELINE_PLAYER_TARGET)
    task_notification_updates = _config_core.normalize_task_notification_settings(updates)
    for _task_key, _task_value in task_notification_updates.items():
        setattr(mod, _task_key, _task_value)
        mod.merged[_task_key] = _task_value
    if "AUTO_EXPORT_LIBRARY" in updates:
        global AUTO_EXPORT_LIBRARY
        AUTO_EXPORT_LIBRARY = bool(_parse_bool(updates["AUTO_EXPORT_LIBRARY"]))
    for _metadata_key, _metadata_value in _config_core.normalize_metadata_worker_settings(updates).items():
        setattr(mod, _metadata_key, _metadata_value)
        merged[_metadata_key] = _metadata_value
    if "BACKUP_BEFORE_FIX" in updates:
        global BACKUP_BEFORE_FIX
        BACKUP_BEFORE_FIX = bool(_parse_bool(updates["BACKUP_BEFORE_FIX"]))
    if "MAGIC_MODE" in updates:
        global MAGIC_MODE
        MAGIC_MODE = bool(_parse_bool(updates["MAGIC_MODE"]))
    if "REPROCESS_INCOMPLETE_ALBUMS" in updates:
        global REPROCESS_INCOMPLETE_ALBUMS
        REPROCESS_INCOMPLETE_ALBUMS = bool(_parse_bool(updates["REPROCESS_INCOMPLETE_ALBUMS"]))
    if "AUTO_FIX_BROKEN_ALBUMS" in updates:
        global AUTO_FIX_BROKEN_ALBUMS
        AUTO_FIX_BROKEN_ALBUMS = bool(_parse_bool(updates["AUTO_FIX_BROKEN_ALBUMS"]))
    if "DISABLE_PATH_CROSSCHECK" in updates:
        global DISABLE_PATH_CROSSCHECK
        DISABLE_PATH_CROSSCHECK = bool(_parse_bool(updates["DISABLE_PATH_CROSSCHECK"]))
    if "PATH_MAP" in updates:
        global PATH_MAP
        PATH_MAP = _parse_path_map(updates["PATH_MAP"])
        logging.info("PATH_MAP updated in memory (%d entries)", len(PATH_MAP))
    if "DUPE_ROOT" in updates:
        global DUPE_ROOT
        DUPE_ROOT = Path(str(updates["DUPE_ROOT"] or "").strip() or "/dupes")
        logging.info("DUPE_ROOT updated in memory: %s", DUPE_ROOT)
    if "MUSIC_PARENT_PATH" in updates:
        merged["MUSIC_PARENT_PATH"] = str(updates["MUSIC_PARENT_PATH"] or "").strip() or ""
        logging.info("MUSIC_PARENT_PATH updated in memory")
    if "LOG_FILE" in updates:
        global LOG_FILE
        LOG_FILE = str(updates["LOG_FILE"] or "").strip() or str(CONFIG_DIR / "pmda.log")
        logging.info("LOG_FILE updated in memory: %s", LOG_FILE)
    if "CROSS_LIBRARY_DEDUPE" in updates:
        global CROSS_LIBRARY_DEDUPE
        CROSS_LIBRARY_DEDUPE = bool(_parse_bool(updates["CROSS_LIBRARY_DEDUPE"]))
        logging.info("CROSS_LIBRARY_DEDUPE updated in memory: %s", CROSS_LIBRARY_DEDUPE)
    if "CROSSCHECK_SAMPLES" in updates:
        global CROSSCHECK_SAMPLES
        try:
            CROSSCHECK_SAMPLES = max(0, int(updates["CROSSCHECK_SAMPLES"]))
        except (ValueError, TypeError):
            CROSSCHECK_SAMPLES = 20
        logging.info("CROSSCHECK_SAMPLES updated in memory: %s", CROSSCHECK_SAMPLES)
    if "FORMAT_PREFERENCE" in updates:
        global FORMAT_PREFERENCE, FMT_SCORE
        FORMAT_PREFERENCE = _parse_format_preference_early(updates["FORMAT_PREFERENCE"])
        FMT_SCORE = {ext: len(FORMAT_PREFERENCE) - i for i, ext in enumerate(FORMAT_PREFERENCE)}
        logging.info("FORMAT_PREFERENCE updated in memory (%d formats)", len(FORMAT_PREFERENCE))
    if "OPENAI_MODEL_FALLBACKS" in updates:
        merged["OPENAI_MODEL_FALLBACKS"] = str(updates["OPENAI_MODEL_FALLBACKS"] or "").strip()
        logging.info("OPENAI_MODEL_FALLBACKS updated in memory")
    if "INCOMPLETE_ALBUMS_TARGET_DIR" in updates:
        target_dir = (updates.get("INCOMPLETE_ALBUMS_TARGET_DIR") or "").strip() or "/dupes/incomplete_albums"
        try:
            Path(target_dir).mkdir(parents=True, exist_ok=True)
            logging.debug("Ensured quarantine folder for incomplete albums exists: %s", target_dir)
        except Exception as e:
            logging.warning("Could not create incomplete albums target dir %s: %s", target_dir, e)

    # Metadata fallback providers (Discogs, Last.fm, Bandcamp)
    if "USE_DISCOGS" in updates:
        mod.USE_DISCOGS = bool(_parse_bool(updates["USE_DISCOGS"]))
        mod.merged["USE_DISCOGS"] = mod.USE_DISCOGS
    if "USE_ITUNES" in updates:
        mod.USE_ITUNES = bool(_parse_bool(updates["USE_ITUNES"]))
        mod.merged["USE_ITUNES"] = mod.USE_ITUNES
    if "USE_DEEZER" in updates:
        mod.USE_DEEZER = bool(_parse_bool(updates["USE_DEEZER"]))
        mod.merged["USE_DEEZER"] = mod.USE_DEEZER
    if "USE_SPOTIFY" in updates:
        mod.USE_SPOTIFY = bool(_parse_bool(updates["USE_SPOTIFY"]))
        mod.merged["USE_SPOTIFY"] = mod.USE_SPOTIFY
    if "USE_QOBUZ" in updates:
        mod.USE_QOBUZ = bool(_parse_bool(updates["USE_QOBUZ"]))
        mod.merged["USE_QOBUZ"] = mod.USE_QOBUZ
    if "USE_TIDAL" in updates:
        mod.USE_TIDAL = bool(_parse_bool(updates["USE_TIDAL"]))
        mod.merged["USE_TIDAL"] = mod.USE_TIDAL
    if "DISCOGS_USER_TOKEN" in updates:
        v = str(updates["DISCOGS_USER_TOKEN"] or "").strip()
        if not _is_masked_secret_placeholder(v):
            mod.DISCOGS_USER_TOKEN = v
    if "USE_LASTFM" in updates:
        mod.USE_LASTFM = bool(_parse_bool(updates["USE_LASTFM"]))
        mod.merged["USE_LASTFM"] = mod.USE_LASTFM
    if "LASTFM_API_KEY" in updates:
        v = str(updates["LASTFM_API_KEY"] or "").strip()
        if not _is_masked_secret_placeholder(v):
            mod.LASTFM_API_KEY = v
    if "LASTFM_API_SECRET" in updates:
        v = str(updates["LASTFM_API_SECRET"] or "").strip()
        if not _is_masked_secret_placeholder(v):
            mod.LASTFM_API_SECRET = v
    if "LASTFM_SCROBBLE_ENABLED" in updates:
        mod.LASTFM_SCROBBLE_ENABLED = bool(_parse_bool(updates["LASTFM_SCROBBLE_ENABLED"]))
    if "LASTFM_NOW_PLAYING_ENABLED" in updates:
        mod.LASTFM_NOW_PLAYING_ENABLED = bool(_parse_bool(updates["LASTFM_NOW_PLAYING_ENABLED"]))
    if "FANART_API_KEY" in updates:
        v = str(updates["FANART_API_KEY"] or "").strip()
        if not _is_masked_secret_placeholder(v):
            mod.FANART_API_KEY = v
    if "THEAUDIODB_API_KEY" in updates:
        v = str(updates["THEAUDIODB_API_KEY"] or "").strip()
        if not _is_masked_secret_placeholder(v):
            mod.THEAUDIODB_API_KEY = v
    if "USE_BANDCAMP" in updates:
        mod.USE_BANDCAMP = bool(_parse_bool(updates["USE_BANDCAMP"]))
        mod.merged["USE_BANDCAMP"] = mod.USE_BANDCAMP
    for _rpm_attr in (
        "PROVIDER_GATEWAY_ITUNES_RPM",
        "PROVIDER_GATEWAY_DEEZER_RPM",
        "PROVIDER_GATEWAY_SPOTIFY_RPM",
        "PROVIDER_GATEWAY_QOBUZ_RPM",
        "PROVIDER_GATEWAY_TIDAL_RPM",
        "PROVIDER_GATEWAY_AUDIODB_RPM",
    ):
        if _rpm_attr in updates:
            try:
                setattr(mod, _rpm_attr, max(1, int(updates[_rpm_attr] or 1)))
            except (TypeError, ValueError):
                pass
            mod.merged[_rpm_attr] = getattr(mod, _rpm_attr, updates[_rpm_attr])
    if any(
        key in updates
        for key in (
            "PROVIDER_GATEWAY_ITUNES_RPM",
            "PROVIDER_GATEWAY_DEEZER_RPM",
            "PROVIDER_GATEWAY_SPOTIFY_RPM",
            "PROVIDER_GATEWAY_QOBUZ_RPM",
            "PROVIDER_GATEWAY_TIDAL_RPM",
            "PROVIDER_GATEWAY_AUDIODB_RPM",
        )
    ):
        _provider_gateway_apply_runtime_settings()
    if "JELLYFIN_URL" in updates:
        global JELLYFIN_URL
        JELLYFIN_URL = _normalize_http_base_url(updates.get("JELLYFIN_URL"))
        mod.merged["JELLYFIN_URL"] = JELLYFIN_URL
    if "JELLYFIN_API_KEY" in updates:
        global JELLYFIN_API_KEY
        v = str(updates.get("JELLYFIN_API_KEY") or "").strip()
        if not _is_masked_secret_placeholder(v):
            JELLYFIN_API_KEY = v
            mod.merged["JELLYFIN_API_KEY"] = JELLYFIN_API_KEY
    if "NAVIDROME_URL" in updates:
        global NAVIDROME_URL
        NAVIDROME_URL = _normalize_http_base_url(updates.get("NAVIDROME_URL"))
        mod.merged["NAVIDROME_URL"] = NAVIDROME_URL
    if "NAVIDROME_USERNAME" in updates:
        global NAVIDROME_USERNAME
        NAVIDROME_USERNAME = str(updates.get("NAVIDROME_USERNAME") or "").strip()
        mod.merged["NAVIDROME_USERNAME"] = NAVIDROME_USERNAME
    if "NAVIDROME_PASSWORD" in updates:
        global NAVIDROME_PASSWORD
        v = str(updates.get("NAVIDROME_PASSWORD") or "")
        if not _is_masked_secret_placeholder(v):
            NAVIDROME_PASSWORD = v
            mod.merged["NAVIDROME_PASSWORD"] = NAVIDROME_PASSWORD
    if "NAVIDROME_API_KEY" in updates:
        global NAVIDROME_API_KEY
        v = str(updates.get("NAVIDROME_API_KEY") or "").strip()
        if not _is_masked_secret_placeholder(v):
            NAVIDROME_API_KEY = v
            mod.merged["NAVIDROME_API_KEY"] = NAVIDROME_API_KEY
    if "SKIP_MB_FOR_LIVE_ALBUMS" in updates:
        mod.SKIP_MB_FOR_LIVE_ALBUMS = bool(_parse_bool(updates["SKIP_MB_FOR_LIVE_ALBUMS"]))
    if "TRACKLIST_MATCH_MIN" in updates:
        try:
            mod.TRACKLIST_MATCH_MIN = float(updates["TRACKLIST_MATCH_MIN"])
        except (TypeError, ValueError):
            pass
    if "LIVE_ALBUMS_MB_STRICT" in updates:
        mod.LIVE_ALBUMS_MB_STRICT = bool(_parse_bool(updates["LIVE_ALBUMS_MB_STRICT"]))

    # AI-related: update globals then reinit clients
    if "AI_PROVIDER" in updates:
        mod.AI_PROVIDER = str(updates["AI_PROVIDER"] or "openai").strip().lower()
    if "OPENAI_API_KEY" in updates:
        v = str(updates["OPENAI_API_KEY"] or "").strip()
        if not _is_masked_secret_placeholder(v):
            mod.OPENAI_API_KEY = v
    if "OPENAI_ENABLE_API_KEY_MODE" in updates:
        global OPENAI_ENABLE_API_KEY_MODE
        OPENAI_ENABLE_API_KEY_MODE = bool(_parse_bool(updates["OPENAI_ENABLE_API_KEY_MODE"]))
        mod.OPENAI_ENABLE_API_KEY_MODE = OPENAI_ENABLE_API_KEY_MODE
    if "OPENAI_ENABLE_CODEX_OAUTH_MODE" in updates:
        global OPENAI_ENABLE_CODEX_OAUTH_MODE
        OPENAI_ENABLE_CODEX_OAUTH_MODE = bool(_parse_bool(updates["OPENAI_ENABLE_CODEX_OAUTH_MODE"]))
        mod.OPENAI_ENABLE_CODEX_OAUTH_MODE = OPENAI_ENABLE_CODEX_OAUTH_MODE
    if "OPENAI_MODEL" in updates:
        mod.OPENAI_MODEL = str(updates["OPENAI_MODEL"] or "gpt-4")
    if "ANTHROPIC_API_KEY" in updates:
        v = str(updates["ANTHROPIC_API_KEY"] or "").strip()
        if not _is_masked_secret_placeholder(v):
            mod.ANTHROPIC_API_KEY = v
    if "GOOGLE_API_KEY" in updates:
        v = str(updates["GOOGLE_API_KEY"] or "").strip()
        if not _is_masked_secret_placeholder(v):
            mod.GOOGLE_API_KEY = v
    if "OLLAMA_URL" in updates:
        mod.OLLAMA_URL = str(updates["OLLAMA_URL"] or "").strip().rstrip("/")
    if "OLLAMA_MODEL" in updates:
        mod.OLLAMA_MODEL = str(updates["OLLAMA_MODEL"] or "").strip() or "qwen3:4b"
    if "OLLAMA_COMPLEX_MODEL" in updates:
        mod.OLLAMA_COMPLEX_MODEL = str(updates["OLLAMA_COMPLEX_MODEL"] or "").strip() or "qwen3:14b"
    if "OLLAMA_RUNTIME_MODE" in updates:
        merged["OLLAMA_RUNTIME_MODE"] = str(updates["OLLAMA_RUNTIME_MODE"] or "").strip()

    if need_ai_reinit:
        _reinit_ai_from_globals()

    # Enforce product defaults for simplified UX (files-only + always-on metadata pipeline).
    _apply_forced_runtime_defaults()


def api_config_put():
    """Persist configuration updates to SQLite (single source of truth).
    Only updates keys present in the request; existing values in SQLite are preserved.
    Settings are applied in memory immediately so no restart is needed.
    """
    data = request.get_json() or {}
    # Only process keys that are in the request AND in the allowed list
    # This preserves existing values in SQLite for keys not in the request
    updates = {k: v for k, v in data.items() if k in _config_core.CONFIG_UPDATE_ALLOWED_KEYS}
    if not updates:
        return jsonify({"status": "ok", "message": "Nothing to save"})
    if "SKIP_FOLDERS" in updates and isinstance(updates["SKIP_FOLDERS"], str):
        updates["SKIP_FOLDERS"] = [p.strip() for p in updates["SKIP_FOLDERS"].split(",") if p.strip()]
    if "FILES_ROOTS" in updates:
        updates["FILES_ROOTS"] = _parse_files_roots(updates["FILES_ROOTS"])
    if "LIBRARY_WORKFLOW_MODE" in updates:
        updates["LIBRARY_WORKFLOW_MODE"] = _normalize_library_workflow_mode(updates["LIBRARY_WORKFLOW_MODE"])
    if "FILES_TAG_WRITE_MODE" in updates:
        mode = str(updates["FILES_TAG_WRITE_MODE"] or "full").strip().lower()
        updates["FILES_TAG_WRITE_MODE"] = mode if mode in {"full", "pmda_id_only"} else "full"
    for _path_key in ("LIBRARY_INTAKE_ROOTS", "LIBRARY_SOURCE_ROOTS"):
        if _path_key in updates:
            updates[_path_key] = _normalize_root_path_list(updates[_path_key])
    for _path_key in ("LIBRARY_SERVING_ROOT", "LIBRARY_DUPES_ROOT", "LIBRARY_INCOMPLETE_ROOT"):
        if _path_key in updates:
            updates[_path_key] = _normalize_root_path(updates[_path_key])
    if "WINNER_SOURCE_ROOT_ID" in updates:
        try:
            updates["WINNER_SOURCE_ROOT_ID"] = max(0, int(updates["WINNER_SOURCE_ROOT_ID"] or 0))
        except (ValueError, TypeError):
            updates["WINNER_SOURCE_ROOT_ID"] = 0
    if "LIBRARY_WINNER_PLACEMENT_STRATEGY" in updates:
        strategy = str(updates["LIBRARY_WINNER_PLACEMENT_STRATEGY"] or "move").strip().lower()
        updates["LIBRARY_WINNER_PLACEMENT_STRATEGY"] = strategy if strategy in {"move", "hardlink", "symlink", "copy"} else "move"
    if "EXPORT_LINK_STRATEGY" in updates:
        strategy = str(updates["EXPORT_LINK_STRATEGY"] or "hardlink").strip().lower()
        updates["EXPORT_LINK_STRATEGY"] = strategy if strategy in {"hardlink", "symlink", "copy", "move"} else "hardlink"
    if "LIBRARY_MATERIALIZATION_MODE" in updates:
        strategy = str(updates["LIBRARY_MATERIALIZATION_MODE"] or "hardlink").strip().lower()
        updates["LIBRARY_MATERIALIZATION_MODE"] = strategy if strategy in {"hardlink", "symlink", "copy", "move"} else "hardlink"
    if "MUSICBRAINZ_MIRROR_ENABLED" in updates:
        updates["MUSICBRAINZ_MIRROR_ENABLED"] = bool(_parse_bool(updates["MUSICBRAINZ_MIRROR_ENABLED"]))
    if "MUSICBRAINZ_BASE_URL" in updates:
        updates["MUSICBRAINZ_BASE_URL"] = str(updates["MUSICBRAINZ_BASE_URL"] or "").strip()
    if "MUSICBRAINZ_MIRROR_NAME" in updates:
        updates["MUSICBRAINZ_MIRROR_NAME"] = str(updates["MUSICBRAINZ_MIRROR_NAME"] or "").strip()
    if "MUSICBRAINZ_RUNTIME_MODE" in updates:
        mode = str(updates["MUSICBRAINZ_RUNTIME_MODE"] or "").strip().lower()
        updates["MUSICBRAINZ_RUNTIME_MODE"] = mode if mode in {"managed", "adopted", "external", "absent"} else "external"
    if "OLLAMA_RUNTIME_MODE" in updates:
        mode = str(updates["OLLAMA_RUNTIME_MODE"] or "").strip().lower()
        updates["OLLAMA_RUNTIME_MODE"] = mode if mode in {"managed", "adopted", "external", "absent"} else "external"
    for _path_key in ("MANAGED_RUNTIME_CONFIG_ROOT", "MANAGED_RUNTIME_DATA_ROOT", "MANAGED_MUSICBRAINZ_INSTALL_ROOT"):
        if _path_key in updates:
            updates[_path_key] = str(updates[_path_key] or "").strip()
    if "MANAGED_MUSICBRAINZ_UPDATE_ENABLED" in updates:
        updates["MANAGED_MUSICBRAINZ_UPDATE_ENABLED"] = bool(_parse_bool(updates["MANAGED_MUSICBRAINZ_UPDATE_ENABLED"]))
    if "MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS" in updates:
        try:
            updates["MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS"] = max(
                1,
                min(24 * 30, int(updates["MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS"])),
            )
        except (TypeError, ValueError):
            updates["MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS"] = 24 * 7
    if "MB_PUBLIC_QUEUE_RPS" in updates:
        try:
            updates["MB_PUBLIC_QUEUE_RPS"] = max(0.1, float(updates["MB_PUBLIC_QUEUE_RPS"] or 1.0))
        except (TypeError, ValueError):
            updates["MB_PUBLIC_QUEUE_RPS"] = float(MB_PUBLIC_QUEUE_RPS)
    if "MB_MIRROR_QUEUE_RPS" in updates:
        try:
            updates["MB_MIRROR_QUEUE_RPS"] = max(1.0, float(updates["MB_MIRROR_QUEUE_RPS"] or 12.0))
        except (TypeError, ValueError):
            updates["MB_MIRROR_QUEUE_RPS"] = float(MB_MIRROR_QUEUE_RPS)
    if "MB_MIRROR_QUEUE_WORKERS" in updates:
        try:
            updates["MB_MIRROR_QUEUE_WORKERS"] = max(1, min(32, int(updates["MB_MIRROR_QUEUE_WORKERS"] or 4)))
        except (TypeError, ValueError):
            updates["MB_MIRROR_QUEUE_WORKERS"] = int(MB_MIRROR_QUEUE_WORKERS)
    if "PIPELINE_PLAYER_TARGET" in updates:
        updates["PIPELINE_PLAYER_TARGET"] = _normalize_player_target(updates["PIPELINE_PLAYER_TARGET"])
    for _k in (
        "USE_ITUNES",
        "USE_DEEZER",
        "USE_SPOTIFY",
        "USE_QOBUZ",
        "USE_TIDAL",
        "LASTFM_SCROBBLE_ENABLED",
        "LASTFM_NOW_PLAYING_ENABLED",
        "USE_AI_WEB_SEARCH_FALLBACK",
        "SCHEDULER_ALLOW_NON_SCAN_JOBS",
        "OPENAI_ENABLE_API_KEY_MODE", "OPENAI_ENABLE_CODEX_OAUTH_MODE",
        "PROVIDER_GATEWAY_ENABLED",
        "PROVIDER_GATEWAY_CACHE_ENABLED",
        "MCP_ENABLED",
    ):
        if _k in updates:
            updates[_k] = bool(_parse_bool(updates[_k]))
    updates.update(_config_core.normalize_pipeline_bool_settings(updates))
    updates.update(_config_core.normalize_metadata_worker_settings(updates))
    updates.update(_config_core.normalize_task_notification_settings(updates))
    updates.update(_config_core.normalize_storage_power_saver_settings(updates))
    if "WEB_SEARCH_PROVIDER" in updates:
        updates["WEB_SEARCH_PROVIDER"] = _normalize_web_search_provider(updates["WEB_SEARCH_PROVIDER"])
    if "AI_MAX_CALLS_PER_SCAN" in updates:
        updates["AI_MAX_CALLS_PER_SCAN"] = 0
    if "AI_CALL_COOLDOWN_SEC" in updates:
        updates["AI_CALL_COOLDOWN_SEC"] = 0.0
    if "AI_GLOBAL_MAX_CALLS_PER_MINUTE" in updates:
        updates["AI_GLOBAL_MAX_CALLS_PER_MINUTE"] = 0
    if "AI_GLOBAL_MAX_CALLS_PER_DAY" in updates:
        updates["AI_GLOBAL_MAX_CALLS_PER_DAY"] = 0
    if "AI_USAGE_LEVEL" in updates:
        normalized_level = _normalize_ai_usage_level(str(updates.get("AI_USAGE_LEVEL") or "auto"))
        updates["AI_USAGE_LEVEL"] = normalized_level
        updates.update(_ai_usage_level_overrides(normalized_level))
    if "CLASSICAL_NAME_PREFERENCE" in updates:
        updates["CLASSICAL_NAME_PREFERENCE"] = _normalize_classical_name_preference(updates.get("CLASSICAL_NAME_PREFERENCE"))
    if "SCAN_AI_POLICY" in updates:
        updates["SCAN_AI_POLICY"] = _normalize_scan_ai_policy(str(updates.get("SCAN_AI_POLICY") or "local_only"))
    if "SCAN_PAID_PROVIDER_ORDER" in updates:
        updates["SCAN_PAID_PROVIDER_ORDER"] = ",".join(
            _normalize_ordered_values(
                updates.get("SCAN_PAID_PROVIDER_ORDER"),
                allowed=("openai-api", "openai-codex", "anthropic", "google"),
                default=("openai-api", "openai-codex", "anthropic", "google"),
            )
        )
    if "WEB_SEARCH_LOCAL_ORDER" in updates:
        updates["WEB_SEARCH_LOCAL_ORDER"] = ",".join(
            _normalize_ordered_values(
                updates.get("WEB_SEARCH_LOCAL_ORDER"),
                allowed=("serper",),
                default=("serper",),
            )
        )
    if "MATCH_COVER_OCR_MODE" in updates:
        updates["MATCH_COVER_OCR_MODE"] = _normalize_match_cover_ocr_mode(updates["MATCH_COVER_OCR_MODE"])
    if "JELLYFIN_URL" in updates:
        updates["JELLYFIN_URL"] = _normalize_http_base_url(updates["JELLYFIN_URL"])
    if "NAVIDROME_URL" in updates:
        updates["NAVIDROME_URL"] = _normalize_http_base_url(updates["NAVIDROME_URL"])
    if "NAVIDROME_USERNAME" in updates:
        updates["NAVIDROME_USERNAME"] = str(updates["NAVIDROME_USERNAME"] or "").strip()
    if "REQUIRED_TAGS" in updates:
        updates["REQUIRED_TAGS"] = _parse_required_tags(updates["REQUIRED_TAGS"])
    if "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD" in updates:
        try:
            updates["BROKEN_ALBUM_CONSECUTIVE_THRESHOLD"] = int(updates["BROKEN_ALBUM_CONSECUTIVE_THRESHOLD"])
        except (ValueError, TypeError):
            updates["BROKEN_ALBUM_CONSECUTIVE_THRESHOLD"] = 2
    if "BROKEN_ALBUM_PERCENTAGE_THRESHOLD" in updates:
        try:
            updates["BROKEN_ALBUM_PERCENTAGE_THRESHOLD"] = float(updates["BROKEN_ALBUM_PERCENTAGE_THRESHOLD"])
        except (ValueError, TypeError):
            updates["BROKEN_ALBUM_PERCENTAGE_THRESHOLD"] = 0.20
    if "MB_SEARCH_ALBUM_TIMEOUT_SEC" in updates:
        try:
            updates["MB_SEARCH_ALBUM_TIMEOUT_SEC"] = max(0, min(3600, int(updates["MB_SEARCH_ALBUM_TIMEOUT_SEC"])))
        except (ValueError, TypeError):
            updates["MB_SEARCH_ALBUM_TIMEOUT_SEC"] = 0
    if "MB_CANDIDATE_FETCH_LIMIT" in updates:
        try:
            updates["MB_CANDIDATE_FETCH_LIMIT"] = max(0, min(100, int(updates["MB_CANDIDATE_FETCH_LIMIT"])))
        except (ValueError, TypeError):
            updates["MB_CANDIDATE_FETCH_LIMIT"] = 0
    if "MB_TRACKLIST_FETCH_LIMIT" in updates:
        try:
            updates["MB_TRACKLIST_FETCH_LIMIT"] = max(0, min(100, int(updates["MB_TRACKLIST_FETCH_LIMIT"])))
        except (ValueError, TypeError):
            updates["MB_TRACKLIST_FETCH_LIMIT"] = 0
    if "ARTWORK_RAM_CACHE_MB" in updates:
        try:
            updates["ARTWORK_RAM_CACHE_MB"] = max(0, min(65536, int(updates["ARTWORK_RAM_CACHE_MB"])))
        except (ValueError, TypeError):
            updates["ARTWORK_RAM_CACHE_MB"] = int(max(0, ARTWORK_RAM_CACHE_MB))
    if "ARTWORK_RAM_CACHE_TTL_SEC" in updates:
        try:
            updates["ARTWORK_RAM_CACHE_TTL_SEC"] = max(60, min(60 * 60 * 24 * 30, int(updates["ARTWORK_RAM_CACHE_TTL_SEC"])))
        except (ValueError, TypeError):
            updates["ARTWORK_RAM_CACHE_TTL_SEC"] = int(max(60, ARTWORK_RAM_CACHE_TTL_SEC))
    if "ARTWORK_RAM_CACHE_MAX_ITEM_MB" in updates:
        try:
            updates["ARTWORK_RAM_CACHE_MAX_ITEM_MB"] = max(1, min(64, int(updates["ARTWORK_RAM_CACHE_MAX_ITEM_MB"])))
        except (ValueError, TypeError):
            updates["ARTWORK_RAM_CACHE_MAX_ITEM_MB"] = int(max(1, ARTWORK_RAM_CACHE_MAX_ITEM_MB))
    if "ARTWORK_RAM_CACHE_AUTO" in updates:
        updates["ARTWORK_RAM_CACHE_AUTO"] = bool(_parse_bool(updates["ARTWORK_RAM_CACHE_AUTO"]))
    if "ARTWORK_RAM_CACHE_AUTO_MAX_MB" in updates:
        try:
            updates["ARTWORK_RAM_CACHE_AUTO_MAX_MB"] = max(0, min(65536, int(updates["ARTWORK_RAM_CACHE_AUTO_MAX_MB"])))
        except (ValueError, TypeError):
            updates["ARTWORK_RAM_CACHE_AUTO_MAX_MB"] = int(max(0, ARTWORK_RAM_CACHE_AUTO_MAX_MB))
    if "ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC" in updates:
        try:
            updates["ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC"] = max(30, min(3600, int(updates["ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC"])))
        except (ValueError, TypeError):
            updates["ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC"] = int(max(30, ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC))
    if "PROVIDER_GATEWAY_MAX_INFLIGHT" in updates:
        try:
            updates["PROVIDER_GATEWAY_MAX_INFLIGHT"] = max(1, min(256, int(updates["PROVIDER_GATEWAY_MAX_INFLIGHT"])))
        except (ValueError, TypeError):
            updates["PROVIDER_GATEWAY_MAX_INFLIGHT"] = 16
    for _rpm_key, _default_rpm, _max_rpm in (
        ("PROVIDER_GATEWAY_DISCOGS_RPM", 40, 600),
        ("PROVIDER_GATEWAY_ITUNES_RPM", 180, 6000),
        ("PROVIDER_GATEWAY_DEEZER_RPM", 120, 6000),
        ("PROVIDER_GATEWAY_SPOTIFY_RPM", 60, 6000),
        ("PROVIDER_GATEWAY_QOBUZ_RPM", 40, 6000),
        ("PROVIDER_GATEWAY_TIDAL_RPM", 20, 6000),
        ("PROVIDER_GATEWAY_LASTFM_RPM", 120, 6000),
        ("PROVIDER_GATEWAY_AUDIODB_RPM", 60, 6000),
        ("PROVIDER_GATEWAY_BANDCAMP_RPM", 12, 600),
    ):
        if _rpm_key in updates:
            try:
                updates[_rpm_key] = max(1, min(_max_rpm, int(updates[_rpm_key])))
            except (ValueError, TypeError):
                updates[_rpm_key] = _default_rpm
    if "AUTO_TUNE_ENABLED" in updates:
        updates["AUTO_TUNE_ENABLED"] = bool(_parse_bool(updates["AUTO_TUNE_ENABLED"]))
    if "AUTO_TUNE_INTERVAL_SEC" in updates:
        try:
            updates["AUTO_TUNE_INTERVAL_SEC"] = max(15, min(900, int(updates["AUTO_TUNE_INTERVAL_SEC"])))
        except (ValueError, TypeError):
            updates["AUTO_TUNE_INTERVAL_SEC"] = int(max(15, AUTO_TUNE_INTERVAL_SEC))
    if "AUTO_TUNE_MB_MIRROR_MIN_RPS" in updates:
        try:
            updates["AUTO_TUNE_MB_MIRROR_MIN_RPS"] = max(1.0, min(100.0, float(updates["AUTO_TUNE_MB_MIRROR_MIN_RPS"])))
        except (ValueError, TypeError):
            updates["AUTO_TUNE_MB_MIRROR_MIN_RPS"] = float(max(1.0, AUTO_TUNE_MB_MIRROR_MIN_RPS))
    if "AUTO_TUNE_MB_MIRROR_MAX_RPS" in updates:
        try:
            updates["AUTO_TUNE_MB_MIRROR_MAX_RPS"] = max(1.0, min(100.0, float(updates["AUTO_TUNE_MB_MIRROR_MAX_RPS"])))
        except (ValueError, TypeError):
            updates["AUTO_TUNE_MB_MIRROR_MAX_RPS"] = float(max(1.0, AUTO_TUNE_MB_MIRROR_MAX_RPS))
    if "AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN" in updates:
        try:
            updates["AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN"] = max(1, min(256, int(updates["AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN"])))
        except (ValueError, TypeError):
            updates["AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN"] = int(max(1, AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN))
    if "AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP" in updates:
        try:
            updates["AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP"] = max(1, min(256, int(updates["AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP"])))
        except (ValueError, TypeError):
            updates["AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP"] = int(max(1, AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP))
    workflow_rows: list[dict] | None = None
    if any(
        key in updates
        for key in (
            "LIBRARY_WORKFLOW_MODE",
            "LIBRARY_SERVING_ROOT",
            "LIBRARY_INTAKE_ROOTS",
            "LIBRARY_SOURCE_ROOTS",
            "LIBRARY_DUPES_ROOT",
            "LIBRARY_INCOMPLETE_ROOT",
            "LIBRARY_MATERIALIZATION_MODE",
            "LIBRARY_INCLUDE_FORMAT_IN_FOLDER",
            "LIBRARY_INCLUDE_TYPE_IN_FOLDER",
        )
    ):
        try:
            workflow_updates, workflow_rows = _library_workflow_prepare_updates(_settings_db_read_all(), updates)
            updates.update(workflow_updates)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
    if "PROVIDER_IDENTITY_MIN_SCORE" in updates:
        try:
            updates["PROVIDER_IDENTITY_MIN_SCORE"] = max(0.0, min(1.0, float(updates["PROVIDER_IDENTITY_MIN_SCORE"])))
        except (ValueError, TypeError):
            updates["PROVIDER_IDENTITY_MIN_SCORE"] = 0.72
    if "PROVIDER_IDENTITY_SCORE_MARGIN" in updates:
        try:
            updates["PROVIDER_IDENTITY_SCORE_MARGIN"] = max(0.0, min(1.0, float(updates["PROVIDER_IDENTITY_SCORE_MARGIN"])))
        except (ValueError, TypeError):
            updates["PROVIDER_IDENTITY_SCORE_MARGIN"] = 0.08
    if "PROVIDER_CACHE_FOUND_TTL_SEC" in updates:
        try:
            updates["PROVIDER_CACHE_FOUND_TTL_SEC"] = max(60, min(60 * 60 * 24 * 365, int(updates["PROVIDER_CACHE_FOUND_TTL_SEC"])))
        except (ValueError, TypeError):
            updates["PROVIDER_CACHE_FOUND_TTL_SEC"] = 60 * 60 * 24 * 30
    if "PROVIDER_CACHE_NOT_FOUND_TTL_SEC" in updates:
        try:
            updates["PROVIDER_CACHE_NOT_FOUND_TTL_SEC"] = max(60, min(60 * 60 * 24 * 30, int(updates["PROVIDER_CACHE_NOT_FOUND_TTL_SEC"])))
        except (ValueError, TypeError):
            updates["PROVIDER_CACHE_NOT_FOUND_TTL_SEC"] = 60 * 60 * 24 * 30
    if "PROVIDER_CACHE_ERROR_TTL_SEC" in updates:
        try:
            updates["PROVIDER_CACHE_ERROR_TTL_SEC"] = max(30, min(60 * 60 * 24, int(updates["PROVIDER_CACHE_ERROR_TTL_SEC"])))
        except (ValueError, TypeError):
            updates["PROVIDER_CACHE_ERROR_TTL_SEC"] = 60 * 60 * 6

    # Concert discovery (UI filtering)
    if "CONCERTS_FILTER_ENABLED" in updates:
        updates["CONCERTS_FILTER_ENABLED"] = bool(_parse_bool(updates["CONCERTS_FILTER_ENABLED"]))
    if "CONCERTS_RADIUS_KM" in updates:
        try:
            updates["CONCERTS_RADIUS_KM"] = str(max(1, min(2000, int(float(updates["CONCERTS_RADIUS_KM"])))))
        except (ValueError, TypeError):
            updates["CONCERTS_RADIUS_KM"] = "150"
    for k in ("CONCERTS_HOME_LAT", "CONCERTS_HOME_LON"):
        if k in updates:
            raw = str(updates.get(k) or "").strip()
            if not raw:
                updates[k] = ""
                continue
            try:
                # Keep as string for config storage; clamp lat/lon bounds.
                v = float(raw)
                if k.endswith("_LAT"):
                    v = max(-90.0, min(90.0, v))
                else:
                    v = max(-180.0, min(180.0, v))
                updates[k] = f"{v:.6f}".rstrip("0").rstrip(".")
            except Exception:
                updates[k] = ""

    # Force files-only mode regardless of incoming value.
    if "LIBRARY_MODE" in updates:
        updates["LIBRARY_MODE"] = "files"
    if "LIBRARY_INCLUDE_UNMATCHED" in updates:
        updates["LIBRARY_INCLUDE_UNMATCHED"] = bool(_parse_bool(updates["LIBRARY_INCLUDE_UNMATCHED"]))

    # Serialize complex types for SQLite storage. Never overwrite real secrets with UI masks.
    masked_secrets = set(MASKED_SECRET_KEYS)
    # Drop masked secrets from the in-memory apply path as well.
    for k in list(updates.keys()):
        if k in masked_secrets and _is_masked_secret_placeholder(updates.get(k)):
            updates.pop(k, None)
    updates_for_db = {}
    forced_db_updates = {
        "LIBRARY_MODE": "files",
        "configured": "1",
    }
    for k, v in updates.items():
        if k in masked_secrets and _is_masked_secret_placeholder(v):
            continue  # Do not overwrite real key with mask
        if k == "SECTION_IDS" and isinstance(v, list):
            # Store SECTION_IDS as comma-separated string so GET /api/config can parse it consistently
            updates_for_db[k] = ",".join(str(x) for x in v) if v else ""
        elif k == "SKIP_FOLDERS" and isinstance(v, list):
            # Store as comma-separated string; load uses _parse_skip_folders (accepts JSON or CSV)
            updates_for_db[k] = ",".join(str(p).strip() for p in v if str(p).strip()) if v else ""
        elif k == "FILES_ROOTS" and isinstance(v, list):
            # Store as comma-separated string to avoid double-encoded JSON values.
            updates_for_db[k] = ",".join(str(p).strip() for p in v if str(p).strip()) if v else ""
        elif isinstance(v, (dict, list)):
            updates_for_db[k] = json.dumps(v)
        else:
            # Preserve empty strings (they are valid values, e.g. empty webhook = disabled)
            updates_for_db[k] = str(v) if v is not None else ""
    for k, v in forced_db_updates.items():
        updates_for_db[k] = v

    try:
        # Save to dedicated settings.db (single source of truth for configuration)
        init_settings_db()
        con = sqlite3.connect(str(SETTINGS_DB_FILE))
        cur = con.cursor()
        for key, value in updates_for_db.items():
            cur.execute("INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)", (key, value))
        con.commit()
        con.close()
        logging.info("Settings saved to settings.db: %s", list(updates_for_db.keys()))
    except Exception as e:
        logging.warning("Failed to save settings to settings.db: %s", e)
        return jsonify({"status": "error", "message": f"Failed to save to database: {str(e)}"}), 500

    if workflow_rows is not None:
        try:
            saved_rows = _files_source_roots_replace(workflow_rows)
            winner_row = next((row for row in saved_rows if bool(row.get("is_winner_root"))), saved_rows[0] if saved_rows else None)
            if winner_row:
                updates["WINNER_SOURCE_ROOT_ID"] = int(winner_row.get("source_id") or 0)
        except Exception as exc:
            logging.exception("Failed to persist guided library workflow source roots")
            return jsonify({"status": "error", "message": f"Failed to save library workflow folders: {str(exc)}"}), 500

    # Apply all saved settings in memory (no restart needed)
    _apply_settings_in_memory(updates)
    if "FILES_ROOTS" in updates:
        # Seed source-roots table from FILES_ROOTS only when no explicit roots exist yet.
        # This avoids the "Music folders set, Sources & autonomy empty" confusion.
        _ensure_files_source_roots_seeded()

    # Only restart container if this is the initial wizard setup (first time saving)
    # After wizard is complete, settings are saved but container doesn't restart
    has_settings = _has_settings_in_db()
    restart_success = False
    if not has_settings:
        # First time saving (wizard), restart container
        logging.info("First time configuration save detected, restarting container")
        restart_success = _restart_container()
        if not restart_success:
            logging.warning("Container restart may have failed, but settings were saved")
    else:
        # Settings already exist, just save without restart
        logging.info("Settings updated (wizard already completed), saving without restart")

    return jsonify({"status": "ok", "restart_initiated": restart_success, "message": "Settings saved successfully"})
