"""Pure configuration parsing helpers for PMDA.

This module intentionally contains no Flask, database, or process-global
dependencies. Runtime settings can keep compatibility wrappers in ``pmda.py``
while the parsing rules live here and are tested independently.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any


DEFAULT_FORMAT_PREFERENCE = [
    "dsf",
    "aif",
    "aiff",
    "wav",
    "flac",
    "m4a",
    "mp4",
    "m4b",
    "m4p",
    "aifc",
    "ogg",
    "opus",
    "mp3",
    "wma",
]

DISABLED_PLAYER_SOURCE_SETTING_KEYS = frozenset({"PLEX_BASE_PATH", "PLEX_" + "DB_PATH", "SECTION_IDS"})
DISABLED_EXTERNAL_INTEGRATION_KEYS = DISABLED_PLAYER_SOURCE_SETTING_KEYS | frozenset(
    {"LIDARR_URL", "LIDARR_API_KEY", "AUTOBRR_URL", "AUTOBRR_API_KEY"}
)

STORAGE_POWER_SAVER_SETTING_KEYS = frozenset(
    {
        "STORAGE_POWER_SAVER_ENABLED",
        "STORAGE_PROVIDER",
        "UNRAID_HOST_MNT_ROOT",
        "UNRAID_USER_SHARE_HOST_ROOT",
        "UNRAID_CONTAINER_SHARE_ROOT",
        "STORAGE_MAX_ACTIVE_DEVICES",
        "STORAGE_SPINDOWN_POLICY",
    }
)

TASK_NOTIFICATION_BOOL_SETTING_KEYS = frozenset(
    {
        "TASK_NOTIFICATIONS_ENABLED",
        "TASK_NOTIFICATIONS_SUCCESS",
        "TASK_NOTIFICATIONS_FAILURE",
        "TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN",
        "TASK_NOTIFY_SCAN_CHANGED",
        "TASK_NOTIFY_SCAN_FULL",
        "TASK_NOTIFY_ENRICH_BATCH",
        "TASK_NOTIFY_DEDUPE",
        "TASK_NOTIFY_INCOMPLETE_MOVE",
        "TASK_NOTIFY_EXPORT",
        "TASK_NOTIFY_PLAYER_SYNC",
    }
)

PIPELINE_BOOL_SETTING_KEYS = frozenset(
    {
        "PIPELINE_ENABLE_MATCH_FIX",
        "PIPELINE_ENABLE_DEDUPE",
        "PIPELINE_ENABLE_INCOMPLETE_MOVE",
        "PIPELINE_ENABLE_EXPORT",
        "PIPELINE_ENABLE_PLAYER_SYNC",
        "PIPELINE_POST_SCAN_ASYNC",
    }
)

METADATA_WORKER_SETTING_KEYS = frozenset(
    {
        "METADATA_QUEUE_ENABLED",
        "METADATA_WORKER_MODE",
        "METADATA_WORKER_COUNT",
        "METADATA_JOB_BATCH_SIZE",
    }
)

CONFIG_UPDATE_ALLOWED_KEYS = frozenset(
    """
    PATH_MAP DUPE_ROOT PMDA_CONFIG_DIR MUSIC_PARENT_PATH SCAN_THREADS LOG_LEVEL
    LOG_FILE AI_PROVIDER AI_USAGE_LEVEL SCAN_AI_POLICY SCAN_PAID_PROVIDER_ORDER
    WEB_SEARCH_LOCAL_ORDER OPENAI_API_KEY OPENAI_ENABLE_API_KEY_MODE
    OPENAI_ENABLE_CODEX_OAUTH_MODE OPENAI_MODEL OPENAI_MODEL_FALLBACKS
    ANTHROPIC_API_KEY GOOGLE_API_KEY OLLAMA_URL OLLAMA_MODEL OLLAMA_COMPLEX_MODEL
    DISCORD_WEBHOOK USE_MUSICBRAINZ MUSICBRAINZ_EMAIL MUSICBRAINZ_MIRROR_ENABLED
    MUSICBRAINZ_BASE_URL MUSICBRAINZ_MIRROR_NAME MUSICBRAINZ_RUNTIME_MODE
    MUSICBRAINZ_REPLICATION_TOKEN MANAGED_RUNTIME_CONFIG_ROOT
    MANAGED_RUNTIME_DATA_ROOT MANAGED_MUSICBRAINZ_INSTALL_ROOT
    MANAGED_MUSICBRAINZ_UPDATE_ENABLED MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS
    OLLAMA_RUNTIME_MODE MB_PUBLIC_QUEUE_RPS MB_MIRROR_QUEUE_RPS
    MB_MIRROR_QUEUE_WORKERS MB_RETRY_NOT_FOUND MB_SEARCH_ALBUM_TIMEOUT_SEC
    MB_CANDIDATE_FETCH_LIMIT MB_TRACKLIST_FETCH_LIMIT MB_FAST_FALLBACK_MODE
    PROVIDER_IDENTITY_STRICT PROVIDER_IDENTITY_USE_AI MATCH_COVER_OCR_MODE
    PROVIDER_IDENTITY_MIN_SCORE PROVIDER_IDENTITY_SCORE_MARGIN
    PROVIDER_CACHE_FOUND_TTL_SEC PROVIDER_CACHE_NOT_FOUND_TTL_SEC
    PROVIDER_CACHE_ERROR_TTL_SEC PROVIDER_GATEWAY_ENABLED
    PROVIDER_GATEWAY_CACHE_ENABLED PROVIDER_GATEWAY_MAX_INFLIGHT
    PROVIDER_GATEWAY_DISCOGS_RPM PROVIDER_GATEWAY_ITUNES_RPM
    PROVIDER_GATEWAY_DEEZER_RPM PROVIDER_GATEWAY_SPOTIFY_RPM
    PROVIDER_GATEWAY_QOBUZ_RPM PROVIDER_GATEWAY_TIDAL_RPM
    PROVIDER_GATEWAY_LASTFM_RPM PROVIDER_GATEWAY_AUDIODB_RPM
    PROVIDER_GATEWAY_BANDCAMP_RPM AUTO_TUNE_ENABLED AUTO_TUNE_INTERVAL_SEC
    AUTO_TUNE_MB_MIRROR_MIN_RPS AUTO_TUNE_MB_MIRROR_MAX_RPS
    AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP
    USE_AI_FOR_MB_MATCH USE_AI_FOR_MB_VERIFY USE_AI_FOR_DEDUPE
    USE_AI_FOR_SOFT_MATCH_PROFILES USE_AI_VISION_FOR_COVER AI_CONFIDENCE_MIN
    OPENAI_VISION_MODEL USE_AI_VISION_BEFORE_COVER_INJECT USE_WEB_SEARCH_FOR_MB
    WEB_SEARCH_PROVIDER SERPER_API_KEY USE_AI_WEB_SEARCH_FALLBACK
    AI_MAX_CALLS_PER_SCAN AI_CALL_COOLDOWN_SEC AI_GLOBAL_MAX_CALLS_PER_MINUTE
    AI_GLOBAL_MAX_CALLS_PER_DAY SCHEDULER_ALLOW_NON_SCAN_JOBS USE_ACOUSTID
    ACOUSTID_API_KEY USE_ACOUSTID_WHEN_TAGGED SKIP_FOLDERS CROSS_LIBRARY_DEDUPE
    CROSSCHECK_SAMPLES DISABLE_PATH_CROSSCHECK FORMAT_PREFERENCE AUTO_MOVE_DUPES
    NORMALIZE_PARENTHETICAL_FOR_DEDUPE BACKUP_BEFORE_FIX MAGIC_MODE
    REPROCESS_INCOMPLETE_ALBUMS IMPROVE_ALL_WORKERS FFPROBE_POOL_SIZE MCP_ENABLED
    PIPELINE_ENABLE_MATCH_FIX PIPELINE_ENABLE_DEDUPE PIPELINE_ENABLE_INCOMPLETE_MOVE
    PIPELINE_ENABLE_EXPORT PIPELINE_ENABLE_PLAYER_SYNC PIPELINE_PLAYER_TARGET
    PLEX_HOST PLEX_TOKEN PIPELINE_POST_SCAN_ASYNC METADATA_QUEUE_ENABLED
    METADATA_WORKER_MODE METADATA_WORKER_COUNT METADATA_JOB_BATCH_SIZE
    TASK_NOTIFICATIONS_ENABLED TASK_NOTIFICATIONS_SUCCESS TASK_NOTIFICATIONS_FAILURE
    TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN TASK_NOTIFICATIONS_COOLDOWN_SEC
    TASK_NOTIFY_SCAN_CHANGED TASK_NOTIFY_SCAN_FULL TASK_NOTIFY_ENRICH_BATCH
    TASK_NOTIFY_DEDUPE TASK_NOTIFY_INCOMPLETE_MOVE TASK_NOTIFY_EXPORT
    TASK_NOTIFY_PLAYER_SYNC ARTIST_CREDIT_MODE CLASSICAL_NAME_PREFERENCE
    LIVE_DEDUPE_MODE AUTO_FIX_BROKEN_ALBUMS BROKEN_ALBUM_CONSECUTIVE_THRESHOLD
    BROKEN_ALBUM_PERCENTAGE_THRESHOLD REQUIRED_TAGS USE_DISCOGS DISCOGS_USER_TOKEN
    USE_ITUNES USE_DEEZER USE_SPOTIFY USE_QOBUZ USE_TIDAL USE_LASTFM
    LASTFM_API_KEY LASTFM_API_SECRET LASTFM_SCROBBLE_ENABLED
    LASTFM_NOW_PLAYING_ENABLED USE_BANDCAMP FANART_API_KEY THEAUDIODB_API_KEY
    JELLYFIN_URL JELLYFIN_API_KEY NAVIDROME_URL NAVIDROME_USERNAME
    NAVIDROME_PASSWORD NAVIDROME_API_KEY INCOMPLETE_ALBUMS_TARGET_DIR
    SCAN_DISABLE_CACHE ARTWORK_RAM_CACHE_MB ARTWORK_RAM_CACHE_TTL_SEC
    ARTWORK_RAM_CACHE_MAX_ITEM_MB ARTWORK_RAM_CACHE_AUTO ARTWORK_RAM_CACHE_AUTO_MAX_MB
    ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC CONCERTS_FILTER_ENABLED CONCERTS_HOME_LAT
    CONCERTS_HOME_LON CONCERTS_RADIUS_KM LIBRARY_MODE LIBRARY_INCLUDE_UNMATCHED
    FILES_ROOTS STORAGE_POWER_SAVER_ENABLED STORAGE_PROVIDER UNRAID_HOST_MNT_ROOT
    UNRAID_USER_SHARE_HOST_ROOT UNRAID_CONTAINER_SHARE_ROOT STORAGE_MAX_ACTIVE_DEVICES
    STORAGE_SPINDOWN_POLICY WINNER_SOURCE_ROOT_ID LIBRARY_WINNER_PLACEMENT_STRATEGY
    EXPORT_ROOT EXPORT_NAMING_TEMPLATE EXPORT_LINK_STRATEGY
    EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER
    MEDIA_CACHE_ROOT AUTO_EXPORT_LIBRARY LIBRARY_WORKFLOW_MODE LIBRARY_SERVING_ROOT
    LIBRARY_INTAKE_ROOTS LIBRARY_SOURCE_ROOTS LIBRARY_DUPES_ROOT
    LIBRARY_INCOMPLETE_ROOT LIBRARY_MATERIALIZATION_MODE
    LIBRARY_INCLUDE_FORMAT_IN_FOLDER LIBRARY_INCLUDE_TYPE_IN_FOLDER
    FILES_TAG_WRITE_MODE
    """.split()
)


def parse_bool(value: str | bool | int | None) -> bool:
    """Return True for PMDA's accepted truthy values."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def is_false(value: str | bool | int | None) -> bool:
    """Return True for PMDA's accepted explicit falsy values."""
    if isinstance(value, bool):
        return not value
    return str(value).strip().lower() in {"0", "false", "no", "off"}


def normalize_ai_usage_level(value: str | None) -> str:
    """Normalize AI usage policy to PMDA's supported levels."""
    raw = str(value or "").strip().lower()
    if raw in {"limited", "medium", "aggressive", "auto"}:
        return raw
    return "auto"


def normalize_web_search_provider(value: str | None) -> str:
    """Normalize the configured web-search provider selection."""
    raw = str(value or "").strip().lower()
    if raw in {"auto", "serper", "ollama", "ai_only", "disabled"}:
        return raw
    return "auto"


def normalize_scan_ai_policy(value: str | None) -> str:
    """Normalize scan AI provider policy."""
    raw = str(value or "").strip().lower()
    if raw in {"local_only", "local_then_paid", "paid_only"}:
        return raw
    return "local_then_paid"


def normalize_classical_name_preference(value: str | None) -> str:
    """Normalize classical artist name display preference."""
    raw = str(value or "").strip().lower()
    return raw if raw in {"original", "english"} else "original"


def normalize_ordered_values(
    value: Any,
    *,
    allowed: tuple[str, ...],
    default: tuple[str, ...],
) -> list[str]:
    """Normalize ordered config values from JSON arrays, CSV, or iterables."""
    allowed_set = {str(item).strip().lower() for item in allowed}
    queue: list[Any] = [value]
    ordered: list[str] = []
    seen: set[str] = set()
    while queue:
        item = queue.pop(0)
        if item is None:
            continue
        if isinstance(item, (list, tuple, set)):
            queue.extend(list(item))
            continue
        if isinstance(item, str):
            raw = item.strip()
            if not raw:
                continue
            if raw.startswith("["):
                try:
                    parsed = json.loads(raw)
                    if parsed != item:
                        queue.append(parsed)
                        continue
                except Exception:
                    pass
            if "," in raw:
                parts = [part.strip() for part in raw.split(",") if str(part).strip()]
                if len(parts) > 1:
                    queue.extend(parts)
                    continue
            token = raw.strip().lower()
        else:
            token = str(item).strip().lower()
        if not token or token in seen or token not in allowed_set:
            continue
        seen.add(token)
        ordered.append(token)
    for token in default:
        norm = str(token).strip().lower()
        if norm and norm in allowed_set and norm not in seen:
            ordered.append(norm)
            seen.add(norm)
    return ordered


def filter_disabled_external_updates(updates: dict[str, Any]) -> tuple[dict[str, Any], set[str]]:
    """Drop retired external-integration settings from runtime updates.

    Plex source-DB, Lidarr, and Autobrr settings are intentionally ignored by
    PMDA v1. Plex remains supported only as a player-refresh target elsewhere.
    """

    ignored = set(updates.keys()) & set(DISABLED_EXTERNAL_INTEGRATION_KEYS)
    if not ignored:
        return dict(updates), set()
    return {key: value for key, value in updates.items() if key not in ignored}, ignored


def parse_int(value: Any, default: int | None = None) -> int | None:
    """Return int(value), or default when coercion fails."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_storage_path(value: Any, default: str) -> str:
    """Normalize storage mount paths without touching the filesystem."""
    raw = str(value if value is not None else default).strip()
    if not raw:
        raw = default
    return raw.rstrip("/") or "/"


def normalize_storage_power_saver_settings(updates: dict[str, Any]) -> dict[str, Any]:
    """Normalize disk-aware power saver updates that are present in a settings payload."""
    normalized: dict[str, Any] = {}
    if "STORAGE_POWER_SAVER_ENABLED" in updates:
        normalized["STORAGE_POWER_SAVER_ENABLED"] = parse_bool(updates["STORAGE_POWER_SAVER_ENABLED"])
    if "STORAGE_PROVIDER" in updates:
        provider = str(updates["STORAGE_PROVIDER"] or "unraid").strip().lower()
        normalized["STORAGE_PROVIDER"] = provider if provider in {"unraid"} else "unraid"
    if "UNRAID_HOST_MNT_ROOT" in updates:
        normalized["UNRAID_HOST_MNT_ROOT"] = normalize_storage_path(updates["UNRAID_HOST_MNT_ROOT"], "/host_mnt")
    if "UNRAID_USER_SHARE_HOST_ROOT" in updates:
        normalized["UNRAID_USER_SHARE_HOST_ROOT"] = normalize_storage_path(
            updates["UNRAID_USER_SHARE_HOST_ROOT"],
            "/host_mnt/user/MURRAY/Music",
        )
    if "UNRAID_CONTAINER_SHARE_ROOT" in updates:
        normalized["UNRAID_CONTAINER_SHARE_ROOT"] = normalize_storage_path(
            updates["UNRAID_CONTAINER_SHARE_ROOT"],
            "/music",
        )
    if "STORAGE_MAX_ACTIVE_DEVICES" in updates:
        normalized["STORAGE_MAX_ACTIVE_DEVICES"] = int(
            max(1, min(64, parse_int(updates["STORAGE_MAX_ACTIVE_DEVICES"], 1) or 1))
        )
    if "STORAGE_SPINDOWN_POLICY" in updates:
        policy = str(updates["STORAGE_SPINDOWN_POLICY"] or "none").strip().lower()
        normalized["STORAGE_SPINDOWN_POLICY"] = policy if policy in {"none"} else "none"
    return normalized


def normalize_task_notification_settings(updates: dict[str, Any]) -> dict[str, Any]:
    """Normalize task-notification update values that are present in a settings payload."""
    normalized = {
        key: parse_bool(updates[key])
        for key in sorted(TASK_NOTIFICATION_BOOL_SETTING_KEYS)
        if key in updates
    }
    if "TASK_NOTIFICATIONS_COOLDOWN_SEC" in updates:
        try:
            normalized["TASK_NOTIFICATIONS_COOLDOWN_SEC"] = max(
                0,
                min(3600, int(updates["TASK_NOTIFICATIONS_COOLDOWN_SEC"])),
            )
        except (TypeError, ValueError):
            normalized["TASK_NOTIFICATIONS_COOLDOWN_SEC"] = 20
    return normalized


def normalize_pipeline_bool_settings(updates: dict[str, Any]) -> dict[str, bool]:
    """Normalize pipeline boolean updates that are present in a settings payload."""
    return {
        key: parse_bool(updates[key])
        for key in sorted(PIPELINE_BOOL_SETTING_KEYS)
        if key in updates
    }


def normalize_metadata_worker_settings(updates: dict[str, Any]) -> dict[str, Any]:
    """Normalize metadata worker settings that are present in a settings payload."""
    normalized: dict[str, Any] = {}
    if "METADATA_QUEUE_ENABLED" in updates:
        normalized["METADATA_QUEUE_ENABLED"] = parse_bool(updates["METADATA_QUEUE_ENABLED"])
    if "METADATA_WORKER_MODE" in updates:
        mode = str(updates["METADATA_WORKER_MODE"] or "local").strip().lower()
        normalized["METADATA_WORKER_MODE"] = mode if mode in {"local", "hybrid"} else "local"
    if "METADATA_WORKER_COUNT" in updates:
        normalized["METADATA_WORKER_COUNT"] = max(0, min(128, parse_int(updates["METADATA_WORKER_COUNT"], 0) or 0))
    if "METADATA_JOB_BATCH_SIZE" in updates:
        normalized["METADATA_JOB_BATCH_SIZE"] = max(0, min(500, parse_int(updates["METADATA_JOB_BATCH_SIZE"], 0) or 0))
    return normalized


def parse_path_map(value: Any) -> dict[str, str]:
    """Parse PATH_MAP from dict, JSON object string, or CSV SRC:DEST pairs."""
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items()}
    if not value:
        return {}
    raw = str(value).strip()
    if raw.startswith("{"):
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
        except json.JSONDecodeError as exc:
            logging.warning("Failed to decode PATH_MAP JSON from config: %s", exc)
        return {}

    mapping: dict[str, str] = {}
    for pair in raw.split(","):
        if ":" not in pair:
            continue
        src, dst = pair.split(":", 1)
        src = src.strip()
        dst = dst.strip()
        if src and dst:
            mapping[src] = dst
    return mapping


def _normalize_root(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw == "/":
        return raw
    return raw.rstrip("/") or raw


def parse_files_roots(value: Any) -> list[str]:
    """Parse FILES_ROOTS from list/tuple/set, CSV, JSON array, or nested JSON."""
    out: list[str] = []
    queue: list[Any] = [value]
    seen: set[str] = set()

    while queue:
        item = queue.pop(0)
        if item is None:
            continue
        if isinstance(item, (list, tuple, set)):
            queue.extend(list(item))
            continue

        if isinstance(item, str):
            raw = item.strip()
            if not raw:
                continue

            if raw[0] in {"[", '"'}:
                try:
                    parsed = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    parsed = None
                if parsed is not None and parsed is not item:
                    queue.append(parsed)
                    continue

            if "," in raw:
                parts = [part.strip() for part in raw.split(",") if part.strip()]
                if len(parts) > 1:
                    queue.extend(parts)
                    continue

            if raw and not raw.startswith("["):
                normalized = _normalize_root(raw)
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    out.append(normalized)
            continue

        raw = str(item).strip()
        if raw and not raw.startswith("["):
            normalized = _normalize_root(raw)
            if normalized and normalized not in seen:
                seen.add(normalized)
                out.append(normalized)

    return out


def parse_skip_folders(value: Any) -> list[str]:
    """Parse SKIP_FOLDERS from list, JSON array, or CSV while dropping corrupt JSON-like entries."""
    if value is None:
        return []
    if isinstance(value, list):
        raw = [str(item).strip() for item in value if str(item).strip()]
    else:
        text = str(value).strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                parsed = json.loads(text)
                raw = [str(item).strip() for item in parsed if str(item).strip()] if isinstance(parsed, list) else []
            except (json.JSONDecodeError, TypeError):
                raw = [part.strip() for part in text.split(",") if part.strip()]
        else:
            raw = [part.strip() for part in text.split(",") if part.strip()]
    return [item for item in raw if item and not item.startswith("[")]


def parse_format_preference(value: Any, default: list[str] | None = None) -> list[str]:
    """Parse FORMAT_PREFERENCE from list, JSON array, or CSV."""
    fallback = list(default or DEFAULT_FORMAT_PREFERENCE)
    if value is None or (isinstance(value, str) and not value.strip()):
        return fallback
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if raw.startswith("["):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, TypeError):
                pass
            return fallback
        parts = [part.strip() for part in raw.split(",") if part.strip()]
        return parts if parts else fallback
    return fallback


def normalize_library_mode(value: Any) -> str:
    """PMDA is files-only; any legacy mode normalizes to files."""
    return "files"


def normalize_library_workflow_mode(value: Any, default: str = "managed") -> str:
    mode = str(value or "").strip().lower()
    if mode in {"managed", "mirror", "inplace", "custom", "audit"}:
        return mode
    return str(default or "managed").strip().lower() or "managed"


def normalize_library_scope(value: Any, default: str = "library") -> str:
    scope = str(value or "").strip().lower()
    if scope in {"library", "inbox", "dupes", "all"}:
        return scope
    return str(default or "library").strip().lower() or "library"


def normalize_files_root_path(value: Any) -> str:
    raw = re.sub(r"/+", "/", str(value or "").strip().replace("\\", "/"))
    if not raw:
        return ""
    if not raw.startswith("/"):
        raw = "/" + raw
    return raw.rstrip("/")
