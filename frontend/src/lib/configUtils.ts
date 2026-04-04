import type { LibraryBrowseScope, PMDAConfig } from '@/lib/api';

/** Coerce API value to boolean (API may return string "True"/"False" from SQLite). */
function toBool(v: unknown): boolean {
  if (typeof v === 'boolean') return v;
  if (v === undefined || v === null) return false;
  const s = String(v).trim().toLowerCase();
  return s === '1' || s === 'true' || s === 'yes' || s === 'on';
}

function parsePathList(value: unknown): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const queue: unknown[] = [value];

  while (queue.length > 0) {
    const item = queue.shift();
    if (item == null) continue;
    if (Array.isArray(item)) {
      queue.push(...item);
      continue;
    }
    if (typeof item === 'string') {
      const s = item.trim();
      if (!s) continue;
      if (s.startsWith('[') || s.startsWith('"')) {
        try {
          const parsed = JSON.parse(s) as unknown;
          if (parsed !== item) {
            queue.push(parsed);
            continue;
          }
        } catch {
          // Fall through to CSV handling.
        }
      }
      if (s.includes(',')) {
        const parts = s.split(',').map((p) => p.trim()).filter(Boolean);
        if (parts.length > 1) {
          queue.push(...parts);
          continue;
        }
      }
      if (!seen.has(s) && !s.startsWith('[')) {
        seen.add(s);
        out.push(s);
      }
      continue;
    }
    const s = String(item).trim();
    if (s && !seen.has(s) && !s.startsWith('[')) {
      seen.add(s);
      out.push(s);
    }
  }

  return out;
}

/** Ensure config values used with .map/.join in UI are safe (array/string/object). Used by Settings page and SettingsWizard. */
export function normalizeConfigForUI(raw: Partial<PMDAConfig>): Partial<PMDAConfig> {
  const out = { ...raw };
  if (out.CROSS_LIBRARY_DEDUPE !== undefined) out.CROSS_LIBRARY_DEDUPE = toBool(out.CROSS_LIBRARY_DEDUPE);
  if (out.AUTO_MOVE_DUPES !== undefined) out.AUTO_MOVE_DUPES = toBool(out.AUTO_MOVE_DUPES);
  if (out.PIPELINE_ENABLE_MATCH_FIX !== undefined) out.PIPELINE_ENABLE_MATCH_FIX = toBool(out.PIPELINE_ENABLE_MATCH_FIX);
  if (out.PIPELINE_ENABLE_DEDUPE !== undefined) out.PIPELINE_ENABLE_DEDUPE = toBool(out.PIPELINE_ENABLE_DEDUPE);
  if (out.PIPELINE_ENABLE_INCOMPLETE_MOVE !== undefined) out.PIPELINE_ENABLE_INCOMPLETE_MOVE = toBool(out.PIPELINE_ENABLE_INCOMPLETE_MOVE);
  if (out.PIPELINE_ENABLE_EXPORT !== undefined) out.PIPELINE_ENABLE_EXPORT = toBool(out.PIPELINE_ENABLE_EXPORT);
  if (out.PIPELINE_ENABLE_PLAYER_SYNC !== undefined) out.PIPELINE_ENABLE_PLAYER_SYNC = toBool(out.PIPELINE_ENABLE_PLAYER_SYNC);
  if (out.PIPELINE_POST_SCAN_ASYNC !== undefined) out.PIPELINE_POST_SCAN_ASYNC = toBool(out.PIPELINE_POST_SCAN_ASYNC);
  if (out.TASK_NOTIFICATIONS_ENABLED !== undefined) out.TASK_NOTIFICATIONS_ENABLED = toBool(out.TASK_NOTIFICATIONS_ENABLED);
  if (out.TASK_NOTIFICATIONS_SUCCESS !== undefined) out.TASK_NOTIFICATIONS_SUCCESS = toBool(out.TASK_NOTIFICATIONS_SUCCESS);
  if (out.TASK_NOTIFICATIONS_FAILURE !== undefined) out.TASK_NOTIFICATIONS_FAILURE = toBool(out.TASK_NOTIFICATIONS_FAILURE);
  if (out.TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN !== undefined) out.TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN = toBool(out.TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN);
  if (out.TASK_NOTIFY_SCAN_CHANGED !== undefined) out.TASK_NOTIFY_SCAN_CHANGED = toBool(out.TASK_NOTIFY_SCAN_CHANGED);
  if (out.TASK_NOTIFY_SCAN_FULL !== undefined) out.TASK_NOTIFY_SCAN_FULL = toBool(out.TASK_NOTIFY_SCAN_FULL);
  if (out.TASK_NOTIFY_ENRICH_BATCH !== undefined) out.TASK_NOTIFY_ENRICH_BATCH = toBool(out.TASK_NOTIFY_ENRICH_BATCH);
  if (out.TASK_NOTIFY_DEDUPE !== undefined) out.TASK_NOTIFY_DEDUPE = toBool(out.TASK_NOTIFY_DEDUPE);
  if (out.TASK_NOTIFY_INCOMPLETE_MOVE !== undefined) out.TASK_NOTIFY_INCOMPLETE_MOVE = toBool(out.TASK_NOTIFY_INCOMPLETE_MOVE);
  if (out.TASK_NOTIFY_EXPORT !== undefined) out.TASK_NOTIFY_EXPORT = toBool(out.TASK_NOTIFY_EXPORT);
  if (out.TASK_NOTIFY_PLAYER_SYNC !== undefined) out.TASK_NOTIFY_PLAYER_SYNC = toBool(out.TASK_NOTIFY_PLAYER_SYNC);
  if (out.SCHEDULER_PAUSED !== undefined) out.SCHEDULER_PAUSED = toBool(out.SCHEDULER_PAUSED);
  if (out.TASK_NOTIFICATIONS_COOLDOWN_SEC !== undefined) {
    out.TASK_NOTIFICATIONS_COOLDOWN_SEC = Math.max(0, Math.min(3600, Number(out.TASK_NOTIFICATIONS_COOLDOWN_SEC) || 0));
  }
  if (out.LIBRARY_INCLUDE_UNMATCHED !== undefined) out.LIBRARY_INCLUDE_UNMATCHED = toBool(out.LIBRARY_INCLUDE_UNMATCHED);
  if (out.LIBRARY_INCLUDE_FORMAT_IN_FOLDER !== undefined) out.LIBRARY_INCLUDE_FORMAT_IN_FOLDER = toBool(out.LIBRARY_INCLUDE_FORMAT_IN_FOLDER);
  if (out.LIBRARY_INCLUDE_TYPE_IN_FOLDER !== undefined) out.LIBRARY_INCLUDE_TYPE_IN_FOLDER = toBool(out.LIBRARY_INCLUDE_TYPE_IN_FOLDER);
  if (out.LIBRARY_HAS_INTAKE !== undefined) out.LIBRARY_HAS_INTAKE = toBool(out.LIBRARY_HAS_INTAKE);
  if (out.MUSICBRAINZ_MIRROR_ENABLED !== undefined) out.MUSICBRAINZ_MIRROR_ENABLED = toBool(out.MUSICBRAINZ_MIRROR_ENABLED);
  if (out.MUSICBRAINZ_REPLICATION_TOKEN_SET !== undefined) out.MUSICBRAINZ_REPLICATION_TOKEN_SET = toBool(out.MUSICBRAINZ_REPLICATION_TOKEN_SET);
  if (out.PROVIDER_GATEWAY_ENABLED !== undefined) out.PROVIDER_GATEWAY_ENABLED = toBool(out.PROVIDER_GATEWAY_ENABLED);
  if (out.PROVIDER_GATEWAY_CACHE_ENABLED !== undefined) out.PROVIDER_GATEWAY_CACHE_ENABLED = toBool(out.PROVIDER_GATEWAY_CACHE_ENABLED);
  if (out.AUTO_TUNE_ENABLED !== undefined) out.AUTO_TUNE_ENABLED = toBool(out.AUTO_TUNE_ENABLED);
  if (out.MANAGED_MUSICBRAINZ_UPDATE_ENABLED !== undefined) out.MANAGED_MUSICBRAINZ_UPDATE_ENABLED = toBool(out.MANAGED_MUSICBRAINZ_UPDATE_ENABLED);
  if (out.METADATA_QUEUE_ENABLED !== undefined) out.METADATA_QUEUE_ENABLED = toBool(out.METADATA_QUEUE_ENABLED);
  if (out.EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER !== undefined) out.EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER = toBool(out.EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER);
  if (out.EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER !== undefined) out.EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER = toBool(out.EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER);
  if (out.NORMALIZE_PARENTHETICAL_FOR_DEDUPE !== undefined) out.NORMALIZE_PARENTHETICAL_FOR_DEDUPE = toBool(out.NORMALIZE_PARENTHETICAL_FOR_DEDUPE);
  if (out.BACKUP_BEFORE_FIX !== undefined) out.BACKUP_BEFORE_FIX = toBool(out.BACKUP_BEFORE_FIX);
  if (out.MAGIC_MODE !== undefined) out.MAGIC_MODE = toBool(out.MAGIC_MODE);
  if (out.REPROCESS_INCOMPLETE_ALBUMS !== undefined) out.REPROCESS_INCOMPLETE_ALBUMS = toBool(out.REPROCESS_INCOMPLETE_ALBUMS);
  if (out.IMPROVE_ALL_WORKERS !== undefined) out.IMPROVE_ALL_WORKERS = Math.max(1, Math.min(8, Number(out.IMPROVE_ALL_WORKERS) || 1));
  if (out.FFPROBE_POOL_SIZE !== undefined) out.FFPROBE_POOL_SIZE = Math.max(1, Math.min(64, Number(out.FFPROBE_POOL_SIZE) || 8));
  if (out.ARTWORK_RAM_CACHE_MB !== undefined) out.ARTWORK_RAM_CACHE_MB = Math.max(0, Math.min(65536, Number(out.ARTWORK_RAM_CACHE_MB) || 0));
  if (out.ARTWORK_RAM_CACHE_TTL_SEC !== undefined) out.ARTWORK_RAM_CACHE_TTL_SEC = Math.max(60, Math.min(60 * 60 * 24 * 30, Number(out.ARTWORK_RAM_CACHE_TTL_SEC) || 21600));
  if (out.ARTWORK_RAM_CACHE_MAX_ITEM_MB !== undefined) out.ARTWORK_RAM_CACHE_MAX_ITEM_MB = Math.max(1, Math.min(64, Number(out.ARTWORK_RAM_CACHE_MAX_ITEM_MB) || 8));
  if (out.ARTWORK_RAM_CACHE_AUTO !== undefined) out.ARTWORK_RAM_CACHE_AUTO = toBool(out.ARTWORK_RAM_CACHE_AUTO);
  if (out.ARTWORK_RAM_CACHE_AUTO_MAX_MB !== undefined) out.ARTWORK_RAM_CACHE_AUTO_MAX_MB = Math.max(0, Math.min(65536, Number(out.ARTWORK_RAM_CACHE_AUTO_MAX_MB) || 0));
  if (out.ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC !== undefined) out.ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC = Math.max(30, Math.min(3600, Number(out.ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC) || 120));
  if (out.PROVIDER_GATEWAY_MAX_INFLIGHT !== undefined) out.PROVIDER_GATEWAY_MAX_INFLIGHT = Math.max(1, Math.min(256, Number(out.PROVIDER_GATEWAY_MAX_INFLIGHT) || 16));
  if (out.PROVIDER_GATEWAY_DISCOGS_RPM !== undefined) out.PROVIDER_GATEWAY_DISCOGS_RPM = Math.max(1, Math.min(600, Number(out.PROVIDER_GATEWAY_DISCOGS_RPM) || 55));
  if (out.PROVIDER_GATEWAY_LASTFM_RPM !== undefined) out.PROVIDER_GATEWAY_LASTFM_RPM = Math.max(1, Math.min(1200, Number(out.PROVIDER_GATEWAY_LASTFM_RPM) || 120));
  if (out.PROVIDER_GATEWAY_BANDCAMP_RPM !== undefined) out.PROVIDER_GATEWAY_BANDCAMP_RPM = Math.max(1, Math.min(240, Number(out.PROVIDER_GATEWAY_BANDCAMP_RPM) || 12));
  if (out.AUTO_TUNE_INTERVAL_SEC !== undefined) out.AUTO_TUNE_INTERVAL_SEC = Math.max(15, Math.min(900, Number(out.AUTO_TUNE_INTERVAL_SEC) || 60));
  if (out.AUTO_TUNE_MB_MIRROR_MIN_RPS !== undefined) out.AUTO_TUNE_MB_MIRROR_MIN_RPS = Math.max(1, Math.min(100, Number(out.AUTO_TUNE_MB_MIRROR_MIN_RPS) || 12));
  if (out.AUTO_TUNE_MB_MIRROR_MAX_RPS !== undefined) out.AUTO_TUNE_MB_MIRROR_MAX_RPS = Math.max(1, Math.min(100, Number(out.AUTO_TUNE_MB_MIRROR_MAX_RPS) || 20));
  if (out.AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN !== undefined) out.AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN = Math.max(1, Math.min(256, Number(out.AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN) || 8));
  if (out.AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP !== undefined) out.AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP = Math.max(1, Math.min(256, Number(out.AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP) || 32));
  if (out.MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS !== undefined) out.MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS = Math.max(1, Math.min(24 * 30, Number(out.MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS) || 24 * 7));
  if (out.METADATA_WORKER_COUNT !== undefined) out.METADATA_WORKER_COUNT = Math.max(0, Math.min(128, Number(out.METADATA_WORKER_COUNT) || 4));
  if (out.METADATA_JOB_BATCH_SIZE !== undefined) out.METADATA_JOB_BATCH_SIZE = Math.max(1, Math.min(500, Number(out.METADATA_JOB_BATCH_SIZE) || 25));
  if (out.USE_ACOUSTID !== undefined) out.USE_ACOUSTID = toBool(out.USE_ACOUSTID);
  if (out.USE_ACOUSTID_WHEN_TAGGED !== undefined) out.USE_ACOUSTID_WHEN_TAGGED = toBool(out.USE_ACOUSTID_WHEN_TAGGED);
  if (out.LASTFM_SCROBBLE_ENABLED !== undefined) out.LASTFM_SCROBBLE_ENABLED = toBool(out.LASTFM_SCROBBLE_ENABLED);
  if (out.LASTFM_NOW_PLAYING_ENABLED !== undefined) out.LASTFM_NOW_PLAYING_ENABLED = toBool(out.LASTFM_NOW_PLAYING_ENABLED);
  if (out.LASTFM_SCROBBLE_CONNECTED !== undefined) out.LASTFM_SCROBBLE_CONNECTED = toBool(out.LASTFM_SCROBBLE_CONNECTED);
  if (out.LASTFM_SCROBBLE_PENDING !== undefined) out.LASTFM_SCROBBLE_PENDING = toBool(out.LASTFM_SCROBBLE_PENDING);
  if (out.MB_RETRY_NOT_FOUND !== undefined) out.MB_RETRY_NOT_FOUND = toBool(out.MB_RETRY_NOT_FOUND);
  if (out.MB_DISABLE_CACHE !== undefined) out.MB_DISABLE_CACHE = toBool(out.MB_DISABLE_CACHE);
  if (out.FORMAT_PREFERENCE != null) {
    const v = out.FORMAT_PREFERENCE as unknown;
    if (typeof v === 'string') {
      try {
        const parsed = JSON.parse(v) as unknown;
        out.FORMAT_PREFERENCE = Array.isArray(parsed) ? parsed : (v as string).split(',').map((s) => s.trim()).filter(Boolean);
      } catch {
        out.FORMAT_PREFERENCE = (v as string).split(',').map((s) => s.trim()).filter(Boolean);
      }
    } else if (!Array.isArray(v)) {
      out.FORMAT_PREFERENCE = undefined;
    }
  }
  if (out.SECTION_IDS != null && Array.isArray(out.SECTION_IDS)) {
    out.SECTION_IDS = (out.SECTION_IDS as unknown as (string | number)[]).map((x) => String(x)).join(',');
  }
  if (out.PATH_MAP != null && typeof out.PATH_MAP === 'string') {
    try {
      out.PATH_MAP = JSON.parse(out.PATH_MAP as unknown as string) as Record<string, string>;
    } catch {
      out.PATH_MAP = {};
    }
  }
  if (out.FILES_ROOTS != null) {
    const roots = parsePathList(out.FILES_ROOTS as unknown);
    out.FILES_ROOTS = roots.join(', ');
  }
  if (out.LIBRARY_INTAKE_ROOTS != null) {
    out.LIBRARY_INTAKE_ROOTS = parsePathList(out.LIBRARY_INTAKE_ROOTS as unknown).join(', ');
  }
  if (out.LIBRARY_SOURCE_ROOTS != null) {
    out.LIBRARY_SOURCE_ROOTS = parsePathList(out.LIBRARY_SOURCE_ROOTS as unknown).join(', ');
  }
  if (out.LIBRARY_VISIBLE_SCOPES != null) {
    const scopes = parsePathList(out.LIBRARY_VISIBLE_SCOPES as unknown)
      .map((item) => String(item).trim().toLowerCase())
      .filter((item): item is LibraryBrowseScope => (
        ['library', 'inbox', 'dupes', 'all'].includes(item)
      ));
    out.LIBRARY_VISIBLE_SCOPES = scopes;
  }
  if (out.LIBRARY_WORKFLOW_MODE != null) {
    const mode = String(out.LIBRARY_WORKFLOW_MODE).trim().toLowerCase();
    out.LIBRARY_WORKFLOW_MODE = (
      ['managed', 'mirror', 'inplace', 'custom'].includes(mode) ? mode : 'managed'
    ) as PMDAConfig['LIBRARY_WORKFLOW_MODE'];
  }
  if (out.LIBRARY_MATERIALIZATION_MODE != null) {
    const strategy = String(out.LIBRARY_MATERIALIZATION_MODE).trim().toLowerCase();
    out.LIBRARY_MATERIALIZATION_MODE = (
      ['move', 'hardlink', 'symlink', 'copy'].includes(strategy) ? strategy : 'hardlink'
    ) as PMDAConfig['LIBRARY_MATERIALIZATION_MODE'];
  }
  if (out.METADATA_WORKER_MODE != null) {
    const mode = String(out.METADATA_WORKER_MODE).trim().toLowerCase();
    out.METADATA_WORKER_MODE = (
      ['local', 'hybrid'].includes(mode) ? mode : 'local'
    ) as PMDAConfig['METADATA_WORKER_MODE'];
  }
  if (out.WINNER_SOURCE_ROOT_ID != null) {
    const sid = Number(out.WINNER_SOURCE_ROOT_ID);
    out.WINNER_SOURCE_ROOT_ID = Number.isFinite(sid) && sid > 0 ? String(Math.trunc(sid)) : '';
  }
  if (out.LIBRARY_WINNER_PLACEMENT_STRATEGY != null) {
    const strategy = String(out.LIBRARY_WINNER_PLACEMENT_STRATEGY).trim().toLowerCase();
    out.LIBRARY_WINNER_PLACEMENT_STRATEGY = (
      ['move', 'hardlink', 'symlink', 'copy'].includes(strategy) ? strategy : 'move'
    ) as PMDAConfig['LIBRARY_WINNER_PLACEMENT_STRATEGY'];
  }
  if (out.PIPELINE_PLAYER_TARGET != null) {
    const target = String(out.PIPELINE_PLAYER_TARGET).trim().toLowerCase();
    out.PIPELINE_PLAYER_TARGET = (['none', 'plex', 'jellyfin', 'navidrome'].includes(target) ? target : 'none') as PMDAConfig['PIPELINE_PLAYER_TARGET'];
  }
  if (out.AI_USAGE_LEVEL != null) {
    const level = String(out.AI_USAGE_LEVEL).trim().toLowerCase();
    out.AI_USAGE_LEVEL = (['limited', 'medium', 'aggressive', 'auto'].includes(level) ? level : 'auto') as PMDAConfig['AI_USAGE_LEVEL'];
  }
  if (out.SCAN_AI_POLICY != null) {
    const policy = String(out.SCAN_AI_POLICY).trim().toLowerCase();
    out.SCAN_AI_POLICY = (
      ['local_only', 'local_then_paid', 'paid_only'].includes(policy) ? policy : 'local_only'
    ) as PMDAConfig['SCAN_AI_POLICY'];
  }
  if (out.WEB_SEARCH_PROVIDER != null) {
    const provider = String(out.WEB_SEARCH_PROVIDER).trim().toLowerCase();
    out.WEB_SEARCH_PROVIDER = (
      ['auto', 'serper', 'ollama', 'ai_only', 'disabled'].includes(provider) ? provider : 'auto'
    ) as PMDAConfig['WEB_SEARCH_PROVIDER'];
  }
  if (out.MUSICBRAINZ_RUNTIME_MODE != null) {
    const mode = String(out.MUSICBRAINZ_RUNTIME_MODE).trim().toLowerCase();
    out.MUSICBRAINZ_RUNTIME_MODE = (
      ['managed', 'adopted', 'external', 'absent'].includes(mode) ? mode : 'external'
    ) as PMDAConfig['MUSICBRAINZ_RUNTIME_MODE'];
  }
  if (out.OLLAMA_RUNTIME_MODE != null) {
    const mode = String(out.OLLAMA_RUNTIME_MODE).trim().toLowerCase();
    out.OLLAMA_RUNTIME_MODE = (
      ['managed', 'adopted', 'external', 'absent'].includes(mode) ? mode : 'external'
    ) as PMDAConfig['OLLAMA_RUNTIME_MODE'];
  }
  if (out.CLASSICAL_NAME_PREFERENCE != null) {
    const value = String(out.CLASSICAL_NAME_PREFERENCE).trim().toLowerCase();
    out.CLASSICAL_NAME_PREFERENCE = (
      ['original', 'english'].includes(value) ? value : 'original'
    ) as PMDAConfig['CLASSICAL_NAME_PREFERENCE'];
  }
  if (out.USE_AI_FOR_SOFT_MATCH_PROFILES !== undefined) {
    out.USE_AI_FOR_SOFT_MATCH_PROFILES = toBool(out.USE_AI_FOR_SOFT_MATCH_PROFILES);
  }
  if (out.USE_AI_WEB_SEARCH_FALLBACK !== undefined) {
    out.USE_AI_WEB_SEARCH_FALLBACK = toBool(out.USE_AI_WEB_SEARCH_FALLBACK);
  }
  if (out.OPENAI_ENABLE_API_KEY_MODE !== undefined) {
    out.OPENAI_ENABLE_API_KEY_MODE = toBool(out.OPENAI_ENABLE_API_KEY_MODE);
  }
  if (out.OPENAI_ENABLE_CODEX_OAUTH_MODE !== undefined) {
    out.OPENAI_ENABLE_CODEX_OAUTH_MODE = toBool(out.OPENAI_ENABLE_CODEX_OAUTH_MODE);
  }
  if (out.AI_MAX_CALLS_PER_SCAN !== undefined) {
    out.AI_MAX_CALLS_PER_SCAN = Math.max(0, Math.min(100000, Number(out.AI_MAX_CALLS_PER_SCAN) || 0));
  }
  if (out.AI_CALL_COOLDOWN_SEC !== undefined) {
    out.AI_CALL_COOLDOWN_SEC = Math.max(0, Math.min(30, Number(out.AI_CALL_COOLDOWN_SEC) || 0));
  }
  return out;
}
