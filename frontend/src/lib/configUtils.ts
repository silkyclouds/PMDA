import type { PMDAConfig } from '@/lib/api';

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
  if (out.USE_ACOUSTID !== undefined) out.USE_ACOUSTID = toBool(out.USE_ACOUSTID);
  if (out.USE_ACOUSTID_WHEN_TAGGED !== undefined) out.USE_ACOUSTID_WHEN_TAGGED = toBool(out.USE_ACOUSTID_WHEN_TAGGED);
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
    out.AI_USAGE_LEVEL = (['limited', 'medium', 'aggressive'].includes(level) ? level : 'medium') as PMDAConfig['AI_USAGE_LEVEL'];
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
