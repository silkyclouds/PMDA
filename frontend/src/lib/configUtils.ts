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
  if (out.NORMALIZE_PARENTHETICAL_FOR_DEDUPE !== undefined) out.NORMALIZE_PARENTHETICAL_FOR_DEDUPE = toBool(out.NORMALIZE_PARENTHETICAL_FOR_DEDUPE);
  if (out.BACKUP_BEFORE_FIX !== undefined) out.BACKUP_BEFORE_FIX = toBool(out.BACKUP_BEFORE_FIX);
  if (out.MAGIC_MODE !== undefined) out.MAGIC_MODE = toBool(out.MAGIC_MODE);
  if (out.REPROCESS_INCOMPLETE_ALBUMS !== undefined) out.REPROCESS_INCOMPLETE_ALBUMS = toBool(out.REPROCESS_INCOMPLETE_ALBUMS);
  if (out.IMPROVE_ALL_WORKERS !== undefined) out.IMPROVE_ALL_WORKERS = Math.max(1, Math.min(8, Number(out.IMPROVE_ALL_WORKERS) || 1));
  if (out.FFPROBE_POOL_SIZE !== undefined) out.FFPROBE_POOL_SIZE = Math.max(1, Math.min(64, Number(out.FFPROBE_POOL_SIZE) || 8));
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
  return out;
}
