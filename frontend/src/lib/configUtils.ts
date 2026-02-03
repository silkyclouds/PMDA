import type { PMDAConfig } from '@/lib/api';

/** Coerce API value to boolean (API may return string "True"/"False" from SQLite). */
function toBool(v: unknown): boolean {
  if (typeof v === 'boolean') return v;
  if (v === undefined || v === null) return false;
  const s = String(v).trim().toLowerCase();
  return s === '1' || s === 'true' || s === 'yes' || s === 'on';
}

/** Ensure config values used with .map/.join in UI are safe (array/string/object). Used by Settings page and SettingsWizard. */
export function normalizeConfigForUI(raw: Partial<PMDAConfig>): Partial<PMDAConfig> {
  const out = { ...raw };
  if (out.CROSS_LIBRARY_DEDUPE !== undefined) out.CROSS_LIBRARY_DEDUPE = toBool(out.CROSS_LIBRARY_DEDUPE);
  if (out.AUTO_MOVE_DUPES !== undefined) out.AUTO_MOVE_DUPES = toBool(out.AUTO_MOVE_DUPES);
  if (out.NORMALIZE_PARENTHETICAL_FOR_DEDUPE !== undefined) out.NORMALIZE_PARENTHETICAL_FOR_DEDUPE = toBool(out.NORMALIZE_PARENTHETICAL_FOR_DEDUPE);
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
  return out;
}
