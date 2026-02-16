import { useCallback, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';

export interface LibraryQueryState {
  search: string;
  genre: string;
  label: string;
  year: number | null;
  includeUnmatched: boolean | null;
}

function parseYear(raw: string | null): number | null {
  if (!raw) return null;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : null;
}

function parseBoolParam(raw: string | null): boolean | null {
  if (raw == null || raw === '') return null;
  const v = String(raw).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(v)) return true;
  if (['0', 'false', 'no', 'off'].includes(v)) return false;
  return null;
}

export function useLibraryQuery() {
  const [sp, setSp] = useSearchParams();

  const state: LibraryQueryState = useMemo(() => {
    return {
      search: (sp.get('search') || '').trim(),
      genre: (sp.get('genre') || '').trim(),
      label: (sp.get('label') || '').trim(),
      year: parseYear(sp.get('year')),
      includeUnmatched: parseBoolParam(sp.get('include_unmatched')),
    };
  }, [sp]);

  const patch = useCallback(
    (updates: Partial<LibraryQueryState>, opts?: { replace?: boolean }) => {
      const next = new URLSearchParams(sp);
      if (updates.search !== undefined) {
        const v = String(updates.search || '').trim();
        if (v) next.set('search', v);
        else next.delete('search');
      }
      if (updates.genre !== undefined) {
        const v = String(updates.genre || '').trim();
        if (v) next.set('genre', v);
        else next.delete('genre');
      }
      if (updates.label !== undefined) {
        const v = String(updates.label || '').trim();
        if (v) next.set('label', v);
        else next.delete('label');
      }
      if (updates.year !== undefined) {
        const y = updates.year;
        if (y && Number.isFinite(y) && y > 0) next.set('year', String(Math.floor(y)));
        else next.delete('year');
      }
      if (updates.includeUnmatched !== undefined) {
        const v = updates.includeUnmatched;
        if (v == null) next.delete('include_unmatched');
        else next.set('include_unmatched', v ? '1' : '0');
      }
      setSp(next, { replace: Boolean(opts?.replace) });
    },
    [setSp, sp]
  );

  const clearFilters = useCallback(() => {
    patch({ genre: '', label: '', year: null }, { replace: true });
  }, [patch]);

  const clearAll = useCallback(() => {
    patch({ search: '', genre: '', label: '', year: null }, { replace: true });
  }, [patch]);

  return {
    ...state,
    patch,
    clearFilters,
    clearAll,
  };
}
