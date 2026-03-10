import { cn } from '@/lib/utils';
import { normalizeProviderId } from '@/lib/providerMeta';

export type PMDABadgeKind =
  | 'genre'
  | 'source'
  | 'label'
  | 'year'
  | 'count'
  | 'duration'
  | 'lossless'
  | 'lossy'
  | 'status_match'
  | 'status_soft'
  | 'status_no_match'
  | 'track_meta'
  | 'muted';

export function badgeKindClass(kind: PMDABadgeKind): string {
  switch (kind) {
    case 'genre':
      return 'border-violet-300 bg-violet-100 text-violet-900 dark:border-violet-700 dark:bg-violet-900 dark:text-violet-100';
    case 'source':
      return 'border-sky-300 bg-sky-100 text-sky-900 dark:border-sky-700 dark:bg-sky-900 dark:text-sky-100';
    case 'label':
      return 'border-amber-300 bg-amber-100 text-amber-900 dark:border-amber-700 dark:bg-amber-900 dark:text-amber-100';
    case 'year':
      return 'border-indigo-300 bg-indigo-100 text-indigo-900 dark:border-indigo-700 dark:bg-indigo-900 dark:text-indigo-100';
    case 'count':
      return 'border-cyan-300 bg-cyan-100 text-cyan-900 dark:border-cyan-700 dark:bg-cyan-900 dark:text-cyan-100';
    case 'duration':
      return 'border-fuchsia-300 bg-fuchsia-100 text-fuchsia-900 dark:border-fuchsia-700 dark:bg-fuchsia-900 dark:text-fuchsia-100';
    case 'lossless':
      return 'border-emerald-300 bg-emerald-100 text-emerald-900 dark:border-emerald-700 dark:bg-emerald-900 dark:text-emerald-100';
    case 'lossy':
      return 'border-orange-300 bg-orange-100 text-orange-900 dark:border-orange-700 dark:bg-orange-900 dark:text-orange-100';
    case 'status_match':
      return 'border-emerald-300 bg-emerald-100 text-emerald-900 dark:border-emerald-700 dark:bg-emerald-900 dark:text-emerald-100';
    case 'status_soft':
      return 'border-yellow-300 bg-yellow-100 text-yellow-900 dark:border-yellow-700 dark:bg-yellow-900 dark:text-yellow-100';
    case 'status_no_match':
      return 'border-rose-300 bg-rose-100 text-rose-900 dark:border-rose-700 dark:bg-rose-900 dark:text-rose-100';
    case 'track_meta':
      return 'border-slate-300 bg-slate-100 text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100';
    default:
      return 'border-border bg-muted text-foreground';
  }
}

export function providerBadgeClass(provider?: string | null): string {
  const p = normalizeProviderId(provider);
  if (p === 'musicbrainz') return badgeKindClass('status_match');
  if (p === 'discogs') return 'border-emerald-300 bg-emerald-100 text-emerald-900 dark:border-emerald-700 dark:bg-emerald-900 dark:text-emerald-100';
  if (p === 'bandcamp') return 'border-fuchsia-300 bg-fuchsia-100 text-fuchsia-900 dark:border-fuchsia-700 dark:bg-fuchsia-900 dark:text-fuchsia-100';
  if (p === 'lastfm') return 'border-red-300 bg-red-100 text-red-900 dark:border-red-700 dark:bg-red-900 dark:text-red-100';
  if (p === 'wikipedia') return 'border-slate-300 bg-slate-100 text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100';
  if (p === 'openai-api' || p === 'openai-codex') return 'border-emerald-300 bg-emerald-100 text-emerald-900 dark:border-emerald-700 dark:bg-emerald-900 dark:text-emerald-100';
  if (p === 'anthropic') return 'border-orange-300 bg-orange-100 text-orange-900 dark:border-orange-700 dark:bg-orange-900 dark:text-orange-100';
  if (p === 'google') return 'border-blue-300 bg-blue-100 text-blue-900 dark:border-blue-700 dark:bg-blue-900 dark:text-blue-100';
  if (p === 'ollama') return 'border-purple-300 bg-purple-100 text-purple-900 dark:border-purple-700 dark:bg-purple-900 dark:text-purple-100';
  if (p === 'acoustid') return 'border-cyan-300 bg-cyan-100 text-cyan-900 dark:border-cyan-700 dark:bg-cyan-900 dark:text-cyan-100';
  if (p === 'audiodb' || p === 'fanart' || p === 'bandsintown') return badgeKindClass('source');
  if (p === 'local' || p === 'media_cache' || p === 'unknown') return 'border-zinc-300 bg-zinc-100 text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100';
  return badgeKindClass('source');
}

export function matchTypeBadgeClass(matchType?: string | null): string {
  const mt = String(matchType || '').trim().toUpperCase();
  if (mt === 'MATCH') return badgeKindClass('status_match');
  if (mt === 'SOFT_MATCH') return badgeKindClass('status_soft');
  return badgeKindClass('status_no_match');
}

export function withBadgeTone(baseClass: string, kind: PMDABadgeKind): string {
  return cn(baseClass, badgeKindClass(kind));
}
