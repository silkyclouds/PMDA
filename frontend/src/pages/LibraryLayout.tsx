import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link, Outlet, useLocation, useSearchParams } from 'react-router-dom';

import { Button } from '@/components/ui/button';
import { useAuth } from '@/contexts/AuthContext';
import * as api from '@/lib/api';
import type { PMDAConfig } from '@/lib/api';
import { normalizeConfigForUI } from '@/lib/configUtils';
import { useLibraryQuery } from '@/hooks/useLibraryQuery';

export interface LibraryEmptyStateCopy {
  title: string;
  description: string;
  actionLabel?: string | null;
  actionScope?: api.LibraryBrowseScope | null;
}

export interface LibraryOutletContext {
  includeUnmatched: boolean;
  scope: api.LibraryBrowseScope;
  setScope: (scope: api.LibraryBrowseScope) => void;
  stats: api.LibraryStats | null;
  statsByScope: Partial<Record<api.LibraryBrowseScope, api.LibraryStats | null>>;
  statsReady: boolean;
  libraryIsEmpty: boolean;
  visibleScopes: api.LibraryBrowseScope[];
  emptyState: LibraryEmptyStateCopy;
}

export default function LibraryLayout() {
  const location = useLocation();
  
  const [, setSearchParams] = useSearchParams();
  const { isAdmin } = useAuth();
  const [statsByScope, setStatsByScope] = useState<Partial<Record<api.LibraryBrowseScope, api.LibraryStats | null>>>({});
  const [statsReady, setStatsReady] = useState(false);
  const [config, setConfig] = useState<Partial<PMDAConfig>>({});
  const includeUnmatched = true;
  const { scope } = useLibraryQuery();

  const setScope = useCallback((nextScope: api.LibraryBrowseScope) => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (!nextScope || nextScope === 'library') next.delete('scope');
      else next.set('scope', nextScope);
      return next;
    }, { replace: false });
  }, [setSearchParams]);

  useEffect(() => {
    let cancelled = false;
    api
      .getConfig()
      .then((data) => {
        if (cancelled) return;
        setConfig(normalizeConfigForUI(data));
      })
      .catch(() => {
        if (cancelled) return;
        setConfig({});
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const statsScopes = useMemo<api.LibraryBrowseScope[]>(() => ['library', 'all', 'inbox', 'dupes'], []);
  const dupesRootConfigured = Boolean(String(config.LIBRARY_DUPES_ROOT || config.DUPE_ROOT || '').trim());
  const incompleteRootConfigured = Boolean(String(config.LIBRARY_INCOMPLETE_ROOT || config.INCOMPLETE_ALBUMS_TARGET_DIR || '').trim());
  const dupesHasContent = ((statsByScope.dupes?.artists ?? 0) + (statsByScope.dupes?.albums ?? 0) + (statsByScope.dupes?.tracks ?? 0)) > 0;

  const visibleScopes = useMemo<api.LibraryBrowseScope[]>(() => {
    const explicit = Array.isArray(config.LIBRARY_VISIBLE_SCOPES)
      ? config.LIBRARY_VISIBLE_SCOPES
      : String(config.LIBRARY_VISIBLE_SCOPES || '')
        .split(',')
        .map((item) => item.trim().toLowerCase())
        .filter(Boolean);
    const valid = explicit.filter((item): item is api.LibraryBrowseScope => ['library', 'inbox', 'dupes', 'all'].includes(item));
    const mode = (config.LIBRARY_WORKFLOW_MODE || 'managed') as NonNullable<PMDAConfig['LIBRARY_WORKFLOW_MODE']>;
    const hasIntake = Boolean(config.LIBRARY_HAS_INTAKE);
    const wantsConsolidated = mode !== 'inplace' || hasIntake;
    if (valid.length > 0) {
      const migrated = [...valid];
      if (wantsConsolidated && !migrated.includes('all')) {
        const inboxIndex = migrated.indexOf('inbox');
        if (inboxIndex >= 0) migrated.splice(inboxIndex, 0, 'all');
        else migrated.push('all');
      }
      if (!dupesRootConfigured && !dupesHasContent) return migrated.filter((item) => item !== 'dupes');
      return migrated;
    }
    const base: api.LibraryBrowseScope[] = mode === 'inplace'
      ? (hasIntake ? ['library', 'all', 'inbox'] : ['library'])
      : ['library', 'all', 'inbox'];
    if (dupesRootConfigured || dupesHasContent) base.push('dupes');
    return base;
  }, [
    config.LIBRARY_HAS_INTAKE,
    config.LIBRARY_VISIBLE_SCOPES,
    config.LIBRARY_WORKFLOW_MODE,
    dupesHasContent,
    dupesRootConfigured,
  ]);

  useEffect(() => {
    if (!visibleScopes.includes(scope)) {
      setScope(visibleScopes[0] || 'library');
    }
  }, [scope, setScope, visibleScopes]);

  const viewTabs = useMemo(() => (
    visibleScopes.map((tab) => ({
      key: tab,
      label:
        tab === 'library'
          ? 'Library'
          : tab === 'all'
            ? 'Consolidated'
            : tab === 'inbox'
              ? 'Inbox'
              : 'Dupes',
    }))
  ), [visibleScopes]);

  useEffect(() => {
    let cancelled = false;
    setStatsReady(false);
    Promise.all(
      statsScopes.map(async (tabScope) => {
        try {
          const data = await api.getLibraryStats({ includeUnmatched: true, scope: tabScope });
          return [tabScope, data] as const;
        } catch {
          return [tabScope, null] as const;
        }
      }),
    )
      .then((entries) => {
        if (cancelled) return;
        setStatsByScope(
          Object.fromEntries(entries) as Partial<Record<api.LibraryBrowseScope, api.LibraryStats | null>>,
        );
        setStatsReady(true);
      })
      .catch(() => {
        if (cancelled) return;
        setStatsByScope({});
        setStatsReady(true);
      });
    return () => {
      cancelled = true;
    };
  }, [location.pathname, location.search, scope, statsScopes]);

  const stats = statsByScope[scope] ?? null;
  const libraryStats = statsByScope.library ?? null;
  const inboxStats = statsByScope.inbox ?? null;
  const dupesStats = statsByScope.dupes ?? null;

  const libraryIsEmpty =
    statsReady &&
    stats != null &&
    ((stats.artists ?? 0) + (stats.albums ?? 0) + (stats.tracks ?? 0) === 0);

  const emptyState = useMemo<LibraryEmptyStateCopy>(() => {
    const inboxHasContent = ((inboxStats?.artists ?? 0) + (inboxStats?.albums ?? 0) + (inboxStats?.tracks ?? 0)) > 0;
    const libraryHasContent = ((libraryStats?.artists ?? 0) + (libraryStats?.albums ?? 0) + (libraryStats?.tracks ?? 0)) > 0;
    const dupesHasContent = ((dupesStats?.artists ?? 0) + (dupesStats?.albums ?? 0) + (dupesStats?.tracks ?? 0)) > 0;
    if (scope === 'library' && inboxHasContent && !libraryHasContent) {
      return {
        title: 'Library is waiting for promotions',
        description: 'PMDA has discovered music in Inbox, but nothing has been promoted to the clean library yet.',
        actionLabel: 'Open Inbox',
        actionScope: 'inbox',
      };
    }
    if (scope === 'inbox' && !inboxHasContent && libraryHasContent) {
      return {
        title: 'Inbox is clear',
        description: 'PMDA is not holding any unmatched or pending albums in Inbox right now.',
        actionLabel: 'Open Library',
        actionScope: 'library',
      };
    }
    if (scope === 'all' && libraryHasContent && !inboxHasContent) {
      return {
        title: 'Consolidated currently matches Library',
        description: 'Everything PMDA knows about is already promoted into the clean library right now.',
        actionLabel: 'Open Library',
        actionScope: 'library',
      };
    }
    if (scope === 'dupes' && !dupesHasContent) {
      return {
        title: 'Dupes is empty',
        description: 'PMDA has not quarantined any duplicate losers here yet. Incomplete albums are tracked separately in Incompletes.',
        actionLabel: libraryHasContent ? 'Open Library' : null,
        actionScope: libraryHasContent ? 'library' : null,
      };
    }
    return {
      title: 'Library is empty',
      description: 'No artists, albums, or tracks are indexed yet. Run your first scan to populate the library.',
      actionLabel: null,
      actionScope: null,
    };
  }, [dupesStats, inboxStats, libraryStats, scope]);

  const scopeMeta = useMemo(() => {
    const getCounts = (tabScope: api.LibraryBrowseScope) => {
      const scoped = statsByScope[tabScope];
      return {
        artists: scoped?.artists ?? 0,
        albums: scoped?.albums ?? 0,
        tracks: scoped?.tracks ?? 0,
      };
    };
    if (scope === 'library') {
      return {
        title: 'Library',
        eyebrow: 'PMDA-built library',
        description: 'Clean, promoted winners materialized by PMDA. This is the serving library you can trust for matched content.',
        counts: getCounts('library'),
      };
    }
    if (scope === 'all') {
      return {
        title: 'Consolidated',
        eyebrow: 'Library + inbox',
        description: 'Combined view of the clean PMDA library plus albums still waiting for a final decision in monitored folders.',
        counts: getCounts('all'),
      };
    }
    if (scope === 'inbox') {
      return {
        title: 'Inbox',
        eyebrow: 'Monitored intake / pending albums',
        description: 'Albums discovered in watched folders that are still unmatched, provider-only, incomplete, or waiting to be promoted.',
        counts: getCounts('inbox'),
      };
    }
    return {
      title: 'Dupes',
      eyebrow: 'Duplicate quarantine',
      description: 'Duplicate losers quarantined out of the clean library. Review, restore, or confirm them from the duplicate management tools.',
      counts: getCounts('dupes'),
    };
  }, [scope, statsByScope]);

  return (
    <>
      <div className="px-4 md:px-6 pt-4 pb-1">
        <div className="flex flex-wrap items-center gap-3">
          {/* Scope tabs */}
          <div className="inline-flex items-center gap-1.5 rounded-xl border border-border/60 bg-card/40 p-1">
            {viewTabs.map((tab) => {
              const active = scope === tab.key;
              return (
                <Button
                  key={tab.key}
                  type="button"
                  size="sm"
                  variant={active ? 'default' : 'ghost'}
                  className="h-8 px-3"
                  onClick={() => setScope(tab.key)}
                >
                  {tab.label}
                </Button>
              );
            })}
          </div>

          {/* Scope title + compact counters */}
          <div className="flex items-center gap-2 text-sm">
            <span className="font-medium text-foreground">{scopeMeta.title}</span>
            <span className="text-xs text-muted-foreground">
              {Number(scopeMeta.counts.albums).toLocaleString()} albums
              {' · '}
              {Number(scopeMeta.counts.artists).toLocaleString()} artists
              <span className="hidden md:inline">
                {' · '}
                {Number(scopeMeta.counts.tracks).toLocaleString()} tracks
              </span>
            </span>
          </div>

          {/* Action buttons — pushed right */}
          <div className="flex items-center gap-2 ml-auto">
            {isAdmin ? (
              <Button type="button" size="sm" variant="outline" asChild className="h-8">
                <Link to="/broken-albums">Incompletes</Link>
              </Button>
            ) : null}
            {scope === 'dupes' && isAdmin ? (
              <Button type="button" size="sm" variant="outline" asChild className="h-8">
                <Link to="/tools/duplicates">Manage dupes</Link>
              </Button>
            ) : null}
          </div>
        </div>
      </div>
      <Outlet context={{ includeUnmatched, scope, setScope, stats, statsByScope, statsReady, libraryIsEmpty, visibleScopes, emptyState }} />
    </>
  );
}
