import { useCallback, useEffect, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutletContext } from 'react-router-dom';
import { Building2, Loader2 } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { AuthenticatedImage } from '@/components/library/AuthenticatedImage';
import { Card } from '@/components/ui/card';
import { LibraryEmptyState } from '@/components/library/LibraryEmptyState';
import { useLibraryQuery } from '@/hooks/useLibraryQuery';
import { withBackLinkState } from '@/lib/backNavigation';
import * as api from '@/lib/api';
import { cn } from '@/lib/utils';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';

export default function LibraryLabels() {
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched, scope, libraryIsEmpty, emptyState, setScope } = useOutletContext<LibraryOutletContext>();
  const { search, genre, year } = useLibraryQuery();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [labels, setLabels] = useState<api.LibraryFacetItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [appending, setAppending] = useState(false);
  const limit = 120;

  const requestIdRef = useRef(0);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const loadingMoreRef = useRef(false);
  const logoRefreshAttemptsRef = useRef(0);

  const fetchLabels = useCallback(async (opts: { reset: boolean; pageOffset: number }) => {
    const rid = ++requestIdRef.current;
    try {
      if (opts.reset) {
        setLoading(true);
        setError(null);
      }
      const res = await api.getLibraryLabels({
        search: search.trim() || undefined,
        genre: genre || undefined,
        year: year ?? undefined,
        includeUnmatched,
        scope,
        limit,
        offset: opts.pageOffset,
        refresh: false,
      });
      if (rid !== requestIdRef.current) return;
      const list = Array.isArray(res.labels) ? res.labels : [];
      setLabels((prev) => (opts.reset ? list : [...prev, ...list]));
      setTotal(typeof res.total === 'number' ? res.total : 0);
      setOffset(opts.pageOffset + list.length);
    } catch (e) {
      if (rid !== requestIdRef.current) return;
      setError(e instanceof Error ? e.message : 'Failed to load labels');
      if (opts.reset) {
        setLabels([]);
        setTotal(0);
        setOffset(0);
      }
    } finally {
      if (rid === requestIdRef.current) setLoading(false);
    }
  }, [genre, includeUnmatched, limit, scope, search, year]);

  useEffect(() => {
    if (libraryIsEmpty) {
      setLabels([]);
      setTotal(0);
      setOffset(0);
      setLoading(false);
      return;
    }
    setOffset(0);
    logoRefreshAttemptsRef.current = 0;
    void fetchLabels({ reset: true, pageOffset: 0 });
  }, [search, genre, year, includeUnmatched, fetchLabels, libraryIsEmpty]);

  useEffect(() => {
    if (labels.length === 0) return;
    const missing = labels.filter((labelItem) => !String(labelItem.thumb || '').trim());
    if (missing.length === 0) return;
    if (logoRefreshAttemptsRef.current >= 3) return;

    let cancelled = false;
    const attemptNo = logoRefreshAttemptsRef.current;
    const timer = setTimeout(async () => {
      if (cancelled) return;
      logoRefreshAttemptsRef.current += 1;
      try {
        const res = await api.getLibraryLabels({
          search: search.trim() || undefined,
          genre: genre || undefined,
          year: year ?? undefined,
          includeUnmatched,
          scope,
          limit,
          offset: 0,
          refresh: true,
        });
        if (cancelled) return;
        const list = Array.isArray(res.labels) ? res.labels : [];
        setLabels(list);
        setTotal(typeof res.total === 'number' ? res.total : 0);
        setOffset(list.length);
      } catch {
        // Best-effort only.
      }
    }, 1800 + attemptNo * 2600);
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [genre, includeUnmatched, labels, limit, scope, search, year]);

  const canLoadMore = labels.length < total && !loading;
  const loadMore = useCallback(async () => {
    if (!canLoadMore || loadingMoreRef.current) return;
    loadingMoreRef.current = true;
    setAppending(true);
    try {
      await fetchLabels({ reset: false, pageOffset: offset });
    } finally {
      setAppending(false);
      loadingMoreRef.current = false;
    }
  }, [canLoadMore, fetchLabels, offset]);

  useEffect(() => {
    const node = sentinelRef.current;
    if (!node) return;
    const obs = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            void loadMore();
            break;
          }
        }
      },
      { root: null, rootMargin: '900px 0px 900px 0px', threshold: 0.01 },
    );
    obs.observe(node);
    return () => obs.disconnect();
  }, [labels.length, loadMore, total]);

  if (libraryIsEmpty) {
    return (
      <div className="pmda-library-shell pb-6">
        <LibraryEmptyState
          title={emptyState.title}
          description={emptyState.description}
          actionLabel={emptyState.actionLabel ?? undefined}
          onAction={emptyState.actionScope ? () => setScope(emptyState.actionScope as api.LibraryBrowseScope) : undefined}
        />
      </div>
    );
  }

  return (
    <div className="pmda-page-shell pb-6 pmda-page-stack">
      <div className="flex items-end justify-between gap-3">
        <div className="space-y-1">
          <h1 className="pmda-page-title">Labels</h1>
          <div className="text-xs text-muted-foreground">
            {total > 0 ? `${labels.length.toLocaleString()} / ${total.toLocaleString()}` : ' '}
          </div>
        </div>
        {error ? (
          <Badge variant="outline" className="text-xs border-destructive/50 text-destructive">
            {error}
          </Badge>
        ) : null}
      </div>

      <Card className="border-border/60 overflow-hidden">
        {loading && labels.length === 0 ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground p-4">
            <Loader2 className="w-4 h-4 animate-spin" /> Loading labels…
          </div>
        ) : labels.length === 0 ? (
          <div className="p-4 text-sm text-muted-foreground">No labels found.</div>
        ) : (
          <div className="divide-y divide-border/50">
            {labels.map((l) => (
              <button
                key={`label-${l.value}`}
                type="button"
                className={cn('w-full px-4 py-3 text-left hover:bg-accent/40 transition-colors')}
                onClick={() => navigate(`/library/label/${encodeURIComponent(l.value)}${location.search || ''}`, { state: withBackLinkState(location) })}
                title="Open label"
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="min-w-0 flex items-center gap-2">
                    <div className="h-20 w-20 rounded-md overflow-hidden border border-border/60 bg-muted shrink-0 flex items-center justify-center">
                      {l.thumb ? (
                        <AuthenticatedImage
                          src={l.thumb}
                          alt={l.value}
                          className="h-full w-full object-contain bg-muted p-2"
                          fallback={<Building2 className="h-6 w-6 text-muted-foreground shrink-0" />}
                        />
                      ) : (
                        <Building2 className="h-6 w-6 text-muted-foreground shrink-0" />
                      )}
                    </div>
                    <div className="font-medium text-sm truncate">{l.value}</div>
                  </div>
                  <div className="text-xs text-muted-foreground tabular-nums shrink-0">{l.count}</div>
                </div>
              </button>
            ))}
          </div>
        )}
      </Card>

      <div ref={sentinelRef} className="h-6" />
      <div className="flex min-h-6 items-center justify-center py-2 text-xs text-muted-foreground">
        {appending ? (
          <span className="inline-flex items-center gap-2">
            <Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading more…
          </span>
        ) : total > 0 && !canLoadMore ? 'All loaded' : null}
      </div>
    </div>
  );
}
