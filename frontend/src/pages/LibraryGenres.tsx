import { useCallback, useEffect, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutletContext } from 'react-router-dom';
import { Loader2, Tag } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Card } from '@/components/ui/card';
import { useLibraryQuery } from '@/hooks/useLibraryQuery';
import * as api from '@/lib/api';
import { cn } from '@/lib/utils';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';

export default function LibraryGenres() {
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched } = useOutletContext<LibraryOutletContext>();
  const { search, label, year } = useLibraryQuery();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [genres, setGenres] = useState<api.LibraryFacetItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [appending, setAppending] = useState(false);
  const limit = 120;

  const requestIdRef = useRef(0);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const loadingMoreRef = useRef(false);

  const fetchGenres = useCallback(async (opts: { reset: boolean; pageOffset: number }) => {
    const rid = ++requestIdRef.current;
    try {
      if (opts.reset) {
        setLoading(true);
        setError(null);
      }
      const res = await api.getLibraryGenres({
        search: search.trim() || undefined,
        label: label || undefined,
        year: year ?? undefined,
        includeUnmatched,
        limit,
        offset: opts.pageOffset,
      });
      if (rid !== requestIdRef.current) return;
      const list = Array.isArray(res.genres) ? res.genres : [];
      setGenres((prev) => (opts.reset ? list : [...prev, ...list]));
      setTotal(typeof res.total === 'number' ? res.total : 0);
      setOffset(opts.pageOffset + list.length);
    } catch (e) {
      if (rid !== requestIdRef.current) return;
      setError(e instanceof Error ? e.message : 'Failed to load genres');
      if (opts.reset) {
        setGenres([]);
        setTotal(0);
        setOffset(0);
      }
    } finally {
      if (rid === requestIdRef.current) setLoading(false);
    }
  }, [includeUnmatched, label, limit, search, year]);

  useEffect(() => {
    setOffset(0);
    void fetchGenres({ reset: true, pageOffset: 0 });
  }, [search, label, year, includeUnmatched, fetchGenres]);

  const canLoadMore = genres.length < total && !loading;
  const loadMore = useCallback(async () => {
    if (!canLoadMore || loadingMoreRef.current) return;
    loadingMoreRef.current = true;
    setAppending(true);
    try {
      await fetchGenres({ reset: false, pageOffset: offset });
    } finally {
      setAppending(false);
      loadingMoreRef.current = false;
    }
  }, [canLoadMore, fetchGenres, offset]);

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
  }, [genres.length, loadMore, total]);

  return (
    <div className="container pb-6 space-y-4">
      <div className="flex items-end justify-between gap-3">
        <div className="space-y-1">
          <div className="text-lg font-semibold">Genres</div>
          <div className="text-xs text-muted-foreground">
            {total > 0 ? `${genres.length.toLocaleString()} / ${total.toLocaleString()}` : ' '}
          </div>
        </div>
        {error ? (
          <Badge variant="outline" className="text-xs border-destructive/50 text-destructive">
            {error}
          </Badge>
        ) : null}
      </div>

      <Card className="border-border/60 overflow-hidden">
        {loading && genres.length === 0 ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground p-4">
            <Loader2 className="w-4 h-4 animate-spin" /> Loading genres…
          </div>
        ) : genres.length === 0 ? (
          <div className="p-4 text-sm text-muted-foreground">No genres found.</div>
        ) : (
          <div className="divide-y divide-border/50">
            {genres.map((g) => (
              <button
                key={`genre-${g.value}`}
                type="button"
                className={cn('w-full px-4 py-3 text-left hover:bg-accent/40 transition-colors')}
                onClick={() => navigate(`/library/genre/${encodeURIComponent(g.value)}${location.search || ''}`)}
                title="Open genre"
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="min-w-0 flex items-center gap-2">
                    <Tag className="h-4 w-4 text-muted-foreground shrink-0" />
                    <div className="font-medium text-sm truncate">{g.value}</div>
                  </div>
                  <div className="text-xs text-muted-foreground tabular-nums shrink-0">{g.count}</div>
                </div>
              </button>
            ))}
          </div>
        )}
      </Card>

      <div ref={sentinelRef} className="h-6" />
      <div className="flex items-center justify-center py-2 text-xs text-muted-foreground">
        {canLoadMore ? (appending ? <span className="inline-flex items-center gap-2"><Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading more…</span> : 'Scroll to load more') : 'All loaded'}
      </div>
    </div>
  );
}
