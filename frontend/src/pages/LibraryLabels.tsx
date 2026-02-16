import { useCallback, useEffect, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutletContext } from 'react-router-dom';
import { Building2, Loader2 } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Card } from '@/components/ui/card';
import { useLibraryQuery } from '@/hooks/useLibraryQuery';
import * as api from '@/lib/api';
import { cn } from '@/lib/utils';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';

export default function LibraryLabels() {
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched } = useOutletContext<LibraryOutletContext>();
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
        limit,
        offset: opts.pageOffset,
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
  }, [genre, includeUnmatched, limit, search, year]);

  useEffect(() => {
    setOffset(0);
    void fetchLabels({ reset: true, pageOffset: 0 });
  }, [search, genre, year, includeUnmatched, fetchLabels]);

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

  return (
    <div className="container pb-6 space-y-4">
      <div className="flex items-end justify-between gap-3">
        <div className="space-y-1">
          <div className="text-lg font-semibold">Labels</div>
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
                onClick={() => navigate(`/library/label/${encodeURIComponent(l.value)}${location.search || ''}`)}
                title="Open label"
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="min-w-0 flex items-center gap-2">
                    <div className="h-7 w-7 rounded-md overflow-hidden border border-border/60 bg-muted shrink-0 flex items-center justify-center">
                      {l.thumb ? (
                        <img src={l.thumb} alt={l.value} className="h-full w-full object-cover" loading="lazy" decoding="async" />
                      ) : (
                        <Building2 className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
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
      <div className="flex items-center justify-center py-2 text-xs text-muted-foreground">
        {canLoadMore ? (appending ? <span className="inline-flex items-center gap-2"><Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading more…</span> : 'Scroll to load more') : 'All loaded'}
      </div>
    </div>
  );
}
