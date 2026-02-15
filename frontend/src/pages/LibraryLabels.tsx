import { useCallback, useEffect, useRef, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { Building2, Loader2 } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { useLibraryQuery } from '@/hooks/useLibraryQuery';
import * as api from '@/lib/api';
import { cn } from '@/lib/utils';

export default function LibraryLabels() {
  const navigate = useNavigate();
  const location = useLocation();
  const { search, genre, year } = useLibraryQuery();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [labels, setLabels] = useState<api.LibraryFacetItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const limit = 120;

  const requestIdRef = useRef(0);

  const fetchLabels = useCallback(async (opts: { reset: boolean; pageOffset: number }) => {
    const rid = ++requestIdRef.current;
    try {
      setLoading(true);
      setError(null);
      const res = await api.getLibraryLabels({
        search: search.trim() || undefined,
        genre: genre || undefined,
        year: year ?? undefined,
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
  }, [genre, limit, search, year]);

  useEffect(() => {
    setOffset(0);
    void fetchLabels({ reset: true, pageOffset: 0 });
  }, [search, genre, year, fetchLabels]);

  const canLoadMore = labels.length < total && !loading;

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
                    <Building2 className="h-4 w-4 text-muted-foreground shrink-0" />
                    <div className="font-medium text-sm truncate">{l.value}</div>
                  </div>
                  <div className="text-xs text-muted-foreground tabular-nums shrink-0">{l.count}</div>
                </div>
              </button>
            ))}
          </div>
        )}
      </Card>

      <div className="flex items-center justify-center py-6">
        <Button
          variant="outline"
          className="gap-2"
          onClick={() => void fetchLabels({ reset: false, pageOffset: offset })}
          disabled={!canLoadMore}
        >
          {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
          {canLoadMore ? 'Load more' : 'All loaded'}
        </Button>
      </div>
    </div>
  );
}

