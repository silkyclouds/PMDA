import { useCallback, useEffect, useRef, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { Loader2, Tag } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { useLibraryQuery } from '@/hooks/useLibraryQuery';
import * as api from '@/lib/api';
import { cn } from '@/lib/utils';

export default function LibraryGenres() {
  const navigate = useNavigate();
  const location = useLocation();
  const { search, label, year } = useLibraryQuery();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [genres, setGenres] = useState<api.LibraryFacetItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const limit = 120;

  const requestIdRef = useRef(0);

  const fetchGenres = useCallback(async (opts: { reset: boolean; pageOffset: number }) => {
    const rid = ++requestIdRef.current;
    try {
      setLoading(true);
      setError(null);
      const res = await api.getLibraryGenres({
        search: search.trim() || undefined,
        label: label || undefined,
        year: year ?? undefined,
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
  }, [label, limit, search, year]);

  useEffect(() => {
    setOffset(0);
    void fetchGenres({ reset: true, pageOffset: 0 });
  }, [search, label, year, fetchGenres]);

  const canLoadMore = genres.length < total && !loading;

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

      <div className="flex items-center justify-center py-6">
        <Button
          variant="outline"
          className="gap-2"
          onClick={() => void fetchGenres({ reset: false, pageOffset: offset })}
          disabled={!canLoadMore}
        >
          {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
          {canLoadMore ? 'Load more' : 'All loaded'}
        </Button>
      </div>
    </div>
  );
}

