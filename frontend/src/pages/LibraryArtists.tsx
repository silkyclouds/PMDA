import { useCallback, useEffect, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutletContext } from 'react-router-dom';
import { Loader2, UserRound } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { useLibraryQuery } from '@/hooks/useLibraryQuery';
import * as api from '@/lib/api';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';

export default function LibraryArtists() {
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched } = useOutletContext<LibraryOutletContext>();
  const { search, genre, label, year } = useLibraryQuery();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [artists, setArtists] = useState<api.LibraryArtistItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const limit = 120;

  const requestIdRef = useRef(0);

  const fetchArtists = useCallback(async (opts: { reset: boolean; pageOffset: number }) => {
    const rid = ++requestIdRef.current;
    try {
      setLoading(true);
      setError(null);
      const res = await api.getLibraryArtists({
        search: search.trim() || undefined,
        genre: genre || undefined,
        label: label || undefined,
        year: year ?? undefined,
        includeUnmatched,
        limit,
        offset: opts.pageOffset,
      });
      if (rid !== requestIdRef.current) return;
      const list = Array.isArray(res.artists) ? res.artists : [];
      setArtists((prev) => (opts.reset ? list : [...prev, ...list]));
      setTotal(typeof res.total === 'number' ? res.total : 0);
      setOffset(opts.pageOffset + list.length);
    } catch (e) {
      if (rid !== requestIdRef.current) return;
      setError(e instanceof Error ? e.message : 'Failed to load artists');
      if (opts.reset) {
        setArtists([]);
        setTotal(0);
        setOffset(0);
      }
    } finally {
      if (rid === requestIdRef.current) setLoading(false);
    }
  }, [genre, includeUnmatched, label, limit, search, year]);

  useEffect(() => {
    setOffset(0);
    void fetchArtists({ reset: true, pageOffset: 0 });
  }, [search, genre, label, year, includeUnmatched, fetchArtists]);

  const canLoadMore = artists.length < total && !loading;

  return (
    <div className="container pb-6 space-y-4">
      <div className="flex items-end justify-between gap-3">
        <div className="space-y-1">
          <div className="text-lg font-semibold">Artists</div>
          <div className="text-xs text-muted-foreground">
            {total > 0 ? `${artists.length.toLocaleString()} / ${total.toLocaleString()}` : ' '}
          </div>
        </div>
        {error ? (
          <Badge variant="outline" className="text-xs border-destructive/50 text-destructive">
            {error}
          </Badge>
        ) : null}
      </div>

      {loading && artists.length === 0 ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-10">
          <Loader2 className="w-4 h-4 animate-spin" /> Loading artists…
        </div>
      ) : null}

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
        {artists.map((a) => (
          <button
            key={`artist-${a.artist_id}`}
            type="button"
            onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)}
            className="text-left"
            title="Open artist"
          >
            <Card className="border-border/60 bg-card hover:bg-accent/30 transition-colors p-4">
              <div className="flex items-center gap-3">
                <div className="h-12 w-12 rounded-full bg-muted overflow-hidden shrink-0 flex items-center justify-center border border-border/60">
                  {a.artist_thumb ? (
                    <img src={a.artist_thumb} alt={a.artist_name} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                  ) : (
                    <UserRound className="w-5 h-5 text-muted-foreground" />
                  )}
                </div>
                <div className="min-w-0 flex-1">
                  <div className="text-sm font-semibold truncate">{a.artist_name}</div>
                  <div className="text-xs text-muted-foreground mt-0.5">
                    {a.album_count} album{a.album_count === 1 ? '' : 's'}
                  </div>
                  {a.broken_albums_count ? (
                    <div className="mt-2">
                      <Badge variant="outline" className="text-[10px] border-amber-500/40 text-amber-700 dark:text-amber-300">
                        {a.broken_albums_count} incomplete
                      </Badge>
                    </div>
                  ) : null}
                </div>
              </div>
            </Card>
          </button>
        ))}
      </div>

      <div className="flex items-center justify-center py-6">
        <Button
          variant="outline"
          className="gap-2"
          onClick={() => void fetchArtists({ reset: false, pageOffset: offset })}
          disabled={!canLoadMore}
        >
          {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
          {canLoadMore ? 'Load more' : 'All loaded'}
        </Button>
      </div>
    </div>
  );
}
