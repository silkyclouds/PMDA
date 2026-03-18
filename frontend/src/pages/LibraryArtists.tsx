import { useCallback, useEffect, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutletContext } from 'react-router-dom';
import { Loader2, UserRound } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { AuthenticatedImage } from '@/components/library/AuthenticatedImage';
import { Card } from '@/components/ui/card';
import { LibraryEmptyState } from '@/components/library/LibraryEmptyState';
import { useLibraryQuery } from '@/hooks/useLibraryQuery';
import { withBackLinkState } from '@/lib/backNavigation';
import * as api from '@/lib/api';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';

export default function LibraryArtists() {
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched, libraryIsEmpty } = useOutletContext<LibraryOutletContext>();
  const { search, genre, label, year } = useLibraryQuery();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [artists, setArtists] = useState<api.LibraryArtistItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [appending, setAppending] = useState(false);
  const limit = 120;

  const requestIdRef = useRef(0);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const loadingMoreRef = useRef(false);

  const fetchArtists = useCallback(async (opts: { reset: boolean; pageOffset: number }) => {
    const rid = ++requestIdRef.current;
    try {
      if (opts.reset) {
        setLoading(true);
        setError(null);
      }
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
    if (libraryIsEmpty) {
      setArtists([]);
      setTotal(0);
      setOffset(0);
      setLoading(false);
      return;
    }
    setOffset(0);
    void fetchArtists({ reset: true, pageOffset: 0 });
  }, [search, genre, label, year, includeUnmatched, fetchArtists, libraryIsEmpty]);

  const canLoadMore = artists.length < total && !loading;
  const loadMore = useCallback(async () => {
    if (!canLoadMore || loadingMoreRef.current) return;
    loadingMoreRef.current = true;
    setAppending(true);
    try {
      await fetchArtists({ reset: false, pageOffset: offset });
    } finally {
      setAppending(false);
      loadingMoreRef.current = false;
    }
  }, [canLoadMore, fetchArtists, offset]);

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
  }, [artists.length, loadMore, total]);

  if (libraryIsEmpty) {
    return (
      <div className="container pb-6">
        <LibraryEmptyState />
      </div>
    );
  }

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

      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4">
        {artists.map((a) => (
          <button
            key={`artist-${a.artist_id}`}
            type="button"
            onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`, { state: withBackLinkState(location) })}
            className="text-left"
            title="Open artist"
          >
            <Card className="pmda-flat-tile overflow-hidden transition-all duration-300 hover:-translate-y-1 hover:border-primary/35 hover:bg-accent/20">
              <div className="aspect-square w-full bg-muted overflow-hidden border-b border-border/60 flex items-center justify-center">
                {a.artist_thumb ? (
                  <AuthenticatedImage
                    src={a.artist_thumb}
                    alt={a.artist_name}
                    className="w-full h-full object-cover animate-in fade-in-0 duration-300"
                  />
                ) : (
                  <UserRound className="w-10 h-10 text-muted-foreground" />
                )}
              </div>
              <div className="space-y-2 p-4">
                <div className="text-base font-semibold leading-tight line-clamp-2 min-h-[2.5rem]">
                  {a.artist_name}
                </div>
                <div className="text-xs text-muted-foreground">
                  {a.album_count} album{a.album_count === 1 ? '' : 's'}
                </div>
              </div>
            </Card>
          </button>
        ))}
      </div>

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
