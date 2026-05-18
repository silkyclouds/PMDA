import { startTransition, useCallback, useEffect, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutletContext } from 'react-router-dom';
import { Loader2, UserRound } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { LibraryLiveStateBadges } from '@/components/library/LibraryLiveStateBadges';
import { Button } from '@/components/ui/button';
import { AuthenticatedImage } from '@/components/library/AuthenticatedImage';
import { GridSizeControl } from '@/components/library/GridSizeControl';
import { Card } from '@/components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { LibraryEmptyState } from '@/components/library/LibraryEmptyState';
import { getLibraryGridTemplateColumns, useLibraryTileSize } from '@/hooks/use-library-tile-size';
import { useLibraryQuery } from '@/hooks/useLibraryQuery';
import { useIsMobile } from '@/hooks/use-mobile';
import { withBackLinkState } from '@/lib/backNavigation';
import * as api from '@/lib/api';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';

type ArtistSortMode = 'recent' | 'alpha' | 'albums' | 'relevance';

export default function LibraryArtists() {
  const isMobile = useIsMobile();
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched, scope, libraryIsEmpty, emptyState, setScope } = useOutletContext<LibraryOutletContext>();
  const { search, genre, label, year, patch } = useLibraryQuery();
  const { tileSize, setTileSize } = useLibraryTileSize();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [artists, setArtists] = useState<api.LibraryArtistItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const [paginationState, setPaginationState] = useState<'idle' | 'all_loaded' | 'page_error'>('idle');
  const [appending, setAppending] = useState(false);
  const [sort, setSort] = useState<ArtistSortMode>(() => (localStorage.getItem('pmda_library_artists_sort') as ArtistSortMode) || 'recent');
  const [genres, setGenres] = useState<api.LibraryFacetItem[]>([]);
  const limit = 120;

  const requestIdRef = useRef(0);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const loadingMoreRef = useRef(false);
  const gridTemplateColumns = getLibraryGridTemplateColumns(tileSize, isMobile, 160, 320);

  const fetchArtists = useCallback(async (opts: { reset: boolean; pageOffset: number; refresh?: boolean; pageLimit?: number; silent?: boolean }) => {
    const rid = ++requestIdRef.current;
    try {
      if (opts.reset && !opts.silent) {
        setLoading(true);
        setError(null);
      }
      const res = await api.getLibraryArtists({
        search: search.trim() || undefined,
        sort,
        genre: genre || undefined,
        label: label || undefined,
        year: year ?? undefined,
        includeUnmatched,
        scope,
        limit: opts.pageLimit ?? limit,
        offset: opts.pageOffset,
        refresh: opts.refresh ?? false,
      });
      if (rid !== requestIdRef.current) return;
      const list = Array.isArray(res.artists) ? res.artists : [];
      const totalNext = typeof res.total === 'number' ? res.total : 0;
      const responseHasMore = typeof res.has_more === 'boolean'
        ? res.has_more
        : (opts.pageOffset + list.length) < totalNext;
      startTransition(() => {
        setError(null);
        setArtists((prev) => (opts.reset ? list : [...prev, ...list]));
        setTotal(totalNext);
        setOffset(opts.pageOffset + list.length);
        setHasMore(Boolean(responseHasMore && list.length > 0));
        setPaginationState(responseHasMore && list.length > 0 ? 'idle' : 'all_loaded');
      });
    } catch (e) {
      if (rid !== requestIdRef.current) return;
      if (!opts.silent && opts.reset) {
        setError(e instanceof Error ? e.message : 'Failed to load artists');
      }
      if (opts.reset && !opts.silent) {
        startTransition(() => {
          setArtists([]);
          setTotal(0);
          setOffset(0);
          setHasMore(false);
          setPaginationState('idle');
        });
      } else if (!opts.reset) {
        startTransition(() => {
          setHasMore(false);
          setPaginationState('page_error');
        });
      }
    } finally {
      if (rid === requestIdRef.current && !opts.silent) setLoading(false);
    }
  }, [genre, includeUnmatched, label, limit, scope, search, sort, year]);

  useEffect(() => {
    if (libraryIsEmpty) {
      setArtists([]);
      setTotal(0);
      setOffset(0);
      setHasMore(false);
      setPaginationState('idle');
      setLoading(false);
      return;
    }
    setOffset(0);
    setHasMore(true);
    setPaginationState('idle');
    void fetchArtists({ reset: true, pageOffset: 0 });
  }, [search, genre, label, year, sort, includeUnmatched, fetchArtists, libraryIsEmpty]);

  useEffect(() => {
    try {
      localStorage.setItem('pmda_library_artists_sort', sort);
    } catch {
      // ignore
    }
  }, [sort]);

  useEffect(() => {
    let cancelled = false;
    api.getLibraryGenres({
      label: label || undefined,
      year: year ?? undefined,
      includeUnmatched,
      scope,
      limit: 120,
    })
      .then((res) => {
        if (cancelled) return;
        setGenres(Array.isArray(res.genres) ? res.genres : []);
      })
      .catch(() => {
        if (cancelled) return;
        setGenres([]);
      });
    return () => {
      cancelled = true;
    };
  }, [includeUnmatched, label, scope, year]);

  const canLoadMore = hasMore && offset < total && !loading;
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
      <div className="flex flex-col md:flex-row md:items-end md:justify-between gap-3">
        <div className="space-y-1">
          <h1 className="pmda-page-title">Artists</h1>
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <span>{total > 0 ? `${artists.length.toLocaleString()} / ${total.toLocaleString()}` : ' '}</span>
          </div>
        </div>
        <div className="flex flex-col sm:flex-row sm:items-center gap-3">
          {error ? (
            <Badge variant="outline" className="text-xs border-destructive/50 text-destructive">
              {error}
            </Badge>
          ) : null}
          <Select value={sort} onValueChange={(value) => setSort(value as ArtistSortMode)}>
            <SelectTrigger className="w-full sm:w-[180px] h-10 bg-background/80">
              <SelectValue placeholder="Sort" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="recent">Recently added</SelectItem>
              <SelectItem value="alpha">Name (A-Z)</SelectItem>
              <SelectItem value="albums">Most albums</SelectItem>
              {search.trim() ? <SelectItem value="relevance">Search relevance</SelectItem> : null}
            </SelectContent>
          </Select>
          <Select
            value={genre || '__all__'}
            onValueChange={(value) => patch({ genre: value === '__all__' ? '' : value }, { replace: true })}
          >
            <SelectTrigger className="w-full sm:w-[220px] h-10 bg-background/80">
              <SelectValue placeholder="All genres" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="__all__">All genres</SelectItem>
              {genres.map((item) => (
                <SelectItem key={`artist-genre-${item.value}`} value={item.value}>
                  {item.value} ({item.count})
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {genre ? (
            <Button type="button" variant="ghost" size="sm" className="h-10 px-3" onClick={() => patch({ genre: '' }, { replace: true })}>
              Clear genre
            </Button>
          ) : null}
          <GridSizeControl value={tileSize} onChange={setTileSize} className="w-full sm:w-[260px]" />
        </div>
      </div>

      {loading && artists.length === 0 ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-10">
          <Loader2 className="w-4 h-4 animate-spin" /> Loading artists…
        </div>
      ) : null}

      <div className="grid gap-4 justify-start" style={{ gridTemplateColumns }}>
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
                {(a.artist_thumb || a.artist_fallback_thumb) ? (
                  <AuthenticatedImage
                    src={a.artist_thumb || a.artist_fallback_thumb}
                    alt={a.artist_name}
                    className="w-full h-full object-cover animate-in fade-in-0 duration-300"
                    fallback={<UserRound className="w-10 h-10 text-muted-foreground" />}
                  />
                ) : (
                  <UserRound className="w-10 h-10 text-muted-foreground" />
                )}
              </div>
              <div className="space-y-2 p-3.5">
                <div className="text-base font-semibold leading-tight line-clamp-2 min-h-[2.5rem]">
                  {a.artist_name}
                </div>
                <div className="text-xs text-muted-foreground">
                  {a.album_count} album{a.album_count === 1 ? '' : 's'}
                </div>
                <LibraryLiveStateBadges item={a} maxBadges={2} className="pt-0.5" />
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
        ) : paginationState === 'page_error' ? (
          'Could not load the next page. Refresh to retry.'
        ) : total > 0 && !canLoadMore ? 'All loaded' : null}
      </div>
    </div>
  );
}
