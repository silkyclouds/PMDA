import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutletContext } from 'react-router-dom';
import { Heart, Loader2, Play, UserRound } from 'lucide-react';

import { AspectRatio } from '@/components/ui/aspect-ratio';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { AlbumBadgeGroups } from '@/components/library/AlbumBadgeGroups';
import { AlbumArtwork } from '@/components/library/AlbumArtwork';
import { GridSizeControl } from '@/components/library/GridSizeControl';
import { LibraryEmptyState } from '@/components/library/LibraryEmptyState';
import { usePlayback } from '@/contexts/PlaybackContext';
import { useAlbumBadgesVisibility } from '@/hooks/use-album-badges';
import { getLibraryGridTemplateColumns, useLibraryTileSize } from '@/hooks/use-library-tile-size';
import { useToast } from '@/hooks/use-toast';
import { useLibraryQuery } from '@/hooks/useLibraryQuery';
import { useIsMobile } from '@/hooks/use-mobile';
import { dedupeAlbumsForDisplay, mergeAlbumsForDisplay } from '@/lib/albumDisplayDedupe';
import { withBackLinkState } from '@/lib/backNavigation';
import { cn } from '@/lib/utils';
import * as api from '@/lib/api';
import type { TrackInfo } from '@/components/library/AudioPlayer';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';

type SortMode = 'recent' | 'year_desc' | 'alpha' | 'artist' | 'user_rating' | 'public_rating' | 'heat';

export default function LibraryAlbums() {
  const isMobile = useIsMobile();
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched, libraryIsEmpty } = useOutletContext<LibraryOutletContext>();
  const { toast } = useToast();
  const { startPlayback, setCurrentTrack } = usePlayback();
  const { showBadges, setShowBadges } = useAlbumBadgesVisibility();
  const { search, genre, label, year } = useLibraryQuery();

  const [albumLoading, setAlbumLoading] = useState(false);
  const [albumError, setAlbumError] = useState<string | null>(null);
  const [albums, setAlbums] = useState<api.LibraryAlbumItem[]>([]);
  const [totalAlbums, setTotalAlbums] = useState(0);
  const [appending, setAppending] = useState(false);

  const [albumLikes, setAlbumLikes] = useState<Record<number, boolean>>({});

  const [sort, setSort] = useState<SortMode>(() => (localStorage.getItem('pmda_library_sort') as SortMode) || 'recent');
  const { tileSize, setTileSize } = useLibraryTileSize();

  const [offset, setOffset] = useState(0);
  const limit = 96;

  const requestIdRef = useRef(0);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const loadingMoreRef = useRef(false);
  const albumLikesRef = useRef<Record<number, boolean>>({});

  useEffect(() => {
    albumLikesRef.current = albumLikes;
  }, [albumLikes]);

  const gridTemplateColumns = useMemo(() => {
    if (isMobile) return 'repeat(2, minmax(0, 1fr))';
    return getLibraryGridTemplateColumns(tileSize, isMobile);
  }, [tileSize, isMobile]);

  const hydrateAlbumLikes = useCallback(async (ids: number[]) => {
    const unique = Array.from(new Set(ids.filter((x) => Number.isFinite(x) && x > 0)));
    if (unique.length === 0) return;
    const unknown = unique.filter((id) => albumLikesRef.current[id] === undefined);
    if (unknown.length === 0) return;
    try {
      const res = await api.getLikes('album', unknown);
      const next: Record<number, boolean> = {};
      for (const it of (res.items || [])) next[it.entity_id] = Boolean(it.liked);
      setAlbumLikes((prev) => ({ ...prev, ...next }));
    } catch {
      // ignore
    }
  }, []);

  const toggleAlbumLike = useCallback(async (albumId: number) => {
    const current = Boolean(albumLikes[albumId]);
    const next = !current;
    setAlbumLikes((prev) => ({ ...prev, [albumId]: next }));
    try {
      await api.setLike({ entity_type: 'album', entity_id: albumId, liked: next, source: 'ui_library_albums' });
    } catch (e) {
      setAlbumLikes((prev) => ({ ...prev, [albumId]: current }));
      toast({
        title: 'Like failed',
        description: e instanceof Error ? e.message : 'Failed to update like',
        variant: 'destructive',
      });
    }
  }, [albumLikes, toast]);

  const fetchAlbums = useCallback(async (opts: { reset: boolean; pageOffset: number }) => {
    const rid = ++requestIdRef.current;
    try {
      if (opts.reset) {
        setAlbumLoading(true);
        setAlbumError(null);
      }
      const data = await api.getLibraryAlbums({
        search: search.trim() || undefined,
        sort,
        limit,
        offset: opts.pageOffset,
        genre: genre || undefined,
        label: label || undefined,
        year: year ?? undefined,
        includeUnmatched,
      });
      if (rid !== requestIdRef.current) return;
      const listRaw = Array.isArray(data.albums) ? data.albums : [];
      const list = dedupeAlbumsForDisplay(listRaw);
      setTotalAlbums(typeof data.total === 'number' ? data.total : 0);
      setAlbums((prev) => {
        if (opts.reset) return list;
        return mergeAlbumsForDisplay(prev, list);
      });
      setOffset(opts.pageOffset + listRaw.length);
      void hydrateAlbumLikes(list.map((a) => a.album_id));
    } catch (e) {
      if (rid !== requestIdRef.current) return;
      setAlbumError(e instanceof Error ? e.message : 'Failed to load albums');
      if (opts.reset) {
        setAlbums([]);
        setOffset(0);
        setTotalAlbums(0);
      }
    } finally {
      if (rid === requestIdRef.current) setAlbumLoading(false);
    }
  }, [genre, hydrateAlbumLikes, includeUnmatched, label, limit, search, sort, year]);

  useEffect(() => {
    try {
      localStorage.setItem('pmda_library_sort', sort);
    } catch {
      // ignore
    }
  }, [sort]);

  useEffect(() => {
    if (libraryIsEmpty) {
      setAlbums([]);
      setOffset(0);
      setTotalAlbums(0);
      setAlbumLoading(false);
      return;
    }
    setOffset(0);
    void fetchAlbums({ reset: true, pageOffset: 0 });
  }, [search, genre, label, year, sort, includeUnmatched, fetchAlbums, libraryIsEmpty]);

  const handlePlayAlbum = async (albumId: number, fallbackTitle: string, fallbackThumb?: string | null) => {
    try {
      const response = await fetch(`/api/library/album/${albumId}/tracks`);
      if (!response.ok) throw new Error('Failed to load tracks');
      const data = await response.json();
      const tracksList: TrackInfo[] = data.tracks || [];
      if (tracksList.length === 0) {
        toast({ title: 'No tracks', description: 'This album has no playable tracks.', variant: 'destructive' });
        return;
      }
      const albumThumb = data.album_thumb || fallbackThumb || null;
      startPlayback(albumId, fallbackTitle || 'Album', albumThumb, tracksList);
      setCurrentTrack(tracksList[0]);
    } catch (error) {
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to load tracks',
        variant: 'destructive',
      });
    }
  };

  const canLoadMore = offset < totalAlbums && !albumLoading;
  const loadMore = useCallback(async () => {
    if (!canLoadMore || loadingMoreRef.current) return;
    loadingMoreRef.current = true;
    setAppending(true);
    try {
      await fetchAlbums({ reset: false, pageOffset: offset });
    } finally {
      setAppending(false);
      loadingMoreRef.current = false;
    }
  }, [canLoadMore, fetchAlbums, offset]);

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
      { root: null, rootMargin: '1000px 0px 1000px 0px', threshold: 0.01 },
    );
    obs.observe(node);
    return () => obs.disconnect();
  }, [albums.length, loadMore, totalAlbums]);

  if (libraryIsEmpty) {
    return (
      <div className="pmda-library-shell pb-6">
        <LibraryEmptyState />
      </div>
    );
  }

  return (
    <div className="pmda-page-shell pb-6 pmda-page-stack">
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-3">
        <div className="space-y-1">
          <h1 className="pmda-page-title">Albums</h1>
          <div className="text-xs text-muted-foreground">
            {totalAlbums > 0 ? `${albums.length.toLocaleString()} / ${totalAlbums.toLocaleString()}` : ' '}
          </div>
        </div>

        <div className="flex flex-col sm:flex-row sm:items-center gap-3">
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-10 sm:h-10"
            onClick={() => setShowBadges(!showBadges)}
          >
            {showBadges ? 'Hide badges' : 'Show badges'}
          </Button>
          <Select value={sort} onValueChange={(v) => setSort(v as SortMode)}>
            <SelectTrigger className="w-full sm:w-[180px] h-10 bg-background/80">
              <SelectValue placeholder="Sort" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="recent">Recently added</SelectItem>
              <SelectItem value="year_desc">Year (desc)</SelectItem>
              <SelectItem value="alpha">Title (A-Z)</SelectItem>
              <SelectItem value="artist">Artist</SelectItem>
              <SelectItem value="user_rating">Your rating</SelectItem>
              <SelectItem value="public_rating">Public rating</SelectItem>
              <SelectItem value="heat">Worth hearing</SelectItem>
            </SelectContent>
          </Select>

          <GridSizeControl value={tileSize} onChange={setTileSize} className="w-full sm:w-[260px]" />
        </div>
      </div>

      {albumError ? (
        <Card className="border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">
          {albumError}
        </Card>
      ) : null}

      <div className="grid gap-4 justify-start" style={{ gridTemplateColumns }}>
        {albums.map((a, idx) => (
          <div key={`alb-${a.album_id}`} className="group">
            <div className={cn(
              'relative overflow-hidden border border-border/60 bg-card'
            )}>
              <AspectRatio
                ratio={1}
                className="bg-muted"
                draggable
                onDragStart={(e) => {
                  try {
                    e.dataTransfer.setData('application/x-pmda-album', JSON.stringify({ album_id: a.album_id }));
                    e.dataTransfer.setData('text/plain', `${a.artist_name} – ${a.title}`);
                    e.dataTransfer.effectAllowed = 'copy';
                  } catch {
                    // ignore
                  }
                }}
              >
                <AlbumArtwork albumThumb={a.thumb} artistId={a.artist_id} alt={a.title} size={512} priority={idx < 24} />
                <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity bg-black/35" />
                <div className="absolute top-2 right-2 opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity">
                  <Button
                    type="button"
                    size="icon"
                    variant="secondary"
                    className={cn('h-9 w-9 rounded-full', albumLikes[a.album_id] ? 'bg-primary text-primary-foreground hover:bg-primary/90' : 'bg-background/70')}
                    onClick={() => void toggleAlbumLike(a.album_id)}
                    title={albumLikes[a.album_id] ? 'Unlike' : 'Like'}
                  >
                    <Heart className={cn('h-4 w-4', albumLikes[a.album_id] ? 'fill-current' : '')} />
                  </Button>
                </div>
                <div className="absolute inset-x-0 bottom-0 p-3 opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity">
                  <div className="flex items-center justify-between gap-2">
                    <Button size="sm" className="h-9 rounded-full gap-2" onClick={() => void handlePlayAlbum(a.album_id, a.title, a.thumb)}>
                      <Play className="h-4 w-4" />
                      Play
                    </Button>
                    <Button size="sm" variant="outline" className="h-9 rounded-full" onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`, { state: withBackLinkState(location) })} title="Open artist">
                      <UserRound className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              </AspectRatio>

              <div className="p-3 space-y-2">
                <div className="min-w-0">
                  <button
                    type="button"
                    className="text-sm font-semibold leading-snug line-clamp-3 min-h-[3.6rem] hover:underline text-left w-full"
                    title="Open album"
                    onClick={() => navigate(`/library/album/${a.album_id}${location.search || ''}`, { state: withBackLinkState(location) })}
                  >
                    {a.title}
                  </button>
                  <button
                    type="button"
                    className="text-xs text-muted-foreground truncate hover:underline"
                    onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`, { state: withBackLinkState(location) })}
                    title="Open artist"
                  >
                    {a.artist_name}
                  </button>
                </div>

                <AlbumBadgeGroups
                  show={showBadges}
                  compact
                  userRating={a.user_rating}
                  publicRating={a.public_rating}
                  publicRatingVotes={a.public_rating_votes}
                  format={a.format}
                  isLossless={a.is_lossless}
                  year={a.year}
                  trackCount={a.track_count}
                  genres={a.genres || (a.genre ? [a.genre] : [])}
                  label={a.label}
                  onGenreClick={(genreName) => navigate(`/library/genre/${encodeURIComponent(genreName)}${location.search || ''}`, { state: withBackLinkState(location) })}
                  onLabelClick={a.label ? () => navigate(`/library/label/${encodeURIComponent(a.label || '')}${location.search || ''}`, { state: withBackLinkState(location) }) : undefined}
                />
                {!showBadges && a.year ? (
                  <div className="text-[11px] text-muted-foreground">{a.year}</div>
                ) : null}
              </div>
            </div>
          </div>
        ))}

        {albumLoading && albums.length === 0 ? (
          <div className="col-span-full flex items-center justify-center py-16 text-muted-foreground">
            <Loader2 className="h-5 w-5 animate-spin mr-2" />
            Loading albums…
          </div>
        ) : null}
      </div>

      <div ref={sentinelRef} className="h-8" />
      <div className="flex items-center justify-center py-2 text-xs text-muted-foreground">
        {appending ? <span className="inline-flex items-center gap-2"><Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading more…</span> : null}
      </div>
    </div>
  );
}
