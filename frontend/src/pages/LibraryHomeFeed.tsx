import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutletContext, useParams } from 'react-router-dom';
import { ArrowLeft, Loader2, Music, Play, UserRound } from 'lucide-react';

import { AspectRatio } from '@/components/ui/aspect-ratio';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { AlbumArtwork } from '@/components/library/AlbumArtwork';
import type { TrackInfo } from '@/components/library/AudioPlayer';
import { usePlayback } from '@/contexts/PlaybackContext';
import { useToast } from '@/hooks/use-toast';
import { useIsMobile } from '@/hooks/use-mobile';
import * as api from '@/lib/api';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';

type FeedSection = 'discover' | 'for_you' | 'top_artists' | 'recent_artists' | 'recently_played' | 'recently_added';

const FEED_TITLES: Record<FeedSection, string> = {
  discover: 'Discover',
  for_you: 'For You',
  top_artists: 'Top Artists',
  recent_artists: 'Recently Added Artists',
  recently_played: 'Recently Played',
  recently_added: 'Recently Added',
};

const ALBUM_FEEDS = new Set<FeedSection>(['discover', 'recently_played', 'recently_added']);
const ARTIST_FEEDS = new Set<FeedSection>(['top_artists', 'recent_artists']);
const TRACK_FEEDS = new Set<FeedSection>(['for_you']);

function asFeedSection(raw: string): FeedSection {
  const value = String(raw || '').trim().toLowerCase();
  if (value === 'discover') return 'discover';
  if (value === 'for_you') return 'for_you';
  if (value === 'top_artists') return 'top_artists';
  if (value === 'recent_artists') return 'recent_artists';
  if (value === 'recently_played') return 'recently_played';
  return 'recently_added';
}

function dedupeAlbumsById(items: api.LibraryAlbumItem[]): api.LibraryAlbumItem[] {
  const out: api.LibraryAlbumItem[] = [];
  const seen = new Set<number>();
  for (const item of items) {
    const id = Number(item?.album_id || 0);
    if (!Number.isFinite(id) || id <= 0 || seen.has(id)) continue;
    seen.add(id);
    out.push(item);
  }
  return out;
}

function mergeAlbumsById(existing: api.LibraryAlbumItem[], incoming: api.LibraryAlbumItem[]): api.LibraryAlbumItem[] {
  if (!existing.length) return dedupeAlbumsById(incoming);
  const out = [...existing];
  const seen = new Set(existing.map((item) => Number(item.album_id || 0)));
  for (const item of incoming) {
    const id = Number(item?.album_id || 0);
    if (!Number.isFinite(id) || id <= 0 || seen.has(id)) continue;
    seen.add(id);
    out.push(item);
  }
  return out;
}

function dedupeArtistsById(items: Array<api.TopArtistItem | api.RecentlyAddedArtistItem>) {
  const out: Array<api.TopArtistItem | api.RecentlyAddedArtistItem> = [];
  const seen = new Set<number>();
  for (const item of items) {
    const id = Number((item as { artist_id?: number })?.artist_id || 0);
    if (!Number.isFinite(id) || id <= 0 || seen.has(id)) continue;
    seen.add(id);
    out.push(item);
  }
  return out;
}

function mergeArtistsById(
  existing: Array<api.TopArtistItem | api.RecentlyAddedArtistItem>,
  incoming: Array<api.TopArtistItem | api.RecentlyAddedArtistItem>,
) {
  if (!existing.length) return dedupeArtistsById(incoming);
  const out = [...existing];
  const seen = new Set(existing.map((item) => Number((item as { artist_id?: number })?.artist_id || 0)));
  for (const item of incoming) {
    const id = Number((item as { artist_id?: number })?.artist_id || 0);
    if (!Number.isFinite(id) || id <= 0 || seen.has(id)) continue;
    seen.add(id);
    out.push(item);
  }
  return out;
}

function dedupeTracks(items: api.RecoTrack[]): api.RecoTrack[] {
  const out: api.RecoTrack[] = [];
  const seen = new Set<string>();
  for (const item of items) {
    const key = `${Number(item?.track_id || 0)}:${Number(item?.album_id || 0)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

function mergeTracks(existing: api.RecoTrack[], incoming: api.RecoTrack[]): api.RecoTrack[] {
  if (!existing.length) return dedupeTracks(incoming);
  const out = [...existing];
  const seen = new Set(existing.map((item) => `${Number(item?.track_id || 0)}:${Number(item?.album_id || 0)}`));
  for (const item of incoming) {
    const key = `${Number(item?.track_id || 0)}:${Number(item?.album_id || 0)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

export default function LibraryHomeFeed() {
  const navigate = useNavigate();
  const location = useLocation();
  const { section: rawSection } = useParams<{ section: string }>();
  const section = asFeedSection(String(rawSection || ''));
  const { includeUnmatched } = useOutletContext<LibraryOutletContext>();
  const isMobile = useIsMobile();
  const { toast } = useToast();
  const { startPlayback, setCurrentTrack, recommendationSessionId, session } = usePlayback();

  const [loading, setLoading] = useState(false);
  const [appending, setAppending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(true);
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState<number | null>(null);

  const [albums, setAlbums] = useState<api.LibraryAlbumItem[]>([]);
  const [artists, setArtists] = useState<Array<api.TopArtistItem | api.RecentlyAddedArtistItem>>([]);
  const [tracks, setTracks] = useState<api.RecoTrack[]>([]);

  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const loadingMoreRef = useRef(false);
  const requestIdRef = useRef(0);
  const currentTrackIdRef = useRef<number | null>(null);
  const pageSize = 120;

  useEffect(() => {
    currentTrackIdRef.current = Number(session?.currentTrack?.track_id || 0) || null;
  }, [session?.currentTrack?.track_id]);

  const coverSize = useMemo(() => {
    try {
      const raw = Number(localStorage.getItem('pmda_library_cover_size') || 220);
      return Number.isFinite(raw) ? Math.max(150, Math.min(320, raw)) : 220;
    } catch {
      return 220;
    }
  }, []);

  const gridTemplateColumns = useMemo(() => {
    if (isMobile) return 'repeat(2, minmax(0, 1fr))';
    const col = Math.max(140, Math.min(340, Math.floor(coverSize)));
    return `repeat(auto-fill, minmax(${col}px, ${col}px))`;
  }, [coverSize, isMobile]);

  const handlePlayAlbum = useCallback(async (albumId: number, fallbackTitle: string, fallbackThumb?: string | null) => {
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
    } catch (err) {
      toast({
        title: 'Error',
        description: err instanceof Error ? err.message : 'Failed to play album',
        variant: 'destructive',
      });
    }
  }, [setCurrentTrack, startPlayback, toast]);

  const handlePlayRecommendedTrack = useCallback(async (rec: api.RecoTrack) => {
    try {
      const response = await fetch(`/api/library/album/${rec.album_id}/tracks`);
      if (!response.ok) throw new Error('Failed to load tracks');
      const data = await response.json();
      const tracksList: TrackInfo[] = data.tracks || [];
      if (!tracksList.length) {
        toast({ title: 'No tracks', description: 'This recommendation has no playable tracks.', variant: 'destructive' });
        return;
      }
      startPlayback(rec.album_id, rec.album_title || 'Album', rec.thumb || null, tracksList);
      const target = tracksList.find((t) => t.track_id === rec.track_id);
      if (target) setCurrentTrack(target);
    } catch (err) {
      toast({
        title: 'Error',
        description: err instanceof Error ? err.message : 'Failed to play recommendation',
        variant: 'destructive',
      });
    }
  }, [setCurrentTrack, startPlayback, toast]);

  const loadSectionPage = useCallback(async (opts: { reset: boolean; pageOffset: number }) => {
    const rid = ++requestIdRef.current;
    const pageOffset = Math.max(0, Number(opts.pageOffset || 0));
    try {
      if (opts.reset) {
        setLoading(true);
        setError(null);
      } else {
        setAppending(true);
      }

      if (section === 'recently_added') {
        const res = await api.getLibraryAlbums({ sort: 'recent', limit: pageSize, offset: pageOffset, includeUnmatched });
        if (rid !== requestIdRef.current) return;
        const listRaw = Array.isArray(res.albums) ? res.albums : [];
        const list = dedupeAlbumsById(listRaw);
        setAlbums((prev) => (opts.reset ? list : mergeAlbumsById(prev, list)));
        setOffset(pageOffset + listRaw.length);
        setTotal(typeof res.total === 'number' ? res.total : 0);
        setHasMore(pageOffset + listRaw.length < Number(res.total || 0));
      } else if (section === 'recently_played') {
        const res = await api.getLibraryRecentlyPlayedAlbumsWithOptions(365, pageSize, false, { includeUnmatched, offset: pageOffset });
        if (rid !== requestIdRef.current) return;
        const listRaw = Array.isArray(res.albums) ? res.albums : [];
        const list = dedupeAlbumsById(listRaw);
        setAlbums((prev) => (opts.reset ? list : mergeAlbumsById(prev, list)));
        const nextTotal = Number(res.total || 0);
        setOffset(pageOffset + listRaw.length);
        setTotal(nextTotal);
        setHasMore(pageOffset + listRaw.length < nextTotal);
      } else if (section === 'discover') {
        const res = await api.getLibraryDiscoverWithOptions(90, 36, !opts.reset, { includeUnmatched });
        if (rid !== requestIdRef.current) return;
        const chunk = dedupeAlbumsById((res.sections || []).flatMap((s) => (Array.isArray(s.albums) ? s.albums : [])));
        setAlbums((prev) => {
          const base = opts.reset ? [] : prev;
          const merged = mergeAlbumsById(base, chunk);
          const add = merged.length - base.length;
          setOffset((opts.reset ? 0 : base.length) + add);
          setHasMore(add > 0);
          return merged;
        });
        setTotal(null);
      } else if (section === 'top_artists') {
        const res = await api.getTopArtists(pageSize, 0, { includeUnmatched, offset: pageOffset });
        if (rid !== requestIdRef.current) return;
        const listRaw = Array.isArray(res.artists) ? res.artists : [];
        const list = dedupeArtistsById(listRaw);
        setArtists((prev) => (opts.reset ? list : mergeArtistsById(prev, list)));
        const nextTotal = Number(res.total || 0);
        setOffset(pageOffset + listRaw.length);
        setTotal(nextTotal);
        setHasMore(pageOffset + listRaw.length < nextTotal);
      } else if (section === 'recent_artists') {
        const res = await api.getRecentlyAddedArtists(pageSize, pageOffset, { includeUnmatched });
        if (rid !== requestIdRef.current) return;
        const listRaw = Array.isArray(res.artists) ? res.artists : [];
        const list = dedupeArtistsById(listRaw);
        setArtists((prev) => (opts.reset ? list : mergeArtistsById(prev, list)));
        setOffset(pageOffset + listRaw.length);
        setTotal(null);
        setHasMore(listRaw.length >= pageSize);
      } else {
        const excludeTrackId = currentTrackIdRef.current ?? undefined;
        const res = await api.getRecommendationsForYou(recommendationSessionId || '', pageSize, excludeTrackId, pageOffset);
        if (rid !== requestIdRef.current) return;
        const listRaw = Array.isArray(res.tracks) ? res.tracks : [];
        const list = dedupeTracks(listRaw);
        setTracks((prev) => (opts.reset ? list : mergeTracks(prev, list)));
        const nextTotal = Number(res.total || 0);
        setOffset(pageOffset + listRaw.length);
        setTotal(nextTotal);
        setHasMore(pageOffset + listRaw.length < nextTotal);
      }
    } catch (err) {
      if (rid !== requestIdRef.current) return;
      setError(err instanceof Error ? err.message : 'Failed to load section');
      if (opts.reset) {
        setAlbums([]);
        setArtists([]);
        setTracks([]);
        setOffset(0);
      }
      setHasMore(false);
    } finally {
      if (rid === requestIdRef.current) {
        setLoading(false);
        setAppending(false);
      }
    }
  }, [includeUnmatched, pageSize, recommendationSessionId, section]);

  useEffect(() => {
    setAlbums([]);
    setArtists([]);
    setTracks([]);
    setOffset(0);
    setTotal(null);
    setHasMore(true);
    loadingMoreRef.current = false;
    void loadSectionPage({ reset: true, pageOffset: 0 });
  }, [section, includeUnmatched, loadSectionPage]);

  const loadMore = useCallback(async () => {
    if (!hasMore || loading || loadingMoreRef.current) return;
    loadingMoreRef.current = true;
    try {
      await loadSectionPage({ reset: false, pageOffset: offset });
    } finally {
      loadingMoreRef.current = false;
    }
  }, [hasMore, loading, loadSectionPage, offset]);

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
  }, [loadMore]);

  return (
    <div className="container py-4 md:py-6 space-y-5 md:space-y-6">
      <div className="flex items-center justify-between gap-3">
        <Button variant="ghost" className="gap-2" onClick={() => navigate(`/library${location.search || ''}`)}>
          <ArrowLeft className="w-4 h-4" />
          Back to Home
        </Button>
        <div className="text-xs text-muted-foreground">
          {total != null ? `${Math.min(offset, total).toLocaleString()} / ${total.toLocaleString()}` : `${offset.toLocaleString()} loaded`}
        </div>
      </div>

      <Card className="border-border/70">
        <CardContent className="p-5">
          <h1 className="text-2xl font-bold truncate">{FEED_TITLES[section]}</h1>
          {error ? (
            <div className="mt-2 text-sm text-destructive">{error}</div>
          ) : null}
        </CardContent>
      </Card>

      {ALBUM_FEEDS.has(section) ? (
        <div className="grid gap-4 justify-start" style={{ gridTemplateColumns }}>
          {albums.map((a) => (
            <div
              key={`hf-alb-${section}-${a.album_id}`}
              role="button"
              tabIndex={0}
              className="group text-left"
              onClick={() => navigate(`/library/album/${a.album_id}${location.search || ''}`)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  navigate(`/library/album/${a.album_id}${location.search || ''}`);
                }
              }}
            >
              <Card className="overflow-hidden border-border/60 bg-card/90">
                <AspectRatio ratio={1} className="bg-muted">
                  <AlbumArtwork albumThumb={a.thumb} artistId={a.artist_id} alt={a.title} size={320} />
                  <button
                    type="button"
                    onClick={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      void handlePlayAlbum(a.album_id, a.title, a.thumb);
                    }}
                    className="absolute inset-0 flex items-center justify-center opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity bg-black/30"
                    title="Play"
                  >
                    <div className="h-10 w-10 rounded-full bg-white/20 backdrop-blur-sm border border-white/20 flex items-center justify-center">
                      <Play className="h-5 w-5 text-white fill-white" />
                    </div>
                  </button>
                </AspectRatio>
                <CardContent className="p-3 space-y-1.5">
                  <div className="text-sm font-semibold leading-snug line-clamp-2 min-h-[2.4rem]" title={a.title}>{a.title}</div>
                  <button
                    type="button"
                    className="text-xs text-muted-foreground truncate hover:underline"
                    onClick={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      navigate(`/library/artist/${a.artist_id}${location.search || ''}`);
                    }}
                    title="Open artist"
                  >
                    {a.artist_name}
                  </button>
                  <div className="flex items-center gap-1.5 flex-wrap">
                    <Badge variant="outline" className="text-[10px]">{a.year ?? '—'}</Badge>
                    <Badge variant="outline" className="text-[10px]">{a.track_count}t</Badge>
                  </div>
                </CardContent>
              </Card>
            </div>
          ))}
        </div>
      ) : null}

      {ARTIST_FEEDS.has(section) ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
          {artists.map((a) => (
            <button
              key={`hf-art-${section}-${a.artist_id}`}
              type="button"
              className="text-left"
              onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)}
            >
              <Card className="border-border/60 bg-card hover:bg-accent/30 transition-colors p-4">
                <div className="flex items-center gap-3">
                  <div className="h-12 w-12 rounded-full bg-muted overflow-hidden shrink-0 flex items-center justify-center border border-border/60">
                    {a.thumb ? (
                      <img src={a.thumb} alt={a.artist_name} className="w-full h-full object-cover" loading="lazy" decoding="async" />
                    ) : (
                      <UserRound className="w-5 h-5 text-muted-foreground" />
                    )}
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="text-sm font-semibold truncate">{a.artist_name}</div>
                    <div className="text-xs text-muted-foreground mt-0.5">{a.album_count} albums</div>
                  </div>
                </div>
              </Card>
            </button>
          ))}
        </div>
      ) : null}

      {TRACK_FEEDS.has(section) ? (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
          {tracks.map((rec) => (
            <button
              key={`hf-tr-${rec.track_id}-${rec.album_id}`}
              type="button"
              onClick={() => void handlePlayRecommendedTrack(rec)}
              className="group rounded-xl border border-border/60 bg-card/90 p-3 text-left hover:-translate-y-0.5 hover:border-primary/40 hover:bg-accent/20 transition-all duration-300"
            >
              <div className="flex items-start gap-3">
                <div className="w-12 h-12 rounded-lg bg-muted overflow-hidden shrink-0 flex items-center justify-center">
                  {rec.thumb ? (
                    <img src={rec.thumb} alt={rec.album_title} className="w-full h-full object-cover" loading="lazy" decoding="async" />
                  ) : (
                    <Music className="w-5 h-5 text-muted-foreground" />
                  )}
                </div>
                <div className="min-w-0 flex-1">
                  <p className="font-medium text-sm truncate">{rec.title}</p>
                  <p className="text-xs text-muted-foreground truncate mt-0.5">{rec.artist_name} · {rec.album_title}</p>
                </div>
                <Play className="w-4 h-4 text-muted-foreground group-hover:text-primary shrink-0 mt-0.5" />
              </div>
            </button>
          ))}
        </div>
      ) : null}

      {loading && (albums.length + artists.length + tracks.length) === 0 ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8">
          <Loader2 className="w-4 h-4 animate-spin" /> Loading…
        </div>
      ) : null}

      <div ref={sentinelRef} className="h-6" />
      <div className="flex min-h-6 items-center justify-center py-2 text-xs text-muted-foreground">
        {appending ? (
          <span className="inline-flex items-center gap-2">
            <Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading more…
          </span>
        ) : !hasMore ? 'All loaded' : null}
      </div>
    </div>
  );
}
