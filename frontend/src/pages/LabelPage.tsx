import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutletContext, useParams } from 'react-router-dom';
import { ArrowLeft, Loader2, Play, UserRound } from 'lucide-react';

import { AspectRatio } from '@/components/ui/aspect-ratio';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { ScrollArea } from '@/components/ui/scroll-area';
import { AlbumArtwork } from '@/components/library/AlbumArtwork';
import { usePlayback } from '@/contexts/PlaybackContext';
import { useToast } from '@/hooks/use-toast';
import { useIsMobile } from '@/hooks/use-mobile';
import * as api from '@/lib/api';
import type { TrackInfo } from '@/components/library/AudioPlayer';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';

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

export default function LabelPage() {
  const isMobile = useIsMobile();
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched } = useOutletContext<LibraryOutletContext>();
  const { startPlayback, setCurrentTrack } = usePlayback();
  const { toast } = useToast();
  const params = useParams<{ label: string }>();
  const label = decodeURIComponent(String(params.label || '')).trim();

  const [profileLoading, setProfileLoading] = useState(false);
  const [profile, setProfile] = useState<api.LabelProfileResponse | null>(null);

  const [loading, setLoading] = useState(false);
  const [appending, setAppending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [albums, setAlbums] = useState<api.LibraryAlbumItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const limit = 120;

  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const loadingMoreRef = useRef(false);
  const requestIdRef = useRef(0);

  const [coverSize] = useState<number>(() => {
    try {
      const raw = Number(localStorage.getItem('pmda_library_cover_size') || 220);
      return Number.isFinite(raw) ? Math.max(150, Math.min(320, raw)) : 220;
    } catch {
      return 220;
    }
  });

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
        description: err instanceof Error ? err.message : 'Failed to load tracks',
        variant: 'destructive',
      });
    }
  }, [setCurrentTrack, startPlayback, toast]);

  const loadProfile = useCallback(async () => {
    if (!label) return;
    try {
      setProfileLoading(true);
      const res = await api.getLabelProfile(label, { includeUnmatched, limit_artists: 24, limit_genres: 24 });
      setProfile(res);
    } catch {
      setProfile(null);
    } finally {
      setProfileLoading(false);
    }
  }, [includeUnmatched, label]);

  const loadAlbumsPage = useCallback(async (opts: { reset: boolean; pageOffset: number }) => {
    const rid = ++requestIdRef.current;
    const pageOffset = Math.max(0, Number(opts.pageOffset || 0));
    try {
      if (opts.reset) {
        setLoading(true);
        setError(null);
      } else {
        setAppending(true);
      }
      const res = await api.getLibraryAlbums({ label, sort: 'year_desc', limit, offset: pageOffset, includeUnmatched });
      if (rid !== requestIdRef.current) return;
      const listRaw = Array.isArray(res.albums) ? res.albums : [];
      const list = dedupeAlbumsById(listRaw);
      const nextTotal = Number(res.total || 0);
      setAlbums((prev) => (opts.reset ? list : mergeAlbumsById(prev, list)));
      setOffset(pageOffset + listRaw.length);
      setTotal(nextTotal);
      setHasMore(pageOffset + listRaw.length < nextTotal);
    } catch (err) {
      if (rid !== requestIdRef.current) return;
      setError(err instanceof Error ? err.message : 'Failed to load label');
      if (opts.reset) {
        setAlbums([]);
        setOffset(0);
        setTotal(0);
      }
      setHasMore(false);
    } finally {
      if (rid === requestIdRef.current) {
        setLoading(false);
        setAppending(false);
      }
    }
  }, [includeUnmatched, label, limit]);

  useEffect(() => {
    if (!label) {
      setError('Invalid label');
      return;
    }
    setAlbums([]);
    setOffset(0);
    setTotal(0);
    setHasMore(true);
    loadingMoreRef.current = false;
    void loadProfile();
    void loadAlbumsPage({ reset: true, pageOffset: 0 });
  }, [label, includeUnmatched, loadAlbumsPage, loadProfile]);

  const loadMore = useCallback(async () => {
    if (!hasMore || loading || loadingMoreRef.current) return;
    loadingMoreRef.current = true;
    try {
      await loadAlbumsPage({ reset: false, pageOffset: offset });
    } finally {
      loadingMoreRef.current = false;
    }
  }, [hasMore, loadAlbumsPage, loading, offset]);

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

  const topArtists = useMemo(() => profile?.influential_artists || [], [profile?.influential_artists]);
  const topGenres = useMemo(() => profile?.genres || [], [profile?.genres]);

  return (
    <div className="container py-4 md:py-6 space-y-5 md:space-y-6">
      <div className="flex items-center justify-between gap-3">
        <Button variant="ghost" className="gap-2" onClick={() => navigate(`/library${location.search || ''}`)}>
          <ArrowLeft className="w-4 h-4" />
          Back to Library
        </Button>
        <div className="text-xs text-muted-foreground">
          {total > 0 ? `${Math.min(offset, total).toLocaleString()} / ${total.toLocaleString()}` : `${albums.length.toLocaleString()} loaded`}
        </div>
      </div>

      <Card className="border-border/70">
        <CardContent className="p-5 space-y-4">
          <div className="flex items-start justify-between gap-4">
            <div className="min-w-0">
              <h1 className="text-2xl font-bold truncate">{label || 'Label'}</h1>
              <p className="text-xs text-muted-foreground mt-1">
                {(profile?.album_count || total || 0).toLocaleString()} release{(profile?.album_count || total || 0) !== 1 ? 's' : ''}
              </p>
            </div>
            {error ? (
              <Badge variant="outline" className="text-xs border-destructive/50 text-destructive">
                {error}
              </Badge>
            ) : null}
          </div>

          {profileLoading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="w-4 h-4 animate-spin" /> Loading label profile…
            </div>
          ) : profile?.description ? (
            <div className="text-sm text-muted-foreground leading-relaxed">{profile.description}</div>
          ) : null}

          <div className="flex flex-wrap items-center gap-2">
            {profile?.owner ? <Badge variant="secondary">Owner: {profile.owner}</Badge> : null}
            {profile?.sub_labels?.slice(0, 8).map((sub) => (
              <Badge key={`sub-${sub}`} variant="outline" className="text-[11px]">{sub}</Badge>
            ))}
          </div>

          {topArtists.length > 0 ? (
            <div className="space-y-2">
              <div className="text-xs font-medium text-muted-foreground">Influential artists</div>
              <ScrollArea className="w-full whitespace-nowrap">
                <div className="flex gap-2 pb-2">
                  {topArtists.map((a) => (
                    <button
                      key={`lab-top-${a.artist_id}`}
                      type="button"
                      className="inline-flex items-center gap-2 rounded-full border border-border/60 bg-muted/40 px-3 py-1.5 text-[11px] hover:bg-muted transition-colors"
                      onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)}
                      title="Open artist"
                    >
                      <span className="truncate max-w-[16rem]">{a.artist_name}</span>
                      <span className="text-muted-foreground tabular-nums">{a.album_count}</span>
                    </button>
                  ))}
                </div>
              </ScrollArea>
            </div>
          ) : null}

          {topGenres.length > 0 ? (
            <div className="space-y-2">
              <div className="text-xs font-medium text-muted-foreground">Genres on this label</div>
              <ScrollArea className="w-full whitespace-nowrap">
                <div className="flex gap-2 pb-2">
                  {topGenres.map((g) => (
                    <button
                      key={`lab-gen-${g.genre}`}
                      type="button"
                      className="inline-flex items-center gap-2 rounded-full border border-border/60 bg-muted/40 px-3 py-1.5 text-[11px] hover:bg-muted transition-colors"
                      onClick={() => navigate(`/library/genre/${encodeURIComponent(g.genre)}${location.search || ''}`)}
                      title="Open genre"
                    >
                      <span className="truncate max-w-[16rem]">{g.genre}</span>
                      <span className="text-muted-foreground tabular-nums">{g.count}</span>
                    </button>
                  ))}
                </div>
              </ScrollArea>
            </div>
          ) : null}
        </CardContent>
      </Card>

      {loading && albums.length === 0 ? (
        <Card className="border-border/70">
          <CardContent className="p-8 text-sm text-muted-foreground">
            <span className="inline-flex items-center gap-2">
              <Loader2 className="w-4 h-4 animate-spin" /> Loading…
            </span>
          </CardContent>
        </Card>
      ) : albums.length === 0 ? (
        <Card className="border-border/70">
          <CardContent className="p-8 text-sm text-muted-foreground">No releases found for this label.</CardContent>
        </Card>
      ) : (
        <div className="grid gap-4 justify-start" style={{ gridTemplateColumns }}>
          {albums.map((a) => (
            <div
              key={`lab-alb-${a.album_id}`}
              className="text-left group"
              role="button"
              tabIndex={0}
              onClick={() => navigate(`/library/album/${a.album_id}${location.search || ''}`)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  navigate(`/library/album/${a.album_id}${location.search || ''}`);
                }
              }}
              title="Open album"
            >
              <div className="relative overflow-hidden rounded-2xl border border-border/60 bg-card shadow-sm">
                <AspectRatio ratio={1} className="bg-muted">
                  <AlbumArtwork albumThumb={a.thumb} artistId={a.artist_id} alt={a.title} size={320} imageClassName="w-full h-full object-cover" />
                  <div className="absolute inset-0 opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity bg-black/25" />
                  <div className="absolute inset-x-0 bottom-0 p-3 opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity">
                    <div className="flex items-center justify-between gap-2">
                      <Button
                        size="sm"
                        className="h-9 rounded-full gap-2"
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          void handlePlayAlbum(a.album_id, a.title, a.thumb);
                        }}
                      >
                        <Play className="h-4 w-4" />
                        Play
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        className="h-9 rounded-full"
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          navigate(`/library/artist/${a.artist_id}${location.search || ''}`);
                        }}
                        title="Open artist"
                      >
                        <UserRound className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                </AspectRatio>
                <div className="p-3 space-y-1.5">
                  <div className="text-sm font-semibold leading-snug line-clamp-2 min-h-[2.4rem]" title={a.title}>{a.title}</div>
                  <button
                    type="button"
                    className="text-xs text-muted-foreground truncate hover:underline"
                    onClick={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      navigate(`/library/artist/${a.artist_id}${location.search || ''}`);
                    }}
                  >
                    {a.artist_name}
                  </button>
                  <div className="flex items-center gap-1.5 flex-wrap">
                    <Badge variant="outline" className="text-[10px]">{a.year ?? '—'}</Badge>
                    <Badge variant="outline" className="text-[10px]">{a.track_count}t</Badge>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

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
