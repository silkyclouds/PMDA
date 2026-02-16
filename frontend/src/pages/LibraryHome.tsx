import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutletContext } from 'react-router-dom';
import { ChevronDown, Flame, Heart, Loader2, Music, Play, RefreshCw, Sparkles, UserRound } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { AspectRatio } from '@/components/ui/aspect-ratio';
import { Carousel, CarouselContent, CarouselItem, CarouselNext, CarouselPrevious } from '@/components/ui/carousel';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { cn } from '@/lib/utils';
import { usePlayback } from '@/contexts/PlaybackContext';
import { useToast } from '@/hooks/use-toast';
import * as api from '@/lib/api';
import type { TrackInfo } from '@/components/library/AudioPlayer';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';

function normalizeGenreBadges(album: api.LibraryAlbumItem): string[] {
  const raw: unknown =
    (album && typeof album === 'object' && 'genres' in album)
      ? (album as { genres?: unknown }).genres
      : undefined;
  const base = Array.isArray(raw) ? raw : (album.genre ? String(album.genre).split(/[;,/|]+/) : []);
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of base) {
    const txt = String(item ?? '').replace(/\s+/g, ' ').trim();
    if (!txt) continue;
    const key = txt.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(txt);
  }
  return out;
}

function isUnmatchedAlbum(album: api.LibraryAlbumItem): boolean {
  return album.mb_identified === false;
}

function unmatchedAlbumCardClass(album: api.LibraryAlbumItem): string {
  return isUnmatchedAlbum(album)
    ? 'ring-1 ring-amber-500/45 shadow-[0_0_0_1px_rgba(245,158,11,0.25),0_0_24px_rgba(245,158,11,0.14)]'
    : '';
}

function unmatchedBadge(album: api.LibraryAlbumItem) {
  if (!isUnmatchedAlbum(album)) return null;
  return (
    <Badge variant="outline" className="text-[10px] border-amber-500/50 text-amber-700 bg-background/75 backdrop-blur dark:text-amber-300">
      Unmatched
    </Badge>
  );
}

export default function LibraryHome() {
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched, stats } = useOutletContext<LibraryOutletContext>();
  const { toast } = useToast();
  const { startPlayback, setCurrentTrack, recommendationSessionId, session } = usePlayback();

  const [recoLoading, setRecoLoading] = useState(false);
  const [recoError, setRecoError] = useState<string | null>(null);
  const [recommendations, setRecommendations] = useState<api.RecoTrack[]>([]);

  const [topArtistsLoading, setTopArtistsLoading] = useState(false);
  const [topArtists, setTopArtists] = useState<api.TopArtistItem[]>([]);
  const [recentArtistsLoading, setRecentArtistsLoading] = useState(false);
  const [recentArtists, setRecentArtists] = useState<api.RecentlyAddedArtistItem[]>([]);

  const [discoverLoading, setDiscoverLoading] = useState(false);
  const [discoverError, setDiscoverError] = useState<string | null>(null);
  const [discover, setDiscover] = useState<api.LibraryDiscoverResponse | null>(null);

  const [recentLoading, setRecentLoading] = useState(false);
  const [recentAlbums, setRecentAlbums] = useState<api.LibraryAlbumItem[]>([]);

  const [recentlyPlayedLoading, setRecentlyPlayedLoading] = useState(false);
  const [recentlyPlayedError, setRecentlyPlayedError] = useState<string | null>(null);
  const [recentlyPlayedAlbums, setRecentlyPlayedAlbums] = useState<api.RecentlyPlayedAlbumItem[]>([]);

  const [digestLoading, setDigestLoading] = useState(false);
  const [digestOpen, setDigestOpen] = useState(false);
  const [digestAlbums, setDigestAlbums] = useState<api.LibraryAlbumItem[]>([]);
  const [digestEnrichment, setDigestEnrichment] = useState<api.LibraryDigestResponse['enrichment'] | null>(null);

  const [albumLikes, setAlbumLikes] = useState<Record<number, boolean>>({});

  const hydrateAlbumLikes = useCallback(async (ids: number[]) => {
    const unique = Array.from(new Set(ids.filter((x) => Number.isFinite(x) && x > 0)));
    if (unique.length === 0) return;
    const unknown = unique.filter((id) => albumLikes[id] === undefined);
    if (unknown.length === 0) return;
    try {
      const res = await api.getLikes('album', unknown);
      const next: Record<number, boolean> = {};
      for (const it of (res.items || [])) next[it.entity_id] = Boolean(it.liked);
      setAlbumLikes((prev) => ({ ...prev, ...next }));
    } catch {
      // ignore
    }
  }, [albumLikes]);

  const toggleAlbumLike = useCallback(async (albumId: number) => {
    const current = Boolean(albumLikes[albumId]);
    const next = !current;
    setAlbumLikes((prev) => ({ ...prev, [albumId]: next }));
    try {
      await api.setLike({ entity_type: 'album', entity_id: albumId, liked: next, source: 'ui_library_home' });
    } catch (e) {
      setAlbumLikes((prev) => ({ ...prev, [albumId]: current }));
      toast({
        title: 'Like failed',
        description: e instanceof Error ? e.message : 'Failed to update like',
        variant: 'destructive',
      });
    }
  }, [albumLikes, toast]);

  const loadRecommendations = useCallback(async () => {
    if (!recommendationSessionId) return;
    try {
      setRecoLoading(true);
      setRecoError(null);
      const excludeTrackId = session?.currentTrack?.track_id;
      const data = await api.getRecommendationsForYou(recommendationSessionId, 12, excludeTrackId);
      setRecommendations(Array.isArray(data.tracks) ? data.tracks : []);
    } catch {
      setRecommendations([]);
      setRecoError('Start listening to personalize this feed.');
    } finally {
      setRecoLoading(false);
    }
  }, [recommendationSessionId, session?.currentTrack?.track_id]);

  const loadTopArtists = useCallback(async () => {
    try {
      setTopArtistsLoading(true);
      const res = await api.getTopArtists(18, 0, { includeUnmatched });
      setTopArtists(Array.isArray(res.artists) ? res.artists : []);
    } catch {
      setTopArtists([]);
    } finally {
      setTopArtistsLoading(false);
    }
  }, [includeUnmatched]);

  const loadRecentArtists = useCallback(async () => {
    try {
      setRecentArtistsLoading(true);
      const res = await api.getRecentlyAddedArtists(18, 0, { includeUnmatched });
      setRecentArtists(Array.isArray(res.artists) ? res.artists : []);
    } catch {
      setRecentArtists([]);
    } finally {
      setRecentArtistsLoading(false);
    }
  }, [includeUnmatched]);

  const loadDiscover = useCallback(async (opts?: { refresh?: boolean }) => {
    try {
      setDiscoverLoading(true);
      setDiscoverError(null);
      const res = await api.getLibraryDiscoverWithOptions(90, 18, Boolean(opts?.refresh), { includeUnmatched });
      setDiscover(res);
    } catch (e) {
      setDiscover(null);
      setDiscoverError(e instanceof Error ? e.message : 'Failed to load discover feed');
    } finally {
      setDiscoverLoading(false);
    }
  }, [includeUnmatched]);

  const loadRecent = useCallback(async () => {
    try {
      setRecentLoading(true);
      const data = await api.getLibraryAlbums({ sort: 'recent', limit: 18, offset: 0, includeUnmatched });
      setRecentAlbums(Array.isArray(data.albums) ? data.albums : []);
    } catch {
      setRecentAlbums([]);
    } finally {
      setRecentLoading(false);
    }
  }, [includeUnmatched]);

  const loadRecentlyPlayed = useCallback(async (opts?: { refresh?: boolean }) => {
    try {
      setRecentlyPlayedLoading(true);
      setRecentlyPlayedError(null);
      const data = await api.getLibraryRecentlyPlayedAlbumsWithOptions(90, 18, Boolean(opts?.refresh), { includeUnmatched });
      setRecentlyPlayedAlbums(Array.isArray(data.albums) ? data.albums : []);
    } catch (e) {
      setRecentlyPlayedAlbums([]);
      setRecentlyPlayedError(e instanceof Error ? e.message : 'Failed to load recently played');
    } finally {
      setRecentlyPlayedLoading(false);
    }
  }, [includeUnmatched]);

  const loadDigest = useCallback(async () => {
    try {
      setDigestLoading(true);
      const data = await api.getLibraryDigestWithOptions(18, true, { includeUnmatched });
      setDigestAlbums(Array.isArray(data.albums) ? data.albums : []);
      setDigestEnrichment(data.enrichment ?? null);
    } catch {
      setDigestAlbums([]);
      setDigestEnrichment(null);
    } finally {
      setDigestLoading(false);
    }
  }, [includeUnmatched]);

  useEffect(() => {
    void loadDiscover();
    void loadTopArtists();
    void loadRecentArtists();
    void loadRecentlyPlayed();
    void loadRecent();
    void loadDigest();
  }, [includeUnmatched, loadDiscover, loadTopArtists, loadRecentArtists, loadRecentlyPlayed, loadRecent, loadDigest]);

  useEffect(() => {
    void loadRecommendations();
  }, [loadRecommendations]);

  useEffect(() => {
    void hydrateAlbumLikes(digestAlbums.map((a) => a.album_id));
  }, [digestAlbums, hydrateAlbumLikes]);

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

  const handlePlayRecommendedTrack = async (rec: api.RecoTrack) => {
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
    } catch (error) {
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to play recommendation',
        variant: 'destructive',
      });
    }
  };

  const browseYear = useCallback((y: number) => {
    const next = new URLSearchParams(location.search);
    if (y > 0) next.set('year', String(y));
    navigate(`/library/albums?${next.toString()}`);
  }, [location.search, navigate]);

  const openLabel = useCallback((lab: string) => {
    navigate(`/library/label/${encodeURIComponent(lab)}${location.search || ''}`);
  }, [location.search, navigate]);

  const openGenre = useCallback((g: string) => {
    navigate(`/library/genre/${encodeURIComponent(g)}${location.search || ''}`);
  }, [location.search, navigate]);

  return (
    <div className="container pb-6 space-y-6">
      <Card className="border-border/60">
        <CardContent className="p-4 sm:p-5">
          <div className="flex items-center justify-between gap-4">
            <div className="space-y-1">
              <div className="text-xs uppercase tracking-wide text-muted-foreground">Identified Library</div>
              <div className="text-lg font-semibold">
                {stats ? `${stats.artists.toLocaleString()} artists · ${stats.albums.toLocaleString()} albums` : 'Loading...'}
              </div>
            </div>
            {!includeUnmatched ? (
              <Badge variant="secondary" className="text-[10px]">Strict matched only</Badge>
            ) : (
              <Badge variant="outline" className="text-[10px]">Including non matched</Badge>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Discover */}
      <Card className="border-border/60 overflow-hidden">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="text-base flex items-center gap-2">
                <Flame className="h-4 w-4 text-primary" />
                Discover
              </CardTitle>
              <CardDescription>Albums picked from your listening history.</CardDescription>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={() => void loadDiscover({ refresh: true })}
              disabled={discoverLoading}
              className="gap-1.5"
              title="Refresh personalized discover feed"
            >
              {discoverLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
              Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent className="space-y-6">
          {discoverError ? (
            <p className="text-sm text-destructive">Failed to load personalized discover feed.</p>
          ) : null}
          {discoverLoading && !discover ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="w-4 h-4 animate-spin" />
              Building your discover feed…
            </div>
          ) : null}
          {!discoverLoading && discover && (discover.sections || []).length === 0 ? (
            <p className="text-sm text-muted-foreground">Start listening to personalize Discover.</p>
          ) : null}

          {(discover?.sections || []).map((sec) => (
            <div key={`disc-sec-${sec.key}`} className="space-y-3">
              <div className="flex items-end justify-between gap-3">
                <div className="min-w-0">
                  <div className="text-sm font-semibold truncate">{sec.title}</div>
                  <div className="text-xs text-muted-foreground truncate">{sec.reason}</div>
                </div>
                {(() => {
                  const seed = sec.seed && typeof sec.seed === 'object' ? (sec.seed as Record<string, unknown>) : {};
                  if (sec.key === 'labels') {
                    const lab = typeof seed.label === 'string' ? seed.label.trim() : '';
                    if (!lab) return null;
                    return (
                      <Button type="button" size="sm" variant="secondary" className="h-7 px-2 text-xs shrink-0" onClick={() => openLabel(lab)} title="Open label">
                        Open label
                      </Button>
                    );
                  }
                  if (sec.key === 'genre') {
                    const g = typeof seed.genre === 'string' ? seed.genre.trim() : '';
                    if (!g) return null;
                    return (
                      <Button type="button" size="sm" variant="secondary" className="h-7 px-2 text-xs shrink-0" onClick={() => openGenre(g)} title="Open genre">
                        Open genre
                      </Button>
                    );
                  }
                  if (sec.key === 'year') {
                    const yRaw = seed.year;
                    const y = typeof yRaw === 'number' ? yRaw : Number(yRaw);
                    if (!Number.isFinite(y) || y <= 0) return null;
                    return (
                      <Button type="button" size="sm" variant="secondary" className="h-7 px-2 text-xs shrink-0" onClick={() => browseYear(y)} title="Browse year">
                        Browse year
                      </Button>
                    );
                  }
                  return null;
                })()}
              </div>

              {sec.albums.length === 0 ? (
                <p className="text-sm text-muted-foreground">No suggestions available.</p>
              ) : (
                <Carousel opts={{ align: 'start', dragFree: true }} className="w-full">
                  <CarouselContent className="-ml-3">
                    {sec.albums.map((a) => (
                      <CarouselItem key={`disc-${sec.key}-${a.album_id}`} className="basis-[160px] sm:basis-[190px] md:basis-[220px] pl-3">
                        <div className="group">
                          <Card className={cn('overflow-hidden border-border/60', unmatchedAlbumCardClass(a))}>
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
                              {a.thumb ? (
                                <img src={a.thumb} alt={a.title} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                              ) : (
                                <div className="w-full h-full flex items-center justify-center">
                                  <Music className="w-10 h-10 text-muted-foreground" />
                                </div>
                              )}
                              {isUnmatchedAlbum(a) ? <div className="absolute top-2 left-2">{unmatchedBadge(a)}</div> : null}
                              <button
                                type="button"
                                onClick={() => void handlePlayAlbum(a.album_id, a.title, a.thumb)}
                                className={cn(
                                  'absolute inset-0 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity',
                                  'bg-black/35',
                                )}
                                title="Play"
                              >
                                <div className="h-12 w-12 rounded-full bg-white/15 backdrop-blur-sm border border-white/20 flex items-center justify-center">
                                  <Play className="h-5 w-5 text-white fill-white" />
                                </div>
                              </button>
                            </AspectRatio>
                            <CardContent className="p-3 space-y-1.5">
                              <div className="flex items-start justify-between gap-2">
                                <div className="min-w-0 text-left">
                                  <button
                                    type="button"
                                    onClick={() => navigate(`/library/album/${a.album_id}${location.search || ''}`)}
                                    className="block min-w-0 text-left hover:underline"
                                    title="Open album"
                                  >
                                    <p className="text-sm font-semibold truncate" title={a.title}>
                                      {a.title}
                                    </p>
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)}
                                    className="block text-xs text-muted-foreground truncate hover:underline"
                                    title="Open artist"
                                  >
                                    {a.artist_name}
                                  </button>
                                </div>
                                <Badge variant="outline" className="text-[10px] shrink-0">
                                  {a.year ?? '—'}
                                </Badge>
                                {isUnmatchedAlbum(a) ? (
                                  <Badge variant="outline" className="text-[10px] border-amber-500/50 text-amber-700 dark:text-amber-300">
                                    Verify tags
                                  </Badge>
                                ) : null}
                              </div>
                            </CardContent>
                          </Card>
                        </div>
                      </CarouselItem>
                    ))}
                  </CarouselContent>
                  <div className="hidden md:block">
                    <CarouselPrevious />
                    <CarouselNext />
                  </div>
                </Carousel>
              )}
            </div>
          ))}
        </CardContent>
      </Card>

      {/* For You */}
      <Card className="border-border/60 overflow-hidden">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="text-base flex items-center gap-2">
                <Sparkles className="h-4 w-4 text-primary" />
                For You
              </CardTitle>
              <CardDescription>Personalized track picks from your listening history.</CardDescription>
            </div>
            <Button variant="outline" size="sm" onClick={() => void loadRecommendations()} disabled={recoLoading} className="gap-1.5">
              {recoLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
              Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {recoLoading && recommendations.length === 0 ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="w-4 h-4 animate-spin" />
              Building recommendations…
            </div>
          ) : recommendations.length === 0 ? (
            <p className="text-sm text-muted-foreground">{recoError ?? 'Start listening to personalize this feed.'}</p>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
              {recommendations.map((rec) => (
                <button
                  key={`rec-${rec.track_id}`}
                  type="button"
                  onClick={() => void handlePlayRecommendedTrack(rec)}
                  className="group rounded-xl border border-border/60 bg-card p-3 text-left hover:border-primary/40 hover:bg-accent/30 transition-colors"
                >
                  <div className="flex items-start gap-3">
                    <div className="w-12 h-12 rounded-lg bg-muted overflow-hidden shrink-0 flex items-center justify-center">
                      {rec.thumb ? (
                        <img src={rec.thumb} alt={rec.album_title} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                      ) : (
                        <Music className="w-5 h-5 text-muted-foreground" />
                      )}
                    </div>
                    <div className="min-w-0 flex-1">
                      <p className="font-medium text-sm truncate">{rec.title}</p>
                      <p className="text-xs text-muted-foreground truncate mt-0.5">
                        {rec.artist_name} · {rec.album_title}
                      </p>
                      {Array.isArray(rec.reasons) && rec.reasons.length > 0 && (
                        <p className="text-[10px] text-muted-foreground truncate mt-2">{rec.reasons.join(' · ')}</p>
                      )}
                    </div>
                    <Play className="w-4 h-4 text-muted-foreground group-hover:text-primary shrink-0 mt-0.5" />
                  </div>
                </button>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Top Artists */}
      <Card className="border-border/60 overflow-hidden">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="text-base">Top Artists</CardTitle>
              <CardDescription>Most played from your local listening stats.</CardDescription>
            </div>
            <Button variant="outline" size="sm" onClick={() => void loadTopArtists()} disabled={topArtistsLoading} className="gap-1.5">
              {topArtistsLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
              Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent className="pt-0">
          {topArtistsLoading && topArtists.length === 0 ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="w-4 h-4 animate-spin" />
              Loading…
            </div>
          ) : topArtists.length === 0 ? (
            <p className="text-sm text-muted-foreground">No listening stats yet. Start playing music to build this section.</p>
          ) : (
            <Carousel opts={{ align: 'start', dragFree: true }} className="w-full">
              <CarouselContent className="-ml-3">
                {topArtists.map((a) => (
                  <CarouselItem key={`ta-${a.artist_id}`} className="basis-[170px] sm:basis-[200px] md:basis-[220px] pl-3">
                    <button type="button" onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)} className="w-full text-left group">
                      <Card className="overflow-hidden border-border/60 hover:bg-accent/30 transition-colors">
                        <CardContent className="p-4 space-y-3">
                          <div className="flex items-center gap-3">
                            <div className="h-12 w-12 rounded-full bg-muted overflow-hidden shrink-0 flex items-center justify-center border border-border/60">
                              {a.thumb ? (
                                <img src={a.thumb} alt={a.artist_name} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                              ) : (
                                <UserRound className="w-5 h-5 text-muted-foreground" />
                              )}
                            </div>
                            <div className="min-w-0">
                              <div className="text-sm font-semibold truncate">{a.artist_name}</div>
                              <div className="text-xs text-muted-foreground truncate">{a.album_count} albums</div>
                            </div>
                          </div>
                          <div className="flex items-center justify-between text-[11px] text-muted-foreground tabular-nums">
                            <span>{a.completion_count} completes</span>
                            <span>{a.play_count} plays</span>
                          </div>
                        </CardContent>
                      </Card>
                    </button>
                  </CarouselItem>
                ))}
              </CarouselContent>
              <div className="hidden md:block">
                <CarouselPrevious />
                <CarouselNext />
              </div>
            </Carousel>
          )}
        </CardContent>
      </Card>

      {/* Recently Added Artists */}
      <Card className="border-border/60 overflow-hidden">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="text-base">Recently Added Artists</CardTitle>
              <CardDescription>Artists with the most recent imports.</CardDescription>
            </div>
            <Button variant="outline" size="sm" onClick={() => void loadRecentArtists()} disabled={recentArtistsLoading} className="gap-1.5">
              {recentArtistsLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
              Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent className="pt-0">
          {recentArtistsLoading && recentArtists.length === 0 ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="w-4 h-4 animate-spin" />
              Loading…
            </div>
          ) : recentArtists.length === 0 ? (
            <p className="text-sm text-muted-foreground">No recently added artists yet.</p>
          ) : (
            <Carousel opts={{ align: 'start', dragFree: true }} className="w-full">
              <CarouselContent className="-ml-3">
                {recentArtists.map((a) => (
                  <CarouselItem key={`ra-${a.artist_id}`} className="basis-[170px] sm:basis-[200px] md:basis-[220px] pl-3">
                    <button type="button" onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)} className="w-full text-left group">
                      <Card className="overflow-hidden border-border/60 hover:bg-accent/30 transition-colors">
                        <CardContent className="p-4 space-y-3">
                          <div className="flex items-center gap-3">
                            <div className="h-12 w-12 rounded-full bg-muted overflow-hidden shrink-0 flex items-center justify-center border border-border/60">
                              {a.thumb ? (
                                <img src={a.thumb} alt={a.artist_name} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                              ) : (
                                <UserRound className="w-5 h-5 text-muted-foreground" />
                              )}
                            </div>
                            <div className="min-w-0">
                              <div className="text-sm font-semibold truncate">{a.artist_name}</div>
                              <div className="text-xs text-muted-foreground truncate">{a.album_count} albums</div>
                            </div>
                          </div>
                          <div className="text-[11px] text-muted-foreground tabular-nums">
                            {a.last_added_at ? `Added ${new Date(a.last_added_at * 1000).toLocaleDateString()}` : ''}
                          </div>
                        </CardContent>
                      </Card>
                    </button>
                  </CarouselItem>
                ))}
              </CarouselContent>
              <div className="hidden md:block">
                <CarouselPrevious />
                <CarouselNext />
              </div>
            </Carousel>
          )}
        </CardContent>
      </Card>

      {/* Recently Played */}
      <Card className="border-border/60 overflow-hidden">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="text-base">Recently Played</CardTitle>
              <CardDescription>What you played lately, across the whole library.</CardDescription>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={() => void loadRecentlyPlayed({ refresh: true })}
              disabled={recentlyPlayedLoading}
              className="gap-1.5"
            >
              {recentlyPlayedLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
              Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent className="pt-0">
          {recentlyPlayedLoading && recentlyPlayedAlbums.length === 0 ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="w-4 h-4 animate-spin" />
              Loading…
            </div>
          ) : recentlyPlayedAlbums.length === 0 ? (
            <p className="text-sm text-muted-foreground">{recentlyPlayedError ?? 'No playback history yet.'}</p>
          ) : (
            <Carousel opts={{ align: 'start', dragFree: true }} className="w-full">
              <CarouselContent className="-ml-3">
                {recentlyPlayedAlbums.map((a) => (
                  <CarouselItem key={`rplay-${a.album_id}`} className="basis-[160px] sm:basis-[190px] md:basis-[220px] pl-3">
                    <div className="group">
                      <Card className={cn('overflow-hidden border-border/60', unmatchedAlbumCardClass(a))}>
                        <AspectRatio ratio={1} className="bg-muted">
                          {a.thumb ? (
                            <img src={a.thumb} alt={a.title} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                          ) : (
                            <div className="w-full h-full flex items-center justify-center">
                              <Music className="w-10 h-10 text-muted-foreground" />
                            </div>
                          )}
                          {isUnmatchedAlbum(a) ? <div className="absolute top-2 left-2">{unmatchedBadge(a)}</div> : null}
                          <button
                            type="button"
                            onClick={() => void handlePlayAlbum(a.album_id, a.title, a.thumb)}
                            className={cn(
                              'absolute inset-0 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity',
                              'bg-black/35',
                            )}
                            title="Play"
                          >
                            <div className="h-12 w-12 rounded-full bg-white/15 backdrop-blur-sm border border-white/20 flex items-center justify-center">
                              <Play className="h-5 w-5 text-white fill-white" />
                            </div>
                          </button>
                        </AspectRatio>
                        <CardContent className="p-3 space-y-1.5">
                          <div className="flex items-start justify-between gap-2">
                            <div className="min-w-0 text-left">
                              <button
                                type="button"
                                onClick={() => navigate(`/library/album/${a.album_id}${location.search || ''}`)}
                                className="block min-w-0 text-left hover:underline"
                                title="Open album"
                              >
                                <p className="text-sm font-semibold truncate" title={a.title}>
                                  {a.title}
                                </p>
                              </button>
                              <button
                                type="button"
                                onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)}
                                className="block text-xs text-muted-foreground truncate hover:underline"
                                title="Open artist"
                              >
                                {a.artist_name}
                              </button>
                            </div>
                            <Badge variant="outline" className="text-[10px] shrink-0">
                              {a.year ?? '—'}
                            </Badge>
                            {isUnmatchedAlbum(a) ? (
                              <Badge variant="outline" className="text-[10px] border-amber-500/50 text-amber-700 dark:text-amber-300">
                                Verify tags
                              </Badge>
                            ) : null}
                          </div>
                        </CardContent>
                      </Card>
                    </div>
                  </CarouselItem>
                ))}
              </CarouselContent>
              <div className="hidden md:block">
                <CarouselPrevious />
                <CarouselNext />
              </div>
            </Carousel>
          )}
        </CardContent>
      </Card>

      {/* Recently Added */}
      <Card className="border-border/60 overflow-hidden">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="text-base">Recently Added</CardTitle>
              <CardDescription>Fresh imports from your storage.</CardDescription>
            </div>
            <Button variant="outline" size="sm" onClick={() => void loadRecent()} disabled={recentLoading} className="gap-1.5">
              {recentLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
              Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent className="pt-0">
          {recentLoading && recentAlbums.length === 0 ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="w-4 h-4 animate-spin" />
              Loading…
            </div>
          ) : recentAlbums.length === 0 ? (
            <p className="text-sm text-muted-foreground">No recent albums yet.</p>
          ) : (
            <Carousel opts={{ align: 'start', dragFree: true }} className="w-full">
              <CarouselContent className="-ml-3">
                {recentAlbums.map((a) => (
                  <CarouselItem key={`recent-${a.album_id}`} className="basis-[160px] sm:basis-[190px] md:basis-[220px] pl-3">
                    <div className="group">
                      <Card className={cn('overflow-hidden border-border/60', unmatchedAlbumCardClass(a))}>
                        <AspectRatio ratio={1} className="bg-muted">
                          {a.thumb ? (
                            <img src={a.thumb} alt={a.title} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                          ) : (
                            <div className="w-full h-full flex items-center justify-center">
                              <Music className="w-10 h-10 text-muted-foreground" />
                            </div>
                          )}
                          {isUnmatchedAlbum(a) ? <div className="absolute top-2 left-2">{unmatchedBadge(a)}</div> : null}
                          <button
                            type="button"
                            onClick={() => void handlePlayAlbum(a.album_id, a.title, a.thumb)}
                            className={cn(
                              'absolute inset-0 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity',
                              'bg-black/35',
                            )}
                            title="Play"
                          >
                            <div className="h-12 w-12 rounded-full bg-white/15 backdrop-blur-sm border border-white/20 flex items-center justify-center">
                              <Play className="h-5 w-5 text-white fill-white" />
                            </div>
                          </button>
                        </AspectRatio>
                        <CardContent className="p-3 space-y-1.5">
                          <div className="flex items-start justify-between gap-2">
                            <div className="min-w-0 text-left">
                              <button
                                type="button"
                                onClick={() => navigate(`/library/album/${a.album_id}${location.search || ''}`)}
                                className="block min-w-0 text-left hover:underline"
                                title="Open album"
                              >
                                <p className="text-sm font-semibold truncate" title={a.title}>
                                  {a.title}
                                </p>
                              </button>
                              <button
                                type="button"
                                onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)}
                                className="block text-xs text-muted-foreground truncate hover:underline"
                                title="Open artist"
                              >
                                {a.artist_name}
                              </button>
                            </div>
                            <Badge variant="outline" className="text-[10px] shrink-0">
                              {a.year ?? '—'}
                            </Badge>
                            {isUnmatchedAlbum(a) ? (
                              <Badge variant="outline" className="text-[10px] border-amber-500/50 text-amber-700 dark:text-amber-300">
                                Verify tags
                              </Badge>
                            ) : null}
                          </div>
                        </CardContent>
                      </Card>
                    </div>
                  </CarouselItem>
                ))}
              </CarouselContent>
              <div className="hidden md:block">
                <CarouselPrevious />
                <CarouselNext />
              </div>
            </Carousel>
          )}
        </CardContent>
      </Card>

      {/* Digest */}
      <Collapsible open={digestOpen} onOpenChange={setDigestOpen}>
        <Card className="border-border/60 overflow-hidden">
          <CardHeader className="pb-3">
            <div className="flex items-start justify-between gap-3">
              <div className="space-y-1">
                <CardTitle className="text-base flex items-center gap-2">
                  Picked for You
                  {digestAlbums.length > 0 ? <Badge variant="secondary" className="text-[10px]">{digestAlbums.length}</Badge> : null}
                  {(digestEnrichment?.missing_total || 0) > 0 ? (
                    <Badge variant="outline" className="text-[10px]">
                      {digestEnrichment?.missing_total} pending
                    </Badge>
                  ) : null}
                </CardTitle>
                <CardDescription>Albums you may like, prioritized when review snippets are available.</CardDescription>
              </div>
              <CollapsibleTrigger asChild>
                <Button variant="ghost" size="sm" className="gap-2">
                  <span className="text-xs text-muted-foreground">{digestOpen ? 'Hide' : 'Show'}</span>
                  <ChevronDown className={cn('h-4 w-4 transition-transform', digestOpen ? 'rotate-180' : '')} />
                </Button>
              </CollapsibleTrigger>
            </div>
          </CardHeader>
          <CollapsibleContent>
            <CardContent className="space-y-3">
              <div className="flex items-center justify-between gap-3">
                <div className="text-xs text-muted-foreground">
                  Reviews are fetched in background when this digest loads, and also when you open an artist page.
                </div>
                <Button variant="outline" size="sm" onClick={() => void loadDigest()} disabled={digestLoading} className="gap-1.5">
                  {digestLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                  Refresh
                </Button>
              </div>

              {digestLoading && digestAlbums.length === 0 ? (
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Loading…
                </div>
              ) : digestAlbums.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  {(digestEnrichment?.missing_total || 0) > 0
                    ? `No review snippets yet. Background enrichment is working on ${digestEnrichment?.missing_total} recent album(s).`
                    : 'No recent albums with reviews yet.'}
                </p>
              ) : (
                <div className="space-y-3">
                  {digestAlbums.slice(0, 8).map((a) => {
                    const liked = Boolean(albumLikes[a.album_id]);
                    const genres = normalizeGenreBadges(a);
                    const shownGenres = genres.slice(0, 3);
                    const moreGenres = Math.max(0, genres.length - shownGenres.length);
                    return (
                      <div key={`dig-${a.album_id}`} className={cn('rounded-2xl border border-border/60 bg-card overflow-hidden', unmatchedAlbumCardClass(a))}>
                        <div className="grid grid-cols-1 sm:grid-cols-[160px,1fr] gap-0">
                          <div className="relative bg-muted">
                            <AspectRatio ratio={1} className="bg-muted">
                              {a.thumb ? (
                                <img src={a.thumb} alt={a.title} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                              ) : (
                                <div className="w-full h-full flex items-center justify-center">
                                  <Music className="w-10 h-10 text-muted-foreground" />
                                </div>
                              )}
                            </AspectRatio>
                            {isUnmatchedAlbum(a) ? <div className="absolute top-2 left-2">{unmatchedBadge(a)}</div> : null}
                            <div className="absolute inset-0 opacity-0 hover:opacity-100 transition-opacity bg-black/35" />
                            <div className="absolute top-2 right-2 flex items-center gap-2">
                              <Button
                                type="button"
                                size="icon"
                                variant="secondary"
                                className={cn('h-9 w-9 rounded-full', liked ? 'bg-primary text-primary-foreground hover:bg-primary/90' : 'bg-background/70')}
                                onClick={() => void toggleAlbumLike(a.album_id)}
                                title={liked ? 'Unlike' : 'Like'}
                              >
                                <Heart className={cn('h-4 w-4', liked ? 'fill-current' : '')} />
                              </Button>
                            </div>
                          </div>
                          <div className="p-4 sm:p-5 space-y-2">
                            <div className="flex items-start justify-between gap-3">
                              <div className="min-w-0">
                                <button
                                  type="button"
                                  className="text-sm font-semibold truncate hover:underline text-left w-full"
                                  onClick={() => navigate(`/library/album/${a.album_id}${location.search || ''}`)}
                                  title="Open album"
                                >
                                  {a.title}
                                </button>
                                <button
                                  type="button"
                                  className="text-xs text-muted-foreground truncate hover:underline"
                                  onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)}
                                  title="Open artist"
                                >
                                  {a.artist_name}
                                </button>
                              </div>
                              <div className="flex items-center justify-end flex-wrap gap-2 shrink-0">
                                <Badge variant="outline" className="text-[10px]">{a.year ?? '—'}</Badge>
                                {shownGenres.map((g) => (
                                  <Badge key={`dig-g-${a.album_id}-${g}`} variant="secondary" className="text-[10px] cursor-pointer" onClick={() => openGenre(g)} title="Browse genre">
                                    {g}
                                  </Badge>
                                ))}
                                {moreGenres > 0 ? <Badge variant="secondary" className="text-[10px]">+{moreGenres}</Badge> : null}
                                {a.label ? (
                                  <Badge variant="outline" className="text-[10px] cursor-pointer" onClick={() => openLabel(String(a.label || ''))} title="Open label">
                                    {a.label}
                                  </Badge>
                                ) : null}
                                {a.profile_source ? <Badge variant="outline" className="text-[10px]">Source: {a.profile_source}</Badge> : null}
                                {isUnmatchedAlbum(a) ? (
                                  <Badge variant="outline" className="text-[10px] border-amber-500/50 text-amber-700 dark:text-amber-300">
                                    Verify tags
                                  </Badge>
                                ) : null}
                              </div>
                            </div>
                            <p className="text-sm text-muted-foreground leading-relaxed line-clamp-3">
                              {a.short_description}
                            </p>
                            <div className="flex items-center gap-2 pt-1">
                              <Button size="sm" className="h-9 rounded-full gap-2" onClick={() => void handlePlayAlbum(a.album_id, a.title, a.thumb)}>
                                <Play className="h-4 w-4" /> Play
                              </Button>
                              <Button size="sm" variant="outline" className="h-9 rounded-full" onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)} title="Open artist">
                                <UserRound className="h-4 w-4" />
                              </Button>
                            </div>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </CardContent>
          </CollapsibleContent>
        </Card>
      </Collapsible>
    </div>
  );
}
