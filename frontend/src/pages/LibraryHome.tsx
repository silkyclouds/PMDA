import { useCallback, useEffect, useState } from 'react';
import { useLocation, useNavigate, useOutletContext } from 'react-router-dom';
import { ArrowDown, ArrowUp, Flame, Loader2, Music, Play, RefreshCw, Sparkles, UserRound } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { AspectRatio } from '@/components/ui/aspect-ratio';
import { Carousel, CarouselContent, CarouselItem, CarouselNext, CarouselPrevious } from '@/components/ui/carousel';
import { AlbumArtwork } from '@/components/library/AlbumArtwork';
import { usePlayback } from '@/contexts/PlaybackContext';
import { useToast } from '@/hooks/use-toast';
import * as api from '@/lib/api';
import type { TrackInfo } from '@/components/library/AudioPlayer';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';

const HOME_SECTION_KEYS = [
  'discover',
  'for_you',
  'top_artists',
  'recent_artists',
  'recently_played',
  'recently_added',
] as const;
type HomeSectionKey = (typeof HOME_SECTION_KEYS)[number];
const HOME_SECTION_LABEL: Record<HomeSectionKey, string> = {
  discover: 'Discover',
  for_you: 'For You',
  top_artists: 'Top Artists',
  recent_artists: 'Recently Added Artists',
  recently_played: 'Recently Played',
  recently_added: 'Recently Added',
};

function normalizeHomeSectionOrder(raw: unknown): HomeSectionKey[] {
  const items = Array.isArray(raw) ? raw : [];
  const out: HomeSectionKey[] = [];
  const seen = new Set<HomeSectionKey>();
  for (const item of items) {
    const key = String(item || '').trim() as HomeSectionKey;
    if (!HOME_SECTION_KEYS.includes(key) || seen.has(key)) continue;
    seen.add(key);
    out.push(key);
  }
  for (const key of HOME_SECTION_KEYS) {
    if (!seen.has(key)) out.push(key);
  }
  return out;
}

export default function LibraryHome() {
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched } = useOutletContext<LibraryOutletContext>();
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
  const [sectionOrder, setSectionOrder] = useState<HomeSectionKey[]>(() => {
    try {
      const raw = localStorage.getItem('pmda_library_home_sections');
      const parsed = raw ? JSON.parse(raw) : [];
      return normalizeHomeSectionOrder(parsed);
    } catch {
      return [...HOME_SECTION_KEYS];
    }
  });

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

  useEffect(() => {
    void loadDiscover();
    void loadTopArtists();
    void loadRecentArtists();
    void loadRecentlyPlayed();
    void loadRecent();
  }, [includeUnmatched, loadDiscover, loadTopArtists, loadRecentArtists, loadRecentlyPlayed, loadRecent]);

  useEffect(() => {
    void loadRecommendations();
  }, [loadRecommendations]);

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

  const moveSection = useCallback((key: HomeSectionKey, dir: -1 | 1) => {
    setSectionOrder((prev) => {
      const list = normalizeHomeSectionOrder(prev);
      const idx = list.indexOf(key);
      if (idx < 0) return list;
      const nextIdx = idx + dir;
      if (nextIdx < 0 || nextIdx >= list.length) return list;
      const next = [...list];
      const [item] = next.splice(idx, 1);
      next.splice(nextIdx, 0, item);
      return next;
    });
  }, []);

  useEffect(() => {
    try {
      localStorage.setItem('pmda_library_home_sections', JSON.stringify(sectionOrder));
    } catch {
      // ignore
    }
  }, [sectionOrder]);

  const sectionOrderStyle = useCallback((key: HomeSectionKey): { order: number } => {
    const idx = sectionOrder.indexOf(key);
    return { order: idx >= 0 ? idx : 999 };
  }, [sectionOrder]);

  return (
    <div className="container pb-6 flex flex-col gap-5 md:gap-6">
      <Card className="pmda-shelf overflow-hidden animate-in fade-in-0 slide-in-from-bottom-2 duration-300">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Home Layout</CardTitle>
          <CardDescription>Reorder home sections.</CardDescription>
        </CardHeader>
        <CardContent className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-2">
          {sectionOrder.map((key, idx) => (
            <div key={`home-sec-order-${key}`} className="flex items-center justify-between rounded-md border border-border/70 bg-background/70 px-2.5 py-2">
              <span className="text-sm">{HOME_SECTION_LABEL[key]}</span>
              <div className="flex items-center gap-1">
                <Button
                  type="button"
                  size="icon"
                  variant="ghost"
                  className="h-7 w-7"
                  onClick={() => moveSection(key, -1)}
                  disabled={idx === 0}
                  title="Move up"
                >
                  <ArrowUp className="h-3.5 w-3.5" />
                </Button>
                <Button
                  type="button"
                  size="icon"
                  variant="ghost"
                  className="h-7 w-7"
                  onClick={() => moveSection(key, 1)}
                  disabled={idx === sectionOrder.length - 1}
                  title="Move down"
                >
                  <ArrowDown className="h-3.5 w-3.5" />
                </Button>
              </div>
            </div>
          ))}
        </CardContent>
      </Card>
      {/* Discover */}
      <div style={sectionOrderStyle('discover')}>
      <Card className="pmda-shelf overflow-hidden animate-in fade-in-0 slide-in-from-bottom-2 duration-300">
        <CardHeader className="pb-3">
          <div className="flex flex-wrap items-start justify-between gap-3">
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
              className="h-9 gap-1.5 shrink-0"
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
                <Carousel opts={{ align: 'start', dragFree: true }} className="w-full pmda-fade-mask">
                  <CarouselContent className="-ml-3">
                    {sec.albums.map((a) => (
                      <CarouselItem key={`disc-${sec.key}-${a.album_id}`} className="basis-[160px] sm:basis-[190px] md:basis-[220px] pl-3">
                        <div className="group">
                          <Card className="overflow-hidden border-border/60 bg-card/90 transition-all duration-300 hover:-translate-y-1 hover:border-primary/35 hover:shadow-card-hover">
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
                              <AlbumArtwork albumThumb={a.thumb} artistId={a.artist_id} alt={a.title} size={512} />
                              <button
                                type="button"
                                onClick={() => void handlePlayAlbum(a.album_id, a.title, a.thumb)}
                                className="absolute inset-0 flex items-center justify-center opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity bg-black/35"
                                title="Play"
                              >
                                <div className="h-10 w-10 sm:h-12 sm:w-12 rounded-full bg-white/15 backdrop-blur-sm border border-white/20 flex items-center justify-center">
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
                              </div>
                            </CardContent>
                          </Card>
                        </div>
                      </CarouselItem>
                    ))}
                  </CarouselContent>
                  <div className="hidden md:block">
                    <CarouselPrevious className="left-2 top-[42%] h-9 w-9 bg-background/70 backdrop-blur-sm" />
                    <CarouselNext className="right-2 top-[42%] h-9 w-9 bg-background/70 backdrop-blur-sm" />
                  </div>
                </Carousel>
              )}
            </div>
          ))}
        </CardContent>
      </Card>
      </div>

      {/* For You */}
      <div style={sectionOrderStyle('for_you')}>
      <Card className="pmda-shelf overflow-hidden animate-in fade-in-0 slide-in-from-bottom-2 duration-300">
        <CardHeader className="pb-3">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="text-base flex items-center gap-2">
                <Sparkles className="h-4 w-4 text-primary" />
                For You
              </CardTitle>
              <CardDescription>Personalized track picks from your listening history.</CardDescription>
            </div>
            <Button variant="outline" size="sm" onClick={() => void loadRecommendations()} disabled={recoLoading} className="h-9 gap-1.5 shrink-0">
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
                  className="group rounded-xl border border-border/60 bg-card/90 p-3 text-left hover:-translate-y-0.5 hover:border-primary/40 hover:bg-accent/20 transition-all duration-300"
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
      </div>

      {/* Top Artists */}
      <div style={sectionOrderStyle('top_artists')}>
      <Card className="pmda-shelf overflow-hidden animate-in fade-in-0 slide-in-from-bottom-2 duration-300">
        <CardHeader className="pb-3">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="text-base">Top Artists</CardTitle>
              <CardDescription>Most played from your local listening stats.</CardDescription>
            </div>
            <Button variant="outline" size="sm" onClick={() => void loadTopArtists()} disabled={topArtistsLoading} className="h-9 gap-1.5 shrink-0">
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
            <Carousel opts={{ align: 'start', dragFree: true }} className="w-full pmda-fade-mask">
              <CarouselContent className="-ml-3">
                {topArtists.map((a) => (
                  <CarouselItem key={`ta-${a.artist_id}`} className="basis-[170px] sm:basis-[200px] md:basis-[220px] pl-3">
                    <button type="button" onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)} className="w-full text-left group">
                      <Card className="overflow-hidden border-border/60 bg-card/90 transition-all duration-300 hover:-translate-y-1 hover:border-primary/35 hover:bg-accent/20">
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
                <CarouselPrevious className="left-2 top-[42%] h-9 w-9 bg-background/70 backdrop-blur-sm" />
                <CarouselNext className="right-2 top-[42%] h-9 w-9 bg-background/70 backdrop-blur-sm" />
              </div>
            </Carousel>
          )}
        </CardContent>
      </Card>
      </div>

      {/* Recently Added Artists */}
      <div style={sectionOrderStyle('recent_artists')}>
      <Card className="pmda-shelf overflow-hidden animate-in fade-in-0 slide-in-from-bottom-2 duration-300">
        <CardHeader className="pb-3">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="text-base">Recently Added Artists</CardTitle>
              <CardDescription>Artists with the most recent imports.</CardDescription>
            </div>
            <Button variant="outline" size="sm" onClick={() => void loadRecentArtists()} disabled={recentArtistsLoading} className="h-9 gap-1.5 shrink-0">
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
            <Carousel opts={{ align: 'start', dragFree: true }} className="w-full pmda-fade-mask">
              <CarouselContent className="-ml-3">
                {recentArtists.map((a) => (
                  <CarouselItem key={`ra-${a.artist_id}`} className="basis-[170px] sm:basis-[200px] md:basis-[220px] pl-3">
                    <button type="button" onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)} className="w-full text-left group">
                      <Card className="overflow-hidden border-border/60 bg-card/90 transition-all duration-300 hover:-translate-y-1 hover:border-primary/35 hover:bg-accent/20">
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
                <CarouselPrevious className="left-2 top-[42%] h-9 w-9 bg-background/70 backdrop-blur-sm" />
                <CarouselNext className="right-2 top-[42%] h-9 w-9 bg-background/70 backdrop-blur-sm" />
              </div>
            </Carousel>
          )}
        </CardContent>
      </Card>
      </div>

      {/* Recently Played */}
      <div style={sectionOrderStyle('recently_played')}>
      <Card className="pmda-shelf overflow-hidden animate-in fade-in-0 slide-in-from-bottom-2 duration-300">
        <CardHeader className="pb-3">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="text-base">Recently Played</CardTitle>
              <CardDescription>What you played lately, across the whole library.</CardDescription>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={() => void loadRecentlyPlayed({ refresh: true })}
              disabled={recentlyPlayedLoading}
              className="h-9 gap-1.5 shrink-0"
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
            <Carousel opts={{ align: 'start', dragFree: true }} className="w-full pmda-fade-mask">
              <CarouselContent className="-ml-3">
                {recentlyPlayedAlbums.map((a) => (
                  <CarouselItem key={`rplay-${a.album_id}`} className="basis-[160px] sm:basis-[190px] md:basis-[220px] pl-3">
                    <div className="group">
                      <Card className="overflow-hidden border-border/60 bg-card/90 transition-all duration-300 hover:-translate-y-1 hover:border-primary/35 hover:shadow-card-hover">
                        <AspectRatio ratio={1} className="bg-muted">
                          <AlbumArtwork albumThumb={a.thumb} artistId={a.artist_id} alt={a.title} size={512} />
                          <button
                            type="button"
                            onClick={() => void handlePlayAlbum(a.album_id, a.title, a.thumb)}
                            className="absolute inset-0 flex items-center justify-center opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity bg-black/35"
                            title="Play"
                          >
                            <div className="h-10 w-10 sm:h-12 sm:w-12 rounded-full bg-white/15 backdrop-blur-sm border border-white/20 flex items-center justify-center">
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
                          </div>
                        </CardContent>
                      </Card>
                    </div>
                  </CarouselItem>
                ))}
              </CarouselContent>
              <div className="hidden md:block">
                <CarouselPrevious className="left-2 top-[42%] h-9 w-9 bg-background/70 backdrop-blur-sm" />
                <CarouselNext className="right-2 top-[42%] h-9 w-9 bg-background/70 backdrop-blur-sm" />
              </div>
            </Carousel>
          )}
        </CardContent>
      </Card>
      </div>

      {/* Recently Added */}
      <div style={sectionOrderStyle('recently_added')}>
      <Card className="pmda-shelf overflow-hidden animate-in fade-in-0 slide-in-from-bottom-2 duration-300">
        <CardHeader className="pb-3">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="text-base">Recently Added</CardTitle>
              <CardDescription>Fresh imports from your storage.</CardDescription>
            </div>
            <Button variant="outline" size="sm" onClick={() => void loadRecent()} disabled={recentLoading} className="h-9 gap-1.5 shrink-0">
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
            <Carousel opts={{ align: 'start', dragFree: true }} className="w-full pmda-fade-mask">
              <CarouselContent className="-ml-3">
                {recentAlbums.map((a) => (
                  <CarouselItem key={`recent-${a.album_id}`} className="basis-[160px] sm:basis-[190px] md:basis-[220px] pl-3">
                    <div className="group">
                      <Card className="overflow-hidden border-border/60 bg-card/90 transition-all duration-300 hover:-translate-y-1 hover:border-primary/35 hover:shadow-card-hover">
                        <AspectRatio ratio={1} className="bg-muted">
                          <AlbumArtwork albumThumb={a.thumb} artistId={a.artist_id} alt={a.title} size={512} />
                          <button
                            type="button"
                            onClick={() => void handlePlayAlbum(a.album_id, a.title, a.thumb)}
                            className="absolute inset-0 flex items-center justify-center opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity bg-black/35"
                            title="Play"
                          >
                            <div className="h-10 w-10 sm:h-12 sm:w-12 rounded-full bg-white/15 backdrop-blur-sm border border-white/20 flex items-center justify-center">
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
                          </div>
                        </CardContent>
                      </Card>
                    </div>
                  </CarouselItem>
                ))}
              </CarouselContent>
              <div className="hidden md:block">
                <CarouselPrevious className="left-2 top-[42%] h-9 w-9 bg-background/70 backdrop-blur-sm" />
                <CarouselNext className="right-2 top-[42%] h-9 w-9 bg-background/70 backdrop-blur-sm" />
              </div>
            </Carousel>
          )}
        </CardContent>
      </Card>
      </div>

    </div>
  );
}
