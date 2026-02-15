import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ChevronDown, Flame, Heart, Loader2, Music, Play, RefreshCw, Search, Sparkles, UserRound, X } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { AspectRatio } from '@/components/ui/aspect-ratio';
import { Slider } from '@/components/ui/slider';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Carousel, CarouselContent, CarouselItem, CarouselNext, CarouselPrevious } from '@/components/ui/carousel';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { cn } from '@/lib/utils';
import { usePlayback } from '@/contexts/PlaybackContext';
import { useToast } from '@/hooks/use-toast';
import * as api from '@/lib/api';
import type { TrackInfo } from '@/components/library/AudioPlayer';
import { FormatBadge } from '@/components/FormatBadge';

type SortMode = 'recent' | 'year_desc' | 'alpha' | 'artist';

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

function FacetSuggestInput({
  label,
  placeholder,
  value,
  fetchSuggestions,
  onSelectValue,
  onBrowseValue,
  onClearValue,
}: {
  label: string;
  placeholder: string;
  value: string;
  fetchSuggestions: (query: string) => Promise<api.LibraryFacetItem[]>;
  onSelectValue: (value: string) => void;
  onBrowseValue?: (value: string) => void;
  onClearValue?: () => void;
}) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const [query, setQuery] = useState<string>(value || '');
  const [items, setItems] = useState<api.LibraryFacetItem[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const trimmed = query.trim();

  useEffect(() => {
    setQuery(value || '');
  }, [value]);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    const id = setTimeout(async () => {
      setLoading(true);
      try {
        const res = await fetchSuggestions(trimmed);
        if (cancelled) return;
        setItems(Array.isArray(res) ? res : []);
      } catch {
        if (!cancelled) setItems([]);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }, 140);
    return () => {
      cancelled = true;
      clearTimeout(id);
    };
  }, [open, trimmed, fetchSuggestions]);

  useEffect(() => {
    const onPointerDown = (evt: MouseEvent) => {
      if (!rootRef.current) return;
      if (!rootRef.current.contains(evt.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', onPointerDown);
    return () => document.removeEventListener('mousedown', onPointerDown);
  }, []);

  return (
    <div ref={rootRef} className="relative">
      <div className="flex items-center justify-between gap-2">
        <div className="text-xs font-medium text-muted-foreground">{label}</div>
        {value ? (
          <div className="flex items-center gap-2">
            {onBrowseValue ? (
              <Button
                type="button"
                size="sm"
                variant="ghost"
                className="h-7 px-2 text-xs"
                onClick={() => onBrowseValue(value)}
                title="Open page"
              >
                Open
              </Button>
            ) : null}
            {onClearValue ? (
              <Button
                type="button"
                size="icon"
                variant="ghost"
                className="h-7 w-7"
                onClick={() => onClearValue()}
                title="Clear"
              >
                <X className="h-4 w-4" />
              </Button>
            ) : null}
          </div>
        ) : null}
      </div>

      <div className="relative mt-1">
        <Input
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            if (!open) setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          placeholder={placeholder}
          className="h-10 bg-background/80"
        />
        {loading ? (
          <Loader2 className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 animate-spin text-muted-foreground" />
        ) : null}
      </div>

      {open ? (
        <div className="absolute top-[calc(100%+0.35rem)] left-0 right-0 z-50 rounded-lg border border-border bg-popover shadow-xl overflow-hidden">
          {items.length === 0 ? (
            <div className="px-3 py-2.5 text-sm text-muted-foreground">
              {trimmed ? 'No matches' : 'Start typing…'}
            </div>
          ) : (
            <div className="max-h-[18rem] overflow-y-auto">
              {items.map((it) => (
                <button
                  key={`${label}:${it.value}`}
                  type="button"
                  onClick={() => {
                    setQuery(it.value);
                    onSelectValue(it.value);
                    setOpen(false);
                  }}
                  className={cn(
                    'w-full px-3 py-2.5 text-left border-b border-border/50 last:border-b-0 hover:bg-accent/70 transition-colors'
                  )}
                  title={`${it.count} album(s)`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="min-w-0">
                      <div className="font-medium text-sm truncate">{it.value}</div>
                    </div>
                    <div className="text-xs text-muted-foreground tabular-nums shrink-0">{it.count}</div>
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}

export default function Library() {
  const navigate = useNavigate();
  const { toast } = useToast();
  const { startPlayback, setCurrentTrack, recommendationSessionId, session } = usePlayback();

  const [stats, setStats] = useState<api.LibraryStats | null>(null);

  const [recoLoading, setRecoLoading] = useState(false);
  const [recoError, setRecoError] = useState<string | null>(null);
  const [recommendations, setRecommendations] = useState<api.RecoTrack[]>([]);

  const [recentLoading, setRecentLoading] = useState(false);
  const [recentAlbums, setRecentAlbums] = useState<api.LibraryAlbumItem[]>([]);

  const [recentlyPlayedLoading, setRecentlyPlayedLoading] = useState(false);
  const [recentlyPlayedError, setRecentlyPlayedError] = useState<string | null>(null);
  const [recentlyPlayedAlbums, setRecentlyPlayedAlbums] = useState<api.RecentlyPlayedAlbumItem[]>([]);

  const [digestLoading, setDigestLoading] = useState(false);
  const [digestOpen, setDigestOpen] = useState(false);
  const [digestAlbums, setDigestAlbums] = useState<api.LibraryAlbumItem[]>([]);
  const [digestEnrichment, setDigestEnrichment] = useState<api.LibraryDigestResponse['enrichment'] | null>(null);

  const [topArtistsLoading, setTopArtistsLoading] = useState(false);
  const [topArtists, setTopArtists] = useState<api.TopArtistItem[]>([]);

  const [facetsLoading, setFacetsLoading] = useState(false);
  const [facets, setFacets] = useState<api.LibraryFacetsResponse | null>(null);

  const [discoverLoading, setDiscoverLoading] = useState(false);
  const [discoverError, setDiscoverError] = useState<string | null>(null);
  const [discover, setDiscover] = useState<api.LibraryDiscoverResponse | null>(null);

  const [filtersOpen, setFiltersOpen] = useState(false);

  const [selectedGenre, setSelectedGenre] = useState<string>('');
  const [selectedLabel, setSelectedLabel] = useState<string>('');
  const [selectedYear, setSelectedYear] = useState<number | null>(null);

  const [albumLoading, setAlbumLoading] = useState(false);
  const [albumError, setAlbumError] = useState<string | null>(null);
  const [albums, setAlbums] = useState<api.LibraryAlbumItem[]>([]);
  const [totalAlbums, setTotalAlbums] = useState(0);

  const [albumLikes, setAlbumLikes] = useState<Record<number, boolean>>({});

  const [search, setSearch] = useState('');
  const searchDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [sort, setSort] = useState<SortMode>(() => (localStorage.getItem('pmda_library_sort') as SortMode) || 'recent');
  const [coverSize, setCoverSize] = useState<number>(() => {
    try {
      const raw = Number(localStorage.getItem('pmda_library_cover_size') || 220);
      return Number.isFinite(raw) ? Math.max(150, Math.min(320, raw)) : 220;
    } catch {
      return 220;
    }
  });

  const [offset, setOffset] = useState(0);
  const limit = 96;

  const gridTemplateColumns = useMemo(() => {
    const col = Math.max(140, Math.min(340, Math.floor(coverSize)));
    return `repeat(auto-fill, minmax(${col}px, 1fr))`;
  }, [coverSize]);

  const loadStats = useCallback(async () => {
    try {
      const s = await api.getLibraryStats();
      setStats(s);
    } catch {
      setStats(null);
    }
  }, []);

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

  const loadRecent = useCallback(async () => {
    try {
      setRecentLoading(true);
      const data = await api.getLibraryAlbums({ sort: 'recent', limit: 18, offset: 0 });
      setRecentAlbums(Array.isArray(data.albums) ? data.albums : []);
    } catch {
      setRecentAlbums([]);
    } finally {
      setRecentLoading(false);
    }
  }, []);

  const loadRecentlyPlayed = useCallback(async (opts?: { refresh?: boolean }) => {
    try {
      setRecentlyPlayedLoading(true);
      setRecentlyPlayedError(null);
      const data = await api.getLibraryRecentlyPlayedAlbums(90, 18, Boolean(opts?.refresh));
      setRecentlyPlayedAlbums(Array.isArray(data.albums) ? data.albums : []);
    } catch (e) {
      setRecentlyPlayedAlbums([]);
      setRecentlyPlayedError(e instanceof Error ? e.message : 'Failed to load recently played');
    } finally {
      setRecentlyPlayedLoading(false);
    }
  }, []);

  const loadDigest = useCallback(async () => {
    try {
      setDigestLoading(true);
      const data = await api.getLibraryDigest(18, true);
      setDigestAlbums(Array.isArray(data.albums) ? data.albums : []);
      setDigestEnrichment(data.enrichment ?? null);
    } catch {
      setDigestAlbums([]);
      setDigestEnrichment(null);
    } finally {
      setDigestLoading(false);
    }
  }, []);

  const loadTopArtists = useCallback(async () => {
    try {
      setTopArtistsLoading(true);
      const res = await api.getTopArtists(18, 0);
      setTopArtists(Array.isArray(res.artists) ? res.artists : []);
    } catch {
      setTopArtists([]);
    } finally {
      setTopArtistsLoading(false);
    }
  }, []);

  const loadFacets = useCallback(async () => {
    try {
      setFacetsLoading(true);
      const res = await api.getLibraryFacets();
      setFacets(res);
    } catch {
      setFacets(null);
    } finally {
      setFacetsLoading(false);
    }
  }, []);

  const loadDiscover = useCallback(async (opts?: { refresh?: boolean }) => {
    try {
      setDiscoverLoading(true);
      setDiscoverError(null);
      const res = await api.getLibraryDiscover(90, 18, Boolean(opts?.refresh));
      setDiscover(res);
    } catch (e) {
      setDiscover(null);
      setDiscoverError(e instanceof Error ? e.message : 'Failed to load discover feed');
    } finally {
      setDiscoverLoading(false);
    }
  }, []);

  const fetchGenreSuggestions = useCallback(async (q: string) => {
    const res = await api.suggestLibraryGenres(q, 16);
    return Array.isArray(res.genres) ? res.genres : [];
  }, []);

  const fetchLabelSuggestions = useCallback(async (q: string) => {
    const res = await api.suggestLibraryLabels(q, 16);
    return Array.isArray(res.labels) ? res.labels : [];
  }, []);

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
      await api.setLike({ entity_type: 'album', entity_id: albumId, liked: next, source: 'ui_library' });
    } catch (e) {
      setAlbumLikes((prev) => ({ ...prev, [albumId]: current }));
      toast({
        title: 'Like failed',
        description: e instanceof Error ? e.message : 'Failed to update like',
        variant: 'destructive',
      });
    }
  }, [albumLikes, toast]);

  const loadAlbums = useCallback(async (opts: { reset: boolean }) => {
    try {
      setAlbumLoading(true);
      setAlbumError(null);
      const nextOffset = opts.reset ? 0 : offset;
      const data = await api.getLibraryAlbums({
        search: search.trim() || undefined,
        sort,
        limit,
        offset: nextOffset,
        genre: selectedGenre || undefined,
        label: selectedLabel || undefined,
        year: selectedYear ?? undefined,
      });
      const list = Array.isArray(data.albums) ? data.albums : [];
      setTotalAlbums(typeof data.total === 'number' ? data.total : 0);
      setAlbums((prev) => (opts.reset ? list : [...prev, ...list]));
      setOffset(nextOffset + list.length);
      void hydrateAlbumLikes(list.map((a) => a.album_id));
    } catch (e) {
      setAlbumError(e instanceof Error ? e.message : 'Failed to load albums');
      if (opts.reset) setAlbums([]);
    } finally {
      setAlbumLoading(false);
    }
  }, [hydrateAlbumLikes, limit, offset, search, sort, selectedGenre, selectedLabel, selectedYear]);

  useEffect(() => {
    void loadStats();
    void loadRecommendations();
    void loadRecent();
    void loadRecentlyPlayed();
    void loadDigest();
    void loadTopArtists();
    void loadFacets();
    void loadDiscover();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    void hydrateAlbumLikes(digestAlbums.map((a) => a.album_id));
  }, [digestAlbums, hydrateAlbumLikes]);

  useEffect(() => {
    try {
      localStorage.setItem('pmda_library_sort', sort);
    } catch {
      // ignore
    }
  }, [sort]);

  useEffect(() => {
    // Debounced search: reset paging.
    if (searchDebounceRef.current) clearTimeout(searchDebounceRef.current);
    searchDebounceRef.current = setTimeout(() => {
      setOffset(0);
      void loadAlbums({ reset: true });
    }, 140);
    return () => {
      if (searchDebounceRef.current) clearTimeout(searchDebounceRef.current);
    };
  }, [search, sort, selectedGenre, selectedLabel, selectedYear, loadAlbums]);

  useEffect(() => {
    // Initial load.
    if (albums.length === 0 && !albumLoading) {
      void loadAlbums({ reset: true });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

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

  const canLoadMore = albums.length < totalAlbums && !albumLoading;

  return (
    <div className="container py-6 space-y-6">
      {/* Hero */}
      <div className="relative overflow-hidden rounded-3xl border border-border/60 bg-gradient-to-br from-card via-card to-accent/20 p-6 md:p-8">
          <div className="absolute inset-0 pointer-events-none opacity-80">
            <div className="absolute -top-20 -right-24 h-64 w-64 rounded-full bg-primary/10 blur-3xl" />
            <div className="absolute -bottom-24 -left-24 h-64 w-64 rounded-full bg-amber-500/10 blur-3xl" />
          </div>
          <div className="relative flex flex-col gap-4">
            <div className="flex items-start justify-between gap-4">
              <div className="min-w-0">
                <h1 className="text-3xl md:text-4xl font-bold tracking-tight">Library</h1>
                <p className="text-sm text-muted-foreground mt-1">
                  {stats ? (
                    <>
                      {stats.artists.toLocaleString()} artists · {stats.albums.toLocaleString()} albums
                    </>
                  ) : (
                    'Browse your local library'
                  )}
                </p>
                {(selectedGenre || selectedLabel || selectedYear) ? (
                  <div className="mt-3 flex flex-wrap items-center gap-2">
                    {selectedGenre ? (
                      <Badge
                        variant="secondary"
                        className="gap-1.5 cursor-pointer"
                        onClick={() => navigate(`/library/genre/${encodeURIComponent(selectedGenre)}`)}
                        title="Open genre"
                      >
                        Genre: {selectedGenre}
                      </Badge>
                    ) : null}
                    {selectedLabel ? (
                      <Badge
                        variant="secondary"
                        className="gap-1.5 cursor-pointer"
                        onClick={() => navigate(`/library/label/${encodeURIComponent(selectedLabel)}`)}
                        title="Open label"
                      >
                        Label: {selectedLabel}
                      </Badge>
                    ) : null}
                    {selectedYear ? <Badge variant="secondary" className="gap-1.5">Year: {selectedYear}</Badge> : null}
                    <Button
                      type="button"
                      size="sm"
                      variant="ghost"
                      className="h-7 px-2"
                      onClick={() => {
                        setSelectedGenre('');
                        setSelectedLabel('');
                        setSelectedYear(null);
                        setOffset(0);
                      }}
                      title="Clear filters"
                    >
                      <X className="h-4 w-4 mr-1" /> Clear
                    </Button>
                  </div>
                ) : null}
              </div>
              <div className="flex items-center gap-2">
                <Button variant="outline" className="gap-2" onClick={() => void loadRecommendations()} disabled={recoLoading}>
                  {recoLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                  Refresh
                </Button>
              </div>
            </div>

            {/* Search + controls */}
            <div className="flex flex-col md:flex-row md:items-center gap-3">
              <div className="relative flex-1">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                <Input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search albums or artists…" className="pl-9 h-11 bg-background/80" />
              </div>
              <div className="flex items-center gap-3">
                <Select value={sort} onValueChange={(v) => setSort(v as SortMode)}>
                  <SelectTrigger className="w-[170px] h-11 bg-background/80">
                    <SelectValue placeholder="Sort" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="recent">Recently added</SelectItem>
                    <SelectItem value="year_desc">Year (desc)</SelectItem>
                    <SelectItem value="alpha">Title (A-Z)</SelectItem>
                    <SelectItem value="artist">Artist</SelectItem>
                  </SelectContent>
                </Select>

                <div className="hidden md:flex items-center gap-2 w-[220px]">
                  <span className="text-xs text-muted-foreground w-12">Size</span>
                  <Slider
                    value={[coverSize]}
                    min={150}
                    max={320}
                    step={10}
                    onValueChange={(v) => {
                      const next = v[0];
                      setCoverSize(next);
                      try {
                        localStorage.setItem('pmda_library_cover_size', String(next));
                      } catch {
                        // ignore
                      }
                    }}
                  />
                </div>
              </div>
            </div>

            {/* Advanced filters */}
            <div className="flex items-center justify-between gap-3">
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="gap-2"
                onClick={() => setFiltersOpen((v) => !v)}
              >
                <ChevronDown className={cn('h-4 w-4 transition-transform', filtersOpen ? 'rotate-180' : '')} />
                Filters
              </Button>
              <div className="text-xs text-muted-foreground">
                Filter by genre, label, or year.
              </div>
            </div>
            {filtersOpen ? (
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                <FacetSuggestInput
                  label="Genre"
                  placeholder="Type a genre…"
                  value={selectedGenre}
                  fetchSuggestions={fetchGenreSuggestions}
                  onSelectValue={(v) => {
                    setSelectedGenre(v);
                    setOffset(0);
                  }}
                  onBrowseValue={(v) => navigate(`/library/genre/${encodeURIComponent(v)}`)}
                  onClearValue={() => {
                    setSelectedGenre('');
                    setOffset(0);
                  }}
                />
                <FacetSuggestInput
                  label="Label"
                  placeholder="Type a label…"
                  value={selectedLabel}
                  fetchSuggestions={fetchLabelSuggestions}
                  onSelectValue={(v) => {
                    setSelectedLabel(v);
                    setOffset(0);
                  }}
                  onBrowseValue={(v) => navigate(`/library/label/${encodeURIComponent(v)}`)}
                  onClearValue={() => {
                    setSelectedLabel('');
                    setOffset(0);
                  }}
                />

                <div>
                  <div className="text-xs font-medium text-muted-foreground">Year</div>
                  <div className="mt-1">
                    <Select
                      value={selectedYear ? String(selectedYear) : 'all'}
                      onValueChange={(v) => {
                        if (v === 'all') {
                          setSelectedYear(null);
                        } else {
                          const n = Number(v);
                          setSelectedYear(Number.isFinite(n) && n > 0 ? n : null);
                        }
                        setOffset(0);
                      }}
                      disabled={facetsLoading && !facets}
                    >
                      <SelectTrigger className="h-10 bg-background/80">
                        <SelectValue placeholder="Any year" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">Any year</SelectItem>
                        {(facets?.years || []).slice(0, 80).map((y) => (
                          <SelectItem key={`yr-${y.value}`} value={String(y.value)}>
                            {y.value} ({y.count})
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>
              </div>
            ) : null}
          </div>
        </div>

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
                      const label = typeof seed.label === 'string' ? seed.label.trim() : '';
                      if (!label) return null;
                      return (
                        <Button
                          type="button"
                          size="sm"
                          variant="secondary"
                          className="h-7 px-2 text-xs shrink-0"
                          onClick={() => navigate(`/library/label/${encodeURIComponent(label)}`)}
                          title="Open label"
                        >
                          Open label
                        </Button>
                      );
                    }
                    if (sec.key === 'genre') {
                      const genre = typeof seed.genre === 'string' ? seed.genre.trim() : '';
                      if (!genre) return null;
                      return (
                        <Button
                          type="button"
                          size="sm"
                          variant="secondary"
                          className="h-7 px-2 text-xs shrink-0"
                          onClick={() => navigate(`/library/genre/${encodeURIComponent(genre)}`)}
                          title="Open genre"
                        >
                          Open genre
                        </Button>
                      );
                    }
                    if (sec.key === 'year') {
                      const yearRaw = seed.year;
                      const year = typeof yearRaw === 'number' ? yearRaw : Number(yearRaw);
                      if (!Number.isFinite(year) || year <= 0) return null;
                      return (
                        <Button
                          type="button"
                          size="sm"
                          variant="secondary"
                          className="h-7 px-2 text-xs shrink-0"
                          onClick={() => {
                            setSelectedYear(year);
                            setOffset(0);
                          }}
                          title="Filter by year"
                        >
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
                            <Card className="overflow-hidden border-border/60">
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
                                      onClick={() => navigate(`/library/album/${a.album_id}`)}
                                      className="block min-w-0 text-left hover:underline"
                                      title="Open album"
                                    >
                                      <p className="text-sm font-semibold truncate" title={a.title}>
                                        {a.title}
                                      </p>
                                    </button>
                                    <button
                                      type="button"
                                      onClick={() => navigate(`/library/artist/${a.artist_id}`)}
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
                                <div className="flex items-center justify-between gap-2">
                                  {a.format ? <FormatBadge format={a.format} size="sm" /> : <span />}
                                  {a.is_lossless ? (
                                    <Badge variant="secondary" className="text-[10px]">Lossless</Badge>
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
                <CardDescription>Personalized picks from your listening history.</CardDescription>
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
                      <button
                        type="button"
                        onClick={() => navigate(`/library/artist/${a.artist_id}`)}
                        className="w-full text-left group"
                      >
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

        {/* Recently Added (carousel) */}
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
                        <Card className="overflow-hidden border-border/60">
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
                                  onClick={() => navigate(`/library/album/${a.album_id}`)}
                                  className="block min-w-0 text-left hover:underline"
                                  title="Open album"
                                >
                                  <p className="text-sm font-semibold truncate" title={a.title}>
                                    {a.title}
                                  </p>
                                </button>
                                <button
                                  type="button"
                                  onClick={() => navigate(`/library/artist/${a.artist_id}`)}
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
                  <CarouselPrevious />
                  <CarouselNext />
                </div>
              </Carousel>
            )}
          </CardContent>
        </Card>

        {/* Recently Played (carousel) */}
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
                        <Card className="overflow-hidden border-border/60">
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
                                  onClick={() => navigate(`/library/album/${a.album_id}`)}
                                  className="block min-w-0 text-left hover:underline"
                                  title="Open album"
                                >
                                  <p className="text-sm font-semibold truncate" title={a.title}>
                                    {a.title}
                                  </p>
                                </button>
                                <button
                                  type="button"
                                  onClick={() => navigate(`/library/artist/${a.artist_id}`)}
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
                  <CarouselPrevious />
                  <CarouselNext />
                </div>
              </Carousel>
            )}
          </CardContent>
        </Card>

        {/* Digest / Journal */}
        <Collapsible open={digestOpen} onOpenChange={setDigestOpen}>
          <Card className="border-border/60 overflow-hidden">
            <CardHeader className="pb-3">
              <div className="flex items-start justify-between gap-3">
                <div className="space-y-1">
                  <CardTitle className="text-base flex items-center gap-2">
                    Library Digest
                    {digestAlbums.length > 0 ? <Badge variant="secondary" className="text-[10px]">{digestAlbums.length}</Badge> : null}
                    {(digestEnrichment?.missing_total || 0) > 0 ? (
                      <Badge variant="outline" className="text-[10px]">
                        {digestEnrichment?.missing_total} pending
                      </Badge>
                    ) : null}
                  </CardTitle>
                  <CardDescription>New arrivals with quick context (reviews when available).</CardDescription>
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
                        <div key={`dig-${a.album_id}`} className="rounded-2xl border border-border/60 bg-card overflow-hidden">
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
                                    onClick={() => navigate(`/library/album/${a.album_id}`)}
                                    title="Open album"
                                  >
                                    {a.title}
                                  </button>
                                  <button type="button" className="text-xs text-muted-foreground truncate hover:underline" onClick={() => navigate(`/library/artist/${a.artist_id}`)}>
                                    {a.artist_name}
                                  </button>
                                </div>
                                <div className="flex items-center justify-end flex-wrap gap-2 shrink-0">
                                  <Badge variant="outline" className="text-[10px]">{a.year ?? '—'}</Badge>
                                  {shownGenres.map((g) => (
                                    <Badge
                                      key={`dig-g-${a.album_id}-${g}`}
                                      variant="secondary"
                                      className="text-[10px] cursor-pointer"
                                      title="Browse genre"
                                      onClick={() => navigate(`/library/genre/${encodeURIComponent(g)}`)}
                                    >
                                      {g}
                                    </Badge>
                                  ))}
                                  {moreGenres > 0 ? <Badge variant="secondary" className="text-[10px]">+{moreGenres}</Badge> : null}
                                  {a.label ? (
                                    <Badge
                                      variant="outline"
                                      className="text-[10px] cursor-pointer"
                                      title="Open label"
                                      onClick={() => navigate(`/library/label/${encodeURIComponent(a.label || '')}`)}
                                    >
                                      {a.label}
                                    </Badge>
                                  ) : null}
                                  {a.profile_source ? <Badge variant="outline" className="text-[10px]">Source: {a.profile_source}</Badge> : null}
                                </div>
                              </div>
                              <p className="text-sm text-muted-foreground leading-relaxed line-clamp-3">
                                {a.short_description}
                              </p>
                              <div className="flex items-center gap-2 pt-1">
                                <Button size="sm" className="h-9 rounded-full gap-2" onClick={() => void handlePlayAlbum(a.album_id, a.title, a.thumb)}>
                                  <Play className="h-4 w-4" /> Play
                                </Button>
                                <Button size="sm" variant="outline" className="h-9 rounded-full" onClick={() => navigate(`/library/artist/${a.artist_id}`)} title="Open artist">
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

        {/* Albums Grid */}
        <div className="space-y-3">
          <div className="flex items-end justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold">Albums</h2>
              <p className="text-xs text-muted-foreground">
                {totalAlbums > 0 ? `${albums.length.toLocaleString()} / ${totalAlbums.toLocaleString()}` : ' '}
              </p>
            </div>
            {albumError ? (
              <Badge variant="outline" className="text-xs border-destructive/50 text-destructive">
                {albumError}
              </Badge>
            ) : null}
          </div>

          <div className="grid gap-4" style={{ gridTemplateColumns }}>
            {albums.map((a) => (
              <div key={`alb-${a.album_id}`} className="group">
                <div className="relative overflow-hidden rounded-2xl border border-border/60 bg-card shadow-sm">
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
                    <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity bg-black/35" />
                    <div className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity">
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
                    <div className="absolute inset-x-0 bottom-0 p-3 opacity-0 group-hover:opacity-100 transition-opacity">
                      <div className="flex items-center justify-between gap-2">
                        <Button size="sm" className="h-9 rounded-full gap-2" onClick={() => void handlePlayAlbum(a.album_id, a.title, a.thumb)}>
                          <Play className="h-4 w-4" />
                          Play
                        </Button>
                        <Button size="sm" variant="outline" className="h-9 rounded-full" onClick={() => navigate(`/library/artist/${a.artist_id}`)}>
                          <UserRound className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>
                  </AspectRatio>

                  <div className="p-3 space-y-2">
                    <div className="min-w-0">
                      <button
                        type="button"
                        className="text-sm font-semibold truncate hover:underline text-left w-full"
                        title="Open album"
                        onClick={() => navigate(`/library/album/${a.album_id}`)}
                      >
                        {a.title}
                      </button>
                      <button type="button" className="text-xs text-muted-foreground truncate hover:underline" onClick={() => navigate(`/library/artist/${a.artist_id}`)}>
                        {a.artist_name}
                      </button>
                    </div>
                    <div className="flex flex-wrap items-center gap-1.5">
                      {a.format ? <FormatBadge format={a.format} size="sm" /> : null}
                      <Badge variant={a.is_lossless ? 'secondary' : 'outline'} className="text-[10px]">
                        {a.is_lossless ? 'Lossless' : 'Lossy'}
                      </Badge>
                      <Badge variant="outline" className="text-[10px]">
                        {a.year ?? '—'}
                      </Badge>
                      <Badge variant="outline" className="text-[10px]">
                        {a.track_count}t
                      </Badge>
                    </div>
                    {(normalizeGenreBadges(a).length > 0 || a.label) ? (
                      <div className="flex flex-wrap items-center gap-1.5">
                        {(() => {
                          const genres = normalizeGenreBadges(a);
                          const shown = genres.slice(0, 6);
                          const more = Math.max(0, genres.length - shown.length);
                          return (
                            <>
                              {shown.map((g) => (
                                <Badge
                                  key={`alb-g-${a.album_id}-${g}`}
                                  variant="secondary"
                                  className="text-[10px] cursor-pointer"
                                  title="Browse genre"
                                  onClick={() => navigate(`/library/genre/${encodeURIComponent(g)}`)}
                                >
                                  {g}
                                </Badge>
                              ))}
                              {more > 0 ? <Badge variant="secondary" className="text-[10px]">+{more}</Badge> : null}
                            </>
                          );
                        })()}
                        {a.label ? (
                          <Badge
                            variant="outline"
                            className="text-[10px] cursor-pointer"
                            title="Open label"
                            onClick={() => navigate(`/library/label/${encodeURIComponent(a.label || '')}`)}
                          >
                            {a.label}
                          </Badge>
                        ) : null}
                      </div>
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

          <div className="flex items-center justify-center py-6">
            <Button
              variant="outline"
              className="gap-2"
              onClick={() => void loadAlbums({ reset: false })}
              disabled={!canLoadMore}
            >
              {albumLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
              {canLoadMore ? 'Load more' : 'All loaded'}
            </Button>
          </div>
        </div>
    </div>
  );
}
