import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutletContext, useParams } from 'react-router-dom';
import { ArrowLeft, Calendar, ChevronDown, ChevronUp, Disc3, ExternalLink, Heart, Loader2, Music, Play, RefreshCw, Sparkles, Users } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent } from '@/components/ui/card';
import { AspectRatio } from '@/components/ui/aspect-ratio';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { cn } from '@/lib/utils';
import { FormatBadge } from '@/components/FormatBadge';
import * as api from '@/lib/api';
import { useToast } from '@/hooks/use-toast';
import { ConcertsMiniMap } from '@/components/concerts/ConcertsMiniMap';
import { usePlayback } from '@/contexts/PlaybackContext';
import type { TrackInfo } from '@/components/library/AudioPlayer';

interface SimilarArtist {
  name: string;
  mbid?: string;
  type?: string;
  artist_id?: number;
  image_url?: string;
}

interface ArtistProfile {
  bio?: string;
  short_bio?: string;
  tags?: string[];
  similar_artists?: SimilarArtist[];
  source?: string;
  updated_at?: number;
}

interface AlbumInfo {
  album_id: number;
  title: string;
  year?: number;
  date?: string;
  track_count: number;
  type: string;
  thumb?: string;
  format?: string;
  is_lossless?: boolean;
  mb_identified?: boolean;
  short_description?: string;
}

interface ArtistDetailResponse {
  artist_id: number;
  artist_name: string;
  artist_thumb?: string;
  albums: AlbumInfo[];
  total_albums: number;
  artist_profile?: ArtistProfile;
  profile_enriching?: boolean;
}

interface ArtistProfileResponse {
  artist_id: number;
  artist_name: string;
  profile?: ArtistProfile;
  enriching?: boolean;
}

const albumTypeOrder = ['Album', 'EP', 'Single', 'Compilation', 'Anthology'];

function wordCount(text: string): number {
  const t = (text || '').trim();
  if (!t) return 0;
  const m = t.match(/[A-Za-z0-9]+(?:'[A-Za-z0-9]+)?/g);
  return m ? m.length : 0;
}

function isAcceptableOriginal(text: string): boolean {
  // Spec: accept original bios when they are long-form (>= 100 words).
  return wordCount(text) >= 100;
}

function isProbablyPlaceholderArtistImageUrl(url: string): boolean {
  const low = (url || '').trim().toLowerCase();
  if (!low) return true;
  const tokens = [
    '2a96cbd8b46e442fc41c2b86b821562f',
    '4128a6eb29f94943c9d206c08e625904',
    'c6f59c1e5e7240a4c0d427abd71f3dbb',
  ];
  if (tokens.some((t) => low.includes(t))) return true;
  if (low.includes('default') && (low.includes('last.fm') || low.includes('lastfm'))) return true;
  return false;
}

function initialsFromName(name: string): string {
  const words = (name || '')
    .trim()
    .split(/\s+/g)
    .filter(Boolean);
  if (words.length === 0) return '?';
  const a = words[0]?.[0] || '?';
  const b = words.length > 1 ? (words[1]?.[0] || '') : '';
  return (a + b).toUpperCase();
}

function toCoord(s?: string): number | null {
  const raw = (s || '').trim();
  if (!raw) return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

function isUnmatchedAlbum(album: AlbumInfo): boolean {
  return album.mb_identified === false;
}

function haversineKm(a: { lat: number; lon: number }, b: { lat: number; lon: number }) {
  const R = 6371;
  const toRad = (deg: number) => (deg * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lon - a.lon);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const x = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * (Math.sin(dLon / 2) ** 2);
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(x)));
}

export default function ArtistPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched: includeUnmatchedContext } = useOutletContext<{ includeUnmatched: boolean }>();
  const { startPlayback, setCurrentTrack } = usePlayback();
  const params = useParams<{ artistId: string }>();
  const artistId = Number(params.artistId);
  const { toast } = useToast();
  const autoAiRequestedRef = useRef(false);
  const autoFactsRequestedRef = useRef(false);
  const similarRefreshAttemptsRef = useRef(0);

  const [loading, setLoading] = useState(true);
  const [details, setDetails] = useState<ArtistDetailResponse | null>(null);
  const [profile, setProfile] = useState<ArtistProfile | null>(null);
  const [profileEnriching, setProfileEnriching] = useState(false);
  const [fallbackSimilar, setFallbackSimilar] = useState<SimilarArtist[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [summary, setSummary] = useState<api.ArtistSummaryPayload | null>(null);
  const [summaryLoading, setSummaryLoading] = useState(false);
  const [concertsLoading, setConcertsLoading] = useState(false);
  const [concertsError, setConcertsError] = useState<string | null>(null);
  const [concerts, setConcerts] = useState<api.ArtistConcertEvent[]>([]);
  const [concertsMeta, setConcertsMeta] = useState<{ provider: string; updated_at: number; source_url?: string | null } | null>(null);
  const [factsLoading, setFactsLoading] = useState(false);
  const [facts, setFacts] = useState<api.ArtistFactsResponse | null>(null);
  const [artistLiked, setArtistLiked] = useState(false);
  const [descExpanded, setDescExpanded] = useState(false);
  const [refreshAllBusy, setRefreshAllBusy] = useState(false);
  const [concertFilter, setConcertFilter] = useState<{ enabled: boolean; lat: number | null; lon: number | null; radiusKm: number } | null>(null);
  const includeUnmatchedParam = useMemo(() => {
    const raw = new URLSearchParams(location.search || '').get('include_unmatched');
    if (raw == null) return includeUnmatchedContext ? '1' : '0';
    const low = String(raw || '').trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(low)) return '1';
    if (['0', 'false', 'no', 'off'].includes(low)) return '0';
    return includeUnmatchedContext ? '1' : '0';
  }, [includeUnmatchedContext, location.search]);
  const appendIncludeUnmatched = useCallback((url: string) => {
    return `${url}${url.includes('?') ? '&' : '?'}include_unmatched=${includeUnmatchedParam}`;
  }, [includeUnmatchedParam]);

  const fetchArtist = useCallback(async () => {
    if (!Number.isFinite(artistId) || artistId <= 0) {
      setError('Invalid artist id');
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(appendIncludeUnmatched(`/api/library/artist/${artistId}`));
      if (!res.ok) throw new Error('Failed to load artist');
      const data = (await res.json()) as ArtistDetailResponse;
      setDetails(data);
      setProfile(data.artist_profile ?? null);
      setProfileEnriching(Boolean(data.profile_enriching));
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load artist');
    } finally {
      setLoading(false);
    }
  }, [appendIncludeUnmatched, artistId]);

  const fetchProfile = useCallback(
    async (refresh: boolean) => {
      if (!Number.isFinite(artistId) || artistId <= 0) return false;
      try {
        const res = await fetch(appendIncludeUnmatched(`/api/library/artist/${artistId}/profile${refresh ? '?refresh=1' : ''}`));
        if (!res.ok) return false;
        const data = (await res.json()) as ArtistProfileResponse;
        if (data.profile) {
          setProfile(data.profile);
        }
        setProfileEnriching(Boolean(data.enriching));
        return Boolean(data.enriching);
      } catch {
        return false;
      }
    },
    [appendIncludeUnmatched, artistId]
  );

  useEffect(() => {
    fetchArtist();
  }, [fetchArtist]);

  const loadSummary = useCallback(async () => {
    if (!Number.isFinite(artistId) || artistId <= 0) return;
    setSummaryLoading(true);
    try {
      const res = await api.getArtistSummary(artistId);
      setSummary(res);
    } catch {
      setSummary(null);
    } finally {
      setSummaryLoading(false);
    }
  }, [artistId]);

  useEffect(() => {
    void loadSummary();
  }, [loadSummary]);

  const loadConcerts = useCallback(
    async (refresh: boolean) => {
      if (!Number.isFinite(artistId) || artistId <= 0) return;
      setConcertsLoading(true);
      setConcertsError(null);
      try {
        const res = await api.getArtistConcerts(artistId, { refresh });
        setConcerts(Array.isArray(res.events) ? res.events : []);
        setConcertsMeta({ provider: res.provider || 'bandsintown', updated_at: res.updated_at || 0, source_url: res.source_url ?? null });
      } catch (e) {
        setConcerts([]);
        setConcertsMeta(null);
        setConcertsError(e instanceof Error ? e.message : 'Failed to load concerts');
      } finally {
        setConcertsLoading(false);
      }
    },
    [artistId]
  );

  const loadFacts = useCallback(async () => {
    if (!Number.isFinite(artistId) || artistId <= 0) return;
    setFactsLoading(true);
    try {
      const res = await api.getArtistFacts(artistId);
      setFacts(res);
    } catch {
      setFacts(null);
    } finally {
      setFactsLoading(false);
    }
  }, [artistId]);

  const extractFacts = useCallback(async () => {
    if (!Number.isFinite(artistId) || artistId <= 0) return;
    setFactsLoading(true);
    try {
      const res = await api.extractArtistFacts(artistId);
      setFacts(res);
      toast({ title: 'Connections updated', description: 'Updated artist connections.' });
    } catch (e) {
      toast({
        title: 'Connections refresh failed',
        description: e instanceof Error ? e.message : 'Failed to refresh connections',
        variant: 'destructive',
      });
    } finally {
      setFactsLoading(false);
    }
  }, [artistId, toast]);

  useEffect(() => {
    void loadConcerts(false);
    void loadFacts();
  }, [loadConcerts, loadFacts]);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      try {
        const cfg = await api.getConfig();
        if (cancelled) return;
        const enabled = Boolean(cfg.CONCERTS_FILTER_ENABLED);
        const lat = cfg.CONCERTS_HOME_LAT != null ? Number(String(cfg.CONCERTS_HOME_LAT).trim()) : NaN;
        const lon = cfg.CONCERTS_HOME_LON != null ? Number(String(cfg.CONCERTS_HOME_LON).trim()) : NaN;
        const radius = cfg.CONCERTS_RADIUS_KM != null ? Number(String(cfg.CONCERTS_RADIUS_KM).trim()) : NaN;
        setConcertFilter({
          enabled,
          lat: Number.isFinite(lat) ? lat : null,
          lon: Number.isFinite(lon) ? lon : null,
          radiusKm: Number.isFinite(radius) && radius > 0 ? radius : 150,
        });
      } catch {
        if (cancelled) return;
        setConcertFilter(null);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    // Auto-extract connections once when we have a description but no extracted facts yet.
    if (factsLoading) return;
    if (autoFactsRequestedRef.current) return;
    const obj = facts?.facts;
    const totalKnown = obj && typeof obj === 'object' ? Object.values(obj as Record<string, unknown>).filter((v) => Array.isArray(v) && v.length > 0).length : 0;
    if (totalKnown > 0) return;
    const anyDesc = ((summary?.original?.text || '') + (summary?.ai?.text || '')).trim();
    if (!anyDesc) return;
    autoFactsRequestedRef.current = true;
    void extractFacts();
  }, [facts, factsLoading, summary, extractFacts]);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      if (!Number.isFinite(artistId) || artistId <= 0) return;
      try {
        const res = await api.getLikes('artist', [artistId]);
        const liked = (res.items || []).some((it) => it.entity_id === artistId && Boolean(it.liked));
        if (!cancelled) setArtistLiked(liked);
      } catch {
        if (!cancelled) setArtistLiked(false);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [artistId]);

  const toggleArtistLike = useCallback(async () => {
    const next = !artistLiked;
    setArtistLiked(next);
    try {
      await api.setLike({ entity_type: 'artist', entity_id: artistId, liked: next, source: 'ui_artist' });
      toast({ title: next ? 'Liked' : 'Unliked', description: next ? 'Artist saved to favorites.' : 'Artist removed from favorites.' });
    } catch (e) {
      setArtistLiked(!next);
      toast({ title: 'Like failed', description: e instanceof Error ? e.message : 'Failed to update like', variant: 'destructive' });
    }
  }, [artistLiked, artistId, toast]);

  const ensureDescription = useCallback(async () => {
    if (!Number.isFinite(artistId) || artistId <= 0) return;
    try {
      setSummaryLoading(true);
      const lang = (navigator.language || '').toLowerCase().startsWith('fr') ? 'fr' : 'en';
      await api.generateArtistAiSummary(artistId, lang);
      await loadSummary();
    } catch {
      // Best-effort: if AI is not configured, we still show provider descriptions.
    } finally {
      setSummaryLoading(false);
    }
  }, [artistId, loadSummary]);

  const openArtistByName = useCallback(async (name: string) => {
    const q = (name || '').trim();
    if (!q) return;
    try {
      const res = await api.getLibrarySearchSuggest(q, 12);
      const items = Array.isArray(res.items) ? res.items : [];
      const norm = q.toLowerCase();
      const exact = items.find((it) => it.type === 'artist' && (it.title || '').trim().toLowerCase() === norm);
      const best = exact || items.find((it) => it.type === 'artist');
      if (best?.artist_id && best.artist_id > 0) {
        navigate(`/library/artist/${best.artist_id}${location.search || ''}`);
        return;
      }
      toast({ title: 'Not found', description: `No artist matched "${q}".`, variant: 'destructive' });
    } catch {
      toast({ title: 'Search failed', description: `Could not resolve "${q}".`, variant: 'destructive' });
    }
  }, [location.search, navigate, toast]);

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
    } catch (error) {
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to load tracks',
        variant: 'destructive',
      });
    }
  }, [setCurrentTrack, startPlayback, toast]);

  const refreshArtistAnalysis = useCallback(async () => {
    if (!Number.isFinite(artistId) || artistId <= 0) return;
    if (refreshAllBusy) return;
    setRefreshAllBusy(true);
    setDescExpanded(false);
    autoAiRequestedRef.current = false;
    autoFactsRequestedRef.current = false;
    try {
      await Promise.allSettled([
        fetchArtist(),
        fetchProfile(true),
        loadConcerts(true),
      ]);

      const res = await api.getArtistSummary(artistId);
      setSummary(res);
      const orig = (res.original?.text || '').trim();
      const ai = (res.ai?.text || '').trim();
      if (orig === '' && ai === '') {
        await ensureDescription();
      }

      // Always attempt to refresh connections; backend will no-op if AI isn't configured.
      await extractFacts();
      await loadFacts();

      toast({ title: 'Refreshed', description: 'Artist analysis updated.' });
    } catch (e) {
      toast({ title: 'Refresh failed', description: e instanceof Error ? e.message : 'Failed to refresh artist', variant: 'destructive' });
    } finally {
      setRefreshAllBusy(false);
    }
  }, [artistId, refreshAllBusy, fetchArtist, fetchProfile, loadConcerts, ensureDescription, extractFacts, loadFacts, toast]);

  useEffect(() => {
    // Auto-generate a description only when nothing exists yet.
    if (summaryLoading) return;
    if (!summary) return;
    if (autoAiRequestedRef.current) return;
    const orig = (summary.original?.text || '').trim();
    const ai = (summary.ai?.text || '').trim();
    if (orig === '' && ai === '') {
      autoAiRequestedRef.current = true;
      void ensureDescription();
    }
  }, [summary, summaryLoading, ensureDescription]);

  const formatUpdated = (ts?: number) => {
    if (!ts) return '—';
    try {
      return new Date(ts * 1000).toLocaleDateString();
    } catch {
      return '—';
    }
  };

  useEffect(() => {
    if (!details) return;
    let cancelled = false;
    let attempts = 0;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const run = async () => {
      attempts += 1;
      const refreshing = attempts === 1;
      const stillEnriching = await fetchProfile(refreshing);
      if (!cancelled && stillEnriching && attempts < 12) {
        timer = setTimeout(run, 1500);
      }
    };
    run();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [details, fetchProfile]);

  useEffect(() => {
    if (!details) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    similarRefreshAttemptsRef.current = 0;
    const run = async () => {
      try {
        const res = await fetch(appendIncludeUnmatched(`/api/library/artist/${details.artist_id}/similar`));
        if (!res.ok) return;
        const data = await res.json();
        if (cancelled) return;
        const list = Array.isArray(data?.similar_artists) ? data.similar_artists : [];
        const normalized = list
            .map((x: unknown) => {
              if (!x || typeof x !== 'object') {
                return { name: '' };
              }
              const obj = x as Record<string, unknown>;
              const name = typeof obj.name === 'string' ? obj.name : String(obj.name ?? '');
              const rawId = obj.artist_id;
              const artist_id =
                typeof rawId === 'number'
                  ? rawId
                  : typeof rawId === 'string'
                    ? Number(rawId)
                    : undefined;
              const image_url = typeof obj.image_url === 'string' ? obj.image_url : undefined;
              return {
                name,
                mbid: typeof obj.mbid === 'string' ? obj.mbid : undefined,
                type: typeof obj.type === 'string' ? obj.type : undefined,
                artist_id: Number.isFinite(artist_id as number) && Number(artist_id) > 0 ? Number(artist_id) : undefined,
                image_url: (image_url || '').trim() || undefined,
              };
            })
            .filter((entry) => entry.name.length > 0)
        setFallbackSimilar(normalized);

        // If some images are missing, the backend may be warming external caches asynchronously.
        // Re-fetch a couple times so the grid upgrades itself without a manual refresh.
        if (similarRefreshAttemptsRef.current < 3) {
          const missingCount = normalized.filter((a) => !a.image_url || isProbablyPlaceholderArtistImageUrl(a.image_url)).length;
          if (missingCount > 0) {
            similarRefreshAttemptsRef.current += 1;
            timer = setTimeout(run, 1800);
          }
        }
      } catch {
        // no-op
      }
    };
    run();
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [appendIncludeUnmatched, details]);

  const grouped = useMemo(() => {
    const src = details?.albums ?? [];
    const map: Record<string, AlbumInfo[]> = {};
    for (const album of src) {
      const t = album.type || 'Album';
      if (!map[t]) map[t] = [];
      map[t].push(album);
    }
    return map;
  }, [details?.albums]);

  const sortedTypes = useMemo(() => {
    return Object.keys(grouped).sort((a, b) => {
      const ai = albumTypeOrder.indexOf(a);
      const bi = albumTypeOrder.indexOf(b);
      if (ai === -1 && bi === -1) return a.localeCompare(b);
      if (ai === -1) return 1;
      if (bi === -1) return -1;
      return ai - bi;
    });
  }, [grouped]);

  // Hooks below must stay above any early-return to preserve hook order.
  const concertHome = useMemo(() => {
    if (!concertFilter?.enabled) return null;
    if (concertFilter.lat == null || concertFilter.lon == null) return null;
    return { lat: concertFilter.lat, lon: concertFilter.lon, radiusKm: concertFilter.radiusKm };
  }, [concertFilter?.enabled, concertFilter?.lat, concertFilter?.lon, concertFilter?.radiusKm]);

  const concertList = useMemo(() => {
    if (!concertHome) return { events: concerts, hiddenNoCoords: 0 };
    const origin = { lat: concertHome.lat, lon: concertHome.lon };
    const radiusKm = Math.max(1, concertHome.radiusKm || 150);
    let hiddenNoCoords = 0;
    const inside: api.ArtistConcertEvent[] = [];
    for (const ev of concerts) {
      const v = ev.venue;
      const lat = toCoord(v?.latitude);
      const lon = toCoord(v?.longitude);
      if (lat == null || lon == null) {
        hiddenNoCoords += 1;
        continue;
      }
      const d = haversineKm(origin, { lat, lon });
      if (d <= radiusKm) inside.push(ev);
    }
    return { events: inside, hiddenNoCoords };
  }, [concerts, concertHome]);

  if (loading) {
    return (
      <div className="container py-8">
        <div className="flex items-center justify-center py-24">
          <Loader2 className="w-10 h-10 animate-spin text-primary" />
        </div>
      </div>
    );
  }

  if (error || !details) {
    return (
      <div className="container py-8">
        <Card>
          <CardContent className="p-8 space-y-4 text-center">
            <p className="text-muted-foreground">{error || 'Artist not found'}</p>
            <Button variant="outline" onClick={() => navigate(`/library${location.search || ''}`)}>
              Back to Library
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  const tags = (profile?.tags || []).slice(0, 8);
  const similar = ((fallbackSimilar && fallbackSimilar.length > 0) ? fallbackSimilar : (profile?.similar_artists || [])).slice(0, 12);
  const heroImage = details.artist_thumb || null;
  const originalText = (summary?.original?.text || '').trim();
  const aiText = (summary?.ai?.text || '').trim();
  const chosenDescription = (() => {
    const origOk = isAcceptableOriginal(originalText);
    if (origOk && originalText) {
      return { text: originalText, source: summary?.original?.source || '', updated_at: summary?.original?.updated_at || 0 };
    }
    if (aiText) {
      return { text: aiText, source: summary?.ai?.source || '', updated_at: summary?.ai?.updated_at || 0 };
    }
    if (originalText) {
      return { text: originalText, source: summary?.original?.source || '', updated_at: summary?.original?.updated_at || 0 };
    }
    return { text: '', source: '', updated_at: 0 };
  })();
  const displayText = chosenDescription.text;
  const displaySource = chosenDescription.source;
  const displayUpdated = chosenDescription.updated_at;

  const getFactsArray = (key: string): string[] => {
    const obj = facts?.facts;
    if (!obj || typeof obj !== 'object') return [];
    const val = (obj as Record<string, unknown>)[key];
    if (!Array.isArray(val)) return [];
    return val.map((x) => String(x || '').trim()).filter((x) => x.length > 0).slice(0, 16);
  };
  const factsAka = getFactsArray('aka');
  const factsAliases = getFactsArray('aliases');
  const factsGroups = getFactsArray('member_of');
  const factsCollabs = getFactsArray('collaborated_with');
  const factsLabels = getFactsArray('labels');
  const factsCities = getFactsArray('notable_cities');

  const formatConcertDate = (dt?: string) => {
    const raw = (dt || '').trim();
    if (!raw) return '';
    try {
      const d = new Date(raw);
      if (Number.isNaN(d.getTime())) return raw;
      return d.toLocaleDateString(undefined, { weekday: 'short', year: 'numeric', month: 'short', day: 'numeric' });
    } catch {
      return raw;
    }
  };

  return (
    <div className="container py-6 space-y-6">
        <div className="flex items-center justify-between gap-3">
          <Button variant="ghost" className="gap-2" onClick={() => navigate(`/library${location.search || ''}`)}>
            <ArrowLeft className="w-4 h-4" />
            Back to Library
          </Button>
          {profileEnriching && (
            <Badge variant="outline" className="gap-1.5">
              <Loader2 className="w-3 h-3 animate-spin" />
              Enriching profile
            </Badge>
          )}
        </div>

        <Card className="overflow-hidden border-border/70">
          <div className="relative">
            <div className="absolute inset-0 bg-gradient-to-r from-background via-background/90 to-background/70 z-10" />
            {heroImage ? (
              <img src={heroImage} alt={details.artist_name} className="w-full h-64 object-cover blur-[1px] scale-105" />
            ) : (
              <div className="h-64 bg-gradient-to-br from-zinc-900 via-zinc-800 to-zinc-900" />
            )}
            <div className="absolute inset-0 z-20 p-6 md:p-8 flex items-end">
              <div className="grid grid-cols-1 md:grid-cols-[8.5rem,1fr] gap-5 w-full items-end">
                <div className="w-28 h-28 md:w-36 md:h-36 rounded-3xl overflow-hidden border border-border/60 bg-muted shrink-0 shadow-sm">
                  {heroImage ? (
                    <img src={heroImage} alt={details.artist_name} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center">
                      <Music className="w-8 h-8 text-muted-foreground" />
                    </div>
                  )}
                </div>
                <div className="min-w-0">
                  <h1 className="text-3xl md:text-4xl font-bold tracking-tight truncate">{details.artist_name}</h1>
                  <p className="text-sm text-muted-foreground mt-1">
                    {details.total_albums.toLocaleString()} album{details.total_albums !== 1 ? 's' : ''}
                  </p>
                  {tags.length > 0 && (
                    <div className="flex flex-wrap gap-1.5 mt-3">
                      {tags.map((tag) => (
                        <Badge key={tag} variant="secondary" className="text-[11px]">
                          {tag}
                        </Badge>
                      ))}
                    </div>
                  )}
                  <div className="flex flex-wrap items-center gap-2 mt-4">
                    <Button
                      type="button"
                      size="sm"
                      variant={artistLiked ? 'default' : 'outline'}
                      className="h-8 gap-2"
                      onClick={() => void toggleArtistLike()}
                      title={artistLiked ? 'Unlike artist' : 'Like artist'}
                    >
                      <Heart className={cn('h-4 w-4', artistLiked ? 'fill-current' : '')} />
                      {artistLiked ? 'Liked' : 'Like'}
                    </Button>
                    {displaySource ? (
                      <Badge variant="outline" className="text-[10px]">
                        Source: {displaySource}
                      </Badge>
                    ) : null}
                    <Badge variant="outline" className="text-[10px]">
                      Updated: {formatUpdated(displayUpdated)}
                    </Badge>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-8 gap-2"
                      onClick={() => void refreshArtistAnalysis()}
                      disabled={refreshAllBusy}
                      title="Refresh artist analysis"
                    >
                      {refreshAllBusy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
                      Refresh
                    </Button>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <CardContent className="pt-4 pb-5 space-y-4">
            {displayText ? (
              <Collapsible open={descExpanded} onOpenChange={setDescExpanded}>
                {!descExpanded ? (
                  <p className="text-sm leading-relaxed text-muted-foreground whitespace-pre-wrap line-clamp-5">
                    {displayText}
                  </p>
                ) : null}
                <CollapsibleContent>
                  <p className="text-sm leading-relaxed text-muted-foreground whitespace-pre-wrap">
                    {displayText}
                  </p>
                </CollapsibleContent>
                {displayText.length > 420 ? (
                  <CollapsibleTrigger asChild>
                    <Button type="button" variant="ghost" size="sm" className="gap-1.5 px-0">
                      {descExpanded ? (
                        <>
                          <ChevronUp className="h-4 w-4" />
                          Show less
                        </>
                      ) : (
                        <>
                          <ChevronDown className="h-4 w-4" />
                          Show more
                        </>
                      )}
                    </Button>
                  </CollapsibleTrigger>
                ) : null}
              </Collapsible>
            ) : (
              <p className="text-sm leading-relaxed text-muted-foreground">
                No artist description available yet.
              </p>
            )}
          </CardContent>
        </Card>

        {/* Concerts */}
        <Card className="border-border/70">
          <CardContent className="p-5 space-y-4">
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <div className="flex items-center gap-2">
                  <Calendar className="w-4 h-4 text-primary" />
                  <p className="font-medium">Upcoming Concerts</p>
                  {concertsMeta?.provider ? (
                    <Badge variant="outline" className="text-[10px]">
                      {concertsMeta.provider}
                    </Badge>
                  ) : null}
                </div>
                <p className="text-xs text-muted-foreground">
                  {concertsMeta?.updated_at ? `Updated ${formatUpdated(concertsMeta.updated_at)}.` : ' '}
                  {concertsMeta?.source_url ? (
                    <a href={concertsMeta.source_url} target="_blank" rel="noreferrer" className="underline underline-offset-2">
                      Provider page
                    </a>
                  ) : null}
                  {concertHome ? (
                    <>
                      {' '}
                      <span className="text-muted-foreground">
                        Filter: {Math.round(concertHome.radiusKm)} km
                      </span>
                      {' '}
                      <button
                        type="button"
                        className="underline underline-offset-2"
                        onClick={() => navigate('/settings#settings-concerts')}
                      >
                        Settings
                      </button>
                    </>
                  ) : null}
                </p>
              </div>
            </div>

            {concertList.events.length > 0 ? (
              <ConcertsMiniMap
                events={concertList.events}
                home={concertHome ? { lat: concertHome.lat, lon: concertHome.lon, radiusKm: concertHome.radiusKm } : null}
              />
            ) : null}

            {concertHome && concertList.hiddenNoCoords > 0 ? (
              <p className="text-xs text-muted-foreground">
                {concertList.hiddenNoCoords} event{concertList.hiddenNoCoords !== 1 ? 's' : ''} could not be located and were hidden by the radius filter.
              </p>
            ) : null}

            {concertsLoading && concerts.length === 0 ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                Loading concerts…
              </div>
            ) : concertsError ? (
              <p className="text-sm text-destructive">{concertsError}</p>
            ) : concertList.events.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                {concertHome ? 'No upcoming concerts found within your radius.' : 'No upcoming concerts found.'}
              </p>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                {concertList.events.slice(0, 8).map((ev, idx) => {
                  const venue = ev.venue || { name: '', city: '' };
                  const where = [venue.city, venue.region, venue.country].filter(Boolean).join(', ');
                  return (
                    <div key={`${ev.id || idx}`} className="rounded-xl border border-border/60 bg-card p-4 flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className="text-sm font-medium truncate">{formatConcertDate(ev.datetime) || 'TBA'}</div>
                        <div className="text-xs text-muted-foreground mt-1 truncate">
                          {venue.name ? venue.name : 'Venue TBA'}
                          {where ? ` · ${where}` : ''}
                        </div>
                      </div>
                      {ev.url ? (
                        <a href={ev.url} target="_blank" rel="noreferrer">
                          <Button variant="outline" size="sm" className="gap-1.5">
                            Tickets
                            <ExternalLink className="h-3.5 w-3.5" />
                          </Button>
                        </a>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Connections / Facts */}
        <Card className="border-border/70">
          <CardContent className="p-5 space-y-4">
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <div className="flex items-center gap-2">
                  <Users className="w-4 h-4 text-primary" />
                  <p className="font-medium">Connections</p>
                  {factsLoading ? (
                    <Badge variant="outline" className="gap-1.5 text-[10px]">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      Updating
                    </Badge>
                  ) : null}
                </div>
                <p className="text-xs text-muted-foreground">
                  {facts?.updated_at ? `Updated ${formatUpdated(facts.updated_at)}.` : ' '}
                </p>
              </div>
            </div>

            {(factsAka.length + factsAliases.length + factsGroups.length + factsCollabs.length + factsLabels.length + factsCities.length) === 0 ? (
              <p className="text-sm text-muted-foreground">
                No connections found yet. Use Refresh to analyze the artist and populate labels, collaborations, and related acts.
              </p>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {factsAka.length > 0 ? (
                  <div className="space-y-2">
                    <div className="text-xs font-medium text-muted-foreground">AKA</div>
                    <div className="flex flex-wrap gap-1.5">
                      {factsAka.map((x) => (
                        <Badge key={`aka-${x}`} variant="secondary" className="text-[11px]">
                          {x}
                        </Badge>
                      ))}
                    </div>
                  </div>
                ) : null}
                {factsAliases.length > 0 ? (
                  <div className="space-y-2">
                    <div className="text-xs font-medium text-muted-foreground">Aliases</div>
                    <div className="flex flex-wrap gap-1.5">
                      {factsAliases.map((x) => (
                        <Badge key={`alias-${x}`} variant="secondary" className="text-[11px]">
                          {x}
                        </Badge>
                      ))}
                    </div>
                  </div>
                ) : null}
                {factsGroups.length > 0 ? (
                  <div className="space-y-2">
                    <div className="text-xs font-medium text-muted-foreground">Groups</div>
                    <div className="flex flex-wrap gap-1.5">
                      {factsGroups.map((x) => (
                        <Button
                          key={`grp-${x}`}
                          type="button"
                          size="sm"
                          variant="secondary"
                          className="h-6 px-2 text-[11px]"
                          onClick={() => void openArtistByName(x)}
                          title="Open artist"
                        >
                          {x}
                        </Button>
                      ))}
                    </div>
                  </div>
                ) : null}
                {factsCollabs.length > 0 ? (
                  <div className="space-y-2">
                    <div className="text-xs font-medium text-muted-foreground">Collaborations</div>
                    <div className="flex flex-wrap gap-1.5">
                      {factsCollabs.map((x) => (
                        <Button
                          key={`col-${x}`}
                          type="button"
                          size="sm"
                          variant="secondary"
                          className="h-6 px-2 text-[11px]"
                          onClick={() => void openArtistByName(x)}
                          title="Open artist"
                        >
                          {x}
                        </Button>
                      ))}
                    </div>
                  </div>
                ) : null}
                {factsLabels.length > 0 ? (
                  <div className="space-y-2">
                    <div className="text-xs font-medium text-muted-foreground">Labels</div>
                    <div className="flex flex-wrap gap-1.5">
                      {factsLabels.map((x) => (
                        <Button
                          key={`lab-${x}`}
                          type="button"
                          size="sm"
                          variant="secondary"
                          className="h-6 px-2 text-[11px]"
                          onClick={() => navigate(`/library/label/${encodeURIComponent(x)}${location.search || ''}`)}
                          title="Open label"
                        >
                          {x}
                        </Button>
                      ))}
                    </div>
                  </div>
                ) : null}
                {factsCities.length > 0 ? (
                  <div className="space-y-2">
                    <div className="text-xs font-medium text-muted-foreground">Cities</div>
                    <div className="flex flex-wrap gap-1.5">
                      {factsCities.map((x) => (
                        <Badge key={`city-${x}`} variant="secondary" className="text-[11px]">
                          {x}
                        </Badge>
                      ))}
                    </div>
                  </div>
                ) : null}
              </div>
            )}
          </CardContent>
        </Card>

        <div className="space-y-6">
          {sortedTypes.map((type) => (
            <section key={type} className="space-y-3">
              <div className="flex items-center gap-2">
                <Disc3 className="w-4 h-4 text-primary" />
                <h2 className="text-lg font-semibold">{type === 'Single' ? 'Singles' : `${type}s`}</h2>
              </div>
	              <ScrollArea className="w-full whitespace-nowrap">
	                <div className="flex gap-4 pb-2">
	                  {grouped[type].map((album) => (
                    <Card
                      key={album.album_id}
                      className={cn(
                        "group w-[200px] shrink-0 overflow-hidden border-border/70 cursor-pointer hover:bg-muted/40 transition-colors",
                        isUnmatchedAlbum(album) && 'ring-1 ring-amber-500/45 shadow-[0_0_0_1px_rgba(245,158,11,0.25),0_0_24px_rgba(245,158,11,0.14)]',
                      )}
                      role="button"
                      tabIndex={0}
                      onClick={() => navigate(`/library/album/${album.album_id}${location.search || ''}`)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter" || e.key === " ") {
                          e.preventDefault();
                          navigate(`/library/album/${album.album_id}${location.search || ''}`);
                        }
                      }}
                      title="Open album"
                    >
                      <AspectRatio
                        ratio={1}
                        className="bg-muted"
                        draggable
                        onDragStart={(e) => {
                          try {
                            e.dataTransfer.setData('application/x-pmda-album', JSON.stringify({ album_id: album.album_id }));
                            e.dataTransfer.setData('text/plain', `${details.artist_name} – ${album.title}`);
                            e.dataTransfer.effectAllowed = 'copy';
                          } catch {
                            // ignore
                          }
                        }}
                      >
                        {album.thumb ? (
                          <img src={album.thumb} alt={album.title} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                        ) : (
                          <div className="w-full h-full flex items-center justify-center">
                            <Music className="w-10 h-10 text-muted-foreground" />
                          </div>
                        )}
                        {isUnmatchedAlbum(album) ? (
                          <div className="absolute top-2 left-2">
                            <Badge variant="outline" className="text-[10px] border-amber-500/50 text-amber-700 bg-background/75 backdrop-blur dark:text-amber-300">
                              Unmatched
                            </Badge>
                          </div>
                        ) : null}
                        <div className="absolute inset-x-0 bottom-0 p-2.5 opacity-0 group-hover:opacity-100 transition-opacity">
                          <Button
                            size="sm"
                            className="h-8 w-full gap-2 rounded-full"
                            onClick={(e) => {
                              e.preventDefault();
                              e.stopPropagation();
                              void handlePlayAlbum(album.album_id, album.title, album.thumb);
                            }}
                          >
                            <Play className="h-4 w-4" />
                            Play
                          </Button>
                        </div>
                      </AspectRatio>
                      <CardContent className="p-3 space-y-2">
                        <h3 className="text-sm font-semibold truncate" title={album.title}>
                          {album.title}
                        </h3>
                        <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground">
                          <span>{album.year || album.date || 'Unknown'}</span>
                          <span>{album.track_count} tracks</span>
                        </div>
                        <div className="flex items-center gap-1.5 flex-wrap">
                          {album.format && <FormatBadge format={album.format} size="sm" />}
                          <Badge variant={album.is_lossless ? 'secondary' : 'outline'} className="text-[10px]">
                            {album.is_lossless ? 'Lossless' : 'Lossy'}
                          </Badge>
                          {isUnmatchedAlbum(album) ? (
                            <Badge variant="outline" className="text-[10px] border-amber-500/50 text-amber-700 dark:text-amber-300">
                              Verify tags
                            </Badge>
                          ) : null}
                        </div>
                        {album.short_description && (
                          <p className={cn('text-[11px] text-muted-foreground line-clamp-3')}>
                            {album.short_description}
                          </p>
                        )}
                      </CardContent>
                    </Card>
                  ))}
                </div>
              </ScrollArea>
            </section>
          ))}
        </div>

        <section className="space-y-3">
          <div className="flex items-center gap-2">
            <Sparkles className="w-4 h-4 text-primary" />
            <h2 className="text-lg font-semibold">Similar Artists</h2>
          </div>
          {similar.length === 0 ? (
            <Card>
              <CardContent className="p-5 text-sm text-muted-foreground">No similar artists available yet.</CardContent>
            </Card>
          ) : (
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-3">
              {similar.map((artist) => (
	                <Card
	                  key={`${artist.artist_id || ''}-${artist.name}-${artist.mbid || ''}`}
	                  className={cn(
	                    "border-border/70 transition-colors",
	                    "cursor-pointer hover:bg-muted/40"
	                  )}
	                  role="button"
	                  tabIndex={0}
	                  onClick={() => {
	                    if (artist.artist_id) {
	                      navigate(`/library/artist/${artist.artist_id}${location.search || ''}`);
	                      return;
	                    }
	                    const href = `https://bandcamp.com/search?q=${encodeURIComponent(artist.name || '')}`;
	                    window.open(href, '_blank', 'noopener,noreferrer');
	                  }}
	                  onKeyDown={(e) => {
	                    if (e.key === "Enter" || e.key === " ") {
	                      e.preventDefault();
	                      if (artist.artist_id) {
	                        navigate(`/library/artist/${artist.artist_id}${location.search || ''}`);
	                      } else {
	                        const href = `https://bandcamp.com/search?q=${encodeURIComponent(artist.name || '')}`;
	                        window.open(href, '_blank', 'noopener,noreferrer');
	                      }
	                    }
	                  }}
	                >
	                  <CardContent className="p-3 space-y-2">
	                    <div className="relative w-12 h-12 rounded-full bg-muted mx-auto overflow-hidden flex items-center justify-center border border-border/60">
	                      {artist.image_url && !isProbablyPlaceholderArtistImageUrl(artist.image_url) ? (
	                        <img
	                          src={artist.image_url}
	                          alt={artist.name}
	                          className="w-full h-full object-cover animate-in fade-in-0 duration-300"
	                          loading="lazy"
	                        />
	                      ) : (
	                        <div className="w-full h-full flex items-center justify-center bg-gradient-to-br from-amber-500/30 via-slate-500/10 to-emerald-500/30 text-[11px] font-semibold text-foreground/80">
	                          {initialsFromName(artist.name)}
	                        </div>
	                      )}
	                      {!artist.artist_id ? (
	                        <div className="absolute -right-1 -bottom-1 w-5 h-5 rounded-full bg-background border border-border/60 flex items-center justify-center">
	                          <ExternalLink className="w-3 h-3 text-muted-foreground" />
	                        </div>
	                      ) : null}
	                    </div>
	                    <p className="text-xs font-medium text-center line-clamp-2 min-h-[2.2rem]">{artist.name}</p>
	                    {artist.type ? (
	                      <p className="text-[10px] text-muted-foreground text-center truncate">{artist.type}</p>
	                    ) : null}
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
        </section>
    </div>
  );
}
