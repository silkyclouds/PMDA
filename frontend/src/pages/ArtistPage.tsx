import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, Calendar, ChevronDown, ChevronUp, Disc3, ExternalLink, Heart, Info, Loader2, Music, Play, RefreshCw, Sparkles, Users } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent } from '@/components/ui/card';
import { AspectRatio } from '@/components/ui/aspect-ratio';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Carousel, CarouselContent, CarouselItem, CarouselNext, CarouselPrevious } from '@/components/ui/carousel';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { AlbumArtwork } from '@/components/library/AlbumArtwork';
import { AlbumBadgeGroups } from '@/components/library/AlbumBadgeGroups';
import { EntityDiscoverDialog } from '@/components/library/EntityDiscoverDialog';
import { MatchDetailDialog } from '@/components/library/MatchDetailDialog';
import { ProviderBadge } from '@/components/providers/ProviderBadge';
import { ShareDialog } from '@/components/social/ShareDialog';
import { useAlbumBadgesVisibility } from '@/hooks/use-album-badges';
import { cn } from '@/lib/utils';
import { badgeKindClass } from '@/lib/badgeStyles';
import { resolveBackLink, withBackLinkState } from '@/lib/backNavigation';
import { formatBadgeDateTime } from '@/lib/dateFormat';
import * as api from '@/lib/api';
import { useToast } from '@/hooks/use-toast';
import { ConcertsMiniMap } from '@/components/concerts/ConcertsMiniMap';
import { usePlayback } from '@/contexts/PlaybackContext';
import type { TrackInfo } from '@/components/library/AudioPlayer';
import { useAuth } from '@/contexts/AuthContext';

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
  genre?: string | null;
  genres?: string[] | null;
  label?: string | null;
  track_count: number;
  type: string;
  thumb?: string;
  format?: string;
  is_lossless?: boolean;
  mb_identified?: boolean;
  short_description?: string;
  user_rating?: number | null;
  public_rating?: number | null;
  public_rating_votes?: number | null;
  public_rating_source?: string | null;
  heat_score?: number | null;
}

interface ArtistDetailResponse {
  artist_id: number;
  artist_name: string;
  created_at?: number | null;
  updated_at?: number | null;
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
  const { startPlayback, setCurrentTrack } = usePlayback();
  const { canUseAI } = useAuth();
  const { showBadges, setShowBadges } = useAlbumBadgesVisibility();
  const params = useParams<{ artistId: string }>();
  const artistId = Number(params.artistId);
  const { toast } = useToast();
  const autoAiRequestedRef = useRef(false);
  const similarRefreshAttemptsRef = useRef(0);

  const [loading, setLoading] = useState(true);
  const [details, setDetails] = useState<ArtistDetailResponse | null>(null);
  const [profile, setProfile] = useState<ArtistProfile | null>(null);
  const [profileEnriching, setProfileEnriching] = useState(false);
  const [profileEnrichState, setProfileEnrichState] = useState<'idle' | 'running' | 'done' | 'timeout'>('idle');
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
  const [factsLoadedOnce, setFactsLoadedOnce] = useState(false);
  const [connectionDetailsOpen, setConnectionDetailsOpen] = useState(false);
  const [artistLiked, setArtistLiked] = useState(false);
  const [descExpanded, setDescExpanded] = useState(false);
  const [refreshAllBusy, setRefreshAllBusy] = useState(false);
  const [aiProcessBusy, setAiProcessBusy] = useState(false);
  const [concertFilter, setConcertFilter] = useState<{ enabled: boolean; lat: number | null; lon: number | null; radiusKm: number } | null>(null);
  const [showConcertsModal, setShowConcertsModal] = useState(false);
  const [matchDialogOpen, setMatchDialogOpen] = useState(false);
  const includeUnmatchedParam = '1';
  const appendIncludeUnmatched = useCallback((url: string) => {
    return `${url}${url.includes('?') ? '&' : '?'}include_unmatched=${includeUnmatchedParam}`;
  }, [includeUnmatchedParam]);
  const apiErrorText = useCallback((value: unknown): string => {
    const bodyMsg = (value as { body?: { message?: unknown } } | null)?.body?.message;
    if (typeof bodyMsg === 'string' && bodyMsg.trim()) return bodyMsg.trim();
    if (value instanceof Error && value.message.trim()) return value.message.trim();
    const txt = String(value ?? '').trim();
    return txt || 'unknown error';
  }, []);

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
      const enriching = Boolean(data.profile_enriching);
      setProfileEnriching(enriching);
      setProfileEnrichState(enriching ? 'running' : 'done');
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
        const enriching = Boolean(data.enriching);
        setProfileEnriching(enriching);
        setProfileEnrichState(enriching ? 'running' : 'done');
        return enriching;
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
      setFactsLoadedOnce(true);
    }
  }, [artistId]);

  useEffect(() => {
    void loadConcerts(false);
  }, [loadConcerts]);

  useEffect(() => {
    setFacts(null);
    setFactsLoadedOnce(false);
    setConnectionDetailsOpen(false);
  }, [artistId]);

  useEffect(() => {
    if (!connectionDetailsOpen) return;
    if (factsLoadedOnce || factsLoading) return;
    void loadFacts();
  }, [connectionDetailsOpen, factsLoadedOnce, factsLoading, loadFacts]);

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
      await api.generateArtistAiSummary(artistId, 'en');
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
        navigate(`/library/artist/${best.artist_id}${location.search || ''}`, { state: withBackLinkState(location) });
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

      // Keep page refresh cheap: only refresh facts when connection details are open.
      if (connectionDetailsOpen) {
        await loadFacts();
      }

      toast({ title: 'Refreshed', description: 'Artist analysis updated.' });
    } catch (e) {
      toast({ title: 'Refresh failed', description: e instanceof Error ? e.message : 'Failed to refresh artist', variant: 'destructive' });
    } finally {
      setRefreshAllBusy(false);
    }
  }, [artistId, refreshAllBusy, fetchArtist, fetchProfile, loadConcerts, ensureDescription, connectionDetailsOpen, loadFacts, toast]);

  const runArtistAiResearch = useCallback(async () => {
    if (!Number.isFinite(artistId) || artistId <= 0) return;
    if (aiProcessBusy) return;
    setAiProcessBusy(true);
    try {
      let started = false;
      let profileRefreshFailed = false;
      let summaryFailed = false;
      let factsFailed = false;
      try {
        const res = await api.enrichArtistWithAI(artistId);
        started = Boolean(res.started);
      } catch {
        started = false;
        profileRefreshFailed = true;
      }

      const shouldRefreshConnections = Boolean(connectionDetailsOpen);
      const stepTasks: Promise<unknown>[] = [api.generateArtistAiSummary(artistId, 'en')];
      if (shouldRefreshConnections) {
        // Keep artist page opening cheap: only refresh AI connections on-demand.
        stepTasks.push(api.extractArtistFacts(artistId));
      }
      const stepResults = await Promise.allSettled(stepTasks);
      summaryFailed = stepResults[0]?.status === 'rejected';
      factsFailed = shouldRefreshConnections && stepResults[1]?.status === 'rejected';
      const summaryErr = stepResults[0]?.status === 'rejected' ? apiErrorText(stepResults[0].reason) : '';
      const factsErr = (shouldRefreshConnections && stepResults[1]?.status === 'rejected')
        ? apiErrorText(stepResults[1].reason)
        : '';

      const targets = (details?.albums || [])
        .map((a) => Number(a.album_id || 0))
        .filter((id) => Number.isFinite(id) && id > 0);
      let generated = 0;
      let failed = 0;
      if (targets.length > 0) {
        let idx = 0;
        const workers = Array.from({ length: Math.min(3, targets.length) }, () => (async () => {
          while (idx < targets.length) {
            const current = targets[idx++];
            try {
              await api.generateAlbumReview(current);
              generated += 1;
            } catch {
              failed += 1;
            }
          }
        })());
        await Promise.all(workers);
      }

      const postTasks: Promise<unknown>[] = [
        fetchArtist(),
        fetchProfile(false),
        loadSummary(),
      ];
      if (connectionDetailsOpen) {
        postTasks.push(loadFacts());
      }
      await Promise.allSettled(postTasks);

      const hasFailure = profileRefreshFailed || summaryFailed || factsFailed || failed > 0;
      const detailParts = [
        summaryErr ? `summary: ${summaryErr}` : '',
        factsErr ? `connections: ${factsErr}` : '',
      ].filter((x) => x.length > 0);
      toast({
        title: hasFailure ? 'AI research partially completed' : 'AI research completed',
        description: `Profiles ${started ? 'queued' : 'not queued'} · album reviews ${generated}${failed > 0 ? ` (${failed} failed)` : ''}${summaryFailed ? ' · summary failed' : ''}${factsFailed ? ' · connections failed' : ''}${!shouldRefreshConnections ? ' · connections skipped (on-demand)' : ''}${detailParts.length ? ` · ${detailParts.join(' | ')}` : ''}.`,
        variant: hasFailure ? 'destructive' : 'default',
      });
    } catch (e) {
      toast({
        title: 'AI research failed',
        description: e instanceof Error ? e.message : 'Failed to run AI artist processing',
        variant: 'destructive',
      });
    } finally {
      setAiProcessBusy(false);
    }
  }, [aiProcessBusy, apiErrorText, artistId, connectionDetailsOpen, details?.albums, fetchArtist, fetchProfile, loadFacts, loadSummary, toast]);

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

  useEffect(() => {
    if (!details || !profileEnriching) return;
    let cancelled = false;
    let attempts = 0;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const run = async () => {
      attempts += 1;
      const stillEnriching = await fetchProfile(false);
      if (!cancelled && stillEnriching && attempts < 20) {
        timer = setTimeout(run, 1500);
      } else if (!cancelled && attempts >= 20) {
        setProfileEnriching(false);
        setProfileEnrichState('timeout');
      }
    };
    run();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [details, fetchProfile, profileEnriching]);

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
    const fallbackBackLink = resolveBackLink(location, {
      path: `/library${location.search || ''}`,
      label: 'Library',
    });
    return (
      <div className="container py-8">
        <Card>
          <CardContent className="p-8 space-y-4 text-center">
            <p className="text-muted-foreground">{error || 'Artist not found'}</p>
            <Button variant="outline" onClick={() => navigate(fallbackBackLink.path)}>
              {`Back to ${fallbackBackLink.label}`}
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  const tags = (profile?.tags || []).slice(0, 8);
  const similar = ((fallbackSimilar && fallbackSimilar.length > 0) ? fallbackSimilar : (profile?.similar_artists || [])).slice(0, 12);
  const heroImage = details.artist_thumb || null;
  const profileText = (
    profile?.bio
    || profile?.short_bio
    || details.artist_profile?.bio
    || details.artist_profile?.short_bio
    || ''
  ).trim();
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
    if (profileText) {
      return {
        text: profileText,
        source: profile?.source || details.artist_profile?.source || '',
        updated_at: profile?.updated_at || details.artist_profile?.updated_at || 0,
      };
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
  const factsMonikers = getFactsArray('monikers');
  const factsGenres = getFactsArray('genres');
  const factsGroups = getFactsArray('member_of');
  const factsCollabs = getFactsArray('collaborated_with');
  const factsLabels = getFactsArray('labels');
  const factsCities = getFactsArray('notable_cities');
  const factsDied = [
    ...getFactsArray('death_date'),
    ...getFactsArray('died'),
    ...getFactsArray('deceased'),
    ...getFactsArray('is_dead'),
  ];
  const connectionCount =
    factsAka.length
    + factsAliases.length
    + factsMonikers.length
    + factsGroups.length
    + factsCollabs.length
    + factsLabels.length
    + factsCities.length;
  const connectionCountDisplay = factsLoadedOnce ? String(connectionCount) : "—";
  const profileGenres = (profile?.tags || []).map((x) => String(x || '').trim()).filter(Boolean);
  const normalizedGenres = Array.from(new Set([...factsGenres, ...profileGenres])).slice(0, 10);
  const isDeceased =
    factsDied.length > 0
    || /(^|\W)(died|deceased|passed away|late)(\W|$)/i.test(displayText.slice(0, 1600));
  const hasConcertSignals = concertsLoading || Boolean(concertsError) || concertList.events.length > 0 || Boolean(isDeceased);
  const hasConnectionSignals = factsLoading || factsLoadedOnce || connectionCount > 0;
  const showInsightsCard = hasConcertSignals || hasConnectionSignals;
  const artistAddedAt = Number(details.created_at || 0) > 0 ? Number(details.created_at || 0) : null;
  const artistUpdatedAt =
    Number(details.updated_at || 0) > 0
      ? Number(details.updated_at || 0)
      : (Number(displayUpdated || 0) > 0 ? Number(displayUpdated || 0) : null);
  const effectiveBackLink = resolveBackLink(location, {
    path: `/library${location.search || ''}`,
    label: 'Library',
  });

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
    <div className="container py-4 md:py-6 space-y-5 md:space-y-6">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <Button variant="ghost" className="gap-2" onClick={() => navigate(effectiveBackLink.path)}>
            <ArrowLeft className="w-4 h-4" />
            {`Back to ${effectiveBackLink.label}`}
          </Button>
          {profileEnriching && (
            <Badge variant="outline" className={`gap-1.5 ${badgeKindClass('source')}`}>
              <Loader2 className="w-3 h-3 animate-spin" />
              Enriching profile
            </Badge>
          )}
          {!profileEnriching && profileEnrichState === 'timeout' ? (
            <Badge variant="outline" className={`gap-1.5 ${badgeKindClass('status_soft')}`} title="Background enrichment timed out for this attempt.">
              Enrichment timed out
            </Badge>
          ) : null}
        </div>

        <Card className="overflow-hidden border-border/70">
          <div className="relative overflow-hidden">
            <div className="absolute inset-0 z-10 bg-background/34 backdrop-blur-md" />
            <div className="absolute inset-0 z-10 bg-gradient-to-r from-background/88 via-background/70 to-background/58" />
            {heroImage ? (
              <img src={heroImage} alt={details.artist_name} className="w-full h-64 md:h-96 object-cover blur-[1.4px] scale-[1.06]" />
            ) : (
              <div className="h-64 md:h-96 bg-gradient-to-br from-zinc-900 via-zinc-800 to-zinc-900" />
            )}
            <div className="absolute inset-0 z-20 p-6 md:p-8 flex items-end">
              <div className="grid grid-cols-1 md:grid-cols-[15rem,minmax(0,1fr)] gap-8 md:gap-14 w-full items-center">
                <div className="w-28 h-28 sm:w-36 sm:h-36 md:w-56 md:h-56 lg:w-60 lg:h-60 rounded-3xl overflow-hidden border border-border/60 bg-muted shrink-0 shadow-[0_24px_60px_rgba(0,0,0,0.35)]">
                  {heroImage ? (
                    <img src={heroImage} alt={details.artist_name} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center">
                      <Music className="w-12 h-12 text-muted-foreground" />
                    </div>
                  )}
                </div>
                <div className="min-w-0 md:pl-2 rounded-2xl bg-background/22 px-4 py-3 backdrop-blur-sm shadow-[0_10px_35px_rgba(0,0,0,0.18)]">
                  <h1 className="text-2xl sm:text-3xl md:text-5xl font-bold tracking-tight leading-tight text-white drop-shadow-[0_2px_10px_rgba(0,0,0,0.55)]">
                    {details.artist_name}
                  </h1>
                  <p className="text-base text-white/82 mt-1.5">
                    {details.total_albums.toLocaleString()} album{details.total_albums !== 1 ? 's' : ''}
                  </p>
                  {tags.length > 0 && (
                    <div className="flex flex-wrap gap-1.5 mt-3">
                      {tags.map((tag) => (
                        <Badge key={tag} variant="outline" className={`text-[11px] ${badgeKindClass('genre')}`}>
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
                      <ProviderBadge provider={displaySource} prefix="Source" className="text-[10px]" />
                    ) : null}
                    {artistAddedAt ? (
                      <Badge variant="outline" className={`text-[10px] ${badgeKindClass('muted')}`}>
                        Added {formatBadgeDateTime(artistAddedAt)}
                      </Badge>
                    ) : null}
                    <button
                      type="button"
                      className={cn(
                        'inline-flex items-center rounded-full border px-2.5 py-1 text-[10px] leading-none transition-colors hover:brightness-110',
                        badgeKindClass('muted')
                      )}
                      onClick={() => setMatchDialogOpen(true)}
                      title="Open match detail"
                    >
                      Updated: {formatBadgeDateTime(artistUpdatedAt)}
                    </button>
                    <ShareDialog
                      entityType="artist"
                      entityId={details.artist_id}
                      entityLabel={details.artist_name}
                      entitySubtitle={`${details.total_albums} album${details.total_albums !== 1 ? 's' : ''}`}
                      trigger={(
                        <Button type="button" size="sm" variant="outline" className="h-8 gap-2">
                          <Users className="h-3.5 w-3.5" />
                          Share
                        </Button>
                      )}
                    />
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
                    {canUseAI ? (
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        className="h-8 gap-2"
                        onClick={() => void runArtistAiResearch()}
                        disabled={aiProcessBusy}
                        title="Run AI research and consistency checks for this artist and albums"
                      >
                        {aiProcessBusy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Sparkles className="h-3.5 w-3.5" />}
                        AI research
                      </Button>
                    ) : null}
                    {canUseAI ? (
                      <EntityDiscoverDialog
                        entityType="artist"
                        artistId={details.artist_id}
                        entityLabel={details.artist_name}
                        triggerLabel="Discover"
                      />
                    ) : null}
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-8 gap-2"
                      onClick={() => setMatchDialogOpen(true)}
                    >
                      <Info className="h-3.5 w-3.5" />
                      Match detail
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

        {showInsightsCard ? (
        <Card className="border-border/70">
          <CardContent className="p-5 space-y-4">
            <div className="flex flex-wrap items-center gap-2">
              {hasConcertSignals ? (
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  className="h-8 gap-2"
                  onClick={() => setShowConcertsModal(true)}
                >
                  <Calendar className="h-3.5 w-3.5" />
                  Concerts
                  <Badge variant={concertList.events.length > 0 ? 'secondary' : 'outline'} className="h-5 px-1.5 text-[10px]">
                    {concertList.events.length}
                  </Badge>
                </Button>
              ) : null}
              {connectionCount > 0 ? (
                <Badge variant="outline" className={`h-8 px-2.5 text-[11px] gap-1.5 ${badgeKindClass('count')}`}>
                  <Users className="h-3.5 w-3.5" />
                  Connections: {connectionCountDisplay}
                </Badge>
              ) : null}
              {isDeceased ? (
                <Badge
                  variant="secondary"
                  className={`h-8 px-2.5 text-[11px] ${badgeKindClass('status_soft')}`}
                  title="At best you'll see a cover band."
                >
                  No more concerts
                </Badge>
              ) : null}
              {factsLoading ? (
                <Badge variant="outline" className={`h-8 px-2.5 text-[11px] gap-1.5 ${badgeKindClass('source')}`}>
                  <Loader2 className="h-3 w-3 animate-spin" />
                  Updating facts
                </Badge>
              ) : null}
            </div>

            <div className="space-y-2">
              {normalizedGenres.length > 0 ? (
                <div className="flex flex-wrap gap-1.5">
                  {normalizedGenres.map((g) => (
                    <Button
                      key={`genre-${g}`}
                      type="button"
                      size="sm"
                      variant="outline"
                      className={`h-6 px-2 text-[11px] ${badgeKindClass('genre')}`}
                      title="Open genre"
                      onClick={() => navigate(`/library/genre/${encodeURIComponent(g)}${location.search || ''}`, { state: withBackLinkState(location) })}
                    >
                      {g}
                    </Button>
                  ))}
                </div>
              ) : null}

              {factsLabels.length > 0 ? (
                <div className="flex flex-wrap gap-1.5">
                  {factsLabels.slice(0, 12).map((x) => (
                    <Button
                      key={`lab-${x}`}
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-6 px-2 text-[11px]"
                      onClick={() => navigate(`/library/label/${encodeURIComponent(x)}${location.search || ''}`, { state: withBackLinkState(location) })}
                      title="Open label"
                    >
                      {x}
                    </Button>
                  ))}
                </div>
              ) : null}

              {(factsMonikers.length > 0 || factsAka.length > 0 || factsAliases.length > 0) ? (
                <div className="flex flex-wrap gap-1.5">
                  {[...factsMonikers, ...factsAka, ...factsAliases].slice(0, 16).map((x) => (
                    <Button
                      key={`alias-${x}`}
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-6 px-2 text-[11px]"
                      onClick={() => void openArtistByName(x)}
                      title="Open artist"
                    >
                      {x}
                    </Button>
                  ))}
                </div>
              ) : null}
            </div>

            {hasConnectionSignals ? (
            <Collapsible open={connectionDetailsOpen} onOpenChange={setConnectionDetailsOpen}>
              <CollapsibleTrigger asChild>
                <Button type="button" variant="ghost" size="sm" className="gap-1.5 px-0">
                  <ChevronDown className="h-4 w-4" />
                  Show connection details
                </Button>
              </CollapsibleTrigger>
              <CollapsibleContent className="pt-2">
                {!factsLoadedOnce && !factsLoading ? (
                  <p className="text-sm text-muted-foreground">
                    Connections are loaded on demand from cached scan data. Open once to fetch.
                  </p>
                ) : connectionCount === 0 ? (
                  <p className="text-sm text-muted-foreground">
                    No connections found yet. Use Refresh to analyze labels, collaborations, and related acts.
                  </p>
                ) : (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
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
                    {factsCities.length > 0 ? (
                      <div className="space-y-2">
                        <div className="text-xs font-medium text-muted-foreground">Cities</div>
                        <div className="flex flex-wrap gap-1.5">
                          {factsCities.map((x) => (
                            <Badge key={`city-${x}`} variant="outline" className={`text-[11px] ${badgeKindClass('source')}`}>
                              {x}
                            </Badge>
                          ))}
                        </div>
                      </div>
                    ) : null}
                  </div>
                )}
              </CollapsibleContent>
            </Collapsible>
            ) : null}
          </CardContent>
        </Card>
        ) : null}

        <Dialog open={showConcertsModal} onOpenChange={setShowConcertsModal}>
          <DialogContent className="max-w-4xl max-h-[86vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                <Calendar className="w-4 h-4 text-primary" />
                Upcoming Concerts
                <Badge variant="outline" className={`text-[10px] ${badgeKindClass('count')}`}>
                  {concertList.events.length}
                </Badge>
                {concertsMeta?.provider ? (
                  <ProviderBadge provider={concertsMeta.provider} className="text-[10px]" />
                ) : null}
              </DialogTitle>
              <DialogDescription>
                {concertsMeta?.updated_at ? `Updated ${formatBadgeDateTime(concertsMeta.updated_at)}.` : 'No update timestamp.'}
                {concertHome ? ` Radius filter: ${Math.round(concertHome.radiusKm)} km.` : ''}
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4">
              {concertList.events.length > 0 ? (
                <ConcertsMiniMap
                  events={concertList.events}
                  home={concertHome ? { lat: concertHome.lat, lon: concertHome.lon, radiusKm: concertHome.radiusKm } : null}
                />
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
                  {isDeceased
                    ? 'No concerts expected. At best you will see a tribute band.'
                    : (concertHome ? 'No upcoming concerts found within your radius.' : 'No upcoming concerts found.')}
                </p>
              ) : (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {concertList.events.map((ev, idx) => {
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
            </div>
          </DialogContent>
        </Dialog>

        <div className="space-y-6">
          {sortedTypes.map((type) => (
            <section key={type} className="space-y-3">
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                  <Disc3 className="w-4 h-4 text-primary" />
                  <h2 className="text-lg font-semibold">{type === 'Single' ? 'Singles' : `${type}s`}</h2>
                </div>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  className="h-8 text-xs"
                  onClick={() => setShowBadges(!showBadges)}
                >
                  {showBadges ? 'Hide badges' : 'Show badges'}
                </Button>
              </div>
              <Carousel opts={{ align: 'start', dragFree: true }} className="w-full">
                <CarouselContent className="-ml-3">
                  {grouped[type].map((album) => (
                    <CarouselItem key={album.album_id} className="pl-3 basis-[180px] sm:basis-[200px] md:basis-[220px]">
                      <Card
                        className="group h-full overflow-hidden border-border/70 bg-card/90 cursor-pointer transition-all duration-300 hover:-translate-y-1 hover:border-primary/35 hover:bg-muted/20"
                        role="button"
                        tabIndex={0}
                        onClick={() => navigate(`/library/album/${album.album_id}${location.search || ''}`, { state: withBackLinkState(location) })}
                        onKeyDown={(e) => {
                          if (e.key === "Enter" || e.key === " ") {
                            e.preventDefault();
                            navigate(`/library/album/${album.album_id}${location.search || ''}`, { state: withBackLinkState(location) });
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
                          <AlbumArtwork albumThumb={album.thumb} artistId={artistId} alt={album.title} size={512} />
                          <div className="absolute inset-x-0 bottom-0 p-2.5 opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity">
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
                          <AlbumBadgeGroups
                            show={showBadges}
                            compact
                            userRating={album.user_rating}
                            publicRating={album.public_rating}
                            publicRatingVotes={album.public_rating_votes}
                            format={album.format}
                            isLossless={album.is_lossless}
                            year={album.year || album.date || null}
                            trackCount={album.track_count}
                            genres={album.genres || (album.genre ? [album.genre] : [])}
                            label={album.label}
                            onGenreClick={(genre) => navigate(`/library/genre/${encodeURIComponent(genre)}${location.search || ''}`, { state: withBackLinkState(location) })}
                            onLabelClick={album.label ? () => navigate(`/library/label/${encodeURIComponent(album.label || '')}${location.search || ''}`, { state: withBackLinkState(location) }) : undefined}
                          />
                          {!showBadges && (album.year || album.date) ? (
                            <p className="text-[11px] text-muted-foreground">
                              {album.year || album.date}
                            </p>
                          ) : null}
                          {showBadges && album.short_description && (
                            <p className={cn('text-[11px] text-muted-foreground line-clamp-3')}>
                              {album.short_description}
                            </p>
                          )}
                        </CardContent>
                      </Card>
                    </CarouselItem>
                  ))}
                </CarouselContent>
                <div className="hidden md:block">
                  <CarouselPrevious className="left-2 top-[42%] h-9 w-9 bg-background/70 backdrop-blur-sm" />
                  <CarouselNext className="right-2 top-[42%] h-9 w-9 bg-background/70 backdrop-blur-sm" />
                </div>
              </Carousel>
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
	                      navigate(`/library/artist/${artist.artist_id}${location.search || ''}`, { state: withBackLinkState(location) });
	                      return;
	                    }
	                    const href = `https://bandcamp.com/search?q=${encodeURIComponent(artist.name || '')}`;
	                    window.open(href, '_blank', 'noopener,noreferrer');
	                  }}
	                  onKeyDown={(e) => {
	                    if (e.key === "Enter" || e.key === " ") {
	                      e.preventDefault();
	                      if (artist.artist_id) {
	                        navigate(`/library/artist/${artist.artist_id}${location.search || ''}`, { state: withBackLinkState(location) });
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

        <MatchDetailDialog
          open={matchDialogOpen}
          onOpenChange={setMatchDialogOpen}
          entity={{ kind: 'artist', artistId: details.artist_id }}
          onDataChanged={() => {
            void fetchArtist();
          }}
        />
    </div>
  );
}
