import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, Calendar, ChevronDown, ChevronUp, Disc3, Download, Heart, Info, ListPlus, Loader2, Music, Pencil, Play, Plus, Share2, Users } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { FormatBadge } from '@/components/FormatBadge';
import { AuthenticatedImage } from '@/components/library/AuthenticatedImage';
import { AlbumArtworkGalleryDialog } from '@/components/library/AlbumArtworkGalleryDialog';
import { AlbumRatingStars } from '@/components/library/AlbumRatingStars';
import { EntityDiscoverDialog } from '@/components/library/EntityDiscoverDialog';
import { SocialActivityBadges } from '@/components/social/SocialActivityBadges';
import { MatchDetailDialog } from '@/components/library/MatchDetailDialog';
import { ProviderBadge } from '@/components/providers/ProviderBadge';
import { ProviderLink } from '@/components/providers/ProviderLink';
import { ShareDialog } from '@/components/social/ShareDialog';
import * as api from '@/lib/api';
import { badgeKindClass } from '@/lib/badgeStyles';
import { resolveBackLink, withBackLinkState } from '@/lib/backNavigation';
import { formatBadgeDateTime } from '@/lib/dateFormat';
import { normalizeProviderId } from '@/lib/providerMeta';
import { cn } from '@/lib/utils';
import { useToast } from '@/hooks/use-toast';
import { usePlayback } from '@/contexts/PlaybackContext';
import { useAuth } from '@/contexts/AuthContext';
import type { TrackInfo } from '@/components/library/AudioPlayer';

function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds || 0));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}:${m.toString().padStart(2, '0')}:${sec.toString().padStart(2, '0')}`;
  return `${m}:${sec.toString().padStart(2, '0')}`;
}

function formatBytes(bytes: number): string {
  const value = Number(bytes || 0);
  if (!Number.isFinite(value) || value <= 0) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = value;
  let idx = 0;
  while (size >= 1024 && idx < units.length - 1) {
    size /= 1024;
    idx += 1;
  }
  const fixed = size >= 10 || idx === 0 ? 0 : 1;
  return `${size.toFixed(fixed)} ${units[idx]}`;
}

function wordCount(text: string): number {
  const t = (text || '').trim();
  if (!t) return 0;
  const m = t.match(/[A-Za-z0-9]+(?:'[A-Za-z0-9]+)?/g);
  return m ? m.length : 0;
}

function hasClassicalIdentity(payload?: api.ClassicalIdentityPayload | null): boolean {
  if (!payload) return false;
  if (payload.is_classical === true) return true;
  return Boolean(
    (payload.work && payload.work.length)
    || (payload.conductor && payload.conductor.length)
    || (payload.orchestra && payload.orchestra.length)
    || (payload.ensemble && payload.ensemble.length)
    || (payload.soloists && payload.soloists.length)
    || (payload.catalog_numbers && payload.catalog_numbers.length)
  );
}

function normalizeArtistName(value: string): string {
  return (value || '').trim().toLowerCase().replace(/\s+/g, ' ');
}

function escapeRegExp(value: string): string {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function albumTitleVariants(value: string): string[] {
  const raw = String(value || '').trim();
  if (!raw) return [];
  const compact = raw
    .normalize('NFKD')
    .replace(/[^\w\s.]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  const deEllipsed = raw.replace(/[.…]+$/g, '').trim();
  const candidates = [raw, compact, deEllipsed];
  if (raw.endsWith('...')) candidates.push(raw.slice(0, -3).trim());
  if (raw.endsWith('…')) candidates.push(raw.slice(0, -1).trim());
  return Array.from(new Set(candidates.filter(Boolean)));
}

function inferDiscAndTrackFromTitle(rawTitle: string): { discNum?: number; trackNum?: number } {
  const raw = String(rawTitle || '').trim();
  if (!raw) return {};
  const match = raw.match(/(?:^|[-\s])(\d{1,2})\s*[-_.]\s*(\d{1,2})(?:\s*[-_.]|\s+)/);
  if (!match) return {};
  const discNum = Number(match[1] || 0);
  const trackNum = Number(match[2] || 0);
  return {
    discNum: Number.isFinite(discNum) && discNum > 0 ? discNum : undefined,
    trackNum: Number.isFinite(trackNum) && trackNum > 0 ? trackNum : undefined,
  };
}

function cleanAlbumTrackTitle(rawTitle: string, albumTitle: string, fallbackTrack: number): string {
  const raw = String(rawTitle || '').trim();
  if (!raw) return `Track ${Math.max(1, fallbackTrack || 1)}`;
  let cleaned = raw.replace(/_/g, ' ').trim();
  for (const variant of albumTitleVariants(albumTitle)) {
    const escaped = escapeRegExp(variant);
    cleaned = cleaned.replace(new RegExp(`^${escaped}\\s*-\\s*`, 'i'), '');
    cleaned = cleaned.replace(new RegExp(`^${escaped}\\s*`, 'i'), '');
  }
  cleaned = cleaned.replace(/^(?:cd|disc)\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\s*[-_. ]*/i, '');
  cleaned = cleaned.replace(/^\d{1,2}\s*[-_.]\s*\d{1,3}\s*[-_. ]*/i, '');
  cleaned = cleaned.replace(/^\d{1,2}\s*[-_.]\s*\d{1,2}\s*[-_.]\s*\d{1,3}\s*[-_. ]*/i, '');
  cleaned = cleaned.replace(/^(?:side\s*)?[a-z]\s*[-_. ]\s*\d{1,3}\s*[-_. ]*/i, '');
  cleaned = cleaned.replace(/^\d{1,3}\s*[-_. ]*/, '');
  cleaned = cleaned.replace(/\s+/g, ' ').replace(/^[-. ]+|[-. ]+$/g, '').trim();
  return cleaned || raw || `Track ${Math.max(1, fallbackTrack || 1)}`;
}

function groupAlbumTracksByDisc(
  tracks: Array<api.AlbumDetailTrack & { display_num: string; display_artist: string; display_title: string; disc_label_text: string }>
) {
  const grouped = new Map<string, { key: string; label: string; tracks: typeof tracks }>();
  for (const track of tracks) {
    const discNum = Math.max(1, Number(track.disc_num || 1));
    const label = String(track.disc_label_text || '').trim() || `Disc ${discNum}`;
    const key = `${discNum}:${label.toLowerCase()}`;
    if (!grouped.has(key)) grouped.set(key, { key, label, tracks: [] as typeof tracks });
    grouped.get(key)?.tracks.push(track);
  }
  return Array.from(grouped.values());
}

function shouldSplitInlineTrackArtist(tracks: api.AlbumDetailTrack[]): boolean {
  const rows = tracks || [];
  if (rows.length === 0) return false;
  const inlineCount = rows.reduce((acc, track) => {
    return acc + (/\s[-–—]\s/.test((track.title || '').trim()) ? 1 : 0);
  }, 0);
  if (rows.length <= 3) return inlineCount >= 2;
  return inlineCount >= Math.max(3, Math.ceil(rows.length * 0.5));
}

function splitInlineTrackArtistTitle(
  rawTitle: string,
  albumArtist: string,
  inlineArtistMode: boolean
): { artist: string; title: string } {
  const fallbackArtist = (albumArtist || '').trim() || 'Unknown artist';
  const title = (rawTitle || '').trim();
  if (!inlineArtistMode || !title) return { artist: fallbackArtist, title: title || 'Untitled' };

  const parts = title.split(/\s[-–—]\s/g).map((x) => x.trim()).filter(Boolean);
  if (parts.length < 2) {
    return { artist: fallbackArtist, title: title || 'Untitled' };
  }

  const left = parts[0] || '';
  const right = parts.slice(1).join(' - ').trim();
  if (!left || !right) {
    return { artist: fallbackArtist, title: title || 'Untitled' };
  }

  const albumNorm = normalizeArtistName(albumArtist || '');
  const leftNorm = normalizeArtistName(left);
  const rightNorm = normalizeArtistName(right);
  if (albumNorm && rightNorm === albumNorm) return { artist: right, title: left };
  if (albumNorm && leftNorm === albumNorm) return { artist: left, title: right };

  const leftWords = wordCount(left);
  const rightWords = wordCount(right);
  if (leftWords <= 4) return { artist: left, title: right };
  if (rightWords <= 3 && leftWords >= 5) return { artist: right, title: left };
  return { artist: left, title: right };
}

function parseGenreBadges(value: string): string[] {
  const raw = String(value || '').trim();
  if (!raw) return [];
  const chunks = raw
    .split(/[;,/|]+/g)
    .map((item) => item.trim())
    .filter(Boolean);
  const uniq = new Set<string>();
  const out: string[] = [];
  for (const chunk of chunks) {
    const norm = chunk.toLowerCase();
    if (uniq.has(norm)) continue;
    uniq.add(norm);
    out.push(chunk);
    if (out.length >= 10) break;
  }
  return out;
}

function formatCompactCount(value?: number | null): string {
  const num = Number(value || 0);
  if (!Number.isFinite(num) || num <= 0) return '0';
  return new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 }).format(num);
}

function joinClassical(values?: string[] | null, limit = 3): string | null {
  const items = Array.isArray(values) ? values.filter((value) => typeof value === 'string' && value.trim()) : [];
  if (!items.length) return null;
  return items.slice(0, limit).join(' · ');
}

function albumHeroTitleClass(title: string): string {
  const length = String(title || '').trim().length;
  if (length >= 70) {
    return 'text-[1.95rem] leading-[1.04] md:text-[2.25rem] md:leading-[1.02] xl:text-[2.85rem] xl:leading-[1]';
  }
  if (length >= 48) {
    return 'text-[2.2rem] leading-[1.03] md:text-[2.55rem] md:leading-[1.01] xl:text-[3.35rem] xl:leading-[0.99]';
  }
  return 'text-3xl leading-[1.02] md:text-[2.85rem] md:leading-[0.98] xl:text-5xl xl:leading-[1.01]';
}

export default function AlbumPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const params = useParams<{ albumId: string }>();
  const albumId = Number(params.albumId);
  const { startPlayback, setCurrentTrack, queueTrack } = usePlayback();
  const { isAdmin, canDownload, canUseAI } = useAuth();
  const { toast } = useToast();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<api.AlbumDetailResponse | null>(null);
  const [reviewExpanded, setReviewExpanded] = useState(false);
  const [playlistsLoading, setPlaylistsLoading] = useState(false);
  const [playlists, setPlaylists] = useState<api.PlaylistSummary[]>([]);
  const [addingTrackId, setAddingTrackId] = useState<number | null>(null);
  const [downloadingAlbum, setDownloadingAlbum] = useState(false);
  const [matchDialogOpen, setMatchDialogOpen] = useState(false);
  const [artworkGalleryOpen, setArtworkGalleryOpen] = useState(false);
  const [trackDetailsLoading, setTrackDetailsLoading] = useState(false);
  const [trackDetailsError, setTrackDetailsError] = useState<string | null>(null);
  const [expandedTrackId, setExpandedTrackId] = useState<number | null>(null);
  const [trackDetailsById, setTrackDetailsById] = useState<Record<number, api.AlbumTrackDetailItem>>({});
  const [savingUserRating, setSavingUserRating] = useState(false);
  const [albumLiked, setAlbumLiked] = useState(false);
  const durationRefreshAttemptsRef = useRef(0);

  const load = useCallback(async () => {
    if (!Number.isFinite(albumId) || albumId <= 0) {
      setError('Invalid album id');
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const res = await api.getAlbumDetail(albumId);
      setData(res);
    } catch (e) {
      setData(null);
      setError(e instanceof Error ? e.message : 'Failed to load album');
    } finally {
      setLoading(false);
    }
  }, [albumId]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    durationRefreshAttemptsRef.current = 0;
    setExpandedTrackId(null);
    setTrackDetailsError(null);
    setTrackDetailsById({});
  }, [albumId]);

  const loadTrackDetails = useCallback(async () => {
    if (!Number.isFinite(albumId) || albumId <= 0) return;
    setTrackDetailsLoading(true);
    setTrackDetailsError(null);
    try {
      const res = await api.getAlbumTrackDetails(albumId, true);
      const map: Record<number, api.AlbumTrackDetailItem> = {};
      for (const item of res.tracks || []) {
        const tid = Number(item?.track_id || 0);
        if (tid > 0) map[tid] = item;
      }
      setTrackDetailsById(map);
    } catch (e) {
      setTrackDetailsError(e instanceof Error ? e.message : 'Failed to load track details');
    } finally {
      setTrackDetailsLoading(false);
    }
  }, [albumId]);

  useEffect(() => {
    if (!Number.isFinite(albumId) || albumId <= 0) return;
    if (!data || loading) return;
    const hasMissingDurations = (data.tracks || []).some((t) => Number(t.duration_sec || 0) <= 0);
    if (!hasMissingDurations) return;
    if (durationRefreshAttemptsRef.current >= 3) return;

    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const run = async () => {
      if (cancelled) return;
      durationRefreshAttemptsRef.current += 1;
      try {
        const refreshed = await api.getAlbumDetail(albumId);
        if (!cancelled) setData(refreshed);
      } catch {
        // Best-effort refresh only.
      }
    };

    const attemptNo = Math.max(0, durationRefreshAttemptsRef.current);
    timer = setTimeout(run, 900 + attemptNo * 1300);
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [albumId, data, loading]);

  const loadPlaylists = useCallback(async () => {
    setPlaylistsLoading(true);
    try {
      const res = await api.getPlaylists();
      setPlaylists(Array.isArray(res.playlists) ? res.playlists : []);
    } catch {
      setPlaylists([]);
    } finally {
      setPlaylistsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadPlaylists();
  }, [loadPlaylists]);

  useEffect(() => {
    let cancelled = false;
    if (!Number.isFinite(albumId) || albumId <= 0) {
      setAlbumLiked(false);
      return;
    }
    void (async () => {
      try {
        const res = await api.getLikes('album', [albumId]);
        const liked = Boolean((res.items || []).find((item) => Number(item.entity_id || 0) === albumId)?.liked);
        if (!cancelled) setAlbumLiked(liked);
      } catch {
        if (!cancelled) setAlbumLiked(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [albumId]);

  const trackRows = useMemo(() => {
    const tracks = data?.tracks || [];
    const albumArtist = data?.artist_name || '';
    const albumTitle = data?.title || '';
    const inlineArtistMode = shouldSplitInlineTrackArtist(tracks);
    const maxDisc = tracks.reduce((acc, track) => {
      const inferred = inferDiscAndTrackFromTitle(track.title || '');
      return Math.max(acc, Number(track.disc_num || inferred.discNum || 1));
    }, 1);
    return tracks.map((t, idx) => {
      const inferred = inferDiscAndTrackFromTitle(t.title || '');
      const discNum = Math.max(1, Number(t.disc_num || inferred.discNum || 1));
      const trackNum = t.track_num > 0 ? t.track_num : Number(inferred.trackNum || idx + 1);
      const cleanedTitle = cleanAlbumTrackTitle(t.title || '', albumTitle, trackNum);
      const parsed = splitInlineTrackArtistTitle(cleanedTitle, albumArtist, inlineArtistMode);
      const n = t.track_num > 0 ? t.track_num : idx + 1;
      return {
        ...t,
        disc_num: discNum,
        track_num: trackNum,
        display_num: String(trackNum > 0 ? trackNum : n),
        display_artist: (parsed.artist || albumArtist || 'Unknown artist').trim(),
        display_title: (parsed.title || cleanedTitle || t.title || `Track ${idx + 1}`).trim(),
        disc_label_text: String(t.disc_label || '').trim() || (maxDisc > 1 ? `Disc ${discNum}` : ''),
      };
    });
  }, [data?.artist_name, data?.title, data?.tracks]);

  const trackGroups = useMemo(() => groupAlbumTracksByDisc(trackRows), [trackRows]);
  const showDiscSections = trackGroups.length > 1;

  const playbackTracks = useMemo<TrackInfo[]>(() => {
    if (!data) return [];
    const albumTitle = data.title || 'Album';
    return trackRows
      .filter((t) => (t.track_id || 0) > 0)
      .map((t, idx) => ({
        track_id: t.track_id,
        title: t.display_title || `Track ${idx + 1}`,
        artist: t.display_artist || data.artist_name || '',
        album: albumTitle,
        duration: t.duration_sec || 0,
        // Keep player behavior consistent: use track_num when present; fall back to sequential index.
        index: t.track_num > 0 ? t.track_num : idx + 1,
        file_url: t.file_url,
      }));
  }, [data, trackRows]);

  const playbackByTrackId = useMemo(() => {
    const map = new Map<number, TrackInfo>();
    for (const t of playbackTracks) map.set(t.track_id, t);
    return map;
  }, [playbackTracks]);

  const handlePlay = () => {
    if (!data) return;
    if (playbackTracks.length === 0) return;
    startPlayback(data.album_id, data.title || 'Album', data.cover_url || null, playbackTracks);
  };

  const handlePlayTrack = (trackId: number) => {
    if (!data) return;
    if (playbackTracks.length === 0) return;
    const target = playbackByTrackId.get(trackId);
    if (!target) return;
    startPlayback(data.album_id, data.title || 'Album', data.cover_url || null, playbackTracks);
    setCurrentTrack(target);
  };

  const handleQueueTrack = (trackId: number) => {
    if (!data) return;
    const target = playbackByTrackId.get(trackId);
    if (!target) return;
    queueTrack(target, {
      albumId: data.album_id,
      albumTitle: data.title || 'Album',
      albumThumb: data.cover_url || null,
    });
  };

  const handleDownloadAlbum = useCallback(async () => {
    if (!data || downloadingAlbum) return;
    setDownloadingAlbum(true);
    try {
      await api.downloadAlbumZip(data.album_id, data.artist_name, data.title);
      toast({ title: 'Download started', description: 'Album archive is being saved.' });
    } catch (e) {
      toast({
        title: 'Download failed',
        description: e instanceof Error ? e.message : 'Unable to download this album',
        variant: 'destructive',
      });
    } finally {
      setDownloadingAlbum(false);
    }
  }, [data, downloadingAlbum, toast]);

  const handleUserRatingChange = useCallback(async (rating: number | null) => {
    if (!data || savingUserRating) return;
    const previous = data.ratings?.user_rating ?? null;
    setSavingUserRating(true);
    setData((prev) => {
      if (!prev) return prev;
      return {
        ...prev,
        ratings: {
          ...(prev.ratings || {}),
          user_rating: rating,
        },
      };
    });
    try {
      const res = await api.setAlbumRating(data.album_id, rating, 'ui_album_page');
      setData((prev) => {
        if (!prev) return prev;
        return {
          ...prev,
          ratings: {
            ...(prev.ratings || {}),
            user_rating: res.rating ?? null,
          },
        };
      });
    } catch (e) {
      setData((prev) => {
        if (!prev) return prev;
        return {
          ...prev,
          ratings: {
            ...(prev.ratings || {}),
            user_rating: previous,
          },
        };
      });
      toast({
        title: 'Rating failed',
        description: e instanceof Error ? e.message : 'Failed to save your rating',
        variant: 'destructive',
      });
    } finally {
      setSavingUserRating(false);
    }
  }, [data, savingUserRating, toast]);

  const toggleAlbumLike = useCallback(async () => {
    if (!data) return;
    const next = !albumLiked;
    setAlbumLiked(next);
    try {
      await api.setLike({ entity_type: 'album', entity_id: data.album_id, liked: next, source: 'ui_album_page' });
      toast({
        title: next ? 'Liked' : 'Unliked',
        description: next ? 'Album saved to favorites.' : 'Album removed from favorites.',
      });
    } catch (e) {
      setAlbumLiked(!next);
      toast({
        title: 'Like failed',
        description: e instanceof Error ? e.message : 'Failed to update like',
        variant: 'destructive',
      });
    }
  }, [albumLiked, data, toast]);

  const handleAddTrackToPlaylist = useCallback(
    async (trackId: number, playlistId: number) => {
      if (!trackId || !playlistId) return;
      const playlistName = playlists.find((p) => p.playlist_id === playlistId)?.name || 'Playlist';
      setAddingTrackId(trackId);
      try {
        await api.addPlaylistItems(playlistId, { track_id: trackId });
        toast({ title: 'Track added', description: `Added to ${playlistName}.` });
      } catch (e) {
        toast({
          title: 'Add failed',
          description: e instanceof Error ? e.message : 'Failed to add track to playlist',
          variant: 'destructive',
        });
      } finally {
        setAddingTrackId(null);
      }
    },
    [playlists, toast]
  );

  if (loading) {
    return (
      <div className="pmda-library-shell py-8">
        <div className="flex items-center justify-center py-24">
          <Loader2 className="w-10 h-10 animate-spin text-primary" />
        </div>
      </div>
    );
  }

  if (error || !data) {
    const fallbackBackLink = resolveBackLink(location, {
      path: `/library${location.search || ''}`,
      label: 'Library',
    });
    return (
      <div className="pmda-library-shell py-8">
        <Card className="border-border/70">
          <CardContent className="p-8 space-y-4 text-center">
            <p className="text-muted-foreground">{error || 'Album not found'}</p>
            <Button variant="outline" onClick={() => navigate(fallbackBackLink.path)}>
              {`Back to ${fallbackBackLink.label}`}
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  const reviewText = (data.review?.description || data.review?.short_description || '').trim();
  const reviewSource = (data.review?.source || '').trim();
  const showReviewToggle = wordCount(reviewText) >= 80 || reviewText.length >= 420;
  const genreBadges = parseGenreBadges(data.genre || '');
  const ratings = data.ratings || {};
  const ratingSignals = ratings.signals || {};
  const publicRatingSourceId = normalizeProviderId(ratings.public_rating_source || '');
  const albumUpdatedAt =
    Number(data.updated_at || 0) > 0
      ? Number(data.updated_at || 0)
      : (Number(data.review?.updated_at || 0) > 0 ? Number(data.review?.updated_at || 0) : null);
  const effectiveBackLink = resolveBackLink(location, {
    path: `/library/artist/${data.artist_id}${location.search || ''}`,
    label: 'Artist',
  });

  return (
    <div className="pmda-page-shell pmda-page-stack">
      <div className="flex flex-wrap items-start justify-between gap-3">
            <Button variant="ghost" className="gap-2" onClick={() => navigate(effectiveBackLink.path)}>
          <ArrowLeft className="w-4 h-4" />
          {`Back to ${effectiveBackLink.label}`}
        </Button>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            size="sm"
            variant={albumLiked ? 'default' : 'outline'}
            className="h-8 gap-2"
            onClick={() => void toggleAlbumLike()}
            title={albumLiked ? 'Unlike album' : 'Like album'}
          >
            <Heart className={cn('w-4 h-4', albumLiked ? 'fill-current' : '')} />
            {albumLiked ? 'Liked' : 'Like'}
          </Button>
          <ShareDialog
            entityType="album"
            entityId={data.album_id}
            entityLabel={data.title}
            entitySubtitle={data.artist_name}
            trigger={(
              <Button size="sm" variant="outline" className="h-8 gap-2">
                <Share2 className="w-4 h-4" />
                Share
              </Button>
            )}
          />
          {canUseAI ? (
            <EntityDiscoverDialog
              entityType="album"
              albumId={data.album_id}
              entityLabel={data.title}
              triggerLabel="Discover"
            />
          ) : null}
          <Button
            size="sm"
            variant="outline"
            className="h-8 gap-2"
            onClick={() => setMatchDialogOpen(true)}
          >
            <Info className="w-4 h-4" />
            Match detail
          </Button>
          {data.label ? (
            <Button
              size="sm"
              variant="outline"
              className="h-8"
              onClick={() => navigate(`/library/label/${encodeURIComponent(data.label || '')}${location.search || ''}`, { state: withBackLinkState(location) })}
              title="Open label"
            >
              Label: {data.label}
            </Button>
          ) : null}
          {(isAdmin || canDownload) ? (
            <Button
              size="sm"
              variant="outline"
              className="h-8 gap-2"
              onClick={() => void handleDownloadAlbum()}
              disabled={downloadingAlbum}
            >
              {downloadingAlbum ? <Loader2 className="w-4 h-4 animate-spin" /> : <Download className="w-4 h-4" />}
              Download
            </Button>
          ) : null}
          {albumUpdatedAt ? (
            <button
              type="button"
              className={cn(
                "inline-flex items-center border px-2.5 py-1 text-[11px] leading-none transition-colors hover:brightness-110",
                badgeKindClass('muted')
              )}
              onClick={() => setMatchDialogOpen(true)}
              title="Open match detail"
            >
              Updated: {formatBadgeDateTime(albumUpdatedAt)}
            </button>
          ) : null}
        </div>
      </div>

      <Card className="pmda-flat-surface overflow-hidden">
        <div className="relative min-h-[22rem] overflow-hidden md:min-h-[26rem]">
          {data.cover_url ? (
            <AuthenticatedImage
              src={data.cover_url}
              alt={data.title}
              className="absolute inset-0 h-full w-full scale-[1.04] object-cover blur-[1.8px] saturate-[0.92]"
            />
          ) : (
            <div className="absolute inset-0 bg-gradient-to-br from-muted via-muted/70 to-accent/20" />
          )}
          <div className="absolute inset-0 z-10 bg-background/34 backdrop-blur-xl" />
          <div className="absolute inset-0 z-10 bg-gradient-to-r from-white/86 via-white/48 to-white/16 dark:from-background dark:via-background/92 dark:to-background/70" />
          <div className="absolute inset-0 z-10 bg-gradient-to-t from-background/76 via-transparent to-transparent" />
          <div className="relative z-20 flex min-h-[22rem] items-end p-6 md:min-h-[28rem] md:p-8 xl:p-10">
            <div className="grid w-full grid-cols-1 items-end gap-6 md:grid-cols-[19rem,1fr] xl:grid-cols-[21rem,1fr]">
              <button
                type="button"
                className="group relative h-56 w-56 overflow-hidden border border-white/12 bg-muted shadow-[0_28px_80px_-48px_rgba(0,0,0,0.9)] md:h-[19rem] md:w-[19rem] xl:h-[21rem] xl:w-[21rem]"
                onClick={() => setArtworkGalleryOpen(true)}
                title="View artwork stack"
              >
                <div className="relative h-full w-full">
                  {data.cover_url ? (
                    <AuthenticatedImage
                      src={data.cover_url}
                      alt={data.title}
                      className="h-full w-full object-cover animate-in fade-in-0 duration-300"
                    />
                  ) : (
                    <div className="flex h-full w-full items-center justify-center">
                      <Music className="h-8 w-8 text-muted-foreground" />
                    </div>
                  )}
                  <div className="absolute inset-0 flex items-center justify-center bg-black/0 opacity-0 transition-all duration-200 group-hover:bg-black/36 group-hover:opacity-100">
                    <span className="inline-flex items-center border border-white/20 bg-black/35 px-3 py-2 text-[11px] font-medium uppercase tracking-[0.24em] text-white backdrop-blur-sm">
                      View sleeves
                    </span>
                  </div>
                  <Button
                    type="button"
                    size="icon"
                    variant="secondary"
                    className="absolute right-2 top-2 z-10 h-8 w-8 opacity-0 transition-opacity group-hover:opacity-100"
                    title="Edit cover source"
                    onClick={(event) => {
                      event.stopPropagation();
                      setMatchDialogOpen(true);
                    }}
                  >
                    <Pencil className="h-3.5 w-3.5" />
                  </Button>
                </div>
              </button>
              <div className="min-w-0 border border-border/70 bg-white/52 px-5 py-5 backdrop-blur-md dark:border-white/10 dark:bg-background/58">
                <h1 className={cn(
                  'max-w-[18ch] pr-2 pt-1 font-bold tracking-tight text-slate-950 [text-wrap:balance] drop-shadow-[0_1px_4px_rgba(255,255,255,0.55)] break-words dark:text-white dark:drop-shadow-[0_2px_10px_rgba(0,0,0,0.7)]',
                  albumHeroTitleClass(data.title)
                )}>
                  {data.title}
                </h1>
                <button
                  type="button"
                  className="mt-2 truncate text-sm text-slate-800 hover:underline dark:text-white/82"
                  onClick={() => navigate(`/library/artist/${data.artist_id}${location.search || ''}`, { state: withBackLinkState(location) })}
                  title="Open artist"
                >
                  {data.artist_name}
                </button>

                <div className="mt-5 flex flex-wrap items-center gap-2">
                  {data.year ? (
                    <Badge variant="outline" className={cn("gap-1.5 text-[11px]", badgeKindClass('year'))}>
                      <Calendar className="w-3 h-3" />
                      {data.year}
                    </Badge>
                  ) : null}
                  <Badge variant="outline" className={cn("text-[11px]", badgeKindClass('count'))}>
                    {data.track_count} tracks
                  </Badge>
                  <Badge variant="outline" className={cn("text-[11px]", badgeKindClass('duration'))}>
                    {formatDuration(data.total_duration_sec || 0)}
                  </Badge>
                  {data.format ? <FormatBadge format={data.format} size="sm" /> : null}
                  <Badge
                    variant="outline"
                    className={cn("text-[11px]", data.is_lossless ? badgeKindClass('lossless') : badgeKindClass('lossy'))}
                  >
                    {data.is_lossless ? 'Lossless' : 'Lossy'}
                  </Badge>
                  {data.metadata_source ? (
                    data.metadata_source_url ? (
                      <ProviderLink
                        provider={data.metadata_source}
                        href={data.metadata_source_url}
                        prefix="Source"
                        className="inline-flex"
                      />
                    ) : (
                      <ProviderBadge provider={data.metadata_source} prefix="Source" className="text-[11px]" />
                    )
                  ) : null}
                  {data.bandcamp_album_url ? (
                    <ProviderLink provider="bandcamp" href={data.bandcamp_album_url} className="inline-flex" />
                  ) : null}
                </div>
                {genreBadges.length > 0 ? (
                  <div className="mt-4 space-y-1.5">
                    <div className="text-[11px] font-medium uppercase tracking-[0.22em] text-slate-700 dark:text-white/68">
                      Genres
                    </div>
                    <div className="flex flex-wrap gap-1.5">
                      {genreBadges.map((genre) => (
                        <Badge
                          key={`album-genre-${genre}`}
                          variant="outline"
                          className={cn("text-[11px] cursor-pointer", badgeKindClass('genre'))}
                          onClick={() => navigate(`/library/genre/${encodeURIComponent(genre)}${location.search || ''}`, { state: withBackLinkState(location) })}
                        >
                          {genre}
                        </Badge>
                      ))}
                    </div>
                  </div>
                ) : null}
                {hasClassicalIdentity(data.classical) ? (
                  <div className="mt-4 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
                    {joinClassical(data.classical.composer, 3) ? (
                      <div className="border border-border/70 bg-white/45 px-3 py-3 dark:border-white/12 dark:bg-black/26">
                        <div className="text-[11px] font-medium uppercase tracking-[0.2em] text-slate-700 dark:text-white/60">Composer</div>
                        <div className="mt-1 text-sm text-slate-900 line-clamp-2 dark:text-white">{joinClassical(data.classical.composer, 3)}</div>
                      </div>
                    ) : null}
                    {joinClassical(data.classical.work, 3) ? (
                      <div className="border border-border/70 bg-white/45 px-3 py-3 dark:border-white/12 dark:bg-black/26">
                        <div className="text-[11px] font-medium uppercase tracking-[0.2em] text-slate-700 dark:text-white/60">Work</div>
                        <div className="mt-1 text-sm text-slate-900 line-clamp-2 dark:text-white">{joinClassical(data.classical.work, 3)}</div>
                      </div>
                    ) : null}
                    {joinClassical(data.classical.conductor, 3) ? (
                      <div className="border border-border/70 bg-white/45 px-3 py-3 dark:border-white/12 dark:bg-black/26">
                        <div className="text-[11px] font-medium uppercase tracking-[0.2em] text-slate-700 dark:text-white/60">Conductor</div>
                        <div className="mt-1 text-sm text-slate-900 line-clamp-2 dark:text-white">{joinClassical(data.classical.conductor, 3)}</div>
                      </div>
                    ) : null}
                    {joinClassical(data.classical.orchestra, 3) ? (
                      <div className="border border-border/70 bg-white/45 px-3 py-3 dark:border-white/12 dark:bg-black/26">
                        <div className="text-[11px] font-medium uppercase tracking-[0.2em] text-slate-700 dark:text-white/60">Orchestra</div>
                        <div className="mt-1 text-sm text-slate-900 line-clamp-2 dark:text-white">{joinClassical(data.classical.orchestra, 3)}</div>
                      </div>
                    ) : null}
                    {joinClassical(data.classical.soloists, 3) ? (
                      <div className="border border-border/70 bg-white/45 px-3 py-3 dark:border-white/12 dark:bg-black/26">
                        <div className="text-[11px] font-medium uppercase tracking-[0.2em] text-slate-700 dark:text-white/60">Soloists</div>
                        <div className="mt-1 text-sm text-slate-900 line-clamp-2 dark:text-white">{joinClassical(data.classical.soloists, 3)}</div>
                      </div>
                    ) : null}
                    {joinClassical(data.classical.catalog_numbers, 3) ? (
                      <div className="border border-border/70 bg-white/45 px-3 py-3 dark:border-white/12 dark:bg-black/26">
                        <div className="text-[11px] font-medium uppercase tracking-[0.2em] text-slate-700 dark:text-white/60">Catalog</div>
                        <div className="mt-1 text-sm text-slate-900 line-clamp-2 dark:text-white">{joinClassical(data.classical.catalog_numbers, 3)}</div>
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </div>
            </div>
          </div>
        </div>
        <CardContent className="pt-4 pb-5 space-y-4">
          <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr),minmax(0,1fr)] gap-4">
            <div className="border border-border/60 bg-background/35 p-4 space-y-2">
              <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-muted-foreground">Your rating</div>
              <div className="flex items-center gap-3">
                <AlbumRatingStars
                  value={ratings.user_rating}
                  editable={!savingUserRating}
                  onChange={(value) => void handleUserRatingChange(value)}
                  size={18}
                />
                {savingUserRating ? <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" /> : null}
              </div>
              <p className="text-xs text-muted-foreground">
                Rate this album from 1 to 5 stars. Click the same value again to clear it.
              </p>
            </div>

            <div className="border border-border/60 bg-background/35 p-4 space-y-3">
              <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-muted-foreground">Public pulse</div>
              <div className="flex flex-wrap items-center gap-2">
                <AlbumRatingStars value={ratings.public_rating} size={18} />
                {ratings.public_rating_source && publicRatingSourceId !== 'unknown' ? (
                  <ProviderBadge provider={ratings.public_rating_source} className="text-[11px]" />
                ) : null}
                {Number(ratings.public_rating_votes || 0) > 0 ? (
                  <Badge variant="outline" className={cn('text-[11px]', badgeKindClass('count'))}>
                    {formatCompactCount(ratings.public_rating_votes)} vote{Number(ratings.public_rating_votes || 0) > 1 ? 's' : ''}
                  </Badge>
                ) : null}
              </div>
              <div className="flex flex-wrap items-center gap-1.5">
                {Number(ratingSignals.discogs_have_count || 0) > 0 ? (
                  <Badge variant="outline" className={cn('gap-1 text-[11px]', badgeKindClass('label'))}>
                    <Disc3 className="h-3 w-3" />
                    {formatCompactCount(ratingSignals.discogs_have_count)} owned
                  </Badge>
                ) : null}
                {Number(ratingSignals.discogs_want_count || 0) > 0 ? (
                  <Badge variant="outline" className={cn('gap-1 text-[11px]', badgeKindClass('source'))}>
                    <Users className="h-3 w-3" />
                    {formatCompactCount(ratingSignals.discogs_want_count)} wanted
                  </Badge>
                ) : null}
                {Number(ratingSignals.bandcamp_supporter_count || 0) > 0 ? (
                  <Badge variant="outline" className={cn('gap-1 text-[11px]', badgeKindClass('genre'))}>
                    <Users className="h-3 w-3" />
                    {formatCompactCount(ratingSignals.bandcamp_supporter_count)} supporters
                  </Badge>
                ) : null}
                {Number(ratingSignals.lastfm_scrobbles || 0) > 0 ? (
                  <Badge variant="outline" className={cn('gap-1 text-[11px]', badgeKindClass('duration'))}>
                    <Users className="h-3 w-3" />
                    {formatCompactCount(ratingSignals.lastfm_scrobbles)} scrobbles
                  </Badge>
                ) : null}
                {Number(ratingSignals.lastfm_listeners || 0) > 0 ? (
                  <Badge variant="outline" className={cn('gap-1 text-[11px]', badgeKindClass('count'))}>
                    <Users className="h-3 w-3" />
                    {formatCompactCount(ratingSignals.lastfm_listeners)} listeners
                  </Badge>
                ) : null}
              </div>
              <SocialActivityBadges
                entityType="album"
                entityId={data.album_id}
                compact
              />
            </div>
          </div>
          <div className="space-y-2">
            <div className="text-xs font-medium text-muted-foreground">Review</div>
            {reviewText ? (
              <Collapsible open={reviewExpanded} onOpenChange={setReviewExpanded}>
                {!reviewExpanded ? (
                  <p className="text-sm leading-relaxed text-muted-foreground whitespace-pre-wrap line-clamp-5">{reviewText}</p>
                ) : null}
                <CollapsibleContent>
                  <p className="text-sm leading-relaxed text-muted-foreground whitespace-pre-wrap">{reviewText}</p>
                </CollapsibleContent>
                <div className="flex flex-wrap items-center justify-between gap-3">
                  {reviewSource ? (
                    <ProviderBadge provider={reviewSource} prefix="Source" className="text-[11px]" />
                  ) : (
                    <span />
                  )}
                  {showReviewToggle ? (
                    <CollapsibleTrigger asChild>
                      <Button type="button" variant="ghost" size="sm" className="px-0">
                        {reviewExpanded ? 'Show less' : 'Show more'}
                      </Button>
                    </CollapsibleTrigger>
                  ) : null}
                </div>
              </Collapsible>
            ) : (
              <p className="text-sm text-muted-foreground">No review snippet available for this album yet.</p>
            )}
          </div>
        </CardContent>
      </Card>

      <Card className="border-border/70">
        <CardContent className="p-5 space-y-3">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="min-w-0">
              <h2 className="pmda-section-title">Tracks</h2>
              <p className="text-xs text-muted-foreground mt-1">
                {trackRows.length > 0 ? `${trackRows.length} track${trackRows.length !== 1 ? 's' : ''}` : ' '}
              </p>
            </div>
            <div className="flex items-center gap-2">
              <Button
                type="button"
                size="icon"
                className="h-10 w-10 rounded-full"
                onClick={handlePlay}
                disabled={playbackTracks.length === 0}
                title="Play album"
              >
                <Play className="h-4 w-4 fill-current" />
              </Button>
              {trackDetailsLoading ? <Loader2 className="w-4 h-4 animate-spin text-muted-foreground" /> : null}
            </div>
          </div>
          {trackDetailsError ? (
            <p className="text-xs text-destructive">{trackDetailsError}</p>
          ) : null}

          {trackRows.length === 0 ? (
            <p className="text-sm text-muted-foreground">No tracks found for this album.</p>
          ) : (
            <div className="overflow-x-auto rounded-md border border-border/60">
              <Table className="min-w-[760px]">
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="h-10 w-[70px] text-center text-[11px] uppercase tracking-wide">
                      <Play className="mx-auto h-3.5 w-3.5 text-emerald-400 fill-emerald-400" />
                    </TableHead>
                    <TableHead className="h-10 w-[80px] text-[11px] uppercase tracking-wide">#</TableHead>
                    <TableHead className="hidden h-10 text-[11px] uppercase tracking-wide md:table-cell">Artist</TableHead>
                    <TableHead className="h-10 min-w-[260px] text-[11px] uppercase tracking-wide">Track name</TableHead>
                    <TableHead className="h-10 w-[90px] text-right text-[11px] uppercase tracking-wide">Duration</TableHead>
                    <TableHead className="h-10 w-[78px] text-right text-[11px] uppercase tracking-wide">Queue</TableHead>
                    <TableHead className="h-10 w-[90px] text-right text-[11px] uppercase tracking-wide">Detail</TableHead>
                    {isAdmin ? (
                      <TableHead className="h-10 w-[92px] text-right text-[11px] uppercase tracking-wide">Playlist</TableHead>
                    ) : null}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {trackGroups.map((group) => (
                    <Fragment key={group.key}>
                      {showDiscSections ? (
                        <TableRow className="bg-muted/30 hover:bg-muted/30">
                          <TableCell colSpan={isAdmin ? 8 : 7} className="py-2 px-4">
                            <div className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">
                              {group.label}
                            </div>
                          </TableCell>
                        </TableRow>
                      ) : null}
                      {group.tracks.map((t, idx) => {
                    const canStream = playbackByTrackId.has(t.track_id);
                    const detail = trackDetailsById[t.track_id];
                    const detailOpen = expandedTrackId === t.track_id;
                    const detailTagEntries = Object.entries(detail?.tags || {}).sort((a, b) => a[0].localeCompare(b[0]));
                    const detailsColspan = isAdmin ? 8 : 7;
                    return (
                      <Fragment key={`alb-tr-${t.track_id || `${group.key}-${idx}`}`}>
                        <TableRow>
                          <TableCell className="py-3 text-center">
                            <Button
                              type="button"
                              size="icon"
                              variant="ghost"
                              className="h-8 w-8 rounded-full text-emerald-400 hover:bg-emerald-500/10 hover:text-emerald-300"
                              onClick={() => handlePlayTrack(t.track_id)}
                              title="Play track"
                              disabled={!canStream}
                            >
                              <Play className="h-4 w-4 fill-current" />
                            </Button>
                          </TableCell>
                          <TableCell className="py-3 text-xs tabular-nums text-muted-foreground">{t.display_num}</TableCell>
                          <TableCell className="hidden py-3 md:table-cell">
                            <div className="max-w-[220px] truncate text-sm text-muted-foreground">{t.display_artist}</div>
                          </TableCell>
                          <TableCell className="py-3">
                            <div className="text-sm font-medium truncate">{t.display_title || 'Untitled'}</div>
                            <div className="flex items-center gap-1.5 flex-wrap mt-1">
                              {t.featured ? (
                                <Badge variant="outline" className={cn("text-[11px]", badgeKindClass('genre'))}>
                                  feat. {t.featured}
                                </Badge>
                              ) : null}
                              {t.format ? (
                                <Badge variant="outline" className={cn("text-[11px]", badgeKindClass('track_meta'))}>
                                  {t.format.toUpperCase()}
                                </Badge>
                              ) : null}
                              {t.sample_rate ? (
                                <Badge variant="outline" className={cn("text-[11px]", badgeKindClass('track_meta'))}>
                                  {Math.round((t.sample_rate || 0) / 100) / 10}kHz
                                </Badge>
                              ) : null}
                              {t.bit_depth ? (
                                <Badge variant="outline" className={cn("text-[11px]", badgeKindClass('track_meta'))}>
                                  {t.bit_depth}bit
                                </Badge>
                              ) : null}
                            </div>
                          </TableCell>
                          <TableCell className="py-3 text-right text-xs tabular-nums text-muted-foreground">
                            {Number(t.duration_sec || 0) > 0 ? formatDuration(t.duration_sec || 0) : '—'}
                          </TableCell>
                          <TableCell className="py-3 text-right">
                            <Button
                              type="button"
                              size="icon"
                              variant="ghost"
                              className="h-8 w-8 rounded-full"
                              onClick={() => handleQueueTrack(t.track_id)}
                              title="Queue track"
                              disabled={!canStream}
                            >
                              <ListPlus className="h-4 w-4" />
                            </Button>
                          </TableCell>
                          <TableCell className="py-3 text-right">
                            <Button
                              type="button"
                              size="sm"
                              variant="ghost"
                              className="h-8 px-2.5"
                              onClick={() => {
                                if (!trackDetailsLoading && Object.keys(trackDetailsById).length === 0) {
                                  void loadTrackDetails();
                                }
                                setExpandedTrackId((prev) => (prev === t.track_id ? null : t.track_id));
                              }}
                            >
                              {detailOpen ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
                            </Button>
                          </TableCell>
                          {isAdmin ? (
                            <TableCell className="py-3 text-right">
                              <DropdownMenu>
                                <DropdownMenuTrigger asChild>
                                  <Button
                                    type="button"
                                    size="icon"
                                    variant="ghost"
                                    className="h-8 w-8 rounded-full"
                                    title="Add to playlist"
                                    disabled={!canStream || addingTrackId === t.track_id}
                                  >
                                    {addingTrackId === t.track_id ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
                                  </Button>
                                </DropdownMenuTrigger>
                                <DropdownMenuContent align="end" className="w-64">
                                  <DropdownMenuLabel>Add to playlist</DropdownMenuLabel>
                                  <DropdownMenuSeparator />
                                  {playlistsLoading ? (
                                    <DropdownMenuItem disabled>Loading playlists…</DropdownMenuItem>
                                  ) : playlists.length === 0 ? (
                                    <DropdownMenuItem
                                      onSelect={() =>
                                        navigate('/library/playlists', {
                                          state: { playlistSeed: { track_id: t.track_id } },
                                        })
                                      }
                                    >
                                      Create a playlist
                                    </DropdownMenuItem>
                                  ) : (
                                    playlists.slice(0, 20).map((pl) => (
                                      <DropdownMenuItem
                                        key={`pl-add-${pl.playlist_id}`}
                                        onSelect={() => void handleAddTrackToPlaylist(t.track_id, pl.playlist_id)}
                                      >
                                        <span className="truncate">{pl.name}</span>
                                        <span className="ml-auto text-xs text-muted-foreground">{pl.item_count}</span>
                                      </DropdownMenuItem>
                                    ))
                                  )}
                                  <DropdownMenuSeparator />
                                  <DropdownMenuItem onSelect={() => navigate('/library/playlists')}>
                                    Manage playlists
                                  </DropdownMenuItem>
                                </DropdownMenuContent>
                              </DropdownMenu>
                            </TableCell>
                          ) : null}
                        </TableRow>
                        {detailOpen ? (
                          <TableRow className="bg-muted/20">
                            <TableCell colSpan={detailsColspan} className="py-3 px-4">
                              <div className="space-y-3">
                                <div className="text-xs">
                                  <span className="text-muted-foreground">Path:</span>{' '}
                                  <code className="rounded bg-muted px-1.5 py-0.5 text-[11px]">{detail?.file_path || t.file_path || '—'}</code>
                                </div>
                                <div className="flex flex-wrap gap-1.5 text-[11px]">
                                  <Badge variant="outline" className={badgeKindClass('track_meta')}>Format: {(detail?.format || t.format || '—').toUpperCase()}</Badge>
                                  <Badge variant="outline" className={badgeKindClass('track_meta')}>Bitrate: {detail?.bitrate || t.bitrate || 0} kb/s</Badge>
                                  <Badge variant="outline" className={badgeKindClass('track_meta')}>
                                    Sample rate: {Math.round(((detail?.sample_rate || t.sample_rate || 0) / 100) || 0) / 10} kHz
                                  </Badge>
                                  <Badge variant="outline" className={badgeKindClass('track_meta')}>Bit depth: {detail?.bit_depth || t.bit_depth || 0} bit</Badge>
                                  <Badge variant="outline" className={badgeKindClass('track_meta')}>Size: {formatBytes(detail?.file_size_bytes || t.file_size_bytes || 0)}</Badge>
                                </div>
                                {detail?.tags_error ? (
                                  <p className="text-xs text-destructive">Tag inspector: {detail.tags_error}</p>
                                ) : null}
                                <div className="space-y-2">
                                  <div className="text-xs font-medium">All file tags ({detailTagEntries.length})</div>
                                  {detailTagEntries.length === 0 ? (
                                    <p className="text-xs text-muted-foreground">
                                      {trackDetailsLoading ? 'Loading tags…' : 'No tag data available for this track.'}
                                    </p>
                                  ) : (
                                    <div className="grid grid-cols-1 md:grid-cols-2 gap-x-4 gap-y-1.5">
                                      {detailTagEntries.map(([k, v]) => (
                                        <div key={`tag-${t.track_id}-${k}`} className="text-[11px] leading-relaxed">
                                          <span className="font-medium">{k}</span>: <span className="text-muted-foreground break-all">{v}</span>
                                        </div>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              </div>
                            </TableCell>
                          </TableRow>
                        ) : null}
                      </Fragment>
                    );
                  })}
                    </Fragment>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      <MatchDetailDialog
        open={matchDialogOpen}
        onOpenChange={setMatchDialogOpen}
        entity={{ kind: 'album', albumId: data.album_id }}
        onDataChanged={() => {
          void load();
        }}
      />
      <AlbumArtworkGalleryDialog
        albumId={data.album_id}
        albumTitle={data.title}
        open={artworkGalleryOpen}
        onOpenChange={setArtworkGalleryOpen}
      />
    </div>
  );
}
