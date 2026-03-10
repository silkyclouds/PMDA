import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, Calendar, ChevronDown, ChevronUp, Download, Info, ListPlus, Loader2, Music, Pencil, Play, Plus, Sparkles } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { FormatBadge } from '@/components/FormatBadge';
import { MatchDetailDialog } from '@/components/library/MatchDetailDialog';
import { ProviderBadge } from '@/components/providers/ProviderBadge';
import { ProviderLink } from '@/components/providers/ProviderLink';
import * as api from '@/lib/api';
import { badgeKindClass } from '@/lib/badgeStyles';
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

function normalizeArtistName(value: string): string {
  return (value || '').trim().toLowerCase().replace(/\s+/g, ' ');
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

export default function AlbumPage() {
  const navigate = useNavigate();
  const params = useParams<{ albumId: string }>();
  const albumId = Number(params.albumId);
  const { startPlayback, setCurrentTrack, queueTrack } = usePlayback();
  const { isAdmin, canDownload } = useAuth();
  const { toast } = useToast();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<api.AlbumDetailResponse | null>(null);
  const [reviewExpanded, setReviewExpanded] = useState(false);
  const [playlistsLoading, setPlaylistsLoading] = useState(false);
  const [playlists, setPlaylists] = useState<api.PlaylistSummary[]>([]);
  const [addingTrackId, setAddingTrackId] = useState<number | null>(null);
  const [downloadingAlbum, setDownloadingAlbum] = useState(false);
  const [aiReviewBusy, setAiReviewBusy] = useState(false);
  const [matchDialogOpen, setMatchDialogOpen] = useState(false);
  const [trackDetailsLoading, setTrackDetailsLoading] = useState(false);
  const [trackDetailsError, setTrackDetailsError] = useState<string | null>(null);
  const [expandedTrackId, setExpandedTrackId] = useState<number | null>(null);
  const [trackDetailsById, setTrackDetailsById] = useState<Record<number, api.AlbumTrackDetailItem>>({});
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

  const trackRows = useMemo(() => {
    const tracks = data?.tracks || [];
    const albumArtist = data?.artist_name || '';
    const inlineArtistMode = shouldSplitInlineTrackArtist(tracks);
    return tracks.map((t, idx) => {
      const parsed = splitInlineTrackArtistTitle(t.title || '', albumArtist, inlineArtistMode);
      const n = t.track_num > 0 ? t.track_num : idx + 1;
      return {
        ...t,
        display_num: t.disc_num > 1 ? `${t.disc_num}.${n}` : String(n),
        display_artist: (parsed.artist || albumArtist || 'Unknown artist').trim(),
        display_title: (parsed.title || t.title || `Track ${idx + 1}`).trim(),
      };
    });
  }, [data?.artist_name, data?.tracks]);

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

  const handleGenerateAiReview = useCallback(async () => {
    if (!data || aiReviewBusy) return;
    setAiReviewBusy(true);
    try {
      const res = await api.generateAlbumReview(data.album_id);
      await load();
      toast({
        title: 'Album review generated',
        description: res.source ? `Source: ${res.source}` : 'Review updated.',
      });
    } catch (e) {
      const apiMsg =
        (e && typeof e === 'object' && 'body' in e && typeof (e as { body?: unknown }).body === 'object'
          ? String(
              ((e as { body?: Record<string, unknown> }).body?.error
                || (e as { body?: Record<string, unknown> }).body?.detail
                || (e as { body?: Record<string, unknown> }).body?.message
                || '')
            ).trim()
          : '') || '';
      toast({
        title: 'AI review failed',
        description: apiMsg || (e instanceof Error ? e.message : 'Unable to generate album review'),
        variant: 'destructive',
      });
    } finally {
      setAiReviewBusy(false);
    }
  }, [aiReviewBusy, data, load, toast]);

  if (loading) {
    return (
      <div className="container py-8">
        <div className="flex items-center justify-center py-24">
          <Loader2 className="w-10 h-10 animate-spin text-primary" />
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="container py-8">
        <Card className="border-border/70">
          <CardContent className="p-8 space-y-4 text-center">
            <p className="text-muted-foreground">{error || 'Album not found'}</p>
            <Button variant="outline" onClick={() => navigate('/library')}>
              Back to Library
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

  return (
    <div className="container py-4 md:py-6 space-y-5 md:space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <Button variant="ghost" className="gap-2" onClick={() => navigate(`/library/artist/${data.artist_id}`)}>
          <ArrowLeft className="w-4 h-4" />
          Back to Artist
        </Button>
        <div className="flex flex-wrap items-center gap-2">
          {data.label ? (
            <Button
              size="sm"
              variant="outline"
              className="h-8"
              onClick={() => navigate(`/library/label/${encodeURIComponent(data.label || '')}`)}
              title="Open label"
            >
              {data.label}
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
          <Button
            size="sm"
            variant="outline"
            className="h-8 gap-2"
            onClick={() => setMatchDialogOpen(true)}
          >
            <Info className="w-4 h-4" />
            Match detail
          </Button>
          <Button
            size="sm"
            variant="outline"
            className="h-8 gap-2"
            onClick={() => void handleGenerateAiReview()}
            disabled={aiReviewBusy}
            title="Generate album review via AI with safety checks"
          >
            {aiReviewBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
            AI review
          </Button>
          <Button size="sm" className="h-8 gap-2" onClick={handlePlay} disabled={playbackTracks.length === 0}>
            <Play className="w-4 h-4" />
            Play
          </Button>
        </div>
      </div>

      <Card className="overflow-hidden border-border/70">
        <div className="relative overflow-hidden">
          <div className="absolute inset-0 z-10 bg-background/18 backdrop-blur-md" />
          <div className="absolute inset-0 z-10 bg-gradient-to-r from-background/76 via-background/58 to-background/42" />
          {data.cover_url ? (
            <img src={data.cover_url} alt={data.title} className="w-full h-64 md:h-72 object-cover blur-[1.4px] scale-[1.06]" />
          ) : (
            <div className="h-64 md:h-72 bg-gradient-to-br from-muted via-muted/70 to-accent/20" />
          )}
          <div className="absolute inset-0 z-20 p-6 md:p-8 flex items-end">
            <div className="grid grid-cols-1 md:grid-cols-[12rem,1fr] gap-5 w-full items-end">
              <div className="w-36 h-36 md:w-48 md:h-48 rounded-3xl overflow-hidden border border-border bg-muted shrink-0 shadow-md">
                <div className="relative w-full h-full group">
                  {data.cover_url ? (
                    <img src={data.cover_url} alt={data.title} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center">
                      <Music className="w-8 h-8 text-muted-foreground" />
                    </div>
                  )}
                  <Button
                    type="button"
                    size="icon"
                    variant="secondary"
                    className="absolute right-1.5 bottom-1.5 h-7 w-7 opacity-0 group-hover:opacity-100 transition-opacity"
                    title="Edit cover source"
                    onClick={() => setMatchDialogOpen(true)}
                  >
                    <Pencil className="w-3.5 h-3.5" />
                  </Button>
                </div>
              </div>
              <div className="min-w-0">
                <h1 className="text-2xl md:text-4xl font-bold tracking-tight truncate">{data.title}</h1>
                <button
                  type="button"
                  className="text-sm text-muted-foreground mt-1 hover:underline truncate"
                  onClick={() => navigate(`/library/artist/${data.artist_id}`)}
                  title="Open artist"
                >
                  {data.artist_name}
                </button>

                <div className="flex flex-wrap items-center gap-2 mt-4">
                  {data.year ? (
                    <Badge variant="outline" className={cn("gap-1.5 text-[10px]", badgeKindClass('year'))}>
                      <Calendar className="w-3 h-3" />
                      {data.year}
                    </Badge>
                  ) : null}
                  <Badge variant="outline" className={cn("text-[10px]", badgeKindClass('count'))}>
                    {data.track_count} tracks
                  </Badge>
                  <Badge variant="outline" className={cn("text-[10px]", badgeKindClass('duration'))}>
                    {formatDuration(data.total_duration_sec || 0)}
                  </Badge>
                  {data.format ? <FormatBadge format={data.format} size="sm" /> : null}
                  <Badge
                    variant="outline"
                    className={cn("text-[10px]", data.is_lossless ? badgeKindClass('lossless') : badgeKindClass('lossy'))}
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
                      <ProviderBadge provider={data.metadata_source} prefix="Source" className="text-[10px]" />
                    )
                  ) : null}
                  {data.bandcamp_album_url ? (
                    <ProviderLink provider="bandcamp" href={data.bandcamp_album_url} className="inline-flex" />
                  ) : null}
                </div>
                {genreBadges.length > 0 ? (
                  <div className="mt-3 flex flex-wrap gap-1.5">
                    {genreBadges.map((genre) => (
                      <Badge
                        key={`album-genre-${genre}`}
                        variant="outline"
                        className={cn("text-[10px] cursor-pointer", badgeKindClass('genre'))}
                        onClick={() => navigate(`/library/genre/${encodeURIComponent(genre)}`)}
                      >
                        {genre}
                      </Badge>
                    ))}
                  </div>
                ) : null}
              </div>
            </div>
          </div>
        </div>
        <CardContent className="pt-4 pb-5 space-y-4">
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
                    <ProviderBadge provider={reviewSource} prefix="Source" className="text-[10px]" />
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
              <h2 className="text-lg font-semibold">Tracks</h2>
              <p className="text-xs text-muted-foreground mt-1">
                {trackRows.length > 0 ? `${trackRows.length} track${trackRows.length !== 1 ? 's' : ''}` : ' '}
              </p>
            </div>
            <div className="flex items-center gap-2">
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
              <Table className="min-w-[920px]">
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="h-10 w-[80px] text-[10px] uppercase tracking-wide">#</TableHead>
                    <TableHead className="h-10 text-[10px] uppercase tracking-wide">Artist</TableHead>
                    <TableHead className="h-10 w-[70px] text-right text-[10px] uppercase tracking-wide">Play</TableHead>
                    <TableHead className="h-10 min-w-[320px] text-[10px] uppercase tracking-wide">Track name</TableHead>
                    <TableHead className="h-10 w-[90px] text-right text-[10px] uppercase tracking-wide">Duration</TableHead>
                    <TableHead className="h-10 w-[78px] text-right text-[10px] uppercase tracking-wide">Queue</TableHead>
                    <TableHead className="h-10 w-[90px] text-right text-[10px] uppercase tracking-wide">Detail</TableHead>
                    {isAdmin ? (
                      <TableHead className="h-10 w-[92px] text-right text-[10px] uppercase tracking-wide">Playlist</TableHead>
                    ) : null}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {trackRows.map((t, idx) => {
                    const canStream = playbackByTrackId.has(t.track_id);
                    const detail = trackDetailsById[t.track_id];
                    const detailOpen = expandedTrackId === t.track_id;
                    const detailTagEntries = Object.entries(detail?.tags || {}).sort((a, b) => a[0].localeCompare(b[0]));
                    const detailsColspan = isAdmin ? 8 : 7;
                    return (
                      <Fragment key={`alb-tr-${t.track_id || idx}`}>
                        <TableRow>
                          <TableCell className="py-3 text-xs tabular-nums text-muted-foreground">{t.display_num}</TableCell>
                          <TableCell className="py-3">
                            <div className="max-w-[220px] truncate text-sm text-muted-foreground">{t.display_artist}</div>
                          </TableCell>
                          <TableCell className="py-3 text-right">
                            <Button
                              type="button"
                              size="icon"
                              variant="ghost"
                              className="h-8 w-8 rounded-full"
                              onClick={() => handlePlayTrack(t.track_id)}
                              title="Play track"
                              disabled={!canStream}
                            >
                              <Play className="h-4 w-4" />
                            </Button>
                          </TableCell>
                          <TableCell className="py-3">
                            <div className="text-sm font-medium truncate">{t.display_title || 'Untitled'}</div>
                            <div className="flex items-center gap-1.5 flex-wrap mt-1">
                              {t.featured ? (
                                <Badge variant="outline" className={cn("text-[10px]", badgeKindClass('genre'))}>
                                  feat. {t.featured}
                                </Badge>
                              ) : null}
                              {t.format ? (
                                <Badge variant="outline" className={cn("text-[10px]", badgeKindClass('track_meta'))}>
                                  {t.format.toUpperCase()}
                                </Badge>
                              ) : null}
                              {t.sample_rate ? (
                                <Badge variant="outline" className={cn("text-[10px]", badgeKindClass('track_meta'))}>
                                  {Math.round((t.sample_rate || 0) / 100) / 10}kHz
                                </Badge>
                              ) : null}
                              {t.bit_depth ? (
                                <Badge variant="outline" className={cn("text-[10px]", badgeKindClass('track_meta'))}>
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
                                    <DropdownMenuItem onSelect={() => navigate('/library/playlists')}>
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
    </div>
  );
}
