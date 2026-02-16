import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, Calendar, ExternalLink, ListPlus, Loader2, Music, Play, Plus } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { FormatBadge } from '@/components/FormatBadge';
import * as api from '@/lib/api';
import { useToast } from '@/hooks/use-toast';
import { usePlayback } from '@/contexts/PlaybackContext';
import type { TrackInfo } from '@/components/library/AudioPlayer';

function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds || 0));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}:${m.toString().padStart(2, '0')}:${sec.toString().padStart(2, '0')}`;
  return `${m}:${sec.toString().padStart(2, '0')}`;
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

export default function AlbumPage() {
  const navigate = useNavigate();
  const params = useParams<{ albumId: string }>();
  const albumId = Number(params.albumId);
  const { startPlayback, setCurrentTrack, queueTrack } = usePlayback();
  const { toast } = useToast();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<api.AlbumDetailResponse | null>(null);
  const [reviewExpanded, setReviewExpanded] = useState(false);
  const [playlistsLoading, setPlaylistsLoading] = useState(false);
  const [playlists, setPlaylists] = useState<api.PlaylistSummary[]>([]);
  const [addingTrackId, setAddingTrackId] = useState<number | null>(null);

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

  return (
    <div className="container py-6 space-y-6">
      <div className="flex items-center justify-between gap-3">
        <Button variant="ghost" className="gap-2" onClick={() => navigate(`/library/artist/${data.artist_id}`)}>
          <ArrowLeft className="w-4 h-4" />
          Back to Artist
        </Button>
        <div className="flex items-center gap-2">
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
          <Button size="sm" className="h-8 gap-2" onClick={handlePlay} disabled={playbackTracks.length === 0}>
            <Play className="w-4 h-4" />
            Play
          </Button>
        </div>
      </div>

      <Card className="overflow-hidden border-border/70">
        <div className="relative">
          <div className="absolute inset-0 bg-gradient-to-r from-background via-background/90 to-background/70 z-10" />
          {data.cover_url ? (
            <img src={data.cover_url} alt={data.title} className="w-full h-64 object-cover blur-[1px] scale-105" />
          ) : (
            <div className="h-64 bg-gradient-to-br from-zinc-900 via-zinc-800 to-zinc-900" />
          )}
          <div className="absolute inset-0 z-20 p-6 md:p-8 flex items-end">
            <div className="grid grid-cols-1 md:grid-cols-[8.5rem,1fr] gap-5 w-full items-end">
              <div className="w-28 h-28 md:w-36 md:h-36 rounded-3xl overflow-hidden border border-border/60 bg-muted shrink-0 shadow-sm">
                {data.cover_url ? (
                  <img src={data.cover_url} alt={data.title} className="w-full h-full object-cover animate-in fade-in-0 duration-300" />
                ) : (
                  <div className="w-full h-full flex items-center justify-center">
                    <Music className="w-8 h-8 text-muted-foreground" />
                  </div>
                )}
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
                    <Badge variant="outline" className="gap-1.5 text-[10px]">
                      <Calendar className="w-3 h-3" />
                      {data.year}
                    </Badge>
                  ) : null}
                  <Badge variant="outline" className="text-[10px]">
                    {data.track_count} tracks
                  </Badge>
                  <Badge variant="outline" className="text-[10px]">
                    {formatDuration(data.total_duration_sec || 0)}
                  </Badge>
                  {data.format ? <FormatBadge format={data.format} size="sm" /> : null}
                  <Badge variant={data.is_lossless ? 'secondary' : 'outline'} className="text-[10px]">
                    {data.is_lossless ? 'Lossless' : 'Lossy'}
                  </Badge>
                  {data.metadata_source ? (
                    <Badge variant="outline" className="text-[10px]">
                      Source: {data.metadata_source}
                    </Badge>
                  ) : null}
                  {data.bandcamp_album_url ? (
                    <a
                      href={data.bandcamp_album_url}
                      target="_blank"
                      rel="noreferrer"
                      className="inline-flex"
                      title="Open on Bandcamp"
                    >
                      <Badge variant="outline" className="gap-1.5 text-[10px] hover:bg-muted transition-colors">
                        <ExternalLink className="w-3 h-3" />
                        Bandcamp
                      </Badge>
                    </a>
                  ) : null}
                </div>
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
                <div className="flex items-center justify-between gap-3">
                  {reviewSource ? (
                    <Badge variant="outline" className="text-[10px]">
                      Source: {reviewSource}
                    </Badge>
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
          <div className="flex items-center justify-between gap-3">
            <div className="min-w-0">
              <h2 className="text-lg font-semibold">Tracks</h2>
              <p className="text-xs text-muted-foreground mt-1">
                {trackRows.length > 0 ? `${trackRows.length} track${trackRows.length !== 1 ? 's' : ''}` : ' '}
              </p>
            </div>
          </div>

          {trackRows.length === 0 ? (
            <p className="text-sm text-muted-foreground">No tracks found for this album.</p>
          ) : (
            <ScrollArea className="h-[460px] md:h-[600px]">
              <Table className="min-w-[920px]">
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="h-10 w-[80px] text-[10px] uppercase tracking-wide">#</TableHead>
                    <TableHead className="h-10 text-[10px] uppercase tracking-wide">Artist</TableHead>
                    <TableHead className="h-10 min-w-[320px] text-[10px] uppercase tracking-wide">Track name</TableHead>
                    <TableHead className="h-10 w-[90px] text-right text-[10px] uppercase tracking-wide">Duration</TableHead>
                    <TableHead className="h-10 w-[70px] text-right text-[10px] uppercase tracking-wide">Play</TableHead>
                    <TableHead className="h-10 w-[78px] text-right text-[10px] uppercase tracking-wide">Queue</TableHead>
                    <TableHead className="h-10 w-[92px] text-right text-[10px] uppercase tracking-wide">Playlist</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {trackRows.map((t, idx) => {
                    const canStream = playbackByTrackId.has(t.track_id);
                    return (
                      <TableRow key={`alb-tr-${t.track_id || idx}`}>
                        <TableCell className="py-3 text-xs tabular-nums text-muted-foreground">{t.display_num}</TableCell>
                        <TableCell className="py-3">
                          <div className="max-w-[220px] truncate text-sm text-muted-foreground">{t.display_artist}</div>
                        </TableCell>
                        <TableCell className="py-3">
                          <div className="text-sm font-medium truncate">{t.display_title || 'Untitled'}</div>
                          <div className="flex items-center gap-1.5 flex-wrap mt-1">
                            {t.featured ? (
                              <Badge variant="secondary" className="text-[10px]">
                                feat. {t.featured}
                              </Badge>
                            ) : null}
                            {t.format ? (
                              <Badge variant="outline" className="text-[10px]">
                                {t.format.toUpperCase()}
                              </Badge>
                            ) : null}
                            {t.sample_rate ? (
                              <Badge variant="outline" className="text-[10px]">
                                {Math.round((t.sample_rate || 0) / 100) / 10}kHz
                              </Badge>
                            ) : null}
                            {t.bit_depth ? (
                              <Badge variant="outline" className="text-[10px]">
                                {t.bit_depth}bit
                              </Badge>
                            ) : null}
                          </div>
                        </TableCell>
                        <TableCell className="py-3 text-right text-xs tabular-nums text-muted-foreground">
                          {formatDuration(t.duration_sec || 0)}
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
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </ScrollArea>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
