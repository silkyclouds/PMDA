import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, Calendar, ExternalLink, Loader2, Music, Play } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { FormatBadge } from '@/components/FormatBadge';
import { cn } from '@/lib/utils';
import * as api from '@/lib/api';
import { usePlayback } from '@/contexts/PlaybackContext';

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

export default function AlbumPage() {
  const navigate = useNavigate();
  const params = useParams<{ albumId: string }>();
  const albumId = Number(params.albumId);
  const { startPlayback } = usePlayback();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<api.AlbumDetailResponse | null>(null);
  const [reviewExpanded, setReviewExpanded] = useState(false);

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

  const playbackTracks = useMemo(() => {
    const tracks = data?.tracks || [];
    const albumTitle = data?.title || 'Album';
    const artistName = data?.artist_name || '';
    return tracks
      .filter((t) => (t.track_id || 0) > 0)
      .map((t, idx) => ({
        track_id: t.track_id,
        title: t.title || `Track ${idx + 1}`,
        artist: artistName,
        album: albumTitle,
        duration: t.duration_sec || 0,
        // Keep player behavior consistent: use track_num when present; fall back to sequential index.
        index: t.track_num > 0 ? t.track_num : idx + 1,
        file_url: t.file_url,
      }));
  }, [data]);

  const handlePlay = () => {
    if (!data) return;
    if (playbackTracks.length === 0) return;
    startPlayback(data.album_id, data.title || 'Album', data.cover_url || null, playbackTracks);
  };

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
                {data.tracks.length > 0 ? `${data.tracks.length} track${data.tracks.length !== 1 ? 's' : ''}` : ' '}
              </p>
            </div>
          </div>

          {data.tracks.length === 0 ? (
            <p className="text-sm text-muted-foreground">No tracks found for this album.</p>
          ) : (
            <ScrollArea className="h-[420px] md:h-[560px] pr-3">
              <div className="space-y-2">
                {data.tracks.map((t, idx) => {
                  const num =
                    t.disc_num > 1
                      ? `${t.disc_num}.${t.track_num || idx + 1}`
                      : String(t.track_num || idx + 1);
                  return (
                    <div
                      key={`alb-tr-${t.track_id || idx}`}
                      className={cn('flex items-center gap-3 p-2.5 rounded-lg border border-border/60 hover:bg-accent/30 transition-colors')}
                    >
                      <div className="w-10 text-right text-xs tabular-nums text-muted-foreground shrink-0">{num}</div>
                      <div className="flex-1 min-w-0">
                        <div className="text-sm font-medium truncate">{t.title || 'Untitled'}</div>
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
                      </div>
                      <div className="w-16 text-right text-xs tabular-nums text-muted-foreground shrink-0">
                        {formatDuration(t.duration_sec || 0)}
                      </div>
                    </div>
                  );
                })}
              </div>
            </ScrollArea>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
