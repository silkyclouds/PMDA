import { useCallback, useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate, useOutletContext, useParams } from 'react-router-dom';
import { ArrowLeft, Play, UserRound } from 'lucide-react';

import { AspectRatio } from '@/components/ui/aspect-ratio';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { AlbumArtwork } from '@/components/library/AlbumArtwork';
import { usePlayback } from '@/contexts/PlaybackContext';
import { useToast } from '@/hooks/use-toast';
import * as api from '@/lib/api';
import type { TrackInfo } from '@/components/library/AudioPlayer';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';

export default function LabelPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched } = useOutletContext<LibraryOutletContext>();
  const { startPlayback, setCurrentTrack } = usePlayback();
  const { toast } = useToast();
  const params = useParams<{ label: string }>();
  const label = decodeURIComponent(String(params.label || '')).trim();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [albums, setAlbums] = useState<api.LibraryAlbumItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [coverSize] = useState<number>(() => {
    try {
      const raw = Number(localStorage.getItem('pmda_library_cover_size') || 220);
      return Number.isFinite(raw) ? Math.max(150, Math.min(320, raw)) : 220;
    } catch {
      return 220;
    }
  });

  const load = useCallback(async () => {
    if (!label) {
      setError('Invalid label');
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const res = await api.getLibraryAlbums({ label, sort: 'year_desc', limit: 120, offset, includeUnmatched });
      setAlbums(Array.isArray(res.albums) ? res.albums : []);
      setTotal(Number(res.total || 0));
    } catch (e) {
      setAlbums([]);
      setTotal(0);
      setError(e instanceof Error ? e.message : 'Failed to load label');
    } finally {
      setLoading(false);
    }
  }, [includeUnmatched, label, offset]);

  useEffect(() => {
    void load();
  }, [load]);

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

  const artists = useMemo(() => {
    const map = new Map<number, string>();
    for (const a of albums) {
      if (a.artist_id > 0 && a.artist_name) map.set(a.artist_id, a.artist_name);
    }
    return Array.from(map.entries())
      .map(([artist_id, artist_name]) => ({ artist_id, artist_name }))
      .sort((a, b) => a.artist_name.localeCompare(b.artist_name))
      .slice(0, 60);
  }, [albums]);
  const gridTemplateColumns = useMemo(() => {
    const col = Math.max(140, Math.min(340, Math.floor(coverSize)));
    return `repeat(auto-fill, minmax(${col}px, 1fr))`;
  }, [coverSize]);

  const canPrev = offset > 0;
  const canNext = offset + albums.length < total;

  return (
    <div className="container py-6 space-y-6">
      <div className="flex items-center justify-between gap-3">
        <Button variant="ghost" className="gap-2" onClick={() => navigate(`/library${location.search || ''}`)}>
          <ArrowLeft className="w-4 h-4" />
          Back to Library
        </Button>
      </div>

      <Card className="border-border/70">
        <CardContent className="p-5 space-y-3">
          <div className="flex items-start justify-between gap-4">
            <div className="min-w-0">
              <h1 className="text-2xl font-bold truncate">{label || 'Label'}</h1>
              <p className="text-xs text-muted-foreground mt-1">
                {total > 0 ? `${total.toLocaleString()} release${total !== 1 ? 's' : ''}` : ' '}
              </p>
            </div>
            {error ? (
              <Badge variant="outline" className="text-xs border-destructive/50 text-destructive">
                {error}
              </Badge>
            ) : null}
          </div>

          {artists.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {artists.map((a) => (
                <button
                  key={`lab-ar-${a.artist_id}`}
                  type="button"
                  className="text-[11px] px-2 py-1 rounded-full bg-muted/70 hover:bg-muted transition-colors"
                  onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`)}
                  title="Open artist"
                >
                  {a.artist_name}
                </button>
              ))}
            </div>
          ) : null}
        </CardContent>
      </Card>

      <div className="space-y-3">
        <div className="flex items-center justify-between gap-3">
          <h2 className="text-lg font-semibold">Releases</h2>
          <div className="flex items-center gap-2">
            <Button size="sm" variant="outline" disabled={!canPrev || loading} onClick={() => setOffset(Math.max(0, offset - 120))}>
              Prev
            </Button>
            <Button size="sm" variant="outline" disabled={!canNext || loading} onClick={() => setOffset(offset + 120)}>
              Next
            </Button>
          </div>
        </div>

        {loading && albums.length === 0 ? (
          <Card className="border-border/70">
            <CardContent className="p-8 text-sm text-muted-foreground">Loading…</CardContent>
          </Card>
        ) : albums.length === 0 ? (
          <Card className="border-border/70">
            <CardContent className="p-8 text-sm text-muted-foreground">No releases found for this label.</CardContent>
          </Card>
        ) : (
          <div className="grid gap-4" style={{ gridTemplateColumns }}>
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
                <div
                  className="relative overflow-hidden rounded-2xl border border-border/60 bg-card shadow-sm"
                >
                  <AspectRatio ratio={1} className="bg-muted">
                    <AlbumArtwork
                      albumThumb={a.thumb}
                      artistId={a.artist_id}
                      alt={a.title}
                      size={512}
                      imageClassName="w-full h-full object-cover"
                    />
                    <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity bg-black/25" />
                    <div className="absolute inset-x-0 bottom-0 p-3 opacity-0 group-hover:opacity-100 transition-opacity">
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
                    <div className="text-sm font-semibold truncate">{a.title}</div>
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
                      <Badge variant="outline" className="text-[10px]">
                        {a.year ?? '—'}
                      </Badge>
                      <Badge variant="outline" className="text-[10px]">
                        {a.track_count}t
                      </Badge>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
