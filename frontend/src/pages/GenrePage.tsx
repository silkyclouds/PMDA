import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, Music } from 'lucide-react';

import { AspectRatio } from '@/components/ui/aspect-ratio';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { ScrollArea } from '@/components/ui/scroll-area';
import * as api from '@/lib/api';

export default function GenrePage() {
  const navigate = useNavigate();
  const params = useParams<{ genre: string }>();
  const genre = decodeURIComponent(String(params.genre || '')).trim();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [albums, setAlbums] = useState<api.LibraryAlbumItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);

  const [labelsLoading, setLabelsLoading] = useState(false);
  const [labels, setLabels] = useState<api.GenreLabelItem[]>([]);
  const [albumCount, setAlbumCount] = useState(0);

  const load = useCallback(async () => {
    if (!genre) {
      setError('Invalid genre');
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const [albumsRes, labelsRes] = await Promise.all([
        api.getLibraryAlbums({ genre, sort: 'year_desc', limit: 120, offset }),
        (async () => {
          setLabelsLoading(true);
          try {
            return await api.getLibraryGenreLabels(genre, 120);
          } finally {
            setLabelsLoading(false);
          }
        })(),
      ]);
      setAlbums(Array.isArray(albumsRes.albums) ? albumsRes.albums : []);
      setTotal(Number(albumsRes.total || 0));
      setLabels(Array.isArray(labelsRes.labels) ? labelsRes.labels : []);
      setAlbumCount(Number(labelsRes.album_count || 0));
    } catch (e) {
      setAlbums([]);
      setTotal(0);
      setLabels([]);
      setAlbumCount(0);
      setError(e instanceof Error ? e.message : 'Failed to load genre');
    } finally {
      setLoading(false);
    }
  }, [genre, offset]);

  useEffect(() => {
    void load();
  }, [load]);

  const canPrev = offset > 0;
  const canNext = offset + albums.length < total;

  const labelChips = useMemo(() => {
    return labels.slice(0, 60);
  }, [labels]);

  return (
    <div className="container py-6 space-y-6">
      <div className="flex items-center justify-between gap-3">
        <Button variant="ghost" className="gap-2" onClick={() => navigate('/library')}>
          <ArrowLeft className="w-4 h-4" />
          Back to Library
        </Button>
      </div>

      <Card className="border-border/70">
        <CardContent className="p-5 space-y-4">
          <div className="flex items-start justify-between gap-4">
            <div className="min-w-0">
              <h1 className="text-2xl font-bold truncate">{genre || 'Genre'}</h1>
              <p className="text-xs text-muted-foreground mt-1">
                {(total > 0 || albumCount > 0) ? `${Math.max(total, albumCount).toLocaleString()} album${Math.max(total, albumCount) !== 1 ? 's' : ''}` : ' '}
              </p>
            </div>
            {error ? (
              <Badge variant="outline" className="text-xs border-destructive/50 text-destructive">
                {error}
              </Badge>
            ) : null}
          </div>

          {labelsLoading && labels.length === 0 ? (
            <div className="text-sm text-muted-foreground">Loading labels…</div>
          ) : labelChips.length > 0 ? (
            <div className="space-y-2">
              <div className="text-xs font-medium text-muted-foreground">Labels publishing this genre</div>
              <ScrollArea className="w-full whitespace-nowrap">
                <div className="flex gap-2 pb-2">
                  {labelChips.map((l) => (
                    <button
                      key={`genre-lab-${l.label}`}
                      type="button"
                      className="inline-flex items-center gap-2 rounded-full border border-border/60 bg-muted/40 px-3 py-1.5 text-[11px] hover:bg-muted transition-colors"
                      onClick={() => navigate(`/library/label/${encodeURIComponent(l.label)}`)}
                      title="Open label"
                    >
                      <span className="truncate max-w-[16rem]">{l.label}</span>
                      <span className="text-muted-foreground tabular-nums">{l.count}</span>
                    </button>
                  ))}
                </div>
              </ScrollArea>
            </div>
          ) : null}
        </CardContent>
      </Card>

      <div className="space-y-3">
        <div className="flex items-center justify-between gap-3">
          <h2 className="text-lg font-semibold">Albums</h2>
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
            <CardContent className="p-8 text-sm text-muted-foreground">No albums found for this genre.</CardContent>
          </Card>
        ) : (
          <div className="grid gap-4" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(10.5rem, 1fr))' }}>
            {albums.map((a) => (
              <button
                key={`genre-alb-${a.album_id}`}
                type="button"
                className="text-left group"
                onClick={() => navigate(`/library/album/${a.album_id}`)}
                title="Open album"
              >
                <div className="relative overflow-hidden rounded-2xl border border-border/60 bg-card shadow-sm">
                  <AspectRatio ratio={1} className="bg-muted">
                    {a.thumb ? (
                      <img src={a.thumb} alt={a.title} className="w-full h-full object-cover" />
                    ) : (
                      <div className="w-full h-full flex items-center justify-center">
                        <Music className="w-10 h-10 text-muted-foreground" />
                      </div>
                    )}
                    <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity bg-black/25" />
                  </AspectRatio>
                  <div className="p-3 space-y-1.5">
                    <div className="text-sm font-semibold truncate">{a.title}</div>
                    <button
                      type="button"
                      className="text-xs text-muted-foreground truncate hover:underline"
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        navigate(`/library/artist/${a.artist_id}`);
                      }}
                      title="Open artist"
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
                      {a.label ? (
                        <Badge
                          variant="outline"
                          className="text-[10px]"
                          onClick={(e) => {
                            e.preventDefault();
                            e.stopPropagation();
                            navigate(`/library/label/${encodeURIComponent(a.label || '')}`);
                          }}
                          title="Open label"
                        >
                          {a.label}
                        </Badge>
                      ) : null}
                    </div>
                  </div>
                </div>
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

