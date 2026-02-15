import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, Music } from 'lucide-react';

import { AspectRatio } from '@/components/ui/aspect-ratio';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import * as api from '@/lib/api';

export default function LabelPage() {
  const navigate = useNavigate();
  const params = useParams<{ label: string }>();
  const label = decodeURIComponent(String(params.label || '')).trim();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [albums, setAlbums] = useState<api.LibraryAlbumItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);

  const load = useCallback(async () => {
    if (!label) {
      setError('Invalid label');
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const res = await api.getLibraryAlbums({ label, sort: 'year_desc', limit: 120, offset });
      setAlbums(Array.isArray(res.albums) ? res.albums : []);
      setTotal(Number(res.total || 0));
    } catch (e) {
      setAlbums([]);
      setTotal(0);
      setError(e instanceof Error ? e.message : 'Failed to load label');
    } finally {
      setLoading(false);
    }
  }, [label, offset]);

  useEffect(() => {
    void load();
  }, [load]);

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

  const canPrev = offset > 0;
  const canNext = offset + albums.length < total;

  return (
    <div className="container py-6 space-y-6">
      <div className="flex items-center justify-between gap-3">
        <Button variant="ghost" className="gap-2" onClick={() => navigate('/library')}>
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
                  onClick={() => navigate(`/library/artist/${a.artist_id}`)}
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
          <div className="grid gap-4" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(10.5rem, 1fr))' }}>
	            {albums.map((a) => (
	              <button
	                key={`lab-alb-${a.album_id}`}
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
                    <div className="text-xs text-muted-foreground truncate">{a.artist_name}</div>
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
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
