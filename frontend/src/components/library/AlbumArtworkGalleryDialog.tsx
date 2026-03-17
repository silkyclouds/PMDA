import { useEffect, useMemo, useState } from 'react';
import { ChevronLeft, ChevronRight, ExternalLink, Loader2 } from 'lucide-react';

import * as api from '@/lib/api';
import { AlbumArtwork } from '@/components/library/AlbumArtwork';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { cn } from '@/lib/utils';

type AlbumArtworkGalleryDialogProps = {
  albumId: number;
  albumTitle: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
};

export function AlbumArtworkGalleryDialog({
  albumId,
  albumTitle,
  open,
  onOpenChange,
}: AlbumArtworkGalleryDialogProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<api.AlbumArtworkGalleryResponse | null>(null);
  const [selectedId, setSelectedId] = useState<string>('');

  useEffect(() => {
    if (!open || !Number.isFinite(albumId) || albumId <= 0) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    void (async () => {
      try {
        const res = await api.getAlbumArtworkGallery(albumId);
        if (cancelled) return;
        setData(res);
        const selected = (res.items || []).find((item) => item.selected) || (res.items || [])[0] || null;
        setSelectedId(selected?.id || '');
      } catch (e) {
        if (cancelled) return;
        setData(null);
        setError(e instanceof Error ? e.message : 'Failed to load artwork gallery');
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [albumId, open]);

  const items = useMemo(() => data?.items || [], [data?.items]);
  const currentIndex = useMemo(() => {
    const idx = items.findIndex((item) => item.id === selectedId);
    return idx >= 0 ? idx : 0;
  }, [items, selectedId]);
  const selected = items[currentIndex] || null;

  const step = (delta: number) => {
    if (!items.length) return;
    const next = (currentIndex + delta + items.length) % items.length;
    setSelectedId(items[next]?.id || '');
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-[min(96vw,84rem)] p-0 overflow-hidden border-border/70 bg-card">
        <DialogHeader className="border-b border-border/60 px-6 py-4">
          <DialogTitle className="flex items-center justify-between gap-4 text-left">
            <span className="truncate">Artwork gallery · {albumTitle}</span>
            {selected?.source_url ? (
              <Button type="button" size="sm" variant="outline" asChild>
                <a href={selected.source_url} target="_blank" rel="noreferrer noopener">
                  Source
                  <ExternalLink className="ml-2 h-4 w-4" />
                </a>
              </Button>
            ) : null}
          </DialogTitle>
        </DialogHeader>
        <div className="grid min-h-[68vh] grid-cols-1 xl:grid-cols-[minmax(0,1fr),18rem]">
          <div className="relative flex min-h-[60vh] items-center justify-center bg-muted/25 px-6 py-6">
            {loading ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                Loading artwork…
              </div>
            ) : error ? (
              <div className="text-sm text-destructive">{error}</div>
            ) : selected ? (
              <>
                <div className="absolute inset-0 bg-[radial-gradient(circle_at_center,rgba(255,255,255,0.06),transparent_45%)]" />
                <div className="relative aspect-square max-h-[72vh] w-full max-w-[72vh] overflow-hidden border border-border/70 bg-background/80 shadow-none">
                  <AlbumArtwork
                    albumThumb={selected.image_url}
                    alt={`${albumTitle} ${selected.label}`}
                    size={1600}
                    priority
                    imageClassName="h-full w-full object-contain bg-black/10"
                    fallbackClassName="flex h-full w-full items-center justify-center"
                  />
                </div>
                {items.length > 1 ? (
                  <>
                    <Button
                      type="button"
                      size="icon"
                      variant="secondary"
                      className="absolute left-4 top-1/2 h-10 w-10 -translate-y-1/2"
                      onClick={() => step(-1)}
                    >
                      <ChevronLeft className="h-5 w-5" />
                    </Button>
                    <Button
                      type="button"
                      size="icon"
                      variant="secondary"
                      className="absolute right-4 top-1/2 h-10 w-10 -translate-y-1/2"
                      onClick={() => step(1)}
                    >
                      <ChevronRight className="h-5 w-5" />
                    </Button>
                  </>
                ) : null}
              </>
            ) : (
              <div className="text-sm text-muted-foreground">No artwork variants available.</div>
            )}
          </div>
          <div className="border-l border-border/60 bg-background/70">
            <div className="border-b border-border/60 px-4 py-3 text-xs uppercase tracking-[0.22em] text-muted-foreground">
              Artwork stack
            </div>
            <div className="max-h-[68vh] space-y-2 overflow-y-auto p-3">
              {items.map((item) => (
                <button
                  key={item.id}
                  type="button"
                  className={cn(
                    'grid w-full grid-cols-[4.5rem,1fr] gap-3 border px-3 py-3 text-left transition-colors',
                    item.id === selectedId
                      ? 'border-primary/60 bg-primary/8'
                      : 'border-border/60 bg-card hover:border-primary/30 hover:bg-accent/20'
                  )}
                  onClick={() => setSelectedId(item.id)}
                >
                  <div className="aspect-square overflow-hidden rounded-sm border border-border/60 bg-muted">
                    <AlbumArtwork
                      albumThumb={item.thumb_url}
                      alt={item.label}
                      size={320}
                      imageClassName="h-full w-full object-cover"
                      fallbackClassName="flex h-full w-full items-center justify-center"
                    />
                  </div>
                  <div className="min-w-0 space-y-1">
                    <div className="flex flex-wrap items-center gap-1.5">
                      <Badge variant="outline" className="text-[10px] uppercase">
                        {item.label}
                      </Badge>
                      {item.provider ? (
                        <Badge variant="outline" className="text-[10px] uppercase">
                          {item.provider}
                        </Badge>
                      ) : null}
                    </div>
                    {item.source_name ? (
                      <div className="truncate text-xs font-medium text-foreground/90">{item.source_name}</div>
                    ) : null}
                    <div className="text-[11px] text-muted-foreground">{item.origin}</div>
                  </div>
                </button>
              ))}
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
