import { useState } from 'react';
import { ExternalLink, Loader2, Sparkles } from 'lucide-react';
import { useNavigate } from 'react-router-dom';

import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import * as api from '@/lib/api';
import { ProviderBadge } from '@/components/providers/ProviderBadge';
import { useToast } from '@/hooks/use-toast';

type Props = {
  entityType: 'artist' | 'album' | 'label';
  artistId?: number;
  albumId?: number;
  label?: string;
  entityLabel: string;
  triggerLabel?: string;
};

export function EntityDiscoverDialog({
  entityType,
  artistId,
  albumId,
  label,
  entityLabel,
  triggerLabel = 'Discover',
}: Props) {
  const navigate = useNavigate();
  const { toast } = useToast();
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<api.EntityDiscoverResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const payload = await api.postEntityDiscover({
        entity_type: entityType,
        artist_id: artistId,
        album_id: albumId,
        label,
      });
      setData(payload);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to build discovery suggestions';
      setError(message);
      toast({ title: 'Discovery failed', description: message, variant: 'destructive' });
    } finally {
      setLoading(false);
    }
  };

  const handleOpenChange = (next: boolean) => {
    setOpen(next);
    if (next && !data && !loading) {
      void load();
    }
  };

  const openLink = (link: api.EntityDiscoverLink) => {
    const href = String(link.href || '').trim();
    if (!href) return;
    if (link.kind === 'internal') {
      navigate(href);
      setOpen(false);
      return;
    }
    window.open(href, '_blank', 'noopener,noreferrer');
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button type="button" size="sm" variant="outline" className="h-8 gap-2">
          <Sparkles className="h-3.5 w-3.5" />
          {triggerLabel}
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-4xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Sparkles className="h-4 w-4" />
            Discover around {entityLabel}
          </DialogTitle>
          <DialogDescription>
            PMDA mixes your local library context with online discovery paths.
          </DialogDescription>
        </DialogHeader>

        {loading ? (
          <div className="flex items-center justify-center py-16 text-sm text-muted-foreground">
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            Building recommendations…
          </div>
        ) : error ? (
          <div className="rounded-xl border border-destructive/40 bg-destructive/5 p-4 text-sm text-destructive">
            {error}
          </div>
        ) : data ? (
          <div className="space-y-4">
            <div className="flex flex-wrap items-center gap-2">
              {data.provider ? <ProviderBadge provider={data.provider} prefix="Source" className="text-[10px]" /> : null}
              {data.ai_used ? (
                <Badge variant="outline" className="text-[10px]">AI used</Badge>
              ) : (
                <Badge variant="outline" className="text-[10px]">Fallback</Badge>
              )}
            </div>
            <div className="rounded-2xl border border-border/70 bg-muted/20 p-4 text-sm leading-relaxed text-muted-foreground">
              {data.summary}
            </div>
            <ScrollArea className="max-h-[58vh] pr-3">
              <div className="space-y-4">
                {(data.sections || []).map((section) => (
                  <div key={section.key} className="rounded-2xl border border-border/70 p-4">
                    <div className="space-y-1">
                      <div className="text-sm font-semibold">{section.title}</div>
                      {section.reason ? <div className="text-xs text-muted-foreground">{section.reason}</div> : null}
                    </div>
                    <div className="mt-3 grid gap-2 sm:grid-cols-2">
                      {(section.links || []).map((link, idx) => (
                        <button
                          key={`${section.key}-${idx}-${link.href}`}
                          type="button"
                          onClick={() => openLink(link)}
                          className="flex items-start gap-3 rounded-xl border border-border/60 bg-background/60 p-3 text-left transition-colors hover:bg-muted/30"
                        >
                          {link.thumb ? (
                            <img src={link.thumb} alt={link.label} className="h-12 w-12 rounded-lg object-cover" />
                          ) : (
                            <div className="flex h-12 w-12 items-center justify-center rounded-lg border border-border/60 bg-muted/30">
                              <Sparkles className="h-4 w-4 text-muted-foreground" />
                            </div>
                          )}
                          <div className="min-w-0 flex-1">
                            <div className="truncate text-sm font-medium">{link.label}</div>
                            {link.subtitle ? <div className="mt-0.5 text-xs text-muted-foreground">{link.subtitle}</div> : null}
                            <div className="mt-1.5 flex flex-wrap items-center gap-2">
                              <Badge variant="outline" className="text-[10px]">{link.kind === 'internal' ? 'In PMDA' : 'On the web'}</Badge>
                              {link.provider ? <ProviderBadge provider={link.provider} className="text-[10px]" /> : null}
                            </div>
                          </div>
                          {link.kind === 'external' ? <ExternalLink className="mt-0.5 h-4 w-4 text-muted-foreground" /> : null}
                        </button>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </ScrollArea>
          </div>
        ) : null}
      </DialogContent>
    </Dialog>
  );
}
