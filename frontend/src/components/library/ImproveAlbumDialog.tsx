import { useState } from 'react';
import { Sparkles, Loader2, CheckCircle2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import { ScrollArea } from '@/components/ui/scroll-area';

interface ImproveAlbumDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  albumId: number;
  albumTitle: string;
  onSuccess?: () => void;
}

export function ImproveAlbumDialog({ open, onOpenChange, albumId, albumTitle, onSuccess }: ImproveAlbumDialogProps) {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<{ steps: (string | { label: string })[]; summary: string; tags_updated: boolean; cover_saved: boolean } | null>(null);

  const handleConfirm = async () => {
    setLoading(true);
    setResult(null);
    try {
      const response = await fetch('/api/library/improve-album', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ album_id: albumId }),
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || data.detail || 'Failed');
      const stepsRaw = data.steps || [];
      const steps = Array.isArray(stepsRaw)
        ? stepsRaw.map((s: unknown) => (typeof s === 'object' && s != null && 'label' in s ? (s as { label: string }).label : String(s)))
        : [];
      setResult({
        steps,
        summary: data.summary ?? 'Done.',
        tags_updated: data.tags_updated ?? false,
        cover_saved: data.cover_saved ?? false,
      });
      onSuccess?.();
    } catch (e) {
      setResult({
        steps: [],
        summary: e instanceof Error ? e.message : 'Failed to improve album',
        tags_updated: false,
        cover_saved: false,
      });
    } finally {
      setLoading(false);
    }
  };

  const handleClose = () => {
    setResult(null);
    onOpenChange(false);
  };

  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent className="max-w-md">
        <AlertDialogHeader>
          <AlertDialogTitle className="flex items-center gap-2">
            <Sparkles className="w-5 h-5" />
            {result == null ? 'Improve this album?' : 'Improve result'}
          </AlertDialogTitle>
          <AlertDialogDescription>
            {result == null ? (
              <>
                This will query MusicBrainz, Discogs, Last.fm, and Bandcamp for album art and tags, then update files.
                Continue?
              </>
            ) : (
              <>
                <strong>{albumTitle}</strong>
              </>
            )}
          </AlertDialogDescription>
        </AlertDialogHeader>
        {loading && (
          <p className="text-sm text-muted-foreground">
            Processing… querying providers, updating tags and cover.
          </p>
        )}
        {result != null && (
          <>
            <div className="rounded-lg border border-green-200 bg-green-50 dark:border-green-800 dark:bg-green-950/50 p-3 text-sm font-medium text-foreground">
              {result.summary}
            </div>
            {result.steps.length > 0 && (
              <div className="space-y-1.5">
                <p className="text-sm font-medium text-foreground">Steps performed</p>
                <ScrollArea className="max-h-52 rounded-md border border-border bg-muted/30 p-3 text-sm">
                  <ul className="space-y-1.5 list-none pl-0 text-muted-foreground">
                    {result.steps.map((step, i) => (
                      <li key={i} className="flex items-center gap-2">
                        <span className="text-primary shrink-0">•</span>
                        <span>{typeof step === 'string' ? step : (step as { label: string }).label}</span>
                      </li>
                    ))}
                  </ul>
                </ScrollArea>
              </div>
            )}
          </>
        )}
        <AlertDialogFooter>
          {result == null ? (
            <>
              <AlertDialogCancel onClick={handleClose} disabled={loading}>Cancel</AlertDialogCancel>
              <AlertDialogAction onClick={handleConfirm} disabled={loading}>
                {loading ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin mr-2" />
                    Improving…
                  </>
                ) : (
                  'Improve'
                )}
              </AlertDialogAction>
            </>
          ) : (
            <Button onClick={handleClose}>
              <CheckCircle2 className="w-4 h-4 mr-2" />
              Done
            </Button>
          )}
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
