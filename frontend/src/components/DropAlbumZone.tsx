import { useState, useCallback } from 'react';
import { Upload, Loader2, CheckCircle2, AlertCircle } from 'lucide-react';
import { cn } from '@/lib/utils';
import { improveDroppedAlbum, type ImproveDropResult } from '@/lib/api';
import { ScrollArea } from '@/components/ui/scroll-area';

const AUDIO_EXT = new Set(['.flac', '.mp3', '.m4a', '.aac', '.ogg', '.opus', '.wav', '.alac', '.ape', '.dsf', '.aif', '.aiff', '.wma', '.m4b', '.mp4']);

function isAudioFile(file: File): boolean {
  const name = file.name.toLowerCase();
  const ext = name.includes('.') ? name.slice(name.lastIndexOf('.')) : '';
  return AUDIO_EXT.has(ext);
}

/** Recursively collect File objects from a DataTransferItem (folder or file). */
async function collectFilesFromItem(item: DataTransferItem): Promise<File[]> {
  const entry = item.webkitGetAsEntry?.() ?? null;
  if (!entry) {
    const file = item.getAsFile();
    return file ? [file] : [];
  }
  if (entry.isFile) {
    return new Promise((resolve) => {
      (entry as FileSystemFileEntry).file((f) => resolve([f]));
    });
  }
  if (entry.isDirectory) {
    const dir = entry as FileSystemDirectoryEntry;
    const reader = dir.createReader();
    const allEntries: FileSystemEntry[] = [];
    const readBatch = (): Promise<void> =>
      new Promise((resolve, reject) => {
        reader.readEntries(
          (entries) => {
            if (entries.length === 0) {
              resolve();
              return;
            }
            allEntries.push(...entries);
            readBatch().then(resolve).catch(reject);
          },
          reject
        );
      });
    await readBatch();
    const out: File[] = [];
    for (const e of allEntries) {
      if (e.isFile) {
        const f = await new Promise<File>((res) => (e as FileSystemFileEntry).file(res));
        out.push(f);
      } else if (e.isDirectory) {
        const sub = await collectFilesFromItem({ webkitGetAsEntry: () => e, getAsFile: () => null } as DataTransferItem);
        out.push(...sub);
      }
    }
    return out;
  }
  return [];
}

export function DropAlbumZone() {
  const [dragOver, setDragOver] = useState(false);
  const [status, setStatus] = useState<'idle' | 'uploading' | 'analyzing' | 'done' | 'error'>('idle');
  const [result, setResult] = useState<ImproveDropResult | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const handleDrop = useCallback(async (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const items = e.dataTransfer?.items;
    if (!items?.length) return;
    const allFiles: File[] = [];
    for (let i = 0; i < items.length; i++) {
      const files = await collectFilesFromItem(items[i]);
      allFiles.push(...files);
    }
    const audioFiles = allFiles.filter(isAudioFile);
    if (audioFiles.length === 0) {
      setStatus('error');
      setErrorMessage('No audio files found. Drop FLAC, MP3, M4A, etc.');
      setResult(null);
      return;
    }
    setStatus('uploading');
    setResult(null);
    setErrorMessage(null);
    try {
      setStatus('analyzing');
      const res = await improveDroppedAlbum(audioFiles);
      setResult(res);
      setStatus('done');
    } catch (err) {
      setStatus('error');
      setErrorMessage(err instanceof Error ? err.message : 'Failed to improve album');
      setResult(null);
    }
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragOver(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragOver(false);
  }, []);

  const reset = useCallback(() => {
    setStatus('idle');
    setResult(null);
    setErrorMessage(null);
  }, []);

  return (
    <div className="space-y-3">
      <div
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        className={cn(
          'rounded-xl border-2 border-dashed p-6 text-center transition-colors',
          dragOver ? 'border-primary bg-primary/5' : 'border-muted-foreground/25 bg-muted/30 hover:bg-muted/50',
          status === 'uploading' || status === 'analyzing' ? 'pointer-events-none opacity-80' : ''
        )}
      >
        {status === 'uploading' || status === 'analyzing' ? (
          <div className="flex flex-col items-center gap-2">
            <Loader2 className="h-10 w-10 animate-spin text-muted-foreground" />
            <p className="text-sm text-muted-foreground">
              {status === 'uploading' ? 'Uploading…' : 'Analyzing and tagging…'}
            </p>
          </div>
        ) : (
          <>
            <Upload className="mx-auto h-10 w-10 text-muted-foreground" />
            <p className="mt-2 text-sm font-medium text-foreground">Drop an album folder here</p>
            <p className="text-xs text-muted-foreground mt-1">
              PMDA will identify the album, fetch tags and cover, and update the files. Duplicate track positions in the folder are reported.
            </p>
          </>
        )}
      </div>

      {status === 'done' && result && (
        <div className="rounded-lg border border-border bg-card p-4 space-y-3">
          <div className="flex items-center gap-2">
            <CheckCircle2 className="h-5 w-5 text-green-600 dark:text-green-400 shrink-0" />
            <span className="font-medium text-foreground">Done</span>
            <button
              type="button"
              onClick={reset}
              className="ml-auto text-xs text-muted-foreground hover:text-foreground"
            >
              Drop another
            </button>
          </div>
          <p className="text-sm text-muted-foreground">{result.summary}</p>
          {result.dupes_in_folder.length > 0 && (
            <div className="flex items-start gap-2 rounded-md bg-amber-500/10 border border-amber-500/20 p-2">
              <AlertCircle className="h-4 w-4 text-amber-600 dark:text-amber-400 shrink-0 mt-0.5" />
              <p className="text-xs text-foreground">
                Duplicate track positions detected: {result.dupes_in_folder.length} group(s). Check the folder and remove extras if needed.
              </p>
            </div>
          )}
          {result.steps.length > 0 && (
            <ScrollArea className="h-24 rounded border border-border">
              <ul className="p-2 text-xs text-muted-foreground space-y-1">
                {result.steps.map((s, i) => (
                  <li key={i}>{s}</li>
                ))}
              </ul>
            </ScrollArea>
          )}
        </div>
      )}

      {status === 'error' && (
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 p-4 flex items-start gap-2">
          <AlertCircle className="h-5 w-5 text-destructive shrink-0 mt-0.5" />
          <div className="flex-1 min-w-0">
            <p className="text-sm font-medium text-destructive">Error</p>
            <p className="text-xs text-muted-foreground mt-1">{errorMessage}</p>
            <button type="button" onClick={reset} className="mt-2 text-xs text-primary hover:underline">
              Try again
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
