import { AlertTriangle, Database, FolderTree, RefreshCw } from 'lucide-react';

import type { LibraryFilesIndexStatus } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { cn } from '@/lib/utils';

interface LibraryIndexStatusBannerProps {
  status: LibraryFilesIndexStatus | null | undefined;
  libraryRoot?: string | null;
}

type PhaseTone = 'progress' | 'warning' | 'error';

interface PhaseMeta {
  label: string;
  description: string;
  progress: number;
  tone: PhaseTone;
}

const PHASE_META: Record<string, PhaseMeta> = {
  discovering: {
    label: 'Scanning library folders',
    description: 'PMDA is discovering album folders and counting audio files in your final library.',
    progress: 16,
    tone: 'progress',
  },
  collapsing: {
    label: 'Grouping release folders',
    description: 'PMDA is collapsing nested discs, works, and segmented releases into clean album groups.',
    progress: 34,
    tone: 'progress',
  },
  parsing: {
    label: 'Reading album metadata',
    description: 'PMDA is parsing albums, tracks, formats, and embedded tags before rebuilding the visible catalog.',
    progress: 56,
    tone: 'progress',
  },
  media_prepare: {
    label: 'Preparing artwork and media',
    description: 'PMDA is collecting covers and artist images for the rebuilt library snapshot.',
    progress: 76,
    tone: 'progress',
  },
  writing: {
    label: 'Updating the library database',
    description: 'PMDA is writing artists, albums, and tracks into the files library database.',
    progress: 86,
    tone: 'progress',
  },
  embeddings: {
    label: 'Refreshing recommendations index',
    description: 'PMDA is rebuilding track embeddings so recommendations and related views stay accurate.',
    progress: 92,
    tone: 'progress',
  },
  artist_enrichment: {
    label: 'Enriching artist pages',
    description: 'PMDA is backfilling artist images and profile data for the rebuilt library.',
    progress: 96,
    tone: 'progress',
  },
  media_cache: {
    label: 'Finalizing media cache',
    description: 'PMDA is warming artwork caches before the new library snapshot becomes active.',
    progress: 99,
    tone: 'progress',
  },
  done: {
    label: 'Library rebuild complete',
    description: 'The new library snapshot is ready.',
    progress: 100,
    tone: 'progress',
  },
  error: {
    label: 'Library rebuild failed',
    description: 'PMDA hit an error while rebuilding the files library index.',
    progress: 100,
    tone: 'error',
  },
  idle: {
    label: 'Library index is idle',
    description: 'No rebuild is currently running.',
    progress: 0,
    tone: 'warning',
  },
};

function formatCompactCount(value: unknown): string {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return '0';
  return n.toLocaleString();
}

function formatLibraryRootLabel(pathValue?: string | null): string {
  const raw = String(pathValue || '').trim();
  if (!raw) return 'your final library';
  const parts = raw.split('/').filter(Boolean);
  return parts[parts.length - 1] || raw;
}

function shortenPath(pathValue?: string | null): string {
  const raw = String(pathValue || '').trim();
  if (!raw) return '';
  const segments = raw.split('/').filter(Boolean);
  if (segments.length <= 4) return raw;
  return `.../${segments.slice(-4).join('/')}`;
}

function resolvePhaseMeta(status: LibraryFilesIndexStatus): PhaseMeta {
  const key = String(status.phase || '').trim().toLowerCase();
  const base = PHASE_META[key] || {
    label: 'Rebuilding library index',
    description: 'PMDA is rebuilding the visible files library snapshot.',
    progress: status.running ? 48 : 0,
    tone: 'progress' as const,
  };

  const totalFolders = Math.max(0, Number(status.total_folders || 0));
  const foldersProcessed = Math.max(0, Number(status.folders_processed || 0));
  const ratio = totalFolders > 0 ? Math.max(0, Math.min(1, foldersProcessed / totalFolders)) : null;
  const backendProgress = Number(status.phase_progress);
  if (Number.isFinite(backendProgress) && backendProgress >= 0) {
    return {
      ...base,
      progress: Math.max(0, Math.min(100, backendProgress)),
    };
  }

  if (key === 'parsing' && ratio != null) {
    return {
      ...base,
      progress: Math.max(base.progress, Math.min(74, 40 + (ratio * 34))),
    };
  }

  if (key === 'discovering' && ratio != null) {
    return {
      ...base,
      progress: Math.max(base.progress, Math.min(30, 8 + (ratio * 22))),
    };
  }

  return base;
}

function formatEta(secondsValue?: number | null): string {
  const seconds = Number(secondsValue);
  if (!Number.isFinite(seconds) || seconds < 0) return '';
  if (seconds < 60) return `${Math.max(1, Math.round(seconds))}s remaining`;
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  if (hours > 0) return `${hours}h ${minutes}m remaining`;
  if (minutes > 0 && secs > 0) return `${minutes}m ${secs}s remaining`;
  return `${minutes}m remaining`;
}

export function LibraryIndexStatusBanner({ status, libraryRoot }: LibraryIndexStatusBannerProps) {
  const running = Boolean(status?.running);
  const hasError = Boolean(status?.error) || String(status?.phase || '').toLowerCase() === 'error';
  if (!running && !hasError) return null;

  const resolvedStatus: LibraryFilesIndexStatus = status || { running: false };
  const phaseMeta = resolvePhaseMeta(resolvedStatus);
  const currentFolder = shortenPath(resolvedStatus.current_folder);
  const folderCount = Math.max(0, Number(resolvedStatus.folders_processed || resolvedStatus.total_folders || 0));
  const totalFolders = Math.max(0, Number(resolvedStatus.total_folders || 0));
  const indexedAlbums = Math.max(0, Number(resolvedStatus.indexed_albums || 0));
  const indexedArtists = Math.max(0, Number(resolvedStatus.indexed_artists || 0));
  const indexedTracks = Math.max(0, Number(resolvedStatus.indexed_tracks || 0));
  const audioCount = Math.max(0, Number(resolvedStatus.discovered_audio_files || resolvedStatus.tracks || 0));
  const phaseKey = String(resolvedStatus.phase || '').trim().toLowerCase();
  const collapsedGroups = Math.max(0, Number(resolvedStatus.collapsed_groups || 0));
  const ratePerSecond = Number(resolvedStatus.phase_rate_per_sec || 0);
  const etaLabel = formatEta(resolvedStatus.phase_eta_seconds);
  const folderUnitLabel = phaseKey === 'collapsing'
    ? 'folder groups checked'
    : phaseKey === 'parsing'
      ? 'album folders read'
      : 'folders';
  const folderSummary = totalFolders > 0
    ? `${formatCompactCount(folderCount)} / ${formatCompactCount(totalFolders)} ${folderUnitLabel}`
    : `${formatCompactCount(folderCount)} ${folderUnitLabel}`;
  const rootLabel = formatLibraryRootLabel(libraryRoot);
  const phaseMessage = String(resolvedStatus.phase_message || '').trim();

  return (
    <div
      className={cn(
        'mt-3 overflow-hidden rounded-2xl border backdrop-blur-sm',
        phaseMeta.tone === 'error'
          ? 'border-destructive/40 bg-destructive/8'
          : 'border-primary/20 bg-gradient-to-r from-primary/10 via-card/95 to-card/90',
      )}
    >
      <div className="px-4 py-3 md:px-5 md:py-4">
        <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
          <div className="min-w-0 flex-1 space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="outline" className="border-primary/30 bg-primary/10 text-primary">
                {running ? 'Library rebuild in progress' : 'Library rebuild error'}
              </Badge>
              <Badge variant="outline" className="border-border/70 bg-background/60 text-muted-foreground">
                {phaseMeta.label}
              </Badge>
              <Badge variant="outline" className="border-border/70 bg-background/60 text-muted-foreground">
                {rootLabel}
              </Badge>
            </div>
            <div>
              <p className="text-sm font-medium text-foreground">{phaseMessage || phaseMeta.description}</p>
              <p className="text-xs text-muted-foreground">
                {running
                  ? 'PMDA keeps the previous library snapshot visible until the rebuild is ready to swap in.'
                  : (resolvedStatus.error || 'PMDA could not finish rebuilding the library index.')}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            {hasError ? <AlertTriangle className="h-4 w-4 text-destructive" /> : <RefreshCw className="h-4 w-4 animate-spin text-primary" />}
            <span>{running ? 'Estimated progress' : 'Needs attention'}</span>
          </div>
        </div>

        <div className="mt-3 space-y-2">
          <Progress value={Math.max(2, Math.min(100, phaseMeta.progress))} className="h-2 bg-background/70" />
          {(etaLabel || ratePerSecond > 0) ? (
            <div className="flex flex-wrap items-center gap-2 text-xs">
              {etaLabel ? (
                <Badge variant="outline" className="border-primary/30 bg-primary/10 text-primary">
                  ETA · {etaLabel}
                </Badge>
              ) : null}
              {ratePerSecond > 0 ? (
                <Badge variant="outline" className="border-border/70 bg-background/60 text-muted-foreground">
                  Speed · {ratePerSecond.toFixed(ratePerSecond >= 10 ? 0 : 1)} items/s
                </Badge>
              ) : null}
            </div>
          ) : null}
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
            <span className="inline-flex items-center gap-1.5">
              <FolderTree className="h-3.5 w-3.5" />
              {folderSummary}
            </span>
            <span className="inline-flex items-center gap-1.5">
              <Database className="h-3.5 w-3.5" />
              {formatCompactCount(audioCount)} audio files discovered
            </span>
            {phaseKey === 'collapsing' ? (
              <span>
                {formatCompactCount(collapsedGroups)} merges applied
              </span>
            ) : null}
            <span>
              Showing previous snapshot: {formatCompactCount(indexedAlbums)} albums · {formatCompactCount(indexedArtists)} artists · {formatCompactCount(indexedTracks)} tracks
            </span>
          </div>
        </div>

        {currentFolder ? (
          <div className="mt-3 rounded-xl border border-border/60 bg-background/55 px-3 py-2 text-xs text-muted-foreground">
            <span className="font-medium text-foreground">Current folder</span>
            <span className="mx-2 text-border">·</span>
            <span className="break-all">{currentFolder}</span>
          </div>
        ) : null}
      </div>
    </div>
  );
}
