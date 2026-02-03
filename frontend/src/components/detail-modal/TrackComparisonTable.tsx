import { useState, useMemo } from 'react';
import { ArrowRight, Loader2, CheckCircle2, AlertCircle } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import { Tooltip, TooltipContent, TooltipTrigger, TooltipProvider } from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';
import type { Edition, Track } from '@/lib/api';
import * as api from '@/lib/api';
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

interface TrackComparisonTableProps {
  editions: Edition[];
  selectedEditionIndex: number;
  artist: string;
  albumId: string;
  /** Bonus track names from AI (e.g. from merge_list). Used to highlight and show merge when backend does not set is_bonus. */
  mergeList?: string[];
  onTrackMoved?: () => void;
}

interface TrackRow {
  index: number;
  tracks: (Track | null)[];
  isBitPerfect: boolean;
  hasBonus: boolean;
  isOnlyInOne: boolean;
  presentInEditions: number[];
}

function normalizeTrackName(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^\w\s]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function trackMatchesMergeList(track: Track | null, mergeList: string[]): boolean {
  if (!track || !mergeList?.length) return false;
  const name = (track as Track & { title?: string }).name ?? (track as Track & { title?: string }).title ?? '';
  const n = normalizeTrackName(name);
  return mergeList.some(m => normalizeTrackName(m) === n);
}

function buildTrackRows(editions: Edition[], mergeList: string[]): TrackRow[] {
  const maxTracks = Math.max(...editions.map(e => e.tracks?.length || 0));
  const rows: TrackRow[] = [];

  for (let i = 0; i < maxTracks; i++) {
    const tracks = editions.map(e => e.tracks?.[i] || null);
    const presentInEditions = tracks
      .map((t, idx) => t ? idx : -1)
      .filter(idx => idx >= 0);

    // Check if bit-perfect (all tracks same name, duration, bitrate)
    const presentTracks = tracks.filter(Boolean) as Track[];
    let isBitPerfect = false;
    if (presentTracks.length > 1) {
      const first = presentTracks[0];
      const getName = (x: Track) => (x as Track & { title?: string }).name ?? (x as Track & { title?: string }).title ?? '';
      const getDurSec = (x: Track) => {
        const d = (x as Track & { dur?: number }).duration ?? (x as Track & { dur?: number }).dur;
        return d != null ? (d < 400 ? d : d / 1000) : undefined;
      };
      isBitPerfect = presentTracks.every(t =>
        normalizeTrackName(getName(t)) === normalizeTrackName(getName(first)) &&
        getDurSec(t) === getDurSec(first) &&
        t.bitrate === first.bitrate
      );
    }

    // Bonus: from backend is_bonus or from AI merge_list
    const hasBonus = presentTracks.some(t => t.is_bonus || trackMatchesMergeList(t, mergeList));

    // Check if only in one edition
    const isOnlyInOne = presentInEditions.length === 1 && editions.length > 1;

    rows.push({
      index: i,
      tracks,
      isBitPerfect,
      hasBonus,
      isOnlyInOne,
      presentInEditions,
    });
  }

  return rows;
}

interface TrackCellProps {
  track: Track | null;
  editionIndex: number;
  rowIndex: number;
  selectedEditionIndex: number;
  isBonus: boolean;
  isOnlyHere: boolean;
  isBitPerfect: boolean;
  artist: string;
  album: string;
  isSelected: boolean;
  onSelect: (selected: boolean) => void;
  showMergeControls: boolean;
}

function TrackCell({ 
  track, 
  editionIndex, 
  selectedEditionIndex, 
  isBonus, 
  isOnlyHere, 
  isBitPerfect,
  artist,
  album,
  isSelected,
  onSelect,
  showMergeControls
}: TrackCellProps) {
  const formatDuration = (seconds?: number, durMs?: number) => {
    const secs = seconds ?? (durMs != null ? durMs / 1000 : undefined);
    if (secs == null || !Number.isFinite(secs)) return '';
    const mins = Math.floor(secs / 60);
    const s = Math.floor(secs % 60);
    return `${mins}:${s.toString().padStart(2, '0')}`;
  };

  if (!track) {
    return (
      <td className="px-2 py-2 text-center text-muted-foreground/50 border-r border-border last:border-r-0">
        —
      </td>
    );
  }

  const durMs = (track as { dur?: number }).dur;
  const secs = track.duration ?? (durMs != null ? durMs / 1000 : undefined);
  const canMerge = (isBonus || isOnlyHere) && editionIndex !== selectedEditionIndex && track.path;
  const isInKeptEdition = editionIndex === selectedEditionIndex;
  const displayName = track.name ?? (track as { title?: string }).title ?? '—';
  return (
    <td className={cn(
      "px-2 py-2 border-r border-border last:border-r-0 text-xs overflow-hidden",
      isBonus && "bg-warning/10",
      isOnlyHere && !isBonus && "bg-accent/30",
      isInKeptEdition && "bg-primary/5"
    )}>
      <div className="flex items-start gap-2 min-w-0">
        {/* Merge checkbox */}
        {showMergeControls && canMerge && (
          <Checkbox
            checked={isSelected}
            onCheckedChange={onSelect}
            className="mt-0.5 flex-shrink-0"
          />
        )}
        
        <div className="flex-1 min-w-0 overflow-hidden">
          {/* Track name */}
          <div className="flex items-center gap-1.5 min-w-0">
            <span className="font-medium text-foreground truncate block" title={displayName}>
              {displayName}
            </span>
            {isBonus && (
              <span className="px-1 py-0.5 rounded text-[9px] font-bold bg-warning/20 text-warning flex-shrink-0">
                BONUS
              </span>
            )}
            {isOnlyHere && !isBonus && (
              <span className="px-1 py-0.5 rounded text-[9px] font-medium bg-accent text-accent-foreground flex-shrink-0">
                UNIQUE
              </span>
            )}
            {isBitPerfect && isInKeptEdition && (
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <CheckCircle2 className="w-3 h-3 text-success flex-shrink-0" />
                  </TooltipTrigger>
                  <TooltipContent>
                    <p>Bit-perfect match across editions</p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            )}
          </div>
          
          {/* Artist / Album */}
          <div className="text-[10px] text-muted-foreground truncate mt-0.5">
            {artist} · {album}
          </div>
          
          {/* Technical info */}
          <div className="flex items-center gap-2 text-[10px] text-muted-foreground mt-1 truncate">
            {formatDuration(secs) && (
              <span>{formatDuration(secs)}</span>
            )}
            {track.format && <span>{track.format}</span>}
            {track.bitrate && <span>{track.bitrate} kbps</span>}
          </div>
        </div>
      </div>
    </td>
  );
}

export function TrackComparisonTable({ 
  editions, 
  selectedEditionIndex, 
  artist, 
  albumId,
  mergeList = [],
  onTrackMoved 
}: TrackComparisonTableProps) {
  const [selectedTracks, setSelectedTracks] = useState<Set<string>>(new Set());
  const [movingTracks, setMovingTracks] = useState(false);
  const [showMergeControls, setShowMergeControls] = useState(false);
  const [confirmMergeOpen, setConfirmMergeOpen] = useState(false);
  const [lastMoved, setLastMoved] = useState<{ count: number; dest?: string }>({ count: 0 });

  const rows = useMemo(() => buildTrackRows(editions, mergeList), [editions, mergeList]);

  const hasMergeableTracks = rows.some(row => 
    (row.hasBonus || row.isOnlyInOne) && 
    row.presentInEditions.some(idx => idx !== selectedEditionIndex)
  );
  const showMergeSection = mergeList.length > 0 || hasMergeableTracks;

  const handleTrackSelect = (editionIndex: number, trackIndex: number, selected: boolean) => {
    const key = `${editionIndex}-${trackIndex}`;
    setSelectedTracks(prev => {
      const next = new Set(prev);
      if (selected) {
        next.add(key);
      } else {
        next.delete(key);
      }
      return next;
    });
  };

  const selectedTrackDetails = useMemo(() => {
    const details: { name: string; editionIdx: number }[] = [];
    selectedTracks.forEach((key) => {
      const [editionIdx, trackIdx] = key.split('-').map(Number);
      const track = editions[editionIdx]?.tracks?.[trackIdx];
      const name = track?.name ?? (track as { title?: string })?.title ?? 'Unknown';
      details.push({ name, editionIdx });
    });
    return details;
  }, [selectedTracks, editions]);

  const handleMergeSelected = async () => {
    if (selectedTracks.size === 0) return;

    setMovingTracks(true);
    let moved = 0;
    let failed = false;
    let lastDest: string | undefined;
    try {
      for (const key of selectedTracks) {
        const [editionIdx, trackIdx] = key.split('-').map(Number);
        const track = editions[editionIdx]?.tracks?.[trackIdx];
        if (track?.path) {
          const result = await api.moveBonusTrack(
            artist,
            albumId,
            editionIdx,
            track.path,
            selectedEditionIndex
          );
          if (result?.success) {
            moved += 1;
            if ((result as { dest?: string }).dest) lastDest = (result as { dest?: string }).dest!;
          } else {
            failed = true;
          }
        }
      }
      setSelectedTracks(new Set());
      onTrackMoved?.();
      setLastMoved({ count: moved, dest: lastDest });
      if (moved > 0 && !failed) {
        toast.success(moved === 1 ? '1 track merged into kept edition' : `${moved} tracks merged into kept edition`);
      } else if (failed) {
        toast.error('Some tracks could not be merged');
      }
    } catch {
      toast.error('Merge failed');
      onTrackMoved?.();
    } finally {
      setMovingTracks(false);
      setConfirmMergeOpen(false);
    }
  };

  if (!editions.some(e => e.tracks && e.tracks.length > 0)) {
    return (
      <div className="text-center py-6 text-muted-foreground text-sm">
        No track information available
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {/* Merge controls header — show when AI reported extra tracks (merge_list) or we have bonus/unique rows */}
      {showMergeSection && (
        <div className="flex items-center justify-between px-1">
          <div className="flex items-center gap-2">
            <Button
              variant={showMergeControls ? "secondary" : "outline"}
              size="sm"
              onClick={() => setShowMergeControls(!showMergeControls)}
              className="h-7 text-xs gap-1.5"
            >
              <AlertCircle className="w-3 h-3" />
              {showMergeControls ? 'Hide merge' : 'Merge tracks'}
            </Button>
            {showMergeControls && selectedTracks.size > 0 && (
              <Button
                size="sm"
                onClick={() => setConfirmMergeOpen(true)}
                disabled={movingTracks}
                className="h-7 text-xs gap-1.5"
              >
                {movingTracks ? (
                  <Loader2 className="w-3 h-3 animate-spin" />
                ) : (
                  <ArrowRight className="w-3 h-3" />
                )}
                Merge {selectedTracks.size} to kept edition
              </Button>
            )}
          </div>
          <div className="text-xs text-muted-foreground">
            <span className="inline-block w-2 h-2 rounded-sm bg-warning/30 mr-1"></span> Bonus
            <span className="inline-block w-2 h-2 rounded-sm bg-accent/50 ml-3 mr-1"></span> Unique
            <span className="inline-block w-2 h-2 rounded-sm bg-primary/20 ml-3 mr-1"></span> Kept
          </div>
        </div>
      )}

      {/* Comparison table — one column per edition, fits width, no horizontal scroll */}
      <div className="border border-border rounded-lg overflow-hidden w-full min-w-0">
        <table className="w-full table-fixed min-w-0">
          <thead className="bg-muted/50">
            <tr>
              <th className="px-2 py-2 text-left text-[10px] font-medium text-muted-foreground w-8 border-r border-border" style={{ width: '2rem' }}>
                #
              </th>
              {editions.map((edition, idx) => (
                <th 
                  key={idx}
                  className={cn(
                    "px-2 py-2 text-left text-[10px] font-medium border-r border-border last:border-r-0 truncate",
                    idx === selectedEditionIndex 
                      ? "text-primary bg-primary/5" 
                      : "text-muted-foreground"
                  )}
                >
                  Ed. {idx + 1}
                  {idx === selectedEditionIndex && (
                    <span className="ml-0.5 text-[9px] text-primary">(kept)</span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {rows.map((row) => (
              <tr key={row.index} className={cn(
                row.isBitPerfect && "bg-success/5"
              )}>
                <td className="px-2 py-2 text-xs text-muted-foreground text-center border-r border-border font-mono">
                  {row.index + 1}
                </td>
                {row.tracks.map((track, editionIdx) => (
                  <TrackCell
                    key={editionIdx}
                    track={track}
                    editionIndex={editionIdx}
                    rowIndex={row.index}
                    selectedEditionIndex={selectedEditionIndex}
                    isBonus={track ? (track.is_bonus || trackMatchesMergeList(track, mergeList)) : false}
                    isOnlyHere={row.isOnlyInOne && row.presentInEditions[0] === editionIdx}
                    isBitPerfect={row.isBitPerfect}
                    artist={artist}
                    album={editions[editionIdx]?.title_raw || ''}
                    isSelected={selectedTracks.has(`${editionIdx}-${row.index}`)}
                    onSelect={(selected) => handleTrackSelect(editionIdx, row.index, selected)}
                    showMergeControls={showMergeControls}
                  />
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Merge confirmation dialog */}
      <AlertDialog open={confirmMergeOpen} onOpenChange={setConfirmMergeOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Merge selected tracks?</AlertDialogTitle>
            <AlertDialogDescription>
              Move {selectedTracks.size} track(s) into the kept edition (Ed. {selectedEditionIndex + 1}). This will physically move files.
            </AlertDialogDescription>
          </AlertDialogHeader>
          {selectedTrackDetails.length > 0 && (
            <div className="max-h-40 overflow-auto text-sm text-muted-foreground border rounded p-2">
              {selectedTrackDetails.map((t, idx) => (
                <div key={idx} className="flex items-center gap-2">
                  <span className="text-xs text-muted-foreground">Ed.{t.editionIdx + 1}</span>
                  <span className="truncate">{t.name}</span>
                </div>
              ))}
            </div>
          )}
          {lastMoved.count > 0 && (
            <div className="mt-2 rounded border border-border bg-muted/50 px-3 py-2 text-xs text-foreground">
              Last merge: {lastMoved.count} moved{lastMoved.dest ? ` → ${lastMoved.dest}` : ''}.
            </div>
          )}
          <AlertDialogFooter>
            <AlertDialogCancel disabled={movingTracks}>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={handleMergeSelected} disabled={movingTracks}>
              {movingTracks ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin mr-2" />
                  Merging…
                </>
              ) : (
                'Confirm merge'
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
