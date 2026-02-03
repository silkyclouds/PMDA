import { useState, useEffect } from 'react';
import { format } from 'date-fns';
import { Loader2, RotateCcw, Trash2, CheckSquare, Square, Package, AlertTriangle, Image, Tag, Database, Music } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { toast } from 'sonner';
import * as api from '@/lib/api';
import type { ScanHistoryEntry, ScanMove } from '@/lib/api';

interface ScanDetailsProps {
  scanId: number;
  onRestore: () => void;
}

export function ScanDetails({ scanId, onRestore }: ScanDetailsProps) {
  const [scan, setScan] = useState<ScanHistoryEntry | null>(null);
  const [moves, setMoves] = useState<ScanMove[]>([]);
  const [selectedMoves, setSelectedMoves] = useState<Set<number>>(new Set());
  const [isLoading, setIsLoading] = useState(true);
  const [isRestoring, setIsRestoring] = useState(false);
  const [isDeduping, setIsDeduping] = useState(false);

  useEffect(() => {
    loadDetails();
  }, [scanId]);

  const loadDetails = async () => {
    setIsLoading(true);
    try {
      const [scanData, movesData] = await Promise.all([
        api.getScanDetails(scanId),
        api.getScanMoves(scanId),
      ]);
      setScan(scanData);
      setMoves(movesData.filter(m => !m.restored));
      setSelectedMoves(new Set());
    } catch (error) {
      console.error('Failed to load scan details:', error);
      toast.error('Failed to load scan details');
    } finally {
      setIsLoading(false);
    }
  };

  const toggleMove = (moveId: number) => {
    const newSelected = new Set(selectedMoves);
    if (newSelected.has(moveId)) {
      newSelected.delete(moveId);
    } else {
      newSelected.add(moveId);
    }
    setSelectedMoves(newSelected);
  };

  const selectAll = () => {
    setSelectedMoves(new Set(moves.map(m => m.move_id)));
  };

  const deselectAll = () => {
    setSelectedMoves(new Set());
  };

  const handleRestore = async (all: boolean) => {
    setIsRestoring(true);
    try {
      const moveIds = all ? undefined : Array.from(selectedMoves);
      const result = await api.restoreMoves(scanId, moveIds, all);
      toast.success(`Restored ${result.restored} album(s)`);
      if (result.restored_paths && result.restored_paths.length > 0) {
        const first = result.restored_paths[0];
        const pathMsg = result.restored_paths.length === 1
          ? `Restored to: ${first.to}`
          : `First restored to: ${first.to}${result.restored_paths.length > 1 ? ` (+${result.restored_paths.length - 1} more)` : ''}`;
        toast.info(pathMsg, { duration: 6000 });
      }
      onRestore();
      loadDetails();
    } catch (error: any) {
      toast.error(error?.message || 'Failed to restore albums');
    } finally {
      setIsRestoring(false);
    }
  };

  const handleDedupe = async () => {
    setIsDeduping(true);
    try {
      await api.dedupeScan(scanId);
      toast.success('Deduplication started');
    } catch (error: any) {
      toast.error(error?.message || 'Failed to start deduplication');
    } finally {
      setIsDeduping(false);
    }
  };

  if (isLoading) {
    return (
      <div className="rounded-lg border bg-card p-8 flex items-center justify-center">
        <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!scan) {
    return (
      <div className="rounded-lg border bg-card p-8 text-center text-muted-foreground">
        Scan not found
      </div>
    );
  }

  const formatDuration = (seconds?: number) => {
    if (!seconds) return 'N/A';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    if (hours > 0) return `${hours}h ${minutes}m ${secs}s`;
    if (minutes > 0) return `${minutes}m ${secs}s`;
    return `${secs}s`;
  };

  const isDedupe = scan.entry_type === 'dedupe';

  return (
    <div className="rounded-lg border bg-card">
      <div className="p-4 border-b">
        <h2 className="text-lg font-semibold">
          {isDedupe ? 'Dedupe Details' : 'Scan Details'}
        </h2>
        <p className="text-sm text-muted-foreground mt-1">
          {format(new Date(scan.start_time * 1000), 'PPp')}
        </p>
      </div>

      <div className="p-4 space-y-4">
        {/* Basic Statistics */}
        <div>
          <h3 className="text-sm font-semibold mb-3">Basic Statistics</h3>
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <div className="text-muted-foreground">Duration</div>
              <div className="font-medium">{formatDuration(scan.duration_seconds)}</div>
            </div>
            {!isDedupe && (
              <>
                <div>
                  <div className="text-muted-foreground">Albums Scanned</div>
                  <div className="font-medium">{scan.albums_scanned.toLocaleString()}</div>
                </div>
                <div>
                  <div className="text-muted-foreground">Duplicates Found</div>
                  <div className="font-medium">{(scan.duplicate_groups_count ?? scan.duplicates_found ?? 0).toLocaleString()}</div>
                </div>
                <div>
                  <div className="text-muted-foreground">Artists Processed</div>
                  <div className="font-medium">
                    {scan.artists_processed.toLocaleString()} / {scan.artists_total.toLocaleString()}
                  </div>
                </div>
              </>
            )}
            {scan.albums_moved > 0 && (
              <>
                <div>
                  <div className="text-muted-foreground">Albums Moved</div>
                  <div className="font-medium">{scan.albums_moved.toLocaleString()}</div>
                </div>
                <div>
                  <div className="text-muted-foreground">Space Saved</div>
                  <div className="font-medium">{scan.space_saved_mb.toLocaleString()} MB</div>
                </div>
              </>
            )}
          </div>
        </div>

        {/* Detailed Statistics */}
        {((scan.duplicate_groups_count ?? 0) > 0 || (scan.total_duplicates_count ?? 0) > 0 || (scan.broken_albums_count ?? 0) > 0 || 
          (scan.missing_albums_count ?? 0) > 0 || (scan.albums_without_mb_id ?? 0) > 0 || (scan.albums_without_artist_mb_id ?? 0) > 0 ||
          (scan.albums_without_complete_tags ?? 0) > 0 || (scan.albums_without_album_image ?? 0) > 0 || (scan.albums_without_artist_image ?? 0) > 0) && (
          <div>
            <h3 className="text-sm font-semibold mb-3">Detailed Statistics</h3>
            <div className="grid grid-cols-2 gap-3 text-sm">
              {(scan.duplicate_groups_count ?? 0) > 0 && (
                <div className="flex items-center gap-2">
                  <Package className="w-4 h-4 text-orange-500" />
                  <div className="flex-1">
                    <div className="text-muted-foreground">Duplicate Groups</div>
                    <div className="font-medium">{(scan.duplicate_groups_count ?? 0).toLocaleString()}</div>
                  </div>
                </div>
              )}
              {(scan.total_duplicates_count ?? 0) > 0 && (
                <div className="flex items-center gap-2">
                  <Music className="w-4 h-4 text-red-500" />
                  <div className="flex-1">
                    <div className="text-muted-foreground">Total Duplicates</div>
                    <div className="font-medium">{(scan.total_duplicates_count ?? 0).toLocaleString()}</div>
                  </div>
                </div>
              )}
              {(scan.broken_albums_count ?? 0) > 0 && (
                <div className="flex items-center gap-2">
                  <AlertTriangle className="w-4 h-4 text-red-500" />
                  <div className="flex-1">
                    <div className="text-muted-foreground">Incomplete albums</div>
                    <div className="font-medium text-red-600 dark:text-red-400">{(scan.broken_albums_count ?? 0).toLocaleString()}</div>
                  </div>
                </div>
              )}
              {(scan.missing_albums_count ?? 0) > 0 && (
                <div className="flex items-center gap-2">
                  <Music className="w-4 h-4 text-yellow-500" />
                  <div className="flex-1">
                    <div className="text-muted-foreground">Missing Albums</div>
                    <div className="font-medium text-yellow-600 dark:text-yellow-400">{(scan.missing_albums_count ?? 0).toLocaleString()}</div>
                  </div>
                </div>
              )}
              {(scan.albums_without_mb_id ?? 0) > 0 && (
                <div className="flex items-center gap-2">
                  <Database className="w-4 h-4 text-blue-500" />
                  <div className="flex-1">
                    <div className="text-muted-foreground">Without MB ID</div>
                    <div className="font-medium">{(scan.albums_without_mb_id ?? 0).toLocaleString()}</div>
                  </div>
                </div>
              )}
              {(scan.albums_without_artist_mb_id ?? 0) > 0 && (
                <div className="flex items-center gap-2">
                  <Database className="w-4 h-4 text-blue-500" />
                  <div className="flex-1">
                    <div className="text-muted-foreground">Without Artist MB ID</div>
                    <div className="font-medium">{(scan.albums_without_artist_mb_id ?? 0).toLocaleString()}</div>
                  </div>
                </div>
              )}
              {(scan.albums_without_complete_tags ?? 0) > 0 && (
                <div className="flex items-center gap-2">
                  <Tag className="w-4 h-4 text-purple-500" />
                  <div className="flex-1">
                    <div className="text-muted-foreground">Incomplete Tags</div>
                    <div className="font-medium">{(scan.albums_without_complete_tags ?? 0).toLocaleString()}</div>
                  </div>
                </div>
              )}
              {(scan.albums_without_album_image ?? 0) > 0 && (
                <div className="flex items-center gap-2">
                  <Image className="w-4 h-4 text-gray-500" />
                  <div className="flex-1">
                    <div className="text-muted-foreground">Without Album Image</div>
                    <div className="font-medium">{(scan.albums_without_album_image ?? 0).toLocaleString()}</div>
                  </div>
                </div>
              )}
              {(scan.albums_without_artist_image ?? 0) > 0 && (
                <div className="flex items-center gap-2">
                  <Image className="w-4 h-4 text-gray-500" />
                  <div className="flex-1">
                    <div className="text-muted-foreground">Without Artist Image</div>
                    <div className="font-medium">{(scan.albums_without_artist_image ?? 0).toLocaleString()}</div>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Actions */}
        {!isDedupe && !scan.auto_move_enabled && scan.duplicates_found > 0 && (
          <Button
            onClick={handleDedupe}
            disabled={isDeduping}
            className="w-full"
            variant="default"
          >
            {isDeduping ? (
              <>
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                Starting...
              </>
            ) : (
              'Dedupe Now'
            )}
          </Button>
        )}

        {/* Moves list */}
        {moves.length > 0 && (
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <h3 className="text-sm font-medium">Moved Albums ({moves.length})</h3>
              <div className="flex gap-2">
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={selectAll}
                  className="h-7 text-xs"
                >
                  Select All
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={deselectAll}
                  className="h-7 text-xs"
                >
                  Deselect All
                </Button>
              </div>
            </div>
            <div className="max-h-64 overflow-y-auto space-y-1 border rounded-lg p-2">
              {moves.map((move) => (
                <label
                  key={move.move_id}
                  className="flex items-center gap-2 p-2 rounded hover:bg-muted cursor-pointer"
                >
                  <input
                    type="checkbox"
                    checked={selectedMoves.has(move.move_id)}
                    onChange={() => toggleMove(move.move_id)}
                    className="rounded"
                  />
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-medium truncate">{move.artist}</div>
                    <div className="text-xs text-muted-foreground truncate">
                      {move.album_title ? `"${move.album_title}"` : move.original_path.split(/[/\\]/).pop() || '—'}
                      {move.fmt_text ? ` · ${move.fmt_text}` : ''}
                      {' · '}{move.size_mb} MB
                    </div>
                  </div>
                </label>
              ))}
            </div>
            <div className="flex gap-2">
              <Button
                onClick={() => handleRestore(false)}
                disabled={isRestoring || selectedMoves.size === 0}
                variant="outline"
                className="flex-1"
                size="sm"
              >
                {isRestoring ? (
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                ) : (
                  <RotateCcw className="w-4 h-4 mr-2" />
                )}
                Restore Selected
              </Button>
              <Button
                onClick={() => handleRestore(true)}
                disabled={isRestoring}
                variant="outline"
                className="flex-1"
                size="sm"
              >
                {isRestoring ? (
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                ) : (
                  <RotateCcw className="w-4 h-4 mr-2" />
                )}
                Restore All
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
