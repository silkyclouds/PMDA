import { useState, useEffect } from 'react';
import { format } from 'date-fns';
import { Loader2, RotateCcw, Trash2, CheckSquare, Square, Package, AlertTriangle, Image, Tag, Database, Music, Sparkles } from 'lucide-react';
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
    if (seconds == null || seconds === undefined) return 'N/A';
    if (seconds === 0) return '0s';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    if (hours > 0) return `${hours}h ${minutes}m ${secs}s`;
    if (minutes > 0) return `${minutes}m ${secs}s`;
    return `${secs}s`;
  };

  const isDedupe = scan.entry_type === 'dedupe';
  const summary = scan.summary_json ?? {};
  const albumsScanned =
    (summary.albums_scanned as number | undefined) ?? scan.albums_scanned ?? 0;
  const albumsWithoutAlbumImage =
    (summary.albums_without_album_image as number | undefined) ??
    scan.albums_without_album_image ??
    0;
  const albumsWithoutArtistImage =
    (summary.albums_without_artist_image as number | undefined) ??
    scan.albums_without_artist_image ??
    0;
  const albumsWithoutCompleteTags =
    (summary.albums_without_complete_tags as number | undefined) ??
    scan.albums_without_complete_tags ??
    0;
  const withCover = Math.max(0, albumsScanned - albumsWithoutAlbumImage);
  const withArtistImage = Math.max(0, albumsScanned - albumsWithoutArtistImage);
  const incompleteAny = Math.max(
    albumsWithoutCompleteTags || 0,
    albumsWithoutAlbumImage || 0,
    albumsWithoutArtistImage || 0
  );
  const completeAlbums = Math.max(0, albumsScanned - incompleteAny);
  const mbAlbums = (summary.albums_with_mb_id as number | undefined) ?? 0;
  const discogsMatches = (summary.scan_discogs_matched as number | undefined) ?? 0;
  const lastfmMatches = (summary.scan_lastfm_matched as number | undefined) ?? 0;
  const bandcampMatches = (summary.scan_bandcamp_matched as number | undefined) ?? 0;
  const matchedAlbums = Math.min(
    albumsScanned,
    Math.max(0, mbAlbums + discogsMatches + lastfmMatches + bandcampMatches)
  );
  const duplicateSummaryAvailable =
    typeof summary.duplicate_groups_total === 'number' ||
    typeof summary.duplicate_groups_ai_decided === 'number';
  const hasProviderSummary =
    (summary.mb_albums_identified ?? 0) > 0 ||
    (summary.scan_discogs_matched ?? 0) > 0 ||
    (summary.scan_lastfm_matched ?? 0) > 0 ||
    (summary.scan_bandcamp_matched ?? 0) > 0;
  const formatPercent = (num: number, denom: number) =>
    denom > 0 ? `${((num / denom) * 100).toFixed(1)}%` : 'N/A';

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

        {/* Metadata & AI summary (full scan only) */}
        {!isDedupe && scan.entry_type !== 'incomplete' && (hasProviderSummary || duplicateSummaryAvailable) && (
          <div>
            <h3 className="text-sm font-semibold mb-3">Metadata & AI Summary</h3>
            <div className="grid grid-cols-1 md:grid-cols-4 gap-3 text-sm">
              {/* PMDA album health */}
              {albumsScanned > 0 && (
                <div className="rounded-lg border border-border bg-muted/40 p-3 space-y-1.5">
                  <div className="flex items-center gap-2">
                    <Sparkles className="w-4 h-4 text-emerald-500" />
                    <span className="font-medium text-foreground text-sm">PMDA album health</span>
                  </div>
                  <ul className="text-xs text-muted-foreground space-y-0.5">
                    <li>
                      Matched albums:{' '}
                      <span className="font-medium text-foreground">
                        {matchedAlbums.toLocaleString()} / {albumsScanned.toLocaleString()} (
                        {formatPercent(matchedAlbums, albumsScanned)})
                      </span>
                    </li>
                    <li>
                      With cover art:{' '}
                      <span className="font-medium text-foreground">
                        {withCover.toLocaleString()} / {albumsScanned.toLocaleString()} (
                        {formatPercent(withCover, albumsScanned)})
                      </span>
                    </li>
                    <li>
                      With artist image:{' '}
                      <span className="font-medium text-foreground">
                        {withArtistImage.toLocaleString()} / {albumsScanned.toLocaleString()} (
                        {formatPercent(withArtistImage, albumsScanned)})
                      </span>
                    </li>
                    <li>
                      Fully complete (tags + cover + artist):{' '}
                      <span className="font-medium text-foreground">
                        {completeAlbums.toLocaleString()} / {albumsScanned.toLocaleString()} (
                        {formatPercent(completeAlbums, albumsScanned)})
                      </span>
                    </li>
                  </ul>
                </div>
              )}

              {hasProviderSummary && (
                <div className="rounded-lg border border-border bg-muted/40 p-3 space-y-1.5">
                  <div className="flex items-center gap-2">
                    <Database className="w-4 h-4 text-blue-500" />
                    <span className="font-medium text-foreground text-sm">Metadata sources</span>
                  </div>
                  <ul className="text-xs text-muted-foreground space-y-0.5">
                    <li>
                      MusicBrainz:{' '}
                      <span className="font-medium text-foreground">
                        {summary.mb_albums_identified ?? 0} album(s)
                        {matchedAlbums > 0 && (
                          <> ({formatPercent(summary.mb_albums_identified ?? 0, matchedAlbums)})</>
                        )}
                      </span>
                    </li>
                    <li>
                      Discogs:{' '}
                      <span className="font-medium text-foreground">
                        {summary.scan_discogs_matched ?? 0} album(s)
                        {matchedAlbums > 0 && (
                          <> ({formatPercent(summary.scan_discogs_matched ?? 0, matchedAlbums)})</>
                        )}
                      </span>
                    </li>
                    <li>
                      Last.fm:{' '}
                      <span className="font-medium text-foreground">
                        {summary.scan_lastfm_matched ?? 0} album(s)
                        {matchedAlbums > 0 && (
                          <> ({formatPercent(summary.scan_lastfm_matched ?? 0, matchedAlbums)})</>
                        )}
                      </span>
                    </li>
                    <li>
                      Bandcamp:{' '}
                      <span className="font-medium text-foreground">
                        {summary.scan_bandcamp_matched ?? 0} album(s)
                        {matchedAlbums > 0 && (
                          <> ({formatPercent(summary.scan_bandcamp_matched ?? 0, matchedAlbums)})</>
                        )}
                      </span>
                    </li>
                  </ul>
                </div>
              )}

              {duplicateSummaryAvailable && (
                <div className="rounded-lg border border-border bg-muted/40 p-3 space-y-1.5">
                  <div className="flex items-center gap-2">
                    <Package className="w-4 h-4 text-orange-500" />
                    <span className="font-medium text-foreground text-sm">Duplicate decisions</span>
                  </div>
                  <ul className="text-xs text-muted-foreground space-y-0.5">
                    <li>
                      Groups (saved):{' '}
                      <span className="font-medium text-foreground">
                        {summary.duplicate_groups_saved ?? summary.duplicate_groups_total ?? scan.duplicate_groups_count ?? 0}
                      </span>
                    </li>
                    <li>
                      AI-decided groups:{' '}
                      <span className="font-medium text-foreground">
                        {summary.duplicate_groups_ai_decided ?? 0}
                      </span>
                    </li>
                    <li>
                      AI errors:{' '}
                      <span className="font-medium text-foreground">
                        {summary.duplicate_groups_ai_failed_total ?? 0}
                      </span>
                      {summary.duplicate_groups_ai_failed_then_recovered != null && (
                        <> (recovered: {summary.duplicate_groups_ai_failed_then_recovered}, unresolved: {summary.duplicate_groups_ai_failed_unresolved ?? 0})</>
                      )}
                    </li>
                  </ul>
                </div>
              )}

              {(summary.cover_from_mb ?? 0) + (summary.cover_from_discogs ?? 0) + (summary.cover_from_lastfm ?? 0) + (summary.cover_from_bandcamp ?? 0) > 0 && (
                <div className="rounded-lg border border-border bg-muted/40 p-3 space-y-1.5">
                  <div className="flex items-center gap-2">
                    <Image className="w-4 h-4 text-teal-500" />
                    <span className="font-medium text-foreground text-sm">Covers fetched</span>
                  </div>
                  <ul className="text-xs text-muted-foreground space-y-0.5">
                    <li>
                      MusicBrainz / CAA:{' '}
                      <span className="font-medium text-foreground">
                        {summary.cover_from_mb ?? 0}
                        {' '}
                        {formatPercent(
                          summary.cover_from_mb ?? 0,
                          (summary.cover_from_mb ?? 0) +
                            (summary.cover_from_discogs ?? 0) +
                            (summary.cover_from_lastfm ?? 0) +
                            (summary.cover_from_bandcamp ?? 0)
                        )}
                      </span>
                    </li>
                    <li>
                      Discogs:{' '}
                      <span className="font-medium text-foreground">
                        {summary.cover_from_discogs ?? 0}
                        {' '}
                        {formatPercent(
                          summary.cover_from_discogs ?? 0,
                          (summary.cover_from_mb ?? 0) +
                            (summary.cover_from_discogs ?? 0) +
                            (summary.cover_from_lastfm ?? 0) +
                            (summary.cover_from_bandcamp ?? 0)
                        )}
                      </span>
                    </li>
                    <li>
                      Last.fm:{' '}
                      <span className="font-medium text-foreground">
                        {summary.cover_from_lastfm ?? 0}
                        {' '}
                        {formatPercent(
                          summary.cover_from_lastfm ?? 0,
                          (summary.cover_from_mb ?? 0) +
                            (summary.cover_from_discogs ?? 0) +
                            (summary.cover_from_lastfm ?? 0) +
                            (summary.cover_from_bandcamp ?? 0)
                        )}
                      </span>
                    </li>
                    <li>
                      Bandcamp:{' '}
                      <span className="font-medium text-foreground">
                        {summary.cover_from_bandcamp ?? 0}
                        {' '}
                        {formatPercent(
                          summary.cover_from_bandcamp ?? 0,
                          (summary.cover_from_mb ?? 0) +
                            (summary.cover_from_discogs ?? 0) +
                            (summary.cover_from_lastfm ?? 0) +
                            (summary.cover_from_bandcamp ?? 0)
                        )}
                      </span>
                    </li>
                  </ul>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Steps executed (full scan only) */}
        {!isDedupe && scan.entry_type !== 'incomplete' && Array.isArray(scan.steps_executed) && scan.steps_executed.length > 0 && (
          <div>
            <h3 className="text-sm font-semibold mb-3">Steps Executed</h3>
            <ul className="list-decimal list-inside space-y-1.5 text-sm text-muted-foreground">
              {scan.steps_executed.map((step, i) => (
                <li key={i} className="leading-snug">{step}</li>
              ))}
            </ul>
            {Array.isArray(summary.ai_errors) && summary.ai_errors.length > 0 && (
              <div className="mt-3 rounded-md border border-amber-300/70 bg-amber-50 dark:bg-amber-950/30 p-3">
                <div className="flex items-center gap-2 mb-1.5">
                  <Sparkles className="w-4 h-4 text-amber-500" />
                  <p className="text-xs font-semibold text-amber-700 dark:text-amber-300">
                    AI diagnostics
                  </p>
                </div>
                <ul className="space-y-0.5 text-[11px] text-amber-800 dark:text-amber-200">
                  {summary.ai_errors.slice(0, 3).map((err, idx) => (
                    <li key={idx} className="leading-snug">
                      {err.group ? <span className="font-medium">{err.group}:</span> : null}{' '}
                      {err.message}
                    </li>
                  ))}
                  {summary.ai_errors.length > 3 && (
                    <li className="opacity-80">… and {summary.ai_errors.length - 3} more AI warning(s)</li>
                  )}
                </ul>
              </div>
            )}
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
