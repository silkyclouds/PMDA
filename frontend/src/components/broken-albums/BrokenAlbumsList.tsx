import { useState, useEffect, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { AlertCircle, Disc3, Loader2, Music, Search, Undo2 } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { ScanMoveReviewDialog } from '@/components/scan-moves/ScanMoveReviewDialog';
import { ProviderBadge } from '@/components/providers/ProviderBadge';
import * as api from '@/lib/api';
import { formatBadgeDateTime } from '@/lib/dateFormat';
import { useToast } from '@/hooks/use-toast';

function fmtMissingIndices(raw: unknown): string {
  if (!Array.isArray(raw)) return '';
  const chunks: string[] = [];
  for (const row of raw) {
    if (!Array.isArray(row) || row.length < 2) continue;
    const start = Number(row[0]);
    const end = Number(row[1]);
    if (!Number.isFinite(start) || !Number.isFinite(end)) continue;
    chunks.push(`${start}-${end}`);
  }
  return chunks.join(', ');
}

export function BrokenAlbumsList() {
  const [searchParams] = useSearchParams();
  const scanId = useMemo(() => {
    const raw = searchParams.get('scan_id');
    const parsed = Number(raw);
    return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : null;
  }, [searchParams]);

  const [brokenAlbums, setBrokenAlbums] = useState<api.BrokenAlbum[]>([]);
  const [movedIncompletes, setMovedIncompletes] = useState<api.ScanMove[]>([]);
  const [movesFilter, setMovesFilter] = useState<'all' | 'active' | 'restored'>('all');
  const [loading, setLoading] = useState(true);
  const [restoring, setRestoring] = useState<number | 'all' | null>(null);
  const [selectedMove, setSelectedMove] = useState<api.ScanMove | null>(null);
  const { toast } = useToast();

  useEffect(() => {
    void loadData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scanId]);

  const loadData = async () => {
    try {
      setLoading(true);
      const [albums, moves] = await Promise.all([
        api.getBrokenAlbums(),
        scanId ? api.getScanMoves(scanId, { reason: 'incomplete', status: 'all' }) : Promise.resolve([]),
      ]);
      setBrokenAlbums(albums);
      const runIncompleteMoves = (moves || []).filter((m) => (m.move_reason || '').toLowerCase() === 'incomplete');
      setMovedIncompletes(runIncompleteMoves);
    } catch {
      toast({
        title: 'Error',
        description: 'Failed to load incomplete albums review',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  const restoreMoves = async (moveIds?: number[]) => {
    if (!scanId) return;
    const selected = Array.isArray(moveIds) ? moveIds.filter((v) => Number.isFinite(v)) : [];
    if (selected.length === 0) {
      toast({
        title: 'Nothing to restore',
        description: 'No moved incomplete albums to restore for this run.',
      });
      return;
    }
    setRestoring(selected.length > 1 ? 'all' : selected[0]);
    try {
      const res = await api.restoreMoves(scanId, selected, false);
      toast({
        title: 'Restore complete',
        description: `${res.restored} incomplete move(s) restored.`,
      });
      await loadData();
    } catch (error) {
      toast({
        title: 'Restore failed',
        description: error instanceof Error ? error.message : 'Unable to restore incomplete moves',
        variant: 'destructive',
      });
    } finally {
      setRestoring(null);
    }
  };
  const movedIncompletesFiltered = useMemo(() => {
    if (movesFilter === 'active') return movedIncompletes.filter((m) => String(m.status || (m.restored ? 'restored' : 'moved')).toLowerCase() !== 'restored');
    if (movesFilter === 'restored') return movedIncompletes.filter((m) => String(m.status || (m.restored ? 'restored' : 'moved')).toLowerCase() === 'restored');
    return movedIncompletes;
  }, [movedIncompletes, movesFilter]);
  const movedIncompletesPending = useMemo(
    () => movedIncompletes.filter((m) => String(m.status || '').toLowerCase() !== 'restored'),
    [movedIncompletes],
  );

  return (
    <div className="container py-6 space-y-4">
      <div className="flex items-center justify-between gap-2">
        <div>
          <h2 className="text-2xl font-bold">Incomplete Albums</h2>
          <p className="text-sm text-muted-foreground mt-1">
            Albums with missing tracks detected by PMDA.
          </p>
        </div>
        {scanId ? <Badge variant="outline">Run #{scanId}</Badge> : null}
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="w-8 h-8 animate-spin text-primary" />
        </div>
      ) : (
        <>
          {scanId ? (
            <Card>
              <CardHeader>
                <CardTitle>Moved to Incomplete Quarantine</CardTitle>
                <CardDescription>
                  Review why each album was moved during this run and restore if needed.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex items-center gap-2">
                  <Badge variant="outline">{movedIncompletes.length} moved album(s)</Badge>
                  <Badge variant="outline">{movedIncompletesPending.length} active move(s)</Badge>
                  <Button size="sm" variant={movesFilter === 'all' ? 'default' : 'outline'} onClick={() => setMovesFilter('all')}>
                    All
                  </Button>
                  <Button size="sm" variant={movesFilter === 'active' ? 'default' : 'outline'} onClick={() => setMovesFilter('active')}>
                    Moved
                  </Button>
                  <Button size="sm" variant={movesFilter === 'restored' ? 'default' : 'outline'} onClick={() => setMovesFilter('restored')}>
                    Restored
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    className="gap-1.5"
                    onClick={() => void restoreMoves(movedIncompletesPending.map((m) => m.move_id))}
                    disabled={movedIncompletesPending.length === 0 || restoring !== null}
                  >
                    {restoring === 'all' ? <Loader2 className="w-4 h-4 animate-spin" /> : <Undo2 className="w-4 h-4" />}
                    Restore all
                  </Button>
                </div>

                {movedIncompletesFiltered.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No incomplete moves for this filter in this run.</p>
                ) : (
                  <div className="space-y-3">
                    {movedIncompletesFiltered.map((move) => {
                      const details = (move.details || {}) as Record<string, unknown>;
                      const expected = Number(details.expected_track_count ?? 0);
                      const actual = Number(details.actual_track_count ?? 0);
                      const missing = fmtMissingIndices(details.missing_indices);
                      const reason = move.reason_label || move.decision_reason || String(details.classification || 'Incomplete album');
                      const status = String(move.status || (move.restored ? 'restored' : 'moved')).toLowerCase();
                      return (
                        <div
                          key={move.move_id}
                          className="rounded-xl border p-4 space-y-3 cursor-pointer hover:border-primary/40 transition-colors"
                          onClick={() => setSelectedMove(move)}
                          role="button"
                          tabIndex={0}
                          onKeyDown={(event) => {
                            if (event.key === 'Enter' || event.key === ' ') {
                              event.preventDefault();
                              setSelectedMove(move);
                            }
                          }}
                        >
                          <div className="flex gap-4">
                            <div className="w-20 h-20 rounded-2xl overflow-hidden border border-border/70 bg-muted shrink-0">
                              {move.thumb_url ? (
                                <img src={move.thumb_url} alt={move.album_title || move.artist} className="w-full h-full object-cover" />
                              ) : (
                                <div className="w-full h-full flex items-center justify-center">
                                  <Disc3 className="w-5 h-5 text-muted-foreground" />
                                </div>
                              )}
                            </div>
                            <div className="min-w-0 flex-1 space-y-2">
                              <div className="flex flex-wrap items-center gap-2">
                                <span className="font-medium">{move.artist}</span>
                                <span className="text-muted-foreground">•</span>
                                <span>{move.album_title || `Album #${move.album_id}`}</span>
                                <Badge variant="outline">{reason}</Badge>
                                <Badge variant="outline">Moved: {formatBadgeDateTime(move.moved_at)}</Badge>
                                <Badge variant={status === 'restored' ? 'secondary' : 'default'}>
                                  {status === 'restored' ? 'Restored' : status === 'missing' ? 'Missing' : 'Moved'}
                                </Badge>
                                {move.decision_provider ? <ProviderBadge provider={move.decision_provider} prefix="Provider" className="text-[10px]" /> : null}
                              </div>
                              <div className="text-xs text-muted-foreground break-all">
                                From: {move.original_path}
                              </div>
                              <div className="text-xs text-muted-foreground break-all">
                                To: {move.moved_to_path}
                              </div>
                              <div className="flex flex-wrap items-center gap-2 text-xs">
                                {expected > 0 ? <Badge variant="outline">Expected {expected}</Badge> : null}
                                {actual > 0 ? <Badge variant="outline">Actual {actual}</Badge> : null}
                                {missing ? <Badge variant="outline">Missing {missing}</Badge> : null}
                              </div>
                            </div>
                          </div>
                          <div className="flex items-center gap-2">
                            <Button
                              size="sm"
                              variant="outline"
                              className="gap-1.5"
                              onClick={(event) => {
                                event.stopPropagation();
                                setSelectedMove(move);
                              }}
                            >
                              <Search className="w-4 h-4" />
                              Review
                            </Button>
                            <Button
                              size="sm"
                              variant="outline"
                              className="gap-1.5"
                              onClick={(event) => {
                                event.stopPropagation();
                                void restoreMoves([move.move_id]);
                              }}
                              disabled={restoring !== null || status === 'restored'}
                            >
                              {restoring === move.move_id ? <Loader2 className="w-4 h-4 animate-spin" /> : <Undo2 className="w-4 h-4" />}
                              Rollback
                            </Button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </CardContent>
            </Card>
          ) : null}

          {brokenAlbums.length === 0 && (!scanId || movedIncompletes.length === 0) ? (
            <Alert>
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>
                No incomplete albums found. All albums appear to have complete track listings.
              </AlertDescription>
            </Alert>
          ) : (
            <div className="grid gap-4">
              {brokenAlbums.map((album) => (
                <Card key={`${album.artist}-${album.album_id}`}>
                  <CardHeader>
                    <div className="flex items-start justify-between">
                      <div className="flex-1">
                        <CardTitle className="flex items-center gap-2">
                          <Music className="w-5 h-5 text-muted-foreground" />
                          {album.album_title}
                        </CardTitle>
                        <CardDescription className="mt-1">{album.artist}</CardDescription>
                      </div>
                      {album.sent_to_lidarr ? (
                        <Badge variant="outline" className="bg-muted">
                          Legacy Lidarr flag
                        </Badge>
                      ) : null}
                    </div>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-3">
                      <div className="flex items-center gap-4 text-sm">
                        <div>
                          <span className="text-muted-foreground">Tracks found:</span>{' '}
                          <span className="font-medium">{album.actual_track_count}</span>
                          {album.expected_track_count ? (
                            <>
                              {' '}/ <span className="text-muted-foreground">{album.expected_track_count} expected</span>
                            </>
                          ) : null}
                        </div>
                        {album.missing_indices.length > 0 ? (
                          <div>
                            <span className="text-muted-foreground">Missing gaps:</span>{' '}
                            <span className="font-medium">
                              {album.missing_indices.map(([start, end]) => `${start}-${end}`).join(', ')}
                            </span>
                          </div>
                        ) : null}
                      </div>

                      {album.musicbrainz_release_group_id ? (
                        <div className="text-xs text-muted-foreground">
                          MusicBrainz ID: {album.musicbrainz_release_group_id}
                        </div>
                      ) : null}
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
        </>
      )}

      <ScanMoveReviewDialog
        move={selectedMove}
        open={Boolean(selectedMove)}
        onOpenChange={(open) => {
          if (!open) setSelectedMove(null);
        }}
        restoring={selectedMove ? restoring === selectedMove.move_id : false}
        onRestore={async (moveId) => {
          await restoreMoves([moveId]);
          setSelectedMove(null);
        }}
      />
    </div>
  );
}
