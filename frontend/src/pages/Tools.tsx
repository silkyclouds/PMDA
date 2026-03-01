import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { AlertTriangle, ChevronLeft, ChevronRight, Loader2, RefreshCw, Tags, Trash2, Undo2 } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { toast } from 'sonner';
import * as api from '@/lib/api';

function fmtDate(ts?: number): string {
  if (!ts) return 'N/A';
  return new Date(ts * 1000).toLocaleString();
}

function fmtDurationSeconds(value?: number): string {
  const sec = Math.max(0, Number(value || 0));
  if (!sec) return '0s';
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

export default function Tools() {
  const navigate = useNavigate();
  const [restoring, setRestoring] = useState<'dedupe' | 'incomplete' | null>(null);
  const [selectedScanId, setSelectedScanId] = useState<number | null>(null);

  const { data: history, isLoading: loadingHistory, refetch: refetchHistory } = useQuery({
    queryKey: ['scan-history-tools'],
    queryFn: api.getScanHistory,
    refetchInterval: 5000,
  });

  const completedRuns = useMemo(() => {
    const rows = Array.isArray(history) ? history : [];
    return rows
      .filter((r) => r.status === 'completed')
      .sort((a, b) => Number(b.scan_id) - Number(a.scan_id));
  }, [history]);

  useEffect(() => {
    if (completedRuns.length === 0) {
      setSelectedScanId(null);
      return;
    }
    if (selectedScanId == null) {
      setSelectedScanId(completedRuns[0].scan_id);
      return;
    }
    if (!completedRuns.some((r) => r.scan_id === selectedScanId)) {
      setSelectedScanId(completedRuns[0].scan_id);
    }
  }, [completedRuns, selectedScanId]);

  const selectedIndex = useMemo(() => {
    if (selectedScanId == null) return -1;
    return completedRuns.findIndex((r) => r.scan_id === selectedScanId);
  }, [completedRuns, selectedScanId]);

  const selectedRun = useMemo(() => {
    if (selectedIndex < 0) return null;
    return completedRuns[selectedIndex] ?? null;
  }, [completedRuns, selectedIndex]);

  const selectedSummary = selectedRun?.summary_json ?? null;
  const selectedAlbumsDone = Number(selectedRun?.albums_scanned ?? 0);
  const selectedAlbumsTotal = Number(
    selectedSummary?.strict_total_albums
      ?? selectedSummary?.albums_scanned
      ?? selectedRun?.albums_scanned
      ?? 0,
  );
  const selectedDurationSeconds = Number(
    selectedRun?.duration_seconds
      ?? selectedSummary?.duration_seconds
      ?? 0,
  );

  const {
    data: moves,
    isLoading: loadingMoves,
    refetch: refetchMoves,
  } = useQuery({
    queryKey: ['scan-moves-tools', selectedRun?.scan_id],
    queryFn: async () => {
      if (!selectedRun?.scan_id) return [];
      return api.getScanMoves(selectedRun.scan_id);
    },
    enabled: Boolean(selectedRun?.scan_id),
    refetchInterval: 5000,
  });

  const dedupeMoves = useMemo(
    () => (moves || []).filter((m) => (m.move_reason || 'dedupe') === 'dedupe' && !m.restored),
    [moves],
  );
  const incompleteMoves = useMemo(
    () => (moves || []).filter((m) => (m.move_reason || '') === 'incomplete' && !m.restored),
    [moves],
  );

  const restoreByReason = async (reason: 'dedupe' | 'incomplete') => {
    if (!selectedRun?.scan_id) return;
    const selected = (moves || [])
      .filter((m) => (m.move_reason || 'dedupe') === reason && !m.restored)
      .map((m) => m.move_id);
    if (selected.length === 0) {
      toast.info(`No ${reason} moves to restore`);
      return;
    }
    setRestoring(reason);
    try {
      const result = await api.restoreMoves(selectedRun.scan_id, selected, false);
      toast.success(`${result.restored} move(s) restored`);
      await Promise.all([refetchMoves(), refetchHistory()]);
    } catch (error: unknown) {
      toast.error(error instanceof Error ? error.message : 'Restore failed');
    } finally {
      setRestoring(null);
    }
  };

  return (
    <div className="container mx-auto p-6 max-w-6xl space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold">Tools</h1>
            <p className="text-muted-foreground mt-1">
              Live view of fixed, deduped, and moved incomplete albums with restore actions.
            </p>
          </div>
          <Button
            variant="outline"
            className="gap-2"
            onClick={() => {
              refetchHistory();
              refetchMoves();
            }}
          >
            <RefreshCw className="w-4 h-4" />
            Refresh
          </Button>
        </div>
        <Card>
          <CardContent className="py-4 flex flex-wrap items-center gap-2">
            <Button
              size="sm"
              className="gap-1.5 bg-amber-500/90 hover:bg-amber-500 text-black border border-amber-300/30"
              onClick={() => navigate('/broken-albums')}
            >
              Review incompletes
            </Button>
            <Button
              size="sm"
              className="gap-1.5 bg-amber-500/90 hover:bg-amber-500 text-black border border-amber-300/30"
              onClick={() => navigate('/tools/duplicates')}
            >
              Fine-check duplicates
            </Button>
            <Badge variant="outline">Quick actions</Badge>
          </CardContent>
        </Card>

        {loadingHistory ? (
          <div className="flex items-center justify-center py-16">
            <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
          </div>
        ) : !selectedRun ? (
          <Card>
            <CardContent className="py-10 text-sm text-muted-foreground">
              No completed scan yet.
            </CardContent>
          </Card>
        ) : (
          <>
            <Card>
              <CardHeader>
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <CardTitle>Run</CardTitle>
                    <CardDescription>
                      Scan #{selectedRun.scan_id} • started {fmtDate(selectedRun.start_time)} • ended {fmtDate(selectedRun.end_time)}
                    </CardDescription>
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    <Button
                      type="button"
                      variant="outline"
                      size="icon"
                      onClick={() => {
                        if (selectedIndex > 0) setSelectedScanId(completedRuns[selectedIndex - 1].scan_id);
                      }}
                      disabled={selectedIndex <= 0}
                      aria-label="Newer run"
                    >
                      <ChevronLeft className="w-4 h-4" />
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      size="icon"
                      onClick={() => {
                        if (selectedIndex >= 0 && selectedIndex < completedRuns.length - 1) setSelectedScanId(completedRuns[selectedIndex + 1].scan_id);
                      }}
                      disabled={selectedIndex < 0 || selectedIndex >= completedRuns.length - 1}
                      aria-label="Older run"
                    >
                      <ChevronRight className="w-4 h-4" />
                    </Button>
                    <Badge variant="outline">
                      {selectedIndex >= 0 ? `${selectedIndex + 1}/${completedRuns.length}` : `0/${completedRuns.length}`}
                    </Badge>
                  </div>
                </div>
              </CardHeader>
              <CardContent className="flex flex-wrap gap-2">
                <Badge variant="outline">
                  {selectedAlbumsDone.toLocaleString()} / {selectedAlbumsTotal.toLocaleString()} albums processed
                </Badge>
                <Badge variant="outline">{selectedRun.artists_processed}/{selectedRun.artists_total} artists processed</Badge>
                <Badge variant="outline">{selectedRun.duplicates_found} duplicate group(s)</Badge>
                <Badge variant="outline">{selectedRun.albums_moved} moved</Badge>
                <Badge variant="outline">runtime {fmtDurationSeconds(selectedDurationSeconds)}</Badge>
              </CardContent>
            </Card>

            <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Tags className="w-4 h-4" />
                    Fixed metadata
                  </CardTitle>
                  <CardDescription>Albums touched by PMDA metadata pipeline.</CardDescription>
                </CardHeader>
                <CardContent className="space-y-2 text-sm">
                  <div>Processed: <span className="font-medium">{selectedSummary?.pmda_albums_processed ?? 0}</span></div>
                  <div>Complete: <span className="font-medium">{selectedSummary?.pmda_albums_complete ?? 0}</span></div>
                  <div>With cover: <span className="font-medium">{selectedSummary?.pmda_albums_with_cover ?? 0}</span></div>
                  <div>With artist image: <span className="font-medium">{selectedSummary?.pmda_albums_with_artist_image ?? 0}</span></div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <AlertTriangle className="w-4 h-4" />
                    Incomplete handling
                  </CardTitle>
                  <CardDescription>Albums moved to incomplete quarantine by pipeline.</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="text-sm">
                    Moved this run: <span className="font-medium">{selectedSummary?.incomplete_moved_this_scan ?? 0}</span>
                    <span className="text-muted-foreground"> ({selectedSummary?.incomplete_moved_mb_this_scan ?? 0} MB)</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Button
                      size="sm"
                      className="gap-1.5 bg-amber-500/90 hover:bg-amber-500 text-black border border-amber-300/30"
                      onClick={() => navigate('/broken-albums')}
                    >
                      Review incompletes
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-1.5"
                      onClick={() => restoreByReason('incomplete')}
                      disabled={restoring !== null || incompleteMoves.length === 0}
                    >
                      {restoring === 'incomplete' ? <Loader2 className="w-4 h-4 animate-spin" /> : <Undo2 className="w-4 h-4" />}
                      Restore incomplete
                    </Button>
                    <Badge variant="outline">{incompleteMoves.length} queued move(s)</Badge>
                  </div>
                </CardContent>
              </Card>
            </div>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Trash2 className="w-4 h-4" />
                  Dedupe moves
                </CardTitle>
                <CardDescription>Albums moved by dedupe (selected run).</CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex items-center gap-2">
                  <Button
                    size="sm"
                    className="gap-1.5 bg-amber-500/90 hover:bg-amber-500 text-black border border-amber-300/30"
                    onClick={() => navigate('/tools/duplicates')}
                  >
                    Fine-check duplicates
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="gap-1.5"
                    onClick={() => restoreByReason('dedupe')}
                    disabled={restoring !== null || dedupeMoves.length === 0}
                  >
                    {restoring === 'dedupe' ? <Loader2 className="w-4 h-4 animate-spin" /> : <Undo2 className="w-4 h-4" />}
                    Restore dedupe moves
                  </Button>
                  <Badge variant="outline">{dedupeMoves.length} queued move(s)</Badge>
                </div>

                {loadingMoves ? (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Loading moves…
                  </div>
                ) : dedupeMoves.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No dedupe moves in latest run.</p>
                ) : (
                  <div className="max-h-72 overflow-auto rounded-md border">
                    <table className="w-full text-sm">
                      <thead className="bg-muted/50 sticky top-0">
                        <tr>
                          <th className="text-left px-3 py-2 font-medium">Artist</th>
                          <th className="text-left px-3 py-2 font-medium">Album</th>
                          <th className="text-left px-3 py-2 font-medium">Moved to</th>
                        </tr>
                      </thead>
                      <tbody>
                        {dedupeMoves.slice(0, 200).map((m) => (
                          <tr key={m.move_id} className="border-t border-border">
                            <td className="px-3 py-2">{m.artist}</td>
                            <td className="px-3 py-2">{m.album_title || `#${m.album_id}`}</td>
                            <td className="px-3 py-2 truncate max-w-[420px]" title={m.moved_to_path}>{m.moved_to_path}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </CardContent>
            </Card>
          </>
        )}
    </div>
  );
}
