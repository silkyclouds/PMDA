import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { AlertTriangle, Loader2, RefreshCw, Tags, Trash2, Undo2 } from 'lucide-react';
import { Header } from '@/components/Header';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { toast } from 'sonner';
import * as api from '@/lib/api';

function fmtDate(ts?: number): string {
  if (!ts) return 'N/A';
  return new Date(ts * 1000).toLocaleString();
}

export default function Tools() {
  const [restoring, setRestoring] = useState<'dedupe' | 'incomplete' | null>(null);

  const { data: history, isLoading: loadingHistory, refetch: refetchHistory } = useQuery({
    queryKey: ['scan-history-tools'],
    queryFn: api.getScanHistory,
    refetchInterval: 5000,
  });

  const latestCompleted = useMemo(() => {
    const rows = Array.isArray(history) ? history : [];
    return rows.find((r) => r.status === 'completed') ?? null;
  }, [history]);

  const latestSummary = latestCompleted?.summary_json ?? null;

  const {
    data: moves,
    isLoading: loadingMoves,
    refetch: refetchMoves,
  } = useQuery({
    queryKey: ['scan-moves-tools', latestCompleted?.scan_id],
    queryFn: async () => {
      if (!latestCompleted?.scan_id) return [];
      return api.getScanMoves(latestCompleted.scan_id);
    },
    enabled: Boolean(latestCompleted?.scan_id),
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
    if (!latestCompleted?.scan_id) return;
    const selected = (moves || [])
      .filter((m) => (m.move_reason || 'dedupe') === reason && !m.restored)
      .map((m) => m.move_id);
    if (selected.length === 0) {
      toast.info(`No ${reason} moves to restore`);
      return;
    }
    setRestoring(reason);
    try {
      const result = await api.restoreMoves(latestCompleted.scan_id, selected, false);
      toast.success(`${result.restored} move(s) restored`);
      await Promise.all([refetchMoves(), refetchHistory()]);
    } catch (error: unknown) {
      toast.error(error instanceof Error ? error.message : 'Restore failed');
    } finally {
      setRestoring(null);
    }
  };

  return (
    <>
      <Header />
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

        {loadingHistory ? (
          <div className="flex items-center justify-center py-16">
            <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
          </div>
        ) : !latestCompleted ? (
          <Card>
            <CardContent className="py-10 text-sm text-muted-foreground">
              No completed scan yet.
            </CardContent>
          </Card>
        ) : (
          <>
            <Card>
              <CardHeader>
                <CardTitle>Latest completed run</CardTitle>
                <CardDescription>
                  Scan #{latestCompleted.scan_id} • started {fmtDate(latestCompleted.start_time)} • ended {fmtDate(latestCompleted.end_time)}
                </CardDescription>
              </CardHeader>
              <CardContent className="flex flex-wrap gap-2">
                <Badge variant="outline">{latestCompleted.albums_scanned} albums scanned</Badge>
                <Badge variant="outline">{latestCompleted.artists_processed}/{latestCompleted.artists_total} artists processed</Badge>
                <Badge variant="outline">{latestCompleted.duplicates_found} duplicate group(s)</Badge>
                <Badge variant="outline">{latestCompleted.albums_moved} moved</Badge>
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
                  <div>Processed: <span className="font-medium">{latestSummary?.pmda_albums_processed ?? 0}</span></div>
                  <div>Complete: <span className="font-medium">{latestSummary?.pmda_albums_complete ?? 0}</span></div>
                  <div>With cover: <span className="font-medium">{latestSummary?.pmda_albums_with_cover ?? 0}</span></div>
                  <div>With artist image: <span className="font-medium">{latestSummary?.pmda_albums_with_artist_image ?? 0}</span></div>
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
                    Moved this run: <span className="font-medium">{latestSummary?.incomplete_moved_this_scan ?? 0}</span>
                    <span className="text-muted-foreground"> ({latestSummary?.incomplete_moved_mb_this_scan ?? 0} MB)</span>
                  </div>
                  <div className="flex items-center gap-2">
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
                <CardDescription>Albums moved by dedupe (latest run).</CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex items-center gap-2">
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
    </>
  );
}
