import { useCallback, useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { AlertTriangle, ChevronLeft, ChevronRight, Database, HardDriveDownload, Loader2, RefreshCw, ServerCog, Tags, Trash2, Undo2 } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { AuthenticatedImage } from '@/components/library/AuthenticatedImage';
import { useAuth } from '@/contexts/AuthContext';
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

function fmtBytes(value?: number): string {
  const bytes = Math.max(0, Number(value || 0));
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = bytes;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size >= 10 || unit === 0 ? size.toFixed(0) : size.toFixed(1)} ${units[unit]}`;
}

export default function Tools() {
  const navigate = useNavigate();
  const { isAdmin } = useAuth();
  const [restoring, setRestoring] = useState<'dedupe' | 'incomplete' | null>(null);
  const [selectedScanId, setSelectedScanId] = useState<number | null>(null);
  const [movesFilter, setMovesFilter] = useState<'all' | 'active' | 'restored'>('all');
  const [trashActionBusy, setTrashActionBusy] = useState<string | null>(null);
  const [opsBackupBusy, setOpsBackupBusy] = useState(false);

  const { data: history, isLoading: loadingHistory, refetch: refetchHistory } = useQuery({
    queryKey: ['scan-history-tools'],
    queryFn: api.getScanHistory,
    refetchInterval: 5000,
  });
  const { data: reviewStats = null, refetch: refetchReviewStats } = useQuery({
    queryKey: ['review-stats-tools'],
    queryFn: () => api.getReviewStats().catch(() => null),
    refetchInterval: 15000,
    enabled: isAdmin,
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
      return api.getScanMoves(selectedRun.scan_id, { status: 'all' });
    },
    enabled: Boolean(selectedRun?.scan_id),
    refetchInterval: 5000,
  });
  const { data: movesSummary } = useQuery({
    queryKey: ['scan-moves-tools-summary', selectedRun?.scan_id],
    queryFn: async () => {
      if (!selectedRun?.scan_id) return null;
      return api.getScanMovesSummary(selectedRun.scan_id);
    },
    enabled: Boolean(selectedRun?.scan_id),
    refetchInterval: 5000,
  });
  const {
    data: opsSnapshot,
    isLoading: loadingOpsSnapshot,
    refetch: refetchOpsSnapshot,
  } = useQuery({
    queryKey: ['tools-admin-ops-snapshot'],
    queryFn: api.getAdminOpsSnapshot,
    enabled: isAdmin,
    refetchInterval: 20000,
  });

  const {
    data: trashReleaseSnapshot,
    isLoading: loadingTrashReleases,
    refetch: refetchTrashReleases,
  } = useQuery({
    queryKey: ['tools-trash-releases'],
    queryFn: () => api.getTrashReleaseCandidates(18, 4),
    enabled: isAdmin,
    refetchInterval: 15000,
  });

  const dedupeMovesAll = useMemo(
    () => (moves || []).filter((m) => (m.move_reason || 'dedupe') === 'dedupe'),
    [moves],
  );
  const incompleteMovesAll = useMemo(
    () => (moves || []).filter((m) => (m.move_reason || '') === 'incomplete'),
    [moves],
  );
  const filterMoves = useCallback(
    (items: api.ScanMove[]) => {
      if (movesFilter === 'active') return items.filter((m) => !m.restored);
      if (movesFilter === 'restored') return items.filter((m) => Boolean(m.restored));
      return items;
    },
    [movesFilter],
  );
  const dedupeMoves = useMemo(() => filterMoves(dedupeMovesAll), [dedupeMovesAll, filterMoves]);
  const incompleteMoves = useMemo(() => filterMoves(incompleteMovesAll), [incompleteMovesAll, filterMoves]);
  const dedupePendingCount = useMemo(() => dedupeMovesAll.filter((m) => !m.restored).length, [dedupeMovesAll]);
  const incompletePendingCount = useMemo(() => incompleteMovesAll.filter((m) => !m.restored).length, [incompleteMovesAll]);
  const hasIncompleteReview = useMemo(() => {
    const globalIncomplete = Number(reviewStats?.incompletes?.albums ?? 0);
    const broken = Number(
      selectedRun?.broken_albums_count
      ?? selectedSummary?.broken_albums_count
      ?? 0,
    );
    return globalIncomplete > 0 || broken > 0 || incompleteMovesAll.length > 0;
  }, [incompleteMovesAll.length, reviewStats?.incompletes?.albums, selectedRun?.broken_albums_count, selectedSummary?.broken_albums_count]);
  const hasDuplicateReview = useMemo(() => {
    const globalDupes = Number(reviewStats?.duplicates?.groups ?? 0);
    const dupes = Number(
      selectedRun?.duplicates_found
      ?? selectedRun?.duplicate_groups_count
      ?? selectedRun?.total_duplicates_count
      ?? selectedSummary?.duplicate_groups_count
      ?? selectedSummary?.total_duplicates_count
      ?? 0,
    );
    return globalDupes > 0 || dupes > 0 || dedupeMovesAll.length > 0;
  }, [
    dedupeMovesAll.length,
    reviewStats?.duplicates?.groups,
    selectedRun?.duplicate_groups_count,
    selectedRun?.duplicates_found,
    selectedRun?.total_duplicates_count,
    selectedSummary?.duplicate_groups_count,
    selectedSummary?.total_duplicates_count,
  ]);

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

  const applyTrashAction = async (
    candidate: api.TrashReleaseCandidate,
    action: 'move_to_dupes' | 'delete_from_disk',
  ) => {
    if (!isAdmin) return;
    const actionLabel = action === 'move_to_dupes' ? 'move this album to the dupes quarantine' : 'delete this album folder from disk permanently';
    const confirmed = window.confirm(
      `Do you want to ${actionLabel}?\n\n${candidate.artist} — ${candidate.album_title}\n${candidate.folder_path}`,
    );
    if (!confirmed) return;
    const actionKey = `${action}:${candidate.album_id}`;
    setTrashActionBusy(actionKey);
    try {
      const result = await api.applyTrashReleaseAction(candidate.album_id, action);
      toast.success(result.message || 'Manual curation action completed');
      await refetchTrashReleases();
    } catch (error: unknown) {
      toast.error(error instanceof Error ? error.message : 'Manual curation action failed');
    } finally {
      setTrashActionBusy(null);
    }
  };

  const createOpsBackup = async () => {
    if (!isAdmin) return;
    setOpsBackupBusy(true);
    try {
      const result = await api.createAdminOpsBackup(true);
      if (result.status === 'ok') toast.success(result.message || 'PMDA backup created');
      else toast.warning(result.message || 'PMDA backup created with warnings');
      await refetchOpsSnapshot();
    } catch (error: unknown) {
      toast.error(error instanceof Error ? error.message : 'PMDA backup failed');
    } finally {
      setOpsBackupBusy(false);
    }
  };

  return (
    <div className="pmda-page-shell pmda-page-stack">
        <div className="pmda-page-header">
          <div>
            <h1 className="pmda-page-title">Tools</h1>
            <p className="pmda-meta-text mt-1">
              Live view of fixed, deduped, and moved incomplete albums with restore actions.
            </p>
          </div>
          <Button
            variant="outline"
            className="gap-2"
            onClick={() => {
              refetchHistory();
              refetchMoves();
              void refetchReviewStats();
              if (isAdmin) void refetchOpsSnapshot();
              if (isAdmin) void refetchTrashReleases();
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
              variant="default"
              className="gap-1.5"
              onClick={() => navigate('/broken-albums')}
              disabled={!hasIncompleteReview}
            >
              Review incompletes
            </Button>
            <Button
              size="sm"
              variant="outline"
              className="gap-1.5"
              onClick={() => navigate('/tools/duplicates')}
              disabled={!hasDuplicateReview}
            >
              Fine-check duplicates
            </Button>
            <Badge variant="outline">Quick actions</Badge>
          </CardContent>
        </Card>

        {isAdmin ? (
          <Card>
            <CardHeader>
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <ServerCog className="w-4 h-4" />
                    Operations & backups
                  </CardTitle>
                  <CardDescription>
                    Admin-only runtime snapshot and PMDA database backup bundle.
                  </CardDescription>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="outline">Admin only</Badge>
                  {opsSnapshot ? (
                    <Badge variant="outline">
                      {opsSnapshot.pipeline_bootstrap_required ? 'First full scan pending' : 'First full scan complete'}
                    </Badge>
                  ) : null}
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              {loadingOpsSnapshot ? (
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Reading PMDA runtime snapshot…
                </div>
              ) : opsSnapshot ? (
                <>
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="outline">Mode {opsSnapshot.library_mode}</Badge>
                    {opsSnapshot.workflow_mode ? <Badge variant="outline">Workflow {opsSnapshot.workflow_mode}</Badge> : null}
                    <Badge variant="outline">
                      PostgreSQL {String((opsSnapshot.postgres?.available as boolean) ? 'ready' : 'unavailable')}
                    </Badge>
                    <Badge variant="outline">
                      Redis {String((opsSnapshot.redis?.mode as string) || 'unknown')}
                    </Badge>
                    <Badge variant="outline">
                      {opsSnapshot.scan_running ? 'Scan running' : 'Scan idle'}
                    </Badge>
                    <Badge variant="outline">
                      Backups {opsSnapshot.backups.length}
                    </Badge>
                  </div>

                  <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                    {([
                      ['Config', opsSnapshot.storage.config],
                      ['Music', opsSnapshot.storage.music],
                      ['Dupes', opsSnapshot.storage.dupes],
                      ['Postgres data', opsSnapshot.storage.pgdata],
                    ] as Array<[string, api.AdminOpsStorageTarget]>).map(([label, row]) => {
                      return (
                        <div key={label} className="rounded-xl border border-border/70 bg-background/30 p-3">
                          <div className="text-xs uppercase tracking-[0.2em] text-muted-foreground">{label}</div>
                          <div className="mt-2 text-sm font-medium text-foreground">{fmtBytes(row.free_bytes)} free</div>
                          <div className="mt-1 text-xs text-muted-foreground">{fmtBytes(row.used_bytes)} used / {fmtBytes(row.total_bytes)} total</div>
                          <code className="mt-2 block text-[11px] text-muted-foreground break-all">{row.path}</code>
                        </div>
                      );
                    })}
                  </div>

                  <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                    <div className="rounded-xl border border-border/70 bg-background/30 p-3">
                      <div className="flex items-center gap-2 text-xs uppercase tracking-[0.2em] text-muted-foreground">
                        <Database className="w-3.5 h-3.5" />
                        Settings DB
                      </div>
                      <div className="mt-2 text-sm font-medium text-foreground">{fmtBytes(Number(opsSnapshot.sqlite.settings_db?.db_bytes || 0))}</div>
                      <div className="mt-1 text-xs text-muted-foreground">{Number(opsSnapshot.sqlite.settings_db?.rows || 0)} row(s)</div>
                    </div>
                    <div className="rounded-xl border border-border/70 bg-background/30 p-3">
                      <div className="flex items-center gap-2 text-xs uppercase tracking-[0.2em] text-muted-foreground">
                        <Database className="w-3.5 h-3.5" />
                        State DB
                      </div>
                      <div className="mt-2 text-sm font-medium text-foreground">{fmtBytes(Number(opsSnapshot.sqlite.state_db?.db_bytes || 0))}</div>
                      <div className="mt-1 text-xs text-muted-foreground">
                        {Number(opsSnapshot.sqlite.state_db?.files_album_scan_cache_rows || 0)} scan cache row(s)
                      </div>
                    </div>
                    <div className="rounded-xl border border-border/70 bg-background/30 p-3">
                      <div className="flex items-center gap-2 text-xs uppercase tracking-[0.2em] text-muted-foreground">
                        <Database className="w-3.5 h-3.5" />
                        Cache DB
                      </div>
                      <div className="mt-2 text-sm font-medium text-foreground">{fmtBytes(Number(opsSnapshot.sqlite.cache_db?.db_bytes || 0))}</div>
                      <div className="mt-1 text-xs text-muted-foreground">
                        {Number(opsSnapshot.sqlite.cache_db?.musicbrainz_cache_rows || 0)} MB cache row(s)
                      </div>
                    </div>
                    <div className="rounded-xl border border-border/70 bg-background/30 p-3">
                      <div className="flex items-center gap-2 text-xs uppercase tracking-[0.2em] text-muted-foreground">
                        <Database className="w-3.5 h-3.5" />
                        Files PostgreSQL
                      </div>
                      <div className="mt-2 text-sm font-medium text-foreground">{fmtBytes(Number(opsSnapshot.postgres?.db_size_bytes || 0))}</div>
                      <div className="mt-1 text-xs text-muted-foreground">
                        {String((opsSnapshot.postgres?.available as boolean) ? 'ready' : (opsSnapshot.postgres?.reason as string) || 'unavailable')}
                      </div>
                    </div>
                  </div>

                  <div className="flex flex-wrap items-center gap-2">
                    <Button size="sm" className="gap-1.5" onClick={() => void createOpsBackup()} disabled={opsBackupBusy}>
                      {opsBackupBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <HardDriveDownload className="w-4 h-4" />}
                      Create PMDA backup
                    </Button>
                    <Badge variant="outline">{opsSnapshot.auth_bootstrap_required ? 'Admin bootstrap pending' : 'Admin bootstrap complete'}</Badge>
                    <Badge variant="outline">{opsSnapshot.files_roots_configured ? 'Roots configured' : 'Roots not configured yet'}</Badge>
                  </div>

                  <div className="space-y-2">
                    <div className="text-xs uppercase tracking-[0.2em] text-muted-foreground">Recent backups</div>
                    {(opsSnapshot.backups || []).length === 0 ? (
                      <p className="text-sm text-muted-foreground">No PMDA backup created yet.</p>
                    ) : (
                      <div className="space-y-2">
                        {opsSnapshot.backups.slice(0, 4).map((backup) => (
                          <div key={backup.path} className="rounded-xl border border-border/70 bg-background/30 px-3 py-2 flex flex-wrap items-center justify-between gap-2">
                            <div className="min-w-0">
                              <div className="text-sm font-medium text-foreground">{backup.name}</div>
                              <code className="block text-[11px] break-all text-muted-foreground">{backup.path}</code>
                            </div>
                            <div className="flex flex-wrap items-center gap-2">
                              <Badge variant="outline">{backup.status}</Badge>
                              <Badge variant="outline">{fmtBytes(backup.size_bytes)}</Badge>
                              <Badge variant="outline">{backup.pg_dump_included ? (backup.pg_dump_ok ? 'PG dump OK' : 'PG dump partial') : 'SQLite only'}</Badge>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </>
              ) : (
                <p className="text-sm text-muted-foreground">Operations snapshot unavailable right now.</p>
              )}
            </CardContent>
          </Card>
        ) : null}

        {isAdmin ? (
          <Card>
            <CardHeader>
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <Trash2 className="w-4 h-4" />
                    Manual curation
                  </CardTitle>
                  <CardDescription>
                    Admin-only suggestions for low-value compilations, workout mixes, Ibiza-style samplers, and karaoke-style clutter in the visible library.
                  </CardDescription>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="outline">Admin only</Badge>
                  {trashReleaseSnapshot?.available ? (
                    <Badge variant="outline">{trashReleaseSnapshot.total} candidate(s)</Badge>
                  ) : null}
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              {!trashReleaseSnapshot?.available && !loadingTrashReleases ? (
                <p className="text-sm text-muted-foreground">
                  Trash-release suggestions are unavailable right now
                  {trashReleaseSnapshot?.reason ? ` (${trashReleaseSnapshot.reason.replace(/_/g, ' ')})` : ''}.
                </p>
              ) : loadingTrashReleases ? (
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Scoring visible library releases…
                </div>
              ) : (trashReleaseSnapshot?.candidates || []).length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  No obvious trash-release candidates found in the visible library.
                </p>
              ) : (
                <>
                  <div className="flex flex-wrap items-center gap-2">
                    {Object.entries(trashReleaseSnapshot?.summary?.by_category || {}).slice(0, 6).map(([category, count]) => (
                      <Badge key={category} variant="outline">
                        {category.replace(/_/g, ' ')} · {count}
                      </Badge>
                    ))}
                  </div>
                  <div className="space-y-3">
                    {(trashReleaseSnapshot?.candidates || []).map((candidate) => {
                      const moveBusy = trashActionBusy === `move_to_dupes:${candidate.album_id}`;
                      const deleteBusy = trashActionBusy === `delete_from_disk:${candidate.album_id}`;
                      return (
                        <div
                          key={candidate.album_id}
                          className="rounded-xl border border-border/70 bg-background/30 p-4 flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between"
                        >
                          <div className="flex gap-4 min-w-0">
                            <div className="h-20 w-20 shrink-0 overflow-hidden rounded-xl border border-border/60 bg-muted/30">
                              <AuthenticatedImage
                                src={candidate.thumb_url}
                                alt={`${candidate.artist} — ${candidate.album_title}`}
                                className="h-full w-full object-cover"
                                fallback={<div className="h-full w-full" />}
                              />
                            </div>
                            <div className="min-w-0 space-y-2">
                              <div className="flex flex-wrap items-center gap-2">
                                <div className="text-sm font-semibold text-foreground">
                                  {candidate.artist} — {candidate.album_title}
                                </div>
                                <Badge variant="outline">Score {candidate.score}</Badge>
                                <Badge variant="outline">{candidate.category.replace(/_/g, ' ')}</Badge>
                                {candidate.metadata_source ? <Badge variant="outline">{candidate.metadata_source}</Badge> : null}
                                {candidate.year ? <Badge variant="outline">{candidate.year}</Badge> : null}
                                {candidate.track_count > 0 ? <Badge variant="outline">{candidate.track_count} tracks</Badge> : null}
                              </div>
                              <div className="flex flex-wrap items-center gap-2">
                                {(candidate.signals || []).slice(0, 4).map((signal, index) => (
                                  <Badge key={`${candidate.album_id}-signal-${index}`} variant="outline">
                                    {signal.label}
                                    {signal.matched ? ` · ${signal.matched}` : ''}
                                  </Badge>
                                ))}
                              </div>
                              <code className="block rounded bg-background/70 px-2 py-1 text-[11px] break-all text-muted-foreground">
                                {candidate.folder_path}
                              </code>
                            </div>
                          </div>
                          <div className="flex shrink-0 flex-col items-stretch gap-2 xl:min-w-[180px]">
                            <Button
                              size="sm"
                              className="gap-1.5 bg-warning hover:bg-warning/90 text-warning-foreground border border-warning/30"
                              onClick={() => void applyTrashAction(candidate, 'move_to_dupes')}
                              disabled={Boolean(trashActionBusy)}
                            >
                              {moveBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <Trash2 className="w-4 h-4" />}
                              Move to dupes
                            </Button>
                            <Button
                              size="sm"
                              variant="destructive"
                              className="gap-1.5"
                              onClick={() => void applyTrashAction(candidate, 'delete_from_disk')}
                              disabled={Boolean(trashActionBusy)}
                            >
                              {deleteBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <Trash2 className="w-4 h-4" />}
                              Delete from disk
                            </Button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </>
              )}
            </CardContent>
          </Card>
        ) : null}

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
                    <Badge variant="outline">
                      history {movesSummary?.total_moved ?? 0} · pending {movesSummary?.pending ?? 0} · restored {movesSummary?.restored ?? 0}
                    </Badge>
                <Badge variant="outline">runtime {fmtDurationSeconds(selectedDurationSeconds)}</Badge>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="py-3 flex flex-wrap items-center gap-2">
                <span className="text-xs text-muted-foreground">Move history filter:</span>
                <Button size="sm" variant={movesFilter === 'all' ? 'default' : 'outline'} onClick={() => setMovesFilter('all')}>
                  All
                </Button>
                <Button size="sm" variant={movesFilter === 'active' ? 'default' : 'outline'} onClick={() => setMovesFilter('active')}>
                  Pending rollback
                </Button>
                <Button size="sm" variant={movesFilter === 'restored' ? 'default' : 'outline'} onClick={() => setMovesFilter('restored')}>
                  Restored
                </Button>
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
                      className="gap-1.5 bg-warning hover:bg-warning/90 text-warning-foreground border border-warning/30"
                      onClick={() => navigate('/broken-albums')}
                      disabled={!hasIncompleteReview}
                    >
                      Review incompletes
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-1.5"
                      onClick={() => restoreByReason('incomplete')}
                      disabled={restoring !== null || incompletePendingCount === 0}
                    >
                      {restoring === 'incomplete' ? <Loader2 className="w-4 h-4 animate-spin" /> : <Undo2 className="w-4 h-4" />}
                      Restore incomplete
                    </Button>
                    <Badge variant="outline">{incompletePendingCount} queued move(s)</Badge>
                    <Badge variant="outline">{incompleteMovesAll.length} total move(s)</Badge>
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
                    className="gap-1.5 bg-warning hover:bg-warning/90 text-warning-foreground border border-warning/30"
                    onClick={() => navigate('/tools/duplicates')}
                    disabled={!hasDuplicateReview}
                  >
                    Fine-check duplicates
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="gap-1.5"
                    onClick={() => restoreByReason('dedupe')}
                    disabled={restoring !== null || dedupePendingCount === 0}
                  >
                    {restoring === 'dedupe' ? <Loader2 className="w-4 h-4 animate-spin" /> : <Undo2 className="w-4 h-4" />}
                    Restore dedupe moves
                  </Button>
                  <Badge variant="outline">{dedupePendingCount} queued move(s)</Badge>
                  <Badge variant="outline">{dedupeMovesAll.length} total move(s)</Badge>
                </div>

                {loadingMoves ? (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Loading moves…
                  </div>
                ) : dedupeMoves.length === 0 ? (
                  <p className="text-sm text-muted-foreground">
                    No dedupe moves for this filter in selected run.
                  </p>
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
