import { useEffect, useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { Package, CheckCircle2, Play, ArrowRight, Loader2, Wrench, AlertTriangle, Terminal, ChevronDown, ChevronUp, Download, Clock3, Database, Bot } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { Progress } from '@/components/ui/progress';
import { Button } from '@/components/ui/button';
import { BackendLogPanel } from '@/components/BackendLogPanel';
import { StatusDot } from '@/components/scan/StatusDot';
import { cn } from '@/lib/utils';
import { useScanProgressShared } from '@/hooks/useScanProgressShared';
import { useDuplicates, useScanControls } from '@/hooks/usePMDA';
import { getLibraryFilesIndexStatus, getManagedRuntimeLogs, getManagedRuntimeStatus, getReviewStats, getScanLogsTail, type LibraryFilesIndexStatus, type LogTailEntry, type ManagedRuntimeBundleStatus, type ManagedRuntimeBundleType, type ManagedRuntimeStatusResponse } from '@/lib/api';

function formatDurationShort(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return '—';
  const total = Math.floor(seconds);
  const days = Math.floor(total / 86400);
  const hours = Math.floor((total % 86400) / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

function formatCompactCount(value: unknown): string {
  const count = Number(value || 0);
  if (!Number.isFinite(count) || count <= 0) return '0';
  return count.toLocaleString();
}

const MANAGED_RUNTIME_ACTIVE_STATES = new Set([
  'preflight',
  'pulling',
  'creating',
  'importing',
  'waiting_health',
  'updating',
  'repairing',
  'starting',
  'stopping',
  'restarting',
  'resetting',
]);

function managedRuntimeLabel(bundleType: ManagedRuntimeBundleType): string {
  return bundleType === 'musicbrainz_local' ? 'MusicBrainz mirror' : 'Ollama runtime';
}

function managedRuntimeActionLabel(action: string | null | undefined): string {
  switch (String(action || '').trim()) {
    case 'bootstrap':
      return 'Provisioning';
    case 'repair-search-index':
      return 'Rebuilding search index';
    case 'retry-update':
      return 'Updating';
    case 'pull-model':
      return 'Downloading model';
    case 'restart':
      return 'Restarting';
    case 'reset':
      return 'Resetting';
    case 'refresh-health':
      return 'Checking health';
    default:
      return 'Runtime work';
  }
}

function managedRuntimeAction(bundle: ManagedRuntimeBundleStatus | null | undefined): string {
  return String(bundle?.latest_action?.action || bundle?.phase || '').trim().toLowerCase();
}

function managedRuntimeAllowsDownloadProgress(bundle: ManagedRuntimeBundleStatus | null | undefined): boolean {
  if (!bundle) return false;
  const action = managedRuntimeAction(bundle);
  const phase = String(bundle.phase || '').trim().toLowerCase();
  const phaseMessage = String(bundle.phase_message || '').trim().toLowerCase();
  if (action === 'repair-search-index') return false;
  return (
    action === 'bootstrap' ||
    action === 'pull-model' ||
    phase.includes('download') ||
    phase.includes('pull') ||
    phase.includes('import') ||
    phaseMessage.includes('download') ||
    phaseMessage.includes('pull') ||
    phaseMessage.includes('import')
  );
}

function managedRuntimeProgress(bundle: ManagedRuntimeBundleStatus | null | undefined): number | null {
  if (!bundle) return null;
  if (managedRuntimeAction(bundle) === 'repair-search-index') return null;
  const meta = (bundle.meta || {}) as Record<string, unknown>;
  const progress = Number(meta.progress);
  if (Number.isFinite(progress) && progress > 0) {
    return Math.max(0, Math.min(100, progress));
  }
  if (!managedRuntimeAllowsDownloadProgress(bundle)) return null;
  const downloadProgress = Number(meta.download_progress);
  if (Number.isFinite(downloadProgress) && downloadProgress > 0) {
    return Math.max(0, Math.min(100, downloadProgress));
  }
  return null;
}

function managedRuntimeDetail(bundle: ManagedRuntimeBundleStatus | null | undefined): string {
  if (!bundle) return '';
  const meta = (bundle.meta || {}) as Record<string, unknown>;
  const lastOutput = String(meta.last_output || '').trim();
  const staleDownloadArtifact =
    managedRuntimeAction(bundle) === 'repair-search-index' &&
    /Cannot write to .*solr-backups\/LATEST/i.test(lastOutput);
  if (lastOutput && !staleDownloadArtifact) return lastOutput;
  return String(meta.current_model || bundle.last_error || '').trim();
}

function managedRuntimeIsActive(bundle: ManagedRuntimeBundleStatus | null | undefined): boolean {
  if (!bundle) return false;
  const state = String(bundle.state || '').trim().toLowerCase();
  const phase = String(bundle.phase || '').trim().toLowerCase();
  const actionStatus = String(bundle.latest_action?.status || '').trim().toLowerCase();
  return actionStatus === 'running' || MANAGED_RUNTIME_ACTIVE_STATES.has(state) || MANAGED_RUNTIME_ACTIVE_STATES.has(phase);
}

function activeManagedRuntimeItems(status: ManagedRuntimeStatusResponse | null | undefined): ManagedRuntimeBundleStatus[] {
  const bundles = status?.bundles;
  if (!bundles) return [];
  return (['musicbrainz_local', 'ollama_local'] as ManagedRuntimeBundleType[])
    .map((bundleType) => bundles[bundleType])
    .filter((bundle): bundle is ManagedRuntimeBundleStatus => managedRuntimeIsActive(bundle));
}

export function GlobalStatusBar() {
  const navigate = useNavigate();
  const scanControls = useScanControls();
  const { 
    progress,
    isScanning, 
    isDeduping,
    isIdle, 
    stageProgressPercent,
    stageProgressDone,
    stageProgressTotal,
    stageProgressUnit,
    overallProgressPercent,
    pipelineOverallProgressPercent,
    pipelineOverallDoneSteps,
    pipelineOverallTotalSteps,
    status,
    phase,
    phaseLabel,
    stageStatusLabel,
    preScanActive,
    preScanIndeterminate,
    preScanStageLabel,
    preScanStatusLabel,
    preScanCountersLabel,
    runScopePreparing,
    runScopeIndeterminate,
    runScopeStage,
    runScopeStatusLabel,
    runScopeCountersLabel,
    pipelineSteps,
    currentPipelineStepIndex,
    pipelineStepsTotal,
    currentPipelineStepLabel,
    presentation,
    currentArtist,
    lastScanSummary,
    dedupePercent,
    dedupeProgressValue,
    dedupeTotal,
    currentDedupeGroup,
  } = useScanProgressShared({ showCompletionToast: true });
  const { data: managedRuntimeStatus = null } = useQuery<ManagedRuntimeStatusResponse | null>({
    queryKey: ['global-managed-runtime-status'],
    queryFn: () => getManagedRuntimeStatus({ skipCandidates: true }).catch(() => null),
    refetchInterval: 5000,
  });
  const { data: filesIndexStatus = null } = useQuery<LibraryFilesIndexStatus | null>({
    queryKey: ['library-files-index-status'],
    queryFn: () => getLibraryFilesIndexStatus().catch(() => null),
    refetchInterval: (query) => {
      const status = query.state.data;
      return status?.running ? 2500 : 15000;
    },
    refetchIntervalInBackground: true,
    staleTime: 0,
  });
  
  const { data: duplicates = [] } = useDuplicates({ refetchInterval: 30000 });
  const { data: reviewStats = null } = useQuery({
    queryKey: ['global-review-stats'],
    queryFn: () => getReviewStats().catch(() => null),
    refetchInterval: 30000,
  });
  const duplicateCount = duplicates.length;
  const [showLogs, setShowLogs] = useState(false);
  const [logLines, setLogLines] = useState<string[]>([]);
  const [logEntries, setLogEntries] = useState<LogTailEntry[]>([]);
  const runtimeItems = activeManagedRuntimeItems(managedRuntimeStatus);
  const primaryRuntime = runtimeItems[0] || null;
  const runtimeActive = runtimeItems.length > 0;
  const runtimeProgressValue = managedRuntimeProgress(primaryRuntime);
  const runtimeProgressKnown = runtimeProgressValue != null;
  const runtimeTitle = primaryRuntime ? managedRuntimeLabel(primaryRuntime.bundle_type) : '';
  const runtimeAction = managedRuntimeActionLabel(primaryRuntime?.latest_action?.action || primaryRuntime?.phase);
  const runtimeMessage = String(primaryRuntime?.phase_message || primaryRuntime?.latest_action?.action || '').trim();
  const runtimeDetail = managedRuntimeDetail(primaryRuntime);
  const libraryIndexActive = Boolean(filesIndexStatus?.running);
  const libraryIndexPhase = String(filesIndexStatus?.phase || 'indexing').trim().replace(/_/g, ' ');
  const libraryIndexMessage = String(filesIndexStatus?.phase_message || '').trim();
  const libraryIndexProgressRaw = Number(filesIndexStatus?.phase_progress);
  const libraryIndexProgressKnown = Number.isFinite(libraryIndexProgressRaw) && libraryIndexProgressRaw >= 0;
  const libraryIndexProgressValue = libraryIndexProgressKnown
    ? Math.max(0, Math.min(100, libraryIndexProgressRaw))
    : 0;
  const libraryIndexDone = Math.max(
    0,
    Number(filesIndexStatus?.folders_processed || filesIndexStatus?.phase_item_done || filesIndexStatus?.entries_scanned || 0),
  );
  const libraryIndexTotal = Math.max(
    0,
    Number(filesIndexStatus?.total_folders || filesIndexStatus?.phase_item_total || 0),
  );
  const libraryIndexCounter = libraryIndexTotal > 0
    ? `${formatCompactCount(libraryIndexDone)} / ${formatCompactCount(libraryIndexTotal)} folders`
    : Number(filesIndexStatus?.discovered_audio_files || 0) > 0
      ? `${formatCompactCount(filesIndexStatus?.discovered_audio_files)} audio files`
      : 'working';
  const libraryIndexEta = formatDurationShort(filesIndexStatus?.phase_eta_seconds ?? null);
  
  // Count unique artists
  const artistSet = new Set<string>();
  for (const dupe of duplicates) {
    if (dupe.artist_key) artistSet.add(dupe.artist_key);
  }
  const artistCount = isScanning
    ? Math.max(0, Number(progress?.artists_processed ?? 0))
    : (lastScanSummary?.artists_total ?? artistSet.size);
  const albumCount = isScanning
    ? Math.max(0, Number(progress?.scan_processed_albums_count ?? progress?.progress ?? 0))
    : (lastScanSummary?.albums_scanned ?? duplicates.length);
  
  // Global open-review counts; scan summary is only a fallback for quality totals.
  const toBeFixedCount = (lastScanSummary?.albums_without_complete_tags ?? 0) + 
                         (lastScanSummary?.albums_without_album_image ?? 0) + 
                         (lastScanSummary?.albums_without_artist_image ?? 0);
  const incompleteCount = Number(reviewStats?.incompletes?.albums ?? lastScanSummary?.broken_albums_count ?? 0);
  const stageTransitioning =
    isScanning &&
    !preScanActive &&
    !runScopePreparing &&
    Number(stageProgressTotal || 0) > 0 &&
    Number(stageProgressDone || 0) >= Number(stageProgressTotal || 0) &&
    !['finalizing', 'background_enrichment'].includes(String(phase || ''));
  const stageIndeterminate =
    isScanning &&
    (
      preScanIndeterminate ||
      runScopeIndeterminate ||
      stageTransitioning ||
      (
        !preScanActive &&
        !runScopePreparing &&
        (
          (phase === 'finalizing' || phase === 'background_enrichment') &&
          Number(stageProgressTotal || 0) <= 0
        )
      )
    );
  const visibleStageProgressPercent =
    !isScanning || stageIndeterminate
      ? 0
      : Number(stageProgressTotal || 0) > 0 && Number(stageProgressDone || 0) > 0
        ? Math.max(1, Math.min(100, Number(stageProgressPercent || 0)))
        : Math.max(0, Math.min(100, Number(stageProgressPercent || 0)));
  const stagePercentLabel = (() => {
    if (stageIndeterminate) {
      return preScanIndeterminate
        ? 'estimating'
        : runScopeIndeterminate
          ? 'scoping'
          : stageTransitioning
            ? 'hand-off'
            : 'finishing';
    }
    const pct = Math.max(0, Math.min(100, Number(stageProgressPercent || 0)));
    if (pct > 0 && pct < 1) return 'stage 0.1%+';
    if (pct < 10) return `stage ${pct.toFixed(1)}%`;
    return `stage ${Math.round(pct)}%`;
  })();
  const overallPercentLabel = (() => {
    const pct = Math.max(0, Math.min(100, Number(pipelineOverallProgressPercent || overallProgressPercent || 0)));
    if (pct > 0 && pct < 0.1) return 'run 0.1%+';
    if (pct > 0 && pct < 1) return `run ${pct.toFixed(1)}%`;
    if (pct < 10) return `run ${pct.toFixed(1)}%`;
    return `run ${Math.round(pct)}%`;
  })();
  const stageSummaryInline = stageIndeterminate
    ? (preScanIndeterminate
      ? `estimating · ${preScanCountersLabel}`
      : runScopeIndeterminate
        ? `estimating scope · ${runScopeCountersLabel}`
        : stageTransitioning
          ? `hand-off · next ${currentPipelineStepIndex < pipelineStepsTotal ? currentPipelineStepIndex + 1 : currentPipelineStepIndex}/${pipelineStepsTotal}`
          : 'finishing')
    : Number(stageProgressTotal || 0) > 0
      ? `${Number(stageProgressDone || 0).toLocaleString()}/${Number(stageProgressTotal || 0).toLocaleString()} ${stageProgressUnit || 'steps'}`
      : '';
  const pipelinePercentCompact = Math.max(0, Math.min(100, Number(presentation?.pipelineOverallPercent || pipelineOverallProgressPercent || 0)));
  const pipelineSummaryText = presentation?.pipelineProgressLabel || `${pipelineOverallDoneSteps.toFixed(1)}/${pipelineOverallTotalSteps || 0} steps`;
  const currentStageText = presentation?.currentStageProgressLabel || stageSummaryInline || stageStatusLabel || currentArtist || '';
  const headerStageHeroText = (() => {
    if (currentStageText) return `Current stage: ${currentStageText}`;
    return presentation?.stageHeroLabel || '';
  })();

  const handleStartScan = () => {
    scanControls.start();
  };

  useEffect(() => {
    if (!showLogs) return;
    let cancelled = false;
    const tick = async () => {
      try {
        if (runtimeActive && primaryRuntime && !isScanning) {
          const data = await getManagedRuntimeLogs({ bundleType: primaryRuntime.bundle_type, limit: 120 });
          const rows = Array.isArray(data?.logs) ? [...data.logs].reverse() : [];
          const entries: LogTailEntry[] = rows.map((row, index) => {
            const timestamp = row.created_at ? new Date(row.created_at * 1000).toLocaleTimeString() : '';
            const service = row.service_name || row.bundle_type || 'runtime';
            const level = String(row.level || 'info').toUpperCase();
            const message = String(row.message || '');
            return {
              raw: `${timestamp} [${level}] [${service}] ${message}`,
              timestamp,
              level,
              thread: service,
              thread_key: service,
              thread_slot: index % 12,
              message,
              kind: level === 'ERROR' ? 'error' : level === 'WARNING' ? 'warning' : 'info',
              marker: '',
            };
          });
          if (!cancelled) {
            setLogEntries(entries.slice(-8));
            setLogLines(entries.slice(-8).map((entry) => entry.raw));
          }
          return;
        }
        const data = await getScanLogsTail(120, { scanMode: isScanning });
        if (!cancelled) {
          const lines = Array.isArray(data?.lines) ? data.lines : [];
          const entries = Array.isArray(data?.entries) ? data.entries : [];
          // Show newest first for quick readability in the drawer.
          setLogLines(lines.slice(-8).reverse());
          setLogEntries(entries.slice(-8).reverse());
        }
      } catch {
        // ignore
      }
    };
    tick();
    const id = setInterval(tick, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [showLogs, isScanning, runtimeActive, primaryRuntime?.bundle_type]);

  if (!isScanning && !isDeduping && !runtimeActive && !libraryIndexActive) {
    return null;
  }

  return (
    <div className="sticky top-[61px] z-40 border-b border-border/80 bg-gradient-to-r from-card via-card to-card/95 backdrop-blur supports-[backdrop-filter]:bg-card/90">
      <div className="container">
        <div className="flex items-center justify-between py-2.5 gap-4">
          {/* Left: Status with quick action */}
          <div className="flex items-center gap-3 min-w-0">
            {isDeduping ? (
              // Unduping state - show dedupe progress
              <>
                <div className="flex items-center gap-2">
                  <Loader2 className="w-4 h-4 text-primary animate-spin" />
                  <span className="text-sm font-medium text-foreground">Unduping</span>
                </div>
                <div className="hidden sm:flex items-center gap-2 min-w-0">
                  <Progress value={dedupePercent} className="w-28 h-1.5" />
                  <span className="text-xs text-muted-foreground tabular-nums font-medium">
                    {dedupeProgressValue}/{dedupeTotal}
                  </span>
                </div>
                {currentDedupeGroup && (
                  <span className="hidden lg:block text-xs text-muted-foreground truncate max-w-48">
                    {currentDedupeGroup.artist} – {currentDedupeGroup.album}
                  </span>
                )}
              </>
            ) : isScanning ? (
              // Scanning state
              <>
                <div className="min-w-0 space-y-1.5">
                  <div className="flex items-center gap-2 min-w-0">
                    <StatusDot state={status === 'paused' ? 'paused' : preScanActive ? 'preparing' : 'running'} className="mt-0.5" />
                    <span className="rounded-full bg-primary/10 px-2 py-0.5 text-[11px] font-medium text-primary">
                      Step {currentPipelineStepIndex}/{pipelineStepsTotal}
                    </span>
                    <span className="truncate text-sm font-medium text-foreground">
                      {presentation?.currentStageLabel || phaseLabel}
                    </span>
                  </div>
                  <div className="hidden md:flex flex-wrap items-center gap-2 text-[11px]">
                    <span className="rounded-full border border-border/80 bg-muted/50 px-2.5 py-1 text-muted-foreground tabular-nums">
                      {overallPercentLabel} · {pipelineSummaryText}
                    </span>
                    {headerStageHeroText ? (
                      <span className="rounded-full border border-primary/25 bg-primary/8 px-2.5 py-1 font-medium text-foreground tabular-nums">
                        {headerStageHeroText}
                      </span>
                    ) : null}
                  </div>
                </div>
                <div className="hidden lg:flex min-w-[17rem] flex-col gap-1.5">
                  <div className="flex items-center justify-between gap-3 text-[10px] uppercase tracking-[0.16em] text-muted-foreground">
                    <span>Run</span>
                    <span className="tabular-nums">{pipelinePercentCompact > 0 && pipelinePercentCompact < 1 ? '0.1%+' : `${pipelinePercentCompact.toFixed(1)}%`}</span>
                  </div>
                  <Progress value={Math.max(1, pipelinePercentCompact)} className="h-1.5" />
                  <div className="flex items-center justify-between gap-3 text-[10px] uppercase tracking-[0.16em] text-muted-foreground">
                    <span>Step</span>
                    <span className="tabular-nums">{stagePercentLabel}</span>
                  </div>
                  {stageIndeterminate ? (
                    <div className="h-1.5 overflow-hidden rounded-full bg-muted">
                      <div className="h-full w-2/5 animate-pulse rounded-full bg-primary/80" />
                    </div>
                  ) : (
                    <Progress value={Math.max(1, visibleStageProgressPercent)} className="h-1.5" />
                  )}
                </div>
              </>
            ) : runtimeActive && primaryRuntime ? (
              <>
                <div className="min-w-0 space-y-1.5">
                  <div className="flex items-center gap-2 min-w-0">
                    <Loader2 className="h-4 w-4 shrink-0 animate-spin text-primary" />
                    <span className="rounded-full bg-primary/10 px-2 py-0.5 text-[11px] font-medium text-primary">
                      Runtime
                    </span>
                    <span className="truncate text-sm font-medium text-foreground">
                      {runtimeTitle}
                    </span>
                    <span className="hidden truncate text-xs text-muted-foreground md:inline">
                      {runtimeAction}
                    </span>
                  </div>
                  <div className="hidden md:flex flex-wrap items-center gap-2 text-[11px]">
                    <span className="rounded-full border border-primary/25 bg-primary/8 px-2.5 py-1 font-medium text-foreground">
                      {runtimeMessage || runtimeAction}
                    </span>
                    {runtimeDetail ? (
                      <span className="max-w-[34rem] truncate rounded-full border border-border/80 bg-muted/50 px-2.5 py-1 text-muted-foreground">
                        {runtimeDetail}
                      </span>
                    ) : null}
                    {runtimeItems.length > 1 ? (
                      <span className="rounded-full border border-border/80 bg-muted/50 px-2.5 py-1 text-muted-foreground">
                        +{runtimeItems.length - 1} runtime job
                      </span>
                    ) : null}
                  </div>
                </div>
                <div className="hidden lg:flex min-w-[17rem] flex-col gap-1.5">
                  <div className="flex items-center justify-between gap-3 text-[10px] uppercase tracking-[0.16em] text-muted-foreground">
                    <span>{runtimeProgressKnown ? 'Progress' : 'Working'}</span>
                    <span className="tabular-nums">{runtimeProgressKnown ? `${Math.round(runtimeProgressValue)}%` : 'live'}</span>
                  </div>
                  {runtimeProgressKnown ? (
                    <Progress value={Math.max(1, runtimeProgressValue)} className="h-1.5" />
                  ) : (
                    <div className="h-1.5 overflow-hidden rounded-full bg-muted">
                      <div className="h-full w-2/5 animate-pulse rounded-full bg-primary/80" />
                    </div>
                  )}
                </div>
              </>
            ) : libraryIndexActive ? (
              <>
                <div className="min-w-0 space-y-1.5">
                  <div className="flex items-center gap-2 min-w-0">
                    <Loader2 className="h-4 w-4 shrink-0 animate-spin text-primary" />
                    <span className="rounded-full bg-primary/10 px-2 py-0.5 text-[11px] font-medium text-primary">
                      Library index
                    </span>
                    <span className="truncate text-sm font-medium capitalize text-foreground">
                      {libraryIndexPhase || 'indexing'}
                    </span>
                  </div>
                  <div className="hidden md:flex flex-wrap items-center gap-2 text-[11px]">
                    <span className="rounded-full border border-primary/25 bg-primary/8 px-2.5 py-1 font-medium text-foreground tabular-nums">
                      {libraryIndexCounter}
                    </span>
                    {libraryIndexMessage ? (
                      <span className="max-w-[34rem] truncate rounded-full border border-border/80 bg-muted/50 px-2.5 py-1 text-muted-foreground">
                        {libraryIndexMessage}
                      </span>
                    ) : null}
                  </div>
                </div>
                <div className="hidden lg:flex min-w-[17rem] flex-col gap-1.5">
                  <div className="flex items-center justify-between gap-3 text-[10px] uppercase tracking-[0.16em] text-muted-foreground">
                    <span>{libraryIndexProgressKnown ? 'Index' : 'Working'}</span>
                    <span className="tabular-nums">
                      {libraryIndexProgressKnown ? `${libraryIndexProgressValue.toFixed(libraryIndexProgressValue < 10 ? 1 : 0)}%` : 'live'}
                    </span>
                  </div>
                  {libraryIndexProgressKnown ? (
                    <Progress value={Math.max(1, libraryIndexProgressValue)} className="h-1.5" />
                  ) : (
                    <div className="h-1.5 overflow-hidden rounded-full bg-muted">
                      <div className="h-full w-2/5 animate-pulse rounded-full bg-primary/80" />
                    </div>
                  )}
                </div>
              </>
            ) : isIdle && duplicateCount > 0 ? (
              // Idle with dupes - prompt to review
              <div className="flex items-center gap-3">
                <div className="flex items-center gap-2">
                  <CheckCircle2 className="w-4 h-4 text-success" />
                  <span className="text-sm text-muted-foreground">Ready</span>
                </div>
                <Link 
                  to="/tools"
                  className="hidden sm:flex items-center gap-1.5 text-sm font-medium text-warning hover:text-warning/80 transition-colors"
                >
                  <Package className="w-3.5 h-3.5" />
                  <span>Review {duplicateCount} duplicates in Tools</span>
                  <ArrowRight className="w-3 h-3" />
                </Link>
              </div>
            ) : (
              // Idle, no dupes
              <div className="flex items-center gap-3">
                <div className="flex items-center gap-2">
                  <CheckCircle2 className="w-4 h-4 text-success" />
                  <span className="text-sm text-muted-foreground">Ready</span>
                </div>
                <Button 
                  variant="ghost" 
                  size="sm" 
                  onClick={handleStartScan}
                  disabled={scanControls.isStarting}
                  className="hidden sm:flex h-7 text-xs gap-1.5 text-muted-foreground hover:text-foreground"
                >
                  <Play className="w-3 h-3" />
                  {scanControls.isStarting ? 'Starting...' : 'Start scan'}
                </Button>
              </div>
            )}
          </div>

          {/* Center/Right: Quick stats */}
          <div className="flex items-center gap-3">
            {isScanning && (
              <>
                <div className="hidden lg:flex items-center gap-1.5 rounded-full border border-border/80 bg-muted/50 px-3 py-1.5 text-xs text-muted-foreground">
                  <Clock3 className="h-3.5 w-3.5" />
                  <span>ETA</span>
                  <span className="font-medium text-foreground tabular-nums">{formatDurationShort(progress?.eta_seconds ?? null)}</span>
                </div>
                <Link
                  to="/scan"
                  className="hidden sm:flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium transition-all bg-primary/10 text-primary hover:bg-primary/20"
                >
                  <ArrowRight className="w-3.5 h-3.5" />
                  <span>Open scan details</span>
                </Link>
              </>
            )}
            {runtimeActive && primaryRuntime ? (
              <button
                onClick={() => navigate('/settings?tab=scaling')}
                className="hidden sm:flex items-center gap-2 rounded-full bg-primary/10 px-3 py-1.5 text-sm font-medium text-primary transition-all hover:bg-primary/20"
              >
                {primaryRuntime.bundle_type === 'musicbrainz_local' ? <Database className="h-3.5 w-3.5" /> : <Bot className="h-3.5 w-3.5" />}
                <span>{runtimeTitle}</span>
                <ArrowRight className="h-3.5 w-3.5" />
              </button>
            ) : null}
            {libraryIndexActive ? (
              <>
                <div className="hidden lg:flex items-center gap-1.5 rounded-full border border-border/80 bg-muted/50 px-3 py-1.5 text-xs text-muted-foreground">
                  <Clock3 className="h-3.5 w-3.5" />
                  <span>Index ETA</span>
                  <span className="font-medium text-foreground tabular-nums">{libraryIndexEta}</span>
                </div>
                <button
                  onClick={() => navigate('/library')}
                  className="hidden sm:flex items-center gap-2 rounded-full bg-primary/10 px-3 py-1.5 text-sm font-medium text-primary transition-all hover:bg-primary/20"
                >
                  <Database className="h-3.5 w-3.5" />
                  <span>Open library</span>
                  <ArrowRight className="h-3.5 w-3.5" />
                </button>
              </>
            ) : null}
            <button
              onClick={() => setShowLogs((v) => !v)}
              className="hidden sm:flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium transition-all bg-muted/60 text-muted-foreground hover:text-foreground hover:bg-muted"
            >
              <Terminal className="w-3.5 h-3.5" />
              <span>Logs</span>
              {showLogs ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
            </button>
            {/* Duplicates badge - prominent when > 0 */}
            {duplicateCount > 0 && (
              <button
                onClick={() => navigate('/tools')}
                className={cn(
                  "flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium transition-all",
                  "bg-warning/15 text-warning hover:bg-warning/25",
                  duplicateCount > 0 && "animate-pulse-subtle"
                )}
              >
                <Package className="w-3.5 h-3.5" />
                <span className="tabular-nums">{duplicateCount}</span>
                <span className="hidden sm:inline">Dupes</span>
              </button>
            )}
            
            {/* To be fixed badge */}
            {toBeFixedCount > 0 && (
              <button
                onClick={() => navigate('/library')}
                className={cn(
                  "flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium transition-all",
                  "bg-primary/15 text-primary hover:bg-primary/25"
                )}
              >
                <Wrench className="w-3.5 h-3.5" />
                <span className="tabular-nums">{toBeFixedCount}</span>
                <span className="hidden sm:inline">To Fix</span>
              </button>
            )}
            
            {/* Incomplete badge */}
            {incompleteCount > 0 && (
              <button
                onClick={() => navigate('/broken-albums')}
                className={cn(
                  "flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium transition-all",
                  "bg-destructive/15 text-destructive hover:bg-destructive/25"
                )}
              >
                <AlertTriangle className="w-3.5 h-3.5" />
                <span className="tabular-nums">{incompleteCount}</span>
                <span className="hidden sm:inline">Incomplete</span>
              </button>
            )}
            
            {/* Artists/Albums counts */}
            <div className="hidden lg:flex items-center gap-3 text-sm text-muted-foreground">
              <button
                onClick={() => navigate('/library')}
                className="hover:text-foreground transition-colors"
              >
                <span className="font-semibold text-foreground tabular-nums">{artistCount}</span>
                {' '}Artists
              </button>
              <span className="text-border">·</span>
              <button
                onClick={() => navigate('/library')}
                className="hover:text-foreground transition-colors"
              >
                <span className="font-semibold text-foreground tabular-nums">{albumCount}</span>
                {' '}Albums
              </button>
            </div>
          </div>
        </div>
        {showLogs && (
          <div className="border-t border-border/80 bg-muted/30">
            {isScanning && pipelineSteps.length > 0 && (
              <div className="px-4 pt-3 pb-1">
                <div className="flex flex-wrap items-center gap-2">
                  {pipelineSteps.map((step) => (
                    <span
                      key={step.key}
                      className={cn(
                        "inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium",
                      step.state === 'done'
                          ? "border-success/30 bg-success/10 text-success"
                          : step.state === 'active'
                            ? "border-primary/40 bg-primary/10 text-primary"
                            : "border-border bg-background/60 text-muted-foreground",
                      )}
                    >
                      {step.index}. {step.label}
                    </span>
                  ))}
                </div>
              </div>
            )}
            <div className="px-4 py-2 flex items-center justify-between gap-3 text-xs text-muted-foreground">
              <span>{runtimeActive && !libraryIndexActive && !isScanning ? 'Live runtime log' : 'Live backend log'} (last 8 lines, newest first)</span>
              <Button
                variant="ghost"
                size="sm"
                className="h-7 px-2 text-xs gap-1.5"
                onClick={() => { window.location.href = '/api/logs/download?lines=20000'; }}
              >
                <Download className="w-3.5 h-3.5" />
                Download
              </Button>
            </div>
            <div className="px-4 pb-3">
              <div className="h-40 overflow-hidden">
                {logLines.length === 0 ? (
                  <div className="text-muted-foreground">No logs yet…</div>
                ) : (
                  <BackendLogPanel
                    entries={logEntries}
                    lines={logLines}
                    maxLines={8}
                    compact
                    newestFirst
                    className="h-full overflow-hidden"
                  />
                )}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
