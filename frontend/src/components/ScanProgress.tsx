import { useState, useRef, useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { Play, Pause, Square, RefreshCw, Loader2, ChevronDown, ChevronUp, Sparkles, Database, Music, Cpu, Zap, AlertTriangle, Image, Tag, Package, Trash2, Clock, FolderInput, Terminal, Download, BarChart3, Activity, TimerReset, Gauge, ChevronRight, Info } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from '@/components/ui/alert-dialog';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { ProviderBadge } from '@/components/providers/ProviderBadge';
import { ProviderIcon } from '@/components/providers/ProviderIcon';
import { BackendLogPanel } from '@/components/BackendLogPanel';
import { MiniSparkline } from '@/components/scan/MiniSparkline';
import { StatusDot } from '@/components/scan/StatusDot';
import { cn } from '@/lib/utils';
import type { LogTailEntry, ProviderGatewayStatsBucket, ScanProgress as ScanProgressType } from '@/lib/api';
import { buildScanPipelineSteps } from '@/lib/scanPipeline';
import {
  getScanPreflight,
  type ScanPreflightResult,
  dedupeAll,
  improveAll,
  getDedupeProgress,
  getImproveAllProgress,
  getScanLogsTail,
  getScalingRuntime,
  type ScalingRuntimeResponse,
} from '@/lib/api';
import { buildScanPresentationModel } from '@/lib/scanPresentation';
import { toast } from 'sonner';

function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return '—';
  const total = Math.floor(seconds);
  const days = Math.floor(total / 86400);
  const hours = Math.floor((total % 86400) / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  const parts: string[] = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0) parts.push(`${minutes}m`);
  if (secs > 0 || parts.length === 0) parts.push(`${secs}s`);
  return parts.join(' ');
}

export type ScanType = 'full' | 'changed_only' | 'incomplete_only';

interface ScanProgressProps {
  progress: ScanProgressType;
  /** Real-time duplicate count from useDuplicates - used to determine card visibility */
  currentDuplicateCount?: number;
  /** Called when user starts a scan. Options: scan_type (full | changed_only | incomplete_only). */
  onStart: (options?: { scan_type?: ScanType; run_improve_after?: boolean }) => void;
  onPause: () => void;
  onResume: () => void;
  onStop: () => void;
  onClear?: (options?: { clear_audio_cache?: boolean; clear_mb_cache?: boolean }) => void;
  isStarting?: boolean;
  isPausing?: boolean;
  isResuming?: boolean;
  isStopping?: boolean;
  isClearing?: boolean;
  className?: string;
  /** Scan type: full (default), changed_only, or incomplete_only */
  scanType?: ScanType;
  /** Callbacks to update scan options from the component (optional; if not provided, options are internal state) */
  onScanTypeChange?: (t: ScanType) => void;
  /** Compact mode for simplified Scan page UI */
  compact?: boolean;
}

function CompactMetricCard({
  label,
  value,
  hint,
}: {
  label: string;
  value: string;
  hint?: string;
}) {
  return (
    <div className="rounded-lg border border-border bg-muted/30 px-3 py-3">
      <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">{label}</div>
      <div className="mt-1 text-lg font-semibold tabular-nums text-foreground">{value}</div>
      {hint ? <div className="mt-1 text-xs text-muted-foreground">{hint}</div> : null}
    </div>
  );
}

function CompactInsightCard({
  title,
  body,
  value,
  footer,
}: {
  title: string;
  body: string;
  value?: string;
  footer?: string;
}) {
  return (
    <div className="rounded-lg border border-border bg-muted/30 px-3 py-3">
      <div className="flex items-center justify-between gap-3">
        <div className="text-sm font-semibold text-foreground">{title}</div>
        {value ? <div className="text-xs font-medium tabular-nums text-primary">{value}</div> : null}
      </div>
      <div className="mt-1 text-xs leading-5 text-muted-foreground">{body}</div>
      {footer ? <div className="mt-2 text-[11px] text-muted-foreground">{footer}</div> : null}
    </div>
  );
}

type MetricTone = 'time' | 'content' | 'throughput' | 'output' | 'issue' | 'neutral';

function metricValueClass(tone: MetricTone) {
  switch (tone) {
    case 'time':
      return 'text-info';
    case 'content':
      return 'text-info';
    case 'throughput':
      return 'text-success';
    case 'output':
      return 'text-warning';
    case 'issue':
      return 'text-destructive';
    case 'neutral':
    default:
      return 'text-foreground';
  }
}

function formatNullableNumber(value: number | null | undefined, digits = 0): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return digits > 0 ? Number(value).toFixed(digits) : Math.round(Number(value)).toLocaleString();
}

function inferActivityType(line: string): 'matched' | 'skipped' | 'flagged' | 'exported' | 'error' {
  const raw = String(line || '').toLowerCase();
  if (raw.includes('error') || raw.includes('failed') || raw.includes('timeout')) return 'error';
  if (raw.includes('hardlink') || raw.includes('export') || raw.includes('library index upserted')) return 'exported';
  if (raw.includes('broken') || raw.includes('incomplete') || raw.includes('quarantine')) return 'flagged';
  if (raw.includes('strict matched') || raw.includes('trusted via') || raw.includes('matched')) return 'matched';
  return 'skipped';
}

function activityBadgeClass(type: 'matched' | 'skipped' | 'flagged' | 'exported' | 'error') {
  switch (type) {
    case 'matched':
      return 'border-success/30 bg-success/10 text-success';
    case 'flagged':
      return 'border-warning/30 bg-warning/10 text-warning';
    case 'exported':
      return 'border-info/30 bg-info/10 text-info';
    case 'error':
      return 'border-destructive/30 bg-destructive/10 text-destructive';
    case 'skipped':
    default:
      return 'border-border bg-background/60 text-muted-foreground';
  }
}

interface ScanStatCellProps {
  icon: React.ReactNode;
  label: string;
  value: string;
  tone?: MetricTone;
  description?: string;
  sparkline?: number[];
}

function ScanStatCell({ icon, label, value, tone = 'neutral', description, sparkline }: ScanStatCellProps) {
  return (
    <div className="rounded-xl border border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-surface))] px-3 py-3 sm:px-4">
      <div className="flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.16em] text-[hsl(var(--scan-text-secondary))]">
        <span className="text-[hsl(var(--scan-text-tertiary))]">{icon}</span>
        <span className="truncate">{label}</span>
      </div>
      <div className={cn('mt-2 text-2xl font-bold tabular-nums tracking-tight', metricValueClass(tone))}>{value}</div>
      {sparkline && sparkline.length > 1 ? (
        <MiniSparkline
          data={sparkline}
          className="mt-2 h-5 w-full"
          color={tone === 'throughput' ? 'hsl(var(--success))' : tone === 'time' ? 'hsl(var(--info))' : 'hsl(var(--muted-foreground))'}
        />
      ) : null}
      {description ? <div className="mt-2 text-xs leading-5 text-[hsl(var(--scan-text-secondary))]">{description}</div> : null}
    </div>
  );
}

interface SystemHealthRow {
  key: string;
  label: string;
  icon?: React.ReactNode;
  provider?: string;
  status: 'healthy' | 'degraded' | 'error' | 'idle';
  lookups?: string;
  hitRate?: number | null;
  latency?: string;
  notes?: string;
}

function SystemHealthTable({ rows }: { rows: SystemHealthRow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-[620px] w-full text-sm">
        <thead>
          <tr className="border-b border-[hsl(var(--scan-border-subtle))] text-[11px] uppercase tracking-[0.14em] text-[hsl(var(--scan-text-tertiary))]">
            <th className="px-4 py-3 text-left font-semibold">Provider</th>
            <th className="px-2 py-3 text-center font-semibold">Status</th>
            <th className="px-3 py-3 text-right font-semibold">Lookups</th>
            <th className="px-3 py-3 text-left font-semibold">Hit rate</th>
            <th className="px-3 py-3 text-right font-semibold">Latency</th>
            <th className="px-3 py-3 text-left font-semibold">Notes</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const hitRateValue = row.hitRate != null && Number.isFinite(row.hitRate) ? Math.max(0, Math.min(100, Number(row.hitRate))) : null;
            const hitTone = hitRateValue == null ? 'bg-[hsl(var(--scan-border))]' : hitRateValue >= 80 ? 'bg-success' : hitRateValue >= 50 ? 'bg-warning' : 'bg-destructive';
            return (
              <tr key={row.key} className="border-b border-[hsl(var(--scan-border-subtle))] text-[13px] text-[hsl(var(--scan-text-secondary))] last:border-b-0 hover:bg-[hsl(var(--scan-surface-raised))]">
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2 text-[13px] text-foreground">
                    {row.provider ? <ProviderIcon provider={row.provider} className="text-[hsl(var(--scan-text-secondary))]" size={14} /> : row.icon}
                    <span>{row.label}</span>
                  </div>
                </td>
                <td className="px-2 py-3 text-center">
                  <StatusDot
                    state={row.status === 'healthy' ? 'success' : row.status === 'degraded' ? 'degraded' : row.status === 'error' ? 'error' : 'idle'}
                    pulse={false}
                    label={row.status}
                    className="mx-auto"
                  />
                </td>
                <td className="px-3 py-3 text-right tabular-nums">{row.lookups || '—'}</td>
                <td className="px-3 py-3">
                  {hitRateValue == null ? (
                    <span className="text-[hsl(var(--scan-text-tertiary))]">—</span>
                  ) : (
                    <div className="flex items-center gap-2">
                      <div className="h-1.5 w-14 overflow-hidden rounded-full bg-[hsl(var(--scan-border))]">
                        <div className={cn('h-full rounded-full', hitTone)} style={{ width: `${hitRateValue}%` }} />
                      </div>
                      <span className="tabular-nums text-[11px] text-[hsl(var(--scan-text-secondary))]">{hitRateValue.toFixed(0)}%</span>
                    </div>
                  )}
                </td>
                <td className="px-3 py-3 text-right tabular-nums">{row.latency || '—'}</td>
                <td className="px-3 py-3 text-[11px] text-[hsl(var(--scan-text-tertiary))]" title={row.notes}>{row.notes || '—'}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function ScanProgress({
  progress,
  currentDuplicateCount,
  onStart,
  onPause,
  onResume,
  onStop,
  onClear,
  isStarting,
  isPausing,
  isResuming,
  isStopping,
  isClearing,
  className,
  scanType: scanTypeProp,
  onScanTypeChange,
  compact,
}: ScanProgressProps) {
  type MetricHistory = {
    elapsed: number[];
    eta: number[];
    estimatedTotal: number[];
    albumsPerHour: number[];
    artistsPerHour: number[];
  };

  const [expanded, setExpanded] = useState(false);
  const [showClearDialog, setShowClearDialog] = useState(false);
  const [preflightLoading, setPreflightLoading] = useState(false);
  const [preflightResult, setPreflightResult] = useState<ScanPreflightResult | null>(null);
  const [preflightVerifiedAtStart, setPreflightVerifiedAtStart] = useState(false);
  const [preflightPaths, setPreflightPaths] = useState<{ music_rw: boolean; dupes_rw: boolean } | null>(null);
  const [waitingForProgress, setWaitingForProgress] = useState(false);
  const [liveLogLines, setLiveLogLines] = useState<string[]>([]);
  const [liveLogEntries, setLiveLogEntries] = useState<LogTailEntry[]>([]);
  const [liveLogPath, setLiveLogPath] = useState('');
  const [showRawLogs, setShowRawLogs] = useState(false);
  const [compactPipelineOpen, setCompactPipelineOpen] = useState(false);
  const [compactLogsOpen, setCompactLogsOpen] = useState(true);
  const [historySamples, setHistorySamples] = useState<MetricHistory>({
    elapsed: [],
    eta: [],
    estimatedTotal: [],
    albumsPerHour: [],
    artistsPerHour: [],
  });
  const rawLogViewportRef = useRef<HTMLDivElement | null>(null);
  const rawLogStickToBottomRef = useRef(true);
  const hasToastedAiErrors = useRef(false);
  const [scanTypeInternal, setScanTypeInternal] = useState<ScanType>('full');
  const scanType = scanTypeProp ?? scanTypeInternal;
  const setScanType = onScanTypeChange ?? setScanTypeInternal;
  const isCompact = Boolean(compact);
  const [improveAllProgressData, setImproveAllProgressData] = useState<Awaited<ReturnType<typeof getImproveAllProgress>> | null>(null);
  const [postScanRunning, setPostScanRunning] = useState(false);
  const [magicRunning, setMagicRunning] = useState(false);

  const safeProgress = progress || {
    scanning: false,
    progress: 0,
    total: 0,
    status: 'idle' as const,
  };

  const {
    scanning,
    library_ready = false,
    background_enrichment_running = false,
    background_jobs = [],
    profile_backfill = null,
    progress: current,
    total,
    effective_progress,
    status,
    phase = null,
    current_step = null,
    ai_provider = '',
    ai_model = '',
    artists_processed = 0,
    artists_total = 0,
    detected_artists_total = 0,
    detected_albums_total = 0,
    resume_skipped_artists = 0,
    resume_skipped_albums = 0,
    scan_run_scope_preparing = false,
    scan_run_scope_stage = 'idle',
    scan_run_scope_done = 0,
    scan_run_scope_total = 0,
    scan_run_scope_percent = 0,
    scan_run_scope_artists_included = 0,
    scan_run_scope_albums_included = 0,
    ai_used_count = 0,
    mb_used_count = 0,
    ai_enabled = false,
    mb_enabled = false,
    audio_cache_hits = 0,
    audio_cache_misses = 0,
    mb_cache_hits = 0,
    mb_cache_misses = 0,
    duplicate_groups_count = 0,
    total_duplicates_count = 0,
    broken_albums_count = 0,
    missing_albums_count = 0,
    albums_without_artist_image = 0,
    albums_without_album_image = 0,
    albums_without_complete_tags = 0,
    albums_without_mb_id = 0,
    albums_without_artist_mb_id = 0,
    eta_seconds,
    threads_in_use,
    active_artists = [],
    format_done_count = 0,
    mb_done_count = 0,
    last_scan_summary = null,
    finalizing = false,
    auto_move_enabled = false,
    deduping = false,
    dedupe_progress = 0,
    dedupe_total = 0,
    paths_status: progressPathsStatus = null,
    scan_ai_batch_total = 0,
    scan_ai_batch_processed = 0,
    scan_ai_current_label = null,
    total_albums = 0,
    scan_steps_log = [],
    post_processing = false,
    post_processing_done = 0,
    post_processing_total = 0,
    post_processing_current_artist = null,
    post_processing_current_album = null,
    scan_discovery_running = false,
    scan_discovery_current_root = null,
    scan_discovery_roots_done = 0,
    scan_discovery_roots_total = 0,
    scan_discovery_files_found = 0,
    scan_discovery_folders_found = 0,
    scan_discovery_albums_found = 0,
    scan_discovery_artists_found = 0,
    scan_discovery_stage = null,
    scan_discovery_entries_scanned = 0,
    scan_discovery_root_entries_scanned = 0,
    scan_discovery_folders_done = 0,
    scan_discovery_folders_total = 0,
    scan_discovery_albums_done = 0,
    scan_discovery_albums_total = 0,
    scan_discovery_started_at = null,
    scan_preplan_done = 0,
    scan_preplan_total = 0,
    scan_pipeline_flags = {},
    scan_pipeline_sync_target = null,
    scan_incomplete_moved_count = 0,
    scan_incomplete_moved_mb = 0,
    scan_player_sync_target = null,
    scan_player_sync_ok = null,
    scan_player_sync_message = null,
    scan_start_time = null,
    resume_available = false,
    resume_available_by_scan_type = {},
    bootstrap_required = false,
    autonomous_mode = false,
    has_completed_full_scan = false,
    default_scan_type = 'full',
    scan_published_albums_count = 0,
    library_visible_albums_count = null,
    library_visible_artists_count = null,
    elapsed_seconds = null,
    scan_runtime_sec = null,
    phase_rate = null,
    phase_progress = null,
    stage_progress_done = 0,
    stage_progress_total = 0,
    stage_progress_percent = 0,
    stage_progress_unit = 'steps',
    overall_progress_done = 0,
    overall_progress_total = 0,
    overall_progress_percent = 0,
  } = safeProgress;

  const effectiveScanStart = useMemo(() => {
    if (scan_start_time != null && Number.isFinite(scan_start_time) && scan_start_time > 0) {
      return Number(scan_start_time);
    }
    if (scan_discovery_started_at != null && Number.isFinite(scan_discovery_started_at) && scan_discovery_started_at > 0) {
      return Number(scan_discovery_started_at);
    }
    return null;
  }, [scan_start_time, scan_discovery_started_at]);

  const elapsedSeconds = useMemo(() => {
    if (!scanning) return null;
    const runtimeValue = scan_runtime_sec ?? elapsed_seconds;
    if (runtimeValue != null && Number.isFinite(runtimeValue) && Number(runtimeValue) >= 0) {
      return Math.max(0, Math.floor(Number(runtimeValue)));
    }
    if (effectiveScanStart == null) return null;
    return Math.max(0, Math.floor(Date.now() / 1000 - effectiveScanStart));
  }, [scanning, scan_runtime_sec, elapsed_seconds, effectiveScanStart, current, scan_discovery_entries_scanned, scan_discovery_files_found, artists_processed, post_processing_done]);

  const etaSecondsValue = useMemo(() => {
    if (eta_seconds == null || !Number.isFinite(eta_seconds) || eta_seconds <= 0) return null;
    return Math.max(0, Math.round(Number(eta_seconds)));
  }, [eta_seconds]);

  const estimatedTotalSeconds = useMemo(() => {
    if (elapsedSeconds == null || etaSecondsValue == null) return null;
    return elapsedSeconds + etaSecondsValue;
  }, [elapsedSeconds, etaSecondsValue]);

  const phaseRateValue = useMemo(() => {
    if (!scanning) return null;
    if (phase_rate == null || !Number.isFinite(phase_rate) || Number(phase_rate) <= 0) return null;
    return Number(phase_rate);
  }, [scanning, phase_rate]);
  const { data: scalingRuntime } = useQuery<ScalingRuntimeResponse | null>({
    queryKey: ['scan-scaling-runtime', scanning ? 'running' : 'idle'],
    queryFn: () => getScalingRuntime(),
    refetchInterval: scanning ? 5000 : 15000,
    staleTime: 2000,
    retry: 1,
  });

  const phaseProgressValue = useMemo(() => {
    if (phase_progress == null || !Number.isFinite(phase_progress)) return null;
    const n = Number(phase_progress);
    return Math.max(0, Math.min(100, n));
  }, [phase_progress]);
  const stageProgressPercentValue = useMemo(() => {
    if (!Number.isFinite(stage_progress_percent)) return 0;
    return Math.max(0, Math.min(100, Number(stage_progress_percent)));
  }, [stage_progress_percent]);
  const stageProgressDoneValue = Math.max(0, Number(stage_progress_done || 0));
  const stageProgressTotalValue = Math.max(0, Number(stage_progress_total || 0));
  const stageProgressUnitLabel = String(stage_progress_unit || '').trim() || 'steps';
  const overallProgressPercentValue = useMemo(() => {
    if (!Number.isFinite(overall_progress_percent)) return 0;
    return Math.max(0, Math.min(100, Number(overall_progress_percent)));
  }, [overall_progress_percent]);
  const overallProgressDoneValue = Math.max(0, Number(overall_progress_done || 0));
  const overallProgressTotalValue = Math.max(0, Number(overall_progress_total || 0));

  const changedOnlyBlocked = Boolean(bootstrap_required);
  const resumableSnapshot =
    !scanning && (scanType === 'full' || scanType === 'changed_only')
      ? (resume_available_by_scan_type?.[scanType] ?? null)
      : null;
  const resumableAvailable = Boolean(resumableSnapshot?.available || (resume_available && resumableSnapshot));
  const startButtonLabel = resumableAvailable ? 'Resume scan' : 'Start scan';
  const resumableSummary = resumableSnapshot
    ? `${Number(resumableSnapshot.remaining_artists || 0).toLocaleString()} artists · ${Number(resumableSnapshot.remaining_albums || 0).toLocaleString()} albums remaining`
    : '';
  const pausedResumeSummary = (() => {
    if (artists_total > 0 || total_albums > 0) {
      return `${Number(artists_total || 0).toLocaleString()} artists · ${Number(total_albums || 0).toLocaleString()} albums still in scope.`;
    }
    return 'Remaining work stays queued in the resume state.';
  })();

  // Stage badge: use backend phase (format_analysis | identification_tags | ia_analysis | finalizing | moving_dupes | post_processing)
  const effectiveStage = phase ?? (post_processing ? 'post_processing' : (finalizing ? 'finalizing' : (deduping ? 'moving_dupes' : 'format_analysis')));
  const runScopePreparing = scanning && (phase === 'preparing_run_scope' || scan_run_scope_preparing);
  const runScopeStage = String(scan_run_scope_stage || 'idle');
  const runScopeStageLabel = runScopeStage === 'signatures'
    ? 'signatures'
    : runScopeStage === 'resume_compare'
      ? 'resume compare'
      : runScopeStage === 'resume_seed'
        ? 'resume seed'
        : runScopeStage === 'done'
          ? 'done'
          : 'preparing run scope';
  const runScopeProgressTotal = Math.max(0, scan_run_scope_total || detected_artists_total || artists_total || 0);
  const runScopeProgressDone = runScopeProgressTotal > 0
    ? Math.max(0, Math.min(scan_run_scope_done || 0, runScopeProgressTotal))
    : Math.max(0, scan_run_scope_done || 0);
  const runScopePercent = runScopeProgressTotal > 0
    ? Math.min(100, (runScopeProgressDone / runScopeProgressTotal) * 100)
    : Math.max(0, Math.min(100, Number(scan_run_scope_percent || 0)));
  const preScanAlbumsTotal = Math.max(0, scan_preplan_total || scan_discovery_albums_total || scan_discovery_folders_total);
  const preScanAlbumsDone = Math.max(0, scan_preplan_done || scan_discovery_albums_done || scan_discovery_folders_done);
  const preScanSnapshotTotal = Math.max(0, safeProgress.scan_prescan_cache_snapshot_total || detected_albums_total || total_albums || 0);
  const preScanSnapshotDone = Math.max(0, safeProgress.scan_prescan_cache_snapshot_rows || 0);
  const preScanSnapshotActive = scanning && Boolean(safeProgress.scan_prescan_cache_snapshot_running) && preScanSnapshotTotal > 0;
  const preScanCatchupTotal = Math.max(0, safeProgress.scan_published_catchup_total || 0);
  const preScanCatchupDone = Math.max(0, safeProgress.scan_published_catchup_done || 0);
  const preScanCatchupOk = Math.max(0, safeProgress.scan_published_catchup_ok || 0);
  const preScanCatchupFailed = Math.max(0, safeProgress.scan_published_catchup_failed || 0);
  const preScanCatchupCurrentArtist = String(safeProgress.scan_published_catchup_current_artist || '').trim();
  const preScanCatchupActive = scanning && Boolean(safeProgress.scan_published_catchup_running) && preScanCatchupTotal > 0;
  const preScanRootsTotal = Math.max(0, scan_discovery_roots_total);
  const preScanRootsDone = Math.max(0, scan_discovery_roots_done);
  const preScanStage = preScanCatchupActive ? 'library_rehydration' : (preScanSnapshotActive ? 'cache_snapshot' : String(scan_discovery_stage || ''));
  const effectivePreScanStage = preScanStage || (safeProgress.scan_resume_run_id ? 'resume_warmup' : '');
  const preScanStageLabel = preScanStage === 'library_rehydration'
    ? 'library rehydration'
    : preScanStage === 'album_candidates'
    ? 'album candidates'
    : preScanStage === 'filesystem'
      ? 'filesystem'
      : preScanStage === 'cache_snapshot'
        ? 'cache snapshot'
      : effectivePreScanStage === 'resume_warmup'
        ? 'resume warm-up'
      : preScanStage === 'ready'
        ? 'ready'
        : 'pre-scan';
  const mainPipelinePhaseActive =
    scanning &&
    Boolean(phase) &&
    !['pre_scan', 'preparing_run_scope'].includes(String(phase));
  const preScanActive =
    scanning &&
    !runScopePreparing &&
    !mainPipelinePhaseActive &&
    (
      phase === 'pre_scan' ||
      preScanCatchupActive ||
      preScanSnapshotActive ||
      scan_discovery_running ||
      scan_discovery_stage === 'filesystem' ||
      scan_discovery_stage === 'album_candidates' ||
      preScanAlbumsTotal > 0 ||
      scan_discovery_files_found > 0
    );
  const preScanPercent = (() => {
    if (!preScanActive) return 0;
    if (preScanCatchupActive) {
      const total = Math.max(1, preScanCatchupTotal);
      const done = Math.max(0, Math.min(preScanCatchupDone, total));
      return Math.floor((100 * done) / total);
    }
    if (preScanSnapshotActive) {
      const total = Math.max(1, preScanSnapshotTotal);
      const done = Math.max(0, Math.min(preScanSnapshotDone, total));
      return Math.floor((100 * done) / total);
    }
    if (preScanStage === 'ready') return 100;
    if (preScanStage === 'album_candidates' || preScanAlbumsTotal > 0) {
      const total = Math.max(1, preScanAlbumsTotal);
      const done = Math.max(0, Math.min(preScanAlbumsDone, total));
      return Math.floor(70 + (25 * done) / total);
    }
    if (preScanStage === 'filesystem' || preScanRootsTotal > 0) {
      const total = Math.max(1, preScanRootsTotal);
      const done = Math.max(0, Math.min(preScanRootsDone, total));
      return Math.floor((70 * done) / total);
    }
    return 0;
  })();
  const preScanProgressTotal = preScanSnapshotActive
    ? preScanSnapshotTotal
    : preScanCatchupActive
    ? preScanCatchupTotal
    : (preScanAlbumsTotal > 0 ? preScanAlbumsTotal : preScanRootsTotal);
  const preScanProgressDone = preScanSnapshotActive
    ? Math.max(0, Math.min(preScanSnapshotDone, preScanSnapshotTotal))
    : preScanCatchupActive
    ? Math.max(0, Math.min(preScanCatchupDone, preScanCatchupTotal))
    : preScanAlbumsTotal > 0
    ? Math.max(0, Math.min(preScanAlbumsDone, preScanAlbumsTotal))
    : Math.max(0, Math.min(preScanRootsDone, preScanRootsTotal));
  const preScanIndeterminate =
    preScanActive &&
    (
      preScanProgressTotal <= 0 ||
      (preScanSnapshotActive && preScanSnapshotDone <= 0) ||
      (preScanCatchupActive && preScanCatchupDone <= 0) ||
      (preScanStage === 'filesystem' && preScanRootsTotal <= 1)
    );
  // Progress bar must reflect real end-to-end work.
  // During scan, include post-processing when present, otherwise use artist progress.
  const hasPostWork = scanning && (post_processing || post_processing_total > 0);
  const scanUnitsDone = Math.max(0, Math.min(artists_processed, artists_total));
  const scanUnitsTotal = Math.max(0, artists_total);
  const postUnitsDone = Math.max(0, Math.min(post_processing_done, post_processing_total));
  const postUnitsTotal = Math.max(0, post_processing_total);
  const compositeDone = scanUnitsDone + postUnitsDone;
  const compositeTotal = scanUnitsTotal + postUnitsTotal;
  const stagePercentageExact = runScopePreparing
    ? runScopePercent
    : preScanActive
      ? preScanPercent
      : (stageProgressTotalValue > 0 ? stageProgressPercentValue : 0);
  const stagePercentage = scanning
    ? Math.min(100, Math.floor(stagePercentageExact))
    : Math.round(stagePercentageExact);
  const runScopeIndeterminate = runScopePreparing && runScopeProgressTotal <= 0;
  const stageTransitioning =
    scanning &&
    !runScopePreparing &&
    !preScanActive &&
    Number(stageProgressTotalValue || 0) > 0 &&
    Number(stageProgressDoneValue || 0) >= Number(stageProgressTotalValue || 0) &&
    !['finalizing', 'background_enrichment'].includes(String(effectiveStage || ''));
  const stageIndeterminate =
    scanning &&
    (
      preScanIndeterminate ||
      runScopeIndeterminate ||
      stageTransitioning ||
      (
        !runScopePreparing &&
        !preScanActive &&
        Number(stageProgressTotalValue || 0) <= 0 &&
        (effectiveStage === 'finalizing' || effectiveStage === 'background_enrichment')
      )
    );
  const visibleStagePercentageExact =
    stageIndeterminate
      ? 0
      : stageProgressTotalValue > 0 && stageProgressDoneValue > 0
        ? Math.max(1, Math.min(100, stagePercentageExact))
        : Math.max(0, Math.min(100, stagePercentageExact));
  const stageHeadline = stageIndeterminate
    ? (
      preScanIndeterminate
        ? 'Estimating…'
        : runScopeIndeterminate
          ? 'Scoping…'
          : stageTransitioning
            ? 'Hand-off…'
            : 'Finishing'
    )
    : (stagePercentageExact > 0 && stagePercentageExact < 1)
      ? '0.1%+'
      : stagePercentageExact < 10
        ? `${stagePercentageExact.toFixed(1)}%`
        : `${stagePercentage}%`;
  const phaseLabel = runScopePreparing
    ? `Preparing run scope · ${runScopeStageLabel}`
    : preScanActive
      ? `Pre-scan · ${preScanStageLabel}`
      : effectiveStage === 'incomplete_move'
        ? 'Quarantine incompletes'
        : effectiveStage === 'export'
          ? 'Build library'
      : effectiveStage === 'profile_enrichment'
        ? 'Profile enrichment'
        : effectiveStage === 'format_analysis'
          ? 'Format analysis'
          : effectiveStage === 'identification_tags'
            ? 'Identification & tags'
            : effectiveStage === 'ia_analysis'
              ? 'AI analysis'
              : effectiveStage === 'moving_dupes'
                ? 'Moving dupes'
                : effectiveStage === 'post_processing'
                  ? 'Post-processing'
                  : effectiveStage === 'background_enrichment'
                    ? 'Background enrichment'
                    : effectiveStage === 'finalizing'
                      ? 'Finalizing'
                      : 'Scanning';
  const stageSummaryLabel = runScopePreparing
    ? (runScopeIndeterminate
      ? `estimating scope · ${scan_run_scope_artists_included.toLocaleString()} artists in scope`
      : runScopeProgressTotal > 0
      ? `${runScopeProgressDone.toLocaleString()} / ${runScopeProgressTotal.toLocaleString()} artists`
      : 'Preparing effective scope')
    : preScanActive
      ? (preScanIndeterminate
        ? `estimating scope · visited ${scan_discovery_entries_scanned.toLocaleString()} · audio ${scan_discovery_files_found.toLocaleString()}`
        : preScanCatchupActive
        ? `${preScanProgressDone.toLocaleString()} / ${preScanProgressTotal.toLocaleString()} artists · ok ${preScanCatchupOk.toLocaleString()} · failed ${preScanCatchupFailed.toLocaleString()}${preScanCatchupCurrentArtist ? ` · ${preScanCatchupCurrentArtist}` : ''}`
        : preScanStage === 'album_candidates' && preScanProgressTotal > 0
        ? `${preScanProgressDone.toLocaleString()} / ${preScanProgressTotal.toLocaleString()} albums`
        : preScanProgressTotal > 0
          ? `${preScanProgressDone.toLocaleString()} / ${preScanProgressTotal.toLocaleString()} roots`
          : (effectivePreScanStage === 'resume_warmup'
            ? 'Restoring cached run plan before worker stages start'
            : 'Discovering library candidates'))
      : effectiveStage === 'incomplete_move'
        ? (safeProgress.scan_incomplete_move_total && safeProgress.scan_incomplete_move_total > 0
          ? `${Number(safeProgress.scan_incomplete_move_done || 0).toLocaleString()} / ${Number(safeProgress.scan_incomplete_move_total || 0).toLocaleString()} albums`
          : 'Quarantining incomplete albums')
      : effectiveStage === 'export'
        ? (safeProgress.export_albums_total && safeProgress.export_albums_total > 0
          ? `${Number(safeProgress.export_albums_done || 0).toLocaleString()} / ${Number(safeProgress.export_albums_total || 0).toLocaleString()} albums`
          : 'Building clean library')
      : stageProgressTotalValue > 0
        ? `${stageProgressDoneValue.toLocaleString()} / ${stageProgressTotalValue.toLocaleString()} ${stageProgressUnitLabel}`
        : effectiveStage === 'finalizing'
          ? 'Saving results and closing run'
          : effectiveStage === 'background_enrichment'
            ? 'Finishing provider-only enrichments'
            : 'Running current stage';

  // Toast once when scan finishes with AI errors
  useEffect(() => {
    if (scanning) hasToastedAiErrors.current = false;
  }, [scanning]);
  useEffect(() => {
    if (changedOnlyBlocked && scanType === 'changed_only') {
      setScanType('full');
    }
  }, [changedOnlyBlocked, scanType, setScanType]);
  useEffect(() => {
    if (!scanning && last_scan_summary?.ai_errors?.length && !hasToastedAiErrors.current) {
      const n = last_scan_summary.ai_errors.length;
      toast.error(`Scan finished but ${n} AI error(s) occurred. Check summary for details.`);
      hasToastedAiErrors.current = true;
    }
  }, [scanning, last_scan_summary]);

  // Hide "Starting scan…" spinner as soon as we either see progress or the scan ends.
  // This prevents the UI from getting stuck on the spinner if the scan is very fast
  // and the frontend never observes a (scanning && total > 0) state.
  useEffect(() => {
    if (!waitingForProgress) return;
    if (!scanning) {
      setWaitingForProgress(false);
      return;
    }
    if (artists_total > 0 || total > 0 || preScanActive || runScopePreparing) {
      setWaitingForProgress(false);
    }
  }, [waitingForProgress, scanning, artists_total, total, preScanActive, runScopePreparing]);

  // Poll improve-all progress when not scanning
  useEffect(() => {
    if (scanning) return;
    const tick = async () => {
      try {
        const improve = await getImproveAllProgress();
        setImproveAllProgressData(improve);
      } catch {
        // ignore
      }
    };
    tick();
    const id = setInterval(tick, 3000);
    return () => clearInterval(id);
  }, [scanning]);

  // Power-user live backend logs while scan/post-processing is active.
  useEffect(() => {
    if (!scanning && !post_processing) return;
    let cancelled = false;
    const tick = async () => {
      try {
        const data = await getScanLogsTail(isCompact ? 80 : 220);
        if (!cancelled) {
          setLiveLogLines(Array.isArray(data?.lines) ? data.lines : []);
          setLiveLogEntries(Array.isArray(data?.entries) ? data.entries : []);
          setLiveLogPath(data?.path ?? '');
        }
      } catch {
        // ignore transient log polling errors
      }
    };
    tick();
    const id = setInterval(tick, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [scanning, post_processing, isCompact]);

  // Keep the raw log viewport pinned to the bottom by default.
  useEffect(() => {
    if (!showRawLogs) return;
    const el = rawLogViewportRef.current;
    if (!el) return;
    rawLogStickToBottomRef.current = true;
    el.scrollTop = el.scrollHeight;

    const onScroll = () => {
      const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
      rawLogStickToBottomRef.current = distanceFromBottom < 24;
    };
    el.addEventListener('scroll', onScroll, { passive: true } as AddEventListenerOptions);
    return () => el.removeEventListener('scroll', onScroll as EventListener);
  }, [showRawLogs]);

  useEffect(() => {
    if (!showRawLogs) return;
    const el = rawLogViewportRef.current;
    if (!el) return;
    if (rawLogStickToBottomRef.current) {
      el.scrollTop = el.scrollHeight;
    }
  }, [liveLogLines, showRawLogs]);

  const hasActiveStep = scanning && active_artists.length > 0 && active_artists[0]?.current_album && active_artists[0].current_album.status !== 'done';
  const currentArtist = active_artists?.[0]?.artist_name || '';
  const improveAllRunning = improveAllProgressData?.running ?? false;
  
  // Use real-time duplicate count if available, fallback to last_scan_summary
  const actualDuplicateCount = currentDuplicateCount ?? (last_scan_summary?.duplicate_groups_count ?? 0);
  const hasDuplicates = actualDuplicateCount > 0;
  const canDedupe = hasDuplicates && (last_scan_summary?.dupes_moved_this_scan ?? 0) < actualDuplicateCount;
  const dupesAlreadyAllMoved = (last_scan_summary?.duplicate_groups_count ?? 0) > 0 && (last_scan_summary?.dupes_moved_this_scan ?? 0) >= (last_scan_summary?.duplicate_groups_count ?? 0);
  const showDiscovery =
    scanning &&
    (preScanActive ||
      scan_discovery_running ||
      scan_discovery_entries_scanned > 0 ||
      scan_discovery_files_found > 0 ||
      scan_discovery_folders_found > 0 ||
      (artists_total === 0 && scan_discovery_albums_found > 0));
  const currentStepLabel = hasActiveStep
    ? (active_artists[0].current_album!.status_details || active_artists[0].current_album!.status || 'processing')
    : '';

  const workflowStage = useMemo<'undupe' | 'fix' | 'done'>(() => {
    if (canDedupe && !dupesAlreadyAllMoved) return 'undupe';
    if (!improveAllRunning) return 'done';
    return 'fix';
  }, [canDedupe, dupesAlreadyAllMoved, improveAllRunning]);

  const profileBackfillRunning = Boolean(profile_backfill?.running);
  const profileBackfillDone = Math.max(0, Number(profile_backfill?.current ?? 0));
  const profileBackfillTotal = Math.max(0, Number(profile_backfill?.total ?? 0));
  const profileBackfillArtist = String(profile_backfill?.current_artist ?? '').trim();
  const backgroundJobSummary = useMemo(() => {
    const names = Array.isArray(background_jobs)
      ? background_jobs
          .map((job) => String(job?.job_type ?? '').trim())
          .filter(Boolean)
      : [];
    if (!names.length) return '';
    const unique = Array.from(new Set(names));
    return unique.join(' · ');
  }, [background_jobs]);
  const libraryUsable = !scanning && Boolean(library_ready || Number(scan_published_albums_count || 0) > 0);
  const visibleAlbums = library_visible_albums_count != null ? Math.max(0, Number(library_visible_albums_count || 0)) : Math.max(0, Number(scan_published_albums_count || 0));
  const visibleArtists = library_visible_artists_count != null ? Math.max(0, Number(library_visible_artists_count || 0)) : 0;
  const libraryStatusTitle = libraryUsable
    ? (background_enrichment_running ? 'Library ready' : 'Library ready')
    : 'Idle';
  const libraryStatusBody = libraryUsable && background_enrichment_running
    ? (
        profileBackfillRunning && profileBackfillTotal > 0
          ? `Visible now: ${visibleAlbums} album(s), ${visibleArtists} artist(s) · background enrichment ${profileBackfillDone}/${profileBackfillTotal} artists`
          : `Visible now: ${visibleAlbums} album(s), ${visibleArtists} artist(s) · background enrichment running`
      )
    : (libraryUsable ? `Visible now: ${visibleAlbums} album(s), ${visibleArtists} artist(s) · background enrichment finished` : 'Ready to scan');
  const pipeline = useMemo(() => buildScanPipelineSteps(safeProgress), [safeProgress]);
  const pipelineStageFraction = runScopePreparing
    ? Math.max(0, Math.min(1, runScopePercent / 100))
    : preScanActive
      ? Math.max(0, Math.min(1, preScanPercent / 100))
      : stageProgressTotalValue > 0
        ? Math.max(0, Math.min(1, stagePercentageExact / 100))
        : Math.max(0, Math.min(1, overallProgressPercentValue / 100));
  const pipelineOverallDoneSteps = scanning && pipeline.total > 0
    ? Math.max(0, Math.min(pipeline.total, (pipeline.currentIndex - 1) + pipelineStageFraction))
    : 0;
  const pipelineOverallTotalSteps = pipeline.total;
  const pipelineOverallPercentageExact = scanning && pipelineOverallTotalSteps > 0
    ? Math.max(0, Math.min(100, (pipelineOverallDoneSteps / pipelineOverallTotalSteps) * 100))
    : overallProgressPercentValue;
  const presentation = useMemo(
    () => buildScanPresentationModel(safeProgress, scalingRuntime),
    [safeProgress, scalingRuntime],
  );

  useEffect(() => {
    if (!scanning) {
      setHistorySamples({
        elapsed: [],
        eta: [],
        estimatedTotal: [],
        albumsPerHour: [],
        artistsPerHour: [],
      });
      return;
    }
    setHistorySamples((prev) => {
      const append = (bucket: number[], value: number | null | undefined) => {
        if (value == null || !Number.isFinite(value)) return bucket;
        const next = [...bucket, Number(value)];
        return next.slice(-24);
      };
      return {
        elapsed: append(prev.elapsed, elapsedSeconds),
        eta: append(prev.eta, etaSecondsValue),
        estimatedTotal: append(prev.estimatedTotal, estimatedTotalSeconds),
        albumsPerHour: append(prev.albumsPerHour, presentation.albumsPerHour),
        artistsPerHour: append(prev.artistsPerHour, presentation.artistsPerHour),
      };
    });
  }, [scanning, elapsedSeconds, etaSecondsValue, estimatedTotalSeconds, presentation.albumsPerHour, presentation.artistsPerHour]);

  if (isCompact) {
    const pipelinePercent = Math.max(0, Math.min(100, presentation.pipelineOverallPercent));
    const stagePercentCompact = Math.max(0, Math.min(100, presentation.currentStagePercent));
    const providerStats = scalingRuntime?.provider_gateway?.providers || {};
    const mbStats = scalingRuntime?.musicbrainz;
    const albumsPerHourLabel = presentation.albumsPerHour != null ? `${presentation.albumsPerHour.toFixed(1)}/h` : '—';
    const artistsPerHourLabel = presentation.artistsPerHour != null ? `${presentation.artistsPerHour.toFixed(1)}/h` : '—';
    const etaConfidenceLabel = presentation.etaConfidence === 'low'
      ? 'ETA is still provisional at this stage.'
      : presentation.etaConfidence === 'medium'
        ? 'ETA is stabilizing from measured throughput.'
        : 'ETA is based on a stable amount of measured work.';
    const providerInsightRows = [
      {
        key: 'discogs',
        title: 'Discogs',
        value: `${presentation.providerMatches.discogs.toLocaleString()} matches`,
        body: providerStats.discogs
          ? `Lookup hit rate ${Number(providerStats.discogs.lookup_hit_rate || 0).toFixed(1)}% · avg ${Number(providerStats.discogs.avg_network_requests_per_lookup || 0).toFixed(2)} network req/lookup`
          : 'Provider gateway metrics are warming up.',
        footer: providerStats.discogs
          ? `429s ${Number(providerStats.discogs.rate_limited_count || 0).toLocaleString()} · latency ${Number(providerStats.discogs.avg_latency_ms || 0).toFixed(0)} ms`
          : undefined,
      },
      {
        key: 'lastfm',
        title: 'Last.fm',
        value: `${presentation.providerMatches.lastfm.toLocaleString()} matches`,
        body: providerStats.lastfm
          ? `Lookup hit rate ${Number(providerStats.lastfm.lookup_hit_rate || 0).toFixed(1)}% · saved ${Number(providerStats.lastfm.lookup_saved_count || 0).toLocaleString()} lookups`
          : 'Provider gateway metrics are warming up.',
        footer: providerStats.lastfm
          ? `Latency ${Number(providerStats.lastfm.avg_latency_ms || 0).toFixed(0)} ms`
          : undefined,
      },
      {
        key: 'bandcamp',
        title: 'Bandcamp',
        value: `${presentation.providerMatches.bandcamp.toLocaleString()} matches`,
        body: providerStats.bandcamp
          ? `Lookup hit rate ${Number(providerStats.bandcamp.lookup_hit_rate || 0).toFixed(1)}% · saved ${Number(providerStats.bandcamp.lookup_saved_count || 0).toLocaleString()} lookups`
          : 'Provider gateway metrics are warming up.',
        footer: providerStats.bandcamp
          ? `Latency ${Number(providerStats.bandcamp.avg_latency_ms || 0).toFixed(0)} ms`
          : undefined,
      },
    ];

    if (!scanning && status !== 'running') {
      if (waitingForProgress) {
        return (
          <div className={cn("rounded-xl bg-card border border-border overflow-hidden", className)}>
            <div className="p-10 flex flex-col items-center justify-center gap-4">
              <Loader2 className="w-10 h-10 animate-spin text-primary" />
              <div className="text-base font-medium text-foreground">Starting scan…</div>
              <div className="text-sm text-muted-foreground">PMDA is restoring the run context and preparing the first measurable counters.</div>
            </div>
          </div>
        );
      }
      return (
        <div className={cn("rounded-xl bg-card border border-border overflow-hidden", className)}>
          <div className="p-6 space-y-5">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div className="space-y-1">
                <h2 className="text-lg font-semibold text-foreground">Library scan</h2>
                <p className="text-sm text-muted-foreground">
                  Configure the next scan run, resume an interrupted one, or inspect the last completed summary.
                </p>
              </div>
              <div className="flex flex-wrap gap-2">
                {(
                  <Button onClick={() => onStart({ scan_type: scanType })} disabled={isStarting} className="gap-2">
                    {isStarting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                    {startButtonLabel}
                  </Button>
                )}
                <Link
                  to="/statistics"
                  className="inline-flex h-10 items-center gap-1.5 rounded-md border border-border px-3 text-xs text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                >
                  <BarChart3 className="h-3.5 w-3.5" />
                  Open advanced statistics
                </Link>
              </div>
            </div>

            <div className={cn(
              "rounded-md border px-3 py-2 text-xs",
              bootstrap_required
                ? "border-warning/40 bg-warning/10 text-warning"
                : autonomous_mode && has_completed_full_scan
                  ? "border-success/30 bg-success/10 text-success"
                  : "border-border bg-muted/40 text-muted-foreground"
            )}>
              {bootstrap_required
                ? "Initial full scan required before changed-only runs."
                : autonomous_mode && has_completed_full_scan
                  ? "Bootstrap complete. Default scan switched to changed only."
                  : "Choose scan mode manually."}
              <span className="ml-1 text-muted-foreground">
                (default: {String(default_scan_type || 'full').replace('_', ' ')})
              </span>
            </div>

            {resumableAvailable && (
              <div className="rounded-md border border-primary/30 bg-primary/10 px-3 py-2 text-xs text-primary/90">
                Interrupted <span className="font-medium text-foreground">{scanType === 'changed_only' ? 'changed-only' : 'full'}</span> scan available.
                <span className="ml-1">{resumableSummary}.</span>
                <span className="ml-1 text-muted-foreground">You can change AI settings before resuming.</span>
              </div>
            )}

            <div className="flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
              <span className="text-foreground font-medium">Mode:</span>
              <div className="flex items-center gap-1">
                <Button
                  variant={scanType === 'full' ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => setScanType('full')}
                >
                  Full
                </Button>
                <Button
                  variant={scanType === 'changed_only' ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => setScanType('changed_only')}
                  disabled={changedOnlyBlocked}
                >
                  Changed only
                </Button>
              </div>
            </div>

            <div className="grid gap-3 md:grid-cols-3">
              <CompactMetricCard label="Library status" value={libraryStatusTitle} hint={libraryStatusBody} />
              <CompactMetricCard label="Visible albums" value={visibleAlbums.toLocaleString()} hint="Published into the live PMDA library view." />
              <CompactMetricCard label="Visible artists" value={visibleArtists.toLocaleString()} hint="Artists currently exposed in the live library view." />
            </div>
          </div>
        </div>
      );
    }

    const pipelineTooltip = presentation.whySteps.join(' ');
    const activeFocus = active_artists[0];
    const currentFocusMode: 'album' | 'artist' | 'system' = hasActiveStep
      ? 'album'
      : currentArtist
        ? 'artist'
        : 'system';
    const currentFocusTitle = currentFocusMode === 'album'
      ? `${activeFocus.artist_name} — "${activeFocus.current_album?.album_title || 'Current album'}"`
      : currentFocusMode === 'artist'
        ? currentArtist
        : presentation.currentStageLabel;
    const currentFocusSubtitle = currentFocusMode === 'album'
      ? (activeFocus.current_album?.step_summary || activeFocus.current_album?.status_details || 'Processing current album')
      : currentFocusMode === 'artist'
        ? `${activeFocus?.albums_processed || 0}/${activeFocus?.total_albums || 0} albums processed for this artist`
        : presentation.humanExplanation;
    const currentFocusDetail = currentFocusMode === 'album'
      ? (activeFocus.current_album?.step_response || '')
      : currentFocusMode === 'artist'
        ? `Current artist focus`
        : presentation.stageHeroLabel;
    const recentActivity = Array.isArray(scan_steps_log) ? scan_steps_log.slice(-18).reverse() : [];

    const gatewayProviderStats: Record<string, ProviderGatewayStatsBucket> = providerStats;
    const providerLookupsTotal = Object.values(gatewayProviderStats).reduce((sum, bucket) => sum + Number(bucket.lookup_request_count || bucket.request_count || 0), 0);
    const cacheTotal = Number(audio_cache_hits || 0) + Number(audio_cache_misses || 0);
    const audioCacheHitRate = cacheTotal > 0 ? (Number(audio_cache_hits || 0) / cacheTotal) * 100 : null;

    const systemHealthRows: SystemHealthRow[] = [
      {
        key: 'musicbrainz',
        label: 'MusicBrainz mirror',
        provider: 'musicbrainz',
        status: mbStats?.enabled ? 'healthy' : 'idle',
        lookups: formatNullableNumber(mbStats?.completed_count),
        hitRate: null,
        latency: mbStats?.avg_latency_ms != null ? `${Number(mbStats.avg_latency_ms).toFixed(0)} ms` : '—',
        notes: mbStats?.mirror_enabled
          ? `Mirror enabled · queue ${Number(mbStats.queue_pending || 0)} pending · ${Number(mbStats.queue_waiters || 0)} waiters`
          : 'Public API path',
      },
      {
        key: 'gateway',
        label: 'Provider gateway',
        icon: <Zap className="h-3.5 w-3.5 text-info" />,
        status: scalingRuntime?.provider_gateway?.enabled ? 'healthy' : 'idle',
        lookups: providerLookupsTotal > 0 ? providerLookupsTotal.toLocaleString() : '—',
        hitRate: null,
        latency: '—',
        notes: scalingRuntime?.provider_gateway?.enabled
          ? `Inflight ${Number(scalingRuntime.provider_gateway.inflight || 0)}/${Number(scalingRuntime.provider_gateway.max_inflight || 0)} · peak ${Number(scalingRuntime.provider_gateway.max_inflight_observed || 0)}`
          : 'Disabled',
      },
      {
        key: 'discogs',
        label: 'Discogs',
        provider: 'discogs',
        status: gatewayProviderStats.discogs?.rate_limited_count ? 'degraded' : 'healthy',
        lookups: formatNullableNumber(gatewayProviderStats.discogs?.lookup_request_count || gatewayProviderStats.discogs?.request_count),
        hitRate: gatewayProviderStats.discogs?.lookup_hit_rate ?? gatewayProviderStats.discogs?.cache_hit_rate ?? null,
        latency: gatewayProviderStats.discogs?.avg_latency_ms != null ? `${Number(gatewayProviderStats.discogs.avg_latency_ms).toFixed(0)} ms` : '—',
        notes: `${presentation.providerMatches.discogs.toLocaleString()} matches${gatewayProviderStats.discogs?.rate_limited_count ? ` · 429s ${Number(gatewayProviderStats.discogs.rate_limited_count || 0)}` : ''}`,
      },
      {
        key: 'lastfm',
        label: 'Last.fm',
        provider: 'lastfm',
        status: gatewayProviderStats.lastfm ? 'healthy' : 'idle',
        lookups: formatNullableNumber(gatewayProviderStats.lastfm?.lookup_request_count || gatewayProviderStats.lastfm?.request_count),
        hitRate: gatewayProviderStats.lastfm?.lookup_hit_rate ?? gatewayProviderStats.lastfm?.cache_hit_rate ?? null,
        latency: gatewayProviderStats.lastfm?.avg_latency_ms != null ? `${Number(gatewayProviderStats.lastfm.avg_latency_ms).toFixed(0)} ms` : '—',
        notes: `${presentation.providerMatches.lastfm.toLocaleString()} matches`,
      },
      {
        key: 'bandcamp',
        label: 'Bandcamp',
        provider: 'bandcamp',
        status: gatewayProviderStats.bandcamp ? 'healthy' : 'idle',
        lookups: formatNullableNumber(gatewayProviderStats.bandcamp?.lookup_request_count || gatewayProviderStats.bandcamp?.request_count),
        hitRate: gatewayProviderStats.bandcamp?.lookup_hit_rate ?? gatewayProviderStats.bandcamp?.cache_hit_rate ?? null,
        latency: gatewayProviderStats.bandcamp?.avg_latency_ms != null ? `${Number(gatewayProviderStats.bandcamp.avg_latency_ms).toFixed(0)} ms` : '—',
        notes: `${presentation.providerMatches.bandcamp.toLocaleString()} matches`,
      },
      {
        key: 'audio-cache',
        label: 'Audio cache',
        icon: <Database className="h-3.5 w-3.5 text-info" />,
        status: cacheTotal > 0 ? 'healthy' : 'idle',
        lookups: cacheTotal > 0 ? cacheTotal.toLocaleString() : '—',
        hitRate: audioCacheHitRate,
        latency: '—',
        notes: `${Number(audio_cache_hits || 0).toLocaleString()} hits · ${Number(audio_cache_misses || 0).toLocaleString()} misses`,
      },
      {
        key: 'ai',
        label: 'AI / ambiguity',
        icon: <Sparkles className="h-3.5 w-3.5 text-primary" />,
        status: ai_enabled ? 'healthy' : 'idle',
        lookups: scan_ai_batch_total > 0 ? `${Number(scan_ai_batch_processed || 0).toLocaleString()}/${Number(scan_ai_batch_total || 0).toLocaleString()}` : '—',
        hitRate: null,
        latency: '—',
        notes: ai_enabled ? `${ai_provider || 'AI'}${ai_model ? ` · ${ai_model}` : ''}` : 'Disabled',
      },
    ];

    return (
      <div className={cn("scan-page rounded-2xl border border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-bg))] text-[hsl(var(--scan-text-primary))] overflow-hidden", className)}>
        <div className="space-y-5 p-4 md:p-5 xl:p-6">
          <section className="rounded-xl border border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-surface))] px-4 py-4 md:px-5 lg:sticky lg:top-[74px] lg:z-20">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
              <div className="min-w-0 flex-1 space-y-2">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="inline-flex items-center gap-2 rounded-full bg-success/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.08em] text-success">
                    <StatusDot state={status === 'paused' ? 'paused' : status === 'running' ? 'running' : 'idle'} />
                    {status}
                  </span>
                  <span className="inline-flex items-center rounded-full bg-info/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.08em] text-info">
                    Step {presentation.pipeline.currentIndex}/{presentation.pipeline.total}
                  </span>
                  <span
                    className={cn(
                      'inline-flex items-center rounded-full px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.08em]',
                    presentation.etaConfidence === 'high'
                        ? 'bg-success/10 text-success'
                        : presentation.etaConfidence === 'medium'
                          ? 'bg-info/10 text-info'
                          : 'bg-warning/10 text-warning',
                    )}
                  >
                    ETA {presentation.etaConfidence}
                  </span>
                </div>
                <div className="min-w-0">
                  <h2 className="truncate text-lg font-semibold leading-7 text-[hsl(var(--scan-text-primary))] md:text-xl">{presentation.currentStageLabel}</h2>
                  {currentArtist ? (
                    <p className="mt-1 truncate text-sm text-[hsl(var(--scan-text-secondary))]">Artist: {currentArtist}</p>
                  ) : null}
                </div>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                {scanning && status === 'running' ? (
                  <Button variant="outline" onClick={onPause} disabled={isPausing} className="gap-2 border-[hsl(var(--scan-border))] bg-transparent text-[hsl(var(--scan-text-primary))] hover:bg-[hsl(var(--scan-surface-raised))]">
                    {isPausing ? <Loader2 className="h-4 w-4 animate-spin" /> : <Pause className="h-4 w-4" />}
                    Pause
                  </Button>
                ) : null}
                {status === 'paused' ? (
                  <Button onClick={onResume} disabled={isResuming} className="gap-2 bg-[hsl(var(--scan-accent))] text-[hsl(var(--scan-text-primary))] hover:bg-[hsl(var(--scan-accent))]/90">
                    {isResuming ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                    Resume
                  </Button>
                ) : null}
                {(scanning || status === 'paused') ? (
                  <Button variant="destructive" onClick={onStop} disabled={isStopping} className="gap-2">
                    {isStopping ? <Loader2 className="h-4 w-4 animate-spin" /> : <Square className="h-4 w-4" />}
                    Stop
                  </Button>
                ) : null}
                <Link
                  to="/statistics"
                  className="inline-flex h-10 items-center gap-1.5 rounded-md border border-[hsl(var(--scan-border))] px-3 text-xs text-[hsl(var(--scan-text-secondary))] transition-colors hover:bg-[hsl(var(--scan-surface-raised))] hover:text-[hsl(var(--scan-text-primary))]"
                >
                  <BarChart3 className="h-3.5 w-3.5" />
                  Advanced statistics
                </Link>
              </div>
            </div>
          </section>

          <section className="rounded-xl border-l-[3px] border-[hsl(var(--scan-border))] border-l-[hsl(var(--scan-accent))] bg-[linear-gradient(90deg,rgba(59,130,246,0.08),rgba(59,130,246,0.03))] px-4 py-3 text-sm text-[hsl(var(--scan-text-secondary))]">
            {presentation.humanExplanation}
          </section>

          <section className="rounded-xl border border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-surface))] p-4 md:p-5">
            <div className="space-y-3">
              <div className="flex flex-wrap items-center gap-1.5">
                {presentation.pipeline.steps.map((step) => (
                  <Tooltip key={step.key}>
                    <TooltipTrigger asChild>
                      <button
                        type="button"
                        className={cn(
                          'flex h-8 min-w-8 flex-1 items-center justify-center rounded-md border text-[11px] font-semibold tabular-nums transition-colors',
                          step.state === 'done'
                            ? 'border-success/25 bg-success/15 text-success'
                            : step.state === 'active'
                              ? 'border-info/40 bg-info/15 text-foreground'
                              : 'border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-bg))] text-[hsl(var(--scan-text-tertiary))]',
                        )}
                      >
                        {step.index}
                      </button>
                    </TooltipTrigger>
                    <TooltipContent sideOffset={8}>{step.label}</TooltipContent>
                  </Tooltip>
                ))}
              </div>
              <div className="text-sm font-medium text-[hsl(var(--scan-text-primary))]">
                Step {presentation.pipeline.currentIndex} — {presentation.pipeline.currentLabel}
              </div>
            </div>
          </section>

          <section className="grid gap-4 xl:grid-cols-[minmax(0,1.4fr)_minmax(320px,0.7fr)]">
            <div className="rounded-xl border border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-surface))] p-4 md:p-5">
              <div className="space-y-4">
                <div className="space-y-2">
                  <div className="flex items-center justify-between gap-3 text-sm">
                    <span className="font-medium text-[hsl(var(--scan-text-primary))]">Pipeline progress</span>
                    <span className="tabular-nums text-[hsl(var(--scan-text-secondary))]">{pipelinePercent.toFixed(1)}%</span>
                  </div>
                  <div className="h-2 rounded-full bg-[hsl(var(--scan-border))] overflow-hidden">
                    <div className="h-full rounded-full bg-[hsl(var(--scan-accent))] transition-all duration-700 ease-out" style={{ width: `${pipelinePercent}%` }} />
                  </div>
                  <div className="text-xs text-[hsl(var(--scan-text-secondary))]">{presentation.pipelineProgressLabel}</div>
                </div>

                <div className="space-y-2">
                  <div className="flex items-center justify-between gap-3 text-sm">
                    <span className="font-medium text-[hsl(var(--scan-text-primary))]">Current stage progress</span>
                    <span className="tabular-nums text-[hsl(var(--scan-text-secondary))]">{presentation.currentStagePercentLabel}</span>
                  </div>
                  <div className="h-2 rounded-full bg-[hsl(var(--scan-border))] overflow-hidden">
                    <div className="h-full rounded-full bg-[hsl(var(--scan-accent))]/80 transition-all duration-700 ease-out" style={{ width: `${stagePercentCompact}%` }} />
                  </div>
                  <div className="text-xs text-[hsl(var(--scan-text-secondary))]">{presentation.stageHeroLabel}</div>
                </div>
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-3 xl:grid-cols-1">
              <ScanStatCell
                icon={<TimerReset className="h-3.5 w-3.5" />}
                label="Elapsed"
                value={formatDuration(elapsedSeconds)}
                tone="time"
                sparkline={historySamples.elapsed}
              />
              <ScanStatCell
                icon={<Gauge className="h-3.5 w-3.5" />}
                label="ETA"
                value={etaSecondsValue != null ? formatDuration(etaSecondsValue) : 'Calculating…'}
                tone="time"
                description={presentation.etaConfidence === 'low' ? 'Low confidence' : presentation.etaConfidence === 'medium' ? 'Stabilizing' : 'Stable estimate'}
                sparkline={historySamples.eta}
              />
              <ScanStatCell
                icon={<Clock className="h-3.5 w-3.5" />}
                label="Estimated total"
                value={estimatedTotalSeconds != null ? formatDuration(estimatedTotalSeconds) : '—'}
                tone="time"
                sparkline={historySamples.estimatedTotal}
              />
            </div>
          </section>

          <section className="grid grid-cols-2 gap-2 rounded-xl border border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-border))] p-px sm:grid-cols-3 xl:grid-cols-9">
            <ScanStatCell icon={<Music className="h-3.5 w-3.5" />} label="Albums processed" value={presentation.albumsProcessed.toLocaleString()} tone="content" />
            <ScanStatCell icon={<FolderInput className="h-3.5 w-3.5" />} label="Artists processed" value={presentation.artistsProcessed.toLocaleString()} tone="content" />
            <ScanStatCell icon={<Activity className="h-3.5 w-3.5" />} label="Active artists" value={presentation.activeArtistsCount.toLocaleString()} tone="content" />
            <ScanStatCell icon={<Zap className="h-3.5 w-3.5" />} label="Albums/hour" value={albumsPerHourLabel} tone="throughput" sparkline={historySamples.albumsPerHour} />
            <ScanStatCell icon={<Cpu className="h-3.5 w-3.5" />} label="Artists/hour" value={artistsPerHourLabel} tone="throughput" sparkline={historySamples.artistsPerHour} />
            <ScanStatCell icon={<Tag className="h-3.5 w-3.5" />} label="Matches" value={presentation.matchesSoFar.toLocaleString()} tone="output" />
            <ScanStatCell icon={<Package className="h-3.5 w-3.5" />} label="Library exports" value={presentation.exportsSoFar.toLocaleString()} tone="output" />
            <ScanStatCell icon={<AlertTriangle className="h-3.5 w-3.5" />} label="Incomplete" value={presentation.incompleteAlbumsSoFar.toLocaleString()} tone="issue" />
            <ScanStatCell icon={<Trash2 className="h-3.5 w-3.5" />} label="Duplicate losers" value={presentation.duplicateLosersSoFar.toLocaleString()} tone="issue" />
          </section>

          <section className="grid gap-5 xl:grid-cols-[minmax(0,1.2fr)_minmax(360px,0.95fr)]">
            <div className="space-y-4">
              <div className="rounded-xl border border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-surface))] p-4 md:p-5">
                <div className="flex items-center gap-2 text-[11px] font-medium uppercase tracking-[0.16em] text-[hsl(var(--scan-text-secondary))]">
                  {currentFocusMode === 'album' ? <Music className="h-3.5 w-3.5" /> : currentFocusMode === 'artist' ? <Activity className="h-3.5 w-3.5" /> : <Cpu className="h-3.5 w-3.5" />}
                  Current focus
                </div>
                <div className="mt-3 rounded-lg border border-[hsl(var(--scan-accent)/0.2)] bg-[hsl(var(--scan-accent)/0.08)] px-4 py-4">
                  <div className="truncate text-base font-semibold text-[hsl(var(--scan-text-primary))]">{currentFocusTitle}</div>
                  <div className="mt-1 text-sm text-[hsl(var(--scan-text-secondary))]">{currentFocusSubtitle || 'Waiting for next task…'}</div>
                  {currentFocusDetail ? (
                    <div className="mt-3 rounded-md border border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-bg))] px-3 py-2 text-xs text-[hsl(var(--scan-text-secondary))]">
                      {currentFocusDetail}
                    </div>
                  ) : null}
                </div>
              </div>

              <div className="rounded-xl border border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-surface))] p-4 md:p-5">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.16em] text-[hsl(var(--scan-text-secondary))]">Recent activity</div>
                  <div className="text-[11px] text-[hsl(var(--scan-text-tertiary))]">Last {Math.min(18, recentActivity.length)} entries</div>
                </div>
                <div className="mt-3 max-h-[360px] overflow-y-auto">
                  {recentActivity.length === 0 ? (
                    <div className="flex min-h-[120px] items-center justify-center text-sm text-[hsl(var(--scan-text-tertiary))]">No activity yet</div>
                  ) : (
                    <ul className="space-y-2">
                      {recentActivity.map((line, idx) => {
                        const type = inferActivityType(line);
                        return (
                          <li key={`${idx}-${line.slice(0, 24)}`} className="flex items-start gap-3 rounded-md px-2 py-1.5 hover:bg-[hsl(var(--scan-surface-raised))]">
                            <span className="min-w-[56px] pt-0.5 text-[11px] tabular-nums text-[hsl(var(--scan-text-tertiary))]">recent</span>
                            <span className={cn('inline-flex shrink-0 rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.08em]', activityBadgeClass(type))}>
                              {type}
                            </span>
                            <span className="min-w-0 truncate text-[13px] text-[hsl(var(--scan-text-secondary))]" title={line}>{line}</span>
                          </li>
                        );
                      })}
                    </ul>
                  )}
                </div>
              </div>

              <Collapsible open={compactLogsOpen} onOpenChange={setCompactLogsOpen}>
                <div className="rounded-xl border border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-surface))]">
                  <div className="flex items-center justify-between gap-3 px-4 py-3">
                    <div className="text-[11px] font-medium uppercase tracking-[0.16em] text-[hsl(var(--scan-text-secondary))]">Backend logs</div>
                    <div className="flex items-center gap-2">
                      <Button
                        type="button"
                        variant="ghost"
                        size="sm"
                        className="h-8 px-2 text-[11px] text-[hsl(var(--scan-text-secondary))] hover:text-[hsl(var(--scan-text-primary))]"
                        onClick={() => { window.location.href = '/api/logs/download?lines=50000'; }}
                      >
                        <Download className="mr-1 h-3.5 w-3.5" />
                        Download
                      </Button>
                      <CollapsibleTrigger asChild>
                        <Button type="button" variant="ghost" size="icon" className="h-8 w-8 text-[hsl(var(--scan-text-secondary))] hover:text-[hsl(var(--scan-text-primary))]">
                          {compactLogsOpen ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                        </Button>
                      </CollapsibleTrigger>
                    </div>
                  </div>
                  <CollapsibleContent>
                    <div className="max-h-72 overflow-y-auto px-4 pb-4">
                      <BackendLogPanel
                        path={liveLogPath}
                        entries={liveLogEntries}
                        lines={liveLogLines.slice(-16)}
                        maxLines={16}
                        className="border-0 rounded-lg"
                      />
                    </div>
                  </CollapsibleContent>
                </div>
              </Collapsible>
            </div>

            <div className="space-y-4">
              <div className="rounded-xl border border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-surface))]">
                <div className="border-b border-[hsl(var(--scan-border-subtle))] px-4 py-3">
                  <div className="text-[11px] font-medium uppercase tracking-[0.16em] text-[hsl(var(--scan-text-secondary))]">System health</div>
                </div>
                <SystemHealthTable rows={systemHealthRows} />
              </div>

              <Collapsible open={compactPipelineOpen} onOpenChange={setCompactPipelineOpen}>
                <div className="rounded-xl border border-[hsl(var(--scan-border))] bg-[hsl(var(--scan-surface))]">
                  <div className="flex items-center justify-between gap-3 px-4 py-3">
                    <div className="flex items-center gap-2">
                      <div className="text-sm font-medium text-[hsl(var(--scan-text-primary))]">Scan pipeline</div>
                      {pipelineTooltip ? (
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <button type="button" className="text-[hsl(var(--scan-text-tertiary))] hover:text-[hsl(var(--scan-text-primary))]">
                              <Info className="h-3.5 w-3.5" />
                            </button>
                          </TooltipTrigger>
                          <TooltipContent sideOffset={8} className="max-w-sm text-xs">
                            {pipelineTooltip}
                          </TooltipContent>
                        </Tooltip>
                      ) : null}
                    </div>
                    <CollapsibleTrigger asChild>
                      <button type="button" className="inline-flex items-center gap-2 text-sm text-[hsl(var(--scan-text-secondary))] hover:text-[hsl(var(--scan-text-primary))]">
                        <span>Step {presentation.pipeline.currentIndex} of {presentation.pipeline.total} active</span>
                        {compactPipelineOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                      </button>
                    </CollapsibleTrigger>
                  </div>
                  <CollapsibleContent>
                    <div className="space-y-1 px-3 pb-3">
                      {presentation.pipeline.steps.map((step) => (
                        <div
                          key={step.key}
                          className={cn(
                            'flex items-start gap-3 rounded-lg px-3 py-3',
                            step.state === 'active' ? 'bg-info/8' : step.state === 'done' ? 'bg-success/8' : 'bg-transparent',
                          )}
                        >
                          <StatusDot
                            state={step.state === 'done' ? 'success' : step.state === 'active' ? 'active' : 'idle'}
                            pulse={step.state === 'active'}
                            className="mt-1"
                          />
                          <div className="min-w-0 flex-1">
                            <div className="flex flex-wrap items-center justify-between gap-2">
                              <div className={cn('text-sm', step.state === 'active' ? 'font-semibold text-[hsl(var(--scan-text-primary))]' : 'font-medium text-[hsl(var(--scan-text-secondary))]')}>
                                {step.index}. {step.label}
                              </div>
                              <span className={cn(
                                'rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.08em]',
                                step.state === 'done'
                                  ? 'bg-success/10 text-success'
                                  : step.state === 'active'
                                    ? 'bg-info/10 text-info'
                                    : 'bg-[hsl(var(--scan-border))] text-[hsl(var(--scan-text-tertiary))]',
                              )}>
                                {step.state}
                              </span>
                            </div>
                            <div className="mt-1 text-xs leading-5 text-[hsl(var(--scan-text-secondary))]">{step.description}</div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </CollapsibleContent>
                </div>
              </Collapsible>
            </div>
          </section>
        </div>
      </div>
    );
  }

  return (
    <div className={cn("rounded-xl bg-card border border-border overflow-hidden", className)}>
      {/* ─── Idle: clean CTA ───────────────────────────────────────────────── */}
      {!scanning && status !== 'running' && (
        <div className="p-6 space-y-4">
          {/* Starting scan: single spinner until progress bar appears */}
          {waitingForProgress ? (
            <div className="flex flex-col items-center justify-center py-12 gap-4">
              <Loader2 className="w-10 h-10 animate-spin text-primary" />
              <p className="text-sm font-medium text-muted-foreground">Starting scan…</p>
            </div>
          ) : (
            <>
          {libraryUsable && (
            <div
              className={cn(
                "rounded-xl border p-4",
              background_enrichment_running
                  ? "border-primary/30 bg-primary/10"
                  : "border-success/30 bg-success/10"
              )}
            >
              <div className="flex items-start justify-between gap-3">
                <div className="space-y-1">
                  <p className="text-sm font-semibold text-foreground">
                    {background_enrichment_running ? 'Library ready' : 'Library ready'}
                  </p>
                  <p className="text-xs text-muted-foreground">
                    {background_enrichment_running
                      ? (
                          profileBackfillRunning && profileBackfillTotal > 0
                            ? `Background enrichment running · ${profileBackfillDone}/${profileBackfillTotal} artists`
                            : 'Background enrichment running'
                        )
                      : 'Background enrichment finished'}
                  </p>
                  {(profileBackfillArtist || backgroundJobSummary) && (
                    <p className="text-xs text-muted-foreground">
                      {[backgroundJobSummary, profileBackfillArtist].filter(Boolean).join(' · ')}
                    </p>
                  )}
                </div>
              </div>
            </div>
          )}
          {/* Last scan summary – 3-tier hierarchy */}
          {last_scan_summary && (
            <div className="space-y-6">
              {/* Show when fix-all is still running after scan so user sees process is not fully done */}
              {improveAllRunning && improveAllProgressData && (
                <div className="flex items-center gap-3 rounded-xl border-2 border-primary/40 bg-primary/10 p-4">
                  <Loader2 className="w-5 h-5 text-primary animate-spin shrink-0" />
                  <div>
                    <p className="font-medium text-foreground">Fixing tags and covers…</p>
                    <p className="text-sm text-muted-foreground">
                      {improveAllProgressData.current}/{improveAllProgressData.total} albums
                      {improveAllProgressData.current_artist && improveAllProgressData.current_album && (
                        <span> — {improveAllProgressData.current_artist} · {improveAllProgressData.current_album}</span>
                      )}
                    </p>
                  </div>
                </div>
              )}
              {/* Tier 1: Hero Metrics - Most important stats with mini charts */}
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                {/* Duplicates Found - with mini bar chart */}
                <Link 
                  to="/tools"
                  className="group flex flex-col gap-2 rounded-xl bg-gradient-to-br from-warning/10 to-warning/5 border border-warning/30 p-5 hover:border-warning/50 transition-all card-hover"
                >
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-semibold text-warning uppercase tracking-wider">Duplicates Found</span>
                    <Package className="w-4 h-4 text-warning" />
                  </div>
                  <div className="flex items-end justify-between gap-3">
                    <span className="text-4xl font-bold tabular-nums text-warning">
                      {last_scan_summary.duplicate_groups_count ?? 0}
                    </span>
                    {/* Mini bar chart showing duplicate ratio */}
                    {(last_scan_summary.albums_scanned ?? 0) > 0 && (
                      <div className="w-16 h-8 flex items-center">
                        <svg viewBox="0 0 64 24" className="w-full h-full">
                          <rect x="0" y="8" width="64" height="8" rx="4" className="fill-warning/20" />
                          <rect 
                            x="0" y="8" 
                            width={Math.max(4, Math.min(64, ((last_scan_summary.duplicate_groups_count ?? 0) / (last_scan_summary.albums_scanned ?? 1)) * 64 * 5))} 
                            height="8" rx="4" 
                            className="fill-warning transition-all" 
                          />
                        </svg>
                      </div>
                    )}
                  </div>
                  <span className="text-xs text-muted-foreground group-hover:text-foreground transition-colors flex items-center gap-1">
                    Review and undupe →
                  </span>
                </Link>

                {/* Albums Scanned - with mini donut chart */}
                <div className="flex flex-col gap-2 rounded-xl bg-gradient-to-br from-primary/10 to-primary/5 border border-border p-5">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Albums Scanned</span>
                    <Music className="w-4 h-4 text-primary" />
                  </div>
                  <div className="flex items-end justify-between gap-3">
                    <span className="text-4xl font-bold tabular-nums text-foreground">
                      {last_scan_summary.albums_scanned ?? 0}
                    </span>
                    {/* Mini donut showing lossless vs lossy ratio */}
                    {(last_scan_summary.albums_scanned ?? 0) > 0 && (
                      <div className="w-10 h-10 flex-shrink-0">
                        <svg viewBox="0 0 36 36" className="w-full h-full -rotate-90">
                          <circle 
                            cx="18" cy="18" r="12" 
                            fill="none" 
                            strokeWidth="5" 
                            className="stroke-muted/30" 
                          />
                          <circle 
                            cx="18" cy="18" r="12" 
                            fill="none" 
                            strokeWidth="5" 
                            strokeDasharray={`${((last_scan_summary.lossless_count ?? 0) / (last_scan_summary.albums_scanned ?? 1)) * 75.4} 75.4`}
                            strokeLinecap="round"
                            className="stroke-primary transition-all" 
                          />
                        </svg>
                      </div>
                    )}
                  </div>
                  <span className="text-xs text-muted-foreground">
                    {last_scan_summary.lossless_count ?? 0} lossless · {last_scan_summary.lossy_count ?? 0} lossy
                  </span>
                </div>

                {/* Scan Duration - with completion check */}
                <div className="flex flex-col gap-2 rounded-xl bg-gradient-to-br from-muted/50 to-muted/30 border border-border p-5">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Scan Duration</span>
                    <Clock className="w-4 h-4 text-muted-foreground" />
                  </div>
                  <div className="flex items-end justify-between gap-3">
                    <span className="text-4xl font-bold tabular-nums text-foreground">
                      {last_scan_summary.duration_seconds != null
                        ? last_scan_summary.duration_seconds >= 60
                          ? `${Math.floor(last_scan_summary.duration_seconds / 60)}m ${last_scan_summary.duration_seconds % 60}s`
                          : `${last_scan_summary.duration_seconds}s`
                        : '—'}
                    </span>
                    {/* Mini completion indicator */}
                    <div className="w-8 h-8 rounded-full bg-success/20 flex items-center justify-center flex-shrink-0">
                      <svg viewBox="0 0 24 24" className="w-5 h-5 text-success">
                        <path 
                          fill="none" 
                          stroke="currentColor" 
                          strokeWidth="2.5" 
                          strokeLinecap="round" 
                          strokeLinejoin="round" 
                          d="M5 13l4 4L19 7" 
                        />
                      </svg>
                    </div>
                  </div>
                  <span className="text-xs text-muted-foreground">
                    {last_scan_summary.artists_total ?? 0} artists processed
                  </span>
                </div>
              </div>

              {/* Tier 2: Key Stats - Secondary important info */}
              <div className="flex flex-wrap gap-2">
                {/* MusicBrainz match (during scan; run Fix Albums to write MBID to file tags) */}
                <div
                  className={cn(
                    "inline-flex items-center gap-2 px-3 py-2 rounded-lg border",
                    (last_scan_summary.mb_match?.matched ?? last_scan_summary.albums_with_mb_id ?? 0) > 0
                      ? "bg-success/10 border-success/30 text-success"
                      : "bg-muted/50 border-border text-muted-foreground"
                  )}
                  title="Matched during scan. Run Fix Albums to write MusicBrainz IDs to file tags on kept editions."
                >
                  <Database className="w-4 h-4" />
                  <span className="text-sm font-medium">
                    MusicBrainz {last_scan_summary.mb_match 
                      ? `${last_scan_summary.mb_match.matched}/${last_scan_summary.mb_match.total}`
                      : `${last_scan_summary.albums_with_mb_id ?? 0}/${(last_scan_summary.albums_with_mb_id ?? 0) + (last_scan_summary.albums_without_mb_id ?? 0)}`
                    }
                  </span>
                </div>

                {/* Incomplete albums */}
                {(last_scan_summary.broken_albums_count ?? 0) > 0 && (
                  <div className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border bg-destructive/10 border-destructive/30 text-destructive">
                    <AlertTriangle className="w-4 h-4" />
                    <span className="text-sm font-medium">
                      {last_scan_summary.broken_albums_count} incomplete
                    </span>
                  </div>
                )}

                {/* AI resolved */}
                {(last_scan_summary.ai_groups_count ?? 0) > 0 && (
                  <div className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border bg-primary/10 border-primary/30 text-primary">
                    <Sparkles className="w-4 h-4" />
                    <span className="text-sm font-medium">
                      {last_scan_summary.ai_groups_count} AI-resolved
                    </span>
                  </div>
                )}

                {/* MB verified by AI */}
                {(last_scan_summary.mb_verified_by_ai ?? 0) > 0 && (
                  <div className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border bg-info/10 border-info/30 text-info">
                    <Sparkles className="w-4 h-4" />
                    <span className="text-sm font-medium">
                      {last_scan_summary.mb_verified_by_ai} MB verified by AI
                    </span>
                  </div>
                )}

                {/* Dupes moved this scan */}
                {(last_scan_summary.dupes_moved_this_scan ?? 0) > 0 && (
                  <div className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border bg-success/10 border-success/30 text-success">
                    <Package className="w-4 h-4" />
                    <span className="text-sm font-medium">
                      {last_scan_summary.dupes_moved_this_scan} moved
                    </span>
                  </div>
                )}

                {/* Space saved */}
                {(last_scan_summary.space_saved_mb_this_scan ?? 0) > 0 && (
                  <div className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border bg-success/10 border-success/30 text-success">
                    <Trash2 className="w-4 h-4" />
                    <span className="text-sm font-medium">
                      {last_scan_summary.space_saved_mb_this_scan >= 1024
                        ? `${(last_scan_summary.space_saved_mb_this_scan / 1024).toFixed(1)} GB saved`
                        : `${last_scan_summary.space_saved_mb_this_scan} MB saved`}
                    </span>
                  </div>
                )}
              </div>

              {/* Tier 3: Collapsible Details - Less important info */}
              <Collapsible>
                <CollapsibleTrigger className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground transition-colors w-full justify-between group">
                  <span className="font-medium">Show more details</span>
                  <ChevronDown className="w-4 h-4 group-data-[state=open]:rotate-180 transition-transform" />
                </CollapsibleTrigger>
                <CollapsibleContent>
                  <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3 pt-4">
                    {/* Missing albums */}
                    {(last_scan_summary.missing_albums_count ?? 0) > 0 && (
                      <div className="flex flex-col gap-1 rounded-lg bg-muted/50 p-3 border border-border/50">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Missing</span>
                        <span className="text-lg font-bold tabular-nums text-foreground">{last_scan_summary.missing_albums_count ?? 0}</span>
                      </div>
                    )}
                    
                    {/* Incomplete tags */}
                    {(last_scan_summary.albums_without_complete_tags ?? 0) > 0 && (
                      <div className="flex flex-col gap-1 rounded-lg bg-muted/50 p-3 border border-border/50">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Incomplete tags</span>
                        <span className="text-lg font-bold tabular-nums text-foreground">{last_scan_summary.albums_without_complete_tags ?? 0}</span>
                      </div>
                    )}

                    {/* Discogs match - only show if > 0 */}
                    {last_scan_summary.discogs_match && last_scan_summary.discogs_match.matched > 0 && (
                      <div className="flex flex-col gap-1 rounded-lg bg-muted/50 p-3 border border-border/50">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Discogs Match</span>
                        <span className="text-lg font-bold tabular-nums text-foreground">
                          {last_scan_summary.discogs_match.matched}/{last_scan_summary.discogs_match.total}
                        </span>
                      </div>
                    )}

                    {/* Last.fm match - only show if > 0 */}
                    {last_scan_summary.lastfm_match && last_scan_summary.lastfm_match.matched > 0 && (
                      <div className="flex flex-col gap-1 rounded-lg bg-muted/50 p-3 border border-border/50">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Last.fm Match</span>
                        <span className="text-lg font-bold tabular-nums text-foreground">
                          {last_scan_summary.lastfm_match.matched}/{last_scan_summary.lastfm_match.total}
                        </span>
                      </div>
                    )}

                    {/* Bandcamp match - only show if > 0 */}
                    {last_scan_summary.bandcamp_match && last_scan_summary.bandcamp_match.matched > 0 && (
                      <div className="flex flex-col gap-1 rounded-lg bg-muted/50 p-3 border border-border/50">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Bandcamp Match</span>
                        <span className="text-lg font-bold tabular-nums text-foreground">
                          {last_scan_summary.bandcamp_match.matched}/{last_scan_summary.bandcamp_match.total}
                        </span>
                      </div>
                    )}

                    {/* Cache stats */}
                    {((last_scan_summary.audio_cache_hits ?? 0) > 0 || (last_scan_summary.audio_cache_misses ?? 0) > 0) && (
                      <div className="flex flex-col gap-1 rounded-lg bg-muted/50 p-3 border border-border/50 col-span-2">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Cache Performance</span>
                        <span className="text-sm font-medium tabular-nums text-foreground">
                          Audio: {last_scan_summary.audio_cache_hits ?? 0} cached, {last_scan_summary.audio_cache_misses ?? 0} fresh
                          {((last_scan_summary.mb_cache_hits ?? 0) > 0 || (last_scan_summary.mb_cache_misses ?? 0) > 0) && (
                            <span className="text-muted-foreground"> · MB: {last_scan_summary.mb_cache_hits ?? 0} cached, {last_scan_summary.mb_cache_misses ?? 0} fresh</span>
                          )}
                        </span>
                      </div>
                    )}
                  </div>
                </CollapsibleContent>
              </Collapsible>

              {/* AI Errors - always visible if present */}
              {last_scan_summary.ai_errors && last_scan_summary.ai_errors.length > 0 && (
                <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4 space-y-2">
                  <h5 className="text-sm font-medium text-destructive flex items-center gap-1.5">
                    <AlertTriangle className="w-4 h-4" />
                    AI errors ({last_scan_summary.ai_errors.length})
                  </h5>
                  <ul className="text-xs text-muted-foreground space-y-1 max-h-32 overflow-y-auto">
                    {last_scan_summary.ai_errors.map((err, i) => (
                      <li key={i} className="flex flex-col gap-0.5">
                        {err.group && <span className="font-medium text-foreground">{err.group}</span>}
                        <span>{err.message}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}
          {/* Post-scan workflow (sequential) */}
          {last_scan_summary && (
            <div className="space-y-4">
              <div className="rounded-xl border border-border bg-card p-4">
                <h3 className="text-base font-semibold text-foreground">Post-scan workflow</h3>
                <p className="text-sm text-muted-foreground mt-1">
                  Resolve actions in order: Undupe → Fix Albums → Incomplete handling.
                </p>
                <p className="text-xs text-muted-foreground mt-2">
                  Current step: <span className="font-medium text-foreground capitalize">{workflowStage}</span>
                </p>
              </div>

              <div className="grid grid-cols-1 gap-3">
                <div className="rounded-xl border border-border bg-card p-4">
                  <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
                    <div>
                      <p className="text-xs uppercase tracking-wider text-muted-foreground">Step 1</p>
                      <h4 className="text-sm font-semibold text-foreground">Undupe duplicates</h4>
                      <p className="text-xs text-muted-foreground mt-1">
                        {hasDuplicates
                          ? `${actualDuplicateCount} duplicate group(s) detected.`
                          : 'No duplicates detected in the last scan.'}
                      </p>
                    </div>
                    <div className="flex items-center gap-2">
                      <Link to="/tools">
                        <Button variant="outline" size="sm">Review</Button>
                      </Link>
                      <Button
                        size="sm"
                        onClick={async () => {
                          setPostScanRunning(true);
                          try {
                            await dedupeAll();
                            toast.success('Undupe started');
                          } catch {
                            toast.error('Failed to start undupe');
                          } finally {
                            setPostScanRunning(false);
                          }
                        }}
                        disabled={postScanRunning || deduping || !canDedupe}
                        className="gap-1.5"
                      >
                        {(postScanRunning || deduping) ? <Loader2 className="w-4 h-4 animate-spin" /> : <Trash2 className="w-4 h-4" />}
                        {canDedupe ? 'Run undupe' : 'Completed'}
                      </Button>
                    </div>
                  </div>
                </div>

                <div className="rounded-xl border border-border bg-card p-4">
                  <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
                    <div>
                      <p className="text-xs uppercase tracking-wider text-muted-foreground">Step 2</p>
                      <h4 className="text-sm font-semibold text-foreground">Fix albums metadata</h4>
                      <p className="text-xs text-muted-foreground mt-1">
                        Update tags, covers and artist images on kept editions.
                      </p>
                    </div>
                    <Button
                      size="sm"
                      onClick={async () => {
                        if (improveAllRunning) return;
                        setPostScanRunning(true);
                        try {
                          await improveAll();
                          toast.success('Fix all albums started');
                        } catch {
                          toast.error('Failed to start');
                        } finally {
                          setPostScanRunning(false);
                        }
                      }}
                      disabled={postScanRunning || improveAllRunning || canDedupe || deduping}
                      className="gap-1.5"
                    >
                      {improveAllRunning ? <Loader2 className="w-4 h-4 animate-spin" /> : <Tag className="w-4 h-4" />}
                      {improveAllRunning ? 'Fix in progress…' : 'Run fix'}
                    </Button>
                  </div>
                  {improveAllRunning && improveAllProgressData && (
                    <p className="text-xs text-muted-foreground mt-2">
                      {improveAllProgressData.current}/{improveAllProgressData.total}
                      {(improveAllProgressData.current_artist || improveAllProgressData.current_album) && (
                        <span> — {improveAllProgressData.current_artist} · {improveAllProgressData.current_album}</span>
                      )}
                    </p>
                  )}
                </div>

                <div className="rounded-xl border border-border bg-card p-4">
                  <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
                    <div>
                      <p className="text-xs uppercase tracking-wider text-muted-foreground">Step 3</p>
                      <h4 className="text-sm font-semibold text-foreground">Incomplete handling</h4>
                      <p className="text-xs text-muted-foreground mt-1">
                        {(scan_pipeline_flags?.incomplete_move ?? false)
                          ? `Auto-moved ${scan_incomplete_moved_count} incomplete album(s) (${scan_incomplete_moved_mb} MB) this run.`
                          : `${last_scan_summary.broken_albums_count ?? 0} incomplete album(s) detected.`}
                      </p>
                    </div>
                    <Link to="/broken-albums">
                      <Button size="sm" variant="outline" className="gap-1.5">
                        <AlertTriangle className="w-4 h-4" />
                        Review
                      </Button>
                    </Link>
                  </div>
                </div>

                {(scan_pipeline_flags?.player_sync ?? false) && (
                  <div className="rounded-xl border border-border bg-card p-4">
                    <div className="flex flex-col gap-1">
                      <p className="text-xs uppercase tracking-wider text-muted-foreground">Step 4</p>
                      <h4 className="text-sm font-semibold text-foreground">Player sync</h4>
                      <p className="text-xs text-muted-foreground">
                        Target: <span className="font-medium text-foreground">{scan_player_sync_target || scan_pipeline_sync_target || 'none'}</span>
                        {scan_player_sync_ok != null && (
                          <span className={cn('ml-2 font-medium', scan_player_sync_ok ? 'text-success' : 'text-destructive')}>
                            {scan_player_sync_ok ? 'OK' : 'Failed'}
                          </span>
                        )}
                      </p>
                      {scan_player_sync_message ? <p className="text-xs text-muted-foreground">{scan_player_sync_message}</p> : null}
                    </div>
                  </div>
                )}
              </div>

              <Collapsible>
                <CollapsibleTrigger className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground transition-colors w-full justify-between group">
                  <span className="font-medium">Advanced post-scan actions</span>
                  <ChevronDown className="w-4 h-4 group-data-[state=open]:rotate-180 transition-transform" />
                </CollapsibleTrigger>
                <CollapsibleContent className="pt-3">
                  <div className="rounded-xl border border-primary/30 bg-primary/5 p-4 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
                    <div>
                      <h4 className="text-sm font-semibold text-foreground">Run Magic</h4>
                      <p className="text-xs text-muted-foreground mt-1">
                        One-click sequence: dedupe first, then fix albums.
                      </p>
                    </div>
                    <Button
                      onClick={async () => {
                        if (magicRunning || deduping || improveAllRunning) return;
                        setMagicRunning(true);
                        try {
                          await dedupeAll();
                          const maxWaitMs = 600000;
                          const pollMs = 2000;
                          const start = Date.now();
                          while (Date.now() - start < maxWaitMs) {
                            const prog = await getDedupeProgress();
                            if (!prog.deduping) break;
                            await new Promise((r) => setTimeout(r, pollMs));
                          }
                          await improveAll();
                          toast.success('Magic started: dedupe done, fixing albums…');
                        } catch (e) {
                          toast.error(e instanceof Error ? e.message : 'Magic failed');
                        } finally {
                          setMagicRunning(false);
                        }
                      }}
                      disabled={magicRunning || deduping || improveAllRunning}
                      className="gap-1.5 shrink-0"
                    >
                      {(magicRunning || deduping) ? <Loader2 className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
                      {magicRunning || deduping ? 'Dedupe in progress…' : improveAllRunning ? 'Fix in progress…' : 'Run Magic'}
                    </Button>
                  </div>
                </CollapsibleContent>
              </Collapsible>
            </div>
          )}

          <div className="flex flex-col gap-3 pt-4 border-t border-border">
            <div className={cn(
              "rounded-md border px-3 py-2 text-xs",
              bootstrap_required
                ? "border-warning/40 bg-warning/10 text-warning"
                : autonomous_mode && has_completed_full_scan
                  ? "border-success/30 bg-success/10 text-success"
                  : "border-border bg-muted/40 text-muted-foreground"
            )}>
              {bootstrap_required
                ? "Initial full scan required. Changed-only is blocked until first full completes."
                : autonomous_mode && has_completed_full_scan
                  ? "Bootstrap complete. Default scan switched to changed only (you can still force full)."
                  : "Default scan mode is still manual."}
              <span className="ml-1 text-muted-foreground">
                Current backend default: {String(default_scan_type || 'full').replace('_', ' ')}.
              </span>
            </div>
            <div className="flex flex-wrap items-center gap-4">
	              <div className="flex items-center gap-2">
	                <span className="text-sm font-medium text-foreground">Scan type</span>
	                <select
	                  className="rounded-md border border-input bg-background px-2 py-1.5 text-sm"
	                  value={scanType}
	                  onChange={(e) => setScanType(e.target.value as ScanType)}
	                  disabled={scanning}
	                >
	                  <option value="full">Full scan (duplicates + incomplete)</option>
	                  <option value="changed_only" disabled={changedOnlyBlocked}>Changed only (new/modified albums)</option>
	                  <option value="incomplete_only">Incomplete albums only</option>
	                </select>
	              </div>
	            </div>
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
            <div>
              <h3 className="text-base font-semibold text-foreground">Ready to scan</h3>
              <p className="text-sm text-muted-foreground mt-1">
                {scanType === 'incomplete_only'
                  ? 'Scan only for incomplete albums (missing tracks). Results appear in Incomplete albums.'
                  : scanType === 'changed_only'
                    ? (bootstrap_required
                        ? 'Changed-only requires one successful initial full scan first.'
                        : 'Scan only new/modified albums. Unchanged and already-complete albums are skipped.')
                    : 'One scan analyzes duplicates, metadata, and tags. Results appear in Tools, Library, and Statistics.'}
              </p>
            </div>
            <div className="flex items-center gap-2 shrink-0">
              {onClear && (
                <AlertDialog open={showClearDialog} onOpenChange={setShowClearDialog}>
                  <AlertDialogTrigger asChild>
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={isClearing}
                      className="gap-1.5 text-muted-foreground hover:text-destructive"
                    >
                      {isClearing ? <Loader2 className="w-4 h-4 animate-spin" /> : <Trash2 className="w-4 h-4" />}
                      Clear results
                    </Button>
                  </AlertDialogTrigger>
                  <AlertDialogContent>
                    <AlertDialogHeader>
                      <AlertDialogTitle>Clear Scan Results</AlertDialogTitle>
                      <AlertDialogDescription>
                        This removes duplicate detection results. You will need a new scan to detect duplicates again. This cannot be undone.
                      </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                      <AlertDialogCancel>Cancel</AlertDialogCancel>
                      <AlertDialogAction
                        onClick={async () => {
                          try {
                            await onClear();
                            setShowClearDialog(false);
                            toast.success('Scan results cleared');
                          } catch {
                            toast.error('Failed to clear');
                          }
                        }}
                        className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                      >
                        Clear
                      </AlertDialogAction>
                    </AlertDialogFooter>
                  </AlertDialogContent>
                </AlertDialog>
              )}
              {preflightResult != null && (
                <div className="w-full sm:w-auto rounded-lg border border-border bg-muted/40 p-3 space-y-2 text-xs">
                  <div className="font-medium text-foreground">Pre-flight check</div>
                  <div className={cn("flex items-center gap-2", preflightResult.musicbrainz.ok ? "text-success" : "text-destructive")}>
                    <Database className="w-4 h-4 shrink-0" />
                    {preflightResult.musicbrainz.ok ? "MusicBrainz: OK" : `MusicBrainz: ${preflightResult.musicbrainz.message || "Error"}`}
                  </div>
                  <div className={cn("flex items-center gap-2", preflightResult.ai.ok ? "text-success" : "text-destructive")}>
                    <Sparkles className="w-4 h-4 shrink-0" />
                    <ProviderBadge provider={preflightResult.ai.provider} className="h-5 px-2 py-0 text-[10px]" />
                    <span>{preflightResult.ai.ok ? 'OK' : (preflightResult.ai.message || 'Error')}</span>
                  </div>
                  {preflightResult.discogs != null && (
                    <div className={cn("flex items-center gap-2", preflightResult.discogs.ok ? "text-success" : "text-muted-foreground")}>
                      <Database className="w-4 h-4 shrink-0" />
                      {preflightResult.discogs.ok ? "Discogs: OK" : `Discogs: ${preflightResult.discogs.message || "—"}`}
                    </div>
                  )}
                  {preflightResult.lastfm != null && (
                    <div className={cn("flex items-center gap-2", preflightResult.lastfm.ok ? "text-success" : "text-muted-foreground")}>
                      <Database className="w-4 h-4 shrink-0" />
                      {preflightResult.lastfm.ok ? "Last.fm: OK" : `Last.fm: ${preflightResult.lastfm.message || "—"}`}
                    </div>
                  )}
                  {preflightResult.bandcamp != null && (
                    <div className={cn("flex items-center gap-2", preflightResult.bandcamp.ok ? "text-success" : "text-muted-foreground")}>
                      <Database className="w-4 h-4 shrink-0" />
                      {preflightResult.bandcamp.ok ? "Bandcamp: OK" : `Bandcamp: ${preflightResult.bandcamp.message || "—"}`}
                    </div>
                  )}
                  {preflightResult.serper != null && (
                    <div className={cn("flex items-center gap-2", preflightResult.serper.ok ? "text-success" : "text-muted-foreground")}>
                      <Database className="w-4 h-4 shrink-0" />
                      {preflightResult.serper.ok ? "Serper: OK" : `Serper: ${preflightResult.serper.message || "—"}`}
                    </div>
                  )}
                  {preflightResult.acoustid != null && (
                    <div className={cn("flex items-center gap-2", preflightResult.acoustid.ok ? "text-success" : "text-muted-foreground")}>
                      <Database className="w-4 h-4 shrink-0" />
                      {preflightResult.acoustid.ok ? "AcousticID: OK" : `AcousticID: ${preflightResult.acoustid.message || "—"}`}
                    </div>
                  )}
	                  {!preflightResult.musicbrainz.ok && preflightResult.ai.ok && (
	                    <Button size="sm" variant="secondary" className="w-full mt-1" onClick={() => { setPreflightVerifiedAtStart(false); onStart({ scan_type: scanType }); setPreflightResult(null); }} disabled={isStarting}>
	                      {isStarting ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
	                      {resumableAvailable ? 'Resume anyway' : 'Start scan anyway'}
	                    </Button>
	                  )}
                </div>
              )}
              <Button
                size="default"
                disabled={isStarting || preflightLoading}
                className="gap-2"
	                onClick={async () => {
	                  if (preflightLoading || isStarting) return;
	                  const startOptions = { scan_type: scanType };
	                  if (scanType === 'incomplete_only') {
	                    setWaitingForProgress(true);
	                    onStart(startOptions);
	                    return;
                  }
                  setWaitingForProgress(true);
                  setPreflightLoading(true);
                  setPreflightResult(null);
                  try {
                    const res = await getScanPreflight();
                    setPreflightResult(res);
                    if (res.musicbrainz.ok && res.ai.ok) {
                      setPreflightVerifiedAtStart(true);
                      setPreflightPaths(res.paths ?? null);
                      onStart(startOptions);
                    } else {
                      setWaitingForProgress(false);
                      if (!res.ai.ok) {
                        toast.error("Configure the AI provider in Settings to run a scan");
                      }
                    }
                  } catch (e) {
                    setWaitingForProgress(false);
                    toast.error("Pre-flight check failed");
                    setPreflightResult({
                      musicbrainz: { ok: false, message: String(e) },
                      ai: { ok: false, message: String(e), provider: "AI" },
                      discogs: { ok: false, message: "—" },
                      lastfm: { ok: false, message: "—" },
                      bandcamp: { ok: false, message: "—" },
                      serper: { ok: false, message: "—" },
                      acoustid: { ok: false, message: "—" },
                    });
                  } finally {
                    setPreflightLoading(false);
                  }
                }}
              >
                {preflightLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                {startButtonLabel}
              </Button>
            </div>
            {resumableAvailable && (
              <div className="rounded-lg border border-primary/30 bg-primary/10 px-3 py-2 text-xs text-primary/90">
                Interrupted <span className="font-medium text-foreground">{scanType === 'changed_only' ? 'changed-only' : 'full'}</span> scan available.
                <span className="ml-1">{resumableSummary}.</span>
                <span className="ml-1 text-muted-foreground">You can change AI settings now; resume continues from the remaining work only.</span>
              </div>
            )}
          </div>
          </div>
          </> )}
        </div>
      )}

      {/* ─── Running / Paused: status + controls ───────────────────────────── */}
      {scanning && (
        <>
          <div className="px-4 py-3 flex items-center justify-between border-b border-border bg-muted/30">
            <div className="flex items-center gap-3">
              <span className={cn(
                "inline-flex items-center gap-2 text-sm font-medium capitalize",
                status === 'running' && "text-success",
                status === 'paused' && "text-warning",
                status === 'stopped' && "text-muted-foreground"
              )}>
                {status === 'running' && (
                  <span className="relative flex h-2 w-2">
                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-success opacity-75" />
                    <span className="relative inline-flex rounded-full h-2 w-2 bg-success" />
                  </span>
                )}
                {status === 'paused' && <span className="h-2 w-2 rounded-full bg-warning" />}
                {status}
              </span>
              {threads_in_use != null && threads_in_use > 0 && (
                <span className="text-xs text-muted-foreground flex items-center gap-1">
                  <Cpu className="w-3.5 h-3.5" />
                  {threads_in_use} thread{threads_in_use !== 1 ? 's' : ''}
                </span>
              )}
            </div>
            <div className="flex items-center gap-2">
              {status === 'running' && (
                <Button size="sm" variant="secondary" onClick={onPause} disabled={isPausing} className="gap-1.5">
                  {isPausing ? <Loader2 className="w-4 h-4 animate-spin" /> : <Pause className="w-4 h-4" />}
                  Pause
                </Button>
              )}
              {status === 'paused' && (
                <Button size="sm" onClick={onResume} disabled={isResuming} className="gap-1.5">
                  {isResuming ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                  Resume
                </Button>
              )}
              <Button size="sm" variant="destructive" onClick={onStop} disabled={isStopping} className="gap-1.5">
                {isStopping ? <Loader2 className="w-4 h-4 animate-spin" /> : <Square className="w-4 h-4" />}
                Stop
              </Button>
            </div>
          </div>

          <div className="p-4 space-y-4">
            {status === 'paused' && (
              <div className="rounded-lg border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning">
                Scan paused. You can change AI settings now; resume reloads the current provider/model and continues from the remaining work only.
                <span className="ml-1 text-muted-foreground">{pausedResumeSummary}</span>
              </div>
            )}
            {(scanning || finalizing || deduping || post_processing) && pipeline.steps.length > 0 && (
              <div className="space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-xs font-medium uppercase tracking-[0.22em] text-muted-foreground">
                    Scan pipeline
                  </div>
                  <div className="rounded-full border border-primary/20 bg-primary/10 px-2.5 py-1 text-[11px] font-medium text-primary">
                    Step {pipeline.currentIndex}/{pipeline.total} · {pipeline.currentLabel}
                  </div>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  {pipeline.steps.map((step, idx) => (
                    <div key={step.key} className="contents">
                      {idx > 0 ? <span className="text-muted-foreground/50">→</span> : null}
                      <span
                        className={cn(
                          'flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium transition-colors',
                          step.state === 'active'
                            ? 'bg-primary/15 text-primary ring-1 ring-primary/30'
                            : step.state === 'done'
                              ? 'bg-success/12 text-success ring-1 ring-success/20'
                              : 'bg-muted/60 text-muted-foreground'
                        )}
                      >
                        {step.state === 'active' ? <Loader2 className="h-3 w-3 animate-spin shrink-0" /> : null}
                        {step.index}/{step.total} {step.label}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
            {/* ─── Progress hero: bar + % + ETA ─────────────────────────────── */}
            <div className="space-y-2">
              <div className="flex items-end justify-between gap-4">
                <div className="flex items-baseline gap-2 min-w-0">
                  <span className="text-2xl font-bold tabular-nums text-foreground">{stageHeadline}</span>
                  <span className="text-sm text-muted-foreground shrink-0">
                    Step {pipeline.currentIndex}/{pipeline.total} · {phaseLabel} · {stageSummaryLabel}
                  </span>
                </div>
                <div className="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-xs shrink-0">
                  <Link
                    to="/statistics"
                    className="inline-flex items-center gap-1 text-primary hover:underline underline-offset-2"
                    title="Open live scan statistics"
                  >
                    <BarChart3 className="w-3.5 h-3.5" />
                    Live stats
                  </Link>
                  <span className="inline-flex items-center gap-1 text-muted-foreground tabular-nums">
                    <Clock className="w-3.5 h-3.5" />
                    Elapsed {formatDuration(elapsedSeconds)}
                  </span>
                  <span className={cn("tabular-nums", etaSecondsValue != null ? "text-primary font-medium" : "text-muted-foreground")}>
                    ETA {etaSecondsValue != null ? formatDuration(etaSecondsValue) : 'calculating…'}
                  </span>
                  <span className="text-muted-foreground tabular-nums">
                    Est. total {estimatedTotalSeconds != null ? formatDuration(estimatedTotalSeconds) : '—'}
                  </span>
                </div>
              </div>
              {stageIndeterminate ? (
                <div className="progress-track h-3 overflow-hidden">
                  <div className="h-full w-1/3 animate-pulse rounded-full bg-primary/80" />
                </div>
              ) : (
                <div className="progress-track h-3">
                  <div
                    className="progress-fill h-full rounded-full transition-all duration-300 ease-out"
                    style={{ width: `${visibleStagePercentageExact}%` }}
                  />
                </div>
              )}
              <div className="flex items-center justify-between gap-3 text-[11px] text-muted-foreground">
                {stageIndeterminate ? (
                  <>
                    <span>
                      {preScanIndeterminate
                        ? 'PMDA is still estimating this step from live discovery counters; the total is not reliable yet.'
                        : runScopeIndeterminate
                          ? 'PMDA is still estimating the effective run scope before locking the true total.'
                          : stageTransitioning
                      ? 'This step is complete; PMDA is handing off to the next pipeline phase.'
                            : 'Current stage progress is not countable; waiting for the final tail to drain.'}
                    </span>
                    <span className="tabular-nums">run {pipelineOverallPercentageExact.toFixed(1)}%</span>
                  </>
                ) : (
                  <>
                    <span>
                      Overall pipeline · {pipelineOverallDoneSteps.toFixed(1)}/{pipelineOverallTotalSteps || 0} steps
                    </span>
                    <span className="tabular-nums">run {pipelineOverallPercentageExact.toFixed(1)}%</span>
                  </>
                )}
              </div>
              {(preScanActive || runScopePreparing) && (
                <p className="text-[11px] text-muted-foreground">
                  {runScopePreparing
                    ? (runScopeIndeterminate
                      ? 'Preparing run scope: PMDA is comparing signatures and estimating the exact remaining scope before it can show a stable percentage.'
                      : 'Preparing run scope: signatures + resume comparison are running. Live counters below.')
                    : preScanCatchupActive
                      ? 'Library rehydration: PMDA is rebuilding the visible published index from the resumed run before artist workers continue. Counters below are real artist progress.'
                    : preScanSnapshotActive
                      ? 'Pre-scan cache snapshot: PMDA is persisting the resumed album map so the run can continue safely and show consistent progress.'
                      : preScanIndeterminate
                        ? 'Pre-scan discovery is active, but the final scope is not known yet. PMDA shows live counters instead of a fake percentage.'
                        : 'Pre-scan phase: depending on library size and album count this can be long. Patience, see logs for details.'}
                </p>
              )}
            </div>

            {scanning && (
              runScopePreparing ? (
                <div className="rounded-lg border border-border bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
                  <span className="font-medium text-foreground/80">Preparing run scope</span>
                  <span className="mx-1">•</span>
                  stage {runScopeStage || 'signatures'}
                  <span className="mx-1">•</span>
                  {runScopeProgressTotal > 0
                    ? `${runScopeProgressDone.toLocaleString()}/${runScopeProgressTotal.toLocaleString()} artists`
                    : `${runScopeProgressDone.toLocaleString()} artists`}
                  <span className="mx-1">•</span>
                  in scope {scan_run_scope_artists_included.toLocaleString()} artists
                  <span className="mx-1">•</span>
                  {scan_run_scope_albums_included.toLocaleString()} albums
                </div>
              ) : preScanActive ? (
                <div className="rounded-lg border border-border bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
                  <span className="font-medium text-foreground/80">Pre-scan phase</span>
                  <span className="mx-1">•</span>
                  stage {preScanStage || 'filesystem'}
                  <span className="mx-1">•</span>
                  {preScanIndeterminate
                    ? `estimating scope · entries ${scan_discovery_entries_scanned.toLocaleString()}`
                    : preScanStage === 'album_candidates' && preScanAlbumsTotal > 0
                    ? `albums ${preScanAlbumsDone.toLocaleString()}/${preScanAlbumsTotal.toLocaleString()}`
                    : preScanRootsTotal > 0
                      ? `roots ${preScanRootsDone.toLocaleString()}/${preScanRootsTotal.toLocaleString()}`
                      : 'discovering album candidates'}
                  <span className="mx-1">•</span>
                  found {scan_discovery_albums_found.toLocaleString()}
                  <span className="mx-1">•</span>
                  artists {scan_discovery_artists_found.toLocaleString()}
                </div>
              ) : (
                <div className="rounded-lg border border-border bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
                  <span className="font-medium text-foreground/80">Run scope</span>
                  <span className="mx-1">•</span>
                  {artists_total.toLocaleString()} artists
                  <span className="mx-1">•</span>
                  {total_albums.toLocaleString()} albums
                  {(detected_artists_total > 0 || detected_albums_total > 0) && (
                    <>
                      <span className="mx-1">|</span>
                      <span className="font-medium text-foreground/80">Detected source</span>
                      <span className="mx-1">•</span>
                      {detected_artists_total.toLocaleString()} artists
                      <span className="mx-1">•</span>
                      {detected_albums_total.toLocaleString()} albums
                    </>
                  )}
                  {(resume_skipped_artists > 0 || resume_skipped_albums > 0) && (
                    <>
                      <span className="mx-1">|</span>
                      <span className="font-medium text-foreground/80">Resume skipped</span>
                      <span className="mx-1">•</span>
                      {resume_skipped_artists.toLocaleString()} artists
                      <span className="mx-1">•</span>
                      {resume_skipped_albums.toLocaleString()} albums
                    </>
                  )}
                </div>
              )
            )}

            {showDiscovery && (
              <div className="rounded-lg border border-border bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
                <span className="font-medium text-foreground/80">Discovery</span>
                {scan_discovery_stage ? (
                  <>
                    <span className="mx-1">•</span>
                    stage {scan_discovery_stage}
                  </>
                ) : null}
                <span className="mx-1">•</span>
                {scan_discovery_files_found.toLocaleString()} files
                <span className="mx-1">•</span>
                {scan_discovery_folders_found.toLocaleString()} folders
                <span className="mx-1">•</span>
                {scan_discovery_albums_found.toLocaleString()} albums
                <span className="mx-1">•</span>
                {scan_discovery_artists_found.toLocaleString()} artists
                {(scan_discovery_albums_total > 0 || scan_discovery_folders_total > 0) && (
                  <>
                    <span className="mx-1">|</span>
                    candidates {(scan_discovery_albums_done || scan_discovery_folders_done).toLocaleString()}/{(scan_discovery_albums_total || scan_discovery_folders_total).toLocaleString()}
                  </>
                )}
                {scan_discovery_roots_total > 0 && (
                  <>
                    <span className="mx-1">•</span>
                    roots {scan_discovery_roots_done}/{scan_discovery_roots_total}
                  </>
                )}
                {scan_discovery_entries_scanned > 0 && (
                  <>
                    <span className="mx-1">•</span>
                    visited {scan_discovery_entries_scanned.toLocaleString()}
                  </>
                )}
                {scan_discovery_current_root ? (
                  <div className="mt-1 truncate font-mono text-[11px]" title={scan_discovery_current_root}>
                    {scan_discovery_running ? 'Scanning root: ' : 'Last root: '}
                    {scan_discovery_current_root}
                  </div>
                ) : null}
              </div>
            )}

            {/* ─── Current step (visible when running, finalizing, moving dupes, or post-processing) ─────────── */}
            {(hasActiveStep || finalizing || deduping || post_processing || (effectiveStage === 'ia_analysis' && scanning)) && (
              <div className="flex items-center gap-2 px-3 py-2.5 rounded-lg border-l-4 border-primary/80 bg-primary/5">
                {finalizing ? (
                  <>
                    <Loader2 className="w-4 h-4 text-primary shrink-0 mt-0.5 animate-spin" />
                    <div className="min-w-0 flex-1 space-y-1.5">
                      <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Now</span>
                      <div className="text-sm font-medium text-foreground">
                        Finalizing… Saving results to Tools and summary.
                      </div>
                    </div>
                  </>
                ) : deduping ? (
                  <>
                    <Loader2 className="w-4 h-4 text-primary shrink-0 mt-0.5 animate-spin" />
                    <div className="min-w-0 flex-1 space-y-1.5">
                      <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Now</span>
                      <div className="text-sm font-medium text-foreground">
                        Moving dupes… {dedupe_total > 0 ? `(${dedupe_progress}/${dedupe_total})` : ''}
                      </div>
                    </div>
                  </>
                ) : post_processing ? (
                  <>
                    <Loader2 className="w-4 h-4 text-primary shrink-0 mt-0.5 animate-spin" />
                    <div className="min-w-0 flex-1 space-y-1.5">
                      <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Now</span>
                      <div className="text-sm font-medium text-foreground truncate">
                        {post_processing_current_artist && post_processing_current_album
                          ? `Fixing: ${post_processing_current_artist} — ${post_processing_current_album}`
                          : 'Fixing metadata / covers…'}
                      </div>
                      {post_processing_total > 0 && (
                        <div className="text-xs text-muted-foreground tabular-nums">
                          {post_processing_done}/{post_processing_total} album(s)
                        </div>
                      )}
                    </div>
                  </>
                ) : effectiveStage === 'ia_analysis' && scanning ? (
                  <>
                    <Loader2 className="w-4 h-4 text-primary shrink-0 mt-0.5 animate-spin" />
                    <div className="min-w-0 flex-1 space-y-1.5">
                      <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Now</span>
                      <div className="text-sm font-medium text-foreground truncate">
                        {scan_ai_current_label ? `Analyzing: ${scan_ai_current_label}` : 'Analyzing duplicate groups…'}
                      </div>
                      {scan_ai_batch_total > 0 && (
                        <div className="text-xs text-muted-foreground tabular-nums">
                          {scan_ai_batch_processed}/{scan_ai_batch_total} groups
                        </div>
                      )}
                    </div>
                  </>
                ) : (
                  <>
                    <Music className="w-4 h-4 text-primary shrink-0 mt-0.5" />
                    <div className="min-w-0 flex-1 space-y-1.5">
                      <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Now</span>
                      <div className="text-sm font-medium text-foreground truncate">
                        {active_artists[0].artist_name} — &quot;{active_artists[0].current_album!.album_title}&quot; — {currentStepLabel}
                      </div>
                      {active_artists[0].current_album!.step_summary && (
                        <div className="text-xs text-muted-foreground font-mono break-words" title={active_artists[0].current_album!.step_summary}>
                          Step: {active_artists[0].current_album!.step_summary}
                        </div>
                      )}
                      {(active_artists[0].current_album!.step_response || active_artists[0].current_album!.step_summary) && (
                        <div className="text-xs font-mono break-words border-l-2 border-primary/40 pl-2 text-foreground/90" title={active_artists[0].current_album!.step_response || active_artists[0].current_album!.step_summary}>
                          <span className="text-muted-foreground font-medium">Response: </span>
                          {active_artists[0].current_album!.step_response || active_artists[0].current_album!.step_summary}
                        </div>
                      )}
                    </div>
                  </>
                )}
              </div>
            )}

            {/* ─── Activity log: per-artist summary lines (latest first) ──────── */}
            {Array.isArray(scan_steps_log) && scan_steps_log.length > 0 && (
              <div className="space-y-1.5">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">
                    Activity log
                  </span>
                  <span className="text-[10px] text-muted-foreground">
                    Showing last {Math.min(8, scan_steps_log.length)} entr{scan_steps_log.length === 1 ? 'y' : 'ies'}
                  </span>
                </div>
                <div className="rounded-lg border border-border bg-muted/40 h-40 overflow-hidden">
                  <ul className="h-full px-3 py-2 space-y-0.5 text-[11px] leading-4 font-mono text-muted-foreground/90">
                    {scan_steps_log.slice(-8).map((line, idx) => (
                      <li key={idx} className="truncate" title={line}>
                        {line}
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
            )}

            {/* ─── Live backend logs (power users) ───────────────────────────── */}
            {(scanning || post_processing) && (
              <div className="space-y-1.5">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wide flex items-center gap-1.5">
                    <Terminal className="w-3.5 h-3.5" />
                    Live backend log
                  </span>
                  <div className="flex items-center gap-2">
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="h-6 px-2 text-[11px]"
                      onClick={() => { window.location.href = '/api/logs/download?lines=50000'; }}
                    >
                      <Download className="w-3.5 h-3.5 mr-1" />
                      Download
                    </Button>
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="h-6 px-2 text-[11px]"
                      onClick={() => setShowRawLogs((v) => !v)}
                    >
                      {showRawLogs ? 'Hide' : 'Show'}
                    </Button>
                  </div>
                </div>
                {showRawLogs && (
                  <div ref={rawLogViewportRef} className="max-h-56 overflow-y-auto rounded-lg border border-border bg-transparent text-[11px] font-mono">
                    <BackendLogPanel
                      path={liveLogPath}
                      entries={liveLogEntries}
                      lines={liveLogLines}
                      maxLines={180}
                      className="border-0 rounded-none"
                    />
                  </div>
                )}
              </div>
            )}

            {/* ─── Services verified at start (when preflight passed) ─────────── */}
            {scanning && preflightVerifiedAtStart && (
              <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
                <span className="font-medium text-foreground/80 w-full sm:w-auto">Services verified at start:</span>
                <span className={cn("inline-flex items-center gap-1.5", (preflightResult?.musicbrainz?.ok ?? true) ? "text-success" : "text-destructive")}>
                  <Database className="w-3.5 h-3.5" />
                  MusicBrainz {(preflightResult?.musicbrainz?.ok ?? true) ? "✓" : "✗"}
                </span>
                <span className={cn("inline-flex items-center gap-1.5", (preflightResult?.ai?.ok ?? true) ? "text-success" : "text-destructive")}>
                  <Sparkles className="w-3.5 h-3.5" />
                  AI {(preflightResult?.ai?.ok ?? true) ? "✓" : "✗"}
                </span>
                {preflightResult?.discogs != null && (
                  <span className={cn("inline-flex items-center gap-1.5", preflightResult.discogs.ok ? "text-success" : "text-muted-foreground")} title={preflightResult.discogs.message ?? undefined}>
                    <Database className="w-3.5 h-3.5" />
                    Discogs {preflightResult.discogs.ok ? "✓" : "—"}
                  </span>
                )}
                {preflightResult?.lastfm != null && (
                  <span className={cn("inline-flex items-center gap-1.5", preflightResult.lastfm.ok ? "text-success" : "text-muted-foreground")} title={preflightResult.lastfm.message ?? undefined}>
                    <Database className="w-3.5 h-3.5" />
                    Last.fm {preflightResult.lastfm.ok ? "✓" : "—"}
                  </span>
                )}
                {preflightResult?.bandcamp != null && (
                  <span className={cn("inline-flex items-center gap-1.5", preflightResult.bandcamp.ok ? "text-success" : "text-muted-foreground")} title={preflightResult.bandcamp.message ?? undefined}>
                    <Database className="w-3.5 h-3.5" />
                    Bandcamp {preflightResult.bandcamp.ok ? "✓" : "—"}
                  </span>
                )}
                {preflightResult?.serper != null && (
                  <span className={cn("inline-flex items-center gap-1.5", preflightResult.serper.ok ? "text-success" : "text-muted-foreground")} title={preflightResult.serper.message ?? undefined}>
                    <Database className="w-3.5 h-3.5" />
                    Serper {preflightResult.serper.ok ? "✓" : "—"}
                  </span>
                )}
                {preflightResult?.acoustid != null && (
                  <span className={cn("inline-flex items-center gap-1.5", preflightResult.acoustid.ok ? "text-success" : "text-muted-foreground")} title={preflightResult.acoustid.message ?? undefined}>
                    <Database className="w-3.5 h-3.5" />
                    AcousticID {preflightResult.acoustid.ok ? "✓" : "—"}
                  </span>
                )}
                {(preflightPaths ?? progressPathsStatus) && (
                  <>
                    <span
                      className={cn(
                        "inline-flex items-center gap-1.5",
                        (preflightPaths?.music_rw ?? progressPathsStatus?.music_rw) ? "text-success" : "text-destructive"
                      )}
                      title="Music folder(s) must be read-write for scan and move"
                    >
                      <FolderInput className="w-3.5 h-3.5" />
                      Music folder: {(preflightPaths?.music_rw ?? progressPathsStatus?.music_rw) ? "RW ✓" : "RW ✗"}
                    </span>
                    <span
                      className={cn(
                        "inline-flex items-center gap-1.5",
                        (preflightPaths?.dupes_rw ?? progressPathsStatus?.dupes_rw) ? "text-success" : "text-destructive"
                      )}
                      title="Dupes folder must be read-write to move duplicates"
                    >
                      <FolderInput className="w-3.5 h-3.5" />
                      Dupes folder: {(preflightPaths?.dupes_rw ?? progressPathsStatus?.dupes_rw) ? "RW ✓" : "RW ✗"}
                    </span>
                  </>
                )}
              </div>
            )}

            {/* ─── Details (collapsible) ────────────────────────────────────── */}
            <Collapsible open={expanded} onOpenChange={setExpanded}>
              <CollapsibleTrigger asChild>
                <Button variant="ghost" size="sm" className="w-full justify-between h-8 text-xs text-muted-foreground hover:text-foreground">
                  <span>Stats &amp; details</span>
                  {expanded ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
                </Button>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <div className="pt-3 mt-2 border-t border-border space-y-3 text-xs">
                  {artists_total > 0 && (
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Artists</span>
                      <span className="font-medium tabular-nums">{artists_processed} / {artists_total}</span>
                    </div>
                  )}
                  <div className="flex justify-between items-center">
                    <span className="text-muted-foreground flex items-center gap-1.5">
                      <Sparkles className="w-3.5 h-3.5" />
                      AI (LLM)
                    </span>
                    {ai_enabled ? (
                      <span className="text-success font-medium inline-flex items-center gap-1.5">
                        On {ai_used_count > 0 && ` · ${ai_used_count} groups`}
                        {ai_provider ? <ProviderBadge provider={ai_provider} className="h-5 px-2 py-0 text-[10px]" /> : null}
                        {ai_model ? <span>· {ai_model}</span> : null}
                      </span>
                    ) : (
                      <span className="text-muted-foreground">Off (Signature)</span>
                    )}
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-muted-foreground flex items-center gap-1.5">
                      <Database className="w-3.5 h-3.5" />
                      MusicBrainz
                    </span>
                    {mb_enabled ? (
                      <span className="text-success font-medium">
                        On {mb_used_count > 0 && ` · ${mb_used_count} enriched`}
                        {mb_cache_hits > 0 && ` · ${mb_cache_hits} cached`}
                      </span>
                    ) : (
                      <span className="text-muted-foreground">Off</span>
                    )}
                  </div>
                  {(audio_cache_hits > 0 || audio_cache_misses > 0) && (
                    <div className="flex justify-between">
                      <span className="text-muted-foreground flex items-center gap-1.5">
                        <Zap className="w-3.5 h-3.5" />
                        Audio cache
                      </span>
                      <span className="font-medium tabular-nums">{audio_cache_hits} hits / {audio_cache_misses} misses</span>
                    </div>
                  )}
                  {(duplicate_groups_count > 0 || total_duplicates_count > 0 || broken_albums_count > 0 || missing_albums_count > 0 ||
                    albums_without_mb_id > 0 || albums_without_artist_mb_id > 0 || albums_without_complete_tags > 0 ||
                    albums_without_album_image > 0 || albums_without_artist_image > 0) && (
                    <div className="pt-2 border-t border-border space-y-1.5">
                      <div className="text-muted-foreground font-medium mb-1">Findings so far</div>
                      {(() => {
                        const nOfM = (n: number, tot: number) => (tot > 0 ? `${n.toLocaleString()} / ${tot.toLocaleString()}` : n.toLocaleString());
                        const tot = total_albums ?? 0;
                        return (
                          <>
                            {duplicate_groups_count > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Package className="w-3 h-3 text-warning" /> Duplicate groups</span>
                                <span className="font-medium">{duplicate_groups_count}</span>
                              </div>
                            )}
                            {total_duplicates_count > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Music className="w-3 h-3 text-destructive" /> Total duplicates</span>
                                <span className="font-medium">{total_duplicates_count}</span>
                              </div>
                            )}
                            {broken_albums_count > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><AlertTriangle className="w-3 h-3 text-destructive" /> Incomplete albums</span>
                                <span className="font-medium text-destructive">{nOfM(broken_albums_count, tot)}</span>
                              </div>
                            )}
                            {missing_albums_count > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Music className="w-3 h-3 text-warning" /> Missing</span>
                                <span className="font-medium">{nOfM(missing_albums_count, tot)}</span>
                              </div>
                            )}
                            {albums_without_mb_id > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Database className="w-3 h-3 text-info" /> No MB ID</span>
                                <span className="font-medium">{nOfM(albums_without_mb_id, tot)}</span>
                              </div>
                            )}
                            {albums_without_complete_tags > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Tag className="w-3 h-3 text-primary" /> Incomplete tags</span>
                                <span className="font-medium">{nOfM(albums_without_complete_tags, tot)}</span>
                              </div>
                            )}
                            {albums_without_album_image > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Image className="w-3 h-3 text-muted-foreground" /> No album art</span>
                                <span className="font-medium">{nOfM(albums_without_album_image, tot)}</span>
                              </div>
                            )}
                            {albums_without_artist_image > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Image className="w-3 h-3 text-muted-foreground" /> No artist art</span>
                                <span className="font-medium">{nOfM(albums_without_artist_image, tot)}</span>
                              </div>
                            )}
                          </>
                        );
                      })()}
                    </div>
                  )}
                </div>
              </CollapsibleContent>
            </Collapsible>
          </div>
        </>
      )}
    </div>
  );
}
