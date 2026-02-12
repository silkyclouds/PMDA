import { useState, useRef, useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { Play, Pause, Square, RefreshCw, Loader2, ChevronDown, ChevronUp, Sparkles, Database, Music, Cpu, Zap, AlertTriangle, Image, Tag, Package, Trash2, Clock, FolderInput, Terminal } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from '@/components/ui/alert-dialog';
import { cn } from '@/lib/utils';
import type { ScanProgress as ScanProgressType } from '@/lib/api';
import {
  getScanPreflight,
  type ScanPreflightResult,
  dedupeAll,
  improveAll,
  getDedupeProgress,
  getImproveAllProgress,
  getScanLogsTail,
} from '@/lib/api';
import { toast } from 'sonner';

function formatETA(seconds: number | undefined): string {
  if (!seconds || seconds <= 0) return '…';
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.round(seconds % 60);
  const parts: string[] = [];
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
  /** Called when user starts a scan. Options: scan_type (full | changed_only | incomplete_only), run_improve_after (except incomplete_only). */
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
  /** After scan: automatically run improve-all (tags + covers). Not used for incomplete_only. */
  runImproveAfter?: boolean;
  /** Callbacks to update scan options from the component (optional; if not provided, options are internal state) */
  onScanTypeChange?: (t: ScanType) => void;
  onRunImproveAfterChange?: (v: boolean) => void;
  /** Compact mode for simplified Scan page UI */
  compact?: boolean;
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
  runImproveAfter: runImproveAfterProp,
  onScanTypeChange,
  onRunImproveAfterChange,
  compact,
}: ScanProgressProps) {
  const [expanded, setExpanded] = useState(false);
  const [showClearDialog, setShowClearDialog] = useState(false);
  const [preflightLoading, setPreflightLoading] = useState(false);
  const [preflightResult, setPreflightResult] = useState<ScanPreflightResult | null>(null);
  const [preflightVerifiedAtStart, setPreflightVerifiedAtStart] = useState(false);
  const [preflightPaths, setPreflightPaths] = useState<{ music_rw: boolean; dupes_rw: boolean } | null>(null);
  const [waitingForProgress, setWaitingForProgress] = useState(false);
  const [liveLogLines, setLiveLogLines] = useState<string[]>([]);
  const [liveLogPath, setLiveLogPath] = useState('');
  const [showRawLogs, setShowRawLogs] = useState(false);
  const hasToastedAiErrors = useRef(false);
  const [scanTypeInternal, setScanTypeInternal] = useState<ScanType>('full');
  const [runImproveAfterInternal, setRunImproveAfterInternal] = useState(false);
  const scanType = scanTypeProp ?? scanTypeInternal;
  const setScanType = onScanTypeChange ?? setScanTypeInternal;
  const runImproveAfter = runImproveAfterProp ?? runImproveAfterInternal;
  const setRunImproveAfter = onRunImproveAfterChange ?? setRunImproveAfterInternal;
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
    scan_pipeline_flags = {},
    scan_pipeline_sync_target = null,
    scan_incomplete_moved_count = 0,
    scan_incomplete_moved_mb = 0,
    scan_player_sync_target = null,
    scan_player_sync_ok = null,
    scan_player_sync_message = null,
  } = safeProgress;

  // Stage badge: use backend phase (format_analysis | identification_tags | ia_analysis | finalizing | moving_dupes | post_processing)
  const effectiveStage = phase ?? (post_processing ? 'post_processing' : (finalizing ? 'finalizing' : (deduping ? 'moving_dupes' : 'format_analysis')));
  // Progress bar must reflect real end-to-end work.
  // During scan, include post-processing when present, otherwise use artist progress.
  const hasPostWork = scanning && (post_processing || post_processing_total > 0);
  const scanUnitsDone = Math.max(0, Math.min(artists_processed, artists_total));
  const scanUnitsTotal = Math.max(0, artists_total);
  const postUnitsDone = Math.max(0, Math.min(post_processing_done, post_processing_total));
  const postUnitsTotal = Math.max(0, post_processing_total);
  const compositeDone = scanUnitsDone + postUnitsDone;
  const compositeTotal = scanUnitsTotal + postUnitsTotal;
  const displayProgress = hasPostWork
    ? compositeDone
    : (scanning && artists_total > 0 ? artists_processed : current);
  const displayTotal = hasPostWork
    ? compositeTotal
    : (scanning && artists_total > 0 ? artists_total : total);
  const percentageExact = displayTotal > 0 ? Math.min(100, (displayProgress / displayTotal) * 100) : 0;
  // Never show 100% while backend still reports scanning=true.
  const percentage = scanning ? Math.min(99, Math.floor(percentageExact)) : Math.round(percentageExact);

  // Toast once when scan finishes with AI errors
  useEffect(() => {
    if (scanning) hasToastedAiErrors.current = false;
  }, [scanning]);
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
    if (artists_total > 0 || total > 0) {
      setWaitingForProgress(false);
    }
  }, [waitingForProgress, scanning, artists_total, total]);

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
    if (isCompact) return;
    if (!scanning && !post_processing) return;
    let cancelled = false;
    const tick = async () => {
      try {
        const data = await getScanLogsTail(220);
        if (!cancelled) {
          setLiveLogLines(Array.isArray(data?.lines) ? data.lines : []);
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
  }, [scanning, post_processing]);

  const hasActiveStep = scanning && active_artists.length > 0 && active_artists[0]?.current_album && active_artists[0].current_album.status !== 'done';
  const currentArtist = active_artists?.[0]?.artist_name || '';
  const improveAllRunning = improveAllProgressData?.running ?? false;
  
  // Use real-time duplicate count if available, fallback to last_scan_summary
  const actualDuplicateCount = currentDuplicateCount ?? (last_scan_summary?.duplicate_groups_count ?? 0);
  const hasDuplicates = actualDuplicateCount > 0;
  const canDedupe = hasDuplicates && (last_scan_summary?.dupes_moved_this_scan ?? 0) < actualDuplicateCount;
  const dupesAlreadyAllMoved = (last_scan_summary?.duplicate_groups_count ?? 0) > 0 && (last_scan_summary?.dupes_moved_this_scan ?? 0) >= (last_scan_summary?.duplicate_groups_count ?? 0);
  const showPostProcessingStep = post_processing || (post_processing_total ?? 0) > 0;
  const showDiscovery =
    scanning &&
    (scan_discovery_running ||
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

  if (isCompact) {
    const statusLabel = scanning ? (phase ? `Scanning · ${phase}` : 'Scanning') : 'Idle';
    const progressLabel = scanning
      ? `${displayProgress}/${displayTotal} artist(s)`
      : 'Ready to scan';
    return (
      <div className={cn("rounded-xl bg-card border border-border overflow-hidden", className)}>
        <div className="p-6 space-y-4">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div className="space-y-1">
              <h2 className="text-lg font-semibold text-foreground">Scan status</h2>
              <p className="text-sm text-muted-foreground">{statusLabel}</p>
              {currentArtist && scanning && (
                <p className="text-xs text-muted-foreground">Artist: <span className="font-medium text-foreground">{currentArtist}</span></p>
              )}
            </div>
            <div className="flex flex-wrap gap-2">
              {!scanning && status !== 'running' && (
                <Button onClick={() => onStart({ scan_type: scanType, run_improve_after: runImproveAfter })} disabled={isStarting} className="gap-2">
                  {isStarting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                  Start scan
                </Button>
              )}
              {scanning && status === 'running' && (
                <Button variant="outline" onClick={onPause} disabled={isPausing} className="gap-2">
                  {isPausing ? <Loader2 className="w-4 h-4 animate-spin" /> : <Pause className="w-4 h-4" />}
                  Pause
                </Button>
              )}
              {status === 'paused' && (
                <Button onClick={onResume} disabled={isResuming} className="gap-2">
                  {isResuming ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                  Resume
                </Button>
              )}
              {(scanning || status === 'paused') && (
                <Button variant="destructive" onClick={onStop} disabled={isStopping} className="gap-2">
                  {isStopping ? <Loader2 className="w-4 h-4 animate-spin" /> : <Square className="w-4 h-4" />}
                  Stop
                </Button>
              )}
            </div>
          </div>
          <div className="space-y-2">
            <div className="h-2 rounded-full bg-muted overflow-hidden">
              <div className="h-full bg-primary transition-all" style={{ width: `${percentage}%` }} />
            </div>
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>{progressLabel}</span>
              <span className="tabular-nums">{percentage}%</span>
            </div>
          </div>
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
              >
                Changed only
              </Button>
            </div>
            <label className="flex items-center gap-2">
              <Checkbox
                checked={runImproveAfter}
                onCheckedChange={(checked) => setRunImproveAfter(Boolean(checked))}
              />
              <span>Fix after scan</span>
            </label>
          </div>
          <div className="text-xs text-muted-foreground">
            Detailed stats live in <Link to="/statistics" className="underline underline-offset-2">Statistics</Link>.
          </div>
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
                  to="/unduper"
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
                      <Link to="/unduper">
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
                          <span className={cn('ml-2 font-medium', scan_player_sync_ok ? 'text-emerald-500' : 'text-red-500')}>
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
                  <option value="changed_only">Changed only (new/modified albums)</option>
                  <option value="incomplete_only">Incomplete albums only</option>
                </select>
              </div>
              {scanType !== 'incomplete_only' && (
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <Checkbox
                    checked={runImproveAfter}
                    onCheckedChange={(v) => setRunImproveAfter(v === true)}
                    disabled={scanning}
                  />
                <span className="text-muted-foreground">After scan: correct tags and covers</span>
                </label>
              )}
            </div>
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
            <div>
              <h3 className="text-base font-semibold text-foreground">Ready to scan</h3>
              <p className="text-sm text-muted-foreground mt-1">
                {scanType === 'incomplete_only'
                  ? 'Scan only for incomplete albums (missing tracks). Results appear in Incomplete albums.'
                  : scanType === 'changed_only'
                    ? 'Scan only new/modified albums. Unchanged and already-complete albums are skipped.'
                  : 'One scan analyzes duplicates, metadata, and tags. Results appear in Unduper, Library, and Tag Fixer.'}
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
                  <div className={cn("flex items-center gap-2", preflightResult.musicbrainz.ok ? "text-green-600 dark:text-green-400" : "text-destructive")}>
                    <Database className="w-4 h-4 shrink-0" />
                    {preflightResult.musicbrainz.ok ? "MusicBrainz: OK" : `MusicBrainz: ${preflightResult.musicbrainz.message || "Error"}`}
                  </div>
                  <div className={cn("flex items-center gap-2", preflightResult.ai.ok ? "text-green-600 dark:text-green-400" : "text-destructive")}>
                    <Sparkles className="w-4 h-4 shrink-0" />
                    {preflightResult.ai.ok ? `${preflightResult.ai.provider}: OK` : `${preflightResult.ai.provider}: ${preflightResult.ai.message || "Error"}`}
                  </div>
                  {preflightResult.discogs != null && (
                    <div className={cn("flex items-center gap-2", preflightResult.discogs.ok ? "text-green-600 dark:text-green-400" : "text-muted-foreground")}>
                      <Database className="w-4 h-4 shrink-0" />
                      {preflightResult.discogs.ok ? "Discogs: OK" : `Discogs: ${preflightResult.discogs.message || "—"}`}
                    </div>
                  )}
                  {preflightResult.lastfm != null && (
                    <div className={cn("flex items-center gap-2", preflightResult.lastfm.ok ? "text-green-600 dark:text-green-400" : "text-muted-foreground")}>
                      <Database className="w-4 h-4 shrink-0" />
                      {preflightResult.lastfm.ok ? "Last.fm: OK" : `Last.fm: ${preflightResult.lastfm.message || "—"}`}
                    </div>
                  )}
                  {preflightResult.bandcamp != null && (
                    <div className={cn("flex items-center gap-2", preflightResult.bandcamp.ok ? "text-green-600 dark:text-green-400" : "text-muted-foreground")}>
                      <Database className="w-4 h-4 shrink-0" />
                      {preflightResult.bandcamp.ok ? "Bandcamp: OK" : `Bandcamp: ${preflightResult.bandcamp.message || "—"}`}
                    </div>
                  )}
                  {preflightResult.serper != null && (
                    <div className={cn("flex items-center gap-2", preflightResult.serper.ok ? "text-green-600 dark:text-green-400" : "text-muted-foreground")}>
                      <Database className="w-4 h-4 shrink-0" />
                      {preflightResult.serper.ok ? "Serper: OK" : `Serper: ${preflightResult.serper.message || "—"}`}
                    </div>
                  )}
                  {preflightResult.acoustid != null && (
                    <div className={cn("flex items-center gap-2", preflightResult.acoustid.ok ? "text-green-600 dark:text-green-400" : "text-muted-foreground")}>
                      <Database className="w-4 h-4 shrink-0" />
                      {preflightResult.acoustid.ok ? "AcousticID: OK" : `AcousticID: ${preflightResult.acoustid.message || "—"}`}
                    </div>
                  )}
                  {!preflightResult.musicbrainz.ok && preflightResult.ai.ok && (
                    <Button size="sm" variant="secondary" className="w-full mt-1" onClick={() => { setPreflightVerifiedAtStart(false); onStart({ scan_type: scanType, run_improve_after: scanType !== 'incomplete_only' ? runImproveAfter : false }); setPreflightResult(null); }} disabled={isStarting}>
                      {isStarting ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
                      Start scan anyway
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
                  const startOptions = { scan_type: scanType, run_improve_after: scanType !== 'incomplete_only' ? runImproveAfter : false };
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
                Start scan
              </Button>
            </div>
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
            {/* ─── Progress hero: bar + % + ETA ─────────────────────────────── */}
            <div className="space-y-2">
              <div className="flex items-end justify-between gap-4">
                <div className="flex items-baseline gap-2 min-w-0">
                  <span className="text-2xl font-bold tabular-nums text-foreground">{percentage}%</span>
                  <span className="text-sm text-muted-foreground shrink-0">
                    {displayTotal > 0
                      ? hasPostWork
                        ? `${scanUnitsDone.toLocaleString()} / ${scanUnitsTotal.toLocaleString()} artists · ${postUnitsDone.toLocaleString()} / ${postUnitsTotal.toLocaleString()} post`
                        : scanning && artists_total > 0
                          ? `${displayProgress.toLocaleString()} / ${displayTotal.toLocaleString()} artists`
                          : `${displayProgress.toLocaleString()} / ${displayTotal.toLocaleString()} steps`
                      : '—'}
                  </span>
                </div>
                {eta_seconds != null && eta_seconds > 0 && (
                  <div className="flex items-center gap-1.5 text-sm font-medium text-primary shrink-0">
                    <Clock className="w-4 h-4" />
                    ~{formatETA(eta_seconds)} left
                  </div>
                )}
                {scanning && (eta_seconds == null || eta_seconds <= 0) && (artists_total > 0 || total > 0) && (
                  <span className="text-xs text-muted-foreground shrink-0">ETA calculating…</span>
                )}
              </div>
              <div className="progress-track h-3">
                <div
                  className="progress-fill h-full rounded-full transition-all duration-300 ease-out"
                  style={{ width: `${percentageExact}%` }}
                />
              </div>
            </div>

            {scanning && (
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
            )}

            {showDiscovery && (
              <div className="rounded-lg border border-border bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
                <span className="font-medium text-foreground/80">Discovery</span>
                <span className="mx-1">•</span>
                {scan_discovery_files_found.toLocaleString()} files
                <span className="mx-1">•</span>
                {scan_discovery_folders_found.toLocaleString()} folders
                <span className="mx-1">•</span>
                {scan_discovery_albums_found.toLocaleString()} albums
                <span className="mx-1">•</span>
                {scan_discovery_artists_found.toLocaleString()} artists
                {scan_discovery_roots_total > 0 && (
                  <>
                    <span className="mx-1">•</span>
                    roots {scan_discovery_roots_done}/{scan_discovery_roots_total}
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

            {/* ─── Phase stepper (compact): highlight stage from backend phase ─── */}
            {(phase || auto_move_enabled || showPostProcessingStep) && (
              <div className="flex flex-wrap items-center gap-2 text-xs">
                <span className={cn(
                  "flex items-center gap-1.5 px-2.5 py-1 rounded-full font-medium",
                  effectiveStage === 'format_analysis' ? "bg-primary/15 text-primary ring-1 ring-primary/30" : "bg-muted/60 text-muted-foreground"
                )}>
                  {effectiveStage === 'format_analysis' && scanning && !deduping && <Loader2 className="w-3 h-3 animate-spin shrink-0" />}
                  1. Format analysis
                </span>
                <span className="text-muted-foreground/60">→</span>
                <span className={cn(
                  "flex items-center gap-1.5 px-2.5 py-1 rounded-full font-medium",
                  effectiveStage === 'identification_tags' ? "bg-primary/15 text-primary ring-1 ring-primary/30" : "bg-muted/60 text-muted-foreground"
                )}>
                  {effectiveStage === 'identification_tags' && scanning && <Loader2 className="w-3 h-3 animate-spin shrink-0" />}
                  2. Identification / Tags
                </span>
                <span className="text-muted-foreground/60">→</span>
                <span className={cn(
                  "px-2.5 py-1 rounded-full font-medium",
                  effectiveStage === 'ia_analysis' ? "bg-primary/15 text-primary ring-1 ring-primary/30" : "bg-muted/60 text-muted-foreground"
                )}>
                  {effectiveStage === 'ia_analysis' && scanning && <Loader2 className="w-3 h-3 animate-spin shrink-0" />}
                  3. IA analysis
                </span>
                <span className="text-muted-foreground/60">→</span>
                <span className={cn(
                  "flex items-center gap-1.5 px-2.5 py-1 rounded-full font-medium",
                  effectiveStage === 'finalizing' ? "bg-primary/15 text-primary ring-1 ring-primary/30" : "bg-muted/60 text-muted-foreground"
                )}>
                  {effectiveStage === 'finalizing' && <Loader2 className="w-3 h-3 animate-spin shrink-0" />}
                  4. Finalizing
                </span>
                {auto_move_enabled && (
                  <>
                    <span className="text-muted-foreground/60">→</span>
                    <span className={cn(
                      "flex items-center gap-1.5 px-2.5 py-1 rounded-full font-medium",
                      effectiveStage === 'moving_dupes' ? "bg-primary/15 text-primary ring-1 ring-primary/30" : "bg-muted/60 text-muted-foreground"
                    )}>
                      {effectiveStage === 'moving_dupes' && deduping && <Loader2 className="w-3 h-3 animate-spin shrink-0" />}
                      5. Moving dupes
                      {deduping && dedupe_total > 0 && (
                        <span className="tabular-nums">
                          ({dedupe_progress}/{dedupe_total})
                        </span>
                      )}
                    </span>
                  </>
                )}
                {showPostProcessingStep && (
                  <>
                    <span className="text-muted-foreground/60">→</span>
                    <span className={cn(
                      "flex items-center gap-1.5 px-2.5 py-1 rounded-full font-medium",
                      effectiveStage === 'post_processing' ? "bg-primary/15 text-primary ring-1 ring-primary/30" : "bg-muted/60 text-muted-foreground"
                    )}>
                      {effectiveStage === 'post_processing' && <Loader2 className="w-3 h-3 animate-spin shrink-0" />}
                      {auto_move_enabled ? '6. Post-processing' : '5. Post-processing'}
                      {post_processing_total > 0 && (
                        <span className="tabular-nums">
                          ({post_processing_done}/{post_processing_total})
                        </span>
                      )}
                    </span>
                  </>
                )}
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
                        Finalizing… Saving results to Unduper and summary.
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
                {showRawLogs && (
                  <div className="rounded-lg border border-border bg-black/90 text-[11px] font-mono max-h-56 overflow-y-auto">
                    {liveLogPath && (
                      <div className="px-3 py-1.5 text-[10px] text-emerald-400/80 border-b border-white/10 truncate" title={liveLogPath}>
                        {liveLogPath}
                      </div>
                    )}
                    <div className="px-3 py-2 space-y-0.5">
                      {(liveLogLines.length > 0 ? liveLogLines.slice(-180) : ['(no logs yet)']).map((line, idx) => (
                        <div
                          key={`${idx}-${line.slice(0, 12)}`}
                          className={cn(
                            "whitespace-pre-wrap break-words",
                            /\bERROR\b/.test(line) ? "text-red-300" : /\bWARNING\b/.test(line) ? "text-amber-200" : "text-zinc-200"
                          )}
                        >
                          {line}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* ─── Services verified at start (when preflight passed) ─────────── */}
            {scanning && preflightVerifiedAtStart && (
              <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
                <span className="font-medium text-foreground/80 w-full sm:w-auto">Services verified at start:</span>
                <span className={cn("inline-flex items-center gap-1.5", (preflightResult?.musicbrainz?.ok ?? true) ? "text-green-600 dark:text-green-400" : "text-destructive")}>
                  <Database className="w-3.5 h-3.5" />
                  MusicBrainz {(preflightResult?.musicbrainz?.ok ?? true) ? "✓" : "✗"}
                </span>
                <span className={cn("inline-flex items-center gap-1.5", (preflightResult?.ai?.ok ?? true) ? "text-green-600 dark:text-green-400" : "text-destructive")}>
                  <Sparkles className="w-3.5 h-3.5" />
                  AI {(preflightResult?.ai?.ok ?? true) ? "✓" : "✗"}
                </span>
                {preflightResult?.discogs != null && (
                  <span className={cn("inline-flex items-center gap-1.5", preflightResult.discogs.ok ? "text-green-600 dark:text-green-400" : "text-muted-foreground")} title={preflightResult.discogs.message ?? undefined}>
                    <Database className="w-3.5 h-3.5" />
                    Discogs {preflightResult.discogs.ok ? "✓" : "—"}
                  </span>
                )}
                {preflightResult?.lastfm != null && (
                  <span className={cn("inline-flex items-center gap-1.5", preflightResult.lastfm.ok ? "text-green-600 dark:text-green-400" : "text-muted-foreground")} title={preflightResult.lastfm.message ?? undefined}>
                    <Database className="w-3.5 h-3.5" />
                    Last.fm {preflightResult.lastfm.ok ? "✓" : "—"}
                  </span>
                )}
                {preflightResult?.bandcamp != null && (
                  <span className={cn("inline-flex items-center gap-1.5", preflightResult.bandcamp.ok ? "text-green-600 dark:text-green-400" : "text-muted-foreground")} title={preflightResult.bandcamp.message ?? undefined}>
                    <Database className="w-3.5 h-3.5" />
                    Bandcamp {preflightResult.bandcamp.ok ? "✓" : "—"}
                  </span>
                )}
                {preflightResult?.serper != null && (
                  <span className={cn("inline-flex items-center gap-1.5", preflightResult.serper.ok ? "text-green-600 dark:text-green-400" : "text-muted-foreground")} title={preflightResult.serper.message ?? undefined}>
                    <Database className="w-3.5 h-3.5" />
                    Serper {preflightResult.serper.ok ? "✓" : "—"}
                  </span>
                )}
                {preflightResult?.acoustid != null && (
                  <span className={cn("inline-flex items-center gap-1.5", preflightResult.acoustid.ok ? "text-green-600 dark:text-green-400" : "text-muted-foreground")} title={preflightResult.acoustid.message ?? undefined}>
                    <Database className="w-3.5 h-3.5" />
                    AcousticID {preflightResult.acoustid.ok ? "✓" : "—"}
                  </span>
                )}
                {(preflightPaths ?? progressPathsStatus) && (
                  <>
                    <span
                      className={cn(
                        "inline-flex items-center gap-1.5",
                        (preflightPaths?.music_rw ?? progressPathsStatus?.music_rw) ? "text-green-600 dark:text-green-400" : "text-destructive"
                      )}
                      title="Music folder(s) must be read-write for scan and move"
                    >
                      <FolderInput className="w-3.5 h-3.5" />
                      Music folder: {(preflightPaths?.music_rw ?? progressPathsStatus?.music_rw) ? "RW ✓" : "RW ✗"}
                    </span>
                    <span
                      className={cn(
                        "inline-flex items-center gap-1.5",
                        (preflightPaths?.dupes_rw ?? progressPathsStatus?.dupes_rw) ? "text-green-600 dark:text-green-400" : "text-destructive"
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
                      <span className="text-green-600 dark:text-green-400 font-medium">
                        On {ai_used_count > 0 && ` · ${ai_used_count} groups`}
                        {(ai_provider || ai_model) && ` · ${[ai_provider, ai_model].filter(Boolean).join(' · ')}`}
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
                      <span className="text-green-600 dark:text-green-400 font-medium">
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
                                <span className="flex items-center gap-1.5"><Package className="w-3 h-3 text-orange-500" /> Duplicate groups</span>
                                <span className="font-medium">{duplicate_groups_count}</span>
                              </div>
                            )}
                            {total_duplicates_count > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Music className="w-3 h-3 text-red-500" /> Total duplicates</span>
                                <span className="font-medium">{total_duplicates_count}</span>
                              </div>
                            )}
                            {broken_albums_count > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><AlertTriangle className="w-3 h-3 text-red-500" /> Incomplete albums</span>
                                <span className="font-medium text-red-600 dark:text-red-400">{nOfM(broken_albums_count, tot)}</span>
                              </div>
                            )}
                            {missing_albums_count > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Music className="w-3 h-3 text-yellow-500" /> Missing</span>
                                <span className="font-medium">{nOfM(missing_albums_count, tot)}</span>
                              </div>
                            )}
                            {albums_without_mb_id > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Database className="w-3 h-3 text-blue-500" /> No MB ID</span>
                                <span className="font-medium">{nOfM(albums_without_mb_id, tot)}</span>
                              </div>
                            )}
                            {albums_without_complete_tags > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Tag className="w-3 h-3 text-purple-500" /> Incomplete tags</span>
                                <span className="font-medium">{nOfM(albums_without_complete_tags, tot)}</span>
                              </div>
                            )}
                            {albums_without_album_image > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Image className="w-3 h-3 text-gray-500" /> No album art</span>
                                <span className="font-medium">{nOfM(albums_without_album_image, tot)}</span>
                              </div>
                            )}
                            {albums_without_artist_image > 0 && (
                              <div className="flex justify-between">
                                <span className="flex items-center gap-1.5"><Image className="w-3 h-3 text-gray-500" /> No artist art</span>
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
