import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useEffect, useRef } from 'react';
import { getScanProgress, getDedupeProgress, type ScanProgress, type DedupeProgress } from '@/lib/api';
import { buildScanPipelineSteps } from '@/lib/scanPipeline';
import { buildScanPresentationModel } from '@/lib/scanPresentation';
import { toast } from 'sonner';

interface UseScanProgressSharedOptions {
  /** Poll interval in ms (default: 2000) */
  pollInterval?: number;
  /** Show toast on scan completion */
  showCompletionToast?: boolean;
}

/**
 * Shared hook for scan progress - used by both GlobalStatusBar and ScanProgress
 * to prevent duplicate API calls and ensure synchronized state.
 * Also tracks dedupe progress for unified status display.
 */
export function useScanProgressShared(options: UseScanProgressSharedOptions = {}) {
  const { pollInterval = 2000, showCompletionToast = false } = options;
  const queryClient = useQueryClient();
  const wasRunningRef = useRef(false);
  const wasDedupingRef = useRef(false);
  const hasToastedRef = useRef(false);
  const hasDedupeToastedRef = useRef(false);

  // Scan progress query
  const { data: progress, isLoading, error } = useQuery<ScanProgress>({
    queryKey: ['scan-progress-shared'],
    queryFn: getScanProgress,
    refetchInterval: pollInterval,
    staleTime: 1000,
    retry: 1,
  });

  // Dedupe progress query
  const { data: dedupeProgress } = useQuery<DedupeProgress>({
    queryKey: ['dedupe-progress-shared'],
    queryFn: getDedupeProgress,
    refetchInterval: pollInterval,
    staleTime: 1000,
    retry: 1,
  });

  const isScanning = progress?.scanning ?? false;
  const isDeduping = dedupeProgress?.deduping ?? false;
  const phase = progress?.phase ?? null;

  // Pre-scan progress (FILES discovery + album candidate planning).
  // We expose a meaningful moving percentage even before we know final album N/M.
  const preScanAlbumsTotal = Math.max(0, progress?.scan_preplan_total || progress?.scan_discovery_albums_total || progress?.scan_discovery_folders_total || 0);
  const preScanAlbumsDone = Math.max(0, progress?.scan_preplan_done || progress?.scan_discovery_albums_done || progress?.scan_discovery_folders_done || 0);
  const preScanSnapshotTotal = Math.max(
    0,
    progress?.scan_prescan_cache_snapshot_total || progress?.detected_albums_total || progress?.total_albums || 0,
  );
  const preScanSnapshotDone = Math.max(0, progress?.scan_prescan_cache_snapshot_rows || 0);
  const preScanSnapshotActive = isScanning && Boolean(progress?.scan_prescan_cache_snapshot_running) && preScanSnapshotTotal > 0;
  const preScanCatchupTotal = Math.max(0, progress?.scan_published_catchup_total || 0);
  const preScanCatchupDone = Math.max(0, progress?.scan_published_catchup_done || 0);
  const preScanCatchupOk = Math.max(0, progress?.scan_published_catchup_ok || 0);
  const preScanCatchupFailed = Math.max(0, progress?.scan_published_catchup_failed || 0);
  const preScanCatchupCurrentArtist = String(progress?.scan_published_catchup_current_artist || '').trim();
  const preScanCatchupActive = isScanning && Boolean(progress?.scan_published_catchup_running) && preScanCatchupTotal > 0;
  const preScanRootsTotal = Math.max(0, progress?.scan_discovery_roots_total || 0);
  const preScanRootsDone = Math.max(0, progress?.scan_discovery_roots_done || 0);
  const preScanEntriesScanned = Math.max(0, progress?.scan_discovery_entries_scanned || 0);
  const preScanFilesFound = Math.max(0, progress?.scan_discovery_files_found || 0);
  const preScanStage = preScanCatchupActive
    ? 'library_rehydration'
    : preScanSnapshotActive
      ? 'cache_snapshot'
      : String(progress?.scan_discovery_stage || (progress?.scan_resume_run_id ? 'resume_warmup' : ''));
  const runScopePreparing = isScanning && (
    phase === 'preparing_run_scope' ||
    Boolean(progress?.scan_run_scope_preparing)
  );
  const mainPipelinePhaseActive = isScanning && Boolean(phase) && !['pre_scan', 'preparing_run_scope'].includes(String(phase));
  const preScanActive = isScanning && !runScopePreparing && !mainPipelinePhaseActive && (
    phase === 'pre_scan' ||
    preScanCatchupActive ||
    preScanSnapshotActive ||
    Boolean(progress?.scan_discovery_running) ||
    preScanStage === 'filesystem' ||
    preScanStage === 'album_candidates' ||
    preScanAlbumsTotal > 0 ||
    Math.max(0, progress?.scan_discovery_files_found || 0) > 0
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
    const stage = preScanStage;
    if (stage === 'ready') return 100;
    if (stage === 'album_candidates' || preScanAlbumsTotal > 0) {
      const total = Math.max(1, preScanAlbumsTotal);
      const done = Math.max(0, Math.min(preScanAlbumsDone, total));
      return Math.floor(70 + (25 * done) / total);
    }
    if (stage === 'filesystem' || preScanRootsTotal > 0) {
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

  const preScanStageLabel = preScanStage === 'library_rehydration'
    ? 'library rehydration'
    : preScanStage === 'album_candidates'
    ? 'album candidates'
    : preScanStage === 'filesystem'
      ? 'filesystem'
      : preScanStage === 'cache_snapshot'
        ? 'cache snapshot'
      : preScanStage === 'resume_warmup'
        ? 'resume warm-up'
      : preScanStage === 'ready'
        ? 'ready'
        : 'pre-scan';

  const preScanStatusLabel = preScanCatchupActive
    ? `artists ${Math.max(0, Math.min(preScanCatchupDone, Math.max(1, preScanCatchupTotal))).toLocaleString()}/${Math.max(0, preScanCatchupTotal).toLocaleString()}`
    : preScanSnapshotActive
    ? `cache ${Math.max(0, Math.min(preScanSnapshotDone, Math.max(1, preScanSnapshotTotal))).toLocaleString()}/${Math.max(0, preScanSnapshotTotal).toLocaleString()}`
    : (preScanStage === 'album_candidates' || preScanAlbumsTotal > 0)
    ? `albums ${Math.max(0, Math.min(preScanAlbumsDone, Math.max(1, preScanAlbumsTotal))).toLocaleString()}/${Math.max(0, preScanAlbumsTotal).toLocaleString()}`
    : preScanRootsTotal > 0
      ? `roots ${Math.max(0, Math.min(preScanRootsDone, preScanRootsTotal)).toLocaleString()}/${preScanRootsTotal.toLocaleString()}`
      : (progress?.scan_resume_run_id ? 'resume warm-up' : `${preScanEntriesScanned.toLocaleString()} entries`);
  const preScanCountersLabel = preScanCatchupActive
    ? `rehydrating visible library index · ok ${preScanCatchupOk.toLocaleString()} · failed ${preScanCatchupFailed.toLocaleString()}${preScanCatchupCurrentArtist ? ` · artist ${preScanCatchupCurrentArtist}` : ''}`
    : preScanSnapshotActive
    ? `persisting resume cache before artist workers start`
    : preScanStage === 'resume_warmup'
      ? `restoring cached run plan before worker stages start`
    : `visited ${preScanEntriesScanned.toLocaleString()} · audio ${preScanFilesFound.toLocaleString()}`;
  const preScanIndeterminate = preScanActive && (
    preScanProgressTotal <= 0 ||
    (preScanSnapshotActive && preScanSnapshotDone <= 0) ||
    (preScanCatchupActive && preScanCatchupDone <= 0) ||
    (preScanStage === 'filesystem' && preScanRootsTotal <= 1)
  );
  const runScopeStage = String(progress?.scan_run_scope_stage || '');
  const runScopeDone = Math.max(0, progress?.scan_run_scope_done || 0);
  const runScopeTotal = Math.max(0, progress?.scan_run_scope_total || 0);
  const runScopePercent = Math.max(0, Math.min(100, progress?.scan_run_scope_percent || 0));
  const runScopeIndeterminate = runScopePreparing && runScopeTotal <= 0;
  const runScopeIncludedArtists = Math.max(0, progress?.scan_run_scope_artists_included || 0);
  const runScopeIncludedAlbums = Math.max(0, progress?.scan_run_scope_albums_included || 0);
  const runScopeStatusLabel = runScopeTotal > 0
    ? `${runScopeDone.toLocaleString()}/${runScopeTotal.toLocaleString()}`
    : runScopeDone > 0
      ? `${runScopeDone.toLocaleString()}/?`
      : 'estimating…';
  const runScopeCountersLabel = `in scope ${runScopeIncludedArtists.toLocaleString()} artists · ${runScopeIncludedAlbums.toLocaleString()} albums`;

  const stageProgressDone = Math.max(0, progress?.stage_progress_done ?? 0);
  const stageProgressTotal = Math.max(0, progress?.stage_progress_total ?? 0);
  const stageProgressUnit = String(progress?.stage_progress_unit || '').trim() || 'steps';
  const rawStagePercent = Number.isFinite(progress?.stage_progress_percent as number)
    ? Number(progress?.stage_progress_percent)
    : 0;
  const stageProgressPercent = isScanning
    ? Math.max(0, Math.min(100, rawStagePercent))
    : 0;
  const overallProgressDone = Math.max(0, progress?.overall_progress_done ?? 0);
  const overallProgressTotal = Math.max(0, progress?.overall_progress_total ?? 0);
  const rawOverallPercent = Number.isFinite(progress?.overall_progress_percent as number)
    ? Number(progress?.overall_progress_percent)
    : 0;
  const overallProgressPercent = isScanning
    ? Math.max(0, Math.min(100, rawOverallPercent))
    : 0;

  // Back-compat overall bar: include post-processing when present so 100% means truly finished.
  const artistsProcessed = progress?.artists_processed ?? 0;
  const artistsTotal = progress?.artists_total ?? 0;
  const postDone = progress?.post_processing_done ?? 0;
  const postTotal = progress?.post_processing_total ?? 0;
  const hasPostWork = isScanning && (Boolean(progress?.post_processing) || postTotal > 0);
  const stepProgress = progress?.progress ?? 0;
  const stepTotal = progress?.total ?? 0;
  const clampedArtistsDone = Math.max(0, Math.min(artistsProcessed, artistsTotal));
  const clampedPostDone = Math.max(0, Math.min(postDone, postTotal));
  const compositeDone = clampedArtistsDone + clampedPostDone;
  const compositeTotal = Math.max(0, artistsTotal) + Math.max(0, postTotal);
  const rawPercent = preScanActive
    ? preScanPercent
    : runScopePreparing
    ? runScopePercent
    : hasPostWork
    ? (compositeTotal > 0 ? Math.min(100, (compositeDone / compositeTotal) * 100) : 0)
    : (isScanning && artistsTotal > 0
      ? Math.min(100, (artistsProcessed / artistsTotal) * 100)
      : (stepTotal > 0 ? Math.min(100, (stepProgress / stepTotal) * 100) : 0));
  const progressPercent = isScanning
    ? (preScanActive || runScopePreparing
      ? Math.min(100, rawPercent)
      : stageProgressTotal > 0
        ? stageProgressPercent
        : 0)
    : rawPercent;
  const status = progress?.status ?? 'idle';

  const phaseLabel = runScopePreparing
    ? `Preparing run scope · ${runScopeStage || 'signatures'}`
    : preScanActive
      ? `Pre-scan · ${preScanStageLabel}`
      : phase === 'incomplete_move'
        ? 'Quarantine incompletes'
        : phase === 'export'
          ? 'Build library'
      : phase === 'profile_enrichment'
        ? 'Profile enrichment'
        : phase === 'format_analysis'
          ? 'Format analysis'
          : phase === 'identification_tags'
            ? 'Identification & tags'
            : phase === 'ia_analysis'
              ? 'AI analysis'
              : phase === 'moving_dupes'
                ? 'Moving dupes'
                : phase === 'post_processing'
                  ? 'Post-processing'
                  : phase === 'background_enrichment'
                    ? 'Background enrichment'
                    : phase === 'finalizing'
                      ? 'Finalizing'
                      : 'Scanning';

  const stageStatusLabel = (() => {
    if (runScopePreparing) return `${runScopeStatusLabel} · ${runScopeCountersLabel}`;
    if (preScanActive) {
      return preScanIndeterminate
        ? `estimating scope · ${preScanCountersLabel}`
        : `${preScanStatusLabel} · ${preScanCountersLabel}`;
    }
    if (stageProgressTotal > 0) {
      return `${stageProgressDone.toLocaleString()}/${stageProgressTotal.toLocaleString()} ${stageProgressUnit}`;
    }
    if (phase === 'finalizing') return 'Saving results and closing the run';
    if (phase === 'background_enrichment') return 'Finishing provider-only enrichments';
    return '';
  })();
  const pipeline = buildScanPipelineSteps(progress);
  const presentation = buildScanPresentationModel(progress);
  const pipelineStageFraction = (() => {
    if (!isScanning || pipeline.total <= 0) return 0;
    if (runScopePreparing) return Math.max(0, Math.min(1, runScopePercent / 100));
    if (preScanActive) return Math.max(0, Math.min(1, preScanPercent / 100));
    if (stageProgressTotal > 0) return Math.max(0, Math.min(1, stageProgressPercent / 100));
    if (rawStagePercent > 0) return Math.max(0, Math.min(1, rawStagePercent / 100));
    return 0;
  })();
  const pipelineOverallDoneSteps = isScanning && pipeline.total > 0
    ? Math.max(0, Math.min(pipeline.total, (pipeline.currentIndex - 1) + pipelineStageFraction))
    : 0;
  const pipelineOverallTotalSteps = pipeline.total;
  const pipelineOverallProgressPercent = isScanning && pipelineOverallTotalSteps > 0
    ? Math.max(0, Math.min(100, (pipelineOverallDoneSteps / pipelineOverallTotalSteps) * 100))
    : overallProgressPercent;
  
  // Dedupe progress values
  const dedupeProgressValue = dedupeProgress?.progress ?? 0;
  const dedupeTotal = dedupeProgress?.total ?? 0;
  const dedupePercent = dedupeProgress?.percent ?? (dedupeTotal > 0 ? Math.round((dedupeProgressValue / dedupeTotal) * 100) : 0);
  const currentDedupeGroup = dedupeProgress?.current_group ?? null;
  
  // Get current artist/album from active_artists array
  const currentArtist = progress?.active_artists?.[0]?.artist_name;
  const currentAlbum = progress?.active_artists?.[0]?.current_album?.album_title;
  
  // Derived state
  const isIdle = !isScanning && !isDeduping && status !== 'running';
  const isPaused = status === 'paused';
  const isRunning = status === 'running' && isScanning;

  // Detect scan completion and trigger callbacks/toasts
  useEffect(() => {
    if (isScanning) {
      wasRunningRef.current = true;
      hasToastedRef.current = false;
    } else if (wasRunningRef.current && !isScanning && !hasToastedRef.current) {
      wasRunningRef.current = false;
      
      // Invalidate related queries on completion
      queryClient.invalidateQueries({ queryKey: ['duplicates'] });
      queryClient.invalidateQueries({ queryKey: ['library-stats'] });
      queryClient.invalidateQueries({ queryKey: ['scan-history'] });
      
      if (showCompletionToast) {
        toast.success('Scan complete!');
        hasToastedRef.current = true;
      }
    }
  }, [isScanning, queryClient, showCompletionToast]);

  // Detect dedupe completion and trigger callbacks/toasts
  useEffect(() => {
    if (isDeduping) {
      wasDedupingRef.current = true;
      hasDedupeToastedRef.current = false;
    } else if (wasDedupingRef.current && !isDeduping && !hasDedupeToastedRef.current) {
      wasDedupingRef.current = false;
      
      // Invalidate related queries on dedupe completion
      queryClient.invalidateQueries({ queryKey: ['duplicates'] });
      queryClient.invalidateQueries({ queryKey: ['library-stats'] });
      queryClient.invalidateQueries({ queryKey: ['scan-history'] });
      queryClient.invalidateQueries({ queryKey: ['scan-progress'] });
      queryClient.invalidateQueries({ queryKey: ['scan-progress-shared'] });
      
      if (showCompletionToast) {
        toast.success('Undupe complete!');
        hasDedupeToastedRef.current = true;
      }
    }
  }, [isDeduping, queryClient, showCompletionToast]);

  // Manually trigger a refresh
  const refresh = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: ['scan-progress-shared'] });
    queryClient.invalidateQueries({ queryKey: ['dedupe-progress-shared'] });
  }, [queryClient]);

  return {
    progress,
    dedupeProgress,
    isLoading,
    error,
    
    // Derived state
    isScanning,
    isDeduping,
    isIdle,
    isPaused,
    isRunning,
    
    // Scan progress values
    progressPercent,
    stageProgressPercent,
    stageProgressDone,
    stageProgressTotal,
    stageProgressUnit,
    overallProgressPercent,
    overallProgressDone,
    overallProgressTotal,
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
    pipelineSteps: pipeline.steps,
    currentPipelineStepIndex: pipeline.currentIndex,
    pipelineStepsTotal: pipeline.total,
    currentPipelineStepLabel: pipeline.currentLabel,
    presentation,

    // Dedupe progress values
    dedupeProgressValue,
    dedupeTotal,
    dedupePercent,
    currentDedupeGroup,
    
    // Current work
    currentArtist,
    currentAlbum,
    etaSeconds: progress?.eta_seconds,
    threadsInUse: progress?.threads_in_use,
    
    // Last scan summary
    lastScanSummary: progress?.last_scan_summary,
    
    // Actions
    refresh,
  };
}
