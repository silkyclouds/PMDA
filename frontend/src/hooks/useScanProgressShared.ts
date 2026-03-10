import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useEffect, useRef } from 'react';
import { getScanProgress, getDedupeProgress, type ScanProgress, type DedupeProgress } from '@/lib/api';
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
  const preScanRootsTotal = Math.max(0, progress?.scan_discovery_roots_total || 0);
  const preScanRootsDone = Math.max(0, progress?.scan_discovery_roots_done || 0);
  const preScanEntriesScanned = Math.max(0, progress?.scan_discovery_entries_scanned || 0);
  const preScanFilesFound = Math.max(0, progress?.scan_discovery_files_found || 0);
  const preScanStage = String(progress?.scan_discovery_stage || '');
  const runScopePreparing = isScanning && (
    phase === 'preparing_run_scope' ||
    Boolean(progress?.scan_run_scope_preparing)
  );
  const preScanActive = isScanning && !runScopePreparing && (
    phase === 'pre_scan' ||
    Boolean(progress?.scan_discovery_running) ||
    preScanStage === 'filesystem' ||
    preScanStage === 'album_candidates' ||
    preScanAlbumsTotal > 0 ||
    Math.max(0, progress?.scan_discovery_files_found || 0) > 0
  );

  const preScanPercent = (() => {
    if (!preScanActive) return 0;
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

  const preScanStageLabel = preScanStage === 'album_candidates'
    ? 'album candidates'
    : preScanStage === 'filesystem'
      ? 'filesystem'
      : preScanStage === 'ready'
        ? 'ready'
        : 'pre-scan';

  const preScanStatusLabel = (preScanStage === 'album_candidates' || preScanAlbumsTotal > 0)
    ? `albums ${Math.max(0, Math.min(preScanAlbumsDone, Math.max(1, preScanAlbumsTotal))).toLocaleString()}/${Math.max(0, preScanAlbumsTotal).toLocaleString()}`
    : preScanRootsTotal > 0
      ? `roots ${Math.max(0, Math.min(preScanRootsDone, preScanRootsTotal)).toLocaleString()}/${preScanRootsTotal.toLocaleString()}`
      : `${preScanEntriesScanned.toLocaleString()} entries`;
  const preScanCountersLabel = `visited ${preScanEntriesScanned.toLocaleString()} · audio ${preScanFilesFound.toLocaleString()}`;
  const runScopeStage = String(progress?.scan_run_scope_stage || '');
  const runScopeDone = Math.max(0, progress?.scan_run_scope_done || 0);
  const runScopeTotal = Math.max(0, progress?.scan_run_scope_total || 0);
  const runScopePercent = Math.max(0, Math.min(100, progress?.scan_run_scope_percent || 0));
  const runScopeIncludedArtists = Math.max(0, progress?.scan_run_scope_artists_included || 0);
  const runScopeIncludedAlbums = Math.max(0, progress?.scan_run_scope_albums_included || 0);
  const runScopeStatusLabel = runScopeTotal > 0
    ? `${runScopeDone.toLocaleString()}/${runScopeTotal.toLocaleString()}`
    : `${runScopeDone.toLocaleString()}/?`;
  const runScopeCountersLabel = `in scope ${runScopeIncludedArtists.toLocaleString()} artists · ${runScopeIncludedAlbums.toLocaleString()} albums`;

  // Bar progress: include post-processing when present so 100% means truly finished.
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
    ? ((preScanActive || runScopePreparing) ? Math.min(100, rawPercent) : Math.min(99, rawPercent))
    : rawPercent;
  const status = progress?.status ?? 'idle';
  
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
    status,
    phase,
    preScanActive,
    preScanStageLabel,
    preScanStatusLabel,
    preScanCountersLabel,
    runScopePreparing,
    runScopeStage,
    runScopeStatusLabel,
    runScopeCountersLabel,

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
