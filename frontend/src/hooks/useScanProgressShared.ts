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
  const progressPercent = progress?.effective_progress ?? progress?.progress ?? 0;
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
    phase: progress?.phase ?? null,
    
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
