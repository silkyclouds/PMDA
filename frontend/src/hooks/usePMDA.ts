import { useState, useEffect, useCallback, useRef } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import * as api from '@/lib/api';
import type { DuplicateCard, ScanProgress, DedupeProgress } from '@/lib/api';

type ApiErrorShape = {
  body?: {
    error?: string;
    requiresConfig?: boolean;
    aiFunctionalFailure?: boolean;
    requiresAiConfig?: boolean;
  };
  response?: {
    status?: number;
  };
};

export function useDuplicates(options?: { refetchInterval?: number; source?: 'scan' | 'all' }) {
  const source = options?.source ?? 'scan';
  return useQuery<DuplicateCard[], Error>({
    queryKey: ['duplicates', source],
    queryFn: () => api.getDuplicates({ source }),
    refetchInterval: options?.refetchInterval ?? 10000,
  });
}

export function useDuplicateDetails(artist: string, albumId: string, enabled: boolean = true) {
  return useQuery({
    queryKey: ['duplicate-details', artist, albumId],
    queryFn: () => api.getDuplicateDetails(artist, albumId),
    enabled: enabled && !!artist && !!albumId,
  });
}

const DEFAULT_SCAN_PROGRESS: ScanProgress = {
  scanning: false,
  progress: 0,
  total: 0,
  status: 'idle',
};

export function useScanProgress() {
  const [progress, setProgress] = useState<ScanProgress>(DEFAULT_SCAN_PROGRESS);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const fetchInFlightRef = useRef(false);

  const fetchProgress = useCallback(async () => {
    if (fetchInFlightRef.current) return null;
    fetchInFlightRef.current = true;
    try {
      const data = await api.getScanProgress();
      let inc: api.IncompleteScanProgress | null = null;
      try {
        inc = await api.getIncompleteScanProgress();
      } catch {
        // Ignore: incomplete endpoint may not be available or may fail
      }
      // If main scan is running, use it
      if (data && typeof data === 'object' && data.scanning) {
        setProgress({ ...DEFAULT_SCAN_PROGRESS, ...data });
        return data;
      }
      // If incomplete-only scan is running, build synthetic progress so UI shows "scanning"
      if (inc?.running) {
        const synthetic: ScanProgress = {
          ...DEFAULT_SCAN_PROGRESS,
          scanning: true,
          progress: inc.progress ?? 0,
          total: inc.total ?? 0,
          status: 'running',
          phase: 'format_analysis',
          current_step: 'incomplete_scan',
          active_artists: inc.current_artist ? [{
            artist_name: inc.current_artist,
            total_albums: inc.total ?? 0,
            albums_processed: inc.progress ?? 0,
            current_album: inc.current_album ? { album_id: 0, album_title: inc.current_album, status: 'running', status_details: '', step_summary: '', step_response: '' } : undefined,
          }] : undefined,
        };
        setProgress(synthetic);
        return synthetic;
      }
      // Normal: use main progress
      if (data && typeof data === 'object') {
        setProgress({ ...DEFAULT_SCAN_PROGRESS, ...data });
        return data;
      }
      setProgress(DEFAULT_SCAN_PROGRESS);
      return DEFAULT_SCAN_PROGRESS;
    } catch {
      return null;
    } finally {
      fetchInFlightRef.current = false;
    }
  }, []);

  useEffect(() => {
    fetchProgress();

    // Poll based on status
    const startPolling = (interval: number) => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
      intervalRef.current = setInterval(fetchProgress, interval);
    };

    // Initial poll
    startPolling(progress.scanning ? 1500 : 5000);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, [fetchProgress, progress.scanning]);

  return { progress, refetch: fetchProgress };
}

export function useDedupeProgress() {
  return useQuery({
    queryKey: ['dedupe-progress'],
    queryFn: api.getDedupeProgress,
    refetchInterval: (query) => (query.state.data?.deduping ? 1000 : 5000),
  });
}

export function useScanControls() {
  const queryClient = useQueryClient();

  const invalidateQueries = () => {
    queryClient.invalidateQueries({ queryKey: ['duplicates'] });
    queryClient.invalidateQueries({ queryKey: ['dedupe-progress'] });
  };

  const startMutation = useMutation<
    { status: string; scan_type?: string; run_improve_after?: boolean },
    unknown,
    api.StartScanOptions | undefined
  >({
    mutationFn: (options) => api.startScan(options),
    onSuccess: invalidateQueries,
    onError: (error: unknown) => {
      const err = error && typeof error === 'object' ? (error as ApiErrorShape) : {};
      const msg = err.body?.error;
      if (err.response?.status === 409) {
        toast.error(msg || 'A scan is already running.');
        return;
      }
      if (err.body?.requiresConfig) {
        toast.error(msg || 'No source folders configured. Go to Settings to add your music folders.');
      } else if (err.body?.aiFunctionalFailure) {
        toast.error(msg || 'No model accepted our parameters. Try another model in Settings.');
      } else if (err.body?.requiresAiConfig) {
        toast.error(msg || 'Configure the AI provider in Settings to run a scan');
      } else if (err.response?.status === 503) {
        toast.error(msg || 'Scan cannot start. Check Settings (folders and AI key).');
      }
    },
  });

  const pauseMutation = useMutation({
    mutationFn: api.pauseScan,
  });

  const resumeMutation = useMutation({
    mutationFn: api.resumeScan,
  });

  const stopMutation = useMutation({
    mutationFn: api.stopScan,
    onSuccess: invalidateQueries,
  });

  const clearMutation = useMutation({
    mutationFn: api.clearScan,
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ['duplicates'] });
      queryClient.invalidateQueries({ queryKey: ['scan-progress'] });
      // Return the result so the caller can handle success/error
      return result;
    },
  });

  const start = (options?: api.StartScanOptions) => startMutation.mutate(options);
  const pause = () => pauseMutation.mutate();
  const resume = () => resumeMutation.mutate();
  const stop = () => stopMutation.mutate();
  const clear = (options?: api.ClearScanOptions) => clearMutation.mutate(options ?? {});

  return {
    start,
    pause,
    resume,
    stop,
    clear,
    isStarting: startMutation.isPending,
    isPausing: pauseMutation.isPending,
    isResuming: resumeMutation.isPending,
    isStopping: stopMutation.isPending,
    isClearing: clearMutation.isPending,
  };
}

export function useDedupeActions() {
  const queryClient = useQueryClient();

  const invalidateQueries = () => {
    queryClient.invalidateQueries({ queryKey: ['duplicates'] });
    queryClient.invalidateQueries({ queryKey: ['dedupe-progress'] });
    queryClient.invalidateQueries({ queryKey: ['scan-history'] });
  };

  const dedupeSingle = useMutation({
    mutationFn: ({ artist, albumId }: { artist: string; albumId: string }) =>
      api.dedupeArtist(artist, albumId),
    onSuccess: invalidateQueries,
  });

  const dedupeSelected = useMutation({
    mutationFn: (selected: string[]) => api.dedupeSelected(selected),
    onSuccess: invalidateQueries,
  });

  const dedupeAll = useMutation({
    mutationFn: api.dedupeAll,
    onSuccess: invalidateQueries,
  });

  const dedupeMergeAndDedupe = useMutation({
    mutationFn: api.dedupeMergeAndDedupe,
    onSuccess: invalidateQueries,
  });

  return {
    dedupeSingle: dedupeSingle.mutateAsync,
    dedupeSelected: dedupeSelected.mutateAsync,
    dedupeAll: dedupeAll.mutateAsync,
    dedupeMergeAndDedupe: dedupeMergeAndDedupe.mutateAsync,
    isDeduping:
      dedupeSingle.isPending ||
      dedupeSelected.isPending ||
      dedupeAll.isPending ||
      dedupeMergeAndDedupe.isPending,
  };
}

export function useSelection(duplicates: DuplicateCard[] = []) {
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const toggle = useCallback((key: string) => {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  }, []);

  const selectAll = useCallback(() => {
    setSelected(new Set(duplicates.map(d => `${d.artist_key}||${d.album_id}`)));
  }, [duplicates]);

  const clearSelection = useCallback(() => {
    setSelected(new Set());
  }, []);

  const isSelected = useCallback((key: string) => selected.has(key), [selected]);

  return {
    selected,
    selectedArray: Array.from(selected),
    count: selected.size,
    toggle,
    selectAll,
    clearSelection,
    isSelected,
  };
}
