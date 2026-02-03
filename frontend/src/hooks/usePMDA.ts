import { useState, useEffect, useCallback, useRef } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import * as api from '@/lib/api';
import type { DuplicateCard, ScanProgress, DedupeProgress } from '@/lib/api';

export function useDuplicates(options?: { refetchInterval?: number }) {
  return useQuery({
    queryKey: ['duplicates'],
    queryFn: api.getDuplicates,
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

  const fetchProgress = useCallback(async () => {
    try {
      const data = await api.getScanProgress();
      // Ensure we always have a valid object, merge with defaults if needed
      if (data && typeof data === 'object') {
        setProgress({ ...DEFAULT_SCAN_PROGRESS, ...data });
        return data;
      }
      // If API returns invalid data, keep defaults
      setProgress(DEFAULT_SCAN_PROGRESS);
      return DEFAULT_SCAN_PROGRESS;
    } catch {
      // Do not log: when server is busy (e.g. long dedupe) or user navigates away,
      // fetch fails repeatedly and would spam the console (TypeError: Failed to fetch).
      // Keep current progress so UI shows last known state.
      return null;
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

  const startMutation = useMutation({
    mutationFn: api.startScan,
    onSuccess: invalidateQueries,
    onError: (error: any) => {
      const msg = error?.body?.error;
      if (error?.body?.requiresConfig) {
        toast.error(msg || 'Plex is not configured. Go to Settings to set up Plex and your music library.');
      } else if (error?.body?.aiFunctionalFailure) {
        toast.error(msg || 'No model accepted our parameters. Try another model in Settings.');
      } else if (error?.body?.requiresAiConfig) {
        toast.error(msg || 'Configure the AI provider in Settings to run a scan');
      } else if (error?.response?.status === 503) {
        toast.error(msg || 'Scan cannot start. Check Settings (Plex and AI provider).');
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

  return {
    start: startMutation.mutate,
    pause: pauseMutation.mutate,
    resume: resumeMutation.mutate,
    stop: stopMutation.mutate,
    clear: clearMutation.mutate,
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
