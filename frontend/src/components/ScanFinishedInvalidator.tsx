import { useEffect, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useScanProgressShared } from '@/hooks/useScanProgressShared';

/**
 * Listens to shared scan/dedupe progress and invalidates related queries only
 * when transitions complete (running -> idle). Avoids extra polling loops.
 */
export function ScanFinishedInvalidator() {
  const queryClient = useQueryClient();
  const { isScanning, isDeduping } = useScanProgressShared({ pollInterval: 3000 });
  const wasScanningRef = useRef(false);
  const wasDedupingRef = useRef(false);

  useEffect(() => {
    // Initial sync on mount (e.g. hard refresh)
    queryClient.invalidateQueries({ queryKey: ['duplicates'] });
    queryClient.invalidateQueries({ queryKey: ['scan-progress'] });
    queryClient.invalidateQueries({ queryKey: ['scan-progress-shared'] });
    queryClient.invalidateQueries({ queryKey: ['dedupe-progress-shared'] });
  }, [queryClient]);

  useEffect(() => {
    const justFinishedScan = wasScanningRef.current && !isScanning;
    const justFinishedDedupe = wasDedupingRef.current && !isDeduping;
    if (justFinishedScan || justFinishedDedupe) {
      queryClient.invalidateQueries({ queryKey: ['duplicates'] });
      queryClient.invalidateQueries({ queryKey: ['scan-progress'] });
      queryClient.invalidateQueries({ queryKey: ['scan-progress-shared'] });
      queryClient.invalidateQueries({ queryKey: ['dedupe-progress'] });
      queryClient.invalidateQueries({ queryKey: ['dedupe-progress-shared'] });
      queryClient.invalidateQueries({ queryKey: ['scan-history'] });
      queryClient.invalidateQueries({ queryKey: ['library-stats'] });
    }
    wasScanningRef.current = isScanning;
    wasDedupingRef.current = isDeduping;
  }, [isDeduping, isScanning, queryClient]);

  return null;
}
