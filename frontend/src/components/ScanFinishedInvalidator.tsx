import { useEffect, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import * as api from '@/lib/api';

/**
 * Polls scan and dedupe progress and invalidates relevant queries when
 * operations complete. This runs regardless of which page is mounted.
 */
const IDLE_INVALIDATE_INTERVAL_MS = 8000;

export function ScanFinishedInvalidator() {
  const queryClient = useQueryClient();
  const wasScanningRef = useRef(false);
  const wasDedupingRef = useRef(false);
  const lastIdleInvalidateRef = useRef(0);
  const idleInvalidateDoneRef = useRef(false);
  const checkInFlightRef = useRef(false);

  useEffect(() => {
    // On mount (e.g. after refresh), invalidate so we refetch and show fresh stats/dupes
    queryClient.invalidateQueries({ queryKey: ['duplicates'] });
    queryClient.invalidateQueries({ queryKey: ['scan-progress'] });

    const check = async () => {
      if (checkInFlightRef.current) return;
      checkInFlightRef.current = true;
      try {
        // Check scan progress
        const scanData = await api.getScanProgress();
        const scanning = scanData?.scanning ?? false;
        const justFinishedScan = wasScanningRef.current && !scanning;
        
        // Check dedupe progress
        const dedupeData = await api.getDedupeProgress();
        const deduping = dedupeData?.deduping ?? false;
        const justFinishedDedupe = wasDedupingRef.current && !deduping;
        
        const idleLongEnough =
          !scanning &&
          !deduping &&
          Date.now() - lastIdleInvalidateRef.current > IDLE_INVALIDATE_INTERVAL_MS;
          
        // Invalidate when: scan or dedupe just finished, or periodic idle refresh
        const shouldInvalidate =
          justFinishedScan ||
          justFinishedDedupe ||
          idleLongEnough ||
          (!scanning && !deduping && !idleInvalidateDoneRef.current);
          
        if (shouldInvalidate) {
          queryClient.invalidateQueries({ queryKey: ['duplicates'] });
          queryClient.invalidateQueries({ queryKey: ['scan-progress'] });
          queryClient.invalidateQueries({ queryKey: ['scan-progress-shared'] });
          queryClient.invalidateQueries({ queryKey: ['dedupe-progress'] });
          queryClient.invalidateQueries({ queryKey: ['dedupe-progress-shared'] });
          queryClient.invalidateQueries({ queryKey: ['scan-history'] });
          queryClient.invalidateQueries({ queryKey: ['library-stats'] });
          
          if (!scanning && !deduping) {
            lastIdleInvalidateRef.current = Date.now();
            idleInvalidateDoneRef.current = true;
          }
        }
        
        if (scanning || deduping) idleInvalidateDoneRef.current = false;
        wasScanningRef.current = scanning;
        wasDedupingRef.current = deduping;
      } catch {
        // Ignore fetch errors (e.g. server busy or wrong origin)
      } finally {
        checkInFlightRef.current = false;
      }
    };
    check();
    const interval = setInterval(check, 2000);
    return () => clearInterval(interval);
  }, [queryClient]);

  return null;
}
