import { useEffect, useRef, useState, useCallback } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { ScanProgress, type ScanType } from '@/components/ScanProgress';
import { useScanControls, useDuplicates } from '@/hooks/usePMDA';
import { useScanProgressShared } from '@/hooks/useScanProgressShared';
import type { ScanProgress as ScanProgressType } from '@/lib/api';

type LastScanSummary = NonNullable<ScanProgressType['last_scan_summary']>;

export default function Scan() {
  const { progress: sharedProgress } = useScanProgressShared({ pollInterval: 2500 });
  const scanProgress = sharedProgress ?? { scanning: false, progress: 0, total: 0, status: 'idle' as const };
  const scanControls = useScanControls();
  const queryClient = useQueryClient();
  const wasScanningRef = useRef(false);
  const [scanType, setScanType] = useState<ScanType>('full');
  const [scanTypeTouched, setScanTypeTouched] = useState(false);
  
  // Persistent summary - survives query invalidation, only clears on explicit "Clear results"
  const [persistedSummary, setPersistedSummary] = useState<LastScanSummary | null>(null);
  
  // Get current duplicate count for real-time card visibility
  const { data: duplicates = [] } = useDuplicates({ refetchInterval: 5000 });
  const currentDuplicateCount = duplicates.length;

  // Capture/update summary when scan completes or when we first get one
  useEffect(() => {
    const scanning = scanProgress?.scanning ?? false;
    const newSummary = scanProgress?.last_scan_summary;
    
    // When scan just finished (was scanning, now not), capture the summary
    if (wasScanningRef.current && !scanning && newSummary) {
      setPersistedSummary(newSummary);
    }
    
    // If we get a summary from API and don't have one persisted, use it
    // This handles page refresh after a scan completed
    if (newSummary && !persistedSummary) {
      setPersistedSummary(newSummary);
    }
    
    wasScanningRef.current = scanning;
  }, [scanProgress?.scanning, scanProgress?.last_scan_summary, persistedSummary]);

  // Handle clear - also clears persisted summary
  const handleClear = useCallback((options?: { clear_audio_cache?: boolean; clear_mb_cache?: boolean }) => {
    scanControls.clear(options);
    setPersistedSummary(null);
  }, [scanControls]);

  // Enhanced progress with persisted summary
  const enhancedProgress: ScanProgressType = scanProgress ? {
    ...scanProgress,
    last_scan_summary: persistedSummary ?? scanProgress.last_scan_summary,
  } : { scanning: false, progress: 0, total: 0, status: 'idle' as const };

  useEffect(() => {
    const backendDefaultRaw = String(scanProgress?.default_scan_type || '').trim().toLowerCase();
    const backendDefault: ScanType = (
      backendDefaultRaw === 'changed_only' || backendDefaultRaw === 'incomplete_only' || backendDefaultRaw === 'full'
        ? (backendDefaultRaw as ScanType)
        : 'full'
    );
    if (!scanTypeTouched) {
      setScanType(backendDefault);
    }
  }, [scanProgress?.default_scan_type, scanTypeTouched]);

  const handleScanTypeChange = useCallback((next: ScanType) => {
    setScanTypeTouched(true);
    setScanType(next);
  }, []);

  useEffect(() => {
    const scanning = scanProgress?.scanning ?? false;
    if (wasScanningRef.current && !scanning) {
      queryClient.invalidateQueries({ queryKey: ['duplicates'] });
      queryClient.invalidateQueries({ queryKey: ['scan-progress'] });
    }
  }, [scanProgress?.scanning, queryClient]);

  return (
    <main className="pmda-page-shell pmda-page-stack">
      <div>
        <h1 className="pmda-page-title">Library Scan</h1>
        <p className="pmda-meta-text mt-1">Run scans, then resolve duplicates and metadata in a clear workflow</p>
      </div>

      <ScanProgress
        progress={enhancedProgress}
        currentDuplicateCount={currentDuplicateCount}
        onStart={scanControls.start}
        onPause={scanControls.pause}
        onResume={scanControls.resume}
        onStop={scanControls.stop}
        onClear={handleClear}
        isStarting={scanControls.isStarting}
        isPausing={scanControls.isPausing}
        isResuming={scanControls.isResuming}
        isStopping={scanControls.isStopping}
        isClearing={scanControls.isClearing}
        scanType={scanType}
        onScanTypeChange={handleScanTypeChange}
        compact
      />
    </main>
  );
}
