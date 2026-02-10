import { useEffect, useRef, useState, useCallback } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { ChevronDown } from 'lucide-react';
import { Header } from '@/components/Header';
import { ScanProgress } from '@/components/ScanProgress';
import { DropAlbumZone } from '@/components/DropAlbumZone';
import { IncompleteResultsPanel } from '@/components/incomplete-results/IncompleteResultsPanel';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Button } from '@/components/ui/button';
import { useScanProgress, useScanControls, useDuplicates } from '@/hooks/usePMDA';
import type { ScanProgress as ScanProgressType } from '@/lib/api';

type LastScanSummary = NonNullable<ScanProgressType['last_scan_summary']>;

export default function Scan() {
  const { progress: scanProgress } = useScanProgress();
  const scanControls = useScanControls();
  const queryClient = useQueryClient();
  const wasScanningRef = useRef(false);
  
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
    const scanning = scanProgress?.scanning ?? false;
    if (wasScanningRef.current && !scanning) {
      queryClient.invalidateQueries({ queryKey: ['duplicates'] });
      queryClient.invalidateQueries({ queryKey: ['scan-progress'] });
    }
  }, [scanProgress?.scanning, queryClient]);

  return (
    <div className="min-h-screen bg-background">
      <Header />

      <main className="container py-6 space-y-6">
        <div>
          <h1 className="text-display text-foreground">Library Scan</h1>
          <p className="text-small text-muted-foreground mt-1">Run scans, then resolve duplicates and metadata in a clear workflow</p>
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
        />

        <Collapsible>
          <div className="rounded-xl border border-border bg-card">
            <CollapsibleTrigger asChild>
              <Button variant="ghost" className="w-full justify-between rounded-xl">
                <span>Advanced scan tools</span>
                <ChevronDown className="w-4 h-4 transition-transform data-[state=open]:rotate-180" />
              </Button>
            </CollapsibleTrigger>
            <CollapsibleContent className="space-y-6 p-4 pt-0">
              <DropAlbumZone />
              <IncompleteResultsPanel />
            </CollapsibleContent>
          </div>
        </Collapsible>
      </main>
    </div>
  );
}
