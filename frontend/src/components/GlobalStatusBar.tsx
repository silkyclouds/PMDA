import { useEffect, useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { Package, RefreshCw, CheckCircle2, Play, ArrowRight, Loader2, Wrench, AlertTriangle, Terminal, ChevronDown, ChevronUp, Download } from 'lucide-react';
import { Progress } from '@/components/ui/progress';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { useScanProgressShared } from '@/hooks/useScanProgressShared';
import { useDuplicates, useScanControls } from '@/hooks/usePMDA';
import { getScanLogsTail } from '@/lib/api';

export function GlobalStatusBar() {
  const navigate = useNavigate();
  const scanControls = useScanControls();
  const { 
    isScanning, 
    isDeduping,
    isIdle, 
    progressPercent, 
    currentArtist,
    lastScanSummary,
    dedupePercent,
    dedupeProgressValue,
    dedupeTotal,
    currentDedupeGroup,
  } = useScanProgressShared({ showCompletionToast: true });
  
  const { data: duplicates = [] } = useDuplicates({ refetchInterval: 30000 });
  const duplicateCount = duplicates.length;
  const [showLogs, setShowLogs] = useState(false);
  const [logLines, setLogLines] = useState<string[]>([]);
  
  // Count unique artists
  const artistSet = new Set<string>();
  for (const dupe of duplicates) {
    if (dupe.artist_key) artistSet.add(dupe.artist_key);
  }
  const artistCount = lastScanSummary?.artists_total ?? artistSet.size;
  const albumCount = lastScanSummary?.albums_scanned ?? duplicates.length;
  
  // Get "to be fixed" and "incomplete" counts from last scan summary
  const toBeFixedCount = (lastScanSummary?.albums_without_complete_tags ?? 0) + 
                         (lastScanSummary?.albums_without_album_image ?? 0) + 
                         (lastScanSummary?.albums_without_artist_image ?? 0);
  const incompleteCount = lastScanSummary?.broken_albums_count ?? 0;

  const handleStartScan = () => {
    scanControls.start();
  };

  useEffect(() => {
    if (!showLogs) return;
    let cancelled = false;
    const tick = async () => {
      try {
        const data = await getScanLogsTail(120);
        if (!cancelled) {
          const lines = Array.isArray(data?.lines) ? data.lines : [];
          // Show newest first for quick readability in the drawer.
          setLogLines(lines.slice(-8).reverse());
        }
      } catch {
        // ignore
      }
    };
    tick();
    const id = setInterval(tick, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [showLogs, isScanning]);

  if (!isScanning && !isDeduping) {
    return null;
  }

  return (
    <div className="sticky top-[61px] z-40 border-b border-border/80 bg-gradient-to-r from-card via-card to-card/95 backdrop-blur supports-[backdrop-filter]:bg-card/90">
      <div className="container">
        <div className="flex items-center justify-between py-2.5 gap-4">
          {/* Left: Status with quick action */}
          <div className="flex items-center gap-3 min-w-0">
            {isDeduping ? (
              // Unduping state - show dedupe progress
              <>
                <div className="flex items-center gap-2">
                  <Loader2 className="w-4 h-4 text-primary animate-spin" />
                  <span className="text-sm font-medium text-foreground">Unduping</span>
                </div>
                <div className="hidden sm:flex items-center gap-2 min-w-0">
                  <Progress value={dedupePercent} className="w-28 h-1.5" />
                  <span className="text-xs text-muted-foreground tabular-nums font-medium">
                    {dedupeProgressValue}/{dedupeTotal}
                  </span>
                </div>
                {currentDedupeGroup && (
                  <span className="hidden lg:block text-xs text-muted-foreground truncate max-w-48">
                    {currentDedupeGroup.artist} – {currentDedupeGroup.album}
                  </span>
                )}
              </>
            ) : isScanning ? (
              // Scanning state
              <>
                <div className="flex items-center gap-2">
                  <RefreshCw className="w-4 h-4 text-primary animate-spin" />
                  <span className="text-sm font-medium text-foreground">Scanning</span>
                </div>
                <div className="hidden sm:flex items-center gap-2 min-w-0">
                  <Progress value={progressPercent} className="w-28 h-1.5" />
                  <span className="text-xs text-muted-foreground tabular-nums font-medium">{Math.round(progressPercent)}%</span>
                </div>
                {currentArtist && (
                  <span className="hidden lg:block text-xs text-muted-foreground truncate max-w-48">
                    {currentArtist}
                  </span>
                )}
              </>
            ) : isIdle && duplicateCount > 0 ? (
              // Idle with dupes - prompt to review
              <div className="flex items-center gap-3">
                <div className="flex items-center gap-2">
                  <CheckCircle2 className="w-4 h-4 text-success" />
                  <span className="text-sm text-muted-foreground">Ready</span>
                </div>
                <Link 
                  to="/tools"
                  className="hidden sm:flex items-center gap-1.5 text-sm font-medium text-warning hover:text-warning/80 transition-colors"
                >
                  <Package className="w-3.5 h-3.5" />
                  <span>Review {duplicateCount} duplicates in Tools</span>
                  <ArrowRight className="w-3 h-3" />
                </Link>
              </div>
            ) : (
              // Idle, no dupes
              <div className="flex items-center gap-3">
                <div className="flex items-center gap-2">
                  <CheckCircle2 className="w-4 h-4 text-success" />
                  <span className="text-sm text-muted-foreground">Ready</span>
                </div>
                <Button 
                  variant="ghost" 
                  size="sm" 
                  onClick={handleStartScan}
                  disabled={scanControls.isStarting}
                  className="hidden sm:flex h-7 text-xs gap-1.5 text-muted-foreground hover:text-foreground"
                >
                  <Play className="w-3 h-3" />
                  {scanControls.isStarting ? 'Starting...' : 'Start scan'}
                </Button>
              </div>
            )}
          </div>

          {/* Center/Right: Quick stats */}
          <div className="flex items-center gap-3">
            <button
              onClick={() => setShowLogs((v) => !v)}
              className="hidden sm:flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium transition-all bg-muted/60 text-muted-foreground hover:text-foreground hover:bg-muted"
            >
              <Terminal className="w-3.5 h-3.5" />
              <span>Logs</span>
              {showLogs ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
            </button>
            {/* Duplicates badge - prominent when > 0 */}
            {duplicateCount > 0 && (
              <button
                onClick={() => navigate('/tools')}
                className={cn(
                  "flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium transition-all",
                  "bg-warning/15 text-warning hover:bg-warning/25",
                  duplicateCount > 0 && "animate-pulse-subtle"
                )}
              >
                <Package className="w-3.5 h-3.5" />
                <span className="tabular-nums">{duplicateCount}</span>
                <span className="hidden sm:inline">Dupes</span>
              </button>
            )}
            
            {/* To be fixed badge */}
            {toBeFixedCount > 0 && (
              <button
                onClick={() => navigate('/library')}
                className={cn(
                  "flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium transition-all",
                  "bg-primary/15 text-primary hover:bg-primary/25"
                )}
              >
                <Wrench className="w-3.5 h-3.5" />
                <span className="tabular-nums">{toBeFixedCount}</span>
                <span className="hidden sm:inline">To Fix</span>
              </button>
            )}
            
            {/* Incomplete badge */}
            {incompleteCount > 0 && (
              <button
                onClick={() => navigate('/broken-albums')}
                className={cn(
                  "flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium transition-all",
                  "bg-destructive/15 text-destructive hover:bg-destructive/25"
                )}
              >
                <AlertTriangle className="w-3.5 h-3.5" />
                <span className="tabular-nums">{incompleteCount}</span>
                <span className="hidden sm:inline">Incomplete</span>
              </button>
            )}
            
            {/* Artists/Albums counts */}
            <div className="hidden lg:flex items-center gap-3 text-sm text-muted-foreground">
              <button
                onClick={() => navigate('/library')}
                className="hover:text-foreground transition-colors"
              >
                <span className="font-semibold text-foreground tabular-nums">{artistCount}</span>
                {' '}Artists
              </button>
              <span className="text-border">·</span>
              <button
                onClick={() => navigate('/library')}
                className="hover:text-foreground transition-colors"
              >
                <span className="font-semibold text-foreground tabular-nums">{albumCount}</span>
                {' '}Albums
              </button>
            </div>
          </div>
        </div>
        {showLogs && (
          <div className="border-t border-border/80 bg-muted/30">
            <div className="px-4 py-2 flex items-center justify-between gap-3 text-xs text-muted-foreground">
              <span>Live backend log (last 8 lines, newest first)</span>
              <Button
                variant="ghost"
                size="sm"
                className="h-7 px-2 text-xs gap-1.5"
                onClick={() => { window.location.href = '/api/logs/download?lines=20000'; }}
              >
                <Download className="w-3.5 h-3.5" />
                Download
              </Button>
            </div>
            <div className="px-4 pb-3">
              <div className="rounded-md border border-border bg-background/80 p-3 font-mono text-xs leading-5 h-40 overflow-hidden">
                {logLines.length === 0 ? (
                  <div className="text-muted-foreground">No logs yet…</div>
                ) : (
                  logLines.map((line, idx) => (
                    <div key={`${idx}-${line}`} className="truncate">
                      {line}
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
