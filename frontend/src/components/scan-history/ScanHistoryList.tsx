import { format, isToday, isYesterday, isSameDay } from 'date-fns';
import { Clock, CheckCircle2, XCircle, AlertCircle, RefreshCw, Trash2, Package } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { ScanHistoryEntry } from '@/lib/api';

interface ScanHistoryListProps {
  history: ScanHistoryEntry[];
  selectedScan: number | null;
  onSelectScan: (scanId: number | null) => void;
}

function formatDateGroup(date: Date): string {
  if (isToday(date)) return 'Today';
  if (isYesterday(date)) return 'Yesterday';
  return format(date, 'EEEE, MMMM d');
}

export function ScanHistoryList({ history, selectedScan, onSelectScan }: ScanHistoryListProps) {
  const formatDuration = (seconds?: number) => {
    if (!seconds) return 'N/A';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    if (hours > 0) return `${hours}h ${minutes}m ${secs}s`;
    if (minutes > 0) return `${minutes}m ${secs}s`;
    return `${secs}s`;
  };

  const isDedupe = (entry: ScanHistoryEntry) => entry.entry_type === 'dedupe';

  // Group entries by date
  const groupedHistory = history.reduce((groups, entry) => {
    const date = new Date(entry.start_time * 1000);
    const dateKey = format(date, 'yyyy-MM-dd');
    if (!groups[dateKey]) {
      groups[dateKey] = { date, entries: [] };
    }
    groups[dateKey].entries.push(entry);
    return groups;
  }, {} as Record<string, { date: Date; entries: ScanHistoryEntry[] }>);

  const sortedGroups = Object.values(groupedHistory).sort((a, b) => b.date.getTime() - a.date.getTime());

  return (
    <div className="rounded-lg border bg-card">
      <div className="p-4 border-b">
        <h2 className="text-lg font-semibold">History</h2>
        <p className="text-xs text-muted-foreground mt-1">Scans and dedupes</p>
      </div>
      
      {history.length === 0 ? (
        <div className="p-8 text-center text-muted-foreground">
          No history available
        </div>
      ) : (
        <div className="p-4">
          {sortedGroups.map((group) => (
            <div key={format(group.date, 'yyyy-MM-dd')} className="mb-6 last:mb-0">
              {/* Date group header */}
              <div className="flex items-center gap-3 mb-3">
                <span className="text-sm font-semibold text-foreground">
                  {formatDateGroup(group.date)}
                </span>
                <div className="flex-1 h-px bg-border" />
              </div>
              
              {/* Timeline entries */}
              <div className="relative pl-6">
                {/* Vertical timeline line */}
                <div className="absolute left-2 top-3 bottom-3 w-px bg-border" />
                
                {group.entries.map((scan, idx) => (
                  <button
                    key={scan.scan_id}
                    onClick={() => onSelectScan(scan.scan_id === selectedScan ? null : scan.scan_id)}
                    className={cn(
                      "relative w-full text-left mb-3 last:mb-0 p-3 rounded-lg transition-all",
                      "hover:bg-accent/50",
                      selectedScan === scan.scan_id && "bg-accent ring-1 ring-primary/30"
                    )}
                  >
                    {/* Timeline dot */}
                    <div className={cn(
                      "absolute -left-4 top-4 w-3 h-3 rounded-full border-2 border-background",
                      isDedupe(scan) ? "bg-primary" : "bg-muted-foreground",
                      scan.status === 'completed' && "bg-success",
                      scan.status === 'failed' && "bg-destructive",
                      scan.status === 'cancelled' && "bg-warning"
                    )} />
                    
                    <div className="flex items-start justify-between gap-3">
                      <div className="flex-1 min-w-0">
                        {/* Entry type badge + time */}
                        <div className="flex items-center gap-2 flex-wrap mb-1">
                          <span
                            className={cn(
                              "inline-flex items-center gap-1 text-xs font-medium px-2 py-0.5 rounded",
                              isDedupe(scan)
                                ? "bg-primary/15 text-primary"
                                : "bg-muted text-muted-foreground"
                            )}
                          >
                            {isDedupe(scan) ? (
                              <>
                                <Package className="w-3 h-3" />
                                Dedupe
                              </>
                            ) : (
                              <>
                                <RefreshCw className="w-3 h-3" />
                                Scan
                              </>
                            )}
                          </span>
                          <span className="text-sm font-medium text-foreground">
                            {format(new Date(scan.start_time * 1000), 'h:mm a')}
                          </span>
                          {scan.status === 'completed' && (
                            <CheckCircle2 className="w-3.5 h-3.5 text-success" />
                          )}
                          {scan.status === 'failed' && (
                            <XCircle className="w-3.5 h-3.5 text-destructive" />
                          )}
                          {scan.status === 'cancelled' && (
                            <AlertCircle className="w-3.5 h-3.5 text-warning" />
                          )}
                        </div>
                        
                        {/* Stats row */}
                        <div className="flex items-center gap-3 text-xs text-muted-foreground flex-wrap">
                          <span className="inline-flex items-center gap-1">
                            <Clock className="w-3 h-3" />
                            {formatDuration(scan.duration_seconds)}
                          </span>
                          {isDedupe(scan) ? (
                            <>
                              {scan.albums_moved > 0 && (
                                <span className="text-success">{scan.albums_moved} moved</span>
                              )}
                              {scan.space_saved_mb > 0 && (
                                <span className="text-success">{scan.space_saved_mb >= 1024 ? `${(scan.space_saved_mb / 1024).toFixed(1)} GB` : `${scan.space_saved_mb} MB`} saved</span>
                              )}
                            </>
                          ) : (
                            <>
                              <span>{scan.albums_scanned.toLocaleString()} albums</span>
                              {scan.duplicates_found > 0 && (
                                <span className="text-warning font-medium">{scan.duplicates_found} duplicates</span>
                              )}
                            </>
                          )}
                        </div>
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
