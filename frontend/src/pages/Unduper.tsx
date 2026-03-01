import { useState, useMemo, useCallback, useEffect, useRef } from 'react';
import { Trash2, Loader2, GitMerge } from 'lucide-react';
import { toast } from 'sonner';
import { useQueryClient } from '@tanstack/react-query';
import { SearchInput } from '@/components/SearchInput';
import { ListModeToggle, type ListMode } from '@/components/ListModeToggle';
import { DuplicateTable } from '@/components/DuplicateTable';
import { DetailModal } from '@/components/DetailModal';
import { Pagination } from '@/components/Pagination';
import { EmptyState } from '@/components/EmptyState';
import { Button } from '@/components/ui/button';
import {
  useDuplicates,
  useScanProgress,
  useDedupeProgress,
  useScanControls,
  useDedupeActions,
  useSelection,
} from '@/hooks/usePMDA';
import { getLibraryStats } from '@/lib/api';
import { useQuery } from '@tanstack/react-query';
import type { DuplicateCard as DuplicateCardType, DedupeCurrentGroup } from '@/lib/api';
import { formatETA } from '@/lib/utils';

const ITEMS_PER_PAGE = 50;

function DedupeCurrentGroupCard({ group }: { group: DedupeCurrentGroup }) {
  const { artist, album, num_dupes, winner, losers, destination, status } = group;
  return (
    <div className="rounded-lg border border-border bg-muted/40 px-4 py-3 space-y-2">
      <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
        Current group · {status}
      </p>
      <p className="text-sm font-medium text-foreground">
        {artist} – {album}
      </p>
      <p className="text-xs text-muted-foreground">
        {num_dupes} version(s): 1 winner, {losers.length} loser(s)
      </p>
      <div className="grid gap-1 text-xs">
        <div className="flex flex-wrap items-baseline gap-x-2">
          <span className="text-muted-foreground">Winner:</span>
          <span className="text-foreground font-medium">{winner.title_raw}</span>
          {winner.folder && <span className="text-muted-foreground truncate max-w-[40ch]" title={winner.folder}>{winner.folder}</span>}
        </div>
        {losers.length > 0 && (
          <div className="flex flex-wrap gap-x-2 gap-y-0.5">
            <span className="text-muted-foreground shrink-0">Losers:</span>
            {losers.map((l, i) => (
              <span key={i} className="text-foreground/90">
                {l.title_raw}
                {i < losers.length - 1 ? ',' : ''}
              </span>
            ))}
          </div>
        )}
        <div className="flex flex-wrap items-baseline gap-x-2">
          <span className="text-muted-foreground">Destination:</span>
          <span className="text-foreground font-mono text-[11px]" title={destination}>{destination}</span>
        </div>
      </div>
    </div>
  );
}

export default function Unduper() {
  // View state
  const [listMode, setListMode] = useState<ListMode>(() => {
    return (localStorage.getItem('pmda-list-mode') as ListMode) || 'compact';
  });
  const [searchQuery, setSearchQuery] = useState('');
  const [currentPage, setCurrentPage] = useState(1);
  const [selectedDuplicate, setSelectedDuplicate] = useState<DuplicateCardType | null>(null);
  const [dedupingId, setDedupingId] = useState<string | null>(null);

  // Data hooks (refetch duplicates every 2s during scan so list grows as artists finish, 1s during dedupe)
  const { progress: scanProgress } = useScanProgress();
  const { data: dedupeProgress } = useDedupeProgress();
  const { data: duplicates = [], isLoading: loadingDuplicates } = useDuplicates({
    refetchInterval: scanProgress?.scanning ? 2000 : dedupeProgress?.deduping ? 1000 : 10000,
  });
  const { data: libraryStats, error: libraryStatsError } = useQuery({
    queryKey: ['library-stats'],
    queryFn: () => getLibraryStats(),
    staleTime: 60000,
    retry: 2,
  });
  const queryClient = useQueryClient();
  const scanControls = useScanControls();
  const { dedupeSingle, dedupeSelected, dedupeAll, dedupeMergeAndDedupe, isDeduping } = useDedupeActions();
  const selection = useSelection(duplicates);
  const wasDedupingRef = useRef(false);

  // When background dedupe finishes (deduping goes true -> false), refresh list and toast
  useEffect(() => {
    const deduping = dedupeProgress?.deduping ?? false;
    if (wasDedupingRef.current && !deduping) {
      queryClient.invalidateQueries({ queryKey: ['duplicates'] });
      queryClient.invalidateQueries({ queryKey: ['scan-history'] });
      toast.success('Undupe complete');
    }
    wasDedupingRef.current = deduping;
  }, [dedupeProgress?.deduping, queryClient]);

  // Toggle list mode and persist
  const handleListModeChange = (mode: ListMode) => {
    setListMode(mode);
    localStorage.setItem('pmda-list-mode', mode);
  };

  // Filter duplicates by search
  const filteredDuplicates = useMemo(() => {
    if (!searchQuery.trim()) return duplicates;
    const query = searchQuery.toLowerCase();
    return duplicates.filter(
      (d) =>
        d.artist.toLowerCase().includes(query) ||
        d.best_title.toLowerCase().includes(query)
    );
  }, [duplicates, searchQuery]);

  // Pagination (totalPages must be after filteredDuplicates)
  const totalPages = Math.ceil(filteredDuplicates.length / ITEMS_PER_PAGE);
  useEffect(() => {
    if (totalPages > 0 && currentPage > totalPages) {
      setCurrentPage(1);
    }
  }, [totalPages, currentPage]);

  const paginatedDuplicates = useMemo(() => {
    const start = (currentPage - 1) * ITEMS_PER_PAGE;
    return filteredDuplicates.slice(start, start + ITEMS_PER_PAGE);
  }, [filteredDuplicates, currentPage]);

  // Reset to page 1 when search changes
  const handleSearch = (query: string) => {
    setSearchQuery(query);
    setCurrentPage(1);
  };

  const showLibraryStatsHint = !loadingDuplicates && filteredDuplicates.length === 0 && libraryStatsError != null;

  // Dedupe handlers
  const handleDedupeSingle = useCallback(
    async (dup: DuplicateCardType) => {
      const key = `${dup.artist_key}||${dup.album_id}`;
      setDedupingId(key);
      try {
        const result = await dedupeSingle({ artist: dup.artist_key, albumId: dup.album_id });
        if (result?.status === 'started') {
          toast.success('Undupe started — progress below');
        } else if (result?.moved?.length != null) {
          toast.success(`Moved ${result.moved.length} duplicate(s)`);
        } else {
          toast.success('Undupe started');
        }
      } catch (error) {
        toast.error('Failed to undupe');
      } finally {
        setDedupingId(null);
      }
    },
    [dedupeSingle]
  );

  const handleDedupeSelected = async () => {
    if (selection.count === 0) return;
    const toDedupe = selection.selectedArray.filter((key) => {
      const dup = duplicates.find((d) => `${d.artist_key}||${d.album_id}` === key);
      return dup && !dup.no_move;
    });
    if (toDedupe.length === 0) {
      toast.error('Selected items need a scan first — run scan to choose edition to keep');
      return;
    }
    try {
      const result = await dedupeSelected(toDedupe);
      selection.clearSelection();
      toast.success(`Moved ${result.moved.length} duplicate(s)`);
    } catch (error) {
      toast.error('Failed to undupe selected');
    }
  };

  const handleDedupeAll = async () => {
    try {
      const result = await dedupeAll();
      const moved = (result as { moved?: unknown[] })?.moved;
      toast.success(moved?.length != null ? `Moved ${moved.length} duplicate(s)` : 'Undupe all started');
    } catch {
      toast.error('Failed to undupe all');
    }
  };

  const handleMergeAndDedupeAll = async () => {
    try {
      await dedupeMergeAndDedupe();
      toast.success('Merge & Undupe started — bonus tracks merged, then duplicates removed');
    } catch {
      toast.error('Failed to start Merge & Undupe');
    }
  };

  const handleModalDedupe = async () => {
    if (!selectedDuplicate) return;
    await handleDedupeSingle(selectedDuplicate);
  };

  return (
    <div className="container pb-6 space-y-6">
        {/* Dedupe in progress: banner + current group detail + percent & ETA */}
        {dedupeProgress?.deduping && (
          <div className="space-y-3">
            <div className="rounded-lg border border-primary/30 bg-primary/10 px-4 py-3 flex flex-wrap items-center gap-3">
              <Loader2 className="w-5 h-5 shrink-0 animate-spin text-primary" />
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-foreground">
                  Undupe in progress…
                </p>
                <p className="text-xs text-muted-foreground mt-0.5">
                  {dedupeProgress.progress} / {dedupeProgress.total} group(s)
                  {dedupeProgress.current_group && (
                    <> · <span className="font-medium text-foreground">{dedupeProgress.current_group.artist} – {dedupeProgress.current_group.album}</span></>
                  )}
                  {((dedupeProgress.saved_this_run ?? 0) > 0) && ` · ${dedupeProgress.saved_this_run} MB saved so far`}
                  {dedupeProgress.percent != null && dedupeProgress.total > 0 && ` · ${dedupeProgress.percent}%`}
                  {dedupeProgress.eta_seconds != null && dedupeProgress.eta_seconds > 0 && ` · ETA ${formatETA(dedupeProgress.eta_seconds)}`}
                </p>
              </div>
              <div className="w-32 h-2 rounded-full bg-muted overflow-hidden shrink-0">
                <div
                  className="h-full bg-primary transition-all duration-300"
                  style={{
                    width: dedupeProgress.total > 0
                      ? `${dedupeProgress.percent ?? Math.round((dedupeProgress.progress / dedupeProgress.total) * 100)}%`
                      : '0%',
                  }}
                />
              </div>
            </div>
            {dedupeProgress.current_group && (
              <DedupeCurrentGroupCard group={dedupeProgress.current_group} />
            )}
            {dedupeProgress.deduping && dedupeProgress.last_write && (
              <p className="text-xs text-muted-foreground font-mono">
                Last written to /dupes: {dedupeProgress.last_write.path} at {new Date(dedupeProgress.last_write.at * 1000).toLocaleTimeString()}
              </p>
            )}
          </div>
        )}
        {showLibraryStatsHint && (
          <p className="text-sm text-muted-foreground">
            Library stats unavailable. Run a scan or check your source folders.
          </p>
        )}

        {/* Actions row */}
        <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
          <div className="flex items-center gap-2 flex-wrap">
            {/* Primary CTA - Undupe ALL */}
            <Button
              onClick={handleDedupeAll}
              disabled={filteredDuplicates.length === 0 || isDeduping}
              className="gap-1.5 shadow-md hover:shadow-lg transition-shadow"
            >
              {isDeduping ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Trash2 className="w-4 h-4" />
              )}
              Undupe ALL
            </Button>
            
            {/* Secondary - Undupe Selected */}
            <Button
              variant="secondary"
              onClick={handleDedupeSelected}
              disabled={selection.count === 0 || isDeduping}
              className="gap-1.5"
            >
              {isDeduping ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Trash2 className="w-4 h-4" />
              )}
              Undupe Selected ({selection.count})
            </Button>
            
            {/* Destructive outline - Merge & Undupe */}
            <Button
              variant="outline"
              onClick={handleMergeAndDedupeAll}
              disabled={filteredDuplicates.length === 0 || isDeduping}
              className="gap-1.5 border-destructive/50 text-destructive hover:bg-destructive/10 hover:text-destructive hover:border-destructive"
            >
              <GitMerge className="w-4 h-4" />
              Merge & Undupe ALL
            </Button>
          </div>

          <div className="flex items-center gap-3 w-full sm:w-auto">
            <SearchInput
              value={searchQuery}
              onChange={handleSearch}
              className="flex-1 sm:w-64"
            />
            <ListModeToggle mode={listMode} onChange={handleListModeChange} />
          </div>
        </div>

        {/* Loading state */}
        {loadingDuplicates && (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="w-8 h-8 animate-spin text-primary" />
          </div>
        )}

        {/* Scan in progress (when list empty): show artists/dupe stats so user sees increment */}
        {!loadingDuplicates && filteredDuplicates.length === 0 && (scanProgress?.scanning ?? false) && (
          <div className="rounded-lg border border-primary/30 bg-primary/5 px-4 py-3 flex flex-wrap items-center gap-3 mb-6">
            <Loader2 className="w-5 h-5 shrink-0 animate-spin text-primary" />
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium text-foreground">
                Scan in progress…
              </p>
              <p className="text-xs text-muted-foreground mt-0.5 tabular-nums">
                {(scanProgress?.artists_processed ?? 0).toLocaleString()} / {(scanProgress?.artists_total ?? 0).toLocaleString()} artists
                {(scanProgress?.duplicate_groups_count ?? 0) > 0 && (
                  <> · <span className="font-medium text-foreground">{(scanProgress?.duplicate_groups_count ?? 0).toLocaleString()} duplicate group(s) so far</span></>
                )}
              </p>
            </div>
          </div>
        )}

        {/* Empty state */}
        {!loadingDuplicates && filteredDuplicates.length === 0 && (
          <EmptyState
            onStartScan={scanControls.start}
            isScanning={scanProgress?.scanning ?? false}
          />
        )}

        {/* Duplicates list */}
        {!loadingDuplicates && filteredDuplicates.length > 0 && (
          <>
            <DuplicateTable
              duplicates={paginatedDuplicates}
              selectedIds={selection.selected}
              onSelect={selection.toggle}
              onSelectAll={selection.selectAll}
              onClearSelection={selection.clearSelection}
              onOpen={setSelectedDuplicate}
              onDedupe={handleDedupeSingle}
              dedupingId={dedupingId}
              listMode={listMode}
            />

            <Pagination
              currentPage={currentPage}
              totalPages={totalPages}
              onPageChange={setCurrentPage}
            />
          </>
        )}
      {selectedDuplicate && (
        <DetailModal
          artist={selectedDuplicate.artist_key}
          albumId={selectedDuplicate.album_id}
          onClose={() => setSelectedDuplicate(null)}
          onDedupe={handleModalDedupe}
          no_move={selectedDuplicate.no_move}
          best_title={selectedDuplicate.best_title}
        />
      )}
    </div>
  );
}
