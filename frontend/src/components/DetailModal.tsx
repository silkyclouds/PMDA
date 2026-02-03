import { useState, useEffect } from 'react';
import { X, Loader2, Trash2, Sparkles, GitMerge, Music } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { RadioGroup } from '@/components/ui/radio-group';
import { useDuplicateDetails } from '@/hooks/usePMDA';
import { cn } from '@/lib/utils';
import * as api from '@/lib/api';

// Sub-components
import { EditionColumn } from '@/components/detail-modal/EditionColumn';
import { TrackComparisonTable } from '@/components/detail-modal/TrackComparisonTable';
import { PlexLink } from '@/components/detail-modal/PlexLink';

interface DetailModalProps {
  artist: string;
  albumId: string;
  onClose: () => void;
  onDedupe: () => Promise<void>;
  /** When true, group was detected by name only (no scan best/loser) — show message, no Dedupe */
  no_move?: boolean;
  best_title?: string;
}

export function DetailModal({ artist, albumId, onClose, onDedupe, no_move, best_title }: DetailModalProps) {
  const { data: details, isLoading, error, refetch } = useDuplicateDetails(artist, albumId, !no_move);
  const [isDeduping, setIsDeduping] = useState(false);
  const [selectedEdition, setSelectedEdition] = useState<string>('0');
  const [showTracks, setShowTracks] = useState(true);

  // Reset selection when details load
  useEffect(() => {
    if (details?.editions?.length) {
      setSelectedEdition('0'); // Default to first (best) edition
    }
  }, [details]);

  // Handle Escape key to close modal
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };
    window.addEventListener('keydown', handleEscape);
    return () => window.removeEventListener('keydown', handleEscape);
  }, [onClose]);

  const handleDedupe = async () => {
    setIsDeduping(true);
    try {
      const keepId = details?.editions?.[selectedEditionIndex]?.album_id;
      if (selectedEdition !== '0' && keepId != null) {
        await api.dedupeArtist(artist, albumId, { keep_edition_album_id: keepId });
      } else {
        await onDedupe();
      }
      onClose();
    } finally {
      setIsDeduping(false);
    }
  };

  const handleTrackMoved = () => {
    refetch();
  };

  // Parse rationale into bullets
  const rationaleItems = details?.rationale
    ? details.rationale.split(';').filter(Boolean).map(s => s.trim())
    : [];

  const hasMergeTracks = details?.merge_list && details.merge_list.length > 0;
  const selectedEditionIndex = parseInt(selectedEdition);
  const hasTracks = details?.editions?.some(e => e.tracks && e.tracks.length > 0);

  return (
    <>
      {/* Overlay */}
      <div className="modal-overlay" onClick={onClose} />

      {/* Content */}
      <div className="modal-content">
        {/* Header */}
        <div className="sticky top-0 z-10 flex items-center justify-between gap-4 p-4 border-b border-border bg-card">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-3 flex-wrap">
              <h2 className="text-lg font-semibold text-foreground truncate">
                {no_move ? `${artist.replace(/_/g, ' ')} – ${best_title ?? 'Unknown'}` : (details?.artist || artist) + ' – ' + (details?.album || 'Loading...')}
              </h2>
              {details?.editions && (
                <PlexLink 
                  artistId={details.artist_id}
                  editions={details.editions} 
                  selectedEditionIndex={selectedEditionIndex} 
                />
              )}
            </div>
            <p className="text-sm text-muted-foreground mt-1">
              Select which edition to keep. Others will be moved to duplicates folder.
            </p>
          </div>
          <Button
            size="icon"
            variant="ghost"
            onClick={onClose}
            className="flex-shrink-0"
          >
            <X className="w-5 h-5" />
          </Button>
        </div>

        {/* Body — full width, no horizontal overflow */}
        <div className="p-4 md:p-6 space-y-6 w-full min-w-0 overflow-x-hidden">
          {no_move && (
            <div className="p-4 rounded-lg bg-muted/50 border border-border text-center text-muted-foreground">
              <p className="font-medium text-foreground mb-1">Duplicate detected by name only</p>
              <p className="text-sm">Run a scan to choose which edition to keep and move the others.</p>
            </div>
          )}
          {!no_move && isLoading && (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-8 h-8 animate-spin text-primary" />
            </div>
          )}

          {!no_move && error && (
            <div className="p-4 rounded-lg bg-destructive/10 text-destructive text-center">
              Failed to load details. Please try again.
            </div>
          )}

          {!no_move && details && (
            <>
              {/* Rationale */}
              {rationaleItems.length > 0 && (
                <div className="p-4 rounded-lg bg-primary/5 border border-primary/20">
                  <div className="flex items-center gap-2 mb-2">
                    <Sparkles className="w-4 h-4 text-primary" />
                    <h3 className="text-sm font-medium text-foreground">
                      AI Decision Rationale
                    </h3>
                  </div>
                  <ul className="space-y-1 text-sm text-muted-foreground">
                    {rationaleItems.map((item, i) => (
                      <li key={i} className="flex items-start gap-2">
                        <span className="text-primary mt-1">•</span>
                        {item}
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Merge tracks info */}
              {hasMergeTracks && (
                <div className="p-4 rounded-lg bg-warning/5 border border-warning/20">
                  <div className="flex items-center gap-2 mb-2">
                    <GitMerge className="w-4 h-4 text-warning" />
                    <h3 className="text-sm font-medium text-foreground">
                      Extra Tracks Detected
                    </h3>
                  </div>
                  <ul className="space-y-1 text-sm text-muted-foreground">
                    {details.merge_list.map((track, i) => (
                      <li key={i}>{track}</li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Editions side-by-side — one column per edition, no wrap */}
              <div className="space-y-3 w-full min-w-0">
                <h3 className="text-sm font-medium text-foreground">
                  Choose Edition to Keep ({details.editions.length} available)
                </h3>
                <div className="flex flex-nowrap gap-4 w-full min-w-0">
                  <RadioGroup
                    value={selectedEdition}
                    onValueChange={setSelectedEdition}
                    className="contents"
                  >
                    {details.editions.map((edition, index) => (
                      <EditionColumn
                        key={index}
                        edition={edition}
                        index={index}
                        isSelected={index === selectedEditionIndex}
                        totalEditions={details.editions.length}
                      />
                    ))}
                  </RadioGroup>
                </div>
              </div>

              {/* Track comparison table */}
              {hasTracks && (
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <h3 className="text-sm font-medium text-foreground flex items-center gap-2">
                      <Music className="w-4 h-4" />
                      Track Comparison
                    </h3>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => setShowTracks(!showTracks)}
                      className="h-7 text-xs"
                    >
                      {showTracks ? 'Hide tracks' : 'Show tracks'}
                    </Button>
                  </div>
                  
                  {showTracks && (
                    <TrackComparisonTable
                      editions={details.editions}
                      selectedEditionIndex={selectedEditionIndex}
                      artist={details.artist}
                      albumId={albumId}
                      mergeList={details.merge_list ?? []}
                      onTrackMoved={handleTrackMoved}
                    />
                  )}
                </div>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        <div className="sticky bottom-0 flex items-center justify-between gap-3 p-4 border-t border-border bg-card">
          <p className="text-xs text-muted-foreground">
            {!no_move && selectedEdition !== '0' && (
              <span className="text-warning">⚠ You've overridden the AI recommendation</span>
            )}
          </p>
          <div className="flex items-center gap-3">
            <Button variant="outline" onClick={onClose}>
              Cancel
            </Button>
            {!no_move && (
              <Button
                onClick={handleDedupe}
                disabled={isDeduping || isLoading}
                className="gap-1.5"
              >
                {isDeduping ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Trash2 className="w-4 h-4" />
                )}
                Undupe
              </Button>
            )}
          </div>
        </div>
      </div>
    </>
  );
}
