import { memo, useState } from 'react';
import { Loader2, Trash2, Sparkles, FileSearch } from 'lucide-react';
import { Checkbox } from '@/components/ui/checkbox';
import { Button } from '@/components/ui/button';
import { FormatBadge } from '@/components/FormatBadge';
import { cn } from '@/lib/utils';
import type { DuplicateCard } from '@/lib/api';
import type { ListMode } from '@/components/ListModeToggle';

interface DuplicateTableProps {
  duplicates: DuplicateCard[];
  selectedIds: Set<string>;
  onSelect: (key: string) => void;
  onSelectAll: () => void;
  onClearSelection: () => void;
  onOpen: (dup: DuplicateCard) => void;
  onDedupe: (dup: DuplicateCard) => void;
  dedupingId: string | null;
  listMode: ListMode;
}

const TableRow = memo(function TableRow({
  dup,
  isSelected,
  onSelect,
  onOpen,
  onDedupe,
  isDeduping,
  listMode,
}: {
  dup: DuplicateCard;
  isSelected: boolean;
  onSelect: () => void;
  onOpen: () => void;
  onDedupe: () => void;
  isDeduping: boolean;
  listMode: ListMode;
}) {
  const [coverBroken, setCoverBroken] = useState(false);
  const formatSize = (bytes: number) => {
    if (!bytes) return '—';
    const mb = bytes / (1024 * 1024);
    return `${mb.toFixed(1)} MB`;
  };

  return (
    <tr 
      className={cn(
        "group transition-colors cursor-pointer",
        isSelected ? "bg-primary/10" : "hover:bg-muted/50"
      )}
      onClick={onOpen}
    >
      <td className="px-3 py-3" onClick={(e) => e.stopPropagation()}>
        <Checkbox
          checked={isSelected}
          onCheckedChange={onSelect}
          aria-label={`Select ${dup.artist} - ${dup.best_title}`}
        />
      </td>
      <td className="px-3 py-3">
        <div className="w-10 h-10 rounded overflow-hidden bg-muted flex-shrink-0">
          {dup.best_thumb && !coverBroken ? (
            <img
              src={dup.best_thumb}
              alt=""
              className="w-full h-full object-cover"
              loading="lazy"
              onError={() => setCoverBroken(true)}
            />
          ) : (
            <div className="w-full h-full flex items-center justify-center">
              <FileSearch className="w-4 h-4 text-muted-foreground/50" />
            </div>
          )}
        </div>
      </td>
      <td className="px-3 py-3 font-medium text-foreground">
        {dup.artist}
      </td>
      <td className="px-3 py-3 text-foreground">
        {dup.best_title}
      </td>
      <td className="px-3 py-3 text-center">
        <span className="inline-flex items-center justify-center min-w-6 h-6 rounded-full bg-muted text-xs font-medium px-2">
          {dup.n}
        </span>
      </td>
      <td className="px-3 py-3">
        <div className="flex flex-col gap-1">
          <div className="flex flex-wrap gap-1">
            {dup.formats.slice(0, 2).map((fmt) => (
              <FormatBadge key={fmt} format={fmt} size="sm" />
            ))}
            {dup.formats.length > 2 && (
              <span className="text-xs text-muted-foreground">+{dup.formats.length - 2}</span>
            )}
          </div>
          {/* Quality indicator - show best specs */}
          {(dup.br || dup.sr || dup.bd) && (
            <span className="text-[10px] text-muted-foreground">
              {dup.br && `${dup.br}kbps`}
              {dup.bd && ` · ${dup.bd}-bit`}
            </span>
          )}
        </div>
      </td>
      <td className="px-3 py-3 text-right text-sm text-muted-foreground tabular-nums">
        {formatSize(dup.size || 0)}
      </td>
      <td className="px-3 py-3 text-center text-sm text-muted-foreground tabular-nums">
        {dup.track_count || '—'}
      </td>
      {/* AI recommendation badge */}
      {dup.used_ai && (
        <td className="px-3 py-3">
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-primary/10 text-primary text-[10px] font-medium">
            <Sparkles className="w-3 h-3" />
            AI
          </span>
        </td>
      )}
      {listMode === 'detailed' && (
        <>
          <td className="px-3 py-3 text-sm text-muted-foreground max-w-[200px] truncate" title={dup.path || ''}>
            <code className="text-xs">{dup.path || '—'}</code>
          </td>
          <td className="px-3 py-3 text-sm text-muted-foreground text-right">
            {dup.br ? `${dup.br} kbps` : '—'}
          </td>
          <td className="px-3 py-3 text-sm text-muted-foreground text-right">
            {dup.sr ? `${(dup.sr / 1000).toFixed(1)} kHz` : '—'}
          </td>
          <td className="px-3 py-3 text-sm text-muted-foreground text-right">
            {dup.bd ? `${dup.bd}-bit` : '—'}
          </td>
        </>
      )}
      <td className="px-3 py-3" onClick={(e) => e.stopPropagation()}>
        {dup.no_move ? (
          <Button
            size="sm"
            variant="outline"
            onClick={onOpen}
            className="gap-1"
            title="Manual review required for this duplicate group"
          >
            Review
          </Button>
        ) : (
          <Button
            size="sm"
            variant="ghost"
            onClick={onDedupe}
            disabled={isDeduping}
            className="gap-1 opacity-0 group-hover:opacity-100 transition-opacity"
          >
            {isDeduping ? (
              <Loader2 className="w-3.5 h-3.5 animate-spin" />
            ) : (
              <Trash2 className="w-3.5 h-3.5" />
            )}
            Undupe
          </Button>
        )}
      </td>
    </tr>
  );
});

export const DuplicateTable = memo(function DuplicateTable({
  duplicates,
  selectedIds,
  onSelect,
  onSelectAll,
  onClearSelection,
  onOpen,
  onDedupe,
  dedupingId,
  listMode,
}: DuplicateTableProps) {
  const allSelected = duplicates.length > 0 && duplicates.every(
    d => selectedIds.has(`${d.artist_key}||${d.album_id}`)
  );

  const handleSelectAll = () => {
    if (allSelected) {
      onClearSelection();
    } else {
      onSelectAll();
    }
  };

  return (
    <div className="rounded-lg border border-border overflow-hidden bg-card">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-muted/50 border-b border-border">
            <tr>
              <th className="px-3 py-3 text-left">
                <Checkbox
                  checked={allSelected}
                  onCheckedChange={handleSelectAll}
                  aria-label="Select all"
                />
              </th>
              <th className="px-3 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Cover
              </th>
              <th className="px-3 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Artist
              </th>
              <th className="px-3 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Album
              </th>
              <th className="px-3 py-3 text-center text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Ver
              </th>
              <th className="px-3 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Quality
              </th>
              <th className="px-3 py-3 text-right text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Size
              </th>
              <th className="px-3 py-3 text-center text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Tracks
              </th>
              {listMode === 'detailed' && (
                <>
                  <th className="px-3 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">
                    Path
                  </th>
                  <th className="px-3 py-3 text-right text-xs font-medium text-muted-foreground uppercase tracking-wider">
                    Bitrate
                  </th>
                  <th className="px-3 py-3 text-right text-xs font-medium text-muted-foreground uppercase tracking-wider">
                    Sample Rate
                  </th>
                  <th className="px-3 py-3 text-right text-xs font-medium text-muted-foreground uppercase tracking-wider">
                    Bit Depth
                  </th>
                </>
              )}
              <th className="px-3 py-3 text-right text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {duplicates.map((dup) => {
              const key = `${dup.artist_key}||${dup.album_id}`;
              return (
                <TableRow
                  key={key}
                  dup={dup}
                  isSelected={selectedIds.has(key)}
                  onSelect={() => onSelect(key)}
                  onOpen={() => onOpen(dup)}
                  onDedupe={() => onDedupe(dup)}
                  isDeduping={dedupingId === key}
                  listMode={listMode}
                />
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
});
