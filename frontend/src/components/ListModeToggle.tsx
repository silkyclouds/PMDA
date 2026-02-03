import { List, ListCollapse } from 'lucide-react';
import { cn } from '@/lib/utils';

export type ListMode = 'compact' | 'detailed';

interface ListModeToggleProps {
  mode: ListMode;
  onChange: (mode: ListMode) => void;
  className?: string;
}

export function ListModeToggle({ mode, onChange, className }: ListModeToggleProps) {
  return (
    <div className={cn("view-toggle", className)}>
      <button
        onClick={() => onChange('compact')}
        className={cn(
          "view-toggle-button flex items-center gap-1.5",
          mode === 'compact' && "view-toggle-button-active"
        )}
        title="Compact view without details"
      >
        <ListCollapse className="w-4 h-4" />
        Without details
      </button>
      <button
        onClick={() => onChange('detailed')}
        className={cn(
          "view-toggle-button flex items-center gap-1.5",
          mode === 'detailed' && "view-toggle-button-active"
        )}
        title="Detailed view with path, bitrate, etc."
      >
        <List className="w-4 h-4" />
        With details
      </button>
    </div>
  );
}
