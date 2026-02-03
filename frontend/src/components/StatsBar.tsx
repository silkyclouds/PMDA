import { Users, Disc3, Trash2, Clock, HardDrive } from 'lucide-react';
import { cn } from '@/lib/utils';

interface StatsBarProps {
  artists: number;
  albums: number;
  remainingDupes: number;
  removedDupes: number;
  spaceSaved: number;
  className?: string;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 MB';
  const mb = bytes / (1024 * 1024);
  if (mb >= 1000) {
    return `${(mb / 1024).toFixed(1)} GB`;
  }
  return `${mb.toFixed(1)} MB`;
}

function StatItem({ 
  icon: Icon, 
  value, 
  label,
  highlight = false,
}: { 
  icon: React.ElementType; 
  value: string | number; 
  label: string;
  highlight?: boolean;
}) {
  return (
    <div className="stat-badge group">
      <div className={cn(
        "p-2 rounded-lg transition-colors",
        highlight ? "bg-primary/10" : "bg-muted"
      )}>
        <Icon className={cn(
          "w-4 h-4",
          highlight ? "text-primary" : "text-muted-foreground"
        )} />
      </div>
      <div className="flex flex-col">
        <span className={cn(
          "stat-value",
          highlight && "text-primary"
        )}>
          {typeof value === 'number' ? value.toLocaleString() : value}
        </span>
        <span className="stat-label">{label}</span>
      </div>
    </div>
  );
}

export function StatsBar({ 
  artists, 
  albums, 
  remainingDupes, 
  removedDupes, 
  spaceSaved,
  className,
}: StatsBarProps) {
  return (
    <div className={cn(
      "flex flex-wrap items-center gap-4",
      className
    )}>
      <StatItem icon={Users} value={artists} label="Artists" />
      <StatItem icon={Disc3} value={albums} label="Albums" />
      <StatItem 
        icon={Clock} 
        value={remainingDupes} 
        label="Remaining dupes" 
        highlight={remainingDupes > 0}
      />
      <StatItem icon={Trash2} value={removedDupes} label="Moved dupes" />
      <StatItem icon={HardDrive} value={formatBytes(spaceSaved)} label="Saved space" />
    </div>
  );
}
