import { Slider } from '@/components/ui/slider';
import { cn } from '@/lib/utils';

interface GridSizeControlProps {
  value: number;
  onChange: (value: number) => void;
  className?: string;
}

export function GridSizeControl({ value, onChange, className }: GridSizeControlProps) {
  return (
    <div className={cn('flex items-center gap-3 min-w-[220px]', className)}>
      <span className="text-xs text-muted-foreground shrink-0">Size</span>
      <Slider
        value={[value]}
        min={150}
        max={340}
        step={10}
        onValueChange={(next) => {
          const candidate = Number(next?.[0] || 0);
          if (candidate > 0) onChange(candidate);
        }}
      />
    </div>
  );
}
