import { Slider } from '@/components/ui/slider';
import { cn } from '@/lib/utils';
import { useIsMobile } from '@/hooks/use-mobile';

interface GridSizeControlProps {
  value: number;
  onChange: (value: number) => void;
  className?: string;
}

export function GridSizeControl({ value, onChange, className }: GridSizeControlProps) {
  const isMobile = useIsMobile();

  if (isMobile) return null;

  return (
    <div className={cn('flex w-full min-w-0 items-center gap-3', className)}>
      <span className="text-xs text-muted-foreground shrink-0">Size</span>
      <Slider
        className="min-w-[120px] flex-1"
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
