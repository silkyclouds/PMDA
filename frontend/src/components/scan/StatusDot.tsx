import { cn } from '@/lib/utils';

type StatusDotState = 'running' | 'preparing' | 'active' | 'paused' | 'degraded' | 'error' | 'success' | 'idle';

interface StatusDotProps {
  state?: StatusDotState;
  className?: string;
  pulse?: boolean;
  label?: string;
}

function colorsForState(state: StatusDotState) {
  switch (state) {
    case 'running':
    case 'success':
      return 'bg-success';
    case 'preparing':
    case 'active':
      return 'bg-info';
    case 'paused':
    case 'degraded':
      return 'bg-warning';
    case 'error':
      return 'bg-destructive';
    case 'idle':
    default:
      return 'bg-muted-foreground';
  }
}

export function StatusDot({ state = 'idle', className, pulse, label }: StatusDotProps) {
  const shouldPulse = pulse ?? ['running', 'preparing', 'active'].includes(state);
  const colorClass = colorsForState(state);
  return (
    <span
      className={cn('relative inline-flex h-2.5 w-2.5 shrink-0', className)}
      aria-label={label || state}
      role="img"
    >
      {shouldPulse ? <span className={cn('absolute inset-0 rounded-full animate-ping opacity-75', colorClass)} /> : null}
      <span className={cn('relative inline-flex h-2.5 w-2.5 rounded-full', colorClass)} />
    </span>
  );
}
