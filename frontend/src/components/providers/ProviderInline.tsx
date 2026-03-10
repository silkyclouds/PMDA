import { getProviderMeta } from '@/lib/providerMeta';
import { cn } from '@/lib/utils';

import { ProviderIcon } from './ProviderIcon';

interface ProviderInlineProps {
  label: string;
  provider?: string | null;
  className?: string;
}

export function ProviderInline({ label, provider, className }: ProviderInlineProps) {
  const meta = getProviderMeta(provider);
  return (
    <div className={cn('text-xs text-muted-foreground inline-flex items-center gap-1.5', className)}>
      <span>{label}</span>
      <ProviderIcon provider={meta.id} size={12} />
      <span>{meta.label}</span>
    </div>
  );
}

