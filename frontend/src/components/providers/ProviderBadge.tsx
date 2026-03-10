import { ExternalLink } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { providerBadgeClass } from '@/lib/badgeStyles';
import { getProviderMeta } from '@/lib/providerMeta';
import { cn } from '@/lib/utils';

import { ProviderIcon } from './ProviderIcon';

interface ProviderBadgeProps {
  provider?: string | null;
  prefix?: string;
  labelOverride?: string;
  className?: string;
  variant?: 'default' | 'secondary' | 'destructive' | 'outline';
  external?: boolean;
}

export function ProviderBadge({
  provider,
  prefix,
  labelOverride,
  className,
  variant = 'outline',
  external = false,
}: ProviderBadgeProps) {
  const meta = getProviderMeta(provider);
  const label = labelOverride || meta.label;
  return (
    <Badge variant={variant} className={cn('gap-1.5', providerBadgeClass(meta.id), className)}>
      <ProviderIcon provider={meta.id} size={12} />
      <span>{prefix ? `${prefix}: ${label}` : label}</span>
      {external ? <ExternalLink className="w-3 h-3" /> : null}
    </Badge>
  );
}

