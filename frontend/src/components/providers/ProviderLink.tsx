import type { MouseEvent } from 'react';

import { getProviderMeta } from '@/lib/providerMeta';
import { cn } from '@/lib/utils';

import { ProviderBadge } from './ProviderBadge';

interface ProviderLinkProps {
  provider?: string | null;
  href: string;
  prefix?: string;
  labelOverride?: string;
  className?: string;
  variant?: 'default' | 'secondary' | 'destructive' | 'outline';
  onClick?: (event: MouseEvent<HTMLAnchorElement>) => void;
}

export function ProviderLink({
  provider,
  href,
  prefix,
  labelOverride,
  className,
  variant = 'outline',
  onClick,
}: ProviderLinkProps) {
  const meta = getProviderMeta(provider);
  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer"
      className={cn('inline-flex', className)}
      title={`Open on ${labelOverride || meta.label}`}
      onClick={onClick}
    >
      <ProviderBadge
        provider={provider}
        prefix={prefix}
        labelOverride={labelOverride}
        variant={variant}
        external
        className="hover:bg-muted transition-colors"
      />
    </a>
  );
}
