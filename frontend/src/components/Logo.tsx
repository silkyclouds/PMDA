import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { cn } from '@/lib/utils';

interface LogoProps {
  className?: string;
  showText?: boolean;
  variant?: 'icon' | 'wordmark';
  size?: 'sm' | 'md' | 'lg' | 'xl';
}

export function Logo({ className, showText = true, variant, size = 'md' }: LogoProps) {
  const resolvedVariant = variant ?? (showText ? 'wordmark' : 'icon');
  const sizes = {
    sm: { icon: 'w-5 h-5', wordmark: 'h-7 w-auto max-w-[140px]' },
    md: { icon: 'w-10 h-10', wordmark: 'h-8 w-auto max-w-[170px]' },
    lg: { icon: 'w-12 h-12', wordmark: 'h-10 w-auto max-w-[220px]' },
    xl: { icon: 'w-14 h-14', wordmark: 'h-14 w-auto max-w-[320px]' },
  };
  const sources = useMemo(
    () =>
      Array.from(
        new Set(
          resolvedVariant === 'icon'
            ? [
                '/pmda-p-icon-transparent-1024.png',
                '/static/PMDA-p-mute-v1-transparent-1024.png',
                '/icon-192.png',
              ]
            : [
                '/pmda-logo-mute-v1-transparent-cropped.png',
                '/pmda-logo-mute-v1-transparent.png',
                '/static/PMDA-mute-v1-transparent-cropped.png',
                '/static/PMDA-mute-v1-transparent.png',
                '/pmda-p-icon-transparent-1024.png',
              ]
        )
      ),
    [resolvedVariant]
  );
  const [srcIndex, setSrcIndex] = useState(0);
  useEffect(() => {
    setSrcIndex(0);
  }, [resolvedVariant]);
  const src = sources[Math.min(srcIndex, sources.length - 1)] ?? '';
  const dimension = resolvedVariant === 'icon' ? sizes[size].icon : sizes[size].wordmark;

  return (
    <Link 
      to="/library" 
      className={cn('inline-flex items-center hover:opacity-90 transition-opacity', className)}
      aria-label="PMDA Home"
    >
      <img
        src={src}
        alt="PMDA"
        className={cn('object-contain shrink-0 select-none', dimension)}
        loading="eager"
        decoding="async"
        onError={() => {
          setSrcIndex((prev) => (prev < sources.length - 1 ? prev + 1 : prev));
        }}
      />
    </Link>
  );
}
