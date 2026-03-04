import { Link } from 'react-router-dom';
import { cn } from '@/lib/utils';

interface LogoProps {
  className?: string;
  showText?: boolean;
  variant?: 'icon' | 'wordmark';
  size?: 'sm' | 'md' | 'lg';
}

export function Logo({ className, showText = true, variant, size = 'md' }: LogoProps) {
  const resolvedVariant = variant ?? (showText ? 'wordmark' : 'icon');
  const sizes = {
    sm: { icon: 'w-8 h-8', wordmark: 'h-7 w-auto max-w-[140px]' },
    md: { icon: 'w-10 h-10', wordmark: 'h-8 w-auto max-w-[170px]' },
    lg: { icon: 'w-12 h-12', wordmark: 'h-10 w-auto max-w-[220px]' },
  };
  const src = resolvedVariant === 'icon'
    ? '/pmda-p-icon-transparent-1024.png'
    : '/pmda-logo-mute-v1-transparent-cropped.png';
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
      />
    </Link>
  );
}
