import { Link } from 'react-router-dom';
import { cn } from '@/lib/utils';

interface LogoProps {
  className?: string;
  showText?: boolean;
  size?: 'sm' | 'md' | 'lg';
}

export function Logo({ className, showText = true, size = 'md' }: LogoProps) {
  const sizes = {
    sm: { icon: 'w-8 h-8', text: 'text-lg' },
    md: { icon: 'w-10 h-10', text: 'text-xl' },
    lg: { icon: 'w-12 h-12', text: 'text-2xl' },
  };

  return (
    <Link 
      to="/" 
      className={cn("flex items-center gap-2.5 hover:opacity-90 transition-opacity", className)}
      aria-label="PMDA Home"
    >
      {/* Logo SVG - P with duplicate shadow effect */}
      <div className={cn("rounded-xl overflow-hidden flex-shrink-0", sizes[size].icon)}>
        <svg 
          viewBox="0 0 512 512" 
          fill="none" 
          xmlns="http://www.w3.org/2000/svg"
          className="w-full h-full"
        >
          {/* Background */}
          <rect width="512" height="512" rx="96" fill="hsl(var(--primary))"/>
          
          {/* Duplicate shadow P (offset) - represents dedupe concept */}
          <path 
            d="M200 136h100c55.23 0 100 44.77 100 100s-44.77 100-100 100h-60v80c0 11.05-8.95 20-20 20s-20-8.95-20-20V156c0-11.05 8.95-20 20-20z" 
            fill="hsl(var(--primary))"
            opacity="0.6"
            transform="translate(12, 12)"
          />
          
          {/* Main P */}
          <path 
            d="M188 124h100c55.23 0 100 44.77 100 100s-44.77 100-100 100h-60v80c0 11.05-8.95 20-20 20s-20-8.95-20-20V144c0-11.05 8.95-20 20-20z" 
            fill="hsl(var(--primary-foreground))"
          />
          <path 
            d="M228 164v120h60c33.14 0 60-26.86 60-60s-26.86-60-60-60h-60z" 
            fill="hsl(var(--primary))"
          />
        </svg>
      </div>
      
      {showText && (
        <span className={cn("font-bold tracking-tight text-foreground", sizes[size].text)}>
          PMDA
        </span>
      )}
    </Link>
  );
}
