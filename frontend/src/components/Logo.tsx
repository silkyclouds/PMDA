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
      to="/library" 
      className={cn("flex items-center gap-2.5 hover:opacity-90 transition-opacity", className)}
      aria-label="PMDA Home"
    >
      <div className={cn("rounded-xl overflow-hidden flex-shrink-0", sizes[size].icon)}>
        <svg 
          viewBox="0 0 512 512" 
          fill="none" 
          xmlns="http://www.w3.org/2000/svg"
          className="w-full h-full"
        >
          <defs>
            <linearGradient id="pmda-bg" x1="38" y1="32" x2="474" y2="480" gradientUnits="userSpaceOnUse">
              <stop stopColor="#12D8CA" />
              <stop offset="0.55" stopColor="#2C9DFF" />
              <stop offset="1" stopColor="#FF7A59" />
            </linearGradient>
            <linearGradient id="pmda-ring" x1="128" y1="112" x2="394" y2="402" gradientUnits="userSpaceOnUse">
              <stop stopColor="#F8FBFF" />
              <stop offset="1" stopColor="#DCE7FF" stopOpacity="0.9" />
            </linearGradient>
            <linearGradient id="pmda-wave" x1="96" y1="262" x2="418" y2="262" gradientUnits="userSpaceOnUse">
              <stop stopColor="#FFD166" />
              <stop offset="1" stopColor="#FF9D5C" />
            </linearGradient>
          </defs>
          <rect width="512" height="512" rx="104" fill="url(#pmda-bg)"/>
          <rect x="18" y="18" width="476" height="476" rx="92" fill="black" fillOpacity="0.08" />
          <path
            d="M158 130h126c67 0 122 55 122 122s-55 122-122 122h-66v62c0 19-15 34-34 34s-34-15-34-34V164c0-19 15-34 34-34Z"
            fill="url(#pmda-ring)"
          />
          <path d="M218 190v124h64c34 0 62-28 62-62s-28-62-62-62h-64Z" fill="#0C1524" fillOpacity="0.72" />
          <path
            d="M94 270c30-21 62-31 96-31 34 0 63 10 91 20 24 9 46 17 70 17 24 0 46-8 71-22l1 43c-24 14-48 21-73 21-32 0-60-10-85-19-24-9-45-17-69-17-24 0-47 8-71 24l-31-36Z"
            fill="url(#pmda-wave)"
            fillOpacity="0.96"
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
