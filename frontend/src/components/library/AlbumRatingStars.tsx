import { Star } from 'lucide-react';

import { cn } from '@/lib/utils';

type AlbumRatingStarsProps = {
  value?: number | null;
  editable?: boolean;
  onChange?: (value: number | null) => void;
  size?: number;
  showValue?: boolean;
  className?: string;
};

export function AlbumRatingStars({
  value,
  editable = false,
  onChange,
  size = 16,
  showValue = true,
  className,
}: AlbumRatingStarsProps) {
  const normalized = Math.max(0, Math.min(5, Number(value || 0)));
  const roundedDisplay = Math.round(normalized * 10) / 10;

  return (
    <div className={cn('flex items-center gap-1.5', className)}>
      <div className="flex items-center gap-0.5">
        {Array.from({ length: 5 }, (_, idx) => {
          const starIndex = idx + 1;
          const fillPct = Math.max(0, Math.min(1, normalized - idx));
          const baseStar = (
            <span
              className="relative inline-flex"
              style={{ width: size, height: size }}
            >
              <Star
                className="absolute inset-0 text-white/15"
                style={{ width: size, height: size }}
                strokeWidth={1.8}
              />
              <span
                className="absolute inset-0 overflow-hidden"
                style={{ width: `${fillPct * 100}%` }}
              >
                <Star
                  className="text-amber-400 fill-amber-400"
                  style={{ width: size, height: size }}
                  strokeWidth={1.8}
                />
              </span>
            </span>
          );
          if (!editable) return <span key={`star-${starIndex}`}>{baseStar}</span>;
          return (
            <button
              key={`star-${starIndex}`}
              type="button"
              className="inline-flex rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary"
              onClick={() => onChange?.(Math.round(normalized) === starIndex ? null : starIndex)}
              aria-label={`Rate ${starIndex} star${starIndex > 1 ? 's' : ''}`}
              title={`Rate ${starIndex} star${starIndex > 1 ? 's' : ''}`}
            >
              {baseStar}
            </button>
          );
        })}
      </div>
      {showValue ? (
        <span className="text-xs text-muted-foreground tabular-nums min-w-[2.5rem]">
          {roundedDisplay > 0 ? roundedDisplay.toFixed(1) : '—'}
        </span>
      ) : null}
    </div>
  );
}
