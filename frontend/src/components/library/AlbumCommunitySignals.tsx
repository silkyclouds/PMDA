import { Badge } from '@/components/ui/badge';
import { AlbumRatingStars } from '@/components/library/AlbumRatingStars';
import { badgeKindClass } from '@/lib/badgeStyles';
import { cn } from '@/lib/utils';

function formatCompactCount(value?: number | null): string {
  const num = Number(value || 0);
  if (!Number.isFinite(num) || num <= 0) return '0';
  return new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 }).format(num);
}

type AlbumCommunitySignalsProps = {
  userRating?: number | null;
  publicRating?: number | null;
  publicRatingVotes?: number | null;
  heatLabel?: string | null;
  className?: string;
  compact?: boolean;
};

export function AlbumCommunitySignals({
  userRating,
  publicRating,
  publicRatingVotes,
  heatLabel,
  className,
  compact = false,
}: AlbumCommunitySignalsProps) {
  const normalizedUser = Number(userRating || 0);
  const normalizedPublic = Number(publicRating || 0);
  const votes = Number(publicRatingVotes || 0);
  const hasAny = normalizedUser > 0 || normalizedPublic > 0 || Boolean(heatLabel);

  if (!hasAny) return null;

  return (
    <div className={cn('flex flex-wrap items-center gap-2', className)}>
      {normalizedUser > 0 ? (
        <div className="inline-flex items-center gap-1 rounded-full border border-border/70 bg-background/40 px-2 py-1">
          <span className="text-[10px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            You
          </span>
          <AlbumRatingStars value={normalizedUser} size={compact ? 12 : 14} showValue={false} />
        </div>
      ) : null}
      {normalizedPublic > 0 ? (
        <div className="inline-flex items-center gap-1.5 rounded-full border border-border/70 bg-background/40 px-2 py-1">
          <span className="text-[10px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
            Public
          </span>
          <AlbumRatingStars value={normalizedPublic} size={compact ? 12 : 14} showValue={!compact} />
          {compact ? (
            <span className="text-[10px] text-muted-foreground tabular-nums">
              {normalizedPublic.toFixed(1)}
            </span>
          ) : null}
          {votes > 0 ? (
            <span className="text-[10px] text-muted-foreground">
              {formatCompactCount(votes)} vote{votes > 1 ? 's' : ''}
            </span>
          ) : null}
        </div>
      ) : null}
      {heatLabel ? (
        <Badge
          variant="outline"
          className={cn(
            'text-[10px] font-medium',
            heatLabel.toLowerCase().includes('essential')
              ? badgeKindClass('status_match')
              : heatLabel.toLowerCase().includes('recommended')
                ? badgeKindClass('status_soft')
                : badgeKindClass('source')
          )}
        >
          {heatLabel}
        </Badge>
      ) : null}
    </div>
  );
}
