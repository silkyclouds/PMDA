import { AlbumCommunitySignals } from '@/components/library/AlbumCommunitySignals';
import { FormatBadge } from '@/components/FormatBadge';
import { Badge } from '@/components/ui/badge';
import { formatAudioSpec } from '@/lib/audioFormat';
import { badgeKindClass } from '@/lib/badgeStyles';
import { cn } from '@/lib/utils';

type AlbumBadgeGroupsProps = {
  show: boolean;
  compact?: boolean;
  userRating?: number | null;
  publicRating?: number | null;
  publicRatingVotes?: number | null;
  format?: string | null;
  isLossless?: boolean | null;
  sampleRate?: number | null;
  bitDepth?: number | null;
  year?: number | string | null;
  trackCount?: number | null;
  genres?: string[] | null;
  label?: string | null;
  onGenreClick?: (genre: string) => void;
  onLabelClick?: () => void;
};

function normalizeGenres(genres?: string[] | null): string[] {
  if (!Array.isArray(genres)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of genres) {
    const value = String(item || '').replace(/\s+/g, ' ').trim();
    if (!value || value.toLowerCase() === 'unknown') continue;
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(value);
  }
  return out;
}

export function AlbumBadgeGroups({
  show,
  compact = false,
  userRating,
  publicRating,
  publicRatingVotes,
  format,
  isLossless,
  sampleRate,
  bitDepth,
  year,
  trackCount,
  genres,
  label,
  onGenreClick,
  onLabelClick,
}: AlbumBadgeGroupsProps) {
  if (!show) return null;

  const cleanGenres = normalizeGenres(genres);
  const cleanLabelRaw = String(label || '').trim();
  const cleanLabel = cleanLabelRaw && cleanLabelRaw.toLowerCase() !== 'unknown' ? cleanLabelRaw : '';
  const audioSpec = formatAudioSpec(bitDepth, sampleRate);
  const hasPulse = Number(userRating || 0) > 0 || Number(publicRating || 0) > 0;
  const audioBadgeText = audioSpec || (isLossless === false ? 'Lossy' : '');
  const hasAlbumFacts = Boolean(String(format || '').trim()) || Boolean(audioBadgeText) || Number(year || 0) > 0 || Number(trackCount || 0) > 0;
  const hasTaxonomy = cleanGenres.length > 0 || Boolean(cleanLabel);
  const headingClass = compact
    ? 'text-[10px] font-medium uppercase tracking-[0.18em] text-muted-foreground/80'
    : 'text-[11px] font-medium uppercase tracking-[0.2em] text-muted-foreground/85';
  const badgeClass = 'text-[11px]';

  return (
    <div className="space-y-2">
      {hasPulse ? (
        <div className="space-y-1">
          <div className={headingClass}>Pulse</div>
          <AlbumCommunitySignals
            userRating={userRating}
            publicRating={publicRating}
            publicRatingVotes={publicRatingVotes}
            compact={compact}
            className="gap-1.5"
          />
        </div>
      ) : null}

      {hasAlbumFacts ? (
        <div className="space-y-1">
          <div className={headingClass}>Album</div>
          <div className="flex flex-wrap items-center gap-1.5">
            {String(format || '').trim() ? <FormatBadge format={String(format)} size="sm" /> : null}
            {audioBadgeText ? (
              <Badge
                variant="outline"
                className={cn(badgeClass, isLossless ? badgeKindClass('lossless') : badgeKindClass('lossy'))}
              >
                {audioBadgeText}
              </Badge>
            ) : null}
            {Number(year || 0) > 0 ? (
              <Badge variant="outline" className={cn(badgeClass, badgeKindClass('year'))}>
                {year}
              </Badge>
            ) : null}
            {Number(trackCount || 0) > 0 ? (
              <Badge variant="outline" className={cn(badgeClass, badgeKindClass('count'))}>
                {trackCount}t
              </Badge>
            ) : null}
          </div>
        </div>
      ) : null}

      {hasTaxonomy ? (
        <div className="space-y-1">
          <div className={headingClass}>Genres & label</div>
          <div className="flex flex-wrap items-center gap-1.5">
            {cleanGenres.slice(0, compact ? 4 : 6).map((genre) => (
              <Badge
                key={genre}
                variant="outline"
                className={cn(badgeClass, onGenreClick ? 'cursor-pointer' : '', badgeKindClass('genre'))}
                onClick={onGenreClick ? () => onGenreClick(genre) : undefined}
              >
                {genre}
              </Badge>
            ))}
            {cleanLabel ? (
              <Badge
                variant="outline"
                className={cn(badgeClass, onLabelClick ? 'cursor-pointer' : '', badgeKindClass('label'))}
                onClick={onLabelClick}
              >
                {cleanLabel}
              </Badge>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}
