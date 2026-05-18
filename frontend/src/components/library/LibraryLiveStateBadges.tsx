import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';

type LiveStateItem = {
  publication_state?: string | null;
  cover_state?: string | null;
  artist_media_state?: string | null;
  profile_state?: string | null;
};

function buildBadgeSpecs(item: LiveStateItem): Array<{ key: string; label: string; className: string }> {
  const specs: Array<{ key: string; label: string; className: string }> = [];
  const publicationState = String(item.publication_state || '').trim().toLowerCase();
  const coverState = String(item.cover_state || '').trim().toLowerCase();
  const artistMediaState = String(item.artist_media_state || '').trim().toLowerCase();
  const profileState = String(item.profile_state || '').trim().toLowerCase();

  if (publicationState === 'published') {
    specs.push({
      key: 'published',
      label: 'Live now',
      className: 'border-emerald-500/35 bg-emerald-500/10 text-emerald-100',
    });
  } else if (publicationState === 'enriching') {
    specs.push({
      key: 'enriching',
      label: 'Enriching',
      className: 'border-sky-500/35 bg-sky-500/10 text-sky-100',
    });
  }

  if (coverState === 'fallback') {
    specs.push({
      key: 'cover-fallback',
      label: 'Cover fallback',
      className: 'border-amber-500/35 bg-amber-500/10 text-amber-100',
    });
  } else if (coverState === 'enriching') {
    specs.push({
      key: 'cover-enriching',
      label: 'Cover pending',
      className: 'border-fuchsia-500/30 bg-fuchsia-500/10 text-fuchsia-100',
    });
  }

  if (artistMediaState === 'enriching') {
    specs.push({
      key: 'artist-media-enriching',
      label: 'Artist image pending',
      className: 'border-violet-500/30 bg-violet-500/10 text-violet-100',
    });
  }

  if (profileState === 'enriching') {
    specs.push({
      key: 'profile-enriching',
      label: 'Profile pending',
      className: 'border-orange-500/30 bg-orange-500/10 text-orange-100',
    });
  }

  return specs;
}

export function LibraryLiveStateBadges({
  item,
  className,
  maxBadges = 2,
}: {
  item: LiveStateItem;
  className?: string;
  maxBadges?: number;
}) {
  const specs = buildBadgeSpecs(item).slice(0, Math.max(0, maxBadges));
  if (specs.length <= 0) return null;
  return (
    <div className={cn('flex flex-wrap gap-1.5', className)}>
      {specs.map((badge) => (
        <Badge key={badge.key} variant="outline" className={cn('text-[10px] font-medium tracking-[0.18em] uppercase', badge.className)}>
          {badge.label}
        </Badge>
      ))}
    </div>
  );
}
