import { ProviderBadge } from '@/components/providers/ProviderBadge';
import { cn } from '@/lib/utils';

type AlbumSourceFields = {
  metadata_source?: string | null;
  strict_match_provider?: string | null;
  musicbrainz_release_group_id?: string | null;
  discogs_release_id?: string | null;
  lastfm_album_mbid?: string | null;
  bandcamp_album_url?: string | null;
};

function normalizeProvider(value?: string | null): string {
  return String(value || '').trim().toLowerCase();
}

function collectAlbumSources(album: AlbumSourceFields): string[] {
  const ordered: string[] = [];
  const seen = new Set<string>();

  const push = (provider?: string | null) => {
    const normalized = normalizeProvider(provider);
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    ordered.push(normalized);
  };

  push(album.strict_match_provider);
  push(album.metadata_source);
  if (album.musicbrainz_release_group_id) push('musicbrainz');
  if (album.discogs_release_id) push('discogs');
  if (album.lastfm_album_mbid) push('lastfm');
  if (album.bandcamp_album_url) push('bandcamp');

  return ordered;
}

export function AlbumMatchSources({
  album,
  className,
}: {
  album: AlbumSourceFields;
  className?: string;
}) {
  const sources = collectAlbumSources(album);
  if (sources.length === 0) return null;

  return (
    <div
      className={cn('flex flex-wrap items-center gap-1', className)}
      title={`Album sources: ${sources.join(', ')}`}
    >
      {sources.map((provider, index) => (
        <ProviderBadge
          key={provider}
          provider={provider}
          className={cn(
            'h-5 rounded-sm px-1.5 text-[9px] font-normal leading-none opacity-70',
            index === 0 ? 'opacity-95' : 'opacity-70'
          )}
        />
      ))}
    </div>
  );
}
