import { useState, useEffect } from 'react';
import { ExternalLink } from 'lucide-react';
import { Button } from '@/components/ui/button';
import * as api from '@/lib/api';
import type { Edition } from '@/lib/api';

/** Build Plex Web URL for a rating key (artist or album). */
export function buildPlexMetadataUrl(plexHost: string, ratingKey: number): string {
  const base = plexHost.replace(/\/$/, '');
  return `${base}/web/index.html#!/library/metadata/${ratingKey}`;
}

interface PlexLinkProps {
  /** Plex rating key of the artist — opens artist page so user can see all albums (including duplicates) */
  artistId?: number | null;
  editions: Edition[];
  selectedEditionIndex: number;
}

export function PlexLink({ artistId, editions, selectedEditionIndex }: PlexLinkProps) {
  const [plexHost, setPlexHost] = useState<string | null>(null);

  useEffect(() => {
    api.getConfig().then(config => {
      if (config.PLEX_HOST) {
        setPlexHost(config.PLEX_HOST.replace(/\/$/, ''));
      }
    });
  }, []);

  if (!plexHost) return null;

  // Always use artist page if available, otherwise fallback to album
  const plexUrl = artistId != null && artistId > 0
    ? buildPlexMetadataUrl(plexHost, artistId)
    : (editions[selectedEditionIndex] || editions[0])?.album_id != null
      ? buildPlexMetadataUrl(plexHost, (editions[selectedEditionIndex] || editions[0]).album_id!)
      : `${plexHost}/web/index.html`;

  return (
    <Button
      variant="outline"
      size="sm"
      asChild
      className="gap-1.5 h-7 text-xs"
    >
      <a href={plexUrl} target="_blank" rel="noopener noreferrer">
        <ExternalLink className="w-3 h-3" />
        Show in Plex
      </a>
    </Button>
  );
}

/** Standalone link to open an artist or album in Plex by rating key (e.g. Library page). */
interface PlexOpenLinkProps {
  /** Plex rating key: artist (metadata_type 8) or album (metadata_type 9). */
  ratingKey: number;
  label?: string;
  variant?: 'default' | 'outline' | 'ghost' | 'link' | 'destructive' | 'secondary';
  size?: 'default' | 'sm' | 'lg' | 'icon';
  className?: string;
}

export function PlexOpenLink({ ratingKey, label = 'Open in Plex', variant = 'outline', size = 'sm', className }: PlexOpenLinkProps) {
  const [plexHost, setPlexHost] = useState<string | null>(null);

  useEffect(() => {
    api.getConfig().then(config => {
      if (config.PLEX_HOST) {
        setPlexHost(config.PLEX_HOST.replace(/\/$/, ''));
      }
    });
  }, []);

  if (!plexHost || ratingKey <= 0) return null;

  const plexUrl = buildPlexMetadataUrl(plexHost, ratingKey);

  return (
    <Button variant={variant} size={size} asChild className={className ?? 'gap-1.5'}>
      <a href={plexUrl} target="_blank" rel="noopener noreferrer">
        <ExternalLink className="w-3 h-3" />
        {label}
      </a>
    </Button>
  );
}
