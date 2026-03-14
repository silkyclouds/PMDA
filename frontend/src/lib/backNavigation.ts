import type { Location } from 'react-router-dom';

export interface BackLinkState {
  path: string;
  label: string;
}

type LocationLike = Pick<Location, 'pathname' | 'search' | 'state'>;

function pathnameLabel(pathname: string): string {
  const path = String(pathname || '').trim();
  if (path.startsWith('/library/album/')) return 'Album';
  if (path.startsWith('/library/artist/')) return 'Artist';
  if (path.startsWith('/library/albums')) return 'Albums';
  if (path.startsWith('/library/artists')) return 'Artists';
  if (path.startsWith('/library/labels')) return 'Labels';
  if (path.startsWith('/library/label/')) return 'Label';
  if (path.startsWith('/library/genres')) return 'Genres';
  if (path.startsWith('/library/genre/')) return 'Genre';
  if (path.startsWith('/library/playlists/')) return 'Playlist';
  if (path.startsWith('/library/playlists')) return 'Playlists';
  if (path.startsWith('/library/liked')) return 'Liked';
  if (path.startsWith('/library/recommendations')) return 'Recommendations';
  if (path.startsWith('/library/home/feed/')) return 'Home';
  return 'Library';
}

export function buildBackLink(location: LocationLike): BackLinkState {
  const pathname = String(location.pathname || '').trim() || '/library';
  const search = String(location.search || '').trim();
  return {
    path: `${pathname}${search}`,
    label: pathnameLabel(pathname),
  };
}

export function withBackLinkState<T extends Record<string, unknown> | undefined>(
  location: LocationLike,
  extraState?: T,
): (T extends undefined ? {} : T) & { backLink: BackLinkState } {
  const base = (extraState && typeof extraState === 'object' ? extraState : {}) as Record<string, unknown>;
  return {
    ...(base as T extends undefined ? {} : T),
    backLink: buildBackLink(location),
  } as (T extends undefined ? {} : T) & { backLink: BackLinkState };
}

export function resolveBackLink(
  location: LocationLike,
  fallback: BackLinkState,
): BackLinkState {
  const raw = (location.state as { backLink?: BackLinkState } | null)?.backLink;
  if (raw && typeof raw.path === 'string' && raw.path.trim()) {
    return {
      path: raw.path,
      label: typeof raw.label === 'string' && raw.label.trim() ? raw.label : fallback.label,
    };
  }
  return fallback;
}
