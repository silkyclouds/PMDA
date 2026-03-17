import { useCallback, useEffect, useMemo, useState } from 'react';
import { Heart, Loader2, Music2 } from 'lucide-react';
import { useLocation, useNavigate } from 'react-router-dom';

import * as api from '@/lib/api';
import { useAuth } from '@/contexts/AuthContext';
import { AlbumArtwork } from '@/components/library/AlbumArtwork';
import { AlbumBadgeGroups } from '@/components/library/AlbumBadgeGroups';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useAlbumBadgesVisibility } from '@/hooks/use-album-badges';
import { withBackLinkState } from '@/lib/backNavigation';

export default function LikedPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { user } = useAuth();
  const { showBadges, setShowBadges } = useAlbumBadgesVisibility();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<api.LikedSummaryResponse | null>(null);
  const [usersLoading, setUsersLoading] = useState(false);
  const [visibleUsers, setVisibleUsers] = useState<api.SocialUser[]>([]);
  const [selectedUserId, setSelectedUserId] = useState<string>('me');
  const requestedUserId = useMemo(() => {
    const value = new URLSearchParams(location.search).get('user');
    return value && /^\d+$/.test(value) ? value : 'me';
  }, [location.search]);

  useEffect(() => {
    setSelectedUserId(requestedUserId);
  }, [requestedUserId]);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.getLikedSummary(selectedUserId === 'me' ? undefined : Number(selectedUserId));
      setData(res);
    } catch (err) {
      setData(null);
      setError(err instanceof Error ? err.message : 'Failed to load liked items');
    } finally {
      setLoading(false);
    }
  }, [selectedUserId]);

  const loadUsers = useCallback(async () => {
    setUsersLoading(true);
    try {
      const res = await api.getSocialUsers('liked');
      setVisibleUsers(Array.isArray(res.users) ? res.users : []);
    } catch {
      setVisibleUsers([]);
    } finally {
      setUsersLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    void loadUsers();
  }, [loadUsers]);

  useEffect(() => {
    const qs = new URLSearchParams(location.search);
    if (selectedUserId === 'me') {
      qs.delete('user');
    } else {
      qs.set('user', selectedUserId);
    }
    const nextSearch = qs.toString() ? `?${qs.toString()}` : '';
    if (nextSearch !== location.search) {
      navigate({ pathname: location.pathname, search: nextSearch }, { replace: true });
    }
  }, [location.pathname, location.search, navigate, selectedUserId]);

  const albums = useMemo(() => data?.albums || [], [data?.albums]);
  const tracks = useMemo(() => data?.tracks || [], [data?.tracks]);
  const artists = useMemo(() => data?.artists || [], [data?.artists]);
  const labels = useMemo(() => data?.labels || [], [data?.labels]);
  const suggestions = useMemo(() => data?.recommended_albums || [], [data?.recommended_albums]);

  const formatDuration = (seconds: number) => {
    const safe = Math.max(0, Math.floor(seconds || 0));
    const minutes = Math.floor(safe / 60);
    const secs = safe % 60;
    return `${minutes}:${secs.toString().padStart(2, '0')}`;
  };

  const likedTrackHref = (trackId: number, albumId: number) => {
    const qs = new URLSearchParams(location.search);
    qs.set('track_id', String(trackId));
    const search = qs.toString();
    return `/library/album/${albumId}${search ? `?${search}` : ''}`;
  };

  return (
    <div className="container py-6 space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-1">
          <h1 className="text-3xl font-bold tracking-tight">Liked</h1>
          <p className="text-sm text-muted-foreground">
            Your own likes and PMDA suggestions derived from your ratings and favorites.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button type="button" variant="outline" size="sm" onClick={() => setShowBadges(!showBadges)}>
            {showBadges ? 'Hide badges' : 'Show badges'}
          </Button>
          <Select value={selectedUserId} onValueChange={setSelectedUserId}>
            <SelectTrigger className="w-[220px]">
              <SelectValue placeholder="Choose a user" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="me">My liked</SelectItem>
              {visibleUsers.map((candidate) => (
                <SelectItem key={`liked-user-${candidate.id}`} value={String(candidate.id)}>
                  {candidate.username}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button type="button" variant="outline" size="sm" onClick={() => void load()}>
            Refresh
          </Button>
        </div>
      </div>

      {usersLoading ? (
        <p className="text-xs text-muted-foreground">Loading visible user lists…</p>
      ) : null}
      {data?.owner?.username ? (
        <div className="text-sm text-muted-foreground">
          Viewing liked content for <span className="font-medium text-foreground">{selectedUserId === 'me' ? (user?.username || data.owner.username) : data.owner.username}</span>
        </div>
      ) : null}

      {loading ? (
        <div className="flex items-center justify-center py-24 text-muted-foreground">
          <Loader2 className="mr-2 h-5 w-5 animate-spin" />
          Loading liked items…
        </div>
      ) : error ? (
        <Card className="border-destructive/40">
          <CardContent className="p-6 text-sm text-destructive">{error}</CardContent>
        </Card>
      ) : (
        <>
          <section className="space-y-3">
            <div className="flex items-center gap-2">
              <Music2 className="h-4 w-4 text-primary" />
              <h2 className="text-lg font-semibold">Liked tracks</h2>
              <Badge variant="outline" className="text-[10px]">{tracks.length}</Badge>
            </div>
            {tracks.length === 0 ? (
              <Card><CardContent className="p-6 text-sm text-muted-foreground">No liked tracks yet.</CardContent></Card>
            ) : (
              <Card className="pmda-flat-surface">
                <CardContent className="p-0">
                  <div className="divide-y divide-border/70">
                    {tracks.map((track) => (
                      <button
                        key={`liked-track-${track.track_id}`}
                        type="button"
                        className="flex w-full items-center gap-3 px-4 py-3 text-left transition-colors hover:bg-accent/40"
                        onClick={() => navigate(likedTrackHref(track.track_id, track.album_id), { state: withBackLinkState(location) })}
                      >
                        <div className="h-12 w-12 shrink-0 overflow-hidden bg-muted">
                          {track.thumb ? <img src={track.thumb} alt={track.album_title} className="h-full w-full object-cover" /> : null}
                        </div>
                        <div className="min-w-0 flex-1">
                          <div className="truncate text-sm font-semibold">{track.title}</div>
                          <div className="truncate text-xs text-muted-foreground">
                            {track.artist_name} · {track.album_title}
                          </div>
                        </div>
                        <div className="shrink-0 text-right text-[11px] text-muted-foreground">
                          {track.disc_num && track.disc_num > 1 ? <div>D{track.disc_num}</div> : null}
                          {track.track_num && track.track_num > 0 ? <div>#{track.track_num}</div> : null}
                        </div>
                        <div className="shrink-0 text-xs tabular-nums text-muted-foreground">
                          {formatDuration(track.duration_sec)}
                        </div>
                      </button>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}
          </section>

          <section className="space-y-3">
            <div className="flex items-center gap-2">
              <Heart className="h-4 w-4 text-primary" />
              <h2 className="text-lg font-semibold">Liked albums</h2>
              <Badge variant="outline" className="text-[10px]">{albums.length}</Badge>
            </div>
            {albums.length === 0 ? (
              <Card><CardContent className="p-6 text-sm text-muted-foreground">No liked albums yet.</CardContent></Card>
            ) : (
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                {albums.map((album) => (
                  <Card
                    key={`liked-album-${album.album_id}`}
                    className="pmda-flat-tile cursor-pointer"
                    role="button"
                    tabIndex={0}
                    onClick={() => navigate(`/library/album/${album.album_id}${location.search || ''}`, { state: withBackLinkState(location) })}
                  >
                    <div className="aspect-square bg-muted">
                      <AlbumArtwork albumThumb={album.thumb} artistId={album.artist_id} alt={album.title} size={512} />
                    </div>
                    <CardContent className="p-4 space-y-2">
                      <div className="space-y-1">
                        <div className="truncate text-sm font-semibold">{album.title}</div>
                        <button
                          type="button"
                          className="truncate text-xs text-muted-foreground hover:underline"
                          onClick={(event) => {
                            event.stopPropagation();
                            navigate(`/library/artist/${album.artist_id}${location.search || ''}`, { state: withBackLinkState(location) });
                          }}
                        >
                          {album.artist_name}
                        </button>
                        {!showBadges && album.year ? (
                          <div className="text-[11px] text-muted-foreground">{album.year}</div>
                        ) : null}
                      </div>
                      <AlbumBadgeGroups
                        show={showBadges}
                        compact
                        userRating={album.user_rating}
                        publicRating={album.public_rating}
                        publicRatingVotes={album.public_rating_votes}
                        format={album.format}
                        isLossless={album.is_lossless}
                        year={album.year}
                        trackCount={album.track_count}
                        genres={album.genres || (album.genre ? [album.genre] : [])}
                        label={album.label}
                        onGenreClick={(genre) => navigate(`/library/genre/${encodeURIComponent(genre)}${location.search || ''}`, { state: withBackLinkState(location) })}
                        onLabelClick={album.label ? () => navigate(`/library/label/${encodeURIComponent(album.label || '')}${location.search || ''}`, { state: withBackLinkState(location) }) : undefined}
                      />
                    </CardContent>
                  </Card>
                ))}
              </div>
            )}
          </section>

          <section className="space-y-3">
            <h2 className="text-lg font-semibold">Liked artists</h2>
            {artists.length === 0 ? (
              <Card><CardContent className="p-6 text-sm text-muted-foreground">No liked artists yet.</CardContent></Card>
            ) : (
              <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-3">
                {artists.map((artist) => (
                  <button
                    key={`liked-artist-${artist.artist_id}`}
                    type="button"
                    className="pmda-flat-surface p-4 text-left transition-colors"
                    onClick={() => navigate(`/library/artist/${artist.artist_id}${location.search || ''}`, { state: withBackLinkState(location) })}
                  >
                    <div className="flex items-center gap-3">
                      <div className="h-14 w-14 overflow-hidden rounded-sm bg-muted">
                        {artist.thumb ? (
                          <img src={artist.thumb} alt={artist.artist_name} className="h-full w-full object-cover" />
                        ) : null}
                      </div>
                      <div className="min-w-0">
                        <div className="truncate text-sm font-semibold">{artist.artist_name}</div>
                        <div className="text-xs text-muted-foreground">
                          {artist.album_count} album{artist.album_count !== 1 ? 's' : ''}
                        </div>
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            )}
          </section>

          <section className="space-y-3">
            <h2 className="text-lg font-semibold">Liked labels</h2>
            {labels.length === 0 ? (
              <Card><CardContent className="p-6 text-sm text-muted-foreground">No liked labels yet.</CardContent></Card>
            ) : (
              <ScrollArea className="w-full whitespace-nowrap border border-border/70 bg-card">
                <div className="flex gap-2 p-4">
                  {labels.map((label) => (
                    <Button
                      key={`liked-label-${label.label}`}
                      type="button"
                      size="sm"
                      variant="outline"
                      onClick={() => navigate(`/library/label/${encodeURIComponent(label.label)}${location.search || ''}`, { state: withBackLinkState(location) })}
                    >
                      {label.label}
                    </Button>
                  ))}
                </div>
              </ScrollArea>
            )}
          </section>

          <section className="space-y-3">
            <h2 className="text-lg font-semibold">Because you liked this</h2>
            {suggestions.length === 0 ? (
              <Card><CardContent className="p-6 text-sm text-muted-foreground">PMDA has no suggestions yet.</CardContent></Card>
            ) : (
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                {suggestions.map((album) => (
                  <Card
                    key={`liked-suggestion-${album.album_id}`}
                    className="pmda-flat-tile cursor-pointer"
                    role="button"
                    tabIndex={0}
                    onClick={() => navigate(`/library/album/${album.album_id}${location.search || ''}`, { state: withBackLinkState(location) })}
                  >
                    <div className="aspect-square bg-muted">
                      <AlbumArtwork albumThumb={album.thumb} artistId={album.artist_id} alt={album.title} size={512} />
                    </div>
                    <CardContent className="p-4 space-y-2">
                      <div className="space-y-1">
                        <div className="truncate text-sm font-semibold">{album.title}</div>
                        <div className="truncate text-xs text-muted-foreground">{album.artist_name}</div>
                        {!showBadges && album.year ? (
                          <div className="text-[11px] text-muted-foreground">{album.year}</div>
                        ) : null}
                      </div>
                      <AlbumBadgeGroups
                        show={showBadges}
                        compact
                        userRating={album.user_rating}
                        publicRating={album.public_rating}
                        publicRatingVotes={album.public_rating_votes}
                        format={album.format}
                        isLossless={album.is_lossless}
                        year={album.year}
                        trackCount={album.track_count}
                        genres={album.genres || (album.genre ? [album.genre] : [])}
                        label={album.label}
                      />
                    </CardContent>
                  </Card>
                ))}
              </div>
            )}
          </section>
        </>
      )}
    </div>
  );
}
