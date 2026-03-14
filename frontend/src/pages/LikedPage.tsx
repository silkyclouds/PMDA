import { useCallback, useEffect, useMemo, useState } from 'react';
import { Heart, Loader2 } from 'lucide-react';
import { useLocation, useNavigate } from 'react-router-dom';

import * as api from '@/lib/api';
import { AlbumArtwork } from '@/components/library/AlbumArtwork';
import { AlbumBadgeGroups } from '@/components/library/AlbumBadgeGroups';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { useAlbumBadgesVisibility } from '@/hooks/use-album-badges';
import { withBackLinkState } from '@/lib/backNavigation';

export default function LikedPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { showBadges, setShowBadges } = useAlbumBadgesVisibility();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<api.LikedSummaryResponse | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.getLikedSummary();
      setData(res);
    } catch (err) {
      setData(null);
      setError(err instanceof Error ? err.message : 'Failed to load liked items');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const albums = useMemo(() => data?.albums || [], [data?.albums]);
  const artists = useMemo(() => data?.artists || [], [data?.artists]);
  const labels = useMemo(() => data?.labels || [], [data?.labels]);
  const suggestions = useMemo(() => data?.recommended_albums || [], [data?.recommended_albums]);

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
          <Button type="button" variant="outline" size="sm" onClick={() => void load()}>
            Refresh
          </Button>
        </div>
      </div>

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
                    className="overflow-hidden cursor-pointer border-border/70 hover:border-primary/35 transition-colors"
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
                    className="rounded-2xl border border-border/70 bg-card p-4 text-left transition-colors hover:border-primary/35"
                    onClick={() => navigate(`/library/artist/${artist.artist_id}${location.search || ''}`, { state: withBackLinkState(location) })}
                  >
                    <div className="flex items-center gap-3">
                      <div className="h-14 w-14 overflow-hidden rounded-2xl bg-muted">
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
              <ScrollArea className="w-full whitespace-nowrap rounded-2xl border border-border/70 bg-card">
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
                    className="overflow-hidden cursor-pointer border-border/70 hover:border-primary/35 transition-colors"
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
