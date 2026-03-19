import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutletContext, useParams } from 'react-router-dom';
import { ArrowLeft, Heart, Loader2, Play, Share2, UserRound } from 'lucide-react';

import { AspectRatio } from '@/components/ui/aspect-ratio';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { EntityDiscoverDialog } from '@/components/library/EntityDiscoverDialog';
import { GridSizeControl } from '@/components/library/GridSizeControl';
import { SocialActivityBadges } from '@/components/social/SocialActivityBadges';
import { ScrollArea } from '@/components/ui/scroll-area';
import { AlbumArtwork } from '@/components/library/AlbumArtwork';
import { AlbumBadgeGroups } from '@/components/library/AlbumBadgeGroups';
import { ShareDialog } from '@/components/social/ShareDialog';
import { usePlayback } from '@/contexts/PlaybackContext';
import { getLibraryGridTemplateColumns, useLibraryTileSize } from '@/hooks/use-library-tile-size';
import { useToast } from '@/hooks/use-toast';
import { useIsMobile } from '@/hooks/use-mobile';
import { dedupeAlbumsForDisplay, mergeAlbumsForDisplay } from '@/lib/albumDisplayDedupe';
import { resolveBackLink, withBackLinkState } from '@/lib/backNavigation';
import { cn } from '@/lib/utils';
import * as api from '@/lib/api';
import type { TrackInfo } from '@/components/library/AudioPlayer';
import type { LibraryOutletContext } from '@/pages/LibraryLayout';
import { useAuth } from '@/contexts/AuthContext';

export default function LabelPage() {
  const isMobile = useIsMobile();
  const navigate = useNavigate();
  const location = useLocation();
  const { includeUnmatched } = useOutletContext<LibraryOutletContext>();
  const { startPlayback, setCurrentTrack } = usePlayback();
  const { canUseAI } = useAuth();
  const { toast } = useToast();
  const { tileSize, setTileSize } = useLibraryTileSize();
  const params = useParams<{ label: string }>();
  const label = decodeURIComponent(String(params.label || '')).trim();

  const [profileLoading, setProfileLoading] = useState(false);
  const [profile, setProfile] = useState<api.LabelProfileResponse | null>(null);
  const [labelLiked, setLabelLiked] = useState(false);

  const [loading, setLoading] = useState(false);
  const [appending, setAppending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [albums, setAlbums] = useState<api.LibraryAlbumItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const limit = 120;

  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const loadingMoreRef = useRef(false);
  const requestIdRef = useRef(0);

  const gridTemplateColumns = useMemo(() => {
    return getLibraryGridTemplateColumns(tileSize, isMobile);
  }, [tileSize, isMobile]);

  const handlePlayAlbum = useCallback(async (albumId: number, fallbackTitle: string, fallbackThumb?: string | null) => {
    try {
      const response = await fetch(`/api/library/album/${albumId}/tracks`);
      if (!response.ok) throw new Error('Failed to load tracks');
      const data = await response.json();
      const tracksList: TrackInfo[] = data.tracks || [];
      if (tracksList.length === 0) {
        toast({ title: 'No tracks', description: 'This album has no playable tracks.', variant: 'destructive' });
        return;
      }
      const albumThumb = data.album_thumb || fallbackThumb || null;
      startPlayback(albumId, fallbackTitle || 'Album', albumThumb, tracksList);
      setCurrentTrack(tracksList[0]);
    } catch (err) {
      toast({
        title: 'Error',
        description: err instanceof Error ? err.message : 'Failed to load tracks',
        variant: 'destructive',
      });
    }
  }, [setCurrentTrack, startPlayback, toast]);

  const loadProfile = useCallback(async () => {
    if (!label) return;
    try {
      setProfileLoading(true);
      const res = await api.getLabelProfile(label, { includeUnmatched, limit_artists: 24, limit_genres: 24 });
      setProfile(res);
    } catch {
      setProfile(null);
    } finally {
      setProfileLoading(false);
    }
  }, [includeUnmatched, label]);

  const loadAlbumsPage = useCallback(async (opts: { reset: boolean; pageOffset: number }) => {
    const rid = ++requestIdRef.current;
    const pageOffset = Math.max(0, Number(opts.pageOffset || 0));
    try {
      if (opts.reset) {
        setLoading(true);
        setError(null);
      } else {
        setAppending(true);
      }
      const res = await api.getLibraryAlbums({ label, sort: 'year_desc', limit, offset: pageOffset, includeUnmatched });
      if (rid !== requestIdRef.current) return;
      const listRaw = Array.isArray(res.albums) ? res.albums : [];
      const list = dedupeAlbumsForDisplay(listRaw);
      const nextTotal = Number(res.total || 0);
      setAlbums((prev) => (opts.reset ? list : mergeAlbumsForDisplay(prev, list)));
      setOffset(pageOffset + listRaw.length);
      setTotal(nextTotal);
      setHasMore(pageOffset + listRaw.length < nextTotal);
    } catch (err) {
      if (rid !== requestIdRef.current) return;
      setError(err instanceof Error ? err.message : 'Failed to load label');
      if (opts.reset) {
        setAlbums([]);
        setOffset(0);
        setTotal(0);
      }
      setHasMore(false);
    } finally {
      if (rid === requestIdRef.current) {
        setLoading(false);
        setAppending(false);
      }
    }
  }, [includeUnmatched, label, limit]);

  useEffect(() => {
    if (!label) {
      setError('Invalid label');
      return;
    }
    setAlbums([]);
    setOffset(0);
    setTotal(0);
    setHasMore(true);
    loadingMoreRef.current = false;
    void loadProfile();
    void loadAlbumsPage({ reset: true, pageOffset: 0 });
  }, [label, includeUnmatched, loadAlbumsPage, loadProfile]);

  useEffect(() => {
    let cancelled = false;
    if (!label) {
      setLabelLiked(false);
      return;
    }
    void (async () => {
      try {
        const res = await api.getLikes('label', undefined, [label]);
        const liked = Boolean((res.items || []).find((item) => String(item.entity_key || '').trim().toLowerCase() === label.toLowerCase())?.liked);
        if (!cancelled) setLabelLiked(liked);
      } catch {
        if (!cancelled) setLabelLiked(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [label]);

  const loadMore = useCallback(async () => {
    if (!hasMore || loading || loadingMoreRef.current) return;
    loadingMoreRef.current = true;
    try {
      await loadAlbumsPage({ reset: false, pageOffset: offset });
    } finally {
      loadingMoreRef.current = false;
    }
  }, [hasMore, loadAlbumsPage, loading, offset]);

  useEffect(() => {
    const node = sentinelRef.current;
    if (!node) return;
    const obs = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            void loadMore();
            break;
          }
        }
      },
      { root: null, rootMargin: '900px 0px 900px 0px', threshold: 0.01 },
    );
    obs.observe(node);
    return () => obs.disconnect();
  }, [loadMore]);

  const topArtists = useMemo(() => profile?.influential_artists || [], [profile?.influential_artists]);
  const topGenres = useMemo(() => profile?.genres || [], [profile?.genres]);
  const backLink = useMemo(
    () => resolveBackLink(location, { path: `/library/labels${location.search || ''}`, label: 'Labels' }),
    [location],
  );

  const toggleLabelLike = useCallback(async () => {
    if (!label) return;
    const next = !labelLiked;
    setLabelLiked(next);
    try {
      await api.setLike({ entity_type: 'label', entity_key: label, liked: next, source: 'ui_label' });
      toast({ title: next ? 'Liked' : 'Unliked', description: next ? 'Label saved to favorites.' : 'Label removed from favorites.' });
    } catch (e) {
      setLabelLiked(!next);
      toast({ title: 'Like failed', description: e instanceof Error ? e.message : 'Failed to update like', variant: 'destructive' });
    }
  }, [label, labelLiked, toast]);

  return (
    <div className="pmda-page-shell pmda-page-stack">
      <div className="flex items-center justify-between gap-3">
        <Button variant="ghost" className="gap-2" onClick={() => navigate(backLink.path)}>
          <ArrowLeft className="w-4 h-4" />
          {`Back to ${backLink.label}`}
        </Button>
        <div className="text-xs text-muted-foreground">
          {total > 0 ? `${Math.min(offset, total).toLocaleString()} / ${total.toLocaleString()}` : `${albums.length.toLocaleString()} loaded`}
        </div>
      </div>

      <Card className="pmda-flat-surface overflow-hidden">
        <CardContent className="p-5 space-y-4">
          <div className="flex items-start justify-between gap-4">
            <div className="min-w-0">
              <h1 className="pmda-page-title break-words">{label || 'Label'}</h1>
              <p className="text-xs text-muted-foreground mt-1">
                {(profile?.album_count || total || 0).toLocaleString()} release{(profile?.album_count || total || 0) !== 1 ? 's' : ''}
              </p>
            </div>
            {error ? (
              <Badge variant="outline" className="text-xs border-destructive/50 text-destructive">
                {error}
              </Badge>
            ) : null}
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <Button type="button" size="sm" variant={labelLiked ? 'default' : 'outline'} className="h-8 gap-2" onClick={() => void toggleLabelLike()}>
              <Heart className={cn('h-4 w-4', labelLiked ? 'fill-current' : '')} />
              {labelLiked ? 'Liked' : 'Like'}
            </Button>
            <ShareDialog
              entityType="label"
              entityKey={label}
              entityLabel={label || 'Label'}
              entitySubtitle={`${(profile?.album_count || total || 0).toLocaleString()} releases`}
              trigger={(
                <Button type="button" size="sm" variant="outline" className="h-8 gap-2">
                  <Share2 className="h-4 w-4" />
                  Share
                </Button>
              )}
            />
            {canUseAI ? (
              <EntityDiscoverDialog
                entityType="label"
                label={label}
                entityLabel={label || 'Label'}
                triggerLabel="Discover"
              />
            ) : null}
          </div>

          {profileLoading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="w-4 h-4 animate-spin" /> Loading label profile…
            </div>
          ) : profile?.description ? (
            <div className="text-sm text-muted-foreground leading-relaxed">{profile.description}</div>
          ) : null}

          <div className="flex flex-wrap items-center gap-2">
            {profile?.owner ? <Badge variant="secondary">Owner: {profile.owner}</Badge> : null}
            {profile?.sub_labels?.slice(0, 8).map((sub) => (
              <Badge key={`sub-${sub}`} variant="outline" className="text-[11px]">{sub}</Badge>
            ))}
          </div>

          <SocialActivityBadges
            entityType="label"
            entityKey={label}
            compact
          />

          {topArtists.length > 0 ? (
            <div className="space-y-2">
              <div className="text-xs font-medium text-muted-foreground">Influential artists</div>
              <ScrollArea className="w-full whitespace-nowrap">
                <div className="flex gap-2 pb-2">
                  {topArtists.map((a) => (
                    <button
                      key={`lab-top-${a.artist_id}`}
                      type="button"
                      className="inline-flex items-center gap-2 border border-border/60 bg-muted/40 px-3 py-1.5 text-[11px] hover:bg-muted transition-colors"
                      onClick={() => navigate(`/library/artist/${a.artist_id}${location.search || ''}`, { state: withBackLinkState(location) })}
                      title="Open artist"
                    >
                      <span className="truncate max-w-[16rem]">{a.artist_name}</span>
                      <span className="text-muted-foreground tabular-nums">{a.album_count}</span>
                    </button>
                  ))}
                </div>
              </ScrollArea>
            </div>
          ) : null}

          {topGenres.length > 0 ? (
            <div className="space-y-2">
              <div className="text-xs font-medium text-muted-foreground">Genres on this label</div>
              <ScrollArea className="w-full whitespace-nowrap">
                <div className="flex gap-2 pb-2">
                  {topGenres.map((g) => (
                    <button
                      key={`lab-gen-${g.genre}`}
                      type="button"
                      className="inline-flex items-center gap-2 border border-border/60 bg-muted/40 px-3 py-1.5 text-[11px] hover:bg-muted transition-colors"
                      onClick={() => navigate(`/library/genre/${encodeURIComponent(g.genre)}${location.search || ''}`, { state: withBackLinkState(location) })}
                      title="Open genre"
                    >
                      <span className="truncate max-w-[16rem]">{g.genre}</span>
                      <span className="text-muted-foreground tabular-nums">{g.count}</span>
                    </button>
                  ))}
                </div>
              </ScrollArea>
            </div>
          ) : null}
        </CardContent>
      </Card>

      {loading && albums.length === 0 ? (
        <Card className="pmda-flat-surface">
          <CardContent className="p-8 text-sm text-muted-foreground">
            <span className="inline-flex items-center gap-2">
              <Loader2 className="w-4 h-4 animate-spin" /> Loading…
            </span>
          </CardContent>
        </Card>
      ) : albums.length === 0 ? (
        <Card className="pmda-flat-surface">
          <CardContent className="p-8 text-sm text-muted-foreground">No releases found for this label.</CardContent>
        </Card>
      ) : (
        <div className="space-y-4">
          <div className="flex justify-end">
            <GridSizeControl value={tileSize} onChange={setTileSize} className="w-full sm:w-[260px]" />
          </div>
          <div className="grid gap-4 justify-start" style={{ gridTemplateColumns }}>
            {albums.map((a) => (
              <div
                key={`lab-alb-${a.album_id}`}
                className="text-left group"
                role="button"
                tabIndex={0}
                onClick={() => navigate(`/library/album/${a.album_id}${location.search || ''}`, { state: withBackLinkState(location) })}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    navigate(`/library/album/${a.album_id}${location.search || ''}`, { state: withBackLinkState(location) });
                  }
                }}
                title="Open album"
              >
                <div className="pmda-flat-tile relative overflow-hidden">
                  <AspectRatio ratio={1} className="bg-muted">
                    <AlbumArtwork albumThumb={a.thumb} artistId={a.artist_id} alt={a.title} size={320} imageClassName="w-full h-full object-cover" />
                    <div className="absolute inset-0 opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity bg-black/25" />
                    <div className="absolute inset-x-0 bottom-0 p-3 opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity">
                      <div className="flex items-center justify-between gap-2">
                        <Button
                          size="sm"
                          className="h-9 gap-2"
                          onClick={(e) => {
                            e.preventDefault();
                            e.stopPropagation();
                            void handlePlayAlbum(a.album_id, a.title, a.thumb);
                          }}
                        >
                          <Play className="h-4 w-4" />
                          Play
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          className="h-9"
                          onClick={(e) => {
                            e.preventDefault();
                            e.stopPropagation();
                            navigate(`/library/artist/${a.artist_id}${location.search || ''}`, { state: withBackLinkState(location) });
                          }}
                          title="Open artist"
                        >
                          <UserRound className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>
                  </AspectRatio>
                  <div className="p-3 space-y-1.5">
                    <div className="text-sm font-semibold leading-snug line-clamp-3 min-h-[3.6rem]" title={a.title}>{a.title}</div>
                    <button
                      type="button"
                      className="text-xs text-muted-foreground truncate hover:underline"
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        navigate(`/library/artist/${a.artist_id}${location.search || ''}`, { state: withBackLinkState(location) });
                      }}
                    >
                      {a.artist_name}
                    </button>
                    <AlbumBadgeGroups
                      show
                      compact
                      userRating={a.user_rating}
                      publicRating={a.public_rating}
                      publicRatingVotes={a.public_rating_votes}
                      format={a.format}
                      isLossless={a.is_lossless}
                      year={a.year}
                      trackCount={a.track_count}
                      genres={a.genres || (a.genre ? [a.genre] : [])}
                      label={a.label}
                      onGenreClick={(genreName) => navigate(`/library/genre/${encodeURIComponent(genreName)}${location.search || ''}`, { state: withBackLinkState(location) })}
                      onLabelClick={a.label ? () => navigate(`/library/label/${encodeURIComponent(a.label || '')}${location.search || ''}`, { state: withBackLinkState(location) }) : undefined}
                    />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div ref={sentinelRef} className="h-6" />
      <div className="flex min-h-6 items-center justify-center py-2 text-xs text-muted-foreground">
        {appending ? (
          <span className="inline-flex items-center gap-2">
            <Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading more…
          </span>
        ) : !hasMore ? 'All loaded' : null}
      </div>
    </div>
  );
}
