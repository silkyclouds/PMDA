import { startTransition, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { ArrowRight, Loader2, Sparkles, UserRound } from 'lucide-react';

import { AlbumArtwork } from '@/components/library/AlbumArtwork';
import { AuthenticatedImage } from '@/components/library/AuthenticatedImage';
import { LibraryLiveStateBadges } from '@/components/library/LibraryLiveStateBadges';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { cn } from '@/lib/utils';
import * as api from '@/lib/api';

function compactNumber(value: unknown): string {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return '0';
  return n.toLocaleString();
}

export function LibraryLiveNowPanel({
  scope,
  includeUnmatched,
  scanProgress,
  variant = 'full',
  className,
}: {
  scope: api.LibraryBrowseScope;
  includeUnmatched: boolean;
  scanProgress?: api.ScanProgress | null;
  variant?: 'full' | 'compact';
  className?: string;
}) {
  const phase = String(scanProgress?.phase || '').trim();
  const liveActive = Boolean(
    scanProgress?.scanning
      || scanProgress?.scan_starting
      || scanProgress?.background_enrichment_running
      || scanProgress?.status === 'running',
  );
  const [artists, setArtists] = useState<api.LibraryArtistItem[]>([]);
  const [albums, setAlbums] = useState<api.LibraryAlbumItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const requestIdRef = useRef(0);

  const visibleAlbums = Number(scanProgress?.library_visible_albums_count ?? scanProgress?.scan_published_albums_count ?? 0);
  const visibleArtists = Number(scanProgress?.library_visible_artists_count ?? 0);
  const publishedThisRun = Number(scanProgress?.scan_published_albums_count ?? 0);
  const enrichedArtistsDone = Number(scanProgress?.profile_backfill?.current ?? 0);
  const enrichedArtistsTotal = Number(scanProgress?.profile_backfill?.total ?? 0);
  const currentPhase = String(scanProgress?.current_stage_human_label || scanProgress?.pipeline_step_human_label || scanProgress?.phase || '').trim();
  const liveLimit = variant === 'compact' ? 4 : 6;
  const showLiveLibraryPanel = liveActive && (
    publishedThisRun > 0
      || visibleAlbums > 0
      || visibleArtists > 0
      || scanProgress?.background_enrichment_running
      || phase === 'export'
      || phase === 'post_processing'
      || phase === 'profile_enrichment'
      || phase === 'background_enrichment'
      || phase === 'finalizing'
  );

  const metrics = useMemo(() => ([
    { key: 'artists', label: 'Visible artists', value: compactNumber(visibleArtists) },
    { key: 'albums', label: 'Visible albums', value: compactNumber(visibleAlbums) },
    { key: 'published', label: 'Published this run', value: compactNumber(publishedThisRun) },
    {
      key: 'profiles',
      label: 'Artist enrich',
      value: enrichedArtistsTotal > 0
        ? `${compactNumber(enrichedArtistsDone)} / ${compactNumber(enrichedArtistsTotal)}`
        : compactNumber(enrichedArtistsDone),
    },
  ]), [enrichedArtistsDone, enrichedArtistsTotal, publishedThisRun, visibleAlbums, visibleArtists]);

  useEffect(() => {
    if (!showLiveLibraryPanel) {
      setArtists([]);
      setAlbums([]);
      setLoading(false);
      setError(null);
      return;
    }

    let cancelled = false;
    let intervalId: ReturnType<typeof setInterval> | null = null;

    const fetchLive = async () => {
      if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return;
      const rid = ++requestIdRef.current;
      setLoading(true);
      try {
        const [artistsRes, albumsRes] = await Promise.all([
          api.getLibraryArtists({
            sort: 'recent',
            limit: liveLimit,
            offset: 0,
            includeUnmatched,
            scope,
            browseSource: 'published',
          }),
          api.getLibraryAlbums({
            sort: 'recent',
            limit: liveLimit,
            offset: 0,
            includeUnmatched,
            scope,
            browseSource: 'published',
          }),
        ]);
        if (cancelled || rid !== requestIdRef.current) return;
        startTransition(() => {
          setArtists(Array.isArray(artistsRes.artists) ? artistsRes.artists : []);
          setAlbums(Array.isArray(albumsRes.albums) ? albumsRes.albums : []);
          setError(null);
        });
      } catch (e) {
        if (cancelled || rid !== requestIdRef.current) return;
        setError(e instanceof Error ? e.message : 'Failed to refresh live library');
      } finally {
        if (!cancelled && rid === requestIdRef.current) setLoading(false);
      }
    };

    void fetchLive();
    intervalId = setInterval(() => {
      void fetchLive();
    }, 15000);

    return () => {
      cancelled = true;
      if (intervalId) clearInterval(intervalId);
    };
  }, [includeUnmatched, liveLimit, scope, showLiveLibraryPanel]);

  if (!showLiveLibraryPanel) return null;

  if (variant === 'compact') {
    const compactArtists = artists.slice(0, 3);
    const compactAlbums = albums.slice(0, 3);
    return (
      <Card className={cn('overflow-hidden border-primary/15 bg-card/80', className)}>
        <div className="border-b border-border/50 px-3 py-3 md:px-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline" className="border-primary/30 bg-primary/10 text-primary">
                  <Sparkles className="mr-1.5 h-3.5 w-3.5" />
                  Library rebuild is in progress
                </Badge>
                {currentPhase ? (
                  <Badge variant="outline" className="border-border/70 bg-background/45 text-muted-foreground">
                    {currentPhase}
                  </Badge>
                ) : null}
                {loading ? (
                  <Badge variant="outline" className="border-border/70 bg-background/45 text-muted-foreground">
                    <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
                    syncing
                  </Badge>
                ) : null}
              </div>
            </div>
            <div className="text-right text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
              <span className="font-semibold tabular-nums text-foreground">{compactNumber(publishedThisRun || visibleAlbums)}</span>
              {' '}albums landing this run
            </div>
          </div>
          {error ? (
            <div className="mt-2 text-xs text-warning">
              {error}
            </div>
          ) : null}
        </div>

        <div className="grid gap-3 px-3 py-3 md:grid-cols-2 md:px-4">
          <div className="min-w-0 space-y-2">
            <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Artists landing now</div>
            <div className="grid gap-1.5">
              {compactArtists.length > 0 ? compactArtists.map((artist) => (
                <Link
                  key={`live-artist-compact-${artist.artist_id}`}
                  to={`/library/artist/${artist.artist_id}`}
                  className="flex min-w-0 items-center gap-2 rounded-xl border border-border/50 bg-background/35 px-2 py-1.5 transition-colors hover:border-primary/30 hover:bg-background/55"
                >
                  <div className="h-8 w-8 shrink-0 overflow-hidden rounded-lg border border-border/60 bg-muted">
                    {(artist.artist_thumb || artist.artist_fallback_thumb) ? (
                      <AuthenticatedImage
                        src={artist.artist_thumb || artist.artist_fallback_thumb}
                        alt={artist.artist_name}
                        className="h-full w-full object-cover"
                      />
                    ) : (
                      <div className="flex h-full w-full items-center justify-center text-muted-foreground">
                        <UserRound className="h-3.5 w-3.5" />
                      </div>
                    )}
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-xs font-medium text-foreground">{artist.artist_name}</div>
                    <div className="text-[11px] text-muted-foreground">{artist.album_count} album{artist.album_count === 1 ? '' : 's'}</div>
                  </div>
                </Link>
              )) : (
                <div className="rounded-xl border border-dashed border-border/50 bg-background/25 px-2 py-2 text-xs text-muted-foreground">
                  Waiting for newly published artists.
                </div>
              )}
            </div>
          </div>

          <div className="min-w-0 space-y-2">
            <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Albums landing now</div>
            <div className="grid gap-1.5">
              {compactAlbums.length > 0 ? compactAlbums.map((album, index) => (
                <Link
                  key={`live-album-compact-${album.album_id}`}
                  to={`/library/album/${album.album_id}`}
                  className="flex min-w-0 items-center gap-2 rounded-xl border border-border/50 bg-background/35 px-2 py-1.5 transition-colors hover:border-primary/30 hover:bg-background/55"
                >
                  <div className="h-8 w-8 shrink-0 overflow-hidden rounded-lg border border-border/60 bg-muted">
                    <AlbumArtwork albumThumb={album.thumb} artistId={album.artist_id} alt={album.title} size={96} priority={index === 0} />
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-xs font-medium text-foreground">{album.title}</div>
                    <div className="truncate text-[11px] text-muted-foreground">{album.artist_name}</div>
                  </div>
                </Link>
              )) : (
                <div className="rounded-xl border border-dashed border-border/50 bg-background/25 px-2 py-2 text-xs text-muted-foreground">
                  Waiting for newly published albums.
                </div>
              )}
            </div>
          </div>
        </div>
      </Card>
    );
  }

  return (
    <div className={cn('px-4 md:px-6 pb-3', className)}>
      <Card className="overflow-hidden border-primary/20 bg-[radial-gradient(circle_at_top_left,rgba(251,146,60,0.16),transparent_35%),linear-gradient(180deg,rgba(15,23,42,0.92),rgba(15,23,42,0.72))]">
        <div className="border-b border-border/60 px-4 py-4 md:px-5">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline" className="border-primary/35 bg-primary/10 text-primary">
                  <Sparkles className="mr-1.5 h-3.5 w-3.5" />
                  Entering library now
                </Badge>
                {currentPhase ? (
                  <Badge variant="outline" className="border-border/70 bg-background/50 text-muted-foreground">
                    {currentPhase}
                  </Badge>
                ) : null}
                {loading ? (
                  <Badge variant="outline" className="border-border/70 bg-background/50 text-muted-foreground">
                    <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
                    Live refresh
                  </Badge>
                ) : null}
              </div>
              <div>
                <h2 className="text-lg font-semibold tracking-tight text-foreground">Library is filling live</h2>
                <p className="max-w-3xl text-sm text-muted-foreground">
                  PMDA is publishing winners while the scan continues, then enriching images and profiles in the background.
                </p>
              </div>
            </div>
            <div className="grid min-w-full gap-2 sm:grid-cols-2 lg:min-w-[22rem] lg:max-w-[24rem]">
              {metrics.map((metric) => (
                <div key={metric.key} className="rounded-2xl border border-border/60 bg-background/40 px-3 py-3">
                  <div className="text-[10px] uppercase tracking-[0.22em] text-muted-foreground">{metric.label}</div>
                  <div className="mt-1 text-lg font-semibold text-foreground">{metric.value}</div>
                </div>
              ))}
            </div>
          </div>
          {error ? (
            <div className="mt-3 text-xs text-amber-100/90">
              {error}
            </div>
          ) : null}
        </div>

        <div className="grid gap-4 px-4 py-4 md:px-5 lg:grid-cols-[1fr_1.35fr]">
          <div className="space-y-3">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-xs uppercase tracking-[0.2em] text-muted-foreground">Artists landing now</div>
                <div className="text-sm text-foreground/80">Newest visible artist buckets in this scope.</div>
              </div>
              <Button type="button" size="sm" variant="ghost" asChild className="h-8 px-2 text-xs">
                <Link to="/library/artists">
                  Open artists
                  <ArrowRight className="ml-1.5 h-3.5 w-3.5" />
                </Link>
              </Button>
            </div>
            <div className="grid gap-2 sm:grid-cols-2">
              {artists.length > 0 ? artists.map((artist) => (
                <Link
                  key={`live-artist-${artist.artist_id}`}
                  to={`/library/artist/${artist.artist_id}`}
                  className="group rounded-2xl border border-border/60 bg-background/35 p-2.5 transition-colors hover:border-primary/35 hover:bg-background/55"
                >
                  <div className="flex items-center gap-3">
                    <div className="h-14 w-14 shrink-0 overflow-hidden rounded-xl border border-border/60 bg-muted">
                      {(artist.artist_thumb || artist.artist_fallback_thumb) ? (
                        <AuthenticatedImage
                          src={artist.artist_thumb || artist.artist_fallback_thumb}
                          alt={artist.artist_name}
                          className="h-full w-full object-cover"
                        />
                      ) : (
                        <div className="flex h-full w-full items-center justify-center text-muted-foreground">
                          <UserRound className="h-5 w-5" />
                        </div>
                      )}
                    </div>
                    <div className="min-w-0 space-y-1">
                      <div className="truncate text-sm font-medium text-foreground">{artist.artist_name}</div>
                      <div className="text-xs text-muted-foreground">{artist.album_count} album{artist.album_count === 1 ? '' : 's'}</div>
                      <LibraryLiveStateBadges item={artist} maxBadges={2} />
                    </div>
                  </div>
                </Link>
              )) : (
                <div className="rounded-2xl border border-dashed border-border/60 bg-background/25 px-3 py-5 text-sm text-muted-foreground sm:col-span-2">
                  PMDA has not published any artist buckets into this scope yet.
                </div>
              )}
            </div>
          </div>

          <div className="space-y-3">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-xs uppercase tracking-[0.2em] text-muted-foreground">Albums landing now</div>
                <div className="text-sm text-foreground/80">Recent winners already visible while the rest of the scan keeps moving.</div>
              </div>
              <Button type="button" size="sm" variant="ghost" asChild className="h-8 px-2 text-xs">
                <Link to="/library/albums">
                  Open albums
                  <ArrowRight className="ml-1.5 h-3.5 w-3.5" />
                </Link>
              </Button>
            </div>
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
              {albums.length > 0 ? albums.map((album, index) => (
                <Link
                  key={`live-album-${album.album_id}`}
                  to={`/library/album/${album.album_id}`}
                  className="group overflow-hidden rounded-2xl border border-border/60 bg-background/35 transition-colors hover:border-primary/35 hover:bg-background/55"
                >
                  <div className="aspect-square border-b border-border/60 bg-muted">
                    <AlbumArtwork albumThumb={album.thumb} artistId={album.artist_id} alt={album.title} size={320} priority={index < 3} />
                  </div>
                  <div className="space-y-2 p-3">
                    <div className="line-clamp-2 text-sm font-medium text-foreground">{album.title}</div>
                    <div className="truncate text-xs text-muted-foreground">{album.artist_name}</div>
                    <LibraryLiveStateBadges item={album} maxBadges={2} className="pt-1" />
                  </div>
                </Link>
              )) : (
                <div className="rounded-2xl border border-dashed border-border/60 bg-background/25 px-3 py-5 text-sm text-muted-foreground sm:col-span-2 xl:col-span-3">
                  PMDA has not published any albums into this scope yet.
                </div>
              )}
            </div>
          </div>
        </div>
      </Card>
    </div>
  );
}
