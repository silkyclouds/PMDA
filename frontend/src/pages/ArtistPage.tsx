import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, Disc3, Loader2, Music, Sparkles } from 'lucide-react';
import { Header } from '@/components/Header';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent } from '@/components/ui/card';
import { AspectRatio } from '@/components/ui/aspect-ratio';
import { ScrollArea } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';
import { FormatBadge } from '@/components/FormatBadge';

interface SimilarArtist {
  name: string;
  mbid?: string;
  type?: string;
}

interface ArtistProfile {
  bio?: string;
  short_bio?: string;
  tags?: string[];
  similar_artists?: SimilarArtist[];
  source?: string;
  updated_at?: number;
}

interface AlbumInfo {
  album_id: number;
  title: string;
  year?: number;
  date?: string;
  track_count: number;
  type: string;
  thumb?: string;
  format?: string;
  is_lossless?: boolean;
  short_description?: string;
}

interface ArtistDetailResponse {
  artist_id: number;
  artist_name: string;
  artist_thumb?: string;
  albums: AlbumInfo[];
  total_albums: number;
  artist_profile?: ArtistProfile;
  profile_enriching?: boolean;
}

interface ArtistProfileResponse {
  artist_id: number;
  artist_name: string;
  profile?: ArtistProfile;
  enriching?: boolean;
}

const albumTypeOrder = ['Album', 'EP', 'Single', 'Compilation', 'Anthology'];

export default function ArtistPage() {
  const navigate = useNavigate();
  const params = useParams<{ artistId: string }>();
  const artistId = Number(params.artistId);

  const [loading, setLoading] = useState(true);
  const [details, setDetails] = useState<ArtistDetailResponse | null>(null);
  const [profile, setProfile] = useState<ArtistProfile | null>(null);
  const [profileEnriching, setProfileEnriching] = useState(false);
  const [fallbackSimilar, setFallbackSimilar] = useState<SimilarArtist[]>([]);
  const [error, setError] = useState<string | null>(null);

  const fetchArtist = useCallback(async () => {
    if (!Number.isFinite(artistId) || artistId <= 0) {
      setError('Invalid artist id');
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`/api/library/artist/${artistId}`);
      if (!res.ok) throw new Error('Failed to load artist');
      const data = (await res.json()) as ArtistDetailResponse;
      setDetails(data);
      setProfile(data.artist_profile ?? null);
      setProfileEnriching(Boolean(data.profile_enriching));
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load artist');
    } finally {
      setLoading(false);
    }
  }, [artistId]);

  const fetchProfile = useCallback(
    async (refresh: boolean) => {
      if (!Number.isFinite(artistId) || artistId <= 0) return false;
      try {
        const res = await fetch(`/api/library/artist/${artistId}/profile${refresh ? '?refresh=1' : ''}`);
        if (!res.ok) return false;
        const data = (await res.json()) as ArtistProfileResponse;
        if (data.profile) {
          setProfile(data.profile);
        }
        setProfileEnriching(Boolean(data.enriching));
        return Boolean(data.enriching);
      } catch {
        return false;
      }
    },
    [artistId]
  );

  useEffect(() => {
    fetchArtist();
  }, [fetchArtist]);

  useEffect(() => {
    if (!details) return;
    let cancelled = false;
    let attempts = 0;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const run = async () => {
      attempts += 1;
      const refreshing = attempts === 1;
      const stillEnriching = await fetchProfile(refreshing);
      if (!cancelled && stillEnriching && attempts < 12) {
        timer = setTimeout(run, 1500);
      }
    };
    run();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [details, fetchProfile]);

  useEffect(() => {
    if (!details) return;
    if ((profile?.similar_artists || []).length > 0) return;
    let cancelled = false;
    const run = async () => {
      try {
        const res = await fetch(`/api/library/artist/${details.artist_id}/similar`);
        if (!res.ok) return;
        const data = await res.json();
        if (cancelled) return;
        const list = Array.isArray(data?.similar_artists) ? data.similar_artists : [];
        setFallbackSimilar(
          list
            .map((x: unknown) => {
              if (!x || typeof x !== 'object') {
                return { name: '' };
              }
              const obj = x as Record<string, unknown>;
              const name = typeof obj.name === 'string' ? obj.name : String(obj.name ?? '');
              return {
                name,
                mbid: typeof obj.mbid === 'string' ? obj.mbid : undefined,
                type: typeof obj.type === 'string' ? obj.type : undefined,
              };
            })
            .filter((entry) => entry.name.length > 0)
        );
      } catch {
        // no-op
      }
    };
    run();
    return () => {
      cancelled = true;
    };
  }, [details, profile?.similar_artists]);

  const grouped = useMemo(() => {
    const src = details?.albums ?? [];
    const map: Record<string, AlbumInfo[]> = {};
    for (const album of src) {
      const t = album.type || 'Album';
      if (!map[t]) map[t] = [];
      map[t].push(album);
    }
    return map;
  }, [details?.albums]);

  const sortedTypes = useMemo(() => {
    return Object.keys(grouped).sort((a, b) => {
      const ai = albumTypeOrder.indexOf(a);
      const bi = albumTypeOrder.indexOf(b);
      if (ai === -1 && bi === -1) return a.localeCompare(b);
      if (ai === -1) return 1;
      if (bi === -1) return -1;
      return ai - bi;
    });
  }, [grouped]);

  if (loading) {
    return (
      <>
        <Header />
        <div className="container py-8">
          <div className="flex items-center justify-center py-24">
            <Loader2 className="w-10 h-10 animate-spin text-primary" />
          </div>
        </div>
      </>
    );
  }

  if (error || !details) {
    return (
      <>
        <Header />
        <div className="container py-8">
          <Card>
            <CardContent className="p-8 space-y-4 text-center">
              <p className="text-muted-foreground">{error || 'Artist not found'}</p>
              <Button variant="outline" onClick={() => navigate('/library')}>
                Back to Library
              </Button>
            </CardContent>
          </Card>
        </div>
      </>
    );
  }

  const tags = (profile?.tags || []).slice(0, 8);
  const similar = ((profile?.similar_artists && profile.similar_artists.length > 0)
    ? profile.similar_artists
    : fallbackSimilar
  ).slice(0, 12);
  const heroImage = details.artist_thumb || null;

  return (
    <>
      <Header />
      <div className="container py-6 space-y-6">
        <div className="flex items-center justify-between gap-3">
          <Button variant="ghost" className="gap-2" onClick={() => navigate('/library')}>
            <ArrowLeft className="w-4 h-4" />
            Back to Library
          </Button>
          {profileEnriching && (
            <Badge variant="outline" className="gap-1.5">
              <Loader2 className="w-3 h-3 animate-spin" />
              Enriching profile
            </Badge>
          )}
        </div>

        <Card className="overflow-hidden border-border/70">
          <div className="relative">
            <div className="absolute inset-0 bg-gradient-to-r from-background via-background/90 to-background/70 z-10" />
            {heroImage ? (
              <img src={heroImage} alt={details.artist_name} className="w-full h-64 object-cover blur-[1px] scale-105" />
            ) : (
              <div className="h-64 bg-gradient-to-br from-zinc-900 via-zinc-800 to-zinc-900" />
            )}
            <div className="absolute inset-0 z-20 p-6 md:p-8 flex items-end">
              <div className="flex items-end gap-5 w-full">
                <div className="w-24 h-24 md:w-28 md:h-28 rounded-2xl overflow-hidden border border-border/60 bg-muted shrink-0">
                  {heroImage ? (
                    <img src={heroImage} alt={details.artist_name} className="w-full h-full object-cover" />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center">
                      <Music className="w-8 h-8 text-muted-foreground" />
                    </div>
                  )}
                </div>
                <div className="min-w-0">
                  <h1 className="text-3xl md:text-4xl font-bold tracking-tight truncate">{details.artist_name}</h1>
                  <p className="text-sm text-muted-foreground mt-1">
                    {details.total_albums.toLocaleString()} album{details.total_albums !== 1 ? 's' : ''}
                  </p>
                  {tags.length > 0 && (
                    <div className="flex flex-wrap gap-1.5 mt-3">
                      {tags.map((tag) => (
                        <Badge key={tag} variant="secondary" className="text-[11px]">
                          {tag}
                        </Badge>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
          {(profile?.short_bio || profile?.bio) && (
            <CardContent className="pt-4 pb-5">
              <p className="text-sm leading-relaxed text-muted-foreground">
                {profile?.short_bio || profile?.bio}
              </p>
            </CardContent>
          )}
        </Card>

        <div className="space-y-6">
          {sortedTypes.map((type) => (
            <section key={type} className="space-y-3">
              <div className="flex items-center gap-2">
                <Disc3 className="w-4 h-4 text-primary" />
                <h2 className="text-lg font-semibold">{type === 'Single' ? 'Singles' : `${type}s`}</h2>
              </div>
              <ScrollArea className="w-full whitespace-nowrap">
                <div className="flex gap-4 pb-2">
                  {grouped[type].map((album) => (
                    <Card key={album.album_id} className="w-[200px] shrink-0 overflow-hidden border-border/70">
                      <AspectRatio ratio={1} className="bg-muted">
                        {album.thumb ? (
                          <img src={album.thumb} alt={album.title} className="w-full h-full object-cover" />
                        ) : (
                          <div className="w-full h-full flex items-center justify-center">
                            <Music className="w-10 h-10 text-muted-foreground" />
                          </div>
                        )}
                      </AspectRatio>
                      <CardContent className="p-3 space-y-2">
                        <h3 className="text-sm font-semibold truncate" title={album.title}>
                          {album.title}
                        </h3>
                        <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground">
                          <span>{album.year || album.date || 'Unknown'}</span>
                          <span>{album.track_count} tracks</span>
                        </div>
                        <div className="flex items-center gap-1.5 flex-wrap">
                          {album.format && <FormatBadge format={album.format} size="sm" />}
                          <Badge variant={album.is_lossless ? 'secondary' : 'outline'} className="text-[10px]">
                            {album.is_lossless ? 'Lossless' : 'Lossy'}
                          </Badge>
                        </div>
                        {album.short_description && (
                          <p className={cn('text-[11px] text-muted-foreground line-clamp-3')}>
                            {album.short_description}
                          </p>
                        )}
                      </CardContent>
                    </Card>
                  ))}
                </div>
              </ScrollArea>
            </section>
          ))}
        </div>

        <section className="space-y-3">
          <div className="flex items-center gap-2">
            <Sparkles className="w-4 h-4 text-primary" />
            <h2 className="text-lg font-semibold">Similar Artists</h2>
          </div>
          {similar.length === 0 ? (
            <Card>
              <CardContent className="p-5 text-sm text-muted-foreground">No similar artists available yet.</CardContent>
            </Card>
          ) : (
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-3">
              {similar.map((artist) => (
                <Card key={`${artist.name}-${artist.mbid || ''}`} className="border-border/70">
                  <CardContent className="p-3 space-y-2">
                    <div className="w-12 h-12 rounded-full bg-muted mx-auto flex items-center justify-center">
                      <Music className="w-5 h-5 text-muted-foreground" />
                    </div>
                    <p className="text-xs font-medium text-center line-clamp-2 min-h-[2.2rem]">{artist.name}</p>
                    {artist.type ? (
                      <p className="text-[10px] text-muted-foreground text-center truncate">{artist.type}</p>
                    ) : null}
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
        </section>
      </div>
    </>
  );
}
