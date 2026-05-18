import { useState, useEffect } from 'react';
import { Users, Loader2, Music } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { AuthenticatedImage } from '@/components/library/AuthenticatedImage';
import { useToast } from '@/hooks/use-toast';
import { cn } from '@/lib/utils';

interface SimilarArtist {
  name: string;
  mbid?: string;
  type?: string;
  artist_id?: number;
  image_url?: string;
}

interface SimilarArtistsProps {
  artistId: number;
  artistName: string;
}

function isProbablyPlaceholderArtistImageUrl(url: string): boolean {
  const low = (url || '').trim().toLowerCase();
  if (!low) return true;
  const tokens = [
    '2a96cbd8b46e442fc41c2b86b821562f',
    '4128a6eb29f94943c9d206c08e625904',
    'c6f59c1e5e7240a4c0d427abd71f3dbb',
  ];
  if (tokens.some((t) => low.includes(t))) return true;
  if (low.includes('default') && (low.includes('last.fm') || low.includes('lastfm'))) return true;
  return false;
}

function initialsFromName(name: string): string {
  const words = (name || '')
    .trim()
    .split(/\s+/g)
    .filter(Boolean);
  if (words.length === 0) return '?';
  const a = words[0]?.[0] || '?';
  const b = words.length > 1 ? (words[1]?.[0] || '') : '';
  return (a + b).toUpperCase();
}

export function SimilarArtists({ artistId, artistName }: SimilarArtistsProps) {
  const [similarArtists, setSimilarArtists] = useState<SimilarArtist[]>([]);
  const [source, setSource] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const { toast } = useToast();

  const artistKey = (a: SimilarArtist): string => {
    const mbid = (a.mbid || '').trim();
    if (mbid) return `mb:${mbid}`;
    if (a.artist_id && a.artist_id > 0) return `local:${a.artist_id}`;
    return `name:${(a.name || '').trim().toLowerCase()}`;
  };

  const loadSimilarArtists = async () => {
    try {
      setLoading(true);
      const response = await fetch(`/api/library/artist/${artistId}/similar`);
      if (response.ok) {
        const data = await response.json();
        setSimilarArtists(data.similar_artists || []);
        setSource(String(data.source || ''));
      } else {
        if (response.status !== 404) {
          toast({
            title: 'Error',
            description: 'Failed to load similar artists',
            variant: 'destructive',
          });
        }
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: 'Failed to load similar artists',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (artistId) {
      loadSimilarArtists();
    }
  }, [artistId]);

  if (loading) {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="flex items-center justify-center">
            <Loader2 className="w-6 h-6 animate-spin text-primary" />
          </div>
        </CardContent>
      </Card>
    );
  }

  if (similarArtists.length === 0) {
    return (
      <Card>
        <CardContent className="p-6 text-center text-muted-foreground">
          <Users className="w-8 h-8 mx-auto mb-2 opacity-50" />
          <p>No similar artists found</p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <Users className="w-5 h-5" />
                Similar Artists
              </CardTitle>
              <CardDescription>
                Found {similarArtists.length} similar artist(s){source ? ` (source: ${source})` : ''}
              </CardDescription>
            </div>
          </div>
          
          <p className="text-xs text-muted-foreground">
            Similar artists are informational in this build; external automation integrations are disabled.
          </p>
        </div>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-3">
          {similarArtists.map((artist) => (
            <div
              key={artistKey(artist)}
              className="border border-border hover:bg-accent/50 transition-colors"
            >
              <div className="relative aspect-square w-full bg-muted overflow-hidden border-b border-border/60 flex items-center justify-center">
                {artist.image_url && !isProbablyPlaceholderArtistImageUrl(artist.image_url) ? (
                  <AuthenticatedImage
                    src={artist.image_url}
                    alt={artist.name}
                    className="w-full h-full object-cover animate-in fade-in-0 duration-300"
                    loading="lazy"
                    fallback={(
                      <div className="w-full h-full flex items-center justify-center bg-gradient-to-br from-warning/30 via-muted/10 to-success/30 text-lg font-semibold text-foreground/80">
                        {initialsFromName(artist.name)}
                      </div>
                    )}
                  />
                ) : (
                  <div className="w-full h-full flex items-center justify-center bg-gradient-to-br from-warning/30 via-muted/10 to-success/30 text-lg font-semibold text-foreground/80">
                    {initialsFromName(artist.name)}
                  </div>
                )}
              </div>
              <div className="space-y-2 p-4">
                <div className="font-medium leading-tight line-clamp-2 min-h-[2.5rem]">{artist.name}</div>
                <div className="text-xs text-muted-foreground">
                  {artist.type || 'Similar'}
                </div>
                <Badge variant="outline" className={cn("text-xs", artist.mbid ? "" : "opacity-70")}>
                  {artist.mbid ? `${artist.mbid.slice(0, 8)}…` : (artist.artist_id ? `#${artist.artist_id}` : '—')}
                </Badge>
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
