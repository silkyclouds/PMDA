import { useState, useEffect } from 'react';
import { Users, Plus, Loader2, Music } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Checkbox } from '@/components/ui/checkbox';
import { Badge } from '@/components/ui/badge';
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
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [adding, setAdding] = useState(false);
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

  const handleToggle = (key: string) => {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  const handleAddSelected = async () => {
    if (selected.size === 0) return;
    
    setAdding(true);
    try {
      const artistsToAdd = similarArtists.filter((a) => selected.has(artistKey(a)));
      
      // Create Autobrr filter
      const artistNames = artistsToAdd.map(a => a.name);
      const response = await fetch('/api/autobrr/create-filter', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          artist_names: artistNames,
        }),
      });

      if (response.ok) {
        const result = await response.json();
        toast({
          title: 'Success',
          description: result.message,
        });
        setSelected(new Set());
      } else {
        const error = await response.json();
        throw new Error(error.message || 'Failed to create Autobrr filter');
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to add artists to Autobrr',
        variant: 'destructive',
      });
    } finally {
      setAdding(false);
    }
  };

  const handleAddAll = () => {
    setSelected(new Set(similarArtists.map((a) => artistKey(a))));
    handleAddSelected();
  };

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
            Add selected artists to Autobrr as a filter for automated monitoring.
          </p>
          
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              variant="outline"
              onClick={handleAddSelected}
              disabled={selected.size === 0 || adding}
              className="gap-1.5"
            >
              {adding ? (
                <Loader2 className="w-3 h-3 animate-spin" />
              ) : (
                <Plus className="w-3 h-3" />
              )}
              Add Selected ({selected.size})
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={handleAddAll}
              disabled={adding}
              className="gap-1.5"
            >
              <Plus className="w-3 h-3" />
              Add All
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <div className="space-y-2">
          {similarArtists.map((artist) => (
            <div
              key={artistKey(artist)}
              className="flex items-center gap-3 p-3 rounded-lg border border-border hover:bg-accent/50 transition-colors"
            >
              <Checkbox
                checked={selected.has(artistKey(artist))}
                onCheckedChange={() => handleToggle(artistKey(artist))}
              />
              <div className="w-9 h-9 rounded-full bg-muted overflow-hidden flex items-center justify-center border border-border/60 shrink-0">
                {artist.image_url && !isProbablyPlaceholderArtistImageUrl(artist.image_url) ? (
                  <img
                    src={artist.image_url}
                    alt={artist.name}
                    className="w-full h-full object-cover animate-in fade-in-0 duration-300"
                    loading="lazy"
                  />
                ) : (
                  <div className="w-full h-full flex items-center justify-center bg-gradient-to-br from-amber-500/30 via-slate-500/10 to-emerald-500/30 text-[10px] font-semibold text-foreground/80">
                    {initialsFromName(artist.name)}
                  </div>
                )}
              </div>
              <div className="flex-1 min-w-0">
                <div className="font-medium truncate">{artist.name}</div>
                <div className="text-xs text-muted-foreground">
                  {artist.type || 'Similar'}
                </div>
              </div>
              <Badge variant="outline" className={cn("text-xs", artist.mbid ? "" : "opacity-70")}>
                {artist.mbid ? `${artist.mbid.slice(0, 8)}…` : (artist.artist_id ? `#${artist.artist_id}` : '—')}
              </Badge>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
