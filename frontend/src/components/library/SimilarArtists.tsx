import { useState, useEffect } from 'react';
import { Users, Plus, Loader2, Radio } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Checkbox } from '@/components/ui/checkbox';
import { Badge } from '@/components/ui/badge';
import { useToast } from '@/hooks/use-toast';

interface SimilarArtist {
  name: string;
  mbid: string;
  type: string;
}

interface SimilarArtistsProps {
  artistId: number;
  artistName: string;
}

export function SimilarArtists({ artistId, artistName }: SimilarArtistsProps) {
  const [similarArtists, setSimilarArtists] = useState<SimilarArtist[]>([]);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [adding, setAdding] = useState(false);
  const { toast } = useToast();

  const loadSimilarArtists = async () => {
    try {
      setLoading(true);
      const response = await fetch(`/api/library/artist/${artistId}/similar`);
      if (response.ok) {
        const data = await response.json();
        setSimilarArtists(data.similar_artists || []);
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

  const handleToggle = (mbid: string) => {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(mbid)) {
        next.delete(mbid);
      } else {
        next.add(mbid);
      }
      return next;
    });
  };

  const handleAddSelected = async () => {
    if (selected.size === 0) return;
    
    setAdding(true);
    try {
      const artistsToAdd = similarArtists.filter(a => selected.has(a.mbid));
      
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
    setSelected(new Set(similarArtists.map(a => a.mbid)));
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
                Found {similarArtists.length} similar artist(s) via MusicBrainz
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
              key={artist.mbid}
              className="flex items-center gap-3 p-3 rounded-lg border border-border hover:bg-accent/50 transition-colors"
            >
              <Checkbox
                checked={selected.has(artist.mbid)}
                onCheckedChange={() => handleToggle(artist.mbid)}
              />
              <div className="flex-1 min-w-0">
                <div className="font-medium truncate">{artist.name}</div>
                <div className="text-xs text-muted-foreground">
                  {artist.type}
                </div>
              </div>
              <Badge variant="outline" className="text-xs">
                {artist.mbid.slice(0, 8)}...
              </Badge>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
