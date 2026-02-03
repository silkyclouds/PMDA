import { useState, useEffect, useCallback } from 'react';
import { Search, Music, Loader2, Tag, Filter } from 'lucide-react';
import { Header } from '@/components/Header';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useToast } from '@/hooks/use-toast';

interface MissingTagAlbum {
  album_id: number;
  album_title: string;
  artist_name: string;
  year?: number;
  missing_tags: string[];
}

export default function TagFixer() {
  const [loading, setLoading] = useState(false);
  const [scanning, setScanning] = useState(false);
  const [albums, setAlbums] = useState<MissingTagAlbum[]>([]);
  const [filteredAlbums, setFilteredAlbums] = useState<MissingTagAlbum[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [filterArtist, setFilterArtist] = useState<string>('all');
  const [artists, setArtists] = useState<string[]>([]);
  const { toast } = useToast();

  const loadMissingTags = useCallback(async (showToast = false) => {
    setScanning(true);
    try {
      const response = await fetch('/api/library/missing-tags');
      if (response.ok) {
        const data = await response.json();
        const list = data.albums || [];
        setAlbums(list);
        setFilteredAlbums(list);
        const uniqueArtists = Array.from(new Set(list.map((a: MissingTagAlbum) => a.artist_name))).sort() as string[];
        setArtists(uniqueArtists);
        if (showToast) {
          toast({
            title: 'Scan complete',
            description: `Found ${list.length} album(s) with missing tags`,
          });
        }
      } else {
        throw new Error('Failed to scan for missing tags');
      }
    } catch (error) {
      if (showToast) {
        toast({
          title: 'Error',
          description: 'Failed to scan for missing tags',
          variant: 'destructive',
        });
      }
    } finally {
      setScanning(false);
    }
  }, [toast]);

  useEffect(() => {
    loadMissingTags(false);
  }, [loadMissingTags]);

  useEffect(() => {
    let filtered = albums;
    
    if (searchQuery) {
      filtered = filtered.filter(a =>
        a.album_title.toLowerCase().includes(searchQuery.toLowerCase()) ||
        a.artist_name.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }
    
    if (filterArtist !== 'all') {
      filtered = filtered.filter(a => a.artist_name === filterArtist);
    }
    
    setFilteredAlbums(filtered);
  }, [searchQuery, filterArtist, albums]);

  return (
    <>
      <Header />
      <div className="container py-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold">Tag Fixer</h1>
            <p className="text-sm text-muted-foreground mt-1">
              Find and fix albums with missing MusicBrainz tags
            </p>
          </div>
          <Button onClick={() => loadMissingTags(true)} disabled={scanning} className="gap-1.5">
            {scanning ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Search className="w-4 h-4" />
            )}
            {scanning ? 'Scanning...' : 'Scan Library'}
          </Button>
        </div>

        {albums.length > 0 && (
          <div className="mb-4 flex items-center gap-4">
            <div className="relative flex-1 max-w-sm">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input
                placeholder="Search albums..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-9"
              />
            </div>
            <Select value={filterArtist} onValueChange={setFilterArtist}>
              <SelectTrigger className="w-[200px]">
                <SelectValue placeholder="Filter by artist" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Artists</SelectItem>
                {artists.map(artist => (
                  <SelectItem key={artist} value={artist}>{artist}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <div className="text-sm text-muted-foreground">
              {filteredAlbums.length} / {albums.length} albums
            </div>
          </div>
        )}

        {filteredAlbums.length > 0 ? (
          <div className="grid gap-4">
            {filteredAlbums.map((album) => (
              <Card key={album.album_id}>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div className="flex-1 min-w-0">
                      <CardTitle className="truncate">{album.album_title}</CardTitle>
                      <CardDescription>
                        {album.artist_name} {album.year && `• ${album.year}`}
                      </CardDescription>
                    </div>
                    <div className="flex items-center gap-2 shrink-0">
                      {album.missing_tags.length > 0 && (
                        <Badge variant="outline" className="gap-1">
                          <Tag className="w-3 h-3" />
                          {album.missing_tags.length} missing
                        </Badge>
                      )}
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  {album.missing_tags.length > 0 && (
                    <div className="text-xs text-muted-foreground">
                      Missing: {album.missing_tags.join(', ')}
                    </div>
                  )}
                </CardContent>
              </Card>
            ))}
          </div>
        ) : albums.length === 0 ? (
          <Card>
            <CardContent className="p-12 text-center">
              <Music className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
              <p className="text-muted-foreground">
                  {scanning ? 'Loading...' : 'No albums with missing tags. Use "Scan Library" to refresh.'}
              </p>
            </CardContent>
          </Card>
        ) : (
          <Card>
            <CardContent className="p-12 text-center">
              <Filter className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
              <p className="text-muted-foreground">
                No albums match your filters
              </p>
            </CardContent>
          </Card>
        )}
      </div>
    </>
  );
}
