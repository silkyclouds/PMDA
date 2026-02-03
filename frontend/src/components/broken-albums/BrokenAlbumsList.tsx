import { useState, useEffect } from 'react';
import { AlertCircle, Music, Send, Loader2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Header } from '@/components/Header';
import * as api from '@/lib/api';
import { useToast } from '@/hooks/use-toast';

export function BrokenAlbumsList() {
  const [brokenAlbums, setBrokenAlbums] = useState<api.BrokenAlbum[]>([]);
  const [loading, setLoading] = useState(true);
  const [sending, setSending] = useState<Set<number>>(new Set());
  const { toast } = useToast();

  useEffect(() => {
    loadBrokenAlbums();
  }, []);

  const loadBrokenAlbums = async () => {
    try {
      setLoading(true);
      const albums = await api.getBrokenAlbums();
      setBrokenAlbums(albums);
    } catch (error) {
      toast({
        title: 'Error',
        description: 'Failed to load incomplete albums',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  const handleSendToLidarr = async (album: api.BrokenAlbum) => {
    if (!album.musicbrainz_release_group_id) {
      toast({
        title: 'Error',
        description: 'Album missing MusicBrainz ID',
        variant: 'destructive',
      });
      return;
    }

    setSending(prev => new Set(prev).add(album.album_id));
    try {
      const result = await api.addAlbumToLidarr(album);
      if (result.success) {
        toast({
          title: 'Success',
          description: result.message,
        });
        // Update local state
        setBrokenAlbums(prev => prev.map(a => 
          a.album_id === album.album_id ? { ...a, sent_to_lidarr: true } : a
        ));
      } else {
        throw new Error(result.message);
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to send album to Lidarr',
        variant: 'destructive',
      });
    } finally {
      setSending(prev => {
        const next = new Set(prev);
        next.delete(album.album_id);
        return next;
      });
    }
  };

  return (
    <>
      <Header />
      <div className="container py-6 space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-2xl font-bold">Incomplete Albums</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Albums with missing tracks detected. Send them to Lidarr for re-download.
            </p>
          </div>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="w-8 h-8 animate-spin text-primary" />
          </div>
        ) : brokenAlbums.length === 0 ? (
          <Alert>
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>
              No incomplete albums found. All albums appear to have complete track listings.
            </AlertDescription>
          </Alert>
        ) : (
          <div className="grid gap-4">
            {brokenAlbums.map((album) => (
              <Card key={`${album.artist}-${album.album_id}`}>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <CardTitle className="flex items-center gap-2">
                        <Music className="w-5 h-5 text-muted-foreground" />
                        {album.album_title}
                      </CardTitle>
                      <CardDescription className="mt-1">
                        {album.artist}
                      </CardDescription>
                    </div>
                    {album.sent_to_lidarr && (
                      <Badge variant="outline" className="bg-green-500/10 text-green-600 dark:text-green-400">
                        Sent to Lidarr
                      </Badge>
                    )}
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    <div className="flex items-center gap-4 text-sm">
                      <div>
                        <span className="text-muted-foreground">Tracks found:</span>{' '}
                        <span className="font-medium">{album.actual_track_count}</span>
                        {album.expected_track_count && (
                          <>
                            {' '}/ <span className="text-muted-foreground">{album.expected_track_count} expected</span>
                          </>
                        )}
                      </div>
                      {album.missing_indices.length > 0 && (
                        <div>
                          <span className="text-muted-foreground">Missing gaps:</span>{' '}
                          <span className="font-medium">
                            {album.missing_indices.map(([start, end]) => `${start}-${end}`).join(', ')}
                          </span>
                        </div>
                      )}
                    </div>

                    {album.musicbrainz_release_group_id && (
                      <div className="text-xs text-muted-foreground">
                        MusicBrainz ID: {album.musicbrainz_release_group_id}
                      </div>
                    )}

                    <div className="flex items-center gap-2 pt-2">
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => handleSendToLidarr(album)}
                        disabled={album.sent_to_lidarr || sending.has(album.album_id) || !album.musicbrainz_release_group_id}
                        className="gap-1.5"
                      >
                        {sending.has(album.album_id) ? (
                          <Loader2 className="w-3 h-3 animate-spin" />
                        ) : (
                          <Send className="w-3 h-3" />
                        )}
                        {album.sent_to_lidarr ? 'Sent to Lidarr' : 'Send to Lidarr'}
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        )}
      </div>
    </>
  );
}
