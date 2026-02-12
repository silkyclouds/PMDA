import { useState, useEffect } from 'react';
import { AlertCircle, Music, Loader2 } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Header } from '@/components/Header';
import * as api from '@/lib/api';
import { useToast } from '@/hooks/use-toast';

export function BrokenAlbumsList() {
  const [brokenAlbums, setBrokenAlbums] = useState<api.BrokenAlbum[]>([]);
  const [loading, setLoading] = useState(true);
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

  return (
    <>
      <Header />
      <div className="container py-6 space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-2xl font-bold">Incomplete Albums</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Albums with missing tracks detected by PMDA.
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
                      <Badge variant="outline" className="bg-muted">
                        Legacy Lidarr flag
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

                    <p className="text-xs text-muted-foreground pt-2">
                      Configure <span className="font-medium">Move incomplete albums</span> in Settings → Automation to quarantine incomplete releases automatically.
                    </p>
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
