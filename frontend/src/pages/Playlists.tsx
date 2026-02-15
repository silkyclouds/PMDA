import { useCallback, useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ListMusic, Loader2, Plus } from 'lucide-react';

import * as api from '@/lib/api';
import { useToast } from '@/hooks/use-toast';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Badge } from '@/components/ui/badge';

export default function Playlists() {
  const navigate = useNavigate();
  const { toast } = useToast();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [playlists, setPlaylists] = useState<api.PlaylistSummary[]>([]);

  const [createOpen, setCreateOpen] = useState(false);
  const [createName, setCreateName] = useState('');
  const [createDescription, setCreateDescription] = useState('');
  const [creating, setCreating] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.getPlaylists();
      setPlaylists(Array.isArray(res.playlists) ? res.playlists : []);
    } catch (e) {
      setPlaylists([]);
      setError(e instanceof Error ? e.message : 'Failed to load playlists');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const create = useCallback(async () => {
    const name = createName.trim();
    if (!name) return;
    setCreating(true);
    try {
      const pl = await api.createPlaylist({ name, description: createDescription.trim() || undefined });
      toast({ title: 'Playlist created', description: pl.name });
      setCreateOpen(false);
      setCreateName('');
      setCreateDescription('');
      await load();
      navigate(`/library/playlists/${pl.playlist_id}`);
    } catch (e) {
      toast({ title: 'Create failed', description: e instanceof Error ? e.message : 'Failed to create playlist', variant: 'destructive' });
    } finally {
      setCreating(false);
    }
  }, [createDescription, createName, load, navigate, toast]);

  return (
    <div className="container py-6 space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-1">
          <h1 className="text-3xl font-bold tracking-tight">Playlists</h1>
          <p className="text-sm text-muted-foreground">
            Local playlists stored in PostgreSQL. Drag tracks from the Now Playing queue onto a playlist in the sidebar.
          </p>
        </div>
        <Dialog open={createOpen} onOpenChange={setCreateOpen}>
          <DialogTrigger asChild>
            <Button className="gap-2">
              <Plus className="h-4 w-4" />
              New playlist
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Create playlist</DialogTitle>
              <DialogDescription>Name it, then start dragging tracks into it.</DialogDescription>
            </DialogHeader>
            <div className="space-y-3">
              <div className="space-y-1">
                <div className="text-sm font-medium">Name</div>
                <Input value={createName} onChange={(e) => setCreateName(e.target.value)} placeholder="e.g. Late-night headphones" />
              </div>
              <div className="space-y-1">
                <div className="text-sm font-medium">Description (optional)</div>
                <Textarea value={createDescription} onChange={(e) => setCreateDescription(e.target.value)} placeholder="Mood, gear, notes…" />
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setCreateOpen(false)} disabled={creating}>
                Cancel
              </Button>
              <Button onClick={() => void create()} disabled={creating || !createName.trim()} className="gap-2">
                {creating ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                Create
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-16 text-muted-foreground">
          <Loader2 className="h-5 w-5 animate-spin mr-2" />
          Loading playlists…
        </div>
      ) : error ? (
        <Card className="border-destructive/40">
          <CardContent className="p-6 text-sm text-destructive">{error}</CardContent>
        </Card>
      ) : playlists.length === 0 ? (
        <Card>
          <CardContent className="p-10 text-center space-y-3">
            <div className="mx-auto h-12 w-12 rounded-full bg-muted flex items-center justify-center">
              <ListMusic className="h-6 w-6 text-muted-foreground" />
            </div>
            <div className="text-sm text-muted-foreground">No playlists yet.</div>
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          {playlists.map((pl) => (
            <button
              key={pl.playlist_id}
              type="button"
              onClick={() => navigate(`/library/playlists/${pl.playlist_id}`)}
              className="text-left"
            >
              <Card className="h-full border-border/70 hover:border-primary/40 hover:bg-accent/20 transition-colors">
                <CardHeader className="pb-3">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <CardTitle className="text-base truncate">{pl.name}</CardTitle>
                      <CardDescription className="line-clamp-2">{pl.description || ' '}</CardDescription>
                    </div>
                    <Badge variant="outline" className="shrink-0 text-[10px]">
                      {pl.item_count} tracks
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent className="pt-0">
                  <div className="text-xs text-muted-foreground">
                    {pl.updated_at ? `Updated ${new Date(pl.updated_at * 1000).toLocaleDateString()}` : ' '}
                  </div>
                </CardContent>
              </Card>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

