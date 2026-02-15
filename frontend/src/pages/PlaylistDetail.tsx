import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, GripVertical, ListMusic, Loader2, Play, RefreshCw, Trash2 } from 'lucide-react';

import * as api from '@/lib/api';
import { cn } from '@/lib/utils';
import { useToast } from '@/hooks/use-toast';
import { usePlayback } from '@/contexts/PlaybackContext';
import type { TrackInfo } from '@/components/library/AudioPlayer';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog';

function parseDropPayload(e: React.DragEvent): { track_id?: number; album_id?: number } | null {
  const rawTrack = e.dataTransfer.getData('application/x-pmda-track');
  if (rawTrack) {
    try {
      const obj = JSON.parse(rawTrack) as { track_id?: number };
      const tid = Number(obj?.track_id || 0);
      if (Number.isFinite(tid) && tid > 0) return { track_id: tid };
    } catch {
      // ignore
    }
  }
  const rawAlbum = e.dataTransfer.getData('application/x-pmda-album');
  if (rawAlbum) {
    try {
      const obj = JSON.parse(rawAlbum) as { album_id?: number };
      const aid = Number(obj?.album_id || 0);
      if (Number.isFinite(aid) && aid > 0) return { album_id: aid };
    } catch {
      // ignore
    }
  }
  return null;
}

export default function PlaylistDetail() {
  const navigate = useNavigate();
  const params = useParams<{ playlistId: string }>();
  const playlistId = Number(params.playlistId);

  const { toast } = useToast();
  const { startPlayback, setCurrentTrack } = usePlayback();

  const [loading, setLoading] = useState(true);
  const [savingOrder, setSavingOrder] = useState(false);
  const [dropping, setDropping] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [detail, setDetail] = useState<api.PlaylistDetailResponse | null>(null);
  const [deleteOpen, setDeleteOpen] = useState(false);

  const dragItemIdRef = useRef<number | null>(null);
  const [dragOverIndex, setDragOverIndex] = useState<number | null>(null);

  const load = useCallback(async () => {
    if (!Number.isFinite(playlistId) || playlistId <= 0) {
      setError('Invalid playlist id');
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const res = await api.getPlaylist(playlistId);
      setDetail(res);
    } catch (e) {
      setDetail(null);
      setError(e instanceof Error ? e.message : 'Failed to load playlist');
    } finally {
      setLoading(false);
    }
  }, [playlistId]);

  useEffect(() => {
    void load();
  }, [load]);

  const tracksForPlayback = useMemo<TrackInfo[]>(() => {
    const items = detail?.items ?? [];
    return items.map((it) => ({
      track_id: it.track.track_id,
      title: it.track.title,
      artist: it.track.artist_name,
      album: it.track.album_title,
      duration: it.track.duration_sec,
      index: it.track.track_num,
      file_url: it.track.file_url,
    }));
  }, [detail?.items]);

  const handlePlay = () => {
    if (!detail) return;
    if (tracksForPlayback.length === 0) {
      toast({ title: 'Empty playlist', description: 'Add some tracks first.' });
      return;
    }
    startPlayback(-detail.playlist_id, detail.name || 'Playlist', null, tracksForPlayback);
    setCurrentTrack(tracksForPlayback[0]);
  };

  const handleDrop = useCallback(
    async (e: React.DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      if (!detail) return;
      const payload = parseDropPayload(e);
      if (!payload) return;
      setDropping(true);
      try {
        if (payload.track_id) await api.addPlaylistItems(detail.playlist_id, { track_id: payload.track_id });
        if (payload.album_id) await api.addPlaylistItems(detail.playlist_id, { album_id: payload.album_id });
        toast({ title: 'Added', description: 'Appended to playlist.' });
        await load();
      } catch (err) {
        toast({ title: 'Add failed', description: err instanceof Error ? err.message : 'Failed to add', variant: 'destructive' });
      } finally {
        setDropping(false);
      }
    },
    [detail, load, toast]
  );

  const handleReorder = useCallback(
    async (fromItemId: number, toIndex: number) => {
      if (!detail) return;
      const current = [...(detail.items || [])];
      const fromIndex = current.findIndex((x) => x.item_id === fromItemId);
      if (fromIndex < 0 || toIndex < 0 || toIndex >= current.length) return;
      const [moved] = current.splice(fromIndex, 1);
      current.splice(toIndex, 0, moved);
      const next = { ...detail, items: current.map((it, idx) => ({ ...it, position: idx })) };
      setDetail(next);
      setSavingOrder(true);
      try {
        await api.reorderPlaylist(detail.playlist_id, current.map((x) => x.item_id));
      } catch {
        // If it fails, reload from server (source of truth).
        await load();
      } finally {
        setSavingOrder(false);
      }
    },
    [detail, load]
  );

  const removeItem = useCallback(
    async (itemId: number) => {
      if (!detail) return;
      try {
        await api.deletePlaylistItem(detail.playlist_id, itemId);
        setDetail({ ...detail, items: detail.items.filter((x) => x.item_id !== itemId) });
      } catch (e) {
        toast({ title: 'Remove failed', description: e instanceof Error ? e.message : 'Failed to remove', variant: 'destructive' });
      }
    },
    [detail, toast]
  );

  const deletePlaylist = useCallback(async () => {
    if (!detail) return;
    try {
      await api.deletePlaylist(detail.playlist_id);
      toast({ title: 'Playlist deleted', description: detail.name });
      setDeleteOpen(false);
      navigate('/library/playlists');
    } catch (e) {
      toast({ title: 'Delete failed', description: e instanceof Error ? e.message : 'Failed to delete', variant: 'destructive' });
    }
  }, [detail, navigate, toast]);

  if (loading) {
    return (
      <div className="container py-10">
        <div className="flex items-center justify-center py-24">
          <Loader2 className="w-10 h-10 animate-spin text-primary" />
        </div>
      </div>
    );
  }

  if (error || !detail) {
    return (
      <div className="container py-10 space-y-4">
        <Card>
          <CardContent className="p-8 space-y-3 text-center">
            <div className="text-sm text-muted-foreground">{error || 'Playlist not found'}</div>
            <Button variant="outline" onClick={() => navigate('/library/playlists')}>Back</Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="container py-6 space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-2">
          <Button variant="ghost" className="gap-2" onClick={() => navigate('/library/playlists')}>
            <ArrowLeft className="h-4 w-4" />
            Back to Playlists
          </Button>
          <div>
            <h1 className="text-3xl font-bold tracking-tight">{detail.name}</h1>
            <p className="text-sm text-muted-foreground">{detail.description || ' '}</p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline" className="text-[10px]">{detail.items.length} tracks</Badge>
            {savingOrder ? <Badge variant="outline" className="text-[10px]">Saving order…</Badge> : null}
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Button variant="outline" className="gap-2" onClick={() => void load()}>
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
          <Button className="gap-2" onClick={handlePlay}>
            <Play className="h-4 w-4" />
            Play
          </Button>
          <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
            <DialogTrigger asChild>
              <Button variant="destructive" className="gap-2">
                <Trash2 className="h-4 w-4" />
                Delete
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Delete playlist?</DialogTitle>
                <DialogDescription>This cannot be undone.</DialogDescription>
              </DialogHeader>
              <DialogFooter>
                <Button variant="outline" onClick={() => setDeleteOpen(false)}>Cancel</Button>
                <Button variant="destructive" onClick={() => void deletePlaylist()}>Delete</Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      <div
        className={cn(
          'rounded-2xl border border-dashed p-4',
          'bg-muted/20 border-border/70',
          'transition-colors',
          dropping ? 'opacity-70 pointer-events-none' : 'hover:bg-accent/20'
        )}
        onDragOver={(e) => e.preventDefault()}
        onDrop={(e) => void handleDrop(e)}
      >
        <div className="flex items-center gap-2 text-sm">
          <ListMusic className="h-4 w-4 text-primary" />
          <span className="font-medium">Drop tracks here</span>
          {dropping ? <Loader2 className="h-4 w-4 animate-spin ml-1 text-muted-foreground" /> : null}
        </div>
        <div className="text-xs text-muted-foreground mt-1">
          Drag from Now Playing queue (bottom player) into this playlist, or drop onto a playlist in the sidebar.
        </div>
      </div>

      <Card className="border-border/70 overflow-hidden">
        <CardContent className="p-0">
          {detail.items.length === 0 ? (
            <div className="p-10 text-center text-sm text-muted-foreground">Empty playlist.</div>
          ) : (
            <div className="divide-y divide-border/70">
              {detail.items.map((it, idx) => (
                <div
                  key={it.item_id}
                  className={cn(
                    'flex items-center gap-3 px-4 py-3',
                    dragOverIndex === idx ? 'bg-accent/25' : 'bg-background'
                  )}
                  draggable
                  onDragStart={(e) => {
                    dragItemIdRef.current = it.item_id;
                    e.dataTransfer.setData('text/plain', it.track.title);
                    e.dataTransfer.effectAllowed = 'move';
                  }}
                  onDragOver={(e) => {
                    e.preventDefault();
                    setDragOverIndex(idx);
                  }}
                  onDragLeave={() => setDragOverIndex(null)}
                  onDrop={(e) => {
                    e.preventDefault();
                    const from = dragItemIdRef.current;
                    dragItemIdRef.current = null;
                    setDragOverIndex(null);
                    if (from) void handleReorder(from, idx);
                  }}
                >
                  <GripVertical className="h-4 w-4 text-muted-foreground cursor-grab active:cursor-grabbing" />
                  <div className="min-w-0 flex-1">
                    <div className="text-sm font-medium truncate">{it.track.title}</div>
                    <div className="text-xs text-muted-foreground truncate">
                      {it.track.artist_name} · {it.track.album_title}
                    </div>
                  </div>
                  <Button variant="ghost" size="sm" onClick={() => void removeItem(it.item_id)} className="text-muted-foreground hover:text-destructive">
                    Remove
                  </Button>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

