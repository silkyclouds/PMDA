import { useCallback, useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { BarChart3, Library, ListMusic, Plus, Scan, Settings2, Wrench } from 'lucide-react';

import * as api from '@/lib/api';
import { cn } from '@/lib/utils';
import { useToast } from '@/hooks/use-toast';
import { Button } from '@/components/ui/button';
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupAction,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuBadge,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
  SidebarSeparator,
} from '@/components/ui/sidebar';
import { Logo } from '@/components/Logo';

function isPathActive(pathname: string, target: string): boolean {
  if (target === '/library') {
    return pathname === '/library' || pathname.startsWith('/library/');
  }
  return pathname === target || pathname.startsWith(`${target}/`);
}

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

export function AppSidebar() {
  const { toast } = useToast();
  const location = useLocation();
  const navigate = useNavigate();

  const [playlists, setPlaylists] = useState<api.PlaylistSummary[]>([]);
  const [loadingPlaylists, setLoadingPlaylists] = useState(false);
  const [dragOverPlaylistId, setDragOverPlaylistId] = useState<number | null>(null);

  const refreshPlaylists = useCallback(async () => {
    try {
      setLoadingPlaylists(true);
      const res = await api.getPlaylists();
      setPlaylists(Array.isArray(res.playlists) ? res.playlists : []);
    } catch {
      setPlaylists([]);
    } finally {
      setLoadingPlaylists(false);
    }
  }, []);

  useEffect(() => {
    void refreshPlaylists();
    const t = setInterval(() => void refreshPlaylists(), 60_000);
    return () => clearInterval(t);
  }, [refreshPlaylists]);

  const mainItems = useMemo(
    () => [
      { to: '/', label: 'Scan', icon: Scan },
      { to: '/library', label: 'Library', icon: Library },
      { to: '/library/playlists', label: 'Playlists', icon: ListMusic },
    ],
    []
  );

  const adminItems = useMemo(
    () => [
      { to: '/tools', label: 'Tools', icon: Wrench },
      { to: '/statistics', label: 'Statistics', icon: BarChart3 },
      { to: '/settings', label: 'Settings', icon: Settings2 },
    ],
    []
  );

  const playlistItems = useMemo(() => playlists.slice(0, 10), [playlists]);

  const handleDropOnPlaylist = async (playlistId: number, e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragOverPlaylistId(null);
    const payload = parseDropPayload(e);
    if (!payload) return;
    try {
      if (payload.track_id) {
        await api.addPlaylistItems(playlistId, { track_id: payload.track_id });
        toast({ title: 'Added to playlist', description: 'Track appended.' });
      } else if (payload.album_id) {
        await api.addPlaylistItems(playlistId, { album_id: payload.album_id });
        toast({ title: 'Added to playlist', description: 'Album tracks appended.' });
      }
      void refreshPlaylists();
    } catch (err) {
      toast({
        title: 'Playlist add failed',
        description: err instanceof Error ? err.message : 'Failed to add item(s)',
        variant: 'destructive',
      });
    }
  };

  return (
    <Sidebar collapsible="icon" variant="sidebar">
      <SidebarHeader>
        <div className="flex items-center gap-2 px-2 py-1">
          <Logo showText={false} size="sm" />
          <div className="min-w-0 group-data-[collapsible=icon]:hidden">
            <div className="font-semibold leading-tight">PMDA</div>
            <div className="text-[11px] text-muted-foreground leading-tight">Local library</div>
          </div>
        </div>
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Browse</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {mainItems.map((it) => {
                const Icon = it.icon;
                const active = isPathActive(location.pathname, it.to);
                return (
                  <SidebarMenuItem key={it.to}>
                    <SidebarMenuButton
                      isActive={active}
                      tooltip={it.label}
                      onClick={() => navigate(it.to)}
                    >
                      <Icon className="h-4 w-4" />
                      <span>{it.label}</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarSeparator />

        <SidebarGroup>
          <SidebarGroupLabel>Playlists</SidebarGroupLabel>
          <SidebarGroupAction asChild title="New playlist">
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="h-7 w-7"
              onClick={() => navigate('/library/playlists')}
            >
              <Plus className="h-4 w-4" />
            </Button>
          </SidebarGroupAction>
          <SidebarGroupContent>
            {loadingPlaylists && playlistItems.length === 0 ? (
              <div className="px-2 py-2 text-xs text-muted-foreground">Loading…</div>
            ) : playlistItems.length === 0 ? (
              <div className="px-2 py-2 text-xs text-muted-foreground">
                No playlists yet. Create one and drag tracks here.
              </div>
            ) : (
              <SidebarMenu>
                {playlistItems.map((pl) => {
                  const active = isPathActive(location.pathname, `/library/playlists/${pl.playlist_id}`);
                  const dragOver = dragOverPlaylistId === pl.playlist_id;
                  return (
                    <SidebarMenuItem key={`pl-${pl.playlist_id}`} className="relative">
                      <SidebarMenuButton
                        isActive={active}
                        tooltip={pl.name}
                        onClick={() => navigate(`/library/playlists/${pl.playlist_id}`)}
                        onDragOver={(e) => {
                          e.preventDefault();
                          setDragOverPlaylistId(pl.playlist_id);
                        }}
                        onDragLeave={() => setDragOverPlaylistId(null)}
                        onDrop={(e) => void handleDropOnPlaylist(pl.playlist_id, e)}
                        className={cn(dragOver ? 'ring-2 ring-primary/30 bg-sidebar-accent' : '')}
                      >
                        <ListMusic className="h-4 w-4" />
                        <span className="truncate">{pl.name}</span>
                      </SidebarMenuButton>
                      {pl.item_count > 0 ? (
                        <SidebarMenuBadge className="text-[10px]">
                          {pl.item_count > 999 ? '999+' : pl.item_count}
                        </SidebarMenuBadge>
                      ) : null}
                    </SidebarMenuItem>
                  );
                })}
              </SidebarMenu>
            )}
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarSeparator />

        <SidebarGroup>
          <SidebarGroupLabel>System</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {adminItems.map((it) => {
                const Icon = it.icon;
                const active = isPathActive(location.pathname, it.to);
                return (
                  <SidebarMenuItem key={it.to}>
                    <SidebarMenuButton isActive={active} tooltip={it.label} onClick={() => navigate(it.to)}>
                      <Icon className="h-4 w-4" />
                      <span>{it.label}</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter>
        <div className="px-2 py-1 text-[11px] text-muted-foreground group-data-[collapsible=icon]:hidden">
          Tip: drag a track from Now Playing queue onto a playlist.
        </div>
      </SidebarFooter>

      <SidebarRail />
    </Sidebar>
  );
}

