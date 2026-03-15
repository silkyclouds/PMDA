import { useCallback, useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { BarChart3, Building2, Disc3, Heart, House, Library, ListMusic, LogOut, Plus, Scan, Settings2, Share2, Shield, Tags, Users, Wrench } from 'lucide-react';

import * as api from '@/lib/api';
import { useAuth } from '@/contexts/AuthContext';
import { cn } from '@/lib/utils';
import { useToast } from '@/hooks/use-toast';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Button } from '@/components/ui/button';
import { ThemeToggle } from '@/components/ThemeToggle';
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
  SidebarTrigger,
} from '@/components/ui/sidebar';
import { Logo } from '@/components/Logo';
import { useIsMobile } from '@/hooks/use-mobile';

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
  const isMobile = useIsMobile();
  const { user, isAdmin, canViewStatistics, logout } = useAuth();
  const { toast } = useToast();
  const location = useLocation();
  const navigate = useNavigate();

  const [playlists, setPlaylists] = useState<api.PlaylistSummary[]>([]);
  const [loadingPlaylists, setLoadingPlaylists] = useState(false);
  const [dragOverPlaylistId, setDragOverPlaylistId] = useState<number | null>(null);
  const [toolsBadgeCount, setToolsBadgeCount] = useState(0);
  const [recommendationsUnreadCount, setRecommendationsUnreadCount] = useState(0);
  const userInitials = useMemo(() => {
    const clean = String(user?.username || '').trim();
    return clean ? clean.slice(0, 2).toUpperCase() : 'U';
  }, [user?.username]);

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

  const refreshToolsBadge = useCallback(async () => {
    if (!isAdmin) {
      setToolsBadgeCount(0);
      return;
    }
    try {
      const runs = await api.getScanHistory();
      const completed = (Array.isArray(runs) ? runs : [])
        .filter((r) => r.status === 'completed')
        .sort((a, b) => Number(b.scan_id) - Number(a.scan_id));
      const latest = completed[0];
      const dedupeCount = Number(
        latest?.duplicates_found
          ?? latest?.duplicate_groups_count
          ?? latest?.total_duplicates_count
          ?? 0,
      );
      const incompleteCount = Number(
        latest?.broken_albums_count
          ?? latest?.summary_json?.broken_albums_count
          ?? 0,
      );
      setToolsBadgeCount(Math.max(0, dedupeCount) + Math.max(0, incompleteCount));
    } catch {
      setToolsBadgeCount(0);
    }
  }, [isAdmin]);

  const refreshRecommendationsBadge = useCallback(async () => {
    try {
      const res = await api.getRecommendations();
      setRecommendationsUnreadCount(Math.max(0, Number(res.unread_count || 0)));
    } catch {
      setRecommendationsUnreadCount(0);
    }
  }, []);

  useEffect(() => {
    void refreshPlaylists();
    void refreshToolsBadge();
    void refreshRecommendationsBadge();
    const t = setInterval(() => {
      void refreshPlaylists();
      void refreshToolsBadge();
      void refreshRecommendationsBadge();
    }, 60_000);
    return () => clearInterval(t);
  }, [refreshPlaylists, refreshRecommendationsBadge, refreshToolsBadge]);

  const mainItems = useMemo(
    () => [{ to: '/library', label: 'Library', icon: Library }],
    []
  );

  const librarySubItems = useMemo(
    () => [
      { to: '/library', label: 'Home', icon: House },
      { to: '/library/artists', label: 'Artists', icon: Users },
      { to: '/library/albums', label: 'Albums', icon: Disc3 },
      { to: '/library/genres', label: 'Genres', icon: Tags },
      { to: '/library/labels', label: 'Labels', icon: Building2 },
      { to: '/library/liked', label: 'Liked', icon: Heart },
      { to: '/library/recommendations', label: 'Recommendations', icon: Share2 },
    ],
    []
  );

  const systemItems = useMemo(
    () => {
      if (isAdmin) {
        return [
          { to: '/scan', label: 'Scan', icon: Scan },
          { to: '/tools', label: 'Tools', icon: Wrench },
          { to: '/statistics', label: 'Statistics', icon: BarChart3 },
          { to: '/settings', label: 'Settings', icon: Settings2 },
          { to: '/admin/users', label: 'Users', icon: Shield },
        ];
      }
      const items = [{ to: '/settings', label: 'Settings', icon: Settings2 }];
      if (canViewStatistics) {
        items.unshift({ to: '/statistics', label: 'Statistics', icon: BarChart3 });
      }
      return items;
    },
    [canViewStatistics, isAdmin]
  );

  const playlistItems = useMemo(() => playlists.slice(0, 10), [playlists]);
  const visiblePlaylistItems = useMemo(() => (isMobile ? playlistItems.slice(0, 5) : playlistItems), [isMobile, playlistItems]);

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

  const handleLogout = async () => {
    await logout();
    navigate('/auth/login', { replace: true });
  };

  return (
    <Sidebar collapsible="icon" variant="sidebar">
      <SidebarHeader>
        <div className="flex items-center gap-2 px-2 py-1.5 group-data-[collapsible=icon]:justify-center group-data-[collapsible=icon]:gap-0.5 group-data-[collapsible=icon]:px-0.5">
          <SidebarTrigger className="h-8 w-8 rounded-lg group-data-[collapsible=icon]:h-6 group-data-[collapsible=icon]:w-6" />
          <Logo variant="wordmark" size="xl" className="group-data-[collapsible=icon]:hidden" />
          <Logo
            showText={false}
            variant="icon"
            size="sm"
            className="hidden group-data-[collapsible=icon]:inline-flex"
          />
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

              <SidebarMenuItem className="group-data-[collapsible=icon]:hidden">
                <div className="pt-0.5" />
              </SidebarMenuItem>
              {librarySubItems.map((it) => {
                const Icon = it.icon;
                const path = location.pathname || '';
                const active =
                  (it.to === '/library' && (path === '/library' || path === '/library/')) ||
                  (it.to === '/library/artists' && (path.startsWith('/library/artists') || path.startsWith('/library/artist/'))) ||
                  (it.to === '/library/albums' && (path.startsWith('/library/albums') || path.startsWith('/library/album/'))) ||
                  (it.to === '/library/genres' && (path.startsWith('/library/genres') || path.startsWith('/library/genre/'))) ||
                  (it.to === '/library/labels' && (path.startsWith('/library/labels') || path.startsWith('/library/label/'))) ||
                  (it.to === '/library/liked' && path.startsWith('/library/liked')) ||
                  (it.to === '/library/recommendations' && path.startsWith('/library/recommendations'));
                return (
                  <SidebarMenuItem key={`library-sub-${it.to}`} className="relative">
                    <SidebarMenuButton
                      isActive={active}
                      tooltip={it.label}
                      size="sm"
                      className="ml-5 group-data-[collapsible=icon]:ml-0"
                      onClick={() => navigate(it.to)}
                    >
                      <Icon className="h-3.5 w-3.5" />
                      <span>{it.label}</span>
                    </SidebarMenuButton>
                    {it.to === '/library/recommendations' && recommendationsUnreadCount > 0 ? (
                      <SidebarMenuBadge className="text-[10px]">
                        {recommendationsUnreadCount > 99 ? '99+' : recommendationsUnreadCount}
                      </SidebarMenuBadge>
                    ) : null}
                  </SidebarMenuItem>
                );
              })}
              <SidebarMenuItem>
                <SidebarMenuButton
                  isActive={isPathActive(location.pathname, '/library/playlists')}
                  tooltip="Playlists"
                  onClick={() => navigate('/library/playlists')}
                >
                  <ListMusic className="h-4 w-4" />
                  <span>Playlists</span>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarSeparator />

        <SidebarGroup className={cn(isMobile ? 'pt-1' : '', 'group-data-[collapsible=icon]:hidden')}>
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
            {loadingPlaylists && visiblePlaylistItems.length === 0 ? (
              <div className="px-2 py-2 text-xs text-muted-foreground group-data-[collapsible=icon]:hidden">Loading…</div>
            ) : visiblePlaylistItems.length === 0 ? null : (
              <SidebarMenu>
                {visiblePlaylistItems.map((pl) => {
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

        {systemItems.length > 0 ? (
          <>
            <SidebarSeparator />
            <SidebarGroup>
              <SidebarGroupLabel>System</SidebarGroupLabel>
              <SidebarGroupContent>
                <SidebarMenu>
                  {systemItems.map((it) => {
                    const Icon = it.icon;
                    const active = isPathActive(location.pathname, it.to);
                    return (
                      <SidebarMenuItem key={it.to} className="relative">
                        <SidebarMenuButton isActive={active} tooltip={it.label} onClick={() => navigate(it.to)}>
                          <Icon className="h-4 w-4" />
                          <span>{it.label}</span>
                        </SidebarMenuButton>
                        {it.to === '/tools' && toolsBadgeCount > 0 ? (
                          <SidebarMenuBadge className="text-[10px]">
                            {toolsBadgeCount > 99 ? '99+' : toolsBadgeCount}
                          </SidebarMenuBadge>
                        ) : null}
                      </SidebarMenuItem>
                    );
                  })}
                </SidebarMenu>
              </SidebarGroupContent>
            </SidebarGroup>
          </>
        ) : null}
      </SidebarContent>

      <SidebarFooter className="group-data-[collapsible=icon]:items-center">
        <div className="flex items-center gap-3 px-2 py-2 text-xs text-muted-foreground group-data-[collapsible=icon]:justify-center">
          <Avatar className="h-10 w-10 rounded-xl border border-border/60 group-data-[collapsible=icon]:h-8 group-data-[collapsible=icon]:w-8">
            {user?.avatar_data_url ? <AvatarImage src={user.avatar_data_url} alt={user?.username || 'User avatar'} /> : null}
            <AvatarFallback className="rounded-xl text-xs font-semibold">{userInitials}</AvatarFallback>
          </Avatar>
          <div className="min-w-0 group-data-[collapsible=icon]:hidden">
            <div className="truncate font-medium text-foreground">{user?.username || 'Unknown user'}</div>
            <div>{isAdmin ? 'Administrator' : 'Library user'}</div>
          </div>
        </div>
        <div className="flex justify-start px-2 pb-2 group-data-[collapsible=icon]:justify-center group-data-[collapsible=icon]:px-0.5">
          <ThemeToggle
            showLabel={false}
            align="start"
            className="h-9 w-9 group-data-[collapsible=icon]:h-8 group-data-[collapsible=icon]:w-8"
          />
        </div>
        <div className="px-2 pb-2 group-data-[collapsible=icon]:px-0.5 group-data-[collapsible=icon]:pb-1">
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="w-full group-data-[collapsible=icon]:h-8 group-data-[collapsible=icon]:w-8 group-data-[collapsible=icon]:px-0"
            onClick={() => void handleLogout()}
            aria-label="Logout"
            title="Logout"
          >
            <LogOut className="mr-2 h-3.5 w-3.5 group-data-[collapsible=icon]:mr-0" />
            <span className="group-data-[collapsible=icon]:hidden">Logout</span>
          </Button>
        </div>
      </SidebarFooter>

      <SidebarRail />
    </Sidebar>
  );
}
