import { useMemo, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import {
  BarChart3,
  Building2,
  Disc3,
  Heart,
  House,
  ListMusic,
  LogOut,
  MoreHorizontal,
  Scan,
  Settings2,
  Share2,
  Shield,
  Tags,
  Users,
  Wrench,
  type LucideIcon,
} from 'lucide-react';

import { Logo } from '@/components/Logo';
import { useAuth } from '@/contexts/AuthContext';
import { cn } from '@/lib/utils';
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet';

interface PrimaryNavItem {
  to: string;
  label: string;
  icon: LucideIcon;
  isActive: (pathname: string) => boolean;
}

interface SecondaryNavItem {
  to?: string;
  label: string;
  icon: LucideIcon;
  isActive?: (pathname: string) => boolean;
  action?: 'logout';
}

const primaryItems: PrimaryNavItem[] = [
  {
    to: '/library',
    label: 'Home',
    icon: House,
    isActive: (pathname) =>
      pathname === '/library' || pathname === '/library/' || pathname === '/library/home' || pathname.startsWith('/library/home/'),
  },
  {
    to: '/library/albums',
    label: 'Albums',
    icon: Disc3,
    isActive: (pathname) => pathname.startsWith('/library/albums') || pathname.startsWith('/library/album/'),
  },
  {
    to: '/library/artists',
    label: 'Artists',
    icon: Users,
    isActive: (pathname) => pathname.startsWith('/library/artists') || pathname.startsWith('/library/artist/'),
  },
  {
    to: '/library/playlists',
    label: 'Playlists',
    icon: ListMusic,
    isActive: (pathname) => pathname.startsWith('/library/playlists'),
  },
];

function buildSecondaryItems(isAdmin: boolean, canViewStatistics: boolean): SecondaryNavItem[] {
  const base: SecondaryNavItem[] = [
    {
      to: '/library/genres',
      label: 'Genres',
      icon: Tags,
      isActive: (pathname) => pathname.startsWith('/library/genres') || pathname.startsWith('/library/genre/'),
    },
    {
      to: '/library/labels',
      label: 'Labels',
      icon: Building2,
      isActive: (pathname) => pathname.startsWith('/library/labels') || pathname.startsWith('/library/label/'),
    },
    {
      to: '/library/liked',
      label: 'Liked',
      icon: Heart,
      isActive: (pathname) => pathname.startsWith('/library/liked'),
    },
    {
      to: '/library/recommendations',
      label: 'Recommendations',
      icon: Share2,
      isActive: (pathname) => pathname.startsWith('/library/recommendations'),
    },
    {
      to: '/settings',
      label: 'Settings',
      icon: Settings2,
      isActive: (pathname) => pathname.startsWith('/settings'),
    },
  ];

  if (canViewStatistics) {
    base.push({
      to: '/statistics',
      label: 'Statistics',
      icon: BarChart3,
      isActive: (pathname) => pathname.startsWith('/statistics'),
    });
  }

  if (isAdmin) {
    base.push(
      {
        to: '/scan',
        label: 'Scan',
        icon: Scan,
        isActive: (pathname) => pathname.startsWith('/scan'),
      },
      {
        to: '/tools',
        label: 'Tools',
        icon: Wrench,
        isActive: (pathname) => pathname.startsWith('/tools'),
      },
      {
        to: '/admin/users',
        label: 'Users',
        icon: Shield,
        isActive: (pathname) => pathname.startsWith('/admin/users'),
      },
    );
  }

  base.push({
    label: 'Logout',
    icon: LogOut,
    action: 'logout',
  });

  return base;
}

function isMoreActive(pathname: string): boolean {
  if (primaryItems.some((item) => item.isActive(pathname))) return false;
  return (
    pathname.startsWith('/statistics') ||
    pathname.startsWith('/settings') ||
    pathname.startsWith('/scan') ||
    pathname.startsWith('/tools') ||
    pathname.startsWith('/admin/users') ||
    pathname.startsWith('/library/genres') ||
    pathname.startsWith('/library/genre/') ||
    pathname.startsWith('/library/labels') ||
    pathname.startsWith('/library/label/') ||
    pathname.startsWith('/library/liked') ||
    pathname.startsWith('/library/recommendations')
  );
}

export function MobileBottomNav({ playerOffsetPx = 0 }: { playerOffsetPx?: number }) {
  const { isAdmin, canViewStatistics, logout } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const [open, setOpen] = useState(false);

  const secondaryItems = useMemo(
    () => buildSecondaryItems(isAdmin, canViewStatistics),
    [canViewStatistics, isAdmin],
  );

  const handleNavigate = async (item: SecondaryNavItem | PrimaryNavItem) => {
    if ('action' in item && item.action === 'logout') {
      setOpen(false);
      await logout();
      navigate('/auth/login', { replace: true });
      return;
    }
    if ('to' in item && item.to) {
      setOpen(false);
      navigate(item.to);
    }
  };

  return (
    <>
      <nav
        className="fixed inset-x-0 z-40 border-t border-border/70 bg-card/95 backdrop-blur-xl md:hidden"
        style={{ bottom: `${Math.max(0, playerOffsetPx)}px` }}
      >
        <div
          className="grid grid-cols-5 items-center gap-1 px-2 pt-2"
          style={{ paddingBottom: 'max(0.5rem, env(safe-area-inset-bottom))' }}
        >
          {primaryItems.map((item) => {
            const Icon = item.icon;
            const active = item.isActive(location.pathname);
            return (
              <button
                key={item.to}
                type="button"
                onClick={() => navigate(item.to)}
                className={cn(
                  'flex min-w-0 flex-col items-center justify-center gap-1 rounded-xl px-2 py-2 transition-colors',
                  active ? 'bg-primary/12 text-primary' : 'text-muted-foreground hover:bg-accent/60 hover:text-foreground',
                )}
              >
                <Icon className="h-5 w-5 shrink-0" />
                <span className="truncate text-[11px] font-medium">{item.label}</span>
              </button>
            );
          })}

          <button
            type="button"
            onClick={() => setOpen(true)}
            className={cn(
              'flex min-w-0 flex-col items-center justify-center gap-1 rounded-xl px-2 py-2 transition-colors',
              isMoreActive(location.pathname) || open
                ? 'bg-primary/12 text-primary'
                : 'text-muted-foreground hover:bg-accent/60 hover:text-foreground',
            )}
          >
            <MoreHorizontal className="h-5 w-5 shrink-0" />
            <span className="truncate text-[11px] font-medium">More</span>
          </button>
        </div>
      </nav>

      <Sheet open={open} onOpenChange={setOpen}>
        <SheetContent side="bottom" className="max-h-[82dvh] rounded-t-3xl border-border/70 bg-card/98 p-0 md:hidden">
          <SheetHeader className="border-b border-border/60 px-5 py-4 text-left">
            <SheetTitle className="flex items-center gap-3">
              <Logo variant="wordmark" size="sm" className="pointer-events-none select-none" />
            </SheetTitle>
          </SheetHeader>

          <div className="space-y-5 overflow-y-auto px-4 pb-[max(1rem,env(safe-area-inset-bottom))] pt-4">
            <div className="space-y-2">
              <div className="px-1 text-[11px] font-medium uppercase tracking-[0.24em] text-muted-foreground">Library</div>
              <div className="grid grid-cols-2 gap-2">
                {secondaryItems
                  .filter((item) => item.to?.startsWith('/library/'))
                  .map((item) => {
                    const Icon = item.icon;
                    const active = item.isActive?.(location.pathname);
                    return (
                      <button
                        key={item.label}
                        type="button"
                        onClick={() => void handleNavigate(item)}
                        className={cn(
                          'flex items-center gap-3 rounded-xl border border-border/60 bg-background/70 px-3 py-3 text-left transition-colors',
                          active ? 'border-primary/35 bg-primary/8 text-primary' : 'hover:bg-accent/40',
                        )}
                      >
                        <Icon className="h-4 w-4 shrink-0" />
                        <span className="min-w-0 truncate text-sm font-medium">{item.label}</span>
                      </button>
                    );
                  })}
              </div>
            </div>

            <div className="space-y-2">
              <div className="px-1 text-[11px] font-medium uppercase tracking-[0.24em] text-muted-foreground">System</div>
              <div className="space-y-2">
                {secondaryItems
                  .filter((item) => !item.to?.startsWith('/library/') && item.action !== 'logout')
                  .map((item) => {
                    const Icon = item.icon;
                    const active = item.isActive?.(location.pathname);
                    return (
                      <button
                        key={item.label}
                        type="button"
                        onClick={() => void handleNavigate(item)}
                        className={cn(
                          'flex w-full items-center gap-3 rounded-xl border border-border/60 bg-background/70 px-3 py-3 text-left transition-colors',
                          active ? 'border-primary/35 bg-primary/8 text-primary' : 'hover:bg-accent/40',
                        )}
                      >
                        <Icon className="h-4 w-4 shrink-0" />
                        <span className="min-w-0 truncate text-sm font-medium">{item.label}</span>
                      </button>
                    );
                  })}
              </div>
            </div>

            <div className="space-y-2">
              {secondaryItems
                .filter((item) => item.action === 'logout')
                .map((item) => {
                  const Icon = item.icon;
                  return (
                    <button
                      key={item.label}
                      type="button"
                      onClick={() => void handleNavigate(item)}
                      className="flex w-full items-center gap-3 rounded-xl border border-border/60 bg-background/70 px-3 py-3 text-left transition-colors hover:bg-accent/40"
                    >
                      <Icon className="h-4 w-4 shrink-0" />
                      <span className="min-w-0 truncate text-sm font-medium">{item.label}</span>
                    </button>
                  );
                })}
            </div>
          </div>
        </SheetContent>
      </Sheet>
    </>
  );
}
