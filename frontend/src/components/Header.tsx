import { useMemo } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { ChevronDown, LogOut, Settings2 } from 'lucide-react';
import { GlobalStatusBar } from '@/components/GlobalStatusBar';
import { GlobalSearch } from '@/components/GlobalSearch';
import { Logo } from '@/components/Logo';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Button } from '@/components/ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { useAuth } from '@/contexts/AuthContext';

export function Header() {
  const navigate = useNavigate();
  const { isAdmin, user, logout } = useAuth();
  const location = useLocation();
  const showGlobalSearch = location.pathname.startsWith('/library');
  const userInitials = useMemo(() => {
    const clean = String(user?.username || '').trim();
    return clean ? clean.slice(0, 2).toUpperCase() : 'U';
  }, [user?.username]);

  const handleLogout = async () => {
    await logout();
    navigate('/auth/login', { replace: true });
  };

  return (
    <>
      <header className="sticky top-0 z-50 safe-top border-b border-border/70 bg-card/85 backdrop-blur-xl supports-[backdrop-filter]:bg-card/70">
        <div className="pmda-library-shell py-2 md:py-2.5">
          <div className="flex items-center gap-3 md:gap-4">
            <Logo variant="wordmark" size="lg" className="shrink-0" />
            <div className="min-w-0 flex-1">
              {showGlobalSearch ? (
                <GlobalSearch className="max-w-none animate-in fade-in-0 slide-in-from-bottom-1 duration-300" />
              ) : null}
            </div>
            {user ? (
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    type="button"
                    variant="ghost"
                    className="h-11 shrink-0 rounded-xl border border-border/70 bg-background/70 px-2.5 hover:bg-accent/70"
                  >
                    <Avatar className="h-8 w-8 rounded-lg border border-border/60">
                      {user.avatar_data_url ? <AvatarImage src={user.avatar_data_url} alt={user.username} /> : null}
                      <AvatarFallback className="rounded-lg text-[11px] font-semibold">{userInitials}</AvatarFallback>
                    </Avatar>
                    <div className="hidden min-w-0 text-left sm:block">
                      <div className="truncate text-sm font-medium text-foreground">{user.username}</div>
                    </div>
                    <ChevronDown className="ml-1 hidden h-4 w-4 text-muted-foreground sm:block" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-56">
                  <DropdownMenuLabel className="space-y-0.5">
                    <div className="truncate">{user.username}</div>
                    <div className="text-xs font-normal text-muted-foreground">
                      {isAdmin ? 'Administrator' : 'Library user'}
                    </div>
                  </DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onSelect={() => navigate('/settings/user')}>
                    <Settings2 className="mr-2 h-4 w-4" />
                    User Settings
                  </DropdownMenuItem>
                  <DropdownMenuItem onSelect={() => void handleLogout()}>
                    <LogOut className="mr-2 h-4 w-4" />
                    Logout
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            ) : null}
          </div>
        </div>
      </header>

      {/* Global status bar below header */}
      {isAdmin ? <GlobalStatusBar /> : null}
    </>
  );
}
