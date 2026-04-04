import { useLocation } from 'react-router-dom';
import { GlobalStatusBar } from '@/components/GlobalStatusBar';
import { GlobalSearch } from '@/components/GlobalSearch';
import { Logo } from '@/components/Logo';
import { useAuth } from '@/contexts/AuthContext';

export function Header() {
  const { isAdmin } = useAuth();
  const location = useLocation();
  const showGlobalSearch = location.pathname.startsWith('/library');

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
          </div>
        </div>
      </header>

      {/* Global status bar below header */}
      {isAdmin ? <GlobalStatusBar /> : null}
    </>
  );
}
