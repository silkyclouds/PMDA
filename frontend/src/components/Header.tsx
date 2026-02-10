import { useState, useEffect, useRef } from 'react';
import { Settings, RefreshCw, History, BarChart2, Library, Scan, Package } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import { WelcomeModal } from '@/components/WelcomeModal';
import { ThemeToggle } from '@/components/ThemeToggle';
import { Progress } from '@/components/ui/progress';
import { NavLink } from '@/components/NavLink';
import { GlobalStatusBar } from '@/components/GlobalStatusBar';
import { MobileNav } from '@/components/MobileNav';
import { Badge } from '@/components/ui/badge';
import { Logo } from '@/components/Logo';
import { GlobalSearch } from '@/components/GlobalSearch';
import * as api from '@/lib/api';

function RebootCountdown({ onComplete, onProgress }: { onComplete: () => void; onProgress: (countdown: number, progress: number) => void }) {
  const onCompleteRef = useRef(onComplete);
  const onProgressRef = useRef(onProgress);
  
  // Update refs when callbacks change
  useEffect(() => {
    onCompleteRef.current = onComplete;
    onProgressRef.current = onProgress;
  }, [onComplete, onProgress]);
  
  useEffect(() => {
    const startTime = Date.now();
    const totalTime = 30000; // 30 seconds
    
    const interval = setInterval(() => {
      const elapsed = Date.now() - startTime;
      const remaining = Math.max(0, totalTime - elapsed);
      const secondsRemaining = Math.ceil(remaining / 1000);
      const progress = Math.min(100, (elapsed / totalTime) * 100);
      
      onProgressRef.current(secondsRemaining, progress);
      
      if (remaining <= 0) {
        clearInterval(interval);
        onCompleteRef.current();
      }
    }, 100); // Update every 100ms for smooth progress
    
    return () => clearInterval(interval);
  }, []); // Empty deps - only run once
  
  return null;
}

export function Header() {
  const navigate = useNavigate();
  const [showSettings, setShowSettings] = useState(false);
  const [isRebooting, setIsRebooting] = useState(false);
  const [rebootCountdown, setRebootCountdown] = useState(30);
  const [rebootProgress, setRebootProgress] = useState(0);
  const [isConfigured, setIsConfigured] = useState<boolean | null>(null);
  const [config, setConfig] = useState<api.ConfigResponse | null>(null);
  const [duplicateCount, setDuplicateCount] = useState(0);

  // Check if PMDA is configured and load config (for welcome modal mounts checklist)
  useEffect(() => {
    api.getConfig().then((data) => {
      setConfig(data);
      setIsConfigured(data.configured === true);
      if (data.configured === false) {
        setShowSettings(true);
      }
    }).catch(() => {});
    
    // Fetch counts for badges
    api.getDuplicates().then((dupes) => {
      const list = Array.isArray(dupes) ? dupes : [];
      setDuplicateCount(list.length);
    }).catch(() => {});
  }, []);

  const handleSettingsClick = () => {
    if (isConfigured === true) {
      navigate('/settings');
    } else {
      setShowSettings(true);
    }
  };

  return (
    <>
      <header className="sticky top-0 z-50 border-b border-border bg-card/95 backdrop-blur supports-[backdrop-filter]:bg-card/80">
        <div className="container py-3">
          <div className="flex items-center justify-between">
            {/* Logo */}
            <div className="flex items-center gap-3">
              {/* Mobile menu */}
              <MobileNav 
                duplicateCount={duplicateCount} 
                onSettingsClick={handleSettingsClick}
              />
              
              <Logo size="md" />
            </div>

            {/* Desktop Navigation */}
            <nav className="hidden lg:flex items-center gap-1">
              <NavLink 
                to="/" 
                className="flex items-center gap-2 px-3 py-2 rounded-md text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
                activeClassName="text-foreground bg-accent"
              >
                <Scan className="w-4 h-4" />
                <span>Scan</span>
              </NavLink>
              <NavLink 
                to="/unduper" 
                className="flex items-center gap-2 px-3 py-2 rounded-md text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
                activeClassName="text-foreground bg-accent"
              >
                <Package className="w-4 h-4" />
                <span>Unduper</span>
                {duplicateCount > 0 && (
                  <Badge variant="outline" className="ml-1 h-5 px-1.5 text-[10px] border-warning text-warning">
                    {duplicateCount > 99 ? '99+' : duplicateCount}
                  </Badge>
                )}
              </NavLink>
              <NavLink 
                to="/library" 
                className="flex items-center gap-2 px-3 py-2 rounded-md text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
                activeClassName="text-foreground bg-accent"
              >
                <Library className="w-4 h-4" />
                <span>Library</span>
              </NavLink>
              {/* Tag Fixer and Incomplete Albums removed from main nav */}
              <NavLink 
                to="/history" 
                className="flex items-center gap-2 px-3 py-2 rounded-md text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
                activeClassName="text-foreground bg-accent"
              >
                <History className="w-4 h-4" />
                <span>History</span>
              </NavLink>
              <NavLink 
                to="/statistics" 
                className="flex items-center gap-2 px-3 py-2 rounded-md text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
                activeClassName="text-foreground bg-accent"
              >
                <BarChart2 className="w-4 h-4" />
                <span>Statistics</span>
              </NavLink>
            </nav>

            <GlobalSearch />

            {/* Right actions */}
            <div className="flex items-center gap-2">
              <ThemeToggle />
              <Button
                variant="ghost"
                size="sm"
                onClick={handleSettingsClick}
                className="flex items-center gap-2 text-muted-foreground hover:text-foreground"
              >
                <Settings className="w-4 h-4" />
                <span className="hidden sm:inline">Settings</span>
              </Button>
            </div>
          </div>
        </div>
      </header>
      
      {/* Global status bar below header */}
      <GlobalStatusBar />

      {/* Welcome modal when not configured */}
      {showSettings && !isConfigured && (
        <WelcomeModal onClose={() => setShowSettings(false)} config={config} />
      )}

      {/* Effect to handle rebooting countdown */}
      {isRebooting && (
        <RebootCountdown
          onComplete={() => window.location.reload()}
          onProgress={(countdown, progress) => {
            setRebootCountdown(countdown);
            setRebootProgress(progress);
          }}
        />
      )}

      {/* Rebooting Overlay */}
      {isRebooting && (
        <>
          <div className="fixed inset-0 z-[10000] bg-black/80 backdrop-blur-md" />
          <div className="fixed left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 z-[10001] w-full max-w-md p-6 bg-card border border-border rounded-2xl shadow-2xl">
            <div className="flex flex-col items-center gap-4 text-center">
              <div className="p-4 rounded-full bg-primary/10">
                <RefreshCw className="w-8 h-8 text-primary animate-spin" />
              </div>
              <div className="space-y-2">
                <h3 className="text-lg font-semibold">PMDA is rebooting</h3>
                <p className="text-sm text-muted-foreground">
                  Page will auto-refresh in <span className="font-mono font-semibold text-primary">{rebootCountdown}</span> {rebootCountdown === 1 ? 'second' : 'seconds'}
                </p>
              </div>
              <div className="w-full space-y-2">
                <Progress value={rebootProgress} className="h-2" />
                <p className="text-xs text-muted-foreground">
                  Waiting for container to restart...
                </p>
              </div>
            </div>
          </div>
        </>
      )}
    </>
  );
}
