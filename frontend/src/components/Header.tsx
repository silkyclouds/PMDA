import { useState, useEffect, useRef } from 'react';
import { RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { WelcomeModal } from '@/components/WelcomeModal';
import { ThemeToggle } from '@/components/ThemeToggle';
import { Progress } from '@/components/ui/progress';
import { GlobalStatusBar } from '@/components/GlobalStatusBar';
import { Logo } from '@/components/Logo';
import { GlobalSearch } from '@/components/GlobalSearch';
import * as api from '@/lib/api';
import { SidebarTrigger } from '@/components/ui/sidebar';
import { useIsMobile } from '@/hooks/use-mobile';

const WELCOME_COOKIE = 'pmda_welcome_dismissed';

function hasWelcomeCookie(): boolean {
  try {
    return document.cookie.split(';').some((c) => c.trim().startsWith(`${WELCOME_COOKIE}=`));
  } catch {
    return false;
  }
}

function setWelcomeCookie(): void {
  try {
    document.cookie = `${WELCOME_COOKIE}=1; Max-Age=31536000; Path=/; SameSite=Lax`;
  } catch {
    // ignore
  }
}

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
  const isMobile = useIsMobile();
  const [showSettings, setShowSettings] = useState(false);
  const [isRebooting, setIsRebooting] = useState(false);
  const [rebootCountdown, setRebootCountdown] = useState(30);
  const [rebootProgress, setRebootProgress] = useState(0);
  const [isConfigured, setIsConfigured] = useState<boolean | null>(null);
  const [config, setConfig] = useState<api.ConfigResponse | null>(null);

  // Check if PMDA is configured and load config (for welcome modal mounts checklist)
  useEffect(() => {
    api.getConfig().then((data) => {
      setConfig(data);
      const configured = data.configured === true;
      setIsConfigured(configured);
      if (!configured && !hasWelcomeCookie()) {
        setShowSettings(true);
      }
    }).catch(() => {});
  }, []);

  return (
    <>
      <header className="sticky top-0 z-50 safe-top border-b border-border/70 bg-card/85 backdrop-blur-xl supports-[backdrop-filter]:bg-card/70">
        <div className="container py-2.5 md:py-3">
          <div className="space-y-2.5 md:space-y-3">
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-2.5 min-w-0">
                <SidebarTrigger />
                <Logo size={isMobile ? 'sm' : 'md'} showText={!isMobile} />
              </div>
              <div className="flex items-center gap-1.5">
                <ThemeToggle />
              </div>
            </div>
            <GlobalSearch className="max-w-none animate-in fade-in-0 slide-in-from-bottom-1 duration-300" />
          </div>
        </div>
      </header>
      
      {/* Global status bar below header */}
      <GlobalStatusBar />

      {/* Welcome modal when not configured */}
      {showSettings && !isConfigured && (
        <WelcomeModal
          onClose={() => {
            setWelcomeCookie();
            setShowSettings(false);
          }}
          config={config}
        />
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
