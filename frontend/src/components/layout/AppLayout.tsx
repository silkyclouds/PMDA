import { useState, useEffect, useRef } from 'react';
import { Outlet, useLocation } from 'react-router-dom';
import { RefreshCw } from 'lucide-react';

import { Header } from '@/components/Header';
import { SidebarInset, SidebarProvider } from '@/components/ui/sidebar';
import { AppSidebar } from '@/components/layout/AppSidebar';
import { SocialNotificationsBridge } from '@/components/social/SocialNotificationsBridge';
import { UiBuildWatcher } from '@/components/UiBuildWatcher';
import { WelcomeModal } from '@/components/WelcomeModal';
import { Progress } from '@/components/ui/progress';
import * as api from '@/lib/api';
import { useAuth } from '@/contexts/AuthContext';

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

  useEffect(() => {
    onCompleteRef.current = onComplete;
    onProgressRef.current = onProgress;
  }, [onComplete, onProgress]);

  useEffect(() => {
    const startTime = Date.now();
    const totalTime = 30000;

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
    }, 100);

    return () => clearInterval(interval);
  }, []);

  return null;
}

export function AppLayout() {
  const location = useLocation();
  const { isAdmin } = useAuth();

  const [showSettings, setShowSettings] = useState(false);
  const [welcomeMode, setWelcomeMode] = useState<'welcome' | 'bootstrap'>('welcome');
  const [isRebooting, setIsRebooting] = useState(false);
  const [rebootCountdown, setRebootCountdown] = useState(30);
  const [rebootProgress, setRebootProgress] = useState(0);
  const [config, setConfig] = useState<api.ConfigResponse | null>(null);

  useEffect(() => {
    if (!isAdmin) {
      setShowSettings(false);
      setWelcomeMode('welcome');
      setConfig(null);
      return;
    }
    Promise.all([api.getConfig(), api.getScanProgress()])
      .then(([data, progress]) => {
        setConfig(data);
        const hasConfiguredRoots = Boolean(String(data.FILES_ROOTS || '').trim());
        const configured = data.configured === true || hasConfiguredRoots;
        const bootstrapPending = configured && Boolean(progress.bootstrap_required);
        setWelcomeMode(bootstrapPending ? 'bootstrap' : 'welcome');
        if (bootstrapPending) {
          setShowSettings(true);
        } else if (!configured && !hasWelcomeCookie()) {
          setShowSettings(true);
        } else {
          setShowSettings(false);
        }
      })
      .catch(() => {});
  }, [isAdmin]);

  return (
    <SidebarProvider defaultOpen={true}>
      <AppSidebar />
      <SidebarInset>
        <Header />
        <UiBuildWatcher />
        <SocialNotificationsBridge />
        <div key={location.pathname} className="pmda-page-transition safe-bottom">
          <Outlet />
        </div>
      </SidebarInset>

      {/* Welcome modal when not configured */}
      {isAdmin && showSettings && (
        <WelcomeModal
          onClose={() => {
            if (welcomeMode === 'welcome') {
              setWelcomeCookie();
            }
            setShowSettings(false);
          }}
          config={config}
          mode={welcomeMode}
        />
      )}

      {/* Reboot countdown effect */}
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
          <div className="fixed inset-0 z-[10000] bg-background/90 backdrop-blur-md" />
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
    </SidebarProvider>
  );
}
