import { useState, useEffect, useRef } from 'react';
import { Outlet, useLocation } from 'react-router-dom';
import { RefreshCw } from 'lucide-react';

import { Header } from '@/components/Header';
import { SidebarInset, SidebarProvider } from '@/components/ui/sidebar';
import { AppSidebar } from '@/components/layout/AppSidebar';
import { SocialNotificationsBridge } from '@/components/social/SocialNotificationsBridge';
import { UiBuildWatcher } from '@/components/UiBuildWatcher';
import { WelcomeModal } from '@/components/WelcomeModal';
import { GuidedOnboardingDialog } from '@/components/settings/GuidedOnboardingDialog';
import { Progress } from '@/components/ui/progress';
import * as api from '@/lib/api';
import { normalizeConfigForUI } from '@/lib/configUtils';
import { useAuth } from '@/contexts/AuthContext';

const WELCOME_COOKIE = 'pmda_welcome_dismissed';
const GUIDED_ONBOARDING_STORAGE_KEY = 'pmda_guided_onboarding_open';

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

function hasGuidedOnboardingFlag(): boolean {
  try {
    return window.localStorage.getItem(GUIDED_ONBOARDING_STORAGE_KEY) === '1';
  } catch {
    return false;
  }
}

function setGuidedOnboardingFlag(open: boolean): void {
  try {
    if (open) {
      window.localStorage.setItem(GUIDED_ONBOARDING_STORAGE_KEY, '1');
    } else {
      window.localStorage.removeItem(GUIDED_ONBOARDING_STORAGE_KEY);
    }
  } catch {
    // ignore
  }
}

function normalizeFolderPath(input: string): string {
  const raw = String(input || '').trim();
  if (!raw) return '';
  if (raw === '/') return '/';
  return raw.replace(/\/+$/, '') || raw;
}

function parsePathList(value: unknown): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const queue: unknown[] = [value];

  while (queue.length > 0) {
    const item = queue.shift();
    if (item == null) continue;
    if (Array.isArray(item)) {
      queue.push(...item);
      continue;
    }
    if (typeof item === 'string') {
      const raw = item.trim();
      if (!raw) continue;
      if (raw.startsWith('[') || raw.startsWith('"')) {
        try {
          const parsed = JSON.parse(raw) as unknown;
          if (parsed !== item) {
            queue.push(parsed);
            continue;
          }
        } catch {
          // fall through
        }
      }
      if (raw.includes(',')) {
        const parts = raw.split(',').map((part) => part.trim()).filter(Boolean);
        if (parts.length > 1) {
          queue.push(...parts);
          continue;
        }
      }
      const normalized = normalizeFolderPath(raw);
      if (normalized && !seen.has(normalized)) {
        seen.add(normalized);
        out.push(normalized);
      }
      continue;
    }
    const normalized = normalizeFolderPath(String(item));
    if (normalized && !seen.has(normalized)) {
      seen.add(normalized);
      out.push(normalized);
    }
  }

  return out;
}

function isBundleReady(bundle: api.ManagedRuntimeBundleStatus | null | undefined): boolean {
  if (!bundle) return false;
  return bundle.state === 'ready' && Boolean(bundle.health?.available);
}

function bundleModels(bundle: api.ManagedRuntimeBundleStatus | null | undefined): string[] {
  const metaModels = Array.isArray(bundle?.meta?.models) ? bundle.meta.models : [];
  const healthModels = Array.isArray(bundle?.health?.models) ? bundle.health.models : [];
  return Array.from(new Set([...healthModels, ...metaModels].map((value) => String(value || '').trim()).filter(Boolean)));
}

function hasScanHistory(progress: api.ScanProgress | null | undefined): boolean {
  if (!progress) return false;
  return Boolean(
    progress.scanning
      || progress.scan_starting
      || progress.resume_available
      || progress.scan_resume_run_id
      || progress.scan_start_time != null
      || (progress.artists_total ?? 0) > 0
      || (progress.detected_albums_total ?? 0) > 0
      || (progress.scan_run_scope_total ?? 0) > 0
      || progress.last_scan_summary,
  );
}

function isOnboardingIncomplete(
  rawConfig: api.ConfigResponse,
  progress: api.ScanProgress | null,
  managedStatus: api.ManagedRuntimeStatusResponse | null,
): boolean {
  const config = normalizeConfigForUI(rawConfig);
  const workflowMode = String(config.LIBRARY_WORKFLOW_MODE || 'managed').trim().toLowerCase();
  const intakeRoots = parsePathList(config.LIBRARY_INTAKE_ROOTS);
  const sourceRoots = parsePathList(config.LIBRARY_SOURCE_ROOTS);
  const servingRoot = String(config.LIBRARY_SERVING_ROOT || config.EXPORT_ROOT || '').trim();
  const dupesRoot = String(config.LIBRARY_DUPES_ROOT || config.DUPE_ROOT || '').trim();
  const incompleteRoot = String(config.LIBRARY_INCOMPLETE_ROOT || config.INCOMPLETE_ALBUMS_TARGET_DIR || '').trim();

  const foldersReady = (
    !(workflowMode === 'managed' && intakeRoots.length === 0)
    && !((workflowMode === 'mirror' || workflowMode === 'inplace') && sourceRoots.length === 0)
    && !((workflowMode === 'managed' || workflowMode === 'mirror') && !servingRoot)
    && Boolean(dupesRoot)
    && Boolean(incompleteRoot)
  );

  const localStackSelected = Boolean(
    config.MUSICBRAINZ_MIRROR_ENABLED
      || String(config.WEB_SEARCH_PROVIDER || '').trim().toLowerCase() === 'ollama'
      || String(config.AI_PROVIDER || '').trim().toLowerCase() === 'ollama',
  );

  let localRuntimeReady = true;
  if (localStackSelected) {
    const managedConfigRoot = String(config.MANAGED_RUNTIME_CONFIG_ROOT || '').trim() || '/config/managed-runtime';
    const managedDataRoot = String(config.MANAGED_RUNTIME_DATA_ROOT || '').trim() || '/config/managed-runtime-data';
    const managedPreflightReady = Boolean(managedStatus?.preflight.available);
    const musicbrainzBundle = managedStatus?.bundles?.musicbrainz_local || null;
    const ollamaBundle = managedStatus?.bundles?.ollama_local || null;
    const ollamaModel = String(config.OLLAMA_MODEL || '').trim() || 'qwen3:4b';
    const ollamaHardModel = String(config.OLLAMA_COMPLEX_MODEL || '').trim() || 'qwen3:14b';
    const availableModels = bundleModels(ollamaBundle);
    const musicbrainzReady = isBundleReady(musicbrainzBundle);
    const ollamaReady = isBundleReady(ollamaBundle)
      && availableModels.includes(ollamaModel)
      && availableModels.includes(ollamaHardModel);

    localRuntimeReady = Boolean(managedConfigRoot)
      && Boolean(managedDataRoot)
      && managedPreflightReady
      && musicbrainzReady
      && ollamaReady;
  }

  return !foldersReady || !localRuntimeReady || !hasScanHistory(progress);
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
  const [showGuidedOnboarding, setShowGuidedOnboarding] = useState(false);
  const [welcomeMode, setWelcomeMode] = useState<'welcome' | 'bootstrap'>('welcome');
  const [isRebooting, setIsRebooting] = useState(false);
  const [rebootCountdown, setRebootCountdown] = useState(30);
  const [rebootProgress, setRebootProgress] = useState(0);
  const [config, setConfig] = useState<api.ConfigResponse | null>(null);

  useEffect(() => {
    if (!isAdmin) {
      setShowSettings(false);
      setShowGuidedOnboarding(false);
      setWelcomeMode('welcome');
      setConfig(null);
      setGuidedOnboardingFlag(false);
      return;
    }
    let cancelled = false;
    Promise.all([
      api.getConfig(),
      api.getScanProgress().catch(() => null),
      api.getManagedRuntimeStatus({ skipCandidates: true }).catch(() => null),
    ])
      .then(([data, progress, managedStatus]) => {
        if (cancelled) return;
        setConfig(data);
        const normalized = normalizeConfigForUI(data);
        const hasConfiguredRoots = Boolean(String(normalized.FILES_ROOTS || '').trim());
        const configured = data.configured === true || hasConfiguredRoots;
        const bootstrapPending = configured && Boolean(progress?.bootstrap_required);
        const onboardingIncomplete = isOnboardingIncomplete(data, progress, managedStatus);
        const shouldOpenGuidedOnboarding = hasGuidedOnboardingFlag() || onboardingIncomplete;

        setWelcomeMode(bootstrapPending ? 'bootstrap' : 'welcome');

        if (shouldOpenGuidedOnboarding) {
          setShowGuidedOnboarding(true);
          setGuidedOnboardingFlag(true);
          setShowSettings(false);
        } else if (bootstrapPending) {
          setShowSettings(true);
        } else if (!configured && !hasWelcomeCookie()) {
          setShowSettings(true);
        } else {
          setShowSettings(false);
        }
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [isAdmin]);

  useEffect(() => {
    if (!isAdmin) return;
    const handleOpenGuidedOnboarding = () => {
      setShowSettings(false);
      setShowGuidedOnboarding(true);
      setGuidedOnboardingFlag(true);
    };
    window.addEventListener('pmda:open-guided-onboarding', handleOpenGuidedOnboarding);
    return () => {
      window.removeEventListener('pmda:open-guided-onboarding', handleOpenGuidedOnboarding);
    };
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
          onOpenGuidedOnboarding={() => {
            setShowGuidedOnboarding(true);
            setGuidedOnboardingFlag(true);
          }}
        />
      )}

      {isAdmin && (
        <GuidedOnboardingDialog
          open={showGuidedOnboarding}
          onOpenChange={(open) => {
            setShowGuidedOnboarding(open);
            setGuidedOnboardingFlag(open);
          }}
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
