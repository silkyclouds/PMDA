import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { Play, Settings, X, CheckCircle2, XCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Logo } from '@/components/Logo';
import { GuidedOnboardingDialog } from '@/components/settings/GuidedOnboardingDialog';
import { cn } from '@/lib/utils';
import { getScanProgress, type ConfigResponse } from '@/lib/api';

interface WelcomeModalProps {
  onClose: () => void;
  config?: ConfigResponse | null;
  mode?: 'welcome' | 'bootstrap';
}

export function WelcomeModal({ onClose, config, mode = 'welcome' }: WelcomeModalProps) {
  const navigate = useNavigate();
  const [showOnboarding, setShowOnboarding] = useState(false);
  const mounts = config?.container_mounts;
  const hasConfiguredRoots = Boolean(String(config?.FILES_ROOTS || '').trim());
  const isBootstrapMode = mode === 'bootstrap' || hasConfiguredRoots;
  const { data: scanProgress } = useQuery({
    queryKey: ['welcome-modal-scan-progress'],
    queryFn: getScanProgress,
    enabled: isBootstrapMode,
    refetchInterval: isBootstrapMode ? 2000 : false,
    staleTime: 1000,
    retry: 1,
  });

  const initialFullScanAlreadyStarted = Boolean(
    scanProgress && (
      scanProgress.scanning ||
      scanProgress.scan_starting ||
      scanProgress.resume_available ||
      scanProgress.scan_resume_run_id ||
      (scanProgress.scan_start_time != null) ||
      (scanProgress.artists_total ?? 0) > 0 ||
      (scanProgress.detected_albums_total ?? 0) > 0 ||
      (scanProgress.scan_run_scope_total ?? 0) > 0 ||
      scanProgress.last_scan_summary
    )
  );

  if (isBootstrapMode && initialFullScanAlreadyStarted) {
    return null;
  }

  if (showOnboarding) {
    return (
      <GuidedOnboardingDialog
        open={showOnboarding}
        onOpenChange={(open) => {
          setShowOnboarding(open);
          if (!open) onClose();
        }}
      />
    );
  }

  const goToScan = () => {
    navigate('/scan');
    onClose();
  };

  return (
    <>
      <div
        className="fixed inset-0 z-[9998] bg-background/80 backdrop-blur-md"
        onClick={(e) => {
          if (e.target === e.currentTarget) onClose();
        }}
        aria-hidden="true"
      />
      <div
        className="fixed left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 z-[9999] w-[calc(100%-2rem)] sm:w-full max-w-md rounded-2xl border border-border/60 bg-card p-5 sm:p-6 shadow-2xl animate-in fade-in-0 zoom-in-95"
        role="dialog"
        aria-modal="true"
        aria-labelledby="welcome-title"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex justify-end">
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
            aria-label="Close"
          >
            <X className="w-5 h-5" />
          </button>
        </div>
        <div className="space-y-4 pt-2">
          <div className="flex justify-center">
            <Logo variant="wordmark" size="xl" />
          </div>
          <h2 id="welcome-title" className="text-lg font-semibold text-foreground">
            {isBootstrapMode ? 'Initial Full Scan Required' : 'Welcome to PMDA'}
          </h2>
          <div className="space-y-2 text-sm text-muted-foreground">
            {isBootstrapMode ? (
              <>
                <p>
                  PMDA is configured, but the Files library is still empty because the initial full scan has not completed yet.
                </p>
                <ul className="space-y-1 pl-4 list-disc">
                  <li>Source folders are already configured</li>
                  <li>The published Files index is still waiting for its first completed full scan</li>
                  <li>Once that scan finishes, albums, artists, covers and reviews can be published to the library</li>
                </ul>
                <p>
                  Go to the guided onboarding to review the workflow, folders, local-vs-online metadata mode, and start the first full scan with clear status.
                </p>
              </>
            ) : (
              <>
                <p>
                  PMDA automates the boring part of managing a large music library.
                </p>
                <ul className="space-y-1 pl-4 list-disc">
                  <li>Scan one or more source folders</li>
                  <li>Match albums (MusicBrainz + fallbacks) and fix tags</li>
                  <li>Download best possible covers and artist images</li>
                  <li>Move duplicates and incomplete albums to quarantine folders</li>
                  <li>Export a clean library tree (hardlink/symlink/copy/move)</li>
                  <li>Optionally trigger Plex/Jellyfin/Navidrome refresh</li>
                </ul>
                <p>
                  Start with the guided onboarding. It explains the workflow, lets you choose how Library / Inbox / Dupes should behave, and shows initialization progress before the first scan.
                </p>
              </>
            )}
          </div>

          {mounts && (
            <div className="rounded-lg border border-border/60 bg-card p-3 space-y-2">
              <p className="text-xs font-medium text-foreground/90">
                Container mounts
              </p>
              <ul className="space-y-1.5 text-sm">
                <li className={cn("flex items-center gap-2", mounts.config_rw ? "text-success" : "text-destructive")}>
                  {mounts.config_rw ? <CheckCircle2 className="w-4 h-4 shrink-0" /> : <XCircle className="w-4 h-4 shrink-0" />}
                  <span>Config folder (RW)</span>
                </li>
                <li className={cn("flex items-center gap-2", mounts.music_rw ? "text-success" : "text-destructive")}>
                  {mounts.music_rw ? <CheckCircle2 className="w-4 h-4 shrink-0" /> : <XCircle className="w-4 h-4 shrink-0" />}
                  <span>Parent music folder (RW)</span>
                </li>
                <li className={cn("flex items-center gap-2", mounts.dupes_rw ? "text-success" : "text-destructive")}>
                  {mounts.dupes_rw ? <CheckCircle2 className="w-4 h-4 shrink-0" /> : <XCircle className="w-4 h-4 shrink-0" />}
                  <span>Dupes folder (RW)</span>
                </li>
              </ul>
            </div>
          )}

          <div className="flex flex-col gap-2 sm:flex-row">
            <Button onClick={() => setShowOnboarding(true)} className="flex-1 gap-2">
              <Settings className="w-4 h-4" />
              Open guided onboarding
            </Button>
            {isBootstrapMode ? (
              <Button onClick={goToScan} variant="outline" className="flex-1 gap-2">
                <Play className="w-4 h-4" />
                Open Scan
              </Button>
            ) : null}
          </div>
        </div>
      </div>
    </>
  );
}
