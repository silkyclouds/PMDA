import { useNavigate } from 'react-router-dom';
import { Settings, X, CheckCircle2, XCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import type { ConfigResponse } from '@/lib/api';

interface WelcomeModalProps {
  onClose: () => void;
  config?: ConfigResponse | null;
}

export function WelcomeModal({ onClose, config }: WelcomeModalProps) {
  const navigate = useNavigate();
  const mounts = config?.container_mounts;

  const goToSettings = () => {
    navigate('/settings');
    onClose();
  };

  return (
    <>
      <div
        className="fixed inset-0 z-[9998] bg-black/60 backdrop-blur-md"
        onClick={(e) => {
          if (e.target === e.currentTarget) onClose();
        }}
        aria-hidden="true"
      />
      <div
        className="fixed left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 z-[9999] w-full max-w-md rounded-2xl border border-border bg-card p-6 shadow-2xl animate-in fade-in-0 zoom-in-95"
        role="dialog"
        aria-modal="true"
        aria-labelledby="welcome-title"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex justify-end">
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
            aria-label="Close"
          >
            <X className="w-5 h-5" />
          </button>
        </div>
        <div className="space-y-4 pt-2">
          <h2 id="welcome-title" className="text-lg font-semibold text-foreground">
            Welcome to PMDA
          </h2>
          <div className="space-y-2 text-sm text-muted-foreground">
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
              Start by configuring your folders. Nothing is deleted: PMDA only moves files when needed.
            </p>
          </div>

          {mounts && (
            <div className="rounded-lg border border-border bg-muted/30 p-3 space-y-2">
              <p className="text-xs font-medium text-foreground/90">
                Container mounts — everything the app needs:
              </p>
              <ul className="space-y-1.5 text-sm">
                <li className={cn("flex items-center gap-2", mounts.config_rw ? "text-green-600 dark:text-green-400" : "text-destructive")}>
                  {mounts.config_rw ? <CheckCircle2 className="w-4 h-4 shrink-0" /> : <XCircle className="w-4 h-4 shrink-0" />}
                  <span>Config folder (RW)</span>
                  <span className="ml-auto font-medium">{mounts.config_rw ? "✓" : "✗"}</span>
                </li>
                <li className={cn("flex items-center gap-2", mounts.music_rw ? "text-green-600 dark:text-green-400" : "text-destructive")}>
                  {mounts.music_rw ? <CheckCircle2 className="w-4 h-4 shrink-0" /> : <XCircle className="w-4 h-4 shrink-0" />}
                  <span>Parent music folder (RW)</span>
                  <span className="ml-auto font-medium">{mounts.music_rw ? "✓" : "✗"}</span>
                </li>
                <li className={cn("flex items-center gap-2", mounts.dupes_rw ? "text-green-600 dark:text-green-400" : "text-destructive")}>
                  {mounts.dupes_rw ? <CheckCircle2 className="w-4 h-4 shrink-0" /> : <XCircle className="w-4 h-4 shrink-0" />}
                  <span>Dupes folder (RW)</span>
                  <span className="ml-auto font-medium">{mounts.dupes_rw ? "✓" : "✗"}</span>
                </li>
              </ul>
            </div>
          )}

          <Button onClick={goToSettings} className="w-full gap-2">
            <Settings className="w-4 h-4" />
            Open Settings
          </Button>
        </div>
      </div>
    </>
  );
}
