import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AlertTriangle, Check, Loader2, Save, Server, Trash2 } from 'lucide-react';
import { toast } from 'sonner';

import * as api from '@/lib/api';
import type { PMDAConfig, ScanPreflightResult } from '@/lib/api';
import { normalizeConfigForUI } from '@/lib/configUtils';
import { useAuth } from '@/contexts/AuthContext';
import { SettingsControlPlane } from '@/components/settings/SettingsControlPlane';
import { LibraryWorkflowSettings } from '@/components/settings/LibraryWorkflowSettings';
import { McpAccessSettings } from '@/components/settings/McpAccessSettings';
import { ScalingSettings } from '@/components/settings/ScalingSettings';
import { SchedulerSettings } from '@/components/settings/SchedulerSettings';
import { StoragePowerSaverSettings } from '@/components/settings/StoragePowerSaverSettings';
import {
  MetadataSourcesPanel,
  OptionalDestinationsSettingsPanel,
  PipelineSettingsPanel,
  PublishedLibrarySettingsPanel,
} from '@/components/settings/SystemSettingsPanels';
import { FolderBrowserInput } from '@/components/settings/FolderBrowserInput';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '@/components/ui/alert-dialog';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog';

type FlowDialogId = 'workflow' | 'sources' | 'pipeline' | 'published-library' | 'destinations' | null;
type DangerPreset = 'reset_all_keep_settings';
type SaveFeedbackOptions = {
  showSuccessToast?: boolean;
  toastMessage?: string;
};

const DANGER_PRESETS: Array<{
  id: DangerPreset;
  title: string;
  description: string;
  buttonLabel: string;
  resetActions: api.MaintenanceResetAction[];
}> = [
  {
    id: 'reset_all_keep_settings',
    title: 'Reset PMDA (keep settings)',
    description:
      "Fully resets PMDA library data: media cache, scan/cache state, published library, playlists, likes, recommendations, notifications, concerts, assistant/RAG data, and playback history. Settings, users, OAuth/API credentials, and folder/provider configuration are preserved.",
    buttonLabel: "Reset PMDA now",
    resetActions: ["media_cache", "cache_db", "state_db", "files_index"],
  },
];

function dialogTitle(dialog: Exclude<FlowDialogId, null>): { title: string; description: string } {
  switch (dialog) {
    case 'workflow':
      return {
        title: 'Library workflow',
        description: 'Choose how PMDA interprets your folders and how the library topology should behave.',
      };
    case 'sources':
      return {
        title: 'Sources',
        description: 'Configure metadata providers, add API keys inline, and validate them live.',
      };
    case 'pipeline':
      return {
        title: 'Pipeline',
        description: 'Set duplicates, incompletes, write mode and scan behavior from one focused surface.',
      };
    case 'published-library':
      return {
        title: 'Published library',
        description: 'Control the serving root, materialization mode and naming options.',
      };
    case 'destinations':
      return {
        title: 'Optional destinations',
        description: 'Connect Jellyfin or Navidrome if PMDA should refresh them automatically.',
      };
  }
}

function RebootOverlay({ countdown, progress }: { countdown: number; progress: number }) {
  return (
    <>
      <div className="fixed inset-0 z-[10000] bg-black/80 backdrop-blur-md" />
      <div className="fixed left-1/2 top-1/2 z-[10001] w-full max-w-md -translate-x-1/2 -translate-y-1/2 rounded-2xl border border-border bg-card p-6 shadow-2xl">
        <div className="flex flex-col items-center gap-4 text-center">
          <div className="h-10 w-10 animate-spin rounded-full border-2 border-primary border-t-transparent" />
          <div className="space-y-2">
            <h3 className="text-lg font-semibold">PMDA is rebooting</h3>
            <p className="text-sm text-muted-foreground">
              Page will auto-refresh in <span className="font-mono font-semibold text-primary">{countdown}</span> second{countdown === 1 ? '' : 's'}
            </p>
          </div>
          <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
            <div className="h-full bg-primary transition-[width]" style={{ width: `${Math.max(0, Math.min(100, progress))}%` }} />
          </div>
        </div>
      </div>
    </>
  );
}

export default function Settings() {
  const auth = useAuth();
  const [config, setConfig] = useState<Partial<PMDAConfig>>({});
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [providersChecking, setProvidersChecking] = useState(false);
  const [providersPreflight, setProvidersPreflight] = useState<ScanPreflightResult | null>(null);
  const [providersPreflightAt, setProvidersPreflightAt] = useState<number | null>(null);
  const [openDialog, setOpenDialog] = useState<FlowDialogId>(null);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [dangerDialogOpen, setDangerDialogOpen] = useState(false);
  const [pendingDangerPreset, setPendingDangerPreset] = useState<DangerPreset | null>(null);
  const [dangerBusyPreset, setDangerBusyPreset] = useState<DangerPreset | null>(null);
  const [lastSaved, setLastSaved] = useState<boolean | null>(null);
  const [isRestarting, setIsRestarting] = useState(false);
  const [rebootCountdown, setRebootCountdown] = useState(35);
  const [rebootProgress, setRebootProgress] = useState(0);
  const pendingSaveRef = useRef<Partial<PMDAConfig>>({});
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const loadConfig = useCallback(async () => {
    setIsLoading(true);
    try {
      const next = normalizeConfigForUI(await api.getConfig());
      setConfig(next);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to load configuration');
    } finally {
      setIsLoading(false);
    }
  }, []);

  const flushPendingSave = useCallback(async ({ showSuccessToast = false, toastMessage = 'Settings saved' }: SaveFeedbackOptions = {}) => {
    const toSave = pendingSaveRef.current;
    pendingSaveRef.current = {};
    if (Object.keys(toSave).length === 0) {
      if (showSuccessToast) toast.success(toastMessage);
      return true;
    }
    setIsSaving(true);
    try {
      const result = await api.saveConfig(toSave);
      setLastSaved(true);
      window.setTimeout(() => setLastSaved(null), 4000);
      if (result.restart_initiated) {
        toast.success(result.message || 'Configuration saved. PMDA will restart automatically.');
        setRebootCountdown(35);
        setRebootProgress(0);
        setIsRestarting(true);
      } else if (showSuccessToast) {
        toast.success(result.message || toastMessage);
      }
      return true;
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to save configuration');
      return false;
    } finally {
      setIsSaving(false);
    }
  }, []);

  const saveConfigNow = useCallback(async (options?: SaveFeedbackOptions) => {
    if (debounceTimerRef.current) {
      clearTimeout(debounceTimerRef.current);
      debounceTimerRef.current = null;
    }
    return flushPendingSave(options);
  }, [flushPendingSave]);

  const handlePrimarySave = useCallback(() => {
    void saveConfigNow({ showSuccessToast: true, toastMessage: 'Settings saved' });
  }, [saveConfigNow]);

  const handleDialogSave = useCallback(async () => {
    const saved = await saveConfigNow({ showSuccessToast: true, toastMessage: 'Settings saved' });
    if (saved) setOpenDialog(null);
  }, [saveConfigNow]);

  const updateConfig = useCallback((updates: Partial<PMDAConfig>) => {
    setConfig((prev) => ({ ...prev, ...updates }));
    pendingSaveRef.current = { ...pendingSaveRef.current, ...updates };
    if (debounceTimerRef.current) clearTimeout(debounceTimerRef.current);
    debounceTimerRef.current = setTimeout(() => {
      debounceTimerRef.current = null;
      void flushPendingSave();
    }, 350);
  }, [flushPendingSave]);

  const openGuidedOnboarding = useCallback(() => {
    window.dispatchEvent(new CustomEvent("pmda:open-guided-onboarding"));
  }, []);

  const refreshProviderStatus = useCallback(async () => {
    setProvidersChecking(true);
    try {
      const status = await api.getProvidersPreflight();
      setProvidersPreflight(status);
      setProvidersPreflightAt(Date.now());
      toast.success('Provider checks completed');
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Provider checks failed');
    } finally {
      setProvidersChecking(false);
    }
  }, []);

  const openDangerConfirm = useCallback((action: DangerPreset) => {
    if (dangerBusyPreset) return;
    setPendingDangerPreset(action);
    setDangerDialogOpen(true);
  }, [dangerBusyPreset]);

  const runDangerAction = useCallback(async () => {
    if (!pendingDangerPreset || dangerBusyPreset) return;
    const preset = DANGER_PRESETS.find((item) => item.id === pendingDangerPreset);
    if (!preset) return;
    setDangerBusyPreset(preset.id);
    try {
      const result = await api.runMaintenanceReset({
        actions: preset.resetActions,
        restart: true,
      });
      if (result.status === "blocked") {
        toast.error(
          result.message ||
            "Stop the running scan before using Danger Zone actions.",
        );
        return;
      }
      if (
        result.status === "partial" ||
        (result.errors && result.errors.length > 0)
      ) {
        toast.error(
          result.message || "Maintenance action completed with errors.",
        );
      } else {
        toast.success(result.message || "Maintenance action completed.");
      }
      if (result.restart_initiated) {
        setDangerDialogOpen(false);
        setPendingDangerPreset(null);
        setRebootCountdown(35);
        setRebootProgress(0);
        setIsRestarting(true);
        return;
      }
      await loadConfig();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Maintenance action failed');
    } finally {
      setDangerBusyPreset(null);
    }
  }, [dangerBusyPreset, loadConfig, pendingDangerPreset]);

  useEffect(() => {
    if (!auth.isAdmin) return;
    void loadConfig();
  }, [auth.isAdmin, loadConfig]);

  useEffect(() => {
    return () => {
      if (debounceTimerRef.current) clearTimeout(debounceTimerRef.current);
    };
  }, []);

  useEffect(() => {
    if (!isRestarting) return;
    const totalMs = 35_000;
    const startedAt = Date.now();
    const timer = window.setInterval(() => {
      const elapsed = Date.now() - startedAt;
      const remaining = Math.max(0, totalMs - elapsed);
      setRebootCountdown(Math.ceil(remaining / 1000));
      setRebootProgress(Math.min(100, (elapsed / totalMs) * 100));
      if (remaining <= 0) {
        clearInterval(timer);
        window.location.reload();
      }
    }, 100);
    return () => clearInterval(timer);
  }, [isRestarting]);

  const auditMode = useMemo(
    () => String(config.LIBRARY_WORKFLOW_MODE || 'managed').trim().toLowerCase() === 'audit',
    [config.LIBRARY_WORKFLOW_MODE],
  );

  if (!auth.isAdmin) return null;

  if (isLoading) {
    return (
      <div className="flex min-h-[50vh] items-center justify-center">
        <div className="flex items-center gap-3 text-sm text-muted-foreground">
          <Loader2 className="h-5 w-5 animate-spin" />
          Loading system settings…
        </div>
      </div>
    );
  }

  return (
    <>
      <div className="space-y-6 p-6">
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-3">
            <div className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">Settings</div>
            {lastSaved ? (
              <Badge variant="outline" className="gap-1">
                <Check className="h-3.5 w-3.5" />
                Saved
              </Badge>
            ) : null}
            {isSaving ? (
              <Badge variant="secondary" className="gap-1">
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                Saving
              </Badge>
            ) : null}
          </div>
          <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
            <div className="space-y-1">
              <h1 className="text-3xl font-semibold tracking-tight">PMDA system setup</h1>
              <p className="max-w-3xl text-sm text-muted-foreground">
                Keep the main setup readable. Each block opens a focused modal aligned with the actual PMDA flow.
              </p>
            </div>
            <Button variant="outline" className="gap-2" onClick={handlePrimarySave} disabled={isSaving}>
              {isSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
              Save all now
            </Button>
          </div>
        </div>
        <SettingsControlPlane
          config={config}
          providersPreflight={providersPreflight}
          providersChecking={providersChecking}
          providersPreflightAt={providersPreflightAt}
          onRefreshProviders={() => void refreshProviderStatus()}
          onOpenSection={(sectionId) => setOpenDialog(sectionId as Exclude<FlowDialogId, null>)}
        />

        {auditMode ? (
          <Card className="border-amber-500/20 bg-amber-500/10">
            <CardContent className="flex items-start gap-3 p-4">
              <span className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-amber-500/15 text-amber-200">
                <AlertTriangle className="h-4 w-4" />
              </span>
              <div className="space-y-1">
                <div className="text-sm font-semibold text-amber-100">Audit mode keeps PMDA read-only</div>
                <p className="text-xs leading-5 text-amber-50/85">
                  PMDA can still index the library, surface dupes and incompletes, and enrich the database view, but it will not move files, publish a clean tree, or refresh players automatically.
                </p>
              </div>
            </CardContent>
          </Card>
        ) : null}

        <Collapsible open={advancedOpen} onOpenChange={setAdvancedOpen}>
          <Card className="border-border/60 bg-muted/10">
            <CollapsibleTrigger asChild>
              <Button type="button" variant="ghost" className="flex w-full items-center justify-between rounded-none px-6 py-4 text-left">
                <div className="space-y-1">
                  <div className="text-sm font-semibold text-foreground">Advanced / Expert / Debug</div>
                  <div className="text-xs text-muted-foreground">
                    Scaling, runtime auto-tune, scheduler rules, media cache path, maintenance reset and low-level runtime controls.
                  </div>
                </div>
                <Server className={`h-4 w-4 transition-transform ${advancedOpen ? 'rotate-180' : ''}`} />
              </Button>
            </CollapsibleTrigger>
            <CollapsibleContent>
              <div className="space-y-5 border-t border-border/60 p-6">
                <Card className="border-border/60 bg-background/30">
                  <CardHeader>
                    <CardTitle className="text-base">Debug-only media cache path</CardTitle>
                    <CardDescription>
                      PMDA defaults to <span className="font-mono">/config/media_cache</span> if nothing is configured. Keep this hidden from the main setup flow.
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-2">
                    <FolderBrowserInput
                      value={String(config.MEDIA_CACHE_ROOT || '/config/media_cache')}
                      onChange={(path) => updateConfig({ MEDIA_CACHE_ROOT: path || '/config/media_cache' })}
                      placeholder="/config/media_cache"
                      selectLabel="Select media cache root"
                    />
                  </CardContent>
                </Card>

                <ScalingSettings config={config} updateConfig={updateConfig} />

                <StoragePowerSaverSettings config={config} updateConfig={updateConfig} />

                <McpAccessSettings config={config} updateConfig={updateConfig} />

                <Card className="border-border/60 bg-background/30">
                  <CardHeader>
                    <CardTitle className="text-base">Scheduler advanced rules</CardTitle>
                    <CardDescription>
                      Keep the low-level scheduler rules here, out of the main setup flow.
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <SchedulerSettings config={config} updateConfig={updateConfig} />
                  </CardContent>
                </Card>

                <Card className="border-destructive/30 bg-destructive/[0.04]">
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-destructive">
                      <AlertTriangle className="h-4 w-4" />
                      Danger Zone
                    </CardTitle>
                    <CardDescription>
                      Full maintenance reset. This clears PMDA library data and restarts automatically, while keeping settings, users, and stored credentials.
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    {DANGER_PRESETS.map((action) => (
                      <div
                        key={action.id}
                        className="flex flex-col gap-3 rounded-md border border-destructive/20 bg-destructive/5 p-3 md:flex-row md:items-center md:justify-between"
                      >
                        <div className="space-y-1">
                          <p className="text-sm font-medium">{action.title}</p>
                          <p className="text-xs text-muted-foreground">{action.description}</p>
                        </div>
                        <Button
                          type="button"
                          variant="destructive"
                          className="shrink-0 gap-2"
                          onClick={() => openDangerConfirm(action.id)}
                          disabled={Boolean(dangerBusyPreset) || isRestarting}
                        >
                          {dangerBusyPreset === action.id ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                          {action.buttonLabel}
                        </Button>
                      </div>
                    ))}
                  </CardContent>
                </Card>
              </div>
            </CollapsibleContent>
          </Card>
        </Collapsible>
      </div>

      <Dialog open={openDialog !== null} onOpenChange={(open) => setOpenDialog(open ? openDialog : null)}>
        <DialogContent className="max-w-5xl border-border/60 bg-card shadow-2xl">
          {openDialog ? (
            <>
              <DialogHeader>
                <DialogTitle>{dialogTitle(openDialog).title}</DialogTitle>
                <DialogDescription>{dialogTitle(openDialog).description}</DialogDescription>
              </DialogHeader>
              <div className="max-h-[72vh] overflow-y-auto pr-1">
                {openDialog === 'workflow' ? (
                  <LibraryWorkflowSettings config={config} updateConfig={updateConfig} />
                ) : null}
                {openDialog === 'sources' ? (
                  <MetadataSourcesPanel
                    config={config}
                    updateConfig={updateConfig}
                    providersPreflight={providersPreflight}
                    providersChecking={providersChecking}
                    providersPreflightAt={providersPreflightAt}
                    onRefreshProviders={() => void refreshProviderStatus()}
                  />
                ) : null}
                {openDialog === 'pipeline' ? <PipelineSettingsPanel config={config} updateConfig={updateConfig} /> : null}
                {openDialog === 'published-library' ? <PublishedLibrarySettingsPanel config={config} updateConfig={updateConfig} /> : null}
                {openDialog === 'destinations' ? <OptionalDestinationsSettingsPanel config={config} updateConfig={updateConfig} /> : null}
              </div>
              <div className="flex justify-end gap-2 border-t border-border/60 pt-4">
                <Button type="button" variant="outline" onClick={() => setOpenDialog(null)}>
                  Close
                </Button>
                <Button type="button" className="gap-2" onClick={() => void handleDialogSave()} disabled={isSaving}>
                  {isSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                  Save changes
                </Button>
              </div>
            </>
          ) : null}
        </DialogContent>
      </Dialog>

      <AlertDialog
        open={dangerDialogOpen}
        onOpenChange={(open) => {
          if (dangerBusyPreset) return;
          setDangerDialogOpen(open);
          if (!open) setPendingDangerPreset(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{DANGER_PRESETS.find((item) => item.id === pendingDangerPreset)?.title || 'Confirm reset'}</AlertDialogTitle>
            <AlertDialogDescription>
              {DANGER_PRESETS.find((item) => item.id === pendingDangerPreset)?.description || 'This action will reset PMDA library data and restart the container.'}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={Boolean(dangerBusyPreset)}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={(event) => {
                event.preventDefault();
                void runDangerAction();
              }}
              disabled={Boolean(dangerBusyPreset)}
            >
              {dangerBusyPreset ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
              Run reset
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {isRestarting ? <RebootOverlay countdown={rebootCountdown} progress={rebootProgress} /> : null}
    </>
  );
}
