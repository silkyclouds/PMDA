import { useCallback, useEffect, useRef, useState } from 'react';
import { Loader2 } from 'lucide-react';
import { toast } from 'sonner';

import * as api from '@/lib/api';
import type { PMDAConfig } from '@/lib/api';
import { normalizeConfigForUI } from '@/lib/configUtils';
import { OnboardingWizard } from '@/components/settings/OnboardingWizard';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';

type Props = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
};

export function GuidedOnboardingDialog({ open, onOpenChange }: Props) {
  const [config, setConfig] = useState<Partial<PMDAConfig>>({});
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [dirty, setDirty] = useState(false);
  const configRef = useRef<Partial<PMDAConfig>>({});
  const pendingSaveRef = useRef<Partial<PMDAConfig>>({});
  const saveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const loadConfig = useCallback(async () => {
    setLoading(true);
    try {
      const next = normalizeConfigForUI(await api.getConfig());
      setConfig(next);
      setDirty(false);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to load PMDA config');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!open) return;
    void loadConfig();
  }, [loadConfig, open]);

  useEffect(() => {
    configRef.current = config;
  }, [config]);

  const flushPendingSave = useCallback(async (options?: { reloadAfter?: boolean; silent?: boolean }) => {
    const payload = pendingSaveRef.current;
    if (Object.keys(payload).length === 0) {
      return true;
    }

    pendingSaveRef.current = {};
    setSaving(true);
    try {
      const result = await api.saveConfig(payload);
      if (options?.reloadAfter) {
        const refreshed = normalizeConfigForUI(await api.getConfig());
        configRef.current = refreshed;
        setConfig(refreshed);
      }
      setDirty(Object.keys(pendingSaveRef.current).length > 0);
      if (!options?.silent) {
        toast.success(result.message || 'Onboarding settings saved');
      }
      return true;
    } catch (error) {
      pendingSaveRef.current = { ...payload, ...pendingSaveRef.current };
      setDirty(true);
      toast.error(error instanceof Error ? error.message : 'Failed to save onboarding settings');
      return false;
    } finally {
      setSaving(false);
    }
  }, []);

  const updateConfig = useCallback((updates: Partial<PMDAConfig>) => {
    setConfig((prev) => {
      const next = { ...prev, ...updates };
      configRef.current = next;
      return next;
    });
    pendingSaveRef.current = { ...pendingSaveRef.current, ...updates };
    setDirty(true);
    if (saveTimerRef.current) {
      clearTimeout(saveTimerRef.current);
    }
    saveTimerRef.current = setTimeout(() => {
      saveTimerRef.current = null;
      void flushPendingSave({ silent: true, reloadAfter: false });
    }, 500);
  }, [flushPendingSave]);

  const saveConfig = useCallback(async () => {
    if (saveTimerRef.current) {
      clearTimeout(saveTimerRef.current);
      saveTimerRef.current = null;
    }
    if (!dirty && Object.keys(pendingSaveRef.current).length === 0) return true;
    return flushPendingSave({ silent: false, reloadAfter: true });
  }, [dirty, flushPendingSave]);

  useEffect(() => {
    return () => {
      if (saveTimerRef.current) {
        clearTimeout(saveTimerRef.current);
      }
    };
  }, []);

  const configured = (config as api.ConfigResponse).configured === true || Boolean(String(config.FILES_ROOTS || '').trim());

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="max-w-4xl border-border/60 bg-card p-0 shadow-2xl"
        onPointerDownOutside={(event) => event.preventDefault()}
        onEscapeKeyDown={(event) => event.preventDefault()}
      >
        <DialogHeader className="sr-only">
          <DialogTitle>Guided onboarding</DialogTitle>
          <DialogDescription>
            Configure the minimum PMDA workflow, folders, metadata mode and first scan in a short guided flow.
          </DialogDescription>
        </DialogHeader>
        <div data-guided-onboarding-scroll="true" className="max-h-[90vh] overflow-y-auto p-3 md:p-4">
          {loading ? (
            <div className="flex min-h-[50vh] items-center justify-center rounded-[24px] border border-border/40 bg-muted/10">
              <div className="flex items-center gap-3 text-sm text-muted-foreground">
                <Loader2 className="h-5 w-5 animate-spin" />
                Loading guided onboarding…
              </div>
            </div>
          ) : (
            <OnboardingWizard
              config={config}
              updateConfig={updateConfig}
              configured={configured}
              presentation="modal"
              dirty={dirty}
              isSaving={saving}
              onSave={saveConfig}
              onClose={() => onOpenChange(false)}
            />
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
