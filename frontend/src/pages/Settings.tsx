import { useState, useEffect, useRef, useCallback } from 'react';
import { Save, Loader2, Check } from 'lucide-react';
import { Header } from '@/components/Header';
import { Button } from '@/components/ui/button';
import { toast } from 'sonner';
import { PlexSettings } from '@/components/settings/PlexSettings';
import { LibrariesSettings } from '@/components/settings/LibrariesSettings';
import { PathSettings } from '@/components/settings/PathSettings';
import { ScanSettings } from '@/components/settings/ScanSettings';
import { AISettings } from '@/components/settings/AISettings';
import { MetadataSettings } from '@/components/settings/MetadataSettings';
import { NotificationSettings } from '@/components/settings/NotificationSettings';
import { IntegrationsSettings } from '@/components/settings/IntegrationsSettings';
import * as api from '@/lib/api';
import type { PMDAConfig } from '@/lib/api';
import { normalizeConfigForUI } from '@/lib/configUtils';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Separator } from '@/components/ui/separator';

const SETTINGS_SECTIONS: { id: string; label: string }[] = [
  { id: 'settings-plex', label: 'Plex' },
  { id: 'settings-libraries', label: 'Libraries' },
  { id: 'settings-paths', label: 'Paths & Mapping' },
  { id: 'settings-scan', label: 'Scan' },
  { id: 'settings-ai', label: 'AI' },
  { id: 'settings-metadata', label: 'Metadata' },
  { id: 'settings-integrations', label: 'Integrations' },
  { id: 'settings-notifications', label: 'Notifications' },
];

function SettingsPage() {
  const [config, setConfig] = useState<Partial<PMDAConfig>>({});
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});

  useEffect(() => {
    loadConfig();
  }, []);

  const loadConfig = async () => {
    setIsLoading(true);
    try {
      const data = await api.getConfig();
      setConfig(normalizeConfigForUI(data));
    } catch (error) {
      console.error('Failed to load config:', error);
      toast.error('Failed to load configuration');
    } finally {
      setIsLoading(false);
    }
  };

  const pendingSaveRef = useRef<Partial<PMDAConfig>>({});
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [lastSaved, setLastSaved] = useState<boolean | null>(null);

  const flushSave = useCallback(async () => {
    const toSave = pendingSaveRef.current;
    pendingSaveRef.current = {};
    if (Object.keys(toSave).length === 0) return;
    try {
      await api.saveConfig(toSave);
      setLastSaved(true);
      setTimeout(() => setLastSaved(null), 4000);
      toast.success('Setting saved', { duration: 2500 });
    } catch (e) {
      console.error('Auto-save failed:', e);
      toast.error('Failed to save setting');
    }
  }, []);

  const updateConfig = useCallback((updates: Partial<PMDAConfig>) => {
    setConfig(prev => ({ ...prev, ...updates }));
    setErrors(prev => {
      const next = { ...prev };
      Object.keys(updates).forEach(key => delete next[key as keyof typeof next]);
      return next;
    });
    pendingSaveRef.current = { ...pendingSaveRef.current, ...updates };
    if (debounceTimerRef.current) clearTimeout(debounceTimerRef.current);
    debounceTimerRef.current = setTimeout(() => {
      debounceTimerRef.current = null;
      flushSave();
    }, 350);
  }, [flushSave]);

  useEffect(() => {
    return () => {
      if (debounceTimerRef.current) clearTimeout(debounceTimerRef.current);
    };
  }, []);

  const saveConfig = async () => {
    if (debounceTimerRef.current) {
      clearTimeout(debounceTimerRef.current);
      debounceTimerRef.current = null;
    }
    pendingSaveRef.current = {};
    setIsSaving(true);
    try {
      const configToSave = Object.fromEntries(
        Object.entries(config).filter(([_, v]) => v !== undefined)
      );
      const result = await api.saveConfig(configToSave);
      if (result.restart_initiated) {
        toast.success('Configuration saved successfully! The container will restart automatically.', { duration: 3000 });
        setTimeout(() => window.location.reload(), 3000);
      } else {
        toast.success(result.message || 'Settings saved successfully', { duration: 2000 });
        setIsSaving(false);
        loadConfig();
      }
    } catch (error) {
      console.error('Failed to save config:', error);
      setErrors({ save: 'Failed to save configuration. Please try again.' });
      toast.error('Failed to save configuration');
      setIsSaving(false);
    }
  };

  if (isLoading) {
    return (
      <>
        <Header />
        <div className="flex items-center justify-center min-h-screen">
          <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
        </div>
      </>
    );
  }

  return (
    <>
      <Header />
      <div className="container mx-auto p-6 max-w-5xl">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold">Settings</h1>
            <p className="text-muted-foreground mt-1">
              Changes are saved automatically. No restart needed.
            </p>
          </div>
          <div className="flex items-center gap-2">
            {lastSaved === true && (
              <span className="text-sm font-medium text-green-600 dark:text-green-400 flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-green-500/10">
                <Check className="w-4 h-4" /> Saved
              </span>
            )}
            <Button onClick={saveConfig} disabled={isSaving} variant="outline" size="sm" className="gap-2">
              {isSaving ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Saving...
                </>
              ) : (
                <>
                  <Save className="w-4 h-4" />
                  Save all now
                </>
              )}
            </Button>
          </div>
        </div>

        {errors.save && (
          <div className="p-4 bg-destructive/10 border border-destructive/20 rounded-md text-destructive text-sm mb-6">
            {errors.save}
          </div>
        )}

        {/* Mobile: horizontal scroll nav */}
        <nav className="md:hidden mb-4 -mx-6 px-6 overflow-x-auto pb-2 border-b border-border" aria-label="Settings sections">
          <div className="flex gap-1 min-w-max">
            {SETTINGS_SECTIONS.map(({ id, label }) => (
              <a
                key={id}
                href={`#${id}`}
                className="shrink-0 py-2 px-3 rounded-md text-sm text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors whitespace-nowrap"
              >
                {label}
              </a>
            ))}
          </div>
        </nav>

        <div className="flex gap-8">
          {/* Desktop: sticky left sidebar with anchors */}
          <nav className="hidden md:block shrink-0 w-44 top-24 self-start sticky" aria-label="Settings sections">
            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2">Sections</p>
            <ul className="space-y-0.5">
              {SETTINGS_SECTIONS.map(({ id, label }) => (
                <li key={id}>
                  <a
                    href={`#${id}`}
                    className="block py-1.5 px-2 rounded-md text-sm text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors"
                  >
                    {label}
                  </a>
                </li>
              ))}
            </ul>
          </nav>

          <div className="min-w-0 flex-1 space-y-6">
            {/* Plex Settings */}
            <Card id="settings-plex" className="scroll-mt-24">
              <CardHeader>
                <CardTitle>Plex Configuration</CardTitle>
                <CardDescription>Configure your Plex Media Server connection</CardDescription>
              </CardHeader>
              <CardContent>
                <PlexSettings config={config} updateConfig={updateConfig} errors={errors} />
              </CardContent>
            </Card>

            <Separator />

            {/* Libraries Settings */}
            <Card id="settings-libraries" className="scroll-mt-24">
              <CardHeader>
                <CardTitle>Libraries</CardTitle>
                <CardDescription>Select which Plex libraries to scan</CardDescription>
              </CardHeader>
              <CardContent>
                <LibrariesSettings config={config} updateConfig={updateConfig} errors={errors} />
              </CardContent>
            </Card>

            <Separator />

            {/* Paths Settings */}
            <Card id="settings-paths" className="scroll-mt-24">
              <CardHeader>
                <CardTitle>Paths & Mapping</CardTitle>
                <CardDescription>Configure directory paths and container bindings</CardDescription>
              </CardHeader>
              <CardContent>
                <PathSettings config={config} updateConfig={updateConfig} errors={errors} />
              </CardContent>
            </Card>

            <Separator />

            {/* Scan Settings */}
            <Card id="settings-scan" className="scroll-mt-24">
              <CardHeader>
                <CardTitle>Scan Settings</CardTitle>
                <CardDescription>Configure how PMDA scans your library</CardDescription>
              </CardHeader>
              <CardContent>
                <ScanSettings config={config} updateConfig={updateConfig} errors={errors} />
              </CardContent>
            </Card>

            <Separator />

            {/* AI Settings */}
            <Card id="settings-ai" className="scroll-mt-24">
              <CardHeader>
                <CardTitle>AI Configuration</CardTitle>
                <CardDescription>Configure AI provider for duplicate detection</CardDescription>
              </CardHeader>
              <CardContent>
                <AISettings config={config} updateConfig={updateConfig} errors={errors} />
              </CardContent>
            </Card>

            <Separator />

            {/* Metadata Settings */}
            <Card id="settings-metadata" className="scroll-mt-24">
              <CardHeader>
                <CardTitle>Metadata</CardTitle>
                <CardDescription>Configure metadata lookup and enrichment</CardDescription>
              </CardHeader>
              <CardContent>
                <MetadataSettings config={config} updateConfig={updateConfig} errors={errors} />
              </CardContent>
            </Card>

            <Separator />

            {/* Integrations Settings */}
            <Card id="settings-integrations" className="scroll-mt-24">
              <CardHeader>
                <CardTitle>Integrations</CardTitle>
                <CardDescription>Configure external service integrations (Lidarr, Autobrr)</CardDescription>
              </CardHeader>
              <CardContent>
                <IntegrationsSettings config={config} updateConfig={updateConfig} errors={errors} />
              </CardContent>
            </Card>

            <Separator />

            {/* Notifications Settings */}
            <Card id="settings-notifications" className="scroll-mt-24">
              <CardHeader>
                <CardTitle>Notifications</CardTitle>
                <CardDescription>Configure notification settings</CardDescription>
              </CardHeader>
              <CardContent>
                <NotificationSettings config={config} updateConfig={updateConfig} errors={errors} />
              </CardContent>
            </Card>

            <div className="flex justify-end pt-4">
              <Button onClick={saveConfig} disabled={isSaving} variant="outline" className="gap-2">
                {isSaving ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Saving...
                  </>
                ) : (
                  <>
                    <Save className="w-4 h-4" />
                    Save all now
                  </>
                )}
              </Button>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

export default SettingsPage;
