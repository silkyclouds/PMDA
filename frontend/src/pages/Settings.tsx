import { useState, useEffect, useRef, useCallback } from 'react';
import { Save, Loader2, Check, FolderOutput, RefreshCw } from 'lucide-react';
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
import { Label } from '@/components/ui/label';
import { Input } from '@/components/ui/input';
import { Switch } from '@/components/ui/switch';
import { FieldTooltip } from '@/components/ui/field-tooltip';

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
  const [exportRebuilding, setExportRebuilding] = useState(false);
  const [exportStatus, setExportStatus] = useState<api.FilesExportStatus | null>(null);
  const exportPollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    loadConfig();
  }, []);

  useEffect(() => {
    if (!exportStatus?.running) return;
    const t = setInterval(async () => {
      try {
        const s = await api.getFilesExportStatus();
        setExportStatus(s);
      } catch {
        // ignore
      }
    }, 2000);
    return () => clearInterval(t);
  }, [exportStatus?.running]);

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
            {/* Library source (Plex vs Files) */}
            <Card id="settings-library-source" className="scroll-mt-24">
              <CardHeader>
                <CardTitle>Library source</CardTitle>
                <CardDescription>Use Plex if you have it; otherwise use Folders only and set your music and library paths below.</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <div className="flex flex-wrap gap-2">
                    <Button
                      type="button"
                      variant={(config.LIBRARY_MODE ?? 'plex') === 'plex' ? 'default' : 'outline'}
                      size="sm"
                      onClick={() => updateConfig({ LIBRARY_MODE: 'plex' })}
                    >
                      Plex
                    </Button>
                    <Button
                      type="button"
                      variant={(config.LIBRARY_MODE ?? 'plex') === 'files' ? 'default' : 'outline'}
                      size="sm"
                      onClick={() => updateConfig({ LIBRARY_MODE: 'files' })}
                    >
                      Folders only
                    </Button>
                  </div>
                  {(config.LIBRARY_MODE ?? 'plex') === 'files' && (
                    <p className="text-xs text-muted-foreground">
                      Set your music folder and library folder below, then click Build library. Duplicates go to /dupes.
                    </p>
                  )}
                </div>
              </CardContent>
            </Card>

            {(config.LIBRARY_MODE ?? 'plex') === 'files' && (
              <>
                <Card id="settings-files-export" className="scroll-mt-24">
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <FolderOutput className="w-5 h-5" />
                      Folders
                    </CardTitle>
                    <CardDescription>
                      Set where your music is, where to build the clean library (hardlinks), and where duplicates go. PMDA does the rest.
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="space-y-2">
                      <Label htmlFor="music-folder">Music folder</Label>
                      <p className="text-xs text-muted-foreground">
                        Where your music files are (container path, e.g. /music).
                      </p>
                      <Input
                        id="music-folder"
                        value={typeof config.FILES_ROOTS === 'string' ? config.FILES_ROOTS : (Array.isArray(config.FILES_ROOTS) ? (config.FILES_ROOTS as string[]).join(', ') : '')}
                        onChange={(e) => updateConfig({ FILES_ROOTS: e.target.value.trim() || '/music' })}
                        placeholder="/music"
                        className="font-mono"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="library-folder">Library folder</Label>
                      <p className="text-xs text-muted-foreground">
                        Where PMDA will create a clean, organized copy using hardlinks (no extra space). Use a subfolder or a separate mount, e.g. /music/library.
                      </p>
                      <Input
                        id="library-folder"
                        value={config.EXPORT_ROOT ?? ''}
                        onChange={(e) => updateConfig({ EXPORT_ROOT: e.target.value.trim() })}
                        placeholder="/music/library"
                        className="font-mono"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="media-cache-folder">Media cache folder</Label>
                      <p className="text-xs text-muted-foreground">
                        NVMe cache for instant artist/album artwork rendering (thumbnails pre-generated by PMDA).
                      </p>
                      <Input
                        id="media-cache-folder"
                        value={config.MEDIA_CACHE_ROOT ?? '/config/media_cache'}
                        onChange={(e) => updateConfig({ MEDIA_CACHE_ROOT: e.target.value.trim() || '/config/media_cache' })}
                        placeholder="/config/media_cache"
                        className="font-mono"
                      />
                    </div>
                    <div className="flex items-center justify-between gap-4">
                      <div className="space-y-1">
                        <Label>Build library automatically</Label>
                        <p className="text-xs text-muted-foreground">
                          After a Magic scan in Folders mode, rebuild the hardlink library in the folder above.
                        </p>
                      </div>
                      <Switch
                        checked={Boolean(config.AUTO_EXPORT_LIBRARY)}
                        onCheckedChange={(checked) => updateConfig({ AUTO_EXPORT_LIBRARY: checked })}
                        aria-label="Build library automatically after Magic scan"
                      />
                    </div>
                    <div className="space-y-1">
                      <Label className="text-muted-foreground">Duplicates folder</Label>
                      <p className="text-xs text-muted-foreground">
                        Duplicates are moved to <span className="font-mono">/dupes</span> (set by your Docker volume). No need to configure.
                      </p>
                    </div>
                    <div className="flex flex-wrap items-center gap-3 pt-2 border-t pt-4">
                      <Button
                        type="button"
                        variant="default"
                        disabled={exportRebuilding}
                        onClick={async () => {
                          setExportRebuilding(true);
                          setExportStatus(null);
                          try {
                            const res = await api.postFilesExportRebuild();
                            if (res.status === 'already_running') {
                              toast.info('Build already in progress');
                              const s = await api.getFilesExportStatus();
                              setExportStatus(s);
                            } else if (res.status === 'started') {
                              toast.success('Building library…');
                              const s = await api.getFilesExportStatus();
                              setExportStatus(s);
                            } else {
                              toast.error(res.message ?? 'Failed to start');
                            }
                          } catch (e) {
                            toast.error('Failed to start');
                          } finally {
                            setExportRebuilding(false);
                          }
                        }}
                        className="gap-2"
                      >
                        {exportRebuilding ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                        Build library
                      </Button>
                      {exportStatus && (exportStatus.running || exportStatus.error) && (
                        <span className="text-sm text-muted-foreground">
                          {exportStatus.running
                            ? `${exportStatus.tracks_done}/${exportStatus.total_tracks} tracks`
                            : exportStatus.error ?? 'Done'}
                        </span>
                      )}
                    </div>
                  </CardContent>
                </Card>
              </>
            )}

            <Separator />

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
