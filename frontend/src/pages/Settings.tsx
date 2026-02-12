import { useState, useEffect, useRef, useCallback } from 'react';
import { Save, Loader2, Check, FolderOutput, RefreshCw, Plus, X, Database, Sparkles } from 'lucide-react';
import { Header } from '@/components/Header';
import { Button } from '@/components/ui/button';
import { toast } from 'sonner';
import { NotificationSettings } from '@/components/settings/NotificationSettings';
import { IntegrationsSettings } from '@/components/settings/IntegrationsSettings';
import * as api from '@/lib/api';
import type { PMDAConfig } from '@/lib/api';
import { normalizeConfigForUI } from '@/lib/configUtils';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Separator } from '@/components/ui/separator';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { Input } from '@/components/ui/input';
import { PasswordInput } from '@/components/ui/password-input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { FolderBrowserInput } from '@/components/settings/FolderBrowserInput';

const SETTINGS_SECTIONS: { id: string; label: string }[] = [
  { id: 'settings-files-export', label: 'Folders' },
  { id: 'settings-pipeline', label: 'Pipeline' },
  { id: 'settings-ai', label: 'AI' },
  { id: 'settings-providers', label: 'Providers' },
  { id: 'settings-notifications', label: 'Notifications' },
];

function parsePathListValue(value: unknown): string[] {
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
      const s = item.trim();
      if (!s) continue;
      if (s.startsWith('[') || s.startsWith('"')) {
        try {
          const parsed = JSON.parse(s) as unknown;
          if (parsed !== item) {
            queue.push(parsed);
            continue;
          }
        } catch {
          // Fall back to CSV split.
        }
      }
      if (s.includes(',')) {
        const parts = s.split(',').map((p) => p.trim()).filter(Boolean);
        if (parts.length > 1) {
          queue.push(...parts);
          continue;
        }
      }
      if (!seen.has(s) && !s.startsWith('[')) {
        seen.add(s);
        out.push(s);
      }
      continue;
    }
    const s = String(item).trim();
    if (s && !seen.has(s) && !s.startsWith('[')) {
      seen.add(s);
      out.push(s);
    }
  }

  return out;
}

function serializePathList(paths: string[]): string {
  return paths.map((p) => p.trim()).filter(Boolean).join(', ');
}

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
  const filesRoots = parsePathListValue(config.FILES_ROOTS);

  const setFilesRoots = useCallback((roots: string[]) => {
    updateConfig({ FILES_ROOTS: serializePathList(roots) });
  }, [updateConfig]);

  const addFilesRoot = useCallback((path: string) => {
    const clean = path.trim();
    if (!clean) return;
    const current = parsePathListValue(config.FILES_ROOTS);
    if (current.includes(clean)) return;
    setFilesRoots([...current, clean]);
  }, [config.FILES_ROOTS, setFilesRoots]);

  const removeFilesRoot = useCallback((path: string) => {
    const current = parsePathListValue(config.FILES_ROOTS);
    setFilesRoots(current.filter((p) => p !== path));
  }, [config.FILES_ROOTS, setFilesRoots]);
  const [pendingFilesRoot, setPendingFilesRoot] = useState('/music');

  useEffect(() => {
    if (!pendingFilesRoot.trim()) {
      setPendingFilesRoot(filesRoots[0] ?? '/music');
    }
  }, [filesRoots, pendingFilesRoot]);

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
            <Card id="settings-files-export" className="scroll-mt-24">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <FolderOutput className="w-5 h-5" />
                  Folders
                </CardTitle>
                <CardDescription>
                  Configure source folders, destination library, media cache, and export strategy (hardlink/symlink/copy/move).
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label>Music folders</Label>
                  <p className="text-xs text-muted-foreground">
                    Add one or more source folders to scan (container paths).
                  </p>
                  <div className="space-y-2">
                    {filesRoots.length === 0 ? (
                      <p className="text-xs text-muted-foreground border rounded-md px-3 py-2">
                        No source folder configured.
                      </p>
                    ) : (
                      <div className="space-y-2">
                        {filesRoots.map((rootPath) => (
                          <div key={rootPath} className="flex items-center justify-between gap-2 border rounded-md px-3 py-2">
                            <span className="font-mono text-sm truncate" title={rootPath}>{rootPath}</span>
                            <Button
                              type="button"
                              variant="ghost"
                              size="icon"
                              onClick={() => removeFilesRoot(rootPath)}
                              aria-label={`Remove ${rootPath}`}
                            >
                              <X className="w-4 h-4" />
                            </Button>
                          </div>
                        ))}
                      </div>
                    )}
                    <div className="flex items-center gap-2">
                      <div className="flex-1">
                        <FolderBrowserInput
                          value={pendingFilesRoot}
                          onChange={setPendingFilesRoot}
                          placeholder="/music"
                          selectLabel="Add music source folder"
                        />
                      </div>
                      <Button
                        type="button"
                        variant="outline"
                        className="gap-2 shrink-0"
                        onClick={() => addFilesRoot(pendingFilesRoot)}
                        disabled={!pendingFilesRoot.trim()}
                      >
                        <Plus className="w-4 h-4" />
                        Add
                      </Button>
                    </div>
                  </div>
                </div>
                <div className="space-y-2">
                  <Label>Library folder</Label>
                  <p className="text-xs text-muted-foreground">
                    Destination folder for exported files (clean structure), e.g. `/music_matched`.
                  </p>
                  <FolderBrowserInput
                    value={config.EXPORT_ROOT ?? '/music/library'}
                    onChange={(path) => updateConfig({ EXPORT_ROOT: path })}
                    placeholder="/music/library"
                    selectLabel="Select library destination folder"
                  />
                </div>
                <div className="space-y-2">
                  <Label>Media cache folder</Label>
                  <p className="text-xs text-muted-foreground">
                    NVMe cache for instant artist/album artwork rendering (thumbnails pre-generated by PMDA).
                  </p>
                  <FolderBrowserInput
                    value={config.MEDIA_CACHE_ROOT ?? '/config/media_cache'}
                    onChange={(path) => updateConfig({ MEDIA_CACHE_ROOT: path || '/config/media_cache' })}
                    placeholder="/config/media_cache"
                    selectLabel="Select media cache folder"
                  />
                </div>
                <div className="space-y-2">
                  <Label>Export strategy</Label>
                  <p className="text-xs text-muted-foreground">
                    Choose how PMDA writes files to the library folder.
                  </p>
                  <Select
                    value={(config.EXPORT_LINK_STRATEGY as 'hardlink' | 'symlink' | 'copy' | 'move' | undefined) ?? 'hardlink'}
                    onValueChange={(value: 'hardlink' | 'symlink' | 'copy' | 'move') => updateConfig({ EXPORT_LINK_STRATEGY: value })}
                  >
                    <SelectTrigger className="w-full md:w-[320px]">
                      <SelectValue placeholder="Select strategy" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="hardlink">Hardlink (fast, no extra space)</SelectItem>
                      <SelectItem value="symlink">Symlink (keeps original files)</SelectItem>
                      <SelectItem value="copy">Copy (duplicates files)</SelectItem>
                      <SelectItem value="move">Move (relocate files)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="flex items-center justify-between gap-4">
                  <div className="space-y-1">
                    <Label>Build library automatically</Label>
                    <p className="text-xs text-muted-foreground">
                      After a Magic scan in Folders mode, rebuild the library in the folder above.
                    </p>
                  </div>
                  <Switch
                    checked={Boolean(config.AUTO_EXPORT_LIBRARY)}
                    onCheckedChange={(checked) => updateConfig({ AUTO_EXPORT_LIBRARY: checked })}
                    aria-label="Build library automatically after Magic scan"
                  />
                </div>
                <div className="space-y-1">
                  <Label>Duplicates folder</Label>
                  <p className="text-xs text-muted-foreground">
                    Destination used by the dedupe step. Default is <span className="font-mono">/dupes</span>, but you can choose another writable folder.
                  </p>
                  <FolderBrowserInput
                    value={config.DUPE_ROOT ?? '/dupes'}
                    onChange={(path) => updateConfig({ DUPE_ROOT: path || '/dupes' })}
                    placeholder="/dupes"
                    selectLabel="Select duplicates destination folder"
                  />
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

            <Separator />

            <Card id="settings-pipeline" className="scroll-mt-24">
              <CardHeader>
                <CardTitle>Pipeline automation</CardTitle>
                <CardDescription>Enable/disable each pipeline stage and configure external player sync.</CardDescription>
              </CardHeader>
              <CardContent>
                <IntegrationsSettings config={config} updateConfig={updateConfig} errors={errors} />
              </CardContent>
            </Card>

            <Separator />

            <Card id="settings-ai" className="scroll-mt-24">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Sparkles className="w-5 h-5" />
                  AI
                </CardTitle>
                <CardDescription>Provide your OpenAI API key (required for match verification).</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label>OpenAI API key</Label>
                  <p className="text-xs text-muted-foreground">Required for AI match verification and cover checks.</p>
                  <PasswordInput
                    value={config.OPENAI_API_KEY || ''}
                    onChange={(e) => updateConfig({ OPENAI_API_KEY: e.target.value, AI_PROVIDER: 'openai' })}
                    placeholder="sk-..."
                  />
                </div>
                <div className="space-y-2">
                  <Label>Model (optional)</Label>
                  <Input
                    placeholder="gpt-4.1-mini"
                    value={config.OPENAI_MODEL || ''}
                    onChange={(e) => updateConfig({ OPENAI_MODEL: e.target.value, AI_PROVIDER: 'openai' })}
                  />
                </div>
              </CardContent>
            </Card>

            <Separator />

            <Card id="settings-providers" className="scroll-mt-24">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Database className="w-5 h-5" />
                  Providers
                </CardTitle>
                <CardDescription>Optional API keys to improve matches (all providers are always on).</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label>Discogs token (optional)</Label>
                  <Input
                    placeholder="Discogs user token"
                    value={config.DISCOGS_USER_TOKEN || ''}
                    onChange={(e) => updateConfig({ DISCOGS_USER_TOKEN: e.target.value })}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Last.fm API key (optional)</Label>
                  <Input
                    placeholder="Last.fm API key"
                    value={config.LASTFM_API_KEY || ''}
                    onChange={(e) => updateConfig({ LASTFM_API_KEY: e.target.value })}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Last.fm API secret (optional)</Label>
                  <Input
                    placeholder="Last.fm API secret"
                    value={config.LASTFM_API_SECRET || ''}
                    onChange={(e) => updateConfig({ LASTFM_API_SECRET: e.target.value })}
                  />
                </div>
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
