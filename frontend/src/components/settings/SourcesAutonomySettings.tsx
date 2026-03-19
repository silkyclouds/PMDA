import { useCallback, useEffect, useMemo, useState } from 'react';
import { ChevronDown, Loader2, Plus, RefreshCw, Save, Trash2 } from 'lucide-react';
import { toast } from 'sonner';

import * as api from '@/lib/api';
import type { FileSourceRoot } from '@/lib/api';
import { Button } from '@/components/ui/button';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { FolderBrowserInput } from '@/components/settings/FolderBrowserInput';

function normalizePath(raw: string): string {
  const txt = String(raw || '').trim();
  if (!txt) return '';
  if (txt === '/') return '/';
  return txt.replace(/\/+$/, '') || txt;
}

export function SourcesAutonomySettings() {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [runningIncoming, setRunningIncoming] = useState(false);
  const [advancedStatusOpen, setAdvancedStatusOpen] = useState(false);

  const [roots, setRoots] = useState<FileSourceRoot[]>([]);
  const [winnerId, setWinnerId] = useState<number | null>(null);
  const [winnerPath, setWinnerPath] = useState<string>('');
  const [strategy, setStrategy] = useState<'move' | 'hardlink' | 'symlink' | 'copy'>('move');

  const [bootstrap, setBootstrap] = useState<api.BootstrapStatus | null>(null);
  const [incomingStatus, setIncomingStatus] = useState<api.IncomingDeltaStatus | null>(null);
  const [watcherStatus, setWatcherStatus] = useState<api.FilesWatcherStatus | null>(null);

  const [pendingPath, setPendingPath] = useState('');
  const [pendingRole, setPendingRole] = useState<'library' | 'incoming'>('library');

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [sourcesRes, bootstrapRes, incomingRes, watcherRes] = await Promise.all([
        api.getFileSources(),
        api.getPipelineBootstrapStatus(),
        api.getIncomingStatus(),
        api.getFilesWatcherStatus(),
      ]);
      setRoots(Array.isArray(sourcesRes.roots) ? sourcesRes.roots : []);
      setWinnerId(typeof sourcesRes.winner_source_root_id === 'number' ? sourcesRes.winner_source_root_id : null);
      const winnerRow = Array.isArray(sourcesRes.roots)
        ? (sourcesRes.roots.find((row) => Boolean(row.is_winner_root)) ?? null)
        : null;
      setWinnerPath(normalizePath(String(winnerRow?.path || '')));
      setStrategy((sourcesRes.winner_placement_strategy || 'move') as 'move' | 'hardlink' | 'symlink' | 'copy');
      setBootstrap(bootstrapRes);
      setIncomingStatus(incomingRes);
      setWatcherStatus(watcherRes);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to load sources & autonomy settings');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const watcherDegraded = useMemo(() => {
    if (!watcherStatus) return false;
    if (watcherStatus.degraded_mode != null) return Boolean(watcherStatus.degraded_mode);
    if (!watcherStatus.enabled) return false;
    if (!watcherStatus.available) return true;
    return !watcherStatus.running;
  }, [watcherStatus]);

  const libraryRootsCount = useMemo(
    () => roots.filter((row) => row.role === 'library').length,
    [roots],
  );

  const incomingRootsCount = useMemo(
    () => roots.filter((row) => row.role === 'incoming').length,
    [roots],
  );

  const currentWinnerPath = useMemo(() => {
    if (winnerPath) return winnerPath;
    const byId = roots.find((row) => Number(row.source_id || 0) > 0 && Number(row.source_id || 0) === Number(winnerId || 0));
    return normalizePath(String(byId?.path || ''));
  }, [roots, winnerId, winnerPath]);

  const libraryDestinationRoots = useMemo(
    () => roots.filter((row) => row.role === 'library' && normalizePath(String(row.path || ''))),
    [roots],
  );

  const updateRoot = useCallback((index: number, patch: Partial<FileSourceRoot>) => {
    setRoots((prev) => {
      const next: FileSourceRoot[] = prev.map((row, i) => {
        if (i === index) return { ...row, ...patch };
        return row;
      });
      if (patch.role === 'incoming') {
        const targetPath = normalizePath(String(next[index]?.path || ''));
        if (targetPath && targetPath === currentWinnerPath) {
          const fallback = next.find((row, i) => i !== index && row.role === 'library' && row.enabled)
            ?? next.find((row, i) => i !== index && row.role === 'library')
            ?? null;
          setWinnerPath(normalizePath(String(fallback?.path || '')));
          setWinnerId(Number(fallback?.source_id || 0) > 0 ? Number(fallback?.source_id || 0) : null);
        }
      }
      return next;
    });
  }, [currentWinnerPath]);

  const removeRoot = useCallback((index: number) => {
    setRoots((prev) => {
      const removed = prev[index];
      const next = prev.filter((_, i) => i !== index);
      if (normalizePath(String(removed?.path || '')) === currentWinnerPath) {
        const fallback = next.find((row) => row.role === 'library' && row.enabled) ?? next.find((row) => row.role === 'library') ?? next[0];
        setWinnerPath(normalizePath(String(fallback?.path || '')));
        setWinnerId(Number(fallback?.source_id || 0) > 0 ? Number(fallback?.source_id || 0) : null);
      }
      return next;
    });
  }, [currentWinnerPath]);

  const addRoot = useCallback(() => {
    const normalized = normalizePath(pendingPath);
    if (!normalized) return;
    setRoots((prev) => {
      if (prev.some((r) => normalizePath(r.path) === normalized)) {
        return prev;
      }
      const nextPriority = (prev.reduce((acc, row) => Math.max(acc, Number(row.priority || 0)), 0) || 0) + 10;
      const nextRows: FileSourceRoot[] = [
        ...prev,
        {
          path: normalized,
          role: pendingRole,
          enabled: true,
          priority: nextPriority,
          is_winner_root: false,
        },
      ];
      if (!currentWinnerPath && pendingRole === 'library') {
        setWinnerPath(normalized);
        setWinnerId(null);
      }
      return nextRows;
    });
    setPendingPath('');
  }, [currentWinnerPath, pendingPath, pendingRole]);

  const save = useCallback(async () => {
    if (roots.length === 0) {
      toast.error('Add at least one source root.');
      return;
    }
    setSaving(true);
    try {
      const payloadRoots = roots
        .map((row, idx) => {
          const normalized = normalizePath(row.path);
          if (!normalized) return null;
          const selectedByPath = currentWinnerPath && normalizePath(normalized) === currentWinnerPath;
          const selectedById = winnerId != null && Number(row.source_id || 0) === Number(winnerId);
          return {
            source_id: row.source_id,
            path: normalized,
            role: row.role === 'incoming' ? 'incoming' : 'library',
            enabled: Boolean(row.enabled),
            priority: Math.max(1, Number(row.priority || ((idx + 1) * 10))),
            is_winner_root: Boolean(selectedByPath || selectedById),
          } satisfies FileSourceRoot;
        })
        .filter((row) => row != null) as FileSourceRoot[];
      if (payloadRoots.length === 0) {
        toast.error('No valid source roots to save.');
        return;
      }
      const res = await api.updateFileSources({
        roots: payloadRoots,
        winner_source_root_id: winnerId,
        winner_placement_strategy: strategy,
      });
      setRoots(Array.isArray(res.roots) ? res.roots : payloadRoots);
      setWinnerId(typeof res.winner_source_root_id === 'number' ? res.winner_source_root_id : winnerId);
      const savedWinner = Array.isArray(res.roots) ? res.roots.find((row) => Boolean(row.is_winner_root)) : null;
      setWinnerPath(normalizePath(String(savedWinner?.path || currentWinnerPath || '')));
      setStrategy((res.winner_placement_strategy || strategy) as 'move' | 'hardlink' | 'symlink' | 'copy');
      toast.success('Folders saved');
      await load();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to save folders');
    } finally {
      setSaving(false);
    }
  }, [roots, winnerId, currentWinnerPath, strategy, load]);

  const runIncoming = useCallback(async () => {
    setRunningIncoming(true);
    try {
      const res = await api.triggerIncomingRescan();
      if (res.status === 'started') {
        toast.success('Incoming changed-only scan started');
      } else {
        toast.info(res.message || res.reason || 'Incoming scan request blocked');
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to trigger incoming rescan');
    } finally {
      setRunningIncoming(false);
    }
  }, []);

  if (loading) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="w-4 h-4 animate-spin" />
        Loading sources & autonomy…
      </div>
    );
  }

  return (
    <div className="space-y-5">
      <div className="rounded-lg border border-border bg-muted/20 p-4 space-y-2">
        <p className="text-sm font-medium">Configuration flow</p>
        <ol className="list-decimal pl-4 text-xs text-muted-foreground space-y-1">
          <li>Add every <span className="text-foreground">Standard source folder</span> PMDA is allowed to scan.</li>
          <li>Optionally add one or more <span className="text-foreground">Incoming folders</span> if that is where new downloads land.</li>
          <li>If you use incoming folders, choose which <span className="text-foreground">Standard source folder</span> receives processed albums.</li>
          <li>Save once, then optionally run an incoming scan immediately.</li>
        </ol>
      </div>

      <div className="grid grid-cols-1 gap-2 text-xs md:grid-cols-2">
        <div className="rounded-md border border-border bg-muted/30 px-3 py-2 text-muted-foreground">
          Standard source folders: <span className="text-foreground font-medium">{libraryRootsCount}</span>
        </div>
        <div className="rounded-md border border-border bg-muted/30 px-3 py-2 text-muted-foreground">
          Incoming folders: <span className="text-foreground font-medium">{incomingRootsCount}</span>
        </div>
        <div className="rounded-md border border-border bg-muted/30 px-3 py-2 text-muted-foreground md:col-span-2">
          <div className="text-[11px] uppercase tracking-wide text-muted-foreground">Processed incoming albums go to</div>
          <div className="mt-1 overflow-x-auto whitespace-nowrap font-mono text-xs text-foreground">
            {currentWinnerPath || 'not set'}
          </div>
        </div>
      </div>

      <div className="space-y-3 rounded-xl border border-cyan-500/20 bg-cyan-500/[0.04] p-4">
        <div className="flex items-center justify-between gap-3">
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <span className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-cyan-500/15 text-cyan-200 text-xs font-semibold">1</span>
              <Label className="text-sm">What are your music source folders?</Label>
            </div>
            <p className="text-xs text-muted-foreground">
              Add every folder PMDA must scan. You can add as many folders as you want and mark each one as either a standard source folder or an incoming folder.
            </p>
            <p className="text-[11px] text-muted-foreground">
              Standard source folders are already part of your collection. Incoming folders are optional and are meant for new downloads or manual drops.
            </p>
          </div>
          <Button type="button" variant="outline" size="sm" onClick={load} className="gap-1.5">
            <RefreshCw className="w-4 h-4" />
            Reload
          </Button>
        </div>

        <div className="space-y-2">
          {roots.length === 0 ? (
            <div className="text-xs text-muted-foreground border rounded-md px-3 py-2">No source roots configured.</div>
          ) : roots.map((row, index) => {
            const sid = Number(row.source_id || 0);
            const isWinner = normalizePath(String(row.path || '')) === currentWinnerPath || (winnerId != null && sid > 0 && winnerId === sid);
            return (
              <div key={`${row.source_id || 'new'}-${index}`} className="rounded-md border border-border bg-muted/20 p-3 space-y-3">
                <FolderBrowserInput
                  value={String(row.path || '')}
                  onChange={(nextPath) => updateRoot(index, { path: nextPath })}
                  placeholder="/music/library"
                  selectLabel="Select source root folder"
                />
                <div className="flex flex-wrap items-center gap-2">
                  <div className="min-w-[240px] flex-1">
                    <Select
                      value={row.role === 'incoming' ? 'incoming' : 'library'}
                      onValueChange={(value: 'library' | 'incoming') => updateRoot(index, { role: value })}
                    >
                      <SelectTrigger><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="library">Standard source folder</SelectItem>
                        <SelectItem value="incoming">Incoming folder (new arrivals)</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="flex items-center justify-between rounded-md border border-border px-3 py-2 min-w-[140px]">
                    <span className="text-xs text-muted-foreground">Enabled</span>
                    <Switch checked={Boolean(row.enabled)} onCheckedChange={(checked) => updateRoot(index, { enabled: Boolean(checked) })} />
                  </div>
                  <Button
                    type="button"
                    variant={isWinner ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => {
                      setWinnerPath(normalizePath(String(row.path || '')));
                      setWinnerId(sid > 0 ? sid : null);
                    }}
                    disabled={row.role === 'incoming'}
                  >
                    {isWinner ? 'Processed here' : 'Set as destination'}
                  </Button>
                  <Button type="button" variant="ghost" size="icon" onClick={() => removeRoot(index)} className="shrink-0">
                    <Trash2 className="w-4 h-4" />
                  </Button>
                </div>
              </div>
            );
          })}
        </div>

        <div className="rounded-lg border border-border/70 bg-background/30 p-3 space-y-3">
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <Label className="text-sm">Add a folder</Label>
            </div>
            <p className="text-xs text-muted-foreground">
              Choose the folder type before adding it. Incoming is optional; use it only if fresh music lands in a separate drop zone.
            </p>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-[1fr_220px_110px] gap-2">
          <FolderBrowserInput
            value={pendingPath}
            onChange={setPendingPath}
            placeholder="/music/library"
            selectLabel="Add source root"
          />
          <Select value={pendingRole} onValueChange={(value: 'library' | 'incoming') => setPendingRole(value)}>
            <SelectTrigger><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="library">Standard source folder</SelectItem>
              <SelectItem value="incoming">Incoming folder</SelectItem>
            </SelectContent>
          </Select>
          <Button type="button" variant="outline" onClick={addRoot} disabled={!normalizePath(pendingPath)} className="gap-1.5">
            <Plus className="w-4 h-4" />
            Add folder
          </Button>
        </div>
        </div>

        <div className="rounded-lg border border-border/70 bg-background/30 p-3 space-y-3">
          <div className="space-y-1">
            <Label className="text-sm">If you use incoming folders, where should processed albums end up?</Label>
            <p className="text-xs text-muted-foreground">
              Choose the standard source folder that should receive albums once PMDA has matched them, checked duplicates/incompletes, and validated metadata.
            </p>
          </div>
          <div className="grid grid-cols-1 gap-3 md:grid-cols-[minmax(0,1fr),220px]">
            <Select
              value={currentWinnerPath || '__none__'}
              onValueChange={(value) => {
                if (value === '__none__') {
                  setWinnerPath('');
                  setWinnerId(null);
                  return;
                }
                const normalized = normalizePath(value);
                const selected = libraryDestinationRoots.find((row) => normalizePath(String(row.path || '')) === normalized) ?? null;
                setWinnerPath(normalized);
                setWinnerId(Number(selected?.source_id || 0) > 0 ? Number(selected?.source_id || 0) : null);
              }}
              disabled={libraryDestinationRoots.length === 0}
            >
              <SelectTrigger>
                <SelectValue placeholder="Select a standard source folder" />
              </SelectTrigger>
              <SelectContent>
                {libraryDestinationRoots.map((row) => {
                  const normalized = normalizePath(String(row.path || ''));
                  return (
                    <SelectItem key={`winner-root-${row.source_id || normalized}`} value={normalized}>
                      {normalized}
                    </SelectItem>
                  );
                })}
              </SelectContent>
            </Select>
            <Select value={strategy} onValueChange={(value: 'move' | 'hardlink' | 'symlink' | 'copy') => setStrategy(value)}>
              <SelectTrigger className="w-full"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="move">Move</SelectItem>
                <SelectItem value="hardlink">Hardlink</SelectItem>
                <SelectItem value="symlink">Symlink</SelectItem>
                <SelectItem value="copy">Copy</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <p className="text-[11px] text-muted-foreground">
            Recommended for most users: <span className="text-foreground">Hardlink</span>. If you do not use incoming folders, this setting can stay prepared for later.
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button type="button" onClick={save} disabled={saving} className="gap-2">
            {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
            Save folders
          </Button>
          <Button
            type="button"
            variant="outline"
            onClick={runIncoming}
            disabled={runningIncoming || incomingRootsCount <= 0}
            className="gap-2"
          >
            {runningIncoming ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
            Run incoming scan now
          </Button>
          {incomingRootsCount <= 0 ? (
            <span className="text-[11px] text-muted-foreground">No incoming folders configured. This is optional.</span>
          ) : null}
        </div>
      </div>

      <Collapsible open={advancedStatusOpen} onOpenChange={setAdvancedStatusOpen}>
        <div className="rounded-lg border border-border/70 bg-muted/20">
          <CollapsibleTrigger asChild>
            <Button type="button" variant="ghost" className="w-full justify-between rounded-none px-3 py-2 text-left">
              <span className="text-sm font-medium">Advanced status</span>
              <ChevronDown className={`w-4 h-4 transition-transform ${advancedStatusOpen ? 'rotate-180' : ''}`} />
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 border-t border-border/60 p-3 text-xs">
              <div className="rounded-lg border border-border bg-muted/30 p-3">
                <div className="font-medium text-foreground">Bootstrap</div>
                <div className="mt-1 text-muted-foreground">
                  {bootstrap?.bootstrap_required ? 'Initial full required' : 'Completed'}
                </div>
                <div className="mt-1 text-muted-foreground">Default scan: <span className="text-foreground">{String(bootstrap?.default_scan_type || 'full').replace('_', ' ')}</span></div>
              </div>
              <div className="rounded-lg border border-border bg-muted/30 p-3">
                <div className="font-medium text-foreground">Incoming Queue</div>
                <div className="mt-1 text-muted-foreground">Pending folders: <span className="text-foreground">{Number(incomingStatus?.pending_folders || 0).toLocaleString()}</span></div>
                <div className="mt-1 text-muted-foreground">Sources: <span className="text-foreground">{Array.isArray(incomingStatus?.incoming_source_ids) ? incomingStatus?.incoming_source_ids?.length : 0}</span></div>
              </div>
              <div className="rounded-lg border border-border bg-muted/30 p-3">
                <div className="font-medium text-foreground">Watcher</div>
                <div className="mt-1 text-muted-foreground">Running: <span className="text-foreground">{String(Boolean(watcherStatus?.running))}</span></div>
                <div className="mt-1 text-muted-foreground">Degraded: <span className="text-foreground">{String(Boolean(watcherDegraded))}</span></div>
              </div>
            </div>
          </CollapsibleContent>
        </div>
      </Collapsible>
    </div>
  );
}
