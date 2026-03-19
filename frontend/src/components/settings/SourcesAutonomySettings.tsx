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

  const enabledIncomingRootsCount = useMemo(
    () => roots.filter((row) => row.role === 'incoming' && row.enabled && normalizePath(String(row.path || ''))).length,
    [roots],
  );

  const currentWinnerPath = useMemo(() => {
    if (winnerPath) return winnerPath;
    const byId = roots.find((row) => Number(row.source_id || 0) > 0 && Number(row.source_id || 0) === Number(winnerId || 0));
    return normalizePath(String(byId?.path || ''));
  }, [roots, winnerId, winnerPath]);

  const libraryDestinationRoots = useMemo(
    () => roots.filter((row) => row.role === 'library' && row.enabled && normalizePath(String(row.path || ''))),
    [roots],
  );

  const hasIncomingRoots = useMemo(
    () => roots.some((row) => row.role === 'incoming' && row.enabled && normalizePath(String(row.path || ''))),
    [roots],
  );

  const hasIncompleteRows = useMemo(
    () => roots.some((row) => !normalizePath(String(row.path || ''))),
    [roots],
  );

  const updateRoot = useCallback((index: number, patch: Partial<FileSourceRoot>) => {
    setRoots((prev) => {
      const currentRow = prev[index];
      const currentPath = normalizePath(String(currentRow?.path || ''));
      const wasWinner = Boolean(
        (currentPath && currentPath === currentWinnerPath) ||
        (winnerId != null && Number(currentRow?.source_id || 0) > 0 && Number(currentRow?.source_id || 0) === Number(winnerId))
      );
      const next: FileSourceRoot[] = prev.map((row, i) => {
        if (i === index) return { ...row, ...patch };
        return row;
      });
      const nextRow = next[index];
      const nextPath = normalizePath(String(nextRow?.path || ''));

      if (wasWinner && patch.path !== undefined) {
        setWinnerPath(nextPath);
      }

      if (
        wasWinner &&
        (nextRow?.role === 'incoming' || !nextRow?.enabled || !nextPath)
      ) {
        const fallback = next.find((row, i) => i !== index && row.role === 'library' && row.enabled && normalizePath(String(row.path || '')))
          ?? next.find((row, i) => i !== index && row.role === 'library' && normalizePath(String(row.path || '')))
          ?? null;
        setWinnerPath(normalizePath(String(fallback?.path || '')));
        setWinnerId(Number(fallback?.source_id || 0) > 0 ? Number(fallback?.source_id || 0) : null);
      }
      return next;
    });
  }, [currentWinnerPath, winnerId]);

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
    setRoots((prev) => {
      const nextPriority = (prev.reduce((acc, row) => Math.max(acc, Number(row.priority || 0)), 0) || 0) + 10;
      return [
        ...prev,
        {
          path: '',
          role: 'library',
          enabled: true,
          priority: nextPriority,
          is_winner_root: false,
        },
      ];
    });
  }, []);

  useEffect(() => {
    if (libraryDestinationRoots.length === 0) {
      if (currentWinnerPath || winnerId != null) {
        setWinnerPath('');
        setWinnerId(null);
      }
      return;
    }
    const hasSelectedWinner = libraryDestinationRoots.some((row) => {
      const normalized = normalizePath(String(row.path || ''));
      return (
        (currentWinnerPath && normalized === currentWinnerPath) ||
        (winnerId != null && Number(row.source_id || 0) > 0 && Number(row.source_id || 0) === Number(winnerId))
      );
    });
    if (hasSelectedWinner) return;
    const fallback = libraryDestinationRoots[0];
    setWinnerPath(normalizePath(String(fallback?.path || '')));
    setWinnerId(Number(fallback?.source_id || 0) > 0 ? Number(fallback?.source_id || 0) : null);
  }, [libraryDestinationRoots, currentWinnerPath, winnerId]);

  const save = useCallback(async () => {
    if (roots.length === 0) {
      toast.error('Add at least one source root.');
      return;
    }
    if (hasIncompleteRows) {
      toast.error('Complete or remove empty folder rows before saving.');
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
  }, [roots, winnerId, currentWinnerPath, strategy, load, hasIncompleteRows]);

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
      <div className="space-y-4 rounded-xl border border-cyan-500/20 bg-cyan-500/[0.04] p-4">
        <div className="flex items-center justify-between gap-3">
          <div className="space-y-1">
            <Label className="text-sm">Source folders</Label>
            <p className="text-xs text-muted-foreground">
              Add every folder PMDA should scan. Use <span className="text-foreground">Incoming</span> only if new music lands in a separate drop zone.
            </p>
          </div>
          <Button type="button" variant="outline" size="sm" onClick={load} className="gap-1.5">
            <RefreshCw className="w-4 h-4" />
            Reload
          </Button>
        </div>

        <div className="flex flex-wrap gap-2 text-[11px] text-muted-foreground">
          <div className="rounded-md border border-border/70 bg-background/30 px-2.5 py-1">
            Standard source: <span className="font-medium text-foreground">{libraryRootsCount}</span>
          </div>
          <div className="rounded-md border border-border/70 bg-background/30 px-2.5 py-1">
            Incoming: <span className="font-medium text-foreground">{incomingRootsCount}</span>
          </div>
        </div>

        <div className="overflow-hidden rounded-lg border border-border/70 bg-background/25">
          <div className="hidden items-center gap-3 border-b border-border/60 px-3 py-2 text-[11px] uppercase tracking-wide text-muted-foreground lg:grid lg:grid-cols-[minmax(0,1.45fr)_230px_120px_44px]">
            <div>Folder</div>
            <div>Type</div>
            <div>Enabled</div>
            <div />
          </div>
          {roots.length === 0 ? (
            <div className="px-3 py-4 text-sm text-muted-foreground">
              No folders configured. Add your first music folder to get started.
            </div>
          ) : roots.map((row, index) => {
            return (
              <div
                key={`${row.source_id || 'new'}-${index}`}
                className="grid gap-3 border-t border-border/60 px-3 py-3 first:border-t-0 lg:grid-cols-[minmax(0,1.45fr)_230px_120px_44px] lg:items-center"
              >
                <FolderBrowserInput
                  value={String(row.path || '')}
                  onChange={(nextPath) => updateRoot(index, { path: nextPath })}
                  placeholder="/music/library"
                  selectLabel="Select source root folder"
                  compact
                />
                <Select
                  value={row.role === 'incoming' ? 'incoming' : 'library'}
                  onValueChange={(value: 'library' | 'incoming') => updateRoot(index, { role: value })}
                >
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="library">Standard source</SelectItem>
                    <SelectItem value="incoming">Incoming</SelectItem>
                  </SelectContent>
                </Select>
                <div className="flex h-10 items-center justify-between rounded-md border border-border bg-background/40 px-3">
                  <span className="text-xs text-muted-foreground">Enabled</span>
                  <Switch checked={Boolean(row.enabled)} onCheckedChange={(checked) => updateRoot(index, { enabled: Boolean(checked) })} />
                </div>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  onClick={() => removeRoot(index)}
                  className="shrink-0"
                  aria-label="Delete folder"
                  title="Delete folder"
                >
                  <Trash2 className="w-4 h-4" />
                </Button>
              </div>
            );
          })}
        </div>

        <Button type="button" variant="outline" size="sm" onClick={addRoot} className="gap-1.5">
          <Plus className="w-4 h-4" />
          + Add folder
        </Button>

        {hasIncomingRoots ? (
          <div className="rounded-lg border border-border/70 bg-background/30 p-3 space-y-3">
            <div className="space-y-1">
              <Label className="text-sm">Processed incoming albums go to</Label>
              <p className="text-xs text-muted-foreground">
                Choose which enabled standard source folder should receive albums once PMDA has processed them.
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
              Recommended for most users: <span className="text-foreground">Hardlink</span>. Files stay on disk once and PMDA keeps the processed library clean.
            </p>
          </div>
        ) : (
          <p className="text-[11px] text-muted-foreground">
            Incoming folders are optional. Add one only if new music lands in a separate drop zone.
          </p>
        )}

        <div className="flex flex-wrap items-center gap-2">
          <Button
            type="button"
            onClick={save}
            disabled={saving || hasIncompleteRows || roots.length === 0 || (hasIncomingRoots && (libraryDestinationRoots.length === 0 || !currentWinnerPath))}
            className="gap-2"
          >
            {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
            Save folders
          </Button>
          <Button
            type="button"
            variant="outline"
            onClick={runIncoming}
            disabled={runningIncoming || enabledIncomingRootsCount <= 0}
            className="gap-2"
          >
            {runningIncoming ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
            Run incoming scan now
          </Button>
          {hasIncompleteRows ? (
            <span className="text-[11px] text-muted-foreground">Complete or remove empty folder rows before saving.</span>
          ) : enabledIncomingRootsCount <= 0 ? (
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
