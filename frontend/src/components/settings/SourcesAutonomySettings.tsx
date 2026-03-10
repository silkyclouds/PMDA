import { useCallback, useEffect, useMemo, useState } from 'react';
import { ChevronDown, Loader2, Plus, RefreshCw, Save, Trash2 } from 'lucide-react';
import { toast } from 'sonner';

import * as api from '@/lib/api';
import type { FileSourceRoot } from '@/lib/api';
import { Button } from '@/components/ui/button';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Input } from '@/components/ui/input';
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
  const [strategy, setStrategy] = useState<'move' | 'hardlink' | 'symlink' | 'copy'>('move');

  const [bootstrap, setBootstrap] = useState<api.BootstrapStatus | null>(null);
  const [incomingStatus, setIncomingStatus] = useState<api.IncomingDeltaStatus | null>(null);
  const [watcherStatus, setWatcherStatus] = useState<api.FilesWatcherStatus | null>(null);

  const [pendingPath, setPendingPath] = useState('');
  const [pendingRole, setPendingRole] = useState<'library' | 'incoming'>('incoming');

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

  const updateRoot = useCallback((index: number, patch: Partial<FileSourceRoot>) => {
    setRoots((prev) => prev.map((row, i) => (i === index ? { ...row, ...patch } : row)));
  }, []);

  const removeRoot = useCallback((index: number) => {
    setRoots((prev) => prev.filter((_, i) => i !== index));
  }, []);

  const addRoot = useCallback(() => {
    const normalized = normalizePath(pendingPath);
    if (!normalized) return;
    setRoots((prev) => {
      if (prev.some((r) => normalizePath(r.path) === normalized)) {
        return prev;
      }
      const nextPriority = (prev.reduce((acc, row) => Math.max(acc, Number(row.priority || 0)), 0) || 0) + 10;
      return [
        ...prev,
        {
          path: normalized,
          role: pendingRole,
          enabled: true,
          priority: nextPriority,
          is_winner_root: false,
        },
      ];
    });
    setPendingPath('');
  }, [pendingPath, pendingRole]);

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
          return {
            source_id: row.source_id,
            path: normalized,
            role: row.role === 'incoming' ? 'incoming' : 'library',
            enabled: Boolean(row.enabled),
            priority: Math.max(1, Number(row.priority || ((idx + 1) * 10))),
            is_winner_root: winnerId != null && Number(row.source_id || 0) === Number(winnerId),
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
      setStrategy((res.winner_placement_strategy || strategy) as 'move' | 'hardlink' | 'symlink' | 'copy');
      toast.success('Sources & autonomy settings saved');
      await load();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to save source roots');
    } finally {
      setSaving(false);
    }
  }, [roots, winnerId, strategy, load]);

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
        <p className="text-sm font-medium">How to configure this</p>
        <ol className="list-decimal pl-4 text-xs text-muted-foreground space-y-1">
          <li>Add your folders, then mark each one as <span className="text-foreground">Library</span> or <span className="text-foreground">Incoming</span>.</li>
          <li>Pick one <span className="text-foreground">Primary library</span> folder (winner destination).</li>
          <li>Save, then optionally trigger an incoming changed-only scan.</li>
        </ol>
      </div>

      <div className="grid grid-cols-1 gap-2 text-xs md:grid-cols-3">
        <div className="rounded-md border border-border bg-muted/30 px-3 py-2 text-muted-foreground">
          Library roots: <span className="text-foreground font-medium">{libraryRootsCount}</span>
        </div>
        <div className="rounded-md border border-border bg-muted/30 px-3 py-2 text-muted-foreground">
          Incoming roots: <span className="text-foreground font-medium">{incomingRootsCount}</span>
        </div>
        <div className="rounded-md border border-border bg-muted/30 px-3 py-2 text-muted-foreground">
          Primary root: <span className="text-foreground font-medium">{winnerId ? `#${winnerId}` : 'not set'}</span>
        </div>
      </div>

      <div className="space-y-3 rounded-lg border border-border p-4">
        <div className="flex items-center justify-between gap-3">
          <div className="space-y-1">
            <Label>Primary library placement</Label>
            <p className="text-xs text-muted-foreground">How PMDA writes the winner album into the primary library root.</p>
          </div>
          <Select value={strategy} onValueChange={(value: 'move' | 'hardlink' | 'symlink' | 'copy') => setStrategy(value)}>
            <SelectTrigger className="w-[220px]"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="move">Move (single copy)</SelectItem>
              <SelectItem value="hardlink">Hardlink (recommended)</SelectItem>
              <SelectItem value="symlink">Symlink</SelectItem>
              <SelectItem value="copy">Copy (extra space)</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      <div className="space-y-3 rounded-lg border border-border p-4">
        <div className="flex items-center justify-between gap-3">
          <div className="space-y-1">
            <Label>Source folders</Label>
            <p className="text-xs text-muted-foreground">Library = your current collection. Incoming = new arrivals to process.</p>
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
            const isWinner = winnerId != null && sid > 0 && winnerId === sid;
            return (
              <div key={`${row.source_id || 'new'}-${index}`} className="grid grid-cols-1 md:grid-cols-[1fr_220px_90px_110px_80px] gap-2 rounded-md border border-border bg-muted/20 p-2">
                <Input
                  value={String(row.path || '')}
                  onChange={(e) => updateRoot(index, { path: e.target.value })}
                  placeholder="/music/library"
                />
                <Select
                  value={row.role === 'incoming' ? 'incoming' : 'library'}
                  onValueChange={(value: 'library' | 'incoming') => updateRoot(index, { role: value })}
                >
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="library">Library (already in collection)</SelectItem>
                    <SelectItem value="incoming">Incoming (new arrivals)</SelectItem>
                  </SelectContent>
                </Select>
                <div className="flex items-center justify-between rounded-md border border-border px-2 py-1.5">
                  <span className="text-xs text-muted-foreground">Enabled</span>
                  <Switch checked={Boolean(row.enabled)} onCheckedChange={(checked) => updateRoot(index, { enabled: Boolean(checked) })} />
                </div>
                <Button
                  type="button"
                  variant={isWinner ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => setWinnerId(sid > 0 ? sid : null)}
                  disabled={sid <= 0}
                >
                  {isWinner ? 'Primary' : 'Set primary'}
                </Button>
                <Button type="button" variant="ghost" size="icon" onClick={() => removeRoot(index)}>
                  <Trash2 className="w-4 h-4" />
                </Button>
              </div>
            );
          })}
        </div>

        <div className="grid grid-cols-1 md:grid-cols-[1fr_220px_110px] gap-2">
          <FolderBrowserInput
            value={pendingPath}
            onChange={setPendingPath}
            placeholder="/music/incoming"
            selectLabel="Add source root"
          />
          <Select value={pendingRole} onValueChange={(value: 'library' | 'incoming') => setPendingRole(value)}>
            <SelectTrigger><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="library">Library (already in collection)</SelectItem>
              <SelectItem value="incoming">Incoming (new arrivals)</SelectItem>
            </SelectContent>
          </Select>
          <Button type="button" variant="outline" onClick={addRoot} disabled={!normalizePath(pendingPath)} className="gap-1.5">
            <Plus className="w-4 h-4" />
            Add root
          </Button>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button type="button" onClick={save} disabled={saving} className="gap-2">
            {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
            Save sources
          </Button>
          <Button type="button" variant="outline" onClick={runIncoming} disabled={runningIncoming} className="gap-2">
            {runningIncoming ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
            Run incoming scan now
          </Button>
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
