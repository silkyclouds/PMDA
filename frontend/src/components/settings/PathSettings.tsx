import { useState, useCallback, useEffect, useRef } from 'react';
import { FolderOpen, Wand2, Loader2, ChevronDown, CheckCircle2, XCircle, RefreshCw, RotateCcw } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Button } from '@/components/ui/button';
import { FieldTooltip } from '@/components/ui/field-tooltip';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Switch } from '@/components/ui/switch';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Progress } from '@/components/ui/progress';
import * as api from '@/lib/api';
import type { PMDAConfig, PathVerifyResult } from '@/lib/api';
import { cn } from '@/lib/utils';

type AutoStep = 'idle' | 'detecting' | 'resolving' | 'verifying' | 'done' | 'error';

interface PathSettingsProps {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
  errors: Record<string, string>;
}

export function PathSettings({ config, updateConfig, errors }: PathSettingsProps) {
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [autodetecting, setAutodetecting] = useState(false);
  const [autodetectResult, setAutodetectResult] = useState<{ success: boolean; message?: string } | null>(null);
  const [verifying, setVerifying] = useState(false);
  const [verifyResults, setVerifyResults] = useState<PathVerifyResult[] | null>(null);
  const [verifyError, setVerifyError] = useState<string | null>(null);
  const [verifyHint, setVerifyHint] = useState<string | null>(null);

  const [autoStep, setAutoStep] = useState<AutoStep>('idle');
  const [autoError, setAutoError] = useState<string | null>(null);
  const [discoveredList, setDiscoveredList] = useState<Array<{ plex: string; host: string }>>([]);
  const [resolvingProgress, setResolvingProgress] = useState<{
    current: number;
    total: number;
    plex: string;
    host: string;
  } | null>(null);
  const [reverifyingPlex, setReverifyingPlex] = useState<string | null>(null);
  const [resolvingPlex, setResolvingPlex] = useState<string | null>(null);
  const [bindingProgress, setBindingProgress] = useState<{
    phase: 'detecting' | 'resolving' | 'verifying';
    current: number;
    total: number;
  } | null>(null);
  const lastAutoSectionIdsRef = useRef<string>('');

  const pathMapObj = config.PATH_MAP != null && typeof config.PATH_MAP === 'object' && !Array.isArray(config.PATH_MAP)
    ? config.PATH_MAP as Record<string, string>
    : {};
  const pathMapString = Object.keys(pathMapObj).length
    ? Object.entries(pathMapObj).map(([k, v]) => `${k}=${v}`).join('\n')
    : '';

  const runAutoSequence = useCallback(async () => {
    const host = (config.PLEX_HOST ?? '').trim();
    const token = (config.PLEX_TOKEN ?? '').trim();
    const sectionIds = config.SECTION_IDS;
    if (!host || !token || !sectionIds) {
      setAutoStep('idle');
      setBindingProgress(null);
      return;
    }
    setVerifyError(null);
    setVerifyHint(null);
    setAutoError(null);
    setVerifyResults(null);
    setDiscoveredList([]);
    updateConfig({ PATH_MAP: {} });

    try {
      setAutoStep('detecting');
      setBindingProgress({ phase: 'detecting', current: 0, total: 1 });
      const autodetect = await api.autodetectPaths({
        PLEX_HOST: host,
        PLEX_TOKEN: token,
        SECTION_IDS: sectionIds,
      });
      if (!autodetect.success || !autodetect.paths || Object.keys(autodetect.paths).length === 0) {
        setAutoStep('error');
        setAutoError(autodetect.message || 'No paths detected from Plex.');
        setBindingProgress(null);
        return;
      }

      const plexRoots = Object.keys(autodetect.paths);
      const N = plexRoots.length;
      const totalSteps = 1 + N * 2;

      setAutoStep('resolving');
      const musicRoot = (config.MUSIC_PARENT_PATH ?? '').trim() || '/music';
      const samples = config.CROSSCHECK_SAMPLES ?? 15;
      const resolvedPaths: Record<string, string> = {};
      const discovered: Array<{ plex: string; host: string }> = [];
      for (let i = 0; i < plexRoots.length; i++) {
        const plex = plexRoots[i];
        setBindingProgress({ phase: 'resolving', current: i + 1, total: totalSteps });
        setResolvingProgress({ current: i + 1, total: plexRoots.length, plex, host: '' });
        const one = await api.discoverPathOne({
          plex_root: plex,
          PLEX_DB_PATH: config.PLEX_DB_PATH,
          MUSIC_PARENT_PATH: musicRoot,
          CROSSCHECK_SAMPLES: samples,
        });
        if (!one.success || !one.host_root) {
          setResolvingProgress(null);
          setBindingProgress(null);
          setAutoStep('error');
          setAutoError(one.message || `Could not resolve: ${plex}`);
          return;
        }
        resolvedPaths[plex] = one.host_root;
        discovered.push({ plex, host: one.host_root });
        setDiscoveredList([...discovered]);
        setResolvingProgress({ current: i + 1, total: plexRoots.length, plex, host: one.host_root });
      }
      setResolvingProgress(null);
      updateConfig({ PATH_MAP: resolvedPaths });

      setAutoStep('verifying');
      const mergedResults: PathVerifyResult[] = [];
      const samplesVerify = config.CROSSCHECK_SAMPLES ?? 15;
      const entries = Object.entries(resolvedPaths);
      for (let i = 0; i < entries.length; i++) {
        setBindingProgress({ phase: 'verifying', current: 1 + N + i, total: totalSteps });
        const [plex_root, host_root] = entries[i];
        const verify = await api.verifyPaths({
          PATH_MAP: { [plex_root]: host_root },
          PLEX_DB_PATH: config.PLEX_DB_PATH,
          CROSSCHECK_SAMPLES: samplesVerify,
        });
        const one = verify.results?.[0];
        if (one) mergedResults.push(one);
        if (verify.hint) setVerifyHint(verify.hint);
      }
      setVerifyResults(mergedResults.length > 0 ? mergedResults : null);
      const hasFailures = mergedResults.some((r) => r.status === 'fail');
      if (hasFailures) setVerifyError('Some bindings failed verification.');
      setBindingProgress(null);
      setAutoStep('done');
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Detect & verify failed';
      setAutoStep('error');
      setAutoError(message);
      setResolvingProgress(null);
      setBindingProgress(null);
    }
  }, [config.PLEX_HOST, config.PLEX_TOKEN, config.SECTION_IDS, config.MUSIC_PARENT_PATH, config.PLEX_DB_PATH, config.CROSSCHECK_SAMPLES, updateConfig]);

  // Load last verification result when PATH_MAP is configured so Status/Samples/Message show without re-running verify
  useEffect(() => {
    if (Object.keys(pathMapObj).length === 0) return;
    api.getPathVerifyLast().then(({ results }) => {
      if (results && results.length > 0) setVerifyResults(results);
    });
  }, [pathMapString]);

  // When libraries (SECTION_IDS) are chosen and Plex is configured, auto-run Detect & verify bindings
  useEffect(() => {
    const host = (config.PLEX_HOST ?? '').trim();
    const token = (config.PLEX_TOKEN ?? '').trim();
    const sectionIdsRaw = config.SECTION_IDS;
    const sectionIdsStr = Array.isArray(sectionIdsRaw)
      ? sectionIdsRaw.map(String).sort().join(',')
      : String(sectionIdsRaw ?? '')
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean)
          .sort()
          .join(',');
    if (
      !host ||
      !token ||
      !sectionIdsStr ||
      sectionIdsStr === lastAutoSectionIdsRef.current ||
      autoStep === 'detecting' ||
      autoStep === 'resolving' ||
      autoStep === 'verifying'
    ) {
      return;
    }
    lastAutoSectionIdsRef.current = sectionIdsStr;
    runAutoSequence();
  }, [config.PLEX_HOST, config.PLEX_TOKEN, config.SECTION_IDS, autoStep, runAutoSequence]);

  const handlePathMapChange = (value: string) => {
    const lines = value.split('\n').filter(Boolean);
    const map: Record<string, string> = {};
    lines.forEach((line) => {
      const [key, ...rest] = line.split('=');
      if (key && rest.length) {
        map[key.trim()] = rest.join('=').trim();
      }
    });
    updateConfig({ PATH_MAP: map });
  };

  const runAutodetect = async () => {
    setAutodetecting(true);
    setAutodetectResult(null);
    setVerifyResults(null);
    setVerifyError(null);
    try {
      const result = await api.autodetectPaths({
        PLEX_HOST: config.PLEX_HOST,
        PLEX_TOKEN: config.PLEX_TOKEN,
        SECTION_IDS: config.SECTION_IDS,
      });
      if (result.success && Object.keys(result.paths).length > 0) {
        updateConfig({ PATH_MAP: result.paths });
        setAutodetectResult({ success: true, message: `Detected ${Object.keys(result.paths).length} path mappings` });
      } else {
        setAutodetectResult({ success: false, message: result.message || 'No paths detected' });
      }
    } catch {
      setAutodetectResult({ success: false, message: 'Autodetection failed' });
    } finally {
      setAutodetecting(false);
    }
  };

  const verifyBindings = async () => {
    if (!config.PATH_MAP || Object.keys(config.PATH_MAP).length === 0) {
      setVerifyError('Add path mappings first (or use Autodetect).');
      setVerifyResults(null);
      return;
    }
    setVerifying(true);
    setVerifyResults(null);
    setVerifyError(null);
    try {
      const res = await api.verifyPaths({
        PATH_MAP: config.PATH_MAP,
        PLEX_DB_PATH: config.PLEX_DB_PATH,
        CROSSCHECK_SAMPLES: config.CROSSCHECK_SAMPLES ?? 5,
      });
      if (res.success) {
        setVerifyResults(res.results);
        if (res.message) setVerifyError(res.message);
      } else {
        setVerifyError(res.message || 'Verification failed');
        setVerifyResults(res.results?.length ? res.results : null);
      }
      if (res.hint) setVerifyHint(res.hint);
    } catch {
      setVerifyError('Verification failed (check Plex DB path).');
      setVerifyResults(null);
    } finally {
      setVerifying(false);
    }
  };

  const reverifyOne = async (plex_root: string, host_root: string) => {
    setReverifyingPlex(plex_root);
    setVerifyError(null);
    try {
      const res = await api.verifyPaths({
        PATH_MAP: { [plex_root]: host_root },
        PLEX_DB_PATH: config.PLEX_DB_PATH,
        CROSSCHECK_SAMPLES: config.CROSSCHECK_SAMPLES ?? 15,
      });
      const one = res.results?.[0];
      setVerifyResults((prev) =>
        prev && one
          ? prev.map((r) => (r.plex_root === plex_root ? one : r))
          : prev
      );
      if (res.hint) setVerifyHint(res.hint);
      if (!res.success && res.message) setVerifyError(res.message);
    } catch {
      setVerifyError('Re-verification failed.');
    } finally {
      setReverifyingPlex(null);
    }
  };

  const resolveOneBinding = async (plex_root: string) => {
    setResolvingPlex(plex_root);
    setVerifyError(null);
    const musicRoot = (config.MUSIC_PARENT_PATH ?? '').trim() || '/music';
    const samples = config.CROSSCHECK_SAMPLES ?? 15;
    try {
      const one = await api.discoverPathOne({
        plex_root,
        PLEX_DB_PATH: config.PLEX_DB_PATH,
        MUSIC_PARENT_PATH: musicRoot,
        CROSSCHECK_SAMPLES: samples,
      });
      if (!one.success || !one.host_root) {
        setVerifyError(one.message || `Could not resolve: ${plex_root}`);
        return;
      }
      const newMap = { ...(config.PATH_MAP || {}), [plex_root]: one.host_root };
      updateConfig({ PATH_MAP: newMap });
      await reverifyOne(plex_root, one.host_root);
      setDiscoveredList((prev) => {
        const out = prev.filter((p) => p.plex !== plex_root);
        out.push({ plex: plex_root, host: one.host_root });
        return out.sort((a, b) => a.plex.localeCompare(b.plex));
      });
    } catch {
      setVerifyError('Resolve failed.');
    } finally {
      setResolvingPlex(null);
    }
  };

  const hasPlexConfig = (config.PLEX_HOST ?? '').trim() && (config.PLEX_TOKEN ?? '').trim() && config.SECTION_IDS;

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3 mb-4">
        <div className="p-2 rounded-lg bg-primary/10">
          <FolderOpen className="w-5 h-5 text-primary" />
        </div>
        <div>
          <h3 className="font-medium">Paths & Mapping</h3>
          <p className="text-sm text-muted-foreground">
            Configure directory paths for PMDA. Paths are detected and verified automatically.
          </p>
        </div>
      </div>

      {/* Path access (read-write) – confirmed by backend so scan and auto-move can run */}
      {config.paths_status && (
        <div className="rounded-lg border border-border bg-muted/30 p-3 flex flex-wrap items-center gap-x-4 gap-y-2 text-sm">
          <span className="font-medium text-foreground/90">Path access:</span>
          <span
            className={cn(
              "inline-flex items-center gap-1.5",
              config.paths_status.music_rw ? "text-green-600 dark:text-green-400" : "text-destructive"
            )}
            title="Music folder(s) must be read-write for scan and moving duplicates"
          >
            {config.paths_status.music_rw ? <CheckCircle2 className="w-4 h-4" /> : <XCircle className="w-4 h-4" />}
            Music folder(s): {config.paths_status.music_rw ? "RW ✓" : "RW ✗"}
          </span>
          <span
            className={cn(
              "inline-flex items-center gap-1.5",
              config.paths_status.dupes_rw ? "text-green-600 dark:text-green-400" : "text-destructive"
            )}
            title="Dupes folder must be read-write to move duplicate albums"
          >
            {config.paths_status.dupes_rw ? <CheckCircle2 className="w-4 h-4" /> : <XCircle className="w-4 h-4" />}
            Dupes folder: {config.paths_status.dupes_rw ? "RW ✓" : "RW ✗"}
          </span>
        </div>
      )}

      <div className="space-y-4">
        {!hasPlexConfig && (
          <p className="text-sm text-muted-foreground">
            Configure Plex and libraries in the previous steps, then return here to detect and verify paths.
          </p>
        )}

        {/* Progress bar: shown inside tile when detect/resolve/verify is running */}
        {hasPlexConfig && bindingProgress && (
          <div className="rounded-lg border border-border bg-muted/30 p-3 space-y-2">
            <div className="flex items-center justify-between text-sm">
              <span className="font-medium text-foreground/90">
                {bindingProgress.phase === 'detecting' && 'Detecting path mappings…'}
                {bindingProgress.phase === 'resolving' && (() => {
                  const n = Math.floor((bindingProgress.total - 1) / 2);
                  return `Resolving bindings (${Math.min(bindingProgress.current, n)}/${n})…`;
                })()}
                {bindingProgress.phase === 'verifying' && (() => {
                  const n = Math.floor((bindingProgress.total - 1) / 2);
                  const v = bindingProgress.current - n;
                  return `Verifying bindings (${Math.min(v, n)}/${n})…`;
                })()}
              </span>
              <span className="text-muted-foreground tabular-nums">
                {bindingProgress.total > 0 ? Math.round((bindingProgress.current / bindingProgress.total) * 100) : 0}%
              </span>
            </div>
            <Progress
              value={bindingProgress.total > 0 ? (bindingProgress.current / bindingProgress.total) * 100 : 0}
              className="h-2"
            />
          </div>
        )}

        {hasPlexConfig && (
          <>
            {autoStep === 'resolving' && resolvingProgress && (
              <div className="space-y-1.5">
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Loader2 className="w-4 h-4 animate-spin shrink-0" />
                  Resolving {resolvingProgress.current}/{resolvingProgress.total}:{' '}
                  <span className="font-mono text-foreground">{resolvingProgress.plex}</span>
                </div>
                {resolvingProgress.host && (
                  <p className="text-xs text-green-600 dark:text-green-500 pl-6">
                    Found: <span className="font-mono">{resolvingProgress.plex}</span> → <span className="font-mono">{resolvingProgress.host}</span>
                  </p>
                )}
                {discoveredList.length > 0 && (
                  <ul className="text-xs text-muted-foreground pl-6 list-disc list-inside">
                    {Array.isArray(discoveredList) && discoveredList.map(({ plex, host }) => (
                      <li key={plex}>
                        <span className="font-mono text-foreground">{plex}</span> → <span className="font-mono text-foreground">{host}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            )}
            {autoStep === 'error' && autoError && (
              <div className="rounded-md border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive">
                {autoError}
              </div>
            )}

            {/* Table: show when we have bindings (from config or from a completed run) and not currently detecting/resolving/verifying */}
            {(autoStep === 'idle' || autoStep === 'done') && Object.keys(pathMapObj).length > 0 && (
              <>
                <p className="text-sm text-muted-foreground">
                  {Object.keys(pathMapObj).length} path mapping{Object.keys(pathMapObj).length !== 1 ? 's' : ''} configured.
                  Re-detect below to verify bindings and refresh status.
                </p>
                {verifyHint && (
                  <p className="text-xs text-muted-foreground rounded-md bg-muted/50 p-2 border border-border">
                    {verifyHint}
                  </p>
                )}
                {verifyError && (
                  <p className="text-xs text-destructive">{verifyError}</p>
                )}
                <div className="rounded-md border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="font-mono text-xs">Plex path</TableHead>
                        <TableHead className="font-mono text-xs">Host path</TableHead>
                        <TableHead className="w-24">Status</TableHead>
                        <TableHead className="text-right">Samples</TableHead>
                        <TableHead className="text-muted-foreground text-xs">Message</TableHead>
                        <TableHead className="w-40">Action</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {Object.entries(pathMapObj).map(([plex_root, host_root]) => {
                        const verified = verifyResults?.find((r) => r.plex_root === plex_root);
                        return (
                          <TableRow key={plex_root}>
                            <TableCell className="font-mono text-xs">{plex_root}</TableCell>
                            <TableCell className="font-mono text-xs">{host_root}</TableCell>
                            <TableCell>
                              {verified ? (
                                verified.status === 'ok' ? (
                                  <span className="inline-flex items-center gap-1 text-green-600 dark:text-green-500">
                                    <CheckCircle2 className="w-4 h-4" /> OK
                                  </span>
                                ) : (
                                  <span className="inline-flex items-center gap-1 text-destructive">
                                    <XCircle className="w-4 h-4" /> Fail
                                  </span>
                                )
                              ) : (
                                <span className="text-muted-foreground text-xs">—</span>
                              )}
                            </TableCell>
                            <TableCell className="text-right">{verified?.samples_checked ?? '—'}</TableCell>
                            <TableCell className="text-muted-foreground text-xs">{verified?.message ?? '—'}</TableCell>
                            <TableCell>
                              <div className="flex items-center gap-1 flex-wrap">
                                {(!verified || verified.status === 'fail') && (
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    className="h-7 gap-1 text-xs"
                                    disabled={resolvingPlex === plex_root || reverifyingPlex === plex_root}
                                    onClick={() => resolveOneBinding(plex_root)}
                                  >
                                    {resolvingPlex === plex_root ? (
                                      <Loader2 className="w-3 h-3 animate-spin" />
                                    ) : (
                                      <Wand2 className="w-3 h-3" />
                                    )}
                                    Resolve
                                  </Button>
                                )}
                                <Button
                                  type="button"
                                  variant="ghost"
                                  size="sm"
                                  className="h-7 gap-1 text-xs"
                                  disabled={reverifyingPlex === plex_root || resolvingPlex === plex_root}
                                  onClick={() => reverifyOne(plex_root, host_root)}
                                >
                                  {reverifyingPlex === plex_root ? (
                                    <Loader2 className="w-3 h-3 animate-spin" />
                                  ) : (
                                    <RotateCcw className="w-3 h-3" />
                                  )}
                                  Verify
                                </Button>
                              </div>
                            </TableCell>
                          </TableRow>
                        );
                      })}
                    </TableBody>
                  </Table>
                </div>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => runAutoSequence()}
                  className="gap-1.5 mt-2"
                >
                  <RefreshCw className="w-3 h-3" />
                  Re-detect and verify bindings
                </Button>
              </>
            )}

            {/* No bindings yet: only show the button */}
            {(autoStep === 'idle' || autoStep === 'error') && Object.keys(pathMapObj).length === 0 && (
              <>
                {autoStep === 'error' && autoError ? null : (
                  <p className="text-sm text-muted-foreground">
                    No path mappings yet. Click the button below to detect and verify bindings from Plex.
                  </p>
                )}
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => runAutoSequence()}
                  className="gap-1.5"
                >
                  <Wand2 className="w-3 h-3" />
                  Detect & verify bindings
                </Button>
              </>
            )}
          </>
        )}

        {/* Advanced Options */}
        <Collapsible open={showAdvanced} onOpenChange={setShowAdvanced}>
          <CollapsibleTrigger asChild>
            <Button variant="ghost" size="sm" className="gap-1.5 text-muted-foreground">
              <ChevronDown className={`w-4 h-4 transition-transform ${showAdvanced ? 'rotate-180' : ''}`} />
              Advanced Options
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent className="mt-4 space-y-4">
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="dupe-root">Duplicates Folder</Label>
                <FieldTooltip content="Folder where duplicate albums will be moved (e.g. /dupes in container). This is where PMDA stores albums it marks as duplicates." />
              </div>
              <Input
                id="dupe-root"
                placeholder="/dupes"
                value={config.DUPE_ROOT || ''}
                onChange={(e) => updateConfig({ DUPE_ROOT: e.target.value })}
              />
            </div>

            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="music-parent-path">Path to parent folder (music root)</Label>
                <FieldTooltip content="Parent directory that contains your music library folders (e.g. /music). Used for path mapping verification. In Docker, mount this path so PMDA can validate bindings." />
              </div>
              <Input
                id="music-parent-path"
                placeholder="/music"
                value={config.MUSIC_PARENT_PATH || ''}
                onChange={(e) => updateConfig({ MUSIC_PARENT_PATH: e.target.value })}
              />
            </div>

            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-1.5">
                  <Label htmlFor="path-map">Path Mapping</Label>
                  <FieldTooltip content="Optional path overrides. Maps Plex paths to host paths if they differ. Format: plex_path=host_path (one per line). Use Autodetect to discover mappings." />
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={runAutodetect}
                    disabled={autodetecting}
                    className="gap-1.5"
                  >
                    {autodetecting ? (
                      <Loader2 className="w-3 h-3 animate-spin" />
                    ) : (
                      <Wand2 className="w-3 h-3" />
                    )}
                    Autodetect
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={verifyBindings}
                    disabled={verifying || !config.PATH_MAP || Object.keys(config.PATH_MAP).length === 0}
                    className="gap-1.5"
                  >
                    {verifying ? (
                      <Loader2 className="w-3 h-3 animate-spin" />
                    ) : null}
                    Verify bindings
                  </Button>
                </div>
              </div>
              <Textarea
                id="path-map"
                placeholder={`/music/matched=/music/Music_matched\n/music/unmatched=/music/Music_dump\n/music/compilations=/music/Compilations`}
                value={pathMapString}
                onChange={(e) => handlePathMapChange(e.target.value)}
                rows={4}
                className="font-mono text-sm"
              />
              {autodetectResult && (
                <p className={`text-xs ${autodetectResult.success ? 'text-green-600 dark:text-green-500' : 'text-destructive'}`}>
                  {autodetectResult.message}
                </p>
              )}
              {verifying && (
                <p className="text-sm text-muted-foreground">Please wait, cross-checking the folder bindings…</p>
              )}
              {verifyError && !verifying && (
                <p className="text-xs text-destructive">{verifyError}</p>
              )}
              {verifyHint && !verifying && (
                <p className="text-xs text-muted-foreground rounded-md bg-muted/50 p-2 border border-border">
                  {verifyHint}
                </p>
              )}
              {verifyResults && verifyResults.length > 0 && !verifying && (
                <div className="rounded-md border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="font-mono text-xs">Plex path</TableHead>
                        <TableHead className="font-mono text-xs">Host path</TableHead>
                        <TableHead className="w-24">Status</TableHead>
                        <TableHead className="text-right">Samples</TableHead>
                        <TableHead className="text-muted-foreground text-xs">Message</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {Array.isArray(verifyResults) && verifyResults.map((r, i) => (
                        <TableRow key={`${r.plex_root}-${i}`}>
                          <TableCell className="font-mono text-xs">{r.plex_root}</TableCell>
                          <TableCell className="font-mono text-xs">{r.host_root}</TableCell>
                          <TableCell>
                            {r.status === 'ok' ? (
                              <span className="inline-flex items-center gap-1 text-green-600 dark:text-green-500">
                                <CheckCircle2 className="w-4 h-4" /> OK
                              </span>
                            ) : (
                              <span className="inline-flex items-center gap-1 text-destructive">
                                <XCircle className="w-4 h-4" /> Fail
                              </span>
                            )}
                          </TableCell>
                          <TableCell className="text-right">{r.samples_checked}</TableCell>
                          <TableCell className="text-muted-foreground text-xs">{r.message}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
            </div>

            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="config-dir">Config Directory</Label>
                <FieldTooltip content="Directory for config files, state database, and cache (usually /config in Docker). PMDA stores its working data here." />
              </div>
              <Input
                id="config-dir"
                placeholder="/config"
                value={config.PMDA_CONFIG_DIR || ''}
                onChange={(e) => updateConfig({ PMDA_CONFIG_DIR: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="crosscheck-samples">Crosscheck Samples</Label>
                <FieldTooltip content="Number of sample paths to validate at startup for path mapping verification. Set to 0 to disable crosschecking." />
              </div>
              <Input
                id="crosscheck-samples"
                type="number"
                min={0}
                placeholder="15"
                value={config.CROSSCHECK_SAMPLES?.toString() ?? ''}
                onChange={(e) => updateConfig({ CROSSCHECK_SAMPLES: parseInt(e.target.value, 10) || 0 })}
              />
            </div>
            <div className="flex items-center justify-between gap-4">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="disable-path-crosscheck">Disable path crosscheck</Label>
                <FieldTooltip content="If enabled, PMDA will not verify path bindings at startup. Use only if you are sure paths are correct and want to skip verification." />
              </div>
              <Switch
                id="disable-path-crosscheck"
                checked={config.DISABLE_PATH_CROSSCHECK ?? false}
                onCheckedChange={(checked) => updateConfig({ DISABLE_PATH_CROSSCHECK: checked })}
              />
            </div>
          </CollapsibleContent>
        </Collapsible>
      </div>
    </div>
  );
}
