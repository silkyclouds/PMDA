import { useCallback, useEffect, useMemo, useState } from 'react';
import { ArrowUp, FolderOpen, Loader2, RefreshCw } from 'lucide-react';
import * as api from '@/lib/api';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { ScrollArea } from '@/components/ui/scroll-area';

interface FolderBrowserInputProps {
  value: string;
  onChange: (nextPath: string) => void;
  placeholder?: string;
  selectLabel?: string;
  compact?: boolean;
  browseRoot?: string;
  lockToBrowseRoot?: boolean;
  allowManualEntry?: boolean;
}

interface BrowseState {
  currentPath: string;
  parentPath: string | null;
  roots: string[];
  directories: api.FilesystemDirectoryEntry[];
  writable: boolean;
  truncated: boolean;
}

function normalizePath(pathValue: string | undefined | null): string {
  const p = (pathValue || '').trim();
  return p || '/';
}

function isPathWithinRoot(pathValue: string, rootValue: string): boolean {
  const path = normalizePath(pathValue);
  const root = normalizePath(rootValue);
  if (root === '/') return true;
  return path === root || path.startsWith(`${root}/`);
}

export function FolderBrowserInput({
  value,
  onChange,
  placeholder = '/music',
  selectLabel = 'Select folder',
  compact = false,
  browseRoot,
  lockToBrowseRoot = false,
  allowManualEntry = true,
}: FolderBrowserInputProps) {
  const normalizedBrowseRoot = browseRoot ? normalizePath(browseRoot) : '';
  const clampPathToRoot = useCallback((pathValue: string | undefined | null) => {
    const normalized = normalizePath(pathValue);
    if (!lockToBrowseRoot || !normalizedBrowseRoot) return normalized;
    return isPathWithinRoot(normalized, normalizedBrowseRoot) ? normalized : normalizedBrowseRoot;
  }, [lockToBrowseRoot, normalizedBrowseRoot]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pathInput, setPathInput] = useState('');
  const [candidatePath, setCandidatePath] = useState(clampPathToRoot(value || normalizedBrowseRoot || placeholder));
  const [state, setState] = useState<BrowseState>({
    currentPath: clampPathToRoot(value || normalizedBrowseRoot || placeholder),
    parentPath: null,
    roots: [],
    directories: [],
    writable: false,
    truncated: false,
  });

  const selectedPath = useMemo(() => {
    const raw = String(value || '').trim();
    return raw ? clampPathToRoot(raw) : '';
  }, [clampPathToRoot, value]);

  const loadPath = useCallback(async (pathToLoad?: string, options?: { selectLoadedPath?: boolean }) => {
    setLoading(true);
    setError(null);
    const nextPath = clampPathToRoot(pathToLoad ?? value ?? normalizedBrowseRoot);
    try {
      if (options?.selectLoadedPath !== false) {
        setCandidatePath(nextPath);
        setPathInput(nextPath);
      }
      setState((prev) => ({
        ...prev,
        currentPath: nextPath,
      }));
      const result = await api.getFilesystemDirectories(nextPath, { timeoutMs: 45000 });
      setState({
        currentPath: result.path,
        parentPath: result.parent,
        roots: lockToBrowseRoot && normalizedBrowseRoot ? [normalizedBrowseRoot] : (Array.isArray(result.roots) ? result.roots : []),
        directories: Array.isArray(result.directories)
          ? result.directories.filter((dir) => !lockToBrowseRoot || !normalizedBrowseRoot || isPathWithinRoot(dir.path, normalizedBrowseRoot))
          : [],
        writable: Boolean(result.writable),
        truncated: Boolean(result.truncated),
      });
      if (options?.selectLoadedPath !== false) {
        setCandidatePath(result.path);
        setPathInput(result.path);
      }
    } catch (e) {
      if (lockToBrowseRoot && normalizedBrowseRoot && nextPath !== normalizedBrowseRoot) {
        try {
          const fallback = await api.getFilesystemDirectories(normalizedBrowseRoot, { timeoutMs: 45000 });
          setState({
            currentPath: fallback.path,
            parentPath: fallback.parent,
            roots: [normalizedBrowseRoot],
            directories: Array.isArray(fallback.directories)
              ? fallback.directories.filter((dir) => isPathWithinRoot(dir.path, normalizedBrowseRoot))
              : [],
            writable: Boolean(fallback.writable),
            truncated: Boolean(fallback.truncated),
          });
          setCandidatePath(fallback.path);
          setPathInput(fallback.path);
          setError(`Folder not found. Browsing reset to ${normalizedBrowseRoot}.`);
          return;
        } catch {
          // fall through to generic error below
        }
      }
      setError(e instanceof Error ? e.message : 'Unable to browse this folder');
    } finally {
      setLoading(false);
    }
  }, [clampPathToRoot, lockToBrowseRoot, normalizedBrowseRoot, value]);

  useEffect(() => {
    if (!open) return;
    const initial = lockToBrowseRoot && normalizedBrowseRoot
      ? normalizedBrowseRoot
      : clampPathToRoot(selectedPath || normalizedBrowseRoot || '/');
    setCandidatePath(initial);
    setPathInput(initial);
    loadPath(initial, { selectLoadedPath: true });
  }, [clampPathToRoot, loadPath, lockToBrowseRoot, normalizedBrowseRoot, open, selectedPath]);

  const confirmSelection = useCallback(async () => {
    const nextPath = (pathInput || '').trim() ? clampPathToRoot(pathInput) : clampPathToRoot(candidatePath);
    setConfirming(true);
    setError(null);
    try {
      const result = await api.getFilesystemDirectories(nextPath, { timeoutMs: 20000, limit: 0 });
      onChange(result.path);
      setOpen(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unable to use this folder');
    } finally {
      setConfirming(false);
    }
  }, [candidatePath, clampPathToRoot, onChange, pathInput]);

  const confirmSpecificPath = useCallback(async (pathValue: string) => {
    const nextPath = clampPathToRoot(pathValue);
    setConfirming(true);
    setError(null);
    try {
      const result = await api.getFilesystemDirectories(nextPath, { timeoutMs: 20000, limit: 0 });
      onChange(result.path);
      setCandidatePath(result.path);
      setPathInput(result.path);
      setOpen(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unable to use this folder');
    } finally {
      setConfirming(false);
    }
  }, [clampPathToRoot, onChange]);

  const displayedValue = selectedPath || ((lockToBrowseRoot && normalizedBrowseRoot) ? normalizedBrowseRoot : '');

  return (
    <>
      <div className="space-y-2 min-w-0">
        {!compact ? (
          <div className="rounded-md border border-border/70 bg-background/40 px-3 py-2">
            <div className="text-[11px] uppercase tracking-wide text-muted-foreground">Current path</div>
            <div className="mt-1 overflow-x-auto whitespace-nowrap font-mono text-xs text-foreground">
              {selectedPath || placeholder}
            </div>
          </div>
        ) : null}
        <div className={`grid gap-2 ${compact ? 'grid-cols-[minmax(0,1fr)_auto]' : 'grid-cols-1 sm:grid-cols-[minmax(0,1fr)_auto]'}`}>
          <Input
            value={displayedValue}
            onChange={(e) => {
              if (!allowManualEntry) return;
              onChange(e.target.value);
            }}
            placeholder={placeholder}
            className={`font-mono min-w-0 ${!allowManualEntry ? 'cursor-pointer' : ''}`}
            readOnly={!allowManualEntry}
            onClick={!allowManualEntry ? () => setOpen(true) : undefined}
          />
          <Button type="button" variant="outline" className="gap-2 shrink-0" onClick={() => setOpen(true)}>
            <FolderOpen className="w-4 h-4" />
            Browse
          </Button>
        </div>
      </div>

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>{selectLabel}</DialogTitle>
            <DialogDescription>
              {lockToBrowseRoot && normalizedBrowseRoot
                ? `Select an existing folder under ${normalizedBrowseRoot}.`
                : 'Select a folder from the container filesystem.'}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-3">
            {allowManualEntry ? (
              <div className="flex gap-2">
                <Input
                  value={pathInput}
                  onChange={(e) => setPathInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      e.preventDefault();
                      loadPath(pathInput);
                    }
                  }}
                  placeholder={normalizedBrowseRoot || '/'}
                  className="font-mono"
                />
                <Button type="button" variant="outline" onClick={() => loadPath(pathInput)}>
                  Go
                </Button>
                <Button type="button" variant="ghost" onClick={() => loadPath(candidatePath)} disabled={loading}>
                  {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                </Button>
              </div>
            ) : (
              <div className="flex flex-wrap items-center justify-between gap-3 rounded-md border border-border/70 bg-background/40 px-3 py-2 text-xs text-muted-foreground">
                <div>
                  Browsing is locked under <span className="font-mono text-foreground">{normalizedBrowseRoot || '/'}</span>.
                </div>
                <div className="flex items-center gap-2">
                  {normalizedBrowseRoot && state.currentPath !== normalizedBrowseRoot ? (
                    <Button type="button" variant="outline" size="sm" onClick={() => loadPath(normalizedBrowseRoot)}>
                      Back to root
                    </Button>
                  ) : null}
                  <Button type="button" variant="ghost" size="sm" onClick={() => loadPath(state.currentPath, { selectLoadedPath: false })} disabled={loading}>
                    {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                    Refresh
                  </Button>
                </div>
              </div>
            )}

            {state.roots.length > 0 && (
              <div className="flex flex-wrap gap-2">
                {state.roots.map((root) => (
                  <Button key={root} type="button" variant="outline" size="sm" onClick={() => loadPath(root)}>
                    {root}
                  </Button>
                ))}
              </div>
            )}

            <div className="rounded-md border border-border">
              <div className="flex items-center justify-between gap-2 px-3 py-2 border-b border-border">
                <div className="min-w-0 flex-1">
                  <div className="text-[11px] uppercase tracking-wide text-muted-foreground">Selected folder</div>
                  <Input value={candidatePath} readOnly className="mt-1 font-mono text-xs" />
                </div>
                <div className="flex items-center gap-2">
                  {state.parentPath && (
                    <Button type="button" variant="ghost" size="icon" onClick={() => loadPath(state.parentPath, { selectLoadedPath: false })}>
                      <ArrowUp className="w-4 h-4" />
                    </Button>
                  )}
                </div>
              </div>
              <div className="border-b border-border px-3 py-2 text-[11px] text-muted-foreground">
                Browsing inside <span className="font-mono text-foreground">{state.currentPath}</span>
              </div>

              <ScrollArea className="h-64">
                <div className="p-1">
                  {loading && (
                    <div className="flex items-center gap-2 text-sm text-muted-foreground px-2 py-2">
                      <Loader2 className="w-4 h-4 animate-spin" />
                      Loading folders…
                    </div>
                  )}
                  {!loading && error && (
                    <div className="text-sm text-destructive px-2 py-2">{error}</div>
                  )}
                  {!loading && !error && state.directories.length === 0 && (
                    <div className="text-sm text-muted-foreground px-2 py-2">No subfolders found.</div>
                  )}
                  {!loading && !error && state.directories.map((dir) => {
                    const selected = candidatePath === dir.path;
                    return (
                      <div
                        key={dir.path}
                        className={`flex items-center gap-2 rounded px-2 py-1 ${selected ? 'bg-primary/10 ring-1 ring-primary/30' : ''}`}
                      >
                        <button
                          type="button"
                          className="min-w-0 flex-1 rounded px-2 py-2 text-left hover:bg-muted"
                          onClick={() => loadPath(dir.path, { selectLoadedPath: true })}
                          title={`Open ${dir.path}`}
                        >
                          <span className="block truncate font-mono text-sm text-foreground">{dir.name}</span>
                          <span className="block text-[11px] text-muted-foreground">{dir.path}</span>
                        </button>
                        <Button
                          type="button"
                          size="sm"
                          variant={selected ? 'default' : 'outline'}
                          className="shrink-0"
                          onClick={() => void confirmSpecificPath(dir.path)}
                          disabled={confirming}
                        >
                          Select
                        </Button>
                      </div>
                    );
                  })}
                </div>
              </ScrollArea>
            </div>

            {state.truncated && (
              <p className="text-xs text-muted-foreground">
                Folder list truncated. Narrow down by navigating to a deeper path.
              </p>
            )}
            <p className="text-xs text-muted-foreground">
              Click a folder to open it. Use <span className="font-medium text-foreground">Select</span> to choose it immediately.
            </p>
            <p className="text-xs text-muted-foreground">
              Current folder is {state.writable ? 'writable' : 'read-only'} for PMDA.
            </p>
          </div>

          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button
              type="button"
              onClick={confirmSelection}
              disabled={confirming}
            >
              {confirming ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
              {candidatePath ? `Use ${candidatePath}` : 'Use this folder'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
