import { useCallback, useEffect, useMemo, useState } from 'react';
import { ArrowUp, ChevronRight, FolderOpen, Loader2, RefreshCw } from 'lucide-react';
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

export function FolderBrowserInput({
  value,
  onChange,
  placeholder = '/music',
  selectLabel = 'Select folder',
}: FolderBrowserInputProps) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pathInput, setPathInput] = useState('');
  const [state, setState] = useState<BrowseState>({
    currentPath: normalizePath(value),
    parentPath: null,
    roots: [],
    directories: [],
    writable: false,
    truncated: false,
  });

  const selectedPath = useMemo(() => normalizePath(value), [value]);

  const loadPath = useCallback(async (pathToLoad?: string) => {
    setLoading(true);
    setError(null);
    try {
      const nextPath = normalizePath(pathToLoad ?? value);
      const result = await api.getFilesystemDirectories(nextPath);
      setState({
        currentPath: result.path,
        parentPath: result.parent,
        roots: Array.isArray(result.roots) ? result.roots : [],
        directories: Array.isArray(result.directories) ? result.directories : [],
        writable: Boolean(result.writable),
        truncated: Boolean(result.truncated),
      });
      setPathInput(result.path);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unable to browse this folder');
    } finally {
      setLoading(false);
    }
  }, [value]);

  useEffect(() => {
    if (!open) return;
    const initial = selectedPath || '/';
    setPathInput(initial);
    loadPath(initial);
  }, [open, selectedPath, loadPath]);

  return (
    <>
      <div className="flex gap-2">
        <Input
          value={selectedPath}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          className="font-mono"
        />
        <Button type="button" variant="outline" className="gap-2 shrink-0" onClick={() => setOpen(true)}>
          <FolderOpen className="w-4 h-4" />
          Browse
        </Button>
      </div>

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>{selectLabel}</DialogTitle>
            <DialogDescription>Select a folder from the container filesystem.</DialogDescription>
          </DialogHeader>

          <div className="space-y-3">
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
                placeholder="/"
                className="font-mono"
              />
              <Button type="button" variant="outline" onClick={() => loadPath(pathInput)}>
                Go
              </Button>
              <Button type="button" variant="ghost" onClick={() => loadPath(state.currentPath)} disabled={loading}>
                {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
              </Button>
            </div>

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
                <div className="text-xs text-muted-foreground truncate font-mono" title={state.currentPath}>
                  {state.currentPath}
                </div>
                <div className="flex items-center gap-2">
                  {state.parentPath && (
                    <Button type="button" variant="ghost" size="icon" onClick={() => loadPath(state.parentPath)}>
                      <ArrowUp className="w-4 h-4" />
                    </Button>
                  )}
                </div>
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
                  {!loading && !error && state.directories.map((dir) => (
                    <button
                      key={dir.path}
                      type="button"
                      className="w-full flex items-center justify-between gap-2 rounded px-2 py-1.5 hover:bg-muted text-left"
                      onClick={() => loadPath(dir.path)}
                    >
                      <span className="truncate font-mono text-sm">{dir.name}</span>
                      <ChevronRight className="w-4 h-4 text-muted-foreground shrink-0" />
                    </button>
                  ))}
                </div>
              </ScrollArea>
            </div>

            {state.truncated && (
              <p className="text-xs text-muted-foreground">
                Folder list truncated. Narrow down by navigating to a deeper path.
              </p>
            )}
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
              onClick={() => {
                onChange(state.currentPath);
                setOpen(false);
              }}
            >
              Use this folder
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
