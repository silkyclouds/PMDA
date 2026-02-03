import { useState, useEffect, useRef } from 'react';
import { Library, Loader2, RefreshCw, CheckCircle2, XCircle } from 'lucide-react';
import { Label } from '@/components/ui/label';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import { FieldTooltip } from '@/components/ui/field-tooltip';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import * as api from '@/lib/api';
import type { PMDAConfig } from '@/lib/api';

interface PlexLibrary {
  id: string;
  name: string;
  type: string;
}

interface LibrariesSettingsProps {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
  errors: Record<string, string>;
}

export function LibrariesSettings({ config, updateConfig, errors }: LibrariesSettingsProps) {
  const [loading, setLoading] = useState(false);
  const [libraries, setLibraries] = useState<PlexLibrary[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [hasFetched, setHasFetched] = useState(false);
  const prevPlexRef = useRef({ host: config.PLEX_HOST ?? '', token: config.PLEX_TOKEN ?? '' });

  // Initialize selected IDs from config (SECTION_IDS may be string or array from API)
  useEffect(() => {
    const raw = config.SECTION_IDS;
    if (raw != null && raw !== '') {
      const ids = Array.isArray(raw)
        ? raw.map((x) => String(x).trim()).filter(Boolean)
        : String(raw).split(',').map(id => id.trim()).filter(Boolean);
      setSelectedIds(new Set(ids));
    }
  }, [config.SECTION_IDS]);

  // When Plex server (host or token) changes, refresh libraries list
  useEffect(() => {
    const host = config.PLEX_HOST ?? '';
    const token = config.PLEX_TOKEN ?? '';
    if (host !== prevPlexRef.current.host || token !== prevPlexRef.current.token) {
      prevPlexRef.current = { host, token };
      setHasFetched(false);
      setLibraries([]);
      setFetchError(null);
    }
  }, [config.PLEX_HOST, config.PLEX_TOKEN]);

  // Auto-fetch libraries when we have Plex credentials and haven't fetched (or server changed)
  useEffect(() => {
    if (config.PLEX_HOST && config.PLEX_TOKEN && !hasFetched) {
      fetchLibraries();
    }
  }, [config.PLEX_HOST, config.PLEX_TOKEN, hasFetched]);

  const fetchLibraries = async () => {
    setLoading(true);
    setFetchError(null);
    try {
      const result = await api.autodetectLibraries(config.PLEX_HOST!, config.PLEX_TOKEN!);
      const libList = result.libraries && Array.isArray(result.libraries) ? result.libraries : [];
      if (result.success && libList.length > 0) {
        const libs: PlexLibrary[] = libList.map((lib: { id: string; name: string; type?: string }) => ({
          id: lib.id,
          name: lib.name,
          type: lib.type || 'artist',
        }));
        setLibraries(libs);
        setHasFetched(true);

        // Auto-select all music libraries only when user has never set SECTION_IDS (avoid overwriting saved choice)
        const hasSavedSectionIds = config.SECTION_IDS != null && config.SECTION_IDS !== '' &&
          (Array.isArray(config.SECTION_IDS) ? config.SECTION_IDS.length > 0 : String(config.SECTION_IDS).trim() !== '');
        if (!hasSavedSectionIds && selectedIds.size === 0) {
          const musicLibs = libs.filter(l => l.type === 'artist' || l.type === 'music');
          const newSelection = new Set(musicLibs.map(l => l.id));
          const sectionIdsStr = Array.from(newSelection).join(',');
          setSelectedIds(newSelection);
          updateConfig({ SECTION_IDS: sectionIdsStr });
          api.saveConfig({ SECTION_IDS: sectionIdsStr }).catch(() => {});
        }
      } else {
        setFetchError(result.message || 'No libraries found');
      }
    } catch (error) {
      setFetchError('Failed to fetch libraries');
    } finally {
      setLoading(false);
    }
  };

  const persistSectionIds = (sectionIds: string) => {
    updateConfig({ SECTION_IDS: sectionIds });
    // Persist immediately so scan and APIs use current selection (no need to click Save)
    api.saveConfig({ SECTION_IDS: sectionIds }).catch(() => {
      // Non-blocking; user can still Save Configuration later
    });
  };

  const toggleLibrary = (id: string) => {
    const newSelection = new Set(selectedIds);
    if (newSelection.has(id)) {
      newSelection.delete(id);
    } else {
      newSelection.add(id);
    }
    setSelectedIds(newSelection);
    persistSectionIds(Array.from(newSelection).join(','));
  };

  const selectAllMusic = () => {
    const musicLibs = libraries.filter(l => l.type === 'artist' || l.type === 'music');
    const newSelection = new Set(musicLibs.map(l => l.id));
    setSelectedIds(newSelection);
    persistSectionIds(Array.from(newSelection).join(','));
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3 mb-4">
        <div className="p-2 rounded-lg bg-primary/10">
          <Library className="w-5 h-5 text-primary" />
        </div>
        <div>
          <h3 className="font-medium">Library Selection</h3>
          <p className="text-sm text-muted-foreground">
            Select which Plex libraries to scan for duplicates
          </p>
        </div>
      </div>

      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-1.5">
            <Label>Available Libraries</Label>
            <FieldTooltip content="Select the music libraries you want PMDA to scan. Only selected libraries will be checked for duplicates." />
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={fetchLibraries}
            disabled={loading}
            className="gap-1.5"
          >
            {loading ? (
              <Loader2 className="w-3 h-3 animate-spin" />
            ) : (
              <RefreshCw className="w-3 h-3" />
            )}
            Refresh
          </Button>
        </div>

        {loading && libraries.length === 0 && (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="w-6 h-6 animate-spin text-primary" />
          </div>
        )}

        {fetchError && (
          <div className="p-3 rounded-lg bg-destructive/10 flex items-start gap-2">
            <XCircle className="w-5 h-5 text-destructive flex-shrink-0" />
            <p className="text-sm text-destructive">{fetchError}</p>
          </div>
        )}

        {libraries.length > 0 && (
          <>
            <div className="rounded-lg border border-border overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-12">Include</TableHead>
                    <TableHead>Name</TableHead>
                    <TableHead>ID</TableHead>
                    <TableHead>Type</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {Array.isArray(libraries) && libraries.map((lib) => (
                    <TableRow 
                      key={lib.id}
                      className={selectedIds.has(lib.id) ? 'bg-primary/5' : ''}
                    >
                      <TableCell>
                        <Checkbox
                          checked={selectedIds.has(lib.id)}
                          onCheckedChange={() => toggleLibrary(lib.id)}
                        />
                      </TableCell>
                      <TableCell className="font-medium">{lib.name}</TableCell>
                      <TableCell className="text-muted-foreground">{lib.id}</TableCell>
                      <TableCell>
                        <span className={`text-xs px-2 py-0.5 rounded-full ${
                          lib.type === 'artist' || lib.type === 'music'
                            ? 'bg-primary/10 text-primary'
                            : 'bg-muted text-muted-foreground'
                        }`}>
                          {lib.type}
                        </span>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>

            <div className="flex items-center justify-between">
              <p className="text-sm text-muted-foreground">
                {selectedIds.size} of {libraries.length} libraries selected
              </p>
              <Button
                variant="ghost"
                size="sm"
                onClick={selectAllMusic}
                className="text-xs"
              >
                Select all music libraries
              </Button>
            </div>
          </>
        )}

        {!loading && libraries.length === 0 && !fetchError && (
          <div className="text-center py-8 text-muted-foreground">
            <Library className="w-12 h-12 mx-auto mb-3 opacity-50" />
            <p>Click Refresh to fetch libraries from Plex</p>
          </div>
        )}

        {selectedIds.size > 0 && (
          <div className="p-3 rounded-lg bg-success/10 flex items-start gap-2">
            <CheckCircle2 className="w-5 h-5 text-success flex-shrink-0" />
            <div>
              <p className="text-sm font-medium text-success">
                Libraries selected: {Array.from(selectedIds).join(', ')}
              </p>
              <p className="text-xs text-muted-foreground mt-0.5">
                These libraries will be scanned for duplicates
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
