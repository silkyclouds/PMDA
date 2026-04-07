import { Plus, Trash2, Workflow } from 'lucide-react';

import type { PMDAConfig } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { FolderBrowserInput } from '@/components/settings/FolderBrowserInput';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';

function normalizeFolderPath(input: string): string {
  const raw = String(input || '').trim();
  if (!raw) return '';
  if (raw === '/') return '/';
  return raw.replace(/\/+$/, '') || raw;
}

function parsePathList(value: unknown): string[] {
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
      if (s.startsWith('[')) {
        try {
          const parsed = JSON.parse(s) as unknown;
          if (parsed !== item) {
            queue.push(parsed);
            continue;
          }
        } catch {
          // Fall through to CSV split.
        }
      }
      if (s.includes(',')) {
        const parts = s.split(',').map((p) => p.trim()).filter(Boolean);
        if (parts.length > 1) {
          queue.push(...parts);
          continue;
        }
      }
      const normalized = normalizeFolderPath(s);
      if (normalized && !seen.has(normalized)) {
        seen.add(normalized);
        out.push(normalized);
      }
      continue;
    }
    const normalized = normalizeFolderPath(String(item));
    if (normalized && !seen.has(normalized)) {
      seen.add(normalized);
      out.push(normalized);
    }
  }

  return out;
}

type WorkflowMode = NonNullable<PMDAConfig['LIBRARY_WORKFLOW_MODE']>;

const WORKFLOW_CARDS: Array<{
  value: WorkflowMode;
  label: string;
  description: string;
  scenario: string;
}> = [
  {
    value: 'managed',
    label: 'Managed library',
    description: 'Drop new music into intake folders and let PMDA build a clean serving library elsewhere.',
    scenario: 'I drop new albums into a holding area and want PMDA to build a clean library.',
  },
  {
    value: 'mirror',
    label: 'Mirror library',
    description: 'Scan an existing source library and materialize a cleaner serving tree elsewhere.',
    scenario: 'I already have a library and want PMDA to mirror a cleaner version.',
  },
  {
    value: 'inplace',
    label: 'Organize in place',
    description: 'Use the existing library folders directly, with optional intake folders on the side.',
    scenario: 'I want PMDA to organize my existing library where it already lives.',
  },
  {
    value: 'custom',
    label: 'Custom / Advanced',
    description: 'Keep using the low-level source/export controls directly.',
    scenario: 'I need raw control over roots, winner placement and export behavior.',
  },
];

type Props = {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
  onSwitchToCustom?: () => void;
};

function PathListEditor({
  label,
  description,
  paths,
  onChange,
  placeholder,
  selectLabel,
}: {
  label: string;
  description: string;
  paths: string[];
  onChange: (paths: string[]) => void;
  placeholder: string;
  selectLabel: string;
}) {
  const setPath = (index: number, value: string) => {
    const next = [...paths];
    next[index] = normalizeFolderPath(value);
    onChange(next.filter(Boolean));
  };

  const removePath = (index: number) => {
    onChange(paths.filter((_, i) => i !== index));
  };

  const addPath = () => {
    onChange([...paths, '']);
  };

  return (
    <div className="space-y-3 rounded-lg border border-border/60 bg-background/30 p-4">
      <div className="space-y-1">
        <Label>{label}</Label>
        <p className="text-xs text-muted-foreground">{description}</p>
      </div>
      <div className="space-y-3">
        {(paths.length > 0 ? paths : ['']).map((path, index) => (
          <div key={`${label}-${index}`} className="flex items-start gap-2">
            <div className="min-w-0 flex-1">
              <FolderBrowserInput
                value={path}
                onChange={(next) => setPath(index, next || '')}
                placeholder={placeholder}
                selectLabel={selectLabel}
              />
            </div>
            <Button
              type="button"
              size="icon"
              variant="outline"
              className="mt-1 h-10 w-10 shrink-0"
              onClick={() => removePath(index)}
              disabled={paths.length <= 1 && !path}
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          </div>
        ))}
      </div>
      <Button type="button" variant="outline" size="sm" className="gap-2" onClick={addPath}>
        <Plus className="h-4 w-4" />
        Add folder
      </Button>
    </div>
  );
}

export function LibraryWorkflowSettings({ config, updateConfig, onSwitchToCustom }: Props) {
  const mode = (config.LIBRARY_WORKFLOW_MODE || 'managed') as WorkflowMode;
  const intakeRoots = parsePathList(config.LIBRARY_INTAKE_ROOTS);
  const sourceRoots = parsePathList(config.LIBRARY_SOURCE_ROOTS);
  const visibleScopes = Array.isArray(config.LIBRARY_VISIBLE_SCOPES)
    ? config.LIBRARY_VISIBLE_SCOPES
    : parsePathList(config.LIBRARY_VISIBLE_SCOPES).map((value) => value.toLowerCase());
  const materialization = (config.LIBRARY_MATERIALIZATION_MODE || 'hardlink') as NonNullable<PMDAConfig['LIBRARY_MATERIALIZATION_MODE']>;
  const servingRoot = normalizeFolderPath(config.LIBRARY_SERVING_ROOT || config.EXPORT_ROOT || '');
  const dupesRoot = normalizeFolderPath(config.LIBRARY_DUPES_ROOT || config.DUPE_ROOT || '/dupes');
  const incompleteRoot = normalizeFolderPath(config.LIBRARY_INCOMPLETE_ROOT || config.INCOMPLETE_ALBUMS_TARGET_DIR || '/dupes/incomplete_albums');
  const includeFormat = Boolean(config.LIBRARY_INCLUDE_FORMAT_IN_FOLDER ?? config.EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER);
  const includeType = Boolean(config.LIBRARY_INCLUDE_TYPE_IN_FOLDER ?? config.EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER);
  const effectiveScanRoots = mode === 'managed'
    ? intakeRoots
    : Array.from(new Set([...sourceRoots, ...(mode === 'inplace' ? intakeRoots : [])]));
  const selectedMode = WORKFLOW_CARDS.find((card) => card.value === mode) || WORKFLOW_CARDS[0]!;

  const setPaths = (key: 'LIBRARY_INTAKE_ROOTS' | 'LIBRARY_SOURCE_ROOTS', next: string[]) => {
    updateConfig({ [key]: next.join(', ') } as Partial<PMDAConfig>);
  };

  const applyWorkflowSelection = (nextMode: WorkflowMode) => {
    const needsPublishedLibrary = nextMode === 'managed' || nextMode === 'mirror';
    updateConfig({
      LIBRARY_WORKFLOW_MODE: nextMode,
      PIPELINE_ENABLE_EXPORT: needsPublishedLibrary,
      EXPORT_LINK_STRATEGY: materialization,
      LIBRARY_MATERIALIZATION_MODE: materialization,
    });
  };

  const setMaterialization = (
    value: NonNullable<PMDAConfig['LIBRARY_MATERIALIZATION_MODE']>,
  ) => {
    updateConfig({
      LIBRARY_MATERIALIZATION_MODE: value,
      EXPORT_LINK_STRATEGY: value,
    });
  };

  return (
    <Card id="settings-library-workflow" className="scroll-mt-24 border-success/20 bg-success/[0.04]">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <span className="inline-flex h-8 w-8 items-center justify-center rounded-md bg-success/20 text-success">
            <Workflow className="h-4 w-4" />
          </span>
          Library workflow
        </CardTitle>
        <CardDescription>
          Choose how PMDA should interpret your folders: intake + clean library, mirrored library, or organize in place.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-3">
          {WORKFLOW_CARDS.map((card) => {
            const active = card.value === mode;
            return (
              <button
                key={card.value}
                type="button"
                onClick={() => applyWorkflowSelection(card.value)}
                className={`rounded-xl border p-4 text-left transition ${
                  active
                    ? 'border-primary/50 bg-primary/10 ring-1 ring-primary/20'
                    : 'border-border/60 bg-background/30 hover:border-border'
                }`}
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="text-sm font-medium text-foreground">{card.label}</div>
                  <Badge variant={active ? 'default' : 'outline'}>
                    {active ? 'Selected' : 'Available'}
                  </Badge>
                </div>
                <p className="mt-2 text-xs text-muted-foreground">{card.description}</p>
                <p className="mt-3 text-[11px] text-muted-foreground">
                  Scenario: <span className="text-foreground">{card.scenario}</span>
                </p>
              </button>
            );
          })}
        </div>

        {mode === 'managed' ? (
          <>
            <PathListEditor
              label="Intake folders"
              description="PMDA scans these folders for new albums, then promotes strict winners into the clean library."
              paths={intakeRoots}
              onChange={(next) => setPaths('LIBRARY_INTAKE_ROOTS', next)}
              placeholder="/music/incoming"
              selectLabel="Select intake folder"
            />
            <div className="rounded-lg border border-border/60 bg-background/30 p-4 space-y-2">
              <Label>Clean library folder</Label>
              <p className="text-xs text-muted-foreground">
                This is the serving library PMDA builds from matched winners.
              </p>
              <FolderBrowserInput
                value={servingRoot}
                onChange={(path) => updateConfig({ LIBRARY_SERVING_ROOT: path || '' })}
                placeholder="/music/library"
                selectLabel="Select clean library folder"
              />
            </div>
          </>
        ) : null}

        {mode === 'mirror' ? (
          <>
            <PathListEditor
              label="Source library folders"
              description="PMDA scans these folders, keeps the originals intact, and materializes a cleaner serving library elsewhere."
              paths={sourceRoots}
              onChange={(next) => setPaths('LIBRARY_SOURCE_ROOTS', next)}
              placeholder="/music/source_library"
              selectLabel="Select source library folder"
            />
            <div className="rounded-lg border border-border/60 bg-background/30 p-4 space-y-2">
              <Label>Clean library folder</Label>
              <p className="text-xs text-muted-foreground">
                The library view points here. Source folders remain visible through Inbox until albums are promoted.
              </p>
              <FolderBrowserInput
                value={servingRoot}
                onChange={(path) => updateConfig({ LIBRARY_SERVING_ROOT: path || '' })}
                placeholder="/music/library"
                selectLabel="Select clean library folder"
              />
            </div>
          </>
        ) : null}

        {mode === 'inplace' ? (
          <>
            <PathListEditor
              label="Library folders"
              description="These folders are both the scan source and the serving library. PMDA organizes them in place."
              paths={sourceRoots}
              onChange={(next) => setPaths('LIBRARY_SOURCE_ROOTS', next)}
              placeholder="/music/library"
              selectLabel="Select library folder"
            />
            <PathListEditor
              label="Optional intake folders"
              description="If you also want a holding area for new arrivals, PMDA will keep those albums in Inbox until they are promoted."
              paths={intakeRoots}
              onChange={(next) => setPaths('LIBRARY_INTAKE_ROOTS', next)}
              placeholder="/music/incoming"
              selectLabel="Select optional intake folder"
            />
          </>
        ) : null}

        {mode === 'custom' ? (
          <div className="rounded-xl border border-dashed border-border/70 bg-background/20 p-4 text-sm text-muted-foreground">
            Custom mode keeps the low-level source/export controls authoritative. Use the advanced section below to edit roots, winner placement, and export behavior directly.
          </div>
        ) : (
          <>
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-3">
              <div className="rounded-lg border border-border/60 bg-background/30 p-4 space-y-2">
                <Label>How PMDA should place albums into the library</Label>
                <Select
                  value={materialization}
                  onValueChange={setMaterialization}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select materialization mode" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="hardlink">Hardlink</SelectItem>
                    <SelectItem value="copy">Copy</SelectItem>
                    <SelectItem value="move">Move</SelectItem>
                    <SelectItem value="symlink">Symlink</SelectItem>
                  </SelectContent>
                </Select>
                <p className="text-[11px] text-muted-foreground">
                  This only changes how winners are materialized into Library. PMDA still decides winners, dupes, and incompletes.
                </p>
              </div>
              <div className="rounded-lg border border-border/60 bg-background/30 p-4 space-y-2">
                <Label>Duplicates folder</Label>
                <FolderBrowserInput
                  value={dupesRoot}
                  onChange={(path) => updateConfig({ LIBRARY_DUPES_ROOT: path || '/dupes' })}
                  placeholder="/dupes"
                  selectLabel="Select duplicates folder"
                />
              </div>
              <div className="rounded-lg border border-border/60 bg-background/30 p-4 space-y-2">
                <Label>Incomplete folder</Label>
                <FolderBrowserInput
                  value={incompleteRoot}
                  onChange={(path) => updateConfig({ LIBRARY_INCOMPLETE_ROOT: path || '/dupes/incomplete_albums' })}
                  placeholder="/dupes/incomplete_albums"
                  selectLabel="Select incomplete folder"
                />
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div className="flex items-center justify-between rounded-lg border border-border/40 bg-muted/10 px-3 py-2.5">
                <div className="space-y-0.5 pr-4">
                  <div className="text-sm font-medium">Include format in folder name</div>
                  <div className="text-[11px] text-muted-foreground">Example: Desert Solitaire (Flac)</div>
                </div>
                <Switch
                  checked={includeFormat}
                  onCheckedChange={(checked) => updateConfig({ LIBRARY_INCLUDE_FORMAT_IN_FOLDER: checked })}
                />
              </div>
              <div className="flex items-center justify-between rounded-lg border border-border/40 bg-muted/10 px-3 py-2.5">
                <div className="space-y-0.5 pr-4">
                  <div className="text-sm font-medium">Include album type in folder name</div>
                  <div className="text-[11px] text-muted-foreground">Example: Desert Solitaire (Flac,  Album)</div>
                </div>
                <Switch
                  checked={includeType}
                  onCheckedChange={(checked) => updateConfig({ LIBRARY_INCLUDE_TYPE_IN_FOLDER: checked })}
                />
              </div>
            </div>
          </>
        )}

        <div className="rounded-xl border border-border/60 bg-background/20 p-4 space-y-3">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <h3 className="text-sm font-semibold text-foreground">Effective behavior</h3>
              <p className="text-xs text-muted-foreground">
                PMDA translates the selected workflow into the low-level source/export settings used by the current pipeline.
              </p>
            </div>
            <Badge variant="secondary">{selectedMode.label}</Badge>
          </div>
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-2 text-sm">
            <div>
              <span className="text-muted-foreground">PMDA scans:</span>{' '}
              <span className="text-foreground">
                {effectiveScanRoots.length > 0 ? effectiveScanRoots.join(', ') : 'no folders configured yet'}
              </span>
            </div>
            <div>
              <span className="text-muted-foreground">PMDA builds library in:</span>{' '}
              <span className="text-foreground">
                {mode === 'inplace' ? (sourceRoots[0] || servingRoot || 'current library folders') : (servingRoot || 'not configured yet')}
              </span>
            </div>
            <div>
              <span className="text-muted-foreground">Unmatched albums stay in:</span>{' '}
              <span className="text-foreground">
                {visibleScopes.includes('inbox') ? 'Inbox' : 'Library (organized in place)'}
              </span>
            </div>
            <div>
              <span className="text-muted-foreground">Duplicate losers go to:</span>{' '}
              <span className="text-foreground">{dupesRoot || '/dupes'}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Incomplete albums go to:</span>{' '}
              <span className="text-foreground">{incompleteRoot || '/dupes/incomplete_albums'}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Materialization method:</span>{' '}
              <span className="text-foreground capitalize">{materialization}</span>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            {visibleScopes.map((scope) => (
              <Badge key={scope} variant="outline" className="capitalize">
                {scope === 'all' ? 'Consolidated' : scope}
              </Badge>
            ))}
          </div>
        </div>

        {mode !== 'custom' ? (
          <div className="flex justify-end">
            <Button type="button" variant="outline" onClick={() => {
              updateConfig({ LIBRARY_WORKFLOW_MODE: 'custom' });
              onSwitchToCustom?.();
            }}>
              Switch to Custom / Advanced
            </Button>
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
