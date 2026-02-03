import { useState, useCallback } from 'react';
import { ScanLine, ChevronDown, GripVertical, X, Plus } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { Button } from '@/components/ui/button';
import { FieldTooltip } from '@/components/ui/field-tooltip';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { cn } from '@/lib/utils';
import type { PMDAConfig } from '@/lib/api';

const KNOWN_FORMATS = ['dsf', 'aif', 'aiff', 'wav', 'flac', 'm4a', 'mp4', 'm4b', 'm4p', 'aifc', 'ogg', 'opus', 'mp3', 'wma', 'aac', 'ape', 'alac'];

/** Normalize FORMAT_PREFERENCE from API (may be string from DB or array). */
function normalizeFormatPreference(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw.every((x) => typeof x === 'string') ? raw as string[] : KNOWN_FORMATS;
  }
  if (typeof raw === 'string') {
    try {
      const parsed = JSON.parse(raw) as unknown;
      return Array.isArray(parsed) ? parsed.filter((x): x is string => typeof x === 'string') : KNOWN_FORMATS;
    } catch {
      return raw ? raw.split(',').map((s) => s.trim()).filter(Boolean) : KNOWN_FORMATS;
    }
  }
  return KNOWN_FORMATS;
}

interface ScanSettingsProps {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
  errors: Record<string, string>;
}

export function ScanSettings({ config, updateConfig, errors }: ScanSettingsProps) {
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [newFormat, setNewFormat] = useState('');
  const [dragOverIndex, setDragOverIndex] = useState<number | null>(null);

  const formats: string[] = normalizeFormatPreference(config.FORMAT_PREFERENCE);

  const setFormats = useCallback(
    (next: string[]) => {
      updateConfig({ FORMAT_PREFERENCE: next });
    },
    [updateConfig]
  );

  const handleDragStart = (e: React.DragEvent, index: number) => {
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setData('text/plain', String(index));
    e.dataTransfer.setData('application/json', JSON.stringify({ index }));
  };

  const handleDragOver = (e: React.DragEvent, index: number) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
    setDragOverIndex(index);
  };

  const handleDragLeave = () => {
    setDragOverIndex(null);
  };

  const handleDrop = (e: React.DragEvent, dropIndex: number) => {
    e.preventDefault();
    setDragOverIndex(null);
    const raw = e.dataTransfer.getData('text/plain');
    const dragIndex = parseInt(raw, 10);
    if (Number.isNaN(dragIndex) || dragIndex === dropIndex) return;
    const next = [...formats];
    const [removed] = next.splice(dragIndex, 1);
    next.splice(dropIndex, 0, removed);
    setFormats(next);
  };

  const handleDragEnd = () => {
    setDragOverIndex(null);
  };

  const removeFormat = (index: number) => {
    const next = formats.filter((_, i) => i !== index);
    setFormats(next);
  };

  const addFormat = () => {
    const codec = newFormat.trim().toLowerCase().replace(/^\./, '');
    if (!codec || formats.includes(codec)) {
      setNewFormat('');
      return;
    }
    if (!/^[a-z0-9]{1,8}$/i.test(codec)) return;
    setFormats([...formats, codec]);
    setNewFormat('');
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3 mb-4">
        <div className="p-2 rounded-lg bg-primary/10">
          <ScanLine className="w-5 h-5 text-primary" />
        </div>
        <div>
          <h3 className="font-medium">Scan Settings</h3>
          <p className="text-sm text-muted-foreground">
            Configure how PMDA scans your library
          </p>
        </div>
      </div>

      <div className="space-y-4">
        <div className="flex items-center justify-between p-4 rounded-lg bg-muted/50">
          <div className="space-y-0.5">
            <div className="flex items-center gap-1.5">
              <Label>Cross-Library Dedupe</Label>
              <FieldTooltip content="When enabled, PMDA detects duplicates across all configured libraries. When disabled, it only looks within each library separately." />
            </div>
            <p className="text-xs text-muted-foreground">
              Detect duplicates across all libraries
            </p>
          </div>
          <Switch
            checked={config.CROSS_LIBRARY_DEDUPE ?? false}
            onCheckedChange={(checked) => updateConfig({ CROSS_LIBRARY_DEDUPE: checked })}
          />
        </div>

        <div className="flex items-center justify-between p-4 rounded-lg bg-muted/50">
          <div className="space-y-0.5">
            <div className="flex items-center gap-1.5">
              <Label>Normalize parenthetical suffixes for duplicate detection</Label>
              <FieldTooltip content="When enabled, album titles like 'Lemodie (Flac)' and 'Lemodie' are treated as the same album for duplicate detection. Formats and versions in parentheses (e.g. (flac), (mp3), (EP)) are ignored when grouping. When disabled, such titles are treated as different albums." />
            </div>
            <p className="text-xs text-muted-foreground">
              Treat &quot;Album (flac)&quot; and &quot;Album&quot; as the same when detecting duplicates
            </p>
          </div>
          <Switch
            checked={config.NORMALIZE_PARENTHETICAL_FOR_DEDUPE ?? true}
            onCheckedChange={(checked) => updateConfig({ NORMALIZE_PARENTHETICAL_FOR_DEDUPE: checked })}
          />
        </div>

        <div className="flex items-start justify-between p-4 rounded-lg bg-muted/50">
          <div className="space-y-0.5 flex-1">
            <div className="flex items-center gap-1.5">
              <Label>Automatically move dupes to the dupe folder</Label>
              <FieldTooltip content="If enabled, duplicate albums will be automatically moved to the dupe folder after each scan. Files remain recoverable via the scan history if not manually deleted." />
            </div>
            <p className="text-xs text-muted-foreground">
              Automatically move duplicate albums to the dupe folder after each scan. Files remain recoverable via the scan history if not manually deleted.
            </p>
          </div>
          <Switch
            checked={config.AUTO_MOVE_DUPES ?? false}
            onCheckedChange={(checked) => updateConfig({ AUTO_MOVE_DUPES: checked })}
            className="mt-1"
          />
        </div>

        <div className="space-y-2">
          <div className="flex items-center gap-1.5">
            <Label>Format Preference</Label>
            <FieldTooltip content="Preferred audio formats in priority order (highest first). Drag to reorder. PMDA will prefer keeping albums in these formats when choosing the 'best' version." />
          </div>
          <p className="text-xs text-muted-foreground">
            Drag codecs to reorder. Add a format below (e.g. opus, aac).
          </p>
          <div
            className="flex flex-wrap gap-2 rounded-lg border border-border bg-muted/30 p-3 min-h-[52px]"
            onDragLeave={handleDragLeave}
          >
            {formats.map((fmt, index) => (
              <div
                key={`${fmt}-${index}`}
                draggable
                onDragStart={(e) => handleDragStart(e, index)}
                onDragOver={(e) => handleDragOver(e, index)}
                onDrop={(e) => handleDrop(e, index)}
                onDragEnd={handleDragEnd}
                className={cn(
                  'flex items-center gap-1 rounded-md border bg-background px-2 py-1.5 text-sm font-medium cursor-grab active:cursor-grabbing',
                  dragOverIndex === index ? 'border-primary ring-2 ring-primary/20' : 'border-border'
                )}
              >
                <GripVertical className="w-3.5 h-3.5 text-muted-foreground shrink-0" aria-hidden />
                <span className="font-mono uppercase">{fmt}</span>
                <button
                  type="button"
                  onClick={() => removeFormat(index)}
                  className="ml-0.5 rounded p-0.5 hover:bg-muted text-muted-foreground hover:text-foreground"
                  aria-label={`Remove ${fmt}`}
                >
                  <X className="w-3 h-3" />
                </button>
              </div>
            ))}
          </div>
          <div className="flex gap-2">
            <Input
              placeholder="Add codec (e.g. opus, aac)"
              value={newFormat}
              onChange={(e) => setNewFormat(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && (e.preventDefault(), addFormat())}
              className="max-w-[220px] font-mono"
            />
            <Button type="button" variant="outline" size="sm" onClick={addFormat} className="gap-1">
              <Plus className="w-3 h-3" />
              Add
            </Button>
          </div>
        </div>

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
                <Label htmlFor="scan-threads">Scan Threads</Label>
                <FieldTooltip content="Number of parallel threads for scanning. Use 'auto' to let PMDA decide based on your system, or specify a number (e.g. 4)." />
              </div>
              <Input
                id="scan-threads"
                placeholder="auto"
                value={config.SCAN_THREADS?.toString() || 'auto'}
                onChange={(e) => {
                  const val = e.target.value;
                  updateConfig({
                    SCAN_THREADS: val === '' || val === 'auto' ? 'auto' : parseInt(val) || 'auto',
                  });
                }}
              />
            </div>

            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="skip-folders">Skip Folders</Label>
                <FieldTooltip content="Comma-separated path prefixes to skip during scan (e.g. /music/samples,/music/temp). Albums in these folders will be ignored." />
              </div>
              <Input
                id="skip-folders"
                placeholder="/music/samples,/music/temp"
                value={config.SKIP_FOLDERS || ''}
                onChange={(e) => updateConfig({ SKIP_FOLDERS: e.target.value })}
              />
            </div>

            <div className="pt-2 border-t border-border space-y-4">
              <h5 className="text-sm font-medium">Broken Album Detection</h5>
              
              <div className="space-y-2">
                <div className="flex items-center gap-1.5">
                  <Label htmlFor="broken-consecutive">Consecutive Missing Tracks Threshold</Label>
                  <FieldTooltip content="An album is considered broken if it has more than this number of consecutive missing tracks (e.g. tracks 1, 2, 3, 4 missing). Default: 3" />
                </div>
                <Input
                  id="broken-consecutive"
                  type="number"
                  min="1"
                  max="20"
                  placeholder="3"
                  value={config.BROKEN_ALBUM_CONSECUTIVE_THRESHOLD?.toString() || '3'}
                  onChange={(e) => {
                    const val = parseInt(e.target.value) || 3;
                    updateConfig({ BROKEN_ALBUM_CONSECUTIVE_THRESHOLD: val });
                  }}
                />
                <p className="text-xs text-muted-foreground">
                  Number of consecutive missing tracks to consider an album broken
                </p>
              </div>

              <div className="space-y-2">
                <div className="flex items-center gap-1.5">
                  <Label htmlFor="broken-percentage">Missing Tracks Percentage Threshold</Label>
                  <FieldTooltip content="An album is considered broken if more than this percentage of tracks are missing (e.g. 20% = 0.20). Default: 0.20 (20%)" />
                </div>
                <Input
                  id="broken-percentage"
                  type="number"
                  min="0.01"
                  max="1"
                  step="0.01"
                  placeholder="0.20"
                  value={config.BROKEN_ALBUM_PERCENTAGE_THRESHOLD?.toString() || '0.20'}
                  onChange={(e) => {
                    const val = parseFloat(e.target.value) || 0.20;
                    updateConfig({ BROKEN_ALBUM_PERCENTAGE_THRESHOLD: val });
                  }}
                />
                <p className="text-xs text-muted-foreground">
                  Percentage of missing tracks (0.01 to 1.0) to consider an album broken
                </p>
              </div>
            </div>

            <div className="pt-2 border-t border-border space-y-2">
              <div className="flex items-center justify-between gap-4">
                <div className="flex items-center gap-1.5">
                  <Label htmlFor="pmda-default-mode">Default mode</Label>
                  <FieldTooltip content="Default run mode for the container. Usually 'serve' to run the web UI. Leave as serve unless you know otherwise." />
                </div>
                <Input
                  id="pmda-default-mode"
                  placeholder="serve"
                  value={config.PMDA_DEFAULT_MODE ?? 'serve'}
                  onChange={(e) => updateConfig({ PMDA_DEFAULT_MODE: e.target.value || 'serve' })}
                />
              </div>
            </div>
          </CollapsibleContent>
        </Collapsible>
      </div>
    </div>
  );
}
