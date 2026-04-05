import {
  ArrowRight,
  CheckCircle2,
  ChevronRight,
  Cpu,
  Database,
  FolderOutput,
  HardDrive,
  Library,
  Lock,
  RefreshCw,
  Sparkles,
  Workflow,
} from 'lucide-react';

import type { PMDAConfig, PlayerTarget, ScanPreflightResult } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Switch } from '@/components/ui/switch';
import { ProviderIcon } from '@/components/providers/ProviderIcon';
import { cn } from '@/lib/utils';

type Props = {
  config: Partial<PMDAConfig>;
  providersPreflight: ScanPreflightResult | null;
  providersChecking: boolean;
  providersPreflightAt: number | null;
  onRefreshProviders: () => void;
  onOpenGuidedSetup: () => void;
  onOpenSection: (sectionId: string) => void;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
};

type ProviderTone = 'ready' | 'warn' | 'off' | 'configured';

type ProviderCardDefinition = {
  id: 'musicbrainz' | 'discogs' | 'lastfm' | 'bandcamp' | 'acoustid';
  label: string;
  description: string;
  toggleKey: keyof PMDAConfig;
  defaultEnabled: boolean;
  sectionId: string;
  preflightKey?: keyof ScanPreflightResult;
  configured: (config: Partial<PMDAConfig>) => boolean;
};

type PipelineStageDefinition = {
  id: string;
  label: string;
  description: string;
  mandatory?: boolean;
  toggleKey?: keyof PMDAConfig;
};

type PlayerCardDefinition = {
  id: Exclude<PlayerTarget, 'none'>;
  label: string;
  description: string;
  configured: (config: Partial<PMDAConfig>) => boolean;
};

const PROVIDER_CARDS: ProviderCardDefinition[] = [
  {
    id: 'musicbrainz',
    label: 'MusicBrainz',
    description: 'Canonical release identities and artist relationships.',
    toggleKey: 'USE_MUSICBRAINZ',
    defaultEnabled: true,
    sectionId: 'settings-providers',
    preflightKey: 'musicbrainz',
    configured: (config) => Boolean(String(config.MUSICBRAINZ_EMAIL || '').trim()),
  },
  {
    id: 'discogs',
    label: 'Discogs',
    description: 'Physical variants, labels and release catalog detail.',
    toggleKey: 'USE_DISCOGS',
    defaultEnabled: true,
    sectionId: 'settings-providers',
    preflightKey: 'discogs',
    configured: (config) => Boolean(String(config.DISCOGS_USER_TOKEN || '').trim()),
  },
  {
    id: 'lastfm',
    label: 'Last.fm',
    description: 'Community signals, descriptions and scrobble context.',
    toggleKey: 'USE_LASTFM',
    defaultEnabled: true,
    sectionId: 'settings-providers',
    preflightKey: 'lastfm',
    configured: (config) => Boolean(String(config.LASTFM_API_KEY || '').trim() && String(config.LASTFM_API_SECRET || '').trim()),
  },
  {
    id: 'bandcamp',
    label: 'Bandcamp',
    description: 'Independent release verification and direct artist metadata.',
    toggleKey: 'USE_BANDCAMP',
    defaultEnabled: false,
    sectionId: 'settings-providers',
    preflightKey: 'bandcamp',
    configured: () => true,
  },
  {
    id: 'acoustid',
    label: 'AcoustID',
    description: 'Audio fingerprint matching when tags or structure are weak.',
    toggleKey: 'USE_ACOUSTID',
    defaultEnabled: true,
    sectionId: 'settings-providers',
    preflightKey: 'acoustid',
    configured: (config) => Boolean(String(config.ACOUSTID_API_KEY || '').trim()),
  },
];

const PIPELINE_STAGES: PipelineStageDefinition[] = [
  {
    id: 'discover',
    label: 'Discover',
    description: 'Read the configured folders and build the scan scope.',
    mandatory: true,
  },
  {
    id: 'match',
    label: 'Match & tags',
    description: 'Identity, metadata and artwork fixes.',
    mandatory: false,
    toggleKey: 'PIPELINE_ENABLE_MATCH_FIX',
  },
  {
    id: 'dedupe',
    label: 'Move dupes',
    description: 'Quarantine duplicate losers for review.',
    mandatory: false,
    toggleKey: 'PIPELINE_ENABLE_DEDUPE',
  },
  {
    id: 'incomplete',
    label: 'Incomplete',
    description: 'Move broken or missing-track albums aside.',
    mandatory: false,
    toggleKey: 'PIPELINE_ENABLE_INCOMPLETE_MOVE',
  },
  {
    id: 'publish',
    label: 'Publish library',
    description: 'Materialize the clean serving library.',
    mandatory: false,
    toggleKey: 'PIPELINE_ENABLE_EXPORT',
  },
  {
    id: 'player-sync',
    label: 'Player sync',
    description: 'Refresh Plex, Jellyfin or Navidrome after publish.',
    mandatory: false,
    toggleKey: 'PIPELINE_ENABLE_PLAYER_SYNC',
  },
];

const PLAYER_CARDS: PlayerCardDefinition[] = [
  {
    id: 'plex',
    label: 'Plex',
    description: 'Refresh a Plex library after PMDA publishes winners.',
    configured: (config) => Boolean(String(config.PLEX_HOST || '').trim() && String(config.PLEX_TOKEN || '').trim()),
  },
  {
    id: 'jellyfin',
    label: 'Jellyfin',
    description: 'Trigger a Jellyfin refresh after the pipeline completes.',
    configured: (config) => Boolean(String(config.JELLYFIN_URL || '').trim() && String(config.JELLYFIN_API_KEY || '').trim()),
  },
  {
    id: 'navidrome',
    label: 'Navidrome',
    description: 'Push PMDA publication into a Navidrome library refresh.',
    configured: (config) => Boolean(
      String(config.NAVIDROME_URL || '').trim()
        && (
          Boolean(String(config.NAVIDROME_API_KEY || '').trim())
          || Boolean(String(config.NAVIDROME_USERNAME || '').trim() && String(config.NAVIDROME_PASSWORD || '').trim())
        )
    ),
  },
];

function normalizeFolderPath(input: unknown): string {
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
      const trimmed = item.trim();
      if (!trimmed) continue;
      if (trimmed.startsWith('[') || trimmed.startsWith('"')) {
        try {
          const parsed = JSON.parse(trimmed) as unknown;
          if (parsed !== item) {
            queue.push(parsed);
            continue;
          }
        } catch {
          // Keep walking the plain value.
        }
      }
      if (trimmed.includes(',')) {
        const parts = trimmed.split(',').map((part) => part.trim()).filter(Boolean);
        if (parts.length > 1) {
          queue.push(...parts);
          continue;
        }
      }
      const normalized = normalizeFolderPath(trimmed);
      if (normalized && !seen.has(normalized)) {
        seen.add(normalized);
        out.push(normalized);
      }
      continue;
    }
    const normalized = normalizeFolderPath(item);
    if (normalized && !seen.has(normalized)) {
      seen.add(normalized);
      out.push(normalized);
    }
  }

  return out;
}

function compactPath(path: string, maxLen = 30): string {
  if (!path) return 'Not set';
  if (path.length <= maxLen) return path;
  const parts = path.split('/').filter(Boolean);
  if (parts.length <= 2) return path;
  return `…/${parts.slice(-2).join('/')}`;
}

function countEnabledProviders(config: Partial<PMDAConfig>): number {
  return PROVIDER_CARDS.reduce((count, provider) => {
    const raw = config[provider.toggleKey];
    const enabled = raw == null ? provider.defaultEnabled : Boolean(raw);
    return count + (enabled ? 1 : 0);
  }, 0);
}

function folderReadiness(config: Partial<PMDAConfig>) {
  const workflowMode = String(config.LIBRARY_WORKFLOW_MODE || 'managed').trim().toLowerCase();
  const intakeRoots = parsePathList(config.LIBRARY_INTAKE_ROOTS);
  const sourceRoots = parsePathList(config.LIBRARY_SOURCE_ROOTS || config.FILES_ROOTS);
  const servingRoot = normalizeFolderPath(config.LIBRARY_SERVING_ROOT || config.EXPORT_ROOT);
  const dupesRoot = normalizeFolderPath(config.LIBRARY_DUPES_ROOT || config.DUPE_ROOT);
  const incompleteRoot = normalizeFolderPath(config.LIBRARY_INCOMPLETE_ROOT || config.INCOMPLETE_ALBUMS_TARGET_DIR);

  const blockers: string[] = [];
  if (workflowMode === 'managed' && intakeRoots.length === 0) blockers.push('Add at least one intake folder.');
  if ((workflowMode === 'mirror' || workflowMode === 'inplace') && sourceRoots.length === 0) blockers.push('Add at least one source library folder.');
  if ((workflowMode === 'managed' || workflowMode === 'mirror') && !servingRoot) blockers.push('Choose the serving library folder.');
  if (!dupesRoot) blockers.push('Choose the duplicates folder.');
  if (!incompleteRoot) blockers.push('Choose the incomplete albums folder.');

  const sourceLabel = workflowMode === 'managed' ? 'Intake' : 'Source';
  const sourcePaths = workflowMode === 'managed' ? intakeRoots : sourceRoots;
  const visibleLibrary = normalizeFolderPath(config.LIBRARY_SERVING_ROOT || config.EXPORT_ROOT || sourceRoots[0] || intakeRoots[0] || '');

  return {
    ready: blockers.length === 0,
    blockers,
    workflowMode,
    sourceLabel,
    sourcePaths,
    servingRoot,
    dupesRoot,
    incompleteRoot,
    visibleLibrary,
  };
}

function providerToneClass(tone: ProviderTone): string {
  switch (tone) {
    case 'ready':
      return 'border-emerald-500/25 bg-emerald-500/10';
    case 'warn':
      return 'border-amber-500/30 bg-amber-500/10';
    case 'configured':
      return 'border-primary/25 bg-primary/[0.08]';
    default:
      return 'border-border/60 bg-background/40 opacity-70';
  }
}

function providerBadgeVariant(tone: ProviderTone): 'default' | 'secondary' | 'destructive' | 'outline' {
  switch (tone) {
    case 'ready':
      return 'default';
    case 'warn':
      return 'destructive';
    case 'configured':
      return 'secondary';
    default:
      return 'outline';
  }
}

function providerState(
  provider: ProviderCardDefinition,
  config: Partial<PMDAConfig>,
  providersPreflight: ScanPreflightResult | null,
): { enabled: boolean; tone: ProviderTone; label: string; detail: string } {
  const raw = config[provider.toggleKey];
  const enabled = raw == null ? provider.defaultEnabled : Boolean(raw);
  if (!enabled) {
    return { enabled, tone: 'off', label: 'Off', detail: 'Disabled for the pipeline.' };
  }
  if (!provider.configured(config)) {
    return { enabled, tone: 'warn', label: 'Needs setup', detail: 'Active, but still missing required credentials or identifiers.' };
  }
  if (providersPreflight && provider.preflightKey) {
    const result = providersPreflight[provider.preflightKey];
    if (result && typeof result === 'object' && 'ok' in result) {
      return result.ok
        ? { enabled, tone: 'ready', label: 'Ready', detail: result.message || 'Credential check passed.' }
        : { enabled, tone: 'warn', label: 'Issue', detail: result.message || 'Credential check failed.' };
    }
  }
  return { enabled, tone: 'configured', label: 'Configured', detail: 'Enabled and waiting for a provider check or first scan usage.' };
}

function stageEnabled(stage: PipelineStageDefinition, config: Partial<PMDAConfig>): boolean {
  if (stage.mandatory || !stage.toggleKey) return true;
  const raw = config[stage.toggleKey];
  return raw == null ? stage.id !== 'publish' && stage.id !== 'player-sync' : Boolean(raw);
}

function stageReady(stage: PipelineStageDefinition, config: Partial<PMDAConfig>, folders: ReturnType<typeof folderReadiness>): boolean {
  if (stage.id === 'publish') {
    return !stageEnabled(stage, config) || Boolean(folders.servingRoot);
  }
  if (stage.id === 'player-sync') {
    const target = String(config.PIPELINE_PLAYER_TARGET || 'none').trim().toLowerCase();
    return !stageEnabled(stage, config) || target !== 'none';
  }
  return true;
}

function playerSelected(config: Partial<PMDAConfig>, target: PlayerTarget): boolean {
  return Boolean(config.PIPELINE_ENABLE_PLAYER_SYNC) && String(config.PIPELINE_PLAYER_TARGET || 'none').trim().toLowerCase() === target;
}

function playerState(player: PlayerCardDefinition, config: Partial<PMDAConfig>) {
  const selected = playerSelected(config, player.id);
  const configured = player.configured(config);
  if (selected && configured) {
    return { selected, tone: 'ready' as ProviderTone, label: 'Ready', detail: 'Selected target and connection settings are present.' };
  }
  if (selected && !configured) {
    return { selected, tone: 'warn' as ProviderTone, label: 'Needs setup', detail: 'Selected for sync, but connection settings are incomplete.' };
  }
  if (!selected && configured) {
    return { selected, tone: 'configured' as ProviderTone, label: 'Configured', detail: 'Connection details are stored, but this player is not the active sync target.' };
  }
  return { selected, tone: 'off' as ProviderTone, label: 'Off', detail: 'Not selected for automatic sync.' };
}

function sectionHeadlineClass(tone: ProviderTone): string {
  if (tone === 'ready') return 'text-emerald-100';
  if (tone === 'warn') return 'text-amber-100';
  if (tone === 'configured') return 'text-foreground';
  return 'text-muted-foreground';
}

export function SettingsControlPlane({
  config,
  providersPreflight,
  providersChecking,
  providersPreflightAt,
  onRefreshProviders,
  onOpenGuidedSetup,
  onOpenSection,
  updateConfig,
}: Props) {
  const folders = folderReadiness(config);
  const activeProviderCount = countEnabledProviders(config);
  const enabledPipelineCount = PIPELINE_STAGES.filter((stage) => stageEnabled(stage, config)).length;
  const playerSyncTarget = String(config.PIPELINE_PLAYER_TARGET || 'none').trim().toLowerCase() as PlayerTarget;
  const selectedPlayersCount = playerSyncTarget !== 'none' && Boolean(config.PIPELINE_ENABLE_PLAYER_SYNC) ? 1 : 0;
  const playerSyncReady = selectedPlayersCount === 0 || PLAYER_CARDS.some((player) => player.id === playerSyncTarget && player.configured(config));
  const systemReady = folders.ready && activeProviderCount > 0 && playerSyncReady;
  const workflowLabel = folders.workflowMode === 'managed'
    ? 'Managed intake'
    : folders.workflowMode === 'mirror'
      ? 'Mirror library'
      : folders.workflowMode === 'inplace'
        ? 'In-place library'
        : 'Custom workflow';

  return (
    <Card className="border-primary/20 bg-[linear-gradient(135deg,hsl(var(--card))_0%,hsl(var(--card)/0.96)_55%,hsl(var(--primary)/0.06)_100%)] shadow-[0_24px_80px_rgba(15,23,42,0.18)]">
      <CardHeader className="space-y-4">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div className="space-y-2">
            <div className="inline-flex items-center gap-2 rounded-full border border-primary/20 bg-primary/[0.08] px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-primary">
              Setup control plane
            </div>
            <CardTitle className="text-2xl">Configure PMDA by following the actual flow.</CardTitle>
            <CardDescription className="max-w-3xl text-sm leading-6">
              Sources turn on what PMDA may trust, the pipeline shows what PMDA will do, the published library makes folder topology explicit, and players stay optional. Details still exist below, but the first setup should be readable from here.
            </CardDescription>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button type="button" className="gap-2" onClick={onOpenGuidedSetup}>
              <Sparkles className="h-4 w-4" />
              Guided setup
            </Button>
            <Button type="button" variant="outline" className="gap-2" onClick={onRefreshProviders} disabled={providersChecking}>
              {providersChecking ? <RefreshCw className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
              Check provider keys
            </Button>
          </div>
        </div>

        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <div className="rounded-2xl border border-border/70 bg-background/50 p-4">
            <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Sources</div>
            <div className="mt-2 flex items-center gap-2 text-sm font-semibold text-foreground">
              <Database className="h-4 w-4 text-primary" />
              {activeProviderCount} of {PROVIDER_CARDS.length} active
            </div>
            <p className="mt-1 text-xs text-muted-foreground">
              {providersPreflightAt ? `Last check ${new Date(providersPreflightAt).toLocaleTimeString()}` : 'No live credential check run yet.'}
            </p>
          </div>
          <div className="rounded-2xl border border-border/70 bg-background/50 p-4">
            <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Pipeline</div>
            <div className="mt-2 flex items-center gap-2 text-sm font-semibold text-foreground">
              <Workflow className="h-4 w-4 text-primary" />
              {enabledPipelineCount} of {PIPELINE_STAGES.length} stages enabled
            </div>
            <p className="mt-1 text-xs text-muted-foreground">{workflowLabel}</p>
          </div>
          <div className="rounded-2xl border border-border/70 bg-background/50 p-4">
            <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Published library</div>
            <div className="mt-2 flex items-center gap-2 text-sm font-semibold text-foreground">
              {folders.ready ? <CheckCircle2 className="h-4 w-4 text-emerald-500" /> : <FolderOutput className="h-4 w-4 text-amber-500" />}
              {folders.ready ? 'Topology ready' : 'Folders missing'}
            </div>
            <p className="mt-1 text-xs text-muted-foreground">{folders.blockers[0] || 'PMDA knows where to read, publish, and quarantine.'}</p>
          </div>
          <div className="rounded-2xl border border-border/70 bg-background/50 p-4">
            <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">System status</div>
            <div className="mt-2 flex items-center gap-2 text-sm font-semibold text-foreground">
              {systemReady ? <CheckCircle2 className="h-4 w-4 text-emerald-500" /> : <Sparkles className="h-4 w-4 text-amber-500" />}
              {systemReady ? 'Ready' : 'Needs setup'}
            </div>
            <p className="mt-1 text-xs text-muted-foreground">
              {systemReady ? 'A fresh install user can finish setup from this page without hunting for hidden fields.' : folders.blockers[0] || 'Finish folders or player sync before expecting a complete first-run flow.'}
            </p>
          </div>
        </div>
      </CardHeader>

      <CardContent className="space-y-5">
        <div className="grid gap-4 xl:grid-cols-[1.15fr_1fr_1fr_1fr]">
          <section className="rounded-[24px] border border-border/70 bg-background/45 p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Sources</div>
                <div className="mt-1 text-sm font-semibold text-foreground">Metadata providers</div>
              </div>
              <Button type="button" variant="ghost" size="sm" className="h-8 gap-1 px-2 text-xs" onClick={() => onOpenSection('settings-providers')}>
                Configure
                <ChevronRight className="h-3.5 w-3.5" />
              </Button>
            </div>
            <div className="mt-4 space-y-2.5">
              {PROVIDER_CARDS.map((provider) => {
                const state = providerState(provider, config, providersPreflight);
                return (
                  <div key={provider.id} className={cn('rounded-2xl border px-3 py-3 transition-colors', providerToneClass(state.tone))}>
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className={cn('flex items-center gap-2 text-sm font-semibold', sectionHeadlineClass(state.tone))}>
                          <ProviderIcon provider={provider.id} size={14} />
                          {provider.label}
                        </div>
                        <p className="mt-1 text-[11px] leading-5 text-muted-foreground">{provider.description}</p>
                        <p className="mt-2 text-[11px] leading-5 text-muted-foreground">{state.detail}</p>
                      </div>
                      <div className="flex shrink-0 flex-col items-end gap-2">
                        <Badge variant={providerBadgeVariant(state.tone)}>{state.label}</Badge>
                        <Switch
                          checked={state.enabled}
                          onCheckedChange={(checked) => updateConfig({ [provider.toggleKey]: checked } as Partial<PMDAConfig>)}
                          aria-label={`Toggle ${provider.label}`}
                        />
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </section>

          <section className="rounded-[24px] border border-border/70 bg-background/45 p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">PMDA pipeline</div>
                <div className="mt-1 text-sm font-semibold text-foreground">What the scan will actually do</div>
              </div>
              <Button type="button" variant="ghost" size="sm" className="h-8 gap-1 px-2 text-xs" onClick={() => onOpenSection('settings-pipeline')}>
                Configure
                <ChevronRight className="h-3.5 w-3.5" />
              </Button>
            </div>
            <div className="mt-4 space-y-2">
              {PIPELINE_STAGES.map((stage, index) => {
                const enabled = stageEnabled(stage, config);
                const ready = stageReady(stage, config, folders);
                const tone: ProviderTone = !enabled ? 'off' : ready ? 'ready' : 'warn';
                return (
                  <div key={stage.id} className="space-y-2">
                    <div className={cn('rounded-2xl border px-3 py-3', providerToneClass(tone))}>
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className={cn('flex items-center gap-2 text-sm font-semibold', sectionHeadlineClass(tone))}>
                            {stage.mandatory ? <Lock className="h-3.5 w-3.5" /> : <Workflow className="h-3.5 w-3.5" />}
                            {stage.label}
                          </div>
                          <p className="mt-1 text-[11px] leading-5 text-muted-foreground">{stage.description}</p>
                        </div>
                        {stage.mandatory || !stage.toggleKey ? (
                          <Badge variant="outline">Required</Badge>
                        ) : (
                          <Switch
                            checked={enabled}
                            onCheckedChange={(checked) => updateConfig({ [stage.toggleKey]: checked } as Partial<PMDAConfig>)}
                            aria-label={`Toggle ${stage.label}`}
                          />
                        )}
                      </div>
                    </div>
                    {index < PIPELINE_STAGES.length - 1 ? (
                      <div className="flex justify-center">
                        <ArrowRight className="h-3.5 w-3.5 rotate-90 text-muted-foreground/70" />
                      </div>
                    ) : null}
                  </div>
                );
              })}
            </div>
          </section>

          <section className="rounded-[24px] border border-border/70 bg-background/45 p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Published library</div>
                <div className="mt-1 text-sm font-semibold text-foreground">Folder topology</div>
              </div>
              <Button type="button" variant="ghost" size="sm" className="h-8 gap-1 px-2 text-xs" onClick={() => onOpenSection('settings-files-export')}>
                Configure
                <ChevronRight className="h-3.5 w-3.5" />
              </Button>
            </div>
            <div className="mt-4 space-y-2.5">
              <div className="rounded-2xl border border-border/60 bg-background/40 p-3">
                <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">{folders.sourceLabel}</div>
                <div className="mt-2 space-y-1.5">
                  {folders.sourcePaths.length > 0 ? folders.sourcePaths.slice(0, 2).map((path) => (
                    <div key={path} className="rounded-md border border-border/60 bg-background/60 px-2.5 py-2 font-mono text-[11px] text-foreground" title={path}>
                      {compactPath(path)}
                    </div>
                  )) : (
                    <div className="rounded-md border border-dashed border-border/60 px-2.5 py-2 text-[11px] text-muted-foreground">Not set yet</div>
                  )}
                </div>
              </div>
              <div className="flex justify-center">
                <ArrowRight className="h-3.5 w-3.5 rotate-90 text-muted-foreground/70" />
              </div>
              <div className="rounded-2xl border border-primary/20 bg-primary/[0.08] p-3">
                <div className="flex items-center gap-2 text-sm font-semibold text-foreground">
                  <Sparkles className="h-4 w-4 text-primary" />
                  PMDA
                </div>
                <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
                  {workflowLabel}. Reads from the source side, promotes winners, and diverts dupes and incompletes.
                </p>
              </div>
              <div className="flex justify-center">
                <ArrowRight className="h-3.5 w-3.5 rotate-90 text-muted-foreground/70" />
              </div>
              <div className="rounded-2xl border border-emerald-500/20 bg-emerald-500/10 p-3">
                <div className="flex items-center gap-2 text-sm font-semibold text-foreground">
                  <Library className="h-4 w-4 text-emerald-500" />
                  Visible library
                </div>
                <div className="mt-2 rounded-md border border-emerald-500/20 bg-background/60 px-2.5 py-2 font-mono text-[11px] text-foreground" title={folders.visibleLibrary || 'Not set'}>
                  {compactPath(folders.visibleLibrary)}
                </div>
              </div>
              <div className="grid gap-2 sm:grid-cols-2">
                <div className="rounded-2xl border border-amber-500/20 bg-amber-500/10 p-3">
                  <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-amber-100">Dupes</div>
                  <div className="mt-2 font-mono text-[11px] text-foreground" title={folders.dupesRoot || 'Not set'}>
                    {compactPath(folders.dupesRoot)}
                  </div>
                </div>
                <div className="rounded-2xl border border-amber-500/20 bg-amber-500/10 p-3">
                  <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-amber-100">Incompletes</div>
                  <div className="mt-2 font-mono text-[11px] text-foreground" title={folders.incompleteRoot || 'Not set'}>
                    {compactPath(folders.incompleteRoot)}
                  </div>
                </div>
              </div>
            </div>
          </section>

          <section className="rounded-[24px] border border-border/70 bg-background/45 p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Players</div>
                <div className="mt-1 text-sm font-semibold text-foreground">Optional destinations</div>
              </div>
              <Button type="button" variant="ghost" size="sm" className="h-8 gap-1 px-2 text-xs" onClick={() => onOpenSection('settings-pipeline')}>
                Configure
                <ChevronRight className="h-3.5 w-3.5" />
              </Button>
            </div>
            <div className="mt-4 space-y-2.5">
              {PLAYER_CARDS.map((player) => {
                const state = playerState(player, config);
                return (
                  <div key={player.id} className={cn('rounded-2xl border px-3 py-3', providerToneClass(state.tone))}>
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className={cn('flex items-center gap-2 text-sm font-semibold', sectionHeadlineClass(state.tone))}>
                          <HardDrive className="h-3.5 w-3.5" />
                          {player.label}
                        </div>
                        <p className="mt-1 text-[11px] leading-5 text-muted-foreground">{player.description}</p>
                        <p className="mt-2 text-[11px] leading-5 text-muted-foreground">{state.detail}</p>
                      </div>
                      <div className="flex shrink-0 flex-col items-end gap-2">
                        <Badge variant={providerBadgeVariant(state.tone)}>{state.label}</Badge>
                        <Switch
                          checked={state.selected}
                          onCheckedChange={(checked) => {
                            if (checked) {
                              updateConfig({ PIPELINE_PLAYER_TARGET: player.id, PIPELINE_ENABLE_PLAYER_SYNC: true });
                              return;
                            }
                            if (playerSelected(config, player.id)) {
                              updateConfig({ PIPELINE_PLAYER_TARGET: 'none', PIPELINE_ENABLE_PLAYER_SYNC: false });
                            }
                          }}
                          aria-label={`Toggle ${player.label} sync`}
                        />
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </section>
        </div>
      </CardContent>
    </Card>
  );
}
