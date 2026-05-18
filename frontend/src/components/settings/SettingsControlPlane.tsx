import {
  CheckCircle2,
  ChevronRight,
  Database,
  HardDrive,
  Library,
  RefreshCw,
  Sparkles,
  Workflow,
} from 'lucide-react';

import type { PMDAConfig, PlayerTarget, ScanPreflightResult } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { ProviderIcon } from '@/components/providers/ProviderIcon';

type Props = {
  config: Partial<PMDAConfig>;
  providersPreflight: ScanPreflightResult | null;
  providersChecking: boolean;
  providersPreflightAt: number | null;
  onRefreshProviders: () => void;
  onOpenSection: (sectionId: 'workflow' | 'sources' | 'pipeline' | 'published-library' | 'destinations') => void;
};

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
  return [
    config.USE_MUSICBRAINZ !== false,
    config.USE_DISCOGS !== false,
    config.USE_ITUNES !== false,
    config.USE_DEEZER !== false,
    config.USE_SPOTIFY !== false,
    config.USE_QOBUZ !== false,
    config.USE_TIDAL !== false,
    config.USE_LASTFM !== false,
    Boolean(config.USE_BANDCAMP ?? false),
    config.USE_ACOUSTID !== false,
    Boolean(String(config.FANART_API_KEY || '').trim()),
    Boolean(String(config.THEAUDIODB_API_KEY || '').trim()),
    Boolean((config.USE_WEB_SEARCH_FOR_MB ?? true) && String(config.SERPER_API_KEY || '').trim()),
  ].filter(Boolean).length;
}

function workflowLabel(config: Partial<PMDAConfig>): string {
  const mode = String(config.LIBRARY_WORKFLOW_MODE || 'managed').trim().toLowerCase();
  if (mode === 'mirror') return 'Mirror library';
  if (mode === 'inplace') return 'In-place library';
  if (mode === 'audit') return 'Audit / read-only';
  if (mode === 'custom') return 'Custom workflow';
  return 'Managed intake';
}

function currentPlayerTarget(config: Partial<PMDAConfig>): PlayerTarget {
  if (!Boolean(config.PIPELINE_ENABLE_PLAYER_SYNC)) return 'none';
  const target = String(config.PIPELINE_PLAYER_TARGET || 'none').trim().toLowerCase();
  return ['plex', 'jellyfin', 'navidrome'].includes(target) ? (target as PlayerTarget) : 'none';
}

export function SettingsControlPlane({
  config,
  providersPreflight,
  providersChecking,
  providersPreflightAt,
  onRefreshProviders,
  onOpenSection,
}: Props) {
  const scanRoots = parsePathList(config.LIBRARY_EFFECTIVE_SCAN_ROOTS || config.LIBRARY_INTAKE_ROOTS || config.LIBRARY_SOURCE_ROOTS || config.FILES_ROOTS);
  const servingRoot = normalizeFolderPath(config.LIBRARY_SERVING_ROOT || config.EXPORT_ROOT);
  const materialization = String(config.LIBRARY_MATERIALIZATION_MODE || config.EXPORT_LINK_STRATEGY || 'hardlink').trim().toLowerCase();
  const playerTarget = currentPlayerTarget(config);
  const activeProviderCount = countEnabledProviders(config);
  const schedulerPaused = config.SCHEDULER_PAUSED !== false;
  const allowNonScanJobs = Boolean(config.SCHEDULER_ALLOW_NON_SCAN_JOBS ?? false);
  const postScanAsync = Boolean(config.PIPELINE_POST_SCAN_ASYNC ?? false);
  const scanFirstModeEnabled = schedulerPaused && !allowNonScanJobs && !postScanAsync;
  const keyIssues = providersPreflight
    ? [
        { active: config.USE_DISCOGS !== false && Boolean(String(config.DISCOGS_USER_TOKEN || '').trim()), result: providersPreflight.discogs },
        { active: config.USE_LASTFM !== false && Boolean(String(config.LASTFM_API_KEY || '').trim() && String(config.LASTFM_API_SECRET || '').trim()), result: providersPreflight.lastfm },
        { active: config.USE_ACOUSTID !== false && Boolean(String(config.ACOUSTID_API_KEY || '').trim()), result: providersPreflight.acoustid },
        { active: Boolean(String(config.FANART_API_KEY || '').trim()), result: providersPreflight.fanart },
        { active: Boolean(String(config.THEAUDIODB_API_KEY || '').trim()), result: providersPreflight.audiodb },
        { active: Boolean((config.USE_WEB_SEARCH_FOR_MB ?? true) && String(config.SERPER_API_KEY || '').trim()), result: providersPreflight.serper },
        { active: Boolean(String(config.MUSICBRAINZ_EMAIL || '').trim()), result: providersPreflight.musicbrainz },
      ].filter(({ active, result }) => active && result && result.ok === false).length
    : 0;

  const cards: Array<{
    id: 'workflow' | 'sources' | 'pipeline' | 'published-library' | 'destinations';
    eyebrow: string;
    title: string;
    description: string;
    detail: string;
    icon: typeof Workflow;
    badges?: Array<{ label: string; variant?: 'default' | 'secondary' | 'outline' | 'destructive' }>;
  }> = [
    {
      id: 'workflow',
      eyebrow: '1. Library workflow',
      title: workflowLabel(config),
      description: scanRoots.length > 0 ? `${scanRoots.length} source folder${scanRoots.length === 1 ? '' : 's'} configured` : 'No scan roots configured yet',
      detail: scanRoots.length > 0 ? scanRoots.slice(0, 2).map((path) => compactPath(path)).join(' · ') : 'Choose managed, mirror, in-place or audit, then point PMDA at the right folders.',
      icon: Workflow,
      badges: [{ label: String(config.LIBRARY_WORKFLOW_MODE || 'managed'), variant: 'secondary' }],
    },
    {
      id: 'sources',
      eyebrow: '2. Sources',
      title: `${activeProviderCount} providers active`,
      description: providersPreflightAt ? `Last validation ${new Date(providersPreflightAt).toLocaleTimeString()}` : 'No live validation run yet',
      detail: keyIssues > 0 ? `${keyIssues} provider credential checks still need attention.` : 'Add provider keys inline and validate them from the modal.',
      icon: Database,
      badges: [
        { label: config.USE_DISCOGS !== false ? 'Discogs' : 'Discogs off', variant: config.USE_DISCOGS !== false ? 'outline' : 'secondary' },
        { label: config.USE_LASTFM !== false ? 'Last.fm' : 'Last.fm off', variant: config.USE_LASTFM !== false ? 'outline' : 'secondary' },
        { label: config.USE_ACOUSTID !== false ? 'AcoustID' : 'AcoustID off', variant: config.USE_ACOUSTID !== false ? 'outline' : 'secondary' },
        ...(String(config.FANART_API_KEY || '').trim() ? [{ label: 'Fanart.tv', variant: 'outline' as const }] : []),
        ...(String(config.THEAUDIODB_API_KEY || '').trim() ? [{ label: 'TheAudioDB', variant: 'outline' as const }] : []),
      ],
    },
    {
      id: 'pipeline',
      eyebrow: '3. Pipeline',
      title: scanFirstModeEnabled ? 'Scan-first pipeline' : 'Flexible pipeline',
      description: String(config.FILES_TAG_WRITE_MODE || 'full').trim() === 'pmda_id_only' ? 'DB-only write mode' : 'Write tags into files',
      detail: `Dupes: ${!config.PIPELINE_ENABLE_DEDUPE ? 'ignore' : config.AUTO_MOVE_DUPES ? 'move losers' : 'review only'} · Incompletes: ${!config.PIPELINE_ENABLE_INCOMPLETE_MOVE ? 'keep' : 'quarantine'} · Post-scan: ${postScanAsync ? 'async' : 'inline'}`,
      icon: Sparkles,
      badges: [
        { label: schedulerPaused ? 'Schedule paused' : 'Scheduled scans', variant: schedulerPaused ? 'secondary' : 'outline' },
        { label: allowNonScanJobs ? 'Background jobs on' : 'Background jobs off', variant: allowNonScanJobs ? 'outline' : 'secondary' },
      ],
    },
    {
      id: 'published-library',
      eyebrow: '4. Published library',
      title: servingRoot ? compactPath(servingRoot, 36) : 'Serving root missing',
      description: materialization ? `${materialization} materialization` : 'No materialization selected',
      detail: Boolean(config.LIBRARY_INCLUDE_FORMAT_IN_FOLDER ?? config.EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER) || Boolean(config.LIBRARY_INCLUDE_TYPE_IN_FOLDER ?? config.EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER)
        ? 'Folder naming extras enabled.'
        : 'Folder names stay minimal by default.',
      icon: Library,
      badges: [{ label: materialization || 'hardlink', variant: 'secondary' }],
    },
    {
      id: 'destinations',
      eyebrow: '5. Optional destinations',
      title: playerTarget === 'none' ? 'No player sync' : `${playerTarget} selected`,
      description: playerTarget === 'none' ? 'External players stay untouched.' : 'PMDA will refresh the selected player after the pipeline.',
      detail: playerTarget === 'none' ? 'Jellyfin and Navidrome are optional.' : 'Credentials live in the modal so the main settings page stays readable.',
      icon: HardDrive,
      badges: [{ label: playerTarget === 'none' ? 'Optional' : playerTarget, variant: playerTarget === 'none' ? 'secondary' : 'outline' }],
    },
  ];

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
              Keep the setup readable. Each block opens a focused modal instead of exposing every low-level option inline on the page.
            </CardDescription>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Button type="button" variant="outline" className="gap-2" onClick={onRefreshProviders} disabled={providersChecking}>
              {providersChecking ? <RefreshCw className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
              Check provider keys
            </Button>
            <Badge variant={keyIssues > 0 ? 'destructive' : 'outline'}>
              {keyIssues > 0 ? `${keyIssues} source issues` : 'Flow segmented by step'}
            </Badge>
          </div>
        </div>
      </CardHeader>

      <CardContent>
        <div className="grid gap-4 xl:grid-cols-5">
          {cards.map((card) => {
            const Icon = card.icon;
            return (
              <section key={card.id} className="rounded-[24px] border border-border/70 bg-background/45 p-4">
                <div className="flex items-start justify-between gap-3">
                  <div className="space-y-1">
                    <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">{card.eyebrow}</div>
                    <div className="flex items-center gap-2 text-sm font-semibold text-foreground">
                      <Icon className="h-4 w-4 text-primary" />
                      {card.title}
                    </div>
                  </div>
                  <Button type="button" variant="ghost" size="sm" className="h-8 gap-1 px-2 text-xs" onClick={() => onOpenSection(card.id)}>
                    Configure
                    <ChevronRight className="h-3.5 w-3.5" />
                  </Button>
                </div>
                <p className="mt-4 text-sm text-foreground/85">{card.description}</p>
                <p className="mt-2 text-xs leading-5 text-muted-foreground">{card.detail}</p>
                {card.id === 'sources' ? (
                  <div className="mt-4 flex flex-wrap gap-1.5">
                    <Badge variant="outline" className="gap-1.5"><ProviderIcon provider="discogs" size={12} />Discogs</Badge>
                    <Badge variant="outline" className="gap-1.5"><ProviderIcon provider="lastfm" size={12} />Last.fm</Badge>
                    <Badge variant="outline" className="gap-1.5"><ProviderIcon provider="acoustid" size={12} />AcoustID</Badge>
                  </div>
                ) : null}
                {card.badges && card.badges.length > 0 ? (
                  <div className="mt-4 flex flex-wrap gap-1.5">
                    {card.badges.map((badge) => (
                      <Badge key={badge.label} variant={badge.variant || 'outline'}>
                        {badge.label}
                      </Badge>
                    ))}
                  </div>
                ) : null}
              </section>
            );
          })}
        </div>

        <div className="mt-5 grid gap-3 md:grid-cols-3">
          <div className="rounded-2xl border border-border/70 bg-background/50 p-4">
            <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Sources</div>
            <div className="mt-2 flex items-center gap-2 text-sm font-semibold text-foreground">
              <Database className="h-4 w-4 text-primary" />
              {activeProviderCount} of 10 active
            </div>
          </div>
          <div className="rounded-2xl border border-border/70 bg-background/50 p-4">
            <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Published library</div>
            <div className="mt-2 flex items-center gap-2 text-sm font-semibold text-foreground">
              {servingRoot ? <CheckCircle2 className="h-4 w-4 text-emerald-500" /> : <Library className="h-4 w-4 text-amber-500" />}
              {servingRoot ? 'Topology ready' : 'Serving root missing'}
            </div>
          </div>
          <div className="rounded-2xl border border-border/70 bg-background/50 p-4">
            <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Destinations</div>
            <div className="mt-2 flex items-center gap-2 text-sm font-semibold text-foreground">
              <HardDrive className="h-4 w-4 text-primary" />
              {playerTarget === 'none' ? 'No player sync' : `${playerTarget} ready to configure`}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
