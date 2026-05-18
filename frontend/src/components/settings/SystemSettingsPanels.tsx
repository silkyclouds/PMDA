import type { ReactNode } from 'react';
import { Database, ExternalLink, HardDrive, Library, RefreshCw, ShieldCheck, Workflow } from 'lucide-react';

import type { PMDAConfig, PlayerTarget, ScanPreflightResult } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { FolderBrowserInput } from '@/components/settings/FolderBrowserInput';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { PasswordInput } from '@/components/ui/password-input';
import { Switch } from '@/components/ui/switch';
import { ProviderIcon } from '@/components/providers/ProviderIcon';

function normalizeFolderPath(input: unknown): string {
  const raw = String(input || '').trim();
  if (!raw) return '';
  if (raw === '/') return '/';
  return raw.replace(/\/+$/, '') || raw;
}

type ProviderKeyId = 'musicbrainz' | 'discogs' | 'lastfm' | 'acoustid' | 'fanart' | 'serper' | 'audiodb';

function providerBadgeState(
  provider: ProviderKeyId,
  configured: boolean,
  providersPreflight: ScanPreflightResult | null,
): { variant: 'default' | 'secondary' | 'destructive' | 'outline'; label: string; message: string } {
  if (!configured) {
    return { variant: 'outline', label: 'Missing', message: 'Configuration still missing.' };
  }
  if (!providersPreflight || provider === 'musicbrainz') {
    return { variant: 'secondary', label: 'Configured', message: 'Configured locally. Run validation for a live check.' };
  }
  const result = providersPreflight[provider];
  if (result?.ok) {
    return { variant: 'default', label: 'Valid', message: result.message || 'Credential check passed.' };
  }
  if (result && !result.ok) {
    return { variant: 'destructive', label: 'Issue', message: result.message || 'Credential check failed.' };
  }
  return { variant: 'secondary', label: 'Configured', message: 'Waiting for validation.' };
}

type PanelProps = {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
};

type SourcesPanelProps = PanelProps & {
  providersPreflight: ScanPreflightResult | null;
  providersChecking: boolean;
  providersPreflightAt: number | null;
  onRefreshProviders: () => void;
};

export function MetadataSourcesPanel({
  config,
  updateConfig,
  providersPreflight,
  providersChecking,
  providersPreflightAt,
  onRefreshProviders,
}: SourcesPanelProps) {
  const musicbrainzState = providerBadgeState('musicbrainz', Boolean(String(config.MUSICBRAINZ_EMAIL || '').trim()), providersPreflight);
  const discogsState = providerBadgeState('discogs', Boolean(String(config.DISCOGS_USER_TOKEN || '').trim()), providersPreflight);
  const lastfmState = providerBadgeState(
    'lastfm',
    Boolean(String(config.LASTFM_API_KEY || '').trim() && String(config.LASTFM_API_SECRET || '').trim()),
    providersPreflight,
  );
  const acoustidState = providerBadgeState('acoustid', Boolean(String(config.ACOUSTID_API_KEY || '').trim()), providersPreflight);
  const fanartState = providerBadgeState('fanart', Boolean(String(config.FANART_API_KEY || '').trim()), providersPreflight);
  const audiodbState = providerBadgeState('audiodb', Boolean(String(config.THEAUDIODB_API_KEY || '').trim()), providersPreflight);
  const serperState = providerBadgeState('serper', Boolean(String(config.SERPER_API_KEY || '').trim()), providersPreflight);

  const providerRows: Array<{
    id: 'musicbrainz' | 'discogs' | 'itunes' | 'deezer' | 'spotify' | 'qobuz' | 'tidal' | 'lastfm' | 'bandcamp' | 'acoustid' | 'fanart' | 'audiodb' | 'serper';
    label: string;
    description: string;
    enabled: boolean;
    setEnabled: (enabled: boolean) => void;
    toggleDisabled?: boolean;
    toggleHidden?: boolean;
    keyUrl?: string;
    keyLabel?: string;
    details?: ReactNode;
  }> = [
    {
      id: 'musicbrainz',
      label: 'MusicBrainz',
      description: 'Canonical identities, release groups and artist relationships.',
      enabled: true,
      setEnabled: () => undefined,
      details: (
        <div className="grid gap-2 md:grid-cols-[minmax(0,1fr)_auto]">
          <Input
            value={String(config.MUSICBRAINZ_EMAIL || '')}
            onChange={(event) => updateConfig({ MUSICBRAINZ_EMAIL: event.target.value })}
            placeholder="you@example.com"
          />
          <Badge variant={musicbrainzState.variant} className="self-start">
            {musicbrainzState.label}
          </Badge>
        </div>
      ),
    },
    {
      id: 'discogs',
      label: 'Discogs',
      description: 'Physical variants, labels and catalog detail.',
      enabled: Boolean(config.USE_DISCOGS ?? true),
      setEnabled: (enabled) => updateConfig({ USE_DISCOGS: enabled }),
      keyUrl: 'https://www.discogs.com/settings/developers',
      keyLabel: 'Create token',
      details: (
        <div className="grid gap-2 md:grid-cols-[minmax(0,1fr)_auto]">
          <PasswordInput
            value={String(config.DISCOGS_USER_TOKEN || '')}
            onChange={(event) => updateConfig({ DISCOGS_USER_TOKEN: event.target.value })}
            placeholder="Discogs user token"
          />
          <Badge variant={discogsState.variant} className="self-start">
            {discogsState.label}
          </Badge>
        </div>
      ),
    },
    {
      id: 'itunes',
      label: 'iTunes / Apple Music',
      description: 'Commercial catalog cross-checks and fast cover fallback.',
      enabled: Boolean(config.USE_ITUNES ?? true),
      setEnabled: (enabled) => updateConfig({ USE_ITUNES: enabled }),
    },
    {
      id: 'deezer',
      label: 'Deezer',
      description: 'Additional album, genre and cover cross-check.',
      enabled: Boolean(config.USE_DEEZER ?? true),
      setEnabled: (enabled) => updateConfig({ USE_DEEZER: enabled }),
    },
    {
      id: 'spotify',
      label: 'Spotify',
      description: 'Extra public catalog cross-check for title, year and artwork.',
      enabled: Boolean(config.USE_SPOTIFY ?? true),
      setEnabled: (enabled) => updateConfig({ USE_SPOTIFY: enabled }),
    },
    {
      id: 'qobuz',
      label: 'Qobuz',
      description: 'Store metadata and artwork cross-check.',
      enabled: Boolean(config.USE_QOBUZ ?? true),
      setEnabled: (enabled) => updateConfig({ USE_QOBUZ: enabled }),
    },
    {
      id: 'tidal',
      label: 'TIDAL',
      description: 'Public metadata fallback for extra artwork evidence.',
      enabled: Boolean(config.USE_TIDAL ?? true),
      setEnabled: (enabled) => updateConfig({ USE_TIDAL: enabled }),
    },
    {
      id: 'lastfm',
      label: 'Last.fm',
      description: 'Community descriptions, tags and similarity context.',
      enabled: Boolean(config.USE_LASTFM ?? true),
      setEnabled: (enabled) => updateConfig({ USE_LASTFM: enabled }),
      keyUrl: 'https://www.last.fm/api/account/create',
      keyLabel: 'Create keys',
      details: (
        <div className="grid gap-2 md:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]">
          <PasswordInput
            value={String(config.LASTFM_API_KEY || '')}
            onChange={(event) => updateConfig({ LASTFM_API_KEY: event.target.value })}
            placeholder="Last.fm API key"
          />
          <PasswordInput
            value={String(config.LASTFM_API_SECRET || '')}
            onChange={(event) => updateConfig({ LASTFM_API_SECRET: event.target.value })}
            placeholder="Last.fm API secret"
          />
          <Badge variant={lastfmState.variant} className="self-start">
            {lastfmState.label}
          </Badge>
        </div>
      ),
    },
    {
      id: 'bandcamp',
      label: 'Bandcamp',
      description: 'Independent release verification and artist metadata.',
      enabled: Boolean(config.USE_BANDCAMP ?? false),
      setEnabled: (enabled) => updateConfig({ USE_BANDCAMP: enabled }),
    },
    {
      id: 'acoustid',
      label: 'AcoustID',
      description: 'Audio fingerprint matching when tags or structure are weak.',
      enabled: Boolean(config.USE_ACOUSTID ?? true),
      setEnabled: (enabled) => updateConfig({ USE_ACOUSTID: enabled }),
      keyUrl: 'https://acoustid.org/new-application',
      keyLabel: 'Create key',
      details: (
        <div className="grid gap-2 md:grid-cols-[minmax(0,1fr)_auto]">
          <PasswordInput
            value={String(config.ACOUSTID_API_KEY || '')}
            onChange={(event) => updateConfig({ ACOUSTID_API_KEY: event.target.value })}
            placeholder="AcoustID API key"
          />
          <Badge variant={acoustidState.variant} className="self-start">
            {acoustidState.label}
          </Badge>
        </div>
      ),
    },
    {
      id: 'fanart',
      label: 'Fanart.tv',
      description: 'Optional extra artist artwork fallback when the core sources do not provide a strong enough image set.',
      enabled: Boolean(String(config.FANART_API_KEY || '').trim()),
      setEnabled: () => undefined,
      toggleHidden: true,
      keyUrl: 'https://fanart.tv/get-an-api-key/',
      keyLabel: 'Get API key',
      details: (
        <div className="space-y-2">
          <div className="grid gap-2 md:grid-cols-[minmax(0,1fr)_auto]">
            <PasswordInput
              value={String(config.FANART_API_KEY || '')}
              onChange={(event) => updateConfig({ FANART_API_KEY: event.target.value })}
              placeholder="Fanart.tv API key"
            />
            <Badge variant={fanartState.variant} className="self-start">
              {fanartState.label}
            </Badge>
          </div>
          <p className="text-xs text-muted-foreground">
            Optional. PMDA uses this only for extra MBID-based artist artwork when the primary providers are not enough.
          </p>
        </div>
      ),
    },
    {
      id: 'audiodb',
      label: 'TheAudioDB',
      description: 'Optional extra album artwork and artist image fallback for harder-to-complete profiles.',
      enabled: Boolean(String(config.THEAUDIODB_API_KEY || '').trim()),
      setEnabled: () => undefined,
      toggleHidden: true,
      keyUrl: 'https://www.theaudiodb.com/api_apply.php',
      keyLabel: 'Get API key',
      details: (
        <div className="space-y-2">
          <div className="grid gap-2 md:grid-cols-[minmax(0,1fr)_auto]">
            <PasswordInput
              value={String(config.THEAUDIODB_API_KEY || '')}
              onChange={(event) => updateConfig({ THEAUDIODB_API_KEY: event.target.value })}
              placeholder="TheAudioDB API key"
            />
            <Badge variant={audiodbState.variant} className="self-start">
              {audiodbState.label}
            </Badge>
          </div>
          <p className="text-xs text-muted-foreground">
            Optional. Leave it empty if you do not want TheAudioDB as a fallback artwork source.
          </p>
        </div>
      ),
    },
    {
      id: 'serper',
      label: 'Serper',
      description: 'Optional web search backend when PMDA needs outside evidence to resolve or verify a MusicBrainz candidate.',
      enabled: Boolean(config.USE_WEB_SEARCH_FOR_MB ?? true),
      setEnabled: (enabled) => updateConfig({ USE_WEB_SEARCH_FOR_MB: enabled }),
      keyUrl: 'https://serper.dev',
      keyLabel: 'Get API key',
      details: (
        <div className="space-y-2">
          <div className="grid gap-2 md:grid-cols-[minmax(0,1fr)_auto]">
            <PasswordInput
              value={String(config.SERPER_API_KEY || '')}
              onChange={(event) => updateConfig({ SERPER_API_KEY: event.target.value })}
              placeholder="Serper API key"
            />
            <Badge variant={serperState.variant} className="self-start">
              {serperState.label}
            </Badge>
          </div>
          <p className="text-xs text-muted-foreground">
            Optional. PMDA can still work without it, but Serper gives the web-search fallback a deterministic external backend when that path is needed.
          </p>
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-border/60 bg-background/40 p-4">
        <div className="space-y-1">
          <div className="text-sm font-semibold text-foreground">Metadata providers and keys</div>
          <p className="text-xs text-muted-foreground">
            Add credentials inline, validate them live, and keep only the providers you want active for the pipeline.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button type="button" variant="outline" className="gap-2" onClick={onRefreshProviders} disabled={providersChecking}>
            {providersChecking ? <RefreshCw className="h-4 w-4 animate-spin" /> : <ShieldCheck className="h-4 w-4" />}
            Validate keys
          </Button>
          <Badge variant="outline">
            {providersPreflightAt ? `Checked ${new Date(providersPreflightAt).toLocaleTimeString()}` : 'Not checked yet'}
          </Badge>
        </div>
      </div>

      <div className="space-y-3">
        {providerRows.map((provider) => (
          <div key={provider.id} className="rounded-2xl border border-border/60 bg-background/30 p-4">
            <div className="flex items-start justify-between gap-3">
              <div className="space-y-1">
                <div className="flex items-center gap-2 text-sm font-semibold text-foreground">
                  <ProviderIcon provider={provider.id} size={14} />
                  {provider.label}
                </div>
                <p className="text-xs text-muted-foreground">{provider.description}</p>
                {provider.keyUrl ? (
                  <a
                    href={provider.keyUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center gap-1 text-xs text-primary hover:underline"
                  >
                    {provider.keyLabel || 'Create key'}
                    <ExternalLink className="h-3 w-3" />
                  </a>
                ) : null}
              </div>
              {provider.toggleHidden ? null : (
                <Switch
                  checked={provider.enabled}
                  onCheckedChange={provider.id === 'musicbrainz' || provider.toggleDisabled ? undefined : provider.setEnabled}
                  disabled={provider.id === 'musicbrainz' || provider.toggleDisabled}
                  aria-label={`Toggle ${provider.label}`}
                />
              )}
            </div>
            {provider.details ? <div className="mt-3">{provider.details}</div> : null}
          </div>
        ))}
      </div>
    </div>
  );
}

export function ScanBehaviorSettingsPanel({ config, updateConfig }: PanelProps) {
  const schedulerPaused = config.SCHEDULER_PAUSED !== false;
  const allowNonScanJobs = Boolean(config.SCHEDULER_ALLOW_NON_SCAN_JOBS ?? false);
  const postScanAsync = Boolean(config.PIPELINE_POST_SCAN_ASYNC ?? false);
  const scanFirstModeEnabled = schedulerPaused && !allowNonScanJobs && !postScanAsync;

  const toggleScanFirstMode = (enabled: boolean) => {
    if (enabled) {
      updateConfig({
        SCHEDULER_PAUSED: true,
        SCHEDULER_ALLOW_NON_SCAN_JOBS: false,
        PIPELINE_POST_SCAN_ASYNC: false,
      });
      return;
    }
    updateConfig({ SCHEDULER_PAUSED: false });
  };

  return (
    <div className="space-y-4 rounded-2xl border border-border/60 bg-background/30 p-4">
      <div className="space-y-1">
        <div className="text-sm font-semibold text-foreground">Scan behavior</div>
        <p className="text-xs text-muted-foreground">
          Keep scan ownership simple: decide when PMDA may launch scans and whether trailing work stays inside the scan or continues afterwards.
        </p>
      </div>

      <div className="flex items-start justify-between gap-4 rounded-xl border border-border/60 bg-background/40 p-4">
        <div className="space-y-1">
          <Label>Scan-first mode</Label>
          <p className="text-xs text-muted-foreground">
            Recommended. PMDA only scans when you decide to launch it, and the main pipeline stays attached to that scan.
          </p>
        </div>
        <Switch checked={scanFirstModeEnabled} onCheckedChange={toggleScanFirstMode} />
      </div>

      <div className="grid gap-3 md:grid-cols-3">
        <div className="rounded-xl border border-border/60 bg-background/40 p-3">
          <div className="text-xs text-muted-foreground">Scheduled scans</div>
          <div className="mt-1 text-sm font-medium text-foreground">{schedulerPaused ? 'Paused' : 'Enabled'}</div>
        </div>
        <div className="rounded-xl border border-border/60 bg-background/40 p-3">
          <div className="text-xs text-muted-foreground">Background jobs</div>
          <div className="mt-1 text-sm font-medium text-foreground">{allowNonScanJobs ? 'Allowed' : 'Disabled'}</div>
        </div>
        <div className="rounded-xl border border-border/60 bg-background/40 p-3">
          <div className="text-xs text-muted-foreground">Post-scan chain</div>
          <div className="mt-1 text-sm font-medium text-foreground">{postScanAsync ? 'Async queue' : 'Inline in scan'}</div>
        </div>
      </div>

      <div className="space-y-3">
        <div className="flex items-start justify-between gap-4 rounded-xl border border-border/60 bg-background/40 p-4">
          <div className="space-y-1">
            <Label>Allow scheduled scans</Label>
            <p className="text-xs text-muted-foreground">
              Lets PMDA launch the same full pipeline automatically on schedule.
            </p>
          </div>
          <Switch checked={!schedulerPaused} onCheckedChange={(checked) => updateConfig({ SCHEDULER_PAUSED: !checked })} />
        </div>
        <div className="flex items-start justify-between gap-4 rounded-xl border border-border/60 bg-background/40 p-4">
          <div className="space-y-1">
            <Label>Allow non-scan background jobs</Label>
            <p className="text-xs text-muted-foreground">
              Lets PMDA continue filling gaps outside a scan, for example on artist images, label logos or descriptions.
            </p>
          </div>
          <Switch checked={allowNonScanJobs} onCheckedChange={(checked) => updateConfig({ SCHEDULER_ALLOW_NON_SCAN_JOBS: checked })} />
        </div>
        <div className="flex items-start justify-between gap-4 rounded-xl border border-border/60 bg-background/40 p-4">
          <div className="space-y-1">
            <Label>Post-scan chain in async queue</Label>
            <p className="text-xs text-muted-foreground">
              Enabled means PMDA may finish the scan first and complete the tail of the pipeline in background.
            </p>
          </div>
          <Switch checked={postScanAsync} onCheckedChange={(checked) => updateConfig({ PIPELINE_POST_SCAN_ASYNC: checked })} />
        </div>
      </div>
    </div>
  );
}

export function PipelineSettingsPanel({ config, updateConfig }: PanelProps) {
  const workflowMode = String(config.LIBRARY_WORKFLOW_MODE || 'managed').trim().toLowerCase();
  const auditMode = workflowMode === 'audit';
  const dedupeMode = !Boolean(config.PIPELINE_ENABLE_DEDUPE)
    ? 'ignore'
    : Boolean(config.AUTO_MOVE_DUPES)
      ? 'move'
      : 'detect';
  const incompleteMode = !Boolean(config.PIPELINE_ENABLE_INCOMPLETE_MOVE)
    ? 'keep'
    : (Number(config.BROKEN_ALBUM_CONSECUTIVE_THRESHOLD ?? 2) <= 1 || Number(config.BROKEN_ALBUM_PERCENTAGE_THRESHOLD ?? 0.25) <= 0.12)
      ? 'strict'
      : 'obvious';
  const tagWriteMode = String(config.FILES_TAG_WRITE_MODE || 'full').trim() === 'pmda_id_only' ? 'pmda_id_only' : 'full';

  const setDuplicateMode = (mode: 'ignore' | 'detect' | 'move') => {
    if (mode === 'ignore') {
      updateConfig({ PIPELINE_ENABLE_DEDUPE: false, AUTO_MOVE_DUPES: false });
      return;
    }
    if (mode === 'detect') {
      updateConfig({ PIPELINE_ENABLE_DEDUPE: true, AUTO_MOVE_DUPES: false });
      return;
    }
    updateConfig({ PIPELINE_ENABLE_DEDUPE: true, AUTO_MOVE_DUPES: true });
  };

  const setIncompleteMode = (mode: 'keep' | 'obvious' | 'strict') => {
    if (mode === 'keep') {
      updateConfig({ PIPELINE_ENABLE_INCOMPLETE_MOVE: false });
      return;
    }
    if (mode === 'obvious') {
      updateConfig({
        PIPELINE_ENABLE_INCOMPLETE_MOVE: true,
        BROKEN_ALBUM_CONSECUTIVE_THRESHOLD: 2,
        BROKEN_ALBUM_PERCENTAGE_THRESHOLD: 0.25,
      });
      return;
    }
    updateConfig({
      PIPELINE_ENABLE_INCOMPLETE_MOVE: true,
      BROKEN_ALBUM_CONSECUTIVE_THRESHOLD: 1,
      BROKEN_ALBUM_PERCENTAGE_THRESHOLD: 0.12,
    });
  };

  return (
    <div className="space-y-4">
      <div className="rounded-2xl border border-border/60 bg-background/30 p-4">
        <div className="space-y-1">
          <div className="text-sm font-semibold text-foreground">Duplicates</div>
          <p className="text-xs text-muted-foreground">
            Decide whether PMDA should ignore duplicates, surface them for review, or move the losing editions automatically.
          </p>
        </div>
        <div className="mt-4 grid gap-3 md:grid-cols-3">
          {[
            { value: 'ignore' as const, label: 'Ignore', description: 'Keep duplicates in place.' },
            { value: 'detect' as const, label: 'Review only', description: 'Flag them without moving files.' },
            { value: 'move' as const, label: 'Move losers', description: 'Send losing editions to the duplicates folder.' },
          ].map((option) => (
            <button
              key={option.value}
              type="button"
              onClick={() => setDuplicateMode(option.value)}
              className={`rounded-2xl border p-4 text-left transition ${dedupeMode === option.value ? 'border-primary/45 bg-primary/12' : 'border-border/60 bg-background/40 hover:border-border'}`}
            >
              <div className="flex items-center justify-between gap-3">
                <div className="text-sm font-semibold text-foreground">{option.label}</div>
                {dedupeMode === option.value ? <Badge>Selected</Badge> : null}
              </div>
              <p className="mt-2 text-xs text-muted-foreground">{option.description}</p>
            </button>
          ))}
        </div>
      </div>

      <div className="rounded-2xl border border-border/60 bg-background/30 p-4">
        <div className="space-y-1">
          <div className="text-sm font-semibold text-foreground">Incomplete albums</div>
          <p className="text-xs text-muted-foreground">
            Choose how strict PMDA should be before it quarantines a release as incomplete.
          </p>
        </div>
        <div className="mt-4 grid gap-3 md:grid-cols-3">
          {[
            { value: 'keep' as const, label: 'Keep everything', description: 'Do not move incomplete albums automatically.' },
            { value: 'obvious' as const, label: 'Only obvious gaps', description: 'Recommended. Keep near-complete alt editions in place.' },
            { value: 'strict' as const, label: 'Strict mode', description: 'Move even small gaps to the incomplete folder.' },
          ].map((option) => (
            <button
              key={option.value}
              type="button"
              onClick={() => setIncompleteMode(option.value)}
              className={`rounded-2xl border p-4 text-left transition ${incompleteMode === option.value ? 'border-primary/45 bg-primary/12' : 'border-border/60 bg-background/40 hover:border-border'}`}
            >
              <div className="flex items-center justify-between gap-3">
                <div className="text-sm font-semibold text-foreground">{option.label}</div>
                {incompleteMode === option.value ? <Badge>Selected</Badge> : null}
              </div>
              <p className="mt-2 text-xs text-muted-foreground">{option.description}</p>
            </button>
          ))}
        </div>
        <div className="mt-4 flex items-start justify-between gap-4 rounded-xl border border-border/60 bg-background/40 p-4">
          <div className="space-y-1">
            <Label>Keep repairing incomplete albums later</Label>
            <p className="text-xs text-muted-foreground">
              Lets future runs keep trying to fill missing metadata or artwork for releases that were not complete yet.
            </p>
          </div>
          <Switch
            checked={Boolean(config.REPROCESS_INCOMPLETE_ALBUMS)}
            onCheckedChange={(checked) => updateConfig({ REPROCESS_INCOMPLETE_ALBUMS: checked })}
          />
        </div>
      </div>

      <div className="rounded-2xl border border-border/60 bg-background/30 p-4">
        <div className="space-y-1">
          <div className="text-sm font-semibold text-foreground">Write mode</div>
          <p className="text-xs text-muted-foreground">
            Choose whether PMDA writes full tags into files or keeps metadata only in the PMDA database view.
          </p>
        </div>
        <div className="mt-4 grid gap-3 md:grid-cols-2">
          {[
            {
              value: 'full' as const,
              label: 'Write tags into files',
              description: auditMode
                ? 'Deferred in audit mode. If you export later, PMDA will write enriched tags into files.'
                : 'PMDA writes enriched tags and artwork directly into files.',
            },
            {
              value: 'pmda_id_only' as const,
              label: 'DB-only (PMDA ID only)',
              description: auditMode
                ? 'Recommended for audit. PMDA stays database-first until you explicitly export later.'
                : 'PMDA keeps metadata in PostgreSQL and only stores a PMDA ID tag on files.',
            },
          ].map((option) => (
            <button
              key={option.value}
              type="button"
              onClick={() => updateConfig({ FILES_TAG_WRITE_MODE: option.value })}
              className={`rounded-2xl border p-4 text-left transition ${tagWriteMode === option.value ? 'border-primary/45 bg-primary/12' : 'border-border/60 bg-background/40 hover:border-border'}`}
            >
              <div className="flex items-center justify-between gap-3">
                <div className="text-sm font-semibold text-foreground">{option.label}</div>
                {tagWriteMode === option.value ? <Badge>Selected</Badge> : null}
              </div>
              <p className="mt-2 text-xs text-muted-foreground">{option.description}</p>
            </button>
          ))}
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        <div className="rounded-2xl border border-border/60 bg-background/30 p-4 space-y-2">
          <Label>Duplicates folder</Label>
          <FolderBrowserInput
            value={String(config.LIBRARY_DUPES_ROOT || config.DUPE_ROOT || '/dupes')}
            onChange={(path) => updateConfig({ LIBRARY_DUPES_ROOT: normalizeFolderPath(path), DUPE_ROOT: normalizeFolderPath(path) })}
            placeholder="/dupes"
            selectLabel="Select duplicates folder"
          />
        </div>
        <div className="rounded-2xl border border-border/60 bg-background/30 p-4 space-y-2">
          <Label>Incomplete albums folder</Label>
          <FolderBrowserInput
            value={String(config.LIBRARY_INCOMPLETE_ROOT || config.INCOMPLETE_ALBUMS_TARGET_DIR || '/dupes/incomplete_albums')}
            onChange={(path) => updateConfig({ LIBRARY_INCOMPLETE_ROOT: normalizeFolderPath(path), INCOMPLETE_ALBUMS_TARGET_DIR: normalizeFolderPath(path) })}
            placeholder="/dupes/incomplete_albums"
            selectLabel="Select incomplete albums folder"
          />
        </div>
      </div>

      <ScanBehaviorSettingsPanel config={config} updateConfig={updateConfig} />
    </div>
  );
}

export function PublishedLibrarySettingsPanel({ config, updateConfig }: PanelProps) {
  const workflowMode = String(config.LIBRARY_WORKFLOW_MODE || 'managed').trim().toLowerCase();
  const publishable = workflowMode === 'managed' || workflowMode === 'mirror';
  const materialization = String(config.LIBRARY_MATERIALIZATION_MODE || config.EXPORT_LINK_STRATEGY || 'hardlink').trim().toLowerCase();

  return (
    <div className="space-y-4">
      <div className="rounded-2xl border border-border/60 bg-background/30 p-4">
        <div className="flex items-center gap-2 text-sm font-semibold text-foreground">
          <Library className="h-4 w-4 text-primary" />
          Published library
        </div>
        <p className="mt-1 text-xs text-muted-foreground">
          Set where the clean library lives, how PMDA materializes it, and how folder names are rendered.
        </p>
      </div>

      {publishable ? (
        <>
          <div className="rounded-2xl border border-border/60 bg-background/30 p-4 space-y-2">
            <Label>Serving library root</Label>
            <FolderBrowserInput
              value={String(config.LIBRARY_SERVING_ROOT || config.EXPORT_ROOT || '/music/Music_matched')}
              onChange={(path) => updateConfig({ LIBRARY_SERVING_ROOT: normalizeFolderPath(path), EXPORT_ROOT: normalizeFolderPath(path) })}
              placeholder="/music/Music_matched"
              selectLabel="Select serving library root"
            />
          </div>

          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            {[
              { value: 'hardlink', label: 'Hardlink', description: 'Fastest, no extra disk usage on the same filesystem.' },
              { value: 'symlink', label: 'Symlink', description: 'References originals instead of duplicating files.' },
              { value: 'copy', label: 'Copy', description: 'Safest, but duplicates data.' },
              { value: 'move', label: 'Move', description: 'Relocates files physically into the published library.' },
            ].map((option) => (
              <button
                key={option.value}
                type="button"
                onClick={() => updateConfig({ LIBRARY_MATERIALIZATION_MODE: option.value as PMDAConfig['LIBRARY_MATERIALIZATION_MODE'], EXPORT_LINK_STRATEGY: option.value as PMDAConfig['EXPORT_LINK_STRATEGY'] })}
                className={`rounded-2xl border p-4 text-left transition ${materialization === option.value ? 'border-primary/45 bg-primary/12' : 'border-border/60 bg-background/40 hover:border-border'}`}
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="text-sm font-semibold text-foreground">{option.label}</div>
                  {materialization === option.value ? <Badge>Selected</Badge> : null}
                </div>
                <p className="mt-2 text-xs text-muted-foreground">{option.description}</p>
              </button>
            ))}
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div className="flex items-start justify-between gap-4 rounded-2xl border border-border/60 bg-background/30 p-4">
              <div className="space-y-1">
                <Label>Include album format in folder name</Label>
                <p className="text-xs text-muted-foreground">Example: Desert Solitaire (Flac)</p>
              </div>
              <Switch
                checked={Boolean(config.LIBRARY_INCLUDE_FORMAT_IN_FOLDER ?? config.EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER)}
                onCheckedChange={(checked) => updateConfig({ LIBRARY_INCLUDE_FORMAT_IN_FOLDER: checked, EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER: checked })}
              />
            </div>
            <div className="flex items-start justify-between gap-4 rounded-2xl border border-border/60 bg-background/30 p-4">
              <div className="space-y-1">
                <Label>Include album type in folder name</Label>
                <p className="text-xs text-muted-foreground">Example: Desert Solitaire (Flac, Album)</p>
              </div>
              <Switch
                checked={Boolean(config.LIBRARY_INCLUDE_TYPE_IN_FOLDER ?? config.EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER)}
                onCheckedChange={(checked) => updateConfig({ LIBRARY_INCLUDE_TYPE_IN_FOLDER: checked, EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER: checked })}
              />
            </div>
          </div>
        </>
      ) : (
        <div className="rounded-2xl border border-border/60 bg-background/30 p-4 text-sm text-muted-foreground">
          This workflow serves the current library directly. PMDA will not build a second published tree until you switch back to a managed or mirror workflow.
        </div>
      )}
    </div>
  );
}

export function OptionalDestinationsSettingsPanel({ config, updateConfig }: PanelProps) {
  const auditMode = String(config.LIBRARY_WORKFLOW_MODE || 'managed').trim().toLowerCase() === 'audit';
  const playerTarget = (Boolean(config.PIPELINE_ENABLE_PLAYER_SYNC)
    ? String(config.PIPELINE_PLAYER_TARGET || 'none').trim().toLowerCase()
    : 'none') as PlayerTarget;

  const setPlayerTarget = (target: PlayerTarget) => {
    if (target === 'none') {
      updateConfig({ PIPELINE_PLAYER_TARGET: 'none', PIPELINE_ENABLE_PLAYER_SYNC: false });
      return;
    }
    updateConfig({ PIPELINE_PLAYER_TARGET: target, PIPELINE_ENABLE_PLAYER_SYNC: true });
  };

  return (
    <div className="space-y-4">
      <div className="rounded-2xl border border-border/60 bg-background/30 p-4">
        <div className="flex items-center gap-2 text-sm font-semibold text-foreground">
          <HardDrive className="h-4 w-4 text-primary" />
          Optional destinations
        </div>
        <p className="mt-1 text-xs text-muted-foreground">
          Connect Plex, Jellyfin, or Navidrome only if you want PMDA to refresh them automatically after publication.
        </p>
      </div>

      {auditMode ? (
        <div className="rounded-2xl border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-100">
          Audit mode keeps automatic player sync off. PMDA stays read-only until you switch workflow or trigger a later explicit export path.
        </div>
      ) : null}

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        {[
          { value: 'none' as const, label: 'Not now', description: 'Skip automatic player sync.' },
          { value: 'plex' as const, label: 'Plex', description: 'Refresh Plex after the run.' },
          { value: 'jellyfin' as const, label: 'Jellyfin', description: 'Refresh Jellyfin after the run.' },
          { value: 'navidrome' as const, label: 'Navidrome', description: 'Refresh Navidrome after the run.' },
        ].map((option) => (
          <button
            key={option.value}
            type="button"
            disabled={auditMode}
            onClick={() => setPlayerTarget(option.value)}
            className={`rounded-2xl border p-4 text-left transition ${playerTarget === option.value ? 'border-primary/45 bg-primary/12' : 'border-border/60 bg-background/40 hover:border-border'} ${auditMode ? 'opacity-60' : ''}`}
          >
            <div className="flex items-center justify-between gap-3">
              <div className="text-sm font-semibold text-foreground">{option.label}</div>
              {playerTarget === option.value ? <Badge>Selected</Badge> : null}
            </div>
            <p className="mt-2 text-xs text-muted-foreground">{option.description}</p>
          </button>
        ))}
      </div>

      {playerTarget === 'plex' ? (
        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2">
            <Label>Plex URL</Label>
            <Input value={String(config.PLEX_HOST || '')} onChange={(event) => updateConfig({ PLEX_HOST: event.target.value })} placeholder="http://plex:32400" />
          </div>
          <div className="space-y-2">
            <Label>Plex token</Label>
            <PasswordInput value={String(config.PLEX_TOKEN || '')} onChange={(event) => updateConfig({ PLEX_TOKEN: event.target.value })} placeholder={config.PLEX_TOKEN_SET ? 'Stored token unchanged' : 'Plex token'} />
          </div>
        </div>
      ) : null}

      {playerTarget === 'jellyfin' ? (
        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2">
            <Label>Jellyfin URL</Label>
            <Input value={String(config.JELLYFIN_URL || '')} onChange={(event) => updateConfig({ JELLYFIN_URL: event.target.value })} placeholder="http://jellyfin:8096" />
          </div>
          <div className="space-y-2">
            <Label>Jellyfin API key</Label>
            <PasswordInput value={String(config.JELLYFIN_API_KEY || '')} onChange={(event) => updateConfig({ JELLYFIN_API_KEY: event.target.value })} placeholder="Jellyfin API key" />
          </div>
        </div>
      ) : null}

      {playerTarget === 'navidrome' ? (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <div className="space-y-2">
            <Label>Navidrome URL</Label>
            <Input value={String(config.NAVIDROME_URL || '')} onChange={(event) => updateConfig({ NAVIDROME_URL: event.target.value })} placeholder="http://navidrome:4533" />
          </div>
          <div className="space-y-2">
            <Label>API key</Label>
            <PasswordInput value={String(config.NAVIDROME_API_KEY || '')} onChange={(event) => updateConfig({ NAVIDROME_API_KEY: event.target.value })} placeholder="Navidrome API key" />
          </div>
          <div className="space-y-2">
            <Label>Username / password fallback</Label>
            <div className="grid gap-2">
              <Input value={String(config.NAVIDROME_USERNAME || '')} onChange={(event) => updateConfig({ NAVIDROME_USERNAME: event.target.value })} placeholder="Username" />
              <PasswordInput value={String(config.NAVIDROME_PASSWORD || '')} onChange={(event) => updateConfig({ NAVIDROME_PASSWORD: event.target.value })} placeholder="Password" />
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
