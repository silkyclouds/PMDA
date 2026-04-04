import { useCallback, useEffect, useMemo, useState } from 'react';
import { AlertTriangle, Bot, Cpu, Database, Globe, Loader2, RefreshCw, Server } from 'lucide-react';

import * as api from '@/lib/api';
import type { ManagedRuntimeBundleStatus, ManagedRuntimeBundleType, ManagedRuntimeStatusResponse, PMDAConfig, ScalingRuntimeResponse } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { FolderBrowserInput } from '@/components/settings/FolderBrowserInput';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { toast } from 'sonner';

type Props = {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
};

function formatRate(value: number | undefined): string {
  const safe = Number.isFinite(Number(value)) ? Number(value) : 0;
  return `${safe.toFixed(safe >= 100 ? 0 : 1)}/h`;
}

function formatAgo(epoch: number | null | undefined): string {
  if (!epoch || !Number.isFinite(epoch)) return 'never';
  const diff = Math.max(0, Date.now() / 1000 - Number(epoch));
  if (diff < 60) return `${Math.round(diff)}s ago`;
  if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
  return `${Math.round(diff / 3600)}h ago`;
}

function NumericSetting({
  label,
  help,
  value,
  min,
  max,
  onChange,
}: {
  label: string;
  help: string;
  value: number | undefined;
  min: number;
  max: number;
  onChange: (value: number) => void;
}) {
  return (
    <div className="space-y-2 rounded-lg border border-border/60 bg-background/30 p-3">
      <div className="space-y-1">
        <Label>{label}</Label>
        <p className="text-[11px] text-muted-foreground">{help}</p>
      </div>
      <Input
        type="number"
        min={min}
        max={max}
        value={value ?? ''}
        onChange={(event) => {
          const next = Number(event.target.value);
          if (!Number.isFinite(next)) return;
          onChange(Math.min(max, Math.max(min, Math.trunc(next))));
        }}
      />
    </div>
  );
}

function isManagedBundleReady(bundle: ManagedRuntimeBundleStatus | null | undefined): boolean {
  if (!bundle) return false;
  return bundle.state === 'ready' && Boolean(bundle.health?.available);
}

function formatManagedModels(bundle: ManagedRuntimeBundleStatus | null | undefined): string {
  const healthModels = Array.isArray(bundle?.health?.models) ? bundle.health.models : [];
  const metaModels = Array.isArray(bundle?.meta?.models) ? bundle.meta.models : [];
  const models = Array.from(new Set([...healthModels, ...metaModels].map((value) => String(value || '').trim()).filter(Boolean)));
  return models.length > 0 ? models.join(', ') : 'none';
}

function formatManagedTimestamp(epoch?: number | null): string {
  if (!epoch || !Number.isFinite(epoch)) return '—';
  return new Date(Number(epoch) * 1000).toLocaleString();
}

export function ScalingSettings({ config, updateConfig }: Props) {
  const [runtime, setRuntime] = useState<ScalingRuntimeResponse | null>(null);
  const [managedStatus, setManagedStatus] = useState<ManagedRuntimeStatusResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string>('');
  const [managedBusy, setManagedBusy] = useState<string>('');

  const refreshRuntime = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const [data, managed] = await Promise.all([
        api.getScalingRuntime(),
        api.getManagedRuntimeStatus().catch(() => null),
      ]);
      setRuntime(data);
      setManagedStatus(managed || data.managed_runtime || null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load scaling runtime');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refreshRuntime();
    const timer = window.setInterval(() => {
      void refreshRuntime();
    }, 15000);
    return () => window.clearInterval(timer);
  }, [refreshRuntime]);

  const effectiveMbTarget = useMemo(() => {
    if (config.MUSICBRAINZ_EFFECTIVE_BASE_URL && String(config.MUSICBRAINZ_EFFECTIVE_BASE_URL).trim()) {
      return String(config.MUSICBRAINZ_EFFECTIVE_BASE_URL).trim();
    }
    if (runtime?.musicbrainz.base_url) return runtime.musicbrainz.base_url;
    return 'https://musicbrainz.org';
  }, [config.MUSICBRAINZ_EFFECTIVE_BASE_URL, runtime?.musicbrainz.base_url]);

  const providerRows = useMemo(() => {
    const source = runtime?.provider_gateway.providers || {};
    return ['discogs', 'lastfm', 'bandcamp'].map((provider) => ({
      provider,
      stats: source[provider],
    }));
  }, [runtime]);

  const managedMbBundle = managedStatus?.bundles?.musicbrainz_local || null;
  const managedOllamaBundle = managedStatus?.bundles?.ollama_local || null;

  const runManagedAction = useCallback(async (
    bundleType: ManagedRuntimeBundleType,
    action: api.ManagedRuntimeActionRequest['action'],
    extra?: Partial<api.ManagedRuntimeActionRequest>,
  ) => {
    setManagedBusy(`${bundleType}:${action}`);
    try {
      const result = await api.managedRuntimeAction({
        bundle_type: bundleType,
        action,
        config_root: String(config.MANAGED_RUNTIME_CONFIG_ROOT || '').trim(),
        data_root: String(config.MANAGED_RUNTIME_DATA_ROOT || '').trim(),
        fast_model: bundleType === 'ollama_local' ? String(config.OLLAMA_MODEL || '').trim() : undefined,
        hard_model: bundleType === 'ollama_local' ? String(config.OLLAMA_COMPLEX_MODEL || '').trim() : undefined,
        ...extra,
      });
      if (result.snapshot) setManagedStatus(result.snapshot);
      toast.success(result.message || `${bundleType} ${action} started`);
      void refreshRuntime();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : `Failed to ${action}`);
    } finally {
      setManagedBusy('');
    }
  }, [config.MANAGED_RUNTIME_CONFIG_ROOT, config.MANAGED_RUNTIME_DATA_ROOT, config.OLLAMA_COMPLEX_MODEL, config.OLLAMA_MODEL, refreshRuntime]);

  const bootstrapManagedBundle = useCallback(async (bundleType: ManagedRuntimeBundleType) => {
    const configRoot = String(config.MANAGED_RUNTIME_CONFIG_ROOT || '').trim();
    const dataRoot = String(config.MANAGED_RUNTIME_DATA_ROOT || '').trim();
    if (!configRoot || !dataRoot) {
      toast.error('Set both managed runtime roots first.');
      return;
    }
    setManagedBusy(`${bundleType}:bootstrap`);
    try {
      const payload =
        bundleType === 'musicbrainz_local'
          ? { action: 'auto' as const, mirror_name: String(config.MUSICBRAINZ_MIRROR_NAME || 'Managed local MusicBrainz').trim() || 'Managed local MusicBrainz' }
          : { action: 'auto' as const, fast_model: String(config.OLLAMA_MODEL || 'qwen3:4b').trim() || 'qwen3:4b', hard_model: String(config.OLLAMA_COMPLEX_MODEL || 'qwen3:14b').trim() || 'qwen3:14b' };
      const result = await api.bootstrapManagedRuntime({
        config_root: configRoot,
        data_root: dataRoot,
        bundle_type: bundleType,
        payload,
      });
      setManagedStatus(result.snapshot);
      toast.success(result.results[0]?.message || `Started ${bundleType} bootstrap`);
      void refreshRuntime();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to start bootstrap');
    } finally {
      setManagedBusy('');
    }
  }, [config.MANAGED_RUNTIME_CONFIG_ROOT, config.MANAGED_RUNTIME_DATA_ROOT, config.MUSICBRAINZ_MIRROR_NAME, config.OLLAMA_COMPLEX_MODEL, config.OLLAMA_MODEL, refreshRuntime]);

  return (
    <Card id="settings-scaling" className="scroll-mt-24 border-border/60 bg-muted/10">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <span className="pmda-settings-section-icon">
            <Server className="h-4 w-4" />
          </span>
          Scaling & orchestration
        </CardTitle>
        <CardDescription>
          Keep file work local, optionally target a local MusicBrainz mirror, centralize provider throttling, and prepare hybrid metadata workers without turning the scan into a WAN crawler.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
          <div className="rounded-xl border border-border/60 bg-background/30 p-4 space-y-4">
            <div className="flex items-center justify-between gap-3">
              <div className="space-y-1">
                <div className="flex items-center gap-2 text-sm font-medium">
                  <Globe className="h-4 w-4" />
                  MusicBrainz target
                </div>
                <p className="text-xs text-muted-foreground">
                  Public MusicBrainz stays available, but a local mirror removes the 1 req/sec public bottleneck and stabilizes retries.
                </p>
              </div>
              <Switch
                checked={Boolean(config.MUSICBRAINZ_MIRROR_ENABLED)}
                onCheckedChange={(checked) => updateConfig({ MUSICBRAINZ_MIRROR_ENABLED: checked })}
              />
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div className="space-y-2">
                <Label>Mirror base URL</Label>
                <Input
                  value={config.MUSICBRAINZ_BASE_URL || ''}
                  onChange={(event) => updateConfig({ MUSICBRAINZ_BASE_URL: event.target.value })}
                  placeholder="https://musicbrainz.internal"
                />
              </div>
              <div className="space-y-2">
                <Label>Mirror label</Label>
                <Input
                  value={config.MUSICBRAINZ_MIRROR_NAME || ''}
                  onChange={(event) => updateConfig({ MUSICBRAINZ_MIRROR_NAME: event.target.value })}
                  placeholder="LAN mirror"
                />
              </div>
            </div>
            <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
              <Badge variant={runtime?.musicbrainz.mirror_enabled ? 'default' : 'outline'}>
                {runtime?.musicbrainz.mirror_enabled ? 'Mirror active' : 'Public MB'}
              </Badge>
              <Badge variant="outline">Effective target: {effectiveMbTarget}</Badge>
              <Badge variant="outline">Current rate: {runtime?.musicbrainz.current_rate_limit_per_sec ?? config.MB_MIRROR_QUEUE_RPS ?? 12} req/s</Badge>
              <Badge variant="outline">Queue pending: {runtime?.musicbrainz.queue_pending ?? 0}</Badge>
              <Badge variant="outline">Waiters: {runtime?.musicbrainz.queue_waiters ?? 0}</Badge>
            </div>
          </div>

          <div className="rounded-xl border border-border/60 bg-background/30 p-4 space-y-4">
            <div className="flex items-center justify-between gap-3">
              <div className="space-y-1">
                <div className="flex items-center gap-2 text-sm font-medium">
                  <Cpu className="h-4 w-4" />
                  Metadata workers
                </div>
                <p className="text-xs text-muted-foreground">
                  Keep file I/O local. Hybrid mode only offloads normalized metadata manifests; raw audio, OCR, tagging and materialization stay on the storage node.
                </p>
              </div>
              <Switch
                checked={Boolean(config.METADATA_QUEUE_ENABLED)}
                onCheckedChange={(checked) => updateConfig({ METADATA_QUEUE_ENABLED: checked })}
              />
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div className="space-y-2 rounded-lg border border-border/60 bg-background/20 p-3">
                <Label>Worker mode</Label>
                <Select
                  value={(config.METADATA_WORKER_MODE || 'local') as NonNullable<PMDAConfig['METADATA_WORKER_MODE']>}
                  onValueChange={(value: NonNullable<PMDAConfig['METADATA_WORKER_MODE']>) => updateConfig({ METADATA_WORKER_MODE: value })}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select mode" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="local">Local only</SelectItem>
                    <SelectItem value="hybrid">Hybrid metadata workers</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <NumericSetting
                label="Worker count"
                help="How many metadata workers can process manifests in parallel."
                value={config.METADATA_WORKER_COUNT}
                min={0}
                max={128}
                onChange={(value) => updateConfig({ METADATA_WORKER_COUNT: value })}
              />
              <NumericSetting
                label="Batch size"
                help="How many metadata jobs a worker can claim at once."
                value={config.METADATA_JOB_BATCH_SIZE}
                min={1}
                max={500}
                onChange={(value) => updateConfig({ METADATA_JOB_BATCH_SIZE: value })}
              />
            </div>
            <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
              <Badge variant={runtime?.metadata_workers.queue_enabled ? 'default' : 'outline'}>
                {runtime?.metadata_workers.queue_enabled ? 'Queue enabled' : 'Queue disabled'}
              </Badge>
              <Badge variant="outline">Queued: {runtime?.metadata_workers.queued ?? 0}</Badge>
              <Badge variant="outline">Running: {runtime?.metadata_workers.running ?? 0}</Badge>
              <Badge variant="outline">Completed: {runtime?.metadata_workers.completed ?? 0}</Badge>
              <Badge variant="outline">Failed: {runtime?.metadata_workers.failed ?? 0}</Badge>
            </div>
          </div>
        </div>

        <div className="rounded-xl border border-border/60 bg-background/30 p-4 space-y-4">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <h3 className="text-sm font-semibold text-foreground">Runtime auto-tune</h3>
              <p className="text-xs text-muted-foreground">
                Adapts local MusicBrainz throughput and gateway concurrency to the current host instead of hard-freezing one static rate forever.
              </p>
            </div>
            <Switch
              checked={Boolean(config.AUTO_TUNE_ENABLED)}
              onCheckedChange={(checked) => updateConfig({ AUTO_TUNE_ENABLED: checked })}
            />
          </div>
          <div className="grid grid-cols-1 xl:grid-cols-4 gap-3">
            <NumericSetting
              label="Interval (s)"
              help="How often PMDA re-evaluates throughput limits."
              value={config.AUTO_TUNE_INTERVAL_SEC}
              min={15}
              max={900}
              onChange={(value) => updateConfig({ AUTO_TUNE_INTERVAL_SEC: value })}
            />
            <NumericSetting
              label="MB min RPS"
              help="Lower bound for the local MusicBrainz mirror queue."
              value={config.AUTO_TUNE_MB_MIRROR_MIN_RPS}
              min={1}
              max={100}
              onChange={(value) => updateConfig({ AUTO_TUNE_MB_MIRROR_MIN_RPS: value })}
            />
            <NumericSetting
              label="MB max RPS"
              help="Upper bound for the local MusicBrainz mirror queue."
              value={config.AUTO_TUNE_MB_MIRROR_MAX_RPS}
              min={1}
              max={100}
              onChange={(value) => updateConfig({ AUTO_TUNE_MB_MIRROR_MAX_RPS: value })}
            />
            <NumericSetting
              label="Gateway inflight cap"
              help="Upper bound for simultaneous provider requests."
              value={config.AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP}
              min={1}
              max={256}
              onChange={(value) => updateConfig({ AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP: value })}
            />
          </div>
          <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
            <Badge variant={runtime?.auto_tune?.enabled ? 'default' : 'outline'}>
              {runtime?.auto_tune?.enabled ? 'Auto-tune active' : 'Auto-tune off'}
            </Badge>
            <Badge variant="outline">Last run: {formatAgo(runtime?.auto_tune?.last_run_at)}</Badge>
            <Badge variant="outline">Last change: {formatAgo(runtime?.auto_tune?.last_change_at)}</Badge>
            {runtime?.auto_tune?.last_reason ? <Badge variant="outline">{runtime.auto_tune.last_reason}</Badge> : null}
          </div>
        </div>

        <div className="rounded-xl border border-border/60 bg-background/30 p-4 space-y-4">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <h3 className="text-sm font-semibold text-foreground">Provider gateway</h3>
              <p className="text-xs text-muted-foreground">
                Shared cache and rate limiting for provider traffic. This is the safe way to accelerate metadata lookups without flooding Discogs, Last.fm or Bandcamp.
              </p>
            </div>
            <div className="flex items-center gap-2">
              <Badge variant={runtime?.provider_gateway.enabled ? 'default' : 'outline'}>
                {runtime?.provider_gateway.enabled ? 'Enabled' : 'Disabled'}
              </Badge>
              <Switch
                checked={Boolean(config.PROVIDER_GATEWAY_ENABLED)}
                onCheckedChange={(checked) => updateConfig({ PROVIDER_GATEWAY_ENABLED: checked })}
              />
            </div>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div className="flex items-center justify-between rounded-lg border border-border/60 bg-background/20 px-3 py-3">
              <div className="space-y-1 pr-4">
                <div className="text-sm font-medium">Gateway cache</div>
                <div className="text-[11px] text-muted-foreground">Reuse identical provider responses across workers.</div>
              </div>
              <Switch
                checked={Boolean(config.PROVIDER_GATEWAY_CACHE_ENABLED)}
                onCheckedChange={(checked) => updateConfig({ PROVIDER_GATEWAY_CACHE_ENABLED: checked })}
              />
            </div>
            <NumericSetting
              label="Max in-flight"
              help="Global simultaneous provider requests allowed through the gateway."
              value={config.PROVIDER_GATEWAY_MAX_INFLIGHT}
              min={1}
              max={256}
              onChange={(value) => updateConfig({ PROVIDER_GATEWAY_MAX_INFLIGHT: value })}
            />
            <div className="rounded-lg border border-border/60 bg-background/20 p-3 text-sm">
              <div className="font-medium text-foreground">Runtime</div>
              <div className="mt-2 space-y-1 text-xs text-muted-foreground">
                <div>Current in-flight: <span className="text-foreground">{runtime?.provider_gateway.inflight ?? 0}</span></div>
                <div>Peak in-flight: <span className="text-foreground">{runtime?.provider_gateway.max_inflight_observed ?? 0}</span></div>
                <div>Last refresh: <span className="text-foreground">{loading ? 'loading…' : 'live'}</span></div>
              </div>
            </div>
          </div>
          <div className="grid grid-cols-1 xl:grid-cols-3 gap-3">
            <NumericSetting
              label="Discogs RPM"
              help="Shared Discogs budget per minute."
              value={config.PROVIDER_GATEWAY_DISCOGS_RPM}
              min={1}
              max={600}
              onChange={(value) => updateConfig({ PROVIDER_GATEWAY_DISCOGS_RPM: value })}
            />
            <NumericSetting
              label="Last.fm RPM"
              help="Shared Last.fm budget per minute."
              value={config.PROVIDER_GATEWAY_LASTFM_RPM}
              min={1}
              max={1200}
              onChange={(value) => updateConfig({ PROVIDER_GATEWAY_LASTFM_RPM: value })}
            />
            <NumericSetting
              label="Bandcamp RPM"
              help="Shared Bandcamp budget per minute. Keep this conservative."
              value={config.PROVIDER_GATEWAY_BANDCAMP_RPM}
              min={1}
              max={240}
              onChange={(value) => updateConfig({ PROVIDER_GATEWAY_BANDCAMP_RPM: value })}
            />
          </div>
          <div className="grid grid-cols-1 xl:grid-cols-3 gap-3">
            {providerRows.map(({ provider, stats }) => (
              <div key={provider} className="rounded-lg border border-border/60 bg-background/20 p-3 text-sm">
                <div className="flex items-center justify-between gap-2">
                  <span className="font-medium capitalize text-foreground">{provider}</span>
                  <Badge variant="outline">{stats?.rpm_limit ?? 0} rpm</Badge>
                </div>
                <div className="mt-3 space-y-1 text-xs text-muted-foreground">
                  <div>Requests: <span className="text-foreground">{stats?.request_count ?? 0}</span></div>
                  <div>Network: <span className="text-foreground">{stats?.network_request_count ?? 0}</span></div>
                  <div>Cache hits: <span className="text-foreground">{stats?.cache_hits ?? 0}</span></div>
                  <div>Lookups: <span className="text-foreground">{stats?.lookup_request_count ?? 0}</span></div>
                  <div>Lookup network: <span className="text-foreground">{stats?.lookup_network_request_count ?? 0}</span></div>
                  <div>Lookup saved: <span className="text-foreground">{stats?.lookup_saved_count ?? 0}</span></div>
                  <div>Negative hits: <span className="text-foreground">{stats?.negative_cache_hits ?? 0}</span></div>
                  <div>Coalesced waits: <span className="text-foreground">{stats?.coalesced_waits ?? 0}</span></div>
                  <div>Lookup cache hits: <span className="text-foreground">{stats?.lookup_cache_hits ?? 0}</span></div>
                  <div>Lookup coalesced: <span className="text-foreground">{stats?.lookup_coalesced_waits ?? 0}</span></div>
                  <div>Lookup hit rate: <span className="text-foreground">{stats?.lookup_hit_rate ?? 0}%</span></div>
                  <div>HTTP / network lookup: <span className="text-foreground">{stats?.avg_network_requests_per_lookup ?? 0}</span></div>
                  <div>Hit rate: <span className="text-foreground">{stats?.cache_hit_rate ?? 0}%</span></div>
                  <div>Failures: <span className="text-foreground">{stats?.failure_count ?? 0}</span></div>
                  <div>Timeouts: <span className="text-foreground">{stats?.timeout_count ?? 0}</span></div>
                  <div>429s: <span className="text-foreground">{stats?.rate_limited_count ?? 0}</span></div>
                  <div>Avg latency: <span className="text-foreground">{stats?.avg_latency_ms ?? 0} ms</span></div>
                  <div>Last request: <span className="text-foreground">{formatAgo(stats?.last_request_at)}</span></div>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="rounded-xl border border-border/60 bg-background/20 p-4 space-y-3">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <h3 className="text-sm font-semibold text-foreground">Managed local runtimes</h3>
              <p className="text-xs text-muted-foreground">
                PMDA can provision or adopt a local MusicBrainz mirror and a local Ollama runtime. The wizard uses the same state machine; this panel is the post-setup control surface.
              </p>
            </div>
            <div className="flex items-center gap-2">
              <Badge variant={managedStatus?.preflight.available ? 'default' : 'outline'}>
                {managedStatus?.preflight.available ? 'Docker ready' : 'Docker unavailable'}
              </Badge>
              <Button type="button" variant="outline" size="sm" className="gap-2" onClick={() => void refreshRuntime()} disabled={loading}>
                {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                Refresh
              </Button>
            </div>
          </div>

          <div className="grid grid-cols-1 xl:grid-cols-2 gap-3">
            <div className="rounded-lg border border-border/60 bg-background/20 p-3 space-y-2">
              <Label>Managed config/runtime root</Label>
              <FolderBrowserInput
                value={String(config.MANAGED_RUNTIME_CONFIG_ROOT || '')}
                onChange={(path) => updateConfig({ MANAGED_RUNTIME_CONFIG_ROOT: path || '' })}
                placeholder="/mnt/user/appdata/pmda"
                selectLabel="Select managed config root"
              />
            </div>
            <div className="rounded-lg border border-border/60 bg-background/20 p-3 space-y-2">
              <Label>Managed data root</Label>
              <FolderBrowserInput
                value={String(config.MANAGED_RUNTIME_DATA_ROOT || '')}
                onChange={(path) => updateConfig({ MANAGED_RUNTIME_DATA_ROOT: path || '' })}
                placeholder="/mnt/user/data/pmda"
                selectLabel="Select managed data root"
              />
            </div>
          </div>

          <div className="grid grid-cols-1 xl:grid-cols-2 gap-3">
            <div className="rounded-xl border border-border/60 bg-background/30 p-4 space-y-3">
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                  <Database className="h-4 w-4" />
                  MusicBrainz local bundle
                </div>
                <Badge variant={isManagedBundleReady(managedMbBundle) ? 'default' : managedMbBundle?.state === 'failed' ? 'destructive' : 'outline'}>
                  {managedMbBundle?.state || 'absent'}
                </Badge>
              </div>
              <p className="text-xs text-muted-foreground">{managedMbBundle?.phase_message || 'No managed MusicBrainz bundle initialized yet.'}</p>
              {managedMbBundle?.last_error ? (
                <div className="flex items-start gap-2 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning-foreground">
                  <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                  <span>{managedMbBundle.last_error}</span>
                </div>
              ) : null}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-2 text-xs text-muted-foreground">
                <div>Mode: <span className="text-foreground">{managedMbBundle?.mode || 'absent'}</span></div>
                <div>URL: <span className="text-foreground">{managedMbBundle?.effective_url || '—'}</span></div>
                <div>Install root: <span className="text-foreground">{managedMbBundle?.install_root || '—'}</span></div>
                <div>Services: <span className="text-foreground">{managedMbBundle?.services?.length || 0}</span></div>
                <div>Last success: <span className="text-foreground">{formatManagedTimestamp(managedMbBundle?.update_state?.last_success_at)}</span></div>
                <div>Next update: <span className="text-foreground">{formatManagedTimestamp(managedMbBundle?.update_state?.next_planned_at)}</span></div>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div className="flex items-center justify-between rounded-lg border border-border/60 bg-background/20 px-3 py-3">
                  <div className="space-y-0.5 pr-4">
                    <div className="text-sm font-medium">Managed MB updates</div>
                    <div className="text-[11px] text-muted-foreground">Deferred automatically while scans are active.</div>
                  </div>
                  <Switch
                    checked={Boolean(config.MANAGED_MUSICBRAINZ_UPDATE_ENABLED ?? true)}
                    onCheckedChange={(checked) => updateConfig({ MANAGED_MUSICBRAINZ_UPDATE_ENABLED: checked })}
                  />
                </div>
                <NumericSetting
                  label="Reindex interval (hours)"
                  help="How often PMDA schedules managed MusicBrainz maintenance."
                  value={config.MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS}
                  min={1}
                  max={24 * 30}
                  onChange={(value) => updateConfig({ MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS: value })}
                />
              </div>
              <div className="flex flex-wrap gap-2">
                <Button type="button" variant="outline" size="sm" onClick={() => void bootstrapManagedBundle('musicbrainz_local')} disabled={managedBusy !== ''}>
                  {managedBusy === 'musicbrainz_local:bootstrap' ? <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" /> : null}
                  Bootstrap
                </Button>
                <Button type="button" variant="outline" size="sm" onClick={() => void runManagedAction('musicbrainz_local', 'refresh-health')} disabled={managedBusy !== ''}>
                  Refresh health
                </Button>
                <Button type="button" variant="outline" size="sm" onClick={() => void runManagedAction('musicbrainz_local', 'retry-update')} disabled={managedBusy !== ''}>
                  Run update now
                </Button>
                <Button type="button" variant="outline" size="sm" onClick={() => void runManagedAction('musicbrainz_local', 'restart')} disabled={managedBusy !== '' || managedMbBundle?.mode !== 'managed'}>
                  Restart
                </Button>
                <Button type="button" variant="outline" size="sm" onClick={() => void runManagedAction('musicbrainz_local', 'reset')} disabled={managedBusy !== '' || managedMbBundle?.mode !== 'managed'}>
                  Reset
                </Button>
              </div>
            </div>

            <div className="rounded-xl border border-border/60 bg-background/30 p-4 space-y-3">
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                  <Bot className="h-4 w-4" />
                  Ollama local bundle
                </div>
                <Badge variant={isManagedBundleReady(managedOllamaBundle) ? 'default' : managedOllamaBundle?.state === 'failed' ? 'destructive' : 'outline'}>
                  {managedOllamaBundle?.state || 'absent'}
                </Badge>
              </div>
              <p className="text-xs text-muted-foreground">{managedOllamaBundle?.phase_message || 'No managed Ollama runtime initialized yet.'}</p>
              {managedOllamaBundle?.last_error ? (
                <div className="flex items-start gap-2 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning-foreground">
                  <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                  <span>{managedOllamaBundle.last_error}</span>
                </div>
              ) : null}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-2 text-xs text-muted-foreground">
                <div>Mode: <span className="text-foreground">{managedOllamaBundle?.mode || 'absent'}</span></div>
                <div>URL: <span className="text-foreground">{managedOllamaBundle?.effective_url || '—'}</span></div>
                <div>Models: <span className="text-foreground">{formatManagedModels(managedOllamaBundle)}</span></div>
                <div>Services: <span className="text-foreground">{managedOllamaBundle?.services?.length || 0}</span></div>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div className="space-y-2">
                  <Label>Fast model</Label>
                  <Input value={String(config.OLLAMA_MODEL || '')} onChange={(event) => updateConfig({ OLLAMA_MODEL: event.target.value })} placeholder="qwen3:4b" />
                </div>
                <div className="space-y-2">
                  <Label>Hard model</Label>
                  <Input value={String(config.OLLAMA_COMPLEX_MODEL || '')} onChange={(event) => updateConfig({ OLLAMA_COMPLEX_MODEL: event.target.value })} placeholder="qwen3:14b" />
                </div>
              </div>
              <div className="flex flex-wrap gap-2">
                <Button type="button" variant="outline" size="sm" onClick={() => void bootstrapManagedBundle('ollama_local')} disabled={managedBusy !== ''}>
                  {managedBusy === 'ollama_local:bootstrap' ? <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" /> : null}
                  Bootstrap
                </Button>
                <Button type="button" variant="outline" size="sm" onClick={() => void runManagedAction('ollama_local', 'refresh-health')} disabled={managedBusy !== ''}>
                  Refresh health
                </Button>
                <Button type="button" variant="outline" size="sm" onClick={() => void runManagedAction('ollama_local', 'pull-model', { model: String(config.OLLAMA_MODEL || '').trim() || 'qwen3:4b' })} disabled={managedBusy !== ''}>
                  Pull fast model
                </Button>
                <Button type="button" variant="outline" size="sm" onClick={() => void runManagedAction('ollama_local', 'restart')} disabled={managedBusy !== '' || managedOllamaBundle?.mode !== 'managed'}>
                  Restart
                </Button>
                <Button type="button" variant="outline" size="sm" onClick={() => void runManagedAction('ollama_local', 'reset')} disabled={managedBusy !== '' || managedOllamaBundle?.mode !== 'managed'}>
                  Reset
                </Button>
              </div>
            </div>
          </div>
        </div>

        <div className="rounded-xl border border-border/60 bg-background/20 p-4 space-y-3">
          <div className="flex items-center justify-between gap-3">
            <div className="space-y-1">
              <h3 className="text-sm font-semibold text-foreground">Effective behavior</h3>
              <p className="text-xs text-muted-foreground">
                PMDA keeps filesystem work local. OCR stays local. AI remains useful only as an ambiguity resolver, not as the main scan engine.
              </p>
            </div>
            <Button type="button" variant="outline" size="sm" className="gap-2" onClick={() => void refreshRuntime()} disabled={loading}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
              Refresh runtime
            </Button>
          </div>
          {error ? (
            <div className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
              {error}
            </div>
          ) : null}
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-2 text-sm">
            <div><span className="text-muted-foreground">Orchestrator:</span> <span className="text-foreground">{runtime?.pipeline.local_orchestrator ? 'Local node near storage' : 'externalized'}</span></div>
            <div><span className="text-muted-foreground">Materialization:</span> <span className="text-foreground">{runtime?.pipeline.materialization_local ? 'Local only' : 'remote'}</span></div>
            <div><span className="text-muted-foreground">OCR execution:</span> <span className="text-foreground">{runtime?.pipeline.ocr_execution || 'local'}</span></div>
            <div><span className="text-muted-foreground">AI policy:</span> <span className="text-foreground">{runtime?.pipeline.ai_mode || 'ambiguous_only'}</span></div>
            <div><span className="text-muted-foreground">Scan threads:</span> <span className="text-foreground">{runtime?.pipeline.scan_threads ?? config.SCAN_THREADS ?? 'auto'}</span></div>
            <div><span className="text-muted-foreground">FFprobe pool:</span> <span className="text-foreground">{runtime?.pipeline.ffprobe_pool_size ?? config.FFPROBE_POOL_SIZE ?? 8}</span></div>
            <div><span className="text-muted-foreground">Current phase:</span> <span className="text-foreground">{runtime?.stage_rates.phase || 'idle'}</span></div>
            <div><span className="text-muted-foreground">Runtime:</span> <span className="text-foreground">{runtime?.stage_rates.runtime_sec ?? 0}s</span></div>
            <div><span className="text-muted-foreground">Filesystem rate:</span> <span className="text-foreground">{formatRate(runtime?.stage_rates.filesystem_entries_per_hour)}</span></div>
            <div><span className="text-muted-foreground">Audio rate:</span> <span className="text-foreground">{formatRate(runtime?.stage_rates.audio_files_per_hour)}</span></div>
            <div><span className="text-muted-foreground">Albums processed:</span> <span className="text-foreground">{formatRate(runtime?.stage_rates.albums_processed_per_hour)}</span></div>
            <div><span className="text-muted-foreground">Albums published:</span> <span className="text-foreground">{formatRate(runtime?.stage_rates.albums_published_per_hour)}</span></div>
            <div><span className="text-muted-foreground">Artists processed:</span> <span className="text-foreground">{formatRate(runtime?.stage_rates.artists_processed_per_hour)}</span></div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
