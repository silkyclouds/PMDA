import { useCallback, useEffect, useMemo, useState } from 'react';
import { AlertTriangle, CalendarClock, ChevronDown, Loader2, PauseCircle, PlayCircle, RotateCcw, Save, Zap } from 'lucide-react';
import { toast } from 'sonner';

import * as api from '@/lib/api';
import type { PMDAConfig, SchedulerRule, TaskJobType, TaskScope } from '@/lib/api';
import { Button } from '@/components/ui/button';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';

const DAY_OPTIONS = [
  { value: '0', label: 'Monday' },
  { value: '1', label: 'Tuesday' },
  { value: '2', label: 'Wednesday' },
  { value: '3', label: 'Thursday' },
  { value: '4', label: 'Friday' },
  { value: '5', label: 'Saturday' },
  { value: '6', label: 'Sunday' },
];

const JOB_OPTIONS: Array<{ value: TaskJobType; label: string }> = [
  { value: 'scan_changed', label: 'Scan changed' },
  { value: 'scan_full', label: 'Scan full' },
  { value: 'enrich_batch', label: 'Enrich batch' },
  { value: 'dedupe', label: 'Dedupe' },
  { value: 'incomplete_move', label: 'Incomplete move' },
  { value: 'export', label: 'Export' },
  { value: 'player_sync', label: 'Player sync' },
];

const QUICK_POST_JOBS: Array<{ jobType: TaskJobType; label: string; description: string; defaultScope: TaskScope }> = [
  { jobType: 'enrich_batch', label: 'Enrichment', description: 'Fetch extra metadata/covers after scan.', defaultScope: 'both' },
  { jobType: 'dedupe', label: 'Dedupe', description: 'Move duplicate loser albums.', defaultScope: 'both' },
  { jobType: 'incomplete_move', label: 'Incomplete move', description: 'Move incomplete albums to target folder.', defaultScope: 'new' },
  { jobType: 'export', label: 'Export', description: 'Rebuild exported clean library.', defaultScope: 'both' },
  { jobType: 'player_sync', label: 'Player sync', description: 'Trigger Plex/Jellyfin/Navidrome refresh.', defaultScope: 'both' },
];

interface SchedulerSettingsProps {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
}

function defaultRule(jobType: TaskJobType): SchedulerRule {
  if (jobType === 'scan_changed') {
    return {
      job_type: 'scan_changed',
      enabled: true,
      trigger_type: 'interval',
      interval_min: 20,
      scope: 'new',
      post_scan_chain: false,
      priority: 10,
      max_concurrency: 1,
      days_of_week: '',
      time_local: '',
    };
  }
  if (jobType === 'scan_full') {
    return {
      job_type: 'scan_full',
      enabled: true,
      trigger_type: 'weekly',
      interval_min: null,
      days_of_week: '6',
      time_local: '02:00',
      scope: 'full',
      post_scan_chain: false,
      priority: 20,
      max_concurrency: 1,
    };
  }
  return {
    job_type: jobType,
    enabled: true,
    trigger_type: 'interval',
    interval_min: 30,
    days_of_week: '',
    time_local: '',
    scope: jobType === 'incomplete_move' ? 'new' : 'both',
    post_scan_chain: true,
    priority: 50,
    max_concurrency: 1,
  };
}

function firstDay(daysRaw?: string): string {
  const raw = String(daysRaw || '').trim();
  if (!raw) return '6';
  const first = raw.split(',').map((x) => x.trim()).find(Boolean);
  if (!first) return '6';
  return DAY_OPTIONS.some((d) => d.value === first) ? first : '6';
}

export function SchedulerSettings({ config, updateConfig }: SchedulerSettingsProps) {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [paused, setPaused] = useState(false);
  const [rules, setRules] = useState<SchedulerRule[]>([]);
  const [manualJob, setManualJob] = useState<TaskJobType>('scan_changed');
  const [manualScope, setManualScope] = useState<TaskScope>('new');
  const [manualRunning, setManualRunning] = useState(false);
  const [watcherStatus, setWatcherStatus] = useState<api.FilesWatcherStatus | null>(null);
  const [watcherRestarting, setWatcherRestarting] = useState(false);
  const [advancedOpen, setAdvancedOpen] = useState(false);

  const loadRules = useCallback(async () => {
    setLoading(true);
    try {
      const [rulesRes, statusRes, watcherRes] = await Promise.all([
        api.getSchedulerRules(),
        api.getSchedulerJobsStatus(),
        api.getFilesWatcherStatus(),
      ]);
      setRules(Array.isArray(rulesRes.rules) ? rulesRes.rules : []);
      setPaused(Boolean(statusRes.paused));
      setWatcherStatus(watcherRes);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to load scheduler settings');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadRules();
  }, [loadRules]);

  const changedRule = useMemo(
    () => rules.find((r) => r.job_type === 'scan_changed'),
    [rules],
  );

  const fullRule = useMemo(
    () => rules.find((r) => r.job_type === 'scan_full'),
    [rules],
  );

  const patchFirstRule = useCallback((jobType: TaskJobType, patch: Partial<SchedulerRule>) => {
    setRules((prev) => {
      const idx = prev.findIndex((r) => r.job_type === jobType);
      if (idx < 0) return [...prev, { ...defaultRule(jobType), ...patch }];
      const next = [...prev];
      next[idx] = { ...next[idx], ...patch };
      return next;
    });
  }, []);

  const patchAllRulesForJob = useCallback((jobType: TaskJobType, patch: Partial<SchedulerRule>) => {
    setRules((prev) => {
      const hasAny = prev.some((r) => r.job_type === jobType);
      if (!hasAny) return [...prev, { ...defaultRule(jobType), ...patch }];
      return prev.map((r) => (r.job_type === jobType ? { ...r, ...patch } : r));
    });
  }, []);

  const saveRules = useCallback(async () => {
    setSaving(true);
    try {
      const payload = rules.map((rule) => {
        const trigger = rule.trigger_type === 'weekly' ? 'weekly' : 'interval';
        const cleaned: SchedulerRule = {
          ...rule,
          trigger_type: trigger,
          enabled: Boolean(rule.enabled),
          interval_min: trigger === 'interval' ? Math.max(1, Math.min(24 * 60, Number(rule.interval_min || 20))) : null,
          days_of_week: trigger === 'weekly' ? firstDay(rule.days_of_week) : '',
          time_local: trigger === 'weekly' ? String(rule.time_local || '02:00') : '',
          scope: (['new', 'full', 'both'].includes(String(rule.scope)) ? rule.scope : 'both') as TaskScope,
          post_scan_chain: Boolean(rule.post_scan_chain),
          priority: Math.max(1, Math.min(999, Number(rule.priority || 50))),
          max_concurrency: Math.max(1, Math.min(8, Number(rule.max_concurrency || 1))),
        };
        return cleaned;
      });
      const res = await api.saveSchedulerRules(payload);
      setRules(Array.isArray(res.rules) ? res.rules : payload);
      toast.success('Scheduler rules saved');
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to save scheduler rules');
    } finally {
      setSaving(false);
    }
  }, [rules]);

  const togglePause = useCallback(async () => {
    try {
      if (paused) {
        const res = await api.resumeSchedulerJobs();
        setPaused(Boolean(res.paused));
        toast.success('Scheduler resumed');
      } else {
        const res = await api.pauseSchedulerJobs();
        setPaused(Boolean(res.paused));
        toast.success('Scheduler paused');
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to update scheduler state');
    }
  }, [paused]);

  const runManualJob = useCallback(async () => {
    setManualRunning(true);
    try {
      const res = await api.runSchedulerJob({ job_type: manualJob, scope: manualScope, source: 'manual' });
      if (res.status === 'started') {
        toast.success('Job started', { description: `${manualJob} (${manualScope})` });
      } else {
        toast.info('Job blocked', { description: res.message || 'Already running or currently blocked.' });
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to run manual job');
    } finally {
      setManualRunning(false);
    }
  }, [manualJob, manualScope]);

  const restartWatcher = useCallback(async () => {
    setWatcherRestarting(true);
    try {
      await api.restartFilesWatcher();
      toast.success('Watcher restart requested');
      setTimeout(() => {
        void loadRules();
      }, 500);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to restart watcher');
    } finally {
      setWatcherRestarting(false);
    }
  }, [loadRules]);

  const watcherDegraded = useMemo(() => {
    if (!watcherStatus) return false;
    if (!watcherStatus.enabled) return false;
    if (!watcherStatus.available) return true;
    return !watcherStatus.running;
  }, [watcherStatus]);

  const applyRecommendedDefaults = useCallback(() => {
    setRules([
      defaultRule('scan_changed'),
      defaultRule('scan_full'),
      { ...defaultRule('enrich_batch'), enabled: true, post_scan_chain: true, scope: 'both' },
      { ...defaultRule('dedupe'), enabled: true, post_scan_chain: true, scope: 'both' },
      { ...defaultRule('incomplete_move'), enabled: true, post_scan_chain: true, scope: 'new' },
      { ...defaultRule('export'), enabled: false, post_scan_chain: true, scope: 'both' },
      { ...defaultRule('player_sync'), enabled: false, post_scan_chain: true, scope: 'both' },
    ]);
    updateConfig({ PIPELINE_POST_SCAN_ASYNC: true });
    toast.success('Recommended defaults applied. Save scheduler rules to persist.');
  }, [updateConfig]);

  if (loading) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="w-4 h-4 animate-spin" />
        Loading scheduler rules…
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="rounded-lg border border-border bg-muted/20 p-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <CalendarClock className="w-4 h-4 text-primary" />
              <Label>Recommended defaults</Label>
            </div>
            <p className="text-xs text-muted-foreground">
              Changed-only every <span className="font-medium text-foreground">20 min</span>, full scan every
              <span className="font-medium text-foreground"> Sunday at 02:00</span>, post-scan background mode ON.
            </p>
          </div>
          <Button type="button" variant="outline" size="sm" onClick={applyRecommendedDefaults}>
            Apply defaults
          </Button>
        </div>
      </div>

      <div className="flex items-start justify-between gap-4 rounded-lg border border-border p-4">
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <CalendarClock className="w-4 h-4 text-primary" />
            <Label>Hybrid orchestration</Label>
          </div>
          <p className="text-xs text-muted-foreground">
            Queue post-scan tasks in background for faster time-to-first-results.
          </p>
        </div>
        <Switch
          checked={Boolean(config.PIPELINE_POST_SCAN_ASYNC ?? true)}
          onCheckedChange={(checked) => updateConfig({ PIPELINE_POST_SCAN_ASYNC: Boolean(checked) })}
        />
      </div>

      <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
        <div className="space-y-2 rounded-lg border border-border p-4">
          <Label>Changed-only scan interval (minutes)</Label>
          <Input
            type="number"
            min={1}
            max={1440}
            value={Math.max(1, Number(changedRule?.interval_min || 20))}
            onChange={(e) => patchFirstRule('scan_changed', {
              trigger_type: 'interval',
              interval_min: Math.max(1, Math.min(1440, Number(e.target.value) || 20)),
            })}
          />
          <p className="text-xs text-muted-foreground">Recommended: every 20 minutes.</p>
        </div>

        <div className="space-y-2 rounded-lg border border-border p-4">
          <Label>Full scan weekly</Label>
          <div className="grid grid-cols-2 gap-2">
            <Select
              value={firstDay(fullRule?.days_of_week)}
              onValueChange={(value) => patchFirstRule('scan_full', {
                trigger_type: 'weekly',
                days_of_week: value,
              })}
            >
              <SelectTrigger>
                <SelectValue placeholder="Day" />
              </SelectTrigger>
              <SelectContent>
                {DAY_OPTIONS.map((day) => (
                  <SelectItem key={day.value} value={day.value}>
                    {day.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Input
              type="time"
              value={String(fullRule?.time_local || '02:00')}
              onChange={(e) => patchFirstRule('scan_full', {
                trigger_type: 'weekly',
                time_local: e.target.value || '02:00',
              })}
            />
          </div>
          <p className="text-xs text-muted-foreground">Recommended: Sunday at 02:00.</p>
        </div>
      </div>

      <div className="space-y-2 rounded-lg border border-border p-4">
        <Label>After each scan, run these jobs</Label>
        <p className="text-xs text-muted-foreground">
          Keep this simple: enable only what you want PMDA to run automatically after scans.
        </p>
        <div className="space-y-2">
          {QUICK_POST_JOBS.map((entry) => {
            const match = rules.find((r) => r.job_type === entry.jobType);
            const enabled = Boolean(match?.enabled);
            return (
              <div key={entry.jobType} className="flex items-start justify-between gap-3 rounded-md bg-muted/40 p-3">
                <div className="space-y-0.5">
                  <p className="text-sm font-medium">{entry.label}</p>
                  <p className="text-xs text-muted-foreground">{entry.description}</p>
                  <p className="text-[11px] text-muted-foreground">Default scope: {entry.defaultScope}</p>
                </div>
                <Switch
                  checked={enabled}
                  onCheckedChange={(checked) => patchAllRulesForJob(entry.jobType, {
                    enabled: Boolean(checked),
                    post_scan_chain: Boolean(checked),
                    scope: entry.defaultScope,
                  })}
                />
              </div>
            );
          })}
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        <Button type="button" onClick={saveRules} disabled={saving} className="gap-2">
          {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
          Save scheduler
        </Button>
        <span className="text-xs text-muted-foreground">Advanced options are available below.</span>
      </div>

      <Collapsible open={advancedOpen} onOpenChange={setAdvancedOpen}>
        <div className="rounded-lg border border-border/70 bg-muted/20">
          <CollapsibleTrigger asChild>
            <Button type="button" variant="ghost" className="w-full justify-between rounded-none px-3 py-2 text-left">
              <span className="text-sm font-medium">Advanced scheduler options</span>
              <ChevronDown className={`w-4 h-4 transition-transform ${advancedOpen ? 'rotate-180' : ''}`} />
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent>
            <div className="space-y-4 border-t border-border/60 p-3">
              <div className="space-y-3 rounded-lg border border-border p-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="space-y-1">
                    <Label>Files watcher</Label>
                    <p className="text-xs text-muted-foreground">
                      Detects filesystem deltas for fast changed-only scans.
                    </p>
                  </div>
                  <Button type="button" variant="outline" onClick={restartWatcher} disabled={watcherRestarting} className="gap-2">
                    {watcherRestarting ? <Loader2 className="w-4 h-4 animate-spin" /> : <RotateCcw className="w-4 h-4" />}
                    Restart watcher
                  </Button>
                </div>
                {watcherStatus ? (
                  <div className="grid grid-cols-1 gap-2 text-xs text-muted-foreground md:grid-cols-3">
                    <div>Running: <span className="text-foreground">{String(Boolean(watcherStatus.running))}</span></div>
                    <div>Available: <span className="text-foreground">{String(Boolean(watcherStatus.available))}</span></div>
                    <div>Dirty events: <span className="text-foreground">{Number(watcherStatus.dirty_count || 0).toLocaleString()}</span></div>
                    <div>Reason: <span className="text-foreground">{String(watcherStatus.reason || 'n/a')}</span></div>
                    <div>Failures: <span className="text-foreground">{Number(watcherStatus.consecutive_failures || 0)}</span></div>
                    <div>Last restart: <span className="text-foreground">{watcherStatus.last_restart_duration_ms != null ? `${Math.round(Number(watcherStatus.last_restart_duration_ms || 0))} ms` : 'n/a'}</span></div>
                  </div>
                ) : (
                  <p className="text-xs text-muted-foreground">Watcher status unavailable.</p>
                )}
                {watcherDegraded ? (
                  <div className="flex items-center gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-700 dark:text-amber-300">
                    <AlertTriangle className="h-4 w-4" />
                    Watcher degraded: changed-only uses discovery fallback.
                  </div>
                ) : null}
              </div>

              <div className="flex flex-wrap items-center gap-2">
                <Button type="button" variant="outline" onClick={togglePause} className="gap-2">
                  {paused ? <PlayCircle className="w-4 h-4" /> : <PauseCircle className="w-4 h-4" />}
                  {paused ? 'Resume scheduler' : 'Pause scheduler'}
                </Button>
                <Button type="button" variant="outline" onClick={loadRules} className="gap-2">
                  <RotateCcw className="w-4 h-4" />
                  Reload
                </Button>
              </div>

              <div className="space-y-3 rounded-lg border border-border p-4">
                <div className="flex items-center justify-between">
                  <Label>Advanced rules</Label>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={() => setRules((prev) => [...prev, defaultRule('dedupe')])}
                  >
                    Add rule
                  </Button>
                </div>
                <div className="space-y-2">
                  {rules.map((rule, index) => (
                    <div key={`${rule.rule_id || 'new'}-${index}`} className="rounded-md border border-border bg-muted/30 p-3">
                      <div className="grid grid-cols-1 gap-2 md:grid-cols-6">
                        <Select
                          value={rule.job_type}
                          onValueChange={(value: TaskJobType) => setRules((prev) => prev.map((item, idx) => idx === index ? { ...item, job_type: value } : item))}
                        >
                          <SelectTrigger><SelectValue /></SelectTrigger>
                          <SelectContent>
                            {JOB_OPTIONS.map((job) => (
                              <SelectItem key={job.value} value={job.value}>{job.label}</SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        <Select
                          value={rule.trigger_type}
                          onValueChange={(value: 'interval' | 'weekly') => setRules((prev) => prev.map((item, idx) => idx === index ? { ...item, trigger_type: value } : item))}
                        >
                          <SelectTrigger><SelectValue /></SelectTrigger>
                          <SelectContent>
                            <SelectItem value="interval">interval</SelectItem>
                            <SelectItem value="weekly">weekly</SelectItem>
                          </SelectContent>
                        </Select>
                        {rule.trigger_type === 'interval' ? (
                          <Input
                            type="number"
                            min={1}
                            max={1440}
                            value={Math.max(1, Number(rule.interval_min || 20))}
                            onChange={(e) => setRules((prev) => prev.map((item, idx) => idx === index ? { ...item, interval_min: Math.max(1, Math.min(1440, Number(e.target.value) || 20)) } : item))}
                          />
                        ) : (
                          <Select
                            value={firstDay(rule.days_of_week)}
                            onValueChange={(value) => setRules((prev) => prev.map((item, idx) => idx === index ? { ...item, days_of_week: value } : item))}
                          >
                            <SelectTrigger><SelectValue /></SelectTrigger>
                            <SelectContent>
                              {DAY_OPTIONS.map((day) => (
                                <SelectItem key={day.value} value={day.value}>{day.label}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        )}
                        {rule.trigger_type === 'weekly' ? (
                          <Input
                            type="time"
                            value={String(rule.time_local || '02:00')}
                            onChange={(e) => setRules((prev) => prev.map((item, idx) => idx === index ? { ...item, time_local: e.target.value || '02:00' } : item))}
                          />
                        ) : (
                          <Select
                            value={rule.scope}
                            onValueChange={(value: TaskScope) => setRules((prev) => prev.map((item, idx) => idx === index ? { ...item, scope: value } : item))}
                          >
                            <SelectTrigger><SelectValue /></SelectTrigger>
                            <SelectContent>
                              <SelectItem value="new">new</SelectItem>
                              <SelectItem value="full">full</SelectItem>
                              <SelectItem value="both">both</SelectItem>
                            </SelectContent>
                          </Select>
                        )}
                        <Input
                          type="number"
                          min={1}
                          max={999}
                          value={Math.max(1, Number(rule.priority || 50))}
                          onChange={(e) => setRules((prev) => prev.map((item, idx) => idx === index ? { ...item, priority: Math.max(1, Math.min(999, Number(e.target.value) || 50)) } : item))}
                        />
                        <div className="flex items-center justify-between gap-2">
                          <Switch
                            checked={Boolean(rule.enabled)}
                            onCheckedChange={(checked) => setRules((prev) => prev.map((item, idx) => idx === index ? { ...item, enabled: Boolean(checked) } : item))}
                          />
                          <Button
                            type="button"
                            variant="ghost"
                            size="sm"
                            onClick={() => setRules((prev) => prev.filter((_, idx) => idx !== index))}
                          >
                            Remove
                          </Button>
                        </div>
                      </div>
                      <div className="mt-2 grid grid-cols-2 gap-2 md:grid-cols-4">
                        <div className="space-y-1">
                          <Label className="text-xs">Scope</Label>
                          <Select
                            value={rule.scope}
                            onValueChange={(value: TaskScope) => setRules((prev) => prev.map((item, idx) => idx === index ? { ...item, scope: value } : item))}
                          >
                            <SelectTrigger><SelectValue /></SelectTrigger>
                            <SelectContent>
                              <SelectItem value="new">new</SelectItem>
                              <SelectItem value="full">full</SelectItem>
                              <SelectItem value="both">both</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="space-y-1">
                          <Label className="text-xs">Max concurrency</Label>
                          <Input
                            type="number"
                            min={1}
                            max={8}
                            value={Math.max(1, Number(rule.max_concurrency || 1))}
                            onChange={(e) => setRules((prev) => prev.map((item, idx) => idx === index ? { ...item, max_concurrency: Math.max(1, Math.min(8, Number(e.target.value) || 1)) } : item))}
                          />
                        </div>
                        <div className="col-span-2 flex items-center justify-between rounded-md border border-border bg-background px-3 py-2">
                          <span className="text-xs text-muted-foreground">Enable post-scan chain</span>
                          <Switch
                            checked={Boolean(rule.post_scan_chain)}
                            onCheckedChange={(checked) => setRules((prev) => prev.map((item, idx) => idx === index ? { ...item, post_scan_chain: Boolean(checked) } : item))}
                          />
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="space-y-3 rounded-lg border border-border p-4">
                <div className="flex items-center gap-2">
                  <Zap className="w-4 h-4 text-primary" />
                  <Label>Run job now</Label>
                </div>
                <div className="grid grid-cols-1 gap-2 md:grid-cols-3">
                  <Select value={manualJob} onValueChange={(value: TaskJobType) => setManualJob(value)}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {JOB_OPTIONS.map((job) => (
                        <SelectItem key={job.value} value={job.value}>{job.label}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <Select value={manualScope} onValueChange={(value: TaskScope) => setManualScope(value)}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="new">new</SelectItem>
                      <SelectItem value="full">full</SelectItem>
                      <SelectItem value="both">both</SelectItem>
                    </SelectContent>
                  </Select>
                  <Button type="button" onClick={runManualJob} disabled={manualRunning} className="gap-2">
                    {manualRunning ? <Loader2 className="w-4 h-4 animate-spin" /> : <PlayCircle className="w-4 h-4" />}
                    Run now
                  </Button>
                </div>
              </div>
            </div>
          </CollapsibleContent>
        </div>
      </Collapsible>
    </div>
  );
}
