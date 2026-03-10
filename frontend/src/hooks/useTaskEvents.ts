import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useRef } from 'react';
import { toast } from 'sonner';
import { getConfig, getTaskEvents, type TaskEvent, type TaskJobType } from '@/lib/api';

const JOB_LABELS: Record<TaskJobType, string> = {
  scan_changed: 'Changed scan',
  scan_full: 'Full scan',
  enrich_batch: 'Enrichment',
  dedupe: 'Dedupe',
  incomplete_move: 'Incomplete move',
  export: 'Export',
  player_sync: 'Player sync',
};

function asBool(value: unknown, fallback: boolean): boolean {
  if (typeof value === 'boolean') return value;
  if (value == null) return fallback;
  const raw = String(value).trim().toLowerCase();
  if (!raw) return fallback;
  return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on';
}

function asInt(value: unknown, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.round(n);
}

function pickNumber(source: Record<string, unknown>, key: string): number | null {
  const val = source?.[key];
  const n = Number(val);
  if (!Number.isFinite(n)) return null;
  return n;
}

function formatTaskDescription(event: TaskEvent): string {
  const metrics = (event.metrics && typeof event.metrics === 'object' ? event.metrics : {}) as Record<string, unknown>;
  const baseMessage = String(event.message || '').trim();
  if (event.status === 'failed') {
    const err = String(event.error || '').trim();
    return err || baseMessage || 'Task failed.';
  }
  if (event.job_type === 'dedupe') {
    const moved = pickNumber(metrics, 'moved');
    const saved = pickNumber(metrics, 'space_saved_mb');
    const parts: string[] = [];
    if (moved != null) parts.push(`${Math.max(0, Math.round(moved)).toLocaleString()} moved`);
    if (saved != null) parts.push(`${Math.max(0, Math.round(saved)).toLocaleString()} MB saved`);
    if (parts.length > 0) return parts.join(' • ');
  }
  if (event.job_type === 'incomplete_move') {
    const moved = pickNumber(metrics, 'moved');
    if (moved != null) return `${Math.max(0, Math.round(moved)).toLocaleString()} album(s) moved`;
  }
  if (event.job_type === 'export') {
    const done = pickNumber(metrics, 'tracks_done');
    const total = pickNumber(metrics, 'total_tracks');
    if (done != null && total != null && total > 0) {
      return `${Math.max(0, Math.round(done)).toLocaleString()} / ${Math.max(0, Math.round(total)).toLocaleString()} tracks`;
    }
  }
  if (event.job_type === 'scan_changed' || event.job_type === 'scan_full') {
    const albums = pickNumber(metrics, 'albums_scanned');
    const groups = pickNumber(metrics, 'duplicate_groups_count');
    const parts: string[] = [];
    if (albums != null) parts.push(`${Math.max(0, Math.round(albums)).toLocaleString()} albums`);
    if (groups != null) parts.push(`${Math.max(0, Math.round(groups)).toLocaleString()} duplicate groups`);
    if (parts.length > 0) return parts.join(' • ');
  }
  return baseMessage || 'Task finished.';
}

function isJobEnabled(event: TaskEvent, config: Record<string, unknown> | undefined): boolean {
  if (!config) return true;
  const byJob: Partial<Record<TaskJobType, boolean>> = {
    scan_changed: asBool(config.TASK_NOTIFY_SCAN_CHANGED, true),
    scan_full: asBool(config.TASK_NOTIFY_SCAN_FULL, true),
    enrich_batch: asBool(config.TASK_NOTIFY_ENRICH_BATCH, true),
    dedupe: asBool(config.TASK_NOTIFY_DEDUPE, true),
    incomplete_move: asBool(config.TASK_NOTIFY_INCOMPLETE_MOVE, true),
    export: asBool(config.TASK_NOTIFY_EXPORT, true),
    player_sync: asBool(config.TASK_NOTIFY_PLAYER_SYNC, true),
  };
  return byJob[event.job_type] ?? true;
}

interface UseTaskEventsOptions {
  pollIntervalMs?: number;
  enabled?: boolean;
}

export function useTaskEvents(options: UseTaskEventsOptions = {}): void {
  const pollIntervalMs = Math.max(1000, options.pollIntervalMs ?? 3000);
  const enabled = options.enabled ?? true;
  const lastIdRef = useRef<number | null>(null);
  const seenEventIdsRef = useRef<Set<number>>(new Set());
  const lastToastAtRef = useRef<Map<string, number>>(new Map());

  const { data: config } = useQuery({
    queryKey: ['task-events-config'],
    queryFn: getConfig,
    enabled,
    staleTime: 10_000,
    refetchInterval: 30_000,
    retry: 1,
  });

  const settings = useMemo(() => {
    const cfg = (config || {}) as Record<string, unknown>;
    return {
      enabled: asBool(cfg.TASK_NOTIFICATIONS_ENABLED, true),
      success: asBool(cfg.TASK_NOTIFICATIONS_SUCCESS, true),
      failure: asBool(cfg.TASK_NOTIFICATIONS_FAILURE, true),
      silentInteractive: asBool(cfg.TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN, false),
      cooldownSec: Math.max(0, Math.min(3600, asInt(cfg.TASK_NOTIFICATIONS_COOLDOWN_SEC, 20))),
      config: cfg,
    };
  }, [config]);

  const settingsSignature = `${settings.enabled}-${settings.success}-${settings.failure}-${settings.silentInteractive}-${settings.cooldownSec}-${String((config as Record<string, unknown> | undefined)?.TASK_NOTIFY_SCAN_CHANGED)}-${String((config as Record<string, unknown> | undefined)?.TASK_NOTIFY_SCAN_FULL)}-${String((config as Record<string, unknown> | undefined)?.TASK_NOTIFY_ENRICH_BATCH)}-${String((config as Record<string, unknown> | undefined)?.TASK_NOTIFY_DEDUPE)}-${String((config as Record<string, unknown> | undefined)?.TASK_NOTIFY_INCOMPLETE_MOVE)}-${String((config as Record<string, unknown> | undefined)?.TASK_NOTIFY_EXPORT)}-${String((config as Record<string, unknown> | undefined)?.TASK_NOTIFY_PLAYER_SYNC)}`;

  useEffect(() => {
    if (!enabled) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const scheduleNext = () => {
      if (cancelled) return;
      timer = setTimeout(runPoll, pollIntervalMs);
    };

    const notifyForEvent = (event: TaskEvent) => {
      if (!settings.enabled) return;
      if (!isJobEnabled(event, settings.config)) return;
      if (settings.silentInteractive && String(event.source || '').trim().toLowerCase() === 'interactive') return;
      if (event.status === 'completed' && !settings.success) return;
      if (event.status === 'failed' && !settings.failure) return;
      if (event.status !== 'completed' && event.status !== 'failed') return;

      const now = Date.now();
      const cooldownKey = `${event.job_type}:${event.status}`;
      const lastToastAt = lastToastAtRef.current.get(cooldownKey) || 0;
      if (settings.cooldownSec > 0 && now - lastToastAt < settings.cooldownSec * 1000) return;
      lastToastAtRef.current.set(cooldownKey, now);

      const label = JOB_LABELS[event.job_type] || event.job_type;
      const title = event.status === 'failed' ? `${label} failed` : `${label} finished`;
      const description = formatTaskDescription(event);
      if (event.status === 'failed') toast.error(title, { description });
      else toast.success(title, { description });
    };

    const runPoll = async () => {
      try {
        if (lastIdRef.current == null) {
          const bootstrap = await getTaskEvents(0, 1);
          if (cancelled) return;
          lastIdRef.current = Math.max(0, Number(bootstrap.last_id) || 0);
          scheduleNext();
          return;
        }

        const afterId = Math.max(0, Number(lastIdRef.current) || 0);
        const res = await getTaskEvents(afterId, 200);
        if (cancelled) return;
        const incoming = Array.isArray(res.events) ? res.events : [];
        for (const event of incoming) {
          if (!event || typeof event.event_id !== 'number') continue;
          if (seenEventIdsRef.current.has(event.event_id)) continue;
          seenEventIdsRef.current.add(event.event_id);
          notifyForEvent(event);
        }
        const candidateLast = Math.max(afterId, Number(res.last_id) || 0);
        if (incoming.length > 0) {
          const maxFromEvents = incoming.reduce((acc, item) => Math.max(acc, Number(item.event_id) || 0), 0);
          lastIdRef.current = Math.max(candidateLast, maxFromEvents);
        } else {
          lastIdRef.current = candidateLast;
        }
      } catch {
        // Best-effort notifications: polling errors should not break the app.
      } finally {
        scheduleNext();
      }
    };

    runPoll();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [enabled, pollIntervalMs, settingsSignature]);
}
