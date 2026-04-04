import type { LogTailEntry } from '@/lib/api';
import { cn } from '@/lib/utils';

const THREAD_STYLES = [
  {
    pill: 'border-sky-400/35 bg-sky-500/15 text-sky-100',
    rail: 'border-l-sky-400/40',
  },
  {
    pill: 'border-fuchsia-400/35 bg-fuchsia-500/15 text-fuchsia-100',
    rail: 'border-l-fuchsia-400/40',
  },
  {
    pill: 'border-emerald-400/35 bg-emerald-500/15 text-emerald-100',
    rail: 'border-l-emerald-400/40',
  },
  {
    pill: 'border-amber-400/35 bg-amber-500/15 text-amber-100',
    rail: 'border-l-amber-400/40',
  },
  {
    pill: 'border-violet-400/35 bg-violet-500/15 text-violet-100',
    rail: 'border-l-violet-400/40',
  },
  {
    pill: 'border-cyan-400/35 bg-cyan-500/15 text-cyan-100',
    rail: 'border-l-cyan-400/40',
  },
  {
    pill: 'border-rose-400/35 bg-rose-500/15 text-rose-100',
    rail: 'border-l-rose-400/40',
  },
  {
    pill: 'border-lime-400/35 bg-lime-500/15 text-lime-100',
    rail: 'border-l-lime-400/40',
  },
  {
    pill: 'border-orange-400/35 bg-orange-500/15 text-orange-100',
    rail: 'border-l-orange-400/40',
  },
  {
    pill: 'border-teal-400/35 bg-teal-500/15 text-teal-100',
    rail: 'border-l-teal-400/40',
  },
  {
    pill: 'border-indigo-400/35 bg-indigo-500/15 text-indigo-100',
    rail: 'border-l-indigo-400/40',
  },
  {
    pill: 'border-pink-400/35 bg-pink-500/15 text-pink-100',
    rail: 'border-l-pink-400/40',
  },
] as const;

function threadStyle(slot?: number) {
  const safe = Number.isFinite(slot) ? Math.abs(Number(slot)) % THREAD_STYLES.length : 0;
  return THREAD_STYLES[safe];
}

function levelClass(level?: string) {
  switch ((level || '').toUpperCase()) {
    case 'ERROR':
      return 'border-red-400/35 bg-red-500/15 text-red-100';
    case 'WARNING':
      return 'border-amber-400/35 bg-amber-500/15 text-amber-100';
    case 'DEBUG':
      return 'border-cyan-400/35 bg-cyan-500/15 text-cyan-100';
    case 'INFO':
    default:
      return 'border-white/10 bg-white/5 text-zinc-100';
  }
}

function kindClass(kind?: string) {
  switch ((kind || '').toLowerCase()) {
    case 'match':
      return {
        row: 'bg-emerald-500/6 text-emerald-50',
        marker: 'border-emerald-400/40 bg-emerald-500 text-black',
      };
    case 'miss':
      return {
        row: 'bg-red-500/8 text-red-50',
        marker: 'border-red-400/40 bg-red-500 text-white',
      };
    case 'soft':
      return {
        row: 'bg-amber-500/8 text-amber-50',
        marker: 'border-amber-400/40 bg-amber-400 text-black',
      };
    case 'warning':
      return {
        row: 'bg-amber-500/6 text-amber-50',
        marker: 'border-amber-400/40 bg-amber-500 text-black',
      };
    case 'error':
      return {
        row: 'bg-red-500/8 text-red-50',
        marker: 'border-red-400/40 bg-red-500 text-white',
      };
    case 'scan':
      return {
        row: 'bg-sky-500/6 text-sky-50',
        marker: '',
      };
    case 'provider':
      return {
        row: 'bg-violet-500/6 text-violet-50',
        marker: '',
      };
    case 'ai':
      return {
        row: 'bg-fuchsia-500/6 text-fuchsia-50',
        marker: '',
      };
    default:
      return {
        row: 'text-zinc-100',
        marker: '',
      };
  }
}

function fallbackEntry(raw: string): LogTailEntry {
  return {
    raw,
    timestamp: '',
    level: '',
    thread: '',
    thread_key: '',
    thread_slot: 0,
    message: raw,
    kind: 'info',
    marker: '',
  };
}

interface BackendLogPanelProps {
  path?: string;
  entries?: LogTailEntry[];
  lines?: string[];
  maxLines?: number;
  compact?: boolean;
  newestFirst?: boolean;
  className?: string;
}

export function BackendLogPanel({
  path,
  entries = [],
  lines = [],
  maxLines = 180,
  compact = false,
  newestFirst = false,
  className,
}: BackendLogPanelProps) {
  const base = entries.length > 0 ? entries : lines.map(fallbackEntry);
  const sliced = base.slice(-maxLines);
  const visible = newestFirst ? [...sliced].reverse() : sliced;

  return (
    <div className={cn('rounded-lg border border-border bg-black/90 font-mono', className)}>
      {path && (
        <div className="border-b border-white/10 px-3 py-1.5 text-[10px] text-emerald-300/90 truncate" title={path}>
          {path}
        </div>
      )}
      <div className={cn('px-2 py-2', compact ? 'space-y-1' : 'space-y-1.5')}>
        {visible.length === 0 ? (
          <div className="px-2 py-1 text-[11px] text-zinc-400">No logs yet…</div>
        ) : (
          visible.map((entry, idx) => {
            const thread = threadStyle(entry.thread_slot);
            const kind = kindClass(entry.kind);
            return (
              <div
                key={`${idx}-${entry.raw.slice(0, 24)}`}
                className={cn(
                  'border-l-2 rounded-md px-2 py-1.5',
                  thread.rail,
                  kind.row,
                )}
              >
                <div className={cn('flex items-start gap-2', compact ? 'text-[10px]' : 'text-[11px]')}>
                  {entry.timestamp ? (
                    <span className="shrink-0 pt-0.5 tabular-nums text-zinc-500">
                      {entry.timestamp}
                    </span>
                  ) : null}
                  {entry.level ? (
                    <span className={cn('shrink-0 rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide', levelClass(entry.level))}>
                      {entry.level}
                    </span>
                  ) : null}
                  {entry.thread ? (
                    <span className={cn('shrink-0 rounded-full border px-2 py-0.5 text-[10px] font-semibold tracking-wide', thread.pill)}>
                      {entry.thread}
                    </span>
                  ) : null}
                  {entry.marker ? (
                    <span className={cn('mt-0.5 inline-flex h-5 min-w-5 shrink-0 items-center justify-center rounded border text-[10px] font-black', kind.marker)}>
                      {entry.marker}
                    </span>
                  ) : null}
                  <span className="min-w-0 whitespace-pre-wrap break-words leading-5 text-zinc-100">
                    {entry.message}
                  </span>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
