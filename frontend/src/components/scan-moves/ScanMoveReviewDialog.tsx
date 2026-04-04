import { useEffect, useMemo, useState } from 'react';
import { AlertTriangle, CheckCircle2, Disc3, Loader2, Undo2 } from 'lucide-react';

import * as api from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { ProviderBadge } from '@/components/providers/ProviderBadge';
import { cn } from '@/lib/utils';
import { badgeKindClass } from '@/lib/badgeStyles';
import { formatBadgeDateTime } from '@/lib/dateFormat';

interface ScanMoveReviewDialogProps {
  move: api.ScanMove | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  restoring?: boolean;
  onRestore?: (moveId: number) => Promise<void> | void;
}

interface GroupedTracks {
  key: string;
  label: string;
  tracks: api.ScanMoveDetailTrack[];
}

function formatDuration(seconds?: number): string {
  const value = Number(seconds || 0);
  if (!Number.isFinite(value) || value <= 0) return '—';
  const h = Math.floor(value / 3600);
  const m = Math.floor((value % 3600) / 60);
  const s = Math.floor(value % 60);
  if (h > 0) return `${h}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
  return `${m}:${s.toString().padStart(2, '0')}`;
}

function statusLabel(status?: string): string {
  const value = String(status || '').trim().toLowerCase();
  if (value === 'restored') return 'Restored';
  if (value === 'missing') return 'Missing';
  return 'Moved';
}

function statusBadgeClass(status?: string): string {
  const value = String(status || '').trim().toLowerCase();
  if (value === 'restored') return badgeKindClass('muted');
  if (value === 'missing') return badgeKindClass('source');
  return badgeKindClass('count');
}

function groupTracksByDisc(tracks: api.ScanMoveDetailTrack[]): GroupedTracks[] {
  const groups = new Map<string, GroupedTracks>();
  for (const track of tracks || []) {
    const discNum = Math.max(1, Number(track.disc_num || 1));
    const label = String(track.disc_label || '').trim() || `Disc ${discNum}`;
    const key = `${discNum}:${label.toLowerCase()}`;
    if (!groups.has(key)) {
      groups.set(key, { key, label, tracks: [] });
    }
    groups.get(key)?.tracks.push(track);
  }
  const out = Array.from(groups.values());
  for (const group of out) {
    group.tracks.sort((a, b) => {
      const discDiff = Number(a.disc_num || 1) - Number(b.disc_num || 1);
      if (discDiff !== 0) return discDiff;
      return Number(a.track_num || 0) - Number(b.track_num || 0);
    });
  }
  return out;
}

function parseRationale(rationale: unknown): string[] {
  return String(rationale || '')
    .split(/[;\n]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeMissingIndex(raw: unknown): number[] {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value) && value > 0)
    .map((value) => Math.floor(value));
}

function EditionPanel({
  title,
  edition,
  accent,
}: {
  title: string;
  edition: api.ScanMoveDetailEdition;
  accent?: 'winner' | 'moved';
}) {
  const grouped = useMemo(() => groupTracksByDisc(edition.tracks || []), [edition.tracks]);
  const accentClass =
    accent === 'winner'
      ? 'border-success/30 bg-success/5'
      : 'border-border/70 bg-muted/20';

  return (
    <div className={cn('rounded-xl border p-4 space-y-4', accentClass)}>
      <div className="flex items-start gap-4">
        <div className="w-24 h-24 rounded-2xl overflow-hidden border border-border/70 bg-muted shrink-0">
          {edition.thumb_url ? (
            <img
              src={edition.thumb_url}
              alt={edition.album_title || title}
              className="w-full h-full object-cover"
              loading="lazy"
              decoding="async"
            />
          ) : (
            <div className="w-full h-full flex items-center justify-center">
              <Disc3 className="w-6 h-6 text-muted-foreground" />
            </div>
          )}
        </div>
        <div className="min-w-0 space-y-1.5">
          <div className="flex flex-wrap items-center gap-2">
            <h4 className="text-base font-semibold">{title}</h4>
            <Badge variant="outline" className={cn('text-[10px]', badgeKindClass('count'))}>
              {edition.track_count || 0} tracks
            </Badge>
            {edition.fmt_text ? (
              <Badge variant="outline" className={cn('text-[10px]', badgeKindClass('track_meta'))}>
                {edition.fmt_text}
              </Badge>
            ) : null}
          </div>
          {edition.album_title ? <p className="text-sm text-muted-foreground">{edition.album_title}</p> : null}
          <code className="block rounded bg-background/70 px-2 py-1 text-[11px] break-all">{edition.path || '—'}</code>
        </div>
      </div>

      {grouped.length === 0 ? (
        <p className="text-sm text-muted-foreground">No track detail available for this edition.</p>
      ) : (
        <div className="space-y-3">
          {grouped.map((group) => (
            <div key={group.key} className="space-y-2">
              {grouped.length > 1 ? (
                <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  {group.label}
                </div>
              ) : null}
              <div className="rounded-lg border border-border/60 overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="bg-muted/40 text-muted-foreground">
                    <tr>
                      <th className="px-3 py-2 text-left text-[10px] uppercase tracking-wide w-16">#</th>
                      <th className="px-3 py-2 text-left text-[10px] uppercase tracking-wide">Track</th>
                      <th className="px-3 py-2 text-right text-[10px] uppercase tracking-wide w-24">Duration</th>
                    </tr>
                  </thead>
                  <tbody>
                    {group.tracks.map((track, idx) => (
                      <tr key={`${group.key}-${track.track_num}-${idx}`} className="border-t border-border/50">
                        <td className="px-3 py-2 text-xs text-muted-foreground tabular-nums">
                          {Number(track.track_num || idx + 1)}
                        </td>
                        <td className="px-3 py-2">
                          <div className="font-medium">{track.title || `Track ${idx + 1}`}</div>
                          {track.file_path ? (
                            <div className="text-[11px] text-muted-foreground break-all">{track.file_path}</div>
                          ) : null}
                        </td>
                        <td className="px-3 py-2 text-right text-xs text-muted-foreground tabular-nums">
                          {formatDuration(track.duration_sec)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export function ScanMoveReviewDialog({
  move,
  open,
  onOpenChange,
  restoring = false,
  onRestore,
}: ScanMoveReviewDialogProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [detail, setDetail] = useState<api.ScanMoveDetailResponse | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function run() {
      if (!open || !move?.move_id) return;
      setLoading(true);
      setError(null);
      try {
        const payload = await api.getScanMoveDetail(move.move_id);
        if (!cancelled) setDetail(payload);
      } catch (err) {
        if (!cancelled) {
          setDetail(null);
          setError(err instanceof Error ? err.message : 'Failed to load move detail');
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    void run();
    return () => {
      cancelled = true;
    };
  }, [move?.move_id, open]);

  const rationaleItems = useMemo(
    () => parseRationale(detail?.winner?.analysis?.rationale),
    [detail?.winner?.analysis],
  );
  const evidenceItems = useMemo(() => {
    const raw = detail?.winner?.analysis?.dupe_evidence;
    return Array.isArray(raw) ? raw.map((item) => String(item || '').trim()).filter(Boolean) : [];
  }, [detail?.winner?.analysis]);
  const missingIndices = useMemo(
    () => normalizeMissingIndex(detail?.incomplete?.missing_indices),
    [detail?.incomplete?.missing_indices],
  );

  const incompleteRows = useMemo(() => {
    const expected = detail?.incomplete?.expected_tracks || [];
    const actual = detail?.moved?.tracks || [];
    const actualByIndex = new Map<number, api.ScanMoveDetailTrack>();
    for (const track of actual) {
      actualByIndex.set(Number(track.track_num || 0), track);
    }
    return expected.map((row) => {
      const index = Number(row.index || 0);
      return {
        index,
        expected: row.title,
        actual: actualByIndex.get(index),
        missing: missingIndices.includes(index),
      };
    });
  }, [detail?.incomplete?.expected_tracks, detail?.moved?.tracks, missingIndices]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto p-0">
        <DialogHeader className="px-6 py-5 border-b border-border/70">
          <DialogTitle>{move?.artist || detail?.artist || 'Move review'} · {move?.album_title || detail?.album_title || 'Album'}</DialogTitle>
          <DialogDescription>
            Review why PMDA moved this album, inspect the compared editions, and rollback if needed.
          </DialogDescription>
        </DialogHeader>

        <div className="px-6 py-5 space-y-5">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline" className={cn('text-[10px]', badgeKindClass('muted'))}>
              Run #{detail?.scan_id || move?.scan_id || 0}
            </Badge>
            <Badge variant="outline" className={cn('text-[10px]', statusBadgeClass(detail?.status || move?.status))}>
              {statusLabel(detail?.status || move?.status)}
            </Badge>
            <Badge variant="outline" className={cn('text-[10px]', badgeKindClass('source'))}>
              {detail?.reason_label || move?.reason_label || 'Moved'}
            </Badge>
            <Badge variant="outline" className={cn('text-[10px]', badgeKindClass('muted'))}>
              {formatBadgeDateTime((detail?.moved_at ?? move?.moved_at) || 0)}
            </Badge>
            {detail?.decision_provider ? <ProviderBadge provider={detail.decision_provider} prefix="Provider" className="text-[10px]" /> : null}
            {detail?.decision_source ? <ProviderBadge provider={detail.decision_source} prefix="Source" className="text-[10px]" /> : null}
            {detail?.decision_confidence != null ? (
              <Badge variant="outline" className={cn('text-[10px]', badgeKindClass('count'))}>
                Confidence {(Number(detail.decision_confidence) * 100).toFixed(0)}%
              </Badge>
            ) : null}
          </div>

          {detail?.decision_reason ? (
            <div className="rounded-xl border border-border/70 bg-muted/20 p-4">
              <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Why moved</div>
              <p className="mt-2 text-sm leading-relaxed">{detail.decision_reason}</p>
            </div>
          ) : null}

          {loading ? (
            <div className="flex items-center justify-center py-16">
              <Loader2 className="w-8 h-8 animate-spin text-primary" />
            </div>
          ) : error ? (
            <div className="rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
              {error}
            </div>
          ) : detail ? (
            <>
              {detail.move_reason === 'dedupe' ? (
                <>
                  <div className="grid gap-5 xl:grid-cols-2">
                    <EditionPanel title="Moved duplicate" edition={detail.moved} accent="moved" />
                    {detail.winner ? <EditionPanel title="Kept winner" edition={detail.winner} accent="winner" /> : null}
                  </div>

                  <div className="rounded-xl border border-border/70 bg-muted/20 p-4 space-y-3">
                    <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                      Winner selection analysis
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {detail.winner?.analysis?.dupe_signal ? (
                        <Badge variant="outline" className={cn('text-[10px]', badgeKindClass('source'))}>
                          Signal: {String(detail.winner.analysis.dupe_signal)}
                        </Badge>
                      ) : null}
                      {detail.winner?.analysis?.strict_match_verified ? (
                        <Badge variant="outline" className={cn('text-[10px]', badgeKindClass('count'))}>
                          Strict match verified
                        </Badge>
                      ) : null}
                      {detail.winner?.analysis?.match_verified_by_ai ? (
                        <Badge variant="outline" className={cn('text-[10px]', badgeKindClass('muted'))}>
                          AI verified
                        </Badge>
                      ) : null}
                      {detail.winner?.analysis?.strict_tracklist_score ? (
                        <Badge variant="outline" className={cn('text-[10px]', badgeKindClass('count'))}>
                          Tracklist score {Number(detail.winner.analysis.strict_tracklist_score).toFixed(2)}
                        </Badge>
                      ) : null}
                    </div>

                    {rationaleItems.length > 0 ? (
                      <div className="space-y-2">
                        {rationaleItems.map((item, index) => (
                          <div key={`rationale-${index}`} className="flex items-start gap-2 text-sm">
                            <CheckCircle2 className="w-4 h-4 mt-0.5 text-success shrink-0" />
                            <span>{item}</span>
                          </div>
                        ))}
                      </div>
                    ) : null}

                    {evidenceItems.length > 0 ? (
                      <div className="flex flex-wrap gap-2">
                        {evidenceItems.map((item) => (
                          <Badge key={item} variant="outline" className={cn('text-[10px]', badgeKindClass('track_meta'))}>
                            {item}
                          </Badge>
                        ))}
                      </div>
                    ) : null}
                  </div>
                </>
              ) : (
                <>
                  <div className="grid gap-5 xl:grid-cols-[1.2fr,1fr]">
                    <EditionPanel title="Moved incomplete edition" edition={detail.moved} accent="moved" />
                    <div className="rounded-xl border border-border/70 bg-muted/20 p-4 space-y-4">
                      <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                        Incomplete analysis
                      </div>
                      <div className="flex flex-wrap gap-2">
                        <Badge variant="outline" className={cn('text-[10px]', badgeKindClass('count'))}>
                          Expected {detail.incomplete?.expected_track_count || 0}
                        </Badge>
                        <Badge variant="outline" className={cn('text-[10px]', badgeKindClass('source'))}>
                          Actual {detail.incomplete?.actual_track_count || 0}
                        </Badge>
                        {detail.incomplete?.strict_match_provider ? (
                          <ProviderBadge provider={detail.incomplete.strict_match_provider} prefix="Expected from" className="text-[10px]" />
                        ) : null}
                      </div>
                      {detail.incomplete?.strict_reject_reason ? (
                        <p className="text-sm">{detail.incomplete.strict_reject_reason}</p>
                      ) : null}
                      {missingIndices.length > 0 ? (
                        <div className="space-y-2">
                          <div className="text-sm font-medium">Missing track indices</div>
                          <div className="flex flex-wrap gap-2">
                            {missingIndices.map((index) => (
                              <Badge key={`miss-${index}`} variant="outline" className="text-[10px] border-destructive/40 text-destructive">
                                Missing #{index}
                              </Badge>
                            ))}
                          </div>
                        </div>
                      ) : null}
                      {detail.incomplete?.missing_required_tags?.length ? (
                        <div className="space-y-2">
                          <div className="text-sm font-medium">Missing required tags</div>
                          <div className="flex flex-wrap gap-2">
                            {detail.incomplete.missing_required_tags.map((tag) => (
                              <Badge key={tag} variant="outline" className={cn('text-[10px]', badgeKindClass('source'))}>
                                {tag}
                              </Badge>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </div>
                  </div>

                  <div className="rounded-xl border border-border/70 overflow-hidden">
                    <table className="w-full text-sm">
                      <thead className="bg-muted/40">
                        <tr>
                          <th className="px-3 py-2 text-left text-[10px] uppercase tracking-wide w-16">#</th>
                          <th className="px-3 py-2 text-left text-[10px] uppercase tracking-wide">Expected track</th>
                          <th className="px-3 py-2 text-left text-[10px] uppercase tracking-wide">Found in moved folder</th>
                        </tr>
                      </thead>
                      <tbody>
                        {incompleteRows.map((row) => (
                          <tr key={`incomplete-row-${row.index}`} className="border-t border-border/50">
                            <td className="px-3 py-2 text-xs tabular-nums text-muted-foreground">{row.index}</td>
                            <td className="px-3 py-2">
                              <div className="font-medium">{row.expected}</div>
                            </td>
                            <td className="px-3 py-2">
                              {row.actual ? (
                                <div>
                                  <div className="font-medium">{row.actual.title}</div>
                                  <div className="text-[11px] text-muted-foreground">{formatDuration(row.actual.duration_sec)}</div>
                                </div>
                              ) : (
                                <div className="inline-flex items-center gap-2 text-destructive">
                                  <AlertTriangle className="w-4 h-4" />
                                  <span className="text-sm font-medium">Missing from moved folder</span>
                                </div>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </>
          ) : null}
        </div>

        <div className="px-6 py-4 border-t border-border/70 flex items-center justify-end gap-3">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
          {move?.move_id ? (
            <Button
              variant="outline"
              className="gap-2"
              onClick={() => void onRestore?.(move.move_id)}
              disabled={restoring || String(detail?.status || move.status || '').toLowerCase() === 'restored'}
            >
              {restoring ? <Loader2 className="w-4 h-4 animate-spin" /> : <Undo2 className="w-4 h-4" />}
              Rollback
            </Button>
          ) : null}
        </div>
      </DialogContent>
    </Dialog>
  );
}
