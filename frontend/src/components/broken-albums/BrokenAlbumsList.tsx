import { useState, useEffect, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { AlertCircle, CheckCircle2, Disc3, Loader2, Music, Search, Undo2 } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { AuthenticatedImage } from '@/components/library/AuthenticatedImage';
import { ScanMoveReviewDialog } from '@/components/scan-moves/ScanMoveReviewDialog';
import { ProviderBadge } from '@/components/providers/ProviderBadge';
import * as api from '@/lib/api';
import { formatBadgeDateTime } from '@/lib/dateFormat';
import { useToast } from '@/hooks/use-toast';

function fmtMissingIndices(raw: unknown): string {
  if (!Array.isArray(raw)) return '';
  const chunks: string[] = [];
  for (const row of raw) {
    if (Array.isArray(row)) {
      if (row.length < 2) continue;
      const start = Number(row[0]);
      const end = Number(row[1]);
      if (!Number.isFinite(start) || !Number.isFinite(end)) continue;
      chunks.push(start === end ? `${start}` : `${start}-${end}`);
      continue;
    }
    const single = Number(row);
    if (!Number.isFinite(single)) continue;
    const start = single;
    const end = single;
    if (!Number.isFinite(start) || !Number.isFinite(end)) continue;
    chunks.push(`${start}`);
  }
  return chunks.join(', ');
}

function humanizeReason(raw: string | null | undefined): string {
  const value = String(raw || '').trim();
  if (!value) return '';
  return value
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function humanizeClassification(raw: string | null | undefined): string {
  const value = String(raw || '').trim();
  if (!value) return '';
  const labels: Record<string, string> = {
    confirmed_incomplete: 'Confirmed incomplete',
    likely_incomplete_review: 'Likely incomplete — review',
    current_folder_matches_expected_not_incomplete: 'Current folder matches expected edition',
    alternate_edition_not_incomplete: 'Alternate edition, not incomplete',
    identity_mismatch_not_incomplete: 'Provider mismatch, not incomplete',
    numbering_or_tag_issue_not_incomplete: 'Numbering/tag issue, not missing audio',
    insufficient_evidence_manual_review: 'Not enough evidence yet',
  };
  if (labels[value]) return labels[value];
  return value
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

type BrokenAlbumBucket = 'confirmed' | 'likely' | 'uncertain' | 'not_incomplete';

function bucketForBrokenAlbum(album: api.BrokenAlbum): BrokenAlbumBucket {
  const classification = String(album.classification || '').trim().toLowerCase();
  if (classification === 'confirmed_incomplete') return 'confirmed';
  if (classification === 'likely_incomplete_review') return 'likely';
  if (
    classification === 'current_folder_matches_expected_not_incomplete'
    || classification === 'alternate_edition_not_incomplete'
    || classification === 'identity_mismatch_not_incomplete'
    || classification === 'numbering_or_tag_issue_not_incomplete'
  ) {
    return 'not_incomplete';
  }
  return 'uncertain';
}

function bucketTitle(bucket: BrokenAlbumBucket): string {
  if (bucket === 'confirmed') return 'Confirmed incomplete';
  if (bucket === 'likely') return 'Likely incomplete — manual review';
  if (bucket === 'not_incomplete') return 'Probably not incomplete';
  return 'Not enough evidence yet';
}

function bucketDescription(bucket: BrokenAlbumBucket): string {
  if (bucket === 'confirmed') {
    return 'These albums have a strong missing-audio signal and are the only ones that should be treated as truly incomplete by default.';
  }
  if (bucket === 'likely') {
    return 'PMDA sees a probable track deficit, but provider identity or tracklist evidence is still too weak for automatic quarantine.';
  }
  if (bucket === 'not_incomplete') {
    return 'These entries now look more like stale snapshots, alternate editions, numbering problems, or provider mismatches than missing audio.';
  }
  return 'These entries were surfaced for review noise only. They should not be read as confirmed incomplete albums.';
}

function classificationExplanation(album: api.BrokenAlbum): string {
  const bucket = bucketForBrokenAlbum(album);
  if (bucket === 'confirmed') {
    return 'PMDA found a strong enough missing-audio deficit to classify this as truly incomplete.';
  }
  if (bucket === 'likely') {
    return 'PMDA found a probable deficit, but the provider evidence is still too weak for auto-quarantine.';
  }
  if (bucket === 'not_incomplete') {
    if (String(album.classification || '').trim() === 'current_folder_matches_expected_not_incomplete') {
      return 'The current local folder now matches the expected edition. This incomplete flag came from a stale earlier snapshot.';
    }
    return 'The latest evidence points to an alternate edition, numbering issue, or provider mismatch rather than missing audio.';
  }
  return 'PMDA could not confirm a reliable provider tracklist or identity match here. Treat this as uncertain review data, not as a true incomplete album.';
}

function aiVerdictLabel(verdict: api.BrokenAlbumAiVerdict | null | undefined): string {
  const status = String(verdict?.status || '').trim().toLowerCase();
  if (status === 'failed') return 'AI shadow failed';
  if (status === 'skipped') return 'AI shadow skipped';
  const value = String(verdict?.verdict || '').trim();
  if (!value) return '';
  return `AI: ${humanizeClassification(value)}`;
}

function aiVerdictBadgeVariant(verdict: api.BrokenAlbumAiVerdict | null | undefined): 'default' | 'secondary' | 'destructive' | 'outline' {
  const status = String(verdict?.status || '').trim().toLowerCase();
  if (status === 'failed') return 'destructive';
  if (status === 'skipped') return 'outline';
  const value = String(verdict?.verdict || '').trim();
  if (value === 'confirmed_incomplete') return 'destructive';
  if (value === 'current_folder_matches_expected_not_incomplete' || value === 'alternate_edition_not_incomplete' || value === 'identity_mismatch_not_incomplete' || value === 'numbering_or_tag_issue_not_incomplete') {
    return 'secondary';
  }
  return 'outline';
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

export function BrokenAlbumsList() {
  const [searchParams] = useSearchParams();
  const scanId = useMemo(() => {
    const raw = searchParams.get('scan_id');
    const parsed = Number(raw);
    return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : null;
  }, [searchParams]);

  const [brokenAlbums, setBrokenAlbums] = useState<api.BrokenAlbum[]>([]);
  const [movedIncompletes, setMovedIncompletes] = useState<api.ScanMove[]>([]);
  const [movesFilter, setMovesFilter] = useState<'all' | 'active' | 'restored'>('all');
  const [loading, setLoading] = useState(true);
  const [restoring, setRestoring] = useState<number | 'all' | null>(null);
  const [reviewingAlbum, setReviewingAlbum] = useState<number | null>(null);
  const [selectedMove, setSelectedMove] = useState<api.ScanMove | null>(null);
  const [selectedAlbum, setSelectedAlbum] = useState<api.BrokenAlbum | null>(null);
  const [selectedAlbumDetail, setSelectedAlbumDetail] = useState<api.BrokenAlbumDetail | null>(null);
  const [selectedAlbumDetailLoading, setSelectedAlbumDetailLoading] = useState(false);
  const [selectedAlbumDetailError, setSelectedAlbumDetailError] = useState<string | null>(null);
  const [selectedAlbumAiRefreshing, setSelectedAlbumAiRefreshing] = useState(false);
  const { toast } = useToast();

  useEffect(() => {
    void loadData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scanId]);

  const groupedAlbums = useMemo(() => {
    const groups: Record<BrokenAlbumBucket, api.BrokenAlbum[]> = {
      confirmed: [],
      likely: [],
      uncertain: [],
      not_incomplete: [],
    };
    for (const album of brokenAlbums) {
      groups[bucketForBrokenAlbum(album)].push(album);
    }
    return groups;
  }, [brokenAlbums]);

  const confirmedAlbums = groupedAlbums.confirmed;
  const likelyAlbums = groupedAlbums.likely;
  const uncertainAlbums = groupedAlbums.uncertain;
  const notIncompleteAlbums = groupedAlbums.not_incomplete;

  const loadData = async () => {
    try {
      setLoading(true);
      const [albums, moves] = await Promise.all([
        api.getBrokenAlbums(),
        scanId ? api.getScanMoves(scanId, { reason: 'incomplete', status: 'all' }) : Promise.resolve([]),
      ]);
      setBrokenAlbums(albums);
      const runIncompleteMoves = (moves || []).filter((m) => (m.move_reason || '').toLowerCase() === 'incomplete');
      setMovedIncompletes(runIncompleteMoves);
    } catch {
      toast({
        title: 'Error',
        description: 'Failed to load incomplete albums review',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  const restoreMoves = async (moveIds?: number[]) => {
    if (!scanId) return;
    const selected = Array.isArray(moveIds) ? moveIds.filter((v) => Number.isFinite(v)) : [];
    if (selected.length === 0) {
      toast({
        title: 'Nothing to restore',
        description: 'No moved incomplete albums to restore for this run.',
      });
      return;
    }
    setRestoring(selected.length > 1 ? 'all' : selected[0]);
    try {
      const res = await api.restoreMoves(scanId, selected, false);
      toast({
        title: 'Restore complete',
        description: `${res.restored} incomplete move(s) restored.`,
      });
      await loadData();
    } catch (error) {
      toast({
        title: 'Restore failed',
        description: error instanceof Error ? error.message : 'Unable to restore incomplete moves',
        variant: 'destructive',
      });
    } finally {
      setRestoring(null);
    }
  };

  const refreshSelectedAlbumAiVerdict = async () => {
    if (!selectedAlbum) return;
    try {
      setSelectedAlbumAiRefreshing(true);
      setSelectedAlbumDetailError(null);
      const payload = await api.getBrokenAlbumDetail(selectedAlbum.artist, selectedAlbum.album_id, { refreshAi: true });
      setSelectedAlbumDetail((current) => ({
        ...(current || {}),
        ...payload,
        album_title: payload.album_title && !/^Album \d+$/.test(payload.album_title)
          ? payload.album_title
          : selectedAlbum.album_title,
        thumb_url: payload.thumb_url || selectedAlbum.thumb_url || null,
        metadata_source: payload.metadata_source || selectedAlbum.metadata_source || null,
        strict_match_provider: payload.strict_match_provider || selectedAlbum.strict_match_provider || null,
        strict_reject_reason: payload.strict_reject_reason || selectedAlbum.strict_reject_reason || null,
        folder_path: payload.folder_path || selectedAlbum.folder_path || null,
        recoverable: Boolean(payload.recoverable || selectedAlbum.recoverable),
        sent_to_external_recovery: Boolean(
          payload.sent_to_external_recovery ||
          payload.sent_to_lidarr ||
          selectedAlbum.sent_to_external_recovery ||
          selectedAlbum.sent_to_lidarr,
        ),
        review_status: payload.review_status || selectedAlbum.review_status || 'pending',
      }));
      toast({
        title: 'AI shadow refreshed',
        description: 'The incomplete arbitration shadow verdict has been recomputed for this album.',
      });
    } catch (error) {
      setSelectedAlbumDetailError(error instanceof Error ? error.message : 'Unable to refresh AI shadow verdict');
      toast({
        title: 'AI shadow refresh failed',
        description: error instanceof Error ? error.message : 'Unable to refresh AI shadow verdict',
        variant: 'destructive',
      });
    } finally {
      setSelectedAlbumAiRefreshing(false);
    }
  };

  const renderAlbumSection = (bucket: BrokenAlbumBucket, albums: api.BrokenAlbum[]) => {
    if (albums.length === 0) return null;
    const toneClass =
      bucket === 'confirmed'
        ? 'border-destructive/40 bg-destructive/5'
        : bucket === 'likely'
          ? 'border-primary/30 bg-primary/5'
          : 'border-border/70 bg-muted/15';
    return (
      <Card className={toneClass}>
        <CardHeader className="pb-3">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <CardTitle className="text-lg">{bucketTitle(bucket)}</CardTitle>
              <CardDescription className="mt-1">{bucketDescription(bucket)}</CardDescription>
            </div>
            <Badge variant={bucket === 'confirmed' ? 'destructive' : bucket === 'likely' ? 'default' : 'secondary'}>
              {albums.length} album{albums.length > 1 ? 's' : ''}
            </Badge>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {albums.map((album) => (
            <Card key={`${bucket}-${album.artist}-${album.album_id}`}>
              <CardHeader className="pb-3">
                <div className="flex gap-4">
                  <div className="w-20 h-20 rounded-2xl overflow-hidden border border-border/70 bg-muted shrink-0">
                    {album.thumb_url ? (
                      <AuthenticatedImage
                        src={album.thumb_url}
                        alt={album.album_title}
                        className="w-full h-full object-cover"
                        fallback={<Disc3 className="w-5 h-5 text-muted-foreground" />}
                      />
                    ) : (
                      <div className="w-full h-full flex items-center justify-center">
                        <Disc3 className="w-5 h-5 text-muted-foreground" />
                      </div>
                    )}
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0 flex-1">
                        <CardTitle className="flex items-center gap-2">
                          <Music className="w-5 h-5 text-muted-foreground shrink-0" />
                          <span className="truncate">{album.album_title}</span>
                        </CardTitle>
                        <CardDescription className="mt-1">{album.artist}</CardDescription>
                      </div>
                      <div className="flex flex-wrap justify-end gap-2">
                        {album.strict_match_provider ? (
                          <ProviderBadge provider={album.strict_match_provider} prefix="Match" className="text-[10px]" />
                        ) : null}
                        {album.metadata_source ? (
                          <ProviderBadge provider={album.metadata_source} prefix="Source" className="text-[10px]" />
                        ) : null}
                        {album.review_status === 'ignored' ? (
                          <Badge variant="secondary">Ignored</Badge>
                        ) : null}
                        {!album.recoverable ? (
                          <Button
                            size="sm"
                            variant="outline"
                            className="h-6 px-2 text-[10px]"
                            onClick={() => setSelectedAlbum(album)}
                          >
                            Manual review
                          </Button>
                        ) : null}
                      </div>
                    </div>
                    <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
                      {album.expected_track_count ? <Badge variant="outline">Expected {album.expected_track_count}</Badge> : null}
                      {album.actual_track_count > 0 ? <Badge variant="outline">Actual {album.actual_track_count}</Badge> : null}
                      {fmtMissingIndices(album.missing_indices) ? (
                        <Badge variant="outline">Missing {fmtMissingIndices(album.missing_indices)}</Badge>
                      ) : null}
                      {album.strict_reject_reason ? (
                        <Badge variant="outline">{humanizeReason(album.strict_reject_reason)}</Badge>
                      ) : null}
                      {album.classification ? (
                        <Badge variant={album.quarantine_eligible ? 'destructive' : bucket === 'likely' ? 'default' : 'secondary'}>
                          {humanizeClassification(album.classification)}
                        </Badge>
                      ) : null}
                      {typeof album.classification_confidence === 'number' && Number.isFinite(album.classification_confidence) ? (
                        <Badge variant="outline">{Math.round(album.classification_confidence * 100)}%</Badge>
                      ) : null}
                      {album.ai_verdict ? (
                        <Badge variant={aiVerdictBadgeVariant(album.ai_verdict)}>
                          {aiVerdictLabel(album.ai_verdict)}
                        </Badge>
                      ) : null}
                      {typeof album.ai_verdict?.confidence === 'number' && Number.isFinite(album.ai_verdict.confidence) ? (
                        <Badge variant="outline">AI {Math.round(album.ai_verdict.confidence * 100)}%</Badge>
                      ) : null}
                    </div>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <Alert>
                    <AlertCircle className="h-4 w-4" />
                    <AlertDescription>
                      {classificationExplanation(album)}
                      {album.reason_summary ? ` ${album.reason_summary}` : ''}
                    </AlertDescription>
                  </Alert>
                  {album.musicbrainz_release_group_id ? (
                    <div className="text-xs text-muted-foreground">
                      MusicBrainz ID: {album.musicbrainz_release_group_id}
                    </div>
                  ) : null}
                  {album.folder_path ? (
                    <div className="text-xs text-muted-foreground break-all">
                      Folder: {album.folder_path}
                    </div>
                  ) : null}
                  <div className="flex flex-wrap items-center gap-2">
                    <Button
                      size="sm"
                      variant="ghost"
                      className="gap-1.5"
                      onClick={() => setSelectedAlbum(album)}
                    >
                      <Search className="w-4 h-4" />
                      Review details
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      className="gap-1.5"
                      onClick={() => void setAlbumReviewStatus(album, album.review_status === 'ignored' ? 'pending' : 'ignored')}
                      disabled={reviewingAlbum !== null}
                    >
                      {reviewingAlbum === album.album_id ? <Loader2 className="w-4 h-4 animate-spin" /> : <CheckCircle2 className="w-4 h-4" />}
                      {album.review_status === 'ignored' ? 'Back to review' : 'Ignore for now'}
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </CardContent>
      </Card>
    );
  };

  const setAlbumReviewStatus = async (album: api.BrokenAlbum, nextStatus: 'pending' | 'ignored') => {
    setReviewingAlbum(album.album_id);
    try {
      await api.setBrokenAlbumReviewStatus(album.artist, album.album_id, nextStatus);
      toast({
        title: nextStatus === 'ignored' ? 'Marked as ignored' : 'Marked for review',
        description: `${album.album_title} is now ${nextStatus === 'ignored' ? 'ignored for now' : 'back in the review queue'}.`,
      });
      await loadData();
    } catch (error) {
      toast({
        title: 'Update failed',
        description: error instanceof Error ? error.message : 'Unable to update incomplete album status',
        variant: 'destructive',
      });
    } finally {
      setReviewingAlbum(null);
    }
  };

  useEffect(() => {
    let cancelled = false;
    async function run() {
      if (!selectedAlbum) return;
      setSelectedAlbumDetailLoading(true);
      setSelectedAlbumDetailError(null);
      setSelectedAlbumAiRefreshing(false);
      try {
        const payload = await api.getBrokenAlbumDetail(selectedAlbum.artist, selectedAlbum.album_id);
        if (!cancelled) {
          setSelectedAlbumDetail({
            ...payload,
            album_title: payload.album_title && !/^Album \d+$/.test(payload.album_title)
              ? payload.album_title
              : selectedAlbum.album_title,
            thumb_url: payload.thumb_url || selectedAlbum.thumb_url || null,
            metadata_source: payload.metadata_source || selectedAlbum.metadata_source || null,
            strict_match_provider: payload.strict_match_provider || selectedAlbum.strict_match_provider || null,
            strict_reject_reason: payload.strict_reject_reason || selectedAlbum.strict_reject_reason || null,
            folder_path: payload.folder_path || selectedAlbum.folder_path || null,
            recoverable: Boolean(payload.recoverable || selectedAlbum.recoverable),
            sent_to_external_recovery: Boolean(
              payload.sent_to_external_recovery ||
              payload.sent_to_lidarr ||
              selectedAlbum.sent_to_external_recovery ||
              selectedAlbum.sent_to_lidarr,
            ),
            review_status: payload.review_status || selectedAlbum.review_status || 'pending',
          });
        }
      } catch (error) {
        if (!cancelled) {
          setSelectedAlbumDetail(null);
          setSelectedAlbumDetailError(error instanceof Error ? error.message : 'Unable to load incomplete album detail');
        }
      } finally {
        if (!cancelled) setSelectedAlbumDetailLoading(false);
      }
    }
    void run();
    return () => {
      cancelled = true;
    };
  }, [selectedAlbum]);

  const movedIncompletesFiltered = useMemo(() => {
    if (movesFilter === 'active') return movedIncompletes.filter((m) => String(m.status || (m.restored ? 'restored' : 'moved')).toLowerCase() !== 'restored');
    if (movesFilter === 'restored') return movedIncompletes.filter((m) => String(m.status || (m.restored ? 'restored' : 'moved')).toLowerCase() === 'restored');
    return movedIncompletes;
  }, [movedIncompletes, movesFilter]);
  const movedIncompletesPending = useMemo(
    () => movedIncompletes.filter((m) => String(m.status || '').toLowerCase() !== 'restored'),
    [movedIncompletes],
  );
  return (
    <div className="container py-6 space-y-4">
      <div className="flex items-center justify-between gap-2">
        <div>
          <h2 className="text-2xl font-bold">Incomplete Album Review</h2>
          <p className="text-sm text-muted-foreground mt-1">
            PMDA separates confirmed missing-audio cases from weak provider mismatches and other review-only noise.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="destructive">{confirmedAlbums.length} confirmed</Badge>
          <Badge variant="default">{likelyAlbums.length} likely review</Badge>
          <Badge variant="secondary">{uncertainAlbums.length + notIncompleteAlbums.length} uncertain / not incomplete</Badge>
          {scanId ? <Badge variant="outline">Run #{scanId}</Badge> : null}
        </div>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="w-8 h-8 animate-spin text-primary" />
        </div>
      ) : (
        <>
          {scanId ? (
            <Card>
              <CardHeader>
                <CardTitle>Moved to Incomplete Quarantine</CardTitle>
                <CardDescription>
                  Review why each album was moved during this run and restore if needed.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex items-center gap-2">
                  <Badge variant="outline">{movedIncompletes.length} moved album(s)</Badge>
                  <Badge variant="outline">{movedIncompletesPending.length} active move(s)</Badge>
                  <Button size="sm" variant={movesFilter === 'all' ? 'default' : 'outline'} onClick={() => setMovesFilter('all')}>
                    All
                  </Button>
                  <Button size="sm" variant={movesFilter === 'active' ? 'default' : 'outline'} onClick={() => setMovesFilter('active')}>
                    Moved
                  </Button>
                  <Button size="sm" variant={movesFilter === 'restored' ? 'default' : 'outline'} onClick={() => setMovesFilter('restored')}>
                    Restored
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    className="gap-1.5"
                    onClick={() => void restoreMoves(movedIncompletesPending.map((m) => m.move_id))}
                    disabled={movedIncompletesPending.length === 0 || restoring !== null}
                  >
                    {restoring === 'all' ? <Loader2 className="w-4 h-4 animate-spin" /> : <Undo2 className="w-4 h-4" />}
                    Restore all
                  </Button>
                </div>

                {movedIncompletesFiltered.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No incomplete moves for this filter in this run.</p>
                ) : (
                  <div className="space-y-3">
                    {movedIncompletesFiltered.map((move) => {
                      const details = (move.details || {}) as Record<string, unknown>;
                      const expected = Number(details.expected_track_count ?? 0);
                      const actual = Number(details.actual_track_count ?? 0);
                      const missing = fmtMissingIndices(details.missing_indices);
                      const reason = move.reason_label || move.decision_reason || String(details.classification || 'Incomplete album');
                      const status = String(move.status || (move.restored ? 'restored' : 'moved')).toLowerCase();
                      return (
                        <div
                          key={move.move_id}
                          className="rounded-xl border p-4 space-y-3 cursor-pointer hover:border-primary/40 transition-colors"
                          onClick={() => setSelectedMove(move)}
                          role="button"
                          tabIndex={0}
                          onKeyDown={(event) => {
                            if (event.key === 'Enter' || event.key === ' ') {
                              event.preventDefault();
                              setSelectedMove(move);
                            }
                          }}
                        >
                          <div className="flex gap-4">
                            <div className="w-20 h-20 rounded-2xl overflow-hidden border border-border/70 bg-muted shrink-0">
                              {move.thumb_url ? (
                                <img
                                  src={move.thumb_url}
                                  alt={move.album_title || move.artist}
                                  className="w-full h-full object-cover"
                                  loading="lazy"
                                  decoding="async"
                                />
                              ) : (
                                <div className="w-full h-full flex items-center justify-center">
                                  <Disc3 className="w-5 h-5 text-muted-foreground" />
                                </div>
                              )}
                            </div>
                            <div className="min-w-0 flex-1 space-y-2">
                              <div className="flex flex-wrap items-center gap-2">
                                <span className="font-medium">{move.artist}</span>
                                <span className="text-muted-foreground">•</span>
                                <span>{move.album_title || `Album #${move.album_id}`}</span>
                                <Badge variant="outline">{reason}</Badge>
                                <Badge variant="outline">Moved: {formatBadgeDateTime(move.moved_at)}</Badge>
                                <Badge variant={status === 'restored' ? 'secondary' : 'default'}>
                                  {status === 'restored' ? 'Restored' : status === 'missing' ? 'Missing' : 'Moved'}
                                </Badge>
                                {move.decision_provider ? <ProviderBadge provider={move.decision_provider} prefix="Provider" className="text-[10px]" /> : null}
                              </div>
                              <div className="text-xs text-muted-foreground break-all">
                                From: {move.original_path}
                              </div>
                              <div className="text-xs text-muted-foreground break-all">
                                To: {move.moved_to_path}
                              </div>
                              <div className="flex flex-wrap items-center gap-2 text-xs">
                                {expected > 0 ? <Badge variant="outline">Expected {expected}</Badge> : null}
                                {actual > 0 ? <Badge variant="outline">Actual {actual}</Badge> : null}
                                {missing ? <Badge variant="outline">Missing {missing}</Badge> : null}
                              </div>
                            </div>
                          </div>
                          <div className="flex items-center gap-2">
                            <Button
                              size="sm"
                              variant="outline"
                              className="gap-1.5"
                              onClick={(event) => {
                                event.stopPropagation();
                                setSelectedMove(move);
                              }}
                            >
                              <Search className="w-4 h-4" />
                              Review
                            </Button>
                            <Button
                              size="sm"
                              variant="outline"
                              className="gap-1.5"
                              onClick={(event) => {
                                event.stopPropagation();
                                void restoreMoves([move.move_id]);
                              }}
                              disabled={restoring !== null || status === 'restored'}
                            >
                              {restoring === move.move_id ? <Loader2 className="w-4 h-4 animate-spin" /> : <Undo2 className="w-4 h-4" />}
                              Rollback
                            </Button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </CardContent>
            </Card>
          ) : null}

          {brokenAlbums.length === 0 && (!scanId || movedIncompletes.length === 0) ? (
            <Alert>
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>
                No albums currently need incomplete review.
              </AlertDescription>
            </Alert>
          ) : (
            <div className="grid gap-4">
              {renderAlbumSection('confirmed', confirmedAlbums)}
              {renderAlbumSection('likely', likelyAlbums)}
              {renderAlbumSection('uncertain', uncertainAlbums)}
              {renderAlbumSection('not_incomplete', notIncompleteAlbums)}
            </div>
          )}
        </>
      )}

      <ScanMoveReviewDialog
        move={selectedMove}
        open={Boolean(selectedMove)}
        onOpenChange={(open) => {
          if (!open) setSelectedMove(null);
        }}
        restoring={selectedMove ? restoring === selectedMove.move_id : false}
        onRestore={async (moveId) => {
          await restoreMoves([moveId]);
          setSelectedMove(null);
        }}
      />

      <Dialog
        open={Boolean(selectedAlbum)}
        onOpenChange={(open) => {
          if (!open) {
            setSelectedAlbum(null);
            setSelectedAlbumDetail(null);
            setSelectedAlbumDetailError(null);
          }
        }}
      >
        <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Incomplete album review</DialogTitle>
            <DialogDescription>
              PMDA now only keeps albums here when the local folder has zero readable tracks or obvious numbering holes. Provider tracklists stay visible as reference only.
            </DialogDescription>
          </DialogHeader>

          {selectedAlbumDetailLoading ? (
            <div className="flex items-center justify-center py-10">
              <Loader2 className="w-6 h-6 animate-spin text-primary" />
            </div>
          ) : selectedAlbumDetailError ? (
            <Alert>
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>{selectedAlbumDetailError}</AlertDescription>
            </Alert>
          ) : selectedAlbumDetail ? (
            <div className="space-y-4">
              <div className="flex gap-4">
                <div className="w-24 h-24 rounded-2xl overflow-hidden border border-border/70 bg-muted shrink-0">
                  {selectedAlbumDetail.thumb_url ? (
                    <AuthenticatedImage
                      src={selectedAlbumDetail.thumb_url}
                      alt={selectedAlbumDetail.album_title}
                      className="w-full h-full object-cover"
                      fallback={<Disc3 className="w-6 h-6 text-muted-foreground" />}
                    />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center">
                      <Disc3 className="w-6 h-6 text-muted-foreground" />
                    </div>
                  )}
                </div>
                <div className="min-w-0 flex-1 space-y-2">
                  <div>
                    <h3 className="text-lg font-semibold">{selectedAlbumDetail.album_title}</h3>
                    <p className="text-sm text-muted-foreground">{selectedAlbumDetail.artist}</p>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {selectedAlbumDetail.strict_match_provider ? (
                      <ProviderBadge provider={selectedAlbumDetail.strict_match_provider} prefix="Match" className="text-[10px]" />
                    ) : null}
                    {selectedAlbumDetail.metadata_source ? (
                      <ProviderBadge provider={selectedAlbumDetail.metadata_source} prefix="Source" className="text-[10px]" />
                    ) : null}
                    {selectedAlbumDetail.expected_track_count ? (
                      <Badge variant="outline">Expected {selectedAlbumDetail.expected_track_count}</Badge>
                    ) : null}
                    <Badge variant="outline">Actual {selectedAlbumDetail.actual_track_count}</Badge>
                    {fmtMissingIndices(selectedAlbumDetail.missing_indices) ? (
                      <Badge variant="outline">Missing {fmtMissingIndices(selectedAlbumDetail.missing_indices)}</Badge>
                    ) : null}
                    {selectedAlbumDetail.review_status === 'ignored' ? <Badge variant="secondary">Ignored</Badge> : null}
                    {selectedAlbumDetail.classification ? (
                      <Badge variant={selectedAlbumDetail.quarantine_eligible ? 'destructive' : 'secondary'}>
                        {humanizeClassification(selectedAlbumDetail.classification)}
                      </Badge>
                    ) : null}
                    {typeof selectedAlbumDetail.classification_confidence === 'number' && Number.isFinite(selectedAlbumDetail.classification_confidence) ? (
                      <Badge variant="outline">{Math.round(selectedAlbumDetail.classification_confidence * 100)}%</Badge>
                    ) : null}
                    {selectedAlbumDetail.ai_verdict ? (
                      <Badge variant={aiVerdictBadgeVariant(selectedAlbumDetail.ai_verdict)}>
                        {aiVerdictLabel(selectedAlbumDetail.ai_verdict)}
                      </Badge>
                    ) : null}
                    {typeof selectedAlbumDetail.ai_verdict?.confidence === 'number' && Number.isFinite(selectedAlbumDetail.ai_verdict.confidence) ? (
                      <Badge variant="outline">AI {Math.round(selectedAlbumDetail.ai_verdict.confidence * 100)}%</Badge>
                    ) : null}
                  </div>
                  {selectedAlbumDetail.reason_summary ? (
                    <Alert>
                      <AlertCircle className="h-4 w-4" />
                      <AlertDescription>{selectedAlbumDetail.reason_summary}</AlertDescription>
                    </Alert>
                  ) : null}
                  <div className="flex flex-wrap gap-2">
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() => void refreshSelectedAlbumAiVerdict()}
                      disabled={selectedAlbumAiRefreshing || selectedAlbumDetailLoading}
                    >
                      {selectedAlbumAiRefreshing ? <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" /> : null}
                      Run AI shadow
                    </Button>
                  </div>
                  {selectedAlbumDetail.ai_verdict?.evidence_summary ? (
                    <Alert>
                      <CheckCircle2 className="h-4 w-4" />
                      <AlertDescription>{selectedAlbumDetail.ai_verdict.evidence_summary}</AlertDescription>
                    </Alert>
                  ) : null}
                  {selectedAlbumDetail.strict_reject_reason ? (
                    <div className="text-xs text-muted-foreground">
                      Strict reject reason: {humanizeReason(selectedAlbumDetail.strict_reject_reason)}
                    </div>
                  ) : null}
                  {Array.isArray(selectedAlbumDetail.ai_verdict?.reasoning_flags) && selectedAlbumDetail.ai_verdict.reasoning_flags.length > 0 ? (
                    <div className="flex flex-wrap gap-2">
                      {selectedAlbumDetail.ai_verdict.reasoning_flags.map((flag) => (
                        <Badge key={flag} variant="outline">
                          {humanizeReason(flag)}
                        </Badge>
                      ))}
                    </div>
                  ) : null}
                  {selectedAlbumDetail.folder_path ? (
                    <div className="text-xs text-muted-foreground break-all">
                      Folder: {selectedAlbumDetail.folder_path}
                    </div>
                  ) : null}
                  {selectedAlbumDetail.musicbrainz_release_group_id ? (
                    <div className="text-xs text-muted-foreground break-all">
                      MusicBrainz release group: {selectedAlbumDetail.musicbrainz_release_group_id}
                    </div>
                  ) : null}
                </div>
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                <Card>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base">Local folder</CardTitle>
                    <CardDescription>
                      {selectedAlbumDetail.local_tracks.length} track(s) detected on disk
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    {selectedAlbumDetail.local_tracks.length === 0 ? (
                      <p className="text-sm text-muted-foreground">No local track detail could be read from the folder.</p>
                    ) : (
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
                            {selectedAlbumDetail.local_tracks.map((track, index) => (
                              <tr key={`local-${index}-${track.track_num || index + 1}`} className="border-t border-border/50">
                                <td className="px-3 py-2 text-xs text-muted-foreground tabular-nums">
                                  {Number(track.track_num || index + 1)}
                                </td>
                                <td className="px-3 py-2">
                                  <div className="font-medium">{track.title || `Track ${index + 1}`}</div>
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
                    )}
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base">Expected edition</CardTitle>
                    <CardDescription>
                      {selectedAlbumDetail.expected_tracks.length} expected track(s) from the trusted provider
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    {selectedAlbumDetail.expected_tracks.length === 0 ? (
                      <p className="text-sm text-muted-foreground">No provider tracklist is currently available for comparison.</p>
                    ) : (
                      <div className="rounded-lg border border-border/60 overflow-hidden">
                        <table className="w-full text-sm">
                          <thead className="bg-muted/40 text-muted-foreground">
                            <tr>
                              <th className="px-3 py-2 text-left text-[10px] uppercase tracking-wide w-16">#</th>
                              <th className="px-3 py-2 text-left text-[10px] uppercase tracking-wide">Expected track</th>
                            </tr>
                          </thead>
                          <tbody>
                            {selectedAlbumDetail.expected_tracks.map((track) => (
                              <tr key={`expected-${track.index}`} className="border-t border-border/50">
                                <td className="px-3 py-2 text-xs text-muted-foreground tabular-nums">{track.index}</td>
                                <td className="px-3 py-2">{track.title || `Track ${track.index}`}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>

              <div className="flex flex-wrap items-center gap-2">
                <Button
                  size="sm"
                  variant="ghost"
                  className="gap-1.5"
                  onClick={() => void setAlbumReviewStatus(selectedAlbumDetail, selectedAlbumDetail.review_status === 'ignored' ? 'pending' : 'ignored')}
                  disabled={reviewingAlbum !== null}
                >
                  {reviewingAlbum === selectedAlbumDetail.album_id ? <Loader2 className="w-4 h-4 animate-spin" /> : <CheckCircle2 className="w-4 h-4" />}
                  {selectedAlbumDetail.review_status === 'ignored' ? 'Back to review' : 'Ignore for now'}
                </Button>
              </div>
            </div>
          ) : null}
        </DialogContent>
      </Dialog>
    </div>
  );
}
