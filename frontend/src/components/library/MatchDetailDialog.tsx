import { useCallback, useEffect, useMemo, useState } from 'react';
import { Check, ExternalLink, Loader2, RefreshCw, RotateCcw, Sparkles } from 'lucide-react';
import { useNavigate } from 'react-router-dom';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { ProviderBadge } from '@/components/providers/ProviderBadge';
import { ProviderInline } from '@/components/providers/ProviderInline';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Separator } from '@/components/ui/separator';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { useToast } from '@/hooks/use-toast';
import * as api from '@/lib/api';
import { badgeKindClass, matchTypeBadgeClass } from '@/lib/badgeStyles';
import { normalizeProviderId, providerLabel } from '@/lib/providerMeta';
import { cn } from '@/lib/utils';

function providerId(provider?: string | null): string {
  return normalizeProviderId(provider);
}

function cleanAttemptReason(raw?: string | null): string {
  const line = String(raw || '').trim().replace(/\s+/g, ' ');
  if (!line) return '';
  return line.length > 130 ? `${line.slice(0, 127)}...` : line;
}

function matchBadgeVariant(value?: string | null): 'secondary' | 'outline' | 'destructive' {
  const v = String(value || '').toUpperCase();
  if (v === 'MATCH') return 'secondary';
  if (v === 'SOFT_MATCH') return 'outline';
  return 'destructive';
}

function formatTs(ts?: number | null): string {
  const n = Number(ts || 0);
  if (!Number.isFinite(n) || n <= 0) return '—';
  try {
    return new Date(n * 1000).toLocaleString();
  } catch {
    return '—';
  }
}

type MatchEntity =
  | { kind: 'album'; albumId: number }
  | { kind: 'artist'; artistId: number };

interface MatchDetailDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  entity: MatchEntity | null;
  onDataChanged?: () => void;
}

export function MatchDetailDialog({ open, onOpenChange, entity, onDataChanged }: MatchDetailDialogProps) {
  const navigate = useNavigate();
  const { toast } = useToast();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rematchBusy, setRematchBusy] = useState(false);
  const [reviewGenerateBusy, setReviewGenerateBusy] = useState(false);
  const [artistAiBusy, setArtistAiBusy] = useState(false);
  const [coverApplyBusyUrl, setCoverApplyBusyUrl] = useState<string | null>(null);
  const [albumDetail, setAlbumDetail] = useState<api.AlbumMatchDetailResponse | null>(null);
  const [artistDetail, setArtistDetail] = useState<api.ArtistMatchDetailResponse | null>(null);

  const isAlbum = entity?.kind === 'album';
  const isArtist = entity?.kind === 'artist';

  const load = useCallback(async () => {
    if (!open || !entity) return;
    setLoading(true);
    setError(null);
    try {
      if (entity.kind === 'album') {
        const res = await api.getAlbumMatchDetail(entity.albumId);
        setAlbumDetail(res);
        setArtistDetail(null);
      } else {
        const res = await api.getArtistMatchDetail(entity.artistId);
        setArtistDetail(res);
        setAlbumDetail(null);
      }
    } catch (e) {
      setAlbumDetail(null);
      setArtistDetail(null);
      setError(e instanceof Error ? e.message : 'Failed to load match detail');
    } finally {
      setLoading(false);
    }
  }, [entity, open]);

  useEffect(() => {
    void load();
  }, [load]);

  const doRematch = useCallback(async () => {
    if (!entity || rematchBusy) return;
    setRematchBusy(true);
    try {
      if (entity.kind === 'album') {
        const res = await api.rematchAlbum(entity.albumId);
        toast({
          title: 'Album rematch completed',
          description: res.summary || 'Manual rematch finished.',
        });
      } else {
        const res = await api.rematchArtist(entity.artistId);
        toast({
          title: 'Artist rematch started',
          description: `Queued ${res.total || 0} album(s) for rematch.`,
        });
      }
      await load();
      if (onDataChanged) onDataChanged();
    } catch (e) {
      toast({
        title: 'Rematch failed',
        description: e instanceof Error ? e.message : 'Unable to start rematch',
        variant: 'destructive',
      });
    } finally {
      setRematchBusy(false);
    }
  }, [entity, load, onDataChanged, rematchBusy, toast]);

  const applyAlternativeCover = useCallback(
    async (cover: api.AlbumAlternativeCover) => {
      if (!entity || entity.kind !== 'album') return;
      const url = String(cover.cover_url || '').trim();
      if (!url) return;
      setCoverApplyBusyUrl(url);
      try {
        await api.selectAlbumCover(entity.albumId, {
          cover_url: url,
          provider: cover.provider || null,
          source_url: cover.source_url || null,
        });
        toast({
          title: 'Cover updated',
          description: `Manual cover set from ${providerLabel(cover.provider)}.`,
        });
        await load();
        if (onDataChanged) onDataChanged();
      } catch (e) {
        toast({
          title: 'Cover update failed',
          description: e instanceof Error ? e.message : 'Unable to apply selected cover',
          variant: 'destructive',
        });
      } finally {
        setCoverApplyBusyUrl(null);
      }
    },
    [entity, load, onDataChanged, toast]
  );

  const doGenerateAlbumReview = useCallback(async () => {
    if (!entity || entity.kind !== 'album' || reviewGenerateBusy) return;
    setReviewGenerateBusy(true);
    try {
      const res = await api.generateAlbumReview(entity.albumId);
      toast({
        title: 'Album review generated',
        description: res.source ? `Source: ${res.source}` : 'Review updated.',
      });
      await load();
      if (onDataChanged) onDataChanged();
    } catch (e) {
      toast({
        title: 'Review generation failed',
        description: e instanceof Error ? e.message : 'Unable to generate album review',
        variant: 'destructive',
      });
    } finally {
      setReviewGenerateBusy(false);
    }
  }, [entity, load, onDataChanged, reviewGenerateBusy, toast]);

  const doArtistAiEnrich = useCallback(async () => {
    if (!entity || entity.kind !== 'artist' || artistAiBusy) return;
    setArtistAiBusy(true);
    try {
      const res = await api.enrichArtistWithAI(entity.artistId);
      toast({
        title: 'AI enrichment started',
        description: `Targeted ${res.profiles_targeted || 0} album profile(s).`,
      });
      await load();
      if (onDataChanged) onDataChanged();
    } catch (e) {
      toast({
        title: 'AI enrichment failed',
        description: e instanceof Error ? e.message : 'Unable to start artist AI enrichment',
        variant: 'destructive',
      });
    } finally {
      setArtistAiBusy(false);
    }
  }, [artistAiBusy, entity, load, onDataChanged, toast]);

  const headerTitle = useMemo(() => {
    if (!entity) return 'Match detail';
    if (entity.kind === 'album') return 'Album match detail';
    return 'Artist match detail';
  }, [entity]);

  const headerDesc = useMemo(() => {
    if (!entity) return '';
    if (entity.kind === 'album') {
      return 'Provider chain, retained source, confidence, and manual rematch history for this album.';
    }
    return 'Source and match report for this artist and all albums currently indexed.';
  }, [entity]);

  const providerAttemptsViz = useMemo(() => {
    if (!albumDetail) return [];
    const attempts = Array.isArray(albumDetail.provider_attempts) ? albumDetail.provider_attempts : [];
    const crosscheck = Array.isArray(albumDetail.provider_crosscheck) ? albumDetail.provider_crosscheck : [];
    const links = Array.isArray(albumDetail.links) ? albumDetail.links : [];
    const strictProvider = providerId(albumDetail.decision?.strict_match_provider);
    const selectedProvider = providerId(albumDetail.decision?.selected_provider);
    const metadataProvider = providerId(albumDetail.decision?.metadata_source);

    const crossByProvider = new Map<string, api.ProviderCrosscheckItem>();
    for (const row of crosscheck) {
      const key = providerId(row.provider);
      if (!key || crossByProvider.has(key)) continue;
      crossByProvider.set(key, row);
    }
    const linksByProvider = new Map<string, api.AlbumMatchDetailResponse['links'][number]>();
    for (const link of links) {
      const key = providerId(link.provider);
      if (!key || linksByProvider.has(key)) continue;
      linksByProvider.set(key, link);
    }
    const attemptsByProvider = new Map<string, api.MatchProviderAttempt>();
    for (const attempt of attempts) {
      const key = providerId(attempt.provider);
      if (!key || attemptsByProvider.has(key)) continue;
      attemptsByProvider.set(key, attempt);
    }

    const providerSet = new Set<string>();
    for (const key of attemptsByProvider.keys()) providerSet.add(key);
    for (const key of crossByProvider.keys()) providerSet.add(key);
    if (selectedProvider) providerSet.add(selectedProvider);
    if (strictProvider) providerSet.add(strictProvider);
    if (metadataProvider) providerSet.add(metadataProvider);

    const orderedProviders: string[] = [];
    for (const provider of albumDetail.providers_order || []) {
      const key = providerId(provider);
      if (key && providerSet.has(key) && !orderedProviders.includes(key)) {
        orderedProviders.push(key);
      }
    }
    for (const key of providerSet) {
      if (!orderedProviders.includes(key)) orderedProviders.push(key);
    }

    return orderedProviders.map((provider) => {
      const attempt = attemptsByProvider.get(provider);
      const cross = crossByProvider.get(provider);
      const link = linksByProvider.get(provider);
      const selected = Boolean(attempt?.selected || cross?.selected || (selectedProvider && selectedProvider === provider));
      const strict = Boolean(albumDetail.decision?.strict_match_verified && strictProvider && strictProvider === provider);

      let score = 0;
      if (typeof cross?.confidence === 'number' && Number.isFinite(cross.confidence)) {
        score = Math.max(0, Math.min(100, Math.round(cross.confidence * 100)));
      } else {
        score = 30;
        if (attempt) score += 20;
        if (metadataProvider && metadataProvider === provider) score += 15;
        if (selected) score += 30;
        if (strict) score = 100;
        score = Math.max(10, Math.min(100, score));
      }

      const reasons: string[] = [];
      if (strict) reasons.push('Strict identity + tracklist checks passed.');
      if (selected && !strict) reasons.push('Retained by fallback pipeline as best source.');
      if (metadataProvider && metadataProvider === provider) reasons.push('Persisted as metadata source in PMDA state.');
      if (cross) {
        reasons.push(
          `Scores: title ${(cross.title_score * 100).toFixed(0)}% · artist ${(cross.artist_score * 100).toFixed(0)}% · tracklist ${(cross.track_score * 100).toFixed(0)}%`,
        );
        if (cross.soft_match_ok && cross.soft_match_reason) {
          reasons.push(`Soft-match reason: ${cross.soft_match_reason}`);
        }
        if (cross.strict_reject_reason && !cross.strict_match_verified) {
          reasons.push(`Strict reject: ${cross.strict_reject_reason}`);
        }
      } else {
        const note = (attempt?.notes || [])
          .map((line) => cleanAttemptReason(line))
          .find((line) => Boolean(line));
        if (note) reasons.push(note);
      }
      if (reasons.length === 0) reasons.push('Attempt logged, but no detailed reason persisted for this run.');

      const releaseArtist = String(cross?.artist || link?.release_artist || albumDetail.artist_name || '').trim();
      const releaseTitle = String(cross?.title || link?.release_title || albumDetail.album_title || '').trim();
      const releaseYear =
        (typeof cross?.year === 'number' && cross.year > 1800 ? cross.year : null) ??
        (typeof link?.release_year === 'number' && link.release_year > 1800 ? link.release_year : null) ??
        (typeof albumDetail.year === 'number' && albumDetail.year > 1800 ? albumDetail.year : null);
      const releaseTextBase = [releaseArtist, releaseTitle].filter(Boolean).join(' — ') || releaseTitle || releaseArtist || 'Unknown release';
      const releaseText = releaseYear ? `${releaseTextBase} (${releaseYear})` : releaseTextBase;

      return {
        provider,
        label: providerLabel(provider),
        selected,
        strict,
        score,
        reasons,
        sourceUrl: String(cross?.source_url || attempt?.url || link?.url || '').trim() || null,
        releaseText,
      };
    });
  }, [albumDetail]);

  const sourceLinkRows = useMemo(() => {
    if (!albumDetail) return [];
    const crosscheck = Array.isArray(albumDetail.provider_crosscheck) ? albumDetail.provider_crosscheck : [];
    const crossByProvider = new Map<string, api.ProviderCrosscheckItem>();
    for (const row of crosscheck) {
      const key = providerId(row.provider);
      if (!key || crossByProvider.has(key)) continue;
      crossByProvider.set(key, row);
    }
    const rows: Array<{
      key: string;
      provider: string;
      providerName: string;
      url: string;
      releaseText: string;
      detailText: string | null;
    }> = [];
    const seen = new Set<string>();
    for (const link of albumDetail.links || []) {
      const href = String(link.url || '').trim();
      if (!href || seen.has(href)) continue;
      seen.add(href);
      const provider = providerId(link.provider);
      const cross = crossByProvider.get(provider);
      const relArtist = String(link.release_artist || cross?.artist || albumDetail.artist_name || '').trim();
      const relTitle = String(link.release_title || cross?.title || albumDetail.album_title || '').trim();
      const relYear =
        (typeof link.release_year === 'number' && link.release_year > 1800 ? link.release_year : null) ??
        (typeof cross?.year === 'number' && cross.year > 1800 ? cross.year : null) ??
        (typeof albumDetail.year === 'number' && albumDetail.year > 1800 ? albumDetail.year : null);
      const releaseBase = [relArtist, relTitle].filter(Boolean).join(' — ') || relTitle || relArtist || link.label || 'Unknown release';
      const releaseText = relYear ? `${releaseBase} (${relYear})` : releaseBase;
      const providerRef = String(link.provider_ref || cross?.provider_id || '').trim();
      const detailText = providerRef ? `ID: ${providerRef}` : (String(link.label || '').trim() || null);
      rows.push({
        key: `${provider}:${href}`,
        provider,
        providerName: providerLabel(provider),
        url: href,
        releaseText,
        detailText,
      });
    }
    return rows;
  }, [albumDetail]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-4xl max-h-[88vh] overflow-hidden p-0">
        <DialogHeader className="px-6 pt-6 pb-3 border-b border-border/60">
          <DialogTitle>{headerTitle}</DialogTitle>
          <DialogDescription>{headerDesc}</DialogDescription>
        </DialogHeader>

        <ScrollArea className="h-[70vh]">
          <div className="px-6 py-5 space-y-5">
            {loading ? (
              <div className="flex items-center justify-center py-16">
                <Loader2 className="w-8 h-8 animate-spin text-primary" />
              </div>
            ) : null}

            {!loading && error ? (
              <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
                {error}
              </div>
            ) : null}

            {!loading && !error && isAlbum && albumDetail ? (
              <div className="space-y-4">
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant={matchBadgeVariant(albumDetail.match_type)} className={matchTypeBadgeClass(albumDetail.match_type)}>{albumDetail.match_type}</Badge>
                  {albumDetail.confidence != null ? (
                    <Badge variant="outline" className={badgeKindClass('duration')}>Confidence {(albumDetail.confidence * 100).toFixed(0)}%</Badge>
                  ) : null}
                  <Badge variant={albumDetail.ai?.used ? 'secondary' : 'outline'} className={`gap-1.5 ${albumDetail.ai?.used ? badgeKindClass('status_match') : badgeKindClass('muted')}`}>
                    <Sparkles className="w-3 h-3" />
                    AI {albumDetail.ai?.used ? 'used' : 'not used'}
                  </Badge>
                  {albumDetail.ai?.used && albumDetail.ai?.source ? (
                    <Badge variant="outline" className={badgeKindClass('source')}>AI source: {albumDetail.ai.source}</Badge>
                  ) : null}
                  {albumDetail.decision?.selected_provider ? (
                    <ProviderBadge provider={albumDetail.decision.selected_provider} prefix="Provider" />
                  ) : null}
                </div>

                <div className="rounded-md border border-border/70 p-3 space-y-2">
                  <div className="text-sm font-medium">{albumDetail.artist_name} - {albumDetail.album_title}</div>
                  <div className="text-xs text-muted-foreground">Updated: {formatTs(albumDetail.updated_at)}</div>
                  <div className="flex flex-wrap gap-2 text-xs">
                    <Badge variant="outline" className={badgeKindClass('count')}>Tracks: {albumDetail.track_count}</Badge>
                    {albumDetail.year ? <Badge variant="outline" className={badgeKindClass('year')}>Year: {albumDetail.year}</Badge> : null}
                    {albumDetail.decision?.metadata_source ? (
                      <ProviderBadge provider={albumDetail.decision.metadata_source} prefix="Metadata" />
                    ) : null}
                  </div>
                </div>

                <div className="grid gap-3 md:grid-cols-2">
                  <div className="rounded-md border border-border/70 p-3 space-y-2">
                    <div className="text-sm font-medium">Artwork & description</div>
                    <div className="text-xs text-muted-foreground">
                      Cover: {albumDetail.cover?.has_cover ? 'yes' : 'no'}
                      {albumDetail.cover?.origin ? ` (${albumDetail.cover.origin})` : ''}
                    </div>
                    {albumDetail.cover?.provider ? (
                      <ProviderInline label="Cover provider:" provider={albumDetail.cover.provider} />
                    ) : null}
                    <div className="text-xs text-muted-foreground">Artist image: {albumDetail.artist_image?.has_image ? 'yes' : 'no'}</div>
                    {albumDetail.artist_image?.provider ? (
                      <ProviderInline label="Artist image provider:" provider={albumDetail.artist_image.provider} />
                    ) : null}
                    {albumDetail.description?.album_profile_source ? (
                      <ProviderInline label="Album description source:" provider={albumDetail.description.album_profile_source} />
                    ) : (
                      <div className="text-xs text-muted-foreground">Album description source: n/a</div>
                    )}
                    <div className="text-xs text-muted-foreground">
                      Auto soft-match AI review: {albumDetail.description?.soft_match_ai_auto_enabled ? 'enabled' : 'disabled'}
                    </div>
                    {albumDetail.description?.artist_profile_source ? (
                      <ProviderInline label="Artist description source:" provider={albumDetail.description.artist_profile_source} />
                    ) : (
                      <div className="text-xs text-muted-foreground">Artist description source: n/a</div>
                    )}
                  </div>

                  <div className="rounded-md border border-border/70 p-3 space-y-2">
                    <div className="text-sm font-medium">PMDA state</div>
                    <div className="flex flex-wrap gap-2 text-xs">
                      <Badge variant={albumDetail.pmda?.matched ? 'secondary' : 'outline'} className={albumDetail.pmda?.matched ? badgeKindClass('status_match') : badgeKindClass('muted')}>Matched: {albumDetail.pmda?.matched ? 'yes' : 'no'}</Badge>
                      <Badge variant={albumDetail.pmda?.cover ? 'secondary' : 'outline'} className={albumDetail.pmda?.cover ? badgeKindClass('status_match') : badgeKindClass('muted')}>Cover: {albumDetail.pmda?.cover ? 'yes' : 'no'}</Badge>
                      <Badge variant={albumDetail.pmda?.artist_image ? 'secondary' : 'outline'} className={albumDetail.pmda?.artist_image ? badgeKindClass('status_match') : badgeKindClass('muted')}>Artist image: {albumDetail.pmda?.artist_image ? 'yes' : 'no'}</Badge>
                      <Badge variant={albumDetail.pmda?.complete ? 'secondary' : 'outline'} className={albumDetail.pmda?.complete ? badgeKindClass('status_match') : badgeKindClass('muted')}>Complete: {albumDetail.pmda?.complete ? 'yes' : 'no'}</Badge>
                    </div>
                    <ProviderInline label="PMDA match provider:" provider={albumDetail.pmda?.match_provider} />
                    <ProviderInline label="PMDA cover provider:" provider={albumDetail.pmda?.cover_provider} />
                    <ProviderInline label="PMDA artist provider:" provider={albumDetail.pmda?.artist_provider} />
                  </div>
                </div>

                <div className="rounded-md border border-border/70 p-3 space-y-2">
                  <div className="text-sm font-medium">Provider attempts</div>
                  {providerAttemptsViz.length === 0 ? (
                    <div className="text-xs text-muted-foreground">No attempt log persisted yet for this album.</div>
                  ) : (
                    <div className="space-y-2">
                      {providerAttemptsViz.map((attempt, idx) => (
                        <div key={`attempt-${idx}`} className="rounded border border-border/60 px-2.5 py-2 text-xs">
                          <div className="flex flex-wrap items-center gap-2">
                            <ProviderBadge
                              provider={attempt.provider}
                              labelOverride={attempt.label}
                              variant={attempt.selected ? 'secondary' : 'outline'}
                            />
                            {attempt.selected ? (
                              <Badge variant="secondary" className={attempt.strict ? badgeKindClass('status_match') : badgeKindClass('status_soft')}>
                                {attempt.strict ? 'selected (strict)' : 'selected'}
                              </Badge>
                            ) : (
                              <Badge variant="outline" className={badgeKindClass('muted')}>
                                not selected
                              </Badge>
                            )}
                            <Badge variant="outline" className={badgeKindClass('duration')}>
                              Score {attempt.score}%
                            </Badge>
                            <span className="text-muted-foreground truncate max-w-full">{attempt.releaseText}</span>
                            {attempt.sourceUrl ? (
                              <a href={attempt.sourceUrl} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-primary hover:underline">
                                source
                                <ExternalLink className="w-3 h-3" />
                              </a>
                            ) : null}
                          </div>
                          <div className="mt-2">
                            <div className="h-1.5 w-full rounded bg-muted overflow-hidden">
                              <div
                                className={cn(
                                  'h-full transition-all',
                                  attempt.strict
                                    ? 'bg-emerald-500'
                                    : attempt.selected
                                      ? 'bg-sky-500'
                                      : 'bg-zinc-400',
                                )}
                                style={{ width: `${Math.max(4, Math.min(100, attempt.score))}%` }}
                              />
                            </div>
                          </div>
                          <div className="mt-1.5 text-muted-foreground whitespace-pre-wrap">
                            {attempt.reasons.slice(0, 2).join(' • ')}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                <div className="rounded-md border border-border/70 p-3 space-y-2">
                  <div className="text-sm font-medium">Provider cross-check (all providers)</div>
                  {(albumDetail.provider_crosscheck || []).length === 0 ? (
                    <div className="text-xs text-muted-foreground">No cross-check data available.</div>
                  ) : (
                    <div className="space-y-2">
                      {(albumDetail.provider_crosscheck || []).map((row, idx) => (
                        <div key={`cross-${idx}`} className="rounded border border-border/60 px-2.5 py-2 text-xs">
                          <div className="flex flex-wrap items-center gap-2">
                            <ProviderBadge provider={row.provider} variant={row.selected ? 'secondary' : 'outline'} />
                            {row.selected ? <Badge variant="secondary" className={badgeKindClass('status_match')}>selected</Badge> : null}
                            {row.strict_match_verified ? <Badge variant="secondary" className={badgeKindClass('status_match')}>strict</Badge> : null}
                            {row.soft_match_ok ? <Badge variant="outline" className={badgeKindClass('status_soft')}>soft ok</Badge> : null}
                            {row.ai_used ? <Badge variant="secondary" className={badgeKindClass('source')}>AI</Badge> : null}
                            <Badge variant="outline" className={badgeKindClass('duration')}>Conf {(Number(row.confidence || 0) * 100).toFixed(0)}%</Badge>
                            {row.track_count > 0 ? (
                              <Badge variant="outline" className={badgeKindClass('count')}>Tracks {row.track_count}/{row.local_track_count}</Badge>
                            ) : null}
                            {row.source_url ? (
                              <a href={row.source_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-primary hover:underline">
                                source
                                <ExternalLink className="w-3 h-3" />
                              </a>
                            ) : null}
                          </div>
                          <div className="mt-1 text-muted-foreground">
                            {row.artist || 'Unknown artist'} - {row.title || 'Unknown title'}
                            {row.year ? ` (${row.year})` : ''}
                          </div>
                          {row.strict_reject_reason ? <div className="mt-1 text-muted-foreground">Strict reject: {row.strict_reject_reason}</div> : null}
                          {row.soft_match_reason ? <div className="mt-1 text-muted-foreground">Soft reason: {row.soft_match_reason}</div> : null}
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                <div className="rounded-md border border-border/70 p-3 space-y-2">
                  <div className="text-sm font-medium">Alternative covers</div>
                  {(albumDetail.alternative_covers || []).length === 0 ? (
                    <div className="text-xs text-muted-foreground">No alternative cover source available.</div>
                  ) : (
                    <div className="space-y-2">
                      {(albumDetail.alternative_covers || []).map((cover, idx) => {
                        const busy = coverApplyBusyUrl === cover.cover_url;
                        return (
                          <div key={`cover-alt-${idx}`} className="rounded border border-border/60 px-2.5 py-2 text-xs">
                            <div className="flex flex-wrap items-center gap-2">
                              <ProviderBadge provider={cover.provider} variant={cover.selected ? 'secondary' : 'outline'} />
                              {cover.selected ? (
                                <Badge variant="secondary" className={`gap-1 ${badgeKindClass('status_match')}`}>
                                  <Check className="w-3 h-3" />
                                  selected
                                </Badge>
                              ) : (
                                <Button
                                  type="button"
                                  size="sm"
                                  variant="outline"
                                  className="h-7 text-xs"
                                  disabled={busy || rematchBusy}
                                  onClick={() => void applyAlternativeCover(cover)}
                                >
                                  {busy ? <Loader2 className="w-3.5 h-3.5 mr-1 animate-spin" /> : null}
                                  Use this cover
                                </Button>
                              )}
                              {cover.source_url ? (
                                <a href={cover.source_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-primary hover:underline">
                                  source
                                  <ExternalLink className="w-3 h-3" />
                                </a>
                              ) : null}
                            </div>
                            <div className="mt-2">
                              <img
                                src={cover.cover_url}
                                alt={`${cover.label} cover`}
                                className="h-20 w-20 rounded-md border border-border/60 object-cover"
                                loading="lazy"
                              />
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>

                <div className="rounded-md border border-border/70 p-3 space-y-2">
                  <div className="text-sm font-medium">Other versions</div>
                  {!albumDetail.versions?.has_alternatives ? (
                    <div className="text-xs text-muted-foreground">No alternate editions detected.</div>
                  ) : (
                    <div className="space-y-2">
                      <div className="text-xs text-muted-foreground flex flex-wrap items-center gap-2">
                        <span>{albumDetail.versions.count} version(s) found on</span>
                        <ProviderBadge provider={albumDetail.versions.provider} className="h-5 px-2 py-0 text-[10px]" />
                        {albumDetail.versions.source_url ? (
                          <>
                            {' '}
                            <a
                              href={albumDetail.versions.source_url}
                              target="_blank"
                              rel="noreferrer"
                              className="inline-flex items-center gap-1 text-primary hover:underline"
                            >
                              Open source
                              <ExternalLink className="w-3 h-3" />
                            </a>
                          </>
                        ) : null}
                      </div>
                      {(albumDetail.versions.items || []).slice(0, 10).map((v, idx) => (
                        <div key={`ver-${idx}`} className="rounded border border-border/60 px-2 py-1.5 text-xs">
                          <div className="font-medium">{v.title || 'Untitled release'}</div>
                          <div className="text-muted-foreground">
                            {v.date || 'n/a'}
                            {v.country ? ` • ${v.country}` : ''}
                            {v.status ? ` • ${v.status}` : ''}
                          </div>
                          {v.url ? (
                            <a href={v.url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-primary hover:underline mt-1">
                              Open release
                              <ExternalLink className="w-3 h-3" />
                            </a>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                <div className="rounded-md border border-border/70 p-3 space-y-2">
                  <div className="text-sm font-medium">Source links</div>
                  {sourceLinkRows.length === 0 ? (
                    <div className="text-xs text-muted-foreground">No provider links available.</div>
                  ) : (
                    <div className="rounded border border-border/60 overflow-hidden">
                      <Table>
                        <TableHeader>
                          <TableRow className="hover:bg-transparent">
                            <TableHead className="h-9 text-[10px] uppercase tracking-wide">Provider</TableHead>
                            <TableHead className="h-9 text-[10px] uppercase tracking-wide">Release identified</TableHead>
                            <TableHead className="h-9 text-right text-[10px] uppercase tracking-wide">Open</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {sourceLinkRows.map((row) => (
                            <TableRow key={row.key}>
                              <TableCell className="py-2">
                                <ProviderBadge provider={row.provider} labelOverride={row.providerName} className="h-5 px-2 py-0 text-[10px]" />
                              </TableCell>
                              <TableCell className="py-2">
                                <a
                                  href={row.url}
                                  target="_blank"
                                  rel="noreferrer"
                                  className="font-medium text-primary hover:underline"
                                >
                                  {row.releaseText}
                                </a>
                                {row.detailText ? (
                                  <div className="text-[11px] text-muted-foreground mt-0.5">{row.detailText}</div>
                                ) : null}
                              </TableCell>
                              <TableCell className="py-2 text-right">
                                <a
                                  href={row.url}
                                  target="_blank"
                                  rel="noreferrer"
                                  className="inline-flex items-center gap-1 text-primary hover:underline"
                                  title={`Open on ${row.providerName}`}
                                >
                                  Open
                                  <ExternalLink className="w-3 h-3" />
                                </a>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  )}
                </div>

                <div className="rounded-md border border-border/70 p-3 space-y-2">
                  <div className="text-sm font-medium">Manual history</div>
                  {(albumDetail.history || []).length === 0 ? (
                    <div className="text-xs text-muted-foreground">No manual run history yet.</div>
                  ) : (
                    <div className="space-y-2">
                      {albumDetail.history.slice(0, 10).map((run) => (
                        <div key={`run-${run.id}`} className="rounded border border-border/60 px-2.5 py-2 text-xs">
                          <div className="flex flex-wrap items-center gap-2">
                            <Badge variant="outline" className={badgeKindClass('source')}>{run.run_kind || 'manual'}</Badge>
                            <Badge variant={matchBadgeVariant(run.match_type)} className={matchTypeBadgeClass(run.match_type)}>{run.match_type || 'N/A'}</Badge>
                            {run.provider_used ? <ProviderBadge provider={run.provider_used} /> : null}
                            <span className="text-muted-foreground">{formatTs(run.created_at)}</span>
                          </div>
                          {run.summary ? <div className="mt-1 text-muted-foreground">{run.summary}</div> : null}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            ) : null}

            {!loading && !error && isArtist && artistDetail ? (
              <div className="space-y-4">
                <div className="rounded-md border border-border/70 p-3 space-y-2">
                  <div className="text-sm font-medium">{artistDetail.artist_name}</div>
                  <div className="flex flex-wrap gap-2 text-xs">
                    <Badge variant="secondary" className={badgeKindClass('status_match')}>MATCH: {artistDetail.summary?.matched || 0}</Badge>
                    <Badge variant="outline" className={badgeKindClass('status_soft')}>SOFT: {artistDetail.summary?.soft_matched || 0}</Badge>
                    <Badge variant="destructive" className={badgeKindClass('status_no_match')}>NO_MATCH: {artistDetail.summary?.not_matched || 0}</Badge>
                    <Badge variant="outline" className={badgeKindClass('count')}>Total: {artistDetail.summary?.total || 0}</Badge>
                  </div>
                  {artistDetail.artist_profile_source ? (
                    <ProviderInline label="Artist profile source:" provider={artistDetail.artist_profile_source} />
                  ) : (
                    <div className="text-xs text-muted-foreground">Artist profile source: n/a</div>
                  )}
                  <div className="text-xs text-muted-foreground">
                    Artist image: {artistDetail.artist_image?.has_image ? 'yes' : 'no'}
                  </div>
                  {artistDetail.artist_image?.provider ? (
                    <ProviderInline label="Artist image provider:" provider={artistDetail.artist_image.provider} />
                  ) : null}
                </div>

                <div className="rounded-md border border-border/70 p-3 space-y-2">
                  <div className="text-sm font-medium">Albums ({(artistDetail.albums || []).length})</div>
                  {(artistDetail.albums || []).length === 0 ? (
                    <div className="text-xs text-muted-foreground">No albums indexed.</div>
                  ) : (
                    <div className="space-y-2">
                      {artistDetail.albums.map((album) => (
                        <div key={`artist-alb-${album.album_id}`} className="rounded border border-border/60 px-2.5 py-2 text-xs">
                          <div className="flex flex-wrap items-center gap-2">
                            <button
                              type="button"
                              className="font-medium hover:underline"
                              onClick={() => navigate(`/library/album/${album.album_id}`)}
                            >
                              {album.album_title}
                            </button>
                            <Badge variant={matchBadgeVariant(album.match_type)} className={matchTypeBadgeClass(album.match_type)}>{album.match_type}</Badge>
                            {album.selected_provider ? <ProviderBadge provider={album.selected_provider} /> : null}
                            {album.year ? <Badge variant="outline" className={badgeKindClass('year')}>{album.year}</Badge> : null}
                          </div>
                          {album.strict_reject_reason ? (
                            <div className="mt-1 text-muted-foreground">{album.strict_reject_reason}</div>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                <div className="rounded-md border border-border/70 p-3 space-y-2">
                  <div className="text-sm font-medium">Manual history</div>
                  {(artistDetail.history || []).length === 0 ? (
                    <div className="text-xs text-muted-foreground">No manual run history yet.</div>
                  ) : (
                    <div className="space-y-2">
                      {artistDetail.history.slice(0, 12).map((run) => (
                        <div key={`artist-run-${run.id}`} className="rounded border border-border/60 px-2.5 py-2 text-xs">
                          <div className="flex flex-wrap items-center gap-2">
                            <Badge variant="outline" className={badgeKindClass('count')}>album #{run.album_id || 0}</Badge>
                            <Badge variant="outline" className={badgeKindClass('source')}>{run.run_kind || 'manual'}</Badge>
                            <Badge variant={matchBadgeVariant(run.match_type)} className={matchTypeBadgeClass(run.match_type)}>{run.match_type || 'N/A'}</Badge>
                            {run.provider_used ? <ProviderBadge provider={run.provider_used} /> : null}
                            <span className="text-muted-foreground">{formatTs(run.created_at)}</span>
                          </div>
                          {run.summary ? <div className="mt-1 text-muted-foreground">{run.summary}</div> : null}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            ) : null}
          </div>
        </ScrollArea>

        <Separator />
        <div className="px-6 py-4 flex flex-wrap items-center justify-between gap-2">
          <Button type="button" variant="outline" className="gap-2" onClick={() => void load()} disabled={loading || rematchBusy || reviewGenerateBusy || artistAiBusy}>
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
            Refresh details
          </Button>
          <div className="flex flex-wrap items-center gap-2">
            {isAlbum ? (
              <Button
                type="button"
                variant="outline"
                className="gap-2"
                onClick={() => void doGenerateAlbumReview()}
                disabled={loading || rematchBusy || reviewGenerateBusy || !entity}
              >
                {reviewGenerateBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
                Generate AI review
              </Button>
            ) : null}
            {isArtist ? (
              <Button
                type="button"
                variant="outline"
                className="gap-2"
                onClick={() => void doArtistAiEnrich()}
                disabled={loading || rematchBusy || reviewGenerateBusy || artistAiBusy || !entity}
              >
                {artistAiBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
                AI enrich artist
              </Button>
            ) : null}
            <Button type="button" className="gap-2" onClick={() => void doRematch()} disabled={loading || rematchBusy || reviewGenerateBusy || artistAiBusy || !entity}>
              {rematchBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <RotateCcw className="w-4 h-4" />}
              {isAlbum ? 'Rematch this album' : 'Rematch this artist'}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
