import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import {
  Loader2,
  BarChart3,
  Database,
  Sparkles,
  Image,
  Tag,
  HardDrive,
  Clock,
  Gauge,
  Layers,
  AlertCircle,
  Server,
  RefreshCw,
} from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { format } from 'date-fns';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Filler,
  Tooltip,
  Legend,
} from 'chart.js';
import { Bar, Line, Doughnut, Pie } from 'react-chartjs-2';
import type {
  BenchmarkReportSummary,
  CacheControlMetrics,
  ScanAICostSummary,
  ScanHistoryEntry,
  ScanProgress,
} from '@/lib/api';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ProviderBadge } from '@/components/providers/ProviderBadge';
import { StatisticsPageNav } from '@/components/statistics/StatisticsPageNav';
import { PipelineTracePanel } from '@/components/statistics/PipelineTracePanel';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useAuth } from '@/contexts/AuthContext';
import * as api from '@/lib/api';
import { toast } from 'sonner';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Filler,
  Tooltip,
  Legend,
);

const PERIODS = [
  { key: 'daily', label: 'Daily', hours: 24 },
  { key: 'weekly', label: 'Weekly', hours: 24 * 7 },
  { key: 'monthly', label: 'Monthly', hours: 24 * 30 },
  { key: 'last', label: 'Last scan', hours: 0 },
  { key: 'forever', label: 'Forever', hours: 0 },
] as const;
const AI_COST_ALBUM_LIMIT = 200;
const AI_COST_SCAN_ROWS_LIMIT = 30;

type PeriodKey = (typeof PERIODS)[number]['key'];
type StatsTab = 'overview' | 'metadata' | 'quality' | 'operations' | 'duplicates' | 'incompletes' | 'pipeline' | 'ai' | 'benchmark';

interface ScanSnapshot {
  scanId: number;
  startTime: number;
  albumsScanned: number;
  duplicatesFound: number;
  spaceSavedMb: number;
  albumsMoved: number;
  durationSeconds: number;
  mode: string;
  matchedAlbums: number;
  albumsWithMbId: number;
  mbVerifiedByAi: number;
  discogsMatches: number;
  lastfmMatches: number;
  bandcampMatches: number;
  withTags: number;
  withCover: number;
  withArtistImage: number;
  fullyComplete: number;
  withoutTags: number;
  withoutCover: number;
  withoutArtistImage: number;
  brokenAlbums: number;
  audioCacheHits: number;
  audioCacheMisses: number;
  mbCacheHits: number;
  mbCacheMisses: number;
  aiCallsTotal: number;
  aiCallsProviderIdentity: number;
  aiCallsMbVerify: number;
  aiCallsWebMbid: number;
  aiCallsVision: number;
  aiErrorsTotal: number;
  aiTokensTotal: number;
  aiCostUsdTotal: number;
  aiUnpricedCalls: number;
}

interface AggregateStats {
  scans: number;
  albumsScanned: number;
  duplicatesFound: number;
  spaceSavedMb: number;
  albumsMoved: number;
  durationSeconds: number;
  matchedAlbums: number;
  albumsWithMbId: number;
  mbVerifiedByAi: number;
  discogsMatches: number;
  lastfmMatches: number;
  bandcampMatches: number;
  withTags: number;
  withCover: number;
  withArtistImage: number;
  fullyComplete: number;
  withoutTags: number;
  withoutCover: number;
  withoutArtistImage: number;
  brokenAlbums: number;
  audioCacheHits: number;
  audioCacheMisses: number;
  mbCacheHits: number;
  mbCacheMisses: number;
  aiCallsTotal: number;
  aiCallsProviderIdentity: number;
  aiCallsMbVerify: number;
  aiCallsWebMbid: number;
  aiCallsVision: number;
  aiErrorsTotal: number;
  aiTokensTotal: number;
  aiCostUsdTotal: number;
  aiUnpricedCalls: number;
}

function n(value: number | null | undefined): number {
  return value ?? 0;
}

function formatMb(mb: number): string {
  if (mb >= 1024) return `${(mb / 1024).toFixed(1)} GB`;
  return `${mb.toFixed(0)} MB`;
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let idx = 0;
  let val = bytes;
  while (val >= 1024 && idx < units.length - 1) {
    val /= 1024;
    idx += 1;
  }
  return `${val.toFixed(val >= 100 || idx === 0 ? 0 : 1)} ${units[idx]}`;
}

function formatUsd(value: number): string {
  const v = Number.isFinite(value) ? Math.max(0, value) : 0;
  if (v >= 100) return `$${v.toFixed(2)}`;
  if (v >= 1) return `$${v.toFixed(3)}`;
  return `$${v.toFixed(4)}`;
}

function formatPercent(value: number, total: number): string {
  if (total <= 0) return 'N/A';
  return `${((value / total) * 100).toFixed(1)}%`;
}

function formatDuration(seconds: number): string {
  if (seconds <= 0) return '0s';
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

const chartOptions = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: { position: 'top' as const },
  },
};

function normalizeScan(entry: ScanHistoryEntry): ScanSnapshot {
  const summary = entry.summary_json ?? null;
  const albumsScanned = n(summary?.albums_scanned ?? entry.albums_scanned);
  const withoutTags = n(summary?.albums_without_complete_tags ?? entry.albums_without_complete_tags);
  const withoutCover = n(summary?.albums_without_album_image ?? entry.albums_without_album_image);
  const withoutArtistImage = n(summary?.albums_without_artist_image ?? entry.albums_without_artist_image);
  const albumsWithMbId = n(
    summary?.strict_matched_albums
      ?? summary?.albums_with_mb_id
      ?? Math.max(0, albumsScanned - n(summary?.albums_without_mb_id ?? entry.albums_without_mb_id)),
  );

  const discogsMatches = n(summary?.scan_discogs_matched);
  const lastfmMatches = n(summary?.scan_lastfm_matched);
  const bandcampMatches = n(summary?.scan_bandcamp_matched);

  const matchedAlbums = Math.min(albumsScanned, Math.max(0, albumsWithMbId));

  return {
    scanId: entry.scan_id,
    startTime: n(entry.start_time),
    albumsScanned,
    duplicatesFound: n(summary?.duplicate_groups_count ?? entry.duplicate_groups_count ?? entry.duplicates_found),
    spaceSavedMb: n(summary?.space_saved_mb_this_scan ?? entry.space_saved_mb),
    albumsMoved: n(summary?.dupes_moved_this_scan ?? entry.albums_moved),
    durationSeconds: n(summary?.duration_seconds ?? entry.duration_seconds),
    mode: String((summary as { library_mode?: string } | null)?.library_mode || 'unknown'),
    matchedAlbums,
    albumsWithMbId,
    mbVerifiedByAi: n(summary?.mb_verified_by_ai),
    discogsMatches,
    lastfmMatches,
    bandcampMatches,
    withTags: Math.max(0, albumsScanned - withoutTags),
    withCover: Math.max(0, albumsScanned - withoutCover),
    withArtistImage: Math.max(0, albumsScanned - withoutArtistImage),
    fullyComplete: Math.max(0, albumsScanned - Math.max(withoutTags, withoutCover, withoutArtistImage)),
    withoutTags,
    withoutCover,
    withoutArtistImage,
    brokenAlbums: n(summary?.broken_albums_count ?? entry.broken_albums_count),
    audioCacheHits: n(summary?.audio_cache_hits),
    audioCacheMisses: n(summary?.audio_cache_misses),
    mbCacheHits: n(summary?.mb_cache_hits),
    mbCacheMisses: n(summary?.mb_cache_misses),
    aiCallsTotal: n(summary?.ai_calls_total),
    aiCallsProviderIdentity: n(summary?.ai_calls_provider_identity),
    aiCallsMbVerify: n(summary?.ai_calls_mb_verify),
    aiCallsWebMbid: n(summary?.ai_calls_web_mbid),
    aiCallsVision: n(summary?.ai_calls_vision),
    aiErrorsTotal: Array.isArray(summary?.ai_errors) ? summary!.ai_errors!.length : 0,
    aiTokensTotal: n(summary?.ai_tokens_total ?? entry.ai_tokens_total),
    aiCostUsdTotal: n(summary?.ai_cost_usd_total ?? entry.ai_cost_usd_total),
    aiUnpricedCalls: n(summary?.ai_unpriced_calls ?? entry.ai_unpriced_calls),
  };
}

function normalizeLiveScan(progress: ScanProgress, fallbackStartTime: number): ScanSnapshot {
  const scopeAlbums = Math.max(
    n(progress.scan_run_scope_albums_included),
    n(progress.total_albums),
    n(progress.detected_albums_total),
  );
  const processedAlbums = Math.max(
    n(progress.scan_processed_albums_count),
    n(progress.scan_published_albums_count),
    n(progress.scan_postprocessed_albums_count),
    n(progress.mb_done_count),
    n(progress.format_done_count),
  );
  const totalAlbums = scopeAlbums > 0 ? Math.min(processedAlbums, scopeAlbums) : processedAlbums;
  const withoutTags = n(progress.albums_without_complete_tags);
  const withoutCover = n(progress.albums_without_album_image);
  const withoutArtistImage = n(progress.albums_without_artist_image);
  const albumsWithMbId = Math.max(0, totalAlbums - n(progress.albums_without_mb_id));
  const discogsMatches = n(progress.scan_discogs_matched);
  const lastfmMatches = n(progress.scan_lastfm_matched);
  const bandcampMatches = n(progress.scan_bandcamp_matched);
  const matchedAlbums = Math.min(totalAlbums, Math.max(0, albumsWithMbId));
  const startTime = n(progress.scan_start_time ?? fallbackStartTime);
  const now = Math.floor(Date.now() / 1000);
  const durationSeconds = startTime > 0 ? Math.max(0, now - startTime) : 0;

  return {
    scanId: -1,
    startTime: startTime > 0 ? startTime : now,
    albumsScanned: totalAlbums,
    duplicatesFound: n(progress.duplicate_groups_count),
    spaceSavedMb: 0,
    albumsMoved: 0,
    durationSeconds,
    mode: 'live',
    matchedAlbums: Math.min(processedAlbums, matchedAlbums),
    albumsWithMbId,
    mbVerifiedByAi: 0,
    discogsMatches,
    lastfmMatches,
    bandcampMatches,
    withTags: Math.max(0, processedAlbums - withoutTags),
    withCover: Math.max(0, processedAlbums - withoutCover),
    withArtistImage: Math.max(0, processedAlbums - withoutArtistImage),
    fullyComplete: Math.max(0, processedAlbums - Math.max(withoutTags, withoutCover, withoutArtistImage)),
    withoutTags,
    withoutCover,
    withoutArtistImage,
    brokenAlbums: n(progress.broken_albums_count),
    audioCacheHits: n(progress.audio_cache_hits),
    audioCacheMisses: n(progress.audio_cache_misses),
    mbCacheHits: n(progress.mb_cache_hits),
    mbCacheMisses: n(progress.mb_cache_misses),
    aiCallsTotal: 0,
    aiCallsProviderIdentity: 0,
    aiCallsMbVerify: 0,
    aiCallsWebMbid: 0,
    aiCallsVision: 0,
    aiErrorsTotal: 0,
    aiTokensTotal: 0,
    aiCostUsdTotal: 0,
    aiUnpricedCalls: 0,
  };
}

function aggregate(scans: ScanSnapshot[]): AggregateStats {
  return scans.reduce<AggregateStats>(
    (acc, scan) => {
      acc.scans += 1;
      acc.albumsScanned += scan.albumsScanned;
      acc.duplicatesFound += scan.duplicatesFound;
      acc.spaceSavedMb += scan.spaceSavedMb;
      acc.albumsMoved += scan.albumsMoved;
      acc.durationSeconds += scan.durationSeconds;
      acc.matchedAlbums += scan.matchedAlbums;
      acc.albumsWithMbId += scan.albumsWithMbId;
      acc.mbVerifiedByAi += scan.mbVerifiedByAi;
      acc.discogsMatches += scan.discogsMatches;
      acc.lastfmMatches += scan.lastfmMatches;
      acc.bandcampMatches += scan.bandcampMatches;
      acc.withTags += scan.withTags;
      acc.withCover += scan.withCover;
      acc.withArtistImage += scan.withArtistImage;
      acc.fullyComplete += scan.fullyComplete;
      acc.withoutTags += scan.withoutTags;
      acc.withoutCover += scan.withoutCover;
      acc.withoutArtistImage += scan.withoutArtistImage;
      acc.brokenAlbums += scan.brokenAlbums;
      acc.audioCacheHits += scan.audioCacheHits;
      acc.audioCacheMisses += scan.audioCacheMisses;
      acc.mbCacheHits += scan.mbCacheHits;
      acc.mbCacheMisses += scan.mbCacheMisses;
      acc.aiCallsTotal += scan.aiCallsTotal;
      acc.aiCallsProviderIdentity += scan.aiCallsProviderIdentity;
      acc.aiCallsMbVerify += scan.aiCallsMbVerify;
      acc.aiCallsWebMbid += scan.aiCallsWebMbid;
      acc.aiCallsVision += scan.aiCallsVision;
      acc.aiErrorsTotal += scan.aiErrorsTotal;
      acc.aiTokensTotal += scan.aiTokensTotal;
      acc.aiCostUsdTotal += scan.aiCostUsdTotal;
      acc.aiUnpricedCalls += scan.aiUnpricedCalls;
      return acc;
    },
    {
      scans: 0,
      albumsScanned: 0,
      duplicatesFound: 0,
      spaceSavedMb: 0,
      albumsMoved: 0,
      durationSeconds: 0,
      matchedAlbums: 0,
      albumsWithMbId: 0,
      mbVerifiedByAi: 0,
      discogsMatches: 0,
      lastfmMatches: 0,
      bandcampMatches: 0,
      withTags: 0,
      withCover: 0,
      withArtistImage: 0,
      fullyComplete: 0,
      withoutTags: 0,
      withoutCover: 0,
      withoutArtistImage: 0,
      brokenAlbums: 0,
      audioCacheHits: 0,
      audioCacheMisses: 0,
      mbCacheHits: 0,
      mbCacheMisses: 0,
      aiCallsTotal: 0,
      aiCallsProviderIdentity: 0,
      aiCallsMbVerify: 0,
      aiCallsWebMbid: 0,
      aiCallsVision: 0,
      aiErrorsTotal: 0,
      aiTokensTotal: 0,
      aiCostUsdTotal: 0,
      aiUnpricedCalls: 0,
    },
  );
}

function DeltaPill({
  current,
  previous,
  positiveIsGood = true,
  suffix = '',
}: {
  current: number;
  previous: number;
  positiveIsGood?: boolean;
  suffix?: string;
}) {
  const diff = current - previous;
  if (diff === 0) {
    return <span className="text-xs text-muted-foreground">vs prev: 0{suffix}</span>;
  }
  const good = diff > 0 ? positiveIsGood : !positiveIsGood;
  const sign = diff > 0 ? '+' : '';
  return (
    <span className={good ? 'text-xs text-emerald-600' : 'text-xs text-red-500'}>
      vs prev: {sign}{diff.toFixed(Math.abs(diff) < 10 ? 1 : 0)}{suffix}
    </span>
  );
}

function StatCard({
  title,
  value,
  description,
  icon,
  delta,
}: {
  title: string;
  value: string;
  description?: string;
  icon: ReactNode;
  delta?: ReactNode;
}) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs font-medium flex items-center gap-1.5 text-muted-foreground uppercase tracking-wider">
          {icon}
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-1">
        <p className="text-3xl font-bold tracking-tight text-foreground tabular-nums">{value}</p>
        {description && <p className="text-xs text-muted-foreground">{description}</p>}
        {delta}
      </CardContent>
    </Card>
  );
}

export default function Statistics() {
  const { isAdmin } = useAuth();
  const [period, setPeriod] = useState<PeriodKey>('last');
  const [activeTab, setActiveTab] = useState<StatsTab>('overview');
  const [aiCostScope, setAiCostScope] = useState<'scan' | 'lifecycle'>('lifecycle');
  const [watcherRestarting, setWatcherRestarting] = useState(false);
  const liveScanStartRef = useRef<number>(Math.floor(Date.now() / 1000));

  const { data: history = [], isLoading } = useQuery({
    queryKey: ['scan-history'],
    queryFn: api.getScanHistory,
  });

  const { data: scanProgress } = useQuery<ScanProgress>({
    queryKey: ['scan-progress'],
    queryFn: api.getScanProgress,
    refetchInterval: 2000,
    refetchIntervalInBackground: true,
  });
  const { data: configData } = useQuery({
    queryKey: ['config-ai-billing'],
    queryFn: api.getConfig,
    staleTime: 60_000,
    refetchInterval: false,
  });
  const {
    data: cacheControl,
    refetch: refetchCacheControl,
  } = useQuery<CacheControlMetrics>({
    queryKey: ['stats-cache-control'],
    queryFn: () => api.getCacheControlMetrics(false),
    refetchInterval: (scanProgress?.scanning || scanProgress?.post_processing) ? 5000 : 15000,
    refetchIntervalInBackground: true,
  });
  const {
    data: watcherRuntimeStatus,
    refetch: refetchWatcherRuntimeStatus,
  } = useQuery({
    queryKey: ['files-watcher-status'],
    queryFn: api.getFilesWatcherStatus,
    refetchInterval: (scanProgress?.scanning || scanProgress?.post_processing) ? 5000 : 15000,
    refetchIntervalInBackground: true,
  });
  const {
    data: libraryStats,
  } = useQuery({
    queryKey: ['library-stats-library'],
    queryFn: api.getLibraryStatsLibrary,
    refetchInterval: (scanProgress?.scanning || scanProgress?.post_processing) ? 5000 : 30000,
    refetchIntervalInBackground: true,
  });
  const isLiveRunActive = Boolean(scanProgress?.scanning || scanProgress?.post_processing);
  const liveRunArtistsTotal = n(scanProgress?.artists_total);
  const liveRunAlbumsTotal = n(scanProgress?.total_albums);
  const liveDetectedArtistsTotal = n(scanProgress?.detected_artists_total);
  const liveDetectedAlbumsTotal = n(scanProgress?.detected_albums_total);
  const liveSkippedArtists = n(scanProgress?.resume_skipped_artists);
  const liveSkippedAlbums = n(scanProgress?.resume_skipped_albums);
  const isRunScopePreparing = Boolean(
    isLiveRunActive &&
    (scanProgress?.phase === 'preparing_run_scope' || scanProgress?.scan_run_scope_preparing),
  );
  const isPreScanPhase = Boolean(isLiveRunActive && scanProgress?.phase === 'pre_scan');
  const preScanStage = String(scanProgress?.scan_discovery_stage || '');
  const preScanRootsDone = n(scanProgress?.scan_discovery_roots_done);
  const preScanRootsTotal = n(scanProgress?.scan_discovery_roots_total);
  const preScanEntriesScanned = n(scanProgress?.scan_discovery_entries_scanned);
  const preScanFilesFound = n(scanProgress?.scan_discovery_files_found);
  const preScanAlbumsFound = n(scanProgress?.scan_discovery_albums_found);
  const preScanArtistsFound = n(scanProgress?.scan_discovery_artists_found);
  const preScanProgressTotal = n(
    scanProgress?.scan_preplan_total
      || scanProgress?.scan_discovery_albums_total
      || scanProgress?.scan_discovery_folders_total,
  );
  const preScanProgressDoneRaw = n(
    scanProgress?.scan_preplan_done
      || scanProgress?.scan_discovery_albums_done
      || scanProgress?.scan_discovery_folders_done,
  );
  const preScanProgressDone = preScanProgressTotal > 0
    ? Math.min(preScanProgressDoneRaw, preScanProgressTotal)
    : preScanProgressDoneRaw;
  const runScopePrepStage = String(scanProgress?.scan_run_scope_stage || '');
  const runScopePrepDoneRaw = n(scanProgress?.scan_run_scope_done);
  const runScopePrepTotal = n(scanProgress?.scan_run_scope_total);
  const runScopePrepDone = runScopePrepTotal > 0
    ? Math.min(runScopePrepDoneRaw, runScopePrepTotal)
    : runScopePrepDoneRaw;
  const runScopeIncludedArtists = n(scanProgress?.scan_run_scope_artists_included);
  const runScopeIncludedAlbums = n(scanProgress?.scan_run_scope_albums_included);
  const runScopePending = (isPreScanPhase || isRunScopePreparing) && liveDetectedArtistsTotal === 0 && liveDetectedAlbumsTotal === 0;
  const liveTracksDetected = n(scanProgress?.scan_tracks_detected_total);
  const liveTracksLibraryKept = n(scanProgress?.scan_tracks_library_kept);
  const liveTracksMovedDupes = n(scanProgress?.scan_tracks_moved_dupes);
  const liveTracksMovedIncomplete = n(scanProgress?.scan_tracks_moved_incomplete);
  const liveTracksUnaccounted = n(scanProgress?.scan_tracks_unaccounted);
  const liveTrackScopeLabel = scanProgress?.scan_type === 'changed_only' ? 'run scope (delta)' : 'run scope';

  useEffect(() => {
    if (isLiveRunActive) {
      if (scanProgress.scan_start_time) {
        liveScanStartRef.current = n(scanProgress.scan_start_time);
      }
      return;
    }
    liveScanStartRef.current = Math.floor(Date.now() / 1000);
  }, [isLiveRunActive, scanProgress?.scan_start_time]);

  const completedScans = useMemo(() => {
    const entries = history
      .filter((entry) => entry.entry_type === 'scan' && entry.status === 'completed' && Boolean(entry.end_time))
      .sort((a, b) => n(b.start_time) - n(a.start_time));
    return entries.map(normalizeScan);
  }, [history]);
  const latestCompletedScanId = useMemo(() => {
    if (completedScans.length <= 0) return null;
    return Number.isFinite(completedScans[0].scanId) ? completedScans[0].scanId : null;
  }, [completedScans]);
  const {
    data: scanMovesAudit,
  } = useQuery({
    queryKey: ['scan-moves-audit', latestCompletedScanId],
    queryFn: () => api.getScanMovesAudit(latestCompletedScanId ?? undefined),
    enabled: (latestCompletedScanId ?? 0) > 0,
    refetchInterval: 60000,
    refetchIntervalInBackground: true,
  });
  const {
    data: scanMovesSummary,
  } = useQuery({
    queryKey: ['scan-moves-summary', latestCompletedScanId],
    queryFn: () => api.getScanMovesSummary(latestCompletedScanId ?? 0),
    enabled: (latestCompletedScanId ?? 0) > 0,
    refetchInterval: 60000,
    refetchIntervalInBackground: true,
  });
  const {
    data: duplicateMoves = [],
  } = useQuery({
    queryKey: ['scan-moves', latestCompletedScanId, 'dedupe'],
    queryFn: () => api.getScanMoves(latestCompletedScanId ?? 0, { reason: 'dedupe', status: 'all' }),
    enabled: (latestCompletedScanId ?? 0) > 0,
    refetchInterval: 60000,
    refetchIntervalInBackground: true,
  });
  const {
    data: incompleteMoves = [],
  } = useQuery({
    queryKey: ['scan-moves', latestCompletedScanId, 'incomplete'],
    queryFn: () => api.getScanMoves(latestCompletedScanId ?? 0, { reason: 'incomplete', status: 'all' }),
    enabled: (latestCompletedScanId ?? 0) > 0,
    refetchInterval: 60000,
    refetchIntervalInBackground: true,
  });
  const {
    data: benchmarkReportsResponse,
  } = useQuery({
    queryKey: ['benchmark-reports'],
    queryFn: () => api.getBenchmarkReports(80),
    refetchInterval: (scanProgress?.scanning || scanProgress?.post_processing) ? 5000 : 30000,
    refetchIntervalInBackground: true,
  });
  const benchmarkReports = useMemo<BenchmarkReportSummary[]>(
    () => benchmarkReportsResponse?.reports ?? [],
    [benchmarkReportsResponse?.reports],
  );
  const latestBenchmark = benchmarkReports[0] ?? null;
  const benchmarkAverageScore = useMemo(() => {
    if (benchmarkReports.length <= 0) return 0;
    const total = benchmarkReports.reduce((acc, row) => acc + Number(row.score || 0), 0);
    return total / benchmarkReports.length;
  }, [benchmarkReports]);
  const benchmarkBestScore = useMemo(
    () => benchmarkReports.reduce((acc, row) => Math.max(acc, Number(row.score || 0)), 0),
    [benchmarkReports],
  );
  const summarizeMoveReasons = useCallback((moves: api.ScanMove[]) => {
    const counts = new Map<string, number>();
    for (const move of moves) {
      const label = String(move.reason_label || move.decision_reason || move.move_reason || 'Other').trim() || 'Other';
      counts.set(label, (counts.get(label) || 0) + 1);
    }
    const entries = Array.from(counts.entries()).sort((a, b) => b[1] - a[1]).slice(0, 8);
    return {
      labels: entries.map(([label]) => label),
      values: entries.map(([, value]) => value),
    };
  }, []);
  const summarizeMoveProviders = useCallback((moves: api.ScanMove[]) => {
    const counts = new Map<string, number>();
    for (const move of moves) {
      const label = String(move.decision_provider || 'Unknown').trim() || 'Unknown';
      counts.set(label, (counts.get(label) || 0) + 1);
    }
    const entries = Array.from(counts.entries()).sort((a, b) => b[1] - a[1]).slice(0, 8);
    return {
      labels: entries.map(([label]) => label),
      values: entries.map(([, value]) => value),
    };
  }, []);
  const summarizeMoveStatuses = useCallback((moves: api.ScanMove[]) => {
    const counts = new Map<string, number>();
    for (const move of moves) {
      const label = String(move.status || 'unknown').trim() || 'unknown';
      counts.set(label, (counts.get(label) || 0) + 1);
    }
    const entries = Array.from(counts.entries()).sort((a, b) => b[1] - a[1]);
    return {
      labels: entries.map(([label]) => label),
      values: entries.map(([, value]) => value),
    };
  }, []);
  const duplicateReasonSummary = useMemo(() => summarizeMoveReasons(duplicateMoves), [duplicateMoves, summarizeMoveReasons]);
  const duplicateProviderSummary = useMemo(() => summarizeMoveProviders(duplicateMoves), [duplicateMoves, summarizeMoveProviders]);
  const duplicateStatusSummary = useMemo(() => summarizeMoveStatuses(duplicateMoves), [duplicateMoves, summarizeMoveStatuses]);
  const incompleteReasonSummary = useMemo(() => summarizeMoveReasons(incompleteMoves), [incompleteMoves, summarizeMoveReasons]);
  const incompleteProviderSummary = useMemo(() => summarizeMoveProviders(incompleteMoves), [incompleteMoves, summarizeMoveProviders]);
  const incompleteStatusSummary = useMemo(() => summarizeMoveStatuses(incompleteMoves), [incompleteMoves, summarizeMoveStatuses]);

  const liveScan = useMemo(() => {
    if (!scanProgress || !isLiveRunActive) return null;
    return normalizeLiveScan(scanProgress, liveScanStartRef.current);
  }, [isLiveRunActive, scanProgress]);

  const historicalSelectedScans = useMemo(() => {
    if (period === 'last') {
      return completedScans.length > 0 ? [completedScans[0]] : [];
    }
    if (period === 'forever') {
      return completedScans;
    }
    const hours = PERIODS.find((p) => p.key === period)?.hours ?? 0;
    const cutoff = Date.now() / 1000 - hours * 3600;
    return completedScans.filter((scan) => scan.startTime >= cutoff);
  }, [completedScans, period]);

  const selectedScans = useMemo(() => {
    if (!liveScan) return historicalSelectedScans;
    if (period === 'last') return [liveScan];
    return [liveScan, ...historicalSelectedScans];
  }, [historicalSelectedScans, liveScan, period]);

  const baselineScans = useMemo(() => {
    if (selectedScans.length === 0) return [];
    const selectedIds = new Set(selectedScans.map((s) => s.scanId));
    return completedScans.filter((s) => !selectedIds.has(s.scanId)).slice(0, selectedScans.length);
  }, [completedScans, selectedScans]);

  const current = useMemo(() => aggregate(selectedScans), [selectedScans]);
  const previous = useMemo(() => aggregate(baselineScans), [baselineScans]);

  const latestSelected = selectedScans[0] ?? null;
  const latestCompletedEntry = useMemo(() => {
    return [...history]
      .filter((entry) => entry.entry_type === 'scan' && entry.status === 'completed' && Boolean(entry.end_time))
      .sort((a, b) => n(b.start_time) - n(a.start_time))[0] ?? null;
  }, [history]);
  const aiCostScanId = useMemo(() => {
    if (latestSelected && Number.isFinite(latestSelected.scanId) && latestSelected.scanId > 0) {
      return latestSelected.scanId;
    }
    const fallback = latestCompletedEntry?.scan_id ?? 0;
    return fallback > 0 ? fallback : null;
  }, [latestCompletedEntry?.scan_id, latestSelected]);
  const { data: latestAiCostSummary } = useQuery<ScanAICostSummary>({
    queryKey: ['scan-ai-cost-summary', aiCostScanId, aiCostScope],
    queryFn: () =>
      api.getScanAICostSummary(aiCostScanId as number, {
        includeLifecycle: aiCostScope === 'lifecycle',
        groupBy: 'analysis_type',
      }),
    enabled: (aiCostScanId ?? 0) > 0,
    refetchInterval: (scanProgress?.scanning || scanProgress?.post_processing) ? 5000 : 30000,
    refetchIntervalInBackground: true,
  });
  const { data: latestAiCostByAlbum } = useQuery<ScanAICostSummary>({
    queryKey: ['scan-ai-cost-album', aiCostScanId, aiCostScope, AI_COST_ALBUM_LIMIT],
    queryFn: () =>
      api.getScanAICostSummary(aiCostScanId as number, {
        includeLifecycle: aiCostScope === 'lifecycle',
        groupBy: 'album',
        limit: AI_COST_ALBUM_LIMIT,
      }),
    enabled: (aiCostScanId ?? 0) > 0,
    refetchInterval: (scanProgress?.scanning || scanProgress?.post_processing) ? 5000 : 30000,
    refetchIntervalInBackground: true,
  });
  const { data: latestAiCostByProvider } = useQuery<ScanAICostSummary>({
    queryKey: ['scan-ai-cost-provider', aiCostScanId, aiCostScope],
    queryFn: () =>
      api.getScanAICostSummary(aiCostScanId as number, {
        includeLifecycle: aiCostScope === 'lifecycle',
        groupBy: 'provider',
      }),
    enabled: (aiCostScanId ?? 0) > 0,
    refetchInterval: (scanProgress?.scanning || scanProgress?.post_processing) ? 5000 : 30000,
    refetchIntervalInBackground: true,
  });
  const { data: latestAiCostByAuthMode } = useQuery<ScanAICostSummary>({
    queryKey: ['scan-ai-cost-auth-mode', aiCostScanId, aiCostScope],
    queryFn: () =>
      api.getScanAICostSummary(aiCostScanId as number, {
        includeLifecycle: aiCostScope === 'lifecycle',
        groupBy: 'auth_mode',
      }),
    enabled: (aiCostScanId ?? 0) > 0,
    refetchInterval: (scanProgress?.scanning || scanProgress?.post_processing) ? 5000 : 30000,
    refetchIntervalInBackground: true,
  });
  const latestSummaryTracksDetected = n(latestCompletedEntry?.summary_json?.scan_tracks_detected_total);
  const latestSummaryTracksLibraryKept = n(latestCompletedEntry?.summary_json?.scan_tracks_library_kept);
  const latestSummaryTracksMovedDupes = n(latestCompletedEntry?.summary_json?.scan_tracks_moved_dupes);
  const latestSummaryTracksMovedIncomplete = n(latestCompletedEntry?.summary_json?.scan_tracks_moved_incomplete);
  const latestSummaryTracksUnaccounted = n(latestCompletedEntry?.summary_json?.scan_tracks_unaccounted);
  const hasLatestTrackReconciliation = latestSummaryTracksDetected > 0
    || latestSummaryTracksLibraryKept > 0
    || latestSummaryTracksMovedDupes > 0
    || latestSummaryTracksMovedIncomplete > 0
    || latestSummaryTracksUnaccounted > 0;
  const modeSummary = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const scan of selectedScans) {
      const key = scan.mode || 'unknown';
      counts[key] = (counts[key] ?? 0) + 1;
    }
    return Object.entries(counts)
      .map(([mode, count]) => `${mode}:${count}`)
      .join(' · ');
  }, [selectedScans]);

  const sourceLabel = useMemo(() => {
    if (isLiveRunActive && period === 'last') {
      if (liveDetectedArtistsTotal > 0 || liveDetectedAlbumsTotal > 0) {
        return `source = live scan (run scope ${liveRunArtistsTotal}/${liveRunAlbumsTotal} from detected ${liveDetectedArtistsTotal}/${liveDetectedAlbumsTotal})`;
      }
      return 'source = live scan (updates every 2s)';
    }
    if (isLiveRunActive) {
      return `source = live scan + ${historicalSelectedScans.length} completed scan(s)`;
    }
    if (!latestSelected) return 'No completed scan in this period';
    if (period === 'last') {
      return `source = scan #${latestSelected.scanId} (${format(new Date(latestSelected.startTime * 1000), 'yyyy-MM-dd HH:mm')})`;
    }
    return `source = ${selectedScans.length} scan(s) aggregated (${modeSummary || 'unknown'})`;
  }, [
    historicalSelectedScans.length,
    isLiveRunActive,
    latestSelected,
    liveDetectedAlbumsTotal,
    liveDetectedArtistsTotal,
    liveRunAlbumsTotal,
    liveRunArtistsTotal,
    modeSummary,
    period,
    selectedScans.length,
  ]);
  const latestAiTotals = latestAiCostSummary?.totals;
  const latestAiBreakdown = latestAiCostSummary?.breakdown ?? [];
  const latestAiAlbumBreakdown = latestAiCostByAlbum?.breakdown ?? [];
  const latestAiProviderBreakdown = latestAiCostByProvider?.breakdown ?? [];
  const latestAiAuthModeBreakdown = latestAiCostByAuthMode?.breakdown ?? [];
  const recentAiScanRows = completedScans.slice(0, AI_COST_SCAN_ROWS_LIMIT);
  const liveGuardCallsUsed = n(scanProgress?.scan_ai_guard_calls_used);
  const liveGuardCallsBlocked = n(scanProgress?.scan_ai_guard_calls_blocked);
  const liveGuardLastReason = String(scanProgress?.scan_ai_guard_last_reason || '').trim();
  const aiBillingModeLabel = useMemo(() => {
    const provider = String(configData?.AI_PROVIDER || '').trim().toLowerCase();
    if (!provider) return 'Unknown billing mode';
    if (!['openai', 'openai-api', 'openai-codex'].includes(provider)) return `Provider billing: ${provider}`;
    const authMode = String(configData?.OPENAI_AUTH_MODE || '').trim().toLowerCase();
    const codexConnected = Boolean(configData?.OPENAI_CODEX_OAUTH_CONNECTED);
    if (provider === 'openai-codex') {
      if (codexConnected) return 'OpenAI Codex OAuth connected (billing may be account-plan dependent)';
      return 'OpenAI Codex selected but OAuth is not connected';
    }
    if (provider === 'openai-api') {
      return 'OpenAI API metered (API key mode)';
    }
    if (authMode === 'oauth_api_key') return 'OpenAI API metered (OAuth login connected)';
    if (authMode === 'oauth_connected_no_api_key') return 'OAuth connected, but no API key available';
    if (authMode === 'api_key') return 'OpenAI API metered';
    return 'OpenAI not configured';
  }, [configData?.AI_PROVIDER, configData?.OPENAI_AUTH_MODE, configData?.OPENAI_CODEX_OAUTH_CONNECTED]);

  const throughputAlbumsPerMin =
    current.durationSeconds > 0 ? (current.albumsScanned / current.durationSeconds) * 60 : 0;
  const averageDuration = current.scans > 0 ? current.durationSeconds / current.scans : 0;

  const audioCacheTotal = current.audioCacheHits + current.audioCacheMisses;
  const mbCacheTotal = current.mbCacheHits + current.mbCacheMisses;
  const audioCacheHitRate = audioCacheTotal > 0 ? (current.audioCacheHits / audioCacheTotal) * 100 : 0;
  const mbCacheHitRate = mbCacheTotal > 0 ? (current.mbCacheHits / mbCacheTotal) * 100 : 0;

  const scansChrono = useMemo(
    () => [...selectedScans].sort((a, b) => a.startTime - b.startTime),
    [selectedScans],
  );

  const scanTrendData = useMemo(() => {
    const pointRadius = scansChrono.length <= 1 ? 4 : 2;
    return {
      labels: scansChrono.map((scan) => format(new Date(scan.startTime * 1000), 'MM-dd HH:mm')),
      datasets: [
        {
          label: 'Albums scanned',
          data: scansChrono.map((scan) => scan.albumsScanned),
          borderColor: '#3b82f6',
          backgroundColor: 'rgba(59,130,246,0.2)',
          tension: 0.3,
          fill: true,
          borderWidth: 2,
          pointRadius,
          pointHoverRadius: Math.max(4, pointRadius + 1),
        },
        {
          label: 'Duplicate groups',
          data: scansChrono.map((scan) => scan.duplicatesFound),
          borderColor: '#f59e0b',
          backgroundColor: 'rgba(245,158,11,0.2)',
          tension: 0.3,
          fill: true,
          borderWidth: 2,
          pointRadius,
          pointHoverRadius: Math.max(4, pointRadius + 1),
        },
      ],
    };
  }, [scansChrono]);

  const metadataProvidersData = useMemo(() => {
    const mbProviderMatches = Math.max(
      0,
      current.albumsWithMbId - current.discogsMatches - current.lastfmMatches - current.bandcampMatches,
    );
    return {
      labels: ['MusicBrainz', 'Discogs', 'Last.fm', 'Bandcamp'],
      datasets: [
        {
          data: [
            mbProviderMatches,
            current.discogsMatches,
            current.lastfmMatches,
            current.bandcampMatches,
          ],
          backgroundColor: [
            'rgba(59,130,246,0.85)',
            'rgba(34,197,94,0.85)',
            'rgba(249,115,22,0.85)',
            'rgba(168,85,247,0.85)',
          ],
          borderColor: ['#2563eb', '#16a34a', '#ea580c', '#9333ea'],
          borderWidth: 1,
        },
      ],
    };
  }, [current.albumsWithMbId, current.discogsMatches, current.lastfmMatches, current.bandcampMatches]);

  const metadataProvidersBarData = useMemo(() => {
    const mbProviderMatches = Math.max(
      0,
      current.albumsWithMbId - current.discogsMatches - current.lastfmMatches - current.bandcampMatches,
    );
    return {
      labels: ['MusicBrainz', 'Discogs', 'Last.fm', 'Bandcamp'],
      datasets: [
        {
          label: 'Matched albums',
          data: [
            mbProviderMatches,
            current.discogsMatches,
            current.lastfmMatches,
            current.bandcampMatches,
          ],
          backgroundColor: [
            'rgba(59,130,246,0.85)',
            'rgba(34,197,94,0.85)',
            'rgba(249,115,22,0.85)',
            'rgba(168,85,247,0.85)',
          ],
          borderColor: ['#2563eb', '#16a34a', '#ea580c', '#9333ea'],
          borderWidth: 1,
        },
      ],
    };
  }, [current.albumsWithMbId, current.discogsMatches, current.lastfmMatches, current.bandcampMatches]);

  const aiCallsBreakdownData = useMemo(() => {
    return {
      labels: ['Provider identity', 'MB verify', 'Web MBID', 'Vision'],
      datasets: [
        {
          data: [
            current.aiCallsProviderIdentity,
            current.aiCallsMbVerify,
            current.aiCallsWebMbid,
            current.aiCallsVision,
          ],
          backgroundColor: [
            'rgba(59,130,246,0.82)',
            'rgba(34,197,94,0.82)',
            'rgba(249,115,22,0.82)',
            'rgba(168,85,247,0.82)',
          ],
          borderColor: ['#2563eb', '#16a34a', '#ea580c', '#9333ea'],
          borderWidth: 1,
        },
      ],
    };
  }, [
    current.aiCallsMbVerify,
    current.aiCallsProviderIdentity,
    current.aiCallsVision,
    current.aiCallsWebMbid,
  ]);

  const aiCallsTrendData = useMemo(() => {
    return {
      labels: scansChrono.map((scan) => format(new Date(scan.startTime * 1000), 'MM-dd HH:mm')),
      datasets: [
        {
          label: 'Total AI calls',
          data: scansChrono.map((scan) => scan.aiCallsTotal),
          borderColor: '#2563eb',
          backgroundColor: 'rgba(37,99,235,0.18)',
          tension: 0.25,
          fill: true,
          borderWidth: 2,
        },
        {
          label: 'AI errors',
          data: scansChrono.map((scan) => scan.aiErrorsTotal),
          borderColor: '#dc2626',
          backgroundColor: 'rgba(220,38,38,0.18)',
          tension: 0.25,
          fill: true,
          borderWidth: 2,
        },
      ],
    };
  }, [scansChrono]);

  const qualityCoverageData = useMemo(() => {
    return {
      labels: ['Tags', 'Cover', 'Artist image', 'Fully complete'],
      datasets: [
        {
          label: 'Coverage (%)',
          data: [
            current.albumsScanned > 0 ? (current.withTags / current.albumsScanned) * 100 : 0,
            current.albumsScanned > 0 ? (current.withCover / current.albumsScanned) * 100 : 0,
            current.albumsScanned > 0 ? (current.withArtistImage / current.albumsScanned) * 100 : 0,
            current.albumsScanned > 0 ? (current.fullyComplete / current.albumsScanned) * 100 : 0,
          ],
          backgroundColor: [
            'rgba(59,130,246,0.8)',
            'rgba(34,197,94,0.8)',
            'rgba(14,165,233,0.8)',
            'rgba(16,185,129,0.8)',
          ],
          borderColor: ['#2563eb', '#16a34a', '#0284c7', '#059669'],
          borderWidth: 1,
        },
      ],
    };
  }, [current.albumsScanned, current.withCover, current.withTags, current.withArtistImage, current.fullyComplete]);

  const qualityIssuePieData = useMemo(() => {
    return {
      labels: ['Missing tags', 'Missing cover', 'Missing artist image', 'Broken albums'],
      datasets: [
        {
          data: [current.withoutTags, current.withoutCover, current.withoutArtistImage, current.brokenAlbums],
          backgroundColor: [
            'rgba(59,130,246,0.8)',
            'rgba(34,197,94,0.8)',
            'rgba(14,165,233,0.8)',
            'rgba(239,68,68,0.8)',
          ],
          borderColor: ['#2563eb', '#16a34a', '#0284c7', '#dc2626'],
          borderWidth: 1,
        },
      ],
    };
  }, [current.withoutTags, current.withoutCover, current.withoutArtistImage, current.brokenAlbums]);

  const operationsTrendData = useMemo(() => {
    return {
      labels: scansChrono.map((scan) => format(new Date(scan.startTime * 1000), 'MM-dd HH:mm')),
      datasets: [
        {
          label: 'Duration (s)',
          data: scansChrono.map((scan) => scan.durationSeconds),
          borderColor: '#ef4444',
          backgroundColor: 'rgba(239,68,68,0.2)',
          yAxisID: 'y',
          tension: 0.25,
        },
        {
          label: 'Throughput (albums/min)',
          data: scansChrono.map((scan) =>
            scan.durationSeconds > 0 ? (scan.albumsScanned / scan.durationSeconds) * 60 : 0,
          ),
          borderColor: '#22c55e',
          backgroundColor: 'rgba(34,197,94,0.2)',
          yAxisID: 'y1',
          tension: 0.25,
        },
      ],
    };
  }, [scansChrono]);

  const benchmarkTrendData = useMemo(() => {
    const rows = [...benchmarkReports]
      .filter((row) => Number.isFinite(Number(row.generated_at)))
      .sort((a, b) => Number(a.generated_at || 0) - Number(b.generated_at || 0));
    const pointRadius = rows.length <= 1 ? 4 : 2;
    return {
      labels: rows.map((row) => format(new Date(Number(row.generated_at || 0) * 1000), 'MM-dd HH:mm')),
      datasets: [
        {
          label: 'Benchmark score',
          data: rows.map((row) => Number(row.score || 0)),
          borderColor: '#22c55e',
          backgroundColor: 'rgba(34,197,94,0.2)',
          tension: 0.25,
          fill: true,
          borderWidth: 2,
          pointRadius,
          pointHoverRadius: Math.max(4, pointRadius + 1),
        },
      ],
    };
  }, [benchmarkReports]);

  const cacheMixData = useMemo(() => {
    return {
      labels: ['Audio cache', 'MusicBrainz cache'],
      datasets: [
        {
          label: 'Hits',
          data: [current.audioCacheHits, current.mbCacheHits],
          backgroundColor: 'rgba(34,197,94,0.75)',
          borderColor: '#16a34a',
          borderWidth: 1,
        },
        {
          label: 'Misses',
          data: [current.audioCacheMisses, current.mbCacheMisses],
          backgroundColor: 'rgba(239,68,68,0.75)',
          borderColor: '#dc2626',
          borderWidth: 1,
        },
      ],
    };
  }, [current.audioCacheHits, current.audioCacheMisses, current.mbCacheHits, current.mbCacheMisses]);

  const redisUsedBytes = n(cacheControl?.redis?.used_memory_bytes);
  const redisMaxBytes = n(cacheControl?.redis?.maxmemory_bytes);
  const redisHeadroomBytes = Math.max(0, redisMaxBytes - redisUsedBytes);
  const redisHitRateLive = cacheControl?.redis?.keyspace_hit_rate_pct ?? null;
  const processRssBytes = n(cacheControl?.runtime?.process_rss_bytes);
  const containerMemoryUsed = n(cacheControl?.runtime?.container_memory?.current_bytes);
  const containerMemoryLimit = n(cacheControl?.runtime?.container_memory?.limit_bytes);
  const containerMemoryUsedPct = cacheControl?.runtime?.container_memory?.used_pct ?? null;

  const sqliteCacheDbBytes = n(cacheControl?.sqlite_cache_db?.db_bytes)
    + n(cacheControl?.sqlite_cache_db?.wal_bytes)
    + n(cacheControl?.sqlite_cache_db?.shm_bytes);
  const sqliteStateDbBytes = n(cacheControl?.sqlite_state_db?.db_bytes)
    + n(cacheControl?.sqlite_state_db?.wal_bytes)
    + n(cacheControl?.sqlite_state_db?.shm_bytes);
  const mediaCacheBytes = n(cacheControl?.media_cache?.total?.bytes_total);
  const postgresDbBytes = n(cacheControl?.postgres?.db_size_bytes);
  const hasTelemetry = Boolean(cacheControl && Number(cacheControl.generated_at || 0) > 0);

  const redisStatus = useMemo(() => {
    if (!hasTelemetry) return { label: 'unknown', variant: 'outline' as const, reason: '' };
    const mode = String(cacheControl?.redis?.mode || '').trim().toLowerCase();
    const reason = String(cacheControl?.redis?.reason || '').trim();
    const available = cacheControl?.redis?.available === true;
    if (available || mode === 'redis') return { label: 'redis', variant: 'secondary' as const, reason };
    if (mode === 'local') return { label: 'local-fallback', variant: 'secondary' as const, reason };
    if (mode === 'none' || mode === 'disabled') return { label: 'disabled', variant: 'outline' as const, reason };
    return { label: 'unavailable', variant: 'outline' as const, reason };
  }, [cacheControl?.redis?.available, cacheControl?.redis?.mode, cacheControl?.redis?.reason, hasTelemetry]);

  const postgresStatus = useMemo(() => {
    if (!hasTelemetry) return { label: 'unknown', variant: 'outline' as const, reason: '' };
    const mode = String(cacheControl?.postgres?.mode || '').trim().toLowerCase();
    const reason = String(cacheControl?.postgres?.reason || '').trim();
    const available = cacheControl?.postgres?.available === true;
    if (available || mode === 'postgres') return { label: 'postgres', variant: 'secondary' as const, reason };
    if (mode === 'disabled') return { label: 'disabled', variant: 'outline' as const, reason };
    if (mode === 'none') return { label: 'unavailable', variant: 'outline' as const, reason };
    return { label: 'unknown', variant: 'outline' as const, reason };
  }, [cacheControl?.postgres?.available, cacheControl?.postgres?.mode, cacheControl?.postgres?.reason, hasTelemetry]);

  const watcherStatus = useMemo(() => {
    if (!hasTelemetry) return { label: 'unknown', variant: 'outline' as const, reason: '' };
    const running = watcherRuntimeStatus?.running ?? (cacheControl?.files_watcher?.running === true);
    const enabled = watcherRuntimeStatus?.enabled ?? (cacheControl?.files_watcher?.enabled !== false);
    const available = watcherRuntimeStatus?.available ?? (cacheControl?.files_watcher?.available !== false);
    const reason = String(watcherRuntimeStatus?.reason || cacheControl?.files_watcher?.reason || '').trim();
    if (running) return { label: 'running', variant: 'secondary' as const, reason };
    if (!enabled) return { label: 'disabled', variant: 'outline' as const, reason: reason || 'disabled_by_setting' };
    if (!available) return { label: 'unavailable', variant: 'outline' as const, reason: reason || 'watchdog_unavailable' };
    return { label: 'stopped', variant: 'outline' as const, reason };
  }, [
    watcherRuntimeStatus?.available,
    watcherRuntimeStatus?.enabled,
    watcherRuntimeStatus?.reason,
    watcherRuntimeStatus?.running,
    cacheControl?.files_watcher?.available,
    cacheControl?.files_watcher?.enabled,
    cacheControl?.files_watcher?.reason,
    cacheControl?.files_watcher?.running,
    hasTelemetry,
  ]);

  const restartWatcher = async () => {
    setWatcherRestarting(true);
    try {
      await api.restartFilesWatcher();
      toast.success('Watcher restart requested');
      void Promise.all([refetchWatcherRuntimeStatus(), refetchCacheControl()]);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to restart watcher');
    } finally {
      setWatcherRestarting(false);
    }
  };

  const cacheStorageData = useMemo(() => {
    return {
      labels: ['Redis RAM', 'PostgreSQL', 'Media cache', 'SQLite cache.db', 'SQLite state.db'],
      datasets: [
        {
          data: [
            redisUsedBytes,
            postgresDbBytes,
            mediaCacheBytes,
            sqliteCacheDbBytes,
            sqliteStateDbBytes,
          ],
          backgroundColor: [
            'rgba(249,115,22,0.82)',
            'rgba(59,130,246,0.82)',
            'rgba(34,197,94,0.82)',
            'rgba(168,85,247,0.82)',
            'rgba(14,165,233,0.82)',
          ],
          borderColor: ['#ea580c', '#2563eb', '#16a34a', '#9333ea', '#0284c7'],
          borderWidth: 1,
        },
      ],
    };
  }, [mediaCacheBytes, postgresDbBytes, redisUsedBytes, sqliteCacheDbBytes, sqliteStateDbBytes]);

  const cacheEntriesBarData = useMemo(() => {
    return {
      labels: ['Audio cache', 'MB RG cache', 'MB album lookup', 'Provider lookup', 'Files scan cache', 'Watcher queue', 'Redis PMDA keys'],
      datasets: [
        {
          label: 'Entries',
          data: [
            n(cacheControl?.sqlite_cache_db?.audio_cache_rows),
            n(cacheControl?.sqlite_cache_db?.musicbrainz_cache_rows),
            n(cacheControl?.sqlite_cache_db?.musicbrainz_album_lookup_rows),
            n(cacheControl?.sqlite_cache_db?.provider_album_lookup_rows),
            n(cacheControl?.sqlite_state_db?.files_album_scan_cache_rows),
            n(cacheControl?.sqlite_state_db?.files_pending_changes_rows),
            n(cacheControl?.redis?.pmda_prefix_keys),
          ],
          backgroundColor: [
            'rgba(34,197,94,0.78)',
            'rgba(59,130,246,0.78)',
            'rgba(14,165,233,0.78)',
            'rgba(244,114,182,0.78)',
            'rgba(168,85,247,0.78)',
            'rgba(249,115,22,0.78)',
            'rgba(239,68,68,0.78)',
          ],
          borderColor: ['#16a34a', '#2563eb', '#0284c7', '#db2777', '#9333ea', '#ea580c', '#dc2626'],
          borderWidth: 1,
        },
      ],
    };
  }, [
    cacheControl?.redis?.pmda_prefix_keys,
    cacheControl?.sqlite_cache_db?.audio_cache_rows,
    cacheControl?.sqlite_cache_db?.musicbrainz_album_lookup_rows,
    cacheControl?.sqlite_cache_db?.musicbrainz_cache_rows,
    cacheControl?.sqlite_cache_db?.provider_album_lookup_rows,
    cacheControl?.sqlite_state_db?.files_album_scan_cache_rows,
    cacheControl?.sqlite_state_db?.files_pending_changes_rows,
  ]);
  const sourcePathRows = useMemo(() => {
    const rows = Array.isArray(libraryStats?.source_paths) ? libraryStats.source_paths : [];
    return rows.filter((row) => Number(row.albums || 0) > 0);
  }, [libraryStats?.source_paths]);
  const sourcePathShareData = useMemo(() => {
    const top = sourcePathRows.slice(0, 8);
    const labels = top.map((row) => row.path || '(unknown)');
    const values = top.map((row) => Number(row.albums || 0));
    return {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: [
            'rgba(59,130,246,0.82)',
            'rgba(34,197,94,0.82)',
            'rgba(249,115,22,0.82)',
            'rgba(168,85,247,0.82)',
            'rgba(14,165,233,0.82)',
            'rgba(236,72,153,0.82)',
            'rgba(234,179,8,0.82)',
            'rgba(99,102,241,0.82)',
          ],
          borderWidth: 1,
        },
      ],
    };
  }, [sourcePathRows]);

  const redisMemoryData = useMemo(() => {
    const hasMax = redisMaxBytes > 0;
    return {
      labels: hasMax ? ['Used', 'Headroom'] : ['Used'],
      datasets: [
        {
          data: hasMax ? [redisUsedBytes, redisHeadroomBytes] : [redisUsedBytes],
          backgroundColor: hasMax
            ? ['rgba(249,115,22,0.84)', 'rgba(34,197,94,0.28)']
            : ['rgba(249,115,22,0.84)'],
          borderColor: hasMax ? ['#ea580c', '#16a34a'] : ['#ea580c'],
          borderWidth: 1,
        },
      ],
    };
  }, [redisHeadroomBytes, redisMaxBytes, redisUsedBytes]);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="pmda-page-shell pmda-page-stack">
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div>
            <h1 className="pmda-page-title">Statistics</h1>
            <p className="pmda-meta-text mt-1">Single source of truth from completed scans</p>
          </div>
          <StatisticsPageNav active="scan" />
        </div>

        <div className="flex flex-wrap items-center justify-between gap-2">
          <div />
          <Tabs value={period} onValueChange={(value) => setPeriod(value as PeriodKey)}>
            <TabsList>
              {PERIODS.map((p) => (
                <TabsTrigger key={p.key} value={p.key}>
                  {p.label}
                </TabsTrigger>
              ))}
            </TabsList>
          </Tabs>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline">{sourceLabel}</Badge>
          {isLiveRunActive && <Badge variant="secondary">Live scan in progress</Badge>}
          {baselineScans.length > 0 && <Badge variant="outline">delta baseline = previous {baselineScans.length} scan(s)</Badge>}
        </div>

        {isLiveRunActive && scanProgress && (
          <Card className="border-primary/30 bg-primary/5">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm flex items-center gap-2">
                <Loader2 className="w-4 h-4 animate-spin text-primary" />
                Live scan metrics
              </CardTitle>
              <CardDescription>
                Live counters update every 2s during scan and post-processing.
              </CardDescription>
            </CardHeader>
            <CardContent className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3 text-sm">
              {(isPreScanPhase || isRunScopePreparing) ? (
                <>
                  <div className="rounded-lg border border-border bg-background/70 p-3">
                    <p className="text-muted-foreground">{isRunScopePreparing ? 'Preparing run scope' : 'Pre-scan progress'}</p>
                    <p className="text-xl font-semibold tabular-nums">
                      {isRunScopePreparing
                        ? runScopePrepTotal > 0
                          ? `${runScopePrepDone.toLocaleString()} / ${runScopePrepTotal.toLocaleString()}`
                          : `${runScopePrepDone.toLocaleString()} / ?`
                        : preScanProgressTotal > 0
                          ? `${preScanProgressDone.toLocaleString()} / ${preScanProgressTotal.toLocaleString()}`
                          : preScanRootsTotal > 0
                            ? `${preScanRootsDone.toLocaleString()} / ${preScanRootsTotal.toLocaleString()}`
                            : `${preScanProgressDone.toLocaleString()} / ?`}
                    </p>
                    <p className="text-xs text-muted-foreground tabular-nums">
                      {isRunScopePreparing
                        ? (runScopePrepStage || 'resume')
                        : (preScanProgressTotal > 0 ? 'albums' : 'roots')}
                    </p>
                  </div>
                  <div className="rounded-lg border border-border bg-background/70 p-3">
                    <p className="text-muted-foreground">Filesystem walk</p>
                    <p className="text-xl font-semibold tabular-nums">
                      {preScanEntriesScanned.toLocaleString()}
                    </p>
                    <p className="text-xs text-muted-foreground tabular-nums">
                      {preScanFilesFound.toLocaleString()} audio files
                    </p>
                  </div>
                  <div className="rounded-lg border border-border bg-background/70 p-3">
                    <p className="text-muted-foreground">Discovery</p>
                    <p className="text-xl font-semibold tabular-nums">
                      {preScanArtistsFound.toLocaleString()} artists
                    </p>
                    <p className="text-xs text-muted-foreground tabular-nums">
                      {preScanAlbumsFound.toLocaleString()} albums
                    </p>
                  </div>
                  <div className="rounded-lg border border-border bg-background/70 p-3">
                    <p className="text-muted-foreground">Run scope</p>
                    <p className="text-xl font-semibold tabular-nums">
                      {(isRunScopePreparing ? runScopeIncludedArtists : n(scanProgress.detected_artists_total)).toLocaleString()} artists
                    </p>
                    <p className="text-xs text-muted-foreground tabular-nums">
                      {(isRunScopePreparing ? runScopeIncludedAlbums : n(scanProgress.detected_albums_total)).toLocaleString()} albums
                    </p>
                    {runScopePending && (
                      <p className="text-[11px] text-muted-foreground">pending until plan ready</p>
                    )}
                  </div>
                  <div className="rounded-lg border border-border bg-background/70 p-3">
                    <p className="text-muted-foreground">Resume skipped</p>
                    <p className="text-xl font-semibold tabular-nums">
                      {n(scanProgress.resume_skipped_artists).toLocaleString()} artists
                    </p>
                    <p className="text-xs text-muted-foreground tabular-nums">
                      {n(scanProgress.resume_skipped_albums).toLocaleString()} albums
                    </p>
                  </div>
                </>
              ) : (
                <>
                  <div className="rounded-lg border border-border bg-background/70 p-3">
                    <p className="text-muted-foreground">Artists progress</p>
                    <p className="text-xl font-semibold tabular-nums">
                      {n(scanProgress.artists_processed).toLocaleString()} / {n(scanProgress.artists_total).toLocaleString()}
                    </p>
                  </div>
                  <div className="rounded-lg border border-border bg-background/70 p-3">
                    <p className="text-muted-foreground">Run scope</p>
                    <p className="text-xl font-semibold tabular-nums">
                      {n(scanProgress.artists_total).toLocaleString()} artists
                    </p>
                    <p className="text-xs text-muted-foreground tabular-nums">
                      {n(scanProgress.total_albums).toLocaleString()} albums
                    </p>
                  </div>
                  <div className="rounded-lg border border-border bg-background/70 p-3">
                    <p className="text-muted-foreground">Detected source</p>
                    <p className="text-xl font-semibold tabular-nums">
                      {n(scanProgress.detected_artists_total).toLocaleString()} artists
                    </p>
                    <p className="text-xs text-muted-foreground tabular-nums">
                      {n(scanProgress.detected_albums_total).toLocaleString()} albums
                    </p>
                  </div>
                  <div className="rounded-lg border border-border bg-background/70 p-3">
                    <p className="text-muted-foreground">Resume skipped</p>
                    <p className="text-xl font-semibold tabular-nums">
                      {n(scanProgress.resume_skipped_artists).toLocaleString()} artists
                    </p>
                    <p className="text-xs text-muted-foreground tabular-nums">
                      {n(scanProgress.resume_skipped_albums).toLocaleString()} albums
                    </p>
                  </div>
                  <div className="rounded-lg border border-border bg-background/70 p-3">
                    <p className="text-muted-foreground">Post-processing</p>
                    <p className="text-xl font-semibold tabular-nums">
                      {n(scanProgress.post_processing_done).toLocaleString()} / {n(scanProgress.post_processing_total).toLocaleString()}
                    </p>
                  </div>
                </>
              )}
            </CardContent>
            <CardContent className="pt-0">
              <div className="rounded-lg border border-border bg-background/70 p-3 text-sm">
                <p className="text-muted-foreground">Track reconciliation</p>
                <p className="text-xl font-semibold tabular-nums">
                  {liveTracksLibraryKept.toLocaleString()} / {liveTracksDetected.toLocaleString()}
                </p>
                <p className="text-xs text-muted-foreground tabular-nums">
                  library kept / detected ({liveTrackScopeLabel})
                </p>
                <p className="text-xs text-muted-foreground tabular-nums">
                  moved dupes {liveTracksMovedDupes.toLocaleString()} · incomplete {liveTracksMovedIncomplete.toLocaleString()} · unaccounted {liveTracksUnaccounted.toLocaleString()}
                </p>
              </div>
            </CardContent>
            {isPreScanPhase && preScanStage === 'filesystem' && (
              <CardContent className="pt-0">
                <p className="text-xs text-muted-foreground">
                  Album total becomes exact during album-candidate pass.
                </p>
              </CardContent>
            )}
            {(liveSkippedArtists > 0 || liveSkippedAlbums > 0) && (
              <CardContent className="pt-0">
                <p className="text-xs text-muted-foreground">
                  Resume mode active: completed/unchanged entities are skipped and excluded from run scope totals.
                </p>
              </CardContent>
            )}
          </Card>
        )}

        {current.scans === 0 && !isLiveRunActive ? (
          <Card>
            <CardHeader>
              <CardTitle className="text-base flex items-center gap-2">
                <AlertCircle className="w-4 h-4 text-muted-foreground" />
                No data
              </CardTitle>
              <CardDescription>Run at least one completed scan to populate statistics.</CardDescription>
            </CardHeader>
          </Card>
        ) : (
          <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as StatsTab)}>
            <div className="space-y-2">
              <p className="text-sm font-medium">Scan sections</p>
              <TabsList className="grid h-auto w-full grid-cols-1 gap-2 rounded-xl border border-border bg-muted/30 p-2 shadow-sm sm:grid-cols-2 xl:grid-cols-3">
                <TabsTrigger
                  value="overview"
                  className="h-11 justify-start rounded-lg border border-border/60 bg-card/70 px-3 text-left text-sm font-semibold text-foreground transition-colors hover:bg-accent/70 data-[state=active]:border-cyan-400/60 data-[state=active]:bg-cyan-500/20 data-[state=active]:text-cyan-100 data-[state=active]:shadow-[0_0_0_1px_rgba(34,211,238,0.22)]"
                >
                  <span className="flex items-center gap-2">
                    <BarChart3 className="w-4 h-4" />
                    Overview
                  </span>
                </TabsTrigger>
                <TabsTrigger
                  value="metadata"
                  className="h-11 justify-start rounded-lg border border-border/60 bg-card/70 px-3 text-left text-sm font-semibold text-foreground transition-colors hover:bg-accent/70 data-[state=active]:border-indigo-400/60 data-[state=active]:bg-indigo-500/20 data-[state=active]:text-indigo-100 data-[state=active]:shadow-[0_0_0_1px_rgba(129,140,248,0.22)]"
                >
                  <span className="flex items-center gap-2">
                    <Database className="w-4 h-4" />
                    Metadata
                  </span>
                </TabsTrigger>
                <TabsTrigger
                  value="ai"
                  className="h-11 justify-start rounded-lg border border-border/60 bg-card/70 px-3 text-left text-sm font-semibold text-foreground transition-colors hover:bg-accent/70 data-[state=active]:border-violet-400/60 data-[state=active]:bg-violet-500/20 data-[state=active]:text-violet-100 data-[state=active]:shadow-[0_0_0_1px_rgba(167,139,250,0.22)]"
                >
                  <span className="flex items-center gap-2">
                    <Sparkles className="w-4 h-4" />
                    AI Cost
                  </span>
                </TabsTrigger>
                <TabsTrigger
                  value="quality"
                  className="h-11 justify-start rounded-lg border border-border/60 bg-card/70 px-3 text-left text-sm font-semibold text-foreground transition-colors hover:bg-accent/70 data-[state=active]:border-emerald-400/60 data-[state=active]:bg-emerald-500/20 data-[state=active]:text-emerald-100 data-[state=active]:shadow-[0_0_0_1px_rgba(52,211,153,0.22)]"
                >
                  <span className="flex items-center gap-2">
                    <Sparkles className="w-4 h-4" />
                    Quality
                  </span>
                </TabsTrigger>
                <TabsTrigger
                  value="operations"
                  className="h-11 justify-start rounded-lg border border-border/60 bg-card/70 px-3 text-left text-sm font-semibold text-foreground transition-colors hover:bg-accent/70 data-[state=active]:border-amber-400/60 data-[state=active]:bg-amber-500/20 data-[state=active]:text-amber-100 data-[state=active]:shadow-[0_0_0_1px_rgba(251,191,36,0.22)]"
                >
                  <span className="flex items-center gap-2">
                    <Gauge className="w-4 h-4" />
                    Operations
                  </span>
                </TabsTrigger>
                <TabsTrigger
                  value="duplicates"
                  className="h-11 justify-start rounded-lg border border-border/60 bg-card/70 px-3 text-left text-sm font-semibold text-foreground transition-colors hover:bg-accent/70 data-[state=active]:border-rose-400/60 data-[state=active]:bg-rose-500/20 data-[state=active]:text-rose-100 data-[state=active]:shadow-[0_0_0_1px_rgba(251,113,133,0.22)]"
                >
                  <span className="flex items-center gap-2">
                    <Layers className="w-4 h-4" />
                    Duplicates
                  </span>
                </TabsTrigger>
                <TabsTrigger
                  value="incompletes"
                  className="h-11 justify-start rounded-lg border border-border/60 bg-card/70 px-3 text-left text-sm font-semibold text-foreground transition-colors hover:bg-accent/70 data-[state=active]:border-orange-400/60 data-[state=active]:bg-orange-500/20 data-[state=active]:text-orange-100 data-[state=active]:shadow-[0_0_0_1px_rgba(251,146,60,0.22)]"
                >
                  <span className="flex items-center gap-2">
                    <AlertCircle className="w-4 h-4" />
                    Incompletes
                  </span>
                </TabsTrigger>
                {isAdmin ? (
                  <TabsTrigger
                    value="pipeline"
                    className="h-11 justify-start rounded-lg border border-border/60 bg-card/70 px-3 text-left text-sm font-semibold text-foreground transition-colors hover:bg-accent/70 data-[state=active]:border-sky-400/60 data-[state=active]:bg-sky-500/20 data-[state=active]:text-sky-100 data-[state=active]:shadow-[0_0_0_1px_rgba(56,189,248,0.22)]"
                  >
                    <span className="flex items-center gap-2">
                      <Server className="w-4 h-4" />
                      Pipeline
                    </span>
                  </TabsTrigger>
                ) : null}
                <TabsTrigger
                  value="benchmark"
                  className="h-11 justify-start rounded-lg border border-border/60 bg-card/70 px-3 text-left text-sm font-semibold text-foreground transition-colors hover:bg-accent/70 data-[state=active]:border-pink-400/60 data-[state=active]:bg-pink-500/20 data-[state=active]:text-pink-100 data-[state=active]:shadow-[0_0_0_1px_rgba(244,114,182,0.22)]"
                >
                  <span className="flex items-center gap-2">
                    <Gauge className="w-4 h-4" />
                    Benchmark
                  </span>
                </TabsTrigger>
              </TabsList>
            </div>

            <TabsContent value="overview" className="space-y-4">
              <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-6">
                <StatCard
                  title="Scans"
                  icon={<Layers className="w-4 h-4 text-primary" />}
                  value={current.scans.toLocaleString()}
                  delta={<DeltaPill current={current.scans} previous={previous.scans} />}
                />
                <StatCard
                  title="Albums Scanned"
                  icon={<BarChart3 className="w-4 h-4 text-info" />}
                  value={current.albumsScanned.toLocaleString()}
                  delta={<DeltaPill current={current.albumsScanned} previous={previous.albumsScanned} />}
                />
                <StatCard
                  title="Duplicates"
                  icon={<AlertCircle className="w-4 h-4 text-warning" />}
                  value={current.duplicatesFound.toLocaleString()}
                  description="Duplicate groups detected"
                  delta={<DeltaPill current={current.duplicatesFound} previous={previous.duplicatesFound} positiveIsGood={false} />}
                />
                <StatCard
                  title="Space Saved"
                  icon={<HardDrive className="w-4 h-4 text-success" />}
                  value={formatMb(current.spaceSavedMb)}
                  delta={<DeltaPill current={current.spaceSavedMb} previous={previous.spaceSavedMb} suffix=" MB" />}
                />
                <StatCard
                  title="Matched During Scan"
                  icon={<Database className="w-4 h-4 text-primary" />}
                  value={`${current.matchedAlbums.toLocaleString()} / ${current.albumsScanned.toLocaleString()}`}
                  description={`${formatPercent(current.matchedAlbums, current.albumsScanned)} strict 100% matches (artist + album + full tracklist exact)`}
                  delta={<DeltaPill current={current.matchedAlbums} previous={previous.matchedAlbums} />}
                />
                <StatCard
                  title="Fully Complete"
                  icon={<Sparkles className="w-4 h-4 text-emerald-500" />}
                  value={`${current.fullyComplete.toLocaleString()} / ${current.albumsScanned.toLocaleString()}`}
                  description={`${formatPercent(current.fullyComplete, current.albumsScanned)} tags + cover + artist image`}
                  delta={<DeltaPill current={current.fullyComplete} previous={previous.fullyComplete} />}
                />
                {hasLatestTrackReconciliation && (
                  <StatCard
                    title="Track Reconciliation"
                    icon={<Database className="w-4 h-4 text-primary" />}
                    value={`${latestSummaryTracksLibraryKept.toLocaleString()} / ${latestSummaryTracksDetected.toLocaleString()}`}
                    description={`kept/detected · moved dupes ${latestSummaryTracksMovedDupes.toLocaleString()} · incomplete ${latestSummaryTracksMovedIncomplete.toLocaleString()} · unaccounted ${latestSummaryTracksUnaccounted.toLocaleString()}`}
                  />
                )}
              </div>
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Scan trend</CardTitle>
                  <CardDescription>Albums scanned and duplicates over selected scans.</CardDescription>
                </CardHeader>
                <CardContent>
                  {scansChrono.length < 2 && (
                    <div className="mb-3 flex items-center gap-2 text-xs text-muted-foreground">
                      <AlertCircle className="w-3.5 h-3.5" />
                      Run at least 2 completed scans to see a meaningful trend.
                    </div>
                  )}
                  <div className="h-[260px]">
                    <Line data={scanTrendData} options={chartOptions} />
                  </div>
                </CardContent>
              </Card>
              <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
                <Card className="xl:col-span-1">
                  <CardHeader>
                    <CardTitle className="text-sm">Albums by Source Path</CardTitle>
                    <CardDescription>Share of albums per configured source root.</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-[260px] flex items-center justify-center">
                      <Pie data={sourcePathShareData} options={chartOptions} />
                    </div>
                  </CardContent>
                </Card>
                <Card className="xl:col-span-2">
                  <CardHeader>
                    <CardTitle className="text-sm">Source Path Breakdown</CardTitle>
                    <CardDescription>Albums, artists, labels and tracks per source path.</CardDescription>
                  </CardHeader>
                  <CardContent>
                    {sourcePathRows.length === 0 ? (
                      <p className="text-sm text-muted-foreground">No source path stats available yet.</p>
                    ) : (
                      <div className="max-h-[260px] overflow-auto rounded-md border">
                        <table className="w-full text-sm">
                          <thead className="bg-muted/50 sticky top-0">
                            <tr>
                              <th className="text-left px-3 py-2 font-medium">Path</th>
                              <th className="text-right px-3 py-2 font-medium">Albums</th>
                              <th className="text-right px-3 py-2 font-medium">Artists</th>
                              <th className="text-right px-3 py-2 font-medium">Labels</th>
                              <th className="text-right px-3 py-2 font-medium">Tracks</th>
                              <th className="text-right px-3 py-2 font-medium">Share</th>
                            </tr>
                          </thead>
                          <tbody>
                            {sourcePathRows.map((row) => (
                              <tr key={`src-${row.path}`} className="border-t border-border">
                                <td className="px-3 py-2 truncate max-w-[420px]" title={row.path}>{row.path}</td>
                                <td className="px-3 py-2 text-right tabular-nums">{Number(row.albums || 0).toLocaleString()}</td>
                                <td className="px-3 py-2 text-right tabular-nums">{Number(row.artists || 0).toLocaleString()}</td>
                                <td className="px-3 py-2 text-right tabular-nums">{Number(row.labels || 0).toLocaleString()}</td>
                                <td className="px-3 py-2 text-right tabular-nums">{Number(row.tracks || 0).toLocaleString()}</td>
                                <td className="px-3 py-2 text-right tabular-nums">{Number(row.albums_pct || 0).toFixed(2)}%</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            <TabsContent value="metadata" className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <StatCard
                  title="Strict Coverage"
                  icon={<Database className="w-4 h-4 text-primary" />}
                  value={`${current.albumsWithMbId.toLocaleString()} / ${current.albumsScanned.toLocaleString()}`}
                  description={formatPercent(current.albumsWithMbId, current.albumsScanned)}
                  delta={<DeltaPill current={current.albumsWithMbId} previous={previous.albumsWithMbId} />}
                />
                <StatCard
                  title="AI-Assisted MB"
                  icon={<Sparkles className="w-4 h-4 text-info" />}
                  value={current.mbVerifiedByAi.toLocaleString()}
                  description="MB matches confirmed by AI"
                  delta={<DeltaPill current={current.mbVerifiedByAi} previous={previous.mbVerifiedByAi} />}
                />
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-xs font-medium flex items-center gap-1.5 text-muted-foreground uppercase tracking-wider">
                      <Database className="w-4 h-4 text-secondary" />
                      Fallback Providers
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-2 text-sm">
                    <div className="flex items-center justify-between">
                      <span className="text-muted-foreground">Discogs</span>
                      <span className="font-medium tabular-nums">{current.discogsMatches.toLocaleString()}</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-muted-foreground">Last.fm</span>
                      <span className="font-medium tabular-nums">{current.lastfmMatches.toLocaleString()}</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-muted-foreground">Bandcamp</span>
                      <span className="font-medium tabular-nums">{current.bandcampMatches.toLocaleString()}</span>
                    </div>
                  </CardContent>
                </Card>
              </div>
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Metadata source share</CardTitle>
                  <CardDescription>Distribution of album matches by provider.</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-[260px] flex items-center justify-center">
                    <Doughnut data={metadataProvidersData} options={{ ...chartOptions, cutout: '60%' }} />
                  </div>
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Metadata provider counts</CardTitle>
                  <CardDescription>Absolute volume by provider (horizontal bars).</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-[260px]">
                    <Bar
                      data={metadataProvidersBarData}
                      options={{
                        ...chartOptions,
                        indexAxis: 'y' as const,
                        plugins: { ...chartOptions.plugins, legend: { display: false } },
                        scales: {
                          x: { beginAtZero: true },
                          y: { beginAtZero: true },
                        },
                      }}
                    />
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="ai" className="space-y-4">
              <div className="flex items-center justify-between gap-3 rounded-lg border border-border bg-card p-3">
                <div>
                  <p className="text-sm font-medium">Cost scope</p>
                  <p className="text-xs text-muted-foreground">
                    {aiCostScanId ? `Scan #${aiCostScanId}` : 'No completed scan selected'}
                  </p>
                  <p className="text-xs text-muted-foreground mt-1">{aiBillingModeLabel}</p>
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    size="sm"
                    variant={aiCostScope === 'lifecycle' ? 'default' : 'outline'}
                    onClick={() => setAiCostScope('lifecycle')}
                  >
                    Lifecycle
                  </Button>
                  <Button
                    size="sm"
                    variant={aiCostScope === 'scan' ? 'default' : 'outline'}
                    onClick={() => setAiCostScope('scan')}
                  >
                    Scan only
                  </Button>
                </div>
              </div>

              <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
                <StatCard
                  title="Total AI Calls"
                  icon={<Sparkles className="w-4 h-4 text-primary" />}
                  value={current.aiCallsTotal.toLocaleString()}
                  description={`${formatPercent(current.aiCallsTotal, Math.max(1, current.albumsScanned))} calls vs albums scanned`}
                  delta={<DeltaPill current={current.aiCallsTotal} previous={previous.aiCallsTotal} />}
                />
                <StatCard
                  title="Provider Identity"
                  icon={<Database className="w-4 h-4 text-info" />}
                  value={current.aiCallsProviderIdentity.toLocaleString()}
                  description="Discogs/Last.fm/Bandcamp arbitration"
                  delta={<DeltaPill current={current.aiCallsProviderIdentity} previous={previous.aiCallsProviderIdentity} />}
                />
                <StatCard
                  title="MB Verify + Web"
                  icon={<Server className="w-4 h-4 text-secondary" />}
                  value={(current.aiCallsMbVerify + current.aiCallsWebMbid).toLocaleString()}
                  description={`MB verify: ${current.aiCallsMbVerify.toLocaleString()} · Web MBID: ${current.aiCallsWebMbid.toLocaleString()}`}
                  delta={
                    <DeltaPill
                      current={current.aiCallsMbVerify + current.aiCallsWebMbid}
                      previous={previous.aiCallsMbVerify + previous.aiCallsWebMbid}
                    />
                  }
                />
                <StatCard
                  title="Vision Calls"
                  icon={<Image className="w-4 h-4 text-warning" />}
                  value={current.aiCallsVision.toLocaleString()}
                  description={`AI errors: ${current.aiErrorsTotal.toLocaleString()}`}
                  delta={<DeltaPill current={current.aiCallsVision} previous={previous.aiCallsVision} />}
                />
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-5 gap-4">
                <StatCard
                  title="Total USD"
                  icon={<Gauge className="w-4 h-4 text-primary" />}
                  value={formatUsd(latestAiTotals?.cost_usd ?? current.aiCostUsdTotal)}
                  description={`Scope: ${aiCostScope}`}
                />
                <StatCard
                  title="Total Tokens"
                  icon={<Server className="w-4 h-4 text-secondary" />}
                  value={(latestAiTotals?.total_tokens ?? current.aiTokensTotal).toLocaleString()}
                  description="Input + output + cache"
                />
                <StatCard
                  title="Unpriced Calls"
                  icon={<AlertCircle className="w-4 h-4 text-warning" />}
                  value={(latestAiTotals?.unpriced_calls ?? current.aiUnpricedCalls).toLocaleString()}
                  description="Provider usage or pricing missing"
                />
                <StatCard
                  title="Guardrail Blocks"
                  icon={<AlertCircle className="w-4 h-4 text-red-500" />}
                  value={liveGuardCallsBlocked.toLocaleString()}
                  description={liveGuardLastReason || `Allowed this run: ${liveGuardCallsUsed.toLocaleString()}`}
                />
                <StatCard
                  title="Lifecycle"
                  icon={<Clock className="w-4 h-4 text-info" />}
                  value={(latestAiCostSummary?.lifecycle_complete ?? false) ? 'Complete' : 'In progress'}
                  description={latestAiCostSummary?.last_updated_at ? `Updated ${format(new Date(latestAiCostSummary.last_updated_at * 1000), 'HH:mm:ss')}` : 'No rollup yet'}
                />
              </div>

              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">AI calls breakdown</CardTitle>
                  <CardDescription>How AI usage is distributed across PMDA features.</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-[260px] flex items-center justify-center">
                    <Doughnut data={aiCallsBreakdownData} options={{ ...chartOptions, cutout: '60%' }} />
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">AI activity trend</CardTitle>
                  <CardDescription>Calls and errors by scan.</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-[260px]">
                    <Line data={aiCallsTrendData} options={chartOptions} />
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">AI cost breakdown</CardTitle>
                  <CardDescription>Persisted USD + tokens by analysis type (includes web-search tool fees when priced).</CardDescription>
                </CardHeader>
                <CardContent>
                  {latestAiBreakdown.length <= 0 ? (
                    <p className="text-sm text-muted-foreground">No persisted AI cost rows for this scope yet.</p>
                  ) : (
                    <div className="border rounded-lg overflow-hidden">
                      <div className="grid grid-cols-12 gap-2 px-3 py-2 text-[11px] uppercase tracking-wide text-muted-foreground bg-muted/30">
                        <div className="col-span-5">Analysis type</div>
                        <div className="col-span-2 text-right">Calls</div>
                        <div className="col-span-3 text-right">Tokens</div>
                        <div className="col-span-2 text-right">USD</div>
                      </div>
                      <div className="divide-y">
                        {latestAiBreakdown.map((row, idx) => (
                          <div key={`${row.analysis_type || 'unknown'}-${idx}`} className="grid grid-cols-12 gap-2 px-3 py-2 text-xs">
                            <div className="col-span-5 truncate">{row.analysis_type || 'unknown'}</div>
                            <div className="col-span-2 text-right tabular-nums">{(row.calls ?? 0).toLocaleString()}</div>
                            <div className="col-span-3 text-right tabular-nums">{(row.total_tokens ?? 0).toLocaleString()}</div>
                            <div className="col-span-2 text-right tabular-nums">{formatUsd(row.cost_usd ?? 0)}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">AI cost by provider</CardTitle>
                  <CardDescription>Calls/tokens/USD grouped by effective provider for this scan scope.</CardDescription>
                </CardHeader>
                <CardContent>
                  {latestAiProviderBreakdown.length <= 0 ? (
                    <p className="text-sm text-muted-foreground">No provider-level AI usage rows for this scope yet.</p>
                  ) : (
                    <div className="border rounded-lg overflow-hidden">
                      <div className="grid grid-cols-12 gap-2 px-3 py-2 text-[11px] uppercase tracking-wide text-muted-foreground bg-muted/30">
                        <div className="col-span-4">Provider</div>
                        <div className="col-span-2 text-right">Calls</div>
                        <div className="col-span-3 text-right">Tokens</div>
                        <div className="col-span-3 text-right">USD</div>
                      </div>
                      <div className="divide-y">
                        {latestAiProviderBreakdown.map((row, idx) => (
                          <div key={`ai-provider-${row.provider || 'unknown'}-${idx}`} className="grid grid-cols-12 gap-2 px-3 py-2 text-xs">
                            <div className="col-span-4 truncate"><ProviderBadge provider={row.provider || 'unknown'} className="h-5 px-2 py-0 text-[10px]" /></div>
                            <div className="col-span-2 text-right tabular-nums">{(row.calls ?? 0).toLocaleString()}</div>
                            <div className="col-span-3 text-right tabular-nums">{(row.total_tokens ?? 0).toLocaleString()}</div>
                            <div className="col-span-3 text-right tabular-nums">{formatUsd(row.cost_usd ?? 0)}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">AI cost by auth mode</CardTitle>
                  <CardDescription>Shows whether usage came from `api_key`, `oauth` or other auth modes.</CardDescription>
                </CardHeader>
                <CardContent>
                  {latestAiAuthModeBreakdown.length <= 0 ? (
                    <p className="text-sm text-muted-foreground">No auth-mode AI usage rows for this scope yet.</p>
                  ) : (
                    <div className="border rounded-lg overflow-hidden">
                      <div className="grid grid-cols-12 gap-2 px-3 py-2 text-[11px] uppercase tracking-wide text-muted-foreground bg-muted/30">
                        <div className="col-span-4">Auth mode</div>
                        <div className="col-span-2 text-right">Calls</div>
                        <div className="col-span-3 text-right">Tokens</div>
                        <div className="col-span-3 text-right">USD</div>
                      </div>
                      <div className="divide-y">
                        {latestAiAuthModeBreakdown.map((row, idx) => (
                          <div key={`ai-auth-${row.auth_mode || 'unknown'}-${idx}`} className="grid grid-cols-12 gap-2 px-3 py-2 text-xs">
                            <div className="col-span-4 truncate">{row.auth_mode || 'unknown'}</div>
                            <div className="col-span-2 text-right tabular-nums">{(row.calls ?? 0).toLocaleString()}</div>
                            <div className="col-span-3 text-right tabular-nums">{(row.total_tokens ?? 0).toLocaleString()}</div>
                            <div className="col-span-3 text-right tabular-nums">{formatUsd(row.cost_usd ?? 0)}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">AI cost per scan</CardTitle>
                  <CardDescription>Recent completed scans with calls/tokens/USD.</CardDescription>
                </CardHeader>
                <CardContent>
                  {recentAiScanRows.length <= 0 ? (
                    <p className="text-sm text-muted-foreground">No completed scan yet.</p>
                  ) : (
                    <div className="border rounded-lg overflow-hidden">
                      <div className="grid grid-cols-12 gap-2 px-3 py-2 text-[11px] uppercase tracking-wide text-muted-foreground bg-muted/30">
                        <div className="col-span-2">Scan</div>
                        <div className="col-span-3">Started</div>
                        <div className="col-span-2 text-right">Calls</div>
                        <div className="col-span-2 text-right">Tokens</div>
                        <div className="col-span-1 text-right">Albums</div>
                        <div className="col-span-2 text-right">USD</div>
                      </div>
                      <div className="divide-y">
                        {recentAiScanRows.map((row) => (
                          <div key={`ai-scan-${row.scanId}`} className="grid grid-cols-12 gap-2 px-3 py-2 text-xs">
                            <div className="col-span-2 tabular-nums">#{row.scanId}</div>
                            <div className="col-span-3 truncate">{format(new Date(row.startTime * 1000), 'MM-dd HH:mm')}</div>
                            <div className="col-span-2 text-right tabular-nums">{row.aiCallsTotal.toLocaleString()}</div>
                            <div className="col-span-2 text-right tabular-nums">{row.aiTokensTotal.toLocaleString()}</div>
                            <div className="col-span-1 text-right tabular-nums">{row.albumsScanned.toLocaleString()}</div>
                            <div className="col-span-2 text-right tabular-nums">{formatUsd(row.aiCostUsdTotal)}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">AI cost per album</CardTitle>
                  <CardDescription>
                    Scope: {aiCostScope} · top {latestAiCostByAlbum?.limit || AI_COST_ALBUM_LIMIT} albums for scan #{aiCostScanId ?? '-'}.
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  {latestAiAlbumBreakdown.length <= 0 ? (
                    <p className="text-sm text-muted-foreground">No album-level AI usage rows for this scope yet.</p>
                  ) : (
                    <div className="border rounded-lg overflow-hidden">
                      <div className="grid grid-cols-12 gap-2 px-3 py-2 text-[11px] uppercase tracking-wide text-muted-foreground bg-muted/30">
                        <div className="col-span-4">Artist</div>
                        <div className="col-span-3">Album</div>
                        <div className="col-span-1 text-right">Calls</div>
                        <div className="col-span-2 text-right">Tokens</div>
                        <div className="col-span-2 text-right">USD</div>
                      </div>
                      <div className="divide-y">
                        {latestAiAlbumBreakdown.map((row, idx) => (
                          <div key={`ai-album-${row.album_id ?? 'none'}-${idx}`} className="grid grid-cols-12 gap-2 px-3 py-2 text-xs">
                            <div className="col-span-4 truncate">{row.album_artist || 'Unknown artist'}</div>
                            <div className="col-span-3 truncate">{row.album_title || (row.album_id ? `Album #${row.album_id}` : 'Unknown album')}</div>
                            <div className="col-span-1 text-right tabular-nums">{(row.calls ?? 0).toLocaleString()}</div>
                            <div className="col-span-2 text-right tabular-nums">{(row.total_tokens ?? 0).toLocaleString()}</div>
                            <div className="col-span-2 text-right tabular-nums">{formatUsd(row.cost_usd ?? 0)}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="quality" className="space-y-4">
              <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
                <StatCard
                  title="All Tags"
                  icon={<Tag className="w-4 h-4 text-primary" />}
                  value={`${current.withTags.toLocaleString()} / ${current.albumsScanned.toLocaleString()}`}
                  description={formatPercent(current.withTags, current.albumsScanned)}
                  delta={<DeltaPill current={current.withTags} previous={previous.withTags} />}
                />
                <StatCard
                  title="Cover Art"
                  icon={<Image className="w-4 h-4 text-secondary" />}
                  value={`${current.withCover.toLocaleString()} / ${current.albumsScanned.toLocaleString()}`}
                  description={formatPercent(current.withCover, current.albumsScanned)}
                  delta={<DeltaPill current={current.withCover} previous={previous.withCover} />}
                />
                <StatCard
                  title="Artist Image"
                  icon={<Image className="w-4 h-4 text-info" />}
                  value={`${current.withArtistImage.toLocaleString()} / ${current.albumsScanned.toLocaleString()}`}
                  description={formatPercent(current.withArtistImage, current.albumsScanned)}
                  delta={<DeltaPill current={current.withArtistImage} previous={previous.withArtistImage} />}
                />
                <StatCard
                  title="Fully Complete"
                  icon={<Sparkles className="w-4 h-4 text-emerald-500" />}
                  value={`${current.fullyComplete.toLocaleString()} / ${current.albumsScanned.toLocaleString()}`}
                  description={formatPercent(current.fullyComplete, current.albumsScanned)}
                  delta={<DeltaPill current={current.fullyComplete} previous={previous.fullyComplete} />}
                />
              </div>
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Missing counts</CardTitle>
                  <CardDescription>Use this to target the next fix pass.</CardDescription>
                </CardHeader>
                <CardContent className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                  <div className="rounded-lg border border-border p-3">
                    <p className="text-muted-foreground">Missing tags</p>
                    <p className="text-xl font-semibold tabular-nums">{current.withoutTags.toLocaleString()}</p>
                  </div>
                  <div className="rounded-lg border border-border p-3">
                    <p className="text-muted-foreground">Missing cover</p>
                    <p className="text-xl font-semibold tabular-nums">{current.withoutCover.toLocaleString()}</p>
                  </div>
                  <div className="rounded-lg border border-border p-3">
                    <p className="text-muted-foreground">Missing artist image</p>
                    <p className="text-xl font-semibold tabular-nums">{current.withoutArtistImage.toLocaleString()}</p>
                  </div>
                  <div className="rounded-lg border border-border p-3">
                    <p className="text-muted-foreground">Broken albums</p>
                    <p className="text-xl font-semibold tabular-nums">{current.brokenAlbums.toLocaleString()}</p>
                  </div>
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Quality coverage</CardTitle>
                  <CardDescription>Coverage rate by quality dimension.</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-[260px]">
                    <Bar
                      data={qualityCoverageData}
                      options={{
                        ...chartOptions,
                        scales: {
                          y: {
                            beginAtZero: true,
                            max: 100,
                            ticks: { callback: (value) => `${value}%` },
                          },
                        },
                      }}
                    />
                  </div>
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Issue composition</CardTitle>
                  <CardDescription>Pie chart of missing metadata and broken albums.</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-[260px] flex items-center justify-center">
                    <Pie data={qualityIssuePieData} options={chartOptions} />
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="benchmark" className="space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
                <StatCard
                  title="Benchmark Runs"
                  icon={<Layers className="w-4 h-4 text-primary" />}
                  value={benchmarkReports.length.toLocaleString()}
                  description="Saved benchmark reports"
                />
                <StatCard
                  title="Latest Score"
                  icon={<Gauge className="w-4 h-4 text-success" />}
                  value={latestBenchmark ? `${Number(latestBenchmark.score || 0).toFixed(2)}%` : 'N/A'}
                  description={latestBenchmark ? `scan #${latestBenchmark.scan_id}` : 'No report yet'}
                />
                <StatCard
                  title="Average Score"
                  icon={<BarChart3 className="w-4 h-4 text-info" />}
                  value={benchmarkReports.length > 0 ? `${benchmarkAverageScore.toFixed(2)}%` : 'N/A'}
                />
                <StatCard
                  title="Best Score"
                  icon={<Sparkles className="w-4 h-4 text-emerald-500" />}
                  value={benchmarkReports.length > 0 ? `${benchmarkBestScore.toFixed(2)}%` : 'N/A'}
                />
              </div>

              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Benchmark Trend</CardTitle>
                  <CardDescription>Score evolution across saved benchmark runs.</CardDescription>
                </CardHeader>
                <CardContent>
                  {benchmarkReports.length <= 1 ? (
                    <p className="text-sm text-muted-foreground">Run at least 2 benchmark cycles to display a trend.</p>
                  ) : (
                    <div className="h-[260px]">
                      <Line
                        data={benchmarkTrendData}
                        options={{
                          ...chartOptions,
                          scales: {
                            y: {
                              beginAtZero: true,
                              max: 100,
                              ticks: { callback: (value) => `${value}%` },
                            },
                          },
                        }}
                      />
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Benchmark Runs</CardTitle>
                  <CardDescription>
                    {benchmarkReportsResponse?.available
                      ? `Source: ${benchmarkReportsResponse.path}`
                      : 'Benchmark reports directory is not available.'}
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  {benchmarkReports.length <= 0 ? (
                    <p className="text-sm text-muted-foreground">No benchmark report found yet.</p>
                  ) : (
                    <div className="max-h-[320px] overflow-auto rounded-md border">
                      <table className="w-full text-sm">
                        <thead className="bg-muted/50 sticky top-0">
                          <tr>
                            <th className="text-left px-3 py-2 font-medium">Generated</th>
                            <th className="text-right px-3 py-2 font-medium">Scan</th>
                            <th className="text-right px-3 py-2 font-medium">Score</th>
                            <th className="text-right px-3 py-2 font-medium">Checks</th>
                            <th className="text-left px-3 py-2 font-medium">Failed checks</th>
                          </tr>
                        </thead>
                        <tbody>
                          {benchmarkReports.map((row) => (
                            <tr key={`bench-${row.file}`} className="border-t border-border">
                              <td className="px-3 py-2 tabular-nums">
                                {Number(row.generated_at || 0) > 0
                                  ? format(new Date(Number(row.generated_at || 0) * 1000), 'yyyy-MM-dd HH:mm:ss')
                                  : 'unknown'}
                              </td>
                              <td className="px-3 py-2 text-right tabular-nums">{Number(row.scan_id || 0).toLocaleString()}</td>
                              <td className="px-3 py-2 text-right tabular-nums font-semibold">{Number(row.score || 0).toFixed(2)}%</td>
                              <td className="px-3 py-2 text-right tabular-nums">{Number(row.pass_count || 0)}/{Number(row.check_count || 0)}</td>
                              <td className="px-3 py-2">
                                {Array.isArray(row.failed_checks) && row.failed_checks.length > 0
                                  ? row.failed_checks.join(', ')
                                  : 'none'}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="operations" className="space-y-4">
              <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
                <StatCard
                  title="Total Duration"
                  icon={<Clock className="w-4 h-4 text-primary" />}
                  value={formatDuration(current.durationSeconds)}
                />
                <StatCard
                  title="Avg Duration"
                  icon={<Clock className="w-4 h-4 text-info" />}
                  value={formatDuration(Math.round(averageDuration))}
                  description="Average duration per scan"
                />
                <StatCard
                  title="Throughput"
                  icon={<Gauge className="w-4 h-4 text-success" />}
                  value={`${throughputAlbumsPerMin.toFixed(1)} albums/min`}
                />
                <StatCard
                  title="Moved"
                  icon={<Layers className="w-4 h-4 text-warning" />}
                  value={current.albumsMoved.toLocaleString()}
                  description="Albums moved to dupes"
                  delta={<DeltaPill current={current.albumsMoved} previous={previous.albumsMoved} />}
                />
                <StatCard
                  title="Audio Cache Hit Rate"
                  icon={<Gauge className="w-4 h-4 text-primary" />}
                  value={audioCacheTotal > 0 ? `${audioCacheHitRate.toFixed(1)}%` : 'N/A'}
                  description={`${current.audioCacheHits.toLocaleString()} hits / ${audioCacheTotal.toLocaleString()} req`}
                />
                <StatCard
                  title="MB Cache Hit Rate"
                  icon={<Gauge className="w-4 h-4 text-secondary" />}
                  value={mbCacheTotal > 0 ? `${mbCacheHitRate.toFixed(1)}%` : 'N/A'}
                  description={`${current.mbCacheHits.toLocaleString()} hits / ${mbCacheTotal.toLocaleString()} req`}
                />
              </div>
              <Card>
                <CardHeader className="pb-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <CardTitle className="text-sm flex items-center gap-2">
                        <Server className="w-4 h-4 text-primary" />
                        Cache Control Center
                      </CardTitle>
                      <CardDescription>
                        Redis/PostgreSQL/SQLite/media cache telemetry with live runtime memory.
                      </CardDescription>
                    </div>
                    <div className="flex items-center gap-2">
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        className="gap-1.5"
                        onClick={() => {
                          void restartWatcher();
                        }}
                        disabled={watcherRestarting}
                      >
                        {watcherRestarting ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                        Restart watcher
                      </Button>
                    </div>
                  </div>
                </CardHeader>
                <CardContent className="space-y-2">
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant={cacheControl?.cache_policies?.scan_disable_cache ? 'destructive' : 'secondary'}>
                      SCAN_DISABLE_CACHE: {cacheControl?.cache_policies?.scan_disable_cache ? 'ON' : 'OFF'}
                    </Badge>
                    <Badge variant={cacheControl?.cache_policies?.mb_disable_cache ? 'destructive' : 'secondary'}>
                      MB_DISABLE_CACHE: {cacheControl?.cache_policies?.mb_disable_cache ? 'ON' : 'OFF'}
                    </Badge>
                    <Badge
                      variant={redisStatus.variant}
                      title={redisStatus.reason ? `Reason: ${redisStatus.reason}` : undefined}
                    >
                      Redis: {redisStatus.label}
                    </Badge>
                    <Badge
                      variant={postgresStatus.variant}
                      title={postgresStatus.reason ? `Reason: ${postgresStatus.reason}` : undefined}
                    >
                      PostgreSQL: {postgresStatus.label}
                    </Badge>
                    <Badge
                      variant={watcherStatus.variant}
                      title={watcherStatus.reason ? `Reason: ${watcherStatus.reason}` : undefined}
                    >
                      Watcher: {watcherStatus.label}
                    </Badge>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Last update: {cacheControl?.generated_at ? format(new Date(cacheControl.generated_at * 1000), 'yyyy-MM-dd HH:mm:ss') : 'n/a'}
                  </p>
                </CardContent>
              </Card>
              <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
                <StatCard
                  title="Container RAM"
                  icon={<HardDrive className="w-4 h-4 text-warning" />}
                  value={containerMemoryLimit > 0 ? `${formatBytes(containerMemoryUsed)} / ${formatBytes(containerMemoryLimit)}` : formatBytes(containerMemoryUsed)}
                  description={containerMemoryUsedPct != null ? `${containerMemoryUsedPct.toFixed(1)}% used` : 'No cgroup limit detected'}
                />
                <StatCard
                  title="PMDA Process RSS"
                  icon={<Gauge className="w-4 h-4 text-info" />}
                  value={formatBytes(processRssBytes)}
                  description="Backend process resident memory"
                />
                <StatCard
                  title="Redis Memory"
                  icon={<Database className="w-4 h-4 text-primary" />}
                  value={formatBytes(redisUsedBytes)}
                  description={redisMaxBytes > 0 ? `${formatBytes(redisHeadroomBytes)} headroom` : 'No maxmemory configured'}
                />
                <StatCard
                  title="PostgreSQL Size"
                  icon={<Database className="w-4 h-4 text-secondary" />}
                  value={formatBytes(postgresDbBytes)}
                  description={cacheControl?.postgres?.db_cache_hit_rate_pct != null ? `DB cache hit ${cacheControl.postgres.db_cache_hit_rate_pct.toFixed(1)}%` : 'DB cache hit n/a'}
                />
                <StatCard
                  title="Media Cache"
                  icon={<Image className="w-4 h-4 text-success" />}
                  value={formatBytes(mediaCacheBytes)}
                  description={`${n(cacheControl?.media_cache?.total?.file_count).toLocaleString()} files`}
                />
                <StatCard
                  title="Files Scan Cache"
                  icon={<Layers className="w-4 h-4 text-primary" />}
                  value={n(cacheControl?.sqlite_state_db?.files_album_scan_cache_rows).toLocaleString()}
                  description={`${n(cacheControl?.sqlite_state_db?.files_album_scan_cache_healthy_rows).toLocaleString()} healthy rows`}
                />
                <StatCard
                  title="Provider Lookup Cache"
                  icon={<Database className="w-4 h-4 text-pink-500" />}
                  value={n(cacheControl?.sqlite_cache_db?.provider_album_lookup_rows).toLocaleString()}
                  description={`${n(cacheControl?.sqlite_cache_db?.provider_album_lookup_not_found_rows).toLocaleString()} negative rows`}
                />
                <StatCard
                  title="Watcher Queue"
                  icon={<AlertCircle className="w-4 h-4 text-warning" />}
                  value={n(cacheControl?.sqlite_state_db?.files_pending_changes_rows).toLocaleString()}
                  description={`${n(cacheControl?.files_watcher?.dirty_count).toLocaleString()} events since boot`}
                />
                <StatCard
                  title="Redis PMDA Keys"
                  icon={<Database className="w-4 h-4 text-destructive" />}
                  value={n(cacheControl?.redis?.pmda_prefix_keys).toLocaleString()}
                  description={cacheControl?.redis?.pmda_prefix_scan_truncated ? 'Prefix count capped for safety' : `${n(cacheControl?.redis?.db_keys).toLocaleString()} keys in DB`}
                />
              </div>
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Operations trend</CardTitle>
                  <CardDescription>Scan duration vs throughput over time.</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-[260px]">
                    <Line
                      data={operationsTrendData}
                      options={{
                        ...chartOptions,
                        scales: {
                          y: { type: 'linear', position: 'left', beginAtZero: true },
                          y1: {
                            type: 'linear',
                            position: 'right',
                            beginAtZero: true,
                            grid: { drawOnChartArea: false },
                          },
                        },
                      }}
                    />
                  </div>
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Cache hits vs misses</CardTitle>
                  <CardDescription>Stacked bars for audio and MusicBrainz cache effectiveness.</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-[260px]">
                    <Bar
                      data={cacheMixData}
                      options={{
                        ...chartOptions,
                        scales: {
                          x: { stacked: true },
                          y: { stacked: true, beginAtZero: true },
                        },
                      }}
                    />
                  </div>
                </CardContent>
              </Card>
              <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
                <Card className="xl:col-span-2">
                  <CardHeader>
                    <CardTitle className="text-sm">Cache footprint by layer</CardTitle>
                    <CardDescription>Combined memory/storage footprint across Redis, PostgreSQL, media cache, and SQLite files.</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-[260px]">
                      <Bar
                        data={cacheStorageData}
                        options={{
                          ...chartOptions,
                          plugins: { ...chartOptions.plugins, legend: { display: false } },
                          scales: {
                            y: {
                              beginAtZero: true,
                              ticks: {
                                callback: (value) => formatBytes(Number(value)),
                              },
                            },
                          },
                        }}
                      />
                    </div>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">Redis memory usage</CardTitle>
                    <CardDescription>
                      {redisHitRateLive != null
                        ? `Keyspace hit rate ${redisHitRateLive.toFixed(1)}%`
                        : 'Keyspace hit rate unavailable'}
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-[260px] flex items-center justify-center">
                      <Doughnut data={redisMemoryData} options={{ ...chartOptions, cutout: '62%' }} />
                    </div>
                  </CardContent>
                </Card>
              </div>
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Cache entries by subsystem</CardTitle>
                  <CardDescription>How many objects are currently indexed in each cache layer.</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-[280px]">
                    <Bar
                      data={cacheEntriesBarData}
                      options={{
                        ...chartOptions,
                        plugins: { ...chartOptions.plugins, legend: { display: false } },
                        scales: {
                          y: { beginAtZero: true },
                        },
                      }}
                    />
                  </div>
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Scan Move Audit (strict gate)</CardTitle>
                  <CardDescription>Result of `tools/audit_scan_moves.py` for latest completed scan.</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  {!scanMovesAudit ? (
                    <p className="text-sm text-muted-foreground">No audit data available yet.</p>
                  ) : (
                    <>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                        <div className="rounded-lg border border-border p-3">
                          <p className="text-muted-foreground">Moves total</p>
                          <p className="text-xl font-semibold tabular-nums">{Number(scanMovesAudit.summary?.moves_total ?? 0).toLocaleString()}</p>
                        </div>
                        <div className="rounded-lg border border-border p-3">
                          <p className="text-muted-foreground">Dedupe strict ✅</p>
                          <p className="text-xl font-semibold tabular-nums">{Number(scanMovesAudit.summary?.dedupe_strict_yes ?? 0).toLocaleString()}</p>
                        </div>
                        <div className="rounded-lg border border-border p-3">
                          <p className="text-muted-foreground">Dedupe strict ❌</p>
                          <p className="text-xl font-semibold tabular-nums">{Number(scanMovesAudit.summary?.dedupe_strict_no ?? 0).toLocaleString()}</p>
                        </div>
                        <div className="rounded-lg border border-border p-3">
                          <p className="text-muted-foreground">Incomplete strict ❌</p>
                          <p className="text-xl font-semibold tabular-nums">{Number(scanMovesAudit.summary?.incomplete_strict_no ?? 0).toLocaleString()}</p>
                        </div>
                      </div>
                      <p className="text-xs text-muted-foreground">
                        Scan #{scanMovesAudit.scan_id} · strict columns: {scanMovesAudit.strict_columns_present ? 'present' : 'missing'}
                      </p>
                    </>
                  )}
                </CardContent>
              </Card>
            </TabsContent>
            <TabsContent value="duplicates" className="space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
                <StatCard
                  title="Moved duplicates"
                  icon={<Layers className="w-4 h-4 text-rose-400" />}
                  value={duplicateMoves.length.toLocaleString()}
                  description={`Latest scan #${latestCompletedScanId ?? '—'}`}
                />
                <StatCard
                  title="Active moves"
                  icon={<AlertCircle className="w-4 h-4 text-warning" />}
                  value={duplicateMoves.filter((move) => String(move.status || '').toLowerCase() === 'moved').length.toLocaleString()}
                  description="Still quarantined in dupe storage"
                />
                <StatCard
                  title="Restored"
                  icon={<RefreshCw className="w-4 h-4 text-success" />}
                  value={duplicateMoves.filter((move) => String(move.status || '').toLowerCase() === 'restored').length.toLocaleString()}
                  description="Rolled back by the user"
                />
                <StatCard
                  title="Space involved"
                  icon={<HardDrive className="w-4 h-4 text-info" />}
                  value={formatBytes(Math.round(duplicateMoves.reduce((acc, move) => acc + Number(move.size_mb || 0), 0) * 1024 * 1024))}
                  description="Total moved duplicate footprint"
                />
              </div>
              <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">Why duplicates were moved</CardTitle>
                    <CardDescription>Top decision reasons captured for duplicate losers.</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-[260px] flex items-center justify-center">
                      <Doughnut
                        data={{
                          labels: duplicateReasonSummary.labels,
                          datasets: [{
                            data: duplicateReasonSummary.values,
                            backgroundColor: ['#fb7185', '#f97316', '#f59e0b', '#ef4444', '#ec4899', '#a855f7', '#14b8a6', '#60a5fa'],
                            borderWidth: 0,
                          }],
                        }}
                        options={{ ...chartOptions, cutout: '58%' }}
                      />
                    </div>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">Duplicate providers</CardTitle>
                    <CardDescription>Which provider supplied the dedupe decision context.</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-[260px]">
                      <Bar
                        data={{
                          labels: duplicateProviderSummary.labels,
                          datasets: [{
                            label: 'Moves',
                            data: duplicateProviderSummary.values,
                            backgroundColor: '#fb7185',
                            borderRadius: 8,
                          }],
                        }}
                        options={{ ...chartOptions, plugins: { ...chartOptions.plugins, legend: { display: false } }, scales: { y: { beginAtZero: true } } }}
                      />
                    </div>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">Duplicate move status</CardTitle>
                    <CardDescription>Current lifecycle of duplicate moves from the latest scan.</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-3">
                      {duplicateStatusSummary.labels.length === 0 ? (
                        <p className="text-sm text-muted-foreground">No duplicate move data available yet.</p>
                      ) : duplicateStatusSummary.labels.map((label, idx) => (
                        <div key={`dup-status-${label}`} className="flex items-center justify-between rounded-lg border border-border p-3 text-sm">
                          <span className="capitalize text-muted-foreground">{label.replace(/_/g, ' ')}</span>
                          <span className="font-semibold tabular-nums">{duplicateStatusSummary.values[idx].toLocaleString()}</span>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              </div>
            </TabsContent>
            <TabsContent value="incompletes" className="space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
                <StatCard
                  title="Moved incompletes"
                  icon={<AlertCircle className="w-4 h-4 text-orange-400" />}
                  value={incompleteMoves.length.toLocaleString()}
                  description={`Latest scan #${latestCompletedScanId ?? '—'}`}
                />
                <StatCard
                  title="Active moves"
                  icon={<Layers className="w-4 h-4 text-warning" />}
                  value={incompleteMoves.filter((move) => String(move.status || '').toLowerCase() === 'moved').length.toLocaleString()}
                  description="Still quarantined as incomplete"
                />
                <StatCard
                  title="Restored"
                  icon={<RefreshCw className="w-4 h-4 text-success" />}
                  value={incompleteMoves.filter((move) => String(move.status || '').toLowerCase() === 'restored').length.toLocaleString()}
                  description="Rolled back by the user"
                />
                <StatCard
                  title="Space involved"
                  icon={<HardDrive className="w-4 h-4 text-info" />}
                  value={formatBytes(Math.round(incompleteMoves.reduce((acc, move) => acc + Number(move.size_mb || 0), 0) * 1024 * 1024))}
                  description="Total moved incomplete footprint"
                />
              </div>
              <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">Why albums were marked incomplete</CardTitle>
                    <CardDescription>Trend of incomplete reasons from the latest scan.</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-[260px] flex items-center justify-center">
                      <Doughnut
                        data={{
                          labels: incompleteReasonSummary.labels,
                          datasets: [{
                            data: incompleteReasonSummary.values,
                            backgroundColor: ['#fb923c', '#f59e0b', '#f97316', '#f43f5e', '#facc15', '#22c55e', '#38bdf8', '#a855f7'],
                            borderWidth: 0,
                          }],
                        }}
                        options={{ ...chartOptions, cutout: '58%' }}
                      />
                    </div>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">Incomplete providers</CardTitle>
                    <CardDescription>Providers involved in incomplete diagnostics and move decisions.</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-[260px]">
                      <Bar
                        data={{
                          labels: incompleteProviderSummary.labels,
                          datasets: [{
                            label: 'Moves',
                            data: incompleteProviderSummary.values,
                            backgroundColor: '#fb923c',
                            borderRadius: 8,
                          }],
                        }}
                        options={{ ...chartOptions, plugins: { ...chartOptions.plugins, legend: { display: false } }, scales: { y: { beginAtZero: true } } }}
                      />
                    </div>
                  </CardContent>
                </Card>
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">Incomplete move status</CardTitle>
                    <CardDescription>Current lifecycle of incomplete moves from the latest scan.</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-3">
                      {incompleteStatusSummary.labels.length === 0 ? (
                        <p className="text-sm text-muted-foreground">No incomplete move data available yet.</p>
                      ) : incompleteStatusSummary.labels.map((label, idx) => (
                        <div key={`inc-status-${label}`} className="flex items-center justify-between rounded-lg border border-border p-3 text-sm">
                          <span className="capitalize text-muted-foreground">{label.replace(/_/g, ' ')}</span>
                          <span className="font-semibold tabular-nums">{incompleteStatusSummary.values[idx].toLocaleString()}</span>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              </div>
              {scanMovesSummary ? (
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">Move summary snapshot</CardTitle>
                    <CardDescription>Latest scan-level move totals persisted by PMDA.</CardDescription>
                  </CardHeader>
                  <CardContent className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-3">
                    {Object.entries(scanMovesSummary.by_reason || {}).map(([reason, stats]) => (
                      <div key={`move-summary-${reason}`} className="rounded-lg border border-border p-3 text-sm">
                        <div className="font-medium capitalize">{reason}</div>
                        <div className="mt-2 space-y-1 text-muted-foreground">
                          <div>Total moved: {Number(stats.total_moved || 0).toLocaleString()}</div>
                          <div>Active: {Number(stats.pending || 0).toLocaleString()}</div>
                          <div>Restored: {Number(stats.restored || 0).toLocaleString()}</div>
                          <div>Size: {formatBytes(Math.round(Number(stats.size_mb || 0) * 1024 * 1024))}</div>
                        </div>
                      </div>
                    ))}
                  </CardContent>
                </Card>
              ) : null}
            </TabsContent>
            {isAdmin ? (
              <TabsContent value="pipeline" className="space-y-4">
                <PipelineTracePanel
                  history={history}
                  liveScanId={scanProgress?.scan_id ?? null}
                  liveScanActive={Boolean(scanProgress?.scanning || scanProgress?.post_processing)}
                />
              </TabsContent>
            ) : null}
          </Tabs>
        )}
    </div>
  );
}
