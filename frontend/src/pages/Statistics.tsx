import { useEffect, useMemo, useRef, useState } from 'react';
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
import { useNavigate } from 'react-router-dom';
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
import type { CacheControlMetrics, ScanHistoryEntry, ScanProgress } from '@/lib/api';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import * as api from '@/lib/api';

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

type PeriodKey = (typeof PERIODS)[number]['key'];
type StatsTab = 'overview' | 'metadata' | 'quality' | 'operations';

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
  };
}

function normalizeLiveScan(progress: ScanProgress, fallbackStartTime: number): ScanSnapshot {
  const totalAlbums = Math.max(
    n(progress.total_albums),
    n(progress.post_processing_total),
    n(progress.mb_done_count),
    n(progress.format_done_count),
  );
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
    matchedAlbums,
    albumsWithMbId,
    mbVerifiedByAi: 0,
    discogsMatches,
    lastfmMatches,
    bandcampMatches,
    withTags: Math.max(0, totalAlbums - withoutTags),
    withCover: Math.max(0, totalAlbums - withoutCover),
    withArtistImage: Math.max(0, totalAlbums - withoutArtistImage),
    fullyComplete: Math.max(0, totalAlbums - Math.max(withoutTags, withoutCover, withoutArtistImage)),
    withoutTags,
    withoutCover,
    withoutArtistImage,
    brokenAlbums: n(progress.broken_albums_count),
    audioCacheHits: n(progress.audio_cache_hits),
    audioCacheMisses: n(progress.audio_cache_misses),
    mbCacheHits: n(progress.mb_cache_hits),
    mbCacheMisses: n(progress.mb_cache_misses),
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
        <CardTitle className="text-caption font-medium flex items-center gap-1.5 text-muted-foreground uppercase tracking-wider">
          {icon}
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-1">
        <p className="text-display font-bold text-foreground tabular-nums">{value}</p>
        {description && <p className="text-caption text-muted-foreground">{description}</p>}
        {delta}
      </CardContent>
    </Card>
  );
}

export default function Statistics() {
  const navigate = useNavigate();
  const [period, setPeriod] = useState<PeriodKey>('last');
  const [activeTab, setActiveTab] = useState<StatsTab>('overview');
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
  const {
    data: cacheControl,
    refetch: refetchCacheControl,
    isFetching: cacheControlRefreshing,
  } = useQuery<CacheControlMetrics>({
    queryKey: ['stats-cache-control'],
    queryFn: () => api.getCacheControlMetrics(false),
    refetchInterval: (scanProgress?.scanning || scanProgress?.post_processing) ? 5000 : 15000,
    refetchIntervalInBackground: true,
  });
  const isLiveRunActive = Boolean(scanProgress?.scanning || scanProgress?.post_processing);
  const liveRunArtistsTotal = n(scanProgress?.artists_total);
  const liveRunAlbumsTotal = n(scanProgress?.total_albums);
  const liveDetectedArtistsTotal = n(scanProgress?.detected_artists_total);
  const liveDetectedAlbumsTotal = n(scanProgress?.detected_albums_total);
  const liveSkippedArtists = n(scanProgress?.resume_skipped_artists);
  const liveSkippedAlbums = n(scanProgress?.resume_skipped_albums);

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
    <div className="container mx-auto p-6 space-y-6">
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div>
            <h1 className="text-display text-foreground">Statistics</h1>
            <p className="text-small text-muted-foreground mt-1">Single source of truth from completed scans</p>
          </div>
          <div className="flex flex-wrap items-center justify-end gap-2">
            <Button variant="outline" size="sm" onClick={() => navigate('/statistics/listening')} className="gap-2">
              <Clock className="h-4 w-4" />
              Listening
            </Button>
            <Button variant="outline" size="sm" onClick={() => navigate('/statistics/library')} className="gap-2">
              <Database className="h-4 w-4" />
              Library
            </Button>
            <div className="flex rounded-lg border border-border p-0.5 bg-muted/30">
              {PERIODS.map((p) => (
                <Button
                  key={p.key}
                  variant={period === p.key ? 'secondary' : 'ghost'}
                  size="sm"
                  className="px-3"
                  onClick={() => setPeriod(p.key)}
                >
                  {p.label}
                </Button>
              ))}
            </div>
          </div>
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
            </CardContent>
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
            <TabsList className="grid grid-cols-2 md:grid-cols-4 w-full md:w-auto rounded-xl bg-muted/60 border border-border p-1 shadow-sm">
              <TabsTrigger value="overview" className="gap-2 rounded-lg">
                <BarChart3 className="w-4 h-4" />
                Overview
              </TabsTrigger>
              <TabsTrigger value="metadata" className="gap-2 rounded-lg">
                <Database className="w-4 h-4" />
                Metadata
              </TabsTrigger>
              <TabsTrigger value="quality" className="gap-2 rounded-lg">
                <Sparkles className="w-4 h-4" />
                Quality
              </TabsTrigger>
              <TabsTrigger value="operations" className="gap-2 rounded-lg">
                <Gauge className="w-4 h-4" />
                Operations
              </TabsTrigger>
            </TabsList>

            <TabsContent value="overview" className="space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-4">
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
                    <CardTitle className="text-caption font-medium flex items-center gap-1.5 text-muted-foreground uppercase tracking-wider">
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

            <TabsContent value="quality" className="space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
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

            <TabsContent value="operations" className="space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-4">
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
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="gap-1.5"
                      onClick={() => {
                        void refetchCacheControl();
                      }}
                      disabled={cacheControlRefreshing}
                    >
                      {cacheControlRefreshing ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                      Refresh
                    </Button>
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
                    <Badge variant={cacheControl?.redis?.available ? 'secondary' : 'outline'}>
                      Redis: {cacheControl?.redis?.available ? 'connected' : 'offline'}
                    </Badge>
                    <Badge variant={cacheControl?.postgres?.available ? 'secondary' : 'outline'}>
                      PostgreSQL: {cacheControl?.postgres?.available ? 'connected' : 'offline'}
                    </Badge>
                    <Badge variant={cacheControl?.files_watcher?.running ? 'secondary' : 'outline'}>
                      Watcher: {cacheControl?.files_watcher?.running ? 'running' : 'stopped'}
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
            </TabsContent>
          </Tabs>
        )}
    </div>
  );
}
