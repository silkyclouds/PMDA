import { useMemo, useState } from 'react';
import { Loader2, BarChart2, Disc3, Trash2, HardDrive, Cpu, AlertCircle } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { format, subHours, subDays } from 'date-fns';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js';
import { Bar, Line, Doughnut } from 'react-chartjs-2';
import { Header } from '@/components/Header';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import * as api from '@/lib/api';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

const PERIODS = [
  { key: 'daily', label: 'Daily', hours: 24 },
  { key: 'weekly', label: 'Weekly', hours: 24 * 7 },
  { key: 'monthly', label: 'Monthly', hours: 24 * 30 },
  { key: 'forever', label: 'Forever', hours: 0 },
] as const;

type PeriodKey = (typeof PERIODS)[number]['key'];

const chartOptions = {
  responsive: true,
  maintainAspectRatio: false,
  animation: { duration: 600 },
  plugins: {
    legend: { position: 'top' as const },
  },
};

const transparentFill = (hex: string, alpha: number) => {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r},${g},${b},${alpha})`;
};

const COLORS = {
  primary: '#8884d8',
  secondary: '#82ca9d',
  tertiary: '#22c55e',
  quaternary: '#f59e0b',
  muted: '#94a3b8',
};

export default function Statistics() {
  const [period, setPeriod] = useState<PeriodKey>('forever');
  const { data: history = [], isLoading } = useQuery({
    queryKey: ['scan-history'],
    queryFn: api.getScanHistory,
  });

  const filtered = useMemo(() => {
    if (period === 'forever') return history;
    const hours = PERIODS.find((p) => p.key === period)?.hours ?? 0;
    const cutoff = Date.now() / 1000 - hours * 3600;
    return history.filter((e) => (e.start_time ?? 0) >= cutoff);
  }, [history, period]);

  const withEndTime = useMemo(() => filtered.filter((e) => e.end_time), [filtered]);
  const scansOnly = useMemo(() => withEndTime.filter((e) => e.entry_type !== 'dedupe'), [withEndTime]);

  const kpis = useMemo(() => {
    const scans = withEndTime.filter((e) => e.entry_type === 'scan').length;
    const dedupes = withEndTime.filter((e) => e.entry_type === 'dedupe').length;
    const albumsScanned = scansOnly.reduce((s, e) => s + (e.albums_scanned ?? 0), 0);
    const duplicatesFound = scansOnly.reduce((s, e) => s + (e.duplicates_found ?? 0), 0);
    const spaceSavedMb = withEndTime.reduce((s, e) => s + (e.space_saved_mb ?? 0), 0);
    const albumsMoved = withEndTime.reduce((s, e) => s + (e.albums_moved ?? 0), 0);
    return { scans, dedupes, albumsScanned, duplicatesFound, spaceSavedMb, albumsMoved };
  }, [withEndTime, scansOnly]);

  const chartDataScans = useMemo(() => {
    const ordered = [...scansOnly].reverse();
    return {
      labels: ordered.map((e) => format(new Date((e.start_time ?? 0) * 1000), 'MMM d HH:mm')),
      datasets: [
        {
          label: 'Albums scanned',
          data: ordered.map((e) => e.albums_scanned ?? 0),
          borderColor: COLORS.primary,
          backgroundColor: transparentFill(COLORS.primary, 0.2),
          fill: true,
          tension: 0.3,
        },
        {
          label: 'Duplicates found',
          data: ordered.map((e) => e.duplicates_found ?? 0),
          borderColor: COLORS.secondary,
          backgroundColor: transparentFill(COLORS.secondary, 0.2),
          fill: true,
          tension: 0.3,
        },
      ],
    };
  }, [scansOnly]);

  const chartDataSpaceSaved = useMemo(() => {
    const ordered = [...withEndTime].reverse();
    return {
      labels: ordered.map((e) => format(new Date((e.start_time ?? 0) * 1000), 'MMM d HH:mm')),
      datasets: [
        {
          label: 'Space saved (MB)',
          data: ordered.map((e) => e.space_saved_mb ?? 0),
          backgroundColor: ordered.map((_, i) => transparentFill(COLORS.primary, 0.6 - (i * 0.1) / Math.max(ordered.length, 1))),
          borderColor: COLORS.primary,
          borderWidth: 1,
        },
      ],
    };
  }, [withEndTime]);

  const chartDataCumulative = useMemo(() => {
    const ordered = [...withEndTime].reverse();
    let cum = 0;
    const cumulative = ordered.map((e) => {
      cum += e.space_saved_mb ?? 0;
      return cum;
    });
    return {
      labels: ordered.map((e) => format(new Date((e.start_time ?? 0) * 1000), 'MMM d HH:mm')),
      datasets: [
        {
          label: 'Cumulative space saved (MB)',
          data: cumulative,
          borderColor: COLORS.tertiary,
          backgroundColor: transparentFill(COLORS.tertiary, 0.15),
          fill: true,
          tension: 0.3,
        },
      ],
    };
  }, [withEndTime]);

  const chartDataAiMb = useMemo(() => {
    const aiTotal = scansOnly.reduce((s, e) => s + (e.ai_used_count ?? 0), 0);
    const mbTotal = scansOnly.reduce((s, e) => s + (e.mb_used_count ?? 0), 0);
    const other = Math.max(0, kpis.albumsScanned - aiTotal - mbTotal);
    return {
      labels: ['AI used', 'MusicBrainz used', 'Other'],
      datasets: [
        {
          data: [aiTotal, mbTotal, other],
          backgroundColor: [transparentFill(COLORS.quaternary, 0.8), transparentFill(COLORS.primary, 0.8), transparentFill(COLORS.muted, 0.5)],
          borderWidth: 1,
          borderColor: ['#f59e0b', '#8884d8', '#94a3b8'],
        },
      ],
    };
  }, [scansOnly, kpis.albumsScanned]);

  const chartDataProblems = useMemo(() => {
    const ordered = [...scansOnly].slice(-15).reverse();
    return {
      labels: ordered.map((e) => format(new Date((e.start_time ?? 0) * 1000), 'MMM d')),
      datasets: [
        {
          label: 'Broken albums',
          data: ordered.map((e) => e.broken_albums_count ?? 0),
          backgroundColor: transparentFill('#ef4444', 0.6),
        },
        {
          label: 'Missing albums',
          data: ordered.map((e) => e.missing_albums_count ?? 0),
          backgroundColor: transparentFill('#f59e0b', 0.6),
        },
        {
          label: 'Without MBID',
          data: ordered.map((e) => e.albums_without_mb_id ?? 0),
          backgroundColor: transparentFill(COLORS.muted, 0.6),
        },
      ],
    };
  }, [scansOnly]);

  const formatMb = (mb: number) => (mb >= 1024 ? `${(mb / 1024).toFixed(1)} GB` : `${mb.toFixed(0)} MB`);

  if (isLoading) {
    return (
      <>
        <Header />
        <div className="flex items-center justify-center min-h-[60vh]">
          <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
        </div>
      </>
    );
  }

  return (
    <>
      <Header />
      <div className="container mx-auto p-6 space-y-6">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h1 className="text-display text-foreground">Statistics</h1>
            <p className="text-small text-muted-foreground mt-1">Scan and undupe metrics over time</p>
          </div>
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

        {/* KPI cards - differentiated with semantic colors */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          <Card className="border-l-4 border-l-primary">
            <CardHeader className="pb-2">
              <CardTitle className="text-caption font-medium flex items-center gap-1.5 text-muted-foreground uppercase tracking-wider">
                <BarChart2 className="w-4 h-4 text-primary" />
                Scans
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-display font-bold text-foreground tabular-nums">{kpis.scans}</p>
              <p className="text-caption text-muted-foreground mt-1">Total runs</p>
            </CardContent>
          </Card>
          <Card className="border-l-4 border-l-secondary">
            <CardHeader className="pb-2">
              <CardTitle className="text-caption font-medium flex items-center gap-1.5 text-muted-foreground uppercase tracking-wider">
                <Trash2 className="w-4 h-4 text-secondary" />
                Undupes
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-display font-bold text-foreground tabular-nums">{kpis.dedupes}</p>
              <p className="text-caption text-muted-foreground mt-1">Cleanup sessions</p>
            </CardContent>
          </Card>
          <Card className="border-l-4 border-l-info">
            <CardHeader className="pb-2">
              <CardTitle className="text-caption font-medium flex items-center gap-1.5 text-muted-foreground uppercase tracking-wider">
                <Disc3 className="w-4 h-4 text-info" />
                Albums
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-display font-bold text-foreground tabular-nums">{kpis.albumsScanned.toLocaleString()}</p>
              <p className="text-caption text-muted-foreground mt-1">Scanned</p>
            </CardContent>
          </Card>
          <Card className="border-l-4 border-l-warning">
            <CardHeader className="pb-2">
              <CardTitle className="text-caption font-medium flex items-center gap-1.5 text-muted-foreground uppercase tracking-wider">
                <AlertCircle className="w-4 h-4 text-warning" />
                Duplicates
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-display font-bold text-warning tabular-nums">{kpis.duplicatesFound.toLocaleString()}</p>
              <p className="text-caption text-muted-foreground mt-1">Found</p>
            </CardContent>
          </Card>
          <Card className="border-l-4 border-l-success">
            <CardHeader className="pb-2">
              <CardTitle className="text-caption font-medium flex items-center gap-1.5 text-muted-foreground uppercase tracking-wider">
                <HardDrive className="w-4 h-4 text-success" />
                Space
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-display font-bold text-success tabular-nums">{formatMb(kpis.spaceSavedMb)}</p>
              <p className="text-caption text-muted-foreground mt-1">Saved</p>
            </CardContent>
          </Card>
          <Card className="border-l-4 border-l-muted-foreground">
            <CardHeader className="pb-2">
              <CardTitle className="text-caption font-medium flex items-center gap-1.5 text-muted-foreground uppercase tracking-wider">
                <Cpu className="w-4 h-4" />
                Moved
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-display font-bold text-foreground tabular-nums">{kpis.albumsMoved.toLocaleString()}</p>
              <p className="text-caption text-muted-foreground mt-1">Albums</p>
            </CardContent>
          </Card>
        </div>

        {filtered.length === 0 ? (
          <Card>
            <CardContent className="py-12 text-center text-muted-foreground">
              No data for the selected period. Run a scan or choose &quot;Forever&quot;.
            </CardContent>
          </Card>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Albums scanned & duplicates */}
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Albums scanned & duplicates found</CardTitle>
                <CardDescription>Per scan (scans only)</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-[280px]">
                  {chartDataScans.datasets[0].data.length > 0 ? (
                    <Line
                      data={chartDataScans}
                      options={{
                        ...chartOptions,
                        scales: {
                          x: { display: true, ticks: { maxTicksLimit: 8 } },
                          y: { beginAtZero: true },
                        },
                      }}
                    />
                  ) : (
                    <p className="text-sm text-muted-foreground flex items-center justify-center h-full">No scan data</p>
                  )}
                </div>
              </CardContent>
            </Card>

            {/* Space saved per scan */}
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Space saved (MB) per scan</CardTitle>
                <CardDescription>Each bar = one scan or dedupe</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-[280px]">
                  {chartDataSpaceSaved.datasets[0].data.some((v) => v > 0) ? (
                    <Bar
                      data={chartDataSpaceSaved}
                      options={{
                        ...chartOptions,
                        scales: {
                          x: { display: true, ticks: { maxTicksLimit: 8 } },
                          y: { beginAtZero: true },
                        },
                      }}
                    />
                  ) : (
                    <p className="text-sm text-muted-foreground flex items-center justify-center h-full">No space saved in period</p>
                  )}
                </div>
              </CardContent>
            </Card>

            {/* Cumulative space saved */}
            <Card className="lg:col-span-2">
              <CardHeader>
                <CardTitle className="text-base">Cumulative space saved (MB)</CardTitle>
                <CardDescription>Running total over the selected period</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-[280px]">
                  {chartDataCumulative.datasets[0].data.some((v) => v > 0) ? (
                    <Line
                      data={chartDataCumulative}
                      options={{
                        ...chartOptions,
                        scales: {
                          x: { display: true, ticks: { maxTicksLimit: 10 } },
                          y: { beginAtZero: true },
                        },
                      }}
                    />
                  ) : (
                    <p className="text-sm text-muted-foreground flex items-center justify-center h-full">No cumulative data</p>
                  )}
                </div>
              </CardContent>
            </Card>

            {/* AI vs MusicBrainz */}
            <Card>
              <CardHeader>
                <CardTitle className="text-base flex items-center gap-1.5">
                  <Cpu className="w-4 h-4" />
                  AI & MusicBrainz usage
                </CardTitle>
                <CardDescription>Albums identified by AI vs MB (scans)</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-[240px] flex items-center justify-center">
                  {(chartDataAiMb.datasets[0].data[0] > 0 || chartDataAiMb.datasets[0].data[1] > 0) ? (
                    <Doughnut
                      data={chartDataAiMb}
                      options={{
                        ...chartOptions,
                        cutout: '60%',
                      }}
                    />
                  ) : (
                    <p className="text-sm text-muted-foreground">No AI/MB data in period</p>
                  )}
                </div>
              </CardContent>
            </Card>

            {/* Problems detected */}
            <Card>
              <CardHeader>
                <CardTitle className="text-base flex items-center gap-1.5">
                  <AlertCircle className="w-4 h-4" />
                  Issues detected
                </CardTitle>
                <CardDescription>Broken, missing, without MBID (last 15 scans)</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-[240px]">
                  {chartDataProblems.datasets.some((d) => d.data.some((v) => v > 0)) ? (
                    <Bar
                      data={chartDataProblems}
                      options={{
                        ...chartOptions,
                        scales: {
                          x: { display: true, stacked: true, ticks: { maxTicksLimit: 8 } },
                          y: { stacked: true, beginAtZero: true },
                        },
                      }}
                    />
                  ) : (
                    <p className="text-sm text-muted-foreground flex items-center justify-center h-full">No issues in period</p>
                  )}
                </div>
              </CardContent>
            </Card>
          </div>
        )}
      </div>
    </>
  );
}
