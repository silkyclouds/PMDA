import { useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { BarChart3, Database, Music2, RefreshCw } from 'lucide-react';

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Tooltip,
  Legend,
} from 'chart.js';
import type { ChartData, ChartOptions } from 'chart.js';
import { Bar, Chart, Doughnut } from 'react-chartjs-2';

import * as api from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';

ChartJS.register(CategoryScale, LinearScale, BarElement, LineElement, PointElement, ArcElement, Tooltip, Legend);

function sumCounts(items: Array<{ count: number }>): number {
  return items.reduce((acc, it) => acc + (it?.count ?? 0), 0);
}

function topWithOther<T extends { count: number }>(items: T[], topN: number): { top: T[]; otherCount: number } {
  const sorted = [...(items || [])].sort((a, b) => (b.count ?? 0) - (a.count ?? 0));
  const top = sorted.slice(0, Math.max(1, topN));
  const otherCount = Math.max(0, sumCounts(sorted) - sumCounts(top));
  return { top, otherCount };
}

export default function LibraryStatsPage() {
  const navigate = useNavigate();

  const { data, isLoading, error, refetch, isFetching } = useQuery({
    queryKey: ['library-stats-library'],
    queryFn: () => api.getLibraryStatsLibrary(),
    staleTime: 10_000,
    refetchOnWindowFocus: false,
  });

  const yearsChart = useMemo(() => {
    const pts = data?.years || [];
    return {
      labels: pts.map((p) => String(p.year)),
      datasets: [
        {
          label: 'Albums',
          data: pts.map((p) => p.count || 0),
          backgroundColor: 'rgba(59, 130, 246, 0.55)',
          borderColor: 'rgba(59, 130, 246, 0.9)',
          borderWidth: 1,
        },
      ],
    };
  }, [data?.years]);

  const growthChart = useMemo(() => {
    const pts = data?.growth || [];
    let cum = 0;
    const cumulative = pts.map((p) => {
      cum += p.count || 0;
      return cum;
    });
    return {
      labels: pts.map((p) => p.month),
      datasets: [
        {
          type: 'bar' as const,
          label: 'Albums added / month',
          data: pts.map((p) => p.count || 0),
          backgroundColor: 'rgba(34, 197, 94, 0.35)',
          borderColor: 'rgba(34, 197, 94, 0.75)',
          borderWidth: 1,
        },
        {
          type: 'line' as const,
          label: 'Cumulative',
          data: cumulative,
          borderColor: 'hsl(38 92% 50%)',
          backgroundColor: 'rgba(245, 158, 11, 0.15)',
          tension: 0.25,
          pointRadius: 1,
          yAxisID: 'y1',
        },
      ],
    };
  }, [data?.growth]);

  const genresChart = useMemo(() => {
    const { top, otherCount } = topWithOther(data?.genres || [], 10);
    const labels = top.map((r) => r.genre);
    const values = top.map((r) => r.count || 0);
    if (otherCount > 0) {
      labels.push('Other');
      values.push(otherCount);
    }
    return {
      labels,
      datasets: [
        {
          label: 'Albums',
          data: values,
          backgroundColor: [
            'rgba(14, 165, 233, 0.6)',
            'rgba(99, 102, 241, 0.6)',
            'rgba(34, 197, 94, 0.6)',
            'rgba(16, 185, 129, 0.6)',
            'rgba(245, 158, 11, 0.6)',
            'rgba(239, 68, 68, 0.6)',
            'rgba(168, 85, 247, 0.6)',
            'rgba(244, 114, 182, 0.6)',
            'rgba(148, 163, 184, 0.6)',
            'rgba(2, 132, 199, 0.6)',
            'rgba(71, 85, 105, 0.5)',
          ],
          borderWidth: 0,
        },
      ],
    };
  }, [data?.genres]);

  const labelsChart = useMemo(() => {
    const top = [...(data?.labels || [])].sort((a, b) => (b.count ?? 0) - (a.count ?? 0)).slice(0, 10);
    return {
      labels: top.map((r) => r.label),
      datasets: [
        {
          label: 'Albums',
          data: top.map((r) => r.count || 0),
          backgroundColor: 'rgba(148, 163, 184, 0.55)',
          borderWidth: 0,
        },
      ],
    };
  }, [data?.labels]);

  const labelsChartOptions = useMemo<ChartOptions<'bar'>>(() => {
    const labels = (labelsChart?.labels || []) as string[];
    return {
      indexAxis: 'y' as const,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { beginAtZero: true } },
      onClick: (event: unknown, elements: unknown[], chart: unknown) => {
        // Prefer clicking the bar itself.
        const elems = Array.isArray(elements) ? elements : [];
        let idx: number | null =
          elems.length && typeof (elems[0] as { index?: unknown })?.index !== 'undefined'
            ? Number((elems[0] as { index?: unknown }).index)
            : null;

        // Also allow clicking the Y-axis label area (best-effort).
        const ev = (event && typeof event === 'object') ? (event as { x?: unknown; y?: unknown }) : {};
        const ch = (chart && typeof chart === 'object') ? (chart as { scales?: { y?: unknown } }) : {};
        const yScale = ch.scales?.y as undefined | { left: number; right: number; getValueForPixel: (v: number) => unknown };
        if ((idx == null || !Number.isFinite(idx)) && yScale && typeof ev.x === 'number' && typeof ev.y === 'number') {
          const y = yScale;
          if (ev.x >= y.left && ev.x <= y.right) {
            const v = y.getValueForPixel(ev.y);
            const n = typeof v === 'number' ? v : Number(v);
            if (Number.isFinite(n)) idx = Math.round(n);
          }
        }

        if (idx == null || !Number.isFinite(idx)) return;
        const name = labels[idx];
        if (!name) return;
        navigate(`/library/label/${encodeURIComponent(name)}`);
      },
      onHover: (event: unknown, elements: unknown[], chart: unknown) => {
        const c = (chart && typeof chart === 'object') ? (chart as { canvas?: unknown; scales?: { y?: unknown } }) : {};
        const canvas = c.canvas as HTMLCanvasElement | undefined;
        if (!canvas) return;
        const y = c.scales?.y as undefined | { left: number; right: number; top: number; bottom: number };
        let onLabel = false;
        const ev = (event && typeof event === 'object') ? (event as { x?: unknown; y?: unknown }) : {};
        if (y && typeof ev.x === 'number' && typeof ev.y === 'number') {
          onLabel = ev.x >= y.left && ev.x <= y.right && ev.y >= y.top && ev.y <= y.bottom;
        }
        canvas.style.cursor = elements?.length || onLabel ? 'pointer' : 'default';
      },
    };
  }, [labelsChart?.labels, navigate]);

  const formatsChart = useMemo(() => {
    const top = [...(data?.formats || [])].sort((a, b) => (b.count ?? 0) - (a.count ?? 0)).slice(0, 10);
    return {
      labels: top.map((r) => r.format),
      datasets: [
        {
          label: 'Albums',
          data: top.map((r) => r.count || 0),
          backgroundColor: 'rgba(59, 130, 246, 0.25)',
          borderColor: 'rgba(59, 130, 246, 0.6)',
          borderWidth: 1,
        },
      ],
    };
  }, [data?.formats]);

  const coverChart = useMemo(() => {
    const withCover = data?.quality?.with_cover ?? 0;
    const withoutCover = data?.quality?.without_cover ?? 0;
    return {
      labels: ['With cover', 'Missing cover'],
      datasets: [
        {
          data: [withCover, withoutCover],
          backgroundColor: ['rgba(34, 197, 94, 0.55)', 'rgba(239, 68, 68, 0.45)'],
          borderWidth: 0,
        },
      ],
    };
  }, [data?.quality?.with_cover, data?.quality?.without_cover]);

  const losslessChart = useMemo(() => {
    const lossless = data?.quality?.lossless ?? 0;
    const lossy = data?.quality?.lossy ?? 0;
    return {
      labels: ['Lossless', 'Lossy'],
      datasets: [
        {
          data: [lossless, lossy],
          backgroundColor: ['rgba(14, 165, 233, 0.55)', 'rgba(148, 163, 184, 0.55)'],
          borderWidth: 0,
        },
      ],
    };
  }, [data?.quality?.lossless, data?.quality?.lossy]);

  return (
    <div className="container py-6 space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="space-y-1">
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Database className="h-5 w-5 text-primary" />
            Library Statistics
          </h1>
          <p className="text-xs text-muted-foreground">Distributions and growth from the indexed library (Files mode).</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => navigate('/statistics')} className="gap-2">
            <BarChart3 className="h-4 w-4" />
            Scan stats
          </Button>
          <Button variant="outline" size="sm" onClick={() => navigate('/statistics/listening')} className="gap-2">
            <Music2 className="h-4 w-4" />
            Listening stats
          </Button>
          <Button variant="outline" size="sm" onClick={() => void refetch()} disabled={isFetching} className="gap-2">
            <RefreshCw className={isFetching ? 'h-4 w-4 animate-spin' : 'h-4 w-4'} />
            Refresh
          </Button>
        </div>
      </div>

      {error ? (
        <Card className="border-border/70">
          <CardContent className="p-5 text-sm text-destructive">Failed to load library stats (Files mode required).</CardContent>
        </Card>
      ) : null}

      <div className="flex flex-wrap items-center gap-2">
        <Badge variant="outline">Artists: {isLoading ? '—' : (data?.artists ?? 0).toLocaleString()}</Badge>
        <Badge variant="outline">Albums: {isLoading ? '—' : (data?.albums ?? 0).toLocaleString()}</Badge>
        <Badge variant="outline">Tracks: {isLoading ? '—' : (data?.tracks ?? 0).toLocaleString()}</Badge>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card className="border-border/70">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">With cover</CardTitle>
          </CardHeader>
          <CardContent className="text-2xl font-semibold">{isLoading ? '—' : (data?.quality?.with_cover ?? 0).toLocaleString()}</CardContent>
        </Card>
        <Card className="border-border/70">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Missing cover</CardTitle>
          </CardHeader>
          <CardContent className="text-2xl font-semibold">{isLoading ? '—' : (data?.quality?.without_cover ?? 0).toLocaleString()}</CardContent>
        </Card>
        <Card className="border-border/70">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Lossless</CardTitle>
          </CardHeader>
          <CardContent className="text-2xl font-semibold">{isLoading ? '—' : (data?.quality?.lossless ?? 0).toLocaleString()}</CardContent>
        </Card>
        <Card className="border-border/70">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Lossy</CardTitle>
          </CardHeader>
          <CardContent className="text-2xl font-semibold">{isLoading ? '—' : (data?.quality?.lossy ?? 0).toLocaleString()}</CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">Albums by year</CardTitle>
          </CardHeader>
          <CardContent className="h-72">
            <Bar
              data={yearsChart}
              options={{
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { x: { ticks: { maxTicksLimit: 10 } }, y: { beginAtZero: true } },
              }}
            />
          </CardContent>
        </Card>
        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">Library growth (albums created_at)</CardTitle>
          </CardHeader>
          <CardContent className="h-72">
            <Chart
              type="bar"
              // Mixed bar + line chart (cumulative) triggers strict TS types; Chart.js supports it.
              data={growthChart as unknown as ChartData}
              options={{
                maintainAspectRatio: false,
                scales: {
                  y: { beginAtZero: true, title: { display: true, text: 'Per month' } },
                  y1: {
                    beginAtZero: true,
                    position: 'right',
                    grid: { drawOnChartArea: false },
                    title: { display: true, text: 'Cumulative' },
                  },
                  x: { ticks: { maxTicksLimit: 8 } },
                },
              }}
            />
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">Genre distribution (top 10)</CardTitle>
          </CardHeader>
          <CardContent className="h-72 flex items-center justify-center">
            <div className="h-64 w-64">
              <Doughnut data={genresChart} options={{ maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } } }} />
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">Formats (top 10)</CardTitle>
          </CardHeader>
          <CardContent className="h-72">
            <Bar
              data={formatsChart}
              options={{
                indexAxis: 'y',
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { x: { beginAtZero: true } },
              }}
            />
          </CardContent>
        </Card>
        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">Quality</CardTitle>
          </CardHeader>
          <CardContent className="h-72 grid grid-cols-2 gap-4">
            <div className="h-64">
              <Doughnut data={coverChart} options={{ maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } } }} />
            </div>
            <div className="h-64">
              <Doughnut data={losslessChart} options={{ maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } } }} />
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">Top labels</CardTitle>
          </CardHeader>
          <CardContent className="h-80">
            <Bar
              data={labelsChart}
              options={labelsChartOptions}
            />
          </CardContent>
        </Card>
        <Card className="border-border/70">
          <CardHeader>
            <CardTitle className="text-sm">Notes</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-muted-foreground space-y-2">
            <p>These stats are computed from the indexed library database (Files mode).</p>
            <p>
              Growth uses <code>files_albums.created_at</code> (index insertion time), not the album release date.
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
