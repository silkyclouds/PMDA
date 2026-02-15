import { useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { BarChart3, Clock, Headphones, Music2 } from 'lucide-react';
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
import { Bar, Doughnut, Line } from 'react-chartjs-2';

import * as api from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';

ChartJS.register(CategoryScale, LinearScale, BarElement, LineElement, PointElement, ArcElement, Tooltip, Legend);

function fmtDuration(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds || 0));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (h <= 0) return `${m}m`;
  return `${h}h ${m}m`;
}

export default function ListeningStatsPage() {
  const navigate = useNavigate();
  const [days, setDays] = useState(30);

  const { data, isLoading, error, refetch, isFetching } = useQuery({
    queryKey: ['playback-stats', days],
    queryFn: () => api.getPlaybackStats(days),
    staleTime: 10_000,
    refetchOnWindowFocus: false,
  });

  const dailyChart = useMemo(() => {
    const pts = data?.daily || [];
    return {
      labels: pts.map((p) => p.day),
      datasets: [
        {
          label: 'Minutes listened',
          data: pts.map((p) => Math.round((p.seconds || 0) / 60)),
          borderColor: 'hsl(38 92% 50%)',
          backgroundColor: 'rgba(245, 158, 11, 0.18)',
          fill: true,
          tension: 0.25,
          pointRadius: 2,
        },
      ],
    };
  }, [data?.daily]);

  const topArtistsChart = useMemo(() => {
    const rows = (data?.top_artists || []).slice(0, 10);
    return {
      labels: rows.map((r) => r.artist_name),
      datasets: [
        {
          label: 'Minutes',
          data: rows.map((r) => Math.round((r.seconds || 0) / 60)),
          backgroundColor: 'rgba(59, 130, 246, 0.55)',
          borderColor: 'rgba(59, 130, 246, 0.9)',
          borderWidth: 1,
        },
      ],
    };
  }, [data?.top_artists]);

  const topGenresChart = useMemo(() => {
    const rows = (data?.top_genres || []).slice(0, 10);
    const labels = rows.map((r) => r.genre);
    const values = rows.map((r) => Math.round((r.seconds || 0) / 60));
    return {
      labels,
      datasets: [
        {
          label: 'Minutes',
          data: values,
          backgroundColor: [
            'rgba(34, 197, 94, 0.6)',
            'rgba(16, 185, 129, 0.6)',
            'rgba(14, 165, 233, 0.6)',
            'rgba(99, 102, 241, 0.6)',
            'rgba(244, 114, 182, 0.6)',
            'rgba(245, 158, 11, 0.6)',
            'rgba(239, 68, 68, 0.6)',
            'rgba(168, 85, 247, 0.6)',
            'rgba(148, 163, 184, 0.6)',
            'rgba(2, 132, 199, 0.6)',
          ],
          borderWidth: 0,
        },
      ],
    };
  }, [data?.top_genres]);

  const hoursChart = useMemo(() => {
    const pts = data?.hours || [];
    const map = new Map<number, number>();
    for (const p of pts) map.set(p.hour, p.seconds || 0);
    const labels = Array.from({ length: 24 }, (_, i) => String(i).padStart(2, '0'));
    const values = labels.map((_, i) => Math.round((map.get(i) || 0) / 60));
    return {
      labels,
      datasets: [
        {
          label: 'Minutes',
          data: values,
          backgroundColor: 'rgba(148, 163, 184, 0.55)',
          borderWidth: 0,
        },
      ],
    };
  }, [data?.hours]);

  return (
    <div className="container py-6 space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="space-y-1">
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Headphones className="h-5 w-5 text-primary" />
            Listening Statistics
          </h1>
          <p className="text-xs text-muted-foreground">Personal listening habits (single user).</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => navigate('/statistics')} className="gap-2">
            <BarChart3 className="h-4 w-4" />
            Scan stats
          </Button>
          <Button variant="outline" size="sm" onClick={() => navigate('/statistics/library')} className="gap-2">
            <Music2 className="h-4 w-4" />
            Library stats
          </Button>
          <Button variant="outline" size="sm" onClick={() => void refetch()} disabled={isFetching}>
            Refresh
          </Button>
        </div>
      </div>

      <Tabs value={String(days)} onValueChange={(v) => setDays(Number(v) || 30)}>
        <TabsList>
          <TabsTrigger value="7">7d</TabsTrigger>
          <TabsTrigger value="30">30d</TabsTrigger>
          <TabsTrigger value="90">90d</TabsTrigger>
          <TabsTrigger value="365">365d</TabsTrigger>
        </TabsList>
        <TabsContent value={String(days)} className="space-y-6">
          {error ? (
            <Card className="border-border/70">
              <CardContent className="p-5 text-sm text-destructive">Failed to load listening stats.</CardContent>
            </Card>
          ) : null}

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Card className="border-border/70">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm flex items-center gap-2">
                  <Clock className="h-4 w-4 text-primary" />
                  Listening time
                </CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">
                {isLoading ? '—' : fmtDuration(data?.total_seconds || 0)}
              </CardContent>
            </Card>
            <Card className="border-border/70">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">Tracks listened</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">
                {isLoading ? '—' : (data?.distinct_tracks ?? 0).toLocaleString()}
              </CardContent>
            </Card>
            <Card className="border-border/70">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">Plays</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">
                {isLoading ? '—' : (data?.events ?? 0).toLocaleString()}
              </CardContent>
            </Card>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <Card className="border-border/70">
              <CardHeader>
                <CardTitle className="text-sm">Minutes per day</CardTitle>
              </CardHeader>
              <CardContent className="h-72">
                <Line
                  data={dailyChart}
                  options={{
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: { x: { ticks: { maxTicksLimit: 8 } }, y: { beginAtZero: true } },
                  }}
                />
              </CardContent>
            </Card>
            <Card className="border-border/70">
              <CardHeader>
                <CardTitle className="text-sm">Listening by hour</CardTitle>
              </CardHeader>
              <CardContent className="h-72">
                <Bar
                  data={hoursChart}
                  options={{
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: { x: { ticks: { maxTicksLimit: 12 } }, y: { beginAtZero: true } },
                  }}
                />
              </CardContent>
            </Card>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <Card className="border-border/70">
              <CardHeader>
                <CardTitle className="text-sm">Top artists</CardTitle>
              </CardHeader>
              <CardContent className="h-72">
                <Bar
                  data={topArtistsChart}
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
                <CardTitle className="text-sm">Top genres</CardTitle>
              </CardHeader>
              <CardContent className="h-72 flex items-center justify-center">
                <div className="h-64 w-64">
                  <Doughnut data={topGenresChart} options={{ maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } } }} />
                </div>
              </CardContent>
            </Card>
          </div>

          <Card className="border-border/70">
            <CardHeader>
              <CardTitle className="text-sm">Top tracks</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2">
              {(data?.top_tracks || []).length === 0 ? (
                <p className="text-sm text-muted-foreground">No listening data yet.</p>
              ) : (
                <div className="space-y-2">
                  {(data?.top_tracks || []).slice(0, 10).map((t) => (
                    <div key={`pt-${t.track_id}`} className="flex items-center justify-between gap-3 border border-border/60 rounded-lg px-3 py-2">
                      <div className="min-w-0">
                        <button
                          type="button"
                          className="text-sm font-medium truncate hover:underline"
                          onClick={() => navigate(`/library/artist/${t.artist_id}`)}
                          title="Open artist"
                        >
                          {t.track_title}
                        </button>
                        <div className="text-xs text-muted-foreground truncate">
                          {t.artist_name} · {t.album_title}
                        </div>
                      </div>
                      <div className="flex items-center gap-2 shrink-0">
                        <Badge variant="outline" className="text-[10px]">
                          {fmtDuration(t.seconds)}
                        </Badge>
                        <Badge variant="secondary" className="text-[10px]">
                          {t.plays} plays
                        </Badge>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}

