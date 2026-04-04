import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { format } from 'date-fns';
import { Download, Loader2, RefreshCw } from 'lucide-react';
import { toast } from 'sonner';

import * as api from '@/lib/api';
import type { ScanHistoryEntry, ScanPipelineOutcome, ScanPipelineTraceRow } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';

interface PipelineTracePanelProps {
  history: ScanHistoryEntry[];
  liveScanId?: number | null;
  liveScanActive?: boolean;
}

const PAGE_SIZE = 100;

const OUTCOMES: Array<{ value: 'all' | ScanPipelineOutcome; label: string }> = [
  { value: 'all', label: 'All outcomes' },
  { value: 'matched', label: 'Matched' },
  { value: 'provider_only', label: 'Provider only' },
  { value: 'unmatched', label: 'Unmatched' },
  { value: 'duplicate_winner', label: 'Duplicate winner' },
  { value: 'duplicate_loser', label: 'Duplicate loser' },
  { value: 'duplicate_candidate', label: 'Duplicate candidate' },
  { value: 'incomplete', label: 'Incomplete' },
  { value: 'moved_duplicate', label: 'Moved duplicate' },
  { value: 'moved_incomplete', label: 'Moved incomplete' },
  { value: 'restored_duplicate', label: 'Restored duplicate' },
  { value: 'restored_incomplete', label: 'Restored incomplete' },
];

const PROVIDERS: Array<{ value: 'all' | 'musicbrainz' | 'discogs' | 'lastfm' | 'bandcamp' | 'none'; label: string }> = [
  { value: 'all', label: 'All providers' },
  { value: 'musicbrainz', label: 'MusicBrainz' },
  { value: 'discogs', label: 'Discogs' },
  { value: 'lastfm', label: 'Last.fm' },
  { value: 'bandcamp', label: 'Bandcamp' },
  { value: 'none', label: 'No provider' },
];

function fmtDateTime(ts?: number | null): string {
  if (!ts) return '—';
  try {
    return format(new Date(ts * 1000), 'MM/dd/yyyy HH:mm');
  } catch {
    return '—';
  }
}

function statusTone(status: string): string {
  switch (status) {
    case 'matched':
      return 'bg-success/15 text-success border-success/40';
    case 'provider_only':
      return 'bg-info/15 text-info border-info/40';
    case 'unmatched':
      return 'bg-muted/15 text-muted-foreground border-border';
    case 'duplicate_winner':
    case 'restored_duplicate':
      return 'bg-primary/15 text-primary border-primary/40';
    case 'duplicate_loser':
    case 'moved_duplicate':
      return 'bg-warning/15 text-warning border-warning/40';
    case 'duplicate_candidate':
      return 'bg-warning/15 text-warning border-warning/40';
    case 'incomplete':
    case 'moved_incomplete':
    case 'restored_incomplete':
      return 'bg-destructive/15 text-destructive border-destructive/40';
    default:
      return 'bg-muted/40 text-muted-foreground border-border';
  }
}

function tinyBadge(label: string, active = false) {
  return (
    <Badge
      key={label}
      variant="outline"
      className={active ? 'border-info/50 bg-info/10 text-info' : 'border-border/70 bg-background/60 text-muted-foreground'}
    >
      {label}
    </Badge>
  );
}

function providerSet(row: ScanPipelineTraceRow) {
  return [
    tinyBadge('MB', row.providers.musicbrainz),
    tinyBadge('DG', row.providers.discogs),
    tinyBadge('LF', row.providers.lastfm),
    tinyBadge('BC', row.providers.bandcamp),
  ];
}

function qualityBadges(row: ScanPipelineTraceRow) {
  const out = [
    <Badge key="cover" variant="outline" className={row.has_cover ? 'border-success/40 bg-success/10 text-success' : 'border-destructive/40 bg-destructive/10 text-destructive'}>
      {row.has_cover ? 'Cover' : 'No cover'}
    </Badge>,
  ];
  if ((row.missing_required_tags || []).length > 0) {
    out.push(
      <Badge key="tags" variant="outline" className="border-warning/40 bg-warning/10 text-warning">
        {row.missing_required_tags.length} tag(s) missing
      </Badge>,
    );
  }
  if (row.is_broken) {
    out.push(
      <Badge key="broken" variant="outline" className="border-destructive/40 bg-destructive/10 text-destructive">
        Incomplete {row.actual_track_count || 0}/{row.expected_track_count || 0}
      </Badge>,
    );
  }
  if (row.ai_used) {
    out.push(
      <Badge key="ai" variant="outline" className="border-primary/40 bg-primary/10 text-primary">
        AI {row.ai_provider || row.ai_model || 'used'}
      </Badge>,
    );
  }
  return out;
}

function duplicateBadges(row: ScanPipelineTraceRow) {
  const out: JSX.Element[] = [];
  if (row.dupe_role && row.dupe_role !== 'none') {
    out.push(
      <Badge key="role" variant="outline" className="border-primary/40 bg-primary/10 text-primary">
        {row.dupe_role.replace(/_/g, ' ')}
      </Badge>,
    );
  }
  if (row.dupe_signal) {
    out.push(
      <Badge key="signal" variant="outline" className="border-border/70 bg-background/60 text-muted-foreground">
        {row.dupe_signal}
      </Badge>,
    );
  }
  if (row.move_reason && row.move_reason !== 'none' && row.move_status && row.move_status !== 'none') {
    out.push(
      <Badge key="move" variant="outline" className="border-success/40 bg-success/10 text-success">
        {row.move_status} {row.move_reason}
      </Badge>,
    );
  }
  if (row.manual_review) {
    out.push(
      <Badge key="review" variant="outline" className="border-warning/40 bg-warning/10 text-warning">
        Review
      </Badge>,
    );
  }
  return out;
}

export function PipelineTracePanel({ history, liveScanId, liveScanActive }: PipelineTracePanelProps) {
  const [selectedScanId, setSelectedScanId] = useState<number | null>(null);
  const [page, setPage] = useState(1);
  const [q, setQ] = useState('');
  const [provider, setProvider] = useState<'all' | 'musicbrainz' | 'discogs' | 'lastfm' | 'bandcamp' | 'none'>('all');
  const [outcome, setOutcome] = useState<'all' | ScanPipelineOutcome>('all');

  const scanChoices = useMemo(() => {
    return [...history]
      .filter((entry) => entry.entry_type === 'scan')
      .sort((a, b) => (Number(b.start_time || 0) - Number(a.start_time || 0)));
  }, [history]);

  useEffect(() => {
    if (selectedScanId && scanChoices.some((entry) => entry.scan_id === selectedScanId)) return;
    if (liveScanId && liveScanId > 0) {
      setSelectedScanId(liveScanId);
      return;
    }
    const fallback = scanChoices[0]?.scan_id ?? null;
    setSelectedScanId(fallback);
  }, [liveScanId, scanChoices, selectedScanId]);

  useEffect(() => {
    setPage(1);
  }, [selectedScanId, q, provider, outcome]);

  const selectedEntry = useMemo(
    () => scanChoices.find((entry) => entry.scan_id === selectedScanId) ?? null,
    [scanChoices, selectedScanId],
  );
  const isLiveSelected = Boolean(liveScanActive && liveScanId && selectedScanId === liveScanId);

  const traceQuery = useQuery({
    queryKey: ['scan-pipeline-trace', selectedScanId, page, q, provider, outcome],
    queryFn: () => api.getScanPipelineTrace(selectedScanId as number, { page, pageSize: PAGE_SIZE, q, provider, outcome }),
    enabled: (selectedScanId ?? 0) > 0,
    refetchInterval: isLiveSelected ? 3000 : 30000,
    refetchIntervalInBackground: true,
  });

  const rows = traceQuery.data?.items ?? [];
  const total = traceQuery.data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const summary = traceQuery.data?.summary;

  const onExport = async (format: 'csv' | 'json') => {
    if (!selectedScanId) return;
    try {
      await api.downloadScanPipelineTrace(selectedScanId, format, { q, provider, outcome });
    } catch (error) {
      toast.error(error instanceof Error ? error.message : `Export ${format.toUpperCase()} failed`);
    }
  };

  if (!selectedScanId) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Pipeline trace</CardTitle>
          <CardDescription>No scan available yet.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <CardTitle className="text-sm">Pipeline trace</CardTitle>
            <CardDescription>
              Folder-by-folder summary of match, duplicate, incomplete and move outcomes.
            </CardDescription>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Select value={String(selectedScanId)} onValueChange={(value) => setSelectedScanId(Number(value))}>
              <SelectTrigger className="w-[260px] bg-background/70">
                <SelectValue placeholder="Select scan" />
              </SelectTrigger>
              <SelectContent>
                {scanChoices.map((entry) => (
                  <SelectItem key={entry.scan_id} value={String(entry.scan_id)}>
                    #{entry.scan_id} · {entry.status} · {fmtDateTime(entry.start_time)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button variant="outline" size="sm" className="gap-2" onClick={() => traceQuery.refetch()}>
              {traceQuery.isFetching ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
              Refresh
            </Button>
            <Button variant="outline" size="sm" className="gap-2" onClick={() => onExport('csv')}>
              <Download className="h-4 w-4" />
              CSV
            </Button>
            <Button variant="outline" size="sm" className="gap-2" onClick={() => onExport('json')}>
              <Download className="h-4 w-4" />
              JSON
            </Button>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
            <Badge variant="outline">Scan #{selectedScanId}</Badge>
            {selectedEntry && <Badge variant="outline">Started {fmtDateTime(selectedEntry.start_time)}</Badge>}
            {isLiveSelected && <Badge variant="secondary">Live scan</Badge>}
          </div>
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
            <Card className="border-border/60 bg-card/60">
              <CardHeader className="pb-2"><CardTitle className="text-xs uppercase tracking-wide text-muted-foreground">Rows</CardTitle></CardHeader>
              <CardContent><p className="text-2xl font-semibold tabular-nums">{summary?.total ?? 0}</p></CardContent>
            </Card>
            <Card className="border-border/60 bg-card/60">
              <CardHeader className="pb-2"><CardTitle className="text-xs uppercase tracking-wide text-muted-foreground">Strict matches</CardTitle></CardHeader>
              <CardContent><p className="text-2xl font-semibold tabular-nums">{summary?.strict_matches ?? 0}</p></CardContent>
            </Card>
            <Card className="border-border/60 bg-card/60">
              <CardHeader className="pb-2"><CardTitle className="text-xs uppercase tracking-wide text-muted-foreground">Duplicates moved</CardTitle></CardHeader>
              <CardContent><p className="text-2xl font-semibold tabular-nums">{summary?.moved_duplicates ?? 0}</p></CardContent>
            </Card>
            <Card className="border-border/60 bg-card/60">
              <CardHeader className="pb-2"><CardTitle className="text-xs uppercase tracking-wide text-muted-foreground">Incompletes moved</CardTitle></CardHeader>
              <CardContent><p className="text-2xl font-semibold tabular-nums">{summary?.moved_incompletes ?? 0}</p></CardContent>
            </Card>
            <Card className="border-border/60 bg-card/60">
              <CardHeader className="pb-2"><CardTitle className="text-xs uppercase tracking-wide text-muted-foreground">AI touched</CardTitle></CardHeader>
              <CardContent><p className="text-2xl font-semibold tabular-nums">{summary?.ai_touched ?? 0}</p></CardContent>
            </Card>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <Input
              value={q}
              onChange={(event) => setQ(event.target.value)}
              placeholder="Search artist, album or folder..."
              className="w-full md:max-w-sm bg-background/70"
            />
            <Select value={provider} onValueChange={(value) => setProvider(value as typeof provider)}>
              <SelectTrigger className="w-[180px] bg-background/70"><SelectValue /></SelectTrigger>
              <SelectContent>
                {PROVIDERS.map((item) => <SelectItem key={item.value} value={item.value}>{item.label}</SelectItem>)}
              </SelectContent>
            </Select>
            <Select value={outcome} onValueChange={(value) => setOutcome(value as typeof outcome)}>
              <SelectTrigger className="w-[220px] bg-background/70"><SelectValue /></SelectTrigger>
              <SelectContent>
                {OUTCOMES.map((item) => <SelectItem key={item.value} value={item.value}>{item.label}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>

          <ScrollArea className="w-full whitespace-nowrap border border-border/70 bg-card/40">
            <Table className="min-w-[1540px]">
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="w-[360px]">Folder / album</TableHead>
                  <TableHead className="w-[240px]">Match</TableHead>
                  <TableHead className="w-[180px]">Providers</TableHead>
                  <TableHead className="w-[320px]">Quality</TableHead>
                  <TableHead className="w-[280px]">Duplicate / move</TableHead>
                  <TableHead className="w-[360px]">Timeline</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {traceQuery.isLoading ? (
                  <TableRow>
                    <TableCell colSpan={6} className="h-32 text-center text-muted-foreground">
                      <Loader2 className="mx-auto mb-2 h-5 w-5 animate-spin" />
                      Loading pipeline trace...
                    </TableCell>
                  </TableRow>
                ) : rows.length <= 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="h-28 text-center text-muted-foreground">
                      No trace row matches the current filters.
                    </TableCell>
                  </TableRow>
                ) : rows.map((row) => (
                  <TableRow key={`${row.scan_id}-${row.album_id}-${row.folder}`}>
                    <TableCell className="align-top">
                      <div className="space-y-1">
                        <div className="text-sm font-semibold leading-snug text-foreground">{row.album_title || 'Untitled album'}</div>
                        <div className="text-xs text-muted-foreground">{row.artist || 'Unknown artist'}</div>
                        <div className="text-[11px] text-muted-foreground break-all">{row.folder || '—'}</div>
                      </div>
                    </TableCell>
                    <TableCell className="align-top">
                      <div className="space-y-2">
                        <Badge variant="outline" className={statusTone(row.pipeline_status)}>{row.pipeline_status.replace(/_/g, ' ')}</Badge>
                        <div className="flex flex-wrap gap-1.5">
                          {row.strict_match_verified && (
                            <Badge variant="outline" className="border-success/40 bg-success/10 text-success">
                              Strict via {row.strict_match_provider || row.metadata_source || 'provider'}
                            </Badge>
                          )}
                          {!row.strict_match_verified && row.metadata_source && (
                            <Badge variant="outline" className="border-info/40 bg-info/10 text-info">
                              {row.metadata_source}
                            </Badge>
                          )}
                          {row.strict_tracklist_score ? (
                            <Badge variant="outline" className="border-border/70 bg-background/60 text-muted-foreground">
                              score {row.strict_tracklist_score.toFixed(2)}
                            </Badge>
                          ) : null}
                        </div>
                        {row.strict_reject_reason ? (
                          <p className="text-[11px] text-warning">{row.strict_reject_reason}</p>
                        ) : null}
                      </div>
                    </TableCell>
                    <TableCell className="align-top">
                      <div className="flex flex-wrap gap-1.5">{providerSet(row)}</div>
                    </TableCell>
                    <TableCell className="align-top">
                      <div className="flex flex-wrap gap-1.5">{qualityBadges(row)}</div>
                    </TableCell>
                    <TableCell className="align-top">
                      <div className="flex flex-wrap gap-1.5">{duplicateBadges(row)}</div>
                      {row.winner_title ? <p className="mt-2 text-[11px] text-muted-foreground">winner: {row.winner_title}</p> : null}
                      {row.decision_reason ? <p className="mt-1 text-[11px] text-muted-foreground">reason: {row.decision_reason}</p> : null}
                    </TableCell>
                    <TableCell className="align-top">
                      <div className="flex flex-wrap gap-1.5">
                        {(row.timeline || []).slice(0, 6).map((event, index) => (
                          <Badge key={`${row.album_id}-${event.stage}-${index}`} variant="outline" className="border-border/70 bg-background/60 text-muted-foreground">
                            {event.label}
                          </Badge>
                        ))}
                      </div>
                      <p className="mt-2 text-[11px] text-muted-foreground">updated {fmtDateTime(row.updated_at)}</p>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </ScrollArea>

          <div className="flex flex-wrap items-center justify-between gap-2 text-sm">
            <p className="text-muted-foreground">
              Page {page} / {totalPages} · {total.toLocaleString()} row(s)
            </p>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" disabled={page <= 1} onClick={() => setPage((current) => Math.max(1, current - 1))}>
                Previous
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={page >= totalPages}
                onClick={() => setPage((current) => Math.min(totalPages, current + 1))}
              >
                Next
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
