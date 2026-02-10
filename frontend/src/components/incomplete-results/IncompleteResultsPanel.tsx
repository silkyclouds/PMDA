import { useState, useCallback } from 'react';
import { Loader2, FolderInput, Download, Music } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Checkbox } from '@/components/ui/checkbox';
import * as api from '@/lib/api';
import { toast } from 'sonner';

export function IncompleteResultsPanel() {
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<{ run_id: number; items: api.IncompleteAlbumItem[] } | null>(null);
  const [moving, setMoving] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const loadResults = useCallback(async () => {
    try {
      setLoading(true);
      const res = await api.getIncompleteAlbumsResults();
      if (res.run_id != null && res.items.length > 0) {
        setData({ run_id: res.run_id, items: res.items });
        setSelected(new Set());
      } else {
        setData(null);
        toast.info('No incomplete scan results found. Run an "Incomplete albums only" scan first.');
      }
    } catch {
      toast.error('Failed to load incomplete scan results');
    } finally {
      setLoading(false);
    }
  }, []);

  const toggle = (key: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const selectAll = () => {
    if (!data) return;
    if (selected.size === data.items.length) setSelected(new Set());
    else setSelected(new Set(data.items.map((i) => `${i.artist}|${i.album_id}`)));
  };

  const handleMove = async () => {
    if (!data || selected.size === 0) return;
    const items = data.items.filter((i) => selected.has(`${i.artist}|${i.album_id}`));
    try {
      setMoving(true);
      const res = await api.moveIncompleteAlbums(data.run_id, items.map((i) => ({ artist: i.artist, album_id: i.album_id, title_raw: i.title_raw })));
      toast.success(`Moved ${res.moved.length} album(s) to quarantine`);
      setData((prev) => prev ? { ...prev, items: prev.items.filter((i) => !selected.has(`${i.artist}|${i.album_id}`)) } : null);
      setSelected(new Set());
    } catch {
      toast.error('Failed to move albums');
    } finally {
      setMoving(false);
    }
  };

  const handleExport = (format: 'json' | 'csv') => {
    if (!data) return;
    const url = api.getIncompleteAlbumsExportUrl(data.run_id, format);
    window.open(url, '_blank');
  };

  if (!data && !loading) {
    return (
      <div className="rounded-lg border border-border bg-card p-4">
        <p className="text-sm text-muted-foreground mb-3">View and manage results from an &quot;Incomplete albums only&quot; scan.</p>
        <Button variant="secondary" size="sm" onClick={loadResults} disabled={loading} className="gap-2">
          {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Music className="w-4 h-4" />}
          Load incomplete scan results
        </Button>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="rounded-lg border border-border bg-card p-6 flex items-center justify-center gap-2 text-muted-foreground">
        <Loader2 className="w-5 h-5 animate-spin" /> Loading…
      </div>
    );
  }

  if (!data || data.items.length === 0) return null;

  const classificationList = (c: string) => c ? c.split(',').map((x) => x.trim()).filter(Boolean) : [];

  return (
    <div className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex flex-wrap items-center justify-between gap-2">
        <h3 className="font-semibold text-foreground">Incomplete albums (run {data.run_id})</h3>
        <div className="flex items-center gap-2">
          <Button size="sm" variant="secondary" onClick={handleMove} disabled={selected.size === 0 || moving} className="gap-1.5">
            {moving ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <FolderInput className="w-3.5 h-3.5" />}
            Move selected ({selected.size})
          </Button>
          <Button size="sm" variant="outline" onClick={() => handleExport('json')} className="gap-1.5">
            <Download className="w-3.5 h-3.5" /> JSON
          </Button>
          <Button size="sm" variant="outline" onClick={() => handleExport('csv')} className="gap-1.5">
            <Download className="w-3.5 h-3.5" /> CSV
          </Button>
          <Button size="sm" variant="ghost" onClick={loadResults}>Refresh</Button>
        </div>
      </div>
      <div className="overflow-x-auto max-h-[360px] overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-muted/80 border-b border-border">
            <tr>
              <th className="text-left p-2 w-10">
                <Checkbox checked={selected.size === data.items.length} onCheckedChange={selectAll} aria-label="Select all" />
              </th>
              <th className="text-left p-2 font-medium">Artist</th>
              <th className="text-left p-2 font-medium">Album</th>
              <th className="text-left p-2 font-medium">Classification</th>
              <th className="text-left p-2 font-medium text-muted-foreground">Tracks</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((item) => {
              const key = `${item.artist}|${item.album_id}`;
              const classes = classificationList(item.classification);
              return (
                <tr key={key} className="border-b border-border/50 hover:bg-muted/30">
                  <td className="p-2">
                    <Checkbox checked={selected.has(key)} onCheckedChange={() => toggle(key)} aria-label={`Select ${item.title_raw}`} />
                  </td>
                  <td className="p-2">{item.artist}</td>
                  <td className="p-2">{item.title_raw || `Album ${item.album_id}`}</td>
                  <td className="p-2">
                    <div className="flex flex-wrap gap-1">
                      {classes.map((c) => (
                        <Badge key={c} variant="secondary" className="text-xs">{c}</Badge>
                      ))}
                    </div>
                  </td>
                  <td className="p-2 text-muted-foreground">{item.actual_track_count} / {item.expected_track_count}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
