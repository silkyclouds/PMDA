import { useState, useEffect } from 'react';
import { Edit, Loader2, Save, Database, Music, Search, Sparkles } from 'lucide-react';
import { ImproveAlbumDialog } from './ImproveAlbumDialog';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { AspectRatio } from '@/components/ui/aspect-ratio';
import { useToast } from '@/hooks/use-toast';

interface AlbumEditorProps {
  albumId: number;
  albumTitle: string;
  artistName: string;
  /** Plex thumbnail URL for album cover (from Library browser). */
  albumThumb?: string | null;
  /** Primary audio format (e.g. MP3, FLAC). */
  format?: string;
  /** True if album could be improved (lossy, missing cover, or broken). */
  canImprove?: boolean;
  /** Reasons why this album can be improved (for display when canImprove is true). */
  improveReasons?: string[];
  /** When album is broken: expected/actual track count and missing indices. */
  brokenDetail?: {
    expected_track_count: number;
    actual_track_count: number;
    missing_indices: number[];
  };
  onClose: () => void;
}

export function AlbumEditor({ albumId, albumTitle, artistName, albumThumb, format, canImprove, improveReasons, brokenDetail, onClose }: AlbumEditorProps) {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [albumData, setAlbumData] = useState<any>(null);
  const [tags, setTags] = useState<Record<string, string>>({});
  const [coverError, setCoverError] = useState(false);
  const [queryingMb, setQueryingMb] = useState(false);
  const [suggestedTags, setSuggestedTags] = useState<Record<string, string> | null>(null);
  const [showImproveDialog, setShowImproveDialog] = useState(false);

  const updateTag = (key: string, value: string) => {
    setTags(prev => ({ ...prev, [key]: value }));
  };
  const { toast } = useToast();

  useEffect(() => {
    loadAlbumTags();
  }, [albumId]);

  const loadAlbumTags = async () => {
    try {
      setLoading(true);
      const response = await fetch(`/api/library/album/${albumId}/tags`);
      if (response.ok) {
        const data = await response.json();
        setAlbumData(data);
        setTags(data.current_tags || {});
      } else {
        throw new Error('Failed to load album tags');
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: 'Failed to load album tags',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  const handleQueryMusicBrainz = async () => {
    setQueryingMb(true);
    setSuggestedTags(null);
    try {
      const response = await fetch('/api/musicbrainz/suggest-album-tags', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ album_id: albumId }),
      });
      const data = await response.json();
      if (data.success && data.suggested_tags && Object.keys(data.suggested_tags).length > 0) {
        setSuggestedTags(data.suggested_tags);
        toast({ title: 'MusicBrainz', description: data.message ?? 'Suggested tags loaded.' });
      } else {
        toast({ title: 'MusicBrainz', description: data.message ?? 'No match found.', variant: 'destructive' });
      }
    } catch (error) {
      toast({ title: 'Error', description: 'Failed to query MusicBrainz', variant: 'destructive' });
    } finally {
      setQueryingMb(false);
    }
  };

  const handleApplySuggested = () => {
    if (suggestedTags) {
      setTags(prev => ({ ...prev, ...suggestedTags }));
      setSuggestedTags(null);
      toast({ title: 'Applied', description: 'Suggested tags applied. Click Apply Tags to save to files.' });
    }
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const response = await fetch('/api/musicbrainz/fix-album-tags', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          album_id: albumId,
          tags: tags,
        }),
      });
      
      if (response.ok) {
        const result = await response.json();
        toast({
          title: 'Info',
          description: result.message,
        });
      } else {
        throw new Error('Failed to save tags');
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: 'Failed to save tags',
        variant: 'destructive',
      });
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="flex items-center justify-center">
            <Loader2 className="w-6 h-6 animate-spin text-primary" />
          </div>
        </CardContent>
      </Card>
    );
  }

  if (!albumData) {
    return (
      <Card>
        <CardContent className="p-6 text-center text-muted-foreground">
          Album data not available
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="overflow-hidden">
      <div className="flex flex-col sm:flex-row gap-4 p-4">
        {/* Album cover: compact, no empty frame */}
        <div className="shrink-0 w-24 sm:w-28">
          <AspectRatio ratio={1} className="rounded-lg overflow-hidden bg-muted relative">
            {/* Placeholder behind so cover can sit on top */}
            <div className="absolute inset-0 z-0 flex items-center justify-center bg-muted">
              <Music className="w-10 h-10 text-muted-foreground" />
            </div>
            {(albumThumb ?? albumData?.thumb_url) && !coverError && (
              <img
                src={albumThumb ?? albumData?.thumb_url ?? ''}
                alt={albumTitle}
                className="absolute inset-0 z-10 w-full h-full object-cover"
                onError={() => setCoverError(true)}
              />
            )}
          </AspectRatio>
        </div>
        <div className="flex-1 min-w-0">
          <CardHeader className="p-0 pb-2">
            <div className="flex items-center justify-between gap-2 flex-wrap">
              <div>
                <CardTitle className="flex items-center gap-2 text-lg">
                  <Edit className="w-4 h-4" />
                  Edit Album Tags
                </CardTitle>
                {(format || canImprove || brokenDetail) && (
                  <p className="flex flex-wrap items-center gap-x-2 gap-y-1 mt-1 text-sm text-muted-foreground">
                    {format && <span>Format: {format}</span>}
                    {format && (canImprove || brokenDetail) && <span>•</span>}
                    {canImprove && !brokenDetail && (
                      <span>
                        {improveReasons && improveReasons.length > 0
                          ? `Improve: ${improveReasons.join(' · ')}`
                          : 'Can be improved (lossy, missing cover, or incomplete)'}
                      </span>
                    )}
                    {brokenDetail && (
                      <span className="text-destructive/90">
                        Broken: expected {brokenDetail.expected_track_count} tracks, have {brokenDetail.actual_track_count}
                        {brokenDetail.missing_indices?.length ? ` · Missing track #${brokenDetail.missing_indices.join(', #')}` : ''}
                      </span>
                    )}
                  </p>
                )}
                <CardDescription>
                  {albumTitle} by {artistName}
                </CardDescription>
              </div>
              {albumData.musicbrainz_id && (
                <Badge variant="outline" className="gap-1.5">
                  <Database className="w-3 h-3" />
                  {albumData.musicbrainz_id.slice(0, 8)}...
                </Badge>
              )}
            </div>
          </CardHeader>
        </div>
      </div>
      <CardContent className="space-y-4 pt-0">
        {albumData.musicbrainz_info && (
          <div className="p-3 rounded-lg bg-primary/5 border border-primary/20">
            <div className="text-sm font-medium mb-2">MusicBrainz Info:</div>
            <div className="text-xs text-muted-foreground space-y-1">
              <div>Type: {(albumData.musicbrainz_info as any).primary_type || 'Unknown'}</div>
              {(albumData.musicbrainz_info as any).format_summary && (
                <div>Formats: {(albumData.musicbrainz_info as any).format_summary}</div>
              )}
            </div>
          </div>
        )}

        <div className="flex flex-wrap items-center gap-2">
          <Button variant="default" size="sm" onClick={() => setShowImproveDialog(true)} className="gap-1.5">
            <Sparkles className="w-4 h-4" />
            Improve this album
          </Button>
          <Button variant="outline" size="sm" onClick={handleQueryMusicBrainz} disabled={queryingMb} className="gap-1.5">
            {queryingMb ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
            Query MusicBrainz
          </Button>
          {suggestedTags && (
            <Button variant="secondary" size="sm" onClick={handleApplySuggested} className="gap-1.5">
              Apply suggested tags
            </Button>
          )}
        </div>

        {suggestedTags && (
          <div className="p-3 rounded-lg bg-muted/50 border text-xs space-y-1">
            <div className="font-medium">Suggested from MusicBrainz:</div>
            <pre className="whitespace-pre-wrap break-all text-muted-foreground">{JSON.stringify(suggestedTags, null, 2)}</pre>
          </div>
        )}

        <div className="space-y-3">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div>
              <Label>Artist</Label>
              <Input
                value={tags['artist'] || tags['albumartist'] || ''}
                onChange={(e) => updateTag('artist', e.target.value)}
                placeholder="Artist name"
              />
            </div>
            <div>
              <Label>Album Artist</Label>
              <Input
                value={tags['albumartist'] || tags['artist'] || ''}
                onChange={(e) => updateTag('albumartist', e.target.value)}
                placeholder="Album artist"
              />
            </div>
          </div>
          <div>
            <Label>Album</Label>
            <Input
              value={tags['album'] || ''}
              onChange={(e) => updateTag('album', e.target.value)}
              placeholder="Album title"
            />
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div>
              <Label>Year / Date</Label>
              <Input
                value={tags['date'] || tags['originaldate'] || ''}
                onChange={(e) => updateTag('date', e.target.value)}
                placeholder="Year"
              />
            </div>
            <div>
              <Label>Original Date</Label>
              <Input
                value={tags['originaldate'] || tags['date'] || ''}
                onChange={(e) => updateTag('originaldate', e.target.value)}
                placeholder="Original date"
              />
            </div>
          </div>
          <div>
            <Label>Genre</Label>
            <Input
              value={tags['genre'] || ''}
              onChange={(e) => updateTag('genre', e.target.value)}
              placeholder="Genre"
            />
          </div>
          <div>
            <Label>Comment</Label>
            <Input
              value={tags['comment'] || ''}
              onChange={(e) => updateTag('comment', e.target.value)}
              placeholder="Comment"
            />
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2 pt-4">
          <Button onClick={handleSave} disabled={saving} className="gap-1.5">
            {saving ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Save className="w-4 h-4" />
            )}
            Apply Tags
          </Button>
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
        </div>
      </CardContent>
      <ImproveAlbumDialog
        open={showImproveDialog}
        onOpenChange={setShowImproveDialog}
        albumId={albumId}
        albumTitle={albumTitle}
        onSuccess={() => loadAlbumTags()}
      />
    </Card>
  );
}
