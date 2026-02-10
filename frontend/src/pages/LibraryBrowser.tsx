import { useState, useEffect, useCallback, useRef } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { Search, Music, Star, Loader2, Edit, Image as ImageIcon, RefreshCw, LayoutGrid, List, Sparkles, Play, Check, X, Circle, CopyMinus, Wrench, FolderInput } from 'lucide-react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Header } from '@/components/Header';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { SimilarArtists } from '@/components/library/SimilarArtists';
import { AlbumEditor } from '@/components/library/AlbumEditor';
import { ImproveAlbumDialog } from '@/components/library/ImproveAlbumDialog';
import { type TrackInfo } from '@/components/library/AudioPlayer';
import { usePlayback } from '@/contexts/PlaybackContext';
import { FormatBadge } from '@/components/FormatBadge';
import { useToast } from '@/hooks/use-toast';
import { cn } from '@/lib/utils';
import * as api from '@/lib/api';
import { AspectRatio } from '@/components/ui/aspect-ratio';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';

interface ArtistInfo {
  artist_id: number;
  artist_name: string;
  album_count: number;
  broken_albums_count: number;
  artist_thumb?: string | null;
}

interface AlbumInfo {
  album_id: number;
  title: string;
  year?: number;
  date?: string;
  track_count: number;
  is_broken: boolean;
  thumb?: string;
  type: string;
  /** Primary audio format (e.g. MP3, FLAC) */
  format?: string;
  /** True if format is lossless (FLAC, etc.) */
  is_lossless?: boolean;
  /** True if album could be improved (lossy format, missing cover, or broken) */
  can_improve?: boolean;
  /** True when Plex DB has no thumb (we still send thumb URL for display) */
  thumb_empty?: boolean;
  /** True when album is identified via MusicBrainz (cache source or MBID) */
  mb_identified?: boolean;
  /** MusicBrainz release-group ID when available (for link to MusicBrainz) */
  musicbrainz_release_group_id?: string;
  /** True when album is in a duplicate group (from scan) */
  in_duplicate_group?: boolean;
  /** When is_broken: expected/actual track count and missing indices */
  broken_detail?: {
    expected_track_count: number;
    actual_track_count: number;
    missing_indices: number[];
  };
}

interface ArtistDetails {
  artist_id: number;
  artist_name: string;
  artist_thumb?: string;
  albums: AlbumInfo[];
  total_albums: number;
  /** True when this data came from artist analysis cache (after POST analyze) */
  analysis_cached?: boolean;
  analyzed_at?: number;
  stats?: {
    duplicates: number;
    no_cover: number;
    mb_identified: number;
    broken: number;
  };
}

/** Reasons why an album "can be improved" for the popover. Never show "Missing cover art" when a thumb URL is present. */
function getCanImproveReasons(album: AlbumInfo): string[] {
  const reasons: string[] = [];
  if (!album.is_lossless && album.format) {
    reasons.push(`Lossy format (${album.format}) – upgrade to FLAC possible`);
  }
  const hasThumb = album.thumb != null && String(album.thumb).trim() !== '';
  if (!hasThumb) {
    reasons.push('Missing cover art');
  }
  if (album.mb_identified === false) {
    reasons.push('Not identified in MusicBrainz – can fetch tags and cover');
  }
  if (album.is_broken) {
    reasons.push('Incomplete – missing tracks (recover via Lidarr/Autobrr)');
  }
  return reasons;
}

/** True when album has at least one issue (no cover, no MBID, duplicate, incomplete) – show Fix control. */
function albumHasIssue(album: AlbumInfo): boolean {
  const hasThumb = album.thumb != null && String(album.thumb).trim() !== '';
  if (album.thumb_empty && !hasThumb) return true;
  if (album.mb_identified === false) return true;
  if (album.in_duplicate_group) return true;
  if (album.is_broken) return true;
  return false;
}

/** Button and dialog to list albums with parenthetical folder names (e.g. "Album (flac)") and rename them to "Album". */
function NormalizeAlbumNamesButton({ onDone }: { onDone?: () => void }) {
  const [open, setOpen] = useState(false);
  const [albums, setAlbums] = useState<api.AlbumWithParentheticalName[]>([]);
  const [loading, setLoading] = useState(false);
  const [renaming, setRenaming] = useState(false);
  const { toast } = useToast();

  const handleOpen = useCallback(async () => {
    setOpen(true);
    setLoading(true);
    setAlbums([]);
    try {
      const res = await api.getAlbumsWithParentheticalNames();
      setAlbums(res.albums ?? []);
    } catch {
      toast({ title: 'Error', description: 'Failed to load albums with parenthetical names', variant: 'destructive' });
    } finally {
      setLoading(false);
    }
  }, [toast]);

  const handleRenameAll = useCallback(async () => {
    if (albums.length === 0) return;
    setRenaming(true);
    try {
      const result = await api.normalizeAlbumNames(albums.map((a) => a.album_id));
      const n = result.renamed?.length ?? 0;
      const errs = result.errors?.length ?? 0;
      if (n > 0) {
        toast({ title: 'Renamed', description: `${n} folder${n !== 1 ? 's' : ''} renamed.${errs > 0 ? ` ${errs} error(s).` : ''} Re-scan your library in Plex to refresh.` });
        setOpen(false);
        onDone?.();
      }
      if (errs > 0 && n === 0) {
        toast({ title: 'Errors', description: result.errors?.map((e) => e.message).join('; ') ?? 'Rename failed', variant: 'destructive' });
      }
    } catch (e) {
      toast({ title: 'Error', description: e instanceof Error ? e.message : 'Rename failed', variant: 'destructive' });
    } finally {
      setRenaming(false);
    }
  }, [albums, onDone, toast]);

  return (
    <>
      <Button variant="outline" size="sm" onClick={handleOpen} className="gap-1.5">
        <FolderInput className="w-4 h-4" />
        Normalize album names
      </Button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-w-2xl max-h-[85vh] flex flex-col">
          <DialogHeader>
            <DialogTitle>Normalize album names</DialogTitle>
            <DialogDescription>
              Remove format/version suffixes in parentheses from folder names (e.g. &quot;Album (flac)&quot; → &quot;Album&quot;). Re-scan your library in Plex after renaming.
            </DialogDescription>
          </DialogHeader>
          {loading ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
            </div>
          ) : albums.length === 0 ? (
            <p className="text-sm text-muted-foreground py-4">No albums with parenthetical suffixes found.</p>
          ) : (
            <>
              <ScrollArea className="flex-1 max-h-[50vh] rounded-md border p-3">
                <ul className="space-y-2 text-sm">
                  {albums.map((a) => (
                    <li key={a.album_id} className="flex items-center gap-2 flex-wrap">
                      <span className="font-medium truncate">{a.artist}</span>
                      <span className="text-muted-foreground">—</span>
                      <span className="truncate" title={a.current_name}>{a.current_name}</span>
                      <span className="text-muted-foreground">→</span>
                      <span className="truncate text-primary" title={a.proposed_name}>{a.proposed_name}</span>
                    </li>
                  ))}
                </ul>
              </ScrollArea>
              <div className="flex justify-end gap-2 pt-2">
                <Button variant="outline" onClick={() => setOpen(false)}>Cancel</Button>
                <Button onClick={handleRenameAll} disabled={renaming} className="gap-2">
                  {renaming ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
                  Rename {albums.length} folder{albums.length !== 1 ? 's' : ''}
                </Button>
              </div>
            </>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
}

/** Issue badges with distinct colors for list/tile views. */
function IssueBadges({ album, className }: { album: AlbumInfo; className?: string }) {
  const hasThumb = album.thumb != null && String(album.thumb).trim() !== '';
  const noCover = album.thumb_empty && !hasThumb;
  return (
    <div className={cn('flex flex-wrap items-center gap-1', className)}>
      {noCover && (
        <Badge className="text-xs bg-slate-500/20 text-slate-700 dark:bg-slate-500/30 dark:text-slate-300 border-slate-500/30">
          No cover
        </Badge>
      )}
      {album.musicbrainz_release_group_id ? (
        <a
          href={`https://musicbrainz.org/release-group/${album.musicbrainz_release_group_id}`}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex"
          onClick={(e) => e.stopPropagation()}
        >
          <Badge className="text-xs bg-emerald-500/20 text-emerald-700 dark:bg-emerald-500/30 dark:text-emerald-300 border-emerald-500/30 hover:underline cursor-pointer">
            MBID
          </Badge>
        </a>
      ) : album.mb_identified === false && (
        <Badge className="text-xs bg-amber-500/20 text-amber-700 dark:bg-amber-500/30 dark:text-amber-300 border-amber-500/30">
          No MBID
        </Badge>
      )}
      {album.in_duplicate_group && (
        <Badge className="text-xs border-blue-500/50 text-blue-700 dark:text-blue-300 bg-transparent">
          Duplicate
        </Badge>
      )}
      {album.is_broken && (
        <Badge variant="destructive" className="text-xs" title={album.broken_detail ? `Expected ${album.broken_detail.expected_track_count} tracks, have ${album.broken_detail.actual_track_count}. Missing: ${album.broken_detail.missing_indices?.join(', ') || '—'}` : undefined}>
          Incomplete
        </Badge>
      )}
    </div>
  );
}

// Debounce hook
function useDebounce<T>(value: T, delay: number): T {
  const [debouncedValue, setDebouncedValue] = useState<T>(value);

  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedValue(value);
    }, delay);

    return () => {
      clearTimeout(handler);
    };
  }, [value, delay]);

  return debouncedValue;
}

export default function LibraryBrowser() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [artists, setArtists] = useState<ArtistInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState('');
  const debouncedSearch = useDebounce(searchQuery, 120);
  const [selectedArtist, setSelectedArtist] = useState<number | null>(null);
  const [artistDetails, setArtistDetails] = useState<ArtistDetails | null>(null);
  const [loadingDetails, setLoadingDetails] = useState(false);
  const [detailsLoadError, setDetailsLoadError] = useState<string | null>(null);
  const [monitoredArtists, setMonitoredArtists] = useState<Set<number>>(new Set());
  const [addingToLidarr, setAddingToLidarr] = useState(false);
  const [editingAlbum, setEditingAlbum] = useState<number | null>(null);
  const [albumViewMode, setAlbumViewMode] = useState<'tile' | 'list'>(() => (localStorage.getItem('pmda-library-album-view') as 'tile' | 'list') || 'tile');
  const [improvingAlbums, setImprovingAlbums] = useState(false);
  const [dedupingArtist, setDedupingArtist] = useState(false);
  const [showImproveAllModal, setShowImproveAllModal] = useState(false);
  const [improveDialogAlbum, setImproveDialogAlbum] = useState<{ albumId: number; albumTitle: string } | null>(null);
  type ImproveStep = { label: string; success: boolean | null };
  type ImproveAlbumLogEntry = { album_id: number; title: string; steps: ImproveStep[]; provider_hits?: string[] };
  const [improveAllProgress, setImproveAllProgress] = useState<{
    running: boolean;
    finished?: boolean;
    current_provider?: string | null;
    provider_status?: Record<string, 'ok' | 'fail' | 'pending'>;
    current_album?: string | null;
    current_album_id?: number | null;
    albums_processed?: number;
    total_albums?: number;
    current_steps?: ImproveStep[];
    album_log?: ImproveAlbumLogEntry[];
    result?: {
      message: string;
      albums_processed: number;
      albums_improved: number;
      by_provider: Record<string, { identified: number; covers: number; tags: number }>;
      album_log: ImproveAlbumLogEntry[];
    };
    error?: string | null;
  } | null>(null);
  const [improveAllResult, setImproveAllResult] = useState<{
    message: string;
    albums_processed: number;
    albums_improved: number;
    covers_downloaded: number;
    tags_updated: number;
    by_provider: Record<string, { identified: number; covers: number; tags: number }>;
    album_log: ImproveAlbumLogEntry[];
  } | null>(null);
  const improveAllPollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [totalArtists, setTotalArtists] = useState(0);
  const { startPlayback, setCurrentTrack, recommendationSessionId, session } = usePlayback();
  const [recommendations, setRecommendations] = useState<api.RecoTrack[]>([]);
  const [loadingRecommendations, setLoadingRecommendations] = useState(false);
  const [recommendationsError, setRecommendationsError] = useState<string | null>(null);
  const artistsListRef = useRef<HTMLDivElement>(null);
  const { toast } = useToast();
  const [showAnalyzeChoiceModal, setShowAnalyzeChoiceModal] = useState(false);
  const [analyzeProgress, setAnalyzeProgress] = useState<{
    running: boolean;
    current_album_index?: number;
    total_albums?: number;
    current_album_title?: string | null;
    step?: string | null;
    finished?: boolean;
    error?: string | null;
  } | null>(null);
  const analyzePollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const rowVirtualizer = useVirtualizer({
    count: artists.length,
    getScrollElement: () => artistsListRef.current,
    estimateSize: () => 96,
    overscan: 5,
  });

  const loadArtists = useCallback(async (search: string = '') => {
    try {
      setLoading(true);
      const q = (search || '').trim();
      const response = await fetch(
        q
          ? `/api/library/artists/suggest?q=${encodeURIComponent(q)}&limit=100`
          : '/api/library/artists?limit=100&offset=0'
      );
      if (!response.ok) throw new Error('Failed to load artists');
      const data = await response.json();
      setArtists(data.artists || []);
      setTotalArtists((typeof data.total === 'number' ? data.total : (data.artists || []).length) || 0);
    } catch (error) {
      toast({
        title: 'Error',
        description: 'Failed to load artists',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    loadArtists(debouncedSearch);
  }, [debouncedSearch, loadArtists]);

  const loadRecommendations = useCallback(async () => {
    if (!recommendationSessionId) return;
    try {
      setLoadingRecommendations(true);
      setRecommendationsError(null);
      const excludeTrackId = session?.currentTrack?.track_id;
      const data = await api.getRecommendationsForYou(recommendationSessionId, 12, excludeTrackId);
      setRecommendations(Array.isArray(data.tracks) ? data.tracks : []);
    } catch {
      setRecommendations([]);
      setRecommendationsError('Failed to load recommendations');
    } finally {
      setLoadingRecommendations(false);
    }
  }, [recommendationSessionId, session?.currentTrack?.track_id]);

  useEffect(() => {
    loadRecommendations();
  }, [loadRecommendations]);

  const loadArtistDetails = async (artistId: number) => {
    setDetailsLoadError(null);
    const maxAttempts = 3;
    const retryDelayMs = 1000;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        setLoadingDetails(true);
        const response = await fetch(`/api/library/artist/${artistId}`);
        if (!response.ok) throw new Error('Failed to load artist details');
        const data = await response.json();
        setArtistDetails(data);
        setDetailsLoadError(null);
        if (data.analysis_cached === false) {
          setShowAnalyzeChoiceModal(true);
        }
        return;
      } catch (error) {
        const isNetworkError =
          error instanceof TypeError ||
          (error instanceof Error && (
            /fetch|network|suspended|failed to fetch/i.test(error.message)
          ));
        const lastAttempt = attempt === maxAttempts;
        if (lastAttempt || !isNetworkError) {
          setDetailsLoadError(error instanceof Error ? error.message : 'Failed to load artist details');
          // Don't clear artist details when refetch fails for the same artist we're viewing
          // (e.g. during "Improve All Albums" the server may be busy and refetch can fail)
          setArtistDetails((prev) => {
            if (prev && prev.artist_id === artistId) return prev;
            return null;
          });
          toast({
            title: 'Error',
            description: lastAttempt && isNetworkError
              ? 'Network issue. Check connection and try again.'
              : 'Failed to load artist details',
            variant: 'destructive',
          });
          return;
        }
        await new Promise((r) => setTimeout(r, retryDelayMs));
      } finally {
        setLoadingDetails(false);
      }
    }
  };

  const handleArtistClick = (artistId: number) => {
    setSelectedArtist(artistId);
    loadArtistDetails(artistId);
    checkMonitoredStatus(artistId);
  };

  useEffect(() => {
    const artistParamRaw = searchParams.get('artist');
    const artistParam = artistParamRaw ? Number(artistParamRaw) : 0;
    if (!artistParam || !Number.isFinite(artistParam) || artistParam <= 0) return;
    if (selectedArtist === artistParam) return;
    handleArtistClick(artistParam);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams, selectedArtist]);

  const checkMonitoredStatus = async (artistId: number) => {
    try {
      const response = await fetch(`/api/library/artist/${artistId}/monitored`);
      if (response.ok) {
        const data = await response.json();
        if (data.monitored) {
          setMonitoredArtists(prev => new Set(prev).add(artistId));
        }
      }
    } catch (error) {
      // Silently fail - not critical
    }
  };

  const handleAddToLidarr = async (artistId: number, artistName: string) => {
    setAddingToLidarr(true);
    try {
      const response = await fetch('/api/lidarr/add-artist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          artist_id: artistId,
          artist_name: artistName,
        }),
      });
      
      if (response.ok) {
        const result = await response.json();
        if (result.success) {
          toast({
            title: 'Success',
            description: result.message,
          });
          setMonitoredArtists(prev => new Set(prev).add(artistId));
        } else {
          throw new Error(result.message);
        }
      } else {
        const error = await response.json();
        throw new Error(error.message || 'Failed to add artist to Lidarr');
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to add artist to Lidarr',
        variant: 'destructive',
      });
    } finally {
      setAddingToLidarr(false);
    }
  };

  const handlePlayAlbum = async (albumId: number) => {
    try {
      const response = await fetch(`/api/library/album/${albumId}/tracks`);
      if (!response.ok) throw new Error('Failed to load tracks');
      const data = await response.json();
      const tracksList: TrackInfo[] = data.tracks || [];
      if (tracksList.length === 0) {
        toast({ title: 'No tracks', description: 'This album has no playable tracks.', variant: 'destructive' });
        return;
      }
      const albumTitle = artistDetails?.albums?.find((a) => a.album_id === albumId)?.title ?? 'Album';
      const albumThumb = data.album_thumb || artistDetails?.albums?.find((a) => a.album_id === albumId)?.thumb || null;
      startPlayback(albumId, albumTitle, albumThumb, tracksList);
    } catch (error) {
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to load tracks',
        variant: 'destructive',
      });
    }
  };

  const handlePlayRecommendedTrack = async (rec: api.RecoTrack) => {
    try {
      const response = await fetch(`/api/library/album/${rec.album_id}/tracks`);
      if (!response.ok) throw new Error('Failed to load tracks');
      const data = await response.json();
      const tracksList: TrackInfo[] = data.tracks || [];
      if (!tracksList.length) {
        toast({ title: 'No tracks', description: 'This recommendation has no playable tracks.', variant: 'destructive' });
        return;
      }
      startPlayback(rec.album_id, rec.album_title || 'Album', rec.thumb || null, tracksList);
      const target = tracksList.find((t) => t.track_id === rec.track_id);
      if (target) {
        setCurrentTrack(target);
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to play recommendation',
        variant: 'destructive',
      });
    }
  };

  const fetchAnalyzeProgress = useCallback(async () => {
    if (!selectedArtist) return;
    try {
      const res = await fetch(`/api/library/artist/${selectedArtist}/analyze/progress`);
      const data = await res.json();
      setAnalyzeProgress(data);
      if (data.finished || data.error) {
        if (analyzePollRef.current) {
          clearInterval(analyzePollRef.current);
          analyzePollRef.current = null;
        }
        if (data.error) {
          toast({ title: 'Analysis failed', description: data.error, variant: 'destructive' });
        } else {
          const getRes = await fetch(`/api/library/artist/${selectedArtist}`);
          if (getRes.ok) {
            const artistData = await getRes.json();
            setArtistDetails(artistData);
            toast({ title: 'Analysis complete', description: 'Artist data updated with duplicates, covers, MBID and incomplete albums.' });
          }
        }
        setAnalyzeProgress(null);
      }
    } catch {
      // ignore
    }
  }, [selectedArtist, toast]);

  const handleStartAnalyze = useCallback(async () => {
    if (!selectedArtist) return;
    setShowAnalyzeChoiceModal(false);
    try {
      const res = await fetch(`/api/library/artist/${selectedArtist}/analyze`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed to start analysis');
      if (data.started) {
        setAnalyzeProgress({ running: true });
        analyzePollRef.current = setInterval(fetchAnalyzeProgress, 450);
        fetchAnalyzeProgress();
      } else {
        throw new Error(data.error || 'Failed to start');
      }
    } catch (e) {
      toast({ title: 'Error', description: e instanceof Error ? e.message : 'Failed to start analysis', variant: 'destructive' });
    }
  }, [selectedArtist, fetchAnalyzeProgress, toast]);

  const fetchImproveAllProgress = useCallback(async () => {
    try {
      const res = await fetch('/api/library/improve-all-albums/progress');
      const data = await res.json();
      setImproveAllProgress(data);
      if (data.finished && data.result) {
        setImproveAllResult({
          message: data.result.message ?? '',
          albums_processed: data.result.albums_processed ?? 0,
          albums_improved: data.result.albums_improved ?? 0,
          covers_downloaded: data.result.covers_downloaded ?? 0,
          tags_updated: data.result.tags_updated ?? 0,
          by_provider: data.result.by_provider ?? {},
          album_log: data.result.album_log ?? [],
        });
        setImprovingAlbums(false);
        if (improveAllPollRef.current) {
          clearInterval(improveAllPollRef.current);
          improveAllPollRef.current = null;
        }
        toast({ title: 'Success', description: data.result.message });
        if (selectedArtist) loadArtistDetails(selectedArtist);
      }
      if (data.error) {
        toast({ title: 'Error', description: data.error, variant: 'destructive' });
        setImprovingAlbums(false);
        if (improveAllPollRef.current) {
          clearInterval(improveAllPollRef.current);
          improveAllPollRef.current = null;
        }
      }
    } catch {
      // keep polling on network glitch
    }
  }, [selectedArtist, loadArtistDetails, toast]);

  const handleImproveAllAlbums = async () => {
    if (!selectedArtist) return;
    setShowImproveAllModal(true);
    setImproveAllResult(null);
    setImproveAllProgress(null);
    setImprovingAlbums(true);
    try {
      const response = await fetch('/api/library/improve-all-albums', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ artist_id: selectedArtist }),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'Failed to start improve all');
      if (result.started) {
        improveAllPollRef.current = setInterval(fetchImproveAllProgress, 450);
        fetchImproveAllProgress();
      } else {
        setImprovingAlbums(false);
        toast({ title: 'Error', description: result.error ?? 'Failed to start', variant: 'destructive' });
      }
    } catch (error) {
      setImprovingAlbums(false);
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to improve albums',
        variant: 'destructive',
      });
    }
  };

  const handleDedupeArtist = async () => {
    if (!artistDetails || selectedArtist == null) return;
    setDedupingArtist(true);
    try {
      const result = await api.dedupeArtist(artistDetails.artist_name);
      const moved = result?.moved?.length ?? 0;
      if (moved > 0) {
        toast({
          title: 'Deduplication complete',
          description: `Removed ${moved} duplicate album(s). Refreshing artist…`,
        });
        await loadArtistDetails(selectedArtist);
      } else {
        toast({
          title: 'No duplicates removed',
          description: 'Duplicate groups may come from a full library scan. Run a scan to detect and remove them.',
          variant: 'default',
        });
        await loadArtistDetails(selectedArtist);
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Deduplication failed',
        variant: 'destructive',
      });
    } finally {
      setDedupingArtist(false);
    }
  };

  useEffect(() => {
    return () => {
      if (improveAllPollRef.current) {
        clearInterval(improveAllPollRef.current);
        improveAllPollRef.current = null;
      }
    };
  }, []);

  // Group albums by type
  const groupedAlbums = artistDetails?.albums.reduce((acc, album) => {
    const type = album.type || 'Album';
    if (!acc[type]) {
      acc[type] = [];
    }
    acc[type].push(album);
    return acc;
  }, {} as Record<string, AlbumInfo[]>) || {};

  const albumTypeOrder = ['Album', 'EP', 'Single', 'Compilation', 'Anthology'];
  const sortedTypes = Object.keys(groupedAlbums).sort((a, b) => {
    const aIndex = albumTypeOrder.indexOf(a);
    const bIndex = albumTypeOrder.indexOf(b);
    if (aIndex === -1 && bIndex === -1) return a.localeCompare(b);
    if (aIndex === -1) return 1;
    if (bIndex === -1) return -1;
    return aIndex - bIndex;
  });

  if (loading && artists.length === 0) {
    return (
      <>
        <Header />
        <div className="container py-6">
          <div className="flex items-center justify-center py-12">
            <Loader2 className="w-8 h-8 animate-spin text-primary" />
          </div>
        </div>
      </>
    );
  }

  return (
    <>
      <Header />
      <div className="container py-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold">Library Browser</h1>
            <p className="text-sm text-muted-foreground mt-1">
              {totalArtists > 0 ? `${totalArtists.toLocaleString()} artist${totalArtists !== 1 ? 's' : ''}` : 'Browse your music library by artist'}
            </p>
          </div>
          <NormalizeAlbumNamesButton onDone={() => { if (selectedArtist) loadArtistDetails(selectedArtist); }} />
        </div>

        <Card className="mb-6 border-border/70">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between gap-3">
              <div>
                <CardTitle className="text-base">For You</CardTitle>
                <CardDescription>Session-aware recommendations (embedding + behavior ranking)</CardDescription>
              </div>
              <Button variant="outline" size="sm" onClick={loadRecommendations} disabled={loadingRecommendations} className="gap-1.5">
                {loadingRecommendations ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                Refresh
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            {loadingRecommendations && recommendations.length === 0 ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="w-4 h-4 animate-spin" />
                Building recommendations...
              </div>
            ) : recommendations.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                {recommendationsError ?? 'Start listening to tracks to personalize this feed.'}
              </p>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
                {recommendations.map((rec) => (
                  <button
                    key={`rec-${rec.track_id}`}
                    type="button"
                    onClick={() => handlePlayRecommendedTrack(rec)}
                    className="group rounded-lg border border-border/70 bg-card p-3 text-left hover:border-primary/40 hover:bg-accent/30 transition-colors"
                  >
                    <div className="flex items-start gap-3">
                      <div className="w-12 h-12 rounded-md bg-muted overflow-hidden shrink-0 flex items-center justify-center">
                        {rec.thumb ? (
                          <img src={rec.thumb} alt={rec.album_title} className="w-full h-full object-cover" />
                        ) : (
                          <Music className="w-5 h-5 text-muted-foreground" />
                        )}
                      </div>
                      <div className="min-w-0 flex-1">
                        <p className="font-medium text-sm truncate">{rec.title}</p>
                        <p className="text-xs text-muted-foreground truncate mt-0.5">
                          {rec.artist_name} · {rec.album_title}
                        </p>
                        <div className="flex items-center gap-2 mt-2">
                          <Badge variant="outline" className="text-[10px]">score {(rec.score ?? 0).toFixed(2)}</Badge>
                          {Array.isArray(rec.reasons) && rec.reasons.length > 0 && (
                            <span className="text-[10px] text-muted-foreground truncate">{rec.reasons.join(' · ')}</span>
                          )}
                        </div>
                      </div>
                      <Play className="w-4 h-4 text-muted-foreground group-hover:text-primary shrink-0 mt-0.5" />
                    </div>
                  </button>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Artists List */}
          <div className="lg:col-span-1">
            <div className="mb-4">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                <Input
                  placeholder="Search artists..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && artists.length > 0) {
                      navigate(`/library/artist/${artists[0].artist_id}`);
                    }
                  }}
                  className="pl-9"
                />
              </div>
            </div>

            <div
              ref={artistsListRef}
              className="max-h-[calc(100vh-250px)] overflow-y-auto"
            >
              {loading && artists.length === 0 ? (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="w-6 h-6 animate-spin text-primary" />
                </div>
              ) : artists.length === 0 ? (
                <Card>
                  <CardContent className="p-6 text-center text-muted-foreground">
                    <Music className="w-8 h-8 mx-auto mb-2 opacity-50" />
                    <p>No artists found</p>
                  </CardContent>
                </Card>
              ) : (
                <div
                  style={{ height: `${rowVirtualizer.getTotalSize()}px`, width: '100%', position: 'relative' }}
                >
                  {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                    const artist = artists[virtualRow.index];
                    return (
                      <div
                        key={artist.artist_id}
                        style={{
                          position: 'absolute',
                          top: 0,
                          left: 0,
                          width: '100%',
                          height: `${virtualRow.size}px`,
                          transform: `translateY(${virtualRow.start}px)`,
                        }}
                        className="pb-2 pr-1"
                      >
                        <Card
                          className={cn(
                            "cursor-pointer transition-all hover:shadow-md h-full",
                            selectedArtist === artist.artist_id ? 'border-primary bg-primary/5 shadow-md' : ''
                          )}
                          onClick={() => handleArtistClick(artist.artist_id)}
                        >
                          <CardContent className="p-4">
                            <div className="flex items-center gap-3">
                              <div className="w-12 h-12 rounded-full bg-muted flex items-center justify-center shrink-0 overflow-hidden">
                                {artist.artist_thumb ? (
                                  <img
                                    src={artist.artist_thumb}
                                    alt={artist.artist_name}
                                    className="w-12 h-12 object-cover"
                                  />
                                ) : (
                                  <Music className="w-6 h-6 text-muted-foreground" />
                                )}
                              </div>
                              <div className="flex-1 min-w-0">
                                <div className="font-medium truncate">{artist.artist_name}</div>
                                <div className="flex items-center gap-2 mt-1">
                                  <Badge variant="outline" className="text-xs">
                                    {artist.album_count} album{artist.album_count !== 1 ? 's' : ''}
                                  </Badge>
                                  {artist.broken_albums_count > 0 && (
                                    <Badge variant="destructive" className="text-xs">
                                      {artist.broken_albums_count} incomplete
                                    </Badge>
                                  )}
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    className="h-6 px-2 text-[11px]"
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      navigate(`/library/artist/${artist.artist_id}`);
                                    }}
                                  >
                                    Open
                                  </Button>
                                </div>
                              </div>
                            </div>
                          </CardContent>
                        </Card>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>

          {/* Artist Details */}
          <div className="lg:col-span-2">
            {selectedArtist ? (
              loadingDetails ? (
                <Dialog open={true} onOpenChange={() => {}}>
                  <DialogContent className="sm:max-w-md" onPointerDownOutside={(e) => e.preventDefault()} onEscapeKeyDown={(e) => e.preventDefault()}>
                    <DialogHeader>
                      <DialogTitle>Analyzing artist</DialogTitle>
                      <DialogDescription>
                        Checking duplicates, covers, MusicBrainz IDs, and incomplete albums before displaying the list…
                      </DialogDescription>
                    </DialogHeader>
                    <div className="flex items-center justify-center py-6">
                      <Loader2 className="w-10 h-10 animate-spin text-primary" />
                    </div>
                  </DialogContent>
                </Dialog>
              ) : detailsLoadError ? (
                <Card>
                  <CardContent className="p-8 text-center">
                    <p className="text-muted-foreground mb-4">
                      {detailsLoadError.includes('fetch') || detailsLoadError.includes('network')
                        ? 'Network request was interrupted (e.g. tab in background or connection suspended).'
                        : 'Could not load artist details.'}
                    </p>
                    <Button
                      variant="outline"
                      onClick={() => selectedArtist && loadArtistDetails(selectedArtist)}
                      className="gap-2"
                    >
                      <RefreshCw className="w-4 h-4" />
                      Retry
                    </Button>
                  </CardContent>
                </Card>
              ) : artistDetails ? (
                  <Tabs defaultValue="albums" className="space-y-4">
                    <div className="flex items-center gap-4 flex-wrap">
                      {/* Artist name first so always visible */}
                      <div className="flex-1 min-w-0">
                        <h2 className="text-2xl sm:text-3xl font-bold text-foreground truncate" title={artistDetails.artist_name}>
                          {artistDetails.artist_name}
                        </h2>
                        <p className="text-sm text-muted-foreground mt-0.5">
                          {artistDetails.total_albums} album{artistDetails.total_albums !== 1 ? 's' : ''}
                        </p>
                        {artistDetails.stats && (
                          <div className="flex flex-wrap gap-4 text-xs text-muted-foreground mt-2">
                            <span>Duplicates: {artistDetails.stats.duplicates}/{artistDetails.total_albums}</span>
                            <span>Missing covers: {artistDetails.stats.no_cover}/{artistDetails.total_albums}</span>
                            <span>MB identified: {artistDetails.stats.mb_identified}/{artistDetails.total_albums}</span>
                            <span>Incomplete: {artistDetails.stats.broken}/{artistDetails.total_albums}</span>
                          </div>
                        )}
                        {loadingDetails && (
                          <p className="text-xs text-muted-foreground mt-1 italic">
                            Loading album titles... This may take a moment if titles are being read from audio files for the first time.
                          </p>
                        )}
                      </div>
                      <div className="w-full sm:w-auto shrink-0">
                        <div className="flex items-center gap-2 flex-wrap">
                            <Button
                              variant="ghost"
                              size="sm"
                              className="gap-1.5"
                              onClick={() => {
                                if (artistDetails.analysis_cached) {
                                  handleStartAnalyze();
                                } else {
                                  setArtistDetails(null);
                                  loadArtistDetails(selectedArtist);
                                }
                              }}
                              disabled={loadingDetails || analyzeProgress != null}
                            >
                              <RefreshCw className="w-4 h-4" />
                              {artistDetails.analysis_cached ? 'Re-analyze' : 'Refresh analysis'}
                            </Button>
                            <Button 
                              variant="outline" 
                              size="sm" 
                              className={cn("gap-1.5", monitoredArtists.has(selectedArtist) && "bg-primary/10 border-primary")}
                              onClick={() => handleAddToLidarr(selectedArtist, artistDetails.artist_name)}
                              disabled={addingToLidarr || monitoredArtists.has(selectedArtist)}
                            >
                              {addingToLidarr ? (
                                <Loader2 className="w-4 h-4 animate-spin" />
                              ) : (
                                <Star className={cn("w-4 h-4", monitoredArtists.has(selectedArtist) && "fill-primary text-primary")} />
                              )}
                              {monitoredArtists.has(selectedArtist) ? 'Monitored' : 'Monitor'}
                            </Button>
                            {artistDetails.stats && artistDetails.stats.duplicates > 0 && (
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={handleDedupeArtist}
                                disabled={dedupingArtist}
                                className="gap-1.5"
                              >
                                {dedupingArtist ? (
                                  <Loader2 className="w-4 h-4 animate-spin" />
                                ) : (
                                  <CopyMinus className="w-4 h-4" />
                                )}
                                Undupe artist
                              </Button>
                            )}
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={handleImproveAllAlbums}
                              disabled={improvingAlbums}
                              className="gap-1.5"
                            >
                              {improvingAlbums ? (
                                <Loader2 className="w-4 h-4 animate-spin" />
                              ) : (
                                <Sparkles className="w-4 h-4" />
                              )}
                              Fix All Albums
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => navigate(`/library/artist/${selectedArtist}`)}
                              className="gap-1.5"
                            >
                              Artist Page
                            </Button>
                        </div>
                      </div>
                    </div>

                    <TabsList>
                      <TabsTrigger value="albums">Albums</TabsTrigger>
                      <TabsTrigger value="similar">Similar Artists</TabsTrigger>
                    </TabsList>

                    <TabsContent value="albums" className="space-y-6">
                      <>
                          <div className="flex items-center justify-between gap-2 flex-wrap">
                            <div className="flex flex-col gap-1">
                              <span className="text-sm text-muted-foreground">
                                {artistDetails.albums.length} album{artistDetails.albums.length !== 1 ? 's' : ''}
                              </span>
                              {loadingDetails && (
                                <p className="text-xs text-muted-foreground italic">
                                  Loading album titles... This may take a moment if titles are being read from audio files for the first time.
                                </p>
                              )}
                            </div>
                            <div className="flex rounded-lg border border-border p-0.5">
                              <Button
                                variant={albumViewMode === 'tile' ? 'secondary' : 'ghost'}
                                size="sm"
                                className="h-8 px-2.5"
                                onClick={() => { setAlbumViewMode('tile'); localStorage.setItem('pmda-library-album-view', 'tile'); }}
                              >
                                <LayoutGrid className="w-4 h-4" />
                                <span className="ml-1.5 text-xs">Tile</span>
                              </Button>
                              <Button
                                variant={albumViewMode === 'list' ? 'secondary' : 'ghost'}
                                size="sm"
                                className="h-8 px-2.5"
                                onClick={() => { setAlbumViewMode('list'); localStorage.setItem('pmda-library-album-view', 'list'); }}
                              >
                                <List className="w-4 h-4" />
                                <span className="ml-1.5 text-xs">List</span>
                              </Button>
                            </div>
                          </div>
                          {sortedTypes.length === 0 ? (
                            <Card>
                              <CardContent className="p-12 text-center text-muted-foreground">
                                <Music className="w-12 h-12 mx-auto mb-4 opacity-50" />
                                <p>No albums found</p>
                              </CardContent>
                            </Card>
                          ) : albumViewMode === 'list' ? (
                            <div className="space-y-6">
                              {sortedTypes.map((type) => (
                                <div key={type} className="space-y-2">
                                  <h3 className="text-lg font-semibold">{type}s</h3>
                                  <div className="rounded-md border">
                                    <table className="w-full text-sm">
                                      <thead>
                                        <tr className="border-b bg-muted/50">
                                          <th className="w-10 p-2 text-left font-medium"></th>
                                          <th className="w-12 p-2 text-left font-medium">Cover</th>
                                          <th className="p-2 text-left font-medium">Title</th>
                                          <th className="w-20 p-2 text-left font-medium">Year</th>
                                          <th className="w-16 p-2 text-left font-medium">Tracks</th>
                                          <th className="w-20 p-2 text-left font-medium">Format</th>
                                          <th className="w-20 p-2 text-left font-medium">Type</th>
                                          <th className="w-32 p-2 text-left font-medium">Tags</th>
                                          <th className="w-14 p-2 text-left font-medium">Fix</th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {groupedAlbums[type].flatMap((album) => [
                                          <tr
                                            key={album.album_id}
                                            className={cn(
                                              "border-b hover:bg-muted/30 cursor-pointer transition-colors",
                                              editingAlbum === album.album_id && "bg-muted/20"
                                            )}
                                            onClick={() => setEditingAlbum((prev) => (prev === album.album_id ? null : album.album_id))}
                                          >
                                            <td className="p-2 w-10" onClick={(e) => e.stopPropagation()}>
                                              <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => handlePlayAlbum(album.album_id)} title="Play album">
                                                <Play className="h-4 w-4" />
                                              </Button>
                                            </td>
                                            <td className="p-2 w-12">
                                              <div className="w-10 h-10 rounded overflow-hidden bg-muted shrink-0">
                                                {album.thumb ? (
                                                  <img src={album.thumb} alt="" className="w-full h-full object-cover" onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }} />
                                                ) : (
                                                  <div className="w-full h-full flex items-center justify-center"><ImageIcon className="w-5 h-5 text-muted-foreground" /></div>
                                                )}
                                              </div>
                                            </td>
                                            <td className="p-2 font-medium text-foreground min-w-[140px] max-w-[280px] truncate" title={album.title}>{album.title}</td>
                                            <td className="p-2 text-muted-foreground">{album.year || album.date || '—'}</td>
                                            <td className="p-2 text-muted-foreground">{album.track_count || '—'}</td>
                                            <td className="p-2">
                                              {album.format ? <FormatBadge format={album.format} size="sm" /> : '—'}
                                            </td>
                                            <td className="p-2">
                                              <Badge variant="outline" className="text-xs">{type}</Badge>
                                            </td>
                                            <td className="p-2">
                                              <IssueBadges album={album} />
                                              {album.is_broken && album.broken_detail && (
                                                <p className="mt-1 text-xs text-muted-foreground">
                                                  {album.broken_detail.expected_track_count} expected, {album.broken_detail.actual_track_count} present
                                                  {album.broken_detail.missing_indices?.length ? ` · Missing #${album.broken_detail.missing_indices.join(', #')}` : ''}
                                                </p>
                                              )}
                                            </td>
                                            <td className="p-2" onClick={(e) => e.stopPropagation()}>
                                              {albumHasIssue(album) && (
                                                <Button
                                                  variant="ghost"
                                                  size="sm"
                                                  className="h-8 gap-1 text-xs"
                                                  onClick={() => setImproveDialogAlbum({ albumId: album.album_id, albumTitle: album.title || 'Album' })}
                                                >
                                                  <Wrench className="h-3.5 w-3.5" />
                                                  Fix
                                                </Button>
                                              )}
                                            </td>
                                          </tr>,
                                          <tr key={`${album.album_id}-drawer`} className="border-b">
                                            <td colSpan={9} className="p-0 align-top">
                                              <div
                                                className={cn(
                                                  "grid transition-[grid-template-rows] duration-300 ease-out",
                                                  editingAlbum === album.album_id ? "grid-rows-[1fr]" : "grid-rows-[0fr]"
                                                )}
                                              >
                                                <div className="min-h-0 overflow-hidden">
                                                  <div className="border-y border-border bg-muted/30 p-4">
                                                    <AlbumEditor
                                                      albumId={album.album_id}
                                                      albumTitle={album.title}
                                                      artistName={artistDetails.artist_name}
                                                      albumThumb={album.thumb}
                                                      format={album.format}
                                                      canImprove={album.can_improve}
                                                      improveReasons={getCanImproveReasons(album)}
                                                      brokenDetail={album.broken_detail}
                                                      onClose={() => setEditingAlbum(null)}
                                                    />
                                                  </div>
                                                </div>
                                              </div>
                                            </td>
                                          </tr>,
                                        ])}
                                      </tbody>
                                    </table>
                                  </div>
                                </div>
                              ))}
                            </div>
                          ) : (
                            sortedTypes.map((type) => {
                              const albums = groupedAlbums[type];
                              // Group albums into rows based on responsive grid columns
                              // We'll use a flexible approach: group by chunks that represent rows
                              const rows: AlbumInfo[][] = [];
                              const colsPerRow = 4; // md:grid-cols-4 is the max, we'll use this as base
                              for (let i = 0; i < albums.length; i += colsPerRow) {
                                rows.push(albums.slice(i, i + colsPerRow));
                              }
                              
                              return (
                                <div key={type} className="space-y-3">
                                  <h3 className="text-lg font-semibold">{type}s</h3>
                                  {rows.map((row, rowIndex) => {
                                    const hasSelectedInRow = row.some(album => editingAlbum === album.album_id);
                                    const selectedAlbumInRow = row.find(album => editingAlbum === album.album_id);
                                    
                                    return (
                                      <div key={rowIndex} className="space-y-2">
                                        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-4">
                                          {row.map((album) => (
                                            <Card
                                              key={album.album_id}
                                              className={cn(
                                                "group cursor-pointer hover:shadow-lg transition-all overflow-hidden",
                                                editingAlbum === album.album_id && "ring-2 ring-primary"
                                              )}
                                              onClick={() => setEditingAlbum((prev) => (prev === album.album_id ? null : album.album_id))}
                                            >
                                              <AspectRatio ratio={1} className="relative overflow-hidden bg-muted">
                                                {album.thumb ? (
                                                  <img
                                                    src={album.thumb}
                                                    alt={album.title || 'Album'}
                                                    className="w-full h-full object-cover group-hover:scale-105 transition-transform"
                                                    onError={(e) => {
                                                      (e.target as HTMLImageElement).style.display = 'none';
                                                    }}
                                                  />
                                                ) : (
                                                  <div className="w-full h-full flex items-center justify-center">
                                                    <ImageIcon className="w-12 h-12 text-muted-foreground" />
                                                  </div>
                                                )}
                                                <Button
                                                  variant="secondary"
                                                  size="icon"
                                                  className="absolute bottom-2 right-2 h-9 w-9 rounded-full opacity-0 group-hover:opacity-100 shadow-md"
                                                  onClick={(e) => { e.stopPropagation(); handlePlayAlbum(album.album_id); }}
                                                  title="Play album"
                                                >
                                                  <Play className="h-4 h-4 fill-current" />
                                                </Button>
                                              </AspectRatio>
                                              <CardContent className="p-3">
                                                <CardTitle className="text-sm truncate mb-1 text-foreground">{album.title}</CardTitle>
                                                <CardDescription className="text-xs">
                                                  {album.year || album.date || 'Unknown year'}
                                                  {album.track_count > 0 && ` • ${album.track_count} track${album.track_count !== 1 ? 's' : ''}`}
                                                </CardDescription>
                                                <div className="flex flex-wrap items-center gap-1 mt-2">
                                                  {album.format && <FormatBadge format={album.format} size="sm" className="text-xs" />}
                                                  <Badge variant="outline" className="text-xs">{type}</Badge>
                                                  <IssueBadges album={album} />
                                                  {albumHasIssue(album) && (
                                                    <Button
                                                      variant="ghost"
                                                      size="sm"
                                                      className="h-7 gap-1 text-xs shrink-0"
                                                      onClick={(e) => { e.stopPropagation(); setImproveDialogAlbum({ albumId: album.album_id, albumTitle: album.title || 'Album' }); }}
                                                    >
                                                      <Wrench className="h-3 w-3" />
                                                      Fix
                                                    </Button>
                                                  )}
                                                </div>
                                                {album.is_broken && album.broken_detail && (
                                                  <p className="mt-1 text-[10px] text-muted-foreground">
                                                    {album.broken_detail.expected_track_count} expected, {album.broken_detail.actual_track_count} present
                                                    {album.broken_detail.missing_indices?.length ? ` · Missing #${album.broken_detail.missing_indices.join(', #')}` : ''}
                                                  </p>
                                                )}
                                              </CardContent>
                                            </Card>
                                          ))}
                                        </div>
                                        {/* Drawer for selected album in this row - uses full width */}
                                        {hasSelectedInRow && selectedAlbumInRow && (
                                          <div
                                            className={cn(
                                              "grid transition-[grid-template-rows] duration-300 ease-out",
                                              editingAlbum === selectedAlbumInRow.album_id ? "grid-rows-[1fr]" : "grid-rows-[0fr]"
                                            )}
                                          >
                                            <div className="min-h-0 overflow-hidden">
                                              <div className="rounded-md border border-border bg-muted/30 p-4">
                                                <AlbumEditor
                                                  albumId={selectedAlbumInRow.album_id}
                                                  albumTitle={selectedAlbumInRow.title || 'Untitled'}
                                                  artistName={artistDetails.artist_name}
                                                  albumThumb={selectedAlbumInRow.thumb}
                                                  format={selectedAlbumInRow.format}
                                                  canImprove={selectedAlbumInRow.can_improve}
                                                  improveReasons={getCanImproveReasons(selectedAlbumInRow)}
                                                  brokenDetail={selectedAlbumInRow.broken_detail}
                                                  onClose={() => setEditingAlbum(null)}
                                                />
                                              </div>
                                            </div>
                                          </div>
                                        )}
                                      </div>
                                    );
                                  })}
                                </div>
                              );
                            })
                          )}
                        </>

                    </TabsContent>

                    <TabsContent value="similar" className="space-y-4">
                      <SimilarArtists artistId={selectedArtist!} artistName={artistDetails.artist_name} />
                    </TabsContent>
                  </Tabs>
              ) : null
            ) : (
              <Card>
                <CardContent className="p-12 text-center">
                  <Music className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
                  <p className="text-muted-foreground">
                    Select an artist from the list to view their albums
                  </p>
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      </div>

      <Dialog open={showImproveAllModal} onOpenChange={(open) => {
        setShowImproveAllModal(open);
        if (!open && improveAllPollRef.current) {
          clearInterval(improveAllPollRef.current);
          improveAllPollRef.current = null;
        }
      }}>
        <DialogContent className="max-w-2xl max-h-[85vh] flex flex-col" aria-describedby="improve-all-description">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Sparkles className="w-5 h-5" />
              Improve All Albums
            </DialogTitle>
            <DialogDescription id="improve-all-description">
              {(improveAllResult?.message != null ? improveAllResult.message : improveAllProgress?.running
                ? (improveAllProgress.total_albums != null && improveAllProgress.total_albums > 0
                    ? `Processing album ${improveAllProgress.albums_processed ?? 0} of ${improveAllProgress.total_albums}`
                    : 'Processing…') +
                  (improveAllProgress.current_album_id != null && improveAllProgress.current_album
                    ? ` — #${improveAllProgress.current_album_id} ${improveAllProgress.current_album}`
                    : improveAllProgress.current_album ? ` — ${improveAllProgress.current_album}` : '')
                : 'Query MusicBrainz, Discogs, Last.fm, and Bandcamp for tags and cover art, then update files.') || 'Improve all albums.'}
            </DialogDescription>
          </DialogHeader>
          {(improvingAlbums || improveAllProgress?.running) && !improveAllResult ? (
            <div className="space-y-4 overflow-hidden flex flex-col min-h-0">
              <div className="flex items-center gap-3 flex-wrap">
                <span className="text-xs font-medium text-muted-foreground">Providers</span>
                {['musicbrainz', 'discogs', 'lastfm', 'bandcamp'].map((name) => {
                  const status = improveAllProgress?.provider_status?.[name] ?? 'pending';
                  const isCurrent = improveAllProgress?.current_provider === name;
                  return (
                    <span key={name} className="flex items-center gap-1.5 text-xs capitalize">
                      {status === 'ok' && <Check className="h-4 w-4 text-green-600" aria-hidden />}
                      {status === 'fail' && <X className="h-4 w-4 text-destructive" aria-hidden />}
                      {status === 'pending' && !isCurrent && <Circle className="h-3.5 w-3.5 text-muted-foreground" aria-hidden />}
                      {isCurrent && <Loader2 className="h-4 w-4 animate-spin text-primary" aria-hidden />}
                      <span className={cn(isCurrent && 'font-medium')}>{name}</span>
                    </span>
                  );
                })}
              </div>
              {(improveAllProgress?.current_album_id != null || improveAllProgress?.current_album) && (
                <p className="text-sm text-muted-foreground">
                  Current album:{' '}
                  <span className="font-medium text-foreground">
                    {improveAllProgress.current_album_id != null && improveAllProgress.current_album
                      ? `#${improveAllProgress.current_album_id} – ${improveAllProgress.current_album}`
                      : improveAllProgress.current_album ?? `#${improveAllProgress.current_album_id}`}
                  </span>
                </p>
              )}
              {improveAllProgress?.current_steps && improveAllProgress.current_steps.length > 0 && (
                <div className="space-y-1">
                  <p className="text-xs font-medium text-muted-foreground">Current steps</p>
                  <ul className="space-y-0.5 text-xs">
                    {improveAllProgress.current_steps.map((step, i) => (
                      <li key={i} className="flex items-center gap-2">
                        {step.success === true && <Check className="h-3.5 w-3.5 text-green-600 shrink-0" />}
                        {step.success === false && <X className="h-3.5 w-3.5 text-destructive shrink-0" />}
                        {step.success === null && <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground shrink-0" />}
                        <span>{step.label}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {improveAllProgress?.album_log && improveAllProgress.album_log.length > 0 && (
                <Collapsible defaultOpen={true} className="min-h-0 flex flex-col">
                  <CollapsibleTrigger className="text-sm font-medium text-left py-1">Albums processed</CollapsibleTrigger>
                  <CollapsibleContent className="min-h-0 overflow-hidden">
                    <ScrollArea className="h-40 rounded border p-2">
                      <ul className="space-y-2 text-xs">
                        {improveAllProgress.album_log.map((entry) => (
                          <li key={entry.album_id}>
                            <span className="font-medium">{entry.title}</span>
                            <ul className="mt-0.5 pl-4 space-y-0.5 text-muted-foreground">
                              {entry.steps?.map((step, i) => (
                                <li key={i} className="flex items-center gap-2">
                                  {step.success === true && <Check className="h-3 w-3 text-green-600 shrink-0" />}
                                  {step.success === false && <X className="h-3 w-3 text-destructive shrink-0" />}
                                  <span>{step.label}</span>
                                </li>
                              ))}
                            </ul>
                          </li>
                        ))}
                      </ul>
                    </ScrollArea>
                  </CollapsibleContent>
                </Collapsible>
              )}
              {improveAllProgress?.error && (
                <p className="text-sm text-destructive">{improveAllProgress.error}</p>
              )}
            </div>
          ) : improveAllResult ? (
            <div className="space-y-4 overflow-hidden flex flex-col min-h-0">
              <p className="text-sm font-medium">{improveAllResult.message}</p>
              <div className="rounded-md border overflow-hidden">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b bg-muted/50">
                      <th className="text-left py-2 px-3 font-medium">Provider</th>
                      <th className="text-right py-2 px-3 font-medium">Identified</th>
                      <th className="text-right py-2 px-3 font-medium">Covers</th>
                      <th className="text-right py-2 px-3 font-medium">Tags</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(improveAllResult.by_provider || {}).map(([provider, stats]) => (
                      <tr key={provider} className="border-b last:border-0">
                        <td className="py-1.5 px-3 capitalize">{provider}</td>
                        <td className="py-1.5 px-3 text-right">{stats.identified}</td>
                        <td className="py-1.5 px-3 text-right">{stats.covers}</td>
                        <td className="py-1.5 px-3 text-right">{stats.tags}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {improveAllResult.album_log && improveAllResult.album_log.length > 0 && (
                <Collapsible defaultOpen={true} className="min-h-0 flex flex-col">
                  <CollapsibleTrigger className="text-sm font-medium text-left py-1">Report by album</CollapsibleTrigger>
                  <CollapsibleContent className="min-h-0 overflow-hidden">
                    <ScrollArea className="h-48 rounded border p-2">
                      <ul className="space-y-2 text-xs">
                        {improveAllResult.album_log.map((entry) => (
                          <li key={entry.album_id}>
                            <span className="font-medium">{entry.title}</span>
                            <ul className="mt-0.5 pl-4 space-y-0.5 text-muted-foreground">
                              {entry.steps?.map((step, i) => (
                                <li key={i} className="flex items-center gap-2">
                                  {step.success === true && <Check className="h-3 w-3 text-green-600 shrink-0" />}
                                  {step.success === false && <X className="h-3 w-3 text-destructive shrink-0" />}
                                  <span>{step.label}</span>
                                </li>
                              ))}
                            </ul>
                          </li>
                        ))}
                      </ul>
                    </ScrollArea>
                  </CollapsibleContent>
                </Collapsible>
              )}
            </div>
          ) : null}
        </DialogContent>
      </Dialog>

      {/* Choice: run full analysis or show without */}
      <Dialog open={showAnalyzeChoiceModal} onOpenChange={setShowAnalyzeChoiceModal}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Artist analysis</DialogTitle>
            <DialogDescription>
              Analyze this artist to see duplicates, missing cover art, MBID, and incomplete albums? Results will be cached for future openings.
            </DialogDescription>
          </DialogHeader>
          <div className="flex justify-end gap-2 pt-2">
            <Button variant="outline" onClick={() => setShowAnalyzeChoiceModal(false)}>
              Show without analyzing
            </Button>
            <Button onClick={handleStartAnalyze}>
              Analyze
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      {/* Artist analysis progress */}
      <Dialog open={analyzeProgress != null} onOpenChange={() => {}}>
        <DialogContent className="sm:max-w-md" onPointerDownOutside={(e) => e.preventDefault()} onEscapeKeyDown={(e) => e.preventDefault()}>
          <DialogHeader>
            <DialogTitle>Analyzing artist…</DialogTitle>
            <DialogDescription>
              {analyzeProgress?.total_albums != null && analyzeProgress.total_albums > 0
                ? `Album ${analyzeProgress.current_album_index ?? 0} / ${analyzeProgress.total_albums}`
                : null}
              {analyzeProgress?.current_album_title ? (
                <span className="block mt-1 truncate text-muted-foreground">{analyzeProgress.current_album_title}</span>
              ) : null}
              {analyzeProgress?.step ? (
                <span className="block mt-1 text-muted-foreground">{analyzeProgress.step}</span>
              ) : null}
            </DialogDescription>
          </DialogHeader>
          <div className="flex items-center justify-center py-6">
            {analyzeProgress?.error ? (
              <p className="text-sm text-destructive">{analyzeProgress.error}</p>
            ) : (
              <Loader2 className="w-10 h-10 animate-spin text-primary" />
            )}
          </div>
        </DialogContent>
      </Dialog>

      {/* Fix single album (from Fix column) */}
      <ImproveAlbumDialog
        open={!!improveDialogAlbum}
        onOpenChange={(open) => { if (!open) setImproveDialogAlbum(null); }}
        albumId={improveDialogAlbum?.albumId ?? 0}
        albumTitle={improveDialogAlbum?.albumTitle ?? ''}
        onSuccess={() => {
          if (selectedArtist != null) loadArtistDetails(selectedArtist);
        }}
      />

    </>
  );
}
