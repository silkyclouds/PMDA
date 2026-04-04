import { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import {
  ChevronLeft,
  ChevronDown,
  Play,
  Pause,
  SkipBack,
  SkipForward,
  Volume2,
  VolumeX,
  List,
  X,
  Music,
  ThumbsUp,
  ThumbsDown,
  Maximize2,
  GripVertical,
  SlidersHorizontal,
  RotateCcw,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Slider } from '@/components/ui/slider';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Switch } from '@/components/ui/switch';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { cn } from '@/lib/utils';
import * as api from '@/lib/api';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Badge } from '@/components/ui/badge';
import { FormatBadge } from '@/components/FormatBadge';
import { formatAudioSpec } from '@/lib/audioFormat';
import { badgeKindClass } from '@/lib/badgeStyles';
import { useTheme } from 'next-themes';
import { normalizePmdaAssetUrl } from '@/lib/api';
import { useIsMobile } from '@/hooks/use-mobile';

export interface TrackInfo {
  track_id: number;
  title: string;
  artist: string;
  album: string;
  album_id?: number;
  album_thumb?: string | null;
  duration: number;
  index: number;
  file_url: string;
}

interface AudioPlayerProps {
  albumId: number;
  albumTitle: string;
  albumThumb?: string | null;
  recommendationSessionId?: string | null;
  tracks: TrackInfo[];
  currentTrack: TrackInfo | null;
  onTrackSelect: (track: TrackInfo) => void;
  onClose: () => void;
}

type DeckIndex = 0 | 1;

interface PlayerPrefs {
  volume: number;
  muted: boolean;
  crossfadeEnabled: boolean;
  crossfadeSeconds: number;
  eqEnabled: boolean;
  eqPreset: string;
  eqGains: number[];
}

const PLAYER_PREFS_KEY = 'pmda_player_fx_v1';
const EQ_FREQUENCIES = [31, 62, 125, 250, 500, 1000, 2000, 4000, 8000, 16000] as const;
const EQ_PRESETS: Record<string, { label: string; gains: number[] }> = {
  flat: { label: 'Flat', gains: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0] },
  bass: { label: 'Bass boost', gains: [5, 4, 3, 2, 1, 0, -1, -1, -1, -1] },
  vocal: { label: 'Vocal focus', gains: [-2, -1, 0, 1, 2, 3, 3, 2, 1, 0] },
  clarity: { label: 'Clarity', gains: [-1, -1, 0, 1, 2, 2, 2, 2, 1, 0] },
  warm: { label: 'Warm', gains: [2, 2, 1, 1, 0, 0, -1, -1, -2, -2] },
  electronic: { label: 'Electronic', gains: [4, 3, 1, 0, -1, 1, 2, 3, 3, 2] },
  classical: { label: 'Classical', gains: [1, 0, -1, -1, 0, 1, 2, 2, 1, 1] },
};
const MIN_EQ_GAIN = -12;
const MAX_EQ_GAIN = 12;
const GAPLESS_LEAD_SECONDS = 0.10;

function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}:${m.toString().padStart(2, '0')}:${sec.toString().padStart(2, '0')}`;
  return `${m}:${sec.toString().padStart(2, '0')}`;
}

function parseGenreBadges(value?: string | null): string[] {
  const raw = String(value || '').trim();
  if (!raw) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of raw.split(/[;,/|]+/g).map((part) => part.trim()).filter(Boolean)) {
    const norm = item.toLowerCase();
    if (seen.has(norm)) continue;
    seen.add(norm);
    out.push(item);
    if (out.length >= 6) break;
  }
  return out;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function loadPlayerPrefs(): PlayerPrefs {
  try {
    const raw = window.localStorage.getItem(PLAYER_PREFS_KEY);
    if (!raw) throw new Error('missing');
    const parsed = JSON.parse(raw) as Partial<PlayerPrefs>;
    const presetKey = String(parsed.eqPreset || 'flat').toLowerCase();
    const gains = Array.isArray(parsed.eqGains)
      ? parsed.eqGains.slice(0, EQ_FREQUENCIES.length).map((value, index) => {
          const n = Number(value);
          const fallback = EQ_PRESETS.flat.gains[index] ?? 0;
          return Number.isFinite(n) ? clamp(n, MIN_EQ_GAIN, MAX_EQ_GAIN) : fallback;
        })
      : EQ_PRESETS[presetKey]?.gains?.slice() || EQ_PRESETS.flat.gains.slice();
    while (gains.length < EQ_FREQUENCIES.length) gains.push(0);
    return {
      volume: clamp(Number(parsed.volume ?? 1), 0, 1),
      muted: Boolean(parsed.muted),
      crossfadeEnabled: Boolean(parsed.crossfadeEnabled),
      crossfadeSeconds: clamp(Number(parsed.crossfadeSeconds ?? 4), 1, 12),
      eqEnabled: Boolean(parsed.eqEnabled),
      eqPreset: EQ_PRESETS[presetKey] ? presetKey : 'flat',
      eqGains: gains,
    };
  } catch {
    return {
      volume: 1,
      muted: false,
      crossfadeEnabled: false,
      crossfadeSeconds: 4,
      eqEnabled: false,
      eqPreset: 'flat',
      eqGains: EQ_PRESETS.flat.gains.slice(),
    };
  }
}

function savePlayerPrefs(prefs: PlayerPrefs): void {
  try {
    window.localStorage.setItem(PLAYER_PREFS_KEY, JSON.stringify(prefs));
  } catch {
    // ignore
  }
}

function PlaybackFxPanel({
  prefs,
  compact,
  isLightTheme,
  onCrossfadeEnabledChange,
  onCrossfadeSecondsChange,
  onEqEnabledChange,
  onEqPresetChange,
  onEqBandChange,
  onResetEq,
}: {
  prefs: PlayerPrefs;
  compact?: boolean;
  isLightTheme: boolean;
  onCrossfadeEnabledChange: (next: boolean) => void;
  onCrossfadeSecondsChange: (next: number) => void;
  onEqEnabledChange: (next: boolean) => void;
  onEqPresetChange: (preset: string) => void;
  onEqBandChange: (index: number, next: number) => void;
  onResetEq: () => void;
}) {
  return (
    <div
      className={cn(
        'space-y-5 rounded-2xl border p-4 backdrop-blur-sm',
        compact
          ? 'border-border/60 bg-card/90'
          : 'border-border/60 bg-card/80'
      )}
    >
      <div className="grid gap-4 lg:grid-cols-[1.05fr_1.45fr]">
        <div className="space-y-4">
          <div className="space-y-1.5">
            <div className="text-[11px] uppercase tracking-[0.22em] text-muted-foreground">
              Playback
            </div>
            <div className="flex items-center justify-between gap-3 rounded-xl border border-border/40 px-3 py-3">
              <div className="space-y-1">
                <div className="text-sm font-medium">Gapless playback</div>
                <div className="text-xs text-muted-foreground">
                  Always on. PMDA preloads the next track for seamless transitions.
                </div>
              </div>
              <Badge className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('source'))}>On</Badge>
            </div>
            <div className="rounded-xl border border-border/40 px-3 py-3">
              <div className="flex items-center justify-between gap-3">
                <div className="space-y-1">
                  <div className="text-sm font-medium">Crossfade</div>
                  <div className="text-xs text-muted-foreground">
                    Blend the outgoing and incoming tracks instead of a hard hand-off.
                  </div>
                </div>
                <Switch checked={prefs.crossfadeEnabled} onCheckedChange={onCrossfadeEnabledChange} />
              </div>
              <div className="mt-3 space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <span className="text-xs uppercase tracking-[0.18em] text-muted-foreground">
                    Fade length
                  </span>
                  <span className="text-sm tabular-nums">{prefs.crossfadeSeconds.toFixed(0)}s</span>
                </div>
                <Slider
                  value={[prefs.crossfadeSeconds]}
                  min={1}
                  max={12}
                  step={1}
                  onValueChange={(value) => onCrossfadeSecondsChange(value[0] ?? prefs.crossfadeSeconds)}
                  disabled={!prefs.crossfadeEnabled}
                />
              </div>
            </div>
          </div>
        </div>

        <div className="space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="space-y-1">
              <div className="text-[11px] uppercase tracking-[0.22em] text-muted-foreground">
                Equalizer
              </div>
              <div className="text-xs text-muted-foreground">
                Ten-band EQ tuned for music playback. Flat stays transparent when disabled.
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Select value={prefs.eqPreset} onValueChange={onEqPresetChange}>
                <SelectTrigger className="w-[170px]">
                  <SelectValue placeholder="Preset" />
                </SelectTrigger>
                <SelectContent>
                  {Object.entries(EQ_PRESETS).map(([key, preset]) => (
                    <SelectItem key={key} value={key}>{preset.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button type="button" variant="outline" size="sm" className="h-9 gap-2" onClick={onResetEq}>
                <RotateCcw className="h-4 w-4" />
                Reset
              </Button>
              <Switch checked={prefs.eqEnabled} onCheckedChange={onEqEnabledChange} />
            </div>
          </div>
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
            {EQ_FREQUENCIES.map((frequency, index) => (
              <div key={frequency} className="rounded-xl border border-border/40 px-3 py-3">
                <div className="flex items-center justify-between gap-2">
                  <span className="text-sm font-medium">{frequency >= 1000 ? `${frequency / 1000}k` : frequency} Hz</span>
                  <span className="text-xs tabular-nums">{prefs.eqGains[index] > 0 ? '+' : ''}{prefs.eqGains[index].toFixed(0)} dB</span>
                </div>
                <Slider
                  value={[prefs.eqGains[index]]}
                  min={MIN_EQ_GAIN}
                  max={MAX_EQ_GAIN}
                  step={1}
                  onValueChange={(value) => onEqBandChange(index, value[0] ?? 0)}
                  disabled={!prefs.eqEnabled}
                  className="mt-3"
                />
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

export function AudioPlayer({
  albumId,
  albumTitle,
  albumThumb,
  recommendationSessionId,
  tracks,
  currentTrack,
  onTrackSelect,
  onClose,
}: AudioPlayerProps) {
  const isMobile = useIsMobile();
  const [prefs, setPrefs] = useState<PlayerPrefs>(() => loadPlayerPrefs());
  const [isPlaying, setIsPlaying] = useState(false);
  const [currentTime, setCurrentTime] = useState(0);
  const [duration, setDuration] = useState(0);
  const [bufferedEnd, setBufferedEnd] = useState(0);
  const [showList, setShowList] = useState(false);
  const [showNowPlaying, setShowNowPlaying] = useState(false);
  const [showFxPanel, setShowFxPanel] = useState(false);
  const [coverError, setCoverError] = useState(false);
  const [trackLiked, setTrackLiked] = useState(false);
  const [albumMeta, setAlbumMeta] = useState<api.AlbumDetailResponse | null>(null);
  const tracksSectionRef = useRef<HTMLDivElement | null>(null);
  const mobileTitleViewportRef = useRef<HTMLDivElement | null>(null);
  const mobileTitleTextRef = useRef<HTMLSpanElement | null>(null);
  const [mobileTitleNeedsMarquee, setMobileTitleNeedsMarquee] = useState(false);

  const deckARef = useRef<HTMLAudioElement>(null);
  const deckBRef = useRef<HTMLAudioElement>(null);
  const audioContextRef = useRef<AudioContext | null>(null);
  const masterGainRef = useRef<GainNode | null>(null);
  const deckGainNodesRef = useRef<[GainNode | null, GainNode | null]>([null, null]);
  const deckSourceNodesRef = useRef<[MediaElementAudioSourceNode | null, MediaElementAudioSourceNode | null]>([null, null]);
  const eqFiltersRef = useRef<BiquadFilterNode[]>([]);
  const activeDeckRef = useRef<DeckIndex>(0);
  const activeTrackRef = useRef<TrackInfo | null>(null);
  const deckTracksRef = useRef<[TrackInfo | null, TrackInfo | null]>([null, null]);
  const playedSecondsRef = useRef(0);
  const finalizedTrackIdRef = useRef<number | null>(null);
  const startedTrackIdsRef = useRef<Set<number>>(new Set());
  const lastTrackLoadTimeRef = useRef<number>(0);
  const transitionActiveRef = useRef(false);
  const transitionTimerRef = useRef<number | null>(null);
  const transitionTargetTrackRef = useRef<TrackInfo | null>(null);
  const transitionTargetDeckRef = useRef<DeckIndex>(1);
  const lastAutoOpenSessionKeyRef = useRef<string>('');
  const currentIndex = currentTrack ? tracks.findIndex((t) => t.track_id === currentTrack.track_id) : -1;
  const displayDuration = duration > 0 ? duration : (currentTrack?.duration ?? 0);
  const { resolvedTheme } = useTheme();
  const isLightTheme = resolvedTheme === 'light';

  const resolvedAlbumId = (() => {
    if (Number.isFinite(albumId) && albumId > 0) return albumId;
    const trackAlbumId = Number(currentTrack?.album_id || 0);
    return Number.isFinite(trackAlbumId) && trackAlbumId > 0 ? trackAlbumId : albumId;
  })();
  const displayAlbumTitle = String(currentTrack?.album || '').trim() || albumTitle;
  const displayAlbumThumb = albumThumb || currentTrack?.album_thumb || null;
  const releaseYear = (() => {
    const year = Number(albumMeta?.year || 0);
    if (Number.isFinite(year) && year > 0) return String(year);
    const dateText = String(albumMeta?.date_text || '').trim();
    const m = dateText.match(/\b(19|20)\d{2}\b/);
    return m ? m[0] : '';
  })();
  const genres = parseGenreBadges(albumMeta?.genre);
  const labelText = String(albumMeta?.label || '').trim();
  const trackCountText = Number(albumMeta?.track_count || 0) > 0 ? `${Number(albumMeta?.track_count || 0)} tracks` : '';
  const discCountText = (() => {
    const rows = Array.isArray(albumMeta?.tracks) ? albumMeta?.tracks : [];
    const discNums = Array.from(new Set(rows.map((track) => Math.max(1, Number(track.disc_num || 1))))).sort((a, b) => a - b);
    return discNums.length > 1 ? `${discNums.length} discs` : '';
  })();

  const deckRefs = useMemo(() => [deckARef, deckBRef] as const, []);
  const isSimpleMobilePlayback = isMobile;
  const primaryMobileBadges = useMemo(() => {
    if (!albumMeta) return [] as React.ReactNode[];
    const badges: React.ReactNode[] = [];
    if (releaseYear) badges.push(<Badge key="year" variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('year'))}>{releaseYear}</Badge>);
    if (albumMeta.total_duration_sec > 0) badges.push(<Badge key="duration" variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('duration'))}>{formatDuration(albumMeta.total_duration_sec)}</Badge>);
    if (albumMeta.format) badges.push(<FormatBadge key="format" format={albumMeta.format} size="sm" className="h-7 rounded-full px-3 py-1 text-xs" />);
    if (trackCountText) badges.push(<Badge key="count" variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('count'))}>{trackCountText}</Badge>);
    return badges;
  }, [albumMeta, releaseYear, trackCountText]);
  const secondaryMobileMeta = useMemo(() => {
    const parts: string[] = [];
    const audioSpec = formatAudioSpec(albumMeta?.bit_depth, albumMeta?.sample_rate);
    if (audioSpec) parts.push(audioSpec);
    if (!albumMeta?.is_lossless && albumMeta) parts.push('Lossy');
    if (labelText) parts.push(labelText);
    if (genres[0]) parts.push(genres[0]);
    if (discCountText) parts.push(discCountText);
    return parts.join(' · ');
  }, [albumMeta, discCountText, genres, labelText]);

  useEffect(() => {
    savePlayerPrefs(prefs);
  }, [prefs]);

  useEffect(() => {
    const nextVolume = prefs.muted ? 0 : prefs.volume;
    [deckARef.current, deckBRef.current].forEach((deck) => {
      if (!deck) return;
      deck.preload = 'auto';
      deck.crossOrigin = 'anonymous';
      deck.loop = false;
      deck.setAttribute('playsinline', '');
      deck.setAttribute('webkit-playsinline', 'true');
      deck.volume = nextVolume;
    });
  }, [prefs.muted, prefs.volume]);

  const sendRecoEvent = useCallback((eventType: api.RecoEventType, track: TrackInfo | null, playedSeconds?: number) => {
    if (!track || !recommendationSessionId) return;
    void api.postRecommendationEvent({
      session_id: recommendationSessionId,
      track_id: track.track_id,
      event_type: eventType,
      played_seconds: Math.max(0, Math.floor(playedSeconds ?? 0)),
    }).catch(() => {
      // Non-blocking telemetry path.
    });
  }, [recommendationSessionId]);

  const postPlaybackEvent = useCallback((eventType: api.RecoEventType, track: TrackInfo | null, playedSeconds?: number) => {
    if (!track) return;
    void api.postPlaybackEvent({
      track_id: track.track_id,
      event_type: eventType,
      played_seconds: Math.max(0, Math.floor(playedSeconds ?? 0)),
    }).catch(() => {
      // Non-blocking telemetry/scrobble path.
    });
  }, []);

  const sendPlayStart = useCallback((track: TrackInfo | null) => {
    if (!track) return;
    if (startedTrackIdsRef.current.has(track.track_id)) return;
    startedTrackIdsRef.current.add(track.track_id);
    sendRecoEvent('play_start', track, 0);
    postPlaybackEvent('play_start', track, 0);
  }, [postPlaybackEvent, sendRecoEvent]);

  const finalizeTrack = useCallback((track: TrackInfo | null, playedSeconds: number, reason: 'switch' | 'ended' | 'close') => {
    if (!track) return;
    if (finalizedTrackIdRef.current === track.track_id) return;
    const completeThreshold = Math.max(30, Math.floor((track.duration || 0) * 0.85));
    let eventType: api.RecoEventType = 'skip';
    if (reason === 'ended' || playedSeconds >= completeThreshold) {
      eventType = 'play_complete';
    } else if (playedSeconds >= 12) {
      eventType = 'play_partial';
    } else if (reason === 'close') {
      eventType = 'stop';
    }
    sendRecoEvent(eventType, track, playedSeconds);
    postPlaybackEvent(eventType, track, playedSeconds);
    finalizedTrackIdRef.current = track.track_id;
    startedTrackIdsRef.current.delete(track.track_id);
  }, [postPlaybackEvent, sendRecoEvent]);

  const ensureAudioGraph = useCallback(async () => {
    if (isSimpleMobilePlayback) return;
    if (audioContextRef.current) {
      if (audioContextRef.current.state === 'suspended') {
        await audioContextRef.current.resume().catch(() => {});
      }
      return;
    }
    const AudioCtx = window.AudioContext || (window as typeof window & { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
    if (!AudioCtx) return;
    const deckA = deckARef.current;
    const deckB = deckBRef.current;
    if (!deckA || !deckB) return;

    const ctx = new AudioCtx();
    const masterGain = ctx.createGain();
    const filters = EQ_FREQUENCIES.map((frequency) => {
      const filter = ctx.createBiquadFilter();
      filter.type = 'peaking';
      filter.frequency.value = frequency;
      filter.Q.value = 1.05;
      filter.gain.value = 0;
      return filter;
    });

    if (filters.length > 0) {
      for (let index = 0; index < filters.length - 1; index += 1) {
        filters[index].connect(filters[index + 1]);
      }
      filters[filters.length - 1].connect(masterGain);
    } else {
      masterGain.connect(ctx.destination);
    }
    masterGain.connect(ctx.destination);

    [deckA, deckB].forEach((deck, index) => {
      deck.preload = 'auto';
      deck.crossOrigin = 'anonymous';
      deck.volume = 1;
      deck.muted = false;
      const source = ctx.createMediaElementSource(deck);
      const gain = ctx.createGain();
      gain.gain.value = index === 0 ? 1 : 0;
      source.connect(gain);
      if (filters.length > 0) {
        gain.connect(filters[0]);
      } else {
        gain.connect(masterGain);
      }
      deckSourceNodesRef.current[index as DeckIndex] = source;
      deckGainNodesRef.current[index as DeckIndex] = gain;
    });

    audioContextRef.current = ctx;
    masterGainRef.current = masterGain;
    eqFiltersRef.current = filters;
    const now = ctx.currentTime;
    masterGain.gain.setValueAtTime(prefs.muted ? 0 : prefs.volume, now);
    filters.forEach((filter, index) => {
      const gain = prefs.eqEnabled ? (prefs.eqGains[index] ?? 0) : 0;
      filter.gain.setValueAtTime(gain, now);
    });
    await ctx.resume().catch(() => {});
  }, [isSimpleMobilePlayback, prefs.eqEnabled, prefs.eqGains, prefs.muted, prefs.volume]);

  useEffect(() => {
    if (isSimpleMobilePlayback) return;
    const ctx = audioContextRef.current;
    const master = masterGainRef.current;
    if (!ctx || !master) return;
    const now = ctx.currentTime;
    master.gain.cancelScheduledValues(now);
    master.gain.setValueAtTime(master.gain.value, now);
    master.gain.linearRampToValueAtTime(prefs.muted ? 0 : prefs.volume, now + 0.04);
  }, [isSimpleMobilePlayback, prefs.muted, prefs.volume]);

  useEffect(() => {
    if (isSimpleMobilePlayback) return;
    const ctx = audioContextRef.current;
    const filters = eqFiltersRef.current;
    if (!ctx || filters.length === 0) return;
    const now = ctx.currentTime;
    filters.forEach((filter, index) => {
      const gain = prefs.eqEnabled ? (prefs.eqGains[index] ?? 0) : 0;
      filter.gain.cancelScheduledValues(now);
      filter.gain.setValueAtTime(gain, now);
    });
  }, [isSimpleMobilePlayback, prefs.eqEnabled, prefs.eqGains]);

  const getDeckTrack = useCallback((deckIndex: DeckIndex) => deckTracksRef.current[deckIndex], []);

  const getNextTrackFor = useCallback((track: TrackInfo | null) => {
    if (!track) return null;
    const index = tracks.findIndex((item) => item.track_id === track.track_id);
    if (index < 0 || index >= tracks.length - 1) return null;
    return tracks[index + 1] ?? null;
  }, [tracks]);

  const clearTransitionTimer = useCallback(() => {
    if (transitionTimerRef.current != null) {
      window.clearTimeout(transitionTimerRef.current);
      transitionTimerRef.current = null;
    }
  }, []);

  const stopDeck = useCallback((deckIndex: DeckIndex, reset = false) => {
    const el = deckRefs[deckIndex].current;
    if (!el) return;
    el.pause();
    if (reset) {
      try {
        el.currentTime = 0;
      } catch {
        // ignore
      }
    }
  }, [deckRefs]);

  const setDeckGain = useCallback((deckIndex: DeckIndex, value: number, rampSeconds = 0.04) => {
    const gainNode = deckGainNodesRef.current[deckIndex];
    const ctx = audioContextRef.current;
    if (!gainNode || !ctx) return;
    const now = ctx.currentTime;
    gainNode.gain.cancelScheduledValues(now);
    gainNode.gain.setValueAtTime(gainNode.gain.value, now);
    gainNode.gain.linearRampToValueAtTime(value, now + rampSeconds);
  }, []);

  const assignTrackToDeck = useCallback((deckIndex: DeckIndex, track: TrackInfo | null) => {
    const el = deckRefs[deckIndex].current;
    if (!el) return;
    deckTracksRef.current[deckIndex] = track;
    if (!track) {
      stopDeck(deckIndex, true);
      el.removeAttribute('src');
      el.load();
      return;
    }
    const trackUrl = normalizePmdaAssetUrl(track.file_url);
    if (!trackUrl) return;
    if (el.src !== trackUrl) {
      el.src = trackUrl;
      el.load();
    }
  }, [deckRefs, stopDeck]);

  const preloadDeckForTrack = useCallback((deckIndex: DeckIndex, track: TrackInfo | null) => {
    if (!track) return;
    assignTrackToDeck(deckIndex, track);
    const el = deckRefs[deckIndex].current;
    if (!el) return;
    el.preload = 'auto';
    setDeckGain(deckIndex, 0, 0.02);
  }, [assignTrackToDeck, deckRefs, setDeckGain]);

  const preloadUpcomingTrack = useCallback(() => {
    if (isSimpleMobilePlayback) return;
    const activeTrack = transitionActiveRef.current ? (transitionTargetTrackRef.current || activeTrackRef.current) : activeTrackRef.current;
    const nextTrack = getNextTrackFor(activeTrack);
    const preloadDeck = (activeDeckRef.current === 0 ? 1 : 0) as DeckIndex;
    if (!nextTrack) {
      if (!transitionActiveRef.current) assignTrackToDeck(preloadDeck, null);
      return;
    }
    if (getDeckTrack(preloadDeck)?.track_id === nextTrack.track_id) return;
    preloadDeckForTrack(preloadDeck, nextTrack);
  }, [assignTrackToDeck, getDeckTrack, getNextTrackFor, isSimpleMobilePlayback, preloadDeckForTrack]);

  const commitTransition = useCallback((pauseAfterCommit = false) => {
    if (!transitionActiveRef.current) return;
    clearTransitionTimer();
    const oldDeck = activeDeckRef.current;
    const nextDeck = transitionTargetDeckRef.current;
    const oldTrack = activeTrackRef.current;
    const nextTrack = transitionTargetTrackRef.current;
    const nextEl = deckRefs[nextDeck].current;
    const oldEl = deckRefs[oldDeck].current;

    if (oldTrack) {
      const played = Math.max(0, Math.floor(playedSecondsRef.current));
      finalizeTrack(oldTrack, played, 'ended');
    }
    if (oldEl) {
      oldEl.pause();
      try {
        oldEl.currentTime = 0;
      } catch {
        // ignore
      }
    }

    activeDeckRef.current = nextDeck;
    activeTrackRef.current = nextTrack;
    finalizedTrackIdRef.current = null;
    playedSecondsRef.current = nextEl?.currentTime || 0;
    transitionActiveRef.current = false;
    transitionTargetTrackRef.current = null;
    setCurrentTime(nextEl?.currentTime || 0);
    setBufferedEnd(0);
    setDuration(nextEl && Number.isFinite(nextEl.duration) ? nextEl.duration : (nextTrack?.duration || 0));
    if (pauseAfterCommit) {
      nextEl?.pause();
      setIsPlaying(false);
    } else {
      if (nextEl) {
        nextEl.muted = false;
        nextEl.volume = 1;
        setDeckGain(nextDeck, 1, 0.02);
        if (nextEl.paused) {
          void nextEl.play().catch(() => {});
        }
      }
      setIsPlaying(Boolean(nextEl && !nextEl.paused) || Boolean(nextEl));
    }
  }, [clearTransitionTimer, deckRefs, finalizeTrack, setDeckGain]);

  const beginTransitionToNext = useCallback(async (nextTrack: TrackInfo, fadeSeconds: number) => {
    if (transitionActiveRef.current) return false;
    await ensureAudioGraph();
    const oldDeck = activeDeckRef.current;
    const nextDeck = (oldDeck === 0 ? 1 : 0) as DeckIndex;
    const oldEl = deckRefs[oldDeck].current;
    const nextEl = deckRefs[nextDeck].current;
    if (!oldEl || !nextEl) return false;
    preloadDeckForTrack(nextDeck, nextTrack);
    try {
      nextEl.currentTime = 0;
    } catch {
      // ignore
    }
    transitionActiveRef.current = true;
    transitionTargetTrackRef.current = nextTrack;
    transitionTargetDeckRef.current = nextDeck;
    sendPlayStart(nextTrack);
    setCurrentTime(0);
    setDuration(nextTrack.duration || 0);
    try {
      await nextEl.play();
    } catch {
      transitionActiveRef.current = false;
      transitionTargetTrackRef.current = null;
      return false;
    }
    const effectiveFade = Math.max(0.04, fadeSeconds);
    setDeckGain(nextDeck, 1, effectiveFade);
    setDeckGain(oldDeck, 0, effectiveFade);
    onTrackSelect(nextTrack);
    transitionTimerRef.current = window.setTimeout(() => {
      commitTransition(false);
      preloadUpcomingTrack();
    }, Math.ceil(effectiveFade * 1000) + 40);
    return true;
  }, [commitTransition, deckRefs, ensureAudioGraph, onTrackSelect, preloadDeckForTrack, preloadUpcomingTrack, sendPlayStart, setDeckGain]);

  const startTrackOnDeck = useCallback(async (
    targetDeck: DeckIndex,
    track: TrackInfo,
    options?: { announceSelection?: boolean; stopOtherDeck?: boolean },
  ) => {
    const targetEl = deckRefs[targetDeck].current;
    if (!targetEl) return false;
    const otherDeck = (targetDeck === 0 ? 1 : 0) as DeckIndex;
    await ensureAudioGraph();
    if (options?.stopOtherDeck !== false) {
      stopDeck(otherDeck, true);
      setDeckGain(otherDeck, 0, 0.04);
    }
    if (getDeckTrack(targetDeck)?.track_id !== track.track_id) {
      assignTrackToDeck(targetDeck, track);
    }
    targetEl.preload = 'auto';
    targetEl.muted = false;
    targetEl.volume = 1;
    try {
      targetEl.currentTime = 0;
    } catch {
      // ignore
    }
    setDeckGain(targetDeck, 1, 0.04);
    activeDeckRef.current = targetDeck;
    activeTrackRef.current = track;
    finalizedTrackIdRef.current = null;
    playedSecondsRef.current = 0;
    setCurrentTime(0);
    setDuration(track.duration || 0);
    setBufferedEnd(0);
    setIsPlaying(false);
    sendPlayStart(track);
    lastTrackLoadTimeRef.current = Date.now();
    try {
      await targetEl.play();
      if (options?.announceSelection) {
        onTrackSelect(track);
      }
      setIsPlaying(true);
      preloadUpcomingTrack();
      return true;
    } catch {
      setIsPlaying(false);
      return false;
    }
  }, [
    assignTrackToDeck,
    deckRefs,
    ensureAudioGraph,
    getDeckTrack,
    onTrackSelect,
    preloadUpcomingTrack,
    sendPlayStart,
    setDeckGain,
    stopDeck,
  ]);

  const syncToCurrentTrack = useCallback(async (track: TrackInfo | null) => {
    if (!track) return;
    if (isSimpleMobilePlayback) {
      clearTransitionTimer();
      transitionActiveRef.current = false;
      transitionTargetTrackRef.current = null;

      if (activeTrackRef.current && activeTrackRef.current.track_id !== track.track_id) {
        const played = Math.max(0, Math.floor(playedSecondsRef.current));
        finalizeTrack(activeTrackRef.current, played, 'switch');
      }

      assignTrackToDeck(0, track);
      assignTrackToDeck(1, null);
      const targetEl = deckARef.current;
      if (!targetEl) return;
      try {
        targetEl.currentTime = 0;
      } catch {
        // ignore
      }
      targetEl.volume = prefs.muted ? 0 : prefs.volume;
      activeDeckRef.current = 0;
      activeTrackRef.current = track;
      finalizedTrackIdRef.current = null;
      playedSecondsRef.current = 0;
      setCurrentTime(0);
      setDuration(track.duration || 0);
      setBufferedEnd(0);
      setIsPlaying(false);
      sendPlayStart(track);
      lastTrackLoadTimeRef.current = Date.now();
      try {
        await targetEl.play();
        setIsPlaying(true);
      } catch {
        setIsPlaying(false);
      }
      return;
    }
    const activeDeck = activeDeckRef.current;
    const inactiveDeck = (activeDeck === 0 ? 1 : 0) as DeckIndex;
    const activeDeckTrack = getDeckTrack(activeDeck);
    const inactiveDeckTrack = getDeckTrack(inactiveDeck);

    if (transitionActiveRef.current && transitionTargetTrackRef.current?.track_id === track.track_id) {
      preloadUpcomingTrack();
      return;
    }
    if (activeTrackRef.current?.track_id === track.track_id && activeDeckTrack?.track_id === track.track_id) {
      preloadUpcomingTrack();
      return;
    }

    await ensureAudioGraph();
    clearTransitionTimer();
    transitionActiveRef.current = false;
    transitionTargetTrackRef.current = null;

    if (activeTrackRef.current && activeTrackRef.current.track_id !== track.track_id) {
      const played = Math.max(0, Math.floor(playedSecondsRef.current));
      finalizeTrack(activeTrackRef.current, played, 'switch');
    }

    const targetDeck = inactiveDeckTrack?.track_id === track.track_id ? inactiveDeck : activeDeck;
    const oldDeck = activeDeckRef.current;
    if (oldDeck !== targetDeck) {
      stopDeck(oldDeck, true);
      setDeckGain(oldDeck, 0, 0.04);
    }
    if (getDeckTrack(targetDeck)?.track_id !== track.track_id) {
      assignTrackToDeck(targetDeck, track);
    }
    await startTrackOnDeck(targetDeck, track, { announceSelection: false, stopOtherDeck: oldDeck !== targetDeck });
  }, [
    assignTrackToDeck,
    clearTransitionTimer,
    deckRefs,
    ensureAudioGraph,
    finalizeTrack,
    getDeckTrack,
    isSimpleMobilePlayback,
    preloadUpcomingTrack,
    prefs.muted,
    prefs.volume,
    setDeckGain,
    startTrackOnDeck,
    stopDeck,
  ]);

  useEffect(() => {
    if (!currentTrack) return;
    void syncToCurrentTrack(currentTrack);
  }, [currentTrack?.track_id, syncToCurrentTrack]);

  useEffect(() => {
    preloadUpcomingTrack();
  }, [tracks, currentTrack?.track_id, preloadUpcomingTrack]);

  useEffect(() => {
    if (!isMobile) return;
    const firstTrackId = Number(tracks[0]?.track_id || 0) || 0;
    const albumKey = Number(resolvedAlbumId || 0) > 0 ? String(resolvedAlbumId) : 'no-album';
    const sessionKey = `${albumKey}:${tracks.length}:${firstTrackId}`;
    if (firstTrackId > 0 && sessionKey !== lastAutoOpenSessionKeyRef.current) {
      setShowNowPlaying(true);
      lastAutoOpenSessionKeyRef.current = sessionKey;
    }
  }, [isMobile, resolvedAlbumId, tracks]);

  useEffect(() => {
    if (!isMobile || !showNowPlaying) {
      setMobileTitleNeedsMarquee(false);
      return;
    }
    const viewport = mobileTitleViewportRef.current;
    const textNode = mobileTitleTextRef.current;
    if (!viewport || !textNode) return;
    const check = () => {
      setMobileTitleNeedsMarquee(textNode.scrollWidth > viewport.clientWidth + 4);
    };
    check();
    if (typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver(check);
    observer.observe(viewport);
    observer.observe(textNode);
    return () => observer.disconnect();
  }, [currentTrack?.title, isMobile, showNowPlaying]);

  useEffect(() => {
    setCoverError(false);
  }, [resolvedAlbumId, displayAlbumThumb]);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      if (!Number.isFinite(resolvedAlbumId) || resolvedAlbumId <= 0) {
        if (!cancelled) setAlbumMeta(null);
        return;
      }
      try {
        const detail = await api.getAlbumDetail(resolvedAlbumId);
        if (!cancelled) setAlbumMeta(detail);
      } catch {
        if (!cancelled) setAlbumMeta(null);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [resolvedAlbumId]);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      if (!currentTrack?.track_id) {
        setTrackLiked(false);
        return;
      }
      try {
        const res = await api.getLikes('track', [currentTrack.track_id]);
        const liked = (res.items || []).some((it) => it.entity_id === currentTrack.track_id && Boolean(it.liked));
        if (!cancelled) setTrackLiked(liked);
      } catch {
        if (!cancelled) setTrackLiked(false);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [currentTrack?.track_id]);

  useEffect(() => {
    return () => {
      clearTransitionTimer();
      stopDeck(0, true);
      stopDeck(1, true);
      if (activeTrackRef.current) {
        finalizeTrack(activeTrackRef.current, Math.max(0, Math.floor(playedSecondsRef.current)), 'close');
      }
      if (audioContextRef.current) {
        void audioContextRef.current.close().catch(() => {});
      }
    };
  }, [clearTransitionTimer, finalizeTrack, stopDeck]);

  const getVisibleDeckIndex = useCallback((): DeckIndex => {
    if (transitionActiveRef.current && transitionTargetTrackRef.current && currentTrack?.track_id === transitionTargetTrackRef.current.track_id) {
      return transitionTargetDeckRef.current;
    }
    return activeDeckRef.current;
  }, [currentTrack?.track_id]);

  const handleDeckTimeUpdate = useCallback((deckIndex: DeckIndex) => {
    const el = deckRefs[deckIndex].current;
    if (!el) return;
    if (isSimpleMobilePlayback) {
      if (deckIndex !== 0) return;
      setCurrentTime(el.currentTime);
      if (duration === 0 && !Number.isNaN(el.duration) && Number.isFinite(el.duration)) {
        setDuration(el.duration);
      }
      if (el.buffered.length > 0) {
        const end = el.buffered.end(el.buffered.length - 1);
        setBufferedEnd(end);
      }
      playedSecondsRef.current = el.currentTime;
      return;
    }
    if (deckIndex === getVisibleDeckIndex()) {
      setCurrentTime(el.currentTime);
      if (duration === 0 && !Number.isNaN(el.duration) && Number.isFinite(el.duration)) {
        setDuration(el.duration);
      }
      playedSecondsRef.current = activeTrackRef.current?.track_id === currentTrack?.track_id
        ? el.currentTime
        : playedSecondsRef.current;
    }
    if (deckIndex !== activeDeckRef.current) return;
    if (el.paused || !isPlaying) return;
    const nextTrack = getNextTrackFor(activeTrackRef.current);
    if (!nextTrack || transitionActiveRef.current || !Number.isFinite(el.duration) || el.duration <= 0) return;
    const transitionLead = prefs.crossfadeEnabled ? clamp(prefs.crossfadeSeconds, 1, 12) : GAPLESS_LEAD_SECONDS;
    const remaining = el.duration - el.currentTime;
    if (remaining <= transitionLead + 0.02) {
      void beginTransitionToNext(nextTrack, transitionLead);
    }
  }, [beginTransitionToNext, currentTrack?.track_id, deckRefs, duration, getNextTrackFor, getVisibleDeckIndex, isPlaying, isSimpleMobilePlayback, prefs.crossfadeEnabled, prefs.crossfadeSeconds]);

  const handleDeckLoadedMetadata = useCallback((deckIndex: DeckIndex) => {
    const el = deckRefs[deckIndex].current;
    if (!el || deckIndex !== getVisibleDeckIndex() || Number.isNaN(el.duration)) return;
    setDuration(el.duration);
  }, [deckRefs, getVisibleDeckIndex]);

  const handleDeckProgress = useCallback((deckIndex: DeckIndex) => {
    const el = deckRefs[deckIndex].current;
    if (!el || deckIndex !== getVisibleDeckIndex() || el.buffered.length === 0) return;
    const end = el.buffered.end(el.buffered.length - 1);
    setBufferedEnd(end);
  }, [deckRefs, getVisibleDeckIndex]);

  const handleDeckPause = useCallback((deckIndex: DeckIndex) => {
    if (deckIndex !== getVisibleDeckIndex()) return;
    const elapsed = Date.now() - lastTrackLoadTimeRef.current;
    if (elapsed < 800) return;
    setIsPlaying(false);
  }, [getVisibleDeckIndex]);

  const cancelTransition = useCallback(() => {
    if (!transitionActiveRef.current) return;
    clearTransitionTimer();
    const oldDeck = activeDeckRef.current;
    const nextDeck = transitionTargetDeckRef.current;
    const oldEl = deckRefs[oldDeck].current;
    const nextEl = deckRefs[nextDeck].current;
    const oldTrack = activeTrackRef.current;
    transitionActiveRef.current = false;
    transitionTargetTrackRef.current = null;
    if (nextEl) {
      nextEl.pause();
      try {
        nextEl.currentTime = 0;
      } catch {
        // ignore
      }
    }
    setDeckGain(nextDeck, 0, 0.02);
    setDeckGain(oldDeck, 1, 0.02);
    if (oldEl) {
      setCurrentTime(oldEl.currentTime || 0);
      if (Number.isFinite(oldEl.duration) && oldEl.duration > 0) {
        setDuration(oldEl.duration);
      }
    } else {
      setCurrentTime(0);
      setDuration(oldTrack?.duration || 0);
    }
    setBufferedEnd(0);
    if (oldTrack && currentTrack?.track_id !== oldTrack.track_id) {
      onTrackSelect(oldTrack);
    }
  }, [clearTransitionTimer, currentTrack?.track_id, deckRefs, onTrackSelect, setDeckGain]);

  const handleDeckEnded = useCallback((deckIndex: DeckIndex) => {
    void (async () => {
      if (isSimpleMobilePlayback) {
        if (deckIndex !== 0) return;
        const nextTrackItem = getNextTrackFor(activeTrackRef.current);
        if (nextTrackItem) {
          const started = await startTrackOnDeck(0, nextTrackItem, { announceSelection: true, stopOtherDeck: true });
          if (!started) {
            onTrackSelect(nextTrackItem);
            setIsPlaying(false);
          }
          return;
        }
        finalizeTrack(activeTrackRef.current, Math.max(0, Math.floor(playedSecondsRef.current)), 'ended');
        setIsPlaying(false);
        return;
      }
      if (transitionActiveRef.current) return;
      if (deckIndex !== activeDeckRef.current) return;
      const nextTrack = getNextTrackFor(activeTrackRef.current);
      if (nextTrack) {
        const transitioned = await beginTransitionToNext(nextTrack, GAPLESS_LEAD_SECONDS);
        if (transitioned) return;
        // If gapless handoff cannot start, force a normal deck switch immediately.
        finalizeTrack(activeTrackRef.current, Math.max(0, Math.floor(playedSecondsRef.current)), 'ended');
        const forcedDeck = (activeDeckRef.current === 0 ? 1 : 0) as DeckIndex;
        const started = await startTrackOnDeck(forcedDeck, nextTrack, { announceSelection: true, stopOtherDeck: true });
        if (!started) {
          setIsPlaying(false);
        }
        return;
      }
      finalizeTrack(activeTrackRef.current, Math.max(0, Math.floor(playedSecondsRef.current)), 'ended');
      setIsPlaying(false);
    })();
  }, [beginTransitionToNext, finalizeTrack, getNextTrackFor, isSimpleMobilePlayback, onTrackSelect, startTrackOnDeck]);

  const toggleTrackLike = async () => {
    const track = currentTrack;
    if (!track) return;
    const next = !trackLiked;
    setTrackLiked(next);
    if (next) sendRecoEvent('like', track, playedSecondsRef.current);
    try {
      await api.setLike({ entity_type: 'track', entity_id: track.track_id, liked: next, source: 'ui_player' });
    } catch {
      setTrackLiked(!next);
    }
  };

  const dislikeTrack = async () => {
    const track = currentTrack;
    if (!track) return;
    setTrackLiked(false);
    sendRecoEvent('dislike', track, playedSecondsRef.current);
    try {
      await api.setLike({ entity_type: 'track', entity_id: track.track_id, liked: false, source: 'ui_player' });
    } catch {
      // ignore
    }
  };

  const handlePauseAll = useCallback(() => {
    if (transitionActiveRef.current) {
      cancelTransition();
    }
    stopDeck(0, false);
    stopDeck(1, false);
    setIsPlaying(false);
  }, [cancelTransition, stopDeck]);

  const handlePlayAll = useCallback(async () => {
    if (isSimpleMobilePlayback) {
      const el = deckARef.current;
      if (!el) return;
      el.volume = prefs.muted ? 0 : prefs.volume;
      await el.play().then(() => setIsPlaying(true)).catch(() => {
        setIsPlaying(false);
      });
      return;
    }
    const visibleDeck = getVisibleDeckIndex();
    const visibleEl = deckRefs[visibleDeck].current;
    if (!visibleEl) return;
    await ensureAudioGraph();
    if (transitionActiveRef.current) {
      const oldDeck = activeDeckRef.current;
      const oldEl = deckRefs[oldDeck].current;
      if (oldEl?.paused) {
        await Promise.allSettled([
          oldEl.play(),
          visibleEl.play(),
        ]);
      } else {
        await visibleEl.play().catch(() => {});
      }
      setIsPlaying(true);
      return;
    }
    const hiddenDeck = (visibleDeck === 0 ? 1 : 0) as DeckIndex;
    visibleEl.muted = false;
    visibleEl.volume = 1;
    if (!transitionActiveRef.current) {
      setDeckGain(hiddenDeck, 0, 0.02);
    }
    setDeckGain(visibleDeck, 1, 0.02);
    await visibleEl.play().catch(() => {
      setIsPlaying(false);
    });
    if (!visibleEl.paused) {
      setIsPlaying(true);
    }
  }, [deckRefs, ensureAudioGraph, getVisibleDeckIndex, isSimpleMobilePlayback, prefs.muted, prefs.volume]);

  const togglePlayPause = useCallback(() => {
    if (isPlaying) {
      handlePauseAll();
      return;
    }
    void handlePlayAll();
  }, [handlePauseAll, handlePlayAll, isPlaying]);

  const handleSeek = useCallback((value: number[]) => {
    const target = value[0];
    const visibleDeck = getVisibleDeckIndex();
    const el = deckRefs[visibleDeck].current;
    if (!el) return;
    try {
      el.currentTime = target;
    } catch {
      // ignore
    }
    setCurrentTime(target);
    if (!transitionActiveRef.current) {
      playedSecondsRef.current = target;
    }
  }, [deckRefs, getVisibleDeckIndex]);

  const updateVolume = useCallback((nextVolume: number, forceMuted?: boolean) => {
    setPrefs((prev) => ({
      ...prev,
      volume: clamp(nextVolume, 0, 1),
      muted: typeof forceMuted === 'boolean' ? forceMuted : nextVolume === 0 ? true : prev.muted,
    }));
  }, []);

  const handleVolumeChange = useCallback((value: number[]) => {
    const nextVolume = clamp(value[0] ?? prefs.volume, 0, 1);
    updateVolume(nextVolume, nextVolume === 0);
  }, [prefs.volume, updateVolume]);

  const toggleMute = useCallback(() => {
    setPrefs((prev) => ({ ...prev, muted: !prev.muted }));
  }, []);

  const prevTrack = useCallback(() => {
    if (currentIndex > 0) {
      onTrackSelect(tracks[currentIndex - 1]);
    }
  }, [currentIndex, onTrackSelect, tracks]);

  const nextTrack = useCallback(() => {
    if (currentIndex >= 0 && currentIndex < tracks.length - 1) {
      onTrackSelect(tracks[currentIndex + 1]);
    }
  }, [currentIndex, onTrackSelect, tracks]);

  const handleClosePlayer = useCallback(() => {
    if (activeTrackRef.current) {
      finalizeTrack(activeTrackRef.current, Math.max(0, Math.floor(playedSecondsRef.current)), 'close');
    }
    stopDeck(0, true);
    stopDeck(1, true);
    clearTransitionTimer();
    onClose();
  }, [clearTransitionTimer, finalizeTrack, onClose, stopDeck]);

  const scrollToTracks = useCallback(() => {
    tracksSectionRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }, []);

  useEffect(() => {
    if (!isSimpleMobilePlayback) return;
    const activeEl = deckARef.current;
    if (!activeEl) return;
    activeEl.volume = prefs.muted ? 0 : prefs.volume;
  }, [isSimpleMobilePlayback, prefs.muted, prefs.volume]);

  useEffect(() => {
    if (typeof navigator === 'undefined' || !('mediaSession' in navigator)) return;
    const mediaSession = navigator.mediaSession;
    const artworkSrc = normalizePmdaAssetUrl(displayAlbumThumb);
    try {
      mediaSession.metadata = new MediaMetadata({
        title: currentTrack?.title || displayAlbumTitle || 'PMDA',
        artist: currentTrack?.artist || '',
        album: displayAlbumTitle || '',
        artwork: artworkSrc
          ? [
              { src: artworkSrc, sizes: '96x96' },
              { src: artworkSrc, sizes: '192x192' },
              { src: artworkSrc, sizes: '512x512' },
            ]
          : [],
      });
    } catch {
      // ignore unsupported metadata surfaces
    }
    const safeSetActionHandler = (action: MediaSessionAction, handler: MediaSessionActionHandler | null) => {
      try {
        mediaSession.setActionHandler(action, handler);
      } catch {
        // ignore unsupported actions
      }
    };
    safeSetActionHandler('play', () => { void handlePlayAll(); });
    safeSetActionHandler('pause', handlePauseAll);
    safeSetActionHandler('previoustrack', currentIndex > 0 ? prevTrack : null);
    safeSetActionHandler('nexttrack', currentIndex >= 0 && currentIndex < tracks.length - 1 ? nextTrack : null);
    safeSetActionHandler('seekbackward', () => handleSeek([Math.max(0, currentTime - 10)]));
    safeSetActionHandler('seekforward', () => handleSeek([Math.min(Math.max(1, displayDuration), currentTime + 10)]));
    safeSetActionHandler('seekto', (details) => {
      const position = Number(details?.seekTime);
      if (Number.isFinite(position)) {
        handleSeek([Math.min(Math.max(0, position), Math.max(1, displayDuration))]);
      }
    });
    return () => {
      safeSetActionHandler('play', null);
      safeSetActionHandler('pause', null);
      safeSetActionHandler('previoustrack', null);
      safeSetActionHandler('nexttrack', null);
      safeSetActionHandler('seekbackward', null);
      safeSetActionHandler('seekforward', null);
      safeSetActionHandler('seekto', null);
    };
  }, [
    currentIndex,
    currentTime,
    currentTrack?.artist,
    currentTrack?.title,
    displayAlbumThumb,
    displayAlbumTitle,
    displayDuration,
    handlePauseAll,
    handlePlayAll,
    handleSeek,
    nextTrack,
    prevTrack,
    tracks.length,
  ]);

  useEffect(() => {
    if (typeof navigator === 'undefined' || !('mediaSession' in navigator)) return;
    try {
      navigator.mediaSession.playbackState = isPlaying ? 'playing' : 'paused';
    } catch {
      // ignore
    }
  }, [isPlaying]);

  useEffect(() => {
    if (typeof navigator === 'undefined' || !('mediaSession' in navigator)) return;
    const mediaSession = navigator.mediaSession as MediaSession & {
      setPositionState?: (state?: MediaPositionState) => void;
    };
    if (typeof mediaSession.setPositionState !== 'function') return;
    try {
      if (displayDuration > 0) {
        mediaSession.setPositionState({
          duration: displayDuration,
          playbackRate: 1,
          position: Math.min(currentTime, displayDuration),
        });
      } else {
        mediaSession.setPositionState();
      }
    } catch {
      // ignore
    }
  }, [currentTime, displayDuration]);

  const handleEqPresetChange = useCallback((preset: string) => {
    const nextPreset = EQ_PRESETS[preset] ? preset : 'flat';
    setPrefs((prev) => ({
      ...prev,
      eqPreset: nextPreset,
      eqGains: EQ_PRESETS[nextPreset].gains.slice(),
    }));
  }, []);

  const handleEqBandChange = useCallback((bandIndex: number, nextGain: number) => {
    setPrefs((prev) => {
      const next = prev.eqGains.slice();
      next[bandIndex] = clamp(nextGain, MIN_EQ_GAIN, MAX_EQ_GAIN);
      return {
        ...prev,
        eqPreset: 'custom',
        eqGains: next,
      };
    });
  }, []);

  const handleResetEq = useCallback(() => {
    setPrefs((prev) => ({
      ...prev,
      eqPreset: 'flat',
      eqGains: EQ_PRESETS.flat.gains.slice(),
      eqEnabled: false,
    }));
  }, []);

  if (tracks.length === 0) return null;

  const footerFxPanel = (
    <PlaybackFxPanel
      prefs={prefs}
      compact
      isLightTheme={false}
      onCrossfadeEnabledChange={(next) => setPrefs((prev) => ({ ...prev, crossfadeEnabled: next }))}
      onCrossfadeSecondsChange={(next) => setPrefs((prev) => ({ ...prev, crossfadeSeconds: clamp(next, 1, 12) }))}
      onEqEnabledChange={(next) => setPrefs((prev) => ({ ...prev, eqEnabled: next }))}
      onEqPresetChange={handleEqPresetChange}
      onEqBandChange={handleEqBandChange}
      onResetEq={handleResetEq}
    />
  );

  return (
    <>
      <audio
        ref={deckARef}
        preload="auto"
        onTimeUpdate={() => handleDeckTimeUpdate(0)}
        onLoadedMetadata={() => handleDeckLoadedMetadata(0)}
        onProgress={() => handleDeckProgress(0)}
        onEnded={() => handleDeckEnded(0)}
        onPlay={() => { if (getVisibleDeckIndex() === 0) setIsPlaying(true); }}
        onPause={() => handleDeckPause(0)}
      />
      <audio
        ref={deckBRef}
        preload="auto"
        onTimeUpdate={() => handleDeckTimeUpdate(1)}
        onLoadedMetadata={() => handleDeckLoadedMetadata(1)}
        onProgress={() => handleDeckProgress(1)}
        onEnded={() => handleDeckEnded(1)}
        onPlay={() => { if (getVisibleDeckIndex() === 1) setIsPlaying(true); }}
        onPause={() => handleDeckPause(1)}
      />

      <div className={cn('fixed bottom-0 left-0 right-0 z-50 border-t pmda-player-bar shadow-[0_-4px_20px_rgba(0,0,0,0.3)]', isMobile && showNowPlaying && 'hidden')}>
        <div
          className="relative h-1 w-full cursor-pointer group"
          style={{ background: 'hsl(var(--player-surface))' }}
          onClick={(e) => {
            if (displayDuration <= 0) return;
            const rect = e.currentTarget.getBoundingClientRect();
            const pct = (e.clientX - rect.left) / rect.width;
            const t = clamp(pct, 0, 1) * displayDuration;
            handleSeek([t]);
          }}
        >
          <div className="absolute inset-y-0 left-0 transition-all" style={{ width: `${displayDuration > 0 ? (bufferedEnd / displayDuration) * 100 : 0}%`, background: 'hsl(var(--player-text-muted) / 0.3)' }} />
          <div className="absolute inset-y-0 left-0 bg-primary transition-all" style={{ width: `${displayDuration > 0 ? (currentTime / displayDuration) * 100 : 0}%` }} />
        </div>

        {showFxPanel && !isSimpleMobilePlayback ? (
          <div className="border-b px-4 py-4" style={{ borderColor: 'hsl(var(--player-border))' }}>
            {footerFxPanel}
          </div>
        ) : null}

        <div className="flex items-center gap-4 px-4 py-2">
          <div className="flex min-w-0 flex-1 items-center gap-3">
            <button
              type="button"
              className="h-14 w-14 shrink-0 overflow-hidden rounded focus:outline-none focus:ring-2 focus:ring-primary/60"
              style={{ background: 'hsl(var(--player-surface))' }}
              onClick={() => setShowNowPlaying(true)}
              title="Open Now Playing"
              draggable
              onDragStart={(e) => {
                try {
                  e.dataTransfer.setData('application/x-pmda-album', JSON.stringify({ album_id: resolvedAlbumId }));
                  e.dataTransfer.setData('text/plain', displayAlbumTitle || 'Album');
                  e.dataTransfer.effectAllowed = 'copy';
                } catch {
                  // ignore
                }
              }}
            >
              {(displayAlbumThumb && !coverError) ? (
                <img src={displayAlbumThumb} alt="" className="h-full w-full object-cover" onError={() => setCoverError(true)} />
              ) : (
                <div className="flex h-full w-full items-center justify-center" style={{ color: 'hsl(var(--player-text-muted))' }}>
                  <Music className="h-7 w-7" />
                </div>
              )}
            </button>
            <button
              type="button"
              className="min-w-0 flex-1 rounded text-left focus:outline-none focus:ring-2 focus:ring-primary/60"
              onClick={() => setShowNowPlaying(true)}
              title="Open Now Playing"
            >
              <p className="truncate text-sm font-medium">{currentTrack?.title ?? '—'}</p>
              <p className="truncate text-xs" style={{ color: 'hsl(var(--player-text-muted))' }}>{currentTrack?.artist} · {displayAlbumTitle}</p>
            </button>
          </div>

          <div className="flex items-center gap-2">
            <Button variant="ghost" size="icon" className="h-9 w-9 hover:bg-accent/40" onClick={prevTrack} disabled={currentIndex <= 0}>
              <SkipBack className="h-5 w-5" />
            </Button>
            <Button variant="ghost" size="icon" className="h-10 w-10 hover:bg-accent/40" onClick={togglePlayPause}>
              {isPlaying ? <Pause className="h-6 w-6 fill-current" /> : <Play className="h-6 w-6 fill-current" />}
            </Button>
            <Button variant="ghost" size="icon" className="h-9 w-9 hover:bg-accent/40" onClick={nextTrack} disabled={currentIndex < 0 || currentIndex >= tracks.length - 1}>
              <SkipForward className="h-5 w-5" />
            </Button>
          </div>

          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="icon"
              className={cn('h-8 w-8 hover:bg-accent/40', trackLiked ? 'text-primary' : '')}
              onClick={() => void toggleTrackLike()}
              title={trackLiked ? 'Liked' : 'Like'}
            >
              <ThumbsUp className={cn('h-4 w-4', trackLiked ? 'fill-current' : '')} />
            </Button>
            <Button variant="ghost" size="icon" className="h-8 w-8 hover:bg-accent/40" onClick={() => void dislikeTrack()} title="Dislike">
              <ThumbsDown className="h-4 w-4" />
            </Button>
          </div>

          <div className={cn('min-w-[100px] justify-center text-xs tabular-nums flex items-center gap-1', isMobile && 'hidden sm:flex')} style={{ color: 'hsl(var(--player-text-muted))' }}>
            <span>{formatDuration(currentTime)}</span>
            <span>/</span>
            <span>{formatDuration(displayDuration)}</span>
          </div>

          <div className={cn('flex w-28 items-center gap-1', isMobile && 'hidden')}>
            <Button variant="ghost" size="icon" className="h-8 w-8 shrink-0 hover:bg-accent/40" onClick={toggleMute}>
              {prefs.muted || prefs.volume === 0 ? <VolumeX className="h-4 w-4" /> : <Volume2 className="h-4 w-4" />}
            </Button>
            <Slider value={[prefs.muted ? 0 : prefs.volume]} max={1} step={0.05} onValueChange={handleVolumeChange} className="w-full" />
          </div>

          {!isSimpleMobilePlayback ? (
            <Button variant="ghost" size="icon" className={cn('h-8 w-8 hover:bg-accent/40', showFxPanel ? 'bg-accent/40' : '')} onClick={() => setShowFxPanel((value) => !value)} title="Audio effects">
              <SlidersHorizontal className="h-4 w-4" />
            </Button>
          ) : null}
          <Button variant="ghost" size="icon" className="h-8 w-8 hover:bg-accent/40" onClick={() => setShowList((s) => !s)} title="Track list">
            <List className="h-4 w-4" />
          </Button>
          <Button variant="ghost" size="icon" className="h-8 w-8 hover:bg-accent/40" onClick={() => setShowNowPlaying(true)} title="Now Playing">
            <Maximize2 className="h-4 w-4" />
          </Button>
          <Button variant="ghost" size="icon" className="h-8 w-8 hover:bg-accent/40" onClick={handleClosePlayer} title="Close">
            <X className="h-4 w-4" />
          </Button>
        </div>

        {showList ? (
          <div className="border-t" style={{ borderColor: 'hsl(var(--player-border))', background: 'hsl(var(--player-surface) / 0.5)' }}>
            <ScrollArea className="h-[min(60vh,560px)]">
              <div className="space-y-0.5 px-4 py-3 pr-4">
                {tracks.map((track) => (
                  <button
                    key={track.track_id}
                    type="button"
                    onClick={() => {
                      onTrackSelect(track);
                    }}
                    draggable
                    onDragStart={(e) => {
                      try {
                        e.dataTransfer.setData('application/x-pmda-track', JSON.stringify({ track_id: track.track_id }));
                        e.dataTransfer.setData('text/plain', track.title);
                        e.dataTransfer.effectAllowed = 'copy';
                      } catch {
                        // ignore
                      }
                    }}
                    className={cn(
                      'flex w-full cursor-grab items-center gap-2 rounded px-2 py-1.5 text-left text-sm transition-colors active:cursor-grabbing hover:bg-accent/40',
                      currentTrack?.track_id === track.track_id && 'bg-accent/40 font-medium'
                    )}
                  >
                    <GripVertical className="h-4 w-4" style={{ color: 'hsl(var(--player-text-muted) / 0.5)' }} />
                    <span className="w-6 shrink-0" style={{ color: 'hsl(var(--player-text-muted))' }}>{track.index}</span>
                    <span className="min-w-0 flex-1 truncate">{track.title}</span>
                    <span className="shrink-0 text-xs tabular-nums" style={{ color: 'hsl(var(--player-text-muted) / 0.7)' }}>{formatDuration(track.duration)}</span>
                  </button>
                ))}
              </div>
            </ScrollArea>
          </div>
        ) : null}
      </div>

      <Dialog open={showNowPlaying} onOpenChange={setShowNowPlaying}>
        <DialogContent className={cn('max-w-none overflow-y-auto overflow-x-hidden rounded-none border-0 p-0 [&>button]:hidden', isMobile ? 'z-[90] !left-0 !top-0 !h-[100dvh] !w-screen !max-w-screen !translate-x-0 !translate-y-0 overscroll-contain' : 'h-[100dvh] w-[100dvw]')}>
          <div className="relative min-h-full overflow-x-hidden text-foreground">
            <div className={cn('absolute inset-0', isLightTheme ? 'bg-background' : 'bg-background')} />
            {(displayAlbumThumb && !coverError) ? (
              <img
                src={displayAlbumThumb}
                alt=""
                className={cn('absolute inset-0 h-full w-full scale-110 object-cover blur-3xl', isLightTheme ? 'opacity-10' : 'opacity-20')}
                onError={() => setCoverError(true)}
              />
            ) : null}
            <div className={cn('absolute inset-0', isLightTheme ? 'bg-gradient-to-b from-background/50 via-background/85 to-background' : 'bg-gradient-to-b from-background/20 via-background/65 to-background')} />

            <div className="relative z-10 flex h-full flex-col">
              <div className={cn('flex items-center justify-between gap-3 border-b px-4 backdrop-blur-sm md:px-6', isMobile ? 'py-2.5' : 'py-3', 'border-border/60 bg-card/60')}>
                {isMobile ? (
                  <>
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="h-10 w-10 rounded-full"
                      onClick={() => setShowNowPlaying(false)}
                      title="Back to mini player"
                    >
                      <ChevronLeft className="h-5 w-5" />
                    </Button>
                    <DialogHeader className="min-w-0 flex-1 space-y-0 text-center">
                      <DialogTitle className={cn(isMobile ? 'text-lg font-semibold' : 'text-xl font-semibold')}>Now Playing</DialogTitle>
                      <DialogDescription className="sr-only">
                        Full-screen player showing the current track and the queued track list.
                      </DialogDescription>
                    </DialogHeader>
                    <div className="w-10 shrink-0" />
                  </>
                ) : (
                  <>
                    <DialogHeader className="space-y-0">
                      <DialogTitle>Now Playing</DialogTitle>
                      <DialogDescription className="sr-only">
                        Full-screen player showing the current track and the queued track list.
                      </DialogDescription>
                    </DialogHeader>
                    <div className="flex items-center gap-2">
                      <Badge className="hidden sm:inline-flex" variant="outline">
                        {tracks.length} tracks
                      </Badge>
                      <Button type="button" variant="ghost" size="icon" className="h-9 w-9" onClick={() => setShowNowPlaying(false)} title="Collapse">
                        <ChevronDown className="h-5 w-5" />
                      </Button>
                    </div>
                  </>
                )}
              </div>

              <div className={cn('flex min-h-0 flex-1 flex-col', isMobile ? 'overflow-y-auto overflow-x-hidden overscroll-contain' : 'overflow-hidden')}>
                <div className={cn('flex flex-col items-center md:p-10', isMobile ? 'gap-4 px-5 pb-5 pt-3' : 'gap-6 p-6')}>
                  <div className={cn('relative aspect-square overflow-hidden rounded-3xl border shadow-[0_20px_72px_rgba(0,0,0,0.28)]', isMobile ? 'w-[min(64vw,300px)] self-center' : 'w-[min(82vw,520px)]', 'border-border/60 bg-card')}>
                    {(displayAlbumThumb && !coverError) ? (
                      <img src={displayAlbumThumb} alt="" className="absolute inset-0 h-full w-full animate-in object-cover fade-in-0 duration-300" />
                    ) : (
                      <div className="absolute inset-0 flex items-center justify-center">
                        <Music className="h-20 w-20 text-muted-foreground/70" />
                      </div>
                    )}
                    <div className="absolute inset-0 bg-gradient-to-t from-foreground/30 via-foreground/8 to-transparent" />
                  </div>

                  <div className={cn('space-y-1 text-center', isMobile ? 'w-full max-w-[92vw] overflow-hidden' : 'max-w-[860px]')}>
                    <div className={cn(isMobile ? 'text-base' : 'truncate text-sm', 'text-muted-foreground')}>{currentTrack?.artist ?? ''}</div>
                    {isMobile ? (
                      <div ref={mobileTitleViewportRef} className="overflow-hidden">
                        <div className={cn('inline-flex min-w-full items-center justify-center whitespace-nowrap font-semibold tracking-tight', mobileTitleNeedsMarquee ? 'pmda-mobile-marquee' : '')}>
                          <span ref={mobileTitleTextRef} className="shrink-0 text-[1.9rem] leading-[1.1]">{currentTrack?.title ?? '—'}</span>
                          {mobileTitleNeedsMarquee ? <span aria-hidden className="shrink-0 pl-10 text-[1.9rem] leading-[1.1]">{currentTrack?.title ?? '—'}</span> : null}
                        </div>
                      </div>
                    ) : (
                      <div className="text-balance font-semibold tracking-tight truncate text-3xl md:text-4xl">
                        {currentTrack?.title ?? '—'}
                      </div>
                    )}
                    <div className={cn(isMobile ? 'text-sm leading-5 line-clamp-2' : 'truncate text-sm', 'text-muted-foreground')}>{displayAlbumTitle}</div>
                    {albumMeta ? (
                      <div className={cn('mt-2 flex max-w-[920px] flex-col items-center', isMobile ? 'gap-1.5' : 'gap-2')}>
                        {!isMobile ? <div className="text-[11px] uppercase tracking-[0.24em] text-muted-foreground">Album metadata</div> : null}
                        <div className="flex flex-wrap items-center justify-center gap-2">
                          {isMobile ? primaryMobileBadges : (
                            <>
                              {releaseYear ? <Badge variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('year'))}>{releaseYear}</Badge> : null}
                              {albumMeta.total_duration_sec > 0 ? <Badge variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('duration'))}>{formatDuration(albumMeta.total_duration_sec)}</Badge> : null}
                              {albumMeta.format ? <FormatBadge format={albumMeta.format} size="sm" className="h-7 rounded-full px-3 py-1 text-xs" /> : null}
                              {(() => {
                                const audioBadgeText = formatAudioSpec(albumMeta.bit_depth, albumMeta.sample_rate) || (albumMeta.is_lossless ? '' : 'Lossy');
                                if (!audioBadgeText) return null;
                                return (
                                  <Badge variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass(albumMeta.is_lossless ? 'lossless' : 'lossy'))}>
                                    {audioBadgeText}
                                  </Badge>
                                );
                              })()}
                              {trackCountText ? <Badge variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('count'))}>{trackCountText}</Badge> : null}
                              {discCountText ? <Badge variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('count'))}>{discCountText}</Badge> : null}
                              {labelText ? <Badge variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('label'))}>{`Label: ${labelText}`}</Badge> : null}
                            </>
                          )}
                        </div>
                        {isMobile ? (
                          secondaryMobileMeta ? <div className="max-w-[92vw] text-center text-xs leading-5 text-muted-foreground/80">{secondaryMobileMeta}</div> : null
                        ) : genres.length > 0 ? (
                          <div className="flex flex-wrap items-center justify-center gap-2">
                            {genres.map((genre) => (
                              <Badge key={genre} variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('genre'))}>
                                {genre}
                              </Badge>
                            ))}
                          </div>
                        ) : null}
                      </div>
                    ) : null}
                  </div>

                  <div className={cn('w-full max-w-[780px] space-y-2', isMobile ? 'pt-1' : '')}>
                    <Slider
                      value={[currentTime]}
                      max={Math.max(1, displayDuration)}
                      step={1}
                      onValueChange={handleSeek}
                      className="w-full [&_[data-orientation=horizontal]]:bg-muted"
                    />
                    <div className="flex items-center justify-between text-xs tabular-nums text-muted-foreground">
                      <span>{formatDuration(currentTime)}</span>
                      <span>{formatDuration(displayDuration)}</span>
                    </div>
                  </div>

                  <div className={cn('flex flex-wrap items-center justify-center', isMobile ? 'gap-2.5' : 'gap-3')}>
                    <Button variant="ghost" size="icon" className="h-11 w-11 rounded-full" onClick={prevTrack} disabled={currentIndex <= 0} title="Previous">
                      <SkipBack className="h-5 w-5" />
                    </Button>
                    <Button
                      variant="secondary"
                      size="icon"
                      className="h-16 w-16 rounded-full border border-border/60"
                      onClick={togglePlayPause}
                      title={isPlaying ? 'Pause' : 'Play'}
                    >
                      {isPlaying ? <Pause className="h-8 w-8 fill-current" /> : <Play className="h-8 w-8 fill-current" />}
                    </Button>
                    <Button variant="ghost" size="icon" className="h-11 w-11 rounded-full" onClick={nextTrack} disabled={currentIndex < 0 || currentIndex >= tracks.length - 1} title="Next">
                      <SkipForward className="h-5 w-5" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      className={cn('h-10 w-10 rounded-full', trackLiked ? 'text-primary' : '')}
                      onClick={() => void toggleTrackLike()}
                      title={trackLiked ? 'Liked' : 'Like'}
                    >
                      <ThumbsUp className={cn('h-5 w-5', trackLiked ? 'fill-current' : '')} />
                    </Button>
                    <Button variant="ghost" size="icon" className="h-10 w-10 rounded-full" onClick={() => void dislikeTrack()} title="Dislike">
                      <ThumbsDown className="h-5 w-5" />
                    </Button>
                    <Button variant="ghost" size="icon" className={cn('h-10 w-10 rounded-full', showFxPanel ? 'bg-accent' : '')} onClick={() => setShowFxPanel((value) => !value)} title="Audio effects">
                      <SlidersHorizontal className="h-5 w-5" />
                    </Button>
                  </div>

                  {isMobile ? (
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="mt-1 h-9 gap-2 rounded-full px-4"
                      onClick={scrollToTracks}
                    >
                      <ChevronDown className="h-4 w-4" />
                      Tracks
                    </Button>
                  ) : null}

                  {showFxPanel && !isSimpleMobilePlayback ? (
                    <div className="w-full max-w-[980px]">
                      <PlaybackFxPanel
                        prefs={prefs}
                        isLightTheme={isLightTheme}
                        onCrossfadeEnabledChange={(next) => setPrefs((prev) => ({ ...prev, crossfadeEnabled: next }))}
                        onCrossfadeSecondsChange={(next) => setPrefs((prev) => ({ ...prev, crossfadeSeconds: clamp(next, 1, 12) }))}
                        onEqEnabledChange={(next) => setPrefs((prev) => ({ ...prev, eqEnabled: next }))}
                        onEqPresetChange={handleEqPresetChange}
                        onEqBandChange={handleEqBandChange}
                        onResetEq={handleResetEq}
                      />
                    </div>
                  ) : null}
                </div>

                <div ref={tracksSectionRef} className="flex min-h-0 flex-1 flex-col border-t border-border/60 bg-card/50">
                  <div className="flex items-center justify-between gap-3 px-4 py-3 md:px-6">
                    <div className="text-sm font-medium">Tracks</div>
                    <div className="text-xs tabular-nums text-muted-foreground">
                      {currentIndex >= 0 ? `${currentIndex + 1}/${tracks.length}` : `${tracks.length}`}
                    </div>
                  </div>
                  <ScrollArea className={cn('min-h-0 flex-1', isMobile ? 'max-h-none' : '')}>
                    <div className="space-y-1 px-4 pb-6 md:px-6">
                      {tracks.map((track) => (
                        <button
                          key={track.track_id}
                          type="button"
                          onClick={() => {
                            onTrackSelect(track);
                          }}
                          draggable
                          onDragStart={(e) => {
                            try {
                              e.dataTransfer.setData('application/x-pmda-track', JSON.stringify({ track_id: track.track_id }));
                              e.dataTransfer.setData('text/plain', track.title);
                              e.dataTransfer.effectAllowed = 'copy';
                            } catch {
                              // ignore
                            }
                          }}
                          className={cn(
                            'flex w-full cursor-grab items-center gap-3 rounded-xl px-3 py-2 text-left text-sm transition-colors active:cursor-grabbing hover:bg-accent/50',
                            currentTrack?.track_id === track.track_id && 'bg-accent font-medium'
                          )}
                        >
                          <GripVertical className="h-4 w-4 shrink-0 text-muted-foreground/50" />
                          <span className="w-10 shrink-0 tabular-nums text-muted-foreground">{track.index}</span>
                          <span className="min-w-0 flex-1 truncate">{track.title}</span>
                          <span className="shrink-0 text-xs tabular-nums text-muted-foreground">{formatDuration(track.duration)}</span>
                        </button>
                      ))}
                    </div>
                  </ScrollArea>
                </div>
              </div>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}
