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
          ? isLightTheme
            ? 'border-border/70 bg-background/90'
            : 'border-white/10 bg-zinc-900/95'
          : isLightTheme
            ? 'border-border/70 bg-white/80'
            : 'border-white/10 bg-black/30'
      )}
    >
      <div className="grid gap-4 lg:grid-cols-[1.05fr_1.45fr]">
        <div className="space-y-4">
          <div className="space-y-1.5">
            <div className={cn('text-[11px] uppercase tracking-[0.22em]', isLightTheme ? 'text-muted-foreground/80' : 'text-white/45')}>
              Playback
            </div>
            <div className="flex items-center justify-between gap-3 rounded-xl border border-border/40 px-3 py-3">
              <div className="space-y-1">
                <div className="text-sm font-medium">Gapless playback</div>
                <div className={cn('text-xs', isLightTheme ? 'text-muted-foreground' : 'text-white/60')}>
                  Always on. PMDA preloads the next track for seamless transitions.
                </div>
              </div>
              <Badge className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('source'))}>On</Badge>
            </div>
            <div className="rounded-xl border border-border/40 px-3 py-3">
              <div className="flex items-center justify-between gap-3">
                <div className="space-y-1">
                  <div className="text-sm font-medium">Crossfade</div>
                  <div className={cn('text-xs', isLightTheme ? 'text-muted-foreground' : 'text-white/60')}>
                    Blend the outgoing and incoming tracks instead of a hard hand-off.
                  </div>
                </div>
                <Switch checked={prefs.crossfadeEnabled} onCheckedChange={onCrossfadeEnabledChange} />
              </div>
              <div className="mt-3 space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <span className={cn('text-xs uppercase tracking-[0.18em]', isLightTheme ? 'text-muted-foreground/80' : 'text-white/45')}>
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
              <div className={cn('text-[11px] uppercase tracking-[0.22em]', isLightTheme ? 'text-muted-foreground/80' : 'text-white/45')}>
                Equalizer
              </div>
              <div className={cn('text-xs', isLightTheme ? 'text-muted-foreground' : 'text-white/60')}>
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
  const lastSeenTrackIdRef = useRef<number | null>(null);
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

  useEffect(() => {
    savePlayerPrefs(prefs);
  }, [prefs]);

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
  }, [prefs.eqEnabled, prefs.eqGains, prefs.muted, prefs.volume]);

  useEffect(() => {
    const ctx = audioContextRef.current;
    const master = masterGainRef.current;
    if (!ctx || !master) return;
    const now = ctx.currentTime;
    master.gain.cancelScheduledValues(now);
    master.gain.setValueAtTime(master.gain.value, now);
    master.gain.linearRampToValueAtTime(prefs.muted ? 0 : prefs.volume, now + 0.04);
  }, [prefs.muted, prefs.volume]);

  useEffect(() => {
    const ctx = audioContextRef.current;
    const filters = eqFiltersRef.current;
    if (!ctx || filters.length === 0) return;
    const now = ctx.currentTime;
    filters.forEach((filter, index) => {
      const gain = prefs.eqEnabled ? (prefs.eqGains[index] ?? 0) : 0;
      filter.gain.cancelScheduledValues(now);
      filter.gain.setValueAtTime(gain, now);
    });
  }, [prefs.eqEnabled, prefs.eqGains]);

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
    const activeTrack = transitionActiveRef.current ? (transitionTargetTrackRef.current || activeTrackRef.current) : activeTrackRef.current;
    const nextTrack = getNextTrackFor(activeTrack);
    const preloadDeck = (activeDeckRef.current === 0 ? 1 : 0) as DeckIndex;
    if (!nextTrack) {
      if (!transitionActiveRef.current) assignTrackToDeck(preloadDeck, null);
      return;
    }
    if (getDeckTrack(preloadDeck)?.track_id === nextTrack.track_id) return;
    preloadDeckForTrack(preloadDeck, nextTrack);
  }, [assignTrackToDeck, getDeckTrack, getNextTrackFor, preloadDeckForTrack]);

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
      setIsPlaying(Boolean(nextEl && !nextEl.paused));
    }
  }, [clearTransitionTimer, deckRefs, finalizeTrack]);

  const beginTransitionToNext = useCallback(async (nextTrack: TrackInfo, fadeSeconds: number) => {
    if (transitionActiveRef.current) return;
    await ensureAudioGraph();
    const oldDeck = activeDeckRef.current;
    const nextDeck = (oldDeck === 0 ? 1 : 0) as DeckIndex;
    const oldEl = deckRefs[oldDeck].current;
    const nextEl = deckRefs[nextDeck].current;
    if (!oldEl || !nextEl) return;
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
      return;
    }
    const effectiveFade = Math.max(0.04, fadeSeconds);
    setDeckGain(nextDeck, 1, effectiveFade);
    setDeckGain(oldDeck, 0, effectiveFade);
    onTrackSelect(nextTrack);
    transitionTimerRef.current = window.setTimeout(() => {
      commitTransition(false);
      preloadUpcomingTrack();
    }, Math.ceil(effectiveFade * 1000) + 40);
  }, [commitTransition, deckRefs, ensureAudioGraph, onTrackSelect, preloadDeckForTrack, preloadUpcomingTrack, sendPlayStart, setDeckGain]);

  const syncToCurrentTrack = useCallback(async (track: TrackInfo | null) => {
    if (!track) return;
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
    const targetEl = deckRefs[targetDeck].current;
    if (!targetEl) return;
    targetEl.preload = 'auto';
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
    } catch {
      setIsPlaying(false);
    }
    preloadUpcomingTrack();
  }, [
    assignTrackToDeck,
    clearTransitionTimer,
    deckRefs,
    ensureAudioGraph,
    finalizeTrack,
    getDeckTrack,
    preloadUpcomingTrack,
    sendPlayStart,
    setDeckGain,
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
    const nextTrackId = Number(currentTrack?.track_id || 0) || null;
    const prevTrackId = lastSeenTrackIdRef.current;
    if (isMobile && nextTrackId && prevTrackId == null) {
      setShowNowPlaying(true);
    }
    lastSeenTrackIdRef.current = nextTrackId;
  }, [currentTrack?.track_id, isMobile]);

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
    const nextTrack = getNextTrackFor(activeTrackRef.current);
    if (!nextTrack || transitionActiveRef.current || !Number.isFinite(el.duration) || el.duration <= 0) return;
    const transitionLead = prefs.crossfadeEnabled ? clamp(prefs.crossfadeSeconds, 1, 12) : GAPLESS_LEAD_SECONDS;
    const remaining = el.duration - el.currentTime;
    if (remaining <= transitionLead + 0.02) {
      void beginTransitionToNext(nextTrack, transitionLead);
    }
  }, [beginTransitionToNext, currentTrack?.track_id, deckRefs, duration, getNextTrackFor, getVisibleDeckIndex, prefs.crossfadeEnabled, prefs.crossfadeSeconds]);

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
    if (transitionActiveRef.current) return;
    if (deckIndex !== activeDeckRef.current) return;
    const nextTrack = getNextTrackFor(activeTrackRef.current);
    if (nextTrack) {
      void beginTransitionToNext(nextTrack, GAPLESS_LEAD_SECONDS);
      return;
    }
    finalizeTrack(activeTrackRef.current, Math.max(0, Math.floor(playedSecondsRef.current)), 'ended');
    setIsPlaying(false);
  }, [beginTransitionToNext, finalizeTrack, getNextTrackFor]);

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
    await visibleEl.play().catch(() => {
      setIsPlaying(false);
    });
  }, [deckRefs, ensureAudioGraph, getVisibleDeckIndex]);

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

      <div className="fixed bottom-0 left-0 right-0 z-50 border-t bg-zinc-900 text-white shadow-[0_-4px_20px_rgba(0,0,0,0.3)]">
        <div
          className="relative h-1 w-full cursor-pointer bg-zinc-700 group"
          onClick={(e) => {
            if (displayDuration <= 0) return;
            const rect = e.currentTarget.getBoundingClientRect();
            const pct = (e.clientX - rect.left) / rect.width;
            const t = clamp(pct, 0, 1) * displayDuration;
            handleSeek([t]);
          }}
        >
          <div className="absolute inset-y-0 left-0 bg-zinc-500 transition-all" style={{ width: `${displayDuration > 0 ? (bufferedEnd / displayDuration) * 100 : 0}%` }} />
          <div className="absolute inset-y-0 left-0 bg-amber-500 transition-all" style={{ width: `${displayDuration > 0 ? (currentTime / displayDuration) * 100 : 0}%` }} />
        </div>

        {showFxPanel ? (
          <div className="border-b border-zinc-800 px-4 py-4">
            {footerFxPanel}
          </div>
        ) : null}

        <div className="flex items-center gap-4 px-4 py-2">
          <div className="flex min-w-0 flex-1 items-center gap-3">
            <button
              type="button"
              className="h-14 w-14 shrink-0 overflow-hidden rounded bg-zinc-800 focus:outline-none focus:ring-2 focus:ring-amber-500/60"
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
                <div className="flex h-full w-full items-center justify-center">
                  <Music className="h-7 w-7 text-zinc-500" />
                </div>
              )}
            </button>
            <button
              type="button"
              className="min-w-0 flex-1 rounded text-left focus:outline-none focus:ring-2 focus:ring-amber-500/60"
              onClick={() => setShowNowPlaying(true)}
              title="Open Now Playing"
            >
              <p className="truncate text-sm font-medium text-white">{currentTrack?.title ?? '—'}</p>
              <p className="truncate text-xs text-zinc-400">{currentTrack?.artist} · {displayAlbumTitle}</p>
            </button>
          </div>

          <div className="flex items-center gap-2">
            <Button variant="ghost" size="icon" className="h-9 w-9 text-white hover:bg-zinc-700" onClick={prevTrack} disabled={currentIndex <= 0}>
              <SkipBack className="h-5 w-5" />
            </Button>
            <Button variant="ghost" size="icon" className="h-10 w-10 text-white hover:bg-zinc-700" onClick={togglePlayPause}>
              {isPlaying ? <Pause className="h-6 w-6 fill-current" /> : <Play className="h-6 w-6 fill-current" />}
            </Button>
            <Button variant="ghost" size="icon" className="h-9 w-9 text-white hover:bg-zinc-700" onClick={nextTrack} disabled={currentIndex < 0 || currentIndex >= tracks.length - 1}>
              <SkipForward className="h-5 w-5" />
            </Button>
          </div>

          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="icon"
              className={cn('h-8 w-8 text-white hover:bg-zinc-700', trackLiked ? 'text-emerald-300' : '')}
              onClick={() => void toggleTrackLike()}
              title={trackLiked ? 'Liked' : 'Like'}
            >
              <ThumbsUp className={cn('h-4 w-4', trackLiked ? 'fill-current' : '')} />
            </Button>
            <Button variant="ghost" size="icon" className="h-8 w-8 text-white hover:bg-zinc-700" onClick={() => void dislikeTrack()} title="Dislike">
              <ThumbsDown className="h-4 w-4" />
            </Button>
          </div>

          <div className="min-w-[100px] justify-center text-xs tabular-nums text-zinc-400 flex items-center gap-1">
            <span>{formatDuration(currentTime)}</span>
            <span>/</span>
            <span>{formatDuration(displayDuration)}</span>
          </div>

          <div className="flex w-28 items-center gap-1">
            <Button variant="ghost" size="icon" className="h-8 w-8 shrink-0 text-white hover:bg-zinc-700" onClick={toggleMute}>
              {prefs.muted || prefs.volume === 0 ? <VolumeX className="h-4 w-4" /> : <Volume2 className="h-4 w-4" />}
            </Button>
            <Slider value={[prefs.muted ? 0 : prefs.volume]} max={1} step={0.05} onValueChange={handleVolumeChange} className="w-full [&_[data-orientation=horizontal]]:bg-zinc-700" />
          </div>

          <Button variant="ghost" size="icon" className={cn('h-8 w-8 text-white hover:bg-zinc-700', showFxPanel ? 'bg-zinc-800' : '')} onClick={() => setShowFxPanel((value) => !value)} title="Audio effects">
            <SlidersHorizontal className="h-4 w-4" />
          </Button>
          <Button variant="ghost" size="icon" className="h-8 w-8 text-white hover:bg-zinc-700" onClick={() => setShowList((s) => !s)} title="Track list">
            <List className="h-4 w-4" />
          </Button>
          <Button variant="ghost" size="icon" className="h-8 w-8 text-white hover:bg-zinc-700" onClick={() => setShowNowPlaying(true)} title="Now Playing">
            <Maximize2 className="h-4 w-4" />
          </Button>
          <Button variant="ghost" size="icon" className="h-8 w-8 text-white hover:bg-zinc-700" onClick={handleClosePlayer} title="Close">
            <X className="h-4 w-4" />
          </Button>
        </div>

        {showList ? (
          <div className="border-t border-zinc-700 bg-zinc-800/50">
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
                      'flex w-full cursor-grab items-center gap-2 rounded px-2 py-1.5 text-left text-sm transition-colors active:cursor-grabbing hover:bg-zinc-700',
                      currentTrack?.track_id === track.track_id && 'bg-zinc-700 font-medium text-white'
                    )}
                  >
                    <GripVertical className="h-4 w-4 text-zinc-600" />
                    <span className="w-6 shrink-0 text-zinc-400">{track.index}</span>
                    <span className="min-w-0 flex-1 truncate text-zinc-200">{track.title}</span>
                    <span className="shrink-0 text-xs tabular-nums text-zinc-500">{formatDuration(track.duration)}</span>
                  </button>
                ))}
              </div>
            </ScrollArea>
          </div>
        ) : null}
      </div>

      <Dialog open={showNowPlaying} onOpenChange={setShowNowPlaying}>
        <DialogContent className="h-[100dvh] w-[100dvw] max-w-none overflow-y-auto rounded-none border-0 p-0 !left-0 !top-0 !translate-x-0 !translate-y-0 [&>button]:hidden">
          <div className={cn('relative min-h-full', isLightTheme ? 'text-foreground' : 'text-white')}>
            <div className={cn('absolute inset-0', isLightTheme ? 'bg-slate-100' : 'bg-zinc-950')} />
            {(displayAlbumThumb && !coverError) ? (
              <img
                src={displayAlbumThumb}
                alt=""
                className={cn('absolute inset-0 h-full w-full scale-110 object-cover blur-3xl', isLightTheme ? 'opacity-15' : 'opacity-25')}
                onError={() => setCoverError(true)}
              />
            ) : null}
            <div className={cn('absolute inset-0', isLightTheme ? 'bg-gradient-to-b from-white/55 via-white/88 to-white' : 'bg-gradient-to-b from-black/25 via-black/70 to-black')} />
            <div className="pointer-events-none absolute inset-0 opacity-50">
              <div className="absolute -right-20 -top-24 h-80 w-80 rounded-full bg-amber-500/10 blur-3xl" />
              <div className="absolute -bottom-28 -left-24 h-80 w-80 rounded-full bg-sky-500/10 blur-3xl" />
            </div>

            <div className="relative z-10 flex h-full flex-col">
              <div className={cn('flex items-center justify-between gap-3 border-b px-4 py-3 backdrop-blur-sm md:px-6', isLightTheme ? 'border-border/70 bg-white/60' : 'border-white/10 bg-black/25')}>
                {isMobile ? (
                  <>
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className={cn('h-10 w-10 rounded-full', isLightTheme ? 'text-foreground hover:bg-black/5' : 'text-white hover:bg-white/10')}
                      onClick={() => setShowNowPlaying(false)}
                      title="Back to mini player"
                    >
                      <ChevronLeft className="h-5 w-5" />
                    </Button>
                    <DialogHeader className="min-w-0 flex-1 space-y-0 text-center">
                      <DialogTitle className={cn('truncate text-xl font-semibold', isLightTheme ? 'text-foreground' : 'text-white')}>Now Playing</DialogTitle>
                      <DialogDescription className="sr-only">
                        Full-screen player showing the current track and the queued track list.
                      </DialogDescription>
                    </DialogHeader>
                    <div className="w-10 shrink-0" />
                  </>
                ) : (
                  <>
                    <DialogHeader className="space-y-0">
                      <DialogTitle className={cn(isLightTheme ? 'text-foreground' : 'text-white')}>Now Playing</DialogTitle>
                      <DialogDescription className="sr-only">
                        Full-screen player showing the current track and the queued track list.
                      </DialogDescription>
                    </DialogHeader>
                    <div className="flex items-center gap-2">
                      <Badge className={cn('hidden sm:inline-flex', isLightTheme ? 'border-border/60 bg-background/75 text-foreground' : 'border-white/10 bg-white/10 text-white')} variant="outline">
                        {tracks.length} tracks
                      </Badge>
                      <Button type="button" variant="ghost" size="icon" className={cn('h-9 w-9', isLightTheme ? 'text-foreground hover:bg-black/5' : 'text-white hover:bg-white/10')} onClick={() => setShowNowPlaying(false)} title="Collapse">
                        <ChevronDown className="h-5 w-5" />
                      </Button>
                    </div>
                  </>
                )}
              </div>

              <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
                <div className={cn('flex flex-col items-center gap-6 p-6 md:p-10', isMobile ? 'pb-8 pt-5' : '')}>
                  <div className={cn('relative aspect-square overflow-hidden rounded-3xl border shadow-[0_20px_72px_rgba(0,0,0,0.28)]', isMobile ? 'w-[min(78vw,360px)] self-center' : 'w-[min(82vw,520px)]', isLightTheme ? 'border-border/60 bg-card' : 'border-white/10 bg-zinc-900 shadow-[0_30px_110px_rgba(0,0,0,0.65)]')}>
                    {(displayAlbumThumb && !coverError) ? (
                      <img src={displayAlbumThumb} alt="" className="absolute inset-0 h-full w-full animate-in object-cover fade-in-0 duration-300" />
                    ) : (
                      <div className="absolute inset-0 flex items-center justify-center">
                        <Music className={cn('h-20 w-20', isLightTheme ? 'text-muted-foreground/70' : 'text-zinc-600')} />
                      </div>
                    )}
                    <div className={cn('absolute inset-0', isLightTheme ? 'bg-gradient-to-t from-black/30 via-black/5 to-transparent' : 'bg-gradient-to-t from-black/65 via-black/20 to-transparent')} />
                  </div>

                  <div className={cn('space-y-1 text-center', isMobile ? 'w-full max-w-[92vw]' : 'max-w-[860px]')}>
                    <div className={cn('truncate text-sm', isLightTheme ? 'text-muted-foreground' : 'text-white/70')}>{currentTrack?.artist ?? ''}</div>
                    <div className={cn('text-balance font-semibold tracking-tight', isMobile ? 'text-[2rem] leading-tight' : 'truncate text-3xl md:text-4xl', isLightTheme ? 'text-foreground' : 'text-white')}>
                      {currentTrack?.title ?? '—'}
                    </div>
                    <div className={cn(isMobile ? 'text-sm leading-6' : 'truncate text-sm', isLightTheme ? 'text-muted-foreground/90' : 'text-white/55')}>{displayAlbumTitle}</div>
                    {albumMeta ? (
                      <div className="mt-4 flex max-w-[920px] flex-col items-center gap-2">
                        <div className={cn('text-[11px] uppercase tracking-[0.24em]', isLightTheme ? 'text-muted-foreground/80' : 'text-white/45')}>Album metadata</div>
                        <div className="flex flex-wrap items-center justify-center gap-2">
                          {releaseYear ? <Badge variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('year'))}>{releaseYear}</Badge> : null}
                          {albumMeta.total_duration_sec > 0 ? <Badge variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('duration'))}>{formatDuration(albumMeta.total_duration_sec)}</Badge> : null}
                          {albumMeta.format ? <FormatBadge format={albumMeta.format} size="sm" className="h-7 rounded-full px-3 py-1 text-xs" /> : null}
                          <Badge variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass(albumMeta.is_lossless ? 'lossless' : 'lossy'))}>
                            {albumMeta.is_lossless ? 'Lossless' : 'Lossy'}
                          </Badge>
                          {trackCountText ? <Badge variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('count'))}>{trackCountText}</Badge> : null}
                          {discCountText ? <Badge variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('count'))}>{discCountText}</Badge> : null}
                          {labelText ? <Badge variant="outline" className={cn('h-7 rounded-full px-3 text-xs', badgeKindClass('label'))}>{`Label: ${labelText}`}</Badge> : null}
                        </div>
                        {genres.length > 0 ? (
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

                  <div className="w-full max-w-[780px] space-y-2">
                    <Slider
                      value={[currentTime]}
                      max={Math.max(1, displayDuration)}
                      step={1}
                      onValueChange={handleSeek}
                      className={cn('w-full', isLightTheme ? '[&_[data-orientation=horizontal]]:bg-black/10' : '[&_[data-orientation=horizontal]]:bg-white/15')}
                    />
                    <div className={cn('flex items-center justify-between text-xs tabular-nums', isLightTheme ? 'text-muted-foreground' : 'text-white/60')}>
                      <span>{formatDuration(currentTime)}</span>
                      <span>{formatDuration(displayDuration)}</span>
                    </div>
                  </div>

                  <div className="flex flex-wrap items-center justify-center gap-3">
                    <Button variant="ghost" size="icon" className={cn('h-11 w-11 rounded-full', isLightTheme ? 'text-foreground hover:bg-black/5' : 'text-white hover:bg-white/10')} onClick={prevTrack} disabled={currentIndex <= 0} title="Previous">
                      <SkipBack className="h-5 w-5" />
                    </Button>
                    <Button
                      variant="secondary"
                      size="icon"
                      className={cn('h-16 w-16 rounded-full border', isLightTheme ? 'border-border/60 bg-background/80 text-foreground hover:bg-background' : 'border-white/20 bg-white/15 text-white hover:bg-white/20')}
                      onClick={togglePlayPause}
                      title={isPlaying ? 'Pause' : 'Play'}
                    >
                      {isPlaying ? <Pause className="h-8 w-8 fill-current" /> : <Play className="h-8 w-8 fill-current" />}
                    </Button>
                    <Button variant="ghost" size="icon" className={cn('h-11 w-11 rounded-full', isLightTheme ? 'text-foreground hover:bg-black/5' : 'text-white hover:bg-white/10')} onClick={nextTrack} disabled={currentIndex < 0 || currentIndex >= tracks.length - 1} title="Next">
                      <SkipForward className="h-5 w-5" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      className={cn('h-10 w-10 rounded-full', isLightTheme ? 'text-foreground hover:bg-black/5' : 'text-white hover:bg-white/10', trackLiked ? 'text-emerald-300' : '')}
                      onClick={() => void toggleTrackLike()}
                      title={trackLiked ? 'Liked' : 'Like'}
                    >
                      <ThumbsUp className={cn('h-5 w-5', trackLiked ? 'fill-current' : '')} />
                    </Button>
                    <Button variant="ghost" size="icon" className={cn('h-10 w-10 rounded-full', isLightTheme ? 'text-foreground hover:bg-black/5' : 'text-white hover:bg-white/10')} onClick={() => void dislikeTrack()} title="Dislike">
                      <ThumbsDown className="h-5 w-5" />
                    </Button>
                    <Button variant="ghost" size="icon" className={cn('h-10 w-10 rounded-full', isLightTheme ? 'text-foreground hover:bg-black/5' : 'text-white hover:bg-white/10', showFxPanel ? (isLightTheme ? 'bg-black/5' : 'bg-white/10') : '')} onClick={() => setShowFxPanel((value) => !value)} title="Audio effects">
                      <SlidersHorizontal className="h-5 w-5" />
                    </Button>
                  </div>

                  {showFxPanel ? (
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

                <div className={cn('flex min-h-0 flex-1 flex-col border-t', isLightTheme ? 'border-border/70 bg-white/55' : 'border-white/10 bg-black/25')}>
                  <div className="flex items-center justify-between gap-3 px-4 py-3 md:px-6">
                    <div className={cn('text-sm font-medium', isLightTheme ? 'text-foreground' : 'text-white/85')}>Tracks</div>
                    <div className={cn('text-xs tabular-nums', isLightTheme ? 'text-muted-foreground' : 'text-white/55')}>
                      {currentIndex >= 0 ? `${currentIndex + 1}/${tracks.length}` : `${tracks.length}`}
                    </div>
                  </div>
                  <ScrollArea className="min-h-0 flex-1">
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
                            'flex w-full cursor-grab items-center gap-3 rounded-xl px-3 py-2 text-left text-sm transition-colors active:cursor-grabbing',
                            isLightTheme ? 'hover:bg-black/5' : 'hover:bg-white/10',
                            currentTrack?.track_id === track.track_id && (isLightTheme ? 'bg-black/10 text-foreground' : 'bg-white/10 text-white')
                          )}
                        >
                          <GripVertical className={cn('h-4 w-4 shrink-0', isLightTheme ? 'text-muted-foreground/50' : 'text-white/25')} />
                          <span className={cn('w-10 shrink-0 tabular-nums', isLightTheme ? 'text-muted-foreground/80' : 'text-white/45')}>{track.index}</span>
                          <span className={cn('min-w-0 flex-1 truncate', isLightTheme ? 'text-foreground/95' : 'text-white/90')}>{track.title}</span>
                          <span className={cn('shrink-0 text-xs tabular-nums', isLightTheme ? 'text-muted-foreground/80' : 'text-white/45')}>{formatDuration(track.duration)}</span>
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
