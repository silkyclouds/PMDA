import { useState, useRef, useEffect } from 'react';
import { ChevronDown, Play, Pause, SkipBack, SkipForward, Volume2, VolumeX, List, X, Music, ThumbsUp, ThumbsDown, Maximize2, GripVertical } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Slider } from '@/components/ui/slider';
import { ScrollArea } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';
import * as api from '@/lib/api';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Badge } from '@/components/ui/badge';

export interface TrackInfo {
  track_id: number;
  title: string;
  artist: string;
  album: string;
  duration: number;
  index: number;
  file_url: string;
}

interface AudioPlayerProps {
  albumId: number;
  albumTitle: string;
  /** Plex or local URL for album cover */
  albumThumb?: string | null;
  recommendationSessionId?: string | null;
  tracks: TrackInfo[];
  currentTrack: TrackInfo | null;
  onTrackSelect: (track: TrackInfo) => void;
  onClose: () => void;
}

function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds));
  const m = Math.floor(s / 60);
  const sec = s % 60;
  return `${m}:${sec.toString().padStart(2, '0')}`;
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
  const [isPlaying, setIsPlaying] = useState(false);
  const [currentTime, setCurrentTime] = useState(0);
  const [duration, setDuration] = useState(0);
  const [bufferedEnd, setBufferedEnd] = useState(0);
  const [volume, setVolume] = useState(1);
  const [muted, setMuted] = useState(false);
  const [showList, setShowList] = useState(false);
  const [showNowPlaying, setShowNowPlaying] = useState(false);
  const [coverError, setCoverError] = useState(false);
  const [trackLiked, setTrackLiked] = useState(false);
  const audioRef = useRef<HTMLAudioElement>(null);
  const lastTrackLoadTimeRef = useRef<number>(0);
  const activeTrackRef = useRef<TrackInfo | null>(null);
  const playedSecondsRef = useRef(0);
  const finalizedTrackIdRef = useRef<number | null>(null);

  const currentIndex = currentTrack ? tracks.findIndex((t) => t.track_id === currentTrack.track_id) : -1;
  const displayDuration = duration > 0 ? duration : (currentTrack?.duration ?? 0);

  const sendRecoEvent = (eventType: api.RecoEventType, track: TrackInfo | null, playedSeconds?: number) => {
    if (!track || !recommendationSessionId) return;
    void api.postRecommendationEvent({
      session_id: recommendationSessionId,
      track_id: track.track_id,
      event_type: eventType,
      played_seconds: Math.max(0, Math.floor(playedSeconds ?? 0)),
    }).catch(() => {
      // Non-blocking telemetry path.
    });
  };

  const finalizeCurrentTrack = (reason: 'switch' | 'ended' | 'close') => {
    const track = activeTrackRef.current;
    if (!track) return;
    if (finalizedTrackIdRef.current === track.track_id) return;
    const played = Math.max(0, Math.floor(playedSecondsRef.current));
    const completeThreshold = Math.max(30, Math.floor((track.duration || 0) * 0.85));
    let eventType: api.RecoEventType = 'skip';
    if (reason === 'ended' || played >= completeThreshold) {
      eventType = 'play_complete';
    } else if (played >= 12) {
      eventType = 'play_partial';
    } else if (reason === 'close') {
      eventType = 'stop';
    } else {
      eventType = 'skip';
    }
    sendRecoEvent(eventType, track, played);
    // Global listening stats (independent of recommendation session).
    void api.postPlaybackEvent({
      track_id: track.track_id,
      event_type: eventType,
      played_seconds: played,
    }).catch(() => {
      // Non-blocking telemetry path.
    });
    finalizedTrackIdRef.current = track.track_id;
  };

  // When currentTrack changes: set src, reset time, and start playing (user already clicked Play on album)
  useEffect(() => {
    if (activeTrackRef.current && (!currentTrack || activeTrackRef.current.track_id !== currentTrack.track_id)) {
      finalizeCurrentTrack('switch');
    }
    if (!currentTrack || !audioRef.current) return;
    activeTrackRef.current = currentTrack;
    finalizedTrackIdRef.current = null;
    playedSecondsRef.current = 0;
    // UI state follows the audio element events; reset to avoid showing stale play state during source swap.
    setIsPlaying(false);
    sendRecoEvent('play_start', currentTrack, 0);
    lastTrackLoadTimeRef.current = Date.now();
    setCurrentTime(0);
    setDuration(currentTrack.duration || 0);
    setBufferedEnd(0);
    audioRef.current.src = currentTrack.file_url;
    audioRef.current.load();
    // Start playback; if the browser blocks it, the user can press Play again.
    void audioRef.current.play().catch(() => {
      // Keep UI in sync; the element will remain paused.
      setIsPlaying(false);
    });
  }, [currentTrack?.track_id]);

  // Reset cover error when album or thumb URL changes so we retry showing the image for the new session
  useEffect(() => {
    setCoverError(false);
  }, [albumId, albumThumb]);

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

  const handlePause = () => {
    const elapsed = Date.now() - lastTrackLoadTimeRef.current;
    if (elapsed < 1500) return;
    setIsPlaying(false);
  };

  const togglePlayPause = () => {
    const el = audioRef.current;
    if (!el) return;
    if (el.paused) {
      void el.play().catch(() => {
        // Leave the UI as-is; onPlay/onPause events will reconcile.
        setIsPlaying(false);
      });
      return;
    }
    el.pause();
  };

  const handleTimeUpdate = () => {
    if (audioRef.current) {
      setCurrentTime(audioRef.current.currentTime);
      playedSecondsRef.current = audioRef.current.currentTime;
      if (duration === 0 && !isNaN(audioRef.current.duration)) {
        setDuration(audioRef.current.duration);
      }
    }
  };

  const handleLoadedMetadata = () => {
    if (audioRef.current && !isNaN(audioRef.current.duration)) {
      setDuration(audioRef.current.duration);
    }
  };

  const handleProgress = () => {
    if (audioRef.current && audioRef.current.buffered.length > 0) {
      const end = audioRef.current.buffered.end(audioRef.current.buffered.length - 1);
      setBufferedEnd(end);
    }
  };

  const handleEnded = () => {
    finalizeCurrentTrack('ended');
    if (currentIndex >= 0 && currentIndex < tracks.length - 1) {
      onTrackSelect(tracks[currentIndex + 1]);
    } else {
      setIsPlaying(false);
    }
  };

  const handleSeek = (value: number[]) => {
    const t = value[0];
    if (audioRef.current) {
      audioRef.current.currentTime = t;
      setCurrentTime(t);
    }
  };

  const handleVolumeChange = (value: number[]) => {
    const v = value[0];
    setVolume(v);
    if (audioRef.current) audioRef.current.volume = v;
    setMuted(v === 0);
  };

  const prevTrack = () => {
    if (currentIndex > 0) onTrackSelect(tracks[currentIndex - 1]);
  };

  const nextTrack = () => {
    if (currentIndex >= 0 && currentIndex < tracks.length - 1) onTrackSelect(tracks[currentIndex + 1]);
  };

  const handleClosePlayer = () => {
    finalizeCurrentTrack('close');
    onClose();
  };

  if (tracks.length === 0) return null;

  return (
    <>
      <audio
        ref={audioRef}
        onTimeUpdate={handleTimeUpdate}
        onLoadedMetadata={handleLoadedMetadata}
        onProgress={handleProgress}
        onEnded={handleEnded}
        onPlay={() => setIsPlaying(true)}
        onPause={handlePause}
      />
      <div className="fixed bottom-0 left-0 right-0 z-50 border-t bg-zinc-900 text-white shadow-[0_-4px_20px_rgba(0,0,0,0.3)]">
        {/* Progress bar on top (Plex-style: orange progress, lighter buffered) */}
        <div className="h-1 w-full bg-zinc-700 relative cursor-pointer group" onClick={(e) => {
          if (!audioRef.current || displayDuration <= 0) return;
          const rect = e.currentTarget.getBoundingClientRect();
          const pct = (e.clientX - rect.left) / rect.width;
          const t = pct * displayDuration;
          audioRef.current.currentTime = t;
          setCurrentTime(t);
        }}>
          <div
            className="absolute inset-y-0 left-0 bg-zinc-500 transition-all"
            style={{ width: `${(bufferedEnd / displayDuration) * 100}%` }}
          />
          <div
            className="absolute inset-y-0 left-0 bg-amber-500 transition-all"
            style={{ width: `${displayDuration > 0 ? (currentTime / displayDuration) * 100 : 0}%` }}
          />
        </div>

        <div className="flex items-center gap-4 px-4 py-2">
          {/* Cover + title + artist - album (left) */}
          <div className="flex min-w-0 flex-1 items-center gap-3">
            <button
              type="button"
              className="h-14 w-14 shrink-0 rounded overflow-hidden bg-zinc-800 focus:outline-none focus:ring-2 focus:ring-amber-500/60"
              onClick={() => setShowNowPlaying(true)}
              title="Open Now Playing"
              draggable
              onDragStart={(e) => {
                try {
                  e.dataTransfer.setData('application/x-pmda-album', JSON.stringify({ album_id: albumId }));
                  e.dataTransfer.setData('text/plain', albumTitle || 'Album');
                  e.dataTransfer.effectAllowed = 'copy';
                } catch {
                  // ignore
                }
              }}
            >
              {(albumThumb && !coverError) ? (
                <img
                  src={albumThumb}
                  alt=""
                  className="h-full w-full object-cover"
                  onError={() => setCoverError(true)}
                />
              ) : (
                <div className="h-full w-full flex items-center justify-center">
                  <Music className="h-7 w-7 text-zinc-500" />
                </div>
              )}
            </button>
            <button
              type="button"
              className="min-w-0 flex-1 text-left focus:outline-none focus:ring-2 focus:ring-amber-500/60 rounded"
              onClick={() => setShowNowPlaying(true)}
              title="Open Now Playing"
            >
              <p className="truncate text-sm font-medium text-white">
                {currentTrack?.title ?? '—'}
              </p>
              <p className="truncate text-xs text-zinc-400">
                {currentTrack?.artist} · {albumTitle}
              </p>
            </button>
          </div>

          {/* Controls + time MM:SS / MM:SS */}
          <div className="flex items-center gap-2">
            <Button variant="ghost" size="icon" className="h-9 w-9 text-white hover:bg-zinc-700" onClick={prevTrack} disabled={currentIndex <= 0}>
              <SkipBack className="h-5 w-5" />
            </Button>
            <Button
              variant="ghost"
              size="icon"
              className="h-10 w-10 text-white hover:bg-zinc-700"
              onClick={togglePlayPause}
            >
              {isPlaying ? <Pause className="h-6 w-6 fill-current" /> : <Play className="h-6 w-6 fill-current" />}
            </Button>
            <Button
              variant="ghost"
              size="icon"
              className="h-9 w-9 text-white hover:bg-zinc-700"
              onClick={nextTrack}
              disabled={currentIndex < 0 || currentIndex >= tracks.length - 1}
            >
              <SkipForward className="h-5 w-5" />
            </Button>
          </div>
          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="icon"
              className={cn("h-8 w-8 text-white hover:bg-zinc-700", trackLiked ? "text-emerald-300" : "")}
              onClick={() => void toggleTrackLike()}
              title={trackLiked ? "Liked" : "Like"}
            >
              <ThumbsUp className={cn("h-4 w-4", trackLiked ? "fill-current" : "")} />
            </Button>
            <Button
              variant="ghost"
              size="icon"
              className="h-8 w-8 text-white hover:bg-zinc-700"
              onClick={() => void dislikeTrack()}
              title="Dislike"
            >
              <ThumbsDown className="h-4 w-4" />
            </Button>
          </div>
          <div className="flex items-center gap-2 text-xs text-zinc-400 tabular-nums min-w-[100px] justify-center">
            <span>{formatDuration(currentTime)}</span>
            <span>/</span>
            <span>{formatDuration(displayDuration)}</span>
          </div>

          {/* Volume */}
          <div className="flex items-center gap-1 w-28">
            <Button
              variant="ghost"
              size="icon"
              className="h-8 w-8 text-white hover:bg-zinc-700 shrink-0"
              onClick={() => setMuted((m) => !m)}
            >
              {muted || volume === 0 ? (
                <VolumeX className="h-4 w-4" />
              ) : (
                <Volume2 className="h-4 w-4" />
              )}
            </Button>
            <Slider
              value={[muted ? 0 : volume]}
              max={1}
              step={0.05}
              onValueChange={handleVolumeChange}
              className="w-full [&_[data-orientation=horizontal]]:bg-zinc-700"
            />
          </div>

          <Button variant="ghost" size="icon" className="h-8 w-8 text-white hover:bg-zinc-700" onClick={() => setShowList((s) => !s)} title="Track list">
            <List className="h-4 w-4" />
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8 text-white hover:bg-zinc-700"
            onClick={() => setShowNowPlaying(true)}
            title="Now Playing"
          >
            <Maximize2 className="h-4 w-4" />
          </Button>
          <Button variant="ghost" size="icon" className="h-8 w-8 text-white hover:bg-zinc-700" onClick={handleClosePlayer} title="Close">
            <X className="h-4 w-4" />
          </Button>
        </div>

        {showList && (
          <div className="border-t border-zinc-700 bg-zinc-800/50">
            {/* Tall enough for ~15 tracks on desktop; scroll when longer */}
            <ScrollArea className="h-[min(60vh,560px)]">
              <div className="px-4 py-3 space-y-0.5 pr-4">
                {tracks.map((track) => (
                  <button
                    key={track.track_id}
                    type="button"
                    onClick={() => onTrackSelect(track)}
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
                      'flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-sm transition-colors hover:bg-zinc-700',
                      'cursor-grab active:cursor-grabbing',
                      currentTrack?.track_id === track.track_id && 'bg-zinc-700 font-medium text-white'
                    )}
                  >
                    <GripVertical className="h-4 w-4 text-zinc-600" />
                    <span className="w-6 shrink-0 text-zinc-400">{track.index}</span>
                    <span className="min-w-0 flex-1 truncate text-zinc-200">{track.title}</span>
                    <span className="shrink-0 text-xs text-zinc-500 tabular-nums">
                      {formatDuration(track.duration)}
                    </span>
                  </button>
                ))}
              </div>
            </ScrollArea>
          </div>
        )}
      </div>

      <Dialog open={showNowPlaying} onOpenChange={setShowNowPlaying}>
        <DialogContent className="w-[100dvw] h-[100dvh] max-w-none p-0 overflow-hidden rounded-none border-0">
          <div className="relative h-full text-white">
            <div className="absolute inset-0 bg-zinc-950" />
            {(albumThumb && !coverError) ? (
              <img
                src={albumThumb}
                alt=""
                className="absolute inset-0 h-full w-full object-cover blur-3xl scale-110 opacity-25"
                onError={() => setCoverError(true)}
              />
            ) : null}
            <div className="absolute inset-0 bg-gradient-to-b from-black/25 via-black/70 to-black" />
            <div className="absolute inset-0 opacity-50 pointer-events-none">
              <div className="absolute -top-24 -right-20 h-80 w-80 rounded-full bg-amber-500/10 blur-3xl" />
              <div className="absolute -bottom-28 -left-24 h-80 w-80 rounded-full bg-sky-500/10 blur-3xl" />
            </div>

            <div className="relative z-10 h-full flex flex-col">
              <div className="flex items-center justify-between gap-3 px-4 md:px-6 py-3 border-b border-white/10 bg-black/25 backdrop-blur-sm">
                <DialogHeader className="space-y-0">
                  <DialogTitle className="text-white">Now Playing</DialogTitle>
                  <DialogDescription className="sr-only">
                    Full-screen player showing the current track and the queued track list.
                  </DialogDescription>
                </DialogHeader>
                <div className="flex items-center gap-2">
                  <Badge className="bg-white/10 text-white border-white/10 hidden sm:inline-flex" variant="outline">
                    {tracks.length} tracks
                  </Badge>
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    className="h-9 w-9 text-white hover:bg-white/10"
                    onClick={() => setShowNowPlaying(false)}
                    title="Collapse"
                  >
                    <ChevronDown className="h-5 w-5" />
                  </Button>
                </div>
              </div>

              <div className="flex-1 min-h-0 overflow-hidden flex flex-col">
                {/* Top: big cover + controls */}
                <div className="p-6 md:p-10 flex flex-col items-center gap-6">
                  <div className="relative w-[min(82vw,520px)] aspect-square rounded-3xl overflow-hidden bg-zinc-900 border border-white/10 shadow-[0_30px_110px_rgba(0,0,0,0.65)]">
                    {(albumThumb && !coverError) ? (
                      <img src={albumThumb} alt="" className="absolute inset-0 h-full w-full object-cover animate-in fade-in-0 duration-300" />
                    ) : (
                      <div className="absolute inset-0 flex items-center justify-center">
                        <Music className="h-20 w-20 text-zinc-600" />
                      </div>
                    )}
                    <div className="absolute inset-0 bg-gradient-to-t from-black/65 via-black/20 to-transparent" />
                  </div>

                  <div className="text-center space-y-1 max-w-[860px]">
                    <div className="text-white/70 text-sm truncate">{currentTrack?.artist ?? ''}</div>
                    <div className="text-white text-3xl md:text-4xl font-semibold tracking-tight truncate">
                      {currentTrack?.title ?? '—'}
                    </div>
                    <div className="text-white/55 text-sm truncate">{albumTitle}</div>
                  </div>

                  <div className="w-full max-w-[780px] space-y-2">
                    <Slider
                      value={[currentTime]}
                      max={Math.max(1, displayDuration)}
                      step={1}
                      onValueChange={handleSeek}
                      className="w-full [&_[data-orientation=horizontal]]:bg-white/15"
                    />
                    <div className="flex items-center justify-between text-xs text-white/60 tabular-nums">
                      <span>{formatDuration(currentTime)}</span>
                      <span>{formatDuration(displayDuration)}</span>
                    </div>
                  </div>

                  <div className="flex flex-wrap items-center justify-center gap-3">
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-11 w-11 rounded-full text-white hover:bg-white/10"
                      onClick={prevTrack}
                      disabled={currentIndex <= 0}
                      title="Previous"
                    >
                      <SkipBack className="h-5 w-5" />
                    </Button>
                    <Button
                      variant="secondary"
                      size="icon"
                      className="h-16 w-16 rounded-full bg-white/15 text-white hover:bg-white/20 border border-white/20"
                      onClick={togglePlayPause}
                      title={isPlaying ? 'Pause' : 'Play'}
                    >
                      {isPlaying ? <Pause className="h-8 w-8 fill-current" /> : <Play className="h-8 w-8 fill-current" />}
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-11 w-11 rounded-full text-white hover:bg-white/10"
                      onClick={nextTrack}
                      disabled={currentIndex < 0 || currentIndex >= tracks.length - 1}
                      title="Next"
                    >
                      <SkipForward className="h-5 w-5" />
                    </Button>
                    <div className="flex items-center gap-2">
                      <Button
                        variant="ghost"
                        size="icon"
                        className={cn("h-10 w-10 rounded-full text-white hover:bg-white/10", trackLiked ? "text-emerald-300" : "")}
                        onClick={() => void toggleTrackLike()}
                        title={trackLiked ? "Liked" : "Like"}
                      >
                        <ThumbsUp className={cn("h-5 w-5", trackLiked ? "fill-current" : "")} />
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-10 w-10 rounded-full text-white hover:bg-white/10"
                        onClick={() => void dislikeTrack()}
                        title="Dislike"
                      >
                        <ThumbsDown className="h-5 w-5" />
                      </Button>
                    </div>
                  </div>
                </div>

                {/* Bottom: track list */}
                <div className="flex-1 min-h-0 border-t border-white/10 bg-black/25 flex flex-col">
                  <div className="px-4 md:px-6 py-3 flex items-center justify-between gap-3">
                    <div className="text-sm font-medium text-white/85">Tracks</div>
                    <div className="text-xs text-white/55 tabular-nums">
                      {currentIndex >= 0 ? `${currentIndex + 1}/${tracks.length}` : `${tracks.length}`}
                    </div>
                  </div>
                  <ScrollArea className="flex-1 min-h-0">
                    <div className="px-4 md:px-6 pb-6 space-y-1">
                      {tracks.map((track) => (
                        <button
                          key={track.track_id}
                          type="button"
                          onClick={() => onTrackSelect(track)}
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
                            'flex w-full items-center gap-3 rounded-xl px-3 py-2 text-left text-sm transition-colors hover:bg-white/10',
                            'cursor-grab active:cursor-grabbing',
                            currentTrack?.track_id === track.track_id && 'bg-white/10 text-white'
                          )}
                        >
                          <GripVertical className="h-4 w-4 text-white/25 shrink-0" />
                          <span className="w-10 shrink-0 text-white/45 tabular-nums">{track.index}</span>
                          <span className="min-w-0 flex-1 truncate text-white/90">{track.title}</span>
                          <span className="shrink-0 text-xs text-white/45 tabular-nums">{formatDuration(track.duration)}</span>
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
