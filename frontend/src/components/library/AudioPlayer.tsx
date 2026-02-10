import { useState, useRef, useEffect } from 'react';
import { Play, Pause, SkipBack, SkipForward, Volume2, VolumeX, List, X, Music, ThumbsUp, ThumbsDown } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Slider } from '@/components/ui/slider';
import { ScrollArea } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';
import * as api from '@/lib/api';

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
  const [coverError, setCoverError] = useState(false);
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
    sendRecoEvent('play_start', currentTrack, 0);
    lastTrackLoadTimeRef.current = Date.now();
    setCurrentTime(0);
    setDuration(currentTrack.duration || 0);
    setBufferedEnd(0);
    audioRef.current.src = currentTrack.file_url;
    audioRef.current.load();
    setIsPlaying(true);
    const playPromise = audioRef.current.play();
    if (playPromise && typeof playPromise.then === 'function') {
      playPromise.catch(() => setIsPlaying(false));
    }
  }, [currentTrack?.track_id]);

  // Sync play/pause state to the audio element (only when user toggles; ignore brief pauses right after load)
  useEffect(() => {
    if (!audioRef.current) return;
    if (isPlaying) audioRef.current.play().catch(() => setIsPlaying(false));
    else audioRef.current.pause();
  }, [isPlaying]);

  // Reset cover error when album or thumb URL changes so we retry showing the image for the new session
  useEffect(() => {
    setCoverError(false);
  }, [albumId, albumThumb]);

  const handlePause = () => {
    const elapsed = Date.now() - lastTrackLoadTimeRef.current;
    if (elapsed < 1500) return;
    setIsPlaying(false);
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
            <div className="h-14 w-14 shrink-0 rounded overflow-hidden bg-zinc-800">
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
            </div>
            <div className="min-w-0 flex-1">
              <p className="truncate text-sm font-medium text-white">
                {currentTrack?.title ?? '—'}
              </p>
              <p className="truncate text-xs text-zinc-400">
                {currentTrack?.artist} · {albumTitle}
              </p>
            </div>
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
              onClick={() => setIsPlaying((p) => !p)}
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
              className="h-8 w-8 text-white hover:bg-zinc-700"
              onClick={() => sendRecoEvent('like', currentTrack, playedSecondsRef.current)}
              title="Like"
            >
              <ThumbsUp className="h-4 w-4" />
            </Button>
            <Button
              variant="ghost"
              size="icon"
              className="h-8 w-8 text-white hover:bg-zinc-700"
              onClick={() => sendRecoEvent('dislike', currentTrack, playedSecondsRef.current)}
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
          <Button variant="ghost" size="icon" className="h-8 w-8 text-white hover:bg-zinc-700" onClick={handleClosePlayer} title="Close">
            <X className="h-4 w-4" />
          </Button>
        </div>

        {showList && (
          <div className="border-t border-zinc-700 px-4 py-2 max-h-40 bg-zinc-800/50">
            <ScrollArea className="h-36">
              <div className="space-y-0.5 pr-4">
                {tracks.map((track) => (
                  <button
                    key={track.track_id}
                    type="button"
                    onClick={() => onTrackSelect(track)}
                    className={cn(
                      'flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-sm transition-colors hover:bg-zinc-700',
                      currentTrack?.track_id === track.track_id && 'bg-zinc-700 font-medium text-white'
                    )}
                  >
                    <span className="w-6 shrink-0 text-zinc-400">{track.index}</span>
                    <span className="min-w-0 flex-1 truncate text-zinc-200">{track.title}</span>
                    <span className="shrink-0 text-xs text-zinc-500">
                      {formatDuration(track.duration)}
                    </span>
                  </button>
                ))}
              </div>
            </ScrollArea>
          </div>
        )}
      </div>
    </>
  );
}
