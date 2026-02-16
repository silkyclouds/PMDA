import { createContext, useContext, useState, useCallback, type ReactNode } from 'react';
import type { TrackInfo } from '@/components/library/AudioPlayer';

export interface PlaybackSession {
  albumId: number;
  albumTitle: string;
  albumThumb: string | null;
  tracks: TrackInfo[];
  currentTrack: TrackInfo | null;
}

interface QueueTrackOptions {
  albumId?: number;
  albumTitle?: string;
  albumThumb?: string | null;
  playNow?: boolean;
}

interface PlaybackContextValue {
  session: PlaybackSession | null;
  recommendationSessionId: string;
  startPlayback: (albumId: number, albumTitle: string, albumThumb: string | null, tracks: TrackInfo[]) => void;
  queueTrack: (track: TrackInfo, options?: QueueTrackOptions) => void;
  setCurrentTrack: (track: TrackInfo) => void;
  closePlayer: () => void;
}

const PlaybackContext = createContext<PlaybackContextValue | null>(null);

export function PlaybackProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<PlaybackSession | null>(null);
  const [recommendationSessionId] = useState<string>(() => {
    try {
      const existing = localStorage.getItem('pmda_reco_session_id');
      if (existing && existing.length >= 6) return existing;
      const generated = `pmda-${crypto.randomUUID()}`;
      localStorage.setItem('pmda_reco_session_id', generated);
      return generated;
    } catch {
      return `pmda-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;
    }
  });

  const startPlayback = useCallback(
    (albumId: number, albumTitle: string, albumThumb: string | null, tracks: TrackInfo[]) => {
      if (tracks.length === 0) return;
      setSession({
        albumId,
        albumTitle,
        albumThumb,
        tracks,
        currentTrack: tracks[0],
      });
    },
    []
  );

  const queueTrack = useCallback((track: TrackInfo, options?: QueueTrackOptions) => {
    if (!track || (track.track_id || 0) <= 0) return;
    setSession((prev) => {
      if (!prev) {
        const rawAlbumId = Number(options?.albumId);
        const seedAlbumId = Number.isFinite(rawAlbumId) ? rawAlbumId : -1;
        const seedAlbumTitle = (options?.albumTitle || track.album || 'Queue').trim();
        const seedAlbumThumb = options?.albumThumb ?? null;
        return {
          albumId: seedAlbumId,
          albumTitle: seedAlbumTitle,
          albumThumb: seedAlbumThumb,
          tracks: [track],
          currentTrack: track,
        };
      }
      const alreadyQueued = prev.tracks.some((t) => t.track_id === track.track_id);
      const nextTracks = alreadyQueued ? prev.tracks : [...prev.tracks, track];
      const nextCurrent = options?.playNow ? track : (prev.currentTrack || nextTracks[0] || null);
      return {
        ...prev,
        tracks: nextTracks,
        currentTrack: nextCurrent,
      };
    });
  }, []);

  const setCurrentTrack = useCallback((track: TrackInfo) => {
    setSession((prev) => {
      if (!prev) return prev;
      return { ...prev, currentTrack: track };
    });
  }, []);

  const closePlayer = useCallback(() => {
    setSession(null);
  }, []);

  return (
    <PlaybackContext.Provider
      value={{
        session,
        recommendationSessionId,
        startPlayback,
        queueTrack,
        setCurrentTrack,
        closePlayer,
      }}
    >
      {children}
    </PlaybackContext.Provider>
  );
}

export function usePlayback() {
  const ctx = useContext(PlaybackContext);
  if (!ctx) throw new Error('usePlayback must be used within PlaybackProvider');
  return ctx;
}
