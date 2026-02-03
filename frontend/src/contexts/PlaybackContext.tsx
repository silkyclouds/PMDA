import { createContext, useContext, useState, useCallback, type ReactNode } from 'react';
import type { TrackInfo } from '@/components/library/AudioPlayer';

export interface PlaybackSession {
  albumId: number;
  albumTitle: string;
  albumThumb: string | null;
  tracks: TrackInfo[];
  currentTrack: TrackInfo | null;
}

interface PlaybackContextValue {
  session: PlaybackSession | null;
  startPlayback: (albumId: number, albumTitle: string, albumThumb: string | null, tracks: TrackInfo[]) => void;
  setCurrentTrack: (track: TrackInfo) => void;
  closePlayer: () => void;
}

const PlaybackContext = createContext<PlaybackContextValue | null>(null);

export function PlaybackProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<PlaybackSession | null>(null);

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
        startPlayback,
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
