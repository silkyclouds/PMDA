import { useCallback, useEffect, useState } from 'react';

const STORAGE_KEY = 'pmda_library_cover_size';
const EVENT_NAME = 'pmda:library-tile-size';
const MIN_TILE_SIZE = 150;
const MAX_TILE_SIZE = 340;
const DEFAULT_TILE_SIZE = 220;

function clampTileSize(value: number): number {
  if (!Number.isFinite(value)) return DEFAULT_TILE_SIZE;
  return Math.max(MIN_TILE_SIZE, Math.min(MAX_TILE_SIZE, Math.round(value / 10) * 10));
}

function readTileSize(): number {
  try {
    return clampTileSize(Number(localStorage.getItem(STORAGE_KEY) || DEFAULT_TILE_SIZE));
  } catch {
    return DEFAULT_TILE_SIZE;
  }
}

export function useLibraryTileSize() {
  const [tileSize, setTileSizeState] = useState<number>(() => readTileSize());

  const setTileSize = useCallback((value: number) => {
    const next = clampTileSize(value);
    setTileSizeState(next);
    try {
      localStorage.setItem(STORAGE_KEY, String(next));
      window.dispatchEvent(new Event(EVENT_NAME));
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    const syncFromStorage = () => setTileSizeState(readTileSize());
    const onStorage = (event: StorageEvent) => {
      if (event.key && event.key !== STORAGE_KEY) return;
      syncFromStorage();
    };
    window.addEventListener('storage', onStorage);
    window.addEventListener(EVENT_NAME, syncFromStorage as EventListener);
    return () => {
      window.removeEventListener('storage', onStorage);
      window.removeEventListener(EVENT_NAME, syncFromStorage as EventListener);
    };
  }, []);

  return { tileSize, setTileSize };
}

export function getLibraryGridTemplateColumns(tileSize: number, isMobile: boolean, min = 140, max = 340): string {
  if (isMobile) return 'repeat(2, minmax(0, 1fr))';
  const col = Math.max(min, Math.min(max, Math.floor(tileSize)));
  return `repeat(auto-fill, minmax(${col}px, ${col}px))`;
}

export function getLibraryTileBasis(tileSize: number, min = 160, max = 340): number {
  return Math.max(min, Math.min(max, Math.floor(tileSize)));
}
