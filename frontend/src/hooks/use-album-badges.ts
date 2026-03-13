import { useCallback, useEffect, useState } from 'react';

const STORAGE_KEY = 'pmda_album_badges_visible';
const COOKIE_KEY = 'pmda_album_badges_visible';
const COOKIE_MAX_AGE = 60 * 60 * 24 * 365;

function readCookie(): boolean | null {
  if (typeof document === 'undefined') return null;
  const token = document.cookie
    .split(';')
    .map((part) => part.trim())
    .find((part) => part.startsWith(`${COOKIE_KEY}=`));
  if (!token) return null;
  const value = token.slice(COOKIE_KEY.length + 1).trim().toLowerCase();
  if (value === '1' || value === 'true') return true;
  if (value === '0' || value === 'false') return false;
  return null;
}

function readPreference(): boolean {
  const cookieValue = readCookie();
  if (cookieValue != null) return cookieValue;
  if (typeof window === 'undefined') return true;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (raw == null) return true;
    return raw !== '0' && raw.toLowerCase() !== 'false';
  } catch {
    return true;
  }
}

function writePreference(next: boolean) {
  if (typeof window !== 'undefined') {
    try {
      window.localStorage.setItem(STORAGE_KEY, next ? '1' : '0');
    } catch {
      // ignore
    }
  }
  if (typeof document !== 'undefined') {
    document.cookie = `${COOKIE_KEY}=${next ? '1' : '0'}; Max-Age=${COOKIE_MAX_AGE}; Path=/; SameSite=Lax`;
  }
}

export function useAlbumBadgesVisibility() {
  const [showBadges, setShowBadgesState] = useState<boolean>(() => readPreference());

  useEffect(() => {
    setShowBadgesState(readPreference());
  }, []);

  const setShowBadges = useCallback((next: boolean) => {
    writePreference(Boolean(next));
    setShowBadgesState(Boolean(next));
  }, []);

  const toggleShowBadges = useCallback(() => {
    setShowBadges(!showBadges);
  }, [setShowBadges, showBadges]);

  return { showBadges, setShowBadges, toggleShowBadges };
}
