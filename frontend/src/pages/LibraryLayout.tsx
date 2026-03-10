import { useEffect, useState } from 'react';
import { Outlet, useLocation } from 'react-router-dom';

import * as api from '@/lib/api';

export interface LibraryOutletContext {
  includeUnmatched: boolean;
  stats: api.LibraryStats | null;
  statsReady: boolean;
  libraryIsEmpty: boolean;
}

export default function LibraryLayout() {
  const location = useLocation();
  const [stats, setStats] = useState<api.LibraryStats | null>(null);
  const [statsReady, setStatsReady] = useState(false);
  const includeUnmatched = true;

  useEffect(() => {
    let cancelled = false;
    setStatsReady(false);
    api
      .getLibraryStats({ includeUnmatched: true })
      .then((data) => {
        if (cancelled) return;
        setStats(data);
        setStatsReady(true);
      })
      .catch(() => {
        if (cancelled) return;
        setStats(null);
        setStatsReady(true);
      });
    return () => {
      cancelled = true;
    };
  }, [location.pathname, location.search]);

  const libraryIsEmpty =
    statsReady &&
    stats != null &&
    ((stats.artists ?? 0) + (stats.albums ?? 0) + (stats.tracks ?? 0) === 0);

  return <Outlet context={{ includeUnmatched, stats, statsReady, libraryIsEmpty }} />;
}
