import { useEffect, useState } from 'react';
import { Outlet, useLocation } from 'react-router-dom';

import * as api from '@/lib/api';

export interface LibraryOutletContext {
  includeUnmatched: boolean;
  stats: api.LibraryStats | null;
}

export default function LibraryLayout() {
  const location = useLocation();
  const [stats, setStats] = useState<api.LibraryStats | null>(null);
  const includeUnmatched = true;

  useEffect(() => {
    api.getLibraryStats({ includeUnmatched: true }).then(setStats).catch(() => setStats(null));
  }, [location.pathname, location.search]);

  return <Outlet context={{ includeUnmatched, stats }} />;
}
