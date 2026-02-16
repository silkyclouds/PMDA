import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { ChevronDown, Search, X } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { cn } from '@/lib/utils';
import * as api from '@/lib/api';
import { FacetSuggestInput } from '@/components/library/FacetSuggestInput';
import { useLibraryQuery } from '@/hooks/useLibraryQuery';

type LibraryTabKey = 'home' | 'artists' | 'albums' | 'genres' | 'labels';

export interface LibraryOutletContext {
  includeUnmatched: boolean;
  stats: api.LibraryStats | null;
}

function activeTabFromPath(pathname: string): LibraryTabKey {
  const p = pathname || '';
  if (p === '/library' || p === '/library/') return 'home';
  if (p.startsWith('/library/artists') || p.startsWith('/library/artist/')) return 'artists';
  if (p.startsWith('/library/albums') || p.startsWith('/library/album/')) return 'albums';
  if (p.startsWith('/library/genres') || p.startsWith('/library/genre/')) return 'genres';
  if (p.startsWith('/library/labels') || p.startsWith('/library/label/')) return 'labels';
  return 'home';
}

export default function LibraryLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const tab = activeTabFromPath(location.pathname);
  const { search, genre, label, year, includeUnmatched: includeUnmatchedQuery, patch, clearFilters } = useLibraryQuery();

  const [stats, setStats] = useState<api.LibraryStats | null>(null);
  const [facetsLoading, setFacetsLoading] = useState(false);
  const [years, setYears] = useState<api.LibraryFacetYearItem[]>([]);
  const [configIncludeUnmatched, setConfigIncludeUnmatched] = useState<boolean>(false);

  const [filtersOpen, setFiltersOpen] = useState(false);

  const [searchDraft, setSearchDraft] = useState(search);
  const searchDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    setSearchDraft(search);
  }, [search]);

  useEffect(() => {
    let cancelled = false;
    api.getConfig()
      .then((cfg) => {
        if (cancelled) return;
        setConfigIncludeUnmatched(Boolean(cfg.LIBRARY_INCLUDE_UNMATCHED));
      })
      .catch(() => {
        if (cancelled) return;
        setConfigIncludeUnmatched(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const includeUnmatched = includeUnmatchedQuery ?? configIncludeUnmatched;

  useEffect(() => {
    if (searchDebounceRef.current) clearTimeout(searchDebounceRef.current);
    searchDebounceRef.current = setTimeout(() => {
      patch({ search: searchDraft }, { replace: true });
    }, 140);
    return () => {
      if (searchDebounceRef.current) clearTimeout(searchDebounceRef.current);
    };
  }, [searchDraft, patch]);

  useEffect(() => {
    // Header counts on Home must always reflect formally identified items only.
    api.getLibraryStats({ includeUnmatched: false }).then(setStats).catch(() => setStats(null));
  }, [location.pathname, location.search]);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      try {
        setFacetsLoading(true);
        const f = await api.getLibraryFacets({ includeUnmatched });
        if (!cancelled) setYears(Array.isArray(f.years) ? f.years : []);
      } catch {
        if (!cancelled) setYears([]);
      } finally {
        if (!cancelled) setFacetsLoading(false);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [includeUnmatched]);

  const gotoTab = useCallback(
    (target: LibraryTabKey) => {
      const qs = location.search || '';
      if (target === 'home') navigate(`/library${qs}`);
      else navigate(`/library/${target}${qs}`);
    },
    [location.search, navigate]
  );

  const fetchGenreSuggestions = useCallback(
    async (q: string) => {
      const res = await api.suggestLibraryGenres(q, 16, false, { label, year, includeUnmatched });
      return Array.isArray(res.genres) ? res.genres : [];
    },
    [includeUnmatched, label, year]
  );

  const fetchLabelSuggestions = useCallback(
    async (q: string) => {
      const res = await api.suggestLibraryLabels(q, 16, false, { genre, year, includeUnmatched });
      return Array.isArray(res.labels) ? res.labels : [];
    },
    [genre, includeUnmatched, year]
  );

  const showGenreFilter = tab !== 'genres';
  const showLabelFilter = tab !== 'labels';

  const activeBadges = useMemo(() => {
    const out: Array<{ kind: 'genre' | 'label' | 'year'; value: string }> = [];
    if (genre) out.push({ kind: 'genre', value: genre });
    if (label) out.push({ kind: 'label', value: label });
    if (year) out.push({ kind: 'year', value: String(year) });
    return out;
  }, [genre, label, year]);

  return (
    <>
      <div className="container pt-6 pb-4 space-y-4">
        <div className="relative overflow-hidden rounded-3xl border border-border/60 bg-gradient-to-br from-card via-card to-accent/20 p-5 md:p-7">
          <div className="absolute inset-0 pointer-events-none opacity-80">
            <div className="absolute -top-20 -right-24 h-64 w-64 rounded-full bg-primary/10 blur-3xl" />
            <div className="absolute -bottom-24 -left-24 h-64 w-64 rounded-full bg-amber-500/10 blur-3xl" />
          </div>
          <div className="relative space-y-4">
            <div className="flex items-start justify-between gap-4">
              <div className="min-w-0">
                <h1 className="text-3xl md:text-4xl font-bold tracking-tight">Library</h1>
                <p className="text-sm text-muted-foreground mt-1">
                  Browse your local library
                </p>
                {activeBadges.length > 0 ? (
                  <div className="mt-3 flex flex-wrap items-center gap-2">
                    {genre ? (
                      <Badge
                        variant="secondary"
                        className="cursor-pointer"
                        onClick={() => navigate(`/library/genre/${encodeURIComponent(genre)}${location.search || ''}`)}
                        title="Open genre"
                      >
                        Genre: {genre}
                      </Badge>
                    ) : null}
                    {label ? (
                      <Badge
                        variant="secondary"
                        className="cursor-pointer"
                        onClick={() => navigate(`/library/label/${encodeURIComponent(label)}${location.search || ''}`)}
                        title="Open label"
                      >
                        Label: {label}
                      </Badge>
                    ) : null}
                    {year ? <Badge variant="secondary">Year: {year}</Badge> : null}
                    <Button type="button" size="sm" variant="ghost" className="h-7 px-2" onClick={() => clearFilters()} title="Clear filters">
                      <X className="h-4 w-4 mr-1" /> Clear
                    </Button>
                  </div>
                ) : null}
              </div>
            </div>

            {/* Tabs */}
            <Card className="border-border/60 bg-background/60 backdrop-blur-sm p-2">
              <div className="flex flex-wrap items-center gap-2">
                <Button type="button" size="sm" variant={tab === 'home' ? 'secondary' : 'outline'} onClick={() => gotoTab('home')}>
                  Home
                </Button>
                <Button type="button" size="sm" variant={tab === 'artists' ? 'secondary' : 'outline'} onClick={() => gotoTab('artists')}>
                  Artists
                </Button>
                <Button type="button" size="sm" variant={tab === 'albums' ? 'secondary' : 'outline'} onClick={() => gotoTab('albums')}>
                  Albums
                </Button>
                <Button type="button" size="sm" variant={tab === 'genres' ? 'secondary' : 'outline'} onClick={() => gotoTab('genres')}>
                  Genres
                </Button>
                <Button type="button" size="sm" variant={tab === 'labels' ? 'secondary' : 'outline'} onClick={() => gotoTab('labels')}>
                  Labels
                </Button>
              </div>
            </Card>

            {/* Search + filters */}
            <div className="space-y-3">
              <div className="flex flex-col md:flex-row md:items-center gap-3">
                <div className="relative flex-1">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                  <Input
                    value={searchDraft}
                    onChange={(e) => setSearchDraft(e.target.value)}
                    placeholder="Search artists, albums, tracks, genres, labels…"
                    className="pl-9 pr-9 h-11 bg-background/80"
                  />
                  {searchDraft ? (
                    <button
                      type="button"
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                      onClick={() => setSearchDraft('')}
                      title="Clear search"
                    >
                      <X className="h-4 w-4" />
                    </button>
                  ) : null}
                </div>
                <div className="flex items-center gap-2">
                  <div className="flex items-center gap-2 rounded-lg border border-border/60 bg-background/80 px-3 py-2">
                    <Switch
                      checked={includeUnmatched}
                      onCheckedChange={(checked) => patch({ includeUnmatched: Boolean(checked) }, { replace: true })}
                    />
                    <span className="text-xs text-muted-foreground">Include non matched</span>
                  </div>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    className="gap-2"
                    onClick={() => setFiltersOpen((v) => !v)}
                  >
                    <ChevronDown className={cn('h-4 w-4 transition-transform', filtersOpen ? 'rotate-180' : '')} />
                    Filters
                  </Button>
                </div>
              </div>

              {filtersOpen ? (
                <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                  {showGenreFilter ? (
                    <FacetSuggestInput
                      label="Genre"
                      placeholder="Type a genre…"
                      value={genre}
                      fetchSuggestions={fetchGenreSuggestions}
                      onSelectValue={(v) => patch({ genre: v }, { replace: true })}
                      onBrowseValue={(v) => navigate(`/library/genre/${encodeURIComponent(v)}${location.search || ''}`)}
                      onClearValue={() => patch({ genre: '' }, { replace: true })}
                    />
                  ) : (
                    <div className="hidden md:block" />
                  )}

                  {showLabelFilter ? (
                    <FacetSuggestInput
                      label="Label"
                      placeholder="Type a label…"
                      value={label}
                      fetchSuggestions={fetchLabelSuggestions}
                      onSelectValue={(v) => patch({ label: v }, { replace: true })}
                      onBrowseValue={(v) => navigate(`/library/label/${encodeURIComponent(v)}${location.search || ''}`)}
                      onClearValue={() => patch({ label: '' }, { replace: true })}
                    />
                  ) : (
                    <div className="hidden md:block" />
                  )}

                  <div>
                    <div className="text-xs font-medium text-muted-foreground">Year</div>
                    <div className="mt-1">
                      <Select
                        value={year ? String(year) : 'all'}
                        onValueChange={(v) => patch({ year: v === 'all' ? null : Number(v) }, { replace: true })}
                        disabled={facetsLoading && years.length === 0}
                      >
                        <SelectTrigger className="h-10 bg-background/80">
                          <SelectValue placeholder="Any year" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="all">Any year</SelectItem>
                          {years.slice(0, 200).map((y) => (
                            <SelectItem key={`yr-${y.value}`} value={String(y.value)}>
                              {y.value} ({y.count})
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        </div>
      </div>

      <Outlet context={{ includeUnmatched, stats }} />
    </>
  );
}
