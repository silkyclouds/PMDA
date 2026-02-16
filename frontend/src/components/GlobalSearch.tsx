import { useEffect, useMemo, useRef, useState, type KeyboardEventHandler } from 'react';
import { useNavigate } from 'react-router-dom';
import { Disc3, Loader2, Music2, Search, UserRound } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { cn } from '@/lib/utils';
import type { LibrarySearchSuggestionItem } from '@/lib/api';
import * as api from '@/lib/api';

export function GlobalSearch({ className }: { className?: string }) {
  const navigate = useNavigate();
  const rootRef = useRef<HTMLDivElement | null>(null);
  const [query, setQuery] = useState('');
  const [items, setItems] = useState<LibrarySearchSuggestionItem[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);

  const trimmed = query.trim();
  const includeUnmatched = true;

  useEffect(() => {
    if (!trimmed) {
      setItems([]);
      setOpen(false);
      return;
    }
    let cancelled = false;
    const id = setTimeout(async () => {
      setLoading(true);
      try {
        const res = await api.getLibrarySearchSuggestWithOptions(trimmed, 12, { includeUnmatched });
        if (cancelled) return;
        setItems(Array.isArray(res.items) ? res.items : []);
        setOpen(true);
        setActiveIndex(0);
      } catch {
        if (!cancelled) {
          setItems([]);
          setOpen(false);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }, 120);
    return () => {
      cancelled = true;
      clearTimeout(id);
    };
  }, [includeUnmatched, trimmed]);

  useEffect(() => {
    const onPointerDown = (evt: MouseEvent) => {
      if (!rootRef.current) return;
      if (!rootRef.current.contains(evt.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', onPointerDown);
    return () => document.removeEventListener('mousedown', onPointerDown);
  }, []);

  const groupedLabel = useMemo(() => {
    return (item: LibrarySearchSuggestionItem) => {
      if (item.type === 'artist') return 'Artist';
      if (item.type === 'album') return 'Album';
      return 'Track';
    };
  }, []);

  const goToItem = (item: LibrarySearchSuggestionItem | undefined) => {
    if (!item) return;
    if (item.type === 'track' && item.album_id) {
      const trackParam = item.track_id && item.track_id > 0 ? `?track_id=${item.track_id}` : '';
      navigate(`/library/album/${item.album_id}${trackParam}`);
    } else if (item.type === 'album' && item.album_id) {
      navigate(`/library/album/${item.album_id}`);
    } else if (item.artist_id) {
      navigate(`/library/artist/${item.artist_id}`);
    } else {
      navigate('/library');
    }
    setQuery('');
    setOpen(false);
  };

  const onKeyDown: KeyboardEventHandler<HTMLInputElement> = (evt) => {
    if (evt.key === 'ArrowDown') {
      evt.preventDefault();
      if (!items.length) return;
      setOpen(true);
      setActiveIndex((idx) => (idx + 1) % items.length);
      return;
    }
    if (evt.key === 'ArrowUp') {
      evt.preventDefault();
      if (!items.length) return;
      setOpen(true);
      setActiveIndex((idx) => (idx - 1 + items.length) % items.length);
      return;
    }
    if (evt.key === 'Enter') {
      evt.preventDefault();
      if (items.length > 0 && open) {
        goToItem(items[activeIndex]);
      } else if (items.length > 0) {
        goToItem(items[0]);
      }
      return;
    }
    if (evt.key === 'Escape') {
      setOpen(false);
    }
  };

  const iconFor = (type: LibrarySearchSuggestionItem['type']) => {
    if (type === 'artist') return <UserRound className="w-4 h-4 text-muted-foreground" />;
    if (type === 'album') return <Disc3 className="w-4 h-4 text-muted-foreground" />;
    return <Music2 className="w-4 h-4 text-muted-foreground" />;
  };

  return (
    <div ref={rootRef} className={cn('relative w-full max-w-[28rem]', className)}>
      <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none" />
      <Input
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onFocus={() => setOpen(items.length > 0)}
        onKeyDown={onKeyDown}
        placeholder="Search artist, album, track..."
        className="pl-9 pr-9 bg-background/90"
      />
      {loading ? (
        <Loader2 className="w-4 h-4 absolute right-3 top-1/2 -translate-y-1/2 animate-spin text-muted-foreground" />
      ) : null}

      {open && (
        <div className="absolute top-[calc(100%+0.35rem)] left-0 right-0 z-50 rounded-lg border border-border bg-popover shadow-xl overflow-hidden">
          {items.length === 0 ? (
            <div className="px-3 py-2.5 text-sm text-muted-foreground">No results</div>
          ) : (
            <div className="max-h-[26rem] overflow-y-auto">
              {items.map((item, idx) => (
                <button
                  key={`${item.type}:${item.artist_id ?? ''}:${item.album_id ?? ''}:${item.track_id ?? ''}:${item.title}:${idx}`}
                  type="button"
                  onMouseEnter={() => setActiveIndex(idx)}
                  onClick={() => goToItem(item)}
                  className={cn(
                    'w-full px-3 py-2.5 text-left border-b border-border/50 last:border-b-0 hover:bg-accent/70 transition-colors',
                    idx === activeIndex && 'bg-accent'
                  )}
                >
                  <div className="flex items-center gap-3 min-w-0">
                    <div className="w-9 h-9 rounded-md bg-muted overflow-hidden shrink-0 flex items-center justify-center">
                      {item.thumb ? (
                        <img src={item.thumb} alt={item.title} className="w-full h-full object-cover" />
                      ) : (
                        iconFor(item.type)
                      )}
                    </div>
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-sm truncate">{item.title}</span>
                        <span className="text-[10px] uppercase tracking-wider text-muted-foreground shrink-0">
                          {groupedLabel(item)}
                        </span>
                      </div>
                      {item.subtitle ? (
                        <div className="text-xs text-muted-foreground truncate mt-0.5">{item.subtitle}</div>
                      ) : null}
                    </div>
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
