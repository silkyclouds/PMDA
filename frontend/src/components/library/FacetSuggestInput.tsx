import { useEffect, useRef, useState } from 'react';
import { Loader2, X } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { cn } from '@/lib/utils';
import type { LibraryFacetItem } from '@/lib/api';

export function FacetSuggestInput({
  label,
  placeholder,
  value,
  fetchSuggestions,
  onSelectValue,
  onBrowseValue,
  onClearValue,
}: {
  label: string;
  placeholder: string;
  value: string;
  fetchSuggestions: (query: string) => Promise<LibraryFacetItem[]>;
  onSelectValue: (value: string) => void;
  onBrowseValue?: (value: string) => void;
  onClearValue?: () => void;
}) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const [query, setQuery] = useState<string>(value || '');
  const [items, setItems] = useState<LibraryFacetItem[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const trimmed = query.trim();

  useEffect(() => {
    setQuery(value || '');
  }, [value]);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    const id = setTimeout(async () => {
      setLoading(true);
      try {
        const res = await fetchSuggestions(trimmed);
        if (cancelled) return;
        setItems(Array.isArray(res) ? res : []);
      } catch {
        if (!cancelled) setItems([]);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }, 140);
    return () => {
      cancelled = true;
      clearTimeout(id);
    };
  }, [open, trimmed, fetchSuggestions]);

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

  return (
    <div ref={rootRef} className="relative">
      <div className="flex items-center justify-between gap-2">
        <div className="text-xs font-medium text-muted-foreground">{label}</div>
        {value ? (
          <div className="flex items-center gap-2">
            {onBrowseValue ? (
              <Button
                type="button"
                size="sm"
                variant="ghost"
                className="h-7 px-2 text-xs"
                onClick={() => onBrowseValue(value)}
                title="Open page"
              >
                Open
              </Button>
            ) : null}
            {onClearValue ? (
              <Button
                type="button"
                size="icon"
                variant="ghost"
                className="h-7 w-7"
                onClick={() => onClearValue()}
                title="Clear"
              >
                <X className="h-4 w-4" />
              </Button>
            ) : null}
          </div>
        ) : null}
      </div>

      <div className="relative mt-1">
        <Input
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            if (!open) setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          placeholder={placeholder}
          className="h-10 bg-background/80"
        />
        {loading ? (
          <Loader2 className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 animate-spin text-muted-foreground" />
        ) : null}
      </div>

      {open ? (
        <div className="absolute top-[calc(100%+0.35rem)] left-0 right-0 z-50 rounded-lg border border-border bg-popover shadow-xl overflow-hidden">
          {items.length === 0 ? (
            <div className="px-3 py-2.5 text-sm text-muted-foreground">
              {trimmed ? 'No matches' : 'Start typing…'}
            </div>
          ) : (
            <div className="max-h-[18rem] overflow-y-auto">
              {items.map((it) => (
                <button
                  key={`${label}:${it.value}`}
                  type="button"
                  onClick={() => {
                    setQuery(it.value);
                    onSelectValue(it.value);
                    setOpen(false);
                  }}
                  className={cn(
                    'w-full px-3 py-2.5 text-left border-b border-border/50 last:border-b-0 hover:bg-accent/70 transition-colors'
                  )}
                  title={`${it.count} album(s)`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="min-w-0">
                      <div className="font-medium text-sm truncate">{it.value}</div>
                    </div>
                    <div className="text-xs text-muted-foreground tabular-nums shrink-0">{it.count}</div>
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}

