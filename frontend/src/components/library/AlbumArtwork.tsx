import { useEffect, useMemo, useRef, useState } from 'react';
import { Music } from 'lucide-react';
import { getAuthToken, normalizePmdaAssetUrl } from '@/lib/api';

interface AlbumArtworkProps {
  albumThumb?: string | null;
  artistId?: number | null;
  alt: string;
  size?: number;
  priority?: boolean;
  imageClassName?: string;
  fallbackClassName?: string;
  iconClassName?: string;
}

function normalizeUrl(value?: string | null): string | null {
  const txt = normalizePmdaAssetUrl(value);
  return txt || null;
}

function withRequestedCoverSize(url: string | null, size: number): string | null {
  if (!url) return null;
  const px = Math.max(64, Math.min(2048, Math.floor(size)));
  if (!/\/api\/library\/files\/album\/\d+\/cover/i.test(url)) return url;
  if (/([?&])size=\d+/i.test(url)) {
    return url.replace(/([?&]size=)\d+/i, `$1${px}`);
  }
  return `${url}${url.includes('?') ? '&' : '?'}size=${px}`;
}

export function AlbumArtwork({
  albumThumb,
  artistId,
  alt,
  size = 512,
  priority = false,
  imageClassName = 'w-full h-full object-cover animate-in fade-in-0 duration-300',
  fallbackClassName = 'w-full h-full flex items-center justify-center',
  iconClassName = 'w-10 h-10 text-muted-foreground',
}: AlbumArtworkProps) {
  const albumUrl = useMemo(
    () => withRequestedCoverSize(normalizeUrl(albumThumb), size),
    [albumThumb, size]
  );
  const artistUrl = useMemo(() => {
    const id = Number(artistId || 0);
    if (!Number.isFinite(id) || id <= 0) return null;
    const px = Math.max(64, Math.min(2048, Math.floor(size)));
    return `/api/library/files/artist/${id}/image?size=${px}`;
  }, [artistId, size]);

  const [src, setSrc] = useState<string | null>(null);
  const objectUrlRef = useRef<string | null>(null);

  useEffect(() => {
    return () => {
      const prev = objectUrlRef.current;
      objectUrlRef.current = null;
      if (prev) URL.revokeObjectURL(prev);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    const applyObjectUrl = (next: string | null) => {
      if (cancelled) {
        if (next) URL.revokeObjectURL(next);
        return;
      }
      const prev = objectUrlRef.current;
      objectUrlRef.current = next;
      if (prev && prev !== next) URL.revokeObjectURL(prev);
      setSrc(next);
    };

    const candidates = [albumUrl, artistUrl].filter((value): value is string => Boolean(value));
    if (!candidates.length) {
      applyObjectUrl(null);
      return () => {
        cancelled = true;
      };
    }

    const load = async () => {
      for (let i = 0; i < candidates.length; i += 1) {
        const url = candidates[i];
        try {
          const token = getAuthToken();
          const headers = token ? { Authorization: `Bearer ${token}` } : undefined;
          const res = await fetch(url, { cache: 'no-cache', headers });
          if (!res.ok) continue;
          const blob = await res.blob();
          if (!blob || blob.size <= 0) continue;

          // Backend returns a tiny transparent PNG when no cover is available.
          // If album fallback is tiny and artist art exists, continue to next candidate.
          const hasArtistFallback = Boolean(artistUrl) && candidates.length > 1;
          const isAlbumCandidate = Boolean(albumUrl) && url === albumUrl;
          const isTransparentPlaceholder =
            isAlbumCandidate && hasArtistFallback && blob.type === 'image/png' && blob.size <= 512;
          if (isTransparentPlaceholder) continue;

          const blobUrl = URL.createObjectURL(blob);
          applyObjectUrl(blobUrl);
          return;
        } catch {
          // Ignore and try next candidate.
        }
      }
      applyObjectUrl(null);
    };

    void load();

    return () => {
      cancelled = true;
    };
  }, [albumUrl, artistUrl]);

  if (src) {
    return (
      <img
        src={src}
        alt={alt}
        loading={priority ? 'eager' : 'lazy'}
        fetchPriority={priority ? 'high' : 'auto'}
        decoding="async"
        className={imageClassName}
        onError={() => {
          const prev = objectUrlRef.current;
          objectUrlRef.current = null;
          if (prev) URL.revokeObjectURL(prev);
          setSrc(null);
        }}
      />
    );
  }

  return (
    <div className={fallbackClassName}>
      <Music className={iconClassName} />
    </div>
  );
}
