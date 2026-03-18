import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';

import { getAuthToken, normalizePmdaAssetUrl } from '@/lib/api';

interface AuthenticatedImageProps {
  src?: string | null;
  alt: string;
  className?: string;
  loading?: 'eager' | 'lazy';
  fetchPriority?: 'high' | 'low' | 'auto';
  decoding?: 'async' | 'sync' | 'auto';
  fallback?: ReactNode;
}

function isPmdaAssetUrl(url: string | null): boolean {
  if (!url) return false;
  return url.startsWith('/api/');
}

export function AuthenticatedImage({
  src,
  alt,
  className = 'h-full w-full object-cover',
  loading = 'lazy',
  fetchPriority = 'auto',
  decoding = 'async',
  fallback = null,
}: AuthenticatedImageProps) {
  const normalizedSrc = useMemo(() => {
    const txt = normalizePmdaAssetUrl(src);
    return txt || null;
  }, [src]);
  const shouldFetchWithAuth = isPmdaAssetUrl(normalizedSrc);
  const [blobSrc, setBlobSrc] = useState<string | null>(null);
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

    const applyBlobUrl = (next: string | null) => {
      if (cancelled) {
        if (next) URL.revokeObjectURL(next);
        return;
      }
      const prev = objectUrlRef.current;
      objectUrlRef.current = next;
      if (prev && prev !== next) URL.revokeObjectURL(prev);
      setBlobSrc(next);
    };

    if (!normalizedSrc || !shouldFetchWithAuth) {
      applyBlobUrl(null);
      return () => {
        cancelled = true;
      };
    }

    const load = async () => {
      try {
        const token = getAuthToken();
        const headers = token ? { Authorization: `Bearer ${token}` } : undefined;
        const res = await fetch(normalizedSrc, { headers });
        if (!res.ok) {
          applyBlobUrl(null);
          return;
        }
        const blob = await res.blob();
        if (!blob || blob.size <= 0) {
          applyBlobUrl(null);
          return;
        }
        applyBlobUrl(URL.createObjectURL(blob));
      } catch {
        applyBlobUrl(null);
      }
    };

    void load();

    return () => {
      cancelled = true;
    };
  }, [normalizedSrc, shouldFetchWithAuth]);

  const displaySrc = shouldFetchWithAuth ? blobSrc : normalizedSrc;
  if (!displaySrc) return <>{fallback}</>;

  return (
    <img
      src={displaySrc}
      alt={alt}
      className={className}
      loading={loading}
      fetchPriority={fetchPriority}
      decoding={decoding}
      onError={() => {
        const prev = objectUrlRef.current;
        objectUrlRef.current = null;
        if (prev) URL.revokeObjectURL(prev);
        setBlobSrc(null);
      }}
    />
  );
}
