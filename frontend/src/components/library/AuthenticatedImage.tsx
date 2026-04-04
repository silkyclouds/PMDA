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

const PMDA_TRANSPARENT_PLACEHOLDER_BYTES = 68;
const PMDA_RETRY_DELAYS_MS = [1200, 3200, 6500, 12000, 20000];

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
  const [retryAttempt, setRetryAttempt] = useState(0);
  const fetchSrc = useMemo(() => {
    if (!normalizedSrc || !shouldFetchWithAuth || retryAttempt <= 0) return normalizedSrc;
    return `${normalizedSrc}${normalizedSrc.includes('?') ? '&' : '?'}r=${retryAttempt}`;
  }, [normalizedSrc, retryAttempt, shouldFetchWithAuth]);
  const objectUrlRef = useRef<string | null>(null);
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (retryTimerRef.current) clearTimeout(retryTimerRef.current);
      retryTimerRef.current = null;
      const prev = objectUrlRef.current;
      objectUrlRef.current = null;
      if (prev) URL.revokeObjectURL(prev);
    };
  }, []);

  useEffect(() => {
    setRetryAttempt(0);
    if (retryTimerRef.current) clearTimeout(retryTimerRef.current);
    retryTimerRef.current = null;
  }, [normalizedSrc]);

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
      const scheduleRetry = () => {
        if (!cancelled && retryAttempt < PMDA_RETRY_DELAYS_MS.length) {
          const delay = PMDA_RETRY_DELAYS_MS[retryAttempt];
          retryTimerRef.current = setTimeout(() => {
            setRetryAttempt((prev) => prev + 1);
          }, delay);
        }
      };
      try {
        const token = getAuthToken();
        const headers = token ? { Authorization: `Bearer ${token}` } : undefined;
        const res = await fetch(fetchSrc || normalizedSrc, { headers, cache: 'no-store' });
        if (!res.ok) {
          applyBlobUrl(null);
          scheduleRetry();
          return;
        }
        const blob = await res.blob();
        if (!blob || blob.size <= 0) {
          applyBlobUrl(null);
          scheduleRetry();
          return;
        }
        const looksLikePmdaPlaceholder = shouldFetchWithAuth
          && String(blob.type || '').toLowerCase() === 'image/png'
          && Number(blob.size || 0) === PMDA_TRANSPARENT_PLACEHOLDER_BYTES;
        if (looksLikePmdaPlaceholder) {
          applyBlobUrl(null);
          scheduleRetry();
          return;
        }
        applyBlobUrl(URL.createObjectURL(blob));
      } catch {
        applyBlobUrl(null);
        scheduleRetry();
      }
    };

    void load();

    return () => {
      cancelled = true;
    };
  }, [fetchSrc, normalizedSrc, retryAttempt, shouldFetchWithAuth]);

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
