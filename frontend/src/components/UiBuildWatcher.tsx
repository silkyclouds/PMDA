import { useEffect, useRef } from 'react';

import { toast } from '@/hooks/use-toast';
import { ToastAction } from '@/components/ui/toast';

type UiBuildResponse = {
  ok?: boolean;
  asset_js?: string | null;
  asset_css?: string | null;
  index_mtime?: number | null;
  error?: string;
};

function getCurrentMainAssetPath(): string | null {
  const scripts = Array.from(document.querySelectorAll('script[type="module"][src]')) as HTMLScriptElement[];
  const main = scripts.find((s) => {
    const src = s.getAttribute('src') || '';
    return src.startsWith('/assets/') && src.includes('index-') && src.endsWith('.js');
  });
  const src = main?.getAttribute('src');
  if (!src) return null;
  try {
    return new URL(src, window.location.href).pathname;
  } catch {
    return src;
  }
}

function normalizePath(input: string): string {
  try {
    return new URL(input, window.location.href).pathname;
  } catch {
    return input;
  }
}

export function UiBuildWatcher() {
  const shownRef = useRef(false);

  useEffect(() => {
    if (import.meta.env.DEV) return;

    let cancelled = false;

    const check = async () => {
      if (cancelled) return;
      if (shownRef.current) return;
      const current = getCurrentMainAssetPath();
      if (!current) return;

      try {
        const res = await fetch('/api/ui/build', { cache: 'no-store' });
        if (!res.ok) return;
        const data = (await res.json()) as UiBuildResponse;
        const serverAsset = data?.asset_js ? normalizePath(String(data.asset_js)) : null;
        if (!serverAsset) return;
        if (normalizePath(current) === serverAsset) return;

        shownRef.current = true;
        toast({
          title: 'New UI build available',
          description: 'PMDA was updated on the server. Reload to avoid stale assets and runtime errors.',
          action: (
            <ToastAction altText="Reload PMDA" onClick={() => window.location.reload()}>
              Reload
            </ToastAction>
          ),
          duration: 60_000,
        });
      } catch {
        // ignore
      }
    };

    const id = window.setInterval(check, 60_000);
    const t = window.setTimeout(check, 2_000);
    const onFocus = () => check();
    const onVis = () => {
      if (document.visibilityState === 'visible') check();
    };

    window.addEventListener('focus', onFocus);
    document.addEventListener('visibilitychange', onVis);

    return () => {
      cancelled = true;
      window.clearInterval(id);
      window.clearTimeout(t);
      window.removeEventListener('focus', onFocus);
      document.removeEventListener('visibilitychange', onVis);
    };
  }, []);

  return null;
}

