import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import { render } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';

import { ScanProgress } from '@/components/ScanProgress';
import { TooltipProvider } from '@/components/ui/tooltip';

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>();
  return {
    ...actual,
    getConfig: vi.fn(async () => ({})),
    getScanPreflight: vi.fn(async () => ({})),
    dedupeAll: vi.fn(async () => ({ ok: true })),
    improveAll: vi.fn(async () => ({ ok: true })),
    getDedupeProgress: vi.fn(async () => ({ deduping: false })),
    getImproveAllProgress: vi.fn(async () => ({ running: false })),
    getScanLogsTail: vi.fn(async () => []),
    getScalingRuntime: vi.fn(async () => null),
    getLibraryArtists: vi.fn(async () => ({ artists: [], total: 0 })),
    getLibraryAlbums: vi.fn(async () => ({ albums: [], total: 0 })),
  };
});

describe('ScanProgress live snapshot', () => {
  it('renders the current live progress snapshot without crashing', () => {
    const progress = JSON.parse(
      readFileSync('/tmp/pmda-progress-live.json', 'utf8'),
    );

    const queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
      },
    });

    expect(() => {
      render(
        <MemoryRouter>
          <TooltipProvider>
            <QueryClientProvider client={queryClient}>
              <ScanProgress
                progress={progress}
                currentDuplicateCount={0}
                onStart={() => {}}
                onPause={() => {}}
                onResume={() => {}}
                onStop={() => {}}
                onClear={() => {}}
                compact
              />
            </QueryClientProvider>
          </TooltipProvider>
        </MemoryRouter>,
      );
    }).not.toThrow();
  });
});
