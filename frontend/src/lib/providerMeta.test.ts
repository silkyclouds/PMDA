import { describe, expect, it } from 'vitest';

import { getProviderMeta, normalizeProviderId } from '@/lib/providerMeta';

describe('providerMeta', () => {
  it('normalizes common aliases', () => {
    expect(normalizeProviderId('last.fm')).toBe('lastfm');
    expect(normalizeProviderId('wikipedia:en')).toBe('wikipedia');
    expect(normalizeProviderId('openai-codex')).toBe('openai-codex');
    expect(normalizeProviderId('media_cache')).toBe('media_cache');
  });

  it('returns fallback unknown for empty values', () => {
    expect(normalizeProviderId('')).toBe('unknown');
    expect(normalizeProviderId(null)).toBe('unknown');
  });

  it('returns provider metadata with canonical labels', () => {
    expect(getProviderMeta('musicbrainz').label).toBe('MusicBrainz');
    expect(getProviderMeta('lastfm').label).toBe('Last.fm');
    expect(getProviderMeta('wikipedia:fr').label).toBe('Wikipedia');
  });
});

