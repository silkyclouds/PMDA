import { describe, expect, it } from 'vitest';

import { getProviderMeta, normalizeProviderId } from '@/lib/providerMeta';

describe('providerMeta', () => {
  it('normalizes common aliases', () => {
    expect(normalizeProviderId('last.fm')).toBe('lastfm');
    expect(normalizeProviderId('wikipedia:en')).toBe('wikipedia');
    expect(normalizeProviderId('openai-codex')).toBe('openai-codex');
    expect(normalizeProviderId('media_cache')).toBe('media_cache');
    expect(normalizeProviderId('apple music')).toBe('itunes');
    expect(normalizeProviderId('deezer')).toBe('deezer');
    expect(normalizeProviderId('spotify')).toBe('spotify');
    expect(normalizeProviderId('qobuz')).toBe('qobuz');
    expect(normalizeProviderId('tidal')).toBe('tidal');
  });

  it('returns fallback unknown for empty values', () => {
    expect(normalizeProviderId('')).toBe('unknown');
    expect(normalizeProviderId(null)).toBe('unknown');
  });

  it('returns provider metadata with canonical labels', () => {
    expect(getProviderMeta('musicbrainz').label).toBe('MusicBrainz');
    expect(getProviderMeta('lastfm').label).toBe('Last.fm');
    expect(getProviderMeta('wikipedia:fr').label).toBe('Wikipedia');
    expect(getProviderMeta('itunes').label).toBe('iTunes / Apple Music');
    expect(getProviderMeta('deezer').label).toBe('Deezer');
    expect(getProviderMeta('spotify').label).toBe('Spotify');
    expect(getProviderMeta('qobuz').label).toBe('Qobuz');
    expect(getProviderMeta('tidal').label).toBe('TIDAL');
  });
});
