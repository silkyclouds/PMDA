import { describe, expect, it } from 'vitest';

import type { LibraryAlbumItem } from '@/lib/api';
import { countNewAlbumsById, dedupeAlbumsForDisplay, mergeAlbumsForDisplay } from '@/lib/albumDisplayDedupe';

function makeAlbum(overrides: Partial<LibraryAlbumItem>): LibraryAlbumItem {
  return {
    album_id: 1,
    title: 'Album',
    year: 2024,
    genre: null,
    genres: [],
    label: null,
    track_count: 1,
    format: 'FLAC',
    is_lossless: true,
    sample_rate: 44100,
    bit_depth: 16,
    mb_identified: false,
    musicbrainz_release_group_id: null,
    discogs_release_id: null,
    lastfm_album_mbid: null,
    bandcamp_album_url: null,
    metadata_source: null,
    strict_match_provider: null,
    thumb: null,
    artist_id: 1,
    artist_name: 'Artist',
    short_description: null,
    profile_source: null,
    user_rating: null,
    public_rating: null,
    public_rating_votes: null,
    public_rating_source: null,
    heat_score: null,
    classical: null,
    publication_state: 'ready',
    cover_state: 'missing',
    artist_media_state: 'missing',
    profile_state: 'missing',
    ...overrides,
  };
}

describe('albumDisplayDedupe', () => {
  it('keeps distinct albums even when artist, title, and year are identical', () => {
    const deduped = dedupeAlbumsForDisplay([
      makeAlbum({
        album_id: 101,
        artist_name: 'Anton Bruckner',
        title: 'Nine Symphonies',
        year: 1967,
        track_count: 2,
        cover_state: 'missing',
      }),
      makeAlbum({
        album_id: 102,
        artist_name: 'Anton Bruckner',
        title: 'Nine Symphonies',
        year: 1967,
        track_count: 4,
        thumb: '/covers/102.jpg',
        cover_state: 'ready',
      }),
      makeAlbum({
        album_id: 103,
        artist_name: 'Anton Bruckner',
        title: 'Nine Symphonies',
        year: 1967,
        track_count: 3,
        thumb: '/covers/103.jpg',
        cover_state: 'ready',
      }),
    ]);

    expect(deduped).toHaveLength(3);
    expect(deduped.map((item) => item.album_id)).toEqual([101, 102, 103]);
  });

  it('keeps a full page of albums with repeated artist/title/year metadata', () => {
    const page = Array.from({ length: 96 }, (_, idx) => makeAlbum({
      album_id: 1_000 + idx,
      artist_name: 'Various Artists',
      title: 'Unknown Album',
      year: 2025,
      track_count: 1 + idx,
    }));

    expect(dedupeAlbumsForDisplay(page)).toHaveLength(96);
  });

  it('keeps distinct releases when the year changes', () => {
    const deduped = dedupeAlbumsForDisplay([
      makeAlbum({ album_id: 201, artist_name: 'Artist', title: 'Collected Works', year: 2001 }),
      makeAlbum({ album_id: 202, artist_name: 'Artist', title: 'Collected Works', year: 2005 }),
    ]);

    expect(deduped).toHaveLength(2);
    expect(deduped.map((item) => item.album_id)).toEqual([201, 202]);
  });

  it('dedupes exact album ids across paginated merges', () => {
    const merged = mergeAlbumsForDisplay(
      [
        makeAlbum({
          album_id: 301,
          artist_name: 'Wilhelm Furtwängler',
          title: 'The Collection',
          year: 2018,
          track_count: 1,
          cover_state: 'missing',
        }),
      ],
      [
        makeAlbum({
          album_id: 301,
          artist_name: 'Wilhelm Furtwängler',
          title: 'The Collection',
          year: 2018,
          track_count: 7,
          thumb: '/covers/302.jpg',
          cover_state: 'ready',
        }),
      ],
    );

    expect(merged).toHaveLength(1);
    expect(merged[0]?.album_id).toBe(301);
  });

  it('keeps rows when a legacy snapshot reuses an album id for different albums', () => {
    const deduped = dedupeAlbumsForDisplay([
      makeAlbum({ album_id: 4, artist_name: 'Artist A', title: 'Album A', year: 2020 }),
      makeAlbum({ album_id: 4, artist_name: 'Artist B', title: 'Album B', year: 2021 }),
    ]);

    expect(deduped).toHaveLength(2);
  });

  it('reports when a paginated page adds no new album ids', () => {
    const existing = [
      makeAlbum({ album_id: 501, artist_name: 'Artist', title: 'Album A' }),
      makeAlbum({ album_id: 502, artist_name: 'Artist', title: 'Album B' }),
    ];
    const duplicatePage = [
      makeAlbum({ album_id: 501, artist_name: 'Artist', title: 'Album A' }),
      makeAlbum({ album_id: 502, artist_name: 'Artist', title: 'Album B' }),
    ];
    const mixedPage = [
      makeAlbum({ album_id: 502, artist_name: 'Artist', title: 'Album B' }),
      makeAlbum({ album_id: 503, artist_name: 'Artist', title: 'Album C' }),
    ];

    expect(countNewAlbumsById(existing, duplicatePage)).toBe(0);
    expect(countNewAlbumsById(existing, mixedPage)).toBe(1);
  });
});
