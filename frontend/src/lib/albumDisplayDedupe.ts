import type { LibraryAlbumItem } from '@/lib/api';

function albumIdValue(value: unknown): number {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function normalizeText(value: unknown): string {
  return String(value ?? '').trim().toLocaleLowerCase();
}

export function albumDisplayIdentity(item: LibraryAlbumItem): string {
  const id = albumIdValue(item?.album_id);
  const title = normalizeText(item?.title);
  const artist = normalizeText(item?.artist_name);
  const year = Number(item?.year || 0) || 0;
  return `${id}|${artist}|${title}|${year}`;
}

export function dedupeAlbumsForDisplay(items: LibraryAlbumItem[]): LibraryAlbumItem[] {
  const seen = new Set<string>();
  const out: LibraryAlbumItem[] = [];
  for (const item of items) {
    const id = albumIdValue(item?.album_id);
    if (!Number.isFinite(id) || id <= 0) continue;
    const identity = albumDisplayIdentity(item);
    if (seen.has(identity)) continue;
    seen.add(identity);
    out.push(item);
  }
  return out;
}

export function countNewAlbumsById(existing: LibraryAlbumItem[], incoming: LibraryAlbumItem[]): number {
  const seen = new Set(existing.filter((item) => albumIdValue(item.album_id) > 0).map(albumDisplayIdentity));
  let added = 0;
  for (const item of incoming) {
    const id = albumIdValue(item.album_id);
    if (id <= 0) continue;
    const identity = albumDisplayIdentity(item);
    if (seen.has(identity)) continue;
    seen.add(identity);
    added += 1;
  }
  return added;
}

export function mergeAlbumsForDisplay(existing: LibraryAlbumItem[], incoming: LibraryAlbumItem[]): LibraryAlbumItem[] {
  return dedupeAlbumsForDisplay([...existing, ...incoming]);
}
