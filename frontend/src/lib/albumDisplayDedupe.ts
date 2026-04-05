import type { LibraryAlbumItem } from '@/lib/api';

export function dedupeAlbumsForDisplay(items: LibraryAlbumItem[]): LibraryAlbumItem[] {
  const out: LibraryAlbumItem[] = [];
  const seenIds = new Set<number>();
  for (const item of items) {
    const id = Number(item?.album_id || 0);
    if (!Number.isFinite(id) || id <= 0 || seenIds.has(id)) continue;
    seenIds.add(id);
    out.push(item);
  }
  return out;
}

export function mergeAlbumsForDisplay(existing: LibraryAlbumItem[], incoming: LibraryAlbumItem[]): LibraryAlbumItem[] {
  if (!existing.length) return dedupeAlbumsForDisplay(incoming);
  const out = [...existing];
  const seenIds = new Set(existing.map((item) => Number(item.album_id || 0)));
  for (const item of incoming) {
    const id = Number(item?.album_id || 0);
    if (!Number.isFinite(id) || id <= 0 || seenIds.has(id)) continue;
    seenIds.add(id);
    out.push(item);
  }
  return out;
}
