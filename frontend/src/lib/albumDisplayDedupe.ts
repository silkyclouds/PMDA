import type { LibraryAlbumItem } from '@/lib/api';

function normalizePiece(value: unknown): string {
  return String(value ?? '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
}

function albumDisplayKey(item: LibraryAlbumItem): string {
  const artist = normalizePiece(item.artist_name);
  const title = normalizePiece(item.title);
  const year = normalizePiece(item.year);
  const tracks = normalizePiece(item.track_count);
  return `${artist}|${title}|${year}|${tracks}`;
}

export function dedupeAlbumsForDisplay(items: LibraryAlbumItem[]): LibraryAlbumItem[] {
  const out: LibraryAlbumItem[] = [];
  const seenIds = new Set<number>();
  const seenDisplayKeys = new Set<string>();
  for (const item of items) {
    const id = Number(item?.album_id || 0);
    if (!Number.isFinite(id) || id <= 0 || seenIds.has(id)) continue;
    seenIds.add(id);
    const key = albumDisplayKey(item);
    if (key && seenDisplayKeys.has(key)) continue;
    if (key) seenDisplayKeys.add(key);
    out.push(item);
  }
  return out;
}

export function mergeAlbumsForDisplay(existing: LibraryAlbumItem[], incoming: LibraryAlbumItem[]): LibraryAlbumItem[] {
  if (!existing.length) return dedupeAlbumsForDisplay(incoming);
  const out = [...existing];
  const seenIds = new Set(existing.map((item) => Number(item.album_id || 0)));
  const seenDisplayKeys = new Set(existing.map((item) => albumDisplayKey(item)));
  for (const item of incoming) {
    const id = Number(item?.album_id || 0);
    if (!Number.isFinite(id) || id <= 0 || seenIds.has(id)) continue;
    const key = albumDisplayKey(item);
    if (key && seenDisplayKeys.has(key)) continue;
    seenIds.add(id);
    if (key) seenDisplayKeys.add(key);
    out.push(item);
  }
  return out;
}
