#!/usr/bin/env python3
"""
Query Plex library database (plex-saturday) and output a status table:
- Duplicates (same artist + normalized title, multiple album entries)
- Same-folder (multiple Plex album entries pointing to same path)
- Missing thumb (no cover in Plex DB)
- MBID (from Plex guid if present; PMDA also checks file tags)
- Track count (and gap detection for "incomplete")

Usage:
  python scripts/plex_db_status.py /path/to/com.plexapp.plugins.library.db [section_name_or_id] [--csv]

  # On Unraid server (plex-saturday), from repo root or after copying script:
  python3 scripts/plex_db_status.py "/mnt/cache/plex-saturday/config/Library/Application Support/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db" "PMDA tests"

  # Or copy DB locally and run:
  python3 scripts/plex_db_status.py ./com.plexapp.plugins.library.db "PMDA tests"

Output: Markdown table + summary to stdout; use --csv for CSV.
"""

import re
import sqlite3
import sys
from pathlib import Path
from collections import defaultdict


def norm_album(title: str) -> str:
    """Normalize album title for grouping (match pmda norm_album logic)."""
    if not title:
        return ""
    raw = (title or "").strip()
    cleaned = re.sub(r"[\(\[][^(\)\]]*[\)\]]", "", raw)
    cleaned = " ".join(cleaned.split()).lower()
    if len(cleaned) >= 3:
        return cleaned
    fallback = raw.lower()
    fallback = " ".join(fallback.split())
    if len(fallback) >= 3:
        return fallback
    return fallback or raw.lower()


def main():
    args = sys.argv[1:]
    if not args:
        print("Usage: plex_db_status.py <path_to_com.plexapp.plugins.library.db> [section_name_or_id]", file=sys.stderr)
        sys.exit(1)
    db_path = Path(args[0])
    section_arg = args[1] if len(args) > 1 else None
    out_csv = "--csv" in args

    if not db_path.is_file():
        print(f"Error: DB file not found: {db_path}", file=sys.stderr)
        sys.exit(2)

    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row

    # Detect schema
    has_thumb = any(
        r[1] == "thumb" for r in conn.execute("PRAGMA table_info(metadata_items)")
    )
    has_guid = any(
        r[1] == "guid" for r in conn.execute("PRAGMA table_info(metadata_items)")
    )

    # Resolve section id(s)
    section_ids = []
    if section_arg:
        try:
            sid = int(section_arg)
            section_ids = [sid]
        except ValueError:
            # Try by name (library_sections or section name in metadata_items)
            try:
                row = conn.execute(
                    "SELECT id FROM library_sections WHERE name = ?", (section_arg,)
                ).fetchone()
                if row:
                    section_ids = [row[0]]
            except sqlite3.OperationalError:
                pass
            if not section_ids:
                # Fallback: try tag/name in a different table
                for row in conn.execute("SELECT id, name FROM library_sections").fetchall():
                    if section_arg.lower() in (row[1] or "").lower():
                        section_ids.append(row[0])
                        break
    if not section_ids:
        # All music sections (type 2 = music in Plex)
        try:
            for row in conn.execute(
                "SELECT id, name FROM library_sections WHERE section_type = 2"
            ).fetchall():
                section_ids.append(row[0])
        except sqlite3.OperationalError:
            for row in conn.execute(
                "SELECT DISTINCT library_section_id FROM metadata_items WHERE metadata_type = 8"
            ).fetchall():
                section_ids.append(row[0])

    if not section_ids:
        print("No library sections found.", file=sys.stderr)
        conn.close()
        sys.exit(3)

    placeholders = ",".join("?" for _ in section_ids)

    # Albums: id, parent_id (artist), title, thumb, guid, track_count, first_file_path
    artist_cache = {}

    albums_sql = f"""
    SELECT alb.id AS album_id, alb.parent_id, alb.title AS album_title,
           alb.library_section_id
    FROM metadata_items alb
    WHERE alb.metadata_type = 9 AND alb.library_section_id IN ({placeholders})
    ORDER BY alb.parent_id, alb.title
    """
    albums = conn.execute(albums_sql, section_ids).fetchall()

    # Get thumb/guid if columns exist
    extra_thumb = {}
    extra_guid = {}
    if has_thumb or has_guid:
        cols = []
        if has_thumb:
            cols.append("alb.thumb")
        else:
            cols.append("NULL AS thumb")
        if has_guid:
            cols.append("alb.guid")
        else:
            cols.append("NULL AS guid")
        extra_sql = f"""
        SELECT alb.id, {", ".join(cols)}
        FROM metadata_items alb
        WHERE alb.metadata_type = 9 AND alb.library_section_id IN ({placeholders})
        """
        for r in conn.execute(extra_sql, section_ids).fetchall():
            rid = r[0]
            if has_thumb and has_guid:
                extra_thumb[rid], extra_guid[rid] = r[1], r[2]
            elif has_thumb:
                extra_thumb[rid] = r[1]
            elif has_guid:
                extra_guid[rid] = r[1]
    extra = {}  # unused legacy; we use extra_thumb / extra_guid

    # Track count and first file path per album
    track_count_sql = """
    SELECT parent_id, COUNT(*) AS cnt
    FROM metadata_items
    WHERE metadata_type = 10 AND parent_id IN (
      SELECT id FROM metadata_items WHERE metadata_type = 9 AND library_section_id IN ({0})
    )
    GROUP BY parent_id
    """.format(placeholders)
    track_counts = {r[0]: r[1] for r in conn.execute(track_count_sql, section_ids).fetchall()}

    # Track indices per album (for gap detection = incomplete)
    indices_sql = """
    SELECT tr.parent_id, tr."index"
    FROM metadata_items tr
    WHERE tr.metadata_type = 10 AND tr.parent_id IN (
      SELECT id FROM metadata_items WHERE metadata_type = 9 AND library_section_id IN ({0})
    )
    ORDER BY tr.parent_id, tr."index"
    """.format(placeholders)
    indices_by_album = defaultdict(list)
    for r in conn.execute(indices_sql, section_ids).fetchall():
        indices_by_album[r[0]].append(r[1] or 0)

    path_sql = """
    SELECT tr.parent_id, mp.file
    FROM metadata_items tr
    JOIN media_items mi ON mi.metadata_item_id = tr.id
    JOIN media_parts mp ON mp.media_item_id = mi.id
    WHERE tr.metadata_type = 10 AND tr.parent_id IN (
      SELECT id FROM metadata_items WHERE metadata_type = 9 AND library_section_id IN ({0})
    )
    ORDER BY tr.parent_id
    """.format(placeholders)
    first_path = {}
    for r in conn.execute(path_sql, section_ids).fetchall():
        if r[0] not in first_path:
            first_path[r[0]] = (r[1] or "").strip()

    # Build album folder path (parent of file) for "same folder" detection
    def album_folder_path(album_id):
        p = first_path.get(album_id) or ""
        if not p:
            return ""
        return str(Path(p).parent)

    rows = []
    for a in albums:
        album_id = a["album_id"]
        parent_id = a["parent_id"]
        album_title = a["album_title"] or ""
        if parent_id not in artist_cache:
            r = conn.execute(
                "SELECT title FROM metadata_items WHERE id = ?", (parent_id,)
            ).fetchone()
            artist_cache[parent_id] = r[0] if r else ""
        artist = artist_cache[parent_id] or ""
        thumb_ok = True
        if album_id in extra_thumb:
            t = extra_thumb[album_id]
            thumb_ok = bool(t and str(t).strip())
        guid_ok = False
        if album_id in extra_guid:
            g = extra_guid[album_id]
            guid_ok = bool(g and ("musicbrainz" in str(g).lower() or "mbid" in str(g).lower()))
        tc = track_counts.get(album_id, 0)
        path = album_folder_path(album_id)
        norm = norm_album(album_title)
        idx_list = sorted(indices_by_album.get(album_id, []))
        has_gap = False
        if len(idx_list) >= 2:
            for i in range(len(idx_list) - 1):
                if idx_list[i + 1] - idx_list[i] > 1:
                    has_gap = True
                    break
        rows.append({
            "album_id": album_id,
            "artist": artist,
            "album_title": album_title,
            "norm": norm,
            "thumb_ok": thumb_ok,
            "guid_ok": guid_ok,
            "track_count": tc,
            "path": path,
            "has_gap": has_gap,
        })

    conn.close()

    # Duplicate groups: same (artist, norm)
    by_key = defaultdict(list)
    for r in rows:
        by_key[(r["artist"], r["norm"])].append(r)

    dup_groups = {k: v for k, v in by_key.items() if len(v) > 1}

    # Same-folder: within each dup group, which albums share the same path
    path_to_albums = defaultdict(list)
    for r in rows:
        if r["path"]:
            path_to_albums[r["path"]].append(r["album_id"])

    same_folder = {}
    for path, aids in path_to_albums.items():
        if len(aids) > 1:
            for aid in aids:
                same_folder[aid] = f"Same path as {len(aids)} album(s)"

    # Track gaps (incomplete): would need per-album track indices; skip if no track list
    # We only have track_count here; "incomplete" in PMDA is gap in indices. Omit for DB-only.

    # Build output table
    if out_csv:
        print("artist,album_title,album_id,duplicate_group_size,same_folder,thumb,mbid_guid,track_count,incomplete_gap,path")
        for r in rows:
            key = (r["artist"], r["norm"])
            grp_size = len(by_key[key])
            dup = str(grp_size) if grp_size > 1 else ""
            sf = same_folder.get(r["album_id"], "")
            thumb = "OK" if r["thumb_ok"] else "MISSING"
            mbid = "OK" if r["guid_ok"] else "MISSING"
            gap = "Yes" if r.get("has_gap") else ""
            path_esc = (r["path"] or "").replace('"', '""')
            print(f'"{r["artist"]}","{r["album_title"]}",{r["album_id"]},{dup},"{sf}","{thumb}","{mbid}",{r["track_count"]},"{gap}","{path_esc}"')
    else:
        # Markdown
        print("# Plex DB status – albums")
        print()
        print("Section(s):", section_ids)
        print()
        print("| Artist | Album | ID | Duplicate group | Same folder | Thumb | MBID (guid) | Tracks | Incomplete (gap) |")
        print("|--------|-------|-----|-----------------|-------------|-------|-------------|--------|------------------|")
        for r in rows:
            key = (r["artist"], r["norm"])
            grp_size = len(by_key[key])
            dup = f"{grp_size}×" if grp_size > 1 else ""
            sf = same_folder.get(r["album_id"], "")
            thumb = "OK" if r["thumb_ok"] else "**Missing**"
            mbid = "OK" if r["guid_ok"] else "**Missing**"
            gap = "**Yes**" if r.get("has_gap") else ""
            artist_esc = (r["artist"] or "").replace("|", "\\|")
            title_esc = (r["album_title"] or "").replace("|", "\\|")[:50]
            print(f"| {artist_esc} | {title_esc} | {r['album_id']} | {dup} | {sf} | {thumb} | {mbid} | {r['track_count']} | {gap} |")

        print()
        print("## Summary")
        print()
        total = len(rows)
        dup_albums = sum(len(v) for v in dup_groups.values())
        dup_groups_count = len(dup_groups)
        same_folder_count = len(same_folder)
        missing_thumb = sum(1 for r in rows if not r["thumb_ok"])
        missing_mbid = sum(1 for r in rows if not r["guid_ok"])
        zero_tracks = sum(1 for r in rows if r["track_count"] == 0)
        incomplete_gap = sum(1 for r in rows if r.get("has_gap"))
        print(f"- **Total albums:** {total}")
        print(f"- **Duplicate groups (same artist + title):** {dup_groups_count} groups, {dup_albums} albums involved")
        print(f"- **Same folder (multiple Plex entries, one path):** {same_folder_count} albums")
        print(f"- **Missing thumb (no cover in DB):** {missing_thumb}")
        print(f"- **Missing MBID in guid:** {missing_mbid} (PMDA also checks file tags)")
        print(f"- **Zero tracks:** {zero_tracks}")
        print(f"- **Incomplete (gap in track indices):** {incomplete_gap}")
        print()
        if dup_groups:
            print("## Duplicate groups (artist | normalized title | count)")
            print()
            for (artist, norm), group in sorted(dup_groups.items(), key=lambda x: -len(x[1])):
                paths = {album_folder_path(r["album_id"]) for r in group}
                print(f"- **{artist}** | `{norm}` | {len(group)} albums, {len(paths)} distinct path(s)")

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
