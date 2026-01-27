#!/usr/bin/env python3
"""
Discover correct PATH_MAP by content: for each Plex root, sample files from DB and find
which subdir of the music parent actually contains them. Run inside PMDA container or
with same DB + music layout. Aligns with backend _discover_bindings_by_content logic.
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

# Same extensions as pmda.py _PATH_VERIFY_EXTENSIONS
_PATH_VERIFY_EXTENSIONS = (
    "mp.file LIKE '%.flac' OR mp.file LIKE '%.wav' OR mp.file LIKE '%.m4a' OR mp.file LIKE '%.mp3'"
    " OR mp.file LIKE '%.ogg' OR mp.file LIKE '%.opus' OR mp.file LIKE '%.aac' OR mp.file LIKE '%.ape' OR mp.file LIKE '%.alac'"
    " OR mp.file LIKE '%.dsf' OR mp.file LIKE '%.aif' OR mp.file LIKE '%.aiff' OR mp.file LIKE '%.wma'"
    " OR mp.file LIKE '%.mp4' OR mp.file LIKE '%.m4b' OR mp.file LIKE '%.m4p' OR mp.file LIKE '%.aifc'"
)

DEFAULT_DB = "/database/com.plexapp.plugins.library.db"
DEFAULT_MUSIC_ROOT = "/music"
SAMPLES = 15


def load_path_map(path: str | None):
    """Load path_map: None => default plex roots; file path => JSON or key=value lines."""
    if path is None:
        return {p: p for p in ["/music/compilations", "/music/matched", "/music/unmatched"]}
    p = Path(path)
    if not p.exists():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)
    text = p.read_text(encoding="utf-8").strip()
    if text.startswith("{"):
        data = json.loads(text)
        return {str(k): str(v) for k, v in data.items()}
    out = {}
    for line in text.splitlines():
        line = line.strip()
        if "=" in line:
            k, _, v = line.partition("=")
            if k.strip():
                out[k.strip()] = v.strip()
    return out if out else {p: p for p in ["/music/compilations", "/music/matched", "/music/unmatched"]}


def main():
    ap = argparse.ArgumentParser(description="Discover path bindings by content (Plex DB + music root).")
    ap.add_argument("--db", default=DEFAULT_DB, help=f"Plex DB directory or path to library db (default: {DEFAULT_DB})")
    ap.add_argument("--music-root", default=DEFAULT_MUSIC_ROOT, help=f"Music parent path in container (default: {DEFAULT_MUSIC_ROOT})")
    ap.add_argument("--samples", type=int, default=SAMPLES, help=f"Sample size per plex root (default: {SAMPLES})")
    ap.add_argument("path_map_file", nargs="?", help="Optional JSON or key=value file for path_map; else default roots")
    args = ap.parse_args()

    db_path = args.db
    if not db_path.endswith(".db"):
        db_path = str(Path(db_path) / "com.plexapp.plugins.library.db")
    music_root = Path(args.music_root)
    samples = max(1, args.samples)

    if not Path(db_path).exists():
        print("DB not found:", db_path, file=sys.stderr)
        sys.exit(1)
    if not music_root.exists() or not music_root.is_dir():
        print("Music root not found or not a directory:", music_root, file=sys.stderr)
        sys.exit(1)

    path_map = load_path_map(args.path_map_file)
    if not path_map:
        print("PATH_MAP is empty.", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.text_factory = lambda b: b.decode("utf-8", "surrogateescape")
    cur = conn.cursor()

    candidates_cache = sorted([d for d in music_root.iterdir() if d.is_dir()], key=lambda x: str(x))

    for plex_root in path_map:
        cur.execute(
            f"""
            SELECT mp.file FROM media_parts mp
            WHERE mp.file LIKE ? AND ({_PATH_VERIFY_EXTENSIONS})
            ORDER BY RANDOM() LIMIT ?
            """,
            (plex_root + "/%", samples),
        )
        rows = [r[0] for r in cur.fetchall()]
        if not rows:
            print(f"{plex_root} -> (no files in DB)", flush=True)
            continue
        rels = [r[len(plex_root):].lstrip("/") for r in rows]
        best_path = None
        best_count = 0
        total = len(rels)
        for cand in candidates_cache:
            n = sum(1 for rel in rels if (cand / rel).exists())
            if n > best_count:
                best_count = n
                best_path = str(cand)
                if best_count == total:
                    break
        if best_count == 0:
            print(f"{plex_root} -> (no matching folder under music root)", flush=True)
        else:
            print(f"{plex_root} => {best_path}  ({best_count}/{total} files)", flush=True)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
