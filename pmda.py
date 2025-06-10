#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PMDA V0.5.0

Summary:
- Scans your Plex music library to identify duplicate albums by exact track signatures and, optionally, fuzzy matching via AI.
- Selects the best edition based on format quality, bit depth, track count, and bitrate (with AI fallback).
- Presents a web UI to merge bonus tracks, review duplicates, and deduplicate automatically or selectively.
- Moves duplicate folders to a dedicated dupe root and cleans up Plex metadata.
- Don't be stupid, use it with caution ! 

"""

import argparse
import base64
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import NamedTuple, List, Dict, Optional
from urllib.parse import quote_plus

import requests
import openai
from flask import Flask, render_template_string, request, jsonify

# ───────────────────────────────── CONFIGURATION LOADING ─────────────────────────────────
CONFIG_PATH = Path(__file__).parent / "config.json"
if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"Configuration file not found: {CONFIG_PATH}")

import re
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    raw = f.read()
# Strip any stray control characters that break JSON parsing
raw = re.sub(r'[\x00-\x1f]', '', raw)
conf = json.loads(raw)

# Path to external AI prompt template
AI_PROMPT_FILE = Path(conf.get("AI_PROMPT_FILE", Path(__file__).parent / "ai_prompt.txt"))

# Optional custom format lists
FORMAT_PREFERENCE = conf.get("FORMAT_PREFERENCE", [
    "dsf","aif","aiff","wav","flac","m4a","mp4","m4b","m4p","aifc","ogg","mp3","wma"
])

required_keys = [
    "PLEX_DB_FILE", "PLEX_HOST", "PLEX_TOKEN",
    "SECTION_ID", "PATH_MAP", "DUPE_ROOT", "STATE_DB_FILE", "WEBUI_PORT",
    "CACHE_DB_FILE", "SCAN_THREADS", "OPENAI_API_KEY", "OPENAI_MODEL"
]
for key in required_keys:
    if key not in conf:
        raise KeyError(f"Missing required configuration key: '{key}' in {CONFIG_PATH}")

PLEX_DB_FILE    = conf["PLEX_DB_FILE"]
PLEX_HOST       = conf["PLEX_HOST"]
PLEX_TOKEN      = conf["PLEX_TOKEN"]
SECTION_ID      = int(conf["SECTION_ID"])
PATH_MAP        = {str(k): str(v) for k, v in conf["PATH_MAP"].items()}
DUPE_ROOT       = Path(conf["DUPE_ROOT"])
STATE_DB_FILE   = Path(conf["STATE_DB_FILE"])
CACHE_DB_FILE   = Path(conf["CACHE_DB_FILE"])
WEBUI_PORT      = int(conf["WEBUI_PORT"])
SCAN_THREADS    = int(conf["SCAN_THREADS"])
OPENAI_API_KEY = conf.get("OPENAI_API_KEY")
# Model to use for AI selection (configurable in config.json)
OPENAI_MODEL = conf.get("OPENAI_MODEL", "gpt-4")

# Initialize OpenAI API key if provided
if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY
else:
    logging.info("No OPENAI_API_KEY provided; AI-driven selection disabled.")

DUPE_ROOT.mkdir(parents=True, exist_ok=True)
STATE_DB_FILE.parent.mkdir(parents=True, exist_ok=True)
CACHE_DB_FILE.parent.mkdir(parents=True, exist_ok=True)

# ───────────────────────────────── OTHER CONSTANTS ──────────────────────────────────
AUDIO_RE    = re.compile(r"\.(flac|ape|alac|wav|m4a|aac|mp3|ogg)$", re.I)
# Derive format scores from user preference order
FMT_SCORE   = {ext: len(FORMAT_PREFERENCE)-i for i, ext in enumerate(FORMAT_PREFERENCE)}
OVERLAP_MIN = 0.85  # 85% track-title overlap minimum

# ───────────────────────────────── STATE DB SETUP ──────────────────────────────────
def init_state_db():
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    # Table for duplicate “best” entries
    cur.execute("""
        CREATE TABLE IF NOT EXISTS duplicates_best (
            artist      TEXT,
            album_id    INTEGER,
            title_raw   TEXT,
            album_norm  TEXT,
            folder      TEXT,
            fmt_text    TEXT,
            br          INTEGER,
            sr          INTEGER,
            bd          INTEGER,
            dur         INTEGER,
            discs       INTEGER,
            rationale   TEXT,
            merge_list  TEXT,
            ai_used     INTEGER DEFAULT 0,
            PRIMARY KEY (artist, album_id)
        )
    """)
    # Table for duplicate “loser” entries
    cur.execute("""
        CREATE TABLE IF NOT EXISTS duplicates_loser (
            artist      TEXT,
            album_id    INTEGER,
            folder      TEXT,
            fmt_text    TEXT,
            br          INTEGER,
            sr          INTEGER,
            bd          INTEGER,
            size_mb     INTEGER,
            FOREIGN KEY (artist, album_id) REFERENCES duplicates_best(artist, album_id)
        )
    """)
    # Table for stats like space_saved and removed_dupes
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            key   TEXT PRIMARY KEY,
            value INTEGER
        )
    """)
    # Initialize stats if missing
    for stat_key in ("space_saved", "removed_dupes"):
        cur.execute("INSERT OR IGNORE INTO stats(key, value) VALUES(?, 0)", (stat_key,))
    con.commit()
    con.close()

def get_stat(key: str) -> int:
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute("SELECT value FROM stats WHERE key = ?", (key,))
    row = cur.fetchone()
    con.close()
    return row[0] if row else 0

def set_stat(key: str, value: int):
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute("UPDATE stats SET value = ? WHERE key = ?", (value, key))
    con.commit()
    con.close()

def increment_stat(key: str, delta: int):
    current = get_stat(key)
    set_stat(key, current + delta)

init_state_db()

# ───────────────────────────────── CACHE DB SETUP ──────────────────────────────────
def init_cache_db():
    con = sqlite3.connect(str(CACHE_DB_FILE))
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS audio_cache (
            path       TEXT PRIMARY KEY,
            mtime      INTEGER,
            bit_rate   INTEGER,
            sample_rate INTEGER,
            bit_depth  INTEGER
        )
    """)
    con.commit()
    con.close()

def get_cached_info(path: str, mtime: int) -> Optional[tuple[int, int, int]]:
    # Open with a 30-second timeout so that concurrent reads/writes don't immediately error
    con = sqlite3.connect(str(CACHE_DB_FILE), timeout=30)
    cur = con.cursor()
    cur.execute("SELECT bit_rate, sample_rate, bit_depth, mtime FROM audio_cache WHERE path = ?", (path,))
    row = cur.fetchone()
    con.close()
    if row:
        br, sr, bd, cached_mtime = row
        if cached_mtime == mtime:
            return (br, sr, bd)
    return None

def set_cached_info(path: str, mtime: int, bit_rate: int, sample_rate: int, bit_depth: int):
    # Open with a 30-second timeout so concurrent writes wait instead of “database is locked”
    con = sqlite3.connect(str(CACHE_DB_FILE), timeout=30)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO audio_cache(path, mtime, bit_rate, sample_rate, bit_depth)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            mtime      = excluded.mtime,
            bit_rate   = excluded.bit_rate,
            sample_rate = excluded.sample_rate,
            bit_depth  = excluded.bit_depth
    """, (path, mtime, bit_rate, sample_rate, bit_depth))
    con.commit()
    con.close()

init_cache_db()

# ───────────────────────────────── STATE IN MEMORY ──────────────────────────────────
state = {
    "scanning": False,
    "scan_progress": 0,
    "scan_total": 0,
    "deduping": False,
    "dedupe_progress": 0,
    "dedupe_total": 0,
    # duplicates: { artist_name: [ { artist, album_id, best, losers } ] }
    "duplicates": {},
}
lock = threading.Lock()

# ───────────────────────────────── LOGGING ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)s │ %(message)s",
    datefmt="%H:%M:%S"
)

# ──────────────────────────────── PLEX DB helper ────────────────────────────────
def plex_connect() -> sqlite3.Connection:
    """
    Open the Plex SQLite DB using UTF-8 *surrogate-escape* decoding so that any
    non-UTF-8 bytes are mapped to the U+DCxx range instead of throwing an error.
    """
    con = sqlite3.connect(PLEX_DB_FILE, timeout=30)
    con.text_factory = lambda b: b.decode("utf-8", "surrogateescape")
    return con

# ───────────────────────────────── UTILITIES ──────────────────────────────────
def plex_api(path: str, method: str = "GET", **kw):
    headers = kw.pop("headers", {})
    headers["X-Plex-Token"] = PLEX_TOKEN
    return requests.request(method, f"{PLEX_HOST}{path}", headers=headers, timeout=60, **kw)

def container_to_host(p: str) -> Optional[Path]:
    for pre, real in PATH_MAP.items():
        if p.startswith(pre):
            return Path(real) / p[len(pre):].lstrip("/")
    return None

def folder_size(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())

def score_format(ext: str) -> int:
    return FMT_SCORE.get(ext.lower(), 0)

def norm_album(title: str) -> str:
    """
    Strip any parenthetical or bracketed content, collapse whitespace,
    lowercase, and trim.
    e.g. "Album Name (Special Edition) [HD]" → "album name"
    """
    # Remove any content in parentheses or brackets
    cleaned = re.sub(r"[\(\[][^\)\]]*[\)\]]", "", title)
    # Collapse multiple spaces into one and strip leading/trailing whitespace
    cleaned = " ".join(cleaned.split())
    return cleaned.lower()

def get_primary_format(folder: Path) -> str:
    for f in folder.rglob("*"):
        if AUDIO_RE.search(f.name):
            return f.suffix[1:].upper()
    return "UNKNOWN"

def thumb_url(album_id: int) -> str:
    return f"{PLEX_HOST}/library/metadata/{album_id}/thumb?X-Plex-Token={PLEX_TOKEN}"

def build_cards() -> list[dict]:
    """
    Convert the live state["duplicates"] structure into the list of card
    dictionaries expected by the front-end.  Called both by the initial
    page render and the /api/duplicates endpoint so that new cards appear
    incrementally while a scan is running.
    """
    cards: list[dict] = []
    for artist, groups in state["duplicates"].items():
        for g in groups:
            best = g["best"]
            best_fmt = best.get("fmt_text") or get_primary_format(Path(best["folder"]))
            cards.append(
                {
                    "artist_key": artist.replace(" ", "_"),
                    "artist": artist,
                    "album_id": best["album_id"],
                    "n": len(g["losers"]) + 1,
                    "best_thumb": thumb_url(best["album_id"]),
                    "best_title": best["title_raw"],
                    "best_fmt": best_fmt,
                    "formats": [best_fmt]
                    + [
                        l.get("fmt_text") or l.get("fmt") or get_primary_format(Path(l["folder"]))
                        for l in g["losers"]
                    ],
                    "used_ai": best.get("used_ai", False),
                }
            )
    return cards

# ───────────────────────────────── DATABASE HELPERS ──────────────────────────────────
class Track(NamedTuple):
    title: str
    idx: int
    disc: int
    dur: int  # duration in ms

def get_tracks(db_conn, album_id: int) -> List[Track]:
    has_parent = any(r[1] == "parent_index"
                     for r in db_conn.execute("PRAGMA table_info(metadata_items)"))
    sql = f"""
      SELECT tr.title, tr."index",
             {'tr.parent_index' if has_parent else 'NULL'} AS disc_no,
             mp.duration
      FROM metadata_items tr
      JOIN media_items mi ON mi.metadata_item_id = tr.id
      JOIN media_parts mp ON mp.media_item_id = mi.id
      WHERE tr.parent_id = ? AND tr.metadata_type = 10
    """
    rows = db_conn.execute(sql, (album_id,)).fetchall()
    return [Track(t.lower().strip(), i or 0, d or 1, dur or 0)
            for t, i, d, dur in rows]

def album_title(db_conn, album_id: int) -> str:
    return db_conn.execute(
        "SELECT title FROM metadata_items WHERE id = ?", (album_id,)
    ).fetchone()[0]

def first_part_path(db_conn, album_id: int) -> Optional[Path]:
    sql = """
      SELECT mp.file
      FROM metadata_items tr
      JOIN media_items mi ON mi.metadata_item_id = tr.id
      JOIN media_parts mp ON mp.media_item_id = mi.id
      WHERE tr.parent_id = ? LIMIT 1
    """
    r = db_conn.execute(sql, (album_id,)).fetchone()
    return container_to_host(r[0]).parent if r and container_to_host(r[0]) else None

def analyse_format(folder: Path) -> tuple[int, int, int, int]:
    """
    For a given album folder, scan the first audio file found and retrieve:
      (fmt_score, bit_rate, sample_rate, bit_depth)

    We treat FLAC specially: fetch “format=bit_rate” and “sample_fmt” to derive bit_depth.
    We cache results keyed by (path, mtime). If a FLAC cache entry is (0,0,0),
    force a re‐probe. SQLite connections use a 30s timeout to avoid “database is locked.”
    """
    # Scan all audio files, collect max values for each property
    fmt_score_max, br_max, sr_max, bd_max = 0, 0, 0, 0
    for f in folder.rglob("*"):
        if AUDIO_RE.search(f.name):
            ext = f.suffix[1:].lower()
            file_path = str(f)
            mtime = int(f.stat().st_mtime)
            # 1) Check cache
            cached = get_cached_info(file_path, mtime)
            if cached:
                br, sr, bd = cached
                if not (ext == "flac" and (br == 0 and sr == 0 and bd == 0)):
                    fmt_score = score_format(ext)
                    fmt_score_max = max(fmt_score_max, fmt_score)
                    br_max = max(br_max, br)
                    sr_max = max(sr_max, sr)
                    bd_max = max(bd_max, bd)
                    continue
                # If FLAC cache is zeroed, re-probe
            # 2) Build ffprobe command
            cmd = [
                "ffprobe", "-v", "error",
                "-select_streams", "a:0",
                "-show_entries", "format=bit_rate",
                "-show_entries", "stream=sample_rate,sample_fmt",
                "-of", "default=noprint_wrappers=1", file_path
            ]
            try:
                output = subprocess.check_output(
                    cmd, stderr=subprocess.DEVNULL, text=True, timeout=10
                )
                br = 0
                sr = 0
                bd = 0
                sample_fmt = None
                format_br = None
                for line in output.splitlines():
                    if line.startswith("bit_rate="):
                        value = line.split("=", 1)[1].strip()
                        try:
                            format_br = int(value)
                        except ValueError:
                            format_br = 0
                    elif line.startswith("sample_rate="):
                        value = line.split("=", 1)[1].strip()
                        try:
                            sr = int(value)
                        except ValueError:
                            sr = 0
                    elif line.startswith("sample_fmt="):
                        sample_fmt = line.split("=", 1)[1].strip()
                if format_br is not None:
                    br = format_br
                if sample_fmt:
                    m = re.match(r"s(\d+)", sample_fmt)
                    if m:
                        bd = int(m.group(1))
                    else:
                        bd = 0
                set_cached_info(file_path, mtime, br, sr, bd)
                fmt_score = score_format(ext)
                fmt_score_max = max(fmt_score_max, fmt_score)
                br_max = max(br_max, br)
                sr_max = max(sr_max, sr)
                bd_max = max(bd_max, bd)
            except Exception as e:
                set_cached_info(file_path, mtime, 0, 0, 0)
                fmt_score = score_format(ext)
                fmt_score_max = max(fmt_score_max, fmt_score)
                # Keep zeros for br, sr, bd
    return (fmt_score_max, br_max, sr_max, bd_max)

# ───────────────────────────────── DUPLICATE DETECTION ─────────────────────────────────
def signature(tracks: List[Track]) -> tuple:
    """
    Include track duration so that two albums with identical titles but
    different durations are NOT grouped. Each tuple is (disc, idx, title, dur).
    """
    return tuple(sorted((t.disc, t.idx, t.title, t.dur) for t in tracks))

def overlap(a: set, b: set) -> float:
    return len(a & b) / max(len(a), len(b))

def choose_best(editions: List[dict]) -> dict:
    """
    Selects the best edition either via OpenAI (if API key is available),
    or via heuristic, first reusing any previously cached AI result.
    """
    import sqlite3, json

    # 1) Reuse previously cached AI choice
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    ids = tuple(e['album_id'] for e in editions)
    placeholders = ",".join("?" for _ in ids)
    # on suppose que toutes les éditions ont le même artist
    artist = editions[0]['artist']
    cur.execute(
        f"SELECT album_id, rationale, merge_list "
        f"FROM duplicates_best "
        f"WHERE artist = ? AND album_id IN ({placeholders}) AND ai_used = 1",
        (artist, ) + ids
    )
    row = cur.fetchone()
    con.close()
    if row:
        prev_id, rationale, merge_json = row
        best = next(e for e in editions if e['album_id'] == prev_id)
        best["rationale"]  = rationale
        best["merge_list"] = json.loads(merge_json)
        best["used_ai"]    = True
        return best

    # 2) If no cached AI result, call OpenAI if API key is available
    used_ai = False
    if OPENAI_API_KEY:
        used_ai = True
        # Load prompt template from external file
        template = AI_PROMPT_FILE.read_text(encoding="utf-8")
        user_msg = template + "\nÉditions candidates :\n"
        for idx, e in enumerate(editions):
            user_msg += (
                f"{idx}: fmt_score={e['fmt_score']}, bitdepth={e['bd']}, "
                f"tracks={len(e['tracks'])}, files={e['file_count']}, "
                f"bitrate={e['br']}, samplerate={e['sr']}, duration={e['dur']}\n"
            )

        system_msg = (
            "You are an expert digital-music librarian. "
            "Reply **only** with: <index>|<brief rationale>|<comma-separated extra tracks>. "
            "Do not add anything before or after. "
            "If there are no extra tracks leave the third field empty but keep the trailing pipe."
        )
        try:
            resp = openai.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": user_msg},
                ],
                temperature=0.0,
                max_tokens=64,
            )
            txt = resp.choices[0].message.content.strip()
            p1, p2 = txt.find("|"), txt.find("|", txt.find("|") + 1)
            if p1 < 0 or p2 < 0:
                raise ValueError("Format de réponse IA invalide")

            idx       = int(re.search(r"\d+", txt[:p1]).group())
            rationale = txt[p1+1 : p2].strip()
            merge_raw = txt[p2+1 :].strip()
            merge_list = [t.strip() for t in merge_raw.split(",") if t.strip()]

            best = editions[idx]
            best.update({
                "rationale":  rationale,
                "merge_list": merge_list,
                "used_ai":    True,
            })
        except Exception as e:
            logging.warning(f"AI failed ({e}), using heuristic fallback")
            used_ai = False

    # 3) Heuristic fallback if AI is disabled or fails
    if not used_ai:
        best = max(
            editions,
            key=lambda e: (
                e["fmt_score"],
                e["bd"],
                len(e["tracks"]),
                e["file_count"],
                e["br"],
            ),
        )
        best["used_ai"] = False

        # mini-rationale
        others = [e for e in editions if e is not best]
        reasons = []
        if any(best["fmt_score"] > o["fmt_score"] for o in others):
            reasons.append("lossless format preferred")
        if any(best["bd"] > o["bd"] for o in others):
            reasons.append(f"bit-depth {best['bd']} bit higher")
        if any(len(best["tracks"]) > len(o["tracks"]) for o in others):
            reasons.append(f"{len(best['tracks'])} tracks (most)")
        if any(best["file_count"] > o["file_count"] for o in others):
            reasons.append("more audio files")
        if any(best["br"] > o["br"] for o in others):
            reasons.append(f"bitrate {best['br']//1000} kbps higher")
        best["rationale"] = "; ".join(reasons)

        # detect bonus tracks
        all_titles = best["titles"]
        extras = sorted({t for o in others for t in o["titles"] if t not in all_titles})
        best["merge_list"] = extras

    # 4) Persister en DB (INSERT OR IGNORE pour ne pas écraser un cache existant)
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO duplicates_best
          (artist, album_id, title_raw, album_norm, folder, fmt_text,
           br, sr, bd, dur, discs, rationale, merge_list, ai_used)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        best["artist"],
        best["album_id"],
        best["title_raw"],
        best["album_norm"],
        str(best["folder"]),
        get_primary_format(Path(best["folder"])),
        best["br"],
        best["sr"],
        best["bd"],
        best["dur"],
        best["discs"],
        best.get("rationale", ""),
        json.dumps(best.get("merge_list", [])),
        int(best.get("used_ai", False)),
    ))
    con.commit()
    con.close()

    return best
app = Flask(__name__)

def scan_artist_duplicates(args):
    """
    ThreadPool worker: scan one artist for duplicate albums.
    Returns (artist_name, list_of_groups, album_count).
    """
    artist_id, artist_name = args
    logging.debug(
        f"scan_artist_duplicates(): start '{artist_name}' (ID {artist_id})"
    )

    db_conn = plex_connect()

    album_ids = [
        row[0]
        for row in db_conn.execute(
            "SELECT id FROM metadata_items "
            "WHERE metadata_type=9 AND parent_id=?",
            (artist_id,),
        )
    ]

    groups = scan_duplicates(db_conn, artist_name, album_ids)
    db_conn.close()

    logging.debug(
        f"scan_artist_duplicates(): done '{artist_name}' – "
        f"{len(groups)} groups, {len(album_ids)} albums"
    )
    return (artist_name, groups, len(album_ids))

def scan_duplicates(db_conn, artist: str, album_ids: List[int]) -> List[dict]:
    editions = []
    for aid in album_ids:
        tr = get_tracks(db_conn, aid)
        if not tr:
            continue
        folder = first_part_path(db_conn, aid)
        if not folder:
            continue
        fmt_score, br, sr, bd = analyse_format(folder)
        editions.append({
            'album_id': aid,
            'title_raw': album_title(db_conn, aid),
            'album_norm': norm_album(album_title(db_conn, aid)),
            'artist': artist,
            'folder': folder,
            'tracks': tr,
            'file_count': sum(
                1 for f in folder.rglob("*") if AUDIO_RE.search(f.name)
            ),
            'sig': signature(tr),
            'titles': {t.title for t in tr},
            'dur': sum(t.dur for t in tr),
            'fmt_score': fmt_score, 'br': br, 'sr': sr, 'bd': bd,
            'discs': len({t.disc for t in tr})
        })

    # --- First pass: exact match on (album_norm, sig) ---
    from collections import defaultdict
    exact_groups = defaultdict(list)
    for e in editions:
        exact_groups[(e['album_norm'], e['sig'])].append(e)

    out = []
    used_ids = set()
    for ed_list in exact_groups.values():
        if len(ed_list) < 2:
            continue
        common = set.intersection(*(e['titles'] for e in ed_list))
        if not all(overlap(common, e['titles']) >= OVERLAP_MIN for e in ed_list):
            continue
        best = choose_best(ed_list)
        losers = [e for e in ed_list if e is not best]
        # Ensure rationale is preserved in group
        out.append({
            'artist': artist,
            'album_id': best['album_id'],
            'best': best,
            'losers': losers,
            'fuzzy': False
        })
        used_ids.update(e['album_id'] for e in ed_list)

    # --- Second pass: fuzzy match on album_norm only, for remaining editions ---
    norm_groups = defaultdict(list)
    for e in editions:
        if e['album_id'] not in used_ids:
            norm_groups[e['album_norm']].append(e)

    for ed_list in norm_groups.values():
        if len(ed_list) < 2:
            continue
        # Only perform fuzzy grouping via AI; skip if no API key
        if not OPENAI_API_KEY:
            continue
        # Force AI selection for fuzzy groups
        best = choose_best(ed_list)
        losers = [e for e in ed_list if e is not best]
        # Mark as fuzzy
        out.append({
            'artist': artist,
            'album_id': best['album_id'],
            'best': best,
            'losers': losers,
            'fuzzy': True
        })

    return out

def save_scan_to_db(scan_results: Dict[str, List[dict]]):
    """
    Given a dict of { artist_name: [group_dicts...] }, clear duplicates tables and re‐populate them.
    """
    import sqlite3, json
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()

    # 1) On vide complètement les tables
    cur.execute("DELETE FROM duplicates_loser")
    cur.execute("DELETE FROM duplicates_best")

    # 2) On ré-insère tout
    for artist, groups in scan_results.items():
        for g in groups:
            best = g['best']
            # Best edition
            cur.execute("""
                INSERT OR IGNORE INTO duplicates_best
                  (artist, album_id, title_raw, album_norm, folder,
                   fmt_text, br, sr, bd, dur, discs, rationale, merge_list, ai_used)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                artist,
                best['album_id'],
                best['title_raw'],
                best['album_norm'],
                str(best['folder']),
                get_primary_format(best['folder']),
                best['br'],
                best['sr'],
                best['bd'],
                best['dur'],
                best['discs'],
                best.get('rationale', ''),
                json.dumps(best.get('merge_list', [])),
                int(best.get('used_ai', False))
            ))

            # Toutes les éditions "loser"
            for e in g['losers']:
                size_mb = folder_size(e['folder']) // (1024 * 1024)
                cur.execute("""
                    INSERT INTO duplicates_loser
                      (artist, album_id, folder, fmt_text, br, sr, bd, size_mb)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    artist,
                    best['album_id'],
                    str(e['folder']),
                    get_primary_format(e['folder']),
                    e['br'],
                    e['sr'],
                    e['bd'],
                    size_mb
                ))

    # 3) Commit & close
    con.commit()
    con.close()

def load_scan_from_db() -> Dict[str, List[dict]]:
    """
    Read the most-recent duplicate-scan from STATE_DB_FILE and rebuild the
    in-memory structure used by the Web UI.

    Returns
    -------
    dict
        { artist_name : [ group_dict, ... ] }
    """
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()

    # ---- 1) Best editions -----------------------------------------------------
    cur.execute(
        """
        SELECT artist, album_id, title_raw, album_norm, folder,
               fmt_text, br, sr, bd, dur, discs, rationale, merge_list, ai_used
        FROM   duplicates_best
        """
    )
    best_rows = cur.fetchall()

    # ---- 2) Loser editions ----------------------------------------------------
    cur.execute(
        """
        SELECT artist, album_id, folder, fmt_text, br, sr, bd, size_mb
        FROM   duplicates_loser
        """
    )
    loser_rows = cur.fetchall()
    con.close()

    # Map losers by (artist, album_id) for quick lookup
    loser_map: Dict[tuple, List[dict]] = defaultdict(list)
    for artist, aid, folder, fmt, br, sr, bd, size_mb in loser_rows:
        loser_map[(artist, aid)].append(
            {
                "folder": Path(folder),
                "fmt": fmt,
                "br": br,
                "sr": sr,
                "bd": bd,
                "size": size_mb,
                "album_id": aid,
                "artist": artist,
                "title_raw": None,  # we may fill this in a moment
            }
        )

    results: Dict[str, List[dict]] = defaultdict(list)

    for (
        artist,
        aid,
        title_raw,
        album_norm,
        folder,
        fmt_txt,
        br,
        sr,
        bd,
        dur,
        discs,
        rationale,
        merge_list_json,
        ai_used,
    ) in best_rows:

        best_entry = {
            "album_id": aid,
            "title_raw": title_raw,
            "album_norm": album_norm,
            "folder": Path(folder),
            "fmt_text": fmt_txt,
            "br": br,
            "sr": sr,
            "bd": bd,
            "dur": dur,
            "discs": discs,
            "rationale": rationale,
            "merge_list": json.loads(merge_list_json) if merge_list_json else [],
            "used_ai": bool(ai_used),
        }

        losers = loser_map.get((artist, aid), [])

        # Some loser rows still need the readable title; fetch it from Plex DB.
        for l in losers:
            if l["title_raw"] is None:
                db_plx = plex_connect()
                l["title_raw"] = album_title(db_plx, aid)
                db_plx.close()

        results[artist].append(
            {
                "artist": artist,
                "album_id": aid,
                "best": best_entry,
                "losers": losers,
            }
        )

    return results

def clear_db_on_new_scan():
    """
    When a user triggers “Start New Scan,” wipe prior duplicates from both memory and DB.
    """
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute("DELETE FROM duplicates_loser")
    cur.execute("DELETE FROM duplicates_best")
    con.commit()
    con.close()
    with lock:
        state["duplicates"].clear()

# ───────────────────────────── BACKGROUND TASKS (WEB) ─────────────────────────────
def background_scan():
    """
    Scan the entire library in parallel, persist results to SQLite,
    and update the in-memory `state` for the Web UI.
    """
    logging.debug(f"background_scan(): opening Plex DB at {PLEX_DB_FILE}")
    db_conn = plex_connect()

    # 1) Total albums for progress bar
    with lock:
        state["scan_progress"] = 0
    total_albums = db_conn.execute(
        "SELECT COUNT(*) FROM metadata_items "
        "WHERE metadata_type=9 AND library_section_id=?",
        (SECTION_ID,),
    ).fetchone()[0]

    # 2) Fetch all artists
    artists = db_conn.execute(
        "SELECT id, title FROM metadata_items "
        "WHERE metadata_type=8 AND library_section_id=?",
        (SECTION_ID,),
    ).fetchall()
    db_conn.close()

    logging.debug(
        f"background_scan(): {len(artists)} artists, {total_albums} albums total"
    )

    # Reset live state
    with lock:
        state.update(scanning=True, scan_progress=0, scan_total=total_albums)
        state["duplicates"].clear()

    clear_db_on_new_scan()  # wipe previous duplicate tables

    all_results, futures = {}, []
    with ThreadPoolExecutor(max_workers=SCAN_THREADS) as executor:
        for artist_id, artist_name in artists:
            futures.append(
                executor.submit(scan_artist_duplicates, (artist_id, artist_name))
            )

        for future in as_completed(futures):
            artist_name, groups, album_cnt = future.result()
            with lock:
                all_results[artist_name] = groups
                if groups:
                    state["duplicates"][artist_name] = groups
                state["scan_progress"] += album_cnt

    save_scan_to_db(all_results)

    with lock:
        state["scanning"] = False
    logging.debug("background_scan(): finished")

def background_dedupe(all_groups: List[dict]):
    """
    Processes deduplication of all groups in a background thread.
    Updates stats in DB and in-memory state.
    """
    with lock:
        state.update(deduping=True, dedupe_progress=0, dedupe_total=len(all_groups))

    total_moved = 0
    removed_count = 0
    artists_to_refresh = set()

    for g in all_groups:
        moved = perform_dedupe(g)
        removed_count += len(g["losers"])
        total_moved += sum(item["size"] for item in moved)
        artists_to_refresh.add(g["artist"])
        with lock:
            state["dedupe_progress"] += 1
            logging.debug(f"background_dedupe(): processed group for '{g['artist']}|{g['best']['title_raw']}', dedupe_progress={state['dedupe_progress']}/{state['dedupe_total']}")
            # Remove this group from in-memory state
            if g["artist"] in state["duplicates"]:
                state["duplicates"][g["artist"]].remove(g)
                if not state["duplicates"][g["artist"]]:
                    del state["duplicates"][g["artist"]]

    # Update stats in DB
    increment_stat("space_saved", total_moved)
    increment_stat("removed_dupes", removed_count)
    logging.debug(f"background_dedupe(): updated stats: space_saved += {total_moved}, removed_dupes += {removed_count}")

    # Refresh Plex for all affected artists
    for artist in artists_to_refresh:
        letter = quote_plus(artist[0].upper())
        art_enc = quote_plus(artist)
        try:
            plex_api(f"/library/sections/{SECTION_ID}/refresh?path=/music/matched/{letter}/{art_enc}", method="GET")
            plex_api(f"/library/sections/{SECTION_ID}/emptyTrash", method="PUT")
        except Exception as e:
            logging.warning(f"background_dedupe(): plex refresh/emptyTrash failed for {artist}: {e}")

    with lock:
        state["deduping"] = False
    logging.debug("background_dedupe(): deduping completed")

# ─────────────────────────────────── SUPPORT FUNCTIONS ──────────────────────────────────
def fetch_cover_as_base64(album_id: int) -> Optional[str]:
    """
    Fetch album thumb from Plex as a base64 data‐URI.
    Returns None on failure.
    """
    try:
        resp = plex_api(f"/library/metadata/{album_id}/thumb")
        if resp.status_code == 200:
            b64 = base64.b64encode(resp.content).decode("utf-8")
            return f"data:image/jpeg;base64,{b64}"
    except Exception:
        pass
    return None

def perform_dedupe(group: dict) -> List[dict]:
    """
    Move each “loser” folder out to DUPE_ROOT, delete metadata in Plex,
    and return a list of dicts describing each moved item:
      {
       	"artist": <artist_name>,
       	"title_raw": <best_album_title>,
       	"size": <size_in_MB>,
       	"fmt": <format_text>,
       	"br": <bitrate_kbps>,
       	"sr": <sample_rate>,
       	"bd": <bit_depth>,
       	"thumb_data": <base64_data_uri_or_None>
      }
    """
    moved_items: List[dict] = []
    artist = group["artist"]
    best_title = group["best"]["title_raw"]
    cover_data = fetch_cover_as_base64(group["best"]["album_id"])

    for loser in group["losers"]:
        src_folder: Path = Path(loser["folder"])
        try:
            base_real = next(iter(PATH_MAP.values()))
            rel = src_folder.relative_to(base_real)
        except Exception:
            rel = src_folder.name

        # Build initial destination path under DUPE_ROOT
        dst = DUPE_ROOT / rel
        dst.parent.mkdir(parents=True, exist_ok=True)

        # If destination already exists, append “ (1)”, “ (2)”, etc.
        if dst.exists():
            base_name = dst.name
            parent_dir = dst.parent
            counter = 1
            while True:
                candidate = parent_dir / f"{base_name} ({counter})"
                if not candidate.exists():
                    dst = candidate
                    break
                counter += 1

        logging.debug(f"perform_dedupe(): moving {src_folder} → {dst}")
        try:
            shutil.move(str(src_folder), str(dst))
        except Exception as e:
            logging.warning(f"perform_dedupe(): failed to move {src_folder} → {dst}: {e}")
            continue

        size_mb = folder_size(dst) // (1024 * 1024)
        fmt_text = loser.get("fmt_text", loser.get("fmt", ""))
        br_kbps = loser["br"] // 1000
        sr = loser["sr"]
        bd = loser["bd"]

        loser_id = loser["album_id"]
        try:
            plex_api(f"/library/metadata/{loser_id}/trash", method="PUT")
            time.sleep(0.3)
            plex_api(f"/library/metadata/{loser_id}", method="DELETE")
        except Exception as e:
            logging.warning(f"perform_dedupe(): failed to delete Plex metadata for {loser_id}: {e}")

        moved_items.append({
            "artist":     artist,
            "title_raw":  best_title,
            "size":       size_mb,
            "fmt":        fmt_text,
            "br":         br_kbps,
            "sr":         sr,
            "bd":         bd,
            "thumb_data": cover_data
        })

    return moved_items

# ───────────────────────────────── HTML TEMPLATE ─────────────────────────────────
HTML = """<!DOCTYPE html>
<script>
  const PLEX_HOST = "{{ plex_host }}";
  const PLEX_TOKEN = "{{ plex_token }}";
</script>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>PMDA</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
    <style>
      body { font-family:Inter,Arial,sans-serif; background:#f5f7fa; margin:0; padding:2rem; }
      h1 { font-weight:600; margin-bottom:1rem; }
      button { cursor:pointer; border:none; border-radius:8px; padding:.5rem 1rem; font-weight:600; }
      #all { background:#e63946; color:#fff; margin-right:1rem; }
      #deleteSel { background:#d90429; color:#fff; margin-right:1rem; }
      #modeswitch { background:#1d3557; color:#fff; }
      #mergeAll { background:#e63946; color:#fff; margin-right:auto; }
      .stats-panel { position:fixed; top:1rem; right:1rem; display:flex; gap:.5rem; }
      .badge { background:#006f5f; color:#fff; padding:.4rem .8rem; border-radius:8px; font-size:.9rem; }
      .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(260px,1fr)); gap:1.2rem; }
      .card { background:#fff; padding:1rem; border-radius:12px; box-shadow:0 4px 14px rgba(0,0,0,.07);
             position:relative; cursor:pointer; transition:box-shadow .2s; display:flex; flex-direction:column; }
      .card:hover { box-shadow:0 6px 18px rgba(0,0,0,.12); }
      .card img { width:100%; border-radius:8px; margin-bottom:.5rem; }
      .tag { background:#eee; border-radius:6px; font-size:.7rem; padding:.1rem .4rem; margin-right:.3rem; }
      .btn-dedup { background:#006f5f; color:#fff; border:none; border-radius:6px;
                   font-size:.75rem; padding:.25rem .7rem; margin-top:.5rem; cursor:pointer; }
      .checkbox-grid { position:absolute; top:8px; left:8px; transform:scale(1.2); }
      .progress { width:100%; background:#ddd; border-radius:8px; overflow:hidden;
                  height:18px; margin:1rem 0; display:none; }
      .bar { background:#006f5f; height:100%; transition:width .3s; }
      #dedupeBox { margin-top:1rem; }
      #logo { display:block; margin:0 auto 1.5rem auto; max-width:400px; }
      .modal { position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,.6);
               display:none; align-items:center; justify-content:center; }
      .modal-content { background:#fff; border-radius:12px; padding:1.2rem;
                       width:600px; max-height:80%; overflow:auto; }
      .close { float:right; font-weight:600; cursor:pointer; }
      .ed-container { display:flex; flex-direction:column; gap:1rem; margin-top:1rem; }
      .edition { display:flex; gap:1rem; align-items:center; background:#f9f9f9;
                 border-radius:8px; padding:.6rem; font-size:.9rem; }
      .edition img { width:80px; height:80px; object-fit:cover; border-radius:4px; }
      #loadingSpinner { font-size:1rem; text-align:center; margin-top:2rem; }
      .table-mode { display:none; margin-top:1rem; }
      .table-mode table { width:100%; border-collapse:collapse; background:#fff;
                          box-shadow:0 4px 14px rgba(0,0,0,.07); }
      .table-mode th, .table-mode td { padding:.6rem; text-align:left; border-bottom:1px solid #ddd; }
      .table-mode th { background:#f0f0f0; }
      .table-row { cursor:pointer; }
      .checkbox-col { width:40px; }
      .cover-col img { width:50px; height:50px; object-fit:cover; border-radius:4px; }
      .row-dedup-btn { background:#006f5f; color:#fff; border:none; border-radius:6px;
                       font-size:.75rem; padding:.25rem .7rem; cursor:pointer; }
    </style>
  </head>
  <body>

    <img id="logo" src="/static/PMDA.png" alt="PMDA Logo"/>

    <div class="stats-panel">
      <!-- 1) Artists -->
      <div class="badge" id="totalArtists">Artists: {{ total_artists }}</div>
      <!-- 2) Albums -->
      <div class="badge" id="totalAlbums">Albums: {{ total_albums }}</div>
      <!-- 3) Removed dupes -->
      <div class="badge" id="removedDupes">Removed dupes: {{ removed_dupes }}</div>
      <!-- 4) Remaining Dupes -->
      <div class="badge" id="remainingDupes">Remaining Dupes: {{ remaining_dupes }}</div>
      <!-- 5) Space saved -->
      <div class="badge" id="saved">Space saved: {{ space_saved }} MB</div>
    </div>


<div style="display:flex; align-items:center; margin-bottom:1rem;">
  <button id="start" onclick="startScan()" style="background:#006f5f;color:#fff;margin-right:.5rem;">
    Start New Scan
  </button>
  <button id="deleteSel" onclick="submitSelected()" style="margin-right:.5rem;">
    Deduplicate Selected Groups
  </button>
  <button id="all" onclick="submitAll()" style="margin-right:.5rem;">
    Deduplicate ALL
  </button>
  <button id="mergeAll" onclick="submitMergeAll()">
    Merge and Deduplicate ALL
  </button>
  <input id="search" type="text" placeholder="Search artist or album..."
         style="padding:.4rem; border-radius:6px; border:1px solid #ccc;"/>
</div>

    <div id="scanBox" class="progress">
      <div id="scanBar" class="bar" style="width:0%"></div>
    </div>
    <!-- on rend toujours dispo le texte, mais caché par défaut -->
    <div id="scanTxt" style="display:block; margin-top:0.5rem;">0 / 0 albums</div>

    <!-- ==== Grid Mode ==== -->
    <div id="gridMode" class="grid">
      {% for g in groups %}
        <div class="card"
             data-artist="{{ g.artist_key }}"
             data-album-id="{{ g.album_id }}"
             data-title="{{ g.best_title }}">
          <input class="checkbox-grid" type="checkbox"
                 name="selected" value="{{ g.artist_key }}||{{ g.album_id }}"
                 onclick="event.stopPropagation();">
          <img src="{{ g.best_thumb }}" alt="cover">
          <div style="font-weight:600;">{{ g.artist }}</div>
          <div style="margin-bottom:.3rem;">{{ g.best_title }}</div>
          <div>
            <span class="tag">versions {{ g.n }}</span>
            <span class="tag">{{ g.best_fmt }}</span>
          </div>
          <button class="btn-dedup"
                  onclick="event.stopPropagation();
                           dedupeSingle({{ g.artist_key|tojson }},
                                        {{ g.album_id }},
                                        {{ g.best_title|tojson }});">
            Deduplicate
          </button>
        </div>
      {% endfor %}
    </div>

  <!-- ==== Table Mode ==== -->
  <div id="tableMode" class="table-mode">
    <table>
      <thead>
        <tr>
          <th class="checkbox-col"></th>
          <th class="cover-col"></th>
          <th>Artist</th>
          <th>Album</th>
          <th># Versions</th>
          <th>Detection Through</th>
          <th>Formats</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {% for g in groups %}
          <tr class="table-row"
              data-artist="{{ g.artist_key }}"
              data-album-id="{{ g.album_id }}"
              data-title="{{ g.best_title }}">
            <td class="checkbox-col">
              <input type="checkbox" name="selected"
                     value="{{ g.artist_key }}||{{ g.album_id }}"
                     onclick="event.stopPropagation();">
            </td>
            <td class="cover-col"><img src="{{ g.best_thumb }}" alt="cover"></td>
            <td>{{ g.artist }}</td>
            <td>{{ g.best_title }}</td>
            <td>{{ g.n }}</td>
            <td>{{ 'LLM' if g.used_ai else 'Signature Match' }}</td>
            <td>
              {{ g.formats|join(', ') }}
            </td>
            <td>
             <button class="row-dedup-btn"
                     onclick="event.stopPropagation();
                              dedupeSingle({{ g.artist_key|tojson }},
                                           {{ g.album_id }},
                                           {{ g.best_title|tojson }});">
               Deduplicate
             </button>
            </td>
          </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
    <!-- end always-show grid/table; removed server-side “no duplicates” branch -->

    <!-- ==== Modal for Edition Details & Confirmations ==== -->
    <div id="modal" class="modal">
      <div class="modal-content">
        <span class="close" onclick="closeModal()">&times;</span>
        <div id="modalBody"></div>
      </div>
    </div>
    <script>
      // ────────────────────── timers & view mode ──────────────────────
      let scanTimer   = null;
      let dedupeTimer = null;

      /*  true  → user last chose Table view
          false → user last chose Grid  view (default)                  */
      let inTableMode = (localStorage.getItem("pmdaViewMode") === "table");

      /* ─── View helpers ─────────────────────────────────────────────── */
      function setViewMode() {
        const gridEl  = document.getElementById("gridMode");
        const tableEl = document.getElementById("tableMode");

        if (gridEl)  gridEl.style.display  = inTableMode ? "none"  : "grid";
        if (tableEl) tableEl.style.display = inTableMode ? "block" : "none";

        const switchBtn = document.getElementById("modeswitch");
        if (switchBtn) {
          switchBtn.innerText = inTableMode
                                ? "Switch to Grid View"
                                : "Switch to Table View";
        }
      }

      function toggleMode() {
        inTableMode = !inTableMode;
        localStorage.setItem("pmdaViewMode", inTableMode ? "table" : "grid");
        setViewMode();
      }

      /* ─── Scan progress polling ────────────────────────────────────── */
      let currentDupTotal = 0;        // how many dup groups are on screen

      /* build ONE card/row – shared by grid & table ----------------------- */
      function dedupeCardHtml(g){
        return `
          <div class="card"
               data-artist="${g.artist_key}"
               data-album-id="${g.album_id}"
               data-title="${g.best_title}">
            <input class="checkbox-grid" type="checkbox"
                   name="selected" value="${g.artist_key}||${g.album_id}"
                   onclick="event.stopPropagation();">
            <img src="${g.best_thumb}" alt="cover">
            <div style="font-weight:600;">${g.artist}</div>
            <div style="margin-bottom:.3rem;">${g.best_title}</div>
            <div><span class="tag">versions ${g.n}</span>
                 <span class="tag">${g.best_fmt}</span></div>
            <button class="btn-dedup"
                    onclick="event.stopPropagation();
                             dedupeSingle('${g.artist_key}',
                                          ${g.album_id},
                                          ${JSON.stringify(g.best_title)});">
              Deduplicate
            </button>
          </div>`;
      }

      /* redraw grid + (optionally) table ---------------------------------- */
      function renderDuplicates(list){
        // GRID
        const grid = document.getElementById("gridMode");
        if (grid){
          grid.innerHTML = list.map(dedupeCardHtml).join("");
          // re-attach click handlers to new cards
          grid.querySelectorAll(".card").forEach(card=>{
            card.addEventListener("click",()=>openModal(card.dataset.artist,
                                                        card.dataset.albumId));
          });
        }
        // TABLE (show only when in Table view)
        const tbody = document.querySelector("#tableMode tbody");
        if (tbody){
          tbody.innerHTML = list.map(g=>`
            <tr class="table-row"
                data-artist="${g.artist_key}"
                data-album-id="${g.album_id}"
                data-title="${g.best_title}">
              <td class="checkbox-col">
                <input type="checkbox" name="selected"
                       value="${g.artist_key}||${g.album_id}"
                       onclick="event.stopPropagation();">
              </td>
              <td class="cover-col"><img src="${g.best_thumb}" alt="cover"></td>
              <td>${g.artist}</td>
              <td>${g.best_title}</td>
              <td>${g.n}</td>
              <td>${g.used_ai ? "LLM" : "Signature Match"}</td>
              <td>${g.formats.join(", ")}</td>
              <td><button class="row-dedup-btn"
                          onclick="event.stopPropagation();
                                   dedupeSingle('${g.artist_key}',
                                                ${g.album_id},
                                                ${JSON.stringify(g.best_title)});">
                    Deduplicate</button></td></tr>`).join("");
          tbody.querySelectorAll(".table-row").forEach(row=>{
            row.addEventListener("click",()=>openModal(row.dataset.artist,
                                                       row.dataset.albumId));
          });
        }
      }

      function pollScan() {
            fetch("/api/progress")
                .then(r => r.json())
                .then(j => {
                    if (j.scanning) {
                        const scanBox = document.getElementById("scanBox");
                        if (scanBox) {
                            scanBox.style.display = "block";
                        }
                        const scanBar = document.getElementById("scanBar");
                        if (scanBar) {
                            const pct = j.total ? Math.round(100 * j.progress / j.total) : 0;
                            scanBar.style.width = pct + "%";
                        }
                        const scanTxt = document.getElementById("scanTxt");
                        if (scanTxt) {
                            scanTxt.innerText = `${j.progress} / ${j.total} albums`;
                        }
                        // live-refresh duplicate cards as they arrive
                        fetch("/api/duplicates")
                            .then(r => r.json())
                            .then(dups => {
                                /* Only redraw when something new arrived */
                                if (dups.length !== currentDupTotal) {
                                    currentDupTotal = dups.length;
                                    renderDuplicates(dups);
                                }
                            });
                    } else {
                        clearInterval(scanTimer);
                    }
                });
      }
      function submitMergeAll() {
        showLoadingModal("Merging all duplicates...");
        fetch("/merge/all", { method: "POST" })
          .then(() => {
            showLoadingModal("Merging and deduplicating all duplicates...");
            fetch("/dedupe/all", { method: "POST" })
              .then(() => {
                dedupeTimer = setInterval(pollDedupe, 1000);
              });
          });
      }

      /* ─── Start scan ───────────────────────────────────────────────── */
      function startScan() {
            fetch("/start", { method: "POST" })
                .then(() => {
                    const scanBox = document.getElementById("scanBox");
                    if (scanBox) {
                        scanBox.style.display = "block";
                    }
                    const scanTxt = document.getElementById("scanTxt");
                    if (scanTxt) {
                        scanTxt.style.display = "block";
                    }
                    scanTimer = setInterval(pollScan, 2000);
                });
      }

      /* ─── Dedupe helpers & polling ─────────────────────────────────── */
      function pollDedupe() {
        fetch("/api/dedupe")
          .then(r => r.json())
          .then(j => {
            if (!j.deduping) {
              clearInterval(dedupeTimer);
              showSimpleModal(`Moved ${j.saved} MB in total`);
              setTimeout(() => location.reload(), 3000);
            }
          });
      }

      function submitAll() {
        fetch("/dedupe/all", { method: "POST" }).then(() => {
          showLoadingModal("Moving all duplicates…");
          dedupeTimer = setInterval(pollDedupe, 1000);
        });
      }

      function submitSelected() {
        const checked = Array.from(
          document.querySelectorAll("input[name='selected']:checked")
        ).map(cb => cb.value);

        if (!checked.length) {
          showSimpleModal("No albums selected.");
          return;
        }

        showLoadingModal("Moving selected duplicates…");
        fetch("/dedupe/selected", {
          method:  "POST",
          headers: { "Content-Type": "application/json" },
          body:    JSON.stringify({ selected: checked })
        })
        .then(r => r.json())
        .then(resp => showConfirmation(resp.moved))
        .catch(() => {
          closeModal();
          showSimpleModal("An error occurred during deduplication.");
        });
      }

      function dedupeSingle(artist, albumId, title) {
        showLoadingModal(`Moving duplicate for ${artist.replace(/_/g," ")} – ${title}`);
        fetch(`/dedupe/artist/${artist}`, {
          method:  "POST",
          headers: { "Content-Type": "application/json" },
          body:    JSON.stringify({ album_id: albumId })
        })
        .then(r => r.json())
        .then(resp => showConfirmation(resp.moved))
        .catch(() => {
          closeModal();
          showSimpleModal("An error occurred during single deduplication.");
        });
      }

      /* ─── Modal helpers ─────────────────────────────────────────────── */
      function showLoadingModal(text) {
        const modalBody = document.getElementById("modalBody");
        if (modalBody) modalBody.innerHTML = `<div id="loadingSpinner">${text}</div>`;
        const modal = document.getElementById("modal");
        if (modal) modal.style.display = "flex";
      }

      function showSimpleModal(msg) {
        const modalBody = document.getElementById("modalBody");
        if (modalBody) modalBody.innerHTML = `<h3>${msg}</h3>`;
        const modal = document.getElementById("modal");
        if (modal) modal.style.display = "flex";
        setTimeout(closeModal, 3000);
      }

    function showConfirmation(moved) {
    const defaultThumb =
        'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAYAAACqaXHeAAAAQklEQVR42u3BAQ0AAADCIPunNscwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD8wDeQAAEmTWlUAAAAASUVORK5CYII=';
    const modalBody = document.getElementById("modalBody");
    const modal     = document.getElementById("modal");

    // Build content
    let html = `<h3>Moved Duplicates</h3>`;
    if (moved.length === 0) {
        html += `<p>No duplicates were moved.</p>`;
    } else {
        html += `<div class="ed-container">`;
        moved.forEach(e => {
        html += `
            <div class="edition">
            <img src="${e.thumb_data || defaultThumb}" alt="cover">
            <div class="edition-info">
                <strong>Duplicate</strong><br>
                ${e.artist} — ${e.title_raw}<br>
                ${e.size} MB · ${e.fmt} · ${e.br} kbps · ${e.sr} Hz · ${e.bd} bit
            </div>
            </div>`;
        });
        html += `</div>`;
    }

    // Close button
    html += `
        <div class="modal-actions" style="text-align:right; margin-top:1rem;">
        <button id="modalCloseBtn" style="
            background:#006f5f;color:#fff;border:none;
            border-radius:6px;padding:.5rem 1rem;
            cursor:pointer;
        ">Close</button>
        </div>`;

    // Render & show
    modalBody.innerHTML = html;
    modal.style.display = "flex";

    // Wire up Escape key and Close button
    function closeHandler() {
        closeModal();
        document.removeEventListener("keydown", escHandler);
    }
    function escHandler(e) {
        if (e.key === "Escape") closeHandler();
    }
    document.getElementById("modalCloseBtn").onclick = closeHandler;
    document.addEventListener("keydown", escHandler);
    }

      function closeModal() {
        const modal = document.getElementById("modal");
        if (modal) modal.style.display = "none";
      }

      /* ─── Details modal ─────────────────────────────────────────────── */
      function openModal(artist, albumId) {
        showLoadingModal("Loading album details…");
        fetch(`/details/${artist}/${albumId}`)
          .then(r => { if (!r.ok) throw new Error("404"); return r.json(); })
          .then(j => {
            let html = `<h3>${j.artist} – ${j.album}</h3>`;
            // Insert rationale as a numbered list if present
            if (j.rationale) {
              const items = j.rationale.split(";");
              html += '<ul style="margin-left:1.2rem;list-style-type:disc;">';
              items.forEach(it => { if (it.trim()) html += `<li>${it.trim()}</li>`; });
              html += "</ul>";
            }
            // Only show merge section if there are extras
            if (j.merge_list && j.merge_list.length > 0) {
              html += `<div><strong>Detected extra tracks:</strong>`;
              html += `<ul style="margin-left:1.2rem;list-style-type:disc;">`;
              j.merge_list.forEach(function(track) {
                html += `<li>${track}</li>`;
              });
              html += `</ul></div>`;
            }
            html += `<div class="ed-container">`;
            j.editions.forEach((e, i) => {
              html += `<div class="edition">`;
              if (e.thumb_data) {
                html += `<img src="${e.thumb_data}" alt="cover">`;
              } else {
                html += `<img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAYAAACqaXHeAAAAQklEQVR42u3BAQ0AAADCIPunNscwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD8wDeQAAEmTWlUAAAAASUVORK5CYII=" alt="no-cover">`;
              }
              html += `<div><b>${i === 0 ? "Best" : "Duplicate"}</b></div>`;
              html += `<div>${j.artist}</div><div>${j.album}</div><div>${e.size} MB</div>`;
              html += `<div>${e.fmt} • ${e.br} kbps • ${e.sr} Hz • ${e.bd} bit</div></div>`;
            });
            html += `</div><button id="modalDedup" style="background:#006f5f;color:#fff;border:none;border-radius:8px;padding:.4rem .9rem;cursor:pointer;margin-top:1rem;">Deduplicate</button>`;
            // Only show merge/merge+dedup buttons if extras
            if (j.merge_list && j.merge_list.length > 0) {
              html += `<button id="modalMerge" style="background:#1d3557;color:#fff;border:none;border-radius:8px;padding:.4rem .9rem;cursor:pointer;margin-top:1rem;margin-left:1rem;">Merge Tracks</button>`;
              html += `<button id="modalMergeDedup" style="background:#1d3557;color:#fff;border:none;border-radius:8px;padding:.4rem .9rem;cursor:pointer;margin-top:1rem;margin-left:1rem;">Merge and Deduplicate</button>`;
            }

            const modalBody = document.getElementById("modalBody");
            if (modalBody) modalBody.innerHTML = html;
            const modal = document.getElementById("modal");
            if (modal) modal.style.display = "flex";

            document.getElementById("modalDedup").onclick = () => {
              showLoadingModal(`Moving duplicate for ${j.artist} – ${j.album}`);
              fetch(`/dedupe/artist/${artist}`, {
                method:  "POST",
                headers: { "Content-Type": "application/json" },
                body:    JSON.stringify({ album_id: albumId })
              })
              .then(r => r.json())
              .then(resp => showConfirmation(resp.moved))
              .catch(() => {
                closeModal();
                showSimpleModal("An error occurred during modal deduplication.");
              });
            };
            if (j.merge_list && j.merge_list.length > 0) {
              const mergeBtn = document.getElementById("modalMerge");
              if (mergeBtn) {
                mergeBtn.onclick = () => {
                  // send merge request to backend
                  fetch(`/merge/${artist}/${albumId}`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ merge_list: j.merge_list })
                  })
                  .then(r => r.json())
                  .then(resp => showSimpleModal(resp.message))
                  .catch(() => showSimpleModal("Merge failed."));
                };
              }
              const mergeDedupBtn = document.getElementById("modalMergeDedup");
              if (mergeDedupBtn) {
                mergeDedupBtn.onclick = () => {
                  // First merge tracks, then deduplicate
                  fetch(`/merge/${artist}/${albumId}`, { method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({merge_list:j.merge_list}) })
                    .then(() => {
                      showLoadingModal(`Merging tracks then deduplicating ${j.artist} – ${j.album}`);
                      fetch(`/dedupe/artist/${artist}`, { method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({album_id:albumId}) })
                        .then(r => r.json()).then(resp => showSimpleModal(resp.message))
                        .catch(() => showSimpleModal("Merge+Dedup failed."));
                    })
                    .catch(() => showSimpleModal("Merge failed."));
                };
              }
            }
          })
          .catch(() => {
            closeModal();
            showSimpleModal("Could not load album details.");
          });
      }

      /* ─── Startup ───────────────────────────────────────────────────── */
      document.addEventListener("DOMContentLoaded", () => {
        setViewMode();                          // restore view mode

        // card / row click-throughs
        document.querySelectorAll(".card").forEach(card => {
          card.addEventListener("click", () => {
            openModal(card.dataset.artist, card.dataset.albumId);
          });
        });
        document.querySelectorAll(".table-row").forEach(row => {
          row.addEventListener("click", () => {
            openModal(row.dataset.artist, row.dataset.albumId);
          });
        });
        document.querySelectorAll(
          "input[type='checkbox'], .btn-dedup, .row-dedup-btn"
        ).forEach(el => el.addEventListener("click", ev => ev.stopPropagation()));

        // resume running tasks if user refreshed mid-operation
        fetch("/api/progress").then(r => r.json()).then(j => {
          if (j.scanning) scanTimer = setInterval(pollScan, 1000);
        });
        fetch("/api/dedupe").then(r => r.json()).then(j => {
          if (j.deduping) dedupeTimer = setInterval(pollDedupe, 1000);
        });

      /* ─── Client-side search filter ──────────────────────────────── */
      const searchInput = document.getElementById("search");
      if (searchInput) {
        searchInput.addEventListener("input", ev => {
          const q = ev.target.value.trim().toLowerCase();

          // Grid cards
          document.querySelectorAll("#gridMode .card").forEach(card => {
            const artist = card.dataset.artist.replace(/_/g," ").toLowerCase();
            const title  = card.dataset.title.toLowerCase();
            card.style.display = (!q || artist.includes(q) || title.includes(q)) ? "flex" : "none";
          });

          // Table rows
          document.querySelectorAll("#tableMode .table-row").forEach(row => {
            const artist = row.dataset.artist.replace(/_/g," ").toLowerCase();
            const title  = row.dataset.title.toLowerCase();
            row.style.display = (!q || artist.includes(q) || title.includes(q)) ? "" : "none";
          });
        });
      }
    });
  // close modal on Escape
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') closeModal();
  });
</script>
  </body>
</html>
"""


# ────────────────────────── UI card helper ──────────────────────────
def _build_card_list(dup_dict) -> list[dict]:
    """
    Convert the nested `state["duplicates"]` dict into the flat list of
    cards expected by both the main page and /api/duplicates.
    """
    cards = []
    for artist, groups in dup_dict.items():
        for g in groups:
            best = g["best"]
            best_fmt = best.get("fmt_text",
                                get_primary_format(Path(best["folder"])))
            formats = [best_fmt] + [
                loser.get("fmt",
                          get_primary_format(Path(loser["folder"])))
                for loser in g["losers"]
            ]
            display_title = best["album_norm"].title()
            cards.append(
                {
                    "artist_key": artist.replace(" ", "_"),
                    "artist": artist,
                    "album_id": best["album_id"],
                    "n": len(g["losers"]) + 1,
                    "best_thumb": thumb_url(best["album_id"]),
                    "best_title": display_title,
                    "best_fmt": best_fmt,
                    "formats": formats,
                    "used_ai": best.get("used_ai", False),
                }
            )
    return cards


@app.get("/")
def index():
    """
    Main page: load duplicates from DB (if not already in memory),
    gather stats, build card list and render the template.
    """
    with lock:
        if not state["duplicates"]:
            logging.debug("index(): loading scan results from DB into memory")
            state["duplicates"] = load_scan_from_db()

    space_saved   = get_stat("space_saved")
    removed_dupes = get_stat("removed_dupes")

    db_conn = plex_connect()
    total_artists = db_conn.execute(
        "SELECT COUNT(*) FROM metadata_items "
        "WHERE metadata_type=8 AND library_section_id=?",
        (SECTION_ID,),
    ).fetchone()[0]
    total_albums = db_conn.execute(
        "SELECT COUNT(*) FROM metadata_items "
        "WHERE metadata_type=9 AND library_section_id=?",
        (SECTION_ID,),
    ).fetchone()[0]
    db_conn.close()

    # Build the card / row structures for the UI
    with lock:
        cards = _build_card_list(state["duplicates"])
        remaining_dupes = len(cards)

    return render_template_string(
        HTML,
        scanning=state["scanning"],
        groups=cards,
        space_saved=space_saved,
        removed_dupes=removed_dupes,
        total_artists=total_artists,
        total_albums=total_albums,
        remaining_dupes=remaining_dupes,
        plex_host=PLEX_HOST,
        plex_token=PLEX_TOKEN,
    )

@app.post("/start")
def start():
    with lock:
        if not state["scanning"]:
            # Pré-flag pour afficher immédiatement la barre de progression
            state.update(scanning=True, scan_progress=0, scan_total=0)
            logging.debug("start(): launching background_scan() thread")
            threading.Thread(target=background_scan, daemon=True).start()
    return "", 204

@app.get("/api/progress")
def api_progress():
    with lock:
        return jsonify(
            scanning=state["scanning"],
            progress=state["scan_progress"],
            total=state["scan_total"]
        )

@app.get("/api/dedupe")
def api_dedupe():
    with lock:
        return jsonify(
            deduping=state["deduping"],
            progress=state["dedupe_progress"],
            total=state["dedupe_total"],
            saved=get_stat("space_saved")
        )

@app.get("/details/<artist>/<int:album_id>")
def details(artist, album_id):
    art = artist.replace("_", " ")
    with lock:
        groups = state["duplicates"].get(art, [])
    for g in groups:
        if g["album_id"] == album_id:
            editions = [g["best"]] + g["losers"]
            out = []
            # Get rationale for best edition, if present
            rationale = g["best"].get("rationale", "")
            for e in editions:
                thumb_data = fetch_cover_as_base64(e["album_id"])
                out.append({
                    "thumb_data": thumb_data,
                    "title_raw": e["title_raw"],
                    "size": folder_size(Path(e["folder"])) // (1024 * 1024),
                    "fmt": e.get("fmt_text", e.get("fmt", "")),
                    "br": (e.get("br", 0) // 1000),
                    "sr": e.get("sr", 0),
                    "bd": e.get("bd", 0)
                })
            return jsonify(
                artist=art,
                album=g["best"]["title_raw"],
                editions=out,
                rationale=rationale,
                merge_list=g["best"].get("merge_list", []),
            )
    return jsonify({}), 404

@app.post("/dedupe/artist/<artist>")
def dedupe_artist(artist):
    art = artist.replace("_", " ")
    data = request.get_json() or {}
    raw = data.get("album_id")
    album_id = int(raw) if raw is not None else None
    moved_list: List[Dict] = []

    with lock:
        groups = state["duplicates"].get(art, [])
        for g in list(groups):
            if g["album_id"] == album_id:
                logging.debug(f"dedupe_artist(): processing artist '{art}', album_id={album_id}")
                moved_list = perform_dedupe(g)
                groups.remove(g)
                if not groups:
                    del state["duplicates"][art]
                con = sqlite3.connect(str(STATE_DB_FILE))
                cur = con.cursor()
                cur.execute("DELETE FROM duplicates_best WHERE artist = ? AND album_id = ?", (art, album_id))
                cur.execute("DELETE FROM duplicates_loser WHERE artist = ? AND album_id = ?", (art, album_id))
                con.commit()
                con.close()
                break

    removed_count = len(moved_list)
    total_mb = sum(item["size"] for item in moved_list)
    increment_stat("removed_dupes", removed_count)
    increment_stat("space_saved", total_mb)
    logging.debug(f"dedupe_artist(): removed {removed_count} dupes, freed {total_mb} MB")

    letter  = quote_plus(art[0].upper())
    art_enc = quote_plus(art)
    try:
        plex_api(f"/library/sections/{SECTION_ID}/refresh?path=/music/matched/{letter}/{art_enc}", method="GET")
        plex_api(f"/library/sections/{SECTION_ID}/emptyTrash", method="PUT")
    except Exception as e:
        logging.warning(f"dedupe_artist(): plex refresh/emptyTrash failed: {e}")

    return jsonify(moved=moved_list), 200

@app.post("/dedupe/all")
def dedupe_all():
    with lock:
        all_groups = [g for lst in state["duplicates"].values() for g in lst]
        state["duplicates"].clear()
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        cur.execute("DELETE FROM duplicates_loser")
        con.commit()
        con.close()
        logging.debug("dedupe_all(): cleared in-memory and DB duplicates tables")
    threading.Thread(target=background_dedupe, args=(all_groups,), daemon=True).start()
    return "", 204

@app.post("/dedupe/selected")
def dedupe_selected():
    data = request.get_json() or {}
    selected = data.get("selected", [])
    moved_list: List[Dict] = []
    total_moved = 0
    removed_count = 0
    artists_to_refresh = set()

    for sel in selected:
        art_key, aid_str = sel.split("||", 1)
        art = art_key.replace("_", " ")
        album_id = int(aid_str)
        with lock:
            groups = state["duplicates"].get(art, [])
            for g in list(groups):
                if g["album_id"] == album_id:
                    logging.debug(f"dedupe_selected(): removing selected group for artist '{art}', album_id={album_id}")
                    moved = perform_dedupe(g)
                    moved_list.extend(moved)
                    total_moved += sum(item["size"] for item in moved)
                    removed_count += len(g["losers"])
                    artists_to_refresh.add(art)
                    groups.remove(g)
                    if not groups:
                        del state["duplicates"][art]
                    con = sqlite3.connect(str(STATE_DB_FILE))
                    cur = con.cursor()
                    cur.execute("DELETE FROM duplicates_best WHERE artist = ? AND album_id = ?", (art, album_id))
                    cur.execute("DELETE FROM duplicates_loser WHERE artist = ? AND album_id = ?", (art, album_id))
                    con.commit()
                    con.close()
                    break

    for art in artists_to_refresh:
        letter  = quote_plus(art[0].upper())
        art_enc = quote_plus(art)
        try:
            plex_api(f"/library/sections/{SECTION_ID}/refresh?path=/music/matched/{letter}/{art_enc}", method="GET")
            plex_api(f"/library/sections/{SECTION_ID}/emptyTrash", method="PUT")
        except Exception as e:
            logging.warning(f"dedupe_selected(): plex refresh/emptyTrash failed for {art}: {e}")

    increment_stat("removed_dupes", removed_count)
    increment_stat("space_saved", total_moved)
    logging.debug(f"dedupe_selected(): removed {removed_count} dupes, freed {total_moved} MB")

    return jsonify(moved=moved_list), 200

# ───────────────────────────────────── CLI MODE ───────────────────────────────────
def dedupe_cli(dry: bool, safe: bool, tag_extra: bool, verbose: bool):
    """
    Command-line mode:
    * Scans every artist and album in Plex.
    * Detects duplicate album groups.
    * Optionally moves loser folders and deletes Plex metadata.

    Parameters
    ----------
    dry : bool
        If True, simulate actions only (no moves / deletes).
    safe : bool
        If True, never delete Plex metadata (even when not dry-run).
    tag_extra : bool
        If True, tag "(Extra Tracks)" on the best edition that has more tracks
        than the shortest edition in the group.
    verbose : bool
        Enable DEBUG-level logging.
    """
    # ─── logging setup ────────────────────────────────────────────────────────
    log_lvl = logging.DEBUG if verbose else logging.INFO
    logging.getLogger().setLevel(log_lvl)

    if verbose:
        logging.debug(f"dedupe_cli(): opening Plex DB at {PLEX_DB_FILE}")

    db_conn = plex_connect()
    cur = db_conn.cursor()

    # ─── headline counters ────────────────────────────────────────────────────
    stats = {
        "total_artists": 0,
        "total_albums": 0,
        "albums_with_dupes": 0,
        "total_dupes": 0,
        "total_moved_mb": 0,
    }

    # ─── iterate over all artists ────────────────────────────────────────────
    artists = cur.execute(
        "SELECT id, title FROM metadata_items "
        "WHERE metadata_type=8 AND library_section_id=?",
        (SECTION_ID,),
    ).fetchall()

    if verbose:
        logging.debug(f"dedupe_cli(): {len(artists)} artists loaded from Plex DB")

    for artist_id, artist_name in artists:
        stats["total_artists"] += 1

        album_ids = [
            row[0]
            for row in cur.execute(
                "SELECT id FROM metadata_items "
                "WHERE metadata_type=9 AND parent_id=?",
                (artist_id,),
            ).fetchall()
        ]
        stats["total_albums"] += len(album_ids)

        dup_groups = scan_duplicates(db_conn, artist_name, album_ids)
        if dup_groups:
            stats["albums_with_dupes"] += len(dup_groups)

        # ---- process each duplicate group -----------------------------------
        for group in dup_groups:
            best = group["best"]
            losers = group["losers"]

            logging.info("-" * 70)
            logging.info(
                f"Duplicate group: {artist_name}  |  {best['title_raw']}"
            )
            sel_method = "AI selection" if best.get("used_ai") else "Heuristic selection"
            logging.info(f"Selection method: {sel_method}")

            best_size = folder_size(best["folder"]) // (1024 * 1024)
            best_fmt = get_primary_format(best["folder"])
            best_br = best["br"] // 1000
            best_sr = best["sr"]
            best_bd = best.get("bd", 0)
            logging.info(
                f" Best  | {best_size} MB | {best_fmt} | "
                f"{best_br} kbps | {best_sr} Hz | {best_bd} bit"
            )

            group_moved_mb = 0

            # ---- each loser --------------------------------------------------
            for loser in losers:
                src = Path(loser["folder"])
                loser_id = loser["album_id"]

                if not src.exists():
                    logging.warning(f"Folder not found, skipping: {src}")
                    continue

                # Build destination path under DUPE_ROOT
                try:
                    base_real = next(iter(PATH_MAP.values()))
                    rel = src.relative_to(base_real)
                except Exception:
                    rel = src.name
                dst = DUPE_ROOT / rel
                dst.parent.mkdir(parents=True, exist_ok=True)

                # Deal with name collisions
                if dst.exists():
                    root_name = dst.name
                    counter = 1
                    while (dst.parent / f"{root_name} ({counter})").exists():
                        counter += 1
                    dst = dst.parent / f"{root_name} ({counter})"

                size_mb = folder_size(src) // (1024 * 1024)
                group_moved_mb += size_mb
                stats["total_moved_mb"] += size_mb
                stats["total_dupes"] += 1

                if dry:
                    logging.info(
                        f" DRY-RUN  | would move {src}  →  {dst}  "
                        f"({size_mb} MB)"
                    )
                else:
                    logging.info(f" Moving   | {src}  →  {dst}")
                    shutil.move(str(src), str(dst))

                # Delete Plex metadata (unless dry/safe)
                if (not dry) and (not safe):
                    logging.debug(f"   deleting Plex metadata rk={loser_id}")
                    plex_api(f"/library/metadata/{loser_id}/trash", method="PUT")
                    time.sleep(0.3)
                    plex_api(f"/library/metadata/{loser_id}", method="DELETE")
                else:
                    logging.debug(f"   Plex delete skipped rk={loser_id}")

            logging.info(f" Group freed {group_moved_mb} MB")

            # ---- optional “Extra Tracks” tag --------------------------------
            if tag_extra:
                all_editions = losers + [best]
                max_tracks = max(len(e["tracks"]) for e in all_editions)
                min_tracks = min(len(e["tracks"]) for e in all_editions)
                if len(best["tracks"]) > min_tracks:
                    logging.info(" Tagging '(Extra Tracks)' on best edition")
                    plex_api(
                        f"/library/metadata/{best['album_id']}"
                        f"?title.value=(Extra Tracks)&title.lock=1",
                        method="PUT",
                    )

        # Refresh Plex for this artist
        prefix = f"/music/matched/{quote_plus(artist_name[0].upper())}/{quote_plus(artist_name)}"
        plex_api(f"/library/sections/{SECTION_ID}/refresh?path={prefix}")
        plex_api(f"/library/sections/{SECTION_ID}/emptyTrash", method="PUT")

    # ---- summary ------------------------------------------------------------
    logging.info("-" * 70)
    logging.info("FINAL SUMMARY")
    for key, val in stats.items():
        logging.info(f"{key.replace('_',' ').title():26}: {val}")
    logging.info("-" * 70)

    db_conn.close()

@app.post("/merge/<artist>/<int:album_id>")
def merge_tracks(artist, album_id):
    data = request.get_json() or {}
    merge_list = data.get("merge_list", [])
    # Locate group in state, get best folder path
    art = artist.replace("_", " ")
    group = next((g for g in state["duplicates"].get(art, []) if g["album_id"] == album_id), None)
    if not group:
        return jsonify(message="Album not found."), 404
    best_folder = Path(group["best"]["folder"])
    # Determine current max track index in best_folder
    max_idx = 0
    for f in best_folder.iterdir():
        m = re.match(r"^(\d{2})\s*-\s*", f.name)
        if m:
            try:
                idx = int(m.group(1))
                if idx > max_idx:
                    max_idx = idx
            except Exception:
                continue
    # For each edition beyond best, copy any tracks matching merge_list into best_folder
    copied = []
    idx_counter = max_idx
    for e in group["losers"]:
        src_folder = Path(e["folder"])
        for track in merge_list:
            src_file = next(src_folder.rglob(f"*{track}*"), None)
            if src_file:
                idx_counter += 1
                orig_name = src_file.name
                ext = src_file.suffix
                base = src_file.stem
                new_name = f"{idx_counter:02d} - {base} (bonus track){ext}"
                shutil.copy2(str(src_file), str(best_folder / new_name))
                copied.append(new_name)
    return jsonify(message=f"Copied {len(copied)} tracks: {', '.join(copied)}")
# Update the /api/duplicates route to use the card helper
@app.get("/api/duplicates")
def api_duplicates():
    """
    Live‑feed of duplicate groups discovered so far, formatted like the
    cards used on the main page, so the JS can drop them straight in.
    """
    with lock:
        return jsonify(_build_card_list(state["duplicates"]))

# ───────────────────────────────── MAIN ───────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scan & dedupe Plex Music duplicates (CLI or WebUI)."
    )
    sub = parser.add_argument_group("Options for WebUI or CLI modes")
    sub.add_argument(
        "--serve", action="store_true", help="Launch Flask web interface"
    )

    cli = parser.add_argument_group("CLI-only options (ignored with --serve)")
    cli.add_argument(
        "--dry-run", action="store_true",
        help="Simulate moves & deletes but do not actually move files or call API."
    )
    cli.add_argument(
        "--safe-mode", action="store_true",
        help="Do not delete Plex metadata even if not dry-run."
    )
    cli.add_argument(
        "--tag-extra", action="store_true",
        help="If an edition has extra tracks, tag 'Extra Tracks' on the best version."
    )
    cli.add_argument(
        "--verbose", action="store_true", help="Enable DEBUG-logging"
    )

    args = parser.parse_args()

    # If --verbose was passed, set root logger to DEBUG (both CLI and Serve modes)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Main: verbose mode enabled; root logger set to DEBUG")

    if args.serve:
        # Mode WebUI
        app.run(host="0.0.0.0", port=WEBUI_PORT)
    else:
        # Mode CLI
        dedupe_cli(
            dry=args.dry_run,
            safe=args.safe_mode,
            tag_extra=args.tag_extra,
            verbose=args.verbose
        )

