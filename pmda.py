#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
v0.6.0
- Added auto detection of plex paths, and automatic configuration of previously existing PATH MAP variable. 
- BE SURE TO USE THE EXACT PLEX PATH STRUCTURE WHEN MAPPING YOUR PATHS USING THE BIND MOUNT! 
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import shutil
import filecmp
import errno
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
import unittest
import sys

from flask import Flask, render_template_string, request, jsonify

# ──────────────────────────── FFmpeg sanity-check ──────────────────────────────
def _check_ffmpeg():
    """
    Log the location of ffmpeg/ffprobe or warn clearly if they are missing.
    Runs once at startup.
    """
    missing = []
    for tool in ("ffmpeg", "ffprobe"):
        path = shutil.which(tool)
        if path:
            logging.info("%s detected at %s", tool, path)
        else:
            missing.append(tool)
    if missing:
        logging.warning(
            "⚠️  %s not found in PATH – bit‑rate, sample‑rate and bit‑depth will be 0",
            ", ".join(missing),
        )

_check_ffmpeg()

# --- Scan control flags (global) ---------------------------------
scan_should_stop = threading.Event()
scan_is_paused   = threading.Event()

#
# ───────────────────────────────── CONFIGURATION LOADING ─────────────────────────────────
"""
Robust configuration helper:

* Loads defaults from the baked‑in config.json shipped inside the Docker image.
* Copies that file (and ai_prompt.txt) into the user‑writable config dir on first run.
* Overrides every value with an environment variable when present.
* Falls back to sensible, documented defaults when neither file nor env provides a value.
* Validates critical keys so we fail early instead of crashing later.
* Logs where each value came from (env vs config vs default).
"""

import filecmp

# Helper parsers --------------------------------------------------------------
def _parse_bool(val: str | bool) -> bool:
    """Return *True* for typical truthy strings / bools."""
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in {"1", "true", "yes", "on"}

def _parse_int(val, default: int | None = None) -> int | None:
    """Return *int* or *default* on failure / None."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _parse_path_map(val) -> dict[str, str]:
    """
    Accept either a dict, a JSON string or a CSV string of ``SRC:DEST`` pairs.
    Every key/val is coerced to *str*.
    """
    if isinstance(val, dict):
        return {str(k): str(v) for k, v in val.items()}
    if not val:
        return {}
    s = str(val).strip()
    if s.startswith("{"):
        try:
            data = json.loads(s)
            return {str(k): str(v) for k, v in data.items()}
        except json.JSONDecodeError as e:
            logging.warning("Failed to decode PATH_MAP JSON from env – %s", e)
            return {}
    mapping: dict[str, str] = {}
    for pair in s.split(","):
        if ":" in pair:
            src, dst = pair.split(":", 1)
            mapping[src.strip()] = dst.strip()
    return mapping

# ──────────────────────── Auto‑detect PATH_MAP from mounts ────────────────────────
def _auto_detect_path_map() -> dict[str, str]:
    """
    Derive a default PATH_MAP by parsing /proc/self/mountinfo.

    For every bind‑mount whose *container* mount‑point starts with
    “/music”, we map that mount‑point to the *host* source path.

    mountinfo format (kernel ≥ 3.8):
        … mount_point … - fstype src_path super_opts

    We must therefore split each line at " - " first, then take the first
    entry *after* the hyphen (the bind source).  Using parts[3] (root)
    was incorrect and trimmed leading segments such as “/mnt/user”.
    """
    mapping: dict[str, str] = {}
    try:
        with open("/proc/self/mountinfo", "r", encoding="utf-8") as fh:
            for line in fh:
                # separate pre/post‑hyphen fields
                pre, _, post = line.partition(" - ")
                if not post:
                    continue
                post_parts = post.split()
                if len(post_parts) < 2:
                    continue
                # Prefer the *root* field (pre‑hyphen, column 4) because it
                # keeps the full original host path even on Unraid, where the
                # mount‑source shown after the hyphen may lose the leading
                # “/mnt/user”.  Fallback to the post‑hyphen source when the
                # root field is “/”.
                pre_parts = pre.split()
                if len(pre_parts) < 5:
                    continue
                host_src = pre_parts[3] if pre_parts[3] != "/" else post_parts[1]
                mount_point = pre_parts[4]         # path inside container
                if mount_point.startswith("/music"):
                    mapping[mount_point] = host_src
    except Exception as e:
        logging.debug("Auto PATH_MAP detection failed: %s", e)
    return mapping

# Determine runtime config dir -------------------------------------------------
BASE_DIR   = Path(__file__).parent
CONFIG_DIR = Path(os.getenv("PMDA_CONFIG_DIR", BASE_DIR))
CONFIG_DIR.mkdir(parents=True, exist_ok=True)

# Location of baked‑in template files (shipped inside the image)
DEFAULT_CONFIG_PATH  = BASE_DIR / "config.json"
DEFAULT_PROMPT_PATH  = BASE_DIR / "ai_prompt.txt"

CONFIG_PATH   = CONFIG_DIR / "config.json"
AI_PROMPT_FILE = CONFIG_DIR / "ai_prompt.txt"

# (1) Ensure config.json exists -----------------------------------------------
if not CONFIG_PATH.exists():
    logging.info("No config.json found — using default template from image")
    shutil.copyfile(DEFAULT_CONFIG_PATH, CONFIG_PATH)

# (2) Ensure ai_prompt.txt exists -------------------------------------------
if not AI_PROMPT_FILE.exists():
    logging.info("ai_prompt.txt not found — default prompt created")
    shutil.copyfile(DEFAULT_PROMPT_PATH, AI_PROMPT_FILE)

# (3) Load JSON config ---------------------------------------------------------
with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
    conf: dict = json.load(fh)

# (4) Merge with environment variables ----------------------------------------
ENV_SOURCES: dict[str, str] = {}

def _get(key: str, *, default=None, cast=lambda x: x):
    """Return the merged value and remember where it came from."""
    if (env_val := os.getenv(key)) is not None:
        ENV_SOURCES[key] = "env"
        raw = env_val
    elif key in conf:
        ENV_SOURCES[key] = "config"
        raw = conf[key]
    else:
        ENV_SOURCES[key] = "default"
        raw = default
    return cast(raw)


# Derive effective PATH_MAP (env/config first, fallback to auto‑detect)
_raw_path_map = _get("PATH_MAP", default="")
_path_map     = _parse_path_map(_raw_path_map)
if not _path_map:
    _path_map = _auto_detect_path_map()
    if _path_map:
        logging.info("Auto‑detected PATH_MAP from container mounts: %s",
                     json.dumps(_path_map))

# --- Ignore ONLY the baked‑in template placeholder --------------------------
# If every destination begins with the canonical "/path/to/" stub shipped in the
# sample config, we treat it as an unedited template and fall back to the
# auto‑detected mapping.  We deliberately *do not* include “/MURRAY/” (or any
# other real username) so that legitimate paths on the host are never rejected.
_placeholder_prefix = "/path/to/"
if _path_map and all(dst.startswith(_placeholder_prefix) for dst in _path_map.values()):
    logging.info("Ignoring placeholder PATH_MAP from default template; falling back to auto‑detect")
    _path_map = _auto_detect_path_map()

merged = {
    "PLEX_DB_PATH":   _get("PLEX_DB_PATH",   default="",                                cast=str),
    "PLEX_HOST":      _get("PLEX_HOST",      default="",                                cast=str),
    "PLEX_TOKEN":     _get("PLEX_TOKEN",     default="",                                cast=str),
    "SECTION_ID":     _get("SECTION_ID",     default=1,                                 cast=_parse_int),
    "SCAN_THREADS":   _get("SCAN_THREADS",   default=os.cpu_count() or 4,               cast=_parse_int),
    "PATH_MAP":       _path_map,
    "LOG_LEVEL":      _get("LOG_LEVEL",      default="INFO").upper(),
    "OPENAI_API_KEY": _get("OPENAI_API_KEY", default="",                                cast=str),
    "OPENAI_MODEL":   _get("OPENAI_MODEL",   default="gpt-4",                           cast=str),
}

# ─────────────────────────────── Fixed container constants ───────────────────────────────
# DB filename is always fixed under the Plex DB folder
PLEX_DB_FILE = str(Path(merged["PLEX_DB_PATH"]) / "com.plexapp.plugins.library.db")
# Duplicates always move to /dupes inside the container
DUPE_ROOT = Path("/dupes")
# WebUI always listens on container port 5005 inside the container
WEBUI_PORT = 5005

# (5) Validate critical values -------------------------------------------------
if not merged["PLEX_DB_PATH"]:
    raise SystemExit("Missing required config value: PLEX_DB_PATH")
for key in ("PLEX_HOST", "PLEX_TOKEN", "SECTION_ID"):
    if not merged[key]:
        raise SystemExit(f"Missing required config value: {key}")

# (7) Export as module‑level constants ----------------------------------------
PLEX_HOST      = merged["PLEX_HOST"]
PLEX_TOKEN     = merged["PLEX_TOKEN"]
SECTION_ID     = int(merged["SECTION_ID"])
PATH_MAP       = merged["PATH_MAP"]
SCAN_THREADS   = int(merged["SCAN_THREADS"])
LOG_LEVEL      = merged["LOG_LEVEL"]
OPENAI_API_KEY = merged["OPENAI_API_KEY"]
OPENAI_MODEL   = merged["OPENAI_MODEL"]

#
# State and cache DB always live in the config directory
STATE_DB_FILE = CONFIG_DIR / "state.db"
CACHE_DB_FILE = CONFIG_DIR / "cache.db"

# File-format preference order (can be overridden in config.json)
FORMAT_PREFERENCE = conf.get(
    "FORMAT_PREFERENCE",
    ["dsf","aif","aiff","wav","flac","m4a","mp4","m4b","m4p","aifc","ogg","mp3","wma"]
)

# (8) Logging setup (must happen BEFORE any log statements elsewhere) ---------
_level_num = getattr(logging, LOG_LEVEL, logging.INFO)
logging.basicConfig(
    level=_level_num,
    format="%(asctime)s │ %(levelname)s │ %(message)s",
    datefmt="%H:%M:%S",
    force=True,                         # ensure we (re‑)configure root logger
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Mask & dump effective config ------------------------------------------------
for k, src in ENV_SOURCES.items():
    val = merged.get(k)
    if k in {"PLEX_TOKEN", "OPENAI_API_KEY"} and val:
        val = val[:4] + "…"  # keep first 4 chars, mask the rest
    logging.info("Config %-15s = %-30s (source: %s)", k, val, src)

if _level_num == logging.DEBUG:
    scrubbed = {k: ("***" if k in {"PLEX_TOKEN", "OPENAI_API_KEY"} else v)
                for k, v in merged.items()}
    logging.debug("Full merged config:\n%s", json.dumps(scrubbed, indent=2))

# (9) Initialise OpenAI if key present ----------------------------------------
if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY
else:
    logging.info("No OPENAI_API_KEY provided; AI-driven selection disabled.")

# (10) Validate Plex connection ------------------------------------------------
# (10) Validate Plex connection ------------------------------------------------
def _validate_plex_connection():
    """
    Perform a lightweight request to Plex `/library/sections` to make sure the
    server is reachable and the token is valid.  We only warn on failure so
    the application can still run in offline mode.
    """
    url = f"{PLEX_HOST}/library/sections"
    try:
        resp = requests.get(url, headers={"X-Plex-Token": PLEX_TOKEN}, timeout=10)
        if resp.status_code != 200:
            logging.warning(
                "⚠️  Plex connection failed (HTTP %s) – check PLEX_HOST and PLEX_TOKEN",
                resp.status_code,
            )
        else:
            logging.info("Plex connection OK (HTTP %s)", resp.status_code)
    except Exception as e:
        logging.warning("⚠️  Plex connection failed – %s", e)

# ─────────────────────────────── SELF‑DIAGNOSTIC ────────────────────────────────
def _self_diag() -> bool:
    """
    Runs a quick start‑up check and prints a colour‑coded report:
    1) Plex DB reachability
    2) Coverage of every PATH_MAP entry
    3) R/W permissions on mapped music folders and /dupes
    4) Rough count of albums with no PATH_MAP match

    Returns *True* when every mandatory check passes, otherwise *False*.
    """
    logging.info("──────── PMDA self‑diagnostic ────────")

    # 1) Plex DB readable?
    try:
        db = plex_connect()
        db.execute("SELECT 1").fetchone()
        logging.info("✓ Plex DB reachable (%s)", PLEX_DB_FILE)
    except Exception as e:
        logging.error("✗ Plex DB ERROR – %s", e)
        return False

    # Compute exact album counts for each PATH_MAP prefix (no sampling)
    prefix_stats: dict[str, int] = {}
    for pre in PATH_MAP:
        cnt = db.execute(
            """
            SELECT COUNT(*)
            FROM media_parts mp
            JOIN metadata_items md ON md.id = mp.media_item_id
            WHERE md.metadata_type = 9
              AND mp.file LIKE ?
            """,
            (f"{pre}/%",)
        ).fetchone()[0]
        prefix_stats[pre] = cnt

    for pre, dest in PATH_MAP.items():
        albums_seen = prefix_stats.get(pre, 0)
        if albums_seen == 0:
            logging.warning("⚠ %s → %s  (prefix not found in DB)", pre, dest)
        elif not Path(dest).exists():
            logging.error("✗ %s → %s  (host path missing)", pre, dest)
            return False
        else:
            logging.info("✓ %s → %s  (%d albums)", pre, dest, albums_seen)

    # 3) Permission checks
    for mount in [*PATH_MAP.values(), str(DUPE_ROOT), str(CONFIG_DIR)]:
        p = Path(mount)
        if not p.exists():
            continue
        rw = ("r" if os.access(p, os.R_OK) else "-") + \
             ("w" if os.access(p, os.W_OK) else "-")
        if rw != "rw":
            logging.warning("⚠ %s permissions: %s", p, rw)
        else:
            logging.info("✓ %s permissions: %s", p, rw)

    # 4) Albums with no mapping
    unmapped = db.execute(
        "SELECT COUNT(*) FROM media_parts WHERE " +
        " AND ".join([f"file NOT LIKE '{pre}%'" for pre in PATH_MAP])
    ).fetchone()[0]
    if unmapped:
        logging.warning("⚠ %d albums have no PATH_MAP match", unmapped)

    logging.info("──────── diagnostic complete ─────────")
    return True


# ───────────────────────────────── OTHER CONSTANTS ──────────────────────────────────
AUDIO_RE    = re.compile(r"\.(flac|ape|alac|wav|m4a|aac|mp3|ogg)$", re.I)
# Derive format scores from user preference order
FMT_SCORE   = {ext: len(FORMAT_PREFERENCE)-i for i, ext in enumerate(FORMAT_PREFERENCE)}
OVERLAP_MIN = 0.85  # 85% track-title overlap minimum

# ───────────────────────────────── STATE DB SETUP ──────────────────────────────────
def init_state_db():
    con = sqlite3.connect(str(STATE_DB_FILE))
    # Enable WAL mode up‑front to allow concurrent reads/writes
    con.execute("PRAGMA journal_mode=WAL;")
    con.commit()
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
    """Atomically add *delta* to a stat counter."""
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    con.execute("PRAGMA busy_timeout=30000;")
    cur = con.cursor()
    cur.execute("UPDATE stats SET value = value + ? WHERE key = ?", (delta, key))
    con.commit()
    con.close()

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



# ──────────────────────────────── PLEX DB helper ────────────────────────────────
def plex_connect() -> sqlite3.Connection:
    """
    Open the Plex SQLite DB using UTF-8 *surrogate-escape* decoding so that any
    non-UTF-8 bytes are mapped to the U+DCxx range instead of throwing an error.
    """
    # Open the Plex database in read-only mode to avoid write errors
    con = sqlite3.connect(f"file:{PLEX_DB_FILE}?mode=ro", uri=True, timeout=30)
    con.text_factory = lambda b: b.decode("utf-8", "surrogateescape")
    return con

# ─── Run connection check & self‑diagnostic only after helpers are defined ───
_validate_plex_connection()
if not _self_diag():
    raise SystemExit("Self‑diagnostic failed – please fix the issues above and restart PMDA.")

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
    Inspect *one* representative audio file inside **folder** and return:

        (fmt_score, bit_rate, sample_rate, bit_depth)

    * **fmt_score** is derived from FORMAT_PREFERENCE.
    * **bit_rate** is in **bps** (0 when unavailable, e.g. lossless FLAC).
    * **sample_rate** is in **Hz**.
    * **bit_depth** is 16 / 24 / 32 when derivable, otherwise 0.

    We probe only the *first* audio file we encounter.  This re‑verts the
    “scan‑every‑track” logic that was flooding the machine with hundreds of
    `ffprobe` processes and causing time‑outs – the root cause of the all‑zero
    technical data you observed in the UI.

    Results are cached in the ``audio_cache`` table keyed on (path, mtime) so
    that we call ``ffprobe`` at most once per file unless it has changed.
    A cached triple ``(0, 0, 0)`` for a **FLAC** file triggers a re‑probe
    because it usually indicates a transient failure.
    """
    # locate *one* audio file
    audio_file = next((p for p in folder.rglob("*") if AUDIO_RE.search(p.name)), None)
    if audio_file is None:
        return (0, 0, 0, 0)

    ext   = audio_file.suffix[1:].lower()
    fpath = str(audio_file)
    mtime = int(audio_file.stat().st_mtime)

    # ─── 1) cache lookup ───────────────────────────────────────────────
    cached = get_cached_info(fpath, mtime)
    if cached and not (ext == "flac" and cached == (0, 0, 0)):
        br, sr, bd = cached
        return (score_format(ext), br, sr, bd)

    # ─── 2) ffprobe ────────────────────────────────────────────────────
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "a:0",
        "-show_entries", "format=bit_rate:stream=bit_rate,sample_rate,bits_per_raw_sample,bits_per_sample,sample_fmt",
        "-of", "default=noprint_wrappers=1",
        fpath,
    ]

    br = sr = bd = 0
    try:
        out = subprocess.check_output(
            cmd, stderr=subprocess.DEVNULL, text=True, timeout=10
        )
        for line in out.splitlines():
            key, _, val = line.partition("=")
            if key == "bit_rate":
                try:
                    v = int(val)
                    if v > br:
                        br = v           # keep the highest bit‑rate seen
                except ValueError:
                    pass
            elif key == "sample_rate":
                try:
                    sr = int(val)
                except ValueError:
                    pass
            elif key in ("bits_per_raw_sample", "bits_per_sample"):
                try:
                    bd = int(val)
                except ValueError:
                    pass
            elif key == "sample_fmt" and not bd:
                m = re.match(r"s(\d+)", val)
                if m:
                    bd = int(m.group(1))
    except Exception:
        # leave br/sr/bd at 0 on failure
        pass

    # ─── 3) cache & return ────────────────────────────────────────────
    set_cached_info(fpath, mtime, br, sr, bd)
    return (score_format(ext), br, sr, bd)

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
    Selects the best edition either via OpenAI (when an API key is provided) or via a local heuristic, re‑using any existing AI cache first.
    """
    import sqlite3, json

    # 1) Re‑use a previously stored AI choice
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

    # 2) If there is no AI cache, call OpenAI when possible
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
            logging.debug(f"AI raw response: {txt}")
            parts = [part.strip() for part in txt.split("|")]
            if len(parts) != 3:
                raise ValueError(f"Invalid AI response format, expected 3 parts but got {len(parts)}")
            idx = int(re.search(r"\d+", parts[0]).group())
            rationale = parts[1]
            merge_list = [t.strip() for t in parts[2].split(",") if t.strip()]

            best = editions[idx]
            best.update({
                "rationale":  rationale,
                "merge_list": merge_list,
                "used_ai":    True,
            })
        except Exception as e:
            logging.warning(f"AI failed ({e}); falling back to heuristic selection")
            used_ai = False

    # 3) Heuristic selection (or fallback when AI is disabled / failed)
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

        # brief-rationale
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

    # 4) Persist choice to DB (INSERT OR IGNORE so we never overwrite an existing cache)
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
    # Persist loser editions so details modal can show all versions after restart
    for e in editions:
        if e['album_id'] != best['album_id']:
            size_mb = folder_size(Path(e['folder'])) // (1024 * 1024)
            cur.execute("""
                INSERT INTO duplicates_loser
                  (artist, album_id, folder, fmt_text, br, sr, bd, size_mb)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                best["artist"],
                best["album_id"],
                str(e["folder"]),
                get_primary_format(Path(e["folder"])),
                e.get("br", 0),
                e.get("sr", 0),
                e.get("bd", 0),
                size_mb
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
    if scan_should_stop.is_set():
        return (artist_name, [], 0)
    while scan_is_paused.is_set() and not scan_should_stop.is_set():
        time.sleep(0.5)
    logging.info(f"Processing artist: {artist_name}")
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
        if scan_should_stop.is_set():
            break
        while scan_is_paused.is_set() and not scan_should_stop.is_set():
            time.sleep(0.5)
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

    # 1) Clear both duplicates tables
    cur.execute("DELETE FROM duplicates_loser")
    cur.execute("DELETE FROM duplicates_best")

    # 2) Re-insert all scan results
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

            # All “loser” editions
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
    When a user triggers “Start New Scan,” wipe prior duplicates from memory.
    The DB will be cleared and repopulated only once the scan completes.
    """
    with lock:
        state["duplicates"].clear()

# ───────────────────────────── BACKGROUND TASKS (WEB) ─────────────────────────────
def background_scan():
    """
    Scan the entire library in parallel, persist results to SQLite,
    and update the in‑memory `state` for the Web UI.

    The function is now exception‑safe: no single worker failure will abort
    the whole scan, and `state["scanning"]` is **always** cleared even when
    an unexpected error occurs, so the front‑end never hangs in “running”.
    """
    logging.debug(f"background_scan(): opening Plex DB at {PLEX_DB_FILE}")

    try:
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

        logging.debug(
            f"background_scan(): {len(artists)} artists, {total_albums} albums total"
        )

        # Reset live state
        with lock:
            state.update(scanning=True, scan_progress=0, scan_total=total_albums)
            state["duplicates"].clear()

        clear_db_on_new_scan()  # wipe previous duplicate tables

        all_results, futures = {}, []
        import concurrent.futures
        future_to_albums: dict[concurrent.futures.Future, int] = {}
        with ThreadPoolExecutor(max_workers=SCAN_THREADS) as executor:
            for artist_id, artist_name in artists:
                album_cnt = db_conn.execute(
                    "SELECT COUNT(*) FROM metadata_items WHERE metadata_type=9 AND parent_id=?",
                    (artist_id,)
                ).fetchone()[0]
                fut = executor.submit(scan_artist_duplicates, (artist_id, artist_name))
                futures.append(fut)
                future_to_albums[fut] = album_cnt
            # close the shared connection; workers use their own
            db_conn.close()

            for future in as_completed(futures):
                # Allow stop/pause mid‑scan
                if scan_should_stop.is_set():
                    break
                album_cnt = future_to_albums.get(future, 0)
                try:
                    artist_name, groups, _ = future.result()
                except Exception as e:
                    logging.exception(f"background_scan(): worker crashed – {e}")
                    groups = []
                finally:
                    with lock:
                        state["scan_progress"] += album_cnt
                        if groups:
                            all_results[artist_name] = groups
                            state["duplicates"][artist_name] = groups

        # Persist what we’ve found so far (even if some artists failed)
        save_scan_to_db(all_results)

    finally:
        # Make absolutely sure we leave the UI in a consistent state
        with lock:
            state["scanning"] = False
        logging.debug("background_scan(): finished (flag cleared)")

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
    and return a list of dicts describing each moved item.
    """
    moved_items: List[dict] = []
    artist = group["artist"]
    best_title = group["best"]["title_raw"]
    cover_data = fetch_cover_as_base64(group["best"]["album_id"])

    for loser in group["losers"]:
        src_folder = Path(loser["folder"])
        # Skip if the source folder is absent (e.g. already moved or path mapping issue)
        if not src_folder.exists():
            logging.warning(f"perform_dedupe(): source folder missing – {src_folder}; skipping.")
            continue
        try:
            base_real = next(iter(PATH_MAP.values()))
            rel = src_folder.relative_to(base_real)
        except Exception:
            rel = src_folder.name

        dst = DUPE_ROOT / rel
        dst.parent.mkdir(parents=True, exist_ok=True)

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
        except OSError as e:
            if e.errno == errno.EXDEV:
                logging.info(f"Cross-device move for {src_folder}; falling back to copy")
                shutil.copytree(str(src_folder), str(dst))
                shutil.rmtree(str(src_folder))
            else:
                raise

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
            "artist":    artist,
            "title_raw": best_title,
            "size":      size_mb,
            "fmt":       fmt_text,
            "br":        br_kbps,
            "sr":        sr,
            "bd":        bd,
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
    <link rel="icon" type="image/png" href="/static/P.png">
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
    <style>
      .pagination {
        display: flex;
        justify-content: center;
        margin: 1rem 0;
        list-style: none;
        padding: 0;
      }
      .pagination li {
        margin: 0 0.25rem;
      }
      .pagination a, .pagination span {
        display: block;
        padding: 0.5rem 0.75rem;
        border: 1px solid #ccc;
        border-radius: 4px;
        text-decoration: none;
        color: #333;
      }
      .pagination a:hover {
        background-color: #f0f0f0;
      }
      .pagination .active {
        background-color: #006f5f;
        color: #fff;
        border-color: #006f5f;
      }
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
    <!-- the text element is always present but hidden by default -->
    <div id="scanTxt" style="display:block; margin-top:0.5rem;">0 / 0 albums</div>
    <div id="scanControls" style="margin-bottom:1rem; display:flex; align-items:center; gap:0.5rem;">
      {% if scanning %}
        <button onclick="fetch('/scan/pause', {method:'POST'})">⏸ Pause</button>
      {% else %}
        {% if remaining_dupes > 0 %}
          <button onclick="scanLibrary()">▶️ Resume</button>
        {% else %}
          <button onclick="scanLibrary()">New Scan</button>
        {% endif %}
      {% endif %}
      <span id="scanStatus">
        Status: 
        {% if scanning and not paused %}running{% elif scanning and paused %}paused{% else %}stopped{% endif %}
      </span>
    </div>

    <!-- ==== Grid Mode ==== -->
    {% if total_pages > 1 %}
    <nav>
      <ul class="pagination">
        {% if page > 1 %}
          <li><a href="/?page=1">First</a></li>
          <li><a href="/?page={{ page-1 }}">Previous</a></li>
        {% endif %}
        {# Calculate window boundaries #}
        {% set start_page = page - 7 if page - 7 > 1 else 1 %}
        {% set end_page = page + 7 if page + 7 < total_pages else total_pages %}
        {% if start_page > 1 %}
          <li><span>…</span></li>
        {% endif %}
        {% for p in range(start_page, end_page + 1) %}
          <li>
            {% if p == page %}
              <span class="active">{{ p }}</span>
            {% else %}
              <a href="/?page={{ p }}">{{ p }}</a>
            {% endif %}
          </li>
        {% endfor %}
        {% if end_page < total_pages %}
          <li><span>…</span></li>
        {% endif %}
        {% if page < total_pages %}
          <li><a href="/?page={{ page+1 }}">Next</a></li>
          <li><a href="/?page={{ total_pages }}">Last</a></li>
        {% endif %}
      </ul>
    </nav>
    {% endif %}
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

            // Update status text
            const statusEl = document.getElementById("scanStatus");
            if (statusEl) {
            statusEl.innerText = "Status: " + j.status;
            }

            // Update control buttons when paused
            const controls = document.getElementById("scanControls");
            if (controls && j.status === "paused") {
            controls.innerHTML = `
                <button onclick="fetch('/scan/resume', {method:'POST'}).then(()=>window.location.reload())">
                ▶️ Resume
                </button>
                <span id="scanStatus">Status: paused</span>
            `;
            }

            // live-refresh duplicate cards as they arrive
            fetch("/api/duplicates")
            .then(r => r.json())
            .then(dups => {
                if (dups.length !== currentDupTotal) {
                currentDupTotal = dups.length;
                renderDuplicates(dups);

                // Update remaining dupes badge
                const remBadge = document.getElementById("remainingDupes");
                if (remBadge) {
                    remBadge.innerText = `Remaining Dupes: ${dups.length}`;
                }

                // Auto-advance page when exceeding 100 dupes per page
                const params = new URLSearchParams(window.location.search);
                const page = parseInt(params.get("page") || "1", 10);
                const PER_PAGE = 100;
                if (dups.length > page * PER_PAGE) {
                    params.set("page", page + 1);
                    window.location.search = params.toString();
                }
                }
            });

        } else {
            clearInterval(scanTimer);
        }
        })
        .catch(err => console.error("pollScan() failed:", err));
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
      function scanLibrary() {
        fetch('/scan/start', { method: 'POST' })
          .then(() => {
            // reload page to update UI buttons and start polling
            window.location.reload();
          })
          .catch(err => console.error("Scan start failed:", err));
      }

      // keep backward compatibility for the old call
      function startScan() { scanLibrary(); }

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

        // resume running tasks and init progress bar if a scan is already in flight
        fetch("/api/progress")
        .then(r => r.json())
        .then(j => {
            // Always show the progress bar/text if any scan ever started
            const scanBox = document.getElementById("scanBox");
            const scanBar = document.getElementById("scanBar");
            const scanTxt = document.getElementById("scanTxt");
            const statusEl = document.getElementById("scanStatus");

            // Display progress UI
            if (scanBox) scanBox.style.display = "block";
            if (scanBar) {
            const pct = j.total ? Math.round(100 * j.progress / j.total) : 0;
            scanBar.style.width = pct + "%";
            }
            if (scanTxt) scanTxt.innerText = `${j.progress} / ${j.total} albums`;

            // Update status text immediately
            if (statusEl) statusEl.innerText = "Status: " + j.status;

            // If scanning is true (running or paused), start the polling loop
            if (j.scanning) {
            scanTimer = setInterval(pollScan, 1000);
            }
        })
        .catch(err => console.error("Failed to init scan status:", err));
        fetch("/api/dedupe").then(r => r.json()).then(j => {
          if (j.deduping) dedupeTimer = setInterval(pollDedupe, 1000);
        });

      /* ─── Client-side search filter ──────────────────────────────── */
    const searchInput = document.getElementById("search");
    if (searchInput) {
    searchInput.addEventListener("input", ev => {
        const q = ev.target.value.trim().toLowerCase();

        if (!q) {
        // reset filter and restore full list with pagination
        fetch("/api/duplicates")
          .then(r => r.json())
          .then(allGroups => {
            renderDuplicates(allGroups);
            document.querySelector("nav .pagination").style.display = "flex";
          });
        return;
        }

        fetch("/api/duplicates")
        .then(r => r.json())
        .then(allGroups => {
            const filtered = allGroups.filter(g =>
            g.artist.toLowerCase().includes(q) ||
            g.best_title.toLowerCase().includes(q)
            );
            renderDuplicates(filtered);
            // hide pagination during filtered search
            document.querySelector("nav .pagination").style.display = "none";
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
        paused = scan_is_paused.is_set()

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
        page = int(request.args.get("page", 1))
        PER_PAGE = 100
        cards_all = _build_card_list(state["duplicates"])
        # Total number of duplicate groups
        total_dup_groups = len(cards_all)
        total_pages = (total_dup_groups + PER_PAGE - 1) // PER_PAGE
        start = (page - 1) * PER_PAGE
        end = start + PER_PAGE
        cards = cards_all[start:end]
        # The "Remaining Dupes" badge should show the total count, not just the current page
        remaining_dupes = total_dup_groups

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
        page=page,
        total_pages=total_pages,
        paused=paused,
    )


# --- New scan control endpoints ---
from flask import Response

def start_background_scan():
    with lock:
        if not state["scanning"]:
            state.update(scanning=True, scan_progress=0, scan_total=0)
            logging.debug("start_scan(): launching background_scan() thread")
            threading.Thread(target=background_scan, daemon=True).start()

@app.route("/scan/start", methods=["POST"])
def start_scan():
    scan_should_stop.clear()
    scan_is_paused.clear()
    start_background_scan()
    return "Scan started"

@app.route("/scan/pause", methods=["POST"])
def pause_scan():
    scan_is_paused.set()
    with lock:
        state["scanning"] = True   # still scanning, just paused
    return "", 204


@app.route("/scan/resume", methods=["POST"])
def resume_scan():
    scan_is_paused.clear()
    # no state change needed; polling loop will continue
    return "", 204


@app.route("/scan/stop", methods=["POST"])
def stop_scan():
    scan_should_stop.set()
    with lock:
        state["scanning"] = False
    return "", 204

@app.get("/api/progress")
def api_progress():
    with lock:
        if state["scan_total"] and state["scan_progress"] >= state["scan_total"]:
            state["scanning"] = False
        scanning = state["scanning"]
        status = "paused" if (scanning and scan_is_paused.is_set()) else ("running" if scanning else "stopped")
        progress = state["scan_progress"]
        total = state["scan_total"]
    return jsonify(
        scanning=scanning,
        progress=progress,
        total=total,
        status=status,
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
    # Prefer in-memory duplicates (with full loser lists) during runtime, fallback to DB
    with lock:
        groups = state["duplicates"].get(art)
    if groups is None:
        groups = load_scan_from_db().get(art, [])
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
    # ─── logging setup ────────────────────────────────────────────────────
    log_lvl = logging.DEBUG if verbose else logging.INFO
    logging.getLogger().setLevel(log_lvl)

    if verbose:
        logging.debug(f"dedupe_cli(): opening Plex DB at {PLEX_DB_FILE}")

    db_conn = plex_connect()
    cur = db_conn.cursor()

    # ─── headline counters ──────────────────────────────────────────────
    stats = {
        "total_artists": 0,
        "total_albums": 0,
        "albums_with_dupes": 0,
        "total_dupes": 0,
        "total_moved_mb": 0,
    }

    # ─── iterate over all artists ───────────────────────────────────────
    artists = cur.execute(
        "SELECT id, title FROM metadata_items "
        "WHERE metadata_type=8 AND library_section_id=?",
        (SECTION_ID,),
    ).fetchall()

    if verbose:
        logging.debug(f"dedupe_cli(): {len(artists)} artists loaded from Plex DB")

    for artist_id, artist_name in artists:
        logging.info("Processing artist: %s", artist_name)
        logging.info(f"Processing artist: {artist_name}")
        # Log each artist being scanned
        logging.info(f"Scanning artist: {artist_name}")
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

        # Track removals per artist
        removed_for_current_artist = 0

        # ---- process each duplicate group -----------------------------
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

            # ---- each loser --------------------------------------------
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
                    parent_dir = dst.parent
                    counter = 1
                    while True:
                        candidate = parent_dir / f"{root_name} ({counter})"
                        if not candidate.exists():
                            dst = candidate
                            break
                        counter += 1

                size_mb = folder_size(src) // (1024 * 1024)
                group_moved_mb += size_mb
                stats["total_moved_mb"] += size_mb
                stats["total_dupes"] += 1
                removed_for_current_artist += 1

                if dry:
                    logging.info(
                        f" DRY-RUN  | would move {src}  →  {dst}  "
                        f"({size_mb} MB)"
                    )
                else:
                    logging.info(f" Moving   | {src}  →  {dst}")
                    try:
                        shutil.move(str(src), str(dst))
                    except OSError as e:
                        if e.errno == errno.EXDEV:
                            logging.info(f"Cross-device move detected for {src} → {dst}, falling back to copy")
                            shutil.copytree(str(src), str(dst))
                            shutil.rmtree(str(src))
                        else:
                            raise

                # Delete Plex metadata (unless dry/safe)
                if (not dry) and (not safe):
                    logging.debug(f"   deleting Plex metadata rk={loser_id}")
                    plex_api(f"/library/metadata/{loser_id}/trash", method="PUT")
                    time.sleep(0.3)
                    plex_api(f"/library/metadata/{loser_id}", method="DELETE")
                else:
                    logging.debug(f"   Plex delete skipped rk={loser_id}")

            logging.info(f" Group freed {group_moved_mb} MB")

            # ---- optional “Extra Tracks” tag ----------------------------
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

        # Refresh Plex only if we removed duplicates for this artist
        if removed_for_current_artist > 0:
            prefix = f"/music/matched/{quote_plus(artist_name[0].upper())}/{quote_plus(artist_name)}"
            try:
                plex_api(f"/library/sections/{SECTION_ID}/refresh?path={prefix}")
                plex_api(f"/library/sections/{SECTION_ID}/emptyTrash", method="PUT")
            except Exception as e:
                logging.warning(f"CLI-mode Plex refresh failed for {artist_name}: {e}")

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
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scan & dedupe Plex Music duplicates (CLI or WebUI)."
    )

    # Options for WebUI or CLI modes
    sub = parser.add_argument_group("Options for WebUI or CLI modes")
    sub.add_argument(
        "--serve",
        action="store_true",
        help="Launch Flask web interface"
    )
    # Legacy alias for Unraid CA compatibility
    sub.add_argument(
        "--webui",
        dest="serve",
        action="store_true",
        help="Alias for --serve (legacy compatibility)"
    )

    # CLI-only options
    cli = parser.add_argument_group("CLI-only options (ignored with --serve)")
    cli.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate moves & deletes but do not actually move files or call API."
    )
    cli.add_argument(
        "--safe-mode",
        action="store_true",
        help="Do not delete Plex metadata even if not dry-run."
    )
    cli.add_argument(
        "--tag-extra",
        action="store_true",
        help="If an edition has extra tracks, tag 'Extra Tracks' on the best version."
    )
    cli.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG-logging"
    )

    args = parser.parse_args()

    # Require PMDA_DEFAULT_MODE when no flags provided
    if not any([args.serve, args.dry_run, args.safe_mode, args.tag_extra, args.verbose]):
        mode = os.environ.get("PMDA_DEFAULT_MODE")
        if mode is None:
            raise SystemExit("Environment variable PMDA_DEFAULT_MODE must be set to 'serve' or 'cli'")
        mode = mode.lower()
        if mode == "serve":
            args.serve = True
        elif mode in ("cli", "run"):
            # CLI mode: no serve flag
            pass
        else:
            raise SystemExit(f"Invalid PMDA_DEFAULT_MODE: {mode}. Must be 'serve' or 'cli'")

    # Early logging setup for verbose
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Main: verbose mode enabled; root logger set to DEBUG")

    if args.serve:
        # WebUI mode
        app.run(host="0.0.0.0", port=WEBUI_PORT)
    else:
        # CLI mode: full scan, then dedupe
        logging.info("CLI mode: starting full library scan")
        background_scan()
        logging.info("CLI mode: scan complete, starting dedupe")
        dedupe_cli(
            dry=args.dry_run,
            safe=args.safe_mode,
            tag_extra=args.tag_extra,
            verbose=args.verbose
        )
