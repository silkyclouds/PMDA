#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

# --- GUI fallback stub for display_popup ---------------------------------------
try:
    import gui
except ModuleNotFoundError:
    # Fallback GUI stub if real gui module is missing
    class gui:
        @staticmethod
        def display_popup(message: str):
            # In headless mode, log the popup message as an error
            logging.error("[GUI POPUP] %s", message)

# Maximum consecutive artists with no valid files before aborting scan
NO_FILE_THRESHOLD = 10
# Global counter for consecutive no-file artists
no_file_streak_global = 0
# Track whether the no‑files popup has been shown to avoid duplicates
popup_displayed = False

"""
v0.7.5
- Improvement of the detection for albums with "no name" 
"""

import base64
import json
import os
import shutil
import filecmp
import errno
import sqlite3
import subprocess
import threading
import time
import atexit
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import NamedTuple, List, Dict, Optional, Tuple
from urllib.parse import quote_plus

import logging
import re
import socket
import struct
import hashlib
import xml.etree.ElementTree as ET

import requests
import musicbrainzngs

# MusicBrainz user agent will be configured after config is loaded
# (see _configure_musicbrainz_useragent function)
# Set rate limiting: 1 request per second (MusicBrainz limit)
musicbrainzngs.set_rate_limit(limit_or_interval=1.0, new_requests=1)
# Reduce noise from musicbrainzngs XML parser (e.g. "in <ws2:release-group>, uncaught attribute type-id")
logging.getLogger("musicbrainzngs").setLevel(logging.WARNING)
from openai import OpenAI
try:
    import anthropic
except ImportError:
    anthropic = None
try:
    import google.generativeai as genai
except ImportError:
    genai = None

# ──────────────── ANSI colours for prettier logs ────────────────
ANSI_RESET  = "\033[0m"
ANSI_BOLD   = "\033[1m"
ANSI_GREEN  = "\033[92m"
ANSI_YELLOW = "\033[93m"
ANSI_CYAN   = "\033[96m"
ANSI_RED    = "\033[91m"

def log_header(title: str) -> None:
    """Print a bold cyan header like `----- TITLE -----`."""
    logging.info("\n%s", colour(f"----- {title.upper()} -----", ANSI_BOLD + ANSI_CYAN))

def colour(txt: str, code: str) -> str:
    """Wrap *txt* in an ANSI colour code unless NO_COLOR env var is set."""
    if os.getenv("NO_COLOR"):
        return txt
    return f"{code}{txt}{ANSI_RESET}"

# ────────────────────── Robust cross‑device move helper ──────────────────────
def safe_move(src: str, dst: str):
    """
    Move *src* → *dst* de façon robuste, y compris entre volumes (EXDEV).
    - Try atomic same‑device rename first (os.replace).
    - On EXDEV or other rename failures, copy (file or tree) then remove source with retries.
    - Handles ENOTEMPTY/EBUSY during rmtree on slow NAS/SMB by retrying.
    """
    src_path = Path(src)
    dst_path = Path(dst)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    # 1) Fast path: atomic rename when on same device
    try:
        os.replace(src, dst)
        return
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            logging.warning("safe_move(): os.replace failed (%s) – falling back to copy", exc)
        # continue to copy fallback

    # 2) Choose a non‑clobbering destination (in case of leftovers)
    final_dst = dst_path
    if final_dst.exists():
        base = final_dst.name
        parent = final_dst.parent
        n = 1
        while (parent / f"{base} ({n})").exists():
            n += 1
        final_dst = parent / f"{base} ({n})"
        logging.warning("safe_move(): destination exists, using %s", final_dst)

    # 3) Copy (dir or single file)
    try:
        if src_path.is_dir():
            shutil.copytree(src_path, final_dst, dirs_exist_ok=False)
        else:
            shutil.copy2(src_path, final_dst)
    except Exception as copy_err:
        logging.error("safe_move(): copy failed %s → %s – %s", src_path, final_dst, copy_err)
        raise

    # 4) Remove source with retries (tolerate ENOTEMPTY/EBUSY on NAS)
    for attempt in range(5):
        try:
            if src_path.is_dir():
                shutil.rmtree(src_path)
            else:
                try:
                    src_path.unlink()
                except FileNotFoundError:
                    pass
            break
        except OSError as e:
            if e.errno in (errno.ENOTEMPTY, errno.EBUSY):
                logging.warning("safe_move(): rmtree(%s) failed (%s) – retry %d/5", src, e, attempt + 1)
                time.sleep(1.5)
                continue
            raise
    else:
        logging.warning("safe_move(): forcing removal of residual files in %s", src)
        shutil.rmtree(src_path, ignore_errors=True)

    # 5) Final safety net
    if os.path.exists(src):
        shutil.rmtree(src, ignore_errors=True)

from queue import SimpleQueue, Queue
import sys
import random



from flask import Flask, request, jsonify, send_from_directory, redirect, Response, send_file

app = Flask(__name__)

# Path to integrated frontend build (self-hosted: one container = backend + UI)
_FRONTEND_DIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend", "dist")
_HAS_STATIC_UI = os.path.isdir(_FRONTEND_DIST)

# CORS: allow frontend (e.g. dev server on port 3000 or 8080) to call the API
@app.after_request
def _cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response

# Ensure LOG_LEVEL exists for initial logging setup (effective level from SQLite applied later via merged)
LOG_LEVEL = "INFO"

# (8) Logging setup (must happen BEFORE any log statements elsewhere) ---------
_level_num = getattr(logging, LOG_LEVEL, logging.INFO)

logging.basicConfig(
    level=_level_num,
    format="%(asctime)s │ %(levelname)s │ %(threadName)s │ %(message)s",
    datefmt="%H:%M:%S",
    force=True,
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Progress header filter: displays current/total progress as [cur/total X.X%]
PROGRESS_STATE = {"total": 0, "current": 0}

# Suppress verbose internal debug from OpenAI and HTTP libraries
logging.getLogger("openai").setLevel(logging.INFO)
logging.getLogger("openai.api_requestor").setLevel(logging.INFO)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


# High-frequency polling routes: do not log each request (only real work / errors matter)
_QUIET_REQUEST_PATHS = (
    "/api/progress",
    "/api/dedupe",
    "/api/duplicates",
    "/api/library/improve-all/progress",
    "/api/lidarr/add-incomplete-albums/progress",
)


class _QuietPollingFilter(logging.Filter):
    """Drop request log lines for high-frequency polling routes to keep logs readable."""
    def filter(self, record):
        try:
            msg = record.getMessage()
            for path in _QUIET_REQUEST_PATHS:
                if path in msg:
                    return False
        except Exception:
            pass
        return True


logging.getLogger("werkzeug").addFilter(_QuietPollingFilter())

# ──────────────────────────── FFmpeg sanity-check ──────────────────────────────
# Central store for worker exceptions
worker_errors = SimpleQueue()

# ──────────────────────────────── AUTO–PURGE "INVALID" EDITIONS ────────────────────────────────
def _purge_invalid_edition(edition: dict):
    """
    Instantly move technically‑invalid rips (0‑byte folder or no media‑info) to
    /dupes and wipe their Plex metadata so they never show up as duplicates.

    This runs during the *scan* phase, therefore it must be completely
    exception‑safe and thread‑safe.
    """
    try:
        src_folder = Path(edition["folder"])
        if not src_folder.exists():
            return                                                    # already gone

        # Destination under /dupes, keep relative path when possible
        base_dst = build_dupe_destination(src_folder)
        dst = base_dst
        counter = 1
        while dst.exists():                                            # avoid clashes
            dst = base_dst.parent / f"{base_dst.name} ({counter})"
            counter += 1
        dst.parent.mkdir(parents=True, exist_ok=True)

        # Move (or copy‑then‑delete) the folder ----------------------
        try:
            safe_move(str(src_folder), str(dst))
        except Exception as move_err:
            logging.warning("Auto‑purge: moving %s → %s failed – %s",
                            src_folder, dst, move_err)
            return

        size_mb = folder_size(dst) // (1024 * 1024)
        increment_stat("removed_dupes", 1)
        increment_stat("space_saved", size_mb)

        # Tech‑data are irrelevant (all zero), but we still log them
        notify_discord(
            f"🗑️  Auto‑purged invalid rip for **{edition['artist']} – "
            f"{edition['title_raw']}** ({size_mb} MB moved to /dupes)"
        )

        # Kill Plex metadata so the ghost album disappears
        try:
            plex_api(f"/library/metadata/{edition['album_id']}/trash", method="PUT")
            time.sleep(0.3)
            plex_api(f"/library/metadata/{edition['album_id']}", method="DELETE")
            # Refresh artist view & empty trash
            art_enc = quote_plus(edition['artist'])
            letter  = quote_plus(edition['artist'][0].upper())
            plex_api(f"/library/sections/{SECTION_ID}/refresh"
                     f"?path=/music/matched/{letter}/{art_enc}", method="GET")
            plex_api(f"/library/sections/{SECTION_ID}/emptyTrash", method="PUT")
        except Exception as e:
            logging.debug("Plex cleanup for invalid edition failed: %s", e)

    except Exception as exc:
        logging.warning("Auto‑purge of invalid edition failed: %s", exc)


# ──────────────────────────── FFmpeg sanity-check ──────────────────────────────
def _check_ffmpeg():
    """
    Log the location of ffmpeg/ffprobe or warn clearly if they are missing.
    Runs once at startup.
    """
    log_header("ffmpeg")
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
Configuration is stored in SQLite (state.db, settings table) only. No config.json is used.
* _get() reads from SQLite first, then falls back to hardcoded defaults.
* Web UI and API save settings to SQLite via api_config_put().
"""

import filecmp

# Helper parsers --------------------------------------------------------------
def _parse_bool(val: str | bool) -> bool:
    """Return *True* for typical truthy strings / bools and *False* otherwise."""
    if isinstance(val, bool):
        return val
    val_normalized = str(val).strip().lower()
    return val_normalized in {"1", "true", "yes", "on"}

# Helper for falsy logic, if needed later
def _is_false(val: str | bool) -> bool:
    """Return *True* for typical falsy strings / bools and *False* otherwise."""
    if isinstance(val, bool):
        return not val
    val_normalized = str(val).strip().lower()
    return val_normalized in {"0", "false", "no", "off"}

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

# Determine runtime config dir -------------------------------------------------
BASE_DIR   = Path(__file__).parent
CONFIG_DIR = Path(os.getenv("PMDA_CONFIG_DIR", BASE_DIR))
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
STATE_DB_FILE = CONFIG_DIR / "state.db"


def _get_from_sqlite(key: str, default=None):
    """Read a single config value from SQLite settings table (used before merged exists)."""
    try:
        if STATE_DB_FILE.exists():
            con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
            cur = con.cursor()
            cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cur.fetchone()
            con.close()
            if row and row[0]:
                return row[0]
    except Exception:
        pass
    return default


# Location of baked‑in template for AI prompt (shipped inside the image)
DEFAULT_PROMPT_PATH  = BASE_DIR / "ai_prompt.txt"
AI_PROMPT_FILE = CONFIG_DIR / "ai_prompt.txt"

# Ensure ai_prompt.txt exists -------------------------------------------
if not AI_PROMPT_FILE.exists():
    logging.info("ai_prompt.txt not found — default prompt created")
    shutil.copyfile(DEFAULT_PROMPT_PATH, AI_PROMPT_FILE)

# ─── Auto‑generate PATH_MAP from Plex at *every* startup ────────────────────
#
# Design goal: avoid requiring users to manually configure PATH_MAP (Plex path → host path).
# Flow:
#   1) Discovery: Plex API returns all <Location> paths for the music section(s).
#      We get the exact paths Plex uses (e.g. /music/matched, /music/unmatched).
#   2) Merge: If the user provided a broader PATH_MAP in SQLite (e.g. /music → /music/…),
#      we apply it by prefix; otherwise we keep Plex path = container path (so Docker
#      volume binds must match: -v /host/Music_matched:/music/matched).
#   3) Cross-check: For each binding we sample CROSSCHECK_SAMPLES tracks from the DB,
#      resolve their paths via PATH_MAP, and verify the files exist on disk. If they
#      don’t, we try to find the correct host root (sibling dirs or rglob) and patch
#      PATH_MAP in memory and SQLite. So even when paths differ (e.g. Plex says /music/unmatched
#      but on host it’s /music/Music_dump), we auto-correct instead of failing.
# Result: users only need to mount volumes; PMDA discovers Plex paths and validates
# (and repairs) bindings so the same paths work for scan/dedupe.

def _discover_path_map(plex_host: str, plex_token: str, section_id: int) -> dict[str, str]:
    """
    Query Plex for all <Location> paths belonging to *section_id* and return
    a mapping of {container_path: container_path}.  This is run at each
    startup so that changes in the Plex UI (adding/removing folders) are
    picked up automatically.

    A hard failure (network/XML/bad token/empty list) is surfaced so that
    users notice mis‑configuration early.
    """
    logging.debug(
        "PATH_MAP discovery: requesting %s (filter section=%s)",
        plex_host.rstrip('/') + "/library/sections",
        section_id
    )
    url = f"{plex_host.rstrip('/')}/library/sections"
    resp = requests.get(url, headers={"X-Plex-Token": plex_token}, timeout=10)
    logging.debug("PATH_MAP discovery: HTTP %s – %d bytes", resp.status_code, len(resp.content))
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug("PATH_MAP discovery response (first 500 chars): %s", resp.text[:500])
    resp.raise_for_status()

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as e:
        raise RuntimeError(f"Invalid XML returned by Plex: {e}") from None

    seen: set[str] = set()
    for directory in root.iter("Directory"):
        if directory.attrib.get("key") != str(section_id):
            continue
        for loc in directory.iter("Location"):
            path = (loc.attrib.get("path") or "").strip()
            if not path:
                continue
            # Support path with comma- or semicolon-separated entries (some Plex versions)
            for single in re.split(r"[,;]", path):
                single = single.strip()
                if single:
                    seen.add(single)
        # Do not break: some Plex versions return one Directory per Location; collect all.
    locations = list(seen)
    logging.debug("PATH_MAP discovery: parsed %d <Location> paths -> %s", len(locations), locations)
    if not locations:
        raise RuntimeError("No <Location> elements found for this section")

    logging.info("PATH_MAP discovery successful – %d paths found", len(locations))
    return {p: p for p in locations}

# Always attempt discovery – even when PATH_MAP already exists – so the file
# stays in sync with the Plex configuration. If PLEX_HOST or PLEX_TOKEN is
# missing, we start in "wizard" (unconfigured) mode and skip discovery.
SECTION_IDS: list[int] = []
SECTION_NAMES: dict[int, str] = {}

# Load SECTION_IDS from SQLite first so user's saved library selection is never overwritten by auto-detect
try:
    state_db_path = CONFIG_DIR / "state.db"
    if state_db_path.exists():
        con = sqlite3.connect(str(state_db_path), timeout=5)
        cur = con.cursor()
        cur.execute("SELECT value FROM settings WHERE key = ?", ("SECTION_IDS",))
        row = cur.fetchone()
        con.close()
        if row and row[0]:
            raw = str(row[0]).strip()
            if raw:
                if raw.startswith("["):
                    SECTION_IDS = [int(x) for x in json.loads(raw)]
                else:
                    SECTION_IDS = [int(x.strip()) for x in raw.split(",") if x.strip()]
                logging.info("Loaded SECTION_IDS from SQLite at startup (saved selection): %s", SECTION_IDS)
except Exception as e:
    logging.debug("Could not load SECTION_IDS from SQLite at startup: %s", e)

try:
    plex_host = (_get_from_sqlite("PLEX_HOST", "") or "").strip() if isinstance(_get_from_sqlite("PLEX_HOST", ""), str) else ""
    plex_token = (_get_from_sqlite("PLEX_TOKEN", "") or "").strip() if isinstance(_get_from_sqlite("PLEX_TOKEN", ""), str) else ""
    # Require a URL-like host so we never call Plex API with empty/invalid base
    if not plex_host or not plex_token or not str(plex_host).strip().startswith(("http://", "https://")):
        logging.info("PLEX_HOST or PLEX_TOKEN missing or invalid – starting in unconfigured (wizard) mode")
    else:
        # SECTION_IDS already loaded from SQLite above; do not use env/config
        raw_sections = None

        # Treat an empty string or whitespace‑only value as “not provided”
        logging.debug("SECTION_IDS from SQLite: %r", SECTION_IDS)

        # Only auto-detect if we don't already have SECTION_IDS from SQLite (user's saved selection)
        if not raw_sections:
            if not SECTION_IDS:
                try:
                    resp = requests.get(f"{plex_host.rstrip('/')}/library/sections", headers={"X-Plex-Token": plex_token}, timeout=10)
                    root = ET.fromstring(resp.text)
                    SECTION_IDS = [int(d.attrib['key']) for d in root.iter("Directory") if d.attrib.get('type') == 'artist']
                    logging.info("Auto-detected SECTION_IDS from Plex: %s", SECTION_IDS)
                except Exception as e:
                    logging.error("Failed to auto-detect SECTION_IDS: %s", e)
                    SECTION_IDS = []
            else:
                logging.info("Using SECTION_IDS from SQLite (saved selection): %s", SECTION_IDS)
        if SECTION_IDS:
            try:
                resp = requests.get(f"{plex_host.rstrip('/')}/library/sections", headers={"X-Plex-Token": plex_token}, timeout=10)
                root = ET.fromstring(resp.text)
                SECTION_NAMES.update({int(directory.attrib['key']): directory.attrib.get('title', '<unknown>') for directory in root.iter('Directory')})
            except Exception:
                pass
            if SECTION_NAMES:
                log_header("libraries")
                for sid in SECTION_IDS:
                    name = SECTION_NAMES.get(sid, "<unknown>")
                    logging.info("  %s (ID %d)", name, sid)
            auto_map = {}
            for sid in SECTION_IDS:
                part = _discover_path_map(plex_host, plex_token, sid)
                auto_map.update(part)
            log_header("path_map discovery")
            logging.info("Auto‑generated raw PATH_MAP from Plex: %s", auto_map)
            raw_env_map = _parse_path_map(_get_from_sqlite("PATH_MAP") or {})
            logging.info("Raw PATH_MAP from SQLite: %s", raw_env_map)
            merged_map = {}
            for cont_path, cont_val in auto_map.items():
                mapped = False
                for prefix, host_base in sorted(raw_env_map.items(), key=lambda item: len(item[0]), reverse=True):
                    if cont_path.startswith(prefix):
                        suffix = cont_path[len(prefix):].lstrip("/")
                        merged_map[cont_path] = os.path.join(host_base, suffix)
                        mapped = True
                        break
                if not mapped:
                    merged_map[cont_path] = cont_val
            logging.info("Merged PATH_MAP for startup: %s", merged_map)
            log_header("volume bindings (plex → pmda → host)")
            logging.info("%-40s | %-30s | %s", "PLEX_PATH", "PMDA_PATH", "HOST_PATH")
            for plex_path, host_path in merged_map.items():
                logging.info("%-40s | %-30s | %s", plex_path, host_path, host_path)
            # Persist PATH_MAP to SQLite (single source of truth)
            try:
                con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
                con.execute("INSERT OR REPLACE INTO settings(key, value) VALUES('PATH_MAP', ?)", (json.dumps(merged_map),))
                con.commit()
                con.close()
            except Exception as e:
                logging.debug("Could not persist PATH_MAP to SQLite at discovery: %s", e)
            logging.info("Auto-generated/updated PATH_MAP from Plex (saved to SQLite)")
except Exception as e:
    logging.warning("⚠️  Failed to auto‑generate PATH_MAP – %s", e)
    SECTION_IDS = []
    SECTION_NAMES = {}

# Load SECTION_IDS from SQLite if saved (so restart uses last library selection)
try:
    state_db_path = CONFIG_DIR / "state.db"
    if state_db_path.exists():
        con = sqlite3.connect(str(state_db_path), timeout=5)
        cur = con.cursor()
        cur.execute("SELECT value FROM settings WHERE key = ?", ("SECTION_IDS",))
        row = cur.fetchone()
        con.close()
        if row and row[0]:
            raw = str(row[0]).strip()
            if raw:
                if raw.startswith("["):
                    SECTION_IDS = [int(x) for x in json.loads(raw)]
                else:
                    SECTION_IDS = [int(x.strip()) for x in raw.split(",") if x.strip()]
                logging.info("Loaded SECTION_IDS from SQLite at startup: %s", SECTION_IDS)
except Exception as e:
    logging.debug("Could not load SECTION_IDS from SQLite at startup: %s", e)

# (4) Merge with environment variables ----------------------------------------
ENV_SOURCES: dict[str, str] = {}

def _get(key: str, *, default=None, cast=lambda x: x):
    """Return the merged value and remember where it came from.
    Priority: SQLite only > default (no env, no config.json).
    """
    sqlite_val = None
    try:
        if STATE_DB_FILE.exists():
            con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
            cur = con.cursor()
            cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cur.fetchone()
            con.close()
            if row and row[0]:
                sqlite_val = row[0]
    except Exception:
        pass

    if sqlite_val is not None:
        ENV_SOURCES[key] = "sqlite"
        raw = sqlite_val
        if isinstance(raw, str) and raw.strip() == "" and default is not None:
            ENV_SOURCES[key] = "default"
            raw = default
        else:
            return cast(raw)
    ENV_SOURCES[key] = "default"
    raw = default
    return cast(raw)


# Old default that included MusicBrainz IDs (caused all albums to show as "incomplete" if no MB tags)
_REQUIRED_TAGS_OLD_DEFAULT_SET = {"artist", "album", "date", "musicbrainz_release_group_id", "musicbrainz_artist_id"}


def _parse_required_tags(val):
    """Return REQUIRED_TAGS as a list of strings. Handles JSON array string from DB, comma-separated string, or list.
    Migrates old default (with musicbrainz_release_group_id, musicbrainz_artist_id) to new default (artist, album, date, genre, year)."""
    default = ["artist", "album", "date", "genre", "year"]
    if val is None:
        return default
    if isinstance(val, list):
        out = [str(t).strip().lower() for t in val if str(t).strip()]
    else:
        s = str(val).strip()
        if not s:
            return default
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    out = [str(t).strip().lower() for t in parsed if str(t).strip()]
                else:
                    out = []
            except (json.JSONDecodeError, TypeError):
                out = [t.strip().lower() for t in s.split(",") if t.strip()]
        else:
            out = [t.strip().lower() for t in s.split(",") if t.strip()]
    if not out:
        return default
    # Migrate old default so all albums are not reported as incomplete
    if set(out) == _REQUIRED_TAGS_OLD_DEFAULT_SET:
        return default
    return out


def _check_required_tags(meta: dict, required_tags: list, edition: dict | None = None) -> list:
    """
    Return the list of required tag names that are missing.
    Uses REQUIRED_TAGS from Settings as the single source of truth.
    meta: tags from first audio file (lowercase keys from ffprobe).
    edition: optional edition dict with 'tracks' (Track NamedTuples or dicts with title, idx/index).
    """
    # Map config tag name (lowercase) -> meta keys to check (ffprobe returns lowercase)
    TAG_META_KEYS = {
        "artist": ["artist", "albumartist"],
        "album": ["album"],
        "date": ["date", "originaldate"],
        "year": ["year", "date"],
        "genre": ["genre"],
        "musicbrainz_release_group_id": ["musicbrainz_releasegroupid", "musicbrainz_release_group_id"],
        "musicbrainz_artist_id": ["musicbrainz_artistid", "musicbrainz_albumartistid", "musicbrainz_artist_id"],
    }
    missing = []
    for tag in required_tags:
        key = (tag or "").strip().lower()
        if not key:
            continue
        if key == "tracks":
            # Require every track to have a non-empty title and a valid index (from Plex/edition)
            if not edition:
                missing.append(tag)
                continue
            tracks = edition.get("tracks") or []
            if not tracks:
                missing.append(tag)
                continue
            def _track_ok(t):
                title = getattr(t, "title", None) if hasattr(t, "title") else (t.get("title") if isinstance(t, dict) else None)
                idx = getattr(t, "idx", None) if hasattr(t, "idx") else (t.get("idx") if isinstance(t, dict) else t.get("index"))
                return bool(title and str(title).strip()) and idx is not None
            if not all(_track_ok(t) for t in tracks):
                missing.append(tag)
            continue
        meta_keys = TAG_META_KEYS.get(key, [key])
        if not any((meta or {}).get(k) for k in meta_keys):
            missing.append(tag)
    return missing


def _parse_skip_folders(val):
    """Return SKIP_FOLDERS as a list of path strings. Handles JSON array from DB (or corrupted double-encoded),
    comma-separated string, or list. Drops any element that looks like JSON (e.g. '[]') so corrupted values become []."""
    if val is None:
        return []
    if isinstance(val, list):
        raw = [str(p).strip() for p in val if str(p).strip()]
    else:
        s = str(val).strip()
        if not s:
            return []
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    raw = [str(p).strip() for p in parsed if str(p).strip()]
                else:
                    raw = []
            except (json.JSONDecodeError, TypeError):
                raw = [p.strip() for p in s.split(",") if p.strip()]
        else:
            raw = [p.strip() for p in s.split(",") if p.strip()]
    # Drop corrupted entries: paths must not look like JSON (e.g. "[]", '["[]"]')
    return [p for p in raw if p and not p.startswith("[")]


def _parse_format_preference_early(val):
    """Parse FORMAT_PREFERENCE for use in merged (before _parse_format_preference is defined)."""
    _default = ["dsf", "aif", "aiff", "wav", "flac", "m4a", "mp4", "m4b", "m4p", "aifc", "ogg", "opus", "mp3", "wma"]
    if val is None or (isinstance(val, str) and not val.strip()):
        return _default
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        s = val.strip()
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, TypeError):
                pass
            return _default
        parts = [x.strip() for x in s.split(",") if x.strip()]
        return parts if parts else _default
    return _default


# ─── Plex DB auto-discovery from base path (one-shot, persisted to SQLite) ───
PLEX_DB_FILENAME = "com.plexapp.plugins.library.db"
# Known relative paths under Plex config root (Linux/Docker/macOS, then Windows-style)
_PLEX_DB_RELATIVE_PATHS = [
    "Library/Application Support/Plex Media Server/Plug-in Support/Databases",
    os.path.join("Plex Media Server", "Plug-in Support", "Databases"),
]


def _resolve_plex_db_from_base(base_path: str) -> str | None:
    """
    Find the directory containing com.plexapp.plugins.library.db under base_path.
    Tries known relative paths first, then recursive search (max depth 10).
    Returns the directory path (str) or None if not found.
    """
    base = Path(base_path).resolve()
    if not base.exists() or not base.is_dir():
        return None
    # Step 1: known relative paths
    for rel in _PLEX_DB_RELATIVE_PATHS:
        candidate = base / rel
        if (candidate / PLEX_DB_FILENAME).exists():
            return str(candidate)
    # Step 2: recursive search with max depth 10
    max_depth = 10
    base_str = str(base)
    for root, _dirs, files in os.walk(base, topdown=True):
        try:
            depth = len(Path(root).relative_to(base).parts) if root != base_str else 0
        except ValueError:
            continue
        if depth > max_depth:
            _dirs.clear()
            continue
        if PLEX_DB_FILENAME in files:
            return root
    return None


def _ensure_plex_db_path_resolved() -> str | None:
    """
    If PLEX_DB_PATH is already in SQLite and the DB file exists, return it.
    Otherwise resolve from PLEX_BASE_PATH (or /database), persist to SQLite if found, and return.
    Called before building merged so _get("PLEX_DB_PATH") can read the persisted value.
    """
    db_path = _get_from_sqlite("PLEX_DB_PATH")
    if db_path and (Path(db_path) / PLEX_DB_FILENAME).exists():
        return str(db_path).strip()
    base = (_get_from_sqlite("PLEX_BASE_PATH") or "").strip() or "/database"
    resolved = _resolve_plex_db_from_base(base)
    if resolved:
        try:
            if STATE_DB_FILE.exists():
                con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
                con.execute("INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)", ("PLEX_DB_PATH", resolved))
                con.commit()
                con.close()
                logging.info("Plex DB discovered at %s (saved to SQLite)", resolved)
        except Exception as e:
            logging.debug("Could not persist PLEX_DB_PATH to SQLite: %s", e)
        return resolved
    return None


# PLEX_DB_PATH defaults to /database in container; no SystemExit if unconfigured.
merged = {
    "PLEX_DB_PATH":   (_ensure_plex_db_path_resolved() or _get("PLEX_DB_PATH",   default="/database", cast=str)),
    "PLEX_HOST":      _get("PLEX_HOST",      default="",                                cast=str),
    "PLEX_TOKEN":     _get("PLEX_TOKEN",     default="",                                cast=str),
    "SECTION_ID": SECTION_IDS[0] if SECTION_IDS else 0,
    "SCAN_THREADS":   _get("SCAN_THREADS",   default=os.cpu_count() or 4,               cast=_parse_int),
    "PATH_MAP":       _parse_path_map(_get("PATH_MAP", default={})),
    "LOG_LEVEL":      _get("LOG_LEVEL",      default="INFO").upper(),
    "AI_PROVIDER": _get("AI_PROVIDER", default="openai", cast=str),
    "OPENAI_API_KEY": _get("OPENAI_API_KEY", default="",                                cast=str),
    "OPENAI_MODEL":   _get("OPENAI_MODEL",   default="gpt-4",                           cast=str),
    "ANTHROPIC_API_KEY": _get("ANTHROPIC_API_KEY", default="", cast=str),
    "GOOGLE_API_KEY": _get("GOOGLE_API_KEY", default="", cast=str),
    "OLLAMA_URL": _get("OLLAMA_URL", default="http://localhost:11434", cast=str),
    "DISCORD_WEBHOOK": _get("DISCORD_WEBHOOK", default="", cast=str),
    "USE_MUSICBRAINZ": _get("USE_MUSICBRAINZ", default=False, cast=_parse_bool),
    "MUSICBRAINZ_EMAIL": _get("MUSICBRAINZ_EMAIL", default="pmda@example.com", cast=str),
    "MB_QUEUE_ENABLED": _get("MB_QUEUE_ENABLED", default=True, cast=_parse_bool),
    "MB_RETRY_NOT_FOUND": _get("MB_RETRY_NOT_FOUND", default=False, cast=_parse_bool),
    "LIDARR_URL": _get("LIDARR_URL", default="", cast=str),
    "LIDARR_API_KEY": _get("LIDARR_API_KEY", default="", cast=str),
    "AUTOBRR_URL": _get("AUTOBRR_URL", default="", cast=str),
    "AUTOBRR_API_KEY": _get("AUTOBRR_API_KEY", default="", cast=str),
    "AUTO_FIX_BROKEN_ALBUMS": _get("AUTO_FIX_BROKEN_ALBUMS", default=False, cast=_parse_bool),
    "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD": _get("BROKEN_ALBUM_CONSECUTIVE_THRESHOLD", default=2, cast=int),
    "BROKEN_ALBUM_PERCENTAGE_THRESHOLD": _get("BROKEN_ALBUM_PERCENTAGE_THRESHOLD", default=0.20, cast=float),
    "REQUIRED_TAGS": _get("REQUIRED_TAGS", default="artist,album,date,genre,year", cast=_parse_required_tags),
    "SKIP_FOLDERS": _get("SKIP_FOLDERS", default="", cast=_parse_skip_folders),
    "AI_BATCH_SIZE": _get("AI_BATCH_SIZE", default=10, cast=int),
    "FFPROBE_POOL_SIZE": _get("FFPROBE_POOL_SIZE", default=4, cast=int),
    "AUTO_MOVE_DUPES": _get("AUTO_MOVE_DUPES", default=False, cast=_parse_bool),
    "USE_AI_FOR_MB_MATCH": _get("USE_AI_FOR_MB_MATCH", default=False, cast=_parse_bool),
    "USE_AI_FOR_MB_VERIFY": _get("USE_AI_FOR_MB_VERIFY", default=False, cast=_parse_bool),
    "USE_AI_VISION_FOR_COVER": _get("USE_AI_VISION_FOR_COVER", default=False, cast=_parse_bool),
    "OPENAI_VISION_MODEL": _get("OPENAI_VISION_MODEL", default="", cast=str),
    "USE_WEB_SEARCH_FOR_MB": _get("USE_WEB_SEARCH_FOR_MB", default=False, cast=_parse_bool),
    "SERPER_API_KEY": _get("SERPER_API_KEY", default="", cast=str),
    "CROSS_LIBRARY_DEDUPE": _get("CROSS_LIBRARY_DEDUPE", default="true", cast=_parse_bool),
    "CROSSCHECK_SAMPLES": _get("CROSSCHECK_SAMPLES", default=20, cast=lambda x: int(x) if x is not None and str(x).strip().isdigit() else 20),
    "LOG_FILE": _get("LOG_FILE", default="", cast=str) or str(CONFIG_DIR / "pmda.log"),
    "OPENAI_MODEL_FALLBACKS": _get("OPENAI_MODEL_FALLBACKS", default="", cast=str),
    "DISABLE_PATH_CROSSCHECK": _get("DISABLE_PATH_CROSSCHECK", default="false", cast=_parse_bool),
    "FORMAT_PREFERENCE": _get("FORMAT_PREFERENCE", default=None, cast=lambda v: _parse_format_preference_early(v)),
    "USE_DISCOGS": _get("USE_DISCOGS", default=False, cast=_parse_bool),
    "DISCOGS_USER_TOKEN": _get("DISCOGS_USER_TOKEN", default="", cast=str),
    "USE_LASTFM": _get("USE_LASTFM", default=False, cast=_parse_bool),
    "LASTFM_API_KEY": _get("LASTFM_API_KEY", default="", cast=str),
    "LASTFM_API_SECRET": _get("LASTFM_API_SECRET", default="", cast=str),
    "USE_BANDCAMP": _get("USE_BANDCAMP", default=False, cast=_parse_bool),
}
# PATH_MAP and all config from _get() (SQLite only > default)

SKIP_FOLDERS: list[str] = merged["SKIP_FOLDERS"]
USE_MUSICBRAINZ: bool = bool(merged["USE_MUSICBRAINZ"])
MUSICBRAINZ_EMAIL: str = merged.get("MUSICBRAINZ_EMAIL", "pmda@example.com")
MB_QUEUE_ENABLED: bool = bool(merged.get("MB_QUEUE_ENABLED", True))
MB_RETRY_NOT_FOUND: bool = bool(merged.get("MB_RETRY_NOT_FOUND", False))

# Configure MusicBrainz User-Agent with user's email (if provided)
def _configure_musicbrainz_useragent():
    """Configure MusicBrainz User-Agent with the user's email."""
    email = MUSICBRAINZ_EMAIL.strip() if MUSICBRAINZ_EMAIL else "pmda@example.com"
    musicbrainzngs.set_useragent(
        "PMDA",               # application name
        "0.6.6",              # application version (sync with header)
        email                 # contact / support email
    )
    logging.debug("MusicBrainz User-Agent configured with email: %s", email)

# Configure User-Agent now that config is loaded
_configure_musicbrainz_useragent()
LIDARR_URL: str = merged.get("LIDARR_URL", "")
LIDARR_API_KEY: str = merged.get("LIDARR_API_KEY", "")
AUTOBRR_URL: str = merged.get("AUTOBRR_URL", "")
AUTOBRR_API_KEY: str = merged.get("AUTOBRR_API_KEY", "")
AUTO_FIX_BROKEN_ALBUMS: bool = bool(merged.get("AUTO_FIX_BROKEN_ALBUMS", False))
BROKEN_ALBUM_CONSECUTIVE_THRESHOLD: int = int(merged.get("BROKEN_ALBUM_CONSECUTIVE_THRESHOLD", 2))
BROKEN_ALBUM_PERCENTAGE_THRESHOLD: float = float(merged.get("BROKEN_ALBUM_PERCENTAGE_THRESHOLD", 0.20))
REQUIRED_TAGS: list[str] = merged.get("REQUIRED_TAGS", ["artist", "album", "date"])
AUTO_MOVE_DUPES: bool = bool(merged["AUTO_MOVE_DUPES"])
USE_AI_FOR_MB_MATCH: bool = bool(merged.get("USE_AI_FOR_MB_MATCH", False))
USE_AI_FOR_MB_VERIFY: bool = bool(merged.get("USE_AI_FOR_MB_VERIFY", False))
USE_AI_VISION_FOR_COVER: bool = bool(merged.get("USE_AI_VISION_FOR_COVER", False))
OPENAI_VISION_MODEL: str = str(merged.get("OPENAI_VISION_MODEL", "") or "").strip()
USE_WEB_SEARCH_FOR_MB: bool = bool(merged.get("USE_WEB_SEARCH_FOR_MB", False))
SERPER_API_KEY: str = str(merged.get("SERPER_API_KEY", "") or "").strip()
AI_BATCH_SIZE: int = int(merged.get("AI_BATCH_SIZE", 10))
FFPROBE_POOL_SIZE: int = int(merged.get("FFPROBE_POOL_SIZE", 4))
# Cross-library dedupe configuration (from SQLite only)
CROSS_LIBRARY_DEDUPE = merged["CROSS_LIBRARY_DEDUPE"]

# Number of sample tracks per Plex mount to verify (from SQLite only)
CROSSCHECK_SAMPLES = merged["CROSSCHECK_SAMPLES"]

# Skip PATH cross-check at startup when set (from SQLite only)
DISABLE_PATH_CROSSCHECK = merged["DISABLE_PATH_CROSSCHECK"]

# Metadata fallback providers (Improve Album)
USE_DISCOGS: bool = bool(merged.get("USE_DISCOGS", False))
DISCOGS_USER_TOKEN: str = str(merged.get("DISCOGS_USER_TOKEN", "") or "")
USE_LASTFM: bool = bool(merged.get("USE_LASTFM", False))
LASTFM_API_KEY: str = str(merged.get("LASTFM_API_KEY", "") or "")
LASTFM_API_SECRET: str = str(merged.get("LASTFM_API_SECRET", "") or "")
USE_BANDCAMP: bool = bool(merged.get("USE_BANDCAMP", False))

# ─────────────────────────────── Fixed container constants ───────────────────────────────
# DB filename is always fixed under the Plex DB folder
PLEX_DB_FILE = str(Path(merged["PLEX_DB_PATH"]) / "com.plexapp.plugins.library.db")
PLEX_DB_EXISTS = Path(PLEX_DB_FILE).exists()
# Fully configured only when we have host, token, sections, and readable DB
PLEX_CONFIGURED = bool(
    merged["PLEX_HOST"] and merged["PLEX_TOKEN"] and SECTION_IDS and PLEX_DB_EXISTS
)
if not PLEX_CONFIGURED:
    logging.info(
        "Starting in unconfigured (wizard) mode – configure Plex and mount the database at /database in Settings."
    )
# Duplicates always move to /dupes inside the container
DUPE_ROOT = Path("/dupes")
# WebUI always listens on container port 5005 inside the container
WEBUI_PORT = 5005


def _paths_rw_status() -> dict:
    """
    Check that music paths (PATH_MAP values, or /music when PATH_MAP empty) and DUPE_ROOT are readable and writable.
    Uses the same logic as _container_mounts_status() so Settings "Path access" matches the welcome modal.
    Returns dict with music_rw, dupes_rw (bool) and optional messages for UI.
    """
    path_map = getattr(sys.modules[__name__], "PATH_MAP", {})
    dupe_root = getattr(sys.modules[__name__], "DUPE_ROOT", Path("/dupes"))
    music_rw = True
    dupes_rw = dupe_root.exists() and os.access(dupe_root, os.R_OK) and os.access(dupe_root, os.W_OK)
    if path_map:
        for dest in path_map.values():
            p = Path(dest)
            if not p.exists():
                logging.debug("_paths_rw_status: music path %s does not exist", p)
                music_rw = False
                break
            if not os.access(p, os.R_OK) or not os.access(p, os.W_OK):
                logging.debug("_paths_rw_status: music path %s not R+W (R=%s W=%s)", p, os.access(p, os.R_OK), os.access(p, os.W_OK))
                music_rw = False
                break
    else:
        # Same fallback as _container_mounts_status(): check /music when no PATH_MAP yet
        default_music = Path("/music")
        music_rw = default_music.exists() and os.access(default_music, os.R_OK) and os.access(default_music, os.W_OK)
        if not music_rw:
            logging.debug("_paths_rw_status: default /music exists=%s R=%s W=%s",
                          default_music.exists(), os.access(default_music, os.R_OK) if default_music.exists() else False, os.access(default_music, os.W_OK) if default_music.exists() else False)
    return {"music_rw": music_rw, "dupes_rw": dupes_rw}


def _container_mounts_status() -> dict:
    """
    Check that all container mounts PMDA needs are present and have the expected access.
    Used for the fresh-config welcome message so users see at a glance if bindings are OK.
    Returns dict with config_rw, plex_db_ro, music_rw, dupes_rw (all bool).
    """
    config_dir = getattr(sys.modules[__name__], "CONFIG_DIR", Path("/config"))
    plex_db_path = getattr(sys.modules[__name__], "PLEX_DB_PATH", "/database")
    path_map = getattr(sys.modules[__name__], "PATH_MAP", {})
    dupe_root = getattr(sys.modules[__name__], "DUPE_ROOT", Path("/dupes"))

    config_rw = config_dir.exists() and os.access(config_dir, os.R_OK) and os.access(config_dir, os.W_OK)
    plex_db_path_p = Path(plex_db_path)
    plex_db_ro = plex_db_path_p.exists() and os.access(plex_db_path_p, os.R_OK)

    # When PATH_MAP is empty (fresh config), check the standard container mount /music (parent music folder)
    music_rw = True
    if path_map:
        for dest in path_map.values():
            p = Path(dest)
            if not p.exists() or not os.access(p, os.R_OK) or not os.access(p, os.W_OK):
                logging.debug("_container_mounts_status: music path %s exists=%s R=%s W=%s", p, p.exists(), os.access(p, os.R_OK) if p.exists() else False, os.access(p, os.W_OK) if p.exists() else False)
                music_rw = False
                break
    else:
        # No PATH_MAP yet: verify the typical bind mount /music (parent music folder) is RW
        default_music = Path("/music")
        music_rw = default_music.exists() and os.access(default_music, os.R_OK) and os.access(default_music, os.W_OK)
        if not music_rw:
            logging.debug("_container_mounts_status: default /music exists=%s R=%s W=%s",
                          default_music.exists(), os.access(default_music, os.R_OK) if default_music.exists() else False, os.access(default_music, os.W_OK) if default_music.exists() else False)

    dupes_rw = dupe_root.exists() and os.access(dupe_root, os.R_OK) and os.access(dupe_root, os.W_OK)

    return {"config_rw": config_rw, "plex_db_ro": plex_db_ro, "music_rw": music_rw, "dupes_rw": dupes_rw}

# (7) Export as module‑level constants ----------------------------------------
PLEX_HOST      = merged["PLEX_HOST"]
PLEX_TOKEN     = merged["PLEX_TOKEN"]
SECTION_IDS    = SECTION_IDS
SECTION_ID     = SECTION_IDS[0] if SECTION_IDS else 0
PATH_MAP       = merged["PATH_MAP"]
import multiprocessing
SCAN_THREADS = merged["SCAN_THREADS"]
LOG_LEVEL      = merged["LOG_LEVEL"]
AI_PROVIDER    = merged["AI_PROVIDER"]
OPENAI_API_KEY = merged["OPENAI_API_KEY"]
OPENAI_MODEL   = merged["OPENAI_MODEL"]
ANTHROPIC_API_KEY = merged["ANTHROPIC_API_KEY"]
GOOGLE_API_KEY = merged["GOOGLE_API_KEY"]
OLLAMA_URL     = merged["OLLAMA_URL"]
DISCORD_WEBHOOK = merged["DISCORD_WEBHOOK"]

#
# State and cache DB always live in the config directory (STATE_DB_FILE defined early after CONFIG_DIR)
CACHE_DB_FILE = CONFIG_DIR / "cache.db"

# File-format preference order (from SQLite only)
FORMAT_PREFERENCE = merged["FORMAT_PREFERENCE"]

# Optional external log file (rotates @ 5 MB x 3) (from SQLite only)
LOG_FILE = merged["LOG_FILE"]

# Attach rotating file handler now that CONFIG_DIR / LOG_FILE are final
try:
    from logging.handlers import RotatingFileHandler
    _final_level = getattr(logging, LOG_LEVEL, logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(_final_level)
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    root_logger.addHandler(file_handler)
except Exception as e:
    print(f"⚠️  File logging disabled – {e}", file=sys.stderr)


log_header("configuration")
# Mask & dump effective config ------------------------------------------------
for k, src in ENV_SOURCES.items():
    val = merged.get(k)
    if k in {"PLEX_TOKEN", "OPENAI_API_KEY", "DISCORD_WEBHOOK"} and val:
        val = val[:4] + "…"  # keep first 4 chars, mask the rest
    logging.info("Config %-15s = %-30s (source: %s)", k, val, src)

logging.info("Config CROSS_LIBRARY_DEDUPE = %s (source: %s)", CROSS_LIBRARY_DEDUPE, ENV_SOURCES.get("CROSS_LIBRARY_DEDUPE", "default"))
if CROSS_LIBRARY_DEDUPE:
    logging.info("➡️  Duplicate detection mode: cross-library (editions compared across ALL libraries)")
else:
    logging.info("➡️  Duplicate detection mode: per-library only (no cross-library comparisons)")

if _level_num == logging.DEBUG:
    scrubbed = {k: ("***" if k in {"PLEX_TOKEN", "OPENAI_API_KEY", "DISCORD_WEBHOOK"} else v)
                for k, v in merged.items()}
    logging.debug("Full merged config:\n%s", json.dumps(scrubbed, indent=2))

# (9) Initialise AI clients based on provider ----------------------------------------

openai_client = None
anthropic_client = None
google_client_configured = False
ollama_url = None
ai_provider_ready = False

if AI_PROVIDER.lower() == "openai":
    if OPENAI_API_KEY:
        os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
        try:
            openai_client = OpenAI()
            ai_provider_ready = True
            logging.info("OpenAI client initialized")
        except Exception as e:
            logging.warning("OpenAI client init failed: %s", e)
    else:
        logging.info("No OPENAI_API_KEY provided; AI-driven selection disabled.")
elif AI_PROVIDER.lower() == "anthropic":
    if ANTHROPIC_API_KEY and anthropic:
        try:
            anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            ai_provider_ready = True
            logging.info("Anthropic client initialized")
        except Exception as e:
            logging.warning("Anthropic client init failed: %s", e)
    else:
        if not anthropic:
            logging.warning("Anthropic SDK not installed. Install with: pip install anthropic")
        else:
            logging.info("No ANTHROPIC_API_KEY provided; AI-driven selection disabled.")
elif AI_PROVIDER.lower() == "google":
    if GOOGLE_API_KEY and genai:
        try:
            genai.configure(api_key=GOOGLE_API_KEY)
            google_client_configured = True
            ai_provider_ready = True
            logging.info("Google Gemini client configured")
        except Exception as e:
            logging.warning("Google client configuration failed: %s", e)
    else:
        if not genai:
            logging.warning("Google Generative AI SDK not installed. Install with: pip install google-generativeai")
        else:
            logging.info("No GOOGLE_API_KEY provided; AI-driven selection disabled.")
elif AI_PROVIDER.lower() == "ollama":
    if OLLAMA_URL:
        ollama_url = OLLAMA_URL.rstrip("/")
        # Test connection
        try:
            response = requests.get(f"{ollama_url}/api/tags", timeout=5)
            if response.status_code == 200:
                ai_provider_ready = True
                logging.info("Ollama connection verified at %s", ollama_url)
            else:
                logging.warning("Ollama not accessible at %s (HTTP %d)", ollama_url, response.status_code)
        except Exception as e:
            logging.warning("Ollama connection test failed: %s", e)
    else:
        logging.info("No OLLAMA_URL provided; AI-driven selection disabled.")
else:
    logging.warning("Unknown AI_PROVIDER: %s. Supported: openai, anthropic, google, ollama", AI_PROVIDER)


def _reinit_ai_from_globals():
    """Re-initialize AI clients from current module globals (after settings save). No restart needed."""
    global openai_client, anthropic_client, google_client_configured, ollama_url, ai_provider_ready
    global RESOLVED_MODEL, RESOLVED_PARAM_STYLE, RESOLVED_STOP_OK, AI_FUNCTIONAL_ERROR_MSG
    mod = sys.modules[__name__]
    provider = (getattr(mod, "AI_PROVIDER", "") or "openai").strip().lower()
    openai_key = getattr(mod, "OPENAI_API_KEY", "") or ""
    anthropic_key = getattr(mod, "ANTHROPIC_API_KEY", "") or ""
    google_key = getattr(mod, "GOOGLE_API_KEY", "") or ""
    ollama_u = (getattr(mod, "OLLAMA_URL", "") or "").strip().rstrip("/")
    openai_model = getattr(mod, "OPENAI_MODEL", "gpt-4") or "gpt-4"
    _merged = getattr(mod, "merged", None) or {}
    user_fallbacks = [m.strip() for m in (_merged.get("OPENAI_MODEL_FALLBACKS") or "").split(",") if m and m.strip()]

    openai_client = None
    anthropic_client = None
    google_client_configured = False
    ollama_url = None
    ai_provider_ready = False

    if provider == "openai":
        if openai_key:
            os.environ["OPENAI_API_KEY"] = openai_key
            try:
                openai_client = OpenAI()
                ai_provider_ready = True
                logging.info("OpenAI client re-initialized (settings applied)")
            except Exception as e:
                logging.warning("OpenAI client re-init failed: %s", e)
        if openai_client:
            compatible_set = set(getattr(mod, "OPENAI_COMPATIBLE_MODELS", []))
            reinit_candidates = []
            for ladder in getattr(mod, "MODEL_LADDERS", []):
                if openai_model in ladder:
                    start = ladder.index(openai_model)
                    reinit_candidates.extend(ladder[start:])
                    break
            if not reinit_candidates and openai_model and openai_model in compatible_set:
                reinit_candidates = [openai_model]
            for m in user_fallbacks:
                if m and m not in reinit_candidates and m in compatible_set:
                    reinit_candidates.append(m)
            for m in ("gpt-4o-mini",):
                if m not in reinit_candidates:
                    reinit_candidates.append(m)
            for cand in reinit_candidates:
                style = _probe_model(cand)
                if not style:
                    continue
                if not _probe_ai_choose_best_response(cand):
                    logging.warning("OpenAI model '%s' failed functional probe (empty or unparseable choose_best response); trying next candidate", cand)
                    continue
                RESOLVED_MODEL = cand
                RESOLVED_PARAM_STYLE = style
                AI_FUNCTIONAL_ERROR_MSG = None
                if RESOLVED_MODEL != openai_model:
                    logging.warning("OPENAI_MODEL '%s' unavailable or unsuitable; falling back to '%s' (%s, stop_ok=%s)", openai_model, RESOLVED_MODEL, RESOLVED_PARAM_STYLE, RESOLVED_STOP_OK)
                else:
                    logging.info("Using requested OpenAI model '%s' (%s, stop_ok=%s)", RESOLVED_MODEL, RESOLVED_PARAM_STYLE, RESOLVED_STOP_OK)
                break
            else:
                ai_provider_ready = False
                if getattr(sys.modules[__name__], "OPENAI_LAST_PROBE_401", False):
                    AI_FUNCTIONAL_ERROR_MSG = (
                        "OpenAI API key is invalid or expired (401 Unauthorized). "
                        "Check your key in Settings → AI and try again."
                    )
                else:
                    tried = ", ".join(reinit_candidates[:5])
                    if len(reinit_candidates) > 5:
                        tried += ", ..."
                    AI_FUNCTIONAL_ERROR_MSG = (
                        f"No OpenAI model returned a valid edition-choice response (tried: {tried}). "
                        "Models that return empty or unparseable output are rejected. Use a working model (e.g. gpt-4o-mini) in Settings."
                    )
                RESOLVED_MODEL = openai_model
                RESOLVED_PARAM_STYLE = "mct"
                RESOLVED_STOP_OK = True
                logging.warning("OpenAI model probe failed for all candidates; AI disabled. %s", AI_FUNCTIONAL_ERROR_MSG)
        else:
            logging.info("No OPENAI_API_KEY; AI-driven selection disabled.")
    elif provider == "anthropic":
        if anthropic_key and anthropic:
            try:
                anthropic_client = anthropic.Anthropic(api_key=anthropic_key)
                ai_provider_ready = True
                logging.info("Anthropic client re-initialized (settings applied)")
            except Exception as e:
                logging.warning("Anthropic client re-init failed: %s", e)
        else:
            logging.info("No ANTHROPIC_API_KEY; AI-driven selection disabled.")
    elif provider == "google":
        if google_key and genai:
            try:
                genai.configure(api_key=google_key)
                google_client_configured = True
                ai_provider_ready = True
                logging.info("Google Gemini client re-initialized (settings applied)")
            except Exception as e:
                logging.warning("Google client re-init failed: %s", e)
        else:
            logging.info("No GOOGLE_API_KEY; AI-driven selection disabled.")
    elif provider == "ollama":
        if ollama_u:
            ollama_url = ollama_u
            try:
                r = requests.get(f"{ollama_url}/api/tags", timeout=5)
                if r.status_code == 200:
                    ai_provider_ready = True
                    logging.info("Ollama re-verified at %s (settings applied)", ollama_url)
                else:
                    logging.warning("Ollama not accessible at %s (HTTP %d)", ollama_url, r.status_code)
            except Exception as e:
                logging.warning("Ollama re-check failed: %s", e)
        else:
            logging.info("No OLLAMA_URL; AI-driven selection disabled.")
    else:
        logging.warning("Unknown AI_PROVIDER: %s", provider)


def _reload_ai_config_and_reinit():
    """Load AI config from DB and run _reinit_ai_from_globals(). Used at scan start and preflight."""
    mod = sys.modules[__name__]
    ai_config_keys = ("AI_PROVIDER", "OPENAI_API_KEY", "OPENAI_MODEL", "OPENAI_MODEL_FALLBACKS",
                      "ANTHROPIC_API_KEY", "GOOGLE_API_KEY", "OLLAMA_URL")
    try:
        if STATE_DB_FILE.exists():
            con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
            cur = con.cursor()
            placeholders = ",".join("?" for _ in ai_config_keys)
            cur.execute(
                f"SELECT key, value FROM settings WHERE key IN ({placeholders})",
                ai_config_keys,
            )
            for key, value in cur.fetchall():
                setattr(mod, key, (value or ""))
            con.close()
        _reinit_ai_from_globals()
    except Exception as e:
        logging.warning("_reload_ai_config_and_reinit failed: %s", e)


# --- Resolve a working OpenAI model (with price-aware fallbacks) -------------
RESOLVED_MODEL = OPENAI_MODEL
# Param style for the resolved model: "mct" -> max_completion_tokens, "mt" -> max_tokens
RESOLVED_PARAM_STYLE = "mct"
# Some models (e.g. reasoning/o-series) do not accept "stop"; when False, call_ai_provider omits stop.
RESOLVED_STOP_OK = True
# When no OpenAI candidate passes probe, set by _reinit_ai_from_globals for 503 / preflight message.
AI_FUNCTIONAL_ERROR_MSG = None
# Set by _probe_model when last failure was 401 so we can show a clear message.
OPENAI_LAST_PROBE_401 = False

def _probe_model(model_name: str) -> str | None:
    """Return param style ("mct" or "mt") if a 1-line ping works, else None.
    Tries max_completion_tokens first, then max_tokens; each with stop.
    If both fail with unsupported_parameter, retries without stop (some models do not accept stop).
    Sets global RESOLVED_STOP_OK to False when the model only works without stop.
    """
    global RESOLVED_STOP_OK, OPENAI_LAST_PROBE_401
    OPENAI_LAST_PROBE_401 = False
    if not (OPENAI_API_KEY and openai_client):
        return None
    # Try with max_completion_tokens + stop
    try:
        openai_client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": "ping"}],
            max_completion_tokens=8,
            stop=["\n"],
        )
        RESOLVED_STOP_OK = True
        return "mct"
    except Exception as e:
        msg = str(e)
        if "401" in msg or "unauthorized" in msg.lower():
            OPENAI_LAST_PROBE_401 = True
        logging.debug("Model probe (mct+stop) failed for %s: %s", model_name, msg)
    # Try legacy max_tokens + stop
    try:
        openai_client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=8,
            stop=["\n"],
        )
        RESOLVED_STOP_OK = True
        return "mt"
    except Exception as e2:
        msg2 = str(e2)
        if "401" in msg2 or "unauthorized" in msg2.lower():
            OPENAI_LAST_PROBE_401 = True
        logging.debug("Model probe (mt+stop) failed for %s: %s", model_name, msg2)
    # If both failed with unsupported_parameter, try without stop (some reasoning models reject stop)
    if "unsupported_parameter" not in msg.lower() and "unsupported_parameter" not in msg2.lower():
        return None
    try:
        openai_client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": "ping"}],
            max_completion_tokens=8,
        )
        RESOLVED_STOP_OK = False
        logging.debug("Model probe: %s works with max_completion_tokens, without stop", model_name)
        return "mct"
    except Exception:
        pass
    try:
        openai_client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=8,
        )
        RESOLVED_STOP_OK = False
        logging.debug("Model probe: %s works with max_tokens, without stop", model_name)
        return "mt"
    except Exception:
        return None


def call_ai_provider(provider: str, model: str, system_msg: str, user_msg: str, max_tokens: int = 256) -> str:
    """
    Call the appropriate AI provider with the given messages.
    Returns the text response from the AI.
    For OpenAI, uses RESOLVED_PARAM_STYLE (mct or mt) and RESOLVED_STOP_OK; on 400 unsupported
    parameter, retries with the other param or without stop.
    """
    provider_lower = provider.lower()

    if provider_lower == "openai":
        if not openai_client:
            raise ValueError("OpenAI client not initialized")
        param_style = getattr(sys.modules[__name__], "RESOLVED_PARAM_STYLE", "mct")
        stop_ok = getattr(sys.modules[__name__], "RESOLVED_STOP_OK", True)
        _kwargs = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
        }
        if stop_ok:
            _kwargs["stop"] = ["\n"]
        if param_style == "mct":
            _kwargs["max_completion_tokens"] = max_tokens
        else:
            _kwargs["max_tokens"] = max_tokens
        try:
            resp = openai_client.chat.completions.create(**_kwargs)
            return (resp.choices[0].message.content or "").strip()
        except Exception as e:
            err_msg = str(e).lower()
            # On 400 unsupported parameter, retry with the other param or without stop
            if "unsupported_parameter" not in err_msg and "400" not in err_msg:
                raise
            if "max_tokens" in err_msg and "max_completion_tokens" in err_msg:
                _kwargs.pop("max_tokens", None)
                _kwargs["max_completion_tokens"] = max_tokens
                try:
                    resp = openai_client.chat.completions.create(**_kwargs)
                    return (resp.choices[0].message.content or "").strip()
                except Exception:
                    raise e
            if "max_completion_tokens" in err_msg and ("max_tokens" in err_msg or "use" in err_msg):
                _kwargs.pop("max_completion_tokens", None)
                _kwargs["max_tokens"] = max_tokens
                try:
                    resp = openai_client.chat.completions.create(**_kwargs)
                    return (resp.choices[0].message.content or "").strip()
                except Exception:
                    raise e
            if "stop" in err_msg or "unsupported" in err_msg:
                _kwargs.pop("stop", None)
                try:
                    resp = openai_client.chat.completions.create(**_kwargs)
                    return (resp.choices[0].message.content or "").strip()
                except Exception:
                    raise e
            raise

    elif provider_lower == "anthropic":
        if not anthropic_client:
            raise ValueError("Anthropic client not initialized")
        resp = anthropic_client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system_msg,
            messages=[{"role": "user", "content": user_msg}],
        )
        return resp.content[0].text.strip()

    elif provider_lower == "google":
        if not google_client_configured:
            raise ValueError("Google client not configured")
        model_instance = genai.GenerativeModel(model)
        prompt = f"{system_msg}\n\n{user_msg}"
        response = model_instance.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                max_output_tokens=max_tokens,
                stop_sequences=["\n"],
            ),
        )
        return response.text.strip()

    elif provider_lower == "ollama":
        if not ollama_url:
            raise ValueError("Ollama URL not configured")
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            "options": {
                "num_predict": max_tokens,
                "stop": ["\n"],
            },
            "stream": False,
        }
        response = requests.post(f"{ollama_url}/api/chat", json=payload, timeout=60)
        if response.status_code != 200:
            raise Exception(f"Ollama API error: {response.status_code} - {response.text}")
        result = response.json()
        return result.get("message", {}).get("content", "").strip()

    else:
        raise ValueError(f"Unknown AI provider: {provider}")


def call_ai_provider_vision(
    provider: str,
    model: str,
    system_msg: str,
    user_msg: str,
    image_urls: Optional[List[str]] = None,
    image_base64: Optional[List[dict]] = None,
    max_tokens: int = 32,
) -> str:
    """
    Call AI with optional images (vision). Used for cover comparison: "Do these two covers represent the same album? Yes/No."
    image_urls: list of image URLs (e.g. Cover Art Archive, or PMDA-served local cover).
    image_base64: list of {"type": "image_url", "image_url": {"url": "data:image/...;base64,..."}} or provider-specific.
    Returns the text response. Only OpenAI is supported for vision; other providers fall back to text-only.
    """
    provider_lower = provider.lower()
    if provider_lower != "openai" or not openai_client:
        # Fallback: call without images
        return call_ai_provider(provider, model, system_msg, user_msg, max_tokens=max_tokens)
    content: List[dict] = [{"type": "text", "text": user_msg}]
    if image_urls:
        for url in image_urls[:10]:
            if url:
                content.append({"type": "image_url", "image_url": {"url": url}})
    if image_base64:
        for img in image_base64[:10]:
            if isinstance(img, dict) and img.get("type") == "image_url" and img.get("image_url", {}).get("url"):
                content.append(img)
    param_style = getattr(sys.modules[__name__], "RESOLVED_PARAM_STYLE", "mct")
    stop_ok = getattr(sys.modules[__name__], "RESOLVED_STOP_OK", True)
    _kwargs = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": content},
        ],
    }
    if stop_ok:
        _kwargs["stop"] = ["\n"]
    if param_style == "mct":
        _kwargs["max_completion_tokens"] = max_tokens
    else:
        _kwargs["max_tokens"] = max_tokens
    try:
        resp = openai_client.chat.completions.create(**_kwargs)
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        logging.debug("[AI Vision] OpenAI vision call failed: %s", e)
        raise


def ai_verify_mb_match(
    artist: str,
    title_raw: Optional[str],
    title_norm: str,
    track_titles: Optional[List[str]],
    track_count: int,
    candidates: List[tuple],
    has_cover: bool = False,
    extra_sources: Optional[List[dict]] = None,
) -> Optional[tuple]:
    """
    Ask the AI to pick which MusicBrainz candidate matches our album (or NONE).
    candidates: list of (rg, result_dict) where rg has 'title','id', result_dict has 'id','track_count'.
    extra_sources: optional list of {"source": "Discogs"|"Last.fm"|"Bandcamp", "title": ..., "artist": ...} for disambiguation.
    Returns (rg, result_dict) for the chosen candidate or None.
    """
    if not getattr(sys.modules[__name__], "USE_AI_FOR_MB_VERIFY", False):
        return None
    if not getattr(sys.modules[__name__], "ai_provider_ready", False):
        return None
    if not candidates:
        return None
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    choices = []
    for i, (rg, result_dict) in enumerate(candidates[:20]):
        tc = result_dict.get("track_count", "?")
        line = f"{letters[i]}: {rg.get('title', 'Unknown')} (id={rg.get('id', '')}, {tc} tracks)"
        mb_tracks = result_dict.get("track_titles")
        if mb_tracks:
            track_preview = ", ".join(mb_tracks[:15]) + ("..." if len(mb_tracks) > 15 else "")
            line += f" — tracks: [{track_preview}]"
        choices.append(line)
    track_list_str = ", ".join((track_titles or [])[:30]) if track_titles else "(none)"
    if track_titles and len(track_titles) > 30:
        track_list_str += ", ..."
    user_msg = (
        f"Our album: artist={artist!r}, title_raw={title_raw or title_norm!r}, normalized={title_norm!r}, "
        f"track_count={track_count}, track_titles=[{track_list_str}]. "
        f"Cover present: {has_cover}.\n\n"
        "MusicBrainz candidates:\n" + "\n".join(choices)
    )
    if extra_sources:
        other_lines = []
        for s in extra_sources:
            src = s.get("source", "?")
            t = s.get("title") or s.get("album") or "?"
            a = s.get("artist") or s.get("artist_name") or "?"
            other_lines.append(f"  {src}: {t!r} (artist: {a!r})")
        user_msg += "\n\nOther sources (for disambiguation):\n" + "\n".join(other_lines)
    user_msg += (
        "\n\nWhich candidate is the same release? Consider title variants (e.g. Volume I = volume i). "
        "Reply with only the letter (A, B, ...) or NONE if no candidate matches."
    )
    system_msg = "You reply with a single letter (A, B, C, ...) or the word NONE. No explanation."
    try:
        model = getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4")
        provider = getattr(sys.modules[__name__], "AI_PROVIDER", "openai")
        reply = call_ai_provider(provider, model, system_msg, user_msg, max_tokens=10)
        reply = (reply or "").strip().upper()
        if reply == "NONE":
            return None
        letter = reply[:1]
        idx = letters.find(letter)
        if 0 <= idx < len(candidates):
            logging.info("[MusicBrainz Verify] AI selected candidate %s: %s", letter, candidates[idx][0].get("title"))
            return candidates[idx]
        return None
    except Exception as e:
        logging.debug("[MusicBrainz Verify] AI verify failed: %s", e)
        return None


def _probe_ai_choose_best_response(model_name: str) -> bool:
    """
    Verify the model returns a parseable choose_best-style response (index|rationale|extras).
    If the model returns empty or unparseable output, scan would show Heuristic fallbacks.
    Return True only if we get a valid response so we block scan when AI would fail.
    """
    if not (OPENAI_API_KEY and openai_client and AI_PROVIDER and getattr(sys.modules[__name__], "AI_PROVIDER", "").strip().lower() == "openai"):
        return True  # Skip functional probe for non-OpenAI
    system_msg = (
        "You are an expert digital-music librarian.\n"
        "OUTPUT RULES (must follow exactly):\n"
        "- Return ONE single line only.\n"
        "- The line must contain EXACTLY two '|' characters.\n"
        "- Format: <index>|<brief rationale>|<comma-separated extra tracks>\n"
        "- If there are no extra tracks, still include the final pipe but leave it empty.\n"
        "- Do not add any other text, do not explain, do not add extra lines.\n"
        "Example of valid outputs:\n"
        "0|Preferred lossless|"
    )
    user_msg = "Candidate editions:\n0: fmt_score=1, bd=24, tracks=10, size_mb=200\n1: fmt_score=0, bd=16, tracks=10, size_mb=100\n"
    try:
        txt = call_ai_provider(AI_PROVIDER, model_name, system_msg, user_msg, max_tokens=256)
        if not txt:
            logging.debug("Functional probe: model %s returned empty response", model_name)
            return False
        lines = [l.strip() for l in txt.replace("```", "").splitlines() if l.strip()]
        txt = lines[0] if lines else txt
        txt = re.sub(r"^(answer|réponse)\s*:\s*", "", txt, flags=re.IGNORECASE).strip()
        m = re.match(r"^(\d+)\s*\|\s*(.*?)\s*\|\s*(.*)$", txt)
        if m:
            idx = int(m.group(1))
            if 0 <= idx <= 1:
                logging.debug("Functional probe: model %s returned parseable response (index=%s)", model_name, idx)
                return True
        logging.debug("Functional probe: model %s response not parseable: %r", model_name, txt[:80])
        return False
    except Exception as e:
        logging.debug("Functional probe failed for %s: %s", model_name, e)
        return False


# User-provided explicit fallbacks override everything (comma-separated) (from SQLite only)
_user_fallbacks = [m.strip() for m in (merged.get("OPENAI_MODEL_FALLBACKS") or "").split(",") if m.strip()]

# Curated list of OpenAI Chat Completions models known to work with PMDA (parseable index|rationale|extras).
# Excludes models that return empty or use different API (e.g. gpt-5-nano). Only these are shown in Settings.
OPENAI_COMPATIBLE_MODELS = [
    "gpt-4o-mini",
    "gpt-4o",
    "gpt-4.1",
    "gpt-4.1-mini",
    "gpt-4.1-nano",
    "gpt-4-turbo",
    "gpt-4",
    "gpt-3.5-turbo",
]

# Price ladders (cheapest → most expensive) – only models from OPENAI_COMPATIBLE_MODELS (no gpt-5-nano etc.)
MODEL_LADDERS = [
    ["gpt-4.1-nano", "gpt-4.1-mini", "gpt-4.1"],
    ["gpt-4o-mini", "gpt-4o"],
    ["gpt-4-turbo", "gpt-4"],
    ["gpt-3.5-turbo"],
]

# Build candidate list: requested → next tiers upward in its ladder → user fallbacks (only if compatible) → safe default
candidates: list[str] = []
compatible_set = set(OPENAI_COMPATIBLE_MODELS)
found = False
for ladder in MODEL_LADDERS:
    if OPENAI_MODEL in ladder:
        found = True
        start = ladder.index(OPENAI_MODEL)
        candidates.extend(ladder[start:])
        break
if not found and OPENAI_MODEL and OPENAI_MODEL in compatible_set:
    candidates.append(OPENAI_MODEL)
for m in _user_fallbacks:
    if m not in candidates and m in compatible_set:
        candidates.append(m)
for m in ("gpt-4o-mini",):
    if m not in candidates:
        candidates.append(m)

for cand in candidates:
    style = _probe_model(cand)
    if style and _probe_ai_choose_best_response(cand):
        RESOLVED_MODEL = cand
        RESOLVED_PARAM_STYLE = style
        if RESOLVED_MODEL != OPENAI_MODEL:
            logging.warning("OPENAI_MODEL '%s' unavailable or unsuitable; falling back to '%s' (%s)", OPENAI_MODEL, RESOLVED_MODEL, RESOLVED_PARAM_STYLE)
        else:
            logging.info("Using requested OpenAI model '%s' (%s)", RESOLVED_MODEL, RESOLVED_PARAM_STYLE)
        break
else:
    # No candidate passed both param probe and functional (choose_best) probe
    if openai_client and candidates:
        ai_provider_ready = False
        tried = ", ".join(candidates[:5])
        if len(candidates) > 5:
            tried += ", ..."
        AI_FUNCTIONAL_ERROR_MSG = (
            f"No OpenAI model returned a valid edition-choice response (tried: {tried}). "
            "Use a working model (e.g. gpt-4o-mini) in Settings."
        )
        logging.warning("OpenAI: all candidates failed functional probe; AI disabled. %s", AI_FUNCTIONAL_ERROR_MSG)

# (10) Validate Plex connection ------------------------------------------------
# (10) Validate Plex connection ------------------------------------------------
def _validate_plex_connection():
    """
    Perform a lightweight request to Plex `/library/sections` to make sure the
    server is reachable and the token is valid.  We only warn on failure so
    the application can still run in offline mode.
    """
    host = (PLEX_HOST or "").strip()
    if not host.startswith(("http://", "https://")):
        logging.debug("Skipping Plex connection check (no valid PLEX_HOST)")
        return
    url = f"{host.rstrip('/')}/library/sections"
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
    log_header("self diagnostic")
    had_warning = False

    # 1) Plex DB readable?
    try:
        db = plex_connect()
        db.execute("SELECT 1").fetchone()
        logging.info("✓ Plex DB reachable (%s)", PLEX_DB_FILE)
    except Exception as e:
        logging.error("✗ Plex DB ERROR – %s", e)
        return False

    # 0) /dupes sanity  (warn but do NOT hard‑fail)
    if not (DUPE_ROOT.exists() and os.access(DUPE_ROOT, os.W_OK)):
        warn = ("⚠ /dupes is missing or read‑only – PMDA can't move duplicates.\n"
                "👉 Please bind‑mount a writable host folder, e.g.  -v /path/on/host:/dupes")
        logging.warning(warn)
        notify_discord(warn)
        had_warning = True

    # Compute exact album counts for each PATH_MAP prefix (no sampling)
    prefix_stats: dict[str, int] = {}
    for pre in PATH_MAP:
        cnt = db.execute(
            """
            SELECT COUNT(DISTINCT alb.id)
            FROM   media_parts      mp
            JOIN   media_items      mi  ON mi.id       = mp.media_item_id
            JOIN   metadata_items   tr  ON tr.id       = mi.metadata_item_id   -- track (type 10)
            JOIN   metadata_items   alb ON alb.id      = tr.parent_id          -- album (type 9)
            WHERE  mp.file LIKE ?
            """,
            (f"{pre}/%",)
        ).fetchone()[0]
        prefix_stats[pre] = cnt

    for pre, dest in PATH_MAP.items():
        albums_seen = prefix_stats.get(pre, 0)
        if albums_seen == 0:
            logging.warning("%s %s → %s  (prefix not found in DB)",
                            colour('⚠', ANSI_YELLOW), pre, dest)
            had_warning = True
        elif not Path(dest).exists():
            logging.error("✗ %s → %s  (host path missing)", pre, dest)
            return False
        else:
            logging.info("%s %s → %s  (%d albums)",
                         colour('✓', ANSI_GREEN), pre, dest, albums_seen)

    # 3) Permission checks
    for mount in [*PATH_MAP.values(), str(DUPE_ROOT), str(CONFIG_DIR)]:
        p = Path(mount)
        if not p.exists():
            continue
        rw = ("r" if os.access(p, os.R_OK) else "-") + \
             ("w" if os.access(p, os.W_OK) else "-")
        if rw != "rw":
            logging.warning("⚠ %s permissions: %s", p, rw)
            had_warning = True
        else:
            logging.info("✓ %s permissions: %s", p, rw)

    # 4) Albums with no mapping (skip if no PATH_MAP entries)
    if PATH_MAP:
        # Restrict the "un‑mapped" check to the chosen MUSIC section(s) only
        where_clauses = " AND ".join(f"mp.file NOT LIKE '{pre}%'" for pre in PATH_MAP)
        placeholders = ",".join("?" for _ in SECTION_IDS)
        query = f"""
            SELECT COUNT(*)
            FROM   media_parts  mp
            JOIN   metadata_items md ON md.id = mp.media_item_id
            WHERE  md.library_section_id IN ({placeholders})
              AND  md.metadata_type     = 9          -- 9 = album
              AND  {where_clauses}
        """
        unmapped = db.execute(query, SECTION_IDS).fetchone()[0]
        if unmapped:
            logging.warning(
                "⚠ %d albums have no PATH_MAP match; this is not necessarily an error. "
                "these albums may belong to Plex library sections you haven't included. "
                "to avoid this warning, set SECTION_IDS to include all relevant section IDs, separated by commas.",
                unmapped
            )
            had_warning = True
    else:
        logging.info("Skipping unmapped album check because PATH_MAP is empty")

    # 5) External service checks -------------------------------------------------
    openai_ok = False
    discord_ok = False

    # --- OpenAI key -------------------------------------------------------------
    if OPENAI_API_KEY and openai_client:
        try:
            # a 1‑token "ping" to verify the key / model combination works
            _kwargs = {
                "model": RESOLVED_MODEL,
                "messages": [{"role": "user", "content": "ping"}],
                "stop": ["\n"],
            }
            if RESOLVED_PARAM_STYLE == "mct":
                _kwargs["max_completion_tokens"] = 8
            else:
                _kwargs["max_tokens"] = 8
            openai_client.chat.completions.create(**_kwargs)
            logging.info("%s OpenAI API key valid – model **%s** reachable",
                         colour("✓", ANSI_GREEN), RESOLVED_MODEL)
            openai_ok = True
        except Exception as e:
            logging.warning("%s OpenAI API key present but failed: %s",
                            colour("⚠", ANSI_YELLOW), e)
    else:
        logging.info("• No OPENAI_API_KEY provided; AI features disabled.")

    # --- Discord webhook --------------------------------------------------------
    if DISCORD_WEBHOOK:
        try:
            resp = requests.post(DISCORD_WEBHOOK, json={"content": "🔔 PMDA startup…"}, timeout=6)
            if resp.status_code == 204:
                logging.info("%s Discord webhook reachable",
                             colour("✓", ANSI_GREEN))
                discord_ok = True
            else:
                logging.warning("%s Discord webhook returned HTTP %s",
                                colour("⚠", ANSI_YELLOW), resp.status_code)
        except Exception as e:
            logging.warning("%s Discord webhook test failed: %s",
                            colour("⚠", ANSI_YELLOW), e)
    else:
        logging.info("• No DISCORD_WEBHOOK configured.")

    # --- MusicBrainz connectivity check --------------------------------------
    if not USE_MUSICBRAINZ:
        logging.info("• Skipping MusicBrainz connectivity check (USE_MUSICBRAINZ=False).")
    else:
        try:
            # Test with a real API call using musicbrainzngs (respects rate limiting)
            # Use a well-known release-group ID for testing
            test_mbid = "9162580e-5df4-32de-80cc-f45a8d8a9b1d"  # The Beatles - Abbey Road
            result = musicbrainzngs.get_release_group_by_id(test_mbid, includes=[])
            logging.info("✓ MusicBrainz reachable and working – tested with release-group %s", test_mbid)
            logging.info("✓ MusicBrainz configured with email: %s", MUSICBRAINZ_EMAIL)
        except musicbrainzngs.WebServiceError as e:
            if "503" in str(e) or "rate" in str(e).lower():
                logging.warning("⚠️ MusicBrainz rate limited – ensure rate limiting is configured (1 req/sec)")
            else:
                logging.warning("⚠️ MusicBrainz API error – %s", e)
        except Exception as e:
            logging.warning("⚠️ MusicBrainz connectivity failed – %s", e)

    # ---------------------------------------------------------------------------
    # ─── Build a richer Discord embed ------------------------------------
    if discord_ok:
        bindings_txt = "\n".join(
            f"`{src}` → `{dst}`"
            for src, dst in PATH_MAP.items()
        )

        albums_txt = "\n".join(
            f"{src}: **{cnt}**"
            for src, cnt in prefix_stats.items()
        )

        fields = [
            {
                "name": "Libraries",
                "value": ", ".join(SECTION_NAMES.get(i, str(i)) for i in SECTION_IDS),
                "inline": False,
            },
            {
                "name": "Volume bindings",
                "value": bindings_txt or "n/a",
                "inline": False,
            },
            {
                "name": "Albums per bind",
                "value": albums_txt or "n/a",
                "inline": False,
            },
            {
                "name": "OpenAI",
                "value": "✅ working" if openai_ok else "❌ disabled / error",
                "inline": True,
            },
            {
                "name": "Discord",
                "value": "✅ webhook OK" if discord_ok else "❌ not configured",
                "inline": True,
            },
        ]

        notify_discord_embed(
            title="🟢 PMDA started",
            description="All folder mappings look good – ready to scan!",
            fields=fields
        )

    logging.info("──────── diagnostic complete ─────────")
    if not had_warning:
        logging.info("%s ALL mapped folders contain albums – ALL GOOD!", colour("✓", ANSI_GREEN))
    # ─── Log AI prompt for user review ─────────────────────────────────
    try:
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            prompt_text = AI_PROMPT_FILE.read_text(encoding="utf-8")
            logging.debug("Using ai_prompt.txt:\n%s", prompt_text)
    except Exception as e:
        logging.warning("Could not read ai_prompt.txt: %s", e)
    return True


# SQL fragment used by both startup cross-check and /api/paths/verify (same logic)
_PATH_VERIFY_EXTENSIONS = (
    "mp.file LIKE '%.flac' OR mp.file LIKE '%.wav' OR mp.file LIKE '%.m4a' OR mp.file LIKE '%.mp3'"
    " OR mp.file LIKE '%.ogg' OR mp.file LIKE '%.opus' OR mp.file LIKE '%.aac' OR mp.file LIKE '%.ape' OR mp.file LIKE '%.alac'"
    " OR mp.file LIKE '%.dsf' OR mp.file LIKE '%.aif' OR mp.file LIKE '%.aiff' OR mp.file LIKE '%.wma'"
    " OR mp.file LIKE '%.mp4' OR mp.file LIKE '%.m4b' OR mp.file LIKE '%.m4p' OR mp.file LIKE '%.aifc'"
)


def _run_path_verification(path_map: dict, db_file: str, samples: int):
    """
    Same logic as startup cross-check: for each PATH_MAP entry, sample audio files from Plex DB
    and verify they exist on disk (container paths). Returns list of result dicts for API.
    If 1 or 2 samples are missing (e.g. one file moved or encoding glitch), retry once with
    a new random sample for that root to avoid false failures. Does not modify config or global state.
    """
    if not Path(db_file).exists():
        return None
    results = []
    try:
        con = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True, timeout=10)
        con.text_factory = lambda b: b.decode("utf-8", "surrogateescape")
        cur = con.cursor()
        for plex_root, host_root in path_map.items():
            try:
                def check_once():
                    cur.execute(
                        f"""
                        SELECT mp.file FROM media_parts mp
                        WHERE mp.file LIKE ? AND ({_PATH_VERIFY_EXTENSIONS})
                        ORDER BY RANDOM() LIMIT ?
                        """,
                        (f"{plex_root}/%", samples),
                    )
                    rows = [r[0] for r in cur.fetchall()]
                    if not rows:
                        return 0, 0, None
                    missing = 0
                    for src_path in rows:
                        rel = src_path[len(plex_root):].lstrip("/")
                        dst_path = os.path.join(host_root, rel)
                        if not Path(dst_path).exists():
                            missing += 1
                    return len(rows), missing, rows
                total, missing, _ = check_once()
                if total == 0:
                    # Path may be valid (Plex uses it) but this DB has no rows (e.g. different Plex server).
                    # If the path exists in the container and has audio files, treat as OK.
                    try:
                        p = Path(host_root)
                        if p.exists() and p.is_dir():
                            audio_count = sum(1 for _ in p.rglob("*") if AUDIO_RE.search(_.name))
                            if audio_count > 0:
                                results.append({
                                    "plex_root": plex_root,
                                    "host_root": host_root,
                                    "status": "ok",
                                    "samples_checked": 0,
                                    "message": "Path exists with audio files (no matching rows in this Plex DB)",
                                })
                                continue
                    except Exception:
                        pass
                    results.append({
                        "plex_root": plex_root,
                        "host_root": host_root,
                        "status": "fail",
                        "samples_checked": 0,
                        "message": "No audio files found in DB under this path",
                    })
                    continue
                if missing > 0 and missing <= 2:
                    total2, missing2, _ = check_once()
                    if total2 > 0 and missing2 == 0:
                        total, missing = total2, 0
                if missing == 0:
                    results.append({
                        "plex_root": plex_root,
                        "host_root": host_root,
                        "status": "ok",
                        "samples_checked": total,
                        "message": "OK",
                    })
                else:
                    results.append({
                        "plex_root": plex_root,
                        "host_root": host_root,
                        "status": "fail",
                        "samples_checked": total,
                        "message": f"{missing}/{total} sample(s) missing on disk",
                    })
            except Exception as path_err:
                logging.warning("Path verification failed for %s: %s", plex_root, path_err)
                results.append({
                    "plex_root": plex_root,
                    "host_root": host_root,
                    "status": "fail",
                    "samples_checked": 0,
                    "message": str(path_err) or "Verification error",
                })
        con.close()
    except Exception as e:
        logging.warning("Path verification failed: %s", e)
        return None
    return results


def _discover_bindings_by_content(path_map: dict, db_file: str, music_root: str, samples: int):
    """
    For each plex_root in path_map, sample audio paths from Plex DB and find which subdir of
    music_root actually contains those files (content-based match, ignores folder names).
    Returns (discovered_map, results) where discovered_map is plex_root -> resolved host_root
    and results is a list of { plex_root, host_root, status, samples_checked, message }.
    Only adds to discovered_map when at least one file is found for that plex_root.
    """
    if not path_map:
        return {}, []
    music_path = Path(music_root)
    if not music_path.exists() or not music_path.is_dir():
        return None
    if not Path(db_file).exists():
        return None
    discovered_map = {}
    results = []
    try:
        con = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True, timeout=10)
        con.text_factory = lambda b: b.decode("utf-8", "surrogateescape")
        cur = con.cursor()
        candidates_cache = None

        for plex_root in path_map:
            cur.execute(
                f"""
                SELECT mp.file FROM media_parts mp
                WHERE mp.file LIKE ? AND ({_PATH_VERIFY_EXTENSIONS})
                ORDER BY RANDOM() LIMIT ?
                """,
                (f"{plex_root}/%", max(1, samples)),
            )
            rows = [r[0] for r in cur.fetchall()]
            if not rows:
                results.append({
                    "plex_root": plex_root,
                    "host_root": path_map[plex_root],
                    "status": "fail",
                    "samples_checked": 0,
                    "message": "No audio files found in DB under this path",
                })
                continue
            rels = [r[len(plex_root):].lstrip("/") for r in rows]
            if candidates_cache is None:
                candidates_cache = sorted([d for d in music_path.iterdir() if d.is_dir()], key=lambda p: str(p))
            candidates = candidates_cache

            best_path = None
            best_count = 0
            total = len(rels)
            for cand in candidates:
                count = 0
                for rel in rels:
                    if (cand / rel).exists():
                        count += 1
                if count > best_count:
                    best_count = count
                    best_path = str(cand)
                    if best_count == total:
                        break
            if best_count == 0:
                results.append({
                    "plex_root": plex_root,
                    "host_root": path_map[plex_root],
                    "status": "fail",
                    "samples_checked": total,
                    "message": "No matching folder under music root",
                })
                continue
            discovered_map[plex_root] = best_path
            status = "ok" if best_count == total else "fail"
            msg = "OK" if best_count == total else f"{best_count}/{total} files found"
            results.append({
                "plex_root": plex_root,
                "host_root": best_path,
                "status": status,
                "samples_checked": total,
                "message": msg,
            })
        con.close()
    except Exception as e:
        logging.warning("Discover bindings by content failed: %s", e)
        return None
    return (discovered_map, results)


def _discover_one_binding(plex_root: str, db_file: str, music_root: str, samples: int):
    """
    Resolve a single plex_root: find which subdir of music_root contains the sampled files.
    Returns (host_root or None, result_dict) for API. Returns None if DB or music_root invalid.
    Always uses DB + content-based discovery so the real folder (e.g. correct case like
    /music/Compilations) is found even when a wrong/empty folder exists (e.g. /music/compilations).
    """
    music_path = Path(music_root)
    if not music_path.exists() or not music_path.is_dir():
        return None
    if not Path(db_file).exists():
        return None
    try:
        con = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True, timeout=10)
        con.text_factory = lambda b: b.decode("utf-8", "surrogateescape")
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT mp.file FROM media_parts mp
            WHERE mp.file LIKE ? AND ({_PATH_VERIFY_EXTENSIONS})
            ORDER BY RANDOM() LIMIT ?
            """,
            (f"{plex_root}/%", max(1, samples)),
        )
        rows = [r[0] for r in cur.fetchall()]
        con.close()
    except Exception as e:
        logging.warning("Discover one failed for %s: %s", plex_root, e)
        return None
    if not rows:
        return (None, {
            "plex_root": plex_root,
            "host_root": plex_root,
            "status": "fail",
            "samples_checked": 0,
            "message": "No audio files found in DB under this path",
        })
    rels = [r[len(plex_root):].lstrip("/") for r in rows]
    candidates = sorted([d for d in music_path.iterdir() if d.is_dir()], key=lambda p: str(p))
    best_path = None
    best_count = 0
    total = len(rels)
    for cand in candidates:
        count = sum(1 for rel in rels if (cand / rel).exists())
        if count > best_count:
            best_count = count
            best_path = str(cand)
            if best_count == total:
                break
    if best_count == 0:
        # Fallback: recursive search by filename (same idea as _cross_check_bindings repair)
        logging.debug("Discover one: no match in immediate children of %s – trying recursive search", music_root)
        candidate_counts: dict[str, int] = {}
        for _, rel in enumerate(rels):
            fname = os.path.basename(rel)
            try:
                for found in music_path.rglob(fname):
                    # Infer host root: go up as many levels as rel has parts
                    root = found
                    for _ in Path(rel).parts:
                        root = root.parent
                    root_str = str(root)
                    candidate_counts[root_str] = candidate_counts.get(root_str, 0) + 1
            except OSError as e:
                logging.debug("Discover one rglob for %s: %s", fname, e)
        if candidate_counts:
            best_path, best_count = max(candidate_counts.items(), key=lambda kv: kv[1])
            logging.info("Discover one: recursive search found best root %s with %d/%d matches", best_path, best_count, total)
        if best_count == 0:
            return (None, {
                "plex_root": plex_root,
                "host_root": plex_root,
                "status": "fail",
                "samples_checked": total,
                "message": "No matching folder under music root",
            })
    status = "ok" if best_count == total else "fail"
    msg = "OK" if best_count == total else f"{best_count}/{total} files found"
    return (best_path, {
        "plex_root": plex_root,
        "host_root": best_path,
        "status": status,
        "samples_checked": total,
        "message": msg,
    })


# ──────────────────────────────── CROSS‑CHECK PATH BINDINGS ────────────────────────────────
def _cross_check_bindings(raise_on_abort: bool = True):
    """
    Verify that every PATH_MAP binding actually resolves to real audio
    files on the host.

    • Randomly samples CROSSCHECK_SAMPLES audio files per Plex root.
    • If all samples exist under the mapped host root, the binding is valid.
    • Otherwise performs a recursive search to locate the files; if they are
      all found under the same parent folder we treat that folder as the
      correct host root and patch PATH_MAP (memory + SQLite).
    • If any binding cannot be validated or repaired and raise_on_abort is True,
      raises SystemExit. When raise_on_abort is False (e.g. background thread),
      logs the error and returns without raising.
    """
    log_header("cross‑check path bindings")

    con = plex_connect()
    cur = con.cursor()
    updates: dict[str, str] = {}
    abort = False

    for plex_root, host_root in PATH_MAP.items():
        # 1) Pull a random sample of audio files (same SQL as _run_path_verification / api paths verify)
        cur.execute(
            f'''
            SELECT mp.file FROM media_parts mp
            WHERE mp.file LIKE ? AND ({_PATH_VERIFY_EXTENSIONS})
            ORDER BY RANDOM() LIMIT ?
            ''',
            (f"{plex_root}/%", CROSSCHECK_SAMPLES)
        )
        rows = [r[0] for r in cur.fetchall()]

        target = len(rows)
        if target == 0:
            logging.error("No audio samples found under %s – cannot validate this binding.", plex_root)
            abort = True
            continue
        if target < CROSSCHECK_SAMPLES:
            logging.warning("PATH CHECK: only %d/%d samples available under %s – proceeding with reduced target.", target, CROSSCHECK_SAMPLES, plex_root)

        # 2) Direct existence test
        missing: list[tuple[str, str]] = []
        for src_path in rows:
            rel = src_path[len(plex_root):].lstrip("/")
            dst_path = os.path.join(host_root, rel)
            try:
                exists = Path(dst_path).exists()
            except OSError as e:
                logging.warning("PATH CHECK: I/O error checking %s – skipping sample: %s", dst_path, e)
                continue
            if not exists:
                missing.append((src_path, rel))

        if not missing:
            logging.info("✓ Binding verified: %s → %s", plex_root, host_root)
            continue

        # Diagnostic: show a few concrete sample mappings that failed
        try:
            for i, (src_path, rel) in enumerate(missing[:5], 1):
                example_dst = os.path.join(host_root, rel)
                logging.debug(
                    "PATH CHECK example %d/%d: src=%s | rel=%s | dst=%s | exists(dst)=%s",
                    i, len(missing), src_path, rel, example_dst, os.path.exists(example_dst)
                )
        except Exception as diag_e:
            logging.debug("PATH CHECK example logging failed: %s", diag_e)

        logging.warning("Binding failed for %s → %s – attempting recursive search…", plex_root, host_root)

        # 3) Guess candidate roots by scanning *immediate* children of the search base
        search_base = Path(host_root).parent           # normally '/music'
        if not search_base.exists():
            search_base = Path("/")                    # last‑chance fallback
        candidate_roots = [d for d in search_base.iterdir() if d.is_dir()]
        if not candidate_roots:
            logging.debug("No sub‑directories under %s – cannot guess roots", search_base)
        candidate_counts: dict[str, int] = {}
        for cand in candidate_roots:
            logging.debug("→ checking %s …", cand)
            ok = 0
            missing_target = len(missing)
            for idx, (_, rel) in enumerate(missing, 1):
                dst = cand / rel
                if dst.exists():
                    ok += 1
                    logging.debug("   %2d/%d matches so far (%s)", ok, missing_target, rel if ok <= 3 else "…")
                # early‑exit: all samples matched
                if ok == missing_target:
                    break
            if ok:
                logging.info("   %s → %d/%d samples matched", cand, ok, missing_target)
                candidate_counts[str(cand)] = ok
            else:
                logging.debug("   %s → 0 matches", cand)
        # Remove unintended reassignment of target here (do not force target = CROSSCHECK_SAMPLES)
        # ─── fallback: deep scan when nothing found above ─────────────────────
        if not candidate_counts:
            logging.debug("Immediate child scan found nothing – performing deep filename search")
            missing_target = len(missing)
            for _, rel in missing:
                fname = os.path.basename(rel)
                for found in search_base.rglob(fname):
                    root = found
                    for _ in Path(rel).parts:
                        root = root.parent
                    root = str(root)
                    cnt = candidate_counts.get(root, 0) + 1
                    candidate_counts[root] = cnt
                    if cnt == max(1, missing_target // 2):
                        logging.info("   halfway there: %d/%d samples align under %s", cnt, missing_target, root)

        best_root, matched = (None, 0)
        if candidate_counts:
            best_root, matched = max(candidate_counts.items(), key=lambda kv: kv[1])
        logging.debug("Candidate roots and match counts: %s", candidate_counts)
        missing_target = len(missing)
        if matched:
            logging.info("Best candidate %s matched %d/%d samples", best_root, matched, missing_target)
        if matched == missing_target:
            updates[plex_root] = best_root
            logging.info("Resolved new host root for %s: %s", plex_root, best_root)
        elif matched > 0:
            logging.info("Partial match: %d/%d samples align under %s", matched, missing_target, best_root or "<none>")
        else:
            logging.error("Could not validate binding for %s – matched %d/%d samples", plex_root, matched, missing_target)
            abort = True

    con.close()

    if abort:
        notify_discord("❌ PMDA startup aborted: PATH_MAP bindings failed cross‑check.")
        if raise_on_abort:
            raise SystemExit("Cross‑check PATH_MAP failed")
        logging.error("PATH_MAP cross-check failed (running in background – check bindings in Settings).")
        return

    # Apply fixes
    if updates:
        PATH_MAP.update(updates)
        try:
            con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
            con.execute("INSERT OR REPLACE INTO settings(key, value) VALUES('PATH_MAP', ?)", (json.dumps(dict(PATH_MAP)),))
            con.commit()
            con.close()
        except Exception as e:
            logging.debug("Could not persist PATH_MAP to SQLite after cross-check: %s", e)
        msg = "\n".join(f"`{k}` → `{v}`" for k, v in updates.items())
        notify_discord_embed(
            title="🔄 PATH_MAP corrected",
            description=msg
        )
        logging.info("Updated PATH_MAP in memory and SQLite")

    logging.info("All PATH_MAP bindings verified OK.")


# ───────────────────────────────── OTHER CONSTANTS ──────────────────────────────────
AUDIO_RE    = re.compile(r"\.(flac|ape|alac|wav|m4a|aac|mp3|ogg|opus|dsf|aif|aiff|wma|mp4|m4b|m4p|aifc)$", re.I)
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
    # Table for duplicate "best" entries
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
    # Extend schema: add meta_json if missing
    cur.execute("PRAGMA table_info(duplicates_best)")
    cols = [r[1] for r in cur.fetchall()]
    if "meta_json" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN meta_json TEXT")
    if "ai_provider" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN ai_provider TEXT")
    if "ai_model" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN ai_model TEXT")
    if "size_mb" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN size_mb INTEGER")
    if "track_count" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN track_count INTEGER")
    if "match_verified_by_ai" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN match_verified_by_ai INTEGER DEFAULT 0")
    # Add indexes for faster lookups
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_duplicates_best_artist ON duplicates_best(artist)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_duplicates_best_album_id ON duplicates_best(album_id)")
    except sqlite3.OperationalError:
        pass
    
    # Table for broken albums (missing tracks)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS broken_albums (
            artist TEXT,
            album_id INTEGER,
            expected_track_count INTEGER,
            actual_track_count INTEGER,
            missing_indices TEXT,
            musicbrainz_release_group_id TEXT,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fixed_at TIMESTAMP,
            sent_to_lidarr BOOLEAN DEFAULT 0,
            PRIMARY KEY (artist, album_id)
        )
    """)
    # Table for monitored artists in Lidarr
    cur.execute("""
        CREATE TABLE IF NOT EXISTS monitored_artists (
            artist_id INTEGER PRIMARY KEY,
            artist_name TEXT,
            musicbrainz_artist_id TEXT,
            lidarr_artist_id INTEGER,
            monitored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(artist_id)
        )
    """)
    # Table for duplicate "loser" entries
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
    # Add indexes for faster lookups
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_duplicates_loser_artist_album ON duplicates_loser(artist, album_id)")
    except sqlite3.OperationalError:
        pass
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
    # Table for persistent settings (wizard configuration)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    # Table for scan history
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_history (
            scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time REAL NOT NULL,
            end_time REAL,
            duration_seconds INTEGER,
            albums_scanned INTEGER DEFAULT 0,
            duplicates_found INTEGER DEFAULT 0,
            artists_processed INTEGER DEFAULT 0,
            artists_total INTEGER DEFAULT 0,
            ai_used_count INTEGER DEFAULT 0,
            mb_used_count INTEGER DEFAULT 0,
            ai_enabled INTEGER DEFAULT 0,
            mb_enabled INTEGER DEFAULT 0,
            auto_move_enabled INTEGER DEFAULT 0,
            space_saved_mb INTEGER DEFAULT 0,
            albums_moved INTEGER DEFAULT 0,
            status TEXT DEFAULT 'completed',
            duplicate_groups_count INTEGER DEFAULT 0,
            total_duplicates_count INTEGER DEFAULT 0,
            broken_albums_count INTEGER DEFAULT 0,
            missing_albums_count INTEGER DEFAULT 0,
            albums_without_artist_image INTEGER DEFAULT 0,
            albums_without_album_image INTEGER DEFAULT 0,
            albums_without_complete_tags INTEGER DEFAULT 0,
            albums_without_mb_id INTEGER DEFAULT 0,
            albums_without_artist_mb_id INTEGER DEFAULT 0
        )
    """)
    # Add index for faster scan history queries
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_history_start_time ON scan_history(start_time DESC)")
    except sqlite3.OperationalError:
        pass
    # Add new columns if they don't exist (migration for existing databases)
    cur.execute("PRAGMA table_info(scan_history)")
    cols = [r[1] for r in cur.fetchall()]
    new_cols = [
        ("duplicate_groups_count", "INTEGER DEFAULT 0"),
        ("total_duplicates_count", "INTEGER DEFAULT 0"),
        ("broken_albums_count", "INTEGER DEFAULT 0"),
        ("missing_albums_count", "INTEGER DEFAULT 0"),
        ("albums_without_artist_image", "INTEGER DEFAULT 0"),
        ("albums_without_album_image", "INTEGER DEFAULT 0"),
        ("albums_without_complete_tags", "INTEGER DEFAULT 0"),
        ("albums_without_mb_id", "INTEGER DEFAULT 0"),
        ("albums_without_artist_mb_id", "INTEGER DEFAULT 0"),
        ("summary_json", "TEXT"),
        ("entry_type", "TEXT DEFAULT 'scan'"),
    ]
    for col_name, col_type in new_cols:
        if col_name not in cols:
            cur.execute(f"ALTER TABLE scan_history ADD COLUMN {col_name} {col_type}")
    # Table for scan moves (tracking file movements)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_moves (
            move_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id INTEGER NOT NULL,
            artist TEXT NOT NULL,
            album_id INTEGER NOT NULL,
            original_path TEXT NOT NULL,
            moved_to_path TEXT NOT NULL,
            size_mb INTEGER,
            moved_at REAL NOT NULL,
            restored INTEGER DEFAULT 0,
            FOREIGN KEY (scan_id) REFERENCES scan_history(scan_id)
        )
    """)
    # Migrate scan_moves: add album_title and fmt_text if missing
    cur.execute("PRAGMA table_info(scan_moves)")
    move_cols = [r[1] for r in cur.fetchall()]
    for col_name, col_type in [("album_title", "TEXT"), ("fmt_text", "TEXT")]:
        if col_name not in move_cols:
            cur.execute(f"ALTER TABLE scan_moves ADD COLUMN {col_name} {col_type}")
    # Table for per-edition scan truth (Library, Tag Fixer read from here when available)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_editions (
            scan_id INTEGER NOT NULL,
            artist TEXT NOT NULL,
            album_id INTEGER NOT NULL,
            title_raw TEXT,
            folder TEXT,
            fmt_text TEXT,
            br INTEGER,
            sr INTEGER,
            bd INTEGER,
            meta_json TEXT,
            musicbrainz_id TEXT,
            is_broken INTEGER DEFAULT 0,
            expected_track_count INTEGER,
            actual_track_count INTEGER,
            missing_indices TEXT,
            has_cover INTEGER DEFAULT 0,
            missing_required_tags TEXT,
            PRIMARY KEY (scan_id, artist, album_id),
            FOREIGN KEY (scan_id) REFERENCES scan_history(scan_id)
        )
    """)
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
    """Atomically add *delta* to a stat counter. Creates the row if it does not exist (upsert)."""
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    con.execute("PRAGMA busy_timeout=30000;")
    cur = con.cursor()
    cur.execute(
        "INSERT INTO stats(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = value + ?",
        (key, delta, delta),
    )
    con.commit()
    con.close()

def get_last_completed_scan_id() -> Optional[int]:
    """Return the scan_id of the last completed scan, or None. Used by Library and Tag Fixer to read from scan_editions."""
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute("SELECT value FROM settings WHERE key = 'last_completed_scan_id'")
    row = cur.fetchone()
    con.close()
    if not row or not row[0]:
        return None
    try:
        return int(row[0])
    except (ValueError, TypeError):
        return None


def ensure_dedupe_scan_id() -> None:
    """If state has no scan_id, create a 'dedupe' scan_history row so moves are recorded. Used when user dedupes without a prior scan."""
    with lock:
        if state.get("scan_id") is not None:
            return
        start_time = time.time()
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        cur.execute("PRAGMA table_info(scan_history)")
        cols = [r[1] for r in cur.fetchall()]
        if "entry_type" in cols:
            cur.execute("""
                INSERT INTO scan_history
                (start_time, albums_scanned, artists_total, ai_enabled, mb_enabled, auto_move_enabled, status, entry_type)
                VALUES (?, 0, 0, 0, 0, 0, 'running', 'dedupe')
            """, (start_time,))
        else:
            cur.execute("""
                INSERT INTO scan_history
                (start_time, albums_scanned, artists_total, ai_enabled, mb_enabled, auto_move_enabled, status)
                VALUES (?, 0, 0, 0, 0, 0, 'running')
            """, (start_time,))
        scan_id = cur.lastrowid
        con.commit()
        con.close()
        state["scan_id"] = scan_id


def update_dedupe_scan_summary(scan_id: int, space_saved_mb: int, albums_moved: int) -> None:
    """Update a dedupe-only scan_history row with end time and stats. No-op if row is not entry_type='dedupe'."""
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute("PRAGMA table_info(scan_history)")
    cols = [r[1] for r in cur.fetchall()]
    if "entry_type" not in cols:
        con.close()
        return
    cur.execute("SELECT entry_type FROM scan_history WHERE scan_id = ?", (scan_id,))
    row = cur.fetchone()
    if not row or row[0] != "dedupe":
        con.close()
        return
    end_time = time.time()
    cur.execute(
        "SELECT start_time FROM scan_history WHERE scan_id = ?", (scan_id,)
    )
    start_row = cur.fetchone()
    duration_seconds = int(end_time - start_row[0]) if start_row else 0
    cur.execute("""
        UPDATE scan_history
        SET end_time = ?, duration_seconds = ?, space_saved_mb = ?, albums_moved = ?, status = 'completed'
        WHERE scan_id = ?
    """, (end_time, duration_seconds, space_saved_mb, albums_moved, scan_id))
    con.commit()
    con.close()


init_state_db()

# ───────────────────────────────── CACHE DB SETUP ──────────────────────────────────
def init_cache_db():
    con = sqlite3.connect(str(CACHE_DB_FILE))
    # Enable WAL mode for concurrent reads/writes (same as state.db)
    con.execute("PRAGMA journal_mode=WAL;")
    con.commit()
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
    # Table for caching MusicBrainz release-group info (by MBID)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS musicbrainz_cache (
            mbid       TEXT PRIMARY KEY,
            info_json  TEXT,
            created_at INTEGER
        )
    """)
    # Table for caching "artist+album -> MBID or not_found" so we don't re-search every scan
    cur.execute("""
        CREATE TABLE IF NOT EXISTS musicbrainz_album_lookup (
            artist_norm TEXT NOT NULL,
            album_norm  TEXT NOT NULL,
            mbid        TEXT,
            info_json   TEXT,
            created_at  INTEGER,
            PRIMARY KEY (artist_norm, album_norm)
        )
    """)
    
    # Add indexes for faster lookups
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audio_cache_path ON audio_cache(path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mb_cache_mbid ON musicbrainz_cache(mbid)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mb_album_lookup_key ON musicbrainz_album_lookup(artist_norm, album_norm)")
    except sqlite3.OperationalError:
        # Indexes may already exist, ignore
        pass
    
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
    # Open with a 30-second timeout so concurrent writes wait instead of "database is locked"
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

# ----- Run summary tracking ---------------------------------------------------
def _count_rows(table: str) -> int:
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    n = cur.fetchone()[0]
    con.close()
    return n

RUN_START_TS = time.time()
RUN_BASELINE = {
    "removed_dupes": get_stat("removed_dupes"),
    "space_saved":  get_stat("space_saved"),
    "best_rows":    0,
    "loser_rows":   0,
}
try:
    RUN_BASELINE["best_rows"]  = _count_rows("duplicates_best")
    RUN_BASELINE["loser_rows"] = _count_rows("duplicates_loser")
except Exception:
    pass

SUMMARY_EMITTED = False

def emit_final_summary(reason: str = "normal"):
    global SUMMARY_EMITTED
    if SUMMARY_EMITTED:
        return

    # Compute duration and delta counters since start
    duration  = max(0, int(time.time() - RUN_START_TS))
    removed   = max(0, get_stat("removed_dupes") - RUN_BASELINE["removed_dupes"])
    saved_mb  = max(0, get_stat("space_saved")  - RUN_BASELINE["space_saved"])
    try:
        new_groups = max(0, _count_rows("duplicates_best")  - RUN_BASELINE.get("best_rows", 0))
        new_losers = max(0, _count_rows("duplicates_loser") - RUN_BASELINE.get("loser_rows", 0))
    except Exception:
        new_groups = new_losers = 0

    # Try to compute library‑wide counts for the selected sections for extra context
    def _library_counts() -> tuple[int, int]:
        try:
            con = plex_connect()
            cur = con.cursor()
            placeholders = ",".join("?" for _ in SECTION_IDS)
            # Artists = metadata_type 8, Albums = metadata_type 9
            cur.execute(f"""
                SELECT
                    (SELECT COUNT(DISTINCT id) FROM metadata_items WHERE metadata_type = 8 AND library_section_id IN ({placeholders})),
                    (SELECT COUNT(DISTINCT id) FROM metadata_items WHERE metadata_type = 9 AND library_section_id IN ({placeholders}))
            """, (*SECTION_IDS, *SECTION_IDS))
            a, b = cur.fetchone()
            con.close()
            return int(a or 0), int(b or 0)
        except Exception:
            return 0, 0

    total_artists, total_albums = _library_counts()

    # Pretty banner with commas and consistent formatting
    bar = "─" * 85
    logging.info("\n%s", bar)
    logging.info("FINAL SUMMARY")
    logging.info("Total artists           : %s", f"{total_artists:,}" if total_artists else "n/a")
    logging.info("Total albums            : %s", f"{total_albums:,}" if total_albums else "n/a")
    logging.info("Albums with dupes       : %s", f"{new_groups:,}")
    logging.info("Folders moved           : %s", f"{removed:,}")
    logging.info("Total space reclaimed   : %s MB", f"{saved_mb:,}")
    logging.info("Duration                : %s s", f"{duration:,}")
    logging.info("%s\n", bar)

    if DISCORD_WEBHOOK:
        fields = [
            {"name": "Artists",          "value": (f"{total_artists:,}" if total_artists else "n/a"), "inline": True},
            {"name": "Albums",           "value": (f"{total_albums:,}" if total_albums else "n/a"), "inline": True},
            {"name": "Groups",           "value": f"{new_groups:,}",                 "inline": True},
            {"name": "Removed",          "value": f"{removed:,}",                    "inline": True},
            {"name": "Reclaimed",        "value": f"{saved_mb:,} MB",               "inline": True},
            {"name": "Duration",         "value": f"{duration:,} s",                "inline": True},
        ]
        notify_discord_embed("✅ PMDA – Final summary", "Run completed.", fields=fields)

    SUMMARY_EMITTED = True

atexit.register(emit_final_summary)

# Shutdown MusicBrainz queue on exit
def _shutdown_mb_queue():
    global _mb_queue
    if _mb_queue is not None:
        _mb_queue.shutdown()
atexit.register(_shutdown_mb_queue)

# Shutdown ffprobe pool on exit
def _shutdown_ffprobe_pool():
    global _ffprobe_pool
    if _ffprobe_pool is not None:
        _ffprobe_pool.shutdown(wait=True)
atexit.register(_shutdown_ffprobe_pool)

# ──────────────────────────────── MusicBrainz Queue (Global Rate Limiting) ────────────────────────────────
class MusicBrainzQueue:
    """
    Global queue for MusicBrainz API calls to respect rate limiting (1 req/sec) 
    while allowing parallel submission from multiple threads.
    """
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        if not enabled:
            return
        self.queue: Queue = Queue()
        self.results: Dict[str, Tuple[Optional[dict], Optional[Exception]]] = {}
        self.locks: Dict[str, threading.Event] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self.worker_thread = threading.Thread(target=self._worker, daemon=True, name="MBQueueWorker")
        self.worker_thread.start()
        logging.info("MusicBrainz queue initialized (rate limit: 1 req/sec)")
    
    def _worker(self):
        """Worker thread that processes MusicBrainz requests sequentially with rate limiting."""
        while not self._stop_event.is_set():
            try:
                # Get next request with timeout to allow checking stop event
                try:
                    item = self.queue.get(timeout=1.0)
                except:
                    continue
                
                if item is None:  # Shutdown signal
                    break
                
                request_id, callback = item
                result = None
                error = None
                
                try:
                    result = callback()
                except Exception as e:
                    error = e
                    logging.debug("[MB Queue] Request %s failed: %s", request_id, e)
                
                # Store result and notify waiting thread
                with self._lock:
                    self.results[request_id] = (result, error)
                    if request_id in self.locks:
                        self.locks[request_id].set()
                
                # Rate limit: 1 request per second
                time.sleep(1.0)
                
            except Exception as e:
                logging.error("[MB Queue] Worker error: %s", e, exc_info=True)
    
    def submit(self, request_id: str, callback) -> dict:
        """
        Submit a MusicBrainz request to the queue.
        Returns the result dict or raises exception.
        """
        if not self.enabled:
            # Direct call if queue disabled
            return callback()
        
        # Check if already in queue or processing
        with self._lock:
            if request_id in self.results:
                result, error = self.results[request_id]
                if error:
                    raise error
                return result
            
            # Create event for this request
            event = threading.Event()
            self.locks[request_id] = event
        
        # Submit to queue
        self.queue.put((request_id, callback))
        
        # Wait for result (with timeout to avoid hanging forever)
        if event.wait(timeout=60):  # 1 minute timeout (was 5 min; reduces scan blockage on MB slowness)
            with self._lock:
                result, error = self.results.pop(request_id, (None, None))
                if request_id in self.locks:
                    del self.locks[request_id]
                
                if error:
                    raise error
                if result is None:
                    raise RuntimeError(f"MusicBrainz request {request_id} returned None")
                return result
        else:
            with self._lock:
                if request_id in self.locks:
                    del self.locks[request_id]
            raise TimeoutError(f"MusicBrainz request {request_id} timed out after 1 minute")
    
    def shutdown(self):
        """Shutdown the queue worker."""
        if not self.enabled:
            return
        self._stop_event.set()
        self.queue.put(None)  # Signal worker to stop
        if self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5.0)

# Global MusicBrainz queue instance
_mb_queue: Optional[MusicBrainzQueue] = None

def get_mb_queue() -> MusicBrainzQueue:
    """Get or create the global MusicBrainz queue."""
    global _mb_queue
    if _mb_queue is None:
        _mb_queue = MusicBrainzQueue(enabled=MB_QUEUE_ENABLED and USE_MUSICBRAINZ)
    return _mb_queue

# --- MusicBrainz cache helpers ---
def get_cached_mb_info(mbid: str) -> dict | None:
    con = sqlite3.connect(str(CACHE_DB_FILE), timeout=30)
    cur = con.cursor()
    cur.execute("SELECT info_json FROM musicbrainz_cache WHERE mbid = ?", (mbid,))
    row = cur.fetchone()
    con.close()
    if row:
        return json.loads(row[0])
    return None

def set_cached_mb_info(mbid: str, info: dict):
    con = sqlite3.connect(str(CACHE_DB_FILE), timeout=30)
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO musicbrainz_cache (mbid, info_json, created_at) VALUES (?, ?, ?)",
        (mbid, json.dumps(info), int(time.time()))
    )
    con.commit()
    con.close()


def get_cached_mb_album_lookup(artist_norm: str, album_norm: str) -> tuple[str | None, dict | None]:
    """
    Return (mbid, info) for artist+album lookup cache.
    - (None, None) = not in cache
    - ("", None) = cached as "no MusicBrainz ID found"
    - (mbid, info_dict) = cached as found (info_dict may be None if only mbid was stored)
    """
    con = sqlite3.connect(str(CACHE_DB_FILE), timeout=30)
    cur = con.cursor()
    cur.execute(
        "SELECT mbid, info_json FROM musicbrainz_album_lookup WHERE artist_norm = ? AND album_norm = ?",
        (artist_norm, album_norm),
    )
    row = cur.fetchone()
    con.close()
    if row is None:
        return (None, None)
    mbid_val, info_json = row[0], row[1]
    if mbid_val is None or mbid_val == "":
        return ("", None)
    info = json.loads(info_json) if info_json else None
    return (mbid_val, info)


def set_cached_mb_album_lookup(artist_norm: str, album_norm: str, mbid: str | None, info: dict | None):
    """Cache result of artist+album lookup. mbid None or '' = not found."""
    con = sqlite3.connect(str(CACHE_DB_FILE), timeout=30)
    cur = con.cursor()
    cur.execute(
        """INSERT OR REPLACE INTO musicbrainz_album_lookup (artist_norm, album_norm, mbid, info_json, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (artist_norm, album_norm, mbid or "", json.dumps(info) if info else None, int(time.time())),
    )
    con.commit()
    con.close()

# ───────────────────────────────── STATE IN MEMORY ──────────────────────────────────
state = {
    "scanning": False,
    "scan_progress": 0,
    "scan_total": 0,
    "deduping": False,
    "dedupe_progress": 0,
    "dedupe_total": 0,
    "dedupe_start_time": None,
    "dedupe_saved_this_run": 0,
    "dedupe_current_group": None,
    "dedupe_last_write": None,  # {"path": str, "at": float} after each move to /dupes
    # duplicates: { artist_name: [ { artist, album_id, best, losers } ] }
    "duplicates": {},
    # Scan details tracking
    "scan_artists_processed": 0,      # Nombre d'artistes traités
    "scan_artists_total": 0,          # Total d'artistes
    "scan_ai_used_count": 0,          # Nombre de groupes où l'IA a été utilisée
    "scan_mb_used_count": 0,          # Nombre d'éditions enrichies avec MusicBrainz
    "scan_ai_enabled": False,         # Si l'IA est configurée et disponible
    "scan_mb_enabled": False,         # Si MusicBrainz est activé
    "scan_audio_cache_hits": 0,       # Nombre de fichiers audio trouvés en cache
    "scan_audio_cache_misses": 0,     # Nombre de fichiers audio nécessitant ffprobe
    "scan_mb_cache_hits": 0,         # Nombre de requêtes MusicBrainz trouvées en cache
    "scan_mb_cache_misses": 0,       # Nombre de requêtes MusicBrainz nécessitant API call
    # ETA tracking
    "scan_start_time": None,          # Timestamp du début du scan
    "scan_last_update_time": None,    # Dernière mise à jour pour calcul ETA
    "scan_last_progress": 0,          # Progression au dernier update
    "scan_format_done_count": 0,     # Albums that completed format (FFprobe) step
    "scan_mb_done_count": 0,         # Albums that completed MusicBrainz lookup step
    "scan_step_total": 0,            # Total steps for progress bar (3*albums + 2 or +3 if move)
    "scan_step_progress": 0,         # Steps completed (format + MB + compare + AI + finalize + move)
    "scan_active_artists": {},       # Dict {artist_name: {"start_time": float, "total_albums": int, "albums_processed": int}}
    "improve_all": None,              # { "running": bool, "artist_id": int, "current": int, "total": int, "log": [], "result": {}, "error": str } or None
    "last_fix_all_by_provider": None, # { "musicbrainz": {identified,covers,tags}, "discogs": ..., "lastfm": ..., "bandcamp": ... } from last global fix-all run
    "last_fix_all_total_albums": 0,   # Total albums processed in that run (for N/M match display)
    "lidarr_add_incomplete": None,    # { "running": bool, "current": int, "total": int, "current_album": str, "current_artist": str, "added": int, "failed": int, "result": {} } or None
    "last_lidarr_add_added": 0,
    "last_lidarr_add_failed": 0,
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


# ───────────────────────────────── UTILITIES ──────────────────────────────────
def plex_api(path: str, method: str = "GET", **kw):
    headers = kw.pop("headers", {})
    headers["X-Plex-Token"] = PLEX_TOKEN
    return requests.request(method, f"{PLEX_HOST}{path}", headers=headers, timeout=60, **kw)

# ──────────────────────────────── Discord notifications ────────────────────────────────
def notify_discord(content: str):
    """
    Fire‑and‑forget Discord webhook notifier.
    Disabled when DISCORD_WEBHOOK is empty.
    """
    if not DISCORD_WEBHOOK:
        return
    try:
        requests.post(DISCORD_WEBHOOK, json={"content": content}, timeout=10)
    except Exception as e:
        logging.warning("Discord notification failed: %s", e)

# ──────────────────────────────── Discord embed notification ────────────────────────────────

def notify_discord_embed(title: str, description: str, thumbnail_url: str = "", fields: list[dict] | None = None):
    """
    Send a nicely formatted Discord embed so we can show album artwork
    and keep the message tidy.
    """
    if not DISCORD_WEBHOOK:
        return
    embed: dict = {
        "title": title,
        "description": description,
    }
    if thumbnail_url:
        embed["thumbnail"] = {"url": thumbnail_url}
    if fields:
        embed["fields"] = fields[:25]   # Discord hard‑limit is 25 fields / embed
    try:
        requests.post(DISCORD_WEBHOOK, json={"embeds": [embed]}, timeout=10)
    except Exception as e:
        logging.warning("Discord embed failed: %s", e)

# ─── Run connection check & self‑diagnostic (called from main so WebUI can start first in serve mode) ───
def run_startup_checks() -> None:
    """Run Plex validation, self-diagnostic and path cross-check. Call after starting the WebUI server in serve mode."""
    if not PLEX_CONFIGURED:
        logging.info("Skipping startup checks (Plex not configured – use Settings to configure).")
        return
    _validate_plex_connection()
    if not _self_diag():
        raise SystemExit("Self‑diagnostic failed – please fix the issues above and restart PMDA.")
    if not DISABLE_PATH_CROSSCHECK:
        _cross_check_bindings()
    else:
        logging.info("PATH cross-check skipped (DISABLE_PATH_CROSSCHECK=true).")


def container_to_host(p: str) -> Optional[Path]:
    for pre, real in PATH_MAP.items():
        if p.startswith(pre):
            return Path(real) / p[len(pre):].lstrip("/")
    return None

def path_for_fs_access(p: Path) -> Path:
    """
    Return a path that the current process can read (e.g. container path when
    running in Docker). If *p* exists, return it. Otherwise, if *p* matches
    a PATH_MAP value (real/host side), convert it to the corresponding key
    (plex/container side) so that safe_folder_size and similar can succeed.
    """
    try:
        if p.exists():
            return p
    except Exception:
        pass
    sp = str(p)
    for plex_prefix, real_prefix in PATH_MAP.items():
        if sp.startswith(real_prefix):
            suffix = sp[len(real_prefix):].lstrip("/")
            return Path(plex_prefix) / suffix if suffix else Path(plex_prefix)
    return p

def relative_path_under_known_roots(path: Path) -> Optional[Path]:
    """
    Return the relative path of *path* under any known PATH_MAP root (host or container).
    Falls back to ``None`` when the path is outside every configured root.
    """
    try:
        resolved = path.resolve()
    except Exception:
        resolved = path

    roots: list[Path] = []
    for value in PATH_MAP.values():
        roots.append(Path(value))
    for key in PATH_MAP.keys():
        roots.append(Path(key))

    for root in roots:
        candidates = {root}
        try:
            candidates.add(root.resolve())
        except Exception:
            pass
        for candidate in candidates:
            try:
                return resolved.relative_to(candidate)
            except ValueError:
                continue
    return None

def build_dupe_destination(src_folder: Path) -> Path:
    """
    Compute the destination path under DUPE_ROOT while preserving the relative
    artist/album structure whenever possible.
    """
    rel = relative_path_under_known_roots(src_folder)
    if rel is None or str(rel).strip() == "":
        rel = Path(src_folder.name)
    return DUPE_ROOT / rel

def folder_size(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())

def safe_folder_size(p: Path) -> int:
    """Return folder size in bytes, or 0 if path missing or not readable."""
    try:
        if not p.exists() or not p.is_dir():
            return 0
        return folder_size(p)
    except Exception:
        return 0

def score_format(ext: str) -> int:
    return FMT_SCORE.get(ext.lower(), 0)

def norm_album(title: str) -> str:
    """
    Normalise an album title for duplicate grouping.
    • Remove parenthetical/bracketed content conservatively.
    • Collapse whitespace and lowercase.
    • If the result is empty or too short (<3), fall back to the raw title (lowercased).
    • If still empty, return a unique placeholder so different unknown titles don't collide.
    """
    raw = (title or "").strip()
    # Remove any content in parentheses or brackets
    cleaned = re.sub(r"[\(\[][^(\)\]]*[\)\]]", "", raw)
    cleaned = " ".join(cleaned.split()).lower()

    if len(cleaned) >= 3:
        return cleaned

    # Fallback to raw (lowercased) if cleaning erased the useful bits
    fallback = raw.lower()
    fallback = " ".join(fallback.split())
    if len(fallback) >= 3:
        return fallback

    # Last resort: avoid collapsing different untitled releases together
    if raw:
        h = hashlib.sha1(raw.encode("utf-8", "ignore")).hexdigest()[:8]
        return f"__untitled__-{h}"
    return "__untitled__"


# Regex for format/version parenthetical suffixes: (flac), (mp3), (EP), (flac, EP), etc.
_PARENTHETICAL_SUFFIX_RE = re.compile(r"(?:\s*\([\w\s,]+\))+$", re.IGNORECASE)


def strip_parenthetical_suffixes(s: str) -> str:
    """
    Remove trailing parenthetical format/version segments from a string.
    Examples: "Album (flac)" -> "Album", "Album (flac) (EP)" -> "Album", "Album (flac, EP)" -> "Album".
    """
    if not s or not s.strip():
        return (s or "").strip()
    out = _PARENTHETICAL_SUFFIX_RE.sub("", (s or "").strip())
    return out.strip()


def norm_album_for_dedup(title: str, normalize_parenthetical: bool) -> str:
    """
    Normalise an album title for duplicate grouping, with optional parenthetical handling.
    When normalize_parenthetical is True: strip format/version parentheticals (flac), (mp3), (EP), etc.
    so that "Lemodie (Flac)" and "Lemodie" group together. When False, do not strip them
    (treat "Lemodie (Flac)" and "Lemodie" as different).
    """
    raw = (title or "").strip()
    if normalize_parenthetical:
        raw = strip_parenthetical_suffixes(raw) or raw
    cleaned = " ".join(raw.split()).lower()
    if len(cleaned) >= 3:
        return cleaned
    fallback = (title or "").strip().lower()
    fallback = " ".join(fallback.split())
    if len(fallback) >= 3:
        return fallback
    if raw or (title or "").strip():
        h = hashlib.sha1((raw or title or "").encode("utf-8", "ignore")).hexdigest()[:8]
        return f"__untitled__-{h}"
    return "__untitled__"

def derive_album_title(plex_title: str, meta: Dict[str, str], folder: Path, album_id: int) -> Tuple[str, str]:
    """
    Pick the most trustworthy album title available and return (title, source).
    Priority: Plex DB → embedded tags → folder name → unique placeholder.
    """
    if plex_title:
        title = plex_title.strip()
        if title:
            return (title, "plex")

    for key in ("album", "title", "release", "albumartist"):
        candidate = meta.get(key, "")
        if candidate:
            title = candidate.strip()
            if title:
                return (title, f"tag:{key}")

    folder_title = folder.name.replace("_", " ").strip()
    if folder_title:
        return (folder_title, "folder")

    return (f"Untitled Album #{album_id}", "placeholder")

def get_primary_format(folder: Path) -> str:
    try:
        for f in folder.rglob("*"):
            if AUDIO_RE.search(f.name):
                return f.suffix[1:].upper()
    except OSError as e:
        logging.debug("get_primary_format I/O error for %s: %s", folder, e)
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
            # Ensure "date" is present in each version for modal rendering
            if "meta" in best:
                best["date"] = best["meta"].get("date") or best["meta"].get("originaldate") or ""
            for l in g.get("losers", []):
                if "meta" in l:
                    l["date"] = l["meta"].get("date") or l["meta"].get("originaldate") or ""
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
@app.route("/api/edition_details")
def edition_details():
    album_id = int(request.args["album_id"])
    folder   = Path(request.args["folder"])
    tracks   = get_tracks(plex_connect(), album_id)
    fmt_score, br, sr, bd, _ = analyse_format(folder)
    info = (fmt_score, br, sr, bd)  # Return 4-tuple for backward compatibility
    track_list = [{"idx": t.idx, "title": t.title, "dur": t.dur} for t in tracks]
    return jsonify({"tracks": track_list, "info": info})

@app.route("/api/dedupe_manual", methods=["POST"])
def dedupe_manual():
    r = _requires_config()
    if r is not None:
        return r
    req = request.get_json(force=True)
    for item in req:
        # reuse existing purge logic
        _purge_invalid_edition({
            "folder"   : item["folder"],
            "artist"   : "",           # not needed for purge
            "title_raw": "",
            "album_id" : int(item["album_id"])
        })
    return jsonify({"status":"ok"})

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

def get_tracks_with_ids(db_conn, album_id: int) -> List[dict]:
    """Return list of track dicts with id, title, index, duration_ms for library playback API."""
    has_parent = any(r[1] == "parent_index"
                     for r in db_conn.execute("PRAGMA table_info(metadata_items)"))
    sql = f"""
      SELECT tr.id, tr.title, tr."index",
             {'tr.parent_index' if has_parent else 'NULL'} AS disc_no,
             COALESCE(mp.duration, tr.duration, 0) AS duration
      FROM metadata_items tr
      JOIN media_items mi ON mi.metadata_item_id = tr.id
      JOIN media_parts mp ON mp.media_item_id = mi.id
      WHERE tr.parent_id = ? AND tr.metadata_type = 10
      ORDER BY tr."index"
    """
    rows = db_conn.execute(sql, (album_id,)).fetchall()
    return [
        {"id": r[0], "title": (r[1] or "").strip(), "index": r[2] or 0, "duration_ms": r[4] or 0}
        for r in rows
    ]

def _stream_columns(db_conn) -> tuple[str, str] | None:
    """If media_streams exists with codec/bitrate, return (codec_col, bitrate_col) else None."""
    try:
        info = db_conn.execute("PRAGMA table_info(media_streams)").fetchall()
        cols = {r[1].lower() for r in info}
        if "codec" in cols and "bitrate" in cols:
            return ("codec", "bitrate")
        return None
    except Exception:
        return None

def get_tracks_for_details(db_conn, album_id: int) -> List[dict]:
    """
    Return list of track dicts for API: name, title, idx, duration (seconds), dur (ms),
    format (codec), bitrate (kbps), for use in /details editions.
    """
    has_parent = any(r[1] == "parent_index"
                     for r in db_conn.execute("PRAGMA table_info(metadata_items)"))
    stream_cols = _stream_columns(db_conn)
    if stream_cols is None:
        # No media_streams codec/bitrate: return basic track info (duration from part or metadata_items)
        rows = db_conn.execute(f"""
          SELECT tr.title, tr."index",
                 {'tr.parent_index' if has_parent else 'NULL'} AS disc_no,
                 COALESCE(mp.duration, tr.duration) AS duration, mp.file
          FROM metadata_items tr
          JOIN media_items mi ON mi.metadata_item_id = tr.id
          JOIN media_parts mp ON mp.media_item_id = mi.id
          WHERE tr.parent_id = ? AND tr.metadata_type = 10
          ORDER BY tr."index"
        """, (album_id,)).fetchall()
        return [
            {
                "name": (t or "").strip(),
                "title": (t or "").strip(),
                "idx": i or 0,
                "duration": (dur or 0) // 1000,
                "dur": dur or 0,
                "format": None,
                "bitrate": None,
                "path": (raw_path or "").strip() or None,
            }
            for t, i, _d, dur, raw_path in rows
        ]
    codec_col, bitrate_col = stream_cols
    # stream_type_id 2 = audio in Plex; one row per track (pick stream with max bitrate if several)
    sql = f"""
      SELECT tr.title, tr."index",
             {'tr.parent_index' if has_parent else 'NULL'} AS disc_no,
             COALESCE(mp.duration, tr.duration), ms.{codec_col}, ms.{bitrate_col}, mp.file
      FROM metadata_items tr
      JOIN media_items mi ON mi.metadata_item_id = tr.id
      JOIN media_parts mp ON mp.media_item_id = mi.id
      LEFT JOIN media_streams ms ON ms.media_part_id = mp.id AND ms.stream_type_id = 2
        AND ms.id = (
          SELECT ms2.id FROM media_streams ms2
          WHERE ms2.media_part_id = mp.id AND ms2.stream_type_id = 2
          ORDER BY COALESCE(ms2.{bitrate_col}, 0) DESC LIMIT 1
        )
      WHERE tr.parent_id = ? AND tr.metadata_type = 10
      ORDER BY tr."index"
    """
    try:
        rows = db_conn.execute(sql, (album_id,)).fetchall()
    except Exception as e:
        logging.warning(
            "get_tracks_for_details (streams) failed for album_id=%s: %s",
            album_id, e
        )
        tracks = get_tracks(db_conn, album_id)
        return [
            {"name": t.title, "title": t.title, "idx": t.idx, "duration": t.dur // 1000, "dur": t.dur, "format": None, "bitrate": None, "path": None}
            for t in tracks
        ]
    out = []
    seen_index = set()
    for row in rows:
        t, i, _d, dur, codec, bitrate, raw_path = row
        idx = i or 0
        if idx in seen_index:
            continue
        seen_index.add(idx)
        title = (t or "").strip()
        dur_ms = dur or 0
        # Plex DB: bitrate is in bps (e.g. 867234 -> 867 kbps)
        br_kbps = None
        if bitrate is not None:
            b = int(bitrate)
            br_kbps = b if b < 100000 else b // 1000
        out.append({
            "name": title,
            "title": title,
            "idx": idx,
            "duration": dur_ms // 1000,
            "dur": dur_ms,
            "format": (codec or "").strip().upper() or None,
            "bitrate": br_kbps,
            "path": (raw_path or "").strip() or None,
        })
    return out

def album_title(db_conn, album_id: int) -> str:
    row = db_conn.execute(
        "SELECT title FROM metadata_items WHERE id = ?", (album_id,)
    ).fetchone()
    return row[0] if row else ""

def first_part_path(db_conn, album_id: int) -> Optional[Path]:
    sql = """
      SELECT mp.file
      FROM metadata_items tr
      JOIN media_items mi ON mi.metadata_item_id = tr.id
      JOIN media_parts mp ON mp.media_item_id = mi.id
      WHERE tr.parent_id = ? LIMIT 1
    """
    row = db_conn.execute(sql, (album_id,)).fetchone()
    if not row:
        return None

    raw_path = row[0]
    # Try to map to host path, fallback to container path if mapping missing
    host_loc = container_to_host(raw_path)
    if host_loc is None:
        return Path(raw_path).parent

    return host_loc.parent


def _album_path_under_dupes(db_conn, album_id: int) -> bool:
    """Return True if the album's path is under DUPE_ROOT (already moved). Used to skip library-only groups that were already deduped."""
    sql = """
      SELECT mp.file FROM metadata_items tr
      JOIN media_items mi ON mi.metadata_item_id = tr.id
      JOIN media_parts mp ON mp.media_item_id = mi.id
      WHERE tr.parent_id = ? LIMIT 1
    """
    row = db_conn.execute(sql, (album_id,)).fetchone()
    if not row or not row[0]:
        return False
    raw = (row[0] or "").replace("\\", "/")
    if str(DUPE_ROOT) in raw or raw.strip().startswith("/dupes") or "/dupes/" in raw:
        return True
    if "dupes" in raw.lower() and ("/dupes" in raw or "Music_dupes" in raw or "dupes/" in raw):
        return True
    return False


# Cover filenames we consider "has cover" (same as create_pmda_test_files.sh)
_COVER_NAMES = (
    "cover.jpg", "cover.png", "cover.jpeg",
    "folder.jpg", "Folder.jpg", "AlbumArt.jpg", "AlbumArtSmall.jpg",
    "front.jpg", "artwork.jpg",
)

def album_folder_has_cover(folder: Path) -> bool:
    """Return True if the album folder contains any known cover image file."""
    if not folder or not folder.is_dir():
        return False
    try:
        for name in _COVER_NAMES:
            if (folder / name).is_file():
                return True
        return False
    except OSError:
        return False


def _first_cover_path(folder: Path) -> Optional[Path]:
    """Return the path of the first existing cover file in the folder, or None."""
    if not folder or not folder.is_dir():
        return None
    try:
        for name in _COVER_NAMES:
            p = folder / name
            if p.is_file():
                return p
        return None
    except OSError:
        return None


_MAX_COVER_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB for vision API


def _encode_local_cover_to_data_uri(cover_path: Path) -> Optional[str]:
    """
    Read the cover file, encode as base64 data URI. Skips if file > 5 MB.
    Returns data:image/jpeg;base64,... or data:image/png;base64,... etc.
    """
    if not cover_path or not cover_path.is_file():
        return None
    try:
        stat = cover_path.stat()
        if stat.st_size > _MAX_COVER_SIZE_BYTES:
            logging.debug("[Vision] Skipping cover %s: size %d > %d", cover_path, stat.st_size, _MAX_COVER_SIZE_BYTES)
            return None
        raw = cover_path.read_bytes()
        b64 = base64.b64encode(raw).decode("ascii")
        suffix = cover_path.suffix.lower()
        mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".gif": "image/gif", ".webp": "image/webp"}
        mime = mime_map.get(suffix, "image/jpeg")
        return f"data:{mime};base64,{b64}"
    except Exception as e:
        logging.debug("[Vision] Failed to encode cover %s: %s", cover_path, e)
        return None

def extract_tags(audio_path: Path) -> dict[str, str]:
    """
    Return *all* container‑level metadata tags for the given audio file
    (FLAC/MP3/M4A/…).

    Uses ffprobe so no external Python deps are required.
    """
    try:
        out = subprocess.check_output(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format_tags",
                "-of", "default=noprint_wrappers=1",
                str(audio_path)
            ],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10
        )
        tags = {}
        for line in out.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                # ffprobe returns TAG:KEY=VAL sometimes – strip the prefix
                if k.startswith("TAG:"):
                    k = k[4:]
                tags[k.lower()] = v.strip()
        return tags
    except Exception:
        return {}

# Global ffprobe pool for parallel processing
_ffprobe_pool: Optional[ThreadPoolExecutor] = None

def get_ffprobe_pool() -> ThreadPoolExecutor:
    """Get or create the global ffprobe pool."""
    global _ffprobe_pool
    if _ffprobe_pool is None:
        _ffprobe_pool = ThreadPoolExecutor(max_workers=FFPROBE_POOL_SIZE, thread_name_prefix="ffprobe")
        logging.debug(f"Created ffprobe pool with {FFPROBE_POOL_SIZE} workers")
    return _ffprobe_pool

def _run_ffprobe(fpath: str) -> tuple[int, int, int]:
    """
    Run ffprobe on a single file and return (bit_rate, sample_rate, bit_depth).
    This is the actual work function that will be run in the pool.
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "a:0",
        "-show_entries",
        "format=bit_rate:stream=bit_rate,sample_rate,bits_per_raw_sample,bits_per_sample,sample_fmt",
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
                        br = v  # keep highest bit‑rate seen
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
    
    return (br, sr, bd)

def analyse_format(folder: Path) -> tuple[int, int, int, int, bool]:
    """
    Inspect up to **three** audio files inside *folder* and return a 4‑tuple:

        (fmt_score, bit_rate, sample_rate, bit_depth)

    *   **fmt_score** derives from the global FORMAT_PREFERENCE list.
    *   **bit_rate** is in **bps** (`0` when not reported, e.g. lossless FLAC).
    *   **sample_rate** is in **Hz**.
    *   **bit_depth** is 16 / 24 / 32 when derivable, otherwise 0.

    Rationale for retry logic
    -------------------------
    A single, transient ffprobe failure (network share hiccup, race during mount,
    etc.) previously led to a *false « invalid »* verdict because all tech values
    were 0.
    We now:

    1. Collect *all* audio files under the folder (breadth‑first, glob pattern
       from `AUDIO_RE`).
    2. Probe **up to three distinct files** or **two attempts per file** (cache +
       fresh call) until we obtain at least one non‑zero technical metric.
    3. Only if **every attempt** yields `(0, 0, 0)` do we fall back to the
       "invalid" classification.

    Each `(path, mtime)` result – even the all‑zero case – is cached so we
    never hammer ffprobe, but a later scan still re‑probes if the file changes.
    
    Non-cached ffprobe calls are now processed in parallel using a thread pool.
    """
    audio_files = [p for p in folder.rglob("*") if AUDIO_RE.search(p.name)]
    if not audio_files:
        return (0, 0, 0, 0, False)

    # First pass: check cache for all files
    files_to_probe = []
    for audio_file in audio_files[:3]:
        ext   = audio_file.suffix[1:].lower()
        fpath = str(audio_file)
        mtime = int(audio_file.stat().st_mtime)

        # Check cache first
        cached = get_cached_info(fpath, mtime)
        if cached and not (cached == (0, 0, 0) and ext == "flac"):
            br, sr, bd = cached
            if br or sr or bd:
                # Track cache hit (will be aggregated in scan_duplicates)
                return (score_format(ext), br, sr, bd, True)  # True = cache hit
        
        # File not in cache or cache miss, add to probe list
        files_to_probe.append((audio_file, ext, fpath, mtime))
    
    # Second pass: probe files in parallel if pool is enabled
    if files_to_probe and FFPROBE_POOL_SIZE > 1:
        futures = {}
        pool = get_ffprobe_pool()
        
        for audio_file, ext, fpath, mtime in files_to_probe:
            future = pool.submit(_run_ffprobe, fpath)
            futures[future] = (audio_file, ext, fpath, mtime)
        
        # Wait for results (with timeout per file)
        for future in as_completed(futures):
            audio_file, ext, fpath, mtime = futures[future]
            try:
                br, sr, bd = future.result(timeout=15)  # Slightly longer timeout for pool
            except Exception:
                br, sr, bd = 0, 0, 0
            
            # Cache the result
            set_cached_info(fpath, mtime, br, sr, bd)
            
            if br or sr or bd:  # success on this file → done
                return (score_format(ext), br, sr, bd, False)  # False = cache miss
    else:
        # Sequential processing (fallback or pool disabled)
        for audio_file, ext, fpath, mtime in files_to_probe:
            br, sr, bd = _run_ffprobe(fpath)
            
            # Cache the result
            set_cached_info(fpath, mtime, br, sr, bd)
            
            if br or sr or bd:  # success on this file → done
                return (score_format(ext), br, sr, bd, False)  # False = cache miss

    # After probing up to 3 files and still nothing usable → treat as invalid
    if audio_files:
        first_ext = audio_files[0].suffix[1:].lower()
        return (score_format(first_ext), 0, 0, 0, False)
    return (0, 0, 0, 0, False)

# ───────────────────────────────── DUPLICATE DETECTION ─────────────────────────────────
def signature(tracks: List[Track]) -> tuple:
    # round durations to seconds before grouping
    return tuple(sorted((
        t.disc,
        t.idx,
        t.title,
        int(round(t.dur/1000))
    ) for t in tracks))

def overlap(a: set, b: set) -> float:
    return len(a & b) / max(len(a), len(b))

def editions_share_confident_signal(ed_list: List[dict]) -> bool:
    """
    Determine whether a potential duplicate group has enough evidence to be trusted.
    Accept when at least two editions have high-confidence titles, all track
    signatures match, they share the same MusicBrainz release-group ID, or
    all have the same album_norm (e.g. folder-derived titles like "Album [dupe]").
    """
    if len(ed_list) < 2:
        return False

    high_conf_prefixes = {"plex", "tag"}
    high_conf_titles = sum(
        1 for e in ed_list
        if e.get("title_source", "").partition(":")[0] in high_conf_prefixes
    )
    if high_conf_titles >= 2:
        return True

    sigs = {e.get("sig") for e in ed_list if e.get("sig")}
    if len(sigs) == 1 and sigs:
        return True

    rg_ids = {e.get("rg_info", {}).get("id") for e in ed_list if e.get("rg_info", {}).get("id")}
    if len(rg_ids) == 1 and rg_ids:
        return True

    # Same normalized title (e.g. "Night Cycle" and "Night Cycle [dupe]" -> "night cycle")
    norms = {e.get("album_norm") for e in ed_list if e.get("album_norm")}
    if len(norms) == 1 and norms:
        return True

    # Same Plex-normalized title (we grouped by this; accept so scan results match library)
    plex_norms = {e.get("plex_norm") for e in ed_list if e.get("plex_norm")}
    if len(plex_norms) == 1 and plex_norms:
        return True

    return False

def detect_broken_album(db_conn, album_id: int, tracks: List[Track], mb_release_group_info: dict | None) -> tuple[bool, int | None, int, list]:
    """
    Detect if an album is broken (missing tracks).
    Returns (is_broken, expected_track_count, actual_track_count, missing_indices)
    
    Detection methods:
    1. MusicBrainz comparison: if expected track count is known and actual < 90% of expected
    2. Heuristic: gaps in track indices (configurable thresholds)
    """
    actual_count = len(tracks)
    if actual_count == 0:
        return True, 0, 0, []
    
    track_indices = sorted([t.idx for t in tracks])
    
    # Check for gaps in track indices
    gaps = []
    for i in range(len(track_indices) - 1):
        if track_indices[i+1] - track_indices[i] > 1:
            gaps.append((track_indices[i], track_indices[i+1]))
    
    # Method 1: MusicBrainz comparison
    if mb_release_group_info:
        # Try to extract expected track count from MusicBrainz release-group
        # This would require parsing the releases/media structure
        # For now, we'll use the heuristic method as primary
        pass
    
    # Method 2: Heuristic (gaps) - using configurable thresholds
    if gaps:
        # Check if gap > configured consecutive threshold
        large_gaps = [g for g in gaps if g[1] - g[0] > BROKEN_ALBUM_CONSECUTIVE_THRESHOLD]
        if large_gaps:
            return True, None, actual_count, gaps
        
        # Check if gaps represent > configured percentage threshold
        total_missing = sum(g[1] - g[0] - 1 for g in gaps)
        if total_missing > actual_count * BROKEN_ALBUM_PERCENTAGE_THRESHOLD:
            return True, None, actual_count, gaps
    
    return False, None, actual_count, []


def _detect_gaps_in_indices(indices: list) -> tuple[bool, int, list]:
    """
    Given sorted track indices, detect if album has gaps (incomplete).
    Returns (is_broken, actual_count, gaps) where gaps is list of (start, end) pairs.
    Uses same thresholds as detect_broken_album.
    """
    if not indices:
        return False, 0, []
    track_indices = sorted([int(i) for i in indices])
    actual_count = len(track_indices)
    gaps = []
    for i in range(len(track_indices) - 1):
        if track_indices[i + 1] - track_indices[i] > 1:
            gaps.append((track_indices[i], track_indices[i + 1]))
    if not gaps:
        return False, actual_count, []
    large_gaps = [g for g in gaps if g[1] - g[0] > BROKEN_ALBUM_CONSECUTIVE_THRESHOLD]
    if large_gaps:
        return True, actual_count, gaps
    total_missing = sum(g[1] - g[0] - 1 for g in gaps)
    if total_missing > actual_count * BROKEN_ALBUM_PERCENTAGE_THRESHOLD:
        return True, actual_count, gaps
    return False, actual_count, []


def resolve_mbid_to_release_group(mbid: str, tag_source: str = "", use_queue: bool = True) -> Optional[str]:
    """
    Return a MusicBrainz release-group ID for use with get_release_group_by_id / fetch_mb_release_group_info.
    - If tag_source is 'musicbrainz_releasegroupid', mbid is already a release-group ID: return it.
    - If tag_source is 'musicbrainz_releaseid' or 'musicbrainz_albumid' (or empty/unknown), mbid is a release ID:
      fetch the release, extract release-group id, return it. On API error, log and return None.
    - use_queue: if False, call API directly (required when already inside MB queue worker to avoid deadlock).
    """
    if not mbid or not mbid.strip():
        return None
    mbid = mbid.strip()
    if tag_source == "musicbrainz_releasegroupid":
        return mbid
    # Release ID (musicbrainz_releaseid, musicbrainz_albumid) or unknown: resolve via get_release_by_id
    # MusicBrainz API expects "release-groups" (plural) for release lookup includes
    try:
        def _fetch_release():
            return musicbrainzngs.get_release_by_id(mbid, includes=["release-groups"])["release"]
        if use_queue and MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
            rel = get_mb_queue().submit(f"rel_{mbid}", _fetch_release)
        else:
            rel = _fetch_release()
        rg = rel.get("release-group")
        if rg and isinstance(rg.get("id"), str):
            return rg["id"]
        return None
    except Exception as e:
        logging.debug("resolve_mbid_to_release_group: failed for mbid=%s tag_source=%s: %s", mbid, tag_source, e)
        return None


def fetch_mb_release_group_info(mbid: str) -> tuple[dict, bool]:
    """
    Fetch primary type, secondary-types, and optional format summary from MusicBrainz release-group.
    Uses musicbrainzngs for proper rate-limiting. Only inc=releases is used (inc=media is not valid
    for release-group and causes 400); format_summary may be empty unless releases include media.
    Returns (info_dict, cache_hit) where cache_hit is True if found in cache.
    """
    # Attempt to reuse cached MusicBrainz release-group info
    cached = get_cached_mb_info(mbid)
    if cached:
        logging.debug("[MusicBrainz RG Info] using cached info for MBID %s", mbid)
        return cached, True  # True = cache hit
    
    # Use queue for rate-limited API call
    # Note: for release-group, only inc=releases is valid; inc=media applies to release, not release-group (400 if used).
    def _fetch():
        try:
            result = musicbrainzngs.get_release_group_by_id(
                mbid,
                includes=["releases"]
            )["release-group"]
            return result
        except musicbrainzngs.WebServiceError as e:
            error_msg = str(e)
            if "503" in error_msg or "rate" in error_msg.lower():
                logging.warning("[MusicBrainz] Rate limited for MBID %s, will retry after delay", mbid)
                time.sleep(1.5)
                try:
                    result = musicbrainzngs.get_release_group_by_id(mbid, includes=["releases"])["release-group"]
                    return result
                except musicbrainzngs.WebServiceError as e2:
                    raise RuntimeError(f"MusicBrainz lookup failed for {mbid} after retry: {e2}") from None
            if "404" in error_msg:
                # mbid may be a release ID; resolve to release-group and retry
                resolved = resolve_mbid_to_release_group(mbid, "")
                if resolved and resolved != mbid:
                    result = musicbrainzngs.get_release_group_by_id(
                        resolved, includes=["releases"]
                    )["release-group"]
                    return result
            raise RuntimeError(f"MusicBrainz lookup failed for {mbid}: {e}") from None
    
    try:
        if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
            result = get_mb_queue().submit(f"rg_{mbid}", _fetch)
        else:
            result = _fetch()
    except Exception as e:
        raise

    primary = result.get("primary-type", "")
    secondary = result.get("secondary-types", [])
    formats = set()

    # Each release may have multiple medium entries
    for release in result.get("releases", []):
        for medium in release.get("media", []):
            fmt = medium.get("format")
            qty = medium.get("track-count") or medium.get("position") or medium.get("discs-count") or medium.get("count")
            # Some media entries include 'track-count' and 'format'
            if fmt:
                # quantity fallback: if medium["discs"] is present
                if isinstance(medium.get("format"), str):
                    quantity = medium.get("track-count", 1)
                else:
                    quantity = 1
                formats.add(f"{quantity}×{fmt}")

    format_summary = ", ".join(sorted(formats))
    logging.debug("[MusicBrainz RG Info] raw response for MBID %s: %s", mbid, result)
    logging.debug("[MusicBrainz RG Info] parsed primary_type=%s, secondary_types=%s, format_summary=%s", primary, secondary, format_summary)
    info = {
        "id": mbid,  # Include the MBID in the info dict
        "primary_type": primary,
        "secondary_types": secondary,
        "format_summary": format_summary
    }
    # Cache the lookup result
    set_cached_mb_info(mbid, info)
    return info, False  # False = cache miss

# ──────────────────────────────── MusicBrainz search fallback ────────────────────────────────
def _extract_track_titles_from_mb_release(release_response: dict) -> List[str]:
    """Extract track titles from a MusicBrainz release response (get_release_by_id with includes=['recordings'])."""
    titles: List[str] = []
    try:
        release = release_response.get("release") if isinstance(release_response.get("release"), dict) else release_response
        for medium in release.get("medium-list", []):
            for track in medium.get("track-list", []):
                rec = track.get("recording") or {}
                t = rec.get("title") if isinstance(rec, dict) else None
                if t:
                    titles.append(str(t))
    except Exception:
        pass
    return titles


def _mb_track_count_from_rg_info(info: dict) -> int:
    """
    Return total track count from release-group info (release-list / medium-list / track-count).
    When the API returns no medium-list (track count 0) but we have release-list, fetch the first
    release with includes=['recordings'] to get the real count (release-group lookup often omits media).
    Tolerates both "release-list"/"medium-list" and "releases"/"media" key names (musicbrainzngs/API variants).
    """
    releases = info.get("release-list") or info.get("releases") or []
    count = 0
    for release in releases:
        media = release.get("medium-list") or release.get("media") or []
        for medium in media:
            count += int(medium.get("track-count", 0) or 0)
    if count > 0:
        return count
    if not releases:
        return 0
    first_id = releases[0].get("id")
    if not first_id:
        return 0
    try:
        rel_resp = musicbrainzngs.get_release_by_id(first_id, includes=["recordings"])
        release = rel_resp.get("release") if isinstance(rel_resp.get("release"), dict) else rel_resp
        n = 0
        for medium in release.get("medium-list") or release.get("media") or []:
            track_list = medium.get("track-list") or medium.get("tracks") or []
            if track_list:
                n += len(track_list)
            else:
                n += int(medium.get("track-count", 0) or 0)
        return n
    except Exception:
        return 0


def _search_mb_rg_candidates(artist: str, release_query: str, strict: bool) -> List[dict]:
    """Run MusicBrainz search_release_groups; return list of release-group dicts (no details).
    release_query can be normalized title or raw title (e.g. 'Isolette') for better API match."""
    result = musicbrainzngs.search_release_groups(
        artist=artist,
        release=release_query,
        limit=15,
        strict=strict
    )
    logging.debug("[MusicBrainz Search] artist=%r release=%r strict=%s -> %d results", artist, release_query, strict, len(result.get('release-group-list', [])))
    return result.get('release-group-list', [])


def _browse_mb_rg_by_artist(artist: str, album_norm: str) -> List[dict]:
    """Get release-group candidates by browsing artist's release groups; filter by title match."""
    if not USE_MUSICBRAINZ:
        return []
    try:
        search_result = musicbrainzngs.search_artists(artist=artist, limit=1)
        artist_list = search_result.get("artist-list", [])
        if not artist_list:
            return []
        artist_mbid = artist_list[0]["id"]
        artist_data = musicbrainzngs.get_artist_by_id(artist_mbid, includes=["release-groups"])
        rg_list = artist_data.get("artist", {}).get("release-group-list", [])
        candidates = []
        for rg in rg_list:
            title = (rg.get("title") or "").strip()
            if not title:
                continue
            rg_norm = norm_album(title)
            if rg_norm == album_norm or album_norm in rg_norm or rg_norm in album_norm:
                candidates.append(rg)
        logging.debug("[MusicBrainz Browse] artist=%s album_norm=%s -> %d title-matched release groups", artist, album_norm, len(candidates))
        return candidates
    except Exception as e:
        logging.debug("[MusicBrainz Browse] failed for '%s' / '%s': %s", artist, album_norm, e)
        return []


def search_mb_release_group_by_metadata(
    artist: str,
    album_norm: str,
    tracks: set[str],
    title_raw: Optional[str] = None,
    album_folder: Optional[Path] = None,
) -> tuple[dict | None, bool]:
    """
    Fallback search on MusicBrainz by artist name, normalized album title, and optional track titles.
    Tries: (1) search with strict=True, (2) search with strict=False, (3) browse by artist and match title,
    (4) if still no candidates and title_raw differs from album_norm, retry search/browse with title_raw (e.g. "Isolette").
    If multiple candidates and USE_AI_FOR_MB_MATCH, uses AI to pick best match.
    When USE_AI_VISION_FOR_COVER and album_folder are set, compares local cover to Cover Art Archive (vision).
    When 0 candidates or AI returns NONE, can use Bandcamp + web search (Serper) + AI to suggest MBID.
    Returns (release-group info dict or None, verified_by_ai: bool). verified_by_ai is True when match was chosen by USE_AI_FOR_MB_VERIFY.
    """
    if album_folder is not None and not isinstance(album_folder, Path):
        album_folder = Path(album_folder) if album_folder else None
    def _fetch_rg_details(rg_id: str):
        """Fetch release group details by ID. On 404, try treating rg_id as a release ID and resolve to release-group."""
        try:
            info = musicbrainzngs.get_release_group_by_id(
                rg_id, includes=["releases"]
            )["release-group"]
            return info
        except musicbrainzngs.WebServiceError as e:
            if "404" not in str(e):
                raise
            # API returned 404: rg_id may be a release ID (search/browse can occasionally return release refs)
            # use_queue=False to avoid deadlock (we are already inside the MB queue worker)
            resolved = resolve_mbid_to_release_group(rg_id, "musicbrainz_releaseid", use_queue=False)
            if resolved and resolved != rg_id:
                info = musicbrainzngs.get_release_group_by_id(
                    resolved, includes=["releases"]
                )["release-group"]
                return info
            raise

    seen_ids: set[str] = set()
    candidates: List[dict] = []

    def _collect_candidates(search_results: List[dict]) -> None:
        for rg in search_results:
            rg_id = rg.get("id")
            if rg_id and rg_id not in seen_ids:
                seen_ids.add(rg_id)
                candidates.append(rg)

    def _run_search_and_browse(release_query: str) -> None:
        if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
            _collect_candidates(get_mb_queue().submit(f"search_{artist}_{release_query}_1", lambda: _search_mb_rg_candidates(artist, release_query, True)))
            _collect_candidates(get_mb_queue().submit(f"search_{artist}_{release_query}_0", lambda: _search_mb_rg_candidates(artist, release_query, False)))
        else:
            _collect_candidates(_search_mb_rg_candidates(artist, release_query, True))
            _collect_candidates(_search_mb_rg_candidates(artist, release_query, False))
        if not candidates:
            browse_list = _browse_mb_rg_by_artist(artist, release_query)
            _collect_candidates(browse_list)

    try:
        # 1–3) Search (strict, non-strict) and browse by artist with normalized title
        _run_search_and_browse(album_norm)

        # 4) If still no candidates, try with raw title (e.g. "Isolette") — MusicBrainz may match exact casing
        if not candidates and title_raw and (title_raw.strip() != album_norm):
            raw_clean = title_raw.strip()
            if raw_clean:
                _run_search_and_browse(raw_clean)

        all_fetched: List[tuple] = []

        # IA-first: ask AI to pick one candidate from titles only, then fetch only that one (reduces MB requests)
        if USE_AI_FOR_MB_VERIFY and getattr(sys.modules[__name__], "ai_provider_ready", False) and candidates:
            letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            choices = [f"{letters[i]}) {rg.get('title', '?')} (id={rg.get('id', '')})" for i, rg in enumerate(candidates[:20])]
            track_list_str = ", ".join((list(tracks)[:30] if tracks else [])) if tracks else "(none)"
            if tracks and len(tracks) > 30:
                track_list_str += ", ..."
            prompt = (
                f"Our album: artist={artist!r}, title_raw={title_raw or album_norm!r}, normalized={album_norm!r}. "
                f"Our track titles: [{track_list_str}].\n\n"
                "MusicBrainz candidates (id + title only):\n" + "\n".join(choices) + "\n\n"
                "Which candidate matches our album? Consider title variants (e.g. Volume I = volume i). "
                "Reply with only the letter (A, B, ...) or the MBID (UUID) or NONE if no match."
            )
            system_msg = "You reply with a single letter (A, B, C, ...) or an MBID (UUID) or the word NONE. No explanation."
            try:
                provider = getattr(sys.modules[__name__], "AI_PROVIDER", "openai")
                model = getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4o-mini")
                reply = call_ai_provider(provider, model, system_msg, prompt, max_tokens=30)
                reply = (reply or "").strip().upper()
                chosen_rg_id = None
                if reply and reply != "NONE":
                    letter = reply[:1]
                    idx = letters.find(letter)
                    if 0 <= idx < len(candidates):
                        chosen_rg_id = candidates[idx].get("id")
                    if not chosen_rg_id:
                        mbid_match = re.search(r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}", reply)
                        if mbid_match:
                            chosen_rg_id = mbid_match.group(0)
                if chosen_rg_id:
                    try:
                        info = _fetch_rg_details(chosen_rg_id)
                        mb_track_count = _mb_track_count_from_rg_info(info)
                        # Sanity check: reject if local track count differs too much from MB
                        local_count = len(tracks) if tracks else 0
                        if abs(local_count - mb_track_count) > 2:
                            logging.debug("[MusicBrainz] AI-first chosen %s rejected: track count mismatch (local=%d, mb=%d)", chosen_rg_id, local_count, mb_track_count)
                            # Fall through to full flow; if still no match, scan will try Discogs/Last.fm/Bandcamp for this album
                        else:
                            formats = set()
                            for release in info.get("release-list", []):
                                for medium in release.get("medium-list", []):
                                    fmt = medium.get("format")
                                    qty = medium.get("track-count", 1)
                                    if fmt:
                                        formats.add(f"{qty}×{fmt}")
                            rg_id_final = info.get("id", chosen_rg_id)
                            result_dict = {
                                "primary_type": info.get("primary-type", ""),
                                "secondary_types": info.get("secondary-types", []),
                                "format_summary": ", ".join(sorted(formats)),
                                "id": rg_id_final,
                                "track_count": mb_track_count,
                                "cover_url": f"https://coverartarchive.org/release-group/{rg_id_final}/front",
                            }
                            set_cached_mb_info(rg_id_final, result_dict)
                            logging.info("[MusicBrainz] Match chosen by AI (title-only) for artist=%r album=%r: %s", artist, album_norm, rg_id_final)
                            return (result_dict, True)
                    except musicbrainzngs.WebServiceError as e:
                        logging.debug("[MusicBrainz] AI-first fetch failed for %s: %s", chosen_rg_id, e)
            except Exception as e:
                logging.debug("[MusicBrainz] AI-first path failed: %s", e)
            # Fall through to full flow (fetch all candidates, then ai_verify_mb_match)

        for rg in candidates:
            try:
                rg_id = rg['id']
                if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
                    info = get_mb_queue().submit(f"rg_{rg_id}", lambda rid=rg_id: _fetch_rg_details(rid))
                else:
                    info = _fetch_rg_details(rg_id)
                mb_track_count = _mb_track_count_from_rg_info(info)
                formats = set()
                for release in info.get('release-list', []):
                    for medium in release.get('medium-list', []):
                        fmt = medium.get('format')
                        qty = medium.get('track-count', 1)
                        if fmt:
                            formats.add(f"{qty}×{fmt}")
                rg_id_final = info.get('id', rg['id'])
                result_dict = {
                    'primary_type': info.get('primary-type', ''),
                    'secondary_types': info.get('secondary-types', []),
                    'format_summary': ', '.join(sorted(formats)),
                    'id': rg_id_final,
                    'track_count': mb_track_count,
                    # Phase 4: Cover Art Archive URL for optional vision comparison
                    'cover_url': f"https://coverartarchive.org/release-group/{rg_id_final}/front",
                }
                # Phase 2: optionally fetch track titles from one release for AI verify prompt
                if USE_AI_FOR_MB_VERIFY and info.get('release-list'):
                    first_release_id = info['release-list'][0].get('id')
                    if first_release_id:
                        try:
                            def _fetch_release_recordings(rel_id: str):
                                return musicbrainzngs.get_release_by_id(rel_id, includes=["recordings"])
                            if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
                                rel_resp = get_mb_queue().submit(f"rel_rec_{first_release_id}", lambda rid=first_release_id: _fetch_release_recordings(rid))
                            else:
                                rel_resp = _fetch_release_recordings(first_release_id)
                            result_dict['track_titles'] = _extract_track_titles_from_mb_release(rel_resp)
                        except musicbrainzngs.WebServiceError:
                            pass
                all_fetched.append((rg, result_dict))
            except musicbrainzngs.WebServiceError:
                continue

        # Partie 2+3: When no MusicBrainz candidates, try Bandcamp + web search + AI to suggest MBID
        if not all_fetched and getattr(sys.modules[__name__], "ai_provider_ready", False):
            title_for_zero = (title_raw or album_norm or "").strip()
            bandcamp_text = ""
            if USE_BANDCAMP and title_for_zero:
                try:
                    bc = _fetch_bandcamp_album_info(artist, title_for_zero)
                    if bc:
                        bandcamp_text = f"Bandcamp: title={bc.get('title')}, artist={bc.get('artist_name')}"
                except Exception:
                    pass
            web_snippets = ""
            if USE_WEB_SEARCH_FOR_MB and SERPER_API_KEY.strip() and title_for_zero:
                q = f"{artist} {title_for_zero} album"
                web_results = _web_search_serper(q, num=10)
                if web_results:
                    parts = [f"Web {i+1}) {r.get('title')} — {r.get('snippet')} ({r.get('link')})" for i, r in enumerate(web_results[:10])]
                    web_snippets = "\n".join(parts)
            if bandcamp_text or web_snippets:
                prompt = f"Our album: artist={artist}, title={title_for_zero}. No MusicBrainz candidates.\n"
                if bandcamp_text:
                    prompt += bandcamp_text + "\n"
                if web_snippets:
                    prompt += "Web results:\n" + web_snippets + "\n"
                prompt += "Suggest a MusicBrainz release group ID (MBID, UUID format) if you can identify it from the above, or reply exactly NONE."
                try:
                    provider = getattr(sys.modules[__name__], "AI_PROVIDER", "openai")
                    model = getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4o-mini")
                    reply = call_ai_provider(provider, model, "You reply with a single MBID (UUID) or the word NONE.", prompt, max_tokens=60)
                    reply = (reply or "").strip().upper()
                    if reply and reply != "NONE":
                        mbid_match = re.search(r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}", reply)
                        if mbid_match:
                            suggested_mbid = mbid_match.group(0)
                            try:
                                info = _fetch_rg_details(suggested_mbid)
                                mb_track_count = _mb_track_count_from_rg_info(info)
                                formats = set()
                                for release in info.get("release-list", []):
                                    for medium in release.get("medium-list", []):
                                        fmt = medium.get("format")
                                        qty = medium.get("track-count", 1)
                                        if fmt:
                                            formats.add(f"{qty}×{fmt}")
                                rg_id_final = info.get("id", suggested_mbid)
                                result_dict = {
                                    "primary_type": info.get("primary-type", ""),
                                    "secondary_types": info.get("secondary-types", []),
                                    "format_summary": ", ".join(sorted(formats)),
                                    "id": rg_id_final,
                                    "track_count": mb_track_count,
                                    "cover_url": f"https://coverartarchive.org/release-group/{rg_id_final}/front",
                                }
                                set_cached_mb_info(rg_id_final, result_dict)
                                logging.info("[MusicBrainz] MBID suggested by AI from Bandcamp/web for artist=%r album=%r: %s", artist, album_norm, rg_id_final)
                                return (result_dict, True)
                            except musicbrainzngs.WebServiceError:
                                logging.debug("[MusicBrainz] AI-suggested MBID %s invalid or 404", suggested_mbid)
                except Exception as e:
                    logging.debug("[MusicBrainz] Bandcamp/web AI suggestion failed: %s", e)
            return (None, False)

        matching: List[tuple] = [
            x for x in all_fetched
            if not tracks or abs(len(tracks) - x[1].get('track_count', 0)) <= 1
        ]

        if USE_AI_FOR_MB_VERIFY and all_fetched:
            # Phase 3: optional Discogs/Last.fm/Bandcamp for AI disambiguation
            extra_sources: List[dict] = []
            title_for_fetch = (title_raw or album_norm or "").strip()
            if title_for_fetch:
                try:
                    discogs_info = _fetch_discogs_release(artist, title_for_fetch)
                    if discogs_info:
                        extra_sources.append({"source": "Discogs", "title": discogs_info.get("title"), "artist_name": discogs_info.get("artist_name")})
                except Exception:
                    pass
                try:
                    lastfm_info = _fetch_lastfm_album_info(artist, title_for_fetch)
                    if lastfm_info:
                        extra_sources.append({"source": "Last.fm", "title": lastfm_info.get("title"), "artist": lastfm_info.get("artist")})
                except Exception:
                    pass
                try:
                    bandcamp_info = _fetch_bandcamp_album_info(artist, title_for_fetch)
                    if bandcamp_info:
                        extra_sources.append({"source": "Bandcamp", "title": bandcamp_info.get("title"), "artist_name": bandcamp_info.get("artist_name")})
                except Exception:
                    pass
            chosen = ai_verify_mb_match(
                artist, title_raw, album_norm,
                list(tracks) if tracks else None,
                len(tracks) if tracks else 0,
                all_fetched,
                has_cover=False,
                extra_sources=extra_sources or None,
            )
            if chosen:
                # Partie 1: Optional vision check (local cover vs Cover Art Archive)
                use_vision = getattr(sys.modules[__name__], "USE_AI_VISION_FOR_COVER", False) and album_folder
                if use_vision:
                    cover_path = _first_cover_path(Path(album_folder) if not isinstance(album_folder, Path) else album_folder)
                    local_data_uri = _encode_local_cover_to_data_uri(cover_path) if cover_path else None
                    mb_cover_url = chosen[1].get("cover_url")
                    if local_data_uri and mb_cover_url:
                        try:
                            vision_model = getattr(sys.modules[__name__], "OPENAI_VISION_MODEL", "") or getattr(sys.modules[__name__], "RESOLVED_MODEL", "gpt-4o-mini")
                            sys_msg = "You reply with exactly Yes or No."
                            user_msg = "Image 1 is the local album cover. Image 2 is the MusicBrainz Cover Art Archive cover. Do they represent the same album? Reply only: Yes or No."
                            resp = call_ai_provider_vision(
                                getattr(sys.modules[__name__], "AI_PROVIDER", "openai"),
                                vision_model,
                                sys_msg,
                                user_msg,
                                image_urls=[mb_cover_url],
                                image_base64=[{"type": "image_url", "image_url": {"url": local_data_uri}}],
                                max_tokens=10,
                            )
                            if resp and "NO" in (resp.strip().upper()):
                                chosen = None
                                logging.info("[MusicBrainz Vision] Cover mismatch: rejecting AI-chosen match for artist=%r album=%r", artist, album_norm)
                        except Exception as e:
                            logging.debug("[MusicBrainz Vision] Vision check failed: %s", e)
                if chosen:
                    set_cached_mb_info(chosen[1]['id'], chosen[1])
                    logging.info("[MusicBrainz Verify] Match verified by AI for artist=%r album=%r", artist, album_norm)
                    return (chosen[1], True)
            # Partie 2 Cas 2: AI said NONE but we have candidates; try web search + one more AI call
            if chosen is None and all_fetched and USE_WEB_SEARCH_FOR_MB and SERPER_API_KEY.strip():
                title_for_web = (title_raw or album_norm or "").strip()
                if title_for_web:
                    q = f"{artist} {title_for_web} album"
                    web_results = _web_search_serper(q, num=10)
                    if web_results:
                        web_snippets = "\n".join([f"Web {i+1}) {r.get('title')} — {r.get('snippet')} ({r.get('link')})" for i, r in enumerate(web_results[:10])])
                        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                        choices = [f"{letters[i]}: {all_fetched[i][0].get('title', '?')} (MBID: {all_fetched[i][1].get('id')})" for i in range(min(len(all_fetched), 10))]
                        prompt = f"Artist: {artist}. Album: {title_for_web}. We had MusicBrainz candidates but none matched. Web results:\n{web_snippets}\n\nCandidates: " + " | ".join(choices)
                        prompt += "\nReply with the letter (A/B/...) of the correct candidate, or an MBID (UUID) if you find one in web results, or NONE."
                        try:
                            provider = getattr(sys.modules[__name__], "AI_PROVIDER", "openai")
                            model = getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4o-mini")
                            reply = call_ai_provider(provider, model, "You reply with a single letter, or an MBID (UUID), or NONE.", prompt, max_tokens=60)
                            reply = (reply or "").strip().upper()
                            if reply and reply != "NONE":
                                letter = reply[:1]
                                idx = letters.find(letter)
                                if 0 <= idx < len(all_fetched):
                                    set_cached_mb_info(all_fetched[idx][1]['id'], all_fetched[idx][1])
                                    logging.info("[MusicBrainz Verify] Web+AI chose candidate %s for artist=%r album=%r", letter, artist, album_norm)
                                    return (all_fetched[idx][1], True)
                                mbid_match = re.search(r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}", reply)
                                if mbid_match:
                                    suggested_mbid = mbid_match.group(0)
                                    try:
                                        info = _fetch_rg_details(suggested_mbid)
                                        mb_track_count = _mb_track_count_from_rg_info(info)
                                        formats = set()
                                        for release in info.get("release-list", []):
                                            for medium in release.get("medium-list", []):
                                                fmt, qty = medium.get("format"), medium.get("track-count", 1)
                                                if fmt:
                                                    formats.add(f"{qty}×{fmt}")
                                        rg_id_final = info.get("id", suggested_mbid)
                                        result_dict = {"primary_type": info.get("primary-type", ""), "secondary_types": info.get("secondary-types", []), "format_summary": ", ".join(sorted(formats)), "id": rg_id_final, "track_count": mb_track_count, "cover_url": f"https://coverartarchive.org/release-group/{rg_id_final}/front"}
                                        set_cached_mb_info(rg_id_final, result_dict)
                                        logging.info("[MusicBrainz] Web+AI suggested MBID %s for artist=%r album=%r", rg_id_final, artist, album_norm)
                                        return (result_dict, True)
                                    except musicbrainzngs.WebServiceError:
                                        pass
                        except Exception as e:
                            logging.debug("[MusicBrainz] Web+AI second call failed: %s", e)
            if not matching:
                if candidates:
                    detail = ", ".join(f"{rg.get('title', '?')} ({rg.get('id', '')})" for rg in candidates[:10])
                    if len(candidates) > 10:
                        detail += f" ... and {len(candidates) - 10} more"
                    logging.debug(
                        "[MusicBrainz Search] artist=%r release=%r: AI said NONE or failed, no track-count match: %s",
                        artist, album_norm, detail,
                    )
                return (None, False)

        if not matching:
            if candidates:
                detail = ", ".join(f"{rg.get('title', '?')} ({rg.get('id', '')})" for rg in candidates[:10])
                if len(candidates) > 10:
                    detail += f" ... and {len(candidates) - 10} more"
                logging.debug(
                    "[MusicBrainz Search] artist=%r release=%r: %d candidate(s) but none matched (track count or fetch failed): %s",
                    artist, album_norm, len(candidates), detail,
                )
            return (None, False)
        if len(matching) == 1:
            set_cached_mb_info(matching[0][1]['id'], matching[0][1])
            return (matching[0][1], False)

        if len(matching) >= 2 and USE_AI_FOR_MB_MATCH:
            letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            choices = [f"{letters[i]}: {m[0].get('title', 'Unknown')}" for i, m in enumerate(matching[:10])]
            prompt = f"Artist: {artist}. Album we have: {album_norm}. Which MusicBrainz release-group title is the same release? Reply with only the letter.\n" + "\n".join(choices)
            try:
                reply = call_ai_provider(AI_PROVIDER, RESOLVED_MODEL or OPENAI_MODEL, "You reply with a single letter.", prompt, max_tokens=10)
                letter = (reply or "").strip().upper()[:1]
                idx = letters.find(letter)
                if 0 <= idx < len(matching):
                    set_cached_mb_info(matching[idx][1]['id'], matching[idx][1])
                    return (matching[idx][1], False)
            except Exception as e:
                logging.debug("[MusicBrainz Search] AI pick failed: %s", e)

        set_cached_mb_info(matching[0][1]['id'], matching[0][1])
        return (matching[0][1], False)
    except Exception as e:
        logging.debug("[MusicBrainz Search Groups] failed for '%s' / '%s': %s", artist, album_norm, e)
    return (None, False)


def process_ai_groups_batch(ai_groups: List[dict], max_workers: int = None) -> List[dict]:
    """
    Process multiple groups requiring AI in parallel.
    Returns list of completed group dicts with 'best' and 'losers' set.
    """
    if not ai_groups:
        return []
    
    if max_workers is None:
        max_workers = min(AI_BATCH_SIZE, len(ai_groups))
    
    def process_group(group):
        """Process a single group requiring AI."""
        editions = group["editions"]
        artist = group["artist"]
        fuzzy = group.get("fuzzy", False)
        
        try:
            # Call choose_best without defer_ai to actually process
            best = choose_best(editions, defer_ai=False)
            # choose_best should never return None when defer_ai=False
            # (it will use heuristic if AI is not available)
            if best is None:
                raise ValueError("choose_best returned None unexpectedly")
            
            losers = [e for e in editions if e['album_id'] != best['album_id']]
            
            return {
                "artist": artist,
                "album_id": best["album_id"],
                "best": best,
                "losers": losers,
                "fuzzy": fuzzy,
                "needs_ai": False,
            }
        except Exception as e:
            logging.error(f"[AI Batch] Error processing group for {artist}: {e}", exc_info=True)
            group_label = f"{artist} – {editions[0].get('title_raw', editions[0].get('album_norm', ''))}" if editions else artist
            with lock:
                state.setdefault("scan_ai_errors", []).append({"message": str(e), "group": group_label})
                if len(state["scan_ai_errors"]) > 100:
                    state["scan_ai_errors"] = state["scan_ai_errors"][-80:]
            # No heuristic fallback: AI-only. Skip this group so it does not appear as "Heuristic".
            return None

    def group_label(g):
        if not g.get("editions"):
            return g.get("artist", "")
        return f"{g['artist']} – {g['editions'][0].get('title_raw', g['editions'][0].get('album_norm', ''))}"
    
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_group, group): group for group in ai_groups}
        try:
            for future in as_completed(futures, timeout=900):
                group = futures[future]
                label = group_label(group)
                try:
                    result = future.result(timeout=60)
                    if result:
                        results.append(result)
                except Exception as e:
                    logging.error(f"[AI Batch] Failed to process group for {group['artist']}: {e}", exc_info=True)
                    with lock:
                        state.setdefault("scan_ai_errors", []).append({"message": str(e), "group": label})
                        if len(state["scan_ai_errors"]) > 100:
                            state["scan_ai_errors"] = state["scan_ai_errors"][-80:]
                with lock:
                    state["scan_ai_batch_processed"] = state.get("scan_ai_batch_processed", 0) + 1
                    state["scan_ai_current_label"] = label
        except TimeoutError:
            logging.warning("[AI Batch] Timeout waiting for all groups; saving partial results.")
    
    return results

def choose_best(editions: List[dict], defer_ai: bool = False) -> dict | None:
    """
    Selects the best edition either via AI provider (when configured) or via a local heuristic, re‑using any existing AI cache first.
    Broken albums (missing tracks) automatically lose against non-broken albums.
    
    Args:
        editions: List of edition dicts to choose from
        defer_ai: If True and AI is needed, return None instead of calling AI (for batch processing)
    
    Returns:
        Best edition dict, or None if defer_ai=True and AI call is needed
    """
    import sqlite3, json

    # 0) Filter out broken albums if there are non-broken alternatives
    non_broken = [e for e in editions if not e.get('is_broken', False)]
    broken = [e for e in editions if e.get('is_broken', False)]
    
    if non_broken and broken:
        # If we have both broken and non-broken, only consider non-broken
        logging.info(
            "[choose_best] Filtering out %d broken album(s) in favor of %d non-broken album(s)",
            len(broken), len(non_broken)
        )
        editions = non_broken
    
    # 1) Re‑use a previously stored AI choice
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    ids = tuple(e['album_id'] for e in editions)
    placeholders = ",".join("?" for _ in ids)
    # assume all editions have the same artist
    artist = editions[0]['artist']
    cur.execute(
        f"SELECT album_id, rationale, merge_list, ai_provider, ai_model "
        f"FROM duplicates_best "
        f"WHERE artist = ? AND album_id IN ({placeholders}) AND ai_used = 1",
        (artist, ) + ids
    )
    row = cur.fetchone()
    con.close()
    existing_list = json.loads(row[2]) if row and row[2] else []
    # Only reuse cache if no new editions have appeared
    if row and (len(existing_list) + 1) == len(editions):
        prev_id, rationale, merge_json = row[0], row[1], row[2]
        best = next(e for e in editions if e['album_id'] == prev_id)
        best["rationale"]  = rationale
        best["merge_list"] = existing_list
        best["used_ai"]    = True
        best["ai_provider"] = (row[3] or "") if len(row) > 3 else ""
        best["ai_model"]   = (row[4] or "") if len(row) > 4 else ""
        return best

    # 2) If there is no AI cache, call AI provider when possible
    used_ai = False
    if ai_provider_ready:
        if defer_ai:
            # Return None to indicate AI call is needed (will be processed in batch)
            return None
        used_ai = True
        logging.info("[choose_best] Using AI for edition selection (artist=%s, %d editions)", artist, len(editions))
        # Load prompt template from external file
        template = AI_PROMPT_FILE.read_text(encoding="utf-8")
        user_msg = template + "\nCandidate editions:\n"
        for idx, e in enumerate(editions):
            track_count = len(e.get("tracks", []))
            folder_path = path_for_fs_access(Path(e["folder"])) if e.get("folder") else None
            size_mb = (safe_folder_size(folder_path) // (1024 * 1024)) if folder_path else 0
            user_msg += (
                f"{idx}: fmt_score={e['fmt_score']}, bitdepth={e['bd']}, "
                f"tracks={track_count}, track_count={track_count}, size_mb={size_mb}, files={e['file_count']}, "
                f"bitrate={e['br']}, samplerate={e['sr']}, duration={e['dur']}"
            )
            # Add year and mbid tags from meta
            year = e["meta"].get("date") or e["meta"].get("originaldate") or ""
            mbid = e["meta"].get("musicbrainz_albumid","")
            user_msg += f" year={year} mbid={mbid}\n"

        # --- Append MusicBrainz release-group info to AI prompt ---
        rg_info = None
        for e in editions:
            if e.get("rg_info"):
                rg_info = e["rg_info"]
                break
        if not rg_info:
            first = editions[0]
            rgid = first.get("musicbrainz_id")
            meta = first.get("meta", {})
            if not rgid:
                rgid = meta.get("musicbrainz_releasegroupid")
            if rgid:
                try:
                    rg_info, _ = fetch_mb_release_group_info(rgid)
                except Exception as e:
                    logging.debug("MusicBrainz lookup failed for rgid %s: %s", rgid, e)
            if not rgid:
                release_mbid = meta.get("musicbrainz_albumid") or meta.get("musicbrainz_releaseid")
                tag_src = "musicbrainz_albumid" if meta.get("musicbrainz_albumid") else "musicbrainz_releaseid"
                if release_mbid:
                    rgid = resolve_mbid_to_release_group(release_mbid, tag_src)
                    if rgid:
                        try:
                            rg_info, _ = fetch_mb_release_group_info(rgid)
                        except Exception as e:
                            logging.debug("MusicBrainz lookup failed for resolved rgid %s: %s", rgid, e)
        if rg_info:
            user_msg += f"Release group info: primary_type={rg_info.get('primary_type', '')}, formats={rg_info.get('format_summary', '')}\n"

        system_msg = (
            "You are an expert digital-music librarian.\n"
            "OUTPUT RULES (must follow exactly):\n"
            "- Return ONE single line only.\n"
            "- The line must contain EXACTLY two '|' characters.\n"
            "- Format: <index>|<brief rationale>|<comma-separated extra tracks>\n"
            "- If there are no extra tracks, still include the final pipe but leave it empty.\n"
            "- Do not add any other text, do not explain, do not add extra lines.\n"
            "Example of valid outputs:\n"
            "2|Winner has: - [CLASSICAL:NO] lossless FLAC, 24-bit, more tracks - Higher bitrate than 1|Track 12 - Live bonus\n"
            "1|Winner has: - [CLASSICAL:YES] same interpretation (shared MBID) - More complete edition, 9 vs 7 tracks|"
        )
        
        # Determine model to use based on provider
        # Use RESOLVED_MODEL for OpenAI (the one that passed functional probe); fallback to OPENAI_MODEL only if unset
        mod = sys.modules[__name__]
        model_to_use = getattr(mod, "RESOLVED_MODEL", None) or OPENAI_MODEL
        if not model_to_use:
            # Set defaults if no model selected
            if AI_PROVIDER.lower() == "anthropic":
                model_to_use = "claude-3-5-sonnet-20241022"
            elif AI_PROVIDER.lower() == "google":
                model_to_use = "gemini-1.5-pro"
            elif AI_PROVIDER.lower() == "ollama":
                model_to_use = "llama2"  # Common default Ollama model
            else:
                model_to_use = "gpt-4o-mini"
        
        # Log concise AI request summary
        logging.info(
            "AI request (%s): model=%s, max_out=256, candidate_editions=%d",
            AI_PROVIDER, model_to_use, len(editions)
        )
        
        try:
            txt = call_ai_provider(AI_PROVIDER, model_to_use, system_msg, user_msg, max_tokens=256)
            logging.debug("AI raw response: %s", txt)

            # --- sanitize: keep only first non-empty line, strip code fences / prefixes ---
            lines = [l.strip() for l in txt.replace("```", "").splitlines() if l.strip()]
            txt = lines[0] if lines else txt
            txt = re.sub(r'^(answer|réponse)\s*:\s*', '', txt, flags=re.IGNORECASE).strip()

            # --- try strict format: <index>|<rationale>|<extras> ---
            m = re.match(r'^(\d+)\s*\|\s*(.*?)\s*\|\s*(.*)$', txt)
            if m:
                idx = int(m.group(1))
                if not (0 <= idx < len(editions)):
                    raise ValueError(f"AI index out of range: {idx} / {len(editions)}")
                rationale = m.group(2).strip()
                extras_raw = m.group(3).strip()
                merge_list = [t.strip() for t in extras_raw.split(',') if t.strip()]
            else:
                # --- tolerant fallback: accept a lone index or any line containing a number ---
                m_num = re.search(r'(\d+)', txt)
                if not m_num:
                    raise ValueError(f"Invalid AI response format (no index found) – got: {txt!r}")
                idx = int(m_num.group(1))
                if not (0 <= idx < len(editions)):
                    raise ValueError(f"AI index out of range: {idx} / {len(editions)}")
                rationale = "minimal AI reply; fallback parser used"
                merge_list = []

            logging.debug("AI parsed result -> idx=%s rationale=%r extras=%r", idx, rationale, merge_list)

            best = editions[idx]
            model_display = getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or OPENAI_MODEL or model_to_use
            best.update({
                "rationale":  rationale,
                "merge_list": merge_list,
                "used_ai":    True,
                "ai_provider": (AI_PROVIDER or ""),
                "ai_model":   (model_display or ""),
            })
        except Exception as e:
            logging.warning("AI failed (%s); no heuristic fallback (AI required). Skipping group.", e)
            group_label = f"{artist} – {editions[0].get('title_raw', editions[0].get('album_norm', ''))}" if editions else (artist or "")
            with lock:
                state.setdefault("scan_ai_errors", []).append({"message": str(e), "group": group_label})
                if len(state["scan_ai_errors"]) > 100:
                    state["scan_ai_errors"] = state["scan_ai_errors"][-80:]
            return None

    # 3) AI is required; no heuristic path. If we reach here without used_ai, AI was not configured.
    if not used_ai:
        logging.info(
            "[choose_best] AI required but not ready (artist=%s, %d editions); skipping group.",
            artist, len(editions),
        )
        return None

    # 4) Persist choice to DB (INSERT OR IGNORE so we never overwrite an existing cache)
    best_folder_path = path_for_fs_access(Path(best["folder"])) if best.get("folder") else None
    best_size_mb = (safe_folder_size(best_folder_path) // (1024 * 1024)) if best_folder_path else 0
    best_track_count = len(best.get("tracks", []))
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO duplicates_best
          (artist, album_id, title_raw, album_norm, folder, fmt_text,
           br, sr, bd, dur, discs, rationale, merge_list, ai_used, meta_json, ai_provider, ai_model, size_mb, track_count, match_verified_by_ai)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        json.dumps(best.get("meta", {})),
        best.get("ai_provider", ""),
        best.get("ai_model", ""),
        best_size_mb,
        best_track_count,
        1 if best.get("match_verified_by_ai") else 0,
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
    # Check for any other literal uses of the French header and replace
    # (This is for the extremely rare case where it appears elsewhere in this function.)

# --- Remove any other temperature=0.0 from openai_client.chat.completions.create calls in this file ---

def scan_artist_duplicates(args):
    """
    ThreadPool worker: scan one artist for duplicate albums.
    args: (artist_id, artist_name) or (artist_id, artist_name, album_ids).
    When album_ids is provided (e.g. merged from multiple Plex artist entries with same name),
    use it so duplicates across folders (e.g. Ochre vs Ochre2) are detected.
    Returns (artist_name, list_of_groups, album_count, stats_dict).
    stats_dict contains: {"ai_used": count, "mb_used": count, "timing": {...}}
    """
    artist_id = args[0]
    artist_name = args[1]
    album_ids = args[2] if len(args) >= 3 else None
    artist_start_time = time.perf_counter()
    timing_stats = {
        "db_query_time": 0.0,
        "audio_analysis_time": 0.0,
        "mb_lookup_time": 0.0,
        "ai_processing_time": 0.0,
        "total_time": 0.0,
    }
    try:
        if scan_should_stop.is_set():
            return (artist_name, [], 0, {"ai_used": 0, "mb_used": 0, "timing": timing_stats}, [])
        while scan_is_paused.is_set() and not scan_should_stop.is_set():
            time.sleep(0.5)
        logging.info("Processing artist: %s", artist_name)
        db_conn = plex_connect()

        if album_ids is None:
            logging.debug("[Artist %s (ID %s)] Fetching album IDs from Plex DB", artist_name, artist_id)
            db_query_start = time.perf_counter()
            placeholders = ",".join("?" for _ in SECTION_IDS)
            if CROSS_LIBRARY_DEDUPE:
                section_filter = ""
                section_args = []
            else:
                section_filter = f"AND alb.library_section_id IN ({placeholders})"
                section_args = list(SECTION_IDS)

            cursor = db_conn.execute(
                f"""
                SELECT alb.id
                FROM metadata_items alb
                JOIN metadata_items tr  ON tr.parent_id      = alb.id
                JOIN media_items      mi ON mi.metadata_item_id = tr.id
                JOIN media_parts      mp ON mp.media_item_id = mi.id
                WHERE alb.metadata_type = 9
                  AND alb.parent_id = ?
                  {section_filter}
                GROUP BY alb.id
                """,
                (artist_id, *section_args)
            )
            album_ids = [row[0] for row in cursor.fetchall()]
            timing_stats["db_query_time"] = time.perf_counter() - db_query_start
        logging.debug("[Artist %s (ID %s)] Album list for scan: %d albums", artist_name, artist_id, len(album_ids))

        # Update total_albums in active tracking
        with lock:
            if artist_name in state.get("scan_active_artists", {}):
                state["scan_active_artists"][artist_name]["total_albums"] = len(album_ids)

        groups = []
        stats = {"ai_used": 0, "mb_used": 0, "timing": {}}
        all_editions_for_stats = []
        if album_ids:
            groups, stats, all_editions_for_stats = scan_duplicates(db_conn, artist_name, album_ids)
            # Merge timing stats
            if "timing" in stats:
                timing_stats.update(stats["timing"])
        db_conn.close()
        
        timing_stats["total_time"] = time.perf_counter() - artist_start_time
        stats["timing"] = timing_stats

        logging.debug(
            "scan_artist_duplicates(): done Artist %s (ID %s) – %d groups, %d albums, AI=%d, MB=%d, "
            "timing: total=%.2fs, db=%.2fs, audio=%.2fs, mb=%.2fs, ai=%.2fs",
            artist_name, artist_id, len(groups), len(album_ids), 
            stats.get("ai_used", 0), stats.get("mb_used", 0),
            timing_stats["total_time"], timing_stats["db_query_time"],
            timing_stats["audio_analysis_time"], timing_stats["mb_lookup_time"],
            timing_stats["ai_processing_time"]
        )
        return (artist_name, groups, len(album_ids), stats, all_editions_for_stats)
    except Exception as e:
        logging.error("Unexpected error scanning artist %s: %s", artist_name, e, exc_info=True)
        # On error, return no groups and zero albums so scan can continue
        timing_stats["total_time"] = time.perf_counter() - artist_start_time
        return (artist_name, [], 0, {"ai_used": 0, "mb_used": 0, "timing": timing_stats}, [])


def scan_duplicates(db_conn, artist: str, album_ids: List[int]) -> tuple[List[dict], dict, list]:
    global no_file_streak_global, popup_displayed, gui
    scan_start_time = time.perf_counter()
    logging.debug("[Artist %s] Starting duplicate scan for album IDs: %s", artist, album_ids)
    logging.debug("Verbose SKIP_FOLDERS: %s", SKIP_FOLDERS)
    skip_count = 0
    editions = []
    total_albums = len(album_ids)
    processed_albums = 0
    PROGRESS_STATE["total"] = total_albums
    # Track folders and all album_ids pointing to each (for same-folder duplicate detection)
    seen_folders: dict[str, list[int]] = {}  # folder_path_resolved -> [album_id, ...]
    # Performance timing
    audio_analysis_time = 0.0
    mb_lookup_time = 0.0
    ai_processing_time = 0.0
    for aid in album_ids:
        processed_albums += 1
        PROGRESS_STATE["current"] = processed_albums
        # Periodic progress update every 100 albums
        if processed_albums % 100 == 0:
            logging.info("[Artist %s] processed %d/%d albums (skipped %d so far)", artist, processed_albums, total_albums, skip_count)
        try:
            if scan_should_stop.is_set():
                break
            while scan_is_paused.is_set() and not scan_should_stop.is_set():
                time.sleep(0.5)
            
            # Update current album tracking and albums_processed so UI shows progress during long artist scan
            with lock:
                if artist in state.get("scan_active_artists", {}):
                    state["scan_active_artists"][artist]["albums_processed"] = processed_albums
                    album_title_str = album_title(db_conn, aid) or f"Album {aid}"
                    state["scan_active_artists"][artist]["current_album"] = {
                        "album_id": aid,
                        "album_title": album_title_str,
                        "status": "fetching_tracks",
                        "status_details": "",
                        "step_summary": "",
                        "step_response": ""
                    }
            
            tr = get_tracks(db_conn, aid)
            if not tr:
                continue
            
            # Update: analyzing format
            with lock:
                if artist in state.get("scan_active_artists", {}):
                    state["scan_active_artists"][artist]["current_album"]["status"] = "analyzing_format"
                    state["scan_active_artists"][artist]["current_album"]["status_details"] = "analyzing audio format"
                    state["scan_active_artists"][artist]["current_album"]["step_summary"] = "Running FFprobe…"
            
            folder = first_part_path(db_conn, aid)
            if not folder:
                continue
            # Skip albums in configured skip folders (path-aware)
            logging.debug("Checking album %s at folder %s against skip prefixes %s", aid, folder, SKIP_FOLDERS)
            folder_resolved = Path(folder).resolve()
            folder_str_resolved = str(folder_resolved)
            
            # Track same-folder duplicates: multiple Plex album entries pointing to the same folder
            if folder_str_resolved in seen_folders:
                seen_folders[folder_str_resolved].append(aid)
                logging.warning(
                    "[Artist %s] Album ID %d points to the same folder as album ID(s) %s: %s. "
                    "Same-folder duplicate (Plex metadata). Will report as duplicate group.",
                    artist, aid, seen_folders[folder_str_resolved], folder_str_resolved
                )
                skip_count += 1
                continue

            # First time we see this folder: record and process
            seen_folders[folder_str_resolved] = [aid]
            
            if SKIP_FOLDERS and any(folder_resolved.is_relative_to(Path(s).resolve()) for s in SKIP_FOLDERS):
                skip_count += 1
                logging.info("Skipping album %s since folder %s matches skip prefixes %s", aid, folder_resolved, SKIP_FOLDERS)
                continue
            # count audio files once – we re‑use it later
            file_count = sum(1 for f in folder.rglob("*") if AUDIO_RE.search(f.name))

            # consider edition invalid when technical data are all zero OR no files found

            # ─── audio‑format inspection ──────────────────────────────────────
            audio_start = time.perf_counter()
            fmt_score, br, sr, bd, audio_cache_hit = analyse_format(folder)
            audio_analysis_time += time.perf_counter() - audio_start

            # --- metadata tags (first track only) -----------------------------
            first_audio = next((p for p in folder.rglob("*") if AUDIO_RE.search(p.name)), None)
            meta_tags = extract_tags(first_audio) if first_audio else {}

            # Mark as invalid if file_count == 0 OR all tech data are zero
            is_invalid = (file_count == 0) or (br == 0 and sr == 0 and bd == 0)

            # --- Quick retry before purging to avoid false negatives -------------
            if is_invalid:
                time.sleep(0.5)
                fmt_score_retry, br_retry, sr_retry, bd_retry, audio_cache_hit_retry = analyse_format(folder)
                file_count_retry = file_count or sum(1 for f in folder.rglob("*") if AUDIO_RE.search(f.name))
                if (file_count_retry == 0) or (br_retry == 0 and sr_retry == 0 and bd_retry == 0):
                    _purge_invalid_edition({
                        "folder":   folder,
                        "artist":   artist,
                        "title_raw": album_title(db_conn, aid),
                        "album_id": aid
                    })
                    continue            # do NOT add to the editions list
                else:
                    fmt_score, br, sr, bd, audio_cache_hit = fmt_score_retry, br_retry, sr_retry, bd_retry, audio_cache_hit_retry
                    is_invalid = False

            plex_title = album_title(db_conn, aid)
            title_raw, title_source = derive_album_title(plex_title, meta_tags, folder, aid)
            normalize_parenthetical = bool(_parse_bool(_get_config_from_db("NORMALIZE_PARENTHETICAL_FOR_DEDUPE") or "true"))
            album_norm_value = norm_album_for_dedup(title_raw, normalize_parenthetical)
            
            # Update: album title + FFprobe result (low-level summary for UI)
            with lock:
                if artist in state.get("scan_active_artists", {}) and state["scan_active_artists"][artist].get("current_album", {}).get("album_id") == aid:
                    state["scan_active_artists"][artist]["current_album"]["album_title"] = title_raw or plex_title or f"Album {aid}"
                    fmt_ext = first_audio.suffix.upper().lstrip(".") if first_audio else "?"
                    br_k = (br // 1000) if br >= 1000 else br
                    state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                        f"FFprobe: {fmt_ext} · {br_k} kbps · {sr} Hz · {bd}-bit"
                        + (" (cached)" if audio_cache_hit else "")
                    )
                    state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                        f"FFprobe: format {fmt_ext}, {br_k} kbps, {sr} Hz, {bd}-bit"
                        + (" (from cache)" if audio_cache_hit else "")
                    )
                    state["scan_format_done_count"] = state.get("scan_format_done_count", 0) + 1
                    state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1

            # Plex-normalized title: same key as get_duplicate_groups_from_library so scan groups match library
            plex_norm_value = norm_album_for_dedup(plex_title or "", normalize_parenthetical) if plex_title else album_norm_value
            editions.append({
                'album_id':  aid,
                'title_raw': title_raw,
                'album_norm': album_norm_value,
                'plex_norm': plex_norm_value,  # For grouping: align with library (norm_album(plex title))
                'artist':    artist,
                'folder':    folder,
                'tracks':    tr,
                'file_count': file_count,
                'sig':       signature(tr),
                'titles':    {t.title for t in tr},
                'dur':       sum(t.dur for t in tr),
                'fmt_score': fmt_score,
                'br':        br,
                'sr':        sr,
                'bd':        bd,
                'discs':     len({t.disc for t in tr}),
                'meta':      meta_tags,
                'invalid':   False,
                'title_source': title_source,
                'plex_title': plex_title or "",
                'audio_cache_hit': audio_cache_hit  # Track if this album used cache
            })
            
            # Mark album as done if it's not part of any duplicate group (single edition)
            # This will be updated later if it becomes part of a group
            with lock:
                if artist in state.get("scan_active_artists", {}) and state["scan_active_artists"][artist].get("current_album", {}).get("album_id") == aid:
                    # Don't mark as done yet - wait to see if it's part of a group
                    pass
        except Exception as e:
            logging.error("Error processing album %s for artist %s: %s", aid, artist, e, exc_info=True)
            # Mark as done even on error
            with lock:
                if artist in state.get("scan_active_artists", {}) and state["scan_active_artists"][artist].get("current_album", {}).get("album_id") == aid:
                    state["scan_active_artists"][artist]["current_album"]["status"] = "done"
                    state["scan_active_artists"][artist]["current_album"]["status_details"] = ""
                    state["scan_active_artists"][artist]["current_album"]["step_summary"] = ""
                    state["scan_active_artists"][artist]["current_album"]["step_response"] = ""
            continue

    logging.debug("[Artist %s] Computed stats for %d valid editions: %s", artist, len(editions), [e['album_id'] for e in editions])

    if not USE_MUSICBRAINZ:
        logging.debug("[Artist %s] Skipping MusicBrainz enrichment (USE_MUSICBRAINZ=False).", artist)
    else:
        # ─── MusicBrainz enrichment & Box Set handling ─────────────────────────────
        mb_start = time.perf_counter()
        # Enrich using any available MusicBrainz ID tags (in priority order)
        id_tags = [
            'musicbrainz_releasegroupid',
            'musicbrainz_releaseid',
            'musicbrainz_originalreleaseid',
            'musicbrainz_albumid'
        ]
        for e in editions:
            # Update current_album to this edition so UI shows MusicBrainz step for every album (not only the last one)
            with lock:
                if artist in state.get("scan_active_artists", {}):
                    state["scan_active_artists"][artist]["current_album"] = {
                        "album_id": e["album_id"],
                        "album_title": e.get("title_raw") or e.get("plex_title") or f"Album {e['album_id']}",
                        "status": "fetching_mb_id",
                        "status_details": "fetching MusicBrainz ID",
                        "step_summary": "Looking up release group from tags…",
                        "step_response": "",
                    }
            
            meta = e.get('meta', {})
            rg_info = None
            mbid_found = None
            mbid_type = None
            tag_used = None

            # Already identified: album has release-group ID in tags — use cache only, no API
            existing_rgid = meta.get('musicbrainz_releasegroupid')
            if existing_rgid:
                rg_info = get_cached_mb_info(existing_rgid)
                mb_cache_hit = rg_info is not None
                mbid_found = existing_rgid
                mbid_type = 'release-group'
                tag_used = 'musicbrainz_releasegroupid'
                e['mb_cache_hit'] = mb_cache_hit
                e['rg_info_source'] = tag_used
                if rg_info:
                    e['rg_info'] = rg_info
                e['musicbrainz_id'] = mbid_found
                e['musicbrainz_type'] = mbid_type
                with lock:
                    if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                        cache_text = " (cached)" if mb_cache_hit else " (from tags, no API)"
                        state["scan_active_artists"][artist]["current_album"]["status"] = "searching_mb"
                        state["scan_active_artists"][artist]["current_album"]["status_details"] = f"MusicBrainz ID from tags{cache_text}"
                        rg_title = (rg_info.get("title") or "") if isinstance(rg_info, dict) else ""
                        state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                            f"MusicBrainz: release group \"{rg_title}\" (id: {mbid_found}){cache_text}"
                        )
                        state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                            f"MusicBrainz: from tags. Release group \"{rg_title}\" (id: {mbid_found}){cache_text}"
                        )
                logging.debug("[Artist %s] Edition %s already has MBID in tags, cache_hit=%s", artist, e['album_id'], mb_cache_hit)
            else:
                # No release-group ID in tags: try other ID tags (release ID etc.) or search
                for tag in id_tags:
                    mbid = meta.get(tag)
                    if not mbid:
                        continue
                    try:
                        mb_cache_hit = False
                        if tag == 'musicbrainz_releasegroupid':
                            rg_info, mb_cache_hit = fetch_mb_release_group_info(mbid)
                            mbid_found = mbid
                            mbid_type = 'release-group'
                        else:
                            def _fetch_release():
                                return musicbrainzngs.get_release_by_id(mbid, includes=["release-groups"])["release"]
                            if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
                                rel = get_mb_queue().submit(f"rel_{mbid}", _fetch_release)
                            else:
                                rel = _fetch_release()
                            rgid = rel['release-group']['id']
                            rg_info, mb_cache_hit = fetch_mb_release_group_info(rgid)
                            mbid_found = rgid
                            mbid_type = 'release-group'
                        e['mb_cache_hit'] = mb_cache_hit
                        tag_used = tag
                        with lock:
                            if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                cache_text = " (cached)" if mb_cache_hit else ""
                                state["scan_active_artists"][artist]["current_album"]["status"] = "searching_mb"
                                state["scan_active_artists"][artist]["current_album"]["status_details"] = f"MusicBrainz ID fetched{cache_text}"
                                rg_title = (rg_info.get("title") or "") if isinstance(rg_info, dict) else ""
                                state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                    f"MusicBrainz: release group \"{rg_title}\" (id: {mbid_found}){cache_text}"
                                )
                                state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                                    f"MusicBrainz: from tags ({tag}). Release group \"{rg_title}\" (id: {mbid_found}){cache_text}"
                                )
                        logging.debug("[Artist %s] Edition %s RG info (via %s %s): %s", artist, e['album_id'], tag, mbid, rg_info)
                        break
                    except Exception as exc:
                        logging.debug("[Artist %s] MusicBrainz lookup failed for %s (%s): %s", artist, tag, mbid, exc)
                if rg_info:
                    e['rg_info_source'] = tag_used
                    e['rg_info'] = rg_info
                    if mbid_found:
                        e['musicbrainz_id'] = mbid_found
                        e['musicbrainz_type'] = mbid_type

            # Fallback: search by metadata if no ID tag yielded results
            album_norm = e['album_norm']
            tracks = {t.title for t in e['tracks']}
            artist_norm_key = artist.lower().strip()
            if not rg_info:
                # Check cache for "artist+album -> MBID". We only skip the API when we have a positive cache.
                # Cached "not found" is never used to skip: we always re-query MusicBrainz so that transient
                # failures or improved data on MusicBrainz can be picked up on the next scan.
                cached_mbid, cached_rg_info = get_cached_mb_album_lookup(artist_norm_key, album_norm)
                if cached_mbid and cached_mbid != "":
                    # Cached: found MBID for this artist+album — use it, no API search
                    rg_info = cached_rg_info or get_cached_mb_info(cached_mbid)
                    if rg_info:
                        e['rg_info_source'] = 'fallback'
                        e['rg_info'] = rg_info
                        e['musicbrainz_id'] = cached_mbid
                        e['musicbrainz_type'] = 'release-group'
                        e['mb_cache_hit'] = True
                        with lock:
                            if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                rg_title = (rg_info.get("title") or "") if isinstance(rg_info, dict) else ""
                                state["scan_active_artists"][artist]["current_album"]["status_details"] = "MusicBrainz found (cached)"
                                state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                    f"MusicBrainz: found \"{rg_title}\" (id: {cached_mbid}) (cached)"
                                )
                                state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                                    f"MusicBrainz: found release group \"{rg_title}\" (id: {cached_mbid}) (cached)"
                                )
                        logging.debug("[Artist %s] Edition %s MBID from album lookup cache: %s", artist, e['album_id'], cached_mbid)
                else:
                    # Not in cache: run search and cache result
                    with lock:
                        if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                            state["scan_active_artists"][artist]["current_album"]["status"] = "searching_mb"
                            state["scan_active_artists"][artist]["current_album"]["status_details"] = "searching MusicBrainz"
                            state["scan_active_artists"][artist]["current_album"]["step_summary"] = "Searching by artist + album name…"
                            state["scan_active_artists"][artist]["current_album"]["step_response"] = "MusicBrainz: querying by artist + album name…"
                    title_raw_mb = e.get("title_raw") or e.get("plex_title") or album_norm
                    album_folder_arg = e.get("folder")
                    rg_info, match_verified_by_ai = search_mb_release_group_by_metadata(artist, album_norm, tracks, title_raw=title_raw_mb, album_folder=album_folder_arg)
                    if rg_info:
                        set_cached_mb_album_lookup(artist_norm_key, album_norm, rg_info.get('id') or "", rg_info)
                    else:
                        set_cached_mb_album_lookup(artist_norm_key, album_norm, "", None)
                    if rg_info:
                        e['rg_info_source'] = 'fallback'
                        e['rg_info'] = rg_info
                        e['match_verified_by_ai'] = bool(match_verified_by_ai)
                        if isinstance(rg_info, dict) and 'id' in rg_info:
                            e['musicbrainz_id'] = rg_info['id']
                            e['musicbrainz_type'] = 'release-group'
                            mbid = rg_info['id']
                            cached_mb = get_cached_mb_info(mbid)
                            e['mb_cache_hit'] = cached_mb is not None
                            with lock:
                                if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                    cache_text = " (cached)" if cached_mb else ""
                                    ai_text = " (match verified by AI)" if match_verified_by_ai else ""
                                    state["scan_active_artists"][artist]["current_album"]["status_details"] = f"MusicBrainz found{cache_text}{ai_text}"
                                    rg_title = (rg_info.get("title") or "") if isinstance(rg_info, dict) else ""
                                    state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                        f"MusicBrainz: found \"{rg_title}\" (id: {mbid}){cache_text}{ai_text}"
                                    )
                                    state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                                        f"MusicBrainz: found release group \"{rg_title}\" (id: {mbid}){cache_text}{ai_text}"
                                    )
                        logging.debug("[Artist %s] Edition %s RG info (search fallback)%s: %s", artist, e['album_id'], " (verified by AI)" if match_verified_by_ai else "", rg_info)
                    else:
                        logging.debug("[Artist %s] No RG info found via search for '%s'", artist, album_norm)
                        e['mb_cache_hit'] = False
                        with lock:
                            if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                    f"MusicBrainz: no release group found for \"{album_norm}\""
                                )
                                state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                                    f"MusicBrainz: no release group found for \"{album_norm}\""
                                )
            # Fallback when MusicBrainz found nothing: try Discogs, Last.fm, Bandcamp for metadata (and optionally MB ID from Last.fm)
            title_raw = e.get("title_raw") or e.get("plex_title") or album_norm
            if not rg_info:
                fallback_sources = []
                if USE_DISCOGS:
                    try:
                        discogs_info = _fetch_discogs_release(artist, title_raw)
                        if discogs_info:
                            e["fallback_discogs"] = discogs_info
                            with lock:
                                state["scan_discogs_matched"] = state.get("scan_discogs_matched", 0) + 1
                            fallback_sources.append("Discogs")
                    except Exception as discogs_exc:
                        logging.debug("[Artist %s] Discogs fallback failed for %s: %s", artist, title_raw, discogs_exc)
                if USE_LASTFM:
                    try:
                        lastfm_info = _fetch_lastfm_album_info(artist, title_raw)
                        if lastfm_info:
                            e["fallback_lastfm"] = lastfm_info
                            with lock:
                                state["scan_lastfm_matched"] = state.get("scan_lastfm_matched", 0) + 1
                            fallback_sources.append("Last.fm")
                            lfm_mbid = (lastfm_info.get("mbid") or "").strip()
                            if lfm_mbid:
                                try:
                                    rg_info, _ = fetch_mb_release_group_info(lfm_mbid)
                                    if rg_info:
                                        e["rg_info"] = rg_info
                                        e["musicbrainz_id"] = lfm_mbid
                                        e["rg_info_source"] = "lastfm_fallback"
                                        with lock:
                                            if artist in state.get("scan_active_artists", {}) and e["album_id"] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                                state["scan_active_artists"][artist]["current_album"]["status_details"] = "MusicBrainz ID from Last.fm"
                                                state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                                                    f"Last.fm: MusicBrainz release group (id: {lfm_mbid})"
                                                )
                                        logging.debug("[Artist %s] Edition %s MB ID from Last.fm mbid: %s", artist, e["album_id"], lfm_mbid)
                                except Exception:
                                    pass
                    except Exception as lastfm_exc:
                        logging.debug("[Artist %s] Last.fm fallback failed for %s: %s", artist, title_raw, lastfm_exc)
                if not rg_info and USE_BANDCAMP:
                    try:
                        bandcamp_info = _fetch_bandcamp_album_info(artist, title_raw)
                        if bandcamp_info:
                            e["fallback_bandcamp"] = bandcamp_info
                            with lock:
                                state["scan_bandcamp_matched"] = state.get("scan_bandcamp_matched", 0) + 1
                            fallback_sources.append("Bandcamp")
                    except Exception as bandcamp_exc:
                        logging.debug("[Artist %s] Bandcamp fallback failed for %s: %s", artist, title_raw, bandcamp_exc)
                if fallback_sources and artist in state.get("scan_active_artists", {}) and e["album_id"] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                    with lock:
                        state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                            state["scan_active_artists"][artist]["current_album"].get("step_response", "")
                            + ("; fallback: " + ", ".join(fallback_sources) if fallback_sources else "")
                        )
            # Increment MB-done count for this edition (whether we found rg_info or not)
            with lock:
                state["scan_mb_done_count"] = state.get("scan_mb_done_count", 0) + 1
                state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1
            # Also store MBID from rg_info if not already set
            if e.get('rg_info') and 'musicbrainz_id' not in e and isinstance(e['rg_info'], dict) and 'id' in e['rg_info']:
                e['musicbrainz_id'] = e['rg_info']['id']
                e['musicbrainz_type'] = 'release-group'
            
            # Detect broken album (missing tracks)
            is_broken, expected_count, actual_count, missing_indices = detect_broken_album(db_conn, e['album_id'], e['tracks'], e.get('rg_info'))
            e['is_broken'] = is_broken
            if is_broken:
                e['expected_track_count'] = expected_count
                e['actual_track_count'] = actual_count
                e['missing_indices'] = missing_indices
                logging.warning(
                    "[Artist %s] Album %s (%s) is broken: %d tracks found, expected %s, gaps: %s",
                    artist, e['album_id'], e.get('title_raw', ''), actual_count, expected_count or 'unknown', missing_indices
                )
        mb_lookup_time = time.perf_counter() - mb_start
        # --- MusicBrainz enrichment summary ---
        direct = sum(1 for e in editions if 'rg_info' in e and e.get('rg_info_source') in id_tags)
        fallback = sum(1 for e in editions if 'rg_info' in e and e.get('rg_info_source') == 'fallback')
        missing = sum(1 for e in editions if 'rg_info' not in e)
        logging.info(f"[Artist {artist}] MusicBrainz enrichment summary: direct={direct}, fallback={fallback}, missing={missing}")

    # Detect and collapse Box Set discs (skip as duplicates)
    from collections import defaultdict
    box_set_groups = defaultdict(list)
    for e in editions:
        sec_types = e.get('rg_info', {}).get('secondary_types', [])
        if 'Box Set' in sec_types:
            parent_folder = e['folder'].parent
            box_set_groups[parent_folder].append(e)

    if box_set_groups:
        for parent_folder, items in box_set_groups.items():
            logging.info(colour(
                f"[Artist {artist}] Box Set detected at {parent_folder} "
                f"with {len(items)} discs – skipping duplicate detection for these discs.",
                ANSI_BOLD + ANSI_CYAN
            ))
        # Exclude all Box Set disc folders from further duplicate grouping
        editions = [e for e in editions if e['folder'].parent not in box_set_groups]
    # --- NO FILES HANDLING ---
    if editions:
        # Reset streak on success
        no_file_streak_global = 0
        ok_msg = colour(
            f"[Artist {artist}] FOUND {len(editions)} valid file editions on filesystem for {len(album_ids)} albums. PATH_MAP and volume bindings appear correct!",
            ANSI_BOLD + ANSI_GREEN
        )
        logging.info("\n%s\n", ok_msg)
    else:
        # No valid editions found
        no_file_streak_global += 1
        if skip_count == len(album_ids):
            logging.info(f"[Artist {artist}] All {skip_count} albums skipped due to SKIP_FOLDERS {SKIP_FOLDERS}")
            return [], {"ai_used": 0, "mb_used": 0}, []
        else:
            logger = logging.getLogger()
            logger.error(f"[Artist {artist}] FOUND 0 valid file editions on filesystem! Checked SKIP_FOLDERS: {SKIP_FOLDERS}")
            notify_discord = globals().get("notify_discord", None)
            if notify_discord:
                notify_discord(f"No files found for {artist}.")
            global popup_displayed
            if no_file_streak_global >= NO_FILE_THRESHOLD:
                if not popup_displayed:
                    gui.display_popup(
                        f"PMDA didn't find any files for {NO_FILE_THRESHOLD} artists in a row. "
                        "Aborting scan. Files appear unreachable from inside the container; "
                        "please check your volume bindings."
                    )
                    popup_displayed = True
                scan_should_stop.set()
                return [], {"ai_used": 0, "mb_used": 0}, []
            # Below threshold, do not show repeated popups -- let scan continue or fail silently
            return [], {"ai_used": 0, "mb_used": 0}, []
    for e in editions:
        logging.debug(
            f"[Artist {artist}] Edition {e['album_id']}: "
            f"norm='{e['album_norm']}', tracks={len(e['tracks'])}, dur_ms={e['dur']}, "
            f"files={e['file_count']}, fmt_score={e['fmt_score']}, "
            f"br={e['br']}, sr={e['sr']}, bd={e['bd']}"
        )
    # Map resolved folder path -> edition (for same-folder duplicate groups later)
    folder_to_edition: dict[str, dict] = {}
    for e in editions:
        try:
            k = str(Path(e["folder"]).resolve())
        except Exception:
            k = str(e["folder"])
        folder_to_edition[k] = e
    # --- First pass: group by album_norm, with classical disambiguation ---
    from collections import defaultdict
    # initial grouping by normalized title
    # Group by Plex-normalized title so we match get_duplicate_groups_from_library (same artist + norm_album(plex title))
    raw_groups: dict[str, list[dict]] = defaultdict(list)
    for e in editions:
        group_key = (e.get('plex_norm') or e['album_norm'] or '').strip()
        if not group_key or group_key.startswith('__untitled__'):
            group_key = e['album_norm']
        raw_groups[group_key].append(e)
    # Log grouping: how many keys have 2+ editions (potential duplicate groups)
    dup_keys = [(k, len(v)) for k, v in raw_groups.items() if len(v) >= 2]
    logging.info(
        "[Artist %s] raw_groups: %d keys, %d with 2+ editions: %s",
        artist, len(raw_groups), len(dup_keys),
        dup_keys[:15] if len(dup_keys) > 15 else dup_keys,
    )
    
    # Filter out groups where all editions share the same folder (not real duplicates)
    # This can happen if Plex has duplicate album entries pointing to the same folder
    # SAFETY: Normalize all folder paths using resolve() to catch same folders with different representations
    filtered_groups: dict[str, list[dict]] = {}
    for norm, ed_list in raw_groups.items():
        if len(ed_list) < 2:
            filtered_groups[norm] = ed_list
            continue
        # Normalize all folders using resolve() for accurate comparison
        folders_resolved = set()
        for e in ed_list:
            folder = e.get('folder')
            if folder:
                try:
                    folder_resolved = str(Path(folder).resolve())
                    folders_resolved.add(folder_resolved)
                except Exception as resolve_err:
                    # If resolve fails, fall back to string representation
                    logging.debug("Could not resolve folder %s: %s, using string representation", folder, resolve_err)
                    folders_resolved.add(str(folder))
        
        if len(folders_resolved) == 1:
            # All editions in same folder - likely duplicate Plex entries, not real duplicates
            logging.warning(
                "[Artist %s] Skipping group '%s' - all %d editions share the same resolved folder: %s. "
                "This indicates duplicate Plex album entries pointing to the same files. "
                "Album IDs in this group: %s",
                artist, norm, len(ed_list), list(folders_resolved)[0],
                [e.get('album_id') for e in ed_list]
            )
            continue
        
        # Additional safety: check if any editions have the same (album_id, folder) combo
        # This should not happen with the earlier check, but double-check here
        seen_combos = set()
        duplicates_found = False
        for e in ed_list:
            folder = e.get('folder')
            album_id = e.get('album_id')
            if folder:
                try:
                    folder_resolved = str(Path(folder).resolve())
                except Exception:
                    folder_resolved = str(folder)
            else:
                folder_resolved = ""
            combo = (album_id, folder_resolved)
            if combo in seen_combos:
                logging.error(
                    "[Artist %s] CRITICAL: Found duplicate edition with same album_id=%d and folder=%s in group '%s'. "
                    "This should not happen. Skipping this edition.",
                    artist, album_id, folder_resolved, norm
                )
                duplicates_found = True
            seen_combos.add(combo)
        
        if duplicates_found:
            # Remove duplicate editions (keep first occurrence)
            unique_editions = []
            seen_combos_clean = set()
            for e in ed_list:
                folder = e.get('folder')
                album_id = e.get('album_id')
                if folder:
                    try:
                        folder_resolved = str(Path(folder).resolve())
                    except Exception:
                        folder_resolved = str(folder)
                else:
                    folder_resolved = ""
                combo = (album_id, folder_resolved)
                if combo not in seen_combos_clean:
                    unique_editions.append(e)
                    seen_combos_clean.add(combo)
            ed_list = unique_editions
        
        if len(ed_list) < 2:
            # After deduplication, not enough editions for a duplicate group
            continue
        
        filtered_groups[norm] = ed_list
    raw_groups = filtered_groups
    logging.info(
        "[Artist %s] filtered_groups (after same-folder filter): %d keys with 2+ editions",
        artist, len([k for k, v in raw_groups.items() if len(v) >= 2]),
    )
    # refine groups: for classical, split by year and first-track duration threshold
    exact_groups: dict[tuple, list[dict]] = {}
    for norm, ed_list in raw_groups.items():
        if len(ed_list) < 2:
            # single edition or no duplicates
            exact_groups[(norm, None, None)] = ed_list
            continue
        # detect classical genre in metadata (case-insensitive)
        genres = [e.get('meta', {}).get('genre', '').lower() for e in ed_list]
        is_classical = all('classical' in g for g in genres if g)
        if is_classical:
            # subdivide editions by year and duration of first track
            subgroups: list[dict] = []
            for e in ed_list:
                year = e.get('meta', {}).get('date') or e.get('meta', {}).get('originaldate') or ''
                dur = e['tracks'][0].dur if e.get('tracks') else 0
                placed = False
                for sg in subgroups:
                    if sg['year'] == year and abs(sg['dur'] - dur) <= 10000:
                        sg['editions'].append(e)
                        placed = True
                        break
                if not placed:
                    subgroups.append({'year': year, 'dur': dur, 'editions': [e]})
            for sg in subgroups:
                exact_groups[(norm, sg['year'], sg['dur'])] = sg['editions']
        else:
            # non-classical: group all under single key
            exact_groups[(norm, None, None)] = ed_list
    logging.debug(
        f"[Artist {artist}] Exact groups after classical filter: "
        f"{[(k, [ed['album_id'] for ed in v]) for k, v in exact_groups.items()]}"
    )

    out: list[dict] = []
    used_ids: set[int] = set()
    # Track statistics
    mb_used_count = 0  # Editions enriched with MusicBrainz
    ai_used_count = 0  # Groups where AI was used
    audio_cache_hits = 0  # Albums that used audio cache
    audio_cache_misses = 0  # Albums that needed ffprobe
    mb_cache_hits = 0  # MusicBrainz lookups from cache
    mb_cache_misses = 0  # MusicBrainz lookups requiring API call
    
    # Count cache usage
    audio_cache_hits = sum(1 for e in editions if e.get('audio_cache_hit', False))
    audio_cache_misses = len(editions) - audio_cache_hits
    
    # Count MusicBrainz usage and cache hits
    if USE_MUSICBRAINZ:
        mb_used_count = sum(1 for e in editions if 'rg_info' in e)
        mb_cache_hits = sum(1 for e in editions if e.get('mb_cache_hit', False))
        mb_cache_misses = mb_used_count - mb_cache_hits
    
    for norm, ed_list in exact_groups.items():
        logging.debug(f"[Artist {artist}] Processing exact group for norm='{norm}' with albums {[e['album_id'] for e in ed_list]}")
    for ed_list in exact_groups.values():
        if len(ed_list) < 2:
            continue
        logging.debug(f"[Artist {artist}] Exact group members: {[e['album_id'] for e in ed_list]}")

        # Update: comparing versions
        current_album_id = None
        with lock:
            if artist in state.get("scan_active_artists", {}):
                current_album = state["scan_active_artists"][artist].get("current_album", {})
                current_album_id = current_album.get("album_id")
                # Check if any edition in this group matches current album
                if current_album_id and any(e['album_id'] == current_album_id for e in ed_list):
                    state["scan_active_artists"][artist]["current_album"]["status"] = "comparing_versions"
                    state["scan_active_artists"][artist]["current_album"]["status_details"] = f"found {len(ed_list)} versions"
                    # Low-level: show bitrate/sample rate/bit depth for each version (max 3)
                    parts = []
                    for ed in ed_list[:3]:
                        br_k = (ed["br"] // 1000) if ed.get("br", 0) >= 1000 else ed.get("br", 0)
                        parts.append(f"{br_k} kbps · {ed.get('sr', 0)} Hz · {ed.get('bd', 0)}-bit")
                    tail = " …" if len(ed_list) > 3 else ""
                    state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                        f"{len(ed_list)} versions: " + " vs ".join(parts) + tail
                    )
                    state["scan_active_artists"][artist]["current_album"]["step_response"] = (
                        f"Comparing: " + " vs ".join(parts) + tail
                    )

        if not editions_share_confident_signal(ed_list):
            logging.info(
                "[Artist %s] Skipping low-confidence exact group (norm=%s) album_ids=%s",
                artist, ed_list[0].get('album_norm') if ed_list else "", [e['album_id'] for e in ed_list],
            )
            continue
        # Choose best across all identical normalized titles
        # Try with defer_ai=True first to check if AI is needed
        best = choose_best(ed_list, defer_ai=True)
        
        if best is None:
            # AI is needed, will be processed in batch later
            # Store group for batch processing
            group_data = {
                "artist": artist,
                "album_id": None,  # Will be set after AI processing
                "editions": ed_list,
                "fuzzy": False,
                "needs_ai": True,
            }
            out.append(group_data)
            continue
        
        # Update: detecting best
        with lock:
            if artist in state.get("scan_active_artists", {}) and current_album_id and any(e['album_id'] == current_album_id for e in ed_list):
                state["scan_active_artists"][artist]["current_album"]["status"] = "detecting_best"
                state["scan_active_artists"][artist]["current_album"]["status_details"] = "detecting best candidate"
                state["scan_active_artists"][artist]["current_album"]["step_summary"] = "Selecting best…"
                state["scan_active_artists"][artist]["current_album"]["step_response"] = "Selecting best edition by format & bitrate…"
        
        losers = [e for e in ed_list if e['album_id'] != best['album_id']]
        logging.debug(f"[Artist {artist}] Exact grouping selected best {best['album_id']}, losers { [e['album_id'] for e in losers] }")
        if not losers:
            continue

        # Track AI usage (note: AI processing time will be measured in batch processing)
        if best.get('used_ai', False):
            ai_used_count += 1

        group_data = {
            "artist":  artist,
            "album_id": best["album_id"],
            "best":    best,
            "losers":  losers,
            "fuzzy":   False,
            "needs_ai": False,
        }
        out.append(group_data)
        used_ids.update(e['album_id'] for e in ed_list)
        # Update progress tracking periodically
        with lock:
            if artist in state.get("scan_active_artists", {}):
                state["scan_active_artists"][artist]["albums_processed"] = len(used_ids)
                # Mark current album as done when we've processed it
                current_album = state["scan_active_artists"][artist].get("current_album", {})
                if current_album.get("album_id") in used_ids:
                    state["scan_active_artists"][artist]["current_album"]["status"] = "done"
                    state["scan_active_artists"][artist]["current_album"]["status_details"] = ""
                    state["scan_active_artists"][artist]["current_album"]["step_summary"] = ""
                    state["scan_active_artists"][artist]["current_album"]["step_response"] = ""

    # Filter out editions already grouped exactly so fuzzy pass only sees the rest
    all_editions_for_stats = list(editions)  # keep full list for stats (broken_albums, cache, etc.)
    editions = [e for e in editions if e['album_id'] not in used_ids]

    # --- Second pass: fuzzy match on album_norm only, for remaining editions ---
    norm_groups = defaultdict(list)
    for e in editions:
        if e['album_id'] not in used_ids:
            norm_groups[e['album_norm']].append(e)

    for norm, ed_list in norm_groups.items():
        logging.debug(f"[Artist {artist}] Processing fuzzy group for norm='{norm}' with albums {[e['album_id'] for e in ed_list]}")
    for ed_list in norm_groups.values():
        if len(ed_list) < 2:
            continue
        logging.debug(f"[Artist {artist}] Fuzzy group members: {[e['album_id'] for e in ed_list]}")
        
        # Update: comparing versions (fuzzy)
        current_album_id = None
        with lock:
            if artist in state.get("scan_active_artists", {}):
                current_album = state["scan_active_artists"][artist].get("current_album", {})
                current_album_id = current_album.get("album_id")
                if current_album_id and any(e['album_id'] == current_album_id for e in ed_list):
                    state["scan_active_artists"][artist]["current_album"]["status"] = "comparing_versions"
                    state["scan_active_artists"][artist]["current_album"]["status_details"] = f"found {len(ed_list)} versions (fuzzy match)"
                    parts = []
                    for ed in ed_list[:3]:
                        br_k = (ed["br"] // 1000) if ed.get("br", 0) >= 1000 else ed.get("br", 0)
                        parts.append(f"{br_k} kbps · {ed.get('sr', 0)} Hz · {ed.get('bd', 0)}-bit")
                    tail = " …" if len(ed_list) > 3 else ""
                    state["scan_active_artists"][artist]["current_album"]["step_summary"] = (
                        f"{len(ed_list)} versions (fuzzy): " + " vs ".join(parts) + tail
                    )
        
        # Only perform fuzzy grouping via AI; skip if no AI provider configured
        if not ai_provider_ready:
            continue
        if not editions_share_confident_signal(ed_list):
            logging.debug(
                "[Artist %s] Skipping low-confidence fuzzy group %s",
                artist,
                [e['album_id'] for e in ed_list]
            )
            continue
        # Force AI selection for fuzzy groups
        # Try with defer_ai=True first to check if AI is needed
        best = choose_best(ed_list, defer_ai=True)
        
        if best is None:
            # AI is needed, will be processed in batch later
            # Store group for batch processing
            group_data = {
                "artist": artist,
                "album_id": None,  # Will be set after AI processing
                "editions": ed_list,
                "fuzzy": True,
                "needs_ai": True,
            }
            out.append(group_data)
            continue
        
        # Update: detecting best (fuzzy)
        with lock:
            if artist in state.get("scan_active_artists", {}) and current_album_id and any(e['album_id'] == current_album_id for e in ed_list):
                state["scan_active_artists"][artist]["current_album"]["status"] = "detecting_best"
                state["scan_active_artists"][artist]["current_album"]["status_details"] = "detecting best candidate (AI)"
                state["scan_active_artists"][artist]["current_album"]["step_summary"] = "Selecting best (AI)…"
                state["scan_active_artists"][artist]["current_album"]["step_response"] = "AI: selecting best edition…"
        
        losers = [e for e in ed_list if e is not best]
        logging.debug(f"[Artist {artist}] Fuzzy grouping selected best {best['album_id']}, losers { [e['album_id'] for e in losers] }")
        
        # Track AI usage (fuzzy groups always use AI if available)
        if best.get('used_ai', False):
            ai_used_count += 1
        
        group_data = {
            'artist': artist,
            'album_id': best['album_id'],
            'best': best,
            'losers': losers,
            'fuzzy': True,
            'needs_ai': False,
        }
        out.append(group_data)
        used_ids.update(e['album_id'] for e in ed_list)
        # Update progress tracking periodically
        with lock:
            if artist in state.get("scan_active_artists", {}):
                state["scan_active_artists"][artist]["albums_processed"] = len(used_ids)
                # Mark current album as done when we've processed it (fuzzy groups)
                current_album = state["scan_active_artists"][artist].get("current_album", {})
                if current_album.get("album_id") in used_ids:
                    state["scan_active_artists"][artist]["current_album"]["status"] = "done"
                    state["scan_active_artists"][artist]["current_album"]["status_details"] = ""
                    state["scan_active_artists"][artist]["current_album"]["step_summary"] = ""
                    state["scan_active_artists"][artist]["current_album"]["step_response"] = ""
        notify_discord_embed(
            title="Duplicate group (fuzzy) found",
            description=(
                f"**{artist} – {best['title_raw']}**\n"
                f"Versions: {len(losers)+1}\n"
                f"Best: {get_primary_format(Path(best['folder']))}, "
                f"{best['bd']}‑bit, {len(best['tracks'])} tracks."
            ),
            thumbnail_url=thumb_url(best['album_id'])
        )
    # --- Same-folder duplicate groups: multiple Plex album entries pointing to one folder ---
    for folder_str, album_ids in seen_folders.items():
        if len(album_ids) < 2:
            continue
        best_edition = folder_to_edition.get(folder_str)
        if not best_edition:
            continue
        losers = []
        for aid in album_ids:
            if aid == best_edition["album_id"]:
                continue
            pt = album_title(db_conn, aid) or f"Album {aid}"
            losers.append({
                "album_id": aid,
                "title_raw": pt,
                "folder": best_edition["folder"],
                "meta": {},
                "plex_title": pt,
                "br": 0,
                "sr": 0,
                "bd": 0,
            })
        if not losers:
            continue
        logging.info(
            "[Artist %s] Same-folder duplicate group: '%s' has %d Plex entries (best=%s, losers=%s)",
            artist, best_edition.get("title_raw", ""), len(album_ids),
            best_edition["album_id"], [l["album_id"] for l in losers]
        )
        out.append({
            "artist": artist,
            "album_id": best_edition["album_id"],
            "best": best_edition,
            "losers": losers,
            "fuzzy": False,
            "needs_ai": False,
            "same_folder": True,
        })
    # Keep groups that have losers (resolved) or need AI (will be resolved in batch).
    # Do not drop needs_ai groups: they have "editions" but no "losers" yet.
    out = [g for g in out if g.get("losers") or g.get("needs_ai")]
    
    # Calculate total scan time
    scan_total_time = time.perf_counter() - scan_start_time
    
    # Compile stats with timing (use all_editions_for_stats for per-edition counts)
    stats = {
        "ai_used": ai_used_count,
        "mb_used": mb_used_count,
        "audio_cache_hits": sum(1 for e in all_editions_for_stats if e.get('audio_cache_hit', False)),
        "audio_cache_misses": sum(1 for e in all_editions_for_stats if not e.get('audio_cache_hit', True)),
        "mb_cache_hits": sum(1 for e in all_editions_for_stats if e.get('mb_cache_hit', False)),
        "mb_cache_misses": sum(1 for e in all_editions_for_stats if 'mb_cache_hit' in e and not e.get('mb_cache_hit', True)),
        "duplicate_groups_count": len(out),
        "total_duplicates_count": sum(len(g.get("losers", [])) for g in out),
        "broken_albums_count": sum(1 for e in all_editions_for_stats if e.get('is_broken', False)),
        "albums_without_mb_id": sum(1 for e in all_editions_for_stats if (not e.get('meta', {}).get('musicbrainz_releasegroupid') and not e.get('meta', {}).get('musicbrainz_releaseid') and not e.get('musicbrainz_id'))),
        "albums_without_artist_mb_id": sum(1 for e in all_editions_for_stats if not e.get('meta', {}).get('musicbrainz_albumartistid') and not e.get('meta', {}).get('musicbrainz_artistid')),
        "albums_without_complete_tags": 0,  # Will be calculated below
        "albums_without_album_image": 0,  # Will be calculated below
        "albums_without_artist_image": 0,  # Will be calculated below
        "timing": {
            "audio_analysis_time": audio_analysis_time,
            "mb_lookup_time": mb_lookup_time,
            "ai_processing_time": ai_processing_time,
            "total_time": scan_total_time,
        }
    }
    
    # Calculate albums without complete tags/images (REQUIRED_TAGS from Settings = source of truth)
    for e in editions:
        meta = e.get('meta', {})
        missing_required = _check_required_tags(meta, REQUIRED_TAGS, edition=e)
        if missing_required:
            stats["albums_without_complete_tags"] += 1
        
        # Check for album cover images
        folder = e.get('folder')
        if folder:
            cover_patterns = ["cover.*", "folder.*", "album.*", "artwork.*", "front.*"]
            has_cover = False
            for pattern in cover_patterns:
                matches = list(folder.glob(pattern))
                image_matches = [f for f in matches if f.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp', '.gif']]
                if image_matches:
                    has_cover = True
                    break
            if not has_cover:
                stats["albums_without_album_image"] += 1
    
    # Mark all remaining albums as done (those not in any duplicate group)
    with lock:
        if artist in state.get("scan_active_artists", {}):
            current_album = state["scan_active_artists"][artist].get("current_album", {})
            current_album_id = current_album.get("album_id")
            # If current album is not in used_ids, it means it's a single edition (no duplicates)
            # Mark it as done
            if current_album_id and current_album_id not in used_ids:
                state["scan_active_artists"][artist]["current_album"]["status"] = "done"
                state["scan_active_artists"][artist]["current_album"]["status_details"] = ""
                state["scan_active_artists"][artist]["current_album"]["step_summary"] = ""
                state["scan_active_artists"][artist]["current_album"]["step_response"] = ""
    
    # Store broken albums in database
    import json
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    cur = con.cursor()
    for e in editions:
        if e.get('is_broken', False):
            missing_indices_json = json.dumps(e.get('missing_indices', []))
            cur.execute("""
                INSERT OR REPLACE INTO broken_albums 
                (artist, album_id, expected_track_count, actual_track_count, missing_indices, musicbrainz_release_group_id, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                artist,
                e['album_id'],
                e.get('expected_track_count'),
                e.get('actual_track_count', len(e.get('tracks', []))),
                missing_indices_json,
                e.get('musicbrainz_id'),
                time.time()
            ))
            
            # Auto-send to Lidarr if enabled
            if AUTO_FIX_BROKEN_ALBUMS and LIDARR_URL and LIDARR_API_KEY and e.get('musicbrainz_id'):
                try:
                    add_broken_album_to_lidarr(artist, e['album_id'], e.get('musicbrainz_id'), e.get('title_raw', ''))
                    cur.execute("""
                        UPDATE broken_albums SET sent_to_lidarr = 1 
                        WHERE artist = ? AND album_id = ?
                    """, (artist, e['album_id']))
                except Exception as lidarr_err:
                    logging.warning("Failed to auto-send broken album %s to Lidarr: %s", e['album_id'], lidarr_err)
    con.commit()
    con.close()
    
    # Collect detailed statistics
    duplicate_groups_count = len(out)
    total_duplicates_count = sum(len(g.get("losers", [])) for g in out)
    broken_albums_count = sum(1 for e in editions if e.get('is_broken', False))
    mb_verified_by_ai = sum(1 for e in editions if e.get('match_verified_by_ai'))

    # Count albums with missing tags/images
    albums_without_mb_id = 0
    albums_without_artist_mb_id = 0
    albums_without_complete_tags = 0
    albums_without_artist_image = 0
    albums_without_album_image = 0
    
    for e in editions:
        meta = e.get('meta', {})
        # Check MusicBrainz IDs (include e.get('musicbrainz_id') so scan/IA matches count as "with MBID")
        if (not meta.get('musicbrainz_releasegroupid') and not meta.get('musicbrainz_releaseid') and not e.get('musicbrainz_id')):
            albums_without_mb_id += 1
        if not meta.get('musicbrainz_albumartistid') and not meta.get('musicbrainz_artistid'):
            albums_without_artist_mb_id += 1

        # Check for complete tags (REQUIRED_TAGS from Settings = source of truth)
        missing_required = _check_required_tags(meta, REQUIRED_TAGS, edition=e)
        if missing_required:
            albums_without_complete_tags += 1
        
        # Check for album cover images
        folder = e.get('folder')
        if folder:
            # Look for common cover art filenames
            cover_patterns = ["cover.*", "folder.*", "album.*", "artwork.*", "front.*"]
            has_cover = False
            for pattern in cover_patterns:
                matches = list(folder.glob(pattern))
                # Filter to image extensions
                image_matches = [f for f in matches if f.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp', '.gif']]
                if image_matches:
                    has_cover = True
                    break
            if not has_cover:
                albums_without_album_image += 1
    
    # Return groups and statistics
    stats = {
        "ai_used": ai_used_count,
        "mb_used": mb_used_count,
        "mb_verified_by_ai": mb_verified_by_ai,
        "audio_cache_hits": audio_cache_hits,
        "audio_cache_misses": audio_cache_misses,
        "mb_cache_hits": mb_cache_hits,
        "mb_cache_misses": mb_cache_misses,
        "duplicate_groups_count": duplicate_groups_count,
        "total_duplicates_count": total_duplicates_count,
        "broken_albums_count": broken_albums_count,
        "albums_without_mb_id": albums_without_mb_id,
        "albums_without_artist_mb_id": albums_without_artist_mb_id,
        "albums_without_complete_tags": albums_without_complete_tags,
        "albums_without_album_image": albums_without_album_image,
        "albums_without_artist_image": albums_without_artist_image,  # Will be calculated per artist later
    }
    return out, stats, all_editions_for_stats

def save_scan_editions_to_db(scan_id: int, all_editions_by_artist: Dict[str, List[dict]]):
    """
    Persist per-edition scan data to scan_editions for Library and Tag Fixer to use.
    Call after a scan completes (or is stopped) so last_completed_scan_id can be used to read from this table.
    """
    import json
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    cur = con.cursor()
    cur.execute("DELETE FROM scan_editions WHERE scan_id = ?", (scan_id,))
    row_count = 0
    for artist, editions_list in all_editions_by_artist.items():
        for e in editions_list:
            folder = e.get("folder")
            meta = e.get("meta", {})
            # has_cover: same logic as stats loop
            has_cover = 0
            if folder:
                folder_path = Path(folder) if not isinstance(folder, Path) else folder
                cover_patterns = ["cover.*", "folder.*", "album.*", "artwork.*", "front.*"]
                for pattern in cover_patterns:
                    matches = list(folder_path.glob(pattern))
                    image_matches = [f for f in matches if f.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp', '.gif']]
                    if image_matches:
                        has_cover = 1
                        break
            # missing_required_tags (REQUIRED_TAGS from Settings = source of truth)
            missing_required = _check_required_tags(meta, REQUIRED_TAGS, edition=e)
            missing_required_json = json.dumps(missing_required) if missing_required else None
            folder_str = str(folder) if folder else ""
            fmt_text = get_primary_format(Path(folder_str)) if folder_str else ""
            cur.execute("""
                INSERT INTO scan_editions
                (scan_id, artist, album_id, title_raw, folder, fmt_text, br, sr, bd, meta_json, musicbrainz_id,
                 is_broken, expected_track_count, actual_track_count, missing_indices, has_cover, missing_required_tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                scan_id,
                artist,
                e.get("album_id"),
                e.get("title_raw", ""),
                folder_str,
                fmt_text,
                e.get("br") or 0,
                e.get("sr") or 0,
                e.get("bd") or 0,
                json.dumps(meta),
                e.get("musicbrainz_id") or "",
                1 if e.get("is_broken") else 0,
                e.get("expected_track_count"),
                e.get("actual_track_count") or len(e.get("tracks", [])),
                json.dumps(e.get("missing_indices", [])),
                has_cover,
                missing_required_json,
            ))
            row_count += 1
    con.commit()
    con.close()
    logging.debug("save_scan_editions_to_db: scan_id=%s, %d edition rows", scan_id, row_count)

def save_scan_to_db(scan_results: Dict[str, List[dict]]):
    """
    Given a dict of { artist_name: [group_dicts...] }, clear duplicates tables and re‐populate them.
    """
    import sqlite3, json

    # (Removed: filtering of invalid editions; already purged upstream)
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()

    # 1) Clear both duplicates tables
    cur.execute("DELETE FROM duplicates_loser")
    cur.execute("DELETE FROM duplicates_best")

    # 2) Re-insert all scan results (skip groups that have no best/losers, e.g. needs_ai not yet processed)
    saved_count = 0
    skipped_count = 0
    saved_with_ai = 0
    for artist, groups in scan_results.items():
        for g in groups:
            if "best" not in g or "losers" not in g:
                skipped_count += 1
                logging.debug("save_scan_to_db: skipping group without best/losers (artist=%s)", artist)
                continue
            saved_count += 1
            best = g["best"]
            if best.get("used_ai"):
                saved_with_ai += 1
            # Persist size_mb and track_count so Unduper shows them after reload
            best_folder_path = path_for_fs_access(Path(best["folder"])) if best.get("folder") else None
            best_size_mb = (safe_folder_size(best_folder_path) // (1024 * 1024)) if best_folder_path else 0
            best_track_count = len(best.get("tracks", []))
            # When used_ai, ensure ai_provider/ai_model are set (e.g. from cache they may be empty)
            used_ai = bool(best.get("used_ai", False))
            ai_provider = best.get("ai_provider") or ""
            ai_model = best.get("ai_model") or ""
            if used_ai and (not ai_provider or not ai_model):
                mod = sys.modules[__name__]
                ai_provider = ai_provider or (getattr(mod, "AI_PROVIDER", None) or "")
                ai_model = ai_model or (getattr(mod, "RESOLVED_MODEL", None) or getattr(mod, "OPENAI_MODEL", None) or "")
            # Best edition
            cur.execute("""
                INSERT OR IGNORE INTO duplicates_best
                  (artist, album_id, title_raw, album_norm, folder,
                   fmt_text, br, sr, bd, dur, discs, rationale, merge_list, ai_used, meta_json, ai_provider, ai_model, size_mb, track_count, match_verified_by_ai)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                artist,
                best['album_id'],
                best['title_raw'],
                best['album_norm'],
                str(best['folder']),
                get_primary_format(Path(best['folder'])),
                best['br'],
                best['sr'],
                best['bd'],
                best['dur'],
                best['discs'],
                best.get('rationale', ''),
                json.dumps(best.get('merge_list', [])),
                int(used_ai),
                json.dumps(best.get('meta', {})),
                ai_provider,
                ai_model,
                best_size_mb,
                best_track_count,
                1 if best.get('match_verified_by_ai') else 0,
            ))

            # All "loser" editions
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
    if saved_count or skipped_count:
        logging.info(
            "save_scan_to_db: saved %d group(s) (%d with AI), skipped %d without best/losers",
            saved_count, saved_with_ai, skipped_count,
        )


def _remove_dedupe_group_from_db(artist: str, best_album_id: int, loser_album_ids: List[int]) -> None:
    """Remove one duplicate group from DB after it has been successfully moved to /dupes."""
    try:
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        cur.execute("DELETE FROM duplicates_best WHERE artist = ? AND album_id = ?", (artist, best_album_id))
        for aid in loser_album_ids:
            cur.execute("DELETE FROM duplicates_loser WHERE artist = ? AND album_id = ?", (artist, aid))
        con.commit()
        con.close()
    except Exception as e:
        logging.warning("_remove_dedupe_group_from_db failed for %s / %s: %s", artist, best_album_id, e)


def load_scan_from_db() -> Dict[str, List[dict]]:
    """
    Read the most-recent duplicate-scan from STATE_DB_FILE and rebuild the
    in-memory structure used by the Web UI.

    Returns
    -------
    dict
        { artist_name : [ group_dict, ... ] }
    """
    import json
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()

    # ---- 1) Best editions -----------------------------------------------------
    cur.execute("PRAGMA table_info(duplicates_best)")
    best_cols = {r[1] for r in cur.fetchall()}
    has_match_verified = "match_verified_by_ai" in best_cols
    cur.execute(
        """
        SELECT artist, album_id, title_raw, album_norm, folder,
               fmt_text, br, sr, bd, dur, discs, rationale, merge_list, ai_used, meta_json,
               ai_provider, ai_model, size_mb, track_count
        """ + (", match_verified_by_ai" if has_match_verified else "") + """
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

    for row in best_rows:
        (artist, aid, title_raw, album_norm, folder, fmt_txt, br, sr, bd, dur, discs,
         rationale, merge_list_json, ai_used, meta_json) = row[:15]
        ai_provider = (row[15] or "") if len(row) > 15 else ""
        ai_model = (row[16] or "") if len(row) > 16 else ""
        size_mb = row[17] if len(row) > 17 else None
        track_count = row[18] if len(row) > 18 else None
        match_verified_by_ai = bool(row[19]) if has_match_verified and len(row) > 19 else False

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
            "meta": json.loads(meta_json or "{}"),
            "ai_provider": ai_provider,
            "ai_model": ai_model,
            "size_mb": size_mb,
            "track_count": track_count,
            "match_verified_by_ai": match_verified_by_ai,
        }

        losers = loser_map.get((artist, aid), [])

        # Some loser rows still need the readable title; fetch it from Plex DB.
        for l in losers:
            if l["title_raw"] is None:
                db_plx = plex_connect()
                title = album_title(db_plx, aid)
                if not title:
                    continue
                l["title_raw"] = title
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
    When a user triggers "Start New Scan," wipe prior duplicates from memory.
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
    an unexpected error occurs, so the front‑end never hangs in "running".
    """
    # Reload SECTION_IDS and PATH_MAP from DB so the scan uses the library selection
    # and path bindings currently saved in Settings (e.g. after Detect & verify)
    _reload_section_ids_from_db()
    _reload_path_map_from_db()
    if not SECTION_IDS:
        logging.warning("background_scan(): SECTION_IDS is empty after reload; aborting scan")
        with lock:
            state["scanning"] = False
        return
    if not PATH_MAP:
        logging.warning("background_scan(): PATH_MAP is empty after reload; albums will not resolve to container paths. Run Detect & verify in Settings.")
    logging.debug(f"background_scan(): SECTION_IDS=%s, PATH_MAP keys=%s, opening Plex DB at {PLEX_DB_FILE}", SECTION_IDS, list(PATH_MAP.keys()))
    start_time = time.perf_counter()
    all_results: Dict[str, List[dict]] = {}  # Always defined so finally can persist
    all_editions_by_artist: Dict[str, List[dict]] = {}  # For scan_editions (Library, Tag Fixer)

    try:
        db_conn = plex_connect()

        # 1) Total albums for progress bar
        with lock:
            state["scan_progress"] = 0
        placeholders = ",".join("?" for _ in SECTION_IDS)
        total_albums = db_conn.execute(
            f"SELECT COUNT(*) FROM metadata_items "
            f"WHERE metadata_type=9 AND library_section_id IN ({placeholders})",
            SECTION_IDS,
        ).fetchone()[0]

        # 2) Fetch all artists
        artists_raw = db_conn.execute(
            f"SELECT id, title FROM metadata_items "
            f"WHERE metadata_type=8 AND library_section_id IN ({placeholders})",
            SECTION_IDS,
        ).fetchall()

        # Merge artists by normalized name so duplicates across Plex "artist" entries
        # (e.g. Ochre from folder A and Ochre from folder B) are scanned together
        from collections import defaultdict
        artists_by_name: dict[str, list[tuple[int, str]]] = defaultdict(list)
        for artist_id, artist_name in artists_raw:
            name_norm = (artist_name or "").strip().lower()
            artists_by_name[name_norm].append((artist_id, artist_name))

        # Build one task per distinct artist name with combined album_ids from all Plex artist entries
        artists_merged: list[tuple[int, str, list[int]]] = []
        for name_norm, id_name_list in artists_by_name.items():
            artist_ids = [aid for aid, _ in id_name_list]
            primary_id, primary_name = id_name_list[0]
            ph = ",".join("?" for _ in artist_ids)
            album_ids_for_name = [
                row[0] for row in db_conn.execute(
                    f"SELECT id FROM metadata_items "
                    f"WHERE metadata_type=9 AND parent_id IN ({ph})",
                    artist_ids,
                ).fetchall()
            ]
            artists_merged.append((primary_id, primary_name, album_ids_for_name))
            if len(id_name_list) > 1:
                logging.info(
                    "Merged %d Plex artist entries for '%s' into one scan (%d albums)",
                    len(id_name_list), primary_name, len(album_ids_for_name)
                )

        total_artists = len(artists_merged)
        logging.info(
            "Scan scope: %d artist(s), %d album(s). Section ID(s): %s. "
            "If this is smaller than expected, check Settings → Libraries (SECTION_IDS) and PATH_MAP.",
            total_artists, total_albums, SECTION_IDS
        )

        # --- Discord: announce scan start ---
        notify_discord_embed(
            title="🔄 PMDA scan started",
            description=(
                f"Scanning {len(artists_merged)} artists / {total_albums} albums… "
                "Buckle up!"
            )
        )

        logging.debug(
            f"background_scan(): {len(artists_merged)} artists (merged by name), {total_albums} albums total"
        )

        # Reload AI config from DB and run functional check (probe/ladder for OpenAI)
        _reload_ai_config_and_reinit()
        logging.debug("Scan: AI reload/reinit done. ai_provider_ready=%s", ai_provider_ready)

        if not ai_provider_ready:
            logging.warning("background_scan(): AI not configured or functional check failed; aborting scan.")
            with lock:
                state["scanning"] = False
                state["scan_ai_preflight_error"] = getattr(sys.modules[__name__], "AI_FUNCTIONAL_ERROR_MSG", None) or "Configure the AI provider in Settings."
            return

        # Reset live state. Step-based progress: total_steps = 3*albums + 2 (+1 if auto-move).
        start_time = time.time()
        with lock:
            state.update(scanning=True, scan_progress=0, scan_total=total_albums + 2)
            state["scan_total_albums"] = total_albums
            state["scan_step_progress"] = 0
            # scan_step_total set after _reload_auto_move_from_db() so AUTO_MOVE_DUPES is current
            state["duplicates"].clear()
            # Initialize scan details tracking
            state["scan_artists_processed"] = 0
            state["scan_artists_total"] = total_artists
            state["scan_ai_used_count"] = 0
            state["scan_mb_used_count"] = 0
            state["scan_ai_enabled"] = ai_provider_ready
            state["scan_mb_enabled"] = USE_MUSICBRAINZ
            # Initialize ETA tracking
            state["scan_start_time"] = start_time
            state["scan_last_update_time"] = start_time
            state["scan_last_progress"] = 0
            state["scan_format_done_count"] = 0
            state["scan_mb_done_count"] = 0
            state["scan_active_artists"] = {}
            # Initialize cache tracking
            state["scan_audio_cache_hits"] = 0
            state["scan_audio_cache_misses"] = 0
            state["scan_mb_cache_hits"] = 0
            state["scan_mb_cache_misses"] = 0
            # Initialize detailed statistics tracking
            state["scan_duplicate_groups_count"] = 0
            state["scan_total_duplicates_count"] = 0
            state["scan_broken_albums_count"] = 0
            state["scan_missing_albums_count"] = 0
            state["scan_albums_without_artist_image"] = 0
            state["scan_albums_without_album_image"] = 0
            state["scan_albums_without_complete_tags"] = 0
            state["scan_albums_without_mb_id"] = 0
            state["scan_albums_without_artist_mb_id"] = 0
            state["scan_mb_verified_by_ai_count"] = 0
            state["scan_discogs_matched"] = 0
            state["scan_lastfm_matched"] = 0
            state["scan_bandcamp_matched"] = 0
            state["scan_ai_errors"] = []
            # Preflight: store MB and AI connection status for end-of-scan summary
            mb_ok, ai_ok = _run_preflight_checks()
            state["scan_mb_connection_ok"] = mb_ok
            state["scan_ai_connection_ok"] = ai_ok
        
        # Reload AUTO_MOVE_DUPES from DB so scan uses current setting (UI may have toggled it)
        _reload_auto_move_from_db()
        with lock:
            # Step-based progress: 3 steps per album (format, MB, compare) + 1 AI + 1 finalize + (1 if move)
            state["scan_step_total"] = 3 * total_albums + 2 + (1 if AUTO_MOVE_DUPES else 0)

        # Create scan history entry
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        cur.execute("PRAGMA table_info(scan_history)")
        scan_cols = [r[1] for r in cur.fetchall()]
        if "entry_type" in scan_cols:
            cur.execute("""
                INSERT INTO scan_history 
                (start_time, albums_scanned, artists_total, ai_enabled, mb_enabled, auto_move_enabled, status,
                 duplicate_groups_count, total_duplicates_count, broken_albums_count, missing_albums_count,
                 albums_without_artist_image, albums_without_album_image, albums_without_complete_tags,
                 albums_without_mb_id, albums_without_artist_mb_id, entry_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'scan')
            """, (
                start_time,
                total_albums,
                total_artists,
                1 if ai_provider_ready else 0,
                1 if USE_MUSICBRAINZ else 0,
                1 if AUTO_MOVE_DUPES else 0,
                'running',
                0, 0, 0, 0, 0, 0, 0, 0, 0  # Initialize all detailed stats to 0
            ))
        else:
            cur.execute("""
                INSERT INTO scan_history 
                (start_time, albums_scanned, artists_total, ai_enabled, mb_enabled, auto_move_enabled, status,
                 duplicate_groups_count, total_duplicates_count, broken_albums_count, missing_albums_count,
                 albums_without_artist_image, albums_without_album_image, albums_without_complete_tags,
                 albums_without_mb_id, albums_without_artist_mb_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                start_time,
                total_albums,
                total_artists,
                1 if ai_provider_ready else 0,
                1 if USE_MUSICBRAINZ else 0,
                1 if AUTO_MOVE_DUPES else 0,
                'running',
                0, 0, 0, 0, 0, 0, 0, 0, 0  # Initialize all detailed stats to 0
            ))
        scan_id = cur.lastrowid
        con.commit()
        con.close()
        
        # Store scan_id in state for linking moves
        with lock:
            state["scan_id"] = scan_id

        # Clear scan_editions for this scan_id so only the latest run's data is stored
        con = sqlite3.connect(str(STATE_DB_FILE))
        con.execute("DELETE FROM scan_editions WHERE scan_id = ?", (scan_id,))
        con.commit()
        con.close()

        clear_db_on_new_scan()  # wipe previous duplicate tables

        futures = []
        import concurrent.futures
        future_to_albums: dict[concurrent.futures.Future, int] = {}
        future_to_artist: dict[concurrent.futures.Future, str] = {}
        with ThreadPoolExecutor(max_workers=SCAN_THREADS) as executor:
            for primary_id, artist_name, album_ids_list in artists_merged:
                album_cnt = len(album_ids_list)
                # Track artist before submitting
                with lock:
                    state["scan_active_artists"][artist_name] = {
                        "start_time": time.time(),
                        "total_albums": album_cnt,
                        "albums_processed": 0
                    }
                # Pass (artist_id, artist_name, album_ids) so worker uses combined albums (merged by name)
                fut = executor.submit(scan_artist_duplicates, (primary_id, artist_name, album_ids_list))
                futures.append(fut)
                future_to_albums[fut] = album_cnt
                future_to_artist[fut] = artist_name
            # close the shared connection; workers use their own
            db_conn.close()

            artists_processed = 0
            for future in as_completed(futures):
                # Allow stop/pause mid‑scan
                if scan_should_stop.is_set():
                    break
                album_cnt = future_to_albums.get(future, 0)
                artist_name = future_to_artist.get(future, "<unknown>")
                stats = {"ai_used": 0, "mb_used": 0}
                try:
                    result = future.result()
                    if len(result) == 5:
                        artist_name, groups, _, stats, all_editions = result
                        all_editions_by_artist[artist_name] = all_editions
                    elif len(result) == 4:
                        artist_name, groups, _, stats = result
                        all_editions_by_artist[artist_name] = []
                    else:
                        # Backward compatibility: old format without stats
                        artist_name, groups, _ = result
                        stats = {"ai_used": 0, "mb_used": 0}
                        all_editions_by_artist[artist_name] = []
                except Exception as e:
                    logging.exception("Worker crash for artist %s: %s", artist_name, e)
                    worker_errors.put((artist_name, str(e)))
                    groups = []
                    stats = {"ai_used": 0, "mb_used": 0}
                    all_editions_by_artist[artist_name] = []
                finally:
                    with lock:
                        state["scan_progress"] += album_cnt
                        state["scan_step_progress"] = state.get("scan_step_progress", 0) + album_cnt  # compare step: 1 per album
                        state["scan_artists_processed"] += 1
                        state["scan_ai_used_count"] += stats.get("ai_used", 0)
                        state["scan_mb_used_count"] += stats.get("mb_used", 0)
                        state["scan_audio_cache_hits"] += stats.get("audio_cache_hits", 0)
                        state["scan_audio_cache_misses"] += stats.get("audio_cache_misses", 0)
                        state["scan_mb_cache_hits"] += stats.get("mb_cache_hits", 0)
                        state["scan_mb_cache_misses"] += stats.get("mb_cache_misses", 0)
                        # Aggregate detailed statistics
                        state["scan_duplicate_groups_count"] += stats.get("duplicate_groups_count", 0)
                        state["scan_total_duplicates_count"] += stats.get("total_duplicates_count", 0)
                        state["scan_broken_albums_count"] += stats.get("broken_albums_count", 0)
                        state["scan_albums_without_mb_id"] += stats.get("albums_without_mb_id", 0)
                        state["scan_albums_without_artist_mb_id"] += stats.get("albums_without_artist_mb_id", 0)
                        state["scan_mb_verified_by_ai_count"] += stats.get("mb_verified_by_ai", 0)
                        state["scan_albums_without_complete_tags"] += stats.get("albums_without_complete_tags", 0)
                        state["scan_albums_without_album_image"] += stats.get("albums_without_album_image", 0)
                        state["scan_albums_without_artist_image"] += stats.get("albums_without_artist_image", 0)
                        # Remove artist from active tracking when done
                        if artist_name in state.get("scan_active_artists", {}):
                            del state["scan_active_artists"][artist_name]
                        if groups:
                            all_results[artist_name] = groups
                            state["duplicates"][artist_name] = groups
                    artists_processed += 1
                    # Log scan progress every 10 artists or if debug/verbose
                    if artists_processed % 10 == 0 or logging.getLogger().isEnabledFor(logging.DEBUG):
                        logging.info(f"Scanning artist {artists_processed} / {total_artists}: {artist_name}")

        # Collect all groups requiring AI processing (must be kept in scan_duplicates output, not filtered by "losers")
        ai_groups_to_process = []
        ai_group_positions = {}  # Track position of each AI group for replacement (key = artist + sorted edition ids)
        for artist_name, groups in all_results.items():
            for i, group in enumerate(groups):
                if group.get("needs_ai", False):
                    ai_groups_to_process.append(group)
                    # Normalize to sorted ints so key matches when merging ai_result (same type/order)
                    key = (artist_name, tuple(sorted(int(e['album_id']) for e in group.get("editions", []))))
                    ai_group_positions[key] = (artist_name, i)
        logging.info(
            "Collected %d group(s) requiring AI (from %d artist(s) with duplicates)",
            len(ai_groups_to_process),
            sum(1 for a, grps in all_results.items() if any(g.get("needs_ai") for g in grps)),
        )
        
        # Process AI groups in parallel batch
        if ai_groups_to_process and ai_provider_ready:
            logging.info(f"Processing {len(ai_groups_to_process)} groups requiring AI in parallel batch (max {AI_BATCH_SIZE} concurrent)...")
            with lock:
                state["scan_active_artists"]["_ai_batch"] = {
                    "start_time": time.time(),
                    "total_groups": len(ai_groups_to_process),
                    "groups_processed": 0
                }
                state["scan_ai_batch_total"] = len(ai_groups_to_process)
                state["scan_ai_batch_processed"] = 0
                state["scan_ai_current_label"] = None
            
            ai_results = process_ai_groups_batch(ai_groups_to_process, max_workers=AI_BATCH_SIZE)
            logging.info("AI batch returned %d result(s) for %d group(s)", len(ai_results), len(ai_groups_to_process))
            
            # Update all_results with AI-processed groups; ensure used_ai/ai_provider/ai_model so Unduper shows "AI"
            mod = sys.modules[__name__]
            for ai_result in ai_results:
                try:
                    artist_name = ai_result.get("artist")
                    best = ai_result.get("best")
                    losers = ai_result.get("losers")
                    if not artist_name or best is None or not isinstance(losers, list):
                        continue
                    best.setdefault("used_ai", True)
                    if not best.get("ai_provider"):
                        best["ai_provider"] = getattr(mod, "AI_PROVIDER", None) or ""
                    if not best.get("ai_model"):
                        best["ai_model"] = getattr(mod, "RESOLVED_MODEL", None) or getattr(mod, "OPENAI_MODEL", None) or ""
                    # Normalize to sorted ints to match ai_group_positions key
                    result_edition_ids = tuple(sorted(int(e.get("album_id") or 0) for e in [best] + losers))
                    key = (artist_name, result_edition_ids)
                    
                    if key in ai_group_positions:
                        target_artist, target_index = ai_group_positions[key]
                        if target_artist in all_results and target_index < len(all_results[target_artist]):
                            all_results[target_artist][target_index] = ai_result
                        else:
                            if target_artist not in all_results:
                                all_results[target_artist] = []
                            all_results[target_artist].append(ai_result)
                    else:
                        if artist_name not in all_results:
                            all_results[artist_name] = []
                        all_results[artist_name].append(ai_result)
                except Exception as merge_err:
                    logging.warning("Skipping malformed AI result: %s", merge_err)
            
            # Update state (start_time was stored with time.time(), so use time.time() for duration)
            _ai_batch = state.get("scan_active_artists", {}).get("_ai_batch", {})
            ai_batch_start = _ai_batch.get("start_time", time.time())
            ai_batch_time = time.time() - ai_batch_start
            with lock:
                if "_ai_batch" in state.get("scan_active_artists", {}):
                    del state["scan_active_artists"]["_ai_batch"]
                state.pop("scan_ai_batch_total", None)
                state.pop("scan_ai_batch_processed", None)
                state.pop("scan_ai_current_label", None)
                state["scan_ai_used_count"] += len(ai_results)
                # Update duplicates in state
                state["duplicates"] = all_results
                # Progress: step 2/3 done (albums + AI batch); bar shows e.g. 39/40 until finalize
                state["scan_progress"] = state["scan_total"] - 1
                state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1  # AI batch step done
            logging.info(
                f"AI batch processing completed: {len(ai_results)}/{len(ai_groups_to_process)} groups processed successfully "
                f"in {ai_batch_time:.2f}s (avg {ai_batch_time/max(len(ai_groups_to_process), 1):.2f}s per group)"
            )
        else:
            # No AI batch run (no groups or no provider): still count the AI step so progress reaches step_total
            with lock:
                state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1
        
        # Final pass: any group still with needs_ai and no best/losers -> run AI (no heuristic fallback)
        fallback_count = 0
        for artist_name, groups in list(all_results.items()):
            for i, g in enumerate(groups):
                if g.get("needs_ai", False) and "best" not in g and "editions" in g:
                    editions = g["editions"]
                    if len(editions) >= 2:
                        best = choose_best(editions, defer_ai=False)
                        if best:
                            losers = [e for e in editions if e["album_id"] != best["album_id"]]
                            all_results[artist_name][i] = {
                                "artist": artist_name,
                                "album_id": best["album_id"],
                                "best": best,
                                "losers": losers,
                                "fuzzy": g.get("fuzzy", False),
                                "needs_ai": False,
                            }
                            fallback_count += 1
        if fallback_count:
            logging.info("Final pass: applied AI selection to %d group(s) that had no best/losers.", fallback_count)
            with lock:
                state["scan_ai_used_count"] = state.get("scan_ai_used_count", 0) + fallback_count
                state["duplicates"] = all_results
        
        # Calculate missing albums count (compare Plex albums with MusicBrainz)
        # This is a simplified version - for now, we'll set it to 0 as calculating it requires
        # MusicBrainz API calls for each artist which could be slow during scan
        # This can be implemented later as a post-scan analysis or background task
        missing_albums_total = 0
        with lock:
            state["scan_missing_albums_count"] = missing_albums_total
        
        # Persist is done in finally so we always save (even on stop/exception); auto-move runs synchronously in finally after save.

    finally:
        # "Finalizing": persist to DB and update scan_history before marking scan done.
        # UI shows "Finalizing" until this is complete; only then scanning=False and stats appear.
        with lock:
            state["scan_finalizing"] = True
            state["last_dedupe_moved_count"] = 0
            state["last_dedupe_saved_mb"] = 0
            # Progress: ensure we're at step 2/3 (e.g. 39/40) during finalize; if there was no AI batch we were still at 38
            state["scan_progress"] = max(state["scan_progress"], state["scan_total"] - 1)
            state["scan_step_progress"] = state.get("scan_step_progress", 0) + 1  # finalize step started
        try:
            save_scan_to_db(all_results)
            with lock:
                state["duplicates"] = all_results
        except Exception as e:
            logging.warning("save_scan_to_db in finally failed: %s", e)
        try:
            _scan_id = state.get("scan_id")
            if _scan_id and all_editions_by_artist is not None:
                save_scan_editions_to_db(_scan_id, all_editions_by_artist)
                # So Library always shows last run's editions (even if scan was stopped)
                con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
                con.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_completed_scan_id', ?)", (str(_scan_id),))
                con.commit()
                con.close()
        except Exception as e:
            logging.warning("save_scan_editions_to_db in finally failed: %s", e)
        # Auto-move dupes synchronously so UI shows "Moving dupes" and scan_history gets correct albums_moved/space_saved
        if AUTO_MOVE_DUPES and all_results:
            flat_groups = [g for groups in all_results.values() for g in groups]
            if flat_groups:
                logging.info("Auto-moving dupes enabled, running automatic deduplication (synchronous)...")
                try:
                    background_dedupe(flat_groups)
                except Exception as e:
                    logging.warning("background_dedupe in finally failed: %s", e)
        end_time = time.time()
        scan_id = None
        with lock:
            state["scan_progress"] = state["scan_total"]  # force 100 % before stopping
            state["scan_step_progress"] = state.get("scan_step_total", state["scan_step_progress"])  # ensure 100% for steps
            scan_id = state.get("scan_id")
            start_time = state.get("scan_start_time", end_time)
        
        # Update scan history entry (summary_json etc.); only then mark scan done
        if scan_id:
            con = sqlite3.connect(str(STATE_DB_FILE))
            cur = con.cursor()
            duration = int(end_time - start_time) if start_time else None
            with lock:
                duplicates_found = sum(len(groups) for groups in all_results.values())
                artists_processed = state.get("scan_artists_processed", 0)
                ai_used_count = state.get("scan_ai_used_count", 0)
                mb_used_count = state.get("scan_mb_used_count", 0)
                space_saved = get_stat("space_saved")
                # Get detailed statistics
                duplicate_groups_count = state.get("scan_duplicate_groups_count", 0)
                total_duplicates_count = state.get("scan_total_duplicates_count", 0)
                broken_albums_count = state.get("scan_broken_albums_count", 0)
                missing_albums_count = state.get("scan_missing_albums_count", 0)  # Will be calculated separately
                albums_without_artist_image = state.get("scan_albums_without_artist_image", 0)
                albums_without_album_image = state.get("scan_albums_without_album_image", 0)
                albums_without_complete_tags = state.get("scan_albums_without_complete_tags", 0)
                albums_without_mb_id = state.get("scan_albums_without_mb_id", 0)
                albums_without_artist_mb_id = state.get("scan_albums_without_artist_mb_id", 0)
                # When auto-move ran this scan (in finally), these are set by background_dedupe
                dupes_moved_this_scan = state.get("last_dedupe_moved_count", 0)
                space_saved_mb_this_scan = state.get("last_dedupe_saved_mb", 0)
                albums_moved_this_scan = state.get("last_dedupe_moved_count", 0)
            
            status = 'cancelled' if scan_should_stop.is_set() else 'completed'
            # Build summary_json for end-of-scan summary (FFmpeg formats, MB, AI)
            mb_conn_ok = state.get("scan_mb_connection_ok", False)
            ai_conn_ok = state.get("scan_ai_connection_ok", False)
            mb_done = state.get("scan_mb_done_count", 0)
            mb_used = state.get("scan_mb_used_count", 0)
            ai_groups = state.get("scan_ai_used_count", 0)
            mb_verified_by_ai = state.get("scan_mb_verified_by_ai_count", 0)
            cur.execute(
                "SELECT fmt_text, COUNT(*) FROM scan_editions WHERE scan_id = ? GROUP BY fmt_text",
                (scan_id,),
            )
            ffmpeg_formats = {row[0] or "?": row[1] for row in cur.fetchall()}
            ai_errors_raw = state.get("scan_ai_errors", [])
            # Deduplicate by message, keep last occurrence; limit to 20 for summary
            seen_msg = set()
            ai_errors_dedup = []
            for entry in reversed(ai_errors_raw):
                msg = entry.get("message", "")
                if msg and msg not in seen_msg:
                    seen_msg.add(msg)
                    ai_errors_dedup.append(entry)
                if len(ai_errors_dedup) >= 20:
                    break
            ai_errors_dedup.reverse()
            # Last-scan-only stats for "Last scan summary" UI (only this scan's numbers)
            artists_total = state.get("scan_artists_total", 0)
            albums_scanned = state.get("scan_total_albums", state.get("scan_total", 0))
            scan_discogs_matched = state.get("scan_discogs_matched", 0)
            scan_lastfm_matched = state.get("scan_lastfm_matched", 0)
            scan_bandcamp_matched = state.get("scan_bandcamp_matched", 0)
            audio_hits = state.get("scan_audio_cache_hits", 0)
            audio_misses = state.get("scan_audio_cache_misses", 0)
            mb_hits = state.get("scan_mb_cache_hits", 0)
            mb_misses = state.get("scan_mb_cache_misses", 0)
            albums_without_mb = state.get("scan_albums_without_mb_id", 0)
            albums_with_mb = max(0, albums_scanned - albums_without_mb) if albums_scanned else mb_used
            # Lossy vs lossless from ffmpeg_formats (lossless: FLAC, ALAC, WAV, AIFF, etc.)
            lossless_keys = {"FLAC", "ALAC", "WAV", "AIFF", "APE", "WV", "TAK"}
            lossless_count = sum(c for fmt, c in ffmpeg_formats.items() if (fmt or "").upper().strip() in lossless_keys)
            lossy_count = max(0, sum(ffmpeg_formats.values()) - lossless_count)
            summary = {
                "ffmpeg_formats": ffmpeg_formats,
                "mb_connection_ok": mb_conn_ok,
                "mb_albums_verified": mb_done,
                "mb_albums_identified": mb_used,
                "ai_connection_ok": ai_conn_ok,
                "ai_groups_count": ai_groups,
                "mb_verified_by_ai": mb_verified_by_ai,
                "ai_errors": ai_errors_dedup,
                # Last-scan-only stats for "Last scan summary" UI
                "duration_seconds": duration,
                "artists_total": artists_total,
                "albums_scanned": albums_scanned,
                "duplicate_groups_count": duplicate_groups_count,
                "total_duplicates_count": total_duplicates_count,
                "broken_albums_count": broken_albums_count,
                "missing_albums_count": missing_albums_count,
                "albums_without_artist_image": albums_without_artist_image,
                "albums_without_album_image": albums_without_album_image,
                "albums_without_complete_tags": albums_without_complete_tags,
                "albums_without_mb_id": albums_without_mb_id,
                "albums_without_artist_mb_id": albums_without_artist_mb_id,
                "audio_cache_hits": audio_hits,
                "audio_cache_misses": audio_misses,
                "mb_cache_hits": mb_hits,
                "mb_cache_misses": mb_misses,
                "lossy_count": lossy_count,
                "lossless_count": lossless_count,
                "albums_with_mb_id": albums_with_mb,
                "albums_without_mb_id": albums_without_mb,
                # When auto-move ran this scan
                "dupes_moved_this_scan": dupes_moved_this_scan,
                "space_saved_mb_this_scan": space_saved_mb_this_scan,
                # Fallback sources during scan (when MusicBrainz found nothing)
                "scan_discogs_matched": scan_discogs_matched,
                "scan_lastfm_matched": scan_lastfm_matched,
                "scan_bandcamp_matched": scan_bandcamp_matched,
            }
            summary_json_str = json.dumps(summary)
            cur.execute("""
                UPDATE scan_history
                SET end_time = ?,
                    duration_seconds = ?,
                    duplicates_found = ?,
                    artists_processed = ?,
                    ai_used_count = ?,
                    mb_used_count = ?,
                    space_saved_mb = ?,
                    albums_moved = ?,
                    status = ?,
                    duplicate_groups_count = ?,
                    total_duplicates_count = ?,
                    broken_albums_count = ?,
                    missing_albums_count = ?,
                    albums_without_artist_image = ?,
                    albums_without_album_image = ?,
                    albums_without_complete_tags = ?,
                    albums_without_mb_id = ?,
                    albums_without_artist_mb_id = ?,
                    summary_json = ?
                WHERE scan_id = ?
            """, (
                end_time,
                duration,
                duplicates_found,
                artists_processed,
                ai_used_count,
                mb_used_count,
                space_saved_mb_this_scan,
                albums_moved_this_scan,
                status,
                duplicate_groups_count,
                total_duplicates_count,
                broken_albums_count,
                missing_albums_count,
                albums_without_artist_image,
                albums_without_album_image,
                albums_without_complete_tags,
                albums_without_mb_id,
                albums_without_artist_mb_id,
                summary_json_str,
                scan_id
            ))
            if status == 'completed':
                cur.execute(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES ('last_completed_scan_id', ?)",
                    (str(scan_id),),
                )
            con.commit()
            con.close()
        
        with lock:
            state["scan_finalizing"] = False
            state["scanning"] = False
        logging.debug("background_scan(): finished (flag cleared)")
        duration = time.perf_counter() - start_time
        groups_found = sum(len(v) for v in all_results.values()) if 'all_results' in locals() else 0
        removed_dupes = get_stat("removed_dupes")
        space_saved   = get_stat("space_saved")
        total_artists = len(artists) if 'artists' in locals() else 0
        err_count = worker_errors.qsize()
        if err_count:
            errs = []
            while not worker_errors.empty():
                errs.append(worker_errors.get())
            err_file = CONFIG_DIR / f"scan_errors_{int(time.time())}.log"
            with err_file.open("w", encoding="utf-8") as fh:
                for art, msg in errs:
                    fh.write(f"{art}: {msg}\n")
            logging.warning("⚠️  %d worker errors – details in %s", err_count, err_file)
            notify_discord(
                f"⚠️  PMDA scan finished with {err_count} errors. "
                f"See {err_file.name} for details."
            )
        notify_discord(
            "🟢 PMDA scan completed in "
            f"{duration:.1f}s\n"
            f"Artists: {total_artists}\n"
            f"Albums: {total_albums if 'total_albums' in locals() else 0}\n"
            f"Duplicate groups found: {groups_found}\n"
            f"Duplicates removed so far: {removed_dupes}\n"
            f"Space saved: {space_saved}  MB"
        )

def background_dedupe(all_groups: List[dict]):
    """
    Processes deduplication of all groups in a background thread.
    Updates stats in DB and in-memory state.
    Sets dedupe_current_group so the UI can show artist, album, winner, losers, destination.
    """
    ensure_dedupe_scan_id()
    dupe_root = getattr(sys.modules[__name__], "DUPE_ROOT", Path("/dupes"))
    with lock:
        state.update(
            deduping=True,
            dedupe_progress=0,
            dedupe_total=len(all_groups),
            dedupe_start_time=time.time(),
            dedupe_saved_this_run=0,
            dedupe_current_group=None,
            dedupe_last_write=None,
        )

    total_moved = 0
    removed_count = 0
    artists_to_refresh = set()

    for g in all_groups:
        best = g.get("best", {})
        losers = g.get("losers", [])
        artist = g["artist"]
        album_title = best.get("title_raw", "")
        num_dupes = 1 + len(losers)
        # Ensure folder values are str for JSON (edition dict may store Path)
        current_group = {
            "artist": artist,
            "album": album_title,
            "num_dupes": num_dupes,
            "winner": {
                "title_raw": best.get("title_raw", ""),
                "album_id": best.get("album_id"),
                "folder": str(best.get("folder") or ""),
            },
            "losers": [
                {"title_raw": e.get("title_raw", ""), "album_id": e.get("album_id"), "folder": str(e.get("folder") or "")}
                for e in losers
            ],
            "destination": str(dupe_root),
            "status": "moving",
        }
        with lock:
            state["dedupe_current_group"] = current_group

        moved = perform_dedupe(g)
        removed_count += len(g["losers"])
        group_saved = sum(item["size"] for item in moved)
        total_moved += group_saved
        artists_to_refresh.add(g["artist"])

        with lock:
            state["dedupe_progress"] += 1
            state["dedupe_saved_this_run"] = state.get("dedupe_saved_this_run", 0) + group_saved
            state["dedupe_current_group"] = None
            logging.debug(f"background_dedupe(): processed group for '{artist}|{album_title}', dedupe_progress={state['dedupe_progress']}/{state['dedupe_total']}")
            # Remove this group from in-memory state so the list shrinks on next /api/duplicates
            if artist in state["duplicates"]:
                state["duplicates"][artist].remove(g)
                if not state["duplicates"][artist]:
                    del state["duplicates"][artist]
        # Remove from DB so /api/duplicates (and reload) shows shrinking list
        best_album_id = best.get("album_id")
        loser_album_ids = [e.get("album_id") for e in losers if e.get("album_id") is not None]
        if best_album_id is not None:
            _remove_dedupe_group_from_db(artist, best_album_id, loser_album_ids)

    # Update stats in DB
    increment_stat("space_saved", total_moved)
    increment_stat("removed_dupes", removed_count)
    notify_discord(
        f"🟢 Deduplication finished: {removed_count} duplicate folders moved, "
        f"{total_moved}  MB reclaimed."
    )
    logging.debug(f"background_dedupe(): updated stats: space_saved += {total_moved}, removed_dupes += {removed_count}")

    # Refresh Plex for all affected artists (each section in SECTION_IDS)
    section_ids = getattr(sys.modules[__name__], "SECTION_IDS", []) or []
    for artist in artists_to_refresh:
        letter = quote_plus(artist[0].upper())
        art_enc = quote_plus(artist)
        for sid in section_ids:
            try:
                plex_api(f"/library/sections/{sid}/refresh?path=/music/matched/{letter}/{art_enc}", method="GET")
                plex_api(f"/library/sections/{sid}/emptyTrash", method="PUT")
            except Exception as e:
                logging.warning(f"background_dedupe(): plex refresh/emptyTrash failed for artist={artist} section={sid}: {e}")

    with lock:
        scan_id = state.get("scan_id")
        state["deduping"] = False
        state["dedupe_current_group"] = None
        state["dedupe_last_write"] = None
        state["dedupe_start_time"] = None
        state["dedupe_saved_this_run"] = 0
        # For "Last scan summary": dupes moved and space saved in this run (when auto-move was used)
        state["last_dedupe_moved_count"] = removed_count
        state["last_dedupe_saved_mb"] = total_moved
    if scan_id is not None:
        update_dedupe_scan_summary(scan_id, total_moved, removed_count)
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
    Move each "loser" folder out to DUPE_ROOT, delete metadata in Plex,
    and return a list of dicts describing each moved item.
    Cover is fetched after moves so the first group does not block on Plex API (avoids stuck 1/N).
    """
    moved_items: List[dict] = []
    artist = group["artist"]
    best_title = group["best"]["title_raw"]

    num_losers = len(group["losers"])
    for idx, loser in enumerate(group["losers"], 1):
        src_folder = Path(loser["folder"])
        # Skip if the source folder is absent (e.g. already moved or path mapping issue)
        if not src_folder.exists():
            logging.warning(f"perform_dedupe(): source folder missing – {src_folder}; skipping.")
            continue
        base_dst = build_dupe_destination(src_folder)
        dst = base_dst
        counter = 1
        while dst.exists():
            candidate = base_dst.parent / f"{base_dst.name} ({counter})"
            if not candidate.exists():
                dst = candidate
                break
            counter += 1
        dst.parent.mkdir(parents=True, exist_ok=True)

        logging.info("Moving dupe %s/%s: %s  →  %s", idx, num_losers, src_folder, dst)
        logging.debug("perform_dedupe(): moving %s → %s", src_folder, dst)
        try:
            safe_move(str(src_folder), str(dst))
            with lock:
                state["dedupe_last_write"] = {"path": str(dst), "at": time.time()}
            logging.info("Moved to /dupes: %s", dst)
        except Exception as move_err:
            logging.error("perform_dedupe(): move failed for %s → %s – %s",
                          src_folder, dst, move_err)
            continue

        # warn if something prevented full deletion (e.g. Thumbs.db)
        if src_folder.exists():
            logging.warning("perform_dedupe(): %s was not fully removed (left‑over non‑audio files?)", src_folder)
            notify_discord(f"⚠ Folder **{src_folder.name}** could not be fully removed (non‑audio files locked?). Check manually.")

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

        # Record move in scan_moves table
        moved_at = time.time()
        scan_id = None
        with lock:
            scan_id = state.get("scan_id")
        
        if scan_id:
            try:
                con = sqlite3.connect(str(STATE_DB_FILE))
                cur = con.cursor()
                cur.execute("PRAGMA table_info(scan_moves)")
                move_cols = [r[1] for r in cur.fetchall()]
                if "album_title" in move_cols and "fmt_text" in move_cols:
                    cur.execute("""
                        INSERT INTO scan_moves
                        (scan_id, artist, album_id, original_path, moved_to_path, size_mb, moved_at, album_title, fmt_text)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        scan_id,
                        artist,
                        loser_id,
                        str(src_folder),
                        str(dst),
                        size_mb,
                        moved_at,
                        best_title or "",
                        fmt_text or ""
                    ))
                else:
                    cur.execute("""
                        INSERT INTO scan_moves
                        (scan_id, artist, album_id, original_path, moved_to_path, size_mb, moved_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        scan_id,
                        artist,
                        loser_id,
                        str(src_folder),
                        str(dst),
                        size_mb,
                        moved_at
                    ))
                con.commit()
                con.close()
            except Exception as e:
                logging.warning(f"perform_dedupe(): failed to record move in scan_moves: {e}")

        moved_items.append({
            "artist":    artist,
            "title_raw": best_title,
            "size":      size_mb,
            "fmt":       fmt_text,
            "br":        br_kbps,
            "sr":        sr,
            "bd":        bd,
            "thumb_data": None
        })

    # Fetch cover after moves so we do not block the first group on Plex API (fixes stuck 1/N dedupe).
    cover_data = fetch_cover_as_base64(group["best"]["album_id"]) if group.get("best", {}).get("album_id") else None
    for m in moved_items:
        m["thumb_data"] = cover_data

    return moved_items


# ────────────────────────── UI card helper ──────────────────────────
def _build_card_list(dup_dict) -> list[dict]:
    """
    Convert the nested `state["duplicates"]` dict into the flat list of
    cards expected by both the main page and /api/duplicates.
    """
    cards = []
    db_conn = None
    try:
        db_conn = plex_connect()
    except Exception:
        pass
    for artist, groups in dup_dict.items():
        for g in groups:
            if "best" not in g or "losers" not in g:
                continue
            best = g["best"]
            folder_path = path_for_fs_access(Path(best["folder"]))
            best_fmt = best.get("fmt_text", get_primary_format(folder_path))
            formats = [best_fmt] + [
                loser.get("fmt", get_primary_format(path_for_fs_access(Path(loser["folder"]))))
                for loser in g["losers"]
            ]
            display_title = best["album_norm"].title()
            # Ensure used_ai groups have provider/model for METHOD column (backfill from globals if missing)
            used_ai = best.get("used_ai", False)
            ai_provider = best.get("ai_provider") or ""
            ai_model = best.get("ai_model") or ""
            if used_ai and (not ai_provider or not ai_model):
                mod = sys.modules[__name__]
                ai_provider = ai_provider or (getattr(mod, "AI_PROVIDER", None) or "")
                ai_model = ai_model or (getattr(mod, "RESOLVED_MODEL", None) or getattr(mod, "OPENAI_MODEL", None) or "")
            # Use persisted size_mb/track_count when available (so Unduper shows data after reload)
            if best.get("size_mb") is not None:
                size_mb = int(best["size_mb"])
                size_bytes = size_mb * (1024 * 1024)
            else:
                size_bytes = safe_folder_size(folder_path)
                size_mb = size_bytes // (1024 * 1024)
            if best.get("track_count") is not None:
                track_count = int(best["track_count"])
            else:
                track_count = 0
                if db_conn:
                    try:
                        track_count = len(get_tracks(db_conn, best["album_id"]))
                    except Exception:
                        pass
            cards.append({
                "artist_key": artist.replace(" ", "_"),
                "artist": artist,
                "album_id": best["album_id"],
                "n": len(g["losers"]) + 1,
                "best_thumb": thumb_url(best["album_id"]),
                "best_title": display_title,
                "best_fmt": best_fmt,
                "formats": formats,
                "used_ai": used_ai,
                "ai_provider": ai_provider,
                "ai_model": ai_model,
                "size": size_bytes,
                "size_mb": size_mb,
                "track_count": track_count,
                "path": str(folder_path),
                "no_move": False,
                "match_verified_by_ai": bool(best.get("match_verified_by_ai", False)),
            })
    if db_conn:
        try:
            db_conn.close()
        except Exception:
            pass
    return cards


# --- New scan control endpoints ---
from flask import Response

def _requires_config():
    """Return 503 response when Plex is not configured (wizard-first mode)."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured", "requiresConfig": True}), 503
    return None

def start_background_scan():
    if not ai_provider_ready:
        logging.warning("start_background_scan(): AI not configured; scan not started")
        return
    with lock:
        if not state["scanning"]:
            state.update(scanning=True, scan_progress=0, scan_total=0, scan_step_progress=0, scan_step_total=0)
            state["scan_ai_enabled"] = True
            state["scan_mb_enabled"] = USE_MUSICBRAINZ
            logging.debug("start_scan(): launching background_scan() thread")
            threading.Thread(target=background_scan, daemon=True).start()

def _run_preflight_checks():
    """Run MusicBrainz and AI connectivity checks. Returns (mb_ok: bool, ai_ok: bool).
    For AI, ai_ok reflects ai_provider_ready (functional check is done by _reload_ai_config_and_reinit)."""
    mb_ok = False
    if USE_MUSICBRAINZ:
        try:
            test_mbid = "9162580e-5df4-32de-80cc-f45a8d8a9b1d"
            musicbrainzngs.get_release_group_by_id(test_mbid, includes=[])
            mb_ok = True
        except Exception:
            pass
    ai_ok = bool(ai_provider_ready)
    return mb_ok, ai_ok


def _run_discogs_preflight() -> tuple[bool, str]:
    """Test Discogs API connectivity. Returns (ok, message)."""
    if not USE_DISCOGS or not (getattr(sys.modules[__name__], "DISCOGS_USER_TOKEN", "") or "").strip():
        return False, "Disabled (no token)"
    try:
        import discogs_client
        token = (getattr(sys.modules[__name__], "DISCOGS_USER_TOKEN", "") or "").strip()
        d = discogs_client.Client("PMDA/0.6.6", user_token=token)
        d.identity()
        return True, "Discogs reachable"
    except Exception as e:
        return False, f"Discogs unreachable: {e}"


def _run_lastfm_preflight() -> tuple[bool, str]:
    """Test Last.fm API connectivity. Returns (ok, message)."""
    if not USE_LASTFM or not (getattr(sys.modules[__name__], "LASTFM_API_KEY", "") or "").strip():
        return False, "Disabled (no API key)"
    try:
        api_key = (getattr(sys.modules[__name__], "LASTFM_API_KEY", "") or "").strip()
        resp = requests.get(
            "https://ws.audioscrobbler.com/2.0/",
            params={"method": "album.getInfo", "artist": "Cher", "album": "Believe", "api_key": api_key, "format": "json"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            if "error" in data and data.get("error") != 0:
                return False, f"Last.fm API error: {data.get('message', 'Unknown')}"
            return True, "Last.fm reachable"
        return False, f"Last.fm HTTP {resp.status_code}"
    except Exception as e:
        return False, f"Last.fm unreachable: {e}"


def _fetch_discogs_release(artist_name: str, album_title: str) -> Optional[dict]:
    """
    Search Discogs for a release by artist + album. Returns dict with title, year, cover_url, artist_name, or None.
    Requires USE_DISCOGS and DISCOGS_USER_TOKEN.
    Search can return Masters first; d.release(master_id) would 404. We collect candidate release IDs (from
    type=release or from master.main_release). main_release is a Release object in discogs_client, so we must
    use its .id for d.release(). We try each candidate until one succeeds.
    """
    if not USE_DISCOGS:
        return None
    token = (getattr(sys.modules[__name__], "DISCOGS_USER_TOKEN", "") or "").strip()
    if not token:
        return None
    def _page_to_candidate_ids(d_client, page_list: list) -> list:
        out = []
        for item in page_list or []:
            item_id = getattr(item, "id", None) or (getattr(item, "data", None) or {}).get("id")
            if item_id is None:
                continue
            item_type = getattr(item, "type", None) or (getattr(item, "data", None) or {}).get("type", "release")
            if item_type == "release":
                rid = getattr(item_id, "id", item_id) if not isinstance(item_id, int) else item_id
                out.append(int(rid))
            elif item_type == "master":
                try:
                    master = d_client.master(int(item_id))
                    main = getattr(master, "main_release", None) or (getattr(master, "data", None) or {}).get("main_release")
                    if main is not None:
                        rid = getattr(main, "id", main) if not isinstance(main, int) else main
                        out.append(int(rid))
                except Exception:
                    continue
        return out

    try:
        import discogs_client
        from discogs_client.exceptions import HTTPError
        d = discogs_client.Client("PMDA/0.6.6", user_token=token)
        album_norm = norm_album(album_title) or album_title
        results = d.search(album_norm, artist=artist_name, type="release")
        page = results.page(1)
        candidate_ids = _page_to_candidate_ids(d, page)
        if not candidate_ids:
            combined = f"{artist_name} {album_title}".strip()
            if combined:
                results = d.search(combined, type="release")
                page = results.page(1)
                candidate_ids = _page_to_candidate_ids(d, page)
        seen = set()
        unique_ids = [x for x in candidate_ids if x not in seen and not seen.add(x)]
        for release_id in unique_ids:
            try:
                full_release = d.release(release_id)
                title = getattr(full_release, "title", None) or (full_release.data.get("title") if getattr(full_release, "data", None) else album_title)
                year = getattr(full_release, "year", None) or (full_release.data.get("year") if getattr(full_release, "data", None) else "")
                cover_url = None
                if getattr(full_release, "images", None) and len(full_release.images) > 0:
                    img = full_release.images[0]
                    cover_url = img.get("uri") or img.get("resource_url")
                artist_str = artist_name
                if getattr(full_release, "artists", None) and full_release.artists:
                    artist_str = getattr(full_release.artists[0], "name", None) or artist_name
                return {"title": title, "year": str(year) if year else "", "cover_url": cover_url, "artist_name": artist_str}
            except HTTPError as he:
                if getattr(he, "status_code", None) == 404:
                    continue
                raise
        return None
    except Exception as e:
        logging.warning("Discogs fetch failed for %s / %s: %s", artist_name, album_title, e)
        return None


def _fetch_lastfm_album_info(artist_name: str, album_title: str, mbid: Optional[str] = None) -> Optional[dict]:
    """
    Call Last.fm album.getInfo. Returns dict with cover_url, toptags (list of str), title, artist, or None.
    """
    if not USE_LASTFM:
        return None
    api_key = (getattr(sys.modules[__name__], "LASTFM_API_KEY", "") or "").strip()
    if not api_key:
        return None
    try:
        params = {"method": "album.getInfo", "artist": artist_name, "album": album_title, "api_key": api_key, "format": "json"}
        if mbid:
            params["mbid"] = mbid
        resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data.get("error") and data.get("error") != 0:
            search_params = {"method": "album.search", "album": f"{artist_name} {album_title}".strip(), "api_key": api_key, "format": "json"}
            search_resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=search_params, timeout=10)
            if search_resp.status_code == 200:
                search_data = search_resp.json()
                matches = (search_data.get("results") or {}).get("albummatches") or {}
                album_list = matches.get("album") or []
                if isinstance(album_list, dict):
                    album_list = [album_list]
                if album_list:
                    first = album_list[0]
                    search_artist = (first.get("artist") or "").strip() or artist_name
                    search_album = (first.get("name") or "").strip() or album_title
                    params2 = {"method": "album.getInfo", "artist": search_artist, "album": search_album, "api_key": api_key, "format": "json"}
                    resp2 = requests.get("https://ws.audioscrobbler.com/2.0/", params=params2, timeout=10)
                    if resp2.status_code == 200:
                        data = resp2.json()
                        if not (data.get("error") and data.get("error") != 0):
                            album_title = search_album
                            artist_name = search_artist
            if data.get("error") and data.get("error") != 0:
                return None
        album = data.get("album") or {}
        images = album.get("image") or []
        cover_url = None
        for img in images:
            if img.get("size") == "extralarge" or img.get("#text"):
                cover_url = img.get("#text") or cover_url
        if not cover_url and images:
            cover_url = images[-1].get("#text")
        toptags = []
        for t in (album.get("toptags", {}).get("tag") or []):
            name = t.get("name") if isinstance(t, dict) else str(t)
            if name:
                toptags.append(name)
        mbid = (album.get("mbid") or "").strip()
        return {
            "cover_url": cover_url,
            "toptags": toptags,
            "title": album.get("name") or album_title,
            "artist": album.get("artist", {}).get("name") if isinstance(album.get("artist"), dict) else artist_name,
            "mbid": mbid,
        }
    except Exception as e:
        logging.warning("Last.fm fetch failed for %s / %s: %s", artist_name, album_title, e)
        return None


_last_bandcamp_request = 0.0
_bandcamp_lock = threading.Lock()


def _fetch_bandcamp_album_info(artist_name: str, album_title: str) -> Optional[dict]:
    """
    Search Bandcamp for an album by artist + title. Returns dict with title, artist_name, year, cover_url, or None.
    Uses scraping; no public API. Rate-limited (5s between calls). User-Agent identifies PMDA.
    Use at your own risk regarding Bandcamp ToS.
    """
    if not USE_BANDCAMP:
        return None
    global _last_bandcamp_request
    with _bandcamp_lock:
        now = time.time()
        wait = 5.0 - (now - _last_bandcamp_request)
        if wait > 0:
            time.sleep(wait)
        _last_bandcamp_request = time.time()
    headers = {"User-Agent": "PMDA/1.0 (metadata fallback; https://github.com/silkyclouds/PMDA)"}
    try:
        q = quote_plus(f"{artist_name} {album_title}".strip())
        search_url = f"https://bandcamp.com/search?q={q}"
        resp = requests.get(search_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None
        html = resp.text
        album_clean = (norm_album(album_title) or album_title).lower().replace(" ", "").replace("-", "")
        all_album_links = re.findall(r'href="(https?://[^"]*bandcamp\.com/album/([^"?]+))', html)
        album_url = None
        for full_url, slug in all_album_links:
            slug_clean = (slug or "").lower().replace("-", "").replace("_", "")
            if album_clean and (album_clean in slug_clean or slug_clean in album_clean):
                album_url = full_url.split("?")[0]
                break
        if not album_url and all_album_links:
            album_url = all_album_links[0][0].split("?")[0]
        if not album_url:
            return None
        album_resp = requests.get(album_url, headers=headers, timeout=15)
        if album_resp.status_code != 200:
            return None
        page = album_resp.text
        title = None
        cover_url = None
        og_title = re.search(r'<meta\s+property="og:title"\s+content="([^"]+)"', page)
        if og_title:
            title = og_title.group(1).strip()
        og_image = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', page)
        if og_image:
            cover_url = og_image.group(1).strip()
        # Artist often in og:title as "Album Title, by Artist Name" or in itemprop
        artist_str = artist_name
        if title and " by " in title:
            parts = title.split(" by ", 1)
            title = parts[0].strip()
            if len(parts) > 1:
                artist_str = parts[1].strip()
        if not title:
            title = album_title
        year = ""
        year_m = re.search(r'released\s+(\w+\s+\d{1,2},?\s+\d{4})', page)
        if year_m:
            year = year_m.group(1)
        return {"title": title, "artist_name": artist_str, "year": year, "cover_url": cover_url}
    except Exception as e:
        logging.warning("Bandcamp fetch failed for %s / %s: %s", artist_name, album_title, e)
        return None


def _web_search_serper(query: str, num: int = 10) -> List[dict]:
    """
    Run a web search via Serper.dev API. Returns list of {"title", "link", "snippet"}.
    Empty list if no API key, or on network/API error.
    """
    key = getattr(sys.modules[__name__], "SERPER_API_KEY", "") or ""
    if not key.strip():
        return []
    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": key, "Content-Type": "application/json"}
    body = {"q": query, "num": num}
    try:
        resp = requests.post(url, json=body, headers=headers, timeout=10)
        if resp.status_code != 200:
            logging.debug("[Serper] HTTP %s: %s", resp.status_code, resp.text[:200])
            return []
        data = resp.json()
        organic = data.get("organic") or []
        out = []
        for item in organic:
            if isinstance(item, dict):
                out.append({
                    "title": item.get("title") or "",
                    "link": item.get("link") or "",
                    "snippet": item.get("snippet") or "",
                })
        return out
    except Exception as e:
        logging.debug("[Serper] Request failed: %s", e)
        return []


@app.get("/api/scan/preflight")
def scan_preflight():
    """Check MusicBrainz, AI, Discogs, Last.fm and Bandcamp. Returns clear ok/error for UI."""
    _reload_ai_config_and_reinit()
    mb_ok, ai_ok = _run_preflight_checks()
    provider_name = getattr(sys.modules[__name__], "AI_PROVIDER", None) or "OpenAI"
    resolved_model = getattr(sys.modules[__name__], "RESOLVED_MODEL", None)
    ai_func_err = getattr(sys.modules[__name__], "AI_FUNCTIONAL_ERROR_MSG", None)
    if not USE_MUSICBRAINZ:
        mb_msg = "MusicBrainz disabled in settings"
    elif mb_ok:
        mb_msg = "MusicBrainz reachable"
    else:
        mb_msg = "MusicBrainz unreachable"
    musicbrainz = {"ok": mb_ok, "message": mb_msg}
    if ai_ok:
        ai_msg = f"{provider_name} reachable" + (f", model {resolved_model} (params verified)" if resolved_model else "") if (OPENAI_API_KEY and openai_client) else f"{provider_name} configured"
    else:
        ai_msg = ai_func_err or "No API key or provider configured"
    ai_provider = {"ok": ai_ok, "message": ai_msg, "provider": provider_name}

    discogs_ok, discogs_msg = _run_discogs_preflight()
    discogs = {"ok": discogs_ok, "message": discogs_msg}
    lastfm_ok, lastfm_msg = _run_lastfm_preflight()
    lastfm = {"ok": lastfm_ok, "message": lastfm_msg}
    if USE_BANDCAMP:
        bandcamp = {"ok": True, "message": "Configured (fallback ultime, no connection test)"}
    else:
        bandcamp = {"ok": False, "message": "Disabled"}

    paths = _paths_rw_status()
    return jsonify(musicbrainz=musicbrainz, ai=ai_provider, discogs=discogs, lastfm=lastfm, bandcamp=bandcamp, paths=paths)


@app.route("/scan/start", methods=["POST"])
def start_scan():
    r = _requires_config()
    if r is not None:
        return r
    _reload_ai_config_and_reinit()
    if not ai_provider_ready:
        func_msg = getattr(sys.modules[__name__], "AI_FUNCTIONAL_ERROR_MSG", None)
        if func_msg:
            return jsonify({
                "error": func_msg,
                "aiFunctionalFailure": True,
            }), 503
        return jsonify({
            "error": "Configure the AI provider in Settings to run a scan",
            "requiresAiConfig": True,
        }), 503
    scan_should_stop.clear()
    scan_is_paused.clear()
    start_background_scan()
    return jsonify({"status": "ok"})

@app.route("/scan/pause", methods=["POST"])
def pause_scan():
    scan_is_paused.set()
    with lock:
        state["scanning"] = True   # still scanning, just paused
    return jsonify({"status": "ok"})


@app.route("/scan/resume", methods=["POST"])
def resume_scan():
    scan_is_paused.clear()
    # no state change needed; polling loop will continue
    return jsonify({"status": "ok"})


@app.route("/scan/stop", methods=["POST"])
def stop_scan():
    scan_should_stop.set()
    with lock:
        state["scanning"] = False
    return jsonify({"status": "ok"})


@app.route("/api/scan/clear", methods=["POST"])
def clear_scan():
    """
    Clear all scan results from the database: duplicates, broken albums,
    scan editions (Tag Fixer), and last completed scan id.
    Optionally clear audio and MusicBrainz caches.
    """
    import sqlite3
    data = request.get_json() or {}
    clear_audio_cache = data.get("clear_audio_cache", False)
    clear_mb_cache = data.get("clear_mb_cache", False)
    
    try:
        # Clear all scan-derived data so Unduper, Tag Fixer, Incomplete Albums show nothing
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        cur.execute("DELETE FROM duplicates_loser")
        deleted_losers = cur.rowcount
        cur.execute("DELETE FROM duplicates_best")
        deleted_best = cur.rowcount
        cur.execute("DELETE FROM broken_albums")
        deleted_broken = cur.rowcount
        cur.execute("DELETE FROM scan_editions")
        deleted_editions = cur.rowcount
        cur.execute("DELETE FROM settings WHERE key = 'last_completed_scan_id'")
        # Clear last scan summary so "Last scan summary" UI disappears until next scan
        cur.execute(
            "UPDATE scan_history SET summary_json = NULL WHERE scan_id = (SELECT scan_id FROM scan_history WHERE status = 'completed' AND end_time IS NOT NULL ORDER BY end_time DESC LIMIT 1)"
        )
        con.commit()
        con.close()
        
        # Clear in-memory state
        with lock:
            state["duplicates"] = {}
            state["scan_active_artists"] = {}
        
        result = {
            "status": "ok",
            "message": "Scan results cleared successfully",
            "cleared": {
                "duplicates_best": deleted_best,
                "duplicates_loser": deleted_losers,
                "broken_albums": deleted_broken,
                "scan_editions": deleted_editions,
            }
        }
        
        # Optionally clear audio cache
        if clear_audio_cache:
            con = sqlite3.connect(str(CACHE_DB_FILE))
            cur = con.cursor()
            cur.execute("DELETE FROM audio_cache")
            audio_cache_deleted = cur.rowcount
            con.commit()
            con.close()
            result["cleared"]["audio_cache"] = audio_cache_deleted
            result["message"] += f", {audio_cache_deleted} audio cache entries cleared"
        
        # Optionally clear MusicBrainz cache
        if clear_mb_cache:
            con = sqlite3.connect(str(CACHE_DB_FILE))
            cur = con.cursor()
            cur.execute("DELETE FROM musicbrainz_cache")
            mb_cache_deleted = cur.rowcount
            cur.execute("DELETE FROM musicbrainz_album_lookup")
            mb_album_lookup_deleted = cur.rowcount
            con.commit()
            con.close()
            result["cleared"]["musicbrainz_cache"] = mb_cache_deleted
            result["cleared"]["musicbrainz_album_lookup"] = mb_album_lookup_deleted
            result["message"] += f", {mb_cache_deleted} MB cache + {mb_album_lookup_deleted} album lookup cache cleared"
        
        logging.info("Scan results cleared: %s", result)
        return jsonify(result)
    except Exception as e:
        logging.error("Failed to clear scan results: %s", e, exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


# ────────────────────────── Wizard / Web UI helpers ─────────────────────────

@app.get("/api/plex/check")
def api_plex_check_get():
    """Test Plex connection using current server config (PLEX_HOST, PLEX_TOKEN)."""
    return _do_plex_check(PLEX_HOST, PLEX_TOKEN)


@app.post("/api/plex/check")
def api_plex_check_post():
    """Test Plex connection; optional body { PLEX_HOST, PLEX_TOKEN } to test before saving."""
    data = request.get_json(silent=True) or {}
    host = (data.get("PLEX_HOST") or "").strip() or PLEX_HOST
    token = (data.get("PLEX_TOKEN") or "").strip() or PLEX_TOKEN
    return _do_plex_check(host, token)


def _do_plex_check(host: str, token: str):
    if not host or not token:
        return jsonify({"success": False, "message": "PLEX_HOST and PLEX_TOKEN are required"}), 400
    host = host.strip().rstrip("/")
    if host and not host.startswith(("http://", "https://")):
        host = "http://" + host
    url = f"{host}/library/sections"
    try:
        resp = requests.get(url, headers={"X-Plex-Token": token}, timeout=10)
        if resp.status_code != 200:
            return jsonify({"success": False, "message": f"Plex returned HTTP {resp.status_code}"})
        return jsonify({"success": True, "message": "Connection successful"})
    except requests.exceptions.ConnectionError as e:
        return jsonify({"success": False, "message": "Connection refused or host unreachable. Check URL and that Plex is running."})
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "message": "Connection timed out. Check URL and network."})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


# ─── Plex.tv PIN auth (Tautulli-style: no manual token) ─────────────────────
PLEX_PIN_HEADERS = {
    "X-Plex-Client-Identifier": "pmda-webui-1",
    "X-Plex-Product": "PMDA",
    "X-Plex-Version": "1.0",
    "X-Plex-Device": "Web",
    "X-Plex-Platform": "Web",
    "Accept": "application/json",
}


@app.post("/api/plex/pin")
def api_plex_pin_create():
    """
    Create a Plex.tv PIN for sign-in (like Tautulli "Fetch New Token").
    User opens https://www.plex.tv/link, enters the returned code; we poll GET /api/plex/pin?id=... for the token.
    No auth required. Returns { id, code, link_url }.
    """
    # strong=false (default) → 4-character code for plex.tv/link; strong=true → long code for other flows
    url = "https://plex.tv/api/v2/pins"
    try:
        resp = requests.post(url, headers=PLEX_PIN_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        pin_id = data.get("id")
        code = data.get("code", "")
        if not pin_id:
            return jsonify({"success": False, "message": "Plex did not return a PIN id"}), 502
        return jsonify({
            "success": True,
            "id": pin_id,
            "code": code,
            "link_url": "https://www.plex.tv/link/",
        })
    except requests.exceptions.ConnectionError:
        return jsonify({"success": False, "message": "Cannot reach plex.tv. Check network and DNS."}), 502
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "message": "Request to plex.tv timed out."}), 502
    except (requests.RequestException, ValueError) as e:
        return jsonify({"success": False, "message": str(e)}), 502


@app.get("/api/plex/pin")
def api_plex_pin_poll():
    """
    Poll PIN status. Query param: id (pin id from POST /api/plex/pin).
    Returns { status: 'waiting' } or { status: 'linked', token } when user has signed in on plex.tv/link.
    """
    pin_id = request.args.get("id", "").strip()
    if not pin_id:
        return jsonify({"success": False, "status": "error", "message": "Missing id"}), 400
    url = f"https://plex.tv/api/v2/pins/{pin_id}"
    try:
        resp = requests.get(url, headers=PLEX_PIN_HEADERS, timeout=10)
        if resp.status_code == 404:
            return jsonify({"success": True, "status": "expired", "message": "PIN expired"})
        resp.raise_for_status()
        data = resp.json()
        token = data.get("authToken")
        if token:
            return jsonify({"success": True, "status": "linked", "token": token})
        return jsonify({"success": True, "status": "waiting"})
    except requests.exceptions.ConnectionError:
        return jsonify({"success": False, "status": "error", "message": "Cannot reach plex.tv"}), 502
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "status": "error", "message": "Request timed out"}), 502
    except (requests.RequestException, ValueError) as e:
        return jsonify({"success": False, "status": "error", "message": str(e)}), 502


def _is_lan_address(address: str) -> bool:
    """True if address looks like a classic LAN IP (192.168.x.x or 10.x.x.x), not Docker (172.16-31)."""
    if not address:
        return False
    parts = address.split(".")
    if len(parts) != 4:
        return False
    try:
        a, b, c, d = (int(x) for x in parts)
        if 0 <= a <= 255 and 0 <= b <= 255 and 0 <= c <= 255 and 0 <= d <= 255:
            if (a == 192 and b == 168) or (a == 10):
                return True
            if a == 172 and 16 <= b <= 31:  # Docker/private
                return False
    except (ValueError, TypeError):
        pass
    return False


def _parse_plex_resources_xml(text: str) -> list:
    """Parse plex.tv /api/resources XML (MediaContainer > Device > Connection). Same flow as Tautulli.
    Returns list of { name, uri, address, port, scheme, localAddresses, machineIdentifier }.
    One entry per Device; prefers LAN URL (192.168.x.x / 10.x.x.x) over Docker/plex.direct so the UI shows a reachable URL.
    """
    text = re.sub(r"&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)", "&amp;", text)
    servers = []
    try:
        root = ET.fromstring(text)
    except Exception:
        return []
    for device in root.iter("Device"):
        attr = device.attrib
        provides = (attr.get("provides") or "").strip().lower()
        if "server" not in provides:
            continue
        owned = attr.get("owned", "0")
        if owned != "1":
            continue
        name = attr.get("name", "Plex")
        client_id = attr.get("clientIdentifier", "")
        connections = []
        for conn in device.iter("Connection"):
            c = conn.attrib
            uri = (c.get("uri") or "").strip()
            if not uri or not uri.startswith("http"):
                continue
            address = (c.get("address") or "").strip()
            port = (c.get("port") or "32400").strip()
            if port == "0":
                port = "32400"
            scheme = "https" if uri.startswith("https") else "http"
            is_local = (c.get("local") or "").strip() == "1"
            is_lan = _is_lan_address(address)
            # Prefer: (1) local + LAN IP, (2) local + not Docker, (3) LAN IP, (4) local, (5) any
            if is_local and is_lan:
                rank = 0
            elif is_local and address and not (address.startswith("172.") and _is_private_172(address)):
                rank = 1
            elif is_lan:
                rank = 2
            elif is_local:
                rank = 3
            else:
                rank = 4
            connections.append((rank, address, port, scheme, uri))
        if not connections:
            continue
        connections.sort(key=lambda x: (x[0], x[1] or "zzz"))
        _, address, port, scheme, uri = connections[0]
        # Build a clean URL with dots (not plex.direct with dashes) when we have a real IP
        if address and re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", address):
            display_uri = f"{scheme}://{address}:{port}"
        else:
            display_uri = uri
        servers.append({
            "name": name,
            "uri": display_uri,
            "address": address,
            "port": port,
            "scheme": scheme,
            "localAddresses": address,
            "machineIdentifier": client_id,
        })
    return servers


def _is_private_172(address: str) -> bool:
    """True if address is in 172.16.0.0/12 (e.g. Docker)."""
    parts = address.split(".")
    if len(parts) != 4:
        return False
    try:
        a, b, *_ = (int(x) for x in parts)
        return a == 172 and 16 <= b <= 31
    except (ValueError, TypeError):
        return False


def _parse_plex_servers_xml(text: str) -> list:
    """Parse plex.tv servers XML; fix unescaped & and extract Server elements. Returns list of dicts."""
    text = re.sub(r"&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)", "&amp;", text)

    def _extract_servers_regex() -> list:
        servers = []
        for m in re.finditer(r"<Server\s+([^>]+)/?>", text, re.DOTALL):
            attrs_str = m.group(1)
            attrs = {}
            for a in re.finditer(r'(\w+)="([^"]*)"', attrs_str):
                attrs[a.group(1)] = a.group(2).replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
            name = attrs.get("name", "Plex")
            port = attrs.get("port", "32400")
            if port == "0":
                port = "32400"
            scheme = attrs.get("scheme", "http")
            address = (attrs.get("address") or "").strip()
            local_addresses = (attrs.get("localAddresses") or "").strip()
            if local_addresses:
                first_local = local_addresses.split(",")[0].strip()
                host = first_local or address or "localhost"
            else:
                host = address or "localhost"
            uri = f"{scheme}://{host}:{port}" if host else ""
            servers.append({
                "name": name,
                "uri": uri,
                "address": address,
                "port": port,
                "scheme": scheme,
                "localAddresses": local_addresses,
                "machineIdentifier": attrs.get("machineIdentifier", ""),
            })
        return servers

    try:
        root = ET.fromstring(text)
    except Exception:
        return _extract_servers_regex()
    servers = []
    for server in root.iter("Server"):
        attrs = server.attrib
        name = attrs.get("name", "Plex")
        port = attrs.get("port", "32400")
        if port == "0":
            port = "32400"
        scheme = attrs.get("scheme", "http")
        address = attrs.get("address", "").strip()
        local_addresses = (attrs.get("localAddresses") or "").strip()
        if local_addresses:
            first_local = local_addresses.split(",")[0].strip()
            host = first_local or address or "localhost"
        else:
            host = address or "localhost"
        uri = f"{scheme}://{host}:{port}" if host else ""
        servers.append({
            "name": name,
            "uri": uri,
            "address": address,
            "port": port,
            "scheme": scheme,
            "localAddresses": local_addresses,
            "machineIdentifier": attrs.get("machineIdentifier", ""),
        })
    return servers


@app.post("/api/plex/servers")
def api_plex_servers():
    """
    List Plex servers for the current account (Tautulli-style).
    Body: { "PLEX_TOKEN": "..." }. Returns list of { name, uri, localAddresses, port, ... }.
    Tries plex.tv API v2 (JSON) first, then falls back to servers.xml (XML).
    """
    data = request.get_json(silent=True) or {}
    token = (data.get("PLEX_TOKEN") or data.get("token") or "").strip()
    if not token:
        return jsonify({"success": False, "servers": [], "message": "PLEX_TOKEN is required"}), 400
    headers = {"X-Plex-Token": token, "Accept": "application/json"}

    def _build_servers_from_servers_json(data: dict) -> list:
        """Build server list from Plex GET /servers JSON (MediaContainer.Server[]).
        Docs: https://plexapi.dev/api-reference/server/get-server-list
        """
        servers = []
        if not isinstance(data, dict):
            return servers
        media = data.get("MediaContainer", data.get("mediaContainer"))
        if not isinstance(media, dict):
            return servers
        items = media.get("Server") or media.get("server") or []
        if not isinstance(items, list):
            items = [items] if items else []
        for s in items:
            if not isinstance(s, dict):
                continue
            def _g(k, default=None):
                return s.get(k) or s.get(k[0].upper() + k[1:] if k else k) or default
            name = _g("name", "Plex")
            port = str(_g("port") or "32400")
            if port == "0":
                port = "32400"
            scheme = (str(_g("scheme") or "http")).strip().lower()
            if scheme not in ("http", "https"):
                scheme = "http"
            host = (str(_g("host") or _g("address") or "")).strip()
            address = (str(_g("address") or host or "")).strip()
            local_addresses = (str(_g("localAddresses") or address or "")).strip()
            if local_addresses:
                host = local_addresses.split(",")[0].strip() or host
            if not host:
                continue
            uri = f"{scheme}://{host}:{port}"
            servers.append({
                "name": name,
                "uri": uri,
                "address": address,
                "port": port,
                "scheme": scheme,
                "localAddresses": local_addresses,
                "machineIdentifier": str(_g("machineIdentifier") or ""),
            })
        return servers

    try:
        seen_machine_ids: set[str] = set()
        servers: list[dict] = []

        # 1) servers.xml first — often returns all servers (including multiple instances on same host, e.g. :32400 + :32401)
        resp_xml = requests.get(
            f"https://plex.tv/servers.xml?includeLite=1&X-Plex-Token={requests.utils.quote(token, safe='')}",
            headers={"X-Plex-Token": token},
            timeout=15,
        )
        if resp_xml.status_code == 401:
            return jsonify({"success": False, "servers": [], "message": "Invalid Plex token"}), 401
        if resp_xml.ok:
            try:
                from_xml = _parse_plex_servers_xml(resp_xml.text)
                for s in from_xml:
                    mid = (s.get("machineIdentifier") or "").strip()
                    if not mid or mid in seen_machine_ids:
                        continue
                    servers.append(s)
                    seen_machine_ids.add(mid)
            except Exception:
                pass

        # 2) GET https://plex.tv/api/resources?includeHttps=1 — merge in any device not yet listed (by machineIdentifier)
        resp_resources = requests.get(
            "https://plex.tv/api/resources?includeHttps=1",
            headers={"X-Plex-Token": token},
            timeout=15,
        )
        if resp_resources.status_code == 401:
            return jsonify({"success": False, "servers": [], "message": "Invalid Plex token"}), 401
        if resp_resources.ok:
            from_resources = _parse_plex_resources_xml(resp_resources.text)
            for s in from_resources:
                mid = (s.get("machineIdentifier") or "").strip()
                if not mid or mid in seen_machine_ids:
                    continue
                # Avoid duplicate by (name, port) in case machineIdentifier differs
                if any(x.get("name") == s.get("name") and x.get("port") == s.get("port") for x in servers):
                    continue
                servers.append(s)
                seen_machine_ids.add(mid)

        # 3) GET https://plex.tv/servers (JSON or XML) — merge any remaining
        resp = requests.get("https://plex.tv/servers", headers=headers, timeout=15)
        if resp.status_code != 401 and resp.ok:
            ct = (resp.headers.get("Content-Type") or "").lower()
            if "json" in ct:
                try:
                    data = resp.json()
                    for s in _build_servers_from_servers_json(data):
                        mid = (s.get("machineIdentifier") or "").strip()
                        if mid and mid not in seen_machine_ids:
                            servers.append(s)
                            seen_machine_ids.add(mid)
                except (ValueError, TypeError, KeyError):
                    pass
            for s in _parse_plex_servers_xml(resp.text):
                mid = (s.get("machineIdentifier") or "").strip()
                if mid and mid not in seen_machine_ids:
                    servers.append(s)
                    seen_machine_ids.add(mid)

        if servers:
            return jsonify({"success": True, "servers": servers})
        # Empty list: token accepted but no servers linked to account
        return jsonify({
            "success": True,
            "servers": [],
            "message": "No Plex servers found for this account. Link your server at plex.tv or check the token.",
        })

    except requests.exceptions.ConnectionError:
        return jsonify({"success": False, "servers": [], "message": "Cannot reach plex.tv. Check network and DNS from the machine running PMDA (e.g. Docker has outbound internet)."}), 502
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "servers": [], "message": "Request to plex.tv timed out. Check network."}), 502
    except requests.RequestException as e:
        return jsonify({"success": False, "servers": [], "message": str(e)}), 502


# ─── Plex database path hints (official locations by platform / image) ─────────
# Full paths to the folder containing com.plexapp.plugins.library.db (for wizard help)
# Source: https://support.plex.tv/articles/202915258-where-is-the-plex-media-server-data-directory-located/
PLEX_DATABASE_PATH_HINTS: list[dict] = [
    {"platform": "Docker (generic)", "path": "<config_mount>/Library/Application Support/Plex Media Server/Plug-in Support/Databases", "note": "Mount the host path that maps to /config in the Plex container."},
    {"platform": "plexinc/pms-docker", "path": "<config_volume>/Library/Application Support/Plex Media Server/Plug-in Support/Databases", "note": "Same as Docker generic; -v host_path:/config."},
    {"platform": "linuxserver/plex", "path": "<config_volume>/Library/Application Support/Plex Media Server/Plug-in Support/Databases", "note": "Same as Docker generic."},
    {"platform": "Debian / Ubuntu / Fedora / CentOS", "path": "/var/lib/plexmediaserver/Library/Application Support/Plex Media Server/Plug-in Support/Databases", "note": "Native Linux package."},
    {"platform": "FreeBSD", "path": "/usr/local/plexdata/Plex Media Server/Plug-in Support/Databases", "note": "Native install."},
    {"platform": "macOS", "path": "~/Library/Application Support/Plex Media Server/Plug-in Support/Databases", "note": "Expand ~ to your home."},
    {"platform": "Windows", "path": "%LOCALAPPDATA%\\Plex Media Server\\Plug-in Support\\Databases", "note": "Per-user (account running Plex)."},
    {"platform": "Synology DSM 7", "path": "/volume1/PlexMediaServer/AppData/Plex Media Server/Plug-in Support/Databases", "note": "Volume name may vary."},
    {"platform": "Synology DSM 6", "path": "/volume1/Plex/Library/Application Support/Plex Media Server/Plug-in Support/Databases", "note": "Volume name may vary."},
    {"platform": "QNAP", "path": "<Install_path>/Library/Plex Media Server/Plug-in Support/Databases", "note": "Install_path from: getcfg -f /etc/config/qpkg.conf PlexMediaServer Install_path."},
    {"platform": "ASUSTOR", "path": "/volume1/Plex/Library/Plug-in Support/Databases", "note": "Under Plex data directory."},
    {"platform": "FreeNAS 11.3+", "path": "${JAIL_ROOT}/Plex Media Server/Plug-in Support/Databases", "note": "JAIL_ROOT is the jail root."},
    {"platform": "Snap", "path": "/var/snap/plexmediaserver/common/Library/Application Support/Plex Media Server/Plug-in Support/Databases", "note": "Snap package."},
    {"platform": "ReadyNAS", "path": "/apps/plexmediaserver/MediaLibrary/Plex Media Server/Plug-in Support/Databases", "note": ""},
    {"platform": "TerraMaster", "path": "/Volume1/Plex/Library/Application Support/Plex Media Server/Plug-in Support/Databases", "note": ""},
]


def _gdm_discover_servers(timeout_sec: float = 2.0) -> list[dict]:
    """
    Discover Plex Media Servers on the local network via GDM (Good Day Mate) multicast.
    Sends M-SEARCH to 239.0.0.250:32414 and parses HTTP/1.0 200 OK responses.
    Returns list of dicts with name, uri (http://ip:port), address, port.
    """
    gdm_ip, gdm_port = "239.0.0.250", 32414
    msg = b"M-SEARCH * HTTP/1.0"
    seen: set[str] = set()
    result: list[dict] = []
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("B", 2))
    sock.settimeout(0.5)
    try:
        sock.sendto(msg, (gdm_ip, gdm_port))
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            try:
                bdata, from_addr = sock.recvfrom(1024)
            except socket.timeout:
                continue
            data = bdata.decode("utf-8", errors="replace")
            lines = data.splitlines()
            if not lines or "200 OK" not in lines[0]:
                continue
            ddata: dict[str, str] = {}
            for line in lines[1:]:
                if ":" in line:
                    k, _, v = line.partition(":")
                    ddata[k.strip()] = v.strip()
            if ddata.get("Content-Type") != "plex/media-server":
                continue
            rid = ddata.get("Resource-Identifier") or ""
            if rid in seen:
                continue
            seen.add(rid)
            name = ddata.get("Name", "Plex Media Server")
            port = ddata.get("Port", "32400")
            from_ip = from_addr[0]
            uri = f"http://{from_ip}:{port}"
            result.append({
                "name": name,
                "uri": uri,
                "address": from_ip,
                "port": port,
                "scheme": "http",
                "localAddresses": from_ip,
                "machineIdentifier": rid,
            })
    except Exception as e:
        logging.warning("GDM discover failed: %s", e)
    finally:
        sock.close()
    return result


def _probe_plex_at(host: str, port: int) -> dict | None:
    """
    Probe a single host:port to see if Plex is listening (GET /identity).
    Returns a server dict like GDM format, or None if not Plex / unreachable.
    """
    url = f"http://{host}:{port}/identity"
    try:
        resp = requests.get(url, timeout=2)
        if resp.status_code != 200:
            return None
        root = ET.fromstring(resp.text)
        if root.tag != "MediaContainer":
            return None
        rid = root.attrib.get("machineIdentifier", "")
        version = root.attrib.get("version", "")
        name = f"Plex Media Server ({host}:{port})"
        uri = f"http://{host}:{port}"
        return {
            "name": name,
            "uri": uri,
            "address": host,
            "port": str(port),
            "scheme": "http",
            "localAddresses": host,
            "machineIdentifier": rid,
        }
    except Exception:
        return None


def _discover_via_host_fallback(host_str: str, extra_ports: list[int] | None = None) -> list[dict]:
    """
    When GDM fails (e.g. in Docker, multicast is not forwarded), try the host
    the user used to reach PMDA (from Host header) and the Docker gateway (172.17.0.1).
    Tries common Plex ports 32400, 32401, 32402 and any extra_ports.
    """
    ports = [32400, 32401, 32402]
    if extra_ports:
        ports = list(dict.fromkeys(ports + list(extra_ports)))
    candidates: list[str] = []
    host = (host_str or "").strip()
    if host and not host.startswith("["):
        if ":" in host:
            host = host.rsplit(":", 1)[0]
        if host and host not in ("localhost", "127.0.0.1"):
            candidates.append(host)
    # From inside Docker, the host is often reachable via the bridge gateway
    candidates.append("172.17.0.1")
    result: list[dict] = []
    seen_uris: set[str] = set()
    for h in candidates:
        for port in ports:
            entry = _probe_plex_at(h, port)
            if entry and entry["uri"] not in seen_uris:
                result.append(entry)
                seen_uris.add(entry["uri"])
    return result


def _parse_port_from_url(url: str) -> int | None:
    """Extract port from http(s)://host:port if present."""
    if not url or not isinstance(url, str):
        return None
    url = url.strip()
    for prefix in ("https://", "http://"):
        if url.startswith(prefix):
            url = url[len(prefix) :].split("/")[0]
            break
    if ":" in url:
        try:
            return int(url.rsplit(":", 1)[1])
        except ValueError:
            return None
    return None


def _subnet_ips_24(ip_str: str) -> list[str]:
    """
    Given an IPv4 address, return a list of all /24 host IPs (x.y.z.1 .. x.y.z.254).
    Returns [] if the string is not a valid IPv4.
    """
    parts = ip_str.strip().split(".")
    if len(parts) != 4:
        return []
    try:
        a, b, c, _ = (int(p) for p in parts)
        if not all(0 <= x <= 255 for x in (a, b, c)):
            return []
        return [f"{a}.{b}.{c}.{i}" for i in range(1, 255)]
    except ValueError:
        return []


def _discover_via_subnet(client_ip: str, ports: list[int] | None = None) -> list[dict]:
    """
    Scan the client's /24 subnet for Plex (ports 32400, 32401, 32402 by default).
    Uses a thread pool and short timeouts to complete in a few seconds.
    """
    if not client_ip or client_ip.startswith("127.") or client_ip == "::1":
        return []
    ips = _subnet_ips_24(client_ip)
    if not ips:
        return []
    port_list = ports or [32400, 32401, 32402]
    seen_uris: set[str] = set()
    result: list[dict] = []
    max_workers = 48

    def probe(host: str, port: int) -> dict | None:
        return _probe_plex_at(host, port)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(probe, ip, port): (ip, port) for ip in ips for port in port_list}
        for future in as_completed(futures, timeout=60):
            try:
                entry = future.result()
                if entry and entry["uri"] not in seen_uris:
                    result.append(entry)
                    seen_uris.add(entry["uri"])
            except Exception:
                pass
    return result


@app.get("/api/plex/client-ip")
def api_plex_client_ip():
    """
    Return the IP address of the client that is viewing the WebUI.
    Uses X-Forwarded-For (first hop) when behind a proxy, else request.remote_addr.
    Allows the frontend to request a discovery scan of that client's subnet (same LAN).
    """
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()
    else:
        client_ip = request.remote_addr or ""
    return jsonify({"client_ip": client_ip})


@app.get("/api/plex/discover")
@app.post("/api/plex/discover")
def api_plex_discover():
    """
    Discover Plex Media Servers: GDM multicast first, then fallback to probing
    the host the user used to reach PMDA and Docker gateway 172.17.0.1 (ports 32400–32402).
    POST body may include:
      - "PLEX_HOST": "..." to add that URL's port to the probe list;
      - "client_ip": "x.y.z.w" to scan the client's /24 subnet (same LAN as the machine viewing the WebUI).
    No token required. Returns same shape as /api/plex/servers: { success, servers: [ { name, uri, ... } ] }.
    """
    servers = _gdm_discover_servers()
    extra_ports: list[int] = []
    client_ip: str | None = None
    try:
        data = request.get_json(silent=True) or {}
        if data.get("PLEX_HOST"):
            p = _parse_port_from_url(str(data.get("PLEX_HOST", "")))
            if p and p not in (32400, 32401, 32402):
                extra_ports.append(p)
        client_ip = (data.get("client_ip") or "").strip() or None
    except Exception:
        pass
    try:
        host_header = request.headers.get("Host") or request.host or ""
        fallback = _discover_via_host_fallback(host_header, extra_ports=extra_ports or None)
        seen_uris = {s["uri"] for s in servers}
        for s in fallback:
            if s["uri"] not in seen_uris:
                servers.append(s)
                seen_uris.add(s["uri"])
    except Exception as e:
        logging.debug("Discover host fallback failed: %s", e)
    if client_ip:
        try:
            subnet_servers = _discover_via_subnet(client_ip)
            seen_uris = {s["uri"] for s in servers}
            for s in subnet_servers:
                if s["uri"] not in seen_uris:
                    servers.append(s)
                    seen_uris.add(s["uri"])
        except Exception as e:
            logging.debug("Discover subnet failed: %s", e)
    return jsonify({"success": True, "servers": servers})


@app.get("/api/plex/database-paths")
def api_plex_database_paths():
    """
    Return common Plex database directory locations (by platform / image)
    so the wizard can suggest where to mount or point to.
    """
    return jsonify({"success": True, "paths": PLEX_DATABASE_PATH_HINTS})


@app.get("/api/plex/verify-db")
def api_plex_verify_db():
    """
    Verify that the current Plex DB file exists and is readable (e.g. open read-only and run a trivial query).
    Returns { success: true } or { success: false, message: "..." }.
    """
    if not Path(PLEX_DB_FILE).exists():
        return jsonify({"success": False, "message": "Plex database file not found at " + PLEX_DB_FILE}), 200
    try:
        con = sqlite3.connect(f"file:{PLEX_DB_FILE}?mode=ro", uri=True, timeout=5)
        con.execute("SELECT 1 FROM metadata_items LIMIT 1")
        con.close()
    except sqlite3.OperationalError as e:
        return jsonify({"success": False, "message": "Plex database not readable or invalid: " + str(e)}), 200
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 200
    return jsonify({"success": True})


@app.post("/api/autodetect/libraries")
def api_autodetect_libraries():
    """Return list of Plex libraries (sections) for the wizard. Uses config or optional body { PLEX_HOST, PLEX_TOKEN }."""
    data = request.get_json(silent=True) or {}
    host = (data.get("PLEX_HOST") or "").strip() or PLEX_HOST
    token = (data.get("PLEX_TOKEN") or "").strip() or PLEX_TOKEN
    if not host or not token:
        return jsonify({"success": False, "libraries": [], "message": "PLEX_HOST and PLEX_TOKEN required"}), 400
    url = f"{host.rstrip('/')}/library/sections"
    try:
        resp = requests.get(url, headers={"X-Plex-Token": token}, timeout=10)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        libraries = []
        for d in root.iter("Directory"):
            libraries.append({
                "id": d.attrib.get("key", ""),
                "name": d.attrib.get("title", ""),
                "type": d.attrib.get("type", ""),
            })
        return jsonify({"success": True, "libraries": libraries})
    except Exception as e:
        logging.warning("Autodetect libraries failed: %s", e)
        return jsonify({"success": False, "libraries": [], "message": str(e)})


@app.post("/api/autodetect/paths")
def api_autodetect_paths():
    """Discover PATH_MAP from Plex for given sections. Uses config or optional body { PLEX_HOST, PLEX_TOKEN, SECTION_IDS }."""
    data = request.get_json(silent=True) or {}
    host = (data.get("PLEX_HOST") or "").strip() or PLEX_HOST
    token = (data.get("PLEX_TOKEN") or "").strip() or PLEX_TOKEN
    section_ids = data.get("SECTION_IDS")
    if section_ids is None:
        section_ids = list(SECTION_IDS)
    elif isinstance(section_ids, str):
        section_ids = [int(x.strip()) for x in section_ids.split(",") if x.strip()]
    elif isinstance(section_ids, list):
        section_ids = [int(x) for x in section_ids]
    if not host or not token:
        return jsonify({"success": False, "paths": {}, "message": "PLEX_HOST and PLEX_TOKEN required"}), 400
    if not section_ids:
        return jsonify({"success": False, "paths": {}, "message": "No SECTION_IDS provided"}), 400
    paths = {}
    errors = []
    for sid in section_ids:
        try:
            part = _discover_path_map(host, token, sid)
            paths.update(part)
        except Exception as e:
            logging.warning("Autodetect paths failed for section %s: %s", sid, e)
            errors.append(f"Section {sid}: {e}")
    if not paths and errors:
        return jsonify({"success": False, "paths": {}, "message": "; ".join(errors)})
    return jsonify({"success": True, "paths": paths, "message": "; ".join(errors) if errors else None})


def _path_map_from_verify_body(raw):
    """Parse PATH_MAP from verify request: dict, or string with key=value lines or key:value pairs."""
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return {}
    if s.startswith("{"):
        try:
            data = json.loads(s)
            return {str(k): str(v) for k, v in data.items()}
        except json.JSONDecodeError:
            pass
    out = {}
    for line in s.replace(",", "\n").splitlines():
        line = line.strip()
        if "=" in line:
            k, _, v = line.partition("=")
            if k.strip():
                out[k.strip()] = v.strip()
        elif ":" in line:
            k, _, v = line.partition(":")
            if k.strip():
                out[k.strip()] = v.strip()
    return out


@app.post("/api/paths/discover")
def api_paths_discover():
    """
    Discover actual container paths for each Plex library root by content matching: sample files
    from Plex DB and find which subdir of MUSIC_PARENT_PATH contains them. Body: PATH_MAP
    (required, e.g. from autodetect/paths), PLEX_DB_PATH?, MUSIC_PARENT_PATH?, CROSSCHECK_SAMPLES?.
    Returns { success, paths: discovered map, results: list of result dicts }.
    """
    data = request.get_json(silent=True) or {}
    path_map_raw = data.get("PATH_MAP")
    path_map = _path_map_from_verify_body(path_map_raw) if path_map_raw is not None else {}
    if not path_map:
        return jsonify({
            "success": False,
            "paths": {},
            "results": [],
            "message": "PATH_MAP is required and must not be empty",
        }), 400
    db_path = (data.get("PLEX_DB_PATH") or "").strip() or merged.get("PLEX_DB_PATH") or ""
    if not db_path:
        db_path = "/database"
    db_file = str(Path(db_path) / "com.plexapp.plugins.library.db")
    music_root = (data.get("MUSIC_PARENT_PATH") or "").strip() or merged.get("MUSIC_PARENT_PATH") or "/music"
    samples = max(1, int(data.get("CROSSCHECK_SAMPLES") or CROSSCHECK_SAMPLES or 15))
    if not Path(db_file).exists():
        return jsonify({
            "success": False,
            "paths": {},
            "results": [],
            "message": f"Plex DB not found: {db_file}",
        }), 400
    music_path = Path(music_root)
    if not music_path.exists() or not music_path.is_dir():
        return jsonify({
            "success": False,
            "paths": {},
            "results": [],
            "message": f"Music parent path not found or not a directory: {music_root}",
        }), 400
    out = _discover_bindings_by_content(path_map, db_file, music_root, samples)
    if out is None:
        return jsonify({
            "success": False,
            "paths": {},
            "results": [],
            "message": "Discover by content failed",
        }), 500
    discovered_map, results = out
    return jsonify({
        "success": True,
        "paths": discovered_map,
        "results": results,
    })


@app.post("/api/paths/discover-one")
def api_paths_discover_one():
    """
    Discover the actual container path for a single Plex root by content matching.
    Body: { plex_root (required), PLEX_DB_PATH?, MUSIC_PARENT_PATH?, CROSSCHECK_SAMPLES? }.
    Returns { success, host_root?, result } so the UI can show progress per mapping.
    """
    data = request.get_json(silent=True) or {}
    plex_root = (data.get("plex_root") or "").strip()
    if not plex_root:
        return jsonify({"success": False, "host_root": None, "result": None, "message": "plex_root is required"}), 400
    db_path = (data.get("PLEX_DB_PATH") or "").strip() or merged.get("PLEX_DB_PATH") or ""
    if not db_path:
        db_path = "/database"
    db_file = str(Path(db_path) / "com.plexapp.plugins.library.db")
    music_root = (data.get("MUSIC_PARENT_PATH") or "").strip() or merged.get("MUSIC_PARENT_PATH") or "/music"
    samples = max(1, int(data.get("CROSSCHECK_SAMPLES") or CROSSCHECK_SAMPLES or 15))
    out = _discover_one_binding(plex_root, db_file, music_root, samples)
    if out is None:
        return jsonify({
            "success": False,
            "host_root": None,
            "result": None,
            "message": "Discover failed (DB or music root invalid)",
        }), 500
    host_root, result = out
    success = host_root is not None
    return jsonify({
        "success": success,
        "host_root": host_root,
        "result": result,
        "message": (result.get("message") or "") if not success and isinstance(result, dict) else None,
    })


# Last path verification result (for GET /api/paths/verify/last)
_last_path_verify_result: list = []
_last_path_verify_at: float = 0.0


@app.get("/api/paths/verify/last")
def api_paths_verify_last():
    """Return the last path verification result and timestamp (from last POST /api/paths/verify)."""
    return jsonify({"results": _last_path_verify_result, "at": _last_path_verify_at if _last_path_verify_at else None})


@app.post("/api/paths/verify")
def api_paths_verify():
    """
    Verify PATH_MAP bindings by sampling tracks from the Plex DB and checking file existence.
    Body: { PATH_MAP?, PLEX_DB_PATH?, CROSSCHECK_SAMPLES? }. Returns list of { plex_root, host_root, status, samples_checked, message }.
    Does not modify config.
    """
    global _last_path_verify_result, _last_path_verify_at
    raw_json = request.get_json(silent=True)
    data = raw_json if isinstance(raw_json, dict) else {}
    if raw_json is None:
        logging.warning("Paths verify: request body is not valid JSON (Content-Type: %s)", request.content_type)
    logging.info(
        "Paths verify: request body keys=%s, PATH_MAP type=%s",
        list(data.keys()), type(data.get("PATH_MAP")).__name__ if "PATH_MAP" in data else "missing",
    )
    path_map_raw = data.get("PATH_MAP")
    path_map = _path_map_from_verify_body(path_map_raw)
    if path_map is None:
        path_map = dict(getattr(sys.modules[__name__], "PATH_MAP", {}))
        logging.info("Paths verify: no PATH_MAP in body, using server config → %d entries", len(path_map))
    elif not path_map and path_map_raw is not None:
        path_map = dict(getattr(sys.modules[__name__], "PATH_MAP", {}))
        logging.info(
            "Paths verify: parsed PATH_MAP from body was empty (raw type=%s), using server config → %d entries",
            type(path_map_raw).__name__, len(path_map),
        )
    if not path_map:
        logging.warning(
            "Paths verify: returning 400 – PATH_MAP is empty (body keys=%s)",
            list(data.keys()),
        )
        return jsonify({"success": False, "results": [], "message": "PATH_MAP is empty"}), 400
    db_path = (data.get("PLEX_DB_PATH") or "").strip() or merged.get("PLEX_DB_PATH") or ""
    if not db_path:
        db_path = "/database"
    db_file = str(Path(db_path) / "com.plexapp.plugins.library.db")
    samples = max(0, int(data.get("CROSSCHECK_SAMPLES") or CROSSCHECK_SAMPLES))
    if not Path(db_file).exists():
        logging.warning(
            "Paths verify: returning 400 – Plex DB not found at %s (PLEX_DB_PATH=%s)",
            db_file, db_path,
        )
        return jsonify({"success": False, "results": [], "message": f"Plex DB not found: {db_file}"}), 400
    try:
        results = _run_path_verification(path_map, db_file, samples or CROSSCHECK_SAMPLES)
    except Exception as e:
        logging.warning("Paths verify exception: %s", e)
        return jsonify({"success": False, "results": [], "message": str(e) or "Path verification failed"}), 500
    if results is None:
        return jsonify({"success": False, "results": [], "message": "Path verification failed"}), 500
    _last_path_verify_result = list(results)
    _last_path_verify_at = time.time()
    has_failures = any(r.get("status") == "fail" for r in results)
    hint = None
    if has_failures:
        hint = (
            "Files were not found inside the container. Start Docker with a volume mount from your "
            "host music folder to the path shown above (e.g. -v /path/on/host/music:/music). "
            "Use the same path as 'Path to parent folder (music root)' in this step."
        )
    out = {"success": True, "results": results}
    if hint:
        out["hint"] = hint
    return jsonify(out)


@app.post("/api/openai/check")
def api_openai_check():
    """Test OpenAI API key; optional body { OPENAI_API_KEY } to test before saving."""
    data = request.get_json(silent=True) or {}
    key = (data.get("OPENAI_API_KEY") or "").strip() or OPENAI_API_KEY
    if not key:
        return jsonify({"success": False, "message": "OPENAI_API_KEY is required"}), 400
    
    # Validate key format (should start with sk-)
    if not key.startswith("sk-"):
        return jsonify({"success": False, "message": "Invalid API key format. OpenAI keys start with 'sk-'"}), 400
    
    try:
        client = OpenAI(api_key=key)
        # Try with max_completion_tokens first (newer API)
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": "OK"}],
                max_completion_tokens=5,
            )
            # Verify we got a response
            if response.choices and len(response.choices) > 0:
                return jsonify({"success": True, "message": "OpenAI connection successful"})
        except Exception as e1:
            error_msg = str(e1)
            # If max_completion_tokens is not supported, try max_tokens
            if "max_completion_tokens" in error_msg or "unsupported_parameter" in error_msg.lower():
                try:
                    response = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[{"role": "user", "content": "OK"}],
                        max_tokens=5,
                    )
                    if response.choices and len(response.choices) > 0:
                        return jsonify({"success": True, "message": "OpenAI connection successful"})
                except Exception as e2:
                    # If both fail, check for authentication errors
                    error_msg2 = str(e2)
                    if "invalid_api_key" in error_msg2.lower() or "authentication" in error_msg2.lower() or "401" in error_msg2:
                        return jsonify({"success": False, "message": "Invalid API key. Please check your key and try again."}), 401
                    elif "insufficient_quota" in error_msg2.lower() or "quota" in error_msg2.lower():
                        return jsonify({"success": False, "message": "API key has insufficient quota. Please check your OpenAI account billing."}), 402
                    else:
                        logging.warning("OpenAI check failed with max_tokens: %s", error_msg2)
                        return jsonify({"success": False, "message": f"OpenAI API error: {error_msg2}"}), 500
            else:
                # Other error (auth, quota, etc.)
                if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg:
                    return jsonify({"success": False, "message": "Invalid API key. Please check your key and try again."}), 401
                elif "insufficient_quota" in error_msg.lower() or "quota" in error_msg.lower():
                    return jsonify({"success": False, "message": "API key has insufficient quota. Please check your OpenAI account billing."}), 402
                else:
                    logging.warning("OpenAI check failed: %s", error_msg)
                    return jsonify({"success": False, "message": f"OpenAI API error: {error_msg}"}), 500
    except Exception as e:
        error_msg = str(e)
        logging.error("OpenAI check exception: %s", error_msg)
        # Catch network errors, etc.
        if "connection" in error_msg.lower() or "timeout" in error_msg.lower():
            return jsonify({"success": False, "message": "Connection to OpenAI API failed. Please check your internet connection."}), 503
        return jsonify({"success": False, "message": f"Error: {error_msg}"}), 500


@app.get("/api/musicbrainz/test")
@app.post("/api/musicbrainz/test")
def api_musicbrainz_test():
    """Test MusicBrainz connectivity and rate limiting.
    Returns success status and any error messages.
    Accepts USE_MUSICBRAINZ in request body (POST) to allow testing before config is saved."""
    # Check if USE_MUSICBRAINZ is provided in request body (for POST) or use global config
    data = request.get_json(silent=True) or {}
    use_mb = data.get("USE_MUSICBRAINZ")
    if use_mb is not None:
        # Use value from request body if provided
        use_mb_enabled = bool(use_mb)
    else:
        # Fall back to global config
        use_mb_enabled = USE_MUSICBRAINZ
    
    if not use_mb_enabled:
        return jsonify({"success": False, "message": "MusicBrainz is disabled. Enable it first."}), 400
    
    try:
        # Test with a well-known release-group ID
        test_mbid = "9162580e-5df4-32de-80cc-f45a8d8a9b1d"  # The Beatles - Abbey Road
        result = musicbrainzngs.get_release_group_by_id(test_mbid, includes=[])
        if result and result.get("release-group"):
            return jsonify({
                "success": True,
                "message": "MusicBrainz connection successful",
                "tested_mbid": test_mbid
            })
        else:
            return jsonify({"success": False, "message": "MusicBrainz returned empty response"}), 500
    except musicbrainzngs.WebServiceError as e:
        error_msg = str(e)
        logging.warning("MusicBrainz WebServiceError: %s", error_msg)
        # Check for specific error codes
        if hasattr(e, 'code'):
            error_code = str(e.code)
            if error_code == "503" or "rate" in error_msg.lower():
                return jsonify({
                    "success": False,
                    "message": "MusicBrainz rate limited. Please wait a moment and try again. Rate limit: 1 request per second."
                }), 503
            elif error_code == "404" or "404" in error_msg:
                return jsonify({
                    "success": False,
                    "message": f"MusicBrainz returned 404 (Not Found). This may be a temporary issue. Error: {error_msg}"
                }), 404
            elif error_code == "503":
                return jsonify({
                    "success": False,
                    "message": "MusicBrainz service temporarily unavailable (503). Please try again later."
                }), 503
        # Fallback to message-based detection
        if "503" in error_msg or "rate" in error_msg.lower() or "service unavailable" in error_msg.lower():
            return jsonify({
                "success": False,
                "message": "MusicBrainz rate limited or service unavailable. Please wait a moment and try again. Rate limit: 1 request per second."
            }), 503
        elif "404" in error_msg or "not found" in error_msg.lower():
            return jsonify({
                "success": False,
                "message": f"MusicBrainz API returned 404. This may be a temporary issue or network problem. Error details: {error_msg}"
            }), 404
        else:
            logging.warning("MusicBrainz test failed: %s", error_msg)
            return jsonify({
                "success": False,
                "message": f"MusicBrainz API error: {error_msg}"
            }), 500
    except Exception as e:
        error_msg = str(e)
        logging.error("MusicBrainz test exception: %s", error_msg)
        if "connection" in error_msg.lower() or "timeout" in error_msg.lower():
            return jsonify({
                "success": False,
                "message": "Connection to MusicBrainz failed. Please check your internet connection."
            }), 503
        return jsonify({
            "success": False,
            "message": f"Error: {error_msg}"
        }), 500




@app.get("/api/openai/models")
@app.post("/api/openai/models")
def api_openai_models():
    """Return only OpenAI model IDs that are compatible with PMDA (Chat Completions, parseable output).
    Uses a curated list so we never show models that return empty or fail (e.g. gpt-5-nano)."""
    from flask import g
    data = getattr(g, 'ai_models_request_data', None) or request.get_json(silent=True) or {}
    key = (data.get("OPENAI_API_KEY") or "").strip() or OPENAI_API_KEY

    if not key:
        return jsonify({"error": "OPENAI_API_KEY is required"}), 400

    if not key.startswith("sk-"):
        return jsonify({"error": "Invalid API key format. OpenAI keys start with 'sk-'"}), 400

    try:
        client = OpenAI(api_key=key)
        # Return only curated compatible models (no API list fetch – we never show incompatible models)
        available_models = list(OPENAI_COMPATIBLE_MODELS)
        logging.info("Returning %d compatible OpenAI models for Settings", len(available_models))
        return jsonify(available_models)
    except Exception as e:
        error_msg = str(e)
        logging.error("OpenAI client init for models list: %s", error_msg)
        if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg or "unauthorized" in error_msg.lower():
            return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
        return jsonify({"error": f"Failed to initialize OpenAI client: {error_msg}"}), 500


# Curated list of Anthropic Claude models compatible with Messages API and text output (PMDA format).
ANTHROPIC_COMPATIBLE_MODELS = [
    "claude-sonnet-4-5",
    "claude-haiku-4-5",
    "claude-opus-4-5",
    "claude-3-5-sonnet-20241022",
    "claude-3-5-sonnet-20240620",
    "claude-3-opus-20240229",
    "claude-3-sonnet-20240229",
    "claude-3-haiku-20240307",
]


@app.post("/api/anthropic/models")
def api_anthropic_models():
    """Return only Anthropic model IDs compatible with PMDA (Messages API, parseable output)."""
    from flask import g
    data = getattr(g, 'ai_models_request_data', None) or request.get_json(silent=True) or {}
    key = (data.get("ANTHROPIC_API_KEY") or "").strip() or ANTHROPIC_API_KEY

    if not key:
        return jsonify({"error": "ANTHROPIC_API_KEY is required"}), 400

    if not anthropic:
        return jsonify({"error": "Anthropic SDK not installed. Please install anthropic package."}), 500

    try:
        client = anthropic.Anthropic(api_key=key)
        try:
            client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=1,
                messages=[{"role": "user", "content": "test"}],
            )
        except anthropic.APIError as e:
            if e.status_code == 401:
                return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
            elif e.status_code == 402:
                return jsonify({"error": "API key has insufficient quota. Please check your Anthropic account billing."}), 402

        available_models = list(ANTHROPIC_COMPATIBLE_MODELS)
        logging.info("Returning %d compatible Anthropic models for Settings", len(available_models))
        return jsonify(available_models)
    except Exception as e:
        error_msg = str(e)
        logging.error("Failed to fetch Anthropic models: %s", error_msg)
        if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg or "unauthorized" in error_msg.lower():
            return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
        elif "connection" in error_msg.lower() or "timeout" in error_msg.lower() or "network" in error_msg.lower():
            return jsonify({"error": "Connection to Anthropic API failed. Please check your internet connection."}), 503
        return jsonify({"error": f"Failed to fetch models: {error_msg}"}), 500


# Curated list of Google Gemini model IDs compatible with generateContent and text output (PMDA format).
GOOGLE_COMPATIBLE_MODELS = [
    "gemini-3-pro-preview",
    "gemini-3-flash-preview",
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-2.5-flash-lite",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-1.5-pro",
    "gemini-1.5-flash",
    "gemini-1.5-flash-8b",
    "gemini-pro",
]


@app.post("/api/google/models")
def api_google_models():
    """Return only Google Gemini model IDs compatible with PMDA (generateContent, text output)."""
    from flask import g
    data = getattr(g, 'ai_models_request_data', None) or request.get_json(silent=True) or {}
    key = (data.get("GOOGLE_API_KEY") or "").strip() or GOOGLE_API_KEY

    if not key:
        return jsonify({"error": "GOOGLE_API_KEY is required"}), 400

    if not genai:
        return jsonify({"error": "Google Generative AI SDK not installed. Please install google-generativeai package."}), 500

    try:
        genai.configure(api_key=key)
        try:
            models_list = genai.list_models()
        except Exception as e:
            error_msg = str(e)
            if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg or "unauthorized" in error_msg.lower():
                return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
            raise

        compatible_set = set(GOOGLE_COMPATIBLE_MODELS)
        available_models = []
        for model in models_list:
            model_name = model.name
            model_id = model_name.split("/")[-1] if "/" in model_name else model_name
            if model_id in compatible_set and "generateContent" in str(getattr(model, "supported_generation_methods", [])):
                if model_id not in available_models:
                    available_models.append(model_id)

        if not available_models:
            available_models = list(GOOGLE_COMPATIBLE_MODELS)

        def model_sort_key(name: str) -> tuple:
            if "gemini-3" in name:
                tier = 0
            elif "gemini-2.5" in name:
                tier = 1
            elif "gemini-2.0" in name:
                tier = 2
            elif "gemini-1.5" in name:
                tier = 3
            elif "gemini-pro" in name:
                tier = 4
            else:
                tier = 5
            return (tier, name)

        available_models.sort(key=model_sort_key)
        logging.info("Returning %d compatible Google Gemini models for Settings", len(available_models))
        return jsonify(available_models)
    except Exception as e:
        error_msg = str(e)
        logging.error("Failed to fetch Google models: %s", error_msg)
        if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg or "unauthorized" in error_msg.lower():
            return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
        elif "connection" in error_msg.lower() or "timeout" in error_msg.lower() or "network" in error_msg.lower():
            return jsonify({"error": "Connection to Google API failed. Please check your internet connection."}), 503
        return jsonify({"error": f"Failed to fetch models: {error_msg}"}), 500


@app.post("/api/ollama/models")
def api_ollama_models():
    """Return list of Ollama model IDs available at the provided URL."""
    from flask import g
    data = getattr(g, 'ai_models_request_data', None) or request.get_json(silent=True) or {}
    url = (data.get("OLLAMA_URL") or "").strip() or OLLAMA_URL
    
    if not url:
        return jsonify({"error": "OLLAMA_URL is required"}), 400
    
    # Normalize URL (remove trailing slash)
    url = url.rstrip("/")
    
    try:
        # Test connection and fetch models
        models_endpoint = f"{url}/api/tags"
        response = requests.get(models_endpoint, timeout=10)
        
        if response.status_code == 404:
            return jsonify({"error": "Ollama API not found at this URL. Make sure Ollama is running and the URL is correct."}), 404
        elif response.status_code != 200:
            return jsonify({"error": f"Failed to connect to Ollama: HTTP {response.status_code}"}), response.status_code
        
        models_data = response.json()
        available_models = []
        
        if "models" in models_data:
            for model in models_data["models"]:
                model_name = model.get("name", "")
                if model_name:
                    available_models.append(model_name)
        
        if not available_models:
            logging.warning("Ollama returned no models")
            return jsonify({"error": "No models available at this Ollama instance. Please pull some models first."}), 404
        
        # Sort models alphabetically
        available_models.sort()
        logging.info("Fetched %d Ollama models from %s", len(available_models), url)
        return jsonify(available_models)
        
    except requests.exceptions.Timeout:
        return jsonify({"error": "Connection to Ollama timed out. Make sure Ollama is running and accessible."}), 503
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Failed to connect to Ollama. Make sure Ollama is running and the URL is correct."}), 503
    except Exception as e:
        error_msg = str(e)
        logging.error("Failed to fetch Ollama models: %s", error_msg)
        return jsonify({"error": f"Failed to fetch models: {error_msg}"}), 500


@app.post("/api/ai/models")
def api_ai_models():
    """Route to the appropriate AI provider's models endpoint based on AI_PROVIDER."""
    data = request.get_json(silent=True) or {}
    provider = (data.get("AI_PROVIDER") or "").strip().lower() or AI_PROVIDER.lower()
    
    # Store data in request context so sub-functions can access it
    # (Flask request body can only be read once)
    from flask import g
    g.ai_models_request_data = data
    
    if provider == "openai":
        return api_openai_models()
    elif provider == "anthropic":
        return api_anthropic_models()
    elif provider == "google":
        return api_google_models()
    elif provider == "ollama":
        return api_ollama_models()
    else:
        return jsonify({"error": f"Unknown AI provider: {provider}"}), 400


def _has_settings_in_db() -> bool:
    """Check if settings exist in the database (wizard was completed)."""
    try:
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM settings")
        count = cur.fetchone()[0]
        con.close()
        return count > 0
    except Exception:
        return False


def _get_config_from_db(key: str, default_value=None):
    """Get a config value from SQLite settings table, with fallback to default."""
    try:
        if STATE_DB_FILE.exists():
            con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
            cur = con.cursor()
            cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cur.fetchone()
            con.close()
            if row and row[0]:
                return row[0]
    except Exception:
        pass
    return default_value


def _reload_auto_move_from_db():
    """Reload AUTO_MOVE_DUPES from SQLite so the current scan uses the value saved in Settings (UI)."""
    global AUTO_MOVE_DUPES
    val = _get_config_from_db("AUTO_MOVE_DUPES")
    if val is not None:
        AUTO_MOVE_DUPES = bool(_parse_bool(val))
        logging.debug("AUTO_MOVE_DUPES reloaded from DB: %s", AUTO_MOVE_DUPES)


def _reload_section_ids_from_db():
    """Reload SECTION_IDS from SQLite so library APIs use latest saved selection."""
    global SECTION_IDS, SECTION_ID
    section_ids_str = _get_config_from_db("SECTION_IDS")
    if not section_ids_str:
        return
    raw = str(section_ids_str).strip()
    if not raw:
        return
    try:
        if raw.startswith("["):
            SECTION_IDS = [int(x) for x in json.loads(raw)]
        else:
            SECTION_IDS = [int(x.strip()) for x in raw.split(",") if x.strip()]
        SECTION_ID = SECTION_IDS[0] if SECTION_IDS else 0
    except Exception:
        pass


def _reload_path_map_from_db():
    """Reload PATH_MAP from SQLite so scan/dedupe use latest saved bindings (e.g. after Detect & verify)."""
    global PATH_MAP
    path_map_val = _get_config_from_db("PATH_MAP")
    if path_map_val is None:
        return
    parsed = _parse_path_map(path_map_val)
    if parsed:
        PATH_MAP = parsed
        logging.info("PATH_MAP reloaded from SQLite at scan start (%d entries)", len(PATH_MAP))


def _parse_format_preference(val):
    """Return FORMAT_PREFERENCE as a list. Handles JSON string, comma-separated string, or list from DB/API."""
    _default = ["dsf", "aif", "aiff", "wav", "flac", "m4a", "mp4", "m4b", "m4p", "aifc", "ogg", "opus", "mp3", "wma"]
    if val is None:
        return _default
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        s = val.strip()
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, TypeError):
                pass
            return _default
        parts = [x.strip() for x in s.split(",") if x.strip()]
        return parts if parts else _default
    return _default

@app.get("/api/config")
def api_config_get():
    """Return current effective configuration for the Web UI.
    Loads from SQLite (settings table) first, then falls back to runtime variables.
    SQLite is the single source of truth for all saved configuration.
    """
    # Load from SQLite first (source of truth), fallback to runtime variables
    def get_setting(key: str, runtime_value, default=""):
        db_value = _get_config_from_db(key)
        if db_value is not None:
            return db_value
        return runtime_value if runtime_value is not None else default

    def get_setting_bool(key: str, runtime_value):
        """Return config value as a real boolean for JSON (so frontend toggles work)."""
        raw = get_setting(key, runtime_value)
        return bool(_parse_bool(raw)) if raw not in (None, "") else bool(_parse_bool(str(runtime_value)))
    
    path_map = getattr(sys.modules[__name__], "PATH_MAP", {})
    section_ids = getattr(sys.modules[__name__], "SECTION_IDS", [])
    skip_folders = getattr(sys.modules[__name__], "SKIP_FOLDERS", [])
    
    # Check if settings exist in DB (wizard was completed)
    has_settings = _has_settings_in_db()
    # Wizard should not show if settings exist in DB OR if Plex is configured
    configured = has_settings or PLEX_CONFIGURED
    
    # Load SECTION_IDS from SQLite if available (stored as comma-separated "1,5" or legacy JSON "[1,5]")
    section_ids_str = _get_config_from_db("SECTION_IDS")
    if section_ids_str:
        raw = str(section_ids_str).strip()
        try:
            if raw.startswith("["):
                section_ids = [int(x) for x in json.loads(raw)]
            else:
                section_ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
        except Exception:
            pass
    
    # Load PATH_MAP from SQLite if available
    path_map_str = _get_config_from_db("PATH_MAP")
    if path_map_str:
        try:
            path_map = json.loads(path_map_str) if isinstance(path_map_str, str) else path_map_str
        except Exception:
            pass
    
    return jsonify({
        "configured": configured,
        "PLEX_HOST": get_setting("PLEX_HOST", PLEX_HOST),
        "PLEX_TOKEN": get_setting("PLEX_TOKEN", PLEX_TOKEN),
        "PLEX_BASE_PATH": get_setting("PLEX_BASE_PATH", "/database"),
        "PLEX_DB_PATH": get_setting("PLEX_DB_PATH", merged.get("PLEX_DB_PATH", "/database")),
        "PLEX_DB_FILE": "com.plexapp.plugins.library.db",
        "SECTION_IDS": ",".join(str(s) for s in section_ids),
        "PATH_MAP": path_map,
        "DUPE_ROOT": str(DUPE_ROOT),
        "PMDA_CONFIG_DIR": str(CONFIG_DIR),
        "MUSIC_PARENT_PATH": get_setting("MUSIC_PARENT_PATH", merged.get("MUSIC_PARENT_PATH", "")),
        "SCAN_THREADS": get_setting("SCAN_THREADS", SCAN_THREADS if isinstance(SCAN_THREADS, int) else "auto"),
        "SKIP_FOLDERS": get_setting("SKIP_FOLDERS", ",".join(skip_folders) if isinstance(skip_folders, list) else (skip_folders or "")),
        "CROSS_LIBRARY_DEDUPE": get_setting_bool("CROSS_LIBRARY_DEDUPE", CROSS_LIBRARY_DEDUPE),
        "CROSSCHECK_SAMPLES": get_setting("CROSSCHECK_SAMPLES", CROSSCHECK_SAMPLES),
        "FORMAT_PREFERENCE": _parse_format_preference(get_setting("FORMAT_PREFERENCE", FORMAT_PREFERENCE)),
        "AI_PROVIDER": get_setting("AI_PROVIDER", AI_PROVIDER),
        "OPENAI_API_KEY": get_setting("OPENAI_API_KEY", OPENAI_API_KEY),
        "OPENAI_MODEL": get_setting("OPENAI_MODEL", OPENAI_MODEL),
        "OPENAI_MODEL_FALLBACKS": get_setting("OPENAI_MODEL_FALLBACKS", merged.get("OPENAI_MODEL_FALLBACKS", "")),
        "ANTHROPIC_API_KEY": get_setting("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY),
        "GOOGLE_API_KEY": get_setting("GOOGLE_API_KEY", GOOGLE_API_KEY),
        "OLLAMA_URL": get_setting("OLLAMA_URL", OLLAMA_URL),
        "USE_MUSICBRAINZ": get_setting_bool("USE_MUSICBRAINZ", USE_MUSICBRAINZ),
        "MUSICBRAINZ_EMAIL": get_setting("MUSICBRAINZ_EMAIL", MUSICBRAINZ_EMAIL),
        "MB_RETRY_NOT_FOUND": get_setting_bool("MB_RETRY_NOT_FOUND", MB_RETRY_NOT_FOUND),
        "USE_AI_FOR_MB_MATCH": get_setting_bool("USE_AI_FOR_MB_MATCH", USE_AI_FOR_MB_MATCH),
        "USE_AI_FOR_MB_VERIFY": get_setting_bool("USE_AI_FOR_MB_VERIFY", USE_AI_FOR_MB_VERIFY),
        "USE_AI_VISION_FOR_COVER": get_setting_bool("USE_AI_VISION_FOR_COVER", USE_AI_VISION_FOR_COVER),
        "OPENAI_VISION_MODEL": get_setting("OPENAI_VISION_MODEL", OPENAI_VISION_MODEL),
        "USE_WEB_SEARCH_FOR_MB": get_setting_bool("USE_WEB_SEARCH_FOR_MB", USE_WEB_SEARCH_FOR_MB),
        "SERPER_API_KEY": get_setting("SERPER_API_KEY", SERPER_API_KEY),
        "LIDARR_URL": get_setting("LIDARR_URL", merged.get("LIDARR_URL", "")),
        "LIDARR_API_KEY": get_setting("LIDARR_API_KEY", merged.get("LIDARR_API_KEY", "")),
        "AUTOBRR_URL": get_setting("AUTOBRR_URL", merged.get("AUTOBRR_URL", "")),
        "AUTOBRR_API_KEY": get_setting("AUTOBRR_API_KEY", merged.get("AUTOBRR_API_KEY", "")),
        "AUTO_FIX_BROKEN_ALBUMS": get_setting_bool("AUTO_FIX_BROKEN_ALBUMS", AUTO_FIX_BROKEN_ALBUMS),
        "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD": get_setting("BROKEN_ALBUM_CONSECUTIVE_THRESHOLD", BROKEN_ALBUM_CONSECUTIVE_THRESHOLD),
        "BROKEN_ALBUM_PERCENTAGE_THRESHOLD": get_setting("BROKEN_ALBUM_PERCENTAGE_THRESHOLD", BROKEN_ALBUM_PERCENTAGE_THRESHOLD),
        "REQUIRED_TAGS": _parse_required_tags(get_setting("REQUIRED_TAGS", ",".join(REQUIRED_TAGS))),
        "DISCORD_WEBHOOK": get_setting("DISCORD_WEBHOOK", DISCORD_WEBHOOK),
        "LOG_LEVEL": get_setting("LOG_LEVEL", LOG_LEVEL),
        "LOG_FILE": get_setting("LOG_FILE", LOG_FILE),
        "AUTO_MOVE_DUPES": get_setting_bool("AUTO_MOVE_DUPES", AUTO_MOVE_DUPES),
        "NORMALIZE_PARENTHETICAL_FOR_DEDUPE": get_setting_bool("NORMALIZE_PARENTHETICAL_FOR_DEDUPE", True),
        "DISABLE_PATH_CROSSCHECK": get_setting_bool("DISABLE_PATH_CROSSCHECK", DISABLE_PATH_CROSSCHECK),
        "USE_DISCOGS": get_setting_bool("USE_DISCOGS", USE_DISCOGS),
        "DISCOGS_USER_TOKEN": get_setting("DISCOGS_USER_TOKEN", DISCOGS_USER_TOKEN),
        "USE_LASTFM": get_setting_bool("USE_LASTFM", USE_LASTFM),
        "LASTFM_API_KEY": get_setting("LASTFM_API_KEY", LASTFM_API_KEY),
        "LASTFM_API_SECRET": get_setting("LASTFM_API_SECRET", LASTFM_API_SECRET),
        "USE_BANDCAMP": get_setting_bool("USE_BANDCAMP", USE_BANDCAMP),
        "paths_status": _paths_rw_status(),
        "container_mounts": _container_mounts_status(),
    })


def _restart_container():
    """Attempt to restart the container. Tries docker socket first, then falls back to signal."""
    import subprocess
    import signal
    import os
    
    def _do_restart():
        """Perform restart in a separate thread to allow HTTP response to be sent first."""
        time.sleep(2)  # Give time for HTTP response to be sent
        # Try to restart via docker socket if available
        docker_socket = Path("/var/run/docker.sock")
        container_name = os.getenv("HOSTNAME", "PMDA_WEBUI")
        
        if docker_socket.exists():
            try:
                # Try to restart via docker command
                result = subprocess.run(
                    ["docker", "restart", container_name],
                    capture_output=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    logging.info("Container restart initiated via docker socket")
                    return
            except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
                logging.debug("Docker restart failed: %s", e)
        
        # Fallback: use signal to trigger graceful shutdown (container manager will restart)
        # This works if the container is managed by docker-compose, systemd, or has restart policy
        try:
            logging.info("Sending SIGTERM to trigger container restart")
            os.kill(os.getpid(), signal.SIGTERM)
        except Exception as e:
            logging.warning("Failed to restart container: %s", e)
    
    # Start restart in background thread
    restart_thread = threading.Thread(target=_do_restart, daemon=True)
    restart_thread.start()
    return True


def _apply_settings_in_memory(updates: dict):
    """Apply saved settings to in-memory globals so they take effect without restart."""
    global PLEX_CONFIGURED  # declared once so it can be set in any of the Plex blocks below
    mod = sys.modules[__name__]
    ai_keys = {"AI_PROVIDER", "OPENAI_API_KEY", "OPENAI_MODEL", "OPENAI_MODEL_FALLBACKS",
               "ANTHROPIC_API_KEY", "GOOGLE_API_KEY", "OLLAMA_URL"}
    need_ai_reinit = bool(ai_keys & set(updates.keys()))

    if "PLEX_HOST" in updates:
        global PLEX_HOST
        PLEX_HOST = str(updates["PLEX_HOST"] or "").strip()
        logging.info("PLEX_HOST updated in memory")
    if "PLEX_TOKEN" in updates:
        v = str(updates["PLEX_TOKEN"] or "").strip()
        if v != "***":
            global PLEX_TOKEN
            PLEX_TOKEN = v
            logging.info("PLEX_TOKEN updated in memory")
    if "PLEX_BASE_PATH" in updates:
        base = str(updates["PLEX_BASE_PATH"] or "").strip() or "/database"
        resolved = _resolve_plex_db_from_base(base)
        if resolved:
            updates = dict(updates)
            updates["PLEX_DB_PATH"] = resolved
            logging.info("PLEX_DB_PATH resolved from PLEX_BASE_PATH: %s", resolved)
    if "PLEX_DB_PATH" in updates:
        global PLEX_DB_FILE, PLEX_DB_EXISTS, PLEX_CONFIGURED
        db_path = str(updates["PLEX_DB_PATH"] or "").strip() or "/database"
        PLEX_DB_FILE = str(Path(db_path) / PLEX_DB_FILENAME)
        PLEX_DB_EXISTS = Path(PLEX_DB_FILE).exists()
        PLEX_CONFIGURED = bool(
            getattr(mod, "PLEX_HOST", "") and getattr(mod, "PLEX_TOKEN", "") and getattr(mod, "SECTION_IDS", []) and PLEX_DB_EXISTS
        )
        logging.info("PLEX_DB_PATH updated in memory: %s (exists=%s)", PLEX_DB_FILE, PLEX_DB_EXISTS)
    if "SECTION_IDS" in updates:
        global SECTION_IDS, SECTION_ID
        SECTION_IDS = list(updates["SECTION_IDS"])
        SECTION_ID = SECTION_IDS[0] if SECTION_IDS else 0
        logging.info("SECTION_IDS updated in memory: %s", SECTION_IDS)
    # Recompute PLEX_CONFIGURED whenever any Plex-related setting was updated
    plex_keys = {"PLEX_HOST", "PLEX_TOKEN", "SECTION_IDS", "PLEX_DB_PATH"}
    if plex_keys & set(updates.keys()):
        PLEX_CONFIGURED = bool(
            getattr(mod, "PLEX_HOST", "") and getattr(mod, "PLEX_TOKEN", "")
            and getattr(mod, "SECTION_IDS", []) and getattr(mod, "PLEX_DB_EXISTS", False)
        )
        logging.info("PLEX_CONFIGURED recomputed: %s (host=%s, token=%s, sections=%s, db_exists=%s)",
                     PLEX_CONFIGURED, bool(getattr(mod, "PLEX_HOST", "")), bool(getattr(mod, "PLEX_TOKEN", "")),
                     len(getattr(mod, "SECTION_IDS", [])), getattr(mod, "PLEX_DB_EXISTS", False))
    if "MUSICBRAINZ_EMAIL" in updates:
        global MUSICBRAINZ_EMAIL
        MUSICBRAINZ_EMAIL = str(updates["MUSICBRAINZ_EMAIL"] or "")
        _configure_musicbrainz_useragent()
        logging.info("MusicBrainz User-Agent updated")
    if "USE_MUSICBRAINZ" in updates:
        global USE_MUSICBRAINZ
        USE_MUSICBRAINZ = bool(_parse_bool(updates["USE_MUSICBRAINZ"]))
    if "MB_RETRY_NOT_FOUND" in updates:
        global MB_RETRY_NOT_FOUND
        MB_RETRY_NOT_FOUND = bool(_parse_bool(updates["MB_RETRY_NOT_FOUND"]))
    if "USE_AI_FOR_MB_MATCH" in updates:
        global USE_AI_FOR_MB_MATCH
        USE_AI_FOR_MB_MATCH = bool(_parse_bool(updates["USE_AI_FOR_MB_MATCH"]))
    if "USE_AI_FOR_MB_VERIFY" in updates:
        global USE_AI_FOR_MB_VERIFY
        USE_AI_FOR_MB_VERIFY = bool(_parse_bool(updates["USE_AI_FOR_MB_VERIFY"]))
    if "USE_AI_VISION_FOR_COVER" in updates:
        global USE_AI_VISION_FOR_COVER
        USE_AI_VISION_FOR_COVER = bool(_parse_bool(updates["USE_AI_VISION_FOR_COVER"]))
    if "OPENAI_VISION_MODEL" in updates:
        global OPENAI_VISION_MODEL
        OPENAI_VISION_MODEL = str(updates.get("OPENAI_VISION_MODEL") or "").strip()
    if "USE_WEB_SEARCH_FOR_MB" in updates:
        global USE_WEB_SEARCH_FOR_MB
        USE_WEB_SEARCH_FOR_MB = bool(_parse_bool(updates["USE_WEB_SEARCH_FOR_MB"]))
    if "SERPER_API_KEY" in updates:
        global SERPER_API_KEY
        v = str(updates.get("SERPER_API_KEY") or "").strip()
        if v != "***":
            SERPER_API_KEY = v
    if "SCAN_THREADS" in updates:
        global SCAN_THREADS
        v = updates["SCAN_THREADS"]
        if isinstance(v, str) and str(v).strip().lower() == "auto":
            SCAN_THREADS = max(1, os.cpu_count() or 4)
        else:
            try:
                SCAN_THREADS = max(1, int(v))
            except (ValueError, TypeError):
                pass
        logging.info("SCAN_THREADS updated in memory: %s", SCAN_THREADS)
    if "LOG_LEVEL" in updates:
        global LOG_LEVEL
        LOG_LEVEL = str(updates["LOG_LEVEL"] or "INFO").upper()
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    if "DISCORD_WEBHOOK" in updates:
        v = str(updates["DISCORD_WEBHOOK"] or "").strip()
        if v != "***":
            global DISCORD_WEBHOOK
            DISCORD_WEBHOOK = v
    if "SKIP_FOLDERS" in updates:
        global SKIP_FOLDERS
        v = updates["SKIP_FOLDERS"]
        if isinstance(v, list):
            SKIP_FOLDERS = [str(p).strip() for p in v if str(p).strip()]
        else:
            SKIP_FOLDERS = [p.strip() for p in str(v or "").split(",") if p.strip()]
    if "REQUIRED_TAGS" in updates:
        global REQUIRED_TAGS
        REQUIRED_TAGS = _parse_required_tags(updates["REQUIRED_TAGS"]) if updates["REQUIRED_TAGS"] else []
    if "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD" in updates:
        global BROKEN_ALBUM_CONSECUTIVE_THRESHOLD
        try:
            BROKEN_ALBUM_CONSECUTIVE_THRESHOLD = int(updates["BROKEN_ALBUM_CONSECUTIVE_THRESHOLD"])
        except (ValueError, TypeError):
            pass
    if "BROKEN_ALBUM_PERCENTAGE_THRESHOLD" in updates:
        global BROKEN_ALBUM_PERCENTAGE_THRESHOLD
        try:
            BROKEN_ALBUM_PERCENTAGE_THRESHOLD = float(updates["BROKEN_ALBUM_PERCENTAGE_THRESHOLD"])
        except (ValueError, TypeError):
            pass
    if "AUTO_MOVE_DUPES" in updates:
        global AUTO_MOVE_DUPES
        AUTO_MOVE_DUPES = bool(_parse_bool(updates["AUTO_MOVE_DUPES"]))
    if "AUTO_FIX_BROKEN_ALBUMS" in updates:
        global AUTO_FIX_BROKEN_ALBUMS
        AUTO_FIX_BROKEN_ALBUMS = bool(_parse_bool(updates["AUTO_FIX_BROKEN_ALBUMS"]))
    if "LIDARR_URL" in updates:
        mod.LIDARR_URL = str(updates["LIDARR_URL"] or "")
    if "LIDARR_API_KEY" in updates:
        mod.LIDARR_API_KEY = str(updates["LIDARR_API_KEY"] or "")
    if "AUTOBRR_URL" in updates:
        mod.AUTOBRR_URL = str(updates["AUTOBRR_URL"] or "")
    if "AUTOBRR_API_KEY" in updates:
        mod.AUTOBRR_API_KEY = str(updates["AUTOBRR_API_KEY"] or "")
    if "DISABLE_PATH_CROSSCHECK" in updates:
        global DISABLE_PATH_CROSSCHECK
        DISABLE_PATH_CROSSCHECK = bool(_parse_bool(updates["DISABLE_PATH_CROSSCHECK"]))
    if "PATH_MAP" in updates:
        global PATH_MAP
        PATH_MAP = _parse_path_map(updates["PATH_MAP"])
        logging.info("PATH_MAP updated in memory (%d entries)", len(PATH_MAP))
    if "DUPE_ROOT" in updates:
        global DUPE_ROOT
        DUPE_ROOT = Path(str(updates["DUPE_ROOT"] or "").strip() or "/dupes")
        logging.info("DUPE_ROOT updated in memory: %s", DUPE_ROOT)
    if "MUSIC_PARENT_PATH" in updates:
        merged["MUSIC_PARENT_PATH"] = str(updates["MUSIC_PARENT_PATH"] or "").strip() or ""
        logging.info("MUSIC_PARENT_PATH updated in memory")
    if "LOG_FILE" in updates:
        global LOG_FILE
        LOG_FILE = str(updates["LOG_FILE"] or "").strip() or str(CONFIG_DIR / "pmda.log")
        logging.info("LOG_FILE updated in memory: %s", LOG_FILE)
    if "CROSS_LIBRARY_DEDUPE" in updates:
        global CROSS_LIBRARY_DEDUPE
        CROSS_LIBRARY_DEDUPE = bool(_parse_bool(updates["CROSS_LIBRARY_DEDUPE"]))
        logging.info("CROSS_LIBRARY_DEDUPE updated in memory: %s", CROSS_LIBRARY_DEDUPE)
    if "CROSSCHECK_SAMPLES" in updates:
        global CROSSCHECK_SAMPLES
        try:
            CROSSCHECK_SAMPLES = max(0, int(updates["CROSSCHECK_SAMPLES"]))
        except (ValueError, TypeError):
            CROSSCHECK_SAMPLES = 20
        logging.info("CROSSCHECK_SAMPLES updated in memory: %s", CROSSCHECK_SAMPLES)
    if "FORMAT_PREFERENCE" in updates:
        global FORMAT_PREFERENCE, FMT_SCORE
        FORMAT_PREFERENCE = _parse_format_preference_early(updates["FORMAT_PREFERENCE"])
        FMT_SCORE = {ext: len(FORMAT_PREFERENCE) - i for i, ext in enumerate(FORMAT_PREFERENCE)}
        logging.info("FORMAT_PREFERENCE updated in memory (%d formats)", len(FORMAT_PREFERENCE))
    if "OPENAI_MODEL_FALLBACKS" in updates:
        merged["OPENAI_MODEL_FALLBACKS"] = str(updates["OPENAI_MODEL_FALLBACKS"] or "").strip()
        logging.info("OPENAI_MODEL_FALLBACKS updated in memory")

    # Metadata fallback providers (Discogs, Last.fm, Bandcamp)
    if "USE_DISCOGS" in updates:
        mod.USE_DISCOGS = bool(_parse_bool(updates["USE_DISCOGS"]))
    if "DISCOGS_USER_TOKEN" in updates:
        mod.DISCOGS_USER_TOKEN = str(updates["DISCOGS_USER_TOKEN"] or "").strip()
    if "USE_LASTFM" in updates:
        mod.USE_LASTFM = bool(_parse_bool(updates["USE_LASTFM"]))
    if "LASTFM_API_KEY" in updates:
        mod.LASTFM_API_KEY = str(updates["LASTFM_API_KEY"] or "").strip()
    if "LASTFM_API_SECRET" in updates:
        mod.LASTFM_API_SECRET = str(updates["LASTFM_API_SECRET"] or "").strip()
    if "USE_BANDCAMP" in updates:
        mod.USE_BANDCAMP = bool(_parse_bool(updates["USE_BANDCAMP"]))

    # AI-related: update globals then reinit clients
    if "AI_PROVIDER" in updates:
        mod.AI_PROVIDER = str(updates["AI_PROVIDER"] or "openai").strip().lower()
    if "OPENAI_API_KEY" in updates:
        v = str(updates["OPENAI_API_KEY"] or "").strip()
        if v != "***":
            mod.OPENAI_API_KEY = v
    if "OPENAI_MODEL" in updates:
        mod.OPENAI_MODEL = str(updates["OPENAI_MODEL"] or "gpt-4")
    if "ANTHROPIC_API_KEY" in updates:
        mod.ANTHROPIC_API_KEY = str(updates["ANTHROPIC_API_KEY"] or "")
    if "GOOGLE_API_KEY" in updates:
        mod.GOOGLE_API_KEY = str(updates["GOOGLE_API_KEY"] or "")
    if "OLLAMA_URL" in updates:
        mod.OLLAMA_URL = str(updates["OLLAMA_URL"] or "").strip().rstrip("/")

    if need_ai_reinit:
        _reinit_ai_from_globals()


@app.put("/api/config")
def api_config_put():
    """Persist configuration updates to SQLite (single source of truth).
    Only updates keys present in the request; existing values in SQLite are preserved.
    Settings are applied in memory immediately so no restart is needed.
    """
    data = request.get_json() or {}
    allowed = {
        "PLEX_HOST", "PLEX_TOKEN", "PLEX_BASE_PATH", "PLEX_DB_PATH", "SECTION_IDS", "PATH_MAP",
        "DUPE_ROOT", "PMDA_CONFIG_DIR", "MUSIC_PARENT_PATH",
        "SCAN_THREADS", "LOG_LEVEL", "LOG_FILE", "AI_PROVIDER", "OPENAI_API_KEY", "OPENAI_MODEL",
        "OPENAI_MODEL_FALLBACKS", "ANTHROPIC_API_KEY", "GOOGLE_API_KEY", "OLLAMA_URL",
        "DISCORD_WEBHOOK", "USE_MUSICBRAINZ", "MUSICBRAINZ_EMAIL", "MB_RETRY_NOT_FOUND",
        "USE_AI_FOR_MB_MATCH", "USE_AI_FOR_MB_VERIFY",
        "USE_AI_VISION_FOR_COVER", "OPENAI_VISION_MODEL", "USE_WEB_SEARCH_FOR_MB", "SERPER_API_KEY",
        "SKIP_FOLDERS", "CROSS_LIBRARY_DEDUPE", "CROSSCHECK_SAMPLES", "DISABLE_PATH_CROSSCHECK",
        "FORMAT_PREFERENCE", "AUTO_MOVE_DUPES", "NORMALIZE_PARENTHETICAL_FOR_DEDUPE",
        "LIDARR_URL", "LIDARR_API_KEY", "AUTOBRR_URL", "AUTOBRR_API_KEY", "AUTO_FIX_BROKEN_ALBUMS",
        "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD", "BROKEN_ALBUM_PERCENTAGE_THRESHOLD", "REQUIRED_TAGS",
        "USE_DISCOGS", "DISCOGS_USER_TOKEN", "USE_LASTFM", "LASTFM_API_KEY", "LASTFM_API_SECRET", "USE_BANDCAMP",
    }
    # Only process keys that are in the request AND in the allowed list
    # This preserves existing values in SQLite for keys not in the request
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return jsonify({"status": "ok", "message": "Nothing to save"})
    # When PLEX_BASE_PATH is updated, clear PLEX_DB_PATH so next startup re-discovers
    if "PLEX_BASE_PATH" in updates:
        updates["PLEX_DB_PATH"] = ""
    if "SECTION_IDS" in updates:
        raw = updates["SECTION_IDS"]
        if isinstance(raw, str):
            updates["SECTION_IDS"] = [int(x.strip()) for x in raw.split(",") if x.strip()]
        elif isinstance(raw, list):
            updates["SECTION_IDS"] = [int(x) for x in raw]
    if "SKIP_FOLDERS" in updates and isinstance(updates["SKIP_FOLDERS"], str):
        updates["SKIP_FOLDERS"] = [p.strip() for p in updates["SKIP_FOLDERS"].split(",") if p.strip()]
    if "REQUIRED_TAGS" in updates:
        updates["REQUIRED_TAGS"] = _parse_required_tags(updates["REQUIRED_TAGS"])
    if "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD" in updates:
        try:
            updates["BROKEN_ALBUM_CONSECUTIVE_THRESHOLD"] = int(updates["BROKEN_ALBUM_CONSECUTIVE_THRESHOLD"])
        except (ValueError, TypeError):
            updates["BROKEN_ALBUM_CONSECUTIVE_THRESHOLD"] = 2
    if "BROKEN_ALBUM_PERCENTAGE_THRESHOLD" in updates:
        try:
            updates["BROKEN_ALBUM_PERCENTAGE_THRESHOLD"] = float(updates["BROKEN_ALBUM_PERCENTAGE_THRESHOLD"])
        except (ValueError, TypeError):
            updates["BROKEN_ALBUM_PERCENTAGE_THRESHOLD"] = 0.20
    
    # Serialize complex types for SQLite storage. Do not overwrite secret keys with mask (e.g. "***").
    MASKED_SECRETS = (
        "OPENAI_API_KEY", "PLEX_TOKEN", "DISCORD_WEBHOOK",
        "ANTHROPIC_API_KEY", "GOOGLE_API_KEY",
        "DISCOGS_USER_TOKEN", "LASTFM_API_KEY", "LASTFM_API_SECRET",
        "LIDARR_API_KEY", "AUTOBRR_API_KEY",
    )
    updates_for_db = {}
    for k, v in updates.items():
        if k in MASKED_SECRETS and str(v).strip() == "***":
            continue  # Do not overwrite real key with mask
        if k == "SECTION_IDS" and isinstance(v, list):
            # Store SECTION_IDS as comma-separated string so GET /api/config can parse it consistently
            updates_for_db[k] = ",".join(str(x) for x in v) if v else ""
        elif k == "SKIP_FOLDERS" and isinstance(v, list):
            # Store as comma-separated string; load uses _parse_skip_folders (accepts JSON or CSV)
            updates_for_db[k] = ",".join(str(p).strip() for p in v if str(p).strip()) if v else ""
        elif isinstance(v, (dict, list)):
            updates_for_db[k] = json.dumps(v)
        else:
            # Preserve empty strings (they are valid values, e.g. empty webhook = disabled)
            updates_for_db[k] = str(v) if v is not None else ""
    
    try:
        # Save to SQLite (single source of truth)
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        for key, value in updates_for_db.items():
            cur.execute("INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)", (key, value))
        con.commit()
        con.close()
        logging.info("Settings saved to SQLite database: %s", list(updates_for_db.keys()))
    except Exception as e:
        logging.warning("Failed to save settings to SQLite: %s", e)
        return jsonify({"status": "error", "message": f"Failed to save to database: {str(e)}"}), 500
    
    # Apply all saved settings in memory (no restart needed)
    _apply_settings_in_memory(updates)

    # Only restart container if this is the initial wizard setup (first time saving)
    # After wizard is complete, settings are saved but container doesn't restart
    has_settings = _has_settings_in_db()
    restart_success = False
    if not has_settings:
        # First time saving (wizard), restart container
        logging.info("First time configuration save detected, restarting container")
        restart_success = _restart_container()
        if not restart_success:
            logging.warning("Container restart may have failed, but settings were saved")
    else:
        # Settings already exist, just save without restart
        logging.info("Settings updated (wizard already completed), saving without restart")
    
    return jsonify({"status": "ok", "restart_initiated": restart_success, "message": "Settings saved successfully"})


def get_duplicate_groups_from_library():
    """
    Return duplicate groups from Plex (same logic as Library: same artist + same album name).
    Returns list of { artist, norm_title, album_ids } for groups with >1 album.
    Used to show in Unduper even when scan produced 0 groups (e.g. all same-folder).
    """
    _reload_section_ids_from_db()
    if not PLEX_CONFIGURED or not SECTION_IDS:
        return []
    db_conn = plex_connect()
    try:
        ph = ",".join("?" for _ in SECTION_IDS)
        rows = db_conn.execute(f"""
            SELECT alb.id, alb.title, alb.parent_id
            FROM metadata_items alb
            WHERE alb.metadata_type = 9 AND alb.library_section_id IN ({ph})
        """, list(SECTION_IDS)).fetchall()
        artist_cache = {}
        norm_to_albums: dict[tuple[str, str], list[int]] = {}
        for album_id, title, parent_id in rows:
            artist_name = ""
            if parent_id:
                if parent_id not in artist_cache:
                    r = db_conn.execute(
                        "SELECT title FROM metadata_items WHERE id = ?", (parent_id,)
                    ).fetchone()
                    artist_cache[parent_id] = (r[0] or "").strip() if r else ""
                artist_name = artist_cache[parent_id]
            normalize_parenthetical = bool(_parse_bool(_get_config_from_db("NORMALIZE_PARENTHETICAL_FOR_DEDUPE") or "true"))
            norm = norm_album_for_dedup(title or "", normalize_parenthetical)
            key = (artist_name, norm)
            norm_to_albums.setdefault(key, []).append(album_id)
        out = []
        for (artist_name, norm_title), album_ids in norm_to_albums.items():
            if len(album_ids) > 1:
                out.append({"artist": artist_name, "norm_title": norm_title, "album_ids": album_ids})
        return out
    finally:
        db_conn.close()


@app.get("/api/duplicates")
def api_duplicates():
    """
    Return the full list of duplicate-group cards for the Web UI.
    Includes (1) scan results (best/loser, can dedupe) and (2) library duplicate groups
    not in scan (same name = dupe, no_move so user sees them and can run scan).
    """
    if not PLEX_CONFIGURED:
        resp = jsonify([])
        resp.headers["X-PMDA-Requires-Config"] = "true"
        return resp
    with lock:
        if not state["duplicates"]:
            if not state.get("_api_duplicates_load_logged"):
                logging.debug("api_duplicates(): loading scan results from DB into memory")
                state["_api_duplicates_load_logged"] = True
            state["duplicates"] = load_scan_from_db()
        cards = _build_card_list(state["duplicates"])
        scan_keys = set()
        for artist, groups in state["duplicates"].items():
            for g in groups:
                if "best" not in g:
                    continue
                norm = (g["best"].get("album_norm") or "").strip().lower()
                if norm:
                    scan_keys.add((artist, norm))
        # Only add library-only groups when there is at least one scan result (so "Clear results" shows nothing)
        if cards or scan_keys:
            library_groups = get_duplicate_groups_from_library()
            db_plex = None
            try:
                db_plex = plex_connect()
            except Exception:
                pass
            for lg in library_groups:
                artist, norm_title = lg["artist"], (lg["norm_title"] or "").strip().lower()
                if (artist, norm_title) in scan_keys:
                    continue
                album_ids = lg["album_ids"]
                # Skip group if any edition is already under /dupes (already deduped by auto-move or manual)
                if db_plex:
                    if any(_album_path_under_dupes(db_plex, aid) for aid in album_ids):
                        continue
                first_id = album_ids[0]
                n = len(album_ids)
                display_title = (norm_title or "").title() or "Unknown"
                cards.append({
                    "artist_key": artist.replace(" ", "_"),
                    "artist": artist,
                    "album_id": first_id,
                    "n": n,
                    "best_thumb": thumb_url(first_id),
                    "best_title": display_title,
                    "best_fmt": "—",
                    "formats": ["—"] * n,
                    "used_ai": False,
                    "ai_provider": "",
                    "ai_model": "",
                    "size": 0,
                    "size_mb": 0,
                    "track_count": 0,
                    "path": "",
                    "no_move": True,
                })
            if db_plex:
                try:
                    db_plex.close()
                except Exception:
                    pass
    return jsonify(cards)


@app.get("/api/progress")
def api_progress():
    with lock:
        # Do NOT set scanning=False here when progress >= total. The scan thread still runs
        # the AI batch and finally block (save_scan_to_db, scan_history) after progress hits
        # 100%; only that thread must set scanning=False so the UI does not show "finished"
        # while the scan is still running.
        scanning = state["scanning"]
        status = "paused" if (scanning and scan_is_paused.is_set()) else ("running" if scanning else "stopped")
        # Step-based progress for bar: progress/total = steps done / step total (3*albums+2 or +3)
        progress = state.get("scan_step_progress", 0)
        total = state.get("scan_step_total", 0) or state["scan_total"]
        format_done_count = state.get("scan_format_done_count", 0)
        mb_done_count = state.get("scan_mb_done_count", 0)
        
        # Keep scan_progress for effective_progress / legacy; bar uses step progress
        active_artists_dict = state.get("scan_active_artists", {})
        effective_progress = state.get("scan_progress", 0)
        
        # ETA from step progress
        eta_seconds = None
        threads_in_use = SCAN_THREADS
        if scanning and state.get("scan_start_time") and total > 0:
            current_time = time.time()
            start_time = state["scan_start_time"]
            elapsed_time = current_time - start_time
            if elapsed_time > 0 and progress > 0:
                speed = progress / elapsed_time
                remaining_steps = total - progress
                if speed > 0 and remaining_steps > 0:
                    eta_seconds = int(remaining_steps / speed)
        
        active_artists_list = [
            {
                "artist_name": name,
                "total_albums": info.get("total_albums", 0) if isinstance(info, dict) else 0,
                "albums_processed": info.get("albums_processed", 0) if isinstance(info, dict) else 0,
                "current_album": info.get("current_album") if isinstance(info, dict) else None  # Include current album tracking
            }
            for name, info in active_artists_dict.items()
            if not name.startswith("_") and isinstance(info, dict)  # Filter out internal keys like "_ai_batch"
        ]
        
        # Copy all state values we need while still in the lock
        artists_processed = state.get("scan_artists_processed", 0)
        artists_total = state.get("scan_artists_total", 0)
        ai_used_count = state.get("scan_ai_used_count", 0)
        mb_used_count = state.get("scan_mb_used_count", 0)
        ai_enabled = state.get("scan_ai_enabled", False)
        mb_enabled = state.get("scan_mb_enabled", False)
        audio_cache_hits = state.get("scan_audio_cache_hits", 0)
        audio_cache_misses = state.get("scan_audio_cache_misses", 0)
        mb_cache_hits = state.get("scan_mb_cache_hits", 0)
        mb_cache_misses = state.get("scan_mb_cache_misses", 0)
        duplicate_groups_count = state.get("scan_duplicate_groups_count", 0)
        total_duplicates_count = state.get("scan_total_duplicates_count", 0)
        broken_albums_count = state.get("scan_broken_albums_count", 0)
        missing_albums_count = state.get("scan_missing_albums_count", 0)
        albums_without_artist_image = state.get("scan_albums_without_artist_image", 0)
        albums_without_album_image = state.get("scan_albums_without_album_image", 0)
        albums_without_complete_tags = state.get("scan_albums_without_complete_tags", 0)
        albums_without_mb_id = state.get("scan_albums_without_mb_id", 0)
        albums_without_artist_mb_id = state.get("scan_albums_without_artist_mb_id", 0)
        format_done_count = state.get("scan_format_done_count", 0)
        mb_done_count = state.get("scan_mb_done_count", 0)
        scan_ai_batch_total = state.get("scan_ai_batch_total", 0)
        scan_ai_batch_processed = state.get("scan_ai_batch_processed", 0)
        scan_ai_current_label = state.get("scan_ai_current_label")
        last_fix_all_by_provider = state.get("last_fix_all_by_provider")
        last_fix_all_total_albums = state.get("last_fix_all_total_albums", 0)
        
        # Current micro-step (from first active artist with non-done status) for live indicators
        current_step = None
        for _name, info in active_artists_dict.items():
            if not _name.startswith("_") and isinstance(info, dict):
                cur_album = info.get("current_album")
                if isinstance(cur_album, dict):
                    s = cur_album.get("status")
                    if s and s != "done":
                        current_step = s
                        break
        finalizing = state.get("scan_finalizing", False)
        deduping = state.get("deduping", False)
        dedupe_progress = state.get("dedupe_progress", 0)
        dedupe_total = state.get("dedupe_total", 0)
        dedupe_current_group = state.get("dedupe_current_group")
        auto_move_enabled = getattr(sys.modules[__name__], "AUTO_MOVE_DUPES", False)
        # Phase: derive from current_step and flags for UI (format_analysis | identification_tags | ia_analysis | finalizing | moving_dupes)
        if not scanning:
            phase = None
        elif deduping:
            phase = "moving_dupes"
        elif finalizing:
            phase = "finalizing"
        elif current_step in ("comparing_versions", "detecting_best") or "_ai_batch" in active_artists_dict:
            phase = "ia_analysis"
        elif current_step == "searching_mb":
            phase = "identification_tags"
        elif current_step == "analyzing_format":
            phase = "format_analysis"
        else:
            phase = "format_analysis"
    
    # AI provider/model for display (read outside lock)
    ai_provider_display = AI_PROVIDER or ""
    ai_model_display = getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or OPENAI_MODEL or ""
    
    # When not scanning, attach last completed scan summary for "Scan complete – Summary" UI
    last_scan_summary = None
    if not scanning:
        try:
            con = sqlite3.connect(str(STATE_DB_FILE), timeout=2)
            cur = con.cursor()
            cur.execute(
                "SELECT summary_json FROM scan_history WHERE status = 'completed' AND end_time IS NOT NULL ORDER BY end_time DESC LIMIT 1"
            )
            row = cur.fetchone()
            con.close()
            if row and row[0]:
                last_scan_summary = json.loads(row[0])
                # Merge mb_match (from scan) and discogs/lastfm/bandcamp match (from scan fallback or last fix-all run) for chart-ready summary
                aw = last_scan_summary.get("albums_with_mb_id") or 0
                awo = last_scan_summary.get("albums_without_mb_id") or 0
                total_mb = aw + awo
                last_scan_summary["mb_match"] = {"matched": aw, "total": total_mb} if total_mb else {"matched": 0, "total": 0}
                albums_scanned = last_scan_summary.get("albums_scanned") or 0
                # Prefer scan fallback stats when present (Discogs/Last.fm/Bandcamp during scan when MB found nothing)
                scan_discogs = last_scan_summary.get("scan_discogs_matched")
                scan_lastfm = last_scan_summary.get("scan_lastfm_matched")
                scan_bandcamp = last_scan_summary.get("scan_bandcamp_matched")
                if scan_discogs is not None and albums_scanned:
                    last_scan_summary["discogs_match"] = {"matched": scan_discogs, "total": albums_scanned}
                elif last_fix_all_total_albums:
                    bp = (last_fix_all_by_provider or {}).get("discogs", {})
                    identified = (bp.get("identified") or 0) if isinstance(bp, dict) else 0
                    last_scan_summary["discogs_match"] = {"matched": identified, "total": last_fix_all_total_albums}
                else:
                    last_scan_summary["discogs_match"] = {"matched": 0, "total": 0}
                if scan_lastfm is not None and albums_scanned:
                    last_scan_summary["lastfm_match"] = {"matched": scan_lastfm, "total": albums_scanned}
                elif last_fix_all_total_albums:
                    bp = (last_fix_all_by_provider or {}).get("lastfm", {})
                    identified = (bp.get("identified") or 0) if isinstance(bp, dict) else 0
                    last_scan_summary["lastfm_match"] = {"matched": identified, "total": last_fix_all_total_albums}
                else:
                    last_scan_summary["lastfm_match"] = {"matched": 0, "total": 0}
                if scan_bandcamp is not None and albums_scanned:
                    last_scan_summary["bandcamp_match"] = {"matched": scan_bandcamp, "total": albums_scanned}
                elif last_fix_all_total_albums:
                    bp = (last_fix_all_by_provider or {}).get("bandcamp", {})
                    identified = (bp.get("identified") or 0) if isinstance(bp, dict) else 0
                    last_scan_summary["bandcamp_match"] = {"matched": identified, "total": last_fix_all_total_albums}
                else:
                    last_scan_summary["bandcamp_match"] = {"matched": 0, "total": 0}
        except Exception as e:
            logging.debug("api_progress: could not load last_scan_summary: %s", e)
    
    return jsonify(
        scanning=scanning,
        progress=progress,
        total=total,
        effective_progress=effective_progress,
        status=status,
        phase=phase,
        current_step=current_step,
        ai_provider=ai_provider_display,
        ai_model=ai_model_display,
        # Scan details
        artists_processed=artists_processed,
        artists_total=artists_total,
        ai_used_count=ai_used_count,
        mb_used_count=mb_used_count,
        ai_enabled=ai_enabled,
        mb_enabled=mb_enabled,
        # Cache statistics
        audio_cache_hits=audio_cache_hits,
        audio_cache_misses=audio_cache_misses,
        mb_cache_hits=mb_cache_hits,
        mb_cache_misses=mb_cache_misses,
        # Detailed statistics
        duplicate_groups_count=duplicate_groups_count,
        total_duplicates_count=total_duplicates_count,
        broken_albums_count=broken_albums_count,
        missing_albums_count=missing_albums_count,
        albums_without_artist_image=albums_without_artist_image,
        albums_without_album_image=albums_without_album_image,
        albums_without_complete_tags=albums_without_complete_tags,
        albums_without_mb_id=albums_without_mb_id,
        albums_without_artist_mb_id=albums_without_artist_mb_id,
        format_done_count=format_done_count,
        mb_done_count=mb_done_count,
        # Settings visible in scanner (e.g. link to configure)
        mb_retry_not_found=getattr(sys.modules[__name__], "MB_RETRY_NOT_FOUND", False),
        # ETA
        eta_seconds=eta_seconds,
        threads_in_use=threads_in_use,
        active_artists=active_artists_list,
        last_scan_summary=last_scan_summary,
        finalizing=finalizing,
        deduping=deduping,
        dedupe_progress=dedupe_progress,
        dedupe_total=dedupe_total,
        dedupe_current_group=dedupe_current_group,
        auto_move_enabled=auto_move_enabled,
        paths_status=_paths_rw_status(),
        # IA analysis step: current group label and N/M progress
        scan_ai_batch_total=scan_ai_batch_total,
        scan_ai_batch_processed=scan_ai_batch_processed,
        scan_ai_current_label=scan_ai_current_label,
    )

@app.get("/api/scan-history")
def api_scan_history():
    """Return list of all scan history entries."""
    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute("PRAGMA table_info(scan_history)")
    cols_info = [r[1] for r in cur.fetchall()]
    has_entry_type = "entry_type" in cols_info
    has_summary_json = "summary_json" in cols_info
    if has_entry_type and has_summary_json:
        cur.execute("""
            SELECT scan_id, start_time, end_time, duration_seconds, albums_scanned,
                   duplicates_found, artists_processed, artists_total, ai_used_count,
                   mb_used_count, ai_enabled, mb_enabled, auto_move_enabled,
                   space_saved_mb, albums_moved, status,
                   duplicate_groups_count, total_duplicates_count, broken_albums_count,
                   missing_albums_count, albums_without_artist_image, albums_without_album_image,
                   albums_without_complete_tags, albums_without_mb_id, albums_without_artist_mb_id,
                   entry_type, summary_json
            FROM scan_history
            ORDER BY start_time DESC
        """)
    elif has_entry_type:
        cur.execute("""
            SELECT scan_id, start_time, end_time, duration_seconds, albums_scanned,
                   duplicates_found, artists_processed, artists_total, ai_used_count,
                   mb_used_count, ai_enabled, mb_enabled, auto_move_enabled,
                   space_saved_mb, albums_moved, status,
                   duplicate_groups_count, total_duplicates_count, broken_albums_count,
                   missing_albums_count, albums_without_artist_image, albums_without_album_image,
                   albums_without_complete_tags, albums_without_mb_id, albums_without_artist_mb_id,
                   entry_type
            FROM scan_history
            ORDER BY start_time DESC
        """)
    elif has_summary_json:
        cur.execute("""
            SELECT scan_id, start_time, end_time, duration_seconds, albums_scanned,
                   duplicates_found, artists_processed, artists_total, ai_used_count,
                   mb_used_count, ai_enabled, mb_enabled, auto_move_enabled,
                   space_saved_mb, albums_moved, status,
                   duplicate_groups_count, total_duplicates_count, broken_albums_count,
                   missing_albums_count, albums_without_artist_image, albums_without_album_image,
                   albums_without_complete_tags, albums_without_mb_id, albums_without_artist_mb_id,
                   summary_json
            FROM scan_history
            ORDER BY start_time DESC
        """)
    else:
        cur.execute("""
            SELECT scan_id, start_time, end_time, duration_seconds, albums_scanned,
                   duplicates_found, artists_processed, artists_total, ai_used_count,
                   mb_used_count, ai_enabled, mb_enabled, auto_move_enabled,
                   space_saved_mb, albums_moved, status,
                   duplicate_groups_count, total_duplicates_count, broken_albums_count,
                   missing_albums_count, albums_without_artist_image, albums_without_album_image,
                   albums_without_complete_tags, albums_without_mb_id, albums_without_artist_mb_id
            FROM scan_history
            ORDER BY start_time DESC
        """)
    rows = cur.fetchall()
    con.close()

    history = []
    for row in rows:
        base = 25 if has_entry_type else 24
        entry = {
            "scan_id": row[0],
            "start_time": row[1],
            "end_time": row[2],
            "duration_seconds": row[3],
            "albums_scanned": row[4] or 0,
            "duplicates_found": row[5] or 0,
            "artists_processed": row[6] or 0,
            "artists_total": row[7] or 0,
            "ai_used_count": row[8] or 0,
            "mb_used_count": row[9] or 0,
            "ai_enabled": bool(row[10]),
            "mb_enabled": bool(row[11]),
            "auto_move_enabled": bool(row[12]),
            "space_saved_mb": row[13] or 0,
            "albums_moved": row[14] or 0,
            "status": row[15] or "completed",
            "duplicate_groups_count": row[16] or 0 if len(row) > 16 else 0,
            "total_duplicates_count": row[17] or 0 if len(row) > 17 else 0,
            "broken_albums_count": row[18] or 0 if len(row) > 18 else 0,
            "missing_albums_count": row[19] or 0 if len(row) > 19 else 0,
            "albums_without_artist_image": row[20] or 0 if len(row) > 20 else 0,
            "albums_without_album_image": row[21] or 0 if len(row) > 21 else 0,
            "albums_without_complete_tags": row[22] or 0 if len(row) > 22 else 0,
            "albums_without_mb_id": row[23] or 0 if len(row) > 23 else 0,
            "albums_without_artist_mb_id": row[24] or 0 if len(row) > 24 else 0,
        }
        if has_entry_type and len(row) > 25:
            entry["entry_type"] = row[25] or "scan"
        else:
            entry["entry_type"] = "scan"
        if has_summary_json and len(row) > base + 1:
            try:
                raw = row[base + 1]
                entry["summary_json"] = json.loads(raw) if raw else None
            except (TypeError, ValueError):
                entry["summary_json"] = None
        else:
            entry["summary_json"] = None
        history.append(entry)

    return jsonify(history)

@app.delete("/api/scan-history")
def api_scan_history_clear():
    """Delete all scan history entries (and related scan_editions). Requires confirmation from client."""
    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    try:
        cur.execute("DELETE FROM scan_editions")
        cur.execute("DELETE FROM scan_history")
        con.commit()
        deleted = cur.rowcount if hasattr(cur, "rowcount") else 0
    finally:
        con.close()
    return jsonify({"status": "ok", "message": "Scan history cleared."})

def add_broken_album_to_lidarr(artist_name: str, album_id: int, musicbrainz_release_group_id: str, album_title: str) -> bool:
    """
    Add a broken album to Lidarr for re-download.
    Returns True if successful, False otherwise.
    """
    if not LIDARR_URL or not LIDARR_API_KEY:
        logging.warning("Lidarr not configured (LIDARR_URL or LIDARR_API_KEY missing)")
        return False
    
    try:
        # First, search for the artist in Lidarr
        search_url = f"{LIDARR_URL.rstrip('/')}/api/v1/artist/lookup"
        headers = {"X-Api-Key": LIDARR_API_KEY}
        
        # Try to find artist by MusicBrainz ID or name
        # We need the artist MBID - for now, search by name
        search_params = {"term": artist_name}
        response = requests.get(search_url, headers=headers, params=search_params, timeout=10)
        
        if response.status_code != 200:
            logging.error("Lidarr artist search failed: %s", response.text)
            return False
        
        artists = response.json()
        if not artists:
            logging.warning("Artist '%s' not found in Lidarr", artist_name)
            return False
        
        # Use first matching artist
        lidarr_artist = artists[0]
        lidarr_artist_id = lidarr_artist.get('id')
        
        if not lidarr_artist_id:
            logging.warning("Lidarr artist '%s' has no ID", artist_name)
            return False
        
        # Now add the album to Lidarr
        # First, check if album already exists
        album_lookup_url = f"{LIDARR_URL.rstrip('/')}/api/v1/album/lookup"
        album_params = {"term": f"mbid:{musicbrainz_release_group_id}"}
        album_response = requests.get(album_lookup_url, headers=headers, params=album_params, timeout=10)
        
        if album_response.status_code == 200:
            albums = album_response.json()
            if albums:
                # Album found, add it to the artist
                album_data = albums[0]
                add_album_url = f"{LIDARR_URL.rstrip('/')}/api/v1/album"
                add_payload = {
                    "artistId": lidarr_artist_id,
                    "album": album_data,
                    "addOptions": {
                        "searchForMissingAlbums": True,
                        "monitor": "missing"
                    }
                }
                add_response = requests.post(add_album_url, headers=headers, json=add_payload, timeout=10)
                
                if add_response.status_code in (200, 201):
                    logging.info("Successfully added broken album '%s' by '%s' to Lidarr", album_title, artist_name)
                    return True
                else:
                    logging.error("Failed to add album to Lidarr: %s", add_response.text)
                    return False
        
        logging.warning("Album with MBID %s not found in Lidarr", musicbrainz_release_group_id)
        return False
        
    except requests.exceptions.RequestException as e:
        logging.error("Lidarr API request failed: %s", e)
        return False
    except Exception as e:
        logging.error("Unexpected error adding album to Lidarr: %s", e, exc_info=True)
        return False

@app.get("/api/broken-albums")
def api_broken_albums():
    """Return list of broken albums in selected library sections only (SECTION_IDS)."""
    _reload_section_ids_from_db()
    import sqlite3
    import json
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    cur = con.cursor()
    cur.execute("""
        SELECT artist, album_id, expected_track_count, actual_track_count,
               missing_indices, musicbrainz_release_group_id, detected_at, sent_to_lidarr
        FROM broken_albums
        ORDER BY detected_at DESC
    """)
    rows = cur.fetchall()
    con.close()

    # Filter by SECTION_IDS: only include albums that belong to selected library sections
    broken_albums = []
    db_conn = None
    try:
        db_conn = plex_connect()
        placeholders = ",".join("?" for _ in SECTION_IDS) if SECTION_IDS else ""
        for row in rows:
            album_id = row[1]
            if SECTION_IDS and placeholders:
                section_row = db_conn.execute(
                    f"SELECT library_section_id FROM metadata_items WHERE id = ? AND metadata_type = 9",
                    (album_id,),
                ).fetchone()
                if not section_row or section_row[0] not in SECTION_IDS:
                    continue
            title = album_title(db_conn, album_id) if db_conn else f"Album {album_id}"
            missing_indices = json.loads(row[4]) if row[4] else []
            broken_albums.append({
                "artist": row[0],
                "album_id": album_id,
                "album_title": title,
                "expected_track_count": row[2],
                "actual_track_count": row[3],
                "missing_indices": missing_indices,
                "musicbrainz_release_group_id": row[5],
                "detected_at": row[6],
                "sent_to_lidarr": bool(row[7]) if row[7] is not None else False
            })
    except Exception as e:
        logging.warning("Failed to fetch album titles for broken albums: %s", e)
        # Fallback without titles
        for row in rows:
            missing_indices = json.loads(row[4]) if row[4] else []
            broken_albums.append({
                "artist": row[0],
                "album_id": row[1],
                "album_title": f"Album {row[1]}",
                "expected_track_count": row[2],
                "actual_track_count": row[3],
                "missing_indices": missing_indices,
                "musicbrainz_release_group_id": row[5],
                "detected_at": row[6],
                "sent_to_lidarr": bool(row[7]) if row[7] is not None else False
            })
    finally:
        if db_conn:
            try:
                db_conn.close()
            except Exception:
                pass

    return jsonify(broken_albums)


@app.get("/api/library/stats")
def api_library_stats():
    """Return library stats (artists count, albums count) for selected sections. Used by Unduper and others."""
    _reload_section_ids_from_db()
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    if not SECTION_IDS:
        return jsonify({"artists": 0, "albums": 0})
    placeholders = ",".join("?" for _ in SECTION_IDS)
    section_args = list(SECTION_IDS)
    artist_section_filter = f"AND art.library_section_id IN ({placeholders})"
    album_section_filter = f"AND alb.library_section_id IN ({placeholders})"
    db_conn = plex_connect()
    # Artists with at least one album in selected sections
    artist_count_row = db_conn.execute(f"""
        SELECT COUNT(DISTINCT art.id)
        FROM metadata_items art
        INNER JOIN metadata_items alb ON alb.parent_id = art.id AND alb.metadata_type = 9
            {album_section_filter}
        WHERE art.metadata_type = 8
            {artist_section_filter}
    """, section_args + section_args).fetchone()
    artists = (artist_count_row[0] if artist_count_row else 0) or 0
    # Albums in selected sections
    album_count_row = db_conn.execute(f"""
        SELECT COUNT(DISTINCT alb.id)
        FROM metadata_items alb
        WHERE alb.metadata_type = 9
            AND alb.library_section_id IN ({placeholders})
    """, section_args).fetchone()
    albums = (album_count_row[0] if album_count_row else 0) or 0
    db_conn.close()
    return jsonify({"artists": artists, "albums": albums})


@app.get("/api/library/albums-with-parenthetical-names")
def api_library_albums_with_parenthetical_names():
    """Return albums whose folder name (or Plex title) has removable parenthetical suffixes like (flac), (mp3), (EP)."""
    _reload_section_ids_from_db()
    _reload_path_map_from_db()
    if not PLEX_CONFIGURED or not SECTION_IDS:
        return jsonify({"albums": []})
    ph = ",".join("?" for _ in SECTION_IDS)
    db_conn = plex_connect()
    try:
        rows = db_conn.execute(f"""
            SELECT alb.id, alb.title, alb.parent_id
            FROM metadata_items alb
            WHERE alb.metadata_type = 9 AND alb.library_section_id IN ({ph})
        """, list(SECTION_IDS)).fetchall()
        artist_cache = {}
        out = []
        for album_id, title, parent_id in rows:
            artist_name = ""
            if parent_id:
                if parent_id not in artist_cache:
                    r = db_conn.execute("SELECT title FROM metadata_items WHERE id = ?", (parent_id,)).fetchone()
                    artist_cache[parent_id] = (r[0] or "").strip() if r else ""
                artist_name = artist_cache[parent_id]
            folder = first_part_path(db_conn, album_id)
            if not folder or not folder.exists():
                continue
            current_name = folder.name
            proposed_name = strip_parenthetical_suffixes(current_name)
            if not proposed_name or proposed_name == current_name:
                continue
            proposed_path = folder.parent / proposed_name
            if proposed_path == folder:
                continue
            out.append({
                "album_id": album_id,
                "artist": artist_name,
                "title": title or current_name,
                "current_path": str(folder),
                "proposed_path": str(proposed_path),
                "current_name": current_name,
                "proposed_name": proposed_name,
            })
        return jsonify({"albums": out})
    finally:
        db_conn.close()


@app.post("/api/library/normalize-album-names")
def api_library_normalize_album_names():
    """Rename album folders by removing parenthetical format/version suffixes (e.g. (flac), (EP)). Body: { album_ids: number[] } or empty for all from GET list."""
    _reload_path_map_from_db()
    data = request.get_json() or {}
    album_ids = data.get("album_ids")
    if album_ids is not None and not isinstance(album_ids, list):
        return jsonify({"error": "album_ids must be an array"}), 400
    _reload_section_ids_from_db()
    if not PLEX_CONFIGURED or not SECTION_IDS:
        return jsonify({"error": "Plex not configured"}), 503
    db_conn = plex_connect()
    try:
        if not album_ids:
            ph = ",".join("?" for _ in SECTION_IDS)
            rows = db_conn.execute(f"""
                SELECT id FROM metadata_items
                WHERE metadata_type = 9 AND library_section_id IN ({ph})
            """, list(SECTION_IDS)).fetchall()
            album_ids = [r[0] for r in rows]
        renamed = []
        errors = []
        for album_id in album_ids:
            folder = first_part_path(db_conn, album_id)
            if not folder or not folder.exists():
                errors.append({"album_id": album_id, "message": "Folder not found"})
                continue
            current_name = folder.name
            proposed_name = strip_parenthetical_suffixes(current_name)
            if not proposed_name or proposed_name == current_name:
                continue
            proposed_path = folder.parent / proposed_name
            if proposed_path == folder:
                continue
            if proposed_path.exists():
                errors.append({"album_id": album_id, "path": str(proposed_path), "message": "Target path already exists"})
                continue
            try:
                folder.rename(proposed_path)
                renamed.append({"album_id": album_id, "from": str(folder), "to": str(proposed_path)})
            except OSError as e:
                errors.append({"album_id": album_id, "message": str(e)})
        return jsonify({"renamed": renamed, "errors": errors})
    finally:
        db_conn.close()


@app.get("/api/library/artists")
def api_library_artists():
    """Return list of artists with statistics. Supports search and pagination.
    Always restricted to SECTION_IDS (selected libraries) — CROSS_LIBRARY_DEDUPE only affects duplicate detection, not which artists are listed.
    """
    _reload_section_ids_from_db()
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    if not SECTION_IDS:
        return jsonify({"artists": [], "total": 0, "limit": 100, "offset": 0})
    
    import sqlite3
    search_query = request.args.get("search", "").strip()
    limit = int(request.args.get("limit", 100))
    offset = int(request.args.get("offset", 0))
    
    db_conn = plex_connect()
    
    # Build search filter
    if search_query:
        search_filter = "AND art.title LIKE ? ESCAPE '\\'"
        escaped_query = search_query.replace('%', '\\%').replace('_', '\\_')
        search_args = [f"%{escaped_query}%"]
    else:
        search_filter = ""
        search_args = []
    
    # Always filter by selected library sections (SECTION_IDS) for listing — not affected by CROSS_LIBRARY_DEDUPE
    placeholders = ",".join("?" for _ in SECTION_IDS)
    section_args = list(SECTION_IDS)
    artist_section_filter = f"AND art.library_section_id IN ({placeholders})"
    album_section_filter = f"AND alb.library_section_id IN ({placeholders})"
    
    # Get total count for pagination (artists in selected sections only)
    count_cursor = db_conn.execute(f"""
        SELECT COUNT(DISTINCT art.id)
        FROM metadata_items art
        LEFT JOIN metadata_items alb ON alb.parent_id = art.id 
            AND alb.metadata_type = 9
            {album_section_filter}
        WHERE art.metadata_type = 8
            {artist_section_filter}
            {search_filter}
        HAVING COUNT(DISTINCT alb.id) > 0
    """, section_args + section_args + search_args)
    count_row = count_cursor.fetchone() if count_cursor else None
    total_count = count_row[0] if count_row else 0

    # Get paginated artists (selected sections only)
    cursor = db_conn.execute(f"""
        SELECT 
            art.id,
            art.title as artist_name,
            COUNT(DISTINCT alb.id) as album_count
        FROM metadata_items art
        LEFT JOIN metadata_items alb ON alb.parent_id = art.id 
            AND alb.metadata_type = 9
            {album_section_filter}
        WHERE art.metadata_type = 8
            {artist_section_filter}
            {search_filter}
        GROUP BY art.id, art.title
        HAVING COUNT(DISTINCT alb.id) > 0
        ORDER BY art.title
        LIMIT ? OFFSET ?
    """, section_args + section_args + search_args + [limit, offset])
    
    artists = []
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    cur = con.cursor()
    
    aggregated: dict[str, dict] = {}
    for row in cursor.fetchall():
        artist_id, artist_name, album_count = row
        # Get broken albums count
        cur.execute("SELECT COUNT(*) FROM broken_albums WHERE artist = ?", (artist_name,))
        broken_count = cur.fetchone()[0] or 0
        name_norm = (artist_name or "").strip().lower()
        if name_norm not in aggregated:
            aggregated[name_norm] = {
                "artist_id": artist_id,
                "artist_name": artist_name,
                "album_count": album_count or 0,
                "broken_albums_count": broken_count,
                "all_ids": [artist_id],
            }
        else:
            aggregated[name_norm]["album_count"] += album_count or 0
            aggregated[name_norm]["broken_albums_count"] += broken_count
            aggregated[name_norm]["all_ids"].append(artist_id)
    for data in aggregated.values():
        artists.append({
            "artist_id": data["artist_id"],
            "artist_name": data["artist_name"],
            "album_count": data["album_count"],
            "broken_albums_count": data["broken_albums_count"],
        })
    
    con.close()
    db_conn.close()
    return jsonify({
        "artists": artists,
        "total": total_count,
        "limit": limit,
        "offset": offset
    })

@app.get("/api/library/artist/<int:artist_id>")
def api_library_artist_detail(artist_id):
    """Return detailed information about an artist including all albums with images and types."""
    _reload_section_ids_from_db()
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    
    import sqlite3
    db_conn = plex_connect()
    
    # Get artist info
    artist_row = db_conn.execute(
        "SELECT id, title FROM metadata_items WHERE id = ? AND metadata_type = 8",
        (artist_id,)
    ).fetchone()
    
    if not artist_row:
        db_conn.close()
        return jsonify({"error": "Artist not found"}), 404
    
    artist_name = artist_row[1]
    name_norm = (artist_name or "").strip().lower()
    # Collect all artist IDs with same normalized name in selected sections
    artist_ids_same_name = [artist_id]
    try:
        placeholders_sections = ",".join("?" for _ in SECTION_IDS) if SECTION_IDS else ""
        if placeholders_sections:
            rows_same = db_conn.execute(
                f"""
                SELECT id FROM metadata_items 
                WHERE metadata_type = 8 
                  AND title IS NOT NULL 
                  AND LOWER(TRIM(title)) = ? 
                  AND library_section_id IN ({placeholders_sections})
                """,
                [name_norm] + list(SECTION_IDS)
            ).fetchall()
            artist_ids_same_name = list({r[0] for r in rows_same} | {artist_id})
    except Exception:
        artist_ids_same_name = [artist_id]
    
    # Get artist thumb from Plex
    artist_thumb = None
    try:
        thumb_row = db_conn.execute(
            "SELECT thumb FROM metadata_items WHERE id = ? AND metadata_type = 8",
            (artist_id,)
        ).fetchone()
        if thumb_row and thumb_row[0]:
            artist_thumb = thumb_url(artist_id)
    except Exception:
        pass
    
    # Get all albums for this artist (only from selected sections — SECTION_IDS)
    placeholders = ",".join("?" for _ in SECTION_IDS) if SECTION_IDS else ""
    if not placeholders:
        album_rows = []
    else:
        section_filter = f"AND alb.library_section_id IN ({placeholders})"
        section_args = artist_ids_same_name + list(SECTION_IDS)
        # Do not select alb.thumb — column may not exist in all Plex DB versions
        album_rows = db_conn.execute(f"""
            SELECT 
                alb.id,
                alb.title,
                alb.year,
                alb.originally_available_at,
                COUNT(DISTINCT tr.id) as track_count
            FROM metadata_items alb
            LEFT JOIN metadata_items tr ON tr.parent_id = alb.id AND tr.metadata_type = 10
            WHERE alb.parent_id IN ({",".join("?" for _ in artist_ids_same_name)}) AND alb.metadata_type = 9
                {section_filter}
            GROUP BY alb.id, alb.title, alb.year, alb.originally_available_at
            ORDER BY alb.originally_available_at DESC, alb.title
        """, section_args).fetchall()
    
    # Batch-fetch track indices from Plex for gap detection (incomplete albums) without relying on scan
    album_ids = [r[0] for r in album_rows]
    indices_by_album: dict[int, list[int]] = {}
    if album_ids:
        try:
            ph = ",".join("?" for _ in album_ids)
            track_index_rows = db_conn.execute(
                f'SELECT parent_id, "index" FROM metadata_items WHERE metadata_type = 10 AND parent_id IN ({ph})',
                album_ids,
            ).fetchall()
            for pid, idx in track_index_rows:
                if pid is not None and idx is not None:
                    indices_by_album.setdefault(pid, []).append(int(idx))
        except Exception as e:
            logging.debug("Batch track indices query failed: %s", e)
    
    albums = []
    
    # Prefer scan_editions when a completed scan exists (source of truth for format, tags, broken, duplicate group)
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    cur = con.cursor()
    scan_id = get_last_completed_scan_id()
    scan_editions_by_album: dict[int, dict] = {}
    dup_album_ids_from_scan: set[int] = set()
    if scan_id and artist_name:
        try:
            cur.execute("""
                SELECT album_id, title_raw, folder, fmt_text, br, sr, bd, meta_json, musicbrainz_id,
                       is_broken, expected_track_count, actual_track_count, missing_indices, has_cover, missing_required_tags
                FROM scan_editions WHERE scan_id = ? AND artist = ?
            """, (scan_id, artist_name))
            for row in cur.fetchall():
                aid = row[0]
                scan_editions_by_album[aid] = {
                    "title_raw": row[1], "folder": row[2], "fmt_text": row[3], "br": row[4], "sr": row[5], "bd": row[6],
                    "meta_json": row[7], "musicbrainz_id": row[8], "is_broken": row[9], "expected_track_count": row[10],
                    "actual_track_count": row[11], "missing_indices": row[12], "has_cover": row[13], "missing_required_tags": row[14],
                }
            cur.execute("SELECT album_id FROM duplicates_best WHERE artist = ?", (artist_name,))
            dup_album_ids_from_scan.update(r[0] for r in cur.fetchall())
            cur.execute("SELECT album_id FROM duplicates_loser WHERE artist = ?", (artist_name,))
            dup_album_ids_from_scan.update(r[0] for r in cur.fetchall())
        except Exception as e:
            logging.debug("scan_editions query for Library artist failed: %s", e)
            scan_editions_by_album = {}
            dup_album_ids_from_scan = set()
    
    lossless_formats = {"FLAC", "ALAC", "APE", "WV", "WAV", "AIFF", "OGG"}
    
    for album_row in album_rows:
        album_id, title, year, date, track_count = album_row
        se = scan_editions_by_album.get(album_id)
        in_duplicate_group = (album_id in dup_album_ids_from_scan) if dup_album_ids_from_scan else False
        musicbrainz_release_group_id = None

        if se:
            # Use scan_editions as source of truth
            format_str = se.get("fmt_text") or None
            is_lossless = bool(format_str and format_str.upper() in lossless_formats)
            mb_identified = bool(se.get("musicbrainz_id"))
            if se.get("musicbrainz_id"):
                musicbrainz_release_group_id = se.get("musicbrainz_id")
            thumb_empty = not (se.get("has_cover"))
            is_broken = bool(se.get("is_broken"))
            missing_raw = se.get("missing_indices")
            broken_detail = None
            if is_broken and (se.get("expected_track_count") is not None or missing_raw):
                broken_detail = {
                    "expected_track_count": se.get("expected_track_count") or 0,
                    "actual_track_count": se.get("actual_track_count") or 0,
                    "missing_indices": json.loads(missing_raw) if isinstance(missing_raw, str) and missing_raw else (missing_raw or []),
                }
            album_type = "Album"
            if (track_count or 0) <= 3:
                album_type = "Single"
            elif (track_count or 0) <= 6:
                album_type = "EP"
        else:
            # Fallback: broken_albums + on-disk format/tags
            cur.execute("SELECT expected_track_count, actual_track_count, missing_indices FROM broken_albums WHERE artist = ? AND album_id = ?", (artist_name, album_id))
            broken_row = cur.fetchone()
            is_broken = broken_row is not None
            broken_detail = None
            if broken_row:
                missing_raw = broken_row[2]
                broken_detail = {
                    "expected_track_count": broken_row[0] or 0,
                    "actual_track_count": broken_row[1] or 0,
                    "missing_indices": json.loads(missing_raw) if isinstance(missing_raw, str) and missing_raw else []
                }
            else:
                indices = indices_by_album.get(album_id, [])
                if indices:
                    broken_from_gaps, actual_count, gaps = _detect_gaps_in_indices(indices)
                    if broken_from_gaps:
                        is_broken = True
                        broken_detail = {
                            "expected_track_count": None,
                            "actual_track_count": actual_count,
                            "missing_indices": list(gaps),
                        }
            thumb_empty = False
            format_str = None
            is_lossless = False
            mb_identified = False
            album_type = "Album"
            folder = first_part_path(db_conn, album_id)
            if folder:
                if not album_folder_has_cover(folder):
                    thumb_empty = True
                format_str = get_primary_format(folder)
                if format_str and format_str.upper() in lossless_formats:
                    is_lossless = True
                first_audio = next((p for p in folder.rglob("*") if AUDIO_RE.search(p.name)), None)
                if first_audio:
                    meta = extract_tags(first_audio)
                    mb_identified = bool(meta.get("musicbrainz_releasegroupid") or meta.get("musicbrainz_releaseid"))
                    if meta.get("compilation") == "1" or meta.get("compilation") == "true":
                        album_type = "Compilation"
                    elif USE_MUSICBRAINZ:
                        mbid = meta.get("musicbrainz_releasegroupid") or meta.get("musicbrainz_releaseid")
                        if mbid:
                            if meta.get("musicbrainz_releasegroupid"):
                                musicbrainz_release_group_id = meta.get("musicbrainz_releasegroupid")
                            tag_src = "musicbrainz_releasegroupid" if meta.get("musicbrainz_releasegroupid") else "musicbrainz_releaseid"
                            rgid = resolve_mbid_to_release_group(mbid, tag_src)
                            if rgid:
                                if not musicbrainz_release_group_id:
                                    musicbrainz_release_group_id = rgid
                                try:
                                    result = musicbrainzngs.get_release_group_by_id(rgid, includes=["tags"])
                                    release_group = result.get("release-group", {})
                                    primary_type = release_group.get("primary-type", "")
                                    secondary_types = release_group.get("secondary-type-list", [])
                                    if primary_type:
                                        album_type = primary_type
                                    if "Compilation" in secondary_types:
                                        album_type = "Compilation"
                                    elif "Anthology" in secondary_types:
                                        album_type = "Anthology"
                                except Exception:
                                    pass
                    if (track_count or 0) <= 3 and album_type == "Album":
                        album_type = "Single"
                    elif (track_count or 0) <= 6 and album_type == "Album":
                        album_type = "EP"

        try:
            album_thumb = thumb_url(album_id)
        except Exception:
            album_thumb = None

        can_improve = not is_lossless or thumb_empty or not mb_identified or is_broken
        
        albums.append({
            "album_id": album_id,
            "title": title,
            "year": year,
            "date": date,
            "track_count": track_count or 0,
            "is_broken": is_broken,
            "thumb": album_thumb,
            "type": album_type,
            "format": format_str,
            "is_lossless": is_lossless,
            "thumb_empty": thumb_empty,
            "mb_identified": mb_identified,
            "musicbrainz_release_group_id": musicbrainz_release_group_id,
            "in_duplicate_group": in_duplicate_group,
            "can_improve": can_improve,
            "broken_detail": broken_detail,
        })
    
    # Duplicates: when scan data exists use dup_album_ids_from_scan; otherwise same artist + same album name (normalized)
    if dup_album_ids_from_scan:
        stats_duplicates = len(dup_album_ids_from_scan)
    else:
        norm_to_album_ids: dict[str, list[int]] = {}
        for a in albums:
            norm = norm_album(a.get("title", "") or "")
            norm_to_album_ids.setdefault(norm, []).append(a["album_id"])
        dup_album_ids: set[int] = set()
        for aid_list in norm_to_album_ids.values():
            if len(aid_list) > 1:
                dup_album_ids.update(aid_list)
        stats_duplicates = len(dup_album_ids)
        for a in albums:
            if a["album_id"] in dup_album_ids:
                a["in_duplicate_group"] = True
    stats_no_cover = sum(1 for a in albums if a.get("thumb_empty"))
    stats_mb = sum(1 for a in albums if a.get("mb_identified"))
    stats_broken = sum(1 for a in albums if a.get("is_broken"))
    
    con.close()
    db_conn.close()
    
    return jsonify({
        "artist_id": artist_id,
        "artist_name": artist_name,
        "artist_thumb": artist_thumb,
        "albums": albums,
        "total_albums": len(albums),
        "stats": {
            "duplicates": stats_duplicates,
            "no_cover": stats_no_cover,
            "mb_identified": stats_mb,
            "broken": stats_broken,
        },
    })


@app.get("/api/library/missing-tags")
def api_library_missing_tags():
    """Return albums in selected sections that have missing MusicBrainz or required tags.
    Prefer scan_editions from last completed scan when available."""
    _reload_section_ids_from_db()
    if not PLEX_CONFIGURED:
        return jsonify({"albums": []})
    if not SECTION_IDS:
        return jsonify({"albums": []})

    scan_id = get_last_completed_scan_id()
    if scan_id:
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        cur.execute("""
            SELECT artist, album_id, title_raw, missing_required_tags
            FROM scan_editions
            WHERE scan_id = ? AND missing_required_tags IS NOT NULL AND missing_required_tags != '' AND missing_required_tags != '[]'
            ORDER BY artist, title_raw
        """, (scan_id,))
        rows = cur.fetchall()
        con.close()
        if rows:
            results = []
            for artist_name, album_id, title_raw, missing_required_tags in rows:
                try:
                    missing_tags = json.loads(missing_required_tags) if isinstance(missing_required_tags, str) else (missing_required_tags or [])
                except (json.JSONDecodeError, TypeError):
                    missing_tags = []
                if missing_tags:
                    results.append({
                        "artist_name": artist_name or "",
                        "album_id": album_id,
                        "album_title": (title_raw or "").strip() or "",
                        "missing_tags": missing_tags,
                    })
            return jsonify({"albums": results})
        return jsonify({"albums": []})

    # No completed scan (e.g. after Clear results): show nothing until next scan
    return jsonify({"albums": []})


@app.get("/api/library/album/<int:album_id>/tracks")
def api_library_album_tracks(album_id):
    """Return track list for an album for playback (track_id, title, duration, file_url)."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    db_conn = plex_connect()
    try:
        album_row = db_conn.execute(
            "SELECT title, parent_id FROM metadata_items WHERE id = ? AND metadata_type = 9",
            (album_id,),
        ).fetchone()
        if not album_row:
            return jsonify({"error": "Album not found"}), 404
        album_title, artist_parent_id = album_row[0], album_row[1]
        artist_name = ""
        if artist_parent_id:
            artist_row = db_conn.execute(
                "SELECT title FROM metadata_items WHERE id = ? AND metadata_type = 8",
                (artist_parent_id,),
            ).fetchone()
            if artist_row:
                artist_name = artist_row[0] or ""
        raw = get_tracks_with_ids(db_conn, album_id)
        base_url = request.url_root.rstrip("/")
        tracks = [
            {
                "track_id": t["id"],
                "title": t["title"],
                "artist": artist_name,
                "album": album_title or "",
                "duration": (t["duration_ms"] or 0) // 1000,
                "index": t["index"],
                "file_url": f"{base_url}/api/library/track/{t['id']}/stream",
            }
            for t in raw
        ]
        album_thumb = thumb_url(album_id)
        return jsonify({"tracks": tracks, "album_thumb": album_thumb})
    finally:
        db_conn.close()


def _track_file_path(db_conn, track_id: int) -> Optional[Path]:
    """Return the filesystem path for a track (metadata_item id), or None if not found."""
    row = db_conn.execute(
        """
        SELECT mp.file
        FROM metadata_items tr
        JOIN media_items mi ON mi.metadata_item_id = tr.id
        JOIN media_parts mp ON mp.media_item_id = mi.id
        WHERE tr.id = ? AND tr.metadata_type = 10
        LIMIT 1
        """,
        (track_id,),
    ).fetchone()
    if not row or not (raw := (row[0] or "").strip()):
        return None
    p = Path(raw)
    return path_for_fs_access(p)


@app.get("/api/library/track/<int:track_id>/stream")
def api_library_track_stream(track_id):
    """Stream a track from local file when possible, else proxy from Plex (avoids 502 when Plex URL unreachable from container)."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    db_conn = plex_connect()
    try:
        local_path = _track_file_path(db_conn, track_id)
    finally:
        db_conn.close()
    if local_path and local_path.exists() and local_path.is_file():
        try:
            return send_file(str(local_path), as_attachment=False, conditional=True)
        except Exception as e:
            logging.warning("track stream send_file failed for track %s: %s", track_id, e)
    url = f"{PLEX_HOST.rstrip('/')}/library/metadata/{track_id}/file?X-Plex-Token={PLEX_TOKEN}"
    try:
        r = requests.get(url, stream=True, timeout=60)
        r.raise_for_status()
        headers = {}
        if r.headers.get("Content-Type"):
            headers["Content-Type"] = r.headers["Content-Type"]
        if r.headers.get("Content-Length"):
            headers["Content-Length"] = r.headers["Content-Length"]
        return Response(
            r.iter_content(chunk_size=65536),
            status=r.status_code,
            headers=headers,
            direct_passthrough=True,
        )
    except requests.RequestException as e:
        logging.warning("track stream proxy failed for track %s: %s", track_id, e)
        return jsonify({"error": "Stream failed"}), 502


@app.post("/api/lidarr/add-album")
def api_lidarr_add_album():
    """Add a broken album to Lidarr for re-download."""
    data = request.get_json() or {}
    artist_name = data.get("artist_name")
    album_id = data.get("album_id")
    musicbrainz_release_group_id = data.get("musicbrainz_release_group_id")
    album_title = data.get("album_title", "")
    
    if not artist_name or not musicbrainz_release_group_id:
        return jsonify({"error": "Missing required fields: artist_name, musicbrainz_release_group_id"}), 400
    
    success = add_broken_album_to_lidarr(artist_name, album_id or 0, musicbrainz_release_group_id, album_title)
    
    if success:
        # Update database
        import sqlite3
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
        cur = con.cursor()
        cur.execute("""
            UPDATE broken_albums SET sent_to_lidarr = 1 
            WHERE artist = ? AND album_id = ?
        """, (artist_name, album_id))
        con.commit()
        con.close()
        
        return jsonify({"success": True, "message": f"Album '{album_title}' added to Lidarr"})
    else:
        return jsonify({"success": False, "message": "Failed to add album to Lidarr"}), 500


def _run_lidarr_add_incomplete_albums(rows: List[tuple]):
    """Background worker: add each incomplete (broken) album to Lidarr. rows: (artist, album_id, musicbrainz_release_group_id, album_title)."""
    total = len(rows)
    added = 0
    failed = 0
    with lock:
        state["lidarr_add_incomplete"] = {
            "running": True,
            "current": 0,
            "total": total,
            "current_album": None,
            "current_artist": None,
            "added": 0,
            "failed": 0,
            "result": None,
        }
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    try:
        for i, (artist_name, album_id, mbid, album_title) in enumerate(rows):
            with lock:
                if state.get("lidarr_add_incomplete") and state["lidarr_add_incomplete"].get("running"):
                    state["lidarr_add_incomplete"]["current"] = i
                    state["lidarr_add_incomplete"]["current_album"] = album_title
                    state["lidarr_add_incomplete"]["current_artist"] = artist_name
            success = add_broken_album_to_lidarr(artist_name, album_id, mbid or "", album_title)
            if success:
                added += 1
                cur = con.cursor()
                cur.execute("UPDATE broken_albums SET sent_to_lidarr = 1 WHERE artist = ? AND album_id = ?", (artist_name, album_id))
                con.commit()
            else:
                failed += 1
            with lock:
                if state.get("lidarr_add_incomplete") and state["lidarr_add_incomplete"].get("running"):
                    state["lidarr_add_incomplete"]["current"] = i + 1
                    state["lidarr_add_incomplete"]["added"] = added
                    state["lidarr_add_incomplete"]["failed"] = failed
        with lock:
            if state.get("lidarr_add_incomplete"):
                state["lidarr_add_incomplete"]["running"] = False
                state["lidarr_add_incomplete"]["result"] = {
                    "added": added,
                    "failed": failed,
                    "total": total,
                    "skipped": total - added - failed,
                }
            state["last_lidarr_add_added"] = added
            state["last_lidarr_add_failed"] = failed
    except Exception as e:
        logging.exception("lidarr add-incomplete-albums failed: %s", e)
        with lock:
            if state.get("lidarr_add_incomplete"):
                state["lidarr_add_incomplete"]["running"] = False
                state["lidarr_add_incomplete"]["result"] = {"error": str(e)}
    finally:
        con.close()


@app.post("/api/lidarr/add-incomplete-albums")
def api_lidarr_add_incomplete_albums():
    """Start adding all incomplete (broken) albums that are not yet sent to Lidarr."""
    if not LIDARR_URL or not LIDARR_API_KEY:
        return jsonify({"error": "Lidarr not configured (URL and API Key required)", "started": False}), 503
    with lock:
        if state.get("lidarr_add_incomplete") and state["lidarr_add_incomplete"].get("running"):
            return jsonify({"error": "Add incomplete albums already running", "started": False}), 409
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT artist, album_id, musicbrainz_release_group_id FROM broken_albums WHERE sent_to_lidarr = 0"
        )
        raw_rows = cur.fetchall()
    finally:
        con.close()
    # Resolve album titles from Plex for display
    rows = []
    db_conn = None
    try:
        db_conn = plex_connect()
        for (artist_name, album_id, mbid) in raw_rows:
            title = album_title(db_conn, album_id) if db_conn else f"Album {album_id}"
            rows.append((artist_name, album_id, mbid, title))
    except Exception:
        rows = [(r[0], r[1], r[2], f"Album {r[1]}") for r in raw_rows]
    finally:
        if db_conn:
            db_conn.close()
    if not rows:
        return jsonify({"error": "No incomplete albums to add (or all already sent to Lidarr)", "started": False}), 404
    thread = threading.Thread(target=_run_lidarr_add_incomplete_albums, args=(rows,), daemon=True)
    thread.start()
    return jsonify({"started": True, "total": len(rows)})


@app.get("/api/lidarr/add-incomplete-albums/progress")
def api_lidarr_add_incomplete_albums_progress():
    """Return current add-incomplete-albums-to-Lidarr job progress."""
    with lock:
        prog = state.get("lidarr_add_incomplete")
    if prog is None:
        return jsonify({"running": False, "finished": False})
    return jsonify({
        "running": prog.get("running", False),
        "current": prog.get("current", 0),
        "total": prog.get("total", 0),
        "current_album": prog.get("current_album"),
        "current_artist": prog.get("current_artist"),
        "added": prog.get("added", 0),
        "failed": prog.get("failed", 0),
        "finished": not prog.get("running", True) and prog.get("result") is not None,
        "result": prog.get("result"),
    })


def get_artist_albums(db_conn, artist_id: int) -> List[dict]:
    """Get all albums for an artist from Plex DB (selected sections only — SECTION_IDS)."""
    if not SECTION_IDS:
        return []
    placeholders = ",".join("?" for _ in SECTION_IDS)
    section_filter = f"AND library_section_id IN ({placeholders})"
    section_args = [artist_id] + list(SECTION_IDS)
    cursor = db_conn.execute(
        "SELECT id, title FROM metadata_items WHERE parent_id = ? AND metadata_type = 9 " + section_filter,
        section_args,
    )
    return [{"album_id": row[0], "title": row[1]} for row in cursor.fetchall()]

def add_artist_to_lidarr(artist_id: int, artist_name: str, artist_mbid: str | None = None) -> bool:
    """
    Add an artist to Lidarr with monitoring of missing albums.
    Returns True if successful, False otherwise.
    """
    if not LIDARR_URL or not LIDARR_API_KEY:
        logging.warning("Lidarr not configured (LIDARR_URL or LIDARR_API_KEY missing)")
        return False
    
    try:
        headers = {"X-Api-Key": LIDARR_API_KEY}
        
        # Get existing albums from Plex
        db_conn = plex_connect()
        existing_albums = get_artist_albums(db_conn, artist_id)
        db_conn.close()
        
        # Search for artist in Lidarr
        search_url = f"{LIDARR_URL.rstrip('/')}/api/v1/artist/lookup"
        search_term = f"mbid:{artist_mbid}" if artist_mbid else artist_name
        search_params = {"term": search_term}
        
        response = requests.get(search_url, headers=headers, params=search_params, timeout=10)
        
        if response.status_code != 200:
            logging.error("Lidarr artist search failed: %s", response.text)
            return False
        
        artists = response.json()
        if not artists:
            logging.warning("Artist '%s' not found in Lidarr", artist_name)
            return False
        
        # Use first matching artist
        lidarr_artist = artists[0]
        lidarr_artist_id = lidarr_artist.get('id')
        
        if not lidarr_artist_id:
            logging.warning("Lidarr artist '%s' has no ID", artist_name)
            return False
        
        # Check if artist already exists in Lidarr
        existing_url = f"{LIDARR_URL.rstrip('/')}/api/v1/artist/{lidarr_artist_id}"
        existing_response = requests.get(existing_url, headers=headers, timeout=10)
        
        if existing_response.status_code == 200:
            # Artist exists, update monitoring
            artist_data = existing_response.json()
            artist_data["monitored"] = True
            artist_data["monitor"] = "missing"  # Monitor missing albums
            
            update_url = f"{LIDARR_URL.rstrip('/')}/api/v1/artist"
            update_response = requests.put(update_url, headers=headers, json=artist_data, timeout=10)
            
            if update_response.status_code in (200, 202):
                logging.info("Successfully updated artist '%s' monitoring in Lidarr", artist_name)
                return True
            else:
                logging.error("Failed to update artist in Lidarr: %s", update_response.text)
                return False
        else:
            # Artist doesn't exist, add it
            add_url = f"{LIDARR_URL.rstrip('/')}/api/v1/artist"
            add_payload = {
                **lidarr_artist,
                "monitored": True,
                "monitor": "missing",
                "addOptions": {
                    "monitor": "missing",
                    "searchForMissingAlbums": True
                }
            }
            
            add_response = requests.post(add_url, headers=headers, json=add_payload, timeout=10)
            
            if add_response.status_code in (200, 201):
                logging.info("Successfully added artist '%s' to Lidarr", artist_name)
                return True
            else:
                logging.error("Failed to add artist to Lidarr: %s", add_response.text)
                return False
        
    except requests.exceptions.RequestException as e:
        logging.error("Lidarr API request failed: %s", e)
        return False
    except Exception as e:
        logging.error("Unexpected error adding artist to Lidarr: %s", e, exc_info=True)
        return False

@app.post("/api/lidarr/add-artist")
def api_lidarr_add_artist():
    """Add artist to Lidarr with monitoring of missing albums."""
    data = request.get_json() or {}
    artist_id = data.get("artist_id")
    artist_name = data.get("artist_name")
    artist_mbid = data.get("musicbrainz_artist_id")
    
    if not artist_id or not artist_name:
        return jsonify({"error": "Missing required fields: artist_id, artist_name"}), 400
    
    success = add_artist_to_lidarr(artist_id, artist_name, artist_mbid)
    
    if success:
        # Update database
        import sqlite3
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
        cur = con.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO monitored_artists 
            (artist_id, artist_name, musicbrainz_artist_id, lidarr_artist_id, monitored_at)
            VALUES (?, ?, ?, ?, ?)
        """, (artist_id, artist_name, artist_mbid, None, time.time()))
        con.commit()
        con.close()
        
        return jsonify({"success": True, "message": f"Artist '{artist_name}' added to Lidarr"})
    else:
        return jsonify({"success": False, "message": "Failed to add artist to Lidarr"}), 500

def create_autobrr_filter(artist_names: List[str], quality_preferences: dict | None = None) -> bool:
    """
    Create an Autobrr filter for monitoring artists.
    Returns True if successful, False otherwise.
    """
    if not AUTOBRR_URL or not AUTOBRR_API_KEY:
        logging.warning("Autobrr not configured (AUTOBRR_URL or AUTOBRR_API_KEY missing)")
        return False
    
    try:
        headers = {
            "X-API-Token": AUTOBRR_API_KEY,
            "Content-Type": "application/json"
        }
        
        # Build filter data
        filter_name = f"PMDA - {', '.join(artist_names[:3])}" + ("..." if len(artist_names) > 3 else "")
        artists_str = ",".join(artist_names)
        
        filter_data = {
            "name": filter_name,
            "indexers": [],  # User should configure indexers in Autobrr UI
            "artists": artists_str,
            "match_releases": "",
            "except_releases": "",
            "max_size": "",
            "min_size": "",
            "delay": 0,
            "priority": 0,
            "max_downloads": 0,
            "max_downloads_unit": "HOUR",
            "match_hooks": [],
            "except_hooks": [],
            "actions": []  # User should configure actions in Autobrr UI
        }
        
        # Add quality preferences if provided
        if quality_preferences:
            filter_data.update(quality_preferences)
        
        # Create filter
        create_url = f"{AUTOBRR_URL.rstrip('/')}/api/filters"
        response = requests.post(create_url, headers=headers, json=filter_data, timeout=10)
        
        if response.status_code in (200, 201):
            logging.info("Successfully created Autobrr filter '%s' for %d artists", filter_name, len(artist_names))
            return True
        else:
            logging.error("Failed to create Autobrr filter: %s", response.text)
            return False
            
    except requests.exceptions.RequestException as e:
        logging.error("Autobrr API request failed: %s", e)
        return False
    except Exception as e:
        logging.error("Unexpected error creating Autobrr filter: %s", e, exc_info=True)
        return False

@app.post("/api/autobrr/create-filter")
def api_autobrr_create_filter():
    """Create an Autobrr filter for monitoring artists."""
    data = request.get_json() or {}
    artist_names = data.get("artist_names", [])
    quality_preferences = data.get("quality_preferences")
    
    if not artist_names:
        return jsonify({"error": "Missing required field: artist_names"}), 400
    
    success = create_autobrr_filter(artist_names, quality_preferences)
    
    if success:
        return jsonify({"success": True, "message": f"Autobrr filter created for {len(artist_names)} artist(s)"})
    else:
        return jsonify({"success": False, "message": "Failed to create Autobrr filter"}), 500

@app.post("/api/lidarr/test")
def api_lidarr_test():
    """Test Lidarr connection."""
    data = request.get_json() or {}
    url = data.get("url", "").strip().rstrip("/")
    api_key = data.get("api_key", "").strip()
    
    if not url or not api_key:
        return jsonify({"success": False, "message": "Lidarr URL and API Key are required"}), 400
    
    try:
        headers = {"X-Api-Key": api_key}
        # Test with system status endpoint
        test_url = f"{url}/api/v1/system/status"
        response = requests.get(test_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            status_data = response.json()
            version = status_data.get("version", "unknown")
            return jsonify({
                "success": True,
                "message": f"Lidarr connection successful (version {version})"
            })
        elif response.status_code == 401:
            return jsonify({
                "success": False,
                "message": "Authentication failed. Please check your API key."
            }), 401
        else:
            return jsonify({
                "success": False,
                "message": f"Lidarr returned status {response.status_code}: {response.text[:200]}"
            }), response.status_code
    except requests.exceptions.ConnectionError:
        return jsonify({
            "success": False,
            "message": f"Could not connect to Lidarr at {url}. Check if Lidarr is running and the URL is correct."
        }), 503
    except requests.exceptions.Timeout:
        return jsonify({
            "success": False,
            "message": "Connection to Lidarr timed out. Check your network connection."
        }), 504
    except Exception as e:
        logging.exception("Lidarr test failed")
        return jsonify({
            "success": False,
            "message": f"Unexpected error: {str(e)}"
        }), 500

@app.post("/api/autobrr/test")
def api_autobrr_test():
    """Test Autobrr connection."""
    data = request.get_json() or {}
    url = data.get("url", "").strip().rstrip("/")
    api_key = data.get("api_key", "").strip()
    
    if not url or not api_key:
        return jsonify({"success": False, "message": "Autobrr URL and API Key are required"}), 400
    
    try:
        headers = {"X-API-Token": api_key}
        # Test with health check endpoint
        test_url = f"{url}/api/healthz/liveness"
        response = requests.get(test_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            return jsonify({
                "success": True,
                "message": "Autobrr connection successful"
            })
        elif response.status_code == 401:
            return jsonify({
                "success": False,
                "message": "Authentication failed. Please check your API key."
            }), 401
        else:
            # Try alternative endpoint (config)
            config_url = f"{url}/api/config"
            config_response = requests.get(config_url, headers=headers, timeout=10)
            if config_response.status_code == 200:
                return jsonify({
                    "success": True,
                    "message": "Autobrr connection successful"
                })
            else:
                return jsonify({
                    "success": False,
                    "message": f"Autobrr returned status {config_response.status_code}: {config_response.text[:200]}"
                }), config_response.status_code
    except requests.exceptions.ConnectionError:
        return jsonify({
            "success": False,
            "message": f"Could not connect to Autobrr at {url}. Check if Autobrr is running and the URL is correct."
        }), 503
    except requests.exceptions.Timeout:
        return jsonify({
            "success": False,
            "message": "Connection to Autobrr timed out. Check your network connection."
        }), 504
    except Exception as e:
        logging.exception("Autobrr test failed")
        return jsonify({
            "success": False,
            "message": f"Unexpected error: {str(e)}"
        }), 500

def get_similar_artists_mb(artist_mbid: str) -> List[dict]:
    """Get similar artists from MusicBrainz using relations and tags."""
    if not USE_MUSICBRAINZ:
        return []
    
    similar = []
    
    try:
        # Get artist relations
        result = musicbrainzngs.get_artist_by_id(
            artist_mbid,
            includes=["artist-rels", "tags"]
        )
        artist_data = result.get("artist", {})
        relations = artist_data.get("artist-relation-list", [])
        
        # Add artists from relations
        for rel in relations:
            rel_type = rel.get("type", "")
            if rel_type in ["similar to", "influenced by", "collaboration", "member of", "founded"]:
                target_artist = rel.get("artist", {})
                if target_artist:
                    similar.append({
                        "name": target_artist.get("name", ""),
                        "mbid": target_artist.get("id", ""),
                        "type": rel_type
                    })
        
        # Also search by tags/genres for additional similar artists
        tags = artist_data.get("tag-list", [])
        if tags:
            # Get top tags
            top_tags = sorted(tags, key=lambda t: int(t.get("count", 0)), reverse=True)[:3]
            for tag_info in top_tags:
                tag_name = tag_info.get("name", "")
                if tag_name:
                    try:
                        # Search for artists with similar tags
                        search_result = musicbrainzngs.search_artists(tag=tag_name, limit=10)
                        artist_list = search_result.get("artist-list", [])
                        for artist in artist_list:
                            if artist.get("id") != artist_mbid:  # Don't include self
                                # Check if not already in similar list
                                if not any(s.get("mbid") == artist.get("id") for s in similar):
                                    similar.append({
                                        "name": artist.get("name", ""),
                                        "mbid": artist.get("id", ""),
                                        "type": f"tag: {tag_name}"
                                    })
                                if len(similar) >= 20:  # Limit total results
                                    break
                        if len(similar) >= 20:
                            break
                    except Exception:
                        continue
        
        # Remove duplicates and limit
        seen = set()
        unique_similar = []
        for s in similar:
            if s["mbid"] not in seen:
                seen.add(s["mbid"])
                unique_similar.append(s)
                if len(unique_similar) >= 15:
                    break
        
        return unique_similar
    except Exception as e:
        logging.error("Failed to get similar artists for MBID %s: %s", artist_mbid, e)
        return []

@app.get("/api/library/artist/<int:artist_id>/similar")
def api_library_artist_similar(artist_id):
    """Get similar artists for a given artist via MusicBrainz."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    
    if not USE_MUSICBRAINZ:
        return jsonify({"error": "MusicBrainz not enabled"}), 400
    
    db_conn = plex_connect()
    
    # Get artist name
    artist_row = db_conn.execute(
        "SELECT title FROM metadata_items WHERE id = ? AND metadata_type = 8",
        (artist_id,)
    ).fetchone()
    
    if not artist_row:
        db_conn.close()
        return jsonify({"error": "Artist not found"}), 404
    
    artist_name = artist_row[0]
    
    # Try to find MusicBrainz ID from artist's albums
    # Look for musicbrainz_albumartistid in any album's first track
    mbid = None
    album_rows = db_conn.execute("""
        SELECT alb.id
        FROM metadata_items alb
        WHERE alb.parent_id = ? AND alb.metadata_type = 9
        LIMIT 1
    """, (artist_id,)).fetchall()
    
    if album_rows:
        album_id = album_rows[0][0]
        # Get first track
        track_rows = db_conn.execute("""
            SELECT tr.id
            FROM metadata_items tr
            WHERE tr.parent_id = ? AND tr.metadata_type = 10
            LIMIT 1
        """, (album_id,)).fetchall()
        
        if track_rows:
            track_id = track_rows[0][0]
            # Get file path for this track
            file_rows = db_conn.execute("""
                SELECT mp.file
                FROM media_items mi
                JOIN media_parts mp ON mp.media_item_id = mi.id
                WHERE mi.metadata_item_id = ?
                LIMIT 1
            """, (track_id,)).fetchall()
            
            if file_rows:
                file_path = file_rows[0][0]
                # Try to extract MBID from file
                try:
                    folder = first_part_path(db_conn, album_id)
                    if folder:
                        first_audio = next((p for p in folder.rglob("*") if AUDIO_RE.search(p.name)), None)
                        if first_audio:
                            meta = extract_tags(first_audio)
                            mbid = meta.get('musicbrainz_albumartistid') or meta.get('musicbrainz_artistid')
                except Exception:
                    pass
    
    db_conn.close()
    
    if not mbid:
        # Try to search MusicBrainz by artist name
        try:
            search_result = musicbrainzngs.search_artists(artist=artist_name, limit=1)
            if search_result.get("artist-list"):
                mbid = search_result["artist-list"][0]["id"]
        except Exception as e:
            logging.warning("Failed to search MusicBrainz for artist '%s': %s", artist_name, e)
            return jsonify({"error": "Could not find MusicBrainz ID for artist"}), 404
    
    if not mbid:
        return jsonify({"error": "Could not find MusicBrainz ID for artist"}), 404
    
    similar = get_similar_artists_mb(mbid)
    # Partie 4.2: Last.fm fallback for similar artists
    if USE_LASTFM and LASTFM_API_KEY.strip():
        try:
            params = {"method": "artist.getSimilar", "artist": artist_name, "api_key": LASTFM_API_KEY, "format": "json"}
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                lf_artists = data.get("similarartists", {}).get("artist") or []
                if isinstance(lf_artists, dict):
                    lf_artists = [lf_artists]
                seen_mbids = {s.get("mbid") for s in similar if s.get("mbid")}
                seen_names = {s.get("name", "").lower() for s in similar}
                for a in lf_artists[:15]:
                    name = (a.get("name") or "").strip()
                    lf_mbid = (a.get("mbid") or "").strip()
                    if not name:
                        continue
                    if lf_mbid and lf_mbid in seen_mbids:
                        continue
                    if name.lower() in seen_names:
                        continue
                    similar.append({"name": name, "mbid": lf_mbid or "", "type": "Last.fm"})
                    if lf_mbid:
                        seen_mbids.add(lf_mbid)
                    seen_names.add(name.lower())
                    if len(similar) >= 20:
                        break
        except Exception as e:
            logging.debug("[Similar artists] Last.fm fallback failed: %s", e)
    return jsonify({
        "artist_mbid": mbid,
        "similar_artists": similar[:20]
    })

@app.get("/api/library/release-group/<mbid>/labels")
def api_library_release_group_labels(mbid):
    """Get labels for a MusicBrainz release-group (from first release)."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    if not USE_MUSICBRAINZ:
        return jsonify({"error": "MusicBrainz not enabled"}), 400
    mbid = (mbid or "").strip()
    if not mbid or len(mbid) != 36:
        return jsonify({"error": "Invalid release-group MBID"}), 400
    try:
        if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
            rg_data = get_mb_queue().submit(f"rg_labels_{mbid}", lambda: musicbrainzngs.get_release_group_by_id(mbid, includes=["releases"]))
        else:
            rg_data = musicbrainzngs.get_release_group_by_id(mbid, includes=["releases"])
        rg = rg_data.get("release-group", {})
        releases = rg.get("release-list") or []
        if not releases:
            return jsonify({"labels": []})
        first_release_id = releases[0].get("id")
        if not first_release_id:
            return jsonify({"labels": []})
        if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
            rel_data = get_mb_queue().submit(f"rel_labels_{first_release_id}", lambda: musicbrainzngs.get_release_by_id(first_release_id, includes=["labels"]))
        else:
            rel_data = musicbrainzngs.get_release_by_id(first_release_id, includes=["labels"])
        release = rel_data.get("release", {})
        label_list = release.get("label-list") or []
        labels = []
        for lb in label_list:
            label = lb.get("label", {}) if isinstance(lb.get("label"), dict) else {}
            name = label.get("name") or lb.get("name") or ""
            lid = label.get("id") or lb.get("id") or ""
            if name or lid:
                labels.append({"name": name, "id": lid})
        return jsonify({"labels": labels})
    except musicbrainzngs.WebServiceError as e:
        if "404" in str(e):
            return jsonify({"error": "Release group not found"}), 404
        raise
    except Exception as e:
        logging.exception("Failed to get labels for release-group %s", mbid)
        return jsonify({"error": str(e)}), 500

@app.get("/api/library/artist/<int:artist_id>/monitored")
def api_library_artist_monitored(artist_id):
    """Check if an artist is monitored in Lidarr."""
    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM monitored_artists WHERE artist_id = ?", (artist_id,))
    is_monitored = cur.fetchone() is not None
    con.close()
    return jsonify({"monitored": is_monitored})

def get_artist_images_mb(artist_mbid: str) -> List[str]:
    """Get artist images from MusicBrainz/Wikimedia."""
    if not USE_MUSICBRAINZ:
        return []
    
    try:
        result = musicbrainzngs.get_artist_by_id(
            artist_mbid,
            includes=["url-rels"]
        )
        image_urls = []
        artist_data = result.get("artist", {})
        url_relations = artist_data.get("url-relation-list", [])
        
        for url_rel in url_relations:
            target = url_rel.get("target", "")
            if "wikimedia" in target.lower() or "commons.wikimedia" in target.lower():
                image_urls.append(target)
        
        return image_urls
    except Exception as e:
        logging.error("Failed to get artist images for MBID %s: %s", artist_mbid, e)
        return []


def _artist_folder_has_image(artist_folder: Path) -> bool:
    """Return True if the artist folder already has an artist/folder image file."""
    if not artist_folder or not artist_folder.is_dir():
        return False
    names = ("artist.jpg", "artist.jpeg", "artist.png", "folder.jpg", "folder.jpeg", "folder.png")
    return any((artist_folder / n).is_file() for n in names)


def _fetch_and_save_artist_image_mb(artist_mbid: str, artist_folder: Path) -> bool:
    """Fetch first artist image from MusicBrainz/Wikimedia and save to artist_folder/artist.jpg. Returns True if saved."""
    urls = get_artist_images_mb(artist_mbid)
    for url in urls:
        try:
            resp = requests.get(url, timeout=10, allow_redirects=True)
            if resp.status_code != 200:
                continue
            ct = resp.headers.get("content-type", "").lower()
            if "image/" in ct:
                ext = ".jpg"
                if "png" in ct:
                    ext = ".png"
                out = artist_folder / f"artist{ext}"
                out.write_bytes(resp.content)
                logging.info("Saved artist image from MusicBrainz to %s", out)
                return True
            # If HTML (e.g. Commons page), try og:image
            if "text/html" in ct:
                m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', resp.text)
                if m:
                    img_url = m.group(1).strip()
                    img_resp = requests.get(img_url, timeout=10, allow_redirects=True)
                    if img_resp.status_code == 200 and "image/" in img_resp.headers.get("content-type", "").lower():
                        (artist_folder / "artist.jpg").write_bytes(img_resp.content)
                        logging.info("Saved artist image from Wikimedia to %s", artist_folder / "artist.jpg")
                        return True
        except Exception as e:
            logging.warning("Artist image fetch (MB) failed for %s: %s", url, e)
    return False


def _fetch_artist_image_lastfm(artist_name: str) -> Optional[str]:
    """Get largest artist image URL from Last.fm artist.getInfo. Returns URL or None."""
    if not USE_LASTFM:
        return None
    api_key = (getattr(sys.modules[__name__], "LASTFM_API_KEY", "") or "").strip()
    if not api_key:
        return None
    try:
        resp = requests.get(
            "https://ws.audioscrobbler.com/2.0/",
            params={"method": "artist.getInfo", "artist": artist_name, "api_key": api_key, "format": "json"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data.get("error"):
            return None
        artist = data.get("artist", {})
        images = artist.get("image") or []
        url = None
        for img in images:
            if isinstance(img, dict) and img.get("#text"):
                url = img.get("#text")
                if img.get("size") == "extralarge":
                    return url
        return url
    except Exception as e:
        logging.warning("Last.fm artist.getInfo failed for %s: %s", artist_name, e)
        return None


def _fetch_artist_image_discogs(artist_name: str) -> Optional[str]:
    """Get artist image URL from Discogs (search artist, first result). Returns URL or None."""
    if not USE_DISCOGS:
        return None
    token = (getattr(sys.modules[__name__], "DISCOGS_USER_TOKEN", "") or "").strip()
    if not token:
        return None
    try:
        import discogs_client
        d = discogs_client.Client("PMDA/0.6.6", user_token=token)
        results = d.search(artist_name, type="artist")
        page = results.page(1)
        if not page:
            return None
        artist = page[0]
        artist_id = getattr(artist, "id", None) or (artist.data.get("id") if getattr(artist, "data", None) else None)
        if not artist_id:
            return None
        full_artist = d.artist(artist_id)
        images = getattr(full_artist, "images", None)
        if images and len(images) > 0:
            img = images[0]
            return img.get("uri") or img.get("resource_url")
        return None
    except Exception as e:
        logging.warning("Discogs artist image fetch failed for %s: %s", artist_name, e)
        return None


def _fetch_and_save_artist_image(artist_name: str, artist_folder: Path, artist_mbid: Optional[str] = None) -> bool:
    """
    Fetch artist image from MB, then Last.fm, then Discogs and save to artist_folder (artist.jpg).
    Skips if artist_folder already has an artist/folder image. Returns True if saved.
    """
    if not artist_folder or not artist_folder.is_dir():
        return False
    if _artist_folder_has_image(artist_folder):
        return False
    if not USE_MUSICBRAINZ and not USE_LASTFM and not USE_DISCOGS:
        return False
    # Try MusicBrainz first (if we have MBID)
    if artist_mbid and USE_MUSICBRAINZ:
        if _fetch_and_save_artist_image_mb(artist_mbid, artist_folder):
            return True
    # Last.fm
    if USE_LASTFM:
        url = _fetch_artist_image_lastfm(artist_name)
        if url:
            try:
                resp = requests.get(url, timeout=10, allow_redirects=True)
                if resp.status_code == 200 and resp.content:
                    (artist_folder / "artist.jpg").write_bytes(resp.content)
                    logging.info("Saved artist image from Last.fm to %s", artist_folder / "artist.jpg")
                    return True
            except Exception as e:
                logging.warning("Artist image download (Last.fm) failed: %s", e)
    # Discogs
    if USE_DISCOGS:
        url = _fetch_artist_image_discogs(artist_name)
        if url:
            try:
                resp = requests.get(url, timeout=10, allow_redirects=True)
                if resp.status_code == 200 and resp.content:
                    (artist_folder / "artist.jpg").write_bytes(resp.content)
                    logging.info("Saved artist image from Discogs to %s", artist_folder / "artist.jpg")
                    return True
            except Exception as e:
                logging.warning("Artist image download (Discogs) failed: %s", e)
    return False

@app.get("/api/library/artist/<int:artist_id>/images")
def api_library_artist_images(artist_id):
    """Get artist images from MusicBrainz/Wikimedia."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    
    if not USE_MUSICBRAINZ:
        return jsonify({"error": "MusicBrainz not enabled"}), 400
    
    db_conn = plex_connect()
    
    # Get artist name
    artist_row = db_conn.execute(
        "SELECT title FROM metadata_items WHERE id = ? AND metadata_type = 8",
        (artist_id,)
    ).fetchone()
    
    if not artist_row:
        db_conn.close()
        return jsonify({"error": "Artist not found"}), 404
    
    artist_name = artist_row[0]
    
    # Try to find MusicBrainz ID (same logic as similar artists)
    mbid = None
    album_rows = db_conn.execute("""
        SELECT alb.id
        FROM metadata_items alb
        WHERE alb.parent_id = ? AND alb.metadata_type = 9
        LIMIT 1
    """, (artist_id,)).fetchall()
    
    if album_rows:
        album_id = album_rows[0][0]
        folder = first_part_path(db_conn, album_id)
        if folder:
            first_audio = next((p for p in folder.rglob("*") if AUDIO_RE.search(p.name)), None)
            if first_audio:
                meta = extract_tags(first_audio)
                mbid = meta.get('musicbrainz_albumartistid') or meta.get('musicbrainz_artistid')
    
    db_conn.close()
    
    if not mbid:
        # Try to search MusicBrainz by artist name
        try:
            search_result = musicbrainzngs.search_artists(artist=artist_name, limit=1)
            if search_result.get("artist-list"):
                mbid = search_result["artist-list"][0]["id"]
        except Exception as e:
            logging.warning("Failed to search MusicBrainz for artist '%s': %s", artist_name, e)
            return jsonify({"error": "Could not find MusicBrainz ID for artist"}), 404
    
    if not mbid:
        return jsonify({"error": "Could not find MusicBrainz ID for artist"}), 404
    
    image_urls = get_artist_images_mb(mbid)
    return jsonify({
        "artist_mbid": mbid,
        "images": image_urls
    })

@app.get("/api/library/album/<int:album_id>/tags")
def api_library_album_tags(album_id):
    """Get current tags and MusicBrainz info for an album."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    
    db_conn = plex_connect()
    
    # Get album info
    album_row = db_conn.execute(
        "SELECT id, title, parent_id FROM metadata_items WHERE id = ? AND metadata_type = 9",
        (album_id,)
    ).fetchone()
    
    if not album_row:
        db_conn.close()
        return jsonify({"error": "Album not found"}), 404
    
    album_title_str = album_row[1]
    artist_id = album_row[2]
    
    # Get artist name
    artist_row = db_conn.execute(
        "SELECT title FROM metadata_items WHERE id = ? AND metadata_type = 8",
        (artist_id,)
    ).fetchone()
    artist_name = artist_row[0] if artist_row else "Unknown"
    
    # Get folder path
    folder = first_part_path(db_conn, album_id)
    if not folder:
        db_conn.close()
        return jsonify({"error": "Album folder not found"}), 404
    
    # Get tags from first audio file
    first_audio = next((p for p in folder.rglob("*") if AUDIO_RE.search(p.name)), None)
    current_tags = extract_tags(first_audio) if first_audio else {}
    
    # Try to find MusicBrainz release-group info
    mb_info = None
    mbid = current_tags.get('musicbrainz_releasegroupid') or current_tags.get('musicbrainz_releaseid')
    
    if mbid and USE_MUSICBRAINZ:
        try:
            if current_tags.get('musicbrainz_releasegroupid'):
                mb_info, _ = fetch_mb_release_group_info(mbid)
            else:
                # Get release-group from release
                def _fetch_release():
                    return musicbrainzngs.get_release_by_id(mbid, includes=["release-groups"])["release"]
                
                if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
                    rel = get_mb_queue().submit(f"rel_{mbid}", _fetch_release)
                else:
                    rel = _fetch_release()
                rgid = rel['release-group']['id']
                mb_info, _ = fetch_mb_release_group_info(rgid)
                mbid = rgid
        except Exception as e:
            logging.warning("Failed to fetch MB info for album %s: %s", album_id, e)
    
    db_conn.close()
    
    return jsonify({
        "album_id": album_id,
        "album_title": album_title_str,
        "artist_name": artist_name,
        "current_tags": current_tags,
        "musicbrainz_id": mbid,
        "musicbrainz_info": mb_info,
        "folder": str(folder)
    })


def _embed_cover_in_audio_files(cover_path: Path, audio_files: list) -> None:
    """
    Embed the cover image from cover_path into all audio files (MP3, FLAC, MP4).
    Logs errors per file but does not raise.
    """
    if not cover_path or not cover_path.is_file():
        return
    try:
        from mutagen import File as MutagenFile
        from mutagen.id3 import ID3, APIC
        from mutagen.mp3 import MP3
        from mutagen.flac import FLAC, Picture
        from mutagen.mp4 import MP4, MP4Cover
    except ImportError:
        logging.warning("improve-album: mutagen not available for embedding cover")
        return
    data = cover_path.read_bytes()
    suffix = cover_path.suffix.lower()
    if suffix in (".png",):
        mime = "image/png"
        mp4_fmt = MP4Cover.FORMAT_PNG
    else:
        mime = "image/jpeg"
        mp4_fmt = MP4Cover.FORMAT_JPEG
    for audio_file in audio_files:
        try:
            audio = MutagenFile(str(audio_file))
            if audio is None:
                continue
            if isinstance(audio, MP3):
                if audio.tags is None:
                    audio.add_tags(ID3())
                audio.tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=data))
            elif isinstance(audio, FLAC):
                pic = Picture()
                pic.type = 3
                pic.mime = mime
                pic.desc = "front cover"
                pic.data = data
                audio.add_picture(pic)
            elif isinstance(audio, MP4):
                if audio.tags is None:
                    from mutagen.mp4 import MP4Tags
                    audio.add_tags(MP4Tags())
                audio.tags["covr"] = [MP4Cover(data, mp4_fmt)]
            else:
                continue
            audio.save()
        except Exception as e:
            logging.error("improve-album: embed cover failed for %s: %s", audio_file, e)


def _improve_single_album(album_id: int, db_conn, known_release_group_id: Optional[str] = None) -> dict:
    """
    Improve one album: resolve MusicBrainz ID (from known_release_group_id, tags, or search), update tags on all audio files, fetch cover if missing.
    known_release_group_id: optional MBID from last scan (scan_editions); when set, used instead of tags/search.
    Fallback: Discogs then Last.fm when MB does not provide tags or cover.
    Returns dict with steps (list of str), summary (str), tags_updated (bool), cover_saved (bool), provider_used (str|None).
    """
    steps: List[str] = []
    tags_updated = False
    cover_saved = False
    provider_used: Optional[str] = None

    album_row = db_conn.execute(
        "SELECT id, title, parent_id FROM metadata_items WHERE id = ? AND metadata_type = 9",
        (album_id,)
    ).fetchone()
    if not album_row:
        return {"steps": ["Album not found"], "summary": "Album not found.", "tags_updated": False, "cover_saved": False, "provider_used": None}

    album_title_str = album_row[1]
    artist_id = album_row[2]
    artist_row = db_conn.execute(
        "SELECT title FROM metadata_items WHERE id = ? AND metadata_type = 8",
        (artist_id,)
    ).fetchone()
    artist_name = artist_row[0] if artist_row else "Unknown"

    folder = first_part_path(db_conn, album_id)
    if not folder:
        return {"steps": ["Album folder not found"], "summary": "Album folder not found.", "tags_updated": False, "cover_saved": False, "provider_used": None}

    audio_files = [p for p in folder.rglob("*") if AUDIO_RE.search(p.name)]
    if not audio_files:
        return {"steps": ["No audio files in album folder"], "summary": "No audio files found.", "tags_updated": False, "cover_saved": False, "provider_used": None}

    first_audio = audio_files[0]
    current_tags = extract_tags(first_audio)
    release_mbid = known_release_group_id or current_tags.get("musicbrainz_releasegroupid") or current_tags.get("musicbrainz_releaseid")

    if not release_mbid and USE_MUSICBRAINZ:
        album_norm = norm_album(album_title_str)
        tracks = set()
        try:
            for p in audio_files[:20]:
                meta = extract_tags(p)
                t = (meta.get("title") or meta.get("TIT2") or "").strip()
                if t:
                    tracks.add(t)
        except Exception:
            pass
        rg_info, _verified_by_ai = search_mb_release_group_by_metadata(artist_name, album_norm, tracks, title_raw=album_title_str)
        if rg_info and isinstance(rg_info.get("id"), str):
            release_mbid = rg_info["id"]
            steps.append("Found MusicBrainz release group via search")
    if release_mbid:
        if known_release_group_id:
            steps.append("Using MusicBrainz ID from scan")
        elif not steps:
            steps.append("Using existing MusicBrainz ID")

    try:
        from mutagen import File as MutagenFile
        from mutagen.id3 import ID3, TPE1, TALB, TDRC
        from mutagen.mp3 import MP3
        from mutagen.flac import FLAC
        from mutagen.mp4 import MP4
        HAS_MUTAGEN = True
    except ImportError:
        HAS_MUTAGEN = False

    if not HAS_MUTAGEN:
        return {
            "steps": steps + ["Mutagen not installed"],
            "summary": "Cannot update tags: mutagen library not installed.",
            "tags_updated": False,
            "cover_saved": cover_saved,
            "provider_used": None,
        }

    mb_release_info = None
    if release_mbid:
        release_group_id = release_mbid
        tag_src = "musicbrainz_releasegroupid" if current_tags.get("musicbrainz_releasegroupid") else ("musicbrainz_releaseid" if current_tags.get("musicbrainz_releaseid") else "")
        if tag_src:
            resolved = resolve_mbid_to_release_group(release_mbid, tag_src)
            if resolved:
                release_group_id = resolved
        try:
            result = musicbrainzngs.get_release_group_by_id(release_group_id, includes=["releases"])
            mb_release_info = result.get("release-group", {})
        except Exception as e:
            logging.warning("improve-album: failed to fetch release group %s: %s", release_group_id, e)
            steps.append(f"MusicBrainz lookup failed: {e}")

    artist_mbid = current_tags.get("musicbrainz_albumartistid") or current_tags.get("musicbrainz_artistid")
    if not artist_mbid and USE_MUSICBRAINZ:
        try:
            search_result = musicbrainzngs.search_artists(artist=artist_name, limit=1)
            if search_result.get("artist-list"):
                artist_mbid = search_result["artist-list"][0]["id"]
        except Exception as e:
            logging.warning("improve-album: artist search failed for '%s': %s", artist_name, e)

    # Log what we retrieved and from where (for fix-all visibility)
    if mb_release_info and artist_mbid:
        date_str = mb_release_info.get("first-release-date", "")
        year_str = date_str.split("-")[0] if (date_str and "-" in date_str) else (date_str or "")
        logging.info(
            "Fix-all [album_id=%s] source=MusicBrainz tags: ARTIST=%s ALBUM=%s DATE=%s MUSICBRAINZ_ARTISTID=%s MUSICBRAINZ_RELEASEGROUPID=%s -> injecting into %d file(s) in %s",
            album_id, artist_name, mb_release_info.get("title", album_title_str), year_str or "(none)",
            artist_mbid, release_mbid or "(none)", len(audio_files), folder,
        )

    files_updated = 0
    for audio_file in audio_files:
        try:
            audio = MutagenFile(str(audio_file))
            if audio is None:
                continue
            if mb_release_info and artist_mbid:
                if isinstance(audio, (MP3, ID3)):
                    audio.tags.add(TPE1(encoding=3, text=artist_name))
                    audio.tags.add(TALB(encoding=3, text=mb_release_info.get("title", album_title_str)))
                    date_str = mb_release_info.get("first-release-date", "")
                    if date_str:
                        year = date_str.split("-")[0] if "-" in date_str else date_str
                        audio.tags.add(TDRC(encoding=3, text=year))
                elif isinstance(audio, FLAC):
                    audio["ARTIST"] = artist_name
                    audio["ALBUM"] = mb_release_info.get("title", album_title_str)
                    date_str = mb_release_info.get("first-release-date", "")
                    if date_str:
                        audio["DATE"] = date_str.split("-")[0] if "-" in date_str else date_str
                    audio["MUSICBRAINZ_ARTISTID"] = artist_mbid
                    audio["MUSICBRAINZ_ALBUMARTISTID"] = artist_mbid
                    if release_mbid:
                        audio["MUSICBRAINZ_RELEASEGROUPID"] = release_mbid
                elif isinstance(audio, MP4):
                    audio["\xa9ART"] = [artist_name]
                    audio["\xa9alb"] = [mb_release_info.get("title", album_title_str)]
                    date_str = mb_release_info.get("first-release-date", "")
                    if date_str:
                        audio["\xa9day"] = [date_str.split("-")[0] if "-" in date_str else date_str]
                    audio["----:com.apple.iTunes:MusicBrainz Artist Id"] = [artist_mbid.encode("utf-8")]
                    audio["----:com.apple.iTunes:MusicBrainz Album Artist Id"] = [artist_mbid.encode("utf-8")]
                    if release_mbid:
                        audio["----:com.apple.iTunes:MusicBrainz Release Group Id"] = [release_mbid.encode("utf-8")]
                audio.save()
                files_updated += 1
                logging.info("Fix-all: injected MusicBrainz tags into %s", audio_file)
        except Exception as e:
            logging.error("improve-album: error updating %s: %s", audio_file, e)
            steps.append(f"Error updating {audio_file.name}: {e}")

    if files_updated > 0:
        tags_updated = True
        steps.append(f"Updated tags on {files_updated} file(s)")

    has_cover = any(
        f.name.lower().startswith(("cover", "folder", "album", "artwork", "front"))
        and f.suffix.lower() in [".jpg", ".jpeg", ".png", ".webp"]
        for f in folder.iterdir() if f.is_file()
    )
    if not has_cover and release_mbid:
        try:
            cover_url = f"http://coverartarchive.org/release-group/{release_mbid}/front"
            cover_resp = requests.get(cover_url, timeout=5, allow_redirects=True)
            if cover_resp.status_code == 200:
                cover_path = folder / "cover.jpg"
                with open(cover_path, "wb") as f:
                    f.write(cover_resp.content)
                cover_saved = True
                logging.info("Fix-all [album_id=%s] source=Cover Art Archive saved cover to %s", album_id, cover_path)
                steps.append("Fetched and saved cover art")
                _embed_cover_in_audio_files(cover_path, audio_files)
        except Exception as e:
            logging.warning("improve-album: cover fetch failed: %s", e)
            steps.append("Cover fetch failed")

    if tags_updated or cover_saved:
        provider_used = "musicbrainz"

    def _apply_fallback_tags(artist_str: str, album_str: str, year_str: str, source: str = "fallback") -> int:
        """Write artist, album, date to all audio files. Returns number of files updated."""
        logging.info(
            "Fix-all [album_id=%s] source=%s tags: ARTIST=%s ALBUM=%s DATE=%s -> injecting into %d file(s) in %s",
            album_id, source, artist_str, album_str, year_str or "(none)", len(audio_files), folder,
        )
        count = 0
        for audio_file in audio_files:
            try:
                audio = MutagenFile(str(audio_file))
                if audio is None:
                    continue
                if isinstance(audio, (MP3, ID3)):
                    audio.tags.add(TPE1(encoding=3, text=artist_str))
                    audio.tags.add(TALB(encoding=3, text=album_str))
                    if year_str:
                        audio.tags.add(TDRC(encoding=3, text=year_str))
                elif isinstance(audio, FLAC):
                    audio["ARTIST"] = artist_str
                    audio["ALBUM"] = album_str
                    if year_str:
                        audio["DATE"] = year_str
                elif isinstance(audio, MP4):
                    audio["\xa9ART"] = [artist_str]
                    audio["\xa9alb"] = [album_str]
                    if year_str:
                        audio["\xa9day"] = [year_str]
                audio.save()
                count += 1
                logging.info("Fix-all: injected %s tags into %s", source, audio_file)
            except Exception as e:
                logging.error("improve-album: fallback tag error %s: %s", audio_file, e)
        return count

    # Fallback: Discogs then Last.fm when MB did not provide tags or cover
    has_cover_now = has_cover or cover_saved
    if (not tags_updated or not has_cover_now) and not provider_used and USE_DISCOGS:
        discogs_info = _fetch_discogs_release(artist_name, album_title_str)
        if discogs_info:
            artist_str = discogs_info.get("artist_name") or artist_name
            album_str = discogs_info.get("title") or album_title_str
            year_str = (discogs_info.get("year") or "").strip()
            if not tags_updated and (album_str or artist_str):
                n = _apply_fallback_tags(artist_str, album_str, year_str, "Discogs")
                if n > 0:
                    tags_updated = True
                    files_updated = n
                    steps.append(f"Updated tags from Discogs on {n} file(s)")
            if not has_cover_now and discogs_info.get("cover_url"):
                try:
                    cover_resp = requests.get(discogs_info["cover_url"], timeout=10, allow_redirects=True)
                    if cover_resp.status_code == 200:
                        cover_path = folder / "cover.jpg"
                        with open(cover_path, "wb") as f:
                            f.write(cover_resp.content)
                        cover_saved = True
                        has_cover_now = True
                        logging.info("Fix-all [album_id=%s] source=Discogs saved cover to %s", album_id, cover_path)
                        steps.append("Fetched and saved cover art from Discogs")
                        _embed_cover_in_audio_files(cover_path, audio_files)
                except Exception as e:
                    logging.warning("improve-album: Discogs cover fetch failed: %s", e)
            if tags_updated or cover_saved:
                provider_used = "discogs"

    if (not tags_updated or not has_cover_now) and not provider_used and USE_LASTFM:
        lastfm_info = _fetch_lastfm_album_info(artist_name, album_title_str, release_mbid)
        if lastfm_info:
            artist_str = lastfm_info.get("artist") or artist_name
            album_str = lastfm_info.get("title") or album_title_str
            year_str = ""
            if not tags_updated and (album_str or artist_str):
                n = _apply_fallback_tags(artist_str, album_str, year_str, "Last.fm")
                if n > 0:
                    tags_updated = True
                    files_updated = n
                    steps.append(f"Updated tags from Last.fm on {n} file(s)")
            if not has_cover_now and lastfm_info.get("cover_url"):
                try:
                    cover_resp = requests.get(lastfm_info["cover_url"], timeout=10, allow_redirects=True)
                    if cover_resp.status_code == 200:
                        cover_path = folder / "cover.jpg"
                        with open(cover_path, "wb") as f:
                            f.write(cover_resp.content)
                        cover_saved = True
                        has_cover_now = True
                        logging.info("Fix-all [album_id=%s] source=Last.fm saved cover to %s", album_id, cover_path)
                        steps.append("Fetched and saved cover art from Last.fm")
                        _embed_cover_in_audio_files(cover_path, audio_files)
                except Exception as e:
                    logging.warning("improve-album: Last.fm cover fetch failed: %s", e)
            if tags_updated or cover_saved:
                provider_used = "lastfm"

    # Bandcamp: ultimate fallback when still missing tags or cover
    if (not tags_updated or not has_cover_now) and not provider_used and USE_BANDCAMP:
        bandcamp_info = _fetch_bandcamp_album_info(artist_name, album_title_str)
        if bandcamp_info:
            artist_str = bandcamp_info.get("artist_name") or artist_name
            album_str = bandcamp_info.get("title") or album_title_str
            year_str = (bandcamp_info.get("year") or "").strip()
            if not tags_updated and (album_str or artist_str):
                n = _apply_fallback_tags(artist_str, album_str, year_str, "Bandcamp")
                if n > 0:
                    tags_updated = True
                    files_updated = n
                    steps.append(f"Updated tags from Bandcamp on {n} file(s)")
            if not has_cover_now and bandcamp_info.get("cover_url"):
                try:
                    cover_resp = requests.get(bandcamp_info["cover_url"], headers={"User-Agent": "PMDA/1.0 (metadata fallback)"}, timeout=10, allow_redirects=True)
                    if cover_resp.status_code == 200:
                        cover_path = folder / "cover.jpg"
                        with open(cover_path, "wb") as f:
                            f.write(cover_resp.content)
                        cover_saved = True
                        logging.info("Fix-all [album_id=%s] source=Bandcamp saved cover to %s", album_id, cover_path)
                        steps.append("Fetched and saved cover art from Bandcamp")
                        _embed_cover_in_audio_files(cover_path, audio_files)
                except Exception as e:
                    logging.warning("improve-album: Bandcamp cover fetch failed: %s", e)
            if tags_updated or cover_saved:
                provider_used = "bandcamp"

    # Fetch and save artist image to artist folder if missing (MB, Last.fm, Discogs)
    if tags_updated or cover_saved:
        artist_folder = folder.parent
        if _fetch_and_save_artist_image(artist_name, artist_folder, artist_mbid):
            steps.append("Fetched and saved artist image")

    summary = f"Updated tags on {files_updated} file(s)." + (" Fetched cover art." if cover_saved else "")
    return {"steps": steps, "summary": summary, "tags_updated": tags_updated, "cover_saved": cover_saved, "provider_used": provider_used}


@app.post("/api/library/improve-album")
def api_library_improve_album():
    """Improve a single album: query MusicBrainz for tags, update files, fetch cover if missing. Used by Fix column."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    data = request.get_json() or {}
    album_id = data.get("album_id")
    if not album_id:
        return jsonify({"error": "Missing album_id"}), 400
    try:
        album_id = int(album_id)
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid album_id"}), 400
    db_conn = plex_connect()
    try:
        result = _improve_single_album(album_id, db_conn)
        return jsonify(result)
    finally:
        db_conn.close()


def _run_improve_all_albums_global(best_albums_list: List[dict]):
    """Background worker: improve each 'best' album from duplicate groups (global fix-all). Saves last_fix_all_by_provider for summary/chart."""
    total = len(best_albums_list)
    albums_improved = 0
    tags_updated_count = 0
    covers_downloaded = 0
    album_log = []
    providers = ["musicbrainz", "discogs", "lastfm", "bandcamp"]
    by_provider = {p: {"identified": 0, "covers": 0, "tags": 0} for p in providers}
    with lock:
        state["improve_all"] = {
            "running": True,
            "global": True,
            "artist_id": None,
            "current": 0,
            "total": total,
            "current_album_id": None,
            "current_album": None,
            "current_artist": None,
            "current_provider": "musicbrainz",
            "provider_status": {p: "pending" for p in providers},
            "log": [],
            "result": None,
            "error": None,
        }
    try:
        for i, item in enumerate(best_albums_list):
            album_id = item.get("album_id")
            artist_name = item.get("artist", "Unknown")
            album_title = item.get("album_title", f"Album {album_id}")
            with lock:
                if state.get("improve_all") and state["improve_all"].get("running"):
                    state["improve_all"]["current_album_id"] = album_id
                    state["improve_all"]["current_album"] = album_title
                    state["improve_all"]["current_artist"] = artist_name
                    state["improve_all"]["current_provider"] = "musicbrainz"
                    state["improve_all"]["provider_status"] = {p: ("ok" if p == "musicbrainz" else "pending") for p in providers}
            db_conn = plex_connect()
            try:
                known_mbid = item.get("musicbrainz_id") if isinstance(item.get("musicbrainz_id"), str) and (item.get("musicbrainz_id") or "").strip() else None
                result = _improve_single_album(album_id, db_conn, known_release_group_id=known_mbid)
                prov = result.get("provider_used") or "musicbrainz"
                if prov in by_provider:
                    if result.get("tags_updated") or result.get("cover_saved"):
                        by_provider[prov]["identified"] += 1
                    if result.get("cover_saved"):
                        by_provider[prov]["covers"] += 1
                    if result.get("tags_updated"):
                        by_provider[prov]["tags"] += 1
                if result.get("tags_updated"):
                    tags_updated_count += 1
                if result.get("cover_saved"):
                    covers_downloaded += 1
                if result.get("tags_updated") or result.get("cover_saved"):
                    albums_improved += 1
                steps_raw = result.get("steps", [])
                steps = [{"label": s if isinstance(s, str) else s.get("label", str(s)), "success": True} for s in steps_raw]
                with lock:
                    if state.get("improve_all"):
                        state["improve_all"]["provider_status"] = {p: "ok" for p in providers}
                album_log.append({
                    "album_id": album_id,
                    "title": album_title,
                    "artist": artist_name,
                    "summary": result.get("summary", ""),
                    "steps": steps,
                })
            finally:
                db_conn.close()
            with lock:
                if state.get("improve_all") and state["improve_all"].get("running"):
                    state["improve_all"]["current"] = i + 1
                    state["improve_all"]["log"] = list(album_log)
                    state["improve_all"]["current_steps"] = steps
        with lock:
            if state.get("improve_all"):
                state["improve_all"]["running"] = False
                state["improve_all"]["result"] = {
                    "message": f"Processed {total} album(s). Tags updated on {tags_updated_count} album(s), {covers_downloaded} cover(s) saved.",
                    "albums_processed": total,
                    "albums_improved": albums_improved,
                    "covers_downloaded": covers_downloaded,
                    "tags_updated": tags_updated_count,
                    "by_provider": by_provider,
                    "album_log": album_log,
                }
            state["last_fix_all_by_provider"] = by_provider
            state["last_fix_all_total_albums"] = total
    except Exception as e:
        logging.exception("improve-all (global) failed: %s", e)
        with lock:
            if state.get("improve_all"):
                state["improve_all"]["running"] = False
                state["improve_all"]["error"] = str(e)


def _run_improve_all_albums(artist_id: int, album_ids: list, album_titles: dict):
    """Background worker: improve each album for an artist and update state."""
    total = len(album_ids)
    albums_improved = 0
    tags_updated_count = 0
    covers_downloaded = 0
    album_log = []
    providers = ["musicbrainz", "discogs", "lastfm", "bandcamp"]
    by_provider = {p: {"identified": 0, "covers": 0, "tags": 0} for p in providers}
    with lock:
        state["improve_all"] = {
            "running": True,
            "global": False,
            "artist_id": artist_id,
            "current": 0,
            "total": total,
            "current_album_id": None,
            "current_album": None,
            "current_artist": None,
            "current_provider": "musicbrainz",
            "provider_status": {p: "pending" for p in providers},
            "log": [],
            "result": None,
            "error": None,
        }
    try:
        for i, album_id in enumerate(album_ids):
            title = album_titles.get(album_id, f"Album {album_id}")
            with lock:
                if state.get("improve_all") and state["improve_all"].get("running"):
                    state["improve_all"]["current_album_id"] = album_id
                    state["improve_all"]["current_album"] = title
                    state["improve_all"]["current_provider"] = "musicbrainz"
                    state["improve_all"]["provider_status"] = {p: ("ok" if p == "musicbrainz" else "pending") for p in providers}
            db_conn = plex_connect()
            try:
                result = _improve_single_album(album_id, db_conn)
                prov = result.get("provider_used") or "musicbrainz"
                if prov in by_provider:
                    if result.get("tags_updated") or result.get("cover_saved"):
                        by_provider[prov]["identified"] += 1
                    if result.get("cover_saved"):
                        by_provider[prov]["covers"] += 1
                    if result.get("tags_updated"):
                        by_provider[prov]["tags"] += 1
                if result.get("tags_updated"):
                    tags_updated_count += 1
                if result.get("cover_saved"):
                    covers_downloaded += 1
                if result.get("tags_updated") or result.get("cover_saved"):
                    albums_improved += 1
                steps_raw = result.get("steps", [])
                steps = [{"label": s if isinstance(s, str) else s.get("label", str(s)), "success": True} for s in steps_raw]
                with lock:
                    if state.get("improve_all"):
                        state["improve_all"]["provider_status"] = {p: "ok" for p in providers}
                album_log.append({
                    "album_id": album_id,
                    "title": title,
                    "summary": result.get("summary", ""),
                    "steps": steps,
                })
            finally:
                db_conn.close()
            with lock:
                if state.get("improve_all") and state["improve_all"].get("running"):
                    state["improve_all"]["current"] = i + 1
                    state["improve_all"]["log"] = list(album_log)
                    state["improve_all"]["current_steps"] = steps
        with lock:
            if state.get("improve_all"):
                state["improve_all"]["running"] = False
                state["improve_all"]["result"] = {
                    "message": f"Processed {total} album(s). Tags updated on {tags_updated_count} album(s), {covers_downloaded} cover(s) saved.",
                    "albums_processed": total,
                    "albums_improved": albums_improved,
                    "covers_downloaded": covers_downloaded,
                    "tags_updated": tags_updated_count,
                    "by_provider": by_provider,
                    "album_log": album_log,
                }
    except Exception as e:
        logging.exception("improve-all-albums failed: %s", e)
        with lock:
            if state.get("improve_all"):
                state["improve_all"]["running"] = False
                state["improve_all"]["error"] = str(e)


@app.post("/api/library/improve-all-albums")
def api_library_improve_all_albums():
    """Start improving all albums for an artist (MusicBrainz tags + cover)."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    data = request.get_json() or {}
    artist_id = data.get("artist_id")
    if artist_id is None:
        return jsonify({"error": "Missing artist_id"}), 400
    try:
        artist_id = int(artist_id)
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid artist_id"}), 400
    with lock:
        if state.get("improve_all") and state["improve_all"].get("running"):
            return jsonify({"error": "Improve-all already running", "started": False}), 409
    db_conn = plex_connect()
    try:
        if not SECTION_IDS:
            album_ids, album_titles = [], {}
        else:
            placeholders = ",".join("?" for _ in SECTION_IDS)
            section_filter = f"AND library_section_id IN ({placeholders})"
            section_args = [artist_id] + list(SECTION_IDS)
            rows = db_conn.execute(
                f"SELECT id, title FROM metadata_items WHERE parent_id = ? AND metadata_type = 9 {section_filter}",
                section_args,
            ).fetchall()
            album_ids = [r[0] for r in rows]
            album_titles = {r[0]: r[1] for r in rows}
    finally:
        db_conn.close()
    if not album_ids:
        return jsonify({"error": "No albums found for this artist (in selected libraries)", "started": False}), 404
    thread = threading.Thread(
        target=_run_improve_all_albums,
        args=(artist_id, album_ids, album_titles),
        daemon=True,
    )
    thread.start()
    return jsonify({"started": True, "total": len(album_ids)})


@app.post("/api/library/improve-all")
def api_library_improve_all():
    """Start global 'Fix all albums': improve each 'best' edition from duplicate groups + all albums from last scan that have a MusicBrainz match (tags + cover + artist image from MB → Discogs → Last.fm → Bandcamp)."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    with lock:
        if state.get("improve_all") and state["improve_all"].get("running"):
            return jsonify({"error": "Improve-all already running", "started": False}), 409
        if not state["duplicates"]:
            state["duplicates"] = load_scan_from_db()
        best_albums = []
        seen_ids = set()
        for artist_name, groups in state["duplicates"].items():
            for g in groups:
                best = g.get("best")
                if not best:
                    continue
                album_id = best.get("album_id")
                if album_id is None or album_id in seen_ids:
                    continue
                seen_ids.add(album_id)
                best_albums.append({
                    "artist": artist_name,
                    "album_id": album_id,
                    "album_title": best.get("title_raw") or best.get("album_norm") or f"Album {album_id}",
                    "musicbrainz_id": best.get("musicbrainz_id"),
                })
        # Also include all albums from last scan that have a MusicBrainz match (e.g. Volume 2, Volume 3 without duplicate)
        scan_id = get_last_completed_scan_id()
        if scan_id is not None:
            con = sqlite3.connect(str(STATE_DB_FILE))
            cur = con.cursor()
            try:
                cur.execute(
                    "SELECT artist, album_id, title_raw, musicbrainz_id FROM scan_editions WHERE scan_id = ? AND musicbrainz_id IS NOT NULL AND trim(musicbrainz_id) != ''",
                    (scan_id,),
                )
                for row in cur.fetchall():
                    artist_name, album_id, title_raw, mbid = row[0], row[1], row[2] or "", row[3] or ""
                    if album_id in seen_ids or not mbid.strip():
                        continue
                    seen_ids.add(album_id)
                    best_albums.append({
                        "artist": artist_name,
                        "album_id": album_id,
                        "album_title": (title_raw or "").strip() or f"Album {album_id}",
                        "musicbrainz_id": mbid.strip(),
                    })
            except Exception as e:
                logging.debug("Fix-all: could not load scan_editions for extra albums: %s", e)
            finally:
                con.close()
    if not best_albums:
        return jsonify({"error": "No albums to fix (no duplicate groups and no scan with MusicBrainz matches). Run a scan first.", "started": False}), 404
    thread = threading.Thread(target=_run_improve_all_albums_global, args=(best_albums,), daemon=True)
    thread.start()
    return jsonify({"started": True, "total": len(best_albums)})


@app.get("/api/library/improve-all-albums/progress")
@app.get("/api/library/improve-all/progress")
def api_library_improve_all_progress():
    """Return current improve-all job progress (per-artist or global: running, current, total, result, error)."""
    with lock:
        prog = state.get("improve_all")
    if prog is None:
        return jsonify({"running": False, "finished": False})
    out = {
        "running": prog.get("running", False),
        "global": prog.get("global", False),
        "current": prog.get("current", 0),
        "total": prog.get("total", 0),
        "albums_processed": prog.get("current", 0),
        "total_albums": prog.get("total", 0),
        "current_album_id": prog.get("current_album_id"),
        "current_album": prog.get("current_album"),
        "current_artist": prog.get("current_artist"),
        "current_provider": prog.get("current_provider"),
        "provider_status": prog.get("provider_status", {}),
        "current_steps": prog.get("current_steps", []),
        "album_log": prog.get("log", []),
        "finished": not prog.get("running", True) and (prog.get("result") is not None or prog.get("error") is not None),
        "result": prog.get("result"),
        "error": prog.get("error"),
    }
    return jsonify(out)


@app.post("/api/musicbrainz/fix-artist-tags")
def api_musicbrainz_fix_artist_tags():
    """Fix tags for an artist and all their albums using MusicBrainz data. Also fetches missing images."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    
    if not USE_MUSICBRAINZ:
        return jsonify({"error": "MusicBrainz not enabled"}), 400
    
    try:
        from mutagen import File as MutagenFile
        from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC, TCON, APIC, TXXX
        from mutagen.mp3 import MP3
        from mutagen.flac import FLAC
        from mutagen.mp4 import MP4
        HAS_MUTAGEN = True
    except ImportError:
        HAS_MUTAGEN = False
    
    if not HAS_MUTAGEN:
        return jsonify({"error": "mutagen library not installed. Please install it to fix tags."}), 500
    
    data = request.get_json() or {}
    artist_id = data.get("artist_id")
    
    if not artist_id:
        return jsonify({"error": "Missing artist_id"}), 400
    
    import sqlite3
    db_conn = plex_connect()
    
    # Get artist info
    artist_row = db_conn.execute(
        "SELECT id, title FROM metadata_items WHERE id = ? AND metadata_type = 8",
        (artist_id,)
    ).fetchone()
    
    if not artist_row:
        db_conn.close()
        return jsonify({"error": "Artist not found"}), 404
    
    artist_name = artist_row[1]
    
    # Find MusicBrainz ID for artist
    mbid = None
    album_rows = db_conn.execute("""
        SELECT alb.id
        FROM metadata_items alb
        WHERE alb.parent_id = ? AND alb.metadata_type = 9
        LIMIT 1
    """, (artist_id,)).fetchall()
    
    if album_rows:
        album_id = album_rows[0][0]
        folder = first_part_path(db_conn, album_id)
        if folder:
            first_audio = next((p for p in folder.rglob("*") if AUDIO_RE.search(p.name)), None)
            if first_audio:
                meta = extract_tags(first_audio)
                mbid = meta.get('musicbrainz_albumartistid') or meta.get('musicbrainz_artistid')
    
    if not mbid:
        # Search MusicBrainz
        try:
            search_result = musicbrainzngs.search_artists(artist=artist_name, limit=1)
            if search_result.get("artist-list"):
                mbid = search_result["artist-list"][0]["id"]
        except Exception as e:
            logging.warning("Failed to search MusicBrainz for artist '%s': %s", artist_name, e)
    
    if not mbid:
        db_conn.close()
        return jsonify({"error": "Could not find MusicBrainz ID for artist"}), 404
    
    # Get all albums for this artist (selected sections only)
    if not SECTION_IDS:
        album_rows = []
    else:
        placeholders = ",".join("?" for _ in SECTION_IDS)
        section_filter = f"AND library_section_id IN ({placeholders})"
        section_args = [artist_id] + list(SECTION_IDS)
        album_rows = db_conn.execute(
            f"SELECT id, title FROM metadata_items WHERE parent_id = ? AND metadata_type = 9 {section_filter}",
            section_args,
        ).fetchall()
    
    albums_updated = 0
    albums_with_images = 0
    errors = []
    
    # Process each album
    for album_id, album_title in album_rows:
        try:
            folder = first_part_path(db_conn, album_id)
            if not folder:
                continue
            
            # Get all audio files in album
            audio_files = [p for p in folder.rglob("*") if AUDIO_RE.search(p.name)]
            if not audio_files:
                continue
            
            # Get MusicBrainz release info for this album
            first_audio = audio_files[0]
            current_tags = extract_tags(first_audio)
            release_mbid = current_tags.get('musicbrainz_releasegroupid') or current_tags.get('musicbrainz_releaseid')
            release_group_id = None
            if release_mbid:
                tag_src = "musicbrainz_releasegroupid" if current_tags.get("musicbrainz_releasegroupid") else "musicbrainz_releaseid"
                release_group_id = resolve_mbid_to_release_group(release_mbid, tag_src) or release_mbid
            
            mb_release_info = None
            if release_group_id:
                try:
                    result = musicbrainzngs.get_release_group_by_id(release_group_id, includes=["releases"])
                    mb_release_info = result.get("release-group", {})
                except Exception:
                    pass
            
            # Update tags for all audio files
            for audio_file in audio_files:
                try:
                    audio = MutagenFile(str(audio_file))
                    if audio is None:
                        continue
                    
                    # Update basic tags from MusicBrainz
                    if mb_release_info:
                        # Artist
                        if isinstance(audio, (MP3, ID3)):
                            audio.tags.add(TPE1(encoding=3, text=artist_name))
                        elif isinstance(audio, FLAC):
                            audio["ARTIST"] = artist_name
                        elif isinstance(audio, MP4):
                            audio["\xa9ART"] = [artist_name]
                        
                        # Album
                        album_title_mb = mb_release_info.get("title", album_title)
                        if isinstance(audio, (MP3, ID3)):
                            audio.tags.add(TALB(encoding=3, text=album_title_mb))
                        elif isinstance(audio, FLAC):
                            audio["ALBUM"] = album_title_mb
                        elif isinstance(audio, MP4):
                            audio["\xa9alb"] = [album_title_mb]
                        
                        # Date
                        date_str = mb_release_info.get("first-release-date", "")
                        if date_str:
                            year = date_str.split("-")[0] if "-" in date_str else date_str
                            if isinstance(audio, (MP3, ID3)):
                                audio.tags.add(TDRC(encoding=3, text=year))
                            elif isinstance(audio, FLAC):
                                audio["DATE"] = year
                            elif isinstance(audio, MP4):
                                audio["\xa9day"] = [year]
                        
                        # MusicBrainz IDs
                        if isinstance(audio, FLAC):
                            audio["MUSICBRAINZ_ARTISTID"] = mbid
                            audio["MUSICBRAINZ_ALBUMARTISTID"] = mbid
                            if release_group_id:
                                audio["MUSICBRAINZ_RELEASEGROUPID"] = release_group_id
                        elif isinstance(audio, MP4):
                            audio["----:com.apple.iTunes:MusicBrainz Artist Id"] = [mbid.encode('utf-8')]
                            audio["----:com.apple.iTunes:MusicBrainz Album Artist Id"] = [mbid.encode('utf-8')]
                            if release_group_id:
                                audio["----:com.apple.iTunes:MusicBrainz Release Group Id"] = [release_group_id.encode('utf-8')]
                    
                    audio.save()
                    albums_updated += 1
                    
                    # Try to fetch and save album cover if missing
                    if not any(f.name.lower().startswith(('cover', 'folder', 'album', 'artwork', 'front')) 
                              and f.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp'] 
                              for f in folder.iterdir() if f.is_file()):
                        # Try to get cover from MusicBrainz
                        if release_group_id:
                            try:
                                # Get cover art from MusicBrainz Cover Art Archive
                                cover_url = f"http://coverartarchive.org/release-group/{release_group_id}/front"
                                cover_resp = requests.get(cover_url, timeout=5, allow_redirects=True)
                                if cover_resp.status_code == 200:
                                    cover_path = folder / "cover.jpg"
                                    with open(cover_path, 'wb') as f:
                                        f.write(cover_resp.content)
                                    albums_with_images += 1
                            except Exception:
                                pass
                
                except Exception as e:
                    errors.append(f"Error updating {audio_file.name}: {str(e)}")
                    logging.error("Error updating tags for %s: %s", audio_file, e)
        
        except Exception as e:
            errors.append(f"Error processing album {album_title}: {str(e)}")
            logging.error("Error processing album %s: %s", album_title, e)
    
    db_conn.close()
    
    return jsonify({
        "success": True,
        "message": f"Updated tags for {albums_updated} file(s) across {len(album_rows)} album(s). Fetched {albums_with_images} cover image(s).",
        "albums_processed": len(album_rows),
        "files_updated": albums_updated,
        "images_fetched": albums_with_images,
        "errors": errors[:10]  # Limit error messages
    })

@app.post("/api/musicbrainz/fix-album-tags")
def api_musicbrainz_fix_album_tags():
    """Fix tags for a single album using MusicBrainz data."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    
    data = request.get_json() or {}
    album_id = data.get("album_id")
    tags_to_apply = data.get("tags", {})
    
    if not album_id:
        return jsonify({"error": "Missing album_id"}), 400
    
    # For now, return success but don't actually write tags
    # Tag writing requires mutagen or similar library
    # This is a placeholder for future implementation
    return jsonify({
        "success": True,
        "message": "Tag fixing not yet implemented. This will require mutagen library for tag writing.",
        "tags_to_apply": tags_to_apply
    })

@app.get("/api/scan-history/<int:scan_id>")
def api_scan_history_detail(scan_id):
    """Return details of a specific scan or dedupe entry."""
    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute("PRAGMA table_info(scan_history)")
    cols_info = [r[1] for r in cur.fetchall()]
    has_entry_type = "entry_type" in cols_info
    if has_entry_type:
        cur.execute("""
            SELECT scan_id, start_time, end_time, duration_seconds, albums_scanned,
                   duplicates_found, artists_processed, artists_total, ai_used_count,
                   mb_used_count, ai_enabled, mb_enabled, auto_move_enabled,
                   space_saved_mb, albums_moved, status,
                   duplicate_groups_count, total_duplicates_count, broken_albums_count,
                   missing_albums_count, albums_without_artist_image, albums_without_album_image,
                   albums_without_complete_tags, albums_without_mb_id, albums_without_artist_mb_id,
                   entry_type
            FROM scan_history
            WHERE scan_id = ?
        """, (scan_id,))
    else:
        cur.execute("""
            SELECT scan_id, start_time, end_time, duration_seconds, albums_scanned,
                   duplicates_found, artists_processed, artists_total, ai_used_count,
                   mb_used_count, ai_enabled, mb_enabled, auto_move_enabled,
                   space_saved_mb, albums_moved, status,
                   duplicate_groups_count, total_duplicates_count, broken_albums_count,
                   missing_albums_count, albums_without_artist_image, albums_without_album_image,
                   albums_without_complete_tags, albums_without_mb_id, albums_without_artist_mb_id
            FROM scan_history
            WHERE scan_id = ?
        """, (scan_id,))
    row = cur.fetchone()
    con.close()

    if not row:
        return jsonify({"error": "Scan not found"}), 404

    out = {
        "scan_id": row[0],
        "start_time": row[1],
        "end_time": row[2],
        "duration_seconds": row[3],
        "albums_scanned": row[4] or 0,
        "duplicates_found": row[5] or 0,
        "artists_processed": row[6] or 0,
        "artists_total": row[7] or 0,
        "ai_used_count": row[8] or 0,
        "mb_used_count": row[9] or 0,
        "ai_enabled": bool(row[10]),
        "mb_enabled": bool(row[11]),
        "auto_move_enabled": bool(row[12]),
        "space_saved_mb": row[13] or 0,
        "albums_moved": row[14] or 0,
        "status": row[15] or "completed",
        "duplicate_groups_count": row[16] or 0 if len(row) > 16 else 0,
        "total_duplicates_count": row[17] or 0 if len(row) > 17 else 0,
        "broken_albums_count": row[18] or 0 if len(row) > 18 else 0,
        "missing_albums_count": row[19] or 0 if len(row) > 19 else 0,
        "albums_without_artist_image": row[20] or 0 if len(row) > 20 else 0,
        "albums_without_album_image": row[21] or 0 if len(row) > 21 else 0,
        "albums_without_complete_tags": row[22] or 0 if len(row) > 22 else 0,
        "albums_without_mb_id": row[23] or 0 if len(row) > 23 else 0,
        "albums_without_artist_mb_id": row[24] or 0 if len(row) > 24 else 0,
    }
    if has_entry_type and len(row) > 25:
        out["entry_type"] = row[25] or "scan"
    else:
        out["entry_type"] = "scan"
    return jsonify(out)

@app.get("/api/scan-history/<int:scan_id>/moves")
def api_scan_history_moves(scan_id):
    """Return all moves for a specific scan."""
    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute("PRAGMA table_info(scan_moves)")
    move_cols = [r[1] for r in cur.fetchall()]
    has_extra = "album_title" in move_cols and "fmt_text" in move_cols
    if has_extra:
        cur.execute("""
            SELECT move_id, scan_id, artist, album_id, original_path, moved_to_path,
                   size_mb, moved_at, restored, album_title, fmt_text
            FROM scan_moves
            WHERE scan_id = ?
            ORDER BY moved_at DESC
        """, (scan_id,))
    else:
        cur.execute("""
            SELECT move_id, scan_id, artist, album_id, original_path, moved_to_path,
                   size_mb, moved_at, restored
            FROM scan_moves
            WHERE scan_id = ?
            ORDER BY moved_at DESC
        """, (scan_id,))
    rows = cur.fetchall()
    con.close()
    
    moves = []
    for row in rows:
        m = {
            "move_id": row[0],
            "scan_id": row[1],
            "artist": row[2],
            "album_id": row[3],
            "original_path": row[4],
            "moved_to_path": row[5],
            "size_mb": row[6] or 0,
            "moved_at": row[7],
            "restored": bool(row[8]),
        }
        if has_extra and len(row) >= 11:
            m["album_title"] = row[9] or ""
            m["fmt_text"] = row[10] or ""
        moves.append(m)
    
    return jsonify(moves)

@app.post("/api/scan-history/<int:scan_id>/restore")
def api_scan_history_restore(scan_id):
    """Restore moved files to their original location."""
    data = request.get_json() or {}
    move_ids = data.get("move_ids", [])
    restore_all = data.get("all", False)
    
    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    
    if restore_all:
        cur.execute("""
            SELECT move_id, original_path, moved_to_path, artist
            FROM scan_moves
            WHERE scan_id = ? AND restored = 0
        """, (scan_id,))
    else:
        if not move_ids:
            return jsonify({"error": "No move_ids provided"}), 400
        placeholders = ",".join("?" * len(move_ids))
        cur.execute(f"""
            SELECT move_id, original_path, moved_to_path, artist
            FROM scan_moves
            WHERE scan_id = ? AND move_id IN ({placeholders}) AND restored = 0
        """, (scan_id, *move_ids))
    
    rows = cur.fetchall()
    if not rows:
        con.close()
        return jsonify({"error": "No moves found to restore"}), 404
    
    artists_to_refresh = set()
    restored_count = 0
    restored_paths: List[dict] = []
    
    for move_id, original_path, moved_to_path, artist in rows:
        src = Path(moved_to_path)
        dst = Path(original_path)
        
        if not src.exists():
            logging.warning(f"Restore: source {src} does not exist, skipping")
            continue
        
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            safe_move(str(src), str(dst))
            artists_to_refresh.add(artist)
            restored_count += 1
            restored_paths.append({"from": moved_to_path, "to": original_path})
            
            # Mark as restored
            cur.execute("UPDATE scan_moves SET restored = 1 WHERE move_id = ?", (move_id,))
        except Exception as e:
            logging.error(f"Restore: failed to restore {src} → {dst}: {e}")
            continue
    
    con.commit()
    con.close()
    
    # Refresh Plex for affected artists
    for artist in artists_to_refresh:
        letter = quote_plus(artist[0].upper())
        art_enc = quote_plus(artist)
        try:
            plex_api(f"/library/sections/{SECTION_ID}/refresh?path=/music/matched/{letter}/{art_enc}", method="GET")
        except Exception as e:
            logging.warning(f"Restore: plex refresh failed for {artist}: {e}")
    
    return jsonify({
        "restored": restored_count,
        "artists_refreshed": len(artists_to_refresh),
        "restored_paths": restored_paths,
    })

@app.post("/api/scan-history/<int:scan_id>/dedupe")
def api_scan_history_dedupe(scan_id):
    """Manually dedupe albums from a previous scan."""
    # Load scan results from DB (dict artist -> list of groups)
    scan_results = load_scan_from_db()
    if not scan_results:
        return jsonify({"error": "No scan results found for this scan"}), 404
    
    flat_groups = [g for groups in scan_results.values() for g in groups]
    if not flat_groups:
        return jsonify({"error": "No duplicate groups to dedupe"}), 404
    
    # Start deduplication
    background_dedupe(flat_groups)
    return jsonify({"status": "ok", "message": "Deduplication started"})

@app.get("/api/dedupe")
def api_dedupe():
    with lock:
        deduping = state["deduping"]
        progress = state["dedupe_progress"]
        total = state["dedupe_total"]
        start_time = state.get("dedupe_start_time")
        saved_this_run = state.get("dedupe_saved_this_run", 0)
        current_group = state.get("dedupe_current_group")
        last_write = state.get("dedupe_last_write")

    percent = round(100 * progress / total, 1) if total else 0
    eta_seconds = None
    if start_time and total and progress > 0:
        elapsed = time.time() - start_time
        avg_per_group = elapsed / progress
        remaining = total - progress
        eta_seconds = max(0, int(remaining * avg_per_group))

    return jsonify(
        deduping=deduping,
        progress=progress,
        total=total,
        saved=get_stat("space_saved"),
        saved_this_run=saved_this_run,
        moved=get_stat("removed_dupes"),
        percent=percent,
        eta_seconds=eta_seconds,
        current_group=current_group,
        last_write=last_write,
    )

@app.get("/details/<artist>/<int:album_id>")
def details(artist, album_id):
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured", "requiresConfig": True}), 503
    art = artist.replace("_", " ")
    with lock:
        groups = state["duplicates"].get(art)
    if groups is None:
        groups = load_scan_from_db().get(art, [])
    for g in groups:
        best_album_id_in_g = g.get("album_id") or (g.get("best", {}).get("album_id") if g.get("best") else None)
        if best_album_id_in_g == album_id:
            editions = [g["best"]] + g["losers"]
            best_album_id = best_album_id_in_g
            artist_rating_key = None
            try:
                db_conn = plex_connect()
                best_track_titles = {(t.title or "").strip().lower() for t in get_tracks(db_conn, best_album_id)}
                # Plex Web: artist page = /library/metadata/{artist_id}; album's parent is the artist (metadata_type 9)
                row = db_conn.execute(
                    "SELECT parent_id FROM metadata_items WHERE id = ? AND metadata_type = 9",
                    (best_album_id,),
                ).fetchone()
                if row and row[0]:
                    artist_rating_key = int(row[0])
            except Exception:
                best_track_titles = set()
                db_conn = None

            out = []
            rationale = g["best"].get("rationale", "")
            for i, e in enumerate(editions):
                folder_path = path_for_fs_access(Path(e["folder"]))
                is_best = i == 0
                # Size: losers have size_mb in DB; best we compute (frontend expects bytes)
                if is_best:
                    size_mb = safe_folder_size(folder_path) // (1024 * 1024)
                else:
                    size_mb = e.get("size", 0) or (safe_folder_size(folder_path) // (1024 * 1024))
                size_bytes = size_mb * (1024 * 1024)

                track_list = []
                if db_conn:
                    try:
                        for t in get_tracks_for_details(db_conn, e["album_id"]):
                            title_norm = (t.get("title") or t.get("name") or "").strip().lower()
                            is_bonus = not is_best and title_norm not in best_track_titles
                            raw_path = t.get("path")
                            track_path = str(path_for_fs_access(Path(raw_path))) if raw_path else None
                            track_list.append({
                                "idx": t.get("idx", 0),
                                "title": t.get("title") or t.get("name"),
                                "name": t.get("name") or t.get("title"),
                                "dur": t.get("dur", 0),
                                "duration": t.get("duration"),
                                "format": t.get("format"),
                                "bitrate": t.get("bitrate"),
                                "is_bonus": is_bonus,
                                "path": track_path,
                            })
                    except Exception as track_err:
                        logging.warning(
                            "details: tracks failed for edition album_id=%s: %s",
                            e["album_id"], track_err
                        )

                thumb_data = fetch_cover_as_base64(e["album_id"])
                out.append({
                    "thumb_data": thumb_data,
                    "title_raw": e.get("title_raw") or "",
                    "size": size_bytes,
                    "fmt": e.get("fmt_text", e.get("fmt", "")),
                    "br": (e.get("br", 0) // 1000) if isinstance(e.get("br"), int) else 0,
                    "sr": e.get("sr", 0),
                    "bd": e.get("bd", 0),
                    "path": str(folder_path),
                    "folder": str(folder_path),
                    "album_id": e["album_id"],
                    "track_count": len(track_list),
                    "tracks": track_list,
                    "musicbrainz_id": e.get("musicbrainz_id"),  # Include MusicBrainz ID if available
                    "match_verified_by_ai": bool(e.get("match_verified_by_ai", False)),
                })
            if db_conn:
                try:
                    db_conn.close()
                except Exception:
                    pass
            return jsonify(
                artist=art,
                album=g["best"]["title_raw"],
                artist_id=artist_rating_key,
                editions=out,
                rationale=rationale,
                merge_list=g["best"].get("merge_list", []),
            )
    return jsonify({}), 404

def _normalize_edition_as_best(edition: dict, artist: str) -> dict:
    """Ensure an edition has the keys expected for group['best']."""
    e = dict(edition)
    if "title_raw" not in e or e["title_raw"] is None:
        try:
            db = plex_connect()
            e["title_raw"] = album_title(db, e["album_id"])
            db.close()
        except Exception:
            e["title_raw"] = ""
    e.setdefault("album_norm", (e.get("title_raw") or "").lower())
    e.setdefault("fmt_text", e.get("fmt", ""))
    e.setdefault("br", 0)
    e.setdefault("sr", 0)
    e.setdefault("bd", 0)
    e.setdefault("rationale", "")
    e.setdefault("merge_list", [])
    e.setdefault("used_ai", False)
    e.setdefault("meta", {})
    e.setdefault("dur", 0)
    e.setdefault("discs", 1)
    return e


def _run_dedupe_artist_one(art: str, album_id: int, keep_edition_album_id: Optional[int], group_copy: dict) -> None:
    """Run dedupe for one group in a background thread. Updates state and DB."""
    with lock:
        state["deduping"] = True
        state["dedupe_progress"] = 0
        state["dedupe_total"] = 1
    try:
        moved_list = perform_dedupe(group_copy)
        removed_count = len(moved_list)
        total_mb = sum(item["size"] for item in moved_list)
        increment_stat("removed_dupes", removed_count)
        increment_stat("space_saved", total_mb)
        logging.debug(f"dedupe_artist(): removed {removed_count} dupes, freed {total_mb} MB")

        letter = quote_plus(art[0].upper())
        art_enc = quote_plus(art)
        try:
            plex_api(f"/library/sections/{SECTION_ID}/refresh?path=/music/matched/{letter}/{art_enc}", method="GET")
            plex_api(f"/library/sections/{SECTION_ID}/emptyTrash", method="PUT")
        except Exception as e:
            logging.warning(f"dedupe_artist(): plex refresh/emptyTrash failed: {e}")

        with lock:
            groups = state["duplicates"].get(art, [])
            groups[:] = [gr for gr in groups if gr["album_id"] != album_id]
            if not groups:
                state["duplicates"].pop(art, None)
            con = sqlite3.connect(str(STATE_DB_FILE))
            cur = con.cursor()
            cur.execute("DELETE FROM duplicates_best WHERE artist = ? AND album_id = ?", (art, album_id))
            cur.execute("DELETE FROM duplicates_loser WHERE artist = ? AND album_id = ?", (art, album_id))
            con.commit()
            con.close()
            sid = state.get("scan_id")
            state["dedupe_progress"] = 1
            state["deduping"] = False
        if sid is not None:
            update_dedupe_scan_summary(sid, total_mb, removed_count)
    except Exception as e:
        logging.exception("dedupe_artist background: %s", e)
        with lock:
            state["deduping"] = False
            state["dedupe_progress"] = 0
            state["dedupe_total"] = 0


@app.post("/dedupe/artist/<artist>")
def dedupe_artist(artist):
    r = _requires_config()
    if r is not None:
        return r
    ensure_dedupe_scan_id()
    art = artist.replace("_", " ")
    data = request.get_json() or {}
    raw = data.get("album_id")
    album_id = int(raw) if raw is not None else None
    keep_edition_album_id = data.get("keep_edition_album_id")
    if keep_edition_album_id is not None:
        keep_edition_album_id = int(keep_edition_album_id)

    group_copy = None
    with lock:
        groups = state["duplicates"].get(art, [])
        for g in list(groups):
            if g["album_id"] != album_id:
                continue
            if keep_edition_album_id is not None:
                editions = [g["best"]] + g["losers"]
                kept = None
                losers = []
                for e in editions:
                    aid = e.get("album_id")
                    if aid == keep_edition_album_id:
                        kept = e
                    else:
                        losers.append(e)
                if kept is None or not losers:
                    return jsonify({"error": "Invalid keep_edition_album_id or no editions to remove"}), 400
                group_copy = {
                    "artist": art,
                    "album_id": album_id,
                    "best": _normalize_edition_as_best(kept, art),
                    "losers": losers,
                }
            else:
                import copy
                group_copy = copy.deepcopy(g)
            break

    if group_copy is None:
        return jsonify({"error": "Group not found"}), 404

    threading.Thread(target=_run_dedupe_artist_one, args=(art, album_id, keep_edition_album_id, group_copy), daemon=True).start()
    return jsonify(status="started", message="Deduplication started", moved=[]), 202


# Allowed extensions for bonus track move (security)
_MOVE_TRACK_EXTENSIONS = frozenset(
    ".flac .wav .m4a .mp3 .ogg .opus .aac .ape .alac .dsf .aif .aiff .wma .mp4 .m4b .m4p .aifc".split()
)


def _merge_bonus_tracks_for_group(g: dict) -> None:
    """
    For one duplicate group, move bonus tracks (names in merge_list) from loser
    editions into the best edition folder. Idempotent per track.
    """
    merge_list = g["best"].get("merge_list") or []
    if not merge_list:
        return
    merge_set = {(t.strip().lower()): t.strip() for t in merge_list}
    best_folder = path_for_fs_access(Path(g["best"]["folder"]))
    db_conn = None
    try:
        db_conn = plex_connect()
        for loser in g["losers"]:
            source_folder = path_for_fs_access(Path(loser["folder"]))
            for t in get_tracks_for_details(db_conn, loser["album_id"]):
                title = (t.get("title") or t.get("name") or "").strip()
                if not title or title.lower() not in merge_set:
                    continue
                raw_path = t.get("path")
                if not raw_path:
                    continue
                track_path = Path(raw_path)
                try:
                    src_resolved = path_for_fs_access(track_path).resolve()
                    base_resolved = source_folder.resolve()
                except Exception:
                    continue
                if not src_resolved.is_file():
                    continue
                if src_resolved.suffix.lower() not in _MOVE_TRACK_EXTENSIONS:
                    continue
                try:
                    if not src_resolved.is_relative_to(base_resolved):
                        continue
                except AttributeError:
                    if not str(src_resolved).startswith(str(base_resolved)):
                        continue
                dest_file = best_folder / src_resolved.name
                if dest_file.exists():
                    stem, suf = dest_file.stem, dest_file.suffix
                    n = 1
                    while dest_file.exists():
                        dest_file = best_folder / f"{stem} ({n}){suf}"
                        n += 1
                try:
                    safe_move(str(src_resolved), str(dest_file))
                    logging.info("merge_bonus: moved %s → %s", src_resolved.name, best_folder)
                except Exception as e:
                    logging.warning("merge_bonus: failed %s → %s: %s", src_resolved, dest_file, e)
        try:
            plex_path = g["best"]["folder"]
            plex_api(f"/library/sections/{SECTION_ID}/refresh?path={plex_path}", method="GET")
        except Exception as e:
            logging.warning("merge_bonus: Plex refresh failed: %s", e)
    finally:
        if db_conn:
            try:
                db_conn.close()
            except Exception:
                pass


@app.post("/dedupe/move-track/<artist>")
def dedupe_move_track(artist):
    """
    Move a single bonus track file from one edition folder to the kept edition folder.
    Body: { "album_id": "<group id>", "source_index": int, "track_path": str, "target_index": int }.
    """
    r = _requires_config()
    if r is not None:
        return r
    art = artist.replace("_", " ")
    data = request.get_json() or {}
    raw_album_id = data.get("album_id")
    album_id = int(raw_album_id) if raw_album_id is not None else None
    source_index = data.get("source_index")
    target_index = data.get("target_index")
    track_path_raw = data.get("track_path")

    if album_id is None or source_index is None or target_index is None or not track_path_raw:
        return jsonify(success=False, message="Missing album_id, source_index, target_index or track_path"), 400

    try:
        source_index = int(source_index)
        target_index = int(target_index)
    except (TypeError, ValueError):
        return jsonify(success=False, message="source_index and target_index must be integers"), 400

    with lock:
        groups = state["duplicates"].get(art)
        if groups is None:
            groups = load_scan_from_db().get(art, [])
        g = next((gr for gr in groups if gr["album_id"] == album_id), None)

    if g is None:
        return jsonify(success=False, message="Duplicate group not found"), 404

    editions = [g["best"]] + g["losers"]
    if source_index < 0 or source_index >= len(editions) or target_index < 0 or target_index >= len(editions):
        return jsonify(success=False, message="Invalid source_index or target_index"), 400
    if source_index == target_index:
        return jsonify(success=False, message="Source and target editions must differ"), 400

    source_folder = path_for_fs_access(Path(editions[source_index]["folder"]))
    target_folder = path_for_fs_access(Path(editions[target_index]["folder"]))

    track_path = Path(track_path_raw)
    try:
        src_resolved = track_path.resolve()
        base_resolved = source_folder.resolve()
    except Exception as e:
        return jsonify(success=False, message=f"Invalid path: {e}"), 400

    if not src_resolved.is_file():
        return jsonify(success=False, message="track_path is not a file"), 400
    if src_resolved.suffix.lower() not in _MOVE_TRACK_EXTENSIONS:
        return jsonify(success=False, message="File type not allowed for move"), 400
    try:
        if not src_resolved.is_relative_to(base_resolved):
            return jsonify(success=False, message="Track must be inside the source edition folder"), 400
    except AttributeError:
        if not str(src_resolved).startswith(str(base_resolved)):
            return jsonify(success=False, message="Track must be inside the source edition folder"), 400

    dest_file = target_folder / src_resolved.name
    if dest_file.exists():
        stem, suf = dest_file.stem, dest_file.suffix
        n = 1
        while dest_file.exists():
            dest_file = target_folder / f"{stem} ({n}){suf}"
            n += 1

    try:
        safe_move(str(src_resolved), str(dest_file))
    except Exception as e:
        logging.exception("move-track: move failed %s → %s", src_resolved, dest_file)
        return jsonify(success=False, message=str(e)), 500

    # Ask Plex to rescan so the kept album sees the new file
    try:
        plex_path = editions[target_index]["folder"]
        plex_api(f"/library/sections/{SECTION_ID}/refresh?path={plex_path}", method="GET")
    except Exception as e:
        logging.warning("move-track: Plex refresh failed: %s", e)

    return jsonify(success=True, message="Track moved to kept edition", dest=str(dest_file)), 200


@app.post("/dedupe/all")
def dedupe_all():
    r = _requires_config()
    if r is not None:
        return r
    with lock:
        all_groups = [g for lst in state["duplicates"].values() for g in lst]
        if not state["duplicates"]:
            state["duplicates"] = load_scan_from_db()
        all_groups = [g for lst in state["duplicates"].values() for g in lst]
    if not all_groups:
        return "", 204
    threading.Thread(target=background_dedupe, args=(all_groups,), daemon=True).start()
    return "", 204


@app.post("/dedupe/merge-and-dedupe")
def dedupe_merge_and_dedupe():
    """
    First merge bonus tracks (from merge_list) into the kept edition for every group
    that has extra tracks, then run full dedupe (move loser folders, update Plex).
    """
    r = _requires_config()
    if r is not None:
        return r
    with lock:
        if not state["duplicates"]:
            state["duplicates"] = load_scan_from_db()
        all_groups = [g for lst in state["duplicates"].values() for g in lst]

    for g in all_groups:
        if g["best"].get("merge_list"):
            try:
                _merge_bonus_tracks_for_group(g)
            except Exception as e:
                logging.warning("merge_and_dedupe: merge_bonus failed for %s: %s", g.get("artist"), e)

    threading.Thread(target=background_dedupe, args=(all_groups,), daemon=True).start()
    return "", 204

@app.post("/dedupe/selected")
def dedupe_selected():
    r = _requires_config()
    if r is not None:
        return r
    ensure_dedupe_scan_id()
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

    with lock:
        sid = state.get("scan_id")
    if sid is not None:
        update_dedupe_scan_summary(sid, total_moved, removed_count)

    return jsonify(moved=moved_list), 200


# ─── Integrated frontend (self-hosted: serve SPA from same container) ─────────────
if _HAS_STATIC_UI:
    @app.get("/")
    def serve_index():
        return send_from_directory(_FRONTEND_DIST, "index.html")

    @app.get("/assets/<path:path>")
    def serve_assets(path):
        return send_from_directory(os.path.join(_FRONTEND_DIST, "assets"), path)

    @app.get("/<path:path>")
    def serve_spa_fallback(path):
        """SPA: serve static file from dist if present, else index.html for client-side routing."""
        if request.path.startswith(("/api/", "/scan/", "/dedupe/", "/details/")):
            return jsonify(error="Not found"), 404
        path_obj = os.path.join(_FRONTEND_DIST, path)
        if os.path.isfile(path_obj):
            return send_from_directory(_FRONTEND_DIST, path)
        return send_from_directory(_FRONTEND_DIST, "index.html")


# ───────────────────────────────── MAIN ───────────────────────────────────
import os

if __name__ == "__main__":
    # Web UI only: start server first so UI is available immediately, then run startup checks (cross-check in background).
    def run_server():
        app.run(host="0.0.0.0", port=WEBUI_PORT, threaded=True, use_reloader=False)

    # Fast checks before server (can SystemExit)
    if PLEX_CONFIGURED:
        _validate_plex_connection()
        if not _self_diag():
            raise SystemExit("Self‑diagnostic failed – please fix the issues above and restart PMDA.")
    else:
        logging.info("Skipping startup checks (Plex not configured – use Settings to configure).")

    server_thread = threading.Thread(target=run_server, daemon=False)
    server_thread.start()
    logging.info("Web UI listening on http://0.0.0.0:%s – startup path cross-check running in background", WEBUI_PORT)

    def run_cross_check_background():
        if PLEX_CONFIGURED and not DISABLE_PATH_CROSSCHECK:
            _cross_check_bindings(raise_on_abort=False)
        elif DISABLE_PATH_CROSSCHECK:
            logging.info("PATH cross-check skipped (DISABLE_PATH_CROSSCHECK=true).")

    cross_check_thread = threading.Thread(target=run_cross_check_background, daemon=True)
    cross_check_thread.start()

    server_thread.join()  # block forever (app.run never returns)