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

import argparse
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



from flask import Flask, request, jsonify, send_from_directory

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

# Ensure LOG_LEVEL exists for initial logging setup
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

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
Robust configuration helper:

* Loads defaults from the baked‑in config.json shipped inside the Docker image.
* Copies that file (and ai_prompt.txt) into the user‑writable config dir on first run.
* Overrides every value with an environment variable when present.
* Falls back to sensible, documented defaults when neither file nor env provides a value.
* Validates critical keys so we fail early instead of crashing later.
* Logs where each value came from (env vs config vs default).
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

# (3) Load config: JSON is only a fallback; at runtime effective config is
#     env > SQLite (state.db/settings) > config.json > defaults. See _get() below.
with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
    conf: dict = json.load(fh)

# ─── Auto‑generate PATH_MAP from Plex at *every* startup ────────────────────
#
# Design goal: avoid requiring users to manually configure PATH_MAP (Plex path → host path).
# Flow:
#   1) Discovery: Plex API returns all <Location> paths for the music section(s).
#      We get the exact paths Plex uses (e.g. /music/matched, /music/unmatched).
#   2) Merge: If the user provided a broader PATH_MAP in env/config (e.g. /music → /music/…),
#      we apply it by prefix; otherwise we keep Plex path = container path (so Docker
#      volume binds must match: -v /host/Music_matched:/music/matched).
#   3) Cross-check: For each binding we sample CROSSCHECK_SAMPLES tracks from the DB,
#      resolve their paths via PATH_MAP, and verify the files exist on disk. If they
#      don’t, we try to find the correct host root (sibling dirs or rglob) and patch
#      PATH_MAP + config.json. So even when paths differ (e.g. Plex says /music/unmatched
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
try:
    plex_host   = (os.getenv("PLEX_HOST")   or conf.get("PLEX_HOST")) or ""
    plex_token  = (os.getenv("PLEX_TOKEN")  or conf.get("PLEX_TOKEN")) or ""
    plex_host   = plex_host.strip() if isinstance(plex_host, str) else ""
    plex_token  = plex_token.strip() if isinstance(plex_token, str) else ""
    # Require a URL-like host so we never call Plex API with empty/invalid base
    if not plex_host or not plex_token or not str(plex_host).strip().startswith(("http://", "https://")):
        logging.info("PLEX_HOST or PLEX_TOKEN missing or invalid – starting in unconfigured (wizard) mode")
    else:
        # Support multiple section IDs via SECTION_IDS or SECTION_ID (comma-separated)
        # Read any user‑provided value first
        raw_sections = (
            os.getenv("SECTION_IDS")
            or os.getenv("SECTION_ID")
            or conf.get("SECTION_IDS")
            or conf.get("SECTION_ID")
        )

        # Treat an empty string or whitespace‑only value as “not provided”
        if raw_sections is not None and str(raw_sections).strip() == "":
            raw_sections = None

        logging.debug("SECTION_IDS raw input = %r", raw_sections)

        if not raw_sections:
            try:
                resp = requests.get(f"{plex_host.rstrip('/')}/library/sections", headers={"X-Plex-Token": plex_token}, timeout=10)
                root = ET.fromstring(resp.text)
                SECTION_IDS = [int(d.attrib['key']) for d in root.iter("Directory") if d.attrib.get('type') == 'artist']
                logging.info("Auto-detected SECTION_IDS from Plex: %s", SECTION_IDS)
            except Exception as e:
                logging.error("Failed to auto-detect SECTION_IDS: %s", e)
                SECTION_IDS = []
        else:
            if isinstance(raw_sections, list):
                SECTION_IDS = [int(x) for x in raw_sections if str(x).strip().isdigit()]
            else:
                SECTION_IDS = []
                for part in re.split(r'\s*,\s*', str(raw_sections)):
                    if part.strip().isdigit():
                        SECTION_IDS.append(int(part.strip()))

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
            raw_env_map = _parse_path_map(os.getenv("PATH_MAP") or conf.get("PATH_MAP", {}))
            logging.info("Raw PATH_MAP from env/config: %s", raw_env_map)
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
            conf["PATH_MAP"] = merged_map
            with open(CONFIG_PATH, "w", encoding="utf-8") as fh_cfg:
                json.dump(conf, fh_cfg, indent=2)
            logging.info("🔄 Auto‑generated/updated PATH_MAP from Plex: %s", auto_map)
except Exception as e:
    logging.warning("⚠️  Failed to auto‑generate PATH_MAP – %s", e)
    SECTION_IDS = []
    SECTION_NAMES = {}

# (4) Merge with environment variables ----------------------------------------
ENV_SOURCES: dict[str, str] = {}

def _get(key: str, *, default=None, cast=lambda x: x):
    """Return the merged value and remember where it came from.
    Priority: env > SQLite > config.json > default
    """
    # 1. Environment variables (highest priority)
    if (env_val := os.getenv(key)) is not None:
        ENV_SOURCES[key] = "env"
        raw = env_val
    # 2. SQLite database (settings table) - check if DB exists and has the key
    else:
        sqlite_val = None
        try:
            # STATE_DB_FILE is defined later, so we construct the path here
            state_db_path = CONFIG_DIR / "state.db"
            if state_db_path.exists():
                con = sqlite3.connect(str(state_db_path), timeout=5)
                cur = con.cursor()
                cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
                row = cur.fetchone()
                con.close()
                if row and row[0]:
                    sqlite_val = row[0]
        except Exception:
            # If SQLite read fails, fall through to config.json
            pass
        
        if sqlite_val is not None:
            ENV_SOURCES[key] = "sqlite"
            raw = sqlite_val
            # Treat empty strings as "not set" to use default instead
            if isinstance(raw, str) and raw.strip() == "" and default is not None:
                ENV_SOURCES[key] = "default"
                raw = default
            else:
                return cast(raw)
        # 3. config.json
        elif key in conf:
            ENV_SOURCES[key] = "config"
            raw = conf[key]
            # Treat empty strings as "not set" to use default instead
            if isinstance(raw, str) and raw.strip() == "" and default is not None:
                ENV_SOURCES[key] = "default"
                raw = default
        else:
            # 4. Default value
            ENV_SOURCES[key] = "default"
            raw = default
    return cast(raw)

# PLEX_DB_PATH defaults to /database in container; no SystemExit if unconfigured.
merged = {
    "PLEX_DB_PATH":   _get("PLEX_DB_PATH",   default="/database",                       cast=str),
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
    "LIDARR_URL": _get("LIDARR_URL", default="", cast=str),
    "LIDARR_API_KEY": _get("LIDARR_API_KEY", default="", cast=str),
    "AUTOBRR_URL": _get("AUTOBRR_URL", default="", cast=str),
    "AUTOBRR_API_KEY": _get("AUTOBRR_API_KEY", default="", cast=str),
    "AUTO_FIX_BROKEN_ALBUMS": _get("AUTO_FIX_BROKEN_ALBUMS", default=False, cast=_parse_bool),
    "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD": _get("BROKEN_ALBUM_CONSECUTIVE_THRESHOLD", default=3, cast=int),
    "BROKEN_ALBUM_PERCENTAGE_THRESHOLD": _get("BROKEN_ALBUM_PERCENTAGE_THRESHOLD", default=0.20, cast=float),
    "REQUIRED_TAGS": _get("REQUIRED_TAGS", default="artist,album,date", cast=lambda s: [t.strip() for t in str(s).split(",") if t.strip()] if isinstance(s, str) else (s if isinstance(s, list) else ["artist", "album", "date"])),
    "SKIP_FOLDERS": _get("SKIP_FOLDERS", default="", cast=lambda s: [p.strip() for p in str(s).split(",") if p.strip()]),
    "AI_BATCH_SIZE": _get("AI_BATCH_SIZE", default=10, cast=int),
    "FFPROBE_POOL_SIZE": _get("FFPROBE_POOL_SIZE", default=4, cast=int),
    "AUTO_MOVE_DUPES": _get("AUTO_MOVE_DUPES", default=False, cast=_parse_bool),
}
# Always use the auto‑generated PATH_MAP from config.json
merged["PATH_MAP"] = conf.get("PATH_MAP", {})


SKIP_FOLDERS: list[str] = merged["SKIP_FOLDERS"]
USE_MUSICBRAINZ: bool = bool(merged["USE_MUSICBRAINZ"])
MUSICBRAINZ_EMAIL: str = merged.get("MUSICBRAINZ_EMAIL", "pmda@example.com")
MB_QUEUE_ENABLED: bool = bool(merged.get("MB_QUEUE_ENABLED", True))

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
BROKEN_ALBUM_CONSECUTIVE_THRESHOLD: int = int(merged.get("BROKEN_ALBUM_CONSECUTIVE_THRESHOLD", 3))
BROKEN_ALBUM_PERCENTAGE_THRESHOLD: float = float(merged.get("BROKEN_ALBUM_PERCENTAGE_THRESHOLD", 0.20))
REQUIRED_TAGS: list[str] = merged.get("REQUIRED_TAGS", ["artist", "album", "date"])
AUTO_MOVE_DUPES: bool = bool(merged["AUTO_MOVE_DUPES"])
AI_BATCH_SIZE: int = int(merged.get("AI_BATCH_SIZE", 10))
FFPROBE_POOL_SIZE: int = int(merged.get("FFPROBE_POOL_SIZE", 4))
# Cross-library dedupe configuration
CROSS_LIBRARY_DEDUPE = _parse_bool(os.getenv("CROSS_LIBRARY_DEDUPE", "true"))

# Number of sample tracks per Plex mount to verify; override with env CROSSCHECK_SAMPLES
CROSSCHECK_SAMPLES = int(os.getenv("CROSSCHECK_SAMPLES", 20))

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

# (7) Export as module‑level constants ----------------------------------------
PLEX_HOST      = merged["PLEX_HOST"]
PLEX_TOKEN     = merged["PLEX_TOKEN"]
SECTION_IDS    = SECTION_IDS
SECTION_ID     = SECTION_IDS[0] if SECTION_IDS else 0
PATH_MAP       = merged["PATH_MAP"]
import multiprocessing
threads_env = os.getenv("SCAN_THREADS", "auto").strip().lower()
if threads_env in ("auto", "", "all"):
    SCAN_THREADS = multiprocessing.cpu_count()
else:
    try:
        SCAN_THREADS = max(1, int(threads_env))
    except ValueError:
        print(f"⚠️ Invalid SCAN_THREADS value: '{threads_env}', defaulting to 1")
        SCAN_THREADS = 1
LOG_LEVEL      = merged["LOG_LEVEL"]
AI_PROVIDER    = merged["AI_PROVIDER"]
OPENAI_API_KEY = merged["OPENAI_API_KEY"]
OPENAI_MODEL   = merged["OPENAI_MODEL"]
ANTHROPIC_API_KEY = merged["ANTHROPIC_API_KEY"]
GOOGLE_API_KEY = merged["GOOGLE_API_KEY"]
OLLAMA_URL     = merged["OLLAMA_URL"]
DISCORD_WEBHOOK = merged["DISCORD_WEBHOOK"]

#
# State and cache DB always live in the config directory
STATE_DB_FILE = CONFIG_DIR / "state.db"
CACHE_DB_FILE = CONFIG_DIR / "cache.db"

# File-format preference order (can be overridden in config.json)
FORMAT_PREFERENCE = conf.get(
    "FORMAT_PREFERENCE",
    ["dsf","aif","aiff","wav","flac","m4a","mp4","m4b","m4p","aifc","ogg","opus","mp3","wma"]
)

 # Optional external log file (rotates @ 5 MB × 3)
LOG_FILE = os.getenv("LOG_FILE", str(CONFIG_DIR / "pmda.log"))

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

logging.info("Config CROSS_LIBRARY_DEDUPE = %s (source: %s)", CROSS_LIBRARY_DEDUPE, "env" if "CROSS_LIBRARY_DEDUPE" in os.environ else "default")
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

# --- Resolve a working OpenAI model (with price-aware fallbacks) -------------
RESOLVED_MODEL = OPENAI_MODEL
# Param style for the resolved model: "mct" -> max_completion_tokens, "mt" -> max_tokens
RESOLVED_PARAM_STYLE = "mct"

def _probe_model(model_name: str) -> str | None:
    """Return param style ("mct" or "mt") if a 1-line ping works, else None.
    Tries `max_completion_tokens` first; on "unsupported_parameter" falls back to `max_tokens`.
    """
    if not (OPENAI_API_KEY and openai_client):
        return None
    # Try with max_completion_tokens
    try:
        openai_client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": "ping"}],
            max_completion_tokens=8,
            stop=["\n"],
        )
        return "mct"
    except Exception as e:
        msg = str(e)
        logging.debug("Model probe (mct) failed for %s: %s", model_name, msg)
        if "max_completion_tokens" not in msg and "unsupported_parameter" not in msg.lower():
            # Some other hard failure; still try mt just in case
            pass
    # Try legacy max_tokens
    try:
        openai_client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=8,
            stop=["\n"],
        )
        return "mt"
    except Exception as e2:
        logging.debug("Model probe (mt) failed for %s: %s", model_name, e2)
        return None

# User-provided explicit fallbacks override everything (comma-separated)
_user_fallbacks = [m.strip() for m in os.getenv("OPENAI_MODEL_FALLBACKS", "").split(",") if m.strip()]

# Price ladders (cheapest → most expensive) so we "step up" only as needed
MODEL_LADDERS = [
    ["gpt-5-nano", "gpt-5-mini", "gpt-5"],
    ["gpt-4.1-nano", "gpt-4.1-mini", "gpt-4.1"],
    ["gpt-4o-mini", "gpt-4o"],
]

# Build candidate list: requested → next tiers upward in its ladder → user fallbacks → final safe defaults
candidates: list[str] = []
found = False
for ladder in MODEL_LADDERS:
    if OPENAI_MODEL in ladder:
        found = True
        start = ladder.index(OPENAI_MODEL)
        candidates.extend(ladder[start:])  # step up in price within ladder
        break
if not found and OPENAI_MODEL:
    candidates.append(OPENAI_MODEL)
# Append user fallbacks, then a conservative default
for m in _user_fallbacks:
    if m not in candidates:
        candidates.append(m)
for m in ("gpt-4o-mini",):
    if m not in candidates:
        candidates.append(m)

for cand in candidates:
    style = _probe_model(cand)
    if style:
        RESOLVED_MODEL = cand
        RESOLVED_PARAM_STYLE = style
        if RESOLVED_MODEL != OPENAI_MODEL:
            logging.warning("OPENAI_MODEL '%s' unavailable or unsuitable; falling back to '%s' (%s)", OPENAI_MODEL, RESOLVED_MODEL, RESOLVED_PARAM_STYLE)
        else:
            logging.info("Using requested OpenAI model '%s' (%s)", RESOLVED_MODEL, RESOLVED_PARAM_STYLE)
        break

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
    """
    music_path = Path(music_root)
    if not music_path.exists() or not music_path.is_dir() or not Path(db_file).exists():
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
def _cross_check_bindings():
    """
    Verify that every PATH_MAP binding actually resolves to real audio
    files on the host.

    • Randomly samples CROSSCHECK_SAMPLES audio files per Plex root.
    • If all samples exist under the mapped host root, the binding is valid.
    • Otherwise performs a recursive search to locate the files; if they are
      all found under the same parent folder we treat that folder as the
      correct host root and patch PATH_MAP (memory + config.json).
    • If any binding cannot be validated or repaired, the startup aborts
      with SystemExit to avoid destructive behaviour later on.
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
        raise SystemExit("Cross‑check PATH_MAP failed")

    # Apply fixes
    if updates:
        PATH_MAP.update(updates)
        conf['PATH_MAP'] = PATH_MAP
        with open(CONFIG_PATH, 'w', encoding='utf-8') as fh:
            json.dump(conf, fh, indent=2)

        msg = "\n".join(f"`{k}` → `{v}`" for k, v in updates.items())
        notify_discord_embed(
            title="🔄 PATH_MAP corrected",
            description=msg
        )
        logging.info("Updated PATH_MAP in memory and config.json")

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
    # Table for caching MusicBrainz release-group info
    cur.execute("""
        CREATE TABLE IF NOT EXISTS musicbrainz_cache (
            mbid       TEXT PRIMARY KEY,
            info_json  TEXT,
            created_at INTEGER
        )
    """)
    
    # Add indexes for faster lookups
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audio_cache_path ON audio_cache(path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mb_cache_mbid ON musicbrainz_cache(mbid)")
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
        if event.wait(timeout=300):  # 5 minute timeout
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
            raise TimeoutError(f"MusicBrainz request {request_id} timed out after 5 minutes")
    
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
    "scan_active_artists": {},        # Dict {artist_name: {"start_time": float, "total_albums": int, "albums_processed": int}}
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
    if not _parse_bool(os.getenv("DISABLE_PATH_CROSSCHECK", "false")):
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
    signatures match, or they share the same MusicBrainz release-group ID.
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

def fetch_mb_release_group_info(mbid: str) -> tuple[dict, bool]:
    """
    Fetch primary type, secondary-types, and media format summary from MusicBrainz release-group.
    Uses musicbrainzngs for proper rate-limiting and parsing.
    Returns (info_dict, cache_hit) where cache_hit is True if found in cache.
    """
    # Attempt to reuse cached MusicBrainz release-group info
    cached = get_cached_mb_info(mbid)
    if cached:
        logging.debug("[MusicBrainz RG Info] using cached info for MBID %s", mbid)
        return cached, True  # True = cache hit
    
    # Use queue for rate-limited API call
    def _fetch():
        try:
            # Query release-group with all media details
            result = musicbrainzngs.get_release_group_by_id(
                mbid,
                includes=["releases", "media"]
            )["release-group"]
            return result
        except musicbrainzngs.WebServiceError as e:
            error_msg = str(e)
            if "503" in error_msg or "rate" in error_msg.lower():
                logging.warning("[MusicBrainz] Rate limited for MBID %s, will retry after delay", mbid)
                time.sleep(1.5)  # Wait a bit longer than rate limit before retry
                try:
                    result = musicbrainzngs.get_release_group_by_id(mbid, includes=["releases", "media"])["release-group"]
                    return result
                except musicbrainzngs.WebServiceError as e2:
                    raise RuntimeError(f"MusicBrainz lookup failed for {mbid} after retry: {e2}") from None
            else:
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
def search_mb_release_group_by_metadata(artist: str, album_norm: str, tracks: set[str]) -> dict | None:
    """
    Fallback search on MusicBrainz by artist name, normalized album title, and optional track titles.
    Returns the release-group info dict or None if not found.
    """
    def _search():
        # search release-groups by artist and release title
        result = musicbrainzngs.search_release_groups(
            artist=artist,
            release=album_norm,
            limit=5,
            strict=True
        )
        logging.debug("[MusicBrainz Search] raw search response for '%s'/'%s': %s", artist, album_norm, result)
        return result.get('release-group-list', [])
    
    def _fetch_rg_details(rg_id: str):
        """Fetch release group details by ID."""
        info = musicbrainzngs.get_release_group_by_id(
            rg_id, includes=['media']
        )['release-group']
        return info
    
    try:
        # Use queue for search
        if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
            candidates = get_mb_queue().submit(f"search_{artist}_{album_norm}", _search)
        else:
            candidates = _search()
        
        # optionally refine by track count if available
        for rg in candidates:
            logging.debug("[MusicBrainz Search] candidate RG id=%s, title=%s", rg['id'], rg.get('title'))
            # fetch details for each candidate (also queued)
            try:
                rg_id = rg['id']  # Capture in local variable for closure
                if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
                    info = get_mb_queue().submit(f"rg_{rg_id}", lambda: _fetch_rg_details(rg_id))
                else:
                    info = _fetch_rg_details(rg_id)
                # compare track counts if we have track list
                mb_track_count = sum(
                    medium.get('track-count', 0)
                    for release in rg.get('release-list', [])
                    for medium in release.get('medium-list', [])
                )
                if not tracks or abs(len(tracks) - mb_track_count) <= 1:
                    # build summary
                    formats = set()
                    for release in rg.get('release-list', []):
                        for medium in release.get('medium-list', []):
                            fmt = medium.get('format')
                            qty = medium.get('track-count', 1)
                            if fmt:
                                formats.add(f"{qty}×{fmt}")
                    logging.debug("[MusicBrainz Search] selected RG info: { 'id': %s, 'primary_type': %s, 'format_summary': %s }", rg['id'], info.get('primary-type'), ', '.join(sorted(formats)))
                    result_dict = {
                        'primary_type': info.get('primary-type', ''),
                        'secondary_types': info.get('secondary-types', []),
                        'format_summary': ', '.join(sorted(formats)),
                        'id': rg['id']
                    }
                    # Cache fallback match
                    set_cached_mb_info(rg['id'], result_dict)
                    return result_dict
            except musicbrainzngs.WebServiceError:
                continue
    except Exception as e:
        logging.debug("[MusicBrainz Search Groups] failed for '%s' / '%s': %s", artist, album_norm, e)
    return None

def call_ai_provider(provider: str, model: str, system_msg: str, user_msg: str, max_tokens: int = 256) -> str:
    """
    Call the appropriate AI provider with the given messages.
    Returns the text response from the AI.
    """
    provider_lower = provider.lower()
    
    if provider_lower == "openai":
        if not openai_client:
            raise ValueError("OpenAI client not initialized")
        _kwargs = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            "stop": ["\n"],
        }
        # Try max_completion_tokens first, fallback to max_tokens
        try:
            _kwargs["max_completion_tokens"] = max_tokens
            resp = openai_client.chat.completions.create(**_kwargs)
        except Exception:
            _kwargs.pop("max_completion_tokens", None)
            _kwargs["max_tokens"] = max_tokens
            resp = openai_client.chat.completions.create(**_kwargs)
        return resp.choices[0].message.content.strip()
    
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
        # Google uses a different API structure
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
        # Ollama uses REST API
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
            # Fallback to heuristic on error (force heuristic by disabling AI temporarily)
            logging.warning(f"[AI Batch] AI failed for {artist}, falling back to heuristic")
            # Use heuristic selection directly
            from operator import itemgetter
            # Simple heuristic: prefer higher format score, then bit depth, then track count
            best_heuristic = max(editions, key=lambda e: (
                e.get('fmt_score', 0),
                e.get('bd', 0),
                len(e.get('tracks', [])),
                e.get('br', 0)
            ))
            losers = [e for e in editions if e['album_id'] != best_heuristic['album_id']]
            best_heuristic["used_ai"] = False
            best_heuristic["rationale"] = "Heuristic fallback (AI failed)"
            best_heuristic["merge_list"] = []
            return {
                "artist": artist,
                "album_id": best_heuristic["album_id"],
                "best": best_heuristic,
                "losers": losers,
                "fuzzy": fuzzy,
                "needs_ai": False,
            }
    
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_group, group): group for group in ai_groups}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                group = futures[future]
                logging.error(f"[AI Batch] Failed to process group for {group['artist']}: {e}", exc_info=True)
    
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
        f"SELECT album_id, rationale, merge_list "
        f"FROM duplicates_best "
        f"WHERE artist = ? AND album_id IN ({placeholders}) AND ai_used = 1",
        (artist, ) + ids
    )
    row = cur.fetchone()
    con.close()
    existing_list = json.loads(row[2]) if row and row[2] else []
    # Only reuse cache if no new editions have appeared
    if row and (len(existing_list) + 1) == len(editions):
        prev_id, rationale, merge_json = row
        best = next(e for e in editions if e['album_id'] == prev_id)
        best["rationale"]  = rationale
        best["merge_list"] = existing_list
        best["used_ai"]    = True
        return best

    # 2) If there is no AI cache, call AI provider when possible
    used_ai = False
    if ai_provider_ready:
        if defer_ai:
            # Return None to indicate AI call is needed (will be processed in batch)
            return None
        used_ai = True
        # Load prompt template from external file
        template = AI_PROMPT_FILE.read_text(encoding="utf-8")
        user_msg = template + "\nCandidate editions:\n"
        for idx, e in enumerate(editions):
            user_msg += (
                f"{idx}: fmt_score={e['fmt_score']}, bitdepth={e['bd']}, "
                f"tracks={len(e['tracks'])}, files={e['file_count']}, "
                f"bitrate={e['br']}, samplerate={e['sr']}, duration={e['dur']}"
            )
            # Add year and mbid tags from meta
            year = e["meta"].get("date") or e["meta"].get("originaldate") or ""
            mbid = e["meta"].get("musicbrainz_albumid","")
            user_msg += f" year={year} mbid={mbid}\n"

        # --- Append MusicBrainz release-group info to AI prompt ---
        first_mbid = editions[0].get("meta", {}).get("musicbrainz_albumid")
        if first_mbid:
            try:
                rg_info, _ = fetch_mb_release_group_info(first_mbid)
                user_msg += f"Release group info: primary_type={rg_info['primary_type']}, formats={rg_info['format_summary']}\n"
            except Exception as e:
                logging.debug("MusicBrainz lookup failed for %s: %s", first_mbid, e)

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
        # OPENAI_MODEL stores the selected model name regardless of provider
        model_to_use = OPENAI_MODEL
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
            best.update({
                "rationale":  rationale,
                "merge_list": merge_list,
                "used_ai":    True,
            })
        except Exception as e:
            logging.warning("AI failed (%s); falling back to heuristic selection", e)
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
           br, sr, bd, dur, discs, rationale, merge_list, ai_used, meta_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    Returns (artist_name, list_of_groups, album_count, stats_dict).
    stats_dict contains: {"ai_used": count, "mb_used": count, "timing": {...}}
    """
    artist_id, artist_name = args
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
            return (artist_name, [], 0, {"ai_used": 0, "mb_used": 0, "timing": timing_stats})
        while scan_is_paused.is_set() and not scan_should_stop.is_set():
            time.sleep(0.5)
        logging.info("Processing artist: %s", artist_name)
        logging.debug("[Artist %s (ID %s)] Fetching album IDs from Plex DB", artist_name, artist_id)
        logging.debug(
            "scan_artist_duplicates(): start '%s' (ID %s)", artist_name, artist_id
        )

        db_conn = plex_connect()

        # Fetch all album IDs for this artist...
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
        logging.debug("[Artist %s (ID %s)] Retrieved %d album IDs: %s", artist_name, artist_id, len(album_ids), album_ids)
        logging.debug("[Artist %s (ID %s)] Album list for scan: %s", artist_name, artist_id, album_ids)

        # Update total_albums in active tracking
        with lock:
            if artist_name in state.get("scan_active_artists", {}):
                state["scan_active_artists"][artist_name]["total_albums"] = len(album_ids)

        groups = []
        stats = {"ai_used": 0, "mb_used": 0, "timing": {}}
        if album_ids:
            groups, stats = scan_duplicates(db_conn, artist_name, album_ids)
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
        return (artist_name, groups, len(album_ids), stats)
    except Exception as e:
        logging.error("Unexpected error scanning artist %s: %s", artist_name, e, exc_info=True)
        # On error, return no groups and zero albums so scan can continue
        timing_stats["total_time"] = time.perf_counter() - artist_start_time
        return (artist_name, [], 0, {"ai_used": 0, "mb_used": 0, "timing": timing_stats})


def scan_duplicates(db_conn, artist: str, album_ids: List[int]) -> tuple[List[dict], dict]:
    global no_file_streak_global, popup_displayed, gui
    scan_start_time = time.perf_counter()
    logging.debug("[Artist %s] Starting duplicate scan for album IDs: %s", artist, album_ids)
    logging.debug("Verbose SKIP_FOLDERS: %s", SKIP_FOLDERS)
    skip_count = 0
    editions = []
    total_albums = len(album_ids)
    processed_albums = 0
    PROGRESS_STATE["total"] = total_albums
    # Track folders we've already processed to avoid duplicate editions from same folder
    seen_folders: dict[str, int] = {}  # folder_path_resolved -> album_id
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
            
            # Update current album tracking - fetching tracks
            with lock:
                if artist in state.get("scan_active_artists", {}):
                    album_title_str = album_title(db_conn, aid) or f"Album {aid}"
                    state["scan_active_artists"][artist]["current_album"] = {
                        "album_id": aid,
                        "album_title": album_title_str,
                        "status": "fetching_tracks",
                        "status_details": ""
                    }
            
            tr = get_tracks(db_conn, aid)
            if not tr:
                continue
            
            # Update: analyzing format
            with lock:
                if artist in state.get("scan_active_artists", {}):
                    state["scan_active_artists"][artist]["current_album"]["status"] = "analyzing_format"
                    state["scan_active_artists"][artist]["current_album"]["status_details"] = "analyzing audio format"
            
            folder = first_part_path(db_conn, aid)
            if not folder:
                continue
            # Skip albums in configured skip folders (path-aware)
            logging.debug("Checking album %s at folder %s against skip prefixes %s", aid, folder, SKIP_FOLDERS)
            folder_resolved = Path(folder).resolve()
            folder_str_resolved = str(folder_resolved)
            
            # SAFETY: Check if we've already processed this exact folder for a different album_id
            # This prevents duplicate Plex entries pointing to the same folder from creating multiple editions
            if folder_str_resolved in seen_folders:
                existing_album_id = seen_folders[folder_str_resolved]
                logging.warning(
                    "[Artist %s] Album ID %d points to the same folder as album ID %d: %s. "
                    "This indicates duplicate Plex album entries. Skipping album ID %d to avoid false duplicates.",
                    artist, aid, existing_album_id, folder_str_resolved, aid
                )
                skip_count += 1
                continue
            
            # Mark this folder as seen
            seen_folders[folder_str_resolved] = aid
            
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
            album_norm_value = norm_album(title_raw)
            
            # Update: album title extracted
            with lock:
                if artist in state.get("scan_active_artists", {}):
                    state["scan_active_artists"][artist]["current_album"]["album_title"] = title_raw or plex_title or f"Album {aid}"

            editions.append({
                'album_id':  aid,
                'title_raw': title_raw,
                'album_norm': album_norm_value,
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
            # Update: fetching MusicBrainz ID
            with lock:
                if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                    state["scan_active_artists"][artist]["current_album"]["status"] = "fetching_mb_id"
                    state["scan_active_artists"][artist]["current_album"]["status_details"] = "fetching MusicBrainz ID"
            
            meta = e.get('meta', {})
            rg_info = None
            mbid_found = None
            mbid_type = None
            for tag in id_tags:
                mbid = meta.get(tag)
                if not mbid:
                    continue
                try:
                    mb_cache_hit = False
                    if tag == 'musicbrainz_releasegroupid':
                        # direct lookup of release-group
                        rg_info, mb_cache_hit = fetch_mb_release_group_info(mbid)
                        mbid_found = mbid
                        mbid_type = 'release-group'
                    else:
                        # lookup release to derive its release-group ID
                        def _fetch_release():
                            return musicbrainzngs.get_release_by_id(mbid, includes=['release-group'])['release']
                        
                        if MB_QUEUE_ENABLED and USE_MUSICBRAINZ:
                            rel = get_mb_queue().submit(f"rel_{mbid}", _fetch_release)
                        else:
                            rel = _fetch_release()
                        rgid = rel['release-group']['id']
                        rg_info, mb_cache_hit = fetch_mb_release_group_info(rgid)
                        mbid_found = rgid  # Store the release-group ID (more useful for comparison)
                        mbid_type = 'release-group'
                    e['mb_cache_hit'] = mb_cache_hit  # Track MB cache hit for this edition
                    
                    # Update: MusicBrainz ID found
                    with lock:
                        if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                            cache_text = " (cached)" if mb_cache_hit else ""
                            state["scan_active_artists"][artist]["current_album"]["status"] = "searching_mb"
                            state["scan_active_artists"][artist]["current_album"]["status_details"] = f"MusicBrainz ID fetched{cache_text}"
                    
                    logging.debug("[Artist %s] Edition %s RG info (via %s %s): %s", artist, e['album_id'], tag, mbid, rg_info)
                    break
                except Exception as exc:
                    logging.debug("[Artist %s] MusicBrainz lookup failed for %s (%s): %s", artist, tag, mbid, exc)
            if rg_info:
                e['rg_info_source'] = tag
                e['rg_info'] = rg_info
                if mbid_found:
                    e['musicbrainz_id'] = mbid_found
                    e['musicbrainz_type'] = mbid_type
            # fallback: search by metadata if no ID tag yielded results
            album_norm = e['album_norm']
            tracks = {t.title for t in e['tracks']}
            if not rg_info:
                # Update: searching MusicBrainz
                with lock:
                    if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                        state["scan_active_artists"][artist]["current_album"]["status"] = "searching_mb"
                        state["scan_active_artists"][artist]["current_album"]["status_details"] = "searching MusicBrainz"
                
                rg_info = search_mb_release_group_by_metadata(artist, album_norm, tracks)
                if rg_info:
                    e['rg_info_source'] = 'fallback'
                    e['rg_info'] = rg_info
                    # Extract MBID from rg_info if available (it should have 'id' field)
                    if isinstance(rg_info, dict) and 'id' in rg_info:
                        e['musicbrainz_id'] = rg_info['id']
                        e['musicbrainz_type'] = 'release-group'
                        # Check if this MBID was in cache
                        mbid = rg_info['id']
                        cached_mb = get_cached_mb_info(mbid)
                        e['mb_cache_hit'] = cached_mb is not None
                        
                        # Update: MusicBrainz found via search
                        with lock:
                            if artist in state.get("scan_active_artists", {}) and e['album_id'] == state["scan_active_artists"][artist].get("current_album", {}).get("album_id"):
                                cache_text = " (cached)" if cached_mb else ""
                                state["scan_active_artists"][artist]["current_album"]["status_details"] = f"MusicBrainz found{cache_text}"
                    
                    logging.debug("[Artist %s] Edition %s RG info (search fallback): %s", artist, e['album_id'], rg_info)
                else:
                    logging.debug("[Artist %s] No RG info found via search for '%s'", artist, album_norm)
                    e['mb_cache_hit'] = False
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
            return [], {"ai_used": 0, "mb_used": 0}
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
                return [], {"ai_used": 0, "mb_used": 0}
            # Below threshold, do not show repeated popups -- let scan continue or fail silently
            return [], {"ai_used": 0, "mb_used": 0}
    for e in editions:
        logging.debug(
            f"[Artist {artist}] Edition {e['album_id']}: "
            f"norm='{e['album_norm']}', tracks={len(e['tracks'])}, dur_ms={e['dur']}, "
            f"files={e['file_count']}, fmt_score={e['fmt_score']}, "
            f"br={e['br']}, sr={e['sr']}, bd={e['bd']}"
        )
    # --- First pass: group by album_norm, with classical disambiguation ---
    from collections import defaultdict
    # initial grouping by normalized title
    raw_groups: dict[str, list[dict]] = defaultdict(list)
    for e in editions:
        raw_groups[e['album_norm']].append(e)
    
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

        if not editions_share_confident_signal(ed_list):
            logging.debug(
                "[Artist %s] Skipping low-confidence exact group %s",
                artist,
                [e['album_id'] for e in ed_list]
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

    # Filter out editions already grouped exactly so fuzzy pass only sees the rest
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
        
        # Only perform fuzzy grouping via AI; skip if no API key
        if not OPENAI_API_KEY:
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
    # Remove groups where every loser was discarded (e.g. only one valid edition)
    out = [g for g in out if g.get("losers")]
    
    # Calculate total scan time
    scan_total_time = time.perf_counter() - scan_start_time
    
    # Compile stats with timing
    stats = {
        "ai_used": ai_used_count,
        "mb_used": mb_used_count,
        "audio_cache_hits": sum(1 for e in editions if e.get('audio_cache_hit', False)),
        "audio_cache_misses": sum(1 for e in editions if not e.get('audio_cache_hit', True)),
        "mb_cache_hits": sum(1 for e in editions if e.get('mb_cache_hit', False)),
        "mb_cache_misses": sum(1 for e in editions if 'mb_cache_hit' in e and not e.get('mb_cache_hit', True)),
        "duplicate_groups_count": len(out),
        "total_duplicates_count": sum(len(g.get("losers", [])) for g in out),
        "broken_albums_count": sum(1 for e in editions if e.get('is_broken', False)),
        "albums_without_mb_id": sum(1 for e in editions if not e.get('meta', {}).get('musicbrainz_releasegroupid') and not e.get('meta', {}).get('musicbrainz_releaseid')),
        "albums_without_artist_mb_id": sum(1 for e in editions if not e.get('meta', {}).get('musicbrainz_albumartistid') and not e.get('meta', {}).get('musicbrainz_artistid')),
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
    
    # Calculate albums without complete tags/images
    for e in editions:
        meta = e.get('meta', {})
        # Check for complete tags
        tag_checks = {
            'artist': bool(meta.get('artist') or meta.get('albumartist')),
            'album': bool(meta.get('album')),
            'date': bool(meta.get('date') or meta.get('originaldate')),
            'genre': bool(meta.get('genre')),
            'year': bool(meta.get('year') or meta.get('date')),
        }
        missing_required = [tag for tag in REQUIRED_TAGS if not tag_checks.get(tag.lower(), False)]
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
    
    # Count albums with missing tags/images
    albums_without_mb_id = 0
    albums_without_artist_mb_id = 0
    albums_without_complete_tags = 0
    albums_without_artist_image = 0
    albums_without_album_image = 0
    
    for e in editions:
        meta = e.get('meta', {})
        # Check MusicBrainz IDs
        if not meta.get('musicbrainz_releasegroupid') and not meta.get('musicbrainz_releaseid'):
            albums_without_mb_id += 1
        if not meta.get('musicbrainz_albumartistid') and not meta.get('musicbrainz_artistid'):
            albums_without_artist_mb_id += 1
        
        # Check for complete tags (using configurable required tags)
        tag_checks = {
            'artist': bool(meta.get('artist') or meta.get('albumartist')),
            'album': bool(meta.get('album')),
            'date': bool(meta.get('date') or meta.get('originaldate')),
            'genre': bool(meta.get('genre')),
            'year': bool(meta.get('year') or meta.get('date')),
        }
        # Check if all required tags are present
        missing_required = [tag for tag in REQUIRED_TAGS if not tag_checks.get(tag.lower(), False)]
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
    return out, stats

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

    # 2) Re-insert all scan results
    for artist, groups in scan_results.items():
        for g in groups:
            best = g['best']
            # Best edition
            cur.execute("""
                INSERT OR IGNORE INTO duplicates_best
                  (artist, album_id, title_raw, album_norm, folder,
                   fmt_text, br, sr, bd, dur, discs, rationale, merge_list, ai_used, meta_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                int(best.get('used_ai', False)),
                json.dumps(best.get('meta', {})),
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
    cur.execute(
        """
        SELECT artist, album_id, title_raw, album_norm, folder,
               fmt_text, br, sr, bd, dur, discs, rationale, merge_list, ai_used, meta_json
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
        meta_json,
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
            "meta": json.loads(meta_json or "{}"),
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
    logging.debug(f"background_scan(): opening Plex DB at {PLEX_DB_FILE}")
    start_time = time.perf_counter()

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
        artists = db_conn.execute(
            f"SELECT id, title FROM metadata_items "
            f"WHERE metadata_type=8 AND library_section_id IN ({placeholders})",
            SECTION_IDS,
        ).fetchall()
        total_artists = len(artists)

        # --- Discord: announce scan start ---
        notify_discord_embed(
            title="🔄 PMDA scan started",
            description=(
                f"Scanning {len(artists)} artists / {total_albums} albums… "
                "Buckle up!"
            )
        )

        logging.debug(
            f"background_scan(): {len(artists)} artists, {total_albums} albums total"
        )

        # Reset live state
        start_time = time.time()
        with lock:
            state.update(scanning=True, scan_progress=0, scan_total=total_albums)
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
        
        # Create scan history entry
        import sqlite3
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
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

        clear_db_on_new_scan()  # wipe previous duplicate tables

        all_results, futures = {}, []
        import concurrent.futures
        future_to_albums: dict[concurrent.futures.Future, int] = {}
        future_to_artist: dict[concurrent.futures.Future, str] = {}
        with ThreadPoolExecutor(max_workers=SCAN_THREADS) as executor:
            for artist_id, artist_name in artists:
                album_cnt = db_conn.execute(
                    "SELECT COUNT(*) FROM metadata_items WHERE metadata_type=9 AND parent_id=?",
                    (artist_id,)
                ).fetchone()[0]
                # Track artist before submitting
                with lock:
                    state["scan_active_artists"][artist_name] = {
                        "start_time": time.time(),
                        "total_albums": album_cnt,
                        "albums_processed": 0
                    }
                fut = executor.submit(scan_artist_duplicates, (artist_id, artist_name))
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
                    if len(result) == 4:
                        artist_name, groups, _, stats = result
                    else:
                        # Backward compatibility: old format without stats
                        artist_name, groups, _ = result
                        stats = {"ai_used": 0, "mb_used": 0}
                except Exception as e:
                    logging.exception("Worker crash for artist %s: %s", artist_name, e)
                    worker_errors.put((artist_name, str(e)))
                    groups = []
                    stats = {"ai_used": 0, "mb_used": 0}
                finally:
                    with lock:
                        state["scan_progress"] += album_cnt
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

        # Collect all groups requiring AI processing
        ai_groups_to_process = []
        ai_group_positions = {}  # Track position of each AI group for replacement
        for artist_name, groups in all_results.items():
            for i, group in enumerate(groups):
                if group.get("needs_ai", False):
                    ai_groups_to_process.append(group)
                    key = (artist_name, tuple(sorted(e['album_id'] for e in group.get("editions", []))))
                    ai_group_positions[key] = (artist_name, i)
        
        # Process AI groups in parallel batch
        if ai_groups_to_process and ai_provider_ready:
            logging.info(f"Processing {len(ai_groups_to_process)} groups requiring AI in parallel batch (max {AI_BATCH_SIZE} concurrent)...")
            with lock:
                state["scan_active_artists"]["_ai_batch"] = {
                    "start_time": time.time(),
                    "total_groups": len(ai_groups_to_process),
                    "groups_processed": 0
                }
            
            ai_results = process_ai_groups_batch(ai_groups_to_process, max_workers=AI_BATCH_SIZE)
            
            # Update all_results with AI-processed groups
            for ai_result in ai_results:
                artist_name = ai_result["artist"]
                result_edition_ids = tuple(sorted(e['album_id'] for e in [ai_result["best"]] + ai_result["losers"]))
                key = (artist_name, result_edition_ids)
                
                if key in ai_group_positions:
                    # Replace the group at the tracked position
                    target_artist, target_index = ai_group_positions[key]
                    if target_artist in all_results and target_index < len(all_results[target_artist]):
                        all_results[target_artist][target_index] = ai_result
                    else:
                        # Position invalid, append instead
                        if target_artist not in all_results:
                            all_results[target_artist] = []
                        all_results[target_artist].append(ai_result)
                else:
                    # Group not found in tracking, append to artist's groups
                    if artist_name not in all_results:
                        all_results[artist_name] = []
                    all_results[artist_name].append(ai_result)
            
            # Update state
            ai_batch_time = time.perf_counter() - (state.get("scan_active_artists", {}).get("_ai_batch", {}).get("start_time", time.perf_counter()))
            with lock:
                if "_ai_batch" in state.get("scan_active_artists", {}):
                    del state["scan_active_artists"]["_ai_batch"]
                state["scan_ai_used_count"] += len(ai_results)
                # Update duplicates in state
                state["duplicates"] = all_results
            logging.info(
                f"AI batch processing completed: {len(ai_results)}/{len(ai_groups_to_process)} groups processed successfully "
                f"in {ai_batch_time:.2f}s (avg {ai_batch_time/max(len(ai_groups_to_process), 1):.2f}s per group)"
            )
        
        # Calculate missing albums count (compare Plex albums with MusicBrainz)
        # This is a simplified version - for now, we'll set it to 0 as calculating it requires
        # MusicBrainz API calls for each artist which could be slow during scan
        # This can be implemented later as a post-scan analysis or background task
        missing_albums_total = 0
        with lock:
            state["scan_missing_albums_count"] = missing_albums_total
        
        # Persist what we've found so far (even if some artists failed)
        save_scan_to_db(all_results)
        
        # Auto-move dupes if enabled
        if AUTO_MOVE_DUPES and all_results:
            logging.info("Auto-moving dupes enabled, starting automatic deduplication...")
            background_dedupe(all_results)

    finally:
        # Make absolutely sure we leave the UI in a consistent state
        end_time = time.time()
        scan_id = None
        with lock:
            state["scan_progress"] = state["scan_total"]  # force 100 % before stopping
            state["scanning"] = False
            scan_id = state.get("scan_id")
            start_time = state.get("scan_start_time", end_time)
        
        # Update scan history entry
        if scan_id:
            import sqlite3
            con = sqlite3.connect(str(STATE_DB_FILE))
            cur = con.cursor()
            duration = int(end_time - start_time) if start_time else None
            with lock:
                duplicates_found = sum(len(groups) for groups in all_results.values())
                artists_processed = state.get("scan_artists_processed", 0)
                ai_used_count = state.get("scan_ai_used_count", 0)
                mb_used_count = state.get("scan_mb_used_count", 0)
                space_saved = get_stat("space_saved")
                albums_moved = get_stat("removed_dupes")
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
            
            status = 'cancelled' if scan_should_stop.is_set() else 'completed'
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
                    albums_without_artist_mb_id = ?
                WHERE scan_id = ?
            """, (
                end_time,
                duration,
                duplicates_found,
                artists_processed,
                ai_used_count,
                mb_used_count,
                space_saved,
                albums_moved,
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
                scan_id
            ))
            con.commit()
            con.close()
        
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
    notify_discord(
        f"🟢 Deduplication finished: {removed_count} duplicate folders moved, "
        f"{total_moved}  MB reclaimed."
    )
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
    Move each "loser" folder out to DUPE_ROOT, delete metadata in Plex,
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

        logging.info("Moving dupe: %s  →  %s", src_folder, dst)
        logging.debug("perform_dedupe(): moving %s → %s", src_folder, dst)
        try:
            safe_move(str(src_folder), str(dst))
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
            import sqlite3
            try:
                con = sqlite3.connect(str(STATE_DB_FILE))
                cur = con.cursor()
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
            "thumb_data": cover_data
        })

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
            best = g["best"]
            folder_path = path_for_fs_access(Path(best["folder"]))
            best_fmt = best.get("fmt_text", get_primary_format(folder_path))
            formats = [best_fmt] + [
                loser.get("fmt", get_primary_format(path_for_fs_access(Path(loser["folder"]))))
                for loser in g["losers"]
            ]
            display_title = best["album_norm"].title()
            size_bytes = safe_folder_size(folder_path)
            size_mb = size_bytes // (1024 * 1024)
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
                "used_ai": best.get("used_ai", False),
                "size": size_bytes,
                "size_mb": size_mb,
                "track_count": track_count,
                "path": str(folder_path),
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
    with lock:
        if not state["scanning"]:
            state.update(scanning=True, scan_progress=0, scan_total=0)
            logging.debug("start_scan(): launching background_scan() thread")
            threading.Thread(target=background_scan, daemon=True).start()

@app.route("/scan/start", methods=["POST"])
def start_scan():
    r = _requires_config()
    if r is not None:
        return r
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
    Clear all scan results from the database (duplicates_best, duplicates_loser).
    Optionally clear audio and MusicBrainz caches.
    """
    import sqlite3
    data = request.get_json() or {}
    clear_audio_cache = data.get("clear_audio_cache", False)
    clear_mb_cache = data.get("clear_mb_cache", False)
    
    try:
        # Clear scan results
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        cur.execute("DELETE FROM duplicates_loser")
        deleted_losers = cur.rowcount
        cur.execute("DELETE FROM duplicates_best")
        deleted_best = cur.rowcount
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
                "duplicates_loser": deleted_losers
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
            con.commit()
            con.close()
            result["cleared"]["musicbrainz_cache"] = mb_cache_deleted
            result["message"] += f", {mb_cache_deleted} MusicBrainz cache entries cleared"
        
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
    try:
        for sid in section_ids:
            part = _discover_path_map(host, token, sid)
            paths.update(part)
        return jsonify({"success": True, "paths": paths})
    except Exception as e:
        logging.warning("Autodetect paths failed: %s", e)
        return jsonify({"success": False, "paths": {}, "message": str(e)})


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
    return jsonify({
        "success": True,
        "host_root": host_root,
        "result": result,
    })


@app.post("/api/paths/verify")
def api_paths_verify():
    """
    Verify PATH_MAP bindings by sampling tracks from the Plex DB and checking file existence.
    Body: { PATH_MAP?, PLEX_DB_PATH?, CROSSCHECK_SAMPLES? }. Returns list of { plex_root, host_root, status, samples_checked, message }.
    Does not modify config.
    """
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
    results = _run_path_verification(path_map, db_file, samples or CROSSCHECK_SAMPLES)
    if results is None:
        return jsonify({"success": False, "results": [], "message": "Path verification failed"}), 500
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
    """Return list of OpenAI model IDs fetched directly from OpenAI API.
    Requires OPENAI_API_KEY in POST body or in config.
    Returns only chat completion models (gpt-*) available for the provided API key."""
    # Try to get key from POST body first (for testing before saving), then from config
    # Check if data was passed from api_ai_models via Flask g context
    from flask import g
    data = getattr(g, 'ai_models_request_data', None) or request.get_json(silent=True) or {}
    key = (data.get("OPENAI_API_KEY") or "").strip() or OPENAI_API_KEY
    
    if not key:
        return jsonify({"error": "OPENAI_API_KEY is required"}), 400
    
    # Validate key format
    if not key.startswith("sk-"):
        return jsonify({"error": "Invalid API key format. OpenAI keys start with 'sk-'"}), 400
    
    try:
        client = OpenAI(api_key=key)
        # Fetch all models from OpenAI API
        models_response = client.models.list()
        
        # Filter for chat completion models only
        # OpenAI chat models typically start with "gpt-" and are in the "chat" category
        available_models = []
        for model in models_response.data:
            model_id = model.id
            # Only include gpt-* models (chat completion models)
            # Exclude instruct models, vision-only models, and other non-chat models
            if (model_id.startswith("gpt-") and 
                "instruct" not in model_id.lower() and
                "vision" not in model_id.lower() and
                "embedding" not in model_id.lower()):
                available_models.append(model_id)
        
        if not available_models:
            logging.warning("OpenAI API returned no chat completion models")
            return jsonify({"error": "No chat completion models available for this API key"}), 404
        
        # Sort models: newer/better models first
        def model_sort_key(name: str) -> tuple:
            # Priority order: gpt-5 > gpt-4.1 > gpt-4o > gpt-4 > gpt-3.5
            # Within each tier, sort by name (nano < mini < base)
            if name.startswith("gpt-5"):
                tier = 0
            elif name.startswith("gpt-4.1"):
                tier = 1
            elif name.startswith("gpt-4o"):
                tier = 2
            elif name.startswith("gpt-4"):
                tier = 3
            elif name.startswith("gpt-3.5"):
                tier = 4
            else:
                tier = 5
            return (tier, name)
        
        available_models.sort(key=model_sort_key)
        logging.info("Fetched %d chat completion models from OpenAI API", len(available_models))
        return jsonify(available_models)
        
    except Exception as e:
        error_msg = str(e)
        logging.error("Failed to fetch models from OpenAI API: %s", error_msg)
        
        # Handle specific error types
        if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg or "unauthorized" in error_msg.lower():
            return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
        elif "insufficient_quota" in error_msg.lower() or "quota" in error_msg.lower():
            return jsonify({"error": "API key has insufficient quota. Please check your OpenAI account billing."}), 402
        elif "connection" in error_msg.lower() or "timeout" in error_msg.lower() or "network" in error_msg.lower():
            return jsonify({"error": "Connection to OpenAI API failed. Please check your internet connection."}), 503
        else:
            return jsonify({"error": f"Failed to fetch models: {error_msg}"}), 500


@app.post("/api/anthropic/models")
def api_anthropic_models():
    """Return list of Anthropic model IDs available for the provided API key."""
    from flask import g
    data = getattr(g, 'ai_models_request_data', None) or request.get_json(silent=True) or {}
    key = (data.get("ANTHROPIC_API_KEY") or "").strip() or ANTHROPIC_API_KEY
    
    if not key:
        return jsonify({"error": "ANTHROPIC_API_KEY is required"}), 400
    
    if not anthropic:
        return jsonify({"error": "Anthropic SDK not installed. Please install anthropic package."}), 500
    
    try:
        client = anthropic.Anthropic(api_key=key)
        # Anthropic has a fixed list of models, fetch available ones
        # Test with a simple message to validate the key
        try:
            client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=1,
                messages=[{"role": "user", "content": "test"}]
            )
        except anthropic.APIError as e:
            if e.status_code == 401:
                return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
            elif e.status_code == 402:
                return jsonify({"error": "API key has insufficient quota. Please check your Anthropic account billing."}), 402
            # If it's not auth/quota, continue to return available models
        
        # Anthropic models list (as of 2024)
        available_models = [
            "claude-3-5-sonnet-20241022",
            "claude-3-5-sonnet-20240620",
            "claude-3-opus-20240229",
            "claude-3-sonnet-20240229",
            "claude-3-haiku-20240307",
        ]
        
        logging.info("Fetched %d Anthropic models", len(available_models))
        return jsonify(available_models)
        
    except Exception as e:
        error_msg = str(e)
        logging.error("Failed to fetch Anthropic models: %s", error_msg)
        
        if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg or "unauthorized" in error_msg.lower():
            return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
        elif "connection" in error_msg.lower() or "timeout" in error_msg.lower() or "network" in error_msg.lower():
            return jsonify({"error": "Connection to Anthropic API failed. Please check your internet connection."}), 503
        else:
            return jsonify({"error": f"Failed to fetch models: {error_msg}"}), 500


@app.post("/api/google/models")
def api_google_models():
    """Return list of Google Gemini model IDs available for the provided API key."""
    from flask import g
    data = getattr(g, 'ai_models_request_data', None) or request.get_json(silent=True) or {}
    key = (data.get("GOOGLE_API_KEY") or "").strip() or GOOGLE_API_KEY
    
    if not key:
        return jsonify({"error": "GOOGLE_API_KEY is required"}), 400
    
    if not genai:
        return jsonify({"error": "Google Generative AI SDK not installed. Please install google-generativeai package."}), 500
    
    try:
        genai.configure(api_key=key)
        
        # Test the key by listing models
        try:
            models_list = genai.list_models()
        except Exception as e:
            error_msg = str(e)
            if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg or "unauthorized" in error_msg.lower():
                return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
            raise
        
        # Filter for chat completion models (gemini-*)
        available_models = []
        for model in models_list:
            model_name = model.name
            # Only include gemini models that support generateContent
            if "gemini" in model_name.lower() and "generateContent" in str(model.supported_generation_methods):
                # Extract model ID from full name (e.g., "models/gemini-pro" -> "gemini-pro")
                model_id = model_name.split("/")[-1] if "/" in model_name else model_name
                if model_id not in available_models:
                    available_models.append(model_id)
        
        if not available_models:
            logging.warning("Google API returned no chat completion models")
            return jsonify({"error": "No chat completion models available for this API key"}), 404
        
        # Sort models: newer/better models first
        def model_sort_key(name: str) -> tuple:
            if "gemini-2.0" in name:
                tier = 0
            elif "gemini-1.5-pro" in name:
                tier = 1
            elif "gemini-1.5-flash" in name:
                tier = 2
            elif "gemini-pro" in name:
                tier = 3
            else:
                tier = 4
            return (tier, name)
        
        available_models.sort(key=model_sort_key)
        logging.info("Fetched %d Google Gemini models", len(available_models))
        return jsonify(available_models)
        
    except Exception as e:
        error_msg = str(e)
        logging.error("Failed to fetch Google models: %s", error_msg)
        
        if "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower() or "401" in error_msg or "unauthorized" in error_msg.lower():
            return jsonify({"error": "Invalid API key. Please check your key and try again."}), 401
        elif "connection" in error_msg.lower() or "timeout" in error_msg.lower() or "network" in error_msg.lower():
            return jsonify({"error": "Connection to Google API failed. Please check your internet connection."}), 503
        else:
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
    
    path_map = getattr(sys.modules[__name__], "PATH_MAP", {})
    section_ids = getattr(sys.modules[__name__], "SECTION_IDS", [])
    skip_folders = getattr(sys.modules[__name__], "SKIP_FOLDERS", [])
    
    # Check if settings exist in DB (wizard was completed)
    has_settings = _has_settings_in_db()
    # Wizard should not show if settings exist in DB OR if Plex is configured
    configured = has_settings or PLEX_CONFIGURED
    
    # Load SECTION_IDS from SQLite if available
    section_ids_str = _get_config_from_db("SECTION_IDS")
    if section_ids_str:
        try:
            section_ids = [int(x.strip()) for x in str(section_ids_str).split(",") if x.strip()]
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
        "PLEX_DB_PATH": get_setting("PLEX_DB_PATH", merged.get("PLEX_DB_PATH", "/database")),
        "PLEX_DB_FILE": "com.plexapp.plugins.library.db",
        "SECTION_IDS": ",".join(str(s) for s in section_ids),
        "PATH_MAP": path_map,
        "DUPE_ROOT": str(DUPE_ROOT),
        "PMDA_CONFIG_DIR": str(CONFIG_DIR),
        "MUSIC_PARENT_PATH": get_setting("MUSIC_PARENT_PATH", merged.get("MUSIC_PARENT_PATH", "")),
        "SCAN_THREADS": get_setting("SCAN_THREADS", SCAN_THREADS if isinstance(SCAN_THREADS, int) else "auto"),
        "SKIP_FOLDERS": get_setting("SKIP_FOLDERS", ",".join(skip_folders) if isinstance(skip_folders, list) else (skip_folders or "")),
        "CROSS_LIBRARY_DEDUPE": get_setting("CROSS_LIBRARY_DEDUPE", CROSS_LIBRARY_DEDUPE),
        "CROSSCHECK_SAMPLES": get_setting("CROSSCHECK_SAMPLES", CROSSCHECK_SAMPLES),
        "FORMAT_PREFERENCE": get_setting("FORMAT_PREFERENCE", FORMAT_PREFERENCE),
        "AI_PROVIDER": get_setting("AI_PROVIDER", AI_PROVIDER),
        "OPENAI_API_KEY": get_setting("OPENAI_API_KEY", OPENAI_API_KEY),
        "OPENAI_MODEL": get_setting("OPENAI_MODEL", OPENAI_MODEL),
        "OPENAI_MODEL_FALLBACKS": get_setting("OPENAI_MODEL_FALLBACKS", os.getenv("OPENAI_MODEL_FALLBACKS", "")),
        "ANTHROPIC_API_KEY": get_setting("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY),
        "GOOGLE_API_KEY": get_setting("GOOGLE_API_KEY", GOOGLE_API_KEY),
        "OLLAMA_URL": get_setting("OLLAMA_URL", OLLAMA_URL),
        "USE_MUSICBRAINZ": get_setting("USE_MUSICBRAINZ", USE_MUSICBRAINZ),
        "MUSICBRAINZ_EMAIL": get_setting("MUSICBRAINZ_EMAIL", MUSICBRAINZ_EMAIL),
        "LIDARR_URL": get_setting("LIDARR_URL", merged.get("LIDARR_URL", "")),
        "LIDARR_API_KEY": get_setting("LIDARR_API_KEY", merged.get("LIDARR_API_KEY", "")),
        "AUTOBRR_URL": get_setting("AUTOBRR_URL", merged.get("AUTOBRR_URL", "")),
        "AUTOBRR_API_KEY": get_setting("AUTOBRR_API_KEY", merged.get("AUTOBRR_API_KEY", "")),
        "AUTO_FIX_BROKEN_ALBUMS": get_setting("AUTO_FIX_BROKEN_ALBUMS", AUTO_FIX_BROKEN_ALBUMS),
        "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD": get_setting("BROKEN_ALBUM_CONSECUTIVE_THRESHOLD", BROKEN_ALBUM_CONSECUTIVE_THRESHOLD),
        "BROKEN_ALBUM_PERCENTAGE_THRESHOLD": get_setting("BROKEN_ALBUM_PERCENTAGE_THRESHOLD", BROKEN_ALBUM_PERCENTAGE_THRESHOLD),
        "REQUIRED_TAGS": get_setting("REQUIRED_TAGS", REQUIRED_TAGS),
        "DISCORD_WEBHOOK": get_setting("DISCORD_WEBHOOK", DISCORD_WEBHOOK),
        "LOG_LEVEL": get_setting("LOG_LEVEL", LOG_LEVEL),
        "LOG_FILE": get_setting("LOG_FILE", LOG_FILE),
        "AUTO_MOVE_DUPES": get_setting("AUTO_MOVE_DUPES", AUTO_MOVE_DUPES),
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


@app.put("/api/config")
def api_config_put():
    """Persist configuration updates to SQLite (single source of truth).
    Only updates keys present in the request; existing values in SQLite are preserved.
    """
    data = request.get_json() or {}
    allowed = {
        "PLEX_HOST", "PLEX_TOKEN", "PLEX_DB_PATH", "SECTION_IDS", "PATH_MAP",
        "DUPE_ROOT", "PMDA_CONFIG_DIR", "MUSIC_PARENT_PATH",
        "SCAN_THREADS", "LOG_LEVEL", "LOG_FILE", "AI_PROVIDER", "OPENAI_API_KEY", "OPENAI_MODEL",
        "OPENAI_MODEL_FALLBACKS", "ANTHROPIC_API_KEY", "GOOGLE_API_KEY", "OLLAMA_URL",
        "DISCORD_WEBHOOK", "USE_MUSICBRAINZ", "MUSICBRAINZ_EMAIL",
        "SKIP_FOLDERS", "CROSS_LIBRARY_DEDUPE", "CROSSCHECK_SAMPLES",
        "FORMAT_PREFERENCE", "AUTO_MOVE_DUPES",
        "LIDARR_URL", "LIDARR_API_KEY", "AUTOBRR_URL", "AUTOBRR_API_KEY", "AUTO_FIX_BROKEN_ALBUMS",
        "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD", "BROKEN_ALBUM_PERCENTAGE_THRESHOLD", "REQUIRED_TAGS",
    }
    # Only process keys that are in the request AND in the allowed list
    # This preserves existing values in SQLite for keys not in the request
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return jsonify({"status": "ok", "message": "Nothing to save"})
    if "SECTION_IDS" in updates:
        raw = updates["SECTION_IDS"]
        if isinstance(raw, str):
            updates["SECTION_IDS"] = [int(x.strip()) for x in raw.split(",") if x.strip()]
        elif isinstance(raw, list):
            updates["SECTION_IDS"] = [int(x) for x in raw]
    if "SKIP_FOLDERS" in updates and isinstance(updates["SKIP_FOLDERS"], str):
        updates["SKIP_FOLDERS"] = [p.strip() for p in updates["SKIP_FOLDERS"].split(",") if p.strip()]
    if "REQUIRED_TAGS" in updates:
        if isinstance(updates["REQUIRED_TAGS"], str):
            updates["REQUIRED_TAGS"] = [t.strip() for t in updates["REQUIRED_TAGS"].split(",") if t.strip()]
        elif not isinstance(updates["REQUIRED_TAGS"], list):
            updates["REQUIRED_TAGS"] = ["artist", "album", "date"]
    if "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD" in updates:
        try:
            updates["BROKEN_ALBUM_CONSECUTIVE_THRESHOLD"] = int(updates["BROKEN_ALBUM_CONSECUTIVE_THRESHOLD"])
        except (ValueError, TypeError):
            updates["BROKEN_ALBUM_CONSECUTIVE_THRESHOLD"] = 3
    if "BROKEN_ALBUM_PERCENTAGE_THRESHOLD" in updates:
        try:
            updates["BROKEN_ALBUM_PERCENTAGE_THRESHOLD"] = float(updates["BROKEN_ALBUM_PERCENTAGE_THRESHOLD"])
        except (ValueError, TypeError):
            updates["BROKEN_ALBUM_PERCENTAGE_THRESHOLD"] = 0.20
    
    # Serialize complex types for SQLite storage
    updates_for_db = {}
    for k, v in updates.items():
        if isinstance(v, (dict, list)):
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
    
    # Optional mirror to config.json (backup/readability). SQLite is the single source of truth.
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            conf_write = json.load(f)
        conf_write.update(updates)
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(conf_write, f, indent=2)
        logging.info("Settings mirrored to config.json (SQLite remains source of truth)")
    except Exception as e:
        logging.warning("Failed to write config.json: %s (ignored, SQLite is source of truth)", e)
    
    # Update global variables if MusicBrainz email changed
    if "MUSICBRAINZ_EMAIL" in updates:
        global MUSICBRAINZ_EMAIL
        MUSICBRAINZ_EMAIL = updates["MUSICBRAINZ_EMAIL"]
        _configure_musicbrainz_useragent()
        logging.info("MusicBrainz User-Agent updated with new email: %s", MUSICBRAINZ_EMAIL)
    
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


@app.get("/api/duplicates")
def api_duplicates():
    """
    Return the full list of duplicate-group cards for the Web UI.
    Loads from DB into state["duplicates"] on first call (or when empty).
    When Plex is not configured, returns [] and header X-PMDA-Requires-Config: true.
    """
    if not PLEX_CONFIGURED:
        resp = jsonify([])
        resp.headers["X-PMDA-Requires-Config"] = "true"
        return resp
    with lock:
        if not state["duplicates"]:
            logging.debug("api_duplicates(): loading scan results from DB into memory")
            state["duplicates"] = load_scan_from_db()
        cards = _build_card_list(state["duplicates"])
    return jsonify(cards)


@app.get("/api/progress")
def api_progress():
    with lock:
        if state["scan_total"] and state["scan_progress"] >= state["scan_total"]:
            state["scanning"] = False
        scanning = state["scanning"]
        status = "paused" if (scanning and scan_is_paused.is_set()) else ("running" if scanning else "stopped")
        progress = state["scan_progress"]
        total = state["scan_total"]
        
        # Calculate ETA
        eta_seconds = None
        threads_in_use = SCAN_THREADS
        
        if scanning and state.get("scan_start_time") and progress > 0:
            current_time = time.time()
            start_time = state.get("scan_start_time")
            last_update_time = state.get("scan_last_update_time", start_time)
            last_progress = state.get("scan_last_progress", 0)
            
            # Update tracking if enough time has passed or significant progress made
            if current_time - last_update_time >= 5.0 or (progress - last_progress) >= 100:
                state["scan_last_update_time"] = current_time
                state["scan_last_progress"] = progress
            
            # Calculate average speed (albums per second)
            elapsed_time = current_time - start_time
            if elapsed_time > 0:
                speed = progress / elapsed_time
                remaining = total - progress
                if speed > 0 and remaining > 0:
                    eta_seconds = int(remaining / speed)
        
        # Get active artists (exclude internal tracking keys like "_ai_batch")
        # Do this inside the lock to avoid race conditions
        active_artists_dict = state.get("scan_active_artists", {})
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
    
    return jsonify(
        scanning=scanning,
        progress=progress,
        total=total,
        status=status,
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
        # ETA
        eta_seconds=eta_seconds,
        threads_in_use=threads_in_use,
        active_artists=active_artists_list,
    )

@app.get("/api/scan-history")
def api_scan_history():
    """Return list of all scan history entries."""
    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
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
        history.append({
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
            # Detailed statistics
            "duplicate_groups_count": row[16] or 0 if len(row) > 16 else 0,
            "total_duplicates_count": row[17] or 0 if len(row) > 17 else 0,
            "broken_albums_count": row[18] or 0 if len(row) > 18 else 0,
            "missing_albums_count": row[19] or 0 if len(row) > 19 else 0,
            "albums_without_artist_image": row[20] or 0 if len(row) > 20 else 0,
            "albums_without_album_image": row[21] or 0 if len(row) > 21 else 0,
            "albums_without_complete_tags": row[22] or 0 if len(row) > 22 else 0,
            "albums_without_mb_id": row[23] or 0 if len(row) > 23 else 0,
            "albums_without_artist_mb_id": row[24] or 0 if len(row) > 24 else 0,
        })
    
    return jsonify(history)

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
    """Return list of all broken albums."""
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
    
    # Get album titles from Plex DB
    broken_albums = []
    db_conn = None
    try:
        db_conn = plex_connect()
        for row in rows:
            album_id = row[1]
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
        con.close()
    
    return jsonify(broken_albums)

@app.get("/api/library/artists")
def api_library_artists():
    """Return list of artists with statistics. Supports search and pagination."""
    if not PLEX_CONFIGURED:
        return jsonify({"error": "Plex not configured"}), 503
    
    import sqlite3
    search_query = request.args.get("search", "").strip()
    limit = int(request.args.get("limit", 100))
    offset = int(request.args.get("offset", 0))
    
    db_conn = plex_connect()
    
    # Build search filter
    if search_query:
        search_filter = "AND art.title LIKE ? ESCAPE '\\'"
        # Escape special characters for SQL LIKE
        escaped_query = search_query.replace('%', '\\%').replace('_', '\\_')
        search_args = [f"%{escaped_query}%"]
    else:
        search_filter = ""
        search_args = []
    
    # Get artists with their album counts
    # metadata_type 8 = artist, 9 = album
    placeholders = ",".join("?" for _ in SECTION_IDS) if not CROSS_LIBRARY_DEDUPE else ""
    if CROSS_LIBRARY_DEDUPE:
        section_filter = ""
        section_args = []
    else:
        section_filter = f"AND alb.library_section_id IN ({placeholders})"
        section_args = list(SECTION_IDS)
    
    # Get total count for pagination
    count_cursor = db_conn.execute(f"""
        SELECT COUNT(DISTINCT art.id)
        FROM metadata_items art
        LEFT JOIN metadata_items alb ON alb.parent_id = art.id 
            AND alb.metadata_type = 9
            {section_filter}
        WHERE art.metadata_type = 8
            {search_filter}
        HAVING COUNT(DISTINCT alb.id) > 0
    """, section_args + search_args)
    total_count = count_cursor.fetchone()[0] if count_cursor else 0
    
    # Get paginated artists
    cursor = db_conn.execute(f"""
        SELECT 
            art.id,
            art.title as artist_name,
            COUNT(DISTINCT alb.id) as album_count
        FROM metadata_items art
        LEFT JOIN metadata_items alb ON alb.parent_id = art.id 
            AND alb.metadata_type = 9
            {section_filter}
        WHERE art.metadata_type = 8
            {search_filter}
        GROUP BY art.id, art.title
        HAVING COUNT(DISTINCT alb.id) > 0
        ORDER BY art.title
        LIMIT ? OFFSET ?
    """, section_args + search_args + [limit, offset])
    
    artists = []
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    cur = con.cursor()
    
    for row in cursor.fetchall():
        artist_id, artist_name, album_count = row
        # Get broken albums count
        cur.execute("SELECT COUNT(*) FROM broken_albums WHERE artist = ?", (artist_name,))
        broken_count = cur.fetchone()[0]
        
        artists.append({
            "artist_id": artist_id,
            "artist_name": artist_name,
            "album_count": album_count or 0,
            "broken_albums_count": broken_count
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
    
    # Get all albums for this artist
    placeholders = ",".join("?" for _ in SECTION_IDS) if not CROSS_LIBRARY_DEDUPE else ""
    if CROSS_LIBRARY_DEDUPE:
        section_filter = ""
        section_args = [artist_id]
    else:
        section_filter = f"AND alb.library_section_id IN ({placeholders})"
        section_args = [artist_id] + list(SECTION_IDS)
    
    albums = []
    album_rows = db_conn.execute(f"""
        SELECT 
            alb.id,
            alb.title,
            alb.year,
            alb.originally_available_at,
            COUNT(DISTINCT tr.id) as track_count
        FROM metadata_items alb
        LEFT JOIN metadata_items tr ON tr.parent_id = alb.id AND tr.metadata_type = 10
        WHERE alb.parent_id = ? AND alb.metadata_type = 9
            {section_filter}
        GROUP BY alb.id, alb.title, alb.year, alb.originally_available_at
        ORDER BY alb.originally_available_at DESC, alb.title
    """, section_args).fetchall()
    
    # Check for broken albums and get album types
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    cur = con.cursor()
    
    for album_row in album_rows:
        album_id, title, year, date, track_count = album_row
        cur.execute("SELECT 1 FROM broken_albums WHERE artist = ? AND album_id = ?", (artist_name, album_id))
        is_broken = cur.fetchone() is not None
        
        # Get album thumb
        album_thumb = None
        try:
            album_thumb = thumb_url(album_id)
        except Exception:
            pass
        
        # Determine album type from tags or MusicBrainz
        album_type = "Album"  # Default
        folder = first_part_path(db_conn, album_id)
        if folder:
            first_audio = next((p for p in folder.rglob("*") if AUDIO_RE.search(p.name)), None)
            if first_audio:
                meta = extract_tags(first_audio)
                # Check for compilation tag
                if meta.get('compilation') == '1' or meta.get('compilation') == 'true':
                    album_type = "Compilation"
                # Check MusicBrainz release type if available
                elif USE_MUSICBRAINZ:
                    mbid = meta.get('musicbrainz_releasegroupid') or meta.get('musicbrainz_releaseid')
                    if mbid:
                        try:
                            result = musicbrainzngs.get_release_group_by_id(mbid, includes=["tags"])
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
                # Heuristic: if track_count <= 3, likely a Single
                if track_count <= 3 and album_type == "Album":
                    album_type = "Single"
                # Heuristic: if track_count <= 6, might be EP
                elif track_count <= 6 and album_type == "Album":
                    album_type = "EP"
        
        albums.append({
            "album_id": album_id,
            "title": title,
            "year": year,
            "date": date,
            "track_count": track_count or 0,
            "is_broken": is_broken,
            "thumb": album_thumb,
            "type": album_type
        })
    
    con.close()
    db_conn.close()
    
    return jsonify({
        "artist_id": artist_id,
        "artist_name": artist_name,
        "artist_thumb": artist_thumb,
        "albums": albums,
        "total_albums": len(albums)
    })

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

def get_artist_albums(db_conn, artist_id: int) -> List[dict]:
    """Get all albums for an artist from Plex DB."""
    placeholders = ",".join("?" for _ in SECTION_IDS) if not CROSS_LIBRARY_DEDUPE else ""
    if CROSS_LIBRARY_DEDUPE:
        section_filter = ""
        section_args = [artist_id]
    else:
        section_filter = f"AND alb.library_section_id IN ({placeholders})"
        section_args = [artist_id] + list(SECTION_IDS)
    
    cursor = db_conn.execute(f"""
        SELECT alb.id, alb.title
        FROM metadata_items alb
        WHERE alb.parent_id = ? AND alb.metadata_type = 9
            {section_filter}
    """, section_args)
    
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
    return jsonify({
        "artist_mbid": mbid,
        "similar_artists": similar
    })

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
                    return musicbrainzngs.get_release_by_id(mbid, includes=['release-group'])['release']
                
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
    
    # Get all albums for this artist
    placeholders = ",".join("?" for _ in SECTION_IDS) if not CROSS_LIBRARY_DEDUPE else ""
    if CROSS_LIBRARY_DEDUPE:
        section_filter = ""
        section_args = [artist_id]
    else:
        section_filter = f"AND alb.library_section_id IN ({placeholders})"
        section_args = [artist_id] + list(SECTION_IDS)
    
    album_rows = db_conn.execute(f"""
        SELECT alb.id, alb.title
        FROM metadata_items alb
        WHERE alb.parent_id = ? AND alb.metadata_type = 9
            {section_filter}
    """, section_args).fetchall()
    
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
            
            mb_release_info = None
            if release_mbid:
                try:
                    result = musicbrainzngs.get_release_group_by_id(release_mbid, includes=["releases"])
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
                            if release_mbid:
                                audio["MUSICBRAINZ_RELEASEGROUPID"] = release_mbid
                        elif isinstance(audio, MP4):
                            audio["----:com.apple.iTunes:MusicBrainz Artist Id"] = [mbid.encode('utf-8')]
                            audio["----:com.apple.iTunes:MusicBrainz Album Artist Id"] = [mbid.encode('utf-8')]
                            if release_mbid:
                                audio["----:com.apple.iTunes:MusicBrainz Release Group Id"] = [release_mbid.encode('utf-8')]
                    
                    audio.save()
                    albums_updated += 1
                    
                    # Try to fetch and save album cover if missing
                    if not any(f.name.lower().startswith(('cover', 'folder', 'album', 'artwork', 'front')) 
                              and f.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp'] 
                              for f in folder.iterdir() if f.is_file()):
                        # Try to get cover from MusicBrainz
                        if release_mbid:
                            try:
                                # Get cover art from MusicBrainz Cover Art Archive
                                cover_url = f"http://coverartarchive.org/release-group/{release_mbid}/front"
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
    """Return details of a specific scan."""
    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
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
    
    return jsonify({
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
        # Detailed statistics
        "duplicate_groups_count": row[16] or 0 if len(row) > 16 else 0,
        "total_duplicates_count": row[17] or 0 if len(row) > 17 else 0,
        "broken_albums_count": row[18] or 0 if len(row) > 18 else 0,
        "missing_albums_count": row[19] or 0 if len(row) > 19 else 0,
        "albums_without_artist_image": row[20] or 0 if len(row) > 20 else 0,
        "albums_without_album_image": row[21] or 0 if len(row) > 21 else 0,
        "albums_without_complete_tags": row[22] or 0 if len(row) > 22 else 0,
        "albums_without_mb_id": row[23] or 0 if len(row) > 23 else 0,
        "albums_without_artist_mb_id": row[24] or 0 if len(row) > 24 else 0,
    })

@app.get("/api/scan-history/<int:scan_id>/moves")
def api_scan_history_moves(scan_id):
    """Return all moves for a specific scan."""
    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
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
        moves.append({
            "move_id": row[0],
            "scan_id": row[1],
            "artist": row[2],
            "album_id": row[3],
            "original_path": row[4],
            "moved_to_path": row[5],
            "size_mb": row[6] or 0,
            "moved_at": row[7],
            "restored": bool(row[8]),
        })
    
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
    
    return jsonify({"restored": restored_count, "artists_refreshed": len(artists_to_refresh)})

@app.post("/api/scan-history/<int:scan_id>/dedupe")
def api_scan_history_dedupe(scan_id):
    """Manually dedupe albums from a previous scan."""
    # Load scan results from DB
    scan_results = load_scan_from_db()
    if not scan_results:
        return jsonify({"error": "No scan results found for this scan"}), 404
    
    # Start deduplication
    background_dedupe(scan_results)
    return jsonify({"status": "ok", "message": "Deduplication started"})

@app.get("/api/dedupe")
def api_dedupe():
    with lock:
        return jsonify(
            deduping=state["deduping"],
            progress=state["dedupe_progress"],
            total=state["dedupe_total"],
            saved=get_stat("space_saved"),
            moved=get_stat("removed_dupes")
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
        if g["album_id"] == album_id:
            editions = [g["best"]] + g["losers"]
            best_album_id = g["best"]["album_id"]
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


@app.post("/dedupe/artist/<artist>")
def dedupe_artist(artist):
    r = _requires_config()
    if r is not None:
        return r
    art = artist.replace("_", " ")
    data = request.get_json() or {}
    raw = data.get("album_id")
    album_id = int(raw) if raw is not None else None
    keep_edition_album_id = data.get("keep_edition_album_id")
    if keep_edition_album_id is not None:
        keep_edition_album_id = int(keep_edition_album_id)
    moved_list: List[Dict] = []

    with lock:
        groups = state["duplicates"].get(art, [])
        for g in list(groups):
            if g["album_id"] != album_id:
                continue
            # Optional manual selection: keep one edition, treat others as losers
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
                g = {
                    "artist": art,
                    "album_id": album_id,
                    "best": _normalize_edition_as_best(kept, art),
                    "losers": losers,
                }
            logging.debug(f"dedupe_artist(): processing artist '{art}', album_id={album_id}")
            moved_list = perform_dedupe(g)
            groups.remove(next(gr for gr in groups if gr["album_id"] == album_id))
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

    return jsonify(success=True, message="Track moved to kept edition"), 200


@app.post("/dedupe/all")
def dedupe_all():
    r = _requires_config()
    if r is not None:
        return r
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
        all_groups = [g for lst in state["duplicates"].values() for g in lst]
        state["duplicates"].clear()
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        cur.execute("DELETE FROM duplicates_loser")
        con.commit()
        con.close()
        logging.debug("dedupe_merge_and_dedupe(): cleared in-memory and DB duplicates tables")

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


# ───────────────────────────────────── CLI MODE ───────────────────────────────────
def dedupe_cli(
    dry: bool,
    safe: bool,
    tag_extra: bool,
    verbose: bool,
) -> None:
    """
    Command-line mode:
    1. Scan every artist / album in the selected Plex sections.
    2. Detect duplicate album groups.
    3. Optionally move the “loser” folders and clean Plex metadata.

    Parameters
    ----------
    dry       : Simulate actions only (no file moves / API calls).
    safe      : Never delete Plex metadata, even when not dry-run.
    tag_extra : Tag “(Extra Tracks)” on the best edition if it has
                more tracks than the shortest one in the group.
    verbose   : Enable DEBUG-level logging.
    """
    # ------------------------------------------------------------------ #
    #  Logging & DB setup
    # ------------------------------------------------------------------ #
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.getLogger().setLevel(log_level)

    db = plex_connect()
    cur = db.cursor()

    # ------------------------------------------------------------------ #
    #  Statistics that we’ll summarise at the end
    # ------------------------------------------------------------------ #
    stats = dict.fromkeys(
        (
            "total_artists",
            "total_albums",
            "albums_with_dupes",
            "total_dupes",
            "total_moved_mb",
        ),
        0,
    )

    # ------------------------------------------------------------------ #
    #  Pre-compute totals so we can show progress
    # ------------------------------------------------------------------ #
    placeholders = ",".join("?" for _ in SECTION_IDS)
    total_albums_overall = cur.execute(
        f"""
        SELECT COUNT(*)
        FROM   metadata_items
        WHERE  metadata_type = 9
          AND  library_section_id IN ({placeholders})
        """,
        tuple(SECTION_IDS),
    ).fetchone()[0]
    logging.info(f"🔍 About to scan {total_albums_overall:,} albums…")

    artists = cur.execute(
        """
        SELECT id, title
        FROM   metadata_items
        WHERE  metadata_type = 8
          AND  library_section_id = ?
        """,
        (SECTION_ID,),
    ).fetchall()

    processed_albums = 0  # for progress updates

    # ------------------------------------------------------------------ #
    #  Main artist loop
    # ------------------------------------------------------------------ #
    for artist_id, artist_name in artists:
        stats["total_artists"] += 1

        album_ids = [
            row[0]
            for row in cur.execute(
                """
                SELECT id
                FROM   metadata_items
                WHERE  metadata_type = 9
                  AND  parent_id      = ?
                """,
                (artist_id,),
            ).fetchall()
        ]
        stats["total_albums"] += len(album_ids)

        # Progress ticker
        for aid in album_ids:
            processed_albums += 1
            if processed_albums % 100 == 0 or verbose:
                title = album_title(db, aid)
                logging.info(
                    "Progress: %s / %s – %s – %s",
                    f"{processed_albums:,}",
                    f"{total_albums_overall:,}",
                    artist_name,
                    title,
                )

        dup_groups = scan_duplicates(db, artist_name, album_ids)
        if dup_groups:
            stats["albums_with_dupes"] += len(dup_groups)

        removed_this_artist = 0

        # -------------------------------------------------------------- #
        #  Handle each duplicate group
        # -------------------------------------------------------------- #
        for group in dup_groups:
            best   = group["best"]
            losers = group["losers"]

            logging.info(
                "🏷  Duplicate group: %s — %s  (versions: %d)",
                artist_name,
                best["title_raw"],
                len(losers) + 1,
            )
            logging.info(
                "    Selected BEST: %s, %d-bit, %d tracks",
                get_primary_format(Path(best["folder"])),
                best["bd"],
                len(best["tracks"]),
            )

            # Accumulator for this group
            space_freed_mb = 0

            # Move every loser edition
            for loser in losers:
                src = Path(loser["folder"])
                if not src.exists():
                    logging.warning("Source missing (skipped): %s", src)
                    continue

                # Build destination under /dupes
                base_dst = build_dupe_destination(src)
                dst = base_dst
                counter = 1
                while dst.exists():
                    dst = base_dst.parent / f"{base_dst.name} ({counter})"
                    counter += 1
                dst.parent.mkdir(parents=True, exist_ok=True)

                size_mb = folder_size(src) // (1024 * 1024)

                if dry:
                    logging.info("    DRY-RUN – would move %s → %s  (%s MB)", src, dst, f"{size_mb:,}")
                else:
                    logging.info("    Moving %s → %s  (%s MB)", src, dst, f"{size_mb:,}")
                    safe_move(str(src), str(dst))

                space_freed_mb += size_mb
                stats["total_dupes"] += 1
                stats["total_moved_mb"] += size_mb
                removed_this_artist += 1
                # Keep global stats in sync for unified summary
                increment_stat("removed_dupes", 1)
                increment_stat("space_saved", size_mb)

                # Plex metadata cleanup
                if not (dry or safe):
                    try:
                        loser_id = loser["album_id"]
                        plex_api(f"/library/metadata/{loser_id}/trash", method="PUT")
                        time.sleep(0.3)
                        plex_api(f"/library/metadata/{loser_id}", method="DELETE")
                    except Exception as api_err:
                        logging.warning("Could not delete Plex metadata: %s", api_err)

            logging.info("    ➜ Freed %s MB in this group", f"{space_freed_mb:,}")

            # Optional extra-track tagging
            if tag_extra:
                editions = losers + [best]
                min_tracks = min(len(e["tracks"]) for e in editions)
                if len(best["tracks"]) > min_tracks:
                    try:
                        plex_api(
                            f"/library/metadata/{best['album_id']}"
                            "?title.value=(Extra Tracks)&title.lock=1",
                            method="PUT",
                        )
                        logging.info("    Tagged best edition with '(Extra Tracks)'")
                    except Exception as err:
                        logging.warning("Failed to tag edition: %s", err)

        # Refresh Plex for this artist after processing all groups
        if removed_this_artist and not dry:
            try:
                encoded_artist = quote_plus(artist_name)
                prefix = f"/music/matched/{artist_name[0].upper()}/{encoded_artist}"
                plex_api(f"/library/sections/{SECTION_ID}/refresh?path={prefix}")
                plex_api(f"/library/sections/{SECTION_ID}/emptyTrash", method="PUT")
            except Exception as refresh_err:
                logging.warning("Plex refresh failed for %s: %s", artist_name, refresh_err)

    db.close()

    # ------------------------------------------------------------------ #
    #  FINAL SUMMARY (unified)
    # ------------------------------------------------------------------ #
    # Always emit a local summary banner so it shows up in docker logs -f
    summary_lines = [
        "",
        "" + "─" * 69,
        "FINAL SUMMARY",
        f"Total artists           : {stats['total_artists']:,}",
        f"Total albums            : {stats['total_albums']:,}",
        f"Albums with dupes       : {stats['albums_with_dupes']:,}",
        f"Folders moved           : {stats['total_dupes']:,}",
        f"Total space reclaimed   : {stats['total_moved_mb']:,} MB",
        "" + "─" * 69,
        "",
    ]
    for line in summary_lines:
        logging.info(line)

    # Then, if a global emitter exists, call it (keeps Web UI/DB in sync)
    emit = globals().get("emit_final_summary")
    if callable(emit):
        globals()["SUMMARY_EMITTED"] = True
        try:
            emit("cli")
        except Exception as _e:
            logging.debug("emit_final_summary('cli') failed: %s", _e)

    # Flush handlers to force immediate appearance in logs
    for _h in logging.getLogger().handlers:
        try:
            _h.flush()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    #  Discord – always attempt to notify
    # ------------------------------------------------------------------ #
    try:
        notify_discord(
            "\n".join(
                [
                    "🟢 **PMDA CLI run finished**",
                    f"Artists scanned: {stats['total_artists']:,}",
                    f"Albums scanned: {stats['total_albums']:,}",
                    f"Duplicate albums: {stats['albums_with_dupes']:,}",
                    f"Folders moved: {stats['total_dupes']:,}",
                    f"Space reclaimed: {stats['total_moved_mb']:,} MB",
                    "(dry-run)" if dry else "",
                ]
            )
        )
    except Exception as e:
        logging.warning("Discord summary failed: %s", e)
    # Prevent duplicate summary emission by other components
    globals()["SUMMARY_EMITTED"] = True

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
        # WebUI mode: start the HTTP server first so the UI is reachable immediately,
        # then run startup checks (diagnostic + cross-check) in main thread.
        def run_server():
            app.run(host="0.0.0.0", port=WEBUI_PORT, threaded=True, use_reloader=False)

        server_thread = threading.Thread(target=run_server, daemon=False)
        server_thread.start()
        logging.info("WebUI listening on http://0.0.0.0:%s – startup checks running in background", WEBUI_PORT)
        run_startup_checks()
        logging.info("WebUI startup complete – you can open the interface now.")
        server_thread.join()  # block forever (app.run never returns)
    else:
        # CLI mode: run checks then full scan and dedupe
        run_startup_checks()
        logging.info("CLI mode: starting full library scan")
        # Temporarily disable Discord notifications during scan stage
        original_notify = globals().get("notify_discord")
        globals()["notify_discord"] = lambda *args, **kwargs: None
        background_scan()
        # Restore Discord notification function
        globals()["notify_discord"] = original_notify
        logging.info("CLI mode: scan complete, starting dedupe")
        dedupe_cli(
            dry=args.dry_run,
            safe=args.safe_mode,
            tag_extra=args.tag_extra,
            verbose=args.verbose
        )
