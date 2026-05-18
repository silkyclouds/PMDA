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
import ast
import copy
import csv
import html
import io
import json
import os
import secrets
import shutil
import uuid
import ipaddress
import tempfile
import filecmp
import errno
import zlib
from decimal import Decimal, ROUND_HALF_UP
import sqlite3
import subprocess
import threading
import time
import atexit
import weakref
import itertools
from contextlib import contextmanager
from datetime import datetime, timedelta, time as dt_time
from collections import Counter, defaultdict, OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from functools import lru_cache
from pathlib import Path
from typing import NamedTuple, List, Dict, Optional, Tuple, Any, Callable
from urllib.parse import quote, quote_plus, unquote, urlparse

import logging
import re
import socket
import struct
import hashlib
import math
import unicodedata
import xml.etree.ElementTree as ET

import requests
from requests import exceptions as requests_exceptions
import musicbrainzngs
try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:
    Fernet = None
    InvalidToken = Exception

# MusicBrainz client will be configured after config is loaded
# (see _configure_musicbrainz_client function)
# Set rate limiting: 1 request per second (MusicBrainz limit)
musicbrainzngs.set_rate_limit(limit_or_interval=1.0, new_requests=1)
# Reduce noise from musicbrainzngs XML parser (e.g. "in <ws2:release-group>, uncaught attribute type-id")
logging.getLogger("musicbrainzngs").setLevel(logging.WARNING)
from openai import OpenAI
from pmda_ai.selector import select_provider_id
from pmda_ai import assistant_rag_runtime as _assistant_rag_runtime
from pmda_ai import assistant_chat_runtime as _assistant_chat_runtime
from pmda_ai import codex_exec_runtime as _codex_exec_runtime
from pmda_ai import domain_queue_runtime as _ai_domain_queue_runtime
from pmda_ai import provider_config_runtime as _ai_provider_config_runtime
from pmda_ai import provider_runtime as _ai_provider_runtime
from pmda_ai import web_search_runtime as _web_search_runtime
from pmda_ai import usage_runtime as _ai_usage_runtime
from pmda_ai import guardrails_runtime as _ai_guardrails_runtime
from pmda_ai import artist_roles_runtime as _artist_roles_runtime
from pmda_integrations import lastfm_runtime as _lastfm_runtime
from pmda_integrations import plex_source_compat as _plex_source_compat
from pmda_core import logging_runtime as _logging_runtime
from pmda_core import managed_runtime as _managed_runtime
from pmda_core import runtime_tuning as _runtime_tuning
from pmda_core import execution_runtime as _execution_runtime
from pmda_core import job_status_runtime as _job_status_runtime
from pmda_core import number_parsing as _number_parsing
from pmda_core import cache_db_runtime as _cache_db_runtime
from pmda_core import cache_telemetry_runtime as _cache_telemetry_runtime
from pmda_core import auth_runtime as _auth_runtime
from pmda_core import library_workflow_runtime as _library_workflow_runtime
from pmda_core import maintenance_runtime as _maintenance_runtime
from pmda_api import register_api_blueprints
from pmda_api import tools_runtime as _tools_runtime
from pmda_core.log_tail import LogTailParser, tail_log_lines as _tail_log_lines
from pmda_dedupe import review as _dedupe_review_core
from pmda_dedupe import broken_runtime as _dedupe_broken_runtime
from pmda_dedupe import actions_runtime as _dedupe_actions_runtime
from pmda_dedupe import cards_runtime as _dedupe_cards_runtime
from pmda_dedupe import choose_best_runtime as _dedupe_choose_best_runtime
from pmda_dedupe import move_runtime as _dedupe_move_runtime
from pmda_dedupe import perform_runtime as _dedupe_perform_runtime
from pmda_dedupe import scan_runtime as _dedupe_scan_runtime
from pmda_dedupe import signal_runtime as _dedupe_signal_runtime
from pmda_discovery import storage_buckets as _storage_buckets
from pmda_discovery import storage_bucket_runtime as _storage_bucket_runtime
from pmda_discovery import files_editions_runtime as _files_editions_runtime
from pmda_discovery import filesystem_walk_runtime as _filesystem_walk_runtime
from pmda_discovery import audio_runtime as _audio_runtime
from pmda_discovery import source_roots_runtime as _source_roots_runtime
from pmda_discovery import files_watcher_runtime as _files_watcher_runtime
from pmda_enrichment import profiles as _enrichment_profiles
from pmda_enrichment import wikipedia_runtime as _wikipedia_runtime
from pmda_enrichment import profile_runtime as _profile_runtime
from pmda_enrichment import profile_support_runtime as _profile_support_runtime
from pmda_enrichment import similar_images_runtime as _similar_images_runtime
from pmda_enrichment import scan_targets_runtime as _scan_targets_runtime
from pmda_enrichment import status as _enrichment_status
from pmda_enrichment import external_image_cache_runtime as _external_image_cache_runtime
from pmda_enrichment import image_utils_runtime as _image_utils_runtime
from pmda_enrichment import media_cache_runtime as _media_cache_runtime
from pmda_enrichment import artwork_runtime as _artwork_runtime
from pmda_incompletes import broken_album_runtime as _broken_album_runtime
from pmda_incompletes import ai_runtime as _incomplete_ai_runtime
from pmda_incompletes import move_runtime as _incomplete_move_runtime
from pmda_incompletes import review as _incomplete_review_core
from pmda_materialization import helpers_runtime as _materialization_helpers_runtime
from pmda_materialization import export_rebuild_runtime as _export_rebuild_runtime
from pmda_materialization import export_runtime as _materialization_export_runtime
from pmda_materialization import strict_export_runtime as _strict_export_runtime
from pmda_materialization import safe_move
from pmda_mcp import runtime as _mcp_runtime
from pmda_matching import bandcamp_runtime as _bandcamp_runtime
from pmda_matching import musicbrainz_client_runtime as _musicbrainz_client_runtime
from pmda_matching import musicbrainz_runtime as _musicbrainz_runtime
from pmda_matching import provider_gateway_runtime as _provider_gateway_runtime
from pmda_matching import provider_fallback_runtime as _provider_fallback_runtime
from pmda_matching import provider_identity_runtime as _provider_identity_runtime
from pmda_matching import identity_runtime as _identity_runtime
from pmda_matching import identity_hints_runtime as _identity_hints_runtime
from pmda_matching import discogs_runtime as _discogs_runtime
from pmda_matching import public_album_providers_runtime as _public_album_providers_runtime
from pmda_scan import background_runtime as _background_runtime
from pmda_scan import control_runtime as _scan_control_runtime
from pmda_scan import history_runtime as _scan_history_runtime
from pmda_scan import move_audit_runtime as _scan_move_audit_runtime
from pmda_scan import pipeline_trace_runtime as _scan_pipeline_trace_runtime
from pmda_scan import persistence_runtime as _scan_persistence_runtime
from pmda_scan import reconciliation_runtime as _scan_reconciliation_runtime
from pmda_scan import resume_runtime as _scan_resume_runtime
from pmda_scan import summary_runtime as _scan_summary_runtime
from pmda_scan import create_scan_runtime, start_scan_thread, wait_if_paused as _scan_control_wait_if_paused
from pmda_publication import artist_browse_runtime as _artist_browse_runtime
from pmda_publication import artist_publish_runtime as _artist_publish_runtime
from pmda_publication import artist_identity_runtime as _artist_identity_runtime
from pmda_publication import artist_merge_runtime as _artist_merge_runtime
from pmda_publication import cache_quality_runtime as _cache_quality_runtime
from pmda_publication import cover_runtime as _publication_cover_runtime
from pmda_publication import index_rebuild_runtime as _index_rebuild_runtime
from pmda_publication import index_status_runtime as _index_status_runtime
from pmda_publication import reconcile_runtime as _publication_reconcile_runtime
from pmda_publication import published_payload_runtime as _published_payload_runtime
from pmda_publication import row_runtime as _publication_row_runtime
from pmda_publication import snapshot as _publication_snapshot
try:
    import anthropic
except ImportError:
    anthropic = None
try:
    # google-generativeai is deprecated; use google-genai (import path: google.genai).
    from google import genai
except ImportError:
    genai = None

# ──────────────── ANSI colours for prettier logs ────────────────
ANSI_RESET   = "\033[0m"
ANSI_BOLD    = "\033[1m"
ANSI_DIM     = "\033[2m"
ANSI_BLACK   = "\033[30m"
ANSI_WHITE   = "\033[97m"
ANSI_GREEN   = "\033[92m"
ANSI_YELLOW  = "\033[93m"
ANSI_CYAN    = "\033[96m"
ANSI_RED     = "\033[91m"
ANSI_MAGENTA = "\033[95m"
ANSI_BLUE    = "\033[94m"
ANSI_BG_BLUE = "\033[104m"
ANSI_BG_CYAN = "\033[106m"
ANSI_BG_GREEN = "\033[102m"
ANSI_BG_YELLOW = "\033[103m"
ANSI_BG_RED = "\033[101m"
ANSI_BG_MAGENTA = "\033[105m"
ANSI_BG_WHITE = "\033[107m"
ANSI_BG_BLACK = "\033[40m"


def _ansi_256_fg(*args, **kwargs):
    return _logging_runtime._ansi_256_fg_for_runtime(sys.modules[__name__], *args, **kwargs)

def _ansi_256_bg(*args, **kwargs):
    return _logging_runtime._ansi_256_bg_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_header(*args, **kwargs):
    return _logging_runtime.log_header_for_runtime(sys.modules[__name__], *args, **kwargs)

def colour(*args, **kwargs):
    return _logging_runtime.colour_for_runtime(sys.modules[__name__], *args, **kwargs)

def _humanize_log_thread_name(*args, **kwargs):
    return _logging_runtime._humanize_log_thread_name_for_runtime(sys.modules[__name__], *args, **kwargs)

def _plain_log_record_line(*args, **kwargs):
    return _logging_runtime._plain_log_record_line_for_runtime(sys.modules[__name__], *args, **kwargs)

def _pad_log_label(*args, **kwargs):
    return _logging_runtime._pad_log_label_for_runtime(sys.modules[__name__], *args, **kwargs)

def _styled_log_pill(*args, **kwargs):
    return _logging_runtime._styled_log_pill_for_runtime(sys.modules[__name__], *args, **kwargs)

def _log_level_badge(*args, **kwargs):
    return _logging_runtime._log_level_badge_for_runtime(sys.modules[__name__], *args, **kwargs)

def _log_thread_pill(*args, **kwargs):
    return _logging_runtime._log_thread_pill_for_runtime(sys.modules[__name__], *args, **kwargs)

def _parse_log_tag_body(*args, **kwargs):
    return _logging_runtime._parse_log_tag_body_for_runtime(sys.modules[__name__], *args, **kwargs)

def _parse_album_profile_progress(*args, **kwargs):
    return _logging_runtime._parse_album_profile_progress_for_runtime(sys.modules[__name__], *args, **kwargs)

def _log_state_from_domain_body(*args, **kwargs):
    return _logging_runtime._log_state_from_domain_body_for_runtime(sys.modules[__name__], *args, **kwargs)

def _log_marker_visual(*args, **kwargs):
    return _logging_runtime._log_marker_visual_for_runtime(sys.modules[__name__], *args, **kwargs)

def _log_domain_parts(*args, **kwargs):
    return _logging_runtime._log_domain_parts_for_runtime(sys.modules[__name__], *args, **kwargs)

def _summarize_pipeline_flags_for_log(*args, **kwargs):
    return _logging_runtime._summarize_pipeline_flags_for_log_for_runtime(sys.modules[__name__], *args, **kwargs)

def _log(*args, **kwargs):
    return _logging_runtime._log_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_scan(*args, **kwargs):
    return _logging_runtime.log_scan_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_mb(*args, **kwargs):
    return _logging_runtime.log_mb_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_provider(*args, **kwargs):
    return _logging_runtime.log_provider_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_acoustid(*args, **kwargs):
    return _logging_runtime.log_acoustid_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_match(*args, **kwargs):
    return _logging_runtime.log_match_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_soft(*args, **kwargs):
    return _logging_runtime.log_soft_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_miss(*args, **kwargs):
    return _logging_runtime.log_miss_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_ai(*args, **kwargs):
    return _logging_runtime.log_ai_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_dupes(*args, **kwargs):
    return _logging_runtime.log_dupes_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_live(*args, **kwargs):
    return _logging_runtime.log_live_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_path(*args, **kwargs):
    return _logging_runtime.log_path_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_cfg(*args, **kwargs):
    return _logging_runtime.log_cfg_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_cov(*args, **kwargs):
    return _logging_runtime.log_cov_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_art(*args, **kwargs):
    return _logging_runtime.log_art_for_runtime(sys.modules[__name__], *args, **kwargs)

def log_tag(*args, **kwargs):
    return _logging_runtime.log_tag_for_runtime(sys.modules[__name__], *args, **kwargs)

def _compact_mb_rejection_reason(*args, **kwargs):
    return _logging_runtime._compact_mb_rejection_reason_for_runtime(sys.modules[__name__], *args, **kwargs)

def _log_mb_candidate_rejection(*args, **kwargs):
    return _logging_runtime._log_mb_candidate_rejection_for_runtime(sys.modules[__name__], *args, **kwargs)






from queue import SimpleQueue, Queue, Empty
import sys
import random

try:
    import psycopg
except ImportError:
    psycopg = None
try:
    import redis as redis_lib
except ImportError:
    redis_lib = None
try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except ImportError:
    FileSystemEventHandler = None  # type: ignore[assignment]
    Observer = None  # type: ignore[assignment]


from flask import Flask, request, jsonify, redirect, Response, send_file, g, after_this_request, has_request_context

from pmda_ai.openai_auth_service import OpenAIAuthService

app = Flask(__name__)

# Path to integrated frontend build (self-hosted: one container = backend + UI)
_FRONTEND_DIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend", "dist")
_HAS_STATIC_UI = os.path.isdir(_FRONTEND_DIST)


def _normalize_match_cover_ocr_mode(value: Any) -> str:
    mode = str(value or "smart").strip().lower()
    if mode not in {"off", "smart", "always"}:
        return "smart"
    return mode

# CORS: locked down by explicit origin allow-list.
_DEFAULT_CORS_ORIGINS = {
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
}
_env_cors = (os.getenv("PMDA_ALLOWED_ORIGINS", "") or "").strip()
if _env_cors:
    _ALLOWED_CORS_ORIGINS = {o.strip().rstrip("/") for o in _env_cors.split(",") if o.strip()}
else:
    _ALLOWED_CORS_ORIGINS = set(_DEFAULT_CORS_ORIGINS)

@app.before_request
def _cors_preflight():
    if request.method == "OPTIONS":
        resp = Response(status=204)
        return _cors_headers(resp)
    return None

@app.after_request
def _cors_headers(response):
    origin = (request.headers.get("Origin") or "").strip().rstrip("/")
    if origin and origin in _ALLOWED_CORS_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Vary"] = "Origin"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    if str(request.path or "").startswith("/api/auth/"):
        response.headers["Cache-Control"] = "no-store"
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
    handlers=[logging.StreamHandler(sys.stdout)],
)


PMDA_LOG_VERBOSE = bool(os.getenv("PMDA_LOG_VERBOSE"))




_RECENT_LOG_BUFFER_MAX = 6000
_RECENT_LOG_BUFFER: deque[str] = deque(maxlen=_RECENT_LOG_BUFFER_MAX)
_RECENT_LOG_BUFFER_LOCK = threading.Lock()
from pmda_core import logging_utils as _logging_core




_logging_core.set_thread_name_humanizer(_humanize_log_thread_name)
PlainLogFormatter = _logging_core.PlainLogFormatter
_RecentLogBufferHandler = _logging_core.RecentLogBufferHandler


_LOG_TAG_RE = re.compile(r"^\[(?P<tag>[^\]]+)\]\s*")
_THREAD_PILL_WIDTH = 18
_DOMAIN_PILL_WIDTH = 11






















class ColourFormatter(logging.Formatter):
    """Add rich ANSI colours to logs so scanning the console is actually pleasant."""

    def format(self, record):
        original_levelname = record.levelname
        original_thread = record.threadName
        original_msg_obj = record.msg
        original_args = record.args
        thread_display = _humanize_log_thread_name(record.threadName)
        try:
            plain_message = record.getMessage()
            record.levelname = _log_level_badge(record.levelno)
            record.threadName = _log_thread_pill(thread_display)
            domain_pill, marker_pill, body = _log_domain_parts(record.levelno, plain_message)
            if "candidate " in body and "rejected before arbitration" in body:
                body = colour(body, ANSI_DIM) if not os.getenv("NO_COLOR") else body
            record.msg = f"{domain_pill} {marker_pill} {body}"
            record.args = ()
            return super().format(record)
        except Exception:
            record.levelname = _pad_log_label(record.levelname, 5)
            record.threadName = _pad_log_label(thread_display, _THREAD_PILL_WIDTH)
            record.msg = str(record.getMessage() or "")
            record.args = ()
            return super().format(record)
        finally:
            record.levelname = original_levelname
            record.threadName = original_thread
            record.msg = original_msg_obj
            record.args = original_args


_root_logger = logging.getLogger()
_recent_log_handler = _RecentLogBufferHandler(_RECENT_LOG_BUFFER, _RECENT_LOG_BUFFER_LOCK)
_recent_log_handler.setLevel(logging.DEBUG)
try:
    _root_logger.handlers.insert(0, _recent_log_handler)
except Exception:
    _root_logger.addHandler(_recent_log_handler)
for _handler in _root_logger.handlers:
    _handler.setFormatter(
        ColourFormatter(
            "%(asctime)s │ %(levelname)s │ %(threadName)s │ %(message)s",
            datefmt="%H:%M:%S",
        )
    )


































_MB_CANDIDATE_REJECTION_COUNTS: dict[tuple[str, str], int] = {}
_MB_CANDIDATE_REJECTION_LOCK = threading.Lock()
_MB_CANDIDATE_REJECTION_LIMIT = 1
_MB_SEARCH_LOG_CANDIDATE_LIMIT = 5





# Progress header filter: displays current/total progress as [cur/total X.X%]
PROGRESS_STATE = {"total": 0, "current": 0}

# Suppress verbose internal debug from OpenAI, HTTP libraries and werkzeug
# We do NOT want per-request 200 logs in INFO – only surfacing errors/warnings.
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("openai.api_requestor").setLevel(logging.WARNING)
logging.getLogger("openai._base_client").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


# High-frequency polling routes: do not log each request (only real work / errors matter)
_QUIET_REQUEST_PATHS = (
    "/api/progress",
    "/api/incomplete-albums/scan/progress",
    "/api/dedupe",
    "/api/duplicates",
    "/api/library/improve-all/progress",
    "/api/lidarr/add-incomplete-albums/progress",
)


class _QuietPollingFilter(_logging_core.QuietPollingFilter):
    """PMDA compatibility alias for quiet high-frequency request filtering."""

    def __init__(self):
        super().__init__(_QUIET_REQUEST_PATHS)


werk_logger = logging.getLogger("werkzeug")
werk_logger.setLevel(logging.WARNING)
werk_logger.addFilter(_QuietPollingFilter())

# ──────────────────────────── FFmpeg sanity-check ──────────────────────────────
# Central store for worker exceptions
worker_errors = SimpleQueue()

# ──────────────────────────────── AUTO–PURGE "INVALID" EDITIONS ────────────────────────────────
def _purge_invalid_edition(edition: dict):
    """
    Instantly move technically‑invalid rips (0‑byte folder or no media‑info) to
    /dupes so they never show up as valid duplicate candidates.

    This runs during the *scan* phase, therefore it must be completely
    exception‑safe and thread‑safe.
    """
    try:
        src_folder = Path(edition["folder"])
        if not src_folder.exists():
            return                                                    # already gone

        # Destination under /dupes, keep relative path when possible
        base_dst = build_dupe_destination(
            src_folder,
            artist_hint=str(edition.get("artist") or ""),
            album_hint=str(edition.get("title_raw") or src_folder.name or ""),
        )
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
_scan_runtime = create_scan_runtime()
scan_should_stop = _scan_runtime.stop_event
scan_is_paused = _scan_runtime.pause_event
files_cache_snapshot_lock = threading.Lock()


def _scan_wait_if_paused(*, sleep_seconds: float = 0.2) -> bool:
    """
    Cooperative pause checkpoint used by long-running loops (discovery/resume prep).
    Returns False when a stop was requested while paused.
    """
    return _scan_control_wait_if_paused(
        pause_event=scan_is_paused,
        stop_event=scan_should_stop,
        sleep_seconds=sleep_seconds,
    )

#
# ───────────────────────────────── CONFIGURATION LOADING ─────────────────────────────────
"""
Configuration is stored in SQLite (state.db, settings table) only. No config.json is used.
* _get() reads from SQLite first, then falls back to hardcoded defaults.
* Web UI and API save settings to SQLite via api_config_put().
"""

import filecmp
from pmda_core import config as _config_core
from pmda_core import files_pg_runtime as _files_pg_runtime
from pmda_core import settings_runtime as _settings_runtime
from pmda_core import scheduler_runtime as _scheduler_runtime
from pmda_core import library_browse as _library_browse_core
from pmda_core import ops_runtime as _ops_runtime
from pmda_core import pipeline_jobs as _pipeline_jobs_core
from pmda_core import pipeline_jobs_runtime as _pipeline_jobs_runtime
from pmda_core import pagination as _pagination_core
from pmda_core import provider_matching as _provider_matching_core
from pmda_core import scan_progress as _scan_progress_core
from pmda_core import scan_orchestrator as _scan_orchestrator_core
from pmda_enrichment import metadata_jobs as _metadata_jobs_core
from pmda_enrichment import artist_profile_runtime as _artist_profile_runtime
from pmda_library import browse_runtime as _library_browse_runtime
from pmda_library import catalog_runtime as _library_catalog_runtime
from pmda_library import catalog_stats_runtime as _catalog_stats_runtime
from pmda_library import classical_runtime as _classical_runtime
from pmda_library import detail_runtime as _library_detail_runtime
from pmda_library import improve_runtime as _library_improve_runtime
from pmda_library import improve_batch_runtime as _library_improve_batch_runtime
from pmda_library import album_media_runtime as _album_media_runtime
from pmda_library import album_match_runtime as _album_match_runtime
from pmda_library import album_review_lookup_runtime as _album_review_lookup_runtime
from pmda_library import personal_runtime as _library_personal_runtime
from pmda_library import recommendation_runtime as _recommendation_runtime
from pmda_library import published_browse_runtime as _published_browse_runtime
from pmda_library import browse_state_runtime as _browse_state_runtime
from pmda_library import box_set_runtime as _box_set_runtime
from pmda_library import release_group_runtime as _release_group_runtime
from pmda_library import genre_runtime as _genre_runtime
from pmda_materialization import audit as _scan_moves_core
from pmda_materialization import policy as _materialization_policy_core
from pmda_publication import index_rebuild as _library_index_core
from pmda_publication import schema as _files_pg_schema
from pmda_publication import artist_maintenance as _artist_maintenance
from pmda_scan import progress_ai as _progress_ai_core
from pmda_scan import progress_payload as _progress_payload
from pmda_scan import progress_runtime as _progress_runtime_core
from pmda_scan import progress_summary as _progress_summary_core
from pmda_scan import bootstrap_runtime as _scan_bootstrap_runtime

# Helper parsers --------------------------------------------------------------
def _parse_bool(val: str | bool) -> bool:
    """Return *True* for typical truthy strings / bools and *False* otherwise."""
    return _config_core.parse_bool(val)

# Helper for falsy logic, if needed later
def _is_false(val: str | bool) -> bool:
    """Return *True* for typical falsy strings / bools and *False* otherwise."""
    return _config_core.is_false(val)

def _parse_int(val, default: int | None = None) -> int | None:
    """Return *int* or *default* on failure / None."""
    return _config_core.parse_int(val, default)

def _parse_path_map(val) -> dict[str, str]:
    """
    Accept either a dict, a JSON string or a CSV string of ``SRC:DEST`` pairs.
    Every key/val is coerced to *str*.
    """
    return _config_core.parse_path_map(val)

# Determine runtime config dir -------------------------------------------------
BASE_DIR   = Path(__file__).parent
CONFIG_DIR = Path(os.getenv("PMDA_CONFIG_DIR", BASE_DIR))
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
STATE_DB_FILE = CONFIG_DIR / "state.db"
SETTINGS_DB_FILE = CONFIG_DIR / "settings.db"
DROP_ALBUMS_BASE = CONFIG_DIR / "drop_albums"
DROP_MAX_FILES = 50
DROP_MAX_BYTES = 500 * 1024 * 1024  # 500 MB

# Auth / sessions
AUTH_PASSWORD_MIN_LEN = max(10, int(os.getenv("PMDA_AUTH_PASSWORD_MIN_LEN", "12") or "12"))
AUTH_PASSWORD_MAX_LEN = 256
AUTH_PBKDF2_ITERATIONS = max(150_000, int(os.getenv("PMDA_AUTH_PBKDF2_ITERATIONS", "260000") or "260000"))
AUTH_SESSION_TTL_SEC = max(900, int(os.getenv("PMDA_AUTH_SESSION_TTL_SEC", "43200") or "43200"))  # default 12h
AUTH_SESSION_REMEMBER_TTL_SEC = max(
    AUTH_SESSION_TTL_SEC,
    int(os.getenv("PMDA_AUTH_SESSION_TTL_REMEMBER_SEC", str(30 * 24 * 3600)) or str(30 * 24 * 3600)),
)  # default 30d
AUTH_LOGIN_FAILURE_DELAY_MS = max(
    0,
    min(5000, int(os.getenv("PMDA_AUTH_LOGIN_FAILURE_DELAY_MS", "250") or "250")),
)
AUTH_TOKEN_PEPPER = os.getenv("PMDA_AUTH_TOKEN_PEPPER", "") or ""
AUTH_DISABLE = _parse_bool(os.getenv("PMDA_AUTH_DISABLE", "false"))
AUTH_LOGIN_RATE_LIMIT_WINDOW_SEC = max(60, int(os.getenv("PMDA_AUTH_LOGIN_RATE_LIMIT_WINDOW_SEC", "300") or "300"))
AUTH_LOGIN_RATE_LIMIT_IP_MAX_ATTEMPTS = max(3, int(os.getenv("PMDA_AUTH_LOGIN_RATE_LIMIT_IP_MAX_ATTEMPTS", "20") or "20"))
AUTH_LOGIN_RATE_LIMIT_USER_MAX_ATTEMPTS = max(3, int(os.getenv("PMDA_AUTH_LOGIN_RATE_LIMIT_USER_MAX_ATTEMPTS", "10") or "10"))
AUTH_BOOTSTRAP_RATE_LIMIT_IP_MAX_ATTEMPTS = max(3, int(os.getenv("PMDA_AUTH_BOOTSTRAP_RATE_LIMIT_IP_MAX_ATTEMPTS", "10") or "10"))
AUTH_ALLOW_PUBLIC_BOOTSTRAP = _parse_bool(os.getenv("PMDA_AUTH_ALLOW_PUBLIC_BOOTSTRAP", "false"))
AUTH_TRUST_PROXY_HEADERS = _parse_bool(os.getenv("PMDA_AUTH_TRUST_PROXY_HEADERS", "false"))
AUTH_IP_BAN_ENABLED = _parse_bool(os.getenv("PMDA_AUTH_IP_BAN_ENABLED", "true"))
AUTH_IP_BAN_WINDOW_SEC = max(
    AUTH_LOGIN_RATE_LIMIT_WINDOW_SEC,
    int(os.getenv("PMDA_AUTH_IP_BAN_WINDOW_SEC", str(AUTH_LOGIN_RATE_LIMIT_WINDOW_SEC)) or str(AUTH_LOGIN_RATE_LIMIT_WINDOW_SEC)),
)
AUTH_IP_BAN_SHORT_THRESHOLD = max(3, int(os.getenv("PMDA_AUTH_IP_BAN_SHORT_THRESHOLD", "5") or "5"))
AUTH_IP_BAN_SHORT_DURATION_SEC = max(60, int(os.getenv("PMDA_AUTH_IP_BAN_SHORT_DURATION_SEC", "900") or "900"))
AUTH_IP_BAN_LONG_THRESHOLD = max(
    AUTH_IP_BAN_SHORT_THRESHOLD,
    int(os.getenv("PMDA_AUTH_IP_BAN_LONG_THRESHOLD", "10") or "10"),
)
AUTH_IP_BAN_LONG_DURATION_SEC = max(
    AUTH_IP_BAN_SHORT_DURATION_SEC,
    int(os.getenv("PMDA_AUTH_IP_BAN_LONG_DURATION_SEC", str(24 * 3600)) or str(24 * 3600)),
)
AUTH_IP_BAN_EXEMPT_PRIVATE = _parse_bool(os.getenv("PMDA_AUTH_IP_BAN_EXEMPT_PRIVATE", "true"))
AUTH_SESSION_COOKIE_NAME = (os.getenv("PMDA_AUTH_SESSION_COOKIE_NAME", "pmda_session") or "pmda_session").strip() or "pmda_session"
AUTH_SESSION_COOKIE_SECURE = _parse_bool(os.getenv("PMDA_AUTH_SESSION_COOKIE_SECURE", "false"))
_AUTH_SESSION_COOKIE_SAMESITE_RAW = str(os.getenv("PMDA_AUTH_SESSION_COOKIE_SAMESITE", "Lax") or "Lax").strip().lower()
if _AUTH_SESSION_COOKIE_SAMESITE_RAW in {"strict", "none"}:
    AUTH_SESSION_COOKIE_SAMESITE = _AUTH_SESSION_COOKIE_SAMESITE_RAW.title()
else:
    AUTH_SESSION_COOKIE_SAMESITE = "Lax"

_AUTH_RATE_LIMIT_LOCK = threading.Lock()
_AUTH_RATE_LIMIT_BUCKETS: dict[tuple[str, str], deque[int]] = {}
AUTH_DB_BUSY_TIMEOUT_MS = max(500, int(os.getenv("PMDA_AUTH_DB_BUSY_TIMEOUT_MS", "10000") or "10000"))
AUTH_SESSION_TOUCH_MIN_INTERVAL_SEC = max(5, int(os.getenv("PMDA_AUTH_SESSION_TOUCH_MIN_INTERVAL_SEC", "300") or "300"))
AUTH_SESSION_TOUCH_DB_WRITES = _parse_bool(os.getenv("PMDA_AUTH_SESSION_TOUCH_DB_WRITES", "false"))
_AUTH_SESSION_TOUCH_LOCK = threading.Lock()
_AUTH_SESSION_TOUCH_CACHE: dict[str, int] = {}
_AUTH_SESSION_CACHE_LOCK = threading.Lock()
_AUTH_SESSION_USER_CACHE: dict[str, dict[str, Any]] = {}

# Files-mode all-in-one data services (embedded in container entrypoint)
PMDA_PG_HOST = (os.getenv("PMDA_PG_HOST", "127.0.0.1") or "127.0.0.1").strip()
PMDA_PG_PORT = int((os.getenv("PMDA_PG_PORT", "5432") or "5432").strip())
PMDA_PG_DB = (os.getenv("PMDA_PG_DB", "pmda") or "pmda").strip()
PMDA_PG_USER = (os.getenv("PMDA_PG_USER", "pmda") or "pmda").strip()
PMDA_PG_PASSWORD = os.getenv("PMDA_PG_PASSWORD", "pmda") or "pmda"
PMDA_PGDATA = (os.getenv("PMDA_PGDATA", str(CONFIG_DIR / "postgres-data")) or str(CONFIG_DIR / "postgres-data")).strip()
PMDA_REDIS_HOST = (os.getenv("PMDA_REDIS_HOST", "127.0.0.1") or "127.0.0.1").strip()
PMDA_REDIS_PORT = int((os.getenv("PMDA_REDIS_PORT", "6379") or "6379").strip())
PMDA_REDIS_DB = int((os.getenv("PMDA_REDIS_DB", "0") or "0").strip())
PMDA_REDIS_PASSWORD = os.getenv("PMDA_REDIS_PASSWORD", "") or ""
FILES_CACHE_PREFIX = "pmda:files:v2:"
PMDA_MEDIA_CACHE_ROOT = (os.getenv("PMDA_MEDIA_CACHE_ROOT", "") or "").strip()
PMDA_ARTWORK_RAM_CACHE_MB = int(max(0, _parse_int(os.getenv("PMDA_ARTWORK_RAM_CACHE_MB", "1024"), 1024) or 1024))
PMDA_ARTWORK_RAM_CACHE_TTL_SEC = int(max(60, _parse_int(os.getenv("PMDA_ARTWORK_RAM_CACHE_TTL_SEC", "21600"), 21600) or 21600))
PMDA_ARTWORK_RAM_CACHE_MAX_ITEM_MB = int(max(1, _parse_int(os.getenv("PMDA_ARTWORK_RAM_CACHE_MAX_ITEM_MB", "8"), 8) or 8))
PMDA_ARTWORK_RAM_CACHE_AUTO = _parse_bool(os.getenv("PMDA_ARTWORK_RAM_CACHE_AUTO", "true"))
PMDA_ARTWORK_RAM_CACHE_AUTO_MAX_MB = int(max(0, _parse_int(os.getenv("PMDA_ARTWORK_RAM_CACHE_AUTO_MAX_MB", "0"), 0) or 0))
PMDA_ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC = int(max(30, _parse_int(os.getenv("PMDA_ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC", "120"), 120) or 120))
PMDA_FILES_WATCHER_ENABLED = _parse_bool(os.getenv("PMDA_FILES_WATCHER_ENABLED", "true"))
PMDA_FILES_WATCHER_LOG_COOLDOWN_SEC = float(os.getenv("PMDA_FILES_WATCHER_LOG_COOLDOWN_SEC", "10") or "10")
PMDA_AUTO_CHANGED_ONLY_SCAN = _parse_bool(os.getenv("PMDA_AUTO_CHANGED_ONLY_SCAN", "false"))
PMDA_AUTO_CHANGED_ONLY_SCAN_DEBOUNCE_SEC = float(os.getenv("PMDA_AUTO_CHANGED_ONLY_SCAN_DEBOUNCE_SEC", "60") or "60")
PMDA_AUTO_CHANGED_ONLY_SCAN_COOLDOWN_SEC = float(os.getenv("PMDA_AUTO_CHANGED_ONLY_SCAN_COOLDOWN_SEC", "300") or "300")
PMDA_AUTO_CHANGED_ONLY_SCAN_MIN_PENDING = int(os.getenv("PMDA_AUTO_CHANGED_ONLY_SCAN_MIN_PENDING", "1") or "1")
PMDA_CACHE_TELEMETRY_TTL_SEC = float(os.getenv("PMDA_CACHE_TELEMETRY_TTL_SEC", "15") or "15")
PMDA_CACHE_TELEMETRY_MAX_WALK_FILES = int(os.getenv("PMDA_CACHE_TELEMETRY_MAX_WALK_FILES", "400000") or "400000")
PMDA_OPS_SNAPSHOT_MEDIA_CACHE_MAX_WALK_FILES = int(
    os.getenv("PMDA_OPS_SNAPSHOT_MEDIA_CACHE_MAX_WALK_FILES", "5000") or "5000"
)
PMDA_CACHE_TELEMETRY_MAX_REDIS_SCAN_KEYS = int(os.getenv("PMDA_CACHE_TELEMETRY_MAX_REDIS_SCAN_KEYS", "200000") or "200000")
PMDA_REDIS_IDLE_CPU_WARN_PCT = float(os.getenv("PMDA_REDIS_IDLE_CPU_WARN_PCT", "35") or "35")
PMDA_REDIS_IDLE_OPS_MAX = max(0, int(os.getenv("PMDA_REDIS_IDLE_OPS_MAX", "5") or "5"))
PMDA_REDIS_IDLE_KEYS_MAX = max(0, int(os.getenv("PMDA_REDIS_IDLE_KEYS_MAX", "200") or "200"))
PMDA_REDIS_IDLE_CONSECUTIVE = max(1, int(os.getenv("PMDA_REDIS_IDLE_CONSECUTIVE", "3") or "3"))
PMDA_REDIS_IDLE_WARN_COOLDOWN_SEC = max(10.0, float(os.getenv("PMDA_REDIS_IDLE_WARN_COOLDOWN_SEC", "180") or "180"))
PMDA_OPENAI_CODEX_STATUS_TIMEOUT_SEC = max(5.0, float(os.getenv("PMDA_OPENAI_CODEX_STATUS_TIMEOUT_SEC", "30") or "30"))
PMDA_FILES_LOCAL_CACHE_MAX_KEYS = int(max(1000, _parse_int(os.getenv("PMDA_FILES_LOCAL_CACHE_MAX_KEYS", "200000"), 200000) or 200000))
PMDA_FILES_DIR_CACHE_MIN_SKIP_DEPTH = max(1, int(os.getenv("PMDA_FILES_DIR_CACHE_MIN_SKIP_DEPTH", "2") or "2"))
PMDA_FILES_DIR_CACHE_MIN_ALBUMS = max(1, int(os.getenv("PMDA_FILES_DIR_CACHE_MIN_ALBUMS", "1") or "1"))
PMDA_BENCHMARK_REPORTS_DIR = (
    os.getenv("PMDA_BENCHMARK_REPORTS_DIR", "/music/pmda_scan_benchmark/reports")
    or "/music/pmda_scan_benchmark/reports"
).strip()
PMDA_ANALYSIS_REPORTS_DIR = (
    os.getenv("PMDA_ANALYSIS_REPORTS_DIR", str((Path(__file__).resolve().parent / "analysis")))
    or str((Path(__file__).resolve().parent / "analysis"))
).strip()
RECO_EMBED_DIM = 64
RECO_EMBED_SOURCE = "pmda_hash_v1"

# Task events + scheduler (background jobs)
TASK_JOB_TYPES = {
    "scan_changed",
    "scan_full",
    "enrich_batch",
    "dedupe",
    "incomplete_move",
    "export",
    "player_sync",
    "managed_musicbrainz_update",
}
TASK_EVENT_STATUSES = {"started", "completed", "failed", "skipped"}
TASK_SCOPES = {"new", "full", "both"}
SCHEDULER_TRIGGER_TYPES = {"interval", "weekly"}
SCHEDULER_POLL_SEC = max(3.0, float(os.getenv("PMDA_SCHEDULER_POLL_SEC", "10") or "10"))
SCHEDULER_BUSY_RETRY_SEC = max(15.0, float(os.getenv("PMDA_SCHEDULER_BUSY_RETRY_SEC", "60") or "60"))
SCHEDULER_POOL_LIMIT_SCAN = max(1, int(os.getenv("PMDA_SCHEDULER_POOL_LIMIT_SCAN", "1") or "1"))
SCHEDULER_POOL_LIMIT_IO = max(1, int(os.getenv("PMDA_SCHEDULER_POOL_LIMIT_IO", "1") or "1"))
SCHEDULER_POOL_LIMIT_NETWORK = max(1, int(os.getenv("PMDA_SCHEDULER_POOL_LIMIT_NETWORK", "2") or "2"))
SCHEDULER_POOL_LIMIT_POST_SCAN = max(1, int(os.getenv("PMDA_SCHEDULER_POOL_LIMIT_POST_SCAN", "2") or "2"))
SCHEDULER_MANAGED_SCAN_SOURCES = {"schedule", "manual"}
SCHEDULER_ALLOW_NON_SCAN_JOBS = _parse_bool(os.getenv("PMDA_SCHEDULER_ALLOW_NON_SCAN_JOBS", "false"))

# AI usage + cost telemetry (persisted in state.db)
AI_ANALYSIS_TYPES = {
    "provider_identity_verify",
    "mb_match_verify",
    "acoustid_candidate_disambiguation",
    "web_mbid_inference",
    "web_search",
    "mb_retry_disambiguation",
    "mb_candidate_tiebreak",
    "mb_artist_index_choice",
    "cover_vision_verify",
    "dedupe_choose_best",
    "incomplete_album_arbitration",
    "album_review_lookup",
    "album_review_validate",
    "web_search_review",
    "identity_inference_no_tags",
    "other",
}
AI_USAGE_PHASES = {"scan", "post_scan", "scheduled", "manual"}
AI_USAGE_STATUSES = {"completed", "failed", "skipped"}
AI_DOMAIN_NAMES = ("matching", "dedupe", "incomplete", "review")
AI_DOMAIN_ANALYSIS_TYPES: dict[str, set[str]] = {
    "matching": {
        "provider_identity_verify",
        "mb_match_verify",
        "acoustid_candidate_disambiguation",
        "web_mbid_inference",
        "web_search",
        "mb_retry_disambiguation",
        "mb_candidate_tiebreak",
        "mb_artist_index_choice",
        "identity_inference_no_tags",
    },
    "dedupe": {"dedupe_choose_best"},
    "incomplete": {"incomplete_album_arbitration"},
    "review": {
        "album_review_lookup",
        "album_review_validate",
        "web_search_review",
        "cover_vision_verify",
        "assistant_chat",
    },
}
AI_DOMAIN_BENCHMARK_GLOBS: dict[str, list[str]] = {
    "matching": ["matching_ai_benchmark_*.json"],
    "dedupe": ["dedupe_ai_benchmark_*.json"],
    "incomplete": ["incomplete_ai_shadow_cpu_two_stage_*.json", "incomplete_ai_shadow_cpu_warm_*_fast4b_*.json"],
    "review": ["review_ai_benchmark_*.json"],
}
AI_PRICING_VERSION = "v1"
AI_ALLOW_ANALYSIS_OTHER = _parse_bool(os.getenv("PMDA_AI_ALLOW_ANALYSIS_OTHER", "false"))
# Legacy knobs kept for backward compatibility only. PMDA no longer enforces any
# application-level AI/web-search caps; batching, caching and provider-side limits
# are the only remaining constraints. Keep the env reads for observability, but
# force the effective values to 0 so an old container env cannot silently re-enable
# throttling after an upgrade.
_AI_WEB_SEARCH_MAX_CALLS_PER_HOUR_ENV = os.getenv("PMDA_AI_WEB_SEARCH_MAX_CALLS_PER_HOUR", "")
_AI_WEB_SEARCH_MAX_CALLS_PER_DAY_ENV = os.getenv("PMDA_AI_WEB_SEARCH_MAX_CALLS_PER_DAY", "")
AI_WEB_SEARCH_MAX_CALLS_PER_HOUR = 0
AI_WEB_SEARCH_MAX_CALLS_PER_DAY = 0
AI_WEB_SEARCH_MAX_OUTPUT_TOKENS = max(
    120,
    min(4000, int(os.getenv("PMDA_AI_WEB_SEARCH_MAX_OUTPUT_TOKENS", "900") or "900")),
)
AI_WEB_SEARCH_CACHE_TTL_SEC = max(
    60,
    int(os.getenv("PMDA_AI_WEB_SEARCH_CACHE_TTL_SEC", "21600") or "21600"),
)
AI_WEB_SEARCH_CACHE_NEG_TTL_SEC = max(
    60,
    int(os.getenv("PMDA_AI_WEB_SEARCH_CACHE_NEG_TTL_SEC", "3600") or "3600"),
)
AI_WEB_SEARCH_CACHE_MAX_ENTRIES = max(
    100,
    int(os.getenv("PMDA_AI_WEB_SEARCH_CACHE_MAX_ENTRIES", "2000") or "2000"),
)
AI_OPENAI_REQUEST_TIMEOUT_SEC = max(
    10.0,
    min(600.0, float(os.getenv("PMDA_AI_OPENAI_REQUEST_TIMEOUT_SEC", "120") or "120")),
)
MB_AI_VERIFY_TIMEOUT_SEC = max(
    5.0,
    min(300.0, float(os.getenv("PMDA_MB_AI_VERIFY_TIMEOUT_SEC", "90") or "90")),
)
MB_AI_TIEBREAK_TIMEOUT_SEC = max(
    5.0,
    min(300.0, float(os.getenv("PMDA_MB_AI_TIEBREAK_TIMEOUT_SEC", "90") or "90")),
)
PROVIDER_FALLBACK_PARALLEL_TIMEOUT_SEC = max(
    2.0,
    min(120.0, float(os.getenv("PMDA_PROVIDER_FALLBACK_TIMEOUT_SEC", "45") or "45")),
)
AI_SCAN_HARD_TIMEOUT_SEC = max(
    8.0,
    min(600.0, float(os.getenv("PMDA_AI_SCAN_HARD_TIMEOUT_SEC", "120") or "120")),
)
AI_REVIEW_FETCH_TIMEOUT_SEC = max(
    4.0,
    min(300.0, float(os.getenv("PMDA_AI_REVIEW_FETCH_TIMEOUT_SEC", "90") or "90")),
)


def _openai_request_timeout_seconds() -> float:
    return _ai_provider_config_runtime._openai_request_timeout_seconds_for_runtime(sys.modules[__name__])

AI_MAX_CALLS_PER_SCAN = max(
    0,
    int(os.getenv("PMDA_AI_MAX_CALLS_PER_SCAN", "0") or "0"),
)
AI_CALL_COOLDOWN_SEC = max(
    0.0,
    min(30.0, float(os.getenv("PMDA_AI_CALL_COOLDOWN_SEC", "0") or "0")),
)
AI_GLOBAL_MAX_CALLS_PER_MINUTE = max(
    0,
    int(os.getenv("PMDA_AI_GLOBAL_MAX_CALLS_PER_MINUTE", "0") or "0"),
)
AI_GLOBAL_MAX_CALLS_PER_DAY = max(
    0,
    int(os.getenv("PMDA_AI_GLOBAL_MAX_CALLS_PER_DAY", "0") or "0"),
)

# Pricing is expressed in micro-USD (1 USD = 1_000_000 micro-USD).
# Rows can be overridden in DB (ai_pricing_catalog) without code changes.
AI_PRICING_DEFAULT_ROWS: list[tuple[str, str, str, int, int, int, int, int, float, Optional[float]]] = [
    # provider, model, endpoint_kind, rate_input, rate_cached_input, rate_output, rate_image, pricing_version, effective_from, effective_to
    ("openai", "gpt-4o-mini", "text", 150000, 75000, 600000, 0, 1, 0.0, None),
    ("openai", "gpt-4o-mini", "longform", 150000, 75000, 600000, 0, 1, 0.0, None),
    ("openai", "gpt-4o-mini", "vision", 150000, 75000, 600000, 0, 1, 0.0, None),
    # Web-search tool call fee uses rate_image as fixed per-call price (25 USD / 1K calls = 0.025 USD = 25_000 micro-USD).
    ("openai", "gpt-4o-mini", "web_search", 150000, 75000, 600000, 25000, 1, 0.0, None),
    ("openai", "gpt-4o", "text", 2500000, 1250000, 10000000, 0, 1, 0.0, None),
    ("openai", "gpt-4o", "longform", 2500000, 1250000, 10000000, 0, 1, 0.0, None),
    ("openai", "gpt-4o", "vision", 2500000, 1250000, 10000000, 0, 1, 0.0, None),
    # web_search_preview (non-reasoning families): 25 USD / 1K calls = 0.025 USD/call.
    ("openai", "gpt-4o", "web_search", 2500000, 1250000, 10000000, 25000, 1, 0.0, None),
    ("openai", "gpt-4.1", "text", 2000000, 1000000, 8000000, 0, 1, 0.0, None),
    ("openai", "gpt-4.1", "longform", 2000000, 1000000, 8000000, 0, 1, 0.0, None),
    ("openai", "gpt-4.1", "vision", 2000000, 1000000, 8000000, 0, 1, 0.0, None),
    ("openai", "gpt-4.1", "web_search", 2000000, 1000000, 8000000, 25000, 1, 0.0, None),
    ("openai", "gpt-4.1-mini", "text", 400000, 200000, 1600000, 0, 1, 0.0, None),
    ("openai", "gpt-4.1-mini", "longform", 400000, 200000, 1600000, 0, 1, 0.0, None),
    ("openai", "gpt-4.1-mini", "vision", 400000, 200000, 1600000, 0, 1, 0.0, None),
    ("openai", "gpt-4.1-mini", "web_search", 400000, 200000, 1600000, 25000, 1, 0.0, None),
    ("openai", "gpt-4.1-nano", "text", 100000, 50000, 400000, 0, 1, 0.0, None),
    ("openai", "gpt-4.1-nano", "longform", 100000, 50000, 400000, 0, 1, 0.0, None),
    ("openai", "gpt-4.1-nano", "vision", 100000, 50000, 400000, 0, 1, 0.0, None),
    ("openai", "gpt-4.1-nano", "web_search", 100000, 50000, 400000, 25000, 1, 0.0, None),
    ("openai", "gpt-5", "text", 1250000, 625000, 5000000, 0, 1, 0.0, None),
    ("openai", "gpt-5", "longform", 1250000, 625000, 5000000, 0, 1, 0.0, None),
    ("openai", "gpt-5", "vision", 1250000, 625000, 5000000, 0, 1, 0.0, None),
    # web_search_preview (reasoning families incl. gpt-5): 10 USD / 1K calls = 0.01 USD/call.
    ("openai", "gpt-5", "web_search", 1250000, 625000, 5000000, 10000, 1, 0.0, None),
    ("openai", "gpt-5-mini", "text", 250000, 125000, 1000000, 0, 1, 0.0, None),
    ("openai", "gpt-5-mini", "longform", 250000, 125000, 1000000, 0, 1, 0.0, None),
    ("openai", "gpt-5-mini", "vision", 250000, 125000, 1000000, 0, 1, 0.0, None),
    ("openai", "gpt-5-mini", "web_search", 250000, 125000, 1000000, 10000, 1, 0.0, None),
    ("openai", "gpt-5-nano", "text", 50000, 25000, 200000, 0, 1, 0.0, None),
    ("openai", "gpt-5-nano", "longform", 50000, 25000, 200000, 0, 1, 0.0, None),
    ("openai", "gpt-5-nano", "vision", 50000, 25000, 200000, 0, 1, 0.0, None),
    ("openai", "gpt-5-nano", "web_search", 50000, 25000, 200000, 10000, 1, 0.0, None),
]


# Authentication/session/RBAC compatibility wrappers.
def _auth_now_ts(*args, **kwargs):
    return _auth_runtime._auth_now_ts_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_db_connect(*args, **kwargs):
    return _auth_runtime._auth_db_connect_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_should_touch_session(*args, **kwargs):
    return _auth_runtime._auth_should_touch_session_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_session_cache_get(*args, **kwargs):
    return _auth_runtime._auth_session_cache_get_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_session_cache_set(*args, **kwargs):
    return _auth_runtime._auth_session_cache_set_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_session_cache_drop(*args, **kwargs):
    return _auth_runtime._auth_session_cache_drop_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_session_cache_clear(*args, **kwargs):
    return _auth_runtime._auth_session_cache_clear_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_password_hash(*args, **kwargs):
    return _auth_runtime._auth_password_hash_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_make_password_hash(*args, **kwargs):
    return _auth_runtime._auth_make_password_hash_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_verify_password(*args, **kwargs):
    return _auth_runtime._auth_verify_password_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_token_hash(*args, **kwargs):
    return _auth_runtime._auth_token_hash_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_public_user(*args, **kwargs):
    return _auth_runtime._auth_public_user_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_validate_username(*args, **kwargs):
    return _auth_runtime._auth_validate_username_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_validate_password(*args, **kwargs):
    return _auth_runtime._auth_validate_password_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_validate_avatar_data_url(*args, **kwargs):
    return _auth_runtime._auth_validate_avatar_data_url_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_normalize_concerts_filter_enabled(*args, **kwargs):
    return _auth_runtime._auth_normalize_concerts_filter_enabled_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_normalize_concerts_radius_km(*args, **kwargs):
    return _auth_runtime._auth_normalize_concerts_radius_km_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_normalize_concerts_coord(*args, **kwargs):
    return _auth_runtime._auth_normalize_concerts_coord_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_get_user_by_username(*args, **kwargs):
    return _auth_runtime._auth_get_user_by_username_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_get_user_by_id(*args, **kwargs):
    return _auth_runtime._auth_get_user_by_id_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_bootstrap_required(*args, **kwargs):
    return _auth_runtime._auth_bootstrap_required_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_count_admin_users(*args, **kwargs):
    return _auth_runtime._auth_count_admin_users_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_create_user(*args, **kwargs):
    return _auth_runtime._auth_create_user_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_create_session(*args, **kwargs):
    return _auth_runtime._auth_create_session_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_touch_login(*args, **kwargs):
    return _auth_runtime._auth_touch_login_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_rate_limit_key(*args, **kwargs):
    return _auth_runtime._auth_rate_limit_key_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_rate_limit_prune(*args, **kwargs):
    return _auth_runtime._auth_rate_limit_prune_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_rate_limit_is_blocked(*args, **kwargs):
    return _auth_runtime._auth_rate_limit_is_blocked_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_rate_limit_record_failure(*args, **kwargs):
    return _auth_runtime._auth_rate_limit_record_failure_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_rate_limit_clear(*args, **kwargs):
    return _auth_runtime._auth_rate_limit_clear_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_normalize_ip(*args, **kwargs):
    return _auth_runtime._auth_normalize_ip_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_should_exempt_persistent_ip_ban(*args, **kwargs):
    return _auth_runtime._auth_should_exempt_persistent_ip_ban_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_failure_retention_sec(*args, **kwargs):
    return _auth_runtime._auth_failure_retention_sec_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_cleanup_failure_tracking(*args, **kwargs):
    return _auth_runtime._auth_cleanup_failure_tracking_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_persistent_ip_block_info(*args, **kwargs):
    return _auth_runtime._auth_persistent_ip_block_info_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_record_persistent_ip_failure(*args, **kwargs):
    return _auth_runtime._auth_record_persistent_ip_failure_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_clear_persistent_ip_failures(*args, **kwargs):
    return _auth_runtime._auth_clear_persistent_ip_failures_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_retry_response(*args, **kwargs):
    return _auth_runtime._auth_retry_response_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_get_bearer_token(*args, **kwargs):
    return _auth_runtime._auth_get_bearer_token_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_client_ip(*args, **kwargs):
    return _auth_runtime._auth_client_ip_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_log_safe(*args, **kwargs):
    return _auth_runtime._auth_log_safe_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_security_event(*args, **kwargs):
    return _auth_runtime._auth_security_event_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_apply_failure_delay(*args, **kwargs):
    return _auth_runtime._auth_apply_failure_delay_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_ip_is_private_or_loopback(*args, **kwargs):
    return _auth_runtime._auth_ip_is_private_or_loopback_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_resolve_session(*args, **kwargs):
    return _auth_runtime._auth_resolve_session_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_delete_session(*args, **kwargs):
    return _auth_runtime._auth_delete_session_for_runtime(sys.modules[__name__], *args, **kwargs)

def _current_user_id_or_zero(*args, **kwargs):
    return _auth_runtime._current_user_id_or_zero_for_runtime(sys.modules[__name__], *args, **kwargs)

def _current_user_or_empty(*args, **kwargs):
    return _auth_runtime._current_user_or_empty_for_runtime(sys.modules[__name__], *args, **kwargs)

def _current_username_or_blank(*args, **kwargs):
    return _auth_runtime._current_username_or_blank_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_user_snapshot(*args, **kwargs):
    return _auth_runtime._auth_user_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_active_users_list(*args, **kwargs):
    return _auth_runtime._auth_active_users_list_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_user_can_use_ai(*args, **kwargs):
    return _auth_runtime._auth_user_can_use_ai_for_runtime(sys.modules[__name__], *args, **kwargs)

def _require_admin_json(*args, **kwargs):
    return _auth_runtime._require_admin_json_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_resolve_public_user_scope(*args, **kwargs):
    return _auth_runtime._auth_resolve_public_user_scope_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_is_protected_path(*args, **kwargs):
    return _auth_runtime._auth_is_protected_path_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_is_public_path(*args, **kwargs):
    return _auth_runtime._auth_is_public_path_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_is_self_path(*args, **kwargs):
    return _auth_runtime._auth_is_self_path_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_non_admin_read_allowed(*args, **kwargs):
    return _auth_runtime._auth_non_admin_read_allowed_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_non_admin_write_allowed(*args, **kwargs):
    return _auth_runtime._auth_non_admin_write_allowed_for_runtime(sys.modules[__name__], *args, **kwargs)

def _auth_guard(*args, **kwargs):
    return _auth_runtime._auth_guard_for_runtime(sys.modules[__name__], *args, **kwargs)

app.before_request(_auth_guard)









def _browser_base_url() -> str:
    """
    Keep frontend-facing PMDA links host-agnostic.

    JSON payloads for the Web UI are cached in-process. If we cache absolute URLs
    derived from request.url_root, a payload generated from a LAN origin can be
    replayed to the public hostname (or vice versa), which makes browsers attempt
    cross-origin/private-network fetches for covers, images, or audio streams.
    Returning an empty base keeps every PMDA-served asset same-origin.
    """
    return ""


def _browser_api_url(path: str) -> str:
    raw = str(path or "").strip()
    if not raw:
        return "/"
    api_idx = raw.find("/api/")
    if api_idx > 0:
        raw = raw[api_idx:]
    if not raw.startswith("/"):
        raw = "/" + raw
    return raw


def _normalize_browser_payload_urls(value):
    """
    Recursively strip host/scheme from PMDA-served API URLs before they reach caches
    or browsers. This keeps asset URLs same-origin across LAN/public hosts.
    """
    if isinstance(value, dict):
        for key, inner in list(value.items()):
            value[key] = _normalize_browser_payload_urls(inner)
        return value
    if isinstance(value, list):
        for idx, inner in enumerate(list(value)):
            value[idx] = _normalize_browser_payload_urls(inner)
        return value
    if isinstance(value, tuple):
        return tuple(_normalize_browser_payload_urls(inner) for inner in value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw or "/api/" not in raw:
            return value
        try:
            return _browser_api_url(raw)
        except Exception:
            return value
    return value






def _get_from_sqlite(key: str, default=None):
    """Read a single config value from SQLite settings table (used before merged exists)."""
    try:
        if SETTINGS_DB_FILE.exists():
            con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
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

# Plex is no longer a PMDA source database. SECTION_IDS are retained only as an
# optional preference for post-publication Plex player refresh; PMDA never opens
# Plex to discover scan roots, path maps, or metadata.
SECTION_IDS: list[int] = []
SECTION_NAMES: dict[int, str] = {}

# Load SECTION_IDS from SQLite first so user's saved library selection is never overwritten by auto-detect
try:
    cfg_db_path = SETTINGS_DB_FILE
    if cfg_db_path.exists():
        con = sqlite3.connect(str(cfg_db_path), timeout=5)
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

_startup_library_mode_raw = _get_from_sqlite("LIBRARY_MODE", os.getenv("LIBRARY_MODE", "files"))
_startup_library_mode = str(_startup_library_mode_raw or "files").strip().lower()
if _startup_library_mode == "plex":
    logging.info("Ignoring legacy LIBRARY_MODE=plex; PMDA is files-only in this build.")
_startup_library_mode = "files"
logging.info("Skipping Plex source discovery; files mode is the only scan backend.")

# (4) Merge with environment variables ----------------------------------------
ENV_SOURCES: dict[str, str] = {}

def _get(key: str, *, default=None, cast=lambda x: x):
    """Return the merged value and remember where it came from.
    Priority: SQLite > env > default.
    """
    sqlite_val = None
    sqlite_has_row = False
    try:
        if SETTINGS_DB_FILE.exists():
            con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
            cur = con.cursor()
            cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cur.fetchone()
            con.close()
            if row is not None:
                sqlite_has_row = True
                sqlite_val = row[0]
    except Exception:
        pass

    if sqlite_has_row:
        ENV_SOURCES[key] = "sqlite"
        raw = sqlite_val
        if isinstance(raw, str) and raw.strip() == "" and default is not None:
            ENV_SOURCES[key] = "default"
            raw = default
        else:
            # Backward-compatible "perf default" migration:
            # some older builds persisted defaults into SQLite. When that happens, we treat legacy defaults
            # as if they were unset so the newer recommended defaults apply without forcing a manual reset.
            # Users can still override by setting any other value than the legacy default.
            try:
                raw_str = str(raw).strip()
            except Exception:
                raw_str = ""
            if key == "MB_SEARCH_ALBUM_TIMEOUT_SEC" and raw_str == "20" and default is not None:
                ENV_SOURCES[key] = "default"
                return cast(default)
            if key == "MB_TRACKLIST_FETCH_LIMIT" and raw_str == "2" and default is not None:
                ENV_SOURCES[key] = "default"
                return cast(default)
            return cast(raw)
    env_val = os.getenv(key)
    if env_val is not None:
        raw = env_val
        if isinstance(raw, str) and raw.strip() == "" and default is not None:
            ENV_SOURCES[key] = "default"
            return cast(default)
        ENV_SOURCES[key] = "env"
        return cast(raw)
    ENV_SOURCES[key] = "default"
    return cast(default)


# Old default that included MusicBrainz IDs (caused all albums to show as "incomplete" if no MB tags)
_REQUIRED_TAGS_OLD_DEFAULT_SET = {"artist", "album", "date", "musicbrainz_release_group_id", "musicbrainz_artist_id"}


def _parse_required_tags(val):
    """Return REQUIRED_TAGS as a list of strings. Handles JSON array string from DB, comma-separated string, or list.
    Migrates old default (with musicbrainz_release_group_id, musicbrainz_artist_id) to new default (artist, album, genre, year)."""
    # New default: focus on artist/album plus high-level album metadata (genre + year).
    # "date" is no longer required; year-only is sufficient for completeness.
    default = ["artist", "album", "genre", "year", "tracks"]
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
                if isinstance(t, dict):
                    title = t.get("title")
                    idx = t.get("idx")
                    if idx is None:
                        idx = t.get("index")
                    if idx is None:
                        idx = t.get("track_num")
                else:
                    title = getattr(t, "title", None)
                    idx = getattr(t, "idx", None)
                    if idx is None:
                        idx = getattr(t, "index", None)
                return bool(title and str(title).strip()) and idx is not None
            if not all(_track_ok(t) for t in tracks):
                missing.append(tag)
            continue
        meta_keys = TAG_META_KEYS.get(key, [key])
        if not any((meta or {}).get(k) for k in meta_keys):
            missing.append(tag)
    # Genre is useful but non-blocking for completeness.
    out: list[str] = []
    for tag in missing:
        key = str(tag or "").strip().lower()
        if not key:
            continue
        if key == "genre":
            continue
        out.append(tag)
    return out


def _parse_skip_folders(val):
    """Return SKIP_FOLDERS as a list of path strings. Handles JSON array from DB (or corrupted double-encoded),
    comma-separated string, or list. Drops any element that looks like JSON (e.g. '[]') so corrupted values become []."""
    return _config_core.parse_skip_folders(val)


def _parse_files_roots(val) -> list[str]:
    """
    Return FILES_ROOTS as a normalized list of directory strings.
    Handles:
    - list/tuple values
    - CSV strings
    - JSON array strings
    - corrupted double-encoded JSON strings (e.g. "[\"[\\\"/music\\\"]\"]")
    """
    return _config_core.parse_files_roots(val)


def _parse_format_preference_early(val):
    """Parse FORMAT_PREFERENCE for use in merged (before _parse_format_preference is defined)."""
    return _config_core.parse_format_preference(val)


# ─── Legacy Plex DB helpers (disabled in current files-only builds) ───
PLEX_DB_FILENAME = _plex_source_compat.PLEX_DB_FILENAME
_ALLOW_PLEX_DB_IN_FILES_MODE = False


def _resolve_plex_db_from_base(base_path: str) -> str | None:
    return _plex_source_compat.resolve_plex_db_from_base(base_path)


def _ensure_plex_db_path_resolved() -> str | None:
    """
    Compatibility no-op for old settings.

    PMDA no longer discovers or opens Plex as a source database. The only
    supported Plex behavior is post-publication player refresh via
    `pmda_integrations.player_sync`.
    """
    return _plex_source_compat.ensure_plex_db_path_resolved(sys.modules[__name__])


# PLEX_DB_PATH defaults to /database in container; no SystemExit if unconfigured.
merged = {
    "PLEX_DB_PATH":   (_ensure_plex_db_path_resolved() or _get("PLEX_DB_PATH",   default="/database", cast=str)),
    "PLEX_HOST":      _get("PLEX_HOST",      default="",                                cast=str),
    "PLEX_TOKEN":     _get("PLEX_TOKEN",     default="",                                cast=str),
    "SECTION_ID": SECTION_IDS[0] if SECTION_IDS else 0,
    "SCAN_THREADS":   _get("SCAN_THREADS",   default=os.cpu_count() or 4,               cast=_parse_int),
    "PATH_MAP":       _parse_path_map(_get("PATH_MAP", default={})),
    "DUPE_ROOT":      _get("DUPE_ROOT", default="/dupes", cast=str),
    "LOG_LEVEL":      _get("LOG_LEVEL",      default="INFO").upper(),
    "AI_PROVIDER": _get("AI_PROVIDER", default="ollama", cast=str),
    "OPENAI_API_KEY": _get("OPENAI_API_KEY", default="",                                cast=str),
    "OPENAI_ENABLE_API_KEY_MODE": _get("OPENAI_ENABLE_API_KEY_MODE", default=True, cast=_parse_bool),
    "OPENAI_ENABLE_CODEX_OAUTH_MODE": _get("OPENAI_ENABLE_CODEX_OAUTH_MODE", default=True, cast=_parse_bool),
    "OPENAI_MODEL":   _get("OPENAI_MODEL",   default="gpt-4",                           cast=str),
    "AI_USAGE_LEVEL": _get("AI_USAGE_LEVEL", default="auto",                            cast=str),
    "SCAN_AI_POLICY": _get("SCAN_AI_POLICY", default="local_only",                      cast=str),
    "SCAN_PAID_PROVIDER_ORDER": _get("SCAN_PAID_PROVIDER_ORDER", default="openai-api,openai-codex,anthropic,google", cast=str),
    "WEB_SEARCH_LOCAL_ORDER": _get("WEB_SEARCH_LOCAL_ORDER", default="serper", cast=str),
    "ANTHROPIC_API_KEY": _get("ANTHROPIC_API_KEY", default="", cast=str),
    "GOOGLE_API_KEY": _get("GOOGLE_API_KEY", default="", cast=str),
    "OLLAMA_URL": _get("OLLAMA_URL", default="http://localhost:11434", cast=str),
    "OLLAMA_MODEL": _get("OLLAMA_MODEL", default="qwen3:4b", cast=str),
    "OLLAMA_COMPLEX_MODEL": _get("OLLAMA_COMPLEX_MODEL", default="qwen3:14b", cast=str),
    "DISCORD_WEBHOOK": _get("DISCORD_WEBHOOK", default="", cast=str),
    "USE_MUSICBRAINZ": _get("USE_MUSICBRAINZ", default=True, cast=_parse_bool),
    "MUSICBRAINZ_EMAIL": _get("MUSICBRAINZ_EMAIL", default="pmda@example.com", cast=str),
    "MUSICBRAINZ_MIRROR_ENABLED": _get("MUSICBRAINZ_MIRROR_ENABLED", default=False, cast=_parse_bool),
    "MUSICBRAINZ_BASE_URL": _get("MUSICBRAINZ_BASE_URL", default="", cast=str),
    "MUSICBRAINZ_MIRROR_NAME": _get("MUSICBRAINZ_MIRROR_NAME", default="", cast=str),
    "MB_QUEUE_ENABLED": _get("MB_QUEUE_ENABLED", default=True, cast=_parse_bool),
    "MB_PUBLIC_QUEUE_RPS": _get(
        "MB_PUBLIC_QUEUE_RPS",
        default=1.0,
        cast=lambda x: max(0.1, min(100.0, float(x) if x is not None and str(x).strip().replace(".", "", 1).isdigit() else 1.0)),
    ),
    "MB_MIRROR_QUEUE_RPS": _get(
        "MB_MIRROR_QUEUE_RPS",
        default=12.0,
        cast=lambda x: max(1.0, min(100.0, float(x) if x is not None and str(x).strip().replace(".", "", 1).isdigit() else 12.0)),
    ),
    "MB_MIRROR_QUEUE_WORKERS": _get(
        "MB_MIRROR_QUEUE_WORKERS",
        default=4,
        cast=lambda x: max(1, min(32, int(x) if x is not None and str(x).strip().isdigit() else 4)),
    ),
    "MB_RETRY_NOT_FOUND": _get("MB_RETRY_NOT_FOUND", default=False, cast=_parse_bool),
    # Time budget (seconds) per album for MusicBrainz search flow before we fall back faster to other providers.
    "MB_SEARCH_ALBUM_TIMEOUT_SEC": _get(
        "MB_SEARCH_ALBUM_TIMEOUT_SEC",
        # 0 = unlimited per-album MusicBrainz search budget. PMDA should prefer correctness over premature fallback.
        default=0,
        cast=lambda x: max(0, min(3600, int(x) if x is not None and str(x).strip().isdigit() else 0)),
    ),
    # Maximum number of MB candidates for which we fetch full details per album.
    "MB_CANDIDATE_FETCH_LIMIT": _get(
        "MB_CANDIDATE_FETCH_LIMIT",
        # 0 = unlimited candidate detail fetch.
        default=0,
        cast=lambda x: max(0, min(100, int(x) if x is not None and str(x).strip().isdigit() else 0)),
    ),
    # Fetch recording-level track titles only for first N candidates (expensive endpoint).
    "MB_TRACKLIST_FETCH_LIMIT": _get(
        "MB_TRACKLIST_FETCH_LIMIT",
        # 0 = fetch tracklists for all detailed candidates.
        default=0,
        cast=lambda x: max(0, min(100, int(x) if x is not None and str(x).strip().isdigit() else 0)),
    ),
    # Disabled by default: correctness first, we do not skip web+AI MBID hunt merely for speed.
    "MB_FAST_FALLBACK_MODE": _get("MB_FAST_FALLBACK_MODE", default=False, cast=_parse_bool),
    # Provider identity arbitration (Discogs/Last.fm/Bandcamp when MB is unavailable).
    "PROVIDER_IDENTITY_STRICT": _get("PROVIDER_IDENTITY_STRICT", default=True, cast=_parse_bool),
    "PROVIDER_IDENTITY_USE_AI": _get("PROVIDER_IDENTITY_USE_AI", default=True, cast=_parse_bool),
    "MATCH_COVER_OCR_MODE": _get("MATCH_COVER_OCR_MODE", default="smart", cast=_normalize_match_cover_ocr_mode),
    "PROVIDER_IDENTITY_MIN_SCORE": _get(
        "PROVIDER_IDENTITY_MIN_SCORE",
        default=0.72,
        cast=lambda x: float(x) if x is not None and str(x).replace(".", "", 1).replace("-", "", 1).isdigit() else 0.72,
    ),
    "PROVIDER_IDENTITY_SCORE_MARGIN": _get(
        "PROVIDER_IDENTITY_SCORE_MARGIN",
        default=0.08,
        cast=lambda x: float(x) if x is not None and str(x).replace(".", "", 1).replace("-", "", 1).isdigit() else 0.08,
    ),
    # Provider lookup cache TTLs (seconds). For a 60k+ album library, negative
    # provider answers must survive across multi-day runs; otherwise PMDA keeps
    # re-querying the same misses and timeouts.
    "PROVIDER_CACHE_FOUND_TTL_SEC": _get(
        "PROVIDER_CACHE_FOUND_TTL_SEC",
        default=60 * 60 * 24 * 30,
        cast=lambda x: max(60, min(60 * 60 * 24 * 365, int(x) if x is not None and str(x).strip().isdigit() else 60 * 60 * 24 * 30)),
    ),
    "PROVIDER_CACHE_NOT_FOUND_TTL_SEC": _get(
        "PROVIDER_CACHE_NOT_FOUND_TTL_SEC",
        default=60 * 60 * 24 * 30,
        cast=lambda x: max(60, min(60 * 60 * 24 * 30, int(x) if x is not None and str(x).strip().isdigit() else 60 * 60 * 24 * 30)),
    ),
    "PROVIDER_CACHE_ERROR_TTL_SEC": _get(
        "PROVIDER_CACHE_ERROR_TTL_SEC",
        default=60 * 60 * 6,
        cast=lambda x: max(30, min(60 * 60 * 24, int(x) if x is not None and str(x).strip().isdigit() else 60 * 60 * 6)),
    ),
    "PROVIDER_GATEWAY_ENABLED": _get("PROVIDER_GATEWAY_ENABLED", default=True, cast=_parse_bool),
    "PROVIDER_GATEWAY_CACHE_ENABLED": _get("PROVIDER_GATEWAY_CACHE_ENABLED", default=True, cast=_parse_bool),
    "PROVIDER_GATEWAY_MAX_INFLIGHT": _get(
        "PROVIDER_GATEWAY_MAX_INFLIGHT",
        default=16,
        cast=lambda x: max(1, min(256, int(x) if x is not None and str(x).strip().isdigit() else 16)),
    ),
    "PROVIDER_GATEWAY_DISCOGS_RPM": _get(
        "PROVIDER_GATEWAY_DISCOGS_RPM",
        default=40,
        cast=lambda x: max(1, min(600, int(x) if x is not None and str(x).strip().isdigit() else 40)),
    ),
    "PROVIDER_GATEWAY_LASTFM_RPM": _get(
        "PROVIDER_GATEWAY_LASTFM_RPM",
        default=120,
        cast=lambda x: max(1, min(6000, int(x) if x is not None and str(x).strip().isdigit() else 120)),
    ),
    "PROVIDER_GATEWAY_ITUNES_RPM": _get(
        "PROVIDER_GATEWAY_ITUNES_RPM",
        default=180,
        cast=lambda x: max(1, min(6000, int(x) if x is not None and str(x).strip().isdigit() else 180)),
    ),
    "PROVIDER_GATEWAY_DEEZER_RPM": _get(
        "PROVIDER_GATEWAY_DEEZER_RPM",
        default=120,
        cast=lambda x: max(1, min(6000, int(x) if x is not None and str(x).strip().isdigit() else 120)),
    ),
    "PROVIDER_GATEWAY_SPOTIFY_RPM": _get(
        "PROVIDER_GATEWAY_SPOTIFY_RPM",
        default=60,
        cast=lambda x: max(1, min(6000, int(x) if x is not None and str(x).strip().isdigit() else 60)),
    ),
    "PROVIDER_GATEWAY_QOBUZ_RPM": _get(
        "PROVIDER_GATEWAY_QOBUZ_RPM",
        default=40,
        cast=lambda x: max(1, min(6000, int(x) if x is not None and str(x).strip().isdigit() else 40)),
    ),
    "PROVIDER_GATEWAY_TIDAL_RPM": _get(
        "PROVIDER_GATEWAY_TIDAL_RPM",
        default=20,
        cast=lambda x: max(1, min(6000, int(x) if x is not None and str(x).strip().isdigit() else 20)),
    ),
    "PROVIDER_GATEWAY_AUDIODB_RPM": _get(
        "PROVIDER_GATEWAY_AUDIODB_RPM",
        default=60,
        cast=lambda x: max(1, min(6000, int(x) if x is not None and str(x).strip().isdigit() else 60)),
    ),
    "PROVIDER_GATEWAY_BANDCAMP_RPM": _get(
        "PROVIDER_GATEWAY_BANDCAMP_RPM",
        default=12,
        cast=lambda x: max(1, min(600, int(x) if x is not None and str(x).strip().isdigit() else 12)),
    ),
    "AUTO_TUNE_ENABLED": _get("AUTO_TUNE_ENABLED", default=True, cast=_parse_bool),
    "AUTO_TUNE_INTERVAL_SEC": _get(
        "AUTO_TUNE_INTERVAL_SEC",
        default=60,
        cast=lambda x: max(15, min(900, int(x) if x is not None and str(x).strip().isdigit() else 60)),
    ),
    "AUTO_TUNE_MB_MIRROR_MIN_RPS": _get(
        "AUTO_TUNE_MB_MIRROR_MIN_RPS",
        default=12.0,
        cast=lambda x: max(1.0, min(100.0, float(x) if x is not None and str(x).strip() else 12.0)),
    ),
    "AUTO_TUNE_MB_MIRROR_MAX_RPS": _get(
        "AUTO_TUNE_MB_MIRROR_MAX_RPS",
        default=20.0,
        cast=lambda x: max(1.0, min(100.0, float(x) if x is not None and str(x).strip() else 20.0)),
    ),
    "AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN": _get(
        "AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN",
        default=8,
        cast=lambda x: max(1, min(256, int(x) if x is not None and str(x).strip().isdigit() else 8)),
    ),
    "AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP": _get(
        "AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP",
        default=32,
        cast=lambda x: max(1, min(256, int(x) if x is not None and str(x).strip().isdigit() else 32)),
    ),
    # When true, ignore cached MusicBrainz results and stored MBID tags for album→MBID lookup.
    # This forces a full lookup on every scan and is intended for advanced testing/debugging only.
    "MB_DISABLE_CACHE": _get("MB_DISABLE_CACHE", default=False, cast=_parse_bool),
    # When true, ignore *all* caches during scans (audio format cache and MusicBrainz cache).
    # This forces the scan to re-run the full analysis and metadata flow even when cache entries exist.
    "SCAN_DISABLE_CACHE": _get("SCAN_DISABLE_CACHE", default=False, cast=_parse_bool),
    "LIDARR_URL": _get("LIDARR_URL", default="", cast=str),
    "LIDARR_API_KEY": _get("LIDARR_API_KEY", default="", cast=str),
    "AUTOBRR_URL": _get("AUTOBRR_URL", default="", cast=str),
    "AUTOBRR_API_KEY": _get("AUTOBRR_API_KEY", default="", cast=str),
    "AUTO_FIX_BROKEN_ALBUMS": _get("AUTO_FIX_BROKEN_ALBUMS", default=False, cast=_parse_bool),
    "BROKEN_ALBUM_CONSECUTIVE_THRESHOLD": _get("BROKEN_ALBUM_CONSECUTIVE_THRESHOLD", default=2, cast=int),
    "BROKEN_ALBUM_PERCENTAGE_THRESHOLD": _get("BROKEN_ALBUM_PERCENTAGE_THRESHOLD", default=0.20, cast=float),
    # Files-first default required tags: artist/album/genre/year + track numbers/titles.
    "REQUIRED_TAGS": _get("REQUIRED_TAGS", default="artist,album,genre,year,tracks", cast=_parse_required_tags),
    "SKIP_FOLDERS": _get("SKIP_FOLDERS", default="", cast=_parse_skip_folders),
    "AI_BATCH_SIZE": _get("AI_BATCH_SIZE", default=10, cast=int),
    "FFPROBE_POOL_SIZE": _get("FFPROBE_POOL_SIZE", default=8, cast=int),
    "AUTO_MOVE_DUPES": _get("AUTO_MOVE_DUPES", default=False, cast=_parse_bool),
    "AUTO_EXPORT_LIBRARY": _get("AUTO_EXPORT_LIBRARY", default=False, cast=_parse_bool),
    "METADATA_QUEUE_ENABLED": _get("METADATA_QUEUE_ENABLED", default=False, cast=_parse_bool),
    "METADATA_WORKER_MODE": _get("METADATA_WORKER_MODE", default="local", cast=str),
    "METADATA_WORKER_COUNT": _get(
        "METADATA_WORKER_COUNT",
        default=0,
        cast=lambda x: max(0, min(128, int(x) if x is not None and str(x).strip().isdigit() else 0)),
    ),
    "METADATA_JOB_BATCH_SIZE": _get(
        "METADATA_JOB_BATCH_SIZE",
        default=0,
        cast=lambda x: max(0, min(500, int(x) if x is not None and str(x).strip().isdigit() else 0)),
    ),
    # End-to-end pipeline steps (all configurable from Settings)
    "PIPELINE_ENABLE_MATCH_FIX": _get("PIPELINE_ENABLE_MATCH_FIX", default=True, cast=_parse_bool),
    "PIPELINE_ENABLE_DEDUPE": _get("PIPELINE_ENABLE_DEDUPE", default=True, cast=_parse_bool),
    "PIPELINE_ENABLE_INCOMPLETE_MOVE": _get("PIPELINE_ENABLE_INCOMPLETE_MOVE", default=False, cast=_parse_bool),
    "PIPELINE_ENABLE_EXPORT": _get("PIPELINE_ENABLE_EXPORT", default=False, cast=_parse_bool),
    "PIPELINE_ENABLE_PLAYER_SYNC": _get("PIPELINE_ENABLE_PLAYER_SYNC", default=False, cast=_parse_bool),
    "PIPELINE_PLAYER_TARGET": _get("PIPELINE_PLAYER_TARGET", default="none", cast=str),
    # When true, post-scan heavy stages are queued as background jobs (scheduler/chain) instead of blocking scan completion.
    "PIPELINE_POST_SCAN_ASYNC": _get("PIPELINE_POST_SCAN_ASYNC", default=True, cast=_parse_bool),
    # In-app task notifications (toasts)
    "TASK_NOTIFICATIONS_ENABLED": _get("TASK_NOTIFICATIONS_ENABLED", default=True, cast=_parse_bool),
    "TASK_NOTIFICATIONS_SUCCESS": _get("TASK_NOTIFICATIONS_SUCCESS", default=True, cast=_parse_bool),
    "TASK_NOTIFICATIONS_FAILURE": _get("TASK_NOTIFICATIONS_FAILURE", default=True, cast=_parse_bool),
    "TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN": _get("TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN", default=False, cast=_parse_bool),
    "TASK_NOTIFICATIONS_COOLDOWN_SEC": _get(
        "TASK_NOTIFICATIONS_COOLDOWN_SEC",
        default=20,
        cast=lambda x: max(0, min(3600, int(x) if x is not None and str(x).strip().isdigit() else 20)),
    ),
    "TASK_NOTIFY_SCAN_CHANGED": _get("TASK_NOTIFY_SCAN_CHANGED", default=True, cast=_parse_bool),
    "TASK_NOTIFY_SCAN_FULL": _get("TASK_NOTIFY_SCAN_FULL", default=True, cast=_parse_bool),
    "TASK_NOTIFY_ENRICH_BATCH": _get("TASK_NOTIFY_ENRICH_BATCH", default=True, cast=_parse_bool),
    "TASK_NOTIFY_DEDUPE": _get("TASK_NOTIFY_DEDUPE", default=True, cast=_parse_bool),
    "TASK_NOTIFY_INCOMPLETE_MOVE": _get("TASK_NOTIFY_INCOMPLETE_MOVE", default=True, cast=_parse_bool),
    "TASK_NOTIFY_EXPORT": _get("TASK_NOTIFY_EXPORT", default=True, cast=_parse_bool),
    "TASK_NOTIFY_PLAYER_SYNC": _get("TASK_NOTIFY_PLAYER_SYNC", default=True, cast=_parse_bool),
    # External media player integrations (scan trigger only)
    "JELLYFIN_URL": _get("JELLYFIN_URL", default="", cast=str),
    "JELLYFIN_API_KEY": _get("JELLYFIN_API_KEY", default="", cast=str),
    "NAVIDROME_URL": _get("NAVIDROME_URL", default="", cast=str),
    "NAVIDROME_USERNAME": _get("NAVIDROME_USERNAME", default="", cast=str),
    "NAVIDROME_PASSWORD": _get("NAVIDROME_PASSWORD", default="", cast=str),
    "NAVIDROME_API_KEY": _get("NAVIDROME_API_KEY", default="", cast=str),
    "USE_AI_FOR_MB_MATCH": _get("USE_AI_FOR_MB_MATCH", default=True, cast=_parse_bool),
    "USE_AI_FOR_MB_VERIFY": _get("USE_AI_FOR_MB_VERIFY", default=True, cast=_parse_bool),
    "USE_AI_FOR_DEDUPE": _get("USE_AI_FOR_DEDUPE", default=True, cast=_parse_bool),
    # When enabled, PMDA can auto-fetch provider album profiles for SOFT_MATCH albums.
    "USE_AI_FOR_SOFT_MATCH_PROFILES": _get("USE_AI_FOR_SOFT_MATCH_PROFILES", default=False, cast=_parse_bool),
    "USE_AI_VISION_FOR_COVER": _get("USE_AI_VISION_FOR_COVER", default=True, cast=_parse_bool),
    "AI_CONFIDENCE_MIN": _get("AI_CONFIDENCE_MIN", default=50, cast=lambda x: int(x) if x is not None and str(x).strip().isdigit() else 50),
    "OPENAI_VISION_MODEL": _get("OPENAI_VISION_MODEL", default="", cast=str),
    "USE_AI_VISION_BEFORE_COVER_INJECT": _get("USE_AI_VISION_BEFORE_COVER_INJECT", default=True, cast=_parse_bool),
    # AI cost controls for MusicBrainz "verify" prompt size.
    "AI_MB_VERIFY_MAX_CANDIDATES": _get("AI_MB_VERIFY_MAX_CANDIDATES", default=8, cast=lambda x: max(1, min(20, int(x) if x is not None and str(x).strip().isdigit() else 8))),
    "AI_MB_VERIFY_LOCAL_TRACK_PREVIEW": _get("AI_MB_VERIFY_LOCAL_TRACK_PREVIEW", default=12, cast=lambda x: max(0, min(60, int(x) if x is not None and str(x).strip().isdigit() else 12))),
    "AI_MB_VERIFY_MB_TRACK_PREVIEW": _get("AI_MB_VERIFY_MB_TRACK_PREVIEW", default=5, cast=lambda x: max(0, min(30, int(x) if x is not None and str(x).strip().isdigit() else 5))),
    "BACKUP_BEFORE_FIX": _get("BACKUP_BEFORE_FIX", default=False, cast=_parse_bool),
    "MAGIC_MODE": _get("MAGIC_MODE", default=False, cast=_parse_bool),
    "IMPROVE_ALL_WORKERS": _get("IMPROVE_ALL_WORKERS", default=1, cast=lambda x: max(1, min(8, int(x) if x is not None and str(x).strip().isdigit() else 1))),
    "USE_ACOUSTID": _get("USE_ACOUSTID", default=True, cast=_parse_bool),
    "ACOUSTID_API_KEY": _get("ACOUSTID_API_KEY", default="", cast=str),
    "USE_ACOUSTID_WHEN_TAGGED": _get("USE_ACOUSTID_WHEN_TAGGED", default="false", cast=_parse_bool),
    "USE_WEB_SEARCH_FOR_MB": _get("USE_WEB_SEARCH_FOR_MB", default=True, cast=_parse_bool),
    "WEB_SEARCH_PROVIDER": _get("WEB_SEARCH_PROVIDER", default="auto", cast=str),
    # When enabled, PMDA can use local Ollama web search as fallback when provider search is weak/missing.
    "USE_AI_WEB_SEARCH_FALLBACK": _get("USE_AI_WEB_SEARCH_FALLBACK", default=True, cast=_parse_bool),
    "SCHEDULER_ALLOW_NON_SCAN_JOBS": _get("SCHEDULER_ALLOW_NON_SCAN_JOBS", default=SCHEDULER_ALLOW_NON_SCAN_JOBS, cast=_parse_bool),
    # PMDA no longer enforces hard AI call caps/cooldowns. Cost control is handled by batching,
    # caching and provider routing, not by blocking the pipeline mid-scan.
    "AI_MAX_CALLS_PER_SCAN": _get(
        "AI_MAX_CALLS_PER_SCAN",
        default=0,
        cast=lambda x: 0,
    ),
    "AI_CALL_COOLDOWN_SEC": _get(
        "AI_CALL_COOLDOWN_SEC",
        default=0.0,
        cast=lambda x: 0.0,
    ),
    "AI_GLOBAL_MAX_CALLS_PER_MINUTE": _get(
        "AI_GLOBAL_MAX_CALLS_PER_MINUTE",
        default=0,
        cast=lambda x: 0,
    ),
    "AI_GLOBAL_MAX_CALLS_PER_DAY": _get(
        "AI_GLOBAL_MAX_CALLS_PER_DAY",
        default=0,
        cast=lambda x: 0,
    ),
    "SERPER_API_KEY": _get("SERPER_API_KEY", default="", cast=str),
    "CROSS_LIBRARY_DEDUPE": _get("CROSS_LIBRARY_DEDUPE", default="true", cast=_parse_bool),
    "CROSSCHECK_SAMPLES": _get("CROSSCHECK_SAMPLES", default=20, cast=lambda x: int(x) if x is not None and str(x).strip().isdigit() else 20),
    "LOG_FILE": _get("LOG_FILE", default="", cast=str) or str(CONFIG_DIR / "pmda.log"),
    "OPENAI_MODEL_FALLBACKS": _get("OPENAI_MODEL_FALLBACKS", default="", cast=str),
    "DISABLE_PATH_CROSSCHECK": _get("DISABLE_PATH_CROSSCHECK", default="false", cast=_parse_bool),
    "FORMAT_PREFERENCE": _get("FORMAT_PREFERENCE", default=None, cast=lambda v: _parse_format_preference_early(v)),
    "USE_DISCOGS": _get("USE_DISCOGS", default=True, cast=_parse_bool),
    "DISCOGS_USER_TOKEN": _get("DISCOGS_USER_TOKEN", default="", cast=str),
    "USE_ITUNES": _get("USE_ITUNES", default=True, cast=_parse_bool),
    "USE_DEEZER": _get("USE_DEEZER", default=True, cast=_parse_bool),
    "USE_SPOTIFY": _get("USE_SPOTIFY", default=True, cast=_parse_bool),
    "USE_QOBUZ": _get("USE_QOBUZ", default=True, cast=_parse_bool),
    "USE_TIDAL": _get("USE_TIDAL", default=True, cast=_parse_bool),
    "USE_LASTFM": _get("USE_LASTFM", default=True, cast=_parse_bool),
    "LASTFM_API_KEY": _get("LASTFM_API_KEY", default="", cast=str),
    "LASTFM_API_SECRET": _get("LASTFM_API_SECRET", default="", cast=str),
    # Extra artist image providers (optional)
    "FANART_API_KEY": _get("FANART_API_KEY", default="", cast=str),
    "THEAUDIODB_API_KEY": _get("THEAUDIODB_API_KEY", default="", cast=str),
    "USE_BANDCAMP": _get("USE_BANDCAMP", default=True, cast=_parse_bool),
    # Artist / metadata modes
    "ARTIST_CREDIT_MODE": _get("ARTIST_CREDIT_MODE", default="picard_like_default", cast=str),
    "CLASSICAL_NAME_PREFERENCE": _get("CLASSICAL_NAME_PREFERENCE", default="original", cast=str),
    "LIVE_DEDUPE_MODE": _get("LIVE_DEDUPE_MODE", default="safe", cast=str),
    "SKIP_MB_FOR_LIVE_ALBUMS": _get("SKIP_MB_FOR_LIVE_ALBUMS", default="true", cast=_parse_bool),
    "TRACKLIST_MATCH_MIN": _get("TRACKLIST_MATCH_MIN", default="0.9", cast=lambda x: float(x) if x is not None and str(x).replace(".", "", 1).replace("-", "", 1).isdigit() else 0.9),
    "LIVE_ALBUMS_MB_STRICT": _get("LIVE_ALBUMS_MB_STRICT", default="false", cast=_parse_bool),
    # Improve-all reprocess policy
    "REPROCESS_INCOMPLETE_ALBUMS": _get("REPROCESS_INCOMPLETE_ALBUMS", default="true", cast=_parse_bool),
    # Library backend & file-library settings
    "LIBRARY_MODE": _get("LIBRARY_MODE", default="files", cast=str),
    "LIBRARY_INCLUDE_UNMATCHED": _get("LIBRARY_INCLUDE_UNMATCHED", default=True, cast=_parse_bool),
    "FILES_TAG_WRITE_MODE": _get("FILES_TAG_WRITE_MODE", default="full", cast=str),
    "FILES_ROOTS": _get("FILES_ROOTS", default="", cast=_parse_files_roots),
    "STORAGE_POWER_SAVER_ENABLED": _get("STORAGE_POWER_SAVER_ENABLED", default="false", cast=_parse_bool),
    "STORAGE_PROVIDER": _get("STORAGE_PROVIDER", default="unraid", cast=str),
    "UNRAID_HOST_MNT_ROOT": _get("UNRAID_HOST_MNT_ROOT", default="/host_mnt", cast=str),
    "UNRAID_USER_SHARE_HOST_ROOT": _get("UNRAID_USER_SHARE_HOST_ROOT", default="/host_mnt/user/MURRAY/Music", cast=str),
    "UNRAID_CONTAINER_SHARE_ROOT": _get("UNRAID_CONTAINER_SHARE_ROOT", default="/music", cast=str),
    "STORAGE_MAX_ACTIVE_DEVICES": _get("STORAGE_MAX_ACTIVE_DEVICES", default=1, cast=lambda v: max(1, min(64, _parse_int(v, 1) or 1))),
    "STORAGE_SPINDOWN_POLICY": _get("STORAGE_SPINDOWN_POLICY", default="none", cast=str),
    "LIBRARY_WINNER_PLACEMENT_STRATEGY": _get("LIBRARY_WINNER_PLACEMENT_STRATEGY", default="move", cast=str),
    "WINNER_SOURCE_ROOT_ID": _get("WINNER_SOURCE_ROOT_ID", default="", cast=str),
    "EXPORT_ROOT": _get("EXPORT_ROOT", default="", cast=str),
    "EXPORT_NAMING_TEMPLATE": _get("EXPORT_NAMING_TEMPLATE", default="", cast=str),
    "EXPORT_LINK_STRATEGY": _get("EXPORT_LINK_STRATEGY", default="hardlink", cast=str),
    "EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER": _get("EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER", default=False, cast=_parse_bool),
    "EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER": _get("EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER", default=False, cast=_parse_bool),
    "MEDIA_CACHE_ROOT": _get("MEDIA_CACHE_ROOT", default=PMDA_MEDIA_CACHE_ROOT or str(CONFIG_DIR / "media_cache"), cast=str),
    "ARTWORK_RAM_CACHE_MB": _get("ARTWORK_RAM_CACHE_MB", default=PMDA_ARTWORK_RAM_CACHE_MB, cast=lambda v: max(0, _parse_int(v, PMDA_ARTWORK_RAM_CACHE_MB) or PMDA_ARTWORK_RAM_CACHE_MB)),
    "ARTWORK_RAM_CACHE_TTL_SEC": _get("ARTWORK_RAM_CACHE_TTL_SEC", default=PMDA_ARTWORK_RAM_CACHE_TTL_SEC, cast=lambda v: max(60, _parse_int(v, PMDA_ARTWORK_RAM_CACHE_TTL_SEC) or PMDA_ARTWORK_RAM_CACHE_TTL_SEC)),
    "ARTWORK_RAM_CACHE_MAX_ITEM_MB": _get("ARTWORK_RAM_CACHE_MAX_ITEM_MB", default=PMDA_ARTWORK_RAM_CACHE_MAX_ITEM_MB, cast=lambda v: max(1, _parse_int(v, PMDA_ARTWORK_RAM_CACHE_MAX_ITEM_MB) or PMDA_ARTWORK_RAM_CACHE_MAX_ITEM_MB)),
    "ARTWORK_RAM_CACHE_AUTO": _get("ARTWORK_RAM_CACHE_AUTO", default=PMDA_ARTWORK_RAM_CACHE_AUTO, cast=_parse_bool),
    "ARTWORK_RAM_CACHE_AUTO_MAX_MB": _get("ARTWORK_RAM_CACHE_AUTO_MAX_MB", default=PMDA_ARTWORK_RAM_CACHE_AUTO_MAX_MB, cast=lambda v: max(0, _parse_int(v, PMDA_ARTWORK_RAM_CACHE_AUTO_MAX_MB) or PMDA_ARTWORK_RAM_CACHE_AUTO_MAX_MB)),
    "ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC": _get("ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC", default=PMDA_ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC, cast=lambda v: max(30, _parse_int(v, PMDA_ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC) or PMDA_ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC)),
}
# PATH_MAP and all config from _get() (SQLite only > default)

SKIP_FOLDERS: list[str] = merged["SKIP_FOLDERS"]
LIBRARY_MODE: str = str(merged.get("LIBRARY_MODE", "files") or "files").strip().lower()
LIBRARY_INCLUDE_UNMATCHED: bool = bool(merged.get("LIBRARY_INCLUDE_UNMATCHED", True))
FILES_TAG_WRITE_MODE: str = str(merged.get("FILES_TAG_WRITE_MODE", "full") or "full").strip().lower()
if FILES_TAG_WRITE_MODE not in {"full", "pmda_id_only"}:
    FILES_TAG_WRITE_MODE = "full"
FILES_ROOTS: list[str] = merged.get("FILES_ROOTS", []) or []
STORAGE_POWER_SAVER_ENABLED: bool = bool(merged.get("STORAGE_POWER_SAVER_ENABLED", False))
STORAGE_PROVIDER: str = str(merged.get("STORAGE_PROVIDER", "unraid") or "unraid").strip().lower()
if STORAGE_PROVIDER not in {"unraid"}:
    STORAGE_PROVIDER = "unraid"
UNRAID_HOST_MNT_ROOT: str = str(merged.get("UNRAID_HOST_MNT_ROOT", "/host_mnt") or "/host_mnt").strip() or "/host_mnt"
UNRAID_USER_SHARE_HOST_ROOT: str = str(merged.get("UNRAID_USER_SHARE_HOST_ROOT", "/host_mnt/user/MURRAY/Music") or "/host_mnt/user/MURRAY/Music").strip() or "/host_mnt/user/MURRAY/Music"
UNRAID_CONTAINER_SHARE_ROOT: str = str(merged.get("UNRAID_CONTAINER_SHARE_ROOT", "/music") or "/music").strip() or "/music"
STORAGE_MAX_ACTIVE_DEVICES: int = int(max(1, min(64, _parse_int(merged.get("STORAGE_MAX_ACTIVE_DEVICES"), 1) or 1)))
STORAGE_SPINDOWN_POLICY: str = str(merged.get("STORAGE_SPINDOWN_POLICY", "none") or "none").strip().lower()
if STORAGE_SPINDOWN_POLICY not in {"none"}:
    STORAGE_SPINDOWN_POLICY = "none"
LIBRARY_WINNER_PLACEMENT_STRATEGY: str = str(merged.get("LIBRARY_WINNER_PLACEMENT_STRATEGY", "move") or "move").strip().lower()
if LIBRARY_WINNER_PLACEMENT_STRATEGY not in {"move", "hardlink", "symlink", "copy"}:
    LIBRARY_WINNER_PLACEMENT_STRATEGY = "move"
WINNER_SOURCE_ROOT_ID: str = str(merged.get("WINNER_SOURCE_ROOT_ID", "") or "").strip()
EXPORT_ROOT: str = str(merged.get("EXPORT_ROOT", "") or "").strip()
EXPORT_NAMING_TEMPLATE: str = str(merged.get("EXPORT_NAMING_TEMPLATE", "") or "").strip()
EXPORT_LINK_STRATEGY: str = str(merged.get("EXPORT_LINK_STRATEGY", "hardlink") or "hardlink").strip().lower()
if EXPORT_LINK_STRATEGY not in {"hardlink", "symlink", "copy", "move"}:
    EXPORT_LINK_STRATEGY = "hardlink"
EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER: bool = bool(merged.get("EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER", False))
EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER: bool = bool(merged.get("EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER", False))
MEDIA_CACHE_ROOT: str = str(merged.get("MEDIA_CACHE_ROOT", PMDA_MEDIA_CACHE_ROOT or str(CONFIG_DIR / "media_cache")) or str(CONFIG_DIR / "media_cache")).strip()
ARTWORK_RAM_CACHE_MB: int = int(max(0, _parse_int(merged.get("ARTWORK_RAM_CACHE_MB"), PMDA_ARTWORK_RAM_CACHE_MB) or PMDA_ARTWORK_RAM_CACHE_MB))
ARTWORK_RAM_CACHE_TTL_SEC: int = int(max(60, _parse_int(merged.get("ARTWORK_RAM_CACHE_TTL_SEC"), PMDA_ARTWORK_RAM_CACHE_TTL_SEC) or PMDA_ARTWORK_RAM_CACHE_TTL_SEC))
ARTWORK_RAM_CACHE_MAX_ITEM_MB: int = int(max(1, _parse_int(merged.get("ARTWORK_RAM_CACHE_MAX_ITEM_MB"), PMDA_ARTWORK_RAM_CACHE_MAX_ITEM_MB) or PMDA_ARTWORK_RAM_CACHE_MAX_ITEM_MB))
ARTWORK_RAM_CACHE_AUTO: bool = bool(merged.get("ARTWORK_RAM_CACHE_AUTO", PMDA_ARTWORK_RAM_CACHE_AUTO))
ARTWORK_RAM_CACHE_AUTO_MAX_MB: int = int(max(0, _parse_int(merged.get("ARTWORK_RAM_CACHE_AUTO_MAX_MB"), PMDA_ARTWORK_RAM_CACHE_AUTO_MAX_MB) or PMDA_ARTWORK_RAM_CACHE_AUTO_MAX_MB))
ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC: int = int(max(30, _parse_int(merged.get("ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC"), PMDA_ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC) or PMDA_ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC))
USE_MUSICBRAINZ: bool = bool(merged["USE_MUSICBRAINZ"])
MUSICBRAINZ_EMAIL: str = merged.get("MUSICBRAINZ_EMAIL", "pmda@example.com")
MUSICBRAINZ_MIRROR_ENABLED: bool = bool(merged.get("MUSICBRAINZ_MIRROR_ENABLED", False))
MUSICBRAINZ_BASE_URL: str = str(merged.get("MUSICBRAINZ_BASE_URL", "") or "").strip()
MUSICBRAINZ_MIRROR_NAME: str = str(merged.get("MUSICBRAINZ_MIRROR_NAME", "") or "").strip()
MB_QUEUE_ENABLED: bool = bool(merged.get("MB_QUEUE_ENABLED", True))
MB_PUBLIC_QUEUE_RPS: float = float(merged.get("MB_PUBLIC_QUEUE_RPS", 1.0) or 1.0)
MB_MIRROR_QUEUE_RPS: float = float(merged.get("MB_MIRROR_QUEUE_RPS", 12.0) or 12.0)
MB_MIRROR_QUEUE_WORKERS: int = int(max(1, min(32, int(merged.get("MB_MIRROR_QUEUE_WORKERS", 4) or 4))))
MB_RETRY_NOT_FOUND: bool = bool(merged.get("MB_RETRY_NOT_FOUND", False))
MB_SEARCH_ALBUM_TIMEOUT_SEC: int = int(merged.get("MB_SEARCH_ALBUM_TIMEOUT_SEC", 0))
MB_CANDIDATE_FETCH_LIMIT: int = int(merged.get("MB_CANDIDATE_FETCH_LIMIT", 0))
MB_TRACKLIST_FETCH_LIMIT: int = int(merged.get("MB_TRACKLIST_FETCH_LIMIT", 0))
MB_FAST_FALLBACK_MODE: bool = bool(merged.get("MB_FAST_FALLBACK_MODE", False))
PROVIDER_IDENTITY_STRICT: bool = bool(merged.get("PROVIDER_IDENTITY_STRICT", True))
PROVIDER_IDENTITY_USE_AI: bool = bool(merged.get("PROVIDER_IDENTITY_USE_AI", True))
MATCH_COVER_OCR_MODE: str = _normalize_match_cover_ocr_mode(merged.get("MATCH_COVER_OCR_MODE", "smart"))
PROVIDER_IDENTITY_MIN_SCORE: float = float(merged.get("PROVIDER_IDENTITY_MIN_SCORE", 0.72))
PROVIDER_IDENTITY_SCORE_MARGIN: float = float(merged.get("PROVIDER_IDENTITY_SCORE_MARGIN", 0.08))
PROVIDER_CACHE_FOUND_TTL_SEC: int = int(merged.get("PROVIDER_CACHE_FOUND_TTL_SEC", 60 * 60 * 24 * 30))
PROVIDER_CACHE_NOT_FOUND_TTL_SEC: int = int(merged.get("PROVIDER_CACHE_NOT_FOUND_TTL_SEC", 60 * 60 * 24 * 30))
PROVIDER_CACHE_ERROR_TTL_SEC: int = int(merged.get("PROVIDER_CACHE_ERROR_TTL_SEC", 60 * 60 * 6))
PROVIDER_GATEWAY_ENABLED: bool = bool(merged.get("PROVIDER_GATEWAY_ENABLED", True))
PROVIDER_GATEWAY_CACHE_ENABLED: bool = bool(merged.get("PROVIDER_GATEWAY_CACHE_ENABLED", True))
PROVIDER_GATEWAY_MAX_INFLIGHT: int = int(max(1, int(merged.get("PROVIDER_GATEWAY_MAX_INFLIGHT", 16) or 16)))
PROVIDER_GATEWAY_DISCOGS_RPM: int = int(max(1, int(merged.get("PROVIDER_GATEWAY_DISCOGS_RPM", 40) or 40)))
PROVIDER_GATEWAY_LASTFM_RPM: int = int(max(1, int(merged.get("PROVIDER_GATEWAY_LASTFM_RPM", 120) or 120)))
PROVIDER_GATEWAY_ITUNES_RPM: int = int(max(1, int(merged.get("PROVIDER_GATEWAY_ITUNES_RPM", 180) or 180)))
PROVIDER_GATEWAY_DEEZER_RPM: int = int(max(1, int(merged.get("PROVIDER_GATEWAY_DEEZER_RPM", 120) or 120)))
PROVIDER_GATEWAY_SPOTIFY_RPM: int = int(max(1, int(merged.get("PROVIDER_GATEWAY_SPOTIFY_RPM", 60) or 60)))
PROVIDER_GATEWAY_QOBUZ_RPM: int = int(max(1, int(merged.get("PROVIDER_GATEWAY_QOBUZ_RPM", 40) or 40)))
PROVIDER_GATEWAY_TIDAL_RPM: int = int(max(1, int(merged.get("PROVIDER_GATEWAY_TIDAL_RPM", 20) or 20)))
PROVIDER_GATEWAY_AUDIODB_RPM: int = int(max(1, int(merged.get("PROVIDER_GATEWAY_AUDIODB_RPM", 60) or 60)))
PROVIDER_GATEWAY_BANDCAMP_RPM: int = int(max(1, int(merged.get("PROVIDER_GATEWAY_BANDCAMP_RPM", 12) or 12)))
AUTO_TUNE_ENABLED: bool = bool(merged.get("AUTO_TUNE_ENABLED", True))
AUTO_TUNE_INTERVAL_SEC: int = int(max(15, int(merged.get("AUTO_TUNE_INTERVAL_SEC", 60) or 60)))
AUTO_TUNE_MB_MIRROR_MIN_RPS: float = float(max(1.0, float(merged.get("AUTO_TUNE_MB_MIRROR_MIN_RPS", 12.0) or 12.0)))
AUTO_TUNE_MB_MIRROR_MAX_RPS: float = float(max(1.0, float(merged.get("AUTO_TUNE_MB_MIRROR_MAX_RPS", 20.0) or 20.0)))
AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN: int = int(max(1, int(merged.get("AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN", 8) or 8)))
AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP: int = int(max(1, int(merged.get("AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP", 32) or 32)))
# Global flags controlling cache usage during scans
SCAN_DISABLE_CACHE: bool = bool(merged.get("SCAN_DISABLE_CACHE", False))
MB_DISABLE_CACHE: bool = bool(merged.get("MB_DISABLE_CACHE", False) or SCAN_DISABLE_CACHE)

# Configure MusicBrainz client (user-agent + optional mirror target).
def _musicbrainz_target_settings(*, probe_health: bool = True) -> dict[str, Any]:
    return _musicbrainz_client_runtime.musicbrainz_target_settings_for_runtime(
        sys.modules[__name__],
        probe_health=probe_health,
    )


def _configure_musicbrainz_client():
    """Configure MusicBrainz User-Agent and optional mirror target."""
    return _musicbrainz_client_runtime.configure_musicbrainz_client_for_runtime(sys.modules[__name__])

# Configure User-Agent now that config is loaded
_configure_musicbrainz_client()


_ProviderGatewayResponse = _provider_gateway_runtime._ProviderGatewayResponse
_PROVIDER_GATEWAY_LOCK = _provider_gateway_runtime._PROVIDER_GATEWAY_LOCK
_PROVIDER_GATEWAY_CACHE = _provider_gateway_runtime._PROVIDER_GATEWAY_CACHE
_PROVIDER_GATEWAY_ERROR_CACHE = _provider_gateway_runtime._PROVIDER_GATEWAY_ERROR_CACHE
_PROVIDER_GATEWAY_INFLIGHT_REQUESTS = _provider_gateway_runtime._PROVIDER_GATEWAY_INFLIGHT_REQUESTS
_PROVIDER_GATEWAY_BUCKETS = _provider_gateway_runtime._PROVIDER_GATEWAY_BUCKETS
_PROVIDER_GATEWAY_STATS = _provider_gateway_runtime._PROVIDER_GATEWAY_STATS


def _provider_gateway_runtime_settings() -> dict[str, Any]:
    return _provider_gateway_runtime.provider_gateway_runtime_settings_for_runtime(sys.modules[__name__])


def _provider_gateway_reconfigure() -> None:
    return _provider_gateway_runtime.provider_gateway_reconfigure_for_runtime(sys.modules[__name__])


def _provider_gateway_record_lookup_request(provider: str) -> None:
    return _provider_gateway_runtime.provider_gateway_record_lookup_request_for_runtime(sys.modules[__name__], provider)


def _provider_gateway_record_lookup_network_request(provider: str) -> None:
    return _provider_gateway_runtime.provider_gateway_record_lookup_network_request_for_runtime(sys.modules[__name__], provider)


def _provider_gateway_record_result(provider: str, **kwargs: Any) -> None:
    return _provider_gateway_runtime.provider_gateway_record_result_for_runtime(sys.modules[__name__], provider, **kwargs)


def _provider_gateway_record_lookup_cache_hit(provider: str, status: str) -> None:
    return _provider_gateway_runtime.provider_gateway_record_lookup_cache_hit_for_runtime(sys.modules[__name__], provider, status)


def _provider_gateway_record_lookup_coalesced_wait(provider: str, *, context: str = "") -> None:
    return _provider_gateway_runtime.provider_gateway_record_lookup_coalesced_wait_for_runtime(sys.modules[__name__], provider, context=context)


def _provider_gateway_http_get(*args: Any, **kwargs: Any) -> _ProviderGatewayResponse:
    return _provider_gateway_runtime.provider_gateway_http_get_for_runtime(sys.modules[__name__], *args, **kwargs)


def _provider_gateway_stats_snapshot() -> dict[str, Any]:
    return _provider_gateway_runtime.provider_gateway_stats_snapshot_for_runtime(sys.modules[__name__])


def _lock_try_acquire_nonblocking(lock_obj: Any) -> bool:
    return _provider_gateway_runtime._lock_try_acquire_nonblocking(lock_obj)


def _provider_gateway_stats_snapshot_best_effort(
    cached_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _provider_gateway_runtime.provider_gateway_stats_snapshot_best_effort_for_runtime(
        sys.modules[__name__],
        cached_snapshot,
    )


_provider_gateway_reconfigure()
LIDARR_URL: str = merged.get("LIDARR_URL", "")
LIDARR_API_KEY: str = merged.get("LIDARR_API_KEY", "")
AUTOBRR_URL: str = merged.get("AUTOBRR_URL", "")
AUTOBRR_API_KEY: str = merged.get("AUTOBRR_API_KEY", "")
AUTO_FIX_BROKEN_ALBUMS: bool = bool(merged.get("AUTO_FIX_BROKEN_ALBUMS", False))


def _lidarr_feature_enabled() -> bool:
    from pmda_core.legacy_integrations import lidarr_feature_enabled

    return lidarr_feature_enabled()


def _autobrr_feature_enabled() -> bool:
    from pmda_core.legacy_integrations import autobrr_feature_enabled

    return autobrr_feature_enabled()


BROKEN_ALBUM_CONSECUTIVE_THRESHOLD: int = int(merged.get("BROKEN_ALBUM_CONSECUTIVE_THRESHOLD", 2))
BROKEN_ALBUM_PERCENTAGE_THRESHOLD: float = float(merged.get("BROKEN_ALBUM_PERCENTAGE_THRESHOLD", 0.20))
# Default required tags now focus on artist/album + genre/year + tracks.
REQUIRED_TAGS: list[str] = merged.get("REQUIRED_TAGS", ["artist", "album", "genre", "year", "tracks"])
AUTO_MOVE_DUPES: bool = bool(merged["AUTO_MOVE_DUPES"])
AUTO_EXPORT_LIBRARY: bool = bool(merged.get("AUTO_EXPORT_LIBRARY", False))
METADATA_QUEUE_ENABLED: bool = bool(merged.get("METADATA_QUEUE_ENABLED", False))
METADATA_WORKER_MODE: str = str(merged.get("METADATA_WORKER_MODE", "local") or "local").strip().lower()
if METADATA_WORKER_MODE not in {"local", "hybrid"}:
    METADATA_WORKER_MODE = "local"
_METADATA_WORKER_COUNT_RAW = int(max(0, int(merged.get("METADATA_WORKER_COUNT", 0) or 0)))
if _METADATA_WORKER_COUNT_RAW == 4:
    _METADATA_WORKER_COUNT_RAW = 0
_METADATA_JOB_BATCH_SIZE_RAW = int(max(0, int(merged.get("METADATA_JOB_BATCH_SIZE", 0) or 0)))
if _METADATA_JOB_BATCH_SIZE_RAW == 25:
    _METADATA_JOB_BATCH_SIZE_RAW = 0
METADATA_WORKER_COUNT: int = _METADATA_WORKER_COUNT_RAW
METADATA_JOB_BATCH_SIZE: int = _METADATA_JOB_BATCH_SIZE_RAW
PIPELINE_ENABLE_MATCH_FIX: bool = bool(merged.get("PIPELINE_ENABLE_MATCH_FIX", True))
PIPELINE_ENABLE_DEDUPE: bool = bool(merged.get("PIPELINE_ENABLE_DEDUPE", True))
PIPELINE_ENABLE_INCOMPLETE_MOVE: bool = bool(merged.get("PIPELINE_ENABLE_INCOMPLETE_MOVE", False))
PIPELINE_ENABLE_EXPORT: bool = bool(merged.get("PIPELINE_ENABLE_EXPORT", False))
PIPELINE_ENABLE_PLAYER_SYNC: bool = bool(merged.get("PIPELINE_ENABLE_PLAYER_SYNC", False))


def _metadata_worker_auto_count() -> int:
    return max(2, min(16, int(os.cpu_count() or 4)))


def _metadata_worker_effective_count(raw_value: Any = None) -> int:
    try:
        raw = int(METADATA_WORKER_COUNT if raw_value is None else raw_value)
    except Exception:
        raw = 0
    if raw > 0:
        return max(1, min(128, raw))
    return _metadata_worker_auto_count()


def _metadata_job_batch_effective_size(raw_value: Any = None, *, worker_count: Any = None) -> int:
    try:
        raw = int(METADATA_JOB_BATCH_SIZE if raw_value is None else raw_value)
    except Exception:
        raw = 0
    if raw > 0:
        return max(1, min(500, raw))
    effective_workers = _metadata_worker_effective_count(worker_count)
    return max(25, min(64, max(25, effective_workers * 6)))


def _metadata_worker_ui_value(raw_value: Any) -> int:
    try:
        raw = int(raw_value or 0)
    except Exception:
        raw = 0
    return 0 if raw in {0, 4} else max(0, min(128, raw))


def _metadata_job_batch_ui_value(raw_value: Any) -> int:
    try:
        raw = int(raw_value or 0)
    except Exception:
        raw = 0
    return 0 if raw in {0, 25} else max(0, min(500, raw))
PIPELINE_PLAYER_TARGET: str = str(merged.get("PIPELINE_PLAYER_TARGET", "none") or "none").strip().lower()
PIPELINE_POST_SCAN_ASYNC: bool = bool(merged.get("PIPELINE_POST_SCAN_ASYNC", True))
if PIPELINE_PLAYER_TARGET not in {"none", "plex", "jellyfin", "navidrome"}:
    PIPELINE_PLAYER_TARGET = "none"
merged["PIPELINE_PLAYER_TARGET"] = PIPELINE_PLAYER_TARGET

TASK_NOTIFICATIONS_ENABLED: bool = bool(merged.get("TASK_NOTIFICATIONS_ENABLED", True))
TASK_NOTIFICATIONS_SUCCESS: bool = bool(merged.get("TASK_NOTIFICATIONS_SUCCESS", True))
TASK_NOTIFICATIONS_FAILURE: bool = bool(merged.get("TASK_NOTIFICATIONS_FAILURE", True))
TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN: bool = bool(merged.get("TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN", False))
TASK_NOTIFICATIONS_COOLDOWN_SEC: int = int(merged.get("TASK_NOTIFICATIONS_COOLDOWN_SEC", 20) or 20)
TASK_NOTIFY_SCAN_CHANGED: bool = bool(merged.get("TASK_NOTIFY_SCAN_CHANGED", True))
TASK_NOTIFY_SCAN_FULL: bool = bool(merged.get("TASK_NOTIFY_SCAN_FULL", True))
TASK_NOTIFY_ENRICH_BATCH: bool = bool(merged.get("TASK_NOTIFY_ENRICH_BATCH", True))
TASK_NOTIFY_DEDUPE: bool = bool(merged.get("TASK_NOTIFY_DEDUPE", True))
TASK_NOTIFY_INCOMPLETE_MOVE: bool = bool(merged.get("TASK_NOTIFY_INCOMPLETE_MOVE", True))
TASK_NOTIFY_EXPORT: bool = bool(merged.get("TASK_NOTIFY_EXPORT", True))
TASK_NOTIFY_PLAYER_SYNC: bool = bool(merged.get("TASK_NOTIFY_PLAYER_SYNC", True))

JELLYFIN_URL: str = str(merged.get("JELLYFIN_URL", "") or "").strip()
JELLYFIN_API_KEY: str = str(merged.get("JELLYFIN_API_KEY", "") or "").strip()
NAVIDROME_URL: str = str(merged.get("NAVIDROME_URL", "") or "").strip()
NAVIDROME_USERNAME: str = str(merged.get("NAVIDROME_USERNAME", "") or "").strip()
NAVIDROME_PASSWORD: str = str(merged.get("NAVIDROME_PASSWORD", "") or "").strip()
NAVIDROME_API_KEY: str = str(merged.get("NAVIDROME_API_KEY", "") or "").strip()
AI_USAGE_LEVEL: str = str(merged.get("AI_USAGE_LEVEL", "auto") or "auto").strip().lower()
SCAN_AI_POLICY: str = str(merged.get("SCAN_AI_POLICY", "local_only") or "local_only").strip().lower()
SCAN_PAID_PROVIDER_ORDER: str = str(
    merged.get("SCAN_PAID_PROVIDER_ORDER", "openai-api,openai-codex,anthropic,google")
    or "openai-api,openai-codex,anthropic,google"
).strip()
WEB_SEARCH_LOCAL_ORDER: str = str(merged.get("WEB_SEARCH_LOCAL_ORDER", "serper") or "serper").strip()
USE_AI_FOR_MB_MATCH: bool = bool(merged.get("USE_AI_FOR_MB_MATCH", True))
USE_AI_FOR_MB_VERIFY: bool = bool(merged.get("USE_AI_FOR_MB_VERIFY", True))
USE_AI_FOR_DEDUPE: bool = bool(merged.get("USE_AI_FOR_DEDUPE", True))
USE_AI_FOR_SOFT_MATCH_PROFILES: bool = bool(merged.get("USE_AI_FOR_SOFT_MATCH_PROFILES", False))
USE_AI_VISION_FOR_COVER: bool = bool(merged.get("USE_AI_VISION_FOR_COVER", True))
AI_CONFIDENCE_MIN: int = int(merged.get("AI_CONFIDENCE_MIN", 50))
OPENAI_VISION_MODEL: str = str(merged.get("OPENAI_VISION_MODEL", "") or "").strip()
USE_AI_VISION_BEFORE_COVER_INJECT: bool = bool(merged.get("USE_AI_VISION_BEFORE_COVER_INJECT", True))
# MusicBrainz AI verify prompt caps (input-token cost control).
AI_MB_VERIFY_MAX_CANDIDATES: int = int(merged.get("AI_MB_VERIFY_MAX_CANDIDATES", 8) or 8)
AI_MB_VERIFY_LOCAL_TRACK_PREVIEW: int = int(merged.get("AI_MB_VERIFY_LOCAL_TRACK_PREVIEW", 12) or 12)
AI_MB_VERIFY_MB_TRACK_PREVIEW: int = int(merged.get("AI_MB_VERIFY_MB_TRACK_PREVIEW", 5) or 5)
BACKUP_BEFORE_FIX: bool = bool(merged.get("BACKUP_BEFORE_FIX", False))
MAGIC_MODE: bool = bool(merged.get("MAGIC_MODE", False))
USE_ACOUSTID: bool = bool(merged.get("USE_ACOUSTID", True))
ACOUSTID_API_KEY: str = str(merged.get("ACOUSTID_API_KEY", "") or "").strip()
USE_ACOUSTID_WHEN_TAGGED: bool = bool(merged.get("USE_ACOUSTID_WHEN_TAGGED", False))
USE_WEB_SEARCH_FOR_MB: bool = bool(merged.get("USE_WEB_SEARCH_FOR_MB", True))
WEB_SEARCH_PROVIDER: str = str(merged.get("WEB_SEARCH_PROVIDER", "auto") or "auto").strip().lower()
USE_AI_WEB_SEARCH_FALLBACK: bool = bool(merged.get("USE_AI_WEB_SEARCH_FALLBACK", False))
OPENAI_ENABLE_API_KEY_MODE: bool = bool(merged.get("OPENAI_ENABLE_API_KEY_MODE", True))
OPENAI_ENABLE_CODEX_OAUTH_MODE: bool = bool(merged.get("OPENAI_ENABLE_CODEX_OAUTH_MODE", True))
SCHEDULER_ALLOW_NON_SCAN_JOBS: bool = bool(merged.get("SCHEDULER_ALLOW_NON_SCAN_JOBS", SCHEDULER_ALLOW_NON_SCAN_JOBS))
AI_MAX_CALLS_PER_SCAN: int = 0
AI_CALL_COOLDOWN_SEC: float = 0.0
AI_GLOBAL_MAX_CALLS_PER_MINUTE: int = 0
AI_GLOBAL_MAX_CALLS_PER_DAY: int = 0
SERPER_API_KEY: str = str(merged.get("SERPER_API_KEY", "") or "").strip()
AI_BATCH_SIZE: int = int(merged.get("AI_BATCH_SIZE", 10))
FFPROBE_POOL_SIZE: int = int(merged.get("FFPROBE_POOL_SIZE", 8))
REPROCESS_INCOMPLETE_ALBUMS: bool = bool(merged.get("REPROCESS_INCOMPLETE_ALBUMS", True))
IMPROVE_ALL_WORKERS: int = int(merged.get("IMPROVE_ALL_WORKERS", 1))
# Cross-library dedupe configuration (from SQLite only)
CROSS_LIBRARY_DEDUPE = merged["CROSS_LIBRARY_DEDUPE"]

# Number of sample tracks per Plex mount to verify (from SQLite only)
CROSSCHECK_SAMPLES = merged["CROSSCHECK_SAMPLES"]

# Skip PATH cross-check at startup when set (from SQLite only)
DISABLE_PATH_CROSSCHECK = merged["DISABLE_PATH_CROSSCHECK"]

# Metadata fallback providers (Improve Album)
USE_DISCOGS: bool = bool(merged.get("USE_DISCOGS", True))
DISCOGS_USER_TOKEN: str = str(merged.get("DISCOGS_USER_TOKEN", "") or "")
USE_ITUNES: bool = bool(merged.get("USE_ITUNES", True))
USE_DEEZER: bool = bool(merged.get("USE_DEEZER", True))
USE_SPOTIFY: bool = bool(merged.get("USE_SPOTIFY", True))
USE_QOBUZ: bool = bool(merged.get("USE_QOBUZ", True))
USE_TIDAL: bool = bool(merged.get("USE_TIDAL", True))
USE_LASTFM: bool = bool(merged.get("USE_LASTFM", True))
LASTFM_API_KEY: str = str(merged.get("LASTFM_API_KEY", "") or "")
LASTFM_API_SECRET: str = str(merged.get("LASTFM_API_SECRET", "") or "")
FANART_API_KEY: str = str(merged.get("FANART_API_KEY", "") or "")
THEAUDIODB_API_KEY: str = str(merged.get("THEAUDIODB_API_KEY", "") or "")
USE_BANDCAMP: bool = bool(merged.get("USE_BANDCAMP", True))
ARTIST_CREDIT_MODE: str = str(merged.get("ARTIST_CREDIT_MODE", "picard_like_default") or "picard_like_default").strip().lower()
CLASSICAL_NAME_PREFERENCE: str = str(merged.get("CLASSICAL_NAME_PREFERENCE", "original") or "original").strip().lower()
if CLASSICAL_NAME_PREFERENCE not in {"original", "english"}:
    CLASSICAL_NAME_PREFERENCE = "original"
LIVE_DEDUPE_MODE: str = str(merged.get("LIVE_DEDUPE_MODE", "safe") or "safe").strip().lower()


def _normalize_ai_usage_level(value: str | None) -> str:
    return _config_core.normalize_ai_usage_level(value)


def _normalize_web_search_provider(value: str | None) -> str:
    return _config_core.normalize_web_search_provider(value)


def _normalize_scan_ai_policy(value: str | None) -> str:
    return _config_core.normalize_scan_ai_policy(value)


def _normalize_classical_name_preference(value: str | None) -> str:
    return _config_core.normalize_classical_name_preference(value)


def _normalize_ordered_values(
    value: Any,
    *,
    allowed: tuple[str, ...],
    default: tuple[str, ...],
) -> list[str]:
    return _config_core.normalize_ordered_values(value, allowed=allowed, default=default)


def _ai_usage_level_overrides(level: str) -> dict[str, bool]:
    return _ai_usage_runtime._ai_usage_level_overrides_for_runtime(sys.modules[__name__], level)


def _apply_ai_usage_level(level: str | None = None) -> str:
    return _ai_usage_runtime._apply_ai_usage_level_for_runtime(sys.modules[__name__], level)


def _apply_forced_runtime_defaults():
    """
    Keep UX simple without silently re-enabling costly features.

    We still enforce Files mode in this build, but we do NOT force-enable AI/Vision/Web-search
    toggles: those are controlled by settings.db + runtime config, and the pipeline itself
    is responsible for only invoking AI when it is truly ambiguous.
    """
    global LIBRARY_MODE
    LIBRARY_MODE = "files"
    merged["LIBRARY_MODE"] = "files"
    # MusicBrainz tuning is intentionally *not* forced here: the user may set
    # MB_SEARCH_ALBUM_TIMEOUT_SEC / MB_CANDIDATE_FETCH_LIMIT / MB_TRACKLIST_FETCH_LIMIT
    # from Settings (SQLite) and scans should respect those values.


_apply_forced_runtime_defaults()
SCAN_AI_POLICY = _normalize_scan_ai_policy(SCAN_AI_POLICY)
SCAN_PAID_PROVIDER_ORDER = ",".join(
    _normalize_ordered_values(
        SCAN_PAID_PROVIDER_ORDER,
        allowed=("openai-api", "openai-codex", "anthropic", "google"),
        default=("openai-api", "openai-codex", "anthropic", "google"),
    )
)
WEB_SEARCH_LOCAL_ORDER = ",".join(
    _normalize_ordered_values(
        WEB_SEARCH_LOCAL_ORDER,
        allowed=("serper",),
        default=("serper",),
    )
)
merged["SCAN_AI_POLICY"] = SCAN_AI_POLICY
merged["SCAN_PAID_PROVIDER_ORDER"] = SCAN_PAID_PROVIDER_ORDER
merged["WEB_SEARCH_LOCAL_ORDER"] = WEB_SEARCH_LOCAL_ORDER
WEB_SEARCH_PROVIDER = _normalize_web_search_provider(WEB_SEARCH_PROVIDER)
merged["WEB_SEARCH_PROVIDER"] = WEB_SEARCH_PROVIDER
_apply_ai_usage_level(AI_USAGE_LEVEL)
SKIP_MB_FOR_LIVE_ALBUMS: bool = bool(merged.get("SKIP_MB_FOR_LIVE_ALBUMS", True))
TRACKLIST_MATCH_MIN: float = float(merged.get("TRACKLIST_MATCH_MIN", 0.8))
LIVE_ALBUMS_MB_STRICT: bool = bool(merged.get("LIVE_ALBUMS_MB_STRICT", False))

# ─────────────────────────────── Fixed container constants ───────────────────────────────
# Legacy Plex constants are retained only for old settings migrations; runtime access is disabled.
PLEX_DB_FILE = str(Path(merged["PLEX_DB_PATH"]) / "com.plexapp.plugins.library.db")
PLEX_DB_EXISTS = Path(PLEX_DB_FILE).exists()
PLEX_CONFIGURED = False
logging.info("Starting in files mode - Plex source DB integration is disabled in this PMDA build.")
# Duplicates root defaults to /dupes but can be overridden from settings.
DUPE_ROOT = Path(str(merged.get("DUPE_ROOT", "/dupes") or "").strip() or "/dupes")
# WebUI defaults to 5005, but beta/parallel containers may override it.
WEBUI_PORT = int(str(os.getenv("PMDA_WEBUI_PORT", "5005") or "5005").strip() or "5005")


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
    Returns dict with config_rw, music_rw, dupes_rw (all bool).
    """
    config_dir = getattr(sys.modules[__name__], "CONFIG_DIR", Path("/config"))
    path_map = getattr(sys.modules[__name__], "PATH_MAP", {})
    dupe_root = getattr(sys.modules[__name__], "DUPE_ROOT", Path("/dupes"))

    config_rw = config_dir.exists() and os.access(config_dir, os.R_OK) and os.access(config_dir, os.W_OK)

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

    return {"config_rw": config_rw, "music_rw": music_rw, "dupes_rw": dupes_rw}

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
OPENAI_ENABLE_API_KEY_MODE = bool(merged.get("OPENAI_ENABLE_API_KEY_MODE", True))
OPENAI_ENABLE_CODEX_OAUTH_MODE = bool(merged.get("OPENAI_ENABLE_CODEX_OAUTH_MODE", True))
OPENAI_MODEL   = merged["OPENAI_MODEL"]
ANTHROPIC_API_KEY = merged["ANTHROPIC_API_KEY"]
GOOGLE_API_KEY = merged["GOOGLE_API_KEY"]
OLLAMA_URL     = merged["OLLAMA_URL"]
OLLAMA_MODEL   = merged["OLLAMA_MODEL"]
OLLAMA_COMPLEX_MODEL = merged["OLLAMA_COMPLEX_MODEL"]
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
    file_handler.setFormatter(
        PlainLogFormatter(
            "%(asctime)s │ %(levelname)s │ %(threadName)s │ %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    root_logger.addHandler(file_handler)
except Exception as e:
    print(f"⚠️  File logging disabled – {e}", file=sys.stderr)


log_header("configuration")


def _is_sensitive_config_key(key: str) -> bool:
    """Return True for config keys that must never be logged in clear text."""
    k = (key or "").upper()
    sensitive_markers = ("TOKEN", "API_KEY", "SECRET", "PASSWORD", "WEBHOOK")
    return any(marker in k for marker in sensitive_markers)


def _mask_secret_value(value) -> str:
    """Mask secret values while keeping a short prefix for troubleshooting."""
    txt = str(value or "")
    if not txt:
        return ""
    if len(txt) <= 4:
        return "****"
    return txt[:4] + "…"


# Mask & dump effective config ------------------------------------------------
_DISABLED_LEGACY_CONFIG_LOG_KEYS = {
    "PLEX_DB_PATH",
    "PLEX_HOST",
    "PLEX_TOKEN",
    "PIPELINE_PLAYER_TARGET",
    "LIDARR_URL",
    "LIDARR_API_KEY",
    "AUTOBRR_URL",
    "AUTOBRR_API_KEY",
}
for k, src in ENV_SOURCES.items():
    if k in _DISABLED_LEGACY_CONFIG_LOG_KEYS:
        continue
    val = merged.get(k)
    if _is_sensitive_config_key(k) and val:
        val = _mask_secret_value(val)
    logging.info("Config %-15s = %-30s (source: %s)", k, val, src)

logging.info("Config CROSS_LIBRARY_DEDUPE = %s (source: %s)", CROSS_LIBRARY_DEDUPE, ENV_SOURCES.get("CROSS_LIBRARY_DEDUPE", "default"))
logging.info("Config PIPELINE_PLAYER_TARGET = %s", PIPELINE_PLAYER_TARGET)
logging.info("Config LIDARR_FEATURE_ENABLED = %s (removed acquisition workflow)", _lidarr_feature_enabled())
logging.info("Config AUTOBRR_FEATURE_ENABLED = %s (removed acquisition workflow)", _autobrr_feature_enabled())
if CROSS_LIBRARY_DEDUPE:
    logging.info("➡️  Duplicate detection mode: cross-library (editions compared across ALL libraries)")
else:
    logging.info("➡️  Duplicate detection mode: per-library only (no cross-library comparisons)")

if _level_num == logging.DEBUG:
    scrubbed = {k: ("***" if _is_sensitive_config_key(k) else v)
                for k, v in merged.items()}
    logging.debug("Full merged config:\n%s", json.dumps(scrubbed, indent=2))

# (9) Initialise AI clients based on provider ----------------------------------------

openai_client = None
anthropic_client = None
google_client = None
google_client_configured = False
ollama_url = None
ai_provider_ready = False

if AI_PROVIDER.lower() in {"openai", "openai-api", "openai-codex"}:
    if bool(OPENAI_ENABLE_API_KEY_MODE) and OPENAI_API_KEY:
        os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
        try:
            openai_client = OpenAI(timeout=_openai_request_timeout_seconds())
            ai_provider_ready = True
            logging.info("OpenAI client initialized")
        except Exception as e:
            logging.warning("OpenAI client init failed: %s", e)
    else:
        if bool(OPENAI_ENABLE_CODEX_OAUTH_MODE):
            logging.info(
                "OpenAI API key is configured, but API-key mode is disabled; OAuth mode may still be usable."
                if OPENAI_API_KEY
                else "No OPENAI_API_KEY provided; API-key mode unavailable (OAuth mode may still be usable)."
            )
        else:
            logging.info(
                "OpenAI API key is configured, but API-key mode is disabled and OAuth mode is off; AI-driven selection disabled."
                if OPENAI_API_KEY
                else "No OPENAI_API_KEY provided; AI-driven selection disabled."
            )
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
            google_client = genai.Client(api_key=GOOGLE_API_KEY)
            google_client_configured = True
            ai_provider_ready = True
            logging.info("Google Gemini client configured")
        except Exception as e:
            logging.warning("Google client configuration failed: %s", e)
    else:
        if not genai:
            logging.warning("Google GenAI SDK not installed. Install with: pip install google-genai")
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
    logging.warning("Unknown AI_PROVIDER: %s. Supported: openai, openai-api, openai-codex, anthropic, google, ollama", AI_PROVIDER)


def _reinit_ai_from_globals(*args: Any, **kwargs: Any) -> Any:
    return _ai_provider_config_runtime._reinit_ai_from_globals_for_runtime(sys.modules[__name__], *args, **kwargs)

def _reload_ai_config_and_reinit(*args: Any, **kwargs: Any) -> Any:
    return _ai_provider_config_runtime._reload_ai_config_and_reinit_for_runtime(sys.modules[__name__], *args, **kwargs)

def _wait_for_codex_runtime_ready_for_scan(*args: Any, **kwargs: Any) -> Any:
    return _ai_provider_config_runtime._wait_for_codex_runtime_ready_for_scan_for_runtime(sys.modules[__name__], *args, **kwargs)

def _probe_model(*args: Any, **kwargs: Any) -> Any:
    return _ai_provider_config_runtime._probe_model_for_runtime(sys.modules[__name__], *args, **kwargs)

def _openai_chat_text(*args: Any, **kwargs: Any) -> Any:
    return _ai_provider_config_runtime._openai_chat_text_for_runtime(sys.modules[__name__], *args, **kwargs)




_OPENAI_PROVIDER_IDS = {"openai", "openai-api", "openai-codex"}
_PROVIDER_PREF_DEFAULTS = {
    "interactive_provider_id": "openai-codex",
    "batch_provider_id": "openai-codex",
    "web_search_provider_id": "openai-codex",
}
_auth_crypto_lock = threading.RLock()
_auth_fernet_cached: Any | None = None
_auth_seed_cached: str | None = None
_openai_auth_service_cached: OpenAIAuthService | None = None
_openai_codex_health_lock = threading.Lock()
_openai_codex_health_cache: dict[int, tuple[float, bool, str]] = {}
_OPENAI_CODEX_HEALTH_TTL_OK_SEC = 300
_OPENAI_CODEX_HEALTH_TTL_ERR_SEC = 30
_codex_cli_path_lock = threading.Lock()
_codex_cli_path_cached: str | None = None


















def _normalize_provider_id(provider_id: str, *, fallback: str = "openai-api") -> str:
    return _ai_provider_config_runtime._normalize_provider_id_for_runtime(
        sys.modules[__name__],
        provider_id,
        fallback=fallback,
    )


def _provider_auth_mode(provider_id: str) -> str:
    return _ai_provider_config_runtime._provider_auth_mode_for_runtime(sys.modules[__name__], provider_id)


def _openai_api_key_mode_enabled() -> bool:
    return _ai_provider_config_runtime._openai_api_key_mode_enabled_for_runtime(sys.modules[__name__])


def _openai_codex_oauth_mode_enabled() -> bool:
    return _ai_provider_config_runtime._openai_codex_oauth_mode_enabled_for_runtime(sys.modules[__name__])


def _provider_mode_enabled(provider_id: str) -> bool:
    return _ai_provider_config_runtime._provider_mode_enabled_for_runtime(sys.modules[__name__], provider_id)


def _provider_mode_disabled_reason(provider_id: str) -> str:
    return _ai_provider_config_runtime._provider_mode_disabled_reason_for_runtime(sys.modules[__name__], provider_id)


def _openai_api_runtime_available() -> bool:
    return _ai_provider_config_runtime._openai_api_runtime_available_for_runtime(sys.modules[__name__])


def _openai_codex_runtime_available(user_id: int | None = None, *, require_token: bool = True) -> bool:
    return _ai_provider_config_runtime._openai_codex_runtime_available_for_runtime(
        sys.modules[__name__],
        user_id,
        require_token=require_token,
    )


def _openai_error_allows_codex_fallback(exc: Any) -> bool:
    return _ai_provider_config_runtime._openai_error_allows_codex_fallback_for_runtime(sys.modules[__name__], exc)


def _ai_context_from_analysis_type(analysis_type: str) -> str:
    return _ai_provider_config_runtime._ai_context_from_analysis_type_for_runtime(sys.modules[__name__], analysis_type)


def _ollama_service_configured() -> bool:
    return bool(str(getattr(sys.modules[__name__], "OLLAMA_URL", "") or "").strip())


def _ollama_model_configured() -> str:
    return str(getattr(sys.modules[__name__], "OLLAMA_MODEL", "") or "").strip() or "qwen3:4b"


def _ollama_complex_model_configured() -> str:
    return str(getattr(sys.modules[__name__], "OLLAMA_COMPLEX_MODEL", "") or "").strip() or "qwen3:14b"


_OLLAMA_CLI_PATH_LOCK = threading.Lock()
_OLLAMA_CLI_PATH_CACHE: str | None = None


def _ollama_cli_path(refresh: bool = False) -> str:
    global _OLLAMA_CLI_PATH_CACHE
    with _OLLAMA_CLI_PATH_LOCK:
        if _OLLAMA_CLI_PATH_CACHE is not None and not refresh:
            return _OLLAMA_CLI_PATH_CACHE
        resolved = shutil.which("ollama") or ""
        _OLLAMA_CLI_PATH_CACHE = str(resolved or "")
        return _OLLAMA_CLI_PATH_CACHE


def _ollama_cli_available(refresh: bool = False) -> bool:
    return bool(_ollama_cli_path(refresh=refresh))


_OLLAMA_MODEL_CATALOG_CACHE_TTL_SEC = 60.0
_OLLAMA_MODEL_CATALOG_CACHE_LOCK = threading.Lock()
_OLLAMA_MODEL_CATALOG_CACHE: dict[str, Any] = {
    "url": "",
    "expires_at": 0.0,
    "models": set(),
}
_OLLAMA_KEEPALIVE_DEFAULT = "30m"
_OLLAMA_KEEPALIVE_INTERACTIVE = "45m"
_OLLAMA_KEEPALIVE_COMPLEX = "60m"
_OLLAMA_PREWARM_CACHE_TTL_SEC = 180.0
_OLLAMA_PREWARM_STATE_LOCK = threading.Lock()
_OLLAMA_PREWARM_STATE: dict[tuple[str, str], dict[str, Any]] = {}

_OLLAMA_COMPLEX_ANALYSIS_TYPES = {
    "acoustid_candidate_disambiguation",
    "album_review_batch",
    "album_review_generate",
    "dedupe_choose_best",
    "mb_artist_index_choice",
    "mb_candidate_tiebreak",
    "mb_match_verify",
    "mb_retry_disambiguation",
}

_OLLAMA_AMBIGUOUS_ESCALATION_ANALYSIS_TYPES = {
    "provider_identity_verify",
    "web_mbid_inference",
}

_OLLAMA_COMPLEXITY_HINTS = (
    "ambiguous",
    "arbitration",
    "choose",
    "disambiguation",
    "none matched",
    "provider candidates",
    "same release",
    "tie-break",
)

_PROVIDER_IDENTITY_AI_SKIP_REASON_PREFIXES = (
    "track_count_mismatch",
    "track_title_mismatch",
    "classical_track_count_mismatch",
    "classical_work_mismatch",
    "classical_composer_mismatch",
    "classical_performance_mismatch",
    "classical_catalog_mismatch",
    "classical_disc_count_mismatch",
    "classical_duration_mismatch",
    "classical_label_plus_performance_mismatch",
    "classical_year_mismatch",
)


def _ollama_available_models_cached(force_refresh: bool = False) -> set[str]:
    url = str(getattr(sys.modules[__name__], "OLLAMA_URL", "") or "").strip().rstrip("/")
    if not url:
        return set()
    now = time.time()
    with _OLLAMA_MODEL_CATALOG_CACHE_LOCK:
        cached_url = str(_OLLAMA_MODEL_CATALOG_CACHE.get("url") or "")
        expires_at = float(_OLLAMA_MODEL_CATALOG_CACHE.get("expires_at") or 0.0)
        cached_models = _OLLAMA_MODEL_CATALOG_CACHE.get("models") or set()
        if (
            not force_refresh
            and cached_url == url
            and expires_at > now
            and isinstance(cached_models, set)
        ):
            return set(cached_models)
    models: set[str] = set()
    try:
        response = requests.get(f"{url}/api/tags", timeout=5)
        if response.status_code == 200:
            payload = response.json() if response.content else {}
            for item in payload.get("models") or []:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip().lower()
                if name:
                    models.add(name)
    except Exception:
        models = set()
    with _OLLAMA_MODEL_CATALOG_CACHE_LOCK:
        _OLLAMA_MODEL_CATALOG_CACHE["url"] = url
        _OLLAMA_MODEL_CATALOG_CACHE["expires_at"] = now + _OLLAMA_MODEL_CATALOG_CACHE_TTL_SEC
        _OLLAMA_MODEL_CATALOG_CACHE["models"] = set(models)
    return models


def _ollama_model_available(model_name: str) -> bool:
    target = str(model_name or "").strip().lower()
    if not target:
        return False
    available = _ollama_available_models_cached()
    if not available:
        return False
    return target in available


def _ollama_keep_alive_for_analysis(*, analysis_type: str, model_name: str) -> str:
    analysis = str(analysis_type or "").strip().lower()
    model = str(model_name or "").strip().lower()
    context = _ai_context_from_analysis_type(analysis)
    if context == "interactive":
        return _OLLAMA_KEEPALIVE_INTERACTIVE
    if model and model == _ollama_complex_model_configured().strip().lower():
        return _OLLAMA_KEEPALIVE_COMPLEX
    return _OLLAMA_KEEPALIVE_DEFAULT


def _ollama_prewarm_model(
    model_name: str,
    *,
    analysis_type: str = "",
    force: bool = False,
) -> bool:
    return _ai_provider_config_runtime._ollama_prewarm_model_for_runtime(
        sys.modules[__name__],
        model_name,
        analysis_type=analysis_type,
        force=force,
    )


def _ollama_route_for_analysis(
    *,
    requested_model: str,
    analysis_type: str,
    endpoint_kind: str,
    system_msg: str,
    user_msg: str,
) -> tuple[str, dict[str, Any]]:
    return _ai_provider_config_runtime._ollama_route_for_analysis_for_runtime(
        sys.modules[__name__],
        requested_model=requested_model,
        analysis_type=analysis_type,
        endpoint_kind=endpoint_kind,
        system_msg=system_msg,
        user_msg=user_msg,
    )


def _ai_model_display_name(provider: str | None = None) -> str:
    provider_name = str(provider or getattr(sys.modules[__name__], "AI_PROVIDER", "") or "").strip().lower()
    mod = sys.modules[__name__]
    if provider_name == "ollama":
        return _ollama_model_configured()
    if provider_name == "anthropic":
        return str(getattr(mod, "ANTHROPIC_MODEL", "") or getattr(mod, "RESOLVED_MODEL", "") or "").strip()
    if provider_name in {"google", "gemini"}:
        return str(getattr(mod, "GOOGLE_MODEL", "") or getattr(mod, "RESOLVED_MODEL", "") or "").strip()
    return str(getattr(mod, "RESOLVED_MODEL", None) or getattr(mod, "OPENAI_MODEL", "") or "").strip()


def _scan_ai_policy_for_runtime() -> str:
    return _normalize_scan_ai_policy(getattr(sys.modules[__name__], "SCAN_AI_POLICY", "local_only"))


def _scan_paid_provider_chain() -> list[str]:
    return _normalize_ordered_values(
        getattr(sys.modules[__name__], "SCAN_PAID_PROVIDER_ORDER", ""),
        allowed=("openai-api", "openai-codex", "anthropic", "google"),
        default=("openai-api", "openai-codex", "anthropic", "google"),
    )


def _web_search_local_chain() -> list[str]:
    return _normalize_ordered_values(
        getattr(sys.modules[__name__], "WEB_SEARCH_LOCAL_ORDER", ""),
        allowed=("serper",),
        default=("serper",),
    )


def _provider_ready_for_runtime(provider_id: str, user_id: int | None = None) -> bool:
    pid = _normalize_provider_id(provider_id, fallback="")
    uid = max(0, int(user_id or 0))
    if not pid:
        return False
    if pid == "openai-api":
        return bool(_openai_api_runtime_available())
    if pid == "openai-codex":
        return bool(_openai_codex_runtime_available(uid, require_token=True))
    if pid == "anthropic":
        return bool(anthropic_client)
    if pid == "google":
        return bool(google_client_configured and google_client)
    if pid == "ollama":
        return bool(_ollama_service_configured())
    return False


def _scan_paid_provider_fallback(user_id: int | None = None) -> str:
    if _scan_ai_policy_for_runtime() == "local_only":
        return ""
    chain = _scan_paid_provider_chain()
    uid = max(0, int(user_id or 0))
    for provider_id in chain:
        if _provider_mode_enabled(provider_id) and _provider_ready_for_runtime(provider_id, uid):
            return provider_id
    for provider_id in chain:
        if _provider_mode_enabled(provider_id):
            return provider_id
    return ""


def _local_first_scan_ai_enabled() -> bool:
    level = _normalize_ai_usage_level(getattr(sys.modules[__name__], "AI_USAGE_LEVEL", "auto"))
    return level == "auto" and bool(ollama_url) and _ollama_service_configured()


def _resolve_local_first_provider_for_runtime(context: str, requested_provider: str, *, user_id: int | None = None) -> str:
    req_norm = _normalize_provider_id(str(requested_provider or "").strip().lower(), fallback="")
    if req_norm and req_norm not in _OPENAI_PROVIDER_IDS:
        return ""
    if context != "batch":
        return ""
    policy = _scan_ai_policy_for_runtime()
    uid = max(0, int(user_id or 0))
    if policy == "local_only":
        return "ollama"
    if policy == "local_then_paid":
        if _local_first_scan_ai_enabled():
            return "ollama"
        return _scan_paid_provider_fallback(uid)
    if policy == "paid_only":
        return _scan_paid_provider_fallback(uid)
    return ""


def _resolve_model_for_runtime(
    provider_id: str,
    requested_model: str,
    *,
    endpoint_kind: str = "text",
    analysis_type: str = "",
    system_msg: str = "",
    user_msg: str = "",
) -> str:
    pid = _normalize_provider_id(provider_id, fallback="openai-api")
    requested = str(requested_model or "").strip()
    if pid == "ollama":
        configured = _ollama_model_configured()
        selected, route_meta = _ollama_route_for_analysis(
            requested_model=configured or requested or "qwen3:4b",
            analysis_type=analysis_type,
            endpoint_kind=endpoint_kind,
            system_msg=system_msg,
            user_msg=user_msg,
        )
        if route_meta.get("route") == "hard_case":
            logging.info(
                "[Ollama Router] Escalating %s/%s from %s to %s (%s)",
                str(analysis_type or "batch"),
                endpoint_kind,
                route_meta.get("base_model") or configured or requested or "qwen3:4b",
                selected,
                route_meta.get("reason") or "complex",
            )
        return selected or configured or requested or "qwen3:4b"
    if pid == "openai-codex":
        return requested or "codex"
    if pid == "openai-api":
        return requested or str(getattr(sys.modules[__name__], "OPENAI_MODEL", "") or "gpt-4o-mini")
    return requested


def _openai_codex_health_cache_invalidate(user_id: int | None = None) -> None:
    with _openai_codex_health_lock:
        if user_id is None:
            _openai_codex_health_cache.clear()
        else:
            _openai_codex_health_cache.pop(int(user_id or 0), None)


def _codex_cli_path(refresh: bool = False) -> str:
    global _codex_cli_path_cached
    with _codex_cli_path_lock:
        if _codex_cli_path_cached is not None and not refresh:
            return _codex_cli_path_cached
        candidates: list[str] = []
        try:
            configured = str(_get_config_from_db("OPENAI_CODEX_CLI_BIN", "") or "").strip()
        except Exception:
            configured = ""
        if configured:
            candidates.append(configured)
        for item in ("codex", "/usr/local/bin/codex", "/usr/bin/codex"):
            if item not in candidates:
                candidates.append(item)
        resolved = ""
        for item in candidates:
            probe = shutil.which(item) if os.path.sep not in item else item
            if probe and Path(probe).exists():
                resolved = str(probe)
                break
        _codex_cli_path_cached = resolved
        return resolved


def _codex_cli_available(refresh: bool = False) -> bool:
    return bool(_codex_cli_path(refresh=refresh))


def _openai_codex_profile_present(user_id: int | None = None) -> bool:
    uid = int(user_id or 0)
    try:
        con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
        cur = con.cursor()
        if uid > 0:
            cur.execute(
                """
                SELECT 1
                FROM ai_auth_profiles
                WHERE provider_id = 'openai-codex'
                  AND is_active = 1
                  AND user_id IN (?, 0)
                ORDER BY CASE WHEN user_id = ? THEN 0 ELSE 1 END, updated_at DESC
                LIMIT 1
                """,
                (uid, uid),
            )
        else:
            cur.execute(
                """
                SELECT 1
                FROM ai_auth_profiles
                WHERE provider_id = 'openai-codex'
                  AND is_active = 1
                ORDER BY updated_at DESC
                LIMIT 1
                """
            )
        row = cur.fetchone()
        con.close()
        return bool(row)
    except Exception:
        return False


def _openai_codex_any_profile_present() -> bool:
    try:
        con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
        cur = con.cursor()
        cur.execute(
            """
            SELECT 1
            FROM ai_auth_profiles
            WHERE provider_id = 'openai-codex'
              AND is_active = 1
            LIMIT 1
            """
        )
        row = cur.fetchone()
        con.close()
        return bool(row)
    except Exception:
        return False


def _openai_codex_token_health(user_id: int | None = None, *, force_refresh: bool = False) -> tuple[bool, str]:
    return _codex_exec_runtime.openai_codex_token_health_for_runtime(
        sys.modules[__name__],
        user_id,
        force_refresh=force_refresh,
    )


def _openai_codex_connected(user_id: int | None = None, *, require_token: bool = False) -> bool:
    return _codex_exec_runtime.openai_codex_connected_for_runtime(
        sys.modules[__name__],
        user_id,
        require_token=require_token,
    )


if AI_PROVIDER.lower() == "openai-codex" and bool(OPENAI_ENABLE_CODEX_OAUTH_MODE) and not openai_client:
    try:
        _reinit_ai_from_globals()
    except Exception:
        logging.debug("Deferred OpenAI Codex OAuth re-init failed during startup", exc_info=True)


def _get_ai_provider_preferences(user_id: int | None = None) -> dict[str, str]:
    return _ai_provider_config_runtime._get_ai_provider_preferences_for_runtime(sys.modules[__name__], user_id)


def _save_ai_provider_preferences(
    *,
    user_id: int | None,
    interactive_provider_id: str,
    batch_provider_id: str,
    web_search_provider_id: str,
) -> dict[str, str]:
    return _ai_provider_config_runtime._save_ai_provider_preferences_for_runtime(
        sys.modules[__name__],
        user_id=user_id,
        interactive_provider_id=interactive_provider_id,
        batch_provider_id=batch_provider_id,
        web_search_provider_id=web_search_provider_id,
    )


def _resolve_provider_for_runtime(requested_provider: str, analysis_type: str, *, user_id: int | None = None) -> str:
    return _ai_provider_config_runtime._resolve_provider_for_runtime_for_runtime(
        sys.modules[__name__],
        requested_provider,
        analysis_type,
        user_id=user_id,
    )


def _resolve_ai_runtime_availability(
    *,
    analysis_type: str,
    requested_provider: str = "openai",
    user_id: int | None = None,
) -> tuple[bool, str, str, str]:
    return _ai_provider_config_runtime._resolve_ai_runtime_availability_for_runtime(
        sys.modules[__name__],
        analysis_type=analysis_type,
        requested_provider=requested_provider,
        user_id=user_id,
    )


def _assistant_runtime_status(*args, **kwargs):
    return _assistant_chat_runtime._assistant_runtime_status_for_runtime(sys.modules[__name__], *args, **kwargs)


def _auth_encryption_seed() -> bytes:
    global _auth_seed_cached
    seed = str(os.getenv("PMDA_AUTH_ENCRYPTION_KEY") or "").strip()
    if seed:
        return hashlib.sha256(seed.encode("utf-8", errors="ignore")).digest()
    with _auth_crypto_lock:
        if _auth_seed_cached:
            return hashlib.sha256(_auth_seed_cached.encode("utf-8", errors="ignore")).digest()
        cfg_dir = Path(str(CONFIG_DIR or "/config")).expanduser()
        seed_file = cfg_dir / ".pmda_auth_seed"
        try:
            stored = seed_file.read_text(encoding="utf-8").strip()
            if stored:
                _auth_seed_cached = stored
                return hashlib.sha256(stored.encode("utf-8", errors="ignore")).digest()
        except Exception:
            pass
        generated = ""
        try:
            cfg_dir.mkdir(parents=True, exist_ok=True)
            generated = secrets.token_urlsafe(48)
            tmp = seed_file.with_suffix(".tmp")
            tmp.write_text(generated, encoding="utf-8")
            try:
                os.chmod(tmp, 0o600)
            except Exception:
                pass
            os.replace(tmp, seed_file)
        except Exception:
            # Legacy fallback (old behavior). This is unstable across container recreation,
            # but still better than blocking runtime if /config is not writable.
            generated = f"{socket.gethostname()}:{SETTINGS_DB_FILE}"
        _auth_seed_cached = generated
        return hashlib.sha256(generated.encode("utf-8", errors="ignore")).digest()


def _auth_legacy_seeds_for_decrypt() -> list[bytes]:
    seeds: list[bytes] = []

    def _append(raw: str) -> None:
        text = str(raw or "").strip()
        if not text:
            return
        digest = hashlib.sha256(text.encode("utf-8", errors="ignore")).digest()
        if digest not in seeds:
            seeds.append(digest)

    # Legacy deterministic seed (pre-stable-key implementation).
    _append(f"{socket.gethostname()}:{SETTINGS_DB_FILE}")

    # Optional manual recovery hook for old container-bound seeds.
    # Format: comma-separated seed strings.
    extra = str(os.getenv("PMDA_AUTH_LEGACY_SEEDS") or "").strip()
    if extra:
        for part in extra.split(","):
            _append(part)
    return seeds


def _fernet_with_seed(seed_bytes: bytes) -> Any | None:
    if Fernet is None:
        return None
    try:
        return Fernet(base64.urlsafe_b64encode(seed_bytes))
    except Exception:
        return None


def _auth_fernet_instance() -> Any | None:
    global _auth_fernet_cached
    if Fernet is None:
        return None
    with _auth_crypto_lock:
        if _auth_fernet_cached is None:
            key = base64.urlsafe_b64encode(_auth_encryption_seed())
            _auth_fernet_cached = Fernet(key)
        return _auth_fernet_cached


def _xor_cipher(data: bytes, key_seed: bytes) -> bytes:
    if not data:
        return b""
    out = bytearray(len(data))
    counter = 0
    offset = 0
    while offset < len(data):
        block = hashlib.sha256(key_seed + counter.to_bytes(4, byteorder="big", signed=False)).digest()
        for b in block:
            if offset >= len(data):
                break
            out[offset] = data[offset] ^ b
            offset += 1
        counter += 1
    return bytes(out)


def _auth_encrypt(value: str) -> str:
    raw = str(value or "")
    if not raw:
        return ""
    inst = _auth_fernet_instance()
    if inst is not None:
        try:
            tok = inst.encrypt(raw.encode("utf-8", errors="ignore")).decode("utf-8", errors="ignore")
            return f"enc:v1:{tok}"
        except Exception:
            pass
    payload = _xor_cipher(raw.encode("utf-8", errors="ignore"), _auth_encryption_seed())
    return f"enc:xor:{base64.urlsafe_b64encode(payload).decode('ascii', errors='ignore')}"


def _auth_decrypt(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("enc:v1:"):
        token = raw.split("enc:v1:", 1)[1]
        inst = _auth_fernet_instance()
        if inst is not None:
            try:
                return inst.decrypt(token.encode("utf-8", errors="ignore")).decode("utf-8", errors="ignore")
            except InvalidToken:
                pass
            except Exception:
                pass
        for legacy_seed in _auth_legacy_seeds_for_decrypt():
            legacy_inst = _fernet_with_seed(legacy_seed)
            if legacy_inst is None:
                continue
            try:
                return legacy_inst.decrypt(token.encode("utf-8", errors="ignore")).decode("utf-8", errors="ignore")
            except Exception:
                continue
        return ""
    if raw.startswith("enc:xor:"):
        token = raw.split("enc:xor:", 1)[1]
        try:
            payload = base64.urlsafe_b64decode(token.encode("ascii", errors="ignore"))
        except Exception:
            payload = b""
        if payload:
            seeds = [_auth_encryption_seed(), *_auth_legacy_seeds_for_decrypt()]
            for seed in seeds:
                try:
                    candidate = _xor_cipher(payload, seed).decode("utf-8", errors="ignore")
                except Exception:
                    candidate = ""
                if candidate:
                    return candidate
        try:
            # Final fallback: older plaintext values without prefix.
            return base64.urlsafe_b64decode(token.encode("ascii", errors="ignore")).decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return raw


def _set_legacy_openai_oauth_refresh(refresh_token: str) -> None:
    try:
        token = str(refresh_token or "").strip()
        if not token:
            return
        init_settings_db()
        con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
            ("OPENAI_OAUTH_REFRESH_TOKEN", token),
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to persist OPENAI_OAUTH_REFRESH_TOKEN", exc_info=True)


def _get_legacy_openai_oauth_refresh() -> str:
    try:
        return str(_get_config_from_db("OPENAI_OAUTH_REFRESH_TOKEN", "") or "").strip()
    except Exception:
        return ""


def _apply_openai_api_key_from_oauth(api_key: str) -> None:
    key = str(api_key or "").strip()
    if not key:
        return
    init_settings_db()
    con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=10)
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
        ("OPENAI_CODEX_EXCHANGED_API_KEY", key),
    )
    con.commit()
    con.close()
    setattr(sys.modules[__name__], "OPENAI_CODEX_EXCHANGED_API_KEY", key)


def _clear_openai_codex_exchanged_api_key() -> None:
    try:
        init_settings_db()
        con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.execute("DELETE FROM settings WHERE key = ?", ("OPENAI_CODEX_EXCHANGED_API_KEY",))
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to clear OPENAI_CODEX_EXCHANGED_API_KEY", exc_info=True)
    try:
        setattr(sys.modules[__name__], "OPENAI_CODEX_EXCHANGED_API_KEY", "")
    except Exception:
        pass


def _settings_db_set_value(key: str, value: str) -> None:
    init_settings_db()
    con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=10)
    try:
        con.execute("PRAGMA busy_timeout=5000;")
        cur = con.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
            (str(key or "").strip(), "" if value is None else str(value)),
        )
        con.commit()
    finally:
        con.close()


def _settings_db_delete_keys(*keys: str) -> None:
    cleaned = [str(k or "").strip() for k in keys if str(k or "").strip()]
    if not cleaned:
        return
    init_settings_db()
    con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=10)
    try:
        con.execute("PRAGMA busy_timeout=5000;")
        cur = con.cursor()
        cur.executemany("DELETE FROM settings WHERE key = ?", [(k,) for k in cleaned])
        con.commit()
    finally:
        con.close()


def _settings_db_get_secret(key: str) -> str:
    try:
        return str(_auth_decrypt(_get_config_from_db(key, "") or "") or "").strip()
    except Exception:
        return ""


def _settings_db_set_secret(key: str, value: str) -> None:
    token = str(value or "").strip()
    if not token:
        _settings_db_delete_keys(key)
        return
    _settings_db_set_value(key, _auth_encrypt(token))


_MANAGED_RUNTIME_LOCK = threading.Lock()
_MANAGED_RUNTIME_THREADS: dict[str, threading.Thread] = {}
_MANAGED_RUNTIME_NETWORK_NAME = "pmda-managed-runtime"
_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE = "musicbrainz_local"
_MANAGED_RUNTIME_OLLAMA_BUNDLE = "ollama_local"
_MANAGED_RUNTIME_BUNDLE_TYPES = (
    _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
    _MANAGED_RUNTIME_OLLAMA_BUNDLE,
)
_MANAGED_RUNTIME_READY_STATES = {"ready"}
_MANAGED_RUNTIME_ACTIVE_STATES = {"preflight", "pulling", "creating", "importing", "waiting_health", "updating"}
_MANAGED_RUNTIME_MB_DEFAULT_PROJECT = "musicbrainz-docker"
_MANAGED_RUNTIME_OLLAMA_CONTAINER = "pmda-ollama"
_MANAGED_RUNTIME_MB_DEFAULT_PORT = 5500
_MANAGED_RUNTIME_OLLAMA_PORT = 11434
_MANAGED_RUNTIME_MB_DEFAULT_UPDATE_INTERVAL_SEC = 7 * 24 * 3600
_MANAGED_RUNTIME_GPU_VENDOR_MAP = {
    "0x8086": "intel",
    "0x10de": "nvidia",
    "0x1002": "amd",
    "0x1022": "amd",
}
_MANAGED_RUNTIME_OLLAMA_GPU_MODE_AUTO = "auto"
_MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU = "cpu"
_MANAGED_RUNTIME_OLLAMA_GPU_MODE_NVIDIA = "nvidia"
_MANAGED_RUNTIME_OLLAMA_GPU_MODE_AMD_ROCM = "amd_rocm"
_MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN = "vulkan"
_MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_INTEL = "vulkan_intel"
_MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_AMD = "vulkan_amd"
_MANAGED_RUNTIME_OLLAMA_GPU_ALLOWED_MODES = {
    _MANAGED_RUNTIME_OLLAMA_GPU_MODE_AUTO,
    _MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU,
    _MANAGED_RUNTIME_OLLAMA_GPU_MODE_NVIDIA,
    _MANAGED_RUNTIME_OLLAMA_GPU_MODE_AMD_ROCM,
    _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN,
    _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_INTEL,
    _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_AMD,
}


def _managed_runtime_resolve_musicbrainz_install_root(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_resolve_musicbrainz_install_root_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_container_path_to_host_path(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_container_path_to_host_path_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_container_bind_alias_path(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_container_bind_alias_path_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_json_dumps(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_json_dumps_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_json_loads(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_json_loads_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_bundle_defaults(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_bundle_defaults_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_bundle_get(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_bundle_get_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_bundle_upsert(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_bundle_upsert_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_bundle_upsert_best_effort(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_bundle_upsert_best_effort_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_log(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_log_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_logs(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_logs_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_action_update(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_action_update_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_get_latest_action(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_get_latest_action_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_docker_cli(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_docker_cli_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_compose_cli(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_compose_cli_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_git_cli(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_git_cli_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_self_container_name(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_self_container_name_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_sysfs_gpu_vendor(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_sysfs_gpu_vendor_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_collect_dri_devices(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_collect_dri_devices_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_gpu_probe(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_gpu_probe_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_ollama_gpu_requested_mode(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_ollama_gpu_requested_mode_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_ollama_gpu_profile(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_ollama_gpu_profile_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_preflight(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_preflight_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_docker_ps(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_docker_ps_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_parse_ports(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_parse_ports_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_docker_inspect_container(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_docker_inspect_container_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_project_prefix(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_project_prefix_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_health_check_musicbrainz(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_health_check_musicbrainz_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_health_check_ollama(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_health_check_ollama_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_container_labels(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_container_labels_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_detect_musicbrainz_candidates(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_detect_musicbrainz_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_detect_ollama_candidates(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_detect_ollama_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_ensure_network(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_ensure_network_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_connect_container_to_network(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_connect_container_to_network_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_connect_self_to_network(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_connect_self_to_network_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_try_connect_self_to_existing_network(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_try_connect_self_to_existing_network_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_musicbrainz_install_root(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_musicbrainz_install_root_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_musicbrainz_data_root(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_musicbrainz_data_root_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_ollama_data_root(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_ollama_data_root_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_musicbrainz_internal_url(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_musicbrainz_internal_url_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_short_duration(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_short_duration_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_capture_subprocess(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_capture_subprocess_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_health_wait(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_health_wait_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_register_mb_update_schedule(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_register_mb_update_schedule_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_apply_musicbrainz_runtime(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_apply_musicbrainz_runtime_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_apply_ollama_runtime(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_apply_ollama_runtime_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_adopt_musicbrainz(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_adopt_musicbrainz_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_adopt_ollama(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_adopt_ollama_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_ollama_pull_blocking(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_ollama_pull_blocking_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_ensure_ollama_models(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_ensure_ollama_models_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_bootstrap_musicbrainz(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_bootstrap_musicbrainz_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_bootstrap_ollama(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_bootstrap_ollama_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_bootstrap_worker(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_bootstrap_worker_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_launch_bootstrap(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_launch_bootstrap_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_bundle_status(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_bundle_status_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_status_snapshot(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_status_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_musicbrainz_update_due(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_musicbrainz_update_due_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_run_musicbrainz_update(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_run_musicbrainz_update_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_musicbrainz_repair_worker(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_musicbrainz_repair_worker_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_launch_musicbrainz_search_repair(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_launch_musicbrainz_search_repair_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_maybe_enqueue_due_jobs(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_maybe_enqueue_due_jobs_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_resolve_candidate(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_resolve_candidate_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_mb_compose_cmd(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_mb_compose_cmd_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_start_musicbrainz_bundle(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_start_musicbrainz_bundle_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_stop_musicbrainz_bundle(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_stop_musicbrainz_bundle_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_restart_musicbrainz_bundle(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_restart_musicbrainz_bundle_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_reset_musicbrainz_bundle(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_reset_musicbrainz_bundle_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_start_ollama_bundle(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_start_ollama_bundle_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_stop_ollama_bundle(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_stop_ollama_bundle_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_restart_ollama_bundle(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_restart_ollama_bundle_for_runtime(sys.modules[__name__], *args, **kwargs)


def _managed_runtime_reset_ollama_bundle(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._managed_runtime_reset_ollama_bundle_for_runtime(sys.modules[__name__], *args, **kwargs)


def _get_openai_codex_exchanged_api_key() -> str:
    key = str(getattr(sys.modules[__name__], "OPENAI_CODEX_EXCHANGED_API_KEY", "") or "").strip()
    if key:
        return key
    try:
        key = str(_get_config_from_db("OPENAI_CODEX_EXCHANGED_API_KEY", "") or "").strip()
    except Exception:
        key = ""
    if key:
        try:
            setattr(sys.modules[__name__], "OPENAI_CODEX_EXCHANGED_API_KEY", key)
        except Exception:
            pass
    return key


def _openai_auth_service() -> OpenAIAuthService:
    global _openai_auth_service_cached
    if _openai_auth_service_cached is None:
        _openai_auth_service_cached = OpenAIAuthService(
            settings_db_path=SETTINGS_DB_FILE,
            issuer=str(globals().get("_OPENAI_OAUTH_ISSUER") or "https://auth.openai.com"),
            client_id=str(globals().get("_OPENAI_OAUTH_CODEX_CLIENT_ID") or "app_EMoamEEZ73f0CkXaXp7hrann"),
            encrypt=_auth_encrypt,
            decrypt=_auth_decrypt,
            apply_api_key=_apply_openai_api_key_from_oauth,
            set_legacy_refresh=_set_legacy_openai_oauth_refresh,
            get_legacy_refresh=_get_legacy_openai_oauth_refresh,
            clear_derived_api_key=_clear_openai_codex_exchanged_api_key,
            get_user_id=_current_user_id_or_zero,
        )
    return _openai_auth_service_cached


def _codex_home_root() -> Path:
    base = Path(str(CONFIG_DIR or "/config")).expanduser()
    return base / "codex_runtime"


def _codex_home_for_user(user_id: int | None) -> Path:
    uid = max(0, int(user_id or 0))
    return _codex_home_root() / f"user-{uid}"


def _openai_codex_runtime_tokens(
    user_id: int | None,
    *,
    force_refresh: bool = False,
    require_id_token: bool = True,
) -> dict[str, Any]:
    svc = _openai_auth_service()
    return svc.get_runtime_tokens(
        user_id,
        provider_id="openai-codex",
        require_id_token=bool(require_id_token),
        ensure_runtime_key=bool(force_refresh),
    )


def _write_codex_auth_json(user_id: int | None, *, force_refresh: bool = False) -> Path:
    tokens = _openai_codex_runtime_tokens(user_id, force_refresh=force_refresh, require_id_token=True)
    access_token = str(tokens.get("access_token") or "").strip()
    refresh_token = str(tokens.get("refresh_token") or "").strip()
    id_token = str(tokens.get("id_token") or "").strip()
    account_id = str(tokens.get("account_id") or "").strip()
    if not access_token:
        raise RuntimeError("OpenAI Codex OAuth access token is unavailable")
    home = _codex_home_for_user(user_id)
    home.mkdir(parents=True, exist_ok=True)
    payload = {
        "OPENAI_API_KEY": None,
        "auth_mode": "chatgpt",
        "last_refresh": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "tokens": {
            "id_token": id_token,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "account_id": account_id,
        },
    }
    auth_path = home / "auth.json"
    tmp_path = auth_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    try:
        os.chmod(tmp_path, 0o600)
    except Exception:
        pass
    os.replace(tmp_path, auth_path)
    return auth_path


def _openai_usage_dict_from_codex(raw_usage: dict[str, Any] | None) -> dict[str, Any]:
    return _codex_exec_runtime.openai_usage_dict_from_codex(raw_usage)


def _codex_extract_final_text(stdout_text: str) -> tuple[str, dict[str, Any], str | None]:
    return _codex_exec_runtime.codex_extract_final_text(stdout_text)


def _build_codex_prompt(system_msg: str, user_msg: str) -> str:
    return _codex_exec_runtime.build_codex_prompt(system_msg, user_msg)


def _materialize_codex_images(
    temp_dir: Path,
    image_urls: Optional[List[str]] = None,
    image_base64: Optional[List[dict]] = None,
) -> list[Path]:
    return _codex_exec_runtime.materialize_codex_images(
        temp_dir,
        image_urls=image_urls,
        image_base64=image_base64,
    )


def _run_openai_codex_exec(
    *,
    system_msg: str,
    user_msg: str,
    analysis_type: str,
    request_timeout_sec: float | None = None,
    web_search: bool = False,
    image_urls: Optional[List[str]] = None,
    image_base64: Optional[List[dict]] = None,
) -> tuple[str, dict[str, Any]]:
    return _codex_exec_runtime.run_openai_codex_exec_for_runtime(
        sys.modules[__name__],
        system_msg=system_msg,
        user_msg=user_msg,
        analysis_type=analysis_type,
        request_timeout_sec=request_timeout_sec,
        web_search=web_search,
        image_urls=image_urls,
        image_base64=image_base64,
    )


def _resolve_openai_client_for_runtime(provider_for_usage: str, user_id: int | None) -> tuple[Any | None, str, str]:
    return _ai_provider_config_runtime._resolve_openai_client_for_runtime_for_runtime(
        sys.modules[__name__],
        provider_for_usage,
        user_id,
    )


def _ai_guardrail_precheck_safe(**kwargs: Any) -> tuple[bool, str, dict[str, Any]]:
    """
    Guardrails are defined later in this module. During early startup probes, that
    symbol may not be available yet; in that case we allow the probe call through.
    """
    guard_fn = globals().get("_ai_guardrail_precheck")
    if callable(guard_fn):
        return guard_fn(**kwargs)
    return True, "", {}


def call_ai_provider(
    provider: str,
    model: str,
    system_msg: str,
    user_msg: str,
    max_tokens: int = 256,
    *,
    analysis_type: str,
    request_timeout_sec: float | None = None,
) -> str:
    return _ai_provider_runtime.call_ai_provider_for_runtime(
        sys.modules[__name__],
        provider,
        model,
        system_msg,
        user_msg,
        max_tokens=max_tokens,
        analysis_type=analysis_type,
        request_timeout_sec=request_timeout_sec,
    )


def call_ai_provider_vision(
    provider: str,
    model: str,
    system_msg: str,
    user_msg: str,
    image_urls: Optional[List[str]] = None,
    image_base64: Optional[List[dict]] = None,
    max_tokens: int = 32,
    *,
    analysis_type: str,
) -> str:
    return _ai_provider_runtime.call_ai_provider_vision_for_runtime(
        sys.modules[__name__],
        provider,
        model,
        system_msg,
        user_msg,
        image_urls=image_urls,
        image_base64=image_base64,
        max_tokens=max_tokens,
        analysis_type=analysis_type,
    )


def parse_ai_confidence(reply: str) -> tuple[str, Optional[int]]:
    return _ai_provider_runtime.parse_ai_confidence(reply)


def _call_ai_provider_bounded(
    *,
    provider: str,
    model: str,
    system_msg: str,
    user_msg: str,
    max_tokens: int,
    analysis_type: str,
    timeout_sec: float,
    log_prefix: str,
) -> str:
    return _ai_provider_runtime.call_ai_provider_bounded_for_runtime(
        sys.modules[__name__],
        provider=provider,
        model=model,
        system_msg=system_msg,
        user_msg=user_msg,
        max_tokens=max_tokens,
        analysis_type=analysis_type,
        timeout_sec=timeout_sec,
        log_prefix=log_prefix,
    )


def _run_callable_bounded(
    func,
    *args: Any,
    timeout_sec: float,
    log_prefix: str,
    **kwargs: Any,
) -> Any:
    return _execution_runtime._run_callable_bounded_for_runtime(
        sys.modules[__name__],
        func,
        *args,
        timeout_sec=timeout_sec,
        log_prefix=log_prefix,
        **kwargs,
    )


def ai_verify_mb_match(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime.ai_verify_mb_match_for_runtime(sys.modules[__name__], *args, **kwargs)



def _probe_ai_choose_best_response(model_name: str) -> bool:
    return _ai_provider_config_runtime._probe_ai_choose_best_response_for_runtime(sys.modules[__name__], model_name)


# Curated list of OpenAI Chat Completions models known to work with PMDA (parseable index|rationale|extras).
# Only these are shown in Settings. (We probe the model at runtime to verify token params + output format.)
OPENAI_COMPATIBLE_MODELS = [
    "gpt-5",
    "gpt-5-mini",
    "gpt-5-nano",
    "gpt-4o-mini",
    "gpt-4o",
    "gpt-4.1",
    "gpt-4.1-mini",
    "gpt-4.1-nano",
    "gpt-4-turbo",
    "gpt-4",
    "gpt-3.5-turbo",
]

# Probe the configured OpenAI model (no fallbacks). If it fails, AI is disabled and the UI
# will show a clear error. This keeps configuration simple: one model applies everywhere.
_startup_model = (OPENAI_MODEL or "").strip() or "gpt-4o-mini"
style = _probe_model(_startup_model)
if style and _probe_ai_choose_best_response(_startup_model):
    RESOLVED_MODEL = _startup_model
    RESOLVED_PARAM_STYLE = style
    logging.info("Using requested OpenAI model '%s' (%s)", RESOLVED_MODEL, RESOLVED_PARAM_STYLE)
else:
    if openai_client:
        ai_provider_ready = False
        _detail = (getattr(sys.modules[__name__], "OPENAI_MODEL_PROBE_LAST_ERROR", "") or "").strip()
        _suffix = f" Reason: {_detail}" if _detail else ""
        AI_FUNCTIONAL_ERROR_MSG = (
            f"AI disabled: OpenAI model '{_startup_model}' failed PMDA preflight (unsupported params or empty/unparseable output)."
            f"{_suffix}"
        )
        logging.warning("OpenAI: model probe failed; AI disabled. %s", AI_FUNCTIONAL_ERROR_MSG)

# (10) Validate Plex connection ------------------------------------------------
# (10) Validate Plex connection ------------------------------------------------
def _validate_plex_connection():
    """Compatibility no-op for the removed Plex source database startup check."""
    logging.info("Skipping legacy Plex connection check; Plex is supported only as a player refresh target.")
    return True

# ─────────────────────────────── SELF‑DIAGNOSTIC ────────────────────────────────
def _self_diag() -> bool:
    """
    Files-mode startup diagnostic.

    PMDA no longer uses Plex as a source database. Plex source DB integration is
    disabled; Plex support is limited to post-publication player refresh.
    """
    logging.info("Self diagnostic: files mode active; Plex DB access is disabled for source-library checks.")
    if not (DUPE_ROOT.exists() and os.access(DUPE_ROOT, os.W_OK)):
        warn = (
            "⚠ /dupes is missing or read-only – PMDA cannot move duplicates. "
            "Bind-mount a writable host folder to /dupes."
        )
        logging.warning(warn)
        notify_discord(warn)
    for mount in [str(DUPE_ROOT), str(CONFIG_DIR)]:
        p = Path(mount)
        if not p.exists():
            continue
        rw = ("r" if os.access(p, os.R_OK) else "-") + ("w" if os.access(p, os.W_OK) else "-")
        if rw != "rw":
            logging.warning("⚠ %s permissions: %s", p, rw)
        else:
            logging.info("✓ %s permissions: %s", p, rw)
    try:
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            prompt_text = AI_PROMPT_FILE.read_text(encoding="utf-8")
            logging.debug("Using ai_prompt.txt:\n%s", prompt_text)
    except Exception as exc:
        logging.warning("Could not read ai_prompt.txt: %s", exc)
    return True


# SQL fragment used by both startup cross-check and /api/paths/verify (same logic)
_PATH_VERIFY_EXTENSIONS = (
    "mp.file LIKE '%.flac' OR mp.file LIKE '%.wav' OR mp.file LIKE '%.m4a' OR mp.file LIKE '%.mp3'"
    " OR mp.file LIKE '%.ogg' OR mp.file LIKE '%.opus' OR mp.file LIKE '%.aac' OR mp.file LIKE '%.ape' OR mp.file LIKE '%.alac'"
    " OR mp.file LIKE '%.dsf' OR mp.file LIKE '%.aif' OR mp.file LIKE '%.aiff' OR mp.file LIKE '%.wma'"
    " OR mp.file LIKE '%.mp4' OR mp.file LIKE '%.m4b' OR mp.file LIKE '%.m4p' OR mp.file LIKE '%.aifc'"
)


def _run_path_verification(*args, **kwargs):
    return _source_roots_runtime._run_path_verification_for_runtime(sys.modules[__name__], *args, **kwargs)



def _discover_bindings_by_content(*args, **kwargs):
    return _source_roots_runtime._discover_bindings_by_content_for_runtime(sys.modules[__name__], *args, **kwargs)



def _discover_one_binding(*args, **kwargs):
    return _source_roots_runtime._discover_one_binding_for_runtime(sys.modules[__name__], *args, **kwargs)



# ──────────────────────────────── CROSS‑CHECK PATH BINDINGS ────────────────────────────────
def _cross_check_bindings(*args, **kwargs):
    return _source_roots_runtime._cross_check_bindings_for_runtime(sys.modules[__name__], *args, **kwargs)



# ───────────────────────────────── OTHER CONSTANTS ──────────────────────────────────
AUDIO_RE    = re.compile(r"\.(flac|ape|alac|wav|m4a|aac|mp3|ogg|opus|dsf|aif|aiff|wma|mp4|m4b|m4p|aifc)$", re.I)
# Derive format scores from user preference order
FMT_SCORE   = {ext: len(FORMAT_PREFERENCE)-i for i, ext in enumerate(FORMAT_PREFERENCE)}
OVERLAP_MIN = 0.85  # 85% track-title overlap minimum

# ───────────────────────────────── STATE DB SETUP ──────────────────────────────────
STATE_DB_BUSY_TIMEOUT_SECONDS = max(
    30.0,
    float(os.getenv("PMDA_STATE_DB_BUSY_TIMEOUT_SECONDS", "120") or "120"),
)
STATE_DB_BUSY_TIMEOUT_MS = int(STATE_DB_BUSY_TIMEOUT_SECONDS * 1000)
STATE_DB_READ_BUSY_TIMEOUT_MS = int(
    max(1000.0, min(15000.0, STATE_DB_BUSY_TIMEOUT_SECONDS * 1000))
)
_STATE_DB_LOCK_FRAGMENTS = (
    "database is locked",
    "database table is locked",
    "database schema is locked",
    "database is busy",
)
from pmda_core import state_db as _state_db_core
from pmda_core import schema as _state_schema

_state_db_write_lock = _state_db_core.STATE_DB_WRITE_LOCK


def _is_sqlite_lock_error(exc: BaseException) -> bool:
    return _state_db_core.is_sqlite_lock_error(exc, lock_fragments=_STATE_DB_LOCK_FRAGMENTS)


def _state_db_retry(
    operation,
    *,
    label: str,
    attempts: int = 10,
    base_sleep: float = 0.25,
    max_sleep: float = 5.0,
):
    return _state_db_core.retry(
        operation,
        label=label,
        attempts=attempts,
        base_sleep=base_sleep,
        max_sleep=max_sleep,
        lock_fragments=_STATE_DB_LOCK_FRAGMENTS,
    )


def _state_db_write_retry(operation, *, label: str, attempts: int = 10):
    return _state_db_core.write_retry(
        operation,
        label=label,
        attempts=attempts,
        write_lock=_state_db_write_lock,
    )


def _state_db_enable_wal(con: sqlite3.Connection, *, label: str = "state-db") -> None:
    _state_db_core.enable_wal(
        con,
        busy_timeout_ms=STATE_DB_BUSY_TIMEOUT_MS,
        label=label,
        attempts=10,
    )


def init_state_db():
    return _state_schema.init_state_db(
        state_db_file=STATE_DB_FILE,
        state_db_busy_timeout_seconds=STATE_DB_BUSY_TIMEOUT_SECONDS,
        enable_wal=_state_db_enable_wal,
        ai_pricing_default_rows=AI_PRICING_DEFAULT_ROWS,
        ai_pricing_version=AI_PRICING_VERSION,
    )


PIPELINE_JOB_TYPES = _scan_orchestrator_core.PIPELINE_JOB_TYPES
PIPELINE_JOB_STALE_AFTER_SEC = _scan_orchestrator_core.PIPELINE_JOB_STALE_AFTER_SEC


def _pipeline_job_update(*args: Any, **kwargs: Any) -> Any:
    return _pipeline_jobs_runtime._pipeline_job_update_for_runtime(sys.modules[__name__], *args, **kwargs)


def _pipeline_job_snapshot(*args: Any, **kwargs: Any) -> Any:
    return _pipeline_jobs_runtime._pipeline_job_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_moves_columns(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._scan_moves_columns_for_runtime(sys.modules[__name__], *args, **kwargs)


def _insert_scan_move_row(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._insert_scan_move_row_for_runtime(sys.modules[__name__], *args, **kwargs)


def init_settings_db():
    return _state_schema.init_settings_db(settings_db_file=SETTINGS_DB_FILE)


def migrate_settings_from_state_db():
    """One-time migration: copy configuration keys from legacy state.db.settings to settings.db."""
    # Only run if settings.db exists but has an empty settings table, and legacy state.db/settings exists
    try:
        if not STATE_DB_FILE.exists():
            return
        # Check legacy settings in state.db
        con_state = sqlite3.connect(str(STATE_DB_FILE))
        cur_state = con_state.cursor()
        cur_state.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='settings'")
        if not cur_state.fetchone():
            con_state.close()
            return
        cur_state.execute("SELECT key, value FROM settings")
        rows = cur_state.fetchall()
        con_state.close()
        if not rows:
            return
        # Ensure settings.db and its table exist
        init_settings_db()
        con_cfg = sqlite3.connect(str(SETTINGS_DB_FILE))
        cur_cfg = con_cfg.cursor()
        inserted = 0
        # Copy all keys except last_completed_scan_id (runtime-only)
        for key, value in rows:
            if key == "last_completed_scan_id":
                continue
            cur_cfg.execute(
                "INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)",
                (key, value),
            )
            if cur_cfg.rowcount and cur_cfg.rowcount > 0:
                inserted += int(cur_cfg.rowcount)
        con_cfg.commit()
        con_cfg.close()
        if inserted > 0:
            logging.info("Migrated %d legacy setting(s) from state.db to settings.db", inserted)
        else:
            logging.debug("Legacy settings migration skipped (nothing new to import).")
    except Exception as e:
        logging.warning("Failed to migrate settings from state.db to settings.db: %s", e)


MCP_DEFAULT_SCOPES = ("read", "scan_control", "runtime_repair", "review_propose")
MCP_TOKEN_PREFIX = "pmda_mcp_"


def _mcp_token_hash(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_token_hash_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_enabled(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_enabled_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_normalize_scopes(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_normalize_scopes_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_row_to_token(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_row_to_token_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_active_token_snapshot(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_active_token_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_latest_audit(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_latest_audit_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_status_summary(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_status_summary_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_generate_token(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_generate_token_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_revoke_active_tokens(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_revoke_active_tokens_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_authenticate_token(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_authenticate_token_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_request_tool_and_args(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_request_tool_and_args_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_auth_guard(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_auth_guard_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_scrub_args(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_scrub_args_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_audit(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_audit_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_require_scope(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_require_scope_for_runtime(sys.modules[__name__], *args, **kwargs)


def _json_loads_safe(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _mcp_route_payload(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_route_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_scan_status(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_scan_status_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_scan_history(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_scan_history_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_logs_tail(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_logs_tail_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_duplicate_groups(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_duplicate_groups_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_incomplete_albums(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_incomplete_albums_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_safe_limit(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_safe_limit_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_percent(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_percent_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_scan_id_from_args(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_scan_id_from_args_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_scan_history_row(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_scan_history_row_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_scan_trace_where(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_scan_trace_where_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_scan_trace_summary(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_scan_trace_summary_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_scan_pipeline_trace(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_scan_pipeline_trace_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_scan_moves(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_scan_moves_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_scan_resume_state(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_scan_resume_state_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_cache_stats(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_cache_stats_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_provider_cache_stats(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_provider_cache_stats_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_musicbrainz_cache_stats(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_musicbrainz_cache_stats_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_review_proposals(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_review_proposals_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_sqlite_columns(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_sqlite_columns_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_review_stats(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_review_stats_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_iso(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_iso_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_pg_table_columns(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_pg_table_columns_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_pg_group_counts(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_pg_group_counts_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_enrichment_stats_from_cursor(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_enrichment_stats_from_cursor_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_enrichment_stats(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_enrichment_stats_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_library_stats(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_library_stats_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_scan_analytics(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_scan_analytics_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_scan_results(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_scan_results_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_library_search(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_library_search_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_create_review_proposal(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_create_review_proposal_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_storage_current(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_storage_current_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mcp_storage_plan(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_storage_plan_for_runtime(sys.modules[__name__], *args, **kwargs)


def _pmda_jobs_status_snapshot() -> dict[str, Any]:
    return _job_status_runtime.pmda_jobs_status_snapshot_for_runtime(sys.modules[__name__])


def _storage_progress_payload() -> dict[str, Any]:
    acquired = _lock_try_acquire_nonblocking(lock)
    if not acquired:
        return _storage_buckets.build_storage_progress_payload(
            state,
            provider_default=STORAGE_PROVIDER or "unraid",
            include_details=False,
        )
    try:
        return _storage_buckets.build_storage_progress_payload(
            state,
            provider_default=STORAGE_PROVIDER or "unraid",
            include_details=True,
        )
    finally:
        try:
            lock.release()
        except Exception:
            pass


def _mcp_dispatch_tool(*args: Any, **kwargs: Any) -> Any:
    return _mcp_runtime._mcp_dispatch_tool_for_runtime(sys.modules[__name__], *args, **kwargs)












def get_stat(key: str) -> int:
    con = _state_connect_readonly(timeout=2.0)
    cur = con.cursor()
    try:
        cur.execute("SELECT value FROM stats WHERE key = ?", (key,))
    except sqlite3.OperationalError as e:
        # These legacy counters are non-critical UI metadata. During heavy
        # rebuilds, never let a transient SQLite lock fail an API route.
        if "no such table: stats" in str(e):
            con.close()
            return 0
        if _is_sqlite_lock_error(e):
            logging.warning("[STATE DB] stats read skipped while busy key=%s: %s", key, e)
            con.close()
            return 0
        else:
            con.close()
            raise
    row = cur.fetchone()
    con.close()
    return row[0] if row else 0

def set_stat(key: str, value: int):
    def _write() -> None:
        con = _state_connect(timeout=30)
        cur = con.cursor()
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    key   TEXT PRIMARY KEY,
                    value INTEGER
                )
            """)
            cur.execute(
                "INSERT INTO stats(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, int(value or 0)),
            )
            con.commit()
        finally:
            con.close()

    return _state_db_write_retry(_write, label=f"set_stat:{key}", attempts=10)

def increment_stat(key: str, delta: int):
    """Atomically add *delta* to a stat counter. Creates the row if it does not exist (upsert)."""
    con = _state_connect(timeout=30)
    con.execute("PRAGMA busy_timeout=30000;")
    cur = con.cursor()
    try:
        cur.execute(
            "INSERT INTO stats(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = value + ?",
            (key, delta, delta),
        )
    except sqlite3.OperationalError as e:
        # If stats table does not exist yet (legacy DB), create it and retry once.
        if "no such table: stats" in str(e):
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    key   TEXT PRIMARY KEY,
                    value INTEGER
                )
            """)
            con.commit()
            cur.execute(
                "INSERT INTO stats(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = value + ?",
                (key, delta, delta),
            )
        else:
            con.close()
            raise
    con.commit()
    con.close()

def get_last_completed_scan_id() -> Optional[int]:
    """Return the scan_id of the last completed scan, or None. Used by Library and Tag Fixer to read from scan_editions."""
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute("PRAGMA table_info(settings)")
    cols = [r[1] for r in cur.fetchall()]
    # If PRAGMA table_info returns no rows, the table does not exist (legacy DB); otherwise it does.
    if not cols:
        row = None
    else:
        cur.execute("SELECT value FROM settings WHERE key = 'last_completed_scan_id'")
        row = cur.fetchone()
    con.close()
    if not row or not row[0]:
        return None
    try:
        return int(row[0])
    except (ValueError, TypeError):
        return None


def _normalize_root_path(*args, **kwargs):
    return _source_roots_runtime._normalize_root_path_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_source_roots_fetch(*args, **kwargs):
    return _source_roots_runtime._files_source_roots_fetch_for_runtime(sys.modules[__name__], *args, **kwargs)



def _effective_files_source_rows(*args, **kwargs):
    return _source_roots_runtime._effective_files_source_rows_for_runtime(sys.modules[__name__], *args, **kwargs)



def _effective_files_roots(*args, **kwargs):
    return _source_roots_runtime._effective_files_roots_for_runtime(sys.modules[__name__], *args, **kwargs)



def _effective_files_scan_roots(*args, **kwargs):
    return _source_roots_runtime._effective_files_scan_roots_for_runtime(sys.modules[__name__], *args, **kwargs)



def _source_row_for_path(*args, **kwargs):
    return _source_roots_runtime._source_row_for_path_for_runtime(sys.modules[__name__], *args, **kwargs)



def _source_id_for_path(*args, **kwargs):
    return _source_roots_runtime._source_id_for_path_for_runtime(sys.modules[__name__], *args, **kwargs)



def _winner_source_row(*args, **kwargs):
    return _source_roots_runtime._winner_source_row_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ensure_files_source_roots_seeded(*args, **kwargs):
    return _source_roots_runtime._ensure_files_source_roots_seeded_for_runtime(sys.modules[__name__], *args, **kwargs)



def _pipeline_bootstrap_status(*args: Any, **kwargs: Any) -> Any:
    return _scan_bootstrap_runtime._pipeline_bootstrap_status_for_runtime(sys.modules[__name__], *args, **kwargs)


def _has_completed_full_scan(*args: Any, **kwargs: Any) -> Any:
    return _scan_bootstrap_runtime._has_completed_full_scan_for_runtime(sys.modules[__name__], *args, **kwargs)


def _pipeline_bootstrap_refresh_from_history(*args: Any, **kwargs: Any) -> Any:
    return _scan_bootstrap_runtime._pipeline_bootstrap_refresh_from_history_for_runtime(sys.modules[__name__], *args, **kwargs)


def _pipeline_bootstrap_mark_full_completed(*args: Any, **kwargs: Any) -> Any:
    return _scan_bootstrap_runtime._pipeline_bootstrap_mark_full_completed_for_runtime(sys.modules[__name__], *args, **kwargs)


def _pipeline_bootstrap_reset(*args: Any, **kwargs: Any) -> Any:
    return _scan_bootstrap_runtime._pipeline_bootstrap_reset_for_runtime(sys.modules[__name__], *args, **kwargs)


def _auto_changed_only_mode_effective(*args: Any, **kwargs: Any) -> Any:
    return _scan_bootstrap_runtime._auto_changed_only_mode_effective_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_autonomous_mode_effective(*args: Any, **kwargs: Any) -> Any:
    return _scan_bootstrap_runtime._scan_autonomous_mode_effective_for_runtime(sys.modules[__name__], *args, **kwargs)


def get_default_scan_type(*args: Any, **kwargs: Any) -> Any:
    return _scan_bootstrap_runtime.get_default_scan_type_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_source_roots_replace(*args, **kwargs):
    return _source_roots_runtime._files_source_roots_replace_for_runtime(sys.modules[__name__], *args, **kwargs)



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


_scheduler_lock = threading.Lock()
_scheduler_thread: Optional[threading.Thread] = None
_scheduler_stop_event = threading.Event()
_scheduler_paused = False
_scheduler_running_keys: set[str] = set()
_scheduler_running_meta: dict[str, dict[str, Any]] = {}
_scheduler_bootstrap_skip_notice_until: dict[int, float] = {}
_scheduler_skip_notice_until: dict[tuple[int, str], float] = {}
_task_events_cache_lock = threading.Lock()
_task_events_cache: dict[str, Any] = {"events": [], "max_id": 0, "ts": 0.0}
_scan_discovery_runtime_lock = threading.Lock()
_scan_discovery_runtime: dict[str, Any] | None = None


def _configure_state_connection(
    con: sqlite3.Connection,
    *,
    timeout: float = 30.0,
    readonly: bool = False,
) -> sqlite3.Connection:
    requested_ms = int(float(timeout or 0.0) * 1000.0)
    busy_timeout_ms = (
        max(1000, requested_ms, STATE_DB_READ_BUSY_TIMEOUT_MS)
        if readonly
        else max(1000, requested_ms, STATE_DB_BUSY_TIMEOUT_MS)
    )
    try:
        con.execute(f"PRAGMA busy_timeout={busy_timeout_ms};")
    except Exception:
        pass
    if not readonly:
        try:
            _state_db_enable_wal(con, label="_state_connect")
        except Exception:
            pass
    try:
        con.execute("PRAGMA temp_store=MEMORY;")
    except Exception:
        pass
    con.row_factory = sqlite3.Row
    return con


def _state_connect(timeout: float = STATE_DB_BUSY_TIMEOUT_SECONDS) -> sqlite3.Connection:
    resolved_timeout = max(float(timeout or 0.0), STATE_DB_BUSY_TIMEOUT_SECONDS)
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=resolved_timeout, check_same_thread=False)
    return _configure_state_connection(con, timeout=resolved_timeout, readonly=False)


def _state_connect_readonly(timeout: float = 10.0) -> sqlite3.Connection:
    try:
        con = sqlite3.connect(
            f"file:{STATE_DB_FILE}?mode=ro",
            timeout=timeout,
            uri=True,
            check_same_thread=False,
        )
        return _configure_state_connection(con, timeout=timeout, readonly=True)
    except Exception:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=timeout, check_same_thread=False)
        return _configure_state_connection(con, timeout=timeout, readonly=True)


def _task_events_cache_merge(events: list[dict[str, Any]], max_id: int | None = None) -> None:
    if not events and max_id is None:
        return
    with _task_events_cache_lock:
        merged: dict[int, dict[str, Any]] = {
            int(evt.get("event_id") or 0): dict(evt)
            for evt in (_task_events_cache.get("events") or [])
            if int(evt.get("event_id") or 0) > 0
        }
        for evt in events or []:
            event_id = int((evt or {}).get("event_id") or 0)
            if event_id <= 0:
                continue
            prev = dict(merged.get(event_id) or {})
            next_evt = dict(evt)
            for key in ("run_id", "job_type", "scope", "source"):
                if not next_evt.get(key) and prev.get(key):
                    next_evt[key] = prev.get(key)
            if next_evt.get("ended_at") is None and prev.get("ended_at") is not None:
                next_evt["ended_at"] = prev.get("ended_at")
            if next_evt.get("duration_ms") is None and prev.get("duration_ms") is not None:
                next_evt["duration_ms"] = prev.get("duration_ms")
            merged[event_id] = next_evt
        ordered = [merged[eid] for eid in sorted(merged.keys())]
        if len(ordered) > 1000:
            ordered = ordered[-1000:]
        cache_max_id = int(_task_events_cache.get("max_id") or 0)
        if max_id is None:
            max_id = cache_max_id
        if ordered:
            max_id = max(int(max_id or 0), int(ordered[-1].get("event_id") or 0))
        _task_events_cache["events"] = ordered
        _task_events_cache["max_id"] = int(max_id or 0)
        _task_events_cache["ts"] = time.time()


def _task_events_cache_read(after_id: int = 0, limit: int = 100) -> tuple[list[dict[str, Any]], int, float]:
    with _task_events_cache_lock:
        events = [dict(evt) for evt in (_task_events_cache.get("events") or []) if int((evt or {}).get("event_id") or 0) > int(after_id or 0)]
        max_id = int(_task_events_cache.get("max_id") or 0)
        ts = float(_task_events_cache.get("ts") or 0.0)
    if limit > 0:
        events = events[:limit]
    return events, max_id, ts


def _set_scan_discovery_runtime(snapshot: dict[str, Any] | None) -> None:
    global _scan_discovery_runtime
    with _scan_discovery_runtime_lock:
        _scan_discovery_runtime = snapshot


def _clear_scan_discovery_runtime(run_id: str | None = None) -> None:
    global _scan_discovery_runtime
    with _scan_discovery_runtime_lock:
        if run_id and isinstance(_scan_discovery_runtime, dict):
            current_run_id = str(_scan_discovery_runtime.get("run_id") or "").strip()
            if current_run_id and current_run_id != str(run_id).strip():
                return
        _scan_discovery_runtime = None


def _copy_scan_discovery_runtime(run_id: str | None = None) -> dict[str, Any] | None:
    with _scan_discovery_runtime_lock:
        snap = _scan_discovery_runtime
        if not isinstance(snap, dict):
            return None
        current_run_id = str(snap.get("run_id") or "").strip()
        if run_id and current_run_id and current_run_id != str(run_id).strip():
            return None
        return copy.deepcopy(snap)


def _json_dumps_safe(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return "{}"


_ai_usage_local = threading.local()
_ai_usage_worker_lock = threading.Lock()
_ai_usage_worker_thread: Optional[threading.Thread] = None
_ai_usage_stop_event = threading.Event()
_ai_usage_queue: Queue[Any] = Queue()
_AI_USAGE_STOP_SENTINEL = "__pmda_ai_usage_stop__"
_ai_guard_runtime_lock = threading.Lock()
_ai_guard_runtime: dict[int, dict[str, Any]] = {}


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _microusd_to_usd(value_microusd: int | None) -> float:
    if value_microusd is None:
        return 0.0
    return float(Decimal(int(value_microusd)) / Decimal(1_000_000))


def _microusd_half_up_from_rate(quantity: int, rate_microusd_per_1m: int) -> int:
    q = max(0, int(quantity or 0))
    r = int(rate_microusd_per_1m or 0)
    if q <= 0 or r <= 0:
        return 0
    raw = (Decimal(q) * Decimal(r)) / Decimal(1_000_000)
    return int(raw.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _normalize_ai_analysis_type(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in AI_ANALYSIS_TYPES:
        if raw == "other" and not AI_ALLOW_ANALYSIS_OTHER:
            return "other"
        return raw
    return "other"


def _log_preview_text(value: Any, max_chars: int = 180) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        return ""
    if len(text) <= max(16, int(max_chars or 0)):
        return text
    return text[: max(15, int(max_chars or 0) - 1)] + "…"


def _ai_trace_context_summary(ctx: dict[str, Any] | None = None) -> str:
    info = dict(ctx or _ai_infer_runtime_context() or {})
    artist = str(info.get("album_artist") or "").strip()
    title = str(info.get("album_title") or "").strip()
    album_id = _int_or_none(info.get("album_id"))
    phase = str(info.get("phase") or "").strip() or "manual"
    parts = [f"phase={phase}"]
    if album_id:
        parts.append(f"album_id={int(album_id)}")
    if artist:
        parts.append(f"artist={artist!r}")
    if title:
        parts.append(f"title={title!r}")
    return ", ".join(parts)


def _web_results_log_summary(rows: list[dict[str, Any]], *, max_items: int = 3) -> str:
    out: list[str] = []
    for row in list(rows or [])[: max(1, int(max_items or 0))]:
        if not isinstance(row, dict):
            continue
        title = _log_preview_text(row.get("title") or "", 64)
        link = str(row.get("link") or "").strip()
        host = ""
        try:
            host = urlparse(link).netloc.lower()
            if host.startswith("www."):
                host = host[4:]
        except Exception:
            host = ""
        if title and host:
            out.append(f"{title} @ {host}")
        elif title:
            out.append(title)
        elif host:
            out.append(host)
    return " | ".join(out)


def _ai_usage_context_get() -> dict[str, Any]:
    ctx = getattr(_ai_usage_local, "ctx", None)
    if isinstance(ctx, dict):
        return dict(ctx)
    return {}


def _ai_usage_context_push(**updates: Any) -> dict[str, Any]:
    prev = _ai_usage_context_get()
    nxt = dict(prev)
    for key, value in updates.items():
        if value is not None:
            nxt[key] = value
    _ai_usage_local.ctx = nxt
    return prev


def _ai_usage_context_restore(prev: dict[str, Any]) -> None:
    _ai_usage_local.ctx = dict(prev or {})


def _ai_usage_set_album_context(*, album_id: int | None, album_artist: str, album_title: str) -> None:
    ctx = _ai_usage_context_get()
    aid = _int_or_none(album_id)
    if aid and aid > 0:
        ctx["album_id"] = int(aid)
    else:
        ctx.pop("album_id", None)
    artist_value = str(album_artist or "").strip()
    if artist_value:
        ctx["album_artist"] = artist_value
    else:
        ctx.pop("album_artist", None)
    title_value = str(album_title or "").strip()
    if title_value:
        ctx["album_title"] = title_value
    else:
        ctx.pop("album_title", None)
    _ai_usage_local.ctx = ctx


def _ai_usage_endpoint_kind(kind: str | None) -> str:
    raw = str(kind or "").strip().lower()
    if raw in {"text", "vision", "longform", "web_search"}:
        return raw
    return "text"


def _ai_usage_extract_tokens(*args: Any, **kwargs: Any) -> Any:
    return _ai_usage_runtime._ai_usage_extract_tokens_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_infer_runtime_context(*args: Any, **kwargs: Any) -> Any:
    return _ai_guardrails_runtime._ai_infer_runtime_context_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_scan_budget_id_from_context(*args: Any, **kwargs: Any) -> Any:
    return _ai_guardrails_runtime._ai_scan_budget_id_from_context_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_guard_load_scan_snapshot(*args: Any, **kwargs: Any) -> Any:
    return _ai_guardrails_runtime._ai_guard_load_scan_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_guard_update_scan_state_live(*args: Any, **kwargs: Any) -> Any:
    return _ai_guardrails_runtime._ai_guard_update_scan_state_live_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_guardrail_global_non_scan_allows(*args: Any, **kwargs: Any) -> Any:
    return _ai_guardrails_runtime._ai_guardrail_global_non_scan_allows_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_guardrail_precheck(*args: Any, **kwargs: Any) -> Any:
    return _ai_guardrails_runtime._ai_guardrail_precheck_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_guard_reset_scan(*args: Any, **kwargs: Any) -> Any:
    return _ai_guardrails_runtime._ai_guard_reset_scan_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_record_legacy_scan_call_counters(*args: Any, **kwargs: Any) -> Any:
    return _ai_guardrails_runtime._ai_record_legacy_scan_call_counters_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_price_lookup(*args: Any, **kwargs: Any) -> Any:
    return _ai_usage_runtime._ai_price_lookup_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_compute_costs(*args: Any, **kwargs: Any) -> Any:
    return _ai_usage_runtime._ai_compute_costs_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_breakdown_for_scan(*args: Any, **kwargs: Any) -> Any:
    return _ai_usage_runtime._ai_breakdown_for_scan_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_lifecycle_complete_for_scan(*args: Any, **kwargs: Any) -> Any:
    return _ai_usage_runtime._ai_lifecycle_complete_for_scan_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_refresh_rollup_for_scan(*args: Any, **kwargs: Any) -> Any:
    return _ai_usage_runtime._ai_refresh_rollup_for_scan_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_usage_flush_batch(*args: Any, **kwargs: Any) -> Any:
    return _ai_usage_runtime._ai_usage_flush_batch_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_usage_worker_loop() -> None:
    pending: list[dict[str, Any]] = []
    pending_task_count = 0
    while True:
        if _ai_usage_stop_event.is_set() and _ai_usage_queue.empty() and not pending:
            break
        item: Any = None
        try:
            item = _ai_usage_queue.get(timeout=0.4)
        except Empty:
            item = None
        if item == _AI_USAGE_STOP_SENTINEL:
            if pending:
                ok = _ai_usage_flush_batch(pending)
                if not ok:
                    logging.warning("AI usage worker dropped %d pending rows on stop", len(pending))
                for _ in range(max(0, pending_task_count)):
                    try:
                        _ai_usage_queue.task_done()
                    except Exception:
                        break
                pending.clear()
                pending_task_count = 0
            try:
                _ai_usage_queue.task_done()
            except Exception:
                pass
            if _ai_usage_stop_event.is_set():
                break
            continue
        if isinstance(item, dict):
            pending.append(item)
            pending_task_count += 1
        if len(pending) >= 64 or (item is None and pending):
            ok = _ai_usage_flush_batch(pending)
            if not ok:
                # Best effort retry once by immediate sync re-flush.
                _ai_usage_flush_batch(pending)
            for _ in range(max(0, pending_task_count)):
                try:
                    _ai_usage_queue.task_done()
                except Exception:
                    break
            pending.clear()
            pending_task_count = 0


def _start_ai_usage_worker_if_needed() -> None:
    global _ai_usage_worker_thread
    with _ai_usage_worker_lock:
        if _ai_usage_worker_thread is not None and _ai_usage_worker_thread.is_alive():
            return
        _ai_usage_stop_event.clear()
        _ai_usage_worker_thread = threading.Thread(
            target=_ai_usage_worker_loop,
            daemon=True,
            name="ai-usage-writer",
        )
        _ai_usage_worker_thread.start()


def _stop_ai_usage_worker() -> None:
    _ai_usage_stop_event.set()
    try:
        _ai_usage_queue.put_nowait(_AI_USAGE_STOP_SENTINEL)
    except Exception:
        pass
    with _ai_usage_worker_lock:
        t = _ai_usage_worker_thread
    if t is not None and t.is_alive():
        t.join(timeout=3.0)


def _ai_usage_wait_for_idle(max_wait_sec: float = 2.0) -> None:
    deadline = time.time() + max(0.0, float(max_wait_sec or 0.0))
    while time.time() < deadline:
        unfinished = int(getattr(_ai_usage_queue, "unfinished_tasks", 0) or 0)
        if unfinished <= 0 and _ai_usage_queue.empty():
            time.sleep(0.05)
            unfinished2 = int(getattr(_ai_usage_queue, "unfinished_tasks", 0) or 0)
            if unfinished2 <= 0 and _ai_usage_queue.empty():
                return
        time.sleep(0.05)


def _wait_queue_idle(
    queue_obj: Any,
    *,
    max_wait_sec: float,
    label: str,
    worker_thread: threading.Thread | None = None,
    poll_sec: float = 0.25,
) -> bool:
    """
    Wait for a Queue-style worker to drain without risking an infinite join().

    Queue.join() has no timeout. In scan finalization that is unacceptable: a
    dead post-processing worker or a huge optional enrichment backlog can leave
    the operator UI stuck at the last pipeline stage for days.
    """
    deadline = time.time() + max(0.0, float(max_wait_sec or 0.0))
    label_clean = str(label or "queue")
    while True:
        try:
            unfinished = int(getattr(queue_obj, "unfinished_tasks", 0) or 0)
        except Exception:
            unfinished = 0
        if unfinished <= 0:
            return True
        if worker_thread is not None and not worker_thread.is_alive():
            logging.warning(
                "%s drain stopped because worker thread exited with %d unfinished task(s); scan will settle and leave optional work for background recovery.",
                label_clean,
                unfinished,
            )
            return False
        if time.time() >= deadline:
            logging.warning(
                "%s drain timed out after %.1fs with %d unfinished task(s); scan will settle instead of blocking indefinitely.",
                label_clean,
                max(0.0, float(max_wait_sec or 0.0)),
                unfinished,
            )
            return False
        time.sleep(max(0.01, float(poll_sec or 0.25)))


def record_ai_usage(*args: Any, **kwargs: Any) -> Any:
    return _ai_guardrails_runtime.record_ai_usage_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ai_scan_cost_summary(*args: Any, **kwargs: Any) -> Any:
    return _ai_usage_runtime._ai_scan_cost_summary_for_runtime(sys.modules[__name__], *args, **kwargs)



def _record_ai_override_event(
    *,
    domain: str,
    target_key: str,
    action: str,
    details: dict[str, Any] | None = None,
) -> None:
    domain_norm = str(domain or "").strip().lower()
    if domain_norm not in AI_DOMAIN_NAMES:
        return
    key = str(target_key or "").strip()
    act = str(action or "").strip().lower()
    if not key or not act:
        return
    try:
        con = _state_connect(timeout=10)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO ai_override_events (created_at, domain, target_key, action, details_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (time.time(), domain_norm, key, act, _json_dumps_safe(details or {})),
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("record_ai_override_event failed", exc_info=True)


def _analysis_dir_path() -> Path:
    return _ai_usage_runtime._analysis_dir_path_for_runtime(sys.modules[__name__])


def _latest_ai_benchmark_for_domain(domain: str) -> dict[str, Any] | None:
    return _ai_usage_runtime._latest_ai_benchmark_for_domain_for_runtime(sys.modules[__name__], domain)


def _ai_domain_usage_summary(domain: str) -> dict[str, Any]:
    return _ai_usage_runtime._ai_domain_usage_summary_for_runtime(sys.modules[__name__], domain)


def _ai_queue_domain_requires_idle_scan(domain: str) -> bool:
    return _ai_domain_queue_runtime.ai_queue_domain_requires_idle_scan(domain)


def _ai_queue_primary_analysis_type(domain: str) -> str:
    return _ai_domain_queue_runtime.ai_queue_primary_analysis_type(domain)


def _ai_queue_status_snapshot(domain: str | None = None) -> dict[str, Any]:
    return _ai_domain_queue_runtime.ai_queue_status_snapshot_for_runtime(sys.modules[__name__], domain)


def _ai_queue_update_metrics(domain: str, *, status: str, latency_ms: int, result: dict[str, Any] | None = None, error: str = "") -> None:
    return _ai_domain_queue_runtime.ai_queue_update_metrics_for_runtime(
        sys.modules[__name__],
        domain,
        status=status,
        latency_ms=latency_ms,
        result=result,
        error=error,
    )


def _ai_queue_process_matching(item: dict[str, Any]) -> dict[str, Any]:
    return _ai_domain_queue_runtime.ai_queue_process_matching_for_runtime(sys.modules[__name__], item)


def _ai_queue_process_dedupe(item: dict[str, Any]) -> dict[str, Any]:
    return _ai_domain_queue_runtime.ai_queue_process_dedupe_for_runtime(sys.modules[__name__], item)


def _ai_queue_process_review(item: dict[str, Any]) -> dict[str, Any]:
    return _ai_domain_queue_runtime.ai_queue_process_review_for_runtime(sys.modules[__name__], item)


def _run_ai_domain_worker(domain: str) -> None:
    return _ai_domain_queue_runtime.run_ai_domain_worker_for_runtime(sys.modules[__name__], domain)


def _trigger_ai_domain_queue_async(domain: str, payload: dict[str, Any]) -> tuple[bool, str]:
    return _ai_domain_queue_runtime.trigger_ai_domain_queue_async_for_runtime(sys.modules[__name__], domain, payload)


def _ai_overview_snapshot() -> dict[str, Any]:
    return _ai_usage_runtime._ai_overview_snapshot_for_runtime(sys.modules[__name__])


def _normalize_task_job_type(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._normalize_task_job_type_for_runtime(sys.modules[__name__], *args, **kwargs)

def _normalize_task_scope(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._normalize_task_scope_for_runtime(sys.modules[__name__], *args, **kwargs)

def _normalize_scheduler_trigger(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._normalize_scheduler_trigger_for_runtime(sys.modules[__name__], *args, **kwargs)

def _task_event_start(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._task_event_start_for_runtime(sys.modules[__name__], *args, **kwargs)

def _task_event_finish(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._task_event_finish_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_get_paused_from_db(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_get_paused_from_db_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_set_paused(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_set_paused_for_runtime(sys.modules[__name__], *args, **kwargs)

def _parse_days_of_week(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._parse_days_of_week_for_runtime(sys.modules[__name__], *args, **kwargs)

def _parse_time_local(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._parse_time_local_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_compute_next_run(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_compute_next_run_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_rule_scope_matches(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_rule_scope_matches_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_rules_fetch(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_rules_fetch_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_rule_update_runtime(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_rule_update_runtime_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_insert_default_rules_if_empty(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_insert_default_rules_if_empty_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_migrate_legacy_scan_changed_default(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_migrate_legacy_scan_changed_default_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_migrate_legacy_scan_full_default(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_migrate_legacy_scan_full_default_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_ensure_post_scan_chain_defaults(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_ensure_post_scan_chain_defaults_for_runtime(sys.modules[__name__], *args, **kwargs)

def _pipeline_migrate_legacy_post_scan_async_default(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._pipeline_migrate_legacy_post_scan_async_default_for_runtime(sys.modules[__name__], *args, **kwargs)

def _library_migrate_legacy_include_unmatched_default(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._library_migrate_legacy_include_unmatched_default_for_runtime(sys.modules[__name__], *args, **kwargs)

def _provider_gateway_migrate_legacy_discogs_rpm_default(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._provider_gateway_migrate_legacy_discogs_rpm_default_for_runtime(sys.modules[__name__], *args, **kwargs)

def _web_search_migrate_legacy_provider_default(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._web_search_migrate_legacy_provider_default_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_job_insert(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_job_insert_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_job_update(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_job_update_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_record_skipped_job(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_record_skipped_job_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_get_latest_scan_entry(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_get_latest_scan_entry_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_job_key(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_job_key_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_pool_for_job(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_pool_for_job_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_pool_limit(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_pool_limit_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_can_start_job(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_can_start_job_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_start_scan(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_start_scan_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_wait_for_scan_completion(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_wait_for_scan_completion_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_build_improve_candidates(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_build_improve_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_run_enrich_batch(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_run_enrich_batch_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_run_dedupe(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_run_dedupe_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_run_incomplete_move(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_run_incomplete_move_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_run_export(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_run_export_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_run_player_sync(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_run_player_sync_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_execute_job(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_execute_job_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_worker(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_worker_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_launch_job(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_launch_job_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_is_enabled_rule_for_chain(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_is_enabled_rule_for_chain_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_chain_max_concurrency(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_chain_max_concurrency_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_loop(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_loop_for_runtime(sys.modules[__name__], *args, **kwargs)

def _start_scheduler_if_needed(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._start_scheduler_if_needed_for_runtime(sys.modules[__name__], *args, **kwargs)

def _stop_scheduler(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._stop_scheduler_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_chain_post_scan(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_chain_post_scan_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_rule_to_dict(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_rule_to_dict_for_runtime(sys.modules[__name__], *args, **kwargs)

def _scheduler_rules_replace(*args: Any, **kwargs: Any) -> Any:
    return _scheduler_runtime._scheduler_rules_replace_for_runtime(sys.modules[__name__], *args, **kwargs)





init_state_db()
init_settings_db()
migrate_settings_from_state_db()
_ensure_files_source_roots_seeded()
_pipeline_bootstrap_refresh_from_history()
_scheduler_insert_default_rules_if_empty()
_scheduler_migrate_legacy_scan_changed_default()
_scheduler_migrate_legacy_scan_full_default()
_scheduler_ensure_post_scan_chain_defaults()
_pipeline_migrate_legacy_post_scan_async_default()
_library_migrate_legacy_include_unmatched_default()
_provider_gateway_migrate_legacy_discogs_rpm_default()
_web_search_migrate_legacy_provider_default()
_scheduler_paused = _scheduler_get_paused_from_db()


def _metadata_job_enqueue(
    album_manifest: dict[str, Any],
    *,
    provider_hints: dict[str, Any] | None = None,
    cache_keys: list[str] | None = None,
    priority: int = 50,
    queue_name: str = "metadata",
    scope: str = "album",
    run_id: str = "",
    scan_id: int | None = None,
) -> str:
    return _metadata_jobs_core.enqueue_metadata_job(
        _state_connect,
        album_manifest,
        provider_hints=provider_hints,
        cache_keys=cache_keys,
        priority=priority,
        queue_name=queue_name,
        scope=scope,
        run_id=run_id,
        scan_id=scan_id,
    )


def _metadata_jobs_summary() -> dict[str, Any]:
    return _metadata_jobs_core.metadata_jobs_summary(_state_connect)

# ───────────────────────────────── CACHE DB SETUP ──────────────────────────────────
def init_cache_db():
    return _cache_db_runtime.init_cache_db_for_runtime(sys.modules[__name__])

def get_cached_info(path: str, mtime: int) -> Optional[tuple[int, int, int]]:
    return _cache_db_runtime.get_cached_info_for_runtime(sys.modules[__name__], path, mtime)

def set_cached_info(path: str, mtime: int, bit_rate: int, sample_rate: int, bit_depth: int):
    return _cache_db_runtime.set_cached_info_for_runtime(
        sys.modules[__name__],
        path,
        mtime,
        bit_rate,
        sample_rate,
        bit_depth,
    )


def get_cached_acoustid(path: str) -> Optional[tuple[float, str]]:
    return _cache_db_runtime.get_cached_acoustid_for_runtime(sys.modules[__name__], path)


def set_cached_acoustid(path: str, duration: float, fingerprint: str):
    return _cache_db_runtime.set_cached_acoustid_for_runtime(sys.modules[__name__], path, duration, fingerprint)


init_cache_db()

# ───────────────────── Files Library Index (PostgreSQL + Redis) ─────────────────────
_FILES_PG_SCHEMA_READY = False
_FILES_REDIS_CLIENT = None
_FILES_CACHE_LOCAL_ENABLED = True
_FILES_LOCAL_CACHE: "OrderedDict[str, dict]" = OrderedDict()
_FILES_LOCAL_CACHE_LOCK = threading.Lock()
_CACHE_TELEMETRY_SNAPSHOT = {"ts": 0.0, "payload": None}
_CACHE_TELEMETRY_LOCK = threading.Lock()
_FILES_REDIS_LAST_ERROR = ""
_FILES_REDIS_LAST_OK_TS = 0.0
_FILES_REDIS_NEXT_RETRY_TS = 0.0
_FILES_PG_LAST_ERROR = ""
_FILES_PG_LAST_OK_TS = 0.0
_REDIS_CPU_SAMPLE_LOCK = threading.Lock()
_REDIS_CPU_LAST_SAMPLE: dict[str, tuple[float, int, int]] = {}
_REDIS_IDLE_DRIFT_STREAK = 0
_REDIS_IDLE_LAST_WARN_TS = 0.0

_LOSSLESS_FORMATS = {"FLAC", "ALAC", "APE", "WV", "WAV", "AIFF", "DSF"}
_ARTIST_IMAGE_NAMES = (
    "artist.jpg", "artist.jpeg", "artist.png", "artist.webp",
    "artists.jpg", "artists.jpeg", "artists.png", "artists.webp",
    "fanart.jpg", "fanart.jpeg", "fanart.png", "fanart.webp",
)


def _files_pg_connect_kwargs() -> dict:
    return _files_pg_runtime.files_pg_connect_kwargs_for_runtime(sys.modules[__name__])


_FILES_PG_MAX_CONNS = max(4, int(os.getenv("FILES_PG_MAX_CONNS", "16") or 16))
_FILES_PG_ACQUIRE_TIMEOUT_SEC = max(0.5, float(os.getenv("FILES_PG_ACQUIRE_TIMEOUT_SEC", "8") or 8))
_FILES_PG_UI_RESERVED_CONNS = max(
    0,
    min(_FILES_PG_MAX_CONNS - 1, int(os.getenv("FILES_PG_UI_RESERVED_CONNS", "4") or 4)),
)
_FILES_PG_BG_MAX_CONNS = max(1, _FILES_PG_MAX_CONNS - _FILES_PG_UI_RESERVED_CONNS)
_FILES_PG_CONN_GATE = threading.BoundedSemaphore(_FILES_PG_MAX_CONNS)
_FILES_PG_BG_CONN_GATE = threading.BoundedSemaphore(_FILES_PG_BG_MAX_CONNS)
_FILES_PG_CONN_STATE_LOCK = threading.Lock()
_FILES_PG_CONN_ACTIVE = 0
_FILES_PG_IDLE_REAP_SEC = max(15.0, float(os.getenv("FILES_PG_IDLE_REAP_SEC", "90") or 90))
_FILES_PG_CONN_REGISTRY_LOCK = threading.Lock()
_FILES_PG_CONN_REGISTRY: dict[int, dict[str, Any]] = {}
_FILES_PG_CONN_TOKEN_SEQ = itertools.count(1)


def _files_pg_conn_is_idle(conn) -> bool:
    return _files_pg_runtime.files_pg_conn_is_idle_for_runtime(sys.modules[__name__], conn)


def _files_pg_register_connection(conn, *, release_bg_gate: bool = False) -> int:
    return _files_pg_runtime.files_pg_register_connection_for_runtime(
        sys.modules[__name__],
        conn,
        release_bg_gate=release_bg_gate,
    )


def _files_pg_touch_connection(token: int) -> None:
    return _files_pg_runtime.files_pg_touch_connection_for_runtime(sys.modules[__name__], token)


def _files_pg_release_connection_by_token(token: int, *, close_conn: bool = True) -> None:
    return _files_pg_runtime.files_pg_release_connection_by_token_for_runtime(
        sys.modules[__name__],
        token,
        close_conn=close_conn,
    )


def _files_pg_reap_stale_connections(
    *,
    idle_timeout_sec: float | None = None,
    closed_only: bool = False,
    log_reason: str = "",
) -> int:
    return _files_pg_runtime.files_pg_reap_stale_connections_for_runtime(
        sys.modules[__name__],
        idle_timeout_sec=idle_timeout_sec,
        closed_only=closed_only,
        log_reason=log_reason,
    )


def _files_pg_connect(*, autocommit: bool = True, acquire_timeout_sec: float | None = None):
    return _files_pg_runtime.files_pg_connect_for_runtime(
        sys.modules[__name__],
        autocommit=autocommit,
        acquire_timeout_sec=acquire_timeout_sec,
    )


def _files_pg_error_text(exc: Any) -> str:
    return _files_pg_runtime.files_pg_error_text(exc)


def _files_pg_is_connection_dropped_error(exc: Any) -> bool:
    return _files_pg_runtime.files_pg_is_connection_dropped_error(exc)


@contextmanager
def _files_pg_connection(*, autocommit: bool = True):
    with _files_pg_runtime.files_pg_connection_for_runtime(sys.modules[__name__], autocommit=autocommit) as conn:
        yield conn


@contextmanager
def _files_pg_statement_timeout(cur, timeout_ms: int | None):
    raw_ms = int(timeout_ms or 0)
    if raw_ms <= 0:
        yield
        return
    if hasattr(cur, "_responses"):
        # Unit-test cursors use queued responses; issuing SET would consume a
        # mocked result row and hide the query under test.
        yield
        return
    safe_ms = max(1, raw_ms)
    try:
        cur.execute(f"SET statement_timeout TO '{safe_ms}ms'")
    except Exception:
        yield
        return
    yield


def _files_pg_is_statement_timeout_error(exc: Any) -> bool:
    text = _files_pg_error_text(exc)
    return (
        "statement timeout" in text
        or "canceling statement due to statement timeout" in text
        or "query_canceled" in text
    )


def _files_pg_init_schema() -> bool:
    global _FILES_PG_SCHEMA_READY
    ok = _files_pg_schema.init_files_pg_schema(
        schema_ready=_FILES_PG_SCHEMA_READY,
        files_pg_connect=_files_pg_connect,
        migrate_external_artist_images_norm_keys=_files_migrate_external_artist_images_norm_keys,
        backfill_artist_canonical_fields=_files_backfill_artist_canonical_fields,
        backfill_artist_alias_table=_files_backfill_artist_alias_table,
        merge_duplicate_person_artists=_files_merge_duplicate_person_artists,
        relink_external_artist_images_to_canonical_norm=_files_relink_external_artist_images_to_canonical_norm,
        purge_weak_classical_artist_images=_files_purge_weak_classical_artist_images,
        logger=logging,
    )
    if ok:
        _FILES_PG_SCHEMA_READY = True
    return bool(ok)


def _files_migrate_external_artist_images_norm_keys(cur) -> None:
    return _artist_maintenance.migrate_external_artist_images_norm_keys(
        cur,
        norm_artist_key=_norm_artist_key,
        path_size=_path_size,
        is_probably_placeholder_artist_image_url=_is_probably_placeholder_artist_image_url,
        logger=logging,
    )


def _files_backfill_artist_canonical_fields(conn) -> None:
    return _artist_maintenance.backfill_artist_canonical_fields(
        conn,
        norm_artist_key=_norm_artist_key,
        safe_json_load=_safe_json_load,
        artist_role_hints_from_roles_json=_artist_role_hints_from_roles_json,
        artist_is_person_like=_artist_is_person_like,
        select_classical_person_display_name=_select_classical_person_display_name,
        files_merge_artist_alias_values=_files_merge_artist_alias_values,
        files_sync_artist_aliases=_files_sync_artist_aliases,
        logger=logging,
    )


def _files_purge_weak_classical_artist_images(conn) -> None:
    return _artist_maintenance.purge_weak_classical_artist_images(
        conn,
        config_dir=CONFIG_DIR,
        media_cache_root=MEDIA_CACHE_ROOT,
        artist_role_hints_from_roles_json=_artist_role_hints_from_roles_json,
        artist_entity_is_classical_like=_artist_entity_is_classical_like,
        artist_external_image_requires_authoritative_refresh=_artist_external_image_requires_authoritative_refresh,
        artist_image_url_looks_relevant=_artist_image_url_looks_relevant,
        files_artist_reference_folder=_files_artist_reference_folder,
        is_artist_image_distinct_from_local_covers=_is_artist_image_distinct_from_local_covers,
        is_usable_artist_image_path=_is_usable_artist_image_path,
        paths_refer_to_same_file=_paths_refer_to_same_file,
        logger=logging,
    )


def _files_relink_external_artist_images_to_canonical_norm(conn) -> None:
    return _artist_maintenance.relink_external_artist_images_to_canonical_norm(
        conn,
        files_resolve_artist_cache_name_norm=_files_resolve_artist_cache_name_norm,
        files_upsert_external_artist_image=_files_upsert_external_artist_image,
        logger=logging,
    )


def _files_redis_client():
    global _FILES_REDIS_CLIENT, _FILES_REDIS_LAST_ERROR, _FILES_REDIS_LAST_OK_TS, _FILES_REDIS_NEXT_RETRY_TS
    if redis_lib is None:
        _FILES_REDIS_LAST_ERROR = "redis_library_not_installed"
        return None
    if _FILES_REDIS_CLIENT is not None:
        return _FILES_REDIS_CLIENT
    now = time.time()
    if float(_FILES_REDIS_NEXT_RETRY_TS or 0.0) > now:
        return None
    try:
        _FILES_REDIS_CLIENT = redis_lib.Redis(
            host=PMDA_REDIS_HOST,
            port=PMDA_REDIS_PORT,
            db=PMDA_REDIS_DB,
            password=(PMDA_REDIS_PASSWORD or None),
            decode_responses=True,
            socket_connect_timeout=0.15,
            socket_timeout=0.15,
            retry_on_timeout=False,
            retry=None,
            health_check_interval=0,
        )
        _FILES_REDIS_CLIENT.ping()
        _FILES_REDIS_LAST_OK_TS = time.time()
        _FILES_REDIS_LAST_ERROR = ""
        _FILES_REDIS_NEXT_RETRY_TS = 0.0
    except Exception as e:
        _FILES_REDIS_LAST_ERROR = str(e)
        logging.debug("Redis cache unavailable for files library: %s", e)
        _FILES_REDIS_CLIENT = None
        _FILES_REDIS_NEXT_RETRY_TS = time.time() + 5.0
    return _FILES_REDIS_CLIENT


def _files_local_cache_get(cache_key: str):
    if not _FILES_CACHE_LOCAL_ENABLED:
        return None
    now = time.time()
    with _FILES_LOCAL_CACHE_LOCK:
        entry = _FILES_LOCAL_CACHE.get(cache_key)
        if not entry:
            return None
        exp = float(entry.get("exp") or 0.0)
        if exp > 0.0 and now > exp:
            _FILES_LOCAL_CACHE.pop(cache_key, None)
            return None
        payload = entry.get("payload")
        _FILES_LOCAL_CACHE.move_to_end(cache_key)
        return payload


def _files_local_cache_set(cache_key: str, payload, ttl: int = 60) -> None:
    if not _FILES_CACHE_LOCAL_ENABLED:
        return
    now = time.time()
    exp = now + max(1, int(ttl or 60))
    with _FILES_LOCAL_CACHE_LOCK:
        _FILES_LOCAL_CACHE[cache_key] = {"payload": payload, "exp": exp}
        _FILES_LOCAL_CACHE.move_to_end(cache_key)
        while len(_FILES_LOCAL_CACHE) > PMDA_FILES_LOCAL_CACHE_MAX_KEYS:
            _FILES_LOCAL_CACHE.popitem(last=False)


def _files_local_cache_clear() -> None:
    with _FILES_LOCAL_CACHE_LOCK:
        _FILES_LOCAL_CACHE.clear()


def _files_cache_get_json(cache_key: str):
    cli = _files_redis_client()
    if cli is None:
        cached = _files_local_cache_get(cache_key)
        if cached is None:
            return None
        return _normalize_browser_payload_urls(cached)
    try:
        raw = cli.get(FILES_CACHE_PREFIX + cache_key)
        if not raw:
            return None
        return _normalize_browser_payload_urls(json.loads(raw))
    except Exception:
        global _FILES_REDIS_CLIENT, _FILES_REDIS_LAST_ERROR, _FILES_REDIS_NEXT_RETRY_TS
        _FILES_REDIS_CLIENT = None
        _FILES_REDIS_LAST_ERROR = "cache_get_failed"
        _FILES_REDIS_NEXT_RETRY_TS = time.time() + 5.0
        return None


def _files_cache_set_json(cache_key: str, payload, ttl: int = 60) -> None:
    payload = _normalize_browser_payload_urls(payload)
    cli = _files_redis_client()
    if cli is None:
        _files_local_cache_set(cache_key, payload, ttl=ttl)
        return
    try:
        cli.setex(FILES_CACHE_PREFIX + cache_key, ttl, json.dumps(payload))
    except Exception:
        global _FILES_REDIS_CLIENT, _FILES_REDIS_LAST_ERROR, _FILES_REDIS_NEXT_RETRY_TS
        _FILES_REDIS_CLIENT = None
        _FILES_REDIS_LAST_ERROR = "cache_set_failed"
        _FILES_REDIS_NEXT_RETRY_TS = time.time() + 5.0
        _files_local_cache_set(cache_key, payload, ttl=ttl)


def _files_cache_invalidate_all() -> None:
    _files_local_cache_clear()
    cli = _files_redis_client()
    if cli is None:
        return
    try:
        keys = list(cli.scan_iter(f"{FILES_CACHE_PREFIX}*"))
        if keys:
            cli.delete(*keys)
    except Exception:
        pass


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _read_total_cpu_jiffies() -> int:
    try:
        with open("/proc/stat", "r", encoding="utf-8", errors="ignore") as fh:
            first = fh.readline().strip()
        if not first.startswith("cpu "):
            return 0
        parts = first.split()[1:]
        return sum(int(p) for p in parts)
    except Exception:
        return 0


def _read_process_cpu_jiffies(pid: int) -> int:
    if int(pid or 0) <= 0:
        return 0
    try:
        with open(f"/proc/{int(pid)}/stat", "r", encoding="utf-8", errors="ignore") as fh:
            raw = fh.read().strip()
        if not raw:
            return 0
        # /proc/<pid>/stat format: fields 14 and 15 are utime/stime.
        fields = raw.split()
        if len(fields) < 16:
            return 0
        utime = int(fields[13])
        stime = int(fields[14])
        return max(0, utime + stime)
    except Exception:
        return 0


def _sample_process_cpu_pct(sample_key: str, pid: int) -> float | None:
    key = str(sample_key or "").strip()
    proc_jiffies = _read_process_cpu_jiffies(int(pid or 0))
    total_jiffies = _read_total_cpu_jiffies()
    if not key or proc_jiffies <= 0 or total_jiffies <= 0:
        return None
    now = time.time()
    with _REDIS_CPU_SAMPLE_LOCK:
        prev = _REDIS_CPU_LAST_SAMPLE.get(key)
        _REDIS_CPU_LAST_SAMPLE[key] = (float(now), int(proc_jiffies), int(total_jiffies))
    if not prev:
        return None
    _prev_ts, prev_proc, prev_total = prev
    delta_proc = int(proc_jiffies) - int(prev_proc or 0)
    delta_total = int(total_jiffies) - int(prev_total or 0)
    if delta_proc < 0 or delta_total <= 0:
        return None
    cpu_count = max(1, int(os.cpu_count() or 1))
    pct = (float(delta_proc) / float(delta_total)) * 100.0 * float(cpu_count)
    if pct < 0.0:
        return 0.0
    return round(float(pct), 2)


def _read_first_int(paths: list[str]) -> Optional[int]:
    for raw in paths:
        p = Path(raw)
        if not p.exists():
            continue
        try:
            txt = p.read_text(encoding="utf-8", errors="ignore").strip()
        except Exception:
            continue
        if not txt:
            continue
        if txt.lower() == "max":
            return None
        try:
            return int(txt)
        except Exception:
            continue
    return None


def _current_process_rss_bytes() -> int:
    # Linux fast path.
    statm = Path("/proc/self/statm")
    if statm.exists():
        try:
            txt = statm.read_text(encoding="utf-8", errors="ignore").strip()
            parts = txt.split()
            if len(parts) >= 2:
                rss_pages = int(parts[1])
                page_size = os.sysconf("SC_PAGE_SIZE")
                return max(0, rss_pages * int(page_size))
        except Exception:
            pass
    return 0


def _read_container_memory_stats() -> dict:
    # cgroup v2 first.
    current = _read_first_int(
        [
            "/sys/fs/cgroup/memory.current",
            "/sys/fs/cgroup/memory/memory.usage_in_bytes",  # cgroup v1 fallback
        ]
    )
    limit = _read_first_int(
        [
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",  # cgroup v1 fallback
        ]
    )
    # Some runtimes expose huge sentinel values when unlimited.
    if limit is not None and limit >= (1 << 60):
        limit = None
    used_pct = None
    if current is not None and limit and limit > 0:
        used_pct = round((float(current) / float(limit)) * 100.0, 2)
    return {
        "current_bytes": int(current or 0),
        "limit_bytes": int(limit or 0) if limit else 0,
        "used_pct": used_pct,
    }


def _read_host_mem_available_bytes() -> int:
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        return 0
    try:
        for line in meminfo.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.startswith("MemAvailable:"):
                continue
            # e.g. "MemAvailable:   23623452 kB"
            parts = line.split()
            if len(parts) >= 2:
                return max(0, int(parts[1]) * 1024)
    except Exception:
        return 0
    return 0


def _effective_available_memory_bytes() -> int:
    """
    Conservative "currently available" memory budget.
    Uses the minimum of cgroup-available and host MemAvailable when both exist.
    """
    stats = _read_container_memory_stats()
    candidates: list[int] = []
    cgroup_current = int(stats.get("current_bytes") or 0)
    cgroup_limit = int(stats.get("limit_bytes") or 0)
    if cgroup_limit > 0:
        candidates.append(max(0, cgroup_limit - cgroup_current))
    host_available = _read_host_mem_available_bytes()
    if host_available > 0:
        candidates.append(host_available)
    if not candidates:
        return 0
    return max(0, min(candidates))


def _implicit_auto_artwork_ram_cap_mb() -> int:
    """
    Safety cap for artwork RAM auto-tune when the container has no explicit
    cgroup memory limit.

    Without this, the auto-tuner can treat large host MemAvailable values as
    fully usable and reserve tens of GB for artwork alone, which is enough to
    get the PMDA process OOM-killed under real scan load.

    Users can always override this with ARTWORK_RAM_CACHE_AUTO_MAX_MB.
    """
    if int(ARTWORK_RAM_CACHE_AUTO_MAX_MB or 0) > 0:
        return int(max(0, ARTWORK_RAM_CACHE_AUTO_MAX_MB))
    stats = _read_container_memory_stats()
    if int(stats.get("limit_bytes") or 0) > 0:
        return 0
    return 4096


def _artwork_auto_cap_log_label() -> str:
    explicit_mb = int(max(0, ARTWORK_RAM_CACHE_AUTO_MAX_MB or 0))
    if explicit_mb > 0:
        return f"{explicit_mb}MB explicit"
    implicit_mb = int(max(0, _implicit_auto_artwork_ram_cap_mb() or 0))
    if implicit_mb > 0:
        return f"{implicit_mb}MB implicit-unlimited-container guard"
    return "none"


def _path_size_bytes(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except Exception:
        return 0


def _sqlite_table_exists(cur, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table_name,),
    )
    return cur.fetchone() is not None


def _sqlite_scalar(cur, sql: str, args: tuple = ()) -> int:
    cur.execute(sql, args)
    row = cur.fetchone()
    if not row:
        return 0
    return _safe_int(row[0], 0)


def _scan_dir_usage(root: Path, max_files: int = 400000) -> dict:
    return _ops_runtime._scan_dir_usage_for_runtime(sys.modules[__name__], root, max_files=max_files)


def _read_media_cache_usage(*args, **kwargs):
    return _cache_telemetry_runtime._read_media_cache_usage_for_runtime(sys.modules[__name__], *args, **kwargs)




def _read_sqlite_cache_metrics(*args, **kwargs):
    return _cache_telemetry_runtime._read_sqlite_cache_metrics_for_runtime(sys.modules[__name__], *args, **kwargs)




def _read_state_cache_metrics(*args, **kwargs):
    return _cache_telemetry_runtime._read_state_cache_metrics_for_runtime(sys.modules[__name__], *args, **kwargs)




def _read_settings_db_metrics(*args, **kwargs):
    return _cache_telemetry_runtime._read_settings_db_metrics_for_runtime(sys.modules[__name__], *args, **kwargs)




def _read_redis_cache_metrics(*args, **kwargs):
    return _cache_telemetry_runtime._read_redis_cache_metrics_for_runtime(sys.modules[__name__], *args, **kwargs)




def _read_pg_cache_metrics(*args, **kwargs):
    return _cache_telemetry_runtime._read_pg_cache_metrics_for_runtime(sys.modules[__name__], *args, **kwargs)




def _collect_cache_control_metrics(*args, **kwargs):
    return _cache_telemetry_runtime._collect_cache_control_metrics_for_runtime(sys.modules[__name__], *args, **kwargs)




def _ops_storage_target_snapshot(path_like: str | Path | None) -> dict[str, Any]:
    return _ops_runtime._ops_storage_target_snapshot_for_runtime(sys.modules[__name__], path_like)


def _ops_backups_root_dir() -> Path:
    return _ops_runtime._ops_backups_root_dir_for_runtime(sys.modules[__name__])


def _ops_backup_dir_size_bytes(root: Path) -> int:
    return _ops_runtime._ops_backup_dir_size_bytes_for_runtime(sys.modules[__name__], root)


def _ops_list_backups(limit: int = 8) -> list[dict[str, Any]]:
    return _ops_runtime._ops_list_backups_for_runtime(sys.modules[__name__], limit=limit)


def _ops_snapshot_payload() -> dict[str, Any]:
    return _ops_runtime._ops_snapshot_payload_for_runtime(sys.modules[__name__])


def _ops_backup_sqlite_db(src: Path, dst: Path) -> dict[str, Any]:
    return _ops_runtime._ops_backup_sqlite_db_for_runtime(sys.modules[__name__], src, dst)


def _ops_backup_pg_dump(target_file: Path) -> dict[str, Any]:
    return _ops_runtime._ops_backup_pg_dump_for_runtime(sys.modules[__name__], target_file)


def _ops_create_backup_bundle(*, include_pg_dump: bool = True) -> dict[str, Any]:
    return _ops_runtime._ops_create_backup_bundle_for_runtime(
        sys.modules[__name__],
        include_pg_dump=include_pg_dump,
    )


def _normalize_identity_provider(value: str | None) -> str:
    return _materialization_policy_core.normalize_identity_provider(value)


def _scan_provider_match_keys(extra_keys: list[str] | tuple[str, ...] | None = None) -> list[str]:
    return _provider_matching_core.scan_provider_match_keys(extra_keys)


def _normalize_scan_provider_matches(
    raw_matches: dict[str, Any] | None,
    *,
    legacy_discogs: int = 0,
    legacy_lastfm: int = 0,
    legacy_bandcamp: int = 0,
) -> dict[str, int]:
    return _provider_matching_core.normalize_scan_provider_matches(
        raw_matches,
        legacy_discogs=legacy_discogs,
        legacy_lastfm=legacy_lastfm,
        legacy_bandcamp=legacy_bandcamp,
    )


def _scan_provider_matches_snapshot() -> dict[str, int]:
    with lock:
        raw = dict(state.get("scan_provider_matches") or {})
        legacy_discogs = int(state.get("scan_discogs_matched") or 0)
        legacy_lastfm = int(state.get("scan_lastfm_matched") or 0)
        legacy_bandcamp = int(state.get("scan_bandcamp_matched") or 0)
    return _normalize_scan_provider_matches(
        raw,
        legacy_discogs=legacy_discogs,
        legacy_lastfm=legacy_lastfm,
        legacy_bandcamp=legacy_bandcamp,
    )


def _scan_record_provider_match(provider_id: str | None) -> None:
    provider = _normalize_identity_provider(str(provider_id or "").strip())
    if not provider:
        return
    with lock:
        current = dict(state.get("scan_provider_matches") or {})
        current[provider] = int(current.get(provider) or 0) + 1
        state["scan_provider_matches"] = current
        if provider == "discogs":
            state["scan_discogs_matched"] = int(state.get("scan_discogs_matched") or 0) + 1
        elif provider == "lastfm":
            state["scan_lastfm_matched"] = int(state.get("scan_lastfm_matched") or 0) + 1
        elif provider == "bandcamp":
            state["scan_bandcamp_matched"] = int(state.get("scan_bandcamp_matched") or 0) + 1


def _record_files_pending_change(
    folder_path: str,
    reason: str,
    *,
    source_id: int | None = None,
    event_kind: str | None = None,
    event_path: str | None = None,
) -> None:
    return _files_watcher_runtime.record_files_pending_change_for_runtime(
        sys.modules[__name__],
        folder_path,
        reason,
        source_id=source_id,
        event_kind=event_kind,
        event_path=event_path,
    )


# Files watcher suppression ----------------------------------------------------
# The Files watcher can observe PMDA's own writes (tag updates, cover/artwork writes,
# and dupe/incomplete moves) and accidentally retrigger changed-only scans in a loop.
# We suppress events for recently-touched album folders to avoid scan storms.
_FILES_WATCHER_SUPPRESS_LOCK = threading.Lock()
_FILES_WATCHER_SUPPRESS_UNTIL: dict[str, float] = {}


def _files_watcher_suppress_folder(folder: Path | str, *, seconds: float = 90.0, reason: str = "pmda_write") -> None:
    return _files_watcher_runtime.files_watcher_suppress_folder_for_runtime(
        sys.modules[__name__],
        folder,
        seconds=seconds,
        reason=reason,
    )


def _files_watcher_is_suppressed(folder_key: str) -> bool:
    return _files_watcher_runtime.files_watcher_is_suppressed_for_runtime(sys.modules[__name__], folder_key)


def _files_watcher_should_ignore_folder_key(folder_key: str) -> bool:
    return _files_watcher_runtime.files_watcher_should_ignore_folder_key_for_runtime(sys.modules[__name__], folder_key)


def _list_files_pending_changes(limit: int = 10000) -> list[dict]:
    return _files_watcher_runtime.list_files_pending_changes_for_runtime(sys.modules[__name__], limit)


def _clear_files_pending_changes(folder_paths: list[str]) -> int:
    return _files_watcher_runtime.clear_files_pending_changes_for_runtime(sys.modules[__name__], folder_paths)


def _folder_has_audio_files(folder: Path) -> bool:
    try:
        for p in folder.iterdir():
            if p.is_file() and AUDIO_RE.search(p.name):
                return True
    except Exception:
        return False
    return False


def _resolve_album_folders_from_event_path(*args: Any, **kwargs: Any) -> Any:
    return _audio_runtime._resolve_album_folders_from_event_path_for_runtime(sys.modules[__name__], *args, **kwargs)


def _update_files_watcher_state(
    *,
    running: bool,
    roots: list[str] | None = None,
    reason: str | None = None,
) -> None:
    return _files_watcher_runtime.update_files_watcher_state_for_runtime(
        sys.modules[__name__],
        running=running,
        roots=roots,
        reason=reason,
    )


def _update_files_watcher_runtime(**fields: Any) -> None:
    return _files_watcher_runtime.update_files_watcher_runtime_for_runtime(sys.modules[__name__], **fields)


def _files_watcher_status_snapshot() -> dict[str, Any]:
    return _files_watcher_runtime.files_watcher_status_snapshot_for_runtime(sys.modules[__name__])


def _files_watcher_available() -> bool:
    return _files_watcher_runtime.files_watcher_available_for_runtime(sys.modules[__name__])


def _stop_files_watcher() -> None:
    return _files_watcher_runtime.stop_files_watcher_for_runtime(sys.modules[__name__])


def _restart_files_watcher_if_needed() -> bool:
    return _files_watcher_runtime.restart_files_watcher_if_needed_for_runtime(sys.modules[__name__])


def _files_watcher_reconcile_attempt(reason: str) -> bool:
    return _files_watcher_runtime.files_watcher_reconcile_attempt_for_runtime(sys.modules[__name__], reason)


def _files_watcher_manager_loop() -> None:
    return _files_watcher_runtime.files_watcher_manager_loop_for_runtime(sys.modules[__name__])


def _start_files_watcher_manager_if_needed() -> None:
    return _files_watcher_runtime.start_files_watcher_manager_if_needed_for_runtime(sys.modules[__name__])


def _stop_files_watcher_manager() -> None:
    return _files_watcher_runtime.stop_files_watcher_manager_for_runtime(sys.modules[__name__])


def _request_files_watcher_reconcile(reason: str, *, force: bool = False) -> None:
    return _files_watcher_runtime.request_files_watcher_reconcile_for_runtime(
        sys.modules[__name__],
        reason,
        force=force,
    )


_auto_changed_only_scan_lock = threading.Lock()
_auto_changed_only_scan_thread = None
_auto_changed_only_scan_last_started_ts = 0.0


def _start_auto_changed_only_scan_scheduler() -> None:
    """Start a background scheduler that can trigger changed-only scans from watcher events."""
    global _auto_changed_only_scan_thread
    if not _auto_changed_only_mode_effective():
        return
    if not _files_watcher_available():
        return
    with _auto_changed_only_scan_lock:
        if _auto_changed_only_scan_thread is not None and _auto_changed_only_scan_thread.is_alive():
            return
        t = threading.Thread(target=_auto_changed_only_scan_loop, daemon=True, name="auto-changed-only-scan")
        _auto_changed_only_scan_thread = t
        t.start()


def _auto_changed_only_scan_loop() -> None:
    """Debounce/coalesce filesystem events into an efficient changed-only scan."""
    global _auto_changed_only_scan_last_started_ts
    poll_sec = 10.0
    while True:
        try:
            time.sleep(poll_sec)
            if not _auto_changed_only_mode_effective():
                continue
            if bool(_pipeline_bootstrap_status().get("bootstrap_required")):
                continue
            # Do not trigger while a scan is running/finalizing.
            with lock:
                scanning_now = bool(state.get("scanning"))
                finalizing_now = bool(state.get("scan_finalizing"))
                starting_now = bool(state.get("scan_starting"))
                fw_state = dict(state.get("files_watcher") or {})
            if scanning_now or finalizing_now or starting_now:
                continue

            # Require some pending changes.
            try:
                con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
                cur = con.cursor()
                cur.execute("SELECT COUNT(*) FROM files_pending_changes")
                pending = int((cur.fetchone() or [0])[0] or 0)
                cur.execute("SELECT MAX(last_seen) FROM files_pending_changes")
                last_seen_db = float((cur.fetchone() or [0])[0] or 0.0)
                con.close()
            except Exception:
                pending = 0
                last_seen_db = 0.0
            if pending < max(1, int(PMDA_AUTO_CHANGED_ONLY_SCAN_MIN_PENDING or 1)):
                continue

            now = time.time()
            last_event_at = float(fw_state.get("last_event_at") or 0.0)
            last_event_at = max(last_event_at, last_seen_db or 0.0)
            debounce = max(5.0, float(PMDA_AUTO_CHANGED_ONLY_SCAN_DEBOUNCE_SEC or 60.0))
            if last_event_at and (now - last_event_at) < debounce:
                continue

            cooldown = max(30.0, float(PMDA_AUTO_CHANGED_ONLY_SCAN_COOLDOWN_SEC or 300.0))
            if (now - float(_auto_changed_only_scan_last_started_ts or 0.0)) < cooldown:
                continue

            # Trigger scan.
            with lock:
                if bool(state.get("scanning")) or bool(state.get("scan_finalizing")) or bool(state.get("scan_starting")):
                    continue
            ok, _meta = _try_begin_scan(
                scan_type="changed_only",
                source="files_watcher",
                run_improve_after=False,
                scheduler_run_id=None,
            )
            if not ok:
                continue
            _auto_changed_only_scan_last_started_ts = now
            logging.info("AUTO scan: starting changed-only scan (pending=%d)", pending)
        except Exception:
            logging.debug("AUTO scan loop error", exc_info=True)


def _tokenize_reco_text(raw: str) -> list[str]:
    txt = (raw or "").strip().lower()
    if not txt:
        return []
    tokens = re.findall(r"[a-z0-9]+", txt)
    out = [t for t in tokens if len(t) >= 2]
    # Add bigrams for a light semantic boost on short names.
    for i in range(max(0, len(out) - 1)):
        out.append(f"{out[i]}_{out[i+1]}")
    return out


def _build_hashed_embedding(text: str, dims: int = RECO_EMBED_DIM) -> tuple[list[float], float]:
    vec = [0.0] * max(8, int(dims or RECO_EMBED_DIM))
    tokens = _tokenize_reco_text(text)
    if not tokens:
        return vec, 0.0
    for tok in tokens:
        digest = hashlib.sha1(tok.encode("utf-8", errors="ignore")).digest()
        idx = int.from_bytes(digest[:2], "big") % len(vec)
        sign = 1.0 if (digest[2] & 1) == 0 else -1.0
        # Slightly higher weight for longer tokens, capped.
        weight = min(2.5, 1.0 + (len(tok) / 12.0))
        vec[idx] += sign * weight
    norm = math.sqrt(sum(v * v for v in vec))
    if norm > 0.0:
        inv = 1.0 / norm
        vec = [v * inv for v in vec]
    return vec, norm


def _load_embedding_json(raw: str) -> list[float]:
    if not raw:
        return []
    try:
        arr = json.loads(raw)
    except Exception:
        return []
    if not isinstance(arr, list):
        return []
    out: list[float] = []
    for v in arr[:RECO_EMBED_DIM]:
        try:
            out.append(float(v))
        except Exception:
            out.append(0.0)
    if len(out) < RECO_EMBED_DIM:
        out.extend([0.0] * (RECO_EMBED_DIM - len(out)))
    return out


def _vec_cosine(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    n = min(len(a), len(b))
    if n <= 0:
        return 0.0
    return float(sum((a[i] * b[i]) for i in range(n)))


def _reco_event_weight(*args, **kwargs):
    return _recommendation_runtime._reco_event_weight_for_runtime(sys.modules[__name__], *args, **kwargs)

def _reco_build_track_embeddings(*args, **kwargs):
    return _recommendation_runtime._reco_build_track_embeddings_for_runtime(sys.modules[__name__], *args, **kwargs)

def _reco_build_track_embeddings_chunked(*args, **kwargs):
    return _recommendation_runtime._reco_build_track_embeddings_chunked_for_runtime(sys.modules[__name__], *args, **kwargs)

def _reco_upsert_track_embeddings_for_album_ids(*args, **kwargs):
    return _recommendation_runtime._reco_upsert_track_embeddings_for_album_ids_for_runtime(sys.modules[__name__], *args, **kwargs)

def _entity_discover_ai_summary(*args, **kwargs):
    return _recommendation_runtime._entity_discover_ai_summary_for_runtime(sys.modules[__name__], *args, **kwargs)

def _reco_genre_tokens(*args, **kwargs):
    return _recommendation_runtime._reco_genre_tokens_for_runtime(sys.modules[__name__], *args, **kwargs)

def _reco_fetch_embeddings_map(*args, **kwargs):
    return _recommendation_runtime._reco_fetch_embeddings_map_for_runtime(sys.modules[__name__], *args, **kwargs)

def _reco_build_session_profile(*args, **kwargs):
    return _recommendation_runtime._reco_build_session_profile_for_runtime(sys.modules[__name__], *args, **kwargs)

def _reco_fetch_candidates(*args, **kwargs):
    return _recommendation_runtime._reco_fetch_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)

def _reco_rank_candidates(*args, **kwargs):
    return _recommendation_runtime._reco_rank_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)

def _reco_record_event(*args, **kwargs):
    return _recommendation_runtime._reco_record_event_for_runtime(sys.modules[__name__], *args, **kwargs)





def _files_reco_embeddings_set_state(**updates) -> None:
    with lock:
        st = dict(state.get("files_reco_embeddings") or {})
        updates.setdefault("updated_at", time.time())
        st.update(updates)
        state["files_reco_embeddings"] = st






def _files_index_set_state(**updates) -> None:
    with lock:
        state["files_index"] = _library_index_core.merge_index_state(state.get("files_index"), updates)


def _files_index_get_state() -> dict:
    with lock:
        return dict(state.get("files_index") or {})


def _files_index_is_running(*, phases: set[str] | None = None) -> bool:
    return _library_index_core.index_is_running(_files_index_get_state(), phases=phases)


def _files_index_progress_metrics(
    processed: Any,
    total: Any,
    *,
    started_at: Any = None,
) -> tuple[Optional[float], Optional[int], Optional[float]]:
    return _library_index_core.progress_metrics(processed, total, started_at=started_at)


_MEDIA_CACHE_SIZES = (96, 192, 320, 512, 640)
_MEDIA_CACHE_MASTER_PX = 1600
_FILES_BLOCKING_MEDIA_PREP_MAX_ALBUMS = max(
    0,
    int(_parse_int(os.getenv("PMDA_FILES_BLOCKING_MEDIA_PREP_MAX_ALBUMS", "50000"), 50000) or 50000),
)
_FILES_BLOCKING_RECO_EMBED_MAX_TRACKS = max(
    0,
    int(_parse_int(os.getenv("PMDA_FILES_BLOCKING_RECO_EMBED_MAX_TRACKS", "0"), 0) or 0),
)
_ARTWORK_RAM_CACHE_MAX_BYTES = int(max(0, ARTWORK_RAM_CACHE_MB) * 1024 * 1024)
_ARTWORK_RAM_CACHE_MAX_ITEM_BYTES = int(max(1, ARTWORK_RAM_CACHE_MAX_ITEM_MB) * 1024 * 1024)
_ARTWORK_RAM_CACHE: "OrderedDict[str, dict]" = OrderedDict()
_ARTWORK_RAM_CACHE_BYTES = 0
_ARTWORK_RAM_CACHE_LOCK = threading.Lock()
_ARTWORK_RAM_CACHE_AUTO_THREAD: Optional[threading.Thread] = None
_ARTWORK_RAM_CACHE_AUTO_LOCK = threading.Lock()
_ARTWORK_RAM_CACHE_AUTO_LAST_APPLIED_MB: Optional[int] = None
_TRANSPARENT_PNG_1PX = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGNgYAAAAAMAASsJTYQAAAAASUVORK5CYII="
)


def _reconfigure_artwork_ram_cache(*, cache_mb: Optional[int] = None, ttl_sec: Optional[int] = None, item_mb: Optional[int] = None) -> None:
    """Apply artwork RAM cache settings live and trim existing cache to new limits."""
    global ARTWORK_RAM_CACHE_MB, ARTWORK_RAM_CACHE_TTL_SEC, ARTWORK_RAM_CACHE_MAX_ITEM_MB
    global _ARTWORK_RAM_CACHE_MAX_BYTES, _ARTWORK_RAM_CACHE_MAX_ITEM_BYTES, _ARTWORK_RAM_CACHE_BYTES
    if cache_mb is not None:
        ARTWORK_RAM_CACHE_MB = int(max(0, min(65536, int(cache_mb))))
    if ttl_sec is not None:
        ARTWORK_RAM_CACHE_TTL_SEC = int(max(60, min(60 * 60 * 24 * 30, int(ttl_sec))))
    if item_mb is not None:
        ARTWORK_RAM_CACHE_MAX_ITEM_MB = int(max(1, min(64, int(item_mb))))

    _ARTWORK_RAM_CACHE_MAX_BYTES = int(max(0, ARTWORK_RAM_CACHE_MB) * 1024 * 1024)
    _ARTWORK_RAM_CACHE_MAX_ITEM_BYTES = int(max(1, ARTWORK_RAM_CACHE_MAX_ITEM_MB) * 1024 * 1024)

    with _ARTWORK_RAM_CACHE_LOCK:
        if _ARTWORK_RAM_CACHE_MAX_BYTES <= 0:
            _ARTWORK_RAM_CACHE.clear()
            _ARTWORK_RAM_CACHE_BYTES = 0
            return
        for key in list(_ARTWORK_RAM_CACHE.keys()):
            entry = _ARTWORK_RAM_CACHE.get(key) or {}
            if int(entry.get("blob_size") or 0) > _ARTWORK_RAM_CACHE_MAX_ITEM_BYTES:
                old = _ARTWORK_RAM_CACHE.pop(key, None)
                if old:
                    _ARTWORK_RAM_CACHE_BYTES = max(0, int(_ARTWORK_RAM_CACHE_BYTES) - int(old.get("blob_size") or 0))
        while _ARTWORK_RAM_CACHE and int(_ARTWORK_RAM_CACHE_BYTES) > _ARTWORK_RAM_CACHE_MAX_BYTES:
            _, old = _ARTWORK_RAM_CACHE.popitem(last=False)
            _ARTWORK_RAM_CACHE_BYTES = max(0, int(_ARTWORK_RAM_CACHE_BYTES) - int(old.get("blob_size") or 0))


def _compute_auto_artwork_ram_target_mb() -> int:
    """
    Compute a dynamic RAM cache budget from currently available memory.
    Uses ~75% of available bytes (cgroup-aware), optionally capped by user setting.
    """
    available_bytes = _effective_available_memory_bytes()
    if available_bytes <= 0:
        target_mb = int(max(0, ARTWORK_RAM_CACHE_MB))
    else:
        target_mb = int((float(available_bytes) * 0.75) / (1024.0 * 1024.0))
        target_mb = max(256, target_mb)
    implicit_cap_mb = _implicit_auto_artwork_ram_cap_mb()
    if implicit_cap_mb > 0:
        target_mb = min(target_mb, int(implicit_cap_mb))
    return int(max(0, min(65536, target_mb)))


def _apply_auto_artwork_ram_target(*, force: bool = False) -> int:
    """
    Apply auto RAM target when enabled.
    Returns the applied/active cache size in MB.
    """
    global _ARTWORK_RAM_CACHE_AUTO_LAST_APPLIED_MB
    if not bool(ARTWORK_RAM_CACHE_AUTO):
        return int(max(0, ARTWORK_RAM_CACHE_MB))
    target_mb = _compute_auto_artwork_ram_target_mb()
    current_mb = int(max(0, ARTWORK_RAM_CACHE_MB))
    delta = abs(target_mb - current_mb)
    threshold = max(256, int(current_mb * 0.15))
    if (not force) and delta < threshold:
        return current_mb
    _reconfigure_artwork_ram_cache(cache_mb=target_mb)
    _ARTWORK_RAM_CACHE_AUTO_LAST_APPLIED_MB = target_mb
    return target_mb


def _artwork_ram_cache_auto_worker() -> None:
    while True:
        try:
            if bool(ARTWORK_RAM_CACHE_AUTO):
                _apply_auto_artwork_ram_target()
        except Exception:
            logging.debug("Artwork RAM auto-tune iteration failed", exc_info=True)
        # Read dynamic interval every loop so settings apply live.
        sleep_sec = int(max(30, ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC or 120))
        time.sleep(sleep_sec)


def _start_artwork_ram_cache_auto_worker() -> None:
    global _ARTWORK_RAM_CACHE_AUTO_THREAD
    with _ARTWORK_RAM_CACHE_AUTO_LOCK:
        if _ARTWORK_RAM_CACHE_AUTO_THREAD and _ARTWORK_RAM_CACHE_AUTO_THREAD.is_alive():
            return
        _ARTWORK_RAM_CACHE_AUTO_THREAD = threading.Thread(
            target=_artwork_ram_cache_auto_worker,
            daemon=True,
            name="artwork-ram-auto",
        )
        _ARTWORK_RAM_CACHE_AUTO_THREAD.start()


def _media_cache_root_dir(*args, **kwargs):
    return _media_cache_runtime._media_cache_root_dir_for_runtime(sys.modules[__name__], *args, **kwargs)



def _path_is_within(*args, **kwargs):
    return _media_cache_runtime._path_is_within_for_runtime(sys.modules[__name__], *args, **kwargs)



def _is_media_cache_file(*args, **kwargs):
    return _media_cache_runtime._is_media_cache_file_for_runtime(sys.modules[__name__], *args, **kwargs)



def _mime_from_path(*args, **kwargs):
    return _media_cache_runtime._mime_from_path_for_runtime(sys.modules[__name__], *args, **kwargs)



def _artwork_etag_for_stat(*args, **kwargs):
    return _media_cache_runtime._artwork_etag_for_stat_for_runtime(sys.modules[__name__], *args, **kwargs)



def _artwork_cache_control(*args, **kwargs):
    return _media_cache_runtime._artwork_cache_control_for_runtime(sys.modules[__name__], *args, **kwargs)



def _artwork_ram_cache_get(*args, **kwargs):
    return _media_cache_runtime._artwork_ram_cache_get_for_runtime(sys.modules[__name__], *args, **kwargs)



def _artwork_ram_cache_put(*args, **kwargs):
    return _media_cache_runtime._artwork_ram_cache_put_for_runtime(sys.modules[__name__], *args, **kwargs)



def _artwork_ram_cache_prime(*args, **kwargs):
    return _media_cache_runtime._artwork_ram_cache_prime_for_runtime(sys.modules[__name__], *args, **kwargs)



def _serve_image_file_cached(*args, **kwargs):
    return _media_cache_runtime._serve_image_file_cached_for_runtime(sys.modules[__name__], *args, **kwargs)



def _transparent_png_response(*args, **kwargs):
    return _media_cache_runtime._transparent_png_response_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ensure_media_cache_dirs(*args, **kwargs):
    return _media_cache_runtime._ensure_media_cache_dirs_for_runtime(sys.modules[__name__], *args, **kwargs)



def _image_ext_from_mime(*args, **kwargs):
    return _media_cache_runtime._image_ext_from_mime_for_runtime(sys.modules[__name__], *args, **kwargs)



def _media_cache_key_for_path(*args, **kwargs):
    return _media_cache_runtime._media_cache_key_for_path_for_runtime(sys.modules[__name__], *args, **kwargs)



def _media_cache_path_for_key(*args, **kwargs):
    return _media_cache_runtime._media_cache_path_for_key_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ensure_cached_image_for_path(*args, **kwargs):
    return _media_cache_runtime._ensure_cached_image_for_path_for_runtime(sys.modules[__name__], *args, **kwargs)



def _cached_image_for_path_if_exists(*args, **kwargs):
    return _media_cache_runtime._cached_image_for_path_if_exists_for_runtime(sys.modules[__name__], *args, **kwargs)



def _ensure_cached_image_from_bytes(*args, **kwargs):
    return _media_cache_runtime._ensure_cached_image_from_bytes_for_runtime(sys.modules[__name__], *args, **kwargs)



def _existing_file_path(*args, **kwargs):
    return _media_cache_runtime._existing_file_path_for_runtime(sys.modules[__name__], *args, **kwargs)



def _promote_files_media_paths_to_cache(*args, **kwargs):
    return _media_cache_runtime._promote_files_media_paths_to_cache_for_runtime(sys.modules[__name__], *args, **kwargs)



def _precache_files_media_assets(*args, **kwargs):
    return _media_cache_runtime._precache_files_media_assets_for_runtime(sys.modules[__name__], *args, **kwargs)



def _album_artwork_gallery_cache_key(*args, **kwargs):
    return _album_media_runtime._album_artwork_gallery_cache_key_for_runtime(sys.modules[__name__], *args, **kwargs)

def _cover_art_archive_gallery_items(*args, **kwargs):
    return _album_media_runtime._cover_art_archive_gallery_items_for_runtime(sys.modules[__name__], *args, **kwargs)

def _discogs_gallery_items(*args, **kwargs):
    return _album_media_runtime._discogs_gallery_items_for_runtime(sys.modules[__name__], *args, **kwargs)

def _album_artwork_gallery_manifest(*args, **kwargs):
    return _album_media_runtime._album_artwork_gallery_manifest_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_library_album_artwork_gallery(*args, **kwargs):
    return _album_media_runtime.api_library_album_artwork_gallery_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_library_files_album_artwork_item(*args, **kwargs):
    return _album_media_runtime.api_library_files_album_artwork_item_for_runtime(sys.modules[__name__], *args, **kwargs)









def _parse_int_loose(value, default: int = 0) -> int:
    return _number_parsing.parse_int_loose(value, default)


_PG_INT4_MIN = -(2**31)
_PG_INT4_MAX = (2**31) - 1
_PG_INT8_MIN = -(2**63)
_PG_INT8_MAX = (2**63) - 1


def _clamp_int(value, default: int = 0, min_value: int | None = None, max_value: int | None = None) -> int:
    return _number_parsing.clamp_int(value, default, min_value, max_value)


def _parse_float_loose(value, default: float = 0.0) -> float:
    return _number_parsing.parse_float_loose(value, default)


def _parse_duration_seconds_loose(value, default: float = 0.0) -> float:
    return _number_parsing.parse_duration_seconds_loose(value, default)


def _parse_disc_track_loose(tags: dict | None, fallback_disc: int = 1, fallback_track: int = 0) -> tuple[int, int]:
    return _number_parsing.parse_disc_track_loose(tags, fallback_disc, fallback_track)


def _normalize_meta_text(value) -> str:
    """Normalize loose metadata strings from tags (trim + collapse spaces)."""
    if value is None:
        return ""
    txt = str(value).strip()
    if not txt:
        return ""
    return re.sub(r"\s+", " ", txt)

def _first_scalar_meta_value(value):
    """Return the first non-empty scalar value from common tag shapes (list/tuple/scalar)."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        for item in value:
            v = _first_scalar_meta_value(item)
            if v is not None and str(v).strip():
                return v
        return None
    return value


def _pick_weighted_metadata_value(candidates: list[tuple[object, int]]) -> str:
    """
    Pick the most representative value from weighted metadata candidates.
    Higher score wins; ties keep first-seen value.
    """
    scores: dict[str, int] = {}
    first_seen: dict[str, int] = {}
    display: dict[str, str] = {}
    pos = 0
    for raw_value, weight in candidates:
        value = _normalize_meta_text(raw_value)
        if not value:
            continue
        key = value.casefold()
        scores[key] = scores.get(key, 0) + max(1, int(weight))
        if key not in first_seen:
            first_seen[key] = pos
            display[key] = value
        pos += 1
    if not scores:
        return ""
    best_key = min(scores.keys(), key=lambda k: (-scores[k], first_seen[k]))
    return display.get(best_key, "")


def _pick_album_artist_from_tag_dicts(tag_dicts: list[dict], default: str = "Unknown Artist") -> str:
    """Choose album-level artist from tags, preferring albumartist over per-track artist."""
    candidates: list[tuple[object, int]] = []
    for tags in tag_dicts or []:
        if not isinstance(tags, dict):
            continue
        candidates.append((tags.get("albumartist"), 4))
        candidates.append((tags.get("artist"), 1))
    picked = _pick_weighted_metadata_value(candidates)
    return (
        _identity_artist_fallback_candidate(picked)
        or _identity_artist_fallback_candidate(default)
        or picked
        or default
    )


def _pick_album_title_from_tag_dicts(tag_dicts: list[dict], fallback: str = "Unknown Album") -> str:
    """Choose album title from tags (consensus), with fallback when missing."""
    candidates: list[tuple[object, int]] = []
    for tags in tag_dicts or []:
        if not isinstance(tags, dict):
            continue
        candidates.append((tags.get("album"), 4))
        candidates.append((tags.get("release"), 1))
    picked = _pick_weighted_metadata_value(candidates)
    return picked or (_normalize_meta_text(fallback) or "Unknown Album")

def _pick_album_label_from_tag_dicts(tag_dicts: list[dict]) -> str:
    """Pick record label/publisher from tags when available."""
    candidates: list[tuple[object, int]] = []
    keys = [
        ("label", 4),
        ("recordlabel", 3),
        ("record_label", 3),
        ("record label", 3),
        ("publisher", 2),
        ("organization", 1),
    ]
    for tags in tag_dicts or []:
        if not isinstance(tags, dict):
            continue
        for k, w in keys:
            candidates.append((_first_scalar_meta_value(tags.get(k)), w))
    return _pick_weighted_metadata_value(candidates)


def _split_genre_values(raw_value: str) -> list[str]:
    return _genre_runtime._split_genre_values_for_runtime(sys.modules[__name__], raw_value)


def _representative_album_audio_profile(tracks: list[dict[str, Any]] | None) -> tuple[int, int]:
    """
    Pick a representative album audio profile from track-level metadata.

    Preference order:
    1. most common (bit_depth, sample_rate) pair
    2. highest bit depth
    3. highest sample rate
    """
    pairs: dict[tuple[int, int], int] = {}
    for track in tracks or []:
        if not isinstance(track, dict):
            continue
        sample_rate = max(0, _parse_int_loose(track.get("sample_rate"), 0))
        bit_depth = max(0, _parse_int_loose(track.get("bit_depth"), 0))
        if sample_rate <= 0 and bit_depth <= 0:
            continue
        key = (int(bit_depth), int(sample_rate))
        pairs[key] = pairs.get(key, 0) + 1
    if not pairs:
        return 0, 0
    chosen = max(
        pairs.items(),
        key=lambda item: (
            int(item[1] or 0),
            int(item[0][0] or 0),
            int(item[0][1] or 0),
        ),
    )[0]
    return int(chosen[1] or 0), int(chosen[0] or 0)


def _merge_album_genre_lists(*values: Any) -> list[str]:
    return _genre_runtime._merge_album_genre_lists_for_runtime(sys.modules[__name__], *values)


def _first_artist_image_path(artist_folder: Path) -> Optional[Path]:
    if not artist_folder or not artist_folder.is_dir():
        return None
    try:
        for name in _ARTIST_IMAGE_NAMES:
            p = artist_folder / name
            if p.is_file():
                return p
    except OSError:
        return None
    return None


def _files_root_dir_strings() -> set[str]:
    """
    Return normalized string variants for FILES_ROOTS so we can reliably detect
    when we are at a library root while walking parent folders.
    """
    out: set[str] = set()
    for r in (_effective_files_roots(enabled_only=True) or FILES_ROOTS or []):
        rs = str(r or "").strip()
        if not rs:
            continue
        try:
            p = path_for_fs_access(Path(rs))
        except Exception:
            p = Path(rs)
        try:
            out.add(str(p.resolve()))
        except Exception:
            pass
        out.add(str(p))
    return out


def _files_is_files_root_dir(p: Path, *, root_dirs: Optional[set[str]] = None) -> bool:
    if not p:
        return False
    roots = root_dirs if isinstance(root_dirs, set) else _files_root_dir_strings()
    try:
        if str(p.resolve()) in roots:
            return True
    except Exception:
        pass
    return str(p) in roots


def _files_guess_artist_folder(
    album_folder: Path,
    artist_name: str,
    *,
    root_dirs: Optional[set[str]] = None,
    max_up: int = 6,
) -> Optional[Path]:
    """
    Guess the "artist folder" for an album folder in Files mode.

    Why: in flat libraries where albums are directly under FILES_ROOTS, blindly using
    `folder.parent` makes the FILES_ROOT look like the artist folder, causing a single
    `/root/artist.jpg` to be incorrectly assigned to every artist.

    Strategy: walk up from `album_folder` and return the first ancestor directory whose
    name matches the artist name (strict normalization). Never return FILES_ROOT itself.
    """
    if not album_folder:
        return None
    artist_disp = str(artist_name or "").strip()
    if not artist_disp:
        return None
    artist_norm = _normalize_identity_text_strict(artist_disp)
    if not artist_norm:
        return None
    artist_tokens = [t for t in artist_norm.split() if t]
    roots = root_dirs if isinstance(root_dirs, set) else _files_root_dir_strings()

    try:
        cur = path_for_fs_access(Path(album_folder))
    except Exception:
        cur = Path(album_folder)

    try:
        if not cur.is_dir():
            return None
    except Exception:
        return None

    # Walk from the album folder up to a few levels, stopping at FILES_ROOTS.
    for _ in range(max(0, int(max_up)) + 1):
        if _files_is_files_root_dir(cur, root_dirs=roots):
            break
        cand_norm = _normalize_identity_text_strict(cur.name)
        if cand_norm:
            if cand_norm == artist_norm:
                return cur
            # Tolerate simple token reordering (e.g. "Beatles, The" vs "The Beatles").
            cand_tokens = [t for t in cand_norm.split() if t]
            if len(cand_tokens) > 1 and len(artist_tokens) > 1 and sorted(cand_tokens) == sorted(artist_tokens):
                return cur
        parent = cur.parent
        if not parent or parent == cur:
            break
        cur = parent
    return None


def _state_browse_readonly_connect(timeout_sec: float = 0.35) -> sqlite3.Connection:
    """
    Short-timeout state.db reader for browse request paths.

    Library pages must never sit behind scan publication writes. The normal
    state DB helpers intentionally tolerate long waits for durable writers; this
    one is deliberately impatient so HTTP routes can fall back to cache/snapshot
    instead of timing out in the browser.
    """
    timeout = max(0.05, min(2.0, float(timeout_sec or 0.35)))
    try:
        con = sqlite3.connect(f"file:{STATE_DB_FILE}?mode=ro", timeout=timeout, uri=True)
    except Exception:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=timeout)
    try:
        con.execute(f"PRAGMA busy_timeout={int(timeout * 1000)};")
    except Exception:
        pass
    return con


def _files_index_read_counts() -> tuple[int, int, int]:
    if not _files_pg_init_schema():
        return (0, 0, 0)
    conn = _files_pg_connect()
    if conn is None:
        return (0, 0, 0)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM files_artists")
            artists = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT COUNT(*) FROM files_albums")
            albums = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT COUNT(*) FROM files_tracks")
            tracks = int(cur.fetchone()[0] or 0)
            return (artists, albums, tracks)
    except Exception:
        return (0, 0, 0)
    finally:
        conn.close()


def _files_index_read_counts_fast(acquire_timeout_sec: float = 0.20) -> tuple[int, int, int]:
    """
    Best-effort browse counts for request paths.

    The normal count helper is allowed to wait for PostgreSQL because rebuild
    jobs need exact values. Library browse endpoints are different: they must
    return a snapshot quickly and never sit behind a long rebuild/checkpoint.
    """
    if not bool(_FILES_PG_SCHEMA_READY):
        return (0, 0, 0)
    conn = _files_pg_connect(acquire_timeout_sec=max(0.05, float(acquire_timeout_sec or 0.20)))
    if conn is None:
        return (0, 0, 0)
    try:
        timeout_ms = int(max(80, min(700, round(float(acquire_timeout_sec or 0.20) * 1000.0))))
        with conn.cursor() as cur:
            try:
                with _files_pg_statement_timeout(cur, timeout_ms):
                    cur.execute(
                        """
                        SELECT
                            COALESCE(MAX(value) FILTER (WHERE key = 'artists'), '0'),
                            COALESCE(MAX(value) FILTER (WHERE key = 'albums'), '0'),
                            COALESCE(MAX(value) FILTER (WHERE key = 'tracks'), '0')
                        FROM files_index_meta
                        WHERE key IN ('artists', 'albums', 'tracks')
                        """
                    )
                    meta_row = cur.fetchone() or ("0", "0", "0")
                    artists = int(str(meta_row[0] or "0") or 0)
                    albums = int(str(meta_row[1] or "0") or 0)
                    tracks = int(str(meta_row[2] or "0") or 0)
                    if artists > 0 or albums > 0 or tracks > 0:
                        return (artists, albums, tracks)
            except Exception:
                pass
            try:
                with _files_pg_statement_timeout(cur, timeout_ms):
                    cur.execute(
                        """
                        SELECT
                            (SELECT COUNT(*)::BIGINT FROM files_artists),
                            (SELECT COUNT(*)::BIGINT FROM files_albums),
                            (SELECT COUNT(*)::BIGINT FROM files_tracks)
                        """
                    )
                    row = cur.fetchone() or (0, 0, 0)
                    return (int(row[0] or 0), int(row[1] or 0), int(row[2] or 0))
            except Exception:
                return (0, 0, 0)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _files_library_published_row_count(*args: Any, **kwargs: Any) -> Any:
    return _published_browse_runtime._files_library_published_row_count_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_index_read_track_and_embedding_counts() -> tuple[int, int]:
    if not _files_pg_init_schema():
        return (0, 0)
    conn = _files_pg_connect()
    if conn is None:
        return (0, 0)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM files_tracks")
            tracks = int((cur.fetchone() or [0])[0] or 0)
            cur.execute("SELECT COUNT(*) FROM files_track_embeddings")
            embeddings = int((cur.fetchone() or [0])[0] or 0)
            return (tracks, embeddings)
    except Exception:
        return (0, 0)
    finally:
        conn.close()


def _files_index_write_meta(cur, key: str, value: str) -> None:
    cur.execute(
        """
        INSERT INTO files_index_meta(key, value, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """,
        (key, value),
    )


def _files_index_read_meta_value(key: str, default: str = "") -> str:
    if not _files_pg_init_schema():
        return default
    conn = _files_pg_connect()
    if conn is None:
        return default
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(value, '') FROM files_index_meta WHERE key = %s LIMIT 1", (key,))
            row = cur.fetchone()
            if not row:
                return default
            return str(row[0] or default)
    except Exception:
        return default
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _files_index_sync_live_counts_meta(
    *,
    reason: str,
    source: str,
    counts: Optional[tuple[int, int, int]] = None,
) -> bool:
    """
    Keep files_index_meta aligned with the actual PostgreSQL browse index.

    The UI reads live table counts, but operational/debug surfaces also expose
    files_index_meta. Granular artist upserts intentionally do not rebuild the
    whole index, so stale meta counts can otherwise make a completed library
    look like an old rebuild is still authoritative.
    """
    if not _files_pg_init_schema():
        return False
    conn = _files_pg_connect()
    if conn is None:
        return False
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                if counts is None:
                    cur.execute("SELECT COUNT(*) FROM files_artists")
                    artists = int((cur.fetchone() or [0])[0] or 0)
                    cur.execute("SELECT COUNT(*) FROM files_albums")
                    albums = int((cur.fetchone() or [0])[0] or 0)
                    cur.execute("SELECT COUNT(*) FROM files_tracks")
                    tracks = int((cur.fetchone() or [0])[0] or 0)
                else:
                    artists, albums, tracks = (max(0, int(v or 0)) for v in counts)
                _files_index_write_meta(cur, "last_reason", str(reason or "live_counts_sync"))
                _files_index_write_meta(cur, "last_build_ts", str(int(time.time())))
                _files_index_write_meta(cur, "artists", str(artists))
                _files_index_write_meta(cur, "albums", str(albums))
                _files_index_write_meta(cur, "tracks", str(tracks))
                _files_index_write_meta(cur, "source", str(source or "existing_pg_index"))
        return True
    except Exception:
        logging.debug("Failed to sync Files index live counts meta", exc_info=True)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _rebuild_files_library_index_for_artist(
    artist_hint: str,
    *,
    reason: str = "manual_artist_upsert",
    wait_if_running: bool = False,
) -> dict:
    return _index_rebuild_runtime.rebuild_files_library_index_for_artist_for_runtime(
        sys.modules[__name__],
        artist_hint,
        reason=reason,
        wait_if_running=wait_if_running,
    )

def _files_reset_rebuild_tables(cur) -> None:
    """
    Clear Files index tables without TRUNCATE.
    TRUNCATE holds AccessExclusiveLock and can block every reader behind a long-running rebuild.
    Ordered DELETE keeps readers usable while the rebuild transaction is open.
    """
    cur.execute("DELETE FROM files_tracks")
    cur.execute("DELETE FROM files_artist_album_links")
    cur.execute("DELETE FROM files_artist_aliases")
    cur.execute("DELETE FROM files_albums")
    cur.execute("DELETE FROM files_artists")


def _files_index_payload_source_decision(
    reason: str,
    *,
    published_count: int,
    files_scan_running: bool,
) -> tuple[bool, str, bool, str]:
    reason_norm = str(reason or "manual").strip().lower()
    force_filesystem_source = reason_norm in {
        "artist_identity_repair",
        "artist_identity_repair_full",
        "filesystem_rebuild",
        "api_rebuild_filesystem",
        "manual_filesystem_rebuild",
        "settings_files_roots",
    }
    force_reason = "explicit" if force_filesystem_source else ""

    if force_filesystem_source:
        try:
            workflow = _library_workflow_state()
            workflow_mode = _normalize_library_workflow_mode(workflow.get("mode"), default="managed")
            trusted_roots = _normalize_root_path_list(
                [workflow.get("serving_root"), *(workflow.get("source_roots") or [])]
            )
            if workflow_mode == "mirror" and trusted_roots:
                force_filesystem_source = True
                force_reason = "mirror_trusted_library_roots"
        except Exception:
            pass

    use_published_payload = bool(not force_filesystem_source and (published_count > 0 or files_scan_running))
    payload_source = "published_rows" if use_published_payload else "filesystem_roots"
    return use_published_payload, payload_source, force_filesystem_source, force_reason


def _rebuild_files_library_index(reason: str = "manual", wait_if_running: bool = False) -> dict:
    return _index_rebuild_runtime.rebuild_files_library_index_for_runtime(
        sys.modules[__name__],
        reason=reason,
        wait_if_running=wait_if_running,
    )


def _trigger_files_index_rebuild_async(reason: str = "manual") -> bool:
    if files_index_lock.locked():
        return False
    embed_build_lock = globals().get("_FILES_RECO_EMBED_BUILD_LOCK")
    if embed_build_lock is not None and embed_build_lock.locked():
        return False
    reason_norm = str(reason or "manual").strip()
    if reason_norm.startswith("scan_live_sync_"):
        # Per-artist publication already triggers granular upserts; avoid redundant full rebuilds.
        return False

    artist_hint = None
    if reason_norm.startswith("scan_artist_ready_"):
        artist_hint = reason_norm[len("scan_artist_ready_") :].strip() or None

    def _runner():
        if artist_hint:
            _rebuild_files_library_index_for_artist(
                artist_hint,
                reason=reason_norm,
                wait_if_running=False,
            )
            return
        _rebuild_files_library_index(reason=reason_norm, wait_if_running=False)

    tname = "files-index-rebuild-artist" if artist_hint else "files-index-rebuild"
    threading.Thread(target=_runner, name=tname, daemon=True).start()
    return True


_FILES_INDEX_REBUILD_TRIGGER_LOCK = threading.Lock()
_FILES_INDEX_REBUILD_LAST_TRIGGER: dict[str, float] = {}


def _trigger_files_index_rebuild_async_throttled(reason: str, cooldown_sec: float = 30.0) -> bool:
    """
    Trigger an async files index rebuild with a small per-reason cooldown.
    This avoids endpoint polling storms from enqueueing repeated no-op rebuilds.
    """
    reason_norm = str(reason or "manual").strip() or "manual"
    now = time.time()
    cooldown = max(1.0, float(cooldown_sec or 0.0))
    with _FILES_INDEX_REBUILD_TRIGGER_LOCK:
        last_ts = float(_FILES_INDEX_REBUILD_LAST_TRIGGER.get(reason_norm, 0.0) or 0.0)
        if (now - last_ts) < cooldown:
            return False
        _FILES_INDEX_REBUILD_LAST_TRIGGER[reason_norm] = now
    started = _trigger_files_index_rebuild_async(reason=reason_norm)
    if not started:
        with _FILES_INDEX_REBUILD_TRIGGER_LOCK:
            # Allow a quick retry when a worker is already in-flight.
            _FILES_INDEX_REBUILD_LAST_TRIGGER[reason_norm] = max(0.0, now - (cooldown * 0.5))
    return started


_FILES_INDEX_PUBLISHED_CATCHUP_LOCK = threading.Lock()
_FILES_INDEX_PUBLISHED_CATCHUP_RUNNING = False


def _files_published_artist_names(*args: Any, **kwargs: Any) -> Any:
    return _published_browse_runtime._files_published_artist_names_for_runtime(sys.modules[__name__], *args, **kwargs)


def _enqueue_files_index_published_catchup(*args: Any, **kwargs: Any) -> Any:
    return _published_browse_runtime._enqueue_files_index_published_catchup_for_runtime(sys.modules[__name__], *args, **kwargs)


_FILES_RECO_EMBED_BUILD_LOCK = threading.Lock()


def _rebuild_files_reco_embeddings(reason: str = "manual", wait_if_running: bool = False) -> dict:
    return _recommendation_runtime._rebuild_files_reco_embeddings_for_runtime(
        sys.modules[__name__],
        reason=reason,
        wait_if_running=wait_if_running,
    )


_FILES_RECO_EMBED_BACKFILL_LOCK = threading.Lock()
_FILES_RECO_EMBED_BACKFILL_RUNNING = False


def _enqueue_files_reco_embedding_backfill(reason: str = "auto_backfill_missing_embeddings") -> bool:
    global _FILES_RECO_EMBED_BACKFILL_RUNNING
    if _FILES_RECO_EMBED_BUILD_LOCK.locked():
        return False
    with _FILES_RECO_EMBED_BACKFILL_LOCK:
        if _FILES_RECO_EMBED_BACKFILL_RUNNING:
            return False
        _FILES_RECO_EMBED_BACKFILL_RUNNING = True

    def _runner() -> None:
        global _FILES_RECO_EMBED_BACKFILL_RUNNING
        try:
            result = _rebuild_files_reco_embeddings(reason=reason, wait_if_running=True)
            if not bool(result.get("ok")):
                logging.warning("Files reco embedding auto-backfill failed: %s", result.get("error"))
        except Exception:
            logging.debug("Files reco embedding auto-backfill thread crashed", exc_info=True)
        finally:
            with _FILES_RECO_EMBED_BACKFILL_LOCK:
                _FILES_RECO_EMBED_BACKFILL_RUNNING = False

    threading.Thread(target=_runner, daemon=True, name="files-reco-embed-backfill").start()
    return True


def _files_index_needs_mirror_filesystem_rebuild(current_source: str | None = None) -> tuple[bool, str]:
    """
    Mirror workflows have two truths:
    - published scan rows are good for live scan updates;
    - the trusted serving/source roots are authoritative for the visible library.

    If the last full Files index was built from published rows, it can undercount
    an existing Music_matched library. Schedule one filesystem rebuild once no
    Files scan is active.
    """
    if _get_library_mode() != "files":
        return False, ""
    with lock:
        files_scan_running = bool(state.get("scanning")) and _get_library_mode() == "files"
    if files_scan_running:
        return False, "scan_running"
    try:
        workflow = _library_workflow_state()
        workflow_mode = _normalize_library_workflow_mode(workflow.get("mode"), default="managed")
        trusted_roots = _normalize_root_path_list(
            [workflow.get("serving_root"), *(workflow.get("source_roots") or [])]
        )
    except Exception:
        return False, "workflow_unavailable"
    if workflow_mode != "mirror" or not trusted_roots:
        return False, "not_mirror"
    source = str(current_source if current_source is not None else "").strip()
    if not source:
        source = _files_index_read_meta_value("full_source") or _files_index_read_meta_value("source")
    source_norm = source.strip().lower()
    if source_norm == "filesystem_roots":
        return False, source_norm
    return True, source_norm or "unknown"


def _ensure_files_index_ready() -> tuple[bool, Optional[str]]:
    return _index_status_runtime._ensure_files_index_ready_for_runtime(sys.modules[__name__])


def _files_backfill_trusted_match_flags() -> int:
    return _recommendation_runtime._files_backfill_trusted_match_flags_for_runtime(sys.modules[__name__])


def _files_backfill_artist_browse_entities_from_existing_index(*args: Any, **kwargs: Any) -> Any:
    return _artist_browse_runtime._files_backfill_artist_browse_entities_from_existing_index_for_runtime(sys.modules[__name__], *args, **kwargs)


_FILES_PROFILE_MAX_AGE_SEC = 30 * 24 * 3600
_files_profile_jobs_lock = threading.Lock()
_files_profile_jobs_active: set[str] = set()
_files_profile_jobs_last_ts: dict[str, float] = {}
_FILES_PROFILE_ENRICH_COOLDOWN_SEC = 30 * 60
_FILES_PROFILE_ENRICH_EMPTY_COOLDOWN_SEC = 6 * 60 * 60


def _dt_to_epoch(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._dt_to_epoch_for_runtime(sys.modules[__name__], *args, **kwargs)


def _is_profile_stale(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._is_profile_stale_for_runtime(sys.modules[__name__], *args, **kwargs)


def _profile_title_norm_variants(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._profile_title_norm_variants_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_get_artist_profile_cached(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_get_artist_profile_cached_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_get_album_profiles_cached(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_get_album_profiles_cached_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_upsert_artist_profile(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_upsert_artist_profile_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_upsert_album_profile(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_upsert_album_profile_for_runtime(sys.modules[__name__], *args, **kwargs)


def _album_profile_has_payload(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._album_profile_has_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


def _album_profile_has_text(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._album_profile_has_text_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_album_profile_fetch_allowed(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_album_profile_fetch_allowed_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_album_profile_fetch_strength(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_album_profile_fetch_strength_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_album_cover_refresh_allowed(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_album_cover_refresh_allowed_for_runtime(sys.modules[__name__], *args, **kwargs)


_FILES_EXTERNAL_ARTIST_IMAGE_MAX_AGE_SEC = 120 * 24 * 3600
_FILES_EXTERNAL_LABEL_IMAGE_MAX_AGE_SEC = 120 * 24 * 3600


def _norm_label_key(*args, **kwargs):
    return _external_image_cache_runtime._norm_label_key_for_runtime(sys.modules[__name__], *args, **kwargs)



def _is_external_artist_image_stale(*args, **kwargs):
    return _external_image_cache_runtime._is_external_artist_image_stale_for_runtime(sys.modules[__name__], *args, **kwargs)



def _is_external_label_image_stale(*args, **kwargs):
    return _external_image_cache_runtime._is_external_label_image_stale_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_get_external_label_images(*args, **kwargs):
    return _external_image_cache_runtime._files_get_external_label_images_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_upsert_external_label_image(*args, **kwargs):
    return _external_image_cache_runtime._files_upsert_external_label_image_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_cache_external_label_image(*args, **kwargs):
    return _external_image_cache_runtime._files_cache_external_label_image_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_prewarm_label_logo_from_bandcamp(*args, **kwargs):
    return _external_image_cache_runtime._files_prewarm_label_logo_from_bandcamp_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_get_external_artist_images(*args, **kwargs):
    return _external_image_cache_runtime._files_get_external_artist_images_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_upsert_external_artist_image(*args, **kwargs):
    return _external_image_cache_runtime._files_upsert_external_artist_image_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_resolve_artist_cache_name_norm(*args, **kwargs):
    return _external_image_cache_runtime._files_resolve_artist_cache_name_norm_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_clear_external_artist_image_cache(*args, **kwargs):
    return _external_image_cache_runtime._files_clear_external_artist_image_cache_for_runtime(sys.modules[__name__], *args, **kwargs)



def _artist_image_path_is_mirrored_media_cache(*args, **kwargs):
    return _external_image_cache_runtime._artist_image_path_is_mirrored_media_cache_for_runtime(sys.modules[__name__], *args, **kwargs)



def _artist_external_cached_image_is_valid_exact(*args, **kwargs):
    return _external_image_cache_runtime._artist_external_cached_image_is_valid_exact_for_runtime(sys.modules[__name__], *args, **kwargs)



def _artist_effective_image_present(*args, **kwargs):
    return _external_image_cache_runtime._artist_effective_image_present_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_reconcile_artist_image_cache_state(*args, **kwargs):
    return _external_image_cache_runtime._files_reconcile_artist_image_cache_state_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_purge_orphan_mirrored_artist_images(*args, **kwargs):
    return _external_image_cache_runtime._files_purge_orphan_mirrored_artist_images_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_artist_reference_folder(*args, **kwargs):
    return _external_image_cache_runtime._files_artist_reference_folder_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_cache_external_artist_image(*args, **kwargs):
    return _external_image_cache_runtime._files_cache_external_artist_image_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_attach_similar_artist_refs(*args, **kwargs):
    return _external_image_cache_runtime._files_attach_similar_artist_refs_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_promote_artist_alias_cache(*args, **kwargs):
    return _external_image_cache_runtime._files_promote_artist_alias_cache_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_refresh_artist_media_map_from_conn(*args, **kwargs):
    return _external_image_cache_runtime._files_refresh_artist_media_map_from_conn_for_runtime(sys.modules[__name__], *args, **kwargs)



def _files_refresh_artist_media_map_from_db(artists_map: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    if not artists_map:
        return {}
    conn = _files_pg_connect()
    if conn is None:
        return artists_map
    try:
        return _files_refresh_artist_media_map_from_conn(conn, artists_map)
    finally:
        conn.close()


def _files_resolve_artist_norm_map(*args: Any, **kwargs: Any) -> Any:
    return _artist_browse_runtime._files_resolve_artist_norm_map_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_remap_resolved_artist_norms(*args: Any, **kwargs: Any) -> Any:
    return _artist_browse_runtime._files_remap_resolved_artist_norms_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_apply_canonical_artist_resolution(*args: Any, **kwargs: Any) -> Any:
    return _artist_browse_runtime._files_apply_canonical_artist_resolution_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_build_local_artist_profile(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_build_local_artist_profile_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_ensure_local_artist_profile(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_ensure_local_artist_profile_for_runtime(sys.modules[__name__], *args, **kwargs)


_FILES_SIMILAR_IMAGES_WARM_ENABLED = True


def _fetch_discogs_artist_profile_info(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_discogs_artist_profile_info_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_bandcamp_artist_profile_hint(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_bandcamp_artist_profile_hint_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_musicbrainz_artist_profile_info(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_musicbrainz_artist_profile_info_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_pipeline_active() -> bool:
    try:
        with lock:
            return _scan_orchestrator_core.scan_pipeline_active_from_state(dict(state))
    except Exception:
        return False


def _scan_inline_matching_active() -> bool:
    try:
        with lock:
            return _scan_orchestrator_core.scan_inline_matching_active_from_state(dict(state))
    except Exception:
        return False


def _ai_scan_lifecycle_phase_active() -> bool:
    """
    Return True when the current AI usage context belongs to the scan lifecycle.
    This is broader than the inline scan hot path and also catches post-scan chain work.
    """
    try:
        phase = str((_ai_infer_runtime_context() or {}).get("phase") or "").strip().lower()
    except Exception:
        phase = ""
    return phase in {"scan", "post_scan"}


def _files_try_artist_image_refresh(*args: Any, **kwargs: Any) -> Any:
    return _profile_runtime.files_try_artist_image_refresh_for_runtime(sys.modules[__name__], **kwargs)


_FILES_SCAN_INLINE_ARTIST_ENRICH_BUDGET_SEC = 8.0
_FILES_SCAN_WEB_MBID_AI_TIMEOUT_SEC = 20.0
_FILES_SCAN_PROVIDER_IDENTITY_AI_TIMEOUT_SEC = 18.0


def _files_enrich_artists_blocking(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_enrich_artists_blocking_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_similar_artists_by_genre(conn, artist_id: int, *, limit: int = 20) -> list[dict]:
    return _recommendation_runtime._files_similar_artists_by_genre_for_runtime(
        sys.modules[__name__],
        conn,
        artist_id,
        limit=limit,
    )


def _run_files_profile_enrichment_job(
    *,
    job_key: str,
    artist_name: str,
    artist_norm: str,
    albums: list[tuple[str, str]],
    skip_album_profiles: bool = False,
    allow_soft_profiles: Optional[bool] = None,
    fast_mode: bool = False,
    cover_only: bool = False,
    priority_mode: str = "all",
) -> None:
    return _profile_runtime.run_files_profile_enrichment_job_for_runtime(
        sys.modules[__name__],
        job_key=job_key,
        artist_name=artist_name,
        artist_norm=artist_norm,
        albums=albums,
        skip_album_profiles=skip_album_profiles,
        allow_soft_profiles=allow_soft_profiles,
        fast_mode=fast_mode,
        cover_only=cover_only,
        priority_mode=priority_mode,
    )


def _enqueue_files_profile_enrichment(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._enqueue_files_profile_enrichment_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_image_asset_url(base_url: str, artist_id: int | str, *, size: int = 320, version: int | None = None) -> str:
    base = str(base_url or "").rstrip("/")
    aid = int(artist_id or 0)
    if aid <= 0:
        return ""
    suffix = f"&v={int(version or 0)}" if int(version or 0) > 0 else ""
    return f"{base}/api/library/files/artist/{aid}/image?size={int(size)}{suffix}"


def _label_logo_asset_url(base_url: str, label_name: str, *, size: int = 256) -> str:
    base = str(base_url or "").rstrip("/")
    label_norm = _norm_label_key(label_name)
    if not base or not label_norm:
        return ""
    return f"{base}/api/library/external/label-image/{quote(label_norm, safe='')}?size={int(size)}"


def _files_profile_job_is_active(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_profile_job_is_active_for_runtime(sys.modules[__name__], *args, **kwargs)


_files_similar_images_jobs_lock = threading.Lock()
_files_similar_images_jobs_active: set[str] = set()


def _run_files_similar_images_warm_job(*, job_key: str, artist_norm: str, names: list[str]) -> None:
    return _similar_images_runtime.run_files_similar_images_warm_job_for_runtime(
        sys.modules[__name__],
        job_key=job_key,
        artist_norm=artist_norm,
        names=names,
    )


def _enqueue_files_similar_images_warm(artist_norm: str, names: list[str], *, force: bool = False) -> bool:
    if not bool(_FILES_SIMILAR_IMAGES_WARM_ENABLED):
        return False
    if (not force) and _scan_pipeline_active():
        return False
    key = str(artist_norm or "").strip()
    if not key:
        return False
    job_key = f"similar-images:{key}"
    with _files_similar_images_jobs_lock:
        if job_key in _files_similar_images_jobs_active:
            return True
        _files_similar_images_jobs_active.add(job_key)
    threading.Thread(
        target=_run_files_similar_images_warm_job,
        kwargs={"job_key": job_key, "artist_norm": key, "names": list(names or [])},
        daemon=True,
        name=f"similar-img-{key[:24]}",
    ).start()
    return True


_FILES_PROFILE_BACKFILL_ON_REBUILD = True
_FILES_PROFILE_IDLE_AUTOSTART_INTERVAL_SEC = 30 * 60
_files_profile_backfill_lock = threading.Lock()
_files_profile_backfill_state: dict = {
    "running": False,
    "reason": "",
    "started_at": 0,
    "finished_at": 0,
    "cover_only": False,
    "current": 0,
    "total": 0,
    "current_artist": "",
    "errors": 0,
    "pending_artist_profiles": 0,
    "pending_album_profiles": 0,
    "eligible_album_profiles": 0,
    "pending_album_covers": 0,
    "last_probe_at": 0,
    "phase": "",
    "phase_label": "",
    "phase_index": 0,
    "phase_count": 0,
    "phase_current": 0,
    "phase_total": 0,
    "storage_scope_enabled": False,
    "storage_scope_mode": "",
    "storage_scope_devices": [],
}
_files_profile_backfill_idle_state: dict[str, Any] = {
    "last_probe_at": 0.0,
    "last_started_at": 0.0,
    "last_reason": "",
    "pending_artist_profiles": 0,
    "pending_album_profiles": 0,
    "eligible_album_profiles": 0,
    "pending_album_covers": 0,
}

_broken_album_backfill_lock = threading.Lock()
_broken_album_backfill_state: dict = {
    "running": False,
    "reason": "",
    "started_at": 0,
    "finished_at": 0,
    "current": 0,
    "total": 0,
    "current_artist": "",
    "current_album_id": 0,
    "changed": 0,
    "errors": 0,
    "last_error": "",
    "include_ai": False,
    "full_refresh": False,
}


def _files_profile_enrichment_priority_flags(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_profile_enrichment_priority_flags_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_profile_backfill_stage_specs(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_profile_backfill_stage_specs_for_runtime(sys.modules[__name__], *args, **kwargs)

_incomplete_ai_review_queue_lock = threading.Lock()
_incomplete_ai_review_queue: deque[dict[str, Any]] = deque()
_incomplete_ai_review_worker_started = False
_incomplete_ai_review_state: dict[str, Any] = {
    "running": False,
    "waiting_for_idle_scan": False,
    "queued": 0,
    "current_artist": "",
    "current_album_id": 0,
    "current_model": "",
    "last_status": "",
    "last_error": "",
    "last_finished_at": 0.0,
    "last_started_at": 0.0,
    "last_completed_artist": "",
    "last_completed_album_id": 0,
    "last_latency_ms": 0,
    "avg_latency_ms": 0.0,
    "completed_count": 0,
    "failed_count": 0,
    "skipped_count": 0,
    "last_result": {},
}

_AI_QUEUE_DOMAINS = ("matching", "dedupe", "review")
_ai_domain_queue_lock = threading.Lock()
_ai_domain_queues: dict[str, deque[dict[str, Any]]] = {domain: deque() for domain in _AI_QUEUE_DOMAINS}
_ai_domain_worker_started: dict[str, bool] = {domain: False for domain in _AI_QUEUE_DOMAINS}
_ai_domain_states: dict[str, dict[str, Any]] = {
    domain: {
        "running": False,
        "waiting_for_idle_scan": False,
        "queued": 0,
        "current_label": "",
        "current_model": "",
        "last_status": "",
        "last_error": "",
        "last_started_at": 0.0,
        "last_finished_at": 0.0,
        "last_latency_ms": 0,
        "avg_latency_ms": 0.0,
        "completed_count": 0,
        "failed_count": 0,
        "skipped_count": 0,
        "last_result": {},
    }
    for domain in _AI_QUEUE_DOMAINS
}


def _run_files_profile_backfill(*, reason: str = "manual", sleep_sec: float = 0.30, cover_only: bool = False) -> None:
    return _profile_runtime.run_files_profile_backfill_for_runtime(
        sys.modules[__name__],
        reason=reason,
        sleep_sec=sleep_sec,
        cover_only=cover_only,
    )


def _files_profile_backfill_pending_work(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_profile_backfill_pending_work_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_profile_backfill_maybe_start_idle(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_profile_backfill_maybe_start_idle_for_runtime(sys.modules[__name__], *args, **kwargs)


def _trigger_files_profile_backfill_async(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._trigger_files_profile_backfill_async_for_runtime(sys.modules[__name__], *args, **kwargs)


# ─────────────────────────────── Assistant (RAG + Chat) ───────────────────────────────
_ASSISTANT_GC_INTERVAL_SEC = 24 * 3600
_ASSISTANT_SESSION_MAX_AGE_DAYS = 90
_ASSISTANT_SESSION_HARD_CAP = 200


def call_ai_provider_longform(
    provider: str,
    model: str,
    system_msg: str,
    user_msg: str,
    max_tokens: int = 800,
    *,
    analysis_type: str,
    request_timeout_sec: float | None = None,
) -> str:
    return _ai_provider_runtime.call_ai_provider_longform_for_runtime(
        sys.modules[__name__],
        provider,
        model,
        system_msg,
        user_msg,
        max_tokens=max_tokens,
        analysis_type=analysis_type,
        request_timeout_sec=request_timeout_sec,
    )


def _assistant_text_hash(text: str) -> str:
    return _assistant_rag_runtime._assistant_text_hash(text)


def _assistant_chunk_text(text: str, max_chars: int = 900) -> list[str]:
    return _assistant_rag_runtime._assistant_chunk_text(text, max_chars=max_chars)


def _assistant_upsert_doc(
    conn,
    *,
    entity_type: str,
    entity_id: int,
    doc_type: str,
    source: str,
    provider: str = "",
    model: str = "",
    title: str = "",
    url: str = "",
    lang: str = "",
    content: str,
) -> int | None:
    return _assistant_rag_runtime.assistant_upsert_doc_for_runtime(
        sys.modules[__name__],
        conn,
        entity_type=entity_type,
        entity_id=entity_id,
        doc_type=doc_type,
        source=source,
        provider=provider,
        model=model,
        title=title,
        url=url,
        lang=lang,
        content=content,
    )


def _assistant_ingest_library_rag(conn) -> dict:
    return _assistant_rag_runtime.assistant_ingest_library_rag_for_runtime(sys.modules[__name__], conn)


def _assistant_ingest_artist_rag(conn, artist_id: int) -> dict:
    return _assistant_rag_runtime.assistant_ingest_artist_rag_for_runtime(sys.modules[__name__], conn, artist_id)


def _assistant_find_artist_ids_for_query(*args, **kwargs):
    return _assistant_chat_runtime._assistant_find_artist_ids_for_query_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_fetch_session_messages(*args, **kwargs):
    return _assistant_chat_runtime._assistant_fetch_session_messages_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_retrieve_chunks(*args, **kwargs):
    return _assistant_chat_runtime._assistant_retrieve_chunks_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_maybe_gc(*args, **kwargs):
    return _assistant_chat_runtime._assistant_maybe_gc_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_ensure_session(*args, **kwargs):
    return _assistant_chat_runtime._assistant_ensure_session_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_insert_message(*args, **kwargs):
    return _assistant_chat_runtime._assistant_insert_message_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_build_prompt(*args, **kwargs):
    return _assistant_chat_runtime._assistant_build_prompt_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_links_from_citations(*args, **kwargs):
    return _assistant_chat_runtime._assistant_links_from_citations_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_links_from_web_results(*args, **kwargs):
    return _assistant_chat_runtime._assistant_links_from_web_results_for_runtime(sys.modules[__name__], *args, **kwargs)


def _entity_discover_make_internal_link(*, entity_type: str, entity_id: int, label: str, base_url: str, subtitle: str = "", provider: str = "", thumb: str | None = None) -> dict[str, Any]:
    et = str(entity_type or "").strip().lower()
    eid = int(entity_id or 0)
    base = (base_url or "").rstrip("/")
    href = ""
    if et == "artist" and eid > 0:
        href = f"/library/artist/{eid}"
        thumb = thumb or f"{base}/api/library/files/artist/{eid}/image?size=192"
    elif et == "album" and eid > 0:
        href = f"/library/album/{eid}"
        thumb = thumb or f"{base}/api/library/files/album/{eid}/cover?size=192"
    elif et == "label" and label:
        href = f"/library/label/{quote(label)}"
    elif et == "genre" and label:
        href = f"/library/genre/{quote(label)}"
    else:
        href = ""
    return {
        "kind": "internal",
        "entity_type": et,
        "entity_id": eid,
        "label": str(label or "").strip(),
        "subtitle": str(subtitle or "").strip() or None,
        "href": href,
        "thumb": thumb,
        "provider": str(provider or "").strip() or None,
    }


def _entity_discover_dedup_links(links: list[dict[str, Any]], *, limit: int = 12) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for link in links or []:
        if not isinstance(link, dict):
            continue
        href = str(link.get("href") or "").strip()
        label = str(link.get("label") or "").strip()
        if not href or not label:
            continue
        key = f"{href}||{label}"
        if key in seen:
            continue
        seen.add(key)
        out.append(link)
        if len(out) >= int(limit):
            break
    return out


def _entity_discover_fallback_summary(*, entity_type: str, entity_label: str, sections: list[dict[str, Any]]) -> str:
    counts = []
    for sec in sections:
        title = str(sec.get("title") or "").strip()
        count = len(sec.get("links") or [])
        if title and count > 0:
            counts.append(f"{title}: {count}")
    base = f"Discovery for {entity_type} {entity_label}."
    if counts:
        return f"{base} Relevant paths found in PMDA and on the web: {'; '.join(counts[:4])}."
    return f"{base} No strong recommendations were found yet."




def _assistant_should_include_web_discovery(*args, **kwargs):
    return _assistant_chat_runtime._assistant_should_include_web_discovery_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_simplify_for_intent(*args, **kwargs):
    return _assistant_chat_runtime._assistant_simplify_for_intent_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_lang_for_message(*args, **kwargs):
    return _assistant_chat_runtime._assistant_lang_for_message_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_detect_tool_intent(*args, **kwargs):
    return _assistant_chat_runtime._assistant_detect_tool_intent_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_should_force_llm_rag(*args, **kwargs):
    return _assistant_chat_runtime._assistant_should_force_llm_rag_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_tool_library_counts(*args, **kwargs):
    return _assistant_chat_runtime._assistant_tool_library_counts_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_tool_library_top_genres(*args, **kwargs):
    return _assistant_chat_runtime._assistant_tool_library_top_genres_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_tool_library_top_labels(*args, **kwargs):
    return _assistant_chat_runtime._assistant_tool_library_top_labels_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_tool_library_top_artists(*args, **kwargs):
    return _assistant_chat_runtime._assistant_tool_library_top_artists_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_tool_artist_list_albums(*args, **kwargs):
    return _assistant_chat_runtime._assistant_tool_artist_list_albums_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_extract_requested_count(*args, **kwargs):
    return _assistant_chat_runtime._assistant_extract_requested_count_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_find_genre_for_query(*args, **kwargs):
    return _assistant_chat_runtime._assistant_find_genre_for_query_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_playlist_candidate_tracks(*args, **kwargs):
    return _assistant_chat_runtime._assistant_playlist_candidate_tracks_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_playlist_title(*args, **kwargs):
    return _assistant_chat_runtime._assistant_playlist_title_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_create_playlist_from_query(*args, **kwargs):
    return _assistant_chat_runtime._assistant_create_playlist_from_query_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_recommend_albums_from_query(*args, **kwargs):
    return _assistant_chat_runtime._assistant_recommend_albums_from_query_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_tool_artist_concerts(*args, **kwargs):
    return _assistant_chat_runtime._assistant_tool_artist_concerts_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_tool_artist_similar(*args, **kwargs):
    return _assistant_chat_runtime._assistant_tool_artist_similar_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_try_handle_tool_query(*args, **kwargs):
    return _assistant_chat_runtime._assistant_try_handle_tool_query_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_should_try_sql_agent(*args, **kwargs):
    return _assistant_chat_runtime._assistant_should_try_sql_agent_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_extract_json_obj(*args, **kwargs):
    return _assistant_chat_runtime._assistant_extract_json_obj_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_relink_external_artist_images_for_artist(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._files_relink_external_artist_images_for_artist_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_validate_readonly_sql(*args, **kwargs):
    return _assistant_chat_runtime._assistant_validate_readonly_sql_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_sql_agent_generate_query(*args, **kwargs):
    return _assistant_chat_runtime._assistant_sql_agent_generate_query_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_sql_agent_execute(*args, **kwargs):
    return _assistant_chat_runtime._assistant_sql_agent_execute_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_sql_agent_format_result(*args, **kwargs):
    return _assistant_chat_runtime._assistant_sql_agent_format_result_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_sql_agent_links_from_result(*args, **kwargs):
    return _assistant_chat_runtime._assistant_sql_agent_links_from_result_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_try_handle_sql_agent_query(*args, **kwargs):
    return _assistant_chat_runtime._assistant_try_handle_sql_agent_query_for_runtime(sys.modules[__name__], *args, **kwargs)













# ----- Run summary tracking ---------------------------------------------------
_scan_summary_runtime.initialize_run_summary_for_runtime(sys.modules[__name__])


def emit_final_summary(reason: str = "normal") -> None:
    return _scan_summary_runtime.emit_final_summary_for_runtime(sys.modules[__name__], reason=reason)

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

# Shutdown scheduler loop on exit
atexit.register(_stop_scheduler)
atexit.register(_stop_ai_usage_worker)
atexit.register(_stop_files_watcher_manager)
atexit.register(_stop_files_watcher)

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
        target = _musicbrainz_target_settings()
        self.is_mirror = bool(target.get("enabled"))
        if bool(target.get("enabled")):
            self.rate_limit_rps = max(1.0, float(MB_MIRROR_QUEUE_RPS or 12.0))
            self.worker_count = max(1, min(32, int(MB_MIRROR_QUEUE_WORKERS or 1)))
        else:
            self.rate_limit_rps = max(0.1, float(MB_PUBLIC_QUEUE_RPS or 1.0))
            self.worker_count = 1
        # Keep the configured RPS as a total budget across workers. The public API
        # stays single-threaded; a local mirror can absorb concurrent requests.
        self.sleep_interval_sec = max(0.0, float(self.worker_count) / self.rate_limit_rps)
        self.queue: Queue = Queue()
        self.results: Dict[str, Tuple[Optional[dict], Optional[Exception]]] = {}
        self.locks: Dict[str, threading.Event] = {}
        self._lock = threading.Lock()
        self._stats_lock = threading.Lock()
        self._stop_event = threading.Event()
        self.completed_count = 0
        self.error_count = 0
        self.timeout_count = 0
        self.total_latency_ms = 0.0
        self.last_latency_ms = 0.0
        self.last_completed_at = 0.0
        self.last_error = ""
        self.rate_limit_updates = 0
        self.worker_threads: list[threading.Thread] = []
        for idx in range(self.worker_count):
            worker_name = "MBQueueWorker" if self.worker_count == 1 else f"MBQueueWorker-{idx + 1}"
            worker_thread = threading.Thread(target=self._worker, daemon=True, name=worker_name)
            self.worker_threads.append(worker_thread)
            worker_thread.start()
        logging.info(
            "MusicBrainz queue initialized (mirror=%s, workers=%d, total rate limit: %.2f req/sec)",
            bool(target.get("enabled")),
            int(self.worker_count),
            float(self.rate_limit_rps),
        )

    def _request_timeout_seconds(self, request_id: str, *, worker_guard: bool = False) -> float:
        if request_id.startswith("fetch_rg_"):
            timeout_seconds = 300
        else:
            timeout_seconds = max(
                15,
                min(
                    45,
                    int(getattr(sys.modules[__name__], "MB_SEARCH_ALBUM_TIMEOUT_SEC", 20) or 20),
                ),
            )
        if worker_guard:
            # Keep the queue worker slightly stricter than callers waiting on the Event,
            # so a wedged network request cannot freeze the entire MusicBrainz queue.
            timeout_seconds = max(10, int(timeout_seconds) - 5)
        return float(timeout_seconds)

    def _run_callback_bounded(self, request_id: str, callback):
        timeout_seconds = self._request_timeout_seconds(request_id, worker_guard=True)
        result_box: dict[str, Any] = {}
        error_box: dict[str, Exception] = {}
        done = threading.Event()
        started = time.perf_counter()

        def _target() -> None:
            try:
                result_box["value"] = callback()
            except Exception as exc:
                error_box["error"] = exc
            finally:
                done.set()

        runner = threading.Thread(
            target=_target,
            daemon=True,
            name=f"MBQueueCall-{request_id[:32]}",
        )
        runner.start()
        if not done.wait(timeout_seconds):
            elapsed = time.perf_counter() - started
            raise TimeoutError(
                f"MusicBrainz queue request {request_id} timed out after {timeout_seconds:.1f}s "
                f"(elapsed {elapsed:.2f}s)"
            )
        if "error" in error_box:
            raise error_box["error"]
        return result_box.get("value")

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
                started = time.time()

                try:
                    result = self._run_callback_bounded(request_id, callback)
                except Exception as e:
                    error = e
                    if isinstance(e, TimeoutError):
                        logging.warning(
                            "[MB Queue] Request %s timed out inside queue worker; unblocking queue and continuing",
                            request_id,
                        )
                    else:
                        logging.debug("[MB Queue] Request %s failed: %s", request_id, e)
                finally:
                    elapsed_ms = max(0.0, (time.time() - started) * 1000.0)
                    with self._stats_lock:
                        self.completed_count = int(self.completed_count) + 1
                        self.total_latency_ms = float(self.total_latency_ms) + float(elapsed_ms)
                        self.last_latency_ms = float(elapsed_ms)
                        self.last_completed_at = time.time()
                        if error is not None:
                            self.error_count = int(self.error_count) + 1
                            if isinstance(error, TimeoutError):
                                self.timeout_count = int(self.timeout_count) + 1
                            self.last_error = str(error)

                # Store result and notify waiting thread
                with self._lock:
                    self.results[request_id] = (result, error)
                    if request_id in self.locks:
                        self.locks[request_id].set()

                # Public MusicBrainz must stay throttled; local mirrors can run much faster.
                if self.sleep_interval_sec > 0:
                    time.sleep(self.sleep_interval_sec)

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

        # Batch fetch (fetch_rg_*) can take 100+ seconds (100 pages × 1 s rate limit).
        # Single requests use a tighter timeout so scans can fall back to other providers faster.
        timeout_seconds = self._request_timeout_seconds(request_id, worker_guard=False)
        if event.wait(timeout=timeout_seconds):
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
            raise TimeoutError(f"MusicBrainz request {request_id} timed out after {timeout_seconds} seconds")

    def shutdown(self):
        """Shutdown queue workers."""
        if not self.enabled:
            return
        self._stop_event.set()
        worker_threads = list(getattr(self, "worker_threads", []) or [])
        for _ in worker_threads:
            self.queue.put(None)
        for worker_thread in worker_threads:
            if worker_thread.is_alive():
                worker_thread.join(timeout=5.0)

    def reconfigure_rate_limit(self, new_rps: float) -> float:
        next_rps = max(0.1, float(new_rps or self.rate_limit_rps or 1.0))
        with self._lock:
            self.rate_limit_rps = next_rps
            self.sleep_interval_sec = max(0.0, float(getattr(self, "worker_count", 1) or 1) / next_rps)
        with self._stats_lock:
            self.rate_limit_updates = int(self.rate_limit_updates) + 1
        return next_rps

    def stats_snapshot(self) -> dict[str, Any]:
        with self._stats_lock:
            completed = int(self.completed_count or 0)
            total_latency_ms = float(self.total_latency_ms or 0.0)
            return {
                "rate_limit_rps": float(self.rate_limit_rps or 0.0),
                "worker_count": int(getattr(self, "worker_count", 1) or 1),
                "queue_pending": int(self.queue.qsize()),
                "queue_waiters": int(len(self.locks or {})),
                "completed_count": completed,
                "error_count": int(self.error_count or 0),
                "timeout_count": int(self.timeout_count or 0),
                "avg_latency_ms": round(total_latency_ms / completed, 1) if completed > 0 else 0.0,
                "last_latency_ms": round(float(self.last_latency_ms or 0.0), 1),
                "last_completed_at": float(self.last_completed_at or 0.0),
                "last_error": str(self.last_error or ""),
                "rate_limit_updates": int(self.rate_limit_updates or 0),
            }

# Global MusicBrainz queue instance
_mb_queue: Optional[MusicBrainzQueue] = None

def get_mb_queue() -> MusicBrainzQueue:
    """Get or create the global MusicBrainz queue."""
    global _mb_queue
    if _mb_queue is None:
        _mb_queue = MusicBrainzQueue(enabled=MB_QUEUE_ENABLED and USE_MUSICBRAINZ)
    return _mb_queue


_RUNTIME_AUTO_TUNE_LOCK = threading.Lock()
_RUNTIME_AUTO_TUNE_THREAD: threading.Thread | None = None
_RUNTIME_AUTO_TUNE_STATE: dict[str, Any] = {
    "last_run_at": 0.0,
    "last_change_at": 0.0,
    "last_reason": "",
    "mb_last_completed": 0,
    "mb_last_errors": 0,
    "provider_last_timeouts": 0,
    "provider_last_rate_limited": 0,
    "discogs_effective_rpm": 0.0,
    "discogs_last_rate_limited": 0,
    "discogs_last_network_requests": 0,
}


def _runtime_auto_tune_snapshot(*args: Any, **kwargs: Any) -> Any:
    return _runtime_tuning._runtime_auto_tune_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)


def _discogs_effective_rpm(*args: Any, **kwargs: Any) -> Any:
    return _runtime_tuning._discogs_effective_rpm_for_runtime(sys.modules[__name__], *args, **kwargs)


def _runtime_auto_tune_note_discogs_rate_limited(*args: Any, **kwargs: Any) -> Any:
    return _runtime_tuning._runtime_auto_tune_note_discogs_rate_limited_for_runtime(sys.modules[__name__], *args, **kwargs)


def _runtime_auto_tune_apply(*args: Any, **kwargs: Any) -> Any:
    return _runtime_tuning._runtime_auto_tune_apply_for_runtime(sys.modules[__name__], *args, **kwargs)


def _runtime_auto_tune_worker(*args: Any, **kwargs: Any) -> Any:
    return _runtime_tuning._runtime_auto_tune_worker_for_runtime(sys.modules[__name__], *args, **kwargs)


def _start_runtime_auto_tune_worker(*args: Any, **kwargs: Any) -> Any:
    return _runtime_tuning._start_runtime_auto_tune_worker_for_runtime(sys.modules[__name__], *args, **kwargs)

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


def _provider_cache_norm(value: str) -> str:
    return _provider_gateway_runtime._provider_cache_norm(value)


def get_cached_provider_album_lookup(provider: str, artist_name: str, album_title: str) -> tuple[str | None, dict | None]:
    return _provider_gateway_runtime.get_cached_provider_album_lookup_for_runtime(
        sys.modules[__name__],
        provider,
        artist_name,
        album_title,
    )


def set_cached_provider_album_lookup(
    provider: str,
    artist_name: str,
    album_title: str,
    status: str,
    payload: dict | None = None,
) -> None:
    return _provider_gateway_runtime.set_cached_provider_album_lookup_for_runtime(
        sys.modules[__name__],
        provider,
        artist_name,
        album_title,
        status,
        payload,
    )


def fetch_provider_album_lookup_cached(
    provider: str,
    artist_name: str,
    album_title: str,
    fetcher,
) -> dict | None:
    return _provider_gateway_runtime.fetch_provider_album_lookup_cached_for_runtime(
        sys.modules[__name__],
        provider,
        artist_name,
        album_title,
        fetcher,
    )

# ───────────────────────────────── STATE IN MEMORY ──────────────────────────────────
state = {
    "scanning": False,
    "scan_starting": False,
    "scan_start_requested_at": None,
    "scan_type": "full",
    "scan_auto_trigger": None,
    "scan_scheduler_run_id": None,
    "scan_resume_run_id": None,
    "scan_resume_requested_run_id": None,
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
    "scan_detected_artists_total": 0, # Artists detected from source before resume/incremental filtering
    "scan_detected_albums_total": 0,  # Albums detected from source before resume/incremental filtering
    "scan_processed_albums_count": 0, # Albums whose artist scan worker completed
    "scan_published_albums_count": 0, # Albums currently published to live files index for this scan
    "scan_postprocessed_albums_count": 0, # Albums passed through streamed post-processing/fix
    "scan_resume_skipped_artists": 0, # Artists skipped by resume logic for current run
    "scan_resume_skipped_albums": 0,  # Albums skipped by resume logic for current run
    "scan_run_scope_preparing": False,  # True while resume/run-scope preparation is still computing
    "scan_run_scope_stage": "idle",     # idle | signatures | resume_compare | resume_seed | done
    "scan_run_scope_done": 0,           # Progress counter for current run-scope stage
    "scan_run_scope_total": 0,          # Total artists for current run-scope stage
    "scan_run_scope_artists_included": 0,  # Artists kept in the effective run scope
    "scan_run_scope_albums_included": 0,   # Albums kept in the effective run scope
    "scan_run_scope_started_at": None,      # Timestamp when run-scope prep started
    "scan_run_scope_updated_at": None,      # Timestamp of last run-scope progress update
    "scan_prescan_cache_snapshot_running": False,  # True while persisting pre-scan cache snapshot
    "scan_prescan_cache_snapshot_done": False,     # True once snapshot completed for current run
    "scan_prescan_cache_snapshot_rows": 0,         # Number of rows upserted during snapshot
    "scan_prescan_cache_snapshot_total": 0,        # Total album rows targeted by current snapshot
    "scan_prescan_cache_snapshot_updated_at": None,
    "scan_published_catchup_running": False,       # True while rebuilding published library rows during resume/index rehydration
    "scan_published_catchup_reason": None,
    "scan_published_catchup_done": 0,
    "scan_published_catchup_total": 0,
    "scan_published_catchup_ok": 0,
    "scan_published_catchup_failed": 0,
    "scan_published_catchup_current_artist": None,
    "scan_published_catchup_started_at": None,
    "scan_published_catchup_updated_at": None,
    "scan_published_catchup_finished_at": None,
    "files_cache_quality_recalc_running": False,   # True while recalculating files cache quality flags
    "files_cache_quality_recalc_total": 0,         # Total folders queued in the recalc run
    "files_cache_quality_recalc_done": 0,          # Processed folders in the recalc run
    "files_cache_quality_recalc_rows_upserted": 0, # Rows upserted during the recalc run
    "files_cache_quality_recalc_errors": 0,        # Errors while recalculating quality flags
    "files_cache_quality_recalc_missing_folders": 0,  # Missing folders seen during recalc
    "files_cache_quality_recalc_no_audio": 0,      # Folders with no audio files seen during recalc
    "files_cache_quality_recalc_started_at": None,
    "files_cache_quality_recalc_updated_at": None,
    "files_cache_quality_recalc_finished_at": None,
    "files_cache_quality_recalc_reason": None,
    "scan_ai_used_count": 0,          # Nombre de groupes où l'IA a été utilisée
    "scan_mb_used_count": 0,          # Nombre d'éditions enrichies avec MusicBrainz
    "scan_ai_enabled": False,         # Si l'IA est configurée et disponible
    "scan_ai_guard_calls_used": 0,    # API calls actually sent (guard-allowed)
    "scan_ai_guard_calls_blocked": 0, # Calls blocked by hard guardrails (cap/cooldown)
    "scan_ai_guard_last_reason": "",  # Last guardrail block reason
    "scan_ai_guard_last_block_at": None,
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
    "scan_post_processing": False,   # True while post-scan/post-artist metadata fixing is running
    "scan_post_total": 0,            # Total albums scheduled for post-processing
    "scan_post_done": 0,             # Albums processed in post-processing
    "scan_post_current_artist": None,
    "scan_post_current_album": None,
    # Files mode discovery counters (source walk before artist workers start).
    "scan_discovery_running": False,
    "scan_discovery_current_root": None,
    "scan_discovery_roots_done": 0,
    "scan_discovery_roots_total": 0,
    "scan_discovery_files_found": 0,
    "scan_discovery_folders_found": 0,
    "scan_discovery_albums_found": 0,
    "scan_discovery_artists_found": 0,
    "scan_discovery_stage": "idle",   # idle | filesystem | album_candidates | ready | cancelled
    "scan_discovery_entries_scanned": 0,
    "scan_discovery_root_entries_scanned": 0,
    "scan_discovery_folders_done": 0,
    "scan_discovery_folders_total": 0,
    "scan_discovery_albums_done": 0,
    "scan_discovery_albums_total": 0,
    "scan_discovery_started_at": None,
    "scan_discovery_updated_at": None,
    # Disk-aware source scan state (Unraid power saver mode).
    "storage_power_saver_enabled": False,
    "storage_provider": "unraid",
    "storage_active_devices": 0,
    "storage_devices_total": 0,
    "storage_current_device_id": None,
    "storage_current_device_label": None,
    "storage_bucket_done": 0,
    "storage_bucket_total": 0,
    "storage_buckets_done": 0,
    "storage_buckets_total": 0,
    "storage_estimated_watts_saved": 0.0,
    "storage_scan_plan": [],
    "storage_bucket_history": [],
    "storage_current_bucket": None,
    "storage_validation_error": "",
    "storage_started_at": None,
    "storage_bucket_started_at": None,
    # Track-level reconciliation for current/last run.
    "scan_tracks_detected_total": 0,
    "scan_tracks_library_kept": 0,
    "scan_tracks_moved_dupes": 0,
    "scan_tracks_moved_incomplete": 0,
    "scan_tracks_unaccounted": 0,
    "scan_dupe_moved_count": 0,
    "scan_dupe_moved_mb": 0,
    "improve_all": None,              # { "running": bool, "artist_id": int, "current": int, "total": int, "log": [], "result": {}, "error": str } or None
    "last_fix_all_by_provider": None, # { "musicbrainz": {identified,covers,tags}, "discogs": ..., "lastfm": ..., "bandcamp": ... } from last global fix-all run
    "last_fix_all_total_albums": 0,   # Total albums processed in that run (for N/M match display)
    "lidarr_add_incomplete": None,    # { "running": bool, "current": int, "total": int, "current_album": str, "current_artist": str, "added": int, "failed": int, "result": {} } or None
    "last_lidarr_add_added": 0,
    "last_lidarr_add_failed": 0,
    "incomplete_scan": None,           # { "running": bool, "run_id": int, "progress": int, "total": int, "current_artist": str, "current_album": str, "count": int, "error": str } or None
    "files_editions_by_album_id": {},  # Populated by _build_scan_plan in Files mode for workers and export
    "export_progress": None,           # { "running": bool, "tracks_done": int, "total_tracks": int, "albums_done": int, "total_albums": int, "error": str } or None
    "files_index": {
        "running": False,
        "started_at": None,
        "finished_at": None,
        "updated_at": None,
        "phase": None,
        "phase_started_at": None,
        "phase_message": None,
        "phase_progress": None,
        "phase_eta_seconds": None,
        "phase_rate_per_sec": None,
        "current_folder": None,
        "folders_processed": 0,
        "total_folders": 0,
        "collapsed_groups": 0,
        "discovered_folder_groups": 0,
        "collapse_parent_folders_processed": 0,
        "collapse_parent_folders_total": 0,
        "phase_item_done": 0,
        "phase_item_total": 0,
        "phase_item_label": None,
        "entries_scanned": 0,
        "discovered_audio_files": 0,
        "artists": 0,
        "albums": 0,
        "tracks": 0,
        "error": None,
    },
    "files_watcher": {
        "running": False,
        "roots": [],
        "dirty_count": 0,
        "dirty_count_by_root": {},
        "last_event_at": None,
        "last_event_path": None,
        "reason": "",
        "restart_in_progress": False,
        "last_restart_started_at": None,
        "last_restart_ended_at": None,
        "last_restart_duration_ms": None,
        "consecutive_failures": 0,
        "last_error": "",
    },
    "scan_dirty_folders_pending_clear": [],
    "scan_pipeline_flags": {},
    "scan_pipeline_async": False,
    "scan_pipeline_sync_target": "none",
    "scan_incomplete_moved_count": 0,
    "scan_incomplete_moved_mb": 0,
    "scan_incomplete_move_running": False,
    "scan_incomplete_move_done": 0,
    "scan_incomplete_move_total": 0,
    "scan_incomplete_move_current_album": None,
    "scan_player_sync_target": None,
    "scan_player_sync_ok": None,
    "scan_player_sync_message": "",
}
# Shared runtime/state lock. Keep it reentrant so endpoints can briefly pre-acquire
# to avoid blocking behind long scan updates, then enter existing `with lock:`
# sections reentrantly in the same thread.
lock = threading.RLock()
files_index_lock = threading.Lock()
_files_watcher_lock = threading.Lock()
_files_watcher_observer = None
_files_watcher_restart_lock = threading.Lock()
_files_watcher_manager_lock = threading.Lock()
_files_watcher_manager_thread = None
_files_watcher_manager_stop_event = threading.Event()
_files_watcher_manager_event = threading.Event()
_files_watcher_reconcile_requested = False
_files_watcher_reconcile_reason = ""
_files_watcher_retry_pending = False
_files_watcher_next_retry_at = 0.0
_files_watcher_backoff_step = 0
_FILES_WATCHER_RESTART_BACKOFF_SEC = (5.0, 15.0, 60.0, 300.0)


def _rlock_locked_compat(rlock: threading.RLock) -> bool:
    """
    Python 3.11's `_thread.RLock` lacks `.locked()`, while 3.14 exposes it.
    Use a non-blocking acquire/release probe so the code works on both.
    """
    locked_attr = getattr(rlock, "locked", None)
    if callable(locked_attr):
        try:
            return bool(locked_attr())
        except Exception:
            pass
    try:
        acquired = bool(rlock.acquire(blocking=False))
    except Exception:
        return False
    if acquired:
        try:
            rlock.release()
        except Exception:
            pass
        return False
    return True



# ──────────────────────────────── PLEX DB helper ────────────────────────────────
def plex_connect() -> sqlite3.Connection:
    """
    Open the Plex SQLite DB using UTF-8 *surrogate-escape* decoding so that any
    non-UTF-8 bytes are mapped to the U+DCxx range instead of throwing an error.

    We explicitly use immutable=1 to avoid any attempt by SQLite to write
    journal/WAL files on the Plex volume (which is mounted read-only in PMDA
    and can otherwise produce disk I/O errors on some filesystems).
    """
    if _get_library_mode() != "plex" and not _ALLOW_PLEX_DB_IN_FILES_MODE:
        raise RuntimeError("Plex DB access is disabled while PMDA is running in files library mode")
    # Open the Plex database in read-only + immutable mode to avoid write errors
    con = sqlite3.connect(f"file:{PLEX_DB_FILE}?mode=ro&immutable=1", uri=True, timeout=30)
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
    """Run lightweight files-mode startup checks without touching external player databases."""
    logging.info("Skipping legacy Plex startup checks; Plex is player-refresh only.")
    _self_diag()


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


def _storage_clean_path(value: Any, default: str) -> str:
    raw = str(value if value is not None else default).strip()
    if not raw:
        raw = default
    return raw.rstrip("/") or "/"


def _storage_power_saver_active(settings_snapshot: dict[str, Any] | None = None) -> bool:
    """
    True only when the explicit storage power-saver switch is enabled for Files mode.
    The feature is intentionally opt-in and v1 is Unraid-only.
    """
    snap = settings_snapshot or merged
    try:
        enabled = _parse_bool(snap.get("STORAGE_POWER_SAVER_ENABLED", STORAGE_POWER_SAVER_ENABLED))
    except Exception:
        enabled = STORAGE_POWER_SAVER_ENABLED
    provider = str(snap.get("STORAGE_PROVIDER", STORAGE_PROVIDER) or "unraid").strip().lower()
    return bool(enabled and provider == "unraid" and _get_library_mode() == "files")


def _storage_disk_sort_key(path: Path | str) -> tuple[int, str]:
    return _storage_buckets.disk_sort_key(path)


def _storage_unraid_settings(settings_snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    snap = settings_snapshot or merged
    return {
        "enabled": bool(_storage_power_saver_active(snap)),
        "provider": str(snap.get("STORAGE_PROVIDER", STORAGE_PROVIDER) or "unraid").strip().lower() or "unraid",
        "host_mnt_root": Path(_storage_clean_path(snap.get("UNRAID_HOST_MNT_ROOT", UNRAID_HOST_MNT_ROOT), "/host_mnt")),
        "user_share_host_root": Path(_storage_clean_path(snap.get("UNRAID_USER_SHARE_HOST_ROOT", UNRAID_USER_SHARE_HOST_ROOT), "/host_mnt/user/MURRAY/Music")),
        "container_share_root": Path(_storage_clean_path(snap.get("UNRAID_CONTAINER_SHARE_ROOT", UNRAID_CONTAINER_SHARE_ROOT), "/music")),
        "max_active_devices": int(max(1, min(64, _parse_int(snap.get("STORAGE_MAX_ACTIVE_DEVICES", STORAGE_MAX_ACTIVE_DEVICES), 1) or 1))),
        "spindown_policy": str(snap.get("STORAGE_SPINDOWN_POLICY", STORAGE_SPINDOWN_POLICY) or "none").strip().lower() or "none",
    }


def _storage_relative_to(path: Path, root: Path) -> Path | None:
    return _storage_buckets.relative_to(path, root)


def _storage_estimated_watts_saved(active_devices: int, total_devices: int) -> float:
    """
    Conservative estimate using 7 W per avoided spinning HDD. This is UI guidance,
    not a hardware guarantee.
    """
    return _storage_buckets.estimated_watts_saved(active_devices, total_devices)


def _storage_estimated_cost_saved_eur(watts_saved: float, seconds: float) -> float:
    return _storage_buckets.estimated_cost_saved_eur(watts_saved, seconds)


def _storage_unraid_build_scan_roots(
    active_roots: list[str] | tuple[str, ...] | None,
    *,
    settings_snapshot: dict[str, Any] | None = None,
) -> tuple[list[Path], list[dict[str, Any]]]:
    """
    Build direct disk roots for Unraid. Returned roots are readable access paths
    under /host_mnt/diskN, while metadata preserves the canonical /music root.
    Raises RuntimeError with a clear operator-facing reason when enabled but invalid.
    """
    return _storage_buckets.build_unraid_scan_roots(
        active_roots,
        settings=_storage_unraid_settings(settings_snapshot),
    )


_STORAGE_ENTRY_LOOKUP_CACHE: OrderedDict[tuple[int, int], dict[str, Any]] = OrderedDict()


def _storage_rel_text(path: Path | str | None, root: Path | str | None) -> str | None:
    return _storage_buckets.rel_text(path, root)


def _storage_entry_lookup_tables(
    entries: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> dict[str, Any]:
    return _storage_buckets.entry_lookup_tables(entries)


def _storage_find_entry_for_access_path(path: Path, entries: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None) -> dict[str, Any] | None:
    return _storage_buckets.find_entry_for_access_path(path, entries)


def _storage_canonical_path_for_access_path(path: Path, entries: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None) -> Path:
    return _storage_buckets.canonical_path_for_access_path(path, entries)


def _storage_access_path_for_canonical_path(
    canonical_path: Path,
    device_id: str | None,
    entries: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> Path | None:
    return _storage_buckets.access_path_for_canonical_path(canonical_path, device_id, entries)


def _storage_plan_summary(entries: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None) -> list[dict[str, Any]]:
    summary: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in entries or []:
        key = (str(entry.get("storage_device_id") or ""), str(entry.get("canonical_root") or ""))
        item = summary.setdefault(
            key,
            {
                "storage_provider": entry.get("storage_provider") or "unraid",
                "storage_device_id": entry.get("storage_device_id") or "",
                "storage_device_label": entry.get("storage_device_label") or entry.get("storage_device_id") or "",
                "storage_bucket_order": int(entry.get("storage_bucket_order") or 0),
                "canonical_root": entry.get("canonical_root") or "",
                "access_root": entry.get("access_root") or "",
                "albums_total": 0,
                "albums_done": 0,
                "status": "pending",
            },
        )
        item["storage_bucket_order"] = min(int(item.get("storage_bucket_order") or 0), int(entry.get("storage_bucket_order") or 0))
    return sorted(summary.values(), key=lambda x: (int(x.get("storage_bucket_order") or 0), str(x.get("storage_device_id") or "")))


def _storage_plan_summary_from_files_editions(files_editions_by_album_id: dict[int, dict] | dict[Any, Any] | None) -> list[dict[str, Any]]:
    buckets: dict[tuple[int, str, str], dict[str, Any]] = {}
    for fe_raw in (files_editions_by_album_id or {}).values():
        if not isinstance(fe_raw, dict):
            continue
        provider = str(fe_raw.get("storage_provider") or "").strip()
        device_id = str(fe_raw.get("storage_device_id") or "").strip()
        if not provider or not device_id:
            continue
        try:
            bucket_order = int(fe_raw.get("storage_bucket_order") or 0)
        except Exception:
            bucket_order = 0
        folder_raw = str(fe_raw.get("folder") or "").strip()
        access_raw = str(fe_raw.get("storage_access_path") or "").strip()
        canonical_root = str(Path(folder_raw).parent) if folder_raw else ""
        access_root = str(Path(access_raw).parent) if access_raw else ""
        key = (bucket_order, device_id, canonical_root)
        item = buckets.setdefault(
            key,
            {
                "storage_provider": provider,
                "storage_device_id": device_id,
                "storage_device_label": str(fe_raw.get("storage_device_label") or device_id).strip() or device_id,
                "storage_bucket_order": bucket_order,
                "canonical_root": canonical_root,
                "access_root": access_root,
                "albums_total": 0,
                "albums_done": 0,
                "status": "pending",
            },
        )
        item["albums_total"] = int(item.get("albums_total") or 0) + 1
    return sorted(
        buckets.values(),
        key=lambda item: (
            int(item.get("storage_bucket_order") or 0),
            str(item.get("storage_device_id") or ""),
            str(item.get("canonical_root") or ""),
        ),
    )


def _storage_plan_entry_for_canonical_path(
    canonical_path: Path | str | None,
    plan_entries: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> dict[str, Any] | None:
    return _storage_buckets.plan_entry_for_canonical_path(canonical_path, plan_entries)


def _storage_profile_backfill_scope(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._storage_profile_backfill_scope_for_runtime(sys.modules[__name__], *args, **kwargs)


def _storage_profile_backfill_scope_signature(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._storage_profile_backfill_scope_signature_for_runtime(sys.modules[__name__], *args, **kwargs)


def _storage_discovery_device_fields_for_path(
    path: Path | str | None,
    entries: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> dict[str, Any]:
    entry = _storage_find_entry_for_access_path(Path(str(path or "")), entries)
    if not entry:
        return {
            "storage_current_device_id": None,
            "storage_current_device_label": None,
        }
    device_id = str(entry.get("storage_device_id") or "").strip() or None
    label = str(entry.get("storage_device_label") or device_id or "").strip() or device_id
    return {
        "storage_current_device_id": device_id,
        "storage_current_device_label": label,
    }


def _storage_profile_enrichment_scope_for_artist(*args: Any, **kwargs: Any) -> Any:
    return _profile_support_runtime._storage_profile_enrichment_scope_for_artist_for_runtime(sys.modules[__name__], *args, **kwargs)


def _storage_should_defer_live_library_materialization() -> bool:
    """
    When disk-aware power saver is active during a Files scan, avoid writing
    winners into the user-share library tree mid-scan.

    Mid-scan filesystem materialization fans writes out across destination
    disks even though source reads are scoped to one active disk. Keep live
    SQL publication/enrichment, but defer filesystem exports until after the
    scan settles.
    """
    if _get_library_mode() != "files":
        return False
    with lock:
        scan_active = bool(
            state.get("scanning")
            or state.get("scan_starting")
            or state.get("scan_finalizing")
        )
        storage_enabled = bool(state.get("storage_power_saver_enabled"))
    return bool(scan_active and (storage_enabled or _storage_power_saver_active()))


def _storage_background_filesystem_scope(settings_snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return the storage scope that background filesystem I/O must obey."""
    scope = dict(_storage_profile_backfill_scope(settings_snapshot=settings_snapshot) or {})
    if not bool(scope.get("enabled")):
        return scope
    scope["allowed_device_ids"] = [
        str(device_id or "").strip()
        for device_id in list(scope.get("allowed_device_ids") or [])
        if str(device_id or "").strip()
    ]
    scope["plan_entries"] = [dict(item or {}) for item in list(scope.get("plan_entries") or []) if isinstance(item, dict)]
    return scope


def _storage_path_allowed_for_background_io(
    raw_path: Path | str | None,
    scope: dict[str, Any] | None = None,
    *,
    kind: str | None = None,
) -> bool:
    """
    Gate background filesystem reads while Unraid power saver is active.

    Source paths are allowed only when they belong to the currently active disk
    budget. Already-cached media remains safe because it lives under PMDA's cache
    root, not under the HDD array.
    """
    txt = str(raw_path or "").strip()
    if not txt:
        return False
    try:
        fs_path = path_for_fs_access(Path(txt))
    except Exception:
        fs_path = Path(txt)
    try:
        if _is_media_cache_file(fs_path, kind=kind) or _is_media_cache_file(Path(txt), kind=kind):
            return True
    except Exception:
        pass
    scope_now = scope if scope is not None else _storage_background_filesystem_scope()
    if not bool((scope_now or {}).get("enabled")):
        return True
    if not bool((scope_now or {}).get("scan_active")):
        return False
    allowed_device_ids = {
        str(device_id or "").strip()
        for device_id in list((scope_now or {}).get("allowed_device_ids") or [])
        if str(device_id or "").strip()
    }
    if not allowed_device_ids:
        return False
    plan_entries = list((scope_now or {}).get("plan_entries") or [])
    entry = _storage_plan_entry_for_canonical_path(txt, plan_entries)
    if entry is None:
        try:
            entry = _storage_find_entry_for_access_path(Path(txt), plan_entries)
        except Exception:
            entry = None
    if entry is None and str(fs_path) != txt:
        entry = _storage_plan_entry_for_canonical_path(str(fs_path), plan_entries)
        if entry is None:
            try:
                entry = _storage_find_entry_for_access_path(fs_path, plan_entries)
            except Exception:
                entry = None
    device_id = str((entry or {}).get("storage_device_id") or "").strip()
    return bool(device_id and device_id in allowed_device_ids)


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

def _truncate_utf8_component(value: str, max_bytes: int) -> str:
    """Truncate a path component by UTF-8 bytes, not Python characters."""
    text = str(value or "")
    budget = max(1, int(max_bytes or 1))
    raw = text.encode("utf-8", errors="ignore")
    if len(raw) <= budget:
        return text
    return raw[:budget].decode("utf-8", errors="ignore").rstrip(" .") or "Unknown"


def _sanitize_path_component(s: str, max_len: int = 120) -> str:
    """Sanitize artist or album title for use in a path component (no slashes, no leading/trailing dots)."""
    if not s:
        return "Unknown"
    out = re.sub(r'[/\\:*?"<>|]', "_", str(s).strip())
    out = out.strip(" .") or "Unknown"
    return _truncate_utf8_component(out, max_len)


def _artist_letter_bucket(artist_name: str) -> str:
    """
    Bucket folder prefix used by library-style trees (letter/artist/album).
    Keep A-Z buckets; everything else goes to "0".
    """
    raw = str(artist_name or "").strip()
    if not raw:
        return "0"
    first = raw[0].upper()
    return first if ("A" <= first <= "Z") else "0"


def _quarantine_artist_album_parts(
    src_folder: Path,
    *,
    artist_hint: str = "",
    album_hint: str = "",
) -> tuple[str, str, str]:
    """
    Resolve (letter, artist, album) for dupe/incomplete quarantine folders.
    Priority:
    1) explicit hints (metadata/context),
    2) relative path under known roots,
    3) folder name fallbacks.
    """
    src = path_for_fs_access(Path(src_folder))
    rel = relative_path_under_known_roots(src)
    rel_parts = list(rel.parts) if rel is not None else []

    artist_raw = str(artist_hint or "").strip()
    album_raw = str(album_hint or "").strip()

    if not artist_raw:
        if len(rel_parts) >= 2:
            artist_raw = str(rel_parts[-2]).strip()
        else:
            artist_raw = str(src.parent.name or "").strip()
    if not album_raw:
        if len(rel_parts) >= 1:
            album_raw = str(rel_parts[-1]).strip()
        else:
            album_raw = str(src.name or "").strip()

    artist = _sanitize_path_component(artist_raw or "Unknown Artist")
    album = _sanitize_path_component(album_raw or src.name or "Unknown Album")
    letter = _artist_letter_bucket(artist)
    return letter, artist, album


def _sanitize_album_title_display(title: str) -> str:
    """Normalize album titles for display/indexing (avoid obvious provider artifacts like trailing commas)."""
    t = str(title or "").strip()
    # Keep a single canonical display form for ellipsis-bearing titles so
    # provider variants such as "Takk…" and local tags like "Takk..."
    # collapse to the same published album title.
    t = t.replace("…", "...")
    t = re.sub(r"\s+", " ", t).strip()
    # Strip trailing punctuation that is almost certainly an artifact.
    while t and t[-1] in {",", ";", ":"}:
        t = t[:-1].rstrip()
    return t


def _backup_album_folder_before_fix(folder: Path, artist: str, album_title: str) -> Optional[Path]:
    """
    Copy the album folder to DUPE_ROOT/original_version/Artist/Album (with suffix if exists).
    Returns the destination path on success, None on failure. Logs and adds to detailed log via steps (caller adds step).
    """
    dupe_root = getattr(sys.modules[__name__], "DUPE_ROOT", Path("/dupes"))
    base = dupe_root / "original_version" / _sanitize_path_component(artist) / _sanitize_path_component(album_title)
    # We only ever want a single backup per album. If a backup folder already
    # exists for this artist/album, reuse it and do not create numbered
    # variants on subsequent runs.
    dst = base
    if dst.exists():
        logging.info(
            "[Backup before fix] Backup already exists for %s – reusing %s and skipping new copy",
            folder,
            dst,
        )
        return dst
    try:
        logging.info("[Backup before fix] Copying %s -> %s", folder, dst)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(str(folder), str(dst))
        logging.info("[Backup before fix] Backed up album to %s", dst)
        return dst
    except Exception as e:
        logging.error("[Backup before fix] Backup failed: %s -> %s: %s", folder, dst, e)
        return None


def build_dupe_destination(
    src_folder: Path,
    *,
    artist_hint: str = "",
    album_hint: str = "",
) -> Path:
    """
    Compute the destination path under DUPE_ROOT using canonical
    letter/artist/album layout.
    """
    letter, artist, album = _quarantine_artist_album_parts(
        Path(src_folder),
        artist_hint=artist_hint,
        album_hint=album_hint,
    )
    return DUPE_ROOT / letter / artist / album

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
    raw = raw.replace("…", "...")
    raw = re.sub(r"(?:\.{3,})+\s*$", "", raw).strip() or raw
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
    raw = raw.replace("…", "...")
    raw = re.sub(r"(?:\.{3,})+\s*$", "", raw).strip() or raw
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


# Dupe Detection v2: stronger album-title normalization for grouping (recall) while keeping
# "edition markers" available for explainability / classification.
_DUPE_NOISE_WORDS = {
    # Sources / release pipeline noise
    "web", "web-flac", "retail", "scene", "rip",
    # Formats / containers / codecs
    "flac", "mp3", "wav", "aiff", "alac", "ape", "wv", "ogg", "opus", "m4a", "aac",
    "dsd", "dsf", "dff", "mqa", "sacd",
    # Media
    "vinyl", "cassette", "cd", "digital",
}
_DUPE_EDITION_MARKERS = {
    # Variants that are usually the same "core" album for duplicate grouping
    "remaster", "remastered", "remastering",
    "deluxe", "expanded", "anniversary", "edition", "reissue", "re-release", "rerelease",
    "special", "limited", "extended", "enhanced",
    "bonus", "extras", "outtakes",
    "mono", "stereo",
    "explicit", "clean",
}
_DUPE_CONTENT_MARKERS = {
    # Markers that often mean a *different* album (avoid over-grouping by stripping them)
    "live", "demo", "soundtrack", "ost", "score",
}


def _dupe_extract_edition_tokens(title: str) -> list[str]:
    """Extract edition/variant markers from noisy album titles (kept for explainability)."""
    raw = (title or "").strip()
    if not raw:
        return []
    low = raw.lower()
    found: list[str] = []

    # Pull tokens from bracket/parenthetical segments too.
    for seg in re.findall(r"[\(\[]([^)\]]+)[\)\]]", low):
        seg = re.sub(r"\s+", " ", (seg or "").strip())
        if not seg:
            continue
        for tok in re.split(r"[\s,/|;]+", seg):
            t = (tok or "").strip()
            if not t:
                continue
            if t in _DUPE_NOISE_WORDS or t in _DUPE_EDITION_MARKERS or t in _DUPE_CONTENT_MARKERS:
                found.append(t)
            # Hi-res patterns like 24-96, 16/44.1
            if re.fullmatch(r"\d{1,2}[-/]\d{2,3}(?:\.\d)?", t):
                found.append(t)
            # Bitrate like 320kbps
            if re.fullmatch(r"\d{3,4}kbps", t):
                found.append(t)
            # "24bit", "16-bit"
            if re.fullmatch(r"\d{1,2}\s*[- ]?bit", t):
                found.append(t.replace(" ", ""))

    # Whole-title scan for common markers.
    for w in sorted(_DUPE_EDITION_MARKERS | _DUPE_CONTENT_MARKERS | _DUPE_NOISE_WORDS):
        if re.search(rf"\\b{re.escape(w)}\\b", low):
            found.append(w)

    # Resolution patterns in free text: "24-96", "24/96", "16-44.1"
    for m in re.findall(r"\b\d{1,2}\s*[-/]\s*\d{2,3}(?:\.\d)?\b", low):
        found.append(re.sub(r"\s+", "", m))

    # Dedupe keep order
    out: list[str] = []
    seen = set()
    for t in found:
        tt = (t or "").strip().lower()
        if not tt or tt in seen:
            continue
        seen.add(tt)
        out.append(tt)
    return out[:20]


def norm_album_for_dedup_loose(title: str) -> str:
    """
    Aggressive title normalization for dupe candidate grouping.
    Removes common release pipeline noise (WEB-FLAC, 24-96, etc.) and edition markers
    (Remastered/Deluxe/Expanded/Anniversary...) while keeping content markers like "live".
    """
    raw = (title or "").strip()
    if not raw:
        return "__untitled__"
    raw = raw.replace("…", "...")
    raw = re.sub(r"(?:\.{3,})+\s*$", "", raw).strip() or raw

    s = raw.replace("_", " ")
    # Drop bracketed segments entirely (often pure noise).
    s = re.sub(r"\[[^\]]*\]", " ", s)
    # Drop parenthetical segments (keep tokens separately via _dupe_extract_edition_tokens()).
    s = re.sub(r"\([^)]*\)", " ", s)

    low = s.lower()
    # Normalize separators
    low = re.sub(r"[_•·]+", " ", low)
    # Remove common hi-res / bitrate markers
    low = re.sub(r"\b\d{1,2}\s*[-/]\s*\d{2,3}(?:\.\d)?\b", " ", low)  # 24-96, 16/44.1
    low = re.sub(r"\b\d{3,4}\s*kbps\b", " ", low)
    low = re.sub(r"\b\d{1,2}\s*[- ]?bit\b", " ", low)
    low = re.sub(r"\b\d{2,3}(?:\.\d)?\s*khz\b", " ", low)

    # Remove noise words + edition markers, but keep content markers (live, soundtrack, etc.)
    drop_words = (_DUPE_NOISE_WORDS | _DUPE_EDITION_MARKERS) - _DUPE_CONTENT_MARKERS
    for w in sorted(drop_words, key=len, reverse=True):
        low = re.sub(rf"\\b{re.escape(w)}\\b", " ", low)

    # Strip catalog-like tokens (heuristic): ABC-1234, abc1234, etc.
    low = re.sub(r"\b[a-z]{2,6}[- ]?\d{2,6}\b", " ", low)

    # Collapse punctuation and whitespace
    low = re.sub(r"[\"'`]", "", low)
    low = re.sub(r"[^\w\s]+", " ", low)
    low = " ".join(low.split()).strip()

    if len(low) >= 3:
        return low

    # Fallback to strict normalization if loose collapsed too much.
    try:
        return norm_album_for_dedup(raw, normalize_parenthetical=True)
    except Exception:
        return low or "__untitled__"

def derive_album_title(database_title: str, meta: Dict[str, str], folder: Path, album_id: int) -> Tuple[str, str]:
    """
    Pick the most trustworthy album title available and return (title, source).
    Priority: explicit database title -> embedded tags -> folder name -> unique placeholder.
    """
    if database_title:
        title = database_title.strip()
        if title:
            return (title, "database")

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


def _files_album_id_for_folder(folder: Path | str) -> int:
    """Resolve Files-mode album id from folder path (best effort)."""
    try:
        if _get_library_mode() != "files":
            return 0
    except Exception:
        return 0
    try:
        key = _album_folder_cache_key(folder)
    except Exception:
        key = str(folder or "")
    if not key:
        return 0
    conn = _files_pg_connect()
    if conn is None:
        return 0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM files_albums WHERE folder_path = %s LIMIT 1", (key,))
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0
    except Exception:
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _duplicate_album_thumb_url(album_id: int, folder_path: Path | str | None = None) -> str:
    """
    Return a duplicate-table thumbnail URL that works in both library modes.
    """
    try:
        aid = int(album_id or 0)
    except Exception:
        aid = 0
    if aid <= 0:
        return ""
    if _get_library_mode() == "files":
        resolved_id = aid
        if folder_path:
            try:
                mapped = _files_album_id_for_folder(folder_path)
            except Exception:
                mapped = 0
            if mapped > 0:
                resolved_id = mapped
        if resolved_id <= 0:
            return ""
        try:
            base = request.url_root.rstrip("/")
        except Exception:
            base = ""
        path = f"/api/library/files/album/{resolved_id}/cover?size=128"
        return f"{base}{path}" if base else path
    return thumb_url(aid)

def build_cards() -> list[dict]:
    """
    Convert the live state["duplicates"] structure into the list of card
    dictionaries expected by the front-end.  Called both by the initial
    page render and the /api/duplicates endpoint so that new cards appear
    incrementally while a scan is running.
    """
    return _build_card_list(state.get("duplicates") or {})

# ───────────────────────────────── DATABASE HELPERS ──────────────────────────────────
class Track(NamedTuple):
    title: str
    idx: int
    disc: int
    dur: int  # duration in ms

def get_tracks(db_conn, album_id: int) -> List[Track]:
    """Compatibility stub for removed source-database track reads."""
    return []

def get_tracks_with_ids(db_conn, album_id: int) -> List[dict]:
    """Compatibility stub for removed source-database playback reads."""
    return []

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
    """Compatibility stub for removed source-database detail reads."""
    return []

def album_title(db_conn, album_id: int) -> str:
    """Compatibility stub for removed source-database album title reads."""
    return ""

def first_part_path(db_conn, album_id: int) -> Optional[Path]:
    """Compatibility stub for removed source-database path reads."""
    return None


def _album_path_under_dupes(db_conn, album_id: int) -> bool:
    """Compatibility stub for removed source-database duplicate path checks."""
    return False


# Cover filenames we consider "has cover" (same as create_pmda_test_files.sh)
_COVER_NAMES = (
    "cover.jpg", "cover.png", "cover.jpeg", "cover.webp",
    "folder.jpg", "folder.png", "folder.jpeg", "folder.webp",
    "front.jpg", "front.png", "front.jpeg", "front.webp",
    "artwork.jpg", "artwork.png", "artwork.jpeg", "artwork.webp",
    "albumart.jpg", "albumart.png", "albumart.jpeg", "albumart.webp",
    "albumartsmall.jpg", "albumartsmall.png",
    "thumb.jpg", "thumb.png", "thumb.jpeg", "thumb.webp",
)
_COVER_NAMES_LOWER = {n.lower() for n in _COVER_NAMES}

_COVER_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff"}
_COVER_STEM_PREFIXES = ("cover", "folder", "front", "artwork", "albumart", "thumb")
_ARTWORK_SLOT_KEYWORDS: dict[str, tuple[str, ...]] = {
    "front": ("cover", "folder", "front", "artwork", "albumart", "thumb", "jacket", "sleeve"),
    "back": ("back", "rear", "tray", "backcover", "back_cover"),
    "inside": ("inside", "inner", "inlay", "gatefold", "book"),
    "booklet": ("booklet", "insert", "liner", "leaflet"),
    "disc": ("disc", "cd", "vinyl", "lp", "media", "side"),
    "obi": ("obi",),
}
_ARTWORK_SLOT_LABELS = {
    "front": "Front cover",
    "back": "Back cover",
    "inside": "Inside sleeve",
    "booklet": "Booklet / insert",
    "disc": "Disc / media",
    "obi": "Obi / wrap",
    "other": "Additional artwork",
}
_ARTWORK_SLOT_ORDER = {
    "front": 0,
    "back": 1,
    "inside": 2,
    "booklet": 3,
    "disc": 4,
    "obi": 5,
    "other": 6,
}
_EMBEDDED_ARTWORK_TYPE_MAP = {
    3: "front",
    4: "back",
    5: "booklet",
    6: "disc",
}


def _is_probable_cover_filename(name: str) -> bool:
    low = str(name or "").strip().lower()
    if not low:
        return False
    if low in _COVER_NAMES_LOWER:
        return True
    stem, ext = os.path.splitext(low)
    if ext not in _COVER_EXTS:
        return False
    if any(stem.startswith(prefix) for prefix in _COVER_STEM_PREFIXES):
        return True
    if stem in {"art", "album", "jacket", "sleeve"}:
        return True
    return False


def _artwork_slot_from_name(name: str) -> str:
    low = str(name or "").strip().lower()
    if not low:
        return ""
    stem = Path(low).stem
    for slot, keywords in _ARTWORK_SLOT_KEYWORDS.items():
        if any(keyword and keyword in stem for keyword in keywords):
            return slot
    if _is_probable_cover_filename(low):
        return "front"
    return ""


def _artwork_slot_label(slot: str) -> str:
    return str(_ARTWORK_SLOT_LABELS.get(str(slot or "").strip().lower()) or "Artwork")


def _artwork_slot_sort_key(slot: str) -> tuple[int, str]:
    low = str(slot or "").strip().lower() or "other"
    return (int(_ARTWORK_SLOT_ORDER.get(low, 99)), low)


def _collect_folder_artwork_files(folder: Path, *, max_items: int = 12) -> list[tuple[Path, str]]:
    if not folder or not folder.is_dir():
        return []
    explicit: list[tuple[Path, str]] = []
    generic: list[Path] = []
    seen: set[str] = set()
    try:
        for p in sorted(folder.iterdir(), key=lambda x: x.name.lower()):
            if not p.is_file() or p.suffix.lower() not in _COVER_EXTS:
                continue
            slot = _artwork_slot_from_name(p.name)
            if slot:
                key = str(p.resolve())
                if key not in seen:
                    seen.add(key)
                    explicit.append((p, slot))
            else:
                generic.append(p)
    except OSError:
        return []
    if not any(slot == "front" for _, slot in explicit):
        if len(generic) == 1:
            key = str(generic[0].resolve())
            if key not in seen:
                seen.add(key)
                explicit.append((generic[0], "front"))
        elif not explicit and len(generic) <= 4:
            for p in generic[: max(1, int(max_items or 1))]:
                key = str(p.resolve())
                if key in seen:
                    continue
                seen.add(key)
                explicit.append((p, "other"))
    explicit.sort(key=lambda item: (_artwork_slot_sort_key(item[1]), item[0].name.lower()))
    return explicit[: max(1, int(max_items or 1))]


def _embedded_artwork_slot(type_value: Any, desc: str = "") -> str:
    try:
        pic_type = int(type_value)
    except Exception:
        pic_type = 0
    mapped = _EMBEDDED_ARTWORK_TYPE_MAP.get(pic_type)
    if mapped:
        return mapped
    desc_slot = _artwork_slot_from_name(desc)
    return desc_slot or "other"


def _extract_embedded_artworks_from_audio(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._extract_embedded_artworks_from_audio_for_runtime(sys.modules[__name__], *args, **kwargs)


def _extract_embedded_cover_from_folder(
    folder: Path,
    *,
    max_audio_files: int = 6,
) -> Optional[tuple[bytes, str]]:
    """
    Extract embedded cover bytes from one of the first audio files in a folder.
    We do not rely only on the first track because some releases embed art only on
    a subset of files.
    """
    if not folder or not folder.is_dir():
        return None
    try:
        artworks = _extract_embedded_artworks_from_folder(folder, max_audio_files=max_audio_files, max_items=6)
        for raw, mime, slot, _source, _desc in artworks:
            if slot == "front":
                return (raw, mime)
        if artworks:
            raw, mime, _slot, _source, _desc = artworks[0]
            return (raw, mime)
    except OSError:
        return None
    return None


def album_folder_has_cover(folder: Path) -> bool:
    """
    Return True if the album folder has a cover image.
    Priority:
    1) common image files in folder (cover/front/folder/artwork...)
    2) embedded cover in one of the first audio files
    """
    if not folder or not folder.is_dir():
        return False
    try:
        if _first_cover_path(folder):
            return True
        return bool(_extract_embedded_cover_from_folder(folder, max_audio_files=6))
    except OSError:
        return False


def _first_cover_path(folder: Path) -> Optional[Path]:
    """Return the path of the first existing cover file in the folder, or None."""
    if not folder or not folder.is_dir():
        return None
    try:
        generic_images: list[Path] = []
        for name in _COVER_NAMES:
            p = folder / name
            if p.is_file():
                return p
        for p in sorted(folder.iterdir(), key=lambda x: x.name.lower()):
            if not p.is_file():
                continue
            if p.suffix.lower() not in _COVER_EXTS:
                continue
            if _is_probable_cover_filename(p.name):
                return p
            generic_images.append(p)
        # Some releases ship a single image file with a non-standard name
        # (e.g. "00_release.jpg", "p1.jpg"). Treat it as cover.
        if len(generic_images) == 1:
            return generic_images[0]
        return None
    except OSError:
        return None


def _persist_files_album_cover_resolution(
    album_id: int,
    *,
    cover_path: str,
    folder_path: str,
    has_cover: bool,
    lookup_key: str = "",
) -> None:
    aid = int(album_id or 0)
    cover_txt = str(cover_path or "").strip()
    folder_txt = str(folder_path or "").strip()
    if aid <= 0:
        return
    try:
        conn = _files_pg_connect()
        if conn is not None:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE files_albums
                            SET has_cover = %s,
                                cover_path = %s,
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (bool(has_cover), cover_txt, aid),
                        )
            finally:
                conn.close()
    except Exception:
        logging.debug("Failed to persist files album cover resolution album_id=%s", aid, exc_info=True)
    if lookup_key:
        try:
            _files_cache_set_json(
                lookup_key,
                {
                    "cover_path": cover_txt,
                    "folder_path": folder_txt,
                    "no_cover": not bool(has_cover),
                },
                ttl=3600 if has_cover else 900,
            )
        except Exception:
            logging.debug("Failed to update files cover lookup cache album_id=%s", aid, exc_info=True)


def _resolve_files_album_cover_asset(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._resolve_files_album_cover_asset_for_runtime(sys.modules[__name__], *args, **kwargs)



def _extract_embedded_cover_from_audio(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._extract_embedded_cover_from_audio_for_runtime(sys.modules[__name__], *args, **kwargs)



def _extract_embedded_artworks_from_folder(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._extract_embedded_artworks_from_folder_for_runtime(sys.modules[__name__], *args, **kwargs)



def _get_local_cover_data_uri_for_vision(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._get_local_cover_data_uri_for_vision_for_runtime(sys.modules[__name__], *args, **kwargs)



# Vision API: keep covers tiny (comparison does not need high resolution). Resize if larger.
_MAX_COVER_SIZE_BYTES = 100 * 1024  # 100 KB max payload for vision
_MAX_COVER_PIXELS = 256  # Max width/height; enough for album cover comparison


def _resize_cover_for_vision(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._resize_cover_for_vision_for_runtime(sys.modules[__name__], *args, **kwargs)



def _encode_local_cover_to_data_uri(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._encode_local_cover_to_data_uri_for_runtime(sys.modules[__name__], *args, **kwargs)



_OCR_PREFERRED_LANGS = ("eng", "fra", "deu", "spa", "ita")
_CLASSICAL_COVER_OCR_CACHE_LOCK = threading.Lock()
_CLASSICAL_COVER_OCR_CACHE: "OrderedDict[str, str]" = OrderedDict()
_CLASSICAL_COVER_OCR_CACHE_MAX = 256


def _ocr_tesseract_available_langs() -> list[str]:
    cached = getattr(sys.modules[__name__], "_OCR_TESSERACT_AVAILABLE_LANGS", None)
    if isinstance(cached, list):
        return cached
    if not shutil.which("tesseract"):
        langs: list[str] = []
        setattr(sys.modules[__name__], "_OCR_TESSERACT_AVAILABLE_LANGS", langs)
        return langs
    try:
        proc = subprocess.run(
            ["tesseract", "--list-langs"],
            capture_output=True,
            text=True,
            timeout=12,
        )
        raw = "\n".join([proc.stdout or "", proc.stderr or ""])
        langs = [
            line.strip()
            for line in raw.splitlines()
            if line.strip() and not line.lower().startswith("list of available languages")
        ]
    except Exception:
        langs = []
    setattr(sys.modules[__name__], "_OCR_TESSERACT_AVAILABLE_LANGS", langs)
    return langs


def _ocr_tesseract_lang_spec(preferred_langs: tuple[str, ...] = _OCR_PREFERRED_LANGS) -> str:
    available = {str(lang or "").strip() for lang in _ocr_tesseract_available_langs() if str(lang or "").strip()}
    if not available:
        return ""
    chosen = [lang for lang in preferred_langs if lang in available]
    if not chosen:
        if "eng" in available:
            chosen = ["eng"]
        else:
            chosen = [sorted(available)[0]]
    return "+".join(chosen)


def _ocr_text_quality_score(text: str) -> int:
    raw = str(text or "").strip()
    if not raw:
        return 0
    tokens = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ0-9][A-Za-zÀ-ÖØ-öø-ÿ0-9'’&./:-]{2,}", raw)
    uniq = {tok.lower() for tok in tokens if len(tok) >= 3}
    return len(uniq)


def _ocr_prepare_cover_bytes(image_bytes: bytes) -> bytes:
    from io import BytesIO
    from PIL import Image, ImageFilter, ImageOps

    img = Image.open(BytesIO(image_bytes))
    img = ImageOps.exif_transpose(img)
    if img.mode != "L":
        img = ImageOps.grayscale(img)
    img = ImageOps.autocontrast(img)
    w, h = img.size
    longest = max(1, max(w, h))
    if longest < 1800:
        scale = min(3, max(1, int(round(1800 / float(longest)))))
        if scale > 1:
            img = img.resize((max(1, int(w * scale)), max(1, int(h * scale))), Image.Resampling.LANCZOS)
    img = img.filter(ImageFilter.SHARPEN)
    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def _ocr_cover_text_from_image_bytes(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._ocr_cover_text_from_image_bytes_for_runtime(sys.modules[__name__], *args, **kwargs)



def _folder_from_local_paths(local_paths: list[Any] | None) -> Optional[Path]:
    for raw in (local_paths or []):
        try:
            path = Path(str(raw))
        except Exception:
            continue
        try:
            if path.is_file():
                return path.parent
        except Exception:
            continue
    return None


def _cover_ocr_candidates_for_folder(folder: Path) -> list[tuple[str, bytes, str]]:
    if not folder or not folder.is_dir():
        return []
    out: list[tuple[str, bytes, str]] = []
    seen_hashes: set[str] = set()
    for cover_path, slot in _collect_folder_artwork_files(folder, max_items=6):
        if not cover_path.is_file():
            continue
        try:
            raw = cover_path.read_bytes()
            digest = hashlib.sha1(raw).hexdigest()
            if digest not in seen_hashes:
                seen_hashes.add(digest)
                out.append((f"file:{slot}:{cover_path.name}", raw, _mime_from_path(cover_path)))
        except Exception:
            pass
    try:
        embedded_items = _extract_embedded_artworks_from_folder(folder, max_audio_files=6, max_items=6)
    except Exception:
        embedded_items = []
    for raw, mime, slot, source_name, desc in embedded_items:
        digest = hashlib.sha1(bytes(raw or b"")).hexdigest()
        if raw and digest not in seen_hashes:
            seen_hashes.add(digest)
            suffix = f":{desc}" if desc else ""
            out.append((f"embedded:{slot}:{source_name}{suffix}", bytes(raw), str(mime or "image/jpeg")))
    return out


def _cover_ocr_mode() -> str:
    return _normalize_match_cover_ocr_mode(getattr(sys.modules[__name__], "MATCH_COVER_OCR_MODE", "smart"))


def _track_titles_look_unreliable_for_identity(local_tracks: list[Any] | None) -> bool:
    items = _local_track_titles_for_strict(list(local_tracks or []))
    if not items:
        return True
    suspicious = 0
    for title in items[:24]:
        raw = str(title or "").strip()
        if not raw:
            suspicious += 1
            continue
        low = raw.lower()
        if re.search(r"\.(flac|mp3|m4a|wav|aiff|ogg)\b", low):
            suspicious += 1
            continue
        if re.match(r"^(track|audio|piste|titre)\s*\d+\b", low):
            suspicious += 1
            continue
        if re.match(r"^\d{1,2}[-_. ]\d{1,2}\b", low):
            suspicious += 1
            continue
    return suspicious >= max(1, math.ceil(len(items[:24]) * 0.45))


def _cover_ocr_smart_trigger(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._cover_ocr_smart_trigger_for_runtime(sys.modules[__name__], *args, **kwargs)



def _identity_cover_ocr_context(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._identity_cover_ocr_context_for_runtime(sys.modules[__name__], *args, **kwargs)



def _cover_ocr_best_match_score(local_context: dict | None, candidate_value: str, *, album_mode: bool) -> float:
    if not isinstance(local_context, dict):
        return 0.0
    raw_value = str(candidate_value or "").strip()
    if not raw_value:
        return 0.0
    norm_key = "cover_ocr_title_norms" if album_mode else "cover_ocr_artist_norms"
    candidate_norm = _normalize_identity_album_strict(raw_value) if album_mode else _normalize_identity_text_strict(raw_value)
    norms = local_context.get(norm_key) or set()
    try:
        norm_values = set(norms)
    except Exception:
        norm_values = set()
    if candidate_norm and candidate_norm in norm_values:
        return 1.0
    best = 0.0
    for line in list(local_context.get("cover_ocr_lines") or [])[:20]:
        best = max(best, _provider_identity_text_score(raw_value, str(line or "")))
    text_blob = str(local_context.get("cover_ocr_text") or "").strip()
    if text_blob:
        best = max(best, _provider_identity_text_score(raw_value, text_blob))
    return float(max(0.0, min(1.0, best)))

def extract_tags(*args: Any, **kwargs: Any) -> Any:
    return _audio_runtime.extract_tags_for_runtime(sys.modules[__name__], *args, **kwargs)



def _iter_audio_files_under_roots(*args: Any, **kwargs: Any) -> Any:
    return _audio_runtime._iter_audio_files_under_roots_for_runtime(sys.modules[__name__], *args, **kwargs)



def _group_audio_files_by_folder_under_roots(*args: Any, **kwargs: Any) -> Any:
    return _audio_runtime._group_audio_files_by_folder_under_roots_for_runtime(sys.modules[__name__], *args, **kwargs)


def _iter_audio_files_under_roots_checkpointed(
    roots: list[str],
    *,
    run_id: str | None,
    progress_cb=None,
    progress_every: int = 250,
    heartbeat_seconds: float = 10.0,
    stop_event: threading.Event | None = None,
    pause_event: threading.Event | None = None,
    resume_snapshot: dict[str, Any] | None = None,
    checkpoint_cb=None,
    dir_skip_lookup=None,
    dir_skip_resolver=None,
) -> tuple[list[Path], set[str]]:
    return _filesystem_walk_runtime.iter_audio_files_under_roots_checkpointed_for_runtime(
        sys.modules[__name__],
        roots,
        run_id=run_id,
        progress_cb=progress_cb,
        progress_every=progress_every,
        heartbeat_seconds=heartbeat_seconds,
        stop_event=stop_event,
        pause_event=pause_event,
        resume_snapshot=resume_snapshot,
        checkpoint_cb=checkpoint_cb,
        dir_skip_lookup=dir_skip_lookup,
        dir_skip_resolver=dir_skip_resolver,
    )


# Global ffprobe pool for parallel processing
_ffprobe_pool: Optional[ThreadPoolExecutor] = None

def get_ffprobe_pool() -> ThreadPoolExecutor:
    """Get or create the global ffprobe pool."""
    global _ffprobe_pool
    if _ffprobe_pool is None:
        _ffprobe_pool = ThreadPoolExecutor(max_workers=FFPROBE_POOL_SIZE, thread_name_prefix="ffprobe")
        logging.debug(f"Created ffprobe pool with {FFPROBE_POOL_SIZE} workers")
    return _ffprobe_pool

def _run_ffprobe(*args: Any, **kwargs: Any) -> Any:
    return _audio_runtime._run_ffprobe_for_runtime(sys.modules[__name__], *args, **kwargs)


def _run_ffprobe_duration_sec(*args: Any, **kwargs: Any) -> Any:
    return _audio_runtime._run_ffprobe_duration_sec_for_runtime(sys.modules[__name__], *args, **kwargs)

def analyse_format(*args: Any, **kwargs: Any) -> Any:
    return _audio_runtime.analyse_format_for_runtime(sys.modules[__name__], *args, **kwargs)

# ───────────────────────────────── DUPLICATE DETECTION ─────────────────────────────────
def signature(tracks: List[Track]) -> tuple:
    # round durations to seconds before grouping
    return tuple(sorted((
        t.disc,
        t.idx,
        _dupe_norm_track_title(getattr(t, "title", "") or "") or _norm_track_title_strict(getattr(t, "title", "") or ""),
        int(round(t.dur/1000))
    ) for t in tracks))

def overlap(a: set, b: set) -> float:
    return len(a & b) / max(len(a), len(b))

_MUSIC_ASCII_FOLD = str.maketrans(
    {
        "ð": "d",
        "Ð": "D",
        "þ": "th",
        "Þ": "Th",
        "æ": "ae",
        "Æ": "Ae",
        "œ": "oe",
        "Œ": "Oe",
        "ø": "o",
        "Ø": "O",
        "đ": "d",
        "Đ": "D",
        "ł": "l",
        "Ł": "L",
    }
)


def _fold_music_ascii(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._fold_music_ascii_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_norm_track_title(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_norm_track_title_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_track_title_set(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_track_title_set_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_jaccard(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_jaccard_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_track_title_containment(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_track_title_containment_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_track_count_ratio(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_track_count_ratio_for_runtime(sys.modules[__name__], *args, **kwargs)


def _edition_track_count_for_dupe(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._edition_track_count_for_dupe_for_runtime(sys.modules[__name__], *args, **kwargs)


def _edition_total_duration_for_dupe(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._edition_total_duration_for_dupe_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_group_has_exact_provider_trackcount_signal(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_group_has_exact_provider_trackcount_signal_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_roman_to_int(value: str) -> int:
    text = str(value or "").strip().upper()
    if not text or not re.fullmatch(r"[IVXLCDM]+", text):
        return 0
    values = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    total = 0
    prev = 0
    for char in reversed(text):
        current = values.get(char, 0)
        if current < prev:
            total -= current
        else:
            total += current
            prev = current
    return total if 0 < total < 1000 else 0


_DUPE_SEQUENCE_MARKER_RE = re.compile(
    r"\b(?P<label>vol(?:ume)?|part|pt|chapter|chap|disc|disk|cd)\.?\s*(?P<number>\d{1,3}|[ivxlcdm]{1,8})\b",
    re.IGNORECASE,
)


def _dupe_album_title_values(e: dict | None) -> list[str]:
    values: list[str] = []
    if not isinstance(e, dict):
        return values
    for key in ("title_raw", "plex_title", "album_norm"):
        raw = str(e.get(key) or "").strip()
        if raw:
            values.append(raw)
    folder = e.get("folder")
    if folder:
        try:
            folder_name = Path(str(folder)).name.strip()
        except Exception:
            folder_name = str(folder or "").strip()
        if folder_name:
            values.append(folder_name)
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        try:
            loose = str(norm_album_for_dedup_loose(value) or "").strip()
        except Exception:
            loose = str(value or "").strip().lower()
        if not loose or loose.startswith("__untitled__") or loose in seen:
            continue
        seen.add(loose)
        out.append(loose)
    return out


def _dupe_sequence_markers_for_edition(e: dict | None) -> list[tuple[str, int, str]]:
    markers: list[tuple[str, int, str]] = []
    seen: set[tuple[str, int, str]] = set()
    for title in _dupe_album_title_values(e):
        for match in _DUPE_SEQUENCE_MARKER_RE.finditer(title):
            raw_number = str(match.group("number") or "").strip()
            number = int(raw_number) if raw_number.isdigit() else _dupe_roman_to_int(raw_number)
            if number <= 0:
                continue
            label = str(match.group("label") or "").lower().rstrip(".")
            if label.startswith("vol"):
                label = "volume"
            elif label in {"pt"}:
                label = "part"
            elif label == "chap":
                label = "chapter"
            elif label == "disk":
                label = "disc"
            base = (title[: match.start()] + " " + title[match.end() :]).strip()
            base = re.sub(r"\s+", " ", base)
            if not base:
                continue
            marker = (label, number, base)
            if marker in seen:
                continue
            seen.add(marker)
            markers.append(marker)
    return markers


def _dupe_album_sequence_marker_conflict(editions: list[dict]) -> bool:
    """Detect titles like `Vol. 1` vs `Vol. 2`; same provider IDs can be wrong here."""
    if len(editions or []) < 2:
        return False
    by_edition = [_dupe_sequence_markers_for_edition(e) for e in editions or []]
    for i, left_markers in enumerate(by_edition):
        for right_markers in by_edition[i + 1:]:
            for left_label, left_number, left_base in left_markers:
                for right_label, right_number, right_base in right_markers:
                    if left_label != right_label or left_number == right_number:
                        continue
                    if left_base == right_base:
                        return True
                    try:
                        if float(_provider_identity_text_score(left_base, right_base)) >= 0.90:
                            return True
                    except Exception:
                        continue
    return False


def _dupe_provider_id_title_conflict(
    editions: list[dict],
    *,
    max_jaccard: float | None = None,
    min_track_ratio: float | None = None,
) -> bool:
    if len(editions or []) < 2:
        return False
    if _dupe_album_sequence_marker_conflict(editions):
        return True
    try:
        max_jac = float(max_jaccard if max_jaccard is not None else 0.0)
    except Exception:
        max_jac = 0.0
    try:
        min_ratio = float(min_track_ratio if min_track_ratio is not None else 0.0)
    except Exception:
        min_ratio = 0.0
    if max_jac >= 0.10 or min_ratio < 0.95:
        return False

    titles = []
    for e in editions or []:
        vals = _dupe_album_title_values(e)
        if vals:
            titles.append(vals[0])
    unique_titles = sorted({t for t in titles if t})
    if len(unique_titles) < 2:
        return False
    best_similarity = 0.0
    for i, left in enumerate(unique_titles):
        for right in unique_titles[i + 1:]:
            try:
                best_similarity = max(best_similarity, float(_provider_identity_text_score(left, right)))
            except Exception:
                continue
    return best_similarity < 0.72


def _dupe_disc_folder_number(folder) -> int:
    if not folder:
        return 0
    try:
        name = Path(str(folder)).name.strip().lower()
    except Exception:
        name = str(folder or "").strip().lower()
    m = re.fullmatch(r"(?:cd|disc|disk)\s*0*(\d{1,3})", name)
    if not m:
        return 0
    try:
        return int(m.group(1))
    except Exception:
        return 0


def _dupe_is_multidisc_sibling_group(editions: list[dict]) -> bool:
    if len(editions or []) < 2:
        return False
    parent_keys: set[str] = set()
    disc_numbers: set[int] = set()
    total_discs_values: set[int] = set()
    for e in editions or []:
        folder = e.get("folder")
        if not folder:
            return False
        disc_num = _dupe_disc_folder_number(folder)
        if disc_num <= 0:
            return False
        disc_numbers.add(disc_num)
        try:
            parent_keys.add(_dupe_folder_key_str(Path(str(folder)).parent))
        except Exception:
            return False
        combined = {}
        if isinstance(e.get("meta"), dict):
            combined.update(e.get("meta") or {})
        if isinstance(e.get("tags"), dict):
            combined.update(e.get("tags") or {})
        for key in ("totaldiscs", "disctotal", "disc_total", "total_discs"):
            try:
                value = int(str(combined.get(key) or "").strip())
            except Exception:
                value = 0
            if value > 0:
                total_discs_values.add(value)
                break
    if len(parent_keys) != 1 or len(disc_numbers) < 2:
        return False
    if total_discs_values and len(total_discs_values) == 1:
        total = next(iter(total_discs_values))
        if total > 0 and max(disc_numbers) > total:
            return False
    return True


def _dupe_track_sig_title_conflict(editions: list[dict]) -> bool:
    if len(editions or []) < 2:
        return False
    if _dupe_group_has_exact_provider_trackcount_signal(editions):
        return False
    strict_keys = {
        _strict_album_identity_key_for_edition(e)
        for e in editions
        if _strict_album_identity_key_for_edition(e)
    }
    if len(strict_keys) == 1 and strict_keys:
        return False
    titles: list[str] = []
    for e in editions:
        title = str(e.get("title_raw") or e.get("plex_title") or e.get("album_norm") or "").strip()
        loose = str(e.get("_dupe_title_norm_loose") or norm_album_for_dedup_loose(title)).strip()
        if loose:
            titles.append(loose)
    unique_titles = sorted({t for t in titles if t and not t.startswith("__untitled__")})
    if len(unique_titles) <= 1:
        return False
    best_similarity = 0.0
    for i, left in enumerate(unique_titles):
        for right in unique_titles[i + 1:]:
            try:
                best_similarity = max(best_similarity, float(_provider_identity_text_score(left, right)))
            except Exception:
                continue
    return best_similarity < 0.84


def _dupe_ai_used_results_count(results: list[dict]) -> int:
    total = 0
    for group in results or []:
        if not isinstance(group, dict):
            continue
        best = group.get("best")
        if isinstance(best, dict) and bool(best.get("used_ai")):
            total += 1
    return total


def _dupe_folder_key_str(folder) -> str:
    """Stable-ish folder identity for caching/feedback (prefer resolved absolute path)."""
    if not folder:
        return ""
    try:
        return str(Path(str(folder)).resolve())
    except Exception:
        return str(folder)


def _dupe_group_key_from_editions(editions: list[dict]) -> str:
    """Compute a stable group key from the set of edition folders (order-independent)."""
    keys: list[str] = []
    for e in editions or []:
        k = _dupe_folder_key_str((e or {}).get("folder"))
        if k:
            keys.append(k)
    keys = sorted(set(keys))
    payload = "dupe_v2\n" + "\n".join(keys)
    return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()


def _dupe_feedback_pair_key(folder_a: str, folder_b: str) -> tuple[str, str]:
    a = (folder_a or "").strip()
    b = (folder_b or "").strip()
    if not a or not b:
        return ("", "")
    if a == b:
        return (a, b)
    return (a, b) if a < b else (b, a)


def _dupe_load_feedback_pairs_for_artist(artist: str) -> dict[tuple[str, str], str]:
    """Return {(folder_a, folder_b): label} for one artist."""
    out: dict[tuple[str, str], str] = {}
    artist_name = (artist or "").strip()
    if not artist_name:
        return out
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.execute(
            "SELECT folder_a, folder_b, label FROM dupe_feedback_pairs WHERE artist = ?",
            (artist_name,),
        )
        rows = cur.fetchall()
        con.close()
    except Exception:
        return out
    for fa, fb, lab in rows or []:
        a = (fa or "").strip()
        b = (fb or "").strip()
        if not a or not b:
            continue
        key = _dupe_feedback_pair_key(a, b)
        if not key[0] or not key[1]:
            continue
        out[key] = (lab or "").strip().lower()
    return out


def _dupe_ai_cache_get(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_choose_best_runtime._dupe_ai_cache_get_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_ai_cache_put(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_choose_best_runtime._dupe_ai_cache_put_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_choose_best_heuristic(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_choose_best_runtime._dupe_choose_best_heuristic_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_get_mb_release_group_id(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_get_mb_release_group_id_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_get_mb_release_id(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_get_mb_release_id_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_get_discogs_id(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_get_discogs_id_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_get_lastfm_mbid(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_get_lastfm_mbid_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_get_bandcamp_url(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_get_bandcamp_url_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_audio_fp_set_for_edition(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_audio_fp_set_for_edition_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_audio_sig_for_edition(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_audio_sig_for_edition_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dupe_split_editions_by_similarity(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime._dupe_split_editions_by_similarity_for_runtime(sys.modules[__name__], *args, **kwargs)

def editions_share_confident_signal(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_signal_runtime.editions_share_confident_signal_for_runtime(sys.modules[__name__], *args, **kwargs)

def detect_broken_album(*args: Any, **kwargs: Any) -> Any:
    return _broken_album_runtime.detect_broken_album_for_runtime(sys.modules[__name__], *args, **kwargs)


_INCOMPLETE_VERDICT_CONFIRMED = "confirmed_incomplete"
_INCOMPLETE_VERDICT_REVIEW = "likely_incomplete_review"
_INCOMPLETE_VERDICT_ALT_EDITION = "alternate_edition_not_incomplete"
_INCOMPLETE_VERDICT_IDENTITY_MISMATCH = "identity_mismatch_not_incomplete"
_INCOMPLETE_VERDICT_NUMBERING = "numbering_or_tag_issue_not_incomplete"
_INCOMPLETE_VERDICT_STALE_MATCH = "current_folder_matches_expected_not_incomplete"
_INCOMPLETE_VERDICT_MANUAL = "insufficient_evidence_manual_review"
_INCOMPLETE_IDENTITY_REJECT_CODES = {
    "artist_mismatch",
    "album_mismatch",
    "artist_partial_overlap",
    "provider_id_mismatch",
}
_INCOMPLETE_REVIEW_ONLY_REJECT_CODES = {
    "provider_no_tracklist",
    "provider_no_tracklist_full_album",
    "provider_id_missing",
    "classical_context_insufficient",
    "strict_reject",
}
_INCOMPLETE_CLASSICAL_REJECT_CODES = {
    "classical_work_mismatch",
    "classical_composer_mismatch",
    "classical_performance_mismatch",
    "classical_track_count_mismatch",
    "classical_disc_count_mismatch",
    "classical_duration_mismatch",
    "classical_label_plus_performance_mismatch",
    "classical_year_mismatch",
}


def _broken_album_meta_matches_snapshot(
    stored_title: str | None,
    stored_folder: str | None,
    meta_title: str | None,
    meta_folder: str | None,
) -> bool:
    stored_folder_value = os.path.normpath(str(stored_folder or "").strip()) if str(stored_folder or "").strip() else ""
    meta_folder_value = os.path.normpath(str(meta_folder or "").strip()) if str(meta_folder or "").strip() else ""
    if stored_folder_value and meta_folder_value:
        return stored_folder_value == meta_folder_value

    stored_title_value = _norm_track_title_strict(str(stored_title or ""))
    meta_title_value = _norm_track_title_strict(str(meta_title or ""))
    if stored_title_value and meta_title_value:
        return stored_title_value == meta_title_value
    return True


def _broken_album_title_is_placeholder(value: Any) -> bool:
    title = str(value or "").strip()
    if not title:
        return True
    return bool(re.match(r"^(album\s+\d+|unknown album)$", title, flags=re.IGNORECASE))


def _broken_album_snapshot_anchor(
    stored_title: Any,
    stored_folder: Any,
    *,
    trace_title: Any = None,
    trace_folder: Any = None,
    edition_title: Any = None,
    edition_folder: Any = None,
) -> tuple[str, str]:
    title = str(stored_title or "").strip()
    folder = str(stored_folder or "").strip()
    if _broken_album_title_is_placeholder(title):
        title = ""
    folder_norm = os.path.normpath(folder) if folder else ""

    candidates: list[tuple[str, str]] = []
    for candidate_title, candidate_folder in (
        (trace_title, trace_folder),
        (edition_title, edition_folder),
    ):
        cand_title = str(candidate_title or "").strip()
        cand_folder = str(candidate_folder or "").strip()
        if _broken_album_title_is_placeholder(cand_title):
            cand_title = ""
        candidates.append((cand_title, os.path.normpath(cand_folder) if cand_folder else ""))

    if not folder_norm:
        folder_candidates = [cand_folder for _, cand_folder in candidates if cand_folder]
        if folder_candidates:
            unique_folders = list(dict.fromkeys(folder_candidates))
            if len(unique_folders) == 1:
                folder_norm = unique_folders[0]

    if not title:
        if folder_norm:
            title_candidates = [cand_title for cand_title, cand_folder in candidates if cand_title and cand_folder == folder_norm]
            if title_candidates:
                title = title_candidates[0]
        if not title:
            generic_titles = [cand_title for cand_title, _ in candidates if cand_title]
            unique_titles = list(dict.fromkeys(generic_titles))
            if len(unique_titles) == 1:
                title = unique_titles[0]

    return title, folder_norm
_INCOMPLETE_BONUS_TRACK_MARKERS = (
    "bonus",
    "demo",
    "live",
    "mix",
    "remix",
    "instrumental",
    "acoustic",
    "karaoke",
    "alternate",
    "alt ",
    "outtake",
    "version",
    "edit",
)
_INCOMPLETE_QUARANTINE_ALLOWED_VERDICTS = {
    _INCOMPLETE_VERDICT_CONFIRMED,
}


def _incomplete_policy_thresholds() -> tuple[int, float]:
    try:
        consecutive = int(BROKEN_ALBUM_CONSECUTIVE_THRESHOLD or 2)
    except Exception:
        consecutive = 2
    consecutive = max(1, consecutive)
    try:
        percentage = float(BROKEN_ALBUM_PERCENTAGE_THRESHOLD or 0.20)
    except Exception:
        percentage = 0.20
    percentage = max(0.0, min(1.0, percentage))
    return consecutive, percentage


def _incomplete_policy_is_strict() -> bool:
    consecutive, percentage = _incomplete_policy_thresholds()
    return consecutive <= 1 or percentage <= 0.12


def _incomplete_longest_missing_run(missing_indices: list[int] | None) -> int:
    values = sorted(
        {
            int(v)
            for v in (missing_indices or [])
            if _parse_int_loose(v, 0) > 0
        }
    )
    if not values:
        return 0
    longest = 1
    current = 1
    prev = values[0]
    for value in values[1:]:
        if value == prev + 1:
            current += 1
        else:
            longest = max(longest, current)
            current = 1
        prev = value
    return max(longest, current)


def _incomplete_meets_quarantine_policy(
    *,
    expected: int,
    deficit: int,
    missing_indices: list[int] | None,
) -> bool:
    if deficit <= 0:
        return False
    consecutive_threshold, percentage_threshold = _incomplete_policy_thresholds()
    longest_run = _incomplete_longest_missing_run(missing_indices)
    if longest_run >= consecutive_threshold:
        return True
    if expected > 0 and percentage_threshold > 0.0 and (deficit / expected) >= percentage_threshold:
        return True
    return False


def _incomplete_titles_from_source(source: Any) -> list[str]:
    if isinstance(source, dict):
        for key in ("track_titles", "tracklist", "tracks"):
            value = source.get(key)
            if isinstance(value, list):
                out: list[str] = []
                for item in value:
                    if isinstance(item, dict):
                        title = str(item.get("title") or item.get("name") or "").strip()
                    else:
                        title = str(item or "").strip()
                    if title:
                        out.append(title)
                if out:
                    return out
    return []


def _incomplete_expected_track_titles(edition: dict | None, mb_hint: dict | None = None) -> list[str]:
    e = edition if isinstance(edition, dict) else {}
    for source in (
        mb_hint,
        e.get("_strict_provider_payload"),
        e.get("rg_info"),
        e.get("_lookup_identity_hint"),
    ):
        titles = _incomplete_titles_from_source(source)
        if titles:
            return titles
    return []


def _incomplete_prefix_track_score(local_titles: list[str], expected_titles: list[str]) -> float:
    local = [str(v or "").strip() for v in (local_titles or []) if str(v or "").strip()]
    expected = [str(v or "").strip() for v in (expected_titles or []) if str(v or "").strip()]
    if not local or not expected or len(expected) < len(local):
        return 0.0
    try:
        return float(_crosscheck_tracklist_perfect(local, expected[: len(local)]))
    except Exception:
        return 0.0


def _incomplete_tail_only_missing(
    missing_indices: list[int] | None,
    expected_track_count: int,
    actual_track_count: int,
) -> bool:
    expected = max(0, int(expected_track_count or 0))
    actual = max(0, int(actual_track_count or 0))
    flat = sorted(
        {
            int(v)
            for v in (missing_indices or [])
            if _parse_int_loose(v, 0) > 0
        }
    )
    if expected <= 0 or actual <= 0 or not flat:
        return False
    if expected <= actual:
        return False
    return flat == list(range(actual + 1, expected + 1))


def _incomplete_title_has_variant_cue(value: str | None) -> bool:
    raw = _norm_track_title_strict(value)
    if not raw:
        return False
    if any(marker in raw for marker in _DUPE_EDITION_MARKERS):
        return True
    return any(marker in raw for marker in _INCOMPLETE_BONUS_TRACK_MARKERS)


def _incomplete_bonus_tail_suspected(local_titles: list[str], expected_titles: list[str]) -> bool:
    local = [str(v or "").strip() for v in (local_titles or []) if str(v or "").strip()]
    expected = [str(v or "").strip() for v in (expected_titles or []) if str(v or "").strip()]
    if not local or len(expected) <= len(local):
        return False
    prefix_score = _incomplete_prefix_track_score(local, expected)
    if prefix_score < 0.90:
        return False
    tail = expected[len(local):]
    if not tail:
        return False
    marked = sum(1 for title in tail if _incomplete_title_has_variant_cue(title))
    if marked >= max(1, int(len(tail) * 0.5)):
        return True
    if len(tail) <= 2 and prefix_score >= 0.98:
        return True
    return False


def _build_incomplete_assessment(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._build_incomplete_assessment_for_runtime(sys.modules[__name__], *args, **kwargs)


_INCOMPLETE_AI_PROMPT_VERSION = "incomplete-shadow-v2"
_INCOMPLETE_AI_FAST_TIMEOUT_SEC = 45
_INCOMPLETE_AI_HARD_TIMEOUT_SEC = 90
_INCOMPLETE_AI_MAX_TRACK_ROWS = 30
_INCOMPLETE_AI_ALLOWED_CONFLICT_TYPES = (
    "tracklist_deficit",
    "edition_conflict",
    "identity_conflict",
    "numbering_conflict",
    "insufficient_evidence",
)
_INCOMPLETE_AI_ALLOWED_VERDICTS = (
    _INCOMPLETE_VERDICT_CONFIRMED,
    _INCOMPLETE_VERDICT_REVIEW,
    _INCOMPLETE_VERDICT_ALT_EDITION,
    _INCOMPLETE_VERDICT_IDENTITY_MISMATCH,
    _INCOMPLETE_VERDICT_NUMBERING,
    _INCOMPLETE_VERDICT_MANUAL,
)
_INCOMPLETE_AI_SHADOW_VERDICTS = {
    _INCOMPLETE_VERDICT_REVIEW,
    _INCOMPLETE_VERDICT_ALT_EDITION,
    _INCOMPLETE_VERDICT_IDENTITY_MISMATCH,
    _INCOMPLETE_VERDICT_NUMBERING,
    _INCOMPLETE_VERDICT_MANUAL,
}


def _incomplete_ai_model_sequence(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_model_sequence_for_runtime(sys.modules[__name__], *args, **kwargs)


def _incomplete_ai_enabled(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_enabled_for_runtime(sys.modules[__name__], *args, **kwargs)


def _incomplete_ai_should_consider(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_should_consider_for_runtime(sys.modules[__name__], *args, **kwargs)


def _incomplete_ai_track_rows(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_track_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def _incomplete_ai_diff_summary(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_diff_summary_for_runtime(sys.modules[__name__], *args, **kwargs)


def _build_incomplete_ai_evidence(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._build_incomplete_ai_evidence_for_runtime(sys.modules[__name__], *args, **kwargs)


def _incomplete_ai_response_schema(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_response_schema_for_runtime(sys.modules[__name__], *args, **kwargs)


def _incomplete_ai_conflict_schema(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_conflict_schema_for_runtime(sys.modules[__name__], *args, **kwargs)


def _incomplete_ai_allowed_verdicts_for_conflict_type(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_allowed_verdicts_for_conflict_type_for_runtime(sys.modules[__name__], *args, **kwargs)


def _build_incomplete_ai_conflict_payload(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._build_incomplete_ai_conflict_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


def _build_incomplete_ai_verdict_payload(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._build_incomplete_ai_verdict_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


def _normalize_incomplete_ai_conflict(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._normalize_incomplete_ai_conflict_for_runtime(sys.modules[__name__], *args, **kwargs)


def _normalize_incomplete_ai_verdict(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._normalize_incomplete_ai_verdict_for_runtime(sys.modules[__name__], *args, **kwargs)


def _incomplete_ai_should_retry_hard(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_should_retry_hard_for_runtime(sys.modules[__name__], *args, **kwargs)


def _incomplete_ai_should_retry_conflict_hard(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_should_retry_conflict_hard_for_runtime(sys.modules[__name__], *args, **kwargs)


def _run_incomplete_ai_stage(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._run_incomplete_ai_stage_for_runtime(sys.modules[__name__], *args, **kwargs)


def _incomplete_ai_stage_fallback_verdict(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_stage_fallback_verdict_for_runtime(sys.modules[__name__], *args, **kwargs)


def _run_incomplete_ai_shadow_verdict(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._run_incomplete_ai_shadow_verdict_for_runtime(sys.modules[__name__], *args, **kwargs)


def _incomplete_ai_prewarm_sequence(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_prewarm_sequence_for_runtime(sys.modules[__name__], *args, **kwargs)


def _incomplete_ai_review_status_snapshot(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._incomplete_ai_review_status_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)


def _run_incomplete_ai_review_worker(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._run_incomplete_ai_review_worker_for_runtime(sys.modules[__name__], *args, **kwargs)


def _trigger_incomplete_ai_review_async(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._trigger_incomplete_ai_review_async_for_runtime(sys.modules[__name__], *args, **kwargs)


def _detect_gaps_in_indices(indices: list) -> tuple[bool, int, list]:
    """
    Given sorted track indices, detect if album has gaps (incomplete).
    Returns (is_broken, actual_count, gaps) where gaps is list of (start, end) pairs.
    Conservative local-only rule: only internal numbering holes count.
    """
    if not indices:
        return False, 0, []
    track_indices = sorted([int(i) for i in indices if int(i) > 0])
    if not track_indices:
        return False, 0, []
    actual_count = len(track_indices)
    if actual_count < 2:
        return False, actual_count, []
    gaps = []
    for i in range(len(track_indices) - 1):
        if track_indices[i + 1] - track_indices[i] > 1:
            gaps.append((track_indices[i], track_indices[i + 1]))
    if not gaps:
        return False, actual_count, []
    # Any gap is considered incomplete when indices are sane (caller ensures this).
    return True, actual_count, gaps


def _classical_gap_anomaly_should_be_ignored(
    tags: dict | None,
    *,
    actual_count: int,
    max_idx: int,
    gaps: list[tuple[int, int]] | None,
) -> bool:
    return _classical_runtime._classical_gap_anomaly_should_be_ignored_for_runtime(
        sys.modules[__name__],
        tags,
        actual_count=actual_count,
        max_idx=max_idx,
        gaps=gaps,
    )


def _missing_indices_to_gap_pairs(values: list[int] | None) -> list[tuple[int, int]]:
    missing_flat: list[int] = []
    for value in list(values or []):
        try:
            parsed = int(value)
        except Exception:
            continue
        if parsed > 0:
            missing_flat.append(parsed)
    if not missing_flat:
        return []
    missing_flat = sorted(set(missing_flat))
    gaps: list[tuple[int, int]] = []
    start = prev = missing_flat[0]
    for value in missing_flat[1:]:
        if value == prev + 1:
            prev = value
            continue
        gaps.append((start - 1, prev + 1))
        start = prev = value
    gaps.append((start - 1, prev + 1))
    return gaps


def _edition_exact_expected_track_count(edition: dict | None, mb_hint: dict | None = None) -> int:
    e = edition if isinstance(edition, dict) else {}
    for source in (mb_hint, e.get("rg_info"), e.get("_strict_provider_payload"), e.get("_lookup_identity_hint")):
        if not isinstance(source, dict):
            continue
        try:
            count = int(source.get("track_count") or 0)
        except Exception:
            count = 0
        if count > 0:
            return count
        for key in ("track_titles", "tracklist", "tracks"):
            value = source.get(key)
            if isinstance(value, list) and value:
                return len(value)
    try:
        count = int(e.get("_expected_track_count") or 0)
    except Exception:
        count = 0
    return max(0, count)


def _edition_missing_indices_exact(edition: dict | None, expected_count: int, actual_count: int) -> list[int]:
    expected = max(0, int(expected_count or 0))
    actual = max(0, int(actual_count or 0))
    if expected <= 0:
        return []
    e = edition if isinstance(edition, dict) else {}
    seen: set[int] = set()
    for track in (e.get("tracks") or []):
        try:
            if isinstance(track, dict):
                idx = int(track.get("idx") or track.get("index") or track.get("track_num") or track.get("track") or 0)
            else:
                idx = int(getattr(track, "idx", 0) or 0)
        except Exception:
            idx = 0
        if idx > 0:
            seen.add(idx)
    if seen and expected <= 500:
        missing = [i for i in range(1, expected + 1) if i not in seen]
        if missing:
            return missing[:500]
    if expected > actual:
        return list(range(actual + 1, min(expected, actual + 500) + 1))
    return []


def _incomplete_album_disk_crosscheck(*args: Any, **kwargs: Any) -> Any:
    return _broken_album_runtime._incomplete_album_disk_crosscheck_for_runtime(sys.modules[__name__], *args, **kwargs)


def resolve_mbid_to_release_group(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime.resolve_mbid_to_release_group_for_runtime(sys.modules[__name__], *args, **kwargs)


def fetch_mb_release_group_info(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime.fetch_mb_release_group_info_for_runtime(sys.modules[__name__], *args, **kwargs)

# ──────────────────────────────── MusicBrainz search fallback ────────────────────────────────
def _extract_track_titles_from_mb_release(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime._extract_track_titles_from_mb_release_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mb_track_count_from_rg_info(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime._mb_track_count_from_rg_info_for_runtime(sys.modules[__name__], *args, **kwargs)


def _is_likely_live_album(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime._is_likely_live_album_for_runtime(sys.modules[__name__], *args, **kwargs)


def _crosscheck_tracklist(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime._crosscheck_tracklist_for_runtime(sys.modules[__name__], *args, **kwargs)


def _crosscheck_tracklist_perfect(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime._crosscheck_tracklist_perfect_for_runtime(sys.modules[__name__], *args, **kwargs)


def _prepare_mb_submission_payload(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime._prepare_mb_submission_payload_for_runtime(sys.modules[__name__], *args, **kwargs)

def _fpcalc_fingerprint_file(*args: Any, **kwargs: Any) -> Any:
    return _audio_runtime._fpcalc_fingerprint_file_for_runtime(sys.modules[__name__], *args, **kwargs)


def _store_acoustid_fingerprints_for_folder(folder: Path, max_tracks: int = 20) -> int:
    """
    Compute and store AcousticID fingerprint + duration for each audio file in folder (up to max_tracks).
    Used during scan so fingerprints are in DB for later lookup. Returns number of files stored/computed.
    """
    if not getattr(sys.modules[__name__], "USE_ACOUSTID", False):
        return 0
    try:
        import acoustid
    except ImportError:
        return 0
    audio_files = sorted([p for p in folder.rglob("*") if AUDIO_RE.search(p.name)])
    if not audio_files:
        return 0
    stored = 0
    for path in audio_files[:max_tracks]:
        path_str = str(path)
        if get_cached_acoustid(path_str):
            continue
        try:
            res = _fpcalc_fingerprint_file(path_str, length_sec=120, timeout_sec=45)
            if not res:
                continue
            duration, fingerprint = res
            set_cached_acoustid(path_str, duration, fingerprint)
            stored += 1
        except Exception:
            pass
    return stored


def _identify_album_by_acoustic_id(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._identify_album_by_acoustic_id_for_runtime(sys.modules[__name__], *args, **kwargs)



def _search_mb_rg_candidates(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime._search_mb_rg_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)


def _prefilter_mb_release_group_candidates(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime._prefilter_mb_release_group_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)


def fetch_all_mb_release_groups_for_artist(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime.fetch_all_mb_release_groups_for_artist_for_runtime(sys.modules[__name__], *args, **kwargs)


def _build_mb_rg_index_for_artist(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime._build_mb_rg_index_for_artist_for_runtime(sys.modules[__name__], *args, **kwargs)


def _match_album_norm_to_mb_index(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime._match_album_norm_to_mb_index_for_runtime(sys.modules[__name__], *args, **kwargs)


def _browse_mb_rg_by_artist(*args: Any, **kwargs: Any) -> Any:
    return _musicbrainz_runtime._browse_mb_rg_by_artist_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_album_provider_fallbacks_parallel(*args, **kwargs):
    return _provider_fallback_runtime._fetch_album_provider_fallbacks_parallel_for_runtime(sys.modules[__name__], *args, **kwargs)



def _normalize_identity_text_strict(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._normalize_identity_text_strict_for_runtime(sys.modules[__name__], *args, **kwargs)


def _normalize_identity_album_strict(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._normalize_identity_album_strict_for_runtime(sys.modules[__name__], *args, **kwargs)


def _split_identity_artist_credits(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._split_identity_artist_credits_for_runtime(sys.modules[__name__], *args, **kwargs)


def _strip_identity_artist_feature_clause(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._strip_identity_artist_feature_clause_for_runtime(sys.modules[__name__], *args, **kwargs)


def _identity_artist_credit_norms(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._identity_artist_credit_norms_for_runtime(sys.modules[__name__], *args, **kwargs)


def _identity_norm_is_various_artist(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._identity_norm_is_various_artist_for_runtime(sys.modules[__name__], *args, **kwargs)


def _identity_artist_is_various_artists(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._identity_artist_is_various_artists_for_runtime(sys.modules[__name__], *args, **kwargs)


def _identity_tags_mark_compilation(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._identity_tags_mark_compilation_for_runtime(sys.modules[__name__], *args, **kwargs)


def _identity_artist_credit_overlap(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._identity_artist_credit_overlap_for_runtime(sys.modules[__name__], *args, **kwargs)


def _strip_identity_album_trailing_markers(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._strip_identity_album_trailing_markers_for_runtime(sys.modules[__name__], *args, **kwargs)


def _identity_album_variant_norms(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._identity_album_variant_norms_for_runtime(sys.modules[__name__], *args, **kwargs)


def _identity_album_equivalent(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._identity_album_equivalent_for_runtime(sys.modules[__name__], *args, **kwargs)


def _provider_identity_album_score(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._provider_identity_album_score_for_runtime(sys.modules[__name__], *args, **kwargs)


def _provider_identity_artist_score(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._provider_identity_artist_score_for_runtime(sys.modules[__name__], *args, **kwargs)


# Last.fm integration compatibility wrappers.
def _run_lastfm_preflight(*args, **kwargs):
    return _lastfm_runtime._run_lastfm_preflight_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_credentials_effective(*args, **kwargs):
    return _lastfm_runtime._lastfm_credentials_effective_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_session_name(*args, **kwargs):
    return _lastfm_runtime._lastfm_session_name_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_session_key(*args, **kwargs):
    return _lastfm_runtime._lastfm_session_key_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_pending_token(*args, **kwargs):
    return _lastfm_runtime._lastfm_pending_token_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_has_stored_session_ciphertext(*args, **kwargs):
    return _lastfm_runtime._lastfm_has_stored_session_ciphertext_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_has_stored_pending_ciphertext(*args, **kwargs):
    return _lastfm_runtime._lastfm_has_stored_pending_ciphertext_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_status_snapshot(*args, **kwargs):
    return _lastfm_runtime._lastfm_status_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_try_complete_pending_authorization_if_needed(*args, **kwargs):
    return _lastfm_runtime._lastfm_try_complete_pending_authorization_if_needed_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_store_session_payload(*args, **kwargs):
    return _lastfm_runtime._lastfm_store_session_payload_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_try_complete_pending_authorization(*args, **kwargs):
    return _lastfm_runtime._lastfm_try_complete_pending_authorization_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_scrobble_enabled(*args, **kwargs):
    return _lastfm_runtime._lastfm_scrobble_enabled_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_now_playing_enabled(*args, **kwargs):
    return _lastfm_runtime._lastfm_now_playing_enabled_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_auth_callback_url(*args, **kwargs):
    return _lastfm_runtime._lastfm_auth_callback_url_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_auth_url_for_token(*args, **kwargs):
    return _lastfm_runtime._lastfm_auth_url_for_token_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_callback_html(*args, **kwargs):
    return _lastfm_runtime._lastfm_callback_html_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_api_sig(*args, **kwargs):
    return _lastfm_runtime._lastfm_api_sig_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_signed_post(*args, **kwargs):
    return _lastfm_runtime._lastfm_signed_post_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_get(*args, **kwargs):
    return _lastfm_runtime._lastfm_get_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_playback_track_payload(*args, **kwargs):
    return _lastfm_runtime._lastfm_playback_track_payload_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_loved_sync_setting(*args, **kwargs):
    return _lastfm_runtime._lastfm_loved_sync_setting_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_track_identity_map(*args, **kwargs):
    return _lastfm_runtime._lastfm_track_identity_map_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_set_track_love(*args, **kwargs):
    return _lastfm_runtime._lastfm_set_track_love_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_sync_loved_tracks_to_pmda(*args, **kwargs):
    return _lastfm_runtime._lastfm_sync_loved_tracks_to_pmda_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_scrobble_threshold_seconds(*args, **kwargs):
    return _lastfm_runtime._lastfm_scrobble_threshold_seconds_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_submit_now_playing(*args, **kwargs):
    return _lastfm_runtime._lastfm_submit_now_playing_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_submit_scrobble(*args, **kwargs):
    return _lastfm_runtime._lastfm_submit_scrobble_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_handle_playback_event_async(*args, **kwargs):
    return _lastfm_runtime._lastfm_handle_playback_event_async_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_pick_search_candidate(*args, **kwargs):
    return _lastfm_runtime._lastfm_pick_search_candidate_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_payload_has_album_page(*args, **kwargs):
    return _lastfm_runtime._lastfm_payload_has_album_page_for_runtime(sys.modules[__name__], *args, **kwargs)

def _fetch_lastfm_album_info(*args, **kwargs):
    return _lastfm_runtime._fetch_lastfm_album_info_for_runtime(sys.modules[__name__], *args, **kwargs)

def _cleanup_lastfm_bio_text(*args, **kwargs):
    return _lastfm_runtime._cleanup_lastfm_bio_text_for_runtime(sys.modules[__name__], *args, **kwargs)

def _fetch_lastfm_artist_info(*args, **kwargs):
    return _lastfm_runtime._fetch_lastfm_artist_info_for_runtime(sys.modules[__name__], *args, **kwargs)

def _lastfm_cover_url_candidates(*args, **kwargs):
    return _lastfm_runtime._lastfm_cover_url_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)

def _fetch_artist_image_lastfm(*args, **kwargs):
    return _lastfm_runtime._fetch_artist_image_lastfm_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_lastfm_auth_status(*args, **kwargs):
    return _lastfm_runtime.api_lastfm_auth_status_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_lastfm_auth_start(*args, **kwargs):
    return _lastfm_runtime.api_lastfm_auth_start_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_lastfm_auth_callback(*args, **kwargs):
    return _lastfm_runtime.api_lastfm_auth_callback_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_lastfm_auth_complete(*args, **kwargs):
    return _lastfm_runtime.api_lastfm_auth_complete_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_lastfm_auth_disconnect(*args, **kwargs):
    return _lastfm_runtime.api_lastfm_auth_disconnect_for_runtime(sys.modules[__name__], *args, **kwargs)



def _extract_mb_artist_names(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._extract_mb_artist_names_for_runtime(sys.modules[__name__], *args, **kwargs)


_CLASSICAL_GENRE_HINTS = {
    "classical",
    "baroque",
    "romantic",
    "opera",
    "orchestral",
    "chamber",
    "choral",
    "sacred",
    "symphony",
    "concerto",
}
_CLASSICAL_WORK_KEYWORDS = {
    "symphony",
    "symphonie",
    "sinfonia",
    "sinfonie",
    "concerto",
    "sonata",
    "sonate",
    "suite",
    "requiem",
    "mass",
    "missa",
    "quartet",
    "quintet",
    "sextet",
    "septet",
    "octet",
    "trio",
    "duo",
    "partita",
    "cantata",
    "oratorio",
    "overture",
    "prelude",
    "fugue",
    "mazurka",
    "waltz",
    "etude",
    "étude",
    "nocturne",
    "ballade",
    "impromptu",
    "rhapsody",
    "variation",
    "variations",
    "scherzo",
    "lied",
}
_CLASSICAL_PERFORMANCE_TAG_KEYS = (
    "artist",
    "albumartist",
    "album_artist",
    "album artist",
    "performer",
    "performers",
    "soloist",
    "soloists",
    "orchestra",
    "ensemble",
    "choir",
    "chorus",
    "conductor",
    "arranger",
)
_CLASSICAL_COMPOSER_TAG_KEYS = (
    "composer",
    "composers",
    "composer_sort",
    "composer_sort_name",
    "workcomposer",
)
_CLASSICAL_WORK_TAG_KEYS = (
    "grouping",
    "work",
    "work_title",
    "subtitle",
    "movement",
    "movementname",
)
_CLASSICAL_LABEL_TAG_KEYS = (
    "label",
    "organization",
    "recordlabel",
    "publisher",
)
_CLASSICAL_RELEASE_CATALOG_TAG_KEYS = (
    "catalog_number",
    "catalog_numbers",
    "catalog_no",
    "catalogue_number",
    "catalogue_no",
    "catno",
)
_CLASSICAL_CATALOG_RE = re.compile(
    r"\b(?:op(?:us)?|bwv|kv|k|hob|d|sz|rv|hwv|s|wq|wwv|twv|buxwv|l|p)\s*\.?\s*\d+[a-z0-9\-/:]*\b",
    flags=re.IGNORECASE,
)
_CLASSICAL_SPLIT_RE = re.compile(r"\s*(?:;|/|&|\band\b|\bwith\b|\bfeat\.?\b|\bfeaturing\b)\s*", flags=re.IGNORECASE)
_CLASSICAL_GENERIC_VALUE_TOKENS = {
    "0",
    "false",
    "na",
    "n/a",
    "none",
    "null",
    "off",
    "unknown",
}
_CLASSICAL_RELEASE_CATALOG_PATTERNS = (
    re.compile(r"\b[A-Z]{1,6}\s*\d(?:[\s./-]?\d){2,}\b"),
    re.compile(r"\b\d{1,4}(?:[\s./-]\d{1,4}){1,4}\b"),
)
_CLASSICAL_LABEL_OCR_ALIASES = {
    "Deutsche Grammophon": ("deutsche grammophon", "deutsche grammophone", "dgg"),
    "His Master's Voice": ("his master's voice", "his masters voice", "hmv", "la voix de son maitre"),
    "Philips": ("philips",),
    "Philips Classics": ("philips classics", "philips"),
    "Erato": ("erato",),
    "Warner Classics": ("warner classics", "warner",),
    "EMI Classics": ("emi classics", "emi",),
    "Decca Classics": ("decca classics", "decca"),
    "Sony Classical": ("sony classical", "sony music classical", "sony music", "sony classical international"),
    "ATMA Classique": ("atma classique", "atma"),
    "audite Musikproduktion": ("audite musikproduktion", "audite"),
    "Archiv Produktion": ("archiv produktion", "archiv"),
    "Harmonia Mundi": ("harmonia mundi",),
    "Naxos": ("naxos",),
    "Chandos": ("chandos",),
    "BIS": ("bis", "bis records"),
    "Hyperion": ("hyperion",),
    "Teldec": ("teldec",),
    "Virgin Classics": ("virgin classics", "virgin"),
    "RCA Red Seal": ("rca red seal", "red seal"),
    "ECM New Series": ("ecm new series", "ecm"),
    "Alpha Classics": ("alpha classics", "alpha"),
    "Naive Classique": ("naive classique", "naive"),
}
_CLASSICAL_PERSON_SORT_KEY_NORMALS = {
    "composersort",
    "composersortname",
    "conductorsort",
    "artistsort",
    "albumartistsort",
    "performernamesort",
    "soloistsort",
}
_CLASSICAL_NON_COMPOSER_NAME_TOKENS = {
    "alto",
    "baritone",
    "bass",
    "cello",
    "celloist",
    "choir",
    "chorus",
    "conductor",
    "contralto",
    "ensemble",
    "mezzo",
    "mezzo soprano",
    "orchestra",
    "performer",
    "philharmonic",
    "piano",
    "pianist",
    "project",
    "quartet",
    "quintet",
    "soprano",
    "tenor",
    "the",
    "trio",
    "viola",
    "violin",
    "violinist",
}


def _classical_norm_text(value: str | None) -> str:
    return _normalize_identity_text_strict(value)


def _classical_key_norm(key: str | None) -> str:
    return re.sub(r"[\s_]+", "", str(key or "").strip().lower())


def _classical_sort_name_to_display(value: str | None) -> str:
    txt = re.sub(r"\s+", " ", str(value or "").strip(" -–—"))
    if not txt or "," not in txt:
        return txt
    parts = [p.strip() for p in txt.split(",") if p.strip()]
    if len(parts) < 2:
        return txt
    surname = parts[0]
    rest = parts[1:]
    prefix_tokens: list[str] = []
    given_tokens: list[str] = []
    suffix_tokens: list[str] = []
    suffix_norms = {"jr", "sr", "ii", "iii", "iv"}
    for token in rest:
        norm = _classical_norm_text(token)
        if norm in _CLASSICAL_PERSON_NAME_HONORIFICS:
            prefix_tokens.append(token)
        elif norm in suffix_norms:
            suffix_tokens.append(token)
        else:
            given_tokens.append(token)
    reordered = " ".join([*prefix_tokens, *given_tokens, surname, *suffix_tokens]).strip()
    return reordered or txt


def _classical_split_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = [value]
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_items:
        txt = html.unescape(str(raw or "")).strip()
        if not txt:
            continue
        parts = [p.strip() for p in _CLASSICAL_SPLIT_RE.split(txt) if str(p or "").strip()]
        if not parts:
            parts = [txt]
        for part in parts:
            cleaned = re.sub(r"\s+", " ", part).strip(" -–—")
            if not cleaned:
                continue
            key = _classical_norm_text(cleaned)
            if not key or key in _CLASSICAL_GENERIC_VALUE_TOKENS:
                continue
            if key and key not in seen:
                out.append(cleaned)
                seen.add(key)
    return out


def _classical_tag_override(tags: dict | None) -> bool | None:
    if not isinstance(tags, dict):
        return None
    lowered = {str(k or "").strip().lower(): v for k, v in tags.items()}
    for raw_key in ("is_classical", "classical"):
        for cand in {
            raw_key,
            raw_key.replace("_", ""),
            raw_key.replace("_", " "),
        }:
            raw = lowered.get(cand)
            if raw is None:
                continue
            txt = str(raw or "").strip().lower()
            if not txt:
                continue
            if txt in {"1", "true", "yes", "y", "on"}:
                return True
            if txt in {"0", "false", "no", "n", "off"}:
                return False
    return None


def _classical_collect_tag_values(tags: dict | None, keys: tuple[str, ...]) -> list[str]:
    if not isinstance(tags, dict):
        return []
    out: list[str] = []
    seen: set[str] = set()
    lowered = {str(k or "").strip().lower(): v for k, v in tags.items()}
    for key in keys:
        key_norm = _classical_key_norm(key)
        for cand in {
            key,
            key.replace(" ", ""),
            key.replace(" ", "_"),
            key.replace("_", ""),
            key.replace("_", " "),
        }:
            raw = lowered.get(cand)
            raw_values = raw if isinstance(raw, (list, tuple, set)) else [raw]
            normalized_values: list[str] = []
            for raw_value in raw_values:
                txt = html.unescape(str(raw_value or "")).strip()
                if not txt:
                    continue
                if key_norm in _CLASSICAL_PERSON_SORT_KEY_NORMALS:
                    txt = _classical_sort_name_to_display(txt)
                normalized_values.append(txt)
            for value in _classical_split_values(normalized_values):
                norm = _classical_norm_text(value)
                if norm and norm not in seen:
                    out.append(value)
                    seen.add(norm)
    return out


def _classical_display_values(values: list[str], *, limit: int = 6) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        clean = re.sub(r"\s+", " ", str(value or "").strip(" -–—"))
        if not clean:
            continue
        key = _classical_norm_text(clean)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(clean)
        if len(out) >= limit:
            break
    return out


def _classical_title_composer_values(title: str) -> list[str]:
    raw_title = re.sub(r"\s+", " ", str(title or "").strip(" -–—"))
    if not raw_title or ":" not in raw_title:
        return []
    prefix = raw_title.split(":", 1)[0].strip(" -–—")
    if not prefix:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for raw_part in re.split(r"\s*(?:;|/|&|\band\b)\s*", prefix, flags=re.IGNORECASE):
        candidate = re.sub(r"\s+", " ", str(raw_part or "").strip(" -–—"))
        candidate = re.sub(r"^[\"'“”‘’]+|[\"'“”‘’]+$", "", candidate).strip()
        if not candidate:
            continue
        norm = _classical_norm_text(candidate)
        if not norm or norm in seen:
            continue
        tokens = [tok for tok in re.findall(r"[a-z0-9]+", norm) if tok]
        if not tokens or len(tokens) > 5:
            continue
        if any(tok in _CLASSICAL_NON_COMPOSER_NAME_TOKENS for tok in tokens):
            continue
        if len(tokens) == 1 and len(tokens[0]) < 4:
            continue
        seen.add(norm)
        out.append(candidate)
    return out


def _classical_preferred_title_composers(
    *,
    title_values: list[str] | tuple[str, ...] | None,
    tag_values: list[str] | tuple[str, ...] | None,
) -> list[str]:
    return _classical_runtime._classical_preferred_title_composers_for_runtime(
        sys.modules[__name__],
        title_values=title_values,
        tag_values=tag_values,
    )


def _classical_genre_signal(values: list[str] | tuple[str, ...] | None) -> bool:
    genre_norms = {_classical_norm_text(v) for v in (values or []) if _classical_norm_text(v)}
    return bool(genre_norms and any(any(hint in genre for hint in _CLASSICAL_GENRE_HINTS) for genre in genre_norms))


def _classical_title_signal(title: str, *, work_tokens: set[str] | None = None, track_titles: list[str] | None = None) -> bool:
    title_txt = str(title or "").strip()
    if work_tokens:
        return True
    title_norm = _classical_norm_text(title_txt)
    if any(keyword in title_norm for keyword in _CLASSICAL_WORK_KEYWORDS):
        return True
    if _CLASSICAL_CATALOG_RE.search(title_txt):
        return True
    joined_tracks = " ".join([str(item or "").strip() for item in (track_titles or [])[:12] if str(item or "").strip()])
    return bool(joined_tracks and _CLASSICAL_CATALOG_RE.search(joined_tracks))


def _classical_has_explicit_signal(
    *,
    title: str = "",
    track_titles: list[str] | None = None,
    genre_values: list[str] | tuple[str, ...] | None = None,
    composer_values: list[str] | tuple[str, ...] | None = None,
    work_values: list[str] | tuple[str, ...] | None = None,
    conductor_values: list[str] | tuple[str, ...] | None = None,
    orchestra_values: list[str] | tuple[str, ...] | None = None,
    ensemble_values: list[str] | tuple[str, ...] | None = None,
    soloist_values: list[str] | tuple[str, ...] | None = None,
    performer_values: list[str] | tuple[str, ...] | None = None,
    catalog_values: list[str] | tuple[str, ...] | None = None,
) -> bool:
    return _classical_runtime._classical_has_explicit_signal_for_runtime(
        sys.modules[__name__],
        title=title,
        track_titles=track_titles,
        genre_values=genre_values,
        composer_values=composer_values,
        work_values=work_values,
        conductor_values=conductor_values,
        orchestra_values=orchestra_values,
        ensemble_values=ensemble_values,
        soloist_values=soloist_values,
        performer_values=performer_values,
        catalog_values=catalog_values,
    )


def _should_run_profile_enrichment_inline(albums_count: int) -> bool:
    try:
        total = max(0, int(albums_count or 0))
    except Exception:
        total = 0
    if total <= 0:
        return False
    return total <= 24


def _classical_display_payload(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._classical_display_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_collect_album_classical_payload(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._files_collect_album_classical_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_album_is_classical_like_for_browse(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._files_album_is_classical_like_for_browse_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_classical_composer_values(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._files_classical_composer_values_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_album_linked_composer_names_map(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._files_album_linked_composer_names_map_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_apply_canonical_composers_to_classical_payload(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._files_apply_canonical_composers_to_classical_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_album_display_artist_name(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._files_album_display_artist_name_for_runtime(sys.modules[__name__], *args, **kwargs)


_FILES_BROWSE_PRIMARY_ROLES = {"artist"}
# Generic "artist" roles must not flow through the classical-person alias/merge path.
# That path is only for actual people (composer/conductor/soloist/performer).
_FILES_BROWSE_PERSON_ROLES = {"composer", "conductor", "soloist", "performer"}
_FILES_BROWSE_ENSEMBLE_ROLES = {"orchestra", "ensemble"}
_FILES_BROWSE_ROLE_PRIORITY = {
    "artist": 0,
    "featured": 1,
    "appearance": 2,
    "composer": 3,
    "conductor": 4,
    "orchestra": 5,
    "ensemble": 6,
    "soloist": 7,
    "performer": 8,
}
_CLASSICAL_PERSON_NAME_PARTICLES = {
    "de",
    "del",
    "della",
    "der",
    "di",
    "du",
    "la",
    "le",
    "van",
    "von",
}
_CLASSICAL_PERSON_NAME_HONORIFICS = {
    "sir",
    "dame",
    "maestro",
    "master",
    "prof",
    "professor",
    "dr",
    "doctor",
}
_CLASSICAL_PERSON_GIVEN_NAME_VARIANTS = {
    "alexander": {"alexander", "alexandre", "aleksandr"},
    "alexandre": {"alexander", "alexandre", "aleksandr"},
    "aleksandr": {"alexander", "alexandre", "aleksandr"},
    "ilyich": {"ilyich", "ilich", "ilytch", "ilyitch", "ilitch"},
    "ilich": {"ilyich", "ilich", "ilytch", "ilyitch", "ilitch"},
    "ilitch": {"ilyich", "ilich", "ilytch", "ilyitch", "ilitch"},
    "jean": {"jean", "johann", "johan"},
    "johan": {"jean", "johann", "johan"},
    "johann": {"jean", "johann", "johan"},
    "josef": {"josef", "joseph"},
    "joseph": {"josef", "joseph"},
    "peter": {"peter", "pyotr", "piotr", "petr"},
    "petr": {"peter", "pyotr", "piotr", "petr"},
    "piotr": {"peter", "pyotr", "piotr", "petr"},
    "pyotr": {"peter", "pyotr", "piotr", "petr"},
    "sebastian": {"sebastian", "sebastien"},
    "sebastien": {"sebastian", "sebastien"},
}
_CLASSICAL_PERSON_FULLNAME_VARIANTS = {
    ("bach", ("johann", "sebastian")): {
        "Johann Sebastian Bach",
        "Jean Sebastien Bach",
        "Jean-Sebastien Bach",
        "J. S. Bach",
        "JS Bach",
    },
    ("tchaikovsky", ("peter", "ilyich")): {
        "Peter Ilyich Tchaikovsky",
        "Pyotr Ilyich Tchaikovsky",
        "Piotr Ilyich Tchaikovsky",
        "P. I. Tchaikovsky",
        "PI Tchaikovsky",
    },
}


def _files_browse_entity_kind_from_roles(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._files_browse_entity_kind_from_roles_for_runtime(sys.modules[__name__], *args, **kwargs)


def _classical_person_generated_aliases(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._classical_person_generated_aliases_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_identity_primary_lookup_name(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._artist_identity_primary_lookup_name_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_identity_lookup_names(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    candidate_names: list[str] | tuple[str, ...] | None = None,
    limit: int = 12,
) -> list[str]:
    augmented_candidates: list[str] = []
    seen_augmented: set[str] = set()

    def _push_candidate(value: str) -> None:
        clean = " ".join(str(value or "").split()).strip()
        norm = _norm_artist_key(clean)
        if not clean or not norm or norm in seen_augmented:
            return
        seen_augmented.add(norm)
        augmented_candidates.append(clean)

    _push_candidate(artist_name)
    for raw in candidate_names or []:
        _push_candidate(str(raw or ""))

    classical_like = _artist_entity_is_classical_like(entity_kind=entity_kind, role_hints=role_hints)
    if classical_like:
        try:
            mb_identity = _musicbrainz_artist_identity_lookup(
                artist_name,
                entity_kind=entity_kind,
                role_hints=role_hints,
            ) or {}
        except Exception:
            mb_identity = {}
        if isinstance(mb_identity, dict):
            _push_candidate(str(mb_identity.get("name") or ""))
            _push_candidate(str(mb_identity.get("sort_name") or ""))
            for alias in (mb_identity.get("aliases") or []):
                _push_candidate(str(alias or ""))

    primary_lookup_name = _artist_identity_primary_lookup_name(
        artist_name,
        entity_kind=entity_kind,
        role_hints=role_hints,
        candidate_names=augmented_candidates,
    )
    lookup_names = _artist_image_lookup_candidates(
        primary_lookup_name,
        augmented_candidates,
        entity_kind=entity_kind,
        role_hints=role_hints,
        limit=max(1, int(limit or 12)),
    ) or [primary_lookup_name or artist_name]
    return lookup_names


def _artist_profile_text_matches_any_identity(
    artist_name: str,
    text: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    candidate_names: list[str] | tuple[str, ...] | None = None,
) -> bool:
    txt = str(text or "").strip()
    if not txt:
        return False
    for candidate in _artist_identity_lookup_names(
        artist_name,
        entity_kind=entity_kind,
        role_hints=role_hints,
        candidate_names=candidate_names,
        limit=12,
    ):
        if _is_relevant_artist_profile_text(candidate, txt):
            return True
    return False


def _artist_image_page_identity_candidate(value: str) -> str:
    txt = " ".join(str(value or "").replace("_", " ").split()).strip()
    if not txt:
        return ""
    txt = re.sub(r"^[Ff]ile:\s*", "", txt).strip()
    txt = re.sub(r"\.(jpg|jpeg|png|webp|gif|tif|tiff|pdf|djvu|svg)$", "", txt, flags=re.IGNORECASE).strip()
    txt = re.sub(r"\bby\s+[A-Z][A-Za-zÀ-ÿ'._-]+(?:\s+[A-Z][A-Za-zÀ-ÿ'._-]+){0,3}\s*$", "", txt).strip()
    txt = re.sub(r"\bportrait\s+of\s+", "", txt, flags=re.IGNORECASE).strip()
    txt = re.sub(r"\s*\([^)]*\)\s*$", "", txt).strip()
    return " ".join(txt.split()).strip(" -–—")


def _classical_person_alias_signature(name: str) -> dict[str, Any]:
    return _classical_runtime._classical_person_alias_signature_for_runtime(sys.modules[__name__], name)


def _classical_person_names_equivalent(left: str, right: str) -> bool:
    return _classical_runtime._classical_person_names_equivalent_for_runtime(sys.modules[__name__], left, right)


def _classical_person_given_tokens_close(left: str, right: str) -> bool:
    return _classical_runtime._classical_person_given_tokens_close_for_runtime(sys.modules[__name__], left, right)


_CLASSICAL_PERSON_ENGLISH_GIVEN_NAMES = {
    "alexander", "andrew", "anthony", "arthur", "charles", "claude", "edward", "george",
    "gregory", "henry", "ivan", "jack", "jacob", "james", "jean", "jerome", "john",
    "joseph", "jules", "lawrence", "leo", "leonard", "louis", "mark", "michael",
    "nicholas", "peter", "paul", "philip", "richard", "robert", "samuel", "stephen",
    "thomas", "victor", "william",
}


def _classical_person_name_looks_english(value: str) -> bool:
    return _classical_runtime._classical_person_name_looks_english_for_runtime(sys.modules[__name__], value)


def _identity_case_quality_score(value: str) -> tuple[int, int, int]:
    return _classical_runtime._identity_case_quality_score_for_runtime(sys.modules[__name__], value)


def _classical_person_display_preference_score(
    value: str,
    *,
    preference: str | None = None,
    primary: bool = False,
    current: bool = False,
) -> tuple[int, int, int, int, int, int, int, int]:
    return _classical_runtime._classical_person_display_preference_score_for_runtime(
        sys.modules[__name__],
        value,
        preference=preference,
        primary=primary,
        current=current,
    )


def _collapse_classical_person_aliases(
    values: list[str] | tuple[str, ...] | None,
    *,
    preference: str | None = None,
) -> list[str]:
    return _classical_runtime._collapse_classical_person_aliases_for_runtime(
        sys.modules[__name__],
        values,
        preference=preference,
    )


def _select_classical_person_display_name(
    *,
    current_name: str = "",
    primary_name: str = "",
    aliases: list[str] | tuple[str, ...] | None = None,
    preference: str | None = None,
) -> str:
    return _classical_runtime._select_classical_person_display_name_for_runtime(
        sys.modules[__name__],
        current_name=current_name,
        primary_name=primary_name,
        aliases=aliases,
        preference=preference,
    )


def _choose_preferred_person_identity_name(current_value: str, candidate_value: str) -> str:
    return _classical_runtime._choose_preferred_person_identity_name_for_runtime(
        sys.modules[__name__],
        current_value,
        candidate_value,
    )


def _classical_person_signature_key(name: str) -> str:
    return _classical_runtime._classical_person_signature_key_for_runtime(sys.modules[__name__], name)


def _artist_role_hints_from_roles_json(roles_json: Any) -> list[str]:
    return _classical_runtime._artist_role_hints_from_roles_json_for_runtime(sys.modules[__name__], roles_json)


def _artist_is_person_like(*, entity_kind: str = "", role_hints: list[str] | tuple[str, ...] | None = None) -> bool:
    return _classical_runtime._artist_is_person_like_for_runtime(
        sys.modules[__name__],
        entity_kind=entity_kind,
        role_hints=role_hints,
    )



def _library_artist_display_name(*args: Any, **kwargs: Any) -> Any:
    return _artist_identity_runtime._library_artist_display_name_for_runtime(sys.modules[__name__], *args, **kwargs)


def _musicbrainz_artist_identity_lookup_cached(*args: Any, **kwargs: Any) -> Any:
    return _artist_identity_runtime._musicbrainz_artist_identity_lookup_cached_for_runtime(sys.modules[__name__], *args, **kwargs)


def _musicbrainz_artist_identity_lookup(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._musicbrainz_artist_identity_lookup_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_merge_artist_alias_values(*args: Any, **kwargs: Any) -> Any:
    return _artist_identity_runtime._files_merge_artist_alias_values_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_upsert_artist_canonical_identity(*args: Any, **kwargs: Any) -> Any:
    return _artist_identity_runtime._files_upsert_artist_canonical_identity_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_artist_alias_rows_for_identity(*args: Any, **kwargs: Any) -> Any:
    return _artist_identity_runtime._files_artist_alias_rows_for_identity_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_sync_artist_aliases(*args: Any, **kwargs: Any) -> Any:
    return _artist_identity_runtime._files_sync_artist_aliases_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_backfill_artist_alias_table(*args: Any, **kwargs: Any) -> Any:
    return _artist_identity_runtime._files_backfill_artist_alias_table_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_best_person_entity_kind(*args: Any, **kwargs: Any) -> Any:
    return _artist_identity_runtime._files_best_person_entity_kind_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_merge_artist_album_links_to_winner(*args: Any, **kwargs: Any) -> Any:
    return _artist_identity_runtime._files_merge_artist_album_links_to_winner_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_merge_duplicate_person_artists(*args, **kwargs):
    return _artist_merge_runtime._files_merge_duplicate_person_artists_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_get_artist_alias_candidates(*args: Any, **kwargs: Any) -> Any:
    return _artist_identity_runtime._files_get_artist_alias_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_upsert_artist_external_aliases(*args: Any, **kwargs: Any) -> Any:
    return _artist_identity_runtime._files_upsert_artist_external_aliases_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_album_track_tag_dicts(album: dict[str, Any] | None) -> list[dict[str, Any]]:
    payload = album if isinstance(album, dict) else {}
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _push(raw_value: Any) -> None:
        if isinstance(raw_value, dict):
            data = raw_value
        else:
            text = str(raw_value or "").strip()
            if not text:
                return
            try:
                data = json.loads(text)
            except Exception:
                return
        if not isinstance(data, dict):
            return
        try:
            key = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)
        except Exception:
            key = str(data)
        if key in seen:
            return
        seen.add(key)
        out.append(data)

    for track in (payload.get("tracks") or []):
        if not isinstance(track, dict):
            continue
        _push(track.get("primary_tags_json"))
    for raw_value in (payload.get("track_primary_tags_jsons") or []):
        _push(raw_value)
    return out


def _files_split_artist_credit_entities(*args: Any, **kwargs: Any) -> Any:
    return _artist_browse_runtime._files_split_artist_credit_entities_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_collect_nonclassical_album_artist_entities(*args: Any, **kwargs: Any) -> Any:
    return _artist_browse_runtime._files_collect_nonclassical_album_artist_entities_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_collect_track_contributor_entities(*args: Any, **kwargs: Any) -> Any:
    return _artist_browse_runtime._files_collect_track_contributor_entities_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_extract_browse_entities_for_album(*args: Any, **kwargs: Any) -> Any:
    return _artist_browse_runtime._files_extract_browse_entities_for_album_for_runtime(sys.modules[__name__], *args, **kwargs)


def _build_files_browse_artist_entities(*args: Any, **kwargs: Any) -> Any:
    return _artist_browse_runtime._build_files_browse_artist_entities_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ensure_files_album_primary_links(*args: Any, **kwargs: Any) -> Any:
    return _artist_browse_runtime._ensure_files_album_primary_links_for_runtime(sys.modules[__name__], *args, **kwargs)


def _dedupe_files_artist_album_link_rows(*args: Any, **kwargs: Any) -> Any:
    return _artist_browse_runtime._dedupe_files_artist_album_link_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def _classical_label_tokens(values: list[str]) -> set[str]:
    out: set[str] = set()
    for value in values or []:
        norm = _classical_norm_text(value)
        if not norm:
            continue
        out.add(norm[:120])
        for canonical, aliases in _CLASSICAL_LABEL_OCR_ALIASES.items():
            alias_norms = [_classical_norm_text(alias) for alias in aliases]
            if any(alias and alias in norm for alias in alias_norms):
                out.add(_classical_norm_text(canonical))
    return out


def _classical_normalize_release_catalog_token(raw_value: str) -> str:
    token = re.sub(r"[^A-Z0-9]", "", str(raw_value or "").upper())
    if len(token) < 4:
        return ""
    if token.isdigit() and len(token) == 4:
        return ""
    return token[:40]


def _classical_release_catalog_tokens_from_texts(texts: list[str]) -> set[str]:
    out: set[str] = set()
    for raw_text in texts or []:
        raw = str(raw_text or "").strip()
        if not raw:
            continue
        for pattern in _CLASSICAL_RELEASE_CATALOG_PATTERNS:
            for match in pattern.finditer(raw.upper()):
                token = _classical_normalize_release_catalog_token(match.group(0))
                if token:
                    out.add(token)
    return out


def _classical_cover_ocr_context(
    *,
    local_paths: list[Any] | None = None,
    title_hint: str = "",
) -> dict[str, Any]:
    return _identity_cover_ocr_context(
        local_title=title_hint,
        local_paths=list(local_paths or []),
        provider_candidate_count=1,
    )


def _classical_work_tokens_from_texts(texts: list[str]) -> set[str]:
    return _classical_runtime._classical_work_tokens_from_texts_for_runtime(sys.modules[__name__], texts)


def _classical_people_tokens(values: list[str]) -> set[str]:
    out: set[str] = set()
    ensemble_hints = {"orchestra", "philharmonic", "ensemble", "choir", "chorus", "quartet", "quintet", "trio", "symphonieorchester", "symphony"}
    for value in values or []:
        norm = _classical_norm_text(value)
        if not norm:
            continue
        if len(norm) >= 3:
            out.add(norm[:120])
            parts = [p for p in norm.split() if p]
            if len(parts) >= 2 and not any(p in ensemble_hints for p in parts):
                surname = parts[-1]
                if len(surname) >= 4:
                    out.add(surname[:80])
    return out


def _classical_track_entries(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._classical_track_entries_for_runtime(sys.modules[__name__], *args, **kwargs)



def _classical_total_duration_ms_for_paths(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._classical_total_duration_ms_for_paths_for_runtime(sys.modules[__name__], *args, **kwargs)



def _classical_identity_context(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._classical_identity_context_for_runtime(sys.modules[__name__], *args, **kwargs)



def _provider_classical_context(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._provider_classical_context_for_runtime(sys.modules[__name__], *args, **kwargs)



def _classical_context_for_edition(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._classical_context_for_edition_for_runtime(sys.modules[__name__], *args, **kwargs)



def _classical_track_title_set_for_edition(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._classical_track_title_set_for_edition_for_runtime(sys.modules[__name__], *args, **kwargs)


def _classical_same_recording_pair_details(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._classical_same_recording_pair_details_for_runtime(sys.modules[__name__], *args, **kwargs)


def _classical_cluster_same_recording(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._classical_cluster_same_recording_for_runtime(sys.modules[__name__], *args, **kwargs)


def _classical_group_is_same_recording_confident(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._classical_group_is_same_recording_confident_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mark_classical_sibling_incompletes(*args: Any, **kwargs: Any) -> Any:
    return _classical_runtime._mark_classical_sibling_incompletes_for_runtime(sys.modules[__name__], *args, **kwargs)


def _classical_identity_match_details(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._classical_identity_match_details_for_runtime(sys.modules[__name__], *args, **kwargs)



def _strict_identity_match_details(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._strict_identity_match_details_for_runtime(sys.modules[__name__], *args, **kwargs)



def _provider_identity_text_score(*args, **kwargs):
    return _provider_identity_runtime._provider_identity_text_score_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_reject_code(*args, **kwargs):
    return _provider_identity_runtime._strict_reject_code_for_runtime(sys.modules[__name__], *args, **kwargs)

def _norm_track_title_strict(*args, **kwargs):
    return _provider_identity_runtime._norm_track_title_strict_for_runtime(sys.modules[__name__], *args, **kwargs)

def _local_track_titles_for_strict(*args, **kwargs):
    return _provider_identity_runtime._local_track_titles_for_strict_for_runtime(sys.modules[__name__], *args, **kwargs)

def _provider_track_titles_for_strict(*args, **kwargs):
    return _provider_identity_runtime._provider_track_titles_for_strict_for_runtime(sys.modules[__name__], *args, **kwargs)

def _provider_id_for_strict(*args, **kwargs):
    return _provider_identity_runtime._provider_id_for_strict_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_tracklist_match_details(*args, **kwargs):
    return _provider_identity_runtime._strict_tracklist_match_details_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_candidate_artist_text(*args, **kwargs):
    return _provider_identity_runtime._strict_candidate_artist_text_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_year_from_tags(*args, **kwargs):
    return _provider_identity_runtime._strict_year_from_tags_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_year_from_payload(*args, **kwargs):
    return _provider_identity_runtime._strict_year_from_payload_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_flat_text_values(*args, **kwargs):
    return _provider_identity_runtime._strict_flat_text_values_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_tag_text_tokens(*args, **kwargs):
    return _provider_identity_runtime._strict_tag_text_tokens_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_payload_text_tokens(*args, **kwargs):
    return _provider_identity_runtime._strict_payload_text_tokens_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_secondary_identity_signal(*args, **kwargs):
    return _provider_identity_runtime._strict_secondary_identity_signal_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_tracklist_similarity_details(*args, **kwargs):
    return _provider_identity_runtime._strict_tracklist_similarity_details_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_smart_provider_match_verdict(*args, **kwargs):
    return _provider_identity_runtime._strict_smart_provider_match_verdict_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_provider_match_100(*args, **kwargs):
    return _provider_identity_runtime._strict_provider_match_100_for_runtime(sys.modules[__name__], *args, **kwargs)

def _provider_candidate_id(*args, **kwargs):
    return _provider_identity_runtime._provider_candidate_id_for_runtime(sys.modules[__name__], *args, **kwargs)

def _build_provider_identity_candidates(*args, **kwargs):
    return _provider_identity_runtime._build_provider_identity_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)

def _provider_candidate_soft_identity_ok(*args, **kwargs):
    return _provider_identity_runtime._provider_candidate_soft_identity_ok_for_runtime(sys.modules[__name__], *args, **kwargs)

def _provider_candidate_match_classification(*args, **kwargs):
    return _provider_identity_runtime._provider_candidate_match_classification_for_runtime(sys.modules[__name__], *args, **kwargs)

def _annotate_provider_identity_candidates(*args, **kwargs):
    return _provider_identity_runtime._annotate_provider_identity_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)

def _provider_identity_ai_skip_reason(*args, **kwargs):
    return _provider_identity_runtime._provider_identity_ai_skip_reason_for_runtime(sys.modules[__name__], *args, **kwargs)

def _provider_candidate_near_perfect_identity(*args, **kwargs):
    return _provider_identity_runtime._provider_candidate_near_perfect_identity_for_runtime(sys.modules[__name__], *args, **kwargs)

def _provider_candidates_support_consensus(*args, **kwargs):
    return _provider_identity_runtime._provider_candidates_support_consensus_for_runtime(sys.modules[__name__], *args, **kwargs)

def _edition_soft_identity_survives_strict_reject(*args, **kwargs):
    return _provider_identity_runtime._edition_soft_identity_survives_strict_reject_for_runtime(sys.modules[__name__], *args, **kwargs)

def _ai_choose_provider_identity_candidate(*args, **kwargs):
    return _provider_identity_runtime._ai_choose_provider_identity_candidate_for_runtime(sys.modules[__name__], *args, **kwargs)

def _arbitrate_provider_identity(*args, **kwargs):
    return _provider_identity_runtime._arbitrate_provider_identity_for_runtime(sys.modules[__name__], *args, **kwargs)

def _strict_discogs_payload_from_release_data(*args, **kwargs):
    return _provider_identity_runtime._strict_discogs_payload_from_release_data_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_discogs_release_by_id(*args, **kwargs):
    return _discogs_runtime._fetch_discogs_release_by_id_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mb_extract_year(raw_value: Any) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return ""
    match = re.search(r"(19|20)\d{2}", raw)
    return str(match.group(0)) if match else ""


def _extract_musicbrainz_release_label_info(release_data: dict | None) -> tuple[list[str], list[str]]:
    if not isinstance(release_data, dict):
        return ([], [])
    labels: list[str] = []
    catalog_numbers: list[str] = []
    for info in release_data.get("label-info-list") or release_data.get("label_list") or []:
        if not isinstance(info, dict):
            continue
        label_payload = info.get("label") if isinstance(info.get("label"), dict) else {}
        label_name = str(
            label_payload.get("name")
            or info.get("name")
            or ""
        ).strip()
        if label_name:
            labels.append(label_name)
        catalog_number = str(
            info.get("catalog-number")
            or info.get("catalog_number")
            or info.get("catalog-number-list")
            or ""
        ).strip()
        if catalog_number:
            catalog_numbers.append(catalog_number)
    dedup_labels = _dedupe_keep_order([str(v or "").strip() for v in labels if str(v or "").strip()])
    dedup_catalogs = _dedupe_keep_order([str(v or "").strip() for v in catalog_numbers if str(v or "").strip()])
    return (dedup_labels, dedup_catalogs)


def _fetch_musicbrainz_release_group_versions(release_group_id: str) -> tuple[int, list[dict[str, Any]], dict[str, Any]]:
    rgid = str(release_group_id or "").strip()
    if not rgid or not USE_MUSICBRAINZ:
        return (0, [], {})
    try:
        def _fetch_rg():
            return musicbrainzngs.get_release_group_by_id(
                rgid,
                includes=["releases", "artist-credits"],
            )["release-group"]
        rg_raw = get_mb_queue().submit(f"rg_strict_{rgid}", _fetch_rg) if (MB_QUEUE_ENABLED and USE_MUSICBRAINZ) else _fetch_rg()
    except Exception:
        return (0, [], {})

    release_list = rg_raw.get("release-list") or rg_raw.get("releases") or []
    versions: list[dict[str, Any]] = []
    for rel in (release_list or [])[:20]:
        if not isinstance(rel, dict):
            continue
        rel_id = str(rel.get("id") or "").strip()
        if not rel_id:
            continue
        versions.append(
            {
                "id": rel_id,
                "title": str(rel.get("title") or rg_raw.get("title") or "").strip() or None,
                "date": str(rel.get("date") or "").strip() or None,
                "country": str(rel.get("country") or "").strip() or None,
                "status": str(rel.get("status") or "").strip() or None,
                "url": f"https://musicbrainz.org/release/{quote(rel_id, safe='')}",
            }
        )
    try:
        release_count = int(len(release_list or []))
    except Exception:
        release_count = 0
    return (release_count, versions, rg_raw if isinstance(rg_raw, dict) else {})


def _fetch_musicbrainz_strict_payload(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._fetch_musicbrainz_strict_payload_for_runtime(sys.modules[__name__], *args, **kwargs)



def _score_musicbrainz_release_payload_for_local_context(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._score_musicbrainz_release_payload_for_local_context_for_runtime(sys.modules[__name__], *args, **kwargs)



def _fetch_musicbrainz_strict_payload_for_edition(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._fetch_musicbrainz_strict_payload_for_edition_for_runtime(sys.modules[__name__], *args, **kwargs)



def _strict_expected_provider_id(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._strict_expected_provider_id_for_runtime(sys.modules[__name__], *args, **kwargs)


def _strict_payload_for_provider(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._strict_payload_for_provider_for_runtime(sys.modules[__name__], *args, **kwargs)


_SCAN_MATCH_PROVIDER_KEYS: tuple[str, ...] = (
    "discogs",
    "itunes",
    "deezer",
    "spotify",
    "qobuz",
    "tidal",
    "audiodb",
    "bandcamp",
    "lastfm",
)


def _default_scan_provider_payloads(existing: dict | None = None) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key in _SCAN_MATCH_PROVIDER_KEYS:
        value = existing.get(key) if isinstance(existing, dict) else None
        payloads[key] = value if isinstance(value, dict) else None
    return payloads


def _provider_payloads_fetch_bounded_for_scan(
    artist_name: str,
    album_title: str,
    *,
    existing: dict | None = None,
) -> dict[str, Any]:
    """
    During inline scan matching, provider fallback must stay time-bounded.

    The broad provider fanout belongs in the bounded parallel helper. Returning
    here with cached timeouts/errors is preferable to letting scan workers
    serialize on Discogs/Bandcamp cold fetches for minutes.
    """
    payloads = _default_scan_provider_payloads(existing)
    try:
        fetched = _fetch_album_provider_fallbacks_parallel(
            artist_name,
            album_title,
            scan_inline=True,
        ) or {}
    except Exception:
        fetched = {}
    for key in _SCAN_MATCH_PROVIDER_KEYS:
        if isinstance(payloads.get(key), dict):
            continue
        value = fetched.get(key) if isinstance(fetched, dict) else None
        if isinstance(value, dict):
            payloads[key] = value
    return payloads


def _strict_provider_cold_fetch_allowed(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._strict_provider_cold_fetch_allowed_for_runtime(sys.modules[__name__], *args, **kwargs)


def _strict_validate_edition_match(
    *,
    artist_name: str,
    album_title: str,
    edition: dict,
) -> dict:
    return _identity_runtime._strict_validate_edition_match_for_runtime(
        sys.modules[__name__],
        artist_name=artist_name,
        album_title=album_title,
        edition=edition,
    )


def _strict_clear_identity_on_reject(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._strict_clear_identity_on_reject_for_runtime(sys.modules[__name__], *args, **kwargs)


def _strict_mutation_allowed(item: dict | None) -> tuple[bool, str]:
    if not isinstance(item, dict):
        return (False, "strict_match_missing")
    if bool(item.get("strict_match_verified")):
        return (True, "")
    reason = str(item.get("strict_reject_reason") or "").strip() or "strict_match_missing"
    return (False, reason)


def _identity_text_is_generic(value: str) -> bool:
    return _identity_hints_runtime.identity_text_is_generic(value)


def _identity_folder_name_looks_like_container(value: str) -> bool:
    return _identity_hints_runtime.identity_folder_name_looks_like_container(value)


def _identity_strip_track_prefix_artist_artifact(value: str) -> str:
    return _identity_hints_runtime.identity_strip_track_prefix_artist_artifact(value)


def _identity_artist_fallback_candidate(value: str) -> str:
    return _identity_hints_runtime.identity_artist_fallback_candidate(value)


def _identity_artist_fallback_is_usable(value: str) -> bool:
    return _identity_hints_runtime.identity_artist_fallback_is_usable(value)


def _identity_album_fallback_is_usable(
    value: str,
    *,
    missing_required: list[str] | tuple[str, ...] | set[str] | None = None,
    folder_name: str = "",
) -> bool:
    return _identity_hints_runtime.identity_album_fallback_is_usable_for_runtime(
        sys.modules[__name__],
        value,
        missing_required=missing_required,
        folder_name=folder_name,
    )


def _should_try_local_context_identity_ai(
    *,
    local_artist: str,
    local_album: str,
    folder_name: str = "",
    missing_required_tags: list[str] | tuple[str, ...] | set[str] | None = None,
    force_try: bool = False,
) -> bool:
    return _identity_hints_runtime.should_try_local_context_identity_ai_for_runtime(
        sys.modules[__name__],
        local_artist=local_artist,
        local_album=local_album,
        folder_name=folder_name,
        missing_required_tags=missing_required_tags,
        force_try=force_try,
    )


def _edition_missing_required_tags_set(edition: dict | None) -> set[str]:
    return _identity_hints_runtime.edition_missing_required_tags_set(edition)


def _edition_has_verified_provider_identity(edition: dict | None) -> bool:
    return _identity_hints_runtime.edition_has_verified_provider_identity_for_runtime(sys.modules[__name__], edition)


def _identity_hint_safe_for_provider_lookup(
    edition: dict | None,
    *,
    default_artist: str = "",
    default_title: str = "",
) -> bool:
    return _identity_hints_runtime.identity_hint_safe_for_provider_lookup(
        edition,
        default_artist=default_artist,
        default_title=default_title,
    )


def _prefer_identity_hint_value(
    *,
    current_value: str,
    hinted_value: str,
    field_name: str,
    missing_required: set[str],
    folder_name: str = "",
) -> str:
    return _identity_hints_runtime.prefer_identity_hint_value_for_runtime(
        sys.modules[__name__],
        current_value=current_value,
        hinted_value=hinted_value,
        field_name=field_name,
        missing_required=missing_required,
        folder_name=folder_name,
    )


def _resolve_edition_display_identity(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._resolve_edition_display_identity_for_runtime(sys.modules[__name__], *args, **kwargs)



def _apply_resolved_identity_to_edition(
    edition: dict | None,
    *,
    default_artist: str = "",
    default_title: str = "",
    folder_name: str = "",
) -> tuple[str, str]:
    return _identity_hints_runtime.apply_resolved_identity_to_edition_for_runtime(
        sys.modules[__name__],
        edition,
        default_artist=default_artist,
        default_title=default_title,
        folder_name=folder_name,
    )


def _album_hint_from_track_titles(track_titles: list[str]) -> str:
    return _identity_hints_runtime.album_hint_from_track_titles_for_runtime(sys.modules[__name__], track_titles)


def _filename_identity_hints(file_paths: list[Path | str] | None) -> dict[str, str]:
    return _identity_hints_runtime.filename_identity_hints_for_runtime(sys.modules[__name__], file_paths)


def _files_effective_artist_image_path(
    album_folder: Path | str | None,
    artist_name: str,
    artist_norm: str,
    *,
    conn=None,
    root_dirs: Optional[set[str]] = None,
) -> Optional[Path]:
    """
    Resolve the effective artist image for Files mode:
    1) on-disk artist.* file near the artist folder
    2) cached external image in /config/media_cache/artist
    """
    folder_path = None
    try:
        if album_folder:
            folder_path = path_for_fs_access(Path(str(album_folder)))
    except Exception:
        folder_path = None
    if folder_path and folder_path.exists() and folder_path.is_dir():
        try:
            artist_folder = _files_guess_artist_folder(folder_path, artist_name, root_dirs=root_dirs)
        except Exception:
            artist_folder = None
        local_path = _first_artist_image_path(artist_folder) if artist_folder else None
        if local_path and local_path.exists() and local_path.is_file():
            return local_path
    norm_key = _norm_artist_key(artist_norm or artist_name)
    if not norm_key:
        return None
    own_conn = None
    try:
        pg_conn = conn
        if pg_conn is None:
            own_conn = _files_pg_connect()
            pg_conn = own_conn
        if pg_conn is None:
            return None
        ext_row = _files_get_external_artist_images(pg_conn, [norm_key]).get(norm_key) or {}
        ext_path = _existing_file_path(str(ext_row.get("image_path") or "").strip())
        if ext_path and ext_path.is_file():
            if _is_media_cache_file(ext_path, kind="artist") or _is_usable_artist_image_path(ext_path):
                return ext_path
    except Exception:
        return None
    finally:
        try:
            if own_conn is not None:
                own_conn.close()
        except Exception:
            pass
    return None


def _infer_identity_from_local_context_ai(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._infer_identity_from_local_context_ai_for_runtime(sys.modules[__name__], *args, **kwargs)



def search_mb_release_group_by_metadata(
    artist: str,
    album_norm: str,
    tracks: set[str],
    title_raw: Optional[str] = None,
    album_folder: Optional[Path] = None,
    local_tags: Optional[dict] = None,
    local_paths: Optional[list[Any]] = None,
    scan_inline: bool | None = None,
) -> tuple[dict | None, bool]:
    return _musicbrainz_runtime.search_mb_release_group_by_metadata_for_runtime(
        sys.modules[__name__],
        artist,
        album_norm,
        tracks,
        title_raw=title_raw,
        album_folder=album_folder,
        local_tags=local_tags,
        local_paths=local_paths,
        scan_inline=scan_inline,
    )


def process_ai_groups_batch(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_choose_best_runtime.process_ai_groups_batch_for_runtime(sys.modules[__name__], *args, **kwargs)


def ai_suggest_artist_roles(*args: Any, **kwargs: Any) -> Any:
    return _artist_roles_runtime.ai_suggest_artist_roles_for_runtime(sys.modules[__name__], *args, **kwargs)


def choose_best(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_choose_best_runtime.choose_best_for_runtime(sys.modules[__name__], *args, **kwargs)

def scan_artist_duplicates(args):
    return _dedupe_scan_runtime.scan_artist_duplicates_for_runtime(sys.modules[__name__], args)


def scan_duplicates(editions, artist):
    return _dedupe_scan_runtime.scan_duplicates_for_runtime(sys.modules[__name__], editions, artist)


def _duplicate_group_album_ids(group: dict | None) -> set[int]:
    ids: set[int] = set()
    if not isinstance(group, dict):
        return ids
    top_id = _parse_int_loose(group.get("album_id"), 0)
    if top_id > 0:
        ids.add(int(top_id))
    for key in ("best",):
        entry = group.get(key)
        if isinstance(entry, dict):
            aid = _parse_int_loose(entry.get("album_id"), 0)
            if aid > 0:
                ids.add(int(aid))
    for list_key in ("losers", "editions"):
        entries = group.get(list_key) or []
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            aid = _parse_int_loose(entry.get("album_id"), 0)
            if aid > 0:
                ids.add(int(aid))
    return ids


def _reconcile_scan_duplicates_across_artist_buckets(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._reconcile_scan_duplicates_across_artist_buckets_for_runtime(sys.modules[__name__], *args, **kwargs)

def save_scan_editions_to_db(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime.save_scan_editions_to_db_for_runtime(sys.modules[__name__], *args, **kwargs)


def _delete_duplicate_group_rows(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.delete_duplicate_group_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def save_scan_artist_to_db(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.save_scan_artist_to_db_for_runtime(sys.modules[__name__], *args, **kwargs)


def save_scan_editions_artist_to_db(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime.save_scan_editions_artist_to_db_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_pipeline_trace_columns(*args, **kwargs):
    return _scan_pipeline_trace_runtime._scan_pipeline_trace_columns_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_pipeline_trace_move_lookup(*args, **kwargs):
    return _scan_pipeline_trace_runtime._scan_pipeline_trace_move_lookup_for_runtime(sys.modules[__name__], *args, **kwargs)


def _apply_scan_move_to_trace_rows(*args, **kwargs):
    return _scan_pipeline_trace_runtime._apply_scan_move_to_trace_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def _sync_scan_pipeline_trace_move_rows(*args, **kwargs):
    return _scan_pipeline_trace_runtime._sync_scan_pipeline_trace_move_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_move_trace_pending_scan_ids(*args, **kwargs):
    return _scan_pipeline_trace_runtime._scan_move_trace_pending_scan_ids_for_runtime(sys.modules[__name__], *args, **kwargs)


def _reconcile_scan_move_trace_backlog(*args, **kwargs):
    return _scan_pipeline_trace_runtime._reconcile_scan_move_trace_backlog_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_pipeline_trace_incomplete_lookup(*args, **kwargs):
    return _scan_pipeline_trace_runtime._scan_pipeline_trace_incomplete_lookup_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_pipeline_trace_duplicate_lookup(*args, **kwargs):
    return _scan_pipeline_trace_runtime._scan_pipeline_trace_duplicate_lookup_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_pipeline_trace_status(*args, **kwargs):
    return _scan_pipeline_trace_runtime._scan_pipeline_trace_status_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_pipeline_trace_timeline(*args, **kwargs):
    return _scan_pipeline_trace_runtime._scan_pipeline_trace_timeline_for_runtime(sys.modules[__name__], *args, **kwargs)


def _edition_cached_has_cover(*args, **kwargs):
    return _scan_pipeline_trace_runtime._edition_cached_has_cover_for_runtime(sys.modules[__name__], *args, **kwargs)


def _edition_cached_format_text(*args, **kwargs):
    return _scan_pipeline_trace_runtime._edition_cached_format_text_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_pipeline_trace_build_rows(*args, **kwargs):
    return _scan_pipeline_trace_runtime._scan_pipeline_trace_build_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_pipeline_trace_write_rows(*args, **kwargs):
    return _scan_pipeline_trace_runtime._scan_pipeline_trace_write_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def save_scan_pipeline_trace_artist_to_db(*args, **kwargs):
    return _scan_pipeline_trace_runtime.save_scan_pipeline_trace_artist_to_db_for_runtime(sys.modules[__name__], *args, **kwargs)


def save_scan_pipeline_trace_to_db(*args, **kwargs):
    return _scan_pipeline_trace_runtime.save_scan_pipeline_trace_to_db_for_runtime(sys.modules[__name__], *args, **kwargs)



def update_scan_history_incremental(
    *args: Any,
    **kwargs: Any,
) -> None:
    return _scan_persistence_runtime.update_scan_history_incremental_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_provider_no_tracklist_rollup(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.scan_provider_no_tracklist_rollup_for_runtime(sys.modules[__name__], *args, **kwargs)


def _refresh_scan_history_from_published(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._refresh_scan_history_from_published_for_runtime(sys.modules[__name__], *args, **kwargs)


def save_scan_to_db(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.save_scan_to_db_for_runtime(sys.modules[__name__], *args, **kwargs)


def _remove_dedupe_group_from_db(artist: str, best_album_id: int, loser_album_ids: List[int]) -> None:
    """Remove one duplicate group from DB after it has been successfully moved to /dupes."""
    try:
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        cur.execute("DELETE FROM duplicates_best WHERE artist = ? AND album_id = ?", (artist, best_album_id))
        # duplicates_loser.album_id is the winner/group key; loser_album_id stores the real loser edition id.
        # Remove the whole loser set for this winner group in one shot.
        cur.execute("DELETE FROM duplicates_loser WHERE artist = ? AND album_id = ?", (artist, best_album_id))
        con.commit()
        con.close()
    except Exception as e:
        logging.warning("_remove_dedupe_group_from_db failed for %s / %s: %s", artist, best_album_id, e)


def load_scan_from_db(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime.load_scan_from_db_for_runtime(sys.modules[__name__], *args, **kwargs)


def _load_duplicate_groups_from_pipeline_trace(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.load_duplicate_groups_from_pipeline_trace_for_runtime(sys.modules[__name__], *args, **kwargs)


def clear_db_on_new_scan():
    """
    When a user triggers "Start New Scan," clear live duplicate memory only.
    Persisted duplicate rows are a global open-review registry and must survive
    across scans until the user resolves them.
    """
    with lock:
        state["duplicates"].clear()

def _get_library_mode() -> str:
    """
    Return the active library mode.
    PMDA now runs in Files mode only.
    """
    mode = (LIBRARY_MODE or "files").strip().lower()
    if mode != "files":
        logging.warning("LIBRARY_MODE '%s' ignored; forcing 'files' mode.", mode)
    return _config_core.normalize_library_mode(mode)


def _extract_musicbrainz_id_from_meta(meta: dict | None) -> str:
    """Return normalized MBID-like string from tag/meta dict (empty string when absent)."""
    m = meta or {}
    preferred_keys = (
        "musicbrainz_releasegroupid",
        "musicbrainz_release_group_id",
        "musicbrainz_releaseid",
        "musicbrainz_release_id",
        "musicbrainz_id",
        "musicbrainz_albumid",
    )
    for key in preferred_keys:
        v = m.get(key)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    for raw_key, raw_value in m.items():
        norm = re.sub(r"[^a-z0-9]+", "", str(raw_key or "").lower())
        if norm in {
            "musicbrainzreleasegroupid",
            "musicbrainzreleaseid",
            "musicbrainzalbumid",
            "musicbrainzid",
        }:
            s = str(raw_value or "").strip()
            if s:
                return s
    return ""


def _extract_musicbrainz_release_id_from_meta(meta: dict | None) -> str:
    """Return MB release-id like string from tag/meta dict (empty string when absent)."""
    m = meta or {}
    preferred_keys = (
        "musicbrainz_releaseid",
        "musicbrainz_release_id",
        "musicbrainz_albumid",
        "musicbrainz_album_id",
    )
    for key in preferred_keys:
        v = m.get(key)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    for raw_key, raw_value in m.items():
        norm = re.sub(r"[^a-z0-9]+", "", str(raw_key or "").lower())
        if norm in {
            "musicbrainzreleaseid",
            "musicbrainzalbumid",
        }:
            s = str(raw_value or "").strip()
            if s:
                return s
    return ""


def _has_trusted_album_identity(
    *,
    musicbrainz_id: str | None = None,
    discogs_release_id: str | None = None,
    lastfm_album_mbid: str | None = None,
    bandcamp_album_url: str | None = None,
) -> bool:
    """Return True when we have a provider identifier from a trusted music source."""
    return _materialization_policy_core.has_trusted_album_identity(
        musicbrainz_id=musicbrainz_id,
        discogs_release_id=discogs_release_id,
        lastfm_album_mbid=lastfm_album_mbid,
        bandcamp_album_url=bandcamp_album_url,
    )


def _extract_artist_mbid_from_mb_payload(payload: dict | None) -> str:
    return _artist_profile_runtime._extract_artist_mbid_from_mb_payload_for_runtime(sys.modules[__name__], payload)


def _resolve_artist_mbid_for_fanart(
    *,
    artist_name: str,
    artist_mbid: str | None = None,
    musicbrainz_id: str | None = None,
    discogs_release_id: str | None = None,
    lastfm_album_mbid: str | None = None,
    bandcamp_album_url: str | None = None,
) -> str:
    return _artist_profile_runtime._resolve_artist_mbid_for_fanart_for_runtime(
        sys.modules[__name__],
        artist_name=artist_name,
        artist_mbid=artist_mbid,
        musicbrainz_id=musicbrainz_id,
        discogs_release_id=discogs_release_id,
        lastfm_album_mbid=lastfm_album_mbid,
        bandcamp_album_url=bandcamp_album_url,
    )


def _dominant_genre_by_artist(albums_payload: list[dict]) -> dict[str, str]:
    return _genre_runtime._dominant_genre_by_artist_for_runtime(sys.modules[__name__], albums_payload)


def _apply_genre_defaults_to_albums_payload(albums_payload: list[dict]) -> None:
    return _genre_runtime._apply_genre_defaults_to_albums_payload_for_runtime(
        sys.modules[__name__],
        albums_payload,
    )


def _extract_files_identity_fields(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._extract_files_identity_fields_for_runtime(sys.modules[__name__], *args, **kwargs)



def _album_folder_cache_key(folder: Path | str) -> str:
    """Stable key used by files_album_scan_cache for one album folder."""
    raw = str(folder or "").strip()
    if not raw:
        return ""
    raw = raw.rstrip("/") or "/"
    if (
        raw == "/music"
        or raw.startswith("/music/")
        or raw == "/host_mnt"
        or raw.startswith("/host_mnt/")
        or raw == "/dupes"
        or raw.startswith("/dupes/")
    ):
        return raw
    p = Path(raw)
    try:
        return str(p.resolve())
    except (OSError, RuntimeError):
        return str(p)


def _compute_album_fingerprint(paths: list[Path]) -> str:
    """
    Lightweight album fingerprint from path/name/size/mtime.
    Used by changed-only scans and fast incremental skip.
    """
    h = hashlib.blake2b(digest_size=20)
    for p in sorted(paths, key=lambda x: str(x)):
        try:
            st = p.stat()
            h.update(str(p.name).encode("utf-8", "replace"))
            h.update(b"|")
            h.update(str(int(st.st_size)).encode("ascii"))
            h.update(b"|")
            h.update(str(int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))).encode("ascii"))
            h.update(b"\n")
        except OSError:
            h.update(str(p).encode("utf-8", "replace"))
            h.update(b"|missing\n")
    return h.hexdigest()


def _relative_depth_under_root(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.relative_depth_under_root_for_runtime(sys.modules[__name__], *args, **kwargs)


def _compute_dir_scan_fingerprint(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.compute_dir_scan_fingerprint_for_runtime(sys.modules[__name__], *args, **kwargs)


def _load_files_album_scan_cache_map(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._load_files_album_scan_cache_map_for_runtime(sys.modules[__name__], *args, **kwargs)


def _load_files_dir_scan_cache_map(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.load_files_dir_scan_cache_map_for_runtime(sys.modules[__name__], *args, **kwargs)


def _upsert_files_album_scan_cache_rows(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._upsert_files_album_scan_cache_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def _upsert_files_dir_scan_cache_rows(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.upsert_files_dir_scan_cache_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def _build_files_cache_row_from_prescan_item(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.build_files_cache_row_from_prescan_item_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_should_snapshot_prescan_cache_for_run(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.files_should_snapshot_prescan_cache_for_run_for_runtime(sys.modules[__name__], *args, **kwargs)


def _snapshot_files_album_scan_cache_from_prescan(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._snapshot_files_album_scan_cache_from_prescan_for_runtime(sys.modules[__name__], *args, **kwargs)


def _snapshot_files_dir_scan_cache_from_prescan(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.snapshot_files_dir_scan_cache_from_prescan_for_runtime(sys.modules[__name__], *args, **kwargs)


def _trigger_prescan_cache_snapshot_async(*args: Any, **kwargs: Any) -> Any:
    return _scan_persistence_runtime.trigger_prescan_cache_snapshot_async_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_cache_quality_recalc_status_unlocked() -> dict[str, Any]:
    return _cache_quality_runtime.files_cache_quality_recalc_status_unlocked_for_runtime(sys.modules[__name__])


def _recalculate_files_album_scan_cache_quality(*args: Any, **kwargs: Any) -> Any:
    return _cache_quality_runtime.recalculate_files_album_scan_cache_quality_for_runtime(sys.modules[__name__], *args, **kwargs)


def _start_files_cache_quality_recalc_async(
    *,
    batch_size: int = 500,
    source_id: int | None = None,
    limit: int | None = None,
    reason: str = "manual",
) -> tuple[bool, dict[str, Any]]:
    return _cache_quality_runtime.start_files_cache_quality_recalc_async_for_runtime(
        sys.modules[__name__],
        batch_size=batch_size,
        source_id=source_id,
        limit=limit,
        reason=reason,
    )


def _refresh_files_album_scan_cache_from_editions(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._refresh_files_album_scan_cache_from_editions_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_collect_ordered_audio_paths(folder: Path, ordered_paths_raw: list | None = None) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for raw in (ordered_paths_raw or []):
        try:
            p = path_for_fs_access(Path(raw))
        except Exception:
            continue
        try:
            if p.exists() and p.is_file() and AUDIO_RE.search(p.name):
                sp = str(p.resolve())
                if sp in seen:
                    continue
                seen.add(sp)
                out.append(p)
        except Exception:
            continue
    if out:
        return out
    try:
        discovered = [p for p in folder.rglob("*") if p.is_file() and AUDIO_RE.search(p.name)]
    except Exception:
        discovered = []
    return sorted(discovered, key=lambda p: str(p))


def _infer_disc_track_from_text(text: str, fallback_track: int) -> tuple[int, int]:
    raw = str(text or "").strip()
    if not raw:
        return (1, max(1, int(fallback_track or 1)))
    disc = 1
    track = max(1, int(fallback_track or 1))
    m = re.match(r"^\s*(?:cd|disc)\s*(\d{1,2})\s*[-_. ]\s*(\d{1,3})\b", raw, flags=re.IGNORECASE)
    if m:
        return (_parse_int_loose(m.group(1), 1) or 1, _parse_int_loose(m.group(2), track) or track)
    m = re.match(r"^\s*(\d{1,2})\s*[-_.]\s*(\d{1,3})\b", raw)
    if m:
        return (_parse_int_loose(m.group(1), 1) or 1, _parse_int_loose(m.group(2), track) or track)
    m = re.match(r"^\s*([A-Z])\s*(?:[-_. ]?\s*(\d{1,3}))\b", raw, flags=re.IGNORECASE)
    if m:
        disc = (ord(m.group(1).upper()) - ord("A")) + 1
        track = _parse_int_loose(m.group(2), 1) or 1
        return (max(1, disc), max(1, track))
    m = re.match(r"^\s*(\d{1,3})\b", raw)
    if m:
        track = _parse_int_loose(m.group(1), track) or track
        return (1, max(1, track))
    return (disc, track)


def _disc_label_from_text(text: str, disc_num: int) -> str:
    raw = str(text or "").strip()
    if raw:
        m = re.match(r"^\s*([A-Z])\s*(?:[-_. ]?\s*\d{1,3})\b", raw, flags=re.IGNORECASE)
        if m:
            side = str(m.group(1) or "").upper()
            if side:
                return f"Side {side}"
    disc_i = max(1, int(disc_num or 1))
    return f"Disc {disc_i}"


def _track_text_candidates(raw_text: str) -> list[str]:
    raw = str(raw_text or "").strip()
    if not raw:
        return []
    candidates: list[str] = [raw]
    patterns = [
        r"^\s*[^-]+?\s*-\s*[^-]+?\s*-\s*(.+)$",
        r"^\s*[^-]+?\s*-\s*(.+)$",
    ]
    for pat in patterns:
        try:
            m = re.match(pat, raw, flags=re.IGNORECASE)
        except re.error:
            m = None
        if m:
            tail = str(m.group(1) or "").strip()
            if tail:
                candidates.append(tail)
    seen: set[str] = set()
    out: list[str] = []
    for cand in candidates:
        norm = cand.strip().lower()
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(cand.strip())
    return out


def _clean_track_title_from_text(text: str, fallback_index: int) -> str:
    return _album_media_runtime._clean_track_title_from_text_for_runtime(
        sys.modules[__name__],
        text,
        fallback_index,
    )


def _strip_album_artist_prefixes_from_track_title(
    text: str,
    *,
    album_hint: str = "",
    artist_hint: str = "",
) -> str:
    return _album_media_runtime._strip_album_artist_prefixes_from_track_title_for_runtime(
        sys.modules[__name__],
        text,
        album_hint=album_hint,
        artist_hint=artist_hint,
    )


def _track_display_fields_from_sources(
    *,
    raw_title: str,
    file_path: str,
    fallback_disc: int,
    fallback_track: int,
    album_hint: str = "",
    artist_hint: str = "",
) -> dict[str, Any]:
    return _album_media_runtime._track_display_fields_from_sources_for_runtime(
        sys.modules[__name__],
        raw_title=raw_title,
        file_path=file_path,
        fallback_disc=fallback_disc,
        fallback_track=fallback_track,
        album_hint=album_hint,
        artist_hint=artist_hint,
    )


def _provider_track_titles_cached(
    *,
    artist_name: str,
    album_title: str,
    metadata_source: str,
    musicbrainz_release_group_id: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
    edition_payload: dict | None = None,
    cache_only: bool = False,
) -> list[str]:
    return _album_media_runtime._provider_track_titles_cached_for_runtime(
        sys.modules[__name__],
        artist_name=artist_name,
        album_title=album_title,
        metadata_source=metadata_source,
        musicbrainz_release_group_id=musicbrainz_release_group_id,
        discogs_release_id=discogs_release_id,
        lastfm_album_mbid=lastfm_album_mbid,
        bandcamp_album_url=bandcamp_album_url,
        edition_payload=edition_payload,
        cache_only=cache_only,
    )


def _display_tracks_with_provider_overlay(
    rows: list[dict[str, Any]],
    *,
    artist_name: str,
    album_title: str,
    metadata_source: str,
    musicbrainz_release_group_id: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
    edition_payload: dict | None = None,
    cache_only: bool = False,
) -> list[dict[str, Any]]:
    return _album_media_runtime._display_tracks_with_provider_overlay_for_runtime(
        sys.modules[__name__],
        rows,
        artist_name=artist_name,
        album_title=album_title,
        metadata_source=metadata_source,
        musicbrainz_release_group_id=musicbrainz_release_group_id,
        discogs_release_id=discogs_release_id,
        lastfm_album_mbid=lastfm_album_mbid,
        bandcamp_album_url=bandcamp_album_url,
        edition_payload=edition_payload,
        cache_only=cache_only,
    )


def _files_track_value(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.files_track_value_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_build_track_entries_from_item(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.files_build_track_entries_from_item_for_runtime(sys.modules[__name__], *args, **kwargs)


def _clear_files_library_published_rows() -> int:
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
        cur = con.cursor()
        cur.execute("DELETE FROM files_library_published_albums")
        deleted = int(cur.rowcount or 0)
        con.commit()
        con.close()
        return deleted
    except Exception:
        logging.debug("Failed to clear files_library_published_albums", exc_info=True)
        return 0


def _reset_files_live_index_for_scan(*, force: bool = False) -> None:
    """
    Preserve the last readable Files library during normal scan starts.
    Only perform a destructive live-index clear on explicit forced resets.
    """
    if not bool(force):
        logging.info(
            "Files live index reset skipped for scan start: preserving published rows and PG live index until rebuild succeeds"
        )
        return
    deleted = _clear_files_library_published_rows()
    if not _files_pg_init_schema():
        logging.warning("Files live index reset: PostgreSQL schema unavailable")
        return
    # Never block scan startup behind a long-running async index bootstrap.
    # If the lock is busy, skip PG truncate for now and let scan-driven sync refresh progressively.
    acquired = files_index_lock.acquire(blocking=False)
    if not acquired:
        idx_state = _files_index_get_state() or {}
        logging.info(
            "Files live index reset skipped PG truncate (index lock busy, phase=%s); scan will continue.",
            str(idx_state.get("phase") or "unknown"),
        )
        return
    try:
        conn = _files_pg_connect()
        if conn is None:
            logging.warning("Files live index reset: PostgreSQL unavailable")
            return
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    _files_reset_rebuild_tables(cur)
                    _files_index_write_meta(cur, "artists", "0")
                    _files_index_write_meta(cur, "albums", "0")
                    _files_index_write_meta(cur, "tracks", "0")
                    _files_index_write_meta(cur, "last_reason", "scan_full_reset")
                    _files_index_write_meta(cur, "last_build_ts", str(int(time.time())))
        finally:
            conn.close()
        _files_cache_invalidate_all()
        _files_index_set_state(
            running=False,
            started_at=None,
            finished_at=time.time(),
            phase="idle",
            current_folder=None,
            folders_processed=0,
            total_folders=0,
            artists=0,
            albums=0,
            tracks=0,
            error=None,
        )
        logging.info("Files live index reset for new scan: cleared %d published row(s) and truncated PG index tables", deleted)
    except Exception as e:
        logging.warning("Files live index reset failed: %s", e)
    finally:
        files_index_lock.release()


def _files_should_preserve_live_index_for_scan(scan_type: str, requested_resume_run_id: str | None = None) -> bool:
    if _get_library_mode() != "files":
        return False
    scan_type_norm = str(scan_type or "").strip().lower()
    if scan_type_norm != "full":
        return False
    if str(requested_resume_run_id or "").strip():
        return True
    try:
        return bool(_has_unfinished_resume_run("files", scan_type_norm))
    except Exception:
        return False


def _upsert_files_library_published_rows(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.upsert_files_library_published_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def _authoritative_primary_tags_for_publication(*args, **kwargs):
    return _publication_cover_runtime._authoritative_primary_tags_for_publication_for_runtime(sys.modules[__name__], *args, **kwargs)



def _publication_cover_needs_provider_refresh(*args, **kwargs):
    return _publication_cover_runtime._publication_cover_needs_provider_refresh_for_runtime(sys.modules[__name__], *args, **kwargs)



def _cover_provider_from_primary_tags_blob(*args, **kwargs):
    return _publication_cover_runtime._cover_provider_from_primary_tags_blob_for_runtime(sys.modules[__name__], *args, **kwargs)



def _publication_cover_identity_ok(*args, **kwargs):
    return _publication_cover_runtime._publication_cover_identity_ok_for_runtime(sys.modules[__name__], *args, **kwargs)



def _authoritative_publication_cover(*args, **kwargs):
    return _publication_cover_runtime._authoritative_publication_cover_for_runtime(sys.modules[__name__], *args, **kwargs)



def _filter_existing_files_album_items(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.filter_existing_files_album_items_for_runtime(sys.modules[__name__], *args, **kwargs)


def _normalize_bandcamp_album_ref(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.normalize_bandcamp_album_ref_for_runtime(sys.modules[__name__], *args, **kwargs)


def _strict_album_identity_key(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.strict_album_identity_key_for_runtime(sys.modules[__name__], *args, **kwargs)


def _strict_album_identity_key_for_edition(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.strict_album_identity_key_for_edition_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_publication_candidate_score(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.files_publication_candidate_score_for_runtime(sys.modules[__name__], *args, **kwargs)


def _collapse_files_publication_candidates(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.collapse_files_publication_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)


def _delete_files_library_published_rows(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.delete_files_library_published_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_publication_rewrite_path_prefix(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.files_publication_rewrite_path_prefix_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_publication_rewrite_tracks_json(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.files_publication_rewrite_tracks_json_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_publication_remap_published_row(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.files_publication_remap_published_row_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_publication_load_published_rows_by_folder(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.files_publication_load_published_rows_by_folder_for_runtime(sys.modules[__name__], *args, **kwargs)


def _load_files_album_scan_cache_map_for_keys(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._load_files_album_scan_cache_map_for_keys_for_runtime(sys.modules[__name__], *args, **kwargs)


_FILES_SCAN_LIVE_PUBLISH_BATCH_SIZE = 8
_FILES_SCAN_LIVE_PUBLISH_MIN_ITEMS = 12
_FILES_PUBLICATION_RECONCILE_LOCK = threading.Lock()


def _files_live_publish_batches(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.files_live_publish_batches_for_runtime(sys.modules[__name__], *args, **kwargs)


def _publish_files_library_artist_live_batches(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.publish_files_library_artist_live_batches_for_runtime(sys.modules[__name__], *args, **kwargs)


def _publish_files_library_artist_from_items(
    artist_name: str,
    items: list[dict],
    *,
    scan_id: int | None = None,
    results_by_album_id: dict[int, dict] | None = None,
) -> int:
    return _artist_publish_runtime.publish_files_library_artist_from_items_for_runtime(
        sys.modules[__name__],
        artist_name,
        items,
        scan_id=scan_id,
        results_by_album_id=results_by_album_id,
    )


def _rebuild_files_publication_for_scan(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.rebuild_files_publication_for_scan_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_publication_scan_move_maps(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.files_publication_scan_move_maps_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_publication_candidate_existing_path(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.files_publication_candidate_existing_path_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_edition_row_to_publication_item(*args: Any, **kwargs: Any) -> Any:
    return _publication_row_runtime.scan_edition_row_to_publication_item_for_runtime(sys.modules[__name__], *args, **kwargs)


def _reconcile_files_publication_from_scan_editions(*args: Any, **kwargs: Any) -> Any:
    return _publication_reconcile_runtime.reconcile_files_publication_from_scan_editions_for_runtime(sys.modules[__name__], *args, **kwargs)


def _trigger_files_publication_reconcile_async(
    *,
    scan_ids: list[int] | tuple[int, ...] | set[int] | None = None,
    reason: str = "manual",
    rebuild_index: bool = True,
) -> bool:
    return _publication_reconcile_runtime.trigger_files_publication_reconcile_async_for_runtime(
        sys.modules[__name__],
        scan_ids=scan_ids,
        reason=reason,
        rebuild_index=rebuild_index,
    )


def _rows_to_files_library_payload(
    rows: list[tuple],
    *,
    verify_paths: bool = True,
) -> tuple[dict[str, dict], list[dict], int]:
    return _published_payload_runtime.rows_to_files_library_payload_for_runtime(
        sys.modules[__name__],
        rows,
        verify_paths=verify_paths,
    )


def _load_files_library_published_payload() -> tuple[dict[str, dict], list[dict], int]:
    return _published_payload_runtime.load_files_library_published_payload_for_runtime(sys.modules[__name__])


def _load_files_library_published_payload_for_artist(artist_hint: str) -> tuple[dict[str, dict], list[dict], int]:
    return _published_payload_runtime.load_files_library_published_payload_for_artist_for_runtime(
        sys.modules[__name__],
        artist_hint,
    )


def _compute_scan_source_signature(mode: str, scan_type: str) -> str:
    """Build a stable signature describing the scan source and scope."""
    if mode == "files":
        roots = []
        for r in (FILES_ROOTS or []):
            if not r:
                continue
            try:
                roots.append(str(Path(r).resolve()))
            except (OSError, RuntimeError):
                roots.append(str(r))
        skips = []
        for s in (SKIP_FOLDERS or []):
            if not s:
                continue
            try:
                skips.append(str(Path(s).resolve()))
            except (OSError, RuntimeError):
                skips.append(str(s))
        payload = {
            "mode": "files",
            "scan_type": scan_type,
            "roots": sorted(roots),
            "skip_folders": sorted(skips),
        }
    else:
        payload = {
            "mode": "plex",
            "scan_type": scan_type,
            "section_ids": sorted(int(x) for x in (SECTION_IDS or [])),
            "path_map": sorted((k, str(v)) for k, v in (PATH_MAP or {}).items()),
        }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _compute_artist_signature(
    mode: str,
    artist_name: str,
    album_ids: list[int],
    files_editions_by_album_id: dict[int, dict] | None = None,
    files_signature_part_by_album_id: dict[int, str] | None = None,
) -> str:
    """Compute artist signature so resume can detect new/changed albums."""
    h = hashlib.sha256()
    h.update((_norm_artist_key(artist_name) or "").encode("utf-8", errors="ignore"))
    h.update(b"\x1f")
    h.update(str(len(album_ids)).encode("ascii", errors="ignore"))
    h.update(b"\x1f")
    if mode == "files":
        files_map = files_editions_by_album_id or {}
        precomputed_parts = files_signature_part_by_album_id or {}
        for aid in sorted(album_ids):
            part = precomputed_parts.get(aid)
            if part is None:
                fe = files_map.get(aid) or {}
                folder = fe.get("folder")
                folder_key = _album_folder_cache_key(folder) if folder else str(aid)
                fp = (fe.get("fingerprint") or "").strip()
                part = f"{folder_key}|{fp}"
                if isinstance(fe, dict):
                    fe["resume_sig_part"] = part
            h.update(part.encode("utf-8", errors="ignore"))
            h.update(b"\x1e")
    else:
        for aid in sorted(album_ids):
            h.update(str(int(aid)).encode("ascii", errors="ignore"))
            h.update(b"\x1e")
    return h.hexdigest()


def _resume_files_plan_row_tuple(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._resume_files_plan_row_tuple_for_runtime(sys.modules[__name__], *args, **kwargs)


def _persist_resume_files_plan(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._persist_resume_files_plan_for_runtime(sys.modules[__name__], *args, **kwargs)


def _upsert_resume_files_plan_partial(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._upsert_resume_files_plan_partial_for_runtime(sys.modules[__name__], *args, **kwargs)


def _prune_resume_files_plan_artist(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._prune_resume_files_plan_artist_for_runtime(sys.modules[__name__], *args, **kwargs)


def _prune_resume_files_plan_albums(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._prune_resume_files_plan_albums_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ensure_resume_run_started(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._ensure_resume_run_started_for_runtime(sys.modules[__name__], *args, **kwargs)


def _persist_resume_discovery_snapshot(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._persist_resume_discovery_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)


def _persist_resume_discovery_progress_only(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._persist_resume_discovery_progress_only_for_runtime(sys.modules[__name__], *args, **kwargs)


def _load_resume_discovery_snapshot_by_run_id(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._load_resume_discovery_snapshot_by_run_id_for_runtime(sys.modules[__name__], *args, **kwargs)


def _snapshot_current_resume_discovery(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._snapshot_current_resume_discovery_for_runtime(sys.modules[__name__], *args, **kwargs)


def _wait_for_discovery_runtime_update(
    run_id: str | None,
    *,
    previous_updated_at: float | None = None,
    require_paused_ack: bool = False,
    timeout_seconds: float = 3.0,
) -> dict[str, Any] | None:
    run_id = str(run_id or "").strip() or None
    deadline = time.monotonic() + max(0.1, float(timeout_seconds or 0.0))
    latest_snapshot: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        latest_snapshot = _copy_scan_discovery_runtime(run_id)
        if not isinstance(latest_snapshot, dict):
            time.sleep(0.1)
            continue
        updated_at = float(latest_snapshot.get("updated_at") or 0.0)
        if require_paused_ack and bool(latest_snapshot.get("paused_ack")):
            return latest_snapshot
        if previous_updated_at is None:
            return latest_snapshot
        if updated_at > float(previous_updated_at or 0.0):
            return latest_snapshot
        time.sleep(0.1)
    return latest_snapshot


def _snapshot_current_resume_state(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._snapshot_current_resume_state_for_runtime(sys.modules[__name__], *args, **kwargs)


def _restore_resume_files_plan_from_run_row(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._restore_resume_files_plan_from_run_row_for_runtime(sys.modules[__name__], *args, **kwargs)


def _load_resume_files_plan_by_run_id(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._load_resume_files_plan_by_run_id_for_runtime(sys.modules[__name__], *args, **kwargs)


def _load_resume_files_plan_partial_by_run_id(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._load_resume_files_plan_partial_by_run_id_for_runtime(sys.modules[__name__], *args, **kwargs)


def _load_resume_files_plan(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._load_resume_files_plan_for_runtime(sys.modules[__name__], *args, **kwargs)


def _snapshot_current_resume_files_plan(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._snapshot_current_resume_files_plan_for_runtime(sys.modules[__name__], *args, **kwargs)


def _hydrate_resume_files_edition(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._hydrate_resume_files_edition_for_runtime(sys.modules[__name__], *args, **kwargs)


def _prepare_resume_scan_artists(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._prepare_resume_scan_artists_for_runtime(sys.modules[__name__], *args, **kwargs)


def _has_unfinished_resume_run(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._has_unfinished_resume_run_for_runtime(sys.modules[__name__], *args, **kwargs)


def _get_resume_run_snapshot(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._get_resume_run_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)


def _get_resume_run_snapshot_by_run_id(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._get_resume_run_snapshot_by_run_id_for_runtime(sys.modules[__name__], *args, **kwargs)


def _get_latest_resume_run_snapshot_any_signature(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._get_latest_resume_run_snapshot_any_signature_for_runtime(sys.modules[__name__], *args, **kwargs)


def _get_startup_resume_snapshot(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._get_startup_resume_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)


def _maybe_resume_interrupted_scan_on_startup(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._maybe_resume_interrupted_scan_on_startup_for_runtime(sys.modules[__name__], *args, **kwargs)


def _set_resume_artist_status(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._set_resume_artist_status_for_runtime(sys.modules[__name__], *args, **kwargs)


def _set_resume_run_status(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._set_resume_run_status_for_runtime(sys.modules[__name__], *args, **kwargs)


def _build_files_editions(
    scan_type: str = "full",
    *,
    respect_scan_controls: bool = True,
) -> tuple[list[tuple[int, str, list[int]]], int, dict]:
    return _files_editions_runtime.build_files_editions_for_runtime(
        sys.modules[__name__],
        scan_type=scan_type,
        respect_scan_controls=respect_scan_controls,
    )


def _sanitize_export_component(*args, **kwargs):
    return _materialization_helpers_runtime._sanitize_export_component_for_runtime(sys.modules[__name__], *args, **kwargs)




def build_export_path(*args, **kwargs):
    return _materialization_helpers_runtime.build_export_path_for_runtime(sys.modules[__name__], *args, **kwargs)




def _format_label_for_folder(*args, **kwargs):
    return _materialization_helpers_runtime._format_label_for_folder_for_runtime(sys.modules[__name__], *args, **kwargs)




def _derive_album_type_for_folder_name(*args, **kwargs):
    return _materialization_helpers_runtime._derive_album_type_for_folder_name_for_runtime(sys.modules[__name__], *args, **kwargs)




def _build_matched_album_folder_name(*args, **kwargs):
    return _materialization_helpers_runtime._build_matched_album_folder_name_for_runtime(sys.modules[__name__], *args, **kwargs)




def _matched_album_family_key(*args, **kwargs):
    return _materialization_helpers_runtime._matched_album_family_key_for_runtime(sys.modules[__name__], *args, **kwargs)




def _matched_export_target_folder(*args, **kwargs):
    return _materialization_helpers_runtime._matched_export_target_folder_for_runtime(sys.modules[__name__], *args, **kwargs)




def _materialization_confidence_policy(*args, **kwargs):
    return _materialization_helpers_runtime._materialization_confidence_policy_for_runtime(sys.modules[__name__], *args, **kwargs)




def _path_is_under_root(*args, **kwargs):
    return _materialization_helpers_runtime._path_is_under_root_for_runtime(sys.modules[__name__], *args, **kwargs)




def _build_dupe_candidate_from_folder(*args, **kwargs):
    return _materialization_helpers_runtime._build_dupe_candidate_from_folder_for_runtime(sys.modules[__name__], *args, **kwargs)




def _move_folder_to_dupes(*args, **kwargs):
    return _materialization_helpers_runtime._move_folder_to_dupes_for_runtime(sys.modules[__name__], *args, **kwargs)




def _record_scan_move_event(*args, **kwargs):
    return _materialization_helpers_runtime._record_scan_move_event_for_runtime(sys.modules[__name__], *args, **kwargs)




def _folder_file_stat_map(*args, **kwargs):
    return _materialization_helpers_runtime._folder_file_stat_map_for_runtime(sys.modules[__name__], *args, **kwargs)




def _folders_are_hardlink_mirror(*args, **kwargs):
    return _materialization_helpers_runtime._folders_are_hardlink_mirror_for_runtime(sys.modules[__name__], *args, **kwargs)




def _materialize_hardlink_mirror(*args, **kwargs):
    return _materialization_helpers_runtime._materialize_hardlink_mirror_for_runtime(sys.modules[__name__], *args, **kwargs)




def _matched_destination_conflict_folders(*args, **kwargs):
    return _materialization_helpers_runtime._matched_destination_conflict_folders_for_runtime(sys.modules[__name__], *args, **kwargs)




def _build_source_dupe_candidate_from_item(*args, **kwargs):
    return _materialization_helpers_runtime._build_source_dupe_candidate_from_item_for_runtime(sys.modules[__name__], *args, **kwargs)




def _dupe_candidate_track_count(*args, **kwargs):
    return _materialization_helpers_runtime._dupe_candidate_track_count_for_runtime(sys.modules[__name__], *args, **kwargs)




def _dupe_candidate_preview_score(*args, **kwargs):
    return _materialization_helpers_runtime._dupe_candidate_preview_score_for_runtime(sys.modules[__name__], *args, **kwargs)




def _dupe_candidate_folder(*args, **kwargs):
    return _materialization_helpers_runtime._dupe_candidate_folder_for_runtime(sys.modules[__name__], *args, **kwargs)




def _choose_matched_export_conflict_winner(*args, **kwargs):
    return _materialization_helpers_runtime._choose_matched_export_conflict_winner_for_runtime(sys.modules[__name__], *args, **kwargs)




def _export_item_scan_id(*args, **kwargs):
    return _materialization_helpers_runtime._export_item_scan_id_for_runtime(sys.modules[__name__], *args, **kwargs)




def _stable_album_id_for_review(*args, **kwargs):
    return _materialization_helpers_runtime._stable_album_id_for_review_for_runtime(sys.modules[__name__], *args, **kwargs)




def _record_matched_export_conflict_review(*args, **kwargs):
    return _materialization_helpers_runtime._record_matched_export_conflict_review_for_runtime(sys.modules[__name__], *args, **kwargs)




def _move_publish_items_to_matched_library(
    artist_name: str,
    items: list[dict],
    *,
    export_root: str | None = None,
    scan_id_override: int | None = None,
) -> list[dict]:
    return _materialization_export_runtime.move_publish_items_to_matched_library_for_runtime(
        sys.modules[__name__],
        artist_name,
        items,
        export_root=export_root,
        scan_id_override=scan_id_override,
    )


def analyse_directory_structure(roots: list[str], max_samples: int = 300) -> dict:
    """
    Sample audio files under roots, collect relative paths and tags, infer path patterns,
    and return templates + metrics for GET /api/files/structure/overview.
    """
    import random
    paths = _iter_audio_files_under_roots(roots or [])
    if not paths:
        return {"templates": [], "metrics": {}, "samples": [], "sample_count": 0}
    if len(paths) > max_samples:
        paths = random.sample(paths, max_samples)
    samples = []
    for p in paths:
        try:
            rel = p
            for r in roots:
                if r:
                    try:
                        rel = p.relative_to(Path(r))
                        break
                    except ValueError:
                        continue
            rel_str = str(rel)
        except Exception:
            rel_str = str(p)
        tags = extract_tags(p)
        samples.append({
            "path": rel_str,
            "artist": (tags.get("albumartist") or tags.get("artist") or "").strip(),
            "album": (tags.get("album") or "").strip(),
            "year": (tags.get("date") or tags.get("year") or "").strip()[:4],
            "ext": p.suffix.lower().lstrip("."),
        })
    # Simple pattern: count path depth and segment patterns
    depths = [s["path"].count("/") + s["path"].count("\\") for s in samples]
    avg_depth = sum(depths) / len(depths) if depths else 0
    # Dominant "template" as example: first path split into parts
    templates = []
    if samples:
        example = samples[0]["path"]
        templates.append({"name": "sampled", "example": example})
    metrics = {
        "sample_count": len(samples),
        "total_files_estimate": len(_iter_audio_files_under_roots(roots or [])),
        "average_path_depth": round(avg_depth, 1),
        "paths_with_artist_tag": sum(1 for s in samples if s.get("artist")),
        "paths_with_album_tag": sum(1 for s in samples if s.get("album")),
    }
    return {"templates": templates, "metrics": metrics, "samples": samples[:20], "sample_count": len(samples)}


def _storage_materialization_plan_entries() -> list[dict[str, Any]]:
    with lock:
        return [dict(item or {}) for item in list(state.get("storage_scan_plan") or []) if isinstance(item, dict)]


def _storage_materialization_meta_for_folder(
    folder_path: Path | str | None,
    *,
    plan_entries: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    plan = plan_entries if plan_entries is not None else _storage_materialization_plan_entries()
    folder_txt = str(folder_path or "").strip()
    entry: dict[str, Any] | None = None
    if folder_txt and plan:
        entry = _storage_plan_entry_for_canonical_path(folder_txt, plan)
        if entry is None:
            try:
                entry = _storage_find_entry_for_access_path(Path(folder_txt), plan)
            except Exception:
                entry = None
    if not entry:
        return {
            "storage_provider": "",
            "storage_device_id": "",
            "storage_device_label": "",
            "storage_bucket_order": 10**9,
        }
    device_id = str(entry.get("storage_device_id") or "").strip()
    return {
        "storage_provider": str(entry.get("storage_provider") or "unraid").strip() or "unraid",
        "storage_device_id": device_id,
        "storage_device_label": str(entry.get("storage_device_label") or device_id).strip() or device_id,
        "storage_bucket_order": int(entry.get("storage_bucket_order") or 0),
    }


def _storage_ordered_materialization_groups(
    grouped: dict[str, list[dict]] | defaultdict[str, list[dict]],
) -> list[tuple[int, str, str, list[dict]]]:
    flat_items: list[dict] = []
    for artist_name, items in (grouped or {}).items():
        for item in list(items or []):
            item_copy = dict(item or {})
            item_copy["artist"] = str(item_copy.get("artist") or artist_name or "Unknown Artist").strip() or "Unknown Artist"
            item_copy["artist_name"] = str(item_copy.get("artist_name") or item_copy.get("artist") or artist_name or "Unknown Artist").strip() or "Unknown Artist"
            flat_items.append(item_copy)
    flat_items.sort(
        key=lambda item: (
            int(item.get("storage_bucket_order") or 10**9),
            str(item.get("storage_device_id") or ""),
            str(item.get("artist_name") or item.get("artist") or "").lower(),
            str(item.get("album_title") or item.get("title_raw") or "").lower(),
            int(item.get("album_id") or 0),
        )
    )
    out: "OrderedDict[tuple[int, str, str], list[dict]]" = OrderedDict()
    for item in flat_items:
        artist_name = str(item.get("artist_name") or item.get("artist") or "Unknown Artist").strip() or "Unknown Artist"
        key = (
            int(item.get("storage_bucket_order") or 10**9),
            str(item.get("storage_device_id") or ""),
            artist_name,
        )
        out.setdefault(key, []).append(item)
    return [(bucket_order, device_id, artist_name, items) for (bucket_order, device_id, artist_name), items in out.items()]


def _load_scan_strict_export_items(scan_id: int) -> dict[str, list[dict]]:
    return _strict_export_runtime.load_scan_strict_export_items_for_runtime(sys.modules[__name__], scan_id)


def _strict_export_backlog_summary() -> dict[str, Any]:
    return _strict_export_runtime.strict_export_backlog_summary_for_runtime(sys.modules[__name__])


def _smart_provider_promotion_status() -> dict[str, Any]:
    return _strict_export_runtime.smart_provider_promotion_status_for_runtime(sys.modules[__name__])


def _trigger_smart_provider_promotion_async(
    *,
    limit: int | None = None,
    export_after: bool = True,
    reason: str = "smart_provider_promotion",
) -> bool:
    return _strict_export_runtime.trigger_smart_provider_promotion_async_for_runtime(
        sys.modules[__name__],
        limit=limit,
        export_after=export_after,
        reason=reason,
    )


def _run_strict_export_backlog(*, limit: int | None = None, reason: str = "strict_backlog_export") -> dict[str, Any]:
    return _strict_export_runtime.run_strict_export_backlog_for_runtime(
        sys.modules[__name__],
        limit=limit,
        reason=reason,
    )


def _run_scan_strict_match_export(scan_id: int, *, reason: str = "scan_strict_export") -> dict[str, Any]:
    return _strict_export_runtime.run_scan_strict_match_export_for_runtime(
        sys.modules[__name__],
        scan_id,
        reason=reason,
    )


def _trigger_scan_strict_match_export_async(scan_id: int, *, reason: str = "scan_strict_export") -> bool:
    return _strict_export_runtime.trigger_scan_strict_match_export_async_for_runtime(
        sys.modules[__name__],
        scan_id,
        reason=reason,
    )


def _trigger_strict_export_backlog_async(*, limit: int | None = None, reason: str = "strict_backlog_export") -> bool:
    return _strict_export_runtime.trigger_strict_export_backlog_async_for_runtime(
        sys.modules[__name__],
        limit=limit,
        reason=reason,
    )


def _run_export_library() -> None:
    return _export_rebuild_runtime.run_export_library_for_runtime(sys.modules[__name__])


def _trigger_export_library_async(reason: str = "manual") -> bool:
    return _export_rebuild_runtime.trigger_export_library_async_for_runtime(
        sys.modules[__name__],
        reason=reason,
    )


def _build_scan_plan(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._build_scan_plan_for_runtime(sys.modules[__name__], *args, **kwargs)


def _pipeline_flags_for_scan(scan_type: str, run_improve_after_requested: bool) -> dict[str, bool | str]:
    """Resolve effective pipeline flags for the current scan."""
    sync_target = _normalize_player_target(getattr(sys.modules[__name__], "PIPELINE_PLAYER_TARGET", "none"))
    return _scan_orchestrator_core.resolve_pipeline_flags(
        scan_type,
        run_improve_after_requested,
        pipeline_enable_match_fix=PIPELINE_ENABLE_MATCH_FIX,
        pipeline_enable_dedupe=PIPELINE_ENABLE_DEDUPE,
        pipeline_enable_incomplete_move=PIPELINE_ENABLE_INCOMPLETE_MOVE,
        pipeline_enable_export=PIPELINE_ENABLE_EXPORT,
        pipeline_enable_player_sync=PIPELINE_ENABLE_PLAYER_SYNC,
        auto_move_dupes=getattr(sys.modules[__name__], "AUTO_MOVE_DUPES", False),
        magic_mode=getattr(sys.modules[__name__], "MAGIC_MODE", False),
        audit_mode=library_is_audit_mode(),
        sync_target=sync_target,
    )


def _pipeline_inline_flags(
    pipeline_flags_requested: dict[str, bool | str] | None,
    *,
    pipeline_async_enabled: bool,
) -> dict[str, bool | str]:
    return _scan_orchestrator_core.inline_pipeline_flags(
        pipeline_flags_requested,
        pipeline_async_enabled=pipeline_async_enabled,
    )


def _auto_move_incomplete_albums_for_scan(
    scan_id: int | None,
    editions_by_artist: dict[str, list[dict]] | None,
) -> dict:
    return _incomplete_move_runtime.auto_move_incomplete_albums_for_scan_for_runtime(
        sys.modules[__name__],
        scan_id,
        editions_by_artist,
    )


def _compute_scan_track_reconciliation(scan_id: int | None, detected_total: int) -> dict[str, int]:
    return _scan_reconciliation_runtime.compute_scan_track_reconciliation_for_runtime(
        sys.modules[__name__],
        scan_id,
        detected_total,
    )


def _update_scan_storage_bucket_row(
    run_id: str | None,
    meta: dict[str, Any] | None,
    *,
    status: str | None = None,
    albums_done: int | None = None,
    started_at: float | None = None,
    finished_at: float | None = None,
    message: str | None = None,
) -> None:
    return _storage_bucket_runtime.update_scan_storage_bucket_row_for_runtime(
        sys.modules[__name__],
        run_id,
        meta,
        status=status,
        albums_done=albums_done,
        started_at=started_at,
        finished_at=finished_at,
        message=message,
    )


def _mark_broken_from_dupe_groups(
    all_results: dict,
    editions_by_artist: dict[str, list[dict]] | None,
    *,
    ratio_threshold: float = 0.90,
    require_exact_identity: bool = True,
) -> int:
    return _dedupe_broken_runtime.mark_broken_from_dupe_groups_for_runtime(
        sys.modules[__name__],
        all_results,
        editions_by_artist,
        ratio_threshold=ratio_threshold,
        require_exact_identity=require_exact_identity,
    )

def background_scan():
    return _background_runtime.background_scan_for_runtime(sys.modules[__name__])

def background_dedupe(all_groups: List[dict]):
    return _dedupe_move_runtime.background_dedupe_for_runtime(
        sys.modules[__name__],
        all_groups,
    )


# ─────────────────────────────────── SUPPORT FUNCTIONS ──────────────────────────────────
def fetch_cover_as_base64(album_id: int) -> Optional[str]:
    return _dedupe_perform_runtime.fetch_cover_as_base64_for_runtime(sys.modules[__name__], album_id)


def _files_forget_album_folder_global(folder: Path | str) -> bool:
    return _dedupe_perform_runtime.files_forget_album_folder_global_for_runtime(sys.modules[__name__], folder)


def _duplicate_tracks_from_folder(folder: Path, edition_payload: dict | None = None) -> list[dict]:
    return _dedupe_perform_runtime.duplicate_tracks_from_folder_for_runtime(
        sys.modules[__name__],
        folder,
        edition_payload,
    )


def _duplicate_cover_data_for_edition(edition_payload: dict) -> Optional[str]:
    return _dedupe_perform_runtime.duplicate_cover_data_for_edition_for_runtime(sys.modules[__name__], edition_payload)


def _next_available_folder_path(base: Path) -> Path:
    return _dedupe_perform_runtime.next_available_folder_path_for_runtime(sys.modules[__name__], base)


def _hardlink_tree(src: Path, dst: Path) -> None:
    return _dedupe_perform_runtime.hardlink_tree_for_runtime(sys.modules[__name__], src, dst)


def _place_folder_with_strategy(src: Path, dst: Path, strategy: str) -> Path:
    return _dedupe_perform_runtime.place_folder_with_strategy_for_runtime(sys.modules[__name__], src, dst, strategy)


def _winner_destination_for_folder(best_folder: Path) -> Optional[Path]:
    return _dedupe_perform_runtime.winner_destination_for_folder_for_runtime(sys.modules[__name__], best_folder)


def _normalize_winner_folder_to_canonical_root(group: dict) -> Optional[Path]:
    return _dedupe_perform_runtime.normalize_winner_folder_to_canonical_root_for_runtime(sys.modules[__name__], group)


def perform_dedupe(group: dict, best_folders: set = None, manual_override: bool = False) -> List[dict]:
    return _dedupe_perform_runtime.perform_dedupe_for_runtime(
        sys.modules[__name__],
        group,
        best_folders=best_folders,
        manual_override=manual_override,
    )


def _build_card_list(dup_dict) -> list[dict]:
    return _dedupe_cards_runtime.build_card_list_for_runtime(sys.modules[__name__], dup_dict)


# --- New scan control endpoints ---
from flask import Response

def _requires_config():
    return _scan_control_runtime.requires_config_for_runtime(sys.modules[__name__])


def _active_scan_info_locked() -> dict[str, Any]:
    return _scan_control_runtime.active_scan_info_locked_for_runtime(sys.modules[__name__])


def _try_begin_scan(
    *,
    scan_type: str,
    source: str,
    run_improve_after: bool,
    scheduler_run_id: str | None,
) -> tuple[bool, dict[str, Any]]:
    return _scan_control_runtime.try_begin_scan_for_runtime(
        sys.modules[__name__],
        scan_type=scan_type,
        source=source,
        run_improve_after=run_improve_after,
        scheduler_run_id=scheduler_run_id,
    )


def start_background_scan() -> tuple[bool, dict[str, Any]]:
    return _scan_control_runtime.start_background_scan_for_runtime(sys.modules[__name__])


def _run_preflight_checks():
    return _scan_control_runtime.run_preflight_checks_for_runtime(sys.modules[__name__])



# ─────────────────────────────── Discogs throttling ───────────────────────────────
# Discogs enforces a fairly strict rate limit (commonly ~60 req/min for authenticated calls).
# PMDA can issue Discogs calls from multiple scan/background threads, so we need a global throttle
# to avoid 429s and to keep Discogs as a reliable cover/tracklist source.

class DiscogsRateLimited(RuntimeError):
    """Raised when Discogs responds with HTTP 429 (rate limited)."""


class ProviderTransientError(RuntimeError):
    """Raised when a remote provider fails due to a transient network/runtime issue."""


_discogs_lock = threading.Lock()
_discogs_next_allowed_at = 0.0
_discogs_429_streak = 0
_discogs_client = None
_discogs_client_token = None


def _provider_error_text(*args, **kwargs):
    return _discogs_runtime._provider_error_text_for_runtime(sys.modules[__name__], *args, **kwargs)


def _provider_is_name_resolution_failure(*args, **kwargs):
    return _discogs_runtime._provider_is_name_resolution_failure_for_runtime(sys.modules[__name__], *args, **kwargs)


def _discogs_min_interval_sec(*args, **kwargs):
    return _discogs_runtime._discogs_min_interval_sec_for_runtime(sys.modules[__name__], *args, **kwargs)


def _discogs_throttle(*args, **kwargs):
    return _discogs_runtime._discogs_throttle_for_runtime(sys.modules[__name__], *args, **kwargs)


def _discogs_penalize(*args, **kwargs):
    return _discogs_runtime._discogs_penalize_for_runtime(sys.modules[__name__], *args, **kwargs)


def _get_discogs_client(*args, **kwargs):
    return _discogs_runtime._get_discogs_client_for_runtime(sys.modules[__name__], *args, **kwargs)


def _get_or_create_discogs_client(*args, **kwargs):
    return _discogs_runtime._get_or_create_discogs_client_for_runtime(sys.modules[__name__], *args, **kwargs)


def _discogs_api_get_json(*args, **kwargs):
    return _discogs_runtime._discogs_api_get_json_for_runtime(sys.modules[__name__], *args, **kwargs)


def _discogs_hydrate_release_or_master_data(*args, **kwargs):
    return _discogs_runtime._discogs_hydrate_release_or_master_data_for_runtime(sys.modules[__name__], *args, **kwargs)


def _discogs_call(*args, **kwargs):
    return _discogs_runtime._discogs_call_for_runtime(sys.modules[__name__], *args, **kwargs)


def _run_discogs_preflight(*args, **kwargs):
    return _discogs_runtime._run_discogs_preflight_for_runtime(sys.modules[__name__], *args, **kwargs)




_LASTFM_API_ROOT = "https://ws.audioscrobbler.com/2.0/"
_LASTFM_AUTH_ROOT = "https://www.last.fm/api/auth/"
_LASTFM_SESSION_KEY_SETTING = "LASTFM_SESSION_KEY_ENC"
_LASTFM_SESSION_NAME_SETTING = "LASTFM_SESSION_NAME"
_LASTFM_AUTH_TOKEN_SETTING = "LASTFM_AUTH_TOKEN_ENC"
_LASTFM_LOVED_SYNC_AT_PREFIX = "LASTFM_LOVED_SYNC_AT_USER_"
























































def _run_fanart_preflight() -> tuple[bool, str]:
    """Test Fanart.tv API key validity and connectivity. Returns (ok, message)."""
    api_key = (getattr(sys.modules[__name__], "FANART_API_KEY", "") or "").strip()
    if not api_key:
        return False, "No API key"
    # The Beatles MBID (stable public artist id used only for a lightweight connectivity probe).
    probe_mbid = "b10bbbfc-cf9e-42e0-be17-e2c3e1d2600d"
    try:
        resp = requests.get(
            f"https://webservice.fanart.tv/v3/music/{probe_mbid}",
            params={"api_key": api_key},
            timeout=8,
            allow_redirects=True,
        )
        if resp.status_code == 200:
            return True, "Fanart.tv reachable"
        if resp.status_code in (401, 403):
            return False, "Invalid API key"
        if resp.status_code == 429:
            return False, "Rate limited"
        if resp.status_code == 404:
            return True, "Fanart.tv reachable"
        return False, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, f"Fanart.tv unreachable: {e}"


def _run_audiodb_preflight() -> tuple[bool, str]:
    """Test TheAudioDB API key validity and connectivity. Returns (ok, message)."""
    api_key = (getattr(sys.modules[__name__], "THEAUDIODB_API_KEY", "") or "").strip()
    if not api_key:
        return False, "No API key"
    try:
        resp = requests.get(
            f"https://www.theaudiodb.com/api/v1/json/{quote(api_key, safe='')}/searchalbum.php",
            params={"s": "Radiohead", "a": "OK Computer"},
            timeout=8,
            allow_redirects=True,
        )
        if resp.status_code == 200:
            return True, "TheAudioDB reachable"
        if resp.status_code in (401, 403):
            return False, "Invalid API key"
        if resp.status_code == 429:
            return False, "Rate limited"
        return False, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, f"TheAudioDB unreachable: {e}"


_SERPER_RUNTIME_STATUS_LOCK = threading.Lock()
_SERPER_RUNTIME_STATUS_CACHE: dict[str, Any] = {
    "checked_at": 0.0,
    "ok": None,
    "message": "",
}
_SERPER_RUNTIME_STATUS_TTL_SEC = 300.0


def _serper_response_message(resp: requests.Response | None) -> str:
    if resp is None:
        return ""
    try:
        payload = resp.json()
    except Exception:
        payload = None
    if isinstance(payload, dict):
        for key in ("message", "error", "detail"):
            value = str(payload.get(key) or "").strip()
            if value:
                return value
    return str(getattr(resp, "text", "") or "").strip()[:240]


def _serper_runtime_status(*, force: bool = False) -> tuple[bool, str]:
    now_ts = time.time()
    with _SERPER_RUNTIME_STATUS_LOCK:
        checked_at = float(_SERPER_RUNTIME_STATUS_CACHE.get("checked_at") or 0.0)
        cached_ok = _SERPER_RUNTIME_STATUS_CACHE.get("ok")
        cached_message = str(_SERPER_RUNTIME_STATUS_CACHE.get("message") or "").strip()
        if (
            not force
            and cached_ok is not None
            and checked_at > 0
            and (now_ts - checked_at) <= _SERPER_RUNTIME_STATUS_TTL_SEC
        ):
            return bool(cached_ok), cached_message

    key = (getattr(sys.modules[__name__], "SERPER_API_KEY", "") or "").strip()
    if not key:
        result = (False, "No API key (web search disabled)")
    else:
        try:
            resp = requests.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": key, "Content-Type": "application/json"},
                json={"q": "sigur ros takk review", "num": 1},
                timeout=8,
            )
            message = _serper_response_message(resp)
            message_lower = message.lower()
            if resp.status_code == 200:
                result = (True, "Serper reachable")
            elif "not enough credits" in message_lower:
                result = (False, "Not enough credits")
            elif resp.status_code == 429:
                result = (False, "Rate limited or no credits")
            elif message:
                result = (False, message)
            else:
                result = (False, f"HTTP {resp.status_code}")
        except Exception as exc:
            result = (False, f"Serper unreachable: {exc}")

    with _SERPER_RUNTIME_STATUS_LOCK:
        _SERPER_RUNTIME_STATUS_CACHE["checked_at"] = now_ts
        _SERPER_RUNTIME_STATUS_CACHE["ok"] = bool(result[0])
        _SERPER_RUNTIME_STATUS_CACHE["message"] = str(result[1] or "").strip()
    return result


def _run_serper_preflight() -> tuple[bool, str]:
    """Check Serper (web search) API key and optionally connectivity. Returns (ok, message)."""
    return _serper_runtime_status(force=True)


def _log_scan_web_search_backend_status_once() -> None:
    provider = _normalize_web_search_provider(str(getattr(sys.modules[__name__], "WEB_SEARCH_PROVIDER", "auto") or "auto"))
    if provider == "disabled":
        logging.info("[Scan Pipeline] web search backend disabled")
        return
    fallback_targets: list[str] = []
    if _ollama_web_search_enabled(allow_ai_fallback=True):
        fallback_targets.append("ollama_web_search")
    if _web_search_ai_fallback_enabled(allow_ai_fallback=True):
        fallback_targets.append("paid_ai_web_search")
    if str(getattr(sys.modules[__name__], "SERPER_API_KEY", "") or "").strip():
        serper_ok, serper_msg = _serper_runtime_status(force=True)
        if serper_ok:
            logging.info(
                "[Scan Pipeline] web search backend ready: serper%s",
                f" -> {', '.join(fallback_targets)}" if fallback_targets else "",
            )
            return
        logging.warning(
            "[Scan Pipeline] Serper unavailable at scan start: %s. Falling back to %s.",
            serper_msg or "unknown_error",
            ", ".join(fallback_targets) if fallback_targets else "no_web_search_backend",
        )
        return
    if fallback_targets:
        logging.info(
            "[Scan Pipeline] Serper not configured. Using %s for web search.",
            ", ".join(fallback_targets),
        )
        return
    logging.warning("[Scan Pipeline] no web search backend available for this scan")


def _serper_web_search(query: str, num: int = 5, *, allow_ai_fallback: bool = True) -> list[dict]:
    """Backward-compatible alias to the main provider + AI-fallback web search helper."""
    return _web_search_serper(query, num=num, allow_ai_fallback=allow_ai_fallback)


def _fetch_cover_from_web(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._fetch_cover_from_web_for_runtime(sys.modules[__name__], *args, **kwargs)



def _run_acoustid_preflight() -> tuple[bool, str]:
    """Check AcousticID: enabled, API key, and pyacoustid available. Returns (ok, message)."""
    if not getattr(sys.modules[__name__], "USE_ACOUSTID", False):
        return False, "Disabled"
    key = (getattr(sys.modules[__name__], "ACOUSTID_API_KEY", "") or "").strip()
    if not key:
        return False, "No API key"
    try:
        import acoustid  # noqa: F401
        return True, "AcousticID configured"
    except ImportError as e:
        return False, f"pyacoustid not installed: {e}"


def _run_provider_preflights_parallel() -> dict[str, tuple[bool, str]]:
    checks: list[tuple[str, Any]] = [
        ("discogs", _run_discogs_preflight),
        ("lastfm", _run_lastfm_preflight),
        ("fanart", _run_fanart_preflight),
        ("audiodb", _run_audiodb_preflight),
        ("serper", _run_serper_preflight),
        ("acoustid", _run_acoustid_preflight),
    ]
    out: dict[str, tuple[bool, str]] = {}
    with ThreadPoolExecutor(max_workers=len(checks), thread_name_prefix="pmda-preflight") as pool:
        fut_map = {pool.submit(fn): key for key, fn in checks}
        for fut in as_completed(fut_map):
            key = fut_map[fut]
            try:
                ok, msg = fut.result()
                out[key] = (bool(ok), str(msg or ""))
            except Exception as exc:
                out[key] = (False, f"{key} preflight failed: {exc}")
    for key, _ in checks:
        out.setdefault(key, (False, "No result"))
    return out


def _discogs_lookup_candidate_cap(*args, **kwargs):
    return _discogs_runtime._discogs_lookup_candidate_cap_for_runtime(sys.modules[__name__], *args, **kwargs)


def _discogs_search_identity_from_data(*args, **kwargs):
    return _discogs_runtime._discogs_search_identity_from_data_for_runtime(sys.modules[__name__], *args, **kwargs)


def _discogs_search_candidate_score(*args, **kwargs):
    return _discogs_runtime._discogs_search_candidate_score_for_runtime(sys.modules[__name__], *args, **kwargs)




def _bandcamp_lookup_candidate_cap(scores: list[float]) -> int:
    if not scores:
        return 0
    top = float(scores[0] or 0.0)
    next_score = float(scores[1] or 0.0) if len(scores) > 1 else 0.0
    gap = top - next_score
    if top >= 3.2 and gap >= 1.0:
        return 1
    if top >= 2.6:
        return 2
    if top >= 1.7:
        return 3
    return 4


def _fetch_discogs_release(*args, **kwargs):
    return _discogs_runtime._fetch_discogs_release_for_runtime(sys.modules[__name__], *args, **kwargs)


def _provider_album_search_candidate_score(*args, **kwargs):
    return _public_album_providers_runtime._provider_album_search_candidate_score_for_runtime(sys.modules[__name__], *args, **kwargs)


def _public_album_provider_headers(*args, **kwargs):
    return _public_album_providers_runtime._public_album_provider_headers_for_runtime(sys.modules[__name__], *args, **kwargs)


def _extract_json_ld_nodes(*args, **kwargs):
    return _public_album_providers_runtime._extract_json_ld_nodes_for_runtime(sys.modules[__name__], *args, **kwargs)


def _json_ld_type_matches(*args, **kwargs):
    return _public_album_providers_runtime._json_ld_type_matches_for_runtime(sys.modules[__name__], *args, **kwargs)


def _json_ld_music_album_node(*args, **kwargs):
    return _public_album_providers_runtime._json_ld_music_album_node_for_runtime(sys.modules[__name__], *args, **kwargs)


def _json_ld_artist_name(*args, **kwargs):
    return _public_album_providers_runtime._json_ld_artist_name_for_runtime(sys.modules[__name__], *args, **kwargs)


def _json_ld_tracklist(*args, **kwargs):
    return _public_album_providers_runtime._json_ld_tracklist_for_runtime(sys.modules[__name__], *args, **kwargs)


def _provider_album_meta_fallback(*args, **kwargs):
    return _public_album_providers_runtime._provider_album_meta_fallback_for_runtime(sys.modules[__name__], *args, **kwargs)


def _parse_public_album_page_payload(*args, **kwargs):
    return _public_album_providers_runtime._parse_public_album_page_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


def _spotify_album_page_urls(*args, **kwargs):
    return _public_album_providers_runtime._spotify_album_page_urls_for_runtime(sys.modules[__name__], *args, **kwargs)


def _qobuz_album_page_urls(*args, **kwargs):
    return _public_album_providers_runtime._qobuz_album_page_urls_for_runtime(sys.modules[__name__], *args, **kwargs)


def _tidal_album_page_urls(*args, **kwargs):
    return _public_album_providers_runtime._tidal_album_page_urls_for_runtime(sys.modules[__name__], *args, **kwargs)


def _itunes_cover_url_candidates(*args, **kwargs):
    return _public_album_providers_runtime._itunes_cover_url_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_itunes_album_info(*args, **kwargs):
    return _public_album_providers_runtime._fetch_itunes_album_info_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_deezer_album_info(*args, **kwargs):
    return _public_album_providers_runtime._fetch_deezer_album_info_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_spotify_album_info(*args, **kwargs):
    return _public_album_providers_runtime._fetch_spotify_album_info_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_qobuz_album_info(*args, **kwargs):
    return _public_album_providers_runtime._fetch_qobuz_album_info_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_tidal_album_info(*args, **kwargs):
    return _public_album_providers_runtime._fetch_tidal_album_info_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_audiodb_album_info(*args, **kwargs):
    return _public_album_providers_runtime._fetch_audiodb_album_info_for_runtime(sys.modules[__name__], *args, **kwargs)




def _strip_html_text(value: str) -> str:
    txt = str(value or "")
    if not txt:
        return ""
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = txt.replace("&amp;", "&").replace("&quot;", '"').replace("&#39;", "'")
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _truncate_text(value: str, max_chars: int = 420) -> str:
    txt = _strip_html_text(value)
    if len(txt) <= max_chars:
        return txt
    clipped = txt[:max_chars].rsplit(" ", 1)[0].strip()
    return f"{clipped}..." if clipped else txt[:max_chars]


def _safe_nonneg_int(value: Any) -> int:
    try:
        return max(0, int(float(str(value or 0).strip())))
    except Exception:
        return 0


def _safe_bounded_float(value: Any, minimum: float = 0.0, maximum: float = 5.0) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return max(minimum, min(maximum, out))


def _album_heat_label(score: float | None) -> str | None:
    return None


def _compute_album_heat_score(
    *,
    discogs_have_count: int = 0,
    discogs_want_count: int = 0,
    bandcamp_supporter_count: int = 0,
    lastfm_scrobbles: int = 0,
    lastfm_listeners: int = 0,
    public_rating: float | None = None,
    public_rating_votes: int = 0,
) -> float | None:
    have_norm = min(1.0, math.log10(discogs_have_count + 1) / 4.2) if discogs_have_count > 0 else 0.0
    want_norm = min(1.0, math.log10(discogs_want_count + 1) / 4.0) if discogs_want_count > 0 else 0.0
    bandcamp_norm = min(1.0, math.log10(bandcamp_supporter_count + 1) / 3.0) if bandcamp_supporter_count > 0 else 0.0
    scrobble_norm = min(1.0, math.log10(lastfm_scrobbles + 1) / 6.2) if lastfm_scrobbles > 0 else 0.0
    listener_norm = min(1.0, math.log10(lastfm_listeners + 1) / 5.4) if lastfm_listeners > 0 else 0.0
    rating_norm = min(1.0, max(0.0, float(public_rating or 0.0)) / 5.0) if public_rating is not None else 0.0
    vote_norm = min(1.0, math.log10(max(0, int(public_rating_votes or 0)) + 1) / 3.0) if public_rating_votes else 0.0
    score = (
        (have_norm * 0.20)
        + (want_norm * 0.18)
        + (bandcamp_norm * 0.15)
        + (scrobble_norm * 0.24)
        + (listener_norm * 0.13)
        + (rating_norm * 0.07)
        + (vote_norm * 0.03)
    )
    if score <= 0:
        return None
    return round(max(0.0, min(100.0, score * 100.0)), 1)


def _derive_public_rating_from_heat(heat_score: float | None) -> float | None:
    if heat_score is None:
        return None
    scaled = 2.2 + (max(0.0, min(100.0, float(heat_score))) / 100.0) * 2.6
    return round(max(0.0, min(5.0, scaled)), 1)


def _normalize_bandcamp_supporter_comments(raw_comments: Any, limit: int = 12) -> list[dict[str, str]]:
    if not isinstance(raw_comments, list):
        return []
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    max_items = max(1, min(int(limit or 12), 24))
    for item in raw_comments:
        if not isinstance(item, dict):
            continue
        text = _strip_html_text(str(item.get("text") or "").strip())
        if not text:
            continue
        author = _strip_html_text(str(item.get("author") or "").strip())
        key = f"{author.lower()}|{text.lower()}"
        if key in seen:
            continue
        seen.add(key)
        normalized: dict[str, str] = {"text": _truncate_text(text, max_chars=600)}
        if author:
            normalized["author"] = author
        for key_name in ("url", "avatar_url"):
            value = str(item.get(key_name) or "").strip()
            if value:
                normalized[key_name] = value
        out.append(normalized)
        if len(out) >= max_items:
            break
    return out


def _merge_album_public_metrics(*metrics_sources: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "public_rating": None,
        "public_rating_votes": 0,
        "public_rating_source": "",
        "discogs_have_count": 0,
        "discogs_want_count": 0,
        "bandcamp_supporter_count": 0,
        "bandcamp_supporter_comments": [],
        "lastfm_scrobbles": 0,
        "lastfm_listeners": 0,
        "heat_score": None,
        "heat_label": None,
    }
    explicit_rating = None
    explicit_votes = 0
    explicit_source = ""
    for metrics in metrics_sources:
        if not isinstance(metrics, dict):
            continue
        merged["discogs_have_count"] = max(int(merged["discogs_have_count"] or 0), _safe_nonneg_int(metrics.get("discogs_have_count")))
        merged["discogs_want_count"] = max(int(merged["discogs_want_count"] or 0), _safe_nonneg_int(metrics.get("discogs_want_count")))
        merged["bandcamp_supporter_count"] = max(int(merged["bandcamp_supporter_count"] or 0), _safe_nonneg_int(metrics.get("bandcamp_supporter_count")))
        if not merged["bandcamp_supporter_comments"]:
            merged["bandcamp_supporter_comments"] = _normalize_bandcamp_supporter_comments(metrics.get("bandcamp_supporter_comments"))
        merged["lastfm_scrobbles"] = max(int(merged["lastfm_scrobbles"] or 0), _safe_nonneg_int(metrics.get("lastfm_scrobbles")))
        merged["lastfm_listeners"] = max(int(merged["lastfm_listeners"] or 0), _safe_nonneg_int(metrics.get("lastfm_listeners")))
        cand_rating = _safe_bounded_float(metrics.get("public_rating"))
        cand_votes = _safe_nonneg_int(metrics.get("public_rating_votes"))
        cand_source = str(metrics.get("public_rating_source") or "").strip()
        if cand_rating is not None and (explicit_rating is None or cand_votes > explicit_votes):
            explicit_rating = cand_rating
            explicit_votes = cand_votes
            explicit_source = cand_source
    heat_score = _compute_album_heat_score(
        discogs_have_count=int(merged["discogs_have_count"] or 0),
        discogs_want_count=int(merged["discogs_want_count"] or 0),
        bandcamp_supporter_count=int(merged["bandcamp_supporter_count"] or 0),
        lastfm_scrobbles=int(merged["lastfm_scrobbles"] or 0),
        lastfm_listeners=int(merged["lastfm_listeners"] or 0),
        public_rating=explicit_rating,
        public_rating_votes=explicit_votes,
    )
    public_rating = explicit_rating
    public_rating_votes = explicit_votes
    public_rating_source = explicit_source
    if public_rating is None and heat_score is not None:
        public_rating = _derive_public_rating_from_heat(heat_score)
        public_rating_source = "aggregated"
        public_rating_votes = 0
    merged["public_rating"] = public_rating
    merged["public_rating_votes"] = public_rating_votes
    merged["public_rating_source"] = public_rating_source or None
    merged["heat_score"] = heat_score
    merged["heat_label"] = _album_heat_label(heat_score)
    return merged


def _norm_artist_key(name: str) -> str:
    """Stable accent-insensitive key for artist-name lookups and cache joins."""
    return _normalize_identity_text_strict(name or "")


def _files_box_set_normalize_path(value: Any) -> str:
    return _box_set_runtime.files_box_set_normalize_path(value)


def _files_box_set_parent_path(value: Any) -> str:
    return _box_set_runtime.files_box_set_parent_path(value)


def _files_box_set_leaf_name(value: Any) -> str:
    return _box_set_runtime.files_box_set_leaf_name(value)


def _files_box_set_identity_key(row: Mapping[str, Any] | None) -> str:
    return _box_set_runtime.files_box_set_identity_key(
        row,
        normalize_identity=_normalize_identity_text_strict,
    )


def _files_box_set_group_key(row: Mapping[str, Any] | None) -> str:
    return _box_set_runtime.files_box_set_group_key(
        row,
        normalize_identity=_normalize_identity_text_strict,
    )


def _files_box_set_group_is_valid(rows: Sequence[Mapping[str, Any]]) -> bool:
    return _box_set_runtime.files_box_set_group_is_valid(rows)


def _files_box_set_member_sort_key(row: Mapping[str, Any]) -> tuple[int, int, str, int]:
    return _box_set_runtime.files_box_set_member_sort_key(row)


def _files_box_set_display_artist(rows: Sequence[Mapping[str, Any]]) -> tuple[str, int]:
    return _box_set_runtime.files_box_set_display_artist(rows, normalize_artist_key=_norm_artist_key)


def _collapse_files_album_browse_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return _box_set_runtime.collapse_files_album_browse_rows_for_runtime(sys.modules[__name__], rows)


def _files_box_set_reindex_tracks(
    track_rows: Sequence[Mapping[str, Any]],
    member_album_ids: Sequence[int],
) -> tuple[list[dict[str, Any]], int]:
    return _box_set_runtime.files_box_set_reindex_tracks(track_rows, member_album_ids)


def _files_artist_bucket_key(name: str, *, classical_hint: bool = False) -> str:
    base_key = _norm_artist_key(name)
    if not base_key:
        return ""
    if not classical_hint:
        return base_key
    sig = _classical_person_alias_signature(name)
    surname = str(sig.get("surname") or "").strip()
    initials = sorted({str(ch or "").strip()[:1] for ch in (sig.get("initials") or set()) if str(ch or "").strip()})
    if surname and initials:
        return f"classical-person:{surname}:{initials[0]}"
    return base_key


def _identity_display_quality_score(value: str) -> tuple[int, int, int, int, int]:
    txt = " ".join(str(value or "").split()).strip()
    if not txt:
        return (-1, -1, -1, -1, -1)
    non_generic = 0 if _identity_text_is_generic(txt) else 1
    has_non_ascii = 1 if any(ord(ch) > 127 for ch in txt) else 0
    has_punctuation = 1 if any(ch in txt for ch in "…'’&-:;,./()[]{}") else 0
    has_mixed_case = 1 if any(ch.islower() for ch in txt) and any(ch.isupper() for ch in txt) else 0
    return (non_generic, has_non_ascii, has_punctuation, has_mixed_case, len(txt))


def _choose_preferred_identity_display(current_value: str, candidate_value: str) -> str:
    current_txt = " ".join(str(current_value or "").split()).strip()
    candidate_txt = " ".join(str(candidate_value or "").split()).strip()
    if not candidate_txt:
        return current_txt
    if not current_txt:
        return candidate_txt
    current_norm = _normalize_identity_text_strict(current_txt)
    candidate_norm = _normalize_identity_text_strict(candidate_txt)
    if current_norm and candidate_norm and current_norm != candidate_norm:
        return current_txt
    if _identity_display_quality_score(candidate_txt) > _identity_display_quality_score(current_txt):
        return candidate_txt
    return current_txt


def _word_count(text: str) -> int:
    try:
        return len(re.findall(r"[A-Za-z0-9]+(?:'[A-Za-z0-9]+)?", (text or "")))
    except Exception:
        return 0


_LASTFM_BIO_GARBAGE_PATTERNS = (
    r"read more on last\.?fm\.?$",
    r"read more on last\.?fm.*$",
    r"user-contributed text is available under the creative commons.*$",
    r"additional terms may apply.*$",
)




def _is_garbage_bio(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return True
    low = t.lower()
    if "read more on last.fm" in low or "read more on lastfm" in low:
        return True
    if low.startswith("read more on last.fm"):
        return True
    if "user-contributed text is available under" in low:
        return True
    # A handful of tokens is effectively useless as a bio.
    if _word_count(t) < 12:
        return True
    return False


def _is_acceptable_original_bio(text: str) -> bool:
    # Spec target: accept original sources when >= 100 words.
    return _word_count(text or "") >= 100


def _text_mentions_identity_phrase(identity: str, text: str) -> bool:
    """
    Conservative relevance check:
    - exact normalized phrase hit is ideal;
    - for multi-token identities, allow 2+ token hits.
    """
    ident = _normalize_identity_text_strict(identity)
    body = _normalize_identity_text_strict(text)
    if not ident or not body:
        return False
    try:
        if re.search(rf"(?<!\w){re.escape(ident)}(?!\w)", body):
            return True
    except Exception:
        pass
    tokens = [tok for tok in ident.split() if len(tok) >= 3]
    if len(tokens) < 2:
        return False
    hits = 0
    for tok in tokens:
        try:
            if re.search(rf"(?<!\w){re.escape(tok)}(?!\w)", body):
                hits += 1
        except Exception:
            continue
    return hits >= 2


def _is_relevant_artist_profile_text(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._is_relevant_artist_profile_text_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_profile_text_looks_music_related(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._artist_profile_text_looks_music_related_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_profile_text_looks_biographical(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._artist_profile_text_looks_biographical_for_runtime(sys.modules[__name__], *args, **kwargs)


def _is_relevant_album_profile_text(
    artist_name: str,
    album_title: str,
    text: str,
    *,
    allow_short_album_fallback: bool = False,
) -> bool:
    txt = str(text or "").strip()
    if not txt:
        return False
    artist_ok = _text_mentions_identity_phrase(str(artist_name or ""), txt)
    album_ok = _text_mentions_identity_phrase(str(album_title or ""), txt)
    album_norm = _normalize_identity_album_strict(album_title or "")
    if artist_ok and album_ok:
        return True
    # Optional fallback for very short titles (e.g. "X", "IV", "Takk") when artist mention is clear.
    if allow_short_album_fallback and artist_ok and 0 < len(album_norm) <= 4:
        return True
    return False




def _commons_file_path_url(filename: str) -> str:
    return _wikipedia_runtime.commons_file_path_url(filename)


def _fetch_wikidata_media_url(entity_id: str, preferred_props: tuple[str, ...] = ("P18", "P154")) -> str:
    return _wikipedia_runtime.fetch_wikidata_media_url(entity_id, preferred_props=preferred_props)


def _fetch_wikipedia_page_metadata(title: str, lang: str = "en", thumb_px: int = 640) -> dict[str, str]:
    return _wikipedia_runtime.fetch_wikipedia_page_metadata(title, lang=lang, thumb_px=thumb_px)


def _wikipedia_title_from_fullurl(url: str) -> str:
    return _wikipedia_runtime.wikipedia_title_from_fullurl(url)


def _fetch_wikipedia_pageimage(title: str, lang: str = "en", thumb_px: int = 640) -> str:
    return _wikipedia_runtime.fetch_wikipedia_pageimage(title, lang=lang, thumb_px=thumb_px)


def _fetch_wikimedia_commons_artist_image(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_wikimedia_commons_artist_image_for_runtime(sys.modules[__name__], *args, **kwargs)


def _resolve_authoritative_artist_image_url(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._resolve_authoritative_artist_image_url_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_wikipedia_intro_extract(title: str, lang: str = "en") -> tuple[str, str, str]:
    return _wikipedia_runtime.fetch_wikipedia_intro_extract(title, lang=lang)


def _artist_profile_search_queries(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._artist_profile_search_queries_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_wikipedia_artist_bio(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_wikipedia_artist_bio_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_profile_payload_requires_refresh(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._artist_profile_payload_requires_refresh_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_cached_image_provider_is_provider_first(
    *,
    provider: str = "",
    image_url: str = "",
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> bool:
    return _wikipedia_runtime.artist_cached_image_provider_is_provider_first(
        provider=provider,
        image_url=image_url,
        entity_kind=entity_kind,
        role_hints=role_hints,
    )


_ARTIST_MULTI_CREDIT_SPLIT_PATTERNS = (
    r"\s+feat(?:uring)?\.?\s+",
    r"\s+ft\.?\s+",
    r"\s+with\s+",
    r"\s+vs\.?\s+",
    r"\s+versus\s+",
    r"\s+/\s+",
    r"\s+&\s+",
    r"\s+and\s+",
    r"\s+\+\s+",
)
_ARTIST_MULTI_CREDIT_PREFIX_RE = re.compile(r"^(?:feat(?:uring)?\.?|ft\.?|with|vs\.?|versus)\s+", flags=re.IGNORECASE)


def _split_artist_entities_for_profiles(artist_name: str) -> list[str]:
    """
    Split multi-artist credits into distinct artist entities for profile enrichment.
    Examples: "A / B", "A feat. B", "A & B", "A and B", "A featuring B".
    """
    raw = " ".join((artist_name or "").split()).strip()
    if not raw:
        return []

    work = raw
    changed = False
    # Convert parenthesized featuring credits into normal separators.
    paren_expanded = re.sub(
        r"\((?:\s*(?:feat(?:uring)?\.?|ft\.?|with|vs\.?|versus)\s+([^)]{1,120})\s*)\)",
        r" | \1 ",
        work,
        flags=re.IGNORECASE,
    )
    if paren_expanded != work:
        changed = True
        work = paren_expanded
    for pat in _ARTIST_MULTI_CREDIT_SPLIT_PATTERNS:
        updated = re.sub(pat, " | ", work, flags=re.IGNORECASE)
        if updated != work:
            changed = True
            work = updated
    if not changed:
        return [raw]

    out: list[str] = []
    seen: set[str] = set()
    for token in work.split("|"):
        part = str(token or "").strip(" \t\r\n-–—,;:/\\()[]{}")
        part = _ARTIST_MULTI_CREDIT_PREFIX_RE.sub("", part).strip()
        part = re.sub(r"\s+", " ", part).strip()
        if not part or len(part) < 2:
            continue
        key = _norm_artist_key(part)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(part)
        if len(out) >= 4:
            break
    return out or [raw]


def _build_single_artist_profile_payload(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._build_single_artist_profile_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


def _build_artist_profile_payload(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._build_artist_profile_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


_last_bandcamp_request = 0.0
_bandcamp_lock = threading.Lock()


def _dedupe_keep_order(items: List[str]) -> List[str]:
    return _wikipedia_runtime.dedupe_keep_order(items)


def _merge_artist_profile_tags(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._merge_artist_profile_tags_for_runtime(sys.modules[__name__], *args, **kwargs)


def _merge_similar_artist_candidates(*similar_sources: Any, limit: int = 20) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    max_items = max(1, min(int(limit or 20), 40))
    for source in similar_sources:
        if not isinstance(source, list):
            continue
        for item in source:
            normalized: dict[str, Any] | None = None
            mbid = ""
            name = ""
            if isinstance(item, dict):
                name = " ".join(str(item.get("name") or "").split()).strip()
                mbid = str(item.get("mbid") or "").strip()
                if not name:
                    continue
                normalized = {**item, "name": name}
                if mbid:
                    normalized["mbid"] = mbid
                else:
                    normalized.pop("mbid", None)
            else:
                name = " ".join(str(item or "").split()).strip()
                if not name:
                    continue
                normalized = {"name": name}
            key = f"mbid:{mbid.lower()}" if mbid else f"name:{_norm_artist_key(name)}"
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(normalized)
            if len(merged) >= max_items:
                return merged
    return merged


def _bandcamp_cover_url_candidates(cover_url: str) -> List[str]:
    """
    Expand a Bandcamp cover URL into likely higher-resolution candidates.
    Bandcamp URLs usually look like:
      https://f4.bcbits.com/img/a1234567890_16.jpg
    Where `_0` is typically the highest available resolution.
    """
    base = (cover_url or "").strip()
    if not base:
        return []
    candidates = [base]
    m = re.match(r"^(https?://[^/]+/img/(?:[ab])?\d+)_([0-9]+)(\.[a-zA-Z0-9]+)(\?.*)?$", base)
    if m:
        prefix, _size, ext, query = m.groups()
        q = query or ""
        for sz in ("0", "10", "1024", "700", "16", "23", "21"):
            candidates.append(f"{prefix}_{sz}{ext}{q}")
    return _dedupe_keep_order(candidates)


def _bandcamp_preferred_image_url(cover_url: str, *, preferred_size: str = "10") -> str:
    """
    Promote a Bandcamp image URL to a larger deterministic variant when possible.
    This is important for owner/label avatars that often arrive as tiny `_21` thumbnails.
    """
    base = (cover_url or "").strip()
    if not base:
        return ""
    m = re.match(r"^(https?://[^/]+/img/(?:[ab])?\d+)_([0-9]+)(\.[a-zA-Z0-9]+)(\?.*)?$", base)
    if not m:
        return base
    prefix, _size, ext, query = m.groups()
    q = query or ""
    size = str(preferred_size or "").strip() or "10"
    return f"{prefix}_{size}{ext}{q}"




def _download_best_cover_image(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._download_best_cover_image_for_runtime(sys.modules[__name__], *args, **kwargs)



def _infer_genre_from_bandcamp_tags(tags: List[str]) -> Optional[str]:
    return _genre_runtime._infer_genre_from_bandcamp_tags_for_runtime(sys.modules[__name__], tags)


def _fetch_bandcamp_album_info(*args, **kwargs):
    return _bandcamp_runtime.fetch_bandcamp_album_info_for_runtime(sys.modules[__name__], *args, **kwargs)


def _extract_text_from_openai_response(*args, **kwargs):
    return _web_search_runtime._extract_text_from_openai_response_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_extract_json_array(*args, **kwargs):
    return _web_search_runtime._assistant_extract_json_array_for_runtime(sys.modules[__name__], *args, **kwargs)


def _assistant_extract_json_object(*args, **kwargs):
    return _web_search_runtime._assistant_extract_json_object_for_runtime(sys.modules[__name__], *args, **kwargs)


def _normalize_web_results(*args, **kwargs):
    return _web_search_runtime._normalize_web_results_for_runtime(sys.modules[__name__], *args, **kwargs)


_OLLAMA_WEB_SEARCH_TIMEOUT_SEC = 45
_OLLAMA_WEB_SEARCH_COMPLEX_TIMEOUT_SEC = 90
_OLLAMA_WEB_SEARCH_MIN_CONFIDENCE = 0.72
_OLLAMA_WEB_SEARCH_COMPLEX_MIN_CONFIDENCE = 0.66
_OLLAMA_WEB_SEARCH_SEED_MAX_RESULTS = 8
_OLLAMA_WEB_SEARCH_PAGE_CONTEXT_RESULTS = 3
_DUCKDUCKGO_SEARCH_TIMEOUT_SEC = 12
_OLLAMA_WEB_SEARCH_MAX_OUTPUT_TOKENS = 240
_OLLAMA_WEB_SEARCH_ALLOWED_DOMAINS = (
    "allmusic.com",
    "bandcamp.com",
    "discogs.com",
    "last.fm",
    "musicbrainz.org",
    "pitchfork.com",
    "rateyourmusic.com",
    "residentadvisor.net",
    "rollingstone.com",
    "stereogum.com",
    "theguardian.com",
    "wikipedia.org",
)
_OLLAMA_WEB_REVIEW_ALLOWED_DOMAINS = (
    "allmusic.com",
    "albumoftheyear.org",
    "clashmusic.com",
    "lineofbestfit.com",
    "nme.com",
    "pitchfork.com",
    "popmatters.com",
    "progarchives.com",
    "rateyourmusic.com",
    "residentadvisor.net",
    "rollingstone.com",
    "sputnikmusic.com",
    "stereogum.com",
    "theguardian.com",
    "thequietus.com",
    "treblezine.com",
    "undertheradarmag.com",
)
_REVIEW_METADATA_ONLY_DOMAINS = (
    "bandcamp.com",
    "discogs.com",
    "last.fm",
    "musicbrainz.org",
    "wikipedia.org",
)
_REVIEW_METADATA_ONLY_MARKERS = (
    "album page",
    "album page and credits",
    "artist page",
    "catalog",
    "credits",
    "discography",
    "encyclopedia",
    "metadata",
    "release group",
    "release information",
    "track list",
    "track listing",
    "tracklist",
    "wiki",
)
_REVIEW_SIGNAL_MARKERS = (
    "album review",
    "best new music",
    "critic",
    "critique",
    "rated",
    "rating",
    "review",
    "reviewed",
    "score",
    "stars",
)


_ai_web_search_cache_lock = threading.Lock()
_ai_web_search_cache: dict[str, dict[str, Any]] = {}
_ai_web_search_run_seen_lock = threading.Lock()
_ai_web_search_run_seen: dict[str, set[str]] = {}


def _ai_web_search_cache_key(*args, **kwargs):
    return _web_search_runtime._ai_web_search_cache_key_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ai_web_search_cache_get(*args, **kwargs):
    return _web_search_runtime._ai_web_search_cache_get_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ai_web_search_cache_lookup(*args, **kwargs):
    return _web_search_runtime._ai_web_search_cache_lookup_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ai_web_search_cache_set(*args, **kwargs):
    return _web_search_runtime._ai_web_search_cache_set_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ai_query_cache_get(*args, **kwargs):
    return _web_search_runtime._ai_query_cache_get_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ai_query_cache_set(*args, **kwargs):
    return _web_search_runtime._ai_query_cache_set_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ai_web_search_run_key(*args, **kwargs):
    return _web_search_runtime._ai_web_search_run_key_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ai_web_search_mark_run_query_seen(*args, **kwargs):
    return _web_search_runtime._ai_web_search_mark_run_query_seen_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ai_web_search_budget_allows(*args, **kwargs):
    return _web_search_runtime._ai_web_search_budget_allows_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ai_web_search_available(*args, **kwargs):
    return _web_search_runtime._ai_web_search_available_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ollama_web_search_enabled(*args, **kwargs):
    return _web_search_runtime._ollama_web_search_enabled_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ollama_web_search_context_lines(*args, **kwargs):
    return _web_search_runtime._ollama_web_search_context_lines_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ollama_web_search_should_retry_complex(*args, **kwargs):
    return _web_search_runtime._ollama_web_search_should_retry_complex_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ollama_web_search_allowed_domains(*args, **kwargs):
    return _web_search_runtime._ollama_web_search_allowed_domains_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ollama_web_search_prompt(*args, **kwargs):
    return _web_search_runtime._ollama_web_search_prompt_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ollama_web_search_response_schema(*args, **kwargs):
    return _web_search_runtime._ollama_web_search_response_schema_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ollama_chat_json(*args, **kwargs):
    return _web_search_runtime._ollama_chat_json_for_runtime(sys.modules[__name__], *args, **kwargs)


def _duckduckgo_html_search_http(*args, **kwargs):
    return _web_search_runtime._duckduckgo_html_search_http_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ollama_web_search_seed_hits(*args, **kwargs):
    return _web_search_runtime._ollama_web_search_seed_hits_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ollama_web_search_enrich_seed_rows(*args, **kwargs):
    return _web_search_runtime._ollama_web_search_enrich_seed_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_hit_domain(*args, **kwargs):
    return _web_search_runtime._review_hit_domain_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_domain_matches(*args, **kwargs):
    return _web_search_runtime._review_domain_matches_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_row_text(*args, **kwargs):
    return _web_search_runtime._review_row_text_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_hit_has_signal(*args, **kwargs):
    return _web_search_runtime._review_hit_has_signal_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_hit_is_metadata_only(*args, **kwargs):
    return _web_search_runtime._review_hit_is_metadata_only_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_filter_primary_hits(*args, **kwargs):
    return _web_search_runtime._review_filter_primary_hits_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ollama_web_search_parse_rows(*args, **kwargs):
    return _web_search_runtime._ollama_web_search_parse_rows_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ollama_web_search(*args, **kwargs):
    return _web_search_runtime._ollama_web_search_for_runtime(sys.modules[__name__], *args, **kwargs)


def _is_openai_web_search_unsupported_error(*args, **kwargs):
    return _web_search_runtime._is_openai_web_search_unsupported_error_for_runtime(sys.modules[__name__], *args, **kwargs)


def _openai_web_search_model_candidates(*args, **kwargs):
    return _web_search_runtime._openai_web_search_model_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)


def _openai_web_search_fallback(*args, **kwargs):
    return _web_search_runtime._openai_web_search_fallback_for_runtime(sys.modules[__name__], *args, **kwargs)


def _web_search_ai_fallback_enabled(*args, **kwargs):
    return _web_search_runtime._web_search_ai_fallback_enabled_for_runtime(sys.modules[__name__], *args, **kwargs)


def _web_search_provider_order(*args, **kwargs):
    return _web_search_runtime._web_search_provider_order_for_runtime(sys.modules[__name__], *args, **kwargs)


def _web_search_serper_http(*args, **kwargs):
    return _web_search_runtime._web_search_serper_http_for_runtime(sys.modules[__name__], *args, **kwargs)


def _web_search_serper(*args, **kwargs):
    return _web_search_runtime._web_search_serper_for_runtime(sys.modules[__name__], *args, **kwargs)


def _maintenance_reset_sqlite_db(db_path: Path, reinit_fn) -> dict[str, Any]:
    removed_files: list[str] = []
    errors: list[str] = []
    candidates = [db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")]
    for p in candidates:
        try:
            if p.exists():
                p.unlink()
                removed_files.append(str(p))
        except FileNotFoundError:
            pass
        except Exception as e:
            errors.append(f"{p}: {e}")
    reinitialized = False
    try:
        reinit_fn()
        reinitialized = True
        if db_path == STATE_DB_FILE:
            # A live state.db reset happens after module startup, so scheduler defaults
            # must be reseeded explicitly or post-scan chain jobs vanish until restart.
            _scheduler_insert_default_rules_if_empty()
            _scheduler_migrate_legacy_scan_changed_default()
            _scheduler_ensure_post_scan_chain_defaults()
            _pipeline_migrate_legacy_post_scan_async_default()
            _library_migrate_legacy_include_unmatched_default()
            _web_search_migrate_legacy_provider_default()
    except Exception as e:
        errors.append(f"reinit failed: {e}")
    return {
        "db_path": str(db_path),
        "removed_files": removed_files,
        "reinitialized": reinitialized,
        "ok": len(errors) == 0,
        "errors": errors,
    }


def _maintenance_clear_artwork_ram_cache(*args: Any, **kwargs: Any) -> Any:
    return _maintenance_runtime._maintenance_clear_artwork_ram_cache_for_runtime(sys.modules[__name__], *args, **kwargs)


def _maintenance_clear_media_cache(*args: Any, **kwargs: Any) -> Any:
    return _maintenance_runtime._maintenance_clear_media_cache_for_runtime(sys.modules[__name__], *args, **kwargs)


def _maintenance_clear_export_root(*args: Any, **kwargs: Any) -> Any:
    return _maintenance_runtime._maintenance_clear_export_root_for_runtime(sys.modules[__name__], *args, **kwargs)


def _maintenance_clear_files_index(*args: Any, **kwargs: Any) -> Any:
    return _maintenance_runtime._maintenance_clear_files_index_for_runtime(sys.modules[__name__], *args, **kwargs)


def _do_plex_check(host: str, token: str) -> tuple[bool, str]:
    from pmda_integrations.player_sync import check_plex

    result = check_plex(host, token)
    return bool(result.success), str(result.message or "")


def _normalize_player_target(raw: str | None) -> str:
    from pmda_integrations.player_sync import normalize_player_target

    return normalize_player_target(raw)


def _normalize_http_base_url(url: str) -> str:
    from pmda_integrations.player_sync import normalize_http_base_url

    return normalize_http_base_url(url)


def _jellyfin_auth_headers(api_key: str) -> dict[str, str]:
    from pmda_integrations.player_sync import jellyfin_auth_headers

    return jellyfin_auth_headers(api_key)


def _do_jellyfin_check(url: str, api_key: str) -> tuple[bool, str]:
    from pmda_integrations.player_sync import check_jellyfin

    result = check_jellyfin(url, api_key)
    return bool(result.success), str(result.message or "")


def _trigger_jellyfin_refresh(url: str, api_key: str) -> tuple[bool, str]:
    from pmda_integrations.player_sync import trigger_jellyfin_refresh

    result = trigger_jellyfin_refresh(url, api_key)
    return bool(result.success), str(result.message or "")


def _navidrome_auth_params(username: str, password: str, api_key: str = "") -> tuple[dict[str, str], str]:
    from pmda_integrations.player_sync import navidrome_auth_params

    return navidrome_auth_params(username, password, api_key)


def _do_navidrome_check(url: str, username: str, password: str, api_key: str = "") -> tuple[bool, str]:
    from pmda_integrations.player_sync import check_navidrome

    result = check_navidrome(url, username, password, api_key)
    return bool(result.success), str(result.message or "")


def _trigger_navidrome_refresh(url: str, username: str, password: str, api_key: str = "") -> tuple[bool, str]:
    from pmda_integrations.player_sync import trigger_navidrome_refresh

    result = trigger_navidrome_refresh(url, username, password, api_key)
    return bool(result.success), str(result.message or "")


def _effective_player_target(explicit_target: str | None = None) -> str:
    if explicit_target is not None:
        raw_target = str(explicit_target or "").strip()
        if raw_target:
            return _normalize_player_target(raw_target)
    return _normalize_player_target(getattr(sys.modules[__name__], "PIPELINE_PLAYER_TARGET", "none"))


def _trigger_player_refresh_by_target(target: str) -> tuple[bool, str]:
    tgt = _normalize_player_target(target)
    if tgt == "none":
        return False, "No player sync target configured"
    if tgt == "plex":
        from pmda_integrations.player_sync import trigger_plex_refresh

        result = trigger_plex_refresh(
            getattr(sys.modules[__name__], "PLEX_HOST", ""),
            getattr(sys.modules[__name__], "PLEX_TOKEN", ""),
            getattr(sys.modules[__name__], "SECTION_IDS", []),
        )
        return bool(result.success), str(result.message or "")
    if tgt == "jellyfin":
        return _trigger_jellyfin_refresh(
            getattr(sys.modules[__name__], "JELLYFIN_URL", ""),
            getattr(sys.modules[__name__], "JELLYFIN_API_KEY", ""),
        )
    if tgt == "navidrome":
        return _trigger_navidrome_refresh(
            getattr(sys.modules[__name__], "NAVIDROME_URL", ""),
            getattr(sys.modules[__name__], "NAVIDROME_USERNAME", ""),
            getattr(sys.modules[__name__], "NAVIDROME_PASSWORD", ""),
            getattr(sys.modules[__name__], "NAVIDROME_API_KEY", ""),
        )
    return False, f"Unsupported sync target: {tgt}"


def api_openai_check(*args: Any, **kwargs: Any) -> Any:
    return _ai_provider_config_runtime.api_openai_check_for_runtime(sys.modules[__name__], *args, **kwargs)


# ───────────────────────── Last.fm user auth / scrobble ─────────────────────────











# ───────────────────────── OpenAI OAuth (ChatGPT / Codex) ─────────────────────────

_OPENAI_OAUTH_ISSUER = "https://auth.openai.com"
# This is the public OAuth client_id used by OpenAI Codex "Sign in with ChatGPT".
# It enables a device-code flow that works well for server-hosted PMDA (no localhost callback).
_OPENAI_OAUTH_CODEX_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"

def _openai_codex_oauth_start_impl() -> tuple[Response, int]:
    try:
        result = _openai_auth_service().start_device_flow(_current_user_id_or_zero())
        payload = {
            "ok": bool(result.ok),
            "session_id": str(result.session_id or ""),
            "verification_url": str(result.verification_url or ""),
            "user_code": str(result.user_code or ""),
            "interval": int(result.interval or 5),
            "message": str(result.message or ""),
            "warning": str(result.warning or ""),
        }
        status = 200 if result.ok else 500
        return jsonify(payload), status
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc) or "OAuth start failed"}), 500


def _openai_codex_oauth_poll_impl() -> tuple[Response, int]:
    data = request.get_json(silent=True) or {}
    session_id = str(data.get("session_id") or "").strip()
    if not session_id:
        return jsonify({"status": "error", "message": "session_id is required"}), 400
    try:
        result = _openai_auth_service().poll_device_flow(
            session_id=session_id,
            user_id=_current_user_id_or_zero(),
        )
        payload = {
            "status": str(result.status or "error"),
            "message": str(result.message or ""),
        }
        if int(result.retry_after or 0) > 0:
            payload["retry_after"] = int(result.retry_after)
        if result.api_key_saved is not None:
            payload["api_key_saved"] = bool(result.api_key_saved)
        # Keep poll responses HTTP-200 for pending/completed/error state transitions
        # so the UI can always update state deterministically.
        http_status = 200
        if payload["status"] == "completed":
            _openai_codex_health_cache_invalidate(_current_user_id_or_zero())
        return jsonify(payload), http_status
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc) or "OAuth poll failed"}), 500


def api_openai_codex_oauth_device_start():
    return _openai_codex_oauth_start_impl()


def api_openai_codex_oauth_device_poll():
    return _openai_codex_oauth_poll_impl()


def api_openai_codex_oauth_status():
    try:
        uid = _current_user_id_or_zero()
        runtime_check = _parse_bool(request.args.get("check_runtime", "false"))
        status_obj = _openai_auth_service().status(uid, provider_id="openai-codex")
        token_ready = bool(status_obj.connected)
        token_reason = ""
        if bool(status_obj.connected) and not _openai_codex_oauth_mode_enabled():
            token_reason = _provider_mode_disabled_reason("openai-codex") or "OpenAI Codex OAuth mode is disabled in Settings"
            token_ready = False
        elif bool(status_obj.connected) and _openai_codex_oauth_mode_enabled() and bool(runtime_check):
            token_ready, token_reason = _openai_codex_token_health(uid, force_refresh=False)
        payload = {
            # "connected" means runtime-usable (not just profile row present).
            "connected": bool(status_obj.connected and token_ready),
            "profile_connected": bool(status_obj.connected),
            "provider_id": str(status_obj.provider_id or "openai-codex"),
            "auth_mode": str(status_obj.auth_mode or "none"),
            "account_id": str(status_obj.account_id or ""),
            "expires_at": int(status_obj.expires_at) if status_obj.expires_at else None,
            "expires_in_sec": int(status_obj.expires_in_sec) if status_obj.expires_in_sec is not None else None,
            "has_refresh_token": bool(status_obj.has_refresh_token),
            "metadata": status_obj.metadata or {},
            "ready": bool(token_ready),
            "runtime_checked": bool(runtime_check),
            "error": str(token_reason or "") if (bool(status_obj.connected) and not bool(token_ready)) else "",
        }
        return jsonify(payload)
    except Exception as exc:
        return jsonify({"connected": False, "provider_id": "openai-codex", "auth_mode": "none", "error": str(exc)}), 500


def api_openai_codex_oauth_disconnect():
    try:
        _openai_auth_service().disconnect(_current_user_id_or_zero(), provider_id="openai-codex")
        _openai_codex_health_cache_invalidate(_current_user_id_or_zero())
        return jsonify({"ok": True, "message": "OpenAI Codex OAuth disconnected"})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc) or "Disconnect failed"}), 500


# Compatibility routes (legacy one-release bridge)
def api_openai_oauth_device_start():
    return _openai_codex_oauth_start_impl()


def api_openai_oauth_device_poll():
    return _openai_codex_oauth_poll_impl()


def api_openai_oauth_status_legacy():
    return api_openai_codex_oauth_status()


def api_openai_oauth_disconnect_legacy():
    return api_openai_codex_oauth_disconnect()


def api_musicbrainz_test(*args, **kwargs):
    return _managed_runtime.api_musicbrainz_test_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_ollama_models(*args, **kwargs):
    return _managed_runtime.api_ollama_models_for_runtime(sys.modules[__name__], *args, **kwargs)

def _normalize_ollama_probe_url(*args, **kwargs):
    return _managed_runtime._normalize_ollama_probe_url_for_runtime(sys.modules[__name__], *args, **kwargs)

def _ollama_probe(*args, **kwargs):
    return _managed_runtime._ollama_probe_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_ollama_discover(*args, **kwargs):
    return _managed_runtime.api_ollama_discover_for_runtime(sys.modules[__name__], *args, **kwargs)

def _ollama_model_exists(*args, **kwargs):
    return _managed_runtime._ollama_model_exists_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_ollama_pull_status(*args, **kwargs):
    return _managed_runtime.api_ollama_pull_status_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_ollama_pull(*args, **kwargs):
    return _managed_runtime.api_ollama_pull_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_runtime_managed_status(*args, **kwargs):
    return _managed_runtime.api_runtime_managed_status_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_runtime_managed_logs(*args, **kwargs):
    return _managed_runtime.api_runtime_managed_logs_for_runtime(sys.modules[__name__], *args, **kwargs)

def _api_runtime_managed_common_roots(*args, **kwargs):
    return _managed_runtime._api_runtime_managed_common_roots_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_runtime_managed_bootstrap(*args, **kwargs):
    return _managed_runtime.api_runtime_managed_bootstrap_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_runtime_managed_adopt(*args, **kwargs):
    return _managed_runtime.api_runtime_managed_adopt_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_runtime_managed_action(*args, **kwargs):
    return _managed_runtime.api_runtime_managed_action_for_runtime(sys.modules[__name__], *args, **kwargs)





def api_openai_models(*args: Any, **kwargs: Any) -> Any:
    return _ai_provider_config_runtime.api_openai_models_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_anthropic_models(*args: Any, **kwargs: Any) -> Any:
    return _ai_provider_config_runtime.api_anthropic_models_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_google_models(*args: Any, **kwargs: Any) -> Any:
    return _ai_provider_config_runtime.api_google_models_for_runtime(sys.modules[__name__], *args, **kwargs)


def _local_network_ipv4_candidates(*args: Any, **kwargs: Any) -> Any:
    return _ai_provider_config_runtime._local_network_ipv4_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)






_OLLAMA_PULL_STATUS_LOCK = threading.Lock()
_OLLAMA_PULL_STATUS: dict[str, Any] = {
    "active": False,
    "status": "idle",
    "message": "",
    "model": "",
    "url": "",
    "completed": 0,
    "total": 0,
    "progress": 0.0,
    "started_at": None,
    "updated_at": None,
    "finished_at": None,
    "error": "",
    "digest": "",
}


def _ollama_pull_status_snapshot(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._ollama_pull_status_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)


def _ollama_pull_status_update(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._ollama_pull_status_update_for_runtime(sys.modules[__name__], *args, **kwargs)




def _run_ollama_pull_async(*args: Any, **kwargs: Any) -> Any:
    return _managed_runtime._run_ollama_pull_async_for_runtime(sys.modules[__name__], *args, **kwargs)


















def api_ai_models(*args: Any, **kwargs: Any) -> Any:
    return _ai_provider_config_runtime.api_ai_models_for_runtime(sys.modules[__name__], *args, **kwargs)


def _has_settings_in_db() -> bool:
    """Check if settings exist in the configuration database (wizard was completed)."""
    try:
        if not SETTINGS_DB_FILE.exists():
            return False
        con = sqlite3.connect(str(SETTINGS_DB_FILE))
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='settings'")
        row = cur.fetchone()
        if not row:
            con.close()
            return False
        cur.execute("SELECT COUNT(*) FROM settings")
        count = cur.fetchone()[0]
        con.close()
        return count > 0
    except Exception:
        return False


def _get_config_from_db(key: str, default_value=None):
    """Get a config value from SQLite settings table, with fallback to default.

    Configuration now lives in SETTINGS_DB_FILE; this helper no longer reads from state.db.
    """
    db_path = SETTINGS_DB_FILE
    try:
        if db_path.exists():
            con = sqlite3.connect(str(db_path), timeout=5)
            cur = con.cursor()
            cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cur.fetchone()
            con.close()
            if row and row[0] is not None:
                return row[0]
    except Exception:
        pass
    return default_value


def _settings_db_read_all() -> dict[str, str]:
    """Fetch the whole settings table once (fast path for /api/config)."""
    out: dict[str, str] = {}
    db_path = SETTINGS_DB_FILE
    try:
        if not db_path.exists():
            return out
        con = sqlite3.connect(str(db_path), timeout=2)
        cur = con.cursor()
        cur.execute("SELECT key, value FROM settings")
        for row in cur.fetchall():
            if not row:
                continue
            k = str(row[0] or "").strip()
            if not k:
                continue
            out[k] = "" if row[1] is None else str(row[1])
        con.close()
    except Exception:
        return out
    return out


MASKED_SECRET_KEYS: set[str] = {
    "OPENAI_API_KEY", "DISCORD_WEBHOOK",
    "OPENAI_OAUTH_REFRESH_TOKEN",
    "ANTHROPIC_API_KEY", "GOOGLE_API_KEY",
    "DISCOGS_USER_TOKEN", "LASTFM_API_KEY", "LASTFM_API_SECRET",
    "LASTFM_SESSION_KEY_ENC", "LASTFM_AUTH_TOKEN_ENC",
    "FANART_API_KEY", "THEAUDIODB_API_KEY",
    "SERPER_API_KEY", "ACOUSTID_API_KEY",
    "LIDARR_API_KEY", "AUTOBRR_API_KEY",
    "JELLYFIN_API_KEY", "NAVIDROME_PASSWORD", "NAVIDROME_API_KEY",
    "MUSICBRAINZ_REPLICATION_TOKEN",
}


def _is_masked_secret_placeholder(value) -> bool:
    """
    Detect UI mask placeholders so we never overwrite real secrets with masked text.
    Accepts legacy '***' and generic star/bullet masks.
    """
    raw = str(value or "").strip()
    if not raw:
        return False
    if raw in {"***", "(hidden)", "[hidden]", "<hidden>"}:
        return True
    if len(raw) >= 3 and all(ch in {"*", "•"} for ch in raw):
        return True
    return False


def _reload_auto_move_from_db(*args: Any, **kwargs: Any) -> Any:
    return _settings_runtime._reload_auto_move_from_db_for_runtime(sys.modules[__name__], *args, **kwargs)


def _reload_musicbrainz_settings_from_db(*args: Any, **kwargs: Any) -> Any:
    return _settings_runtime._reload_musicbrainz_settings_from_db_for_runtime(sys.modules[__name__], *args, **kwargs)


def _reload_section_ids_from_db(*args: Any, **kwargs: Any) -> Any:
    return _settings_runtime._reload_section_ids_from_db_for_runtime(sys.modules[__name__], *args, **kwargs)


def _reload_path_map_from_db(*args: Any, **kwargs: Any) -> Any:
    return _settings_runtime._reload_path_map_from_db_for_runtime(sys.modules[__name__], *args, **kwargs)


def _reload_library_mode_and_files_roots_from_db(*args: Any, **kwargs: Any) -> Any:
    return _settings_runtime._reload_library_mode_and_files_roots_from_db_for_runtime(sys.modules[__name__], *args, **kwargs)


def _parse_format_preference(val):
    """Return FORMAT_PREFERENCE as a list. Handles JSON string, comma-separated string, or list from DB/API."""
    return _config_core.parse_format_preference(val)


def _library_include_unmatched_effective() -> bool:
    """
    Resolve whether Files library endpoints should include albums not formally matched by PMDA.

    Priority:
    1) Request query param `include_unmatched` (1/0, true/false)
    2) Persisted global setting `LIBRARY_INCLUDE_UNMATCHED`
    """
    raw = request.args.get("include_unmatched")
    if raw is None or str(raw).strip() == "":
        return bool(getattr(sys.modules[__name__], "LIBRARY_INCLUDE_UNMATCHED", True))
    return bool(_parse_bool(raw))


def _normalize_library_workflow_mode(value: Any, default: str = "managed") -> str:
    return _config_core.normalize_library_workflow_mode(value, default=default)


def _files_tag_write_mode(*args, **kwargs):
    return _library_workflow_runtime._files_tag_write_mode_for_runtime(sys.modules[__name__], *args, **kwargs)




def library_is_audit_mode(*args, **kwargs):
    return _library_workflow_runtime.library_is_audit_mode_for_runtime(sys.modules[__name__], *args, **kwargs)




def _normalize_root_path_list(*args, **kwargs):
    return _library_workflow_runtime._normalize_root_path_list_for_runtime(sys.modules[__name__], *args, **kwargs)




def _workflow_serialized_path_list(*args, **kwargs):
    return _library_workflow_runtime._workflow_serialized_path_list_for_runtime(sys.modules[__name__], *args, **kwargs)




def _library_workflow_scope_roots(*args, **kwargs):
    return _library_workflow_runtime._library_workflow_scope_roots_for_runtime(sys.modules[__name__], *args, **kwargs)




def _library_workflow_state(*args, **kwargs):
    return _library_workflow_runtime._library_workflow_state_for_runtime(sys.modules[__name__], *args, **kwargs)




def _library_workflow_prepare_updates(*args, **kwargs):
    return _library_workflow_runtime._library_workflow_prepare_updates_for_runtime(sys.modules[__name__], *args, **kwargs)




def _normalize_library_scope(value: Any, default: str = "library") -> str:
    return _config_core.normalize_library_scope(value, default=default)


def _library_scope_effective() -> str:
    """
    Resolve which logical collection Files browse endpoints should expose.

    - library: albums exported to Music_matched
    - inbox: source roots that are still not strict/library-ready
    - dupes: duplicate + incomplete quarantine
    - all: no root-based restriction
    """
    return _normalize_library_scope(request.args.get("scope"), default="library")


def _normalize_files_root_path(value: str) -> str:
    return _config_core.normalize_files_root_path(value)


def _sql_quote_literal(value: str) -> str:
    raw = str(value or "")
    # psycopg parses `%` in query text for placeholders even inside SQL string
    # literals. Escaping them here keeps generated SQL fragments safe when they
    # are concatenated into parameterized statements.
    return "'" + raw.replace("%", "%%").replace("'", "''") + "'"


def _sql_path_prefix_match(path_expr: str, roots: list[str]) -> str:
    normalized: list[str] = []
    seen: set[str] = set()
    for root in roots or []:
        nr = _normalize_files_root_path(root)
        if not nr or nr in seen:
            continue
        seen.add(nr)
        normalized.append(nr)
    if not normalized:
        return "FALSE"
    clauses: list[str] = []
    for nr in normalized:
        eq = _sql_quote_literal(nr)
        like = _sql_quote_literal(f"{nr}/%")
        clauses.append(f"({path_expr} = {eq} OR {path_expr} LIKE {like})")
    return "(" + " OR ".join(clauses) + ")"


def _files_library_virtual_visibility_clause(alias: str = "alb") -> str:
    return f"COALESCE({alias}.is_broken, FALSE) = FALSE"


def _files_library_use_virtual_scope() -> bool:
    if _get_library_mode() != "files":
        return False
    workflow = _library_workflow_state()
    mode = _normalize_library_workflow_mode(workflow.get("mode"), default="managed")
    return mode in {"managed", "mirror"}


def _library_album_scope_where(scope: str, alias: str = "alb") -> str:
    scope_norm = _normalize_library_scope(scope, default="library")
    path_expr = f"REPLACE(COALESCE({alias}.folder_path, ''), '\\\\', '/')"
    scope_roots = _library_workflow_scope_roots()
    library_roots = [_normalize_files_root_path(root) for root in (scope_roots.get("library_roots") or []) if root]
    inbox_roots = [_normalize_files_root_path(root) for root in (scope_roots.get("inbox_roots") or []) if root]
    dupe_roots = [_normalize_files_root_path(root) for root in (scope_roots.get("dupe_roots") or []) if root]

    if scope_norm == "all":
        return "1=1"
    if scope_norm == "library":
        if _files_library_use_virtual_scope():
            return _files_library_virtual_visibility_clause(alias)
        return _sql_path_prefix_match(path_expr, library_roots)
    if scope_norm == "dupes":
        return _sql_path_prefix_match(path_expr, dupe_roots)
    if scope_norm == "inbox":
        parts: list[str] = []
        if inbox_roots:
            parts.append(_sql_path_prefix_match(path_expr, inbox_roots))
        if _files_library_use_virtual_scope():
            parts.append(f"NOT ({_files_library_virtual_visibility_clause(alias)})")
        elif library_roots:
            parts.append(f"NOT {_sql_path_prefix_match(path_expr, library_roots)}")
        if dupe_roots:
            parts.append(f"NOT {_sql_path_prefix_match(path_expr, dupe_roots)}")
        parts.append(f"COALESCE({alias}.strict_match_verified, FALSE) = FALSE")
        return "(" + " AND ".join(parts) + ")" if parts else "1=1"
    return "1=1"


def _library_cache_scope_suffix(scope: str) -> str:
    return _normalize_library_scope(scope, default="library")


def _library_albums_match_where(include_unmatched: bool, alias: str = "alb") -> str:
    if include_unmatched:
        return "1=1"
    return f"(COALESCE({alias}.strict_match_verified, FALSE) = TRUE OR COALESCE({alias}.mb_identified, FALSE) = TRUE)"


def _library_cache_unmatched_suffix(include_unmatched: bool) -> str:
    return "all" if include_unmatched else "matched"


def _files_library_published_scope_context() -> _publication_snapshot.PublishedScopeContext:
    scope_roots = _library_workflow_scope_roots()
    return _publication_snapshot.PublishedScopeContext(
        library_roots=tuple(_normalize_files_root_path(root) for root in (scope_roots.get("library_roots") or []) if root),
        inbox_roots=tuple(_normalize_files_root_path(root) for root in (scope_roots.get("inbox_roots") or []) if root),
        dupe_roots=tuple(_normalize_files_root_path(root) for root in (scope_roots.get("dupe_roots") or []) if root),
        use_virtual_scope=bool(_files_library_use_virtual_scope()),
    )


def _sqlite_path_prefix_match_sql(column: str, roots: list[str], params: list[Any]) -> str:
    return _publication_snapshot.sqlite_path_prefix_match_sql(
        column,
        [_normalize_files_root_path(root) for root in roots if root],
        params,
    )


def _files_library_published_scope_where_sqlite(scope: str, params: list[Any]) -> str:
    return _publication_snapshot.published_scope_where_sqlite(
        _normalize_library_scope(scope, default="library"),
        params,
        context=_files_library_published_scope_context(),
    )


def _files_library_published_album_where_sqlite(
    *,
    include_unmatched: bool,
    scope: str = "library",
    search_query: str = "",
    genre: str = "",
    label: str = "",
    year: int = 0,
) -> tuple[str, list[Any]]:
    return _publication_snapshot.published_album_where_sqlite(
        include_unmatched=include_unmatched,
        context=_files_library_published_scope_context(),
        scope=_normalize_library_scope(scope, default="library"),
        search_query=search_query,
        genre=genre,
        label=label,
        year=year,
    )

def _files_library_browse_counts(
    include_unmatched: bool,
    *,
    scope: str = "library",
    acquire_timeout_sec: float = 0.20,
) -> tuple[int | None, int | None]:
    return _browse_state_runtime.files_library_browse_counts_for_runtime(
        sys.modules[__name__],
        include_unmatched,
        scope=scope,
        acquire_timeout_sec=acquire_timeout_sec,
    )


def _files_library_published_browse_counts(*args: Any, **kwargs: Any) -> Any:
    return _published_browse_runtime._files_library_published_browse_counts_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_library_browse_snapshot(
    include_unmatched: bool,
    *,
    scope: str = "library",
) -> dict[str, Any]:
    return _browse_state_runtime.files_library_browse_snapshot_for_runtime(
        sys.modules[__name__],
        include_unmatched,
        scope=scope,
    )


def _files_library_effective_browse_snapshot(
    include_unmatched: bool,
    *,
    scope: str = "library",
) -> dict[str, Any]:
    return _browse_state_runtime.files_library_effective_browse_snapshot_for_runtime(
        sys.modules[__name__],
        include_unmatched,
        scope=scope,
    )


def _files_library_api_browse_snapshot(
    include_unmatched: bool,
    *,
    scope: str = "library",
) -> dict[str, Any]:
    return _browse_state_runtime.files_library_api_browse_snapshot_for_runtime(
        sys.modules[__name__],
        include_unmatched,
        scope=scope,
    )


def _files_library_should_fallback_to_published(
    snapshot: dict[str, Any] | None,
    *,
    albums: int | None = None,
    artists: int | None = None,
) -> bool:
    return _publication_snapshot.should_fallback_to_published(
        snapshot,
        albums=albums,
        artists=artists,
    )


def _files_scan_busy() -> bool:
    return _browse_state_runtime.files_scan_busy_for_runtime(sys.modules[__name__])


def _files_library_browse_source_requested(*args: Any, **kwargs: Any) -> Any:
    return _published_browse_runtime._files_library_browse_source_requested_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_library_browse_source_effective(
    *,
    scope: str,
    requested: str | None = None,
    snapshot: dict[str, Any] | None = None,
    scan_busy: bool | None = None,
) -> str:
    return _browse_state_runtime.files_library_browse_source_effective_for_runtime(
        sys.modules[__name__],
        scope=scope,
        requested=requested,
        snapshot=snapshot,
        scan_busy=scan_busy,
    )


def _files_library_live_status_context(*, source_is_published: bool = False) -> dict[str, Any]:
    scan_busy = _files_scan_busy()
    with lock:
        scan_profile_enrich_running = bool(state.get("scan_profile_enrich_running"))
        scan_post_processing = bool(state.get("scan_post_processing"))
    with _files_profile_backfill_lock:
        profile_backfill_running = bool(_files_profile_backfill_state.get("running"))
    with _files_profile_jobs_lock:
        profile_jobs_active = bool(_files_profile_jobs_active)
    return _enrichment_status.live_status_context(
        source_is_published=bool(source_is_published),
        scan_busy=bool(scan_busy),
        scan_profile_enrich_running=bool(scan_profile_enrich_running),
        scan_post_processing=bool(scan_post_processing),
        profile_backfill_running=bool(profile_backfill_running),
        profile_jobs_active=bool(profile_jobs_active),
    )


def _files_library_enrichment_state(*, has_value: bool, active: bool, eligible: bool = True) -> str:
    return _enrichment_status.enrichment_state(has_value=has_value, active=active, eligible=eligible)


def _files_library_artist_status_fields(
    *,
    status_context: dict[str, Any] | None,
    has_image: bool,
    has_profile: bool,
    has_fallback_thumb: bool = False,
) -> dict[str, str]:
    ctx = status_context if isinstance(status_context, dict) else _files_library_live_status_context()
    return _enrichment_status.artist_status_fields(
        status_context=ctx,
        has_image=has_image,
        has_profile=has_profile,
        has_fallback_thumb=has_fallback_thumb,
    )


def _files_library_album_status_fields(
    *,
    status_context: dict[str, Any] | None,
    has_cover: bool,
    has_artist_image: bool,
    has_profile: bool,
    cover_eligible: bool = True,
    artist_media_eligible: bool = True,
    profile_eligible: bool = True,
) -> dict[str, str]:
    ctx = status_context if isinstance(status_context, dict) else _files_library_live_status_context()
    return _enrichment_status.album_status_fields(
        status_context=ctx,
        has_cover=has_cover,
        has_artist_image=has_artist_image,
        has_profile=has_profile,
        cover_eligible=cover_eligible,
        artist_media_eligible=artist_media_eligible,
        profile_eligible=profile_eligible,
    )


def _files_library_resolve_artist_ids_by_norms(artist_norms: list[str]) -> dict[str, int]:
    norms = [str(value or "").strip() for value in (artist_norms or []) if str(value or "").strip()]
    if not norms:
        return {}
    if not _files_pg_init_schema():
        return {}
    conn = _files_pg_connect(acquire_timeout_sec=0.35)
    if conn is None:
        return {}
    try:
        with conn.cursor() as cur:
            with _files_pg_statement_timeout(cur, 1200):
                cur.execute(
                    """
                    SELECT COALESCE(name_norm, ''), id
                    FROM files_artists
                    WHERE name_norm = ANY(%s)
                    """,
                    (norms,),
                )
                return {
                    str(name_norm or "").strip(): int(artist_id or 0)
                    for name_norm, artist_id in (cur.fetchall() or [])
                    if str(name_norm or "").strip() and int(artist_id or 0) > 0
                }
    except Exception:
        logging.debug("Failed to resolve published artist ids", exc_info=True)
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _files_library_resolve_album_ids_by_folder_paths(folder_paths: list[str]) -> dict[str, int]:
    paths = [str(value or "").strip() for value in (folder_paths or []) if str(value or "").strip()]
    if not paths:
        return {}
    if not _files_pg_init_schema():
        return {}
    conn = _files_pg_connect(acquire_timeout_sec=0.35)
    if conn is None:
        return {}
    try:
        with conn.cursor() as cur:
            with _files_pg_statement_timeout(cur, 1200):
                cur.execute(
                    """
                    SELECT COALESCE(folder_path, ''), id
                    FROM files_albums
                    WHERE folder_path = ANY(%s)
                    """,
                    (paths,),
                )
                return {
                    str(folder_path or "").strip(): int(album_id or 0)
                    for folder_path, album_id in (cur.fetchall() or [])
                    if str(folder_path or "").strip() and int(album_id or 0) > 0
                }
    except Exception:
        logging.debug("Failed to resolve published album ids", exc_info=True)
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _files_library_resolve_album_cover_flags_by_ids(album_ids: list[int]) -> dict[int, bool]:
    ids = sorted({int(value or 0) for value in (album_ids or []) if int(value or 0) > 0})
    if not ids:
        return {}
    if not _files_pg_init_schema():
        return {}
    conn = _files_pg_connect(acquire_timeout_sec=0.35)
    if conn is None:
        return {}
    try:
        with conn.cursor() as cur:
            with _files_pg_statement_timeout(cur, 1200):
                cur.execute(
                    """
                    SELECT id, COALESCE(has_cover, FALSE)
                    FROM files_albums
                    WHERE id = ANY(%s)
                    """,
                    (ids,),
                )
                return {int(album_id or 0): bool(has_cover) for album_id, has_cover in (cur.fetchall() or []) if int(album_id or 0) > 0}
    except Exception:
        logging.debug("Failed to resolve live album cover flags", exc_info=True)
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _files_library_published_artists(*args: Any, **kwargs: Any) -> Any:
    return _published_browse_runtime._files_library_published_artists_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_library_published_albums(*args: Any, **kwargs: Any) -> Any:
    return _published_browse_runtime._files_library_published_albums_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_library_discover_scan_safe_payload(
    *,
    include_unmatched: bool,
    scope: str = "library",
    limit: int = 18,
    days: int = 90,
) -> dict[str, Any]:
    albums_payload = _files_library_published_albums(
        include_unmatched=include_unmatched,
        scope=scope,
        sort="recent",
        limit=limit,
        offset=0,
        allow_live_resolution=False,
    )
    albums = list(albums_payload.get("albums") or [])
    sections: list[dict[str, Any]] = []
    if albums:
        sections.append(
            {
                "key": "scan_safe_recent",
                "title": "Fresh in library",
                "reason": "Using the published library snapshot while the live index is busy with scan work.",
                "albums": albums,
            }
        )
    return {
        "days": int(days),
        "limit": int(limit),
        "generated_at": int(time.time()),
        "sections": sections,
        "stale": True,
        "fallback_source": str(albums_payload.get("fallback_source") or "published"),
    }


def _files_library_published_genres(*args: Any, **kwargs: Any) -> Any:
    return _published_browse_runtime._files_library_published_genres_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_library_published_labels(*args: Any, **kwargs: Any) -> Any:
    return _published_browse_runtime._files_library_published_labels_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_index_maybe_enqueue_published_catchup(*args: Any, **kwargs: Any) -> Any:
    return _published_browse_runtime._files_index_maybe_enqueue_published_catchup_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_config_get():
    return _settings_runtime.api_config_get_for_runtime(sys.modules[__name__])


def api_ai_provider_preferences_get():
    user_id = _current_user_id_or_zero()
    prefs = _get_ai_provider_preferences(user_id)
    return jsonify(
        {
            "interactive_provider_id": prefs.get("interactive_provider_id", "openai-codex"),
            "batch_provider_id": prefs.get("batch_provider_id", "openai-codex"),
            "web_search_provider_id": prefs.get("web_search_provider_id", "openai-codex"),
            "effective": {
                "interactive_provider_id": _resolve_provider_for_runtime("openai", "assistant_chat"),
                "batch_provider_id": _resolve_provider_for_runtime("openai", "scan_pipeline"),
                "web_search_provider_id": _resolve_provider_for_runtime("openai", "web_search"),
            },
            # Presence-level signal only (fast, no token refresh).
            "codex_connected": bool(_openai_codex_connected(user_id, require_token=False)),
        }
    )


def api_ai_provider_preferences_put():
    data = request.get_json(silent=True) or {}
    prefs = _save_ai_provider_preferences(
        user_id=_current_user_id_or_zero(),
        interactive_provider_id=str(data.get("interactive_provider_id") or "openai-codex"),
        batch_provider_id=str(data.get("batch_provider_id") or "openai-codex"),
        web_search_provider_id=str(data.get("web_search_provider_id") or "openai-codex"),
    )
    return jsonify(
        {
            "status": "ok",
            "interactive_provider_id": prefs.get("interactive_provider_id", "openai-codex"),
            "batch_provider_id": prefs.get("batch_provider_id", "openai-codex"),
            "web_search_provider_id": prefs.get("web_search_provider_id", "openai-codex"),
        }
    )


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
    return _settings_runtime.apply_settings_in_memory_for_runtime(sys.modules[__name__], updates)


def api_config_put():
    return _settings_runtime.api_config_put_for_runtime(sys.modules[__name__])


def get_duplicate_groups_from_library():
    """Compatibility stub: duplicate review now comes from Files scan/state registries only."""
    return []



def _group_contains_album_id(group: dict, album_id: int) -> bool:
    """Return True when album_id matches this duplicate group best or one of its losers."""
    try:
        target = int(album_id)
    except Exception:
        return False
    if int(group.get("album_id") or 0) == target:
        return True
    best = group.get("best") or {}
    if int(best.get("album_id") or 0) == target:
        return True
    for loser in group.get("losers", []) or []:
        if int((loser or {}).get("album_id") or 0) == target:
            return True
    return False


def _duplicate_group_identity(group: dict) -> tuple[str, str]:
    """Stable-enough identity for merging persisted and live duplicate groups."""
    return _dedupe_review_core.duplicate_group_identity(group)


def _merge_duplicate_results(*sources: dict[str, list[dict]] | None) -> dict[str, list[dict]]:
    """Merge duplicate registries without dropping historical open groups."""
    return _dedupe_review_core.merge_duplicate_results(*sources)


def _duplicate_registry_counts(groups: dict[str, list[dict]] | None) -> dict[str, int]:
    return _dedupe_review_core.duplicate_registry_counts(groups)


def _global_duplicate_review_registry(*, include_live: bool = True, trace_limit: int = 5000) -> dict[str, Any]:
    """
    Return the global unresolved duplicate registry.

    This is intentionally scan-independent: persisted open groups, recovered
    pipeline trace groups and live in-memory scan groups are merged so every
    caller sees the same review truth.
    """
    errors: list[str] = []
    try:
        persisted_groups = load_scan_from_db()
    except Exception as exc:
        logging.debug("Failed to load persisted duplicate registry", exc_info=True)
        persisted_groups = {}
        errors.append(f"persisted:{type(exc).__name__}")
    try:
        trace_groups = _load_duplicate_groups_from_pipeline_trace(limit_groups=int(trace_limit or 5000))
    except Exception as exc:
        logging.debug("Failed to load trace duplicate registry", exc_info=True)
        trace_groups = {}
        errors.append(f"trace:{type(exc).__name__}")
    live_groups: dict[str, list[dict]] = {}
    if include_live:
        try:
            with lock:
                live_groups = dict(state.get("duplicates") or {})
        except Exception as exc:
            live_groups = {}
            errors.append(f"live:{type(exc).__name__}")
    merged = _merge_duplicate_results(persisted_groups, trace_groups, live_groups)
    return {
        "groups": merged,
        "sources": {
            "persisted": _duplicate_registry_counts(persisted_groups),
            "trace": _duplicate_registry_counts(trace_groups),
            "live": _duplicate_registry_counts(live_groups),
            "merged": _duplicate_registry_counts(merged),
        },
        "errors": errors,
    }


def _build_library_duplicate_group_for_artist_album(artist_name: str, album_id: int) -> dict | None:
    """Compatibility stub for the removed Plex-source duplicate group builder."""
    return None



def _find_duplicate_group_by_artist_album(
    artist_name: str,
    album_id: int,
    *,
    allow_library_build: bool = False,
) -> dict | None:
    """Resolve one duplicate group from memory, DB snapshot, then optional library build fallback."""
    try:
        target_id = int(album_id)
    except Exception:
        return None
    artist_norm = artist_name.replace("_", " ").strip()
    if not artist_norm:
        return None

    with lock:
        groups = list(state.get("duplicates", {}).get(artist_norm, []) or [])
    for g in groups:
        if _group_contains_album_id(g, target_id):
            return g

    loaded_groups = load_scan_from_db().get(artist_norm, []) or []
    for g in loaded_groups:
        if _group_contains_album_id(g, target_id):
            with lock:
                artist_groups = state.setdefault("duplicates", {}).setdefault(artist_norm, [])
                if not any(_group_contains_album_id(existing, target_id) for existing in artist_groups):
                    artist_groups.append(g)
            return g

    if not allow_library_build:
        return None
    built = _build_library_duplicate_group_for_artist_album(artist_norm, target_id)
    if built:
        with lock:
            artist_groups = state.setdefault("duplicates", {}).setdefault(artist_norm, [])
            artist_groups.append(built)
    return built


def api_progress():
    return _progress_payload.api_progress_for_runtime(sys.modules[__name__])


def _classify_log_tail_message(level: str, message: str) -> tuple[str, str]:
    msg = str(message or "").strip()
    cleaned = re.sub(r"\s+", " ", msg)
    lvl = str(level or "").upper()
    tag, body = _parse_log_tag_body(
        logging.ERROR if lvl == "ERROR" else logging.WARNING if lvl == "WARNING" else logging.INFO,
        cleaned,
    )
    state = _log_state_from_domain_body(
        logging.ERROR if lvl == "ERROR" else logging.WARNING if lvl == "WARNING" else logging.INFO,
        tag.lower(),
        body,
    )
    marker, _, _ = _log_marker_visual(state)
    kind_map = {
        "success": "match",
        "failure": "miss",
        "partial": "soft",
        "warning": "warning",
        "skip": "provider",
        "progress": "scan",
        "info": "info",
    }
    return kind_map.get(state, "info"), marker


_LOG_TAIL_PARSER = LogTailParser(classify_message=_classify_log_tail_message)
_tail_log_entries_from_lines = _LOG_TAIL_PARSER.entries_from_lines
_log_tail_entry_is_scan_relevant = _LOG_TAIL_PARSER.entry_is_scan_relevant


def _tail_log_entries(path: Path, lines: int = 200, max_bytes: int = 512 * 1024) -> list[dict[str, Any]]:
    return _LOG_TAIL_PARSER.entries(path, lines=lines, max_bytes=max_bytes)


def _recent_log_tail_entries(lines: int = 200, *, scan_mode: bool = False) -> list[dict[str, Any]]:
    with _RECENT_LOG_BUFFER_LOCK:
        snapshot = list(_RECENT_LOG_BUFFER)
    if not snapshot:
        return []
    window = max(lines * 12, 480)
    entries = _tail_log_entries_from_lines(snapshot[-window:])
    if scan_mode:
        relevant = [entry for entry in entries if _log_tail_entry_is_scan_relevant(entry)]
        if relevant:
            entries = relevant
    if len(entries) > lines:
        entries = entries[-lines:]
    return entries


def _set_disabled_lidarr_incomplete_progress(rows: list[tuple]):
    with lock:
        state["lidarr_add_incomplete"] = {
            "running": False,
            "current": 0,
            "total": len(rows),
            "current_album": None,
            "current_artist": None,
            "added": 0,
            "failed": len(rows),
            "result": {"error": "Lidarr integration is currently disabled"},
        }


register_api_blueprints(
    app,
    runtime=sys.modules[__name__],
    log_routes={
        "get_log_file": lambda: str(LOG_FILE or ""),
        "parse_bool": _parse_bool,
        "recent_log_tail_entries": _recent_log_tail_entries,
        "tail_log_entries": _tail_log_entries,
        "tail_log_lines": _tail_log_lines,
    },
    set_lidarr_progress=_set_disabled_lidarr_incomplete_progress,
    include_frontend=_HAS_STATIC_UI,
    frontend_dist=_FRONTEND_DIST,
)

def _scan_history_metadata_rollup(*args: Any, **kwargs: Any) -> Any:
    return _scan_history_runtime._scan_history_metadata_rollup_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_history_summary_with_metadata_rollup(*args: Any, **kwargs: Any) -> Any:
    return _scan_history_runtime._scan_history_summary_with_metadata_rollup_for_runtime(sys.modules[__name__], *args, **kwargs)


def add_broken_album_to_lidarr(artist_name: str, album_id: int, musicbrainz_release_group_id: str, album_title: str) -> bool:
    """Compatibility stub for the removed Lidarr acquisition workflow."""
    from pmda_core.legacy_integrations import ignore_album_acquisition

    return ignore_album_acquisition(artist_name, album_id, musicbrainz_release_group_id, album_title)
def _broken_album_delete_rows(pairs: list[tuple[str, int]] | set[tuple[str, int]]) -> int:
    return _broken_album_runtime._broken_album_delete_rows_for_runtime(sys.modules[__name__], pairs)


def _broken_album_resolve_folder_snapshot(
    *,
    folder_path: str | None,
    artist_name: str,
    album_title: str,
    metadata_source: str,
    detail_map: dict[str, Any] | None = None,
    existing_local_tracks: list[dict[str, Any]] | None = None,
    force_rescan: bool = True,
) -> tuple[Path | None, list[dict[str, Any]], bool]:
    return _broken_album_runtime._broken_album_resolve_folder_snapshot_for_runtime(
        sys.modules[__name__],
        folder_path=folder_path,
        artist_name=artist_name,
        album_title=album_title,
        metadata_source=metadata_source,
        detail_map=detail_map,
        existing_local_tracks=existing_local_tracks,
        force_rescan=force_rescan,
    )


def api_broken_albums():
    return _broken_album_runtime.api_broken_albums_for_runtime(sys.modules[__name__])


def _build_incomplete_assessment_from_payload(*args: Any, **kwargs: Any) -> Any:
    return _incomplete_ai_runtime._build_incomplete_assessment_from_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


def _refresh_broken_album_row(*args, **kwargs):
    return _broken_album_runtime.refresh_broken_album_row_for_runtime(sys.modules[__name__], *args, **kwargs)


def _broken_album_backfill_candidates(*, limit: int = 0, full_refresh: bool = False) -> list[tuple[str, int]]:
    return _broken_album_runtime._broken_album_backfill_candidates_for_runtime(
        sys.modules[__name__],
        limit=limit,
        full_refresh=full_refresh,
    )


def _run_broken_album_backfill(*, reason: str = "manual", include_ai: bool = False, limit: int = 0, full_refresh: bool = False) -> None:
    return _broken_album_runtime._run_broken_album_backfill_for_runtime(
        sys.modules[__name__],
        reason=reason,
        include_ai=include_ai,
        limit=limit,
        full_refresh=full_refresh,
    )


def _trigger_broken_album_backfill_async(*, reason: str = "manual", include_ai: bool = False, limit: int = 0, full_refresh: bool = False) -> bool:
    return _broken_album_runtime._trigger_broken_album_backfill_async_for_runtime(
        sys.modules[__name__],
        reason=reason,
        include_ai=include_ai,
        limit=limit,
        full_refresh=full_refresh,
    )


def api_broken_album_detail():
    return _broken_album_runtime.api_broken_album_detail_for_runtime(sys.modules[__name__])


def _run_incomplete_albums_scan():
    """Compatibility stub for the removed Plex-source incomplete scan.

    Files-mode PMDA records incomplete albums during the scan pipeline and in
    global diagnostics. This legacy one-off scan used to query the Plex source
    database, which is no longer an allowed backend.
    """
    with lock:
        state["incomplete_scan"] = {
            "running": False,
            "run_id": None,
            "progress": 0,
            "total": 0,
            "current_artist": "",
            "current_album": "",
            "count": 0,
            "error": "Manual incomplete scan is disabled; PMDA records incompletes during the scan pipeline.",
        }


def api_library_discover():
    return _library_browse_runtime.api_library_discover_for_runtime(sys.modules[__name__])


def api_library_artists():
    return _library_browse_runtime.api_library_artists_for_runtime(sys.modules[__name__])


def api_library_artists_suggest():
    return _library_catalog_runtime.api_library_artists_suggest_for_runtime(sys.modules[__name__])


def api_library_search_suggest():
    return _library_catalog_runtime.api_library_search_suggest_for_runtime(sys.modules[__name__])


def api_library_albums():
    return _library_browse_runtime.api_library_albums_for_runtime(sys.modules[__name__])


def api_library_digest(*args, **kwargs):
    return _catalog_stats_runtime.api_library_digest_for_runtime(sys.modules[__name__], *args, **kwargs)


_SOCIAL_ENTITY_TYPES = {"artist", "album", "track", "label", "genre", "playlist"}


def _social_entity_type_allowed(entity_type: str) -> bool:
    return _library_personal_runtime._social_entity_type_allowed_for_runtime(sys.modules[__name__], entity_type)


def _social_entity_key_norm(entity_type: str, entity_key: str) -> str:
    return _library_personal_runtime._social_entity_key_norm_for_runtime(
        sys.modules[__name__],
        entity_type,
        entity_key,
    )


def _social_notification_insert(
    cur,
    *,
    user_id: int,
    actor_user_id: int | None,
    actor_username: str,
    kind: str,
    title: str,
    body: str,
    entity_type: str = "",
    entity_id: int = 0,
    entity_key: str = "",
    recommendation_id: int | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    return _library_personal_runtime._social_notification_insert_for_runtime(
        sys.modules[__name__],
        cur,
        user_id=user_id,
        actor_user_id=actor_user_id,
        actor_username=actor_username,
        kind=kind,
        title=title,
        body=body,
        entity_type=entity_type,
        entity_id=entity_id,
        entity_key=entity_key,
        recommendation_id=recommendation_id,
        payload=payload,
    )




def _social_recommendation_payload(row) -> dict[str, Any]:
    return _library_personal_runtime._social_recommendation_payload_for_runtime(sys.modules[__name__], row)


def api_library_top_artists(*args, **kwargs):
    return _catalog_stats_runtime.api_library_top_artists_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_recent_artists():
    return _catalog_stats_runtime.api_library_recent_artists_for_runtime(sys.modules[__name__])


def api_library_facets(*args, **kwargs):
    return _catalog_stats_runtime.api_library_facets_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_genres_suggest(*args, **kwargs):
    return _catalog_stats_runtime.api_library_genres_suggest_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_labels_suggest(*args, **kwargs):
    return _catalog_stats_runtime.api_library_labels_suggest_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_genres(*args, **kwargs):
    return _catalog_stats_runtime.api_library_genres_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_labels(*args, **kwargs):
    return _catalog_stats_runtime.api_library_labels_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_genre_labels(genre: str):
    return _library_catalog_runtime.api_library_genre_labels_for_runtime(sys.modules[__name__], genre)


def api_library_genre_profile(genre):
    return _library_detail_runtime.api_library_genre_profile_for_runtime(sys.modules[__name__], genre)


def api_library_label_profile(label):
    return _library_detail_runtime.api_library_label_profile_for_runtime(sys.modules[__name__], label)


def api_library_recently_played_albums(*args, **kwargs):
    return _catalog_stats_runtime.api_library_recently_played_albums_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_liked_summary(*args, **kwargs):
    return _catalog_stats_runtime.api_library_liked_summary_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_social_users():
    return _library_personal_runtime.api_library_social_users_for_runtime(sys.modules[__name__])


def api_library_social_context():
    return _library_personal_runtime.api_library_social_context_for_runtime(sys.modules[__name__])


def api_library_share():
    return _library_personal_runtime.api_library_share_for_runtime(sys.modules[__name__])


def api_library_recommendations(*args, **kwargs):
    return _library_personal_runtime.api_library_recommendations_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_recommendation_like(*args, **kwargs):
    return _library_personal_runtime.api_library_recommendation_like_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_notifications(*args, **kwargs):
    return _library_personal_runtime.api_library_notifications_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_notifications_mark_read(*args, **kwargs):
    return _library_personal_runtime.api_library_notifications_mark_read_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_playlists(*args, **kwargs):
    return _library_personal_runtime.api_library_playlists_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_playlists_create(*args, **kwargs):
    return _library_personal_runtime.api_library_playlists_create_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_playlist_detail(*args, **kwargs):
    return _library_personal_runtime.api_library_playlist_detail_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_playlist_delete(*args, **kwargs):
    return _library_personal_runtime.api_library_playlist_delete_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_playlist_items_add(*args, **kwargs):
    return _library_personal_runtime.api_library_playlist_items_add_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_playlist_item_delete(*args, **kwargs):
    return _library_personal_runtime.api_library_playlist_item_delete_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_playlist_reorder(*args, **kwargs):
    return _library_personal_runtime.api_library_playlist_reorder_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_reco_event(*args, **kwargs):
    return _library_personal_runtime.api_library_reco_event_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_playback_event(*args, **kwargs):
    return _library_personal_runtime.api_library_playback_event_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_playback_stats(*args, **kwargs):
    return _catalog_stats_runtime.api_library_playback_stats_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_reco_for_you(*args, **kwargs):
    return _library_personal_runtime.api_library_reco_for_you_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_artist_detail(artist_id):
    return _library_detail_runtime.api_library_artist_detail_for_runtime(sys.modules[__name__], artist_id)


def api_library_artist_profile(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime.api_library_artist_profile_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_artist_ai_enrich(artist_id: int):
    return _artist_profile_runtime.api_library_artist_ai_enrich_for_runtime(sys.modules[__name__], artist_id)


def _assistant_preferred_lang() -> str:
    try:
        raw = (request.headers.get("Accept-Language") or "").lower()
    except Exception:
        raw = ""
    # Very small heuristic (good enough for UI defaults).
    if "fr" in raw:
        return "fr"
    if "en" in raw:
        return "en"
    return "en"


def api_library_artist_summary(artist_id: int):
    return _library_detail_runtime.api_library_artist_summary_for_runtime(sys.modules[__name__], artist_id)


def api_library_artist_summary_ai(artist_id):
    return _library_detail_runtime.api_library_artist_summary_ai_for_runtime(sys.modules[__name__], artist_id)


_BANDSINTOWN_BLOCK_UNTIL_TS = 0.0
_BANDSINTOWN_BLOCK_REASON = ""
_BANDSINTOWN_BLOCK_LOGGED_UNTIL_TS = 0.0
_BANDSINTOWN_BLOCK_LOCK = threading.Lock()
_BANDSINTOWN_FORBIDDEN_TTL_SEC = 60 * 60 * 12
_BANDSINTOWN_RATE_LIMIT_TTL_SEC = 60 * 30










# --- Geo helpers (concert map) ------------------------------------------------

_GEO_OSM_CACHE: dict[str, tuple[float, str, str]] = {}
_GEO_OSM_CACHE_TTL_SEC = 60 * 60 * 24 * 30








def api_library_artist_concerts(artist_id: int):
    return _library_detail_runtime.api_library_artist_concerts_for_runtime(sys.modules[__name__], artist_id)


def api_library_artist_facts(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime.api_library_artist_facts_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_artist_facts_extract(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime.api_library_artist_facts_extract_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_missing_tags():
    return _catalog_stats_runtime.api_library_missing_tags_for_runtime(sys.modules[__name__])


def api_library_album_tracks(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime.api_library_album_tracks_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_fix_missing_album_track_durations(conn, *, album_id: int, rows: list[tuple[int, int, str]]) -> dict[int, int]:
    return _album_media_runtime._files_fix_missing_album_track_durations_for_runtime(
        sys.modules[__name__],
        conn,
        album_id=album_id,
        rows=rows,
    )


_ALBUM_DETAIL_ENRICH_INFLIGHT: set[int] = set()
_ALBUM_DETAIL_ENRICH_LAST_TS: dict[int, float] = {}
_ALBUM_DETAIL_ENRICH_LOCK = threading.Lock()
_ALBUM_DETAIL_ENRICH_COOLDOWN_SEC = 60 * 15


def _run_album_detail_enrichment(album_id: int, *, rows: list[tuple[int, int, str]], has_cover: bool, cover_path_raw: str) -> None:
    return _album_media_runtime._run_album_detail_enrichment_for_runtime(
        sys.modules[__name__],
        album_id,
        rows=rows,
        has_cover=has_cover,
        cover_path_raw=cover_path_raw,
    )


def _schedule_album_detail_enrichment(album_id: int, *, rows: list[tuple[int, int, str]], has_cover: bool, cover_path_raw: str) -> None:
    return _album_media_runtime._schedule_album_detail_enrichment_for_runtime(
        sys.modules[__name__],
        album_id,
        rows=rows,
        has_cover=has_cover,
        cover_path_raw=cover_path_raw,
    )


_USER_ALBUM_FEEDBACK_MISSING = object()
_USER_ALBUM_REVIEW_MAX_CHARS = 4000


def _normalize_user_album_review_text(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._normalize_user_album_review_text_for_runtime(sys.modules[__name__], *args, **kwargs)


def _merge_user_album_feedback(
    current_rating: Any,
    current_review_text: Any,
    *,
    rating: Any = _USER_ALBUM_FEEDBACK_MISSING,
    review_text: Any = _USER_ALBUM_FEEDBACK_MISSING,
) -> tuple[int, str, bool]:
    try:
        next_rating = int(current_rating or 0)
    except Exception:
        next_rating = 0
    next_rating = max(0, min(5, next_rating))
    next_review_text = _normalize_user_album_review_text(current_review_text)

    if rating is not _USER_ALBUM_FEEDBACK_MISSING:
        try:
            next_rating = int(rating or 0)
        except Exception:
            next_rating = 0
        next_rating = max(0, min(5, next_rating))
    if review_text is not _USER_ALBUM_FEEDBACK_MISSING:
        next_review_text = _normalize_user_album_review_text(review_text)

    delete_row = bool(next_rating <= 0 and not next_review_text)
    return next_rating, next_review_text, delete_row


def _files_user_album_feedback_row(cur, user_id: int, album_id: int) -> tuple[int, str, int | None]:
    cur.execute(
        """
        SELECT rating, COALESCE(review_text, ''), EXTRACT(EPOCH FROM updated_at)::BIGINT
        FROM files_user_album_ratings
        WHERE user_id = %s AND album_id = %s
        LIMIT 1
        """,
        (int(user_id), int(album_id)),
    )
    row = cur.fetchone()
    if not row:
        return 0, "", None
    try:
        rating = max(0, min(5, int(row[0] or 0)))
    except Exception:
        rating = 0
    review_text = _normalize_user_album_review_text(row[1] if len(row) > 1 else "")
    try:
        updated_at = int(row[2] or 0) if len(row) > 2 and row[2] is not None else None
    except Exception:
        updated_at = None
    return rating, review_text, updated_at


def _files_album_user_ratings_map(conn, user_id: int, album_ids: list[int]) -> dict[int, int]:
    uid = max(0, int(user_id or 0))
    ids = sorted({int(aid) for aid in (album_ids or []) if int(aid or 0) > 0})
    if uid <= 0 or not ids:
        return {}
    out: dict[int, int] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT album_id, rating
            FROM files_user_album_ratings
            WHERE user_id = %s
              AND album_id = ANY(%s)
            """,
            (uid, ids),
        )
        for album_id, rating in cur.fetchall():
            try:
                aid = int(album_id or 0)
                val = max(1, min(5, int(rating or 0)))
            except Exception:
                continue
            if aid > 0 and val > 0:
                out[aid] = val
    return out


def api_library_album_detail(album_id):
    return _library_detail_runtime.api_library_album_detail_for_runtime(sys.modules[__name__], album_id)


def api_library_album_download(album_id: int):
    return _album_media_runtime.api_library_album_download_for_runtime(sys.modules[__name__], album_id)




def _track_file_path(db_conn, track_id: int) -> Optional[Path]:
    """Compatibility stub for the removed Plex-source track lookup path."""
    return None



def api_library_track_stream(track_id):
    """Stream a track from the files library."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 410
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT file_path FROM files_tracks WHERE id = %s", (track_id,))
            row = cur.fetchone()
        if not row or not (row[0] or "").strip():
            return jsonify({"error": "Track not found"}), 404
        local_path = path_for_fs_access(Path(row[0]))
        if not local_path.exists() or not local_path.is_file():
            return jsonify({"error": "Track file missing"}), 404
        return send_file(str(local_path), as_attachment=False, conditional=True)
    finally:
        conn.close()



def api_library_files_album_cover(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime.api_library_files_album_cover_for_runtime(sys.modules[__name__], *args, **kwargs)




def api_library_files_artist_image(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime.api_library_files_artist_image_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_external_artist_image(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime.api_library_external_artist_image_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_external_label_image(label_norm: str):
    return _album_media_runtime.api_library_external_label_image_for_runtime(sys.modules[__name__], label_norm)


def _run_lidarr_add_incomplete_albums(rows: List[tuple]):
    """Compatibility stub for the removed external acquisition workflow."""
    _set_disabled_lidarr_incomplete_progress(rows)


def get_artist_albums(db_conn, artist_id: int) -> List[dict]:
    """Compatibility stub for removed Plex-backed artist album lookup."""
    return []

def add_artist_to_lidarr(artist_id: int, artist_name: str, artist_mbid: str | None = None) -> bool:
    """Compatibility stub for the removed Lidarr acquisition workflow."""
    from pmda_core.legacy_integrations import ignore_artist_acquisition

    return ignore_artist_acquisition(artist_id, artist_name, artist_mbid)

def create_autobrr_filter(artist_names: List[str], quality_preferences: dict | None = None) -> bool:
    """Compatibility stub for the removed Autobrr acquisition workflow."""
    from pmda_core.legacy_integrations import ignore_autobrr_filter

    return ignore_autobrr_filter(artist_names, quality_preferences)

def get_similar_artists_mb(artist_mbid: str) -> List[dict]:
    return _artist_profile_runtime.get_similar_artists_mb_for_runtime(sys.modules[__name__], artist_mbid)

def api_library_artist_similar(artist_id):
    return _library_detail_runtime.api_library_artist_similar_for_runtime(sys.modules[__name__], artist_id)

def api_library_release_group_labels(mbid):
    return _release_group_runtime.api_library_release_group_labels_for_runtime(sys.modules[__name__], mbid)

def api_library_artist_monitored(artist_id):
    """Compatibility response for the removed external acquisition monitor."""
    if _get_library_mode() == "files":
        # files-library artist IDs are internal to the index and may change after rebuilds;
        # keep this endpoint deterministic in files mode.
        return jsonify({"monitored": False})
    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM monitored_artists WHERE artist_id = ?", (artist_id,))
    is_monitored = cur.fetchone() is not None
    con.close()
    return jsonify({"monitored": is_monitored})

def get_artist_images_mb(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime.get_artist_images_mb_for_runtime(sys.modules[__name__], *args, **kwargs)


def _resolve_remote_image_or_og_url(target_url: str, *, timeout: int = 8) -> str:
    return _image_utils_runtime.resolve_remote_image_or_og_url(target_url, timeout=timeout)


def _artist_folder_has_image(artist_folder: Path) -> bool:
    return _image_utils_runtime.artist_folder_has_image(artist_folder, _ARTIST_IMAGE_NAMES)


def _is_probably_placeholder_artist_image_url(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._is_probably_placeholder_artist_image_url_for_runtime(sys.modules[__name__], *args, **kwargs)


def _is_suspicious_external_artist_image_url(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._is_suspicious_external_artist_image_url_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_entity_is_classical_like(
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> bool:
    return _external_image_cache_runtime._artist_entity_is_classical_like_for_runtime(
        sys.modules[__name__],
        entity_kind=entity_kind,
        role_hints=role_hints,
    )


def _artist_wikipedia_lang_candidates(
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> tuple[str, ...]:
    return _external_image_cache_runtime._artist_wikipedia_lang_candidates_for_runtime(
        sys.modules[__name__],
        entity_kind=entity_kind,
        role_hints=role_hints,
    )


def _artist_external_image_requires_authoritative_refresh(
    *,
    provider: str = "",
    image_url: str = "",
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> bool:
    return _external_image_cache_runtime._artist_external_image_requires_authoritative_refresh_for_runtime(
        sys.modules[__name__],
        provider=provider,
        image_url=image_url,
        entity_kind=entity_kind,
        role_hints=role_hints,
    )


def _artist_image_provider_allowed_for_entity(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._artist_image_provider_allowed_for_entity_for_runtime(sys.modules[__name__], *args, **kwargs)


def _paths_refer_to_same_file(left: str | Path | None, right: str | Path | None) -> bool:
    return _image_utils_runtime.paths_refer_to_same_file(
        left,
        right,
        path_for_fs_access=path_for_fs_access,
    )


def _artist_external_image_requires_authoritative_refresh_sql(
    artist_alias: str = "a",
    ext_alias: str = "ext",
) -> str:
    return _external_image_cache_runtime._artist_external_image_requires_authoritative_refresh_sql_for_runtime(
        sys.modules[__name__],
        artist_alias=artist_alias,
        ext_alias=ext_alias,
    )


def _artist_has_true_image_sql(artist_alias: str = "a", ext_alias: str = "ext") -> str:
    return _image_utils_runtime.artist_has_true_image_sql(
        artist_alias,
        ext_alias,
        weak_ext_expr=_artist_external_image_requires_authoritative_refresh_sql(artist_alias, ext_alias),
    )


def _is_usable_artist_image_bytes(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._is_usable_artist_image_bytes_for_runtime(sys.modules[__name__], *args, **kwargs)


def _is_usable_artist_image_path(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._is_usable_artist_image_path_for_runtime(sys.modules[__name__], *args, **kwargs)


def _image_ahash_hex(raw: bytes, size: int = 8) -> str | None:
    return _image_utils_runtime.image_ahash_hex(raw, size=size)


def _hamming_hex(a: str, b: str) -> int:
    return _image_utils_runtime.hamming_hex(a, b)


def _is_artist_image_distinct_from_local_covers(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._is_artist_image_distinct_from_local_covers_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_and_save_artist_image_mb(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_and_save_artist_image_mb_for_runtime(sys.modules[__name__], *args, **kwargs)




def _fetch_artist_image_discogs(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_artist_image_discogs_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_artist_image_fanart(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_artist_image_fanart_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_artist_image_audiodb(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_artist_image_audiodb_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_image_search_queries(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._artist_image_search_queries_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_image_lookup_candidates(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._artist_image_lookup_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)


_ARTIST_IMAGE_GENERIC_ENSEMBLE_TOKENS = {
    "band",
    "choir",
    "chorus",
    "ensemble",
    "group",
    "music",
    "musical",
    "national",
    "orchestra",
    "orchester",
    "orkester",
    "philharmonic",
    "philharmonie",
    "radio",
    "symphonic",
    "symphonie",
    "symphony",
}
_ARTIST_IMAGE_BUILDING_TOKENS = {
    "auditorium",
    "avenue",
    "building",
    "center",
    "centre",
    "city",
    "downtown",
    "estate",
    "hall",
    "house",
    "hudson",
    "manhattan",
    "museum",
    "opera",
    "palace",
    "paris",
    "plaza",
    "prague",
    "real",
    "realestate",
    "rudolfinum",
    "skyline",
    "skyscraper",
    "state",
    "street",
    "theater",
    "theatre",
    "tower",
    "venue",
}
_ARTIST_IMAGE_EVENT_TOKENS = {
    "concert",
    "festival",
    "gala",
    "konzert",
    "live",
    "matinee",
    "performance",
    "rehearsal",
    "session",
    "tour",
}


def _artist_identity_distinctive_tokens(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
) -> set[str]:
    norm = _norm_artist_key(artist_name)
    tokens = {tok for tok in re.findall(r"[a-z0-9]+", norm) if tok}
    if not tokens:
        return set()
    if _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints):
        sig = _classical_person_alias_signature(artist_name)
        surname = str(sig.get("surname") or "").strip()
        long_givens = {str(tok or "").strip() for tok in (sig.get("long_givens") or set()) if str(tok or "").strip()}
        out = {surname} if surname else set()
        out.update({tok for tok in long_givens if len(tok) >= 4})
        return {tok for tok in out if tok}
    kind = str(entity_kind or "").strip().lower()
    roles = {str(role or "").strip().lower() for role in (role_hints or []) if str(role or "").strip()}
    if kind in {"orchestra", "ensemble", "choir", "chorus"} or roles.intersection({"orchestra", "ensemble", "choir", "chorus"}):
        return {tok for tok in tokens if len(tok) >= 4 and tok not in _ARTIST_IMAGE_GENERIC_ENSEMBLE_TOKENS}
    return {tok for tok in tokens if len(tok) >= 4}


def _artist_image_alias_candidate_is_compatible(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._artist_image_alias_candidate_is_compatible_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_image_exact_name_match(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._artist_image_exact_name_match_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_wikipedia_artist_bio_best(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_wikipedia_artist_bio_best_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_image_url_looks_relevant(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._artist_image_url_looks_relevant_for_runtime(sys.modules[__name__], *args, **kwargs)


def _artist_image_result_looks_relevant(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._artist_image_result_looks_relevant_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_artist_image_web(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_artist_image_web_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_and_save_artist_image(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_and_save_artist_image_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_library_artist_images(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime.api_library_artist_images_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_library_album_tags(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime.api_library_album_tags_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_album_tracks_detail(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime.api_library_album_tracks_detail_for_runtime(sys.modules[__name__], *args, **kwargs)


def _vision_verify_cover_before_inject(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._vision_verify_cover_before_inject_for_runtime(sys.modules[__name__], *args, **kwargs)



def _embed_cover_in_audio_files(cover_path: Path, audio_files: list) -> None:
    return _album_match_runtime.embed_cover_in_audio_files(cover_path, audio_files)


def _normalize_artist_credit_mode(mode: str | None) -> str:
    return _album_match_runtime.normalize_artist_credit_mode(mode)


def _split_main_and_featuring(artist_str: str | None, album_artist: str | None) -> tuple[str, list[str]]:
    return _album_match_runtime.split_main_and_featuring(artist_str, album_artist)


# PMDA album-level tags (stored in audio files; keys are lowercased by extract_tags()).
PMDA_ID_TAG = _album_match_runtime.PMDA_ID_TAG
PMDA_MATCHED_TAG = _album_match_runtime.PMDA_MATCHED_TAG
PMDA_MATCH_PROVIDER_TAG = _album_match_runtime.PMDA_MATCH_PROVIDER_TAG
PMDA_COVER_TAG = _album_match_runtime.PMDA_COVER_TAG
PMDA_COVER_PROVIDER_TAG = _album_match_runtime.PMDA_COVER_PROVIDER_TAG
PMDA_ARTIST_IMAGE_TAG = _album_match_runtime.PMDA_ARTIST_IMAGE_TAG
PMDA_ARTIST_PROVIDER_TAG = _album_match_runtime.PMDA_ARTIST_PROVIDER_TAG
PMDA_COMPLETE_TAG = _album_match_runtime.PMDA_COMPLETE_TAG


def _pmda_bool_from_str(val: str) -> bool:
    return _album_match_runtime.pmda_bool_from_str(val)


_MATCH_PROVIDER_ORDER = _album_match_runtime.MATCH_PROVIDER_ORDER
_MATCH_PROVIDER_LABELS = _album_match_runtime.MATCH_PROVIDER_LABELS


def _match_provider_label(provider: str | None) -> str:
    return _album_match_runtime.match_provider_label(
        provider,
        normalize_provider=_normalize_identity_provider,
    )


def _safe_json_load(value: Any, *, fallback: Any) -> Any:
    return _album_match_runtime.safe_json_load(value, fallback=fallback)


def _provider_reference_link(*args: Any, **kwargs: Any) -> Any:
    return _artwork_runtime._provider_reference_link_for_runtime(sys.modules[__name__], *args, **kwargs)



def _match_attempts_from_steps(steps: list[str], provider_used: str | None) -> list[dict[str, Any]]:
    return _album_match_runtime.match_attempts_from_steps(
        steps,
        provider_used,
        normalize_provider=_normalize_identity_provider,
    )


def _match_type_from_flags(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_runtime._match_type_from_flags_for_runtime(sys.modules[__name__], *args, **kwargs)


def _record_files_match_audit_album(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_runtime._record_files_match_audit_album_for_runtime(sys.modules[__name__], *args, **kwargs)


def _serialize_match_audit_row(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_runtime._serialize_match_audit_row_for_runtime(sys.modules[__name__], *args, **kwargs)


def _album_match_links(
    *,
    mbid: str,
    musicbrainz_release_id: str = "",
    discogs_release_id: str,
    lastfm_album_mbid: str,
    bandcamp_album_url: str,
    artist_name: str,
    album_title: str,
) -> list[dict[str, Any]]:
    return _album_match_runtime.album_match_links(
        mbid=mbid,
        musicbrainz_release_id=musicbrainz_release_id,
        discogs_release_id=discogs_release_id,
        lastfm_album_mbid=lastfm_album_mbid,
        bandcamp_album_url=bandcamp_album_url,
        artist_name=artist_name,
        album_title=album_title,
        normalize_provider=_normalize_identity_provider,
        provider_reference_link=_provider_reference_link,
    )


def _provider_payload_title(provider: str, payload: dict | None) -> str:
    return _album_match_runtime.provider_payload_title(provider, payload)


def _provider_payload_artist(provider: str, payload: dict | None) -> str:
    return _album_match_runtime.provider_payload_artist(provider, payload)


def _provider_cover_url_from_payload(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._provider_cover_url_from_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


def _cover_art_archive_front_urls(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._cover_art_archive_front_urls_for_runtime(sys.modules[__name__], *args, **kwargs)


def _cover_art_archive_front_urls_for_identity(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._cover_art_archive_front_urls_for_identity_for_runtime(sys.modules[__name__], *args, **kwargs)


def _download_cover_art_archive_front(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._download_cover_art_archive_front_for_runtime(sys.modules[__name__], *args, **kwargs)


def _provider_year_from_payload(payload: dict | None) -> int | None:
    return _album_match_runtime.provider_year_from_payload(payload)


def _provider_versions_from_payload(provider: str, payload: dict | None) -> list[dict[str, Any]]:
    return _album_match_runtime.provider_versions_from_payload(provider, payload)


def _review_lookup_query_plan(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_lookup_query_plan_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_candidate_signal_blob(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_candidate_signal_blob_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_candidate_has_obvious_match(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_candidate_has_obvious_match_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_score_hits(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_score_hits_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_lines_from_scored(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_lines_from_scored_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_lookup_collect_hits(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_lookup_collect_hits_for_runtime(sys.modules[__name__], *args, **kwargs)


_REVIEW_PAGE_FETCH_TIMEOUT_SEC = 8.0
_REVIEW_PAGE_EXCERPT_MAX_CHARS = 1400
_REVIEW_MAX_CANDIDATE_PAGES = 6
_REVIEW_MAX_CANDIDATE_PROBES = 12
_REVIEW_BODY_MIN_PARAGRAPH_CHARS = 80
_REVIEW_BODY_MAX_PARAGRAPHS = 6

_REVIEW_PARAGRAPH_PREFIX_PATTERNS = (
    r"^(?:save this story(?:\s+save story)?(?:\s+save this story)?\s*)+",
    r"^(?:share this story\s+)+",
    r"^(?:newsletter\s+search(?:\s+search)?(?:\s+news)?(?:\s+reviews)?(?:\s+best new music)?(?:\s+features)?(?:\s+lists)?(?:\s+columns)?(?:\s+video)?(?:\s+open navigation menu)?(?:\s+menu)?(?:\s+search)?\s*)+",
)

_REVIEW_PARAGRAPH_TRIM_MARKERS = (
    "most read",
    "all rights reserved",
    "affiliate partnerships",
    "copyright",
    "newsletter",
    "open navigation menu",
    "save this story",
    "related articles",
    "more from",
)

_REVIEW_PARAGRAPH_DROP_PATTERNS = (
    r"\bnewsletter\b",
    r"\bopen navigation menu\b",
    r"\ball rights reserved\b",
    r"\baffiliate partnerships\b",
    r"\bpitchfork may earn\b",
    r"\bconde nast\b",
    r"\bhas been writing for\b",
    r"\bsign up\b",
    r"\bprivacy policy\b",
    r"\bterms of use\b",
)


def _review_page_meta_content(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_page_meta_content_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_clean_paragraph_text(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_clean_paragraph_text_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_extract_paragraphs_from_html_block(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_extract_paragraphs_from_html_block_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_extract_body_excerpt(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_extract_body_excerpt_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_fetch_page_context(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_fetch_page_context_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_prepare_candidates(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_prepare_candidates_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_candidates_need_broader_retry(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_candidates_need_broader_retry_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_validate_candidates_with_ai(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_validate_candidates_with_ai_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_search_source_from_hits(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_search_source_from_hits_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_summary_fallback_from_hits(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_summary_fallback_from_hits_for_runtime(sys.modules[__name__], *args, **kwargs)


def _review_ai_provider_source(*args: Any, **kwargs: Any) -> Any:
    return _album_review_lookup_runtime._review_ai_provider_source_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_album_review_web_ai(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._fetch_album_review_web_ai_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_album_review_web_ai_batch(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._fetch_album_review_web_ai_batch_for_runtime(sys.modules[__name__], *args, **kwargs)


def _album_review_search_context(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._album_review_search_context_for_runtime(sys.modules[__name__], *args, **kwargs)


def _resolve_album_review_identity_from_provider_hints(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._resolve_album_review_identity_from_provider_hints_for_runtime(sys.modules[__name__], *args, **kwargs)


def _discogs_release_notes_text(payload: dict | None) -> str:
    if not isinstance(payload, dict):
        return ""
    notes = payload.get("notes")
    if isinstance(notes, str):
        return _strip_html_text(notes.strip())
    if isinstance(notes, list):
        parts = [_strip_html_text(str(x or "").strip()) for x in notes]
        return _truncate_text(" ".join([p for p in parts if p]), max_chars=2400)
    return ""


def _fetch_album_profile_from_provider_fallback(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._fetch_album_profile_from_provider_fallback_for_runtime(sys.modules[__name__], *args, **kwargs)


def _album_profile_provider_candidate(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._album_profile_provider_candidate_for_runtime(sys.modules[__name__], *args, **kwargs)


def _choose_best_album_profile_provider_candidate(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._choose_best_album_profile_provider_candidate_for_runtime(sys.modules[__name__], *args, **kwargs)


def _fetch_best_album_profile(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._fetch_best_album_profile_for_runtime(sys.modules[__name__], *args, **kwargs)


def _build_album_provider_crosscheck(*args: Any, **kwargs: Any) -> Any:
    return _identity_runtime._build_album_provider_crosscheck_for_runtime(sys.modules[__name__], *args, **kwargs)



def _is_safe_public_http_url(url: str) -> bool:
    raw = str(url or "").strip()
    if not raw:
        return False
    try:
        parsed = urlparse(raw)
    except Exception:
        return False
    scheme = str(parsed.scheme or "").strip().lower()
    host = str(parsed.hostname or "").strip().lower()
    if scheme not in {"http", "https"}:
        return False
    if not host:
        return False
    if host in {"localhost", "ip6-localhost"} or host.endswith(".local"):
        return False
    try:
        ip_val = ipaddress.ip_address(host)
        if (
            ip_val.is_private
            or ip_val.is_loopback
            or ip_val.is_link_local
            or ip_val.is_reserved
            or ip_val.is_multicast
        ):
            return False
    except Exception:
        # Hostname (not a literal IP) -> allowed.
        pass
    return True


def _set_pmda_tag(audio, key: str, value: str) -> None:
    return _album_match_runtime.set_pmda_tag(audio, key, value)


def _write_pmda_album_tags(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._write_pmda_album_tags_for_runtime(sys.modules[__name__], *args, **kwargs)


def _apply_artist_album_tags_to_audio(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime._apply_artist_album_tags_to_audio_for_runtime(sys.modules[__name__], *args, **kwargs)

def _improve_single_album(album_id: int, db_conn, known_release_group_id: Optional[str] = None) -> dict:
    return _library_improve_runtime.improve_single_album_for_runtime(
        sys.modules[__name__],
        album_id,
        db_conn,
        known_release_group_id=known_release_group_id,
    )


def _track_index_from_file(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_runtime._track_index_from_file_for_runtime(sys.modules[__name__], *args, **kwargs)


def _infer_artist_album_from_folder(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_runtime._infer_artist_album_from_folder_for_runtime(sys.modules[__name__], *args, **kwargs)


def _files_child_folder_name_looks_release_segment(*args: Any, **kwargs: Any) -> Any:
    return _audio_runtime._files_child_folder_name_looks_release_segment_for_runtime(sys.modules[__name__], *args, **kwargs)



def _folder_release_segment_child_dirs(*args: Any, **kwargs: Any) -> Any:
    return _audio_runtime._folder_release_segment_child_dirs_for_runtime(sys.modules[__name__], *args, **kwargs)



def _folder_has_release_segment_children(*args: Any, **kwargs: Any) -> Any:
    return _audio_runtime._folder_has_release_segment_children_for_runtime(sys.modules[__name__], *args, **kwargs)



def _collapse_nested_album_folder_groups(*args: Any, **kwargs: Any) -> Any:
    return _audio_runtime._collapse_nested_album_folder_groups_for_runtime(sys.modules[__name__], *args, **kwargs)



def _improve_folder_by_path(folder_path: Path) -> dict:
    return _library_improve_runtime.improve_folder_by_path_for_runtime(
        sys.modules[__name__],
        folder_path,
    )


def api_library_album_match_detail(album_id):
    return _library_detail_runtime.api_library_album_match_detail_for_runtime(sys.modules[__name__], album_id)


def api_library_album_review_generate(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime.api_library_album_review_generate_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_album_select_cover(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime.api_library_album_select_cover_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_artist_match_detail(artist_id):
    return _library_detail_runtime.api_library_artist_match_detail_for_runtime(sys.modules[__name__], artist_id)


def api_library_album_rematch(album_id: int):
    return _library_improve_runtime.api_library_album_rematch_for_runtime(sys.modules[__name__], album_id)


def api_library_artist_rematch(artist_id: int):
    return _library_improve_runtime.api_library_artist_rematch_for_runtime(sys.modules[__name__], artist_id)


def api_library_improve_album(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_runtime.api_library_improve_album_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_drop_improve(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_runtime.api_drop_improve_for_runtime(sys.modules[__name__], *args, **kwargs)


def _improve_one_album_item(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_batch_runtime._improve_one_album_item_for_runtime(sys.modules[__name__], *args, **kwargs)


def _build_improve_items_from_editions(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_batch_runtime._build_improve_items_from_editions_for_runtime(sys.modules[__name__], *args, **kwargs)


def _run_improve_all_albums_global(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_batch_runtime._run_improve_all_albums_global_for_runtime(sys.modules[__name__], *args, **kwargs)


def _run_scan_profile_enrichment_inline(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_batch_runtime._run_scan_profile_enrichment_inline_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_collect_profile_enrich_targets(*args, **kwargs):
    return _scan_targets_runtime._scan_collect_profile_enrich_targets_for_runtime(sys.modules[__name__], *args, **kwargs)



def _run_improve_all_albums(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_batch_runtime._run_improve_all_albums_for_runtime(sys.modules[__name__], *args, **kwargs)


def _mb_missing_release_group_ids_cache(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_batch_runtime._mb_missing_release_group_ids_cache_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_improve_all_albums(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_runtime.api_library_improve_all_albums_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_improve_all(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_runtime.api_library_improve_all_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_improve_all_progress(*args: Any, **kwargs: Any) -> Any:
    return _library_improve_runtime.api_library_improve_all_progress_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_musicbrainz_fix_artist_tags():
    """Compatibility stub for the removed Plex artist-wide MusicBrainz tag fixer."""
    return jsonify({
        "error": "Legacy artist-wide Plex tag fixer is disabled in Files mode. Use the Files album improvement endpoints instead."
    }), 410

def api_musicbrainz_fix_album_tags(*args: Any, **kwargs: Any) -> Any:
    return _album_media_runtime.api_musicbrainz_fix_album_tags_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_scan_history_detail(*args: Any, **kwargs: Any) -> Any:
    return _scan_history_runtime.api_scan_history_detail_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_pipeline_trace_row_to_api(*args: Any, **kwargs: Any) -> Any:
    return _scan_history_runtime._scan_pipeline_trace_row_to_api_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_pipeline_trace_filtered_query(*args: Any, **kwargs: Any) -> Any:
    return _scan_history_runtime._scan_pipeline_trace_filtered_query_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_scan_history_pipeline_trace(*args: Any, **kwargs: Any) -> Any:
    return _scan_history_runtime.api_scan_history_pipeline_trace_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_scan_history_pipeline_trace_export(*args: Any, **kwargs: Any) -> Any:
    return _scan_history_runtime.api_scan_history_pipeline_trace_export_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_scan_ai_costs(*args: Any, **kwargs: Any) -> Any:
    return _scan_history_runtime.api_scan_ai_costs_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_move_reason_label(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._scan_move_reason_label_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_move_status(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._scan_move_status_for_runtime(sys.modules[__name__], *args, **kwargs)


_TRASH_RELEASE_MIN_SCORE_DEFAULT = 4
_TRASH_RELEASE_GENERIC_ARTISTS = {
    "various",
    "various artists",
    "va",
    "dj mix",
    "soundtrack",
}
_TRASH_RELEASE_RULES: tuple[dict[str, Any], ...] = (
    {
        "category": "fitness",
        "label": "Workout / fitness wording",
        "weight": 5,
        "patterns": (
            "workout",
            "fitness",
            "gym",
            "cardio",
            "zumba",
            "spinning",
            "aerobics",
            "running mix",
            "power walk",
            "exercise",
        ),
    },
    {
        "category": "ibiza_party",
        "label": "Ibiza / party compilation wording",
        "weight": 4,
        "patterns": (
            "ibiza",
            "beach party",
            "pool party",
            "summer hits",
            "club anthems",
            "party hits",
            "dance hits",
            "mega dance",
        ),
    },
    {
        "category": "chart_hits",
        "label": "Chart / top hits wording",
        "weight": 4,
        "patterns": (
            "top hits",
            "top 40",
            "top 100",
            "chart hits",
            "number 1 hits",
            "best hits",
            "hits of 20",
            "best of 20",
            "now that's what i call",
        ),
    },
    {
        "category": "karaoke_tribute",
        "label": "Karaoke / tribute wording",
        "weight": 5,
        "patterns": (
            "karaoke",
            "tribute to",
            "tribute band",
            "instrumental versions",
            "backing tracks",
            "sing along",
            "cover versions",
        ),
    },
)



def _trash_release_safe_json(*args: Any, **kwargs: Any) -> Any:
    return _tools_runtime._trash_release_safe_json_for_runtime(sys.modules[__name__], *args, **kwargs)


def _trash_release_tags_text(*args: Any, **kwargs: Any) -> Any:
    return _tools_runtime._trash_release_tags_text_for_runtime(sys.modules[__name__], *args, **kwargs)


def _trash_release_compilation_flag(*args: Any, **kwargs: Any) -> Any:
    return _tools_runtime._trash_release_compilation_flag_for_runtime(sys.modules[__name__], *args, **kwargs)


def _trash_release_candidate_from_album_row(*args: Any, **kwargs: Any) -> Any:
    return _tools_runtime._trash_release_candidate_from_album_row_for_runtime(sys.modules[__name__], *args, **kwargs)


def _trash_release_candidates_snapshot(*args: Any, **kwargs: Any) -> Any:
    return _tools_runtime._trash_release_candidates_snapshot_for_runtime(sys.modules[__name__], *args, **kwargs)


def _trash_release_fetch_library_album_row(*args: Any, **kwargs: Any) -> Any:
    return _tools_runtime._trash_release_fetch_library_album_row_for_runtime(sys.modules[__name__], *args, **kwargs)


def _trash_release_destination(*args: Any, **kwargs: Any) -> Any:
    return _tools_runtime._trash_release_destination_for_runtime(sys.modules[__name__], *args, **kwargs)


def _record_library_curation_action(*args: Any, **kwargs: Any) -> Any:
    return _tools_runtime._record_library_curation_action_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_tools_trash_releases(*args: Any, **kwargs: Any) -> Any:
    return _tools_runtime.api_tools_trash_releases_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_tools_trash_releases_action(*args: Any, **kwargs: Any) -> Any:
    return _tools_runtime.api_tools_trash_releases_action_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_move_active_folder(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._scan_move_active_folder_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_move_folder_artwork_url(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._scan_move_folder_artwork_url_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_move_artwork_source_path(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._scan_move_artwork_source_path_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_move_prewarm_artwork(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._scan_move_prewarm_artwork_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_move_folder_artwork_response(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._scan_move_folder_artwork_response_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_move_quick_track_entries_from_folder(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._scan_move_quick_track_entries_from_folder_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_move_track_entries_from_folder(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._scan_move_track_entries_from_folder_for_runtime(sys.modules[__name__], *args, **kwargs)


def _scan_move_expected_tracks(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._scan_move_expected_tracks_for_runtime(sys.modules[__name__], *args, **kwargs)


def _broken_album_reason_summary(
    *,
    expected_track_count: int,
    actual_track_count: int,
    missing_indices: list[int] | None = None,
    missing_required_tags: list[str] | None = None,
    strict_reject_reason: str = "",
) -> str:
    missing_flat = [int(v) for v in (missing_indices or []) if _parse_int_loose(v, 0) > 0]
    missing_tags = [str(v).strip() for v in (missing_required_tags or []) if str(v).strip()]
    strict_reason = str(strict_reject_reason or "").strip()
    if missing_tags:
        return "Required tags are missing: " + ", ".join(missing_tags[:8])
    if expected_track_count > 0 and actual_track_count > 0 and actual_track_count < expected_track_count:
        if missing_flat:
            preview = ", ".join(str(v) for v in missing_flat[:12])
            if len(missing_flat) > 12:
                preview += ", ..."
            return (
                f"Local folder has {actual_track_count} track(s) but the expected edition has "
                f"{expected_track_count}; missing track number(s): {preview}."
            )
        return f"Local folder has {actual_track_count} track(s) but the expected edition has {expected_track_count}."
    if expected_track_count > 0 and actual_track_count == expected_track_count and missing_flat:
        preview = ", ".join(str(v) for v in missing_flat[:12])
        if len(missing_flat) > 12:
            preview += ", ..."
        return (
            f"Track numbering has gaps ({preview}) even though the file count matches the expected total. "
            "This usually points to tag or numbering issues rather than missing files."
        )
    if strict_reason:
        return f"Strict verification failed because: {strict_reason.replace('_', ' ')}."
    return "PMDA flagged this album as incomplete during strict verification."


def _broken_album_display_title(album_title: Any, folder_path: Any, album_id: int) -> str:
    title = str(album_title or "").strip()
    if title and not re.match(r"^Album\s+\d+$", title, flags=re.IGNORECASE):
        return title
    folder = str(folder_path or "").strip()
    if folder:
        try:
            name = Path(folder).name.strip()
            if name:
                return name
        except Exception:
            pass
    return title or f"Album {int(album_id or 0)}"


def _scan_move_detail_payload(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime._scan_move_detail_payload_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_scan_history_moves(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime.api_scan_history_moves_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_scan_history_moves_summary(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime.api_scan_history_moves_summary_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_scan_move_artwork(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime.api_scan_move_artwork_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_scan_move_detail(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime.api_scan_move_detail_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_scan_history_restore(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime.api_scan_history_restore_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_scan_history_dedupe(*args: Any, **kwargs: Any) -> Any:
    return _scan_move_audit_runtime.api_scan_history_dedupe_for_runtime(sys.modules[__name__], *args, **kwargs)

def api_dedupe(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_actions_runtime.api_dedupe_for_runtime(sys.modules[__name__], *args, **kwargs)


def details(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_actions_runtime.details_for_runtime(sys.modules[__name__], *args, **kwargs)



def _normalize_edition_as_best(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_actions_runtime._normalize_edition_as_best_for_runtime(sys.modules[__name__], *args, **kwargs)




def _run_dedupe_artist_one(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_actions_runtime._run_dedupe_artist_one_for_runtime(sys.modules[__name__], *args, **kwargs)



def dedupe_artist(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_actions_runtime.dedupe_artist_for_runtime(sys.modules[__name__], *args, **kwargs)



# Allowed extensions for bonus track move (security)
_MOVE_TRACK_EXTENSIONS = frozenset(
    ".flac .wav .m4a .mp3 .ogg .opus .aac .ape .alac .dsf .aif .aiff .wma .mp4 .m4b .m4p .aifc".split()
)


def _merge_bonus_tracks_for_group(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_actions_runtime._merge_bonus_tracks_for_group_for_runtime(sys.modules[__name__], *args, **kwargs)




def dedupe_move_track(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_actions_runtime.dedupe_move_track_for_runtime(sys.modules[__name__], *args, **kwargs)



def _dedupe_all_impl(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_actions_runtime._dedupe_all_impl_for_runtime(sys.modules[__name__], *args, **kwargs)



def api_dedupe_all(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_actions_runtime.api_dedupe_all_for_runtime(sys.modules[__name__], *args, **kwargs)



def dedupe_all(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_actions_runtime.dedupe_all_for_runtime(sys.modules[__name__], *args, **kwargs)



def dedupe_merge_and_dedupe(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_actions_runtime.dedupe_merge_and_dedupe_for_runtime(sys.modules[__name__], *args, **kwargs)


def dedupe_selected(*args: Any, **kwargs: Any) -> Any:
    return _dedupe_actions_runtime.dedupe_selected_for_runtime(sys.modules[__name__], *args, **kwargs)



# ─────────────────────────────── Assistant API ───────────────────────────────
def api_assistant_status(*args, **kwargs):
    return _assistant_chat_runtime.api_assistant_status_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_assistant_get_session(*args, **kwargs):
    return _assistant_chat_runtime.api_assistant_get_session_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_assistant_chat(*args, **kwargs):
    return _assistant_chat_runtime.api_assistant_chat_for_runtime(sys.modules[__name__], *args, **kwargs)


def api_library_entity_discover():
    return _library_catalog_runtime.api_library_entity_discover_for_runtime(sys.modules[__name__])


# ───────────────────────────────── MAIN ───────────────────────────────────
import os

if __name__ == "__main__":
    # Web UI only: start server first so UI is available immediately, then run startup checks (cross-check in background).
    def run_server():
        app.run(host="0.0.0.0", port=WEBUI_PORT, threaded=True, use_reloader=False)

    run_startup_checks()

    server_thread = threading.Thread(target=run_server, daemon=False)
    server_thread.start()
    logging.info("Web UI listening on http://0.0.0.0:%s", WEBUI_PORT)
    _start_artwork_ram_cache_auto_worker()
    _start_runtime_auto_tune_worker()
    if ARTWORK_RAM_CACHE_AUTO:
        try:
            applied = _apply_auto_artwork_ram_target(force=True)
            logging.info(
                "Artwork RAM auto-tune initialized at %dMB (cap=%s, interval=%ss)",
                applied,
                _artwork_auto_cap_log_label(),
                ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC,
            )
        except Exception:
            logging.debug("Artwork RAM auto-tune init failed", exc_info=True)
    if AUTO_TUNE_ENABLED:
        logging.info(
            "Runtime auto-tune initialized (interval=%ss, mb_min=%.1f, mb_max=%.1f, gateway_inflight=%d-%d)",
            int(max(15, AUTO_TUNE_INTERVAL_SEC or 60)),
            float(max(1.0, AUTO_TUNE_MB_MIRROR_MIN_RPS or 1.0)),
            float(max(1.0, AUTO_TUNE_MB_MIRROR_MAX_RPS or 1.0)),
            int(max(1, AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN or 1)),
            int(max(1, AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP or 1)),
        )

    def run_cross_check_background():
        if _get_library_mode() == "files":
            logging.info("PATH cross-check skipped at startup (LIBRARY_MODE=files).")
        elif DISABLE_PATH_CROSSCHECK:
            logging.info("PATH cross-check skipped (DISABLE_PATH_CROSSCHECK=true).")

    cross_check_thread = threading.Thread(target=run_cross_check_background, daemon=True)
    cross_check_thread.start()

    if _get_library_mode() == "files":
        _request_files_watcher_reconcile("startup", force=True)
        def run_files_index_bootstrap():
            ok, err = _ensure_files_index_ready()
            if ok:
                logging.info("Files library index is ready.")
                try:
                    _reconcile_scan_move_trace_backlog(reason="startup")
                except Exception:
                    logging.warning("[Trace] X❌ Startup move trace reconcile failed", exc_info=True)
            else:
                err_msg = str(err or "").strip()
                bootstrap_required = bool(_pipeline_bootstrap_status().get("bootstrap_required"))
                if err_msg == "FILES_ROOTS is empty" and bootstrap_required:
                    logging.info("Files library index bootstrap deferred until onboarding writes library roots.")
                else:
                    logging.warning("Files library index bootstrap failed: %s", err)
        threading.Thread(target=run_files_index_bootstrap, daemon=True, name="files-index-bootstrap").start()
        _maybe_resume_interrupted_scan_on_startup()

    _start_scheduler_if_needed()

    server_thread.join()  # block forever (app.run never returns)
