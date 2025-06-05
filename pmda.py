#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
plex_dedupe_robust_webui.py

A Flask-based Web UI for scanning and deduplicating duplicate albums in Plex Music.
Handles special characters (slashes, etc.) in album titles by using numeric album_id,
and ensures covers appear in the “Moved Duplicates” modal by fetching them as Base64
data URIs before deleting metadata.

Additionally, if run without --serve, it behaves as the original CLI script,
accepting --dry-run, --safe-mode, --tag-extra, --verbose etc. to dedupe from console.
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
from pathlib import Path
from typing import NamedTuple, List, Dict
from urllib.parse import quote_plus

import requests
from flask import Flask, render_template_string, request, jsonify

# ───────────────────────────────── CONFIGURATION LOADING ─────────────────────────────────
#
# We load the JSON config file at startup.  If the file is missing or malformed, we exit.
#
CONFIG_PATH = Path(__file__).parent / "config.json"
if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"Configuration file not found: {CONFIG_PATH}")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    conf = json.load(f)

# Mandatory keys in config.json:
required_keys = [
    "PLEX_DB_FILE", "PLEX_HOST", "PLEX_TOKEN",
    "SECTION_ID", "PATH_MAP", "DUPE_ROOT", "STATS_FILE"
]
for key in required_keys:
    if key not in conf:
        raise KeyError(f"Missing required configuration key: '{key}' in {CONFIG_PATH}")

# Assign global variables from the loaded JSON config
PLEX_DB_FILE = conf["PLEX_DB_FILE"]
PLEX_HOST    = conf["PLEX_HOST"]
PLEX_TOKEN   = conf["PLEX_TOKEN"]
SECTION_ID   = int(conf["SECTION_ID"])

# PATH_MAP is stored as JSON object, so we convert it to a Python dict[str, str]
PATH_MAP = {str(k): str(v) for k, v in conf["PATH_MAP"].items()}

# DUPE_ROOT and STATS_FILE are paths on disk; wrap them in pathlib.Path
DUPE_ROOT  = Path(conf["DUPE_ROOT"])
STATS_FILE = Path(conf["STATS_FILE"])

# Validate that DUPE_ROOT and STATS_FILE parent folder exist (or create them)
DUPE_ROOT.mkdir(parents=True, exist_ok=True)
STATS_FILE.parent.mkdir(parents=True, exist_ok=True)

# ───────────────────────────────── OTHER CONSTANTS ──────────────────────────────────
AUDIO_RE    = re.compile(r"\.(flac|ape|alac|wav|m4a|aac|mp3|ogg)$", re.I)
FMT_SCORE   = {'flac': 3, 'ape': 3, 'alac': 3, 'wav': 3, 'm4a': 2, 'aac': 2, 'mp3': 1, 'ogg': 1}
OVERLAP_MIN = 0.85  # 85% track-title overlap minimum

# ───────────────────────────────── STATE ──────────────────────────────────
state = {
    "scanning": False,
    "scan_progress": 0,
    "scan_total": 0,
    "deduping": False,
    "dedupe_progress": 0,
    "dedupe_total": 0,
    # duplicates: { artist_name: [ { album_id, best, losers } ] }
    "duplicates": {},
    "space_saved": 0
}
lock = threading.Lock()

# ───────────────────────────────── LOGGING ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(message)s",
    datefmt="%H:%M:%S"
)

# ─────────────────────────── LOAD / SAVE STATS ──────────────────────────────
def load_stats():
    if STATS_FILE.exists():
        try:
            d = json.loads(STATS_FILE.read_text())
            state["space_saved"] = d.get("space_saved", 0)
        except Exception:
            pass

def save_stats():
    STATS_FILE.write_text(json.dumps({"space_saved": state["space_saved"]}))

# ───────────────────────────────── UTILITIES ──────────────────────────────────
def plex_api(path: str, method: str = "GET", **kw):
    headers = kw.pop("headers", {})
    headers["X-Plex-Token"] = PLEX_TOKEN
    return requests.request(method, f"{PLEX_HOST}{path}", headers=headers, timeout=60, **kw)

def container_to_host(p: str) -> Path | None:
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
    Strip trailing parenthetical, lowercase, and trim.
    e.g. "Album Name (Special Edition)" → "album name"
    """
    return re.sub(r"\s*\([^)]*\)\s*$", "", title, flags=re.I).strip().lower()

def get_primary_format(folder: Path) -> str:
    for f in folder.rglob("*"):
        if AUDIO_RE.search(f.name):
            return f.suffix[1:].upper()
    return "UNKNOWN"

def thumb_url(album_id: int) -> str:
    # Thumb endpoint for a given metadata item
    return f"{PLEX_HOST}/library/metadata/{album_id}/thumb?X-Plex-Token={PLEX_TOKEN}"

# ───────────────────────────────── DATABASE HELPERS ──────────────────────────────────
class Track(NamedTuple):
    title: str
    idx: int
    disc: int
    dur: int  # duration in ms

def get_tracks(db, album_id: int) -> List[Track]:
    has_parent = any(r[1] == "parent_index"
                     for r in db.execute("PRAGMA table_info(metadata_items)"))
    sql = f"""
      SELECT tr.title, tr."index",
             {'tr.parent_index' if has_parent else 'NULL'} AS disc_no,
             mp.duration
      FROM metadata_items tr
      JOIN media_items mi ON mi.metadata_item_id = tr.id
      JOIN media_parts mp ON mp.media_item_id = mi.id
      WHERE tr.parent_id = ? AND tr.metadata_type = 10
    """
    rows = db.execute(sql, (album_id,)).fetchall()
    return [Track(t.lower().strip(), i or 0, d or 1, dur or 0)
            for t, i, d, dur in rows]

def album_title(db, album_id: int) -> str:
    return db.execute(
        "SELECT title FROM metadata_items WHERE id = ?", (album_id,)
    ).fetchone()[0]

def first_part_path(db, album_id: int) -> Path | None:
    sql = """
      SELECT mp.file
      FROM metadata_items tr
      JOIN media_items mi ON mi.metadata_item_id = tr.id
      JOIN media_parts mp ON mp.media_item_id = mi.id
      WHERE tr.parent_id = ? LIMIT 1
    """
    r = db.execute(sql, (album_id,)).fetchone()
    return container_to_host(r[0]).parent if r and container_to_host(r[0]) else None

def analyse_format(folder: Path) -> tuple[int, int, int]:
    for f in folder.rglob("*"):
        if AUDIO_RE.search(f.name):
            ext = f.suffix[1:]
            try:
                mi = json.loads(subprocess.check_output(
                    ["mediainfo", "--Output=JSON", str(f)], text=True, timeout=10
                ))
                tr = mi["media"]["track"][0]
                return (
                    score_format(ext),
                    int(tr.get("BitRate", 0)),
                    int(tr.get("SamplingRate", 0))
                )
            except Exception:
                return (score_format(ext), 0, 0)
    return (0, 0, 0)

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
    # Compare by (fmt, bitrate, samplerate, fewer discs, longer total dur)
    return max(
        editions,
        key=lambda e: (
            e['fmt'], e['br'], e['sr'],
            -e['discs'], e['dur']
        )
    )

def scan_duplicates(db, artist: str, album_ids: List[int]) -> List[dict]:
    editions = []
    for aid in album_ids:
        tr = get_tracks(db, aid)
        if not tr:
            continue
        folder = first_part_path(db, aid)
        if not folder:
            continue
        fmt, br, sr = analyse_format(folder)
        editions.append({
            'album_id': aid,
            'title_raw': album_title(db, aid),
            'album_norm': norm_album(album_title(db, aid)),
            'folder': folder,
            'tracks': tr,
            'sig': signature(tr),
            'titles': {t.title for t in tr},
            'dur': sum(t.dur for t in tr),
            'fmt': fmt, 'br': br, 'sr': sr,
            'discs': len({t.disc for t in tr})
        })

    # Group by (normalized title, signature) to detect duplicates
    groups = defaultdict(list)
    for e in editions:
        groups[(e['album_norm'], e['sig'])].append(e)

    out = []
    for (alb_norm, _), ed_list in groups.items():
        if len(ed_list) < 2:
            continue
        # Check 85%+ overlap on track titles to skip real box-sets
        common = set.intersection(*(e['titles'] for e in ed_list))
        if not all(overlap(common, e['titles']) >= OVERLAP_MIN for e in ed_list):
            continue
        best = choose_best(ed_list)
        losers = [e for e in ed_list if e is not best]
        out.append({
            'album_id': best['album_id'],
            'artist': artist,
            'best': best,
            'losers': losers
        })
    return out

def fetch_cover_as_base64(album_id: int) -> str:
    """
    Request the Plex thumbnail endpoint, return a data URI:
    data:image/jpeg;base64,<base64 content>
    """
    try:
        resp = requests.get(thumb_url(album_id), timeout=10)
        if resp.status_code == 200:
            b64 = base64.b64encode(resp.content).decode()
            return f"data:image/jpeg;base64,{b64}"
    except Exception:
        pass
    return ""

def perform_dedupe(group: dict) -> List[dict]:
    """
    Moves each “loser” folder to DUPE_ROOT, deletes metadata via Plex API,
    and returns a list of moved-item dicts:
      [ { thumb_data, artist, title_raw, size, fmt }, … ]
    """
    moved = []
    best = group['best']
    artist = group['artist']
    for e in group['losers']:
        # 1) Fetch the cover image as Base64 BEFORE deleting
        thumb_data = fetch_cover_as_base64(e['album_id'])
        try:
            # 2) Move folder on disk
            if e['folder'].exists() and not e['folder'].samefile(best['folder']):
                dst = DUPE_ROOT / e['folder'].relative_to(next(iter(PATH_MAP.values())))
                dst.parent.mkdir(parents=True, exist_ok=True)
                size_mb = folder_size(e['folder']) // (1024 * 1024)
                fmt_txt = get_primary_format(e['folder'])
                shutil.move(str(e['folder']), str(dst))

                # 3) Delete metadata in Plex
                plex_api(f"/library/metadata/{e['album_id']}/trash", method="PUT")
                time.sleep(0.3)
                plex_api(f"/library/metadata/{e['album_id']}", method="DELETE")

                moved.append({
                    'thumb_data': thumb_data,
                    'artist': artist,
                    'title_raw': e['title_raw'],
                    'size': size_mb,
                    'fmt': fmt_txt
                })
            else:
                # If folder is missing or same as best, just delete metadata
                plex_api(f"/library/metadata/{e['album_id']}/trash", method="PUT")
                time.sleep(0.3)
                plex_api(f"/library/metadata/{e['album_id']}", method="DELETE")

                moved.append({
                    'thumb_data': thumb_data,
                    'artist': artist,
                    'title_raw': e['title_raw'],
                    'size': 0,
                    'fmt': get_primary_format(e['folder'])
                })
        except Exception:
            pass
    return moved

# ───────────────────────────── BACKGROUND TASKS (WEB) ─────────────────────────────
def background_scan():
    db = sqlite3.connect(PLEX_DB_FILE)
    with lock:
        state.update(scanning=True, scan_progress=0, duplicates={})
        state["scan_total"] = db.execute(
            "SELECT COUNT(*) FROM metadata_items WHERE metadata_type=9 AND library_section_id=?",
            (SECTION_ID,)
        ).fetchone()[0]

    artists = db.execute(
        "SELECT id, title FROM metadata_items WHERE metadata_type=8 AND library_section_id=?",
        (SECTION_ID,)
    ).fetchall()

    for a_id, artist_name in artists:
        album_ids = [rk for rk, in db.execute(
            "SELECT id FROM metadata_items WHERE metadata_type=9 AND parent_id=?",
            (a_id,)
        ).fetchall()]
        groups = scan_duplicates(db, artist_name, album_ids)
        with lock:
            if groups:
                state["duplicates"][artist_name] = groups
            state["scan_progress"] += len(album_ids)

    with lock:
        state["scanning"] = False

def background_dedupe(all_groups: List[dict]):
    with lock:
        state.update(deduping=True, dedupe_progress=0, dedupe_total=len(all_groups))

    total_moved = 0
    artists_to_refresh = set()

    for g in all_groups:
        moved = perform_dedupe(g)
        total_moved += sum(item['size'] for item in moved)
        artists_to_refresh.add(g['artist'])
        with lock:
            state["dedupe_progress"] += 1

    for artist in artists_to_refresh:
        letter = quote_plus(artist[0].upper())
        art_enc = quote_plus(artist)
        plex_api(f"/library/sections/{SECTION_ID}/refresh?path=/music/matched/{letter}/{art_enc}", method="GET")
        plex_api(f"/library/sections/{SECTION_ID}/emptyTrash", method="PUT")

    with lock:
        state["deduping"] = False
        state["space_saved"] += total_moved
        save_stats()

# ───────────────────────────────── HTML TEMPLATE ─────────────────────────────────
HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<title>Plex Music Duplicates</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
<style>
body { font-family:Inter,Arial,sans-serif; background:#f5f7fa; margin:0; padding:2rem; }
h1 { font-weight:600; margin-bottom:1rem; }
button { cursor:pointer; border:none; border-radius:8px; padding:.5rem 1rem; font-weight:600; }
#all { background:#e63946; color:#fff; margin-right:1rem; }
#deleteSel { background:#d90429; color:#fff; margin-right:1rem; }
#modeswitch { background:#1d3557; color:#fff; }
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
#saved { position:fixed; top:1rem; right:1rem; background:#006f5f; color:#fff;
          padding:.5rem .9rem; border-radius:8px; font-size:.9rem; }
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
</head><body>

<div id="saved">Space saved: {{ space }} MB</div>

<h1>Plex Music Duplicates</h1>

<div style="display:flex; align-items:center; margin-bottom:1rem;">
  {% if not scanning and not groups %}
    <button id="start" onclick="startScan()" style="background:#006f5f;color:#fff;">Start Scan</button>
  {% endif %}
  {% if groups %}
    <button id="deleteSel" onclick="submitSelected()">Delete Selected Dupes</button>
    <button id="all" onclick="submitAll()">Deduplicate ALL</button>
  {% endif %}
  {% if groups %}
    <button id="modeswitch" onclick="toggleMode()" style="margin-left:auto;">Switch to Table View</button>
  {% endif %}
</div>

<div id="scanBox" class="progress"><div id="scanBar" class="bar" style="width:0%"></div></div>
<div id="scanTxt">0 / 0 albums</div>

{% if groups %}
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
                       dedupeSingle('{{ g.artist_key }}', {{ g.album_id }}, '{{ g.best_title|replace(\"'\",\"\\'\") }}')">
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
          <td>{{ g.formats|join(", ") }}</td>
          <td>
            <button class="row-dedup-btn"
                    onclick="event.stopPropagation();
                             dedupeSingle('{{ g.artist_key }}', {{ g.album_id }}, '{{ g.best_title|replace(\"'\",\"\\'\") }}')">
              Deduplicate
            </button>
          </td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
{% endif %}


<!-- ==== Modal for Edition Details & Confirmations ==== -->
<div id="modal" class="modal">
  <div class="modal-content">
    <span class="close" onclick="closeModal()">&times;</span>
    <div id="modalBody"></div>
  </div>
</div>

<script>
let scanTimer = null, dedupeTimer = null, inTableMode = false;

// Toggle Grid/Table View
function toggleMode() {
  const gridEl = document.getElementById("gridMode");
  const tableEl = document.getElementById("tableMode");
  if (gridEl)  gridEl.style.display  = inTableMode ? "grid"  : "none";
  if (tableEl) tableEl.style.display = inTableMode ? "none"  : "block";
  inTableMode = !inTableMode;
  const switchBtn = document.getElementById("modeswitch");
  if (switchBtn) {
    switchBtn.innerText = inTableMode ? "Switch to Grid View" : "Switch to Table View";
  }
}

// Poll scan progress
function pollScan() {
  fetch("/api/progress")
    .then(r => r.json())
    .then(j => {
      if (j.scanning) {
        const scanBox = document.getElementById("scanBox");
        if (scanBox) scanBox.style.display = "block";
        const scanBar = document.getElementById("scanBar");
        if (scanBar) {
          const pct = j.total ? Math.round(100 * j.progress / j.total) : 0;
          scanBar.style.width = pct + "%";
        }
        const scanTxt = document.getElementById("scanTxt");
        if (scanTxt) scanTxt.innerText = `${j.progress} / ${j.total} albums`;
      } else {
        clearInterval(scanTimer);
        location.reload();  // reload to show duplicates
      }
    });
}

// Start scanning
function startScan() {
  fetch("/start", { method: "POST" })
    .then(() => {
      const scanBox = document.getElementById("scanBox");
      if (scanBox) scanBox.style.display = "block";
      scanTimer = setInterval(pollScan, 1000);
    });
}

// Delete ALL dupes
function submitAll() {
  fetch("/dedupe/all", { method: "POST" }).then(() => {
    showLoadingModal("Moving all duplicates…");
    dedupeTimer = setInterval(pollDedupe, 1000);
  });
}

// Poll dedupe-all progress
function pollDedupe() {
  fetch("/api/dedupe")
    .then(r => r.json())
    .then(j => {
      if (!j.deduping) {
        clearInterval(dedupeTimer);
        showSimpleModal(`Moved ${j.saved} MB in total`);
      }
    });
}

// Delete SELECTED dupes
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
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ selected: checked })
  })
  .then(r => r.json())
  .then(resp => {
    showConfirmation(resp.moved);
  })
  .catch(() => {
    closeModal();
    showSimpleModal("An error occurred during deduplication.");
  });
}

// Deduplicate a single group (called from grid or table “Deduplicate” button)
function dedupeSingle(artist, albumId, title) {
  showLoadingModal(`Moving duplicate for ${artist.replace(/_/g, " ")} – ${title}`);
  fetch(`/dedupe/artist/${artist}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ album_id: albumId })
  })
  .then(r => r.json())
  .then(resp => {
    showConfirmation(resp.moved);
  })
  .catch(() => {
    closeModal();
    showSimpleModal("An error occurred during single deduplication.");
  });
}

// Display a “Loading…” modal with custom text
function showLoadingModal(text) {
  const html = `<div id="loadingSpinner">${text}</div>`;
  const modalBody = document.getElementById("modalBody");
  if (modalBody) modalBody.innerHTML = html;
  const modal = document.getElementById("modal");
  if (modal) modal.style.display = "flex";
}

// Display the “Moved Duplicates” confirmation, then auto-close after 5 seconds
function showConfirmation(moved) {
  let html = `<h3>Moved Duplicates</h3>`;
  html += `<div class="ed-container">`;
  moved.forEach(e => {
    html += `<div class="edition">`;
    if (e.thumb_data) {
      html += `<img src="${e.thumb_data}" alt="cover">`;
    } else {
      html += `<img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAYAAACqaXHeAAAAQklEQVR42u3BAQ0AAADCIPunNscwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD8wDeQAAEmTWlUAAAAASUVORK5CYII=" alt="no-cover">`;
    }
    html += `<div><b>Duplicate</b>&nbsp;${e.artist}&nbsp;${e.title_raw}&nbsp;${e.size} MB&nbsp;${e.fmt}&nbsp;Moved to dupes folder</div>`;
    html += `</div>`;
  });
  html += `</div>`;
  const modalBody = document.getElementById("modalBody");
  if (modalBody) modalBody.innerHTML = html;
  const modal = document.getElementById("modal");
  if (modal) modal.style.display = "flex";
  setTimeout(() => {
    closeModal();
    location.reload();
  }, 5000);
}

// Open the “details” modal when clicking on a grid‐card or table‐row
function openModal(artist, albumId) {
  showLoadingModal("Loading album details…");
  fetch(`/details/${artist}/${albumId}`)
    .then(r => {
      if (!r.ok) throw new Error("404");
      return r.json();
    })
    .then(j => {
      let html = `<h3>${j.artist} – ${j.album}</h3>`;
      html += `<div class="ed-container">`;
      j.editions.forEach((e, i) => {
        html += `<div class="edition">`;
        if (e.thumb_data) {
          html += `<img src="${e.thumb_data}" alt="cover">`;
        } else {
          html += `<img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAYAAACqaXHeAAAAQklEQVR42u3BAQ0AAADCIPunNscwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD8wDeQAAEmTWlUAAAAASUVORK5CYII=" alt="no-cover">`;
        }
        html += `<div><b>${i === 0 ? "Best" : "Duplicate"}</b></div>`;
        html += `<div>${j.artist}</div>`;
        html += `<div>${j.album}</div>`;
        html += `<div>${e.size} MB</div>`;
        html += `<div>${e.fmt}</div>`;
        html += `</div>`;
      });
      html += `</div>`;
      html += `<button id="modalDedup" style="
                   background:#006f5f; color:#fff; border:none;
                   border-radius:8px; padding:.4rem .9rem; cursor:pointer; margin-top:1rem;">
                 Deduplicate
               </button>`;
      const modalBody = document.getElementById("modalBody");
      if (modalBody) modalBody.innerHTML = html;
      const modal = document.getElementById("modal");
      if (modal) modal.style.display = "flex";

      const btn = document.getElementById("modalDedup");
      if (btn) {
        btn.onclick = () => {
          showLoadingModal(`Moving duplicate for ${j.artist} – ${j.album}`);
          fetch(`/dedupe/artist/${artist}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ album_id: albumId })
          })
          .then(r => r.json())
          .then(resp => {
            showConfirmation(resp.moved);
          })
          .catch(() => {
            closeModal();
            showSimpleModal("An error occurred during modal deduplication.");
          });
        };
      }
    })
    .catch(() => {
      closeModal();
      showSimpleModal("Could not load album details.");
    });
}

// Close the modal (used by the “×” in the corner)
function closeModal() {
  const modal = document.getElementById("modal");
  if (modal) modal.style.display = "none";
}

// Show a small auto‐hiding modal with a simple message
function showSimpleModal(msg) {
  const html = `<h3>${msg}</h3>`;
  const modalBody = document.getElementById("modalBody");
  if (modalBody) modalBody.innerHTML = html;
  const modal = document.getElementById("modal");
  if (modal) modal.style.display = "flex";
  setTimeout(() => closeModal(), 3000);
}

document.addEventListener("DOMContentLoaded", () => {
  // By default: show grid and hide table (if exist)
  const gridEl = document.getElementById("gridMode");
  const tableEl = document.getElementById("tableMode");
  if (gridEl) gridEl.style.display = "grid";
  if (tableEl) tableEl.style.display = "none";

  // Card click → openModal (Grid mode)
  document.querySelectorAll(".card").forEach(card => {
    card.addEventListener("click", () => {
      const artist  = card.getAttribute("data-artist");
      const albumId = card.getAttribute("data-album-id");
      openModal(artist, albumId);
    });
  });

  // Row click → openModal (Table mode)
  document.querySelectorAll(".table-row").forEach(row => {
    row.addEventListener("click", () => {
      const artist  = row.getAttribute("data-artist");
      const albumId = row.getAttribute("data-album-id");
      openModal(artist, albumId);
    });
  });

  // Prevent checkboxes/buttons from triggering card/row click
  document.querySelectorAll(
    "input[type='checkbox'], .btn-dedup, .row-dedup-btn"
  ).forEach(el => {
    el.addEventListener("click", ev => ev.stopPropagation());
  });

  // Démarrer le polling si on était déjà en scan ou dedupe
  fetch("/api/progress")
    .then(r => r.json())
    .then(j => {
      if (j.scanning) {
        scanTimer = setInterval(pollScan, 1000);
      }
    });

  fetch("/api/dedupe")
    .then(r => r.json())
    .then(j => {
      if (j.deduping) {
        dedupeTimer = setInterval(pollDedupe, 1000);
      }
    });
});
</script>
</body></html>"""

# ───────────────────────────────── FLASK APP ─────────────────────────────────
app = Flask(__name__)

@app.get("/")
def index():
    with lock:
        cards: List[Dict] = []
        for artist, lst in state["duplicates"].items():
            for g in lst:
                best = g['best']
                formats = [get_primary_format(best['folder'])] + [
                    get_primary_format(e['folder']) for e in g['losers']
                ]
                cards.append({
                    "artist_key": artist.replace(" ", "_"),
                    "artist": artist,
                    "album_id": best['album_id'],
                    "n": len(g['losers']) + 1,
                    "best_thumb": thumb_url(best['album_id']),
                    "best_title": best['title_raw'],
                    "best_fmt": formats[0],
                    "formats": formats
                })
        return render_template_string(
            HTML,
            scanning=state["scanning"],
            groups=cards,
            space=state["space_saved"]
        )

@app.post("/start")
def start():
    if not state["scanning"]:
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
            saved=state["space_saved"]
        )

@app.get("/details/<artist>/<int:album_id>")
def details(artist, album_id):
    art = artist.replace("_", " ")
    with lock:
        grps = state["duplicates"].get(art, [])
    for g in grps:
        if g['album_id'] == album_id:
            editions = [g['best']] + g['losers']
            out = []
            for e in editions:
                thumb_data = fetch_cover_as_base64(e['album_id'])
                out.append({
                    "thumb_data": thumb_data,
                    "title_raw": e['title_raw'],
                    "size": folder_size(e['folder']) // (1024 * 1024),
                    "fmt": get_primary_format(e['folder'])
                })
            return jsonify(
                artist=art,
                album=g['best']['title_raw'],
                editions=out
            )
    return jsonify({}), 404

@app.post("/dedupe/artist/<artist>")
def dedupe_artist(artist):
    art = artist.replace("_", " ")
    data = request.get_json() or {}
    album_id = data.get("album_id")
    moved_list: List[Dict] = []
    with lock:
        grps = state["duplicates"].get(art, [])
        for g in grps:
            if g['album_id'] == album_id:
                moved_list = perform_dedupe(g)
                state["duplicates"][art].remove(g)
                if not state["duplicates"][art]:
                    del state["duplicates"][art]
                break
    letter  = quote_plus(art[0].upper())
    art_enc = quote_plus(art)
    plex_api(f"/library/sections/{SECTION_ID}/refresh?path=/music/matched/{letter}/{art_enc}", method="GET")
    plex_api(f"/library/sections/{SECTION_ID}/emptyTrash", method="PUT")
    total_mb = sum(item['size'] for item in moved_list)
    with lock:
        state["space_saved"] += total_mb
        save_stats()
    return jsonify(moved=moved_list), 200

@app.post("/dedupe/all")
def dedupe_all():
    with lock:
        all_groups = [g for lst in state["duplicates"].values() for g in lst]
        state["duplicates"].clear()
    threading.Thread(target=background_dedupe, args=(all_groups,), daemon=True).start()
    return "", 204

@app.post("/dedupe/selected")
def dedupe_selected():
    data = request.get_json() or {}
    selected = data.get("selected", [])
    moved_list: List[Dict] = []
    total_moved = 0
    artists_to_refresh = set()
    for sel in selected:
        art_key, aid_str = sel.split("||", 1)
        art = art_key.replace("_", " ")
        album_id = int(aid_str)
        with lock:
            grps = state["duplicates"].get(art, [])
            for g in grps:
                if g['album_id'] == album_id:
                    moved = perform_dedupe(g)
                    moved_list.extend(moved)
                    total_moved += sum(item['size'] for item in moved)
                    artists_to_refresh.add(art)
                    state["duplicates"][art].remove(g)
                    if not state["duplicates"][art]:
                        del state["duplicates"][art]
                    break
    for art in artists_to_refresh:
        letter  = quote_plus(art[0].upper())
        art_enc = quote_plus(art)
        plex_api(f"/library/sections/{SECTION_ID}/refresh?path=/music/matched/{letter}/{art_enc}", method="GET")
        plex_api(f"/library/sections/{SECTION_ID}/emptyTrash", method="PUT")
    with lock:
        state["space_saved"] += total_moved
        save_stats()
    return jsonify(moved=moved_list), 200

# ───────────────────────────────────── CLI MODE ───────────────────────────────────
def dedupe_cli(dry: bool, safe: bool, tag_extra: bool, verbose: bool):
    """
    Command-line mode:
    - Scans all artists/albums in Plex.
    - Detects duplicate album groups.
    - For each group: moves loser folders (or simulates) and deletes metadata in Plex.
    """
    # 1) Configure logging level (DEBUG if verbose, otherwise INFO)
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.getLogger().setLevel(log_level)

    # 2) Open SQLite connection to Plex database
    db = sqlite3.connect(PLEX_DB_FILE)
    cur = db.cursor()

    # 3) Initialize cumulative statistics
    stats = {
        'total_artists': 0,
        'total_albums': 0,
        'albums_with_dupes': 0,
        'total_dupes': 0,
        'total_moved_mb': 0
    }

    # 4) Fetch all artists in the Music library section
    artists = cur.execute(
        "SELECT id, title FROM metadata_items WHERE metadata_type=8 AND library_section_id=?",
        (SECTION_ID,)
    ).fetchall()

    # 5) Loop over each artist
    for artist_id, artist_name in artists:
        stats['total_artists'] += 1

        # 5a) Fetch all album IDs under this artist
        album_rows = cur.execute(
            "SELECT id FROM metadata_items WHERE metadata_type=9 AND parent_id=?",
            (artist_id,)
        ).fetchall()
        album_ids = [row[0] for row in album_rows]
        stats['total_albums'] += len(album_ids)

        # 5b) Detect duplicate groups for this artist
        dup_groups = scan_duplicates(db, artist_name, album_ids)
        if not dup_groups:
            continue

        stats['albums_with_dupes'] += len(dup_groups)

        # 6) For each duplicate group:
        for group in dup_groups:
            best = group['best']
            losers = group['losers']

            # Print a separation line before each group
            logging.info("-" * 60)

            # 6a) Header: “Detected duplicate group for: Artist | Album”
            logging.info(f"Detected duplicate group for: {artist_name} | {best['title_raw']}")

            # 6b) “Best version    | Artist | Album | Size (MB) | Format”
            best_size_mb = folder_size(best['folder']) // (1024 * 1024)
            best_format = get_primary_format(best['folder'])
            logging.info(
                f"Best version    | {artist_name} | {best['title_raw']} | "
                f"{best_size_mb} MB | {best_format}"
            )

            # 6c) Log each loser “Would have moved” (if dry) or “Moved”
            group_moved_mb = 0
            for loser in losers:
                loser_folder = loser['folder']
                loser_id = loser['album_id']
                loser_title = loser['title_raw']

                # 6c.1) If source folder is missing, skip this loser
                if not loser_folder.exists():
                    logging.warning(
                        f"Source folder not found, skipping: {loser_folder} "
                        f"(rk={loser_id})"
                    )
                    continue

                # Destination path under DUPE_ROOT
                dst = DUPE_ROOT / loser_folder.relative_to(next(iter(PATH_MAP.values())))
                dst.parent.mkdir(parents=True, exist_ok=True)

                # Compute size and format
                loser_size_mb = folder_size(loser_folder) // (1024 * 1024)
                loser_format = get_primary_format(loser_folder)
                group_moved_mb += loser_size_mb
                stats['total_moved_mb'] += loser_size_mb
                stats['total_dupes'] += 1

                if dry:
                    # Dry run: indicate “Would have moved”
                    logging.info(
                        f"Would have moved | {artist_name} | {loser_title} | "
                        f"{loser_size_mb} MB | {loser_format} | (DRY-RUN)"
                    )
                else:
                    # Actually move the folder
                    logging.info(
                        f"Moved            | {artist_name} | {loser_title} | "
                        f"{loser_size_mb} MB | {loser_format}"
                    )
                    try:
                        shutil.move(str(loser_folder), str(dst))
                    except FileNotFoundError:
                        # If it disappears between exists() check and move(), log and continue
                        logging.warning(
                            f"Failed to move, folder disappeared: {loser_folder}"
                        )
                        continue

                # Delete metadata via Plex API (unless dry or safe)
                if not dry and not safe:
                    logging.info(f"    Deleting metadata in Plex for rk={loser_id}")
                    plex_api(f"/library/metadata/{loser_id}/trash", method="PUT")
                    time.sleep(0.3)
                    plex_api(f"/library/metadata/{loser_id}", method="DELETE")
                else:
                    logging.info(f"    [SKIPPED Plex delete for rk={loser_id}]")

            # 6d) Summary line for this group
            logging.info(f"Total moved for this group: {group_moved_mb} MB")
            # (Next iteration will start with its own separator line.)

            # 6e) Optionally tag “Extra Tracks” on the best edition
            if tag_extra:
                max_tracks = max(len(ed['tracks']) for ed in losers + [best])
                min_tracks = min(len(ed['tracks']) for ed in losers + [best])
                if len(best['tracks']) > min_tracks:
                    logging.info(f"    Tagging 'Extra Tracks' on best rk={best['album_id']}")
                    plex_api(
                        f"/library/metadata/{best['album_id']}?title.value=(Extra Tracks)&title.lock=1",
                        method="PUT"
                    )

        # 7) Refresh Plex for this artist so changes become visible immediately
        prefix = f"/music/matched/{quote_plus(artist_name[0].upper())}/{quote_plus(artist_name)}"
        plex_api(f"/library/sections/{SECTION_ID}/refresh?path={prefix}")
        plex_api(f"/library/sections/{SECTION_ID}/emptyTrash", method="PUT")

    # 8) Final summary after processing all artists
    logging.info("-" * 60)
    logging.info("FINAL SUMMARY")
    logging.info(f"Total artists processed      : {stats['total_artists']}")
    logging.info(f"Total albums processed       : {stats['total_albums']}")
    logging.info(f"Albums with duplicates       : {stats['albums_with_dupes']}")
    logging.info(f"Duplicate editions moved     : {stats['total_dupes']}")
    logging.info(f"Total space moved (MB)       : {stats['total_moved_mb']} MB")
    logging.info("-" * 60)

# ───────────────────────────────── MAIN ───────────────────────────────────
if __name__ == "__main__":
    load_stats()

    parser = argparse.ArgumentParser(
        description="Scan & dedupe Plex Music duplicates (CLI or WebUI)."
    )
    sub = parser.add_argument_group("Options for WebUI or CLI modes")
    sub.add_argument(
        "--serve", action="store_true", help="Launch Flask web interface"
    )
    sub.add_argument(
        "--port", type=int, default=5000, help="Port for WebUI (default: 5000)"
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
        "--verbose", action="store_true", help="Enable DEBUG-logging in CLI mode."
    )

    args = parser.parse_args()

if args.serve:
    # Mode WebUI — read port from config.json (fallback to CLI if missing)
    app.run(host="0.0.0.0", port=int(conf.get("WEBUI_PORT", args.port)))
else:
    # Mode CLI
    dedupe_cli(
        dry=args.dry_run,
        safe=args.safe_mode,
        tag_extra=args.tag_extra,
        verbose=args.verbose
    )
