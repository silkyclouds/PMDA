# PMDA – Architecture & Technical Summary

This document describes what PMDA does and how it works internally. It is intended for contributors and advanced users who want to understand the codebase.

---

## What PMDA Does (Summary)

- **Scans** one or more Plex Music libraries (artist/album structure).
- **Auto-discovers** library sections, paths, and DB structure from Plex.
- **Maps** Plex library paths to host/container paths via `PATH_MAP` (auto-generated from Plex or user-defined).
- **Detects duplicate albums** using normalized album titles, track overlap, and optional MusicBrainz/OpenAI logic.
- **Selects the “best” edition** per duplicate group: via OpenAI (recommended) or a local heuristic (format score, bit depth, track count, bitrate).
- **Moves** loser editions to a configurable `DUPE_ROOT` folder and **removes** their metadata from Plex (trash + delete).
- **Supports** dry-run, safe mode (no Plex metadata deletion), Web UI (Flask), and CLI modes.
- **Optionally** uses MusicBrainz for release-group info and Box Set handling; **optionally** notifies Discord.
- **Caches** FFmpeg audio info and AI choices in SQLite for speed.

---

## High-Level Flow

1. **Startup**  
   Load `config.json` and env vars, validate Plex connection, auto-generate/merge `PATH_MAP` from Plex, optionally run self-diagnostic. Initialize state DB and cache DB.

2. **Scan**  
   For each artist in the configured section(s), fetch album IDs from the Plex DB, resolve each album’s folder via `PATH_MAP`, gather tracks and audio metadata (FFmpeg/cache). Group albums by normalized title (and classical disambiguation when applicable). For groups with 2+ editions, run “best edition” selection (AI or heuristic), then persist groups to the state DB.

3. **Dedupe**  
   For each selected group (single, selected, or all): move loser folders to `DUPE_ROOT`, call Plex API to trash and delete metadata, refresh library path and empty trash. Update stats (space saved, removed dupes).

4. **Web UI**  
   Serves a single-page app (HTML template in code). Endpoints: scan start/pause/resume/stop, progress, list of duplicate groups (cards), details per group, dedupe (by artist/album, selected, or all), merge-and-dedupe.

5. **CLI**  
   Runs a full scan in the main process, then runs dedupe in CLI mode (with dry-run/safe-mode/tag-extra/verbose options). Can be driven by `PMDA_DEFAULT_MODE=cli` (or `run`) without `--serve`.

---

## Main Components

| Component | Role |
|----------|------|
| **Config loading** | `config.json` + env; `_get()`, `_parse_path_map()`, `_discover_path_map()`; validation of `PLEX_*`, `SECTION_ID`/`SECTION_IDS`. |
| **Plex DB** | SQLite read via `plex_connect()`; queries for artists, albums, tracks, media parts and paths. |
| **PATH_MAP** | Map from Plex path (e.g. `/music/matched`) to host path; used by `container_to_host()`, `relative_path_under_known_roots()`, `build_dupe_destination()`. |
| **Scan** | `scan_artist_duplicates()` (per-artist worker), `scan_duplicates()` (per-artist album grouping), `choose_best()` (AI or heuristic), `background_scan()`, `save_scan_to_db()` / `load_scan_from_db()`. |
| **Dedupe** | `perform_dedupe()` (move folders + Plex trash/delete), `safe_move()` (cross-device move with retries), `background_dedupe()`. |
| **State DB** | SQLite: `duplicates_best`, `duplicates_loser`, `stats`; path `STATE_DB_FILE` (in config dir). |
| **Cache DB** | SQLite: `audio_cache` (path, mtime, bit_rate, sample_rate, bit_depth), MusicBrainz cache; path `CACHE_DB_FILE`. |
| **OpenAI** | `choose_best()` sends edition summary + optional MusicBrainz info; prompt from `ai_prompt.txt`; parses single line `index|rationale|extra_tracks`. |
| **Flask** | `/`, `/scan/*`, `/api/progress`, `/api/dedupe`, `/details/...`, `/dedupe/artist/...`, `/dedupe/all`, `/dedupe/selected`, `/api/edition_details`, `/api/dedupe_manual`. |

---

## Key Conventions

- **English in code**  
  Comments, identifiers, commit messages, and user-facing docs intended for the wiki should be in English. Localized user docs can be provided in separate files (e.g. French).

- **Config priority**  
  Env var overrides `config.json`; after startup, `PATH_MAP` is merged from Plex discovery + user `PATH_MAP` and written back to `config.json`.

- **Modes**  
  `--serve` → Web UI; otherwise CLI. If no flag is given, `PMDA_DEFAULT_MODE` must be set (`serve`, `cli`, or `run`).

- **Cross-library**  
  `CROSS_LIBRARY_DEDUPE` (env, default true) controls whether duplicates are detected across all configured section IDs or per-section.

---

## File Layout (Reference)

- `pmda.py` – Single main script: config, DBs, scan/dedupe logic, Flask app and HTML template.
- `config.json` – Default/template config; runtime copy in `PMDA_CONFIG_DIR`.
- `ai_prompt.txt` – Default AI prompt; runtime copy in `PMDA_CONFIG_DIR`.
- `requirements.txt` – Flask, requests, openai, musicbrainzngs.
- `static/` – Logo and assets for the Web UI.
- `docs/` – Markdown for wiki (architecture, user guide, configuration).
