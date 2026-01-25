# PMDA – Configuration & parameters

This page lists every config key and environment variable PMDA uses, with short guidelines. Values can be set in `config.json` or via environment variables; **env always overrides config**.

---

## Required

| Variable | Description | Example |
|----------|-------------|---------|
| `PLEX_HOST` | Full URL of the Plex server (no trailing slash). | `http://192.168.1.10:32400` |
| `PLEX_TOKEN` | Plex API token. | Your token from Plex settings. |
| `PLEX_DB_PATH` | Directory (or path) where the Plex database file lives. In Docker, use the path **inside the container**. | `/database` or `/path/to/plex/data` |
| Section(s) | `SECTION_ID` (single) or `SECTION_IDS` (comma-separated). If omitted, PMDA auto-detects all music (artist) sections. | `1` or `1,2,3` |

---

## Paths & mapping (bindings without manual config)

The goal is **not to require users to configure PATH_MAP by hand**. At startup:

1. **Discovery** – PMDA queries the Plex API and gets all `<Location>` paths for the music section(s) (e.g. `/music/matched`, `/music/unmatched`).
2. **Merge** – If you provided a broader PATH_MAP in env/config (e.g. `/music` → `/music/Music_matched`), it is applied by prefix; otherwise “Plex path = container path”, so Docker volume mounts must match (e.g. `-v /host/Music_matched:/music/matched`).
3. **Cross-check** – For each binding, PMDA samples tracks from the DB, resolves paths via PATH_MAP, and verifies files exist on disk. If not, it tries to find the correct host root (sibling dirs or rglob) and **updates PATH_MAP + config.json** automatically.

You only need to mount volumes; PMDA aligns with Plex and corrects bindings when needed.

| Variable | Description | Default / notes |
|----------|-------------|------------------|
| `PLEX_DB_FILE` | Plex DB filename. | `com.plexapp.plugins.library.db` (under `PLEX_DB_PATH`) |
| `PATH_MAP` | Optional. Plex prefix → host path; merged with Plex discovery. Use when Plex and host names differ (e.g. `/music` → `/music/Music_dump`). Cross-check can auto-correct. | Discovered from Plex; fallback container = host |
| `DUPE_ROOT` | Folder where “loser” editions are moved. In Docker this is typically `/dupes` with a bind mount. | `/dupes` |
| `PMDA_CONFIG_DIR` | Directory for config copy, state DB, cache DB, logs, and `ai_prompt.txt`. | Script directory (or env) |

---

## Web UI & ports

| Variable | Description | Default |
|----------|-------------|---------|
| `WEBUI_PORT` | Port the Flask app listens on (inside the container when using Docker). | `5005` |
| `DISABLE_WEBUI` | If set to a truthy value, the web interface is disabled. | (not set) |

---

## Scan & performance

| Variable | Description | Default |
|----------|-------------|---------|
| `SCAN_THREADS` | Number of threads (or workers) for scanning. Use `auto` or leave empty for CPU count. | `auto` (CPU count) or `4` |
| `SKIP_FOLDERS` | Comma-separated list of path prefixes; albums whose folder is under one of these are skipped. | Empty |
| `CROSS_LIBRARY_DEDUPE` | If true, duplicate detection runs across all configured sections; if false, per-section only. | `true` |
| `CROSSCHECK_SAMPLES` | Number of sample paths to check when validating bindings (self-diagnostic). | `20` |

---

## OpenAI (optional)

| Variable | Description | Default |
|----------|-------------|---------|
| `OPENAI_API_KEY` | OpenAI API key for “best edition” and merge suggestions. Leave empty to use heuristic only. | Empty |
| `OPENAI_MODEL` | Model name. | `gpt-4` (PMDA may fall back to a working model in the same “ladder”) |
| `OPENAI_MODEL_FALLBACKS` | Comma-separated list of fallback models if the primary is unavailable. | Empty |

---

## MusicBrainz (optional)

| Variable | Description | Default |
|----------|-------------|---------|
| `USE_MUSICBRAINZ` | If true, PMDA uses MusicBrainz for release-group info and Box Set handling. | `false` |

---

## Behaviour & format preference

| Variable | Description | Default |
|----------|-------------|---------|
| `FORMAT_PREFERENCE` | List of audio extensions in order of preference (best first). Used in scoring. | `["dsf","aif","aiff","wav","flac","m4a",...]` |
| `STATE_DB_FILE` | Path to the SQLite state DB (duplicates, stats). | `{PMDA_CONFIG_DIR}/state.db` |
| `CACHE_DB_FILE` | Path to the SQLite cache (FFmpeg audio info, optional MB cache). | `{PMDA_CONFIG_DIR}/cache.db` |
| `PMDA_DEFAULT_MODE` | Default mode when no CLI flag is passed: `serve` (Web UI), `cli` or `run` (CLI scan + dedupe). | Must be set in Docker if no `--serve` / CLI args |

---

## Logging & notifications

| Variable | Description | Default |
|----------|-------------|---------|
| `LOG_LEVEL` | Logging level. | `INFO` |
| `LOG_FILE` | Path to the rotating log file. | `{PMDA_CONFIG_DIR}/pmda.log` |
| `DISCORD_WEBHOOK` | Discord webhook URL for notifications (e.g. purged invalid, fuzzy duplicate found). | Empty |

---

## Guidelines

1. **Docker**  
   Prefer environment variables for secrets and paths; use `-e` and/or an env file. Mount `PMDA_CONFIG_DIR` so state and cache persist.

2. **PATH_MAP**  
   Let PMDA auto-discover from Plex first; then add only the mappings needed to resolve container paths to host paths (e.g. `/music/matched` → `/host/music`).

3. **First run**  
   Use `LOG_LEVEL=DEBUG` and check logs if something fails; then switch back to `INFO`.

4. **Backup**  
   Back up the Plex DB and music folders before broad dedupe runs; `DUPE_ROOT` holds the moved copies until you delete them.

5. **Sections**  
   Use `SECTION_IDS` when you have multiple music libraries and want to limit or order which ones are scanned.

6. **Skip folders**  
   Use `SKIP_FOLDERS` for paths you never want to consider (e.g. archives, “do not touch” folders). Paths are compared after resolution.

---

## Example config.json (minimal)

```json
{
  "PLEX_HOST": "http://192.168.1.10:32400",
  "PLEX_TOKEN": "YOUR_TOKEN",
  "PLEX_DB_PATH": "/database",
  "OPENAI_API_KEY": "",
  "USE_MUSICBRAINZ": false,
  "SKIP_FOLDERS": []
}
```

After first run with Plex available, `PATH_MAP` will be merged and written back into this file. You can then add or tune `PATH_MAP`, `DUPE_ROOT`, `SECTION_ID`/`SECTION_IDS`, etc. as needed.
