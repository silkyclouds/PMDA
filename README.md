


# PMDA
**Plex Music Deduplication Assistant** — Find duplicate albums, fix tags, and manage your Plex music library from a single web UI.

---

## Quick Start (Docker)

The recommended way to run PMDA is via Docker. You need four volume mounts: config, Plex database, music, and a dupes output folder.

```bash
docker run -d \
  --name PMDA_WEBUI \
  --restart unless-stopped \
  -p 5005:5005 \
  -v /path/to/config:/config \
  -v /path/to/plex/databases:/database:ro \
  -v /path/to/music:/music:rw \
  -v /path/to/dupes:/dupes \
  meaning/pmda:latest
```

Replace the paths with your actual host paths:

| Mount | Purpose |
|-------|---------|
| `/config` | PMDA state, SQLite settings, cache. Persist this. |
| `/database` | Plex DB folder (contains `com.plexapp.plugins.library.db`). Read-only. |
| `/music` | Your music library root. Read-write if you use move/cleanup. |
| `/dupes` | Output folder for moved duplicate editions. |

Open **http://localhost:5005**. On first run, the Settings wizard will guide you (Plex URL, token, library section IDs, path mapping).

---

## Run without Docker

For development or a non-container setup:

```bash
git clone https://github.com/silkyclouds/PMDA.git
cd PMDA
pip install -r requirements.txt
```

Build and serve the web UI:

```bash
cd frontend
npm install
npm run build
cd ..
export PMDA_CONFIG_DIR=/path/to/config   # optional; defaults to current dir
python pmda.py
```

Ensure the Plex database is reachable and your music paths match what Plex uses (see Settings in the UI for path mapping).

---

## What is PMDA?

PMDA connects to your Plex Media Server music library and helps you:

- **Detect duplicate albums** — Same release in multiple editions (e.g. FLAC vs MP3, deluxe vs standard).
- **Deduplicate** — Choose which edition to keep; move others to a dupes folder and optionally clean Plex metadata.
- **Fix tags** — Improve album and artist metadata (optional AI/MusicBrainz).
- **Browse and inspect** — Library browser, missing covers, broken albums, scan history, statistics.

All configuration (Plex URL, token, section IDs, path map, AI keys) is stored in SQLite under `/config`. No env vars required for basic runs.

---

## How it works

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│  Plex Media Server  │     │  PMDA (Docker)      │     │  Web UI             │
│  Database + Music   │ ──▶ │  Backend + Frontend │ ──▶ │  localhost:5005     │
│  (read / read-write)│     │  Web UI only         │     │  Scan, Unduper, etc.│
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
                                      │
                                      │ (optional)
                                      ▼
                             ┌─────────────────────┐
                             │  Dupes folder       │
                             │  Moved editions     │
                             └─────────────────────┘
```

1. PMDA reads the Plex database (artists, albums, tracks) and your music paths.
2. You run a **Scan** to detect duplicates and issues (missing covers, incomplete tags).
3. From the **Unduper** page you select which editions to keep; PMDA moves the rest to `/dupes` and can update Plex.
4. **Library**, **Tag Fixer**, **Statistics**, and **History** give you browse, fix, and analytics from the same UI.

---

## Quick Links

| Resource | Description |
|----------|-------------|
| [Docker Hub](https://hub.docker.com/r/meaning/pmda) | Image `meaning/pmda:latest` and `meaning/pmda:beta` |
| [GitHub](https://github.com/silkyclouds/PMDA) | Source code and issues |

---

## Configuration

After the first run, everything is driven by the Web UI **Settings**:

- **Plex** — Base URL, token, library section IDs.
- **Paths** — Mapping from Plex library roots to container paths (e.g. `/music`). Can be discovered from Plex.
- **Scan / Dedupe** — Threads, cross-library dedupe, MusicBrainz/OpenAI options.
- **Notifications** — Optional webhook for scan completion.

No config files are required on disk; SQLite in `/config` is the source of truth. Optionally, you can still use a `config.json` in `/config` as a fallback.

---

## Requirements

- **Plex Media Server** with at least one music library.
- **Docker** (for the image) or **Python 3.11+** and **Node 20+** (for non-Docker).
- **FFmpeg** and **SQLite** (installed in the Docker image; install locally for non-Docker).

---

## License

This project is open source. See the repository for license details.
