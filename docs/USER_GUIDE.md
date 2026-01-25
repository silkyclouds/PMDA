# PMDA – User Guide

Clear, step-by-step instructions for using PMDA with Plex Music. No coding required.

---

## What is PMDA?

PMDA (Plex Music Dedupe Assistant) scans your Plex Music library, finds duplicate albums (same artist & album, different formats or editions), picks the best version, and moves the rest to a “dupes” folder while cleaning up Plex so you don’t see them anymore.

---

## Before You Start

- A **Plex Media Server** with at least one **Music** library (type “artist”).
- **Plex token**: [How to find your Plex token](https://support.plex.tv/articles/204059436-finding-an-authentication-token-number/).
- **Plex database path**: The folder that contains `com.plexapp.plugins.library.db` (on the same machine or a mounted volume).
- **(Optional)** **OpenAI API key** – for smarter “best edition” choices and merge suggestions.

---

## Running PMDA

### With Docker (recommended)

1. Pull the image:  
   `docker pull meaning/pmda:latest`  
   (or use your own build.)

2. Set the required environment variables and mounts. Minimum:
   - `PLEX_HOST` – e.g. `http://192.168.1.10:32400`
   - `PLEX_TOKEN` – your Plex token
   - `PLEX_DB_PATH` – path **inside the container** to the folder that contains the Plex DB file
   - Mount the Plex DB folder into the container at `PLEX_DB_PATH`
   - Mount your music root(s) so they match what Plex sees (see Configuration)

3. Choose the mode:
   - **Web UI**: `PMDA_DEFAULT_MODE=serve` and expose the Web UI port (e.g. `-p 5005:5005`).
   - **CLI (full scan + dedupe)**: `PMDA_DEFAULT_MODE=cli` (no port needed unless you use UI later).

Example (simplified):

```bash
docker run --rm -it \
  -e PLEX_HOST="http://192.168.1.10:32400" \
  -e PLEX_TOKEN="your-plex-token" \
  -e PLEX_DB_PATH="/database" \
  -e PMDA_CONFIG_DIR="/config" \
  -e PMDA_DEFAULT_MODE="serve" \
  -v "/path/to/plex/database:/database:ro" \
  -v "/path/to/your/config:/config:rw" \
  -v "/path/to/music:/music/matched:rw" \
  -v "/path/to/dupes:/dupes:rw" \
  -p 5005:5005 \
  meaning/pmda:latest
```

Then open `http://localhost:5005` in your browser.

### Without Docker

1. Install Python 3.11+, FFmpeg, and dependencies:  
   `pip install -r requirements.txt`
2. Copy and edit `config.json` (see Configuration).
3. Run:
   - Web UI: `python pmda.py --serve`
   - CLI: set `PMDA_DEFAULT_MODE=cli` and run `python pmda.py`, or use `python pmda.py` with CLI options (e.g. `--dry-run`, `--verbose`).

---

## Web UI – Step by Step

1. **Open the UI**  
   Open the URL and port you exposed (e.g. `http://localhost:5005`).

2. **First scan**  
   Click **New Scan** (or **Resume** if a previous scan was paused). Wait until the progress bar completes. The table/cards will list duplicate groups.

3. **Inspect a group**  
   Click a row or card to open the detail modal: you’ll see each edition (cover, format, bitrate, etc.) and the chosen “best” with rationale.

4. **Deduplicate**  
   - **One group**: use **Deduplicate** on that row/card.
   - **Several**: select groups with the checkboxes, then **Deduplicate Selected**.
   - **Everything**: **Deduplicate ALL** (use with care).

5. **Stats**  
   The top badges show: total artists, total albums, removed dupes, remaining duplicate groups, and space saved (MB).

---

## CLI Mode

- **Full run (scan + dedupe)**:  
  `PMDA_DEFAULT_MODE=cli` and run the container/script with no `--serve`. It will scan all artists, then apply dedupe (unless dry-run).
- **Dry-run (no file moves, no Plex deletes)**:  
  Use `--dry-run` in CLI mode.
- **Safe mode (move files but do not delete Plex metadata)**:  
  Use `--safe-mode`.

---

## After Deduplication

- **Loser folders** are moved under the folder you configured as `DUPE_ROOT` (e.g. `/dupes` in the container), keeping artist/album structure when possible.
- **Plex** will no longer show those albums (metadata is trashed and deleted; library is refreshed).
- You can delete or archive the contents of `DUPE_ROOT` manually once you’re satisfied.

---

## Tips

- **First time**: Use **dry-run** in CLI or run a scan in the Web UI and dedupe only a few groups to confirm behaviour.
- **PATH_MAP**: If you use Docker, ensure your volume mounts match what Plex has as library paths; PMDA can auto-discover paths from Plex and merge them with your `PATH_MAP`.
- **OpenAI**: For best “which edition to keep” decisions (including classical and bonus tracks), set `OPENAI_API_KEY` and optionally `OPENAI_MODEL` in config or env.
- **Backup**: Back up your music library and/or Plex database before running a large “Deduplicate ALL”.

---

## Troubleshooting

| Problem | What to check |
|--------|----------------|
| “No files found” for artists | Volume bindings and `PATH_MAP`: paths Plex sees must be reachable from inside the container at the mapped paths. |
| “Missing required config: PLEX_DB_PATH” | Set `PLEX_DB_PATH` (and mount that path) so the Plex DB file is readable. |
| Scan never finds duplicates | Ensure you have at least two “editions” of the same album (e.g. MP3 and FLAC). Check `SKIP_FOLDERS` and section IDs. |
| Dedupe fails / permission denied | Write access on music folders and on `DUPE_ROOT`; same user/permissions as Plex if needed. |

For more options and variables, see [CONFIGURATION.md](CONFIGURATION.md).
