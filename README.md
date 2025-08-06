PMDA – Plex Music Dedupe Assistant
==================================

<p align="center">
  <img src="/static/PMDA.png" alt="PMDA Logo" width="450"/>
</p>

<p align="center">
  <a href="https://discord.gg/2jkwnNhHHR">
    👉 Join us on Discord — let's improve this thing together!
  </a>
</p>

---

PMDA (Plex Music Dedupe Assistant) is a powerful tool to scan your Plex Music library, detect duplicate albums, and remove the lowest-quality versions — automatically or via a beautiful web interface.

Whether you're a FLAC snob or just want fewer copies of the same album floating around, PMDA's got your back.

## 🌟 What PMDA Does

Here's what PMDA currently supports:

- 🔍 **Scans** your entire Plex Music library
- 🧬 **Auto-detects** Plex libraries, paths and DB structure
- 🔍 **Automatically** maps your Plex Music librar(ies) paths with PMDA paths
- 🎯 **Detects duplicate albums** using precise matching and format heuristics
- 🧠 **(Optionally (but recommended) Uses AI** (OpenAI) to pick the best version among dupes with rationale
- 📉 **Calculates bitrate/sample-rate/bit-depth** via FFmpeg
- 🧹 **Trash and delete duplicate entries from Plex libraries** via Plex API
- 🧪 **Supports dry-run / safe mode** if you want to preview effects
- 🖥️ **Modern Web UI** to dedupe one-by-one or all at once (But you want to use it in CLI mode anyway, right?)
- 🧠 **Fully works offline** (if AI is not used)
- ⚙️ **Full `config.json` support** with baked-in defaults, but you rather want to use the variable config, see below...
- 🐳 **Full Docker variable support** – no file edits needed
- 📊 **Stats panel** in UI: space saved, dupes removed, etc.
- 🔄 **Merge extra tracks** from lesser versions
- 💾 **Caches audio info** with SQLite so re-runs are fast
- 📁 **Uses path mapping (PATH_MAP)** to resolve Docker volume mappings
- 🧠 **Cross-library deduplication mode** toggleable via config/env

🧠 AI-Powered Comparison
-------------------------
PMDA uses OpenAI to determine the "best" version of an album — comparing format score, bitrate, depth, number of tracks, and presence of extra tracks. The UI even shows the rationale behind the decision. This is how you want to run it, really... don't be cheap. 

💻 Web UI Highlights
---------------------
- Table view
- Filter by artist or album
- One-click deduplication
- Merge and deduplicate with rationale
- Statistics on space saved, dedupes removed

<p align="center">
<img width="1904" alt="image" src="https://github.com/user-attachments/assets/637848ff-9710-4e9d-993f-9ca7404bd35a" />
</p>


<p align="center"><i>
 Main dashboard showing total artists, albums, removed and remaining duplicates, total space saved, with live updates during scans.
</i></p>

<p align="center">
<img width="562" alt="image" src="https://github.com/user-attachments/assets/c7149b28-d5a8-4cda-88a4-329fb4211d1b" />
</p>

<p align="center"><i>
  Example of the analysis of two versions of an album.
</i></p>

<p align="center">
<img width="554" alt="image" src="https://github.com/user-attachments/assets/cc7d1ce2-8ade-4451-bde6-396e3a1eaaf7" />
</p>

<p align="center"><i>
  Example of an album with extra tracks detected. Enabling a way to merge extra tracks to the winning edition folder. 
</i></p>

⚙️ Configuration
----------------

All config can be controlled either via `config.json` or environment variables (in Docker).

Supported variables:

- `PLEX_DB_PATH` — Directory or full path to Plex DB
- `PLEX_DB_FILE` — Plex DB filename (default: `com.plexapp.plugins.library.db`)
- `PLEX_HOST` — Base URL to Plex (e.g. `http://192.168.3.2:32400`)
- `PLEX_TOKEN` — Plex auth token
- `SECTION_ID` — Section ID for music library
- `PATH_MAP` — Map container paths to host paths (e.g. `"/mnt:/host/path"`)
- `DUPE_ROOT` — Folder to move removed duplicates
- `WEBUI_PORT` — Port for the Web UI (default: 6000)
- `SCAN_THREADS` — Parallelism level for scanning
- `DISABLE_WEBUI` — If true, disables the web interface
- `LOG_LEVEL` — DEBUG / INFO / WARNING etc.
- `OPENAI_API_KEY` — Optional key for smarter selection
- `OPENAI_MODEL` — Model to use (`gpt-4`, `gpt-3.5-turbo`, etc.)
- `STATE_DB_FILE` — Path for state cache (default: `config_dir/state.db`)
- `CACHE_DB_FILE` — Path for FFmpeg audio info cache
- `FORMAT_PREFERENCE` — List of formats ordered by priority
- `PMDA_CONFIG_DIR` — Path to store config, state, and cache files
- `PMDA_DEFAULT_MODE` — Default mode to launch (`serve`, `cli`, etc.)


### 🐳 Docker Run Example (with inline explanations)

```bash
docker run --rm --name pmda \
  -e PLEX_HOST="http://192.168.3.1:32400" \       # The full URL to your Plex server, including port
  -e PLEX_TOKEN="your-real-plex-token" \         # Your Plex token (required for API access)
  -e SECTION_ID="1" \                            # Section ID of your music library (integer)
  -e PLEX_DB_PATH="/database" \                  # Path *inside the container* to the Plex DB mount
  -e PLEX_DB_FILE="com.plexapp.plugins.library.db" \  # Plex database file name (default name)
  -e PMDA_CONFIG_DIR="/app/config" \             # Config directory inside container (bind-mounted for persistence)
  -e PMDA_DEFAULT_MODE="serve" \                   # Mode to launch: 'serve', 'cli', 'dryrun', or 'dedupe'
  -e LOG_LEVEL="INFO" \                          # Logging level: DEBUG, INFO, WARNING, etc.
  -e SCAN_THREADS="8" \                         # Number of threads for faster scanning
  -e OPENAI_MODEL="gpt-4.1-nano" \               # OpenAI model for enhanced matching (optional)
  -e OPENAI_API_KEY="sk-..." \                   # Your OpenAI API key (optional – leave empty to disable)
  -e PATH_MAP='{"\/music\/matched":"\/music\/matched","\/music\/unmatched":"\/music\/unmatched","\/music\/compilations":"\/music\/compilations"}' \  
                                                # JSON mapping of Plex paths to container mounts
  -e DUPE_ROOT="/dupes" \                        # Directory inside container where deduped albums go
  -v "/path/where/you/store/your/config:/app/config:rw" \  # Mount for config & state files
  -v "/path/to/plex/database:/database:ro" \ # Mount of the Plex database folder
  -v "/first/path/of/your/music/lib:/music/matched:rw" \        
  -v "/second/path/of/your/music/lib:/music/unmatched:rw" \        
  -v "/third/path/of/your/music/lib:/music/compilations:rw" \   
  -v "/mnt/user/MURRAY/Music/Music_dupes/Plex_dupes:/dupes:rw" \      
  -p 5005:5005 \                                  # Web UI port (only needed if using 'serve' mode)
  meaning/pmda:latest                             # Docker image name
```

> 🔁 `PMDA_DEFAULT_MODE` options:
>
> - `"serve"` → Web UI mode
> - `"cli"` → Interactive terminal mode
> - `"dryrun"` → Simulate deduplication without changes
> - `"dedupe"` → Run auto-deduplication immediately

🛠 Example config.json 
-----------------------

```json
{
  "PLEX_DB_FILE": "/database/com.plexapp.plugins.library.db",
  "PLEX_HOST": "http://192.168.3.2:32401",
  "PLEX_TOKEN": "YOUR_TOKEN_HERE",
  "SECTION_ID": 1,
  "PATH_MAP": {
    "/music/matched": "/mnt/user/Music"
  },
  "DUPE_ROOT": "/mnt/user/Music/dupes",
  "WEBUI_PORT": 5005,
  "SCAN_THREADS": 8,
  "STATE_DB_FILE": "/config/pmda_state.db",
  "CACHE_DB_FILE": "/config/pmda_cache.db",
  "OPENAI_API_KEY": "sk-...",
  "OPENAI_MODEL": "gpt-4"
}
```

🔗 Join the community
----------------------
Need help? Want to share cool use cases? Feature ideas? Bug reports?

👉 Join us on Discord: https://discord.gg/2jkwnNhHHR
