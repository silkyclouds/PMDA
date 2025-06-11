<p align="center">
  <img src="/static/PMDA.png" alt="PMDA Logo" width="450"/>
</p>

<p align="center">
  <a href="https://discord.gg/9eEAvvZW">
    👉 Join us on Discord — let's improve this thing together!
  </a>
</p>


PMDA – Plex Music Dedupe Assistant
==================================

You know that special moment when you search for an album in Plex... and find FIVE versions of it? Same title. Different folders. Possibly different formats. Mostly just chaos.

After waiting patiently for Plex to address this (spoiler: they didn’t), I gave up and built PMDA — a tool that automatically finds and deduplicates those pesky duplicate albums in your Plex Music library.

Whether you're a FLAC connoisseur or just tired of MP3 clutter, PMDA has your back.

🌟 What PMDA Does
-----------------

- Scans your entire Plex Music library
- Detects duplicate albums based on:
  - Normalized album titles
  - Identical or overlapping track listings
  - Matching track titles (85%+ similarity)
  - Audio format, bitrate, and sample rate
- Picks the best-quality version (FLAC wins!)
- Moves duplicates to a `Plex_dupes/` folder
- Cleans metadata from Plex via API
- Offers a fast Web UI to review and dedupe



🧠 Now with AI!
---------------

PMDA now (optionally) uses the openai API to detect and explain differences between album versions. This feature costs a few cents to run and can be customized with your own `ai_prompt.txt` to fit your style.

<p align="center">
  <img src="https://github.com/user-attachments/assets/e8691602-e6dc-40ec-a3c1-977b9e4894b9" alt="AI Token Cost" width="400"/>
</p>

<p align="center"><i>
  Example usage of OpenAI with PMDA — this cost ($0.35) reflects several full test scans, identifying ~7000 duplicate album groups within a 150,000 album test library.
</i></p>

<p align="center">
<img width="562" alt="image" src="https://github.com/user-attachments/assets/c7149b28-d5a8-4cda-88a4-329fb4211d1b" />
</p>

<p align="center"><i>
  Example of the analysis of two versions of an album.
</i></p>

💻 Web UI Features
------------------

- Visual grid of duplicates with cover art
- Album format and version info
- Deduplicate one-by-one or all at once
- Shows potential space savings
- Reversible merge logic

<p align="center">
<img width="991" alt="image" src="https://github.com/user-attachments/assets/3e7ee041-0080-42e2-8679-533af99b0671" />
</p>

<p align="center"><i>
 Main dashboard showing total artists, albums, removed and remaining duplicates, total space saved, with live updates during scans.
</i></p>

⚙️ Configuration (config.json)
------------------------------

Here is a sample `config.json` to adjust:

```json
{
  "PLEX_DB_FILE": "/database/com.plexapp.plugins.library.db",
  "PLEX_HOST": "http://192.168.3.2:32401",
  "PLEX_TOKEN": "YOUR_TOKEN_HERE",
  "SECTION_ID": 1,
  "PATH_MAP": {
    "/music/matched": "/music/matched"
  },
  "DUPE_ROOT": "/dupes",
  "WEBUI_PORT": 5005,
  "STATE_DB_FILE": "/app/pmda_state.db",
  "CACHE_DB_FILE": "/app/pmda_cache.db"
}
```

Optional:
- `ai_prompt.txt`: Customize how the AI handles and explains duplicates.

🚀 Run PMDA in Docker
---------------------

```bash
docker run -d \
  --name pmda \
  -p 5005:5005 \
  -v /path/to/config/config.json:/app/config.json:ro \
  -v /path/to/config/ai_prompt.txt:/app/ai_prompt.txt:ro \
  -v "/path/to/plex/Library/Application Support/Plex Media Server/Plug-in Support/Databases":/database:ro \
  -v /path/to/music:/music \
  -v /path/to/dupes:/dupes \
  silkyclouds/pmda:latest
```

It runs in verbose mode by default.

🧪 Run Without Docker (Bare Metal or Virtualenv)
------------------------------------------------

1. Clone the repo:

```bash
git clone https://github.com/silkyclouds/PMDA.git
cd PMDA
```

2. Create a virtualenv and install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
sudo apt install ffmpeg  # for ffprobe
```

3. Adjust `config.json` as shown above.

4. Run the Web UI:

```bash
python3 pmda.py --serve --verbose
```

5. Or run CLI mode:

```bash
python3 pmda.py --dry-run
```

🛠 CLI Options
--------------

- `--dry-run`: Simulate actions
- `--safe-mode`: Skip Plex API deletions
- `--tag-extra`: Keep non-standard album metadata
- `--verbose`: Show detailed logging
- `--serve`: Run Web UI

## 🛣️ Roadmap

Here are the next features planned for PMDA:

1. **Rollback System for Deduped Albums**  
   Add a second page listing all previously moved albums, with cover previews and metadata, allowing users to:
   - Restore individual albums
   - Selectively rollback multiple albums
   - Revert all deduplicated albums in one click

2. **"Re-download This Album" Flagging**  
   Detect albums with:
   - Missing tracks
   - MP3 or mixed FLAC/MP3 content  
   ...and mark them for replacement with clean, lossless versions.

3. **Local LLM Support (OpenLLaMA)**  
   Allow PMDA to run LLM-based logic locally using [OpenLLaMA](https://github.com/openlm-research/open_llama), for environments without internet or for cost-saving purposes.

4. **OpenAI Usage Tracking**  
   Display a badge in the main dashboard showing the current OpenAI API usage cost, if used.

🫶 Thanks & Community
---------------------

Have a bug, suggestion, or idea? Want to add features and help developing PMDA ?  Join the Discord:
👉 https://discord.gg/9eEAvvZW
