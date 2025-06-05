<p align="center">
  <img src="/static/PMDA.png" alt="PMDA Logo" width="450"/>
</p>

<p align="center">
  <a href="https://discord.gg/9eEAvvZW">
    👉 Join us on Discord — let's improve this thing together!
  </a>
</p>


# PMDA – Plex Music Dedupe Assistant

You know that special moment when you search for an album in Plex... and find **five versions** of it? Same title. Different folders. Possibly different formats. Mostly just chaos.

After waiting patiently for Plex to address this (spoiler: they didn’t), I gave up and built **PMDA** (Plex Music Dedupe Assistant...) - a tool that automatically finds and deduplicates those pesky duplicate albums in your Plex Music library.

Whether you're a FLAC connoisseur or just tired of MP3 clutter, PMDA has your back.

---

## ✨ What PMDA Does

- **Scans your entire Plex Music library**
- Detects duplicate albums based on:
  - Normalized title
  - Identical track listing
  - 85%+ overlap in track titles
  - Audio format, bitrate, and samplerate
- Picks the best-quality version (FLAC wins!)
- Moves the losers to a `Plex_dupes/` folder
- Cleans up metadata from Plex via the API
- Offers a fast and clean Web UI to review and manually dedupe albums

---

## 💡 Features

- **Web UI**
  - Visual grid of duplicates
  - Covers, formats, version count
  - “Deduplicate All” or per-album control
  - Shows size savings after cleanup

- **Safe Mode & Dry-Run**
  - `--dry-run`: Simulate every move and delete—log what *would* happen
  - `--safe-mode`: Move files but skip the Plex API calls that delete metadata

- **Stat Tracking**
  - Keeps a persistent “space_saved” tally in `dedupe_stats.json`
  - Displays the total reclaimed space in the top-right of the Web UI

- **Customizable via `config.json`**
  - Easily configure your Plex DB path, Plex token, library section ID, path mappings, and more
  - No more hunting through Python code—everything is in one JSON file

---

## 🚀 Getting Started

### 1. Clone the Repo

```bash
git clone https://github.com/silkyclouds/PMDA.git
cd PMDA
```

You should see:

```
PMDA/
├── PMDA.png
├── config.json
├── pmda.py
├── requirements.txt
├── README.md
└── static/
```

---

### 2. Install Dependencies

We use a minimal set of Python libraries:

```bash
pip install -r requirements.txt
```

The `requirements.txt` might look like:

```
Flask>=2.0
requests>=2.25
```

---

### 3. Configure `config.json`

Update the values for:

- `plex_db_file`: Full path to your `com.plexapp.plugins.library.db` file. On Unraid or Dockerized Plex, this might look like `/mnt/user/appdata/plex/Library/Application Support/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db`.

- `plex_token`: Your Plex token. You can find it [by following these steps](https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/).

- `plex_host`: The full URL to access your Plex server. For example: `http://192.168.1.50:32400`.

- `library_section_id`: The section ID for your music library. You can find it by inspecting the URL when browsing your music in Plex: `.../library/sections/5/...` → your ID is `5`.

- `path_map`: A dictionary mapping internal Plex paths to actual filesystem paths. For example:
  ```json
  {
    "/data/music": "/mnt/user/Music"
  }
  ```

- `dupe_root`: Where the script should move detected duplicates. It must exist and be writable. Example: `/mnt/user/Music/Plex_dupes`.

- `stats_file`: A JSON file where stats about deduplication runs will be stored. Example: `/mnt/user/appdata/plex_dedupe/stats.json`.

- `webui_port`: The port where you want to expose the Web UI. Default is `5005`.

Make sure all these paths exist and are accessible to your Python script!

---

### 4. Launch the Web UI

```bash
python3 pmda.py --serve
```

Then open your browser and go to:

```
http://localhost:5000
```

Or whichever port you set in the config.

---

### 5. Or Use the CLI Mode

```bash
python3 pmda.py --dry-run
```

Other CLI options:

- `--safe-mode`
- `--tag-extra`
- `--verbose`

---

## 🖖 Happy Deduping

Enjoy your streamlined, space-saving Plex music library. And remember: if Plex won’t do it, we will.

