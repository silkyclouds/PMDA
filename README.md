<p align="center">
  <img src="/static/PMDA.png" alt="PMDA Logo" width="200"/>
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

- `plex_db_file`
- `plex_token`
- `plex_host`
- `library_section_id`
- `path_map`
- `dupe_root`
- `stats_file`
- `webui_port`

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

