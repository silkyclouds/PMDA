# Lovable – Prompt: PMDA Web UI (React, full rewrite)

Use this prompt in Lovable to build a **complete React replacement** for the PMDA (Plex Music Dedupe Assistant) Web UI. The app talks to an existing PMDA backend API (Flask); you are building the frontend only unless stated otherwise.

---

## 1. Product summary

PMDA scans Plex Music libraries, detects **duplicate albums** (same album in multiple editions: FLAC, MP3, different rips), picks a **“best”** version (via heuristics or OpenAI), and lets the user **move duplicates** to a “dupes” folder. The UI must:

- Show **stats** (artists, albums, remaining duplicate groups, removed dupes, space saved).
- Let the user **start / pause / resume / stop** a library scan and see **progress** (e.g. “10312 / 232923 albums” with a progress bar).
- Display duplicate groups as **cards** (grid) or **table** (list), with toggle.
- Let the user **search** by artist or album.
- Open a **detail modal** for one duplicate group: show **Best** vs **Duplicate(s)** with cover, format, bitrate, sample rate, bit depth, **and the full folder path** for each edition (so we can confirm files exist on disk).
- Offer actions: **Deduplicate** (single group), **Deduplicate selected**, **Deduplicate ALL**, **Merge and Deduplicate ALL** (when extra tracks exist).
- Provide a **Settings / Configuration wizard** that covers **every** PMDA option (Plex, paths, OpenAI, scan, notifications, etc.) with validation and connection checks.

Visual style must be **polished and modern**: clear hierarchy, subtle animations and transitions, responsive layout, optional list view with more detail. Prefer a distinctive (non-generic “AI”) look: consistent spacing, readable typography, and a cohesive color system (e.g. teal/dark accents for primary actions).

---

## 2. Backend API (existing PMDA Flask app)

Base URL: configurable (e.g. `http://localhost:5005` or env). All endpoints are relative to that base.

### 2.1 Read

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Not used by React; React app is SPA. |
| GET | `/api/duplicates` | Returns a **JSON array** of duplicate-group cards. Each card: `{ artist_key, artist, album_id, best_thumb, best_title, best_fmt, formats, n, used_ai }`. `n` = number of versions (best + losers). `best_thumb` = URL to cover image. `used_ai` = boolean (LLM used or not). |
| GET | `/api/progress` | Scan progress: `{ scanning, progress, total, status }`. `status` = `"running"` \| `"paused"` \| `"stopped"`. |
| GET | `/api/dedupe` | Dedupe run status: `{ deduping, progress, total, saved }`. |
| GET | `/details/<artist>/<album_id>` | Artist is URL-safe (e.g. `Pharoahe_Monch`). Returns `{ artist, album, editions, rationale, merge_list }`. **Each edition must include a `path` (or `folder`) field** with the full filesystem path of that album folder so the UI can display it (backend may need a small change to add this). Edition shape: `{ thumb_data, title_raw, size, fmt, br, sr, bd, path? }`. `rationale` = string (often semicolon-separated bullets). `merge_list` = array of extra track names to merge. |

**Important:** If the backend does not yet return `path` or `folder` per edition in `/details/...`, the prompt recipient should add it (each edition has a `folder` on the server). The React UI must display this path clearly (e.g. monospace, copyable) so users can verify the location on disk.

### 2.2 Actions

| Method | Endpoint | Body | Description |
|--------|----------|------|-------------|
| POST | `/scan/start` | - | Start a new scan. |
| POST | `/scan/pause` | - | Pause scan. |
| POST | `/scan/resume` | - | Resume scan. |
| POST | `/scan/stop` | - | Stop scan. |
| POST | `/dedupe/artist/<artist>` | `{ album_id }` | Deduplicate one group. Returns `{ moved: [...] }` (list of moved items with thumb_data, artist, title_raw, size, fmt, br, sr, bd). |
| POST | `/dedupe/selected` | `{ selected: ["artist_key||album_id", ...] }` | Deduplicate selected groups. Returns `{ moved: [...] }`. |
| POST | `/dedupe/all` | - | Deduplicate all remaining groups. Returns `{ moved: [...] }`. |

The backend may also expose **configuration** (read/write) and **connection checks** (Plex, OpenAI). If not, the React app can still provide a full settings UI that persists to `config.json` or env via a small backend extension (see Settings section below).

---

## 3. Pages and layout

### 3.1 Main dashboard (default view)

- **Header:** Logo (PMDA) and tagline (e.g. “It’s fun to undupe with PMDA”), then a **stats bar**: Artists, Albums, Removed dupes, **Remaining Dupes**, Space saved (MB). Use concise labels and clear numbers; update from initial load and when scan/dedupe progress or API duplicates change.
- **Actions row:** Buttons: “Deduplicate Selected Groups”, “Deduplicate ALL”, “Merge and Deduplicate ALL”. Search input: “Search artist or album…” – filter the current duplicate list client-side (by artist or album title).
- **Scan block:** Progress text (e.g. “10312 / 232923 albums”), a **progress bar** (fill based on progress/total), and controls: **New Scan** / **Resume** / **Pause** (depending on status). Show “Status: running | paused | stopped”. Poll `/api/progress` regularly while scanning (e.g. every 1–2 s); when not scanning, poll less often or stop.
- **View toggle:** Switch between **Grid** and **Table** (list) view; persist preference (e.g. localStorage).
- **Grid view:** Cards in a responsive grid. Each card: checkbox, cover image, artist, album title, “versions N”, format tag(s), **Deduplicate** button. Clicking the card (or a “Details” entry) opens the detail modal.
- **Table view:** Rows with columns: checkbox, cover, Artist, Album, # Versions, “LLM” or “Signature Match”, formats, Deduplicate button. Same modal on row click.
- **Pagination:** If the list is large, paginate (e.g. 100 per page) with First / Previous / page numbers / Next / Last. Keep “Remaining Dupes” as the total count, not just current page.
- **Empty state:** When there are 0 duplicate groups, show a short message and a clear “New Scan” CTA.

Use **React state** for duplicates list, scan progress, dedupe status, and selected IDs. Add **skeleton loaders** or subtle **loading** states when fetching duplicates or details. Use **transitions** (e.g. list enter/leave, modal open/close) and light **animations** (e.g. progress bar fill, button hover) to keep the UI lively but not distracting.

### 3.2 Detail modal (duplicate group)

- **Title:** “Artist – Album” (e.g. “Passarani – Shuffling The Cards Again”).
- **Rationale:** If present, show the “Winner has: …” text as a short bullet list or formatted block (e.g. “\[CLASSICAL:NO\] higher bitrate than 0”).
- **Detected extra tracks:** If `merge_list.length > 0`, show a list of track names; show “Merge Tracks” and “Merge and Deduplicate” buttons in addition to “Deduplicate”.
- **Editions:** For each edition, show:
  - Cover (or placeholder).
  - Label: **Best** (first) or **Duplicate**.
  - Artist, album title.
  - **Full folder path** (e.g. `/music/Music_matched/P/Passarani/...`) in monospace, optionally with a “Copy” button so the user can verify the path in a file manager or terminal.
  - Size (MB), format, bitrate (kbps), sample rate (Hz), bit depth (bit).
- Layout: prefer a **list-like** layout for editions (stacked blocks or a small table) so multiple duplicates are easy to compare. Support **more than two** editions if the API returns them.
- **Actions:** “Deduplicate” (and “Merge Tracks” / “Merge and Deduplicate” when applicable). On success, show a short confirmation (e.g. “Moved N duplicates”) and refresh the main list or close the modal.
- **Close:** Close button (X) and Escape to close. Animate modal in/out (e.g. fade + scale or slide).

---

## 4. Settings / configuration wizard

Implement a **full settings experience** (wizard or multi-step form plus a single “Settings” entry point) that covers **all** configurable options of PMDA. Group them logically (Plex, Paths, Scan, OpenAI, Notifications, Advanced). Assume config can be read/written via a REST API (e.g. `GET /api/config`, `PUT /api/config`) or a dedicated setup API; if the current backend does not expose these, describe the desired payload shape so the backend can be extended.

### 4.1 Plex

- **PLEX_HOST** – URL of Plex server (e.g. `http://192.168.3.2:32401`), no trailing slash.
- **PLEX_TOKEN** – API token (password field).
- **PLEX_DB_PATH** – Path to Plex DB directory **inside the container** (e.g. `/database`).
- **PLEX_DB_FILE** – DB filename (default `com.plexapp.plugins.library.db`).
- **SECTION_IDS** (or SECTION_ID) – Comma-separated section IDs for music libraries (e.g. `1` or `1,2,3`). Optional: “Auto-detect” that asks the backend to discover sections.
- **“Test connection”** button: call backend to **check Plex** (e.g. `GET /api/plex/check` or similar). Show success/failure and optionally list libraries/sections.

Support **multiple Plex instances** if the product roadmap includes it: repeatable block (URL + token + DB path) and run “Test connection” per instance.

### 4.2 Paths & mapping

- **PATH_MAP** – Optional. JSON or key-value list: Plex path prefix → host path (e.g. `/music/matched` → `/music/Music_matched/`). Explain that PMDA can auto-discover from Plex and cross-check; this is for overrides.
- **DUPE_ROOT** – Folder where duplicates are moved (e.g. `/dupes` in container).
- **PMDA_CONFIG_DIR** – Dir for config, state DB, cache DB (usually `/config` in Docker).

### 4.3 Scan & behaviour

- **SCAN_THREADS** – Number of threads (number or “auto”).
- **SKIP_FOLDERS** – Comma-separated path prefixes to skip.
- **CROSS_LIBRARY_DEDUPE** – Boolean: detect dupes across all configured libraries vs per-library only.
- **CROSSCHECK_SAMPLES** – Number of sample paths for path validation at startup (optional).
- **FORMAT_PREFERENCE** – Ordered list of audio extensions (e.g. FLAC, M4A, MP3). Advanced.

### 4.4 OpenAI

- **OPENAI_API_KEY** – API key (password field).
- **OPENAI_MODEL** – Model name (e.g. `gpt-4o-mini`). Provide a **“Fetch models”** (or “Load models”) button that calls the backend (e.g. `GET /api/openai/models`) and fills a dropdown. Show current model and fallback behaviour if any.
- **OPENAI_MODEL_FALLBACKS** – Optional comma-separated fallback models.

**“Test OpenAI”** button: verify key and model (e.g. `POST /api/openai/check`). Show success/failure.

### 4.5 MusicBrainz & notifications

- **USE_MUSICBRAINZ** – Boolean.
- **DISCORD_WEBHOOK** – Optional webhook URL for notifications.
- **LOG_LEVEL** – DEBUG / INFO / WARNING / ERROR.
- **LOG_FILE** – Path to log file (optional).

### 4.6 Wizard flow and UX

- **Wizard:** Step 1 “Plex” → Step 2 “Paths” → Step 3 “Scan” → Step 4 “OpenAI & Notifications” → Step 5 “Review & Save”. Progress indicator, Back/Next, and “Save” on last step. Validate required fields (PLEX_HOST, PLEX_TOKEN, PLEX_DB_PATH, section(s)) before allowing Next or Save.
- **Settings page:** Same fields grouped in sections (not necessarily steps). “Test Plex”, “Test OpenAI”, “Fetch models” clearly visible. Success/error messages inline or in a small toast.

---

## 5. Data and state

- **Duplicates:** Fetch `/api/duplicates` on load and when scan completes (or when user clicks “Refresh”). Update “Remaining Dupes” from the length of this list. Cache in React state; support client-side search and pagination.
- **Progress:** Poll `/api/progress` during scan; drive progress bar and status text; enable/disable Pause/Resume/New Scan according to `status`.
- **Selection:** Track selected cards (e.g. `Set<string>` of `"artist_key||album_id"`). “Deduplicate selected” sends that list to `/dedupe/selected`.
- **Config:** Load once when opening Settings; save via API or form submit. Optionally validate Plex/OpenAI after save.

---

## 6. Visual and interaction requirements

- **Style:** “Canon” – polished, distinctive, not generic. Coherent palette (e.g. teal/dark green for primary actions, neutral grays, clear typography). Enough contrast for text and controls.
- **Animations:** Smooth transitions for modals (open/close), list updates (e.g. when duplicates list changes), progress bar updates. Light hover states on cards and buttons. Optional: staggered appearance of cards when the list loads.
- **List vs grid:** Table/list view should feel first-class: readable rows, aligned columns, sort if useful (e.g. by artist or album). In list view, still show path or a “Show path” expand for each duplicate group if the API provides it in the card payload later.
- **Paths in modal:** Folder path per edition is **mandatory** in the modal: monospace font, copy button, truncated with tooltip or expand if long.
- **Responsive:** Usable on desktop and tablet; grid can collapse to fewer columns; table can scroll horizontally if needed.
- **Accessibility:** Semantic HTML, focus states, ARIA where helpful; Escape closes modals; buttons and links keyboard-activable.

---

## 7. Backend extensions (if you control the backend)

- **GET /details/...**  
  Ensure each edition in the JSON includes a **path** (or **folder**) field with the absolute path of that album folder (e.g. `str(e["folder"])`). The React UI will display it for verification.

- **Config and checks (optional but recommended):**  
  - `GET /api/config` – return current config (safe subset: no raw API keys in logs).  
  - `PUT /api/config` – persist config (e.g. to `config.json`).  
  - `GET /api/plex/check` or `POST /api/plex/check` – test Plex connection; return success + optional library/section list.  
  - `GET /api/openai/models` – return list of available models for dropdown.  
  - `POST /api/openai/check` – test OpenAI key/model.  

If these do not exist yet, the React app can still render the full settings form and display “Backend does not support config/checks yet” until the backend is extended.

---

## 8. Delivered artifact

- A **React** (Vite or CRA) SPA that:
  - Connects to the PMDA backend base URL (configurable via env or settings).
  - Implements the main dashboard (stats, scan, grid/table, search, pagination, actions).
  - Implements the duplicate-group detail modal **with folder path per edition** and all actions (Deduplicate, Merge, Merge and Deduplicate).
  - Implements the **full Settings / wizard** with every option listed above, plus Plex and OpenAI connection checks and model retrieval where the backend supports it.
- **Polished visuals**, transitions, and optional list view and animations as specified.
- **README** with: how to set the API base URL, how to run dev/build, and which backend endpoints (and optional extensions) are required.

Use this prompt in Lovable to generate the React app; adjust the backend base URL and any endpoint paths to match your actual PMDA deployment.
