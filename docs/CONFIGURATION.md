# PMDA Configuration

This page covers the settings that matter for a real PMDA deployment.

Support: [Discord](https://discord.gg/2jkwnNhHHR)

---

## 1. Deployment Model

The recommended deployment is the all-in-one Docker image:

- PMDA backend
- PostgreSQL
- Redis
- React frontend
- OCR and media tooling

Example:

```bash
docker run -d \
  --name pmda \
  --restart unless-stopped \
  -p 5005:5005 \
  -e PMDA_AUTH_ENABLED=1 \
  -e PMDA_MEDIA_CACHE_ROOT=/cache \
  -v /srv/pmda/config:/config \
  -v /srv/pmda/cache:/cache \
  -v /srv/music:/music:rw \
  -v /srv/pmda/review:/dupes:rw \
  -v /srv/pmda/export:/export:rw \
  meaning/pmda:latest
```

---

## 2. Required Mounts

| Mount | Purpose | Typical host path |
|---|---|---|
| `/config` | PostgreSQL data, Redis state, PMDA settings, auth state, logs | `/srv/pmda/config` |
| `/music` | Source folders seen by PMDA | `/srv/music` |
| `/dupes` | Duplicate and incomplete review targets | `/srv/pmda/review` |

### Strongly recommended mounts

| Mount | Purpose |
|---|---|
| `/cache` | SSD/NVMe media cache for artwork and derived assets |
| `/export` | Clean generated library for Plex/Jellyfin/Navidrome or direct use |

---

## 3. Essential Environment Variables

### Auth

| Variable | Purpose | Recommended |
|---|---|---|
| `PMDA_AUTH_ENABLED` | Enables account-based access control | `1` |
| `PMDA_AUTH_ALLOW_PUBLIC_BOOTSTRAP` | Allows or blocks public first-user bootstrap | `false` after setup |
| `PMDA_AUTH_TRUST_PROXY_HEADERS` | Trust reverse-proxy IP headers | `true` behind Cloudflare/reverse proxy |
| `PMDA_AUTH_SESSION_COOKIE_SECURE` | Secure cookie over HTTPS | `true` for public deployments |
| `PMDA_AUTH_SESSION_COOKIE_SAMESITE` | Session cookie policy | `Lax` |

### Media cache

| Variable | Purpose | Recommended |
|---|---|---|
| `PMDA_MEDIA_CACHE_ROOT` | Filesystem cache for artwork and derived images | `/cache` |

### Login protection

| Variable | Purpose | Recommended |
|---|---|---|
| `PMDA_AUTH_LOGIN_RATE_LIMIT_WINDOW_SEC` | Rate-limit window | `900` |
| `PMDA_AUTH_LOGIN_RATE_LIMIT_IP_MAX_ATTEMPTS` | Attempts per IP in the window | `5` |
| `PMDA_AUTH_LOGIN_RATE_LIMIT_USER_MAX_ATTEMPTS` | Attempts per username in the window | `5` |
| `PMDA_AUTH_LOGIN_FAILURE_DELAY_MS` | Delay after failed login | `750` |

---

## 4. Folder Configuration

The preferred way to configure folder roles is through the **Settings** UI.

### Folder types

- **Standard source folder**: main collection roots
- **Incoming folder**: optional drop zone for fresh imports

### Additional destinations

- **Duplicates target**
- **Incomplete target**
- **Export root**

### Important rule

Incoming folders should never be the final published location.

If you use incoming folders, PMDA should move or link processed content into one selected standard source or export destination.

---

## 5. AI Provider Configuration

PMDA supports multiple AI backends.

### OpenAI / Codex

Common variables include:

- `AI_PROVIDER`
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `OPENAI_VISION_MODEL`

### Anthropic

Common variables include:

- `ANTHROPIC_API_KEY`
- `ANTHROPIC_MODEL`

### Gemini

Common variables include:

- `GEMINI_API_KEY`
- `GEMINI_MODEL`

### Ollama

Common variables include:

- `OLLAMA_HOST`
- `OLLAMA_MODEL`

### Guidance

- Use local and deterministic signals first.
- Enable AI only if you want PMDA to solve harder matching and enrichment problems.
- Keep OCR enabled where possible: it often reduces AI usage.
- Put credentials in environment variables, not hardcoded files.

---

## 6. Metadata Providers

PMDA can cross-check these providers:

- MusicBrainz
- Discogs
- Last.fm
- Bandcamp
- AcoustID

Provider tokens and keys should be configured only if you plan to use those services.

---

## 7. Export Configuration

PMDA can build a downstream library using different strategies:

- **Hardlink**: best for same-filesystem deployments with no duplicate storage cost
- **Symlink**: lightweight, but depends on downstream software and path visibility
- **Copy**: safest, but uses more storage
- **Move**: destructive; usually reserved for specific workflows

### Recommendation

Use **Hardlink** if your source and export roots are on compatible filesystems.

---

## 8. Reverse Proxy / Public Access

Recommended public deployment:

- put PMDA behind HTTPS
- enable PMDA auth
- keep PMDA off direct WAN ports where possible
- use Cloudflare Tunnel or a standard reverse proxy

### Recommended app settings for public deployments

```env
PMDA_AUTH_ENABLED=1
PMDA_AUTH_ALLOW_PUBLIC_BOOTSTRAP=false
PMDA_AUTH_TRUST_PROXY_HEADERS=true
PMDA_AUTH_SESSION_COOKIE_SECURE=true
PMDA_AUTH_SESSION_COOKIE_SAMESITE=Lax
PMDA_AUTH_LOGIN_RATE_LIMIT_WINDOW_SEC=900
PMDA_AUTH_LOGIN_RATE_LIMIT_IP_MAX_ATTEMPTS=5
PMDA_AUTH_LOGIN_RATE_LIMIT_USER_MAX_ATTEMPTS=5
PMDA_AUTH_LOGIN_FAILURE_DELAY_MS=750
```

### Cloudflare-specific note

PMDA should consume real client IP headers correctly behind the tunnel. The browser should only talk to PMDA over the public hostname, not to LAN-only media endpoints.

---

## 9. Performance Recommendations

### Storage

- put `/config` on stable, persistent storage
- put `/cache` on SSD or NVMe
- avoid serving hot UI paths from spinning disks

### Scans

- use standard source folders that point at the real collection roots
- use incoming folders only where that flow exists
- keep duplicate and incomplete targets outside the source set

### AI cost control

Use:

- OCR-first matching
- provider cross-checks
- batched AI work
- cache reuse

This keeps scans faster and reduces token cost.

---

## 10. Unraid Guidance

Official template shipped with the repo:

- [unraid/pmda.xml](../unraid/pmda.xml)

Recommended Unraid mappings:

- `/config` -> appdata or another persistent system path
- `/cache` -> SSD/NVMe path
- `/music` -> your source library root(s)
- `/dupes` -> review/quarantine path
- `/export` -> optional generated clean library root

Recommended template philosophy:

- expose only the settings a normal user actually needs
- keep advanced tuning out of the default UI
- favor folder-role setup from the PMDA web settings rather than too many container variables

---

## 11. Recommended Minimal Setup

If you want the shortest path to a working system:

1. mount `/config`, `/music`, `/dupes`, and ideally `/cache`
2. set `PMDA_AUTH_ENABLED=1`
3. start PMDA
4. configure folders in the Settings UI
5. run the first scan
6. review duplicates and incomplete moves
7. enable AI providers only if you need them

---

## 12. Troubleshooting Checklist

### PMDA starts but scans nothing

Check:

- source folders are configured and enabled
- your music is mounted under the expected container path
- the source roots are not pointing at the wrong subdirectory

### UI is slow

Check:

- `/cache` is on SSD/NVMe
- the app is not repeatedly reading slow source disks for artwork
- your reverse proxy is not rewriting PMDA media URLs incorrectly

### Public login behaves strangely

Check:

- HTTPS is correct
- secure cookie settings are enabled
- proxy headers are trusted only behind a real proxy/tunnel

### Export library looks wrong

Check:

- export root
- export strategy
- winner/source-root configuration
- whether the content was published or quarantined during review flows
