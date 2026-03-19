# PMDA Documentation

PMDA is a self-hosted music matching, cleanup, publishing, and playback platform for large libraries.

- Docker image: [meaning/pmda](https://hub.docker.com/r/meaning/pmda)
- Repository: [silkyclouds/PMDA](https://github.com/silkyclouds/PMDA)
- Discord: [discord.gg/2jkwnNhHHR](https://discord.gg/2jkwnNhHHR)

## Core Docs

| Document | What it covers |
|---|---|
| [USER_GUIDE.md](USER_GUIDE.md) | First launch, folder setup, scans, review flows, playback, sharing, export workflows |
| [ARCHITECTURE.md](ARCHITECTURE.md) | System layout, scan pipeline, matching logic, OCR, AI routing, data stores, exported library model |
| [CONFIGURATION.md](CONFIGURATION.md) | Docker mounts, required settings, auth, AI providers, caches, export, reverse proxy, performance |

## Reading Order

1. Start with [USER_GUIDE.md](USER_GUIDE.md)
2. Use [CONFIGURATION.md](CONFIGURATION.md) while deploying
3. Read [ARCHITECTURE.md](ARCHITECTURE.md) if you need to understand how PMDA makes decisions

## Product Summary

PMDA can be used in two ways:

- as the main music web app
- as a middleware layer in front of Plex, Jellyfin, or Navidrome

In both cases it does the same core work:

- match albums from messy sources
- detect duplicates and incomplete releases
- publish a faster cleaned library
- keep every move reviewable and reversible
- serve playback, likes, playlists, and recommendations on top
