#!/bin/bash
# Run ON THE UNRAID SERVER (192.168.3.2). No env vars needed for Plex/config — everything is in SQLite (state.db in /config).
# Usage: ssh root@192.168.3.2 'bash -s' < scripts/update-pmda-on-unraid.sh
# Or copy this script to the server and run: ./update-pmda-on-unraid.sh
#
# Required: 4 volume binds only. Set PLEX_BASE_HOST to the Plex installation base (config root).
# PMDA discovers the database under that path on first startup and saves it to SQLite.

set -e
echo "=== PMDA update on Unraid ==="

CONTAINER_NAME="${CONTAINER_NAME:-PMDA_WEBUI}"
IMAGE="${IMAGE:-meaning/pmda:beta}"

# Plex installation base path on host (config root). Must be the Plex instance that has Music/PMDA tests (e.g. plex-saturday, not plex-sundays).
# PMDA will search for com.plexapp.plugins.library.db under this path and persist the resolved path.
PLEX_BASE_HOST="${PLEX_BASE_HOST:-/mnt/cache/plex-saturday/config}"

echo "1. Pull image..."
docker pull "$IMAGE"

echo "2. Stop and remove existing container..."
docker stop "$CONTAINER_NAME" 2>/dev/null || true
docker rm "$CONTAINER_NAME" 2>/dev/null || true

echo "3. Create container (4 binds only; config from SQLite in /config)..."
docker run -d \
  --name "$CONTAINER_NAME" \
  --restart unless-stopped \
  -p 5005:5005 \
  -v /mnt/cache/appdata/PMDA:/config \
  -v "/mnt/user/MURRAY/Music:/music:rw" \
  -v /mnt/user/MURRAY/Music/Music_dupes:/dupes \
  -v "$PLEX_BASE_HOST:/database:ro" \
  "$IMAGE" --serve

echo "4. Check..."
sleep 5
curl -s -o /dev/null -w "GET /api/config -> %{http_code}\n" http://localhost:5005/api/config || true

echo "=== Done. Web UI: http://192.168.3.2:5005 ==="
