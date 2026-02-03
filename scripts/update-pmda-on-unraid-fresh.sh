#!/bin/bash
# Deploy PMDA with a FRESH config (like a new install). Use this to re-test the full
# flow: wizard, config, scan, and verify that PMDA detects duplicates/incomplete albums.
# Run ON THE UNRAID SERVER (or via: ssh root@192.168.3.2 'bash -s' < scripts/update-pmda-on-unraid-fresh.sh).
#
# Optional: set RECREATE_TEST_FOLDER=1 to re-create the PMDA test folder (pmda_tests) from
# a source artist so you have a clean base for duplicate/incomplete tests. Default source:
# SOURCE_ARTIST_DIR=/mnt/user/MURRAY/Music/Music_matched/O/Ochre, MUSIC_ROOT=/mnt/user/MURRAY/Music.
#
# This script:
# 0. (Optional) Re-create pmda_tests folder so scan has clean test data
# 1. Pull image, stop/remove existing container
# 2. Use fresh config directory (PMDA_fresh), clear state.db/config so PMDA starts unconfigured
# 3. Create container; wizard and "container mounts" message will show at first open

set -e
echo "=== PMDA fresh install (full re-test) on Unraid ==="

CONTAINER_NAME="${CONTAINER_NAME:-PMDA_WEBUI}"
IMAGE="${IMAGE:-meaning/pmda:beta}"
CONFIG_DIR="${CONFIG_DIR:-/mnt/cache/appdata/PMDA_fresh}"
PLEX_BASE_HOST="${PLEX_BASE_HOST:-/mnt/cache/plex-saturday/config}"
MUSIC_ROOT="${MUSIC_ROOT:-/mnt/user/MURRAY/Music}"
SOURCE_ARTIST_DIR="${SOURCE_ARTIST_DIR:-$MUSIC_ROOT/Music_matched/O/Ochre}"

# Optional: re-create PMDA test folder for a clean scan (duplicates, no-cover, gaps)
# Set CREATE_PMDA_TEST_FILES_SCRIPT to the full path of create_pmda_test_files.sh on the server
# when running via: ssh ... 'bash -s' < scripts/update-pmda-on-unraid-fresh.sh (script dir not available).
if [ "${RECREATE_TEST_FOLDER:-1}" = "1" ] && [ -d "$SOURCE_ARTIST_DIR" ]; then
  echo "0. Re-creating PMDA test folder (pmda_tests) from $SOURCE_ARTIST_DIR..."
  CREATOR=""
  if [ -n "${CREATE_PMDA_TEST_FILES_SCRIPT:-}" ] && [ -x "$CREATE_PMDA_TEST_FILES_SCRIPT" ]; then
    CREATOR="$CREATE_PMDA_TEST_FILES_SCRIPT"
  elif [ -n "${BASH_SOURCE[0]:-}" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    [ -x "$SCRIPT_DIR/create_pmda_test_files.sh" ] && CREATOR="$SCRIPT_DIR/create_pmda_test_files.sh"
  fi
  if [ -n "$CREATOR" ]; then
    SOURCE_ARTIST_DIR="$SOURCE_ARTIST_DIR" MUSIC_ROOT="$MUSIC_ROOT" "$CREATOR"
  else
    echo "    (create_pmda_test_files.sh not found; set CREATE_PMDA_TEST_FILES_SCRIPT or run from repo on server)"
  fi
elif [ "${RECREATE_TEST_FOLDER:-1}" = "1" ] && [ ! -d "$SOURCE_ARTIST_DIR" ]; then
  echo "0. Skipping test folder: SOURCE_ARTIST_DIR not found: $SOURCE_ARTIST_DIR"
fi

echo "1. Pull image..."
docker pull "$IMAGE"

echo "2. Stop and remove existing container..."
docker stop "$CONTAINER_NAME" 2>/dev/null || true
docker rm "$CONTAINER_NAME" 2>/dev/null || true

echo "3. Prepare fresh config directory: $CONFIG_DIR"
mkdir -p "$CONFIG_DIR"
rm -f "$CONFIG_DIR/state.db" "$CONFIG_DIR/cache.db" "$CONFIG_DIR/config.json" "$CONFIG_DIR/pmda.log" 2>/dev/null || true

echo "4. Create container with fresh config (wizard will show)..."
docker run -d \
  --name "$CONTAINER_NAME" \
  --restart unless-stopped \
  -p 5005:5005 \
  -v "$CONFIG_DIR:/config" \
  -v "$MUSIC_ROOT:/music:rw" \
  -v "$MUSIC_ROOT/Music_dupes:/dupes" \
  -v "$PLEX_BASE_HOST:/database:ro" \
  "$IMAGE" --serve

echo "5. Check..."
sleep 5
curl -s -o /dev/null -w "GET /api/config -> %{http_code}\n" http://localhost:5005/api/config || true

echo ""
echo "=== Done. Web UI: http://192.168.3.2:5005 ==="
echo ""
echo "--- Full re-test checklist ---"
echo "1. Open http://192.168.3.2:5005 – you should see the welcome modal and container mounts (Config/Plex DB/Music/Dupes)."
echo "2. Go to Settings and complete the wizard: Plex host, token, library (SECTION_IDS), Path mapping (or Autodetect)."
echo "3. In Plex: run 'Scan Library' for the library that contains pmda_tests (so new folders are indexed)."
echo "4. In PMDA Settings: ensure SECTION_IDS includes that library; PATH_MAP maps its root to /music."
echo "5. In PMDA: run a Scan. Then check Unduper (duplicates), Library (incomplete), and that auto-move works if enabled."
echo ""
