#!/usr/bin/env bash
# Create pmda_tests by COPYING selected albums from a real artist folder (e.g. Ochre).
# Never deletes or modifies source files. Destination: $MUSIC_ROOT/pmda_tests/<Artist>/.
#
# Required: SOURCE_ARTIST_DIR = path to the artist folder (e.g. /mnt/user/MURRAY/Music/Ochre).
# Optional: MUSIC_ROOT = music root (default /music). Test dir = $MUSIC_ROOT/pmda_tests.
#
# Run on server (Ochre is under Music_matched/O/):
#   SOURCE_ARTIST_DIR=/mnt/user/MURRAY/Music/Music_matched/O/Ochre MUSIC_ROOT=/mnt/user/MURRAY/Music ./scripts/create_pmda_test_files.sh
# Run locally (replace with your path to Ochre):
#   SOURCE_ARTIST_DIR=/path/to/Ochre MUSIC_ROOT=/path/to/music ./scripts/create_pmda_test_files.sh
# Run with repo fixture (one FLAC + one MP3 album, guarantees duplicate/no-cover/gaps):
#   SOURCE_ARTIST_DIR="$(pwd)/scripts/fixtures/source_artist/TestArtist" MUSIC_ROOT="$(pwd)/scripts/fixtures" ./scripts/create_pmda_test_files.sh
#
# Selected albums (at least):
#   - 1 with missing tracks (gaps) — for "Incomplete" in Library/Unduper
#   - 1 without cover
#   - 1 in FLAC
#   - 1 in MP3
#   - 1 duplicate (same album in two editions/formats, or a "[dupe]" copy)
#
# For PMDA to detect duplicates and incomplete albums: run a scan with SECTION_IDS
# that include the Plex library containing pmda_tests, and ensure PATH_MAP maps
# that library's root to the container path (e.g. /music/pmda_tests).
set -e

MUSIC_ROOT="${MUSIC_ROOT:-/music}"
SOURCE_ARTIST_DIR="${SOURCE_ARTIST_DIR:-$MUSIC_ROOT/Music_matched/O/Ochre}"
BASE="$MUSIC_ROOT/pmda_tests"
ARTIST_NAME="$(basename "$SOURCE_ARTIST_DIR")"
DEST="$BASE/$ARTIST_NAME"

if [ ! -d "$SOURCE_ARTIST_DIR" ]; then
  echo "Error: SOURCE_ARTIST_DIR is not a directory: $SOURCE_ARTIST_DIR"
  echo "Set SOURCE_ARTIST_DIR to your artist folder (e.g. Ochre). Example:"
  echo "  SOURCE_ARTIST_DIR=/mnt/user/MURRAY/Music/Music_matched/O/Ochre MUSIC_ROOT=/mnt/user/MURRAY/Music $0"
  exit 1
fi

# Cover file names we consider as "has cover"
has_cover() {
  local d="$1"
  [ -f "$d/cover.jpg" ] || [ -f "$d/cover.png" ] || [ -f "$d/cover.jpeg" ] || \
  [ -f "$d/folder.jpg" ] || [ -f "$d/Folder.jpg" ] || [ -f "$d/AlbumArt.jpg" ] || \
  [ -f "$d/AlbumArtSmall.jpg" ] || [ -f "$d/front.jpg" ] || [ -f "$d/artwork.jpg" ]
  return $?
}

# Detect track numbers from audio files in dir; set HAS_GAPS=1 if there are gaps
check_gaps() {
  local d="$1"
  HAS_GAPS=0
  local nums=""
  while IFS= read -r -d '' f; do
    local base; base="$(basename "$f")"
    if [[ "$base" =~ ^([0-9]+) ]]; then
      nums="$nums ${BASH_REMATCH[1]}"
    fi
  done < <(find "$d" -maxdepth 1 -type f \( -iname "*.flac" -o -iname "*.mp3" -o -iname "*.m4a" \) -print0 2>/dev/null)
  [ -z "$nums" ] && return
  local sorted; sorted=$(echo "$nums" | tr ' ' '\n' | sort -n | uniq)
  local prev=0
  while read -r n; do
    [ -z "$n" ] && continue
    if [ "$prev" -gt 0 ] && [ "$((n - prev))" -gt 1 ]; then
      HAS_GAPS=1
      return
    fi
    prev=$n
  done <<< "$sorted"
}

# Primary format in dir: flac, mp3, or mixed
primary_format() {
  local d="$1"
  local flac mp3
  flac=$(find "$d" -maxdepth 1 -type f \( -iname "*.flac" \) 2>/dev/null | wc -l | tr -d ' ')
  mp3=$(find "$d" -maxdepth 1 -type f \( -iname "*.mp3" \) 2>/dev/null | wc -l | tr -d ' ')
  [ "$flac" -ge "$mp3" ] && [ "${flac:-0}" -gt 0 ] && echo "flac" && return
  [ "${mp3:-0}" -gt 0 ] && echo "mp3" && return
  echo ""
}

# Normalize album name for duplicate detection (lowercase, strip common suffixes)
normalize_album() {
  local name="$1"
  name="$(echo "$name" | tr '[:upper:]' '[:lower:]')"
  name="${name// (flac)/}"
  name="${name// (mp3)/}"
  name="${name// (disc 1)/}"
  name="${name// (disc 2)/}"
  name="${name//(flac)/}"
  name="${name//(mp3)/}"
  echo "$name" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
}

echo "Scanning artist folder: $SOURCE_ARTIST_DIR"
mkdir -p "$DEST"

ALBUM_WITH_GAPS=""
ALBUM_NO_COVER=""
ALBUM_FLAC=""
ALBUM_MP3=""
declare -A NORM_TO_DIRS   # normalized name -> "dir1|dir2" for duplicate pairs

while IFS= read -r -d '' album_dir; do
  [ -d "$album_dir" ] || continue
  name="$(basename "$album_dir")"
  norm="$(normalize_album "$name")"
  [ -z "$norm" ] && continue

  # Track normalized name for duplicate detection
  if [ -n "${NORM_TO_DIRS[$norm]}" ]; then
    NORM_TO_DIRS[$norm]="${NORM_TO_DIRS[$norm]}|$album_dir"
  else
    NORM_TO_DIRS[$norm]="$album_dir"
  fi

  fmt="$(primary_format "$album_dir")"
  if [ "$fmt" = "flac" ] && [ -z "$ALBUM_FLAC" ]; then ALBUM_FLAC="$album_dir"; fi
  if [ "$fmt" = "mp3" ] && [ -z "$ALBUM_MP3" ]; then ALBUM_MP3="$album_dir"; fi

  if ! has_cover "$album_dir" && [ -z "$ALBUM_NO_COVER" ]; then
    ALBUM_NO_COVER="$album_dir"
  fi

  check_gaps "$album_dir"
  if [ "$HAS_GAPS" -eq 1 ] && [ -z "$ALBUM_WITH_GAPS" ]; then
    ALBUM_WITH_GAPS="$album_dir"
  fi
done < <(find "$SOURCE_ARTIST_DIR" -maxdepth 1 -mindepth 1 -type d -print0 2>/dev/null)

# Pick a duplicate pair (same normalized name, two or more dirs)
DUP_SRC1="" DUP_SRC2=""
for norm in "${!NORM_TO_DIRS[@]}"; do
  IFS='|' read -ra DIRS <<< "${NORM_TO_DIRS[$norm]}"
  if [ ${#DIRS[@]} -ge 2 ]; then
    DUP_SRC1="${DIRS[0]}"
    DUP_SRC2="${DIRS[1]}"
    break
  fi
done

# Build list of dirs to copy (unique)
declare -A TO_COPY
[ -n "$ALBUM_WITH_GAPS" ] && TO_COPY["$ALBUM_WITH_GAPS"]=1
[ -n "$ALBUM_NO_COVER" ] && TO_COPY["$ALBUM_NO_COVER"]=1
[ -n "$ALBUM_FLAC" ] && TO_COPY["$ALBUM_FLAC"]=1
[ -n "$ALBUM_MP3" ] && TO_COPY["$ALBUM_MP3"]=1
[ -n "$DUP_SRC1" ] && TO_COPY["$DUP_SRC1"]=1
[ -n "$DUP_SRC2" ] && TO_COPY["$DUP_SRC2"]=1

# If we found nothing, copy first few albums so we have something
if [ ${#TO_COPY[@]} -eq 0 ]; then
  count=0
  while IFS= read -r -d '' album_dir; do
    [ -d "$album_dir" ] || continue
    TO_COPY["$album_dir"]=1
    count=$((count+1))
    [ "$count" -ge 5 ] && break
  done < <(find "$SOURCE_ARTIST_DIR" -maxdepth 1 -mindepth 1 -type d -print0 2>/dev/null)
fi

for src in "${!TO_COPY[@]}"; do
  name="$(basename "$src")"
  dest_album="$DEST/$name"
  if [ -d "$dest_album" ]; then
    echo "  (skip existing) $name"
    continue
  fi
  echo "  Copying: $name"
  cp -R "$src" "$dest_album"
done

# GUARANTEE 1: Always have a duplicate for unduper (same album, two folders)
# GUARANTEE 2: Always have one album without cover
# GUARANTEE 3: Always have one album with gaps (incomplete)
# Collect base albums (excluding already-generated variants)
BASE_ALBUMS=()
while IFS= read -r -d '' dir; do
  [ -d "$dir" ] || continue
  base="$(basename "$dir")"
  [[ "$base" == *" [dupe]" ]] && continue
  [[ "$base" == *" (no cover)" ]] && continue
  [[ "$base" == *" (gaps)" ]] && continue
  BASE_ALBUMS+=("$dir")
done < <(find "$DEST" -maxdepth 1 -mindepth 1 -type d -print0 2>/dev/null)

if [ ${#BASE_ALBUMS[@]} -gt 0 ]; then
  first="${BASE_ALBUMS[0]}"
  name="$(basename "$first")"
  dest_dup="$DEST/${name} [dupe]"
  rm -rf "$dest_dup"
  echo "  Creating duplicate (for unduper): ${name} [dupe]"
  cp -R "$first" "$dest_dup"
fi

# No-cover variant (use first or second base album)
if [ ${#BASE_ALBUMS[@]} -gt 0 ]; then
  src="${BASE_ALBUMS[0]}"
  base="$(basename "$src")"
  no_cover_dest="$DEST/${base} (no cover)"
  rm -rf "$no_cover_dest"
  cp -R "$src" "$no_cover_dest"
  (cd "$no_cover_dest" && rm -f cover.jpg cover.png cover.jpeg folder.jpg Folder.jpg AlbumArt.jpg AlbumArtSmall.jpg front.jpg artwork.jpg 2>/dev/null; true)
  echo "  Creating no-cover variant: ${base} (no cover)"
fi

# Gaps variant: pick first album with 3+ audio files
for dir in "${BASE_ALBUMS[@]}"; do
  count=0
  while IFS= read -r -d '' f; do
    [ -f "$f" ] && count=$((count+1))
  done < <(find "$dir" -maxdepth 1 -type f \( -iname "*.flac" -o -iname "*.mp3" -o -iname "*.m4a" \) -print0 2>/dev/null)
  [ "$count" -lt 3 ] && continue
  base="$(basename "$dir")"
  gaps_dest="$DEST/${base} (gaps)"
  rm -rf "$gaps_dest"
  cp -R "$dir" "$gaps_dest"
  idx=0
  while IFS= read -r -d '' f; do
    [ -f "$f" ] || continue
    idx=$((idx+1))
    [ "$idx" -eq 2 ] && { rm -f "$f"; break; }
  done < <(find "$gaps_dest" -maxdepth 1 -type f \( -iname "*.flac" -o -iname "*.mp3" -o -iname "*.m4a" \) -print0 2>/dev/null)
  echo "  Creating gaps variant: ${base} (gaps)"
  break
done

echo "Created pmda_tests under $MUSIC_ROOT: $DEST"
echo "Summary:"
echo "  - Base albums from source (FLAC, MP3, gaps, no-cover, etc.)"
echo "  - Duplicate:   (created [dupe] copy for unduper)"
echo "  - No cover:    (created (no cover) variant)"
echo "  - Gaps:        (created (gaps) variant for incomplete-album tests)"
echo ""
echo "=== BON SET DE TEST – CHECKLIST ==="
echo "1. Plex: run 'Scan Library' for the library that contains $BASE"
echo "   (so Plex indexes the new folders: [dupe], (no cover), (gaps))"
echo "2. PMDA Settings: SECTION_IDS must include that library's section ID"
echo "   (e.g. section 1). Check Plex → Manage → Libraries → your Music library."
echo "3. PMDA Settings: PATH_MAP must map the Plex path for that library to the"
echo "   container path (e.g. /music). Same root under which pmda_tests lives."
echo "4. PMDA: run 'Scan' so duplicates, missing covers, and incomplete albums are detected."
echo ""
echo "If PMDA still finds nothing: SECTION_IDS wrong, PATH_MAP not matching album paths,"
echo "or Plex DB paths not mapped (container sees different paths than Plex)."
ls -la "$DEST"
