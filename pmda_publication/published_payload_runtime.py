"""Runtime-owned loading of published Files library payloads."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "rows_to_files_library_payload_for_runtime",
            "load_files_library_published_payload_for_runtime",
            "load_files_library_published_payload_for_artist_for_runtime",
            "_rows_to_files_library_payload",
            "_load_files_library_published_payload",
            "_load_files_library_published_payload_for_artist",
        }:
            continue
        globals()[name] = value


def rows_to_files_library_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> tuple[dict[str, dict], list[dict], int]:
    _bind_runtime(runtime)
    return _rows_to_files_library_payload(*args, **kwargs)


def load_files_library_published_payload_for_runtime(runtime: Any) -> tuple[dict[str, dict], list[dict], int]:
    _bind_runtime(runtime)
    return _load_files_library_published_payload()


def load_files_library_published_payload_for_artist_for_runtime(runtime: Any, artist_hint: str) -> tuple[dict[str, dict], list[dict], int]:
    _bind_runtime(runtime)
    return _load_files_library_published_payload_for_artist(artist_hint)


def _rows_to_files_library_payload(
    rows: list[tuple],
    *,
    verify_paths: bool = True,
) -> tuple[dict[str, dict], list[dict], int]:
    """
    Convert persisted publication rows into the PostgreSQL browse payload.

    When rebuilding from files_library_published_albums, the publication table is
    the scan truth. Do not re-stat every source folder unless explicitly asked:
    on large Unraid arrays that turns a DB rebuild into a slow HDD walk and can
    leave the visible PG index stale long after the scan already finished.
    """
    artists_map: dict[str, dict] = {}
    albums_payload: list[dict] = []
    root_dirs = _files_root_dir_strings()
    for row in rows:
        folder_path = (row[0] or "").strip()
        if not folder_path:
            continue
        album_folder_live: Path | None = None
        if verify_paths:
            # Published rows can become stale when PMDA moves albums out of FILES_ROOTS (dupes/incomplete)
            # or when the user deletes folders. Expensive verification is reserved for explicit filesystem
            # rebuilds; normal publication rebuilds trust persisted scan rows.
            try:
                album_folder_live = path_for_fs_access(Path(folder_path))
                if not album_folder_live.exists() or not album_folder_live.is_dir():
                    continue
            except Exception:
                continue
        artist_name = (row[1] or "").strip() or "Unknown Artist"
        artist_norm = (row[2] or "").strip() or _norm_artist_key(artist_name) or "unknown artist"
        album_title = _sanitize_album_title_display((row[3] or "").strip()) or "Unknown Album"
        # Recompute local artist image from the actual folder structure to avoid
        # falsely assigning a single FILES_ROOT/artist.jpg to every artist in flat libraries.
        image_path = ""
        has_image = False
        if verify_paths:
            try:
                album_folder = album_folder_live or path_for_fs_access(Path(folder_path))
                artist_folder = _files_guess_artist_folder(album_folder, artist_name, root_dirs=root_dirs)
                local_img = _first_artist_image_path(artist_folder) if artist_folder else None
                if local_img and local_img.is_file():
                    image_path = str(local_img)
                    has_image = True
            except Exception:
                has_image = False
                image_path = ""
        if not has_image:
            # Fallback to the published stored path only when it exists and is not a FILES_ROOT-level image.
            stored_path = (row[15] or "").strip()
            stored_has = bool(row[14]) and bool(stored_path)
            if stored_has:
                if not verify_paths:
                    image_path = stored_path
                    has_image = True
                else:
                    try:
                        sp = path_for_fs_access(Path(stored_path))
                    except Exception:
                        sp = Path(stored_path)
                    try:
                        if sp.is_file():
                            if _is_media_cache_file(sp, kind="artist"):
                                image_path = str(sp)
                                has_image = True
                            elif not _files_is_files_root_dir(sp.parent, root_dirs=root_dirs):
                                # Extra guard for non-cache files: image should live under the album folder's ancestry.
                                try:
                                    album_folder = album_folder_live or path_for_fs_access(Path(folder_path))
                                    if sp.parent == album_folder or sp.parent in album_folder.parents:
                                        image_path = str(sp)
                                        has_image = True
                                except Exception:
                                    image_path = str(sp)
                                    has_image = True
                    except Exception:
                        pass
        if artist_norm not in artists_map:
            artists_map[artist_norm] = {
                "name": artist_name,
                "image_path": image_path or None,
                "has_image": has_image,
            }
        else:
            artists_map[artist_norm]["name"] = _choose_preferred_identity_display(
                str(artists_map[artist_norm].get("name") or ""),
                artist_name,
            )
            if has_image and not artists_map[artist_norm].get("has_image"):
                artists_map[artist_norm]["image_path"] = image_path
                artists_map[artist_norm]["has_image"] = True
        try:
            tags_json = json.loads(row[9] or "[]") if row[9] else []
            if not isinstance(tags_json, list):
                tags_json = []
        except Exception:
            tags_json = []
        try:
            tracks = json.loads(row[31] or "[]") if row[31] else []
            if not isinstance(tracks, list):
                tracks = []
        except Exception:
            tracks = []
        strict_match_verified = bool(row[17])
        strict_match_provider = _normalize_identity_provider((row[18] or "").strip())
        strict_reject_reason = str(row[19] or "").strip()
        try:
            strict_tracklist_score = float(row[20] or 0.0)
        except Exception:
            strict_tracklist_score = 0.0
        musicbrainz_id = (row[21] or "").strip()
        musicbrainz_release_id = (row[22] or "").strip()
        discogs_release_id = (row[32] or "").strip()
        lastfm_album_mbid = (row[33] or "").strip()
        bandcamp_album_url = (row[34] or "").strip()
        has_cover_row = bool(row[12])
        if verify_paths and not has_cover_row:
            try:
                has_cover_row = album_folder_has_cover(path_for_fs_access(Path(folder_path)))
            except Exception:
                has_cover_row = False
        albums_payload.append(
            {
                "artist_norm": artist_norm,
                "artist_name": artist_name,
                "title": album_title,
                "title_norm": (row[4] or "").strip() or norm_album_for_dedup(album_title, normalize_parenthetical=True),
                "folder_path": folder_path,
                "year": row[5],
                "date_text": (row[6] or "").strip(),
                "genre": (row[7] or "").strip(),
                "label": (row[8] or "").strip(),
                "tags_json": json.dumps(tags_json),
                "format": (row[10] or "").strip(),
                "is_lossless": bool(row[11]),
                "has_cover": has_cover_row,
                "cover_path": (row[13] or "").strip(),
                "mb_identified": bool(strict_match_verified),
                "strict_match_verified": bool(strict_match_verified),
                "strict_match_provider": strict_match_provider,
                "strict_reject_reason": strict_reject_reason,
                "strict_tracklist_score": strict_tracklist_score,
                "musicbrainz_release_group_id": musicbrainz_id,
                "musicbrainz_release_id": musicbrainz_release_id,
                "track_count": int(row[23] or 0),
                "total_duration_sec": int(row[24] or 0),
                "is_broken": bool(row[25]),
                "expected_track_count": row[26],
                "actual_track_count": int(row[27] or 0),
                "missing_indices_json": row[28] or "[]",
                "missing_required_tags_json": row[29] or "[]",
                "primary_tags_json": row[30] or "{}",
                "tracks": tracks,
                "discogs_release_id": discogs_release_id,
                "lastfm_album_mbid": lastfm_album_mbid,
                "bandcamp_album_url": bandcamp_album_url,
                "metadata_source": _normalize_identity_provider((row[35] or "").strip()),
                "source_id": int(row[36] or 0) if len(row) > 36 and row[36] is not None else None,
            }
        )
    _apply_genre_defaults_to_albums_payload(albums_payload)
    return artists_map, albums_payload, len(albums_payload)


def _load_files_library_published_payload() -> tuple[dict[str, dict], list[dict], int]:
    """Load published albums from state.db as payload for Files PG index rebuild."""
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
        cur = con.cursor()
        cur.execute(
            """
            SELECT
                folder_path, artist_name, artist_norm, album_title, title_norm,
                year, date_text, genre, label, tags_json, format, is_lossless,
                has_cover, cover_path, has_artist_image, artist_image_path,
                mb_identified, strict_match_verified, strict_match_provider, strict_reject_reason, strict_tracklist_score,
                musicbrainz_release_group_id, musicbrainz_release_id, track_count, total_duration_sec,
                is_broken, expected_track_count, actual_track_count, missing_indices_json,
                missing_required_tags_json, primary_tags_json, tracks_json,
                discogs_release_id, lastfm_album_mbid, bandcamp_album_url, primary_metadata_source, source_id
            FROM files_library_published_albums
            ORDER BY lower(artist_name), lower(album_title), folder_path
            """
        )
        rows = cur.fetchall()
        con.close()
    except Exception:
        logging.debug("Failed to load files_library_published_albums", exc_info=True)
        return {}, [], 0
    return _rows_to_files_library_payload(rows, verify_paths=False)


def _load_files_library_published_payload_for_artist(artist_hint: str) -> tuple[dict[str, dict], list[dict], int]:
    """Load published payload for one artist (by normalized name)."""
    artist_name = str(artist_hint or "").strip()
    if not artist_name:
        return {}, [], 0
    artist_norm = _norm_artist_key(artist_name)
    artist_norm_alt = norm_album(artist_name or "") or artist_norm
    artist_like = "%" + " ".join(artist_name.lower().split()).replace("%", "").replace("_", "") + "%"
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
        cur = con.cursor()
        cur.execute(
            """
            SELECT
                folder_path, artist_name, artist_norm, album_title, title_norm,
                year, date_text, genre, label, tags_json, format, is_lossless,
                has_cover, cover_path, has_artist_image, artist_image_path,
                mb_identified, strict_match_verified, strict_match_provider, strict_reject_reason, strict_tracklist_score,
                musicbrainz_release_group_id, musicbrainz_release_id, track_count, total_duration_sec,
                is_broken, expected_track_count, actual_track_count, missing_indices_json,
                missing_required_tags_json, primary_tags_json, tracks_json,
                discogs_release_id, lastfm_album_mbid, bandcamp_album_url, primary_metadata_source, source_id
            FROM files_library_published_albums
            WHERE artist_norm = ?
               OR artist_norm = ?
               OR lower(artist_name) = lower(?)
               OR lower(artist_name) LIKE ?
            ORDER BY lower(album_title), folder_path
            """,
            (artist_norm, artist_norm_alt, artist_name, artist_like),
        )
        rows = cur.fetchall()
        con.close()
    except Exception:
        logging.debug("Failed to load files_library_published_albums for artist %s", artist_name, exc_info=True)
        return {}, [], 0
    return _rows_to_files_library_payload(rows, verify_paths=False)
