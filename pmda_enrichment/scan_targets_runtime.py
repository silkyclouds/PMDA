"""Runtime-owned scan target collection for post-scan enrichment/publication."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any


_LOCAL_NAMES = {
    '_bind_runtime',
    '_scan_collect_profile_enrich_targets',
    '_scan_collect_profile_enrich_targets_for_runtime',
}


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in _LOCAL_NAMES:
            continue
        globals()[name] = value

def _scan_collect_profile_enrich_targets_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_collect_profile_enrich_targets(*args, **kwargs)


def _scan_collect_profile_enrich_targets(scan_id: int | None) -> list[dict]:
    """
    Build album targets from scan_editions for inline profile enrichment and
    final Files publication rebuild.

    These targets must carry the identity/match fields discovered during the
    scan, otherwise the post-scan publication rebuild drops metadata_source,
    strict match flags, provider ids, and MBIDs from the live Files index.
    """
    sid = _parse_int_loose(scan_id, 0)
    if sid <= 0:
        return []
    rows: list[dict] = []
    duplicate_loser_album_ids: set[int] = set()
    try:
        con = sqlite3.connect(str(STATE_DB_FILE))
        cur = con.cursor()
        try:
            cur.execute("PRAGMA table_info(duplicates_loser)")
            loser_cols = {str(r[1] or "").strip() for r in cur.fetchall() if len(r) > 1}
            loser_id_col = "loser_album_id" if "loser_album_id" in loser_cols else "album_id"
            cur.execute(f"SELECT COALESCE({loser_id_col}, 0) FROM duplicates_loser")
            duplicate_loser_album_ids = {
                int(_parse_int_loose(r[0], 0) or 0)
                for r in cur.fetchall()
                if int(_parse_int_loose(r[0], 0) or 0) > 0
            }
        except Exception:
            duplicate_loser_album_ids = set()
        cur.execute(
            """
            SELECT artist, album_id, title_raw, folder,
                   meta_json,
                   musicbrainz_id,
                   musicbrainz_release_id,
                   discogs_release_id,
                   lastfm_album_mbid,
                   bandcamp_album_url,
                   metadata_source,
                   strict_match_verified,
                   strict_match_provider,
                   strict_reject_reason,
                   strict_tracklist_score,
                   is_broken,
                   expected_track_count,
                   actual_track_count,
                   missing_indices
            FROM scan_editions
            WHERE scan_id = ?
            ORDER BY artist, album_id
            """,
            (sid,),
        )
        for (
            artist,
            album_id,
            title_raw,
            folder,
            meta_json,
            musicbrainz_id,
            musicbrainz_release_id,
            discogs_release_id,
            lastfm_album_mbid,
            bandcamp_album_url,
            metadata_source,
            strict_match_verified,
            strict_match_provider,
            strict_reject_reason,
            strict_tracklist_score,
            is_broken,
            expected_track_count,
            actual_track_count,
            missing_indices,
        ) in cur.fetchall():
            artist_name = str(artist or "").strip()
            parsed_album_id = int(_parse_int_loose(album_id, 0) or 0)
            if parsed_album_id > 0 and parsed_album_id in duplicate_loser_album_ids:
                continue
            album_title = str(title_raw or "").strip()
            if not artist_name or not album_title:
                continue
            try:
                meta = json.loads(str(meta_json or "").strip()) if meta_json else {}
            except Exception:
                meta = {}
            rows.append(
                {
                    "artist": artist_name,
                    "album_id": parsed_album_id,
                    "album_title": album_title,
                    "title_raw": album_title,
                    "folder": str(folder or "").strip(),
                    "meta": meta if isinstance(meta, dict) else {},
                    "musicbrainz_id": str(musicbrainz_id or "").strip(),
                    "musicbrainz_release_id": str(musicbrainz_release_id or "").strip(),
                    "discogs_release_id": str(discogs_release_id or "").strip(),
                    "lastfm_album_mbid": str(lastfm_album_mbid or "").strip(),
                    "bandcamp_album_url": str(bandcamp_album_url or "").strip(),
                    "metadata_source": _normalize_identity_provider(str(metadata_source or "")),
                    "strict_match_verified": bool(strict_match_verified),
                    "strict_match_provider": _normalize_identity_provider(str(strict_match_provider or "")),
                    "strict_reject_reason": str(strict_reject_reason or "").strip(),
                    "strict_tracklist_score": float(strict_tracklist_score or 0.0),
                    "is_broken": bool(is_broken),
                    "expected_track_count": int(_parse_int_loose(expected_track_count, 0) or 0) or None,
                    "actual_track_count": int(_parse_int_loose(actual_track_count, 0) or 0) or None,
                    "missing_indices": (
                        json.loads(str(missing_indices or "").strip())
                        if isinstance(missing_indices, str) and str(missing_indices or "").strip()
                        else (missing_indices or [])
                    ),
                }
            )
        con.close()
    except Exception:
        logging.debug("Failed to collect scan profile enrichment targets for scan_id=%s", sid, exc_info=True)
        return []
    folder_paths = [str((row or {}).get("folder") or "").strip() for row in rows if str((row or {}).get("folder") or "").strip()]
    if not folder_paths:
        return rows
    live_by_folder: dict[str, dict[str, Any]] = {}
    try:
        conn = _files_pg_connect()
        if conn is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            alb.folder_path,
                            COALESCE(art.name, ''),
                            COALESCE(alb.title, ''),
                            COALESCE(alb.year, 0),
                            COALESCE(alb.date_text, ''),
                            COALESCE(alb.genre, ''),
                            COALESCE(alb.label, ''),
                            COALESCE(alb.musicbrainz_release_group_id, ''),
                            COALESCE(alb.musicbrainz_release_id, ''),
                            COALESCE(alb.discogs_release_id, ''),
                            COALESCE(alb.lastfm_album_mbid, ''),
                            COALESCE(alb.bandcamp_album_url, ''),
                            COALESCE(alb.metadata_source, ''),
                            COALESCE(alb.strict_match_verified, FALSE),
                            COALESCE(alb.strict_match_provider, ''),
                            COALESCE(alb.strict_reject_reason, ''),
                            COALESCE(alb.strict_tracklist_score, 0.0),
                            COALESCE(alb.primary_tags_json, '{}')
                        FROM files_albums alb
                        JOIN files_artists art ON art.id = alb.artist_id
                        WHERE alb.folder_path = ANY(%s)
                        """,
                        (folder_paths,),
                    )
                    for (
                        folder_path,
                        artist_name,
                        album_title,
                        year,
                        date_text,
                        genre,
                        label,
                        mbid,
                        musicbrainz_release_id,
                        discogs_release_id,
                        lastfm_album_mbid,
                        bandcamp_album_url,
                        metadata_source,
                        strict_match_verified,
                        strict_match_provider,
                        strict_reject_reason,
                        strict_tracklist_score,
                        primary_tags_json,
                    ) in cur.fetchall():
                        live_by_folder[str(folder_path or "").strip()] = {
                            "artist": str(artist_name or "").strip(),
                            "album_title": _sanitize_album_title_display(str(album_title or "").strip()),
                            "year": int(_parse_int_loose(year, 0) or 0) or None,
                            "date_text": str(date_text or "").strip(),
                            "genre": str(genre or "").strip(),
                            "label": str(label or "").strip(),
                            "musicbrainz_id": str(mbid or "").strip(),
                            "musicbrainz_release_id": str(musicbrainz_release_id or "").strip(),
                            "discogs_release_id": str(discogs_release_id or "").strip(),
                            "lastfm_album_mbid": str(lastfm_album_mbid or "").strip(),
                            "bandcamp_album_url": str(bandcamp_album_url or "").strip(),
                            "metadata_source": _normalize_identity_provider(str(metadata_source or "")),
                            "strict_match_verified": bool(strict_match_verified),
                            "strict_match_provider": _normalize_identity_provider(str(strict_match_provider or "")),
                            "strict_reject_reason": str(strict_reject_reason or "").strip(),
                            "strict_tracklist_score": float(strict_tracklist_score or 0.0),
                            "primary_tags_json": primary_tags_json or "{}",
                        }
            finally:
                conn.close()
    except Exception:
        logging.debug("Failed to merge live publication hints for scan_id=%s", sid, exc_info=True)
    if not live_by_folder:
        filtered_rows: list[dict] = []
        skipped_missing = 0
        for row in rows:
            folder_key = str((row or {}).get("folder") or "").strip()
            if not folder_key:
                continue
            try:
                folder_live = path_for_fs_access(Path(folder_key))
            except Exception:
                skipped_missing += 1
                continue
            if not folder_live.exists() or not folder_live.is_dir():
                skipped_missing += 1
                continue
            filtered_rows.append(row)
        if skipped_missing:
            logging.info(
                "Scan profile/publication targets filtered missing folders for scan_id=%s: kept=%d skipped=%d",
                sid,
                len(filtered_rows),
                skipped_missing,
            )
        return filtered_rows
    for row in rows:
        folder_key = str((row or {}).get("folder") or "").strip()
        live = live_by_folder.get(folder_key)
        if not live:
            continue
        live_artist = str(live.get("artist") or "").strip()
        if live_artist and not _identity_text_is_generic(live_artist):
            row["artist"] = _choose_preferred_identity_display(str(row.get("artist") or ""), live_artist) or live_artist
        live_title = _sanitize_album_title_display(str(live.get("album_title") or "").strip())
        if live_title and not _identity_text_is_generic(live_title):
            row["album_title"] = _choose_preferred_identity_display(str(row.get("album_title") or ""), live_title) or live_title
            row["title_raw"] = row["album_title"]
        for key in (
            "musicbrainz_id",
            "musicbrainz_release_id",
            "discogs_release_id",
            "lastfm_album_mbid",
            "bandcamp_album_url",
            "metadata_source",
            "strict_match_provider",
            "strict_reject_reason",
            "primary_tags_json",
        ):
            live_value = live.get(key)
            if isinstance(live_value, str):
                live_value = live_value.strip()
            if live_value:
                row[key] = live_value
        if live.get("strict_match_verified"):
            row["strict_match_verified"] = True
        try:
            live_score = float(live.get("strict_tracklist_score") or 0.0)
        except Exception:
            live_score = 0.0
        try:
            row_score = float(row.get("strict_tracklist_score") or 0.0)
        except Exception:
            row_score = 0.0
        if live_score > row_score:
            row["strict_tracklist_score"] = live_score
        row_meta = row.get("meta")
        if not isinstance(row_meta, dict):
            row_meta = {}
            row["meta"] = row_meta
        if live.get("year") and not row_meta.get("year"):
            row_meta["year"] = live.get("year")
        if live.get("date_text") and not row_meta.get("date"):
            row_meta["date"] = live.get("date_text")
        if live.get("genre") and not row_meta.get("genre"):
            row_meta["genre"] = live.get("genre")
        if live.get("label") and not row_meta.get("label"):
            row_meta["label"] = live.get("label")
    filtered_rows: list[dict] = []
    skipped_missing = 0
    for row in rows:
        folder_key = str((row or {}).get("folder") or "").strip()
        if not folder_key:
            continue
        try:
            folder_live = path_for_fs_access(Path(folder_key))
        except Exception:
            skipped_missing += 1
            continue
        if not folder_live.exists() or not folder_live.is_dir():
            skipped_missing += 1
            continue
        filtered_rows.append(row)
    if skipped_missing:
        logging.info(
            "Scan profile/publication targets filtered missing folders for scan_id=%s: kept=%d skipped=%d",
            sid,
            len(filtered_rows),
            skipped_missing,
        )
    return filtered_rows
