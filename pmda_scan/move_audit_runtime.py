"""Runtime-owned scan move audit/detail handlers."""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

from flask import Response, jsonify, request, send_file


_LOCAL_NAMES = {
    '_bind_runtime',
    '_scan_move_active_folder',
    '_scan_move_active_folder_for_runtime',
    '_scan_move_artwork_source_path',
    '_scan_move_artwork_source_path_for_runtime',
    '_scan_move_detail_payload',
    '_scan_move_detail_payload_for_runtime',
    '_scan_move_expected_tracks',
    '_scan_move_expected_tracks_for_runtime',
    '_scan_move_folder_artwork_response',
    '_scan_move_folder_artwork_response_for_runtime',
    '_scan_move_folder_artwork_url',
    '_scan_move_folder_artwork_url_for_runtime',
    '_scan_move_prewarm_artwork',
    '_scan_move_prewarm_artwork_for_runtime',
    '_scan_move_quick_track_entries_from_folder',
    '_scan_move_quick_track_entries_from_folder_for_runtime',
    '_scan_move_reason_label',
    '_scan_move_reason_label_for_runtime',
    '_scan_move_status',
    '_scan_move_status_for_runtime',
    '_scan_move_track_entries_from_folder',
    '_scan_move_track_entries_from_folder_for_runtime',
    '_scan_moves_columns',
    '_scan_moves_columns_for_runtime',
    '_insert_scan_move_row',
    '_insert_scan_move_row_for_runtime',
    'api_scan_history_dedupe',
    'api_scan_history_dedupe_for_runtime',
    'api_scan_history_moves',
    'api_scan_history_moves_for_runtime',
    'api_scan_history_moves_summary',
    'api_scan_history_moves_summary_for_runtime',
    'api_scan_history_restore',
    'api_scan_history_restore_for_runtime',
    'api_scan_move_artwork',
    'api_scan_move_artwork_for_runtime',
    'api_scan_move_detail',
    'api_scan_move_detail_for_runtime',
}


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in _LOCAL_NAMES:
            continue
        globals()[name] = value


def _scan_move_reason_label_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_move_reason_label(*args, **kwargs)

def _scan_move_status_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_move_status(*args, **kwargs)

def _scan_moves_columns_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_moves_columns(*args, **kwargs)

def _insert_scan_move_row_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _insert_scan_move_row(*args, **kwargs)

def _scan_move_active_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_move_active_folder(*args, **kwargs)

def _scan_move_folder_artwork_url_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_move_folder_artwork_url(*args, **kwargs)

def _scan_move_artwork_source_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_move_artwork_source_path(*args, **kwargs)

def _scan_move_prewarm_artwork_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_move_prewarm_artwork(*args, **kwargs)

def _scan_move_folder_artwork_response_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_move_folder_artwork_response(*args, **kwargs)

def _scan_move_quick_track_entries_from_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_move_quick_track_entries_from_folder(*args, **kwargs)

def _scan_move_track_entries_from_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_move_track_entries_from_folder(*args, **kwargs)

def _scan_move_expected_tracks_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_move_expected_tracks(*args, **kwargs)

def _scan_move_detail_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_move_detail_payload(*args, **kwargs)

def api_scan_history_moves_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_scan_history_moves(*args, **kwargs)

def api_scan_history_moves_summary_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_scan_history_moves_summary(*args, **kwargs)

def api_scan_move_artwork_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_scan_move_artwork(*args, **kwargs)

def api_scan_move_detail_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_scan_move_detail(*args, **kwargs)

def api_scan_history_restore_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_scan_history_restore(*args, **kwargs)

def api_scan_history_dedupe_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_scan_history_dedupe(*args, **kwargs)


def _scan_move_reason_label(reason: str | None, details: dict | None = None, move_reason: str | None = None) -> str:
    code = str(reason or "").strip().lower()
    details_map = details if isinstance(details, dict) else {}
    if not code:
        code = str(details_map.get("strict_reject_reason") or details_map.get("classification") or move_reason or "").strip().lower()
    mapping = {
        "track_count_mismatch": "Track count mismatch",
        "broken_album": "Incomplete album",
        "broken_album_missing_tracks": "Missing tracks",
        "provider_no_tracklist": "Provider tracklist missing",
        "track_title_mismatch": "Track titles mismatch",
        "artist_mismatch": "Artist mismatch",
        "album_mismatch": "Album title mismatch",
        "strict_reject": "Strict match rejected",
        "dedupe": "Duplicate edition",
        "incomplete": "Incomplete album",
        "matched_export": "Exported to library",
        "matched_export_conflict": "Destination duplicate",
    }
    if code in mapping:
        return mapping[code]
    human = re.sub(r"[_\\-]+", " ", code).strip()
    return human[:1].upper() + human[1:] if human else ("Duplicate edition" if str(move_reason or "").strip().lower() == "dedupe" else "Incomplete album")


def _scan_moves_columns(cur: sqlite3.Cursor) -> set[str]:
    """Return scan_moves columns for backward-compatible inserts/selects."""
    try:
        cur.execute("PRAGMA table_info(scan_moves)")
        return {str(row[1]) for row in cur.fetchall() if len(row) > 1}
    except Exception:
        return set()


def _insert_scan_move_row(
    cur: sqlite3.Cursor,
    *,
    scan_id: int,
    artist: str,
    album_id: int,
    original_path: str,
    moved_to_path: str,
    size_mb: int,
    moved_at: float | None = None,
    album_title: str = "",
    fmt_text: str = "",
    move_reason: str = "dedupe",
    winner_album_id: int | None = None,
    winner_title: str = "",
    winner_path: str = "",
    decision_source: str = "",
    decision_provider: str = "",
    decision_reason: str = "",
    decision_confidence: float | None = None,
    source_path: str = "",
    destination_path: str = "",
    materialization_strategy: str = "",
    arbitration_result: str = "",
    details: dict | None = None,
) -> None:
    """
    Insert one scan_moves row with graceful schema fallback.
    Older DBs may miss optional columns; we only insert what exists.
    """
    cols_present = _scan_moves_columns(cur)
    moved_ts = float(moved_at or time.time())
    payload = _scan_moves_core.build_move_payload(
        scan_id=scan_id,
        artist=artist,
        album_id=album_id,
        original_path=original_path,
        moved_to_path=moved_to_path,
        size_mb=size_mb,
        moved_at=moved_ts,
        album_title=album_title,
        fmt_text=fmt_text,
        move_reason=move_reason,
        winner_album_id=winner_album_id,
        winner_title=winner_title,
        winner_path=winner_path,
        decision_source=decision_source,
        decision_provider=decision_provider,
        decision_reason=decision_reason,
        decision_confidence=decision_confidence,
        source_path=source_path,
        destination_path=destination_path,
        materialization_strategy=materialization_strategy,
        arbitration_result=arbitration_result,
        details=details,
    )
    ordered_keys = _scan_moves_core.ordered_insert_columns(cols_present)
    if not ordered_keys:
        return
    placeholders = ", ".join(["?"] * len(ordered_keys))
    cols_sql = ", ".join(ordered_keys)
    values = [payload.get(key) for key in ordered_keys]
    cur.execute(
        f"INSERT INTO scan_moves ({cols_sql}) VALUES ({placeholders})",
        values,
    )


def _scan_move_status(restored: bool, original_path: str, moved_to_path: str) -> str:
    if bool(restored):
        return "restored"
    try:
        moved_exists = bool(moved_to_path) and path_for_fs_access(Path(moved_to_path)).exists()
    except Exception:
        moved_exists = False
    if moved_exists:
        return "moved"
    try:
        original_exists = bool(original_path) and path_for_fs_access(Path(original_path)).exists()
    except Exception:
        original_exists = False
    if original_exists:
        return "restored"
    return "missing"


def _scan_move_active_folder(*, original_path: str, moved_to_path: str, restored: bool) -> Optional[Path]:
    candidates: list[str] = []
    if bool(restored):
        candidates.extend([original_path, moved_to_path])
    else:
        candidates.extend([moved_to_path, original_path])
    for raw in candidates:
        txt = str(raw or "").strip()
        if not txt:
            continue
        try:
            folder = path_for_fs_access(Path(txt))
        except Exception:
            continue
        if folder.exists() and folder.is_dir():
            return folder
    return None


def _scan_move_folder_artwork_url(move_id: int, *, target: str = "moved", size: int = 160) -> str:
    try:
        base = request.url_root.rstrip("/")
    except Exception:
        base = ""
    path = f"/api/scan-move/{int(move_id)}/artwork?target={quote_plus(str(target or 'moved'))}&size={int(size)}"
    return f"{base}{path}" if base else path


def _scan_move_artwork_source_path(move_id: int, *, target: str = "moved") -> Optional[Path]:
    target_key = str(target or "moved").strip().lower() or "moved"
    import sqlite3

    con = sqlite3.connect(str(STATE_DB_FILE))
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT move_id,
                   COALESCE(original_path, '') AS original_path,
                   COALESCE(moved_to_path, '') AS moved_to_path,
                   COALESCE(restored, 0) AS restored,
                   COALESCE(winner_path, '') AS winner_path
            FROM scan_moves
            WHERE move_id = ?
            LIMIT 1
            """,
            (int(move_id),),
        )
        row = cur.fetchone()
    finally:
        con.close()
    if not row:
        return None

    original_path = str(row["original_path"] or "").strip()
    moved_to_path = str(row["moved_to_path"] or "").strip()
    winner_path = str(row["winner_path"] or "").strip()
    restored = bool(row["restored"])
    version_seed = "|".join(
        [
            str(int(move_id)),
            target_key,
            "1" if restored else "0",
            original_path,
            moved_to_path,
            winner_path,
            "v1",
        ]
    )
    lookup_key = f"scan_move_artwork:{hashlib.sha1(version_seed.encode('utf-8', errors='ignore')).hexdigest()}"
    cached = _files_cache_get_json(lookup_key)
    if isinstance(cached, dict):
        cached_cover = _existing_file_path(str(cached.get("cover_path") or ""))
        if cached_cover is not None:
            return cached_cover

    folder: Optional[Path] = None
    if target_key == "winner":
        if winner_path:
            try:
                candidate = path_for_fs_access(Path(winner_path))
                if candidate.exists() and candidate.is_dir():
                    folder = candidate
            except Exception:
                folder = None
    else:
        folder = _scan_move_active_folder(
            original_path=original_path,
            moved_to_path=moved_to_path,
            restored=restored,
        )
    if folder is None or not folder.exists() or not folder.is_dir():
        return None

    cover_path = _first_cover_path(folder)
    if cover_path is None or not cover_path.exists() or not cover_path.is_file():
        return None

    _files_cache_set_json(
        lookup_key,
        {
            "folder_path": str(folder),
            "cover_path": str(cover_path),
        },
        ttl=60 * 60,
    )
    return cover_path


def _scan_move_prewarm_artwork(move_id: int, *, target: str = "moved", size: int = 160) -> None:
    cover_path = _scan_move_artwork_source_path(int(move_id), target=target)
    if cover_path is None:
        return
    try:
        _ensure_cached_image_for_path(cover_path, kind="album", max_px=max(64, min(1024, int(size or 160))))
    except Exception:
        logging.debug(
            "Scan move artwork prewarm failed for move_id=%s target=%s",
            int(move_id),
            str(target or "moved"),
            exc_info=True,
        )


def _scan_move_folder_artwork_response(folder: Path, *, size: int) -> Response:
    if not folder.exists() or not folder.is_dir():
        return _transparent_png_response(max_age=0, revalidate=True)
    cover_path = _first_cover_path(folder)
    if cover_path and cover_path.exists() and cover_path.is_file():
        cached = _ensure_cached_image_for_path(cover_path, kind="album", max_px=size)
        return _serve_image_file_cached(cached or cover_path, max_age=0, revalidate=True)
    return _transparent_png_response(max_age=0, revalidate=True)


def _scan_move_quick_track_entries_from_folder(
    folder: Path,
    *,
    artist_name: str,
    album_title: str,
) -> list[dict[str, Any]]:
    if not folder or not folder.exists() or not folder.is_dir():
        return []
    ordered_paths = _files_collect_ordered_audio_paths(folder, [])
    if not ordered_paths:
        return []
    out: list[dict[str, Any]] = []
    for index, audio_path in enumerate(ordered_paths, start=1):
        display = _track_display_fields_from_sources(
            raw_title=str(audio_path.stem or f"Track {index}"),
            file_path=str(audio_path),
            fallback_disc=1,
            fallback_track=index,
            album_hint=album_title,
            artist_hint=artist_name,
        )
        out.append(
            {
                "title": str(display.get("display_title") or audio_path.stem or f"Track {index}").strip(),
                "track_num": max(1, int(display.get("display_track_num") or index)),
                "disc_num": max(1, int(display.get("display_disc_num") or 1)),
                "disc_label": str(display.get("display_disc_label") or "").strip(),
                "duration_sec": 0,
                "file_path": str(audio_path),
            }
        )
    max_disc = max((int(t.get("disc_num") or 1) for t in out), default=1)
    for t in out:
        if not str(t.get("disc_label") or "").strip():
            t["disc_label"] = f"Disc {int(t.get('disc_num') or 1)}" if max_disc > 1 else ""
    out.sort(
        key=lambda t: (
            int(t.get("disc_num") or 1),
            int(t.get("track_num") or 0),
            str(t.get("file_path") or ""),
        )
    )
    return out


def _scan_move_track_entries_from_folder(
    folder: Path,
    *,
    artist_name: str,
    album_title: str,
    metadata_source: str = "",
    details: dict | None = None,
    quick: bool = True,
) -> list[dict[str, Any]]:
    if not folder or not folder.exists() or not folder.is_dir():
        return []
    ordered_paths = _files_collect_ordered_audio_paths(folder, [])
    if not ordered_paths:
        return []
    if quick:
        return _scan_move_quick_track_entries_from_folder(
            folder,
            artist_name=artist_name,
            album_title=album_title,
        )
    first_tags: dict[str, Any] = {}
    track_objs: list[dict[str, Any]] = []
    for index, audio_path in enumerate(ordered_paths, start=1):
        try:
            tags = dict(extract_tags(audio_path) or {})
        except Exception:
            tags = {}
        if index == 1:
            first_tags = dict(tags)
        raw_dur = tags.get("duration") or tags.get("length") or tags.get("dur") or 0
        try:
            dur_ms = int(float(raw_dur) * 1000.0) if float(raw_dur or 0) < 5000 else int(float(raw_dur or 0))
        except Exception:
            dur_ms = 0
        track_objs.append(
            {
                "title": str(tags.get("title") or tags.get("tit2") or tags.get("track_title") or "").strip(),
                "idx": _parse_int_loose(tags.get("track") or tags.get("tracknumber") or tags.get("trck"), index) or index,
                "disc": _parse_int_loose(tags.get("disc") or tags.get("discnumber") or tags.get("disc_num") or tags.get("tpas"), 1) or 1,
                "dur": dur_ms,
            }
        )
    item = {
        "artist": artist_name,
        "title_raw": album_title,
        "album_title": album_title,
        "tracks": track_objs,
        "meta": first_tags,
        "musicbrainz_id": str(first_tags.get("musicbrainz_releasegroupid") or first_tags.get("musicbrainz_releaseid") or "").strip(),
        "discogs_release_id": str(first_tags.get("discogs_release_id") or "").strip(),
        "lastfm_album_mbid": str(first_tags.get("lastfm_album_mbid") or "").strip(),
        "bandcamp_album_url": str(first_tags.get("bandcamp_album_url") or "").strip(),
        "strict_match_provider": str((details or {}).get("strict_match_provider") or metadata_source or first_tags.get(PMDA_MATCH_PROVIDER_TAG) or "").strip(),
        "primary_metadata_source": str(metadata_source or (details or {}).get("strict_match_provider") or first_tags.get(PMDA_MATCH_PROVIDER_TAG) or "").strip(),
        "metadata_source": str(metadata_source or "").strip(),
        "br": 0,
        "sr": 0,
        "bd": 0,
    }
    tracks = _files_build_track_entries_from_item(item, folder)
    max_disc = max((int(t.get("disc_num") or 1) for t in tracks), default=1)
    for t in tracks:
        if not str(t.get("disc_label") or "").strip():
            t["disc_label"] = f"Disc {int(t.get('disc_num') or 1)}" if max_disc > 1 else ""
    return tracks


def _scan_move_expected_tracks(
    *,
    folder: Path | None,
    artist_name: str,
    album_title: str,
    metadata_source: str,
    details: dict | None = None,
    cache_only: bool = True,
) -> list[dict[str, Any]]:
    details_map = details if isinstance(details, dict) else {}
    first_tags: dict[str, Any] = {}
    if folder and folder.exists() and folder.is_dir():
        ordered_paths = _files_collect_ordered_audio_paths(folder, [])
        if ordered_paths:
            try:
                first_tags = dict(extract_tags(ordered_paths[0]) or {})
            except Exception:
                first_tags = {}
    titles = _provider_track_titles_cached(
        artist_name=artist_name,
        album_title=album_title,
        metadata_source=str(metadata_source or details_map.get("strict_match_provider") or first_tags.get(PMDA_MATCH_PROVIDER_TAG) or ""),
        musicbrainz_release_group_id=str(
            first_tags.get("musicbrainz_releasegroupid")
            or first_tags.get("musicbrainz_releaseid")
            or details_map.get("musicbrainz_release_group_id")
            or details_map.get("musicbrainz_release_id")
            or ""
        ).strip(),
        discogs_release_id=str(first_tags.get("discogs_release_id") or details_map.get("discogs_release_id") or "").strip(),
        lastfm_album_mbid=str(first_tags.get("lastfm_album_mbid") or details_map.get("lastfm_album_mbid") or "").strip(),
        bandcamp_album_url=str(first_tags.get("bandcamp_album_url") or details_map.get("bandcamp_album_url") or "").strip(),
        edition_payload={"meta": first_tags},
        cache_only=bool(cache_only),
    )
    return [
        {
            "index": index,
            "title": str(title or "").strip() or f"Track {index}",
        }
        for index, title in enumerate(titles, start=1)
    ]


def _scan_move_detail_payload(move_id: int) -> Optional[dict[str, Any]]:
    import sqlite3

    con = sqlite3.connect(str(STATE_DB_FILE))
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT move_id, scan_id, artist, album_id, original_path, moved_to_path, size_mb, moved_at, restored,
                   COALESCE(album_title, '') AS album_title,
                   COALESCE(fmt_text, '') AS fmt_text,
                   COALESCE(move_reason, 'dedupe') AS move_reason,
                   winner_album_id,
                   COALESCE(winner_title, '') AS winner_title,
                   COALESCE(winner_path, '') AS winner_path,
                   COALESCE(decision_source, '') AS decision_source,
                   COALESCE(decision_provider, '') AS decision_provider,
                   COALESCE(decision_reason, '') AS decision_reason,
                   decision_confidence,
                   COALESCE(details_json, '{}') AS details_json
            FROM scan_moves
            WHERE move_id = ?
            LIMIT 1
            """,
            (int(move_id),),
        )
        row = cur.fetchone()
    finally:
        con.close()
    if not row:
        return None
    try:
        details = json.loads(str(row["details_json"] or "{}"))
        if not isinstance(details, dict):
            details = {}
    except Exception:
        details = {}

    move_reason = str(row["move_reason"] or "dedupe").strip().lower() or "dedupe"
    status = _scan_move_status(bool(row["restored"]), str(row["original_path"] or ""), str(row["moved_to_path"] or ""))
    reason_label = _scan_move_reason_label(str(row["decision_reason"] or ""), details, move_reason)
    moved_folder = _scan_move_active_folder(
        original_path=str(row["original_path"] or ""),
        moved_to_path=str(row["moved_to_path"] or ""),
        restored=bool(row["restored"]),
    )
    winner_folder = None
    winner_path_raw = str(row["winner_path"] or "").strip()
    if winner_path_raw:
        try:
            candidate = path_for_fs_access(Path(winner_path_raw))
            if candidate.exists() and candidate.is_dir():
                winner_folder = candidate
        except Exception:
            winner_folder = None

    moved_tracks = _scan_move_track_entries_from_folder(
        moved_folder,
        artist_name=str(row["artist"] or ""),
        album_title=str(row["album_title"] or ""),
        metadata_source=str(row["decision_provider"] or ""),
        details=details,
    ) if moved_folder else []
    winner_details = details.get("winner") if isinstance(details.get("winner"), dict) else {}
    winner_tracks = _scan_move_track_entries_from_folder(
        winner_folder,
        artist_name=str(row["artist"] or ""),
        album_title=str(row["winner_title"] or row["album_title"] or ""),
        metadata_source=str((winner_details or {}).get("strict_match_provider") or row["decision_provider"] or ""),
        details=(winner_details or {}),
    ) if winner_folder else []

    payload: dict[str, Any] = {
        "move_id": int(row["move_id"] or 0),
        "scan_id": int(row["scan_id"] or 0),
        "artist": str(row["artist"] or ""),
        "album_id": int(row["album_id"] or 0),
        "album_title": str(row["album_title"] or ""),
        "move_reason": move_reason,
        "reason_label": reason_label,
        "status": status,
        "moved_at": float(row["moved_at"] or 0),
        "decision_source": str(row["decision_source"] or ""),
        "decision_provider": str(row["decision_provider"] or ""),
        "decision_reason": str(row["decision_reason"] or ""),
        "decision_confidence": float(row["decision_confidence"]) if row["decision_confidence"] is not None else None,
        "details": details,
        "moved": {
            "path": str(moved_folder or row["moved_to_path"] or row["original_path"] or ""),
            "thumb_url": _scan_move_folder_artwork_url(int(row["move_id"] or 0), target="moved", size=256),
            "tracks": moved_tracks,
            "track_count": len(moved_tracks),
            "fmt_text": str(row["fmt_text"] or ""),
        },
    }
    _scan_move_prewarm_artwork(int(row["move_id"] or 0), target="moved", size=256)
    if move_reason == "dedupe":
        analysis = details.get("analysis") if isinstance(details.get("analysis"), dict) else {}
        payload["winner"] = {
            "album_id": int(row["winner_album_id"]) if row["winner_album_id"] is not None else None,
            "album_title": str(row["winner_title"] or ""),
            "path": str(winner_folder or row["winner_path"] or ""),
            "thumb_url": _scan_move_folder_artwork_url(int(row["move_id"] or 0), target="winner", size=256) if winner_folder else None,
            "tracks": winner_tracks,
            "track_count": len(winner_tracks),
            "analysis": analysis,
        }
        if winner_folder:
            _scan_move_prewarm_artwork(int(row["move_id"] or 0), target="winner", size=256)
    else:
        payload["incomplete"] = {
            "expected_track_count": int(details.get("expected_track_count") or 0),
            "actual_track_count": int(details.get("actual_track_count") or 0),
            "missing_indices": list(details.get("missing_indices") or []),
            "missing_required_tags": list(details.get("missing_required_tags") or []),
            "strict_match_provider": str(details.get("strict_match_provider") or ""),
            "strict_reject_reason": str(details.get("strict_reject_reason") or ""),
            "strict_tracklist_score": float(details.get("strict_tracklist_score") or 0.0),
            "expected_tracks": _scan_move_expected_tracks(
                folder=moved_folder,
                artist_name=str(row["artist"] or ""),
                album_title=str(row["album_title"] or ""),
                metadata_source=str(row["decision_provider"] or ""),
                details=details,
            ),
        }
    return payload


def api_scan_history_moves(scan_id):
    """Return all moves for a specific scan (with review metadata when available)."""
    reason_filter = str(request.args.get("reason") or "").strip().lower()
    status_filter = str(request.args.get("status") or "all").strip().lower() or "all"
    if status_filter not in {"all", "active", "restored"}:
        status_filter = "all"
    reason_allowed = {"dedupe", "incomplete"}
    if reason_filter not in reason_allowed:
        reason_filter = ""

    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE))
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    move_cols = _scan_moves_columns(cur)
    base_cols = [
        "move_id",
        "scan_id",
        "artist",
        "album_id",
        "original_path",
        "moved_to_path",
        "size_mb",
        "moved_at",
        "restored",
    ]
    optional_cols = [
        "album_title",
        "fmt_text",
        "move_reason",
        "winner_album_id",
        "winner_title",
        "winner_path",
        "decision_source",
        "decision_provider",
        "decision_reason",
        "decision_confidence",
        "details_json",
    ]
    select_cols = [c for c in base_cols if c in move_cols] + [c for c in optional_cols if c in move_cols]
    where_parts = ["scan_id = ?"]
    params: list[Any] = [int(scan_id)]
    if reason_filter:
        if "move_reason" in move_cols:
            where_parts.append("LOWER(COALESCE(move_reason, 'dedupe')) = ?")
            params.append(reason_filter)
    if status_filter == "active":
        where_parts.append("COALESCE(restored, 0) = 0")
    elif status_filter == "restored":
        where_parts.append("COALESCE(restored, 0) = 1")
    where_sql = " AND ".join(where_parts)
    cur.execute(
        f"""
        SELECT {", ".join(select_cols)}
        FROM scan_moves
        WHERE {where_sql}
        ORDER BY moved_at DESC
        """,
        tuple(params),
    )
    rows = cur.fetchall()
    con.close()

    moves = []
    for row in rows:
        m = {
            "move_id": int(row["move_id"]),
            "scan_id": int(row["scan_id"]),
            "artist": str(row["artist"] or ""),
            "album_id": int(row["album_id"] or 0),
            "original_path": str(row["original_path"] or ""),
            "moved_to_path": str(row["moved_to_path"] or ""),
            "size_mb": int(row["size_mb"] or 0),
            "moved_at": float(row["moved_at"] or 0),
            "restored": bool(row["restored"]),
            "album_title": str(row["album_title"] or "") if "album_title" in row.keys() else "",
            "fmt_text": str(row["fmt_text"] or "") if "fmt_text" in row.keys() else "",
            "move_reason": (
                str(row["move_reason"] or "").strip().lower() or "dedupe"
                if "move_reason" in row.keys()
                else "dedupe"
            ),
            "winner_album_id": int(row["winner_album_id"]) if ("winner_album_id" in row.keys() and row["winner_album_id"] is not None) else None,
            "winner_title": str(row["winner_title"] or "") if "winner_title" in row.keys() else "",
            "winner_path": str(row["winner_path"] or "") if "winner_path" in row.keys() else "",
            "decision_source": str(row["decision_source"] or "") if "decision_source" in row.keys() else "",
            "decision_provider": str(row["decision_provider"] or "") if "decision_provider" in row.keys() else "",
            "decision_reason": str(row["decision_reason"] or "") if "decision_reason" in row.keys() else "",
            "decision_confidence": float(row["decision_confidence"]) if ("decision_confidence" in row.keys() and row["decision_confidence"] is not None) else None,
        }
        details_obj: dict = {}
        if "details_json" in row.keys() and row["details_json"]:
            try:
                parsed = json.loads(row["details_json"])
                if isinstance(parsed, dict):
                    details_obj = parsed
            except Exception:
                details_obj = {}
        m["details"] = details_obj
        m["status"] = _scan_move_status(m["restored"], m["original_path"], m["moved_to_path"])
        m["reason_label"] = _scan_move_reason_label(m.get("decision_reason"), details_obj, m.get("move_reason"))
        m["thumb_url"] = _scan_move_folder_artwork_url(int(m["move_id"]), target="moved", size=128)
        m["winner_thumb_url"] = (
            _scan_move_folder_artwork_url(int(m["move_id"]), target="winner", size=128)
            if str(m.get("winner_path") or "").strip()
            else None
        )
        _scan_move_prewarm_artwork(int(m["move_id"]), target="moved", size=128)
        if m["winner_thumb_url"]:
            _scan_move_prewarm_artwork(int(m["move_id"]), target="winner", size=128)
        moves.append(m)

    return jsonify(moves)


def api_scan_history_moves_summary(scan_id: int):
    import sqlite3

    con = sqlite3.connect(str(STATE_DB_FILE))
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("PRAGMA table_info(scan_moves)")
    cols = {str(r[1]) for r in cur.fetchall() if len(r) > 1}
    has_move_reason = "move_reason" in cols
    reason_expr = "LOWER(COALESCE(move_reason, 'dedupe'))" if has_move_reason else "'dedupe'"

    cur.execute(
        f"""
        SELECT
            COUNT(*) AS total_moved,
            COALESCE(SUM(CASE WHEN COALESCE(restored, 0) = 0 THEN 1 ELSE 0 END), 0) AS pending,
            COALESCE(SUM(CASE WHEN COALESCE(restored, 0) = 1 THEN 1 ELSE 0 END), 0) AS restored,
            COALESCE(SUM(COALESCE(size_mb, 0)), 0) AS size_mb,
            MIN(moved_at) AS first_moved_at,
            MAX(moved_at) AS last_moved_at
        FROM scan_moves
        WHERE scan_id = ?
        """,
        (int(scan_id),),
    )
    row = cur.fetchone() or {}

    cur.execute(
        f"""
        SELECT {reason_expr} AS move_reason,
               COUNT(*) AS total,
               COALESCE(SUM(CASE WHEN COALESCE(restored, 0) = 0 THEN 1 ELSE 0 END), 0) AS pending,
               COALESCE(SUM(CASE WHEN COALESCE(restored, 0) = 1 THEN 1 ELSE 0 END), 0) AS restored,
               COALESCE(SUM(COALESCE(size_mb, 0)), 0) AS size_mb
        FROM scan_moves
        WHERE scan_id = ?
        GROUP BY {reason_expr}
        ORDER BY move_reason ASC
        """,
        (int(scan_id),),
    )
    by_reason_rows = cur.fetchall()
    con.close()

    by_reason: dict[str, dict[str, Any]] = {}
    for rr in by_reason_rows:
        reason = str(rr["move_reason"] or "").strip().lower() or "dedupe"
        by_reason[reason] = {
            "total_moved": int(rr["total"] or 0),
            "pending": int(rr["pending"] or 0),
            "restored": int(rr["restored"] or 0),
            "size_mb": int(rr["size_mb"] or 0),
        }
    return jsonify(
        {
            "scan_id": int(scan_id),
            "total_moved": int(row["total_moved"] or 0),
            "pending": int(row["pending"] or 0),
            "restored": int(row["restored"] or 0),
            "size_mb": int(row["size_mb"] or 0),
            "first_moved_at": (float(row["first_moved_at"]) if row["first_moved_at"] is not None else None),
            "last_moved_at": (float(row["last_moved_at"]) if row["last_moved_at"] is not None else None),
            "by_reason": by_reason,
        }
    )


def api_scan_move_artwork(move_id: int):
    size = max(64, min(1024, _parse_int_loose(request.args.get("size"), 320)))
    target = str(request.args.get("target") or "moved").strip().lower() or "moved"
    cover_path = _scan_move_artwork_source_path(int(move_id), target=target)
    if cover_path is None:
        return _transparent_png_response(max_age=0, revalidate=True)
    try:
        cached = _ensure_cached_image_for_path(cover_path, kind="album", max_px=size)
    except Exception:
        cached = cover_path
    return _serve_image_file_cached(cached or cover_path, max_age=300, revalidate=True)


def api_scan_move_detail(move_id: int):
    payload = _scan_move_detail_payload(int(move_id))
    if not isinstance(payload, dict):
        return jsonify({"error": "Move not found"}), 404
    return jsonify(payload)


def api_scan_history_restore(scan_id):
    """Restore moved files to their original location."""
    if library_is_audit_mode():
        admin_gate = _require_admin_json()
        if admin_gate is not None:
            return admin_gate
    data = request.get_json() or {}
    move_ids = data.get("move_ids", [])
    restore_all = data.get("all", False)

    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()

    if restore_all:
        cur.execute("""
            SELECT move_id, original_path, moved_to_path, artist
            FROM scan_moves
            WHERE scan_id = ? AND restored = 0
        """, (scan_id,))
    else:
        if not move_ids:
            return jsonify({"error": "No move_ids provided"}), 400
        placeholders = ",".join("?" * len(move_ids))
        cur.execute(f"""
            SELECT move_id, original_path, moved_to_path, artist
            FROM scan_moves
            WHERE scan_id = ? AND move_id IN ({placeholders}) AND restored = 0
        """, (scan_id, *move_ids))

    rows = cur.fetchall()
    if not rows:
        con.close()
        return jsonify({"error": "No moves found to restore"}), 404

    artists_to_refresh = set()
    restored_count = 0
    restored_paths: List[dict] = []

    for move_id, original_path, moved_to_path, artist in rows:
        src = Path(moved_to_path)
        dst = Path(original_path)

        if not src.exists():
            logging.warning(f"Restore: source {src} does not exist, skipping")
            continue

        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            safe_move(str(src), str(dst))
            artists_to_refresh.add(artist)
            restored_count += 1
            restored_paths.append({"from": moved_to_path, "to": original_path})

            # Mark as restored
            cur.execute("UPDATE scan_moves SET restored = 1 WHERE move_id = ?", (move_id,))
        except Exception as e:
            logging.error(f"Restore: failed to restore {src} → {dst}: {e}")
            continue

    con.commit()
    con.close()

    return jsonify({
        "restored": restored_count,
        "artists_refreshed": len(artists_to_refresh),
        "restored_paths": restored_paths,
    })


def api_scan_history_dedupe(scan_id):
    """Manually dedupe albums from a previous scan."""
    if library_is_audit_mode():
        admin_gate = _require_admin_json()
        if admin_gate is not None:
            return admin_gate
    # Load scan results from DB (dict artist -> list of groups)
    scan_results = load_scan_from_db()
    if not scan_results:
        return jsonify({"error": "No scan results found for this scan"}), 404

    flat_groups = [g for groups in scan_results.values() for g in groups]
    if not flat_groups:
        return jsonify({"error": "No duplicate groups to dedupe"}), 404

    # Start deduplication
    background_dedupe(flat_groups)
    return jsonify({"status": "ok", "message": "Deduplication started"})
