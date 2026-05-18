"""Runtime-backed operator tools handlers.

This module contains the heavy trash-release curation logic extracted from
``pmda.py`` while keeping the public tools API routes stable.
"""

from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

_RUNTIME: Any | None = None
_EXTRACTED_NAMES = {
    '_trash_release_safe_json',
    '_trash_release_tags_text',
    '_trash_release_compilation_flag',
    '_trash_release_candidate_from_album_row',
    '_trash_release_candidates_snapshot',
    '_trash_release_fetch_library_album_row',
    '_trash_release_destination',
    '_record_library_curation_action',
    'api_tools_trash_releases',
    'api_tools_trash_releases_action',
}


def _runtime_module() -> Any:
    if _RUNTIME is None:
        raise RuntimeError("PMDA runtime is not bound")
    return _RUNTIME


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one tools request."""
    global _RUNTIME
    _RUNTIME = runtime
    globals().update({key: value for key, value in vars(runtime).items() if key not in _EXTRACTED_NAMES})

def _trash_release_safe_json(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    if isinstance(value, (list, dict)):
        return value
    try:
        parsed = json.loads(str(value))
    except Exception:
        return fallback
    return parsed


def _trash_release_tags_text(row: dict[str, Any]) -> str:
    chunks: list[str] = []
    for key, fallback in (("tags_json", []), ("primary_tags_json", {})):
        parsed = _trash_release_safe_json(row.get(key), fallback)
        if isinstance(parsed, list):
            chunks.extend(str(item or "").strip() for item in parsed if str(item or "").strip())
        elif isinstance(parsed, dict):
            for field in (
                "genre",
                "genres",
                "style",
                "styles",
                "albumartist",
                "album artist",
                "comment",
                "grouping",
                "album_grouping",
            ):
                value = parsed.get(field)
                if isinstance(value, list):
                    chunks.extend(str(item or "").strip() for item in value if str(item or "").strip())
                elif value not in (None, ""):
                    chunks.append(str(value).strip())
    return " ".join(chunk for chunk in chunks if chunk).strip()


def _trash_release_compilation_flag(row: dict[str, Any]) -> bool:
    primary = _trash_release_safe_json(row.get("primary_tags_json"), {})
    tags = _trash_release_safe_json(row.get("tags_json"), [])
    values: list[str] = []
    if isinstance(primary, dict):
        for key in ("compilation", "itunescompilation"):
            values.append(str(primary.get(key) or "").strip().lower())
    if isinstance(tags, list):
        values.extend(str(item or "").strip().lower() for item in tags if str(item or "").strip())
    return any(value in {"1", "true", "yes", "compilation"} for value in values)


def _trash_release_candidate_from_album_row(row: dict[str, Any], *, min_score: int | None = None) -> Optional[dict[str, Any]]:
    artist_name = str(row.get("artist_name") or row.get("artist") or "").strip()
    album_title = _sanitize_album_title_display(str(row.get("album_title") or row.get("title") or "").strip())
    folder_path = str(row.get("folder_path") or "").strip()
    if not artist_name or not album_title or not folder_path:
        return None

    genre_text = str(row.get("genre") or "").strip()
    label_text = str(row.get("label") or "").strip()
    tags_text = _trash_release_tags_text(row)
    track_count = int(_parse_int_loose(row.get("track_count"), 0) or 0)
    metadata_source = _normalize_identity_provider(str(row.get("metadata_source") or "")) or None

    signals: list[dict[str, Any]] = []

    def add_signal(kind: str, label: str, score: int, *, matched: str = "") -> None:
        signals.append(
            {
                "kind": str(kind or "").strip(),
                "label": str(label or "").strip(),
                "score": int(score or 0),
                "matched": str(matched or "").strip() or None,
            }
        )

    artist_lower = artist_name.lower()
    title_lower = album_title.lower()
    folder_lower = folder_path.lower()
    genre_lower = genre_text.lower()
    label_lower = label_text.lower()
    tags_lower = tags_text.lower()

    if artist_lower in _TRASH_RELEASE_GENERIC_ARTISTS:
        add_signal("generic_artist", "Generic / compilation-style artist name", 2, matched=artist_name)

    compilation_flag = _trash_release_compilation_flag(row)
    if compilation_flag:
        add_signal("compilation", "Compilation tag detected", 2)

    searchable_fields = (
        title_lower,
        folder_lower,
        genre_lower,
        label_lower,
        tags_lower,
    )
    for rule in _TRASH_RELEASE_RULES:
        matched_pattern = ""
        for pattern in (rule.get("patterns") or []):
            pattern_text = str(pattern or "").strip().lower()
            if not pattern_text:
                continue
            if any(pattern_text in field for field in searchable_fields):
                matched_pattern = pattern_text
                break
        if matched_pattern:
            add_signal(str(rule.get("category") or "trash"), str(rule.get("label") or "Suspicious wording"), int(rule.get("weight") or 0), matched=matched_pattern)

    if track_count >= 30 and (compilation_flag or artist_lower in _TRASH_RELEASE_GENERIC_ARTISTS):
        add_signal("oversized_compilation", "Large tracklist for a compilation-style release", 1, matched=str(track_count))

    score = int(sum(max(0, int(signal.get("score") or 0)) for signal in signals))
    default_min_score = int(getattr(_runtime_module(), "_TRASH_RELEASE_MIN_SCORE_DEFAULT", 6) or 6)
    if score < max(1, int(min_score or default_min_score)):
        return None

    dominant = max(signals, key=lambda item: (int(item.get("score") or 0), str(item.get("kind") or "")))
    return {
        "album_id": int(_parse_int_loose(row.get("album_id") or row.get("id"), 0) or 0),
        "artist": artist_name,
        "album_title": album_title,
        "folder_path": folder_path,
        "year": int(_parse_int_loose(row.get("year"), 0) or 0) or None,
        "genre": genre_text or None,
        "label": label_text or None,
        "track_count": track_count,
        "metadata_source": metadata_source,
        "score": score,
        "category": str(dominant.get("kind") or "trash"),
        "reasons": [str(signal.get("label") or "").strip() for signal in signals if str(signal.get("label") or "").strip()],
        "signals": signals,
        "has_cover": bool(row.get("has_cover")),
    }


def _trash_release_candidates_snapshot(*, limit: int = 24, min_score: int | None = None) -> dict[str, Any]:
    limit = max(1, min(int(limit or 24), 200))
    default_min_score = int(getattr(_runtime_module(), "_TRASH_RELEASE_MIN_SCORE_DEFAULT", 6) or 6)
    min_score = max(1, int(min_score or default_min_score))
    if _get_library_mode() != "files":
        return {
            "available": False,
            "reason": "files_mode_only",
            "generated_at": time.time(),
            "total": 0,
            "summary": {"by_category": {}, "top_score": 0},
            "candidates": [],
        }
    conn = _files_pg_connect(acquire_timeout_sec=0.75)
    if conn is None:
        return {
            "available": False,
            "reason": "postgres_unavailable",
            "generated_at": time.time(),
            "total": 0,
            "summary": {"by_category": {}, "top_score": 0},
            "candidates": [],
        }
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    alb.id AS album_id,
                    COALESCE(art.name, '') AS artist_name,
                    COALESCE(alb.title, '') AS album_title,
                    COALESCE(alb.folder_path, '') AS folder_path,
                    COALESCE(alb.year, 0) AS year,
                    COALESCE(alb.genre, '') AS genre,
                    COALESCE(alb.label, '') AS label,
                    COALESCE(alb.tags_json, '[]') AS tags_json,
                    COALESCE(alb.primary_tags_json, '{{}}') AS primary_tags_json,
                    COALESCE(alb.track_count, 0) AS track_count,
                    COALESCE(alb.metadata_source, '') AS metadata_source,
                    COALESCE(alb.has_cover, FALSE) AS has_cover
                FROM files_albums alb
                JOIN files_artists art ON art.id = alb.artist_id
                WHERE {_library_album_scope_where('library', 'alb')}
                ORDER BY COALESCE(alb.updated_at, alb.created_at) DESC, alb.id DESC
                LIMIT 800
                """
            )
            columns = [getattr(desc, "name", desc[0]) for desc in (cur.description or [])]
            rows = [
                {str(columns[idx]): value for idx, value in enumerate(values)}
                for values in (cur.fetchall() or [])
            ]
    finally:
        try:
            conn.close()
        except Exception:
            pass

    candidates: list[dict[str, Any]] = []
    for row in rows:
        candidate = _trash_release_candidate_from_album_row(row, min_score=min_score)
        if candidate:
            candidates.append(candidate)
    candidates.sort(
        key=lambda item: (
            -int(item.get("score") or 0),
            str(item.get("artist") or "").lower(),
            str(item.get("album_title") or "").lower(),
        )
    )
    by_category: dict[str, int] = {}
    for item in candidates:
        category = str(item.get("category") or "trash").strip().lower() or "trash"
        by_category[category] = by_category.get(category, 0) + 1
    return {
        "available": True,
        "generated_at": time.time(),
        "total": len(candidates),
        "summary": {
            "by_category": dict(sorted(by_category.items(), key=lambda entry: (-entry[1], entry[0]))),
            "top_score": max((int(item.get("score") or 0) for item in candidates), default=0),
        },
        "candidates": candidates[:limit],
    }


def _trash_release_fetch_library_album_row(album_id: int) -> Optional[dict[str, Any]]:
    aid = max(0, int(album_id or 0))
    if aid <= 0 or _get_library_mode() != "files":
        return None
    conn = _files_pg_connect(acquire_timeout_sec=0.75)
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    alb.id AS album_id,
                    COALESCE(art.name, '') AS artist_name,
                    COALESCE(alb.title, '') AS album_title,
                    COALESCE(alb.folder_path, '') AS folder_path,
                    COALESCE(alb.year, 0) AS year,
                    COALESCE(alb.genre, '') AS genre,
                    COALESCE(alb.label, '') AS label,
                    COALESCE(alb.tags_json, '[]') AS tags_json,
                    COALESCE(alb.primary_tags_json, '{{}}') AS primary_tags_json,
                    COALESCE(alb.track_count, 0) AS track_count,
                    COALESCE(alb.metadata_source, '') AS metadata_source,
                    COALESCE(alb.has_cover, FALSE) AS has_cover
                FROM files_albums alb
                JOIN files_artists art ON art.id = alb.artist_id
                WHERE alb.id = %s
                  AND {_library_album_scope_where('library', 'alb')}
                LIMIT 1
                """,
                (aid,),
            )
            row = cur.fetchone()
            if not row:
                return None
            columns = [getattr(desc, "name", desc[0]) for desc in (cur.description or [])]
            return {str(columns[idx]): value for idx, value in enumerate(row)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _trash_release_destination(folder_path: str, *, artist_name: str, album_title: str) -> Path:
    letter, artist_dir, album_dir = _quarantine_artist_album_parts(
        Path(folder_path),
        artist_hint=artist_name,
        album_hint=album_title,
    )
    base = DUPE_ROOT / "_trash_releases" / letter / artist_dir / album_dir
    candidate = base
    suffix = 1
    while candidate.exists():
        candidate = base.parent / f"{base.name} ({suffix})"
        suffix += 1
    return candidate


def _record_library_curation_action(
    *,
    album_id: int,
    artist: str,
    album_title: str,
    folder_path: str,
    action: str,
    destination_path: str = "",
    status: str = "completed",
    reason_payload: Any = None,
    user_id: int = 0,
    username: str = "",
) -> int:
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO library_curation_actions (
                album_id, artist, album_title, folder_path, action, destination_path,
                status, reason_json, created_at, user_id, username
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(album_id or 0),
                str(artist or ""),
                str(album_title or ""),
                str(folder_path or ""),
                str(action or ""),
                str(destination_path or ""),
                str(status or "completed"),
                json.dumps(reason_payload or [], default=str),
                float(time.time()),
                int(user_id or 0),
                str(username or ""),
            ),
        )
        action_id = int(cur.lastrowid or 0)
        con.commit()
        con.close()
        return action_id
    except Exception:
        logging.debug("Failed to record library curation action for album_id=%s", int(album_id or 0), exc_info=True)
        return 0


def api_tools_trash_releases():
    admin_gate = _require_admin_json()
    if admin_gate is not None:
        return admin_gate
    limit = max(1, min(200, _parse_int_loose(request.args.get("limit"), 24)))
    min_score = max(1, min(20, _parse_int_loose(request.args.get("min_score"), _TRASH_RELEASE_MIN_SCORE_DEFAULT)))
    payload = _trash_release_candidates_snapshot(limit=limit, min_score=min_score)
    if bool(payload.get("available")):
        base_url = request.url_root.rstrip("/")
        for item in (payload.get("candidates") or []):
            album_id = int(item.get("album_id") or 0)
            if album_id > 0 and bool(item.get("has_cover")):
                item["thumb_url"] = f"{base_url}/api/library/files/album/{album_id}/cover?size=160"
            else:
                item["thumb_url"] = None
    return jsonify(payload)


def api_tools_trash_releases_action():
    admin_gate = _require_admin_json()
    if admin_gate is not None:
        return admin_gate
    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action") or "").strip().lower()
    album_id = max(0, int(_parse_int_loose(payload.get("album_id"), 0) or 0))
    if action not in {"move_to_dupes", "delete_from_disk"}:
        return jsonify({"error": "action must be move_to_dupes or delete_from_disk"}), 400
    if album_id <= 0:
        return jsonify({"error": "album_id is required"}), 400

    row = _runtime_module()._trash_release_fetch_library_album_row(album_id)
    if not row:
        return jsonify({"error": "Album not found in the visible library"}), 404
    candidate = _runtime_module()._trash_release_candidate_from_album_row(row)
    if not candidate:
        return jsonify({"error": "Album is not currently flagged as a trash-release candidate"}), 409

    folder_path = str(row.get("folder_path") or "").strip()
    folder = path_for_fs_access(Path(folder_path))
    if not folder.exists() or not folder.is_dir():
        return jsonify({"error": "Album folder is missing on disk"}), 404

    user = _current_user_or_empty()
    user_id = int(user.get("id") or 0)
    username = str(user.get("username") or "").strip()
    destination_path = ""
    try:
        if action == "move_to_dupes":
            destination = _runtime_module()._trash_release_destination(
                str(folder),
                artist_name=str(row.get("artist_name") or ""),
                album_title=str(row.get("album_title") or ""),
            )
            destination.parent.mkdir(parents=True, exist_ok=True)
            safe_move(str(folder), str(destination))
            destination_path = str(destination)
            message = f"Moved to {destination_path}"
        else:
            shutil.rmtree(folder)
            message = f"Deleted {folder}"
        _files_forget_album_folder_global(folder_path)
        _files_cache_invalidate_all()
        action_id = _record_library_curation_action(
            album_id=album_id,
            artist=str(row.get("artist_name") or ""),
            album_title=str(row.get("album_title") or ""),
            folder_path=folder_path,
            action=action,
            destination_path=destination_path,
            status="completed",
            reason_payload=candidate.get("signals") or [],
            user_id=user_id,
            username=username,
        )
        logging.info(
            "[Tools] admin curation action=%s album_id=%s artist=%s title=%s from=%s to=%s user=%s",
            action,
            album_id,
            str(row.get("artist_name") or ""),
            str(row.get("album_title") or ""),
            folder_path,
            destination_path,
            username or user_id,
        )
        return jsonify(
            {
                "success": True,
                "action": action,
                "action_id": action_id,
                "album_id": album_id,
                "artist": str(row.get("artist_name") or ""),
                "album_title": str(row.get("album_title") or ""),
                "from": folder_path,
                "to": destination_path or None,
                "message": message,
            }
        )
    except Exception as exc:
        logging.exception("Trash-release action failed for album_id=%s action=%s", album_id, action)
        _record_library_curation_action(
            album_id=album_id,
            artist=str(row.get("artist_name") or ""),
            album_title=str(row.get("album_title") or ""),
            folder_path=folder_path,
            action=action,
            destination_path=destination_path,
            status="failed",
            reason_payload={"error": str(exc), "signals": candidate.get("signals") or []},
            user_id=user_id,
            username=username,
        )
        return jsonify({"error": str(exc) or "Trash-release action failed"}), 500

_ORIGINAL_EXTRACTED_FUNCTIONS = {
    "_trash_release_safe_json": _trash_release_safe_json,
    "_trash_release_tags_text": _trash_release_tags_text,
    "_trash_release_compilation_flag": _trash_release_compilation_flag,
    "_trash_release_candidate_from_album_row": _trash_release_candidate_from_album_row,
    "_trash_release_candidates_snapshot": _trash_release_candidates_snapshot,
    "_trash_release_fetch_library_album_row": _trash_release_fetch_library_album_row,
    "_trash_release_destination": _trash_release_destination,
    "_record_library_curation_action": _record_library_curation_action,
    "api_tools_trash_releases": api_tools_trash_releases,
    "api_tools_trash_releases_action": api_tools_trash_releases_action,
}


def _trash_release_safe_json_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _trash_release_safe_json(*args, **kwargs)


def _trash_release_tags_text_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _trash_release_tags_text(*args, **kwargs)


def _trash_release_compilation_flag_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _trash_release_compilation_flag(*args, **kwargs)


def _trash_release_candidate_from_album_row_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _trash_release_candidate_from_album_row(*args, **kwargs)


def _trash_release_candidates_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _trash_release_candidates_snapshot(*args, **kwargs)


def _trash_release_fetch_library_album_row_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _trash_release_fetch_library_album_row(*args, **kwargs)


def _trash_release_destination_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _trash_release_destination(*args, **kwargs)


def _record_library_curation_action_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _record_library_curation_action(*args, **kwargs)


def api_tools_trash_releases_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_tools_trash_releases(*args, **kwargs)


def api_tools_trash_releases_action_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_tools_trash_releases_action(*args, **kwargs)
