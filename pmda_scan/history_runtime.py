"""Runtime-owned scan history detail, trace, and AI-cost handlers."""

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
    '_scan_history_metadata_rollup',
    '_scan_history_metadata_rollup_for_runtime',
    '_scan_history_summary_with_metadata_rollup',
    '_scan_history_summary_with_metadata_rollup_for_runtime',
    '_scan_pipeline_trace_filtered_query',
    '_scan_pipeline_trace_filtered_query_for_runtime',
    '_scan_pipeline_trace_row_to_api',
    '_scan_pipeline_trace_row_to_api_for_runtime',
    'api_scan_ai_costs',
    'api_scan_ai_costs_for_runtime',
    'api_scan_history_detail',
    'api_scan_history_detail_for_runtime',
    'api_scan_history_pipeline_trace',
    'api_scan_history_pipeline_trace_export',
    'api_scan_history_pipeline_trace_export_for_runtime',
    'api_scan_history_pipeline_trace_for_runtime',
}


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in _LOCAL_NAMES:
            continue
        globals()[name] = value


def _scan_history_metadata_rollup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_history_metadata_rollup(*args, **kwargs)

def _scan_history_summary_with_metadata_rollup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_history_summary_with_metadata_rollup(*args, **kwargs)

def api_scan_history_detail_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_scan_history_detail(*args, **kwargs)

def _scan_pipeline_trace_row_to_api_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_pipeline_trace_row_to_api(*args, **kwargs)

def _scan_pipeline_trace_filtered_query_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_pipeline_trace_filtered_query(*args, **kwargs)

def api_scan_history_pipeline_trace_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_scan_history_pipeline_trace(*args, **kwargs)

def api_scan_history_pipeline_trace_export_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_scan_history_pipeline_trace_export(*args, **kwargs)

def api_scan_ai_costs_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_scan_ai_costs(*args, **kwargs)


def _scan_history_metadata_rollup(cur: sqlite3.Cursor, scan_id: int) -> dict[str, Any]:
    sid = int(_parse_int_loose(scan_id, 0) or 0)
    out: dict[str, Any] = {
        "strict_total_albums": 0,
        "strict_matched_albums": 0,
        "strict_unmatched_albums": 0,
        "strict_provider_total": 0,
        "strict_provider_counts": {},
        "musicbrainz_identity_hits": 0,
        "musicbrainz_identity_verified": 0,
        "musicbrainz_strict_wins": 0,
        "musicbrainz_identity_non_wins": 0,
        "musicbrainz_ids_captured": 0,
        "musicbrainz_outcome_counts": {},
        "musicbrainz_non_win_by_winner": {},
        "musicbrainz_non_win_by_reason": {},
    }
    if sid <= 0:
        return out

    provider_counts: dict[str, int] = {}
    musicbrainz_outcome_counts: dict[str, int] = {}
    musicbrainz_non_win_by_winner: dict[str, int] = {}
    musicbrainz_non_win_by_reason: dict[str, int] = {}
    if _sqlite_table_exists(cur, "scan_pipeline_trace"):
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_albums,
                SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 1 THEN 1 ELSE 0 END) AS strict_matched_albums,
                SUM(CASE WHEN COALESCE(has_musicbrainz, 0) = 1 THEN 1 ELSE 0 END) AS musicbrainz_identity_hits,
                SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 1 AND COALESCE(has_musicbrainz, 0) = 1 THEN 1 ELSE 0 END) AS musicbrainz_identity_verified,
                SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 1 AND COALESCE(strict_match_provider, '') = 'musicbrainz' THEN 1 ELSE 0 END) AS musicbrainz_strict_wins
            FROM scan_pipeline_trace
            WHERE scan_id = ?
            """,
            (sid,),
        )
        row = cur.fetchone() or (0, 0, 0, 0, 0)
        out["strict_total_albums"] = int(row[0] or 0)
        out["strict_matched_albums"] = int(row[1] or 0)
        out["musicbrainz_identity_hits"] = int(row[2] or 0)
        out["musicbrainz_identity_verified"] = int(row[3] or 0)
        out["musicbrainz_strict_wins"] = int(row[4] or 0)
        cur.execute(
            """
            SELECT COALESCE(strict_match_provider, ''), COUNT(*)
            FROM scan_pipeline_trace
            WHERE scan_id = ?
              AND COALESCE(strict_match_verified, 0) = 1
            GROUP BY COALESCE(strict_match_provider, '')
            """,
            (sid,),
        )
        for raw_provider, raw_count in (cur.fetchall() or []):
            provider = _normalize_identity_provider(str(raw_provider or "")) or "unknown"
            provider_counts[provider] = int(provider_counts.get(provider, 0) or 0) + int(raw_count or 0)
        cur.execute(
            """
            SELECT
                COALESCE(has_musicbrainz, 0),
                COALESCE(strict_match_verified, 0),
                COALESCE(strict_match_provider, ''),
                COALESCE(strict_reject_reason, ''),
                COALESCE(metadata_source, '')
            FROM scan_pipeline_trace
            WHERE scan_id = ?
            """,
            (sid,),
        )
        for has_musicbrainz, strict_match_verified, strict_match_provider_raw, strict_reject_reason_raw, metadata_source_raw in (cur.fetchall() or []):
            has_mb = bool(has_musicbrainz)
            strict_verified = bool(strict_match_verified)
            strict_provider = _normalize_identity_provider(str(strict_match_provider_raw or "")) or ""
            strict_reason = _strict_reject_code(str(strict_reject_reason_raw or ""))
            metadata_source = _normalize_identity_provider(str(metadata_source_raw or "")) or ""
            outcome_key = ""
            if strict_verified and strict_provider == "musicbrainz":
                outcome_key = "strict_win"
            elif has_mb and strict_verified and strict_provider and strict_provider != "musicbrainz":
                outcome_key = "id_captured_other_provider_won"
                musicbrainz_non_win_by_winner[strict_provider] = int(musicbrainz_non_win_by_winner.get(strict_provider, 0) or 0) + 1
            elif has_mb and strict_reason in {
                "provider_no_tracklist",
                "track_count_mismatch",
                "track_title_mismatch",
                "classical_track_count_mismatch",
                "classical_disc_count_mismatch",
                "classical_duration_mismatch",
            }:
                outcome_key = "id_captured_tracklist_reject"
                musicbrainz_non_win_by_reason[strict_reason] = int(musicbrainz_non_win_by_reason.get(strict_reason, 0) or 0) + 1
            elif has_mb and strict_reason in {
                "artist_mismatch",
                "album_mismatch",
                "provider_id_missing",
                "provider_id_mismatch",
                "classical_work_mismatch",
                "classical_composer_mismatch",
                "classical_performance_mismatch",
                "classical_label_plus_performance_mismatch",
                "classical_year_mismatch",
                "classical_context_insufficient",
            }:
                outcome_key = "id_captured_identity_reject"
                musicbrainz_non_win_by_reason[strict_reason] = int(musicbrainz_non_win_by_reason.get(strict_reason, 0) or 0) + 1
            elif has_mb:
                outcome_key = "id_captured_no_strict_match"
                fallback_reason = strict_reason or (metadata_source if metadata_source and metadata_source != "musicbrainz" else "strict_reject")
                musicbrainz_non_win_by_reason[fallback_reason] = int(musicbrainz_non_win_by_reason.get(fallback_reason, 0) or 0) + 1
            elif strict_verified and strict_provider:
                outcome_key = "no_mb_signal_other_provider_won"
            else:
                outcome_key = "no_mb_signal_no_match"
            musicbrainz_outcome_counts[outcome_key] = int(musicbrainz_outcome_counts.get(outcome_key, 0) or 0) + 1
    elif _sqlite_table_exists(cur, "scan_editions"):
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_albums,
                SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 1 THEN 1 ELSE 0 END) AS strict_matched_albums
            FROM scan_editions
            WHERE scan_id = ?
            """,
            (sid,),
        )
        row = cur.fetchone() or (0, 0)
        out["strict_total_albums"] = int(row[0] or 0)
        out["strict_matched_albums"] = int(row[1] or 0)
        cur.execute(
            """
            SELECT COALESCE(strict_match_provider, ''), COUNT(*)
            FROM scan_editions
            WHERE scan_id = ?
              AND COALESCE(strict_match_verified, 0) = 1
            GROUP BY COALESCE(strict_match_provider, '')
            """,
            (sid,),
        )
        for raw_provider, raw_count in (cur.fetchall() or []):
            provider = _normalize_identity_provider(str(raw_provider or "")) or "unknown"
            provider_counts[provider] = int(provider_counts.get(provider, 0) or 0) + int(raw_count or 0)

    out["strict_unmatched_albums"] = max(0, int(out["strict_total_albums"] or 0) - int(out["strict_matched_albums"] or 0))
    out["strict_provider_counts"] = dict(provider_counts)
    out["strict_provider_total"] = int(sum(int(v or 0) for v in provider_counts.values()))
    out["musicbrainz_identity_non_wins"] = max(
        0,
        int(out["musicbrainz_identity_verified"] or 0) - int(out["musicbrainz_strict_wins"] or 0),
    )
    out["musicbrainz_ids_captured"] = int(out.get("musicbrainz_identity_hits") or 0)
    out["musicbrainz_outcome_counts"] = dict(musicbrainz_outcome_counts)
    out["musicbrainz_non_win_by_winner"] = dict(musicbrainz_non_win_by_winner)
    out["musicbrainz_non_win_by_reason"] = dict(musicbrainz_non_win_by_reason)
    return out


def _scan_history_summary_with_metadata_rollup(
    cur: sqlite3.Cursor,
    scan_id: int,
    summary: dict[str, Any] | None,
) -> dict[str, Any]:
    summary_obj = dict(summary or {})
    has_explicit_rollup = bool(
        isinstance(summary_obj.get("strict_provider_counts"), dict)
        and "musicbrainz_identity_hits" in summary_obj
        and "musicbrainz_strict_wins" in summary_obj
        and isinstance(summary_obj.get("musicbrainz_outcome_counts"), dict)
    )
    if has_explicit_rollup:
        return summary_obj
    rollup = _scan_history_metadata_rollup(cur, scan_id)
    summary_obj["strict_total_albums"] = int(summary_obj.get("strict_total_albums") or rollup.get("strict_total_albums") or 0)
    summary_obj["strict_matched_albums"] = int(summary_obj.get("strict_matched_albums") or rollup.get("strict_matched_albums") or 0)
    summary_obj["strict_unmatched_albums"] = int(summary_obj.get("strict_unmatched_albums") or rollup.get("strict_unmatched_albums") or 0)
    summary_obj["albums_with_mb_id"] = int(summary_obj.get("albums_with_mb_id") or summary_obj.get("strict_matched_albums") or 0)
    summary_obj["strict_provider_counts"] = dict(rollup.get("strict_provider_counts") or {})
    summary_obj["strict_provider_total"] = int(rollup.get("strict_provider_total") or 0)
    summary_obj["musicbrainz_identity_hits"] = int(rollup.get("musicbrainz_identity_hits") or 0)
    summary_obj["musicbrainz_identity_verified"] = int(rollup.get("musicbrainz_identity_verified") or 0)
    summary_obj["musicbrainz_strict_wins"] = int(rollup.get("musicbrainz_strict_wins") or 0)
    summary_obj["musicbrainz_identity_non_wins"] = int(rollup.get("musicbrainz_identity_non_wins") or 0)
    summary_obj["musicbrainz_ids_captured"] = int(rollup.get("musicbrainz_ids_captured") or summary_obj.get("musicbrainz_identity_hits") or 0)
    summary_obj["musicbrainz_outcome_counts"] = dict(rollup.get("musicbrainz_outcome_counts") or {})
    summary_obj["musicbrainz_non_win_by_winner"] = dict(rollup.get("musicbrainz_non_win_by_winner") or {})
    summary_obj["musicbrainz_non_win_by_reason"] = dict(rollup.get("musicbrainz_non_win_by_reason") or {})
    summary_obj["scan_discogs_matched"] = int(
        summary_obj.get("scan_discogs_matched")
        or (summary_obj.get("strict_provider_counts") or {}).get("discogs")
        or 0
    )
    summary_obj["scan_lastfm_matched"] = int(
        summary_obj.get("scan_lastfm_matched")
        or (summary_obj.get("strict_provider_counts") or {}).get("lastfm")
        or 0
    )
    summary_obj["scan_bandcamp_matched"] = int(
        summary_obj.get("scan_bandcamp_matched")
        or (summary_obj.get("strict_provider_counts") or {}).get("bandcamp")
        or 0
    )
    raw_provider_matches = summary_obj.get("scan_provider_matches")
    if isinstance(raw_provider_matches, dict):
        normalized_matches = {
            key: int(raw_provider_matches.get(key) or 0)
            for key in _scan_provider_match_keys(tuple(raw_provider_matches.keys()))
        }
    else:
        strict_counts = dict(summary_obj.get("strict_provider_counts") or {})
        normalized_matches = {
            key: int(strict_counts.get(key) or 0)
            for key in _scan_provider_match_keys(tuple(strict_counts.keys()))
        }
    summary_obj["scan_provider_matches"] = normalized_matches
    return summary_obj


def api_scan_history_detail(scan_id):
    """Return details of a specific scan or dedupe entry."""
    import sqlite3
    con = sqlite3.connect(str(STATE_DB_FILE))
    cur = con.cursor()
    cur.execute("PRAGMA table_info(scan_history)")
    cols_info = [r[1] for r in cur.fetchall()]
    has_entry_type = "entry_type" in cols_info
    has_summary_json = "summary_json" in cols_info
    has_ai_cost_cols = all(
        c in cols_info for c in ("ai_tokens_total", "ai_cost_usd_total", "ai_unpriced_calls", "ai_lifecycle_complete")
    )
    if has_entry_type and has_summary_json:
        cur.execute("""
            SELECT scan_id, start_time, end_time, duration_seconds, albums_scanned,
                   duplicates_found, artists_processed, artists_total, ai_used_count,
                   mb_used_count, ai_enabled, mb_enabled, auto_move_enabled,
                   space_saved_mb, albums_moved, status,
                   duplicate_groups_count, total_duplicates_count, broken_albums_count,
                   missing_albums_count, albums_without_artist_image, albums_without_album_image,
                   albums_without_complete_tags, albums_without_mb_id, albums_without_artist_mb_id,
                   ai_tokens_total, ai_cost_usd_total, ai_unpriced_calls, ai_lifecycle_complete,
                   COALESCE(scan_type, 'full') AS scan_type,
                   entry_type, summary_json
            FROM scan_history
            WHERE scan_id = ?
        """, (scan_id,))
    elif has_entry_type:
        cur.execute("""
            SELECT scan_id, start_time, end_time, duration_seconds, albums_scanned,
                   duplicates_found, artists_processed, artists_total, ai_used_count,
                   mb_used_count, ai_enabled, mb_enabled, auto_move_enabled,
                   space_saved_mb, albums_moved, status,
                   duplicate_groups_count, total_duplicates_count, broken_albums_count,
                   missing_albums_count, albums_without_artist_image, albums_without_album_image,
                   albums_without_complete_tags, albums_without_mb_id, albums_without_artist_mb_id,
                   ai_tokens_total, ai_cost_usd_total, ai_unpriced_calls, ai_lifecycle_complete,
                   COALESCE(scan_type, 'full') AS scan_type,
                   entry_type
            FROM scan_history
            WHERE scan_id = ?
        """, (scan_id,))
    else:
        cur.execute("""
            SELECT scan_id, start_time, end_time, duration_seconds, albums_scanned,
                   duplicates_found, artists_processed, artists_total, ai_used_count,
                   mb_used_count, ai_enabled, mb_enabled, auto_move_enabled,
                   space_saved_mb, albums_moved, status,
                   duplicate_groups_count, total_duplicates_count, broken_albums_count,
                   missing_albums_count, albums_without_artist_image, albums_without_album_image,
                   albums_without_complete_tags, albums_without_mb_id, albums_without_artist_mb_id,
                   ai_tokens_total, ai_cost_usd_total, ai_unpriced_calls, ai_lifecycle_complete,
                   COALESCE(scan_type, 'full') AS scan_type
            FROM scan_history
            WHERE scan_id = ?
        """, (scan_id,))
    row = cur.fetchone()

    if not row:
        con.close()
        return jsonify({"error": "Scan not found"}), 404

    out = {
        "scan_id": row[0],
        "start_time": row[1],
        "end_time": row[2],
        "duration_seconds": row[3],
        "albums_scanned": row[4] or 0,
        "duplicates_found": row[5] or 0,
        "artists_processed": row[6] or 0,
        "artists_total": row[7] or 0,
        "ai_used_count": row[8] or 0,
        "mb_used_count": row[9] or 0,
        "ai_enabled": bool(row[10]),
        "mb_enabled": bool(row[11]),
        "auto_move_enabled": bool(row[12]),
        "space_saved_mb": row[13] or 0,
        "albums_moved": row[14] or 0,
        "status": row[15] or "completed",
        "duplicate_groups_count": row[16] or 0 if len(row) > 16 else 0,
        "total_duplicates_count": row[17] or 0 if len(row) > 17 else 0,
        "broken_albums_count": row[18] or 0 if len(row) > 18 else 0,
        "missing_albums_count": row[19] or 0 if len(row) > 19 else 0,
        "albums_without_artist_image": row[20] or 0 if len(row) > 20 else 0,
        "albums_without_album_image": row[21] or 0 if len(row) > 21 else 0,
        "albums_without_complete_tags": row[22] or 0 if len(row) > 22 else 0,
        "albums_without_mb_id": row[23] or 0 if len(row) > 23 else 0,
        "albums_without_artist_mb_id": row[24] or 0 if len(row) > 24 else 0,
        "ai_tokens_total": int(row[25] or 0) if has_ai_cost_cols and len(row) > 25 else 0,
        "ai_cost_usd_total": float(row[26] or 0.0) if has_ai_cost_cols and len(row) > 26 else 0.0,
        "ai_unpriced_calls": int(row[27] or 0) if has_ai_cost_cols and len(row) > 27 else 0,
        "ai_lifecycle_complete": bool(row[28]) if has_ai_cost_cols and len(row) > 28 else False,
        "scan_type": str(row[29] or "full") if len(row) > 29 else "full",
    }
    if has_entry_type and len(row) > 30:
        out["entry_type"] = row[30] or "scan"
    else:
        out["entry_type"] = "scan"
    summary_idx = 31 if has_entry_type else 30
    if has_summary_json and len(row) > summary_idx and row[summary_idx]:
        try:
            raw_summary = json.loads(row[summary_idx])
            summary = raw_summary if isinstance(raw_summary, dict) else None
            summary = _scan_history_summary_with_metadata_rollup(cur, int(scan_id), summary)
            out["summary_json"] = summary
            if isinstance(summary, dict) and "steps_executed" in summary:
                out["steps_executed"] = summary["steps_executed"]
        except (TypeError, ValueError):
            summary = _scan_history_summary_with_metadata_rollup(cur, int(scan_id), None)
            has_augmented_summary = bool(summary.get("strict_provider_counts")) or int(summary.get("strict_total_albums") or 0) > 0 or int(summary.get("musicbrainz_identity_hits") or 0) > 0
            out["summary_json"] = summary if has_augmented_summary else None
    else:
        summary = _scan_history_summary_with_metadata_rollup(cur, int(scan_id), None)
        has_augmented_summary = bool(summary.get("strict_provider_counts")) or int(summary.get("strict_total_albums") or 0) > 0 or int(summary.get("musicbrainz_identity_hits") or 0) > 0
        out["summary_json"] = summary if has_augmented_summary else None
    con.close()
    return jsonify(out)


def _scan_pipeline_trace_row_to_api(row: sqlite3.Row) -> dict[str, Any]:
    def _json_list(raw: Any) -> list[Any]:
        try:
            parsed = json.loads(str(raw or "[]"))
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []

    def _json_obj(raw: Any) -> dict[str, Any]:
        try:
            parsed = json.loads(str(raw or "{}"))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    return {
        "scan_id": int(row["scan_id"] or 0),
        "artist": str(row["artist"] or ""),
        "album_id": int(row["album_id"] or 0),
        "album_title": str(row["album_title"] or ""),
        "folder": str(row["folder"] or ""),
        "folder_name": str(row["folder_name"] or ""),
        "fmt_text": str(row["fmt_text"] or ""),
        "metadata_source": str(row["metadata_source"] or ""),
        "strict_match_verified": bool(row["strict_match_verified"]),
        "strict_match_provider": str(row["strict_match_provider"] or ""),
        "strict_reject_reason": str(row["strict_reject_reason"] or ""),
        "strict_tracklist_score": float(row["strict_tracklist_score"] or 0.0),
        "has_cover": bool(row["has_cover"]),
        "is_broken": bool(row["is_broken"]),
        "expected_track_count": _parse_int_loose(row["expected_track_count"], 0),
        "actual_track_count": _parse_int_loose(row["actual_track_count"], 0),
        "missing_indices": _json_list(row["missing_indices"]),
        "missing_required_tags": _json_list(row["missing_required_tags"]),
        "providers": {
            "musicbrainz": bool(row["has_musicbrainz"]),
            "discogs": bool(row["has_discogs"]),
            "lastfm": bool(row["has_lastfm"]),
            "bandcamp": bool(row["has_bandcamp"]),
        },
        "provider_refs": {
            "musicbrainz_release_id": str(row["musicbrainz_release_id"] or ""),
            "discogs_release_id": str(row["discogs_release_id"] or ""),
            "lastfm_album_mbid": str(row["lastfm_album_mbid"] or ""),
            "bandcamp_album_url": str(row["bandcamp_album_url"] or ""),
        },
        "dupe_role": str(row["dupe_role"] or "none"),
        "dupe_signal": str(row["dupe_signal"] or ""),
        "dupe_peer_count": _parse_int_loose(row["dupe_peer_count"], 0),
        "dupe_needs_ai": bool(row["dupe_needs_ai"]),
        "no_move": bool(row["no_move"]),
        "manual_review": bool(row["manual_review"]),
        "same_folder": bool(row["same_folder"]),
        "winner_album_id": _parse_int_loose(row["winner_album_id"], 0) or None,
        "winner_title": str(row["winner_title"] or ""),
        "ai_used": bool(row["ai_used"]),
        "ai_provider": str(row["ai_provider"] or ""),
        "ai_model": str(row["ai_model"] or ""),
        "pipeline_status": str(row["pipeline_status"] or "active"),
        "move_reason": str(row["move_reason"] or ""),
        "move_status": str(row["move_status"] or "none"),
        "moved_to_path": str(row["moved_to_path"] or ""),
        "decision_provider": str(row["decision_provider"] or ""),
        "decision_reason": str(row["decision_reason"] or ""),
        "decision_confidence": float(row["decision_confidence"]) if row["decision_confidence"] is not None else None,
        "timeline": _json_list(row["timeline_json"]),
        "meta_summary": _json_obj(row["meta_summary_json"]),
        "updated_at": float(row["updated_at"] or 0.0),
    }


def _scan_pipeline_trace_filtered_query(
    scan_id: int,
    *,
    q: str = "",
    provider: str = "",
    outcome: str = "",
) -> tuple[str, list[Any]]:
    where_parts = ["scan_id = ?"]
    params: list[Any] = [int(scan_id)]
    search = str(q or "").strip().lower()
    if search:
        needle = f"%{search}%"
        where_parts.append(
            "("
            "LOWER(COALESCE(artist, '')) LIKE ? OR "
            "LOWER(COALESCE(album_title, '')) LIKE ? OR "
            "LOWER(COALESCE(folder_name, '')) LIKE ? OR "
            "LOWER(COALESCE(folder, '')) LIKE ?"
            ")"
        )
        params.extend([needle, needle, needle, needle])
    provider_norm = str(provider or "").strip().lower()
    if provider_norm in {"musicbrainz", "discogs", "lastfm", "bandcamp"}:
        where_parts.append(f"COALESCE(has_{provider_norm}, 0) = 1")
    elif provider_norm == "none":
        where_parts.append(
            "COALESCE(has_musicbrainz, 0) = 0 AND COALESCE(has_discogs, 0) = 0 "
            "AND COALESCE(has_lastfm, 0) = 0 AND COALESCE(has_bandcamp, 0) = 0"
        )
    outcome_norm = str(outcome or "").strip().lower()
    allowed_outcomes = {
        "matched",
        "provider_only",
        "unmatched",
        "duplicate_winner",
        "duplicate_loser",
        "duplicate_candidate",
        "incomplete",
        "moved_duplicate",
        "moved_incomplete",
        "restored_duplicate",
        "restored_incomplete",
    }
    if outcome_norm in allowed_outcomes:
        where_parts.append("LOWER(COALESCE(pipeline_status, 'active')) = ?")
        params.append(outcome_norm)
    return " AND ".join(where_parts), params


def api_scan_history_pipeline_trace(scan_id: int):
    admin_gate = _require_admin_json()
    if admin_gate is not None:
        return admin_gate
    page = max(1, _parse_int_loose(request.args.get("page"), 1))
    page_size = max(25, min(500, _parse_int_loose(request.args.get("page_size"), 100)))
    q = str(request.args.get("q") or "").strip()
    provider = str(request.args.get("provider") or "").strip()
    outcome = str(request.args.get("outcome") or "").strip()
    where_sql, params = _scan_pipeline_trace_filtered_query(scan_id, q=q, provider=provider, outcome=outcome)
    offset = (page - 1) * page_size
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute(f"SELECT COUNT(*) AS total FROM scan_pipeline_trace WHERE {where_sql}", tuple(params))
        total = int((cur.fetchone() or {"total": 0})["total"] or 0)
        cur.execute(
            f"""
            SELECT *
            FROM scan_pipeline_trace
            WHERE {where_sql}
            ORDER BY updated_at DESC, artist COLLATE NOCASE ASC, album_title COLLATE NOCASE ASC
            LIMIT ? OFFSET ?
            """,
            tuple(params + [page_size, offset]),
        )
        rows = cur.fetchall()
        cur.execute(
            f"""
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 1 THEN 1 ELSE 0 END), 0) AS strict_matches,
                COALESCE(SUM(CASE WHEN COALESCE(metadata_source, '') <> '' THEN 1 ELSE 0 END), 0) AS provider_matches,
                COALESCE(SUM(CASE WHEN COALESCE(is_broken, 0) = 1 THEN 1 ELSE 0 END), 0) AS broken,
                COALESCE(SUM(CASE WHEN LOWER(COALESCE(dupe_role, 'none')) = 'loser' THEN 1 ELSE 0 END), 0) AS duplicate_losers,
                COALESCE(SUM(CASE WHEN LOWER(COALESCE(dupe_role, 'none')) = 'winner' THEN 1 ELSE 0 END), 0) AS duplicate_winners,
                COALESCE(SUM(CASE WHEN LOWER(COALESCE(move_reason, '')) = 'dedupe' AND LOWER(COALESCE(move_status, '')) = 'moved' THEN 1 ELSE 0 END), 0) AS moved_duplicates,
                COALESCE(SUM(CASE WHEN LOWER(COALESCE(move_reason, '')) = 'incomplete' AND LOWER(COALESCE(move_status, '')) = 'moved' THEN 1 ELSE 0 END), 0) AS moved_incompletes,
                COALESCE(SUM(CASE WHEN COALESCE(ai_used, 0) = 1 THEN 1 ELSE 0 END), 0) AS ai_touched
            FROM scan_pipeline_trace
            WHERE {where_sql}
            """,
            tuple(params),
        )
        summary_row = cur.fetchone() or {}
        cur.execute(
            f"""
            SELECT LOWER(COALESCE(pipeline_status, 'active')) AS pipeline_status, COUNT(*) AS count
            FROM scan_pipeline_trace
            WHERE {where_sql}
            GROUP BY LOWER(COALESCE(pipeline_status, 'active'))
            ORDER BY count DESC, pipeline_status ASC
            """,
            tuple(params),
        )
        status_counts = {str(r["pipeline_status"] or "active"): int(r["count"] or 0) for r in cur.fetchall()}
        cur.execute(
            f"""
            SELECT LOWER(COALESCE(metadata_source, '')) AS provider, COUNT(*) AS count
            FROM scan_pipeline_trace
            WHERE {where_sql}
            GROUP BY LOWER(COALESCE(metadata_source, ''))
            ORDER BY count DESC, provider ASC
            """,
            tuple(params),
        )
        provider_counts = {str(r["provider"] or "none") or "none": int(r["count"] or 0) for r in cur.fetchall()}
    finally:
        con.close()
    return jsonify(
        {
            "scan_id": int(scan_id),
            "page": int(page),
            "page_size": int(page_size),
            "total": int(total),
            "items": [_scan_pipeline_trace_row_to_api(row) for row in rows],
            "summary": {
                "total": int(summary_row["total"] or 0),
                "strict_matches": int(summary_row["strict_matches"] or 0),
                "provider_matches": int(summary_row["provider_matches"] or 0),
                "broken": int(summary_row["broken"] or 0),
                "duplicate_winners": int(summary_row["duplicate_winners"] or 0),
                "duplicate_losers": int(summary_row["duplicate_losers"] or 0),
                "moved_duplicates": int(summary_row["moved_duplicates"] or 0),
                "moved_incompletes": int(summary_row["moved_incompletes"] or 0),
                "ai_touched": int(summary_row["ai_touched"] or 0),
                "status_counts": status_counts,
                "provider_counts": provider_counts,
            },
        }
    )


def api_scan_history_pipeline_trace_export(scan_id: int):
    admin_gate = _require_admin_json()
    if admin_gate is not None:
        return admin_gate
    export_format = str(request.args.get("format") or "csv").strip().lower()
    if export_format not in {"csv", "json"}:
        return jsonify({"error": "Invalid export format"}), 400
    q = str(request.args.get("q") or "").strip()
    provider = str(request.args.get("provider") or "").strip()
    outcome = str(request.args.get("outcome") or "").strip()
    where_sql, params = _scan_pipeline_trace_filtered_query(scan_id, q=q, provider=provider, outcome=outcome)
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT *
            FROM scan_pipeline_trace
            WHERE {where_sql}
            ORDER BY updated_at DESC, artist COLLATE NOCASE ASC, album_title COLLATE NOCASE ASC
            """,
            tuple(params),
        )
        rows = [_scan_pipeline_trace_row_to_api(row) for row in cur.fetchall()]
    finally:
        con.close()
    if export_format == "json":
        return Response(
            json.dumps({"scan_id": int(scan_id), "items": rows}, ensure_ascii=False, indent=2),
            mimetype="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="scan-{int(scan_id)}-pipeline-trace.json"',
            },
        )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "artist",
            "album_title",
            "folder",
            "pipeline_status",
            "metadata_source",
            "strict_match_verified",
            "strict_match_provider",
            "strict_reject_reason",
            "strict_tracklist_score",
            "has_musicbrainz",
            "has_discogs",
            "has_lastfm",
            "has_bandcamp",
            "dupe_role",
            "dupe_signal",
            "dupe_peer_count",
            "is_broken",
            "expected_track_count",
            "actual_track_count",
            "missing_indices",
            "missing_required_tags",
            "move_reason",
            "move_status",
            "moved_to_path",
            "decision_provider",
            "decision_reason",
            "ai_used",
            "ai_provider",
            "ai_model",
            "timeline",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.get("artist"),
                row.get("album_title"),
                row.get("folder"),
                row.get("pipeline_status"),
                row.get("metadata_source"),
                1 if row.get("strict_match_verified") else 0,
                row.get("strict_match_provider"),
                row.get("strict_reject_reason"),
                row.get("strict_tracklist_score"),
                1 if row.get("providers", {}).get("musicbrainz") else 0,
                1 if row.get("providers", {}).get("discogs") else 0,
                1 if row.get("providers", {}).get("lastfm") else 0,
                1 if row.get("providers", {}).get("bandcamp") else 0,
                row.get("dupe_role"),
                row.get("dupe_signal"),
                row.get("dupe_peer_count"),
                1 if row.get("is_broken") else 0,
                row.get("expected_track_count"),
                row.get("actual_track_count"),
                json.dumps(row.get("missing_indices") or [], ensure_ascii=False),
                json.dumps(row.get("missing_required_tags") or [], ensure_ascii=False),
                row.get("move_reason"),
                row.get("move_status"),
                row.get("moved_to_path"),
                row.get("decision_provider"),
                row.get("decision_reason"),
                1 if row.get("ai_used") else 0,
                row.get("ai_provider"),
                row.get("ai_model"),
                json.dumps(row.get("timeline") or [], ensure_ascii=False),
            ]
        )
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="scan-{int(scan_id)}-pipeline-trace.csv"',
        },
    )


def api_scan_ai_costs(scan_id: int):
    if int(scan_id or 0) <= 0:
        return jsonify({"error": "Invalid scan_id"}), 400
    include_lifecycle = _parse_bool(request.args.get("include_lifecycle", "true"))
    group_by = str(request.args.get("group_by") or "analysis_type").strip().lower()
    if group_by not in {"analysis_type", "job_type", "model", "provider", "album", "auth_mode"}:
        return jsonify({"error": "Invalid group_by"}), 400
    try:
        limit = max(0, min(5000, int(request.args.get("limit") or 0)))
    except (TypeError, ValueError):
        limit = 0
    con = _state_connect(timeout=10)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM scan_history WHERE scan_id = ?", (int(scan_id),))
    row = cur.fetchone()
    con.close()
    if not row or int(row["c"] or 0) <= 0:
        return jsonify({"error": "Scan not found"}), 404
    payload = _ai_scan_cost_summary(
        int(scan_id),
        include_lifecycle=bool(include_lifecycle),
        group_by=group_by,
        limit=int(limit or 0),
    )
    return jsonify(payload)
