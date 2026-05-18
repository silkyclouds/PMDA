"""Scan history API routes."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from flask import Blueprint, jsonify


def create_scan_history_blueprint(*, runtime: Any) -> Blueprint:
    """Create scan history list/clear routes."""

    blueprint = Blueprint("pmda_scan_history", __name__)

    @blueprint.get("/api/scan-history", endpoint="api_scan_history")
    def api_scan_history():
        con = sqlite3.connect(str(runtime.STATE_DB_FILE))
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
                ORDER BY start_time DESC
            """)
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
                ORDER BY start_time DESC
            """)
        elif has_summary_json:
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
                       summary_json
                FROM scan_history
                ORDER BY start_time DESC
            """)
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
                ORDER BY start_time DESC
            """)
        rows = cur.fetchall()

        history = []
        try:
            for row in rows:
                entry = {
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
                    entry["entry_type"] = row[30] or "scan"
                else:
                    entry["entry_type"] = "scan"
                parsed_summary: dict[str, Any] | None = None
                if has_summary_json:
                    try:
                        raw = row[31] if has_entry_type else row[30]
                        maybe_summary = json.loads(raw) if raw else None
                        parsed_summary = maybe_summary if isinstance(maybe_summary, dict) else None
                    except (TypeError, ValueError):
                        parsed_summary = None
                scan_id_value = int(runtime._parse_int_loose(entry.get("scan_id"), 0) or 0)
                augmented_summary = runtime._scan_history_summary_with_metadata_rollup(cur, scan_id_value, parsed_summary)
                has_augmented_summary = (
                    bool(parsed_summary)
                    or bool(augmented_summary.get("strict_provider_counts"))
                    or int(augmented_summary.get("strict_total_albums") or 0) > 0
                    or int(augmented_summary.get("musicbrainz_identity_hits") or 0) > 0
                )
                entry["summary_json"] = augmented_summary if has_augmented_summary else None
                history.append(entry)
        finally:
            con.close()

        return jsonify(history)

    @blueprint.delete("/api/scan-history", endpoint="api_scan_history_clear")
    def api_scan_history_clear():
        con = sqlite3.connect(str(runtime.STATE_DB_FILE))
        cur = con.cursor()
        try:
            cur.execute("DELETE FROM ai_scan_cost_rollups")
            cur.execute("DELETE FROM ai_call_usage")
            cur.execute("DELETE FROM scan_pipeline_trace")
            cur.execute("DELETE FROM scan_editions")
            cur.execute("DELETE FROM scan_history")
            con.commit()
        finally:
            con.close()
        return jsonify({"status": "ok", "message": "Scan history cleared."})

    @blueprint.get("/api/scan-history/<int:scan_id>", endpoint="api_scan_history_detail")
    def api_scan_history_detail(scan_id: int):
        return runtime.api_scan_history_detail(scan_id)

    @blueprint.get("/api/scan-history/<int:scan_id>/pipeline-trace", endpoint="api_scan_history_pipeline_trace")
    def api_scan_history_pipeline_trace(scan_id: int):
        return runtime.api_scan_history_pipeline_trace(scan_id)

    @blueprint.get(
        "/api/scan-history/<int:scan_id>/pipeline-trace/export",
        endpoint="api_scan_history_pipeline_trace_export",
    )
    def api_scan_history_pipeline_trace_export(scan_id: int):
        return runtime.api_scan_history_pipeline_trace_export(scan_id)

    @blueprint.get("/api/scans/<int:scan_id>/ai-costs", endpoint="api_scan_ai_costs")
    def api_scan_ai_costs(scan_id: int):
        return runtime.api_scan_ai_costs(scan_id)

    return blueprint
