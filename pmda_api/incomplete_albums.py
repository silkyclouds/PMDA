"""Incomplete-album diagnostics API blueprint.

The old manual Plex-source incomplete scanner is disabled. These endpoints now
serve pipeline-produced diagnostics and manual review/move actions only.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import sqlite3
import time
from pathlib import Path
from types import ModuleType
from typing import Any

from flask import Blueprint, Response, jsonify, request


def _diagnostic_items(rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in rows:
        try:
            missing_from_index = json.loads(row[5]) if row[5] else []
            missing_on_disk = json.loads(row[6]) if row[6] else []
        except (TypeError, ValueError):
            missing_from_index, missing_on_disk = [], []
        items.append(
            {
                "artist": row[0],
                "album_id": row[1],
                "title_raw": row[2],
                "folder": row[3],
                "classification": row[4] or "",
                "missing_in_plex": missing_from_index,
                "missing_from_index": missing_from_index,
                "missing_on_disk": missing_on_disk,
                "expected_track_count": row[7],
                "actual_track_count": row[8],
                "detected_at": row[9],
            }
        )
    return items


def create_incomplete_albums_blueprint(*, runtime: ModuleType) -> Blueprint:
    blueprint = Blueprint("incomplete_albums_api", __name__)

    @blueprint.post("/api/incomplete-albums/scan", endpoint="api_incomplete_albums_scan_start")
    def api_incomplete_albums_scan_start():
        runtime._run_incomplete_albums_scan()
        return jsonify(
            {
                "error": "Manual incomplete scan is disabled; PMDA records incompletes during the scan pipeline.",
                "started": False,
            }
        ), 400

    @blueprint.get("/api/incomplete-albums/scan/progress", endpoint="api_incomplete_albums_scan_progress")
    def api_incomplete_albums_scan_progress():
        with runtime.lock:
            inc = runtime.state.get("incomplete_scan")
        if not inc:
            return jsonify({"running": False, "run_id": None})
        return jsonify(
            {
                "running": inc.get("running", False),
                "run_id": inc.get("run_id"),
                "progress": inc.get("progress", 0),
                "total": inc.get("total", 0),
                "current_artist": inc.get("current_artist", ""),
                "current_album": inc.get("current_album", ""),
                "count": inc.get("count", 0),
                "error": inc.get("error"),
            }
        )

    @blueprint.get("/api/incomplete-albums/results", endpoint="api_incomplete_albums_results")
    def api_incomplete_albums_results():
        run_id = request.args.get("run_id", type=int)
        con = sqlite3.connect(str(runtime.STATE_DB_FILE))
        cur = con.cursor()
        if run_id is None:
            cur.execute(
                """
                SELECT d.artist, d.album_id, d.title_raw, d.folder, d.classification,
                       d.missing_in_plex, d.missing_on_disk, d.expected_track_count,
                       d.actual_track_count, d.detected_at
                FROM incomplete_album_diagnostics d
                JOIN (
                    SELECT artist, album_id, MAX(rowid) AS rowid
                    FROM incomplete_album_diagnostics
                    GROUP BY artist, album_id
                ) latest ON latest.rowid = d.rowid
                ORDER BY d.artist, d.album_id
                """
            )
        else:
            cur.execute(
                """
                SELECT artist, album_id, title_raw, folder, classification,
                       missing_in_plex, missing_on_disk, expected_track_count,
                       actual_track_count, detected_at
                FROM incomplete_album_diagnostics
                WHERE run_id = ?
                ORDER BY artist, album_id
                """,
                (run_id,),
            )
        rows = cur.fetchall()
        con.close()
        return jsonify({"run_id": run_id, "items": _diagnostic_items(rows)})

    @blueprint.post("/api/incomplete-albums/move", endpoint="api_incomplete_albums_move")
    def api_incomplete_albums_move():
        requirement = runtime._requires_config()
        if requirement is not None:
            return requirement
        data = request.get_json() or {}
        run_id = data.get("run_id")
        items = data.get("items") or []
        if run_id is None or not items:
            return jsonify({"error": "Missing run_id or items"}), 400
        try:
            run_id = int(run_id)
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid run_id"}), 400

        target_dir = str(runtime._get_config_from_db("INCOMPLETE_ALBUMS_TARGET_DIR") or "/dupes/incomplete_albums").strip()
        target_path = Path(target_dir or "/dupes/incomplete_albums")
        target_path.mkdir(parents=True, exist_ok=True)

        con = sqlite3.connect(str(runtime.STATE_DB_FILE))
        cur = con.cursor()
        cur.execute(
            """
            SELECT artist, album_id, title_raw, folder, classification,
                   missing_in_plex, missing_on_disk, expected_track_count, actual_track_count
            FROM incomplete_album_diagnostics
            WHERE run_id = ?
            """,
            (run_id,),
        )
        diag_rows = {
            (row[0], int(row[1])): {
                "title_raw": row[2] or "",
                "folder": row[3] or "",
                "classification": row[4] or "",
                "missing_in_plex": row[5] or "[]",
                "missing_from_index": row[5] or "[]",
                "missing_on_disk": row[6] or "[]",
                "expected_track_count": int(row[7] or 0),
                "actual_track_count": int(row[8] or 0),
            }
            for row in cur.fetchall()
        }
        con.close()

        moved = []
        for item in items:
            artist = item.get("artist")
            album_id = item.get("album_id")
            if not artist or album_id is None:
                continue
            diag = diag_rows.get((artist, int(album_id)))
            if not diag:
                continue
            src_folder = runtime.path_for_fs_access(Path(str(diag.get("folder") or "")))
            if not src_folder.exists():
                continue
            destination = target_path / src_folder.name
            counter = 1
            while destination.exists():
                destination = target_path / f"{src_folder.name} ({counter})"
                counter += 1
            try:
                runtime.safe_move(str(src_folder), str(destination))
                moved.append({"artist": artist, "album_id": album_id, "moved_to": str(destination)})
                con = sqlite3.connect(str(runtime.STATE_DB_FILE))
                cur = con.cursor()
                try:
                    missing_from_index = json.loads(str(diag.get("missing_in_plex") or "[]"))
                except Exception:
                    missing_from_index = []
                try:
                    missing_on_disk = json.loads(str(diag.get("missing_on_disk") or "[]"))
                except Exception:
                    missing_on_disk = []
                runtime._insert_scan_move_row(
                    cur,
                    scan_id=int(run_id),
                    artist=str(artist or ""),
                    album_id=int(album_id or 0),
                    original_path=str(src_folder),
                    moved_to_path=str(destination),
                    size_mb=0,
                    moved_at=time.time(),
                    album_title=str(item.get("title_raw") or diag.get("title_raw") or ""),
                    fmt_text="incomplete",
                    move_reason="incomplete",
                    decision_source="manual_incomplete_move",
                    decision_provider="pmda",
                    decision_reason=str(diag.get("classification") or "incomplete_album"),
                    details={
                        "kind": "incomplete",
                        "classification": str(diag.get("classification") or ""),
                        "missing_in_plex": missing_from_index,
                        "missing_from_index": missing_from_index,
                        "missing_on_disk": missing_on_disk,
                        "expected_track_count": int(diag.get("expected_track_count") or 0),
                        "actual_track_count": int(diag.get("actual_track_count") or 0),
                    },
                )
                con.commit()
                con.close()
            except Exception as exc:
                logging.warning("Move incomplete album failed %s -> %s: %s", src_folder, destination, exc)
        return jsonify({"moved": moved})

    @blueprint.get("/api/incomplete-albums/export/<int:run_id>", endpoint="api_incomplete_albums_export")
    def api_incomplete_albums_export(run_id: int):
        fmt = (request.args.get("format") or "json").strip().lower()
        con = sqlite3.connect(str(runtime.STATE_DB_FILE))
        cur = con.cursor()
        cur.execute(
            """
            SELECT artist, album_id, title_raw, folder, classification, missing_in_plex,
                   missing_on_disk, expected_track_count, actual_track_count, detected_at
            FROM incomplete_album_diagnostics
            WHERE run_id = ?
            ORDER BY artist, album_id
            """,
            (run_id,),
        )
        rows = cur.fetchall()
        con.close()
        if fmt == "csv":
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(
                [
                    "artist",
                    "album_id",
                    "title_raw",
                    "folder",
                    "classification",
                    "missing_in_plex",
                    "missing_on_disk",
                    "expected_track_count",
                    "actual_track_count",
                    "detected_at",
                ]
            )
            for row in rows:
                writer.writerow(list(row))
            return Response(
                buf.getvalue(),
                mimetype="text/csv",
                headers={"Content-Disposition": f"attachment; filename=incomplete_albums_run_{run_id}.csv"},
            )
        return Response(
            json.dumps({"run_id": run_id, "items": _diagnostic_items(rows)}, indent=2),
            mimetype="application/json",
            headers={"Content-Disposition": f"attachment; filename=incomplete_albums_run_{run_id}.json"},
        )

    @blueprint.get("/api/broken-albums/backfill/status", endpoint="api_broken_albums_backfill_status")
    def api_broken_albums_backfill_status():
        with runtime._broken_album_backfill_lock:
            status = dict(runtime._broken_album_backfill_state or {})
        if not status:
            status = {"running": False}
        return jsonify(status)

    @blueprint.post("/api/broken-albums/backfill/start", endpoint="api_broken_albums_backfill_start")
    def api_broken_albums_backfill_start():
        payload = request.get_json(silent=True) or {}
        if runtime._trigger_broken_album_backfill_async(
            reason=str(payload.get("reason") or "api_manual"),
            include_ai=bool(runtime._parse_bool(payload.get("include_ai"))),
            limit=runtime._parse_int_loose(payload.get("limit"), 0),
            full_refresh=bool(runtime._parse_bool(payload.get("full_refresh"))),
        ):
            return jsonify({"status": "started"})
        with runtime._broken_album_backfill_lock:
            status = dict(runtime._broken_album_backfill_state or {})
        return jsonify({"status": "already_running", "progress": status}), 409

    @blueprint.post("/api/broken-albums/backfill/stop", endpoint="api_broken_albums_backfill_stop")
    def api_broken_albums_backfill_stop():
        with runtime._broken_album_backfill_lock:
            runtime._broken_album_backfill_state["running"] = False
        return jsonify({"status": "stopping"})

    @blueprint.get("/api/broken-albums/ai-review/status", endpoint="api_broken_albums_ai_review_status")
    def api_broken_albums_ai_review_status():
        return jsonify(runtime._incomplete_ai_review_status_snapshot())

    @blueprint.post("/api/broken-albums/ai-review/enqueue", endpoint="api_broken_albums_ai_review_enqueue")
    def api_broken_albums_ai_review_enqueue():
        payload = request.get_json(silent=True) or {}
        artist = str(payload.get("artist") or "").strip()
        album_id = runtime._parse_int_loose(payload.get("album_id"), 0)
        queued, status = runtime._trigger_incomplete_ai_review_async(artist, album_id)
        snapshot = runtime._incomplete_ai_review_status_snapshot()
        if queued:
            return jsonify({"status": status, "progress": snapshot})
        return jsonify({"status": status, "progress": snapshot}), 409

    @blueprint.get("/api/ai/queues/status", endpoint="api_ai_queues_status")
    def api_ai_queues_status():
        return jsonify(runtime._ai_queue_status_snapshot())

    @blueprint.post("/api/ai/queues/<domain>/enqueue", endpoint="api_ai_queue_enqueue")
    def api_ai_queue_enqueue(domain: str):
        domain_norm = str(domain or "").strip().lower()
        if domain_norm not in runtime.AI_DOMAIN_NAMES:
            return jsonify({"error": "unsupported_domain"}), 404
        payload = request.get_json(silent=True) or {}
        queued, status = runtime._trigger_ai_domain_queue_async(domain_norm, payload)
        snapshot = runtime._ai_queue_status_snapshot(domain_norm)
        if queued:
            return jsonify({"status": status, "queue": snapshot})
        return jsonify({"status": status, "queue": snapshot}), 409

    @blueprint.get("/api/ai/overview", endpoint="api_ai_overview")
    def api_ai_overview():
        return jsonify(runtime._ai_overview_snapshot())

    @blueprint.post("/api/broken-albums/review", endpoint="api_broken_albums_review")
    def api_broken_albums_review():
        payload = request.get_json(force=True, silent=True) or {}
        artist = str(payload.get("artist") or "").strip()
        album_id = runtime._parse_int_loose(payload.get("album_id"), 0)
        review_status = str(payload.get("review_status") or "").strip().lower()
        allowed = {"", "ignored", "pending"}
        if not artist or album_id <= 0:
            return jsonify({"error": "artist and album_id are required"}), 400
        if review_status not in allowed:
            return jsonify({"error": "review_status must be one of: pending, ignored, ''"}), 400
        con = runtime._state_connect(timeout=30)
        cur = con.cursor()
        cur.execute(
            """
            UPDATE broken_albums
               SET review_status = ?
             WHERE artist = ? AND album_id = ?
            """,
            (review_status, artist, int(album_id)),
        )
        changed = int(cur.rowcount or 0)
        con.commit()
        con.close()
        if changed <= 0:
            return jsonify({"error": "broken album not found"}), 404
        runtime._record_ai_override_event(
            domain="incomplete",
            target_key=f"{artist}|{int(album_id)}",
            action=f"review_status:{review_status or 'pending'}",
            details={
                "artist": artist,
                "album_id": int(album_id),
                "review_status": review_status or "pending",
            },
        )
        return jsonify(
            {
                "success": True,
                "artist": artist,
                "album_id": int(album_id),
                "review_status": review_status or "pending",
            }
        )

    return blueprint
