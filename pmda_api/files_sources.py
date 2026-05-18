"""Files source, incoming queue, and watcher API routes."""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

from flask import Blueprint, jsonify, request


def create_files_sources_blueprint(*, runtime: Any) -> Blueprint:
    """Create routes that manage files-mode source roots and watcher state."""

    blueprint = Blueprint("pmda_files_sources", __name__)

    @blueprint.get("/api/files/sources", endpoint="api_files_sources_get")
    def api_files_sources_get():
        rows = runtime._effective_files_source_rows(enabled_only=False)
        winner = runtime._winner_source_row()
        return jsonify(
            {
                "roots": rows,
                "winner_source_root_id": int(winner.get("source_id") or 0) if winner else None,
                "winner_placement_strategy": str(
                    runtime._get_config_from_db(
                        "LIBRARY_WINNER_PLACEMENT_STRATEGY",
                        runtime.LIBRARY_WINNER_PLACEMENT_STRATEGY,
                    )
                    or "move"
                ).strip().lower(),
            }
        )

    @blueprint.put("/api/files/sources", endpoint="api_files_sources_put")
    def api_files_sources_put():
        data = request.get_json(silent=True) or {}
        roots_payload = data.get("roots")
        if not isinstance(roots_payload, list):
            return jsonify({"error": "Body must include roots: []"}), 400
        winner_source_root_id = data.get("winner_source_root_id")
        winner_strategy = str(data.get("winner_placement_strategy") or "move").strip().lower()
        if winner_strategy not in {"move", "hardlink", "symlink", "copy"}:
            winner_strategy = "move"
        try:
            winner_int = int(winner_source_root_id) if winner_source_root_id is not None else None
        except Exception:
            winner_int = None
        try:
            rows = runtime._files_source_roots_replace(roots_payload, winner_source_root_id=winner_int)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception:
            logging.exception("Failed to save files source roots")
            return jsonify({"error": "Failed to save source roots"}), 500

        try:
            runtime.init_settings_db()
            con_cfg = sqlite3.connect(str(runtime.SETTINGS_DB_FILE), timeout=5)
            con_cfg.execute(
                "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
                ("LIBRARY_WINNER_PLACEMENT_STRATEGY", winner_strategy),
            )
            con_cfg.execute(
                "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
                ("LIBRARY_WORKFLOW_MODE", "custom"),
            )
            winner_row = next((r for r in rows if bool(r.get("is_winner_root"))), rows[0] if rows else None)
            if winner_row:
                con_cfg.execute(
                    "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
                    ("WINNER_SOURCE_ROOT_ID", str(int(winner_row.get("source_id") or 0))),
                )
            con_cfg.commit()
            con_cfg.close()
            runtime._apply_settings_in_memory(
                {
                    "LIBRARY_WORKFLOW_MODE": "custom",
                    "LIBRARY_WINNER_PLACEMENT_STRATEGY": winner_strategy,
                    "WINNER_SOURCE_ROOT_ID": str(int(winner_row.get("source_id") or 0)) if winner_row else "",
                }
            )
        except Exception:
            logging.debug("Failed to persist winner strategy/source id", exc_info=True)

        winner_now = runtime._winner_source_row()
        return jsonify(
            {
                "status": "ok",
                "roots": rows,
                "winner_source_root_id": int(winner_now.get("source_id") or 0) if winner_now else None,
                "winner_placement_strategy": winner_strategy,
            }
        )

    @blueprint.get("/api/incoming/status", endpoint="api_incoming_status")
    def api_incoming_status():
        rows = runtime._files_source_roots_fetch(enabled_only=True)
        incoming_ids = {int(r["source_id"]) for r in rows if str(r.get("role") or "") == "incoming"}
        pending_total = 0
        oldest_event_at = None
        last_processed_at = None
        pending_by_source: dict[str, int] = {}
        errors: list[str] = []
        try:
            con = sqlite3.connect(str(runtime.STATE_DB_FILE), timeout=10)
            cur = con.cursor()
            if incoming_ids:
                placeholders = ",".join("?" for _ in incoming_ids)
                cur.execute(
                    f"""
                    SELECT source_id, COUNT(*) AS c, MIN(first_seen) AS oldest
                    FROM files_pending_changes
                    WHERE source_id IN ({placeholders})
                    GROUP BY source_id
                    """,
                    tuple(sorted(incoming_ids)),
                )
                grouped = cur.fetchall()
                for row in grouped:
                    sid = int(row[0] or 0)
                    cnt = int(row[1] or 0)
                    pending_total += cnt
                    pending_by_source[str(sid)] = cnt
                    oldest = float(row[2] or 0.0) if row[2] is not None else None
                    if oldest is not None and (oldest_event_at is None or oldest < oldest_event_at):
                        oldest_event_at = oldest
            cur.execute("SELECT MAX(last_seen) FROM files_pending_changes")
            max_seen_row = cur.fetchone()
            if max_seen_row and max_seen_row[0] is not None:
                last_processed_at = float(max_seen_row[0] or 0.0)
            con.close()
        except Exception as e:
            errors.append(str(e))
        return jsonify(
            {
                "pending_folders": int(pending_total),
                "oldest_event_at": oldest_event_at,
                "last_processed_at": last_processed_at,
                "errors": errors,
                "pending_by_source": pending_by_source,
                "incoming_source_ids": sorted(list(incoming_ids)),
            }
        )

    @blueprint.post("/api/incoming/rescan", endpoint="api_incoming_rescan")
    def api_incoming_rescan():
        if bool(runtime._pipeline_bootstrap_status().get("bootstrap_required")):
            return (
                jsonify({"status": "blocked", "reason": "bootstrap_required", "message": "Initial full scan required"}),
                409,
            )
        ok, meta = runtime._try_begin_scan(
            scan_type="changed_only",
            source="incoming_manual",
            run_improve_after=False,
            scheduler_run_id=None,
        )
        if not ok:
            reason = str(meta.get("reason") or "scan_already_running")
            status_code = 409 if reason == "scan_already_running" else 500
            return jsonify({"status": "blocked", "reason": reason, "message": str(meta.get("message") or reason)}), status_code
        return jsonify({"status": "started", "scan_type": "changed_only"})

    @blueprint.get("/api/files/watcher/status", endpoint="api_files_watcher_status")
    def api_files_watcher_status():
        snap = runtime._files_watcher_status_snapshot()
        return jsonify(
            {
                "running": bool(snap.get("running")),
                "enabled": bool(snap.get("enabled")),
                "available": bool(snap.get("available")),
                "degraded_mode": bool(snap.get("degraded_mode")),
                "reason": str(snap.get("reason") or ""),
                "dirty_count": int(snap.get("dirty_count") or 0),
                "dirty_count_by_root": dict(snap.get("dirty_count_by_root") or {}),
                "last_event_at": snap.get("last_event_at"),
                "last_event_path": snap.get("last_event_path"),
                "restart_in_progress": bool(snap.get("restart_in_progress")),
                "consecutive_failures": int(snap.get("consecutive_failures") or 0),
                "last_restart_duration_ms": runtime._int_or_none(snap.get("last_restart_duration_ms")),
                "last_error": str(snap.get("last_error") or ""),
                "last_restart_started_at": snap.get("last_restart_started_at"),
                "last_restart_ended_at": snap.get("last_restart_ended_at"),
                "roots": list(snap.get("roots") or []),
            }
        )

    @blueprint.post("/api/files/watcher/restart", endpoint="api_files_watcher_restart")
    def api_files_watcher_restart():
        now = time.time()
        runtime._request_files_watcher_reconcile("manual_api_restart", force=True)
        return jsonify({"status": "accepted", "requested_at": now})

    @blueprint.get("/api/files/structure/overview", endpoint="api_files_structure_overview")
    def api_files_structure_overview():
        """Return structure analysis: sampled paths, inferred templates, metrics."""
        if runtime._get_library_mode() != "files":
            return jsonify({"error": "Structure overview is only available in Files library mode"}), 400
        roots = runtime.FILES_ROOTS or []
        if not roots:
            return jsonify({"templates": [], "metrics": {}, "samples": [], "sample_count": 0})
        data = runtime.analyse_directory_structure(roots)
        return jsonify(data)

    @blueprint.get("/api/fs/list", endpoint="api_fs_list")
    def api_fs_list():
        """List subdirectories for folder navigation in the Settings UI."""
        path_raw = str(request.args.get("path") or "").strip()
        if not path_raw:
            roots = runtime._parse_files_roots(runtime.FILES_ROOTS)
            path_raw = roots[0] if roots else "/"
        include_hidden = bool(runtime._parse_bool(request.args.get("hidden") or False))
        try:
            limit = max(0, min(1000, int(request.args.get("limit") or 300)))
        except (TypeError, ValueError):
            limit = 300

        path_obj = Path(path_raw)
        if not path_obj.is_absolute():
            return jsonify({"error": "Path must be absolute"}), 400
        path_obj = runtime.path_for_fs_access(path_obj)
        if not path_obj.exists():
            return jsonify({"error": "Path does not exist", "path": str(path_obj)}), 404
        if not path_obj.is_dir():
            return jsonify({"error": "Path is not a directory", "path": str(path_obj)}), 400

        directories = []
        truncated = False
        if limit > 0:
            try:
                # Return the first directories found instead of sorting a spun-down array.
                with os.scandir(path_obj) as iterator:
                    for entry in iterator:
                        try:
                            name = entry.name
                            if not include_hidden and name.startswith("."):
                                continue
                            if not entry.is_dir(follow_symlinks=False):
                                continue
                            child = Path(path_obj, name)
                            directories.append(
                                {
                                    "name": name,
                                    "path": str(child),
                                    "writable": bool(os.access(child, os.W_OK)),
                                }
                            )
                            if len(directories) >= limit:
                                truncated = True
                                break
                        except OSError:
                            continue
            except PermissionError:
                return jsonify({"error": "Permission denied", "path": str(path_obj)}), 403
            except OSError as e:
                return jsonify({"error": str(e), "path": str(path_obj)}), 500

        common_roots: list[str] = []
        for candidate in [
            *(runtime._parse_files_roots(runtime.FILES_ROOTS) or []),
            (runtime.EXPORT_ROOT or "").strip(),
            (runtime.MEDIA_CACHE_ROOT or "").strip(),
            "/music",
            "/config",
            "/",
        ]:
            c = str(candidate or "").strip()
            if not c or c in common_roots:
                continue
            common_roots.append(c)

        parent = None
        if path_obj.parent != path_obj:
            parent = str(path_obj.parent)

        return jsonify(
            {
                "path": str(path_obj),
                "parent": parent,
                "writable": bool(os.access(path_obj, os.W_OK)),
                "directories": directories,
                "truncated": truncated,
                "roots": common_roots,
            }
        )

    return blueprint
