"""Scheduler and task-event API routes."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any

from flask import Blueprint, jsonify, request


def create_scheduler_blueprint(*, runtime: Any) -> Blueprint:
    """Create scheduler/task-event routes while preserving legacy URLs."""

    blueprint = Blueprint("pmda_scheduler", __name__)

    @blueprint.get("/api/events/tasks", endpoint="api_task_events")
    def api_task_events():
        try:
            after_id = max(0, int(request.args.get("after_id", 0) or 0))
        except Exception:
            after_id = 0
        try:
            limit = max(1, min(500, int(request.args.get("limit", 100) or 100)))
        except Exception:
            limit = 100
        try:
            con = runtime._state_connect_readonly(timeout=3)
            cur = con.cursor()
            cur.execute(
                """
                SELECT event_id, run_id, job_type, scope, status, message, metrics_json, summary_json, error,
                       source, started_at, ended_at, duration_ms
                FROM task_events
                WHERE event_id > ?
                ORDER BY event_id ASC
                LIMIT ?
                """,
                (after_id, limit),
            )
            rows = cur.fetchall()
            cur.execute("SELECT MAX(event_id) AS max_id FROM task_events")
            max_row = cur.fetchone()
            con.close()
            events = []
            for row in rows:
                metrics = {}
                summary = {}
                try:
                    metrics = json.loads(row["metrics_json"] or "{}")
                except Exception:
                    metrics = {}
                try:
                    summary = json.loads(row["summary_json"] or "{}")
                except Exception:
                    summary = {}
                events.append(
                    {
                        "event_id": int(row["event_id"] or 0),
                        "run_id": row["run_id"],
                        "job_type": str(row["job_type"] or ""),
                        "scope": str(row["scope"] or "both"),
                        "status": str(row["status"] or ""),
                        "message": str(row["message"] or ""),
                        "metrics": metrics,
                        "summary": summary,
                        "error": str(row["error"] or ""),
                        "source": str(row["source"] or ""),
                        "started_at": float(row["started_at"] or 0.0),
                        "ended_at": float(row["ended_at"] or 0.0) if row["ended_at"] is not None else None,
                        "duration_ms": int(row["duration_ms"] or 0) if row["duration_ms"] is not None else None,
                    }
                )
            last_id = int(max_row["max_id"] or 0) if max_row else 0
            runtime._task_events_cache_merge(events, max_id=last_id)
            return jsonify(
                {
                    "events": events,
                    "last_id": last_id,
                    "server_time": time.time(),
                    "stale": False,
                }
            )
        except sqlite3.OperationalError as exc:
            logging.debug("api_task_events: returning cached response after SQLite lock: %s", exc)
            events, last_id, _cached_ts = runtime._task_events_cache_read(after_id=after_id, limit=limit)
            return jsonify(
                {
                    "events": events,
                    "last_id": int(last_id or 0),
                    "server_time": time.time(),
                    "stale": True,
                }
            )

    @blueprint.get("/api/scheduler/rules", endpoint="api_scheduler_rules_get")
    def api_scheduler_rules_get():
        rules = [runtime._scheduler_rule_to_dict(r) for r in runtime._scheduler_rules_fetch()]
        return jsonify({"rules": rules, "paused": bool(runtime._scheduler_paused)})

    @blueprint.put("/api/scheduler/rules", endpoint="api_scheduler_rules_put")
    def api_scheduler_rules_put():
        data = request.get_json(silent=True) or {}
        rules_payload = data.get("rules")
        if not isinstance(rules_payload, list):
            return jsonify({"error": "Body must include rules: []"}), 400
        rules = runtime._scheduler_rules_replace([r for r in rules_payload if isinstance(r, dict)])
        return jsonify({"status": "ok", "rules": rules})

    @blueprint.post("/api/scheduler/jobs/run", endpoint="api_scheduler_jobs_run")
    def api_scheduler_jobs_run():
        data = request.get_json(silent=True) or {}
        job_type = runtime._normalize_task_job_type(data.get("job_type"))
        scope = runtime._normalize_task_scope(data.get("scope"), default="both")
        source = str(data.get("source") or "manual").strip().lower() or "manual"
        if not job_type:
            return jsonify({"error": "Invalid job_type"}), 400
        ok, reason, run_id = runtime._scheduler_launch_job(job_type, scope, source, rule_id=None)
        if not ok:
            return jsonify({"status": "blocked", "message": reason, "run_id": None}), 409
        return jsonify({"status": "started", "message": reason, "run_id": run_id})

    @blueprint.post("/api/scheduler/jobs/pause", endpoint="api_scheduler_jobs_pause")
    def api_scheduler_jobs_pause():
        runtime._scheduler_set_paused(True)
        return jsonify({"status": "ok", "paused": True})

    @blueprint.post("/api/scheduler/jobs/resume", endpoint="api_scheduler_jobs_resume")
    def api_scheduler_jobs_resume():
        runtime._scheduler_set_paused(False)
        runtime._start_scheduler_if_needed()
        return jsonify({"status": "ok", "paused": False})

    @blueprint.get("/api/scheduler/jobs/status", endpoint="api_scheduler_jobs_status")
    def api_scheduler_jobs_status():
        with runtime._scheduler_lock:
            running = list(runtime._scheduler_running_meta.values())
            alive = bool(runtime._scheduler_thread is not None and runtime._scheduler_thread.is_alive())
        con = runtime._state_connect(timeout=10)
        cur = con.cursor()
        cur.execute(
            """
            SELECT job_run_id, rule_id, job_type, scope, source, status, message, metrics_json, error,
                   origin_scan_id,
                   created_at, started_at, ended_at, duration_ms
            FROM scheduler_jobs
            ORDER BY created_at DESC
            LIMIT 80
            """
        )
        rows = cur.fetchall()
        con.close()
        jobs = []
        for row in rows:
            metrics = {}
            try:
                metrics = json.loads(row["metrics_json"] or "{}")
            except Exception:
                metrics = {}
            jobs.append(
                {
                    "job_run_id": row["job_run_id"],
                    "rule_id": int(row["rule_id"] or 0) if row["rule_id"] is not None else None,
                    "job_type": str(row["job_type"] or ""),
                    "scope": str(row["scope"] or "both"),
                    "source": str(row["source"] or ""),
                    "status": str(row["status"] or ""),
                    "message": str(row["message"] or ""),
                    "metrics": metrics,
                    "error": str(row["error"] or ""),
                    "origin_scan_id": int(row["origin_scan_id"] or 0) if row["origin_scan_id"] is not None else None,
                    "created_at": float(row["created_at"] or 0.0),
                    "started_at": float(row["started_at"] or 0.0) if row["started_at"] is not None else None,
                    "ended_at": float(row["ended_at"] or 0.0) if row["ended_at"] is not None else None,
                    "duration_ms": int(row["duration_ms"] or 0) if row["duration_ms"] is not None else None,
                }
            )
        return jsonify(
            {
                "paused": bool(runtime._scheduler_paused),
                "thread_alive": alive,
                "running": running,
                "jobs": jobs,
            }
        )

    return blueprint
