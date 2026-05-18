"""Scan control and preflight API routes."""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from flask import Blueprint, jsonify, request


SCAN_CONTROL_COMPAT_ENDPOINTS = {
    "scan_preflight": "pmda_scan_control.scan_preflight",
    "providers_preflight": "pmda_scan_control.providers_preflight",
    "start_scan": "pmda_scan_control.start_scan",
    "api_scan_defaults": "pmda_scan_control.api_scan_defaults",
    "api_pipeline_bootstrap_status": "pmda_scan_control.api_pipeline_bootstrap_status",
    "pause_scan": "pmda_scan_control.pause_scan",
    "resume_scan": "pmda_scan_control.resume_scan",
    "stop_scan": "pmda_scan_control.stop_scan",
    "clear_scan": "pmda_scan_control.clear_scan",
}


def bind_scan_control_compat_aliases(*, runtime: Any, app: Any) -> None:
    """Expose legacy module-level scan control callables for MCP compatibility."""

    for attr_name, endpoint_name in SCAN_CONTROL_COMPAT_ENDPOINTS.items():
        setattr(runtime, attr_name, app.view_functions[endpoint_name])


def create_scan_control_blueprint(*, runtime: Any) -> Blueprint:
    """Create routes used to start, pause, resume, stop, and preflight scans."""

    blueprint = Blueprint("pmda_scan_control", __name__)

    @blueprint.get("/api/scan/preflight", endpoint="scan_preflight")
    def scan_preflight():
        """Check configured providers and return clear ok/error details for the UI."""

        mb_ok, ai_ok = runtime._run_preflight_checks()
        ai_runtime_ok, ai_runtime_provider, ai_runtime_auth, ai_runtime_reason = runtime._resolve_ai_runtime_availability(
            analysis_type="assistant_chat",
            requested_provider="openai",
            user_id=runtime._current_user_id_or_zero(),
        )
        ai_ok = bool(ai_ok or ai_runtime_ok)
        provider_name = getattr(runtime, "AI_PROVIDER", None) or "OpenAI"
        resolved_model = getattr(runtime, "RESOLVED_MODEL", None)
        ai_func_err = getattr(runtime, "AI_FUNCTIONAL_ERROR_MSG", None)
        if not bool(getattr(runtime, "USE_MUSICBRAINZ", False)):
            mb_msg = "MusicBrainz disabled in settings"
        elif mb_ok:
            mb_msg = "MusicBrainz reachable"
        else:
            mb_msg = "MusicBrainz unreachable"
        musicbrainz = {"ok": mb_ok, "message": mb_msg}
        if ai_ok:
            if ai_runtime_ok:
                ai_msg = f"{ai_runtime_provider} reachable ({ai_runtime_auth})"
            elif (getattr(runtime, "OPENAI_API_KEY", None) and getattr(runtime, "openai_client", None)):
                ai_msg = f"{provider_name} reachable" + (f", model {resolved_model} (params verified)" if resolved_model else "")
            else:
                ai_msg = f"{provider_name} configured"
        else:
            ai_msg = ai_runtime_reason or ai_func_err or "No API key or provider configured"
        ai_provider = {"ok": ai_ok, "message": ai_msg, "provider": provider_name}

        provider_checks = runtime._run_provider_preflights_parallel()
        discogs_ok, discogs_msg = provider_checks.get("discogs", (False, "No result"))
        lastfm_ok, lastfm_msg = provider_checks.get("lastfm", (False, "No result"))
        fanart_ok, fanart_msg = provider_checks.get("fanart", (False, "No result"))
        if getattr(runtime, "USE_BANDCAMP", False):
            bandcamp = {"ok": True, "message": "Configured (ultimate fallback, no connection test)"}
        else:
            bandcamp = {"ok": False, "message": "Disabled"}
        serper_ok, serper_msg = provider_checks.get("serper", (False, "No result"))
        acoustid_ok, acoustid_msg = provider_checks.get("acoustid", (False, "No result"))
        audiodb_ok, audiodb_msg = provider_checks.get("audiodb", (False, "No result"))
        return jsonify(
            musicbrainz=musicbrainz,
            ai=ai_provider,
            discogs={"ok": discogs_ok, "message": discogs_msg},
            lastfm={"ok": lastfm_ok, "message": lastfm_msg},
            fanart={"ok": fanart_ok, "message": fanart_msg},
            audiodb={"ok": audiodb_ok, "message": audiodb_msg},
            bandcamp=bandcamp,
            serper={"ok": serper_ok, "message": serper_msg},
            acoustid={"ok": acoustid_ok, "message": acoustid_msg},
            paths=runtime._paths_rw_status(),
        )

    @blueprint.get("/api/providers/preflight", endpoint="providers_preflight")
    def providers_preflight():
        checks = runtime._run_provider_preflights_parallel()
        return jsonify(
            discogs={"ok": checks["discogs"][0], "message": checks["discogs"][1]},
            lastfm={"ok": checks["lastfm"][0], "message": checks["lastfm"][1]},
            fanart={"ok": checks["fanart"][0], "message": checks["fanart"][1]},
            audiodb={"ok": checks["audiodb"][0], "message": checks["audiodb"][1]},
            serper={"ok": checks["serper"][0], "message": checks["serper"][1]},
            acoustid={"ok": checks["acoustid"][0], "message": checks["acoustid"][1]},
            checked_at=int(runtime.time.time()),
        )

    @blueprint.route("/scan/start", methods=["POST"], endpoint="start_scan")
    def start_scan():
        required = runtime._requires_config()
        if required is not None:
            return required
        data = request.get_json(silent=True) or {}
        default_scan_type = runtime.get_default_scan_type()
        raw_scan_type = str(data.get("scan_type") or "").strip().lower()
        scan_type = raw_scan_type or default_scan_type
        run_improve_after = bool(data.get("run_improve_after", False))
        if scan_type not in {"full", "changed_only", "incomplete_only"}:
            return jsonify({"error": f"Invalid scan_type: {scan_type}"}), 400
        bootstrap_required = bool(runtime._pipeline_bootstrap_status().get("bootstrap_required"))
        if scan_type == "changed_only" and bootstrap_required:
            return (
                jsonify(
                    {
                        "status": "blocked",
                        "reason": "bootstrap_required",
                        "message": "Initial full scan required before changed-only scans.",
                        "default_scan_type": default_scan_type,
                    }
                ),
                409,
            )

        if scan_type == "incomplete_only":
            runtime._run_incomplete_albums_scan()
            return (
                jsonify(
                    {
                        "status": "blocked",
                        "reason": "manual_incomplete_scan_removed",
                        "message": "Manual incomplete scans are disabled; PMDA records incompletes during the scan pipeline.",
                        "scan_type": "incomplete_only",
                    }
                ),
                400,
            )

        ai_enabled_for_start = bool(getattr(runtime, "ai_provider_ready", False))
        if not ai_enabled_for_start:
            try:
                uid = runtime._current_user_id_or_zero()
                effective_scan_provider = runtime._resolve_provider_for_runtime(
                    str(getattr(runtime, "AI_PROVIDER", None) or "openai"),
                    "provider_identity",
                    user_id=uid,
                )
                if (
                    effective_scan_provider == "openai-codex"
                    and runtime._openai_codex_oauth_mode_enabled()
                    and (runtime._openai_codex_profile_present(uid) or runtime._openai_codex_any_profile_present())
                ):
                    ai_enabled_for_start = True
            except Exception:
                pass
        ai_warning = None
        if not ai_enabled_for_start:
            ai_warning = getattr(runtime, "AI_FUNCTIONAL_ERROR_MSG", None) or "AI is not ready; scan will run without AI."
        runtime.scan_should_stop.clear()
        runtime.scan_is_paused.clear()
        ok, meta = runtime._try_begin_scan(
            scan_type=scan_type,
            source="interactive",
            run_improve_after=run_improve_after if scan_type in {"full", "changed_only"} else False,
            scheduler_run_id=None,
        )
        if not ok:
            reason = str(meta.get("reason") or "scan_already_running").strip() or "scan_already_running"
            if reason == "scan_already_running":
                return (
                    jsonify(
                        {
                            "status": "blocked",
                            "reason": "scan_already_running",
                            "active_scan_type": meta.get("active_scan_type"),
                            "started_at": meta.get("started_at"),
                        }
                    ),
                    409,
                )
            return (
                jsonify(
                    {
                        "status": "blocked",
                        "reason": reason,
                        "message": str(meta.get("message") or "Unable to start scan"),
                    }
                ),
                500,
            )
        return jsonify(
            {
                "status": "started",
                "scan_type": scan_type,
                "default_scan_type": default_scan_type,
                "run_improve_after": run_improve_after,
                "ai_enabled": bool(ai_enabled_for_start),
                "ai_warning": ai_warning,
            }
        )

    @blueprint.get("/api/scan/defaults", endpoint="api_scan_defaults")
    def api_scan_defaults():
        bootstrap = runtime._pipeline_bootstrap_status()
        return jsonify(
            {
                "default_scan_type": runtime.get_default_scan_type(),
                "bootstrap_required": bool(bootstrap.get("bootstrap_required")),
                "autonomous_mode": runtime._scan_autonomous_mode_effective(),
                "has_completed_full_scan": not bool(bootstrap.get("bootstrap_required")),
                "first_full_scan_id": bootstrap.get("first_full_scan_id"),
                "first_full_completed_at": bootstrap.get("first_full_completed_at"),
            }
        )

    @blueprint.get("/api/pipeline/bootstrap/status", endpoint="api_pipeline_bootstrap_status")
    def api_pipeline_bootstrap_status():
        bootstrap = runtime._pipeline_bootstrap_status()
        return jsonify(
            {
                "bootstrap_required": bool(bootstrap.get("bootstrap_required")),
                "autonomous_mode": runtime._scan_autonomous_mode_effective(),
                "first_full_scan_id": bootstrap.get("first_full_scan_id"),
                "first_full_completed_at": bootstrap.get("first_full_completed_at"),
                "default_scan_type": runtime.get_default_scan_type(),
            }
        )

    @blueprint.route("/scan/pause", methods=["POST"], endpoint="pause_scan")
    def pause_scan():
        with runtime.lock:
            runtime.state["scanning"] = True
            mode = runtime._get_library_mode()
            resume_run_id = (
                str(runtime.state.get("scan_resume_run_id") or "").strip()
                or str(runtime.state.get("scan_resume_requested_run_id") or "").strip()
                or None
            )
            discovery_running = bool(mode == "files" and runtime.state.get("scan_discovery_running"))
        previous_updated_at = None
        if discovery_running:
            try:
                previous_updated_at = float((runtime._copy_scan_discovery_runtime(resume_run_id) or {}).get("updated_at") or 0.0)
            except Exception:
                previous_updated_at = None
        runtime.scan_is_paused.set()
        if discovery_running:
            runtime._wait_for_discovery_runtime_update(
                resume_run_id,
                previous_updated_at=previous_updated_at,
                require_paused_ack=True,
                timeout_seconds=3.0,
            )
        snap = runtime._snapshot_current_resume_state("manual_pause")
        snapshot_triggered = bool(snap.get("ok"))
        snapshot_rows = int(snap.get("rows") or 0)
        snapshot_kind = str(snap.get("snapshot_kind") or "none")
        runtime._set_resume_run_status(resume_run_id, "paused")
        return jsonify(
            {
                "status": "ok",
                "snapshot_triggered": bool(snapshot_triggered),
                "snapshot_rows": snapshot_rows,
                "snapshot_kind": snapshot_kind,
            }
        )

    @blueprint.route("/scan/resume", methods=["POST"], endpoint="resume_scan")
    def resume_scan():
        runtime._reload_ai_config_and_reinit()
        body = request.get_json(silent=True) or {}
        requested_scan_type = str(body.get("scan_type") or "").strip().lower()
        runtime.scan_should_stop.clear()
        runtime.scan_is_paused.clear()
        with runtime.lock:
            scanning_now = bool(
                runtime.state.get("scanning") or runtime.state.get("scan_starting") or runtime.state.get("scan_finalizing")
            )
            current_scan_type = str(runtime.state.get("scan_type") or "full").strip().lower() or "full"
            run_improve_after = bool(runtime.state.get("run_improve_after", False))
            current_resume_run_id = str(runtime.state.get("scan_resume_run_id") or "").strip() or None
        if scanning_now:
            runtime._set_resume_run_status(current_resume_run_id, "running")
            return jsonify(
                {
                    "status": "ok",
                    "resume_run_id": current_resume_run_id,
                    "ai_provider": str(getattr(runtime, "AI_PROVIDER", None) or ""),
                    "ai_model": runtime._ai_model_display_name(getattr(runtime, "AI_PROVIDER", None)),
                }
            )

        current_mode = runtime._get_library_mode()
        candidate_scan_types: list[str] = []
        for candidate in [requested_scan_type, current_scan_type, "full", "changed_only"]:
            scan_type_norm = str(candidate or "").strip().lower()
            if scan_type_norm in {"full", "changed_only"} and scan_type_norm not in candidate_scan_types:
                candidate_scan_types.append(scan_type_norm)

        chosen_snapshot = None
        chosen_scan_type = None
        for candidate in candidate_scan_types:
            snap = runtime._get_resume_run_snapshot(current_mode, candidate)
            if not (isinstance(snap, dict) and snap.get("available")):
                snap = runtime._get_latest_resume_run_snapshot_any_signature(current_mode, candidate)
            if isinstance(snap, dict) and snap.get("available"):
                chosen_snapshot = snap
                chosen_scan_type = candidate
                break
        if not chosen_snapshot or not chosen_scan_type:
            return jsonify({"status": "blocked", "reason": "no_resume_available"}), 409

        requested_resume_run_id = str(chosen_snapshot.get("run_id") or "").strip() or None
        chosen_scan_id = int(chosen_snapshot.get("scan_id") or 0)
        try:
            reconciled = runtime._reconcile_scan_move_trace_backlog(reason="resume_scan")
            if reconciled:
                logging.info(
                    "[Trace] Resume preflight reconciled %s move trace row(s) before restarting scan_id=%s",
                    int(reconciled),
                    int(chosen_scan_id),
                )
        except Exception:
            logging.warning("[Trace] Resume preflight move trace reconcile failed", exc_info=True)
        with runtime.lock:
            runtime.state["scan_resume_requested_run_id"] = requested_resume_run_id

        ok, meta = runtime._try_begin_scan(
            scan_type=chosen_scan_type,
            source="interactive_resume",
            run_improve_after=run_improve_after,
            scheduler_run_id=None,
        )
        if not ok:
            with runtime.lock:
                if str(runtime.state.get("scan_resume_requested_run_id") or "").strip() == requested_resume_run_id:
                    runtime.state["scan_resume_requested_run_id"] = None
            reason = str(meta.get("reason") or "resume_start_failed").strip() or "resume_start_failed"
            http_status = 409 if reason == "scan_already_running" else 500
            return jsonify({"status": "blocked", "reason": reason, "message": str(meta.get("message") or "")}), http_status
        runtime._set_resume_run_status(str(chosen_snapshot.get("run_id") or ""), "running")
        return jsonify(
            {
                "status": "started",
                "scan_type": chosen_scan_type,
                "resume_run_id": str(chosen_snapshot.get("run_id") or ""),
                "ai_provider": str(getattr(runtime, "AI_PROVIDER", None) or ""),
                "ai_model": runtime._ai_model_display_name(getattr(runtime, "AI_PROVIDER", None)),
            }
        )

    @blueprint.route("/scan/stop", methods=["POST"], endpoint="stop_scan")
    def stop_scan():
        with runtime.lock:
            mode = runtime._get_library_mode()
            resume_run_id = (
                str(runtime.state.get("scan_resume_run_id") or "").strip()
                or str(runtime.state.get("scan_resume_requested_run_id") or "").strip()
                or None
            )
            discovery_running = bool(mode == "files" and runtime.state.get("scan_discovery_running"))
        previous_updated_at = None
        if discovery_running:
            try:
                previous_updated_at = float((runtime._copy_scan_discovery_runtime(resume_run_id) or {}).get("updated_at") or 0.0)
            except Exception:
                previous_updated_at = None
        runtime.scan_should_stop.set()
        if discovery_running:
            runtime._wait_for_discovery_runtime_update(
                resume_run_id,
                previous_updated_at=previous_updated_at,
                require_paused_ack=False,
                timeout_seconds=3.0,
            )
        snap = runtime._snapshot_current_resume_state("manual_stop")
        snapshot_triggered = bool(snap.get("ok"))
        snapshot_rows = int(snap.get("rows") or 0)
        snapshot_kind = str(snap.get("snapshot_kind") or "none")
        runtime._set_resume_run_status(resume_run_id, "stopped")
        return jsonify(
            {
                "status": "ok",
                "snapshot_triggered": bool(snapshot_triggered),
                "snapshot_rows": snapshot_rows,
                "snapshot_kind": snapshot_kind,
                "resume_preserved": bool(resume_run_id),
                "resume_run_id": resume_run_id,
            }
        )

    @blueprint.route("/api/scan/clear", methods=["POST"], endpoint="clear_scan")
    def clear_scan():
        data = request.get_json() or {}
        clear_audio_cache = data.get("clear_audio_cache", False)
        clear_mb_cache = data.get("clear_mb_cache", False)

        try:
            con = sqlite3.connect(str(runtime.STATE_DB_FILE))
            cur = con.cursor()
            cur.execute("DELETE FROM duplicates_loser")
            deleted_losers = cur.rowcount
            cur.execute("DELETE FROM duplicates_best")
            deleted_best = cur.rowcount
            cur.execute("DELETE FROM broken_albums")
            deleted_broken = cur.rowcount
            cur.execute("DELETE FROM scan_editions")
            deleted_editions = cur.rowcount
            cur.execute("DELETE FROM files_library_published_albums")
            deleted_published = cur.rowcount
            cur.execute("DELETE FROM files_pending_changes")
            deleted_pending_changes = cur.rowcount
            cur.execute("DELETE FROM settings WHERE key = 'last_completed_scan_id'")
            cur.execute("DELETE FROM settings WHERE key = 'last_completed_full_scan_id'")
            cur.execute(
                "UPDATE scan_history SET summary_json = NULL WHERE scan_id = "
                "(SELECT scan_id FROM scan_history WHERE status = 'completed' AND end_time IS NOT NULL "
                "ORDER BY end_time DESC LIMIT 1)"
            )
            con.commit()
            con.close()

            with runtime.lock:
                runtime.state["duplicates"] = {}
                runtime.state["scan_active_artists"] = {}
                fw = dict(runtime.state.get("files_watcher") or {})
                fw["dirty_count"] = 0
                fw["dirty_count_by_root"] = {}
                fw["last_event_at"] = None
                fw["last_event_path"] = None
                runtime.state["files_watcher"] = fw

            result = {
                "status": "ok",
                "message": "Scan results cleared successfully",
                "cleared": {
                    "duplicates_best": deleted_best,
                    "duplicates_loser": deleted_losers,
                    "broken_albums": deleted_broken,
                    "scan_editions": deleted_editions,
                    "files_library_published_albums": deleted_published,
                    "files_pending_changes": deleted_pending_changes,
                },
            }
            if runtime._get_library_mode() == "files":
                runtime._reset_files_live_index_for_scan(force=True)
            runtime._pipeline_bootstrap_reset()

            if clear_audio_cache:
                con = sqlite3.connect(str(runtime.CACHE_DB_FILE))
                cur = con.cursor()
                cur.execute("DELETE FROM audio_cache")
                audio_cache_deleted = cur.rowcount
                con.commit()
                con.close()
                result["cleared"]["audio_cache"] = audio_cache_deleted
                result["message"] += f", {audio_cache_deleted} audio cache entries cleared"

            if clear_mb_cache:
                con = sqlite3.connect(str(runtime.CACHE_DB_FILE))
                cur = con.cursor()
                cur.execute("DELETE FROM musicbrainz_cache")
                mb_cache_deleted = cur.rowcount
                cur.execute("DELETE FROM musicbrainz_album_lookup")
                mb_album_lookup_deleted = cur.rowcount
                cur.execute("DELETE FROM provider_album_lookup")
                provider_album_lookup_deleted = cur.rowcount
                con.commit()
                con.close()
                result["cleared"]["musicbrainz_cache"] = mb_cache_deleted
                result["cleared"]["musicbrainz_album_lookup"] = mb_album_lookup_deleted
                result["cleared"]["provider_album_lookup"] = provider_album_lookup_deleted
                result["message"] += (
                    f", {mb_cache_deleted} MB cache + {mb_album_lookup_deleted} album lookup cache"
                    f" + {provider_album_lookup_deleted} provider lookup cache cleared"
                )

            logging.info("Scan results cleared: %s", result)
            return jsonify(result)
        except Exception as e:
            logging.error("Failed to clear scan results: %s", e, exc_info=True)
            return jsonify({"status": "error", "message": str(e)}), 500

    return blueprint
