"""Statistics and job-status API routes."""

from __future__ import annotations

from pathlib import Path
import json
import time
from typing import Any

from flask import Blueprint, jsonify, request


def create_statistics_blueprint(*, runtime: Any) -> Blueprint:
    """Create read-only statistics routes while keeping legacy URLs stable."""

    blueprint = Blueprint("pmda_statistics", __name__)

    @blueprint.get("/api/statistics/cache-control", endpoint="api_statistics_cache_control")
    def api_statistics_cache_control():
        force = runtime._parse_bool(request.args.get("force", "false"))
        return jsonify(runtime._collect_cache_control_metrics(force=force))

    @blueprint.get("/api/statistics/enrichment-sources", endpoint="api_statistics_enrichment_sources")
    def api_statistics_enrichment_sources():
        if runtime._get_library_mode() != "files":
            return jsonify(
                {
                    "available": False,
                    "reason": "files_mode_only",
                    "overall": {"total": 0, "providers": {}},
                    "album_profiles": {"total": 0, "providers": {}},
                    "artist_profiles": {"total": 0, "providers": {}},
                    "artist_images": {"total": 0, "providers": {}},
                    "label_logos": {"total": 0, "providers": {}},
                }
            )
        ok, err = runtime._ensure_files_index_ready()
        if not ok:
            return jsonify({"error": err or "Files index unavailable"}), 503
        conn = runtime._files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            def _provider_counts(sql: str) -> dict[str, int]:
                out: dict[str, int] = {}
                with conn.cursor() as cur:
                    cur.execute(sql)
                    for provider_raw, count_raw in cur.fetchall():
                        provider = runtime._normalize_identity_provider(str(provider_raw or "").strip()) or "unknown"
                        out[provider] = out.get(provider, 0) + int(count_raw or 0)
                return dict(sorted(out.items(), key=lambda item: (-item[1], item[0])))

            album_profiles = _provider_counts(
                """
                SELECT COALESCE(source, '') AS provider, COUNT(*)
                FROM files_album_profiles
                WHERE
                    BTRIM(COALESCE(description, '')) <> ''
                    OR BTRIM(COALESCE(short_description, '')) <> ''
                    OR COALESCE(tags_json, '[]') <> '[]'
                    OR COALESCE(public_rating_votes, 0) > 0
                    OR COALESCE(discogs_have_count, 0) > 0
                    OR COALESCE(discogs_want_count, 0) > 0
                    OR COALESCE(bandcamp_supporter_count, 0) > 0
                    OR COALESCE(lastfm_scrobbles, 0) > 0
                    OR COALESCE(lastfm_listeners, 0) > 0
                GROUP BY COALESCE(source, '')
                """
            )
            artist_profiles = _provider_counts(
                """
                SELECT COALESCE(source, '') AS provider, COUNT(*)
                FROM files_artist_profiles
                WHERE
                    BTRIM(COALESCE(bio, '')) <> ''
                    OR BTRIM(COALESCE(short_bio, '')) <> ''
                    OR COALESCE(tags_json, '[]') <> '[]'
                    OR COALESCE(similar_json, '[]') <> '[]'
                GROUP BY COALESCE(source, '')
                """
            )
            artist_images = _provider_counts(
                """
                SELECT COALESCE(provider, '') AS provider, COUNT(*)
                FROM files_external_artist_images
                WHERE BTRIM(COALESCE(image_path, '')) <> '' OR BTRIM(COALESCE(image_url, '')) <> ''
                GROUP BY COALESCE(provider, '')
                """
            )
            label_logos = _provider_counts(
                """
                SELECT COALESCE(provider, '') AS provider, COUNT(*)
                FROM files_external_label_images
                WHERE BTRIM(COALESCE(image_path, '')) <> '' OR BTRIM(COALESCE(image_url, '')) <> ''
                GROUP BY COALESCE(provider, '')
                """
            )
            overall: dict[str, int] = {}
            for bucket in (album_profiles, artist_profiles, artist_images, label_logos):
                for provider, count in bucket.items():
                    overall[provider] = overall.get(provider, 0) + int(count or 0)
            overall = dict(sorted(overall.items(), key=lambda item: (-item[1], item[0])))
            return jsonify(
                {
                    "available": True,
                    "overall": {"total": int(sum(overall.values())), "providers": overall},
                    "album_profiles": {"total": int(sum(album_profiles.values())), "providers": album_profiles},
                    "artist_profiles": {"total": int(sum(artist_profiles.values())), "providers": artist_profiles},
                    "artist_images": {"total": int(sum(artist_images.values())), "providers": artist_images},
                    "label_logos": {"total": int(sum(label_logos.values())), "providers": label_logos},
                }
            )
        finally:
            try:
                conn.close()
            except Exception:
                pass

    @blueprint.get("/api/statistics/scaling-runtime", endpoint="api_statistics_scaling_runtime")
    def api_statistics_scaling_runtime():
        target = runtime._musicbrainz_target_settings(probe_health=False)
        gateway = runtime._provider_gateway_stats_snapshot()
        metadata_summary = runtime._metadata_jobs_summary()
        auto_tune = runtime._runtime_auto_tune_snapshot()
        managed_runtime = runtime._managed_runtime_status_snapshot(include_candidates=False)
        mb_queue_pending = 0
        mb_queue_waiters = 0
        mb_queue_stats: dict[str, Any] = {}
        try:
            queue_obj = runtime.get_mb_queue()
            if bool(getattr(queue_obj, "enabled", False)):
                mb_queue_stats = queue_obj.stats_snapshot()
                mb_queue_pending = int(mb_queue_stats.get("queue_pending") or getattr(queue_obj, "queue").qsize())
                mb_queue_waiters = int(mb_queue_stats.get("queue_waiters") or len(getattr(queue_obj, "locks", {}) or {}))
        except Exception:
            mb_queue_pending = 0
            mb_queue_waiters = 0
            mb_queue_stats = {}
        with runtime.lock:
            runtime_sec = 0.0
            start_time = runtime.state.get("scan_start_time")
            if start_time:
                try:
                    runtime_sec = max(0.0, time.time() - float(start_time))
                except Exception:
                    runtime_sec = 0.0
            discovery_files = int(runtime.state.get("scan_discovery_files_found") or 0)
            discovery_audio = int(runtime.state.get("scan_discovery_audio_found") or 0)
            processed_albums = int(runtime.state.get("scan_processed_albums_count") or 0)
            published_albums = int(runtime.state.get("scan_published_albums_count") or 0)
            artists_done = int(runtime.state.get("scan_artists_processed") or 0)
            current_phase = str(runtime.state.get("scan_discovery_stage") or runtime.state.get("scan_phase") or "")

        def _rate(value: int) -> float:
            if runtime_sec <= 0 or value <= 0:
                return 0.0
            return round((float(value) / runtime_sec) * 3600.0, 2)

        return jsonify(
            {
                "musicbrainz": {
                    "enabled": bool(runtime.USE_MUSICBRAINZ),
                    "queue_enabled": bool(runtime.MB_QUEUE_ENABLED),
                    "mirror_enabled": bool(target["enabled"]),
                    "configured_mirror_enabled": bool(target.get("configured_enabled")),
                    "fallback_to_public": bool(target.get("fallback_to_public")),
                    "fallback_reason": str(target.get("fallback_reason") or ""),
                    "mirror_name": str(target["mirror_name"] or ""),
                    "base_url": str(target["base_url"] or ""),
                    "hostname": str(target["hostname"] or ""),
                    "configured_base_url": str(runtime.MUSICBRAINZ_BASE_URL or ""),
                    "mirror_health": dict(target.get("mirror_health") or {}),
                    "queue_pending": mb_queue_pending,
                    "queue_waiters": mb_queue_waiters,
                    "queue_workers": int(
                        mb_queue_stats.get("worker_count")
                        or (runtime.MB_MIRROR_QUEUE_WORKERS if bool(target["enabled"]) else 1)
                    ),
                    "public_rate_limit_per_sec": 1.0,
                    "current_rate_limit_per_sec": float(
                        mb_queue_stats.get("rate_limit_rps")
                        or (runtime.MB_MIRROR_QUEUE_RPS if bool(target["enabled"]) else runtime.MB_PUBLIC_QUEUE_RPS)
                    ),
                    "avg_latency_ms": float(mb_queue_stats.get("avg_latency_ms") or 0.0),
                    "last_latency_ms": float(mb_queue_stats.get("last_latency_ms") or 0.0),
                    "completed_count": int(mb_queue_stats.get("completed_count") or 0),
                    "error_count": int(mb_queue_stats.get("error_count") or 0),
                    "timeout_count": int(mb_queue_stats.get("timeout_count") or 0),
                },
                "provider_gateway": gateway,
                "metadata_workers": {
                    "queue_enabled": bool(runtime.METADATA_QUEUE_ENABLED),
                    "mode": str(runtime.METADATA_WORKER_MODE or "local"),
                    "worker_count": int(runtime.METADATA_WORKER_COUNT or 0),
                    "batch_size": int(runtime.METADATA_JOB_BATCH_SIZE or 0),
                    "worker_count_mode": "manual" if int(runtime.METADATA_WORKER_COUNT or 0) > 0 else "auto",
                    "batch_size_mode": "manual" if int(runtime.METADATA_JOB_BATCH_SIZE or 0) > 0 else "auto",
                    "effective_worker_count": runtime._metadata_worker_effective_count(),
                    "effective_batch_size": runtime._metadata_job_batch_effective_size(),
                    **metadata_summary,
                },
                "pipeline": {
                    "local_orchestrator": True,
                    "materialization_local": True,
                    "ocr_execution": "local",
                    "ai_mode": "ambiguous_only"
                    if str(runtime.SCAN_AI_POLICY or "local_only") != "aggressive"
                    else "aggressive",
                    "scan_threads": int(runtime.SCAN_THREADS or 0),
                    "ffprobe_pool_size": int(runtime.FFPROBE_POOL_SIZE or 0),
                    "match_cover_ocr_mode": str(runtime.MATCH_COVER_OCR_MODE or "smart"),
                    "ai_usage_level": str(runtime.AI_USAGE_LEVEL or "auto"),
                    "scan_ai_policy": str(runtime.SCAN_AI_POLICY or "local_only"),
                },
                "auto_tune": auto_tune,
                "managed_runtime": managed_runtime,
                "stage_rates": {
                    "runtime_sec": int(runtime_sec or 0),
                    "phase": current_phase or "",
                    "filesystem_entries_per_hour": _rate(discovery_files),
                    "audio_files_per_hour": _rate(discovery_audio),
                    "albums_processed_per_hour": _rate(processed_albums),
                    "albums_published_per_hour": _rate(published_albums),
                    "artists_processed_per_hour": _rate(artists_done),
                },
            }
        )

    @blueprint.get("/api/jobs/status", endpoint="api_jobs_status")
    def api_jobs_status():
        return jsonify(runtime._pmda_jobs_status_snapshot())

    @blueprint.get("/api/review/stats", endpoint="api_review_stats")
    def api_review_stats():
        """Return global duplicate/incomplete review counts for UI badges and MCP parity."""
        return jsonify(runtime._mcp_review_stats({}))

    @blueprint.get("/api/pipeline/jobs", endpoint="api_pipeline_jobs")
    def api_pipeline_jobs():
        """Return durable job state for scan/publication/materialization/index/background phases."""
        return jsonify({"jobs": runtime._pipeline_job_snapshot(), "generated_at": int(time.time())})

    @blueprint.get("/api/statistics/benchmark-reports", endpoint="api_statistics_benchmark_reports")
    def api_statistics_benchmark_reports():
        limit = runtime._parse_int_loose(request.args.get("limit"), 40) or 40
        limit = max(1, min(int(limit), 200))
        reports_dir = Path(str(runtime.PMDA_BENCHMARK_REPORTS_DIR or "/music/pmda_scan_benchmark/reports")).expanduser()
        if not reports_dir.exists() or not reports_dir.is_dir():
            return jsonify({"available": False, "path": str(reports_dir), "reports": []})

        report_files = sorted(
            [p for p in reports_dir.glob("scan_*.json") if p.is_file()],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        out_reports: list[dict[str, Any]] = []
        for path in report_files[:limit]:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            raw_checks = payload.get("checks") if isinstance(payload, dict) else []
            checks_out: list[dict[str, Any]] = []
            if isinstance(raw_checks, list):
                for chk in raw_checks:
                    if not isinstance(chk, dict):
                        continue
                    checks_out.append(
                        {
                            "name": str(chk.get("name") or ""),
                            "pass": bool(chk.get("pass")),
                            "weight": int(chk.get("weight") or 0),
                            "detail": str(chk.get("detail") or ""),
                        }
                    )
            pass_count = sum(1 for c in checks_out if c.get("pass"))
            fail_names = [str(c.get("name") or "") for c in checks_out if not c.get("pass")]
            try:
                generated_at = float(path.stat().st_mtime)
            except Exception:
                generated_at = 0.0
            out_reports.append(
                {
                    "file": path.name,
                    "generated_at": generated_at,
                    "scan_id": int((payload or {}).get("scan_id") or 0),
                    "score": float((payload or {}).get("score") or 0.0),
                    "earned_weight": int((payload or {}).get("earned_weight") or 0),
                    "total_weight": int((payload or {}).get("total_weight") or 0),
                    "check_count": len(checks_out),
                    "pass_count": int(pass_count),
                    "fail_count": max(0, len(checks_out) - int(pass_count)),
                    "failed_checks": fail_names,
                    "checks": checks_out,
                }
            )

        return jsonify({"available": True, "path": str(reports_dir), "reports": out_reports})

    return blueprint
