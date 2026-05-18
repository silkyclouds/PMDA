#!/usr/bin/env python3
"""Guarded autonomous modularization runner.

This script does not rewrite code by itself. It is the phase gate used between
refactor passes: compile, static gates, focused tests, and optionally full
backend/frontend checks. When checks pass it records the completed phase and
prints the next phase so the operator/agent can continue safely.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = ROOT / ".tmp" / "autonomy" / "modularization_state.json"

PHASES = [
    "backup_autonomy",
    "materialization_mover",
    "scan_control_runtime",
    "api_blueprints",
    "scan_runner",
    "filesystem_discovery",
    "matching_providers",
    "publication_index",
    "materialization_services",
    "global_reviews",
    "post_publication_enrichment",
    "state_schema_core",
    "publication_schema",
    "publication_artist_maintenance",
    "legacy_acquisition_blueprint",
    "statistics_blueprint",
    "files_cache_blueprint",
    "files_sources_blueprint",
    "library_browse_blueprint",
    "scan_history_blueprint",
    "scan_moves_blueprint",
    "legacy_incomplete_scan_removed",
    "incomplete_albums_blueprint",
    "library_stats_blueprint",
    "library_normalization_blueprint",
    "library_index_control_blueprint",
    "library_index_status_blueprint",
    "profile_backfill_blueprint",
    "publication_reconcile_blueprint",
    "runtime_ai_blueprint",
    "settings_config_blueprint",
    "user_feedback_blueprint",
    "player_blueprint",
    "admin_ops_blueprint",
    "auth_admin_blueprint",
    "mcp_admin_blueprint",
    "dedupe_details_blueprint",
    "scan_control_blueprint",
    "legacy_cleanup_final",
    "pmda_bootstrap_reduction",
    "state_db_core",
    "logging_core",
    "legacy_boundaries",
    "player_sync_core",
    "config_core",
    "library_index_core",
    "scan_progress_core",
    "scan_orchestrator_core",
    "pipeline_jobs_core",
    "materialization_policy_core",
    "provider_matching_core",
    "scan_moves_core",
]

COMPILE_TARGETS = [
    "pmda.py",
    "pmda_core/config.py",
    "pmda_core/library_index.py",
    "pmda_core/materialization_policy.py",
    "pmda_core/pipeline_jobs.py",
    "pmda_core/pipeline_jobs_runtime.py",
    "pmda_core/provider_matching.py",
    "pmda_core/runtime_tuning.py",
    "pmda_core/scan_moves.py",
    "pmda_core/scan_progress.py",
    "pmda_core/scan_orchestrator.py",
    "pmda_core/scheduler_runtime.py",
    "pmda_core/schema.py",
    "pmda_core/state_db.py",
    "pmda_core/logging_utils.py",
    "pmda_core/legacy_integrations.py",
    "pmda_integrations/player_sync.py",
    "pmda_api/admin_ops.py",
    "pmda_api/auth_admin.py",
    "pmda_api/dedupe_details.py",
    "pmda_api/files_cache.py",
    "pmda_api/files_sources.py",
    "pmda_api/incomplete_albums.py",
    "pmda_api/legacy_acquisition.py",
    "pmda_api/library_browse.py",
    "pmda_api/library_index_control.py",
    "pmda_api/library_index_status.py",
    "pmda_api/library_normalization.py",
    "pmda_api/library_stats.py",
    "pmda_api/logs.py",
    "pmda_api/mcp_admin.py",
    "pmda_api/player.py",
    "pmda_api/profile_backfill.py",
    "pmda_api/publication_reconcile.py",
    "pmda_api/runtime_ai.py",
    "pmda_api/scan_control.py",
    "pmda_api/scan_history.py",
    "pmda_api/scan_moves.py",
    "pmda_api/settings_config.py",
    "pmda_api/statistics.py",
    "pmda_api/tools_runtime.py",
    "pmda_api/user_feedback.py",
    "pmda_ai/assistant_chat_runtime.py",
    "pmda_ai/provider_config_runtime.py",
    "pmda_ai/artist_roles_runtime.py",
    "pmda_ai/guardrails_runtime.py",
    "pmda_ai/web_search_runtime.py",
    "pmda_ai/usage_runtime.py",
    "pmda_core/logging_runtime.py",
    "pmda_core/managed_runtime.py",
    "pmda_core/cache_telemetry_runtime.py",
    "pmda_core/auth_runtime.py",
    "pmda_core/library_workflow_runtime.py",
    "pmda_integrations/lastfm_runtime.py",
    "pmda_dedupe/review.py",
    "pmda_dedupe/actions_runtime.py",
    "pmda_dedupe/broken_runtime.py",
    "pmda_dedupe/cards_runtime.py",
    "pmda_dedupe/choose_best_runtime.py",
    "pmda_dedupe/move_runtime.py",
    "pmda_dedupe/perform_runtime.py",
    "pmda_dedupe/signal_runtime.py",
    "pmda_discovery/files_editions_runtime.py",
    "pmda_discovery/storage_bucket_runtime.py",
    "pmda_discovery/filesystem_walk_runtime.py",
    "pmda_discovery/audio_runtime.py",
    "pmda_discovery/source_roots_runtime.py",
    "pmda_discovery/storage_buckets.py",
    "pmda_enrichment/artist_profile_runtime.py",
    "pmda_enrichment/artwork_runtime.py",
    "pmda_enrichment/external_image_cache_runtime.py",
    "pmda_enrichment/media_cache_runtime.py",
    "pmda_enrichment/profile_support_runtime.py",
    "pmda_enrichment/profiles.py",
    "pmda_enrichment/scan_targets_runtime.py",
    "pmda_enrichment/status.py",
    "pmda_incompletes/ai_runtime.py",
    "pmda_incompletes/review.py",
    "pmda_incompletes/move_runtime.py",
    "pmda_library/album_review_lookup_runtime.py",
    "pmda_library/album_media_runtime.py",
    "pmda_library/browse_runtime.py",
    "pmda_library/catalog_runtime.py",
    "pmda_library/catalog_stats_runtime.py",
    "pmda_library/classical_runtime.py",
    "pmda_library/detail_runtime.py",
    "pmda_library/improve_batch_runtime.py",
    "pmda_library/improve_runtime.py",
    "pmda_library/personal_runtime.py",
    "pmda_library/recommendation_runtime.py",
    "pmda_library/published_browse_runtime.py",
    "pmda_materialization/audit.py",
    "pmda_materialization/export_rebuild_runtime.py",
    "pmda_materialization/export_runtime.py",
    "pmda_materialization/helpers_runtime.py",
    "pmda_materialization/mover.py",
    "pmda_materialization/policy.py",
    "pmda_materialization/strict_export_runtime.py",
    "pmda_mcp/runtime.py",
    "pmda_matching/arbitration.py",
    "pmda_matching/confidence.py",
    "pmda_matching/discogs_runtime.py",
    "pmda_matching/provider_gateway_runtime.py",
    "pmda_matching/provider_fallback_runtime.py",
    "pmda_matching/provider_identity_runtime.py",
    "pmda_matching/identity_runtime.py",
    "pmda_matching/musicbrainz_runtime.py",
    "pmda_matching/public_album_providers_runtime.py",
    "pmda_publication/artist_maintenance.py",
    "pmda_publication/artist_browse_runtime.py",
    "pmda_publication/artist_identity_runtime.py",
    "pmda_publication/artist_merge_runtime.py",
    "pmda_publication/artist_publish_runtime.py",
    "pmda_publication/cache_quality_runtime.py",
    "pmda_publication/cover_runtime.py",
    "pmda_publication/index_rebuild.py",
    "pmda_publication/index_rebuild_runtime.py",
    "pmda_publication/index_status_runtime.py",
    "pmda_publication/published_payload_runtime.py",
    "pmda_publication/row_runtime.py",
    "pmda_publication/reconcile_runtime.py",
    "pmda_publication/snapshot.py",
    "pmda_publication/schema.py",
    "pmda_scan/bootstrap_runtime.py",
    "pmda_scan/control.py",
    "pmda_scan/control_runtime.py",
    "pmda_scan/history_runtime.py",
    "pmda_scan/move_audit_runtime.py",
    "pmda_scan/persistence_runtime.py",
    "pmda_scan/pipeline_trace_runtime.py",
    "pmda_scan/reconciliation_runtime.py",
    "pmda_scan/resume_runtime.py",
    "pmda_scan/runner.py",
    "scripts/pmda_bootstrap_gate.py",
]

FOCUSED_TESTS = [
    "tests/test_config_core.py",
    "tests/test_library_index_core.py",
    "tests/test_materialization_policy_core.py",
    "tests/test_pipeline_jobs_core.py",
    "tests/test_provider_matching_core.py",
    "tests/test_scan_moves_core.py",
    "tests/test_scan_progress_core.py",
    "tests/test_scan_orchestrator_core.py",
    "tests/test_state_db_utils.py",
    "tests/test_logging_utils.py",
    "tests/test_legacy_integrations.py",
    "tests/test_api_legacy_acquisition_blueprint.py",
    "tests/test_api_files_cache_blueprint.py",
    "tests/test_api_files_sources_blueprint.py",
    "tests/test_api_library_browse_blueprint.py",
    "tests/test_api_incomplete_albums_blueprint.py",
    "tests/test_api_library_index_control_blueprint.py",
    "tests/test_api_library_index_status_blueprint.py",
    "tests/test_api_library_normalization_blueprint.py",
    "tests/test_api_library_stats_blueprint.py",
    "tests/test_api_profile_backfill_blueprint.py",
    "tests/test_api_publication_reconcile_blueprint.py",
    "tests/test_api_runtime_ai_blueprint.py",
    "tests/test_api_scan_control_blueprint.py",
    "tests/test_api_scan_history_blueprint.py",
    "tests/test_api_scan_moves_blueprint.py",
    "tests/test_api_settings_config_blueprint.py",
    "tests/test_api_statistics_blueprint.py",
    "tests/test_api_user_feedback_blueprint.py",
    "tests/test_player_sync.py",
    "tests/test_library_scan_safe_fallbacks.py",
    "tests/test_materialization_mover.py",
    "tests/test_materialization_services.py",
    "tests/test_scan_control.py",
    "tests/test_scan_runner.py",
    "tests/test_api_logs_blueprint.py",
    "tests/test_api_player_blueprint.py",
    "tests/test_api_admin_ops_blueprint.py",
    "tests/test_api_auth_admin_blueprint.py",
    "tests/test_auth_rbac.py",
    "tests/test_api_mcp_admin_blueprint.py",
    "tests/test_api_dedupe_details_blueprint.py",
    "tests/test_storage_buckets.py",
    "tests/test_matching_confidence.py",
    "tests/test_publication_artist_maintenance.py",
    "tests/test_publication_snapshot.py",
    "tests/test_publication_schema.py",
    "tests/test_global_reviews.py",
    "tests/test_enrichment_services.py",
    "tests/test_schema_bootstrap.py",
]


def run(cmd: list[str], *, cwd: Path = ROOT) -> None:
    print(f"[guard] $ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=cwd, check=True)


def python_bin() -> str:
    candidate = ROOT / ".venv-codex-tests" / "bin" / "python"
    if candidate.exists():
        return str(candidate)
    return sys.executable


def read_state() -> dict:
    if not STATE_PATH.exists():
        return {"completed": []}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"completed": []}


def write_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def next_phase_after(completed: list[str]) -> str | None:
    seen = set(completed)
    for phase in PHASES:
        if phase not in seen:
            return phase
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=PHASES, required=True)
    parser.add_argument("--full", action="store_true", help="Run full backend pytest and frontend build.")
    parser.add_argument("--focused-only", action="store_true", help="Skip slow checks even if --full is absent.")
    args = parser.parse_args()

    py = python_bin()
    tests_run = []
    run(["python3", "-m", "py_compile", *COMPILE_TARGETS])
    run(["python3", "scripts/pipeline_audit_gate.py"])
    run(["python3", "scripts/legacy_cleanup_gate.py"])
    run(["python3", "scripts/pmda_bootstrap_gate.py"])
    run([py, "-m", "pytest", *FOCUSED_TESTS, "-q"])
    tests_run.extend(FOCUSED_TESTS)

    if args.full:
        run([py, "-m", "pytest", "-q"])
        tests_run.append("full pytest")
        run(["npm", "run", "build"], cwd=ROOT / "frontend")
        tests_run.append("frontend build")

    state = read_state()
    completed = list(dict.fromkeys([*state.get("completed", []), args.phase]))
    state.update(
        {
            "completed": completed,
            "last_phase": args.phase,
            "last_ok_at": int(time.time()),
            "next_phase": next_phase_after(completed),
            "status": "green",
            "tests_run": tests_run,
            "resume_instruction": "Continue with --phase "
            + str(next_phase_after(completed))
            if next_phase_after(completed)
            else "All registered phases are complete.",
        }
    )
    write_state(state)
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
