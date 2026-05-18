#!/usr/bin/env python3
"""Static release gate for the PMDA pipeline cleanup.

This gate complements pytest. It verifies that the major operator guarantees
remain represented in code, tests, MCP, and frontend entry points before a build
is considered releasable.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class AuditCheck:
    name: str
    ok: bool
    detail: str = ""


def read(rel: str) -> str:
    path = ROOT / rel
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def exists(rel: str) -> bool:
    return (ROOT / rel).exists()


def has_all(text: str, tokens: list[str]) -> bool:
    return all(token in text for token in tokens)


def check(name: str, condition: bool, detail: str = "") -> AuditCheck:
    return AuditCheck(name=name, ok=bool(condition), detail=detail if not condition else "ok")


def main() -> int:
    pmda = read("pmda.py")
    statistics_api = read("pmda_api/statistics.py")
    pmda_and_statistics = pmda + "\n" + statistics_api
    browse_state_runtime = read("pmda_library/browse_state_runtime.py")
    published_browse_runtime = read("pmda_library/published_browse_runtime.py")
    browse_pipeline_surface = pmda + "\n" + browse_state_runtime + "\n" + published_browse_runtime
    mcp = read("pmda_mcp/server.py")
    mcp_runtime = read("pmda_mcp/runtime.py")
    player_sync = read("pmda_integrations/player_sync.py")
    legacy_integrations = read("pmda_core/legacy_integrations.py")
    materialization_policy = read("pmda_core/materialization_policy.py")
    materialization_policy_new = read("pmda_materialization/policy.py")
    scan_resume_runtime = read("pmda_scan/resume_runtime.py")
    library_workflow_runtime = read("pmda_core/library_workflow_runtime.py")
    profile_support_runtime = read("pmda_enrichment/profile_support_runtime.py")
    provider_matching = read("pmda_core/provider_matching.py")
    matching_confidence = read("pmda_matching/confidence.py")
    pytest_ini = read("pytest.ini")
    library_query = read("frontend/src/hooks/useLibraryQuery.ts")
    scaling_settings = read("frontend/src/components/settings/ScalingSettings.tsx")
    library_pages = "\n".join(
        read(str(path.relative_to(ROOT)))
        for path in sorted((ROOT / "frontend" / "src" / "pages").glob("Library*.tsx"))
    )
    tests = {str(path.relative_to(ROOT)) for path in (ROOT / "tests").glob("test_*.py")}

    required_tests = {
        "tests/test_auth_rbac.py",
        "tests/test_files_index_status.py",
        "tests/test_files_publication_regressions.py",
        "tests/test_files_profile_backfill_runtime.py",
        "tests/test_library_scan_safe_fallbacks.py",
        "tests/test_mcp_access.py",
        "tests/test_provider_identity_arbitration.py",
        "tests/test_provider_lookup_optimization.py",
        "tests/test_player_sync.py",
        "tests/test_scan_progress_state.py",
        "tests/test_scaling_runtime.py",
        "tests/test_storage_power_saver.py",
        "tests/test_web_search_runtime.py",
    }
    missing_tests = sorted(required_tests - tests)

    mcp_tool_names = {
        match.group(1)
        for match in re.finditer(r'"name"\s*:\s*"([^"]+)"', mcp)
    }
    required_mcp_tools = {
        "pmda.jobs.status",
        "pmda.scan.analytics",
        "pmda.scan.results",
        "pmda.scan.pipeline_trace",
        "pmda.scan.moves",
        "pmda.storage.current",
        "pmda.storage.plan",
        "pmda.duplicates.list",
        "pmda.incompletes.list",
        "pmda.review.propose",
        "pmda.review.proposals",
    }
    missing_mcp = sorted(required_mcp_tools - mcp_tool_names)

    checks: list[AuditCheck] = [
        check(
            "pytest_collection_scoped",
            "testpaths = tests" in pytest_ini and "recovery" in pytest_ini and "norecursedirs" in pytest_ini,
            "pytest must collect tests/ only and ignore recovery snapshots",
        ),
        check("required_regression_tests_exist", not missing_tests, f"missing={missing_tests}"),
        check(
            "jobs_status_control_plane",
            has_all(pmda_and_statistics, ["/api/jobs/status", "_pmda_jobs_status_snapshot", "publication", "materialization", "library_index", "media_cache", "profile_backfill", "embeddings", "runtime_repair"]),
            "durable job status endpoint/categories missing",
        ),
        check(
            "browse_api_uses_fast_snapshots",
            has_all(browse_pipeline_surface, ["_files_library_api_browse_snapshot", "api_lightweight", "_files_library_published_artists", "_files_library_published_albums"]),
            "library browse API must have lightweight/published snapshot path",
        ),
        check(
            "browse_api_statement_timeout_safe_for_tests",
            'hasattr(cur, "_responses")' in pmda and "SET statement_timeout" in pmda,
            "statement timeout helper should not consume mocked result rows",
        ),
        check(
            "frontend_library_query_no_refresh_pollution",
            "refresh=1" not in library_query and "refresh=1" not in library_pages,
            "Library browse pages must not force refresh=1 auto-rebuild calls",
        ),
        check(
            "plex_is_player_sync_only",
            has_all(player_sync, ["PMDA no longer uses Plex as a source database", "check_plex", "trigger_plex_refresh"])
            and has_all(pmda, ["_ALLOW_PLEX_DB_IN_FILES_MODE = False", "Plex source DB integration is disabled", "Plex DB access is disabled", "Skipping Plex source discovery; files mode is the only scan backend."])
            and '"plex", "jellyfin", "navidrome"' in pmda,
            "Plex may only be used as a post-publication player refresh target, never as a source DB",
        ),
        check(
            "lidarr_autobrr_acquisition_removed",
            has_all(pmda, ["def _lidarr_feature_enabled() -> bool:", "def _autobrr_feature_enabled() -> bool:", "Compatibility stub for the removed Lidarr acquisition workflow", "Compatibility stub for the removed Autobrr acquisition workflow"])
            and has_all(legacy_integrations, ["def lidarr_feature_enabled() -> bool:", "def autobrr_feature_enabled() -> bool:", "return False", "LIDARR_DISABLED_MESSAGE", "AUTOBRR_DISABLED_MESSAGE"])
            and "Lidarr integration is currently disabled" in legacy_integrations
            and "Autobrr integration is currently disabled" in legacy_integrations,
            "Lidarr/Autobrr acquisition must stay removed while old API routes return 410 for compatibility",
        ),
        check(
            "mcp_stdio_and_tools",
            "opens no listener" in mcp and not missing_mcp,
            f"missing_mcp_tools={missing_mcp}",
        ),
        check(
            "mcp_security_and_audit",
            has_all(pmda + mcp_runtime, ["MCP_ENABLED", "mcp_service_tokens", "mcp_audit_log", "mcp_review_proposals"]) and "PMDA_MCP_TOKEN" in mcp,
            "MCP toggle/token/audit/proposals must exist",
        ),
        check(
            "mcp_review_proposals_are_non_destructive",
            has_all(mcp, ["review_propose", "Create a duplicate/incomplete review proposal without moving files"]) and "mcp_review_proposals" in (pmda + mcp_runtime),
            "MCP review proposal path must not move files directly",
        ),
        check(
            "global_dupes_and_incompletes",
            has_all(pmda + mcp_runtime, ["_mcp_duplicate_groups", "_mcp_incomplete_albums", "incomplete_album_diagnostics", "duplicate groups"]),
            "global duplicate/incomplete read models missing",
        ),
        check(
            "materialization_confidence_policy",
            "_materialization_confidence_policy" in pmda
            and (
                has_all(materialization_policy, ["strict_mb", "strong_provider", "soft_provider", "ai_review", "unresolved"])
                or has_all(materialization_policy_new, ["strict_mb", "strong_provider", "soft_provider", "ai_review", "unresolved"])
            ),
            "materialization confidence tiers missing",
        ),
        check(
            "materialization_move_audit",
            has_all(pmda + mcp_runtime, ["scan_moves", "source_path", "destination_path", "materialization_strategy", "arbitration_result", "confidence_tier"]),
            "move audit fields missing",
        ),
        check(
            "trusted_intake_workflow",
            has_all(pmda + library_workflow_runtime, ["LIBRARY_SERVING_ROOT", "LIBRARY_INTAKE_ROOTS", "is_winner_root", "_library_workflow_scope_roots"])
            and "Music_matched" in (pmda + library_workflow_runtime),
            "trusted destination/intake workflow missing",
        ),
        check(
            "disk_aware_schema_and_resume",
            has_all(pmda + scan_resume_runtime, ["scan_storage_buckets", "storage_device_id", "storage_access_path", "_persist_resume_files_plan", "_restore_resume_files_plan_from_run_row"]),
            "disk-aware bucket/resume storage missing",
        ),
        check(
            "disk_aware_materialization_and_backfill",
            has_all(
                pmda + profile_support_runtime,
                [
                    "_storage_should_defer_live_library_materialization",
                    "_storage_materialization_plan_entries",
                    "_storage_ordered_materialization_groups",
                    "Files profile backfill delayed",
                ],
            ),
            "disk-aware constraints must apply beyond matching",
        ),
        check(
            "matching_intelligence_tiers_visible",
            "_provider_candidate_match_classification" in pmda
            and has_all(pmda + mcp_runtime, ["confidence_tiers", "confidence_tier_percent"])
            and (
                has_all(provider_matching, ["safe_for_auto_materialization", "strict_mb", "strong_provider", "soft_provider", "ai_review", "unresolved"])
                or has_all(matching_confidence, ["safe_for_auto_materialization", "strict_mb", "strong_provider", "soft_provider", "ai_review", "unresolved"])
            ),
            "matching analytics/confidence classification missing",
        ),
        check(
            "musicbrainz_runtime_no_request_probe_on_stats",
            "_musicbrainz_target_settings(probe_health=False)" in pmda_and_statistics and "configured_mirror_enabled" in pmda_and_statistics,
            "scaling-runtime must report config without live mirror probe",
        ),
        check(
            "ollama_runtime_management_visible",
            has_all(pmda, ["_managed_runtime_detect_ollama_candidates", "_managed_runtime_ensure_ollama_models", "_managed_runtime_bootstrap_ollama", "managed_runtime", "qwen3:4b", "qwen3:14b"])
            and has_all(scaling_settings, ["Ollama local bundle", "Pull fast model", "qwen3:4b", "qwen3:14b"]),
            "Ollama adoption/provisioning surface missing",
        ),
    ]

    ok = all(item.ok for item in checks)
    payload = {
        "ok": ok,
        "checks": [item.__dict__ for item in checks],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
