"""Pure scan orchestration decisions.

The scan runner remains responsible for side effects. This module owns the
small policy decisions that should be stable, testable, and shared between the
HTTP API, MCP, scheduler, and logs.
"""

from __future__ import annotations

from typing import Any


PIPELINE_JOB_TYPES = (
    "scan",
    "publication",
    "materialization",
    "library_index",
    "media_cache",
    "profile_backfill",
    "embeddings",
    "runtime_repair",
)

PIPELINE_JOB_STALE_AFTER_SEC = 15 * 60
CONTENT_SCAN_TYPES = {"full", "changed_only"}
PIPELINE_FLAG_ORDER = ("match_fix", "dedupe", "incomplete_move", "export", "player_sync")
PIPELINE_FLAG_KEYS = {*PIPELINE_FLAG_ORDER, "sync_target"}


def normalize_scan_type(scan_type: Any) -> str:
    """Normalize user/API scan type values."""
    scan_kind = str(scan_type or "full").strip().lower()
    return scan_kind if scan_kind in CONTENT_SCAN_TYPES else "full"


def pipeline_async_enabled(scan_type: Any, post_scan_async: Any) -> bool:
    """Return whether post-scan jobs should be deferred for this scan."""
    return bool(post_scan_async and normalize_scan_type(scan_type) in CONTENT_SCAN_TYPES)


def summarize_pipeline_flags(flags: dict[str, Any] | None) -> str:
    """Build the concise flag summary used in operator logs."""
    if not isinstance(flags, dict) or not flags:
        return "none"
    parts: list[str] = []
    for key in PIPELINE_FLAG_ORDER:
        if key in flags:
            parts.append(f"{key}={'yes' if bool(flags.get(key)) else 'no'}")
    sync_target = str(flags.get("sync_target") or "").strip()
    if sync_target:
        parts.append(f"sync_target={sync_target}")
    for key in sorted(k for k in flags.keys() if k not in PIPELINE_FLAG_KEYS):
        parts.append(f"{key}={flags.get(key)!r}")
    return ", ".join(parts) if parts else "none"


def resolve_pipeline_flags(
    scan_type: Any,
    run_improve_after_requested: Any,
    *,
    pipeline_enable_match_fix: Any,
    pipeline_enable_dedupe: Any,
    pipeline_enable_incomplete_move: Any,
    pipeline_enable_export: Any,
    pipeline_enable_player_sync: Any,
    auto_move_dupes: Any,
    magic_mode: Any,
    audit_mode: Any,
    sync_target: str,
) -> dict[str, bool | str]:
    """Resolve effective pipeline flags for a scan without side effects."""
    scan_kind = normalize_scan_type(scan_type)
    scan_is_content = scan_kind in CONTENT_SCAN_TYPES
    run_match_fix = bool(scan_is_content and (pipeline_enable_match_fix or run_improve_after_requested))
    auto_move_dup = bool(auto_move_dupes or magic_mode)
    run_dedupe = bool(scan_is_content and pipeline_enable_dedupe and auto_move_dup)
    run_incomplete_move = bool(scan_is_content and pipeline_enable_incomplete_move)
    run_export = bool(scan_is_content and pipeline_enable_export)
    run_player_sync = bool(scan_is_content and pipeline_enable_player_sync)
    if bool(audit_mode):
        run_match_fix = bool(scan_is_content and run_improve_after_requested)
        run_dedupe = False
        run_incomplete_move = False
        run_export = False
        run_player_sync = False
    sync_target_norm = str(sync_target or "none").strip().lower() or "none"
    if sync_target_norm == "none":
        run_player_sync = False
    return {
        "match_fix": run_match_fix,
        "dedupe": run_dedupe,
        "incomplete_move": run_incomplete_move,
        "export": run_export,
        "player_sync": run_player_sync,
        "sync_target": sync_target_norm,
    }


def inline_pipeline_flags(
    requested: dict[str, bool | str] | None,
    *,
    pipeline_async_enabled: bool,
) -> dict[str, bool | str]:
    """Return the subset of pipeline flags that must run inline."""
    flags = dict(requested or {})
    if pipeline_async_enabled:
        # Keep truth-changing validation inline; defer heavy publication/media sync.
        flags.update(match_fix=False, export=False, player_sync=False)
    return flags


def scan_pipeline_active_from_state(state: dict[str, Any] | None) -> bool:
    """Return whether any scan lifecycle phase is active."""
    current = state or {}
    return bool(
        current.get("scanning")
        or current.get("scan_starting")
        or current.get("scan_finalizing")
        or current.get("scan_post_processing")
        or current.get("scan_profile_enrich_running")
    )


def scan_inline_matching_active_from_state(state: dict[str, Any] | None) -> bool:
    """Return whether inline matching/discovery should be considered active."""
    current = state or {}
    return bool(
        current.get("scan_discovery_running")
        or current.get("scanning")
        or current.get("scan_starting")
        or current.get("scan_finalizing")
        or current.get("scan_post_processing")
    )
