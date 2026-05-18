#!/usr/bin/env python3
"""Fail fast when removed PMDA legacy integrations leak back into active code.

Allowed:
- Plex as a post-publication player refresh target.
- Legacy database column names that are needed for migrations/compatibility.
- Compatibility API routes that return HTTP 410 for removed Lidarr/Autobrr flows.

Forbidden:
- Plex as a source database/library backend.
- Any active Lidarr/Autobrr acquisition network calls.
- User-facing frontend/docs text that suggests Lidarr/Autobrr are still product
  features.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class GateResult:
    name: str
    ok: bool
    detail: str = "ok"


def read(rel: str) -> str:
    path = ROOT / rel
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def grep_tree(pattern: str, *roots: str) -> list[str]:
    regex = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
    matches: list[str] = []
    for root in roots:
        base = ROOT / root
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file() or any(part in {".git", "node_modules", "dist", "build", "__pycache__"} for part in path.parts):
                continue
            rel = str(path.relative_to(ROOT))
            text = path.read_text(encoding="utf-8", errors="replace")
            for index, line in enumerate(text.splitlines(), start=1):
                if regex.search(line):
                    matches.append(f"{rel}:{index}:{line.strip()}")
    return matches


def grep_tree_excluding(pattern: str, roots: list[str], *, allowed_files: set[str]) -> list[str]:
    regex = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
    matches: list[str] = []
    for root in roots:
        base = ROOT / root
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file() or any(part in {".git", "node_modules", "dist", "build", "__pycache__"} for part in path.parts):
                continue
            rel = str(path.relative_to(ROOT))
            if rel in allowed_files:
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            for index, line in enumerate(text.splitlines(), start=1):
                if regex.search(line):
                    matches.append(f"{rel}:{index}:{line.strip()}")
    return matches


def main() -> int:
    pmda = read("pmda.py")
    config_core = read("pmda_core/config.py")
    player_sync = read("pmda_integrations/player_sync.py")
    legacy_integrations = read("pmda_core/legacy_integrations.py")

    active_lidarr_network = re.findall(r"requests\.(?:get|post|put|delete|request)\([^\n]*(?:LIDARR|AUTOBRR|lidarr|autobrr)", pmda)
    frontend_lidarr_text = grep_tree(r"\b(?:Lidarr|Autobrr)\b", "frontend/src", "docs")
    extracted_legacy_source_refs = grep_tree_excluding(
        r"\b(?:plex_connect|PLEX_DB_PATH|metadata_items|library_sections)\b",
        [
            "pmda_api",
            "pmda_core",
            "pmda_dedupe",
            "pmda_discovery",
            "pmda_enrichment",
            "pmda_incompletes",
            "pmda_library",
            "pmda_materialization",
            "pmda_matching",
            "pmda_publication",
            "pmda_scan",
        ],
        allowed_files={"pmda_core/legacy_integrations.py"},
    )
    allowed_frontend_lidarr = [
        item for item in frontend_lidarr_text
        if "sent_to_lidarr" in item or "AUTOBRR_FEATURE_ENABLED" in item or "LIDARR_FEATURE_ENABLED" in item
    ]
    unexpected_frontend_lidarr = [item for item in frontend_lidarr_text if item not in allowed_frontend_lidarr]
    removed_startup_plex_checks = [
        needle
        for needle in (
            "Plex DB reachable",
            "Plex connection OK",
            "Self-diagnostic failed in Plex mode",
            "startup_mode = _get_library_mode()",
            "Run Plex validation, self-diagnostic and path cross-check",
        )
        if needle in pmda
    ]
    removed_artist_tag_fixer = [
        needle
        for needle in (
            "Updated tags for {albums_updated}",
            "mutagen library not installed. Please install it to fix tags.",
            "Get all albums for this artist (selected sections only)",
            "from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC, TCON, APIC, TXXX",
            "Legacy artist-wide Plex tag fixer is disabled in Files mode. Use the Files album improvement endpoints instead.\\n    if not PLEX_CONFIGURED",
        )
        if needle in pmda
    ]

    results = [
        GateResult(
            "plex_player_refresh_module_exists",
            "trigger_plex_refresh" in player_sync and "PMDA no longer uses Plex as a source database" in player_sync,
            "Plex support must live in pmda_integrations.player_sync and be documented as player refresh only",
        ),
        GateResult(
            "files_mode_forces_no_plex_source_db",
            "def normalize_library_mode" in config_core
            and "return \"files\"" in config_core
            and "Plex source DB integration is disabled" in pmda
            and "Plex DB access is disabled" in pmda,
            "PMDA must force files mode and block Plex DB access",
        ),
        GateResult(
            "no_active_lidarr_autobrr_network_calls",
            not active_lidarr_network,
            f"unexpected={active_lidarr_network[:5]}",
        ),
        GateResult(
            "no_user_facing_lidarr_autobrr_copy",
            not unexpected_frontend_lidarr,
            f"unexpected={unexpected_frontend_lidarr[:10]}",
        ),
        GateResult(
            "legacy_routes_are_compatibility_only",
            "Lidarr integration is currently disabled" in legacy_integrations
            and "Autobrr integration is currently disabled" in legacy_integrations
            and "Compatibility stub for the removed Lidarr acquisition workflow" in pmda
            and "Compatibility stub for the removed Autobrr acquisition workflow" in pmda,
            "Old routes may remain only as disabled compatibility endpoints",
        ),
        GateResult(
            "extracted_modules_do_not_reintroduce_plex_source_db",
            not extracted_legacy_source_refs,
            f"unexpected={extracted_legacy_source_refs[:10]}",
        ),
        GateResult(
            "legacy_plex_startup_checks_removed",
            not removed_startup_plex_checks,
            f"unexpected={removed_startup_plex_checks}",
        ),
        GateResult(
            "legacy_plex_artist_tag_fixer_removed",
            not removed_artist_tag_fixer,
            f"unexpected={removed_artist_tag_fixer}",
        ),
    ]

    ok = all(result.ok for result in results)
    print(json.dumps({"ok": ok, "checks": [result.__dict__ for result in results]}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
