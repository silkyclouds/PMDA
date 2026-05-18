"""Pure arbitration helpers for provider identity decisions."""

from __future__ import annotations

from collections import Counter
from typing import Any

from .confidence import is_auto_materialization_safe


def summarize_candidate_tiers(candidates: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None) -> dict[str, int]:
    """Count match confidence tiers in an already annotated candidate list."""
    counter: Counter[str] = Counter()
    for candidate in candidates or []:
        if not isinstance(candidate, dict):
            continue
        tier = str(candidate.get("confidence_tier") or "").strip().lower()
        if not tier and isinstance(candidate.get("match_explanation"), dict):
            tier = str(candidate["match_explanation"].get("tier") or "").strip().lower()
        if tier:
            counter[tier] += 1
    return dict(counter)


def has_auto_materialization_candidate(
    candidates: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> bool:
    """Return True when at least one candidate carries strict move-safe proof."""
    for candidate in candidates or []:
        if not isinstance(candidate, dict):
            continue
        explanation = candidate.get("match_explanation")
        if is_auto_materialization_safe(explanation if isinstance(explanation, dict) else candidate):
            return True
    return False
