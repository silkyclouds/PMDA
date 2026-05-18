"""Backward-compatible materialization policy helpers."""

from __future__ import annotations

from pmda_materialization.policy import (
    confidence_policy,
    has_trusted_album_identity,
    normalize_identity_provider,
)

__all__ = ["confidence_policy", "has_trusted_album_identity", "normalize_identity_provider"]
