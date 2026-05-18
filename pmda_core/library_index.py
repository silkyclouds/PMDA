"""Backward-compatible library index helpers.

New publication/index code should import from :mod:`pmda_publication`. This
module remains for older call sites and tests while the monolith is split.
"""

from __future__ import annotations

from pmda_publication.index_rebuild import (
    index_is_running,
    merge_index_state,
    progress_metrics,
    status_payload,
)

__all__ = ["index_is_running", "merge_index_state", "progress_metrics", "status_payload"]
