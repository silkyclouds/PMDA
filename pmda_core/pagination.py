"""Pagination helpers shared by API browse endpoints."""

from __future__ import annotations


def page_has_more(*, total: int | None, offset: int | None, returned: int | None) -> bool:
    """Return whether a paginated API page has more rows after this response."""
    try:
        total_i = max(0, int(total or 0))
        offset_i = max(0, int(offset or 0))
        returned_i = max(0, int(returned or 0))
    except (TypeError, ValueError):
        return False
    return (offset_i + returned_i) < total_i
