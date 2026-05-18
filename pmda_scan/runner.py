"""Thread launch helpers for the scan runner.

The full scan body is still being extracted incrementally. This module owns the
runtime boundary that starts scan work so API handlers do not instantiate scan
threads directly.
"""

from __future__ import annotations

from collections.abc import Callable
import threading
from typing import Any


def normalize_scan_type(scan_type: str | None) -> str:
    """Normalize supported scan types for thread names and runtime state."""

    value = str(scan_type or "full").strip().lower()
    return value if value in {"full", "changed_only"} else "full"


def start_scan_thread(target: Callable[[], Any], *, scan_type: str | None) -> threading.Thread:
    """Start the background scan thread and return the thread object."""

    scan_type_norm = normalize_scan_type(scan_type)
    thread = threading.Thread(target=target, daemon=True, name=f"scan-{scan_type_norm}")
    thread.start()
    return thread
