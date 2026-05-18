"""Cooperative scan control primitives.

The historical runtime exposed module-level ``threading.Event`` objects from
``pmda.py``. This module keeps the behavior explicit while allowing the scan
runner to be extracted incrementally.
"""

from __future__ import annotations

from dataclasses import dataclass
import threading
import time


@dataclass(slots=True)
class ScanRuntime:
    """Mutable cooperative controls shared by scan loops and API handlers."""

    stop_event: threading.Event
    pause_event: threading.Event

    @classmethod
    def create(cls) -> "ScanRuntime":
        return cls(stop_event=threading.Event(), pause_event=threading.Event())

    def reset(self) -> None:
        self.stop_event.clear()
        self.pause_event.clear()

    def pause(self) -> None:
        self.pause_event.set()

    def resume(self) -> None:
        self.stop_event.clear()
        self.pause_event.clear()

    def stop(self) -> None:
        self.stop_event.set()

    @property
    def is_paused(self) -> bool:
        return self.pause_event.is_set()

    @property
    def is_stopping(self) -> bool:
        return self.stop_event.is_set()


def create_scan_runtime() -> ScanRuntime:
    """Return a fresh scan runtime with stop/pause events."""

    return ScanRuntime.create()


def wait_if_paused(
    *,
    pause_event: threading.Event,
    stop_event: threading.Event,
    sleep_seconds: float = 0.2,
) -> bool:
    """Wait while paused and return ``False`` if a stop was requested."""

    delay = max(0.05, float(sleep_seconds or 0.2))
    while pause_event.is_set() and not stop_event.is_set():
        time.sleep(delay)
    return not stop_event.is_set()
