"""Logging helpers for PMDA.

The main application still owns the domain-specific coloured formatter because
it depends on scan vocabulary. Generic formatter, recent-buffer, and quiet-route
filtering live here so the monolith no longer owns every logging primitive.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Callable
from threading import Lock


ThreadNameHumanizer = Callable[[str], str]

_thread_name_humanizer: ThreadNameHumanizer = lambda value: str(value or "")


def set_thread_name_humanizer(humanizer: ThreadNameHumanizer) -> None:
    global _thread_name_humanizer
    _thread_name_humanizer = humanizer


def plain_log_record_line(record: logging.LogRecord) -> str:
    timestamp = time.strftime("%H:%M:%S", time.localtime(float(getattr(record, "created", time.time()) or time.time())))
    level = str(logging.getLevelName(int(getattr(record, "levelno", logging.INFO) or logging.INFO)) or "INFO").upper()
    thread_display = _thread_name_humanizer(str(getattr(record, "threadName", "") or ""))
    message = str(record.getMessage() or "")
    return f"{timestamp} │ {level} │ {thread_display} │ {message}"


class PlainLogFormatter(logging.Formatter):
    """Plain-text formatter for file logs and downloadable tails."""

    def format(self, record: logging.LogRecord) -> str:
        original_levelname = record.levelname
        original_thread = record.threadName
        try:
            record.levelname = str(logging.getLevelName(record.levelno) or original_levelname).upper()
            record.threadName = _thread_name_humanizer(original_thread)
            return super().format(record)
        finally:
            record.levelname = original_levelname
            record.threadName = original_thread


class RecentLogBufferHandler(logging.Handler):
    """Keeps a rolling in-memory buffer of recent plain-text backend logs."""

    def __init__(self, buffer: deque[str], buffer_lock: Lock) -> None:
        super().__init__()
        self._buffer = buffer
        self._buffer_lock = buffer_lock

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = plain_log_record_line(record)
        except Exception:
            return
        with self._buffer_lock:
            self._buffer.append(line)


class QuietPollingFilter(logging.Filter):
    """Drop request log lines for high-frequency polling routes."""

    def __init__(self, quiet_paths: tuple[str, ...]) -> None:
        super().__init__()
        self._quiet_paths = tuple(str(path) for path in quiet_paths)

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
            return not any(path in msg for path in self._quiet_paths)
        except Exception:
            return True
