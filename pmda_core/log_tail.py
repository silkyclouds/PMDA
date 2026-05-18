"""Utilities for reading and structuring PMDA backend log tails."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import logging
import os
import re
import zlib


LogMessageClassifier = Callable[[str, str], tuple[str, str]]


def tail_log_lines(path: Path, lines: int = 200, max_bytes: int = 512 * 1024) -> list[str]:
    """Return the last log lines from *path*, stripping ANSI escape codes."""

    if lines <= 0:
        return []
    ansi_re = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
    try:
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            read_size = min(max_bytes, size)
            if read_size > 0:
                fh.seek(-read_size, os.SEEK_END)
            raw = fh.read()
    except FileNotFoundError:
        return []
    except Exception:
        logging.debug("Could not tail log file %s", path, exc_info=True)
        return []
    out = raw.decode("utf-8", "replace").splitlines()
    if len(out) > lines:
        out = out[-lines:]
    return [ansi_re.sub("", line) for line in out]


_LOG_TAIL_LINE_RE = re.compile(
    r"^(?P<timestamp>\d{2}:\d{2}:\d{2})\s+│\s+(?P<level>[A-Z ]+?)\s+│\s+(?P<thread>[^│]+?)\s+│\s+(?P<message>.*)$"
)

_SCAN_LOG_NOISE_HINTS = (
    "assistant_chat/",
    "settings saved to settings.db",
    "settings updated (wizard already completed)",
    "checkpoint starting:",
    "checkpoint complete:",
)

_SCAN_LOG_RELEVANT_HINTS = (
    "[scan]",
    "[mb]",
    "[match]",
    "[miss]",
    "[soft]",
    "[providers]",
    "[acoustid]",
    "[post]",
    "[publish]",
    "[storage]",
    "worker queue primed",
    "matched-library move",
    "musicbrainz",
    "processing artist:",
    "resume:",
    "files library live sync",
    "strict matched",
    "background scan started",
    "scan [",
    "no release group found",
    "fallback:",
)

_SCAN_LOG_RELEVANT_THREAD_HINTS = (
    "scan:",
    "worker",
    "index rebuild",
    "files discovery",
    "files-profile-backfill",
    "profile-enrich",
    "provider fallback",
)


def stable_log_thread_slot(thread_label: str) -> int:
    """Return a stable palette slot for a log thread label."""

    raw = str(thread_label or "").strip()
    if not raw:
        return 0
    match = re.search(r"(\d+)$", raw)
    if match:
        try:
            return int(match.group(1)) % 12
        except Exception:
            pass
    return zlib.crc32(raw.encode("utf-8", errors="ignore")) % 12


@dataclass(frozen=True)
class LogTailParser:
    """Parse PMDA log tail lines into frontend-ready entries."""

    classify_message: LogMessageClassifier

    def kind_from_message(self, level: str, message: str) -> tuple[str, str]:
        return self.classify_message(level, message)

    def entries_from_lines(self, raw_lines: list[str]) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for raw_line in raw_lines:
            line = str(raw_line or "")
            match = _LOG_TAIL_LINE_RE.match(line)
            if not match:
                kind, marker = self.kind_from_message("", line)
                entries.append(
                    {
                        "raw": line,
                        "timestamp": "",
                        "level": "",
                        "thread": "",
                        "thread_key": "",
                        "thread_slot": 0,
                        "message": line,
                        "kind": kind,
                        "marker": marker,
                    }
                )
                continue
            timestamp = str(match.group("timestamp") or "")
            level = str(match.group("level") or "").strip()
            thread = str(match.group("thread") or "").strip()
            message = str(match.group("message") or "")
            kind, marker = self.kind_from_message(level, message)
            entries.append(
                {
                    "raw": line,
                    "timestamp": timestamp,
                    "level": level,
                    "thread": thread,
                    "thread_key": thread.lower(),
                    "thread_slot": stable_log_thread_slot(thread),
                    "message": message,
                    "kind": kind,
                    "marker": marker,
                }
            )
        return entries

    def entry_is_scan_relevant(self, entry: dict[str, Any]) -> bool:
        message = str(entry.get("message") or "").strip().lower()
        thread_key = str(entry.get("thread_key") or "").strip().lower()
        if not message and not thread_key:
            return False
        if any(hint in message for hint in _SCAN_LOG_NOISE_HINTS):
            return False
        if any(hint in thread_key for hint in _SCAN_LOG_RELEVANT_THREAD_HINTS):
            return True
        if any(hint in message for hint in _SCAN_LOG_RELEVANT_HINTS):
            return True
        if str(entry.get("kind") or "") in {"match", "miss", "soft", "scan", "provider", "warning"} and thread_key != "http":
            return True
        return False

    def entries(self, path: Path, lines: int = 200, max_bytes: int = 512 * 1024) -> list[dict[str, Any]]:
        return self.entries_from_lines(tail_log_lines(path, lines=lines, max_bytes=max_bytes))
