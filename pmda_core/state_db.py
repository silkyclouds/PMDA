"""SQLite state database retry and WAL helpers.

PMDA's scan workers are highly concurrent while SQLite remains a single-writer
database. These helpers centralize the retry policy so write contention is
serialized and transient lock errors are retried instead of dropping progress.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from typing import TypeVar


T = TypeVar("T")

SQLITE_LOCK_FRAGMENTS = (
    "database is locked",
    "database table is locked",
    "database schema is locked",
    "database is busy",
)

STATE_DB_WRITE_LOCK = threading.RLock()


def is_sqlite_lock_error(exc: BaseException, *, lock_fragments: tuple[str, ...] = SQLITE_LOCK_FRAGMENTS) -> bool:
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    msg = str(exc or "").lower()
    return any(fragment in msg for fragment in lock_fragments)


def retry(
    operation: Callable[[], T],
    *,
    label: str,
    attempts: int = 10,
    base_sleep: float = 0.25,
    max_sleep: float = 5.0,
    lock_fragments: tuple[str, ...] = SQLITE_LOCK_FRAGMENTS,
) -> T:
    last_exc: sqlite3.OperationalError | None = None
    max_attempts = max(1, int(attempts or 1))
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            if not is_sqlite_lock_error(exc, lock_fragments=lock_fragments) or attempt >= max_attempts:
                raise
            last_exc = exc
            delay = min(float(max_sleep), float(base_sleep) * (2 ** (attempt - 1)))
            if attempt in (1, 3, 6) or attempt == max_attempts - 1:
                logging.warning(
                    "[STATE DB] SQLite busy during %s; retrying %s/%s in %.2fs: %s",
                    str(label or "operation"),
                    attempt,
                    max_attempts,
                    delay,
                    exc,
                )
            time.sleep(delay)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("state DB retry operation did not return")


def write_retry(
    operation: Callable[[], T],
    *,
    label: str,
    attempts: int = 10,
    write_lock: threading.RLock = STATE_DB_WRITE_LOCK,
) -> T:
    with write_lock:
        return retry(operation, label=label, attempts=attempts)


def enable_wal(
    con: sqlite3.Connection,
    *,
    busy_timeout_ms: int,
    label: str = "state-db",
    attempts: int = 10,
) -> None:
    def apply_pragmas() -> None:
        con.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)};")
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA wal_autocheckpoint=1000;")

    retry(apply_pragmas, label=f"{label}: enable WAL", attempts=attempts)
