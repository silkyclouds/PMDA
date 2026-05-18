from __future__ import annotations

import sqlite3

from pmda_core import state_db


def test_sqlite_lock_detection_is_specific() -> None:
    assert state_db.is_sqlite_lock_error(sqlite3.OperationalError("database is locked"))
    assert state_db.is_sqlite_lock_error(sqlite3.OperationalError("database table is locked"))
    assert not state_db.is_sqlite_lock_error(sqlite3.OperationalError("no such table: x"))
    assert not state_db.is_sqlite_lock_error(RuntimeError("database is locked"))


def test_retry_retries_transient_lock(monkeypatch) -> None:
    monkeypatch.setattr(state_db.time, "sleep", lambda _seconds: None)
    calls = {"count": 0}

    def operation() -> str:
        calls["count"] += 1
        if calls["count"] < 3:
            raise sqlite3.OperationalError("database is locked")
        return "ok"

    assert state_db.retry(operation, label="unit", attempts=5) == "ok"
    assert calls["count"] == 3


def test_retry_does_not_retry_non_lock_errors(monkeypatch) -> None:
    monkeypatch.setattr(state_db.time, "sleep", lambda _seconds: None)
    calls = {"count": 0}

    def operation() -> None:
        calls["count"] += 1
        raise sqlite3.OperationalError("no such table: missing")

    try:
        state_db.retry(operation, label="unit", attempts=5)
    except sqlite3.OperationalError as exc:
        assert "no such table" in str(exc)
    else:
        raise AssertionError("expected OperationalError")
    assert calls["count"] == 1


def test_enable_wal_applies_expected_pragmas(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    con = sqlite3.connect(db_path)
    try:
        state_db.enable_wal(con, busy_timeout_ms=5000, label="unit")
        mode = con.execute("PRAGMA journal_mode").fetchone()[0]
        timeout = con.execute("PRAGMA busy_timeout").fetchone()[0]
    finally:
        con.close()

    assert str(mode).lower() == "wal"
    assert timeout == 5000
