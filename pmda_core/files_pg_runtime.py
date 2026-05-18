"""Runtime PostgreSQL connection helpers for the files library index."""

from __future__ import annotations

import logging
import time
import weakref
from contextlib import contextmanager
from typing import Any


def files_pg_connect_kwargs_for_runtime(runtime: Any) -> dict:
    idle_ms = int(max(15000.0, float(runtime._FILES_PG_IDLE_REAP_SEC or 90.0) * 1000.0))
    return {
        "host": runtime.PMDA_PG_HOST,
        "port": runtime.PMDA_PG_PORT,
        "dbname": runtime.PMDA_PG_DB,
        "user": runtime.PMDA_PG_USER,
        "password": runtime.PMDA_PG_PASSWORD,
        "connect_timeout": 5,
        "application_name": "pmda-files",
        "options": f"-c idle_session_timeout={idle_ms} -c idle_in_transaction_session_timeout={idle_ms}",
    }


def files_pg_conn_is_idle_for_runtime(runtime: Any, conn: Any) -> bool:
    try:
        info = getattr(conn, "info", None)
        status = getattr(info, "transaction_status", None)
        if status is None:
            return True
        psycopg = getattr(runtime, "psycopg", None)
        pq_mod = getattr(psycopg, "pq", None) if psycopg is not None else None
        idle_marker = getattr(getattr(pq_mod, "TransactionStatus", None), "IDLE", None)
        if idle_marker is not None:
            return status == idle_marker
        status_name = str(getattr(status, "name", "") or status).strip().upper()
        return status_name in {"IDLE", "0"}
    except Exception:
        return True


def files_pg_register_connection_for_runtime(runtime: Any, conn: Any, *, release_bg_gate: bool = False) -> int:
    token = int(next(runtime._FILES_PG_CONN_TOKEN_SEQ))
    now = time.monotonic()
    with runtime._FILES_PG_CONN_REGISTRY_LOCK:
        runtime._FILES_PG_CONN_REGISTRY[token] = {
            "conn": conn,
            "release_bg_gate": bool(release_bg_gate),
            "created_at": now,
            "last_touch": now,
            "thread": str(runtime.threading.current_thread().name or ""),
        }
    return token


def files_pg_touch_connection_for_runtime(runtime: Any, token: int) -> None:
    if int(token or 0) <= 0:
        return
    with runtime._FILES_PG_CONN_REGISTRY_LOCK:
        entry = runtime._FILES_PG_CONN_REGISTRY.get(int(token))
        if entry is not None:
            entry["last_touch"] = time.monotonic()


def files_pg_release_connection_by_token_for_runtime(runtime: Any, token: int, *, close_conn: bool = True) -> None:
    if int(token or 0) <= 0:
        return
    with runtime._FILES_PG_CONN_REGISTRY_LOCK:
        entry = runtime._FILES_PG_CONN_REGISTRY.pop(int(token), None)
    if not entry:
        return
    conn = entry.get("conn")
    release_bg_gate = bool(entry.get("release_bg_gate"))
    try:
        if close_conn and conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    finally:
        with runtime._FILES_PG_CONN_STATE_LOCK:
            runtime._FILES_PG_CONN_ACTIVE = max(0, int(runtime._FILES_PG_CONN_ACTIVE) - 1)
        try:
            runtime._FILES_PG_CONN_GATE.release()
        except Exception:
            pass
        if release_bg_gate:
            try:
                runtime._FILES_PG_BG_CONN_GATE.release()
            except Exception:
                pass


def files_pg_reap_stale_connections_for_runtime(
    runtime: Any,
    *,
    idle_timeout_sec: float | None = None,
    closed_only: bool = False,
    log_reason: str = "",
) -> int:
    timeout = max(5.0, float(idle_timeout_sec or runtime._FILES_PG_IDLE_REAP_SEC or 90.0))
    now = time.monotonic()
    stale_tokens: list[int] = []
    with runtime._FILES_PG_CONN_REGISTRY_LOCK:
        for token, entry in list(runtime._FILES_PG_CONN_REGISTRY.items()):
            conn = entry.get("conn")
            try:
                conn_closed = bool(getattr(conn, "closed", False))
            except Exception:
                conn_closed = True
            if conn_closed:
                stale_tokens.append(int(token))
                continue
            if closed_only:
                continue
            last_touch = float(entry.get("last_touch") or entry.get("created_at") or now)
            if (now - last_touch) < timeout:
                continue
            if files_pg_conn_is_idle_for_runtime(runtime, conn):
                stale_tokens.append(int(token))
    for token in stale_tokens:
        files_pg_release_connection_by_token_for_runtime(runtime, token, close_conn=True)
    if stale_tokens and log_reason:
        logging.warning(
            "Files PG: reaped %d stale connection(s) during %s",
            len(stale_tokens),
            log_reason,
        )
    return len(stale_tokens)


class FilesPgCursorProxy:
    __slots__ = ("_cursor", "_touch")

    def __init__(self, cursor: Any, touch_cb: Any) -> None:
        self._cursor = cursor
        self._touch = touch_cb

    def __enter__(self):
        self._touch()
        self._cursor.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._touch()
        return self._cursor.__exit__(exc_type, exc, tb)

    def __getattr__(self, name: str):
        attr = getattr(self._cursor, name)
        if callable(attr) and name in {
            "execute",
            "executemany",
            "fetchone",
            "fetchmany",
            "fetchall",
            "copy",
            "stream",
            "close",
        }:
            def _wrapped(*args, **kwargs):
                self._touch()
                result = attr(*args, **kwargs)
                self._touch()
                return result
            return _wrapped
        return attr


class FilesPgTransactionProxy:
    __slots__ = ("_tx", "_touch")

    def __init__(self, tx: Any, touch_cb: Any) -> None:
        self._tx = tx
        self._touch = touch_cb

    def __enter__(self):
        self._touch()
        return self._tx.__enter__()

    def __exit__(self, exc_type, exc, tb):
        self._touch()
        return self._tx.__exit__(exc_type, exc, tb)


class FilesPgConnectionProxy:
    __slots__ = ("_runtime", "_conn", "_token", "_finalizer", "_closed", "__weakref__")

    def __init__(self, runtime: Any, conn: Any, *, release_bg_gate: bool = False) -> None:
        self._runtime = runtime
        self._conn = conn
        self._token = files_pg_register_connection_for_runtime(runtime, conn, release_bg_gate=release_bg_gate)
        self._finalizer = weakref.finalize(
            self,
            files_pg_release_connection_by_token_for_runtime,
            runtime,
            self._token,
            close_conn=True,
        )
        self._closed = False

    def _touch(self) -> None:
        files_pg_touch_connection_for_runtime(self._runtime, self._token)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._finalizer()

    def cursor(self, *args, **kwargs):
        self._touch()
        return FilesPgCursorProxy(self._conn.cursor(*args, **kwargs), self._touch)

    def transaction(self, *args, **kwargs):
        self._touch()
        return FilesPgTransactionProxy(self._conn.transaction(*args, **kwargs), self._touch)

    def __getattr__(self, name: str):
        attr = getattr(self._conn, name)
        if callable(attr) and name in {"execute", "commit", "rollback"}:
            def _wrapped(*args, **kwargs):
                self._touch()
                result = attr(*args, **kwargs)
                self._touch()
                return result
            return _wrapped
        return attr

    def __enter__(self):
        self._touch()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False


def files_pg_connect_for_runtime(
    runtime: Any,
    *,
    autocommit: bool = True,
    acquire_timeout_sec: float | None = None,
):
    psycopg = getattr(runtime, "psycopg", None)
    if psycopg is None:
        runtime._FILES_PG_LAST_ERROR = "psycopg_not_installed"
        return None
    acquired = False
    acquired_bg = False
    acquire_timeout = max(0.2, float(acquire_timeout_sec if acquire_timeout_sec is not None else runtime._FILES_PG_ACQUIRE_TIMEOUT_SEC))
    request_ctx = bool(runtime.has_request_context())
    try:
        files_pg_reap_stale_connections_for_runtime(runtime, closed_only=False, log_reason="pre-acquire")
        if (not request_ctx) and runtime._FILES_PG_UI_RESERVED_CONNS > 0:
            acquired_bg = runtime._FILES_PG_BG_CONN_GATE.acquire(timeout=acquire_timeout)
            if not acquired_bg:
                files_pg_reap_stale_connections_for_runtime(runtime, closed_only=False, log_reason="bg-pool-timeout")
                acquired_bg = runtime._FILES_PG_BG_CONN_GATE.acquire(timeout=min(1.0, acquire_timeout))
            if not acquired_bg:
                runtime._FILES_PG_LAST_ERROR = "background_pool_reserved"
                logging.debug(
                    "Files PG background connection deferred: reserved %d slot(s) for requests (%d active / max %d)",
                    int(runtime._FILES_PG_UI_RESERVED_CONNS),
                    int(runtime._FILES_PG_CONN_ACTIVE),
                    int(runtime._FILES_PG_MAX_CONNS),
                )
                return None
        acquired = runtime._FILES_PG_CONN_GATE.acquire(timeout=acquire_timeout)
        if not acquired:
            files_pg_reap_stale_connections_for_runtime(runtime, closed_only=False, log_reason="pool-timeout")
            acquired = runtime._FILES_PG_CONN_GATE.acquire(timeout=min(1.0, acquire_timeout))
        if not acquired:
            runtime._FILES_PG_LAST_ERROR = "pool_timeout"
            logging.warning(
                "Files PG connection delayed: app pool exhausted (%d active / max %d)",
                int(runtime._FILES_PG_CONN_ACTIVE),
                int(runtime._FILES_PG_MAX_CONNS),
            )
            return None
        conn = psycopg.connect(**files_pg_connect_kwargs_for_runtime(runtime), autocommit=autocommit)
        with runtime._FILES_PG_CONN_STATE_LOCK:
            runtime._FILES_PG_CONN_ACTIVE += 1
        runtime._FILES_PG_LAST_OK_TS = time.time()
        runtime._FILES_PG_LAST_ERROR = ""
        return FilesPgConnectionProxy(runtime, conn, release_bg_gate=acquired_bg)
    except Exception as e:
        if acquired:
            try:
                runtime._FILES_PG_CONN_GATE.release()
            except Exception:
                pass
        if acquired_bg:
            try:
                runtime._FILES_PG_BG_CONN_GATE.release()
            except Exception:
                pass
        err_kind = e.__class__.__name__
        runtime._FILES_PG_LAST_ERROR = err_kind
        logging.warning(
            "Files PG connection failed (%s, active=%d/%d)",
            err_kind,
            int(runtime._FILES_PG_CONN_ACTIVE),
            int(runtime._FILES_PG_MAX_CONNS),
        )
        return None


def files_pg_error_text(exc: Any) -> str:
    try:
        return " ".join(str(exc or "").split()).strip().lower()
    except Exception:
        return ""


def files_pg_is_connection_dropped_error(exc: Any) -> bool:
    if exc is None:
        return False
    cls_name = str(getattr(exc.__class__, "__name__", "") or "").strip().lower()
    text = files_pg_error_text(exc)
    markers = (
        "server closed the connection unexpectedly",
        "terminating connection due to",
        "consuming input failed",
        "connection is closed",
        "connection already closed",
        "broken pipe",
        "connection reset by peer",
        "admin shutdown",
    )
    if any(marker in text for marker in markers):
        return True
    return cls_name in {"operationalerror", "interfaceerror"} and (
        "connection" in text or "server" in text or "input failed" in text
    )


@contextmanager
def files_pg_connection_for_runtime(runtime: Any, *, autocommit: bool = True):
    conn = files_pg_connect_for_runtime(runtime, autocommit=autocommit)
    try:
        yield conn
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
