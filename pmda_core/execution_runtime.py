"""Bounded execution helpers for slow manual/runtime operations."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from typing import Any


def _run_callable_bounded(
    func,
    *args: Any,
    timeout_sec: float,
    log_prefix: str,
    **kwargs: Any,
) -> Any:
    """Run a callable in a short-lived worker and fail fast on timeout."""
    timeout_val = max(1.0, float(timeout_sec or 0.0))
    started = time.perf_counter()
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pmda-call-bounded")
    fut = pool.submit(func, *args, **kwargs)
    try:
        out = fut.result(timeout=timeout_val)
        elapsed = time.perf_counter() - started
        logging.info("%s call completed in %.2fs", log_prefix, elapsed)
        return out
    except FutureTimeout:
        elapsed = time.perf_counter() - started
        logging.warning(
            "%s call timed out after %.1fs (elapsed %.2fs)",
            log_prefix,
            timeout_val,
            elapsed,
        )
        raise TimeoutError(f"{log_prefix} timeout")
    finally:
        try:
            fut.cancel()
        except Exception:
            pass
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass


def _run_callable_bounded_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    return _run_callable_bounded(*args, **kwargs)
