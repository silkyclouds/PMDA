import time

import pytest

from pmda_core.execution_runtime import _run_callable_bounded


def test_run_callable_bounded_returns_value():
    assert _run_callable_bounded(lambda value: value + 1, 41, timeout_sec=1, log_prefix="test") == 42


def test_run_callable_bounded_times_out():
    with pytest.raises(TimeoutError):
        _run_callable_bounded(time.sleep, 1.5, timeout_sec=0.01, log_prefix="test")
