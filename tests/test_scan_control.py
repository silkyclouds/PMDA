from __future__ import annotations

import threading

from pmda_scan.control import create_scan_runtime, wait_if_paused


def test_scan_runtime_reset_pause_resume_stop():
    runtime = create_scan_runtime()

    runtime.pause()
    assert runtime.is_paused
    assert not runtime.is_stopping

    runtime.stop()
    assert runtime.is_stopping

    runtime.resume()
    assert not runtime.is_paused
    assert not runtime.is_stopping

    runtime.pause()
    runtime.stop()
    runtime.reset()
    assert not runtime.is_paused
    assert not runtime.is_stopping


def test_wait_if_paused_returns_false_when_stop_requested():
    runtime = create_scan_runtime()
    runtime.pause()

    timer = threading.Timer(0.05, runtime.stop)
    timer.start()
    try:
        assert wait_if_paused(
            pause_event=runtime.pause_event,
            stop_event=runtime.stop_event,
            sleep_seconds=0.01,
        ) is False
    finally:
        timer.cancel()
