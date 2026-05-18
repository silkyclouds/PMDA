from __future__ import annotations

import logging
from collections import deque
from threading import Lock

from pmda_core import logging_utils


def test_plain_log_record_line_uses_configured_thread_humanizer() -> None:
    logging_utils.set_thread_name_humanizer(lambda raw: f"thread:{raw}")
    record = logging.LogRecord(
        name="pmda",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello %s",
        args=("world",),
        exc_info=None,
    )
    record.created = 0

    line = logging_utils.plain_log_record_line(record)

    assert "INFO" in line
    assert "thread:MainThread" in line
    assert line.endswith("hello world")


def test_recent_log_buffer_handler_appends_plain_lines() -> None:
    logging_utils.set_thread_name_humanizer(lambda raw: raw)
    buffer: deque[str] = deque(maxlen=2)
    handler = logging_utils.RecentLogBufferHandler(buffer, Lock())
    record = logging.LogRecord("pmda", logging.WARNING, __file__, 1, "warn", (), None)

    handler.emit(record)

    assert len(buffer) == 1
    assert "WARNING" in buffer[0]
    assert buffer[0].endswith("warn")


def test_quiet_polling_filter_blocks_configured_paths() -> None:
    filt = logging_utils.QuietPollingFilter(("/api/progress",))
    quiet = logging.LogRecord("werkzeug", logging.INFO, __file__, 1, "GET /api/progress 200", (), None)
    normal = logging.LogRecord("werkzeug", logging.INFO, __file__, 1, "GET /api/library 200", (), None)

    assert filt.filter(quiet) is False
    assert filt.filter(normal) is True
