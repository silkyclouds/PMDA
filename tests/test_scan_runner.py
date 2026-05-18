from __future__ import annotations

import threading

from pmda_scan.runner import normalize_scan_type, start_scan_thread


def test_normalize_scan_type_allows_known_values_only():
    assert normalize_scan_type("full") == "full"
    assert normalize_scan_type("changed_only") == "changed_only"
    assert normalize_scan_type("bad") == "full"
    assert normalize_scan_type(None) == "full"


def test_start_scan_thread_uses_normalized_name_and_runs_target():
    ran = threading.Event()

    def target():
        ran.set()

    thread = start_scan_thread(target, scan_type="changed_only")
    thread.join(timeout=2)

    assert ran.is_set()
    assert thread.daemon
    assert thread.name == "scan-changed_only"
