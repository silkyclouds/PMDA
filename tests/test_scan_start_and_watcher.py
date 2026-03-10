import tempfile
import unittest
from pathlib import Path

import pmda


class ScanStartAndWatcherTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-scan-watcher-")
        tmp_path = Path(self._tmp.name)
        self._orig = {
            "CONFIG_DIR": pmda.CONFIG_DIR,
            "STATE_DB_FILE": pmda.STATE_DB_FILE,
            "SETTINGS_DB_FILE": pmda.SETTINGS_DB_FILE,
            "CACHE_DB_FILE": pmda.CACHE_DB_FILE,
            "AUTH_DISABLE": pmda.AUTH_DISABLE,
        }
        pmda._stop_files_watcher_manager()
        pmda._stop_files_watcher()
        pmda.CONFIG_DIR = tmp_path
        pmda.STATE_DB_FILE = tmp_path / "state.db"
        pmda.SETTINGS_DB_FILE = tmp_path / "settings.db"
        pmda.CACHE_DB_FILE = tmp_path / "cache.db"
        pmda.AUTH_DISABLE = True
        pmda.init_state_db()
        pmda.init_settings_db()
        pmda.init_cache_db()
        self.client = pmda.app.test_client()

    def tearDown(self):
        pmda._stop_files_watcher_manager()
        pmda._stop_files_watcher()
        with pmda.lock:
            pmda.state["scanning"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_starting"] = False
            pmda.state["scan_start_requested_at"] = None
            pmda.state["scan_start_time"] = None
            pmda.state["scan_type"] = "full"
        for key, value in self._orig.items():
            setattr(pmda, key, value)
        self._tmp.cleanup()

    def test_scan_start_returns_409_when_scan_is_active(self):
        orig_requires = pmda._requires_config
        orig_reload_ai = pmda._reload_ai_config_and_reinit
        try:
            pmda._requires_config = lambda: None
            pmda._reload_ai_config_and_reinit = lambda: None
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_type"] = "full"
                pmda.state["scan_start_time"] = 1234.0
            resp = self.client.post("/scan/start", json={"scan_type": "full"})
            self.assertEqual(resp.status_code, 409)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("status"), "blocked")
            self.assertEqual(payload.get("reason"), "scan_already_running")
            self.assertEqual(payload.get("active_scan_type"), "full")
            self.assertTrue(payload.get("started_at") is not None)
        finally:
            pmda._requires_config = orig_requires
            pmda._reload_ai_config_and_reinit = orig_reload_ai

    def test_files_watcher_restart_endpoint_accepts_request(self):
        resp = self.client.post("/api/files/watcher/restart")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload.get("status"), "accepted")
        self.assertTrue(isinstance(payload.get("requested_at"), (int, float)))

    def test_files_watcher_status_exposes_runtime_fields(self):
        with pmda.lock:
            fw = dict(pmda.state.get("files_watcher") or {})
            fw.update(
                running=False,
                reason="watchdog_unavailable",
                restart_in_progress=False,
                consecutive_failures=2,
                last_restart_duration_ms=345,
                last_error="observer_start_failed",
            )
            pmda.state["files_watcher"] = fw
        resp = self.client.get("/api/files/watcher/status")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertIn("restart_in_progress", payload)
        self.assertIn("consecutive_failures", payload)
        self.assertIn("last_restart_duration_ms", payload)
        self.assertIn("last_error", payload)
        self.assertEqual(int(payload.get("consecutive_failures") or 0), 2)


if __name__ == "__main__":
    unittest.main()
