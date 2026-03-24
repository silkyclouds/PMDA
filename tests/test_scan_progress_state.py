import tempfile
import time
import unittest
from pathlib import Path

import pmda


class ScanProgressStateTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-progress-state-")
        tmp_path = Path(self._tmp.name)
        self._orig = {
            "CONFIG_DIR": pmda.CONFIG_DIR,
            "STATE_DB_FILE": pmda.STATE_DB_FILE,
            "SETTINGS_DB_FILE": pmda.SETTINGS_DB_FILE,
            "CACHE_DB_FILE": pmda.CACHE_DB_FILE,
            "AUTH_DISABLE": pmda.AUTH_DISABLE,
            "CLASSICAL_NAME_PREFERENCE": pmda.CLASSICAL_NAME_PREFERENCE,
            "_API_PROGRESS_CACHE": getattr(pmda, "_API_PROGRESS_CACHE", None),
        }
        pmda.CONFIG_DIR = tmp_path
        pmda.STATE_DB_FILE = tmp_path / "state.db"
        pmda.SETTINGS_DB_FILE = tmp_path / "settings.db"
        pmda.CACHE_DB_FILE = tmp_path / "cache.db"
        pmda.AUTH_DISABLE = True
        pmda.init_state_db()
        pmda.init_settings_db()
        pmda.init_cache_db()
        setattr(pmda, "_API_PROGRESS_CACHE", None)
        self.client = pmda.app.test_client()

    def tearDown(self):
        with pmda.lock:
            pmda.state["scanning"] = False
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_post_processing"] = False
            pmda.state["scan_profile_enrich_running"] = False
            pmda.state["scan_artists_processed"] = 0
            pmda.state["scan_artists_total"] = 0
            pmda.state["scan_total_albums"] = 0
            pmda.state["scan_processed_albums_count"] = 0
            pmda.state["scan_published_albums_count"] = 0
            pmda.state["scan_start_time"] = None
            pmda.state["scan_steps_log"] = []
            pmda.state["scan_active_artists"] = {}
        for key, value in self._orig.items():
            setattr(pmda, key, value)
        self._tmp.cleanup()

    def test_api_progress_finalizing_does_not_reference_missing_total_artists(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (3, 2)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 8
                pmda.state["scan_artists_processed"] = 2
                pmda.state["scan_artists_total"] = 2
                pmda.state["scan_total_albums"] = 3
                pmda.state["scan_processed_albums_count"] = 3
                pmda.state["scan_published_albums_count"] = 3
                pmda.state["scan_pipeline_async"] = True
                pmda.state["scan_active_artists"] = {}
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertTrue(bool(payload.get("library_ready")))
            self.assertEqual(payload.get("phase"), "finalizing")
            self.assertGreaterEqual(int(payload.get("scan_runtime_sec") or 0), 1)
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_library_artist_display_name_prefers_original_classical_alias(self):
        orig_pref = pmda.CLASSICAL_NAME_PREFERENCE
        try:
            pmda.CLASSICAL_NAME_PREFERENCE = "original"
            display = pmda._library_artist_display_name(
                current_name="Peter Tchaikovsky",
                canonical_name="Peter Tchaikovsky",
                entity_kind="composer",
                roles_json='["composer"]',
                aliases_json='["Peter Tchaikovsky", "Pyotr Ilyich Tchaikovsky"]',
            )
            self.assertEqual(display, "Pyotr Ilyich Tchaikovsky")
            pmda.CLASSICAL_NAME_PREFERENCE = "english"
            display_en = pmda._library_artist_display_name(
                current_name="Peter Tchaikovsky",
                canonical_name="Peter Tchaikovsky",
                entity_kind="composer",
                roles_json='["composer"]',
                aliases_json='["Peter Tchaikovsky", "Pyotr Ilyich Tchaikovsky"]',
            )
            self.assertEqual(display_en, "Peter Tchaikovsky")
        finally:
            pmda.CLASSICAL_NAME_PREFERENCE = orig_pref


if __name__ == "__main__":
    unittest.main()
