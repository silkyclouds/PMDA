import json
import tempfile
import threading
import time
import unittest
import sqlite3
from pathlib import Path
from unittest import mock

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
            "_PIPELINE_BOOTSTRAP_STATUS_CACHE": getattr(pmda, "_PIPELINE_BOOTSTRAP_STATUS_CACHE", None),
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
        with pmda._RECENT_LOG_BUFFER_LOCK:
            pmda._RECENT_LOG_BUFFER.clear()
        self.client = pmda.app.test_client()

    def tearDown(self):
        with pmda.lock:
            pmda.state["scanning"] = False
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_resume_run_id"] = None
            pmda.state["scan_resume_requested_run_id"] = None
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
            pmda.state["scan_incomplete_move_running"] = False
            pmda.state["scan_incomplete_move_done"] = 0
            pmda.state["scan_incomplete_move_total"] = 0
            pmda.state["scan_incomplete_move_current_album"] = None
            pmda.state["scan_dupe_moved_count"] = 0
            pmda.state["scan_dupe_moved_mb"] = 0
            pmda.state["export_progress"] = None
            pmda.state["scan_published_catchup_running"] = False
            pmda.state["scan_published_catchup_reason"] = None
            pmda.state["scan_published_catchup_done"] = 0
            pmda.state["scan_published_catchup_total"] = 0
            pmda.state["scan_published_catchup_ok"] = 0
            pmda.state["scan_published_catchup_failed"] = 0
            pmda.state["scan_published_catchup_current_artist"] = None
            pmda.state["scan_discovery_running"] = False
            pmda.state["scan_discovery_stage"] = ""
            pmda.state["scan_discovery_roots_done"] = 0
            pmda.state["scan_discovery_roots_total"] = 0
            pmda.state["scan_discovery_files_found"] = 0
            pmda.state["scan_discovery_folders_found"] = 0
            pmda.state["scan_discovery_albums_found"] = 0
            pmda.state["scan_discovery_artists_found"] = 0
            pmda.state["scan_discovery_entries_scanned"] = 0
            pmda.state["scan_discovery_root_entries_scanned"] = 0
            pmda.state["scan_discovery_folders_done"] = 0
            pmda.state["scan_discovery_folders_total"] = 0
            pmda.state["scan_discovery_albums_done"] = 0
            pmda.state["scan_discovery_albums_total"] = 0
            pmda.state["scan_prescan_cache_snapshot_running"] = False
            pmda.state["scan_prescan_cache_snapshot_done"] = False
            pmda.state["scan_prescan_cache_snapshot_rows"] = 0
            pmda.state["scan_prescan_cache_snapshot_total"] = 0
            pmda.state["scan_prescan_cache_snapshot_updated_at"] = None
            pmda.state["scan_format_done_count"] = 0
            pmda.state["scan_mb_done_count"] = 0
            pmda.state["scan_provider_matches"] = {}
            pmda.state["scan_finalizing_stage"] = "idle"
            pmda.state["scan_finalizing_label"] = ""
            pmda.state["scan_finalizing_done"] = 0
            pmda.state["scan_finalizing_total"] = 0
            pmda.state["scan_finalizing_item_done"] = 0
            pmda.state["scan_finalizing_item_total"] = 0
            pmda.state["scan_finalizing_item_label"] = ""
            pmda.state["scan_finalizing_updated_at"] = None
        for key, value in self._orig.items():
            setattr(pmda, key, value)
        with pmda._RECENT_LOG_BUFFER_LOCK:
            pmda._RECENT_LOG_BUFFER.clear()
        self._tmp.cleanup()

    def test_api_progress_finalizing_does_not_reference_missing_total_artists(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
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
                pmda.state["scan_finalizing"] = True
                pmda.state["scan_finalizing_stage"] = "pipeline_trace"
                pmda.state["scan_finalizing_label"] = "Saving pipeline trace"
                pmda.state["scan_finalizing_done"] = 7
                pmda.state["scan_finalizing_total"] = 12
                pmda.state["scan_finalizing_item_done"] = 125
                pmda.state["scan_finalizing_item_total"] = 500
                pmda.state["scan_finalizing_item_label"] = "Saving pipeline trace (125/500 artists)"
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertFalse(bool(payload.get("library_ready")))
            self.assertEqual(payload.get("phase"), "finalizing")
            self.assertIn("Saving pipeline trace", str(payload.get("current_stage_human_label") or ""))
            self.assertIn("125/500 artists", str(payload.get("current_stage_human_label") or ""))
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 125)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 500)
            self.assertEqual(payload.get("stage_progress_unit"), "items")
            self.assertEqual(int(payload.get("scan_finalizing_item_done") or 0), 125)
            self.assertEqual(int(payload.get("scan_finalizing_item_total") or 0), 500)
            self.assertGreaterEqual(int(payload.get("scan_runtime_sec") or 0), 1)
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_track_reconciliation_returns_counters_for_active_scan(self):
        orig_mode = pmda._get_library_mode
        try:
            pmda._get_library_mode = lambda: "plex"
            result = pmda._compute_scan_track_reconciliation(123, 42)
            self.assertIsInstance(result, dict)
            self.assertEqual(result.get("detected_total"), 42)
            self.assertEqual(result.get("library_kept"), 0)
            self.assertEqual(result.get("moved_dupes"), 0)
            self.assertEqual(result.get("moved_incomplete"), 0)
            self.assertEqual(result.get("unaccounted"), 42)
        finally:
            pmda._get_library_mode = orig_mode

    def test_api_progress_uses_cached_bootstrap_status_when_hot_path_probe_fails(self):
        orig_counts = pmda._files_library_browse_counts
        try:
            setattr(
                pmda,
                "_PIPELINE_BOOTSTRAP_STATUS_CACHE",
                {
                    "bootstrap_required": False,
                    "autonomous_mode": True,
                    "first_full_scan_id": 9,
                    "first_full_completed_at": 123.0,
                    "updated_at": 456.0,
                },
            )
            pmda._files_library_browse_counts = lambda *args, **kwargs: (12, 8)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 9
                pmda.state["scan_artists_processed"] = 1
                pmda.state["scan_artists_total"] = 10
                pmda.state["scan_total_albums"] = 25
                pmda.state["scan_mb_done_count"] = 4
                pmda.state["scan_active_artists"] = {
                    "Orbital": {
                        "total_albums": 1,
                        "albums_processed": 0,
                        "current_album": {
                            "album_id": 1,
                            "album_title": "Snivilisation",
                            "status": "searching_mb",
                        },
                    }
                }
                pmda.state["scan_steps_log"] = []
            with mock.patch.object(pmda.sqlite3, "connect", side_effect=sqlite3.OperationalError("database is locked")):
                resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertTrue(bool(payload.get("scanning")))
            self.assertFalse(bool(payload.get("bootstrap_required")))
            self.assertTrue(bool(payload.get("has_completed_full_scan")))
            self.assertEqual(payload.get("phase"), "identification_tags")
        finally:
            pmda._files_library_browse_counts = orig_counts

    def test_runtime_auto_tune_snapshot_does_not_reenter_discogs_effective_rpm(self):
        orig_discogs_effective_rpm = pmda._discogs_effective_rpm
        try:
            def _boom():
                raise AssertionError("_runtime_auto_tune_snapshot should not call _discogs_effective_rpm while holding the lock")

            pmda._discogs_effective_rpm = _boom
            with pmda._RUNTIME_AUTO_TUNE_LOCK:
                pmda._RUNTIME_AUTO_TUNE_STATE["discogs_effective_rpm"] = 42.0
                pmda._RUNTIME_AUTO_TUNE_STATE["last_reason"] = "test"
            snapshot = pmda._runtime_auto_tune_snapshot()
            self.assertEqual(float(snapshot.get("discogs_effective_rpm") or 0.0), 42.0)
            self.assertEqual(snapshot.get("last_reason"), "test")
        finally:
            pmda._discogs_effective_rpm = orig_discogs_effective_rpm

    def test_api_progress_scan_hot_path_skips_db_dependent_helpers(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        orig_state_connect_readonly = pmda._state_connect_readonly
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}

            def _counts_boom(*args, **kwargs):
                raise AssertionError("scan hot path should not call _files_library_browse_counts")

            def _state_boom(*args, **kwargs):
                raise AssertionError("scan hot path should not open state.db")

            pmda._files_library_browse_counts = _counts_boom
            pmda._state_connect_readonly = _state_boom
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 12
                pmda.state["scan_artists_processed"] = 2
                pmda.state["scan_artists_total"] = 12
                pmda.state["scan_total_albums"] = 30
                pmda.state["scan_mb_done_count"] = 5
                pmda.state["scan_provider_matches"] = {"discogs": 2, "lastfm": 1}
                pmda.state["scan_active_artists"] = {
                    "Orbital": {
                        "total_albums": 1,
                        "albums_processed": 0,
                        "current_album": {
                            "album_id": 1,
                            "album_title": "Snivilisation",
                            "status": "searching_mb",
                        },
                    }
                }
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertTrue(bool(payload.get("scanning")))
            self.assertEqual(payload.get("phase"), "identification_tags")
            self.assertEqual(int(payload.get("matches_so_far") or 0), 3)
            self.assertFalse(bool(payload.get("stale")))
            self.assertFalse(bool(payload.get("lock_contention")))
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts
            pmda._state_connect_readonly = orig_state_connect_readonly

    def test_api_progress_scan_hot_path_reuses_cached_side_snapshots_when_side_locks_are_busy(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        orig_try_lock = pmda._lock_try_acquire_nonblocking
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            setattr(
                pmda,
                "_API_PROGRESS_CACHE",
                {
                    "ts": time.time(),
                    "state_key": None,
                    "payload": {
                        "background_jobs": [
                            {
                                "run_id": "job-1",
                                "job_type": "enrich_batch",
                                "scope": "library",
                                "source": "scan",
                                "origin_scan_id": 7,
                                "started_at": 123.0,
                            }
                        ],
                        "profile_backfill": {
                            "running": True,
                            "current": 12,
                            "total": 80,
                            "current_artist": "Orbital",
                        },
                        "scan_provider_stats_live": {
                            "discogs": {
                                "lookup_request_count": 44,
                                "lookup_hit_rate": 25.0,
                                "avg_latency_ms": 210.0,
                            }
                        },
                        "provider_gateway_inflight": 3,
                        "provider_gateway_max_inflight_observed": 5,
                    },
                },
            )

            def _busy(_lock_obj):
                return False

            pmda._lock_try_acquire_nonblocking = _busy
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 12
                pmda.state["scan_artists_processed"] = 2
                pmda.state["scan_artists_total"] = 12
                pmda.state["scan_total_albums"] = 30
                pmda.state["scan_mb_done_count"] = 5
                pmda.state["scan_provider_matches"] = {"discogs": 2}
                pmda.state["scan_active_artists"] = {
                    "Orbital": {
                        "total_albums": 1,
                        "albums_processed": 0,
                        "current_album": {
                            "album_id": 1,
                            "album_title": "Snivilisation",
                            "status": "searching_mb",
                        },
                    }
                }
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertTrue(bool(payload.get("scanning")))
            self.assertEqual(payload.get("phase"), "identification_tags")
            self.assertEqual(len(payload.get("background_jobs") or []), 1)
            self.assertEqual((payload.get("profile_backfill") or {}).get("current_artist"), "Orbital")
            stats = payload.get("scan_provider_stats_live") or {}
            self.assertEqual(int((stats.get("discogs") or {}).get("lookup_request_count") or 0), 44)
            self.assertEqual(int(payload.get("provider_gateway_inflight") or 0), 3)
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts
            pmda._lock_try_acquire_nonblocking = orig_try_lock

    def test_api_progress_background_enrichment_uses_visible_browse_counts(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        with pmda._files_profile_jobs_lock:
            orig_profile_jobs_active = set(pmda._files_profile_jobs_active)
            pmda._files_profile_jobs_active.clear()
            pmda._files_profile_jobs_active.add("artist:pyotr")
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (4, 3)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 22
                pmda.state["scan_artists_processed"] = 6
                pmda.state["scan_artists_total"] = 6
                pmda.state["scan_total_albums"] = 12
                pmda.state["scan_processed_albums_count"] = 12
                pmda.state["scan_published_albums_count"] = 9
                pmda.state["scan_pipeline_async"] = True
                pmda.state["scan_active_artists"] = {}
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertFalse(bool(payload.get("library_ready")))
            self.assertTrue(bool(payload.get("background_enrichment_running")))
            self.assertEqual(payload.get("phase"), "background_enrichment")
            self.assertEqual(int(payload.get("scan_published_albums_count") or 0), 9)
            self.assertEqual(int(payload.get("library_visible_albums_count") or 0), 9)
            self.assertGreaterEqual(int(payload.get("scan_runtime_sec") or 0), 1)
        finally:
            with pmda._files_profile_jobs_lock:
                pmda._files_profile_jobs_active.clear()
                pmda._files_profile_jobs_active.update(orig_profile_jobs_active)
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_exposes_live_dupe_move_telemetry(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 14
                pmda.state["scan_artists_processed"] = 3
                pmda.state["scan_artists_total"] = 10
                pmda.state["scan_total_albums"] = 40
                pmda.state["scan_mb_done_count"] = 9
                pmda.state["scan_dupe_moved_count"] = 7
                pmda.state["scan_dupe_moved_mb"] = 1536
                pmda.state["scan_incomplete_moved_count"] = 4
                pmda.state["scan_incomplete_moved_mb"] = 320
                pmda.state["scan_active_artists"] = {
                    "Orbital": {
                        "total_albums": 1,
                        "albums_processed": 0,
                        "current_album": {
                            "album_id": 1,
                            "album_title": "Snivilisation",
                            "status": "searching_mb",
                        },
                    }
                }
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(int(payload.get("scan_dupe_moved_count") or 0), 7)
            self.assertEqual(int(payload.get("scan_dupe_moved_mb") or 0), 1536)
            self.assertEqual(int(payload.get("scan_incomplete_moved_count") or 0), 4)
            self.assertEqual(int(payload.get("scan_incomplete_moved_mb") or 0), 320)
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_clears_current_step_when_scan_stopped(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = False
                pmda.state["scan_starting"] = False
                pmda.state["scan_active_artists"] = {
                    "Sigur Rós": {
                        "total_albums": 1,
                        "albums_processed": 0,
                        "current_album": {
                            "album_id": 1,
                            "album_title": "Takk...",
                            "status": "searching_mb",
                        },
                    }
                }
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertFalse(bool(payload.get("scanning")))
            self.assertIsNone(payload.get("current_step"))
            self.assertEqual(payload.get("active_artists"), [])
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_reports_stage_progress_for_identification_phase(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 10
                pmda.state["scan_artists_processed"] = 1
                pmda.state["scan_artists_total"] = 4
                pmda.state["scan_total_albums"] = 10
                pmda.state["scan_mb_done_count"] = 3
                pmda.state["scan_active_artists"] = {
                    "Orbital": {
                        "total_albums": 1,
                        "albums_processed": 0,
                        "current_album": {
                            "album_id": 1,
                            "album_title": "Snivilisation",
                            "status": "searching_mb",
                        },
                    }
                }
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "identification_tags")
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 3)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 10)
            self.assertEqual(payload.get("stage_progress_unit"), "albums")
            self.assertAlmostEqual(float(payload.get("stage_progress_percent") or 0.0), 30.0, places=2)
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_uses_album_candidates_label_during_resume_discovery(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_type"] = "full"
                pmda.state["scan_start_time"] = time.time() - 120
                pmda.state["scan_resume_run_id"] = "resume-123"
                pmda.state["scan_discovery_running"] = True
                pmda.state["scan_discovery_stage"] = "album_candidates"
                pmda.state["scan_discovery_albums_done"] = 3860
                pmda.state["scan_discovery_albums_total"] = 60535
                pmda.state["scan_detected_artists_total"] = 3142
                pmda.state["scan_detected_albums_total"] = 3859
                pmda.state["scan_steps_log"] = []
                pmda.state["scan_active_artists"] = {}
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "pre_scan")
            self.assertEqual(payload.get("current_step"), "building_album_candidates")
            self.assertEqual(payload.get("current_stage_human_label"), "Building album candidates")
            self.assertEqual(payload.get("pipeline_step_human_label"), "Preparing the scan")
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 3860)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 60535)
            self.assertEqual(payload.get("stage_progress_unit"), "albums")
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_returns_stale_lock_fallback_when_state_lock_is_contended(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        release = threading.Event()
        acquired = threading.Event()

        def _holder():
            pmda.lock.acquire()
            acquired.set()
            try:
                release.wait(2.0)
            finally:
                pmda.lock.release()

        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            setattr(pmda, "_API_PROGRESS_CACHE", None)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_type"] = "full"
                pmda.state["scan_start_time"] = time.time() - 30
                pmda.state["scan_artists_processed"] = 12
                pmda.state["scan_artists_total"] = 200
                pmda.state["scan_total_albums"] = 400
                pmda.state["scan_processed_albums_count"] = 55
                pmda.state["scan_mb_done_count"] = 60
                pmda.state["scan_active_artists"] = {
                    "Orbital": {
                        "total_albums": 4,
                        "albums_processed": 2,
                        "current_album": {
                            "album_id": 7,
                            "album_title": "In Sides",
                            "status": "searching_mb",
                        },
                    }
                }
            holder = threading.Thread(target=_holder, daemon=True)
            holder.start()
            self.assertTrue(acquired.wait(1.0))
            started = time.time()
            resp = self.client.get("/api/progress")
            elapsed = time.time() - started
            self.assertLess(elapsed, 0.5)
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertTrue(bool(payload.get("stale")))
            self.assertTrue(bool(payload.get("lock_contention")))
            self.assertTrue(bool(payload.get("scanning")))
            self.assertEqual(payload.get("phase"), "identification_tags")
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 60)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 400)
        finally:
            release.set()
            pmda._pipeline_bootstrap_status = orig_bootstrap

    def test_api_progress_reuses_cached_payload_when_rlock_is_contended_without_locked_method(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        release = threading.Event()
        acquired = threading.Event()

        def _holder():
            pmda.lock.acquire()
            acquired.set()
            try:
                release.wait(2.0)
            finally:
                pmda.lock.release()

        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_type"] = "full"
                pmda.state["scan_start_time"] = time.time() - 60
                pmda.state["scan_artists_processed"] = 18
                pmda.state["scan_artists_total"] = 200
                pmda.state["scan_total_albums"] = 400
                pmda.state["scan_processed_albums_count"] = 81
                pmda.state["scan_mb_done_count"] = 91
                pmda.state["scan_active_artists"] = {
                    "Orbital": {
                        "total_albums": 4,
                        "albums_processed": 3,
                        "current_album": {
                            "album_id": 7,
                            "album_title": "In Sides",
                            "status": "searching_mb",
                        },
                    }
                }
                pmda.state["scan_steps_log"] = []
            first = self.client.get("/api/progress")
            self.assertEqual(first.status_code, 200)
            first_payload = first.get_json() or {}
            self.assertFalse(bool(first_payload.get("stale")))
            self.assertEqual(int(first_payload.get("stage_progress_done") or 0), 91)

            holder = threading.Thread(target=_holder, daemon=True)
            holder.start()
            self.assertTrue(acquired.wait(1.0))
            time.sleep(0.85)

            started = time.time()
            second = self.client.get("/api/progress")
            elapsed = time.time() - started
            self.assertLess(elapsed, 0.5)
            self.assertEqual(second.status_code, 200)
            second_payload = second.get_json() or {}
            self.assertTrue(bool(second_payload.get("stale")))
            self.assertEqual(int(second_payload.get("stage_progress_done") or 0), 91)
            self.assertEqual(second_payload.get("phase"), "identification_tags")
        finally:
            release.set()
            pmda._pipeline_bootstrap_status = orig_bootstrap

    def test_api_progress_uses_inflight_album_work_for_stage_progress_and_eta(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 600
                pmda.state["scan_artists_processed"] = 0
                pmda.state["scan_artists_total"] = 100
                pmda.state["scan_total_albums"] = 1000
                pmda.state["scan_mb_done_count"] = 1
                pmda.state["scan_processed_albums_count"] = 0
                pmda.state["scan_active_artists"] = {
                    "Artist A": {
                        "total_albums": 10,
                        "albums_processed": 9,
                        "current_album": {
                            "album_id": 1,
                            "album_title": "Album A",
                            "status": "fetching_mb_id",
                        },
                    },
                    "Artist B": {
                        "total_albums": 4,
                        "albums_processed": 3,
                        "current_album": {
                            "album_id": 2,
                            "album_title": "Album B",
                            "status": "fetching_mb_id",
                        },
                    },
                }
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "identification_tags")
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 12)
            self.assertEqual(int(payload.get("scan_processed_albums_effective") or 0), 12)
            self.assertEqual(int(payload.get("artists_processed_effective") or 0), 2)
            self.assertIsInstance(payload.get("eta_seconds"), int)
            self.assertLess(int(payload.get("eta_seconds") or 0), 60 * 60 * 24 * 60)
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_exposes_human_labels_and_convenience_counts(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (6, 4)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 1800
                pmda.state["scan_artists_processed"] = 2
                pmda.state["scan_artists_total"] = 5
                pmda.state["scan_total_albums"] = 10
                pmda.state["scan_mb_done_count"] = 4
                pmda.state["scan_discogs_matched"] = 3
                pmda.state["scan_lastfm_matched"] = 2
                pmda.state["scan_bandcamp_matched"] = 1
                pmda.state["scan_published_albums_count"] = 4
                pmda.state["scan_active_artists"] = {
                    "Orbital": {
                        "total_albums": 1,
                        "albums_processed": 0,
                        "current_album": {
                            "album_id": 1,
                            "album_title": "Snivilisation",
                            "status": "searching_mb",
                        },
                    }
                }
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("scan_progress_mode"), "stage_active")
            self.assertEqual(payload.get("pipeline_step_human_label"), "Matching albums and verifying tags")
            self.assertEqual(payload.get("current_stage_human_label"), "Querying MusicBrainz and providers")
            self.assertEqual(int(payload.get("matches_so_far") or 0), 6)
            self.assertEqual(int(payload.get("exports_so_far") or 0), 4)
            self.assertEqual(int(payload.get("active_artists_count") or 0), 1)
            provider_matches = payload.get("provider_matches_so_far") or {}
            self.assertEqual(int(provider_matches.get("discogs") or 0), 3)
            self.assertEqual(int(provider_matches.get("lastfm") or 0), 2)
            self.assertEqual(int(provider_matches.get("bandcamp") or 0), 1)
            self.assertEqual(payload.get("scan_eta_confidence"), "medium")
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_treats_fetching_mb_id_as_identification_phase(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 10
                pmda.state["scan_artists_processed"] = 1
                pmda.state["scan_artists_total"] = 4
                pmda.state["scan_total_albums"] = 10
                pmda.state["scan_mb_done_count"] = 4
                pmda.state["scan_active_artists"] = {
                    "Orbital": {
                        "total_albums": 1,
                        "albums_processed": 0,
                        "current_album": {
                            "album_id": 1,
                            "album_title": "Snivilisation",
                            "status": "fetching_mb_id",
                        },
                    }
                }
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "identification_tags")
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 4)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 10)
            self.assertEqual(payload.get("stage_progress_unit"), "albums")
            self.assertAlmostEqual(float(payload.get("stage_progress_percent") or 0.0), 40.0, places=2)
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_uses_prescan_phase_while_scan_is_starting(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = True
                pmda.state["scan_artists_total"] = 0
                pmda.state["scan_total_albums"] = 0
                pmda.state["scan_discovery_running"] = False
                pmda.state["scan_active_artists"] = {}
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "pre_scan")
            self.assertEqual(payload.get("current_step"), "starting_scan")
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_reports_stage_progress_for_prescan_filesystem(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 5
                pmda.state["scan_artists_total"] = 0
                pmda.state["scan_total_albums"] = 1
                pmda.state["scan_processed_albums_count"] = 0
                pmda.state["scan_discovery_running"] = True
                pmda.state["scan_discovery_stage"] = "filesystem"
                pmda.state["scan_discovery_roots_done"] = 2
                pmda.state["scan_discovery_roots_total"] = 5
                pmda.state["scan_discovery_files_found"] = 123
                pmda.state["scan_active_artists"] = {}
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "pre_scan")
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 2)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 5)
            self.assertEqual(payload.get("stage_progress_unit"), "roots")
            self.assertAlmostEqual(float(payload.get("stage_progress_percent") or 0.0), 40.0, places=2)
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_reports_album_candidate_stage_after_resume(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 30
                pmda.state["scan_resume_run_id"] = "resume123"
                pmda.state["scan_total_albums"] = 60535
                pmda.state["scan_discovery_running"] = True
                pmda.state["scan_discovery_stage"] = "album_candidates"
                pmda.state["scan_discovery_albums_done"] = 146
                pmda.state["scan_discovery_albums_total"] = 60535
                pmda.state["scan_discovery_folders_done"] = 146
                pmda.state["scan_discovery_folders_total"] = 60535
                pmda.state["scan_active_artists"] = {}
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "pre_scan")
            self.assertEqual(payload.get("current_step"), "building_album_candidates")
            self.assertEqual(payload.get("current_stage_human_label"), "Building album candidates")
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 146)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 60535)
            self.assertEqual(payload.get("stage_progress_unit"), "albums")
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_reports_stage_progress_for_prescan_snapshot(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 5
                pmda.state["scan_artists_total"] = 0
                pmda.state["scan_total_albums"] = 60473
                pmda.state["scan_processed_albums_count"] = 0
                pmda.state["scan_discovery_running"] = False
                pmda.state["scan_prescan_cache_snapshot_running"] = True
                pmda.state["scan_prescan_cache_snapshot_rows"] = 211
                pmda.state["scan_prescan_cache_snapshot_total"] = 60473
                pmda.state["scan_active_artists"] = {}
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "pre_scan")
            self.assertEqual(payload.get("current_step"), "snapshotting_prescan_cache")
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 211)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 60473)
            self.assertEqual(payload.get("stage_progress_unit"), "albums")
            self.assertEqual(payload.get("scan_progress_mode"), "preparing")
            self.assertEqual(payload.get("scan_eta_confidence"), "low")
            self.assertEqual(payload.get("pipeline_step_human_label"), "Preparing the scan")
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_keeps_prescan_snapshot_phase_after_resume_plan_restored(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 5
                pmda.state["scan_artists_total"] = 37366
                pmda.state["scan_total_albums"] = 60159
                pmda.state["scan_processed_albums_count"] = 0
                pmda.state["scan_discovery_running"] = False
                pmda.state["scan_prescan_cache_snapshot_running"] = True
                pmda.state["scan_prescan_cache_snapshot_rows"] = 0
                pmda.state["scan_prescan_cache_snapshot_total"] = 60159
                pmda.state["scan_mb_done_count"] = 0
                pmda.state["scan_format_done_count"] = 0
                pmda.state["scan_active_artists"] = {}
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "pre_scan")
            self.assertEqual(payload.get("current_step"), "snapshotting_prescan_cache")
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 0)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 60159)
            self.assertEqual(payload.get("stage_progress_unit"), "albums")
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_reports_stage_progress_for_published_catchup(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 5
                pmda.state["scan_artists_total"] = 0
                pmda.state["scan_total_albums"] = 60473
                pmda.state["scan_discovery_running"] = False
                pmda.state["scan_prescan_cache_snapshot_running"] = False
                pmda.state["scan_prescan_cache_snapshot_rows"] = 0
                pmda.state["scan_prescan_cache_snapshot_total"] = 0
                pmda.state["scan_published_catchup_running"] = True
                pmda.state["scan_published_catchup_reason"] = "scan_resume"
                pmda.state["scan_published_catchup_done"] = 50
                pmda.state["scan_published_catchup_total"] = 294
                pmda.state["scan_published_catchup_ok"] = 49
                pmda.state["scan_published_catchup_failed"] = 1
                pmda.state["scan_published_catchup_current_artist"] = "Ash Koosha"
                pmda.state["scan_active_artists"] = {}
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "pre_scan")
            self.assertEqual(payload.get("current_step"), "rehydrating_library_index")
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 50)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 294)
            self.assertEqual(payload.get("stage_progress_unit"), "artists")
            self.assertEqual(int(payload.get("scan_published_catchup_ok") or 0), 49)
            self.assertEqual(payload.get("scan_published_catchup_current_artist"), "Ash Koosha")
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_reports_incomplete_move_as_dedicated_phase(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 9
                pmda.state["scan_artists_processed"] = 5
                pmda.state["scan_artists_total"] = 5
                pmda.state["scan_total_albums"] = 12
                pmda.state["scan_processed_albums_count"] = 12
                pmda.state["scan_incomplete_move_running"] = True
                pmda.state["scan_incomplete_move_done"] = 3
                pmda.state["scan_incomplete_move_total"] = 7
                pmda.state["scan_incomplete_move_current_album"] = {"artist": "A", "album": "B"}
                pmda.state["scan_active_artists"] = {}
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "incomplete_move")
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 3)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 7)
            self.assertEqual(payload.get("stage_progress_unit"), "albums")
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts


class MusicBrainzQueueGuardTests(ScanProgressStateTests):
    def test_worker_timeout_does_not_freeze_following_requests(self):
        queue = pmda.MusicBrainzQueue(enabled=True)
        never = threading.Event()
        orig_timeout_helper = queue._request_timeout_seconds
        orig_sleep_interval = queue.sleep_interval_sec
        try:
            queue._request_timeout_seconds = lambda request_id, worker_guard=False: 0.1 if worker_guard else 0.3
            queue.sleep_interval_sec = 0.0

            with self.assertRaises(TimeoutError):
                queue.submit("search_stuck_album", lambda: never.wait(60.0))

            out = queue.submit("search_fast_album", lambda: {"ok": True})
            self.assertEqual(out, {"ok": True})

            stats = queue.stats_snapshot()
            self.assertGreaterEqual(int(stats.get("completed_count") or 0), 2)
            self.assertGreaterEqual(int(stats.get("timeout_count") or 0), 1)
        finally:
            queue._request_timeout_seconds = orig_timeout_helper
            queue.sleep_interval_sec = orig_sleep_interval
            queue.shutdown()

    def test_api_progress_exposes_dynamic_provider_match_counts(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 60
                pmda.state["scan_artists_processed"] = 1
                pmda.state["scan_artists_total"] = 3
                pmda.state["scan_total_albums"] = 8
                pmda.state["scan_mb_done_count"] = 4
                pmda.state["scan_provider_matches"] = {
                    "musicbrainz": 2,
                    "spotify": 3,
                    "fanart": 1,
                    "theaudiodb": 4,
                }
                pmda.state["scan_discogs_matched"] = 0
                pmda.state["scan_lastfm_matched"] = 0
                pmda.state["scan_bandcamp_matched"] = 0
                pmda.state["scan_active_artists"] = {
                    "Orbital": {
                        "total_albums": 1,
                        "albums_processed": 0,
                        "current_album": {
                            "album_id": 1,
                            "album_title": "Snivilisation",
                            "status": "searching_mb",
                        },
                    }
                }
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            provider_matches = payload.get("provider_matches_so_far") or {}
            self.assertEqual(int(provider_matches.get("musicbrainz") or 0), 2)
            self.assertEqual(int(provider_matches.get("spotify") or 0), 3)
            self.assertEqual(int(provider_matches.get("fanart") or 0), 1)
            self.assertEqual(int(provider_matches.get("audiodb") or 0), 4)
            self.assertEqual(int(payload.get("matches_so_far") or 0), 10)
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_exposes_live_provider_stats_during_scan(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda._PROVIDER_GATEWAY_LOCK:
                pmda._PROVIDER_GATEWAY_STATS["providers"] = {}
                pmda._PROVIDER_GATEWAY_STATS["max_inflight_observed"] = 0
            pmda._provider_gateway_record_lookup_request("discogs")
            pmda._provider_gateway_record_lookup_network_request("discogs")
            pmda._provider_gateway_record_result("discogs", status_code=200, latency_ms=120, context="Artist / Album")
            pmda._provider_gateway_record_lookup_request("lastfm")
            pmda._provider_gateway_record_lookup_cache_hit("lastfm", "not_found")
            pmda._provider_gateway_record_result(
                "bandcamp",
                latency_ms=45000,
                error=pmda.requests.exceptions.Timeout("timed out"),
                context="Slow / Album",
            )
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 30
                pmda.state["scan_artists_processed"] = 1
                pmda.state["scan_artists_total"] = 5
                pmda.state["scan_total_albums"] = 12
                pmda.state["scan_mb_done_count"] = 3
                pmda.state["scan_active_artists"] = {
                    "Orbital": {
                        "total_albums": 1,
                        "albums_processed": 0,
                        "current_album": {
                            "album_id": 1,
                            "album_title": "Snivilisation",
                            "status": "searching_mb",
                        },
                    }
                }
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            stats = payload.get("scan_provider_stats_live") or {}
            self.assertIn("discogs", stats)
            self.assertEqual(int(stats["discogs"].get("lookup_request_count") or 0), 1)
            self.assertEqual(int(stats["discogs"].get("lookup_network_request_count") or 0), 1)
            self.assertEqual(float(stats["discogs"].get("avg_latency_ms") or 0.0), 120.0)
            self.assertIn("lastfm", stats)
            self.assertEqual(int(stats["lastfm"].get("lookup_negative_hits") or 0), 1)
            self.assertIn("bandcamp", stats)
            self.assertEqual(int(stats["bandcamp"].get("timeout_count") or 0), 1)
        finally:
            with pmda._PROVIDER_GATEWAY_LOCK:
                pmda._PROVIDER_GATEWAY_STATS["providers"] = {}
                pmda._PROVIDER_GATEWAY_STATS["max_inflight_observed"] = 0
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_reports_export_as_dedicated_phase(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 9
                pmda.state["scan_artists_processed"] = 5
                pmda.state["scan_artists_total"] = 5
                pmda.state["scan_total_albums"] = 12
                pmda.state["scan_processed_albums_count"] = 12
                pmda.state["export_progress"] = {
                    "running": True,
                    "albums_done": 4,
                    "total_albums": 9,
                    "tracks_done": 25,
                    "total_tracks": 81,
                    "error": None,
                }
                pmda.state["scan_active_artists"] = {}
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "export")
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 4)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 9)
            self.assertEqual(payload.get("stage_progress_unit"), "albums")
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts

    def test_api_progress_does_not_reacquire_provider_matches_snapshot_after_state_release(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        orig_snapshot = pmda._scan_provider_matches_snapshot
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)

            def _boom():
                raise AssertionError("api_progress should use the in-lock provider match snapshot")

            pmda._scan_provider_matches_snapshot = _boom
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 20
                pmda.state["scan_artists_processed"] = 2
                pmda.state["scan_artists_total"] = 5
                pmda.state["scan_total_albums"] = 12
                pmda.state["scan_mb_done_count"] = 3
                pmda.state["scan_discogs_matched"] = 2
                pmda.state["scan_lastfm_matched"] = 1
                pmda.state["scan_bandcamp_matched"] = 0
                pmda.state["scan_provider_matches"] = {
                    "discogs": 2,
                    "lastfm": 1,
                    "musicbrainz": 3,
                }
                pmda.state["scan_active_artists"] = {}
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            provider_matches = payload.get("provider_matches_so_far") or {}
            self.assertEqual(int(provider_matches.get("discogs") or 0), 2)
            self.assertEqual(int(provider_matches.get("lastfm") or 0), 1)
            self.assertEqual(int(provider_matches.get("musicbrainz") or 0), 3)
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts
            pmda._scan_provider_matches_snapshot = orig_snapshot

    def test_api_logs_tail_scan_mode_prefers_recent_relevant_scan_buffer(self):
        with pmda._RECENT_LOG_BUFFER_LOCK:
            pmda._RECENT_LOG_BUFFER.clear()
            pmda._RECENT_LOG_BUFFER.extend(
                [
                    "10:11:21 │ INFO │ http │ Escalating assistant_chat/longform from qwen3:4b to qwen3:14b (longform)",
                    "10:11:22 │ INFO │ scan:resume │ [scan] Heartbeat (waiting_on_workers): artists 4/50, albums 12/800, mb 14/800, active=3 [Orbital 1/4], throughput 312.5/h, matches discogs=2, spotify=1",
                    "10:11:23 │ INFO │ scan:worker │ [match] Orbital — Snivilisation: strict matched via spotify",
                    "10:11:24 │ INFO │ http │ Settings saved to settings.db: ['PIPELINE_POST_SCAN_ASYNC']",
                    "10:11:25 │ INFO │ postgres │ checkpoint complete: wrote 93 buffers",
                ]
            )
        resp = self.client.get("/api/logs/tail?lines=10&scan_mode=1")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertIn("live://process-buffer", str(payload.get("path") or ""))
        entries = payload.get("entries") or []
        self.assertGreaterEqual(len(entries), 2)
        raw_joined = "\n".join(str(entry.get("raw") or "") for entry in entries)
        self.assertIn("Heartbeat (waiting_on_workers)", raw_joined)
        self.assertIn("strict matched via spotify", raw_joined)
        self.assertNotIn("assistant_chat/longform", raw_joined)
        self.assertNotIn("Settings saved to settings.db", raw_joined)
        self.assertNotIn("checkpoint complete", raw_joined)

    def test_api_progress_prefers_active_matching_phase_over_parallel_export(self):
        orig_bootstrap = pmda._pipeline_bootstrap_status
        orig_counts = pmda._files_library_browse_counts
        try:
            pmda._pipeline_bootstrap_status = lambda **kwargs: {"bootstrap_required": False}
            pmda._files_library_browse_counts = lambda *args, **kwargs: (0, 0)
            with pmda.lock:
                pmda.state["scanning"] = True
                pmda.state["scan_starting"] = False
                pmda.state["scan_start_time"] = time.time() - 30
                pmda.state["scan_artists_processed"] = 2
                pmda.state["scan_artists_total"] = 8
                pmda.state["scan_total_albums"] = 24
                pmda.state["scan_mb_done_count"] = 7
                pmda.state["export_progress"] = {
                    "running": True,
                    "albums_done": 4,
                    "total_albums": 9,
                    "tracks_done": 25,
                    "total_tracks": 81,
                    "error": None,
                }
                pmda.state["scan_active_artists"] = {
                    "Orbital": {
                        "total_albums": 1,
                        "albums_processed": 0,
                        "current_album": {
                            "album_id": 1,
                            "album_title": "Snivilisation",
                            "status": "searching_mb",
                        },
                    }
                }
                pmda.state["scan_steps_log"] = []
            resp = self.client.get("/api/progress")
            self.assertEqual(resp.status_code, 200)
            payload = resp.get_json() or {}
            self.assertEqual(payload.get("phase"), "identification_tags")
            self.assertTrue(bool(payload.get("export_running")))
            self.assertEqual(int(payload.get("stage_progress_done") or 0), 7)
            self.assertEqual(int(payload.get("stage_progress_total") or 0), 24)
            self.assertEqual(payload.get("stage_progress_unit"), "albums")
            self.assertEqual(payload.get("pipeline_step_human_label"), "Matching albums and verifying tags")
        finally:
            pmda._pipeline_bootstrap_status = orig_bootstrap
            pmda._files_library_browse_counts = orig_counts



    def test_collapse_classical_person_aliases_prefers_best_display_for_equivalent_aliases(self):
        merged = pmda._collapse_classical_person_aliases(
            ["piotr ilyitch tchaikovsky", "Pyotr Ilyich Tchaikovsky"],
        )
        self.assertEqual(merged, ["Pyotr Ilyich Tchaikovsky"])

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

    def test_choose_preferred_person_identity_name_upgrades_lowercase_transliteration(self):
        orig_pref = pmda.CLASSICAL_NAME_PREFERENCE
        try:
            pmda.CLASSICAL_NAME_PREFERENCE = "original"
            chosen = pmda._choose_preferred_person_identity_name(
                "piotr ilyitch tchaikovsky",
                "Pyotr Ilyich Tchaikovsky",
            )
            self.assertEqual(chosen, "Pyotr Ilyich Tchaikovsky")
        finally:
            pmda.CLASSICAL_NAME_PREFERENCE = orig_pref

    def test_get_startup_resume_snapshot_prefers_running_resume_run(self):
        con = sqlite3.connect(str(pmda.STATE_DB_FILE))
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO scan_resume_runs (
                run_id, created_at, updated_at, mode, scan_type, source_signature, status,
                detected_artists_total, detected_albums_total, detected_tracks_total,
                plan_snapshot_ready, discovery_snapshot_ready, discovery_stage, discovery_state_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-live",
                100.0,
                200.0,
                "files",
                "full",
                "sig",
                "running",
                0,
                0,
                25,
                0,
                1,
                "album_candidates",
                json.dumps({"entries_scanned": 25, "files_found": 25}),
            ),
        )
        cur.execute(
            """
            INSERT INTO scan_resume_runs (
                run_id, created_at, updated_at, mode, scan_type, source_signature, status,
                detected_artists_total, detected_albums_total, detected_tracks_total,
                plan_snapshot_ready, discovery_snapshot_ready, discovery_stage, discovery_state_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-paused",
                101.0,
                201.0,
                "files",
                "full",
                "sig",
                "paused",
                0,
                0,
                30,
                0,
                1,
                "album_candidates",
                json.dumps({"entries_scanned": 30, "files_found": 30}),
            ),
        )
        con.commit()
        con.close()

        snap = pmda._get_startup_resume_snapshot("files")

        self.assertIsNotNone(snap)
        self.assertEqual(snap.get("run_id"), "run-live")
        self.assertEqual(snap.get("status"), "running")

    def test_startup_resume_triggers_scan_thread_for_running_resume_run(self):
        calls = []
        resume_status = []

        class _ImmediateThread:
            def __init__(self, *, target=None, args=(), kwargs=None, **_ignored):
                self._target = target
                self._args = args
                self._kwargs = kwargs or {}

            def start(self):
                if self._target:
                    self._target(*self._args, **self._kwargs)

        snapshot = {"run_id": "run-startup", "scan_type": "full", "available": True}

        with pmda.lock:
            pmda.state["scanning"] = False
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_resume_requested_run_id"] = None
            pmda.state["run_improve_after"] = False

        def _fake_try_begin_scan(**kwargs):
            calls.append(dict(kwargs))
            return True, {"status": "started"}

        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_get_startup_resume_snapshot", return_value=snapshot), \
             mock.patch.object(pmda, "_reconcile_scan_move_trace_backlog", return_value=0), \
             mock.patch.object(pmda, "_try_begin_scan", side_effect=_fake_try_begin_scan), \
             mock.patch.object(pmda, "_set_resume_run_status", side_effect=lambda run_id, status, scan_id=None: resume_status.append((run_id, status, scan_id))), \
             mock.patch.object(pmda.threading, "Thread", _ImmediateThread):
            pmda._maybe_resume_interrupted_scan_on_startup(delay_seconds=0)

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["scan_type"], "full")
        self.assertEqual(calls[0]["source"], "startup_resume")
        self.assertFalse(bool(calls[0]["run_improve_after"]))
        self.assertEqual(resume_status, [("run-startup", "running", None)])
        self.assertEqual(pmda.state.get("scan_resume_requested_run_id"), "run-startup")

    def test_persist_resume_discovery_progress_only_updates_counts_without_rewriting_files(self):
        con = sqlite3.connect(str(pmda.STATE_DB_FILE))
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO scan_resume_runs (
                run_id, created_at, updated_at, mode, scan_type, source_signature, status,
                detected_artists_total, detected_albums_total, detected_tracks_total,
                plan_snapshot_ready, discovery_snapshot_ready, discovery_stage, discovery_state_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-progress",
                100.0,
                100.0,
                "files",
                "full",
                "sig",
                "running",
                0,
                0,
                25,
                0,
                1,
                "album_candidates",
                json.dumps({"entries_scanned": 25, "files_found": 25}),
            ),
        )
        cur.execute(
            """
            INSERT INTO scan_resume_discovery_files (run_id, root_index, file_path)
            VALUES (?, ?, ?)
            """,
            ("run-progress", 0, "/host_mnt/disk1/MURRAY/Music/Music_dump/Test/01.flac"),
        )
        con.commit()
        con.close()

        result = pmda._persist_resume_discovery_progress_only(
            "run-progress",
            {
                "stage": "album_candidates",
                "roots": ["/host_mnt/disk1/MURRAY/Music/Music_dump"],
                "current_root_path": "/host_mnt/disk1/MURRAY/Music/Music_dump/Test",
                "shared_entries_scanned": 999,
                "shared_files_found": 222,
                "shared_roots_done": 1,
                "roots_total": 1,
                "folders_found": 77,
                "folders_done": 12,
                "folders_total": 77,
                "albums_found": 12,
                "artists_found": 8,
                "cached_album_folders": ["/music/Music_dump/Test"],
            },
        )

        self.assertTrue(bool(result.get("ok")))

        con = sqlite3.connect(str(pmda.STATE_DB_FILE))
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        run_row = cur.execute(
            """
            SELECT detected_artists_total, detected_albums_total, detected_tracks_total, discovery_stage, discovery_state_json
            FROM scan_resume_runs
            WHERE run_id = ?
            """,
            ("run-progress",),
        ).fetchone()
        files_count = cur.execute(
            "SELECT COUNT(*) FROM scan_resume_discovery_files WHERE run_id = ?",
            ("run-progress",),
        ).fetchone()[0]
        con.close()

        self.assertEqual(int(run_row["detected_artists_total"] or 0), 8)
        self.assertEqual(int(run_row["detected_albums_total"] or 0), 12)
        self.assertEqual(int(run_row["detected_tracks_total"] or 0), 222)
        self.assertEqual(run_row["discovery_stage"], "album_candidates")
        state_json = json.loads(run_row["discovery_state_json"] or "{}")
        self.assertEqual(int(state_json.get("folders_done") or 0), 12)
        self.assertEqual(int(state_json.get("folders_total") or 0), 77)
        self.assertEqual(files_count, 1)


if __name__ == "__main__":
    unittest.main()
