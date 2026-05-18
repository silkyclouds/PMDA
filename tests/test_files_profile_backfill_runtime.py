import unittest
from unittest import mock

import pmda


class FilesProfileBackfillRuntimeTests(unittest.TestCase):
    def setUp(self):
        with pmda._files_profile_backfill_lock:
            self._prev_backfill_state = dict(pmda._files_profile_backfill_state)
            pmda._files_profile_backfill_state.update(
                {
                    "running": False,
                    "reason": "",
                    "started_at": 0,
                    "finished_at": 0,
                    "cover_only": False,
                    "current": 0,
                    "total": 0,
                    "current_artist": "",
                    "errors": 0,
                    "pending_artist_profiles": 0,
                    "pending_album_profiles": 0,
                    "eligible_album_profiles": 0,
                    "pending_album_covers": 0,
                    "last_probe_at": 0,
                    "storage_scope_enabled": False,
                    "storage_scope_mode": "",
                    "storage_scope_devices": [],
                }
            )
        self._prev_idle_state = dict(pmda._files_profile_backfill_idle_state)
        pmda._files_profile_backfill_idle_state.update(
            {
                "last_probe_at": 0.0,
                "last_started_at": 0.0,
                "last_reason": "",
                "pending_artist_profiles": 0,
                "pending_album_profiles": 0,
                "eligible_album_profiles": 0,
                "pending_album_covers": 0,
            }
        )
        with pmda.lock:
            self._prev_scan_bits = (
                bool(pmda.state.get("scanning")),
                bool(pmda.state.get("scan_finalizing")),
                bool(pmda.state.get("scan_starting")),
                bool(pmda.state.get("scan_discovery_running")),
            )
            self._prev_storage_bits = {
                "storage_current_device_id": pmda.state.get("storage_current_device_id"),
                "storage_current_device_label": pmda.state.get("storage_current_device_label"),
                "storage_scan_plan": list(pmda.state.get("storage_scan_plan") or []),
            }
            pmda.state["scanning"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_starting"] = False
            pmda.state["scan_discovery_running"] = False
            pmda.state["storage_current_device_id"] = None
            pmda.state["storage_current_device_label"] = None
            pmda.state["storage_scan_plan"] = []
        with pmda._files_profile_jobs_lock:
            self._prev_jobs_active = set(pmda._files_profile_jobs_active)
            pmda._files_profile_jobs_active.clear()

    def tearDown(self):
        with pmda._files_profile_backfill_lock:
            pmda._files_profile_backfill_state.clear()
            pmda._files_profile_backfill_state.update(self._prev_backfill_state)
        pmda._files_profile_backfill_idle_state.clear()
        pmda._files_profile_backfill_idle_state.update(self._prev_idle_state)
        with pmda.lock:
            (
                pmda.state["scanning"],
                pmda.state["scan_finalizing"],
                pmda.state["scan_starting"],
                pmda.state["scan_discovery_running"],
            ) = self._prev_scan_bits
            pmda.state["storage_current_device_id"] = self._prev_storage_bits["storage_current_device_id"]
            pmda.state["storage_current_device_label"] = self._prev_storage_bits["storage_current_device_label"]
            pmda.state["storage_scan_plan"] = self._prev_storage_bits["storage_scan_plan"]
        with pmda._files_profile_jobs_lock:
            pmda._files_profile_jobs_active.clear()
            pmda._files_profile_jobs_active.update(self._prev_jobs_active)

    def test_enqueue_files_profile_enrichment_allowed_while_idle_in_files_mode(self):
        fake_thread = mock.Mock()
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), mock.patch.object(
            pmda,
            "_storage_profile_enrichment_scope_for_artist",
            return_value={"allowed": True},
        ), mock.patch.object(pmda.threading, "Thread", return_value=fake_thread):
            started = pmda._enqueue_files_profile_enrichment(
                artist_name="Test Artist",
                artist_norm="test artist",
                albums=[("Album", "album")],
            )
        self.assertTrue(started)
        fake_thread.start.assert_called_once()

    def test_enqueue_files_profile_enrichment_is_blocked_during_cover_only_backfill(self):
        fake_thread = mock.Mock()
        with pmda._files_profile_backfill_lock:
            pmda._files_profile_backfill_state["running"] = True
            pmda._files_profile_backfill_state["cover_only"] = True
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), mock.patch.object(pmda.threading, "Thread", return_value=fake_thread):
            started = pmda._enqueue_files_profile_enrichment(
                artist_name="Test Artist",
                artist_norm="test artist",
                albums=[("Album", "album")],
            )
        self.assertFalse(started)
        fake_thread.start.assert_not_called()

    def test_enqueue_files_profile_enrichment_is_blocked_when_storage_scope_rejects_artist(self):
        fake_thread = mock.Mock()
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), mock.patch.object(
            pmda,
            "_storage_profile_enrichment_scope_for_artist",
            return_value={"allowed": False, "reason": "artist_out_of_scope"},
        ), mock.patch.object(pmda.threading, "Thread", return_value=fake_thread):
            started = pmda._enqueue_files_profile_enrichment(
                artist_name="Test Artist",
                artist_norm="test artist",
                albums=[("Album", "album")],
            )
        self.assertFalse(started)
        fake_thread.start.assert_not_called()

    def test_idle_profile_backfill_autostarts_when_pending_work_exists(self):
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), mock.patch.object(
            pmda,
            "_files_profile_backfill_pending_work",
            return_value={
                "pending_artist_profiles": 3,
                "pending_album_profiles": 12,
                "eligible_album_profiles": 20,
                "pending_album_covers": 5,
            },
        ), mock.patch.object(pmda, "_trigger_files_profile_backfill_async", return_value=True) as trigger, mock.patch.object(
            pmda,
            "_files_index_is_running",
            return_value=False,
        ):
            started = pmda._files_profile_backfill_maybe_start_idle(now_ts=1000.0, reason="test_idle")
        self.assertTrue(started)
        trigger.assert_called_once_with(reason="test_idle")
        self.assertEqual(pmda._files_profile_backfill_idle_state["pending_album_profiles"], 12)
        self.assertEqual(pmda._files_profile_backfill_idle_state["eligible_album_profiles"], 20)
        self.assertEqual(pmda._files_profile_backfill_idle_state["pending_album_covers"], 5)

    def test_idle_profile_backfill_does_not_start_without_pending_work(self):
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), mock.patch.object(
            pmda,
            "_files_profile_backfill_pending_work",
            return_value={
                "pending_artist_profiles": 0,
                "pending_album_profiles": 0,
                "eligible_album_profiles": 20,
                "pending_album_covers": 0,
            },
        ), mock.patch.object(pmda, "_trigger_files_profile_backfill_async", return_value=True) as trigger, mock.patch.object(
            pmda,
            "_files_index_is_running",
            return_value=False,
        ):
            started = pmda._files_profile_backfill_maybe_start_idle(now_ts=1000.0, reason="test_idle")
        self.assertFalse(started)
        trigger.assert_not_called()

    def test_idle_profile_backfill_does_not_start_while_files_index_rebuild_runs(self):
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), mock.patch.object(
            pmda,
            "_files_index_is_running",
            return_value=True,
        ), mock.patch.object(pmda, "_files_profile_backfill_pending_work") as pending, mock.patch.object(
            pmda,
            "_trigger_files_profile_backfill_async",
        ) as trigger:
            started = pmda._files_profile_backfill_maybe_start_idle(now_ts=1000.0, reason="test_idle")
        self.assertFalse(started)
        pending.assert_not_called()
        trigger.assert_not_called()

    def test_idle_profile_backfill_does_not_start_when_storage_power_saver_is_enabled(self):
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), mock.patch.object(
            pmda,
            "_storage_power_saver_active",
            return_value=True,
        ), mock.patch.object(pmda, "_files_profile_backfill_pending_work") as pending, mock.patch.object(
            pmda,
            "_trigger_files_profile_backfill_async",
        ) as trigger:
            started = pmda._files_profile_backfill_maybe_start_idle(now_ts=1000.0, reason="test_idle")
        self.assertFalse(started)
        pending.assert_not_called()
        trigger.assert_not_called()

    def test_trigger_files_profile_backfill_async_rejects_when_files_index_runs(self):
        with mock.patch.object(pmda, "_files_index_is_running", return_value=True):
            started = pmda._trigger_files_profile_backfill_async(reason="manual")
        self.assertFalse(started)

    def test_trigger_files_profile_backfill_async_rejects_cover_only_without_pending_covers(self):
        with mock.patch.object(pmda, "_files_index_is_running", return_value=False), mock.patch.object(
            pmda,
            "_files_profile_backfill_pending_work",
            return_value={
                "pending_artist_profiles": 0,
                "pending_album_profiles": 0,
                "eligible_album_profiles": 0,
                "pending_album_covers": 0,
            },
        ):
            started = pmda._trigger_files_profile_backfill_async(reason="manual_cover_only", cover_only=True)
        self.assertFalse(started)

    def test_trigger_files_profile_backfill_async_rejects_when_scan_scope_has_no_device_budget(self):
        with pmda.lock:
            pmda.state["scanning"] = True
            pmda.state["storage_current_device_id"] = None
            pmda.state["storage_current_device_label"] = None
            pmda.state["storage_scan_plan"] = []
        with mock.patch.object(pmda, "_files_index_is_running", return_value=False), mock.patch.object(
            pmda,
            "_storage_unraid_settings",
            return_value={"enabled": True, "max_active_devices": 1},
        ), mock.patch.object(
            pmda,
            "_storage_power_saver_active",
            return_value=True,
        ), mock.patch.object(pmda.threading, "Thread") as fake_thread:
            started = pmda._trigger_files_profile_backfill_async(reason="scan_started")
        self.assertFalse(started)
        fake_thread.assert_not_called()

    def test_trigger_files_profile_backfill_async_rejects_during_disk_aware_discovery(self):
        with pmda.lock:
            pmda.state["scanning"] = True
            pmda.state["scan_discovery_running"] = True
            pmda.state["storage_current_device_id"] = "disk4"
            pmda.state["storage_current_device_label"] = "disk4"
            pmda.state["storage_scan_plan"] = [
                {
                    "storage_device_id": "disk4",
                    "storage_device_label": "disk4",
                    "storage_bucket_order": 4,
                    "canonical_root": "/music/Music_dump",
                    "status": "running",
                }
            ]
        with mock.patch.object(pmda, "_files_index_is_running", return_value=False), mock.patch.object(
            pmda,
            "_storage_power_saver_active",
            return_value=True,
        ), mock.patch.object(pmda.threading, "Thread") as fake_thread:
            started = pmda._trigger_files_profile_backfill_async(reason="during_discovery")
        self.assertFalse(started)
        fake_thread.assert_not_called()

    def test_run_files_profile_enrichment_job_returns_immediately_when_storage_scope_rejects_artist(self):
        with mock.patch.object(
            pmda,
            "_storage_profile_enrichment_scope_for_artist",
            return_value={
                "allowed": False,
                "reason": "artist_out_of_scope",
                "allowed_device_ids": ["disk4"],
                "matched_device_ids": ["disk8"],
            },
        ), mock.patch.object(pmda, "_files_pg_connection") as pg_conn:
            pmda._run_files_profile_enrichment_job(
                job_key="artist",
                artist_name="Test Artist",
                artist_norm="test artist",
                albums=[("Album", "album")],
            )
        pg_conn.assert_not_called()

    def test_scheduler_enrich_batch_queues_async_backfill_in_files_mode(self):
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), mock.patch.object(
            pmda,
            "_trigger_files_profile_backfill_async",
            return_value=True,
        ) as trigger:
            ok, _message, payload = pmda._scheduler_run_enrich_batch()
        self.assertTrue(ok)
        self.assertTrue(payload["profiles"]["queued"])
        trigger.assert_called_once_with(reason="scheduler_enrich_batch")

    def test_discogs_min_interval_uses_effective_rpm_when_no_override(self):
        with mock.patch.object(pmda, "_discogs_effective_rpm", return_value=30.0):
            interval = pmda._discogs_min_interval_sec()
        self.assertAlmostEqual(interval, 2.0, places=2)

    def test_album_profile_fetch_strength_prefers_strict_then_provider_hints(self):
        self.assertEqual(
            pmda._files_album_profile_fetch_strength(
                strict_verified=True,
                metadata_source="",
            ),
            3,
        )
        self.assertEqual(
            pmda._files_album_profile_fetch_strength(
                strict_verified=False,
                metadata_source="lastfm",
            ),
            2,
        )
        self.assertEqual(
            pmda._files_album_profile_fetch_strength(
                strict_verified=False,
                metadata_source="",
                bandcamp_album_url="https://artist.bandcamp.com/album/test",
            ),
            2,
        )
        self.assertEqual(
            pmda._files_album_profile_fetch_strength(
                strict_verified=False,
                metadata_source="",
            ),
            0,
        )

    def test_files_upsert_album_profile_binds_source_value_before_now(self):
        calls = []

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params):
                calls.append((query, params))

        class FakeConn:
            def cursor(self):
                return FakeCursor()

        pmda._files_upsert_album_profile(
            FakeConn(),
            artist_norm="artist",
            title_norm="album",
            album_title="Album",
            profile={
                "description": "Desc",
                "short_description": "Short",
                "source": "bandcamp",
                "tags": ["ambient"],
            },
        )

        self.assertEqual(len(calls), 1)
        query, params = calls[0]
        self.assertIn("source,\n                updated_at", query)
        self.assertIn("VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())", query)
        self.assertEqual(len(params), 18)
        self.assertEqual(params[-1], "bandcamp")


if __name__ == "__main__":
    unittest.main()
