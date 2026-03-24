import unittest

import pmda


class FilesAsyncPipelineTests(unittest.TestCase):
    def test_scheduler_enrich_batch_skips_legacy_magic_in_files_mode(self):
        orig_get_library_mode = pmda._get_library_mode
        orig_build_candidates = pmda._scheduler_build_improve_candidates
        orig_run_improve = pmda._run_improve_all_albums_global
        orig_profile_backfill = pmda._run_files_profile_backfill
        orig_state = dict(pmda._files_profile_backfill_state or {})
        calls = {"improve": 0, "backfill": 0}
        try:
            pmda._get_library_mode = lambda: "files"

            def _should_not_build():
                raise AssertionError("legacy improve-all candidates should not be built in Files mode")

            def _should_not_run(_albums):
                calls["improve"] += 1

            def _fake_backfill(*, reason="manual", sleep_sec=0.0):
                calls["backfill"] += 1
                with pmda._files_profile_backfill_lock:
                    pmda._files_profile_backfill_state.update(
                        {
                            "running": False,
                            "reason": reason,
                            "started_at": 1,
                            "finished_at": 2,
                            "current": 3,
                            "total": 4,
                            "current_artist": "",
                            "errors": 0,
                        }
                    )

            pmda._scheduler_build_improve_candidates = _should_not_build
            pmda._run_improve_all_albums_global = _should_not_run
            pmda._run_files_profile_backfill = _fake_backfill

            ok, message, metrics = pmda._scheduler_run_enrich_batch()

            self.assertTrue(ok)
            self.assertIn("Enrichment batch finished", message)
            self.assertEqual(calls["improve"], 0)
            self.assertEqual(calls["backfill"], 1)
            self.assertEqual(int(metrics.get("albums") or 0), 0)
            self.assertEqual(int((metrics.get("profiles") or {}).get("artists_done") or 0), 3)
        finally:
            pmda._get_library_mode = orig_get_library_mode
            pmda._scheduler_build_improve_candidates = orig_build_candidates
            pmda._run_improve_all_albums_global = orig_run_improve
            pmda._run_files_profile_backfill = orig_profile_backfill
            with pmda._files_profile_backfill_lock:
                pmda._files_profile_backfill_state.clear()
                pmda._files_profile_backfill_state.update(orig_state)

    def test_enqueue_files_profile_enrichment_forwards_fast_artist_only_flags(self):
        orig_thread = pmda.threading.Thread
        started = {}
        try:
            class _FakeThread:
                def __init__(self, *, target=None, kwargs=None, daemon=None, name=None):
                    started["target"] = target
                    started["kwargs"] = dict(kwargs or {})
                    started["daemon"] = daemon
                    started["name"] = name

                def start(self):
                    started["started"] = True

            pmda.threading.Thread = _FakeThread
            with pmda._files_profile_jobs_lock:
                pmda._files_profile_jobs_active.discard("sigur ros")
            ok = pmda._enqueue_files_profile_enrichment(
                artist_name="Sigur Rós",
                artist_norm="sigur ros",
                albums=[],
                allow_soft_profiles=True,
                skip_album_profiles=True,
                fast_mode=True,
                force=True,
            )
            self.assertTrue(ok)
            self.assertTrue(started.get("started"))
            self.assertTrue(bool(started.get("kwargs", {}).get("skip_album_profiles")))
            self.assertTrue(bool(started.get("kwargs", {}).get("fast_mode")))
        finally:
            pmda.threading.Thread = orig_thread
            with pmda._files_profile_jobs_lock:
                pmda._files_profile_jobs_active.discard("sigur ros")


if __name__ == "__main__":
    unittest.main()
