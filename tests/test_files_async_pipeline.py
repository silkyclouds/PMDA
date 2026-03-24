import unittest
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory

import pmda


class FilesAsyncPipelineTests(unittest.TestCase):
    def test_files_source_roots_fetch_returns_rows(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            orig_state_db = pmda.STATE_DB_FILE
            try:
                pmda.STATE_DB_FILE = db_path
                pmda.init_state_db()
                con = sqlite3.connect(str(db_path))
                cur = con.cursor()
                cur.execute(
                    """
                    INSERT INTO files_source_roots
                    (source_id, path, role, enabled, priority, is_winner_root, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (7, "/music/test", "library", 1, 10, 0, 1.0, 2.0),
                )
                con.commit()
                con.close()
                rows = pmda._files_source_roots_fetch(enabled_only=True)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["source_id"], 7)
                self.assertEqual(rows[0]["path"], "/music/test")
            finally:
                pmda.STATE_DB_FILE = orig_state_db

    def test_pipeline_inline_flags_keep_dedupe_and_incomplete_in_files_async_mode(self):
        requested = {
            "match_fix": True,
            "dedupe": True,
            "incomplete_move": True,
            "export": True,
            "player_sync": True,
            "sync_target": "none",
        }
        inline = pmda._pipeline_inline_flags(requested, pipeline_async_enabled=True)
        self.assertFalse(bool(inline.get("match_fix")))
        self.assertTrue(bool(inline.get("dedupe")))
        self.assertTrue(bool(inline.get("incomplete_move")))
        self.assertFalse(bool(inline.get("export")))
        self.assertFalse(bool(inline.get("player_sync")))

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

    def test_maintenance_reset_state_db_reseeds_scheduler_defaults(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            db_path.write_text("placeholder", encoding="utf-8")
            calls = {"defaults": 0, "legacy": 0, "chain": 0, "async_default": 0}
            orig_state_db = pmda.STATE_DB_FILE
            orig_insert = pmda._scheduler_insert_default_rules_if_empty
            orig_legacy = pmda._scheduler_migrate_legacy_scan_changed_default
            orig_chain = pmda._scheduler_ensure_post_scan_chain_defaults
            orig_async = pmda._pipeline_migrate_legacy_post_scan_async_default
            try:
                pmda.STATE_DB_FILE = db_path
                pmda._scheduler_insert_default_rules_if_empty = lambda: calls.__setitem__("defaults", calls["defaults"] + 1)
                pmda._scheduler_migrate_legacy_scan_changed_default = lambda: calls.__setitem__("legacy", calls["legacy"] + 1)
                pmda._scheduler_ensure_post_scan_chain_defaults = lambda: calls.__setitem__("chain", calls["chain"] + 1)
                pmda._pipeline_migrate_legacy_post_scan_async_default = lambda: calls.__setitem__("async_default", calls["async_default"] + 1)

                def _fake_reinit():
                    db_path.write_text("reinitialized", encoding="utf-8")

                result = pmda._maintenance_reset_sqlite_db(db_path, _fake_reinit)

                self.assertTrue(result["ok"])
                self.assertTrue(result["reinitialized"])
                self.assertEqual(calls["defaults"], 1)
                self.assertEqual(calls["legacy"], 1)
                self.assertEqual(calls["chain"], 1)
                self.assertEqual(calls["async_default"], 1)
            finally:
                pmda.STATE_DB_FILE = orig_state_db
                pmda._scheduler_insert_default_rules_if_empty = orig_insert
                pmda._scheduler_migrate_legacy_scan_changed_default = orig_legacy
                pmda._scheduler_ensure_post_scan_chain_defaults = orig_chain
                pmda._pipeline_migrate_legacy_post_scan_async_default = orig_async


if __name__ == "__main__":
    unittest.main()
