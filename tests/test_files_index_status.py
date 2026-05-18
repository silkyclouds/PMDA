import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import pmda


class FilesIndexStatusTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-files-index-status-")
        tmp_path = Path(self._tmp.name)
        self._orig = {
            "CONFIG_DIR": pmda.CONFIG_DIR,
            "STATE_DB_FILE": pmda.STATE_DB_FILE,
            "SETTINGS_DB_FILE": pmda.SETTINGS_DB_FILE,
            "CACHE_DB_FILE": pmda.CACHE_DB_FILE,
            "AUTH_DISABLE": pmda.AUTH_DISABLE,
            "files_index": dict((pmda.state.get("files_index") or {})),
        }
        pmda.CONFIG_DIR = tmp_path
        pmda.STATE_DB_FILE = tmp_path / "state.db"
        pmda.SETTINGS_DB_FILE = tmp_path / "settings.db"
        pmda.CACHE_DB_FILE = tmp_path / "cache.db"
        pmda.AUTH_DISABLE = True
        pmda.init_state_db()
        pmda.init_settings_db()
        pmda.init_cache_db()
        with pmda.lock:
            pmda.state["files_index"] = {
                "running": False,
                "started_at": None,
                "finished_at": None,
                "updated_at": None,
                "phase": None,
                "phase_started_at": None,
                "phase_message": None,
                "phase_progress": None,
                "phase_eta_seconds": None,
                "phase_rate_per_sec": None,
                "current_folder": None,
                "current_artist": None,
                "phase_item_done": 0,
                "phase_item_total": 0,
                "phase_item_label": None,
                "folders_processed": 0,
                "total_folders": 0,
                "collapsed_groups": 0,
                "entries_scanned": 0,
                "discovered_audio_files": 0,
                "artists": 0,
                "albums": 0,
                "tracks": 0,
                "error": None,
            }
        self.client = pmda.app.test_client()

    def tearDown(self):
        for key, value in self._orig.items():
            if key == "files_index":
                with pmda.lock:
                    pmda.state["files_index"] = dict(value)
            else:
                setattr(pmda, key, value)
        self._tmp.cleanup()

    def test_files_index_set_state_resets_eta_metrics_on_phase_change(self):
        with mock.patch.object(pmda.time, "time", side_effect=[100.0, 120.0]):
            pmda._files_index_set_state(
                phase="discovering",
                phase_progress=15.5,
                phase_eta_seconds=900,
                phase_rate_per_sec=22.0,
                current_folder="/music/Music_matched/A",
            )
            same_phase = pmda._files_index_get_state()
            self.assertEqual(same_phase.get("phase"), "discovering")
            self.assertEqual(same_phase.get("phase_progress"), 15.5)
            self.assertEqual(same_phase.get("phase_eta_seconds"), 900)
            self.assertEqual(same_phase.get("phase_rate_per_sec"), 22.0)
            self.assertEqual(same_phase.get("phase_started_at"), 100.0)
            self.assertEqual(same_phase.get("updated_at"), 100.0)

            pmda._files_index_set_state(
                phase="collapsing",
                current_folder="collapsing_release_segments",
            )

        collapsed = pmda._files_index_get_state()
        self.assertEqual(collapsed.get("phase"), "collapsing")
        self.assertEqual(collapsed.get("current_folder"), "collapsing_release_segments")
        self.assertEqual(collapsed.get("phase_started_at"), 120.0)
        self.assertEqual(collapsed.get("updated_at"), 120.0)
        self.assertIsNone(collapsed.get("phase_progress"))
        self.assertIsNone(collapsed.get("phase_eta_seconds"))
        self.assertIsNone(collapsed.get("phase_rate_per_sec"))

    def test_api_library_files_index_status_includes_indexed_counts_and_phase_fields(self):
        pmda._files_index_set_state(
            running=True,
            started_at=111.0,
            finished_at=None,
            phase="parsing",
            phase_message="Reading 42 / 240 normalized album folders",
            phase_progress=17.5,
            phase_eta_seconds=3600,
            phase_rate_per_sec=3.25,
            current_folder="/music/Music_matched/J/Jim O'Rourke",
            current_artist="Jim O'Rourke",
            phase_item_done=42,
            phase_item_total=240,
            phase_item_label="artist pages",
            folders_processed=42,
            total_folders=240,
            collapsed_groups=7,
            entries_scanned=15000,
            discovered_audio_files=1200,
        )
        with mock.patch.object(pmda, "_files_index_read_counts_fast", return_value=(11, 22, 33)) as counts_mock, \
             mock.patch.object(pmda, "_files_index_read_counts", side_effect=AssertionError("status endpoint must use fast counts")):
            resp = self.client.get("/api/library/files-index/status")
        self.assertEqual(resp.status_code, 200)
        counts_mock.assert_called_once()
        payload = resp.get_json() or {}
        self.assertTrue(bool(payload.get("running")))
        self.assertEqual(payload.get("phase"), "parsing")
        self.assertEqual(payload.get("phase_message"), "Reading 42 / 240 normalized album folders")
        self.assertEqual(float(payload.get("phase_progress") or 0.0), 17.5)
        self.assertEqual(int(payload.get("phase_eta_seconds") or 0), 3600)
        self.assertEqual(float(payload.get("phase_rate_per_sec") or 0.0), 3.25)
        self.assertEqual(payload.get("current_artist"), "Jim O'Rourke")
        self.assertEqual(int(payload.get("phase_item_done") or 0), 42)
        self.assertEqual(int(payload.get("phase_item_total") or 0), 240)
        self.assertEqual(payload.get("phase_item_label"), "artist pages")
        self.assertEqual(int(payload.get("folders_processed") or 0), 42)
        self.assertEqual(int(payload.get("total_folders") or 0), 240)
        self.assertEqual(int(payload.get("collapsed_groups") or 0), 7)
        self.assertEqual(int(payload.get("entries_scanned") or 0), 15000)
        self.assertEqual(int(payload.get("discovered_audio_files") or 0), 1200)
        self.assertEqual(int(payload.get("indexed_artists") or 0), 11)
        self.assertEqual(int(payload.get("indexed_albums") or 0), 22)
        self.assertEqual(int(payload.get("indexed_tracks") or 0), 33)

    def test_files_index_running_helper_respects_running_flag_and_phase_filter(self):
        pmda._files_index_set_state(running=True, phase="artist_enrichment")
        self.assertTrue(pmda._files_index_is_running())
        self.assertTrue(pmda._files_index_is_running(phases={"artist_enrichment"}))
        self.assertFalse(pmda._files_index_is_running(phases={"media_cache"}))
        pmda._files_index_set_state(running=False, phase="done")
        self.assertFalse(pmda._files_index_is_running())

    def test_pipeline_job_snapshot_persists_and_resets_terminal_job_start(self):
        pmda._pipeline_job_update(
            "library_index",
            status="running",
            phase="parsing",
            current=10,
            total=100,
            current_item="/music/A",
            message="Parsing",
            run_id="run-1",
        )
        first = pmda._pipeline_job_snapshot()
        self.assertEqual(first["library_index"]["status"], "running")
        self.assertEqual(first["library_index"]["phase"], "parsing")
        first_started = float(first["library_index"]["started_at"] or 0)
        self.assertGreater(first_started, 0)

        pmda._pipeline_job_update(
            "library_index",
            status="completed",
            phase="done",
            current=100,
            total=100,
            message="Done",
            run_id="run-1",
            finished=True,
        )
        time.sleep(0.01)
        pmda._pipeline_job_update(
            "library_index",
            status="running",
            phase="discovering",
            current=0,
            total=0,
            message="Restarted",
            run_id="run-2",
        )

        restarted = pmda._pipeline_job_snapshot()
        job = restarted["library_index"]
        self.assertEqual(job["status"], "running")
        self.assertEqual(job["run_id"], "run-2")
        self.assertGreater(float(job["started_at"] or 0), first_started)

    def test_pipeline_job_registry_persists_all_long_running_job_types(self):
        for index, job_type in enumerate(pmda.PIPELINE_JOB_TYPES, start=1):
            pmda._pipeline_job_update(
                job_type,
                status="running",
                phase=f"phase-{index}",
                current=index,
                total=100,
                current_item=f"item-{index}",
                message=f"job {job_type} heartbeat",
                run_id="restart-test",
            )

        snapshot = pmda._pipeline_job_snapshot()

        for index, job_type in enumerate(pmda.PIPELINE_JOB_TYPES, start=1):
            self.assertIn(job_type, snapshot)
            job = snapshot[job_type]
            self.assertEqual(job["status"], "running")
            self.assertEqual(job["run_id"], "restart-test")
            self.assertEqual(job["phase"], f"phase-{index}")
            self.assertEqual(job["current"], index)
            self.assertEqual(job["total"], 100)
            self.assertGreater(float(job["heartbeat_at"] or 0), 0)

    def test_pipeline_jobs_api_returns_durable_registry(self):
        pmda._pipeline_job_update(
            "profile_backfill",
            status="running",
            phase="p0",
            current=2,
            total=10,
            current_item="Artist",
            message="Backfilling",
            run_id="test",
        )
        resp = self.client.get("/api/pipeline/jobs")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertIn("profile_backfill", payload.get("jobs") or {})
        self.assertEqual(payload["jobs"]["profile_backfill"]["phase"], "p0")

    def test_collapse_nested_album_folder_groups_emits_final_progress_with_merged_count(self):
        with tempfile.TemporaryDirectory(prefix="pmda-collapse-progress-") as tmpdir:
            root = Path(tmpdir) / "incoming"
            root.mkdir(parents=True, exist_ok=True)
            parent = root / "Massive Box Set"
            parent.mkdir(parents=True, exist_ok=True)
            (parent / "folder.jpg").write_bytes(b"cover")
            by_folder: dict[Path, list[Path]] = {}
            for disc_name in ("CD1", "CD2"):
                child = parent / disc_name
                child.mkdir(parents=True, exist_ok=True)
                files: list[Path] = []
                for index in (1, 2):
                    track = child / f"{index:02d} - Track {index}.flac"
                    track.write_bytes(b"flac")
                    files.append(track)
                by_folder[child] = files

            def _fake_tags(path: Path) -> dict:
                return {
                    "albumartist": "Test Artist",
                    "album": Path(path).parent.name,
                    "title": Path(path).stem,
                }

            progress_events: list[dict] = []
            with mock.patch.object(pmda, "extract_tags", side_effect=_fake_tags):
                collapsed = pmda._collapse_nested_album_folder_groups(
                    by_folder,
                    root_dirs={str(root.resolve())},
                    progress_cb=lambda payload: progress_events.append(dict(payload)),
                )

            self.assertEqual(len(collapsed), 1)
            self.assertGreaterEqual(len(progress_events), 2)
            final_event = progress_events[-1]
            self.assertEqual(final_event.get("phase"), "collapsing")
            self.assertEqual(int(final_event.get("parents_processed") or 0), 1)
            self.assertEqual(int(final_event.get("parents_total") or 0), 1)
            self.assertEqual(int(final_event.get("collapsed_groups") or 0), 1)


if __name__ == "__main__":
    unittest.main()
