import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pmda


class TrashReleaseCurationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-trash-release-")
        tmp_path = Path(self._tmp.name)
        self._orig = {
            "CONFIG_DIR": pmda.CONFIG_DIR,
            "STATE_DB_FILE": pmda.STATE_DB_FILE,
            "SETTINGS_DB_FILE": pmda.SETTINGS_DB_FILE,
            "CACHE_DB_FILE": pmda.CACHE_DB_FILE,
            "DUPE_ROOT": pmda.DUPE_ROOT,
            "AUTH_DISABLE": pmda.AUTH_DISABLE,
        }
        pmda.CONFIG_DIR = tmp_path
        pmda.STATE_DB_FILE = tmp_path / "state.db"
        pmda.SETTINGS_DB_FILE = tmp_path / "settings.db"
        pmda.CACHE_DB_FILE = tmp_path / "cache.db"
        pmda.DUPE_ROOT = tmp_path / "dupes"
        pmda.AUTH_DISABLE = True
        pmda.init_state_db()
        pmda.init_settings_db()
        pmda.init_cache_db()
        self.client = pmda.app.test_client()

    def tearDown(self):
        for key, value in self._orig.items():
            setattr(pmda, key, value)
        self._tmp.cleanup()

    def test_candidate_flags_obvious_workout_chart_compilation(self):
        row = {
            "album_id": 101,
            "artist_name": "Various Artists",
            "album_title": "Top Hits Ibiza Workout 2024",
            "folder_path": "/music/Music_matched/V/Various Artists/Top Hits Ibiza Workout 2024",
            "year": 2024,
            "genre": "dance",
            "label": "",
            "tags_json": json.dumps(["dance", "fitness"]),
            "primary_tags_json": json.dumps({"compilation": "1"}),
            "track_count": 34,
            "metadata_source": "musicbrainz",
            "has_cover": True,
        }
        candidate = pmda._trash_release_candidate_from_album_row(row)
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["category"], "fitness")
        self.assertGreaterEqual(int(candidate["score"] or 0), 10)
        reason_labels = " | ".join(candidate["reasons"])
        self.assertIn("Workout / fitness wording", reason_labels)
        self.assertIn("Chart / top hits wording", reason_labels)

    def test_candidate_ignores_regular_bandcamp_album(self):
        row = {
            "album_id": 202,
            "artist_name": "Jim O'Rourke",
            "album_title": "Shutting Down Here",
            "folder_path": "/music/Music_matched/J/Jim O'Rourke/Shutting Down Here",
            "year": 2020,
            "genre": "electronic",
            "label": "Portraits GRM",
            "tags_json": json.dumps(["electronic"]),
            "primary_tags_json": json.dumps({"albumartist": "Jim O'Rourke"}),
            "track_count": 1,
            "metadata_source": "bandcamp",
            "has_cover": True,
        }
        candidate = pmda._trash_release_candidate_from_album_row(row)
        self.assertIsNone(candidate)

    def test_move_action_moves_folder_and_records_audit_row(self):
        src = Path(self._tmp.name) / "library" / "V" / "Various Artists" / "Top Hits Ibiza Workout 2024"
        src.mkdir(parents=True, exist_ok=True)
        (src / "01 - Intro.flac").write_bytes(b"flac")

        row = {
            "album_id": 303,
            "artist_name": "Various Artists",
            "album_title": "Top Hits Ibiza Workout 2024",
            "folder_path": str(src),
            "year": 2024,
            "genre": "dance",
            "label": "",
            "tags_json": json.dumps(["dance", "fitness"]),
            "primary_tags_json": json.dumps({"compilation": "1"}),
            "track_count": 34,
            "metadata_source": "musicbrainz",
            "has_cover": False,
        }

        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_trash_release_fetch_library_album_row", return_value=row), \
             mock.patch.object(pmda, "_files_forget_album_folder_global", return_value=True) as forget_mock, \
             mock.patch.object(pmda, "_files_cache_invalidate_all") as invalidate_mock:
            resp = self.client.post(
                "/api/tools/trash-releases/action",
                json={"album_id": 303, "action": "move_to_dupes"},
            )

        self.assertEqual(resp.status_code, 200, resp.get_json())
        payload = resp.get_json() or {}
        self.assertTrue(bool(payload.get("success")))
        moved_path = Path(str(payload.get("to") or ""))
        self.assertTrue(moved_path.exists())
        self.assertFalse(src.exists())
        forget_mock.assert_called_once_with(str(src))
        invalidate_mock.assert_called_once()

        con = sqlite3.connect(str(pmda.STATE_DB_FILE))
        cur = con.cursor()
        cur.execute(
            "SELECT action, status, album_id, destination_path FROM library_curation_actions WHERE album_id = ?",
            (303,),
        )
        row_db = cur.fetchone()
        con.close()
        self.assertIsNotNone(row_db)
        self.assertEqual(row_db[0], "move_to_dupes")
        self.assertEqual(row_db[1], "completed")
        self.assertEqual(int(row_db[2]), 303)
        self.assertEqual(row_db[3], str(moved_path))

    def test_delete_action_removes_folder_and_records_audit_row(self):
        src = Path(self._tmp.name) / "library" / "V" / "Various Artists" / "Karaoke Beach Party"
        src.mkdir(parents=True, exist_ok=True)
        (src / "01 - Track.flac").write_bytes(b"flac")

        row = {
            "album_id": 404,
            "artist_name": "Various Artists",
            "album_title": "Karaoke Beach Party",
            "folder_path": str(src),
            "year": 2023,
            "genre": "party",
            "label": "",
            "tags_json": json.dumps(["party"]),
            "primary_tags_json": json.dumps({"compilation": "1"}),
            "track_count": 28,
            "metadata_source": "discogs",
            "has_cover": False,
        }

        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_trash_release_fetch_library_album_row", return_value=row), \
             mock.patch.object(pmda, "_files_forget_album_folder_global", return_value=True) as forget_mock, \
             mock.patch.object(pmda, "_files_cache_invalidate_all") as invalidate_mock:
            resp = self.client.post(
                "/api/tools/trash-releases/action",
                json={"album_id": 404, "action": "delete_from_disk"},
            )

        self.assertEqual(resp.status_code, 200, resp.get_json())
        payload = resp.get_json() or {}
        self.assertTrue(bool(payload.get("success")))
        self.assertFalse(src.exists())
        forget_mock.assert_called_once_with(str(src))
        invalidate_mock.assert_called_once()

        con = sqlite3.connect(str(pmda.STATE_DB_FILE))
        cur = con.cursor()
        cur.execute(
            "SELECT action, status, album_id FROM library_curation_actions WHERE album_id = ?",
            (404,),
        )
        row_db = cur.fetchone()
        con.close()
        self.assertIsNotNone(row_db)
        self.assertEqual(row_db[0], "delete_from_disk")
        self.assertEqual(row_db[1], "completed")
        self.assertEqual(int(row_db[2]), 404)


if __name__ == "__main__":
    unittest.main()
