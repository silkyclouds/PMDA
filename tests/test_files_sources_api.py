import sqlite3
import tempfile
import unittest
from pathlib import Path

import pmda


class FilesSourcesApiTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-files-sources-")
        tmp_path = Path(self._tmp.name)
        self._orig = {
            "CONFIG_DIR": pmda.CONFIG_DIR,
            "STATE_DB_FILE": pmda.STATE_DB_FILE,
            "SETTINGS_DB_FILE": pmda.SETTINGS_DB_FILE,
            "CACHE_DB_FILE": pmda.CACHE_DB_FILE,
            "AUTH_DISABLE": pmda.AUTH_DISABLE,
            "FILES_ROOTS": list(pmda.FILES_ROOTS),
            "LIBRARY_MODE": pmda.LIBRARY_MODE,
        }
        pmda._stop_files_watcher_manager()
        pmda._stop_files_watcher()
        pmda.CONFIG_DIR = tmp_path
        pmda.STATE_DB_FILE = tmp_path / "state.db"
        pmda.SETTINGS_DB_FILE = tmp_path / "settings.db"
        pmda.CACHE_DB_FILE = tmp_path / "cache.db"
        pmda.AUTH_DISABLE = True
        pmda.FILES_ROOTS = []
        pmda.LIBRARY_MODE = "files"
        pmda.init_state_db()
        pmda.init_settings_db()
        pmda.init_cache_db()
        self.client = pmda.app.test_client()

    def tearDown(self):
        pmda._stop_files_watcher_manager()
        pmda._stop_files_watcher()
        for key, value in self._orig.items():
            setattr(pmda, key, value)
        self._tmp.cleanup()

    def _set_files_roots(self, roots: list[str]) -> None:
        pmda._apply_settings_in_memory({"FILES_ROOTS": roots})
        con = sqlite3.connect(str(pmda.SETTINGS_DB_FILE), timeout=5)
        con.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
            ("FILES_ROOTS", ",".join(roots)),
        )
        con.commit()
        con.close()

    def test_get_sources_falls_back_to_files_roots_when_table_empty(self):
        self._set_files_roots(["/music/library", "/music/incoming"])

        resp = self.client.get("/api/files/sources")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        roots = payload.get("roots") or []
        self.assertEqual(len(roots), 2)
        self.assertEqual(str(roots[0].get("path")), "/music/library")
        self.assertEqual(str(roots[1].get("path")), "/music/incoming")
        self.assertEqual(int(payload.get("winner_source_root_id") or 0), 1)

    def test_api_config_files_roots_seeds_source_roots_when_empty(self):
        resp = self.client.put("/api/config", json={"FILES_ROOTS": "/music/a,/music/b"})
        self.assertEqual(resp.status_code, 200)

        con = sqlite3.connect(str(pmda.STATE_DB_FILE), timeout=5)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM files_source_roots")
        count = int((cur.fetchone() or [0])[0] or 0)
        con.close()
        self.assertEqual(count, 2)

    def test_get_sources_prefers_explicit_rows_over_fallback(self):
        self._set_files_roots(["/music/fallback"])
        con = sqlite3.connect(str(pmda.STATE_DB_FILE), timeout=5)
        now = 1.0
        con.execute(
            """
            INSERT INTO files_source_roots(path, role, enabled, priority, is_winner_root, created_at, updated_at)
            VALUES (?, 'library', 1, 10, 1, ?, ?)
            """,
            ("/music/explicit", now, now),
        )
        con.commit()
        con.close()

        resp = self.client.get("/api/files/sources")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        roots = payload.get("roots") or []
        self.assertEqual(len(roots), 1)
        self.assertEqual(str(roots[0].get("path")), "/music/explicit")


if __name__ == "__main__":
    unittest.main()
