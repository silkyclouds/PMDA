import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pmda


class OpsBackupSnapshotTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-ops-backup-")
        tmp_path = Path(self._tmp.name)
        self._orig = {
            "CONFIG_DIR": pmda.CONFIG_DIR,
            "STATE_DB_FILE": pmda.STATE_DB_FILE,
            "SETTINGS_DB_FILE": pmda.SETTINGS_DB_FILE,
            "CACHE_DB_FILE": pmda.CACHE_DB_FILE,
            "DUPE_ROOT": pmda.DUPE_ROOT,
            "PMDA_PGDATA": pmda.PMDA_PGDATA,
            "AUTH_DISABLE": pmda.AUTH_DISABLE,
        }
        pmda.CONFIG_DIR = tmp_path
        pmda.STATE_DB_FILE = tmp_path / "state.db"
        pmda.SETTINGS_DB_FILE = tmp_path / "settings.db"
        pmda.CACHE_DB_FILE = tmp_path / "cache.db"
        pmda.DUPE_ROOT = tmp_path / "dupes"
        pmda.PMDA_PGDATA = str(tmp_path / "postgres-data")
        pmda.AUTH_DISABLE = True
        pmda.init_state_db()
        pmda.init_settings_db()
        pmda.init_cache_db()
        self.client = pmda.app.test_client()

    def tearDown(self):
        for key, value in self._orig.items():
            setattr(pmda, key, value)
        self._tmp.cleanup()

    def test_admin_ops_snapshot_reports_paths_and_backups(self):
        with mock.patch.object(pmda, "_managed_runtime_status_snapshot", return_value=None), mock.patch.object(
            pmda,
            "_read_media_cache_usage",
            return_value={"root": str(pmda.CONFIG_DIR / "media_cache"), "total": {"walk_truncated": False}},
        ) as media_mock:
            resp = self.client.get("/api/admin/ops/snapshot")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        media_mock.assert_called_once_with(max_files=pmda.PMDA_OPS_SNAPSHOT_MEDIA_CACHE_MAX_WALK_FILES)
        payload = resp.get_json() or {}
        self.assertIn("storage", payload)
        self.assertIn("sqlite", payload)
        self.assertIn("backups", payload)
        self.assertEqual(payload.get("config_dir"), str(pmda.CONFIG_DIR))
        self.assertEqual(str((payload.get("storage") or {}).get("config", {}).get("path") or ""), str(pmda.CONFIG_DIR))
        self.assertIsInstance(payload.get("backups"), list)

    def test_admin_ops_backup_creates_sqlite_bundle_and_manifest(self):
        with mock.patch.object(pmda, "_managed_runtime_status_snapshot", return_value=None):
            resp = self.client.post("/api/admin/ops/backup", json={"include_pg_dump": False})
        self.assertEqual(resp.status_code, 200, resp.get_json())
        payload = resp.get_json() or {}
        self.assertEqual(payload.get("status"), "ok")
        backup_path = Path(str(payload.get("backup_path") or ""))
        manifest_path = Path(str(payload.get("manifest_path") or ""))
        snapshot_path = Path(str(payload.get("snapshot_path") or ""))
        self.assertTrue(backup_path.exists())
        self.assertTrue(manifest_path.exists())
        self.assertTrue(snapshot_path.exists())
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(str(manifest.get("status") or ""), "ok")
        sqlite_rows = payload.get("sqlite") or {}
        for key in ("settings_db", "state_db", "cache_db"):
            row = dict(sqlite_rows.get(key) or {})
            self.assertTrue(bool(row.get("ok")), key)
            self.assertTrue(Path(str(row.get("path") or "")).exists(), key)


if __name__ == "__main__":
    unittest.main()
