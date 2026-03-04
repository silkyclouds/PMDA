import tempfile
import unittest
from pathlib import Path

import pmda


class AuthRbacIntegrationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-auth-rbac-")
        tmp_path = Path(self._tmp.name)

        self._orig = {
            "CONFIG_DIR": pmda.CONFIG_DIR,
            "STATE_DB_FILE": pmda.STATE_DB_FILE,
            "SETTINGS_DB_FILE": pmda.SETTINGS_DB_FILE,
            "CACHE_DB_FILE": pmda.CACHE_DB_FILE,
            "DROP_ALBUMS_BASE": pmda.DROP_ALBUMS_BASE,
            "PLEX_CONFIGURED": pmda.PLEX_CONFIGURED,
            "PLEX_DB_FILE": pmda.PLEX_DB_FILE,
            "AUTH_DISABLE": pmda.AUTH_DISABLE,
            "AUTH_ALLOW_PUBLIC_BOOTSTRAP": pmda.AUTH_ALLOW_PUBLIC_BOOTSTRAP,
            "AUTH_TRUST_PROXY_HEADERS": pmda.AUTH_TRUST_PROXY_HEADERS,
        }

        pmda.CONFIG_DIR = tmp_path
        pmda.STATE_DB_FILE = tmp_path / "state.db"
        pmda.SETTINGS_DB_FILE = tmp_path / "settings.db"
        pmda.CACHE_DB_FILE = tmp_path / "cache.db"
        pmda.DROP_ALBUMS_BASE = tmp_path / "drop_albums"
        pmda.PLEX_CONFIGURED = False
        pmda.PLEX_DB_FILE = str(tmp_path / "com.plexapp.plugins.library.db")
        pmda.AUTH_DISABLE = False
        pmda.AUTH_ALLOW_PUBLIC_BOOTSTRAP = True
        pmda.AUTH_TRUST_PROXY_HEADERS = False
        pmda._AUTH_RATE_LIMIT_BUCKETS.clear()

        pmda.init_state_db()
        pmda.init_settings_db()
        pmda.init_cache_db()

        self.client = pmda.app.test_client()

    def tearDown(self):
        pmda._AUTH_RATE_LIMIT_BUCKETS.clear()
        for key, value in self._orig.items():
            setattr(pmda, key, value)
        self._tmp.cleanup()

    @staticmethod
    def _auth_header(token: str) -> dict:
        return {"Authorization": f"Bearer {token}"}

    def _bootstrap_admin_and_login(self) -> dict:
        bootstrap = self.client.post(
            "/api/auth/bootstrap",
            json={
                "username": "admin",
                "password": "AdminPassword123!",
                "password_confirm": "AdminPassword123!",
            },
        )
        self.assertEqual(bootstrap.status_code, 200, bootstrap.get_json())

        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "AdminPassword123!"},
        )
        self.assertEqual(login.status_code, 200, login.get_json())
        payload = login.get_json() or {}
        self.assertTrue(payload.get("ok"))
        self.assertTrue(payload.get("token"))
        return payload

    def test_bootstrap_login_and_logout_flow(self):
        status_before = self.client.get("/api/auth/bootstrap/status")
        self.assertEqual(status_before.status_code, 200)
        self.assertTrue((status_before.get_json() or {}).get("bootstrap_required"))

        protected_before_bootstrap = self.client.get("/api/config")
        self.assertEqual(protected_before_bootstrap.status_code, 428)

        login_before_bootstrap = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "AdminPassword123!"},
        )
        self.assertEqual(login_before_bootstrap.status_code, 428)

        auth_payload = self._bootstrap_admin_and_login()
        token = str(auth_payload["token"])

        status_after = self.client.get("/api/auth/bootstrap/status")
        self.assertEqual(status_after.status_code, 200)
        self.assertFalse((status_after.get_json() or {}).get("bootstrap_required"))

        second_bootstrap = self.client.post(
            "/api/auth/bootstrap",
            json={
                "username": "admin2",
                "password": "AdminPassword123!",
                "password_confirm": "AdminPassword123!",
            },
        )
        self.assertEqual(second_bootstrap.status_code, 409)

        bad_login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "WrongPassword123!"},
        )
        self.assertEqual(bad_login.status_code, 401)

        me = self.client.get("/api/auth/me", headers=self._auth_header(token))
        self.assertEqual(me.status_code, 200, me.get_json())
        me_user = (me.get_json() or {}).get("user") or {}
        self.assertTrue(me_user.get("is_admin"))
        self.assertEqual(me_user.get("username"), "admin")

        logout = self.client.post("/api/auth/logout", headers=self._auth_header(token))
        self.assertEqual(logout.status_code, 200)

        me_after_logout = self.client.get("/api/auth/me", headers=self._auth_header(token))
        self.assertEqual(me_after_logout.status_code, 401)

    def test_admin_vs_read_only_rbac_and_download_permission(self):
        admin_auth = self._bootstrap_admin_and_login()
        admin_token = str(admin_auth["token"])

        create_reader = self.client.post(
            "/api/admin/users",
            headers=self._auth_header(admin_token),
            json={
                "username": "reader",
                "password": "ReaderPassword123!",
                "password_confirm": "ReaderPassword123!",
                "is_admin": False,
                "can_download": False,
                "is_active": True,
            },
        )
        self.assertEqual(create_reader.status_code, 200, create_reader.get_json())
        reader_user = (create_reader.get_json() or {}).get("user") or {}
        reader_id = int(reader_user["id"])

        reader_login = self.client.post(
            "/api/auth/login",
            json={"username": "reader", "password": "ReaderPassword123!"},
        )
        self.assertEqual(reader_login.status_code, 200, reader_login.get_json())
        reader_token = str((reader_login.get_json() or {}).get("token") or "")
        self.assertTrue(reader_token)

        reader_admin_users = self.client.get("/api/admin/users", headers=self._auth_header(reader_token))
        self.assertEqual(reader_admin_users.status_code, 403)

        reader_library_stats = self.client.get("/api/library/stats", headers=self._auth_header(reader_token))
        self.assertNotEqual(reader_library_stats.status_code, 403, reader_library_stats.get_json())

        reader_write_attempt = self.client.post(
            "/api/library/improve-all",
            headers=self._auth_header(reader_token),
            json={},
        )
        self.assertEqual(reader_write_attempt.status_code, 403)

        reader_download_blocked = self.client.get(
            "/api/library/album/1/download",
            headers=self._auth_header(reader_token),
        )
        self.assertEqual(reader_download_blocked.status_code, 403)

        grant_download = self.client.put(
            f"/api/admin/users/{reader_id}",
            headers=self._auth_header(admin_token),
            json={"can_download": True},
        )
        self.assertEqual(grant_download.status_code, 200, grant_download.get_json())

        reader_download_allowed = self.client.get(
            "/api/library/album/1/download",
            headers=self._auth_header(reader_token),
        )
        self.assertNotEqual(reader_download_allowed.status_code, 403, reader_download_allowed.get_json())


if __name__ == "__main__":
    unittest.main()
