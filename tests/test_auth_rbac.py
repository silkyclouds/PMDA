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
            "AUTH_SESSION_COOKIE_NAME": pmda.AUTH_SESSION_COOKIE_NAME,
            "AUTH_SESSION_COOKIE_SECURE": pmda.AUTH_SESSION_COOKIE_SECURE,
            "AUTH_SESSION_COOKIE_SAMESITE": pmda.AUTH_SESSION_COOKIE_SAMESITE,
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
        pmda.AUTH_SESSION_COOKIE_NAME = "pmda_session"
        pmda.AUTH_SESSION_COOKIE_SECURE = False
        pmda.AUTH_SESSION_COOKIE_SAMESITE = "Lax"
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
                "can_view_statistics": False,
                "is_active": True,
            },
        )
        self.assertEqual(create_reader.status_code, 200, create_reader.get_json())
        reader_user = (create_reader.get_json() or {}).get("user") or {}
        reader_id = int(reader_user["id"])
        self.assertFalse(bool(reader_user.get("can_view_statistics")))

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
        reader_statistics_blocked = self.client.get("/api/statistics/cache-control", headers=self._auth_header(reader_token))
        self.assertEqual(reader_statistics_blocked.status_code, 403, reader_statistics_blocked.get_json())

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

        grant_statistics = self.client.put(
            f"/api/admin/users/{reader_id}",
            headers=self._auth_header(admin_token),
            json={"can_view_statistics": True},
        )
        self.assertEqual(grant_statistics.status_code, 200, grant_statistics.get_json())

        reader_statistics_allowed = self.client.get("/api/statistics/cache-control", headers=self._auth_header(reader_token))
        self.assertNotEqual(reader_statistics_allowed.status_code, 403, reader_statistics_allowed.get_json())

    def test_admin_can_delete_user_and_cannot_delete_self(self):
        admin_auth = self._bootstrap_admin_and_login()
        admin_token = str(admin_auth["token"])
        admin_user = (admin_auth.get("user") or {})
        admin_id = int(admin_user.get("id") or 0)
        self.assertGreater(admin_id, 0)

        create_reader = self.client.post(
            "/api/admin/users",
            headers=self._auth_header(admin_token),
            json={
                "username": "reader_delete",
                "password": "ReaderPassword123!",
                "password_confirm": "ReaderPassword123!",
                "is_admin": False,
                "can_download": False,
                "can_view_statistics": False,
                "is_active": True,
            },
        )
        self.assertEqual(create_reader.status_code, 200, create_reader.get_json())
        reader_user = (create_reader.get_json() or {}).get("user") or {}
        reader_id = int(reader_user.get("id") or 0)
        self.assertGreater(reader_id, 0)

        delete_reader = self.client.delete(
            f"/api/admin/users/{reader_id}",
            headers=self._auth_header(admin_token),
        )
        self.assertEqual(delete_reader.status_code, 200, delete_reader.get_json())
        delete_payload = delete_reader.get_json() or {}
        self.assertTrue(delete_payload.get("ok"))
        self.assertEqual(int(delete_payload.get("deleted_user_id") or 0), reader_id)

        users_after_delete = self.client.get("/api/admin/users", headers=self._auth_header(admin_token))
        self.assertEqual(users_after_delete.status_code, 200, users_after_delete.get_json())
        users_rows = (users_after_delete.get_json() or {}).get("users") or []
        self.assertFalse(any(int((u or {}).get("id") or 0) == reader_id for u in users_rows))

        deleted_login = self.client.post(
            "/api/auth/login",
            json={"username": "reader_delete", "password": "ReaderPassword123!"},
        )
        self.assertEqual(deleted_login.status_code, 401, deleted_login.get_json())

        delete_self = self.client.delete(
            f"/api/admin/users/{admin_id}",
            headers=self._auth_header(admin_token),
        )
        self.assertEqual(delete_self.status_code, 400, delete_self.get_json())

    def test_login_remember_me_extends_session_ttl(self):
        self._bootstrap_admin_and_login()

        short_login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "AdminPassword123!", "remember_me": False},
        )
        self.assertEqual(short_login.status_code, 200, short_login.get_json())
        short_payload = short_login.get_json() or {}
        self.assertTrue(short_payload.get("ok"))
        short_ttl = int(short_payload.get("expires_at") or 0) - pmda._auth_now_ts()
        self.assertLessEqual(short_ttl, int(pmda.AUTH_SESSION_TTL_SEC) + 10)
        self.assertGreaterEqual(short_ttl, int(pmda.AUTH_SESSION_TTL_SEC) - 10)

        long_login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "AdminPassword123!", "remember_me": True},
        )
        self.assertEqual(long_login.status_code, 200, long_login.get_json())
        long_payload = long_login.get_json() or {}
        self.assertTrue(long_payload.get("ok"))
        long_ttl = int(long_payload.get("expires_at") or 0) - pmda._auth_now_ts()
        self.assertLessEqual(long_ttl, int(pmda.AUTH_SESSION_REMEMBER_TTL_SEC) + 10)
        self.assertGreaterEqual(long_ttl, int(pmda.AUTH_SESSION_REMEMBER_TTL_SEC) - 10)
        self.assertGreater(long_ttl, short_ttl)

    def test_login_sets_session_cookie_and_cookie_auth_get_only(self):
        self._bootstrap_admin_and_login()

        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "AdminPassword123!", "remember_me": False},
        )
        self.assertEqual(login.status_code, 200, login.get_json())
        set_cookie = login.headers.get("Set-Cookie") or ""
        self.assertIn(f"{pmda.AUTH_SESSION_COOKIE_NAME}=", set_cookie)
        self.assertIn("HttpOnly", set_cookie)
        self.assertIn("SameSite=Lax", set_cookie)

        # Cookie auth must work for safe GET requests (e.g. media endpoints).
        me_with_cookie = self.client.get("/api/auth/me")
        self.assertEqual(me_with_cookie.status_code, 200, me_with_cookie.get_json())

        # Cookie auth must NOT unlock state-changing endpoints (header token required).
        create_without_header = self.client.post(
            "/api/admin/users",
            json={
                "username": "should_fail",
                "password": "ReaderPassword123!",
                "password_confirm": "ReaderPassword123!",
                "is_admin": False,
                "can_download": False,
                "can_view_statistics": False,
                "is_active": True,
            },
        )
        self.assertEqual(create_without_header.status_code, 401, create_without_header.get_json())

    def test_logout_clears_session_cookie(self):
        self._bootstrap_admin_and_login()

        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "AdminPassword123!"},
        )
        self.assertEqual(login.status_code, 200, login.get_json())
        token = str((login.get_json() or {}).get("token") or "")
        self.assertTrue(token)

        logout = self.client.post("/api/auth/logout", headers=self._auth_header(token))
        self.assertEqual(logout.status_code, 200, logout.get_json())
        set_cookie = logout.headers.get("Set-Cookie") or ""
        self.assertIn(f"{pmda.AUTH_SESSION_COOKIE_NAME}=", set_cookie)
        self.assertIn("Max-Age=0", set_cookie)

    def test_non_admin_config_does_not_expose_secrets(self):
        admin_auth = self._bootstrap_admin_and_login()
        admin_token = str(admin_auth["token"])

        save_cfg = self.client.put(
            "/api/config",
            headers=self._auth_header(admin_token),
            json={
                "OPENAI_API_KEY": "sk-test-secret",
                "PLEX_TOKEN": "plex-secret",
                "DISCOGS_USER_TOKEN": "discogs-secret",
            },
        )
        self.assertEqual(save_cfg.status_code, 200, save_cfg.get_json())

        create_reader = self.client.post(
            "/api/admin/users",
            headers=self._auth_header(admin_token),
            json={
                "username": "reader2",
                "password": "ReaderPassword123!",
                "password_confirm": "ReaderPassword123!",
                "is_admin": False,
                "can_download": False,
                "can_view_statistics": False,
                "is_active": True,
            },
        )
        self.assertEqual(create_reader.status_code, 200, create_reader.get_json())

        reader_login = self.client.post(
            "/api/auth/login",
            json={"username": "reader2", "password": "ReaderPassword123!"},
        )
        self.assertEqual(reader_login.status_code, 200, reader_login.get_json())
        reader_token = str((reader_login.get_json() or {}).get("token") or "")
        self.assertTrue(reader_token)

        reader_cfg = self.client.get("/api/config", headers=self._auth_header(reader_token))
        self.assertEqual(reader_cfg.status_code, 200, reader_cfg.get_json())
        payload = reader_cfg.get_json() or {}

        self.assertNotIn("OPENAI_API_KEY", payload)
        self.assertNotIn("PLEX_TOKEN", payload)
        self.assertNotIn("DISCOGS_USER_TOKEN", payload)
        self.assertNotIn("LASTFM_API_SECRET", payload)
        self.assertNotIn("NAVIDROME_PASSWORD", payload)


if __name__ == "__main__":
    unittest.main()
