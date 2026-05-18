import tempfile
import sqlite3
import time
import unittest
from pathlib import Path
from unittest import mock

import pmda


class AuthRbacIntegrationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-auth-rbac-")
        tmp_path = Path(self._tmp.name)
        self._tmp_path = tmp_path

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

    def _insert_published_album(self, album_dir: Path | None = None):
        album_dir = album_dir or (self._tmp_path / "Music_matched" / "Artist" / "Album")
        album_dir.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO files_library_published_albums (
                folder_path, artist_name, artist_norm, album_title, title_norm,
                year, genre, label, tags_json, format, is_lossless,
                has_cover, has_artist_image, mb_identified, strict_match_verified,
                strict_match_provider, track_count, primary_metadata_source,
                primary_tags_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(album_dir),
                "Artist",
                "artist",
                "Album",
                "album",
                1999,
                "Ambient",
                "Warp",
                '["Ambient"]',
                "FLAC",
                1,
                1,
                1,
                1,
                1,
                "discogs",
                9,
                "discogs",
                "{}",
                float(time.time()),
            ),
        )
        con.commit()
        con.close()
        return album_dir

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

    def test_new_install_pipeline_defaults_are_safe(self):
        auth_payload = self._bootstrap_admin_and_login()
        token = str(auth_payload["token"])

        resp = self.client.get("/api/config", headers=self._auth_header(token))
        self.assertEqual(resp.status_code, 200, resp.get_json())
        payload = resp.get_json() or {}

        self.assertTrue(bool(payload.get("PIPELINE_ENABLE_DEDUPE")))
        self.assertFalse(bool(payload.get("AUTO_MOVE_DUPES")))
        self.assertFalse(bool(payload.get("PIPELINE_ENABLE_INCOMPLETE_MOVE")))

    def test_authenticated_library_browse_uses_published_snapshot_during_rebuild(self):
        auth_payload = self._bootstrap_admin_and_login()
        token = str(auth_payload["token"])
        album_dir = self._insert_published_album()

        snapshot = {
            "underbuilt": False,
            "published_albums": 1,
            "pg_albums": 25000,
            "pg_artists": 12000,
            "index_state": {"running": True, "phase": "parsing"},
        }
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
             mock.patch.object(pmda, "_files_cache_set_json", return_value=None), \
             mock.patch.object(pmda, "_ensure_files_index_ready", side_effect=AssertionError("live index should not be touched during rebuild")), \
             mock.patch.object(pmda, "_files_index_maybe_enqueue_published_catchup", return_value=snapshot), \
             mock.patch.object(pmda, "_files_library_resolve_artist_ids_by_norms", return_value={"artist": 321}), \
             mock.patch.object(pmda, "_files_library_resolve_album_ids_by_folder_paths", return_value={str(album_dir): 654}):
            started = time.perf_counter()
            albums_resp = self.client.get(
                "/api/library/albums?sort=recent&limit=96&offset=0&include_unmatched=1&scope=library",
                headers=self._auth_header(token),
            )
            artists_resp = self.client.get(
                "/api/library/artists?sort=recent&limit=96&offset=0&include_unmatched=1&scope=library",
                headers=self._auth_header(token),
            )
            elapsed = time.perf_counter() - started

        self.assertLess(elapsed, 1.0)
        self.assertEqual(albums_resp.status_code, 200, albums_resp.get_json())
        self.assertEqual(artists_resp.status_code, 200, artists_resp.get_json())
        albums_payload = albums_resp.get_json() or {}
        artists_payload = artists_resp.get_json() or {}
        self.assertEqual(albums_payload.get("fallback_source"), "published")
        self.assertEqual(albums_payload.get("browse_source"), "published")
        self.assertEqual(artists_payload.get("fallback_source"), "published")
        self.assertEqual(artists_payload.get("browse_source"), "published")
        self.assertEqual(len(albums_payload.get("albums") or []), 1)
        self.assertEqual(len(artists_payload.get("artists") or []), 1)

    def test_authenticated_artists_browse_defaults_to_published_snapshot(self):
        auth_payload = self._bootstrap_admin_and_login()
        token = str(auth_payload["token"])
        self._insert_published_album()

        snapshot = {
            "underbuilt": False,
            "api_lightweight": True,
            "published_albums": 1,
            "published_artists": 1,
            "pg_albums": 25000,
            "pg_artists": 12000,
            "index_state": {"running": False, "phase": "ready"},
        }
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
             mock.patch.object(pmda, "_files_cache_set_json", return_value=None), \
             mock.patch.object(pmda, "_ensure_files_index_ready", side_effect=AssertionError("artists browse should use published snapshot by default")), \
             mock.patch.object(pmda, "_files_index_maybe_enqueue_published_catchup", return_value=snapshot), \
             mock.patch.object(pmda, "_files_library_resolve_artist_ids_by_norms", return_value={"artist": 321}):
            started = time.perf_counter()
            artists_resp = self.client.get(
                "/api/library/artists?sort=recent&limit=96&offset=0&include_unmatched=1&scope=library",
                headers=self._auth_header(token),
            )
            elapsed = time.perf_counter() - started

        self.assertLess(elapsed, 1.0)
        self.assertEqual(artists_resp.status_code, 200, artists_resp.get_json())
        payload = artists_resp.get_json() or {}
        self.assertEqual(payload.get("fallback_source"), "published")
        self.assertEqual(payload.get("browse_source"), "published")
        self.assertEqual(len(payload.get("artists") or []), 1)

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

    def test_trash_release_curation_endpoints_are_admin_only(self):
        admin_auth = self._bootstrap_admin_and_login()
        admin_token = str(admin_auth["token"])

        create_reader = self.client.post(
            "/api/admin/users",
            headers=self._auth_header(admin_token),
            json={
                "username": "reader_curation",
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
            json={"username": "reader_curation", "password": "ReaderPassword123!"},
        )
        self.assertEqual(reader_login.status_code, 200, reader_login.get_json())
        reader_token = str((reader_login.get_json() or {}).get("token") or "")
        self.assertTrue(reader_token)

        reader_candidates = self.client.get(
            "/api/tools/trash-releases",
            headers=self._auth_header(reader_token),
        )
        self.assertEqual(reader_candidates.status_code, 403, reader_candidates.get_json())

        reader_action = self.client.post(
            "/api/tools/trash-releases/action",
            headers=self._auth_header(reader_token),
            json={"album_id": 1, "action": "move_to_dupes"},
        )
        self.assertEqual(reader_action.status_code, 403, reader_action.get_json())

    def test_profile_update_persists_user_concert_preferences(self):
        auth_payload = self._bootstrap_admin_and_login()
        token = str(auth_payload["token"])

        update = self.client.put(
            "/api/auth/profile",
            headers=self._auth_header(token),
            json={
                "concerts_filter_enabled": True,
                "concerts_home_lat": "50.8503",
                "concerts_home_lon": "4.3517",
                "concerts_radius_km": "180",
            },
        )
        self.assertEqual(update.status_code, 200, update.get_json())
        update_user = (update.get_json() or {}).get("user") or {}
        self.assertTrue(update_user.get("concerts_filter_enabled"))
        self.assertEqual(update_user.get("concerts_home_lat"), "50.8503")
        self.assertEqual(update_user.get("concerts_home_lon"), "4.3517")
        self.assertEqual(update_user.get("concerts_radius_km"), "180")

        me = self.client.get("/api/auth/me", headers=self._auth_header(token))
        self.assertEqual(me.status_code, 200, me.get_json())
        persisted_user = (me.get_json() or {}).get("user") or {}
        self.assertTrue(persisted_user.get("concerts_filter_enabled"))
        self.assertEqual(persisted_user.get("concerts_home_lat"), "50.8503")
        self.assertEqual(persisted_user.get("concerts_home_lon"), "4.3517")
        self.assertEqual(persisted_user.get("concerts_radius_km"), "180")

    def test_admin_ops_endpoints_are_admin_only(self):
        admin_auth = self._bootstrap_admin_and_login()
        admin_token = str(admin_auth["token"])
        self.assertTrue(admin_token)

        create_reader = self.client.post(
            "/api/admin/users",
            headers=self._auth_header(admin_token),
            json={
                "username": "reader_ops",
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
            json={"username": "reader_ops", "password": "ReaderPassword123!"},
        )
        self.assertEqual(reader_login.status_code, 200, reader_login.get_json())
        reader_token = str((reader_login.get_json() or {}).get("token") or "")
        self.assertTrue(reader_token)

        snapshot_resp = self.client.get(
            "/api/admin/ops/snapshot",
            headers=self._auth_header(reader_token),
        )
        self.assertEqual(snapshot_resp.status_code, 403, snapshot_resp.get_json())

        backup_resp = self.client.post(
            "/api/admin/ops/backup",
            headers=self._auth_header(reader_token),
            json={"include_pg_dump": False},
        )
        self.assertEqual(backup_resp.status_code, 403, backup_resp.get_json())

        maintenance_resp = self.client.post(
            "/api/admin/maintenance/reset",
            headers=self._auth_header(reader_token),
            json={"actions": ["media_cache"], "restart": False},
        )
        self.assertEqual(maintenance_resp.status_code, 403, maintenance_resp.get_json())

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

    def test_query_auth_token_only_unlocks_safe_media_get_requests(self):
        self._bootstrap_admin_and_login()

        login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "AdminPassword123!", "remember_me": False},
        )
        self.assertEqual(login.status_code, 200, login.get_json())
        token = str((login.get_json() or {}).get("token") or "").strip()
        self.assertTrue(token)

        fresh_client = pmda.app.test_client()
        fresh_client.set_cookie(
            pmda.AUTH_SESSION_COOKIE_NAME,
            "stale-session-token",
            domain="localhost",
        )

        me_with_query = fresh_client.get(f"/api/auth/me?auth_token={token}")
        self.assertEqual(me_with_query.status_code, 401, me_with_query.get_json())

        library_with_query = fresh_client.get(f"/api/library/track/123/stream?auth_token={token}")
        self.assertNotEqual(library_with_query.status_code, 401, library_with_query.get_data(as_text=True))

        create_with_query = fresh_client.post(
            f"/api/admin/users?auth_token={token}",
            json={
                "username": "should_still_fail",
                "password": "ReaderPassword123!",
                "password_confirm": "ReaderPassword123!",
                "is_admin": False,
                "can_download": False,
                "can_view_statistics": False,
                "is_active": True,
            },
        )
        self.assertEqual(create_with_query.status_code, 401, create_with_query.get_json())

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

    def test_user_can_change_own_password_and_old_sessions_are_revoked(self):
        auth_payload = self._bootstrap_admin_and_login()
        token = str(auth_payload.get("token") or "")
        self.assertTrue(token)

        change = self.client.put(
            "/api/auth/password",
            headers=self._auth_header(token),
            json={
                "current_password": "AdminPassword123!",
                "new_password": "AdminPassword456!",
                "new_password_confirm": "AdminPassword456!",
            },
        )
        self.assertEqual(change.status_code, 200, change.get_json())
        payload = change.get_json() or {}
        self.assertTrue(payload.get("ok"))
        self.assertTrue(payload.get("reauth_required"))
        set_cookie = change.headers.get("Set-Cookie") or ""
        self.assertIn(f"{pmda.AUTH_SESSION_COOKIE_NAME}=", set_cookie)
        self.assertIn("Max-Age=0", set_cookie)

        me_after_change = self.client.get("/api/auth/me", headers=self._auth_header(token))
        self.assertEqual(me_after_change.status_code, 401, me_after_change.get_json())

        old_login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "AdminPassword123!"},
        )
        self.assertEqual(old_login.status_code, 401, old_login.get_json())

        new_login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "AdminPassword456!"},
        )
        self.assertEqual(new_login.status_code, 200, new_login.get_json())

    def test_password_change_rejects_wrong_current_password(self):
        auth_payload = self._bootstrap_admin_and_login()
        token = str(auth_payload.get("token") or "")
        self.assertTrue(token)

        change = self.client.put(
            "/api/auth/password",
            headers=self._auth_header(token),
            json={
                "current_password": "WrongPassword123!",
                "new_password": "AdminPassword456!",
                "new_password_confirm": "AdminPassword456!",
            },
        )
        self.assertEqual(change.status_code, 401, change.get_json())
        payload = change.get_json() or {}
        self.assertEqual(payload.get("error"), "Current password is incorrect")

        unchanged_login = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "AdminPassword123!"},
        )
        self.assertEqual(unchanged_login.status_code, 200, unchanged_login.get_json())

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
