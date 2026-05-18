import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

import pmda


class McpAccessTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-mcp-")
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
        self.admin_token = self._bootstrap_admin_and_login()

    def tearDown(self):
        pmda._AUTH_RATE_LIMIT_BUCKETS.clear()
        for key, value in self._orig.items():
            setattr(pmda, key, value)
        self._tmp.cleanup()

    @staticmethod
    def _auth_header(token: str) -> dict:
        return {"Authorization": f"Bearer {token}"}

    def _bootstrap_admin_and_login(self) -> str:
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
        return str((login.get_json() or {}).get("token") or "")

    def _set_mcp_enabled(self, enabled: bool) -> None:
        resp = self.client.put(
            "/api/config",
            headers=self._auth_header(self.admin_token),
            json={"MCP_ENABLED": enabled},
        )
        self.assertEqual(resp.status_code, 200, resp.get_json())

    def _rotate_mcp_token(self) -> str:
        resp = self.client.post(
            "/api/admin/mcp/token/rotate",
            headers=self._auth_header(self.admin_token),
            json={},
        )
        self.assertEqual(resp.status_code, 200, resp.get_json())
        token = str((resp.get_json() or {}).get("token") or "")
        self.assertTrue(token.startswith(pmda.MCP_TOKEN_PREFIX))
        return token

    def _mcp_tool(self, token: str, tool: str, args: dict | None = None):
        return self.client.post(
            "/api/mcp/tool",
            headers=self._auth_header(token),
            json={"tool": tool, "args": args or {}},
        )

    def test_mcp_disabled_by_default_blocks_even_valid_token(self):
        config = self.client.get("/api/config", headers=self._auth_header(self.admin_token))
        self.assertEqual(config.status_code, 200, config.get_json())
        self.assertFalse(bool((config.get_json() or {}).get("MCP_ENABLED")))

        token = self._rotate_mcp_token()
        resp = self._mcp_tool(token, "pmda.status")
        self.assertEqual(resp.status_code, 403, resp.get_json())
        self.assertEqual((resp.get_json() or {}).get("code"), "mcp_disabled")

        con = sqlite3.connect(str(pmda.SETTINGS_DB_FILE), timeout=5)
        row = con.execute(
            "SELECT tool, status, message FROM mcp_audit_log ORDER BY audit_id DESC LIMIT 1"
        ).fetchone()
        con.close()
        self.assertEqual(row, ("pmda.status", "denied", "mcp_disabled"))

    def test_enabled_valid_token_allows_scoped_read_and_cannot_auth_normal_routes(self):
        self._set_mcp_enabled(True)
        token = self._rotate_mcp_token()

        resp = self._mcp_tool(token, "pmda.status")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        payload = resp.get_json() or {}
        self.assertTrue(payload.get("ok"))
        self.assertEqual(payload.get("tool"), "pmda.status")

        jobs = self._mcp_tool(token, "pmda.jobs.status")
        self.assertEqual(jobs.status_code, 200, jobs.get_json())
        jobs_result = (jobs.get_json() or {}).get("result") or {}
        self.assertIn("scan", jobs_result.get("jobs") or {})
        self.assertIn("library_index", jobs_result.get("jobs") or {})
        self.assertIn("files_mode_opens_plex_db", jobs_result.get("notes") or {})

        jobs_route = self.client.get("/api/jobs/status", headers=self._auth_header(self.admin_token))
        self.assertEqual(jobs_route.status_code, 200, jobs_route.get_json())
        self.assertIn("materialization", (jobs_route.get_json() or {}).get("jobs") or {})

        normal_route = self.client.get("/api/config", headers=self._auth_header(token))
        self.assertEqual(normal_route.status_code, 401, normal_route.get_json())

        status = self.client.get("/api/admin/mcp/status", headers=self._auth_header(self.admin_token))
        self.assertEqual(status.status_code, 200, status.get_json())
        status_payload = status.get_json() or {}
        self.assertTrue(status_payload.get("enabled"))
        self.assertTrue(status_payload.get("last_access_at"))
        self.assertGreaterEqual(len(status_payload.get("audit") or []), 1)

    def test_read_tools_expose_scan_results_and_cache_stats(self):
        self._set_mcp_enabled(True)
        token = self._rotate_mcp_token()
        now = time.time()

        con = sqlite3.connect(str(pmda.STATE_DB_FILE), timeout=5)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO scan_history(start_time, end_time, duration_seconds, scan_type, albums_scanned, artists_processed, artists_total, status)
            VALUES(?, ?, 10, 'full', 3, 2, 2, 'completed')
            """,
            (now - 10, now),
        )
        scan_id = int(cur.lastrowid)
        trace_rows = [
            (scan_id, "Artist A", 1, "Strict Album", "/music/a", "musicbrainz", 1, "musicbrainz", 1, 0, 0, 0, 0, 10, 10, "", "none", 0, 0, now),
            (scan_id, "Artist B", 2, "Fallback Album", "/music/b", "lastfm", 0, "", 0, 0, 0, 1, 0, 8, 8, '["genre"]', "candidate", 1, 1, now),
            (scan_id, "Artist C", 3, "Miss Album", "/music/c", "", 0, "", 0, 1, 0, 0, 1, 12, 10, '["cover"]', "none", 0, 0, now),
        ]
        cur.executemany(
            """
            INSERT INTO scan_pipeline_trace(
                scan_id, artist, album_id, album_title, folder, metadata_source,
                strict_match_verified, strict_match_provider, has_musicbrainz, has_discogs, has_lastfm,
                has_cover, is_broken, expected_track_count, actual_track_count, missing_required_tags,
                dupe_role, dupe_needs_ai, manual_review, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            trace_rows,
        )
        cur.execute(
            """
            INSERT INTO scan_moves(scan_id, artist, album_id, original_path, moved_to_path, size_mb, moved_at, move_reason)
            VALUES(?, 'Artist C', 3, '/music/c', '/dupes/c', 512, ?, 'incomplete')
            """,
            (scan_id, now),
        )
        cur.execute(
            """
            INSERT INTO duplicates_best(
                artist, album_id, title_raw, folder, rationale, size_mb, track_count,
                evidence_json, dupe_signal, no_move, manual_review, same_folder
            )
            VALUES('Artist B', 2, 'Fallback Album', '/music/b', 'Needs review', 256, 8, '{}', 'same-title', 1, 1, 0)
            """
        )
        cur.execute(
            """
            INSERT INTO duplicates_loser(artist, album_id, loser_album_id, folder, fmt_text, size_mb)
            VALUES('Artist B', 2, 22, '/music/b-mp3', 'MP3', 128)
            """
        )
        cur.execute(
            """
            INSERT INTO broken_albums(
                artist, album_id, album_title, folder_path, expected_track_count,
                actual_track_count, missing_indices, review_status, classification,
                quarantine_eligible
            )
            VALUES('Artist C', 3, 'Miss Album', '/music/c', 12, 10, '[11,12]', 'pending', 'missing_tracks', 1)
            """
        )
        con.commit()
        con.close()

        cache = sqlite3.connect(str(pmda.CACHE_DB_FILE), timeout=5)
        cache.execute(
            """
            INSERT INTO musicbrainz_album_lookup(artist_norm, album_norm, mbid, info_json, created_at)
            VALUES('artist a', 'strict album', 'mbid-1', '{}', ?)
            """,
            (int(now),),
        )
        cache.execute(
            """
            INSERT INTO provider_album_lookup(provider, artist_norm, album_norm, status, payload_json, created_at, expires_at)
            VALUES('lastfm', 'artist b', 'fallback album', 'found', '{}', ?, ?)
            """,
            (int(now), int(now) + 3600),
        )
        cache.commit()
        cache.close()

        with pmda.lock:
            pmda.state["scan_id"] = scan_id
            pmda.state["scanning"] = True
            pmda.state["scan_start_time"] = now - 60
            pmda.state["scan_artists_processed"] = 2
            pmda.state["scan_artists_total"] = 2
            pmda.state["scan_processed_albums_count"] = 3
            pmda.state["scan_total_albums"] = 3
            pmda.state["scan_mb_done_count"] = 3
            pmda.state["scan_step_progress"] = 3
            pmda.state["scan_step_total"] = 3
            pmda.state["scan_provider_matches"] = {"musicbrainz": 1, "lastfm": 1}

        analytics = self._mcp_tool(token, "pmda.scan.analytics", {"scan_id": scan_id})
        self.assertEqual(analytics.status_code, 200, analytics.get_json())
        analytics_result = (analytics.get_json() or {}).get("result") or {}
        self.assertEqual(analytics_result.get("scan_id"), scan_id)
        trace_summary = ((analytics_result.get("matches") or {}).get("trace") or {})
        self.assertEqual(trace_summary.get("total"), 3)
        self.assertEqual(trace_summary.get("matched_percent"), 66.667)
        self.assertEqual((trace_summary.get("artwork") or {}).get("album_rows_with_cover"), 1)
        self.assertEqual((trace_summary.get("quality") or {}).get("missing_tags_rows"), 2)
        self.assertEqual((trace_summary.get("reviews") or {}).get("manual_review_rows"), 1)
        self.assertEqual((trace_summary.get("reviews") or {}).get("artists_with_review_or_issue"), 2)
        review_rollup = analytics_result.get("reviews") or {}
        self.assertEqual(((review_rollup.get("duplicates") or {}).get("loser_size_mb")), 128)
        self.assertEqual(((review_rollup.get("incompletes") or {}).get("albums")), 1)
        self.assertIn("available", analytics_result.get("enrichment") or {})

        results = self._mcp_tool(token, "pmda.scan.results", {"scan_id": scan_id, "limit": 10})
        self.assertEqual(results.status_code, 200, results.get_json())
        results_payload = (results.get_json() or {}).get("result") or {}
        self.assertEqual(((results_payload.get("trace") or {}).get("summary") or {}).get("unmatched_rows"), 1)
        self.assertEqual(((results_payload.get("moves") or {}).get("summary") or {}).get("size_mb"), 512)
        self.assertEqual(((results_payload.get("review_stats") or {}).get("duplicates") or {}).get("losers"), 1)

        review_stats = self._mcp_tool(token, "pmda.review.stats", {"scan_id": scan_id})
        self.assertEqual(review_stats.status_code, 200, review_stats.get_json())
        self.assertEqual((((review_stats.get_json() or {}).get("result") or {}).get("scan_trace") or {}).get("incomplete_or_broken"), 1)

        enrichment_stats = self._mcp_tool(token, "pmda.enrichment.stats")
        self.assertEqual(enrichment_stats.status_code, 200, enrichment_stats.get_json())
        self.assertIn("available", (enrichment_stats.get_json() or {}).get("result") or {})

        cache_stats = self._mcp_tool(token, "pmda.cache.stats")
        self.assertEqual(cache_stats.status_code, 200, cache_stats.get_json())
        cache_payload = (cache_stats.get_json() or {}).get("result") or {}
        self.assertEqual((cache_payload.get("musicbrainz_album_lookup") or {}).get("found"), 1)
        self.assertEqual((cache_payload.get("provider_album_lookup") or {}).get("found"), 1)

        provider_cache = self._mcp_tool(token, "pmda.providers.cache", {"provider": "lastfm"})
        self.assertEqual(provider_cache.status_code, 200, provider_cache.get_json())
        self.assertEqual(len(((provider_cache.get_json() or {}).get("result") or {}).get("items") or []), 1)

    def test_revoked_and_expired_tokens_are_refused(self):
        self._set_mcp_enabled(True)
        token = self._rotate_mcp_token()

        revoke = self.client.post("/api/admin/mcp/token/revoke", headers=self._auth_header(self.admin_token))
        self.assertEqual(revoke.status_code, 200, revoke.get_json())
        revoked = self._mcp_tool(token, "pmda.status")
        self.assertEqual(revoked.status_code, 401, revoked.get_json())
        self.assertEqual((revoked.get_json() or {}).get("code"), "invalid_mcp_token")

        token = self._rotate_mcp_token()
        con = sqlite3.connect(str(pmda.SETTINGS_DB_FILE), timeout=5)
        con.execute("UPDATE mcp_service_tokens SET expires_at = 1 WHERE active = 1")
        con.commit()
        con.close()
        expired = self._mcp_tool(token, "pmda.status")
        self.assertEqual(expired.status_code, 401, expired.get_json())
        self.assertEqual((expired.get_json() or {}).get("code"), "expired_mcp_token")

    def test_review_proposal_is_created_without_move_execution(self):
        self._set_mcp_enabled(True)
        token = self._rotate_mcp_token()

        resp = self._mcp_tool(
            token,
            "pmda.review.propose",
            {
                "kind": "duplicate",
                "scan_id": 123,
                "target_key": "artist:album",
                "title": "Move lower-quality duplicate loser",
                "recommendation": "Keep FLAC, move MP3 loser after human validation.",
                "confidence": 0.92,
                "evidence": {"winner": "flac", "loser": "mp3"},
                "proposed_actions": [{"action": "move_duplicate_loser", "path": "/music/lower"}],
            },
        )
        self.assertEqual(resp.status_code, 200, resp.get_json())
        result = ((resp.get_json() or {}).get("result") or {})
        proposal_id = str(result.get("proposal_id") or "")
        self.assertTrue(proposal_id.startswith("mcp_prop_"))
        self.assertEqual(result.get("status"), "pending")

        con = sqlite3.connect(str(pmda.STATE_DB_FILE), timeout=5)
        proposal_count = con.execute("SELECT COUNT(*) FROM mcp_review_proposals WHERE proposal_id = ?", (proposal_id,)).fetchone()[0]
        move_count = con.execute("SELECT COUNT(*) FROM scan_moves").fetchone()[0]
        con.close()
        self.assertEqual(proposal_count, 1)
        self.assertEqual(move_count, 0)

    def test_destructive_routes_remain_refused_with_mcp_token(self):
        self._set_mcp_enabled(True)
        token = self._rotate_mcp_token()
        resp = self.client.delete("/api/scan-history", headers=self._auth_header(token))
        self.assertEqual(resp.status_code, 401, resp.get_json())


if __name__ == "__main__":
    unittest.main()
