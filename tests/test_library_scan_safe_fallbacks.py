import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import pmda


class LibraryScanSafeFallbackTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-library-fallbacks-")
        tmp_path = Path(self._tmp.name)
        self.export_root = tmp_path / "Music_matched"
        album_dir = self.export_root / "Artist" / "Album"
        album_dir.mkdir(parents=True, exist_ok=True)
        self.album_dir = album_dir
        self._orig = {
            "CONFIG_DIR": pmda.CONFIG_DIR,
            "STATE_DB_FILE": pmda.STATE_DB_FILE,
            "SETTINGS_DB_FILE": pmda.SETTINGS_DB_FILE,
            "CACHE_DB_FILE": pmda.CACHE_DB_FILE,
            "AUTH_DISABLE": pmda.AUTH_DISABLE,
            "EXPORT_ROOT": pmda.EXPORT_ROOT,
            "FILES_ROOTS": pmda.FILES_ROOTS,
            "PLEX_CONFIGURED": pmda.PLEX_CONFIGURED,
            "duplicates": dict(pmda.state.get("duplicates") or {}),
            "files_index": dict(pmda.state.get("files_index") or {}),
            "files_reco_embeddings": dict(pmda.state.get("files_reco_embeddings") or {}),
        }
        pmda.CONFIG_DIR = tmp_path
        pmda.STATE_DB_FILE = tmp_path / "state.db"
        pmda.SETTINGS_DB_FILE = tmp_path / "settings.db"
        pmda.CACHE_DB_FILE = tmp_path / "cache.db"
        pmda.AUTH_DISABLE = True
        pmda.EXPORT_ROOT = str(self.export_root)
        pmda.FILES_ROOTS = [str(tmp_path / "Music_dump")]
        pmda.init_state_db()
        pmda.init_settings_db()
        pmda.init_cache_db()
        with pmda.lock:
            pmda.state["scanning"] = True
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_post_processing"] = False
            pmda.state["duplicates"] = {}
        self.client = pmda.app.test_client()

    def tearDown(self):
        with pmda.lock:
            pmda.state["scanning"] = False
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_post_processing"] = False
            pmda.state["duplicates"] = dict(self._orig.get("duplicates") or {})
            pmda.state["files_index"] = dict(self._orig.get("files_index") or {})
            pmda.state["files_reco_embeddings"] = dict(self._orig.get("files_reco_embeddings") or {})
        for key, value in self._orig.items():
            if key in {"duplicates", "files_index", "files_reco_embeddings"}:
                continue
            setattr(pmda, key, value)
        self._tmp.cleanup()

    def _insert_published_album(self):
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
                str(self.album_dir),
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

    def _insert_scan_history_with_pipeline_trace(self) -> int:
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        cur = con.cursor()
        now = float(time.time())
        cur.execute(
            """
            INSERT INTO scan_history (start_time, end_time, status, entry_type, summary_json)
            VALUES (?, ?, 'completed', 'scan', NULL)
            """,
            (now - 5.0, now),
        )
        scan_id = int(cur.lastrowid or 0)
        rows = [
            (scan_id, "Artist MB", 1, "Album MB", "/tmp/mb", 1, 1, "musicbrainz"),
            (scan_id, "Artist Discogs", 2, "Album Discogs", "/tmp/discogs", 1, 1, "discogs"),
            (scan_id, "Artist Bandcamp", 3, "Album Bandcamp", "/tmp/bandcamp", 0, 1, "bandcamp"),
            (scan_id, "Artist Soft", 4, "Album Soft", "/tmp/soft", 1, 0, ""),
        ]
        cur.executemany(
            """
            INSERT INTO scan_pipeline_trace (
                scan_id, artist, album_id, album_title, folder,
                has_musicbrainz, strict_match_verified, strict_match_provider, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [(*row, now) for row in rows],
        )
        con.commit()
        con.close()
        return scan_id

    def _artists_cache_keys(self, browse_source: str = "live") -> tuple[str, str]:
        include_unmatched = True
        scope = "library"
        stable_cache_key = (
            f"library:artists::::0:recent:100:0:"
            f"{pmda._library_cache_scope_suffix(scope)}:{pmda._library_cache_unmatched_suffix(include_unmatched)}:{browse_source}"
        )
        cache_key = f"{stable_cache_key}:0:0:0:{browse_source}"
        return cache_key, stable_cache_key

    def _albums_cache_keys(self, browse_source: str = "live") -> tuple[str, str]:
        include_unmatched = True
        scope = "library"
        stable_cache_key = (
            f"library:albums:u0::::0:recent:80:0:"
            f"{pmda._library_cache_scope_suffix(scope)}:{pmda._library_cache_unmatched_suffix(include_unmatched)}:{browse_source}"
        )
        cache_key = f"{stable_cache_key}:0:0:0:{browse_source}"
        return cache_key, stable_cache_key

    def test_library_artists_uses_published_fallback_while_scan_busy(self):
        self._insert_published_album()
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
             mock.patch.object(pmda, "_files_cache_set_json", return_value=None), \
             mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
             mock.patch.object(pmda, "_files_index_maybe_enqueue_published_catchup", return_value={"underbuilt": True, "published_albums": 1}), \
             mock.patch.object(pmda, "_files_library_resolve_artist_ids_by_norms", return_value={"artist": 321}):
            resp = self.client.get("/api/library/artists?scope=library")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload.get("fallback_source"), "published")
        self.assertEqual(len(payload.get("artists") or []), 1)
        artist = (payload.get("artists") or [{}])[0]
        self.assertEqual(int(artist.get("artist_id") or 0), 321)
        self.assertEqual(artist.get("publication_state"), "published")
        self.assertEqual(artist.get("cover_state"), "ready")
        self.assertEqual(artist.get("artist_media_state"), "ready")
        self.assertEqual(artist.get("profile_state"), "enriching")

    def test_library_albums_uses_published_fallback_while_scan_busy(self):
        self._insert_published_album()
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
             mock.patch.object(pmda, "_files_cache_set_json", return_value=None), \
             mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
             mock.patch.object(pmda, "_files_index_maybe_enqueue_published_catchup", return_value={"underbuilt": True, "published_albums": 1}), \
             mock.patch.object(pmda, "_files_library_resolve_artist_ids_by_norms", return_value={"artist": 321}), \
             mock.patch.object(pmda, "_files_library_resolve_album_ids_by_folder_paths", return_value={str(self.album_dir): 654}):
            resp = self.client.get("/api/library/albums?scope=library")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload.get("fallback_source"), "published")
        self.assertEqual(len(payload.get("albums") or []), 1)
        album = (payload.get("albums") or [{}])[0]
        self.assertEqual(int(album.get("album_id") or 0), 654)
        self.assertEqual(int(album.get("artist_id") or 0), 321)
        self.assertEqual(album.get("publication_state"), "published")
        self.assertEqual(album.get("cover_state"), "ready")
        self.assertEqual(album.get("artist_media_state"), "ready")
        self.assertEqual(album.get("profile_state"), "enriching")

    def test_published_album_fallback_uses_live_cover_flag_when_snapshot_is_stale(self):
        self._insert_published_album()
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        con.execute("UPDATE files_library_published_albums SET has_cover = 0")
        con.commit()
        con.close()
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_library_resolve_artist_ids_by_norms", return_value={"artist": 321}), \
             mock.patch.object(pmda, "_files_library_resolve_album_ids_by_folder_paths", return_value={str(self.album_dir): 654}), \
             mock.patch.object(pmda, "_files_library_resolve_album_cover_flags_by_ids", return_value={654: True}):
            with pmda.app.test_request_context("/api/library/albums?scope=library"):
                payload = pmda._files_library_published_albums(
                    include_unmatched=True,
                    scope="library",
                    limit=10,
                    offset=0,
                )
        album = (payload.get("albums") or [{}])[0]
        self.assertEqual(album.get("cover_state"), "ready")
        self.assertIn("/api/library/files/album/654/cover", album.get("thumb") or "")

    def test_pipeline_snapshot_marks_stale_running_jobs(self):
        pmda._pipeline_job_update(
            "profile_backfill",
            status="running",
            phase="p0",
            current=1,
            total=49010,
            current_item="Zombie Artist",
            message="Visual assets: 1 / 49,010 artist tasks",
        )
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        old = time.time() - (pmda.PIPELINE_JOB_STALE_AFTER_SEC + 60)
        con.execute("UPDATE pipeline_jobs SET heartbeat_at = ? WHERE job_type = 'profile_backfill'", (old,))
        con.commit()
        con.close()

        snapshot = pmda._pipeline_job_snapshot()
        row = snapshot.get("profile_backfill") or {}
        self.assertEqual(row.get("status"), "stale")
        self.assertTrue(bool(row.get("stale")))
        self.assertGreater(int(row.get("seconds_since_heartbeat") or 0), pmda.PIPELINE_JOB_STALE_AFTER_SEC)

    def test_library_auto_browse_source_pins_to_published_snapshot_while_scan_runs(self):
        self._insert_published_album()
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
             mock.patch.object(pmda, "_files_cache_set_json", return_value=None), \
             mock.patch.object(pmda, "_ensure_files_index_ready", side_effect=AssertionError("live index should not be touched")), \
             mock.patch.object(pmda, "_files_index_maybe_enqueue_published_catchup", return_value={"underbuilt": False, "published_albums": 1, "pg_albums": 25000, "pg_artists": 12000}), \
             mock.patch.object(pmda, "_files_library_resolve_artist_ids_by_norms", return_value={"artist": 321}), \
             mock.patch.object(pmda, "_files_library_resolve_album_ids_by_folder_paths", return_value={str(self.album_dir): 654}):
            resp = self.client.get("/api/library/albums?scope=library")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload.get("fallback_source"), "published")
        self.assertEqual(payload.get("browse_source"), "published")
        self.assertEqual(len(payload.get("albums") or []), 1)

    def test_library_auto_browse_source_pins_to_published_snapshot_while_index_rebuild_runs(self):
        self._insert_published_album()
        with pmda.lock:
            pmda.state["scanning"] = False
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_post_processing"] = False
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
             mock.patch.object(pmda, "_files_cache_set_json", return_value=None), \
             mock.patch.object(pmda, "_ensure_files_index_ready", side_effect=AssertionError("live index should not be touched during rebuild")), \
             mock.patch.object(
                 pmda,
                 "_files_index_maybe_enqueue_published_catchup",
                 return_value={
                     "underbuilt": False,
                     "published_albums": 1,
                     "pg_albums": 25000,
                     "pg_artists": 12000,
                     "index_state": {"running": True, "phase": "parsing"},
                 },
             ), \
             mock.patch.object(pmda, "_files_library_resolve_artist_ids_by_norms", return_value={"artist": 321}), \
             mock.patch.object(pmda, "_files_library_resolve_album_ids_by_folder_paths", return_value={str(self.album_dir): 654}):
            resp = self.client.get("/api/library/albums?scope=library")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload.get("fallback_source"), "published")
        self.assertEqual(payload.get("browse_source"), "published")
        self.assertEqual(len(payload.get("albums") or []), 1)

    def test_library_album_auto_browse_uses_published_source_id_without_pg_resolution(self):
        self._insert_published_album()
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        con.execute("UPDATE files_library_published_albums SET source_id = 654")
        con.commit()
        con.close()
        with pmda.lock:
            pmda.state["scanning"] = False
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_post_processing"] = False
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
             mock.patch.object(pmda, "_files_cache_set_json", return_value=None), \
             mock.patch.object(pmda, "_ensure_files_index_ready", side_effect=AssertionError("live index should not be touched")), \
             mock.patch.object(pmda, "_files_index_maybe_enqueue_published_catchup", return_value={"underbuilt": False, "published_albums": 1, "pg_albums": 25000, "pg_artists": 12000}), \
             mock.patch.object(pmda, "_files_library_resolve_artist_ids_by_norms", return_value={"artist": 321}), \
             mock.patch.object(pmda, "_files_library_resolve_album_ids_by_folder_paths", side_effect=AssertionError("published source_id should avoid album-id PG resolution")), \
             mock.patch.object(pmda, "_files_library_resolve_album_cover_flags_by_ids", side_effect=AssertionError("published browse should not resolve live cover flags")):
            resp = self.client.get("/api/library/albums?scope=library")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload.get("fallback_source"), "published")
        self.assertEqual(payload.get("browse_source"), "published")
        album = (payload.get("albums") or [{}])[0]
        self.assertEqual(int(album.get("album_id") or 0), 654)
        self.assertIn("/api/library/files/album/654/cover", album.get("thumb") or "")

    def test_published_catchup_is_not_enqueued_while_scan_busy(self):
        snapshot = {"underbuilt": True, "published_albums": 12}
        with pmda.lock:
            pmda.state["scanning"] = True
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_post_processing"] = False
        with mock.patch.object(pmda, "_enqueue_files_index_published_catchup") as enqueue_mock:
            result = pmda._files_index_maybe_enqueue_published_catchup(
                include_unmatched=True,
                scope="library",
                reason="test_busy_scan",
                snapshot=snapshot,
            )
        self.assertIs(result, snapshot)
        enqueue_mock.assert_not_called()

    def test_broken_albums_uses_stored_track_snapshot_without_refresh_by_default(self):
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO broken_albums (
                artist, album_id, expected_track_count, actual_track_count,
                missing_indices, musicbrainz_release_group_id, detected_at,
                sent_to_lidarr, album_title, folder_path, metadata_source,
                local_tracks_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Artist",
                42,
                3,
                2,
                "[2]",
                "",
                time.time(),
                0,
                "Album",
                str(self.album_dir),
                "discogs",
                '[{"track_num": 1, "title": "One"}, {"track_num": 3, "title": "Three"}]',
            ),
        )
        con.commit()
        con.close()

        seen_force_flags: list[bool] = []

        def _fake_snapshot(**kwargs):
            seen_force_flags.append(bool(kwargs.get("force_rescan")))
            return Path(kwargs.get("folder_path") or self.album_dir), list(kwargs.get("existing_local_tracks") or []), True

        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_pg_connect", return_value=None), \
             mock.patch.object(pmda, "_broken_album_resolve_folder_snapshot", side_effect=_fake_snapshot):
            resp = self.client.get("/api/broken-albums")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(seen_force_flags, [False])
        payload = resp.get_json() or []
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["album_title"], "Album")

    def test_broken_albums_refresh_query_opt_in_rescans_local_folder(self):
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO broken_albums (
                artist, album_id, expected_track_count, actual_track_count,
                missing_indices, musicbrainz_release_group_id, detected_at,
                sent_to_lidarr, album_title, folder_path, metadata_source,
                local_tracks_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Artist",
                43,
                3,
                2,
                "[2]",
                "",
                time.time(),
                0,
                "Album 2",
                str(self.album_dir),
                "discogs",
                '[{"track_num": 1, "title": "One"}, {"track_num": 3, "title": "Three"}]',
            ),
        )
        con.commit()
        con.close()

        seen_force_flags: list[bool] = []

        def _fake_snapshot(**kwargs):
            seen_force_flags.append(bool(kwargs.get("force_rescan")))
            return Path(kwargs.get("folder_path") or self.album_dir), list(kwargs.get("existing_local_tracks") or []), True

        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_pg_connect", return_value=None), \
             mock.patch.object(pmda, "_broken_album_resolve_folder_snapshot", side_effect=_fake_snapshot):
            resp = self.client.get("/api/broken-albums?refresh=1")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(seen_force_flags, [True])

    def test_lidarr_legacy_artist_path_does_not_open_plex_in_files_mode(self):
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_lidarr_feature_enabled", return_value=True), \
             mock.patch.object(pmda, "plex_connect", side_effect=AssertionError("Plex DB must not be opened in files mode")):
            self.assertFalse(pmda.add_artist_to_lidarr(artist_id=1, artist_name="Artist", artist_mbid=None))

    def test_legacy_lidarr_routes_are_removed_even_with_stored_config(self):
        endpoints = [
            ("post", "/api/lidarr/add-album"),
            ("post", "/api/lidarr/add-incomplete-albums"),
            ("get", "/api/lidarr/add-incomplete-albums/progress"),
            ("post", "/api/lidarr/add-artist"),
            ("post", "/api/lidarr/test"),
            ("post", "/api/autobrr/create-filter"),
            ("post", "/api/autobrr/test"),
        ]
        for method, path in endpoints:
            with self.subTest(path=path):
                resp = getattr(self.client, method)(path, json={})
                self.assertEqual(resp.status_code, 410)

    def test_published_catchup_is_not_enqueued_from_browse_api(self):
        snapshot = {"underbuilt": True, "published_albums": 12}
        with pmda.lock:
            pmda.state["scanning"] = False
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_post_processing"] = False
        with mock.patch.object(pmda, "_enqueue_files_index_published_catchup") as enqueue_mock:
            result = pmda._files_index_maybe_enqueue_published_catchup(
                include_unmatched=True,
                scope="library",
                reason="api_library_albums_library",
                snapshot=snapshot,
            )
        self.assertIs(result, snapshot)
        enqueue_mock.assert_not_called()

    def test_plex_db_is_not_opened_in_files_mode_by_default(self):
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_ALLOW_PLEX_DB_IN_FILES_MODE", False):
            with self.assertRaisesRegex(RuntimeError, "Plex DB access is disabled"):
                pmda.plex_connect()

    def test_published_catchup_is_not_enqueued_while_index_rebuild_runs(self):
        snapshot = {"underbuilt": True, "published_albums": 12}
        with pmda.lock:
            pmda.state["scanning"] = False
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_post_processing"] = False
        lock_was_held = pmda.files_index_lock.locked()
        if not lock_was_held:
            pmda.files_index_lock.acquire()
        try:
            with mock.patch.object(pmda, "_enqueue_files_index_published_catchup") as enqueue_mock:
                result = pmda._files_index_maybe_enqueue_published_catchup(
                    include_unmatched=True,
                    scope="library",
                    reason="test_index_rebuild",
                    snapshot=snapshot,
                )
        finally:
            if not lock_was_held and pmda.files_index_lock.locked():
                pmda.files_index_lock.release()
        self.assertIs(result, snapshot)
        enqueue_mock.assert_not_called()

    def test_duplicates_endpoint_serves_scan_groups_in_files_mode_without_plex(self):
        best_dir = self.export_root / "Artist" / "Winner"
        loser_dir = self.export_root / "Artist" / "Loser"
        best_dir.mkdir(parents=True, exist_ok=True)
        loser_dir.mkdir(parents=True, exist_ok=True)
        (best_dir / "01 Winner.flac").write_bytes(b"flac")
        (loser_dir / "01 Loser.flac").write_bytes(b"flac")
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO duplicates_best (
                artist, album_id, title_raw, album_norm, folder, fmt_text,
                br, sr, bd, dur, discs, rationale, merge_list, ai_used,
                size_mb, track_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Artist",
                101,
                "Album",
                "album",
                str(best_dir),
                "FLAC",
                0,
                44100,
                16,
                0,
                1,
                "test",
                "[]",
                0,
                1,
                1,
            ),
        )
        cur.execute(
            """
            INSERT INTO duplicates_loser (
                artist, album_id, loser_album_id, folder, fmt_text,
                br, sr, bd, size_mb
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("Artist", 101, 102, str(loser_dir), "FLAC", 0, 44100, 16, 1),
        )
        con.commit()
        con.close()
        with pmda.lock:
            pmda.state["scanning"] = False
            pmda.state["duplicates"] = {}
        pmda.PLEX_CONFIGURED = False
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"):
            resp = self.client.get("/api/duplicates")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or []
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0].get("artist"), "Artist")
        self.assertEqual(int(payload[0].get("n") or 0), 2)

    def test_save_scan_to_db_preserves_global_open_duplicate_registry(self):
        old_best = self.export_root / "Old Artist" / "Old Winner"
        old_loser = self.export_root / "Old Artist" / "Old Loser"
        new_best = self.export_root / "New Artist" / "New Winner"
        new_loser = self.export_root / "New Artist" / "New Loser"
        for folder in (old_best, old_loser, new_best, new_loser):
            folder.mkdir(parents=True, exist_ok=True)
            (folder / "01 Track.flac").write_bytes(b"flac")

        con = sqlite3.connect(pmda.STATE_DB_FILE)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO duplicates_best (
                artist, album_id, title_raw, album_norm, folder, fmt_text,
                br, sr, bd, dur, discs, rationale, merge_list, ai_used,
                size_mb, track_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("Old Artist", 201, "Old Album", "old album", str(old_best), "FLAC", 0, 44100, 16, 0, 1, "old", "[]", 0, 1, 1),
        )
        cur.execute(
            """
            INSERT INTO duplicates_loser (
                artist, album_id, loser_album_id, folder, fmt_text,
                br, sr, bd, size_mb
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("Old Artist", 201, 202, str(old_loser), "FLAC", 0, 44100, 16, 1),
        )
        con.commit()
        con.close()

        new_group = {
            "album_id": 301,
            "best": {
                "album_id": 301,
                "title_raw": "New Album",
                "album_norm": "new album",
                "folder": new_best,
                "br": 0,
                "sr": 44100,
                "bd": 16,
                "dur": 0,
                "discs": 1,
                "tracks": [{"title": "Track"}],
                "meta": {},
            },
            "losers": [
                {
                    "album_id": 302,
                    "folder": new_loser,
                    "br": 0,
                    "sr": 44100,
                    "bd": 16,
                }
            ],
        }

        pmda.save_scan_to_db({"New Artist": [new_group]})
        loaded = pmda.load_scan_from_db()

        self.assertIn("Old Artist", loaded)
        self.assertIn("New Artist", loaded)
        self.assertEqual(len(loaded["Old Artist"]), 1)
        self.assertEqual(len(loaded["New Artist"]), 1)

    def test_duplicates_endpoint_recovers_groups_from_pipeline_trace(self):
        best_dir = self.export_root / "Trace Artist" / "Trace Winner"
        loser_dir = self.export_root / "Trace Artist" / "Trace Loser"
        for folder in (best_dir, loser_dir):
            folder.mkdir(parents=True, exist_ok=True)
            (folder / "01 Track.flac").write_bytes(b"flac")
        now = time.time()
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO scan_history(start_time, end_time, status, entry_type)
            VALUES (?, ?, 'completed', 'scan')
            """,
            (now - 10, now),
        )
        scan_id = int(cur.lastrowid)
        rows = [
            (scan_id, "Trace Artist", 401, "Trace Album", str(best_dir), "FLAC", "winner", 401, "Trace Album", 2, now),
            (scan_id, "Trace Artist", 402, "Trace Album", str(loser_dir), "MP3", "loser", 401, "Trace Album", 2, now),
        ]
        cur.executemany(
            """
            INSERT INTO scan_pipeline_trace(
                scan_id, artist, album_id, album_title, folder, fmt_text,
                dupe_role, winner_album_id, winner_title, dupe_peer_count, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        con.commit()
        con.close()
        with pmda.lock:
            pmda.state["duplicates"] = {}
            pmda.state["scanning"] = False
        pmda.PLEX_CONFIGURED = False
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"):
            resp = self.client.get("/api/duplicates")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or []
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0].get("artist"), "Trace Artist")
        self.assertEqual(int(payload[0].get("n") or 0), 2)

    def test_mcp_duplicate_groups_use_global_trace_registry(self):
        best_dir = self.export_root / "MCP Trace Artist" / "Winner"
        loser_dir = self.export_root / "MCP Trace Artist" / "Loser"
        for folder in (best_dir, loser_dir):
            folder.mkdir(parents=True, exist_ok=True)
            (folder / "01 Track.flac").write_bytes(b"flac")
        now = time.time()
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        cur = con.cursor()
        cur.execute(
            "INSERT INTO scan_history(start_time, end_time, status, entry_type) VALUES (?, ?, 'completed', 'scan')",
            (now - 10, now),
        )
        scan_id = int(cur.lastrowid)
        cur.executemany(
            """
            INSERT INTO scan_pipeline_trace(
                scan_id, artist, album_id, album_title, folder, fmt_text,
                dupe_role, winner_album_id, winner_title, dupe_peer_count, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (scan_id, "MCP Trace Artist", 501, "Trace Album", str(best_dir), "FLAC", "winner", 501, "Trace Album", 2, now),
                (scan_id, "MCP Trace Artist", 502, "Trace Album", str(loser_dir), "MP3", "loser", 501, "Trace Album", 2, now),
            ],
        )
        con.commit()
        con.close()
        with pmda.lock:
            pmda.state["duplicates"] = {}
        payload = pmda._mcp_duplicate_groups(limit=10)
        self.assertEqual(((payload.get("sources") or {}).get("merged") or {}).get("groups"), 1)
        self.assertEqual(len(payload.get("items") or []), 1)
        self.assertEqual((payload.get("items") or [])[0].get("artist"), "MCP Trace Artist")
        self.assertEqual((payload.get("items") or [])[0].get("loser_count"), 1)

    def test_mcp_incomplete_albums_include_global_diagnostics(self):
        now = time.time()
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        cur = con.cursor()
        cur.execute(
            "INSERT INTO scan_history(start_time, end_time, status, entry_type) VALUES (?, ?, 'completed', 'scan')",
            (now - 10, now),
        )
        scan_id = int(cur.lastrowid)
        cur.execute(
            """
            INSERT INTO incomplete_album_diagnostics(
                run_id, artist, album_id, title_raw, folder, classification,
                missing_in_plex, missing_on_disk, expected_track_count, actual_track_count, detected_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scan_id,
                "Incomplete Artist",
                701,
                "Short Album",
                str(self.export_root / "Incomplete Artist" / "Short Album"),
                "confirmed_incomplete",
                "[]",
                "[4, 5]",
                5,
                3,
                now,
            ),
        )
        con.commit()
        con.close()
        payload = pmda._mcp_incomplete_albums(limit=10)
        self.assertEqual(((payload.get("sources") or {}).get("incomplete_album_diagnostics")), 1)
        self.assertEqual(len(payload.get("items") or []), 1)
        self.assertEqual((payload.get("items") or [])[0].get("artist"), "Incomplete Artist")
        self.assertEqual((payload.get("items") or [])[0].get("source"), "incomplete_album_diagnostics")

    def test_library_artists_ignores_published_cache_once_index_is_usable_again(self):
        self._insert_published_album()
        cache_key, stable_cache_key = self._artists_cache_keys("live")
        pmda._files_cache_set_json(
            stable_cache_key,
            {"artists": [{"artist_id": 997, "artist_name": "Stable Indexed Artist"}], "total": 1, "limit": 100, "offset": 0},
            ttl=300,
        )
        pmda._files_cache_set_json(
            cache_key,
            {"artists": [{"artist_id": 998, "artist_name": "Stale Published Artist"}], "total": 1, "limit": 100, "offset": 0, "fallback_source": "published"},
            ttl=300,
        )
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
             mock.patch.object(pmda, "_files_index_maybe_enqueue_published_catchup", return_value={"underbuilt": False, "published_albums": 1, "pg_albums": 25, "pg_artists": 12}), \
             mock.patch.object(pmda, "_files_library_resolve_artist_ids_by_norms", return_value={"artist": 321}):
            resp = self.client.get("/api/library/artists?scope=library&browse_source=live")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload.get("fallback_source"), "stable_cache")
        artist = (payload.get("artists") or [{}])[0]
        self.assertEqual(int(artist.get("artist_id") or 0), 997)
        self.assertEqual(artist.get("artist_name"), "Stable Indexed Artist")

    def test_library_albums_ignores_published_cache_once_index_is_usable_again(self):
        self._insert_published_album()
        cache_key, stable_cache_key = self._albums_cache_keys("live")
        pmda._files_cache_set_json(
            stable_cache_key,
            {"albums": [{"album_id": 997, "title": "Stable Indexed Album"}], "total": 1, "limit": 80, "offset": 0, "scope": "library"},
            ttl=300,
        )
        pmda._files_cache_set_json(
            cache_key,
            {"albums": [{"album_id": 998, "title": "Stale Published Album"}], "total": 1, "limit": 80, "offset": 0, "scope": "library", "fallback_source": "published"},
            ttl=300,
        )
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
             mock.patch.object(pmda, "_files_index_maybe_enqueue_published_catchup", return_value={"underbuilt": False, "published_albums": 1, "pg_albums": 25, "pg_artists": 12}), \
             mock.patch.object(pmda, "_files_library_resolve_artist_ids_by_norms", return_value={"artist": 321}), \
             mock.patch.object(pmda, "_files_library_resolve_album_ids_by_folder_paths", return_value={str(self.album_dir): 654}):
            resp = self.client.get("/api/library/albums?scope=library&browse_source=live")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload.get("fallback_source"), "stable_cache")
        album = (payload.get("albums") or [{}])[0]
        self.assertEqual(int(album.get("album_id") or 0), 997)
        self.assertEqual(album.get("title"), "Stable Indexed Album")

    def test_library_labels_uses_published_fallback_while_scan_busy_and_index_lock_is_held(self):
        self._insert_published_album()
        files_index_was_locked = pmda.files_index_lock.locked()
        if not files_index_was_locked:
            pmda.files_index_lock.acquire()
        try:
            with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
                 mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
                 mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
                 mock.patch.object(pmda, "_files_cache_set_json", return_value=None), \
                 mock.patch.object(pmda, "_files_library_resolve_album_ids_by_folder_paths", return_value={str(self.album_dir): 654}):
                resp = self.client.get("/api/library/labels?limit=12&offset=0&scope=library")
        finally:
            if not files_index_was_locked and pmda.files_index_lock.locked():
                pmda.files_index_lock.release()

        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload.get("fallback_source"), "published")
        self.assertTrue(bool(payload.get("stale")))
        self.assertEqual(payload.get("total"), 1)
        labels = payload.get("labels") or []
        self.assertEqual([item.get("value") for item in labels], ["Warp"])

    def test_library_labels_uses_published_fallback_while_scan_busy_even_without_index_lock(self):
        self._insert_published_album()
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
             mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
             mock.patch.object(pmda, "_files_cache_set_json", return_value=None):
            resp = self.client.get("/api/library/labels?limit=12&offset=0&scope=library")

        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload.get("fallback_source"), "published")
        self.assertTrue(bool(payload.get("stale")))
        self.assertEqual(payload.get("total"), 1)
        labels = payload.get("labels") or []
        self.assertEqual([item.get("value") for item in labels], ["Warp"])

    def test_ensure_files_index_ready_skips_bootstrap_probe_when_index_counts_exist(self):
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_pg_init_schema", return_value=True), \
             mock.patch.object(pmda, "_files_index_read_counts", return_value=(11, 22, 333)), \
             mock.patch.object(pmda, "_files_index_read_track_and_embedding_counts", return_value=(333, 333)), \
             mock.patch.object(pmda, "_files_backfill_trusted_match_flags", return_value=0), \
             mock.patch.object(pmda, "_files_backfill_artist_browse_entities_from_existing_index", return_value={"links": 0}), \
             mock.patch.object(pmda, "_enqueue_files_reco_embedding_backfill", return_value=None), \
             mock.patch.object(pmda, "_pipeline_bootstrap_status", side_effect=AssertionError("bootstrap status should not be queried when index counts exist")):
            ok, err = pmda._ensure_files_index_ready()

        self.assertTrue(ok)
        self.assertIsNone(err)

    def test_reco_embedding_backfill_uses_separate_state_not_files_index_running(self):
        class FakeTx:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, *_args, **_kwargs):
                return None

        class FakeConn:
            def transaction(self):
                return FakeTx()

            def cursor(self):
                return FakeCursor()

            def close(self):
                return None

        def fake_embedding_build(_conn, *, progress_cb=None, **_kwargs):
            if progress_cb:
                progress_cb(6, 6)
            return 12

        with pmda.lock:
            pmda.state["files_index"] = {"running": False, "phase": "done"}
            pmda.state["files_reco_embeddings"] = {}

        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_pg_init_schema", return_value=True), \
             mock.patch.object(pmda, "_files_index_read_track_and_embedding_counts", return_value=(12, 0)), \
             mock.patch.object(pmda, "_files_pg_connect", return_value=FakeConn()), \
             mock.patch.object(pmda, "_reco_build_track_embeddings_chunked", side_effect=fake_embedding_build), \
             mock.patch.object(pmda, "_files_cache_invalidate_all", return_value=None), \
             mock.patch.object(pmda, "_files_index_read_counts", return_value=(2, 3, 12)):
            result = pmda._rebuild_files_reco_embeddings(reason="unit_test", wait_if_running=False)

        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("track_embeddings"), 12)
        with pmda.lock:
            index_state = dict(pmda.state.get("files_index") or {})
            reco_state = dict(pmda.state.get("files_reco_embeddings") or {})
        self.assertFalse(bool(index_state.get("running")))
        self.assertEqual(index_state.get("phase"), "done")
        self.assertFalse(bool(reco_state.get("running")))
        self.assertEqual(reco_state.get("embeddings_done"), 12)

    def test_library_artist_status_helper_reports_cover_fallback_honestly(self):
        status = pmda._files_library_artist_status_fields(
            status_context={
                "source_is_published": True,
                "background_enrichment_running": True,
            },
            has_image=False,
            has_profile=False,
            has_fallback_thumb=True,
        )
        self.assertEqual(
            status,
            {
                "publication_state": "published",
                "cover_state": "fallback",
                "artist_media_state": "enriching",
                "profile_state": "enriching",
            },
        )

    def test_library_album_status_helper_reports_missing_when_no_active_enrichment(self):
        status = pmda._files_library_album_status_fields(
            status_context={
                "source_is_published": False,
                "background_enrichment_running": False,
            },
            has_cover=False,
            has_artist_image=False,
            has_profile=False,
            cover_eligible=False,
            artist_media_eligible=False,
            profile_eligible=False,
        )
        self.assertEqual(
            status,
            {
                "publication_state": "ready",
                "cover_state": "missing",
                "artist_media_state": "missing",
                "profile_state": "missing",
            },
        )

    def test_library_discover_returns_scan_safe_snapshot_when_index_underbuilt(self):
        self._insert_published_album()
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
             mock.patch.object(pmda, "_files_index_maybe_enqueue_published_catchup", return_value={"underbuilt": True, "published_albums": 1}), \
             mock.patch.object(pmda, "_files_library_resolve_artist_ids_by_norms", return_value={"artist": 321}), \
             mock.patch.object(pmda, "_files_library_resolve_album_ids_by_folder_paths", return_value={str(self.album_dir): 654}):
            resp = self.client.get("/api/library/discover?scope=library")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload.get("fallback_source"), "published")
        sections = payload.get("sections") or []
        self.assertEqual(len(sections), 1)
        self.assertEqual(sections[0].get("key"), "scan_safe_recent")
        self.assertEqual(len(sections[0].get("albums") or []), 1)

    def test_recently_played_returns_empty_snapshot_instead_of_timeout_when_pg_unavailable(self):
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
             mock.patch.object(pmda, "_files_pg_connect", return_value=None):
            resp = self.client.get("/api/library/recently-played/albums?scope=library")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertTrue(bool(payload.get("stale")))
        self.assertIsNone(payload.get("error"))
        self.assertEqual(payload.get("albums"), [])

    def test_files_live_publish_batches_keeps_small_artists_atomic(self):
        items = [{"album_id": idx, "folder": f"/tmp/a{idx}"} for idx in range(1, 10)]
        batches = pmda._files_live_publish_batches(items)
        self.assertEqual(len(batches), 1)
        self.assertEqual(len(batches[0]), 9)

    def test_publish_files_library_artist_live_batches_splits_large_artist_and_reports_each_chunk(self):
        items = [{"album_id": idx, "folder": f"/tmp/a{idx}"} for idx in range(1, 18)]
        publish_sizes: list[int] = []
        batch_events: list[tuple[int, int, int, int]] = []

        def _fake_publish(artist_name, batch_items, *, scan_id=None, results_by_album_id=None):
            publish_sizes.append(len(batch_items))
            return len(batch_items)

        def _on_batch(*, inserted: int, batch_index: int, total_batches: int, batch_size: int) -> None:
            batch_events.append((inserted, batch_index, total_batches, batch_size))

        with mock.patch.object(pmda, "_publish_files_library_artist_from_items", side_effect=_fake_publish):
            summary = pmda._publish_files_library_artist_live_batches(
                "Big Artist",
                items,
                scan_id=42,
                on_batch=_on_batch,
            )

        self.assertEqual(publish_sizes, [8, 8, 1])
        self.assertEqual(
            batch_events,
            [
                (8, 1, 3, 8),
                (8, 2, 3, 8),
                (1, 3, 3, 1),
            ],
        )
        self.assertEqual(summary, {"published": 17, "batches": 3, "chunk_size": 8})

    def test_files_profile_enrichment_priority_flags_default_all_runs_everything(self):
        flags = pmda._files_profile_enrichment_priority_flags()
        self.assertEqual(flags.get("priority_mode"), "all")
        self.assertTrue(bool(flags.get("run_visual_stage")))
        self.assertTrue(bool(flags.get("run_artist_profile_stage")))
        self.assertTrue(bool(flags.get("run_album_profile_stage")))

    def test_files_profile_enrichment_priority_flags_cover_only_forces_visual_stage_only(self):
        flags = pmda._files_profile_enrichment_priority_flags(
            priority_mode="p2",
            skip_album_profiles=False,
            cover_only=True,
        )
        self.assertEqual(flags.get("priority_mode"), "p0")
        self.assertTrue(bool(flags.get("run_visual_stage")))
        self.assertFalse(bool(flags.get("run_artist_profile_stage")))
        self.assertFalse(bool(flags.get("run_album_profile_stage")))

    def test_files_profile_backfill_stage_specs_cover_only_is_visual_only(self):
        self.assertEqual(
            pmda._files_profile_backfill_stage_specs(cover_only=True),
            [("p0", "Visual assets")],
        )

    def test_files_profile_backfill_stage_specs_full_backfill_runs_p0_p1_p2(self):
        self.assertEqual(
            pmda._files_profile_backfill_stage_specs(cover_only=False),
            [
                ("p0", "Visual assets"),
                ("p1", "Artist profiles"),
                ("p2", "Album profiles"),
            ],
        )

    def test_scan_history_metadata_rollup_distinguishes_mb_participation_from_strict_wins(self):
        scan_id = self._insert_scan_history_with_pipeline_trace()
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        try:
            cur = con.cursor()
            rollup = pmda._scan_history_metadata_rollup(cur, scan_id)
            self.assertEqual(rollup.get("strict_total_albums"), 4)
            self.assertEqual(rollup.get("strict_matched_albums"), 3)
            self.assertEqual(
                rollup.get("strict_provider_counts"),
                {
                    "musicbrainz": 1,
                    "discogs": 1,
                    "bandcamp": 1,
                },
            )
            self.assertEqual(rollup.get("musicbrainz_identity_hits"), 3)
            self.assertEqual(rollup.get("musicbrainz_identity_verified"), 2)
            self.assertEqual(rollup.get("musicbrainz_strict_wins"), 1)
            self.assertEqual(rollup.get("musicbrainz_identity_non_wins"), 1)
            self.assertEqual(rollup.get("musicbrainz_ids_captured"), 3)
            self.assertEqual(
                rollup.get("musicbrainz_outcome_counts"),
                {
                    "strict_win": 1,
                    "id_captured_other_provider_won": 1,
                    "no_mb_signal_other_provider_won": 1,
                    "id_captured_no_strict_match": 1,
                },
            )
            self.assertEqual(
                rollup.get("musicbrainz_non_win_by_winner"),
                {
                    "discogs": 1,
                },
            )
            self.assertEqual(
                rollup.get("musicbrainz_non_win_by_reason"),
                {
                    "strict_reject": 1,
                },
            )
        finally:
            con.close()

    def test_mcp_scan_trace_summary_reports_matching_confidence_tiers(self):
        scan_id = self._insert_scan_history_with_pipeline_trace()
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        con.row_factory = sqlite3.Row
        try:
            cur = con.cursor()
            summary = pmda._mcp_scan_trace_summary(cur, scan_id, {})
        finally:
            con.close()
        self.assertEqual(summary.get("confidence_tiers", {}).get("strict_mb"), 1)
        self.assertEqual(summary.get("confidence_tiers", {}).get("strong_provider"), 2)
        self.assertEqual(summary.get("confidence_tiers", {}).get("soft_provider"), 1)
        self.assertEqual(summary.get("confidence_tiers", {}).get("ai_review"), 0)
        self.assertEqual(summary.get("confidence_tiers", {}).get("unresolved"), 0)
        self.assertGreater(summary.get("confidence_tier_percent", {}).get("strong_provider"), 0)

    def test_scan_history_endpoint_backfills_metadata_rollup_into_summary_json(self):
        scan_id = self._insert_scan_history_with_pipeline_trace()
        resp = self.client.get("/api/scan-history")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or []
        entry = next(item for item in payload if int(item.get("scan_id") or 0) == scan_id)
        summary = entry.get("summary_json") or {}
        self.assertEqual(summary.get("strict_provider_counts", {}).get("musicbrainz"), 1)
        self.assertEqual(summary.get("strict_provider_counts", {}).get("discogs"), 1)
        self.assertEqual(summary.get("musicbrainz_identity_hits"), 3)
        self.assertEqual(summary.get("musicbrainz_strict_wins"), 1)
        self.assertEqual(summary.get("musicbrainz_identity_non_wins"), 1)
        self.assertEqual(summary.get("musicbrainz_ids_captured"), 3)
        self.assertEqual(summary.get("musicbrainz_outcome_counts", {}).get("id_captured_other_provider_won"), 1)
        self.assertEqual(summary.get("musicbrainz_non_win_by_winner", {}).get("discogs"), 1)
        self.assertEqual(summary.get("musicbrainz_non_win_by_reason", {}).get("strict_reject"), 1)

    def test_resume_files_plan_restore_stays_metadata_only(self):
        run_id = "resume-run-fast"
        now = float(time.time())
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        try:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO scan_resume_runs (
                    run_id, created_at, updated_at, mode, scan_type, source_signature,
                    status, detected_artists_total, detected_albums_total, detected_tracks_total,
                    plan_snapshot_ready
                ) VALUES (?, ?, ?, 'files', 'full', 'sig', 'running', 1, 1, 9, 1)
                """,
                (run_id, now, now),
            )
            cur.execute(
                """
                INSERT INTO scan_resume_files_plan (
                    run_id, album_id, artist_name, artist_order, album_order, album_title, album_norm,
                    folder_path, fingerprint, file_count, has_cover, has_artist_image, has_mbid,
                    has_identity, identity_provider, strict_match_verified, strict_match_provider,
                    strict_reject_reason, strict_tracklist_score, musicbrainz_id, discogs_release_id,
                    lastfm_album_mbid, bandcamp_album_url, metadata_source, missing_required_tags_json,
                    skip_heavy_processing, lookup_artist_name, lookup_album_title
                ) VALUES (?, 101, 'Artist', 1, 1, 'Album', 'album', ?, 'fp-1', 9, 1, 0, 1, 1, 'musicbrainz', 1, 'discogs', '', 0.98, 'mbid-1', 'discogs-1', 'lfm-1', 'https://bandcamp.test/a', 'discogs', '[\"artist\",\"album\"]', 1, 'Artist', 'Album')
                """,
                (run_id, "/music/Music_dump/Artist/Album"),
            )
            con.commit()
        finally:
            con.close()

        with mock.patch.object(pmda, "path_for_fs_access", side_effect=AssertionError("filesystem access forbidden")), \
             mock.patch.object(pmda, "_album_folder_cache_key", side_effect=AssertionError("folder key must stay raw")):
            restored = pmda._load_resume_files_plan_by_run_id(run_id)

        self.assertIsNotNone(restored)
        self.assertEqual(restored.get("run_id"), run_id)
        self.assertEqual(restored.get("total_albums"), 1)
        self.assertEqual(restored.get("detected_tracks_total"), 9)
        files_map = restored.get("files_editions_by_album_id") or {}
        self.assertEqual(set(files_map.keys()), {101})
        edition = files_map[101]
        self.assertTrue(bool(edition.get("_resume_stub")))
        self.assertEqual(str(edition.get("folder") or ""), "/music/Music_dump/Artist/Album")
        self.assertEqual(edition.get("folder_key"), "/music/Music_dump/Artist/Album")
        self.assertEqual(edition.get("resume_sig_part"), "/music/Music_dump/Artist/Album|fp-1")
        self.assertEqual(edition.get("missing_required_tags"), [])
        self.assertTrue(bool(edition.get("skip_heavy_processing")))

    def test_persist_resume_files_plan_stays_metadata_only(self):
        run_id = "resume-run-persist-fast"
        now = float(time.time())
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        try:
            con.execute(
                """
                INSERT INTO scan_resume_runs (
                    run_id, created_at, updated_at, mode, scan_type, source_signature,
                    status, detected_artists_total, detected_albums_total, detected_tracks_total,
                    plan_snapshot_ready
                ) VALUES (?, ?, ?, 'files', 'full', 'sig', 'running', 0, 0, 0, 0)
                """,
                (run_id, now, now),
            )
            con.commit()
        finally:
            con.close()

        artists_merged = [(0, "Artist", [101])]
        files_editions_by_album_id = {
            101: {
                "folder": "/music/Music_dump/Artist/Album",
                "album_title": "Album",
                "album_norm": "album",
                "fingerprint": "fp-1",
                "file_count": 9,
                "has_cover": True,
                "has_artist_image": False,
                "has_mbid": True,
                "has_identity": True,
                "identity_provider": "musicbrainz",
                "strict_match_verified": True,
                "strict_match_provider": "discogs",
                "strict_tracklist_score": 0.97,
                "musicbrainz_id": "mbid-1",
                "discogs_release_id": "discogs-1",
                "lastfm_album_mbid": "lfm-1",
                "bandcamp_album_url": "https://bandcamp.test/a",
                "metadata_source": "discogs",
                "missing_required_tags": ["artist"],
            }
        }

        with mock.patch.object(pmda, "path_for_fs_access", side_effect=AssertionError("filesystem access forbidden")):
            rows = pmda._persist_resume_files_plan(
                run_id,
                artists_merged,
                files_editions_by_album_id,
                detected_artists_total=1,
                detected_albums_total=1,
                detected_tracks_total=9,
            )

        self.assertEqual(rows, 1)
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        try:
            cur = con.cursor()
            cur.execute(
                "SELECT folder_path, fingerprint, missing_required_tags_json FROM scan_resume_files_plan WHERE run_id = ?",
                (run_id,),
            )
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "/music/Music_dump/Artist/Album")
            self.assertEqual(row[1], "fp-1")
            self.assertEqual(row[2], "[\"artist\"]")
        finally:
            con.close()

    def test_partial_resume_files_plan_restores_rows_before_plan_snapshot_ready(self):
        run_id = "resume-run-partial-fast"
        now = float(time.time())
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        try:
            con.execute(
                """
                INSERT INTO scan_resume_runs (
                    run_id, created_at, updated_at, mode, scan_type, source_signature,
                    status, detected_artists_total, detected_albums_total, detected_tracks_total,
                    plan_snapshot_ready
                ) VALUES (?, ?, ?, 'files', 'full', 'sig', 'running', 0, 0, 0, 0)
                """,
                (run_id, now, now),
            )
            con.commit()
        finally:
            con.close()

        files_editions_by_album_id = {
            101: {
                "folder": "/music/Music_dump/Artist/Album",
                "artist_name": "Artist",
                "album_title": "Album",
                "album_norm": "album",
                "fingerprint": "fp-1",
                "file_count": 9,
                "has_cover": True,
                "has_artist_image": False,
                "has_mbid": True,
                "has_identity": True,
                "identity_provider": "musicbrainz",
                "strict_match_verified": True,
                "strict_match_provider": "discogs",
                "strict_tracklist_score": 0.97,
                "musicbrainz_id": "mbid-1",
                "discogs_release_id": "discogs-1",
                "lastfm_album_mbid": "lfm-1",
                "bandcamp_album_url": "https://bandcamp.test/a",
                "metadata_source": "discogs",
                "missing_required_tags": ["artist"],
            }
        }

        with mock.patch.object(pmda, "path_for_fs_access", side_effect=AssertionError("filesystem access forbidden")):
            rows = pmda._upsert_resume_files_plan_partial(
                run_id,
                [101],
                files_editions_by_album_id,
                detected_artists_total=1,
                detected_albums_total=1,
                detected_tracks_total=9,
            )

        self.assertEqual(rows, 1)
        self.assertIsNone(pmda._load_resume_files_plan_by_run_id(run_id))
        restored = pmda._load_resume_files_plan_partial_by_run_id(run_id)
        self.assertIsNotNone(restored)
        self.assertEqual(restored.get("run_id"), run_id)
        self.assertEqual(restored.get("total_albums"), 1)
        files_map = restored.get("files_editions_by_album_id") or {}
        self.assertEqual(set(files_map.keys()), {101})
        self.assertTrue(bool(files_map[101].get("_resume_stub")))
        self.assertEqual(str(files_map[101].get("folder") or ""), "/music/Music_dump/Artist/Album")

    def test_restored_resume_plan_forces_plan_rows_even_when_artist_marked_done(self):
        run_id = "resume-run-plan-authoritative"
        now = float(time.time())
        files_editions_by_album_id = {
            101: {
                "folder": "/music/Music_dump/Artist/Album",
                "artist_name": "Artist",
                "album_title": "Album",
                "album_norm": "album",
                "fingerprint": "fp-1",
                "resume_sig_part": "/music/Music_dump/Artist/Album|fp-1",
            }
        }
        artists_merged = [(0, "Artist", [101])]
        signature = pmda._compute_artist_signature(
            "files",
            "Artist",
            [101],
            files_editions_by_album_id=files_editions_by_album_id,
            files_signature_part_by_album_id={101: "/music/Music_dump/Artist/Album|fp-1"},
        )
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        try:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO scan_resume_runs (
                    run_id, created_at, updated_at, mode, scan_type, source_signature,
                    status, detected_artists_total, detected_albums_total, detected_tracks_total,
                    plan_snapshot_ready
                ) VALUES (?, ?, ?, 'files', 'full', 'sig', 'running', 1, 1, 9, 1)
                """,
                (run_id, now, now),
            )
            cur.execute(
                """
                INSERT INTO scan_resume_artists (
                    run_id, artist_name, artist_signature, status, album_count, updated_at, error
                ) VALUES (?, 'Artist', ?, 'done', 1, ?, NULL)
                """,
                (run_id, signature, now),
            )
            con.commit()
        finally:
            con.close()

        restored_run_id, artists_to_scan, skipped_artists, skipped_albums = pmda._prepare_resume_scan_artists(
            "files",
            "full",
            artists_merged,
            files_editions_by_album_id=files_editions_by_album_id,
            resume_run_id_override=run_id,
            force_include_plan_rows=True,
        )

        self.assertEqual(restored_run_id, run_id)
        self.assertEqual(artists_to_scan, artists_merged)
        self.assertEqual(skipped_artists, 0)
        self.assertEqual(skipped_albums, 0)
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        try:
            row = con.execute(
                "SELECT status FROM scan_resume_artists WHERE run_id = ? AND artist_name = 'Artist'",
                (run_id,),
            ).fetchone()
        finally:
            con.close()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "pending")

    def test_persist_resume_discovery_progress_only_keeps_album_candidate_counts(self):
        run_id = "resume-run-discovery-progress"
        now = float(time.time())
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        try:
            con.execute(
                """
                INSERT INTO scan_resume_runs (
                    run_id, created_at, updated_at, mode, scan_type, source_signature,
                    status, detected_artists_total, detected_albums_total, detected_tracks_total,
                    discovery_snapshot_ready
                ) VALUES (?, ?, ?, 'files', 'full', 'sig', 'running', 0, 0, 0, 0)
                """,
                (run_id, now, now),
            )
            con.commit()
        finally:
            con.close()

        snapshot = {
            "stage": "album_candidates",
            "roots": ["/music/Music_dump"],
            "shared_entries_scanned": 999578,
            "shared_files_found": 622908,
            "albums_found": 3859,
            "artists_found": 3142,
            "folders_found": 3860,
            "folders_done": 3860,
            "folders_total": 60535,
            "cached_album_folders": ["/music/Music_dump/Artist/Album"],
        }
        result = pmda._persist_resume_discovery_progress_only(run_id, snapshot)
        self.assertTrue(bool(result.get("ok")))

        con = sqlite3.connect(pmda.STATE_DB_FILE)
        try:
            cur = con.cursor()
            cur.execute(
                """
                SELECT detected_artists_total, detected_albums_total, detected_tracks_total,
                       discovery_stage, discovery_state_json
                FROM scan_resume_runs
                WHERE run_id = ?
                """,
                (run_id,),
            )
            row = cur.fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertEqual(int(row[0] or 0), 3142)
        self.assertEqual(int(row[1] or 0), 3859)
        self.assertEqual(int(row[2] or 0), 622908)
        self.assertEqual(str(row[3] or ""), "album_candidates")
        state_json = pmda.json.loads(row[4] or "{}")
        self.assertEqual(int(state_json.get("folders_done") or 0), 3860)
        self.assertEqual(int(state_json.get("folders_total") or 0), 60535)
        self.assertEqual(int(state_json.get("albums_found") or 0), 3859)
        self.assertEqual(int(state_json.get("artists_found") or 0), 3142)

    def test_build_files_cache_row_from_resume_stub_stays_metadata_only(self):
        item = {
            "folder": "/music/Music_dump/Artist/Album",
            "_resume_stub": True,
            "fingerprint": "fp-2",
            "artist_name": "Artist",
            "album_title": "Album",
            "has_cover": True,
            "has_artist_image": False,
            "has_mbid": True,
            "has_identity": True,
            "identity_provider": "musicbrainz",
            "musicbrainz_id": "mbid-2",
            "discogs_release_id": "discogs-2",
            "lastfm_album_mbid": "lfm-2",
            "bandcamp_album_url": "https://bandcamp.test/b",
            "metadata_source": "discogs",
        }
        with mock.patch.object(pmda, "path_for_fs_access", side_effect=AssertionError("filesystem access forbidden")), \
             mock.patch.object(pmda, "_album_folder_cache_key", side_effect=AssertionError("folder key must stay raw")), \
             mock.patch.object(pmda, "_source_id_for_path", side_effect=AssertionError("source lookup must stay lazy")):
            row = pmda._build_files_cache_row_from_prescan_item(item, scan_id=7, now_ts=123.0)

        self.assertEqual(row.get("folder_path"), "/music/Music_dump/Artist/Album")
        self.assertEqual(row.get("fingerprint"), "fp-2")
        self.assertEqual(row.get("artist_name"), "Artist")
        self.assertEqual(row.get("album_title"), "Album")
        self.assertEqual(row.get("last_scan_id"), 7)
        self.assertEqual(row.get("updated_at"), 123.0)
        self.assertEqual(row.get("ordered_paths"), [])
        self.assertEqual(row.get("missing_required_tags"), [])
        self.assertIsNone(row.get("source_id"))

    def test_files_should_snapshot_prescan_cache_for_run_skips_resume_runs(self):
        self.assertTrue(pmda._files_should_snapshot_prescan_cache_for_run())
        self.assertFalse(
            pmda._files_should_snapshot_prescan_cache_for_run(
                requested_resume_run_id="resume-1",
            )
        )
        self.assertFalse(
            pmda._files_should_snapshot_prescan_cache_for_run(
                current_resume_run_id="resume-1",
            )
        )


if __name__ == "__main__":
    unittest.main()
