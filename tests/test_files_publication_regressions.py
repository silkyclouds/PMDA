import tempfile
import unittest
import sqlite3
import json
from pathlib import Path
from unittest import mock

import pmda


class FilesPublicationRegressionTests(unittest.TestCase):
    def test_files_album_scan_cache_map_uses_has_identity_column(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_db = Path(tmp) / "state.db"
            orig_state_db = pmda.STATE_DB_FILE
            try:
                pmda.STATE_DB_FILE = state_db
                pmda.init_state_db()
                con = sqlite3.connect(state_db)
                cur = con.cursor()
                cur.execute(
                    """
                    INSERT INTO files_album_scan_cache (
                        folder_path, source_id, fingerprint, ordered_paths_json,
                        artist_name, album_title, has_cover, has_artist_image,
                        has_complete_tags, has_mbid, has_identity, identity_provider,
                        strict_match_verified, strict_match_provider, strict_reject_reason,
                        strict_tracklist_score, musicbrainz_id, musicbrainz_release_id,
                        discogs_release_id, lastfm_album_mbid, bandcamp_album_url,
                        metadata_source, missing_required_tags, last_scan_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "/music/Music_dump/A/Album",
                        0,
                        "fp",
                        "[]",
                        "Artist",
                        "Album",
                        0,
                        0,
                        1,
                        0,
                        1,
                        "discogs",
                        0,
                        "",
                        "",
                        0.0,
                        "",
                        "",
                        "123",
                        "",
                        "",
                        "discogs",
                        "[]",
                        7,
                        1.0,
                    ),
                )
                con.commit()
                con.close()

                loaded = pmda._load_files_album_scan_cache_map(folder_keys=["/music/Music_dump/A/Album"])
                self.assertTrue(loaded["/music/Music_dump/A/Album"]["has_identity"])
                self.assertEqual(loaded["/music/Music_dump/A/Album"]["identity_provider"], "discogs")
            finally:
                pmda.STATE_DB_FILE = orig_state_db

    def test_reconcile_files_publication_backfills_scan_editions_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_db = Path(tmp) / "state.db"
            album_dir = Path(tmp) / "Music_dump" / "Artist" / "Album"
            album_dir.mkdir(parents=True)
            (album_dir / "01 - Track.mp3").write_bytes(b"fake")
            orig_state_db = pmda.STATE_DB_FILE
            try:
                pmda.STATE_DB_FILE = state_db
                pmda.init_state_db()
                con = sqlite3.connect(state_db)
                cur = con.cursor()
                cur.execute(
                    """
                    INSERT INTO scan_editions (
                        scan_id, artist, album_id, title_raw, folder, fmt_text,
                        br, sr, bd, meta_json, musicbrainz_id, musicbrainz_release_id,
                        is_broken, expected_track_count, actual_track_count, missing_indices,
                        has_cover, missing_required_tags, strict_match_verified,
                        strict_match_provider, strict_reject_reason, strict_tracklist_score,
                        discogs_release_id, lastfm_album_mbid, bandcamp_album_url, metadata_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        7,
                        "Artist",
                        42,
                        "Album",
                        str(album_dir),
                        "MP3",
                        320,
                        44100,
                        16,
                        json.dumps({"albumartist": "Artist", "album": "Album"}),
                        "",
                        "",
                        0,
                        1,
                        1,
                        "[]",
                        0,
                        "[]",
                        1,
                        "discogs",
                        "",
                        1.0,
                        "123",
                        "",
                        "",
                        "discogs",
                    ),
                )
                con.commit()
                con.close()
                with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
                    mock.patch.object(pmda, "_files_pg_connect", return_value=None), \
                    mock.patch.object(pmda, "extract_tags", return_value={"albumartist": "Artist", "album": "Album"}), \
                    mock.patch.object(pmda, "_provider_track_titles_cached", return_value=[]), \
                    mock.patch.object(pmda, "_trigger_files_index_rebuild_async", return_value=True):
                    result = pmda._reconcile_files_publication_from_scan_editions(
                        scan_ids=[7],
                        reason="test",
                        rebuild_index=False,
                    )

                self.assertEqual(result["status"], "completed")
                self.assertEqual(result["published"], 1)
                con = sqlite3.connect(state_db)
                row = con.execute(
                    "SELECT folder_path, artist_name, album_title, strict_match_verified, strict_match_provider FROM files_library_published_albums",
                ).fetchone()
                con.close()
                self.assertEqual(Path(row[0]).resolve(), album_dir.resolve())
                self.assertEqual(row[1:], ("Artist", "Album", 1, "discogs"))
            finally:
                pmda.STATE_DB_FILE = orig_state_db

    def test_reconcile_files_publication_uses_matched_export_destination(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_db = Path(tmp) / "state.db"
            source_dir = Path(tmp) / "Music_dump" / "Artist" / "Album"
            dest_dir = Path(tmp) / "Music_matched" / "A" / "Artist" / "Album"
            dest_dir.mkdir(parents=True)
            (dest_dir / "01 - Track.mp3").write_bytes(b"fake")
            orig_state_db = pmda.STATE_DB_FILE
            try:
                pmda.STATE_DB_FILE = state_db
                pmda.init_state_db()
                con = sqlite3.connect(state_db)
                cur = con.cursor()
                cur.execute(
                    """
                    INSERT INTO scan_editions (
                        scan_id, artist, album_id, title_raw, folder, fmt_text,
                        br, sr, bd, meta_json, is_broken, expected_track_count,
                        actual_track_count, missing_indices, has_cover, missing_required_tags,
                        strict_match_verified, strict_match_provider, strict_reject_reason,
                        strict_tracklist_score, metadata_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        8,
                        "Artist",
                        77,
                        "Album",
                        str(source_dir),
                        "MP3",
                        320,
                        44100,
                        16,
                        json.dumps({"albumartist": "Artist", "album": "Album"}),
                        0,
                        1,
                        1,
                        "[]",
                        0,
                        "[]",
                        1,
                        "lastfm",
                        "",
                        1.0,
                        "lastfm",
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO scan_moves (
                        scan_id, artist, album_id, original_path, moved_to_path,
                        size_mb, moved_at, restored, album_title, fmt_text,
                        move_reason, source_path, destination_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        8,
                        "Artist",
                        77,
                        str(source_dir),
                        str(dest_dir),
                        1,
                        1.0,
                        0,
                        "Album",
                        "MP3",
                        "matched_export",
                        str(source_dir),
                        str(dest_dir),
                    ),
                )
                con.commit()
                con.close()
                with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
                    mock.patch.object(pmda, "_files_pg_connect", return_value=None), \
                    mock.patch.object(pmda, "extract_tags", return_value={"albumartist": "Artist", "album": "Album"}), \
                    mock.patch.object(pmda, "_provider_track_titles_cached", return_value=[]), \
                    mock.patch.object(pmda, "_trigger_files_index_rebuild_async", return_value=True):
                    result = pmda._reconcile_files_publication_from_scan_editions(
                        scan_ids=[8],
                        reason="test",
                        rebuild_index=False,
                    )

                self.assertEqual(result["status"], "completed")
                self.assertEqual(result["published"], 1)
                con = sqlite3.connect(state_db)
                folders = [row[0] for row in con.execute("SELECT folder_path FROM files_library_published_albums").fetchall()]
                con.close()
                self.assertEqual([Path(folder).resolve() for folder in folders], [dest_dir.resolve()])
            finally:
                pmda.STATE_DB_FILE = orig_state_db

    def test_library_scope_uses_virtual_visibility_for_files_managed_library(self):
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), mock.patch.object(
            pmda,
            "_library_workflow_state",
            return_value={"mode": "managed"},
        ), mock.patch.object(
            pmda,
            "_library_workflow_scope_roots",
            return_value={"library_roots": ["/music/Music_matched"], "inbox_roots": ["/music/incomming"], "dupe_roots": ["/dupes"]},
        ):
            where_sql = pmda._library_album_scope_where("library", "alb")
            inbox_sql = pmda._library_album_scope_where("inbox", "alb")

        self.assertIn("COALESCE(alb.is_broken, FALSE) = FALSE", where_sql)
        self.assertIn("NOT (COALESCE(alb.is_broken, FALSE) = FALSE)", inbox_sql)

    def test_sqlite_library_scope_uses_virtual_visibility_for_files_managed_library(self):
        params: list[object] = []
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), mock.patch.object(
            pmda,
            "_library_workflow_state",
            return_value={"mode": "managed"},
        ), mock.patch.object(
            pmda,
            "_library_workflow_scope_roots",
            return_value={"library_roots": ["/music/Music_matched"], "inbox_roots": ["/music/incomming"], "dupe_roots": ["/dupes"]},
        ):
            where_sql = pmda._files_library_published_scope_where_sqlite("library", params)
            inbox_sql = pmda._files_library_published_scope_where_sqlite("inbox", params)

        self.assertEqual(where_sql, "COALESCE(is_broken, 0) = 0")
        self.assertIn("NOT (COALESCE(is_broken, 0) = 0)", inbox_sql)

    def test_files_pg_is_connection_dropped_error_detects_closed_backend(self):
        class FakeOperationalError(Exception):
            pass

        self.assertTrue(
            pmda._files_pg_is_connection_dropped_error(
                FakeOperationalError("consuming input failed: server closed the connection unexpectedly")
            )
        )
        self.assertTrue(
            pmda._files_pg_is_connection_dropped_error(
                FakeOperationalError("the connection is closed")
            )
        )
        self.assertFalse(pmda._files_pg_is_connection_dropped_error(RuntimeError("artist alias mismatch")))

    def test_scan_provider_no_tracklist_rollup_distinguishes_provider_and_cause(self):
        con = sqlite3.connect(":memory:")
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE scan_editions (
                scan_id INTEGER,
                strict_match_provider TEXT,
                metadata_source TEXT,
                strict_reject_reason TEXT,
                musicbrainz_release_id TEXT,
                musicbrainz_id TEXT,
                discogs_release_id TEXT,
                lastfm_album_mbid TEXT,
                bandcamp_album_url TEXT
            )
            """
        )
        cur.executemany(
            """
            INSERT INTO scan_editions (
                scan_id, strict_match_provider, metadata_source, strict_reject_reason,
                musicbrainz_release_id, musicbrainz_id, discogs_release_id,
                lastfm_album_mbid, bandcamp_album_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (7, "", "bandcamp", "provider_no_tracklist", "", "", "", "", "https://example.bandcamp.com/album/x"),
                (7, "", "", "provider_no_tracklist_full_album", "", "", "42", "", ""),
                (7, "", "", "provider_no_tracklist", "", "", "", "", ""),
                (7, "discogs", "bandcamp", "provider_no_tracklist", "", "", "42", "", "https://example.bandcamp.com/album/y"),
            ],
        )
        rollup = pmda._scan_provider_no_tracklist_rollup(cur, 7)
        self.assertEqual(rollup["total"], 4)
        self.assertEqual(rollup["by_provider"]["bandcamp"], 1)
        self.assertEqual(rollup["by_provider"]["discogs"], 2)
        self.assertEqual(rollup["by_provider"]["none"], 1)
        self.assertEqual(rollup["by_cause"]["api_or_parser"], 1)
        self.assertEqual(rollup["by_cause"]["edition"], 2)
        self.assertEqual(rollup["by_cause"]["absence_real"], 1)
        self.assertEqual(rollup["details_by_provider"]["discogs"]["full_album"], 1)
        con.close()

    def test_sync_scan_pipeline_trace_move_rows_propagates_scan_moves(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_db = Path(tmp) / "state.db"
            moved_to_path = Path(tmp) / "Music_matched" / "A" / "Artist" / "Album (Flac,  Album)"
            moved_to_path.mkdir(parents=True)
            con = sqlite3.connect(state_db)
            cur = con.cursor()
            cur.execute(
                """
                CREATE TABLE scan_moves (
                    scan_id INTEGER,
                    album_id INTEGER,
                    move_reason TEXT,
                    original_path TEXT,
                    moved_to_path TEXT,
                    restored INTEGER,
                    decision_provider TEXT,
                    decision_reason TEXT,
                    decision_confidence REAL,
                    source_path TEXT,
                    destination_path TEXT,
                    materialization_strategy TEXT,
                    arbitration_result TEXT,
                    moved_at REAL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE scan_pipeline_trace (
                    scan_id INTEGER,
                    album_id INTEGER,
                    metadata_source TEXT,
                    strict_match_verified INTEGER,
                    is_broken INTEGER,
                    dupe_role TEXT,
                    move_reason TEXT,
                    move_status TEXT,
                    moved_to_path TEXT,
                    decision_provider TEXT,
                    decision_reason TEXT,
                    decision_confidence REAL,
                    pipeline_status TEXT,
                    timeline_json TEXT,
                    meta_summary_json TEXT,
                    updated_at REAL
                )
                """
            )
            cur.execute(
                """
                INSERT INTO scan_moves (
                    scan_id, album_id, move_reason, original_path, moved_to_path,
                    restored, decision_provider, decision_reason, decision_confidence,
                    source_path, destination_path, materialization_strategy, arbitration_result, moved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    8,
                    42,
                    "matched_export",
                    "/music/Music_dump/A/Artist/Album",
                    str(moved_to_path),
                    0,
                    "bandcamp",
                    "strict_verification",
                    1.0,
                    "/music/Music_dump/A/Artist/Album",
                    str(moved_to_path),
                    "hardlink",
                    "promoted",
                    1234.5,
                ),
            )
            cur.execute(
                """
                INSERT INTO scan_pipeline_trace (
                    scan_id, album_id, metadata_source, strict_match_verified, is_broken, dupe_role,
                    move_reason, move_status, moved_to_path, decision_provider, decision_reason,
                    decision_confidence, pipeline_status, timeline_json, meta_summary_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    8,
                    42,
                    "bandcamp",
                    1,
                    0,
                    "",
                    "",
                    "none",
                    "",
                    "",
                    "",
                    None,
                    "matched",
                    "[]",
                    "{}",
                    0.0,
                ),
            )
            con.commit()
            con.close()

            with mock.patch.object(pmda, "STATE_DB_FILE", state_db):
                synced = pmda._sync_scan_pipeline_trace_move_rows(8, album_ids=[42])

            self.assertEqual(synced, 1)

            con = sqlite3.connect(state_db)
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute(
                """
                SELECT move_reason, move_status, moved_to_path, decision_provider,
                       decision_reason, decision_confidence, pipeline_status,
                       timeline_json, meta_summary_json
                FROM scan_pipeline_trace
                WHERE scan_id = ? AND album_id = ?
                """,
                (8, 42),
            )
            row = cur.fetchone()
            con.close()

            self.assertIsNotNone(row)
            self.assertEqual(row["move_reason"], "matched_export")
            self.assertEqual(row["move_status"], "moved")
            self.assertEqual(row["moved_to_path"], str(moved_to_path))
            self.assertEqual(row["decision_provider"], "bandcamp")
            self.assertEqual(row["decision_reason"], "strict_verification")
            self.assertEqual(row["decision_confidence"], 1.0)
            self.assertEqual(row["pipeline_status"], "exported")
            self.assertIn("Exported to library", row["timeline_json"])
            self.assertIn("matched_export", row["meta_summary_json"])
            meta = json.loads(row["meta_summary_json"] or "{}")
            self.assertEqual(meta.get("move", {}).get("materialization_strategy"), "hardlink")
            self.assertEqual(meta.get("move", {}).get("arbitration_result"), "promoted")

    def test_reconcile_scan_move_trace_backlog_finds_pending_scan_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_db = Path(tmp) / "state.db"
            matched_a = Path(tmp) / "matched" / "a"
            matched_b = Path(tmp) / "matched" / "b"
            matched_a.mkdir(parents=True)
            matched_b.mkdir(parents=True)
            con = sqlite3.connect(state_db)
            cur = con.cursor()
            cur.execute(
                """
                CREATE TABLE scan_moves (
                    move_id INTEGER PRIMARY KEY,
                    scan_id INTEGER,
                    album_id INTEGER,
                    move_reason TEXT,
                    original_path TEXT,
                    moved_to_path TEXT,
                    restored INTEGER,
                    decision_provider TEXT,
                    decision_reason TEXT,
                    decision_confidence REAL,
                    source_path TEXT,
                    destination_path TEXT,
                    materialization_strategy TEXT,
                    arbitration_result TEXT,
                    moved_at REAL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE scan_pipeline_trace (
                    scan_id INTEGER,
                    album_id INTEGER,
                    metadata_source TEXT,
                    strict_match_verified INTEGER,
                    is_broken INTEGER,
                    dupe_role TEXT,
                    move_reason TEXT,
                    move_status TEXT,
                    moved_to_path TEXT,
                    decision_provider TEXT,
                    decision_reason TEXT,
                    decision_confidence REAL,
                    pipeline_status TEXT,
                    timeline_json TEXT,
                    meta_summary_json TEXT,
                    updated_at REAL
                )
                """
            )
            cur.executemany(
                """
                INSERT INTO scan_moves (
                    scan_id, album_id, move_reason, original_path, moved_to_path,
                    restored, decision_provider, decision_reason, decision_confidence,
                    source_path, destination_path, materialization_strategy, arbitration_result, moved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (8, 42, "matched_export", "/dump/a", str(matched_a), 0, "bandcamp", "strict", 1.0, "/dump/a", str(matched_a), "hardlink", "promoted", 1.0),
                    (7, 99, "matched_export", "/dump/b", str(matched_b), 0, "discogs", "strict", 1.0, "/dump/b", str(matched_b), "hardlink", "promoted", 2.0),
                ],
            )
            cur.executemany(
                """
                INSERT INTO scan_pipeline_trace (
                    scan_id, album_id, metadata_source, strict_match_verified, is_broken, dupe_role,
                    move_reason, move_status, moved_to_path, decision_provider, decision_reason,
                    decision_confidence, pipeline_status, timeline_json, meta_summary_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (8, 42, "bandcamp", 1, 0, "", "", "none", "", "", "", None, "matched", "[]", "{}", 0.0),
                    (7, 99, "discogs", 1, 0, "", "", "none", "", "", "", None, "matched", "[]", "{}", 0.0),
                ],
            )
            con.commit()
            con.close()

            with mock.patch.object(pmda, "STATE_DB_FILE", state_db):
                pending = pmda._scan_move_trace_pending_scan_ids(limit=10)
                synced = pmda._reconcile_scan_move_trace_backlog(reason="test", limit_scans=10)

            self.assertEqual(pending, [8, 7])
            self.assertEqual(synced, 2)

            con = sqlite3.connect(state_db)
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            rows = cur.execute(
                """
                SELECT scan_id, album_id, move_status, pipeline_status, moved_to_path
                FROM scan_pipeline_trace
                ORDER BY scan_id DESC, album_id DESC
                """
            ).fetchall()
            con.close()

            self.assertEqual(
                [dict(row) for row in rows],
                [
                    {
                        "scan_id": 8,
                        "album_id": 42,
                        "move_status": "moved",
                        "pipeline_status": "exported",
                        "moved_to_path": str(matched_a),
                    },
                    {
                        "scan_id": 7,
                        "album_id": 99,
                        "move_status": "moved",
                        "pipeline_status": "exported",
                        "moved_to_path": str(matched_b),
                    },
                ],
            )

    def test_init_state_db_backfills_scan_move_traceability_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_db = Path(tmp) / "state.db"
            con = sqlite3.connect(state_db)
            cur = con.cursor()
            cur.execute(
                """
                CREATE TABLE scan_moves (
                    move_id INTEGER PRIMARY KEY,
                    scan_id INTEGER,
                    artist TEXT,
                    album_id INTEGER,
                    original_path TEXT,
                    moved_to_path TEXT,
                    size_mb INTEGER,
                    moved_at REAL,
                    move_reason TEXT,
                    decision_reason TEXT,
                    details_json TEXT
                )
                """
            )
            cur.execute(
                """
                INSERT INTO scan_moves (
                    scan_id, artist, album_id, original_path, moved_to_path, size_mb, moved_at,
                    move_reason, decision_reason, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    13,
                    "Agog",
                    123,
                    "/music/Music_dump/Agog - Putting Legs on a Snake",
                    "/music/Music_matched/A/Agog/Putting Legs On A Snake (Flac,  EP)",
                    321,
                    1234.5,
                    "matched_export",
                    "hardlink_strict_match_verified",
                    json.dumps(
                        {
                            "export_strategy": "hardlink",
                            "source": "/music/Music_dump/Agog - Putting Legs on a Snake",
                            "destination": "/music/Music_matched/A/Agog/Putting Legs On A Snake (Flac,  EP)",
                        }
                    ),
                ),
            )
            con.commit()
            con.close()

            with mock.patch.object(pmda, "STATE_DB_FILE", state_db):
                pmda.init_state_db()

            con = sqlite3.connect(state_db)
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            row = cur.execute(
                """
                SELECT source_path, destination_path, materialization_strategy, arbitration_result
                FROM scan_moves
                ORDER BY move_id DESC
                LIMIT 1
                """
            ).fetchone()
            con.close()

            self.assertIsNotNone(row)
            self.assertEqual(row["source_path"], "/music/Music_dump/Agog - Putting Legs on a Snake")
            self.assertEqual(row["destination_path"], "/music/Music_matched/A/Agog/Putting Legs On A Snake (Flac,  EP)")
            self.assertEqual(row["materialization_strategy"], "hardlink")
            self.assertEqual(row["arbitration_result"], "promoted")

    def test_export_conflict_prefers_more_complete_incoming_over_preview_existing(self):
        source = {
            "folder": "/music/Music_dump/Gareth Davis & Scanner - Songlines",
            "title_raw": "Songlines",
            "tracks": [
                {"title": "Structure Of Statements"},
                {"title": "Figurative Language"},
                {"title": "Structure Of Statements Preview"},
                {"title": "Figurative Language Preview"},
            ],
            "file_count": 4,
            "missing_required_tags": [],
            "has_cover": True,
            "fmt_score": 10,
            "bd": 16,
            "sr": 48000,
            "br": 598624,
            "bandcamp_album_url": "https://gareth-davis.bandcamp.com/album/songlines",
        }
        existing = {
            "folder": "/music/Music_matched/G/Gareth Davis & Scanner/Songlines (Flac, Album)",
            "title_raw": "Songlines",
            "tracks": [{"title": "Figurative Language preview"}],
            "file_count": 1,
            "missing_required_tags": [],
            "has_cover": True,
            "fmt_score": 10,
            "bd": 16,
            "sr": 48000,
            "br": 599086,
            "bandcamp_album_url": "https://gareth-davis.bandcamp.com/album/songlines",
        }

        best, rationale, confident = pmda._choose_matched_export_conflict_winner(source, [existing])

        self.assertIs(best, source)
        self.assertTrue(confident)
        self.assertIn("export conflict override", rationale)

    def test_export_conflict_holds_unconfident_candidate_for_review(self):
        source = {
            "folder": "/music/Music_dump/Artist - Album",
            "title_raw": "Album",
            "tracks": [{"title": "Preview"}],
            "file_count": 1,
            "missing_required_tags": [],
            "has_cover": True,
            "fmt_score": 10,
            "bd": 24,
            "sr": 96000,
            "br": 900000,
            "bandcamp_album_url": "https://artist.bandcamp.com/album/album",
        }
        existing = {
            "folder": "/music/Music_matched/A/Artist/Album",
            "title_raw": "Album",
            "tracks": [{"title": "Track 1"}, {"title": "Track 2"}, {"title": "Track 3"}, {"title": "Track 4"}],
            "file_count": 4,
            "missing_required_tags": [],
            "has_cover": True,
            "fmt_score": 10,
            "bd": 16,
            "sr": 48000,
            "br": 600000,
            "bandcamp_album_url": "https://artist.bandcamp.com/album/album",
        }

        best, rationale, confident = pmda._choose_matched_export_conflict_winner(source, [existing])

        self.assertIsNone(best)
        self.assertFalse(confident)
        self.assertIn("held for review", rationale)

    def test_library_workflow_prepare_updates_managed_translates_guided_mode(self):
        with mock.patch.object(pmda, "_effective_files_source_rows", return_value=[]):
            updates, rows = pmda._library_workflow_prepare_updates(
                {},
                {
                    "LIBRARY_WORKFLOW_MODE": "managed",
                    "LIBRARY_SERVING_ROOT": "/music/Music_matched",
                    "LIBRARY_INTAKE_ROOTS": "/music/Music_dump,/music/Incoming",
                    "LIBRARY_DUPES_ROOT": "/dupes",
                    "LIBRARY_INCOMPLETE_ROOT": "/dupes/incomplete_albums",
                    "LIBRARY_MATERIALIZATION_MODE": "hardlink",
                    "LIBRARY_INCLUDE_FORMAT_IN_FOLDER": True,
                    "LIBRARY_INCLUDE_TYPE_IN_FOLDER": True,
                },
            )

        self.assertEqual(updates["LIBRARY_WORKFLOW_MODE"], "managed")
        self.assertEqual(updates["EXPORT_ROOT"], "/music/Music_matched")
        self.assertEqual(updates["EXPORT_LINK_STRATEGY"], "hardlink")
        self.assertEqual(updates["DUPE_ROOT"], "/dupes")
        self.assertEqual(updates["INCOMPLETE_ALBUMS_TARGET_DIR"], "/dupes/incomplete_albums")
        self.assertEqual(updates["FILES_ROOTS"], ["/music/Music_matched", "/music/Music_dump", "/music/Incoming"])
        self.assertEqual(rows[0]["path"], "/music/Music_matched")
        self.assertEqual(rows[0]["role"], "library")
        self.assertTrue(rows[0]["is_winner_root"])
        self.assertEqual(rows[1]["path"], "/music/Music_dump")
        self.assertEqual(rows[1]["role"], "incoming")
        self.assertEqual(rows[2]["path"], "/music/Incoming")
        self.assertEqual(rows[2]["role"], "incoming")

    def test_library_workflow_state_inplace_without_intake_hides_inbox(self):
        snapshot = {
            "LIBRARY_WORKFLOW_MODE": "inplace",
            "LIBRARY_SERVING_ROOT": "/music/Library",
            "LIBRARY_SOURCE_ROOTS": "/music/Library",
            "LIBRARY_INTAKE_ROOTS": "",
            "LIBRARY_DUPES_ROOT": "/dupes",
        }
        with mock.patch.object(pmda, "_effective_files_source_rows", return_value=[]):
            state = pmda._library_workflow_state(snapshot)

        self.assertEqual(state["mode"], "inplace")
        self.assertEqual(state["serving_root"], "/music/Library")
        self.assertEqual(state["scan_roots"], ["/music/Library"])
        self.assertEqual(state["visible_scopes"], ["library", "dupes"])
        self.assertFalse(state["has_intake"])

    def test_library_workflow_prepare_updates_audit_uses_library_root_without_serving_root(self):
        with mock.patch.object(pmda, "_effective_files_source_rows", return_value=[]):
            updates, rows = pmda._library_workflow_prepare_updates(
                {},
                {
                    "LIBRARY_WORKFLOW_MODE": "audit",
                    "LIBRARY_SOURCE_ROOTS": "/music/Music_matched",
                    "LIBRARY_DUPES_ROOT": "/dupes",
                    "LIBRARY_INCOMPLETE_ROOT": "/dupes/incomplete_albums",
                },
            )

        self.assertEqual(updates["LIBRARY_WORKFLOW_MODE"], "audit")
        self.assertEqual(updates["LIBRARY_SOURCE_ROOTS"], "/music/Music_matched")
        self.assertEqual(updates["LIBRARY_INTAKE_ROOTS"], "")
        self.assertEqual(updates["FILES_ROOTS"], ["/music/Music_matched"])
        self.assertEqual(updates["EXPORT_ROOT"], "/music/Music_matched")
        self.assertEqual(rows[0]["path"], "/music/Music_matched")
        self.assertEqual(rows[0]["role"], "library")

    def test_library_workflow_state_audit_without_intake_hides_inbox(self):
        snapshot = {
            "LIBRARY_WORKFLOW_MODE": "audit",
            "LIBRARY_SOURCE_ROOTS": "/music/Library",
            "LIBRARY_DUPES_ROOT": "/dupes",
        }
        with mock.patch.object(pmda, "_effective_files_source_rows", return_value=[]):
            state = pmda._library_workflow_state(snapshot)

        self.assertEqual(state["mode"], "audit")
        self.assertEqual(state["serving_root"], "/music/Library")
        self.assertEqual(state["scan_roots"], ["/music/Library"])
        self.assertEqual(state["visible_scopes"], ["library", "dupes"])
        self.assertFalse(state["has_intake"])

    def test_library_workflow_scope_roots_managed_separates_library_and_inbox(self):
        snapshot = {
            "LIBRARY_WORKFLOW_MODE": "managed",
            "LIBRARY_SERVING_ROOT": "/music/Music_matched",
            "LIBRARY_INTAKE_ROOTS": "/music/incomming,/music/Music_dump",
            "LIBRARY_DUPES_ROOT": "/dupes",
            "LIBRARY_INCOMPLETE_ROOT": "/dupes/Incompletes",
        }
        with mock.patch.object(pmda, "_effective_files_source_rows", return_value=[]):
            scope_roots = pmda._library_workflow_scope_roots(snapshot)

        self.assertEqual(scope_roots["library_roots"], ["/music/Music_matched"])
        self.assertEqual(scope_roots["inbox_roots"], ["/music/incomming", "/music/Music_dump"])
        self.assertEqual(scope_roots["scan_roots"], ["/music/incomming", "/music/Music_dump"])

    def test_library_workflow_scope_roots_mirror_scans_intake_not_clean_library(self):
        snapshot = {
            "LIBRARY_WORKFLOW_MODE": "mirror",
            "LIBRARY_SERVING_ROOT": "/music/Music_matched",
            "LIBRARY_SOURCE_ROOTS": "/music/Music_matched",
            "LIBRARY_INTAKE_ROOTS": "/music/Music_dump",
            "LIBRARY_DUPES_ROOT": "/dupes",
            "LIBRARY_INCOMPLETE_ROOT": "/dupes/Incompletes",
        }
        with mock.patch.object(pmda, "_effective_files_source_rows", return_value=[]):
            scope_roots = pmda._library_workflow_scope_roots(snapshot)

        self.assertEqual(scope_roots["library_roots"], ["/music/Music_matched"])
        self.assertEqual(scope_roots["inbox_roots"], ["/music/Music_dump"])
        self.assertEqual(scope_roots["scan_roots"], ["/music/Music_dump"])

    def test_effective_files_scan_roots_excludes_trusted_mirror_destination(self):
        snapshot = {
            "LIBRARY_WORKFLOW_MODE": "mirror",
            "LIBRARY_SERVING_ROOT": "/music/Music_matched",
            "LIBRARY_SOURCE_ROOTS": "/music/Music_matched",
            "LIBRARY_INTAKE_ROOTS": "/music/Music_dump",
        }
        source_rows = [
            {
                "source_id": 1,
                "path": "/music/Music_matched",
                "role": "library",
                "enabled": True,
                "priority": 10,
                "is_winner_root": True,
            },
            {
                "source_id": 2,
                "path": "/music/Music_dump",
                "role": "incoming",
                "enabled": True,
                "priority": 20,
                "is_winner_root": False,
            },
        ]
        with mock.patch.object(pmda, "_settings_db_read_all", return_value=snapshot), \
             mock.patch.object(pmda, "_effective_files_source_rows", return_value=source_rows):
            self.assertEqual(pmda._effective_files_scan_roots(), ["/music/Music_dump"])

    def test_library_workflow_scope_roots_audit_uses_all_library_roots(self):
        snapshot = {
            "LIBRARY_WORKFLOW_MODE": "audit",
            "LIBRARY_SOURCE_ROOTS": "/music/Music_matched,/music/Classical",
            "LIBRARY_INTAKE_ROOTS": "/music/incomming",
            "LIBRARY_DUPES_ROOT": "/dupes",
            "LIBRARY_INCOMPLETE_ROOT": "/dupes/Incompletes",
        }
        with mock.patch.object(pmda, "_effective_files_source_rows", return_value=[]):
            scope_roots = pmda._library_workflow_scope_roots(snapshot)

        self.assertEqual(scope_roots["library_roots"], ["/music/Music_matched", "/music/Classical"])
        self.assertEqual(scope_roots["inbox_roots"], ["/music/incomming"])
        self.assertEqual(scope_roots["scan_roots"], ["/music/Music_matched", "/music/Classical", "/music/incomming"])

    def test_library_album_scope_where_audit_library_uses_all_source_roots(self):
        snapshot = {
            "LIBRARY_WORKFLOW_MODE": "audit",
            "LIBRARY_SOURCE_ROOTS": "/music/Music_matched,/music/Classical",
            "LIBRARY_DUPES_ROOT": "/dupes",
            "LIBRARY_INCOMPLETE_ROOT": "/dupes/Incompletes",
        }
        with mock.patch.object(pmda, "_settings_db_read_all", return_value=snapshot), \
             mock.patch.object(pmda, "_effective_files_source_rows", return_value=[]):
            where_sql = pmda._library_album_scope_where("library", "alb")

        self.assertIn("/music/Music_matched", where_sql)
        self.assertIn("/music/Classical", where_sql)

    def test_library_workflow_scope_roots_inplace_uses_optional_intake_for_inbox_only(self):
        snapshot = {
            "LIBRARY_WORKFLOW_MODE": "inplace",
            "LIBRARY_SOURCE_ROOTS": "/music/Music_matched",
            "LIBRARY_INTAKE_ROOTS": "/music/incomming",
            "LIBRARY_SERVING_ROOT": "/music/Music_matched",
            "LIBRARY_DUPES_ROOT": "/dupes",
            "LIBRARY_INCOMPLETE_ROOT": "/dupes/Incompletes",
        }
        with mock.patch.object(pmda, "_effective_files_source_rows", return_value=[]):
            scope_roots = pmda._library_workflow_scope_roots(snapshot)

        self.assertEqual(scope_roots["library_roots"], ["/music/Music_matched"])
        self.assertEqual(scope_roots["inbox_roots"], ["/music/incomming"])
        self.assertEqual(scope_roots["scan_roots"], ["/music/Music_matched", "/music/incomming"])

    def test_files_tag_write_mode_accepts_pmda_id_only_snapshot(self):
        self.assertEqual(pmda._files_tag_write_mode({"FILES_TAG_WRITE_MODE": "pmda_id_only"}), "pmda_id_only")
        self.assertEqual(pmda._files_tag_write_mode({"FILES_TAG_WRITE_MODE": "invalid"}), "full")

    def test_build_matched_album_folder_name_with_format_and_type(self):
        self.assertEqual(
            pmda._build_matched_album_folder_name(
                artist_name="Steve Roach",
                album_title="Desert Solitaire",
                format_value="FLAC",
                album_type="Album",
                include_format=True,
                include_type=True,
            ),
            "Desert Solitaire (Flac,  Album)",
        )

    def test_move_publish_items_to_matched_library_keeps_best_and_dupes_existing_destination(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source_folder = tmp_path / "Music_dump" / "03-2026" / "01-03" / "Steve Roach - Desert Solitaire"
            existing_folder = tmp_path / "Music_matched" / "S" / "Steve Roach" / "Desert Solitaire (Mp3,  Album)"
            source_folder.mkdir(parents=True)
            existing_folder.mkdir(parents=True)
            tracks = []
            ordered_paths = []
            for idx in range(1, 8):
                track_name = f"{idx:02d} Track {idx}"
                source_track = source_folder / f"{track_name}.flac"
                existing_track = existing_folder / f"{track_name}.mp3"
                source_track.write_bytes(b"flac")
                existing_track.write_bytes(b"mp3")
                tracks.append({"title": f"Track {idx}", "idx": idx})
                ordered_paths.append(str(source_track))
            item = {
                "artist": "Steve Roach",
                "artist_name": "Steve Roach",
                "album_title": "Desert Solitaire",
                "title_raw": "Desert Solitaire",
                "folder": str(source_folder),
                "strict_match_verified": True,
                "strict_match_provider": "discogs",
                "fmt_text": "FLAC",
                "br": 0,
                "sr": 96000,
                "bd": 24,
                "tracks": tracks,
                "ordered_paths": ordered_paths,
                "meta": {},
            }
            with mock.patch.object(pmda, "LIBRARY_MODE", "files"), \
                 mock.patch.object(pmda, "PIPELINE_ENABLE_EXPORT", True), \
                 mock.patch.object(pmda, "EXPORT_ROOT", str(tmp_path / "Music_matched")), \
                 mock.patch.object(pmda, "EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER", True), \
                 mock.patch.object(pmda, "EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER", True), \
                 mock.patch.object(pmda, "DUPE_ROOT", tmp_path / "dupes"), \
                 mock.patch.object(pmda, "_files_watcher_suppress_folder", return_value=None), \
                 mock.patch.object(pmda, "_files_forget_album_folder_global", return_value=True):
                moved_items = pmda._move_publish_items_to_matched_library("Steve Roach", [item])

            expected_folder = tmp_path / "Music_matched" / "S" / "Steve Roach" / "Desert Solitaire (Flac,  Album)"
            self.assertEqual(len(moved_items), 1)
            self.assertEqual(Path(moved_items[0]["folder"]), expected_folder)
            self.assertTrue(expected_folder.exists())
            self.assertTrue((expected_folder / "01 Track 1.flac").exists())
            self.assertTrue(source_folder.exists())
            self.assertEqual(
                (source_folder / "01 Track 1.flac").stat().st_ino,
                (expected_folder / "01 Track 1.flac").stat().st_ino,
            )
            dupe_entries = list((tmp_path / "dupes").rglob("*"))
            self.assertTrue(any(p.is_dir() and "Desert Solitaire" in p.name for p in dupe_entries))

    def test_sanitize_album_title_display_normalizes_unicode_ellipsis(self):
        self.assertEqual(pmda._sanitize_album_title_display("Takk…"), "Takk...")
        self.assertEqual(pmda._sanitize_album_title_display(" Takk… "), "Takk...")

    def test_track_title_normalization_handles_icelandic_transliteration(self):
        self.assertEqual(
            pmda._norm_track_title_strict("Með blóðnasir"),
            pmda._norm_track_title_strict("Med Blodnasir"),
        )
        self.assertEqual(
            pmda._norm_track_title_strict("Sæglópur"),
            pmda._norm_track_title_strict("Saeglopur"),
        )
        self.assertEqual(
            pmda._dupe_norm_track_title("Með blóðnasir"),
            pmda._dupe_norm_track_title("Med Blodnasir"),
        )
        self.assertEqual(
            pmda._dupe_norm_track_title("Sæglópur"),
            pmda._dupe_norm_track_title("Saeglopur"),
        )

    def test_authoritative_primary_tags_require_strict_match_for_pmda_match_tag(self):
        tags = pmda._authoritative_primary_tags_for_publication(
            tags={"artist": "Wrong", pmda.PMDA_MATCH_PROVIDER_TAG: "lastfm", "pmda_matched": "true"},
            artist_resolved="Sigur Rós",
            album_resolved="Takk...",
            year=2005,
            genre="post-rock",
            label="EMI",
            metadata_source="discogs",
            musicbrainz_release_group_id="",
            musicbrainz_release_id="",
            discogs_release_id="42",
            lastfm_album_mbid="",
            bandcamp_album_url="",
            strict_match_verified=False,
            cover_provider="",
        )
        self.assertEqual(tags.get("primary_metadata_source"), "discogs")
        self.assertNotIn(pmda.PMDA_MATCH_PROVIDER_TAG, tags)
        self.assertNotIn("pmda_matched", tags)

    def test_authoritative_publication_cover_keeps_local_cover_for_soft_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            local_cover = folder / "cover.jpg"
            local_cover.write_bytes(b"fake-cover")
            cover_path, has_cover, provider = pmda._authoritative_publication_cover(
                folder=folder,
                item={},
                result={},
                tags={},
                artist_resolved="Slowdive",
                album_resolved="Souvlaki",
                strict_match_verified=False,
                strict_match_provider="",
                metadata_source="discogs",
                musicbrainz_release_group_id="mb-rg-1",
                musicbrainz_release_id="",
                discogs_release_id="42",
                lastfm_album_mbid="",
                bandcamp_album_url="",
                current_cover_path="",
                current_cover_provider="",
            )
        self.assertTrue(has_cover)
        self.assertEqual(provider, "local")
        self.assertEqual(Path(cover_path).name, "cover.jpg")

    def test_resolve_files_album_cover_asset_detects_local_folder_cover_even_when_pg_row_is_stale(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            local_cover = folder / "cover.jpg"
            local_cover.write_bytes(b"fake-cover")
            has_cover, cover_path = pmda._resolve_files_album_cover_asset(
                album_id=0,
                cover_path_raw="",
                folder_path_raw=str(folder),
                has_cover=False,
                persist=False,
            )
        self.assertTrue(has_cover)
        self.assertEqual(Path(cover_path).name, "cover.jpg")

    def test_authoritative_publication_cover_uses_itunes_cover_candidates(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            cache_dir = folder / "_cache"
            cache_dir.mkdir()
            cached_cover = cache_dir / "cached-cover.jpg"
            cached_cover.write_bytes(b"cached")
            captured: dict[str, object] = {}

            def fake_download(provider, url, cover_candidates=None, timeout=14):
                captured["provider"] = provider
                captured["url"] = url
                captured["cover_candidates"] = list(cover_candidates or [])
                return (b"image-bytes", "image/jpeg", str((cover_candidates or [url])[0]))

            with mock.patch.object(
                pmda,
                "_strict_payload_for_provider",
                return_value={
                    "title": "Souvlaki",
                    "artist_name": "Slowdive",
                    "collection_id": "12345",
                    "cover_url": "https://example.invalid/100x100bb.jpg",
                    "cover_candidates": [
                        "https://example.invalid/1200x1200bb.jpg",
                        "https://example.invalid/600x600bb.jpg",
                    ],
                },
            ), mock.patch.object(pmda, "_download_best_cover_image", side_effect=fake_download), mock.patch.object(
                pmda,
                "_ensure_cached_image_from_bytes",
                return_value=cached_cover,
            ):
                cover_path, has_cover, provider = pmda._authoritative_publication_cover(
                    folder=folder,
                    item={"itunes_collection_id": "12345"},
                    result={"provider_used": "itunes"},
                    tags={},
                    artist_resolved="Slowdive",
                    album_resolved="Souvlaki",
                    strict_match_verified=True,
                    strict_match_provider="itunes",
                    metadata_source="itunes",
                    musicbrainz_release_group_id="",
                    musicbrainz_release_id="",
                    discogs_release_id="",
                    lastfm_album_mbid="",
                    bandcamp_album_url="",
                    current_cover_path="",
                    current_cover_provider="",
                )

        self.assertTrue(has_cover)
        self.assertEqual(provider, "itunes")
        self.assertEqual(cover_path, str(cached_cover))
        self.assertIn("1200x1200bb", str((captured.get("cover_candidates") or [""])[0]))

    def test_verified_provider_payload_overrides_display_identity(self):
        artist, album = pmda._resolve_edition_display_identity(
            {
                "artist": "Sigur Rós",
                "title_raw": "Takk",
                "strict_match_verified": True,
                "strict_match_provider": "discogs",
                "discogs_release_id": "42",
                "_verified_artist_name": "Sigur Rós",
                "_verified_album_title": "Takk...",
            },
            default_artist="Sigur Rós",
            default_title="Takk",
        )
        self.assertEqual((artist, album), ("Sigur Rós", "Takk..."))

    def test_materialization_confidence_policy_allows_strict_musicbrainz(self):
        policy = pmda._materialization_confidence_policy(
            {
                "strict_match_verified": True,
                "strict_match_provider": "musicbrainz",
                "musicbrainz_id": "rg-123",
                "strict_tracklist_score": 0.91,
            }
        )

        self.assertEqual(policy["tier"], "strict_mb")
        self.assertTrue(policy["auto_materialize"])
        self.assertGreaterEqual(policy["confidence"], 0.99)

    def test_materialization_confidence_policy_allows_strong_provider(self):
        policy = pmda._materialization_confidence_policy(
            {
                "strict_match_verified": True,
                "strict_match_provider": "bandcamp",
                "bandcamp_album_url": "https://artist.bandcamp.com/album/release",
                "strict_tracklist_score": 0.93,
            }
        )

        self.assertEqual(policy["tier"], "strong_provider")
        self.assertTrue(policy["auto_materialize"])
        self.assertGreaterEqual(policy["confidence"], 0.95)

    def test_materialization_confidence_policy_holds_soft_provider_for_review(self):
        policy = pmda._materialization_confidence_policy(
            {
                "strict_match_verified": False,
                "metadata_source": "discogs",
                "discogs_release_id": "12345",
            }
        )

        self.assertEqual(policy["tier"], "soft_provider")
        self.assertFalse(policy["auto_materialize"])
        self.assertEqual(policy["reason"], "trusted_provider_id_without_strict_tracklist")

    def test_collapse_files_publication_candidates_keeps_single_winner(self):
        candidates = [
            {
                "item": {
                    "folder": "/music/sigur_ros/takk_mp3",
                    "pre_missing_required_tags": [],
                    "pre_has_cover": True,
                    "pre_has_artist_image": True,
                    "bd": 16,
                    "sr": 44100,
                    "br": 320000,
                },
                "row": {
                    "folder_path": "/music/sigur_ros/takk_mp3",
                    "artist_name": "Sigur Rós",
                    "album_title": "Takk...",
                    "strict_match_verified": True,
                    "musicbrainz_release_group_id": "mb-rg-1",
                    "musicbrainz_release_id": "",
                    "discogs_release_id": "",
                    "lastfm_album_mbid": "",
                    "bandcamp_album_url": "",
                    "has_cover": True,
                    "format": "MP3",
                    "strict_tracklist_score": 1.0,
                    "track_count": 11,
                    "total_duration_sec": 3900,
                },
            },
            {
                "item": {
                    "folder": "/music/sigur_ros/takk_flac",
                    "pre_missing_required_tags": [],
                    "pre_has_cover": True,
                    "pre_has_artist_image": True,
                    "bd": 24,
                    "sr": 96000,
                    "br": 0,
                    "strict_match_verified": True,
                },
                "row": {
                    "folder_path": "/music/sigur_ros/takk_flac",
                    "artist_name": "Sigur Rós",
                    "album_title": "Takk...",
                    "strict_match_verified": True,
                    "musicbrainz_release_group_id": "mb-rg-1",
                    "musicbrainz_release_id": "",
                    "discogs_release_id": "",
                    "lastfm_album_mbid": "",
                    "bandcamp_album_url": "",
                    "has_cover": True,
                    "format": "FLAC",
                    "strict_tracklist_score": 1.0,
                    "track_count": 11,
                    "total_duration_sec": 3900,
                },
            },
        ]
        rows, hidden = pmda._collapse_files_publication_candidates("Sigur Rós", candidates)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["folder_path"], "/music/sigur_ros/takk_flac")
        self.assertEqual(hidden, {"/music/sigur_ros/takk_mp3"})

    def test_collapse_files_publication_candidates_hides_non_strict_shadowed_variant(self):
        candidates = [
            {
                "item": {"folder": "/music/sigur_ros/takk_ref"},
                "row": {
                    "folder_path": "/music/sigur_ros/takk_ref",
                    "artist_name": "Sigur Rós",
                    "album_title": "Takk...",
                    "strict_match_verified": True,
                    "musicbrainz_release_group_id": "mb-rg-1",
                    "musicbrainz_release_id": "",
                    "discogs_release_id": "",
                    "lastfm_album_mbid": "",
                    "bandcamp_album_url": "",
                    "has_cover": True,
                    "format": "FLAC",
                },
            },
            {
                "item": {"folder": "/music/sigur_ros/takk_no_tags"},
                "row": {
                    "folder_path": "/music/sigur_ros/takk_no_tags",
                    "artist_name": "Sigur Rós",
                    "album_title": "Takk...",
                    "strict_match_verified": False,
                    "musicbrainz_release_group_id": "",
                    "musicbrainz_release_id": "",
                    "discogs_release_id": "",
                    "lastfm_album_mbid": "",
                    "bandcamp_album_url": "",
                    "strict_reject_reason": "album_mismatch",
                    "has_cover": False,
                    "format": "MP3",
                },
            },
        ]
        rows, hidden = pmda._collapse_files_publication_candidates("Sigur Rós", candidates)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["folder_path"], "/music/sigur_ros/takk_ref")
        self.assertEqual(hidden, {"/music/sigur_ros/takk_no_tags"})

    def test_strict_identity_key_collapses_cross_provider_same_album(self):
        discogs_key = pmda._strict_album_identity_key(
            artist_name="Sigur Rós",
            album_title="Takk...",
            strict_match_verified=True,
            discogs_release_id="42",
        )
        lastfm_key = pmda._strict_album_identity_key(
            artist_name="Sigur Rós",
            album_title="Takk...",
            strict_match_verified=True,
            lastfm_album_mbid="mbid-1",
        )
        self.assertEqual(discogs_key, lastfm_key)
        self.assertTrue(discogs_key.startswith("strict-title:"))

    def test_files_reset_rebuild_tables_uses_delete_not_truncate(self):
        executed = []

        class _Cursor:
            def execute(self, sql, params=None):
                executed.append(str(sql or "").strip())

        pmda._files_reset_rebuild_tables(_Cursor())
        self.assertEqual(
            executed,
            [
                "DELETE FROM files_tracks",
                "DELETE FROM files_artist_album_links",
                "DELETE FROM files_artist_aliases",
                "DELETE FROM files_albums",
                "DELETE FROM files_artists",
            ],
        )
        self.assertTrue(all("TRUNCATE" not in sql.upper() for sql in executed))

    def test_files_index_api_rebuild_prefers_published_rows_for_mirror_workflow(self):
        with mock.patch.object(
            pmda,
            "_library_workflow_state",
            return_value={
                "mode": "mirror",
                "serving_root": "/music/Music_matched",
                "source_roots": ["/music/Music_matched"],
            },
        ):
            use_published, source, forced, reason = pmda._files_index_payload_source_decision(
                "api_rebuild",
                published_count=65000,
                files_scan_running=False,
            )

        self.assertTrue(use_published)
        self.assertEqual(source, "published_rows")
        self.assertFalse(forced)
        self.assertEqual(reason, "")

    def test_files_index_explicit_filesystem_rebuild_uses_trusted_roots_for_mirror_workflow(self):
        with mock.patch.object(
            pmda,
            "_library_workflow_state",
            return_value={
                "mode": "mirror",
                "serving_root": "/music/Music_matched",
                "source_roots": ["/music/Music_matched"],
            },
        ):
            use_published, source, forced, reason = pmda._files_index_payload_source_decision(
                "api_rebuild_filesystem",
                published_count=65000,
                files_scan_running=False,
            )

        self.assertFalse(use_published)
        self.assertEqual(source, "filesystem_roots")
        self.assertTrue(forced)
        self.assertEqual(reason, "mirror_trusted_library_roots")

    def test_files_index_scan_started_keeps_published_rows_fast_path(self):
        with mock.patch.object(
            pmda,
            "_library_workflow_state",
            return_value={
                "mode": "mirror",
                "serving_root": "/music/Music_matched",
                "source_roots": ["/music/Music_matched"],
            },
        ):
            use_published, source, forced, reason = pmda._files_index_payload_source_decision(
                "scan_started",
                published_count=65000,
                files_scan_running=True,
            )

        self.assertTrue(use_published)
        self.assertEqual(source, "published_rows")
        self.assertFalse(forced)
        self.assertEqual(reason, "")

    def test_files_index_scan_completed_uses_published_rows_for_mirror_workflow(self):
        with mock.patch.object(
            pmda,
            "_library_workflow_state",
            return_value={
                "mode": "mirror",
                "serving_root": "/music/Music_matched",
                "source_roots": ["/music/Music_matched"],
            },
        ):
            use_published, source, forced, reason = pmda._files_index_payload_source_decision(
                "publication_reconcile_scan_completed",
                published_count=65000,
                files_scan_running=False,
            )

        self.assertTrue(use_published)
        self.assertEqual(source, "published_rows")
        self.assertFalse(forced)
        self.assertEqual(reason, "")

    def test_files_index_managed_api_rebuild_uses_published_rows_when_available(self):
        with mock.patch.object(
            pmda,
            "_library_workflow_state",
            return_value={
                "mode": "managed",
                "serving_root": "/music/Music_matched",
                "source_roots": [],
            },
        ):
            use_published, source, forced, reason = pmda._files_index_payload_source_decision(
                "api_rebuild",
                published_count=65000,
                files_scan_running=False,
            )

        self.assertTrue(use_published)
        self.assertEqual(source, "published_rows")
        self.assertFalse(forced)
        self.assertEqual(reason, "")

    def test_files_index_mirror_needs_filesystem_rebuild_after_published_rows_full_source(self):
        with pmda.lock:
            previous_scanning = bool(pmda.state.get("scanning"))
            pmda.state["scanning"] = False
        try:
            with mock.patch.object(pmda, "_get_library_mode", return_value="files"), mock.patch.object(
                pmda,
                "_library_workflow_state",
                return_value={
                    "mode": "mirror",
                    "serving_root": "/music/Music_matched",
                    "source_roots": ["/music/Music_matched"],
                },
            ):
                needs, source = pmda._files_index_needs_mirror_filesystem_rebuild("published_rows")

            self.assertTrue(needs)
            self.assertEqual(source, "published_rows")
        finally:
            with pmda.lock:
                pmda.state["scanning"] = previous_scanning

    def test_files_index_mirror_does_not_rebuild_after_filesystem_full_source(self):
        with pmda.lock:
            previous_scanning = bool(pmda.state.get("scanning"))
            pmda.state["scanning"] = False
        try:
            with mock.patch.object(pmda, "_get_library_mode", return_value="files"), mock.patch.object(
                pmda,
                "_library_workflow_state",
                return_value={
                    "mode": "mirror",
                    "serving_root": "/music/Music_matched",
                    "source_roots": ["/music/Music_matched"],
                },
            ):
                needs, source = pmda._files_index_needs_mirror_filesystem_rebuild("filesystem_roots")

            self.assertFalse(needs)
            self.assertEqual(source, "filesystem_roots")
        finally:
            with pmda.lock:
                pmda.state["scanning"] = previous_scanning

    def test_merge_artist_album_links_dedupes_conflicts_before_repointing_losers(self):
        calls = []

        class _Cursor:
            def execute(self, sql, params=None):
                calls.append((str(sql or ""), params))

        pmda._files_merge_artist_album_links_to_winner(_Cursor(), winner_id=10, loser_ids=[11, 12])

        self.assertEqual(len(calls), 2)
        self.assertIn("WITH loser_links AS", calls[0][0])
        self.assertIn("BOOL_OR", calls[0][0])
        self.assertIn("ON CONFLICT (artist_id, album_id, role) DO UPDATE", calls[0][0])
        self.assertIn("DELETE FROM files_artist_album_links WHERE artist_id", calls[1][0])

    def test_files_should_preserve_live_index_for_scan_on_explicit_or_unfinished_resume(self):
        with mock.patch.object(pmda, "_get_library_mode", return_value="files"):
            self.assertTrue(pmda._files_should_preserve_live_index_for_scan("full", "resume-123"))
            with mock.patch.object(pmda, "_has_unfinished_resume_run", return_value=True):
                self.assertTrue(pmda._files_should_preserve_live_index_for_scan("full", None))
            with mock.patch.object(pmda, "_has_unfinished_resume_run", return_value=False):
                self.assertFalse(pmda._files_should_preserve_live_index_for_scan("full", None))
                self.assertFalse(pmda._files_should_preserve_live_index_for_scan("changed_only", None))

    def test_reset_files_live_index_for_scan_respects_preserve_flag(self):
        with pmda.lock:
            prev = bool(pmda.state.get("scan_preserve_live_index"))
            pmda.state["scan_preserve_live_index"] = True
        try:
            with mock.patch.object(pmda, "_clear_files_library_published_rows") as clear_rows:
                pmda._reset_files_live_index_for_scan()
            clear_rows.assert_not_called()
        finally:
            with pmda.lock:
                pmda.state["scan_preserve_live_index"] = prev

    def test_run_files_profile_backfill_uses_aliased_files_artists_query(self):
        executed = []

        class _Cursor:
            def __init__(self):
                self._last_sql = ""

            def execute(self, sql, params=None):
                self._last_sql = str(sql or "")
                executed.append(self._last_sql)

            def fetchone(self):
                if "to_regclass('public.files_artists')" in self._last_sql:
                    return ("files_artists", "files_albums", "files_artist_album_links")
                return None

            def fetchall(self):
                if "FROM files_artist_album_links" in self._last_sql:
                    return []
                return []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class _Conn:
            def cursor(self):
                return _Cursor()

            def close(self):
                return None

        with mock.patch.object(pmda, "_files_pg_connect", return_value=_Conn()):
            pmda._run_files_profile_backfill(reason="test", sleep_sec=0.0)

        first_select = next(sql for sql in executed if "files_artist_profiles" in sql)
        self.assertIn("FROM files_artists a", first_select)


if __name__ == "__main__":
    unittest.main()
