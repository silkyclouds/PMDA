import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pmda


class ConservativeIncompleteDetectionTests(unittest.TestCase):
    def _edition(self, rows, title="Album") -> dict:
        return {
            "title_raw": title,
            "tracks": list(rows or []),
        }

    def test_detect_broken_album_ignores_leading_gap(self):
        tracks = [
            pmda.Track("track eleven", 11, 1, 1000),
        ]
        is_broken, expected, actual, missing = pmda.detect_broken_album(
            None,
            album_id=1,
            tracks=tracks,
            mb_release_group_info={"track_count": 12, "source": "provider_tracklist"},
            tags={"tracknumber": "11/12"},
        )
        self.assertFalse(is_broken)
        self.assertIsNone(expected)
        self.assertEqual(actual, 1)
        self.assertEqual(missing, [])

    def test_detect_broken_album_marks_internal_gap_only(self):
        tracks = [
            pmda.Track("track one", 1, 1, 1000),
            pmda.Track("track two", 2, 1, 1000),
            pmda.Track("track four", 4, 1, 1000),
        ]
        is_broken, expected, actual, missing = pmda.detect_broken_album(
            None,
            album_id=2,
            tracks=tracks,
            mb_release_group_info={"track_count": 4, "source": "provider_tracklist"},
            tags={"tracknumber": "1/4"},
        )
        self.assertTrue(is_broken)
        self.assertEqual(expected, 4)
        self.assertEqual(actual, 3)
        self.assertEqual(missing, [3])

    def test_detect_broken_album_ignores_multidisc_numbering(self):
        tracks = [
            pmda.Track("disc1-1", 1, 1, 1000),
            pmda.Track("disc1-2", 2, 1, 1000),
            pmda.Track("disc2-1", 1, 2, 1000),
            pmda.Track("disc2-3", 3, 2, 1000),
        ]
        is_broken, expected, actual, missing = pmda.detect_broken_album(
            None,
            album_id=3,
            tracks=tracks,
            mb_release_group_info={"track_count": 5, "source": "provider_tracklist"},
            tags=None,
        )
        self.assertFalse(is_broken)
        self.assertIsNone(expected)
        self.assertEqual(actual, 4)
        self.assertEqual(missing, [])

    def test_assessment_confirms_zero_local_tracks(self):
        assessment = pmda._build_incomplete_assessment(
            edition=self._edition([]),
            tags=None,
            mb_hint=None,
            is_broken_detected=True,
            expected_track_count=0,
            actual_track_count=0,
            missing_indices=[],
            strict_reject_reason="",
        )
        self.assertEqual(assessment["verdict"], "confirmed_incomplete")
        self.assertTrue(assessment["mark_broken"])
        self.assertTrue(assessment["quarantine_eligible"])

    def test_assessment_confirms_obvious_local_gap(self):
        assessment = pmda._build_incomplete_assessment(
            edition=self._edition(
                [
                    {"title": "One", "index": 1},
                    {"title": "Two", "index": 2},
                    {"title": "Four", "index": 4},
                ]
            ),
            tags=None,
            mb_hint={"track_count": 4, "track_titles": ["One", "Two", "Three", "Four"]},
            is_broken_detected=True,
            expected_track_count=4,
            actual_track_count=3,
            missing_indices=[3],
            strict_reject_reason="track_count_mismatch",
        )
        self.assertEqual(assessment["verdict"], "confirmed_incomplete")
        self.assertTrue(assessment["mark_broken"])
        self.assertTrue(assessment["quarantine_eligible"])
        self.assertEqual(assessment["missing_indices"], [3])

    def test_assessment_ignores_provider_only_shortfall(self):
        assessment = pmda._build_incomplete_assessment(
            edition=self._edition(
                [
                    {"title": "One", "index": 1},
                    {"title": "Two", "index": 2},
                ]
            ),
            tags=None,
            mb_hint={"track_count": 3, "track_titles": ["One", "Two", "Three"]},
            is_broken_detected=False,
            expected_track_count=3,
            actual_track_count=2,
            missing_indices=[],
            strict_reject_reason="track_count_mismatch",
        )
        self.assertFalse(assessment["mark_broken"])
        self.assertFalse(assessment["needs_manual_review"])
        self.assertIn("no obvious local numbering holes", assessment["summary"].lower())

    def test_assessment_from_payload_uses_local_tracks_only(self):
        payload = {
            "album_title": "Album",
            "local_tracks": [
                {"title": "One", "track_num": 1},
                {"title": "Two", "track_num": 2},
            ],
            "expected_tracks": [
                {"title": "One", "track_num": 1},
                {"title": "Two", "track_num": 2},
                {"title": "Three", "track_num": 3},
                {"title": "Four", "track_num": 4},
            ],
            "strict_reject_reason": "track_count_mismatch",
        }
        assessment = pmda._build_incomplete_assessment_from_payload(payload)
        self.assertEqual(assessment["verdict"], "current_folder_matches_expected_not_incomplete")
        self.assertFalse(assessment["mark_broken"])
        self.assertFalse(assessment["needs_manual_review"])

    def test_assessment_from_payload_confirms_internal_gap(self):
        payload = {
            "album_title": "Album",
            "local_tracks": [
                {"title": "One", "track_num": 1},
                {"title": "Two", "track_num": 2},
                {"title": "Four", "track_num": 4},
            ],
            "expected_tracks": [
                {"title": "One", "track_num": 1},
                {"title": "Two", "track_num": 2},
                {"title": "Three", "track_num": 3},
                {"title": "Four", "track_num": 4},
            ],
            "strict_reject_reason": "track_count_mismatch",
        }
        assessment = pmda._build_incomplete_assessment_from_payload(payload)
        self.assertEqual(assessment["verdict"], "confirmed_incomplete")
        self.assertTrue(assessment["mark_broken"])
        self.assertEqual(assessment["missing_indices"], [3])

    def test_broken_album_meta_match_requires_same_folder_when_available(self):
        self.assertTrue(
            pmda._broken_album_meta_matches_snapshot(
                "Version 1",
                "/music/incomming/Wereshark - Version 1 (2008) [FLAC]",
                "Version 1",
                "/music/incomming/Wereshark - Version 1 (2008) [FLAC]",
            )
        )
        self.assertFalse(
            pmda._broken_album_meta_matches_snapshot(
                "Version 1",
                "/music/incomming/Wereshark - Version 1 (2008) [FLAC]",
                "Forms of Hands 02",
                "/music/incomming/Various Artists - Forms of Hands 02 (2002) [FLAC]",
            )
        )

    def test_broken_album_snapshot_anchor_uses_trace_when_stored_row_is_placeholder(self):
        title, folder = pmda._broken_album_snapshot_anchor(
            "Album 15",
            "",
            trace_title="Headz 2 Sampler",
            trace_folder="/music/incomming/DJ Krush & Zimbabwe Legit - Headz 2 Sampler [1996] [Vinyl FLAC]",
            edition_title="Headz 2 Sampler",
            edition_folder="/music/incomming/DJ Krush & Zimbabwe Legit - Headz 2 Sampler [1996] [Vinyl FLAC]",
        )
        self.assertEqual(title, "Headz 2 Sampler")
        self.assertEqual(folder, "/music/incomming/DJ Krush & Zimbabwe Legit - Headz 2 Sampler [1996] [Vinyl FLAC]")

    def test_broken_album_detail_stays_cache_only_for_provider_reference(self):
        with tempfile.TemporaryDirectory(prefix="pmda-broken-detail-") as tmp:
            tmp_path = Path(tmp)
            state_db = tmp_path / "state.db"
            settings_db = tmp_path / "settings.db"
            album_dir = tmp_path / "Artist - Album"
            album_dir.mkdir()
            (album_dir / "01 Intro.flac").write_bytes(b"")
            (album_dir / "02 Outro.flac").write_bytes(b"")

            original_state_db = pmda.STATE_DB_FILE
            original_settings_db = pmda.SETTINGS_DB_FILE
            try:
                pmda.STATE_DB_FILE = state_db
                pmda.SETTINGS_DB_FILE = settings_db
                pmda.init_state_db()
                pmda.init_settings_db()
                con = sqlite3.connect(state_db)
                cur = con.cursor()
                cur.execute(
                    """
                    INSERT INTO broken_albums (
                        artist, album_id, expected_track_count, actual_track_count,
                        missing_indices, musicbrainz_release_group_id, detected_at,
                        sent_to_lidarr, review_status, album_title, folder_path,
                        metadata_source, strict_match_provider, strict_reject_reason,
                        provider_refs_json, reason_summary, local_tracks_json,
                        expected_tracks_json, missing_required_tags_json,
                        classification, classification_confidence, classification_source,
                        quarantine_eligible, evidence_json, ai_verdict_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "Artist",
                        55,
                        2,
                        1,
                        json.dumps([2]),
                        "",
                        0.0,
                        0,
                        "",
                        "Album",
                        str(album_dir),
                        "lastfm",
                        "",
                        "track_count_mismatch",
                        json.dumps({}),
                        "",
                        json.dumps([]),
                        json.dumps([]),
                        json.dumps([]),
                        "",
                        0.0,
                        "",
                        1,
                        json.dumps({}),
                        json.dumps({}),
                    ),
                )
                con.commit()
                con.close()

                def _fake_expected_tracks(*, cache_only: bool = True, **kwargs):
                    if not cache_only:
                        raise AssertionError("broken album detail attempted live provider lookup")
                    return []

                with mock.patch.object(pmda, "_files_pg_connect", return_value=None), \
                     mock.patch.object(pmda, "_scan_move_expected_tracks", side_effect=_fake_expected_tracks):
                    with pmda.app.test_request_context("/api/broken-albums/detail?artist=Artist&album_id=55"):
                        response = pmda.api_broken_album_detail()
                payload = response.get_json()
                self.assertEqual(payload["artist"], "Artist")
                self.assertEqual(payload["album_id"], 55)
                self.assertEqual(len(payload["local_tracks"]), 2)
            finally:
                pmda.STATE_DB_FILE = original_state_db
                pmda.SETTINGS_DB_FILE = original_settings_db


if __name__ == "__main__":
    unittest.main()
