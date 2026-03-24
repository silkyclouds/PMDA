import unittest

import pmda


class ProviderIdentityArbitrationTests(unittest.TestCase):
    def setUp(self):
        self._orig_ai_ready = bool(getattr(pmda, "ai_provider_ready", False))
        self._orig_use_ai = bool(getattr(pmda, "PROVIDER_IDENTITY_USE_AI", True))
        pmda.ai_provider_ready = False
        pmda.PROVIDER_IDENTITY_USE_AI = False

    def tearDown(self):
        pmda.ai_provider_ready = self._orig_ai_ready
        pmda.PROVIDER_IDENTITY_USE_AI = self._orig_use_ai

    def test_symbol_only_identity_normalization_and_score(self):
        self.assertEqual(pmda._normalize_identity_text_strict("!!!"), "sym:!!!")
        self.assertGreaterEqual(pmda._provider_identity_text_score("!!!", "!!!"), 0.99)

    def test_arbitration_accepts_soft_safe_match_for_edition_variant(self):
        payloads = {
            "discogs": {
                "title": "Same Album (Deluxe Edition)",
                "artist_name": "Test Artist",
                "tracklist": ["Intro", "Song A", "Song B", "Bonus Track"],
                "release_id": "12345",
                "master_id": "12345",
            }
        }
        result = pmda._arbitrate_provider_identity(
            artist_name="Test Artist",
            album_title="Same Album",
            local_track_titles=["Intro", "Song A", "Song B"],
            provider_payloads=payloads,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.get("provider"), "discogs")
        self.assertTrue(bool(result.get("soft_match_verified")))
        self.assertFalse(bool(result.get("strict_match_verified")))

    def test_arbitration_rejects_incompatible_tracklists(self):
        payloads = {
            "discogs": {
                "title": "Same Album",
                "artist_name": "Test Artist",
                "tracklist": ["Completely Different A", "Completely Different B"],
                "release_id": "999",
                "master_id": "999",
            }
        }
        result = pmda._arbitrate_provider_identity(
            artist_name="Test Artist",
            album_title="Same Album",
            local_track_titles=["Intro", "Song A", "Song B", "Song C"],
            provider_payloads=payloads,
        )
        self.assertIsNone(result)

    def test_arbitration_rejects_weak_no_tracklist_lastfm_candidate(self):
        payloads = {
            "lastfm": {
                "title": "Takk...",
                "artist": "Slowdive",
                "mbid": "fake-lastfm-id",
            }
        }
        result = pmda._arbitrate_provider_identity(
            artist_name="Slowdive",
            album_title="Souvlaki",
            local_track_titles=["Alison", "Machine Gun", "40 Days", "Sing"],
            provider_payloads=payloads,
        )
        self.assertIsNone(result)

    def test_arbitration_rejects_exact_title_artist_no_tracklist_for_full_album(self):
        payloads = {
            "lastfm": {
                "title": "Takk...",
                "artist": "Slowdive",
                "mbid": "fake-lastfm-id",
            }
        }
        result = pmda._arbitrate_provider_identity(
            artist_name="Slowdive",
            album_title="Takk...",
            local_track_titles=[
                "Alison",
                "Machine Gun",
                "40 Days",
                "Sing",
                "Here She Comes",
            ],
            provider_payloads=payloads,
        )
        self.assertIsNone(result)

    def test_arbitration_accepts_near_perfect_top_candidate_without_ai(self):
        candidates = [
            {
                "provider": "discogs",
                "payload": {"title": "Takk…", "artist_name": "Sigur Rós"},
                "provider_id": "discogs-1",
                "title_score": 1.0,
                "artist_score": 1.0,
                "ocr_title_score": 0.0,
                "ocr_artist_score": 0.0,
                "track_score": 1.0,
                "confidence": 0.97,
                "title": "Takk…",
                "artist": "Sigur Rós",
                "strict_match_verified": False,
                "strict_reject_reason": "",
                "strict_tracklist_score": 0.95,
                "has_provider_tracklist": True,
                "has_local_tracklist": True,
                "provider_track_count": 11,
                "local_track_count": 11,
                "track_count_ratio": 1.0,
                "classical_guard_applies": False,
                "classical_guard_ok": True,
            },
            {
                "provider": "lastfm",
                "payload": {"title": "Takk…", "artist": "Sigur Rós"},
                "provider_id": "lastfm-1",
                "title_score": 0.99,
                "artist_score": 0.99,
                "ocr_title_score": 0.0,
                "ocr_artist_score": 0.0,
                "track_score": 0.96,
                "confidence": 0.95,
                "title": "Takk…",
                "artist": "Sigur Rós",
                "strict_match_verified": False,
                "strict_reject_reason": "",
                "strict_tracklist_score": 0.90,
                "has_provider_tracklist": True,
                "has_local_tracklist": True,
                "provider_track_count": 11,
                "local_track_count": 11,
                "track_count_ratio": 1.0,
                "classical_guard_applies": False,
                "classical_guard_ok": True,
            },
        ]
        with unittest.mock.patch.object(pmda, "_build_provider_identity_candidates", return_value=candidates):
            result = pmda._arbitrate_provider_identity(
                artist_name="Sigur Rós",
                album_title="Takk…",
                local_track_titles=["track"] * 11,
                provider_payloads={},
            )
        self.assertIsNotNone(result)
        self.assertEqual(result.get("provider"), "discogs")
        self.assertEqual(result.get("confidence_source"), "heuristic")

    def test_unverified_ai_lookup_hint_does_not_override_display_identity(self):
        edition = {
            "artist": "Sigur Rós",
            "title_raw": "Takk...",
            "_lookup_artist_name": "Slowdive",
            "_lookup_album_title": "Souvlaki",
            "_lookup_identity_hint": {
                "artist": "Slowdive",
                "album": "Souvlaki",
                "confidence": 92,
                "reason": "track titles point there",
                "source": "ai_local_context",
            },
        }
        artist, album = pmda._resolve_edition_display_identity(edition)
        self.assertEqual(artist, "Sigur Rós")
        self.assertEqual(album, "Takk...")

    def test_filename_pattern_hint_is_safe_for_provider_lookup(self):
        edition = {
            "artist": "Unknown Artist",
            "title_raw": "Unknown Album",
            "_lookup_artist_name": "Slowdive",
            "_lookup_album_title": "Souvlaki",
            "_lookup_identity_hint": {
                "artist": "Slowdive",
                "album": "Souvlaki",
                "confidence": 96,
                "reason": "stable filename pattern (artist_missing_or_generic, album_missing_or_generic)",
                "source": "filename_pattern",
            },
            "missing_required_tags": ["artist", "album"],
        }
        self.assertTrue(
            pmda._identity_hint_safe_for_provider_lookup(
                edition,
                default_artist="Unknown Artist",
                default_title="Unknown Album",
            )
        )

    def test_ai_lookup_hint_not_used_for_provider_lookup_when_local_identity_specific(self):
        edition = {
            "artist": "Sigur Rós",
            "title_raw": "Takk...",
            "_lookup_artist_name": "Slowdive",
            "_lookup_album_title": "Souvlaki",
            "_lookup_identity_hint": {
                "artist": "Slowdive",
                "album": "Souvlaki",
                "confidence": 92,
                "reason": "track titles point there",
                "source": "ai_local_context",
            },
        }
        self.assertFalse(
            pmda._identity_hint_safe_for_provider_lookup(
                edition,
                default_artist="Sigur Rós",
                default_title="Takk...",
            )
        )

    def test_musicbrainz_only_soft_identity_does_not_survive_artist_mismatch(self):
        edition = {
            "identity_provider": "musicbrainz",
            "primary_metadata_source": "musicbrainz",
            "metadata_source": "musicbrainz",
            "musicbrainz_id": "fake-mbid",
            "strict_reject_reason": "artist_mismatch",
            "provider_identity_soft_match": False,
        }
        self.assertFalse(pmda._edition_soft_identity_survives_strict_reject(edition))

    def test_extract_identity_fields_uses_provider_ids_without_strict_flag(self):
        identity = pmda._extract_files_identity_fields(
            edition={"discogs_release_id": "42", "metadata_source": "discogs"},
            tags={},
            cached={},
        )
        self.assertTrue(bool(identity.get("has_identity")))
        self.assertEqual(identity.get("identity_provider"), "discogs")

    def test_prescan_snapshot_runs_while_scan_is_paused(self):
        rows_written = []

        def fake_build(item, scan_id=None, now_ts=None):
            return {"folder_path": str(item.get("folder") or "/tmp/album")}

        def fake_upsert(rows):
            rows_written.extend(rows)

        class _AlwaysPaused:
            @staticmethod
            def is_set():
                return True

        class _NeverStop:
            @staticmethod
            def is_set():
                return False

        orig_build = pmda._build_files_cache_row_from_prescan_item
        orig_upsert = pmda._upsert_files_album_scan_cache_rows
        orig_stop = pmda.scan_should_stop
        try:
            pmda._build_files_cache_row_from_prescan_item = fake_build
            pmda._upsert_files_album_scan_cache_rows = fake_upsert
            pmda.scan_should_stop = _NeverStop()
            result = pmda._snapshot_files_album_scan_cache_from_prescan(
                {1: {"folder": "/music/artist/album"}},
                scan_id=123,
                reason="test",
                batch_size=1,
                pause_event=_AlwaysPaused(),
                respect_pause=False,
            )
        finally:
            pmda._build_files_cache_row_from_prescan_item = orig_build
            pmda._upsert_files_album_scan_cache_rows = orig_upsert
            pmda.scan_should_stop = orig_stop
        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(int(result.get("rows_upserted") or 0), 1)
        self.assertEqual(len(rows_written), 1)


if __name__ == "__main__":
    unittest.main()
