import unittest
from unittest import mock
from pathlib import Path

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

    def test_date_like_container_folder_is_not_usable_as_artist(self):
        self.assertFalse(pmda._identity_artist_fallback_is_usable("09-03"))
        self.assertFalse(pmda._identity_artist_fallback_is_usable("01-2002"))
        self.assertFalse(pmda._identity_artist_fallback_is_usable("2020-09"))
        self.assertFalse(pmda._identity_artist_fallback_is_usable("2026-03-09"))
        self.assertFalse(pmda._identity_artist_fallback_is_usable("(2)"))
        self.assertFalse(pmda._identity_artist_fallback_is_usable("/_"))
        self.assertFalse(pmda._identity_artist_fallback_is_usable("01"))
        self.assertFalse(pmda._identity_artist_fallback_is_usable("002"))
        self.assertFalse(pmda._identity_artist_fallback_is_usable("01 DJ Excalibah"))
        self.assertFalse(pmda._identity_artist_fallback_is_usable("01-Kid Commando"))
        self.assertTrue(pmda._identity_artist_fallback_is_usable("Slowdive"))
        self.assertTrue(pmda._identity_artist_fallback_is_usable("(Ghost)"))
        self.assertTrue(pmda._identity_artist_fallback_is_usable("2 Unlimited"))

    def test_track_prefixed_artist_artifact_repairs_to_real_artist_name(self):
        self.assertEqual(pmda._identity_artist_fallback_candidate("01 DJ Excalibah"), "DJ Excalibah")
        self.assertEqual(pmda._identity_artist_fallback_candidate("01-Kid Commando"), "Kid Commando")
        self.assertEqual(pmda._identity_artist_fallback_candidate("1-01. Sasha"), "Sasha")
        self.assertEqual(pmda._identity_artist_fallback_candidate("2 Unlimited"), "2 Unlimited")
        self.assertEqual(pmda._identity_artist_fallback_candidate("220 Kid"), "220 Kid")

    def test_numeric_identity_is_generic(self):
        self.assertTrue(pmda._identity_text_is_generic("01"))
        self.assertTrue(pmda._identity_text_is_generic("002"))
        self.assertFalse(pmda._identity_text_is_generic("2 Unlimited"))

    def test_infer_artist_album_from_folder_ignores_date_like_parent_folder(self):
        folder = Path("/music/09-03/The Music of Vietnam Vol. 2")
        audio_files = [folder / "Various Artist - The Music of Vietnam Vol. 2 - 01 - Opening.flac"]
        with mock.patch.object(
            pmda,
            "extract_tags",
            side_effect=[
                {"artist": "", "albumartist": "", "album": ""},
            ],
        ):
            artist, album = pmda._infer_artist_album_from_folder(folder, audio_files)
        self.assertNotEqual(artist, "09-03")
        self.assertEqual(artist, "Various Artist")
        self.assertEqual(album, "The Music of Vietnam Vol. 2")

    def test_infer_artist_album_from_folder_uses_small_tag_consensus(self):
        folder = Path("/music/15-03/01")
        audio_files = [
            folder / "track1.flac",
            folder / "track2.flac",
            folder / "track3.flac",
        ]
        with mock.patch.object(
            pmda,
            "extract_tags",
            side_effect=[
                {"artist": "", "albumartist": "", "album": ""},
                {"artist": "Abe Duque", "albumartist": "Abe Duque", "album": "Come Back And Dance"},
                {"artist": "Abe Duque", "albumartist": "Abe Duque", "album": "Come Back And Dance"},
            ],
        ):
            artist, album = pmda._infer_artist_album_from_folder(folder, audio_files)
        self.assertEqual(artist, "Abe Duque")
        self.assertEqual(album, "Come Back And Dance")

    def test_infer_artist_album_from_folder_rejects_month_year_tag_artist(self):
        folder = Path("/music/01-2002/Jens Korndörfer - Windows of the Spirit (2020)")
        audio_files = [
            folder / "Jens Korndörfer - Windows of the Spirit - 01 - Intro.flac",
        ]
        with mock.patch.object(
            pmda,
            "extract_tags",
            side_effect=[
                {
                    "artist": "01-2002",
                    "albumartist": "01-2002",
                    "album": "Jens Korndörfer - Windows of the Spirit (2020)",
                },
            ],
        ):
            artist, album = pmda._infer_artist_album_from_folder(folder, audio_files)
        self.assertEqual(artist, "Jens Korndörfer")
        self.assertEqual(album, "Windows of the Spirit")

    def test_infer_artist_album_from_folder_repairs_track_prefixed_tag_artist(self):
        folder = Path("/music/DJ Excalibah/Extinction")
        audio_files = [
            folder / "DJ Excalibah - Extinction - 01 - Intro.flac",
        ]
        with mock.patch.object(
            pmda,
            "extract_tags",
            side_effect=[
                {
                    "artist": "01 DJ Excalibah",
                    "albumartist": "01 DJ Excalibah",
                    "album": "Extinction",
                },
            ],
        ):
            artist, album = pmda._infer_artist_album_from_folder(folder, audio_files)
        self.assertEqual(artist, "DJ Excalibah")
        self.assertEqual(album, "Extinction")

    def test_bandcamp_lookup_skips_web_fallback_during_scan(self):
        class _Resp:
            def __init__(self, text="", status_code=200):
                self.text = text
                self.status_code = status_code
                self.headers = {}

        with mock.patch.object(pmda, "_scan_inline_matching_active", return_value=True):
            with mock.patch.object(pmda, "requests") as requests_mod:
                requests_mod.get.return_value = _Resp("<html><body>no direct album links</body></html>", 200)
                with mock.patch.object(pmda, "_web_search_serper", side_effect=AssertionError("web fallback must stay disabled during scan")):
                    payload = pmda._fetch_bandcamp_album_info("Test Artist", "Test Album")
        self.assertIsNone(payload)

    def test_bandcamp_lookup_skips_web_fallback_during_scan_phase_even_if_not_inline(self):
        class _Resp:
            def __init__(self, text="", status_code=200):
                self.text = text
                self.status_code = status_code
                self.headers = {}

        with mock.patch.object(pmda, "_scan_inline_matching_active", return_value=False), \
            mock.patch.object(pmda, "_ai_infer_runtime_context", return_value={"phase": "scan"}), \
            mock.patch.object(pmda, "requests") as requests_mod:
            requests_mod.get.return_value = _Resp("<html><body>no direct album links</body></html>", 200)
            with mock.patch.object(pmda, "_web_search_serper", side_effect=AssertionError("web fallback must stay disabled during scan phase")):
                payload = pmda._fetch_bandcamp_album_info("Test Artist", "Test Album")
        self.assertIsNone(payload)

    def test_mb_search_skips_web_fallback_during_scan_even_with_grounding(self):
        with mock.patch.object(pmda, "_scan_inline_matching_active", return_value=True), \
            mock.patch.object(pmda, "USE_WEB_SEARCH_FOR_MB", True), \
            mock.patch.object(pmda, "ai_provider_ready", True), \
            mock.patch.object(pmda, "USE_AI_FOR_MB_MATCH", True), \
            mock.patch.object(pmda, "USE_AI_FOR_MB_VERIFY", False), \
            mock.patch.object(pmda, "_search_mb_rg_candidates", return_value=[]), \
            mock.patch.object(pmda, "_browse_mb_rg_by_artist", return_value=[]), \
            mock.patch.object(
                pmda,
                "_fetch_album_provider_fallbacks_parallel",
                return_value={
                    "extra_sources": [
                        {
                            "source": "bandcamp",
                            "title": "Test Album",
                            "artist_name": "Test Artist",
                        }
                    ]
                },
            ), \
            mock.patch.object(pmda, "_web_search_serper", side_effect=AssertionError("scan must not use web fallback")):
            rg_info, verified = pmda.search_mb_release_group_by_metadata(
                "Test Artist",
                pmda.norm_album("Test Album"),
                {"Track 1", "Track 2"},
                title_raw="Test Album",
            )
        self.assertIsNone(rg_info)
        self.assertFalse(bool(verified))

    def test_mb_search_skips_ai_mbid_inference_during_scan_even_with_bandcamp_grounding(self):
        with mock.patch.object(pmda, "_scan_inline_matching_active", return_value=False), \
            mock.patch.object(pmda, "_ai_infer_runtime_context", return_value={"phase": "scan"}), \
            mock.patch.object(pmda, "USE_WEB_SEARCH_FOR_MB", True), \
            mock.patch.object(pmda, "ai_provider_ready", True), \
            mock.patch.object(pmda, "USE_AI_FOR_MB_MATCH", True), \
            mock.patch.object(pmda, "USE_AI_FOR_MB_VERIFY", False), \
            mock.patch.object(pmda, "_search_mb_rg_candidates", return_value=[]), \
            mock.patch.object(pmda, "_browse_mb_rg_by_artist", return_value=[]), \
            mock.patch.object(
                pmda,
                "_fetch_album_provider_fallbacks_parallel",
                return_value={
                    "bandcamp": {
                        "title": "Test Album",
                        "artist_name": "Test Artist",
                        "album_url": "https://artist.bandcamp.com/album/test-album",
                    },
                    "extra_sources": [
                        {
                            "source": "Bandcamp",
                            "title": "Test Album",
                            "artist_name": "Test Artist",
                        }
                    ],
                },
            ), \
            mock.patch.object(pmda, "_call_ai_provider_bounded", side_effect=AssertionError("scan phase must not call AI MBID inference")):
            rg_info, verified = pmda.search_mb_release_group_by_metadata(
                "Test Artist",
                pmda.norm_album("Test Album"),
                {"Track 1", "Track 2"},
                title_raw="Test Album",
                scan_inline=False,
            )
        self.assertIsNone(rg_info)
        self.assertFalse(bool(verified))

    def test_mb_search_allows_manual_web_fallback_even_if_scan_state_exists(self):
        with mock.patch.object(pmda, "_scan_inline_matching_active", return_value=True), \
            mock.patch.object(pmda, "USE_WEB_SEARCH_FOR_MB", True), \
            mock.patch.object(pmda, "ai_provider_ready", True), \
            mock.patch.object(pmda, "USE_AI_FOR_MB_MATCH", True), \
            mock.patch.object(pmda, "USE_AI_FOR_MB_VERIFY", False), \
            mock.patch.object(pmda, "_search_mb_rg_candidates", return_value=[]), \
            mock.patch.object(pmda, "_browse_mb_rg_by_artist", return_value=[]), \
            mock.patch.object(
                pmda,
                "_fetch_album_provider_fallbacks_parallel",
                return_value={
                    "extra_sources": [
                        {
                            "source": "bandcamp",
                            "title": "Test Album",
                            "artist_name": "Test Artist",
                        }
                    ]
                },
            ), \
            mock.patch.object(pmda, "_web_search_serper", return_value=[]) as web_search:
            rg_info, verified = pmda.search_mb_release_group_by_metadata(
                "Test Artist",
                pmda.norm_album("Test Album"),
                {"Track 1", "Track 2"},
                title_raw="Test Album",
                scan_inline=False,
            )
        self.assertIsNone(rg_info)
        self.assertFalse(bool(verified))
        self.assertTrue(web_search.called)

    def test_mb_search_skips_web_fallback_during_scan_phase_even_if_not_inline(self):
        with mock.patch.object(pmda, "_scan_inline_matching_active", return_value=False), \
            mock.patch.object(pmda, "_ai_infer_runtime_context", return_value={"phase": "scan"}), \
            mock.patch.object(pmda, "USE_WEB_SEARCH_FOR_MB", True), \
            mock.patch.object(pmda, "ai_provider_ready", True), \
            mock.patch.object(pmda, "USE_AI_FOR_MB_MATCH", True), \
            mock.patch.object(pmda, "USE_AI_FOR_MB_VERIFY", False), \
            mock.patch.object(pmda, "_search_mb_rg_candidates", return_value=[]), \
            mock.patch.object(pmda, "_browse_mb_rg_by_artist", return_value=[]), \
            mock.patch.object(
                pmda,
                "_fetch_album_provider_fallbacks_parallel",
                return_value={
                    "extra_sources": [
                        {
                            "source": "bandcamp",
                            "title": "Test Album",
                            "artist_name": "Test Artist",
                        }
                    ]
                },
            ), \
            mock.patch.object(pmda, "_web_search_serper", side_effect=AssertionError("scan phase must not use web fallback")):
            rg_info, verified = pmda.search_mb_release_group_by_metadata(
                "Test Artist",
                pmda.norm_album("Test Album"),
                {"Track 1", "Track 2"},
                title_raw="Test Album",
                scan_inline=False,
            )
        self.assertIsNone(rg_info)
        self.assertFalse(bool(verified))

    def test_local_context_identity_ai_skips_scan_inline_without_force(self):
        with mock.patch.object(pmda, "_scan_inline_matching_active", return_value=True), \
            mock.patch.object(pmda, "_call_ai_provider_bounded", side_effect=AssertionError("scan inline should not call local-context AI")):
            result = pmda._infer_identity_from_local_context_ai(
                local_artist="Unknown Artist",
                local_album="Unknown Album",
                folder_path=Path("/music/test/Unknown Album"),
                track_titles=["Track 01", "Track 02"],
                file_paths=[Path("/music/test/Unknown Album/01 - Track 01.flac")],
                local_tags={"artist": "", "album": ""},
                missing_required_tags=["artist", "album"],
                force_try=False,
            )
        self.assertEqual(result, {})

    def test_local_context_identity_ai_skips_scan_phase_without_force(self):
        with mock.patch.object(pmda, "_scan_inline_matching_active", return_value=False), \
            mock.patch.object(pmda, "_ai_infer_runtime_context", return_value={"phase": "scan"}), \
            mock.patch.object(pmda, "_call_ai_provider_bounded", side_effect=AssertionError("scan phase should not call local-context AI")):
            result = pmda._infer_identity_from_local_context_ai(
                local_artist="Unknown Artist",
                local_album="Unknown Album",
                folder_path=Path("/music/test/Unknown Album"),
                track_titles=["Track 01", "Track 02"],
                file_paths=[Path("/music/test/Unknown Album/01 - Track 01.flac")],
                local_tags={"artist": "", "album": ""},
                missing_required_tags=["artist", "album"],
                force_try=False,
            )
        self.assertEqual(result, {})

    def test_provider_arbitration_skips_ai_tiebreak_during_scan_inline(self):
        pmda.ai_provider_ready = True
        pmda.PROVIDER_IDENTITY_USE_AI = True
        payloads = {
            "discogs": {
                "title": "Good Fortune",
                "artist_name": "Airborn Audio",
                "tracklist": ["House of Mirrors", "Bright Lights", "Now I Lay Me Down", "Monday Through Sunday"],
                "release_id": "123",
            },
            "lastfm": {
                "title": "Good Fortune",
                "artist": "Airborn Audio",
                "tracklist": ["House of Mirrors", "Bright Lights", "Now I Lay Me Down", "Monday Through Sunday"],
                "mbid": "",
                "url": "https://www.last.fm/music/Airborn+Audio/Good+Fortune",
            },
        }
        with mock.patch.object(pmda, "_scan_inline_matching_active", return_value=True), \
            mock.patch.object(pmda, "_call_ai_provider_bounded", side_effect=AssertionError("scan inline should not call provider AI")):
            result = pmda._arbitrate_provider_identity(
                artist_name="Airborn Audio",
                album_title="Good Fortune",
                local_track_titles=["House of Mirrors", "Bright Lights", "Now I Lay Me Down", "Monday Through Sunday"],
                provider_payloads=payloads,
            )
        self.assertIsNotNone(result)

    def test_provider_arbitration_skips_ai_tiebreak_during_scan_phase(self):
        pmda.ai_provider_ready = True
        pmda.PROVIDER_IDENTITY_USE_AI = True
        payloads = {
            "discogs": {
                "title": "Good Fortune",
                "artist_name": "Airborn Audio",
                "tracklist": [
                    "House of Mirrors",
                    "Bright Lights",
                    "Now I Lay Me Down",
                    "Monday Through Sunday",
                ],
                "release_id": "123",
            },
            "lastfm": {
                "title": "Good Fortune",
                "artist": "Airborn Audio",
                "tracklist": [
                    "House of Mirrors",
                    "Bright Lights",
                    "Now I Lay Me Down",
                    "Monday Through Sunday",
                ],
                "mbid": "",
                "url": "https://www.last.fm/music/Airborn+Audio/Good+Fortune",
            },
        }
        with mock.patch.object(pmda, "_scan_inline_matching_active", return_value=False), \
            mock.patch.object(pmda, "_ai_infer_runtime_context", return_value={"phase": "scan"}), \
            mock.patch.object(pmda, "_call_ai_provider_bounded", side_effect=AssertionError("scan phase should not call provider AI")):
            result = pmda._arbitrate_provider_identity(
                artist_name="Airborn Audio",
                album_title="Good Fortune",
                local_track_titles=["House of Mirrors", "Bright Lights", "Now I Lay Me Down", "Monday Through Sunday"],
                provider_payloads=payloads,
            )
        self.assertIsNotNone(result)

    def test_provider_arbitration_accepts_consensus_without_ai(self):
        pmda.ai_provider_ready = True
        pmda.PROVIDER_IDENTITY_USE_AI = True
        payloads = {
            "discogs": {
                "title": "Good Fortune",
                "artist_name": "Airborn Audio",
                "tracklist": [
                    "House of Mirrors",
                    "Bright Lights",
                    "Now I Lay Me Down",
                    "Monday Through Sunday",
                    "Close Your Eyes",
                    "NYC",
                    "Know Who You Are",
                    "My Eyes",
                ],
                "release_id": "123",
            },
            "lastfm": {
                "title": "Good Fortune",
                "artist": "Airborn Audio",
                "tracklist": [
                    "House of Mirrors",
                    "Bright Lights",
                    "Now I Lay Me Down",
                    "Monday Through Sunday",
                    "Close Your Eyes",
                    "NYC",
                    "Know Who You Are",
                    "My Eyes",
                ],
                "mbid": "",
                "url": "https://www.last.fm/music/Airborn+Audio/Good+Fortune",
            },
        }
        with mock.patch.object(pmda, "_call_ai_provider_bounded", side_effect=AssertionError("provider consensus should not require AI")):
            result = pmda._arbitrate_provider_identity(
                artist_name="Airborn Audio",
                album_title="Good Fortune",
                local_track_titles=[
                    "House of Mirrors",
                    "Bright Lights",
                    "Now I Lay Me Down",
                    "Monday Through Sunday",
                    "Close Your Eyes",
                    "NYC",
                    "Know Who You Are",
                    "My Eyes",
                ],
                provider_payloads=payloads,
            )
        self.assertIsNotNone(result)
        self.assertEqual(result.get("provider"), "discogs")
        self.assertTrue(bool(result.get("soft_match_verified")))
        self.assertEqual(result.get("confidence_tier"), "strong_provider")
        self.assertIsInstance(result.get("match_explanation"), dict)

    def test_arbitration_promotes_move_safe_match_for_edition_variant(self):
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
        self.assertTrue(bool(result.get("strict_match_verified")))
        self.assertEqual(result.get("confidence_source"), "strict")
        self.assertEqual(result.get("confidence_tier"), "strong_provider")

    def test_strict_provider_promotes_guest_artist_overlap_with_track_evidence(self):
        verdict = pmda._strict_provider_match_100(
            local_artist="Main Artist & Local Guest",
            local_title="Same Album",
            local_tracks=[
                {"title": "Intro", "idx": 1},
                {"title": "Song A", "idx": 2},
                {"title": "Song B", "idx": 3},
            ],
            provider="discogs",
            provider_payload={
                "title": "Same Album",
                "artist_name": "Main Artist & Provider Guest",
                "tracklist": ["Intro", "Song A", "Song B"],
                "release_id": "12345",
            },
        )
        self.assertTrue(bool(verdict.get("strict_match_verified")), verdict)
        self.assertTrue(bool(verdict.get("smart_match_verified")))
        self.assertEqual(verdict.get("smart_match_reason"), "smart_guest_artist_overlap")

    def test_strict_provider_promotes_no_tracklist_when_secondary_signal_agrees(self):
        verdict = pmda._strict_provider_match_100(
            local_artist="Exact Artist",
            local_title="Exact Album",
            local_tracks=[
                {"title": "One", "idx": 1},
                {"title": "Two", "idx": 2},
                {"title": "Three", "idx": 3},
                {"title": "Four", "idx": 4},
            ],
            local_tags={"date": "2021"},
            provider="bandcamp",
            provider_payload={
                "title": "Exact Album",
                "artist_name": "Exact Artist",
                "album_url": "https://example.bandcamp.com/album/exact-album",
                "year": "2021",
            },
        )
        self.assertTrue(bool(verdict.get("strict_match_verified")), verdict)
        self.assertEqual(verdict.get("smart_match_reason"), "smart_no_tracklist_secondary_signal")

    def test_strict_provider_rejects_no_tracklist_full_album_without_secondary_signal(self):
        verdict = pmda._strict_provider_match_100(
            local_artist="Exact Artist",
            local_title="Exact Album",
            local_tracks=[
                {"title": "One", "idx": 1},
                {"title": "Two", "idx": 2},
                {"title": "Three", "idx": 3},
                {"title": "Four", "idx": 4},
            ],
            provider="bandcamp",
            provider_payload={
                "title": "Exact Album",
                "artist_name": "Exact Artist",
                "album_url": "https://example.bandcamp.com/album/exact-album",
            },
        )
        self.assertFalse(bool(verdict.get("strict_match_verified")), verdict)
        self.assertEqual(verdict.get("strict_reject_reason"), "provider_no_tracklist")

    def test_album_score_accepts_artist_prefix_and_lp_suffix_variants(self):
        score = pmda._provider_identity_album_score(
            "Sings For Only The Lonely",
            "Frank Sinatra Sings For Only The Lonely (LP)",
            artist_hints=["Frank Sinatra"],
        )
        self.assertGreaterEqual(score, 0.99)

    def test_album_score_accepts_original_soundtrack_suffix_variant(self):
        score = pmda._provider_identity_album_score(
            "Peppered",
            "Peppered (Original Soundtrack)",
            artist_hints=["Fedya Balashov"],
        )
        self.assertGreaterEqual(score, 0.99)

    def test_artist_score_accepts_feature_clause_variant(self):
        score = pmda._provider_identity_artist_score(
            "Fletina (feat. Leo Okagawa)",
            "Fletina",
        )
        self.assertGreaterEqual(score, 0.99)

    def test_artist_score_accepts_x_collaboration_separator(self):
        score = pmda._provider_identity_artist_score(
            "Autechre x The Hafler Trio",
            "Autechre",
        )
        self.assertGreaterEqual(score, 0.90)

    def test_strict_identity_accepts_various_artists_aliases(self):
        ok, reason = pmda._strict_identity_match_details(
            local_artist="Various Artists",
            local_title="Fabric 99",
            candidate_artist="Various",
            candidate_title="Fabric 99",
        )
        self.assertTrue(ok, reason)

    def test_strict_identity_accepts_compilation_tag_with_provider_va(self):
        ok, reason = pmda._strict_identity_match_details(
            local_artist="",
            local_title="Music Of Vietnam Vol. 2",
            candidate_artist="Various Artists",
            candidate_title="Music Of Vietnam Vol. 2",
            local_tags={"compilation": "1"},
        )
        self.assertTrue(ok, reason)

    def test_album_score_accepts_soundtrack_from_series_suffix_variant(self):
        score = pmda._provider_identity_album_score(
            "Esa Noche",
            "Esa Noche (Soundtrack from the Netflix series)",
            artist_hints=["Aitor Etxebarria"],
        )
        self.assertGreaterEqual(score, 0.96)

    def test_strict_identity_accepts_equal_multi_credit_artist_sets(self):
        ok, reason = pmda._strict_identity_match_details(
            local_artist="Fabio Perletta , Asmus Tietchens",
            local_title="Integral",
            candidate_artist="Fabio Perletta & Asmus Tietchens",
            candidate_title="Integral",
        )
        self.assertTrue(ok, reason)

    def test_strict_identity_accepts_single_artist_inside_provider_cocredit(self):
        ok, reason = pmda._strict_identity_match_details(
            local_artist="F.T.C Project",
            local_title="Eight Two One EP",
            candidate_artist="F.T.C Project, Sharksss & Phineus II",
            candidate_title="Eight Two One EP - DVL004",
        )
        self.assertTrue(ok, reason)

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

    def test_arbitration_accepts_lastfm_album_page_without_mbid_for_full_album(self):
        payloads = {
            "lastfm": {
                "title": "Beyond Blood",
                "artist": "Fredrik Rojas",
                "mbid": "",
                "url": "https://www.last.fm/music/Fredrik+Rojas/Beyond+Blood",
                "lastfm_listeners": 1,
                "lastfm_scrobbles": 1,
            }
        }
        result = pmda._arbitrate_provider_identity(
            artist_name="Fredrik Rojas",
            album_title="Beyond Blood",
            local_track_titles=["Track 1", "Track 2", "Track 3", "Track 4", "Track 5"],
            provider_payloads=payloads,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.get("provider"), "lastfm")
        self.assertTrue(bool(result.get("soft_match_verified")))

    def test_arbitration_accepts_lastfm_album_page_for_title_variant_without_mbid(self):
        payloads = {
            "lastfm": {
                "title": "Lights In The Night (LP)",
                "artist": "Flash and the Pan",
                "mbid": "",
                "url": "https://www.last.fm/music/Flash+and+the+Pan/Lights+In+The+Night+(LP)",
                "lastfm_listeners": 25,
            }
        }
        result = pmda._arbitrate_provider_identity(
            artist_name="Flash And The Pan",
            album_title="Lights In The Night",
            local_track_titles=["Track 1", "Track 2", "Track 3", "Track 4", "Track 5"],
            provider_payloads=payloads,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.get("provider"), "lastfm")
        self.assertTrue(bool(result.get("soft_match_verified")))

    def test_arbitration_accepts_strong_identity_despite_small_track_count_delta(self):
        payloads = {
            "discogs": {
                "title": "In The Wild",
                "artist_name": "FaltyDL",
                "tracklist": [
                    "Track 1",
                    "Track 2",
                    "Track 3",
                    "Track 4",
                    "Track 5",
                    "Track 6",
                    "Track 7",
                    "Track 8",
                    "Track 9",
                    "Track 10",
                    "Track 11",
                ],
                "release_id": "6024003",
                "master_id": "6024003",
            }
        }
        result = pmda._arbitrate_provider_identity(
            artist_name="FaltyDL",
            album_title="In The Wild",
            local_track_titles=[
                "Track 1",
                "Track 2",
                "Track 3",
                "Track 4",
                "Track 5",
                "Track 6",
                "Track 7",
                "Track 8",
                "Track 9",
            ],
            provider_payloads=payloads,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.get("provider"), "discogs")
        self.assertTrue(bool(result.get("soft_match_verified")))

    def test_soft_identity_survives_album_mismatch_when_explicitly_verified(self):
        edition = {
            "identity_provider": "lastfm",
            "strict_reject_reason": "album_mismatch",
            "soft_match_verified": True,
        }
        self.assertTrue(pmda._edition_soft_identity_survives_strict_reject(edition))

    def test_soft_identity_survives_track_count_mismatch_when_explicitly_verified(self):
        edition = {
            "identity_provider": "discogs",
            "strict_reject_reason": "track_count_mismatch",
            "soft_match_verified": True,
            "discogs_release_id": "6024003",
        }
        self.assertTrue(pmda._edition_soft_identity_survives_strict_reject(edition))

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

    def test_provider_candidate_classification_explains_unresolved_reason(self):
        classification = pmda._provider_candidate_match_classification(
            {
                "provider": "lastfm",
                "title_score": 0.40,
                "artist_score": 0.35,
                "track_score": 0.0,
                "confidence": 0.33,
                "strict_reject_reason": "album_mismatch",
                "has_provider_tracklist": False,
                "has_local_tracklist": True,
                "local_track_count": 8,
                "provider_track_count": 0,
            },
            min_confidence=0.72,
        )
        self.assertEqual(classification.get("tier"), "unresolved")
        self.assertIn("reason", classification)

    def test_provider_candidate_classification_marks_ai_as_review_only(self):
        classification = pmda._provider_candidate_match_classification(
            {
                "provider": "discogs",
                "title_score": 0.95,
                "artist_score": 0.90,
                "track_score": 0.76,
                "confidence": 0.78,
                "provider_id": "123",
                "has_provider_tracklist": True,
                "has_local_tracklist": True,
                "local_track_count": 8,
                "provider_track_count": 9,
                "track_count_ratio": 0.88,
            },
            min_confidence=0.72,
            ai_selected=True,
        )
        self.assertEqual(classification.get("tier"), "ai_review")
        self.assertFalse(bool(classification.get("safe_for_auto_materialization")))

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
        with mock.patch.object(pmda, "_build_provider_identity_candidates", return_value=candidates):
            result = pmda._arbitrate_provider_identity(
                artist_name="Sigur Rós",
                album_title="Takk…",
                local_track_titles=["track"] * 11,
                provider_payloads={},
            )
        self.assertIsNotNone(result)
        self.assertEqual(result.get("provider"), "discogs")
        self.assertEqual(result.get("confidence_source"), "heuristic")

    def test_arbitration_skips_ai_tiebreak_for_classical_context(self):
        pmda.ai_provider_ready = True
        pmda.PROVIDER_IDENTITY_USE_AI = True
        candidates = [
            {
                "provider": "discogs",
                "payload": {"title": "Symphony no. 6", "artist_name": "Tchaikovsky"},
                "provider_id": "discogs-1",
                "title_score": 0.99,
                "artist_score": 0.99,
                "ocr_title_score": 0.0,
                "ocr_artist_score": 0.0,
                "track_score": 0.91,
                "confidence": 0.84,
                "title": "Symphony no. 6",
                "artist": "Tchaikovsky",
                "strict_match_verified": False,
                "strict_reject_reason": "",
                "strict_tracklist_score": 0.0,
                "has_provider_tracklist": True,
                "has_local_tracklist": True,
                "provider_track_count": 4,
                "local_track_count": 4,
                "track_count_ratio": 1.0,
                "classical_guard_applies": False,
                "classical_guard_ok": True,
            },
            {
                "provider": "lastfm",
                "payload": {"title": "Symphony no. 6", "artist": "Tchaikovsky"},
                "provider_id": "lastfm-1",
                "title_score": 0.98,
                "artist_score": 0.98,
                "ocr_title_score": 0.0,
                "ocr_artist_score": 0.0,
                "track_score": 0.90,
                "confidence": 0.83,
                "title": "Symphony no. 6",
                "artist": "Tchaikovsky",
                "strict_match_verified": False,
                "strict_reject_reason": "",
                "strict_tracklist_score": 0.0,
                "has_provider_tracklist": True,
                "has_local_tracklist": True,
                "provider_track_count": 4,
                "local_track_count": 4,
                "track_count_ratio": 1.0,
                "classical_guard_applies": False,
                "classical_guard_ok": True,
            },
        ]
        with mock.patch.object(pmda, "_build_provider_identity_candidates", return_value=candidates):
            with mock.patch.object(pmda, "_ai_choose_provider_identity_candidate", side_effect=AssertionError("AI should not run for classical arbitration")):
                result = pmda._arbitrate_provider_identity(
                    artist_name="Tchaikovsky",
                    album_title="Symphony no. 6",
                    local_track_titles=["I", "II", "III", "IV"],
                    provider_payloads={},
                    local_context={"is_classical": True},
                )
        self.assertIsNone(result)

    def test_arbitration_infers_classical_context_from_tags_when_callsite_omits_it(self):
        pmda.ai_provider_ready = True
        pmda.PROVIDER_IDENTITY_USE_AI = True
        candidates = [
            {
                "provider": "discogs",
                "payload": {"title": "Symphony no. 6", "artist_name": "Tchaikovsky"},
                "provider_id": "discogs-1",
                "title_score": 0.96,
                "artist_score": 0.96,
                "ocr_title_score": 0.0,
                "ocr_artist_score": 0.0,
                "track_score": 0.78,
                "confidence": 0.74,
                "title": "Symphony no. 6",
                "artist": "Tchaikovsky",
                "strict_match_verified": False,
                "strict_reject_reason": "classical_track_count_mismatch",
                "strict_tracklist_score": 0.0,
                "has_provider_tracklist": True,
                "has_local_tracklist": True,
                "provider_track_count": 1,
                "local_track_count": 4,
                "track_count_ratio": 0.25,
                "classical_guard_applies": True,
                "classical_guard_ok": False,
            },
            {
                "provider": "lastfm",
                "payload": {"title": "Symphony no. 6", "artist": "Tchaikovsky"},
                "provider_id": "lastfm-1",
                "title_score": 0.95,
                "artist_score": 0.95,
                "ocr_title_score": 0.0,
                "ocr_artist_score": 0.0,
                "track_score": 0.0,
                "confidence": 0.71,
                "title": "Symphony no. 6",
                "artist": "Tchaikovsky",
                "strict_match_verified": False,
                "strict_reject_reason": "provider_id_missing",
                "strict_tracklist_score": 0.0,
                "has_provider_tracklist": False,
                "has_local_tracklist": True,
                "provider_track_count": 0,
                "local_track_count": 4,
                "track_count_ratio": 0.0,
                "classical_guard_applies": True,
                "classical_guard_ok": False,
            },
        ]
        with mock.patch.object(pmda, "_build_provider_identity_candidates", return_value=candidates):
            with mock.patch.object(pmda, "_ai_choose_provider_identity_candidate", side_effect=AssertionError("AI should not run when classical tags imply deterministic-only arbitration")):
                result = pmda._arbitrate_provider_identity(
                    artist_name="Peter Ilyich Tchaikovsky",
                    album_title='Tchaikovsky: Symphony no. 6 "Pathétique"',
                    local_track_titles=["I", "II", "III", "IV"],
                    provider_payloads={},
                    local_tags={"composer": "Pyotr Ilyich Tchaikovsky", "genre": "Classical"},
                )
        self.assertIsNone(result)

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

    def test_filename_pattern_conflict_override_is_forced_for_same_artist(self):
        hint = pmda._infer_identity_from_local_context_ai(
            local_artist="Slowdive",
            local_album="Takk...",
            folder_path="/music/slowdive/sigur_ros__takk__cross_artist_same_album_title",
            track_titles=["Alison", "Machine Gun", "40 Days", "Sing"],
            file_paths=[
                "/music/Slowdive - Souvlaki - 01 - Alison.flac",
                "/music/Slowdive - Souvlaki - 02 - Machine Gun.flac",
                "/music/Slowdive - Souvlaki - 03 - 40 Days.flac",
            ],
            local_tags={"artist": "Slowdive", "album": "Takk..."},
            missing_required_tags=[],
        )
        self.assertEqual(hint.get("source"), "filename_pattern")
        self.assertEqual(hint.get("artist"), "Slowdive")
        self.assertEqual(hint.get("album"), "Souvlaki")

    def test_force_try_allows_ai_local_context_to_correct_specific_wrong_album(self):
        original_ready = pmda.ai_provider_ready
        pmda.ai_provider_ready = True
        try:
            with mock.patch.object(
                pmda,
                "_call_ai_provider_bounded",
                return_value='{"artist":"Sigur Rós","album":"Von","confidence":93,"reason":"track titles clearly match Von"}',
            ):
                hint = pmda._infer_identity_from_local_context_ai(
                    local_artist="Sigur Rós",
                    local_album="Takk...",
                    folder_path="/music/sigur_ros__takk__same_artist_tags_match_tracks_mismatch",
                    track_titles=[
                        "Sigur Ros",
                        "Dögun",
                        "Hún Jörð",
                        "Leit að lífi",
                        "Myrkur",
                    ],
                    file_paths=[],
                    local_tags={"artist": "Sigur Rós", "album": "Takk..."},
                    missing_required_tags=[],
                    force_try=True,
                )
        finally:
            pmda.ai_provider_ready = original_ready
        self.assertEqual(hint.get("artist"), "Sigur Rós")
        self.assertEqual(hint.get("album"), "Von")
        self.assertEqual(int(hint.get("confidence") or 0), 93)

    def test_local_context_ai_skips_when_identity_already_specific(self):
        original_ready = pmda.ai_provider_ready
        pmda.ai_provider_ready = True
        try:
            with mock.patch.object(
                pmda,
                "_call_ai_provider_bounded",
                side_effect=AssertionError("AI should not run for specific tagged identity"),
            ):
                hint = pmda._infer_identity_from_local_context_ai(
                    local_artist="Slowdive",
                    local_album="Souvlaki",
                    folder_path="/music/Slowdive/Souvlaki",
                    track_titles=["Alison", "Machine Gun", "40 Days"],
                    file_paths=[],
                    local_tags={"artist": "Slowdive", "album": "Souvlaki"},
                    missing_required_tags=[],
                )
        finally:
            pmda.ai_provider_ready = original_ready
        self.assertEqual(hint, {})

    def test_local_context_ai_skips_when_resolved_identity_is_specific_even_if_tag_flags_remain(self):
        original_ready = pmda.ai_provider_ready
        pmda.ai_provider_ready = True
        try:
            with mock.patch.object(
                pmda,
                "_call_ai_provider_bounded",
                side_effect=AssertionError("AI should not run when artist/album are already usable"),
            ):
                hint = pmda._infer_identity_from_local_context_ai(
                    local_artist="John Tilbury piano",
                    local_album="For Bunita Marcus Composed by Morton Feldman",
                    folder_path="/music/John Tilbury piano/For Bunita Marcus Composed by Morton Feldman",
                    track_titles=["For Bunita Marcus", "For Bunita Marcus edit 01"],
                    file_paths=[],
                    local_tags={"artist": "John Tilbury piano", "album": "For Bunita Marcus Composed by Morton Feldman"},
                    missing_required_tags=["artist", "album"],
                )
        finally:
            pmda.ai_provider_ready = original_ready
        self.assertEqual(hint, {})

    def test_local_context_ai_runs_when_identity_is_still_missing(self):
        original_ready = pmda.ai_provider_ready
        pmda.ai_provider_ready = True
        try:
            with mock.patch.object(
                pmda,
                "_call_ai_provider_bounded",
                return_value='{"artist":"Abe Duque","album":"Come Back And Dance","confidence":90,"reason":"tags missing but track titles and folder are clear"}',
            ) as call_mock:
                hint = pmda._infer_identity_from_local_context_ai(
                    local_artist="",
                    local_album="",
                    folder_path="/music/15-03/01",
                    track_titles=["Come Back And Dance", "Track 2", "Track 3"],
                    file_paths=[],
                    local_tags={},
                    missing_required_tags=["artist", "album"],
                )
        finally:
            pmda.ai_provider_ready = original_ready
        self.assertTrue(call_mock.called)
        self.assertEqual(hint.get("artist"), "Abe Duque")
        self.assertEqual(hint.get("album"), "Come Back And Dance")

    def test_ollama_identity_inference_call_keeps_json_output_uncut(self):
        original_ollama_url = getattr(pmda, "ollama_url", "")
        pmda.ollama_url = "http://ollama.test"
        captured = {}

        class _Resp:
            status_code = 200

            def json(self):
                return {"message": {"content": '{"artist":"Slowdive","album":"Souvlaki","confidence":95,"reason":"clear"}'}}

        def _fake_post(url, json=None, timeout=None):
            captured["url"] = url
            captured["payload"] = json or {}
            captured["timeout"] = timeout
            return _Resp()

        try:
            with mock.patch.object(pmda.requests, "post", side_effect=_fake_post):
                out = pmda.call_ai_provider(
                    "ollama",
                    "qwen3:4b",
                    "Return strict JSON object only.",
                    "Infer identity.",
                    max_tokens=220,
                    analysis_type="identity_inference_no_tags",
                    request_timeout_sec=15,
                )
        finally:
            pmda.ollama_url = original_ollama_url
        self.assertIn('"artist":"Slowdive"', out)
        self.assertEqual(captured.get("payload", {}).get("options", {}).get("stop"), None)

    def test_ollama_identity_inference_non_json_reply_is_not_counted_as_success(self):
        original_ollama_url = getattr(pmda, "ollama_url", "")
        pmda.ollama_url = "http://ollama.test"

        class _Resp:
            status_code = 200

            def json(self):
                return {"message": {"content": "We are given:"}}

        try:
            with mock.patch.object(pmda.requests, "post", return_value=_Resp()):
                with mock.patch.object(pmda, "record_ai_usage") as recorder:
                    out = pmda.call_ai_provider(
                        "ollama",
                        "qwen3:4b",
                        "Return strict JSON object only.",
                        "Infer identity.",
                        max_tokens=220,
                        analysis_type="identity_inference_no_tags",
                        request_timeout_sec=15,
                    )
        finally:
            pmda.ollama_url = original_ollama_url
        self.assertEqual(out, "We are given:")
        self.assertTrue(recorder.called)
        self.assertEqual(recorder.call_args.kwargs.get("status"), "invalid_format")

    def test_bounded_identity_inference_non_json_reply_is_ignored(self):
        with mock.patch.object(pmda, "call_ai_provider", return_value="We are given:"):
            with mock.patch.object(pmda.logging, "warning") as warning_mock:
                out = pmda._call_ai_provider_bounded(
                    provider="ollama",
                    model="qwen3:4b",
                    system_msg="Return strict JSON object only.",
                    user_msg="Infer identity.",
                    max_tokens=220,
                    analysis_type="identity_inference_no_tags",
                    timeout_sec=10,
                    log_prefix="[AI Identity]",
                )
        self.assertEqual(out, "")
        self.assertTrue(any("non-JSON response" in str(call) for call in warning_mock.call_args_list))

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
