import unittest
from unittest import mock

import pmda


class ProviderLookupOptimizationTests(unittest.TestCase):
    def test_discogs_lookup_candidate_cap_tightens_high_confidence_queries(self):
        self.assertEqual(pmda._discogs_lookup_candidate_cap(0.99), 3)
        self.assertEqual(pmda._discogs_lookup_candidate_cap(0.94), 4)
        self.assertEqual(pmda._discogs_lookup_candidate_cap(0.87), 5)
        self.assertEqual(pmda._discogs_lookup_candidate_cap(0.60), 6)

    def test_lastfm_pick_search_candidate_prefers_best_scored_match(self):
        rows = [
            {"artist": "Random Artist", "name": "Completely Different Album"},
            {"artist": "Aphex Twin", "name": "Selected Ambient Works 85-92"},
            {"artist": "Aphex Twin", "name": "Selected Ambient Works Volume II"},
        ]
        picked = pmda._lastfm_pick_search_candidate(
            "Aphex Twin",
            "Selected Ambient Works 85-92",
            rows,
        )
        self.assertEqual(picked, ("Aphex Twin", "Selected Ambient Works 85-92"))

    def test_lastfm_pick_search_candidate_rejects_weak_search_results(self):
        rows = [
            {"artist": "Random Artist", "name": "Completely Different Album"},
            {"artist": "Also Wrong", "name": "Another Thing"},
        ]
        picked = pmda._lastfm_pick_search_candidate(
            "Aphex Twin",
            "Selected Ambient Works 85-92",
            rows,
        )
        self.assertIsNone(picked)

    def test_bandcamp_lookup_candidate_cap_prefers_obvious_top_hit(self):
        self.assertEqual(pmda._bandcamp_lookup_candidate_cap([3.4, 2.1, 1.8]), 1)
        self.assertEqual(pmda._bandcamp_lookup_candidate_cap([2.9, 2.4, 1.8]), 2)
        self.assertEqual(pmda._bandcamp_lookup_candidate_cap([2.0, 1.7, 1.5]), 3)
        self.assertEqual(pmda._bandcamp_lookup_candidate_cap([1.4, 1.2, 1.1]), 4)

    def test_provider_track_titles_and_ids_support_itunes_and_deezer(self):
        itunes = {
            "collection_id": "12345",
            "tracklist": ["Track A", "Track B"],
        }
        deezer = {
            "album_id": "67890",
            "tracklist": ["Track 1", "Track 2"],
        }
        self.assertEqual(pmda._provider_track_titles_for_strict("itunes", itunes), ["Track A", "Track B"])
        self.assertEqual(pmda._provider_track_titles_for_strict("deezer", deezer), ["Track 1", "Track 2"])
        self.assertEqual(pmda._provider_id_for_strict("itunes", itunes), "12345")
        self.assertEqual(pmda._provider_id_for_strict("deezer", deezer), "67890")

    def test_fetch_album_provider_fallbacks_parallel_collects_itunes_and_deezer(self):
        def fake_lookup(provider, artist, title, fetcher):
            if provider == "itunes":
                return {"title": title, "artist_name": artist, "collection_id": "it-1"}
            if provider == "deezer":
                return {"title": title, "artist_name": artist, "album_id": "dz-1"}
            return None

        with mock.patch.object(pmda, "USE_DISCOGS", False), \
            mock.patch.object(pmda, "USE_BANDCAMP", False), \
            mock.patch.object(pmda, "USE_LASTFM", False), \
            mock.patch.object(pmda, "USE_ITUNES", True), \
            mock.patch.object(pmda, "USE_DEEZER", True), \
            mock.patch.object(pmda, "fetch_provider_album_lookup_cached", side_effect=fake_lookup):
            data = pmda._fetch_album_provider_fallbacks_parallel("Slowdive", "Souvlaki", scan_inline=False)

        self.assertIsInstance(data.get("itunes"), dict)
        self.assertIsInstance(data.get("deezer"), dict)
        sources = [str(item.get("source") or "") for item in (data.get("extra_sources") or []) if isinstance(item, dict)]
        self.assertIn("iTunes / Apple Music", sources)
        self.assertIn("Deezer", sources)

    def test_provider_payloads_fetch_bounded_for_scan_merges_parallel_result(self):
        existing = {
            "discogs": {"title": "Existing", "artist_name": "Slowdive"},
            "lastfm": None,
        }
        fetched = {
            "discogs": {"title": "New", "artist_name": "Slowdive"},
            "bandcamp": {"title": "Souvlaki", "artist_name": "Slowdive"},
            "lastfm": {"title": "Souvlaki", "artist": "Slowdive"},
        }
        with mock.patch.object(pmda, "_fetch_album_provider_fallbacks_parallel", return_value=fetched):
            out = pmda._provider_payloads_fetch_bounded_for_scan("Slowdive", "Souvlaki", existing=existing)

        self.assertEqual(out["discogs"], existing["discogs"])
        self.assertEqual(out["bandcamp"], fetched["bandcamp"])
        self.assertEqual(out["lastfm"], fetched["lastfm"])
        self.assertIn("spotify", out)

    def test_strict_provider_cold_fetch_disallowed_during_scan_without_id(self):
        edition = {"primary_metadata_source": "bandcamp"}
        with mock.patch.object(pmda, "_scan_inline_matching_active", return_value=True), \
            mock.patch.object(pmda, "_ai_scan_lifecycle_phase_active", return_value=False):
            allowed = pmda._strict_provider_cold_fetch_allowed("bandcamp", edition)

        self.assertFalse(allowed)

    def test_strict_validate_skips_primary_cold_fetch_during_scan(self):
        edition = {
            "primary_metadata_source": "bandcamp",
            "tracks": [],
        }
        with mock.patch.object(pmda, "_scan_inline_matching_active", return_value=True), \
            mock.patch.object(pmda, "_ai_scan_lifecycle_phase_active", return_value=False), \
            mock.patch.object(pmda, "_strict_payload_for_provider", side_effect=AssertionError("cold fetch should stay disabled during scan")):
            verdict = pmda._strict_validate_edition_match(
                artist_name="Slowdive",
                album_title="Souvlaki",
                edition=edition,
            )

        self.assertFalse(verdict["strict_match_verified"])
        self.assertEqual(verdict["strict_reject_reason"], "provider_no_tracklist")


if __name__ == "__main__":
    unittest.main()
