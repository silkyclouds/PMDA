import unittest
from pathlib import Path

import pmda


class DedupeAuditRegressionTests(unittest.TestCase):
    def test_dupe_ai_used_results_count_counts_only_true_ai_winners(self):
        results = [
            {"best": {"used_ai": True}},
            {"best": {"used_ai": False}},
            {"best": {}},
            {},
            {"best": {"used_ai": True}},
        ]
        self.assertEqual(pmda._dupe_ai_used_results_count(results), 2)

    def test_multidisc_sibling_group_detects_cd_subfolders(self):
        editions = [
            {
                "folder": Path("/music/Artist/Album/CD1"),
                "meta": {"totaldiscs": "2", "discnumber": "1"},
            },
            {
                "folder": Path("/music/Artist/Album/CD2"),
                "meta": {"totaldiscs": "2", "discnumber": "2"},
            },
        ]
        self.assertTrue(pmda._dupe_is_multidisc_sibling_group(editions))

    def test_track_sig_title_conflict_detects_distinct_album_titles(self):
        editions = [
            {
                "folder": Path("/music/Artist/neurotic"),
                "title_raw": "neurotic",
                "plex_title": "neurotic",
                "album_norm": "neurotic",
                "tracks": [{"title": "One"}, {"title": "Two"}],
            },
            {
                "folder": Path("/music/Artist/lost-illusions"),
                "title_raw": "lost illusions",
                "plex_title": "lost illusions",
                "album_norm": "lost illusions",
                "tracks": [{"title": "One"}, {"title": "Two"}],
            },
        ]
        self.assertTrue(pmda._dupe_track_sig_title_conflict(editions))

    def test_track_sig_title_conflict_allows_exact_provider_identity(self):
        editions = [
            {
                "folder": Path("/music/Artist/A"),
                "title_raw": "Life Force",
                "plex_title": "Life Force",
                "album_norm": "life force",
                "tracks": [{"title": "Intro", "dur": 1000}, {"title": "Main", "dur": 2000}],
                "discogs_release_id": "42",
                "dur": 3000,
            },
            {
                "folder": Path("/music/Artist/B"),
                "title_raw": "Live Force EP",
                "plex_title": "Live Force EP",
                "album_norm": "live force ep",
                "tracks": [{"title": "Intro", "dur": 1000}, {"title": "Main", "dur": 2000}],
                "discogs_release_id": "42",
                "dur": 3000,
            },
        ]
        self.assertFalse(pmda._dupe_track_sig_title_conflict(editions))

    def test_provider_id_title_conflict_detects_different_volume_numbers(self):
        editions = [
            {
                "folder": Path("/music/Gallery Six/Gallery Six Works, Vol. 1"),
                "title_raw": "Gallery Six Works, Vol. 1",
                "plex_title": "Gallery Six Works, Vol. 1",
                "album_norm": "gallery six works vol 1",
                "tracks": [
                    {"title": "Snow Light", "dur": 1000},
                    {"title": "River", "dur": 1000},
                    {"title": "Blue Window", "dur": 1000},
                ],
                "discogs_release_id": "31263739",
                "dur": 3000,
            },
            {
                "folder": Path("/music/Gallery Six/Gallery Six Works, Vol. 2"),
                "title_raw": "Gallery Six Works, Vol. 2",
                "plex_title": "Gallery Six Works, Vol. 2",
                "album_norm": "gallery six works vol 2",
                "tracks": [
                    {"title": "Silent Road", "dur": 1000},
                    {"title": "Glass Moon", "dur": 1000},
                    {"title": "Aftertone", "dur": 1000},
                ],
                "discogs_release_id": "31263739",
                "dur": 3000,
            },
        ]
        self.assertTrue(pmda._dupe_group_has_exact_provider_trackcount_signal(editions))
        self.assertTrue(
            pmda._dupe_provider_id_title_conflict(
                editions,
                max_jaccard=0.0,
                min_track_ratio=1.0,
            )
        )

    def test_provider_id_title_conflict_allows_same_volume_with_provider_identity(self):
        editions = [
            {
                "folder": Path("/music/Artist/Gallery Six Works Vol 1 A"),
                "title_raw": "Gallery Six Works, Vol. 1",
                "tracks": [{"title": "A", "dur": 1000}, {"title": "B", "dur": 1000}],
                "discogs_release_id": "31263739",
                "dur": 2000,
            },
            {
                "folder": Path("/music/Artist/Gallery Six Works Vol 1 B"),
                "title_raw": "Gallery Six Works Volume 1",
                "tracks": [{"title": "A", "dur": 1000}, {"title": "B", "dur": 1000}],
                "discogs_release_id": "31263739",
                "dur": 2000,
            },
        ]
        self.assertFalse(
            pmda._dupe_provider_id_title_conflict(
                editions,
                max_jaccard=1.0,
                min_track_ratio=1.0,
            )
        )


if __name__ == "__main__":
    unittest.main()
