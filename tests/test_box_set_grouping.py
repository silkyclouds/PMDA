import sys
import types
import unittest

sys.modules.setdefault(
    "musicbrainzngs",
    types.SimpleNamespace(
        set_rate_limit=lambda *args, **kwargs: None,
        set_useragent=lambda *args, **kwargs: None,
    ),
)

import pmda


class BoxSetGroupingTests(unittest.TestCase):
    def test_collapse_browse_rows_into_single_box_set_card(self):
        rows = [
            {
                "album_id": 21,
                "title": "Lily Laskine - Complete Erato & HMV Recordings",
                "title_norm": "lily laskine complete erato hmv recordings",
                "folder_path": "/music/Classical/Lily Laskine - Complete Erato & HMV Recordings/CD1",
                "track_count": 9,
                "has_cover": False,
                "artist_id": 201,
                "artist_name": "Lily Laskine, Arielle Nordmann",
            },
            {
                "album_id": 22,
                "title": "Lily Laskine - Complete Erato & HMV Recordings",
                "title_norm": "lily laskine complete erato hmv recordings",
                "folder_path": "/music/Classical/Lily Laskine - Complete Erato & HMV Recordings/CD2",
                "track_count": 11,
                "has_cover": True,
                "artist_id": 202,
                "artist_name": "Lily Laskine",
            },
        ]

        collapsed = pmda._collapse_files_album_browse_rows(rows)

        self.assertEqual(len(collapsed), 1)
        self.assertTrue(bool(collapsed[0].get("is_box_set")))
        self.assertEqual(int(collapsed[0].get("album_id") or 0), 22)
        self.assertEqual(int(collapsed[0].get("track_count") or 0), 20)
        self.assertEqual(int(collapsed[0].get("box_set_disc_count") or 0), 2)
        self.assertEqual(collapsed[0].get("artist_name"), "Lily Laskine")

    def test_format_subfolders_are_not_treated_as_box_sets(self):
        rows = [
            {
                "album_id": 31,
                "title": "Basement",
                "title_norm": "basement",
                "folder_path": "/music/Electronic/Basement/FLAC",
                "track_count": 10,
                "has_cover": True,
                "artist_id": 301,
                "artist_name": "Lime & Malone",
            },
            {
                "album_id": 32,
                "title": "Basement",
                "title_norm": "basement",
                "folder_path": "/music/Electronic/Basement/MP3",
                "track_count": 10,
                "has_cover": False,
                "artist_id": 301,
                "artist_name": "Lime & Malone",
            },
        ]

        collapsed = pmda._collapse_files_album_browse_rows(rows)

        self.assertEqual(len(collapsed), 2)
        self.assertFalse(any(bool(item.get("is_box_set")) for item in collapsed))

    def test_reindex_tracks_assigns_monotonic_disc_numbers_across_members(self):
        tracks = [
            {"album_id": 41, "track_id": 4101, "disc_num": 1, "track_num": 1, "title": "One"},
            {"album_id": 41, "track_id": 4102, "disc_num": 1, "track_num": 2, "title": "Two"},
            {"album_id": 42, "track_id": 4201, "disc_num": 1, "track_num": 1, "title": "Three"},
            {"album_id": 42, "track_id": 4202, "disc_num": 1, "track_num": 2, "title": "Four"},
        ]

        reindexed, disc_count = pmda._files_box_set_reindex_tracks(tracks, [41, 42])

        self.assertEqual(disc_count, 2)
        self.assertEqual([int(track["disc_num"]) for track in reindexed], [1, 1, 2, 2])
        self.assertEqual([track["disc_label"] for track in reindexed], ["Disc 1", "Disc 1", "Disc 2", "Disc 2"])


if __name__ == "__main__":
    unittest.main()
