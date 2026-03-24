import json
import unittest

import pmda


class ArtistBrowseEntityTests(unittest.TestCase):
    def test_generic_artist_role_is_not_person_like(self):
        self.assertFalse(pmda._artist_is_person_like(entity_kind="artist", role_hints=["artist"]))
        self.assertFalse(pmda._artist_is_person_like(entity_kind="", role_hints=["artist"]))
        self.assertTrue(pmda._artist_is_person_like(entity_kind="composer", role_hints=["composer"]))

    def test_generic_artist_alias_rows_do_not_generate_classical_person_aliases(self):
        rows = pmda._files_artist_alias_rows_for_identity(
            artist_name="Sigur Rós",
            artist_norm="sigur ros",
            canonical_name="Sigur Rós",
            entity_kind="artist",
            roles_json=json.dumps(["artist"]),
            aliases_json=json.dumps(["Sigur Ros"]),
        )
        alias_norms = {str(row.get("alias_norm") or "").strip() for row in rows}
        self.assertIn("sigur ros", alias_norms)
        self.assertNotIn("s ros", alias_norms)

    def test_build_files_browse_artist_entities_keeps_nonclassical_artists_distinct(self):
        artists_map = {
            "severed heads": {"name": "Severed Heads", "has_image": False, "image_path": ""},
            "sigur ros": {"name": "Sigur Rós", "has_image": False, "image_path": ""},
            "slowdive": {"name": "Slowdive", "has_image": False, "image_path": ""},
        }
        albums_payload = [
            {
                "artist_norm": "severed heads",
                "title": "Since the Accident",
                "folder_path": "/tmp/severed_heads/since_the_accident",
                "track_count": 10,
                "is_broken": False,
                "primary_tags_json": "{}",
                "tags_json": "[]",
            },
            {
                "artist_norm": "sigur ros",
                "title": "Takk...",
                "folder_path": "/tmp/sigur_ros/takk_ref",
                "track_count": 11,
                "is_broken": False,
                "primary_tags_json": "{}",
                "tags_json": "[]",
            },
            {
                "artist_norm": "slowdive",
                "title": "Slowdive",
                "folder_path": "/tmp/slowdive/slowdive",
                "track_count": 10,
                "is_broken": False,
                "primary_tags_json": "{}",
                "tags_json": "[]",
            },
            {
                "artist_norm": "slowdive",
                "title": "Takk...",
                "folder_path": "/tmp/slowdive/takk_cross_artist",
                "track_count": 6,
                "is_broken": False,
                "primary_tags_json": "{}",
                "tags_json": "[]",
            },
        ]

        entity_map, links = pmda._build_files_browse_artist_entities(artists_map, albums_payload)

        self.assertEqual(set(entity_map.keys()), {"severed heads", "sigur ros", "slowdive"})
        self.assertEqual(entity_map["severed heads"]["name"], "Severed Heads")
        self.assertEqual(entity_map["sigur ros"]["name"], "Sigur Rós")
        self.assertEqual(entity_map["slowdive"]["name"], "Slowdive")
        self.assertNotIn("Sigur Rós", json.loads(entity_map["severed heads"]["aliases_json"]))
        self.assertEqual(
            links["/tmp/sigur_ros/takk_ref"],
            [{"artist_norm": "sigur ros", "role": "artist", "is_primary": True}],
        )
        self.assertEqual(
            links["/tmp/slowdive/takk_cross_artist"],
            [{"artist_norm": "slowdive", "role": "artist", "is_primary": True}],
        )


if __name__ == "__main__":
    unittest.main()
