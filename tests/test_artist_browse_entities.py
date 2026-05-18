import json
import unittest

import pmda


class ArtistBrowseEntityTests(unittest.TestCase):
    def test_apply_canonical_composers_to_classical_payload_prefers_linked_names(self):
        payload = {
            "is_classical": True,
            "composer": ["Peter Tchaikovsky"],
            "work": ["Symphony no. 6"],
        }

        out = pmda._files_apply_canonical_composers_to_classical_payload(
            payload,
            ["Peter Ilyich Tchaikovsky"],
        )

        self.assertEqual(out["composer"], ["Peter Ilyich Tchaikovsky"])
        self.assertEqual(
            pmda._files_album_display_artist_name(
                artist_name="Tchaikovsky",
                classical_payload=out,
            ),
            "Peter Ilyich Tchaikovsky",
        )

    def test_resolve_artist_norm_map_ignores_orphan_alias_rows(self):
        class _Cursor:
            def __init__(self):
                self._rows = []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql, params):
                query = str(sql or "")
                if "SELECT name_norm FROM files_artists" in query:
                    self._rows = []
                elif "FROM files_artist_aliases" in query:
                    self._rows = [] if "JOIN files_artists artist" in query else [("claude debussy", "severed heads")]
                else:
                    self._rows = []

            def fetchall(self):
                return list(self._rows)

        class _Conn:
            def cursor(self):
                return _Cursor()

        resolved = pmda._files_resolve_artist_norm_map(_Conn(), ["claude debussy"])
        self.assertEqual(resolved, {"claude debussy": "claude debussy"})

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

    def test_browse_entities_split_featured_credit_into_primary_and_guests(self):
        artists_map = {
            "the beatles feat michael jackson and annie cordy": {
                "name": "The Beatles feat Michael Jackson & Annie Cordy",
                "has_image": False,
                "image_path": "",
            },
        }
        albums_payload = [
            {
                "artist_norm": "the beatles feat michael jackson and annie cordy",
                "title": "Demo Collab",
                "folder_path": "/tmp/feat/demo_collab",
                "track_count": 8,
                "is_broken": False,
                "primary_tags_json": "{}",
                "tags_json": "[]",
            },
        ]

        entity_map, links = pmda._build_files_browse_artist_entities(artists_map, albums_payload)

        self.assertEqual(set(entity_map.keys()), {"the beatles", "michael jackson", "annie cordy"})
        self.assertEqual(
            links["/tmp/feat/demo_collab"],
            [
                {"artist_norm": "the beatles", "role": "artist", "is_primary": True},
                {"artist_norm": "michael jackson", "role": "featured", "is_primary": False},
                {"artist_norm": "annie cordy", "role": "featured", "is_primary": False},
            ],
        )

    def test_browse_entities_split_joint_album_credit_into_multiple_primary_artists(self):
        artists_map = {
            "christian burkhardt chris wood": {
                "name": "Christian Burkhardt & Chris Wood",
                "has_image": False,
                "image_path": "",
            },
        }
        albums_payload = [
            {
                "artist_norm": "christian burkhardt chris wood",
                "title": "Joint Release",
                "folder_path": "/tmp/joint/release",
                "track_count": 6,
                "is_broken": False,
                "primary_tags_json": "{}",
                "tags_json": "[]",
            },
        ]

        entity_map, links = pmda._build_files_browse_artist_entities(artists_map, albums_payload)

        self.assertEqual(set(entity_map.keys()), {"christian burkhardt", "chris wood"})
        self.assertEqual(
            links["/tmp/joint/release"],
            [
                {"artist_norm": "christian burkhardt", "role": "artist", "is_primary": True},
                {"artist_norm": "chris wood", "role": "artist", "is_primary": False},
            ],
        )

    def test_browse_entities_use_track_artists_for_compilation_appearances(self):
        artists_map = {
            "various artists": {"name": "Various Artists", "has_image": False, "image_path": ""},
        }
        albums_payload = [
            {
                "artist_norm": "various artists",
                "title": "Compilation Sampler",
                "folder_path": "/tmp/compilation/sampler",
                "track_count": 2,
                "is_broken": False,
                "primary_tags_json": json.dumps({"compilation": "1"}),
                "track_primary_tags_jsons": [
                    json.dumps({"artist": "Artist Alpha", "compilation": "1"}),
                    json.dumps({"artist": "Artist Beta feat Guest Gamma", "compilation": "1"}),
                ],
                "tags_json": "[]",
            },
        ]

        entity_map, links = pmda._build_files_browse_artist_entities(artists_map, albums_payload)

        self.assertEqual(set(entity_map.keys()), {"artist alpha", "artist beta", "guest gamma"})
        self.assertEqual(
            links["/tmp/compilation/sampler"],
            [
                {"artist_norm": "artist alpha", "role": "appearance", "is_primary": False},
                {"artist_norm": "artist beta", "role": "appearance", "is_primary": False},
                {"artist_norm": "guest gamma", "role": "featured", "is_primary": False},
            ],
        )

    def test_classical_browse_entities_are_composer_only(self):
        artists_map = {
            "martha argerich": {"name": "Martha Argerich", "has_image": False, "image_path": ""},
        }
        albums_payload = [
            {
                "artist_norm": "martha argerich",
                "title": "Schumann; Ravel; Schubert: Carte Blanche",
                "folder_path": "/tmp/classical/carte_blanche",
                "track_count": 12,
                "is_broken": False,
                "primary_tags_json": json.dumps(
                    {
                        "composer": ["Robert Schumann", "Maurice Ravel", "Franz Schubert"],
                        "work": ["Piano Trio in D major, op. 70 no. 1", "Piano Trio No. 2"],
                        "performer": ["Martha Argerich", "Mischa Maisky"],
                        "conductor": ["Leonard Bernstein"],
                        "orchestra": ["New York Philharmonic"],
                        "genre": ["Classical"],
                    },
                    ensure_ascii=False,
                ),
                "tags_json": json.dumps(["Classical"], ensure_ascii=False),
            },
        ]

        entity_map, links = pmda._build_files_browse_artist_entities(artists_map, albums_payload)

        self.assertEqual(
            set(entity_map.keys()),
            {"robert schumann", "maurice ravel", "franz schubert"},
        )
        self.assertNotIn("martha argerich", entity_map)
        for entity in entity_map.values():
            self.assertEqual(entity["entity_kind"], "composer")
            self.assertEqual(json.loads(entity["roles_json"]), ["composer"])
        self.assertEqual(
            links["/tmp/classical/carte_blanche"],
            [
                {"artist_norm": "robert schumann", "role": "composer", "is_primary": True},
                {"artist_norm": "maurice ravel", "role": "composer", "is_primary": False},
                {"artist_norm": "franz schubert", "role": "composer", "is_primary": False},
            ],
        )

    def test_classical_album_display_artist_name_prefers_composers(self):
        classical_payload = {
            "is_classical": True,
            "composer": ["Ludwig van Beethoven", "Franz Schubert"],
        }
        self.assertEqual(
            pmda._files_album_display_artist_name(
                artist_name="Martha Argerich",
                classical_payload=classical_payload,
            ),
            "Ludwig van Beethoven, Franz Schubert",
        )
        self.assertEqual(
            pmda._files_album_display_artist_name(
                artist_name="Sigur Rós",
                classical_payload=None,
            ),
            "Sigur Rós",
        )

    def test_classical_title_prefix_extracts_multiple_composers_for_browse(self):
        artists_map = {
            "martha argerich": {"name": "Martha Argerich", "has_image": False, "image_path": ""},
        }
        albums_payload = [
            {
                "artist_norm": "martha argerich",
                "title": "Schumann; Ravel; Schubert: Carte Blanche",
                "folder_path": "/tmp/classical/title_only_carte_blanche",
                "track_count": 12,
                "is_broken": False,
                "primary_tags_json": json.dumps({"genre": ["Classical"]}, ensure_ascii=False),
                "tags_json": json.dumps(["Classical"], ensure_ascii=False),
            },
        ]

        entity_map, links = pmda._build_files_browse_artist_entities(artists_map, albums_payload)

        self.assertEqual(
            set(entity_map.keys()),
            {"schumann", "ravel", "schubert"},
        )
        self.assertEqual(
            links["/tmp/classical/title_only_carte_blanche"],
            [
                {"artist_norm": "schumann", "role": "composer", "is_primary": True},
                {"artist_norm": "ravel", "role": "composer", "is_primary": False},
                {"artist_norm": "schubert", "role": "composer", "is_primary": False},
            ],
        )

    def test_classical_title_prefix_ignores_non_composer_project_prefixes(self):
        self.assertEqual(
            pmda._classical_title_composer_values("The Tchaikovsky Project: Complete Symphonies and Piano Concertos"),
            [],
        )

    def test_classical_album_without_composer_does_not_publish_performer_artist_page(self):
        artists_map = {
            "cello": {"name": "Cello", "has_image": False, "image_path": ""},
        }
        albums_payload = [
            {
                "artist_norm": "cello",
                "title": "Carte Blanche",
                "folder_path": "/tmp/classical/cello_only",
                "track_count": 8,
                "is_broken": False,
                "primary_tags_json": json.dumps({"genre": ["Classical"], "artist": ["Cello"]}, ensure_ascii=False),
                "tags_json": json.dumps(["Classical"], ensure_ascii=False),
            },
        ]

        entity_map, links = pmda._build_files_browse_artist_entities(artists_map, albums_payload)

        self.assertEqual(entity_map, {})
        self.assertEqual(links, {})

    def test_classical_surname_bucket_collapses_to_single_equivalent_composer(self):
        artists_map = {
            "pyotr tchaikovsky": {"name": "Pyotr Ilyich Tchaikovsky", "has_image": False, "image_path": ""},
            "peter tchaikovsky": {"name": "Peter Tchaikovsky", "has_image": False, "image_path": ""},
            "tchaikovsky": {"name": "Tchaikovsky", "has_image": False, "image_path": ""},
        }
        albums_payload = [
            {
                "artist_norm": "pyotr tchaikovsky",
                "title": "Symphony no. 6 \"Pathétique\"",
                "folder_path": "/tmp/classical/tchaikovsky_full",
                "track_count": 4,
                "is_broken": False,
                "primary_tags_json": json.dumps({"composer": ["Pyotr Ilyich Tchaikovsky"], "genre": ["Classical"]}, ensure_ascii=False),
                "tags_json": json.dumps(["Classical"], ensure_ascii=False),
            },
            {
                "artist_norm": "peter tchaikovsky",
                "title": "Tchaikovsky: Iolanta",
                "folder_path": "/tmp/classical/tchaikovsky_english",
                "track_count": 20,
                "is_broken": False,
                "primary_tags_json": json.dumps({"composer": ["Peter Tchaikovsky"], "genre": ["Classical"]}, ensure_ascii=False),
                "tags_json": json.dumps(["Classical"], ensure_ascii=False),
            },
            {
                "artist_norm": "tchaikovsky",
                "title": "Tchaikovsky: Symphony no. 6 \"Pathétique\"",
                "folder_path": "/tmp/classical/tchaikovsky_short",
                "track_count": 4,
                "is_broken": False,
                "primary_tags_json": json.dumps({"composer": ["Tchaikovsky"], "genre": ["Classical"]}, ensure_ascii=False),
                "tags_json": json.dumps(["Classical"], ensure_ascii=False),
            },
        ]

        entity_map, links = pmda._build_files_browse_artist_entities(artists_map, albums_payload)

        self.assertEqual(len(entity_map), 1)
        only_norm = next(iter(entity_map))
        linked_norms = {entry["artist_norm"] for items in links.values() for entry in items}
        self.assertEqual(linked_norms, {only_norm})

    def test_classical_person_names_equivalent_matches_tchaikovsky_transliterations(self):
        self.assertTrue(
            pmda._classical_person_names_equivalent(
                "Peter Ilyich Tchaikovsky",
                "Pyotr Ilyich Tchaikovsky",
            )
        )
        self.assertTrue(
            pmda._classical_person_names_equivalent(
                "Piotr Ilyich Tchaikovsky",
                "Petr Ilyich Tchaikovsky",
            )
        )

    def test_classical_person_names_equivalent_accepts_minor_given_name_typo(self):
        self.assertTrue(
            pmda._classical_person_names_equivalent(
                "Calude Debussy",
                "Claude Debussy",
            )
        )

    def test_classical_display_payload_prefers_title_composers_over_conflicting_tags(self):
        payload = pmda._classical_display_payload(
            {
                "genre": ["Classical"],
                "composer": [
                    "Alessandro Marcello",
                    "Johannes Brahms",
                    "Felix Mendelssohn",
                    "Richard Strauss",
                ],
            },
            fallback_title="Brahms; Mendelssohn; Strauss: Glenn Gould ...And Serenity",
            fallback_artist="Glenn Gould",
        )
        self.assertEqual(
            payload["composer"],
            ["Johannes Brahms", "Felix Mendelssohn", "Richard Strauss"],
        )


if __name__ == "__main__":
    unittest.main()
