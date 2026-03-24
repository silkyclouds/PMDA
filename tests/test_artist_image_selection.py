import unittest
from unittest import mock

import pmda


class ArtistImageSelectionTests(unittest.TestCase):
    def test_classical_entities_reject_weak_external_sources(self):
        self.assertTrue(
            pmda._artist_external_image_requires_authoritative_refresh(
                provider="web",
                image_url="https://example.com/random-image.jpg",
                entity_kind="composer",
                role_hints=["composer"],
            )
        )
        self.assertTrue(
            pmda._artist_external_image_requires_authoritative_refresh(
                provider="lastfm",
                image_url="https://lastfm.freetls.fastly.net/i/u/300x300/random.jpg",
                entity_kind="orchestra",
                role_hints=["orchestra"],
            )
        )

    def test_classical_entities_accept_authoritative_sources(self):
        self.assertFalse(
            pmda._artist_external_image_requires_authoritative_refresh(
                provider="wikipedia",
                image_url="https://upload.wikimedia.org/wikipedia/commons/1/12/Composer_portrait.jpg",
                entity_kind="orchestra",
                role_hints=["orchestra"],
            )
        )
        self.assertFalse(
            pmda._artist_external_image_requires_authoritative_refresh(
                provider="musicbrainz_url",
                image_url="https://i.scdn.co/image/ab6761610000e5eb1234567890abcdef12345678",
                entity_kind="conductor",
                role_hints=["conductor"],
            )
        )

    def test_classical_ensemble_musicbrainz_spotify_avatar_requires_refresh(self):
        self.assertTrue(
            pmda._artist_external_image_requires_authoritative_refresh(
                provider="musicbrainz_url",
                image_url="https://i.scdn.co/image/ab6761610000e5ebaac46a30dc91a8148051a633",
                entity_kind="orchestra",
                role_hints=["orchestra"],
            )
        )

    def test_classical_entities_reject_fanart_and_audiodb(self):
        self.assertTrue(
            pmda._artist_external_image_requires_authoritative_refresh(
                provider="fanart",
                image_url="https://assets.fanart.tv/fanart/music/artist-thumb.jpg",
                entity_kind="composer",
                role_hints=["composer"],
            )
        )
        self.assertTrue(
            pmda._artist_external_image_requires_authoritative_refresh(
                provider="audiodb",
                image_url="https://www.theaudiodb.com/images/media/artist/thumb/example.jpg",
                entity_kind="orchestra",
                role_hints=["orchestra"],
            )
        )

    def test_non_classical_entities_keep_non_placeholder_lastfm(self):
        self.assertFalse(
            pmda._artist_external_image_requires_authoritative_refresh(
                provider="lastfm",
                image_url="https://lastfm.freetls.fastly.net/i/u/300x300/real-band-photo.jpg",
                entity_kind="artist",
                role_hints=["band"],
            )
        )

    def test_suspicious_or_placeholder_urls_are_never_accepted(self):
        self.assertTrue(
            pmda._artist_external_image_requires_authoritative_refresh(
                provider="fanart",
                image_url="https://coverartarchive.org/release/abc/front-250.jpg",
                entity_kind="artist",
                role_hints=[],
            )
        )

    def test_choose_preferred_person_identity_name_keeps_specific_variant(self):
        self.assertEqual(
            pmda._choose_preferred_person_identity_name("Semyon", "Semyon Bychkov"),
            "Semyon Bychkov",
        )
        self.assertEqual(
            pmda._choose_preferred_person_identity_name("Peter Tchaikovsky", "Pyotr Ilyich Tchaikovsky"),
            "Pyotr Ilyich Tchaikovsky",
        )
        self.assertTrue(
            pmda._artist_external_image_requires_authoritative_refresh(
                provider="wikipedia",
                image_url="https://lastfm.freetls.fastly.net/i/u/300x300/2a96cbd8b46e442fc41c2b86b821562f.png",
                entity_kind="artist",
                role_hints=[],
            )
        )

    def test_classical_display_preference_uses_original_variant_over_english_primary(self):
        aliases = [
            "Peter Tchaikovsky",
            "Pyotr Ilyich Tchaikovsky",
            "Petr Ilyich Tchaikovsky",
            "Piotr Ilyich Tchaikovsky",
        ]
        self.assertEqual(
            pmda._select_classical_person_display_name(
                current_name="Peter Tchaikovsky",
                primary_name="Peter Tchaikovsky",
                aliases=aliases,
                preference="original",
            ),
            "Pyotr Ilyich Tchaikovsky",
        )
        self.assertEqual(
            pmda._select_classical_person_display_name(
                current_name="Peter Tchaikovsky",
                primary_name="Peter Tchaikovsky",
                aliases=aliases,
                preference="english",
            ),
            "Peter Tchaikovsky",
        )

    def test_classical_name_equivalence_handles_transliteration_but_not_suffix_variants(self):
        self.assertTrue(
            pmda._classical_person_names_equivalent(
                "Peter Tchaikovsky",
                "Pyotr Ilyich Tchaikovsky",
            )
        )
        self.assertFalse(
            pmda._classical_person_names_equivalent(
                "Johann Strauss I",
                "Johann Strauss II",
            )
        )

    def test_classical_person_image_rejects_wrong_same_surname_person(self):
        self.assertFalse(
            pmda._artist_image_url_looks_relevant(
                "https://commons.wikimedia.org/wiki/Special:FilePath/Maria%20Barbara%20Bach%20%281684%E2%80%931720%29.jpg",
                artist_name="Johann Sebastian Bach",
                entity_kind="composer",
                role_hints=["composer"],
                page_title="File:Maria Barbara Bach (1684–1720).jpg",
                page_summary="German singer and first wife of Johann Sebastian Bach.",
            )
        )

    def test_classical_person_image_accepts_real_portrait_file(self):
        self.assertTrue(
            pmda._artist_image_url_looks_relevant(
                "https://commons.wikimedia.org/wiki/Special:FilePath/Tchaikovsky%20by%20Reutlinger.jpg",
                artist_name="Peter Tchaikovsky",
                entity_kind="composer",
                role_hints=["composer"],
                page_title="File:Tchaikovsky by Reutlinger.jpg",
                page_summary="Russian composer portrait.",
            )
        )

    def test_ensemble_image_rejects_event_or_venue_file(self):
        self.assertFalse(
            pmda._artist_image_url_looks_relevant(
                "https://commons.wikimedia.org/wiki/Special:FilePath/Berlin%20Philharmonie%20Ukraine-Konzert%20asv2022-07%20img2.jpg",
                artist_name="Deutsches Symphonie-Orchester Berlin",
                entity_kind="orchestra",
                role_hints=["orchestra"],
                page_title="File:Berlin Philharmonie Ukraine-Konzert asv2022-07 img2.jpg",
                page_summary="Concert image from the Berlin Philharmonie.",
            )
        )

    def test_ensemble_image_rejects_spotify_logo_avatar(self):
        self.assertFalse(
            pmda._artist_image_url_looks_relevant(
                "https://i.scdn.co/image/ab6761610000e5eb65ffed8eba27a6d130accb2d",
                artist_name="Deutsches Symphonie-Orchester Berlin",
                entity_kind="orchestra",
                role_hints=["orchestra"],
            )
        )

    def test_artist_alias_rows_include_canonical_name_and_generated_person_aliases(self):
        rows = pmda._files_artist_alias_rows_for_identity(
            artist_name="Peter Tchaikovsky",
            canonical_name="Pyotr Ilyich Tchaikovsky",
            artist_norm=pmda._norm_artist_key("Pyotr Ilyich Tchaikovsky"),
            entity_kind="composer",
            roles_json=["composer"],
            aliases_json=["P. I. Tchaikovsky"],
        )
        aliases = {row["alias"] for row in rows}
        self.assertIn("Pyotr Ilyich Tchaikovsky", aliases)
        self.assertIn("Peter Tchaikovsky", aliases)
        self.assertTrue(any(alias in aliases for alias in ("P I Tchaikovsky", "P.I. Tchaikovsky", "P. I. Tchaikovsky")))

    def test_primary_lookup_prefers_canonical_classical_name(self):
        self.assertEqual(
            pmda._artist_identity_primary_lookup_name(
                "Peter Tchaikovsky",
                entity_kind="composer",
                role_hints=["composer"],
                candidate_names=[
                    "Pyotr Ilyich Tchaikovsky",
                    "Tchaikovsky, Pyotr Ilyich",
                    "Peter Tchaikovsky",
                ],
            ),
            "Pyotr Ilyich Tchaikovsky",
        )

    def test_profile_text_matches_canonical_aliases(self):
        self.assertTrue(
            pmda._artist_profile_text_matches_any_identity(
                "J.S. Bach",
                "Johann Sebastian Bach was a German composer and musician of the late Baroque period.",
                entity_kind="composer",
                role_hints=["composer"],
                candidate_names=["Johann Sebastian Bach", "Bach, Johann Sebastian"],
            )
        )

    def test_classical_web_image_rejects_untrusted_context_only_match(self):
        self.assertFalse(
            pmda._artist_image_url_looks_relevant(
                "https://example.com/images/bee.jpg",
                artist_name="Peter Tchaikovsky",
                entity_kind="composer",
                role_hints=["composer"],
                page_title="Pyotr Ilyich Tchaikovsky biography",
                page_summary="Russian composer portrait and biography.",
            )
        )

    def test_classical_search_result_rejects_wasp_false_positive(self):
        self.assertFalse(
            pmda._artist_image_result_looks_relevant(
                "Peter Tchaikovsky",
                {
                    "title": "Tchaikovsky wasp",
                    "snippet": "Species page for a parasitoid wasp named after Tchaikovsky.",
                    "link": "https://example.com/tchaikovsky-wasp",
                },
                entity_kind="composer",
                role_hints=["composer"],
                candidate_names=["Pyotr Ilyich Tchaikovsky", "Peter Tchaikovsky"],
            )
        )

    def test_ensemble_search_result_rejects_logo_page(self):
        self.assertFalse(
            pmda._artist_image_result_looks_relevant(
                "Česká filharmonie",
                {
                    "title": "Česká filharmonie visual identity logo",
                    "snippet": "Official logo and brand identity assets for the orchestra.",
                    "link": "https://example.com/ceska-filharmonie-logo",
                },
                entity_kind="orchestra",
                role_hints=["orchestra"],
                candidate_names=["Ceska filharmonie", "Czech Philharmonic"],
            )
        )

    def test_lookup_names_use_musicbrainz_identity_for_transliterated_queries(self):
        with mock.patch.object(
            pmda,
            "_musicbrainz_artist_identity_lookup",
            return_value={
                "name": "Johann Sebastian Bach",
                "sort_name": "Bach, Johann Sebastian",
                "aliases": ["Jean-Sébastien Bach", "J. S. Bach"],
            },
        ):
            names = pmda._artist_identity_lookup_names(
                "Jean-Sebastien Bach",
                entity_kind="composer",
                role_hints=["composer"],
                candidate_names=[],
            )
        self.assertIn("Johann Sebastian Bach", names)
        self.assertTrue(any(value in names for value in ("Jean-Sébastien Bach", "Jean-Sebastien Bach")))

    def test_verified_filename_pattern_hint_promotes_display_identity(self):
        artist, album = pmda._resolve_edition_display_identity(
            {
                "artist": "Sigur Rós",
                "title_raw": "Takk...",
                "_lookup_artist_name": "Slowdive",
                "_lookup_album_title": "Souvlaki",
                "_lookup_identity_hint": {
                    "artist": "Slowdive",
                    "album": "Souvlaki",
                    "confidence": 96,
                    "reason": "stable filename pattern (artist_missing_or_generic, album_missing_or_generic)",
                    "source": "filename_pattern",
                },
                "strict_match_verified": True,
                "strict_match_provider": "discogs",
                "discogs_release_id": "42",
            },
            default_artist="Sigur Rós",
            default_title="Takk...",
        )
        self.assertEqual((artist, album), ("Slowdive", "Souvlaki"))

    def test_files_remap_resolved_artist_norms_merges_classical_alias_payloads(self):
        pyotr_norm = pmda._norm_artist_key("Pyotr Ilyich Tchaikovsky")
        peter_norm = pmda._norm_artist_key("Peter Tchaikovsky")
        albums_payload = [
            {"folder_path": "/music/a", "artist_norm": peter_norm},
            {"folder_path": "/music/b", "artist_norm": pyotr_norm},
        ]
        artists_map = {
            peter_norm: {
                "name": "Peter Tchaikovsky",
                "canonical_name": "Peter Tchaikovsky",
                "canonical_name_norm": peter_norm,
                "entity_kind": "composer",
                "roles_json": '["composer"]',
                "aliases_json": '["Peter Tchaikovsky"]',
                "has_image": False,
                "image_path": "",
            },
            pyotr_norm: {
                "name": "Pyotr Ilyich Tchaikovsky",
                "canonical_name": "Pyotr Ilyich Tchaikovsky",
                "canonical_name_norm": pyotr_norm,
                "entity_kind": "composer",
                "roles_json": '["composer"]',
                "aliases_json": '["Pyotr Ilyich Tchaikovsky"]',
                "has_image": False,
                "image_path": "",
            },
        }
        remapped_artists, remapped_links = pmda._files_remap_resolved_artist_norms(
            artists_map,
            resolved_norm_map={
                peter_norm: pyotr_norm,
                pyotr_norm: pyotr_norm,
            },
            albums_payload=albums_payload,
            album_links_by_folder={
                "/music/a": [{"artist_norm": peter_norm, "role": "composer", "is_primary": True}],
                "/music/b": [{"artist_norm": pyotr_norm, "role": "composer", "is_primary": True}],
            },
        )
        self.assertEqual(list(remapped_artists.keys()), [pyotr_norm])
        self.assertEqual(remapped_artists[pyotr_norm]["name"], "Pyotr Ilyich Tchaikovsky")
        self.assertEqual(albums_payload[0]["artist_norm"], pyotr_norm)
        self.assertEqual(remapped_links["/music/a"][0]["artist_norm"], pyotr_norm)

    def test_browse_entities_keep_original_classical_display_name_after_merge(self):
        original_pref = pmda.CLASSICAL_NAME_PREFERENCE
        try:
            pmda.CLASSICAL_NAME_PREFERENCE = "original"
            artists_map = {
                pmda._norm_artist_key("Pyotr Ilyich Tchaikovsky"): {
                    "name": "Pyotr Ilyich Tchaikovsky",
                    "canonical_name": "Pyotr Ilyich Tchaikovsky",
                    "canonical_name_norm": pmda._norm_artist_key("Pyotr Ilyich Tchaikovsky"),
                    "canonical_mbid": "mbid-1",
                },
                pmda._norm_artist_key("Peter Tchaikovsky"): {
                    "name": "Peter Tchaikovsky",
                    "canonical_name": "Pyotr Ilyich Tchaikovsky",
                    "canonical_name_norm": pmda._norm_artist_key("Pyotr Ilyich Tchaikovsky"),
                    "canonical_mbid": "mbid-1",
                },
            }
            fake_entities = [
                [
                    {
                        "name": "Peter Tchaikovsky",
                        "norm": pmda._norm_artist_key("Peter Tchaikovsky"),
                        "role": "composer",
                        "is_primary": False,
                        "has_image": False,
                        "image_path": "",
                        "canonical_name": "Pyotr Ilyich Tchaikovsky",
                        "canonical_norm": pmda._norm_artist_key("Pyotr Ilyich Tchaikovsky"),
                        "canonical_mbid": "mbid-1",
                    }
                ],
                [
                    {
                        "name": "Pyotr Ilyich Tchaikovsky",
                        "norm": pmda._norm_artist_key("Pyotr Ilyich Tchaikovsky"),
                        "role": "composer",
                        "is_primary": False,
                        "has_image": False,
                        "image_path": "",
                        "canonical_name": "Pyotr Ilyich Tchaikovsky",
                        "canonical_norm": pmda._norm_artist_key("Pyotr Ilyich Tchaikovsky"),
                        "canonical_mbid": "mbid-1",
                    }
                ],
            ]
            with mock.patch.object(pmda, "_files_extract_browse_entities_for_album", side_effect=fake_entities):
                entity_map, _links = pmda._build_files_browse_artist_entities(artists_map, [{}, {}])
            merged = entity_map.get(pmda._norm_artist_key("Pyotr Ilyich Tchaikovsky")) or {}
            self.assertEqual(merged.get("name"), "Pyotr Ilyich Tchaikovsky")
        finally:
            pmda.CLASSICAL_NAME_PREFERENCE = original_pref


if __name__ == "__main__":
    unittest.main()
