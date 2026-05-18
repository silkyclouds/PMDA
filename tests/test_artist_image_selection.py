import unittest
from unittest import mock
import tempfile

import pmda
from PIL import Image


class ArtistImageSelectionTests(unittest.TestCase):
    class _DummyTx:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _DummyCursor:
        def __init__(self, responses):
            self._responses = list(responses)
            self._last = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            if self._responses:
                self._last = self._responses.pop(0)
            else:
                self._last = None

        def fetchone(self):
            if isinstance(self._last, list):
                return self._last[0] if self._last else None
            return self._last

        def fetchall(self):
            if isinstance(self._last, list):
                return self._last
            return []

    class _DummyConn:
        def __init__(self, responses):
            self._responses = list(responses)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def close(self):
            return None

        def cursor(self):
            return ArtistImageSelectionTests._DummyCursor(self._responses)

        def transaction(self):
            return ArtistImageSelectionTests._DummyTx()

    class _DummyHttpResponse:
        def __init__(self, payload, status_code=200, text=""):
            self._payload = payload
            self.status_code = status_code
            self.content = b"{}"
            self.text = text

        def json(self):
            return self._payload

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

    def test_musicbrainz_wikimedia_artist_image_requires_refresh(self):
        self.assertTrue(
            pmda._artist_external_image_requires_authoritative_refresh(
                provider="musicbrainz",
                image_url="https://commons.wikimedia.org/wiki/Special:FilePath/Loz%20bradford%20reed.png",
                entity_kind="artist",
                role_hints=[],
            )
        )

    def test_resolve_artist_mbid_for_fanart_requires_trusted_album_identity(self):
        with mock.patch.object(pmda, "_has_trusted_album_identity", return_value=False), \
             mock.patch.object(pmda.musicbrainzngs, "get_release_by_id") as get_release:
            resolved = pmda._resolve_artist_mbid_for_fanart(
                artist_name="Example Artist",
                musicbrainz_id="11111111-1111-1111-1111-111111111111",
            )
        self.assertEqual(resolved, "")
        get_release.assert_not_called()

    def test_resolve_artist_mbid_for_fanart_uses_musicbrainz_release_artist_credit(self):
        artist_mbid = "22222222-2222-2222-2222-222222222222"
        with mock.patch.object(pmda, "_has_trusted_album_identity", return_value=True), \
             mock.patch.object(pmda, "USE_MUSICBRAINZ", True), \
             mock.patch.object(
                 pmda.musicbrainzngs,
                 "get_release_by_id",
                 return_value={"release": {"artist-credit": [{"artist": {"id": artist_mbid}}]}},
             ):
            resolved = pmda._resolve_artist_mbid_for_fanart(
                artist_name="Example Artist",
                musicbrainz_id="11111111-1111-1111-1111-111111111111",
            )
        self.assertEqual(resolved, artist_mbid)

    def test_artist_image_exact_name_match_rejects_partial_name_overlap(self):
        self.assertFalse(
            pmda._artist_image_exact_name_match(
                "Bradford Reed",
                "Lou Reed",
                entity_kind="artist",
                role_hints=[],
                alias_candidates=["Bradford Reed"],
            )
        )
        self.assertFalse(
            pmda._artist_image_exact_name_match(
                "Charles McGregor",
                "Ray Charles",
                entity_kind="artist",
                role_hints=[],
                alias_candidates=["Charles McGregor"],
            )
        )

    def test_bandcamp_cover_candidates_expand_urls_without_letter_prefix(self):
        candidates = pmda._bandcamp_cover_url_candidates(
            "https://f4.bcbits.com/img/0038811796_21.jpg"
        )
        self.assertIn("https://f4.bcbits.com/img/0038811796_0.jpg", candidates)
        self.assertIn("https://f4.bcbits.com/img/0038811796_10.jpg", candidates)

    def test_bandcamp_preferred_image_url_promotes_small_owner_thumbnail(self):
        self.assertEqual(
            pmda._bandcamp_preferred_image_url("https://f4.bcbits.com/img/0038811796_21.jpg"),
            "https://f4.bcbits.com/img/0038811796_10.jpg",
        )

    def test_fetch_bandcamp_artist_profile_hint_promotes_owner_image_url(self):
        class _ConnCtx:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def cursor(self):
                return ArtistImageSelectionTests._DummyCursor(
                    [[("https://bradfordreed.bandcamp.com/album/example", "Example Album")]]
                )

        with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
             mock.patch.object(pmda, "_files_pg_connection", return_value=_ConnCtx()), \
             mock.patch.object(
                 pmda,
                 "_fetch_bandcamp_album_info",
                 return_value={
                     "page_owner_name": "Bradford Reed",
                     "page_owner_bio": "Bradford Reed is an American musician.",
                     "page_owner_image_url": "https://f4.bcbits.com/img/0038811796_21.jpg",
                     "page_owner_url": "https://bradfordreed.bandcamp.com",
                 },
             ):
            payload = pmda._fetch_bandcamp_artist_profile_hint("Bradford Reed") or {}
        self.assertEqual(
            payload.get("image_url"),
            "https://f4.bcbits.com/img/0038811796_10.jpg",
        )

    def test_artist_has_true_image_sql_rejects_orphan_media_cache_paths(self):
        sql = pmda._artist_has_true_image_sql("a", "ext")
        self.assertIn("/media_cache/artist/", sql)

    def test_artist_profile_text_looks_biographical_rejects_generic_definition(self):
        self.assertFalse(
            pmda._artist_profile_text_looks_biographical(
                "Bruit Blanc",
                "Bruit blanc is white noise generated by combining all audible frequencies at equal intensity.",
                entity_kind="artist",
                role_hints=["artist"],
            )
        )

    def test_artist_profile_text_looks_biographical_accepts_real_artist_bio(self):
        self.assertTrue(
            pmda._artist_profile_text_looks_biographical(
                "Bruit Blanc",
                "Bruit Blanc is a French experimental music project founded in Paris that released its debut cassette in 2018.",
                entity_kind="artist",
                role_hints=["artist"],
            )
        )

    def test_reconcile_artist_image_cache_state_clears_orphan_mirrored_local_cache(self):
        class _Cursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql, params=None):
                return None

        class _Conn:
            def cursor(self):
                return _Cursor()

        conn = _Conn()
        with mock.patch.object(pmda, "_artist_image_path_is_mirrored_media_cache", return_value=True), \
             mock.patch.object(pmda, "_artist_external_cached_image_is_valid_exact", return_value=False), \
             mock.patch.object(pmda, "_files_clear_external_artist_image_cache") as clear_mock:
            local_path, ext_path, ext_ok = pmda._files_reconcile_artist_image_cache_state(
                conn,
                artist_name="Charles McGregor",
                artist_norm="charles mcgregor",
                entity_kind="artist",
                role_hints=[],
                local_image_path="/config/media_cache/artist/aa/bb/orphan.webp",
                ext_image_path="",
                ext_artist_name="",
                ext_provider="",
                ext_image_url="",
            )
        self.assertEqual(local_path, "")
        self.assertEqual(ext_path, "")
        self.assertFalse(ext_ok)
        clear_mock.assert_not_called()

    def test_artist_image_provider_policy_allows_wikipedia(self):
        self.assertTrue(
            pmda._artist_image_provider_allowed_for_entity(
                "wikipedia",
                entity_kind="composer",
                role_hints=["composer"],
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

    def test_lastfm_exact_page_title_accepts_opaque_cdn_image_for_person(self):
        self.assertTrue(
            pmda._artist_image_url_looks_relevant(
                "https://lastfm.freetls.fastly.net/i/u/ar0/4e71f666d5a0e5c38a295d924eb35f13.jpg",
                artist_name="Brain Barricade",
                entity_kind="artist",
                role_hints=[],
                page_title="Brain Barricade",
                page_summary="",
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

    def test_build_single_artist_profile_payload_prefers_exact_bandcamp_bio_when_wiki_absent(self):
        with mock.patch.object(pmda, "_musicbrainz_artist_identity_lookup", return_value={}):
            with mock.patch.object(
                pmda,
                "_fetch_lastfm_artist_info",
                return_value={
                    "bio": "",
                    "short_bio": "",
                    "tags": ["minimal"],
                    "similar": [{"name": "Closer Musik"}],
                    "source": "lastfm",
                },
            ):
                with mock.patch.object(pmda, "_fetch_wikipedia_artist_bio_best", return_value={}):
                    with mock.patch.object(
                        pmda,
                        "_fetch_bandcamp_artist_profile_hint",
                        return_value={
                            "bio": "Matias Aguayo is a Chilean-German musician, singer and producer based in Berlin. He co-founded Closer Musik and released solo records on Kompakt.",
                            "short_bio": "Matias Aguayo is a Chilean-German musician and producer based in Berlin.",
                            "source": "bandcamp",
                        },
                    ):
                        with mock.patch.object(pmda, "_fetch_discogs_artist_profile_info", return_value={}):
                            profile, lastfm_info, wiki_info = pmda._build_single_artist_profile_payload("Matias Aguayo")
        self.assertEqual(profile.get("source"), "bandcamp")
        self.assertIn("Chilean-German musician", profile.get("bio") or "")
        self.assertEqual(lastfm_info.get("tags"), ["minimal"])
        self.assertEqual(wiki_info, {})

    def test_build_single_artist_profile_payload_rejects_non_music_wikipedia_bio(self):
        with mock.patch.object(pmda, "_musicbrainz_artist_identity_lookup", return_value={}):
            with mock.patch.object(
                pmda,
                "_fetch_lastfm_artist_info",
                return_value={"bio": "", "short_bio": "", "tags": [], "similar": [], "source": "lastfm"},
            ):
                with mock.patch.object(
                    pmda,
                    "_fetch_wikipedia_artist_bio_best",
                    return_value={
                        "bio": "Benjamin Franklin McGregor was a farmer and political figure in rural Canada.",
                        "short_bio": "Farmer and political figure.",
                        "source": "wikipedia:en",
                    },
                ):
                    with mock.patch.object(pmda, "_fetch_bandcamp_artist_profile_hint", return_value={}):
                        with mock.patch.object(pmda, "_fetch_discogs_artist_profile_info", return_value={}):
                            with mock.patch.object(pmda, "_fetch_musicbrainz_artist_profile_info", return_value={}):
                                profile, _lastfm_info, wiki_info = pmda._build_single_artist_profile_payload("Charles McGregor")
        self.assertEqual(profile.get("source"), "")
        self.assertEqual(profile.get("bio"), "")
        self.assertEqual(wiki_info, {})

    def test_build_single_artist_profile_payload_merges_lastfm_and_musicbrainz_tags_and_similar(self):
        with mock.patch.object(pmda, "_musicbrainz_artist_identity_lookup", return_value={}):
            with mock.patch.object(
                pmda,
                "_fetch_lastfm_artist_info",
                return_value={
                    "bio": "",
                    "short_bio": "",
                    "tags": ["minimal", "electronic"],
                    "similar": [{"name": "Closer Musik", "mbid": "lf-1", "type": "Last.fm"}],
                    "source": "lastfm",
                },
            ):
                with mock.patch.object(pmda, "_fetch_wikipedia_artist_bio_best", return_value={}):
                    with mock.patch.object(pmda, "_fetch_bandcamp_artist_profile_hint", return_value={}):
                        with mock.patch.object(pmda, "_fetch_discogs_artist_profile_info", return_value={}):
                            with mock.patch.object(
                                pmda,
                                "_fetch_musicbrainz_artist_profile_info",
                                return_value={
                                    "bio": "",
                                    "short_bio": "",
                                    "tags": ["electronic", "microhouse"],
                                    "similar": [
                                        {"name": "Closer Musik", "mbid": "lf-1", "type": "similar to"},
                                        {"name": "Ada", "mbid": "mb-2", "type": "tag: minimal"},
                                    ],
                                    "source": "musicbrainz",
                                },
                            ):
                                profile, _lastfm_info, _wiki_info = pmda._build_single_artist_profile_payload("Matias Aguayo")
        self.assertEqual(profile.get("tags"), ["minimal", "electronic", "microhouse"])
        self.assertEqual(
            [item.get("name") for item in (profile.get("similar") or [])],
            ["Closer Musik", "Ada"],
        )

    def test_artist_profile_payload_requires_refresh_for_bad_wikipedia(self):
        self.assertTrue(
            pmda._artist_profile_payload_requires_refresh(
                {
                    "source": "wikipedia:en",
                    "bio": "Benjamin Franklin McGregor was a farmer and political figure in rural Canada.",
                    "short_bio": "Farmer and political figure.",
                }
            )
        )

    def test_artist_profile_payload_does_not_require_refresh_for_good_provider_bio(self):
        self.assertFalse(
            pmda._artist_profile_payload_requires_refresh(
                {
                    "source": "bandcamp",
                    "bio": "Matias Aguayo is a Chilean-German musician, singer and producer based in Berlin.",
                    "short_bio": "Matias Aguayo is a Chilean-German musician and producer based in Berlin.",
                }
            )
        )

    def test_artist_profile_payload_requires_refresh_for_good_wikipedia_bio(self):
        self.assertTrue(
            pmda._artist_profile_payload_requires_refresh(
                {
                    "source": "wikipedia:en",
                    "bio": "Matias Aguayo is a Chilean-German musician, singer and producer based in Berlin.",
                    "short_bio": "Matias Aguayo is a Chilean-German musician and producer based in Berlin.",
                }
            )
        )

    def test_artist_cached_image_provider_is_not_provider_first_for_wikipedia(self):
        self.assertFalse(
            pmda._artist_cached_image_provider_is_provider_first(
                provider="wikipedia",
                image_url="https://upload.wikimedia.org/example.jpg",
                entity_kind="artist",
                role_hints=[],
            )
        )
        self.assertTrue(
            pmda._artist_cached_image_provider_is_provider_first(
                provider="discogs",
                image_url="https://img.discogs.com/example.jpg",
                entity_kind="artist",
                role_hints=[],
            )
        )

    def test_artist_image_asset_endpoint_lookup_key_uses_version_hint(self):
        seen_keys = []

        def _cache_get(key):
            seen_keys.append(str(key))
            return None

        with pmda.app.test_request_context("/api/library/files/artist/123/image?size=320&v=99"):
            with mock.patch.object(pmda, "_get_library_mode", return_value="files"):
                with mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)):
                    with mock.patch.object(pmda, "_files_cache_get_json", side_effect=_cache_get):
                        with mock.patch.object(pmda, "_files_pg_connect", return_value=None):
                            resp = pmda.api_library_files_artist_image(123)
        self.assertEqual(resp[1], 503)
        self.assertIn("artwork:artist:123:v99", seen_keys)

    def test_files_cache_external_artist_image_rejects_musicbrainz_wikimedia_url(self):
        cached = pmda._files_cache_external_artist_image(
            mock.Mock(),
            artist_name="Bradford Reed",
            provider="musicbrainz",
            image_url="https://commons.wikimedia.org/wiki/Special:FilePath/Loz%20bradford%20reed.png",
            entity_kind="artist",
            role_hints=[],
        )
        self.assertIsNone(cached)

    def test_files_get_external_artist_images_drops_empty_rows(self):
        conn = self._DummyConn(
            responses=[
                [("bing satellites", "Bing Satellites", "musicbrainz", "", "", None)],
                None,
            ]
        )
        out = pmda._files_get_external_artist_images(conn, ["bing satellites"])
        self.assertEqual(out, {})

    def test_files_get_external_artist_images_keeps_wikipedia_rows(self):
        conn = self._DummyConn(
            responses=[
                [("charles mcgregor", "Charles McGregor", "wikipedia", "/tmp/charles.webp", "https://upload.wikimedia.org/wikipedia/commons/1/12/Charles_McGregor.jpg", None)],
                None,
            ]
        )
        out = pmda._files_get_external_artist_images(conn, ["charles mcgregor"])
        self.assertIn("charles mcgregor", out)
        self.assertEqual(out["charles mcgregor"]["provider"], "wikipedia")

    def test_files_get_external_artist_images_keeps_small_exact_lastfm_cache(self):
        image = Image.effect_noise((188, 169), 100).convert("RGB")
        with tempfile.NamedTemporaryFile(suffix=".png") as cached_img:
            image.save(cached_img.name, format="PNG")
            conn = self._DummyConn(
                responses=[
                    [("brain barricade", "Brain Barricade", "lastfm", cached_img.name, "https://lastfm.freetls.fastly.net/i/u/ar0/4e71f666d5a0e5c38a295d924eb35f13.jpg", None)],
                    None,
                ]
            )
            out = pmda._files_get_external_artist_images(conn, ["brain barricade"])
        self.assertIn("brain barricade", out)
        self.assertEqual(out["brain barricade"]["provider"], "lastfm")

    def test_files_upsert_external_artist_image_deletes_empty_payload(self):
        executed: list[str] = []

        class _Cursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql, params=None):
                executed.append(str(sql or "").strip())

        class _Conn:
            def cursor(self):
                return _Cursor()

        pmda._files_upsert_external_artist_image(
            _Conn(),
            name_norm="bing satellites",
            artist_name="Bing Satellites",
            provider="musicbrainz",
            image_path="",
            image_url="",
        )
        self.assertEqual(len(executed), 1)
        self.assertIn("DELETE FROM files_external_artist_images", executed[0])

    def test_files_resolve_artist_cache_name_norm_prefers_canonical_artist_row(self):
        conn = self._DummyConn(
            responses=[
                [],
                [("carl loewe",)],
            ]
        )
        resolved = pmda._files_resolve_artist_cache_name_norm(
            conn,
            artist_name="Johann Carl Gottfried Loewe",
            artist_norm="johann carl gottfried loewe",
        )
        self.assertEqual(resolved, "carl loewe")

    def test_files_cache_external_artist_image_upserts_using_canonical_norm(self):
        conn = self._DummyConn(responses=[])
        with mock.patch.object(pmda, "_files_resolve_artist_cache_name_norm", return_value="carl loewe"), \
             mock.patch.object(pmda, "_files_get_external_artist_images", return_value={}), \
             mock.patch.object(
                 pmda,
                 "_download_best_cover_image",
                 return_value=(b"image-bytes", "image/jpeg", "https://upload.wikimedia.org/wikipedia/commons/5/52/Carl_Loewe.jpg"),
             ), \
             mock.patch.object(pmda, "_artist_image_url_looks_relevant", return_value=True), \
             mock.patch.object(pmda, "_is_usable_artist_image_bytes", return_value=True), \
             mock.patch.object(pmda, "_files_artist_reference_folder", return_value=None), \
             mock.patch.object(pmda, "_ensure_cached_image_from_bytes", return_value=pmda.Path("/tmp/carl-loewe.webp")), \
             mock.patch.object(pmda, "_files_upsert_external_artist_image") as upsert_mock:
            out = pmda._files_cache_external_artist_image(
                conn,
                artist_name="Johann Carl Gottfried Loewe",
                artist_norm="carl loewe",
                provider="wikipedia",
                image_url="https://upload.wikimedia.org/wikipedia/commons/5/52/Carl_Loewe.jpg",
                entity_kind="composer",
                role_hints=["composer"],
                page_title="Johann Carl Gottfried Loewe",
            )
        self.assertEqual(out, "/tmp/carl-loewe.webp")
        upsert_mock.assert_called_once()
        self.assertEqual(upsert_mock.call_args.kwargs.get("name_norm"), "carl loewe")

    def test_files_relink_external_artist_images_to_canonical_norm_moves_orphan_row(self):
        executed: list[tuple[str, object]] = []

        class _Cursor:
            def __init__(self, responses):
                self._responses = list(responses)
                self._last = None

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql, params=None):
                executed.append((str(sql or "").strip(), params))
                if self._responses:
                    self._last = self._responses.pop(0)
                else:
                    self._last = None

            def fetchone(self):
                if isinstance(self._last, list):
                    return self._last[0] if self._last else None
                return self._last

            def fetchall(self):
                if isinstance(self._last, list):
                    return self._last
                return []

        class _Conn:
            def __init__(self):
                self._responses = [
                    None,
                    [("johann carl gottfried loewe", "Johann Carl Gottfried Loewe", "wikipedia", "/tmp/loewe.webp", "https://upload.wikimedia.org/wikipedia/commons/5/52/Carl_Loewe.jpg")],
                    None,
                    None,
                ]

            def cursor(self):
                return _Cursor(self._responses)

        with mock.patch.object(pmda, "_files_resolve_artist_cache_name_norm", return_value="carl loewe"), \
             mock.patch.object(pmda, "_files_upsert_external_artist_image") as upsert_mock:
            pmda._files_relink_external_artist_images_to_canonical_norm(_Conn())

        upsert_mock.assert_called_once()
        self.assertEqual(upsert_mock.call_args.kwargs.get("name_norm"), "carl loewe")
        self.assertTrue(any("DELETE FROM files_external_artist_images" in sql for sql, _params in executed))

    def test_files_relink_external_artist_images_for_artist_moves_exact_name_row(self):
        executed: list[tuple[str, object]] = []

        class _Cursor:
            def __init__(self, responses):
                self._responses = list(responses)
                self._last = None

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql, params=None):
                executed.append((str(sql or "").strip(), params))
                if self._responses:
                    self._last = self._responses.pop(0)
                else:
                    self._last = None

            def fetchone(self):
                if isinstance(self._last, list):
                    return self._last[0] if self._last else None
                return self._last

            def fetchall(self):
                if isinstance(self._last, list):
                    return self._last
                return []

        class _Conn:
            def __init__(self):
                self._responses = [
                    [("johann carl gottfried loewe", "Johann Carl Gottfried Loewe", "wikipedia", "/tmp/loewe.webp", "https://upload.wikimedia.org/wikipedia/commons/5/52/Carl_Loewe.jpg")],
                    None,
                ]

            def cursor(self):
                return _Cursor(self._responses)

        with mock.patch.object(pmda, "_files_upsert_external_artist_image") as upsert_mock:
            moved = pmda._files_relink_external_artist_images_for_artist(
                _Conn(),
                artist_name="Johann Carl Gottfried Loewe",
                artist_norm="carl loewe",
                alias_candidates=[],
            )
        self.assertEqual(moved, 1)
        upsert_mock.assert_called_once()
        self.assertEqual(upsert_mock.call_args.kwargs.get("name_norm"), "carl loewe")
        self.assertTrue(any("DELETE FROM files_external_artist_images" in sql for sql, _params in executed))

    def test_fetch_lastfm_artist_info_returns_mbid(self):
        payload = {
            "artist": {
                "name": "Matias Aguayo",
                "mbid": "lf-mbid-123",
                "bio": {
                    "summary": "Matias Aguayo is a Chilean-German musician and producer based in Berlin.",
                    "content": "",
                },
                "tags": {"tag": [{"name": "minimal"}]},
                "similar": {"artist": []},
                "image": [{"#text": "https://lastfm.freetls.fastly.net/i/u/300x300/matias.jpg", "size": "large"}],
            }
        }
        with mock.patch.object(pmda, "USE_LASTFM", True), \
             mock.patch.object(pmda, "LASTFM_API_KEY", "test-key"), \
             mock.patch.object(pmda.requests, "get", return_value=self._DummyHttpResponse(payload)):
            info = pmda._fetch_lastfm_artist_info("Matias Aguayo")
        self.assertIsInstance(info, dict)
        self.assertEqual(info.get("mbid"), "lf-mbid-123")
        self.assertEqual(info.get("matched_name"), "Matias Aguayo")

    def test_fetch_lastfm_artist_info_falls_back_to_exact_html_without_api_key(self):
        html_doc = """
        <html><head>
        <title>Brian Jackson music, videos, stats, and photos | Last.fm</title>
        <meta property="og:title" content="Brian Jackson music, videos, stats, and photos | Last.fm" />
        <meta property="og:description" content="Listen to music from Brian Jackson like Home is Where The Hatred Is and more." />
        <meta property="og:image" content="https://lastfm.freetls.fastly.net/i/u/ar0/02c6fac254c74955a4eccae1f773d6ae.jpg" />
        </head><body></body></html>
        """
        with mock.patch.object(pmda, "USE_LASTFM", True), \
             mock.patch.object(pmda, "LASTFM_API_KEY", ""), \
             mock.patch.object(pmda.requests, "get", return_value=self._DummyHttpResponse({}, text=html_doc)):
            info = pmda._fetch_lastfm_artist_info("Brian Jackson")
        self.assertIsInstance(info, dict)
        self.assertEqual(info.get("matched_name"), "Brian Jackson")
        self.assertEqual(
            info.get("image_url"),
            "https://lastfm.freetls.fastly.net/i/u/ar0/02c6fac254c74955a4eccae1f773d6ae.jpg",
        )
        self.assertEqual(info.get("source"), "lastfm")

    def test_fetch_lastfm_artist_info_html_fallback_rejects_partial_name_overlap(self):
        html_doc = """
        <html><head>
        <title>Lou Reed music, videos, stats, and photos | Last.fm</title>
        <meta property="og:title" content="Lou Reed music, videos, stats, and photos | Last.fm" />
        <meta property="og:image" content="https://lastfm.freetls.fastly.net/i/u/ar0/loureed.jpg" />
        </head><body></body></html>
        """
        with mock.patch.object(pmda, "USE_LASTFM", True), \
             mock.patch.object(pmda, "LASTFM_API_KEY", ""), \
             mock.patch.object(pmda.requests, "get", return_value=self._DummyHttpResponse({}, text=html_doc)):
            info = pmda._fetch_lastfm_artist_info("Bradford Reed")
        self.assertIsNone(info)

    def test_fetch_lastfm_artist_info_augments_api_result_with_exact_html_image(self):
        api_payload = {
            "artist": {
                "name": "Brian Jackson",
                "mbid": "lf-brian-1",
                "bio": {"summary": "", "content": ""},
                "tags": {"tag": [{"name": "soul"}]},
                "similar": {"artist": []},
                "image": [],
            }
        }
        html_doc = """
        <html><head>
        <title>Brian Jackson music, videos, stats, and photos | Last.fm</title>
        <meta property="og:title" content="Brian Jackson music, videos, stats, and photos | Last.fm" />
        <meta property="og:description" content="Listen to music from Brian Jackson." />
        <meta property="og:image" content="https://lastfm.freetls.fastly.net/i/u/ar0/02c6fac254c74955a4eccae1f773d6ae.jpg" />
        </head><body></body></html>
        """
        with mock.patch.object(pmda, "USE_LASTFM", True), \
             mock.patch.object(pmda, "LASTFM_API_KEY", "test-key"), \
             mock.patch.object(
                 pmda.requests,
                 "get",
                 side_effect=[
                     self._DummyHttpResponse(api_payload),
                     self._DummyHttpResponse({}, text=html_doc),
                 ],
             ):
            info = pmda._fetch_lastfm_artist_info("Brian Jackson")
        self.assertIsInstance(info, dict)
        self.assertEqual(info.get("matched_name"), "Brian Jackson")
        self.assertEqual(info.get("mbid"), "lf-brian-1")
        self.assertEqual(
            info.get("image_url"),
            "https://lastfm.freetls.fastly.net/i/u/ar0/02c6fac254c74955a4eccae1f773d6ae.jpg",
        )

    def test_artist_image_asset_endpoint_preserves_db_ext_artist_name_when_cache_empty(self):
        responses = [
            (
                "bing satellites",
                "",
                "Bing Satellites",
                "artist",
                "[]",
                "/config/media_cache/artist/test.webp",
                "Bing Satellites",
                "bandcamp",
                "https://f4.bcbits.com/img/a1481646903_0.jpg",
            )
        ]
        seen = {}

        def _reconcile(_conn, **kwargs):
            seen.update(kwargs)
            return "", kwargs.get("ext_image_path") or "", True

        with pmda.app.test_request_context("/api/library/files/artist/327/image?size=320&v=1"):
            with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
                 mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
                 mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
                 mock.patch.object(pmda, "_files_pg_connect", return_value=self._DummyConn(responses)), \
                 mock.patch.object(pmda, "_files_cache_set_json"), \
                 mock.patch.object(pmda, "_files_reconcile_artist_image_cache_state", side_effect=_reconcile), \
                 mock.patch.object(pmda, "path_for_fs_access", side_effect=lambda p: p), \
                 mock.patch.object(pmda.Path, "exists", return_value=True), \
                 mock.patch.object(pmda.Path, "is_file", return_value=True), \
                 mock.patch.object(pmda, "_is_media_cache_file", return_value=True), \
                 mock.patch.object(pmda, "_ensure_cached_image_for_path", return_value=pmda.Path("/config/media_cache/artist/test.webp")), \
                 mock.patch.object(pmda, "_serve_image_file_cached", return_value="served"):
                resp = pmda.api_library_files_artist_image(327)

        self.assertEqual(resp, "served")
        self.assertEqual(seen.get("ext_artist_name"), "Bing Satellites")

    def test_fetch_bandcamp_artist_profile_hint_requires_exact_owner_name(self):
        class _Cursor:
            def __init__(self):
                self._rows = []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql, params=None):
                query = str(sql or "")
                if "FROM files_albums alb" in query:
                    self._rows = [("https://example.bandcamp.com/album/hyperdot", "HyperDot")]
                else:
                    self._rows = []

            def fetchall(self):
                return list(self._rows)

        class _Conn:
            def cursor(self):
                return _Cursor()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with mock.patch.object(pmda, "_files_pg_connection", return_value=_Conn()):
            with mock.patch.object(
                pmda,
                "_fetch_bandcamp_album_info",
                return_value={
                    "page_owner_name": "Charles",
                    "page_owner_bio": "Short bio",
                    "page_owner_image_url": "https://f4.bcbits.com/img/a1234567890_16.jpg",
                    "page_owner_url": "https://example.bandcamp.com",
                },
            ):
                self.assertIsNone(
                    pmda._fetch_bandcamp_artist_profile_hint(
                        "Charles McGregor",
                        alias_candidates=["Charles McGregor"],
                    )
                )
        with mock.patch.object(pmda, "_files_pg_connection", return_value=_Conn()):
            with mock.patch.object(
                pmda,
                "_fetch_bandcamp_album_info",
                return_value={
                    "page_owner_name": "Charles McGregor",
                    "page_owner_bio": "Charles McGregor is a composer for games and film based in Scotland, known for the HyperDot soundtrack.",
                    "page_owner_image_url": "https://f4.bcbits.com/img/a1234567890_16.jpg",
                    "page_owner_url": "https://example.bandcamp.com",
                },
            ):
                payload = pmda._fetch_bandcamp_artist_profile_hint(
                    "Charles McGregor",
                    alias_candidates=["Charles McGregor"],
                )
        self.assertIsInstance(payload, dict)
        self.assertEqual(payload.get("matched_name"), "Charles McGregor")
        self.assertEqual(payload.get("source"), "bandcamp")

    def test_files_try_artist_image_refresh_replaces_exact_wikipedia_cache_with_provider_image(self):
        with tempfile.NamedTemporaryFile(suffix=".webp") as existing_img:
            existing_img.write(b"x" * 9000)
            existing_img.flush()

            conn = self._DummyConn(responses=[(True, existing_img.name)])
            with mock.patch.object(pmda, "_files_pg_connection", return_value=conn), \
                 mock.patch.object(
                     pmda,
                     "_files_get_external_artist_images",
                     return_value={
                         "matias aguayo": {
                             "artist_name": "Matias Aguayo",
                             "provider": "wikipedia",
                             "image_path": existing_img.name,
                             "image_url": "https://upload.wikimedia.org/wiki/example.jpg",
                             "stale": False,
                         }
                     },
                 ), \
                 mock.patch.object(pmda, "_files_get_artist_alias_candidates", return_value=["Matias Aguayo"]), \
                 mock.patch.object(pmda, "_artist_image_path_is_mirrored_media_cache", return_value=True), \
                 mock.patch.object(pmda, "_is_usable_artist_image_path", return_value=True), \
                 mock.patch.object(
                     pmda,
                     "_fetch_bandcamp_artist_profile_hint",
                     return_value={
                         "image_url": "https://f4.bcbits.com/img/a1234567890_0.jpg",
                         "matched_name": "Matias Aguayo",
                         "bio": "Matias Aguayo is a Chilean-German musician and producer.",
                         "source": "bandcamp",
                     },
                 ), \
                 mock.patch.object(pmda, "_fetch_discogs_artist_profile_info", return_value={}), \
                 mock.patch.object(pmda, "_fetch_musicbrainz_artist_profile_info", return_value={}), \
                 mock.patch.object(pmda, "_fetch_lastfm_artist_info", return_value={}), \
                 mock.patch.object(pmda, "_fetch_artist_image_audiodb", return_value=""), \
                 mock.patch.object(pmda, "_files_cache_external_artist_image", return_value="/config/media_cache/artist/matias.webp") as cache_mock:
                refreshed = pmda._files_try_artist_image_refresh(
                    artist_name="Matias Aguayo",
                    artist_norm="matias aguayo",
                    entity_kind="artist",
                    role_hints=[],
                )
        self.assertTrue(refreshed)
        cache_mock.assert_called_once()
        self.assertTrue(bool(cache_mock.call_args.kwargs.get("force_replace")))

    def test_files_attach_similar_artist_refs_preserves_provider_image_when_no_local_match(self):
        similar = [
            {
                "name": "Jürgen Paape",
                "type": "Last.fm",
                "image_url": "https://lastfm.example/jurgen.jpg",
            }
        ]
        with mock.patch.object(pmda, "_files_get_external_artist_images", return_value={}):
            out = pmda._files_attach_similar_artist_refs(self._DummyConn([]), similar, "http://pmda.local")
        self.assertEqual(out[0]["image_url"], "https://lastfm.example/jurgen.jpg")
        self.assertIn("/api/library/external/artist-image/jurgen%20paape", out[0]["image_cached_url"])

    def test_api_library_external_artist_image_attempts_exact_refresh_on_demand(self):
        with tempfile.NamedTemporaryFile(suffix=".webp") as cached_img:
            cached_img.write(b"x" * 9000)
            cached_img.flush()
            conn = self._DummyConn(responses=[[("Matias Aguayo", "artist", "[]")]])
            ext_rows = [
                {},
                {
                    "image_path": cached_img.name,
                    "artist_name": "Matias Aguayo",
                    "provider": "discogs",
                    "image_url": "https://discogs.example/matias.jpg",
                },
            ]
            with pmda.app.test_request_context("/api/library/external/artist-image/matias-aguayo?name=Matias%20Aguayo"):
                with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
                     mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
                     mock.patch.object(pmda, "_files_pg_connect", return_value=conn), \
                     mock.patch.object(pmda, "_files_get_external_artist_images", side_effect=ext_rows) as ext_lookup_mock, \
                     mock.patch.object(pmda, "_files_try_artist_image_refresh", return_value=True) as refresh_mock, \
                     mock.patch.object(pmda, "_enqueue_files_similar_images_warm", return_value=True), \
                     mock.patch.object(pmda, "_is_media_cache_file", return_value=True), \
                     mock.patch.object(pmda, "_ensure_cached_image_for_path", return_value=None), \
                     mock.patch.object(pmda, "_serve_image_file_cached", return_value="served") as serve_mock:
                    pmda.api_library_external_artist_image("matias-aguayo")
        refresh_mock.assert_called_once()
        self.assertGreaterEqual(ext_lookup_mock.call_count, 2)

    def test_assistant_tool_library_top_artists_omits_thumb_without_true_image(self):
        conn = self._DummyConn(
            responses=[
                [
                    (
                        12,
                        "Bing Satellites",
                        "bing satellites",
                        10,
                        101,
                        False,
                    )
                ]
            ]
        )
        out = pmda._assistant_tool_library_top_artists(conn, base_url="http://pmda.local", limit=12)
        self.assertEqual(len(out), 1)
        self.assertIsNone(out[0].get("thumb"))

    def test_api_library_artists_omits_thumb_until_exact_image_exists(self):
        conn = self._DummyConn(
            responses=[
                [(1,)],
                [
                    (
                        12,
                        "Bing Satellites",
                        "",
                        "artist",
                        "[]",
                        "[]",
                        10,
                        0,
                        False,
                        1234567890,
                    )
                ],
            ]
        )
        with pmda.app.test_request_context("/api/library/artists?limit=20&offset=0"):
            with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
                 mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
                 mock.patch.object(pmda, "_files_cache_set_json", return_value=None), \
                 mock.patch.object(pmda, "_files_pg_connect", return_value=conn), \
                 mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
                 mock.patch.object(pmda, "_library_include_unmatched_effective", return_value=False), \
                 mock.patch.object(pmda, "_trigger_files_profile_backfill_async", return_value=True), \
                 mock.patch.object(pmda, "_enqueue_files_profile_enrichment", return_value=True):
                response = pmda.api_library_artists()
        payload = response.get_json()
        self.assertEqual(payload["artists"][0]["artist_name"], "Bing Satellites")
        self.assertFalse(payload["artists"][0]["artist_has_image"])
        self.assertIsNone(payload["artists"][0]["artist_thumb"])

    def test_api_library_artists_exposes_album_cover_fallback_thumb(self):
        conn = self._DummyConn(
            responses=[
                [(1,)],
                [
                    (
                        12,
                        "Bing Satellites",
                        "",
                        "artist",
                        "[]",
                        "[]",
                        10,
                        0,
                        False,
                        1234567890,
                    )
                ],
                [
                    (
                        12,
                        77,
                        "/music/Bing Satellites/Album/cover.jpg",
                        "/music/Bing Satellites/Album",
                        True,
                    )
                ],
            ]
        )
        with pmda.app.test_request_context("/api/library/artists?limit=20&offset=0"):
            with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
                 mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
                 mock.patch.object(pmda, "_files_cache_set_json", return_value=None), \
                 mock.patch.object(pmda, "_files_pg_connect", return_value=conn), \
                 mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
                 mock.patch.object(pmda, "_library_include_unmatched_effective", return_value=False), \
                 mock.patch.object(pmda, "_files_pg_statement_timeout", side_effect=lambda cur, timeout_ms: self._DummyTx()), \
                 mock.patch.object(pmda, "_resolve_files_album_cover_asset", return_value=(True, "/music/Bing Satellites/Album/cover.jpg")), \
                 mock.patch.object(pmda, "_trigger_files_profile_backfill_async", return_value=True), \
                 mock.patch.object(pmda, "_enqueue_files_profile_enrichment", return_value=True):
                response = pmda.api_library_artists()
        payload = response.get_json()
        self.assertFalse(payload["artists"][0]["artist_has_image"])
        self.assertIsNone(payload["artists"][0]["artist_thumb"])
        self.assertEqual(
            payload["artists"][0]["artist_fallback_thumb"],
            "http://localhost/api/library/files/album/77/cover?size=512",
        )

    def test_api_library_artists_fallback_cover_query_uses_aligned_album_alias(self):
        executed_sql = []

        class _Cursor(self._DummyCursor):
            def execute(self, sql, params=None):
                executed_sql.append(str(sql or ""))
                super().execute(sql, params)

        class _Conn(self._DummyConn):
            def cursor(self):
                return _Cursor(self._responses)

        conn = _Conn(
            responses=[
                [(1,)],
                [
                    (
                        12,
                        "Bing Satellites",
                        "",
                        "artist",
                        "[]",
                        "[]",
                        10,
                        0,
                        False,
                        1234567890,
                    )
                ],
                [],
            ]
        )
        with pmda.app.test_request_context("/api/library/artists?limit=20&offset=0"):
            with mock.patch.object(pmda, "_get_library_mode", return_value="files"), \
                 mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
                 mock.patch.object(pmda, "_files_cache_set_json", return_value=None), \
                 mock.patch.object(pmda, "_files_pg_connect", return_value=conn), \
                 mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)), \
                 mock.patch.object(pmda, "_library_include_unmatched_effective", return_value=False), \
                 mock.patch.object(pmda, "_files_pg_statement_timeout", side_effect=lambda cur, timeout_ms: self._DummyTx()), \
                 mock.patch.object(pmda, "_trigger_files_profile_backfill_async", return_value=True), \
                 mock.patch.object(pmda, "_enqueue_files_profile_enrichment", return_value=True):
                pmda.api_library_artists()

        fallback_sql = next(sql for sql in executed_sql if "SELECT DISTINCT ON (link.artist_id)" in sql)
        self.assertIn("JOIN files_albums alb_last ON alb_last.id = link.album_id", fallback_sql)
        self.assertIn("COALESCE(alb_last.cover_path, '')", fallback_sql)
        self.assertNotIn("COALESCE(alb.cover_path, '')", fallback_sql)

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

    def test_try_artist_image_refresh_purges_wrong_cached_external_image_and_accepts_exact_musicbrainz(self):
        conn = self._DummyConn(
            responses=[
                (False, ""),  # local files_artists image row
                [],  # bandcamp album hints
            ]
        )

        def _exact_name(local_name, candidate_name, **kwargs):
            return pmda._norm_artist_key(local_name) == pmda._norm_artist_key(candidate_name)

        with (
            mock.patch.object(pmda, "_files_pg_connection", return_value=conn),
            mock.patch.object(
                pmda,
                "_files_get_external_artist_images",
                return_value={
                    pmda._norm_artist_key("Charles McGregor"): {
                        "artist_name": "Ray Charles",
                        "provider": "lastfm",
                        "image_path": "/tmp/wrong-ray-charles.jpg",
                        "image_url": "https://example.com/ray.jpg",
                        "stale": False,
                    }
                },
            ),
            mock.patch.object(pmda, "_files_clear_external_artist_image_cache") as clear_cache,
            mock.patch.object(pmda, "_artist_image_exact_name_match", side_effect=_exact_name),
            mock.patch.object(pmda, "_artist_image_provider_allowed_for_entity", return_value=True),
            mock.patch.object(pmda, "_artist_external_image_requires_authoritative_refresh", return_value=False),
            mock.patch.object(
                pmda,
                "_fetch_musicbrainz_artist_profile_info",
                return_value={
                    "image_url": "https://upload.wikimedia.org/charles-mcgregor.jpg",
                    "matched_name": "Charles McGregor",
                    "bio": "Charles McGregor is a composer.",
                    "tags": ["soundtrack"],
                    "mbid": "mbid-1",
                },
            ),
            mock.patch.object(pmda, "_fetch_artist_image_fanart", return_value=""),
            mock.patch.object(pmda, "_files_cache_external_artist_image", return_value="/tmp/charles-mcgregor.jpg") as cache_image,
        ):
            refreshed = pmda._files_try_artist_image_refresh(
                artist_name="Charles McGregor",
                artist_norm=pmda._norm_artist_key("Charles McGregor"),
                entity_kind="artist",
                role_hints=[],
                fast_mode=False,
            )

        self.assertTrue(refreshed)
        clear_cache.assert_called_once()
        cache_image.assert_called_once()
        self.assertEqual(cache_image.call_args.kwargs["provider"], "musicbrainz")

    def test_artist_external_cached_image_is_valid_exact_accepts_exact_wikipedia(self):
        with tempfile.NamedTemporaryFile(suffix=".webp") as cached_img:
            cached_img.write(b"x" * 9000)
            cached_img.flush()
            with mock.patch.object(pmda, "_is_media_cache_file", return_value=True):
                self.assertTrue(
                    pmda._artist_external_cached_image_is_valid_exact(
                        artist_name="Charles McGregor",
                        entity_kind="artist",
                        role_hints=[],
                        alias_candidates=["Charles McGregor"],
                        ext_artist_name="Charles McGregor",
                        ext_image_path=cached_img.name,
                        ext_provider="wikipedia",
                        ext_image_url="https://upload.wikimedia.org/wikipedia/commons/1/12/Charles_McGregor.jpg",
                    )
                )

    def test_artist_external_cached_image_is_valid_exact_keeps_exact_provider_first_cache(self):
        with tempfile.NamedTemporaryFile(suffix=".webp") as cached_img:
            cached_img.write(b"x" * 9000)
            cached_img.flush()
            with mock.patch.object(pmda, "_is_media_cache_file", return_value=True), \
                 mock.patch.object(pmda, "_artist_image_url_looks_relevant", return_value=False):
                self.assertTrue(
                    pmda._artist_external_cached_image_is_valid_exact(
                        artist_name="Dr. Gabba",
                        entity_kind="artist",
                        role_hints=[],
                        alias_candidates=["Dr. Gabba"],
                        ext_artist_name="Dr. Gabba",
                        ext_image_path=cached_img.name,
                        ext_provider="bandcamp",
                        ext_image_url="https://f4.bcbits.com/img/0040211341_10.jpg",
                    )
                )

    def test_artist_external_cached_image_is_valid_exact_accepts_small_exact_lastfm_cache(self):
        image = Image.effect_noise((188, 169), 100).convert("RGB")
        with tempfile.NamedTemporaryFile(suffix=".png") as cached_img:
            image.save(cached_img.name, format="PNG")
            with mock.patch.object(pmda, "_is_media_cache_file", return_value=True):
                self.assertTrue(
                    pmda._artist_external_cached_image_is_valid_exact(
                        artist_name="Brain Barricade",
                        entity_kind="artist",
                        role_hints=[],
                        alias_candidates=["Brain Barricade"],
                        ext_artist_name="Brain Barricade",
                        ext_image_path=cached_img.name,
                        ext_provider="lastfm",
                        ext_image_url="https://lastfm.freetls.fastly.net/i/u/ar0/4e71f666d5a0e5c38a295d924eb35f13.jpg",
                    )
                )

    def test_artist_effective_image_present_accepts_exact_external_cache(self):
        image = Image.effect_noise((404, 450), 100).convert("RGB")
        with tempfile.NamedTemporaryFile(suffix=".png") as cached_img:
            image.save(cached_img.name, format="PNG")
            with mock.patch.object(pmda, "_is_media_cache_file", return_value=True):
                self.assertTrue(
                    pmda._artist_effective_image_present(
                        artist_name="Arrigo Boito",
                        entity_kind="composer",
                        role_hints=["composer"],
                        local_image_path="",
                        ext_image_path=cached_img.name,
                        ext_artist_name="Arrigo Boito",
                        ext_provider="wikipedia",
                        ext_image_url="https://upload.wikimedia.org/wikipedia/commons/3/3a/Arrigo_Boito.jpg",
                    )
                )

    def test_artist_effective_image_present_accepts_exact_opaque_wikimedia_cache(self):
        image = Image.effect_noise((450, 640), 100).convert("RGB")
        with tempfile.NamedTemporaryFile(suffix=".png") as cached_img:
            image.save(cached_img.name, format="PNG")
            with mock.patch.object(pmda, "_is_media_cache_file", return_value=True):
                self.assertTrue(
                    pmda._artist_effective_image_present(
                        artist_name="Arrigo Boito",
                        entity_kind="composer",
                        role_hints=["composer"],
                        local_image_path="",
                        ext_image_path=cached_img.name,
                        ext_artist_name="Arrigo Boito",
                        ext_provider="wikipedia",
                        ext_image_url="https://upload.wikimedia.org/wikipedia/commons/3/3a/Arrigo_Boito_%28before_1918%29_-_Archivio_Storico_Ricordi_FOTO002997.jpg",
                    )
                )

    def test_files_try_artist_image_refresh_uses_exact_wikipedia_seed(self):
        conn = self._DummyConn(responses=[(False, "")])
        with mock.patch.object(pmda, "_files_pg_connection", return_value=conn), \
             mock.patch.object(pmda, "_files_get_external_artist_images", return_value={}), \
             mock.patch.object(pmda, "_files_get_artist_alias_candidates", return_value=["John Cage"]), \
             mock.patch.object(pmda, "_fetch_bandcamp_artist_profile_hint", return_value={}), \
             mock.patch.object(pmda, "_fetch_discogs_artist_profile_info", return_value={}), \
             mock.patch.object(pmda, "_fetch_musicbrainz_artist_profile_info", return_value={}), \
             mock.patch.object(pmda, "_fetch_artist_image_fanart", return_value=""), \
             mock.patch.object(pmda, "_fetch_artist_image_audiodb", return_value=""), \
             mock.patch.object(pmda, "_files_cache_external_artist_image", return_value="/config/media_cache/artist/john-cage.webp") as cache_mock:
            refreshed = pmda._files_try_artist_image_refresh(
                artist_name="John Cage",
                artist_norm=pmda._norm_artist_key("John Cage"),
                entity_kind="composer",
                role_hints=["composer"],
                lastfm_info={},
                wiki_info={
                    "image_url": "https://upload.wikimedia.org/wikipedia/commons/1/12/John_Cage.jpg",
                    "page_title": "John Cage",
                    "page_description": "American composer",
                    "bio": "John Cage was an American composer, music theorist, and artist.",
                },
            )
        self.assertTrue(refreshed)
        cache_mock.assert_called_once()
        self.assertEqual(cache_mock.call_args.kwargs["provider"], "wikipedia")


if __name__ == "__main__":
    unittest.main()
