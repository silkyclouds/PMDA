import unittest
from unittest import mock
import json
import requests
import urllib.request

import pmda


class WebSearchRuntimeTests(unittest.TestCase):
    def test_album_profile_upsert_preserves_existing_review_when_new_payload_has_only_metrics(self):
        class _Cursor:
            def __init__(self):
                self.sql = ""
                self.params = ()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql, params):
                self.sql = str(sql or "")
                self.params = params

        class _Conn:
            def __init__(self):
                self.cur = _Cursor()

            def cursor(self):
                return self.cur

        conn = _Conn()
        pmda._files_upsert_album_profile(
            conn,
            "severed heads",
            pmda.norm_album("Since the Accident"),
            "Since the Accident",
            {
                "description": "",
                "short_description": "",
                "source": "",
                "public_rating": 4.6,
                "public_rating_votes": 109,
            },
        )

        self.assertIn("CASE", conn.cur.sql)
        self.assertIn("files_album_profiles.description", conn.cur.sql)
        self.assertIn("files_album_profiles.short_description", conn.cur.sql)
        self.assertIn("files_album_profiles.source", conn.cur.sql)
        self.assertIn("bandcamp_supporter_comments_json", conn.cur.sql)

    def test_normalize_web_search_provider_rejects_searxng(self):
        self.assertEqual(pmda._normalize_web_search_provider("searxng"), "auto")
        self.assertEqual(pmda._normalize_web_search_provider("SEARXNG"), "auto")

    def test_review_search_source_prefers_ollama(self):
        source = pmda._review_search_source_from_hits(
            [
                {
                    "title": "Sigur Ros - Takk review",
                    "link": "https://example.com/takk-review",
                    "snippet": "Review snippet",
                    "source": "ollama_web_search",
                }
            ]
        )
        self.assertEqual(source, "ollama")

    def test_review_search_source_recognizes_duckduckgo(self):
        source = pmda._review_search_source_from_hits(
            [
                {
                    "title": "Sigur Ros - Takk review",
                    "link": "https://pitchfork.com/reviews/albums/7155-takk/",
                    "snippet": "Review snippet",
                    "source": "duckduckgo",
                }
            ]
        )
        self.assertEqual(source, "duckduckgo")

    def test_fetch_best_album_profile_passes_provider_context_to_web_lookup(self):
        with mock.patch.object(pmda, "_fetch_lastfm_album_info", return_value={}), \
            mock.patch.object(pmda, "_fetch_album_provider_fallbacks_parallel", return_value={}), \
            mock.patch.object(pmda, "_is_relevant_album_profile_text", return_value=True), \
            mock.patch.object(
                pmda,
                "_fetch_album_review_web_ai",
                return_value={
                    "description": "Reliable summary",
                    "short_description": "Reliable summary",
                    "source": "ollama",
                },
            ) as review_lookup:
            profile = pmda._fetch_best_album_profile(
                "Sigur Rós",
                "Takk...",
                allow_web_ai=True,
                metadata_source="musicbrainz",
                mbid="mbid-123",
                discogs_release_id="discogs-456",
                lastfm_album_mbid="lastfm-789",
                bandcamp_album_url="https://artist.bandcamp.com/album/takk",
                strict_match_verified=True,
            )

        self.assertEqual(profile.get("source"), "ollama")
        self.assertTrue(review_lookup.called)
        kwargs = review_lookup.call_args.kwargs
        search_context = kwargs.get("search_context") or {}
        self.assertEqual(search_context.get("metadata_source"), "musicbrainz")
        self.assertEqual(search_context.get("musicbrainz_release_group_id"), "mbid-123")
        self.assertEqual(search_context.get("discogs_release_id"), "discogs-456")
        self.assertEqual(search_context.get("lastfm_album_mbid"), "lastfm-789")
        self.assertEqual(search_context.get("bandcamp_album_url"), "https://artist.bandcamp.com/album/takk")
        self.assertTrue(bool(search_context.get("strict_match_verified")))

    def test_album_profile_fetch_allowed_accepts_provider_backed_soft_matches_without_ai(self):
        self.assertFalse(
            pmda._files_album_profile_fetch_allowed(
                strict_verified=False,
                metadata_source="",
            )
        )
        self.assertTrue(
            pmda._files_album_profile_fetch_allowed(
                strict_verified=False,
                metadata_source="musicbrainz",
            )
        )
        self.assertTrue(
            pmda._files_album_profile_fetch_allowed(
                strict_verified=False,
                bandcamp_album_url="https://artist.bandcamp.com/album/test",
            )
        )
        self.assertTrue(
            pmda._files_album_profile_fetch_allowed(strict_verified=True)
        )
        self.assertFalse(pmda._files_album_profile_fetch_allowed(strict_verified=False))
        self.assertFalse(pmda._files_album_cover_refresh_allowed(strict_verified=False))
        self.assertTrue(pmda._files_album_cover_refresh_allowed(strict_verified=True))

    def test_fetch_bandcamp_album_info_prefers_direct_album_url_and_extracts_owner_metadata(self):
        album_html = """
        <html>
          <head>
            <meta property="og:title" content="Elevator Ambulance Scum by Angel Of Murder Suicide" />
            <meta property="og:image" content="https://f4.bcbits.com/img/a123_0.jpg" />
            <meta name="description" content="fallback description" />
          </head>
          <body>
            <p id="band-name-location">
              <span class="title">Hospital Productions</span>
              <span class="location secondaryText">New York, New York</span>
            </p>
            <div class="signed-out-artists-bio-text"><p id="bio-text">A Small Sado-Masochistic Family</p></div>
            <img src="https://f4.bcbits.com/img/0013921417_21.jpg" class="band-photo" alt="Hospital Productions image">
            <div id="collectors-data" data-blob="{&quot;shown_reviews&quot;:[{&quot;name&quot;:&quot;fan&quot;,&quot;why&quot;:&quot;great&quot;}]}"></div>
            <div data-tralbum="{&quot;current&quot;:{&quot;title&quot;:&quot;Elevator Ambulance Scum&quot;,&quot;artist&quot;:&quot;Angel Of Murder Suicide&quot;,&quot;about&quot;:&quot;few times in a decade does noise arrive approaching the perfection of violent energy you were told about when you were young.&quot;,&quot;tags&quot;:[&quot;new york&quot;]}}"></div>
          </body>
        </html>
        """

        class _Resp:
            def __init__(self, text="", status_code=200):
                self.text = text
                self.status_code = status_code
                self.headers = {}

        def _fake_get(url, headers=None, timeout=None, allow_redirects=True):
            self.assertIn("hospitalproductions.bandcamp.com/album/elevator-ambulance-scum", url)
            return _Resp(album_html, 200)

        with mock.patch.object(pmda, "USE_BANDCAMP", True), \
            mock.patch.object(pmda, "requests") as requests_mod:
            requests_mod.get.side_effect = _fake_get
            payload = pmda._fetch_bandcamp_album_info(
                "Angel Of Murder Suicide",
                "Elevator Ambulance Scum",
                allow_web_fallback=False,
                album_url_hint="https://hospitalproductions.bandcamp.com/album/elevator-ambulance-scum",
            )

        self.assertIsInstance(payload, dict)
        self.assertEqual(payload.get("title"), "Elevator Ambulance Scum")
        self.assertEqual(payload.get("artist_name"), "Angel Of Murder Suicide")
        self.assertIn("few times in a decade", str(payload.get("description") or ""))
        self.assertEqual(payload.get("tags"), ["new york"])
        self.assertEqual(payload.get("page_owner_name"), "Hospital Productions")
        self.assertEqual(payload.get("page_owner_location"), "New York, New York")
        self.assertEqual(payload.get("page_owner_bio"), "A Small Sado-Masochistic Family")
        self.assertEqual(payload.get("page_owner_image_url"), "https://f4.bcbits.com/img/0013921417_10.jpg")

    def test_album_review_search_context_contains_provider_hints(self):
        ctx = pmda._album_review_search_context(
            artist="Sigur Rós",
            album="Takk...",
            metadata_source="musicbrainz",
            mbid="mbid-123",
            discogs_release_id="discogs-456",
            lastfm_album_mbid="lastfm-789",
            bandcamp_album_url="https://artist.bandcamp.com/album/takk",
            strict_match_verified=True,
        )
        self.assertEqual(ctx["query_kind"], "album_review")
        self.assertEqual(ctx["artist"], "Sigur Rós")
        self.assertEqual(ctx["album"], "Takk...")
        self.assertEqual(ctx["metadata_source"], "musicbrainz")
        self.assertEqual(ctx["musicbrainz_release_group_id"], "mbid-123")
        self.assertEqual(ctx["discogs_release_id"], "discogs-456")
        self.assertEqual(ctx["lastfm_album_mbid"], "lastfm-789")
        self.assertEqual(ctx["bandcamp_album_url"], "https://artist.bandcamp.com/album/takk")
        self.assertTrue(ctx["strict_match_verified"])

    def test_web_search_serper_uses_ollama_before_paid_fallback(self):
        ollama_rows = [
            {
                "title": "Album review",
                "link": "https://example.com/review",
                "snippet": "Useful snippet",
                "source": "ollama_web_search",
            }
        ]
        with mock.patch.object(pmda, "_ai_web_search_cache_lookup", return_value=(False, [])), \
            mock.patch.object(pmda, "_ai_query_cache_get", return_value=(False, [], "")), \
            mock.patch.object(pmda, "_ai_web_search_mark_run_query_seen", return_value=True), \
            mock.patch.object(pmda, "_web_search_provider_order", return_value=[]), \
            mock.patch.object(pmda, "_ollama_web_search_enabled", return_value=True), \
            mock.patch.object(pmda, "_ollama_web_search", return_value=("hit", ollama_rows, {"confidence": 0.91})) as ollama_search, \
            mock.patch.object(pmda, "_web_search_ai_fallback_enabled", return_value=True), \
            mock.patch.object(pmda, "_openai_web_search_fallback", return_value=[]) as openai_fallback, \
            mock.patch.object(pmda, "_ai_web_search_cache_set"), \
            mock.patch.object(pmda, "_ai_query_cache_set"):
            rows = pmda._web_search_serper(
                '"Sigur Rós" "Takk..." album review',
                num=5,
                allow_ai_fallback=True,
                context={"artist": "Sigur Rós", "album": "Takk..."},
                analysis_type="album_review_lookup",
            )

        self.assertEqual(rows, ollama_rows)
        self.assertTrue(ollama_search.called)
        openai_fallback.assert_not_called()

    def test_album_review_batch_uses_request_artist_context(self):
        seen_artists = []

        def fake_plan(artist_name, album_title):
            seen_artists.append(("plan", artist_name, album_title))
            return ("query", [], 5)

        def fake_collect(artist_name, album_title, **kwargs):
            seen_artists.append(("collect", artist_name, album_title))
            return [
                {
                    "title": f"{artist_name} {album_title} review",
                    "link": "https://example.com/review",
                    "snippet": "Useful review snippet",
                    "source": "ollama_web_search",
                }
            ]

        with mock.patch.object(pmda, "_review_lookup_query_plan", side_effect=fake_plan), \
            mock.patch.object(pmda, "_review_lookup_collect_hits", side_effect=fake_collect), \
            mock.patch.object(pmda, "_review_prepare_candidates", return_value=[{
                "position": 1,
                "title": "Slowdive Souvlaki review",
                "link": "https://example.com/review",
                "page_url": "https://example.com/review",
                "page_title": "Slowdive Souvlaki review",
                "page_excerpt": "Useful review snippet",
                "snippet": "Useful review snippet",
            }]), \
            mock.patch.object(pmda, "_review_validate_candidates_with_ai", return_value={
                "selected_index": 1,
                "confidence": 90,
                "selected": {
                    "page_url": "https://example.com/review",
                    "page_excerpt": "Useful review snippet",
                    "snippet": "Useful review snippet",
                },
                "provider_effective": "ollama",
                "auth_mode": "local",
            }), \
            mock.patch.object(pmda, "_is_relevant_album_profile_text", return_value=True):
            out = pmda._fetch_album_review_web_ai_batch(
                "Sigur Rós",
                [
                    {
                        "artist_name": "Slowdive",
                        "album_title": "Souvlaki",
                        "title_norm": pmda.norm_album("Souvlaki"),
                    }
                ],
            )

        self.assertIn(("plan", "Slowdive", "Souvlaki"), seen_artists)
        self.assertIn(("collect", "Slowdive", "Souvlaki"), seen_artists)
        self.assertIn(pmda.norm_album("Souvlaki"), out)
        self.assertEqual(out[pmda.norm_album("Souvlaki")]["source"], "ollama")

    def test_album_review_batch_prefers_local_longform_summary(self):
        with mock.patch.object(
            pmda,
            "_fetch_album_review_web_ai",
            return_value={
                "description": "Local summary",
                "short_description": "Local summary",
                "source": "ollama",
            },
        ) as single_lookup:
            out = pmda._fetch_album_review_web_ai_batch(
                "Sigur Rós",
                [{"album_title": "Takk...", "title_norm": pmda.norm_album("Takk...")}],
            )

        self.assertEqual(out[pmda.norm_album("Takk...")]["description"], "Local summary")
        self.assertEqual(out[pmda.norm_album("Takk...")]["source"], "ollama")
        self.assertTrue(single_lookup.called)

    def test_fetch_album_review_web_ai_accepts_identity_in_page_title_and_snippet(self):
        hits = [
            {
                "title": "Severed Heads - Since The Accident - Reviews - Album of The Year",
                "link": "https://www.albumoftheyear.org/album/30607-severed-heads-since-the-accident.php",
                "snippet": "Severed Heads - Since The Accident - Reviews - Album of The Year",
                "source": "duckduckgo",
            }
        ]
        candidate = {
            "position": 1,
            "title": hits[0]["title"],
            "link": hits[0]["link"],
            "page_url": hits[0]["link"],
            "page_title": "Severed Heads - Since The Accident - Reviews - Album of The Year",
            "page_excerpt": "This record has a few quirks that peak my interest unlike many other industrial records have, but fails to deliver a cohesive listening experience.",
            "snippet": hits[0]["snippet"],
        }
        with mock.patch.object(pmda, "_review_lookup_collect_hits", return_value=hits), \
            mock.patch.object(pmda, "_review_prepare_candidates", return_value=[candidate]), \
            mock.patch.object(
                pmda,
                "_review_validate_candidates_with_ai",
                return_value={
                    "selected_index": 1,
                    "confidence": 95,
                    "selected": candidate,
                    "provider_effective": "ollama",
                    "auth_mode": "local",
                },
            ):
            out = pmda._fetch_album_review_web_ai("Severed Heads", "Since The Accident")

        self.assertEqual(out.get("source"), "ollama")
        self.assertIn("cohesive listening experience", out.get("description", ""))
        self.assertTrue(bool(out.get("short_description")))

    def test_review_lookup_query_plan_prefers_relaxed_primary_for_non_ascii(self):
        primary, expansions, batch_size = pmda._review_lookup_query_plan("Sigur Rós", "Takk…")
        self.assertEqual(primary, "sigur ros takk review")
        self.assertIn("sigur ros takk album review", expansions)
        self.assertIn('"Sigur Rós" "Takk..." review', expansions)
        self.assertIn('"Sigur Rós" "Takk..." album review', expansions)
        self.assertEqual(batch_size, 5)

    def test_review_lookup_query_plan_prefers_relaxed_primary_for_plain_ascii(self):
        primary, expansions, batch_size = pmda._review_lookup_query_plan("Slowdive", "Souvlaki")
        self.assertEqual(primary, "slowdive souvlaki review")
        self.assertIn("slowdive souvlaki album review", expansions)
        self.assertIn('"Slowdive" "Souvlaki" album review', expansions)
        self.assertIn('"Slowdive" "Souvlaki" review', expansions)
        self.assertEqual(batch_size, 5)

    def test_review_lookup_collect_hits_stops_after_primary_has_hits(self):
        with mock.patch.object(
            pmda,
            "_review_lookup_query_plan",
            return_value=("sigur ros takk review", ['"Sigur Rós" "Takk..." album review'], 5),
        ), mock.patch.object(
            pmda,
            "_duckduckgo_html_search_http",
            return_value=(
                "hit",
                [
                    {
                        "title": "Sigur Rós - Takk... review",
                        "link": "https://example.com/takk-review",
                        "snippet": "Review text",
                        "source": "duckduckgo",
                    }
                ],
            ),
        ), mock.patch.object(
            pmda,
            "_web_search_serper",
            return_value=[
                {
                    "title": "Sigur Rós - Takk... review",
                    "link": "https://example.com/takk-review",
                    "snippet": "Review text",
                    "source": "serper",
                }
            ],
        ) as web_search, mock.patch.object(
            pmda,
            "_review_score_hits",
            return_value=[],
        ):
            hits = pmda._review_lookup_collect_hits(
                "Sigur Rós",
                "Takk...",
                query_batch_size=5,
                max_hits=24,
                search_context={"query_kind": "album_review"},
            )

        self.assertEqual(len(hits), 1)
        web_search.assert_not_called()

    def test_review_lookup_collect_hits_falls_back_to_generic_search_when_ddg_misses(self):
        with mock.patch.object(
            pmda,
            "_review_lookup_query_plan",
            return_value=("sigur ros takk review", [], 5),
        ), mock.patch.object(
            pmda,
            "_duckduckgo_html_search_http",
            return_value=("miss", []),
        ), mock.patch.object(
            pmda,
            "_web_search_serper",
            return_value=[
                {
                    "title": "Sigur Rós - Takk... review",
                    "link": "https://example.com/takk-review",
                    "snippet": "Review text",
                    "source": "ollama_web_search",
                }
            ],
        ) as web_search:
            hits = pmda._review_lookup_collect_hits(
                "Sigur Rós",
                "Takk...",
                query_batch_size=5,
                max_hits=24,
                search_context={"query_kind": "album_review"},
            )

        self.assertEqual(len(hits), 1)
        web_search.assert_called_once()

    def test_review_fetch_page_context_extracts_excerpt(self):
        response = mock.Mock(status_code=200, url="https://example.com/review")
        response.headers = {"content-type": "text/html; charset=utf-8"}
        response.text = """
        <html><head>
        <title>Takk review</title>
        <meta name="description" content="A real album review.">
        </head><body>
        <p>Sigur Ros delivers a warm and expansive album that trades in atmosphere, patience, and a sense of scale that keeps widening as each arrangement unfolds.</p>
        <p>This review discusses the album in detail, clearly evaluates the songwriting, and explains why the record succeeds as a coherent full-length statement.</p>
        </body></html>
        """
        with mock.patch.object(pmda.requests, "get", return_value=response):
            page = pmda._review_fetch_page_context("https://example.com/review")

        self.assertEqual(page["page_title"], "Takk review")
        self.assertIn("Sigur Ros delivers a warm and expansive album that trades in atmosphere", page["page_excerpt"])
        self.assertNotIn("A real album review.", page["page_excerpt"])

    def test_review_fetch_page_context_strips_pitchfork_boilerplate(self):
        response = mock.Mock(status_code=200, url="https://pitchfork.com/reviews/albums/7155-takk/")
        response.headers = {"content-type": "text/html; charset=utf-8"}
        response.text = """
        <html><head>
        <title>Sigur Rós: Takk... Album Review | Pitchfork</title>
        <meta name="description" content="When Sigur Rós’ second full-length record, Agetis Byrjun, landed stateside in 2001, its extraterrestrial oozing was so unfamiliar…">
        </head><body>
        <article>
            <p>Newsletter Search Search News Reviews Best New Music Features Lists Columns Video Open Navigation Menu Menu Search Search 0.0</p>
            <p>Save this story Save Story Save this story When Sigur Rós’ second full-length record, Agetis Byrjun, landed stateside in 2001, its extraterrestrial oozing was so unfamiliar to American ears that it attracted endless comparisons to glaciers and fjords.</p>
            <p>Ultimately, Takk... is a warmer, more orchestral take on the band’s defining sound, and easily their most instantly accessible record to date.</p>
            <p>Sigur Rós: Takk... $60 at Rough Trade $60 at Amazon Most Read Reviews ARIRANG</p>
            <p>Amanda Petrusich has been writing for Pitchfork since 2003.</p>
            <p>© 2026 Condé Nast. All rights reserved. Pitchfork may earn a portion of sales from affiliate partnerships.</p>
        </article>
        </body></html>
        """
        with mock.patch.object(pmda.requests, "get", return_value=response):
            page = pmda._review_fetch_page_context("https://pitchfork.com/reviews/albums/7155-takk/")

        excerpt = page["page_excerpt"]
        self.assertIn("When Sigur Rós’ second full-length record", excerpt)
        self.assertIn("Ultimately, Takk...", excerpt)
        self.assertNotIn("Newsletter Search Search", excerpt)
        self.assertNotIn("Save this story", excerpt)
        self.assertNotIn("Most Read Reviews", excerpt)
        self.assertNotIn("Amanda Petrusich has been writing for Pitchfork", excerpt)
        self.assertNotIn("Condé Nast", excerpt)

    def test_duckduckgo_html_search_http_parses_results(self):
        html_body = """
        <div class="result results_links results_links_deep web-result ">
          <div class="links_main links_deep result__body">
            <h2 class="result__title">
              <a rel="nofollow" class="result__a" href="https://pitchfork.com/reviews/albums/7155-takk/">Sigur Rós: Takk... Album Review | Pitchfork</a>
            </h2>
            <a class="result__snippet">Takk is a warmer, more orchestral take on the band's sound.</a>
          </div>
        </div>
        """
        response = mock.Mock(status_code=200, text=html_body)
        with mock.patch.object(pmda.requests, "post", return_value=response):
            status, rows = pmda._duckduckgo_html_search_http("sigur ros takk review", num=3)

        self.assertEqual(status, "hit")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "duckduckgo")
        self.assertIn("Pitchfork", rows[0]["title"])

    def test_review_validate_candidates_with_ai_selects_first_valid_review(self):
        candidates = [
            {
                "position": 1,
                "title": "Takk... Album Review",
                "link": "https://example.com/takk",
                "page_url": "https://example.com/takk",
                "page_title": "Takk... Album Review",
                "page_excerpt": "Sigur Ros album review text.",
                "snippet": "Review snippet",
            }
        ]
        with mock.patch.object(
            pmda,
            "_resolve_ai_runtime_availability",
            return_value=(True, "ollama", "local", ""),
        ), mock.patch.object(
            pmda,
            "_ollama_model_configured",
            return_value="qwen3:4b",
        ), mock.patch.object(
            pmda,
            "_ollama_chat_json",
            return_value={"message": {"content": '{"accepted":true,"confidence":93,"reason":"correct album review"}'}},
        ):
            out = pmda._review_validate_candidates_with_ai("Sigur Rós", "Takk...", candidates)

        self.assertEqual(out["selected_index"], 1)
        self.assertEqual(out["confidence"], 93)
        self.assertEqual(out["reason"], "correct album review")

    def test_review_validate_candidates_with_ai_accepts_obvious_review_without_ai(self):
        candidates = [
            {
                "position": 1,
                "title": "Sigur Rós: Takk... Album Review | Pitchfork",
                "link": "https://pitchfork.com/reviews/albums/7155-takk/",
                "page_url": "https://pitchfork.com/reviews/albums/7155-takk/",
                "page_title": "Sigur Rós: Takk... Album Review | Pitchfork",
                "page_excerpt": "A real review page discussing the album in detail.",
                "snippet": "A detailed review of Takk... by Sigur Rós.",
            }
        ]
        with mock.patch.object(
            pmda,
            "_resolve_ai_runtime_availability",
            return_value=(True, "ollama", "local", ""),
        ), mock.patch.object(
            pmda,
            "_ollama_model_configured",
            return_value="qwen3:4b",
        ), mock.patch.object(
            pmda,
            "_ollama_chat_json",
        ) as ollama_chat:
            out = pmda._review_validate_candidates_with_ai("Sigur Rós", "Takk...", candidates)

        self.assertEqual(out["selected_index"], 1)
        self.assertEqual(out["confidence"], 95)
        self.assertEqual(out["reason"], "strong_title_match")
        ollama_chat.assert_not_called()

    def test_review_validate_candidates_with_ai_checks_candidates_in_order(self):
        candidates = [
            {
                "position": 1,
                "title": "Takk... tracklist and credits",
                "link": "https://example.com/metadata",
                "page_url": "https://example.com/metadata",
                "page_title": "Metadata only",
                "page_excerpt": "Track list, label and release information.",
                "snippet": "Metadata only",
            },
            {
                "position": 2,
                "title": "Takk... Album Review",
                "link": "https://example.com/review",
                "page_url": "https://example.com/review",
                "page_title": "Takk... Album Review",
                "page_excerpt": "A long real review of the Sigur Ros album.",
                "snippet": "Review snippet",
            },
        ]
        with mock.patch.object(
            pmda,
            "_resolve_ai_runtime_availability",
            return_value=(True, "ollama", "local", ""),
        ), mock.patch.object(
            pmda,
            "_ollama_model_configured",
            return_value="qwen3:4b",
        ), mock.patch.object(
            pmda,
            "_ollama_chat_json",
            side_effect=[
                {"message": {"content": '{"accepted":false,"confidence":92,"reason":"metadata_only"}'}},
                {"message": {"content": '{"accepted":true,"confidence":95,"reason":"real_album_review"}'}},
            ],
        ) as validate_call:
            out = pmda._review_validate_candidates_with_ai("Sigur Rós", "Takk...", candidates)

        self.assertEqual(out["selected_index"], 2)
        self.assertEqual(out["confidence"], 95)
        self.assertEqual(validate_call.call_count, 2)

    def test_fetch_album_review_web_ai_uses_ai_validated_candidate(self):
        with mock.patch.object(
            pmda,
            "_review_lookup_collect_hits",
            return_value=[
                {
                    "title": "Takk... Album Review",
                    "link": "https://example.com/takk",
                    "snippet": "Review snippet",
                    "source": "serper",
                }
            ],
        ), mock.patch.object(
            pmda,
            "_review_prepare_candidates",
            return_value=[
                {
                    "position": 1,
                    "title": "Takk... Album Review",
                    "link": "https://example.com/takk",
                    "page_url": "https://example.com/takk",
                    "page_title": "Takk... Album Review",
                    "page_excerpt": "Long review text",
                    "snippet": "Review snippet",
                }
            ],
        ), mock.patch.object(
            pmda,
            "_review_validate_candidates_with_ai",
            return_value={
                "selected_index": 1,
                "confidence": 96,
                "selected": {
                    "page_url": "https://example.com/takk",
                    "page_excerpt": "Long review text",
                    "snippet": "Review snippet",
                },
                "provider_effective": "ollama",
                "auth_mode": "local",
            },
        ), mock.patch.object(
            pmda,
            "_is_relevant_album_profile_text",
            return_value=True,
        ):
            out = pmda._fetch_album_review_web_ai("Sigur Rós", "Takk...")

        self.assertEqual(out["description"], "Long review text")
        self.assertEqual(out["source"], "ollama")
        self.assertEqual(out["source_url"], "https://example.com/takk")

    def test_fetch_album_review_web_ai_retries_broader_search_when_initial_candidates_are_blocked(self):
        blocked_candidate = {
            "position": 1,
            "title": "Ear Bitten review",
            "link": "https://blocked.example/review",
            "page_url": "https://blocked.example/review",
            "page_title": "Ear Bitten review",
            "page_excerpt": "",
            "snippet": "Review snippet",
            "fetch_error": "http_403",
        }
        good_candidate = {
            "position": 2,
            "title": "Severed Heads - Ear Bitten review",
            "link": "https://good.example/review",
            "page_url": "https://good.example/review",
            "page_title": "Severed Heads - Ear Bitten review",
            "page_excerpt": "A proper long-form review of Ear Bitten by Severed Heads.",
            "snippet": "A proper long-form review.",
            "fetch_error": "",
        }
        with mock.patch.object(
            pmda,
            "_review_lookup_collect_hits",
            side_effect=[
                [{"title": "blocked", "link": "https://blocked.example/review", "snippet": "Review", "source": "duckduckgo"}],
                [
                    {"title": "blocked", "link": "https://blocked.example/review", "snippet": "Review", "source": "duckduckgo"},
                    {"title": "good", "link": "https://good.example/review", "snippet": "Review", "source": "duckduckgo"},
                ],
            ],
        ) as collect_hits, mock.patch.object(
            pmda,
            "_review_prepare_candidates",
            side_effect=[[blocked_candidate], [good_candidate]],
        ), mock.patch.object(
            pmda,
            "_review_validate_candidates_with_ai",
            side_effect=[{}, {"selected_index": 2, "confidence": 91, "selected": good_candidate, "provider_effective": "ollama", "auth_mode": "local"}],
        ), mock.patch.object(
            pmda,
            "_is_relevant_album_profile_text",
            return_value=True,
        ):
            out = pmda._fetch_album_review_web_ai("Severed Heads", "Ear Bitten")

        self.assertEqual(out["description"], good_candidate["page_excerpt"])
        self.assertEqual(collect_hits.call_count, 2)
        self.assertTrue(bool(collect_hits.call_args_list[1].kwargs.get("continue_after_hit")))

    def test_review_summary_fallback_rejects_metadata_only_hits(self):
        hits = [
            {
                "title": "Sigur Rós - Takk... (Release Group) - MusicBrainz",
                "link": "https://musicbrainz.org/release-group/47d8e9e2-dc13-3d94-afee-1f8c38cec091",
                "snippet": "MusicBrainz release information.",
                "source": "ollama_web_search",
            },
            {
                "title": "Takk... - Sigur Rós | AllMusic",
                "link": "https://www.allmusic.com/album/takk-/gb-1463875029",
                "snippet": "AllMusic album page and credits.",
                "source": "ollama_web_search",
            },
        ]
        self.assertEqual(pmda._review_summary_fallback_from_hits("Sigur Rós", "Takk...", hits), "")
        self.assertEqual(pmda._review_score_hits("Sigur Rós", "Takk...", hits), [])

    def test_ollama_web_search_stays_on_base_model(self):
        payload = {
            "message": {
                "content": json.dumps(
                    {
                        "query": '"Severed Heads" "Since the Accident" album review',
                        "verdict": "strong",
                        "confidence": 0.91,
                        "decision_reason": "trusted music pages found",
                        "sources": [
                            {
                                "title": "Since the Accident review",
                                "link": "https://example.com/review",
                                "snippet": "Useful review text",
                                "accepted": True,
                                "reason": "reliable source",
                            }
                        ],
                    }
                )
            }
        }
        seed_rows = [
            {
                "title": "Since the Accident review",
                "link": "https://example.com/review",
                "snippet": "Useful review text",
                "source": "duckduckgo",
            }
        ]
        with mock.patch.object(pmda, "_ollama_web_search_enabled", return_value=True), \
            mock.patch.object(pmda, "_ollama_model_configured", return_value="qwen3:4b"), \
            mock.patch.object(pmda, "_ollama_complex_model_configured", return_value="qwen3:14b"), \
            mock.patch.object(pmda, "_ollama_model_available", return_value=True), \
            mock.patch.object(pmda, "_ollama_web_search_seed_hits", return_value=("duckduckgo", "hit", seed_rows)), \
            mock.patch.object(pmda, "_ollama_web_search_enrich_seed_rows", return_value=seed_rows), \
            mock.patch.object(pmda, "_ollama_chat_json", return_value=payload) as chat_json:
            status, rows, meta = pmda._ollama_web_search(
                '"Severed Heads" "Since the Accident" album review',
                num=3,
                reason="album_review_lookup",
                context={"artist": "Severed Heads", "album": "Since the Accident", "query_kind": "identity_lookup"},
                analysis_type="album_review_lookup",
            )

        self.assertEqual(status, "hit")
        self.assertEqual(len(rows), 1)
        self.assertEqual(meta.get("model"), "qwen3:4b")
        self.assertEqual(meta.get("seed_source"), "duckduckgo")
        self.assertEqual(chat_json.call_count, 1)
        self.assertEqual(chat_json.call_args.kwargs["model_name"], "qwen3:4b")

    def test_ollama_web_search_escalates_to_complex_model_after_timeout(self):
        payload = {
            "message": {
                "content": json.dumps(
                    {
                        "query": '"Sigur Rós" "Takk..." album review',
                        "verdict": "strong",
                        "confidence": 0.91,
                        "decision_reason": "review sources found",
                        "sources": [
                            {
                                "title": "Takk... Album Review - Sigur Rós - Pitchfork",
                                "link": "https://pitchfork.com/reviews/albums/7155-takk/",
                                "snippet": "Takk is a warmer, more orchestral take on the band's sound.",
                                "accepted": True,
                                "reason": "trusted review source",
                            }
                        ],
                    }
                )
            }
        }
        seed_rows = [
            {
                "title": "Takk... Album Review - Sigur Rós - Pitchfork",
                "link": "https://pitchfork.com/reviews/albums/7155-takk/",
                "snippet": "Takk is a warmer, more orchestral take on the band's sound.",
                "source": "duckduckgo",
            }
        ]
        with mock.patch.object(pmda, "_ollama_web_search_enabled", return_value=True), \
            mock.patch.object(pmda, "_ollama_model_configured", return_value="qwen3:4b"), \
            mock.patch.object(pmda, "_ollama_complex_model_configured", return_value="qwen3:14b"), \
            mock.patch.object(pmda, "_ollama_model_available", return_value=True), \
            mock.patch.object(pmda, "_ollama_web_search_seed_hits", return_value=("duckduckgo", "hit", seed_rows)), \
            mock.patch.object(pmda, "_ollama_web_search_enrich_seed_rows", return_value=seed_rows), \
            mock.patch.object(pmda, "_ollama_chat_json", side_effect=[TimeoutError("Ollama web search timed out after 30s"), payload]) as chat_json:
            status, rows, meta = pmda._ollama_web_search(
                '"Sigur Rós" "Takk..." album review',
                num=4,
                reason="album_review_lookup",
                context={"artist": "Sigur Rós", "album": "Takk...", "query_kind": "identity_lookup"},
                analysis_type="album_review_lookup",
            )

        self.assertEqual(status, "hit")
        self.assertEqual(meta.get("model"), "qwen3:14b")
        self.assertEqual(len(rows), 1)
        self.assertEqual(chat_json.call_count, 2)
        self.assertEqual(chat_json.call_args_list[0].kwargs["model_name"], "qwen3:4b")
        self.assertEqual(chat_json.call_args_list[1].kwargs["model_name"], "qwen3:14b")

    def test_ollama_web_search_returns_review_seed_rows_without_model_selection(self):
        seed_rows = [
            {
                "title": "Sigur Rós: Takk... Album Review | Pitchfork",
                "link": "https://pitchfork.com/reviews/albums/7155-takk/",
                "snippet": "Takk is a warmer, more orchestral take on the band's sound.",
                "source": "duckduckgo",
            }
        ]
        with mock.patch.object(pmda, "_ollama_web_search_enabled", return_value=True), \
            mock.patch.object(pmda, "_ollama_model_configured", return_value="qwen3:4b"), \
            mock.patch.object(pmda, "_ollama_complex_model_configured", return_value="qwen3:14b"), \
            mock.patch.object(pmda, "_ollama_model_available", return_value=True), \
            mock.patch.object(pmda, "_ollama_web_search_seed_hits", return_value=("duckduckgo", "hit", seed_rows)), \
            mock.patch.object(pmda, "_ollama_web_search_enrich_seed_rows", return_value=seed_rows), \
            mock.patch.object(pmda, "_ollama_chat_json") as chat_json:
            status, rows, meta = pmda._ollama_web_search(
                "sigur ros takk review",
                num=3,
                reason="album_review_lookup",
                context={"artist": "Sigur Rós", "album": "Takk...", "query_kind": "album_review"},
                analysis_type="album_review_lookup",
            )

        self.assertEqual(status, "hit")
        self.assertEqual(rows, seed_rows)
        self.assertEqual(meta.get("fallback"), "direct_seed_rows")
        chat_json.assert_not_called()

    def test_ollama_web_search_accepts_album_reviews_fallback_shape(self):
        payload = {
            "album_reviews": [
                {
                    "title": "Since the Accident",
                    "review_text": "Experimental electronic album with sharp rhythmic shifts.",
                }
            ]
        }
        rows, rejected, confidence, decision_reason = pmda._ollama_web_search_parse_rows(payload, max_items=3)
        self.assertEqual(rejected, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Since the Accident")

    def test_fetch_best_album_profile_prefers_web_review_before_lastfm(self):
        with mock.patch.object(
            pmda,
            "_fetch_lastfm_album_info",
            return_value={
                "wiki_content": "Last.fm fallback text",
                "wiki_summary": "Last.fm fallback text",
                "toptags": ["post-rock"],
            },
        ), mock.patch.object(
            pmda,
            "_fetch_album_provider_fallbacks_parallel",
            return_value={},
        ), mock.patch.object(
            pmda,
            "_fetch_album_review_web_ai",
            return_value={
                "description": "Pitchfork-style review summary",
                "short_description": "Pitchfork-style review summary",
                "source": "ollama",
            },
        ), mock.patch.object(
            pmda,
            "_is_relevant_album_profile_text",
            return_value=True,
        ):
            profile = pmda._fetch_best_album_profile(
                "Sigur Rós",
                "Takk...",
                allow_web_ai=True,
            )

        self.assertEqual(profile.get("source"), "ollama")
        self.assertEqual(profile.get("description"), "Pitchfork-style review summary")

    def test_fetch_best_album_profile_web_review_keeps_provider_tags_and_bandcamp_metrics(self):
        with mock.patch.object(
            pmda,
            "_fetch_lastfm_album_info",
            return_value={
                "wiki_content": "Last.fm fallback text",
                "wiki_summary": "Last.fm fallback text",
                "toptags": ["post-rock"],
            },
        ), mock.patch.object(
            pmda,
            "_fetch_album_provider_fallbacks_parallel",
            return_value={
                "lastfm": {"toptags": ["ambient"]},
                "bandcamp": {
                    "tags": ["shoegaze"],
                    "bandcamp_supporter_count": 17,
                    "bandcamp_supporter_comments": [{"author": "A", "text": "Beautiful release"}],
                },
            },
        ), mock.patch.object(
            pmda,
            "_fetch_album_review_web_ai",
            return_value={
                "description": "Pitchfork-style review summary",
                "short_description": "Pitchfork-style review summary",
                "source": "ollama",
                "tags": ["dream-pop"],
            },
        ), mock.patch.object(
            pmda,
            "_is_relevant_album_profile_text",
            return_value=True,
        ):
            profile = pmda._fetch_best_album_profile(
                "Sigur Rós",
                "Takk...",
                allow_web_ai=True,
            )

        self.assertEqual(profile.get("source"), "ollama")
        self.assertEqual(profile.get("description"), "Pitchfork-style review summary")
        self.assertEqual(
            profile.get("tags"),
            ["post-rock", "ambient", "shoegaze", "dream-pop"],
        )
        self.assertEqual(profile.get("bandcamp_supporter_count"), 17)
        self.assertEqual(
            profile.get("bandcamp_supporter_comments"),
            [{"author": "A", "text": "Beautiful release"}],
        )

    def test_fetch_best_album_profile_uses_lastfm_provider_review_without_web_review(self):
        with mock.patch.object(
            pmda,
            "_fetch_lastfm_album_info",
            return_value={
                "wiki_content": "Last.fm fallback text that should be used as the provider review.",
                "wiki_summary": "Last.fm fallback summary.",
                "toptags": ["post-rock", "icelandic"],
                "public_rating": 4.7,
                "public_rating_votes": 358,
                "public_rating_source": "lastfm",
            },
        ), mock.patch.object(
            pmda,
            "_fetch_album_provider_fallbacks_parallel",
            return_value={},
        ), mock.patch.object(
            pmda,
            "_is_relevant_album_profile_text",
            return_value=True,
        ), mock.patch.object(
            pmda,
            "_fetch_album_review_web_ai",
            return_value={},
        ):
            profile = pmda._fetch_best_album_profile(
                "Sigur Rós",
                "Takk...",
                allow_web_ai=False,
            )

        self.assertEqual(str(profile.get("description") or "").strip(), "Last.fm fallback text that should be used as the provider review.")
        self.assertEqual(str(profile.get("short_description") or "").strip(), "Last.fm fallback summary.")
        self.assertEqual(profile.get("source"), "lastfm")
        self.assertEqual(profile.get("tags"), ["post-rock", "icelandic"])
        self.assertEqual(profile.get("public_rating"), 4.7)

    def test_fetch_best_album_profile_uses_bandcamp_when_lastfm_has_no_prose(self):
        with mock.patch.object(
            pmda,
            "_fetch_lastfm_album_info",
            return_value={
                "wiki_content": "",
                "wiki_summary": "",
                "toptags": ["modern classical"],
                "lastfm_listeners": 2,
            },
        ), mock.patch.object(
            pmda,
            "_fetch_album_provider_fallbacks_parallel",
            return_value={
                "lastfm": {"lastfm_listeners": 2},
                "bandcamp": {
                    "description": "Bandcamp liner note with concrete context about the release, the performers, and the edition.",
                    "tags": ["feldman", "experimental"],
                    "bandcamp_supporter_count": 17,
                    "bandcamp_supporter_comments": [{"author": "A", "text": "Beautiful release"}],
                },
            },
        ), mock.patch.object(
            pmda,
            "_is_relevant_album_profile_text",
            return_value=True,
        ), mock.patch.object(
            pmda,
            "_fetch_album_review_web_ai",
            return_value={},
        ):
            profile = pmda._fetch_best_album_profile(
                "John Tilbury piano",
                "For Bunita Marcus Composed by Morton Feldman",
                allow_web_ai=False,
            )

        self.assertEqual(profile.get("source"), "bandcamp")
        self.assertIn("Bandcamp liner note", str(profile.get("description") or ""))
        self.assertEqual(profile.get("bandcamp_supporter_count"), 17)
        self.assertEqual(profile.get("lastfm_listeners"), 2)
        self.assertIn("modern classical", profile.get("tags"))
        self.assertIn("feldman", profile.get("tags"))

    def test_fetch_best_album_profile_prefers_richer_bandcamp_prose_over_sparse_lastfm(self):
        with mock.patch.object(
            pmda,
            "_fetch_lastfm_album_info",
            return_value={
                "wiki_content": "Short Last.fm stub.",
                "wiki_summary": "Short Last.fm stub.",
                "toptags": ["ambient"],
            },
        ), mock.patch.object(
            pmda,
            "_fetch_album_provider_fallbacks_parallel",
            return_value={
                "bandcamp": {
                    "description": (
                        "Bandcamp description with multiple concrete paragraphs about the composition, "
                        "recording session, edition details, and release context. "
                        "It is substantially richer than the short Last.fm stub."
                    ),
                    "tags": ["composer", "piano"],
                },
            },
        ), mock.patch.object(
            pmda,
            "_is_relevant_album_profile_text",
            return_value=True,
        ), mock.patch.object(
            pmda,
            "_fetch_album_review_web_ai",
            return_value={},
        ):
            profile = pmda._fetch_best_album_profile(
                "John Tilbury piano",
                "For Bunita Marcus Composed by Morton Feldman",
                allow_web_ai=False,
            )

        self.assertEqual(profile.get("source"), "bandcamp")
        self.assertIn("substantially richer", str(profile.get("description") or ""))
        self.assertIn("ambient", profile.get("tags"))
        self.assertIn("composer", profile.get("tags"))

    def test_normalize_bandcamp_supporter_comments_dedupes_and_keeps_text(self):
        comments = pmda._normalize_bandcamp_supporter_comments(
            [
                {"author": "A", "text": "Great record"},
                {"author": "A", "text": "Great record"},
                {"author": "B", "text": "Still works", "url": "https://bandcamp.com/b"},
            ]
        )

        self.assertEqual(len(comments), 2)
        self.assertEqual(comments[0]["author"], "A")
        self.assertEqual(comments[0]["text"], "Great record")
        self.assertEqual(comments[1]["url"], "https://bandcamp.com/b")

    def test_strict_validate_edition_match_degrades_provider_network_failure(self):
        edition = {
            "tracks": [],
            "tags": {},
            "ordered_paths": [],
            "discogs_release_id": "123",
            "primary_metadata_source": "discogs",
        }
        with mock.patch.object(
            pmda,
            "_strict_payload_for_provider",
            side_effect=requests.exceptions.ConnectionError(
                "HTTPSConnectionPool(host='api.discogs.com', port=443): Failed to resolve 'api.discogs.com' ([Errno -3] Temporary failure in name resolution)"
            ),
        ):
            verdict = pmda._strict_validate_edition_match(
                artist_name="Sigur Rós",
                album_title="Takk...",
                edition=edition,
            )

        self.assertFalse(verdict["strict_match_verified"])
        self.assertIn("provider_unreachable", verdict["strict_attempts"][0])

    def test_strict_validate_edition_match_skips_cold_fetch_for_non_primary_provider(self):
        edition = {
            "tracks": [],
            "tags": {},
            "ordered_paths": [],
            "primary_metadata_source": "bandcamp",
            "_strict_provider_payload": {
                "artist_name": "A.J. Kaufmann",
                "title": "Pink Elephant Music Vol.8",
                "tracklist": [],
            },
        }

        def _fake_match(*, provider, **kwargs):
            if provider == "bandcamp":
                return {
                    "strict_match_verified": True,
                    "strict_match_provider": "bandcamp",
                    "strict_reject_reason": "",
                    "strict_tracklist_score": 1.0,
                }
            return {
                "strict_match_verified": False,
                "strict_match_provider": "",
                "strict_reject_reason": "provider_skipped",
                "strict_tracklist_score": 0.0,
            }

        with mock.patch.object(
            pmda,
            "_strict_payload_for_provider",
            side_effect=AssertionError("non-primary providers should not cold-fetch during strict validation"),
        ), mock.patch.object(
            pmda,
            "_strict_provider_match_100",
            side_effect=_fake_match,
        ):
            verdict = pmda._strict_validate_edition_match(
                artist_name="A.J. Kaufmann",
                album_title="Pink Elephant Music Vol.8",
                edition=edition,
            )

        self.assertTrue(verdict["strict_match_verified"])
        self.assertEqual(verdict["strict_match_provider"], "bandcamp")

    def test_fetch_lastfm_album_info_skips_stdlib_fallback_on_dns_failure(self):
        exc = requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(host='ws.audioscrobbler.com', port=443): Failed to resolve 'ws.audioscrobbler.com' ([Errno -3] Temporary failure in name resolution)"
        )
        with mock.patch.object(pmda, "USE_LASTFM", True), \
            mock.patch.object(pmda, "LASTFM_API_KEY", "key"), \
            mock.patch.object(pmda.requests, "get", side_effect=exc), \
            mock.patch.object(urllib.request, "urlopen", side_effect=AssertionError("urlopen should not be used on DNS failure")):
            payload = pmda._fetch_lastfm_album_info("Sigur Rós", "Takk...")

        self.assertIsNone(payload)

    def test_provider_fallback_parallel_caches_timed_out_provider_errors(self):
        fake_future = object()

        class FakePool:
            def submit(self, *args, **kwargs):
                return fake_future

            def shutdown(self, *args, **kwargs):
                return None

        with mock.patch.object(pmda, "USE_DISCOGS", False), \
            mock.patch.object(pmda, "USE_BANDCAMP", False), \
            mock.patch.object(pmda, "USE_LASTFM", True), \
            mock.patch("concurrent.futures.ThreadPoolExecutor", return_value=FakePool()), \
            mock.patch.object(pmda, "wait", return_value=(set(), {fake_future})), \
            mock.patch.object(pmda, "set_cached_provider_album_lookup") as cache_set:
            out = pmda._fetch_album_provider_fallbacks_parallel("Sigur Rós", "Takk...")

        self.assertEqual(out["lastfm"], None)
        cache_set.assert_called_once_with("lastfm", "Sigur Rós", "Takk...", "error", None)

    def test_run_serper_preflight_reports_not_enough_credits(self):
        resp = mock.Mock(status_code=400, text='{"message":"Not enough credits"}')
        resp.json.return_value = {"message": "Not enough credits"}
        with mock.patch.object(pmda, "SERPER_API_KEY", "secret"), \
            mock.patch.object(pmda.requests, "post", return_value=resp):
            ok, message = pmda._run_serper_preflight()

        self.assertFalse(ok)
        self.assertEqual(message, "Not enough credits")

    def test_web_search_provider_order_excludes_serper_when_runtime_unavailable(self):
        with mock.patch.object(pmda, "WEB_SEARCH_PROVIDER", "auto"), \
            mock.patch.object(pmda, "SERPER_API_KEY", "secret"), \
            mock.patch.object(pmda, "_scan_ai_policy_for_runtime", return_value="local_then_paid"), \
            mock.patch.object(pmda, "_web_search_local_chain", return_value=["serper"]), \
            mock.patch.object(pmda, "_serper_runtime_status", return_value=(False, "Not enough credits")):
            self.assertEqual(pmda._web_search_provider_order(), [])

    def test_cover_art_archive_front_urls_for_identity_prefers_release_first(self):
        release_id = "66d0717c-7908-44b4-8dc5-d67b0f14bccf"
        release_group_id = "7b3844d1-5125-3158-8aed-90e0b6f61def"

        self.assertEqual(
            pmda._cover_art_archive_front_urls_for_identity(
                release_id=release_id,
                release_group_id=release_group_id,
            ),
            [
                "https://coverartarchive.org/release/66d0717c-7908-44b4-8dc5-d67b0f14bccf/front",
                "https://coverartarchive.org/release-group/7b3844d1-5125-3158-8aed-90e0b6f61def/front",
            ],
        )

    def test_download_cover_art_archive_front_prefers_exact_release_cover(self):
        release_id = "66d0717c-7908-44b4-8dc5-d67b0f14bccf"
        release_group_id = "7b3844d1-5125-3158-8aed-90e0b6f61def"
        calls = []

        def _fake_get(url, timeout=0, allow_redirects=True):
            calls.append(url)
            resp = mock.Mock()
            if f"/release/{release_id}/front" in url:
                resp.status_code = 200
                resp.content = b"pngdata"
                resp.headers = {"content-type": "image/png"}
            else:
                resp.status_code = 404
                resp.content = b""
                resp.headers = {}
            return resp

        with mock.patch.object(pmda.requests, "get", side_effect=_fake_get):
            result = pmda._download_cover_art_archive_front(
                release_id=release_id,
                release_group_id=release_group_id,
                timeout_sec=2.0,
            )

        self.assertEqual(
            calls,
            [f"https://coverartarchive.org/release/{release_id}/front"],
        )
        self.assertEqual(
            result,
            (
                b"pngdata",
                "image/png",
                f"https://coverartarchive.org/release/{release_id}/front",
            ),
        )


if __name__ == "__main__":
    unittest.main()
