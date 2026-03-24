import tempfile
import unittest
from pathlib import Path

import pmda


class FilesPublicationRegressionTests(unittest.TestCase):
    def test_authoritative_primary_tags_require_strict_match_for_pmda_match_tag(self):
        tags = pmda._authoritative_primary_tags_for_publication(
            tags={"artist": "Wrong", pmda.PMDA_MATCH_PROVIDER_TAG: "lastfm", "pmda_matched": "true"},
            artist_resolved="Sigur Rós",
            album_resolved="Takk...",
            year=2005,
            genre="post-rock",
            label="EMI",
            metadata_source="discogs",
            musicbrainz_release_group_id="",
            musicbrainz_release_id="",
            discogs_release_id="42",
            lastfm_album_mbid="",
            bandcamp_album_url="",
            strict_match_verified=False,
            cover_provider="",
        )
        self.assertEqual(tags.get("primary_metadata_source"), "discogs")
        self.assertNotIn(pmda.PMDA_MATCH_PROVIDER_TAG, tags)
        self.assertNotIn("pmda_matched", tags)

    def test_authoritative_publication_cover_keeps_local_cover_for_soft_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            local_cover = folder / "cover.jpg"
            local_cover.write_bytes(b"fake-cover")
            cover_path, has_cover, provider = pmda._authoritative_publication_cover(
                folder=folder,
                item={},
                result={},
                tags={},
                artist_resolved="Slowdive",
                album_resolved="Souvlaki",
                strict_match_verified=False,
                strict_match_provider="",
                metadata_source="discogs",
                musicbrainz_release_group_id="mb-rg-1",
                musicbrainz_release_id="",
                discogs_release_id="42",
                lastfm_album_mbid="",
                bandcamp_album_url="",
                current_cover_path="",
                current_cover_provider="",
            )
        self.assertTrue(has_cover)
        self.assertEqual(provider, "local")
        self.assertEqual(Path(cover_path).name, "cover.jpg")

    def test_verified_provider_payload_overrides_display_identity(self):
        artist, album = pmda._resolve_edition_display_identity(
            {
                "artist": "Sigur Rós",
                "title_raw": "Takk",
                "strict_match_verified": True,
                "strict_match_provider": "discogs",
                "discogs_release_id": "42",
                "_verified_artist_name": "Sigur Rós",
                "_verified_album_title": "Takk...",
            },
            default_artist="Sigur Rós",
            default_title="Takk",
        )
        self.assertEqual((artist, album), ("Sigur Rós", "Takk..."))

    def test_collapse_files_publication_candidates_keeps_single_winner(self):
        candidates = [
            {
                "item": {
                    "folder": "/music/sigur_ros/takk_mp3",
                    "pre_missing_required_tags": [],
                    "pre_has_cover": True,
                    "pre_has_artist_image": True,
                    "bd": 16,
                    "sr": 44100,
                    "br": 320000,
                },
                "row": {
                    "folder_path": "/music/sigur_ros/takk_mp3",
                    "artist_name": "Sigur Rós",
                    "album_title": "Takk...",
                    "strict_match_verified": True,
                    "musicbrainz_release_group_id": "mb-rg-1",
                    "musicbrainz_release_id": "",
                    "discogs_release_id": "",
                    "lastfm_album_mbid": "",
                    "bandcamp_album_url": "",
                    "has_cover": True,
                    "format": "MP3",
                    "strict_tracklist_score": 1.0,
                    "track_count": 11,
                    "total_duration_sec": 3900,
                },
            },
            {
                "item": {
                    "folder": "/music/sigur_ros/takk_flac",
                    "pre_missing_required_tags": [],
                    "pre_has_cover": True,
                    "pre_has_artist_image": True,
                    "bd": 24,
                    "sr": 96000,
                    "br": 0,
                    "strict_match_verified": True,
                },
                "row": {
                    "folder_path": "/music/sigur_ros/takk_flac",
                    "artist_name": "Sigur Rós",
                    "album_title": "Takk...",
                    "strict_match_verified": True,
                    "musicbrainz_release_group_id": "mb-rg-1",
                    "musicbrainz_release_id": "",
                    "discogs_release_id": "",
                    "lastfm_album_mbid": "",
                    "bandcamp_album_url": "",
                    "has_cover": True,
                    "format": "FLAC",
                    "strict_tracklist_score": 1.0,
                    "track_count": 11,
                    "total_duration_sec": 3900,
                },
            },
        ]
        rows, hidden = pmda._collapse_files_publication_candidates("Sigur Rós", candidates)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["folder_path"], "/music/sigur_ros/takk_flac")
        self.assertEqual(hidden, {"/music/sigur_ros/takk_mp3"})

    def test_strict_identity_key_collapses_cross_provider_same_album(self):
        discogs_key = pmda._strict_album_identity_key(
            artist_name="Sigur Rós",
            album_title="Takk...",
            strict_match_verified=True,
            discogs_release_id="42",
        )
        lastfm_key = pmda._strict_album_identity_key(
            artist_name="Sigur Rós",
            album_title="Takk...",
            strict_match_verified=True,
            lastfm_album_mbid="mbid-1",
        )
        self.assertEqual(discogs_key, lastfm_key)
        self.assertTrue(discogs_key.startswith("strict-title:"))


if __name__ == "__main__":
    unittest.main()
