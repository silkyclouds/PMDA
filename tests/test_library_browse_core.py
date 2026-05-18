from __future__ import annotations

from pmda_core.library_browse import browse_cache_keys, live_cache_generation


def test_live_cache_generation_is_idle_when_scan_is_not_active():
    assert live_cache_generation({"scan_published_albums_count": 9}) == "idle"


def test_live_cache_generation_tracks_active_scan_counters():
    assert (
        live_cache_generation(
            {
                "scanning": True,
                "scan_published_albums_count": 3,
                "scan_processed_albums_count": 7,
                "scan_artists_processed": 2,
            }
        )
        == "3:7:2"
    )


def test_artist_browse_cache_keys_match_existing_contract():
    keys = browse_cache_keys(
        kind="artists",
        search_query="AFX",
        genre="Electro",
        label="Warp",
        year=1995,
        sort="recent",
        limit=100,
        offset=200,
        scope_suffix="library",
        unmatched_suffix="include",
        live_generation="1:2:3",
        browse_source="published",
    )

    assert (
        keys.cache_key
        == "library:artists:afx:electro:warp:1995:recent:100:200:library:include:1:2:3:published"
    )
    assert (
        keys.stable_cache_key
        == "library:artists:afx:electro:warp:1995:recent:100:200:library:include:published"
    )


def test_album_browse_cache_keys_include_user_prefix():
    keys = browse_cache_keys(
        kind="albums",
        user_id=42,
        search_query="AFX",
        genre="Electro",
        label="Warp",
        year=1995,
        sort="recent",
        limit=96,
        offset=0,
        scope_suffix="library",
        unmatched_suffix="include",
        live_generation="idle",
        browse_source="live",
    )

    assert (
        keys.cache_key
        == "library:albums:u42:afx:electro:warp:1995:recent:96:0:library:include:idle:live"
    )
    assert (
        keys.stable_cache_key
        == "library:albums:u42:afx:electro:warp:1995:recent:96:0:library:include:live"
    )
