from pmda_publication import snapshot


def test_should_use_published_snapshot_during_scan_or_rebuild():
    assert snapshot.should_use_published_snapshot(scope="library", scan_running=True)
    assert snapshot.should_use_published_snapshot(scope="auto", index_running=True)
    assert snapshot.should_use_published_snapshot(scope="library", index_underbuilt=True)


def test_should_not_force_snapshot_for_non_library_scopes():
    assert not snapshot.should_use_published_snapshot(scope="inbox", scan_running=True, index_running=True)


def test_snapshot_counts_payload_normalizes_values():
    payload = snapshot.snapshot_counts_payload(albums="12", artists="-4", tracks=None)

    assert payload == {
        "source": "published_snapshot",
        "albums": 12,
        "artists": 0,
        "tracks": 0,
    }


def test_published_scope_where_sqlite_uses_canonical_roots():
    params = []
    context = snapshot.PublishedScopeContext(
        library_roots=("/music/Music_matched",),
        inbox_roots=("/music/Music_dump",),
        dupe_roots=("/dupes",),
        use_virtual_scope=False,
    )

    where_sql = snapshot.published_scope_where_sqlite("library", params, context=context)
    assert where_sql == "((folder_path = ? OR folder_path LIKE ?))"
    assert params == ["/music/Music_matched", "/music/Music_matched/%"]

    inbox_params = []
    inbox_sql = snapshot.published_scope_where_sqlite("inbox", inbox_params, context=context)
    assert "COALESCE(strict_match_verified, 0) = 0" in inbox_sql
    assert "/music/Music_dump" in inbox_params
    assert "/music/Music_matched" in inbox_params
    assert "/dupes" in inbox_params


def test_published_album_where_sqlite_adds_filters():
    context = snapshot.PublishedScopeContext(library_roots=("/music/Music_matched",))
    where_sql, params = snapshot.published_album_where_sqlite(
        include_unmatched=False,
        context=context,
        scope="library",
        search_query="afx",
        genre="electronic, ambient",
        label="warp",
        year=1992,
    )

    assert "COALESCE(strict_match_verified, 0) = 1" in where_sql
    assert "album_title LIKE ?" in where_sql
    assert "COALESCE(year, 0) = ?" in where_sql
    assert "lower(trim(COALESCE(label, ''))) = ?" in where_sql
    assert 'lower(COALESCE(tags_json, \'[]\')) LIKE ?' in where_sql
    assert "/music/Music_matched" in params
    assert "%afx%" in params
    assert 1992 in params
    assert "warp" in params
    assert '%"electronic"%' in params


def test_snapshot_fallback_and_browse_source_policy():
    underbuilt = {"published_albums": 100, "pg_albums": 0, "pg_artists": 0, "underbuilt": True}
    assert snapshot.should_fallback_to_published(underbuilt)
    assert snapshot.browse_source_effective(scope="library", snapshot=underbuilt, scan_busy=False) == "published"
    assert snapshot.browse_source_effective(scope="library", requested="live", snapshot=underbuilt) == "live"
    assert (
        snapshot.browse_source_effective(
            scope="library",
            snapshot={"published_albums": 100, "api_lightweight": True},
        )
        == "published"
    )
    assert (
        snapshot.browse_source_effective(
            scope="library",
            snapshot={"published_albums": 100, "index_state": {"running": True}},
        )
        == "published"
    )
    assert not snapshot.should_fallback_to_published({"published_albums": 0, "underbuilt": True})


def test_progress_library_visibility_uses_fast_counts_during_scan():
    payload = snapshot.progress_library_visibility(
        files_mode=True,
        include_unmatched_default=True,
        scanning=True,
        cached_payload={
            "library_visible_albums_count": 80,
            "library_visible_artists_count": 20,
            "library_visible_tracks_count": 300,
        },
        scan_processed_albums_count=10,
        total_albums=100,
        scan_published_albums_count=42,
        browse_counts=lambda include_unmatched: (12, None),
        effective_browse_snapshot=lambda include_unmatched: {},
    )

    assert payload["albums_count"] == 80
    assert payload["artists_count"] == 20
    assert payload["tracks_count"] == 300
    assert payload["fallback_source"] is None


def test_progress_library_visibility_uses_snapshot_when_idle():
    payload = snapshot.progress_library_visibility(
        files_mode=True,
        include_unmatched_default=False,
        scanning=False,
        cached_payload=None,
        scan_processed_albums_count=0,
        total_albums=0,
        scan_published_albums_count=0,
        browse_counts=lambda include_unmatched: (None, None),
        effective_browse_snapshot=lambda include_unmatched: {
            "visible_albums": 60,
            "visible_artists": 30,
            "visible_tracks": 900,
            "fallback_source": "published",
        },
    )

    assert payload["albums_count"] == 60
    assert payload["artists_count"] == 30
    assert payload["tracks_count"] == 900
    assert payload["fallback_source"] == "published"
