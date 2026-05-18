import json

import pmda


def test_split_genre_values_handles_json_and_delimiters():
    assert pmda._split_genre_values('["ambient", "drone / experimental"]') == [
        "ambient",
        "drone",
        "experimental",
    ]
    assert pmda._split_genre_values("electronic; idm | techno") == [
        "electronic",
        "idm",
        "techno",
    ]


def test_merge_album_genre_lists_dedupes_and_preserves_order():
    assert pmda._merge_album_genre_lists(
        '["Ambient", "Drone"]',
        "ambient; Experimental",
        ["Drone", "Tape"],
    ) == ["Ambient", "Drone", "Experimental", "Tape"]


def test_infer_genre_from_bandcamp_tags_filters_locations_but_keeps_music_tags():
    assert pmda._infer_genre_from_bandcamp_tags(
        ["New Mexico", "Digital", "Ambient", "IDM", "Techno", "Ambient"]
    ) == "ambient; idm; techno"


def test_apply_genre_defaults_backfills_artist_dominant_genre_and_clears_missing_required():
    albums = [
        {
            "artist_norm": "artist-a",
            "genre": "Ambient",
            "tags_json": json.dumps(["Ambient"]),
            "missing_required_tags_json": json.dumps([]),
        },
        {
            "artist_norm": "artist-a",
            "genre": "",
            "tags_json": json.dumps([]),
            "primary_tags_json": json.dumps({}),
            "missing_required_tags_json": json.dumps(["genre", "year"]),
        },
    ]

    pmda._apply_genre_defaults_to_albums_payload(albums)

    assert albums[1]["genre"] == "ambient"
    assert json.loads(albums[1]["tags_json"])[0] == "ambient"
    assert json.loads(albums[1]["primary_tags_json"])["genre"] == "ambient"
    assert json.loads(albums[1]["missing_required_tags_json"]) == ["year"]
