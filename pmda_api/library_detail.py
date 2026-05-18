"""Library artist, album, artwork, and stream detail routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint


def create_library_detail_blueprint(*, runtime: Any) -> Blueprint:
    """Create detail/media routes while keeping public URLs stable."""

    blueprint = Blueprint("pmda_library_detail", __name__)

    @blueprint.get("/api/library/artist/<int:artist_id>", endpoint="api_library_artist_detail")
    def api_library_artist_detail(artist_id: int):
        return runtime.api_library_artist_detail(artist_id)

    @blueprint.get("/api/library/artist/<int:artist_id>/profile", endpoint="api_library_artist_profile")
    def api_library_artist_profile(artist_id: int):
        return runtime.api_library_artist_profile(artist_id)

    @blueprint.post("/api/library/artist/<int:artist_id>/ai/enrich", endpoint="api_library_artist_ai_enrich")
    def api_library_artist_ai_enrich(artist_id: int):
        return runtime.api_library_artist_ai_enrich(artist_id)

    @blueprint.get("/api/library/artist/<int:artist_id>/summary", endpoint="api_library_artist_summary")
    def api_library_artist_summary(artist_id: int):
        return runtime.api_library_artist_summary(artist_id)

    @blueprint.post("/api/library/artist/<int:artist_id>/summary/ai", endpoint="api_library_artist_summary_ai")
    def api_library_artist_summary_ai(artist_id: int):
        return runtime.api_library_artist_summary_ai(artist_id)

    @blueprint.get("/api/library/artist/<int:artist_id>/concerts", endpoint="api_library_artist_concerts")
    def api_library_artist_concerts(artist_id: int):
        return runtime.api_library_artist_concerts(artist_id)

    @blueprint.get("/api/library/artist/<int:artist_id>/facts", endpoint="api_library_artist_facts")
    def api_library_artist_facts(artist_id: int):
        return runtime.api_library_artist_facts(artist_id)

    @blueprint.post("/api/library/artist/<int:artist_id>/facts/extract", endpoint="api_library_artist_facts_extract")
    def api_library_artist_facts_extract(artist_id: int):
        return runtime.api_library_artist_facts_extract(artist_id)

    @blueprint.get("/api/library/album/<int:album_id>/tracks", endpoint="api_library_album_tracks")
    def api_library_album_tracks(album_id: int):
        return runtime.api_library_album_tracks(album_id)

    @blueprint.get("/api/library/album/<int:album_id>", endpoint="api_library_album_detail")
    def api_library_album_detail(album_id: int):
        return runtime.api_library_album_detail(album_id)

    @blueprint.get("/api/library/album/<int:album_id>/download", endpoint="api_library_album_download")
    def api_library_album_download(album_id: int):
        return runtime.api_library_album_download(album_id)

    @blueprint.get("/api/library/album/<int:album_id>/artwork-gallery", endpoint="api_library_album_artwork_gallery")
    def api_library_album_artwork_gallery(album_id: int):
        return runtime.api_library_album_artwork_gallery(album_id)

    @blueprint.get("/api/library/track/<int:track_id>/stream", endpoint="api_library_track_stream")
    def api_library_track_stream(track_id: int):
        return runtime.api_library_track_stream(track_id)

    @blueprint.get("/api/library/files/album/<int:album_id>/cover", endpoint="api_library_files_album_cover")
    def api_library_files_album_cover(album_id: int):
        return runtime.api_library_files_album_cover(album_id)

    @blueprint.get(
        "/api/library/files/album/<int:album_id>/artwork/<artwork_id>",
        endpoint="api_library_files_album_artwork_item",
    )
    def api_library_files_album_artwork_item(album_id: int, artwork_id: str):
        return runtime.api_library_files_album_artwork_item(album_id, artwork_id)

    @blueprint.get("/api/library/files/artist/<int:artist_id>/image", endpoint="api_library_files_artist_image")
    def api_library_files_artist_image(artist_id: int):
        return runtime.api_library_files_artist_image(artist_id)

    @blueprint.get("/api/library/external/artist-image/<path:name_norm>", endpoint="api_library_external_artist_image")
    def api_library_external_artist_image(name_norm: str):
        return runtime.api_library_external_artist_image(name_norm)

    @blueprint.get("/api/library/external/label-image/<path:label_norm>", endpoint="api_library_external_label_image")
    def api_library_external_label_image(label_norm: str):
        return runtime.api_library_external_label_image(label_norm)

    @blueprint.get("/api/library/artist/<int:artist_id>/similar", endpoint="api_library_artist_similar")
    def api_library_artist_similar(artist_id: int):
        return runtime.api_library_artist_similar(artist_id)

    @blueprint.get("/api/library/release-group/<mbid>/labels", endpoint="api_library_release_group_labels")
    def api_library_release_group_labels(mbid: str):
        return runtime.api_library_release_group_labels(mbid)

    @blueprint.get("/api/library/artist/<int:artist_id>/monitored", endpoint="api_library_artist_monitored")
    def api_library_artist_monitored(artist_id: int):
        return runtime.api_library_artist_monitored(artist_id)

    @blueprint.get("/api/library/artist/<int:artist_id>/images", endpoint="api_library_artist_images")
    def api_library_artist_images(artist_id: int):
        return runtime.api_library_artist_images(artist_id)

    @blueprint.get("/api/library/album/<int:album_id>/tags", endpoint="api_library_album_tags")
    def api_library_album_tags(album_id: int):
        return runtime.api_library_album_tags(album_id)

    @blueprint.get("/api/library/album/<int:album_id>/tracks/detail", endpoint="api_library_album_tracks_detail")
    def api_library_album_tracks_detail(album_id: int):
        return runtime.api_library_album_tracks_detail(album_id)

    return blueprint
