"""Library match/improvement API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint


def create_library_improve_blueprint(*, runtime: Any) -> Blueprint:
    """Create match/improvement routes while keeping public URLs stable."""

    blueprint = Blueprint("pmda_library_improve", __name__)

    @blueprint.get("/api/library/album/<int:album_id>/match-detail", endpoint="api_library_album_match_detail")
    def api_library_album_match_detail(album_id: int):
        return runtime.api_library_album_match_detail(album_id)

    @blueprint.post("/api/library/album/<int:album_id>/review/generate", endpoint="api_library_album_review_generate")
    def api_library_album_review_generate(album_id: int):
        return runtime.api_library_album_review_generate(album_id)

    @blueprint.post("/api/library/album/<int:album_id>/cover/select", endpoint="api_library_album_select_cover")
    def api_library_album_select_cover(album_id: int):
        return runtime.api_library_album_select_cover(album_id)

    @blueprint.get("/api/library/artist/<int:artist_id>/match-detail", endpoint="api_library_artist_match_detail")
    def api_library_artist_match_detail(artist_id: int):
        return runtime.api_library_artist_match_detail(artist_id)

    @blueprint.post("/api/library/album/<int:album_id>/rematch", endpoint="api_library_album_rematch")
    def api_library_album_rematch(album_id: int):
        return runtime.api_library_album_rematch(album_id)

    @blueprint.post("/api/library/artist/<int:artist_id>/rematch", endpoint="api_library_artist_rematch")
    def api_library_artist_rematch(artist_id: int):
        return runtime.api_library_artist_rematch(artist_id)

    @blueprint.post("/api/library/improve-album", endpoint="api_library_improve_album")
    def api_library_improve_album():
        return runtime.api_library_improve_album()

    @blueprint.post("/api/drop/improve", endpoint="api_drop_improve")
    def api_drop_improve():
        return runtime.api_drop_improve()

    @blueprint.post("/api/library/improve-all-albums", endpoint="api_library_improve_all_albums")
    def api_library_improve_all_albums():
        return runtime.api_library_improve_all_albums()

    @blueprint.post("/api/library/improve-all", endpoint="api_library_improve_all")
    def api_library_improve_all():
        return runtime.api_library_improve_all()

    @blueprint.get("/api/library/improve-all-albums/progress", endpoint="api_library_improve_all_albums_progress")
    @blueprint.get("/api/library/improve-all/progress", endpoint="api_library_improve_all_progress")
    def api_library_improve_all_progress():
        return runtime.api_library_improve_all_progress()

    @blueprint.post("/api/musicbrainz/fix-artist-tags", endpoint="api_musicbrainz_fix_artist_tags")
    def api_musicbrainz_fix_artist_tags():
        return runtime.api_musicbrainz_fix_artist_tags()

    @blueprint.post("/api/musicbrainz/fix-album-tags", endpoint="api_musicbrainz_fix_album_tags")
    def api_musicbrainz_fix_album_tags():
        return runtime.api_musicbrainz_fix_album_tags()

    return blueprint
