"""Library catalog and discovery API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint


def create_library_catalog_blueprint(*, runtime: Any) -> Blueprint:
    """Create catalog/search/facet routes while keeping public URLs stable."""

    blueprint = Blueprint("pmda_library_catalog", __name__)

    @blueprint.get("/api/library/discover", endpoint="api_library_discover")
    def api_library_discover():
        return runtime.api_library_discover()

    @blueprint.get("/api/library/artists/suggest", endpoint="api_library_artists_suggest")
    def api_library_artists_suggest():
        return runtime.api_library_artists_suggest()

    @blueprint.get("/api/library/search/suggest", endpoint="api_library_search_suggest")
    def api_library_search_suggest():
        return runtime.api_library_search_suggest()

    @blueprint.get("/api/library/digest", endpoint="api_library_digest")
    def api_library_digest():
        return runtime.api_library_digest()

    @blueprint.get("/api/library/artists/top", endpoint="api_library_top_artists")
    def api_library_top_artists():
        return runtime.api_library_top_artists()

    @blueprint.get("/api/library/artists/recent", endpoint="api_library_recent_artists")
    def api_library_recent_artists():
        return runtime.api_library_recent_artists()

    @blueprint.get("/api/library/facets", endpoint="api_library_facets")
    def api_library_facets():
        return runtime.api_library_facets()

    @blueprint.get("/api/library/genres/suggest", endpoint="api_library_genres_suggest")
    def api_library_genres_suggest():
        return runtime.api_library_genres_suggest()

    @blueprint.get("/api/library/labels/suggest", endpoint="api_library_labels_suggest")
    def api_library_labels_suggest():
        return runtime.api_library_labels_suggest()

    @blueprint.get("/api/library/genres", endpoint="api_library_genres")
    def api_library_genres():
        return runtime.api_library_genres()

    @blueprint.get("/api/library/labels", endpoint="api_library_labels")
    def api_library_labels():
        return runtime.api_library_labels()

    @blueprint.get("/api/library/genre/<path:genre>/labels", endpoint="api_library_genre_labels")
    def api_library_genre_labels(genre: str):
        return runtime.api_library_genre_labels(genre)

    @blueprint.get("/api/library/genre/<path:genre>/profile", endpoint="api_library_genre_profile")
    def api_library_genre_profile(genre: str):
        return runtime.api_library_genre_profile(genre)

    @blueprint.get("/api/library/label/<path:label>/profile", endpoint="api_library_label_profile")
    def api_library_label_profile(label: str):
        return runtime.api_library_label_profile(label)

    @blueprint.get("/api/library/missing-tags", endpoint="api_library_missing_tags")
    def api_library_missing_tags():
        return runtime.api_library_missing_tags()

    @blueprint.post("/api/library/entity-discover", endpoint="api_library_entity_discover")
    def api_library_entity_discover():
        return runtime.api_library_entity_discover()

    return blueprint
