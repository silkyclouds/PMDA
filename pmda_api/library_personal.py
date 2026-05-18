"""Personal library, social, playlist, and playback API routes."""

from __future__ import annotations

from typing import Any

from flask import Blueprint


def create_library_personal_blueprint(*, runtime: Any) -> Blueprint:
    """Create personal library routes while keeping public URLs stable."""

    blueprint = Blueprint("pmda_library_personal", __name__)

    @blueprint.get("/api/library/recently-played/albums", endpoint="api_library_recently_played_albums")
    def api_library_recently_played_albums():
        return runtime.api_library_recently_played_albums()

    @blueprint.get("/api/library/liked", endpoint="api_library_liked_summary")
    def api_library_liked_summary():
        return runtime.api_library_liked_summary()

    @blueprint.get("/api/library/social/users", endpoint="api_library_social_users")
    def api_library_social_users():
        return runtime.api_library_social_users()

    @blueprint.get("/api/library/social/context", endpoint="api_library_social_context")
    def api_library_social_context():
        return runtime.api_library_social_context()

    @blueprint.post("/api/library/share", endpoint="api_library_share")
    def api_library_share():
        return runtime.api_library_share()

    @blueprint.get("/api/library/recommendations", endpoint="api_library_recommendations")
    def api_library_recommendations():
        return runtime.api_library_recommendations()

    @blueprint.post(
        "/api/library/recommendations/<int:recommendation_id>/like",
        endpoint="api_library_recommendation_like",
    )
    def api_library_recommendation_like(recommendation_id: int):
        return runtime.api_library_recommendation_like(recommendation_id)

    @blueprint.get("/api/library/notifications", endpoint="api_library_notifications")
    def api_library_notifications():
        return runtime.api_library_notifications()

    @blueprint.post(
        "/api/library/notifications/<int:notification_id>/read",
        endpoint="api_library_notifications_mark_read",
    )
    def api_library_notifications_mark_read(notification_id: int):
        return runtime.api_library_notifications_mark_read(notification_id)

    @blueprint.get("/api/library/playlists", endpoint="api_library_playlists")
    def api_library_playlists():
        return runtime.api_library_playlists()

    @blueprint.post("/api/library/playlists", endpoint="api_library_playlists_create")
    def api_library_playlists_create():
        return runtime.api_library_playlists_create()

    @blueprint.get("/api/library/playlists/<int:playlist_id>", endpoint="api_library_playlist_detail")
    def api_library_playlist_detail(playlist_id: int):
        return runtime.api_library_playlist_detail(playlist_id)

    @blueprint.delete("/api/library/playlists/<int:playlist_id>", endpoint="api_library_playlist_delete")
    def api_library_playlist_delete(playlist_id: int):
        return runtime.api_library_playlist_delete(playlist_id)

    @blueprint.post("/api/library/playlists/<int:playlist_id>/items", endpoint="api_library_playlist_items_add")
    def api_library_playlist_items_add(playlist_id: int):
        return runtime.api_library_playlist_items_add(playlist_id)

    @blueprint.delete(
        "/api/library/playlists/<int:playlist_id>/items/<int:item_id>",
        endpoint="api_library_playlist_item_delete",
    )
    def api_library_playlist_item_delete(playlist_id: int, item_id: int):
        return runtime.api_library_playlist_item_delete(playlist_id, item_id)

    @blueprint.post("/api/library/playlists/<int:playlist_id>/reorder", endpoint="api_library_playlist_reorder")
    def api_library_playlist_reorder(playlist_id: int):
        return runtime.api_library_playlist_reorder(playlist_id)

    @blueprint.post("/api/library/reco/event", endpoint="api_library_reco_event")
    def api_library_reco_event():
        return runtime.api_library_reco_event()

    @blueprint.post("/api/library/playback/event", endpoint="api_library_playback_event")
    def api_library_playback_event():
        return runtime.api_library_playback_event()

    @blueprint.get("/api/library/playback/stats", endpoint="api_library_playback_stats")
    def api_library_playback_stats():
        return runtime.api_library_playback_stats()

    @blueprint.get("/api/library/reco/for-you", endpoint="api_library_reco_for_you")
    def api_library_reco_for_you():
        return runtime.api_library_reco_for_you()

    return blueprint
