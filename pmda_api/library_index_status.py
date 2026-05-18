"""Files library index status API route."""

from __future__ import annotations

from typing import Any

from flask import Blueprint, jsonify


def create_library_index_status_blueprint(*, runtime: Any) -> Blueprint:
    """Create the library index status route."""

    blueprint = Blueprint("pmda_library_index_status", __name__)

    @blueprint.get("/api/library/files-index/status", endpoint="api_library_files_index_status")
    def api_library_files_index_status():
        st = runtime._files_index_get_state()
        artists, albums, tracks = runtime._files_index_read_counts_fast(acquire_timeout_sec=0.20)
        with runtime.lock:
            reco_embeddings = dict(runtime.state.get("files_reco_embeddings") or {})
        return jsonify(
            runtime._library_index_core.status_payload(
                st,
                indexed_artists=artists,
                indexed_albums=albums,
                indexed_tracks=tracks,
                reco_embeddings=reco_embeddings,
            )
        )

    return blueprint
