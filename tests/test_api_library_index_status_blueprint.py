from __future__ import annotations

import threading

from flask import Flask

from pmda_api.library_index_status import create_library_index_status_blueprint


class _IndexCore:
    @staticmethod
    def status_payload(st, *, indexed_artists, indexed_albums, indexed_tracks, reco_embeddings):
        payload = dict(st)
        payload.update(
            {
                "indexed_artists": indexed_artists,
                "indexed_albums": indexed_albums,
                "indexed_tracks": indexed_tracks,
                "reco_embeddings": reco_embeddings,
            }
        )
        return payload


class _Runtime:
    lock = threading.RLock()
    state = {"files_reco_embeddings": {"total": 42}}
    _library_index_core = _IndexCore()

    @staticmethod
    def _files_index_get_state():
        return {"running": False, "phase": "ready"}

    @staticmethod
    def _files_index_read_counts_fast(*, acquire_timeout_sec):
        assert acquire_timeout_sec == 0.20
        return 12, 34, 567


def test_library_index_status_blueprint_keeps_payload_shape():
    app = Flask(__name__)
    app.register_blueprint(create_library_index_status_blueprint(runtime=_Runtime()))
    client = app.test_client()

    res = client.get("/api/library/files-index/status")
    assert res.status_code == 200
    payload = res.get_json()
    assert payload["running"] is False
    assert payload["phase"] == "ready"
    assert payload["indexed_artists"] == 12
    assert payload["indexed_albums"] == 34
    assert payload["indexed_tracks"] == 567
    assert payload["reco_embeddings"] == {"total": 42}
