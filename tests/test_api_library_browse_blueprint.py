from __future__ import annotations

from flask import Flask, jsonify

from pmda_api.library_browse import create_library_browse_blueprint


class _Runtime:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def api_library_artists(self):
        self.calls.append("artists")
        return jsonify({"artists": [], "total": 0})

    def api_library_albums(self):
        self.calls.append("albums")
        return jsonify({"albums": [], "total": 0})


def test_library_browse_blueprint_delegates_public_routes():
    runtime = _Runtime()
    app = Flask(__name__)
    app.register_blueprint(create_library_browse_blueprint(runtime=runtime))
    client = app.test_client()

    artists = client.get("/api/library/artists")
    albums = client.get("/api/library/albums")

    assert artists.status_code == 200
    assert albums.status_code == 200
    assert artists.get_json() == {"artists": [], "total": 0}
    assert albums.get_json() == {"albums": [], "total": 0}
    assert runtime.calls == ["artists", "albums"]
