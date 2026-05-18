from __future__ import annotations

from flask import Flask

from pmda_api.player import create_player_blueprint


class _Runtime:
    PLEX_HOST = "http://plex:32400"
    PLEX_TOKEN = "secret"
    JELLYFIN_URL = "http://jellyfin:8096"
    JELLYFIN_API_KEY = "jf-key"
    NAVIDROME_URL = "http://navidrome:4533"
    NAVIDROME_USERNAME = "nav"
    NAVIDROME_PASSWORD = "pwd"
    NAVIDROME_API_KEY = ""

    def __init__(self):
        self.checked = []
        self.refreshed = []

    @staticmethod
    def _effective_player_target(raw):
        return str(raw or "none").strip().lower()

    def _do_plex_check(self, host, token):
        self.checked.append(("plex", host, token))
        return True, "plex ok"

    def _do_jellyfin_check(self, url, api_key):
        self.checked.append(("jellyfin", url, api_key))
        return True, "jellyfin ok"

    def _do_navidrome_check(self, url, username, password, api_key):
        self.checked.append(("navidrome", url, username, password, api_key))
        return True, "navidrome ok"

    def _trigger_player_refresh_by_target(self, target):
        self.refreshed.append(target)
        return target != "none", f"{target} refresh"


def _app(runtime):
    app = Flask(__name__)
    app.register_blueprint(create_player_blueprint(runtime=runtime))
    return app


def test_player_check_routes_to_selected_target_defaults():
    runtime = _Runtime()
    res = _app(runtime).test_client().post("/api/player/check", json={"target": "plex"})

    assert res.status_code == 200
    assert res.get_json() == {"success": True, "target": "plex", "message": "plex ok"}
    assert runtime.checked == [("plex", "http://plex:32400", "secret")]


def test_player_check_accepts_request_overrides():
    runtime = _Runtime()
    res = _app(runtime).test_client().post(
        "/api/player/check",
        json={"target": "jellyfin", "JELLYFIN_URL": "http://other:8096", "JELLYFIN_API_KEY": "override"},
    )

    assert res.status_code == 200
    assert res.get_json()["message"] == "jellyfin ok"
    assert runtime.checked == [("jellyfin", "http://other:8096", "override")]


def test_player_refresh_uses_runtime_refresh_policy():
    runtime = _Runtime()
    res = _app(runtime).test_client().post("/api/player/refresh", json={"target": "navidrome"})

    assert res.status_code == 200
    assert res.get_json() == {"success": True, "target": "navidrome", "message": "navidrome refresh"}
    assert runtime.refreshed == ["navidrome"]


def test_player_check_rejects_missing_target():
    runtime = _Runtime()
    res = _app(runtime).test_client().post("/api/player/check", json={})

    assert res.status_code == 400
    assert res.get_json()["target"] == "none"
