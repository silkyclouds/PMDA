from __future__ import annotations

from flask import Flask, jsonify

from pmda_api.settings_config import create_settings_config_blueprint


class _Runtime:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def api_config_get(self):
        self.calls.append("config_get")
        return jsonify({"config": True})

    def api_config_put(self):
        self.calls.append("config_put")
        return jsonify({"ok": True})

    def api_ai_provider_preferences_get(self):
        self.calls.append("ai_prefs_get")
        return jsonify({"preferences": {}})

    def api_ai_provider_preferences_put(self):
        self.calls.append("ai_prefs_put")
        return jsonify({"ok": True})


def test_settings_config_blueprint_delegates_public_routes():
    runtime = _Runtime()
    app = Flask(__name__)
    app.register_blueprint(create_settings_config_blueprint(runtime=runtime))
    client = app.test_client()

    assert client.get("/api/config").get_json() == {"config": True}
    assert client.put("/api/config").get_json() == {"ok": True}
    assert client.get("/api/ai/providers/preferences").get_json() == {"preferences": {}}
    assert client.put("/api/ai/providers/preferences").get_json() == {"ok": True}
    assert runtime.calls == ["config_get", "config_put", "ai_prefs_get", "ai_prefs_put"]
