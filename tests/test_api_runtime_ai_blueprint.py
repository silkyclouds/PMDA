from __future__ import annotations

from flask import Flask, jsonify

from pmda_api.runtime_ai import create_runtime_ai_blueprint


class _Runtime:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str):
        if not name.startswith("api_"):
            raise AttributeError(name)

        def _handler():
            self.calls.append(name)
            return jsonify({"handler": name})

        return _handler


def test_runtime_ai_blueprint_delegates_core_runtime_routes():
    runtime = _Runtime()
    app = Flask(__name__)
    app.register_blueprint(create_runtime_ai_blueprint(runtime=runtime))
    client = app.test_client()

    checks = [
        ("post", "/api/openai/check", "api_openai_check"),
        ("get", "/api/musicbrainz/test", "api_musicbrainz_test"),
        ("post", "/api/musicbrainz/test", "api_musicbrainz_test"),
        ("get", "/api/ollama/discover", "api_ollama_discover"),
        ("post", "/api/ollama/pull", "api_ollama_pull"),
        ("get", "/api/runtime/managed/status", "api_runtime_managed_status"),
        ("post", "/api/ai/models", "api_ai_models"),
    ]

    for method, path, expected in checks:
        response = getattr(client, method)(path)
        assert response.status_code == 200
        assert response.get_json() == {"handler": expected}

    assert runtime.calls == [expected for _, _, expected in checks]
