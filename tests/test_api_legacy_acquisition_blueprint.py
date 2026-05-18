from __future__ import annotations

from flask import Flask

from pmda_api.legacy_acquisition import create_legacy_acquisition_blueprint


def test_legacy_acquisition_routes_return_gone_without_work():
    calls: list[list[tuple]] = []
    app = Flask(__name__)
    app.register_blueprint(
        create_legacy_acquisition_blueprint(
            set_lidarr_progress=lambda rows: calls.append(rows),
        )
    )

    client = app.test_client()
    for method, path in (
        ("post", "/api/lidarr/add-album"),
        ("post", "/api/lidarr/add-incomplete-albums"),
        ("get", "/api/lidarr/add-incomplete-albums/progress"),
        ("post", "/api/lidarr/add-artist"),
        ("post", "/api/lidarr/test"),
        ("post", "/api/autobrr/create-filter"),
        ("post", "/api/autobrr/test"),
    ):
        response = getattr(client, method)(path, json={})
        assert response.status_code == 410
        payload = response.get_json()
        assert payload.get("disabled") is True or "disabled" in str(payload).lower() or "currently disabled" in str(payload)

    assert calls == [[]]
