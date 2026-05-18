from __future__ import annotations

from flask import Flask

from pmda_api.logs import create_logs_blueprint


def _make_app():
    app = Flask(__name__)
    app.register_blueprint(
        create_logs_blueprint(
            get_log_file=lambda: "/tmp/pmda.log",
            parse_bool=lambda value: str(value).lower() in {"1", "true", "yes", "on"},
            recent_log_tail_entries=lambda **_kwargs: [
                {
                    "raw": "12:00:00 | INFO | scan | hello",
                    "message": "hello",
                    "kind": "info",
                }
            ],
            tail_log_entries=lambda *_args, **_kwargs: [],
            tail_log_lines=lambda *_args, **_kwargs: ["disk line"],
        )
    )
    return app


def test_logs_tail_uses_live_buffer_when_available():
    client = _make_app().test_client()

    response = client.get("/api/logs/tail")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["path"].startswith("live://process-buffer")
    assert payload["entries"][0]["message"] == "hello"


def test_logs_download_returns_text_attachment():
    client = _make_app().test_client()

    response = client.get("/api/logs/download?lines=200")

    assert response.status_code == 200
    assert response.mimetype == "text/plain"
    assert "attachment" in response.headers["Content-Disposition"]
    assert response.text == "disk line\n"
