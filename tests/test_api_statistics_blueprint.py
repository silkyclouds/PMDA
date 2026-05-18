from __future__ import annotations

from flask import Flask

from pmda_api.statistics import create_statistics_blueprint


class _Runtime:
    @staticmethod
    def _parse_bool(value):
        return str(value).lower() in {"1", "true", "yes"}

    @staticmethod
    def _collect_cache_control_metrics(*, force=False):
        return {"forced": bool(force), "ok": True}

    @staticmethod
    def _pmda_jobs_status_snapshot():
        return {"jobs": {"scan": {"running": False}}}


def test_statistics_blueprint_keeps_basic_routes_stable():
    app = Flask(__name__)
    app.register_blueprint(create_statistics_blueprint(runtime=_Runtime()))
    client = app.test_client()

    cache = client.get("/api/statistics/cache-control?force=true")
    assert cache.status_code == 200
    assert cache.get_json() == {"forced": True, "ok": True}

    jobs = client.get("/api/jobs/status")
    assert jobs.status_code == 200
    assert jobs.get_json()["jobs"]["scan"]["running"] is False
