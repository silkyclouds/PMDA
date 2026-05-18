from __future__ import annotations

from contextlib import contextmanager

from flask import Flask

from pmda_api.library_stats import create_library_stats_blueprint


class _Runtime:
    FILES_ROOTS = ["/music/Music_matched", "/music/Music_dump"]

    def __init__(self):
        self.cache = {}
        self.rebuild_requested = False

    @staticmethod
    def _get_library_mode():
        return "files"

    @staticmethod
    def _library_include_unmatched_effective():
        return True

    @staticmethod
    def _library_scope_effective():
        return "library"

    @staticmethod
    def _files_library_browse_source_requested():
        return "auto"

    @staticmethod
    def _library_albums_match_where(_include_unmatched, _alias):
        return "1=1"

    @staticmethod
    def _library_album_scope_where(_scope, _alias):
        return "1=1"

    @staticmethod
    def _library_cache_scope_suffix(scope):
        return scope

    @staticmethod
    def _library_cache_unmatched_suffix(include_unmatched):
        return "unmatched" if include_unmatched else "matched"

    def _files_cache_get_json(self, key):
        return self.cache.get(key)

    def _files_cache_set_json(self, key, payload, ttl=0):
        self.cache[key] = dict(payload)

    @staticmethod
    def _files_index_maybe_enqueue_published_catchup(**_kwargs):
        return {
            "published_artists": 12,
            "published_albums": 34,
            "published_tracks": 567,
            "underbuilt": True,
            "index_state": {"running": False, "phase": "ready"},
        }

    @staticmethod
    def _files_library_browse_source_effective(**_kwargs):
        return "published"

    @staticmethod
    def _pipeline_bootstrap_status():
        return {"bootstrap_required": False}

    def _trigger_files_index_rebuild_async_throttled(self, **_kwargs):
        self.rebuild_requested = True

    @staticmethod
    def _files_index_get_state():
        return {"running": True, "phase": "queued"}

    @staticmethod
    def _files_pg_init_schema():
        raise AssertionError("published fallback should not initialize PostgreSQL")

    @staticmethod
    def _files_pg_connect(*_args, **_kwargs):
        raise AssertionError("published fallback should not open PostgreSQL")

    @staticmethod
    @contextmanager
    def _files_pg_statement_timeout(_cur, _timeout_ms):
        yield

    @staticmethod
    def _files_library_should_fallback_to_published(*_args, **_kwargs):
        return False

    @staticmethod
    def _parse_files_roots(value):
        return [part.strip() for part in str(value or "").split(",") if part.strip()]

    @staticmethod
    def _get_config_from_db(_key, default=""):
        return default


def test_library_stats_blueprint_uses_published_snapshot_without_pg():
    app = Flask(__name__)
    app.register_blueprint(create_library_stats_blueprint(runtime=_Runtime()))
    client = app.test_client()

    res = client.get("/api/library/stats")
    assert res.status_code == 200
    assert res.get_json()["albums"] == 34
    assert res.get_json()["artists"] == 12
    assert res.get_json()["fallback_source"] == "published"


def test_library_stats_library_blueprint_keeps_distribution_shape_on_snapshot():
    app = Flask(__name__)
    app.register_blueprint(create_library_stats_blueprint(runtime=_Runtime()))
    client = app.test_client()

    res = client.get("/api/library/stats/library")
    assert res.status_code == 200
    payload = res.get_json()
    assert payload["tracks"] == 567
    assert payload["quality"]["without_cover"] == 34
    assert payload["source_paths"] == []
