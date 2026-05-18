from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path

from flask import Flask, jsonify

from pmda_api.dedupe_details import create_dedupe_details_blueprint


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.queries.append((sql, params))

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows):
        self.cursor_obj = _Cursor(rows)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


class _Runtime:
    def __init__(self, *, library_mode="files", rows=None):
        self.library_mode = library_mode
        self.connection = _Connection(rows or [(1, "Track One", 123), (2, "Track Two", 45)])
        self.purged = []
        self.plex_called = False

    def _get_library_mode(self):
        return self.library_mode

    @staticmethod
    def path_for_fs_access(folder):
        return Path(folder)

    @staticmethod
    def _files_album_id_for_folder(_folder):
        return 42

    def _files_pg_connect(self, acquire_timeout_sec=0.75):
        assert acquire_timeout_sec == 0.75
        return self.connection

    @staticmethod
    def _files_pg_statement_timeout(_cur, _timeout_ms):
        return nullcontext()

    @staticmethod
    def _duplicate_tracks_from_folder(_folder, _edition):
        return [{"idx": 1, "title": "Fallback Track", "dur": 1000}]

    @staticmethod
    def analyse_format(_folder):
        return ("FLAC", 900, 44100, 16, "ignored")

    @staticmethod
    def _requires_config():
        return None

    @staticmethod
    def library_is_audit_mode():
        return False

    @staticmethod
    def _require_admin_json():
        return None

    def _purge_invalid_edition(self, edition):
        self.purged.append(edition)

    def plex_connect(self):
        self.plex_called = True
        raise AssertionError("Plex DB must not be used by dedupe detail routes")


def _app(runtime):
    app = Flask(__name__)
    app.register_blueprint(create_dedupe_details_blueprint(runtime=runtime))
    return app


def test_edition_details_reads_files_tracks_without_plex():
    runtime = _Runtime()
    res = _app(runtime).test_client().get("/api/edition_details?album_id=7&folder=/tmp")

    assert res.status_code == 200
    assert res.get_json() == {
        "tracks": [
            {"idx": 1, "title": "Track One", "dur": 123000},
            {"idx": 2, "title": "Track Two", "dur": 45000},
        ],
        "info": ["FLAC", 900, 44100, 16],
    }
    assert runtime.connection.closed is True
    assert runtime.plex_called is False


def test_edition_details_rejects_non_files_mode_instead_of_opening_plex_db():
    runtime = _Runtime(library_mode="legacy")
    res = _app(runtime).test_client().get("/api/edition_details?album_id=7&folder=/tmp")

    assert res.status_code == 410
    assert res.get_json()["error"] == "edition_details_files_only"
    assert runtime.plex_called is False


def test_dedupe_manual_delegates_to_runtime_purge():
    runtime = _Runtime()
    res = _app(runtime).test_client().post(
        "/api/dedupe_manual",
        json=[{"folder": "/music/A", "album_id": 123}],
    )

    assert res.status_code == 200
    assert res.get_json() == {"status": "ok"}
    assert runtime.purged == [{"folder": "/music/A", "artist": "", "title_raw": "", "album_id": 123}]


def test_dedupe_manual_returns_config_gate_response():
    class RuntimeWithGate(_Runtime):
        @staticmethod
        def _requires_config():
            return jsonify({"error": "config_required"}), 503

    res = _app(RuntimeWithGate()).test_client().post("/api/dedupe_manual", json=[])

    assert res.status_code == 503
    assert res.get_json() == {"error": "config_required"}
