from __future__ import annotations

import re

from flask import Flask

from pmda_api.library_normalization import create_library_normalization_blueprint


class _Cursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return list(self.rows)


class _Conn:
    def __init__(self, rows):
        self.rows = rows
        self.closed = False
        self.cursor_obj = _Cursor(rows)

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


class _Runtime:
    def __init__(self, rows, *, mode="files", index_ready=True):
        self.rows = rows
        self.mode = mode
        self.index_ready = index_ready
        self.conn = _Conn(rows)
        self.rebuild_reason = None

    def _get_library_mode(self):
        return self.mode

    def _ensure_files_index_ready(self):
        return (self.index_ready, None if self.index_ready else "not ready")

    def _files_pg_connect(self, *_, **__):
        return self.conn

    @staticmethod
    def path_for_fs_access(path):
        return path

    @staticmethod
    def strip_parenthetical_suffixes(name):
        return re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()

    def _trigger_files_index_rebuild_async(self, *, reason):
        self.rebuild_reason = reason


def test_albums_with_parenthetical_names_reports_candidates(tmp_path):
    folder = tmp_path / "Artist - Album (FLAC)"
    folder.mkdir()
    runtime = _Runtime([(7, "Album", str(folder), "Artist")])
    app = Flask(__name__)
    app.register_blueprint(create_library_normalization_blueprint(runtime=runtime))

    res = app.test_client().get("/api/library/albums-with-parenthetical-names")

    assert res.status_code == 200
    payload = res.get_json()
    assert payload["albums"][0]["album_id"] == 7
    assert payload["albums"][0]["current_name"] == "Artist - Album (FLAC)"
    assert payload["albums"][0]["proposed_name"] == "Artist - Album"
    assert runtime.conn.closed is True


def test_normalize_album_names_renames_and_requests_index_rebuild(tmp_path):
    folder = tmp_path / "Artist - Album (WEB)"
    folder.mkdir()
    runtime = _Runtime([(11, str(folder))])
    app = Flask(__name__)
    app.register_blueprint(create_library_normalization_blueprint(runtime=runtime))

    res = app.test_client().post("/api/library/normalize-album-names", json={"album_ids": [11]})

    assert res.status_code == 200
    payload = res.get_json()
    assert payload["errors"] == []
    assert payload["renamed"][0]["album_id"] == 11
    assert (tmp_path / "Artist - Album").exists()
    assert runtime.rebuild_reason == "normalize_album_names"


def test_normalization_rejects_non_files_mode_without_plex_fallback():
    runtime = _Runtime([], mode="plex")
    app = Flask(__name__)
    app.register_blueprint(create_library_normalization_blueprint(runtime=runtime))

    res = app.test_client().get("/api/library/albums-with-parenthetical-names")

    assert res.status_code == 400
    assert res.get_json()["error"] == "Files mode required"
