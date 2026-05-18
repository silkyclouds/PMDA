from __future__ import annotations

from flask import Flask

from pmda_api.user_feedback import create_user_feedback_blueprint


class _Cursor:
    def __init__(self, rows=None, one=(1,)):
        self.rows = rows or []
        self.one = one
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.one


class _Conn:
    def __init__(self, rows=None, one=(1,)):
        self.cursor_obj = _Cursor(rows=rows, one=one)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def close(self):
        self.closed = True


class _Runtime:
    def __init__(self, rows=None, *, user_id=1, album_exists=True):
        self.conn = _Conn(rows=rows, one=(1,) if album_exists else None)
        self.user_id = user_id
        self.cache_invalidated = False
        self.lastfm_love = None

    @staticmethod
    def _get_library_mode():
        return "files"

    @staticmethod
    def _ensure_files_index_ready():
        return True, None

    def _current_user_id_or_zero(self):
        return self.user_id

    @staticmethod
    def _social_entity_type_allowed(entity_type):
        return entity_type in {"album", "artist", "track", "genre", "label"}

    @staticmethod
    def _social_entity_key_norm(_entity_type, value):
        return str(value or "").strip().lower()

    @staticmethod
    def _parse_int_loose(value, default=0):
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _parse_bool(value):
        return str(value).strip().lower() not in {"0", "false", "no", "off", ""}

    def _files_pg_connect(self, *_, **__):
        return self.conn

    @staticmethod
    def _lastfm_sync_loved_tracks_to_pmda(*_args, **_kwargs):
        return None

    def _lastfm_set_track_love(self, track_id, liked):
        self.lastfm_love = (track_id, liked)

    @staticmethod
    def _files_user_album_feedback_row(_cur, _uid, _album_id):
        return 4, "Existing note", 123

    @staticmethod
    def _merge_user_album_feedback(current_rating, current_review_text, *, rating=None, review_text=None):
        next_rating = current_rating if rating is None else int(rating)
        next_review = current_review_text if review_text is None else str(review_text or "").strip()
        return next_rating, next_review, next_rating <= 0 and not next_review

    @staticmethod
    def _normalize_user_album_review_text(value):
        return str(value or "").strip()

    def _files_cache_invalidate_all(self):
        self.cache_invalidated = True


def _client(runtime):
    app = Flask(__name__)
    app.register_blueprint(create_user_feedback_blueprint(runtime=runtime))
    return app.test_client()


def test_likes_get_returns_rows():
    runtime = _Runtime(rows=[(7, "album:7", True, 1234)])
    res = _client(runtime).get("/api/library/likes?entity_type=album&ids=7")

    assert res.status_code == 200
    payload = res.get_json()
    assert payload["entity_type"] == "album"
    assert payload["items"] == [{"entity_id": 7, "entity_key": "album:7", "liked": True, "updated_at": 1234}]
    assert runtime.conn.closed is True


def test_album_rating_get_returns_feedback():
    runtime = _Runtime()
    res = _client(runtime).get("/api/library/album/42/rating")

    assert res.status_code == 200
    assert res.get_json()["rating"] == 4
    assert res.get_json()["review_text"] == "Existing note"


def test_album_review_put_updates_feedback_and_invalidates_cache():
    runtime = _Runtime()
    res = _client(runtime).put("/api/library/album/42/review", json={"review_text": "  Better now  "})

    assert res.status_code == 200
    payload = res.get_json()
    assert payload["review_text"] == "Better now"
    assert runtime.cache_invalidated is True


def test_album_review_put_reports_missing_album():
    runtime = _Runtime(album_exists=False)
    res = _client(runtime).put("/api/library/album/42/review", json={"review_text": "Missing"})

    assert res.status_code == 404
    assert res.get_json()["error"] == "Album not found"
