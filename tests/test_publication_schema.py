from pmda_publication import schema


class FakeCursor:
    def __init__(self):
        self.sql = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, *args, **kwargs):
        self.sql.append(str(sql))

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


def test_files_pg_schema_short_circuits_when_already_ready():
    called = False

    def connect(**_kwargs):
        nonlocal called
        called = True
        return FakeConnection()

    assert schema.init_files_pg_schema(
        schema_ready=True,
        files_pg_connect=connect,
        migrate_external_artist_images_norm_keys=lambda cur: None,
        backfill_artist_canonical_fields=lambda conn: None,
        backfill_artist_alias_table=lambda conn: None,
        merge_duplicate_person_artists=lambda conn: None,
        relink_external_artist_images_to_canonical_norm=lambda conn: None,
        purge_weak_classical_artist_images=lambda conn: None,
    )
    assert called is False


def test_files_pg_schema_creates_core_tables_and_closes_connection():
    conn = FakeConnection()
    callbacks = []

    def record(name):
        def _inner(_arg):
            callbacks.append(name)

        return _inner

    assert schema.init_files_pg_schema(
        schema_ready=False,
        files_pg_connect=lambda **_kwargs: conn,
        migrate_external_artist_images_norm_keys=record("external-images"),
        backfill_artist_canonical_fields=record("canonical-fields"),
        backfill_artist_alias_table=record("aliases"),
        merge_duplicate_person_artists=record("person-merge"),
        relink_external_artist_images_to_canonical_norm=record("image-relink"),
        purge_weak_classical_artist_images=record("image-policy"),
    )

    executed_sql = "\n".join(conn.cursor_obj.sql)
    assert "CREATE TABLE IF NOT EXISTS files_artists" in executed_sql
    assert "CREATE TABLE IF NOT EXISTS files_albums" in executed_sql
    assert "CREATE TABLE IF NOT EXISTS files_tracks" in executed_sql
    assert callbacks == [
        "external-images",
        "canonical-fields",
        "aliases",
        "person-merge",
        "image-relink",
        "image-policy",
    ]
    assert conn.closed is True
