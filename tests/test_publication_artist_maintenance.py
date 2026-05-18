from pmda_publication import artist_maintenance


class FakeCursor:
    def __init__(self, fetchone_value=None, fetchall_value=None):
        self.fetchone_value = fetchone_value
        self.fetchall_value = fetchall_value if fetchall_value is not None else []
        self.sql = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, *args):
        self.sql.append(str(sql))

    def fetchone(self):
        return self.fetchone_value

    def fetchall(self):
        return self.fetchall_value


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_obj = cursor

    def cursor(self):
        return self.cursor_obj


def test_external_artist_image_migration_short_circuits_when_already_current():
    cur = FakeCursor(fetchone_value=("strict_v2",))

    artist_maintenance.migrate_external_artist_images_norm_keys(
        cur,
        norm_artist_key=lambda value: value.lower(),
        path_size=lambda value: 0,
        is_probably_placeholder_artist_image_url=lambda value: False,
    )

    assert len(cur.sql) == 1
    assert "external_artist_images_norm" in cur.sql[0]


def test_artist_alias_backfill_batches_artist_norms():
    cur = FakeCursor(fetchall_value=[("alpha",), ("beta",)])
    conn = FakeConnection(cur)
    synced = []

    def sync_aliases(conn_arg, *, artist_norms):
        synced.append((conn_arg, list(artist_norms)))

    artist_maintenance.backfill_artist_alias_table(
        conn,
        files_sync_artist_aliases=sync_aliases,
    )

    assert synced == [(conn, ["alpha", "beta"])]
