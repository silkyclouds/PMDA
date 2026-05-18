import sqlite3
from pathlib import Path

from pmda_core import schema


def test_state_schema_bootstrap_creates_core_tables(tmp_path: Path):
    state_db = tmp_path / "state.db"

    def enable_wal(con, *, label="state-db"):
        con.execute("PRAGMA journal_mode=WAL")

    schema.init_state_db(
        state_db_file=state_db,
        state_db_busy_timeout_seconds=30.0,
        enable_wal=enable_wal,
        ai_pricing_default_rows=[],
        ai_pricing_version="test",
    )

    con = sqlite3.connect(state_db)
    try:
        tables = {
            row[0]
            for row in con.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name IN (
                    'scan_history',
                    'pipeline_jobs',
                    'scan_moves',
                    'files_library_published_albums',
                    'metadata_jobs'
                  )
                """
            ).fetchall()
        }
    finally:
        con.close()

    assert tables == {
        "scan_history",
        "pipeline_jobs",
        "scan_moves",
        "files_library_published_albums",
        "metadata_jobs",
    }


def test_settings_schema_bootstrap_creates_auth_and_ai_tables(tmp_path: Path):
    settings_db = tmp_path / "settings.db"

    schema.init_settings_db(settings_db_file=settings_db)

    con = sqlite3.connect(settings_db)
    try:
        tables = {
            row[0]
            for row in con.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name IN (
                    'settings',
                    'auth_users',
                    'auth_sessions',
                    'ai_auth_profiles'
                  )
                """
            ).fetchall()
        }
    finally:
        con.close()

    assert tables == {
        "settings",
        "auth_users",
        "auth_sessions",
        "ai_auth_profiles",
    }
