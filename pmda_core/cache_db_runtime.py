"""SQLite cache database schema and audio cache accessors."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Optional


def _cache_path(runtime_or_path: Any) -> Path:
    if isinstance(runtime_or_path, (str, Path)):
        return Path(runtime_or_path)
    return Path(str(getattr(runtime_or_path, "CACHE_DB_FILE")))


def init_cache_db_for_runtime(runtime: Any) -> None:
    init_cache_db(_cache_path(runtime))


def get_cached_info_for_runtime(runtime: Any, path: str, mtime: int) -> Optional[tuple[int, int, int]]:
    return get_cached_info(_cache_path(runtime), path, mtime)


def set_cached_info_for_runtime(
    runtime: Any,
    path: str,
    mtime: int,
    bit_rate: int,
    sample_rate: int,
    bit_depth: int,
) -> None:
    set_cached_info(_cache_path(runtime), path, mtime, bit_rate, sample_rate, bit_depth)


def get_cached_acoustid_for_runtime(runtime: Any, path: str) -> Optional[tuple[float, str]]:
    return get_cached_acoustid(_cache_path(runtime), path)


def set_cached_acoustid_for_runtime(runtime: Any, path: str, duration: float, fingerprint: str) -> None:
    set_cached_acoustid(_cache_path(runtime), path, duration, fingerprint)


def init_cache_db(cache_db_file: str | Path) -> None:
    con = sqlite3.connect(str(cache_db_file))
    con.execute("PRAGMA journal_mode=WAL;")
    con.commit()
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS audio_cache (
            path       TEXT PRIMARY KEY,
            mtime      INTEGER,
            bit_rate   INTEGER,
            sample_rate INTEGER,
            bit_depth  INTEGER
        )
        """
    )
    try:
        cur.execute("PRAGMA table_info(audio_cache)")
        cols = [r[1] for r in cur.fetchall()]
        if "acoustid_fingerprint" not in cols:
            cur.execute("ALTER TABLE audio_cache ADD COLUMN acoustid_fingerprint TEXT")
        if "acoustid_duration" not in cols:
            cur.execute("ALTER TABLE audio_cache ADD COLUMN acoustid_duration REAL")
    except sqlite3.OperationalError:
        pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS musicbrainz_cache (
            mbid       TEXT PRIMARY KEY,
            info_json  TEXT,
            created_at INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS musicbrainz_album_lookup (
            artist_norm TEXT NOT NULL,
            album_norm  TEXT NOT NULL,
            mbid        TEXT,
            info_json   TEXT,
            created_at  INTEGER,
            PRIMARY KEY (artist_norm, album_norm)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_album_lookup (
            provider    TEXT NOT NULL,
            artist_norm TEXT NOT NULL,
            album_norm  TEXT NOT NULL,
            status      TEXT NOT NULL,
            payload_json TEXT,
            created_at  INTEGER NOT NULL,
            expires_at  INTEGER NOT NULL,
            PRIMARY KEY (provider, artist_norm, album_norm)
        )
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audio_cache_path ON audio_cache(path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mb_cache_mbid ON musicbrainz_cache(mbid)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mb_album_lookup_key ON musicbrainz_album_lookup(artist_norm, album_norm)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_provider_album_lookup_key ON provider_album_lookup(provider, artist_norm, album_norm)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_provider_album_lookup_expires ON provider_album_lookup(expires_at)")
    except sqlite3.OperationalError:
        pass
    con.commit()
    con.close()


def get_cached_info(cache_db_file: str | Path, path: str, mtime: int) -> Optional[tuple[int, int, int]]:
    con = sqlite3.connect(str(cache_db_file), timeout=30)
    cur = con.cursor()
    cur.execute("SELECT bit_rate, sample_rate, bit_depth, mtime FROM audio_cache WHERE path = ?", (path,))
    row = cur.fetchone()
    con.close()
    if row:
        br, sr, bd, cached_mtime = row
        if cached_mtime == mtime:
            return (br, sr, bd)
    return None


def set_cached_info(
    cache_db_file: str | Path,
    path: str,
    mtime: int,
    bit_rate: int,
    sample_rate: int,
    bit_depth: int,
) -> None:
    con = sqlite3.connect(str(cache_db_file), timeout=30)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO audio_cache(path, mtime, bit_rate, sample_rate, bit_depth)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            mtime      = excluded.mtime,
            bit_rate   = excluded.bit_rate,
            sample_rate = excluded.sample_rate,
            bit_depth  = excluded.bit_depth
        """,
        (path, mtime, bit_rate, sample_rate, bit_depth),
    )
    con.commit()
    con.close()


def get_cached_acoustid(cache_db_file: str | Path, path: str) -> Optional[tuple[float, str]]:
    try:
        con = sqlite3.connect(str(cache_db_file), timeout=30)
        cur = con.cursor()
        cur.execute("SELECT acoustid_duration, acoustid_fingerprint FROM audio_cache WHERE path = ?", (path,))
        row = cur.fetchone()
        con.close()
    except Exception:
        return None
    if row and row[0] is not None and row[1]:
        return (float(row[0]), str(row[1]))
    return None


def set_cached_acoustid(cache_db_file: str | Path, path: str, duration: float, fingerprint: str) -> None:
    con = sqlite3.connect(str(cache_db_file), timeout=30)
    cur = con.cursor()
    cur.execute(
        "UPDATE audio_cache SET acoustid_fingerprint = ?, acoustid_duration = ? WHERE path = ?",
        (fingerprint, duration, path),
    )
    if cur.rowcount == 0:
        cur.execute(
            """
            INSERT INTO audio_cache(path, mtime, bit_rate, sample_rate, bit_depth, acoustid_fingerprint, acoustid_duration)
            VALUES (?, 0, 0, 0, 0, ?, ?)
            """,
            (path, fingerprint, duration),
        )
    con.commit()
    con.close()
