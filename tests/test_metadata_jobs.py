from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from pmda_enrichment.metadata_jobs import enqueue_metadata_job, metadata_jobs_summary


def _connect_factory(path: Path):
    def _connect(*, timeout: int = 10):
        return sqlite3.connect(path, timeout=timeout)

    return _connect


def _init_schema(path: Path) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute(
            """
            CREATE TABLE metadata_jobs (
                job_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL,
                queue_name TEXT NOT NULL,
                scope TEXT NOT NULL,
                album_manifest_json TEXT NOT NULL,
                provider_hints_json TEXT NOT NULL,
                cache_keys_json TEXT NOT NULL,
                run_id TEXT,
                scan_id INTEGER,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        con.commit()
    finally:
        con.close()


def test_enqueue_metadata_job_persists_normalized_payload(tmp_path: Path):
    db_path = tmp_path / "state.db"
    _init_schema(db_path)

    job_id = enqueue_metadata_job(
        _connect_factory(db_path),
        {"artist": "Aphex Twin", "album": "Selected Ambient Works"},
        provider_hints={"musicbrainz": "strict"},
        cache_keys=["a", "b"],
        priority=1500,
        queue_name="profiles",
        scope="album",
        run_id="run-1",
        scan_id=42,
        job_id="job-1",
        now=123.0,
    )

    assert job_id == "job-1"
    con = sqlite3.connect(db_path)
    try:
        row = con.execute("SELECT * FROM metadata_jobs WHERE job_id='job-1'").fetchone()
    finally:
        con.close()
    assert row is not None
    assert row[1] == "queued"
    assert row[2] == 1000
    assert row[3] == "profiles"
    assert json.loads(row[5])["artist"] == "Aphex Twin"
    assert json.loads(row[6])["musicbrainz"] == "strict"
    assert json.loads(row[7]) == ["a", "b"]
    assert row[8] == "run-1"
    assert row[9] == 42
    assert row[10] == 123.0


def test_metadata_jobs_summary_counts_statuses(tmp_path: Path):
    db_path = tmp_path / "state.db"
    _init_schema(db_path)
    connect = _connect_factory(db_path)
    enqueue_metadata_job(connect, {"album": "one"}, job_id="queued-1", now=10.0)
    enqueue_metadata_job(connect, {"album": "two"}, job_id="queued-2", now=20.0)
    con = sqlite3.connect(db_path)
    try:
        con.execute("UPDATE metadata_jobs SET status='running' WHERE job_id='queued-2'")
        con.execute(
            """
            INSERT INTO metadata_jobs (
                job_id, status, priority, queue_name, scope,
                album_manifest_json, provider_hints_json, cache_keys_json,
                run_id, scan_id, created_at, updated_at
            ) VALUES ('done-1', 'completed', 50, 'metadata', 'album', '{}', '{}', '[]', '', NULL, 30.0, 30.0)
            """
        )
        con.commit()
    finally:
        con.close()

    summary = metadata_jobs_summary(connect)

    assert summary["queued"] == 1
    assert summary["running"] == 1
    assert summary["completed"] == 1
    assert summary["failed"] == 0
    assert summary["total"] == 3
    assert summary["oldest_queued_at"] == 10.0
