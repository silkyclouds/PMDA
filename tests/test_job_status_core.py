from __future__ import annotations

from pmda_core.job_status import build_jobs_status_snapshot, job_record, percent


def test_job_record_normalizes_progress_and_payload_shape():
    row = job_record(
        "scan",
        job_type="scan",
        status="running",
        running=True,
        done="5",
        total="10",
        heartbeat_at="123.5",
        eta_seconds="42",
        blockers=["provider queue"],
    )

    assert row["job_id"] == "scan"
    assert row["running"] is True
    assert row["done"] == 5
    assert row["total"] == 10
    assert row["percent"] == 50.0
    assert row["heartbeat_at"] == 123.5
    assert row["eta_seconds"] == 42
    assert row["blockers"] == ["provider queue"]


def test_percent_handles_invalid_or_empty_totals():
    assert percent(1, 4) == 25.0
    assert percent(1, 0) is None
    assert percent("bad", 10) is None


def test_build_jobs_status_snapshot_reports_independent_jobs():
    snapshot = build_jobs_status_snapshot(
        now=1000.0,
        state={
            "scanning": True,
            "scan_phase": "matching",
            "scan_current_focus": "AFX - Selected Ambient Works",
            "scan_processed_albums_count": 7,
            "scan_total_albums": 20,
            "scan_published_albums_count": 3,
            "scan_blockers": ["MusicBrainz queue"],
            "export_progress": {"running": True, "albums_done": 2, "total_albums": 4},
            "files_reco_embeddings": {"running": True, "tracks_done": 11, "tracks_total": 20, "phase": "tracks"},
        },
        files_index={"running": True, "phase": "media_cache", "phase_item_done": 8, "phase_item_total": 10},
        profile_backfill={"running": True, "current": 2, "total": 9, "current_artist": "Aphex Twin"},
        metadata_summary={"running": 1, "completed": 5, "total": 10},
        runtime_snapshot={"musicbrainz": {"ready": True}},
        latest_runtime_actions=[{"status": "running", "updated_at": 999.0}],
        published_rows=100,
        media_cache_root="/config/media_cache",
        storage_current={"enabled": True, "active_devices": 1, "current_device_id": "disk1", "bucket_done": 3, "bucket_total": 12},
        allow_plex_db_in_files_mode=False,
    )

    assert snapshot["jobs"]["scan"]["running"] is True
    assert snapshot["jobs"]["scan"]["phase"] == "matching"
    assert snapshot["jobs"]["publication"]["running"] is True
    assert snapshot["jobs"]["materialization"]["phase"] == "export"
    assert snapshot["jobs"]["library_index"]["running"] is True
    assert snapshot["jobs"]["media_cache"]["running"] is True
    assert snapshot["jobs"]["profile_backfill"]["last_item"] == "Aphex Twin"
    assert snapshot["jobs"]["embeddings"]["percent"] == 55.0
    assert snapshot["jobs"]["metadata_workers"]["running"] is True
    assert snapshot["jobs"]["runtime_repair"]["running"] is True
    assert snapshot["jobs"]["storage"]["phase"] == "disk1"
    assert set(snapshot["running"]) >= {"scan", "publication", "materialization", "library_index", "media_cache"}
    assert snapshot["notes"]["files_mode_opens_plex_db"] is False
