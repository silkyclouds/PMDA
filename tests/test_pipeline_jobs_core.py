from pmda_core import pipeline_jobs


def test_finished_at_for_status_tracks_terminal_states():
    assert pipeline_jobs.finished_at_for_status("running", now=10.0) is None
    assert pipeline_jobs.finished_at_for_status("completed", now=10.0) == 10.0
    assert pipeline_jobs.finished_at_for_status("running", now=10.0, finished=True) == 10.0


def test_row_to_status_marks_stale_running_jobs():
    converted = pipeline_jobs.row_to_status(
        {
            "job_type": "scan",
            "scope": "global",
            "run_id": "r1",
            "status": "running",
            "phase": "matching",
            "current": 5,
            "total": 10,
            "current_item": "Orbital",
            "message": "Matching",
            "error": "",
            "started_at": 1.0,
            "heartbeat_at": 10.0,
            "finished_at": None,
            "meta_json": '{"scan_type": "full"}',
        },
        now=1000.0,
        stale_after_sec=60,
    )
    assert converted is not None
    key, payload = converted
    assert key == "scan"
    assert payload["status"] == "stale"
    assert payload["stale"] is True
    assert payload["error"] == pipeline_jobs.STALE_ERROR
    assert payload["seconds_since_heartbeat"] == 990
    assert payload["meta"] == {"scan_type": "full"}


def test_row_to_status_keeps_explicit_error_on_stale_jobs():
    converted = pipeline_jobs.row_to_status(
        {
            "job_type": "profile_backfill",
            "scope": "library",
            "status": "running",
            "error": "worker failed",
            "heartbeat_at": 1.0,
            "meta_json": "not-json",
        },
        now=1000.0,
        stale_after_sec=60,
    )
    assert converted is not None
    key, payload = converted
    assert key == "profile_backfill:library"
    assert payload["error"] == "worker failed"
    assert payload["meta"] == {}


def test_normalizers_keep_pipeline_job_keys_stable():
    assert pipeline_jobs.normalize_job_type(" Scan ") == "scan"
    assert pipeline_jobs.normalize_scope("") == "global"
    assert pipeline_jobs.normalize_status("") == "running"
    assert pipeline_jobs.item_key("scan", "global") == "scan"
    assert pipeline_jobs.item_key("scan", "library") == "scan:library"


def test_running_scheduler_jobs_normalizes_and_sorts_meta():
    jobs = pipeline_jobs.running_scheduler_jobs(
        {
            "b": {
                "run_id": "run-b",
                "job_type": " Dedupe ",
                "scope": "library",
                "source": "SCAN",
                "origin_scan_id": "12",
                "started_at": 20.0,
            },
            "a": {
                "run_id": "run-a",
                "job_type": " enrich_batch ",
                "scope": "",
                "source": "manual",
                "origin_scan_id": "",
                "started_at": 10.0,
            },
            "ignored": "not-a-dict",
        }
    )

    assert [job["run_id"] for job in jobs] == ["run-a", "run-b"]
    assert jobs[0]["job_type"] == "enrich_batch"
    assert jobs[0]["scope"] == "both"
    assert jobs[0]["origin_scan_id"] is None
    assert jobs[1]["job_type"] == "dedupe"
    assert jobs[1]["origin_scan_id"] == 12


def test_background_enrichment_running_detects_active_jobs_and_backfill():
    assert pipeline_jobs.background_enrichment_running(
        background_jobs=[{"job_type": "dedupe"}],
        profile_backfill_state={},
        profile_jobs_active=0,
    )
    assert pipeline_jobs.background_enrichment_running(
        background_jobs=[],
        profile_backfill_state={"running": True},
        profile_jobs_active=0,
    )
    assert pipeline_jobs.background_enrichment_running(
        background_jobs=[],
        profile_backfill_state={},
        profile_jobs_active=1,
    )
    assert not pipeline_jobs.background_enrichment_running(
        background_jobs=[{"job_type": "other"}],
        profile_backfill_state={},
        profile_jobs_active=0,
    )
