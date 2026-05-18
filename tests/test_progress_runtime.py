from __future__ import annotations

import pytest

from pmda_scan.progress_runtime import (
    cached_provider_gateway_snapshot,
    provider_gateway_live_stats,
    resume_availability_snapshot,
)


def test_cached_provider_gateway_snapshot_normalizes_missing_values():
    snapshot = cached_provider_gateway_snapshot(
        {
            "scan_provider_stats_live": {"Discogs": {"lookups": 3}},
            "provider_gateway_inflight": "2",
            "provider_gateway_max_inflight_observed": "7",
        }
    )

    assert snapshot == {
        "providers": {"Discogs": {"lookups": 3}},
        "inflight": 2,
        "max_inflight_observed": 7,
    }


def test_provider_gateway_live_stats_uses_loader_and_lowercases_provider_keys():
    payload = provider_gateway_live_stats(
        scanning=True,
        snapshot_loader=lambda _fallback: {
            "providers": {
                "Discogs": {"lookups": 4},
                " lastfm ": {"lookups": 5},
                "": {"lookups": 99},
            },
            "inflight": 1,
        },
    )

    assert payload["providers"] == {
        "discogs": {"lookups": 4},
        "lastfm": {"lookups": 5},
    }
    assert payload["gateway"]["inflight"] == 1


def test_provider_gateway_live_stats_falls_back_to_cached_payload_on_error():
    def failing_loader(_fallback):
        raise RuntimeError("gateway unavailable")

    payload = provider_gateway_live_stats(
        scanning=True,
        cached_payload={
            "scan_provider_stats_live": {"Bandcamp": {"matches": 8}},
            "provider_gateway_inflight": 3,
            "provider_gateway_max_inflight_observed": 6,
        },
        snapshot_loader=failing_loader,
    )

    assert payload["providers"] == {"bandcamp": {"matches": 8}}
    assert payload["gateway"]["inflight"] == 3
    assert payload["gateway"]["max_inflight_observed"] == 6


def test_provider_gateway_live_stats_avoids_loader_when_not_scanning():
    def should_not_run(_fallback):
        raise AssertionError("loader should not run")

    assert provider_gateway_live_stats(scanning=False, snapshot_loader=should_not_run) == {
        "providers": {},
        "gateway": {},
    }


def test_resume_availability_snapshot_uses_exact_signature_then_any_signature():
    calls: list[tuple[str, str, str]] = []

    def exact(mode: str, scan_type: str):
        calls.append(("exact", mode, scan_type))
        return {"available": scan_type == "changed_only", "remaining": 2}

    def any_signature(mode: str, scan_type: str):
        calls.append(("any", mode, scan_type))
        return {"available": scan_type == "full", "remaining": 9}

    payload = resume_availability_snapshot(
        scanning=False,
        library_mode_loader=lambda: "files",
        get_resume_run_snapshot=exact,
        get_latest_resume_run_snapshot_any_signature=any_signature,
    )

    assert payload["resume_available"] is True
    assert payload["resume_available_by_scan_type"]["full"]["remaining"] == 9
    assert payload["resume_available_by_scan_type"]["changed_only"]["remaining"] == 2
    assert calls == [
        ("exact", "files", "full"),
        ("any", "files", "full"),
        ("exact", "files", "changed_only"),
    ]


def test_resume_availability_snapshot_preserves_existing_payload_while_scanning():
    def should_not_run(*_args):
        raise AssertionError("resume snapshots should not be queried while scanning")

    payload = resume_availability_snapshot(
        scanning=True,
        library_mode_loader=should_not_run,
        get_resume_run_snapshot=should_not_run,
        get_latest_resume_run_snapshot_any_signature=should_not_run,
        existing_available=True,
        existing_by_scan_type={"full": {"available": True, "remaining": 1}},
    )

    assert payload == {
        "resume_available": True,
        "resume_available_by_scan_type": {"full": {"available": True, "remaining": 1}},
    }


def test_resume_availability_snapshot_rejects_unavailable_existing_snapshots():
    payload = resume_availability_snapshot(
        scanning=True,
        library_mode_loader=lambda: "files",
        get_resume_run_snapshot=lambda *_args: pytest.fail("unexpected"),
        get_latest_resume_run_snapshot_any_signature=lambda *_args: pytest.fail("unexpected"),
        existing_by_scan_type={"full": {"available": False, "remaining": 1}},
    )

    assert payload == {
        "resume_available": False,
        "resume_available_by_scan_type": {},
    }
