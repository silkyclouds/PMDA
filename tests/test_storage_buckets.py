from __future__ import annotations

from pathlib import Path

import pytest

from pmda_discovery.storage_buckets import (
    access_path_for_canonical_path,
    build_storage_progress_payload,
    build_unraid_scan_roots,
    canonical_path_for_access_path,
    estimated_cost_saved_eur,
    estimated_watts_saved,
    plan_entry_for_canonical_path,
)


def test_build_unraid_scan_roots_maps_direct_disk_paths_to_canonical_roots(tmp_path):
    host_mnt = tmp_path / "host_mnt"
    user_music = host_mnt / "user" / "MURRAY" / "Music"
    disk1 = host_mnt / "disk1" / "MURRAY" / "Music" / "Music_dump"
    disk2 = host_mnt / "disk2" / "MURRAY" / "Music" / "Music_dump"
    user_music.mkdir(parents=True)
    disk1.mkdir(parents=True)
    disk2.mkdir(parents=True)

    roots, entries = build_unraid_scan_roots(
        ["/music/Music_dump"],
        settings={
            "enabled": True,
            "host_mnt_root": host_mnt,
            "user_share_host_root": user_music,
            "container_share_root": Path("/music"),
        },
    )

    assert roots == [disk1, disk2]
    assert [entry["storage_device_id"] for entry in entries] == ["disk1", "disk2"]
    assert canonical_path_for_access_path(disk1 / "Artist" / "Album", entries) == Path("/music/Music_dump/Artist/Album")
    assert access_path_for_canonical_path(Path("/music/Music_dump/Artist/Album"), "disk2", entries) == disk2 / "Artist" / "Album"


def test_build_unraid_scan_roots_refuses_missing_host_mount(tmp_path):
    with pytest.raises(RuntimeError, match="not mounted"):
        build_unraid_scan_roots(
            ["/music/Music_dump"],
            settings={
                "enabled": True,
                "host_mnt_root": tmp_path / "missing",
                "user_share_host_root": tmp_path / "missing" / "user" / "MURRAY" / "Music",
                "container_share_root": Path("/music"),
            },
        )


def test_plan_entry_for_canonical_path_prefers_longest_root():
    entry = plan_entry_for_canonical_path(
        "/music/Music_dump/Artist/Album",
        [
            {"storage_device_id": "disk1", "canonical_root": "/music"},
            {"storage_device_id": "disk2", "canonical_root": "/music/Music_dump"},
        ],
    )

    assert entry is not None
    assert entry["storage_device_id"] == "disk2"


def test_storage_savings_estimates_are_stable():
    assert estimated_watts_saved(active_devices=1, total_devices=21) == 140.0
    assert estimated_cost_saved_eur(watts_saved=140.0, seconds=3600) == 0.0364


def test_build_storage_progress_payload_can_omit_heavy_details():
    payload = build_storage_progress_payload(
        {
            "storage_power_saver_enabled": True,
            "storage_provider": "unraid",
            "storage_active_devices": "1",
            "storage_devices_total": "24",
            "storage_current_device_id": "disk7",
            "storage_bucket_done": "12",
            "storage_bucket_total": "100",
            "storage_buckets_done": "3",
            "storage_buckets_total": "24",
            "storage_estimated_watts_saved": "138.0",
            "storage_current_bucket": {"album": "heavy"},
            "storage_scan_plan": [{"disk": "disk7"}],
            "storage_bucket_history": [{"disk": "disk1"}],
        },
        include_details=False,
    )

    assert payload["storage_power_saver_enabled"] is True
    assert payload["storage_current_device_id"] == "disk7"
    assert payload["storage_bucket_done"] == 12
    assert payload["storage_estimated_watts_saved"] == 138.0
    assert payload["storage_current_bucket"] == {}
    assert payload["storage_scan_plan"] == []
    assert payload["storage_bucket_history"] == []


def test_build_storage_progress_payload_limits_history_and_copies_details():
    state = {
        "storage_current_bucket": {"disk": "disk2"},
        "storage_scan_plan": [{"disk": "disk1"}],
        "storage_bucket_history": [{"i": i} for i in range(105)],
    }
    payload = build_storage_progress_payload(state, include_details=True)

    assert payload["storage_current_bucket"] == {"disk": "disk2"}
    assert payload["storage_scan_plan"] == [{"disk": "disk1"}]
    assert len(payload["storage_bucket_history"]) == 100
    assert payload["storage_bucket_history"][0] == {"i": 5}
    payload["storage_current_bucket"]["disk"] = "changed"
    assert state["storage_current_bucket"]["disk"] == "disk2"
