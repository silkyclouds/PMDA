import tempfile
import unittest
from unittest import mock
from pathlib import Path

import pmda


class StoragePowerSaverTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-storage-")
        self.root = Path(self._tmp.name)
        self.host_mnt = self.root / "host_mnt"
        self.user_music = self.host_mnt / "user" / "MURRAY" / "Music"
        self.disk1_dump = self.host_mnt / "disk1" / "MURRAY" / "Music" / "Music_dump"
        self.disk2_dump = self.host_mnt / "disk2" / "MURRAY" / "Music" / "Music_dump"
        self.user_music.mkdir(parents=True)
        self.disk1_dump.mkdir(parents=True)
        self.disk2_dump.mkdir(parents=True)
        self.settings = {
            "STORAGE_POWER_SAVER_ENABLED": True,
            "STORAGE_PROVIDER": "unraid",
            "UNRAID_HOST_MNT_ROOT": str(self.host_mnt),
            "UNRAID_USER_SHARE_HOST_ROOT": str(self.user_music),
            "UNRAID_CONTAINER_SHARE_ROOT": "/music",
            "STORAGE_MAX_ACTIVE_DEVICES": 1,
            "STORAGE_SPINDOWN_POLICY": "none",
        }
        with pmda.lock:
            self._prev_scan_state = {
                "scanning": pmda.state.get("scanning"),
                "scan_starting": pmda.state.get("scan_starting"),
                "scan_finalizing": pmda.state.get("scan_finalizing"),
                "scan_discovery_running": pmda.state.get("scan_discovery_running"),
            }

    def tearDown(self):
        with pmda.lock:
            pmda.state["storage_power_saver_enabled"] = False
            pmda.state["storage_scan_plan"] = []
            pmda.state["storage_bucket_history"] = []
            pmda.state["storage_current_bucket"] = None
            pmda.state["storage_validation_error"] = ""
            pmda.state["scanning"] = self._prev_scan_state["scanning"]
            pmda.state["scan_starting"] = self._prev_scan_state["scan_starting"]
            pmda.state["scan_finalizing"] = self._prev_scan_state["scan_finalizing"]
            pmda.state["scan_discovery_running"] = self._prev_scan_state["scan_discovery_running"]
        self._tmp.cleanup()

    def test_unraid_scan_roots_are_built_per_disk_with_canonical_mapping(self):
        roots, entries = pmda._storage_unraid_build_scan_roots(
            [Path("/music/Music_dump")],
            settings_snapshot=self.settings,
        )

        self.assertEqual([path.name for path in roots], ["Music_dump", "Music_dump"])
        self.assertEqual([entry["storage_device_id"] for entry in entries], ["disk1", "disk2"])
        self.assertEqual(str(roots[0]), str(self.disk1_dump))
        self.assertEqual(entries[0]["canonical_root"], "/music/Music_dump")

        access_album = self.disk1_dump / "Artist" / "Album"
        canonical_album = pmda._storage_canonical_path_for_access_path(access_album, entries)
        self.assertEqual(str(canonical_album), "/music/Music_dump/Artist/Album")

        access_again = pmda._storage_access_path_for_canonical_path(
            Path("/music/Music_dump/Artist/Album"),
            "disk2",
            entries,
        )
        self.assertEqual(str(access_again), str(self.disk2_dump / "Artist" / "Album"))

    def test_unraid_scan_roots_refuse_missing_host_mount(self):
        broken = dict(self.settings)
        broken["UNRAID_HOST_MNT_ROOT"] = str(self.root / "missing_host_mnt")

        with self.assertRaisesRegex(RuntimeError, "not mounted"):
            pmda._storage_unraid_build_scan_roots(
                [Path("/music/Music_dump")],
                settings_snapshot=broken,
            )

    def test_progress_payload_exposes_storage_state(self):
        with pmda.lock:
            pmda.state["storage_power_saver_enabled"] = True
            pmda.state["storage_provider"] = "unraid"
            pmda.state["storage_active_devices"] = 1
            pmda.state["storage_devices_total"] = 24
            pmda.state["storage_current_device_id"] = "disk7"
            pmda.state["storage_current_device_label"] = "disk7"
            pmda.state["storage_bucket_done"] = 12
            pmda.state["storage_bucket_total"] = 100
            pmda.state["storage_buckets_done"] = 3
            pmda.state["storage_buckets_total"] = 24
            pmda.state["storage_estimated_watts_saved"] = 138.0

        payload = pmda._storage_progress_payload()

        self.assertTrue(payload["storage_power_saver_enabled"])
        self.assertEqual(payload["storage_current_device_id"], "disk7")
        self.assertEqual(payload["storage_bucket_done"], 12)
        self.assertEqual(payload["storage_buckets_total"], 24)
        self.assertEqual(payload["storage_estimated_watts_saved"], 138.0)

    def test_profile_backfill_scope_uses_current_scan_device_when_budget_is_one(self):
        with pmda.lock:
            pmda.state["scanning"] = True
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_discovery_running"] = False
            pmda.state["storage_current_device_id"] = "disk2"
            pmda.state["storage_current_device_label"] = "disk2"
            pmda.state["storage_scan_plan"] = [
                {
                    "storage_device_id": "disk2",
                    "storage_device_label": "disk2",
                    "storage_bucket_order": 1,
                    "canonical_root": "/music/Music_dump",
                    "status": "running",
                },
                {
                    "storage_device_id": "disk3",
                    "storage_device_label": "disk3",
                    "storage_bucket_order": 2,
                    "canonical_root": "/music/Music_dump",
                    "status": "pending",
                },
            ]
        scope = pmda._storage_profile_backfill_scope(settings_snapshot=self.settings)
        self.assertTrue(scope["enabled"])
        self.assertTrue(scope["scan_active"])
        self.assertEqual(scope["allowed_device_ids"], ["disk2"])

    def test_profile_backfill_scope_can_add_second_device_when_budget_allows_it(self):
        wide_settings = dict(self.settings)
        wide_settings["STORAGE_MAX_ACTIVE_DEVICES"] = 2
        with pmda.lock:
            pmda.state["scanning"] = True
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_discovery_running"] = False
            pmda.state["storage_current_device_id"] = "disk2"
            pmda.state["storage_current_device_label"] = "disk2"
            pmda.state["storage_scan_plan"] = [
                {
                    "storage_device_id": "disk2",
                    "storage_device_label": "disk2",
                    "storage_bucket_order": 1,
                    "canonical_root": "/music/Music_dump",
                    "status": "running",
                },
                {
                    "storage_device_id": "disk3",
                    "storage_device_label": "disk3",
                    "storage_bucket_order": 2,
                    "canonical_root": "/music/Music_dump",
                    "status": "pending",
                },
            ]
        scope = pmda._storage_profile_backfill_scope(settings_snapshot=wide_settings)
        self.assertEqual(scope["allowed_device_ids"], ["disk2", "disk3"])

    def test_profile_backfill_scope_waits_for_current_device_during_active_scan(self):
        wide_settings = dict(self.settings)
        wide_settings["STORAGE_MAX_ACTIVE_DEVICES"] = 2
        with pmda.lock:
            pmda.state["scanning"] = True
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
            pmda.state["scan_discovery_running"] = True
            pmda.state["storage_current_device_id"] = None
            pmda.state["storage_current_device_label"] = None
            pmda.state["storage_scan_plan"] = [
                {
                    "storage_device_id": "disk2",
                    "storage_device_label": "disk2",
                    "storage_bucket_order": 1,
                    "canonical_root": "/music/Music_dump",
                    "status": "pending",
                },
                {
                    "storage_device_id": "disk3",
                    "storage_device_label": "disk3",
                    "storage_bucket_order": 2,
                    "canonical_root": "/music/Music_dump",
                    "status": "pending",
                },
            ]
        scope = pmda._storage_profile_backfill_scope(settings_snapshot=wide_settings)
        self.assertTrue(scope["enabled"])
        self.assertTrue(scope["scan_active"])
        self.assertEqual(scope["allowed_device_ids"], [])
        self.assertEqual(scope["mode"], "scan_waiting_for_current_device")

    def test_storage_plan_entry_for_canonical_path_matches_longest_root(self):
        entries = [
            {"storage_device_id": "disk1", "canonical_root": "/music"},
            {"storage_device_id": "disk2", "canonical_root": "/music/Music_dump"},
        ]
        entry = pmda._storage_plan_entry_for_canonical_path("/music/Music_dump/Artist/Album", entries)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["storage_device_id"], "disk2")

    def test_background_io_scope_allows_only_current_storage_device(self):
        scope = {
            "enabled": True,
            "scan_active": True,
            "allowed_device_ids": ["disk1"],
            "plan_entries": [
                {"storage_device_id": "disk1", "canonical_root": "/music/Music_dump/disk1"},
                {"storage_device_id": "disk2", "canonical_root": "/music/Music_dump/disk2"},
            ],
        }

        self.assertTrue(pmda._storage_path_allowed_for_background_io("/music/Music_dump/disk1/Artist/Album", scope))
        self.assertFalse(pmda._storage_path_allowed_for_background_io("/music/Music_dump/disk2/Artist/Album", scope))

    def test_precache_media_assets_skips_out_of_scope_source_paths(self):
        allowed = self.root / "allowed" / "cover.jpg"
        blocked = self.root / "blocked" / "cover.jpg"
        allowed.parent.mkdir(parents=True)
        blocked.parent.mkdir(parents=True)
        allowed.write_bytes(b"allowed")
        blocked.write_bytes(b"blocked")
        scope = {
            "enabled": True,
            "scan_active": True,
            "allowed_device_ids": ["disk1"],
            "plan_entries": [
                {"storage_device_id": "disk1", "canonical_root": str(allowed.parent)},
                {"storage_device_id": "disk2", "canonical_root": str(blocked.parent)},
            ],
        }

        with mock.patch.object(pmda, "_storage_background_filesystem_scope", return_value=scope), mock.patch.object(
            pmda,
            "_ensure_cached_image_for_path",
            return_value=None,
        ) as ensure_cached:
            pmda._precache_files_media_assets(
                {},
                [{"cover_path": str(allowed)}, {"cover_path": str(blocked)}],
                include_artist_images=False,
            )

        self.assertEqual(ensure_cached.call_count, len(pmda._MEDIA_CACHE_SIZES))
        self.assertTrue(all(str(call.args[0]) == str(allowed) for call in ensure_cached.call_args_list))

    def test_promote_media_paths_preserves_out_of_scope_cover_without_touching_disk(self):
        blocked_folder = self.root / "blocked" / "Album"
        blocked_cover = blocked_folder / "cover.jpg"
        blocked_folder.mkdir(parents=True)
        blocked_cover.write_bytes(b"blocked")
        scope = {
            "enabled": True,
            "scan_active": True,
            "allowed_device_ids": ["disk1"],
            "plan_entries": [
                {"storage_device_id": "disk2", "canonical_root": str(blocked_folder)},
            ],
        }
        album = {
            "artist_norm": "artist",
            "folder_path": str(blocked_folder),
            "cover_path": str(blocked_cover),
            "has_cover": True,
        }

        with mock.patch.object(pmda, "_storage_background_filesystem_scope", return_value=scope), mock.patch.object(
            pmda,
            "_ensure_cached_image_for_path",
            return_value=None,
        ) as ensure_cached:
            promoted = pmda._promote_files_media_paths_to_cache({}, [album])

        self.assertEqual(promoted, (0, 0))
        self.assertEqual(album["cover_path"], str(blocked_cover))
        self.assertTrue(album["has_cover"])
        ensure_cached.assert_not_called()

    def test_materialization_meta_uses_disk_plan_for_folder_ordering(self):
        plan = [
            {"storage_device_id": "disk1", "storage_bucket_order": 1, "canonical_root": "/music/Music_dump/A"},
            {"storage_device_id": "disk2", "storage_bucket_order": 2, "canonical_root": "/music/Music_dump/B"},
        ]

        meta = pmda._storage_materialization_meta_for_folder("/music/Music_dump/B/Artist/Album", plan_entries=plan)

        self.assertEqual(meta["storage_device_id"], "disk2")
        self.assertEqual(meta["storage_bucket_order"], 2)

    def test_materialization_groups_are_sorted_by_storage_bucket_then_artist(self):
        grouped = {
            "Zulu": [{"album_id": 2, "artist_name": "Zulu", "album_title": "B", "storage_bucket_order": 2, "storage_device_id": "disk2"}],
            "Alpha": [{"album_id": 1, "artist_name": "Alpha", "album_title": "A", "storage_bucket_order": 1, "storage_device_id": "disk1"}],
        }

        ordered = pmda._storage_ordered_materialization_groups(grouped)

        self.assertEqual([(row[0], row[1], row[2]) for row in ordered], [(1, "disk1", "Alpha"), (2, "disk2", "Zulu")])


if __name__ == "__main__":
    unittest.main()
