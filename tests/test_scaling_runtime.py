import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlparse

import pmda


class ScalingRuntimeTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-scaling-")
        tmp_path = Path(self._tmp.name)
        self._orig = {
            "CONFIG_DIR": pmda.CONFIG_DIR,
            "STATE_DB_FILE": pmda.STATE_DB_FILE,
            "SETTINGS_DB_FILE": pmda.SETTINGS_DB_FILE,
            "CACHE_DB_FILE": pmda.CACHE_DB_FILE,
            "AUTH_DISABLE": pmda.AUTH_DISABLE,
            "MUSICBRAINZ_MIRROR_ENABLED": pmda.MUSICBRAINZ_MIRROR_ENABLED,
            "MUSICBRAINZ_BASE_URL": pmda.MUSICBRAINZ_BASE_URL,
            "MUSICBRAINZ_MIRROR_NAME": pmda.MUSICBRAINZ_MIRROR_NAME,
            "PROVIDER_GATEWAY_ENABLED": pmda.PROVIDER_GATEWAY_ENABLED,
            "PROVIDER_GATEWAY_CACHE_ENABLED": pmda.PROVIDER_GATEWAY_CACHE_ENABLED,
            "PROVIDER_GATEWAY_MAX_INFLIGHT": pmda.PROVIDER_GATEWAY_MAX_INFLIGHT,
            "PROVIDER_GATEWAY_DISCOGS_RPM": pmda.PROVIDER_GATEWAY_DISCOGS_RPM,
            "PROVIDER_GATEWAY_LASTFM_RPM": pmda.PROVIDER_GATEWAY_LASTFM_RPM,
            "PROVIDER_GATEWAY_BANDCAMP_RPM": pmda.PROVIDER_GATEWAY_BANDCAMP_RPM,
            "AUTO_TUNE_ENABLED": pmda.AUTO_TUNE_ENABLED,
            "AUTO_TUNE_INTERVAL_SEC": pmda.AUTO_TUNE_INTERVAL_SEC,
            "AUTO_TUNE_MB_MIRROR_MIN_RPS": pmda.AUTO_TUNE_MB_MIRROR_MIN_RPS,
            "AUTO_TUNE_MB_MIRROR_MAX_RPS": pmda.AUTO_TUNE_MB_MIRROR_MAX_RPS,
            "AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN": pmda.AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN,
            "AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP": pmda.AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP,
            "METADATA_QUEUE_ENABLED": pmda.METADATA_QUEUE_ENABLED,
            "METADATA_WORKER_MODE": pmda.METADATA_WORKER_MODE,
            "METADATA_WORKER_COUNT": pmda.METADATA_WORKER_COUNT,
            "METADATA_JOB_BATCH_SIZE": pmda.METADATA_JOB_BATCH_SIZE,
        }
        pmda.CONFIG_DIR = tmp_path
        pmda.STATE_DB_FILE = tmp_path / "state.db"
        pmda.SETTINGS_DB_FILE = tmp_path / "settings.db"
        pmda.CACHE_DB_FILE = tmp_path / "cache.db"
        pmda.AUTH_DISABLE = True
        pmda.init_state_db()
        pmda.init_settings_db()
        pmda.init_cache_db()
        self.client = pmda.app.test_client()
        with pmda._PROVIDER_GATEWAY_LOCK:
            pmda._PROVIDER_GATEWAY_CACHE.clear()
            pmda._PROVIDER_GATEWAY_ERROR_CACHE.clear()
            pmda._PROVIDER_GATEWAY_INFLIGHT_REQUESTS.clear()
            pmda._PROVIDER_GATEWAY_BUCKETS.clear()
            pmda._PROVIDER_GATEWAY_STATS["providers"] = {}
            pmda._PROVIDER_GATEWAY_STATS["max_inflight_observed"] = 0

    def tearDown(self):
        for key, value in self._orig.items():
            setattr(pmda, key, value)
        pmda._configure_musicbrainz_client()
        pmda._provider_gateway_reconfigure()
        with pmda._PROVIDER_GATEWAY_LOCK:
            pmda._PROVIDER_GATEWAY_CACHE.clear()
            pmda._PROVIDER_GATEWAY_ERROR_CACHE.clear()
            pmda._PROVIDER_GATEWAY_INFLIGHT_REQUESTS.clear()
            pmda._PROVIDER_GATEWAY_BUCKETS.clear()
            pmda._PROVIDER_GATEWAY_STATS["providers"] = {}
            pmda._PROVIDER_GATEWAY_STATS["max_inflight_observed"] = 0
        self._tmp.cleanup()

    def test_musicbrainz_target_settings_public_default(self):
        pmda.MUSICBRAINZ_MIRROR_ENABLED = False
        pmda.MUSICBRAINZ_BASE_URL = ""
        pmda.MUSICBRAINZ_MIRROR_NAME = ""
        target = pmda._musicbrainz_target_settings()
        self.assertFalse(target["enabled"])
        self.assertEqual(target["base_url"], "https://musicbrainz.org")
        self.assertEqual(target["hostname"], "musicbrainz.org")
        self.assertTrue(target["use_https"])

    def test_musicbrainz_target_settings_mirror_https(self):
        pmda.MUSICBRAINZ_MIRROR_ENABLED = True
        pmda.MUSICBRAINZ_BASE_URL = "http://mb.internal:5000"
        pmda.MUSICBRAINZ_MIRROR_NAME = "LAN MB"
        with mock.patch.object(pmda, "_managed_runtime_health_check_musicbrainz", return_value={"available": True, "message": "ok"}):
            target = pmda._musicbrainz_target_settings()
        self.assertTrue(target["enabled"])
        self.assertEqual(target["base_url"], "http://mb.internal:5000")
        self.assertEqual(target["hostname"], "mb.internal:5000")
        self.assertFalse(target["use_https"])
        self.assertEqual(target["mirror_name"], "LAN MB")
        self.assertFalse(target["fallback_to_public"])

    def test_musicbrainz_target_settings_falls_back_to_public_when_mirror_unhealthy(self):
        pmda.MUSICBRAINZ_MIRROR_ENABLED = True
        pmda.MUSICBRAINZ_BASE_URL = "http://mb.internal:5000"
        pmda.MUSICBRAINZ_MIRROR_NAME = "LAN MB"
        with mock.patch.object(pmda, "_managed_runtime_health_check_musicbrainz", return_value={"available": False, "message": "mirror unhealthy"}):
            target = pmda._musicbrainz_target_settings()
        self.assertFalse(target["enabled"])
        self.assertTrue(target["configured_enabled"])
        self.assertTrue(target["fallback_to_public"])
        self.assertEqual(target["base_url"], "https://musicbrainz.org")
        self.assertEqual(target["fallback_reason"], "mirror unhealthy")

    def test_provider_gateway_http_get_uses_cache(self):
        pmda.PROVIDER_GATEWAY_ENABLED = True
        pmda.PROVIDER_GATEWAY_CACHE_ENABLED = True
        pmda.PROVIDER_GATEWAY_LASTFM_RPM = 120
        pmda._provider_gateway_reconfigure()

        fake_response = mock.Mock()
        fake_response.status_code = 200
        fake_response.text = '{"artist":{"name":"Orbital"}}'
        fake_response.headers = {"content-type": "application/json"}
        fake_response.url = "https://ws.audioscrobbler.com/2.0/"

        with mock.patch.object(pmda.requests, "get", return_value=fake_response) as req_get:
            first = pmda._provider_gateway_http_get(
                "lastfm",
                "https://ws.audioscrobbler.com/2.0/",
                params={"method": "artist.getInfo", "artist": "Orbital"},
                cache_ttl_sec=3600,
                context="test lastfm cache",
            )
            second = pmda._provider_gateway_http_get(
                "lastfm",
                "https://ws.audioscrobbler.com/2.0/",
                params={"method": "artist.getInfo", "artist": "Orbital"},
                cache_ttl_sec=3600,
                context="test lastfm cache",
            )

        self.assertEqual(req_get.call_count, 1)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        stats = pmda._provider_gateway_stats_snapshot()["providers"]["lastfm"]
        self.assertEqual(stats["request_count"], 2)
        self.assertEqual(stats["network_request_count"], 1)
        self.assertEqual(stats["cache_hits"], 1)

    def test_provider_gateway_http_get_negative_cache_uses_cached_404(self):
        pmda.PROVIDER_GATEWAY_ENABLED = True
        pmda.PROVIDER_GATEWAY_CACHE_ENABLED = True
        pmda._provider_gateway_reconfigure()

        fake_response = mock.Mock()
        fake_response.status_code = 404
        fake_response.text = ""
        fake_response.headers = {}
        fake_response.url = "https://example.invalid/not-found"

        with mock.patch.object(pmda.requests, "get", return_value=fake_response) as req_get:
            first = pmda._provider_gateway_http_get(
                "bandcamp",
                "https://example.invalid/not-found",
                cache_ttl_sec=3600,
                context="negative cache test",
            )
            second = pmda._provider_gateway_http_get(
                "bandcamp",
                "https://example.invalid/not-found",
                cache_ttl_sec=3600,
                context="negative cache test",
            )

        self.assertEqual(req_get.call_count, 1)
        self.assertEqual(first.status_code, 404)
        self.assertEqual(second.status_code, 404)
        stats = pmda._provider_gateway_stats_snapshot()["providers"]["bandcamp"]
        self.assertEqual(stats["negative_cache_hits"], 1)

    def test_fetch_provider_album_lookup_cached_coalesces_identical_fetches(self):
        calls = {"count": 0}
        call_lock = threading.Lock()

        def fetcher(artist_name: str, album_title: str):
            with call_lock:
                calls["count"] += 1
            time.sleep(0.2)
            return {"artist": artist_name, "album": album_title}

        results: list[dict | None] = [None, None]

        def worker(slot: int) -> None:
            results[slot] = pmda.fetch_provider_album_lookup_cached("discogs", "Orbital", "In Sides", fetcher)

        threads = [threading.Thread(target=worker, args=(idx,)) for idx in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5.0)

        self.assertEqual(calls["count"], 1)
        self.assertEqual(results[0], results[1])
        stats = pmda._provider_gateway_stats_snapshot()["providers"]["discogs"]
        self.assertEqual(stats["lookup_request_count"], 2)
        self.assertEqual(stats["lookup_network_request_count"], 1)
        self.assertEqual(stats["lookup_saved_count"], 1)
        self.assertEqual(stats["lookup_coalesced_waits"], 1)

    def test_fetch_provider_album_lookup_cached_reports_lookup_cache_hit_rate(self):
        calls = {"count": 0}

        def fetcher(artist_name: str, album_title: str):
            calls["count"] += 1
            return {"artist": artist_name, "album": album_title}

        first = pmda.fetch_provider_album_lookup_cached("lastfm", "Orbital", "In Sides", fetcher)
        second = pmda.fetch_provider_album_lookup_cached("lastfm", "Orbital", "In Sides", fetcher)

        self.assertEqual(calls["count"], 1)
        self.assertEqual(first, second)
        stats = pmda._provider_gateway_stats_snapshot()["providers"]["lastfm"]
        self.assertEqual(stats["lookup_request_count"], 2)
        self.assertEqual(stats["lookup_network_request_count"], 1)
        self.assertEqual(stats["lookup_saved_count"], 1)
        self.assertEqual(stats["lookup_cache_hits"], 1)
        self.assertEqual(stats["lookup_hit_rate"], 50.0)

    def test_provider_track_titles_cached_cache_only_does_not_fetch(self):
        with mock.patch.object(pmda, "_files_cache_get_json", return_value=None), \
             mock.patch.object(pmda, "_strict_payload_for_provider", side_effect=AssertionError("should not fetch")):
            titles = pmda._provider_track_titles_cached(
                artist_name="Orbital",
                album_title="In Sides",
                metadata_source="discogs",
                cache_only=True,
            )
        self.assertEqual(titles, [])

    def test_auto_artwork_ram_target_caps_unlimited_container_by_default(self):
        original_cap = pmda.ARTWORK_RAM_CACHE_AUTO_MAX_MB
        original_mb = pmda.ARTWORK_RAM_CACHE_MB
        try:
            pmda.ARTWORK_RAM_CACHE_AUTO_MAX_MB = 0
            pmda.ARTWORK_RAM_CACHE_MB = 1024
            with mock.patch.object(
                pmda,
                "_read_container_memory_stats",
                return_value={"current_bytes": 0, "limit_bytes": 0, "used_pct": None},
            ), mock.patch.object(pmda, "_read_host_mem_available_bytes", return_value=64 * 1024 * 1024 * 1024):
                target_mb = pmda._compute_auto_artwork_ram_target_mb()
            self.assertEqual(target_mb, 4096)
        finally:
            pmda.ARTWORK_RAM_CACHE_AUTO_MAX_MB = original_cap
            pmda.ARTWORK_RAM_CACHE_MB = original_mb

    def test_auto_artwork_ram_target_honors_explicit_cap_even_when_unlimited(self):
        original_cap = pmda.ARTWORK_RAM_CACHE_AUTO_MAX_MB
        original_mb = pmda.ARTWORK_RAM_CACHE_MB
        try:
            pmda.ARTWORK_RAM_CACHE_AUTO_MAX_MB = 2048
            pmda.ARTWORK_RAM_CACHE_MB = 1024
            with mock.patch.object(
                pmda,
                "_read_container_memory_stats",
                return_value={"current_bytes": 0, "limit_bytes": 0, "used_pct": None},
            ), mock.patch.object(pmda, "_read_host_mem_available_bytes", return_value=64 * 1024 * 1024 * 1024):
                target_mb = pmda._compute_auto_artwork_ram_target_mb()
            self.assertEqual(target_mb, 2048)
        finally:
            pmda.ARTWORK_RAM_CACHE_AUTO_MAX_MB = original_cap
            pmda.ARTWORK_RAM_CACHE_MB = original_mb

    def test_display_tracks_with_provider_overlay_forwards_cache_only(self):
        with mock.patch.object(pmda, "_provider_track_titles_cached", return_value=["Petrol"]) as mocked:
            tracks = pmda._display_tracks_with_provider_overlay(
                [{"track_id": 1, "title": "01 - Petrol", "disc_num": 1, "track_num": 1}],
                artist_name="Orbital",
                album_title="In Sides",
                metadata_source="discogs",
                cache_only=True,
            )
        self.assertEqual(tracks[0]["title"], "Petrol")
        self.assertTrue(bool(mocked.call_args.kwargs.get("cache_only")))

    def test_init_state_db_creates_metadata_job_tables(self):
        con = sqlite3.connect(pmda.STATE_DB_FILE)
        cur = con.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('metadata_jobs', 'metadata_job_attempts')"
        )
        names = {row[0] for row in cur.fetchall()}
        con.close()
        self.assertEqual(names, {"metadata_jobs", "metadata_job_attempts"})

    def test_init_settings_db_creates_managed_runtime_tables(self):
        con = sqlite3.connect(pmda.SETTINGS_DB_FILE)
        cur = con.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('managed_runtime_bundles', 'managed_runtime_logs', 'managed_runtime_actions')"
        )
        names = {row[0] for row in cur.fetchall()}
        con.close()
        self.assertEqual(names, {"managed_runtime_bundles", "managed_runtime_logs", "managed_runtime_actions"})

    def test_managed_runtime_status_endpoint_reports_bundles(self):
        with mock.patch.object(pmda, "_managed_runtime_preflight", return_value={
            "available": True,
            "docker_socket": "/var/run/docker.sock",
            "docker_socket_present": True,
            "docker_cli": "/usr/bin/docker",
            "compose_cli": "/usr/bin/docker compose",
            "git_cli": "/usr/bin/git",
            "docker_ok": True,
            "compose_ok": True,
            "git_ok": True,
            "message": "ready",
            "self_container": "pmda",
            "gpu_probe": {"available": True, "recommended_mode": "nvidia", "available_modes": ["nvidia"], "message": "NVIDIA ready"},
        }), mock.patch.object(pmda, "_managed_runtime_bundle_status", side_effect=[
            {
                "bundle_type": "musicbrainz_local",
                "mode": "managed",
                "state": "ready",
                "phase": "ready",
                "phase_message": "MusicBrainz ready",
                "health": {"available": True, "overall_status": "healthy", "url": "http://pmda-mb:5500"},
                "services": [{"name": "musicbrainz", "status": "healthy"}],
            },
            {
                "bundle_type": "ollama_local",
                "mode": "managed",
                "state": "ready",
                "phase": "ready",
                "phase_message": "Ollama ready",
                "health": {"available": True, "overall_status": "healthy", "models": ["qwen3:4b"]},
                "services": [{"name": "ollama", "status": "healthy"}],
            },
        ]):
            resp = self.client.get("/api/runtime/managed/status")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertTrue(payload["preflight"]["available"])
        self.assertTrue(payload["ready"])
        self.assertEqual(payload["bundles"]["musicbrainz_local"]["state"], "ready")
        self.assertEqual(payload["bundles"]["ollama_local"]["state"], "ready")
        self.assertEqual(payload["preflight"]["gpu_probe"]["recommended_mode"], "nvidia")

    def test_managed_runtime_ollama_gpu_profile_uses_nvidia_when_available(self):
        with mock.patch.object(pmda, "_managed_runtime_gpu_probe", return_value={
            "available": True,
            "recommended_mode": "nvidia",
            "available_modes": ["nvidia"],
            "nvidia_devices": ["/dev/nvidia0", "/dev/nvidiactl"],
            "dri_devices": [],
            "render_devices": [],
            "card_devices": [],
            "kfd_present": False,
            "message": "NVIDIA ready",
        }):
            profile = pmda._managed_runtime_ollama_gpu_profile()
        self.assertEqual(profile["selected_mode"], "nvidia")
        self.assertTrue(profile["active"])
        self.assertIn("--gpus", profile["docker_args"])
        self.assertEqual(profile["env"]["NVIDIA_VISIBLE_DEVICES"], "all")

    def test_managed_runtime_ollama_gpu_profile_uses_intel_vulkan_when_render_only(self):
        with mock.patch.object(pmda, "_managed_runtime_gpu_probe", return_value={
            "available": True,
            "recommended_mode": "vulkan_intel",
            "available_modes": ["vulkan_intel", "vulkan"],
            "nvidia_devices": [],
            "dri_devices": [
                {"path": "/dev/dri/card0", "name": "card0", "kind": "card", "vendor": "intel", "vendor_id": "0x8086"},
                {"path": "/dev/dri/renderD128", "name": "renderD128", "kind": "render", "vendor": "intel", "vendor_id": "0x8086"},
            ],
            "render_devices": [
                {"path": "/dev/dri/renderD128", "name": "renderD128", "kind": "render", "vendor": "intel", "vendor_id": "0x8086"},
            ],
            "card_devices": [
                {"path": "/dev/dri/card0", "name": "card0", "kind": "card", "vendor": "intel", "vendor_id": "0x8086"},
            ],
            "kfd_present": False,
            "message": "Intel Vulkan available",
        }):
            profile = pmda._managed_runtime_ollama_gpu_profile()
        self.assertEqual(profile["selected_mode"], "vulkan_intel")
        self.assertTrue(profile["active"])
        self.assertEqual(profile["env"]["OLLAMA_VULKAN"], "1")
        self.assertIn("/dev/dri/renderD128:/dev/dri/renderD128", profile["docker_args"])

    def test_managed_runtime_preflight_includes_gpu_probe(self):
        with mock.patch.object(pmda, "_managed_runtime_docker_cli", return_value="/usr/bin/docker"), \
             mock.patch.object(pmda, "_managed_runtime_compose_cli", return_value=["/usr/bin/docker", "compose"]), \
             mock.patch.object(pmda, "_managed_runtime_git_cli", return_value="/usr/bin/git"), \
             mock.patch.object(pmda, "_managed_runtime_gpu_probe", return_value={
                 "available": True,
                 "recommended_mode": "amd_rocm",
                 "available_modes": ["amd_rocm", "vulkan_amd"],
                 "message": "AMD GPU ready",
             }), \
             mock.patch.object(pmda.Path, "exists", return_value=True), \
             mock.patch.object(pmda.subprocess, "run", return_value=mock.Mock(returncode=0, stdout="26.1", stderr="")):
            payload = pmda._managed_runtime_preflight()
        self.assertTrue(payload["available"])
        self.assertEqual(payload["gpu_probe"]["recommended_mode"], "amd_rocm")

    def test_managed_runtime_compose_cli_falls_back_to_docker_compose_binary(self):
        def fake_run(cmd, capture_output=True, text=True, timeout=10):
            if cmd == ["/usr/bin/docker", "compose", "version"]:
                return mock.Mock(returncode=1, stdout="", stderr="compose subcommand missing")
            if cmd == ["/usr/bin/docker-compose", "version"]:
                return mock.Mock(returncode=0, stdout="Docker Compose version v2", stderr="")
            raise AssertionError(f"unexpected command: {cmd}")

        with mock.patch.object(pmda.shutil, "which", side_effect=lambda name: {
            "docker": "/usr/bin/docker",
            "docker-compose": "/usr/bin/docker-compose",
        }.get(name, "")), mock.patch.object(pmda.subprocess, "run", side_effect=fake_run):
            self.assertEqual(pmda._managed_runtime_compose_cli(), ["/usr/bin/docker-compose"])

    def test_managed_runtime_mb_compose_cmd_uses_docker_compose_binary_when_needed(self):
        with tempfile.TemporaryDirectory(prefix="pmda-mb-compose-") as tmpdir:
            compose_file = Path(tmpdir) / "docker-compose.yml"
            compose_file.write_text("services: {}\n", encoding="utf-8")
            with mock.patch.object(pmda, "_managed_runtime_compose_cli", return_value=["/usr/bin/docker-compose"]):
                cmd = pmda._managed_runtime_mb_compose_cmd(tmpdir, "up", "-d")
        self.assertEqual(
            cmd,
            ["/usr/bin/docker-compose", "--project-directory", tmpdir, "up", "-d"],
        )

    def test_managed_runtime_bootstrap_endpoint_starts_selected_bundle(self):
        bundle_status = {
            "bundle_type": "musicbrainz_local",
            "mode": "managed",
            "state": "creating",
            "phase": "creating",
            "phase_message": "Provisioning",
            "health": {"available": False, "overall_status": "starting"},
            "services": [],
        }
        snapshot = {
            "preflight": {"available": True},
            "config_root": "/config",
            "data_root": "/data",
            "ready": False,
            "bundles": {
                "musicbrainz_local": bundle_status,
                "ollama_local": {
                    "bundle_type": "ollama_local",
                    "mode": "absent",
                    "state": "idle",
                    "phase": "idle",
                    "phase_message": "",
                    "health": {"available": False, "overall_status": "absent"},
                    "services": [],
                },
            },
        }
        with mock.patch.object(pmda, "_managed_runtime_launch_bootstrap", return_value=(True, "started")) as launch_mock, \
             mock.patch.object(pmda, "_managed_runtime_bundle_status", return_value=bundle_status), \
             mock.patch.object(pmda, "_managed_runtime_status_snapshot", return_value=snapshot):
            resp = self.client.post(
                "/api/runtime/managed/bootstrap",
                json={
                    "bundle_type": "musicbrainz_local",
                    "config_root": "/config",
                    "data_root": "/data",
                    "payload": {"action": "auto", "mirror_name": "LAN MB"},
                },
            )
        self.assertEqual(resp.status_code, 202)
        payload = resp.get_json() or {}
        self.assertEqual(payload["results"][0]["bundle_type"], "musicbrainz_local")
        self.assertTrue(payload["results"][0]["started"])
        launch_mock.assert_called_once()

    def test_managed_runtime_action_refresh_health_returns_status(self):
        bundle_status = {
            "bundle_type": "ollama_local",
            "mode": "managed",
            "state": "ready",
            "phase": "ready",
            "phase_message": "Ollama ready",
            "health": {"available": True, "overall_status": "healthy", "models": ["qwen3:4b", "qwen3:14b"]},
            "services": [{"name": "ollama", "status": "healthy"}],
        }
        with mock.patch.object(pmda, "_managed_runtime_bundle_get", return_value=bundle_status), \
             mock.patch.object(pmda, "_managed_runtime_bundle_status", return_value=bundle_status):
            resp = self.client.post(
                "/api/runtime/managed/action",
                json={"bundle_type": "ollama_local", "action": "refresh-health"},
            )
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload["bundle_type"], "ollama_local")
        self.assertEqual(payload["result"]["state"], "ready")

    def test_managed_runtime_due_update_deferred_while_scan_active(self):
        now = time.time()
        bundle = {
            "bundle_type": "musicbrainz_local",
            "mode": "managed",
            "state": "ready",
            "phase": "ready",
            "phase_message": "MusicBrainz ready",
            "update_state": {
                "enabled": True,
                "interval_sec": 3600,
                "next_planned_at": now - 10,
            },
        }
        with pmda.lock:
            pmda.state["scanning"] = True
            pmda.state["scan_starting"] = False
            pmda.state["scan_finalizing"] = False
        with mock.patch.object(pmda, "_managed_runtime_bundle_get", return_value=bundle), \
             mock.patch.object(pmda, "_managed_runtime_register_mb_update_schedule", return_value=dict(bundle["update_state"])), \
             mock.patch.object(pmda, "_managed_runtime_bundle_upsert") as upsert_mock, \
             mock.patch.object(pmda, "_scheduler_launch_job") as launch_mock:
            pmda._managed_runtime_maybe_enqueue_due_jobs(now)
        launch_mock.assert_not_called()
        self.assertTrue(upsert_mock.called)
        update_state = upsert_mock.call_args.kwargs.get("update_state") or {}
        self.assertEqual(update_state.get("last_deferred_reason"), "scan_active")

    def test_managed_runtime_due_update_skips_adopted_bundle(self):
        now = time.time()
        bundle = {
            "bundle_type": "musicbrainz_local",
            "mode": "adopted",
            "state": "ready",
            "phase": "ready",
            "phase_message": "MusicBrainz ready",
            "update_state": {
                "enabled": True,
                "interval_sec": 3600,
                "next_planned_at": now - 10,
            },
        }
        with mock.patch.object(pmda, "_managed_runtime_bundle_get", return_value=bundle), \
             mock.patch.object(pmda, "_managed_runtime_bundle_upsert") as upsert_mock, \
             mock.patch.object(pmda, "_scheduler_launch_job") as launch_mock:
            pmda._managed_runtime_maybe_enqueue_due_jobs(now)
        launch_mock.assert_not_called()
        upsert_mock.assert_not_called()

    def test_managed_runtime_run_update_skips_adopted_bundle(self):
        bundle = {
            "bundle_type": "musicbrainz_local",
            "mode": "adopted",
            "state": "ready",
            "phase": "ready",
            "phase_message": "MusicBrainz ready",
            "update_state": {
                "enabled": True,
                "interval_sec": 3600,
                "next_planned_at": time.time() - 10,
            },
        }
        with mock.patch.object(pmda, "_managed_runtime_bundle_get", return_value=bundle), \
             mock.patch.object(pmda, "_managed_runtime_bundle_upsert", side_effect=lambda *args, **kwargs: {**bundle, **kwargs}) as upsert_mock:
            ok, message, meta = pmda._managed_runtime_run_musicbrainz_update()
        self.assertTrue(ok)
        self.assertIn("skipped", message.lower())
        self.assertEqual(meta.get("status"), "skipped")
        update_state = upsert_mock.call_args.kwargs.get("update_state") or {}
        self.assertFalse(update_state.get("enabled"))
        self.assertEqual(update_state.get("strategy"), "external_runtime")

    def test_detect_musicbrainz_candidates_exposes_install_root(self):
        docker_rows = [
            {"Names": "musicbrainz-docker-musicbrainz-1", "State": "running", "Image": "mb", "Ports": "0.0.0.0:5500->5000/tcp"},
            {"Names": "musicbrainz-docker-db-1", "State": "running", "Image": "db", "Ports": ""},
            {"Names": "musicbrainz-docker-search-1", "State": "running", "Image": "search", "Ports": ""},
        ]
        with mock.patch.object(pmda, "_managed_runtime_docker_ps", return_value=docker_rows), \
             mock.patch.object(pmda, "_managed_runtime_container_labels", return_value={"com.docker.compose.project.working_dir": "/config/managed-runtime/musicbrainz-docker"}), \
             mock.patch.object(pmda, "_managed_runtime_health_check_musicbrainz", return_value={"available": True, "overall_status": "healthy", "message": "ok"}):
            candidates = pmda._managed_runtime_detect_musicbrainz_candidates()
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["install_root"], "/config/managed-runtime/musicbrainz-docker")

    def test_detect_ollama_candidates_from_existing_docker_stack(self):
        docker_rows = [
            {
                "Names": "ollama",
                "State": "running",
                "Image": "ollama/ollama:latest",
                "Ports": "",
                "ID": "abc123",
            }
        ]
        inspect_payload = {
            "NetworkSettings": {
                "Networks": {
                    "pmda_default": {
                        "Aliases": ["ollama", "ollama-1"],
                        "IPAddress": "172.19.0.7",
                    }
                }
            }
        }

        def fake_probe(url: str):
            normalized = str(url or "").rstrip("/")
            ok = normalized in {"http://ollama:11434", "http://172.19.0.7:11434"}
            return {
                "ok": ok,
                "url": normalized,
                "message": "ok" if ok else "unreachable",
                "models": ["qwen3:4b"] if ok else [],
                "model_count": 1 if ok else 0,
            }

        with mock.patch.object(pmda, "_managed_runtime_docker_ps", return_value=docker_rows), \
             mock.patch.object(pmda, "_managed_runtime_docker_inspect_container", return_value=inspect_payload), \
             mock.patch.object(pmda, "_ollama_probe", side_effect=fake_probe):
            candidates = pmda._managed_runtime_detect_ollama_candidates()
        adoptable = [row for row in candidates if row.get("adoptable")]
        self.assertTrue(adoptable)
        self.assertEqual(adoptable[0]["container_name"], "ollama")
        self.assertIn("pmda_default", adoptable[0]["networks"])

    def test_managed_runtime_ollama_status_marks_external_candidate_adopted(self):
        bundle = {
            "bundle_type": "ollama_local",
            "mode": "managed",
            "state": "failed",
            "phase": "failed",
            "effective_url": "",
            "health": {"available": False},
        }
        candidate = {
            "id": "ollama-1",
            "url": "http://ollama:11434",
            "adoptable": True,
            "source": "docker",
            "container_name": "ollama",
            "networks": ["pmda_default"],
            "aliases": ["ollama"],
            "health": {"available": True, "overall_status": "healthy"},
            "models": ["qwen3:4b"],
            "model_count": 1,
        }
        with mock.patch.object(pmda, "_managed_runtime_bundle_get", return_value=bundle), \
             mock.patch.object(pmda, "_managed_runtime_get_latest_action", return_value=None), \
             mock.patch.object(pmda, "_managed_runtime_detect_ollama_candidates", return_value=[candidate]), \
             mock.patch.object(pmda, "_managed_runtime_bundle_upsert_best_effort", side_effect=lambda _bundle_type, fallback=None, **kwargs: {**(fallback or {}), **kwargs}):
            status = pmda._managed_runtime_bundle_status("ollama_local", include_candidates=True)
        self.assertEqual(status["mode"], "adopted")
        self.assertEqual(status["ownership"], "ollama")
        self.assertEqual((status.get("meta") or {}).get("container_name"), "ollama")

    def test_bootstrap_ollama_adopts_existing_and_only_ensures_models(self):
        candidate = {
            "id": "ollama-1",
            "url": "http://ollama:11434",
            "adoptable": True,
            "source": "docker",
            "container_name": "ollama",
            "health": {"available": True, "overall_status": "healthy"},
            "models": ["qwen3:4b"],
            "model_count": 1,
        }
        adopted = {
            "bundle_type": "ollama_local",
            "mode": "adopted",
            "state": "ready",
            "effective_url": "http://ollama:11434",
            "meta": {"models": ["qwen3:4b"]},
        }
        with mock.patch.object(pmda, "_managed_runtime_preflight", return_value={"docker_ok": True}), \
             mock.patch.object(pmda, "_managed_runtime_detect_ollama_candidates", return_value=[candidate]), \
             mock.patch.object(pmda, "_managed_runtime_adopt_ollama", return_value=adopted) as adopt_mock, \
             mock.patch.object(pmda, "_managed_runtime_ensure_ollama_models", return_value={"available": True, "models": ["qwen3:4b", "qwen3:14b"]}) as ensure_mock, \
             mock.patch.object(pmda.subprocess, "run", side_effect=AssertionError("managed container must not be recreated when adopting existing Ollama")):
            pmda._managed_runtime_bootstrap_ollama(
                {
                    "action": "auto",
                    "config_root": "/config/pmda",
                    "data_root": "/data/pmda",
                    "fast_model": "qwen3:4b",
                    "hard_model": "qwen3:14b",
                }
            )
        adopt_mock.assert_called_once()
        ensure_mock.assert_called_once_with("http://ollama:11434", ["qwen3:4b", "qwen3:14b"], bundle_type="ollama_local")

    def test_musicbrainz_health_check_requires_non_empty_probe_results(self):
        def fake_get(url, timeout=20, headers=None):
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
            artist_q = query.get("query", [""])[0]
            payload = {"artists": [{"id": "123"}]} if "Radiohead" in artist_q else {"artists": []}
            return mock.Mock(status_code=200, content=b"{}", json=lambda: payload)

        with mock.patch.object(pmda.requests, "get", side_effect=fake_get):
            health = pmda._managed_runtime_health_check_musicbrainz("http://mb.internal:5500")
        self.assertTrue(health["available"])
        self.assertGreater(health.get("result_count", 0), 0)

    def test_musicbrainz_health_check_normalizes_host_without_scheme(self):
        fake_response = mock.Mock(
            status_code=200,
            content=b'{"artists":[{"id":"123"}]}',
            json=lambda: {"artists": [{"id": "123"}]},
        )

        with mock.patch.object(pmda.requests, "get", return_value=fake_response) as req_get:
            health = pmda._managed_runtime_health_check_musicbrainz("mb.internal:5500")

        self.assertTrue(health["available"])
        self.assertEqual(health["overall_status"], "healthy")
        called_url = req_get.call_args.args[0]
        self.assertTrue(called_url.startswith("http://mb.internal:5500/ws/2/artist"))

    def test_fetch_qobuz_album_info_uses_current_search_route(self):
        pmda.USE_QOBUZ = True
        search_resp = mock.Mock(status_code=200, text="<html>search</html>")
        detail_resp = mock.Mock(status_code=200, text="<html>detail</html>", url="https://www.qobuz.com/us-en/album/ok-computer/123")
        calls: list[tuple[str, dict | None]] = []

        def fake_http_get(provider, url, params=None, **kwargs):
            calls.append((url, params))
            if "search/albums/" in url:
                return search_resp
            return detail_resp

        with mock.patch.object(pmda, "_provider_gateway_http_get", side_effect=fake_http_get), \
             mock.patch.object(pmda, "_qobuz_album_page_urls", return_value=[("123", "https://www.qobuz.com/us-en/album/ok-computer/123")]), \
             mock.patch.object(
                 pmda,
                 "_parse_public_album_page_payload",
                 return_value={"artist_name": "Radiohead", "title": "OK Computer", "url": "https://www.qobuz.com/us-en/album/ok-computer/123"},
             ):
            payload = pmda._fetch_qobuz_album_info("Radiohead", "OK Computer")

        self.assertIsInstance(payload, dict)
        self.assertIn("/us-en/search/albums/", calls[0][0])
        self.assertIsNone(calls[0][1])

    def test_scaling_runtime_endpoint_reports_runtime_config(self):
        pmda.MUSICBRAINZ_MIRROR_ENABLED = True
        pmda.MUSICBRAINZ_BASE_URL = "https://mb.internal"
        pmda.MUSICBRAINZ_MIRROR_NAME = "LAN mirror"
        pmda.PROVIDER_GATEWAY_ENABLED = True
        pmda.PROVIDER_GATEWAY_CACHE_ENABLED = True
        pmda.PROVIDER_GATEWAY_MAX_INFLIGHT = 12
        pmda.METADATA_QUEUE_ENABLED = True
        pmda.METADATA_WORKER_MODE = "hybrid"
        pmda.METADATA_WORKER_COUNT = 6
        pmda.METADATA_JOB_BATCH_SIZE = 40
        pmda._configure_musicbrainz_client()
        pmda._provider_gateway_reconfigure()

        with pmda.lock:
            pmda.state["scan_start_time"] = pmda.time.time() - 10
            pmda.state["scan_discovery_files_found"] = 120
            pmda.state["scan_discovery_audio_found"] = 60
            pmda.state["scan_processed_albums_count"] = 15
            pmda.state["scan_published_albums_count"] = 9
            pmda.state["scan_artists_processed"] = 3
            pmda.state["scan_discovery_stage"] = "filesystem"

        resp = self.client.get("/api/statistics/scaling-runtime")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertTrue(payload["musicbrainz"]["mirror_enabled"])
        self.assertEqual(payload["musicbrainz"]["base_url"], "https://mb.internal")
        self.assertTrue(payload["provider_gateway"]["enabled"])
        self.assertEqual(payload["provider_gateway"]["max_inflight"], 12)
        self.assertIn("auto_tune", payload)
        self.assertTrue(payload["metadata_workers"]["queue_enabled"])
        self.assertEqual(payload["metadata_workers"]["mode"], "hybrid")
        self.assertEqual(payload["metadata_workers"]["worker_count"], 6)
        self.assertEqual(payload["metadata_workers"]["batch_size"], 40)
        self.assertEqual(payload["pipeline"]["ocr_execution"], "local")
        self.assertEqual(payload["stage_rates"]["phase"], "filesystem")

    def test_scaling_runtime_endpoint_reports_auto_metadata_worker_values(self):
        pmda.METADATA_QUEUE_ENABLED = True
        pmda.METADATA_WORKER_MODE = "hybrid"
        pmda.METADATA_WORKER_COUNT = 0
        pmda.METADATA_JOB_BATCH_SIZE = 0

        with mock.patch.object(pmda.os, "cpu_count", return_value=12):
            resp = self.client.get("/api/statistics/scaling-runtime")

        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json() or {}
        self.assertEqual(payload["metadata_workers"]["worker_count"], 0)
        self.assertEqual(payload["metadata_workers"]["batch_size"], 0)
        self.assertEqual(payload["metadata_workers"]["worker_count_mode"], "auto")
        self.assertEqual(payload["metadata_workers"]["batch_size_mode"], "auto")
        self.assertEqual(payload["metadata_workers"]["effective_worker_count"], 12)
        self.assertEqual(payload["metadata_workers"]["effective_batch_size"], 64)
