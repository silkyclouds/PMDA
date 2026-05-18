from pmda_core import config


def test_bool_parsers_accept_common_values():
    assert config.parse_bool(True) is True
    assert config.parse_bool("true") is True
    assert config.parse_bool("ON") is True
    assert config.parse_bool("0") is False
    assert config.is_false(False) is True
    assert config.is_false("off") is True
    assert config.is_false("yes") is False


def test_filter_disabled_external_updates_drops_retired_integrations():
    filtered, ignored = config.filter_disabled_external_updates(
        {
            "PLEX_DB_PATH": "/config/plex.db",
            "LIDARR_URL": "http://lidarr",
            "PLEX_HOST": "http://plex",
            "USE_MUSICBRAINZ": True,
        }
    )

    assert filtered == {"PLEX_HOST": "http://plex", "USE_MUSICBRAINZ": True}
    assert ignored == {"PLEX_DB_PATH", "LIDARR_URL"}


def test_filter_disabled_external_updates_preserves_clean_payload():
    payload = {"PLEX_HOST": "http://plex", "USE_LASTFM": False}

    filtered, ignored = config.filter_disabled_external_updates(payload)

    assert filtered == payload
    assert ignored == set()
    assert filtered is not payload


def test_config_update_allowed_keys_cover_active_settings_only():
    assert "PLEX_HOST" in config.CONFIG_UPDATE_ALLOWED_KEYS
    assert "PLEX_TOKEN" in config.CONFIG_UPDATE_ALLOWED_KEYS
    assert "LIBRARY_WORKFLOW_MODE" in config.CONFIG_UPDATE_ALLOWED_KEYS
    assert "STORAGE_POWER_SAVER_ENABLED" in config.CONFIG_UPDATE_ALLOWED_KEYS
    assert "MCP_ENABLED" in config.CONFIG_UPDATE_ALLOWED_KEYS
    assert "PLEX_DB_PATH" not in config.CONFIG_UPDATE_ALLOWED_KEYS
    assert "LIDARR_URL" not in config.CONFIG_UPDATE_ALLOWED_KEYS
    assert "AUTOBRR_URL" not in config.CONFIG_UPDATE_ALLOWED_KEYS


def test_normalize_storage_power_saver_settings_only_returns_present_keys():
    normalized = config.normalize_storage_power_saver_settings(
        {
            "STORAGE_POWER_SAVER_ENABLED": "yes",
            "UNRAID_HOST_MNT_ROOT": "/host_mnt/",
            "STORAGE_MAX_ACTIVE_DEVICES": "999",
            "UNRELATED": "ignored",
        }
    )

    assert normalized == {
        "STORAGE_POWER_SAVER_ENABLED": True,
        "UNRAID_HOST_MNT_ROOT": "/host_mnt",
        "STORAGE_MAX_ACTIVE_DEVICES": 64,
    }


def test_normalize_storage_power_saver_settings_enforces_v1_allowed_values():
    normalized = config.normalize_storage_power_saver_settings(
        {
            "STORAGE_PROVIDER": "other",
            "UNRAID_USER_SHARE_HOST_ROOT": "",
            "UNRAID_CONTAINER_SHARE_ROOT": "/music/",
            "STORAGE_MAX_ACTIVE_DEVICES": "0",
            "STORAGE_SPINDOWN_POLICY": "force",
        }
    )

    assert normalized == {
        "STORAGE_PROVIDER": "unraid",
        "UNRAID_USER_SHARE_HOST_ROOT": "/host_mnt/user/MURRAY/Music",
        "UNRAID_CONTAINER_SHARE_ROOT": "/music",
        "STORAGE_MAX_ACTIVE_DEVICES": 1,
        "STORAGE_SPINDOWN_POLICY": "none",
    }


def test_normalize_task_notification_settings_parses_booleans_and_cooldown():
    normalized = config.normalize_task_notification_settings(
        {
            "TASK_NOTIFICATIONS_ENABLED": "on",
            "TASK_NOTIFICATIONS_FAILURE": "0",
            "TASK_NOTIFICATIONS_COOLDOWN_SEC": "9999",
            "TASK_NOTIFY_PLAYER_SYNC": True,
            "OTHER": "ignored",
        }
    )

    assert normalized == {
        "TASK_NOTIFICATIONS_ENABLED": True,
        "TASK_NOTIFICATIONS_FAILURE": False,
        "TASK_NOTIFICATIONS_COOLDOWN_SEC": 3600,
        "TASK_NOTIFY_PLAYER_SYNC": True,
    }


def test_normalize_task_notification_settings_defaults_bad_cooldown():
    assert config.normalize_task_notification_settings({"TASK_NOTIFICATIONS_COOLDOWN_SEC": "bad"}) == {
        "TASK_NOTIFICATIONS_COOLDOWN_SEC": 20
    }


def test_normalize_pipeline_bool_settings_only_returns_present_keys():
    assert config.normalize_pipeline_bool_settings(
        {
            "PIPELINE_ENABLE_MATCH_FIX": "yes",
            "PIPELINE_ENABLE_EXPORT": "0",
            "PIPELINE_PLAYER_TARGET": "plex",
        }
    ) == {
        "PIPELINE_ENABLE_MATCH_FIX": True,
        "PIPELINE_ENABLE_EXPORT": False,
    }


def test_normalize_metadata_worker_settings_clamps_values():
    assert config.normalize_metadata_worker_settings(
        {
            "METADATA_QUEUE_ENABLED": "true",
            "METADATA_WORKER_MODE": "remote",
            "METADATA_WORKER_COUNT": "999",
            "METADATA_JOB_BATCH_SIZE": "-1",
        }
    ) == {
        "METADATA_QUEUE_ENABLED": True,
        "METADATA_WORKER_MODE": "local",
        "METADATA_WORKER_COUNT": 128,
        "METADATA_JOB_BATCH_SIZE": 0,
    }


def test_normalize_metadata_worker_settings_accepts_hybrid_and_bad_int_defaults():
    assert config.normalize_metadata_worker_settings(
        {
            "METADATA_WORKER_MODE": "hybrid",
            "METADATA_WORKER_COUNT": "bad",
            "METADATA_JOB_BATCH_SIZE": "bad",
        }
    ) == {
        "METADATA_WORKER_MODE": "hybrid",
        "METADATA_WORKER_COUNT": 0,
        "METADATA_JOB_BATCH_SIZE": 0,
    }


def test_parse_path_map_accepts_json_and_csv():
    assert config.parse_path_map('{"A": "/music/A"}') == {"A": "/music/A"}
    assert config.parse_path_map("A:/music/A,B:/music/B") == {"A": "/music/A", "B": "/music/B"}
    assert config.parse_path_map("") == {}


def test_parse_files_roots_handles_nested_json_csv_and_dedupe():
    raw = '["/music/Music_dump/", "[\\"/music/Music_matched\\"]", "/music/Music_dump"]'
    assert config.parse_files_roots(raw) == ["/music/Music_dump", "/music/Music_matched"]
    assert config.parse_files_roots("/one,/two/,/one") == ["/one", "/two"]


def test_parse_skip_folders_drops_corrupt_json_like_entries():
    assert config.parse_skip_folders('["skip", "[]"]') == ["skip"]
    assert config.parse_skip_folders("tmp,cache") == ["tmp", "cache"]


def test_format_preference_and_modes_are_stable():
    assert config.parse_format_preference("flac,mp3") == ["flac", "mp3"]
    assert config.parse_format_preference("") == config.DEFAULT_FORMAT_PREFERENCE
    assert config.normalize_library_mode("plex") == "files"
    assert config.normalize_library_workflow_mode("audit") == "audit"
    assert config.normalize_library_workflow_mode("bad", default="mirror") == "mirror"
    assert config.normalize_library_scope("dupes") == "dupes"
    assert config.normalize_library_scope("bad") == "library"


def test_normalize_files_root_path():
    assert config.normalize_files_root_path("music//dump/") == "/music/dump"
    assert config.normalize_files_root_path(r"\\music\\dump\\") == "/music/dump"
    assert config.normalize_files_root_path("") == ""
