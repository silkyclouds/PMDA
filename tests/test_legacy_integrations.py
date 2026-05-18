from __future__ import annotations

from pmda_core import legacy_integrations


def test_removed_acquisition_features_are_disabled() -> None:
    assert legacy_integrations.lidarr_feature_enabled() is False
    assert legacy_integrations.autobrr_feature_enabled() is False


def test_disabled_payloads_are_stable() -> None:
    assert legacy_integrations.disabled_lidarr_payload() == {
        "message": "Lidarr integration is currently disabled",
    }
    assert legacy_integrations.disabled_lidarr_payload(started=False) == {
        "error": "Lidarr integration is currently disabled",
        "started": False,
    }
    assert legacy_integrations.disabled_autobrr_payload() == {
        "message": "Autobrr integration is currently disabled",
    }


def test_removed_acquisition_stubs_do_not_execute_work() -> None:
    assert legacy_integrations.ignore_album_acquisition("Artist", 1, "mbid", "Album") is False
    assert legacy_integrations.ignore_artist_acquisition(1, "Artist", "mbid") is False
    assert legacy_integrations.ignore_autobrr_filter(["Artist"], {"quality": "lossless"}) is False
