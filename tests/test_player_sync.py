from __future__ import annotations

from dataclasses import dataclass

from pmda_integrations import player_sync


@dataclass
class FakeResponse:
    status_code: int
    text: str = ""
    headers: dict[str, str] | None = None

    def json(self) -> dict:
        return {}


def test_normalize_player_target_keeps_supported_targets() -> None:
    assert player_sync.normalize_player_target("plex") == "plex"
    assert player_sync.normalize_player_target("JELLYFIN") == "jellyfin"
    assert player_sync.normalize_player_target("navidrome") == "navidrome"
    assert player_sync.normalize_player_target("legacy-plex-db") == "none"


def test_plex_check_uses_player_api_only(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    def fake_get(url: str, **kwargs):
        calls.append((url, kwargs))
        return FakeResponse(
            200,
            '<MediaContainer><Directory key="1" type="artist" title="Music"/></MediaContainer>',
        )

    monkeypatch.setattr(player_sync.requests, "get", fake_get)

    result = player_sync.check_plex("plex:32400", "secret")

    assert result.success is True
    assert "music section" in result.message
    assert calls == [("http://plex:32400/library/sections", {"headers": {"Accept": "application/json, application/xml;q=0.9, */*;q=0.8", "X-Plex-Token": "secret"}, "timeout": 10})]


def test_plex_refresh_targets_music_sections(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_get(url: str, **kwargs):
        calls.append(("GET", url))
        return FakeResponse(
            200,
            (
                '<MediaContainer>'
                '<Directory key="1" type="movie" title="Movies"/>'
                '<Directory key="2" type="artist" title="Music"/>'
                "</MediaContainer>"
            ),
        )

    def fake_put(url: str, **kwargs):
        calls.append(("PUT", url))
        return FakeResponse(200)

    monkeypatch.setattr(player_sync.requests, "get", fake_get)
    monkeypatch.setattr(player_sync.requests, "put", fake_put)

    result = player_sync.trigger_plex_refresh("http://plex:32400", "secret")

    assert result.success is True
    assert calls == [
        ("GET", "http://plex:32400/library/sections"),
        ("PUT", "http://plex:32400/library/sections/2/refresh"),
    ]


def test_plex_refresh_honors_preferred_section_ids(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_get(url: str, **kwargs):
        calls.append(("GET", url))
        return FakeResponse(
            200,
            (
                '<MediaContainer>'
                '<Directory key="5" type="movie" title="Wrongly typed but user-selected"/>'
                '<Directory key="7" type="artist" title="Music"/>'
                "</MediaContainer>"
            ),
        )

    def fake_put(url: str, **kwargs):
        calls.append(("PUT", url))
        return FakeResponse(202)

    monkeypatch.setattr(player_sync.requests, "get", fake_get)
    monkeypatch.setattr(player_sync.requests, "put", fake_put)

    result = player_sync.trigger_plex_refresh("http://plex:32400", "secret", preferred_section_ids=["5"])

    assert result.success is True
    assert calls == [
        ("GET", "http://plex:32400/library/sections"),
        ("PUT", "http://plex:32400/library/sections/5/refresh"),
        ("PUT", "http://plex:32400/library/sections/7/refresh"),
    ]
