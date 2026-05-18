"""Post-publication player sync helpers.

PMDA no longer uses Plex as a source database. Plex remains supported here only
as an external player that can be asked to refresh its own library after PMDA
has published files, just like Jellyfin and Navidrome.
"""

from __future__ import annotations

import hashlib
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterable

import requests


SUPPORTED_PLAYER_TARGETS = {"none", "plex", "jellyfin", "navidrome"}


@dataclass(frozen=True)
class PlayerResult:
    success: bool
    message: str


def normalize_player_target(raw: str | None) -> str:
    target = str(raw or "").strip().lower()
    return target if target in SUPPORTED_PLAYER_TARGETS else "none"


def normalize_http_base_url(url: str) -> str:
    out = str(url or "").strip().rstrip("/")
    if out and not out.startswith(("http://", "https://")):
        out = "http://" + out
    return out


def _plex_headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/json, application/xml;q=0.9, */*;q=0.8",
        "X-Plex-Token": str(token or "").strip(),
    }


def _positive_ints(values: Iterable[int] | None) -> list[int]:
    out: list[int] = []
    for value in values or []:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in out:
            out.append(parsed)
    return out


def _plex_music_section_ids(base: str, token: str, preferred_section_ids: Iterable[int] | None = None) -> tuple[list[int], str | None]:
    preferred = _positive_ints(preferred_section_ids)
    try:
        response = requests.get(
            f"{base}/library/sections",
            headers=_plex_headers(token),
            timeout=10,
        )
    except requests.exceptions.ConnectionError:
        return [], "Plex host unreachable or connection refused"
    except requests.exceptions.Timeout:
        return [], "Plex request timed out"
    except Exception as exc:
        return [], str(exc)

    if response.status_code in {401, 403}:
        return [], "Plex token invalid or unauthorized"
    if response.status_code >= 400:
        return [], f"Plex library sections returned HTTP {response.status_code}"

    try:
        root = ET.fromstring(response.text)
    except ET.ParseError as exc:
        return [], f"Plex returned invalid XML: {exc}"

    music_ids: list[int] = []
    for directory in root.iter("Directory"):
        raw_key = directory.attrib.get("key")
        section_type = str(directory.attrib.get("type") or "").strip().lower()
        if not raw_key:
            continue
        try:
            key = int(raw_key)
        except ValueError:
            continue
        if preferred and key in preferred:
            music_ids.append(key)
            continue
        if section_type in {"artist", "music"}:
            music_ids.append(key)

    # Preserve user-selected order first, then any discovered music sections.
    ordered = []
    for key in [*preferred, *music_ids]:
        if key not in ordered:
            ordered.append(key)
    return ordered, None


def check_plex(host: str, token: str) -> PlayerResult:
    base = normalize_http_base_url(host)
    key = str(token or "").strip()
    if not base or not key:
        return PlayerResult(False, "Plex URL and token are required")
    sections, error = _plex_music_section_ids(base, key)
    if error:
        return PlayerResult(False, error)
    if not sections:
        return PlayerResult(False, "Plex is reachable, but no music library section was found")
    return PlayerResult(True, f"Plex connection successful ({len(sections)} music section(s))")


def trigger_plex_refresh(host: str, token: str, preferred_section_ids: Iterable[int] | None = None) -> PlayerResult:
    base = normalize_http_base_url(host)
    key = str(token or "").strip()
    if not base or not key:
        return PlayerResult(False, "Plex URL and token are required")
    sections, error = _plex_music_section_ids(base, key, preferred_section_ids)
    if error:
        return PlayerResult(False, error)
    if not sections:
        return PlayerResult(False, "No Plex music library section found to refresh")

    failures: list[str] = []
    refreshed = 0
    for section_id in sections:
        try:
            response = requests.put(
                f"{base}/library/sections/{section_id}/refresh",
                headers=_plex_headers(key),
                timeout=20,
            )
        except requests.exceptions.ConnectionError:
            failures.append(f"section {section_id}: unreachable")
            continue
        except requests.exceptions.Timeout:
            failures.append(f"section {section_id}: timeout")
            continue
        except Exception as exc:
            failures.append(f"section {section_id}: {exc}")
            continue
        if response.status_code in {200, 201, 202, 204}:
            refreshed += 1
            continue
        failures.append(f"section {section_id}: HTTP {response.status_code}")

    if refreshed and not failures:
        return PlayerResult(True, f"Plex library refresh triggered for {refreshed} music section(s)")
    if refreshed:
        return PlayerResult(True, f"Plex refresh partially triggered for {refreshed}/{len(sections)} section(s): {'; '.join(failures[:3])}")
    return PlayerResult(False, f"Plex refresh failed: {'; '.join(failures[:3]) or 'unknown error'}")


def jellyfin_auth_headers(api_key: str) -> dict[str, str]:
    token = str(api_key or "").strip()
    return {
        "Accept": "application/json",
        "Authorization": f'MediaBrowser Token="{token}"',
    }


def check_jellyfin(url: str, api_key: str) -> PlayerResult:
    base = normalize_http_base_url(url)
    key = str(api_key or "").strip()
    if not base or not key:
        return PlayerResult(False, "Jellyfin URL and API key are required")
    try:
        public_resp = requests.get(f"{base}/System/Info/Public", timeout=10)
        if public_resp.status_code >= 400:
            return PlayerResult(False, f"Jellyfin public info returned HTTP {public_resp.status_code}")
        auth_resp = requests.get(
            f"{base}/Items/Counts",
            headers=jellyfin_auth_headers(key),
            timeout=10,
        )
        if auth_resp.status_code == 401:
            return PlayerResult(False, "Jellyfin API key invalid (401)")
        if auth_resp.status_code == 403:
            return PlayerResult(False, "Jellyfin API key lacks permissions (403)")
        if auth_resp.status_code >= 400:
            return PlayerResult(False, f"Jellyfin auth check returned HTTP {auth_resp.status_code}")
        return PlayerResult(True, "Jellyfin connection successful")
    except requests.exceptions.ConnectionError:
        return PlayerResult(False, "Jellyfin host unreachable or connection refused")
    except requests.exceptions.Timeout:
        return PlayerResult(False, "Jellyfin request timed out")
    except Exception as exc:
        return PlayerResult(False, str(exc))


def trigger_jellyfin_refresh(url: str, api_key: str) -> PlayerResult:
    base = normalize_http_base_url(url)
    key = str(api_key or "").strip()
    if not base or not key:
        return PlayerResult(False, "Jellyfin URL and API key are required")
    try:
        response = requests.post(
            f"{base}/Library/Refresh",
            headers=jellyfin_auth_headers(key),
            timeout=20,
        )
        if response.status_code in (200, 204):
            return PlayerResult(True, "Jellyfin library refresh triggered")
        if response.status_code == 401:
            return PlayerResult(False, "Jellyfin API key invalid (401)")
        if response.status_code == 403:
            return PlayerResult(False, "Jellyfin API key lacks required permission (403)")
        return PlayerResult(False, f"Jellyfin refresh failed (HTTP {response.status_code})")
    except requests.exceptions.ConnectionError:
        return PlayerResult(False, "Jellyfin host unreachable or connection refused")
    except requests.exceptions.Timeout:
        return PlayerResult(False, "Jellyfin refresh request timed out")
    except Exception as exc:
        return PlayerResult(False, str(exc))


def navidrome_auth_params(username: str, password: str, api_key: str = "") -> tuple[dict[str, str], str]:
    user = str(username or "").strip()
    pwd = str(password or "")
    key = str(api_key or "").strip()
    if key:
        params = {"v": "1.16.1", "c": "pmda", "f": "json", "apiKey": key}
        if user:
            params["u"] = user
        return params, ""
    if not user or not pwd:
        return {}, "Navidrome username/password are required (or API key)"
    salt = uuid.uuid4().hex[:10]
    token = hashlib.md5((pwd + salt).encode("utf-8")).hexdigest()
    return {"u": user, "t": token, "s": salt, "v": "1.16.1", "c": "pmda", "f": "json"}, ""


def check_navidrome(url: str, username: str, password: str, api_key: str = "") -> PlayerResult:
    base = normalize_http_base_url(url)
    if not base:
        return PlayerResult(False, "Navidrome URL is required")
    params, error = navidrome_auth_params(username, password, api_key)
    if error:
        return PlayerResult(False, error)
    try:
        response = requests.get(f"{base}/rest/ping.view", params=params, timeout=10)
        if response.status_code >= 400:
            return PlayerResult(False, f"Navidrome ping failed (HTTP {response.status_code})")
        payload = response.json() if "json" in (response.headers.get("Content-Type") or "").lower() else {}
        sr = payload.get("subsonic-response") if isinstance(payload, dict) else {}
        if (sr or {}).get("status") == "ok":
            return PlayerResult(True, "Navidrome connection successful")
        err_msg = (sr or {}).get("error", {}).get("message") if isinstance(sr, dict) else None
        return PlayerResult(False, err_msg or "Navidrome authentication failed")
    except requests.exceptions.ConnectionError:
        return PlayerResult(False, "Navidrome host unreachable or connection refused")
    except requests.exceptions.Timeout:
        return PlayerResult(False, "Navidrome request timed out")
    except Exception as exc:
        return PlayerResult(False, str(exc))


def trigger_navidrome_refresh(url: str, username: str, password: str, api_key: str = "") -> PlayerResult:
    base = normalize_http_base_url(url)
    if not base:
        return PlayerResult(False, "Navidrome URL is required")
    params, error = navidrome_auth_params(username, password, api_key)
    if error:
        return PlayerResult(False, error)
    try:
        response = requests.get(f"{base}/rest/startScan.view", params=params, timeout=20)
        if response.status_code >= 400:
            return PlayerResult(False, f"Navidrome startScan failed (HTTP {response.status_code})")
        payload = response.json() if "json" in (response.headers.get("Content-Type") or "").lower() else {}
        sr = payload.get("subsonic-response") if isinstance(payload, dict) else {}
        if (sr or {}).get("status") == "ok":
            return PlayerResult(True, "Navidrome library scan triggered")
        err_msg = (sr or {}).get("error", {}).get("message") if isinstance(sr, dict) else None
        return PlayerResult(False, err_msg or "Navidrome scan trigger failed")
    except requests.exceptions.ConnectionError:
        return PlayerResult(False, "Navidrome host unreachable or connection refused")
    except requests.exceptions.Timeout:
        return PlayerResult(False, "Navidrome startScan timed out")
    except Exception as exc:
        return PlayerResult(False, str(exc))
