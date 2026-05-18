"""Album match links, PMDA tag writing, and audio cover embedding helpers."""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

PMDA_ID_TAG = "pmda_id"
PMDA_MATCHED_TAG = "pmda_matched"
PMDA_MATCH_PROVIDER_TAG = "pmda_match_provider"
PMDA_COVER_TAG = "pmda_cover"
PMDA_COVER_PROVIDER_TAG = "pmda_cover_provider"
PMDA_ARTIST_IMAGE_TAG = "pmda_artist_image"
PMDA_ARTIST_PROVIDER_TAG = "pmda_artist_provider"
PMDA_COMPLETE_TAG = "pmda_complete"

MATCH_PROVIDER_ORDER = ["musicbrainz", "discogs", "itunes", "deezer", "spotify", "qobuz", "lastfm", "bandcamp", "audiodb", "tidal"]
MATCH_PROVIDER_LABELS = {
    "musicbrainz": "MusicBrainz",
    "discogs": "Discogs",
    "itunes": "iTunes / Apple Music",
    "deezer": "Deezer",
    "spotify": "Spotify",
    "qobuz": "Qobuz",
    "tidal": "TIDAL",
    "lastfm": "Last.fm",
    "bandcamp": "Bandcamp",
    "fanart": "Fanart.tv",
    "audiodb": "TheAudioDB",
    "wikipedia": "Wikipedia",
    "web": "Web",
    "local": "Local",
    "media_cache": "Media cache",
}


def _default_normalize_provider(provider: str | None) -> str:
    return str(provider or "").strip().lower().replace(" ", "").replace("_", "")


def embed_cover_in_audio_files(cover_path: Path, audio_files: list) -> None:
    """
    Embed the cover image from cover_path into all audio files (MP3, FLAC, MP4).
    Logs errors per file but does not raise.
    """
    if not cover_path or not cover_path.is_file():
        return
    try:
        from mutagen import File as MutagenFile
        from mutagen.id3 import ID3, APIC
        from mutagen.mp3 import MP3
        from mutagen.flac import FLAC, Picture
        from mutagen.mp4 import MP4, MP4Cover
    except ImportError:
        logging.warning("improve-album: mutagen not available for embedding cover")
        return
    data = cover_path.read_bytes()
    suffix = cover_path.suffix.lower()
    if suffix in (".png",):
        mime = "image/png"
        mp4_fmt = MP4Cover.FORMAT_PNG
    else:
        mime = "image/jpeg"
        mp4_fmt = MP4Cover.FORMAT_JPEG
    for audio_file in audio_files:
        try:
            audio = MutagenFile(str(audio_file))
            if audio is None:
                continue
            if isinstance(audio, MP3):
                if audio.tags is None:
                    audio.add_tags(ID3())
                audio.tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=data))
            elif isinstance(audio, FLAC):
                pic = Picture()
                pic.type = 3
                pic.mime = mime
                pic.desc = "front cover"
                pic.data = data
                audio.add_picture(pic)
            elif isinstance(audio, MP4):
                if audio.tags is None:
                    from mutagen.mp4 import MP4Tags
                    audio.add_tags(MP4Tags())
                audio.tags["covr"] = [MP4Cover(data, mp4_fmt)]
            else:
                continue
            audio.save()
        except Exception as e:
            logging.error("improve-album: embed cover failed for %s: %s", audio_file, e)


def normalize_artist_credit_mode(mode: str | None) -> str:
    """Return a safe, normalized artist credit mode."""
    m = (mode or "").strip().lower()
    if m in ("album_artist_strict", "musicbrainz_full_credit", "picard_like_default"):
        return m
    return "album_artist_strict"


def split_main_and_featuring(artist_str: str | None, album_artist: str | None) -> tuple[str, list[str]]:
    """
    Very small heuristic to split an artist credit into main + featuring artists.
    Does not try to be perfect, just enough to avoid losing obvious guest credits.
    """
    base_album_artist = (album_artist or "").strip()
    if not artist_str:
        return (base_album_artist, [])
    s = str(artist_str).strip()
    if not s:
        return (base_album_artist or s, [])
    lower = s.lower()
    seps = [" feat. ", " featuring ", " ft. ", " feat ", " vs ", " with "]
    cut_pos = -1
    sep_len = 0
    for sep in seps:
        idx = lower.find(sep)
        if idx != -1:
            cut_pos = idx
            sep_len = len(sep)
            break
    if cut_pos != -1:
        main = s[:cut_pos].strip()
        rest = s[cut_pos + sep_len :].strip()
    else:
        main = s
        rest = ""
    main_effective = base_album_artist or main
    if base_album_artist and base_album_artist.lower() not in main.lower():
        main_effective = main or base_album_artist
    featuring: list[str] = []
    if rest:
        for part in re.split(r"[,&/]| and ", rest):
            name = part.strip(" -")
            if name:
                featuring.append(name)
    return (main_effective, featuring)


def pmda_bool_from_str(val: str) -> bool:
    v = (val or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def match_provider_label(
    provider: str | None,
    *,
    normalize_provider: Callable[[str | None], str] = _default_normalize_provider,
) -> str:
    p = normalize_provider(str(provider or ""))
    return MATCH_PROVIDER_LABELS.get(p, (p or "unknown").title())


def safe_json_load(value: Any, *, fallback: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return fallback
        try:
            return json.loads(raw)
        except Exception:
            return fallback
    return fallback


def match_attempts_from_steps(
    steps: list[str],
    provider_used: str | None,
    *,
    normalize_provider: Callable[[str | None], str] = _default_normalize_provider,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    used = normalize_provider(provider_used)
    aliases = {
        "musicbrainz": ["musicbrainz", "mbid", "cover art archive", "caa"],
        "discogs": ["discogs"],
        "itunes": ["itunes", "apple music", "applemusic"],
        "deezer": ["deezer"],
        "lastfm": ["last.fm", "lastfm"],
        "bandcamp": ["bandcamp"],
    }
    for provider in MATCH_PROVIDER_ORDER:
        keys = aliases.get(provider, [provider])
        notes = [s for s in (steps or []) if any(k in str(s or "").lower() for k in keys)]
        attempted = bool(notes) or (provider == used)
        if not attempted:
            continue
        out.append(
            {
                "provider": provider,
                "label": match_provider_label(provider, normalize_provider=normalize_provider),
                "attempted": attempted,
                "selected": provider == used,
                "notes": notes[:6],
            }
        )
    if not out and used:
        out.append(
            {
                "provider": used,
                "label": match_provider_label(used, normalize_provider=normalize_provider),
                "attempted": True,
                "selected": True,
                "notes": [],
            }
        )
    return out


def album_match_links(
    *,
    mbid: str,
    musicbrainz_release_id: str = "",
    discogs_release_id: str,
    lastfm_album_mbid: str,
    bandcamp_album_url: str,
    artist_name: str,
    album_title: str,
    normalize_provider: Callable[[str | None], str] = _default_normalize_provider,
    provider_reference_link: Callable[..., str] | None = None,
) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = []
    artist_clean = str(artist_name or "").strip()
    album_clean = str(album_title or "").strip()
    for provider, ref, label in [
        ("musicbrainz", musicbrainz_release_id or mbid, "MusicBrainz release" if str(musicbrainz_release_id or "").strip() else "MusicBrainz release-group"),
        ("discogs", discogs_release_id, "Discogs release"),
        ("lastfm", lastfm_album_mbid, "Last.fm MBID"),
        ("bandcamp", bandcamp_album_url, "Bandcamp album"),
    ]:
        if provider == "musicbrainz" and str(ref or "").strip():
            if str(musicbrainz_release_id or "").strip():
                href = f"https://musicbrainz.org/release/{quote(str(ref or '').strip(), safe='')}"
            else:
                href = f"https://musicbrainz.org/release-group/{quote(str(ref or '').strip(), safe='')}"
        elif provider_reference_link is not None:
            href = provider_reference_link(provider=provider, ref=ref, artist_name=artist_name, album_title=album_title)
        else:
            href = ""
        if href:
            links.append(
                {
                    "provider": normalize_provider(provider),
                    "label": label,
                    "url": href,
                    "release_artist": artist_clean or None,
                    "release_title": album_clean or None,
                    "release_year": None,
                    "provider_ref": str(ref or "").strip() or None,
                }
            )
    return links


def provider_payload_title(provider: str, payload: dict | None) -> str:
    p = _default_normalize_provider(provider)
    data = payload if isinstance(payload, dict) else {}
    if p == "musicbrainz":
        return str(data.get("title") or "").strip()
    if p == "lastfm":
        return str(data.get("title") or data.get("album") or "").strip()
    return str(data.get("title") or "").strip()


def provider_payload_artist(provider: str, payload: dict | None) -> str:
    p = _default_normalize_provider(provider)
    data = payload if isinstance(payload, dict) else {}
    if p == "musicbrainz":
        names = data.get("mb_artist_names") if isinstance(data.get("mb_artist_names"), list) else []
        if names:
            return str(names[0] or "").strip()
        return ""
    if p == "lastfm":
        return str(data.get("artist") or data.get("artist_name") or "").strip()
    return str(data.get("artist_name") or data.get("artist") or "").strip()


def provider_year_from_payload(payload: dict | None) -> int | None:
    data = payload if isinstance(payload, dict) else {}
    raw = str(data.get("year") or "").strip()
    if not raw:
        return None
    match = re.search(r"(19|20)\d{2}", raw)
    if not match:
        return None
    try:
        year = int(match.group(0))
        return year if year > 1800 else None
    except Exception:
        return None


def provider_versions_from_payload(provider: str, payload: dict | None) -> list[dict[str, Any]]:
    p = _default_normalize_provider(provider)
    data = payload if isinstance(payload, dict) else {}
    if p != "musicbrainz":
        return []
    raw = data.get("versions")
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for row in raw[:20]:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("id") or "").strip()
        if not rid:
            continue
        out.append(
            {
                "id": rid,
                "title": str(row.get("title") or "").strip() or None,
                "date": str(row.get("date") or "").strip() or None,
                "country": str(row.get("country") or "").strip() or None,
                "status": str(row.get("status") or "").strip() or None,
                "url": str(row.get("url") or "").strip() or f"https://musicbrainz.org/release/{quote(rid, safe='')}",
            }
        )
    return out


def set_pmda_tag(audio: Any, key: str, value: str) -> None:
    """
    Write a PMDA_* tag to the given Mutagen audio object.
    - key: canonical tag name, e.g. 'PMDA_ID', 'PMDA_COMPLETE'.
    - value: string value to store (UUID, 'true'/'false', provider name, etc.).
    """
    if not value or not isinstance(value, str):
        return
    value = value.strip()
    if not value:
        return
    key_up = key.strip().upper()
    try:
        from mutagen.flac import FLAC  # type: ignore
    except Exception:  # pragma: no cover
        FLAC = None  # type: ignore[assignment]
    if FLAC is not None and isinstance(audio, FLAC):
        audio[key_up] = value
        return
    try:
        from mutagen.id3 import ID3, TXXX  # type: ignore
        from mutagen.mp3 import MP3  # type: ignore
    except Exception:  # pragma: no cover
        ID3 = TXXX = MP3 = None  # type: ignore[assignment]
    if ID3 is not None and isinstance(audio, (MP3, ID3)):
        if audio.tags is None:
            audio.add_tags()
        frame = TXXX(encoding=3, desc=key_up, text=[value])
        audio["TXXX:" + key_up] = frame
        return
    try:
        from mutagen.mp4 import MP4  # type: ignore
    except Exception:  # pragma: no cover
        MP4 = None  # type: ignore[assignment]
    if MP4 is not None and isinstance(audio, MP4):
        audio["----:com.pmda:" + key_up] = [value.encode("utf-8")]
        return
