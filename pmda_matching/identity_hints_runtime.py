"""Local identity hint and filename-pattern helpers."""

from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any


def identity_text_is_generic(value: str) -> bool:
    raw = str(value or "").strip().lower()
    if not raw:
        return True
    if re.fullmatch(r"0*\d{1,3}", raw):
        return True
    return raw in {
        "unknown",
        "unknown artist",
        "unknown album",
        "various",
        "various artists",
        "run source",
        "music",
        "library",
        "album",
        "artist",
    }


def identity_folder_name_looks_like_container(value: str) -> bool:
    raw = " ".join(str(value or "").replace("_", " ").split()).strip()
    if not raw:
        return False
    compact = re.sub(r"\s+", "", raw)
    lowered = raw.casefold()
    if lowered in {
        "run source",
        "incoming",
        "incomming",
        "downloads",
        "music dump",
        "new",
        "misc",
        "temp",
        "tmp",
    }:
        return True
    if re.fullmatch(r"(?:0?[1-9]|1[0-2])[-./](?:\d{2}|\d{4})", compact):
        return True
    if re.fullmatch(r"(?:19|20)\d{2}[-./](?:0?[1-9]|1[0-2])", compact):
        return True
    if re.fullmatch(r"\d{1,2}[-./]\d{1,2}(?:[-./]\d{2,4})?", compact):
        return True
    if re.fullmatch(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}", compact):
        return True
    return False


def identity_strip_track_prefix_artist_artifact(value: str) -> str:
    raw = " ".join(str(value or "").replace("_", " ").split()).strip()
    if not raw:
        return ""
    patterns = (
        r"^(?:0\d)\s+(?P<artist>.+)$",
        r"^(?:0\d)\s*[-.]+\s*(?P<artist>.+)$",
        r"^(?:0?\d{1,2}\s*[-.]\s*0?\d{1,2})(?:\s*[-.]+\s*|\s+)(?P<artist>.+)$",
    )
    for pattern in patterns:
        match = re.match(pattern, raw)
        if not match:
            continue
        candidate = " ".join(str(match.group("artist") or "").replace("_", " ").split()).strip(" -._")
        if candidate:
            return candidate
    return ""


def identity_artist_fallback_candidate(value: str) -> str:
    raw = " ".join(str(value or "").replace("_", " ").split()).strip()
    if not raw:
        return ""
    candidate = identity_strip_track_prefix_artist_artifact(raw) or raw
    if identity_text_is_generic(candidate):
        return ""
    if identity_folder_name_looks_like_container(candidate):
        return ""
    if not any(ch.isalpha() for ch in candidate):
        return ""
    return candidate


def identity_artist_fallback_is_usable(value: str) -> bool:
    raw = " ".join(str(value or "").replace("_", " ").split()).strip()
    if not raw:
        return False
    if identity_text_is_generic(raw):
        return False
    if identity_folder_name_looks_like_container(raw):
        return False
    if identity_strip_track_prefix_artist_artifact(raw):
        return False
    if not any(ch.isalpha() for ch in raw):
        return False
    return True


def identity_album_fallback_is_usable_for_runtime(
    runtime: Any,
    value: str,
    *,
    missing_required: list[str] | tuple[str, ...] | set[str] | None = None,
    folder_name: str = "",
) -> bool:
    raw = runtime._sanitize_album_title_display(str(value or "").replace("_", " ").strip())
    if not raw:
        return False
    if identity_text_is_generic(raw):
        return False
    missing = {str(item or "").strip().lower() for item in (missing_required or []) if str(item or "").strip()}
    if "album" in missing:
        return False
    folder_txt = runtime._sanitize_album_title_display(str(folder_name or "").replace("_", " ").strip())
    if folder_txt and identity_folder_name_looks_like_container(folder_txt):
        if runtime._normalize_identity_text_strict(folder_txt) == runtime._normalize_identity_text_strict(raw):
            return False
    return True


def should_try_local_context_identity_ai_for_runtime(
    runtime: Any,
    *,
    local_artist: str,
    local_album: str,
    folder_name: str = "",
    missing_required_tags: list[str] | tuple[str, ...] | set[str] | None = None,
    force_try: bool = False,
) -> bool:
    if force_try:
        return True
    artist_usable = identity_artist_fallback_is_usable(local_artist)
    album_usable = identity_album_fallback_is_usable_for_runtime(
        runtime,
        local_album,
        missing_required=missing_required_tags,
        folder_name=folder_name,
    )
    if artist_usable and album_usable:
        return False
    return bool((not artist_usable) or (not album_usable))


def edition_missing_required_tags_set(edition: dict | None) -> set[str]:
    raw = []
    if isinstance(edition, dict):
        raw = edition.get("missing_required_tags") or []
    if isinstance(raw, tuple):
        raw = list(raw)
    if not isinstance(raw, list):
        raw = []
    out: set[str] = set()
    for item in raw:
        txt = str(item or "").strip().lower()
        if txt:
            out.add(txt)
    return out


def edition_has_verified_provider_identity_for_runtime(runtime: Any, edition: dict | None) -> bool:
    item = edition if isinstance(edition, dict) else {}
    provider = runtime._normalize_identity_provider(
        str(
            item.get("identity_provider")
            or item.get("metadata_source")
            or item.get("primary_metadata_source")
            or ""
        )
    )
    return bool(
        item.get("strict_match_verified")
        or item.get("soft_match_verified")
        or item.get("provider_identity_soft_match")
        or str(item.get("musicbrainz_id") or "").strip()
        or str(item.get("discogs_release_id") or "").strip()
        or str(item.get("lastfm_album_mbid") or "").strip()
        or str(item.get("bandcamp_album_url") or "").strip()
        or provider in {"musicbrainz", "discogs", "lastfm", "bandcamp"}
    )


def identity_hint_safe_for_provider_lookup(
    edition: dict | None,
    *,
    default_artist: str = "",
    default_title: str = "",
) -> bool:
    item = edition if isinstance(edition, dict) else {}
    hint = item.get("_lookup_identity_hint") if isinstance(item.get("_lookup_identity_hint"), dict) else {}
    hint_source = str(hint.get("source") or "").strip().lower()
    try:
        hint_confidence = int(float(hint.get("confidence") or 0))
    except Exception:
        hint_confidence = 0
    if hint_source == "filename_pattern":
        return hint_confidence >= 90
    if hint_source != "ai_local_context":
        return False
    missing_required = edition_missing_required_tags_set(item)
    current_artist = str(
        item.get("artist")
        or item.get("artist_name")
        or default_artist
        or ""
    ).strip()
    current_title = str(
        item.get("title_raw")
        or item.get("album_title")
        or default_title
        or ""
    ).strip()
    return bool(
        "artist" in missing_required
        or "album" in missing_required
        or identity_text_is_generic(current_artist)
        or identity_text_is_generic(current_title)
    )


def prefer_identity_hint_value_for_runtime(
    runtime: Any,
    *,
    current_value: str,
    hinted_value: str,
    field_name: str,
    missing_required: set[str],
    folder_name: str = "",
) -> str:
    current_txt = str(current_value or "").strip()
    hinted_txt = str(hinted_value or "").strip()
    if not hinted_txt or identity_text_is_generic(hinted_txt):
        return current_txt
    if identity_text_is_generic(current_txt):
        return hinted_txt
    if field_name in missing_required:
        return hinted_txt
    current_norm = runtime._normalize_identity_text_strict(current_txt)
    hinted_norm = runtime._normalize_identity_text_strict(hinted_txt)
    if current_norm and hinted_norm and current_norm == hinted_norm:
        return runtime._choose_preferred_identity_display(current_txt, hinted_txt)
    if field_name == "album":
        folder_txt = runtime._sanitize_album_title_display(str(folder_name or "").replace("_", " ").strip())
        if folder_txt:
            folder_norm = runtime._normalize_identity_text_strict(folder_txt)
            if current_norm and folder_norm and current_norm == folder_norm:
                return hinted_txt
    return current_txt


def apply_resolved_identity_to_edition_for_runtime(
    runtime: Any,
    edition: dict | None,
    *,
    default_artist: str = "",
    default_title: str = "",
    folder_name: str = "",
) -> tuple[str, str]:
    if not isinstance(edition, dict):
        return (
            str(default_artist or "").strip() or "Unknown Artist",
            runtime._sanitize_album_title_display(str(default_title or "").strip() or "Unknown Album"),
        )
    artist_final, album_final = runtime._resolve_edition_display_identity(
        edition,
        default_artist=default_artist,
        default_title=default_title,
        folder_name=folder_name,
    )
    edition["artist"] = artist_final
    edition["artist_name"] = artist_final
    edition["title_raw"] = album_final
    edition["album_title"] = album_final
    return (artist_final, album_final)


def album_hint_from_track_titles_for_runtime(runtime: Any, track_titles: list[str]) -> str:
    """Extract a likely album-title prefix from filename-like track titles."""
    prefixes: list[str] = []
    for title in track_titles or []:
        item = str(title or "").strip()
        if not item:
            continue
        match = re.match(r"^\s*(.+?)\s*[-–—]\s*\d{1,3}\s*[-–—]\s*.+$", item)
        if not match:
            continue
        prefix = str(match.group(1) or "").strip(" -_")
        if prefix:
            prefixes.append(prefix)
    if not prefixes:
        return ""
    counts: dict[str, int] = {}
    for prefix in prefixes:
        key = runtime._normalize_identity_text_strict(prefix)
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
    if not counts:
        return ""
    best_key = max(counts.keys(), key=lambda item: counts[item])
    best_count = counts.get(best_key, 0)
    if best_count < max(2, int(math.ceil(len(prefixes) * 0.45))):
        return ""
    for prefix in prefixes:
        if runtime._normalize_identity_text_strict(prefix) == best_key:
            return prefix.strip()
    return ""


def filename_identity_hints_for_runtime(runtime: Any, file_paths: list[Path | str] | None) -> dict[str, str]:
    """Infer stable artist/album hints from filename patterns."""
    artist_candidates: list[str] = []
    album_candidates: list[str] = []
    for raw in (file_paths or [])[:80]:
        try:
            path = raw if isinstance(raw, Path) else Path(str(raw))
        except Exception:
            continue
        stem = str(path.stem or "").strip()
        if not stem:
            continue
        parts = [segment.strip() for segment in re.split(r"\s*[-–—]\s*", stem) if str(segment or "").strip()]
        if len(parts) < 4:
            continue
        track_marker = parts[2]
        if not re.match(r"^(?:\d{1,3}|\d{1,2}\s*[-_.]\s*\d{1,2})$", track_marker):
            continue
        artist_guess = str(parts[0] or "").strip(" -_")
        album_guess = str(parts[1] or "").strip(" -_")
        if artist_guess:
            artist_candidates.append(artist_guess)
        if album_guess:
            album_candidates.append(album_guess)

    def pick(values: list[str], *, min_hits: int = 3, min_ratio: float = 0.55) -> str:
        if not values:
            return ""
        counts: dict[str, int] = {}
        display: dict[str, str] = {}
        for value in values:
            key = runtime._normalize_identity_text_strict(value)
            if not key:
                continue
            counts[key] = counts.get(key, 0) + 1
            display.setdefault(key, value)
        if not counts:
            return ""
        best_key = max(counts.keys(), key=lambda item: counts[item])
        hits = int(counts.get(best_key) or 0)
        if hits < max(1, int(min_hits)):
            return ""
        if (hits / max(1, len(values))) < float(min_ratio):
            return ""
        return str(display.get(best_key) or "").strip()

    return {
        "artist": identity_artist_fallback_candidate(pick(artist_candidates)),
        "album": pick(album_candidates),
    }
