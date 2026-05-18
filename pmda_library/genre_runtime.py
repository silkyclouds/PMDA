"""Genre normalization and payload backfill helpers for files-mode library views."""
from __future__ import annotations

import ast
import json
import re
from typing import Any


def _split_genre_values(raw_value: str) -> list[str]:
    if not raw_value:
        return []
    text = str(raw_value or "").strip()
    if not text:
        return []
    if (text.startswith("[") and text.endswith("]")) or (text.startswith("(") and text.endswith(")")):
        for loader in (json.loads, ast.literal_eval):
            try:
                parsed = loader(text)
                if isinstance(parsed, (list, tuple, set)):
                    nested: list[str] = []
                    for item in parsed:
                        nested.extend(_split_genre_values(str(item or "")))
                    if nested:
                        return nested
            except Exception:
                pass
    normalized = text.replace("\\n", ";").replace("\n", ";")
    normalized = re.sub(r"^[\\[\\](){}\\s'\"`]+|[\\[\\](){}\\s'\"`]+$", "", normalized)
    parts = re.split(r"\s*(?:[;,/|]|\s*,\s*)\s*", normalized)
    out = []
    for part in parts:
        value = re.sub(r"\s+", " ", (part or "").strip(" [](){}'\"`"))
        if value:
            out.append(value)
    return out


def _merge_album_genre_lists(*values: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def _push(raw: str) -> None:
        clean = re.sub(r"\s+", " ", str(raw or "").strip())
        if not clean:
            return
        key = clean.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(clean)

    for value in values:
        if isinstance(value, list):
            for item in value:
                _push(str(item or ""))
            continue
        if isinstance(value, str):
            text = value.strip()
            if not text:
                continue
            if text.startswith("[") and text.endswith("]"):
                try:
                    parsed = json.loads(text)
                except Exception:
                    parsed = None
                if isinstance(parsed, list):
                    for item in parsed:
                        _push(str(item or ""))
                    continue
            for item in _split_genre_values(text):
                _push(item)
    return out[:20]


def _infer_genre_from_bandcamp_tags(tags: list[str] | tuple[str, ...]) -> str | None:
    """
    Infer a semicolon-separated genre string from Bandcamp tag lists.

    Location and marketplace tags are filtered first, but if every token is
    filtered we keep the original cleaned tags so the release is not left blank.
    """
    if not tags:
        return None

    cleaned_raw: list[str] = []
    seen_lower: set[str] = set()
    for tag in tags:
        raw = (tag or "").strip()
        if not raw:
            continue
        normalized = re.sub(r"\s+", " ", raw).strip()
        low = normalized.lower()
        if low in seen_lower:
            continue
        seen_lower.add(low)
        cleaned_raw.append(normalized)

    if not cleaned_raw:
        return None

    blacklist_exact = {
        "new mexico",
        "usa",
        "us",
        "uk",
        "united states",
        "united kingdom",
        "france",
        "germany",
        "italy",
        "spain",
        "japan",
        "canada",
        "london",
        "paris",
        "berlin",
        "new york",
        "los angeles",
        "vinyl",
        "cassette",
        "cd",
        "digital",
        "download",
        "album",
        "ep",
        "lp",
        "single",
    }
    genre_candidates: list[str] = []
    for raw in cleaned_raw:
        low = raw.lower()
        if low in blacklist_exact:
            continue
        if len(low) < 2:
            continue
        if re.fullmatch(r"[0-9\W_]+", low):
            continue
        genre_candidates.append(low)

    if not genre_candidates:
        genre_candidates = [value.lower() for value in cleaned_raw]

    return "; ".join(genre_candidates[:6])


def _dominant_genre_by_artist(albums_payload: list[dict]) -> dict[str, str]:
    """Compute the dominant genre per artist from already-discovered album payloads."""
    counts: dict[str, dict[str, int]] = {}
    canon: dict[str, dict[str, str]] = {}
    for album in albums_payload or []:
        artist_norm = str(album.get("artist_norm") or "").strip()
        if not artist_norm:
            continue

        tokens: list[str] = []
        genre_raw = str(album.get("genre") or "").strip()
        if genre_raw:
            tokens.extend(_split_genre_values(genre_raw))
        else:
            try:
                tags_raw = album.get("tags_json") or "[]"
                parsed = json.loads(tags_raw) if isinstance(tags_raw, str) else (tags_raw or [])
                if isinstance(parsed, list):
                    tokens.extend([str(item or "").strip() for item in parsed if str(item or "").strip()])
            except Exception:
                pass
        if not tokens:
            continue
        inferred = _infer_genre_from_bandcamp_tags(tokens)
        chosen = str(inferred or (tokens[0] if tokens else "")).strip()
        if not chosen:
            continue
        key = chosen.lower()
        counts.setdefault(artist_norm, {})[key] = int(counts.setdefault(artist_norm, {}).get(key, 0)) + 1
        canon.setdefault(artist_norm, {})[key] = chosen

    out: dict[str, str] = {}
    for artist_norm, genre_counts in counts.items():
        if not genre_counts:
            continue
        key = max(genre_counts.items(), key=lambda item: (int(item[1]), item[0]))[0]
        value = str(canon.get(artist_norm, {}).get(key) or "").strip()
        if value:
            out[artist_norm] = value
    return out


def _apply_genre_defaults_to_albums_payload(albums_payload: list[dict]) -> None:
    """Backfill missing genre from artist dominant genre and keep missing-required tags in sync."""
    if not albums_payload:
        return
    dominant = _dominant_genre_by_artist(albums_payload)
    for album in albums_payload:
        artist_norm = str(album.get("artist_norm") or "").strip()
        if artist_norm and not str(album.get("genre") or "").strip():
            fallback = str(dominant.get(artist_norm) or "").strip()
            if fallback:
                album["genre"] = fallback
                try:
                    tags_raw = album.get("tags_json") or "[]"
                    tags = json.loads(tags_raw) if isinstance(tags_raw, str) else (tags_raw or [])
                    if not isinstance(tags, list):
                        tags = []
                    if fallback.lower() not in {str(tag or "").strip().lower() for tag in tags}:
                        tags.insert(0, fallback)
                        album["tags_json"] = json.dumps(tags[:20])
                except Exception:
                    pass
                try:
                    primary_raw = album.get("primary_tags_json") or "{}"
                    primary = json.loads(primary_raw) if isinstance(primary_raw, str) else (primary_raw or {})
                    if isinstance(primary, dict) and not str(primary.get("genre") or "").strip():
                        primary["genre"] = fallback
                        album["primary_tags_json"] = json.dumps(primary, default=str)
                except Exception:
                    pass

        try:
            missing_raw = album.get("missing_required_tags_json") or "[]"
            missing = json.loads(missing_raw) if isinstance(missing_raw, str) else (missing_raw or [])
            if not isinstance(missing, list):
                missing = []
        except Exception:
            missing = []
        filtered = [item for item in missing if str(item or "").strip().lower() != "genre"]
        album["missing_required_tags_json"] = json.dumps(filtered)


def _split_genre_values_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    return _split_genre_values(*args, **kwargs)


def _merge_album_genre_lists_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    return _merge_album_genre_lists(*args, **kwargs)


def _infer_genre_from_bandcamp_tags_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    return _infer_genre_from_bandcamp_tags(*args, **kwargs)


def _dominant_genre_by_artist_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    return _dominant_genre_by_artist(*args, **kwargs)


def _apply_genre_defaults_to_albums_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    return _apply_genre_defaults_to_albums_payload(*args, **kwargs)
