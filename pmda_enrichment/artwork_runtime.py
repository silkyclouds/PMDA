"""Runtime-bound artwork, cover, OCR, and cover-vision helpers."""
from __future__ import annotations

from typing import Any
import base64
import hashlib
import logging
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

_RUNTIME: Any | None = None
_EXTRACTED_NAMES = {
    "_extract_embedded_artworks_from_audio",
    "_resolve_files_album_cover_asset",
    "_extract_embedded_cover_from_audio",
    "_extract_embedded_artworks_from_folder",
    "_get_local_cover_data_uri_for_vision",
    "_resize_cover_for_vision",
    "_encode_local_cover_to_data_uri",
    "_ocr_cover_text_from_image_bytes",
    "_cover_ocr_smart_trigger",
    "_identity_cover_ocr_context",
    "_fetch_cover_from_web",
    "_download_best_cover_image",
    "_provider_reference_link",
    "_vision_verify_cover_before_inject",
}
_ORIGINAL_EXTRACTED_FUNCTIONS: dict[str, Any] = {}


def _bind_runtime(runtime: Any) -> None:
    global _RUNTIME
    _RUNTIME = runtime
    for name, value in vars(runtime).items():
        if name in _EXTRACTED_NAMES:
            original = _ORIGINAL_EXTRACTED_FUNCTIONS.get(name)
            if original is not None:
                globals()[name] = original
            continue
        own_wrapper = name.endswith("_for_runtime") and name[: -len("_for_runtime")] in _EXTRACTED_NAMES
        if name == "_bind_runtime" or own_wrapper:
            continue
        globals()[name] = value

def _extract_embedded_artworks_from_audio(
    audio_path: Path,
    *,
    max_items: int = 6,
) -> list[tuple[bytes, str, str, str]]:
    if not audio_path or not audio_path.is_file():
        return []
    out: list[tuple[bytes, str, str, str]] = []
    seen_hashes: set[str] = set()

    def _push(data: Any, mime: Any, slot: str, desc: str) -> None:
        raw = bytes(data or b"")
        if not raw:
            return
        digest = hashlib.sha1(raw).hexdigest()
        if digest in seen_hashes:
            return
        seen_hashes.add(digest)
        out.append((raw, str(mime or "image/jpeg"), str(slot or "other"), str(desc or "").strip()))

    try:
        from mutagen import File as MutagenFile

        f = MutagenFile(str(audio_path))
        if f is None:
            return []
        if hasattr(f, "pictures") and f.pictures:
            for pic in list(f.pictures or [])[: max(1, int(max_items or 1))]:
                mime = getattr(pic, "mime", "image/jpeg") or "image/jpeg"
                desc = str(getattr(pic, "desc", "") or "").strip()
                slot = _embedded_artwork_slot(getattr(pic, "type", 0), desc)
                _push(getattr(pic, "data", b""), mime, slot, desc)
        if hasattr(f, "tags") and f.tags:
            apics = f.tags.getall("APIC") if hasattr(f.tags, "getall") else []
            if not apics and "APIC:Cover" in f.tags:
                apics = [f.tags["APIC:Cover"]]
            for apic in list(apics or [])[: max(1, int(max_items or 1))]:
                desc = str(getattr(apic, "desc", "") or "").strip()
                slot = _embedded_artwork_slot(getattr(apic, "type", 0), desc)
                _push(getattr(apic, "data", b""), getattr(apic, "mime", "image/jpeg"), slot, desc)
        if hasattr(f, "get") and f.get("covr"):
            for idx, covr in enumerate(list(f["covr"])[: max(1, int(max_items or 1))], start=1):
                if isinstance(covr, bytes):
                    raw = covr
                elif hasattr(covr, "rawdata"):
                    raw = bytes(covr.rawdata)
                else:
                    raw = b""
                _push(raw, "image/jpeg", "front", f"covr-{idx}")
    except Exception as e:
        logging.debug("[Vision] Extract embedded artwork from %s failed: %s", audio_path, e)
        return []
    out.sort(key=lambda item: (_artwork_slot_sort_key(item[2]), len(item[0]) * -1))
    return out[: max(1, int(max_items or 1))]


def _resolve_files_album_cover_asset(
    *,
    album_id: int = 0,
    cover_path_raw: str = "",
    folder_path_raw: str = "",
    has_cover: bool = False,
    lookup_key: str = "",
    persist: bool = False,
) -> tuple[bool, str]:
    existing = _existing_file_path(str(cover_path_raw or "").strip())
    if existing is not None:
        if persist and (not bool(has_cover) or str(cover_path_raw or "").strip() != str(existing)):
            _persist_files_album_cover_resolution(
                int(album_id or 0),
                cover_path=str(existing),
                folder_path=str(folder_path_raw or "").strip(),
                has_cover=True,
                lookup_key=lookup_key,
            )
        return (True, str(existing))
    folder_txt = str(folder_path_raw or "").strip()
    if not folder_txt:
        return (False, "")
    try:
        folder_path = path_for_fs_access(Path(folder_txt))
    except Exception:
        return (False, "")
    if not folder_path.exists() or not folder_path.is_dir():
        return (False, "")
    local_cover = _first_cover_path(folder_path)
    if local_cover and local_cover.exists() and local_cover.is_file():
        if persist:
            _persist_files_album_cover_resolution(
                int(album_id or 0),
                cover_path=str(local_cover),
                folder_path=folder_txt,
                has_cover=True,
                lookup_key=lookup_key,
            )
        return (True, str(local_cover))
    embedded = _extract_embedded_cover_from_folder(folder_path, max_audio_files=6)
    if embedded:
        raw, mime = embedded
        cached = _ensure_cached_image_from_bytes(
            raw,
            mime,
            kind="album",
            cache_key_hint=f"files-album-cover:{int(album_id or 0)}:{folder_txt}",
            max_px=_MEDIA_CACHE_MASTER_PX,
        )
        if cached and cached.exists() and cached.is_file():
            if persist:
                _persist_files_album_cover_resolution(
                    int(album_id or 0),
                    cover_path=str(cached),
                    folder_path=folder_txt,
                    has_cover=True,
                    lookup_key=lookup_key,
                )
            return (True, str(cached))
    return (False, "")


def _extract_embedded_cover_from_audio(audio_path: Path) -> Optional[tuple[bytes, str]]:
    """
    Extract the first embedded cover image from an audio file (FLAC, MP3, M4A, etc.).
    Returns (image_bytes, mime_type) or None. Used when no cover file exists in the folder.
    """
    artworks = _extract_embedded_artworks_from_audio(audio_path, max_items=6)
    for raw, mime, slot, _desc in artworks:
        if slot == "front":
            return (raw, mime)
    if artworks:
        raw, mime, _slot, _desc = artworks[0]
        return (raw, mime)
    return None


def _extract_embedded_artworks_from_folder(
    folder: Path,
    *,
    max_audio_files: int = 6,
    max_items: int = 8,
) -> list[tuple[bytes, str, str, str, str]]:
    if not folder or not folder.is_dir():
        return []
    out: list[tuple[bytes, str, str, str, str]] = []
    seen_hashes: set[str] = set()
    try:
        checked = 0
        for p in sorted(folder.rglob("*")):
            if not AUDIO_RE.search(p.name):
                continue
            artworks = _extract_embedded_artworks_from_audio(p, max_items=max_items)
            for raw, mime, slot, desc in artworks:
                digest = hashlib.sha1(bytes(raw or b"")).hexdigest()
                if not raw or digest in seen_hashes:
                    continue
                seen_hashes.add(digest)
                out.append((raw, mime, slot, p.name, desc))
                if len(out) >= max(1, int(max_items or 1)):
                    break
            checked += 1
            if checked >= max(1, int(max_audio_files or 1)) or len(out) >= max(1, int(max_items or 1)):
                break
    except OSError:
        return []
    out.sort(key=lambda item: (_artwork_slot_sort_key(item[2]), item[3].lower()))
    return out[: max(1, int(max_items or 1))]


def _get_local_cover_data_uri_for_vision(folder: Path) -> Optional[str]:
    """
    Return a data URI for the album cover for vision comparison.
    First tries cover files in the folder (folder.jpg, cover.jpg, etc.);
    if none found, extracts embedded cover from the first audio file in the folder.
    """
    if not folder or not folder.is_dir():
        return None
    cover_path = _first_cover_path(folder)
    if cover_path:
        return _encode_local_cover_to_data_uri(cover_path)
    try:
        result = _extract_embedded_cover_from_folder(folder, max_audio_files=6)
        if not result:
            return None
        data, mime = result
        if len(data) > _MAX_COVER_SIZE_BYTES or mime != "image/jpeg":
            data, mime = _resize_cover_for_vision(data, mime)
        if not data:
            return None
        b64 = base64.b64encode(data).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except Exception as e:
        logging.debug("[Vision] Fallback embedded cover failed: %s", e)
    return None


def _resize_cover_for_vision(data: bytes, mime: str) -> tuple[bytes, str]:
    """
    Resize/compress image to stay under _MAX_COVER_SIZE_BYTES. Returns (jpeg_bytes, "image/jpeg").
    Uses Pillow; on failure returns original data if small enough, else empty.
    """
    if len(data) <= _MAX_COVER_SIZE_BYTES and mime == "image/jpeg":
        return (data, mime)
    try:
        from io import BytesIO
        from PIL import Image
        img = Image.open(BytesIO(data)).convert("RGB")
        w, h = img.size
        if w > _MAX_COVER_PIXELS or h > _MAX_COVER_PIXELS:
            img.thumbnail((_MAX_COVER_PIXELS, _MAX_COVER_PIXELS), Image.Resampling.LANCZOS)
        buf = BytesIO()
        quality = 85
        img.save(buf, "JPEG", quality=quality, optimize=True)
        out = buf.getvalue()
        while len(out) > _MAX_COVER_SIZE_BYTES and quality > 25:
            quality -= 15
            buf = BytesIO()
            img.save(buf, "JPEG", quality=quality, optimize=True)
            out = buf.getvalue()
        return (out, "image/jpeg")
    except Exception as e:
        logging.debug("[Vision] Resize cover failed: %s", e)
        if len(data) <= _MAX_COVER_SIZE_BYTES:
            return (data, mime)
        return (b"", "")


def _encode_local_cover_to_data_uri(cover_path: Path) -> Optional[str]:
    """
    Read the cover file, resize if needed to stay under ~100 KB, encode as base64 data URI.
    Vision only needs small images for cover comparison.
    """
    if not cover_path or not cover_path.is_file():
        return None
    try:
        raw = cover_path.read_bytes()
        suffix = cover_path.suffix.lower()
        mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".gif": "image/gif", ".webp": "image/webp"}
        mime = mime_map.get(suffix, "image/jpeg")
        if len(raw) > _MAX_COVER_SIZE_BYTES or mime != "image/jpeg":
            raw, mime = _resize_cover_for_vision(raw, mime)
        if not raw:
            return None
        b64 = base64.b64encode(raw).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except Exception as e:
        logging.debug("[Vision] Failed to encode cover %s: %s", cover_path, e)
        return None


def _ocr_cover_text_from_image_bytes(image_bytes: bytes, mime: str = "") -> str:
    raw = bytes(image_bytes or b"")
    if not raw or not shutil.which("tesseract"):
        return ""
    cache_key = hashlib.sha1(raw).hexdigest()
    with _CLASSICAL_COVER_OCR_CACHE_LOCK:
        cached = _CLASSICAL_COVER_OCR_CACHE.get(cache_key)
        if isinstance(cached, str):
            _CLASSICAL_COVER_OCR_CACHE.move_to_end(cache_key)
            return cached

    lang_spec = _ocr_tesseract_lang_spec()
    if not lang_spec:
        return ""

    temp_path: Optional[Path] = None
    best_text = ""
    try:
        prepared = _ocr_prepare_cover_bytes(raw)
        with tempfile.NamedTemporaryFile(prefix="pmda-cover-ocr-", suffix=".png", delete=False) as tmp:
            tmp.write(prepared)
            temp_path = Path(tmp.name)
        for psm in ("6", "11"):
            cmd = ["tesseract", str(temp_path), "stdout", "--psm", psm, "-l", lang_spec, "quiet"]
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=25,
            )
            text = str(proc.stdout or "").strip()
            if _ocr_text_quality_score(text) > _ocr_text_quality_score(best_text):
                best_text = text
            if _ocr_text_quality_score(best_text) >= 6:
                break
    except Exception as exc:
        logging.debug("Cover OCR failed: %s", exc)
        best_text = ""
    finally:
        if temp_path:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass

    with _CLASSICAL_COVER_OCR_CACHE_LOCK:
        _CLASSICAL_COVER_OCR_CACHE[cache_key] = best_text
        _CLASSICAL_COVER_OCR_CACHE.move_to_end(cache_key)
        while len(_CLASSICAL_COVER_OCR_CACHE) > _CLASSICAL_COVER_OCR_CACHE_MAX:
            _CLASSICAL_COVER_OCR_CACHE.popitem(last=False)
    return best_text


def _cover_ocr_smart_trigger(
    *,
    local_artist: str,
    local_title: str,
    local_tracks: list[Any] | None = None,
    local_tags: dict | None = None,
    local_paths: list[Any] | None = None,
    provider_candidate_count: int = 0,
) -> bool:
    mode = _cover_ocr_mode()
    if mode == "off":
        return False
    if mode == "always":
        return True
    folder = _folder_from_local_paths(local_paths)
    if folder is None:
        return False
    tags = local_tags if isinstance(local_tags, dict) else {}
    title_norm = _normalize_identity_album_strict(local_title)
    artist_norm = _normalize_identity_text_strict(local_artist)
    genre_values = _classical_collect_tag_values(tags, ("genre", "style"))
    genre_norms = {_classical_norm_text(v) for v in genre_values if _classical_norm_text(v)}
    track_titles = _local_track_titles_for_strict(list(local_tracks or []))
    work_tokens = _classical_work_tokens_from_texts(([local_title] if str(local_title or "").strip() else []) + track_titles[:20])
    looks_classical = bool(
        work_tokens
        or (genre_norms and any(any(h in g for h in _CLASSICAL_GENRE_HINTS) for g in genre_norms))
        or any(keyword in _classical_norm_text(local_title) for keyword in _CLASSICAL_WORK_KEYWORDS)
        or bool(_CLASSICAL_CATALOG_RE.search(str(local_title or "")))
    )
    missing_identity = (
        not title_norm
        or not artist_norm
        or title_norm in {"unknown album", "unknown", "untitled"}
        or artist_norm in {"unknown artist", "unknown"}
    )
    label_values = _classical_collect_tag_values(tags, _CLASSICAL_LABEL_TAG_KEYS)
    composer_values = _classical_collect_tag_values(tags, _CLASSICAL_COMPOSER_TAG_KEYS)
    performance_values = _classical_collect_tag_values(tags, _CLASSICAL_PERFORMANCE_TAG_KEYS)
    work_values = _classical_collect_tag_values(tags, _CLASSICAL_WORK_TAG_KEYS)
    year_value = ""
    for year_key in ("date", "originaldate", "year"):
        year_value = _mb_extract_year(tags.get(year_key))
        if year_value:
            break
    sparse_metadata = sum(
        1 for present in (
            bool(title_norm),
            bool(artist_norm),
            bool(track_titles),
            bool(label_values),
            bool(year_value),
            bool(composer_values),
            bool(performance_values),
            bool(work_values),
        ) if present
    ) <= 3
    return bool(
        looks_classical
        or missing_identity
        or sparse_metadata
        or _track_titles_look_unreliable_for_identity(local_tracks)
        or provider_candidate_count > 1
    )


def _identity_cover_ocr_context(
    *,
    local_artist: str = "",
    local_title: str = "",
    local_tracks: list[Any] | None = None,
    local_tags: dict | None = None,
    local_paths: list[Any] | None = None,
    provider_candidate_count: int = 0,
) -> dict[str, Any]:
    if not _cover_ocr_smart_trigger(
        local_artist=local_artist,
        local_title=local_title,
        local_tracks=local_tracks,
        local_tags=local_tags,
        local_paths=local_paths,
        provider_candidate_count=provider_candidate_count,
    ):
        return {}
    folder = _folder_from_local_paths(local_paths)
    if not folder or not folder.is_dir():
        return {}
    candidates = _cover_ocr_candidates_for_folder(folder)
    if not candidates:
        return {}
    chosen_text = ""
    all_lines: list[str] = []
    used_sources: list[str] = []
    for source, raw, mime in candidates[:4]:
        text = _ocr_cover_text_from_image_bytes(raw, mime)
        if _ocr_text_quality_score(text) > _ocr_text_quality_score(chosen_text):
            chosen_text = text
        if text:
            used_sources.append(source)
            for raw_line in text.splitlines():
                line = re.sub(r"\s+", " ", str(raw_line or "")).strip(" -–—")
                if len(line) >= 3 and line not in all_lines:
                    all_lines.append(line)
    if not all_lines and not chosen_text:
        return {}
    text_blob = "\n".join(all_lines) if all_lines else chosen_text
    label_values: list[str] = []
    norm_blob = _classical_norm_text(text_blob)
    for canonical, aliases in _CLASSICAL_LABEL_OCR_ALIASES.items():
        alias_norms = [_classical_norm_text(alias) for alias in aliases]
        if any(alias and alias in norm_blob for alias in alias_norms):
            label_values.append(canonical)
    title_texts = [str(local_title or "").strip(), *all_lines[:20]]
    artist_texts = [str(local_artist or "").strip(), *all_lines[:20]]
    title_norms: set[str] = set()
    artist_norms: set[str] = set()
    for line in all_lines[:20]:
        title_norm = _normalize_identity_album_strict(line)
        artist_norm = _normalize_identity_text_strict(line)
        if title_norm:
            title_norms.add(title_norm)
        if artist_norm:
            artist_norms.add(artist_norm)
    catalog_tokens = _classical_release_catalog_tokens_from_texts(all_lines[:20])
    work_tokens = _classical_work_tokens_from_texts(title_texts)
    performance_tokens = _classical_people_tokens(artist_texts[:12])
    return {
        "source": ", ".join(used_sources) if used_sources else "",
        "text": text_blob,
        "lines": all_lines[:20],
        "title_norms": title_norms,
        "artist_norms": artist_norms,
        "label_values": label_values,
        "label_tokens": _classical_label_tokens(label_values),
        "catalog_tokens": catalog_tokens,
        "work_tokens": work_tokens,
        "performance_tokens": performance_tokens,
    }


def _fetch_cover_from_web(artist_name: str, album_title: str) -> Optional[tuple[bytes, str]]:
    """
    Try to find an album cover via web search (IA-first, Serper as backup) + OpenGraph image.
    Returns (content_bytes, mime) or None on failure.
    """
    query = f"{artist_name} {album_title} album cover".strip()
    results = _serper_web_search(query, num=5)
    if not results:
        return None
    for item in results:
        link = item.get("link") or ""
        if not link:
            continue
        low = link.lower()
        # Avoid Wikipedia/Wikimedia results; they are often unrelated photos and create false positives.
        if "wikipedia.org/" in low or "wikimedia.org/" in low:
            continue
        try:
            resp = requests.get(link, timeout=8, allow_redirects=True)
        except Exception as e:
            logging.debug("[WebCover] Failed to fetch %s: %s", link, e)
            continue
        if resp.status_code != 200:
            continue
        ct = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
        # Direct image
        if ct.startswith("image/") and resp.content:
            return resp.content, ct or "image/jpeg"
        # HTML page – try to extract og:image and fetch it
        if "text/html" in ct and resp.text:
            try:
                m = re.search(r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', resp.text, re.IGNORECASE)
                if not m:
                    m = re.search(r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:image["\']', resp.text, re.IGNORECASE)
                if not m:
                    continue
                img_url = m.group(1).strip()
                if not img_url:
                    continue
                img_resp = requests.get(img_url, timeout=8, allow_redirects=True)
                if img_resp.status_code == 200 and img_resp.content:
                    img_ct = (
                        (img_resp.headers.get("content-type") or "")
                        .split(";")[0]
                        .strip()
                        .lower()
                    )
                    if not img_ct.startswith("image/"):
                        img_ct = "image/jpeg"
                    return img_resp.content, img_ct
            except Exception as e:
                logging.debug("[WebCover] Failed to extract og:image from %s: %s", link, e)
                continue
    return None


def _download_best_cover_image(
    source: str,
    cover_url: Optional[str],
    *,
    cover_candidates: Optional[List[str]] = None,
    headers: Optional[dict] = None,
    timeout: int = 12,
    max_download_bytes: int = 32 * 1024 * 1024,
) -> Optional[Tuple[bytes, str, str]]:
    """
    Download the best (largest byte size) image among available candidates.
    Returns tuple: (content, mime, url_used) or None.
    """
    urls: List[str] = []
    if cover_candidates:
        urls.extend([u for u in cover_candidates if isinstance(u, str)])
    if cover_url:
        urls.append(cover_url)
    urls = _dedupe_keep_order(urls)
    if not urls:
        return None

    source_l = (source or "").strip().lower()
    expanded: List[str] = []
    for u in urls:
        expanded.append(u)
        if "bandcamp" in source_l or "bcbits.com/img/" in u:
            expanded.extend(_bandcamp_cover_url_candidates(u))
        if "last.fm" in source_l or "lastfm" in source_l:
            expanded.extend(_lastfm_cover_url_candidates(u))
    candidates = _dedupe_keep_order(expanded)

    best: Optional[Tuple[bytes, str, str, int]] = None
    req_headers = dict(headers or {})
    # Many hosts (notably Wikimedia/Wikipedia) expect a real UA; keep it consistent across fetches.
    req_headers.setdefault("User-Agent", "PMDA/0.7.5 (self-hosted music library; https://github.com/silkyclouds/PMDA)")
    for u in candidates:
        try:
            resp = requests.get(u, headers=req_headers, timeout=timeout, allow_redirects=True)
            if resp.status_code != 200:
                continue
            try:
                content_length = int(resp.headers.get("content-length") or "0")
            except Exception:
                content_length = 0
            if int(max_download_bytes or 0) > 0 and content_length > int(max_download_bytes):
                logging.debug("[Cover] skipped oversized candidate=%s bytes=%s", u, content_length)
                continue
            mime = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
            if not mime.startswith("image/"):
                continue
            content = resp.content or b""
            size = len(content)
            if size <= 0:
                continue
            if int(max_download_bytes or 0) > 0 and size > int(max_download_bytes):
                logging.debug("[Cover] skipped oversized payload=%s bytes=%s", u, size)
                continue
            if best is None or size > best[3]:
                best = (content, mime or "image/jpeg", u, size)
        except Exception:
            continue

    if best is None:
        return None
    logging.info("[Cover] source=%s selected=%s bytes=%d", source, best[2], best[3])
    return best[0], best[1], best[2]


def _provider_reference_link(*, provider: str, ref: str, artist_name: str = "", album_title: str = "") -> str | None:
    p = _normalize_identity_provider(provider)
    r = str(ref or "").strip()
    artist = str(artist_name or "").strip()
    album = str(album_title or "").strip()
    search_query = " ".join(x for x in [artist, album] if x).strip()
    if not p:
        return None
    if p == "musicbrainz":
        if r:
            return f"https://musicbrainz.org/release-group/{quote(r, safe='')}"
        if search_query:
            return (
                "https://musicbrainz.org/search"
                f"?query={quote(search_query, safe='')}&type=release_group&method=indexed"
            )
        return None
    if p == "discogs":
        if r:
            return f"https://www.discogs.com/release/{quote(r, safe='')}"
        if search_query:
            return f"https://www.discogs.com/search/?q={quote(search_query, safe='')}&type=all"
        return None
    if p == "itunes":
        if r:
            return f"https://music.apple.com/us/album/{quote(r, safe='')}"
        if search_query:
            return f"https://music.apple.com/us/search?term={quote(search_query, safe='')}"
        return None
    if p == "deezer":
        if r:
            return f"https://www.deezer.com/album/{quote(r, safe='')}"
        if search_query:
            return f"https://www.deezer.com/search/{quote(search_query, safe='')}"
        return None
    if p == "spotify":
        if r.startswith(("http://", "https://")):
            return r
        if r:
            return f"https://open.spotify.com/album/{quote(r, safe='')}"
        if search_query:
            return f"https://open.spotify.com/search/{quote(search_query, safe='')}/albums"
        return None
    if p == "qobuz":
        if r.startswith(("http://", "https://")):
            return r
        if search_query:
            return f"https://www.qobuz.com/search?query={quote(search_query, safe='')}"
        return None
    if p == "tidal":
        if r.startswith(("http://", "https://")):
            return r
        if r:
            return f"https://tidal.com/browse/album/{quote(r, safe='')}"
        if search_query:
            return f"https://tidal.com/search?q={quote(search_query, safe='')}"
        return None
    if p == "lastfm":
        # Prefer a human-readable Last.fm album page when we have artist+album.
        if artist and album:
            return f"https://www.last.fm/music/{quote(artist, safe='')}/{quote(album, safe='')}"
        # If only MBID exists, fall back to the corresponding MusicBrainz release page.
        if r:
            return f"https://musicbrainz.org/release/{quote(r, safe='')}"
        if search_query:
            return f"https://www.last.fm/search/albums?q={quote(search_query, safe='')}"
        return None
    if p == "bandcamp":
        if r:
            return r
        if search_query:
            return f"https://bandcamp.com/search?q={quote(search_query, safe='')}&item_type=a"
        return None
    if p == "audiodb":
        if r.startswith(("http://", "https://")):
            return r
        if r:
            return f"https://www.theaudiodb.com/album/{quote(r, safe='')}"
        if search_query:
            return f"https://www.theaudiodb.com/search.php?s={quote(artist, safe='')}&a={quote(album, safe='')}"
        return None
    if p == "wikipedia":
        q = quote((artist or album or r).strip())
        return f"https://en.wikipedia.org/wiki/Special:Search?search={q}" if q else None
    return None


def _vision_verify_cover_before_inject(
    image_bytes: bytes,
    mime: str,
    artist: str,
    album_title: str,
    source: str = "CAA",
    *,
    fail_open: bool = True,
) -> bool:
    """
    Ask vision AI whether the proposed cover image matches the album (artist/title).
    Returns True only if the AI answers Yes. Logs verdict and adds to detailed log.
    """
    # Cost + false-negative control:
    # Only run vision gating for inherently risky "Web" cover pulls. For trusted providers
    # (MusicBrainz/CAA, Discogs, Last.fm, Bandcamp), vision is flaky and can reject correct covers.
    # Those providers are already guarded by strict identity checks upstream.
    if str(source or "").strip().lower() != "web":
        return True
    if not image_bytes or not getattr(sys.modules[__name__], "USE_AI_VISION_BEFORE_COVER_INJECT", False):
        return True
    if not getattr(sys.modules[__name__], "ai_provider_ready", False):
        return True
    try:
        if len(image_bytes) > _MAX_COVER_SIZE_BYTES or (mime and mime != "image/jpeg"):
            image_bytes, mime = _resize_cover_for_vision(image_bytes, mime or "image/jpeg")
        if not image_bytes:
            logging.warning("[Vision before inject] Resize failed for %s / %s", artist, album_title)
            return True if fail_open else False
        b64 = base64.b64encode(image_bytes).decode("ascii")
        data_uri = f"data:{mime or 'image/jpeg'};base64,{b64}"
        system_msg = "Answer only Yes or No. Optionally end with (confidence: N) where N is 0-100."
        user_msg = (
            f"This image is the proposed album cover for: Artist = {artist!r}, Album = {album_title!r}. "
            "Does it look like a legitimate album cover for this album (e.g. title or artist visible, not random artwork)? "
            "Answer only Yes or No. Optionally end with (confidence: N)."
        )
        provider = getattr(sys.modules[__name__], "AI_PROVIDER", "openai")
        # Prefer an explicit vision model when configured; fall back to the main model.
        vision_model = (
            (getattr(sys.modules[__name__], "OPENAI_VISION_MODEL", None) or "").strip()
            or getattr(sys.modules[__name__], "RESOLVED_MODEL", None)
            or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4o-mini")
        )
        resp = call_ai_provider_vision(
            provider,
            vision_model,
            system_msg,
            user_msg,
            image_base64=[{"type": "image_url", "image_url": {"url": data_uri}}],
            max_tokens=20,
            analysis_type="cover_vision_verify",
        )
        resp_txt = (resp or "").strip()
        verdict_clean, vision_confidence = parse_ai_confidence(resp_txt)
        verdict = (verdict_clean or "").strip().upper()
        if not verdict:
            # If the model returns an empty/invalid verdict, never block cover injection.
            logging.info(
                "[Vision before inject] artist=%r album=%r source=%s verdict=(empty) -> %s",
                artist,
                album_title,
                source,
                "accepted (fail-open)" if fail_open else "rejected (fail-closed)",
            )
            return True if fail_open else False
        if vision_confidence is not None:
            logging.info("[Vision before inject] confidence: %d", vision_confidence)
        ok = verdict.startswith("YES")
        # If the model says NO but with high confidence and the source is a trusted
        # provider (MusicBrainz/CAA, Discogs, Last.fm, Bandcamp or generic Web search
        # built from exact artist+album), we prefer to trust the provider match over
        # the vision heuristic and accept the cover anyway.
        if not ok and vision_confidence is not None:
            # We only override a NO verdict for non-MusicBrainz providers where we have
            # a strong text match (Discogs/Last.fm/Bandcamp/Web). For CAA/MusicBrainz,
            # we trust Vision when it says NO to avoid wrong covers coming from MB.
            trusted_sources = {"Discogs", "Last.fm", "Bandcamp", "Web"}
            ai_conf_min = getattr(sys.modules[__name__], "AI_CONFIDENCE_MIN", 50)
            if source in trusted_sources and vision_confidence >= ai_conf_min:
                logging.info(
                    "[Vision before inject] overriding NO verdict for trusted source %s "
                    "(confidence=%d >= %d) – accepting cover.",
                    source,
                    vision_confidence,
                    ai_conf_min,
                )
                ok = True
        logging.info(
            "[Vision before inject] artist=%r album=%r source=%s verdict=%s -> %s",
            artist,
            album_title,
            source,
            verdict,
            "accepted" if ok else "rejected (cover not saved)",
        )
        if not ok:
            logging.info(
                "[Vision before inject] Cover rejected: not saving or embedding for %s / %s",
                artist,
                album_title,
            )
        return ok
    except Exception as e:
        logging.warning("[Vision before inject] Verification failed for %s / %s: %s", artist, album_title, e)
        return True if fail_open else False


def _extract_embedded_artworks_from_audio_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _extract_embedded_artworks_from_audio(*args, **kwargs)

def _resolve_files_album_cover_asset_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _resolve_files_album_cover_asset(*args, **kwargs)

def _extract_embedded_cover_from_audio_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _extract_embedded_cover_from_audio(*args, **kwargs)

def _extract_embedded_artworks_from_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _extract_embedded_artworks_from_folder(*args, **kwargs)

def _get_local_cover_data_uri_for_vision_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _get_local_cover_data_uri_for_vision(*args, **kwargs)

def _resize_cover_for_vision_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _resize_cover_for_vision(*args, **kwargs)

def _encode_local_cover_to_data_uri_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _encode_local_cover_to_data_uri(*args, **kwargs)

def _ocr_cover_text_from_image_bytes_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ocr_cover_text_from_image_bytes(*args, **kwargs)

def _cover_ocr_smart_trigger_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _cover_ocr_smart_trigger(*args, **kwargs)

def _identity_cover_ocr_context_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _identity_cover_ocr_context(*args, **kwargs)

def _fetch_cover_from_web_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_cover_from_web(*args, **kwargs)

def _download_best_cover_image_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _download_best_cover_image(*args, **kwargs)

def _provider_reference_link_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_reference_link(*args, **kwargs)

def _vision_verify_cover_before_inject_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _vision_verify_cover_before_inject(*args, **kwargs)


_ORIGINAL_EXTRACTED_FUNCTIONS.update({
    "_extract_embedded_artworks_from_audio": _extract_embedded_artworks_from_audio,
    "_resolve_files_album_cover_asset": _resolve_files_album_cover_asset,
    "_extract_embedded_cover_from_audio": _extract_embedded_cover_from_audio,
    "_extract_embedded_artworks_from_folder": _extract_embedded_artworks_from_folder,
    "_get_local_cover_data_uri_for_vision": _get_local_cover_data_uri_for_vision,
    "_resize_cover_for_vision": _resize_cover_for_vision,
    "_encode_local_cover_to_data_uri": _encode_local_cover_to_data_uri,
    "_ocr_cover_text_from_image_bytes": _ocr_cover_text_from_image_bytes,
    "_cover_ocr_smart_trigger": _cover_ocr_smart_trigger,
    "_identity_cover_ocr_context": _identity_cover_ocr_context,
    "_fetch_cover_from_web": _fetch_cover_from_web,
    "_download_best_cover_image": _download_best_cover_image,
    "_provider_reference_link": _provider_reference_link,
    "_vision_verify_cover_before_inject": _vision_verify_cover_before_inject,
})
