"""Runtime-bound media cache, artwork serving, and pre-cache helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_media_cache_root_dir',
    '_path_is_within',
    '_is_media_cache_file',
    '_mime_from_path',
    '_artwork_etag_for_stat',
    '_artwork_cache_control',
    '_artwork_ram_cache_get',
    '_artwork_ram_cache_put',
    '_artwork_ram_cache_prime',
    '_serve_image_file_cached',
    '_transparent_png_response',
    '_ensure_media_cache_dirs',
    '_image_ext_from_mime',
    '_media_cache_key_for_path',
    '_media_cache_path_for_key',
    '_ensure_cached_image_for_path',
    '_cached_image_for_path_if_exists',
    '_ensure_cached_image_from_bytes',
    '_existing_file_path',
    '_promote_files_media_paths_to_cache',
    '_precache_files_media_assets',
}


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in _EXTRACTED_NAMES:
            if getattr(value, "__module__", "") != getattr(runtime, "__name__", ""):
                globals()[name] = value
            else:
                original = _ORIGINAL_EXTRACTED_FUNCTIONS.get(name)
                if original is not None:
                    globals()[name] = original
            continue
        own_wrapper = name.endswith("_for_runtime") and name[: -len("_for_runtime")] in _EXTRACTED_NAMES
        if name == "_bind_runtime" or own_wrapper:
            continue
        globals()[name] = value

def _media_cache_root_dir() -> Path:
    root = Path((MEDIA_CACHE_ROOT or "").strip() or str(CONFIG_DIR / "media_cache"))
    return root

def _path_is_within(base: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(base.resolve())
        return True
    except Exception:
        return False

def _is_media_cache_file(path: Path, *, kind: str | None = None) -> bool:
    root = _media_cache_root_dir()
    target_root = root / kind if kind else root
    return _path_is_within(target_root, path)

def _mime_from_path(path: Path) -> str:
    ext = str(path.suffix or "").strip().lower()
    if ext == ".webp":
        return "image/webp"
    if ext in (".jpg", ".jpeg"):
        return "image/jpeg"
    if ext == ".png":
        return "image/png"
    if ext == ".gif":
        return "image/gif"
    return "application/octet-stream"

def _artwork_etag_for_stat(st: os.stat_result) -> str:
    return f"W/\"{int(st.st_mtime_ns):x}-{int(st.st_size):x}\""

def _artwork_cache_control(max_age: int = 86400, *, revalidate: bool = False) -> str:
    if revalidate:
        # IDs can be reused after a maintenance reset; force conditional revalidation
        # so stale browser artwork is replaced immediately.
        return "public, max-age=0, must-revalidate"
    ttl = max(60, int(max_age or 86400))
    return f"public, max-age={ttl}, stale-while-revalidate={ttl}"

def _artwork_ram_cache_get(path: Path, st: os.stat_result) -> Optional[tuple[bytes, str]]:
    global _ARTWORK_RAM_CACHE_BYTES
    if _ARTWORK_RAM_CACHE_MAX_BYTES <= 0:
        return None
    now = time.time()
    key = str(path)
    with _ARTWORK_RAM_CACHE_LOCK:
        entry = _ARTWORK_RAM_CACHE.get(key)
        if not entry:
            return None
        age = now - float(entry.get("ts") or 0.0)
        if age > float(ARTWORK_RAM_CACHE_TTL_SEC):
            old = _ARTWORK_RAM_CACHE.pop(key, None)
            if old:
                _ARTWORK_RAM_CACHE_BYTES = max(0, int(_ARTWORK_RAM_CACHE_BYTES) - int(old.get("blob_size") or 0))
            return None
        if int(entry.get("mtime_ns") or 0) != int(getattr(st, "st_mtime_ns", 0)) or int(entry.get("size") or 0) != int(getattr(st, "st_size", 0)):
            old = _ARTWORK_RAM_CACHE.pop(key, None)
            if old:
                _ARTWORK_RAM_CACHE_BYTES = max(0, int(_ARTWORK_RAM_CACHE_BYTES) - int(old.get("blob_size") or 0))
            return None
        _ARTWORK_RAM_CACHE.move_to_end(key)
        blob = entry.get("blob")
        mime = str(entry.get("mime") or "application/octet-stream")
        if isinstance(blob, (bytes, bytearray)):
            return (bytes(blob), mime)
    return None

def _artwork_ram_cache_put(path: Path, st: os.stat_result, blob: bytes, mime: str) -> None:
    global _ARTWORK_RAM_CACHE_BYTES
    if _ARTWORK_RAM_CACHE_MAX_BYTES <= 0:
        return
    if not blob:
        return
    size = int(len(blob))
    if size <= 0 or size > _ARTWORK_RAM_CACHE_MAX_ITEM_BYTES:
        return
    key = str(path)
    with _ARTWORK_RAM_CACHE_LOCK:
        prev = _ARTWORK_RAM_CACHE.pop(key, None)
        if prev:
            _ARTWORK_RAM_CACHE_BYTES = max(0, int(_ARTWORK_RAM_CACHE_BYTES) - int(prev.get("blob_size") or 0))
        while _ARTWORK_RAM_CACHE and (int(_ARTWORK_RAM_CACHE_BYTES) + size) > _ARTWORK_RAM_CACHE_MAX_BYTES:
            _, old = _ARTWORK_RAM_CACHE.popitem(last=False)
            _ARTWORK_RAM_CACHE_BYTES = max(0, int(_ARTWORK_RAM_CACHE_BYTES) - int(old.get("blob_size") or 0))
        if (int(_ARTWORK_RAM_CACHE_BYTES) + size) > _ARTWORK_RAM_CACHE_MAX_BYTES:
            return
        _ARTWORK_RAM_CACHE[key] = {
            "blob": bytes(blob),
            "blob_size": size,
            "mime": str(mime or "application/octet-stream"),
            "ts": time.time(),
            "mtime_ns": int(getattr(st, "st_mtime_ns", 0)),
            "size": int(getattr(st, "st_size", 0)),
        }
        _ARTWORK_RAM_CACHE_BYTES = int(_ARTWORK_RAM_CACHE_BYTES) + size

def _artwork_ram_cache_prime(path: Path) -> None:
    if _ARTWORK_RAM_CACHE_MAX_BYTES <= 0:
        return
    try:
        st = path.stat()
    except Exception:
        return
    if int(getattr(st, "st_size", 0)) <= 0 or int(getattr(st, "st_size", 0)) > _ARTWORK_RAM_CACHE_MAX_ITEM_BYTES:
        return
    if _artwork_ram_cache_get(path, st) is not None:
        return
    try:
        blob = path.read_bytes()
    except Exception:
        return
    _artwork_ram_cache_put(path, st, blob, _mime_from_path(path))

def _serve_image_file_cached(path: Path, *, max_age: int = 86400, revalidate: bool = False) -> Response:
    st = path.stat()
    etag = _artwork_etag_for_stat(st)
    inm = str(request.headers.get("If-None-Match") or "").strip()
    if inm and etag in inm:
        resp = Response(status=304)
        resp.headers["ETag"] = etag
        resp.headers["Cache-Control"] = _artwork_cache_control(max_age, revalidate=revalidate)
        return resp

    ram_hit = _artwork_ram_cache_get(path, st)
    if ram_hit is not None:
        blob, mime = ram_hit
    else:
        blob = path.read_bytes()
        mime = _mime_from_path(path)
        _artwork_ram_cache_put(path, st, blob, mime)

    resp = Response(blob, mimetype=mime)
    resp.headers["ETag"] = etag
    resp.headers["Cache-Control"] = _artwork_cache_control(max_age, revalidate=revalidate)
    resp.headers["Content-Length"] = str(len(blob))
    return resp

def _transparent_png_response(max_age: int = 3600, revalidate: bool = False) -> Response:
    resp = Response(_TRANSPARENT_PNG_1PX, mimetype="image/png")
    resp.headers["Cache-Control"] = _artwork_cache_control(max_age, revalidate=revalidate)
    resp.headers["Content-Length"] = str(len(_TRANSPARENT_PNG_1PX))
    return resp

def _ensure_media_cache_dirs() -> None:
    root = _media_cache_root_dir()
    try:
        (root / "album").mkdir(parents=True, exist_ok=True)
        (root / "artist").mkdir(parents=True, exist_ok=True)
        (root / "label").mkdir(parents=True, exist_ok=True)
        (root / "embedded").mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logging.debug("Unable to initialize media cache directories at %s: %s", root, e)

def _image_ext_from_mime(mime: str) -> str:
    m = (mime or "").lower()
    if "svg" in m:
        return ".svg"
    if "png" in m:
        return ".png"
    if "webp" in m:
        return ".webp"
    return ".jpg"

def _media_cache_key_for_path(source_path: Path, kind: str, max_px: int) -> Optional[str]:
    try:
        st = source_path.stat()
    except OSError:
        return None
    payload = f"{kind}|{max_px}|{str(source_path)}|{int(st.st_mtime_ns)}|{int(st.st_size)}|v2"
    return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()

def _media_cache_path_for_key(key: str, kind: str, ext: str = ".webp") -> Path:
    root = _media_cache_root_dir()
    return root / kind / key[:2] / key[2:4] / f"{key}{ext}"

def _ensure_cached_image_for_path(source_path: Path, *, kind: str, max_px: int = 320) -> Optional[Path]:
    if not source_path or not source_path.exists() or not source_path.is_file():
        return None
    key = _media_cache_key_for_path(source_path, kind, max_px)
    if not key:
        return None
    target = _media_cache_path_for_key(key, kind, ".webp")
    if target.exists() and target.is_file():
        return target
    try:
        from PIL import Image
    except ImportError:
        return source_path
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with Image.open(source_path) as img:
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")
            img.thumbnail((max_px, max_px), Image.Resampling.LANCZOS)
            img.save(target, format="WEBP", quality=85, method=6)
        return target
    except Exception as e:
        logging.debug("Media cache generation failed for %s: %s", source_path, e)
        return source_path

def _cached_image_for_path_if_exists(source_path: Path, *, kind: str, max_px: int = 320) -> Optional[Path]:
    key = _media_cache_key_for_path(source_path, kind, max_px)
    if not key:
        return None
    target = _media_cache_path_for_key(key, kind, ".webp")
    if target.exists() and target.is_file():
        return target
    return None

def _ensure_cached_image_from_bytes(
    raw: bytes,
    mime: str,
    *,
    kind: str,
    cache_key_hint: str,
    max_px: int = 320,
) -> Optional[Path]:
    if not raw:
        return None
    try:
        raw_sha1 = hashlib.sha1(raw).hexdigest()
        digest = hashlib.sha1(
            f"{kind}|{max_px}|{cache_key_hint}|{mime}|{raw_sha1}|v3".encode("utf-8", errors="ignore")
        ).hexdigest()
    except Exception:
        return None
    raw_ext = _image_ext_from_mime(mime)
    raw_target = _media_cache_path_for_key(digest, kind, raw_ext)
    target = _media_cache_path_for_key(digest, kind, ".webp")
    if target.exists() and target.is_file():
        return target
    if raw_target.exists() and raw_target.is_file():
        return raw_target
    try:
        from io import BytesIO
        from PIL import Image
    except ImportError:
        try:
            raw_target.parent.mkdir(parents=True, exist_ok=True)
            raw_target.write_bytes(raw)
            return raw_target
        except Exception:
            return None
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        img = Image.open(BytesIO(raw))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        img.thumbnail((max_px, max_px), Image.Resampling.LANCZOS)
        img.save(target, format="WEBP", quality=85, method=6)
        return target
    except Exception as e:
        logging.debug("Embedded media cache generation failed: %s", e)
        try:
            raw_target.parent.mkdir(parents=True, exist_ok=True)
            raw_target.write_bytes(raw)
            return raw_target
        except Exception:
            return None

def _existing_file_path(raw: str) -> Optional[Path]:
    txt = str(raw or "").strip()
    if not txt:
        return None
    try:
        p = path_for_fs_access(Path(txt))
    except Exception:
        p = Path(txt)
    if p.exists() and p.is_file():
        return p
    return None

def _promote_files_media_paths_to_cache(
    artists_map: dict[str, dict],
    albums_payload: list[dict],
    *,
    filesystem_allowed: bool = True,
    allow_embedded_cover_extract: bool = True,
    generate_missing_cache: bool = True,
) -> tuple[int, int]:
    """
    Force files-library media paths to point to NVMe cache masters so UI serving does
    not depend on source HDD spin-up.
    """
    if not filesystem_allowed:
        return (0, 0)
    _ensure_media_cache_dirs()
    storage_scope = _storage_background_filesystem_scope()
    storage_io_enforced = bool(storage_scope.get("enabled"))
    root_dirs = _files_root_dir_strings()
    artist_folder_hints: dict[str, Path] = {}
    covers_promoted = 0
    artists_promoted = 0
    total_albums = len(albums_payload or [])
    last_log_ts = 0.0

    for idx, album in enumerate(albums_payload, start=1):
        now = time.time()
        if idx == 1 or idx == total_albums or (now - last_log_ts) >= 30.0:
            logging.info(
                "Files library index media prepare albums=%d/%d promoted_covers=%d embedded=%s generate=%s",
                idx,
                total_albums,
                covers_promoted,
                bool(allow_embedded_cover_extract),
                bool(generate_missing_cache),
            )
            last_log_ts = now
        artist_norm = str(album.get("artist_norm") or "").strip()
        folder_path = None
        folder_raw = str(album.get("folder_path") or "").strip()
        cover_raw = str(album.get("cover_path") or "").strip()
        folder_io_allowed = bool(
            folder_raw and _storage_path_allowed_for_background_io(folder_raw, storage_scope)
        )
        cover_io_allowed = bool(
            cover_raw and _storage_path_allowed_for_background_io(cover_raw, storage_scope, kind="album")
        )
        if storage_io_enforced and not folder_io_allowed and (not cover_raw or not cover_io_allowed):
            # Preserve the already-published metadata. The background media job is
            # not allowed to touch this source path while another disk is active.
            continue
        if folder_raw and folder_io_allowed:
            try:
                folder_path = path_for_fs_access(Path(folder_raw))
            except Exception:
                folder_path = None
        if artist_norm and folder_path and folder_path.exists() and folder_path.is_dir() and artist_norm not in artist_folder_hints:
            artist_name = str((artists_map.get(artist_norm) or {}).get("name") or "").strip()
            hint = _files_guess_artist_folder(folder_path, artist_name or "Unknown Artist", root_dirs=root_dirs)
            if hint and hint.exists() and hint.is_dir():
                artist_folder_hints[artist_norm] = hint

        source_cover = _existing_file_path(cover_raw) if cover_io_allowed else None
        if source_cover is None and folder_path and folder_path.exists() and folder_path.is_dir():
            detected = _first_cover_path(folder_path)
            if detected and detected.exists() and detected.is_file():
                source_cover = detected

        if allow_embedded_cover_extract and source_cover is None and folder_path and folder_path.exists() and folder_path.is_dir():
            try:
                embedded = _extract_embedded_cover_from_folder(folder_path, max_audio_files=8)
            except Exception:
                embedded = None
            if embedded:
                raw, mime = embedded
                cache_hint = str(album.get("album_id") or album.get("title_norm") or folder_path)
                cached_cover = _ensure_cached_image_from_bytes(
                    raw,
                    mime,
                    kind="album",
                    cache_key_hint=f"promote:{cache_hint}",
                    max_px=_MEDIA_CACHE_MASTER_PX,
                )
                if cached_cover and cached_cover.exists() and cached_cover.is_file():
                    album["cover_path"] = str(cached_cover)
                    album["has_cover"] = True
                    covers_promoted += 1
                    continue

        if source_cover is None:
            album["has_cover"] = False
            album["cover_path"] = ""
            continue

        if generate_missing_cache:
            master_cover = _ensure_cached_image_for_path(source_cover, kind="album", max_px=_MEDIA_CACHE_MASTER_PX)
        else:
            master_cover = _cached_image_for_path_if_exists(source_cover, kind="album", max_px=_MEDIA_CACHE_MASTER_PX)
        if master_cover and master_cover.exists() and master_cover.is_file() and _is_media_cache_file(master_cover, kind="album"):
            album["cover_path"] = str(master_cover)
            album["has_cover"] = True
            covers_promoted += 1
        else:
            if generate_missing_cache:
                # Cache-only policy: do not persist source-disk paths for request-time serving.
                album["cover_path"] = ""
                album["has_cover"] = False
            else:
                # Large rebuilds must publish metadata first. Keep source artwork visible and
                # let request-time/background caching migrate it later instead of blocking DB swap.
                album["cover_path"] = str(source_cover)
                album["has_cover"] = True

    for artist_norm, data in artists_map.items():
        image_raw = str((data or {}).get("image_path") or "").strip()
        image_io_allowed = bool(
            image_raw and _storage_path_allowed_for_background_io(image_raw, storage_scope, kind="artist")
        )
        if storage_io_enforced and image_raw and not image_io_allowed:
            continue
        source_image = _existing_file_path(image_raw) if image_io_allowed else None
        if source_image is None:
            hint = artist_folder_hints.get(str(artist_norm or "").strip())
            if hint and hint.exists() and hint.is_dir():
                detected = _first_artist_image_path(hint)
                if detected and detected.exists() and detected.is_file():
                    source_image = detected

        if source_image is None:
            data["has_image"] = False
            data["image_path"] = ""
            continue

        if generate_missing_cache:
            master_image = _ensure_cached_image_for_path(source_image, kind="artist", max_px=_MEDIA_CACHE_MASTER_PX)
        else:
            master_image = _cached_image_for_path_if_exists(source_image, kind="artist", max_px=_MEDIA_CACHE_MASTER_PX)
        if master_image and master_image.exists() and master_image.is_file() and _is_media_cache_file(master_image, kind="artist"):
            data["image_path"] = str(master_image)
            data["has_image"] = True
            artists_promoted += 1
        else:
            if generate_missing_cache:
                data["image_path"] = ""
                data["has_image"] = False
            else:
                data["image_path"] = str(source_image)
                data["has_image"] = True

    return covers_promoted, artists_promoted

def _precache_files_media_assets(
    artists_map: dict[str, dict],
    albums_payload: list[dict],
    *,
    include_album_covers: bool = True,
    include_artist_images: bool = True,
    cache_only: bool = False,
) -> tuple[int, int]:
    _ensure_media_cache_dirs()
    storage_scope = _storage_background_filesystem_scope()
    storage_io_enforced = bool(storage_scope.get("enabled")) and not bool(cache_only)
    cover_paths: list[Path] = []
    artist_paths: list[Path] = []
    if include_album_covers:
        for album in albums_payload:
            raw = str(album.get("cover_path") or "").strip()
            if raw:
                if storage_io_enforced and not _storage_path_allowed_for_background_io(raw, storage_scope, kind="album"):
                    continue
                p = path_for_fs_access(Path(raw))
                if cache_only and not _is_media_cache_file(p, kind="album"):
                    continue
                if p.exists() and p.is_file():
                    cover_paths.append(p)
    if include_artist_images:
        for data in artists_map.values():
            raw = str((data or {}).get("image_path") or "").strip()
            if raw:
                if storage_io_enforced and not _storage_path_allowed_for_background_io(raw, storage_scope, kind="artist"):
                    continue
                p = path_for_fs_access(Path(raw))
                if cache_only and not _is_media_cache_file(p, kind="artist"):
                    continue
                if p.exists() and p.is_file():
                    artist_paths.append(p)
    cover_paths = list(dict.fromkeys(cover_paths))
    artist_paths = list(dict.fromkeys(artist_paths))

    covers_done = 0
    artists_done = 0

    for p in cover_paths:
        for size in _MEDIA_CACHE_SIZES:
            out = _ensure_cached_image_for_path(p, kind="album", max_px=size)
            if out:
                covers_done += 1
                _artwork_ram_cache_prime(out)
    for p in artist_paths:
        for size in _MEDIA_CACHE_SIZES:
            out = _ensure_cached_image_for_path(p, kind="artist", max_px=size)
            if out:
                artists_done += 1
                _artwork_ram_cache_prime(out)
    return covers_done, artists_done

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}

def _media_cache_root_dir_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _media_cache_root_dir(*args, **kwargs)

def _path_is_within_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _path_is_within(*args, **kwargs)

def _is_media_cache_file_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _is_media_cache_file(*args, **kwargs)

def _mime_from_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mime_from_path(*args, **kwargs)

def _artwork_etag_for_stat_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artwork_etag_for_stat(*args, **kwargs)

def _artwork_cache_control_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artwork_cache_control(*args, **kwargs)

def _artwork_ram_cache_get_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artwork_ram_cache_get(*args, **kwargs)

def _artwork_ram_cache_put_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artwork_ram_cache_put(*args, **kwargs)

def _artwork_ram_cache_prime_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _artwork_ram_cache_prime(*args, **kwargs)

def _serve_image_file_cached_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _serve_image_file_cached(*args, **kwargs)

def _transparent_png_response_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _transparent_png_response(*args, **kwargs)

def _ensure_media_cache_dirs_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ensure_media_cache_dirs(*args, **kwargs)

def _image_ext_from_mime_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _image_ext_from_mime(*args, **kwargs)

def _media_cache_key_for_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _media_cache_key_for_path(*args, **kwargs)

def _media_cache_path_for_key_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _media_cache_path_for_key(*args, **kwargs)

def _ensure_cached_image_for_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ensure_cached_image_for_path(*args, **kwargs)

def _cached_image_for_path_if_exists_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _cached_image_for_path_if_exists(*args, **kwargs)

def _ensure_cached_image_from_bytes_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ensure_cached_image_from_bytes(*args, **kwargs)

def _existing_file_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _existing_file_path(*args, **kwargs)

def _promote_files_media_paths_to_cache_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _promote_files_media_paths_to_cache(*args, **kwargs)

def _precache_files_media_assets_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _precache_files_media_assets(*args, **kwargs)
