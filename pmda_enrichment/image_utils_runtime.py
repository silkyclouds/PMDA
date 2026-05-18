"""Shared image/path helper functions for artist and album enrichment."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable

import requests


def resolve_remote_image_or_og_url(target_url: str, *, timeout: int = 8) -> str:
    url = str(target_url or "").strip()
    if not url:
        return ""
    try:
        resp = requests.get(url, timeout=max(3, min(int(timeout or 8), 20)), allow_redirects=True)
    except Exception:
        return ""
    if resp.status_code != 200:
        return ""
    content_type = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
    if content_type.startswith("image/"):
        return url
    if "text/html" not in content_type:
        return ""
    html = resp.text or ""
    if not html:
        return ""
    for pattern in (
        r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
        r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:image["\']',
    ):
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            found = str(match.group(1) or "").strip()
            if found:
                return found
    return ""


def artist_folder_has_image(artist_folder: Path, artist_image_names: tuple[str, ...] | list[str]) -> bool:
    """Return True if the artist folder already has a dedicated artist image file."""
    if not artist_folder or not artist_folder.is_dir():
        return False
    return any((artist_folder / name).is_file() for name in artist_image_names)


def paths_refer_to_same_file(
    left: str | Path | None,
    right: str | Path | None,
    *,
    path_for_fs_access: Callable[[Path], Path],
) -> bool:
    left_raw = str(left or "").strip()
    right_raw = str(right or "").strip()
    if not left_raw or not right_raw:
        return False
    try:
        left_path = path_for_fs_access(Path(left_raw))
    except Exception:
        left_path = Path(left_raw)
    try:
        right_path = path_for_fs_access(Path(right_raw))
    except Exception:
        right_path = Path(right_raw)
    try:
        return left_path.resolve() == right_path.resolve()
    except Exception:
        return str(left_path) == str(right_path)


def artist_has_true_image_sql(
    artist_alias: str = "a",
    ext_alias: str = "ext",
    *,
    weak_ext_expr: str,
) -> str:
    """
    Return SQL that is true only when we have a dedicated artist image.
    Album covers must never count as artist portraits.
    """
    local_path_expr = f"REPLACE(COALESCE({artist_alias}.image_path, ''), '\\\\', '/')"
    local_media_cache_expr = f"({local_path_expr} LIKE '%%/media_cache/artist/%%')"
    exact_ext_name_expr = (
        f"(COALESCE(NULLIF({ext_alias}.name_norm, ''), '') <> ''"
        f" AND COALESCE(NULLIF({ext_alias}.name_norm, ''), '') = COALESCE(NULLIF({artist_alias}.name_norm, ''), ''))"
    )
    mirrored_ext_expr = (
        f"(COALESCE({ext_alias}.image_path, '') <> ''"
        f" AND COALESCE({artist_alias}.image_path, '') = COALESCE({ext_alias}.image_path, ''))"
    )
    return (
        f"("
        f"(({artist_alias}.has_image AND COALESCE({artist_alias}.image_path, '') <> '')"
        f" AND (NOT {local_media_cache_expr}"
        f"      OR ({mirrored_ext_expr} AND {exact_ext_name_expr} AND NOT {weak_ext_expr})))"
        f" OR (COALESCE({ext_alias}.image_path, '') <> '' AND {exact_ext_name_expr} AND NOT {weak_ext_expr})"
        f")"
    )


def image_ahash_hex(raw: bytes, size: int = 8) -> str | None:
    try:
        from PIL import Image
        from io import BytesIO
    except Exception:
        return None
    try:
        with Image.open(BytesIO(raw)) as img:
            gray = img.convert("L").resize((size, size), Image.Resampling.BILINEAR)
            pixels = list(gray.getdata())
        if not pixels:
            return None
        avg = sum(pixels) / len(pixels)
        bits = "".join("1" if px >= avg else "0" for px in pixels)
        return f"{int(bits, 2):0{size * size // 4}x}"
    except Exception:
        return None


def hamming_hex(a: str, b: str) -> int:
    try:
        return bin(int(a, 16) ^ int(b, 16)).count("1")
    except Exception:
        return 999
