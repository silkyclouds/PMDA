"""Album browse box-set grouping helpers."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Callable, Mapping, Sequence
import re


_BOX_SET_DISC_LEAF_RE = re.compile(
    r"^(?:cd|disc|disk|lp|part|pt|vol(?:ume)?)\s*[-_. ]*\d{1,2}$",
    re.IGNORECASE,
)
_BOX_SET_FORMAT_LEAF_RE = re.compile(
    r"^(?:flac|mp3|aac|alac|wav|aiff|aif|m4a|ogg|opus|dsd|dsf|ape|wv)$",
    re.IGNORECASE,
)


def files_box_set_normalize_path(value: Any) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    if not raw:
        return ""
    if len(raw) > 1:
        raw = raw.rstrip("/")
    return raw


def files_box_set_parent_path(value: Any) -> str:
    raw = files_box_set_normalize_path(value)
    if not raw or "/" not in raw:
        return ""
    return raw.rsplit("/", 1)[0]


def files_box_set_leaf_name(value: Any) -> str:
    raw = files_box_set_normalize_path(value)
    if not raw:
        return ""
    return raw.rsplit("/", 1)[-1].strip()


def files_box_set_identity_key(
    row: Mapping[str, Any] | None,
    *,
    normalize_identity: Callable[[str], str],
) -> str:
    payload = row or {}
    for key in (
        "musicbrainz_release_group_id",
        "discogs_release_id",
        "lastfm_album_mbid",
        "bandcamp_album_url",
    ):
        value = str(payload.get(key) or "").strip()
        if value:
            return f"{key}:{value.lower()}"
    title_norm = str(payload.get("title_norm") or "").strip()
    if title_norm:
        return f"title:{title_norm.lower()}"
    title_raw = normalize_identity(str(payload.get("title") or ""))
    return f"title:{title_raw}" if title_raw else ""


def files_box_set_group_key(
    row: Mapping[str, Any] | None,
    *,
    normalize_identity: Callable[[str], str],
) -> str:
    payload = row or {}
    parent_path = files_box_set_parent_path(payload.get("folder_path"))
    identity = files_box_set_identity_key(payload, normalize_identity=normalize_identity)
    if not parent_path or not identity:
        return ""
    return f"{parent_path.lower()}|{identity}"


def files_box_set_group_is_valid(rows: Sequence[Mapping[str, Any]]) -> bool:
    if len(rows or []) <= 1:
        return False
    folders = {
        files_box_set_normalize_path(row.get("folder_path"))
        for row in (rows or [])
        if files_box_set_normalize_path(row.get("folder_path"))
    }
    if len(folders) <= 1:
        return False
    parent_paths = {files_box_set_parent_path(folder) for folder in folders}
    if len(parent_paths) != 1:
        return False
    leaf_names = [files_box_set_leaf_name(folder) for folder in folders]
    if leaf_names and all(_BOX_SET_FORMAT_LEAF_RE.match(name or "") for name in leaf_names):
        return False
    return True


def files_box_set_member_sort_key(row: Mapping[str, Any]) -> tuple[int, int, str, int]:
    leaf = files_box_set_leaf_name(row.get("folder_path"))
    explicit_disc_no = 0
    match = re.search(r"(\d{1,2})", leaf or "")
    if match:
        try:
            explicit_disc_no = int(match.group(1) or 0)
        except Exception:
            explicit_disc_no = 0
    discish = 0 if _BOX_SET_DISC_LEAF_RE.match(leaf or "") else 1
    return (
        discish,
        explicit_disc_no if explicit_disc_no > 0 else 9999,
        (leaf or "").lower(),
        int(row.get("album_id") or row.get("id") or 0),
    )


def files_box_set_display_artist(
    rows: Sequence[Mapping[str, Any]],
    *,
    normalize_artist_key: Callable[[str], str],
) -> tuple[str, int]:
    candidates: list[tuple[str, int]] = []
    for row in (rows or []):
        name = str(row.get("artist_name") or "").strip()
        artist_id = int(row.get("artist_id") or 0)
        if name:
            candidates.append((name, artist_id))
    if not candidates:
        return ("", 0)
    unique_names = []
    seen_names: set[str] = set()
    for name, artist_id in candidates:
        norm = normalize_artist_key(name)
        if not norm or norm in seen_names:
            continue
        seen_names.add(norm)
        unique_names.append((name, artist_id, norm))
    if len(unique_names) == 1:
        return (unique_names[0][0], unique_names[0][1])
    for name, artist_id, norm in sorted(unique_names, key=lambda item: (len(item[0]), item[0].lower())):
        if norm and all(norm in other_norm for _other_name, _other_id, other_norm in unique_names):
            return (name, artist_id)
    counts = Counter(norm for _name, _artist_id, norm in unique_names)
    best_name, best_id, _best_norm = sorted(
        unique_names,
        key=lambda item: (-counts.get(item[2], 0), len(item[0]), item[0].lower()),
    )[0]
    return (best_name, best_id)


def collapse_files_album_browse_rows_for_runtime(
    runtime: Any,
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    ordered_rows = [dict(row) for row in (rows or [])]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in ordered_rows:
        group_key = files_box_set_group_key(
            row,
            normalize_identity=runtime._normalize_identity_text_strict,
        )
        fallback_key = f"album:{int(row.get('album_id') or row.get('id') or 0)}"
        grouped.setdefault(group_key or fallback_key, []).append(row)

    valid_group_keys = {
        key
        for key, members in grouped.items()
        if (key.startswith("/") or "|" in key) and files_box_set_group_is_valid(members)
    }

    emitted: set[str] = set()
    collapsed: list[dict[str, Any]] = []
    for row in ordered_rows:
        album_id = int(row.get("album_id") or row.get("id") or 0)
        group_key = files_box_set_group_key(
            row,
            normalize_identity=runtime._normalize_identity_text_strict,
        )
        if group_key and group_key in valid_group_keys:
            if group_key in emitted:
                continue
            emitted.add(group_key)
            members = list(grouped.get(group_key) or [])
            display_artist_name, display_artist_id = files_box_set_display_artist(
                members,
                normalize_artist_key=runtime._norm_artist_key,
            )
            representative = next((member for member in members if bool(member.get("has_cover"))), members[0])
            collapsed_row = dict(representative)
            collapsed_row["album_id"] = int(representative.get("album_id") or representative.get("id") or album_id)
            collapsed_row["artist_name"] = display_artist_name or str(representative.get("artist_name") or "")
            collapsed_row["artist_id"] = int(display_artist_id or representative.get("artist_id") or 0)
            collapsed_row["track_count"] = sum(int(member.get("track_count") or 0) for member in members)
            collapsed_row["is_box_set"] = True
            collapsed_row["box_set_disc_count"] = len(members)
            collapsed_row["box_set_member_album_ids"] = [
                int(member.get("album_id") or member.get("id") or 0)
                for member in sorted(members, key=files_box_set_member_sort_key)
                if int(member.get("album_id") or member.get("id") or 0) > 0
            ]
            collapsed_row["box_set_root_path"] = files_box_set_parent_path(representative.get("folder_path"))
            collapsed.append(collapsed_row)
            continue
        if album_id <= 0:
            continue
        row_copy = dict(row)
        row_copy["album_id"] = album_id
        row_copy["is_box_set"] = False
        row_copy["box_set_disc_count"] = None
        row_copy["box_set_member_album_ids"] = [album_id]
        row_copy["box_set_root_path"] = None
        collapsed.append(row_copy)
    return collapsed


def files_box_set_reindex_tracks(
    track_rows: Sequence[Mapping[str, Any]],
    member_album_ids: Sequence[int],
) -> tuple[list[dict[str, Any]], int]:
    rows_by_album: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in (track_rows or []):
        rows_by_album[int(row.get("album_id") or 0)].append(dict(row))
    out: list[dict[str, Any]] = []
    disc_offset = 0
    total_disc_count = 0
    for album_id in [int(value or 0) for value in (member_album_ids or []) if int(value or 0) > 0]:
        album_tracks = sorted(
            rows_by_album.get(album_id) or [],
            key=lambda item: (
                int(item.get("disc_num") or 1),
                int(item.get("track_num") or 0),
                int(item.get("track_id") or item.get("id") or 0),
            ),
        )
        if not album_tracks:
            continue
        max_local_disc = max(int(item.get("disc_num") or 1) for item in album_tracks)
        max_local_disc = max(1, int(max_local_disc or 1))
        for item in album_tracks:
            local_disc = max(1, int(item.get("disc_num") or 1))
            global_disc = disc_offset + local_disc
            item["disc_num"] = global_disc
            item["disc_label"] = f"Disc {global_disc}"
            out.append(item)
        disc_offset += max_local_disc
        total_disc_count = max(total_disc_count, disc_offset)
    return (out, int(total_disc_count or 0))
