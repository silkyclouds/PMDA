"""Runtime-bound materialization path, move-audit, and conflict helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_sanitize_export_component',
    'build_export_path',
    '_format_label_for_folder',
    '_derive_album_type_for_folder_name',
    '_build_matched_album_folder_name',
    '_matched_album_family_key',
    '_matched_export_target_folder',
    '_materialization_confidence_policy',
    '_path_is_under_root',
    '_build_dupe_candidate_from_folder',
    '_move_folder_to_dupes',
    '_record_scan_move_event',
    '_folder_file_stat_map',
    '_folders_are_hardlink_mirror',
    '_materialize_hardlink_mirror',
    '_matched_destination_conflict_folders',
    '_build_source_dupe_candidate_from_item',
    '_dupe_candidate_track_count',
    '_dupe_candidate_preview_score',
    '_dupe_candidate_folder',
    '_choose_matched_export_conflict_winner',
    '_export_item_scan_id',
    '_stable_album_id_for_review',
    '_record_matched_export_conflict_review',
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

def _sanitize_export_component(s: str, max_len: int = 200) -> str:
    """Replace filesystem-illegal characters and truncate for use in export paths."""
    if not s:
        return "Unknown"
    # Strip and replace chars that are problematic on Windows/Unix
    bad = re.compile(r'[\x00/\\:*?"<>|]')
    out = bad.sub("_", str(s).strip())
    out = " ".join(out.split())
    return _truncate_utf8_component(out or "Unknown", max_len)

def build_export_path(
    edition: dict,
    track_index: int,
    source_path: Path,
    template: str,
    export_root: str,
) -> Path:
    """
    Build the target path for one track under EXPORT_ROOT using EXPORT_NAMING_TEMPLATE.
    Placeholders: {letter}, {artist}, {album}, {year}, {disc}, {track}, {title}, {format}, {ext}.
    """
    artist = _sanitize_export_component(edition.get("artist_name") or "Unknown Artist")
    album = _sanitize_export_component(edition.get("album_title") or "Unknown Album")
    tags = edition.get("tags") or {}
    year = (tags.get("date") or tags.get("year") or "").strip()[:4] or ""
    fmt = (edition.get("format") or "UNKNOWN").upper()
    ext = (source_path.suffix or "").lower()
    if not ext or ext not in [x.lower() for x in [".flac", ".mp3", ".m4a", ".ogg", ".opus", ".wav", ".aac"]]:
        ext = ".flac"
    tracks_list = edition.get("tracks") or []
    disc_num = 1
    track_num = track_index + 1
    title = f"Track {track_num}"
    if track_index < len(tracks_list):
        t = tracks_list[track_index]
        disc_num = getattr(t, "disc", 1) or 1
        track_num = getattr(t, "idx", track_index + 1) or (track_index + 1)
        title = _sanitize_export_component(getattr(t, "title", "") or title)
    # First letter of artist for folder (A-Z or 0-9)
    first = (artist or "U")[0].upper()
    if not first.isalnum():
        first = "0"
    # Simple placeholder substitution (no format spec like :02d in template for now)
    subs = {
        "letter": first,
        "artist": artist,
        "album": album,
        "year": year,
        "disc": str(disc_num),
        "track": f"{track_num:02d}",
        "title": title,
        "format": fmt,
        "ext": ext,
    }
    path_str = template
    for k, v in subs.items():
        path_str = path_str.replace("{" + k + "}", v)
    # Handle {track:02d} style if present
    path_str = re.sub(r"\{track:02d\}", f"{track_num:02d}", path_str)
    out = Path(export_root) / path_str.lstrip("/")
    return out

def _format_label_for_folder(format_value: str) -> str:
    raw = str(format_value or "").strip().upper()
    if not raw or raw == "UNKNOWN":
        return "Unknown"
    pretty_map = {
        "FLAC": "Flac",
        "MP3": "Mp3",
        "M4A": "M4a",
        "AAC": "Aac",
        "ALAC": "Alac",
        "WAV": "Wav",
        "AIFF": "Aiff",
        "APE": "Ape",
        "WV": "Wv",
        "OGG": "Ogg",
        "OPUS": "Opus",
        "DSD": "Dsd",
        "DSF": "Dsf",
        "DFF": "Dff",
    }
    return pretty_map.get(raw, raw[:1] + raw[1:].lower())

def _derive_album_type_for_folder_name(
    *,
    track_count: int = 0,
    tags: dict | None = None,
    album_title: str = "",
    artist_name: str = "",
) -> str:
    tag_map = {str(k or "").strip().lower(): str(v or "").strip() for k, v in dict(tags or {}).items()}
    title_low = _sanitize_album_title_display(str(album_title or "")).lower()
    artist_low = str(artist_name or "").strip().lower()
    genre_low = str(tag_map.get("genre") or "").lower()
    compilation_flag = str(tag_map.get("compilation") or "").strip().lower() in {"1", "true", "yes"}
    if compilation_flag or artist_low in {"various artists", "various"}:
        return "Compilation"
    if any(token in title_low for token in ("soundtrack", "original soundtrack", "motion picture score")) or "soundtrack" in genre_low:
        return "Soundtrack"
    if "anthology" in title_low:
        return "Anthology"
    if re.search(r"\blive\b", title_low):
        return "Live"
    count = max(0, int(track_count or 0))
    if 0 < count <= 3:
        return "Single"
    if 0 < count <= 6:
        return "EP"
    return "Album"

def _build_matched_album_folder_name(
    *,
    artist_name: str,
    album_title: str,
    format_value: str = "",
    album_type: str = "",
    include_format: bool | None = None,
    include_type: bool | None = None,
) -> str:
    include_format = EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER if include_format is None else bool(include_format)
    include_type = EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER if include_type is None else bool(include_type)
    base = _sanitize_path_component(_sanitize_album_title_display(album_title or "") or "Unknown Album")
    suffix_parts: list[str] = []
    if include_format:
        suffix_parts.append(_format_label_for_folder(format_value))
    if include_type:
        suffix_parts.append(str(album_type or "Album").strip() or "Album")
    if suffix_parts:
        return _sanitize_path_component(f"{base} ({',  '.join(suffix_parts)})")
    return _sanitize_path_component(base)

def _matched_album_family_key(artist_name: str, album_title: str) -> str:
    artist_norm = _norm_artist_key(str(artist_name or "").strip())
    title_norm = norm_album_for_dedup(
        strip_parenthetical_suffixes(_sanitize_album_title_display(str(album_title or "").strip())),
        normalize_parenthetical=True,
    )
    if not artist_norm or not title_norm:
        return ""
    return f"{artist_norm}::{title_norm}"

def _matched_export_target_folder(
    *,
    artist_name: str,
    album_title: str,
    format_value: str = "",
    album_type: str = "",
    export_root: str | None = None,
) -> Path:
    root = Path(str(export_root or EXPORT_ROOT or "").strip() or "/music/Music_matched")
    artist_clean = _sanitize_path_component(str(artist_name or "").strip() or "Unknown Artist")
    letter = _artist_letter_bucket(artist_clean)
    album_dir = _build_matched_album_folder_name(
        artist_name=artist_clean,
        album_title=album_title,
        format_value=format_value,
        album_type=album_type,
    )
    return root / letter / artist_clean / album_dir

def _materialization_confidence_policy(item: dict | None) -> dict[str, Any]:
    """
    Classify whether an album can be materialized automatically.

    The move/copy/hardlink step is intentionally stricter than browse visibility:
    provider metadata can make an album useful in the UI, but filesystem mutation
    requires deterministic identity. Soft provider evidence stays review-only.
    """
    return _materialization_policy_core.confidence_policy(item)

def _path_is_under_root(path_obj: Path | str | None, root_obj: Path | str | None) -> bool:
    if not path_obj or not root_obj:
        return False
    try:
        p = path_for_fs_access(Path(path_obj)).resolve()
        r = path_for_fs_access(Path(root_obj)).resolve()
        p.relative_to(r)
        return True
    except Exception:
        return False

def _build_dupe_candidate_from_folder(
    folder: Path,
    *,
    artist_hint: str = "",
    album_hint: str = "",
) -> dict | None:
    folder_path = path_for_fs_access(Path(folder))
    if not folder_path.exists() or not folder_path.is_dir():
        return None
    ordered_paths = _files_collect_ordered_audio_paths(folder_path, [])
    if not ordered_paths:
        return None
    first_tags = {}
    try:
        first_tags = extract_tags(ordered_paths[0]) or {}
    except Exception:
        first_tags = {}
    artist_name = str(
        _pick_album_artist_from_tag_dicts([first_tags], default=str(artist_hint or "").strip() or folder_path.parent.name)
        or str(artist_hint or "").strip()
        or folder_path.parent.name
        or "Unknown Artist"
    ).strip()
    album_title = _sanitize_album_title_display(
        str(
            _pick_album_title_from_tag_dicts(
                [first_tags],
                fallback=str(album_hint or "").strip() or strip_parenthetical_suffixes(folder_path.name),
            )
            or str(album_hint or "").strip()
            or strip_parenthetical_suffixes(folder_path.name)
            or folder_path.name
        ).strip()
    )
    track_entries: list[dict] = []
    for idx, audio_path in enumerate(ordered_paths, start=1):
        track_tags = {}
        try:
            track_tags = extract_tags(audio_path) or {}
        except Exception:
            track_tags = {}
        title = str(track_tags.get("title") or audio_path.stem or f"Track {idx}").strip()
        disc_num = _parse_int_loose(track_tags.get("discnumber") or track_tags.get("disc"), 1) or 1
        track_num = _parse_int_loose(track_tags.get("tracknumber") or track_tags.get("track"), idx) or idx
        track_entries.append({"title": title, "idx": track_num, "disc": disc_num})
    try:
        missing_required = _check_required_tags(first_tags, REQUIRED_TAGS, edition={"tracks": list(track_entries)})
    except Exception:
        missing_required = []
    try:
        fmt_score, br, sr, bd, _cache_hit = analyse_format(folder_path)
    except Exception:
        fmt_score, br, sr, bd = 0, 0, 0, 0
    try:
        has_cover = bool(album_folder_has_cover(folder_path))
    except Exception:
        has_cover = False
    try:
        artist_folder = _files_guess_artist_folder(folder_path, artist_name, root_dirs=_files_root_dir_strings())
        local_artist_img = _first_artist_image_path(artist_folder) if artist_folder else None
        if local_artist_img and local_artist_img.is_file():
            has_artist_image = True
        else:
            effective_artist_img = _files_effective_artist_image_path(folder_path, artist_name, _norm_artist_key(artist_name))
            has_artist_image = bool(effective_artist_img and effective_artist_img.is_file())
    except Exception:
        has_artist_image = False
    identity_fields = _extract_files_identity_fields(tags=first_tags, edition={}, cached={})
    return {
        "folder": str(folder_path),
        "artist_name": artist_name,
        "title_raw": album_title,
        "album_title": album_title,
        "tracks": track_entries,
        "file_count": len(ordered_paths),
        "missing_required_tags": list(missing_required or []),
        "has_cover": bool(has_cover),
        "has_artist_image": bool(has_artist_image),
        "fmt_score": int(fmt_score or 0),
        "fmt_text": get_primary_format(folder_path),
        "format": get_primary_format(folder_path),
        "br": int(br or 0),
        "sr": int(sr or 0),
        "bd": int(bd or 0),
        "meta": dict(first_tags or {}),
        "musicbrainz_id": identity_fields.get("musicbrainz_id") or "",
        "musicbrainz_release_id": identity_fields.get("musicbrainz_release_id") or "",
        "discogs_release_id": identity_fields.get("discogs_release_id") or "",
        "lastfm_album_mbid": identity_fields.get("lastfm_album_mbid") or "",
        "bandcamp_album_url": identity_fields.get("bandcamp_album_url") or "",
        "metadata_source": identity_fields.get("metadata_source") or "",
        "strict_match_verified": bool(identity_fields.get("strict_match_verified")),
        "strict_match_provider": identity_fields.get("strict_match_provider") or "",
    }

def _move_folder_to_dupes(
    folder: Path,
    *,
    artist_hint: str = "",
    album_hint: str = "",
    reason: str = "matched_library_conflict",
) -> Path | None:
    src = path_for_fs_access(Path(folder))
    if not src.exists() or not src.is_dir():
        return None
    dst = _next_available_folder_path(
        build_dupe_destination(src, artist_hint=artist_hint, album_hint=album_hint)
    )
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        _files_watcher_suppress_folder(src, seconds=180.0, reason=reason)
    except Exception:
        pass
    safe_move(str(src), str(dst))
    try:
        _files_watcher_suppress_folder(dst, seconds=180.0, reason=reason)
    except Exception:
        pass
    _files_forget_album_folder_global(src)
    logging.info(
        "[DUPES] [X❌] sent duplicate loser to quarantine: %s -> %s",
        src,
        dst,
    )
    return dst

def _record_scan_move_event(
    *,
    scan_id_override: int | None = None,
    artist_name: str,
    album_id: int,
    album_title: str,
    fmt_text: str,
    original_path: str,
    moved_to_path: str,
    size_mb: int,
    move_reason: str,
    winner_album_id: int | None = None,
    winner_title: str = "",
    winner_path: str = "",
    decision_source: str = "",
    decision_provider: str = "",
    decision_reason: str = "",
    decision_confidence: float | None = None,
    materialization_strategy: str = "",
    arbitration_result: str = "",
    details: dict | None = None,
) -> bool:
    try:
        scan_id = int(scan_id_override or 0)
    except Exception:
        scan_id = 0
    if scan_id <= 0:
        with lock:
            scan_id = int(state.get("scan_id") or 0)
    if scan_id <= 0:
        return False
    try:
        def _write_move() -> str:
            con = _state_connect()
            try:
                cur = con.cursor()
                _insert_scan_move_row(
                    cur,
                    scan_id=scan_id,
                    artist=str(artist_name or ""),
                    album_id=int(album_id or 0),
                    original_path=str(original_path or ""),
                    moved_to_path=str(moved_to_path or ""),
                    size_mb=int(size_mb or 0),
                    moved_at=time.time(),
                    album_title=str(album_title or ""),
                    fmt_text=str(fmt_text or ""),
                    move_reason=str(move_reason or "").strip().lower() or "matched_export",
                    winner_album_id=winner_album_id,
                    winner_title=str(winner_title or ""),
                    winner_path=str(winner_path or ""),
                    decision_source=str(decision_source or ""),
                    decision_provider=str(decision_provider or ""),
                    decision_reason=str(decision_reason or ""),
                    decision_confidence=decision_confidence,
                    source_path=str(original_path or ""),
                    destination_path=str(moved_to_path or ""),
                    materialization_strategy=str(materialization_strategy or "").strip().lower(),
                    arbitration_result=str(arbitration_result or "").strip().lower(),
                    details=details or {},
                )
                move_status = _scan_move_status(False, str(original_path or ""), str(moved_to_path or ""))
                con.commit()
                return move_status
            finally:
                con.close()

        move_status = _state_db_write_retry(
            _write_move,
            label=f"record_scan_move_event:{scan_id}:{album_id}",
            attempts=12,
        )
        synced = _sync_scan_pipeline_trace_move_rows(
            int(scan_id),
            album_ids=[int(album_id or 0)],
            wait_timeout_sec=5.0,
            poll_interval_sec=0.25,
        )
        if not synced:
            logging.info(
                "[Trace] »⏭ Move trace sync deferred scan_id=%s album_id=%s reason=%s",
                int(scan_id),
                int(album_id or 0),
                str(move_reason or "").strip().lower() or "matched_export",
            )
        return True
    except Exception as exc:
        logging.warning("Matched library trace write failed for %s – %s: %s", artist_name, album_title, exc)
        return False

def _folder_file_stat_map(root: Path) -> dict[str, tuple[int, int, int]]:
    out: dict[str, tuple[int, int, int]] = {}
    base = path_for_fs_access(Path(root))
    if not base.exists() or not base.is_dir():
        return out
    for entry in sorted(base.rglob("*"), key=lambda p: str(p)):
        if not entry.is_file():
            continue
        rel = str(entry.relative_to(base)).replace("\\", "/")
        st = entry.stat()
        out[rel] = (int(st.st_dev), int(st.st_ino), int(st.st_size))
    return out

def _folders_are_hardlink_mirror(src: Path, dst: Path) -> bool:
    try:
        src_map = _folder_file_stat_map(src)
        dst_map = _folder_file_stat_map(dst)
    except Exception:
        return False
    if not src_map or not dst_map or set(src_map.keys()) != set(dst_map.keys()):
        return False
    for rel, src_stat in src_map.items():
        if dst_map.get(rel) != src_stat:
            return False
    return True

def _materialize_hardlink_mirror(src: Path, dst: Path, strategy: str) -> Path:
    mode = str(strategy or "").strip().lower()
    if mode not in {"move", "copy"}:
        return dst
    if not dst.exists() or not dst.is_dir() or not _folders_are_hardlink_mirror(src, dst):
        return dst
    logging.info("[MATERIALIZE] [↻🔄] reconciling hardlink -> %s for exported album: %s -> %s", mode, src, dst)
    shutil.rmtree(dst)
    if mode == "move":
        safe_move(str(src), str(dst))
    else:
        shutil.copytree(str(src), str(dst))
    logging.info("[MATERIALIZE] [V✅] replaced hardlink mirror with %s payload: %s -> %s", mode, src, dst)
    return dst

def _matched_destination_conflict_folders(
    target_folder: Path,
    *,
    artist_name: str,
    album_title: str,
) -> list[Path]:
    artist_dir = target_folder.parent
    if not artist_dir.exists() or not artist_dir.is_dir():
        return []
    wanted_family = _matched_album_family_key(artist_name, album_title)
    if not wanted_family:
        return [p for p in [target_folder] if p.exists()]
    out: list[Path] = []
    for child in artist_dir.iterdir():
        if not child.is_dir():
            continue
        family = _matched_album_family_key(artist_name, strip_parenthetical_suffixes(child.name))
        if family == wanted_family:
            out.append(child)
    return sorted(out, key=lambda p: str(p))

def _build_source_dupe_candidate_from_item(item: dict, folder: Path, *, artist_name: str, album_title: str) -> dict | None:
    folder_path = path_for_fs_access(Path(folder))
    if not folder_path.exists() or not folder_path.is_dir():
        return None
    ordered_paths = _files_collect_ordered_audio_paths(folder_path, item.get("ordered_paths") or [])
    if not ordered_paths:
        return None
    tags = dict(item.get("meta") or {})
    try:
        if ordered_paths:
            live_tags = extract_tags(ordered_paths[0]) or {}
            if live_tags:
                tags.update(live_tags)
    except Exception:
        pass
    try:
        missing_required = _check_required_tags(tags, REQUIRED_TAGS, edition={"tracks": list(item.get("tracks") or [])})
    except Exception:
        missing_required = list(item.get("pre_missing_required_tags") or [])
    try:
        has_cover = bool(album_folder_has_cover(folder_path))
    except Exception:
        has_cover = bool(item.get("pre_has_cover"))
    try:
        artist_folder = _files_guess_artist_folder(folder_path, artist_name, root_dirs=_files_root_dir_strings())
        local_artist_img = _first_artist_image_path(artist_folder) if artist_folder else None
        if local_artist_img and local_artist_img.is_file():
            has_artist_image = True
        else:
            effective_artist_img = _files_effective_artist_image_path(folder_path, artist_name, _norm_artist_key(artist_name))
            has_artist_image = bool(effective_artist_img and effective_artist_img.is_file())
    except Exception:
        has_artist_image = bool(item.get("pre_has_artist_image"))
    return {
        "folder": str(folder_path),
        "artist_name": artist_name,
        "title_raw": album_title,
        "album_title": album_title,
        "tracks": list(item.get("tracks") or []),
        "file_count": len(ordered_paths),
        "missing_required_tags": list(missing_required or []),
        "has_cover": bool(has_cover),
        "has_artist_image": bool(has_artist_image),
        "fmt_score": int(score_format(str(item.get("fmt_text") or get_primary_format(folder_path)).lower()) or 0),
        "fmt_text": str(item.get("fmt_text") or get_primary_format(folder_path) or ""),
        "format": str(item.get("fmt_text") or get_primary_format(folder_path) or ""),
        "br": int(item.get("br") or 0),
        "sr": int(item.get("sr") or 0),
        "bd": int(item.get("bd") or 0),
        "meta": dict(tags or {}),
        "musicbrainz_id": str(item.get("musicbrainz_id") or "").strip(),
        "musicbrainz_release_id": str(item.get("musicbrainz_release_id") or "").strip(),
        "discogs_release_id": str(item.get("discogs_release_id") or "").strip(),
        "lastfm_album_mbid": str(item.get("lastfm_album_mbid") or "").strip(),
        "bandcamp_album_url": str(item.get("bandcamp_album_url") or "").strip(),
        "metadata_source": str(item.get("metadata_source") or item.get("primary_metadata_source") or "").strip(),
        "strict_match_verified": bool(item.get("strict_match_verified")),
        "strict_match_provider": str(item.get("strict_match_provider") or "").strip(),
        "strict_tracklist_score": float(item.get("strict_tracklist_score") or 0.0),
    }

def _dupe_candidate_track_count(candidate: dict | None) -> int:
    if not isinstance(candidate, dict):
        return 0
    tracks = candidate.get("tracks")
    if isinstance(tracks, list) and tracks:
        return len(tracks)
    return _parse_int_loose(candidate.get("file_count"), 0)

def _dupe_candidate_preview_score(candidate: dict | None) -> int:
    if not isinstance(candidate, dict):
        return 0
    preview_tokens = ("preview", "snippet", "sample", "excerpt", "teaser")
    identity_parts: list[str] = []
    for key in ("folder", "title_raw", "album_title", "folder_name"):
        value = candidate.get(key)
        if value:
            identity_parts.append(str(value))
    identity_haystack = " ".join(identity_parts).lower()
    identity_penalty = 100 if any(tok in identity_haystack for tok in preview_tokens) else 0

    tracks = candidate.get("tracks")
    track_texts: list[str] = []
    if isinstance(tracks, list):
        for track in tracks:
            if isinstance(track, dict):
                track_texts.append(f"{track.get('title') or ''} {track.get('file') or track.get('path') or ''}")
            elif track:
                track_texts.append(str(track))
    if track_texts:
        hits = sum(1 for text in track_texts if any(tok in text.lower() for tok in preview_tokens))
        return max(identity_penalty, int(round((hits / max(1, len(track_texts))) * 100)))
    return identity_penalty

def _dupe_candidate_folder(candidate: dict | None) -> str:
    if not isinstance(candidate, dict):
        return ""
    return str(candidate.get("folder") or "").strip()

def _choose_matched_export_conflict_winner(
    source_candidate: dict,
    existing_candidates: list[dict],
) -> tuple[dict | None, str, bool]:
    """
    Pick a safe winner for a matched-library destination collision.

    Export collisions are more dangerous than normal duplicate grouping because a
    wrong choice can immediately move a good source folder to /dupes. Keep the
    normal heuristic, but never auto-move on an unconfident pick, and override
    obvious preview/track-count mistakes in favour of the incoming full edition.
    """
    all_candidates = [source_candidate, *list(existing_candidates or [])]
    best, rationale, _merge, confident = _dupe_choose_best_heuristic(all_candidates)
    if not best:
        return None, str(rationale or "no export conflict winner"), False

    source_folder = _dupe_candidate_folder(source_candidate)
    best_folder = _dupe_candidate_folder(best)
    source_tracks = _dupe_candidate_track_count(source_candidate)
    best_tracks = _dupe_candidate_track_count(best)
    source_preview = _dupe_candidate_preview_score(source_candidate)
    best_preview = _dupe_candidate_preview_score(best)
    best_is_source = bool(source_folder and best_folder == source_folder)

    if not best_is_source:
        source_materially_more_complete = source_tracks >= best_tracks + 2
        source_not_preview_only = source_preview < 100
        existing_looks_preview = best_preview > source_preview
        if source_not_preview_only and source_tracks > best_tracks and (source_materially_more_complete or existing_looks_preview):
            return (
                source_candidate,
                (
                    f"{rationale}; export conflict override: incoming edition has "
                    f"{source_tracks} track(s) vs existing {best_tracks}, preview_score "
                    f"{source_preview} vs {best_preview}"
                ),
                True,
            )

    if not confident:
        return (
            None,
            (
                f"{rationale}; export conflict held for review: heuristic was not confident "
                f"(source_tracks={source_tracks}, best_tracks={best_tracks}, "
                f"source_preview={source_preview}, best_preview={best_preview})"
            ),
            False,
        )

    return best, str(rationale or "export conflict winner"), True

def _export_item_scan_id(item: dict | None, scan_id_override: int | None = None) -> int:
    try:
        override = int(scan_id_override or 0)
    except Exception:
        override = 0
    if override > 0:
        return override
    try:
        return int((item or {}).get("scan_id") or 0)
    except Exception:
        return 0

def _stable_album_id_for_review(item: dict | None, folder_path: str) -> int:
    try:
        album_id = int((item or {}).get("album_id") or (item or {}).get("source_id") or 0)
    except Exception:
        album_id = 0
    if album_id > 0:
        return album_id
    raw = str(folder_path or "").encode("utf-8", errors="ignore")
    return int(zlib.crc32(raw) & 0x7FFFFFFF) or 1

def _record_matched_export_conflict_review(
    *,
    item: dict,
    source_candidate: dict,
    existing_candidates: list[dict],
    artist_name: str,
    album_title: str,
    fmt_text: str,
    rationale: str,
    decision_provider: str,
    export_strategy: str,
    scan_id_override: int | None = None,
) -> None:
    source_path = str(source_candidate.get("folder") or item.get("folder") or "").strip()
    album_id = _stable_album_id_for_review(item, source_path)
    scan_id = _export_item_scan_id(item, scan_id_override)
    evidence = {
        "kind": "matched_export_destination_conflict",
        "source": source_path,
        "existing_candidates": [
            {
                "folder": str(candidate.get("folder") or ""),
                "title": str(candidate.get("title_raw") or candidate.get("album_title") or ""),
                "format": str(candidate.get("fmt_text") or candidate.get("format") or ""),
                "track_count": _dupe_candidate_track_count(candidate),
                "preview_score": _dupe_candidate_preview_score(candidate),
                "strict_match_provider": str(candidate.get("strict_match_provider") or ""),
                "metadata_source": str(candidate.get("metadata_source") or ""),
            }
            for candidate in (existing_candidates or [])
        ],
        "rationale": str(rationale or ""),
        "provider": str(decision_provider or ""),
        "export_strategy": str(export_strategy or ""),
        "scan_id": scan_id or None,
    }
    merge_list = [
        {"role": "incoming_strict_match", "folder": source_path},
        *[
            {"role": "existing_destination", "folder": str(candidate.get("folder") or "")}
            for candidate in (existing_candidates or [])
        ],
    ]

    def _write_review() -> None:
        con = _state_connect()
        try:
            cur = con.cursor()
            cur.execute(
                "DELETE FROM duplicates_loser WHERE artist = ? AND album_id = ?",
                (artist_name, album_id),
            )
            cur.execute(
                "DELETE FROM duplicates_best WHERE artist = ? AND album_id = ?",
                (artist_name, album_id),
            )
            cur.execute(
                """
                INSERT OR REPLACE INTO duplicates_best (
                    artist, album_id, title_raw, album_norm, folder, fmt_text, br, sr, bd,
                    dur, discs, rationale, merge_list, ai_used, meta_json, evidence_json,
                    size_mb, track_count, match_verified_by_ai, dupe_signal, no_move,
                    manual_review, same_folder
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, 0, ?, 1, 1, 0)
                """,
                (
                    artist_name,
                    album_id,
                    album_title,
                    norm_album_for_dedup(album_title, normalize_parenthetical=True),
                    source_path,
                    fmt_text,
                    int(source_candidate.get("br") or 0),
                    int(source_candidate.get("sr") or 0),
                    int(source_candidate.get("bd") or 0),
                    0,
                    1,
                    str(rationale or "Matched export destination conflict needs review"),
                    json.dumps(merge_list, ensure_ascii=False, default=str),
                    json.dumps(source_candidate.get("meta") or {}, ensure_ascii=False, default=str),
                    json.dumps(evidence, ensure_ascii=False, default=str),
                    max(0, folder_size(path_for_fs_access(Path(source_path))) // (1024 * 1024)) if source_path else 0,
                    _dupe_candidate_track_count(source_candidate),
                    "matched_export_conflict",
                ),
            )
            for candidate in existing_candidates or []:
                folder_raw = str(candidate.get("folder") or "").strip()
                if not folder_raw:
                    continue
                loser_id = _stable_album_id_for_review(candidate, folder_raw)
                cur.execute(
                    """
                    INSERT INTO duplicates_loser (
                        artist, album_id, folder, fmt_text, br, sr, bd, size_mb, loser_album_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        artist_name,
                        album_id,
                        folder_raw,
                        str(candidate.get("fmt_text") or candidate.get("format") or ""),
                        int(candidate.get("br") or 0),
                        int(candidate.get("sr") or 0),
                        int(candidate.get("bd") or 0),
                        max(0, folder_size(path_for_fs_access(Path(folder_raw))) // (1024 * 1024)),
                        loser_id,
                    ),
                )
            if scan_id > 0:
                _insert_scan_move_row(
                    cur,
                    scan_id=scan_id,
                    artist=artist_name,
                    album_id=album_id,
                    original_path=source_path,
                    moved_to_path="",
                    size_mb=max(0, folder_size(path_for_fs_access(Path(source_path))) // (1024 * 1024)) if source_path else 0,
                    moved_at=time.time(),
                    album_title=album_title,
                    fmt_text=fmt_text,
                    move_reason="matched_export_conflict",
                    winner_album_id=None,
                    winner_title="",
                    winner_path="",
                    decision_source="pipeline_export",
                    decision_provider=decision_provider,
                    decision_reason=str(rationale or "held_for_review"),
                    decision_confidence=None,
                    source_path=source_path,
                    destination_path="",
                    materialization_strategy=export_strategy,
                    arbitration_result="held_for_review",
                    details=evidence,
                )
            con.commit()
        finally:
            con.close()

    try:
        _state_db_write_retry(
            _write_review,
            label=f"matched_export_conflict_review:{artist_name}:{album_id}",
            attempts=12,
        )
        logging.info(
            "[DUPES] [!⚠] matched export conflict queued for review: %s – %s (%d existing candidate(s))",
            artist_name,
            album_title,
            len(existing_candidates or []),
        )
    except Exception:
        logging.warning(
            "Failed to persist matched export conflict review for %s – %s",
            artist_name,
            album_title,
            exc_info=True,
        )

def _sanitize_export_component_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _sanitize_export_component(*args, **kwargs)

def build_export_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return build_export_path(*args, **kwargs)

def _format_label_for_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _format_label_for_folder(*args, **kwargs)

def _derive_album_type_for_folder_name_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _derive_album_type_for_folder_name(*args, **kwargs)

def _build_matched_album_folder_name_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_matched_album_folder_name(*args, **kwargs)

def _matched_album_family_key_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _matched_album_family_key(*args, **kwargs)

def _matched_export_target_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _matched_export_target_folder(*args, **kwargs)

def _materialization_confidence_policy_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _materialization_confidence_policy(*args, **kwargs)

def _path_is_under_root_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _path_is_under_root(*args, **kwargs)

def _build_dupe_candidate_from_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_dupe_candidate_from_folder(*args, **kwargs)

def _move_folder_to_dupes_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _move_folder_to_dupes(*args, **kwargs)

def _record_scan_move_event_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _record_scan_move_event(*args, **kwargs)

def _folder_file_stat_map_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _folder_file_stat_map(*args, **kwargs)

def _folders_are_hardlink_mirror_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _folders_are_hardlink_mirror(*args, **kwargs)

def _materialize_hardlink_mirror_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _materialize_hardlink_mirror(*args, **kwargs)

def _matched_destination_conflict_folders_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _matched_destination_conflict_folders(*args, **kwargs)

def _build_source_dupe_candidate_from_item_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_source_dupe_candidate_from_item(*args, **kwargs)

def _dupe_candidate_track_count_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _dupe_candidate_track_count(*args, **kwargs)

def _dupe_candidate_preview_score_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _dupe_candidate_preview_score(*args, **kwargs)

def _dupe_candidate_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _dupe_candidate_folder(*args, **kwargs)

def _choose_matched_export_conflict_winner_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _choose_matched_export_conflict_winner(*args, **kwargs)

def _export_item_scan_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _export_item_scan_id(*args, **kwargs)

def _stable_album_id_for_review_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _stable_album_id_for_review(*args, **kwargs)

def _record_matched_export_conflict_review_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _record_matched_export_conflict_review(*args, **kwargs)

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}
