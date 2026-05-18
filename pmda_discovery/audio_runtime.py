"""Runtime-bound audio metadata and filesystem album grouping helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_fpcalc_fingerprint_file',
    'analyse_format',
    '_run_ffprobe_duration_sec',
    '_run_ffprobe',
    '_group_audio_files_by_folder_under_roots',
    '_resolve_album_folders_from_event_path',
    'extract_tags',
    '_iter_audio_files_under_roots',
    '_files_child_folder_name_looks_release_segment',
    '_folder_release_segment_child_dirs',
    '_folder_has_release_segment_children',
    '_collapse_nested_album_folder_groups',
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

def extract_tags(audio_path: Path) -> dict[str, str]:
    """
    Return *all* container‑level metadata tags for the given audio file
    (FLAC/MP3/M4A/…).

    Uses ffprobe so no external Python deps are required.
    """
    try:
        out = subprocess.check_output(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format_tags:format=duration",
                "-of", "default=noprint_wrappers=1",
                str(audio_path)
            ],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10
        )
        tags = {}
        for line in out.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                # ffprobe returns TAG:KEY=VAL sometimes – strip the prefix
                if k.startswith("TAG:"):
                    k = k[4:]
                tags[k.lower()] = v.strip()
        duration_sec = _parse_duration_seconds_loose(tags.get("duration"), 0.0)
        if duration_sec <= 0:
            duration_sec = float(_run_ffprobe_duration_sec(str(audio_path)) or 0)
        if duration_sec > 0:
            tags["duration"] = str(int(max(1, round(duration_sec))))
        identity_keys = {
            "musicbrainz_releasegroupid",
            "musicbrainz_release_group_id",
            "musicbrainz_releaseid",
            "musicbrainz_release_id",
            "musicbrainz_albumid",
            "musicbrainz_id",
            "discogs_release_id",
            "lastfm_album_mbid",
            "bandcamp_album_url",
            PMDA_ID_TAG,
            PMDA_MATCH_PROVIDER_TAG,
            PMDA_COVER_PROVIDER_TAG,
            PMDA_ARTIST_PROVIDER_TAG,
            PMDA_COMPLETE_TAG,
        }
        suffix = str(audio_path.suffix or "").lower()
        need_mutagen_fallback = suffix in {".mp3", ".m4a", ".mp4", ".aac"} and not any(tags.get(k) for k in identity_keys)
        if need_mutagen_fallback:
            try:
                from mutagen import File as MutagenFile  # type: ignore
                from mutagen.id3 import ID3, TXXX  # type: ignore
                from mutagen.mp3 import MP3  # type: ignore
                from mutagen.mp4 import MP4  # type: ignore
            except Exception:
                MutagenFile = ID3 = TXXX = MP3 = MP4 = None  # type: ignore[assignment]

            def _norm_custom_key(key: str) -> str:
                raw = str(key or "").strip().lower()
                raw = raw.replace("----:com.apple.itunes:", "")
                raw = raw.replace("tag:", "")
                raw = re.sub(r"[\s:/\-]+", "_", raw)
                raw = re.sub(r"_+", "_", raw).strip("_")
                aliases = {
                    "musicbrainz_release_group_id": "musicbrainz_releasegroupid",
                    "musicbrainz_releasegroupid": "musicbrainz_releasegroupid",
                    "musicbrainz_album_id": "musicbrainz_releaseid",
                    "musicbrainz_release_id": "musicbrainz_releaseid",
                    "musicbrainz_releaseid": "musicbrainz_releaseid",
                    "musicbrainz_artist_id": "musicbrainz_artistid",
                    "musicbrainz_album_artist_id": "musicbrainz_albumartistid",
                    "discogs_release_id": "discogs_release_id",
                    "lastfm_album_mbid": "lastfm_album_mbid",
                    "bandcamp_album_url": "bandcamp_album_url",
                    "pmda_id": PMDA_ID_TAG,
                    "pmda_match_provider": PMDA_MATCH_PROVIDER_TAG,
                    "pmda_cover_provider": PMDA_COVER_PROVIDER_TAG,
                    "pmda_artist_provider": PMDA_ARTIST_PROVIDER_TAG,
                    "pmda_complete": PMDA_COMPLETE_TAG,
                }
                return aliases.get(raw, raw)

            def _first_scalar(value: object) -> str:
                if value is None:
                    return ""
                if isinstance(value, bytes):
                    try:
                        return value.decode("utf-8", "ignore").strip()
                    except Exception:
                        return ""
                if isinstance(value, (list, tuple)):
                    for item in value:
                        txt = _first_scalar(item)
                        if txt:
                            return txt
                    return ""
                return str(value).strip()

            def _merge_if_missing(key: str, value: object) -> None:
                norm_key = _norm_custom_key(key)
                txt = _first_scalar(value)
                if not norm_key or not txt:
                    return
                if not tags.get(norm_key):
                    tags[norm_key] = txt

            try:
                audio = MutagenFile(str(audio_path), easy=False) if MutagenFile is not None else None
            except Exception:
                audio = None
            tag_obj = getattr(audio, "tags", None) if audio is not None else None
            if tag_obj:
                try:
                    if ID3 is not None and isinstance(tag_obj, ID3):
                        for frame in tag_obj.values():
                            if TXXX is not None and isinstance(frame, TXXX):
                                _merge_if_missing(str(getattr(frame, "desc", "") or ""), getattr(frame, "text", None))
                                continue
                            frame_id = str(getattr(frame, "FrameID", "") or "")
                            if frame_id == "TIT2":
                                _merge_if_missing("title", getattr(frame, "text", None))
                            elif frame_id == "TPE1":
                                _merge_if_missing("artist", getattr(frame, "text", None))
                            elif frame_id == "TPE2":
                                _merge_if_missing("albumartist", getattr(frame, "text", None))
                            elif frame_id == "TALB":
                                _merge_if_missing("album", getattr(frame, "text", None))
                            elif frame_id == "TDRC":
                                _merge_if_missing("date", getattr(frame, "text", None))
                            elif frame_id == "TCON":
                                _merge_if_missing("genre", getattr(frame, "text", None))
                            elif frame_id == "TRCK":
                                _merge_if_missing("tracknumber", getattr(frame, "text", None))
                            elif frame_id == "TPOS":
                                _merge_if_missing("discnumber", getattr(frame, "text", None))
                    elif MP4 is not None and isinstance(audio, MP4):
                        for raw_key, raw_val in dict(tag_obj).items():
                            key_lower = str(raw_key or "").lower()
                            if key_lower == "\xa9nam":
                                _merge_if_missing("title", raw_val)
                            elif key_lower == "\xa9art":
                                _merge_if_missing("artist", raw_val)
                            elif key_lower == "aart":
                                _merge_if_missing("albumartist", raw_val)
                            elif key_lower == "\xa9alb":
                                _merge_if_missing("album", raw_val)
                            elif key_lower == "\xa9day":
                                _merge_if_missing("date", raw_val)
                            elif key_lower == "\xa9gen":
                                _merge_if_missing("genre", raw_val)
                            elif key_lower == "trkn":
                                if isinstance(raw_val, list) and raw_val:
                                    pair = raw_val[0]
                                    if isinstance(pair, tuple) and pair:
                                        _merge_if_missing("tracknumber", pair[0])
                            elif key_lower == "disk":
                                if isinstance(raw_val, list) and raw_val:
                                    pair = raw_val[0]
                                    if isinstance(pair, tuple) and pair:
                                        _merge_if_missing("discnumber", pair[0])
                            elif key_lower.startswith("----:com.apple.itunes:"):
                                _merge_if_missing(str(raw_key).split(":")[-1], raw_val)
                    else:
                        for raw_key, raw_val in dict(tag_obj).items():
                            _merge_if_missing(str(raw_key or ""), raw_val)
                except Exception:
                    logging.debug("mutagen tag fallback failed for %s", audio_path, exc_info=True)
        return tags
    except Exception:
        return {}


def _iter_audio_files_under_roots(
    roots: list[str],
    *,
    progress_cb=None,
    progress_every: int = 250,
    heartbeat_seconds: float = 10.0,
    stop_event: threading.Event | None = None,
    pause_event: threading.Event | None = None,
) -> list[Path]:
    """
    Return a list of audio files under the given filesystem roots.
    This helper is backend‑agnostic and only cares about AUDIO_RE matches.
    """
    roots_list = [str(r) for r in (roots or []) if r]
    roots_total = len(roots_list)
    if roots_total <= 0:
        return []

    progress_every = max(1, int(progress_every)) if progress_every else 0
    heartbeat_seconds = float(heartbeat_seconds or 0.0)
    progress_enabled = callable(progress_cb)

    progress_lock = threading.Lock()
    shared_entries_scanned = 0
    shared_files_found = 0
    shared_roots_done = 0

    def _emit_progress(
        *,
        root: str,
        root_entries_scanned: int,
        delta_entries: int = 0,
        delta_files: int = 0,
        done_root: bool = False,
    ) -> None:
        nonlocal shared_entries_scanned, shared_files_found, shared_roots_done
        if not progress_enabled:
            return
        with progress_lock:
            if delta_entries:
                shared_entries_scanned += max(0, int(delta_entries))
            if delta_files:
                shared_files_found += max(0, int(delta_files))
            if done_root:
                shared_roots_done += 1
            payload = {
                "root": root,
                "roots_done": shared_roots_done,
                "roots_total": roots_total,
                "files_found": shared_files_found,
                "entries_scanned": shared_entries_scanned,
                "root_entries_scanned": max(0, int(root_entries_scanned)),
                "done": shared_roots_done >= roots_total,
            }
        try:
            progress_cb(payload)
        except Exception:
            pass

    def _scan_single_root(root_path: str) -> list[Path]:
        base = Path(root_path)
        if stop_event is not None and stop_event.is_set():
            return []
        if pause_event is not None:
            while pause_event.is_set() and not (stop_event is not None and stop_event.is_set()):
                time.sleep(0.2)

        root_entries = 0
        root_audio_found = 0
        pending_entries = 0
        pending_files = 0
        interrupted = False
        local_out: list[Path] = []
        last_heartbeat = time.monotonic()

        def _flush(*, done_root: bool = False, force: bool = False) -> None:
            nonlocal pending_entries, pending_files, last_heartbeat
            if not progress_enabled:
                pending_entries = 0
                pending_files = 0
                return
            if not force and not done_root and pending_entries <= 0 and pending_files <= 0:
                return
            _emit_progress(
                root=str(base),
                root_entries_scanned=root_entries,
                delta_entries=pending_entries,
                delta_files=pending_files,
                done_root=done_root,
            )
            pending_entries = 0
            pending_files = 0
            last_heartbeat = time.monotonic()

        if not base.exists():
            logging.debug("FILES_ROOTS entry %s does not exist; skipping", root_path)
            _flush(done_root=True, force=True)
            return local_out

        stack: list[str] = [str(base)]
        while stack:
            if stop_event is not None and stop_event.is_set():
                interrupted = True
                break
            if pause_event is not None:
                while pause_event.is_set() and not (stop_event is not None and stop_event.is_set()):
                    time.sleep(0.2)
                if stop_event is not None and stop_event.is_set():
                    interrupted = True
                    break
            current = stack.pop()
            try:
                with os.scandir(current) as it:
                    for entry in it:
                        if stop_event is not None and stop_event.is_set():
                            interrupted = True
                            break
                        if pause_event is not None and pause_event.is_set():
                            while pause_event.is_set() and not (stop_event is not None and stop_event.is_set()):
                                time.sleep(0.2)
                            if stop_event is not None and stop_event.is_set():
                                interrupted = True
                                break
                        root_entries += 1
                        pending_entries += 1
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                stack.append(entry.path)
                            elif entry.is_file(follow_symlinks=False) and AUDIO_RE.search(entry.name):
                                local_out.append(Path(entry.path))
                                root_audio_found += 1
                                pending_files += 1
                                if progress_every > 0 and (root_audio_found % progress_every == 0):
                                    _flush()
                        except (OSError, PermissionError):
                            continue

                        if progress_enabled and heartbeat_seconds > 0:
                            now = time.monotonic()
                            if (now - last_heartbeat) >= heartbeat_seconds:
                                _flush()
            except (FileNotFoundError, NotADirectoryError, PermissionError, OSError):
                continue

        if interrupted:
            _flush(force=True)
        else:
            _flush(done_root=True, force=True)
        return local_out

    workers = 1 if roots_total <= 1 else min(2, roots_total)
    results_by_idx: dict[int, list[Path]] = {}
    if workers == 1:
        for idx, root_path in enumerate(roots_list):
            if stop_event is not None and stop_event.is_set():
                break
            if pause_event is not None:
                while pause_event.is_set() and not (stop_event is not None and stop_event.is_set()):
                    time.sleep(0.2)
                if stop_event is not None and stop_event.is_set():
                    break
            results_by_idx[idx] = _scan_single_root(root_path)
    else:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="pmda-files-discovery") as pool:
            future_to_idx = {
                pool.submit(_scan_single_root, root_path): idx
                for idx, root_path in enumerate(roots_list)
            }
            for fut in as_completed(future_to_idx):
                idx = future_to_idx[fut]
                try:
                    results_by_idx[idx] = fut.result()
                except Exception:
                    logging.debug("FILES discovery worker failed for root index %d", idx, exc_info=True)
                    results_by_idx[idx] = []

    merged: list[Path] = []
    seen_paths: set[str] = set()
    for idx in range(roots_total):
        for path in results_by_idx.get(idx, []):
            sp = str(path)
            if sp in seen_paths:
                continue
            seen_paths.add(sp)
            merged.append(path)
    return merged


def _files_child_folder_name_looks_release_segment(name: str) -> bool:
    txt = str(name or "").strip()
    if not txt:
        return False
    norm = _normalize_identity_text_strict(txt)
    if not norm:
        return False
    if re.match(r"^(?:cd|disc|disk|part|pt|act|scene|movement|mvmt|side|volume|vol|book)\b", norm):
        return True
    if re.match(r"^(?:disc|cd)?\s*\d{1,2}$", norm):
        return True
    if re.match(r"^[a-d]$", norm):
        return True
    return bool(
        re.search(
            r"\b(?:symphony|concerto|sonata|suite|requiem|mass|quartet|quintet|trio|prelude|fugue|overture|movement|act|scene)\b",
            norm,
        )
    )


def _folder_release_segment_child_dirs(folder_path: Path, audio_files: list[Path] | None) -> list[Path]:
    """
    Return immediate child directories that look like work/disc segments under `folder_path`.
    """
    folder = Path(folder_path)
    child_dirs: dict[str, Path] = {}
    direct_audio_present = False
    for raw_path in audio_files or []:
        try:
            p = Path(raw_path)
            rel = p.relative_to(folder)
        except Exception:
            continue
        parts = list(rel.parts)
        if len(parts) <= 1:
            direct_audio_present = True
            continue
        child = folder / parts[0]
        child_dirs[str(child)] = child
    if direct_audio_present or len(child_dirs) < 2:
        return []
    segment_children = [child for child in child_dirs.values() if _files_child_folder_name_looks_release_segment(child.name)]
    if len(segment_children) < 2:
        return []
    if len(segment_children) != len(child_dirs):
        return []
    return sorted(segment_children, key=lambda p: str(p).lower())


def _folder_has_release_segment_children(folder_path: Path, audio_files: list[Path] | None) -> bool:
    return len(_folder_release_segment_child_dirs(folder_path, audio_files)) >= 2


def _collapse_nested_album_folder_groups(
    by_folder: dict[Path, list[Path]],
    *,
    root_dirs: Optional[set[str]] = None,
    progress_cb: Optional[Callable[[dict[str, Any]], None]] = None,
    allow_tag_reads: bool = False,
) -> dict[Path, list[Path]]:
    """
    Collapse child work/disc folders into one parent album folder when they clearly belong
    to the same release.
    """
    if not by_folder:
        return by_folder

    def _normalize_child_entry(
        entry: Any,
    ) -> tuple[Path, list[Path], dict[str, Any]] | None:
        if not isinstance(entry, (tuple, list)):
            return None
        if len(entry) == 2:
            child_raw, paths_raw = entry
            tags_raw: Any = {}
        elif len(entry) >= 3:
            child_raw, paths_raw, tags_raw = entry[:3]
        else:
            return None
        try:
            child_path = Path(child_raw)
        except Exception:
            return None
        ordered_paths = [Path(p) for p in (paths_raw or [])]
        tags_dict = tags_raw if isinstance(tags_raw, dict) else {}
        return (child_path, ordered_paths, tags_dict)

    roots = root_dirs if isinstance(root_dirs, set) else _files_root_dir_strings()
    collapsed = dict(by_folder)
    parent_children: dict[Path, list[tuple[Path, list[Path]]]] = defaultdict(list)
    for folder, paths in by_folder.items():
        folder_key = Path(folder)
        parent = folder_key.parent
        if not parent or parent == folder_key:
            continue
        if _files_is_files_root_dir(parent, root_dirs=roots):
            continue
        try:
            resolved_parent = folder_key.resolve().parent
        except Exception:
            resolved_parent = None
        if resolved_parent and resolved_parent != parent and _files_is_files_root_dir(
            resolved_parent,
            root_dirs=roots,
        ):
            continue
        parent_children[parent].append((folder_key, paths or []))

    cover_names = {name.lower() for name in _COVER_NAMES}
    total_parents = len(parent_children)
    collapsed_groups = 0
    for idx, (parent, children) in enumerate(parent_children.items(), start=1):
        if progress_cb and (idx == 1 or idx == total_parents or idx % 200 == 0):
            try:
                progress_cb(
                    {
                        "phase": "collapsing",
                        "parent": str(parent),
                        "parents_processed": idx,
                        "parents_total": total_parents,
                        "collapsed_groups": collapsed_groups,
                    }
                )
            except Exception:
                pass
        if len(children) < 2:
            continue
        parent_has_cover = False
        try:
            parent_has_cover = any(
                p.is_file() and p.name.lower() in cover_names
                for p in parent.iterdir()
            )
        except Exception:
            parent_has_cover = False
        if parent in by_folder:
            continue

        items: list[tuple[Path, list[Path], dict[str, Any]]] | None = None

        # Library-index rebuilds may traverse hundreds of thousands of folders.
        # Prefer cheap path heuristics here and avoid opening audio files just to
        # decide whether CD1/CD2-style folders should be grouped.
        if parent_has_cover:
            segment_items: list[tuple[Path, list[Path], dict[str, Any]]] = []
            for raw_child in children:
                normalized_child = _normalize_child_entry(raw_child)
                if not normalized_child:
                    continue
                child, ordered, _existing_tags = normalized_child
                if not _files_child_folder_name_looks_release_segment(child.name):
                    continue
                if ordered:
                    segment_items.append((child, ordered, {}))
            if len(segment_items) >= 2 and len(segment_items) == len(children):
                items = segment_items

        if items is None and allow_tag_reads:
            prepared_children: list[tuple[Path, list[Path], dict[str, Any], str, str]] = []
            identity_groups: dict[tuple[str, str], list[tuple[Path, list[Path], dict[str, Any]]]] = defaultdict(list)
            for raw_child in children:
                normalized_child = _normalize_child_entry(raw_child)
                if not normalized_child:
                    continue
                child, ordered, existing_tags = normalized_child
                if not ordered:
                    continue
                try:
                    first_tags = existing_tags or extract_tags(ordered[0]) or {}
                except Exception:
                    first_tags = {}
                first_tags = first_tags if isinstance(first_tags, dict) else {}
                album_title = _sanitize_album_title_display(
                    _pick_album_title_from_tag_dicts([first_tags], fallback="")
                )
                artist_name = _pick_album_artist_from_tag_dicts([first_tags], default="")
                album_norm = (
                    norm_album_for_dedup(album_title, normalize_parenthetical=True)
                    if album_title
                    else ""
                )
                artist_norm = _norm_artist_key(artist_name) if artist_name else ""
                prepared_children.append((child, ordered, first_tags, album_norm, artist_norm))
                if album_norm:
                    identity_groups[(album_norm, artist_norm)].append((child, ordered, first_tags))

            qualifying = [(key, tag_items) for key, tag_items in identity_groups.items() if len(tag_items) >= 2]
            if len(qualifying) == 1:
                (_album_norm, _artist_norm), items = qualifying[0]

        if not items:
            continue

        segment_votes = sum(
            1 for child, _paths, _tags in items if _files_child_folder_name_looks_release_segment(child.name)
        )
        if segment_votes < max(1, len(items) // 2) and not parent_has_cover:
            continue

        combined_paths: list[Path] = []
        seen_paths: set[str] = set()
        for child, ordered, _tags in items:
            for path in ordered:
                key = str(path)
                if key in seen_paths:
                    continue
                seen_paths.add(key)
                combined_paths.append(path)
        if len(combined_paths) <= max(len(items), 1):
            continue

        for child, _ordered, _tags in items:
            collapsed.pop(child, None)
        collapsed[parent] = sorted(combined_paths, key=lambda p: str(p))
        collapsed_groups += 1
        if progress_cb:
            try:
                progress_cb(
                    {
                        "phase": "collapsing",
                        "parent": str(parent),
                        "parents_processed": idx,
                        "parents_total": total_parents,
                        "collapsed_groups": collapsed_groups,
                    }
                )
            except Exception:
                pass
        logging.info(
            "FILES discovery: collapsed %d child album folder(s) into parent release folder %s",
            len(items),
            parent,
        )
    return collapsed



def _resolve_album_folders_from_event_path(raw_path: str) -> list[str]:
    """
    Resolve changed filesystem path to one or more album folders.
    - audio file -> parent album folder
    - cover file -> album folder
    - artist image file -> all immediate child folders containing audio
    """
    if not raw_path:
        return []
    try:
        path = path_for_fs_access(Path(raw_path))
    except Exception:
        return []

    candidates: list[Path] = []
    lowered = path.name.lower()
    path_exists = path.exists()
    if path_exists and path.is_file():
        if AUDIO_RE.search(path.name):
            candidates.append(path.parent)
        elif lowered.startswith(("cover", "folder", "front", "album", "artwork")):
            candidates.append(path.parent)
        elif lowered.startswith("artist."):
            parent = path.parent
            if parent.is_dir():
                for child in parent.iterdir():
                    if child.is_dir() and _folder_has_audio_files(child):
                        candidates.append(child)
    elif path_exists and path.is_dir():
        if _folder_has_audio_files(path):
            candidates.append(path)
        else:
            try:
                for child in path.iterdir():
                    if child.is_dir() and _folder_has_audio_files(child):
                        candidates.append(child)
            except Exception:
                pass
    else:
        # Deleted/moved paths are often no longer present on disk when watchdog fires.
        # Infer best-effort album folder from the event path itself.
        if AUDIO_RE.search(path.name):
            candidates.append(path.parent)
        elif lowered.startswith(("cover", "folder", "front", "album", "artwork")):
            candidates.append(path.parent)
        elif lowered.startswith("artist."):
            candidates.append(path.parent)
        else:
            candidates.append(path)

    out: list[str] = []
    seen: set[str] = set()
    for c in candidates:
        try:
            key = _album_folder_cache_key(c)
        except Exception:
            key = str(c)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _group_audio_files_by_folder_under_roots(
    roots: list[str],
    *,
    progress_cb=None,
    progress_every: int = 250,
    heartbeat_seconds: float = 10.0,
) -> dict[Path, list[Path]]:
    """
    Walk roots once and group discovered audio files directly by folder.

    This is cheaper than collecting every audio file first and only grouping them
    afterwards, and it gives us truthful discovery telemetry while the rebuild is
    still scanning the filesystem.
    """
    roots_list = [str(r) for r in (roots or []) if r]
    roots_total = len(roots_list)
    if roots_total <= 0:
        return {}

    progress_every = max(1, int(progress_every)) if progress_every else 0
    heartbeat_seconds = float(heartbeat_seconds or 0.0)
    progress_enabled = callable(progress_cb)

    by_folder: dict[Path, list[Path]] = defaultdict(list)
    seen_paths: set[str] = set()
    entries_scanned = 0
    files_found = 0
    folders_with_audio = 0
    roots_done = 0
    last_heartbeat = time.monotonic()

    def _emit_progress(*, root: str, force: bool = False) -> None:
        nonlocal last_heartbeat
        if not progress_enabled:
            return
        if not force and heartbeat_seconds > 0:
            now = time.monotonic()
            if (now - last_heartbeat) < heartbeat_seconds:
                return
            last_heartbeat = now
        try:
            progress_cb(
                {
                    "root": root,
                    "roots_done": roots_done,
                    "roots_total": roots_total,
                    "entries_scanned": entries_scanned,
                    "files_found": files_found,
                    "folders_found": folders_with_audio,
                }
            )
        except Exception:
            pass

    for root_path in roots_list:
        base = Path(root_path)
        if not base.exists():
            roots_done += 1
            _emit_progress(root=str(base), force=True)
            continue
        stack: list[str] = [str(base)]
        while stack:
            current = stack.pop()
            try:
                with os.scandir(current) as it:
                    for entry in it:
                        entries_scanned += 1
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                stack.append(entry.path)
                            elif entry.is_file(follow_symlinks=False) and AUDIO_RE.search(entry.name):
                                p = Path(entry.path)
                                sp = str(p)
                                if sp in seen_paths:
                                    continue
                                seen_paths.add(sp)
                                parent = p.parent
                                if parent not in by_folder:
                                    folders_with_audio += 1
                                by_folder[parent].append(p)
                                files_found += 1
                                if progress_every > 0 and (files_found % progress_every == 0):
                                    _emit_progress(root=str(base), force=True)
                        except (OSError, PermissionError):
                            continue
                        _emit_progress(root=str(base), force=False)
            except (FileNotFoundError, NotADirectoryError, PermissionError, OSError):
                continue
        roots_done += 1
        _emit_progress(root=str(base), force=True)

    return by_folder


def _run_ffprobe(fpath: str) -> tuple[int, int, int]:
    """
    Run ffprobe on a single file and return (bit_rate, sample_rate, bit_depth).
    This is the actual work function that will be run in the pool.
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "a:0",
        "-show_entries",
        "format=bit_rate:stream=bit_rate,sample_rate,bits_per_raw_sample,bits_per_sample,sample_fmt",
        "-of", "default=noprint_wrappers=1",
        fpath,
    ]

    br = sr = bd = 0
    try:
        out = subprocess.check_output(
            cmd, stderr=subprocess.DEVNULL, text=True, timeout=10
        )
        for line in out.splitlines():
            key, _, val = line.partition("=")
            if key == "bit_rate":
                try:
                    v = int(val)
                    if v > br:
                        br = v  # keep highest bit‑rate seen
                except ValueError:
                    pass
            elif key == "sample_rate":
                try:
                    sr = int(val)
                except ValueError:
                    pass
            elif key in ("bits_per_raw_sample", "bits_per_sample"):
                try:
                    bd = int(val)
                except ValueError:
                    pass
            elif key == "sample_fmt" and not bd:
                m = re.match(r"s(\d+)", val)
                if m:
                    bd = int(m.group(1))
    except Exception:
        # leave br/sr/bd at 0 on failure
        pass

    return (br, sr, bd)


def _run_ffprobe_duration_sec(fpath: str) -> int:
    """Return media duration (seconds) via ffprobe. Best-effort; returns 0 on failure."""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        fpath,
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True, timeout=20).strip()
        if not out:
            raise RuntimeError("ffprobe returned empty duration")
        parsed = int(max(0.0, float(out)))
        if parsed > 0:
            return parsed
        raise RuntimeError("ffprobe returned non-positive duration")
    except Exception:
        # Fallback: mutagen parser is slower than ffprobe but still in-process and
        # significantly better than keeping 0:00 durations forever.
        try:
            from mutagen import File as MutagenFile

            mf = MutagenFile(fpath)
            length = float(getattr(getattr(mf, "info", None), "length", 0.0) or 0.0)
            if length > 0:
                return int(max(1.0, round(length)))
        except Exception:
            pass
        return 0


def analyse_format(folder: Path) -> tuple[int, int, int, int, bool]:
    """
    Inspect up to **three** audio files inside *folder* and return a 4‑tuple:

        (fmt_score, bit_rate, sample_rate, bit_depth)

    *   **fmt_score** derives from the global FORMAT_PREFERENCE list.
    *   **bit_rate** is in **bps** (`0` when not reported, e.g. lossless FLAC).
    *   **sample_rate** is in **Hz**.
    *   **bit_depth** is 16 / 24 / 32 when derivable, otherwise 0.

    Rationale for retry logic
    -------------------------
    A single, transient ffprobe failure (network share hiccup, race during mount,
    etc.) previously led to a *false « invalid »* verdict because all tech values
    were 0.
    We now:

    1. Collect *all* audio files under the folder (breadth‑first, glob pattern
       from `AUDIO_RE`).
    2. Probe **up to three distinct files** or **two attempts per file** (cache +
       fresh call) until we obtain at least one non‑zero technical metric.
    3. Only if **every attempt** yields `(0, 0, 0)` do we fall back to the
       "invalid" classification.

    Each `(path, mtime)` result – even the all‑zero case – is cached so we
    never hammer ffprobe, but a later scan still re‑probes if the file changes.

    Non-cached ffprobe calls are now processed in parallel using a thread pool.
    """
    audio_files = [p for p in folder.rglob("*") if AUDIO_RE.search(p.name)]
    if not audio_files:
        return (0, 0, 0, 0, False)

    # First pass: check cache for all files (unless global scan setting disables cache usage)
    use_cache = not getattr(sys.modules[__name__], "SCAN_DISABLE_CACHE", False)
    files_to_probe = []
    for audio_file in audio_files[:3]:
        ext   = audio_file.suffix[1:].lower()
        fpath = str(audio_file)
        mtime = int(audio_file.stat().st_mtime)

        # Check cache first (when enabled)
        if use_cache:
            cached = get_cached_info(fpath, mtime)
            if cached and not (cached == (0, 0, 0) and ext == "flac"):
                br, sr, bd = cached
                if br or sr or bd:
                    # Track cache hit (will be aggregated in scan_duplicates)
                    return (score_format(ext), br, sr, bd, True)  # True = cache hit

        # File not in cache or cache miss, add to probe list
        files_to_probe.append((audio_file, ext, fpath, mtime))

    # Second pass: probe files in parallel if pool is enabled
    if files_to_probe and FFPROBE_POOL_SIZE > 1:
        futures = {}
        pool = get_ffprobe_pool()

        for audio_file, ext, fpath, mtime in files_to_probe:
            future = pool.submit(_run_ffprobe, fpath)
            futures[future] = (audio_file, ext, fpath, mtime)

        # Wait for results (with timeout per file)
        for future in as_completed(futures):
            audio_file, ext, fpath, mtime = futures[future]
            try:
                br, sr, bd = future.result(timeout=15)  # Slightly longer timeout for pool
            except Exception:
                br, sr, bd = 0, 0, 0

            # Cache the result
            set_cached_info(fpath, mtime, br, sr, bd)

            if br or sr or bd:  # success on this file → done
                return (score_format(ext), br, sr, bd, False)  # False = cache miss
    else:
        # Sequential processing (fallback or pool disabled)
        for audio_file, ext, fpath, mtime in files_to_probe:
            br, sr, bd = _run_ffprobe(fpath)

            # Cache the result
            set_cached_info(fpath, mtime, br, sr, bd)

            if br or sr or bd:  # success on this file → done
                return (score_format(ext), br, sr, bd, False)  # False = cache miss

    # After probing up to 3 files and still nothing usable → treat as invalid
    if audio_files:
        first_ext = audio_files[0].suffix[1:].lower()
        return (score_format(first_ext), 0, 0, 0, False)
    return (0, 0, 0, 0, False)


def _fpcalc_fingerprint_file(path_str: str, *, length_sec: int = 120, timeout_sec: int = 45) -> Optional[tuple[float, str]]:
    """Compute chromaprint fingerprint via `fpcalc` subprocess.

    Important: We never call chromaprint in-process for scanning because certain
    audio files can trigger a hard abort inside chromaprint (assertion failure),
    killing the entire PMDA server. By isolating fingerprinting in a subprocess,
    we can safely skip problematic files and continue the scan.
    """
    fpcalc = shutil.which("fpcalc")
    if not fpcalc:
        return None
    try:
        length_sec_i = max(1, int(length_sec or 120))
    except Exception:
        length_sec_i = 120

    args = [fpcalc, "-json", "-length", str(length_sec_i), path_str]
    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        logging.debug("[AcousticID] fpcalc timeout for %s", Path(path_str).name)
        return None
    except Exception as e:
        logging.debug("[AcousticID] fpcalc failed for %s: %s", Path(path_str).name, e)
        return None

    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip().replace("\n", " ")[:200]
        logging.debug("[AcousticID] fpcalc rc=%s for %s: %s", proc.returncode, Path(path_str).name, err)
        return None

    out = (proc.stdout or "").strip()
    if not out:
        out = (proc.stderr or "").strip()
    if not out:
        return None

    # Prefer JSON output (fpcalc -json).
    try:
        if out.lstrip().startswith("{"):
            data = json.loads(out)
            fingerprint = data.get("fingerprint")
            duration = data.get("duration")
            if fingerprint:
                try:
                    return float(duration or 0.0), str(fingerprint)
                except Exception:
                    return 0.0, str(fingerprint)
    except Exception as e:
        logging.debug("[AcousticID] fpcalc JSON parse failed for %s: %s", Path(path_str).name, e)

    # Fallback: KEY=VALUE output (older fpcalc).
    duration = None
    fingerprint = None
    for line in out.splitlines():
        up = (line or "").strip().upper()
        if up.startswith("DURATION="):
            try:
                duration = float(line.split("=", 1)[1].strip())
            except Exception:
                duration = None
        elif up.startswith("FINGERPRINT="):
            fingerprint = line.split("=", 1)[1].strip()
    if fingerprint:
        try:
            return float(duration or 0.0), str(fingerprint)
        except Exception:
            return 0.0, str(fingerprint)
    return None

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}

def _resolve_album_folders_from_event_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _resolve_album_folders_from_event_path(*args, **kwargs)

def _group_audio_files_by_folder_under_roots_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _group_audio_files_by_folder_under_roots(*args, **kwargs)

def _run_ffprobe_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_ffprobe(*args, **kwargs)

def _run_ffprobe_duration_sec_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_ffprobe_duration_sec(*args, **kwargs)

def analyse_format_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return analyse_format(*args, **kwargs)

def _fpcalc_fingerprint_file_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fpcalc_fingerprint_file(*args, **kwargs)

def extract_tags_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return extract_tags(*args, **kwargs)

def _iter_audio_files_under_roots_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _iter_audio_files_under_roots(*args, **kwargs)

def _files_child_folder_name_looks_release_segment_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_child_folder_name_looks_release_segment(*args, **kwargs)

def _folder_release_segment_child_dirs_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _folder_release_segment_child_dirs(*args, **kwargs)

def _folder_has_release_segment_children_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _folder_has_release_segment_children(*args, **kwargs)

def _collapse_nested_album_folder_groups_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _collapse_nested_album_folder_groups(*args, **kwargs)
