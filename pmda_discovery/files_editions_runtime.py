"""Runtime-backed Files edition discovery.

The implementation is intentionally kept behavior-compatible with the historical
``pmda.py`` discovery function. The public boundary accepts the live PMDA runtime
module while discovery state, storage policy, and persistence are progressively
split into explicit services.
"""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one Files discovery call."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {"_build_files_editions"}
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def build_files_editions_for_runtime(
    runtime: Any,
    scan_type: str = "full",
    *,
    respect_scan_controls: bool = True,
):
    """Run Files edition discovery using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _build_files_editions_impl(
        scan_type=scan_type,
        respect_scan_controls=respect_scan_controls,
    )


def _build_files_editions_impl(
    scan_type: str = "full",
    *,
    respect_scan_controls: bool = True,
) -> tuple[list[tuple[int, str, list[int]]], int, dict]:
    """
    Scan FILES_ROOTS, group audio files by parent folder (album candidate), infer
    artist/album/tracklist from tags, and return (artists_merged, total_albums, files_editions_by_album_id).
    Caller must store files_editions_by_album_id in state for workers (e.g. state["files_editions_by_album_id"]).
    """
    from collections import defaultdict

    # For Files mode, we want track index gaps to be detectable (incomplete albums) without relying on
    # filenames. `extract_tags()` uses ffprobe (subprocess) and is too expensive to run per file,
    # so we opportunistically use mutagen (fast in-process) for per-file track/disc/title only.
    try:
        from mutagen import File as MutagenFile  # type: ignore
    except Exception:
        MutagenFile = None  # type: ignore

    scan_type = (scan_type or "full").strip().lower()
    if scan_type not in {"full", "changed_only", "incomplete_only"}:
        scan_type = "full"

    active_root_paths = _effective_files_scan_roots(enabled_only=True)
    canonical_root_paths = [str(r) for r in (active_root_paths or FILES_ROOTS or []) if r]
    roots = [Path(r) for r in canonical_root_paths]
    storage_entries: list[dict[str, Any]] = []
    storage_enabled = False
    if _storage_power_saver_active():
        try:
            roots, storage_entries = _storage_unraid_build_scan_roots(canonical_root_paths)
            storage_enabled = bool(storage_entries)
        except RuntimeError as exc:
            with lock:
                state["storage_power_saver_enabled"] = True
                state["storage_provider"] = STORAGE_PROVIDER
                state["storage_active_devices"] = 0
                state["storage_devices_total"] = 0
                state["storage_current_device_id"] = None
                state["storage_current_device_label"] = None
                state["storage_bucket_done"] = 0
                state["storage_bucket_total"] = 0
                state["storage_buckets_done"] = 0
                state["storage_buckets_total"] = 0
                state["storage_estimated_watts_saved"] = 0.0
                state["storage_scan_plan"] = []
                state["storage_current_bucket"] = None
                state["storage_validation_error"] = str(exc)
            log_scan("[STORAGE] Power saver validation failed: %s", exc)
            raise
    else:
        with lock:
            state["storage_power_saver_enabled"] = False
            state["storage_provider"] = STORAGE_PROVIDER
            state["storage_active_devices"] = 0
            state["storage_devices_total"] = 0
            state["storage_current_device_id"] = None
            state["storage_current_device_label"] = None
            state["storage_bucket_done"] = 0
            state["storage_bucket_total"] = 0
            state["storage_buckets_done"] = 0
            state["storage_buckets_total"] = 0
            state["storage_estimated_watts_saved"] = 0.0
            state["storage_scan_plan"] = []
            state["storage_current_bucket"] = None
            state["storage_validation_error"] = ""
    if storage_enabled:
        plan_summary = _storage_plan_summary(storage_entries)
        devices_total = len({str(e.get("storage_device_id") or "") for e in storage_entries if str(e.get("storage_device_id") or "")})
        with lock:
            state["storage_power_saver_enabled"] = True
            state["storage_provider"] = "unraid"
            state["storage_active_devices"] = 1
            state["storage_devices_total"] = devices_total
            state["storage_buckets_done"] = 0
            state["storage_buckets_total"] = len(plan_summary)
            state["storage_estimated_watts_saved"] = _storage_estimated_watts_saved(1, devices_total)
            state["storage_scan_plan"] = plan_summary
            state["storage_bucket_history"] = []
            state["storage_validation_error"] = ""
            state["storage_started_at"] = time.time()
        log_scan(
            "[STORAGE] Power saver active: %d disk bucket(s), max_active=%d, canonical roots=%s.",
            len(plan_summary),
            STORAGE_MAX_ACTIVE_DEVICES,
            canonical_root_paths,
        )
    stop_event = scan_should_stop if respect_scan_controls else None
    pause_event = scan_is_paused if respect_scan_controls else None
    with lock:
        requested_resume_run_id = str(state.get("scan_resume_requested_run_id") or "").strip() or None
        current_resume_run_id = str(state.get("scan_resume_run_id") or "").strip() or None
    resume_run_id = current_resume_run_id or requested_resume_run_id
    if respect_scan_controls and not resume_run_id:
        resume_run_id = _ensure_resume_run_started("files", scan_type, requested_run_id=requested_resume_run_id)
        if resume_run_id:
            with lock:
                state["scan_resume_run_id"] = resume_run_id
    resume_discovery_snapshot = (
        _load_resume_discovery_snapshot_by_run_id(resume_run_id)
        if respect_scan_controls and resume_run_id
        else None
    )

    def _files_scan_stop_requested() -> bool:
        return bool(stop_event is not None and stop_event.is_set())

    pause_flush_cb: Callable[[], None] | None = None

    def _files_scan_wait_if_paused() -> bool:
        if pause_event is None:
            return True
        pause_flush_done = False
        while pause_event.is_set() and not _files_scan_stop_requested():
            if not pause_flush_done and pause_flush_cb is not None:
                try:
                    pause_flush_cb()
                except Exception:
                    logging.debug("FILES discovery pause flush failed", exc_info=True)
                pause_flush_done = True
            time.sleep(0.2)
        return not _files_scan_stop_requested()

    if not roots:
        _clear_scan_discovery_runtime(resume_run_id)
        with lock:
            state["scan_discovery_running"] = False
            state["scan_discovery_current_root"] = None
            state["scan_discovery_roots_done"] = 0
            state["scan_discovery_roots_total"] = 0
            state["scan_discovery_files_found"] = 0
            state["scan_discovery_folders_found"] = 0
            state["scan_discovery_albums_found"] = 0
            state["scan_discovery_artists_found"] = 0
            state["scan_discovery_stage"] = "idle"
            state["scan_discovery_entries_scanned"] = 0
            state["scan_discovery_root_entries_scanned"] = 0
            state["scan_discovery_folders_done"] = 0
            state["scan_discovery_folders_total"] = 0
            state["scan_discovery_albums_done"] = 0
            state["scan_discovery_albums_total"] = 0
            state["scan_discovery_started_at"] = None
            state["scan_discovery_updated_at"] = time.time()
            state["scan_tracks_detected_total"] = 0
        return [], 0, {}

    skip_list = list(SKIP_FOLDERS or [])
    cache_map = _load_files_album_scan_cache_map()
    dir_cache_map: dict[str, dict] = {}
    cached_album_folders_from_discovery: set[str] = set(
        str(p or "").strip()
        for p in ((resume_discovery_snapshot or {}).get("cached_album_folders") or [])
        if str(p or "").strip()
    )
    if not bool(_pipeline_bootstrap_status().get("bootstrap_required")):
        dir_cache_map = _load_files_dir_scan_cache_map()
    ordered_paths_cache: dict[str, list[str]] = {}
    dir_cache_skip_hits = 0
    dir_cache_skip_audio = 0

    def _dir_skip_lookup(dir_path_raw: str, root_path_raw: str) -> dict[str, Any] | None:
        if not dir_cache_map:
            return None
        dir_key = _album_folder_cache_key(dir_path_raw)
        row = dir_cache_map.get(dir_key)
        if not row:
            return None
        depth = _relative_depth_under_root(dir_path_raw, root_path_raw)
        if depth is None or depth < PMDA_FILES_DIR_CACHE_MIN_SKIP_DEPTH:
            return None
        fingerprint_now, _entry_count = _compute_dir_scan_fingerprint(Path(dir_path_raw))
        if not fingerprint_now or fingerprint_now != str(row.get("fingerprint") or "").strip():
            return None
        album_folders = [str(p) for p in (row.get("album_folders") or []) if str(p or "").strip()]
        if len(album_folders) < PMDA_FILES_DIR_CACHE_MIN_ALBUMS:
            return None
        return row

    def _dir_skip_resolver(dir_path_raw: str, row: dict[str, Any]) -> dict[str, Any] | None:
        nonlocal dir_cache_skip_hits, dir_cache_skip_audio
        folder_keys = [str(p) for p in (row.get("album_folders") or []) if str(p or "").strip()]
        if not folder_keys:
            return None
        missing = [key for key in folder_keys if key not in ordered_paths_cache]
        if missing:
            payloads = _load_files_album_scan_cache_map(folder_keys=missing, include_ordered_paths=True)
            for key, payload in payloads.items():
                ordered_paths_cache[key] = [str(p) for p in (payload.get("ordered_paths") or []) if str(p or "").strip()]
        file_paths: list[str] = []
        for key in folder_keys:
            ordered = ordered_paths_cache.get(key) or []
            if not ordered:
                return None
            file_paths.extend(ordered)
        dir_cache_skip_hits += 1
        dir_cache_skip_audio += len(file_paths)
        return {
            "file_paths": file_paths,
            "audio_count": int(row.get("subtree_audio_count") or len(file_paths)),
            "entry_estimate": int(row.get("subtree_entry_estimate") or len(file_paths)),
            "album_folders": folder_keys,
        }
    changed_pending_folder_keys: list[str] = []
    changed_pending_deleted_folder_keys: list[str] = []
    if scan_type == "changed_only":
        seen_pending: set[str] = set()
        for row in _list_files_pending_changes(limit=50000):
            key = str(row.get("folder_path") or "").strip()
            if not key or key in seen_pending:
                continue
            seen_pending.add(key)
            changed_pending_folder_keys.append(key)
        if changed_pending_folder_keys:
            log_scan(
                "FILES changed-only: %d dirty album folder(s) queued by watcher.",
                len(changed_pending_folder_keys),
            )
        else:
            log_scan(
                "FILES changed-only: no watcher queue entries found; falling back to filesystem discovery + fast skip.",
            )
    with lock:
        state["scan_dirty_folders_pending_clear"] = list(changed_pending_folder_keys)
    heartbeat_interval_s = 10.0
    heartbeat_frames = ("|", "/", "-", "\\")
    heartbeat_idx = 0
    last_heartbeat_ts = 0.0
    discovery_started_at = time.time()

    def _set_discovery_state(**updates: object) -> None:
        updates["scan_discovery_updated_at"] = time.time()
        with lock:
            for key, value in updates.items():
                state[key] = value
            discovery_files_found_now = int(state.get("scan_discovery_files_found") or 0)
            discovery_albums_found_now = int(state.get("scan_discovery_albums_found") or 0)
            discovery_artists_found_now = int(state.get("scan_discovery_artists_found") or 0)
            discovery_total_now = int(state.get("scan_discovery_albums_total") or state.get("scan_discovery_folders_total") or 0)
            state["scan_tracks_detected_total"] = max(int(state.get("scan_tracks_detected_total") or 0), discovery_files_found_now)
            state["scan_detected_albums_total"] = max(int(state.get("scan_detected_albums_total") or 0), discovery_albums_found_now)
            state["scan_detected_artists_total"] = max(int(state.get("scan_detected_artists_total") or 0), discovery_artists_found_now)
            if discovery_total_now > 0:
                state["scan_total_albums"] = max(int(state.get("scan_total_albums") or 0), discovery_total_now)

    def _cancel_discovery(reason: str = "cancelled") -> tuple[list[tuple[int, str, list[int]]], int, dict]:
        _clear_scan_discovery_runtime(resume_run_id)
        _set_discovery_state(
            scan_discovery_running=False,
            scan_discovery_stage="cancelled",
            scan_discovery_current_root=None,
        )
        _emit_files_discovery_heartbeat(reason, force=True)
        return [], 0, {}

    def _emit_files_discovery_heartbeat(
        stage: str,
        *,
        root: str | None = None,
        roots_done: int | None = None,
        roots_total: int | None = None,
        files_found: int | None = None,
        entries_scanned: int | None = None,
        folders_done: int | None = None,
        folders_total: int | None = None,
        artists_found: int | None = None,
        albums_found: int | None = None,
        force: bool = False,
    ) -> None:
        nonlocal heartbeat_idx, last_heartbeat_ts
        now = time.monotonic()
        if not force and (now - last_heartbeat_ts) < heartbeat_interval_s:
            return
        last_heartbeat_ts = now
        frame = heartbeat_frames[heartbeat_idx % len(heartbeat_frames)]
        heartbeat_idx += 1
        parts: list[str] = []
        if roots_done is not None and roots_total is not None:
            parts.append(f"roots {int(roots_done)}/{int(roots_total)}")
        if root:
            parts.append(f"root={root}")
        if entries_scanned is not None:
            parts.append(f"visited={int(entries_scanned)}")
        if files_found is not None:
            parts.append(f"audio={int(files_found)}")
        if folders_done is not None and folders_total is not None:
            parts.append(f"folders {int(folders_done)}/{int(folders_total)}")
        if artists_found is not None:
            parts.append(f"artists={int(artists_found)}")
        if albums_found is not None:
            parts.append(f"albums={int(albums_found)}")
        suffix = " | ".join(parts)
        log_scan("FILES discovery %s %s%s", frame, stage, (f" | {suffix}" if suffix else ""))

    def _infer_disc_track_from_filename(path: Path, fallback_track: int) -> tuple[int, int]:
        """
        Fast filename-only track parser for cache-first scan path.
        Examples accepted: 01 ..., 1-05 ..., CD2-07 ..., A1 ...
        """
        stem = path.stem.strip()
        disc = 1
        track = fallback_track
        m = re.match(r"^\s*(?:cd|disc)\s*(\d{1,2})\s*[-_. ]\s*(\d{1,3})\b", stem, flags=re.IGNORECASE)
        if m:
            return (_parse_int_loose(m.group(1), 1) or 1, _parse_int_loose(m.group(2), fallback_track) or fallback_track)
        m = re.match(r"^\s*(\d{1,2})\s*[-_.]\s*(\d{1,3})\b", stem)
        if m:
            return (_parse_int_loose(m.group(1), 1) or 1, _parse_int_loose(m.group(2), fallback_track) or fallback_track)
        # Vinyl-side style: "A1", "B2", optionally with separators. Require digits so we don't mis-detect
        # normal names like "Ochre - ..." as side "O".
        m = re.match(r"^\s*([A-Z])\s*(?:[-_. ]?\s*(\d{1,3}))\b", stem, flags=re.IGNORECASE)
        if m:
            disc = (ord(m.group(1).upper()) - ord("A")) + 1
            track = _parse_int_loose(m.group(2), 1) or 1
            return (max(1, disc), max(1, track))
        m = re.match(r"^\s*(\d{1,3})\b", stem)
        if m:
            track = _parse_int_loose(m.group(1), fallback_track) or fallback_track
            return (disc, max(1, track))
        return (disc, max(1, track))

    def _title_from_filename(path: Path, fallback_index: int) -> str:
        stem = path.stem.strip()
        def _trim_title_separators(value: str) -> str:
            return str(value or "").strip(" -_")
        cleaned = re.sub(
            r"^\s*[^-]+?\s*-\s*[^-]+?\s*-\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\s*[-_. ]*",
            "",
            stem,
            flags=re.IGNORECASE,
        )
        if cleaned != stem:
            cleaned = _trim_title_separators(cleaned)
            return cleaned or stem or f"Track {fallback_index}"
        cleaned = re.sub(
            r"^\s*[^-]+?\s*-\s*[^-]+?\s*-\s*\d{1,3}\s*-\s*",
            "",
            stem,
            flags=re.IGNORECASE,
        )
        if cleaned != stem:
            cleaned = _trim_title_separators(cleaned)
            return cleaned or stem or f"Track {fallback_index}"
        cleaned = re.sub(r"^\s*(?:cd|disc)\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\s*[-_. ]*", "", stem, flags=re.IGNORECASE)
        cleaned = re.sub(r"^\s*\d{1,2}\s*[-_.]\s*\d{1,3}\s*[-_. ]*", "", cleaned)
        # Vinyl-side style: "A1 - Track", "B02_Track". Require digits so we don't strip leading letters
        # from normal names (e.g. "Ochre - ...").
        cleaned = re.sub(r"^\s*[A-Z]\s*(?:[-_. ]?\s*\d{1,3})\s*[-_. ]*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"^\s*\d{1,3}\s*[-_. ]*", "", cleaned)
        cleaned = _trim_title_separators(cleaned)
        return cleaned or stem or f"Track {fallback_index}"

    def _filename_tail_for_track_parsing(path: Path | str) -> str:
        try:
            stem = Path(path).stem.strip()
        except Exception:
            stem = str(path or "").strip()
        if not stem:
            return ""
        m = re.match(r"^\s*[^-]+?\s*-\s*[^-]+?\s*-\s*(.+)$", stem, flags=re.IGNORECASE)
        if m:
            tail = str(m.group(1) or "").strip()
            if tail:
                return tail
        return stem

    def _infer_disc_track_from_track_context(
        file_path: str,
        fallback_disc: int,
        fallback_track: int,
    ) -> tuple[int, int]:
        disc = max(1, int(fallback_disc or 1))
        track = max(1, int(fallback_track or 1))
        raw = str(file_path or "").strip()
        if not raw:
            return (disc, track)
        tail = _filename_tail_for_track_parsing(raw)
        if not tail:
            return (disc, track)
        try:
            parsed_disc, parsed_track = _infer_disc_track_from_filename(Path(tail), track)
        except Exception:
            parsed_disc, parsed_track = (disc, track)
        return (
            max(1, int(parsed_disc or disc)),
            max(1, int(parsed_track or track)),
        )

    def _disc_label_from_track_context(file_path: str, disc_num: int) -> str | None:
        raw = str(file_path or "").strip()
        if not raw:
            return None
        tail = _filename_tail_for_track_parsing(raw)
        if not tail:
            return None
        m = re.match(r"^\s*([A-Z])\s*(?:[-_. ]?\s*\d{1,3})\b", tail, flags=re.IGNORECASE)
        if not m:
            return None
        side = str(m.group(1) or "").upper()
        if not side:
            return None
        return f"Side {side}"

    def _clean_track_display_title(
        raw_title: str,
        *,
        file_path: str = "",
        fallback_index: int = 1,
    ) -> str:
        file_raw = str(file_path or "").strip()
        if file_raw:
            try:
                tail = _filename_tail_for_track_parsing(file_raw)
                file_title = _title_from_filename(Path(tail), fallback_index)
                file_title = re.sub(r"^\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\s*[-_. ]*", "", file_title)
                file_title = file_title.strip(" -_")
                if file_title:
                    return file_title
            except Exception:
                pass

        title = str(raw_title or "").strip()
        if not title:
            return f"Track {fallback_index}"
        cleaned = re.sub(
            r"^\s*[^-]+?\s*-\s*[^-]+?\s*-\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\s*[-_. ]*",
            "",
            title,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            r"^\s*[^-]+?\s*-\s*[^-]+?\s*-\s*\d{1,3}\s*[-_. ]*",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(r"^\s*(?:cd|disc)\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\s*[-_. ]*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"^\s*\d{1,2}\s*[-_.]\s*\d{1,3}\s*[-_. ]*", "", cleaned)
        cleaned = re.sub(r"^\s*\d{1,3}\s*[-_. ]*", "", cleaned)
        cleaned = cleaned.strip(" -_")
        return cleaned or title or f"Track {fallback_index}"

    def _track_title_looks_filename_noise(raw_title: str, album_title: str = "") -> bool:
        title = str(raw_title or "").strip()
        if not title:
            return False
        if re.match(r"^\s*[^-]+?\s*-\s*[^-]+?\s*-\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\b", title, flags=re.IGNORECASE):
            return True
        if re.match(r"^\s*[^-]+?\s*-\s*[^-]+?\s*-\s*\d{1,3}\b", title, flags=re.IGNORECASE):
            return True
        if re.match(r"^\s*(?:cd|disc)\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\b", title, flags=re.IGNORECASE):
            return True
        album_norm = _normalize_identity_text_strict(album_title or "")
        title_norm = _normalize_identity_text_strict(title)
        if album_norm and title_norm.startswith(f"{album_norm} "):
            return True
        return False

    def _filename_has_explicit_track_number(path: Path) -> bool:
        """Return True when filename contains an explicit track/disc+track prefix."""
        stem = path.stem.strip()
        if not stem:
            return False
        if re.match(r"^\s*(?:cd|disc)\s*\d{1,2}\s*[-_. ]\s*\d{1,3}\b", stem, flags=re.IGNORECASE):
            return True
        if re.match(r"^\s*\d{1,2}\s*[-_.]\s*\d{1,3}\b", stem):
            return True
        if re.match(r"^\s*[A-Z]\s*(?:[-_. ]?\s*\d{1,3})\b", stem, flags=re.IGNORECASE):
            return True
        if re.match(r"^\s*\d{1,3}\b", stem):
            return True
        return False

    def _parse_int_tag(value: object, default: int) -> int:
        try:
            if value is None:
                return default
            if isinstance(value, (list, tuple)):
                if not value:
                    return default
                value = value[0]
            s = str(value).strip()
            if not s:
                return default
            # Common forms: "3", "3/12"
            if "/" in s:
                s = s.split("/", 1)[0].strip()
            m = re.search(r"\d+", s)
            if not m:
                return default
            n = int(m.group(0))
            return n if n > 0 else default
        except Exception:
            return default

    def _infer_track_from_mutagen(path: Path, fallback_index: int) -> tuple[int, int, str]:
        """Return (disc, track, title) using mutagen easy tags when available."""
        disc = 1
        trk = max(1, int(fallback_index or 1))
        title = ""
        if MutagenFile is not None:
            try:
                f = MutagenFile(str(path), easy=True)
                tags = getattr(f, "tags", None) or {}
                title = (tags.get("title") or [""])[0] if isinstance(tags.get("title"), list) else (tags.get("title") or "")
                trk = _parse_int_tag(tags.get("tracknumber") or tags.get("track") or tags.get("track_number"), trk)
                disc = _parse_int_tag(tags.get("discnumber") or tags.get("disc") or tags.get("disc_number"), disc)
            except Exception:
                pass
        if not (title or "").strip():
            title = _title_from_filename(path, fallback_index)
        return (max(1, disc or 1), max(1, trk or 1), str(title or "").strip())

    with lock:
        state["scan_discovery_running"] = True
        state["scan_discovery_current_root"] = None
        state["scan_discovery_roots_done"] = 0
        state["scan_discovery_roots_total"] = len(roots)
        state["scan_discovery_files_found"] = 0
        state["scan_discovery_folders_found"] = 0
        state["scan_discovery_albums_found"] = 0
        state["scan_discovery_artists_found"] = 0
        state["scan_discovery_stage"] = "filesystem"
        state["scan_discovery_entries_scanned"] = 0
        state["scan_discovery_root_entries_scanned"] = 0
        state["scan_discovery_folders_done"] = 0
        state["scan_discovery_folders_total"] = 0
        state["scan_discovery_albums_done"] = 0
        state["scan_discovery_albums_total"] = 0
        state["scan_discovery_started_at"] = discovery_started_at
        state["scan_discovery_updated_at"] = discovery_started_at

    if isinstance(resume_discovery_snapshot, dict):
        resume_stage = str(resume_discovery_snapshot.get("stage") or "").strip() or "filesystem"
        resume_storage_updates: dict[str, Any] = {}
        if storage_enabled:
            resume_storage_updates = _storage_discovery_device_fields_for_path(
                resume_discovery_snapshot.get("current_root_path"),
                storage_entries,
            )
        _set_discovery_state(
            scan_discovery_running=True,
            scan_discovery_stage=resume_stage,
            scan_discovery_current_root=resume_discovery_snapshot.get("current_root_path"),
            scan_discovery_roots_done=int(resume_discovery_snapshot.get("shared_roots_done") or 0),
            scan_discovery_roots_total=int(resume_discovery_snapshot.get("roots_total") or len(roots)),
            scan_discovery_files_found=int(resume_discovery_snapshot.get("files_found") or 0),
            scan_discovery_entries_scanned=int(resume_discovery_snapshot.get("entries_scanned") or 0),
            scan_discovery_root_entries_scanned=int(resume_discovery_snapshot.get("current_root_entries_scanned") or 0),
            scan_discovery_folders_found=int(resume_discovery_snapshot.get("folders_found") or 0),
            scan_discovery_albums_found=int(resume_discovery_snapshot.get("albums_found") or 0),
            scan_discovery_artists_found=int(resume_discovery_snapshot.get("artists_found") or 0),
            scan_discovery_folders_done=int(resume_discovery_snapshot.get("folders_done") or 0),
            scan_discovery_folders_total=int(resume_discovery_snapshot.get("folders_total") or 0),
            **resume_storage_updates,
        )
        log_scan(
            "FILES discovery: resuming run_id=%s at stage=%s (visited=%d, audio=%d).",
            resume_run_id,
            resume_stage,
            int(resume_discovery_snapshot.get("entries_scanned") or 0),
            int(resume_discovery_snapshot.get("files_found") or 0),
        )

    def _on_discovery_progress(payload: dict) -> None:
        storage_updates: dict[str, Any] = {}
        if storage_enabled:
            storage_updates = _storage_discovery_device_fields_for_path(payload.get("root"), storage_entries)
        try:
            _set_discovery_state(
                scan_discovery_running=not bool(payload.get("done")),
                scan_discovery_stage="filesystem",
                scan_discovery_current_root=payload.get("root"),
                scan_discovery_roots_done=int(payload.get("roots_done") or 0),
                scan_discovery_roots_total=int(payload.get("roots_total") or len(roots)),
                scan_discovery_files_found=int(payload.get("files_found") or 0),
                scan_discovery_entries_scanned=int(payload.get("entries_scanned") or 0),
                scan_discovery_root_entries_scanned=int(payload.get("root_entries_scanned") or 0),
                **storage_updates,
            )
        except Exception:
            pass
        _emit_files_discovery_heartbeat(
            "scanning filesystem",
            root=str(payload.get("root") or ""),
            roots_done=int(payload.get("roots_done") or 0),
            roots_total=int(payload.get("roots_total") or len(roots)),
            files_found=int(payload.get("files_found") or 0),
            entries_scanned=int(payload.get("entries_scanned") or 0),
        )

    def _on_discovery_checkpoint(snapshot: dict[str, Any]) -> None:
        if not isinstance(snapshot, dict):
            return
        with lock:
            snapshot["albums_found"] = int(state.get("scan_discovery_albums_found") or 0)
            snapshot["artists_found"] = int(state.get("scan_discovery_artists_found") or 0)
            snapshot["folders_found"] = int(state.get("scan_discovery_folders_found") or 0)
            snapshot["folders_done"] = int(state.get("scan_discovery_folders_done") or 0)
            snapshot["folders_total"] = int(state.get("scan_discovery_folders_total") or 0)
        _set_scan_discovery_runtime(snapshot)

    _emit_files_discovery_heartbeat("scanning filesystem", roots_done=0, roots_total=len(roots), force=True)
    by_folder: dict[Path, list[Path]] = defaultdict(list)
    canonical_paths_by_folder: dict[Path, list[Path]] = defaultdict(list)
    storage_meta_by_folder: dict[str, dict[str, Any]] = {}
    audio_files: list[Path] = []

    def _storage_add_audio_path(access_path: Path) -> None:
        canonical_path = _storage_canonical_path_for_access_path(access_path, storage_entries) if storage_enabled else access_path
        canonical_folder = canonical_path.parent
        by_folder[canonical_folder].append(access_path)
        canonical_paths_by_folder[canonical_folder].append(canonical_path)
        if storage_enabled:
            meta = _storage_find_entry_for_access_path(access_path, storage_entries)
            if meta:
                folder_meta = dict(meta)
                folder_meta["storage_access_path"] = str(access_path.parent)
                try:
                    folder_meta["storage_rel_path"] = str(Path(str(meta.get("storage_rel_path") or "")).parent)
                except Exception:
                    pass
                storage_meta_by_folder[str(canonical_folder)] = folder_meta

    if scan_type == "changed_only" and changed_pending_folder_keys:
        folders_total_pending = len(changed_pending_folder_keys)
        for idx, folder_key in enumerate(changed_pending_folder_keys, start=1):
            if _files_scan_stop_requested():
                log_scan("FILES discovery cancelled during watcher queue scan.")
                return _cancel_discovery()
            if not _files_scan_wait_if_paused():
                log_scan("FILES discovery cancelled while paused during watcher queue scan.")
                return _cancel_discovery()
            candidate_folders: list[tuple[Path, Path]] = []
            canonical_folder = Path(folder_key)
            if storage_enabled:
                for entry in storage_entries:
                    access_folder = _storage_access_path_for_canonical_path(canonical_folder, str(entry.get("storage_device_id") or ""), storage_entries)
                    if access_folder and access_folder.exists() and access_folder.is_dir():
                        candidate_folders.append((canonical_folder, access_folder))
            else:
                folder_path = path_for_fs_access(canonical_folder)
                if folder_path.exists() and folder_path.is_dir():
                    candidate_folders.append((folder_path, folder_path))
            if not candidate_folders:
                changed_pending_deleted_folder_keys.append(folder_key)
                _set_discovery_state(
                    scan_discovery_running=True,
                    scan_discovery_stage="filesystem",
                    scan_discovery_roots_done=idx,
                    scan_discovery_roots_total=folders_total_pending,
                    scan_discovery_files_found=len(audio_files),
                )
                _emit_files_discovery_heartbeat(
                    "watcher queue scan",
                    folders_done=idx,
                    folders_total=folders_total_pending,
                    files_found=len(audio_files),
                    force=(idx == folders_total_pending),
                )
                continue
            album_files: list[Path] = []
            for canonical_candidate, access_folder in candidate_folders:
                try:
                    candidate_files = sorted(
                        [p for p in access_folder.rglob("*") if p.is_file() and AUDIO_RE.search(p.name)],
                        key=lambda p: str(p),
                    )
                except Exception:
                    candidate_files = []
                for p in candidate_files:
                    album_files.append(p)
                    audio_files.append(p)
                    if storage_enabled:
                        _storage_add_audio_path(p)
                    else:
                        by_folder[canonical_candidate].append(p)
                        canonical_paths_by_folder[canonical_candidate].append(p)
            try:
                album_file_count = len(album_files)
            except Exception:
                album_file_count = 0
            if album_file_count <= 0:
                changed_pending_deleted_folder_keys.append(folder_key)
            _set_discovery_state(
                scan_discovery_running=True,
                scan_discovery_stage="filesystem",
                scan_discovery_roots_done=idx,
                scan_discovery_roots_total=folders_total_pending,
                scan_discovery_files_found=len(audio_files),
            )
            _emit_files_discovery_heartbeat(
                "watcher queue scan",
                folders_done=idx,
                folders_total=folders_total_pending,
                files_found=len(audio_files),
                force=(idx == folders_total_pending),
            )
    else:
        if isinstance(resume_discovery_snapshot, dict) and str(resume_discovery_snapshot.get("stage") or "").strip().lower() == "album_candidates":
            seen_paths: set[str] = set()
            audio_files = []
            cached_album_folders_from_discovery = {
                str(p or "").strip()
                for p in (resume_discovery_snapshot.get("cached_album_folders") or [])
                if str(p or "").strip()
            }
            for root_idx in sorted((resume_discovery_snapshot.get("results_by_root") or {}).keys()):
                for raw_path in (resume_discovery_snapshot.get("results_by_root") or {}).get(root_idx, []):
                    sp = str(raw_path or "").strip()
                    if not sp or sp in seen_paths:
                        continue
                    seen_paths.add(sp)
                    audio_files.append(Path(sp))
            _emit_files_discovery_heartbeat(
                "resumed album candidates",
                roots_done=len(roots),
                roots_total=len(roots),
                files_found=len(audio_files),
                force=True,
            )
        else:
            audio_files, cached_album_folders_from_discovery = _iter_audio_files_under_roots_checkpointed(
                [str(r) for r in roots],
                run_id=resume_run_id,
                progress_cb=_on_discovery_progress,
                progress_every=250,
                heartbeat_seconds=5.0,
                stop_event=stop_event,
                pause_event=pause_event,
                resume_snapshot=resume_discovery_snapshot,
                checkpoint_cb=_on_discovery_checkpoint,
                dir_skip_lookup=_dir_skip_lookup,
                dir_skip_resolver=_dir_skip_resolver,
            )
        if _files_scan_stop_requested():
            log_scan("FILES discovery cancelled during filesystem walk.")
            return _cancel_discovery()
        for p in audio_files:
            _storage_add_audio_path(p) if storage_enabled else by_folder[p.parent].append(p)
            if not storage_enabled:
                canonical_paths_by_folder[p.parent].append(p)
    with lock:
        discovery_entries_scanned_live = int(state.get("scan_discovery_entries_scanned") or 0)
        discovery_albums_found_live = int(state.get("scan_discovery_albums_found") or 0)
        discovery_artists_found_live = int(state.get("scan_discovery_artists_found") or 0)
        discovery_folders_done_live = int(state.get("scan_discovery_folders_done") or 0)
        discovery_folders_total_live = int(state.get("scan_discovery_folders_total") or 0)
    _set_scan_discovery_runtime(
        {
            "run_id": str(resume_run_id or "").strip(),
            "stage": "album_candidates",
            "roots": [str(r) for r in roots],
            "roots_total": len(roots),
            "results_by_root": {0: list(audio_files)},
            "current_root_index": None,
            "current_root_path": None,
            "current_stack": [],
            "current_root_entries_scanned": 0,
            "current_root_audio_found": len(audio_files),
            "current_root_files": [],
            "shared_entries_scanned": discovery_entries_scanned_live,
            "shared_files_found": len(audio_files),
            "shared_roots_done": len(roots),
            "albums_found": discovery_albums_found_live,
            "artists_found": discovery_artists_found_live,
            "folders_found": len(by_folder),
            "folders_done": discovery_folders_done_live,
            "folders_total": discovery_folders_total_live,
            "cached_album_folders": sorted(cached_album_folders_from_discovery),
            "paused_ack": False,
            "updated_at": time.time(),
        }
    )
    _set_discovery_state(
        scan_discovery_running=True,
        scan_discovery_stage="album_candidates",
        scan_discovery_roots_done=len(roots),
        scan_discovery_roots_total=len(roots),
        scan_discovery_current_root=None,
        scan_discovery_files_found=len(audio_files),
        scan_discovery_folders_found=len(by_folder),
        scan_discovery_folders_done=discovery_folders_done_live,
        scan_discovery_folders_total=len(by_folder),
        scan_discovery_albums_done=discovery_folders_done_live,
        scan_discovery_albums_total=len(by_folder),
        scan_discovery_albums_found=discovery_albums_found_live,
        scan_discovery_artists_found=discovery_artists_found_live,
    )
    if resume_run_id:
        initial_album_candidate_snapshot = _copy_scan_discovery_runtime(resume_run_id)
        if isinstance(initial_album_candidate_snapshot, dict):
            _persist_resume_discovery_progress_only(resume_run_id, initial_album_candidate_snapshot)
    if changed_pending_deleted_folder_keys:
        removed_from_cache = 0
        removed_from_published = 0
        try:
            con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
            cur = con.cursor()
            cur.executemany(
                "DELETE FROM files_album_scan_cache WHERE folder_path = ?",
                [(k,) for k in changed_pending_deleted_folder_keys],
            )
            removed_from_cache = int(cur.rowcount or 0)
            cur.executemany(
                "DELETE FROM files_library_published_albums WHERE folder_path = ?",
                [(k,) for k in changed_pending_deleted_folder_keys],
            )
            removed_from_published = int(cur.rowcount or 0)
            con.commit()
            con.close()
        except Exception:
            logging.debug("Failed to remove deleted changed-only folders from caches", exc_info=True)
        if removed_from_cache or removed_from_published:
            log_scan(
                "FILES changed-only: removed %d deleted album folder(s) from cache (%d scan-cache, %d published rows).",
                len(changed_pending_deleted_folder_keys),
                removed_from_cache,
                removed_from_published,
            )
        else:
            log_scan(
                "FILES changed-only: %d dirty folder(s) no longer exist on disk.",
                len(changed_pending_deleted_folder_keys),
            )
    if _files_scan_stop_requested():
        log_scan("FILES discovery cancelled before album candidate planning.")
        return _cancel_discovery()
    _emit_files_discovery_heartbeat(
        "grouped audio files",
        roots_done=len(roots),
        roots_total=len(roots),
        files_found=len(audio_files),
        folders_done=0,
        folders_total=len(by_folder),
        force=True,
    )
    album_candidate_plan_started_at = time.monotonic()
    if cached_album_folders_from_discovery:
        cache_load_started_at = time.monotonic()
        cache_map.update(
            _load_files_album_scan_cache_map(
                folder_keys=sorted(cached_album_folders_from_discovery),
                include_ordered_paths=True,
            )
        )
        log_scan(
            "FILES discovery: loaded %d cached album subtree row(s) in %.2fs before candidate planning.",
            len(cache_map),
            max(0.0, time.monotonic() - cache_load_started_at),
        )

    files_editions_by_album_id: dict[int, dict] = {}
    artist_to_album_ids: dict[str, list[int]] = defaultdict(list)
    artist_display_names: dict[str, str] = {}
    restored_partial_folder_keys: set[str] = set()
    storage_folder_bucket_key_by_folder_key: dict[str, tuple[int, str]] = {}
    storage_album_candidate_buckets: dict[tuple[int, str], dict[str, Any]] = {}
    storage_album_candidate_current_key: tuple[int, str] | None = None

    def _append_artist_bucket(artist_name_raw: str, album_id: int, *, classical_hint: bool = False) -> None:
        artist_name_clean = str(artist_name_raw or "").strip() or "Unknown Artist"
        bucket_key = _files_artist_bucket_key(artist_name_clean, classical_hint=classical_hint) or artist_name_clean
        artist_to_album_ids[bucket_key].append(int(album_id))
        artist_display_names[bucket_key] = _choose_preferred_identity_display(
            str(artist_display_names.get(bucket_key) or ""),
            artist_name_clean,
        )

    if isinstance(resume_discovery_snapshot, dict) and str(resume_discovery_snapshot.get("stage") or "").strip().lower() == "album_candidates":
        restored_partial_plan = _load_resume_files_plan_partial_by_run_id(resume_run_id)
        if isinstance(restored_partial_plan, dict):
            restored_files_editions = dict(restored_partial_plan.get("files_editions_by_album_id") or {})
            for aid, fe in sorted(restored_files_editions.items(), key=lambda item: int(item[0])):
                album_id_int = _parse_int_loose(aid, 0) or 0
                if album_id_int <= 0:
                    continue
                files_editions_by_album_id[album_id_int] = dict(fe or {})
                artist_name_restored = str((fe or {}).get("artist_name") or (fe or {}).get("artist") or "").strip() or "Unknown Artist"
                _append_artist_bucket(artist_name_restored, album_id_int)
                folder_key_restored = _album_folder_cache_key((fe or {}).get("folder"))
                if folder_key_restored:
                    restored_partial_folder_keys.add(folder_key_restored)
            if files_editions_by_album_id:
                log_scan(
                    "FILES discovery: restored %d partial album candidate(s) from resume plan %s.",
                    len(files_editions_by_album_id),
                    str(resume_run_id or ""),
                )

    def _storage_album_candidate_bucket_key_for_folder(folder_path: Path) -> tuple[int, str] | None:
        if not storage_enabled:
            return None
        meta = storage_meta_by_folder.get(str(folder_path)) or {}
        device_id = str(meta.get("storage_device_id") or "").strip()
        if not device_id:
            return None
        try:
            bucket_order = int(meta.get("storage_bucket_order") or 0)
        except Exception:
            bucket_order = 0
        return (bucket_order, device_id)

    def _storage_album_candidate_rebuild_bucket_runtime(
        *,
        current_key: tuple[int, str] | None = None,
    ) -> None:
        if not storage_enabled:
            return
        plan_summary: list[dict[str, Any]] = []
        buckets_done = 0
        current_bucket_done = 0
        current_bucket_total = 0
        current_bucket_payload: dict[str, Any] | None = None
        for key in sorted(storage_album_candidate_buckets.keys(), key=lambda item: (int(item[0]), str(item[1]))):
            bucket = storage_album_candidate_buckets.get(key) or {}
            albums_total = int(bucket.get("albums_total") or 0)
            albums_done = min(albums_total, int(bucket.get("albums_done") or 0))
            status = str(bucket.get("status") or "").strip().lower()
            if albums_total > 0 and albums_done >= albums_total:
                status = "done"
            elif current_key == key:
                status = "running"
            elif not status:
                status = "pending"
            if status == "done":
                buckets_done += 1
            payload = {
                "storage_provider": str(bucket.get("storage_provider") or "unraid"),
                "storage_device_id": str(bucket.get("storage_device_id") or ""),
                "storage_device_label": str(bucket.get("storage_device_label") or bucket.get("storage_device_id") or ""),
                "storage_bucket_order": int(bucket.get("storage_bucket_order") or 0),
                "albums_total": albums_total,
                "albums_done": albums_done,
                "status": status,
                "started_at": bucket.get("started_at"),
                "finished_at": bucket.get("finished_at"),
                "canonical_root": str(bucket.get("canonical_root") or ""),
                "access_root": str(bucket.get("access_root") or ""),
            }
            plan_summary.append(payload)
            if current_key == key:
                current_bucket_done = albums_done
                current_bucket_total = albums_total
                current_bucket_payload = dict(payload)
        with lock:
            state["storage_scan_plan"] = plan_summary
            state["storage_buckets_total"] = len(plan_summary)
            state["storage_buckets_done"] = buckets_done
            state["storage_current_bucket"] = current_bucket_payload
            state["storage_bucket_done"] = current_bucket_done
            state["storage_bucket_total"] = current_bucket_total
            if current_bucket_payload:
                state["storage_current_device_id"] = str(current_bucket_payload.get("storage_device_id") or "")
                state["storage_current_device_label"] = str(
                    current_bucket_payload.get("storage_device_label")
                    or current_bucket_payload.get("storage_device_id")
                    or ""
                )
                state["storage_active_devices"] = 1
            else:
                state["storage_current_device_id"] = None
                state["storage_current_device_label"] = None
                state["storage_active_devices"] = 0
            state["storage_estimated_watts_saved"] = _storage_estimated_watts_saved(
                int(state.get("storage_active_devices") or 0),
                int(state.get("storage_devices_total") or 0),
            )

    def _storage_album_candidate_mark_start(folder_path: Path) -> None:
        nonlocal storage_album_candidate_current_key
        if not storage_enabled:
            return
        bucket_key = _storage_album_candidate_bucket_key_for_folder(folder_path)
        if not bucket_key:
            return
        bucket = storage_album_candidate_buckets.get(bucket_key) or {}
        if storage_album_candidate_current_key == bucket_key:
            return
        now_ts = time.time()
        storage_album_candidate_current_key = bucket_key
        if not bucket.get("started_at"):
            bucket["started_at"] = now_ts
        if str(bucket.get("status") or "").strip().lower() != "done":
            bucket["status"] = "running"
        storage_album_candidate_buckets[bucket_key] = bucket
        _storage_album_candidate_rebuild_bucket_runtime(current_key=bucket_key)
        log_scan(
            "[STORAGE] Discovery bucket start: %s (%s), folders=%d, active disks 1/%d.",
            str(bucket.get("storage_device_label") or bucket.get("storage_device_id") or ""),
            str(bucket.get("storage_device_id") or ""),
            int(bucket.get("albums_total") or 0),
            int(state.get("storage_devices_total") or 0),
        )
        _update_scan_storage_bucket_row(
            resume_run_id,
            bucket,
            status="running",
            albums_done=int(bucket.get("albums_done") or 0),
            started_at=now_ts,
            message="album_candidates bucket active",
        )

    def _storage_album_candidate_mark_progress(folder_path: Path, *, folder_count: int = 1) -> None:
        if not storage_enabled:
            return
        bucket_key = _storage_album_candidate_bucket_key_for_folder(folder_path)
        if not bucket_key:
            return
        bucket = storage_album_candidate_buckets.get(bucket_key) or {}
        albums_total = int(bucket.get("albums_total") or 0)
        albums_done = min(albums_total, int(bucket.get("albums_done") or 0) + max(0, int(folder_count or 0)))
        bucket["albums_done"] = albums_done
        if albums_total > 0 and albums_done >= albums_total:
            bucket["status"] = "done"
            if not bucket.get("finished_at"):
                bucket["finished_at"] = time.time()
        elif storage_album_candidate_current_key == bucket_key:
            bucket["status"] = "running"
        storage_album_candidate_buckets[bucket_key] = bucket
        _storage_album_candidate_rebuild_bucket_runtime(current_key=storage_album_candidate_current_key)
        _update_scan_storage_bucket_row(
            resume_run_id,
            bucket,
            status=str(bucket.get("status") or "running"),
            albums_done=albums_done,
            finished_at=bucket.get("finished_at"),
            message="album_candidates progress",
        )
        if bucket.get("finished_at") and not bucket.get("_end_logged"):
            bucket["_end_logged"] = True
            storage_album_candidate_buckets[bucket_key] = bucket
            log_scan(
                "[STORAGE] Discovery bucket end: %s (%s), folders %d/%d.",
                str(bucket.get("storage_device_label") or bucket.get("storage_device_id") or ""),
                str(bucket.get("storage_device_id") or ""),
                albums_done,
                albums_total,
            )

    next_album_id = (max(files_editions_by_album_id.keys()) + 1) if files_editions_by_album_id else 1
    skipped_unchanged_complete = 0
    fast_skip_marked = 0
    fast_skip_full_cached = 0

    folder_iteration_items: list[tuple[Path, list[Path]]] = sorted(by_folder.items(), key=lambda x: str(x[0]))
    if storage_enabled:
        for folder_path, _paths in by_folder.items():
            meta = storage_meta_by_folder.get(str(folder_path)) or {}
            device_id = str(meta.get("storage_device_id") or "").strip()
            if not device_id:
                continue
            try:
                bucket_order = int(meta.get("storage_bucket_order") or 0)
            except Exception:
                bucket_order = 0
            bucket_key = (bucket_order, device_id)
            folder_key = _album_folder_cache_key(folder_path)
            if folder_key:
                storage_folder_bucket_key_by_folder_key[folder_key] = bucket_key
            bucket = storage_album_candidate_buckets.setdefault(
                bucket_key,
                {
                    "storage_provider": str(meta.get("storage_provider") or "unraid"),
                    "storage_device_id": device_id,
                    "storage_device_label": str(meta.get("storage_device_label") or device_id).strip() or device_id,
                    "storage_bucket_order": bucket_order,
                    "canonical_root": str(meta.get("canonical_root") or ""),
                    "access_root": str(meta.get("access_root") or ""),
                    "albums_total": 0,
                    "albums_done": 0,
                    "status": "pending",
                    "started_at": None,
                    "finished_at": None,
                },
            )
            bucket["albums_total"] = int(bucket.get("albums_total") or 0) + 1
        for folder_key in restored_partial_folder_keys:
            bucket_key = storage_folder_bucket_key_by_folder_key.get(folder_key)
            if not bucket_key:
                continue
            bucket = storage_album_candidate_buckets.get(bucket_key) or {}
            bucket["albums_done"] = min(
                int(bucket.get("albums_total") or 0),
                int(bucket.get("albums_done") or 0) + 1,
            )
            storage_album_candidate_buckets[bucket_key] = bucket
        folder_iteration_items = sorted(
            by_folder.items(),
            key=lambda item: (
                int((storage_meta_by_folder.get(str(item[0])) or {}).get("storage_bucket_order") or 10**9),
                str((storage_meta_by_folder.get(str(item[0])) or {}).get("storage_device_id") or ""),
                str(item[0]),
            ),
        )
        _storage_album_candidate_rebuild_bucket_runtime()
    first_pending_folder: Path | None = None
    first_pending_device_id = ""
    first_pending_device_label = ""
    for folder_path, _paths in folder_iteration_items:
        folder_key = _album_folder_cache_key(folder_path)
        if folder_key and folder_key in restored_partial_folder_keys:
            continue
        first_pending_folder = folder_path
        first_meta = storage_meta_by_folder.get(str(folder_path)) or {}
        first_pending_device_id = str(first_meta.get("storage_device_id") or "").strip()
        first_pending_device_label = str(first_meta.get("storage_device_label") or first_pending_device_id).strip() or first_pending_device_id
        break
    log_scan(
        "FILES discovery: candidate planning ready in %.2fs | folders=%d | restored=%d | first pending=%s | device=%s (%s).",
        max(0.0, time.monotonic() - album_candidate_plan_started_at),
        len(folder_iteration_items),
        len(restored_partial_folder_keys),
        str(first_pending_folder or ""),
        first_pending_device_label,
        first_pending_device_id,
    )

    folders_total = len(by_folder)
    snapshot_folders_done = (
        int(resume_discovery_snapshot.get("folders_done") or 0)
        if isinstance(resume_discovery_snapshot, dict)
        else 0
    )
    restored_folders_done = min(len(restored_partial_folder_keys), folders_total)
    if snapshot_folders_done > restored_folders_done:
        log_scan(
            "FILES discovery resume: snapshot said %d folder(s) done but only %d candidate row(s) were persisted; resuming from persisted plan boundary.",
            snapshot_folders_done,
            restored_folders_done,
        )
    folders_done = restored_folders_done
    album_candidate_snapshot_every_folders = 250
    album_candidate_snapshot_every_seconds = 10.0
    album_candidate_last_snapshot_done = folders_done
    album_candidate_last_snapshot_ts = 0.0
    partial_plan_flush_every_rows = 100
    partial_plan_flush_every_seconds = 10.0
    partial_plan_last_flush_ts = 0.0
    partial_plan_buffer: set[int] = set()

    if folders_done > 0:
        _set_discovery_state(
            scan_discovery_folders_done=folders_done,
            scan_discovery_albums_done=folders_done,
            scan_discovery_albums_found=next_album_id - 1,
            scan_discovery_artists_found=len(artist_to_album_ids),
        )

    def _sync_album_candidate_progress(
        *,
        current_folder_path: str | None = None,
        force: bool = False,
    ) -> None:
        nonlocal album_candidate_last_snapshot_done, album_candidate_last_snapshot_ts
        effective_run_id = str(resume_run_id or "").strip()
        if not effective_run_id:
            with lock:
                effective_run_id = (
                    str(state.get("scan_resume_run_id") or "").strip()
                    or str(state.get("scan_resume_requested_run_id") or "").strip()
                )
        runtime_snapshot = _copy_scan_discovery_runtime(effective_run_id) or {}
        if not isinstance(runtime_snapshot, dict):
            runtime_snapshot = {}
        runtime_snapshot.update(
            {
                "run_id": effective_run_id,
                "stage": "album_candidates",
                "roots": [str(r) for r in roots],
                "roots_total": len(roots),
                "current_root_path": str(current_folder_path or "").strip() or runtime_snapshot.get("current_root_path"),
                "shared_entries_scanned": discovery_entries_scanned_live,
                "shared_files_found": len(audio_files),
                "shared_roots_done": len(roots),
                "entries_scanned": discovery_entries_scanned_live,
                "files_found": len(audio_files),
                "folders_found": len(by_folder),
                "folders_done": int(folders_done or 0),
                "folders_total": int(folders_total or 0),
                "albums_found": int(next_album_id - 1),
                "artists_found": int(len(artist_to_album_ids)),
                "cached_album_folders": sorted(cached_album_folders_from_discovery),
                "paused_ack": False,
                "updated_at": time.time(),
            }
        )
        _set_scan_discovery_runtime(runtime_snapshot)
        if not effective_run_id:
            return
        now = time.monotonic()
        should_persist = bool(
            force
            or folders_done <= 1
            or folders_done >= folders_total
            or (folders_done - int(album_candidate_last_snapshot_done or 0)) >= album_candidate_snapshot_every_folders
            or (now - float(album_candidate_last_snapshot_ts or 0.0)) >= album_candidate_snapshot_every_seconds
        )
        if not should_persist:
            return
        if _persist_resume_discovery_progress_only(effective_run_id, runtime_snapshot).get("ok"):
            album_candidate_last_snapshot_done = int(folders_done or 0)
            album_candidate_last_snapshot_ts = now

    def _flush_partial_resume_files_plan(*, force: bool = False) -> None:
        nonlocal partial_plan_last_flush_ts
        effective_run_id = str(resume_run_id or "").strip()
        if not effective_run_id:
            return
        if not partial_plan_buffer and not force:
            return
        now = time.monotonic()
        if not force:
            if len(partial_plan_buffer) < partial_plan_flush_every_rows and (
                now - float(partial_plan_last_flush_ts or 0.0)
            ) < partial_plan_flush_every_seconds:
                return
        rows = _upsert_resume_files_plan_partial(
            effective_run_id,
            sorted(partial_plan_buffer),
            files_editions_by_album_id,
            detected_artists_total=len(artist_to_album_ids),
            detected_albums_total=next_album_id - 1,
            detected_tracks_total=len(audio_files),
        )
        if rows > 0:
            partial_plan_buffer.clear()
            partial_plan_last_flush_ts = now

    def _flush_album_candidate_pause_snapshot() -> None:
        effective_run_id = str(resume_run_id or "").strip()
        _flush_partial_resume_files_plan(force=True)
        _sync_album_candidate_progress(force=True)
        runtime_snapshot = _copy_scan_discovery_runtime(effective_run_id) or {}
        if isinstance(runtime_snapshot, dict):
            runtime_snapshot["paused_ack"] = True
            runtime_snapshot["updated_at"] = time.time()
            _set_scan_discovery_runtime(runtime_snapshot)

    pause_flush_cb = _flush_album_candidate_pause_snapshot
    normalize_parenthetical = bool(_parse_bool(_get_config_from_db("NORMALIZE_PARENTHETICAL_FOR_DEDUPE") or "true"))
    resolved_skip_roots: list[Path] = []
    for skip_entry in skip_list:
        if not skip_entry:
            continue
        try:
            resolved_skip_roots.append(Path(skip_entry).resolve())
        except Exception:
            continue
    album_candidate_workers = max(1, int(SCAN_THREADS or 1))

    def _prepare_album_candidate_folder(
        folder: Path,
        paths: list[Path],
        storage_meta: dict[str, Any],
    ) -> dict[str, Any]:
        folder_for_io = Path(str(storage_meta.get("storage_access_path") or folder))
        try:
            folder_resolved = folder if storage_meta else folder.resolve()
        except (OSError, RuntimeError):
            return {"action": "skip"}
        if resolved_skip_roots:
            try:
                if any(folder_resolved.is_relative_to(skip_root) for skip_root in resolved_skip_roots):
                    logging.debug("Skipping folder (SKIP_FOLDERS): %s", folder)
                    return {"action": "skip"}
            except (ValueError, OSError):
                pass

        ordered_paths = sorted(paths, key=lambda p: str(p))
        canonical_ordered_paths = sorted(canonical_paths_by_folder.get(folder) or ordered_paths, key=lambda p: str(p))
        if not ordered_paths:
            return {"action": "skip"}

        folder_key = _album_folder_cache_key(folder_resolved)
        cached = cache_map.get(folder_key) or {}
        folder_from_cached_subtree = folder_key in cached_album_folders_from_discovery
        if folder_from_cached_subtree and cached.get("ordered_paths") and not storage_meta:
            ordered_paths = [Path(str(p)) for p in (cached.get("ordered_paths") or []) if str(p or "").strip()]
            canonical_ordered_paths = list(ordered_paths)
        source_id_current = int(cached.get("source_id") or _source_id_for_path(folder_resolved) or 0)
        cached_missing = cached.get("missing_required_tags") or []
        cached_has_cover = bool(cached.get("has_cover"))
        cached_has_artist_image = bool(cached.get("has_artist_image"))
        cached_healthy = bool(
            cached
            and cached_has_cover
            and cached_has_artist_image
            and cached.get("has_complete_tags")
            and cached.get("has_identity")
            and not cached_missing
        )
        if folder_from_cached_subtree and cached:
            fingerprint = str(cached.get("fingerprint") or "").strip()
            unchanged = bool(fingerprint)
            has_cover_now = cached_has_cover
            has_artist_image_now = cached_has_artist_image
            cached_fast_skip = bool(unchanged and cached_healthy and ordered_paths)
        else:
            fingerprint = _compute_album_fingerprint(ordered_paths)
            unchanged = bool(cached and (cached.get("fingerprint") == fingerprint))
            has_cover_now = album_folder_has_cover(folder_for_io)
            has_artist_image_now = _artist_folder_has_image(folder_for_io.parent if folder_for_io.parent else folder_for_io)
            cached_fast_skip = bool(
                unchanged
                and cached_healthy
                and has_cover_now
                and has_artist_image_now
            )
        if cached_fast_skip and scan_type == "changed_only":
            return {"action": "skip_changed_only"}
        if cached_fast_skip and scan_type in {"full", "incomplete_only"}:
            artist_name = (cached.get("artist_name") or folder_resolved.parent.name.replace("_", " ") or "Unknown Artist").strip() or "Unknown Artist"
            album_title_tag = (cached.get("album_title") or folder_resolved.name.replace("_", " ")).strip() or "Unknown Album"
            album_title_tag = _sanitize_album_title_display(album_title_tag)
            tracks: list[Track] = []
            for i, p in enumerate(ordered_paths):
                disc, trk = _infer_disc_track_from_filename(p, i + 1)
                tracks.append(Track(title=_title_from_filename(p, i + 1), idx=trk, disc=disc, dur=0))
            if not tracks:
                return {"action": "skip"}
            exts = [p.suffix.lower().lstrip(".") for p in ordered_paths]
            format_ext = max(set(exts), key=exts.count).upper() if exts else "UNKNOWN"
            album_norm = norm_album_for_dedup(album_title_tag, normalize_parenthetical)
            identity_now = _extract_files_identity_fields(tags={}, edition={}, cached=cached)
            mbid_now = identity_now["musicbrainz_id"]
            has_mbid_now = bool(identity_now["has_mbid"])
            has_identity_now = bool(identity_now["has_identity"])
            identity_provider_now = identity_now["identity_provider"]
            return {
                "action": "edition",
                "artist_name": artist_name,
                "classical_hint": False,
                "fast_skip_marked": True,
                "fast_skip_full_cached": True,
                "edition": {
                    "folder": folder,
                    "artist": artist_name,
                    "artist_name": artist_name,
                    "title_raw": album_title_tag,
                    "album_title": album_title_tag,
                    "album_norm": album_norm,
                    "tracks": tracks,
                    "format": format_ext,
                    "tags": {
                        "artist": artist_name,
                        "album": album_title_tag,
                        "musicbrainz_releasegroupid": mbid_now,
                        "musicbrainz_albumid": mbid_now,
                    },
                    "confidence_score": 0.9,
                    "file_count": len(ordered_paths),
                    "ordered_paths": ordered_paths,
                    "canonical_ordered_paths": canonical_ordered_paths,
                    "fingerprint": fingerprint,
                    "folder_key": folder_key,
                    "source_id": source_id_current if source_id_current > 0 else None,
                    "storage_provider": storage_meta.get("storage_provider") or "",
                    "storage_device_id": storage_meta.get("storage_device_id") or "",
                    "storage_device_label": storage_meta.get("storage_device_label") or "",
                    "storage_bucket_order": int(storage_meta.get("storage_bucket_order") or 0),
                    "storage_rel_path": storage_meta.get("storage_rel_path") or "",
                    "storage_access_path": storage_meta.get("storage_access_path") or "",
                    "missing_required_tags": list(cached_missing),
                    "has_cover": has_cover_now,
                    "has_artist_image": has_artist_image_now,
                    "has_mbid": has_mbid_now,
                    "has_identity": has_identity_now,
                    "identity_provider": identity_provider_now,
                    "strict_match_verified": bool(identity_now.get("strict_match_verified")),
                    "strict_match_provider": identity_now.get("strict_match_provider") or "",
                    "strict_reject_reason": identity_now.get("strict_reject_reason") or "",
                    "strict_tracklist_score": float(identity_now.get("strict_tracklist_score") or 0.0),
                    "musicbrainz_id": mbid_now,
                    "discogs_release_id": identity_now["discogs_release_id"],
                    "lastfm_album_mbid": identity_now["lastfm_album_mbid"],
                    "bandcamp_album_url": identity_now["bandcamp_album_url"],
                    "metadata_source": identity_now["metadata_source"],
                    "skip_heavy_processing": True,
                },
            }

        first_tags = extract_tags(ordered_paths[0]) or {}
        folder_name_fallback = folder.name.replace("_", " ")
        artist_name = _pick_album_artist_from_tag_dicts([first_tags], default="Unknown Artist")
        album_title_tag = _pick_album_title_from_tag_dicts([first_tags], fallback=folder_name_fallback)
        album_title_tag = _sanitize_album_title_display(album_title_tag)
        filename_identity_hints: dict[str, str] = {}
        try:
            parent_name = (folder.parent.name or "").replace("_", " ").strip()
        except Exception:
            parent_name = ""
        needs_consensus_tags = bool(
            not bool(_normalize_meta_text(first_tags.get("album")))
            or not _identity_artist_fallback_is_usable(artist_name)
            or album_title_tag == _sanitize_album_title_display(folder_name_fallback)
            or _identity_folder_name_looks_like_container(parent_name)
        )
        if needs_consensus_tags and len(ordered_paths) > 1:
            tag_samples: list[dict] = []
            for sample_path in ordered_paths[: min(len(ordered_paths), 6)]:
                try:
                    sample_tags = extract_tags(sample_path) or {}
                except Exception:
                    sample_tags = {}
                if isinstance(sample_tags, dict):
                    tag_samples.append(sample_tags)
            if tag_samples:
                sampled_artist = _pick_album_artist_from_tag_dicts(tag_samples, default=artist_name)
                sampled_album = _pick_album_title_from_tag_dicts(tag_samples, fallback=album_title_tag or folder_name_fallback)
                if _identity_artist_fallback_is_usable(sampled_artist):
                    artist_name = sampled_artist
                if str(sampled_album or "").strip():
                    album_title_tag = _sanitize_album_title_display(sampled_album)
        if (
            not _identity_artist_fallback_is_usable(artist_name)
            or album_title_tag == _sanitize_album_title_display(folder_name_fallback)
            or not bool(_normalize_meta_text(first_tags.get("album")))
        ):
            filename_identity_hints = _filename_identity_hints(ordered_paths)
            hinted_artist = str(filename_identity_hints.get("artist") or "").strip()
            hinted_album = str(filename_identity_hints.get("album") or "").strip()
            if _identity_artist_fallback_is_usable(hinted_artist):
                artist_name = hinted_artist
            if hinted_album:
                album_title_tag = _sanitize_album_title_display(hinted_album)
        if not _identity_artist_fallback_is_usable(artist_name) and _identity_artist_fallback_is_usable(parent_name):
            artist_name = parent_name
        if (
            not bool(_normalize_meta_text(first_tags.get("album")))
            or not _identity_artist_fallback_is_usable(artist_name)
            or album_title_tag == _sanitize_album_title_display(folder_name_fallback)
        ):
            try:
                inferred_artist_name, inferred_album_title = _infer_artist_album_from_folder(folder, ordered_paths)
            except Exception:
                inferred_artist_name, inferred_album_title = ("", "")
            if _identity_artist_fallback_is_usable(inferred_artist_name) and not _identity_artist_fallback_is_usable(artist_name):
                artist_name = inferred_artist_name
            if inferred_album_title and (
                not bool(_normalize_meta_text(first_tags.get("album")))
                or album_title_tag == _sanitize_album_title_display(folder_name_fallback)
            ):
                album_title_tag = _sanitize_album_title_display(inferred_album_title)

        tracks: list[Track] = []
        first_disc, first_trk = _parse_disc_track_loose(first_tags, fallback_disc=1, fallback_track=1)
        first_title = (first_tags.get("title") or first_tags.get("name") or "").strip()
        use_mutagen_tracks = False
        if MutagenFile is not None and ordered_paths:
            explicit = sum(1 for p in ordered_paths if _filename_has_explicit_track_number(p))
            if (explicit / max(1, len(ordered_paths))) < 0.70:
                use_mutagen_tracks = True

        for i, p in enumerate(ordered_paths):
            if use_mutagen_tracks:
                disc, trk, title = _infer_track_from_mutagen(p, i + 1)
            else:
                disc, trk = _infer_disc_track_from_filename(p, i + 1)
                title = _title_from_filename(p, i + 1)
            if i == 0 and not use_mutagen_tracks:
                disc = first_disc or disc
                trk = first_trk or trk
                title = first_title or title
            tracks.append(Track(title=(title or "").strip(), idx=trk, disc=disc, dur=0))

        if not tracks:
            return {"action": "skip"}

        missing_required_now = _check_required_tags(first_tags, REQUIRED_TAGS, edition={"tracks": tracks})
        ai_identity_hint = {}
        if not _ai_scan_lifecycle_phase_active():
            ai_identity_hint = _infer_identity_from_local_context_ai(
                local_artist=artist_name,
                local_album=album_title_tag,
                folder_path=folder,
                track_titles=[str(getattr(t, "title", "") or "") for t in tracks],
                file_paths=ordered_paths,
                local_tags=first_tags if isinstance(first_tags, dict) else {},
                missing_required_tags=list(missing_required_now or []),
            )
        discovery_identity_ctx = {
            "artist": artist_name,
            "artist_name": artist_name,
            "title_raw": album_title_tag,
            "album_title": album_title_tag,
            "missing_required_tags": list(missing_required_now or []),
        }
        if isinstance(ai_identity_hint, dict) and ai_identity_hint:
            hinted_artist = str(ai_identity_hint.get("artist") or "").strip()
            hinted_album = str(ai_identity_hint.get("album") or "").strip()
            hint_conf = int(ai_identity_hint.get("confidence") or 0)
            if hinted_artist and hinted_album and hint_conf >= 65:
                discovery_identity_ctx["_lookup_artist_name"] = hinted_artist
                discovery_identity_ctx["_lookup_album_title"] = hinted_album
                discovery_identity_ctx["_lookup_identity_hint"] = ai_identity_hint
                artist_name, album_title_tag = _apply_resolved_identity_to_edition(
                    discovery_identity_ctx,
                    default_artist=artist_name,
                    default_title=album_title_tag,
                    folder_name=folder.name,
                )
        classical_hint = bool(
            _classical_display_payload(
                first_tags,
                fallback_title=album_title_tag,
                fallback_artist=artist_name,
            )
        )

        exts = [p.suffix.lower().lstrip(".") for p in ordered_paths]
        format_ext = max(set(exts), key=exts.count).upper() if exts else "UNKNOWN"
        has_album_tag = bool(_normalize_meta_text(first_tags.get("album")))
        has_artist_tag = bool(_normalize_meta_text(first_tags.get("artist") or first_tags.get("albumartist")))
        confidence = 0.5 + (0.2 if has_album_tag else 0) + (0.1 if has_artist_tag else 0) + (0.2 if len(ordered_paths) >= 3 else 0)
        album_norm = norm_album_for_dedup(album_title_tag, normalize_parenthetical)
        fingerprint = _compute_album_fingerprint(ordered_paths)
        has_cover_now = album_folder_has_cover(folder_for_io)
        has_artist_image_now = _artist_folder_has_image(folder_for_io.parent if folder_for_io.parent else folder_for_io)
        cached = cache_map.get(folder_key) or {}
        identity_now = _extract_files_identity_fields(tags=first_tags, edition={}, cached=cached)
        mbid_now = identity_now["musicbrainz_id"]
        has_mbid_now = bool(identity_now["has_mbid"])
        has_identity_now = bool(identity_now["has_identity"])
        identity_provider_now = identity_now["identity_provider"]
        cached_missing = cached.get("missing_required_tags") or []
        cached_healthy = bool(
            cached
            and cached.get("has_cover")
            and cached.get("has_artist_image")
            and cached.get("has_complete_tags")
            and cached.get("has_identity")
            and not cached_missing
        )
        unchanged = bool(cached and (cached.get("fingerprint") == fingerprint))
        current_healthy = bool(
            has_cover_now
            and has_artist_image_now
            and has_identity_now
            and not missing_required_now
        )
        fast_skip_heavy = unchanged and cached_healthy and current_healthy
        if scan_type == "changed_only" and fast_skip_heavy:
            return {"action": "skip_changed_only"}

        return {
            "action": "edition",
            "artist_name": artist_name,
            "classical_hint": classical_hint,
            "fast_skip_marked": bool(fast_skip_heavy),
            "fast_skip_full_cached": False,
            "edition": {
                "folder": folder,
                "artist": artist_name,
                "artist_name": artist_name,
                "title_raw": album_title_tag,
                "album_title": album_title_tag,
                "album_norm": album_norm,
                "tracks": tracks,
                "format": format_ext,
                "tags": first_tags,
                "confidence_score": confidence,
                "file_count": len(ordered_paths),
                "ordered_paths": ordered_paths,
                "canonical_ordered_paths": canonical_ordered_paths,
                "fingerprint": fingerprint,
                "folder_key": folder_key,
                "source_id": source_id_current if source_id_current > 0 else None,
                "storage_provider": storage_meta.get("storage_provider") or "",
                "storage_device_id": storage_meta.get("storage_device_id") or "",
                "storage_device_label": storage_meta.get("storage_device_label") or "",
                "storage_bucket_order": int(storage_meta.get("storage_bucket_order") or 0),
                "storage_rel_path": storage_meta.get("storage_rel_path") or "",
                "storage_access_path": storage_meta.get("storage_access_path") or "",
                "missing_required_tags": missing_required_now,
                "has_cover": has_cover_now,
                "has_artist_image": has_artist_image_now,
                "has_mbid": has_mbid_now,
                "has_identity": has_identity_now,
                "identity_provider": identity_provider_now,
                "strict_match_verified": bool(identity_now.get("strict_match_verified")),
                "strict_match_provider": identity_now.get("strict_match_provider") or "",
                "strict_reject_reason": identity_now.get("strict_reject_reason") or "",
                "strict_tracklist_score": float(identity_now.get("strict_tracklist_score") or 0.0),
                "musicbrainz_id": mbid_now,
                "discogs_release_id": identity_now["discogs_release_id"],
                "lastfm_album_mbid": identity_now["lastfm_album_mbid"],
                "bandcamp_album_url": identity_now["bandcamp_album_url"],
                "metadata_source": identity_now["metadata_source"],
                "skip_heavy_processing": fast_skip_heavy,
                "_lookup_artist_name": discovery_identity_ctx.get("_lookup_artist_name") or "",
                "_lookup_album_title": discovery_identity_ctx.get("_lookup_album_title") or "",
                "_lookup_identity_hint": discovery_identity_ctx.get("_lookup_identity_hint") or {},
            },
        }

    pending_folder_iteration_items: list[tuple[Path, list[Path], dict[str, Any], Path, tuple[int, str] | None]] = []
    for folder, paths in folder_iteration_items:
        folder_resume_key = _album_folder_cache_key(folder)
        if folder_resume_key and folder_resume_key in restored_partial_folder_keys:
            continue
        storage_meta = storage_meta_by_folder.get(str(folder)) or {}
        folder_for_io = Path(str(storage_meta.get("storage_access_path") or folder))
        bucket_key = _storage_album_candidate_bucket_key_for_folder(folder) if storage_enabled else None
        pending_folder_iteration_items.append((folder, paths, storage_meta, folder_for_io, bucket_key))

    bucketed_folder_iteration_items: list[list[tuple[Path, list[Path], dict[str, Any], Path, tuple[int, str] | None]]] = []
    if storage_enabled:
        current_bucket_items: list[tuple[Path, list[Path], dict[str, Any], Path, tuple[int, str] | None]] = []
        current_bucket_key: tuple[int, str] | None = None
        for item in pending_folder_iteration_items:
            if current_bucket_items and item[4] != current_bucket_key:
                bucketed_folder_iteration_items.append(current_bucket_items)
                current_bucket_items = []
            if not current_bucket_items:
                current_bucket_key = item[4]
            current_bucket_items.append(item)
        if current_bucket_items:
            bucketed_folder_iteration_items.append(current_bucket_items)
    elif pending_folder_iteration_items:
        bucketed_folder_iteration_items = [pending_folder_iteration_items]

    # Keep one storage bucket active at a time, but saturate that single disk with the
    # full scan worker budget. Commit completed folder results in the original order so
    # resume snapshots remain deterministic across restarts.
    for bucket_items in bucketed_folder_iteration_items:
        if _files_scan_stop_requested():
            _flush_partial_resume_files_plan(force=True)
            log_scan("FILES discovery cancelled while building album candidates (%d/%d folders).", folders_done, folders_total)
            return _cancel_discovery()
        if not _files_scan_wait_if_paused():
            _flush_partial_resume_files_plan(force=True)
            log_scan("FILES discovery cancelled while paused during album candidate build (%d/%d folders).", folders_done, folders_total)
            return _cancel_discovery()
        bucket_workers = max(1, min(album_candidate_workers, len(bucket_items)))
        in_flight: dict[int, Any] = {}
        next_submit_idx = 0

        def _submit_bucket_task(pool: ThreadPoolExecutor) -> bool:
            nonlocal next_submit_idx
            if next_submit_idx >= len(bucket_items):
                return False
            folder, paths, storage_meta, _folder_for_io, _bucket_key = bucket_items[next_submit_idx]
            in_flight[next_submit_idx] = pool.submit(_prepare_album_candidate_folder, folder, paths, storage_meta)
            next_submit_idx += 1
            return True

        with ThreadPoolExecutor(max_workers=bucket_workers, thread_name_prefix="pmda-album-candidates") as pool:
            while len(in_flight) < bucket_workers and _submit_bucket_task(pool):
                pass
            for item_idx, (folder, _paths, storage_meta, folder_for_io, _bucket_key) in enumerate(bucket_items):
                if _files_scan_stop_requested():
                    _flush_partial_resume_files_plan(force=True)
                    log_scan("FILES discovery cancelled while building album candidates (%d/%d folders).", folders_done, folders_total)
                    return _cancel_discovery()
                if not _files_scan_wait_if_paused():
                    _flush_partial_resume_files_plan(force=True)
                    log_scan("FILES discovery cancelled while paused during album candidate build (%d/%d folders).", folders_done, folders_total)
                    return _cancel_discovery()
                future = in_flight.pop(item_idx)
                prepared: dict[str, Any] = {"action": "skip"}
                while True:
                    if _files_scan_stop_requested():
                        _flush_partial_resume_files_plan(force=True)
                        log_scan("FILES discovery cancelled while building album candidates (%d/%d folders).", folders_done, folders_total)
                        return _cancel_discovery()
                    if not _files_scan_wait_if_paused():
                        _flush_partial_resume_files_plan(force=True)
                        log_scan("FILES discovery cancelled while paused during album candidate build (%d/%d folders).", folders_done, folders_total)
                        return _cancel_discovery()
                    try:
                        prepared = future.result(timeout=0.25)
                        break
                    except FutureTimeout:
                        continue
                    except Exception:
                        logging.debug("FILES album candidate worker failed for folder=%s", folder, exc_info=True)
                        prepared = {"action": "skip"}
                        break
                _submit_bucket_task(pool)

                folders_done += 1
                if storage_enabled:
                    _storage_album_candidate_mark_start(folder)
                folder_storage_updates: dict[str, Any] = {}
                if storage_enabled:
                    folder_storage_updates = {
                        "scan_discovery_current_root": str(folder_for_io),
                        **_storage_discovery_device_fields_for_path(folder_for_io, storage_entries),
                    }
                _set_discovery_state(
                    scan_discovery_running=True,
                    scan_discovery_stage="album_candidates",
                    scan_discovery_folders_done=folders_done,
                    scan_discovery_folders_total=folders_total,
                    scan_discovery_albums_done=folders_done,
                    scan_discovery_albums_total=folders_total,
                    **folder_storage_updates,
                )
                _emit_files_discovery_heartbeat(
                    "building album candidates",
                    root=str(folder_for_io),
                    files_found=len(audio_files),
                    folders_done=folders_done,
                    folders_total=folders_total,
                    artists_found=len(artist_to_album_ids),
                    albums_found=next_album_id - 1,
                )
                _sync_album_candidate_progress(current_folder_path=str(folder_for_io), force=(folders_done <= 1))

                action = str(prepared.get("action") or "skip").strip().lower()
                if action == "skip_changed_only":
                    skipped_unchanged_complete += 1
                    _sync_album_candidate_progress(current_folder_path=str(folder_for_io))
                    if storage_enabled:
                        _storage_album_candidate_mark_progress(folder)
                    continue
                if action != "edition":
                    _sync_album_candidate_progress(current_folder_path=str(folder_for_io))
                    if storage_enabled:
                        _storage_album_candidate_mark_progress(folder)
                    continue

                edition_payload = dict(prepared.get("edition") or {})
                artist_name_prepared = str(prepared.get("artist_name") or edition_payload.get("artist_name") or "Unknown Artist").strip() or "Unknown Artist"
                classical_hint = bool(prepared.get("classical_hint"))
                album_id = next_album_id
                next_album_id += 1
                files_editions_by_album_id[album_id] = edition_payload
                _append_artist_bucket(artist_name_prepared, album_id, classical_hint=classical_hint)
                partial_plan_buffer.add(int(album_id))
                if bool(prepared.get("fast_skip_marked")):
                    fast_skip_marked += 1
                if bool(prepared.get("fast_skip_full_cached")):
                    fast_skip_full_cached += 1
                _set_discovery_state(
                    scan_discovery_albums_found=next_album_id - 1,
                    scan_discovery_artists_found=len(artist_to_album_ids),
                )
                _emit_files_discovery_heartbeat(
                    "building album candidates",
                    files_found=len(audio_files),
                    folders_done=folders_done,
                    folders_total=folders_total,
                    artists_found=len(artist_to_album_ids),
                    albums_found=next_album_id - 1,
                )
                _sync_album_candidate_progress(current_folder_path=str(folder_for_io))
                _flush_partial_resume_files_plan()
                if storage_enabled:
                    _storage_album_candidate_mark_progress(folder)
    # Build artists_merged: (artist_id, artist_name, album_ids). For Files we use 0 as artist_id.
    artists_merged = [
        (0, str(artist_display_names.get(bucket_key) or bucket_key), album_ids)
        for bucket_key, album_ids in sorted(
            artist_to_album_ids.items(),
            key=lambda item: str(artist_display_names.get(item[0]) or item[0]).lower(),
        )
    ]
    total_albums = next_album_id - 1
    if storage_enabled:
        bucket_counts: dict[tuple[int, str], int] = defaultdict(int)
        split_rows: list[tuple[int, str, list[int]]] = []
        for _artist_id, artist_name, album_ids in artists_merged:
            by_bucket: dict[tuple[int, str], list[int]] = defaultdict(list)
            for aid in album_ids:
                fe = files_editions_by_album_id.get(int(aid)) or {}
                bucket_order = int(fe.get("storage_bucket_order") or 0)
                device_id = str(fe.get("storage_device_id") or "")
                by_bucket[(bucket_order, device_id)].append(int(aid))
                bucket_counts[(bucket_order, device_id)] += 1
            for _bucket_key, bucket_album_ids in sorted(by_bucket.items(), key=lambda item: (item[0][0], item[0][1], artist_name.lower())):
                split_rows.append((0, artist_name, bucket_album_ids))
        artists_merged = sorted(
            split_rows,
            key=lambda row: (
                int((files_editions_by_album_id.get(int(row[2][0])) or {}).get("storage_bucket_order") or 0) if row[2] else 0,
                str((files_editions_by_album_id.get(int(row[2][0])) or {}).get("storage_device_id") or "") if row[2] else "",
                str(row[1] or "").lower(),
            ),
        )
        plan_summary = _storage_plan_summary(storage_entries)
        for item in plan_summary:
            key = (int(item.get("storage_bucket_order") or 0), str(item.get("storage_device_id") or ""))
            item["albums_total"] = int(bucket_counts.get(key, 0))
        devices_total = len({str(item.get("storage_device_id") or "") for item in plan_summary if str(item.get("storage_device_id") or "")})
        with lock:
            state["storage_scan_plan"] = plan_summary
            state["storage_buckets_total"] = len(plan_summary)
            state["storage_devices_total"] = devices_total
            state["storage_active_devices"] = 1 if plan_summary else 0
            state["storage_estimated_watts_saved"] = _storage_estimated_watts_saved(1 if plan_summary else 0, devices_total)
        log_scan(
            "[STORAGE] Planned %d album(s) across %d disk bucket(s); active disks 1/%d, estimated avoided load %.1f W.",
            total_albums,
            len(plan_summary),
            devices_total,
            _storage_estimated_watts_saved(1 if plan_summary else 0, devices_total),
        )
    _flush_partial_resume_files_plan(force=True)
    _sync_album_candidate_progress(force=True)
    _set_discovery_state(
        scan_discovery_running=False,
        scan_discovery_stage="ready",
        scan_discovery_current_root=None,
        scan_discovery_folders_done=folders_total,
        scan_discovery_folders_total=folders_total,
        scan_discovery_albums_done=folders_total,
        scan_discovery_albums_total=folders_total,
        scan_discovery_albums_found=total_albums,
        scan_discovery_artists_found=len(artists_merged),
        scan_tracks_detected_total=len(audio_files),
    )
    _emit_files_discovery_heartbeat(
        "ready",
        files_found=len(audio_files),
        folders_done=folders_total,
        folders_total=folders_total,
        artists_found=len(artists_merged),
        albums_found=total_albums,
        force=True,
    )
    log_scan(
        "FILES backend: discovered %d artist(s), %d album(s) from %d audio file(s)%s%s%s%s",
        len(artists_merged),
        total_albums,
        len(audio_files),
        f"; changed-only skipped {skipped_unchanged_complete} unchanged+healthy album(s)" if scan_type == "changed_only" else "",
        f"; cache-first fast-skip {fast_skip_full_cached} album(s)" if fast_skip_full_cached else "",
        f"; fast-skip candidates {fast_skip_marked}" if fast_skip_marked else "",
        f"; dir-cache reused {dir_cache_skip_hits} subtree(s) / {dir_cache_skip_audio} audio file(s)" if dir_cache_skip_hits else "",
    )
    return artists_merged, total_albums, files_editions_by_album_id
