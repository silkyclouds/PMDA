"""Runtime-bound source root and host/container binding helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_run_path_verification',
    '_discover_bindings_by_content',
    '_discover_one_binding',
    '_cross_check_bindings',
    '_normalize_root_path',
    '_files_source_roots_fetch',
    '_effective_files_source_rows',
    '_effective_files_roots',
    '_effective_files_scan_roots',
    '_source_row_for_path',
    '_source_id_for_path',
    '_winner_source_row',
    '_ensure_files_source_roots_seeded',
    '_files_source_roots_replace',
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

def _path_has_audio_sample(path: Any, *, max_entries: int = 512) -> bool:
    try:
        root = Path(path)
        if not root.exists() or not root.is_dir():
            return False
        checked = 0
        for candidate in root.rglob("*"):
            checked += 1
            if checked > max_entries:
                return False
            try:
                if candidate.is_file() and AUDIO_RE.search(candidate.name):
                    return True
            except OSError:
                continue
    except Exception:
        return False
    return False


def _run_path_verification(path_map: dict, db_file: str, samples: int):
    """
    Compatibility files-mode verifier.

    PMDA no longer supports using Plex's database as a source of truth. This helper
    therefore validates only configured filesystem roots and never opens Plex DB files.
    """
    results = []
    for source_root, host_root in (path_map or {}).items():
        root = Path(host_root)
        exists = root.exists() and root.is_dir()
        has_audio = _path_has_audio_sample(root, max_entries=max(32, int(samples or 0) * 32))
        results.append(
            {
                "plex_root": source_root,
                "host_root": host_root,
                "status": "ok" if exists else "fail",
                "samples_checked": 0,
                "message": (
                    "Filesystem root exists"
                    if exists and not has_audio
                    else "Filesystem root exists with audio samples"
                    if exists
                    else "Filesystem root does not exist"
                ),
            }
        )
    return results

def _discover_bindings_by_content(path_map: dict, db_file: str, music_root: str, samples: int):
    """
    Compatibility stub for the removed Plex-source binding discovery.

    Files source roots are authoritative now; PMDA must not infer source bindings from
    Plex DB content. Return the existing map unchanged with an explicit skipped status.
    """
    if not path_map:
        return {}, []
    music_path = Path(music_root)
    if not music_path.exists() or not music_path.is_dir():
        return None
    results = [
        {
            "plex_root": source_root,
            "host_root": host_root,
            "status": "skipped",
            "samples_checked": 0,
            "message": "Plex DB source discovery is disabled; configured files source roots are authoritative.",
        }
        for source_root, host_root in path_map.items()
    ]
    return (dict(path_map), results)

def _discover_one_binding(plex_root: str, db_file: str, music_root: str, samples: int):
    """
    Compatibility stub for the removed single Plex-source binding discovery.
    """
    music_path = Path(music_root)
    if not music_path.exists() or not music_path.is_dir():
        return None
    return (None, {
        "plex_root": plex_root,
        "host_root": plex_root,
        "status": "skipped",
        "samples_checked": 0,
        "message": "Plex DB source discovery is disabled; configure files source roots instead.",
    })

def _cross_check_bindings(raise_on_abort: bool = True):
    """
    Legacy startup hook retained for compatibility.

    PATH_MAP/Plex-source cross checks are disabled. Files source roots are the
    authoritative input, and Plex may only be used as a post-pipeline player sync target.
    """
    logging.info("PATH_MAP cross-check skipped: Plex DB source mode is disabled; files source roots are authoritative.")

def _normalize_root_path(raw_path: str | Path | None) -> str:
    txt = str(raw_path or "").strip()
    if not txt:
        return ""
    try:
        p = path_for_fs_access(Path(txt))
    except Exception:
        p = Path(txt)
    try:
        normalized = str(p.resolve())
    except Exception:
        normalized = str(p)
    if normalized != "/":
        normalized = normalized.rstrip("/")
    return normalized or "/"

def _files_source_roots_fetch(*, enabled_only: bool = False) -> list[dict]:
    rows: list[dict] = []
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=15)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        query = """
            SELECT source_id, path, role, enabled, priority, is_winner_root, created_at, updated_at
            FROM files_source_roots
        """
        params: list[Any] = []
        if enabled_only:
            query += " WHERE enabled = 1"
        query += " ORDER BY priority ASC, source_id ASC"
        cur.execute(query, params)
        for row in cur.fetchall():
            rows.append(
                {
                    "source_id": int(row["source_id"] or 0),
                    "path": str(row["path"] or ""),
                    "role": str(row["role"] or "library"),
                    "enabled": bool(row["enabled"]),
                    "priority": int(row["priority"] or 100),
                    "is_winner_root": bool(row["is_winner_root"]),
                    "created_at": float(row["created_at"] or 0.0),
                    "updated_at": float(row["updated_at"] or 0.0),
                }
            )
        con.close()
    except Exception:
        logging.debug("Failed to fetch files_source_roots", exc_info=True)
    return rows

def _effective_files_source_rows(*, enabled_only: bool = True) -> list[dict]:
    rows = _files_source_roots_fetch(enabled_only=enabled_only)
    if rows:
        return rows
    # Legacy fallback: derive rows from FILES_ROOTS until sources are configured.
    out: list[dict] = []
    for idx, path in enumerate((FILES_ROOTS or []), start=1):
        normalized = _normalize_root_path(path)
        if not normalized:
            continue
        out.append(
            {
                "source_id": idx,
                "path": normalized,
                "role": "library",
                "enabled": True,
                "priority": idx * 10,
                "is_winner_root": idx == 1,
                "created_at": 0.0,
                "updated_at": 0.0,
            }
        )
    return out

def _effective_files_roots(*, enabled_only: bool = True) -> list[str]:
    rows = _effective_files_source_rows(enabled_only=enabled_only)
    seen: set[str] = set()
    out: list[str] = []
    for row in rows:
        path = _normalize_root_path(row.get("path"))
        if not path or path in seen:
            continue
        seen.add(path)
        out.append(path)
    return out

def _effective_files_scan_roots(*, enabled_only: bool = True) -> list[str]:
    """
    Return the roots that are allowed to enter the identification/export pipeline.

    This is intentionally narrower than `_effective_files_roots()`: clean library
    roots such as Music_matched must stay visible and usable for duplicate/conflict
    analysis, but should not be re-identified like raw intake when the workflow has
    explicit incoming/intake roots.
    """
    try:
        workflow = _library_workflow_state()
        scan_roots = _normalize_root_path_list(workflow.get("scan_roots"))
        if scan_roots:
            return scan_roots
    except Exception:
        logging.debug("Failed to resolve workflow scan roots; falling back to enabled file roots", exc_info=True)
    return _effective_files_roots(enabled_only=enabled_only)

def _source_row_for_path(path_like: str | Path | None, *, enabled_only: bool = True) -> Optional[dict]:
    path_norm = _normalize_root_path(path_like)
    if not path_norm:
        return None
    best: Optional[dict] = None
    best_len = -1
    for row in _effective_files_source_rows(enabled_only=enabled_only):
        root = _normalize_root_path(row.get("path"))
        if not root:
            continue
        if path_norm == root or path_norm.startswith(root.rstrip("/") + "/"):
            root_len = len(root)
            if root_len > best_len:
                best = row
                best_len = root_len
    return best

def _source_id_for_path(path_like: str | Path | None, *, enabled_only: bool = True) -> Optional[int]:
    row = _source_row_for_path(path_like, enabled_only=enabled_only)
    if not row:
        return None
    try:
        source_id = int(row.get("source_id") or 0)
    except Exception:
        source_id = 0
    return source_id if source_id > 0 else None

def _winner_source_row() -> Optional[dict]:
    rows = _effective_files_source_rows(enabled_only=True)
    if not rows:
        return None
    explicit = [r for r in rows if bool(r.get("is_winner_root"))]
    if explicit:
        return explicit[0]
    setting_raw = str(_get_config_from_db("WINNER_SOURCE_ROOT_ID") or "").strip()
    if setting_raw:
        try:
            wanted = int(setting_raw)
        except Exception:
            wanted = 0
        for row in rows:
            try:
                if int(row.get("source_id") or 0) == wanted:
                    return row
            except Exception:
                continue
    library_rows = [r for r in rows if str(r.get("role") or "library") == "library"]
    return library_rows[0] if library_rows else rows[0]

def _ensure_files_source_roots_seeded() -> None:
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM files_source_roots")
        existing = int((cur.fetchone() or [0])[0] or 0)
        if existing > 0:
            con.close()
            return
        now = time.time()
        roots = _parse_files_roots(_get_config_from_db("FILES_ROOTS", ",".join(FILES_ROOTS or [])))
        rows = []
        for idx, root in enumerate(roots, start=1):
            normalized = _normalize_root_path(root)
            if not normalized:
                continue
            rows.append((normalized, "library", 1, idx * 10, 1 if idx == 1 else 0, now, now))
        if rows:
            cur.executemany(
                """
                INSERT INTO files_source_roots
                (path, role, enabled, priority, is_winner_root, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            con.commit()
            # Mirror first winner source into settings for backward-compatible UI config payload.
            first_id = None
            try:
                cur.execute(
                    "SELECT source_id FROM files_source_roots WHERE is_winner_root = 1 ORDER BY priority ASC, source_id ASC LIMIT 1"
                )
                row = cur.fetchone()
                if row:
                    first_id = int(row[0] or 0)
            except Exception:
                first_id = None
            if first_id:
                try:
                    init_settings_db()
                    con_cfg = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
                    con_cfg.execute(
                        "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
                        ("WINNER_SOURCE_ROOT_ID", str(first_id)),
                    )
                    con_cfg.commit()
                    con_cfg.close()
                except Exception:
                    logging.debug("Failed to persist WINNER_SOURCE_ROOT_ID during source seed", exc_info=True)
        con.close()
    except Exception:
        logging.debug("Failed to seed files_source_roots", exc_info=True)

def _files_source_roots_replace(roots_payload: list[dict], *, winner_source_root_id: int | None = None) -> list[dict]:
    rows_clean: list[dict] = []
    seen_paths: set[str] = set()
    now = time.time()
    existing_rows = _files_source_roots_fetch(enabled_only=False)
    existing_by_id: dict[int, dict] = {}
    existing_id_by_path: dict[str, int] = {}
    for row in existing_rows:
        try:
            sid = int(row.get("source_id") or 0)
        except Exception:
            sid = 0
        if sid <= 0:
            continue
        normalized_path = _normalize_root_path(row.get("path"))
        existing_by_id[sid] = row
        if normalized_path:
            existing_id_by_path[normalized_path] = sid
    previous_winner_row = next((r for r in existing_rows if bool(r.get("is_winner_root"))), None)
    previous_winner_path = _normalize_root_path(previous_winner_row.get("path")) if previous_winner_row else ""

    for idx, raw in enumerate(roots_payload or [], start=1):
        if not isinstance(raw, dict):
            continue
        path = _normalize_root_path(raw.get("path"))
        if not path or path in seen_paths:
            continue
        seen_paths.add(path)
        role = str(raw.get("role") or "library").strip().lower()
        if role not in {"library", "incoming"}:
            role = "library"
        enabled = 1 if _parse_bool(raw.get("enabled", True)) else 0
        try:
            priority = int(raw.get("priority") or (idx * 10))
        except Exception:
            priority = idx * 10
        priority = max(1, min(10000, priority))
        payload_source_id = 0
        try:
            payload_source_id = int(raw.get("source_id") or 0)
        except Exception:
            payload_source_id = 0
        source_id = 0
        if payload_source_id > 0 and payload_source_id in existing_by_id:
            source_id = payload_source_id
        elif path in existing_id_by_path:
            source_id = int(existing_id_by_path[path] or 0)
        rows_clean.append(
            {
                "source_id": source_id if source_id > 0 else None,
                "path": path,
                "role": role,
                "enabled": enabled,
                "priority": priority,
                "is_winner_root": 0,
                "payload_winner": bool(_parse_bool(raw.get("is_winner_root", False))),
                "created_at": now,
                "updated_at": now,
            }
        )
    if not rows_clean:
        raise ValueError("At least one valid source root is required.")

    library_rows = [r for r in rows_clean if str(r.get("role") or "") == "library"]
    incoming_rows = [r for r in rows_clean if str(r.get("role") or "") == "incoming"]
    if incoming_rows and not library_rows:
        raise ValueError("At least one standard source folder is required when incoming folders are configured.")

    winner_path = ""
    if winner_source_root_id is not None:
        try:
            wanted = int(winner_source_root_id or 0)
        except Exception:
            wanted = 0
        if wanted > 0:
            if wanted in existing_by_id:
                winner_path = _normalize_root_path(existing_by_id[wanted].get("path"))
            else:
                for row in rows_clean:
                    if int(row.get("source_id") or 0) == wanted:
                        winner_path = _normalize_root_path(row.get("path"))
                        break
    if not winner_path:
        for row in rows_clean:
            if bool(row.get("payload_winner")):
                winner_path = _normalize_root_path(row.get("path"))
                break
    if not winner_path and previous_winner_path:
        if any(_normalize_root_path(r.get("path")) == previous_winner_path for r in rows_clean):
            winner_path = previous_winner_path
    if winner_path:
        winner_row = next((r for r in rows_clean if _normalize_root_path(r.get("path")) == winner_path), None)
        if winner_row and str(winner_row.get("role") or "") == "incoming":
            winner_path = ""
    if not winner_path:
        library_first = next((r for r in rows_clean if str(r.get("role") or "") == "library"), rows_clean[0])
        winner_path = _normalize_root_path(library_first.get("path"))
    for row in rows_clean:
        row["is_winner_root"] = 1 if _normalize_root_path(row.get("path")) == winner_path else 0

    con = sqlite3.connect(str(STATE_DB_FILE), timeout=20)
    cur = con.cursor()
    persisted_ids: list[int] = []
    try:
        for row in rows_clean:
            sid = int(row.get("source_id") or 0)
            if sid > 0 and sid in existing_by_id:
                cur.execute(
                    """
                    UPDATE files_source_roots
                    SET path = ?, role = ?, enabled = ?, priority = ?, is_winner_root = ?, updated_at = ?
                    WHERE source_id = ?
                    """,
                    (
                        row["path"],
                        row["role"],
                        int(row["enabled"]),
                        int(row["priority"]),
                        int(row["is_winner_root"]),
                        float(row["updated_at"]),
                        sid,
                    ),
                )
                persisted_ids.append(sid)
            else:
                cur.execute(
                    """
                    INSERT INTO files_source_roots
                    (path, role, enabled, priority, is_winner_root, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["path"],
                        row["role"],
                        int(row["enabled"]),
                        int(row["priority"]),
                        int(row["is_winner_root"]),
                        float(row["created_at"]),
                        float(row["updated_at"]),
                    ),
                )
                persisted_ids.append(int(cur.lastrowid or 0))
        if persisted_ids:
            placeholders = ",".join("?" for _ in persisted_ids)
            cur.execute(
                f"DELETE FROM files_source_roots WHERE source_id NOT IN ({placeholders})",
                tuple(persisted_ids),
            )
        else:
            cur.execute("DELETE FROM files_source_roots")
        con.commit()
    finally:
        con.close()

    saved_rows = _files_source_roots_fetch(enabled_only=False)
    winner_saved = next((r for r in saved_rows if bool(r.get("is_winner_root"))), saved_rows[0] if saved_rows else None)
    # Mirror effective scan roots and winner root in settings.db for backward-compatible config UI.
    try:
        effective_roots = [str(r.get("path") or "") for r in saved_rows if bool(r.get("enabled")) and str(r.get("path") or "").strip()]
        init_settings_db()
        con_cfg = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
        con_cfg.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
            ("FILES_ROOTS", ",".join(effective_roots)),
        )
        if winner_saved:
            con_cfg.execute(
                "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
                ("WINNER_SOURCE_ROOT_ID", str(int(winner_saved.get("source_id") or 0))),
            )
        con_cfg.commit()
        con_cfg.close()
    except Exception:
        logging.debug("Failed to mirror files source roots into settings.db", exc_info=True)

    # Hot reload runtime paths/watcher.
    _reload_library_mode_and_files_roots_from_db()
    _request_files_watcher_reconcile("files_sources_updated")
    return saved_rows

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}

def _run_path_verification_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_path_verification(*args, **kwargs)

def _discover_bindings_by_content_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _discover_bindings_by_content(*args, **kwargs)

def _discover_one_binding_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _discover_one_binding(*args, **kwargs)

def _cross_check_bindings_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _cross_check_bindings(*args, **kwargs)

def _normalize_root_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _normalize_root_path(*args, **kwargs)

def _files_source_roots_fetch_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_source_roots_fetch(*args, **kwargs)

def _effective_files_source_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _effective_files_source_rows(*args, **kwargs)

def _effective_files_roots_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _effective_files_roots(*args, **kwargs)

def _effective_files_scan_roots_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _effective_files_scan_roots(*args, **kwargs)

def _source_row_for_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _source_row_for_path(*args, **kwargs)

def _source_id_for_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _source_id_for_path(*args, **kwargs)

def _winner_source_row_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _winner_source_row(*args, **kwargs)

def _ensure_files_source_roots_seeded_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ensure_files_source_roots_seeded(*args, **kwargs)

def _files_source_roots_replace_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_source_roots_replace(*args, **kwargs)
