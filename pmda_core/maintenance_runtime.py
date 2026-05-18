"""Administrative maintenance side effects extracted from the PMDA bootstrap."""

from __future__ import annotations

from pathlib import Path
from typing import Any

_RUNTIME: Any | None = None

_EXTRACTED_NAMES = {
    "_maintenance_clear_artwork_ram_cache",
    "_maintenance_clear_media_cache",
    "_maintenance_clear_export_root",
    "_maintenance_clear_files_index",
}


def _bind_runtime(runtime: Any) -> None:
    """Bind live PMDA globals for one maintenance call."""
    global _RUNTIME
    _RUNTIME = runtime
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


def _maintenance_clear_artwork_ram_cache() -> int:
    global _ARTWORK_RAM_CACHE_BYTES
    with _ARTWORK_RAM_CACHE_LOCK:
        count = int(len(_ARTWORK_RAM_CACHE))
        _ARTWORK_RAM_CACHE.clear()
        _ARTWORK_RAM_CACHE_BYTES = 0
    return count


def _maintenance_clear_media_cache() -> dict[str, Any]:
    root = _media_cache_root_dir()
    summary: dict[str, Any] = {
        "root": str(root),
        "album_files_removed": 0,
        "album_bytes_removed": 0,
        "artist_files_removed": 0,
        "artist_bytes_removed": 0,
        "ram_entries_cleared": 0,
        "errors": [],
        "ok": True,
    }

    root.mkdir(parents=True, exist_ok=True)
    for kind in ("album", "artist"):
        target = root / kind
        files_removed = 0
        bytes_removed = 0
        try:
            if target.exists():
                for fp in target.rglob("*"):
                    if fp.is_file():
                        files_removed += 1
                        try:
                            bytes_removed += int(fp.stat().st_size or 0)
                        except Exception:
                            pass
                shutil.rmtree(target)
            target.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            summary["ok"] = False
            summary["errors"].append(f"{target}: {exc}")
        summary[f"{kind}_files_removed"] = files_removed
        summary[f"{kind}_bytes_removed"] = bytes_removed

    summary["ram_entries_cleared"] = _maintenance_clear_artwork_ram_cache()
    return summary


def _maintenance_clear_export_root() -> dict[str, Any]:
    root = Path(str(EXPORT_ROOT or "").strip())
    summary: dict[str, Any] = {
        "root": str(root),
        "configured": bool(str(root)),
        "files_removed": 0,
        "dirs_removed": 0,
        "bytes_removed": 0,
        "ok": True,
        "error": "",
    }
    if not str(root):
        return summary
    try:
        root.mkdir(parents=True, exist_ok=True)
        for child in list(root.iterdir()):
            try:
                if child.is_file() or child.is_symlink():
                    try:
                        summary["bytes_removed"] += int(child.stat().st_size or 0)
                    except Exception:
                        pass
                    child.unlink()
                    summary["files_removed"] += 1
                    continue
                if child.is_dir():
                    for fp in child.rglob("*"):
                        if fp.is_file():
                            summary["files_removed"] += 1
                            try:
                                summary["bytes_removed"] += int(fp.stat().st_size or 0)
                            except Exception:
                                pass
                        elif fp.is_dir():
                            summary["dirs_removed"] += 1
                    shutil.rmtree(child)
                    summary["dirs_removed"] += 1
            except Exception as child_exc:
                summary["ok"] = False
                summary["error"] = str(child_exc)
                break
        return summary
    except Exception as exc:
        summary["ok"] = False
        summary["error"] = str(exc)
        return summary


def _maintenance_clear_files_index() -> dict[str, Any]:
    out: dict[str, Any] = {
        "published_rows_deleted": 0,
        "pg_truncated": False,
        "pg_cleared": False,
        "ram_entries_cleared": 0,
        "export_root_cleared": False,
        "export_root_summary": {},
        "ok": True,
        "error": "",
    }
    out["published_rows_deleted"] = int(_clear_files_library_published_rows() or 0)
    if not _files_pg_init_schema():
        out["ok"] = False
        out["error"] = "PostgreSQL schema unavailable"
        return out

    acquired = files_index_lock.acquire(timeout=15)
    if not acquired:
        out["ok"] = False
        out["error"] = "Files index lock busy"
        return out

    try:
        conn = _files_pg_connect()
        if conn is None:
            out["ok"] = False
            out["error"] = "PostgreSQL unavailable"
            return out
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM files_match_audit")
                    cur.execute("DELETE FROM files_artist_concerts")
                    cur.execute("DELETE FROM files_external_artist_images")
                    cur.execute("DELETE FROM files_album_profiles")
                    cur.execute("DELETE FROM files_artist_profiles")
                    _files_reset_rebuild_tables(cur)
                    cur.execute("DELETE FROM files_index_meta")
                    cur.execute("DELETE FROM files_reco_sessions")
                    _files_index_write_meta(cur, "artists", "0")
                    _files_index_write_meta(cur, "albums", "0")
                    _files_index_write_meta(cur, "tracks", "0")
                    _files_index_write_meta(cur, "last_reason", "maintenance_reset")
                    _files_index_write_meta(cur, "last_build_ts", str(int(time.time())))
        finally:
            conn.close()
        _files_index_set_state(
            running=False,
            started_at=None,
            finished_at=time.time(),
            phase="idle",
            current_folder=None,
            folders_processed=0,
            total_folders=0,
            artists=0,
            albums=0,
            tracks=0,
            error=None,
        )
        out["pg_cleared"] = True
        out["ram_entries_cleared"] = _maintenance_clear_artwork_ram_cache()
        export_summary = _maintenance_clear_export_root()
        out["export_root_summary"] = export_summary
        out["export_root_cleared"] = bool(export_summary.get("ok", False))
        if not bool(export_summary.get("ok", False)):
            out["ok"] = False
            out["error"] = str(export_summary.get("error") or "Export root reset failed")
        _pipeline_bootstrap_reset()
        return out
    except Exception as exc:
        out["ok"] = False
        out["error"] = str(exc)
        return out
    finally:
        files_index_lock.release()


_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}


def _maintenance_clear_artwork_ram_cache_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _maintenance_clear_artwork_ram_cache(*args, **kwargs)


def _maintenance_clear_media_cache_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _maintenance_clear_media_cache(*args, **kwargs)


def _maintenance_clear_export_root_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _maintenance_clear_export_root(*args, **kwargs)


def _maintenance_clear_files_index_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _maintenance_clear_files_index(*args, **kwargs)
