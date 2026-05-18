"""Admin operations snapshots and backup bundle helpers."""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

_EXTRACTED_NAMES = {
    "_scan_dir_usage",
    "_ops_storage_target_snapshot",
    "_ops_backups_root_dir",
    "_ops_backup_dir_size_bytes",
    "_ops_list_backups",
    "_ops_snapshot_payload",
    "_ops_backup_sqlite_db",
    "_ops_backup_pg_dump",
    "_ops_create_backup_bundle",
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


def _scan_dir_usage(root: Path, max_files: int = 400000) -> dict:
    if not root.exists():
        return {
            "exists": False,
            "bytes_total": 0,
            "file_count": 0,
            "dir_count": 0,
            "walk_truncated": False,
            "walk_errors": 0,
        }
    total_bytes = 0
    file_count = 0
    dir_count = 0
    walk_errors = 0
    walk_truncated = False
    stack = [str(root)]
    max_files = max(10000, int(max_files or 400000))
    while stack:
        cur = stack.pop()
        try:
            with os.scandir(cur) as it:
                for entry in it:
                    try:
                        if entry.is_file(follow_symlinks=False):
                            file_count += 1
                            try:
                                total_bytes += int(entry.stat(follow_symlinks=False).st_size)
                            except Exception:
                                walk_errors += 1
                            if file_count >= max_files:
                                walk_truncated = True
                                break
                        elif entry.is_dir(follow_symlinks=False):
                            dir_count += 1
                            stack.append(entry.path)
                    except Exception:
                        walk_errors += 1
        except Exception:
            walk_errors += 1
        if walk_truncated:
            break
    return {
        "exists": True,
        "bytes_total": int(total_bytes),
        "file_count": int(file_count),
        "dir_count": int(dir_count),
        "walk_truncated": bool(walk_truncated),
        "walk_errors": int(walk_errors),
    }


def _ops_storage_target_snapshot(path_like: str | Path | None) -> dict[str, Any]:
    raw = str(path_like or "").strip()
    target = Path(raw) if raw else CONFIG_DIR
    probe = target
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    try:
        usage = shutil.disk_usage(str(probe))
        total_bytes = int(usage.total or 0)
        free_bytes = int(usage.free or 0)
        used_bytes = max(0, total_bytes - free_bytes)
    except Exception:
        total_bytes = 0
        free_bytes = 0
        used_bytes = 0
    return {
        "path": str(target),
        "probe_path": str(probe),
        "exists": bool(target.exists()),
        "total_bytes": total_bytes,
        "used_bytes": used_bytes,
        "free_bytes": free_bytes,
    }


def _ops_backups_root_dir() -> Path:
    root = CONFIG_DIR / "backups"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _ops_backup_dir_size_bytes(root: Path) -> int:
    if not root.exists() or not root.is_dir():
        return 0
    total = 0
    for base, _dirs, files in os.walk(root):
        for name in files:
            try:
                total += int((Path(base) / name).stat().st_size or 0)
            except Exception:
                continue
    return int(total)


def _ops_list_backups(limit: int = 8) -> list[dict[str, Any]]:
    root = _ops_backups_root_dir()
    out: list[dict[str, Any]] = []
    try:
        candidates = [p for p in root.iterdir() if p.is_dir() and p.name.startswith("pmda-backup-")]
    except Exception:
        candidates = []
    candidates.sort(key=lambda p: float(p.stat().st_mtime if p.exists() else 0.0), reverse=True)
    for backup_dir in candidates[: max(1, int(limit or 8))]:
        manifest_path = backup_dir / "manifest.json"
        manifest: dict[str, Any] = {}
        try:
            if manifest_path.exists():
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            manifest = {}
        pg_dump_meta = manifest.get("pg_dump") if isinstance(manifest, dict) else {}
        if not isinstance(pg_dump_meta, dict):
            pg_dump_meta = {}
        out.append(
            {
                "name": str(backup_dir.name),
                "path": str(backup_dir),
                "created_at": _safe_int((manifest.get("created_at") if isinstance(manifest, dict) else 0), 0),
                "size_bytes": _ops_backup_dir_size_bytes(backup_dir),
                "status": str((manifest.get("status") if isinstance(manifest, dict) else "") or "unknown"),
                "pg_dump_included": bool(pg_dump_meta.get("attempted")),
                "pg_dump_ok": bool(pg_dump_meta.get("ok")),
            }
        )
    return out


def _ops_snapshot_payload() -> dict[str, Any]:
    bootstrap = _pipeline_bootstrap_status()
    workflow_state = _library_workflow_state()
    with lock:
        scan_running = bool(state.get("scanning"))
    settings_metrics = _read_settings_db_metrics()
    state_metrics = _read_state_cache_metrics()
    cache_metrics = _read_sqlite_cache_metrics()
    pg_metrics = _read_pg_cache_metrics()
    redis_metrics = _read_redis_cache_metrics()
    media_cache_metrics = _read_media_cache_usage(max_files=PMDA_OPS_SNAPSHOT_MEDIA_CACHE_MAX_WALK_FILES)
    storage = {
        "config": _ops_storage_target_snapshot(CONFIG_DIR),
        "music": _ops_storage_target_snapshot("/music"),
        "dupes": _ops_storage_target_snapshot(DUPE_ROOT),
        "pgdata": _ops_storage_target_snapshot(PMDA_PGDATA),
    }
    try:
        managed = _managed_runtime_status_snapshot(skip_candidates=True)
    except Exception:
        managed = None
    bundle_summary: dict[str, Any] = {}
    if isinstance(managed, dict):
        for bundle_type, row in dict(managed.get("bundles") or {}).items():
            row_dict = dict(row or {})
            health = dict(row_dict.get("health") or {})
            bundle_summary[str(bundle_type)] = {
                "state": str(row_dict.get("state") or ""),
                "phase": str(row_dict.get("phase") or ""),
                "available": bool(health.get("available")),
                "effective_url": str(row_dict.get("effective_url") or ""),
            }
    return {
        "generated_at": int(time.time()),
        "library_mode": _get_library_mode(),
        "auth_bootstrap_required": bool(_auth_bootstrap_required()),
        "pipeline_bootstrap_required": bool(bootstrap.get("bootstrap_required")),
        "first_full_scan_id": _safe_int(bootstrap.get("first_full_scan_id"), 0),
        "first_full_completed_at": bootstrap.get("first_full_completed_at"),
        "scan_running": scan_running,
        "files_roots_configured": bool(FILES_ROOTS),
        "config_dir": str(CONFIG_DIR),
        "backup_root": str(_ops_backups_root_dir()),
        "workflow_mode": str(workflow_state.get("mode") or ""),
        "storage": storage,
        "sqlite": {
            "settings_db": settings_metrics,
            "state_db": state_metrics,
            "cache_db": cache_metrics,
        },
        "postgres": pg_metrics,
        "redis": redis_metrics,
        "media_cache": media_cache_metrics,
        "managed_runtime": {
            "available": bool(managed),
            "bundles": bundle_summary,
        },
        "backups": _ops_list_backups(limit=8),
    }


def _ops_backup_sqlite_db(src: Path, dst: Path) -> dict[str, Any]:
    result = {
        "ok": False,
        "path": str(dst),
        "size_bytes": 0,
        "error": "",
    }
    if not src.exists():
        result["error"] = "source_missing"
        return result
    dst.parent.mkdir(parents=True, exist_ok=True)
    src_con = None
    dst_con = None
    try:
        src_con = sqlite3.connect(f"file:{src}?mode=ro", uri=True, timeout=20)
        dst_con = sqlite3.connect(str(dst), timeout=20)
        src_con.backup(dst_con)
        result["ok"] = True
        result["size_bytes"] = _path_size_bytes(dst)
        return result
    except Exception as exc:
        result["error"] = str(exc)
        return result
    finally:
        try:
            if dst_con is not None:
                dst_con.close()
        except Exception:
            pass
        try:
            if src_con is not None:
                src_con.close()
        except Exception:
            pass


def _ops_backup_pg_dump(target_file: Path) -> dict[str, Any]:
    result = {
        "requested": True,
        "attempted": False,
        "ok": False,
        "path": str(target_file),
        "error": "",
    }
    if _get_library_mode() != "files":
        result["error"] = "library_mode_not_files"
        return result
    if not _files_pg_init_schema():
        result["error"] = str(_FILES_PG_LAST_ERROR or "pg_init_schema_failed")
        return result
    target_file.parent.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["PGPASSWORD"] = str(PMDA_PG_PASSWORD or "")
    cmd = [
        "pg_dump",
        "-h",
        str(PMDA_PG_HOST),
        "-p",
        str(PMDA_PG_PORT),
        "-U",
        str(PMDA_PG_USER),
        "-d",
        str(PMDA_PG_DB),
        "-Fc",
        "-f",
        str(target_file),
    ]
    try:
        result["attempted"] = True
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=600)
        if proc.returncode != 0:
            result["error"] = str((proc.stderr or proc.stdout or "pg_dump failed").strip())
            return result
        result["ok"] = True
        return result
    except Exception as exc:
        result["attempted"] = True
        result["error"] = str(exc)
        return result


def _ops_create_backup_bundle(*, include_pg_dump: bool = True) -> dict[str, Any]:
    created_at = int(time.time())
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_dir = _ops_backups_root_dir() / f"pmda-backup-{stamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    snapshot = _ops_snapshot_payload()
    snapshot_path = backup_dir / "ops-snapshot.json"
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")

    sqlite_results = {
        "settings_db": _ops_backup_sqlite_db(SETTINGS_DB_FILE, backup_dir / "settings.db"),
        "state_db": _ops_backup_sqlite_db(STATE_DB_FILE, backup_dir / "state.db"),
        "cache_db": _ops_backup_sqlite_db(CACHE_DB_FILE, backup_dir / "cache.db"),
    }
    pg_dump_result = {
        "requested": bool(include_pg_dump),
        "attempted": False,
        "ok": False,
        "path": "",
        "error": "",
    }
    if include_pg_dump:
        pg_dump_result = _ops_backup_pg_dump(backup_dir / "files-postgres.dump")

    sqlite_ok = all(bool((row or {}).get("ok")) for row in sqlite_results.values())
    overall_ok = bool(sqlite_ok and (pg_dump_result.get("ok") if include_pg_dump else True))
    status = "ok" if overall_ok else ("partial" if sqlite_ok else "error")
    manifest = {
        "created_at": created_at,
        "status": status,
        "library_mode": _get_library_mode(),
        "auth_bootstrap_required": bool(snapshot.get("auth_bootstrap_required")),
        "pipeline_bootstrap_required": bool(snapshot.get("pipeline_bootstrap_required")),
        "sqlite": sqlite_results,
        "pg_dump": pg_dump_result,
    }
    manifest_path = backup_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "ok": bool(overall_ok),
        "status": status,
        "message": "PMDA backup created successfully." if overall_ok else "PMDA backup created with warnings.",
        "backup_path": str(backup_dir),
        "manifest_path": str(manifest_path),
        "snapshot_path": str(snapshot_path),
        "sqlite": sqlite_results,
        "pg_dump": pg_dump_result,
        "backups": _ops_list_backups(limit=8),
    }


_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES if name in globals()}


def _scan_dir_usage_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_dir_usage(*args, **kwargs)


def _ops_storage_target_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ops_storage_target_snapshot(*args, **kwargs)


def _ops_backups_root_dir_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ops_backups_root_dir(*args, **kwargs)


def _ops_backup_dir_size_bytes_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ops_backup_dir_size_bytes(*args, **kwargs)


def _ops_list_backups_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ops_list_backups(*args, **kwargs)


def _ops_snapshot_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ops_snapshot_payload(*args, **kwargs)


def _ops_backup_sqlite_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ops_backup_sqlite_db(*args, **kwargs)


def _ops_backup_pg_dump_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ops_backup_pg_dump(*args, **kwargs)


def _ops_create_backup_bundle_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ops_create_backup_bundle(*args, **kwargs)
