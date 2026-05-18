"""Disk-aware storage bucket mapping helpers."""

from __future__ import annotations

from collections import OrderedDict
import copy
from pathlib import Path
import re
from typing import Any


def disk_sort_key(path: Path | str) -> tuple[int, str]:
    """Sort Unraid disk paths by numeric suffix, then name."""

    name = Path(path).name
    match = re.search(r"(\d+)$", name)
    return (int(match.group(1)) if match else 10**9, name.lower())


def relative_to(path: Path, root: Path) -> Path | None:
    """Return ``path`` relative to ``root`` while tolerating trailing slashes."""

    try:
        return path.relative_to(root)
    except Exception:
        try:
            return Path(str(path).rstrip("/")).relative_to(Path(str(root).rstrip("/")))
        except Exception:
            return None


def estimated_watts_saved(active_devices: int, total_devices: int) -> float:
    """Return a conservative 7W-per-avoided-HDD estimate for UI guidance."""

    try:
        avoided = max(0, int(total_devices) - max(0, int(active_devices)))
    except Exception:
        avoided = 0
    return round(float(avoided) * 7.0, 1)


def estimated_cost_saved_eur(watts_saved: float, seconds: float) -> float:
    """Estimate EUR saved using PMDA's default 0.26 EUR/kWh assumption."""

    try:
        watt_hours = max(0.0, float(watts_saved or 0.0)) * max(0.0, float(seconds or 0.0)) / 3600.0
    except Exception:
        return 0.0
    return round((watt_hours / 1000.0) * 0.26, 4)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return int(default)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except Exception:
        return float(default)


def build_storage_progress_payload(
    state_snapshot: dict[str, Any] | None,
    *,
    provider_default: str = "unraid",
    include_details: bool = True,
    history_limit: int = 100,
) -> dict[str, Any]:
    """Build the scan-progress storage payload from a runtime state snapshot."""
    st = dict(state_snapshot or {})
    payload = {
        "storage_power_saver_enabled": bool(st.get("storage_power_saver_enabled")),
        "storage_provider": str(st.get("storage_provider") or provider_default or "unraid"),
        "storage_active_devices": _to_int(st.get("storage_active_devices")),
        "storage_devices_total": _to_int(st.get("storage_devices_total")),
        "storage_current_device_id": st.get("storage_current_device_id"),
        "storage_current_device_label": st.get("storage_current_device_label"),
        "storage_bucket_done": _to_int(st.get("storage_bucket_done")),
        "storage_bucket_total": _to_int(st.get("storage_bucket_total")),
        "storage_buckets_done": _to_int(st.get("storage_buckets_done")),
        "storage_buckets_total": _to_int(st.get("storage_buckets_total")),
        "storage_estimated_watts_saved": _to_float(st.get("storage_estimated_watts_saved")),
        "storage_validation_error": str(st.get("storage_validation_error") or ""),
    }
    if not include_details:
        payload.update(
            {
                "storage_current_bucket": {},
                "storage_scan_plan": [],
                "storage_bucket_history": [],
            }
        )
        return payload
    history = copy.deepcopy(st.get("storage_bucket_history") or [])
    payload.update(
        {
            "storage_current_bucket": copy.deepcopy(st.get("storage_current_bucket") or {}),
            "storage_scan_plan": copy.deepcopy(st.get("storage_scan_plan") or []),
            "storage_bucket_history": history[-max(0, int(history_limit or 0)) :],
        }
    )
    return payload


def build_unraid_scan_roots(
    active_roots: list[str] | tuple[str, ...] | None,
    *,
    settings: dict[str, Any],
) -> tuple[list[Path], list[dict[str, Any]]]:
    """Build direct disk access roots and metadata for Unraid power-saver scans."""

    if not bool(settings.get("enabled")):
        return ([Path(root) for root in (active_roots or []) if root], [])

    host_root = Path(settings.get("host_mnt_root") or "/host_mnt")
    user_share_host_root = Path(settings.get("user_share_host_root") or "/host_mnt/user/MURRAY/Music")
    container_root = Path(settings.get("container_share_root") or "/music")
    if not host_root.exists():
        raise RuntimeError(
            f"Storage power saver is enabled, but {host_root} is not mounted. "
            "Mount host /mnt read-only to container /host_mnt or disable the mode."
        )
    disk_dirs = sorted([path for path in host_root.glob("disk*") if path.is_dir()], key=disk_sort_key)
    if not disk_dirs:
        raise RuntimeError(
            f"Storage power saver is enabled, but no {host_root}/disk* directories are visible. "
            "Check the Unraid /mnt -> /host_mnt read-only bind mount."
        )
    user_rel = relative_to(user_share_host_root, host_root / "user")
    if user_rel is None:
        raise RuntimeError(
            f"Invalid Unraid mapping: UNRAID_USER_SHARE_HOST_ROOT={user_share_host_root} "
            f"must live under {host_root / 'user'}."
        )

    roots: list[Path] = []
    entries: list[dict[str, Any]] = []
    missing: list[str] = []
    for canonical_raw in active_roots or []:
        canonical_root = Path(str(canonical_raw).rstrip("/") or "/")
        rel = relative_to(canonical_root, container_root)
        if rel is None:
            raise RuntimeError(
                f"Storage power saver cannot map configured root {canonical_root}: "
                f"it must live under UNRAID_CONTAINER_SHARE_ROOT={container_root}."
            )
        found_for_root = False
        for disk_dir in disk_dirs:
            access_root = disk_dir / user_rel / rel
            if not access_root.exists():
                continue
            found_for_root = True
            bucket_order = len(entries)
            entry = {
                "storage_provider": "unraid",
                "storage_device_id": disk_dir.name,
                "storage_device_label": disk_dir.name,
                "storage_bucket_order": bucket_order,
                "canonical_root": str(canonical_root),
                "access_root": str(access_root),
                "storage_rel_root": str(rel),
            }
            roots.append(access_root)
            entries.append(entry)
        if not found_for_root:
            missing.append(str(canonical_root))
    if not entries:
        details = ", ".join(missing[:4]) if missing else ", ".join(str(root) for root in active_roots or [])
        raise RuntimeError(
            "Storage power saver found Unraid disks, but none contained the configured music roots"
            f" ({details}). Check UNRAID_USER_SHARE_HOST_ROOT and the /music mapping."
        )
    return roots, entries


_ENTRY_LOOKUP_CACHE: OrderedDict[tuple[int, int], dict[str, Any]] = OrderedDict()


def rel_text(path: Path | str | None, root: Path | str | None) -> str | None:
    """Return slash-relative text for a path/root pair."""

    path_txt = str(path or "").strip().rstrip("/")
    root_txt = str(root or "").strip().rstrip("/")
    if not path_txt or not root_txt:
        return None
    if path_txt == root_txt:
        return ""
    prefix = root_txt + "/"
    if path_txt.startswith(prefix):
        return path_txt[len(prefix) :]
    return None


def entry_lookup_tables(entries: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None) -> dict[str, Any]:
    """Build cached lookup tables for storage plan entries."""

    if not entries:
        return {"by_device": {}, "access_roots": []}
    cache_key = (id(entries), len(entries))
    cached = _ENTRY_LOOKUP_CACHE.get(cache_key)
    if cached is not None:
        _ENTRY_LOOKUP_CACHE.move_to_end(cache_key)
        return cached
    by_device: dict[str, dict[str, Any]] = {}
    access_roots: list[tuple[str, str, dict[str, Any]]] = []
    for raw_entry in entries:
        entry = dict(raw_entry or {})
        device_id = str(entry.get("storage_device_id") or "").strip()
        access_root = str(entry.get("access_root") or "").strip().rstrip("/")
        canonical_root = str(entry.get("canonical_root") or "").strip().rstrip("/")
        if device_id and device_id not in by_device:
            by_device[device_id] = entry
        if access_root:
            access_roots.append((access_root, canonical_root, entry))
    access_roots.sort(key=lambda item: len(item[0]), reverse=True)
    cached = {"by_device": by_device, "access_roots": access_roots}
    _ENTRY_LOOKUP_CACHE[cache_key] = cached
    while len(_ENTRY_LOOKUP_CACHE) > 64:
        _ENTRY_LOOKUP_CACHE.popitem(last=False)
    return cached


def find_entry_for_access_path(
    path: Path,
    entries: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> dict[str, Any] | None:
    """Find the storage plan entry associated with an access path."""

    if not entries:
        return None
    lookup = entry_lookup_tables(entries)
    path_txt = str(path or "").strip().rstrip("/")
    if not path_txt:
        return None
    parts = Path(path_txt).parts
    if len(parts) >= 3 and parts[1] == "host_mnt":
        device_id = str(parts[2] or "").strip()
        entry = dict((lookup.get("by_device") or {}).get(device_id) or {})
        if entry:
            access_root = str(entry.get("access_root") or "").strip().rstrip("/")
            rel = rel_text(path_txt, access_root)
            if rel is not None:
                canonical_root = str(entry.get("canonical_root") or "").strip().rstrip("/")
                out = dict(entry)
                out["storage_rel_path"] = rel
                out["canonical_path"] = canonical_root if not rel else f"{canonical_root}/{rel}"
                return out
    for access_root, canonical_root, entry in lookup.get("access_roots") or []:
        rel = rel_text(path_txt, access_root)
        if rel is None:
            continue
        out = dict(entry)
        out["storage_rel_path"] = rel
        out["canonical_path"] = canonical_root if not rel else f"{canonical_root}/{rel}"
        return out
    return None


def canonical_path_for_access_path(
    path: Path,
    entries: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> Path:
    """Map a direct disk access path back to its canonical PMDA path."""

    entry = find_entry_for_access_path(path, entries)
    if entry and entry.get("canonical_path"):
        return Path(str(entry["canonical_path"]))
    return path


def access_path_for_canonical_path(
    canonical_path: Path,
    device_id: str | None,
    entries: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> Path | None:
    """Map a canonical PMDA path to a direct disk access path for a device."""

    if not entries or not device_id:
        return None
    lookup = entry_lookup_tables(entries)
    entry = dict((lookup.get("by_device") or {}).get(str(device_id or "").strip()) or {})
    if entry:
        canonical_root = str(entry.get("canonical_root") or "").strip().rstrip("/")
        rel = rel_text(canonical_path, canonical_root)
        if rel is not None:
            access_root = str(entry.get("access_root") or "").strip().rstrip("/")
            return Path(access_root if not rel else f"{access_root}/{rel}")
    for access_root, canonical_root, raw_entry in lookup.get("access_roots") or []:
        if str(raw_entry.get("storage_device_id") or "").strip() != str(device_id or "").strip():
            continue
        rel = rel_text(canonical_path, canonical_root)
        if rel is not None:
            return Path(access_root if not rel else f"{access_root}/{rel}")
    return None


def plan_entry_for_canonical_path(
    canonical_path: Path | str | None,
    plan_entries: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> dict[str, Any] | None:
    """Return the longest matching storage plan entry for a canonical path."""

    path_txt = str(canonical_path or "").strip()
    if not path_txt or not plan_entries:
        return None
    best: tuple[int, dict[str, Any]] | None = None
    for raw_entry in plan_entries:
        entry = dict(raw_entry or {})
        canonical_root = str(entry.get("canonical_root") or "").strip().rstrip("/")
        device_id = str(entry.get("storage_device_id") or "").strip()
        if not canonical_root or not device_id:
            continue
        if path_txt != canonical_root and not path_txt.startswith(canonical_root + "/"):
            continue
        score = len(canonical_root)
        if best is None or score > best[0]:
            best = (score, entry)
    return dict(best[1]) if best else None
