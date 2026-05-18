"""Runtime-bound library workflow settings and scope helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_files_tag_write_mode',
    'library_is_audit_mode',
    '_normalize_root_path_list',
    '_workflow_serialized_path_list',
    '_library_workflow_scope_roots',
    '_library_workflow_state',
    '_library_workflow_prepare_updates',
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

def _files_tag_write_mode(settings_snapshot: dict[str, Any] | None = None) -> str:
    raw = None
    if isinstance(settings_snapshot, dict):
        raw = settings_snapshot.get("FILES_TAG_WRITE_MODE")
    if raw is None:
        try:
            raw = _get_config_from_db("FILES_TAG_WRITE_MODE")
        except Exception:
            raw = None
    mode = str(raw or FILES_TAG_WRITE_MODE or "full").strip().lower()
    if mode not in {"full", "pmda_id_only"}:
        mode = "full"
    return mode

def library_is_audit_mode(settings_snapshot: dict[str, Any] | None = None) -> bool:
    raw = None
    if isinstance(settings_snapshot, dict):
        raw = settings_snapshot.get("LIBRARY_WORKFLOW_MODE")
    if raw is None:
        try:
            raw = _get_config_from_db("LIBRARY_WORKFLOW_MODE")
        except Exception:
            raw = None
    return _normalize_library_workflow_mode(raw, default="") == "audit"

def _normalize_root_path_list(value: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in _parse_files_roots(value):
        normalized = _normalize_root_path(raw)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out

def _workflow_serialized_path_list(paths: list[str]) -> str:
    return ",".join(_normalize_root_path_list(paths))

def _library_workflow_scope_roots(settings_snapshot: dict[str, Any] | None = None) -> dict[str, list[str]]:
    state = _library_workflow_state(settings_snapshot)
    mode = _normalize_library_workflow_mode(state.get("mode"), default="managed")
    serving_root = _normalize_root_path(state.get("serving_root"))
    intake_roots = _normalize_root_path_list(state.get("intake_roots"))
    source_roots = _normalize_root_path_list(state.get("source_roots"))
    scan_roots = _normalize_root_path_list(state.get("scan_roots"))
    dupes_root = _normalize_root_path(state.get("dupes_root"))
    incomplete_root = _normalize_root_path(state.get("incomplete_root"))
    files_roots = _normalize_root_path_list(
        (settings_snapshot or {}).get("FILES_ROOTS")
        or _get_config_from_db(
            "FILES_ROOTS",
            ",".join(FILES_ROOTS) if isinstance(FILES_ROOTS, list) else (FILES_ROOTS or ""),
        )
    )

    if mode == "managed":
        library_roots = [serving_root] if serving_root else []
        inbox_roots = intake_roots
    elif mode == "mirror":
        library_roots = [serving_root] if serving_root else []
        inbox_roots = _normalize_root_path_list([*source_roots, *intake_roots])
    elif mode in {"inplace", "audit"}:
        library_roots = source_roots or ([serving_root] if serving_root else [])
        inbox_roots = intake_roots
    else:
        library_roots = [serving_root] if serving_root else []
        inbox_roots = [root for root in files_roots if root not in set(library_roots)]

    dupe_roots = [root for root in [dupes_root, incomplete_root] if root]
    inbox_roots = [root for root in inbox_roots if root not in set(library_roots) and root not in set(dupe_roots)]
    return {
        "library_roots": _normalize_root_path_list(library_roots),
        "inbox_roots": _normalize_root_path_list(inbox_roots),
        "scan_roots": scan_roots or _normalize_root_path_list([*library_roots, *inbox_roots]),
        "dupe_roots": _normalize_root_path_list(dupe_roots),
    }

def _library_workflow_state(settings_snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    snapshot = settings_snapshot if isinstance(settings_snapshot, dict) else _settings_db_read_all()

    def _get_setting(key: str, runtime_value: Any = "") -> Any:
        if key in snapshot:
            return snapshot.get(key)
        return runtime_value

    export_root = _normalize_root_path(_get_setting("EXPORT_ROOT", EXPORT_ROOT))
    dupe_root = _normalize_root_path(_get_setting("DUPE_ROOT", DUPE_ROOT))
    incomplete_root = _normalize_root_path(
        _get_setting(
            "INCOMPLETE_ALBUMS_TARGET_DIR",
            _get_config_from_db("INCOMPLETE_ALBUMS_TARGET_DIR", "/dupes/incomplete_albums"),
        )
        or "/dupes/incomplete_albums"
    )
    materialization_mode = str(
        _get_setting("LIBRARY_MATERIALIZATION_MODE", _get_setting("EXPORT_LINK_STRATEGY", EXPORT_LINK_STRATEGY or "hardlink"))
        or "hardlink"
    ).strip().lower()
    if materialization_mode not in {"hardlink", "copy", "move", "symlink"}:
        materialization_mode = "hardlink"
    include_format = bool(
        _parse_bool(
            _get_setting(
                "LIBRARY_INCLUDE_FORMAT_IN_FOLDER",
                _get_setting("EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER", EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER),
            )
        )
    )
    include_type = bool(
        _parse_bool(
            _get_setting(
                "LIBRARY_INCLUDE_TYPE_IN_FOLDER",
                _get_setting("EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER", EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER),
            )
        )
    )

    rows = _effective_files_source_rows(enabled_only=False)
    enabled_rows = [row for row in rows if bool(row.get("enabled")) and _normalize_root_path(row.get("path"))]
    library_rows = [
        _normalize_root_path(row.get("path"))
        for row in enabled_rows
        if str(row.get("role") or "library").strip().lower() == "library"
    ]
    incoming_rows = [
        _normalize_root_path(row.get("path"))
        for row in enabled_rows
        if str(row.get("role") or "library").strip().lower() == "incoming"
    ]
    files_roots = _normalize_root_path_list(_get_setting("FILES_ROOTS", ",".join(FILES_ROOTS or [])))

    saved_mode_raw = snapshot.get("LIBRARY_WORKFLOW_MODE")
    saved_mode = _normalize_library_workflow_mode(saved_mode_raw, default="")
    if not str(saved_mode_raw or "").strip():
        if export_root and export_root in library_rows:
            mode = "inplace"
        elif export_root and library_rows:
            mode = "mirror"
        elif export_root and files_roots:
            mode = "inplace" if export_root in files_roots else "mirror"
        elif rows:
            mode = "custom"
        else:
            mode = "managed"
    else:
        mode = saved_mode

    serving_root = _normalize_root_path(snapshot.get("LIBRARY_SERVING_ROOT") or export_root)
    intake_roots = _normalize_root_path_list(snapshot.get("LIBRARY_INTAKE_ROOTS"))
    source_roots = _normalize_root_path_list(snapshot.get("LIBRARY_SOURCE_ROOTS"))

    effective_mode = "inplace" if mode == "audit" else mode

    if effective_mode == "managed":
        if not intake_roots:
            intake_roots = incoming_rows or library_rows or files_roots
        source_roots = _normalize_root_path_list(source_roots)
        serving_root = serving_root or export_root or "/music/Music_matched"
    elif effective_mode == "mirror":
        if not source_roots:
            source_roots = library_rows or files_roots
        if not intake_roots:
            intake_roots = incoming_rows
        serving_root = serving_root or export_root or "/music/Music_matched"
    elif effective_mode == "inplace":
        if not source_roots:
            if serving_root:
                source_roots = [serving_root]
            else:
                source_roots = library_rows or files_roots
        if not intake_roots:
            intake_roots = incoming_rows
        serving_root = serving_root or (source_roots[0] if source_roots else "") or export_root
    else:
        if not source_roots:
            source_roots = library_rows or files_roots
        if not intake_roots:
            intake_roots = incoming_rows
        serving_root = serving_root or export_root

    visible_scopes: list[str] = ["library"]
    if effective_mode != "inplace" or intake_roots:
        visible_scopes.append("inbox")
    if dupe_root or incomplete_root:
        visible_scopes.append("dupes")

    scan_roots = intake_roots or source_roots
    if effective_mode == "inplace":
        scan_roots = _normalize_root_path_list([*source_roots, *intake_roots])
    elif effective_mode == "mirror":
        # Mirror mode has two different responsibilities:
        # - source_roots/serving_root are already trusted library content.
        # - intake_roots are raw candidates PMDA should identify and export.
        # Keeping the clean library out of scan_roots prevents re-identifying the
        # destination while still allowing it to be indexed and used for duplicate
        # destination-conflict analysis.
        scan_roots = intake_roots or source_roots

    return {
        "mode": mode,
        "serving_root": serving_root,
        "intake_roots": intake_roots,
        "source_roots": source_roots,
        "dupes_root": dupe_root or "/dupes",
        "incomplete_root": incomplete_root or "/dupes/incomplete_albums",
        "materialization_mode": materialization_mode,
        "include_format": include_format,
        "include_type": include_type,
        "visible_scopes": visible_scopes,
        "has_intake": bool(intake_roots),
        "scan_roots": scan_roots,
    }

def _library_workflow_prepare_updates(
    settings_snapshot: dict[str, Any],
    incoming_updates: dict[str, Any],
) -> tuple[dict[str, Any], list[dict] | None]:
    current = _library_workflow_state(settings_snapshot)
    incoming_has_serving_root = "LIBRARY_SERVING_ROOT" in incoming_updates
    mode = _normalize_library_workflow_mode(
        incoming_updates.get("LIBRARY_WORKFLOW_MODE", current["mode"]),
        default=current["mode"],
    )
    serving_root = _normalize_root_path(
        incoming_updates.get("LIBRARY_SERVING_ROOT", current["serving_root"])
    )
    intake_roots = _normalize_root_path_list(
        incoming_updates.get("LIBRARY_INTAKE_ROOTS", current["intake_roots"])
    )
    source_roots = _normalize_root_path_list(
        incoming_updates.get("LIBRARY_SOURCE_ROOTS", current["source_roots"])
    )
    dupes_root = _normalize_root_path(
        incoming_updates.get("LIBRARY_DUPES_ROOT", current["dupes_root"])
    ) or "/dupes"
    incomplete_root = _normalize_root_path(
        incoming_updates.get("LIBRARY_INCOMPLETE_ROOT", current["incomplete_root"])
    ) or "/dupes/incomplete_albums"
    materialization_mode = str(
        incoming_updates.get("LIBRARY_MATERIALIZATION_MODE", current["materialization_mode"]) or "hardlink"
    ).strip().lower()
    if materialization_mode not in {"hardlink", "copy", "move", "symlink"}:
        materialization_mode = "hardlink"
    include_format = bool(
        _parse_bool(incoming_updates.get("LIBRARY_INCLUDE_FORMAT_IN_FOLDER", current["include_format"]))
    )
    include_type = bool(
        _parse_bool(incoming_updates.get("LIBRARY_INCLUDE_TYPE_IN_FOLDER", current["include_type"]))
    )

    if mode == "managed":
        if not intake_roots:
            raise ValueError("Managed library mode requires at least one intake folder.")
        if not serving_root:
            raise ValueError("Managed library mode requires a clean library folder.")
        source_rows = [
            {
                "path": serving_root,
                "role": "library",
                "enabled": True,
                "priority": 10,
                "is_winner_root": True,
            }
        ]
        for offset, path in enumerate(intake_roots, start=2):
            source_rows.append(
                {
                    "path": path,
                    "role": "incoming",
                    "enabled": True,
                    "priority": offset * 10,
                    "is_winner_root": False,
                }
            )
        effective_files_roots = _normalize_root_path_list([serving_root, *intake_roots])
    elif mode == "mirror":
        if not source_roots:
            raise ValueError("Mirror library mode requires at least one source library folder.")
        if not serving_root:
            raise ValueError("Mirror library mode requires a clean library folder.")
        source_rows = [
            {
                "path": path,
                "role": "library",
                "enabled": True,
                "priority": (idx + 1) * 10,
                "is_winner_root": idx == 0,
            }
            for idx, path in enumerate(source_roots)
        ]
        for offset, path in enumerate(intake_roots, start=len(source_rows) + 1):
            source_rows.append(
                {
                    "path": path,
                    "role": "incoming",
                    "enabled": True,
                    "priority": offset * 10,
                    "is_winner_root": False,
                }
            )
        effective_files_roots = _normalize_root_path_list([*source_roots, *intake_roots])
    elif mode in {"inplace", "audit"}:
        if not source_roots:
            if serving_root:
                source_roots = [serving_root]
            else:
                if mode == "audit":
                    raise ValueError("Audit mode requires at least one library folder.")
                raise ValueError("Organize in place mode requires at least one library folder.")
        if mode == "audit" and not incoming_has_serving_root:
            serving_root = source_roots[0]
        serving_root = serving_root or source_roots[0]
        source_rows = [
            {
                "path": path,
                "role": "library",
                "enabled": True,
                "priority": (idx + 1) * 10,
                "is_winner_root": idx == 0,
            }
            for idx, path in enumerate(source_roots)
        ]
        for offset, path in enumerate(intake_roots, start=len(source_rows) + 1):
            source_rows.append(
                {
                    "path": path,
                    "role": "incoming",
                    "enabled": True,
                    "priority": offset * 10,
                    "is_winner_root": False,
                }
            )
        effective_files_roots = _normalize_root_path_list([*source_roots, *intake_roots])
    else:
        source_rows = None
        effective_files_roots = _normalize_root_path_list(incoming_updates.get("FILES_ROOTS", settings_snapshot.get("FILES_ROOTS")))
        if not source_roots:
            source_roots = current["source_roots"]
        if not intake_roots:
            intake_roots = current["intake_roots"]

    normalized_updates: dict[str, Any] = {
        "LIBRARY_WORKFLOW_MODE": mode,
        "LIBRARY_SERVING_ROOT": serving_root,
        "LIBRARY_INTAKE_ROOTS": _workflow_serialized_path_list(intake_roots),
        "LIBRARY_SOURCE_ROOTS": _workflow_serialized_path_list(source_roots),
        "LIBRARY_DUPES_ROOT": dupes_root,
        "LIBRARY_INCOMPLETE_ROOT": incomplete_root,
        "LIBRARY_MATERIALIZATION_MODE": materialization_mode,
        "LIBRARY_INCLUDE_FORMAT_IN_FOLDER": include_format,
        "LIBRARY_INCLUDE_TYPE_IN_FOLDER": include_type,
        "DUPE_ROOT": dupes_root,
        "INCOMPLETE_ALBUMS_TARGET_DIR": incomplete_root,
        "EXPORT_ROOT": serving_root,
        "EXPORT_LINK_STRATEGY": materialization_mode,
        "EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER": include_format,
        "EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER": include_type,
    }
    if mode != "custom":
        normalized_updates["FILES_ROOTS"] = effective_files_roots
    return normalized_updates, source_rows

def _files_tag_write_mode_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_tag_write_mode(*args, **kwargs)

def library_is_audit_mode_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return library_is_audit_mode(*args, **kwargs)

def _normalize_root_path_list_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _normalize_root_path_list(*args, **kwargs)

def _workflow_serialized_path_list_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _workflow_serialized_path_list(*args, **kwargs)

def _library_workflow_scope_roots_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _library_workflow_scope_roots(*args, **kwargs)

def _library_workflow_state_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _library_workflow_state(*args, **kwargs)

def _library_workflow_prepare_updates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _library_workflow_prepare_updates(*args, **kwargs)

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}
