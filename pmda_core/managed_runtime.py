"""Runtime-owned management for PMDA-managed Docker sidecars.

This module keeps Docker/Ollama/MusicBrainz runtime orchestration out of
``pmda.py`` while still binding the live bootstrap module for compatibility.
"""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_managed_runtime_resolve_musicbrainz_install_root',
    '_managed_runtime_container_path_to_host_path',
    '_managed_runtime_container_bind_alias_path',
    '_managed_runtime_json_dumps',
    '_managed_runtime_json_loads',
    '_managed_runtime_bundle_defaults',
    '_managed_runtime_bundle_get',
    '_managed_runtime_bundle_upsert',
    '_managed_runtime_bundle_upsert_best_effort',
    '_managed_runtime_log',
    '_managed_runtime_logs',
    '_managed_runtime_action_update',
    '_managed_runtime_get_latest_action',
    '_managed_runtime_docker_cli',
    '_managed_runtime_compose_cli',
    '_managed_runtime_git_cli',
    '_managed_runtime_self_container_name',
    '_managed_runtime_sysfs_gpu_vendor',
    '_managed_runtime_collect_dri_devices',
    '_managed_runtime_gpu_probe',
    '_managed_runtime_ollama_gpu_requested_mode',
    '_managed_runtime_ollama_gpu_profile',
    '_managed_runtime_preflight',
    '_managed_runtime_docker_ps',
    '_managed_runtime_parse_ports',
    '_managed_runtime_docker_inspect_container',
    '_managed_runtime_project_prefix',
    '_managed_runtime_health_check_musicbrainz',
    '_managed_runtime_health_check_ollama',
    '_managed_runtime_container_labels',
    '_managed_runtime_detect_musicbrainz_candidates',
    '_managed_runtime_detect_ollama_candidates',
    '_managed_runtime_ensure_network',
    '_managed_runtime_connect_container_to_network',
    '_managed_runtime_connect_self_to_network',
    '_managed_runtime_try_connect_self_to_existing_network',
    '_managed_runtime_musicbrainz_install_root',
    '_managed_runtime_musicbrainz_data_root',
    '_managed_runtime_ollama_data_root',
    '_managed_runtime_musicbrainz_internal_url',
    '_managed_runtime_short_duration',
    '_managed_runtime_capture_subprocess',
    '_managed_runtime_health_wait',
    '_managed_runtime_register_mb_update_schedule',
    '_managed_runtime_apply_musicbrainz_runtime',
    '_managed_runtime_apply_ollama_runtime',
    '_managed_runtime_adopt_musicbrainz',
    '_managed_runtime_adopt_ollama',
    '_managed_runtime_ollama_pull_blocking',
    '_managed_runtime_ensure_ollama_models',
    '_managed_runtime_bootstrap_musicbrainz',
    '_managed_runtime_bootstrap_ollama',
    '_managed_runtime_bootstrap_worker',
    '_managed_runtime_launch_bootstrap',
    '_managed_runtime_bundle_status',
    '_managed_runtime_status_snapshot',
    '_managed_runtime_musicbrainz_update_due',
    '_managed_runtime_run_musicbrainz_update',
    '_managed_runtime_musicbrainz_repair_worker',
    '_managed_runtime_launch_musicbrainz_search_repair',
    '_managed_runtime_maybe_enqueue_due_jobs',
    '_managed_runtime_resolve_candidate',
    '_managed_runtime_mb_compose_cmd',
    '_managed_runtime_start_musicbrainz_bundle',
    '_managed_runtime_stop_musicbrainz_bundle',
    '_managed_runtime_restart_musicbrainz_bundle',
    '_managed_runtime_reset_musicbrainz_bundle',
    '_managed_runtime_start_ollama_bundle',
    '_managed_runtime_stop_ollama_bundle',
    '_managed_runtime_restart_ollama_bundle',
    '_managed_runtime_reset_ollama_bundle',
    '_ollama_pull_status_snapshot',
    '_ollama_pull_status_update',
    '_run_ollama_pull_async',
    'api_musicbrainz_test',
    'api_ollama_models',
    '_normalize_ollama_probe_url',
    '_ollama_probe',
    'api_ollama_discover',
    '_ollama_model_exists',
    'api_ollama_pull_status',
    'api_ollama_pull',
    'api_runtime_managed_status',
    'api_runtime_managed_logs',
    '_api_runtime_managed_common_roots',
    'api_runtime_managed_bootstrap',
    'api_runtime_managed_adopt',
    'api_runtime_managed_action',
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

def _managed_runtime_resolve_musicbrainz_install_root(bundle: dict[str, Any]) -> str:
    """Return the local compose checkout PMDA can use for MB repair/update."""
    install_root = str((bundle or {}).get("install_root") or "").strip()
    if install_root and not install_root.startswith("{"):
        host_install_root = _managed_runtime_container_path_to_host_path(install_root)
        if host_install_root and (Path(host_install_root) / "docker-compose.yml").exists():
            return host_install_root
        if (Path(install_root) / "docker-compose.yml").exists():
            return install_root
    ownership = str((bundle or {}).get("ownership") or "").strip()
    config_root = str((bundle or {}).get("config_root") or "").strip()
    if ownership and config_root:
        candidate = str(Path(config_root) / ownership)
        host_candidate = _managed_runtime_container_path_to_host_path(candidate)
        if host_candidate and (Path(host_candidate) / "docker-compose.yml").exists():
            return host_candidate
        if (Path(candidate) / "docker-compose.yml").exists():
            return candidate
    default_candidate = str(Path(str(CONFIG_DIR)) / "managed-runtime" / _MANAGED_RUNTIME_MB_DEFAULT_PROJECT)
    host_default_candidate = _managed_runtime_container_path_to_host_path(default_candidate)
    if host_default_candidate and (Path(host_default_candidate) / "docker-compose.yml").exists():
        return host_default_candidate
    if (Path(default_candidate) / "docker-compose.yml").exists():
        return default_candidate
    return ""

def _managed_runtime_container_path_to_host_path(container_path: str) -> str:
    """
    Translate a PMDA container path to the host path Docker Compose must use.

    When PMDA controls the host Docker daemon through /var/run/docker.sock,
    compose bind sources are resolved on the host. A project under /config must
    therefore become its host source, e.g. /mnt/cache/appdata/PMDA.
    """
    raw_path = str(container_path or "").strip()
    if not raw_path.startswith("/"):
        return ""
    alias_path = _managed_runtime_container_bind_alias_path(raw_path)
    if alias_path:
        return alias_path
    docker_cli = str(shutil.which("docker") or "").strip()
    container_name = str(os.getenv("HOSTNAME") or "").strip()
    if not docker_cli or not container_name or not Path("/var/run/docker.sock").exists():
        return ""
    try:
        res = subprocess.run(
            [docker_cli, "inspect", container_name, "--format", "{{json .Mounts}}"],
            capture_output=True,
            text=True,
            timeout=8,
        )
        if res.returncode != 0:
            return ""
        mounts = json.loads(str(res.stdout or "[]").strip() or "[]")
    except Exception:
        return ""
    best: tuple[int, str] | None = None
    for mount in mounts if isinstance(mounts, list) else []:
        dest = str((mount or {}).get("Destination") or "").rstrip("/")
        source = str((mount or {}).get("Source") or "").rstrip("/")
        if not dest or not source:
            continue
        if raw_path == dest or raw_path.startswith(dest + "/"):
            rel = raw_path[len(dest):].lstrip("/")
            translated = str(Path(source) / rel) if rel else source
            score = len(dest)
            if best is None or score > best[0]:
                best = (score, translated)
    return best[1] if best else ""

def _managed_runtime_container_bind_alias_path(container_path: str) -> str:
    """Translate through a second bind mount to the same config directory."""
    raw_path = str(container_path or "").strip()
    config_root = str(CONFIG_DIR).rstrip("/") or "/config"
    if not raw_path.startswith(config_root + "/") and raw_path != config_root:
        return ""
    config_path = Path(config_root)
    if not config_path.exists():
        return ""
    mount_points: list[str] = []
    try:
        with open("/proc/self/mountinfo", "r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                parts = line.split()
                if len(parts) >= 5:
                    mount_points.append(parts[4].replace("\\040", " "))
    except Exception:
        mount_points = []
    # The MURRAY deployment intentionally bind-mounts the host appdata path into
    # PMDA at the same absolute path so host Docker can resolve compose volumes.
    mount_points.extend([
        "/mnt/cache/appdata/PMDA",
        "/mnt/user/appdata/PMDA",
    ])
    rel = raw_path[len(config_root):].lstrip("/")
    for mount_point in sorted(set(mount_points), key=len, reverse=True):
        if mount_point.rstrip("/") == config_root:
            continue
        candidate_root = Path(mount_point)
        try:
            if candidate_root.exists() and candidate_root.samefile(config_path):
                return str(candidate_root / rel) if rel else str(candidate_root)
        except Exception:
            continue
    return ""

def _managed_runtime_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value or {}, ensure_ascii=False, sort_keys=True)
    except Exception:
        return "{}"

def _managed_runtime_json_loads(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return copy.deepcopy(default)
    if isinstance(raw, (dict, list)):
        return copy.deepcopy(raw)
    try:
        parsed = json.loads(str(raw))
    except Exception:
        return copy.deepcopy(default)
    if isinstance(default, dict) and not isinstance(parsed, dict):
        return copy.deepcopy(default)
    if isinstance(default, list) and not isinstance(parsed, list):
        return copy.deepcopy(default)
    return parsed

def _managed_runtime_bundle_defaults(bundle_type: str) -> dict[str, Any]:
    return {
        "bundle_type": str(bundle_type or "").strip(),
        "mode": "absent",
        "state": "idle",
        "phase": "",
        "phase_message": "",
        "config_root": "",
        "data_root": "",
        "install_root": "",
        "effective_url": "",
        "ownership": "",
        "health": {"available": False, "overall_status": "absent"},
        "services": [],
        "meta": {},
        "last_error": "",
        "update_state": {},
        "created_at": 0.0,
        "updated_at": 0.0,
    }

def _managed_runtime_bundle_get(bundle_type: str) -> dict[str, Any]:
    bundle = _managed_runtime_bundle_defaults(bundle_type)
    init_settings_db()
    con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=10)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute("SELECT * FROM managed_runtime_bundles WHERE bundle_type = ?", (bundle_type,))
        row = cur.fetchone()
    finally:
        con.close()
    if not row:
        return bundle
    bundle.update(
        {
            "mode": str(row["mode"] or "absent"),
            "state": str(row["state"] or "idle"),
            "phase": str(row["phase"] or ""),
            "phase_message": str(row["phase_message"] or ""),
            "config_root": str(row["config_root"] or ""),
            "data_root": str(row["data_root"] or ""),
            "install_root": str(row["install_root"] or ""),
            "effective_url": str(row["effective_url"] or ""),
            "ownership": str(row["ownership"] or ""),
            "health": _managed_runtime_json_loads(row["health_json"], {"available": False, "overall_status": "absent"}),
            "services": _managed_runtime_json_loads(row["services_json"], []),
            "meta": _managed_runtime_json_loads(row["meta_json"], {}),
            "last_error": str(row["last_error"] or ""),
            "update_state": _managed_runtime_json_loads(row["update_state_json"], {}),
            "created_at": float(row["created_at"] or 0.0),
            "updated_at": float(row["updated_at"] or 0.0),
        }
    )
    return bundle

def _managed_runtime_bundle_upsert(bundle_type: str, **fields: Any) -> dict[str, Any]:
    now = time.time()
    bundle = _managed_runtime_bundle_get(bundle_type)
    bundle.update({k: v for k, v in fields.items() if v is not None})
    if not float(bundle.get("created_at") or 0.0):
        bundle["created_at"] = now
    bundle["updated_at"] = now
    init_settings_db()
    con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=10)
    try:
        con.execute("PRAGMA busy_timeout=5000;")
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO managed_runtime_bundles
            (bundle_type, mode, state, phase, phase_message, config_root, data_root, install_root, effective_url, ownership, health_json, services_json, meta_json, last_error, update_state_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bundle_type) DO UPDATE SET
              mode = excluded.mode,
              state = excluded.state,
              phase = excluded.phase,
              phase_message = excluded.phase_message,
              config_root = excluded.config_root,
              data_root = excluded.data_root,
              install_root = excluded.install_root,
              effective_url = excluded.effective_url,
              ownership = excluded.ownership,
              health_json = excluded.health_json,
              services_json = excluded.services_json,
              meta_json = excluded.meta_json,
              last_error = excluded.last_error,
              update_state_json = excluded.update_state_json,
              updated_at = excluded.updated_at,
              created_at = COALESCE(NULLIF(managed_runtime_bundles.created_at, 0), excluded.created_at)
            """,
            (
                bundle_type,
                str(bundle.get("mode") or "absent"),
                str(bundle.get("state") or "idle"),
                str(bundle.get("phase") or ""),
                str(bundle.get("phase_message") or ""),
                str(bundle.get("config_root") or ""),
                str(bundle.get("data_root") or ""),
                str(bundle.get("install_root") or ""),
                str(bundle.get("effective_url") or ""),
                str(bundle.get("ownership") or ""),
                _managed_runtime_json_dumps(bundle.get("health") or {}),
                _managed_runtime_json_dumps(bundle.get("services") or []),
                _managed_runtime_json_dumps(bundle.get("meta") or {}),
                str(bundle.get("last_error") or ""),
                _managed_runtime_json_dumps(bundle.get("update_state") or {}),
                float(bundle.get("created_at") or now),
                float(bundle.get("updated_at") or now),
            ),
        )
        con.commit()
    finally:
        con.close()
    return bundle

def _managed_runtime_bundle_upsert_best_effort(
    bundle_type: str,
    *,
    fallback: dict[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    try:
        return _managed_runtime_bundle_upsert(bundle_type, **fields)
    except sqlite3.OperationalError as exc:
        if "locked" not in str(exc).lower():
            raise
        logging.debug("managed runtime status upsert skipped because settings.db is locked", exc_info=True)
        bundle = dict(fallback or {})
        bundle.update({k: v for k, v in fields.items() if v is not None})
        bundle["updated_at"] = time.time()
        return bundle

def _managed_runtime_log(bundle_type: str, message: str, *, service_name: str = "", level: str = "info") -> None:
    bundle_name = str(bundle_type or "").strip() or "managed_runtime"
    msg = str(message or "").strip()
    if not msg:
        return
    level_norm = str(level or "info").strip().lower() or "info"
    init_settings_db()
    con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=10)
    try:
        con.execute("PRAGMA busy_timeout=5000;")
        cur = con.cursor()
        cur.execute(
            "INSERT INTO managed_runtime_logs(bundle_type, service_name, level, message, created_at) VALUES(?, ?, ?, ?, ?)",
            (bundle_name, str(service_name or ""), level_norm, msg, time.time()),
        )
        cur.execute(
            """
            DELETE FROM managed_runtime_logs
            WHERE log_id NOT IN (
                SELECT log_id
                FROM managed_runtime_logs
                WHERE bundle_type = ?
                ORDER BY log_id DESC
                LIMIT 500
            )
            AND bundle_type = ?
            """,
            (bundle_name, bundle_name),
        )
        con.commit()
    finally:
        con.close()
    log_fn = logging.info
    if level_norm == "warning":
        log_fn = logging.warning
    elif level_norm == "error":
        log_fn = logging.error
    elif level_norm == "debug":
        log_fn = logging.debug
    prefix = f"[ManagedRuntime][{bundle_name}]"
    if service_name:
        prefix = f"{prefix}[{service_name}]"
    log_fn("%s %s", prefix, msg)

def _managed_runtime_logs(bundle_type: str | None = None, *, service_name: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
    init_settings_db()
    safe_limit = max(1, min(int(limit or 200), 1000))
    sql = "SELECT log_id, bundle_type, service_name, level, message, created_at FROM managed_runtime_logs"
    where: list[str] = []
    params: list[Any] = []
    if bundle_type:
        where.append("bundle_type = ?")
        params.append(str(bundle_type))
    if service_name:
        where.append("service_name = ?")
        params.append(str(service_name))
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY log_id DESC LIMIT ?"
    params.append(safe_limit)
    con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=10)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    finally:
        con.close()
    return [
        {
            "log_id": int(row["log_id"] or 0),
            "bundle_type": str(row["bundle_type"] or ""),
            "service_name": str(row["service_name"] or ""),
            "level": str(row["level"] or "info"),
            "message": str(row["message"] or ""),
            "created_at": float(row["created_at"] or 0.0),
        }
        for row in rows
    ]

def _managed_runtime_action_update(action_id: str, bundle_type: str, action: str, status: str, *, payload: dict[str, Any] | None = None, result: dict[str, Any] | None = None, error: str = "", completed: bool = False) -> None:
    now = time.time()
    init_settings_db()
    con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=10)
    try:
        con.execute("PRAGMA busy_timeout=5000;")
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO managed_runtime_actions(action_id, bundle_type, action, status, payload_json, result_json, error, created_at, updated_at, completed_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(action_id) DO UPDATE SET
              status = excluded.status,
              payload_json = excluded.payload_json,
              result_json = excluded.result_json,
              error = excluded.error,
              updated_at = excluded.updated_at,
              completed_at = excluded.completed_at
            """,
            (
                str(action_id or ""),
                str(bundle_type or ""),
                str(action or ""),
                str(status or "pending"),
                _managed_runtime_json_dumps(payload or {}),
                _managed_runtime_json_dumps(result or {}),
                str(error or ""),
                now,
                now,
                now if completed else None,
            ),
        )
        con.commit()
    finally:
        con.close()

def _managed_runtime_get_latest_action(bundle_type: str) -> dict[str, Any] | None:
    init_settings_db()
    con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=10)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT action_id, bundle_type, action, status, payload_json, result_json, error, created_at, updated_at, completed_at
            FROM managed_runtime_actions
            WHERE bundle_type = ?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """,
            (str(bundle_type or ""),),
        )
        row = cur.fetchone()
    finally:
        con.close()
    if not row:
        return None
    return {
        "action_id": str(row["action_id"] or ""),
        "bundle_type": str(row["bundle_type"] or ""),
        "action": str(row["action"] or ""),
        "status": str(row["status"] or ""),
        "payload": _managed_runtime_json_loads(row["payload_json"], {}),
        "result": _managed_runtime_json_loads(row["result_json"], {}),
        "error": str(row["error"] or ""),
        "created_at": float(row["created_at"] or 0.0),
        "updated_at": float(row["updated_at"] or 0.0),
        "completed_at": float(row["completed_at"] or 0.0) if row["completed_at"] is not None else None,
    }

def _managed_runtime_docker_cli() -> str:
    return str(shutil.which("docker") or "").strip()

def _managed_runtime_compose_cli() -> list[str]:
    docker_cli = _managed_runtime_docker_cli()
    if docker_cli:
        try:
            res = subprocess.run([docker_cli, "compose", "version"], capture_output=True, text=True, timeout=10)
            if res.returncode == 0:
                return [docker_cli, "compose"]
        except Exception:
            pass
    compose_cli = str(shutil.which("docker-compose") or "").strip()
    if compose_cli:
        try:
            res = subprocess.run([compose_cli, "version"], capture_output=True, text=True, timeout=10)
            if res.returncode == 0:
                return [compose_cli]
        except Exception:
            pass
    return []

def _managed_runtime_git_cli() -> str:
    return str(shutil.which("git") or "").strip()

def _managed_runtime_self_container_name() -> str:
    return str(os.getenv("HOSTNAME") or "").strip()

def _managed_runtime_sysfs_gpu_vendor(render_name: str) -> tuple[str, str]:
    render_node = str(render_name or "").strip()
    if not render_node:
        return "", "unknown"
    vendor_file = Path("/sys/class/drm") / render_node / "device" / "vendor"
    try:
        vendor_id = str(vendor_file.read_text(encoding="utf-8", errors="ignore") or "").strip().lower()
    except Exception:
        vendor_id = ""
    vendor = _MANAGED_RUNTIME_GPU_VENDOR_MAP.get(vendor_id, "unknown") if vendor_id else "unknown"
    return vendor_id, vendor

def _managed_runtime_collect_dri_devices() -> list[dict[str, str]]:
    devices: list[dict[str, str]] = []
    dri_root = Path("/dev/dri")
    if not dri_root.exists():
        return devices
    for node in sorted(dri_root.iterdir(), key=lambda item: item.name):
        name = str(node.name or "")
        if not name.startswith(("renderD", "card")):
            continue
        vendor_id, vendor = _managed_runtime_sysfs_gpu_vendor(name)
        devices.append(
            {
                "path": str(node),
                "name": name,
                "kind": "render" if name.startswith("renderD") else "card",
                "vendor_id": vendor_id,
                "vendor": vendor,
            }
        )
    return devices

def _managed_runtime_gpu_probe() -> dict[str, Any]:
    nvidia_devices = sorted(
        {
            str(path)
            for path in list(Path("/dev").glob("nvidia[0-9]*")) + list(Path("/dev").glob("nvidiactl")) + list(Path("/dev").glob("nvidia-uvm*"))
            if path.exists()
        }
    )
    dri_devices = _managed_runtime_collect_dri_devices()
    render_devices = [row for row in dri_devices if row.get("kind") == "render"]
    card_devices = [row for row in dri_devices if row.get("kind") == "card"]
    amd_render = [row for row in render_devices if row.get("vendor") == "amd"]
    intel_render = [row for row in render_devices if row.get("vendor") == "intel"]
    unknown_render = [row for row in render_devices if row.get("vendor") == "unknown"]
    kfd_present = Path("/dev/kfd").exists()

    available_modes: list[str] = []
    recommended_mode = _MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU
    message = "No GPU device was passed to the PMDA container; managed Ollama will run on CPU"

    if nvidia_devices:
        available_modes.append(_MANAGED_RUNTIME_OLLAMA_GPU_MODE_NVIDIA)
        recommended_mode = _MANAGED_RUNTIME_OLLAMA_GPU_MODE_NVIDIA
        message = "NVIDIA GPU devices detected; managed Ollama can request CUDA acceleration"
    elif kfd_present and (amd_render or unknown_render):
        available_modes.extend(
            [
                _MANAGED_RUNTIME_OLLAMA_GPU_MODE_AMD_ROCM,
                _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_AMD,
            ]
        )
        recommended_mode = _MANAGED_RUNTIME_OLLAMA_GPU_MODE_AMD_ROCM
        message = "AMD GPU devices detected via /dev/kfd + /dev/dri; managed Ollama will prefer ROCm"
    elif intel_render:
        available_modes.extend(
            [
                _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_INTEL,
                _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN,
            ]
        )
        recommended_mode = _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_INTEL
        message = "Intel render devices detected via /dev/dri; managed Ollama can try experimental Vulkan acceleration"
    elif amd_render:
        available_modes.extend(
            [
                _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_AMD,
                _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN,
            ]
        )
        recommended_mode = _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_AMD
        message = "AMD render devices detected via /dev/dri; managed Ollama can try Vulkan acceleration"
    elif unknown_render:
        available_modes.append(_MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN)
        recommended_mode = _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN
        message = "Render devices detected via /dev/dri; managed Ollama can try experimental Vulkan acceleration"

    return {
        "available": bool(available_modes),
        "recommended_mode": recommended_mode,
        "available_modes": available_modes,
        "nvidia_devices": nvidia_devices,
        "dri_devices": dri_devices,
        "render_devices": render_devices,
        "card_devices": card_devices,
        "kfd_present": bool(kfd_present),
        "message": message,
    }

def _managed_runtime_ollama_gpu_requested_mode() -> str:
    requested = str(_get_config_from_db("MANAGED_OLLAMA_ACCELERATION_MODE", _MANAGED_RUNTIME_OLLAMA_GPU_MODE_AUTO) or _MANAGED_RUNTIME_OLLAMA_GPU_MODE_AUTO).strip().lower()
    if requested not in _MANAGED_RUNTIME_OLLAMA_GPU_ALLOWED_MODES:
        return _MANAGED_RUNTIME_OLLAMA_GPU_MODE_AUTO
    return requested

def _managed_runtime_ollama_gpu_profile(*, requested_mode: str | None = None) -> dict[str, Any]:
    probe = _managed_runtime_gpu_probe()
    requested = str(requested_mode or _managed_runtime_ollama_gpu_requested_mode() or _MANAGED_RUNTIME_OLLAMA_GPU_MODE_AUTO).strip().lower()
    if requested not in _MANAGED_RUNTIME_OLLAMA_GPU_ALLOWED_MODES:
        requested = _MANAGED_RUNTIME_OLLAMA_GPU_MODE_AUTO
    selected = probe.get("recommended_mode") or _MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU
    fallback_reason = ""
    if requested == _MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU:
        selected = _MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU
    elif requested != _MANAGED_RUNTIME_OLLAMA_GPU_MODE_AUTO:
        available_modes = set(probe.get("available_modes") or [])
        if requested in available_modes:
            selected = requested
        else:
            selected = _MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU
            fallback_reason = f"Requested acceleration mode '{requested}' is not available from devices passed to PMDA"
    device_paths: list[str] = []
    env: dict[str, str] = {}
    docker_args: list[str] = []
    if selected == _MANAGED_RUNTIME_OLLAMA_GPU_MODE_NVIDIA:
        docker_args.extend(["--gpus", "all"])
        env["NVIDIA_VISIBLE_DEVICES"] = "all"
        env["NVIDIA_DRIVER_CAPABILITIES"] = "compute,utility"
    elif selected == _MANAGED_RUNTIME_OLLAMA_GPU_MODE_AMD_ROCM:
        device_paths.extend(
            [
                "/dev/kfd",
                *[str(row.get("path") or "") for row in probe.get("card_devices") or []],
                *[str(row.get("path") or "") for row in probe.get("render_devices") or []],
            ]
        )
    elif selected in {
        _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN,
        _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_INTEL,
        _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_AMD,
    }:
        device_paths.extend(
            [
                *[str(row.get("path") or "") for row in probe.get("card_devices") or []],
                *[str(row.get("path") or "") for row in probe.get("render_devices") or []],
            ]
        )
        env["OLLAMA_VULKAN"] = "1"
    device_paths = [path for path in dict.fromkeys(device_paths) if path]
    for path in device_paths:
        docker_args.extend(["--device", f"{path}:{path}"])
    active = selected != _MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU
    mode_label = {
        _MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU: "CPU",
        _MANAGED_RUNTIME_OLLAMA_GPU_MODE_NVIDIA: "NVIDIA CUDA",
        _MANAGED_RUNTIME_OLLAMA_GPU_MODE_AMD_ROCM: "AMD ROCm",
        _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN: "Vulkan",
        _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_INTEL: "Intel Vulkan",
        _MANAGED_RUNTIME_OLLAMA_GPU_MODE_VULKAN_AMD: "AMD Vulkan",
    }.get(selected, selected.upper())
    message = str(probe.get("message") or "")
    if not active and requested != _MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU:
        message = fallback_reason or message
    return {
        "requested_mode": requested,
        "selected_mode": selected,
        "mode_label": mode_label,
        "active": active,
        "device_paths": device_paths,
        "env": env,
        "docker_args": docker_args,
        "probe": probe,
        "fallback_reason": fallback_reason,
        "message": message,
    }

def _managed_runtime_preflight() -> dict[str, Any]:
    docker_socket = Path("/var/run/docker.sock")
    docker_cli = _managed_runtime_docker_cli()
    compose_cli = _managed_runtime_compose_cli()
    git_cli = _managed_runtime_git_cli()
    gpu_probe = _managed_runtime_gpu_probe()
    compose_ok = False
    docker_ok = False
    docker_message = ""
    if docker_cli and docker_socket.exists():
        try:
            res = subprocess.run([docker_cli, "version", "--format", "{{.Client.Version}}"], capture_output=True, text=True, timeout=10)
            docker_ok = res.returncode == 0
            docker_message = (res.stdout or res.stderr or "").strip()
        except Exception as exc:
            docker_message = str(exc)
        compose_ok = bool(compose_cli)
        if compose_cli and not docker_message:
            docker_message = "Docker and Compose are available"
    elif not docker_socket.exists():
        docker_message = "Docker socket is not mounted at /var/run/docker.sock"
    elif not docker_cli:
        docker_message = "docker CLI is missing from the PMDA container"
    return {
        "available": bool(docker_ok and compose_ok),
        "docker_socket": str(docker_socket),
        "docker_socket_present": bool(docker_socket.exists()),
        "docker_cli": docker_cli,
        "compose_cli": " ".join(compose_cli),
        "git_cli": git_cli,
        "docker_ok": bool(docker_ok),
        "compose_ok": bool(compose_ok),
        "git_ok": bool(git_cli),
        "message": docker_message or ("Docker is available" if docker_ok and compose_ok else "Docker preflight failed"),
        "self_container": _managed_runtime_self_container_name(),
        "gpu_probe": gpu_probe,
    }

def _managed_runtime_docker_ps(*, all_containers: bool = True) -> list[dict[str, Any]]:
    docker_cli = _managed_runtime_docker_cli()
    if not docker_cli:
        return []
    cmd = [docker_cli, "ps", "--format", "{{json .}}"]
    if all_containers:
        cmd.insert(2, "-a")
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    except Exception:
        return []
    if res.returncode != 0:
        return []
    rows: list[dict[str, Any]] = []
    for raw_line in (res.stdout or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows

def _managed_runtime_parse_ports(raw_ports: str) -> list[dict[str, Any]]:
    ports = []
    for chunk in str(raw_ports or "").split(","):
        item = chunk.strip()
        if not item:
            continue
        match = re.search(r"(?:(?P<host>[^:]+):)?(?P<host_port>\d+)->(?P<container_port>\d+)/(tcp|udp)", item)
        if not match:
            continue
        ports.append(
            {
                "host": str(match.group("host") or "127.0.0.1"),
                "host_port": int(match.group("host_port") or 0),
                "container_port": int(match.group("container_port") or 0),
            }
        )
    return ports

def _managed_runtime_docker_inspect_container(container_name: str) -> dict[str, Any]:
    docker_cli = _managed_runtime_docker_cli()
    name = str(container_name or "").strip()
    if not docker_cli or not name:
        return {}
    try:
        res = subprocess.run(
            [docker_cli, "inspect", name],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception:
        return {}
    if res.returncode != 0:
        return {}
    try:
        parsed = json.loads(str(res.stdout or "[]").strip() or "[]")
    except Exception:
        return {}
    if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
        return dict(parsed[0])
    return {}

def _managed_runtime_project_prefix(name: str, services: set[str]) -> str:
    raw = str(name or "").strip()
    for service in sorted(services, key=len, reverse=True):
        suffix = f"-{service}-1"
        if raw.endswith(suffix):
            return raw[: -len(suffix)]
    return raw

def _managed_runtime_health_check_musicbrainz(base_url: str) -> dict[str, Any]:
    url = str(base_url or "").strip().rstrip("/")
    if not url:
        return {"available": False, "overall_status": "unavailable", "reason_code": "not_configured", "message": "No URL configured"}

    def _in_container() -> bool:
        return os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv")

    def _docker_gateway_ip() -> str:
        try:
            with open("/proc/net/route", "r", encoding="utf-8") as handle:
                for line in handle.read().splitlines()[1:]:
                    parts = line.strip().split()
                    if len(parts) < 3:
                        continue
                    dest, gateway = parts[1], parts[2]
                    if dest != "00000000":
                        continue
                    return socket.inet_ntoa(struct.pack("<L", int(gateway, 16)))
        except Exception:
            return ""
        return ""

    def _candidate_urls(raw: str) -> list[str]:
        normalized = str(raw or "").strip()
        if not normalized:
            return []
        if "://" not in normalized:
            normalized = f"http://{normalized}"
        normalized = normalized.rstrip("/")
        out = [normalized]
        if not _in_container():
            return out
        try:
            parsed = urllib.parse.urlparse(normalized)
            host = parsed.hostname or ""
            if host in {"127.0.0.1", "0.0.0.0", "localhost"}:
                port = parsed.port or (443 if parsed.scheme == "https" else 80)
                out.append(f"{parsed.scheme}://host.docker.internal:{port}")
                gateway = _docker_gateway_ip()
                if gateway:
                    out.append(f"{parsed.scheme}://{gateway}:{port}")
        except Exception:
            pass
        return list(dict.fromkeys(out))

    radiohead_mbid = "a74b1b7f-71a5-4011-9441-d0b5e4122711"

    def _direct_artist_probe(candidate_url: str) -> dict[str, Any]:
        endpoint = f"{candidate_url}/ws/2/artist/{radiohead_mbid}?fmt=json"
        started = time.time()
        try:
            resp = requests.get(endpoint, timeout=12, headers={"User-Agent": "PMDA managed runtime/1.0"})
            latency_ms = round((time.time() - started) * 1000.0, 1)
            if resp.status_code != 200:
                return {
                    "available": False,
                    "status_code": int(resp.status_code or 0),
                    "latency_ms": latency_ms,
                    "message": f"Direct lookup HTTP {resp.status_code}",
                }
            payload = resp.json() if resp.content else {}
            return {
                "available": str(payload.get("id") or "") == radiohead_mbid or bool(payload.get("name")),
                "latency_ms": latency_ms,
                "message": str(payload.get("name") or "Direct lookup reachable"),
            }
        except requests.exceptions.Timeout:
            return {"available": False, "reason_code": "timeout", "message": "Direct lookup timed out"}
        except requests.exceptions.ConnectionError:
            return {"available": False, "reason_code": "connection_failed", "message": "Direct lookup connection failed"}
        except Exception as exc:
            return {"available": False, "reason_code": "health_check_failed", "message": str(exc or "Direct lookup failed")}

    last_error: dict[str, Any] = {"available": False, "overall_status": "unavailable", "reason_code": "connection_failed", "message": "Connection failed"}
    for candidate_url in _candidate_urls(url):
        endpoint = f"{candidate_url}/ws/2/artist?query=artist:Radiohead&limit=1&fmt=json"
        started = time.time()
        try:
            resp = requests.get(endpoint, timeout=20, headers={"User-Agent": "PMDA managed runtime/1.0"})
            latency_ms = round((time.time() - started) * 1000.0, 1)
            if resp.status_code != 200:
                last_error = {
                    "available": False,
                    "overall_status": "degraded",
                    "reason_code": "http_error",
                    "message": f"HTTP {resp.status_code}",
                    "latency_ms": latency_ms,
                    "url": candidate_url,
                }
                continue
            payload = resp.json() if resp.content else {}
            count = int(len(payload.get("artists") or []))
            if count <= 0:
                direct = _direct_artist_probe(candidate_url)
                direct_ok = bool(direct.get("available"))
                last_error = {
                    "available": False,
                    "overall_status": "degraded",
                    "reason_code": "search_index_empty" if direct_ok else "search_and_direct_failed",
                    "message": "MusicBrainz mirror search index returned zero results" if direct_ok else "MusicBrainz mirror search returned zero results and direct lookup failed",
                    "latency_ms": latency_ms,
                    "url": candidate_url,
                    "result_count": count,
                    "search_available": False,
                    "direct_lookup_available": direct_ok,
                    "direct_lookup": direct,
                    "repair_action": "repair-search-index",
                }
                continue
            return {
                "available": True,
                "overall_status": "healthy",
                "reason_code": "ok",
                "message": f"MusicBrainz mirror reachable ({count} result(s))",
                "latency_ms": latency_ms,
                "url": candidate_url,
                "result_count": count,
                "search_available": True,
                "direct_lookup_available": True,
            }
        except requests.exceptions.Timeout:
            last_error = {"available": False, "overall_status": "degraded", "reason_code": "timeout", "message": "Timed out", "url": candidate_url}
        except requests.exceptions.ConnectionError:
            last_error = {"available": False, "overall_status": "unavailable", "reason_code": "connection_failed", "message": "Connection failed", "url": candidate_url}
        except Exception as exc:
            last_error = {"available": False, "overall_status": "degraded", "reason_code": "health_check_failed", "message": str(exc or "Health check failed"), "url": candidate_url}
    return last_error

def _managed_runtime_health_check_ollama(base_url: str) -> dict[str, Any]:
    probe = _ollama_probe(str(base_url or ""))
    return {
        "available": bool(probe.get("ok")),
        "overall_status": "healthy" if bool(probe.get("ok")) else "unavailable",
        "message": str(probe.get("message") or ""),
        "models": list(probe.get("models") or []),
        "model_count": int(probe.get("model_count") or 0),
    }

def _managed_runtime_container_labels(container_name: str) -> dict[str, Any]:
    docker_cli = _managed_runtime_docker_cli()
    name = str(container_name or "").strip()
    if not docker_cli or not name:
        return {}
    try:
        res = subprocess.run(
            [docker_cli, "inspect", name, "--format", "{{json .Config.Labels}}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception:
        return {}
    if res.returncode != 0:
        return {}
    raw = str(res.stdout or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return dict(parsed or {}) if isinstance(parsed, dict) else {}

def _managed_runtime_detect_musicbrainz_candidates() -> list[dict[str, Any]]:
    service_names = {"db", "search", "indexer", "mq", "musicbrainz"}
    grouped: dict[str, dict[str, Any]] = {}
    for row in _managed_runtime_docker_ps():
        name = str(row.get("Names") or "").strip()
        match = re.match(r"^(?P<prefix>.+)-(?P<service>db|search|indexer|mq|musicbrainz)-\d+$", name)
        if not match:
            continue
        prefix = str(match.group("prefix") or "").strip()
        service = str(match.group("service") or "").strip()
        candidate = grouped.setdefault(
            prefix,
            {
                "id": prefix,
                "bundle_type": _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
                "services": {},
                "published_url": "",
                "project_name": prefix,
            },
        )
        candidate["services"][service] = {
            "name": name,
            "status": str(row.get("State") or "").strip().lower(),
            "image": str(row.get("Image") or "").strip(),
            "ports": _managed_runtime_parse_ports(str(row.get("Ports") or "")),
        }
        if service == "musicbrainz":
            for port in candidate["services"][service]["ports"]:
                if int(port.get("container_port") or 0) == 5000 and int(port.get("host_port") or 0) > 0:
                    host = str(port.get("host") or "").strip() or "127.0.0.1"
                    if host in {"0.0.0.0", "::"}:
                        host = "127.0.0.1"
                    candidate["published_url"] = f"http://{host}:{int(port['host_port'])}"
                    break
    out: list[dict[str, Any]] = []
    for prefix, candidate in grouped.items():
        services = candidate.get("services") or {}
        service_count = len(services)
        published_url = str(candidate.get("published_url") or "")
        musicbrainz_service = dict(services.get("musicbrainz") or {})
        labels = _managed_runtime_container_labels(str(musicbrainz_service.get("name") or ""))
        install_root = str(labels.get("com.docker.compose.project.working_dir") or "").strip()
        health = _managed_runtime_health_check_musicbrainz(published_url) if published_url else {"available": False, "overall_status": "unavailable", "message": "No published web port detected"}
        out.append(
            {
                "id": prefix,
                "project_name": prefix,
                "bundle_type": _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
                "services_present": sorted(services.keys()),
                "service_count": service_count,
                "expected_service_count": len(service_names),
                "published_url": published_url,
                "install_root": install_root,
                "adoptable": service_count >= 3,
                "health": health,
            }
        )
    out.sort(key=lambda row: (not bool(row.get("health", {}).get("available")), -int(row.get("service_count") or 0), str(row.get("id") or "")))
    return out

def _managed_runtime_detect_ollama_candidates() -> list[dict[str, Any]]:
    urls: list[str] = []
    seen: set[str] = set()
    url_meta: dict[str, dict[str, Any]] = {}

    def _add(url_text: str, *, meta: dict[str, Any] | None = None) -> None:
        normalized = _normalize_ollama_probe_url(url_text)
        if not normalized:
            return
        if normalized not in seen:
            seen.add(normalized)
            urls.append(normalized)
        if meta:
            current = dict(url_meta.get(normalized) or {})
            current.update({k: v for k, v in meta.items() if v not in (None, "", [], {})})
            url_meta[normalized] = current

    current_url = str(_get_config_from_db("OLLAMA_URL", OLLAMA_URL) or "").strip()
    if current_url:
        _add(current_url, meta={"source": "configured"})
    for default_url in (
        "http://127.0.0.1:11434",
        "http://localhost:11434",
        "http://host.docker.internal:11434",
        f"http://{_MANAGED_RUNTIME_OLLAMA_CONTAINER}:11434",
        "http://ollama:11434",
    ):
        _add(default_url, meta={"source": "default"})
    for row in _managed_runtime_docker_ps():
        name = str(row.get("Names") or "").strip()
        image = str(row.get("Image") or "").strip()
        if "ollama" not in f"{name} {image}".lower():
            continue
        inspect = _managed_runtime_docker_inspect_container(name)
        networks_raw = ((inspect.get("NetworkSettings") or {}).get("Networks") or {}) if isinstance(inspect, dict) else {}
        network_names: list[str] = []
        aliases: list[str] = []
        ips: list[str] = []
        if isinstance(networks_raw, dict):
            for network_name, network_info in networks_raw.items():
                if not isinstance(network_info, dict):
                    continue
                network_names.append(str(network_name or "").strip())
                for alias in list(network_info.get("Aliases") or []):
                    alias_text = str(alias or "").strip()
                    if alias_text:
                        aliases.append(alias_text)
                ip_text = str(network_info.get("IPAddress") or "").strip()
                if ip_text:
                    ips.append(ip_text)
        container_meta = {
            "source": "docker",
            "container_name": name,
            "container_id": str(row.get("ID") or row.get("IDOrName") or "").strip(),
            "image": image,
            "networks": sorted(set(n for n in network_names if n)),
            "aliases": sorted(set(a for a in aliases if a)),
        }
        # When PMDA and Ollama are in the same compose/Unraid stack, Docker DNS
        # usually exposes the container name or a service alias even without a
        # published host port.
        for host in [name, *aliases]:
            host_text = str(host or "").strip()
            if host_text:
                _add(f"http://{host_text}:11434", meta=container_meta)
        for ip_text in ips:
            _add(f"http://{ip_text}:11434", meta={**container_meta, "source": "docker-ip"})
        for port in _managed_runtime_parse_ports(str(row.get("Ports") or "")):
            if int(port.get("container_port") or 0) == 11434 and int(port.get("host_port") or 0) > 0:
                _add(f"http://127.0.0.1:{int(port['host_port'])}", meta={**container_meta, "source": "docker-published-port"})
    out: list[dict[str, Any]] = []
    for idx, url in enumerate(urls):
        probe = _ollama_probe(url)
        meta = dict(url_meta.get(url) or {})
        out.append(
            {
                "id": f"ollama-{idx + 1}",
                "bundle_type": _MANAGED_RUNTIME_OLLAMA_BUNDLE,
                "url": probe.get("url") or url,
                "adoptable": bool(probe.get("ok")),
                "source": str(meta.get("source") or ""),
                "container_name": str(meta.get("container_name") or ""),
                "networks": list(meta.get("networks") or []),
                "aliases": list(meta.get("aliases") or []),
                "health": {
                    "available": bool(probe.get("ok")),
                    "overall_status": "healthy" if bool(probe.get("ok")) else "unavailable",
                    "message": str(probe.get("message") or ""),
                },
                "models": list(probe.get("models") or []),
                "model_count": int(probe.get("model_count") or 0),
            }
        )
    out.sort(key=lambda row: (not bool(row.get("adoptable")), -int(row.get("model_count") or 0), str(row.get("url") or "")))
    return out

def _managed_runtime_ensure_network() -> None:
    docker_cli = _managed_runtime_docker_cli()
    if not docker_cli:
        raise RuntimeError("docker CLI is missing")
    inspect = subprocess.run([docker_cli, "network", "inspect", _MANAGED_RUNTIME_NETWORK_NAME], capture_output=True, text=True, timeout=10)
    if inspect.returncode == 0:
        return
    res = subprocess.run([docker_cli, "network", "create", _MANAGED_RUNTIME_NETWORK_NAME], capture_output=True, text=True, timeout=20)
    if res.returncode != 0 and "already exists" not in str(res.stderr or "").lower():
        raise RuntimeError((res.stderr or res.stdout or "Failed to create managed runtime network").strip())

def _managed_runtime_connect_container_to_network(container_name: str, *, alias: str | None = None) -> None:
    docker_cli = _managed_runtime_docker_cli()
    if not docker_cli:
        raise RuntimeError("docker CLI is missing")
    if not container_name:
        return
    cmd = [docker_cli, "network", "connect"]
    if alias:
        cmd.extend(["--alias", str(alias)])
    cmd.extend([_MANAGED_RUNTIME_NETWORK_NAME, container_name])
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    stderr = str(res.stderr or "").lower()
    if res.returncode != 0 and "already exists" not in stderr and "endpoint with name" not in stderr:
        raise RuntimeError((res.stderr or res.stdout or f"Failed to connect {container_name} to network").strip())

def _managed_runtime_connect_self_to_network() -> None:
    container_name = _managed_runtime_self_container_name()
    if not container_name:
        return
    _managed_runtime_connect_container_to_network(container_name, alias="pmda")

def _managed_runtime_try_connect_self_to_existing_network(network_name: str) -> bool:
    docker_cli = _managed_runtime_docker_cli()
    container_name = _managed_runtime_self_container_name()
    network = str(network_name or "").strip()
    if not docker_cli or not container_name or not network:
        return False
    if network in {"bridge", "host", "none"}:
        return False
    try:
        res = subprocess.run(
            [docker_cli, "network", "connect", "--alias", "pmda", network, container_name],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except Exception:
        return False
    stderr = str(res.stderr or "").lower()
    return res.returncode == 0 or "already exists" in stderr or "endpoint with name" in stderr

def _managed_runtime_musicbrainz_install_root(config_root: str) -> str:
    root = Path(str(config_root or "").strip()).expanduser()
    return str(root / _MANAGED_RUNTIME_MB_DEFAULT_PROJECT)

def _managed_runtime_musicbrainz_data_root(data_root: str) -> str:
    root = Path(str(data_root or "").strip()).expanduser()
    return str(root / "musicbrainz-mirror")

def _managed_runtime_ollama_data_root(data_root: str) -> str:
    root = Path(str(data_root or "").strip()).expanduser()
    return str(root / "ollama")

def _managed_runtime_musicbrainz_internal_url(install_root: str) -> str:
    project_name = Path(str(install_root or "").strip()).name or _MANAGED_RUNTIME_MB_DEFAULT_PROJECT
    return f"http://{project_name}-musicbrainz-1:5000"

def _managed_runtime_short_duration(seconds: float | int | None) -> str:
    try:
        total = int(max(0.0, float(seconds or 0.0)))
    except Exception:
        total = 0
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    if hours > 0:
        return f"{hours}h {minutes:02d}m"
    if minutes > 0:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"

def _managed_runtime_capture_subprocess(
    cmd: list[str],
    *,
    bundle_type: str,
    service_name: str = "",
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    phase_map: list[tuple[str, str, str]] | None = None,
    heartbeat_label: str = "",
    heartbeat_interval_sec: float = 60.0,
) -> None:
    progress_re = re.compile(r"\b(\d{1,3})%\b")
    wget_progress_re = re.compile(r"^\s*\d+K\s+(?:\.+\s+)+\d{1,3}%\s+")
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    started_at = time.time()
    last_line_at = started_at
    last_line = ""
    last_meta_output_at = 0.0
    last_noisy_progress_log_at = 0.0
    stop_heartbeat = threading.Event()
    heartbeat_name = str(heartbeat_label or service_name or bundle_type or "managed runtime").strip()

    def _heartbeat_loop() -> None:
        interval = max(15.0, float(heartbeat_interval_sec or 60.0))
        while not stop_heartbeat.wait(interval):
            if proc.poll() is not None:
                return
            now = time.time()
            elapsed = max(0.0, now - started_at)
            idle_for = max(0.0, now - last_line_at)
            bundle_now = _managed_runtime_bundle_get(bundle_type)
            meta_now = dict(bundle_now.get("meta") or {})
            operation = str(meta_now.get("operation") or "").strip().lower()
            progress_value = meta_now.get("progress")
            progress_text = ""
            if operation != "repair-search-index":
                try:
                    progress_num = float(progress_value)
                    if math.isfinite(progress_num):
                        progress_text = f", last progress {max(0.0, min(100.0, progress_num)):.0f}%"
                except Exception:
                    progress_text = ""
            last_line_text = str(last_line or "").strip()
            last_line_text = f", last output {_managed_runtime_short_duration(idle_for)} ago: {last_line_text[:220]}" if last_line_text else f", no output for {_managed_runtime_short_duration(idle_for)}"
            heartbeat_message = (
                f"{heartbeat_name} still running: elapsed {_managed_runtime_short_duration(elapsed)}"
                f"{progress_text}{last_line_text}"
            )
            meta_now["elapsed_seconds"] = round(elapsed, 1)
            meta_now["last_heartbeat_at"] = now
            meta_now["last_output_age_seconds"] = round(idle_for, 1)
            meta_now["last_output"] = heartbeat_message
            meta_now["subprocess_running"] = True
            _managed_runtime_bundle_upsert(bundle_type, meta=meta_now)
            _managed_runtime_log(bundle_type, heartbeat_message, service_name=service_name)

    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        daemon=True,
        name=f"managed-runtime-heartbeat-{bundle_type}",
    )
    heartbeat_thread.start()
    try:
        assert proc.stdout is not None
        for raw_line in proc.stdout:
            line = str(raw_line or "").rstrip()
            if not line:
                continue
            now_line = time.time()
            last_line_at = now_line
            last_line = line
            is_noisy_wget_progress = bool(wget_progress_re.search(line))
            should_log_line = True
            if is_noisy_wget_progress:
                should_log_line = (now_line - last_noisy_progress_log_at) >= 30.0
                if should_log_line:
                    last_noisy_progress_log_at = now_line
            if should_log_line:
                _managed_runtime_log(bundle_type, line, service_name=service_name)
            progress_match = progress_re.search(line)
            should_update_meta = progress_match is not None or (now_line - last_meta_output_at) >= 5.0
            meta_now: dict[str, Any] | None = None
            if should_update_meta:
                bundle_now = _managed_runtime_bundle_get(bundle_type)
                meta_now = dict(bundle_now.get("meta") or {})
                meta_now["elapsed_seconds"] = round(max(0.0, now_line - started_at), 1)
                meta_now["last_output_at"] = now_line
                meta_now["last_output_age_seconds"] = 0.0
                meta_now["last_output"] = line[-500:]
                meta_now["subprocess_running"] = True
                last_meta_output_at = now_line
            if progress_match is not None and meta_now is not None:
                try:
                    progress_value = max(0, min(100, int(progress_match.group(1))))
                except Exception:
                    progress_value = None
                if progress_value is not None:
                    if str(meta_now.get("operation") or "").strip().lower() == "repair-search-index":
                        meta_now["last_emitted_percent"] = progress_value
                    else:
                        meta_now["progress"] = progress_value
            if meta_now is not None:
                _managed_runtime_bundle_upsert(bundle_type, meta=meta_now)
            for needle, phase, message in phase_map or []:
                if needle and needle.lower() in line.lower():
                    bundle_now = _managed_runtime_bundle_get(bundle_type)
                    meta_now = dict(bundle_now.get("meta") or {})
                    if phase in {"preflight", "creating"}:
                        meta_now.setdefault("progress", 18 if phase == "preflight" else 24)
                    elif phase == "pulling":
                        meta_now.setdefault("progress", 40)
                    elif phase == "importing":
                        meta_now.setdefault("progress", 41)
                    _managed_runtime_bundle_upsert(bundle_type, state=phase, phase=phase, phase_message=message, meta=meta_now)
                    break
        proc.wait()
    finally:
        stop_heartbeat.set()
        try:
            heartbeat_thread.join(timeout=2.0)
        except Exception:
            pass
        try:
            if proc.stdout is not None:
                proc.stdout.close()
        except Exception:
            pass
        try:
            bundle_now = _managed_runtime_bundle_get(bundle_type)
            meta_now = dict(bundle_now.get("meta") or {})
            meta_now["subprocess_running"] = False
            meta_now["elapsed_seconds"] = round(max(0.0, time.time() - started_at), 1)
            _managed_runtime_bundle_upsert(bundle_type, meta=meta_now)
        except Exception:
            pass
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}")

def _managed_runtime_health_wait(bundle_type: str, url: str, checker, *, attempts: int = 60, sleep_sec: float = 5.0) -> dict[str, Any]:
    last_health: dict[str, Any] = {}
    for _ in range(max(1, int(attempts or 1))):
        health = checker(url)
        last_health = dict(health or {})
        services = [
            {
                "name": bundle_type,
                "status": "healthy" if bool(last_health.get("available")) else str(last_health.get("overall_status") or "waiting"),
                "message": str(last_health.get("message") or ""),
            }
        ]
        _managed_runtime_bundle_upsert(
            bundle_type,
            state="waiting_health",
            phase="waiting_health",
            phase_message=str(last_health.get("message") or "Waiting for service health"),
            health=last_health,
            services=services,
        )
        if bool(last_health.get("available")):
            return last_health
        time.sleep(max(0.1, float(sleep_sec)))
    return last_health

def _managed_runtime_register_mb_update_schedule(bundle: dict[str, Any]) -> dict[str, Any]:
    update_state = dict(bundle.get("update_state") or {})
    mode = str(bundle.get("mode") or "").strip().lower() or "absent"
    enabled = bool(
        _parse_bool(
            _get_config_from_db(
                "MANAGED_MUSICBRAINZ_UPDATE_ENABLED",
                update_state.get("enabled", True),
            )
        )
    )
    if mode != "managed":
        enabled = False
    interval_hours_raw = _get_config_from_db(
        "MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS",
        int(update_state.get("interval_sec") or _MANAGED_RUNTIME_MB_DEFAULT_UPDATE_INTERVAL_SEC) // 3600,
    )
    try:
        interval_hours = max(1, min(24 * 30, int(interval_hours_raw or 24 * 7)))
    except Exception:
        interval_hours = 24 * 7
    interval_sec = int(interval_hours * 3600)
    if interval_sec <= 0:
        interval_sec = _MANAGED_RUNTIME_MB_DEFAULT_UPDATE_INTERVAL_SEC
    last_success_at = float(update_state.get("last_success_at") or 0.0)
    next_planned_at = float(update_state.get("next_planned_at") or 0.0)
    now = time.time()
    if not enabled:
        next_planned_at = 0.0
    elif next_planned_at <= 0:
        next_planned_at = (last_success_at or now) + interval_sec
    update_state.update(
        {
            "enabled": enabled,
            "interval_sec": interval_sec,
            "last_success_at": last_success_at,
            "next_planned_at": next_planned_at,
            "strategy": "weekly_reindex" if mode == "managed" else "external_runtime",
        }
    )
    return update_state

def _managed_runtime_apply_musicbrainz_runtime(mode: str, base_url: str, mirror_name: str, *, config_root: str = "", data_root: str = "", install_root: str = "") -> None:
    updates = {
        "MUSICBRAINZ_MIRROR_ENABLED": True,
        "MUSICBRAINZ_BASE_URL": str(base_url or "").strip(),
        "MUSICBRAINZ_MIRROR_NAME": str(mirror_name or _MANAGED_RUNTIME_MB_DEFAULT_PROJECT).strip(),
        "MUSICBRAINZ_RUNTIME_MODE": str(mode or "managed"),
    }
    if config_root:
        updates["MANAGED_RUNTIME_CONFIG_ROOT"] = str(config_root)
    if data_root:
        updates["MANAGED_RUNTIME_DATA_ROOT"] = str(data_root)
    if install_root:
        updates["MANAGED_MUSICBRAINZ_INSTALL_ROOT"] = str(install_root)
    for key, value in updates.items():
        _settings_db_set_value(key, value)
    _apply_settings_in_memory({"MUSICBRAINZ_MIRROR_ENABLED": True, "MUSICBRAINZ_BASE_URL": str(base_url or "").strip(), "MUSICBRAINZ_MIRROR_NAME": str(mirror_name or "").strip()})

def _managed_runtime_apply_ollama_runtime(mode: str, url: str, fast_model: str, hard_model: str, *, config_root: str = "", data_root: str = "") -> None:
    updates = {
        "OLLAMA_URL": str(url or "").strip().rstrip("/"),
        "OLLAMA_MODEL": str(fast_model or _ollama_model_configured()).strip() or "qwen3:4b",
        "OLLAMA_COMPLEX_MODEL": str(hard_model or _ollama_complex_model_configured()).strip() or "qwen3:14b",
        "OLLAMA_RUNTIME_MODE": str(mode or "managed"),
    }
    if config_root:
        updates["MANAGED_RUNTIME_CONFIG_ROOT"] = str(config_root)
    if data_root:
        updates["MANAGED_RUNTIME_DATA_ROOT"] = str(data_root)
    for key, value in updates.items():
        _settings_db_set_value(key, value)
    _apply_settings_in_memory(updates)

def _managed_runtime_adopt_musicbrainz(candidate: dict[str, Any], *, mode: str = "adopted", config_root: str = "", data_root: str = "") -> dict[str, Any]:
    url = str(candidate.get("published_url") or candidate.get("effective_url") or "").strip()
    if not url:
        raise RuntimeError("MusicBrainz candidate has no usable URL")
    health = _managed_runtime_health_check_musicbrainz(url)
    if not bool(health.get("available")):
        raise RuntimeError(str(health.get("message") or "MusicBrainz candidate is not healthy"))
    bundle = _managed_runtime_bundle_upsert(
        _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
        mode=str(mode or "adopted"),
        state="ready",
        phase="ready",
        phase_message="MusicBrainz mirror adopted",
        config_root=str(config_root or ""),
        data_root=str(data_root or ""),
        install_root=str(candidate.get("install_root") or ""),
        effective_url=url,
        ownership=str(candidate.get("project_name") or candidate.get("id") or ""),
        health=health,
        services=[
            {"name": service_name, "status": "healthy", "message": "Detected from existing Docker stack"}
            for service_name in list(candidate.get("services_present") or [])
        ],
        last_error="",
    )
    bundle["update_state"] = _managed_runtime_register_mb_update_schedule(bundle)
    bundle = _managed_runtime_bundle_upsert(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, update_state=bundle["update_state"])
    _managed_runtime_apply_musicbrainz_runtime(str(mode or "adopted"), url, _MANAGED_RUNTIME_MB_DEFAULT_PROJECT, config_root=config_root, data_root=data_root, install_root=str(candidate.get("install_root") or ""))
    _managed_runtime_log(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, f"Adopted existing MusicBrainz runtime at {url}")
    return bundle

def _managed_runtime_adopt_ollama(candidate: dict[str, Any], *, fast_model: str, hard_model: str, mode: str = "adopted", config_root: str = "", data_root: str = "") -> dict[str, Any]:
    url = str(candidate.get("url") or "").strip()
    if not url:
        raise RuntimeError("Ollama candidate has no usable URL")
    health = _managed_runtime_health_check_ollama(url)
    if not bool(health.get("available")):
        raise RuntimeError(str(health.get("message") or "Ollama candidate is not healthy"))
    models = list(health.get("models") or candidate.get("models") or [])
    services = [{"name": "ollama", "status": "healthy", "message": f"{len(models)} model(s) available"}]
    bundle = _managed_runtime_bundle_upsert(
        _MANAGED_RUNTIME_OLLAMA_BUNDLE,
        mode=str(mode or "adopted"),
        state="ready",
        phase="ready",
        phase_message="Ollama adopted",
        config_root=str(config_root or ""),
        data_root=str(data_root or ""),
        effective_url=url,
        ownership=str(candidate.get("id") or ""),
        health=health,
        services=services,
        meta={
            "models": models,
            "model_count": len(models),
            "gpu": {
                "requested_mode": _managed_runtime_ollama_gpu_requested_mode(),
                "selected_mode": "external",
                "mode_label": "External runtime",
                "active": False,
                "message": "PMDA adopted an existing Ollama runtime and cannot reconfigure its GPU profile",
                "probe": _managed_runtime_gpu_probe(),
            },
        },
        last_error="",
    )
    _managed_runtime_apply_ollama_runtime(str(mode or "adopted"), url, fast_model, hard_model, config_root=config_root, data_root=data_root)
    _managed_runtime_log(_MANAGED_RUNTIME_OLLAMA_BUNDLE, f"Adopted existing Ollama runtime at {url}")
    return bundle

def _managed_runtime_ollama_pull_blocking(url: str, model_name: str, *, bundle_type: str) -> None:
    _managed_runtime_log(bundle_type, f"Pulling Ollama model {model_name}", service_name="ollama")
    response = requests.post(
        f"{url.rstrip('/')}/api/pull",
        json={"name": model_name, "stream": True},
        stream=True,
        timeout=(10, 900),
    )
    if response.status_code != 200:
        detail = response.text[:400] if response.text else f"HTTP {response.status_code}"
        raise RuntimeError(f"Ollama pull failed: {detail}")
    for raw_line in response.iter_lines(decode_unicode=True):
        if not raw_line:
            continue
        try:
            payload = json.loads(raw_line)
        except Exception:
            continue
        status_text = str(payload.get("status") or "pulling").strip() or "pulling"
        completed = int(payload.get("completed") or 0)
        total = int(payload.get("total") or 0)
        progress = 0.0
        if total > 0:
            progress = max(0.0, min(100.0, (completed / total) * 100.0))
        if payload.get("error"):
            raise RuntimeError(str(payload.get("error")))
        _managed_runtime_bundle_upsert(
            bundle_type,
            state="pulling",
            phase="pulling",
            phase_message=f"{status_text} ({model_name})",
            meta={"current_model": model_name, "progress": progress, "completed": completed, "total": total},
        )
    if not _ollama_model_exists(url, model_name):
        raise RuntimeError(f"Ollama did not report {model_name} after pull")
    _managed_runtime_log(bundle_type, f"Ollama model ready: {model_name}", service_name="ollama")

def _managed_runtime_ensure_ollama_models(url: str, models: list[str], *, bundle_type: str) -> dict[str, Any]:
    target_url = str(url or "").strip().rstrip("/")
    wanted = [str(model or "").strip() for model in models if str(model or "").strip()]
    wanted = list(dict.fromkeys(wanted))
    if not target_url:
        raise RuntimeError("Ollama runtime URL is required before pulling models")
    health = _managed_runtime_health_check_ollama(target_url)
    available = {str(model or "").strip() for model in list(health.get("models") or []) if str(model or "").strip()}
    missing = [model for model in wanted if model not in available]
    if not missing:
        return health
    for model_name in missing:
        _managed_runtime_bundle_upsert(
            bundle_type,
            state="pulling",
            phase="pulling",
            phase_message=f"Pulling {model_name}",
        )
        _managed_runtime_ollama_pull_blocking(target_url, model_name, bundle_type=bundle_type)
    return _managed_runtime_health_check_ollama(target_url)

def _managed_runtime_bootstrap_musicbrainz(payload: dict[str, Any]) -> None:
    bundle_type = _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE
    config_root = str(payload.get("config_root") or "").strip()
    data_root = str(payload.get("data_root") or "").strip()
    action = str(payload.get("action") or "auto").strip().lower() or "auto"
    mirror_name = str(payload.get("mirror_name") or "Managed local MusicBrainz").strip() or "Managed local MusicBrainz"
    action_id = str(payload.get("action_id") or str(uuid.uuid4()))
    _managed_runtime_action_update(action_id, bundle_type, "bootstrap", "running", payload=payload)
    _managed_runtime_bundle_upsert(bundle_type, mode="managed" if action != "adopt" else "adopted", state="preflight", phase="preflight", phase_message="Checking Docker environment", config_root=config_root, data_root=data_root, last_error="")
    try:
        preflight = _managed_runtime_preflight()
        if not bool(preflight.get("available")):
            raise RuntimeError(str(preflight.get("message") or "Docker preflight failed"))
        if action in {"auto", "adopt"}:
            candidates = _managed_runtime_detect_musicbrainz_candidates()
            candidate = next((row for row in candidates if bool(row.get("adoptable")) and bool((row.get("health") or {}).get("available"))), None)
            if candidate is not None:
                adopted = _managed_runtime_adopt_musicbrainz(candidate, mode="adopted", config_root=config_root, data_root=data_root)
                _managed_runtime_action_update(action_id, bundle_type, "bootstrap", "completed", payload=payload, result=adopted, completed=True)
                return
            if action == "adopt":
                raise RuntimeError("No adoptable MusicBrainz stack was detected")
        if not config_root or not data_root:
            raise RuntimeError("Managed local runtimes require both a config/runtime base dir and a data base dir")
        install_root = _managed_runtime_musicbrainz_install_root(config_root)
        bundle_data_root = _managed_runtime_musicbrainz_data_root(data_root)
        Path(install_root).mkdir(parents=True, exist_ok=True)
        Path(bundle_data_root).mkdir(parents=True, exist_ok=True)
        _managed_runtime_bundle_upsert(bundle_type, install_root=install_root, mode="managed", state="creating", phase="creating", phase_message="Provisioning MusicBrainz mirror bundle")
        _managed_runtime_ensure_network()
        script_path = Path(__file__).resolve().parent / "scripts" / "provision_musicbrainz_mirror_unraid.sh"
        if not script_path.exists():
            raise RuntimeError(f"Provision script not found: {script_path}")
        cmd = [
            str(script_path),
            "--install-root", install_root,
            "--data-root", bundle_data_root,
            "--host", "127.0.0.1",
            "--port", str(_MANAGED_RUNTIME_MB_DEFAULT_PORT),
        ]
        token = _settings_db_get_secret("MUSICBRAINZ_REPLICATION_TOKEN")
        token_file = ""
        if token:
            token_dir = Path(config_root).expanduser() / "managed-runtime" / "secrets"
            token_dir.mkdir(parents=True, exist_ok=True)
            token_file = str(token_dir / "musicbrainz_replication_token.txt")
            Path(token_file).write_text(token + "\n", encoding="utf-8")
            cmd.extend(["--token-file", token_file])
        else:
            cmd.append("--skip-replication")
        phase_map = [
            ("Building docker images", "pulling", "Building MusicBrainz images"),
            ("Importing latest full MusicBrainz dumps", "importing", "Importing MusicBrainz dump"),
            ("Building materialized tables", "creating", "Building MusicBrainz materialized tables"),
            ("search index", "creating", "Bootstrapping MusicBrainz search indexes"),
        ]
        _managed_runtime_capture_subprocess(
            cmd,
            bundle_type=bundle_type,
            service_name="musicbrainz",
            phase_map=phase_map,
            heartbeat_label="MusicBrainz mirror provisioning",
            heartbeat_interval_sec=60.0,
        )
        candidates = _managed_runtime_detect_musicbrainz_candidates()
        candidate = next((row for row in candidates if str(row.get("project_name") or "") == Path(install_root).name), None)
        if candidate is None:
            candidate = next((row for row in candidates if bool(row.get("adoptable"))), None)
        if candidate is None:
            raise RuntimeError("Provisioning completed but no MusicBrainz Docker bundle was detected")
        for service_name in list(candidate.get("services_present") or []):
            container_name = f"{candidate.get('project_name')}-{service_name}-1"
            _managed_runtime_connect_container_to_network(container_name)
        _managed_runtime_connect_self_to_network()
        internal_url = _managed_runtime_musicbrainz_internal_url(install_root)
        health = _managed_runtime_health_wait(bundle_type, internal_url, _managed_runtime_health_check_musicbrainz, attempts=60, sleep_sec=10.0)
        if not bool(health.get("available")):
            published_url = str(candidate.get("published_url") or "")
            if published_url:
                health = _managed_runtime_health_wait(bundle_type, published_url, _managed_runtime_health_check_musicbrainz, attempts=6, sleep_sec=5.0)
                if bool(health.get("available")):
                    internal_url = published_url
        if not bool(health.get("available")):
            raise RuntimeError(str(health.get("message") or "MusicBrainz mirror did not become healthy"))
        bundle = _managed_runtime_bundle_upsert(
            bundle_type,
            mode="managed",
            state="ready",
            phase="ready",
            phase_message="MusicBrainz mirror ready",
            config_root=config_root,
            data_root=data_root,
            install_root=install_root,
            effective_url=internal_url,
            ownership=str(candidate.get("project_name") or ""),
            health=health,
            services=[
                {"name": service_name, "status": "healthy", "message": "Running"}
                for service_name in list(candidate.get("services_present") or [])
            ],
            last_error="",
        )
        bundle["update_state"] = _managed_runtime_register_mb_update_schedule(bundle)
        bundle = _managed_runtime_bundle_upsert(bundle_type, update_state=bundle["update_state"])
        _managed_runtime_apply_musicbrainz_runtime("managed", internal_url, mirror_name, config_root=config_root, data_root=data_root, install_root=install_root)
        _settings_db_set_value("MANAGED_RUNTIME_CONFIG_ROOT", config_root)
        _settings_db_set_value("MANAGED_RUNTIME_DATA_ROOT", data_root)
        _managed_runtime_action_update(action_id, bundle_type, "bootstrap", "completed", payload=payload, result=bundle, completed=True)
    except Exception as exc:
        message = str(exc or "MusicBrainz bootstrap failed")
        _managed_runtime_bundle_upsert(bundle_type, state="failed", phase="failed", phase_message=message, last_error=message)
        _managed_runtime_action_update(action_id, bundle_type, "bootstrap", "failed", payload=payload, error=message, completed=True)
        _managed_runtime_log(bundle_type, message, level="error")
        raise

def _managed_runtime_bootstrap_ollama(payload: dict[str, Any]) -> None:
    bundle_type = _MANAGED_RUNTIME_OLLAMA_BUNDLE
    config_root = str(payload.get("config_root") or "").strip()
    data_root = str(payload.get("data_root") or "").strip()
    action = str(payload.get("action") or "auto").strip().lower() or "auto"
    fast_model = str(payload.get("fast_model") or _ollama_model_configured()).strip() or "qwen3:4b"
    hard_model = str(payload.get("hard_model") or _ollama_complex_model_configured()).strip() or "qwen3:14b"
    action_id = str(payload.get("action_id") or str(uuid.uuid4()))
    _managed_runtime_action_update(action_id, bundle_type, "bootstrap", "running", payload=payload)
    _managed_runtime_bundle_upsert(bundle_type, mode="managed" if action != "adopt" else "adopted", state="preflight", phase="preflight", phase_message="Checking Docker environment", config_root=config_root, data_root=data_root, last_error="")
    try:
        preflight = _managed_runtime_preflight()
        if not bool(preflight.get("docker_ok")):
            raise RuntimeError(str(preflight.get("message") or "Docker preflight failed"))
        if action in {"auto", "adopt"}:
            candidates = _managed_runtime_detect_ollama_candidates()
            candidate = next((row for row in candidates if bool(row.get("adoptable"))), None)
            if candidate is None:
                attached = False
                for row in candidates:
                    for network_name in list(row.get("networks") or []):
                        if _managed_runtime_try_connect_self_to_existing_network(str(network_name or "")):
                            attached = True
                if attached:
                    candidates = _managed_runtime_detect_ollama_candidates()
                    candidate = next((row for row in candidates if bool(row.get("adoptable"))), None)
            if candidate is not None:
                adopted = _managed_runtime_adopt_ollama(candidate, fast_model=fast_model, hard_model=hard_model, mode="adopted", config_root=config_root, data_root=data_root)
                url = str(adopted.get("effective_url") or candidate.get("url") or "").strip()
                _managed_runtime_bundle_upsert(
                    bundle_type,
                    state="pulling",
                    phase="pulling",
                    phase_message="Ensuring Ollama models",
                    effective_url=url,
                )
                final_health = _managed_runtime_ensure_ollama_models(url, [fast_model, hard_model], bundle_type=bundle_type)
                models = list(final_health.get("models") or [])
                adopted = _managed_runtime_bundle_upsert(
                    bundle_type,
                    mode="adopted",
                    state="ready",
                    phase="ready",
                    phase_message="Ollama ready",
                    effective_url=url,
                    health=final_health,
                    services=[{"name": "ollama", "status": "healthy" if bool(final_health.get("available")) else "degraded", "message": f"{len(models)} model(s) available"}],
                    meta={
                        **dict(adopted.get("meta") or {}),
                        "models": models,
                        "model_count": len(models),
                    },
                    last_error="",
                )
                _managed_runtime_action_update(action_id, bundle_type, "bootstrap", "completed", payload=payload, result=adopted, completed=True)
                return
            if action == "adopt":
                raise RuntimeError("No adoptable Ollama runtime was detected")
        if not config_root or not data_root:
            raise RuntimeError("Managed local runtimes require both a config/runtime base dir and a data base dir")
        _managed_runtime_ensure_network()
        _managed_runtime_connect_self_to_network()
        docker_cli = _managed_runtime_docker_cli()
        ollama_data_root = _managed_runtime_ollama_data_root(data_root)
        Path(ollama_data_root).mkdir(parents=True, exist_ok=True)
        rm_res = subprocess.run([docker_cli, "rm", "-f", _MANAGED_RUNTIME_OLLAMA_CONTAINER], capture_output=True, text=True, timeout=20)
        if rm_res.returncode == 0:
            _managed_runtime_log(bundle_type, "Removed existing managed Ollama container before recreate", service_name="ollama")
        gpu_profile = _managed_runtime_ollama_gpu_profile()

        def _build_run_cmd(profile: dict[str, Any]) -> list[str]:
            cmd = [
                docker_cli,
                "run",
                "-d",
                "--name",
                _MANAGED_RUNTIME_OLLAMA_CONTAINER,
                "--restart",
                "unless-stopped",
                "--network",
                _MANAGED_RUNTIME_NETWORK_NAME,
                "--volume",
                f"{ollama_data_root}:/root/.ollama",
                "-p",
                f"{_MANAGED_RUNTIME_OLLAMA_PORT}:{_MANAGED_RUNTIME_OLLAMA_PORT}",
            ]
            cmd.extend(list(profile.get("docker_args") or []))
            for key, value in dict(profile.get("env") or {}).items():
                cmd.extend(["-e", f"{key}={value}"])
            cmd.append("ollama/ollama:latest")
            return cmd

        active_profile = dict(gpu_profile)
        _managed_runtime_bundle_upsert(
            bundle_type,
            state="creating",
            phase="creating",
            phase_message=f"Starting managed Ollama runtime ({active_profile.get('mode_label') or 'CPU'})",
            config_root=config_root,
            data_root=data_root,
            meta={"gpu": active_profile},
        )
        start_res = subprocess.run(_build_run_cmd(active_profile), capture_output=True, text=True, timeout=30)
        if start_res.returncode != 0 and str(active_profile.get("selected_mode") or "") != _MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU:
            failure_detail = (start_res.stderr or start_res.stdout or "Failed to start managed Ollama container").strip()
            _managed_runtime_log(
                bundle_type,
                f"GPU acceleration start failed for mode {active_profile.get('selected_mode')}: {failure_detail}. Retrying on CPU.",
                level="warning",
                service_name="ollama",
            )
            active_profile = _managed_runtime_ollama_gpu_profile(requested_mode=_MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU)
            active_profile["fallback_reason"] = failure_detail
            _managed_runtime_bundle_upsert(
                bundle_type,
                state="creating",
                phase="creating",
                phase_message="GPU start failed, retrying managed Ollama on CPU",
                meta={"gpu": active_profile},
            )
            start_res = subprocess.run(_build_run_cmd(active_profile), capture_output=True, text=True, timeout=30)
        if start_res.returncode != 0:
            raise RuntimeError((start_res.stderr or start_res.stdout or "Failed to start managed Ollama container").strip())
        container_id = str(start_res.stdout or "").strip()
        _managed_runtime_log(
            bundle_type,
            f"Managed Ollama container started ({container_id[:12]}) using {active_profile.get('mode_label') or 'CPU'}",
            service_name="ollama",
        )
        url = f"http://{_MANAGED_RUNTIME_OLLAMA_CONTAINER}:{_MANAGED_RUNTIME_OLLAMA_PORT}"
        health = _managed_runtime_health_wait(bundle_type, url, _managed_runtime_health_check_ollama, attempts=30, sleep_sec=3.0)
        if not bool(health.get("available")):
            health = _managed_runtime_health_wait(bundle_type, f"http://127.0.0.1:{_MANAGED_RUNTIME_OLLAMA_PORT}", _managed_runtime_health_check_ollama, attempts=6, sleep_sec=3.0)
            if bool(health.get("available")):
                url = f"http://127.0.0.1:{_MANAGED_RUNTIME_OLLAMA_PORT}"
        if not bool(health.get("available")) and bool(active_profile.get("active")):
            failure_detail = str(health.get("message") or "Ollama did not become healthy with GPU acceleration")
            _managed_runtime_log(
                bundle_type,
                f"GPU acceleration health check failed for mode {active_profile.get('selected_mode')}: {failure_detail}. Retrying on CPU.",
                level="warning",
                service_name="ollama",
            )
            subprocess.run([docker_cli, "rm", "-f", _MANAGED_RUNTIME_OLLAMA_CONTAINER], capture_output=True, text=True, timeout=20)
            active_profile = _managed_runtime_ollama_gpu_profile(requested_mode=_MANAGED_RUNTIME_OLLAMA_GPU_MODE_CPU)
            active_profile["fallback_reason"] = failure_detail
            _managed_runtime_bundle_upsert(
                bundle_type,
                state="creating",
                phase="creating",
                phase_message="GPU health check failed, retrying managed Ollama on CPU",
                meta={"gpu": active_profile},
            )
            start_res = subprocess.run(_build_run_cmd(active_profile), capture_output=True, text=True, timeout=30)
            if start_res.returncode != 0:
                raise RuntimeError((start_res.stderr or start_res.stdout or "Failed to start managed Ollama container after GPU fallback").strip())
            url = f"http://{_MANAGED_RUNTIME_OLLAMA_CONTAINER}:{_MANAGED_RUNTIME_OLLAMA_PORT}"
            health = _managed_runtime_health_wait(bundle_type, url, _managed_runtime_health_check_ollama, attempts=30, sleep_sec=3.0)
            if not bool(health.get("available")):
                health = _managed_runtime_health_wait(bundle_type, f"http://127.0.0.1:{_MANAGED_RUNTIME_OLLAMA_PORT}", _managed_runtime_health_check_ollama, attempts=6, sleep_sec=3.0)
                if bool(health.get("available")):
                    url = f"http://127.0.0.1:{_MANAGED_RUNTIME_OLLAMA_PORT}"
        if not bool(health.get("available")):
            raise RuntimeError(str(health.get("message") or "Ollama did not become healthy"))
        _managed_runtime_bundle_upsert(
            bundle_type,
            state="pulling",
            phase="pulling",
            phase_message=f"Pulling {fast_model}",
            effective_url=url,
            health=health,
            meta={"gpu": active_profile},
        )
        final_health = _managed_runtime_ensure_ollama_models(url, [fast_model, hard_model], bundle_type=bundle_type)
        bundle = _managed_runtime_bundle_upsert(
            bundle_type,
            mode="managed",
            state="ready",
            phase="ready",
            phase_message="Ollama ready",
            config_root=config_root,
            data_root=data_root,
            effective_url=url,
            ownership=_MANAGED_RUNTIME_OLLAMA_CONTAINER,
            health=final_health,
            services=[
                {
                    "name": "ollama",
                    "status": "healthy" if bool(final_health.get("available")) else "degraded",
                    "message": f"{str(final_health.get('message') or '')} • {active_profile.get('mode_label') or 'CPU'}".strip(" •"),
                }
            ],
            meta={
                "models": list(final_health.get("models") or []),
                "model_count": int(final_health.get("model_count") or 0),
                "gpu": active_profile,
            },
            last_error="",
        )
        _managed_runtime_apply_ollama_runtime("managed", url, fast_model, hard_model, config_root=config_root, data_root=data_root)
        _settings_db_set_value("MANAGED_RUNTIME_CONFIG_ROOT", config_root)
        _settings_db_set_value("MANAGED_RUNTIME_DATA_ROOT", data_root)
        _managed_runtime_action_update(action_id, bundle_type, "bootstrap", "completed", payload=payload, result=bundle, completed=True)
    except Exception as exc:
        message = str(exc or "Ollama bootstrap failed")
        _managed_runtime_bundle_upsert(bundle_type, state="failed", phase="failed", phase_message=message, last_error=message)
        _managed_runtime_action_update(action_id, bundle_type, "bootstrap", "failed", payload=payload, error=message, completed=True)
        _managed_runtime_log(bundle_type, message, level="error")
        raise

def _managed_runtime_bootstrap_worker(bundle_type: str, payload: dict[str, Any]) -> None:
    try:
        if bundle_type == _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE:
            _managed_runtime_bootstrap_musicbrainz(payload)
        elif bundle_type == _MANAGED_RUNTIME_OLLAMA_BUNDLE:
            _managed_runtime_bootstrap_ollama(payload)
        else:
            raise RuntimeError(f"Unsupported managed runtime bundle: {bundle_type}")
    finally:
        with _MANAGED_RUNTIME_LOCK:
            _MANAGED_RUNTIME_THREADS.pop(bundle_type, None)

def _managed_runtime_launch_bootstrap(bundle_type: str, payload: dict[str, Any]) -> tuple[bool, str]:
    with _MANAGED_RUNTIME_LOCK:
        existing = _MANAGED_RUNTIME_THREADS.get(bundle_type)
        if existing is not None and existing.is_alive():
            return False, "bootstrap already running"
        worker = threading.Thread(
            target=_managed_runtime_bootstrap_worker,
            args=(bundle_type, dict(payload or {})),
            daemon=True,
            name=f"managed-runtime-{bundle_type}",
        )
        _MANAGED_RUNTIME_THREADS[bundle_type] = worker
        worker.start()
    return True, "started"

def _managed_runtime_bundle_status(bundle_type: str, *, include_candidates: bool = True) -> dict[str, Any]:
    bundle = _managed_runtime_bundle_get(bundle_type)
    latest_action = _managed_runtime_get_latest_action(bundle_type)
    running = False
    with _MANAGED_RUNTIME_LOCK:
        thread = _MANAGED_RUNTIME_THREADS.get(bundle_type)
        running = bool(thread is not None and thread.is_alive())
    if bundle_type == _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE:
        candidates = _managed_runtime_detect_musicbrainz_candidates() if include_candidates else []
        if bundle.get("effective_url"):
            bundle["health"] = _managed_runtime_health_check_musicbrainz(bundle.get("effective_url") or "")
        healthy_candidate = next((row for row in candidates if bool((row.get("health") or {}).get("available")) and bool(row.get("published_url"))), None)
        health_available = bool((bundle.get("health") or {}).get("available"))
        if not running and healthy_candidate is not None and (
            str(bundle.get("state") or "") in _MANAGED_RUNTIME_ACTIVE_STATES
            or not str(bundle.get("effective_url") or "").strip()
            or not health_available
        ):
            services_present = list(healthy_candidate.get("services_present") or [])
            phase_message = "MusicBrainz mirror ready"
            candidate_install_root = str(healthy_candidate.get("install_root") or "").strip()
            bundle_install_root = str(bundle.get("install_root") or "").strip()
            bundle = _managed_runtime_bundle_upsert_best_effort(
                bundle_type,
                fallback=bundle,
                mode=str(bundle.get("mode") or "managed"),
                state="ready",
                phase="ready",
                phase_message=phase_message,
                install_root=bundle_install_root or candidate_install_root,
                effective_url=str(healthy_candidate.get("published_url") or ""),
                ownership=str(healthy_candidate.get("project_name") or bundle.get("ownership") or ""),
                health=dict(healthy_candidate.get("health") or {}),
                services=[
                    {"name": service_name, "status": "healthy", "message": "Detected on this server"}
                    for service_name in services_present
                ],
                last_error="",
            )
            if latest_action and str(latest_action.get("status") or "") == "running":
                _managed_runtime_action_update(
                    str(latest_action.get("action_id") or ""),
                    bundle_type,
                    str(latest_action.get("action") or "bootstrap"),
                    "completed",
                    payload=dict(latest_action.get("payload") or {}),
                    result=bundle,
                    completed=True,
                )
        bundle["update_state"] = _managed_runtime_register_mb_update_schedule(bundle)
        bundle = _managed_runtime_bundle_upsert_best_effort(
            bundle_type,
            fallback=bundle,
            health=bundle.get("health") or {},
            update_state=bundle.get("update_state") or {},
        )
    elif bundle_type == _MANAGED_RUNTIME_OLLAMA_BUNDLE:
        candidates = _managed_runtime_detect_ollama_candidates() if include_candidates else []
        if bundle.get("effective_url"):
            bundle["health"] = _managed_runtime_health_check_ollama(bundle.get("effective_url") or "")
        healthy_candidate = next((row for row in candidates if bool((row.get("health") or {}).get("available")) and bool(row.get("url"))), None)
        health_available = bool((bundle.get("health") or {}).get("available"))
        if not running and healthy_candidate is not None and (
            str(bundle.get("state") or "") in _MANAGED_RUNTIME_ACTIVE_STATES
            or not str(bundle.get("effective_url") or "").strip()
            or not health_available
        ):
            models = list(healthy_candidate.get("models") or [])
            candidate_container = str(healthy_candidate.get("container_name") or "").strip()
            candidate_mode = (
                "managed"
                if candidate_container == _MANAGED_RUNTIME_OLLAMA_CONTAINER
                else "adopted"
            )
            bundle = _managed_runtime_bundle_upsert_best_effort(
                bundle_type,
                fallback=bundle,
                mode=candidate_mode,
                state="ready",
                phase="ready",
                phase_message="Ollama ready",
                effective_url=str(healthy_candidate.get("url") or ""),
                ownership=candidate_container or str(healthy_candidate.get("id") or ""),
                health={
                    "available": True,
                    "overall_status": "healthy",
                    "message": "Ollama reachable",
                    "models": models,
                    "model_count": len(models),
                },
                services=[{"name": "ollama", "status": "healthy", "message": f"{len(models)} model(s) available"}],
                meta={
                    **dict(bundle.get("meta") or {}),
                    "models": models,
                    "model_count": len(models),
                    "source": str(healthy_candidate.get("source") or ""),
                    "container_name": candidate_container,
                    "networks": list(healthy_candidate.get("networks") or []),
                    "aliases": list(healthy_candidate.get("aliases") or []),
                },
                last_error="",
            )
            if latest_action and str(latest_action.get("status") or "") == "running":
                _managed_runtime_action_update(
                    str(latest_action.get("action_id") or ""),
                    bundle_type,
                    str(latest_action.get("action") or "bootstrap"),
                    "completed",
                    payload=dict(latest_action.get("payload") or {}),
                    result=bundle,
                    completed=True,
                )
        bundle = _managed_runtime_bundle_upsert_best_effort(
            bundle_type,
            fallback=bundle,
            health=bundle.get("health") or {},
        )
    else:
        candidates = []
    bundle["active"] = running or str(bundle.get("state") or "") in _MANAGED_RUNTIME_ACTIVE_STATES
    bundle["latest_action"] = latest_action
    bundle["candidates"] = candidates
    return bundle

def _managed_runtime_status_snapshot(*, include_candidates: bool = True) -> dict[str, Any]:
    config_root = str(_get_config_from_db("MANAGED_RUNTIME_CONFIG_ROOT", "") or "").strip()
    data_root = str(_get_config_from_db("MANAGED_RUNTIME_DATA_ROOT", "") or "").strip()
    status = {
        "preflight": _managed_runtime_preflight(),
        "config_root": config_root,
        "data_root": data_root,
        "bundles": {
            _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE: _managed_runtime_bundle_status(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, include_candidates=include_candidates),
            _MANAGED_RUNTIME_OLLAMA_BUNDLE: _managed_runtime_bundle_status(_MANAGED_RUNTIME_OLLAMA_BUNDLE, include_candidates=include_candidates),
        },
    }
    status["ready"] = all(
        bool((status["bundles"].get(bundle_type) or {}).get("state") in _MANAGED_RUNTIME_READY_STATES)
        for bundle_type in _MANAGED_RUNTIME_BUNDLE_TYPES
        if str((status["bundles"].get(bundle_type) or {}).get("mode") or "absent") not in {"absent", "external"}
    )
    return status

def _managed_runtime_musicbrainz_update_due(bundle: dict[str, Any]) -> bool:
    update_state = dict(bundle.get("update_state") or {})
    if not bool(update_state.get("enabled", True)):
        return False
    next_planned_at = float(update_state.get("next_planned_at") or 0.0)
    if next_planned_at <= 0:
        next_planned_at = time.time() + _MANAGED_RUNTIME_MB_DEFAULT_UPDATE_INTERVAL_SEC
    return time.time() >= next_planned_at

def _managed_runtime_run_musicbrainz_update(*, allow_during_scan: bool = False, repair: bool = False) -> tuple[bool, str, dict]:
    bundle = _managed_runtime_bundle_get(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE)
    if not repair and str(bundle.get("state") or "") not in _MANAGED_RUNTIME_READY_STATES:
        return True, "Managed MusicBrainz update skipped (bundle not ready)", {"status": "skipped"}
    mode = str(bundle.get("mode") or "").strip().lower() or "absent"
    repairable_adopted = bool(repair and mode == "adopted")
    if mode != "managed" and not repairable_adopted:
        update_state = _managed_runtime_register_mb_update_schedule(bundle)
        bundle = _managed_runtime_bundle_upsert(
            _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
            update_state=update_state,
            last_error="",
        )
        return True, f"Managed MusicBrainz update skipped (bundle mode: {mode})", {"status": "skipped", "update_state": bundle.get("update_state") or update_state}
    install_root = _managed_runtime_resolve_musicbrainz_install_root(bundle)
    if not install_root:
        message = "MusicBrainz install root is unknown; repair requires a local docker-compose checkout"
        update_state = dict(bundle.get("update_state") or {})
        update_state["last_failure_at"] = time.time()
        update_state["last_error"] = message
        update_state["next_planned_at"] = time.time() + 3600.0
        _managed_runtime_bundle_upsert(
            _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
            state="failed",
            phase="failed",
            phase_message=message,
            update_state=update_state,
            last_error=message,
        )
        _managed_runtime_log(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, message, level="error")
        return False, message, {"update_state": update_state}
    if str(bundle.get("install_root") or "").strip() != install_root:
        bundle = _managed_runtime_bundle_upsert(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, install_root=install_root)
    with lock:
        scanning_now = bool(state.get("scanning") or state.get("scan_starting") or state.get("scan_finalizing"))
    if scanning_now and not allow_during_scan:
        update_state = dict(bundle.get("update_state") or {})
        update_state["last_deferred_at"] = time.time()
        update_state["last_deferred_reason"] = "scan_active"
        update_state["next_planned_at"] = time.time() + 900.0
        _managed_runtime_bundle_upsert(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, update_state=update_state)
        return False, "Managed MusicBrainz update deferred until scans are idle", {"deferred": True}
    script_name = "musicbrainz_mirror_restore_search_archives.sh" if repair else "musicbrainz_mirror_reindex.sh"
    script_path = Path(__file__).resolve().parent / "scripts" / script_name
    if not script_path.exists():
        return False, f"MusicBrainz maintenance script not found: {script_path}", {}
    phase_message = "Restoring MusicBrainz prebuilt search indexes" if repair else "Running scheduled MusicBrainz search reindex"
    meta = dict(bundle.get("meta") or {})
    for stale_key in (
        "download_progress",
        "elapsed_seconds",
        "eta_seconds",
        "eta_text",
        "last_heartbeat_at",
        "last_output",
        "last_output_age_seconds",
        "progress",
        "subprocess_running",
        "completed",
        "total",
        "current_model",
    ):
        meta.pop(stale_key, None)
    meta["operation"] = "repair-search-index" if repair else "musicbrainz-update"
    meta["started_at"] = time.time()
    _managed_runtime_bundle_upsert(
        _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
        state="updating",
        phase="repairing" if repair else "updating",
        phase_message=phase_message,
        meta=meta,
        last_error="",
    )
    try:
        _managed_runtime_capture_subprocess(
            [str(script_path), "--install-root", install_root],
            bundle_type=_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
            service_name="musicbrainz",
            phase_map=[
                ("Fetching prebuilt search index archives", "repairing", "Downloading MusicBrainz prebuilt search indexes"),
                ("Loading prebuilt search index archives", "repairing", "Loading MusicBrainz prebuilt search indexes"),
                ("Removing downloaded search backup archives", "repairing", "Cleaning MusicBrainz search archive cache"),
                ("reindex", "updating", "Reindexing MusicBrainz search"),
            ],
            heartbeat_label="MusicBrainz search archive restore" if repair else "MusicBrainz SIR reindex",
            heartbeat_interval_sec=60.0,
        )
        update_state = _managed_runtime_register_mb_update_schedule(bundle)
        now_ts = time.time()
        update_state["last_success_at"] = now_ts
        update_state["last_error"] = ""
        update_state["next_planned_at"] = time.time() + int(update_state.get("interval_sec") or _MANAGED_RUNTIME_MB_DEFAULT_UPDATE_INTERVAL_SEC)
        if repair:
            update_state["last_repair_at"] = now_ts
        health_url = str(bundle.get("effective_url") or MUSICBRAINZ_BASE_URL or "").strip()
        health = _managed_runtime_health_check_musicbrainz(health_url) if health_url else {}
        health_ok = bool((health or {}).get("available"))
        ready_message = "MusicBrainz mirror ready" if health_ok else str((health or {}).get("message") or "MusicBrainz reindex finished but health is still degraded")
        bundle = _managed_runtime_bundle_upsert(
            _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
            state="ready" if health_ok else "failed",
            phase="ready" if health_ok else "failed",
            phase_message=ready_message,
            health=health,
            update_state=update_state,
            last_error="" if health_ok else ready_message,
        )
        if not health_ok:
            return False, ready_message, {"update_state": bundle.get("update_state") or {}, "health": health}
        return True, "Managed MusicBrainz search repair finished" if repair else "Managed MusicBrainz maintenance finished", {"update_state": bundle.get("update_state") or {}, "health": health}
    except Exception as exc:
        message = str(exc or ("Managed MusicBrainz search repair failed" if repair else "Managed MusicBrainz maintenance failed"))
        update_state = dict(bundle.get("update_state") or {})
        update_state["last_failure_at"] = time.time()
        update_state["last_error"] = message
        update_state["next_planned_at"] = time.time() + 3600.0
        _managed_runtime_bundle_upsert(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, state="failed", phase="failed", phase_message=message, update_state=update_state, last_error=message)
        _managed_runtime_log(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, message, level="error")
        return False, message, {"update_state": update_state}

def _managed_runtime_musicbrainz_repair_worker(action_id: str, payload: dict[str, Any]) -> None:
    bundle_type = _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE
    _managed_runtime_action_update(action_id, bundle_type, "repair-search-index", "running", payload=payload)
    try:
        ok, message, result = _managed_runtime_run_musicbrainz_update(allow_during_scan=True, repair=True)
        _managed_runtime_action_update(
            action_id,
            bundle_type,
            "repair-search-index",
            "completed" if ok else "failed",
            payload=payload,
            result=result,
            error="" if ok else message,
            completed=True,
        )
        if not ok:
            _managed_runtime_log(bundle_type, message, level="error", service_name="musicbrainz")
    except Exception as exc:
        message = str(exc or "MusicBrainz search repair failed")
        _managed_runtime_bundle_upsert(bundle_type, state="failed", phase="failed", phase_message=message, last_error=message)
        _managed_runtime_action_update(action_id, bundle_type, "repair-search-index", "failed", payload=payload, error=message, completed=True)
        _managed_runtime_log(bundle_type, message, level="error", service_name="musicbrainz")
    finally:
        with _MANAGED_RUNTIME_LOCK:
            _MANAGED_RUNTIME_THREADS.pop(bundle_type, None)

def _managed_runtime_launch_musicbrainz_search_repair(payload: dict[str, Any]) -> tuple[bool, str, str | None]:
    bundle_type = _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE
    action_id = str((payload or {}).get("action_id") or uuid.uuid4())
    with _MANAGED_RUNTIME_LOCK:
        existing = _MANAGED_RUNTIME_THREADS.get(bundle_type)
        if existing is not None and existing.is_alive():
            return False, "MusicBrainz runtime action already running", None
        worker = threading.Thread(
            target=_managed_runtime_musicbrainz_repair_worker,
            args=(action_id, dict(payload or {})),
            daemon=True,
            name="managed-runtime-musicbrainz-repair",
        )
        _MANAGED_RUNTIME_THREADS[bundle_type] = worker
        worker.start()
    return True, "MusicBrainz search index repair started", action_id

def _managed_runtime_maybe_enqueue_due_jobs(now_ts: float | None = None) -> None:
    now = float(now_ts or time.time())
    bundle = _managed_runtime_bundle_get(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE)
    if str(bundle.get("mode") or "absent") != "managed":
        return
    if str(bundle.get("state") or "") not in _MANAGED_RUNTIME_READY_STATES:
        return
    update_state = _managed_runtime_register_mb_update_schedule(bundle)
    bundle = _managed_runtime_bundle_upsert(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, update_state=update_state)
    if not bool((bundle.get("update_state") or {}).get("enabled", True)):
        return
    next_planned_at = float((bundle.get("update_state") or {}).get("next_planned_at") or 0.0)
    if next_planned_at <= 0 or now < next_planned_at:
        return
    with lock:
        scan_active = bool(state.get("scanning") or state.get("scan_starting") or state.get("scan_finalizing"))
    if scan_active:
        update_state = dict(bundle.get("update_state") or {})
        last_deferred_at = float(update_state.get("last_deferred_at") or 0.0)
        if now - last_deferred_at >= 300.0:
            update_state["last_deferred_at"] = now
            update_state["last_deferred_reason"] = "scan_active"
            update_state["next_planned_at"] = now + 900.0
            _managed_runtime_bundle_upsert(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, update_state=update_state)
        return
    _scheduler_launch_job("managed_musicbrainz_update", "both", "managed_runtime", max_concurrency=1)

def _managed_runtime_resolve_candidate(bundle_type: str, candidate_id: str = "", *, url: str = "", project_name: str = "") -> dict[str, Any] | None:
    cid = str(candidate_id or "").strip()
    normalized_url = str(url or "").strip()
    project = str(project_name or "").strip()
    if bundle_type == _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE:
        candidates = _managed_runtime_detect_musicbrainz_candidates()
        for candidate in candidates:
            if cid and str(candidate.get("id") or "") == cid:
                return candidate
            if project and str(candidate.get("project_name") or "") == project:
                return candidate
            if normalized_url and normalized_url in {
                str(candidate.get("published_url") or "").strip(),
                str((candidate.get("health") or {}).get("url") or "").strip(),
            }:
                return candidate
        return None
    if bundle_type == _MANAGED_RUNTIME_OLLAMA_BUNDLE:
        candidates = _managed_runtime_detect_ollama_candidates()
        for candidate in candidates:
            if cid and str(candidate.get("id") or "") == cid:
                return candidate
            if normalized_url and normalized_url == str(candidate.get("url") or "").strip():
                return candidate
        return None
    return None

def _managed_runtime_mb_compose_cmd(install_root: str, *args: str) -> list[str]:
    compose_cli = _managed_runtime_compose_cli()
    if not compose_cli:
        raise RuntimeError("Docker Compose is missing from the PMDA container")
    root = str(install_root or "").strip()
    if not root:
        raise RuntimeError("Managed MusicBrainz install root is missing")
    compose_file = Path(root) / "docker-compose.yml"
    if not compose_file.exists():
        raise RuntimeError(f"MusicBrainz compose file not found under {root}")
    if len(compose_cli) >= 2 and compose_cli[1] == "compose":
        return [*compose_cli, "--project-directory", root, *args]
    return [*compose_cli, "--project-directory", root, *args]

def _managed_runtime_start_musicbrainz_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    install_root = str(bundle.get("install_root") or "").strip()
    _managed_runtime_capture_subprocess(
        _managed_runtime_mb_compose_cmd(install_root, "up", "-d"),
        bundle_type=_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
        service_name="musicbrainz",
        phase_map=[("starting", "creating", "Starting MusicBrainz bundle")],
    )
    _managed_runtime_connect_self_to_network()
    return _managed_runtime_bundle_status(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, include_candidates=False)

def _managed_runtime_stop_musicbrainz_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    install_root = str(bundle.get("install_root") or "").strip()
    _managed_runtime_capture_subprocess(
        _managed_runtime_mb_compose_cmd(install_root, "stop"),
        bundle_type=_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
        service_name="musicbrainz",
        phase_map=[("Stopping", "creating", "Stopping MusicBrainz bundle")],
    )
    return _managed_runtime_bundle_upsert(
        _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
        state="idle",
        phase="idle",
        phase_message="MusicBrainz bundle stopped",
        health={"available": False, "overall_status": "stopped", "message": "Stopped"},
    )

def _managed_runtime_restart_musicbrainz_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    install_root = str(bundle.get("install_root") or "").strip()
    _managed_runtime_capture_subprocess(
        _managed_runtime_mb_compose_cmd(install_root, "restart"),
        bundle_type=_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
        service_name="musicbrainz",
        phase_map=[("Restarting", "creating", "Restarting MusicBrainz bundle")],
    )
    _managed_runtime_connect_self_to_network()
    return _managed_runtime_bundle_status(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, include_candidates=False)

def _managed_runtime_reset_musicbrainz_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    install_root = str(bundle.get("install_root") or "").strip()
    if install_root:
        _managed_runtime_capture_subprocess(
            _managed_runtime_mb_compose_cmd(install_root, "down"),
            bundle_type=_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
            service_name="musicbrainz",
            phase_map=[("Stopping", "creating", "Stopping MusicBrainz bundle")],
        )
    cleared = _managed_runtime_bundle_upsert(
        _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE,
        mode="absent",
        state="idle",
        phase="idle",
        phase_message="MusicBrainz managed runtime reset",
        effective_url="",
        install_root="",
        ownership="",
        health={"available": False, "overall_status": "absent", "message": "Reset"},
        services=[],
        meta={},
        last_error="",
    )
    return cleared

def _managed_runtime_start_ollama_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    docker_cli = _managed_runtime_docker_cli()
    if not docker_cli:
        raise RuntimeError("docker CLI is missing from the PMDA container")
    res = subprocess.run([docker_cli, "start", _MANAGED_RUNTIME_OLLAMA_CONTAINER], capture_output=True, text=True, timeout=20)
    if res.returncode != 0:
        raise RuntimeError((res.stderr or res.stdout or "Failed to start managed Ollama").strip())
    _managed_runtime_connect_self_to_network()
    return _managed_runtime_bundle_status(_MANAGED_RUNTIME_OLLAMA_BUNDLE, include_candidates=False)

def _managed_runtime_stop_ollama_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    docker_cli = _managed_runtime_docker_cli()
    if not docker_cli:
        raise RuntimeError("docker CLI is missing from the PMDA container")
    res = subprocess.run([docker_cli, "stop", _MANAGED_RUNTIME_OLLAMA_CONTAINER], capture_output=True, text=True, timeout=20)
    if res.returncode != 0:
        raise RuntimeError((res.stderr or res.stdout or "Failed to stop managed Ollama").strip())
    return _managed_runtime_bundle_upsert(
        _MANAGED_RUNTIME_OLLAMA_BUNDLE,
        state="idle",
        phase="idle",
        phase_message="Ollama runtime stopped",
        health={"available": False, "overall_status": "stopped", "message": "Stopped"},
    )

def _managed_runtime_restart_ollama_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    docker_cli = _managed_runtime_docker_cli()
    if not docker_cli:
        raise RuntimeError("docker CLI is missing from the PMDA container")
    res = subprocess.run([docker_cli, "restart", _MANAGED_RUNTIME_OLLAMA_CONTAINER], capture_output=True, text=True, timeout=20)
    if res.returncode != 0:
        raise RuntimeError((res.stderr or res.stdout or "Failed to restart managed Ollama").strip())
    _managed_runtime_connect_self_to_network()
    return _managed_runtime_bundle_status(_MANAGED_RUNTIME_OLLAMA_BUNDLE, include_candidates=False)

def _managed_runtime_reset_ollama_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    docker_cli = _managed_runtime_docker_cli()
    if not docker_cli:
        raise RuntimeError("docker CLI is missing from the PMDA container")
    subprocess.run([docker_cli, "rm", "-f", _MANAGED_RUNTIME_OLLAMA_CONTAINER], capture_output=True, text=True, timeout=20)
    cleared = _managed_runtime_bundle_upsert(
        _MANAGED_RUNTIME_OLLAMA_BUNDLE,
        mode="absent",
        state="idle",
        phase="idle",
        phase_message="Ollama managed runtime reset",
        effective_url="",
        ownership="",
        health={"available": False, "overall_status": "absent", "message": "Reset"},
        services=[],
        meta={},
        last_error="",
    )
    return cleared

def _ollama_pull_status_snapshot() -> dict[str, Any]:
    with _OLLAMA_PULL_STATUS_LOCK:
        return dict(_OLLAMA_PULL_STATUS)

def _ollama_pull_status_update(**fields: Any) -> dict[str, Any]:
    with _OLLAMA_PULL_STATUS_LOCK:
        _OLLAMA_PULL_STATUS.update(fields)
        _OLLAMA_PULL_STATUS["updated_at"] = time.time()
        return dict(_OLLAMA_PULL_STATUS)

def _run_ollama_pull_async(url: str, model_name: str) -> None:
    started_at = time.time()
    _ollama_pull_status_update(
        active=True,
        status="starting",
        message=f"Pulling {model_name}…",
        model=model_name,
        url=url,
        completed=0,
        total=0,
        progress=0.0,
        started_at=started_at,
        finished_at=None,
        error="",
        digest="",
    )
    try:
        response = requests.post(
            f"{url.rstrip('/')}/api/pull",
            json={"name": model_name, "stream": True},
            stream=True,
            timeout=(10, 900),
        )
        if response.status_code != 200:
            detail = response.text[:400] if response.text else f"HTTP {response.status_code}"
            raise RuntimeError(f"Ollama pull failed: {detail}")
        for raw_line in response.iter_lines(decode_unicode=True):
            if not raw_line:
                continue
            try:
                payload = json.loads(raw_line)
            except Exception:
                continue
            status_text = str(payload.get("status") or "").strip() or "pulling"
            completed = int(payload.get("completed") or 0)
            total = int(payload.get("total") or 0)
            digest = str(payload.get("digest") or "").strip()
            progress = 0.0
            if total > 0:
                progress = max(0.0, min(100.0, (completed / total) * 100.0))
            if payload.get("error"):
                raise RuntimeError(str(payload.get("error")))
            done = bool(payload.get("completed") and payload.get("total") and completed >= total)
            _ollama_pull_status_update(
                active=not done,
                status="completed" if done else status_text,
                message=status_text,
                completed=completed,
                total=total,
                progress=progress,
                digest=digest,
                finished_at=time.time() if done else None,
            )
        if not _ollama_model_exists(url, model_name):
            raise RuntimeError(f"Ollama did not report {model_name} after pull")
        _ollama_pull_status_update(
            active=False,
            status="completed",
            message=f"{model_name} is ready",
            progress=100.0,
            finished_at=time.time(),
        )
    except Exception as exc:
        logging.error("Ollama pull failed for %s via %s: %s", model_name, url, exc)
        _ollama_pull_status_update(
            active=False,
            status="error",
            message=str(exc),
            error=str(exc),
            finished_at=time.time(),
        )

def api_musicbrainz_test():
    """Test MusicBrainz connectivity and rate limiting.
    Returns success status and any error messages.
    Accepts USE_MUSICBRAINZ in request body (POST) to allow testing before config is saved."""
    # Check if USE_MUSICBRAINZ is provided in request body (for POST) or use global config
    data = request.get_json(silent=True) or {}
    use_mb = data.get("USE_MUSICBRAINZ")
    if use_mb is not None:
        # Use value from request body if provided
        use_mb_enabled = bool(use_mb)
    else:
        # Fall back to global config
        use_mb_enabled = USE_MUSICBRAINZ

    if not use_mb_enabled:
        return jsonify({"success": False, "message": "MusicBrainz is disabled. Enable it first."}), 400
    target = _musicbrainz_target_settings()

    try:
        # Test with a well-known release-group ID
        test_mbid = "9162580e-5df4-32de-80cc-f45a8d8a9b1d"  # The Beatles - Abbey Road
        result = musicbrainzngs.get_release_group_by_id(test_mbid, includes=[])
        if result and result.get("release-group"):
            return jsonify({
                "success": True,
                "message": "MusicBrainz connection successful",
                "tested_mbid": test_mbid,
                "base_url": target["base_url"],
                "mirror_enabled": bool(target["enabled"]),
                "mirror_name": str(target["mirror_name"] or ""),
            })
        else:
            return jsonify({"success": False, "message": "MusicBrainz returned empty response"}), 500
    except musicbrainzngs.WebServiceError as e:
        error_msg = str(e)
        logging.warning("MusicBrainz WebServiceError: %s", error_msg)
        # Check for specific error codes
        if hasattr(e, 'code'):
            error_code = str(e.code)
            if error_code == "503" or "rate" in error_msg.lower():
                return jsonify({
                    "success": False,
                    "message": "MusicBrainz rate limited. Please wait a moment and try again. Rate limit: 1 request per second."
                }), 503
            elif error_code == "404" or "404" in error_msg:
                return jsonify({
                    "success": False,
                    "message": f"MusicBrainz returned 404 (Not Found). This may be a temporary issue. Error: {error_msg}"
                }), 404
            elif error_code == "503":
                return jsonify({
                    "success": False,
                    "message": "MusicBrainz service temporarily unavailable (503). Please try again later."
                }), 503
        # Fallback to message-based detection
        if "503" in error_msg or "rate" in error_msg.lower() or "service unavailable" in error_msg.lower():
            return jsonify({
                "success": False,
                "message": "MusicBrainz rate limited or service unavailable. Please wait a moment and try again. Rate limit: 1 request per second."
            }), 503
        elif "404" in error_msg or "not found" in error_msg.lower():
            return jsonify({
                "success": False,
                "message": f"MusicBrainz API returned 404. This may be a temporary issue or network problem. Error details: {error_msg}"
            }), 404
        else:
            logging.warning("MusicBrainz test failed: %s", error_msg)
            return jsonify({
                "success": False,
                "message": f"MusicBrainz API error: {error_msg}"
            }), 500
    except Exception as e:
        error_msg = str(e)
        logging.error("MusicBrainz test exception: %s", error_msg)
        if "connection" in error_msg.lower() or "timeout" in error_msg.lower():
            return jsonify({
                "success": False,
                "message": "Connection to MusicBrainz failed. Please check your internet connection."
            }), 503
        return jsonify({
            "success": False,
            "message": f"Error: {error_msg}"
        }), 500
def api_ollama_models():
    """Return list of Ollama model IDs available at the provided URL."""
    from flask import g
    data = getattr(g, 'ai_models_request_data', None) or request.get_json(silent=True) or {}
    url = (data.get("OLLAMA_URL") or "").strip() or OLLAMA_URL

    if not url:
        return jsonify({"error": "OLLAMA_URL is required"}), 400

    # Normalize URL (remove trailing slash)
    url = url.rstrip("/")

    try:
        # Test connection and fetch models
        models_endpoint = f"{url}/api/tags"
        response = requests.get(models_endpoint, timeout=10)

        if response.status_code == 404:
            return jsonify({"error": "Ollama API not found at this URL. Make sure Ollama is running and the URL is correct."}), 404
        elif response.status_code != 200:
            return jsonify({"error": f"Failed to connect to Ollama: HTTP {response.status_code}"}), response.status_code

        models_data = response.json()
        available_models = []

        if "models" in models_data:
            for model in models_data["models"]:
                model_name = model.get("name", "")
                if model_name:
                    available_models.append(model_name)

        if not available_models:
            logging.warning("Ollama returned no models")
            return jsonify({"error": "No models available at this Ollama instance. Please pull some models first."}), 404

        # Sort models alphabetically
        available_models.sort()
        logging.info("Fetched %d Ollama models from %s", len(available_models), url)
        return jsonify(available_models)

    except requests.exceptions.Timeout:
        return jsonify({"error": "Connection to Ollama timed out. Make sure Ollama is running and accessible."}), 503
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Failed to connect to Ollama. Make sure Ollama is running and the URL is correct."}), 503
    except Exception as e:
        error_msg = str(e)
        logging.error("Failed to fetch Ollama models: %s", error_msg)
        return jsonify({"error": f"Failed to fetch models: {error_msg}"}), 500
def _normalize_ollama_probe_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if not re.match(r"^https?://", raw, re.IGNORECASE):
        raw = f"http://{raw}"
    parsed = urlparse(raw)
    scheme = parsed.scheme or "http"
    host = parsed.hostname or ""
    if not host:
        return ""
    port = parsed.port or 11434
    return f"{scheme}://{host}:{port}"
def _ollama_probe(url: str) -> dict[str, Any]:
    probe_url = _normalize_ollama_probe_url(url)
    if not probe_url:
        return {
            "url": str(url or ""),
            "ok": False,
            "message": "Invalid URL",
            "models": [],
            "model_count": 0,
        }
    try:
        response = requests.get(f"{probe_url}/api/tags", timeout=5)
        if response.status_code != 200:
            return {
                "url": probe_url,
                "ok": False,
                "message": f"HTTP {response.status_code}",
                "models": [],
                "model_count": 0,
            }
        payload = response.json() if response.content else {}
        models_raw = payload.get("models") or []
        models = sorted(
            [
                str(item.get("name") or "").strip()
                for item in models_raw
                if isinstance(item, dict) and str(item.get("name") or "").strip()
            ]
        )
        message = f"{len(models)} model(s) available" if models else "Connected, but no models are installed yet"
        return {
            "url": probe_url,
            "ok": True,
            "message": message,
            "models": models,
            "model_count": len(models),
        }
    except requests.exceptions.Timeout:
        return {
            "url": probe_url,
            "ok": False,
            "message": "Timed out",
            "models": [],
            "model_count": 0,
        }
    except requests.exceptions.ConnectionError:
        return {
            "url": probe_url,
            "ok": False,
            "message": "Connection failed",
            "models": [],
            "model_count": 0,
        }
    except Exception as exc:
        return {
            "url": probe_url,
            "ok": False,
            "message": str(exc or "Probe failed"),
            "models": [],
            "model_count": 0,
        }
def api_ollama_discover():
    candidates: list[str] = []
    seen: set[str] = set()

    def _add(url: str) -> None:
        normalized = _normalize_ollama_probe_url(url)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        candidates.append(normalized)

    configured = str(request.args.get("url") or "").strip() or str(getattr(sys.modules[__name__], "OLLAMA_URL", "") or "").strip()
    if configured:
        _add(configured)
    for raw in (
        "http://127.0.0.1:11434",
        "http://localhost:11434",
        "http://host.docker.internal:11434",
        "http://ollama:11434",
        "http://pmda-ollama:11434",
    ):
        _add(raw)
    for ip_text in _local_network_ipv4_candidates():
        _add(f"http://{ip_text}:11434")

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(candidates)))) as pool:
        future_map = {pool.submit(_ollama_probe, url): url for url in candidates}
        for future in as_completed(future_map):
            try:
                results.append(dict(future.result() or {}))
            except Exception as exc:
                url = future_map.get(future) or ""
                results.append({"url": url, "ok": False, "message": str(exc or "Probe failed"), "models": [], "model_count": 0})

    results.sort(key=lambda row: (not bool(row.get("ok")), -int(row.get("model_count") or 0), str(row.get("url") or "")))
    return jsonify({"results": results})
def _ollama_model_exists(url: str, model_name: str) -> bool:
    try:
        response = requests.get(f"{url.rstrip('/')}/api/tags", timeout=10)
        if response.status_code != 200:
            return False
        payload = response.json() if response.content else {}
        models = payload.get("models") or []
        target = str(model_name or "").strip().lower()
        for item in models:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip().lower()
            if name == target:
                return True
        return False
    except Exception:
        return False
def api_ollama_pull_status():
    return jsonify(_ollama_pull_status_snapshot())
def api_ollama_pull():
    data = request.get_json(silent=True) or {}
    url = str(data.get("OLLAMA_URL") or "").strip() or OLLAMA_URL
    model_name = str(data.get("model") or data.get("name") or "").strip()
    if not url:
        return jsonify({"error": "OLLAMA_URL is required"}), 400
    url = url.rstrip("/")
    if not model_name:
        return jsonify({"error": "Model name is required"}), 400

    current = _ollama_pull_status_snapshot()
    if bool(current.get("active")):
        if str(current.get("model") or "").strip().lower() == model_name.lower():
            return jsonify(current), 202
        return jsonify({"error": "Another Ollama pull is already running", "active": current}), 409

    if _ollama_model_exists(url, model_name):
        ready = _ollama_pull_status_update(
            active=False,
            status="completed",
            message=f"{model_name} is already installed",
            model=model_name,
            url=url,
            completed=0,
            total=0,
            progress=100.0,
            started_at=time.time(),
            finished_at=time.time(),
            error="",
        )
        return jsonify(ready)

    worker = threading.Thread(
        target=_run_ollama_pull_async,
        args=(url, model_name),
        daemon=True,
        name="ollama-model-pull",
    )
    worker.start()
    return jsonify(_ollama_pull_status_snapshot()), 202
def api_runtime_managed_status():
    include_candidates = not bool(_parse_bool(request.args.get("skip_candidates")))
    return jsonify(_managed_runtime_status_snapshot(include_candidates=include_candidates))
def api_runtime_managed_logs():
    bundle_type = str(request.args.get("bundle_type") or "").strip()
    service_name = str(request.args.get("service_name") or "").strip()
    try:
        limit = max(1, min(500, int(request.args.get("limit") or 200)))
    except Exception:
        limit = 200
    if bundle_type and bundle_type not in _MANAGED_RUNTIME_BUNDLE_TYPES:
        return jsonify({"error": f"Unsupported bundle_type: {bundle_type}"}), 400
    rows = _managed_runtime_logs(bundle_type or None, service_name=service_name or None, limit=limit)
    return jsonify({"logs": rows, "bundle_type": bundle_type or None, "service_name": service_name or None})
def _api_runtime_managed_common_roots(data: dict[str, Any]) -> tuple[str, str]:
    config_root = str(
        data.get("config_root")
        or _get_config_from_db("MANAGED_RUNTIME_CONFIG_ROOT", "")
        or ""
    ).strip()
    data_root = str(
        data.get("data_root")
        or _get_config_from_db("MANAGED_RUNTIME_DATA_ROOT", "")
        or ""
    ).strip()
    return config_root, data_root
def api_runtime_managed_bootstrap():
    data = request.get_json(silent=True) or {}
    config_root, data_root = _api_runtime_managed_common_roots(data)
    bundles_raw = data.get("bundles") if isinstance(data.get("bundles"), dict) else {}
    bundle_requests: list[tuple[str, dict[str, Any]]] = []
    if isinstance(data.get("bundle_type"), str) and str(data.get("bundle_type") or "").strip() in _MANAGED_RUNTIME_BUNDLE_TYPES:
        bundle_type = str(data.get("bundle_type") or "").strip()
        bundle_payload = dict(data.get("payload") or {})
        bundle_requests.append((bundle_type, bundle_payload))
    else:
        if isinstance(bundles_raw.get("musicbrainz_local"), dict):
            bundle_requests.append((_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, dict(bundles_raw.get("musicbrainz_local") or {})))
        if isinstance(bundles_raw.get("ollama_local"), dict):
            bundle_requests.append((_MANAGED_RUNTIME_OLLAMA_BUNDLE, dict(bundles_raw.get("ollama_local") or {})))
    if not bundle_requests:
        return jsonify({"error": "No managed runtime bundles were requested"}), 400

    results: list[dict[str, Any]] = []
    status_code = 202
    for bundle_type, bundle_payload in bundle_requests:
        payload = dict(bundle_payload or {})
        payload["config_root"] = config_root
        payload["data_root"] = data_root
        payload["action_id"] = str(uuid.uuid4())
        started, message = _managed_runtime_launch_bootstrap(bundle_type, payload)
        if not started:
            status_code = 409
        results.append(
            {
                "bundle_type": bundle_type,
                "started": bool(started),
                "message": str(message or ""),
                "status": _managed_runtime_bundle_status(bundle_type, include_candidates=False),
            }
        )
    return jsonify({"results": results, "snapshot": _managed_runtime_status_snapshot(include_candidates=False)}), status_code
def api_runtime_managed_adopt():
    data = request.get_json(silent=True) or {}
    bundle_type = str(data.get("bundle_type") or "").strip()
    if bundle_type not in _MANAGED_RUNTIME_BUNDLE_TYPES:
        return jsonify({"error": "bundle_type is required"}), 400
    config_root, data_root = _api_runtime_managed_common_roots(data)
    candidate = _managed_runtime_resolve_candidate(
        bundle_type,
        str(data.get("candidate_id") or "").strip(),
        url=str(data.get("url") or "").strip(),
        project_name=str(data.get("project_name") or "").strip(),
    )
    if candidate is None:
        return jsonify({"error": "No matching adoptable runtime was found"}), 404
    if bundle_type == _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE:
        result = _managed_runtime_adopt_musicbrainz(candidate, mode="adopted", config_root=config_root, data_root=data_root)
    else:
        fast_model = str(data.get("fast_model") or _ollama_model_configured()).strip() or "qwen3:4b"
        hard_model = str(data.get("hard_model") or _ollama_complex_model_configured()).strip() or "qwen3:14b"
        result = _managed_runtime_adopt_ollama(candidate, fast_model=fast_model, hard_model=hard_model, mode="adopted", config_root=config_root, data_root=data_root)
    return jsonify({"status": "ok", "bundle_type": bundle_type, "result": result, "snapshot": _managed_runtime_status_snapshot(include_candidates=False)})
def api_runtime_managed_action():
    data = request.get_json(silent=True) or {}
    bundle_type = str(data.get("bundle_type") or "").strip()
    action = str(data.get("action") or "").strip().lower()
    if bundle_type not in _MANAGED_RUNTIME_BUNDLE_TYPES:
        return jsonify({"error": "bundle_type is required"}), 400
    if not action:
        return jsonify({"error": "action is required"}), 400
    bundle = _managed_runtime_bundle_get(bundle_type)
    if action == "refresh-health":
        return jsonify({"status": "ok", "bundle_type": bundle_type, "result": _managed_runtime_bundle_status(bundle_type, include_candidates=True)})
    if action in {"retry-bootstrap", "rebuild"}:
        config_root, data_root = _api_runtime_managed_common_roots(data)
        payload = {
            "config_root": config_root or str(bundle.get("config_root") or ""),
            "data_root": data_root or str(bundle.get("data_root") or ""),
            "action": "create" if action == "rebuild" else "auto",
            "action_id": str(uuid.uuid4()),
        }
        if bundle_type == _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE:
            payload["mirror_name"] = str(data.get("mirror_name") or _get_config_from_db("MUSICBRAINZ_MIRROR_NAME", "Managed local MusicBrainz") or "Managed local MusicBrainz")
        else:
            payload["fast_model"] = str(data.get("fast_model") or _ollama_model_configured()).strip() or "qwen3:4b"
            payload["hard_model"] = str(data.get("hard_model") or _ollama_complex_model_configured()).strip() or "qwen3:14b"
        started, message = _managed_runtime_launch_bootstrap(bundle_type, payload)
        return jsonify({"status": "accepted" if started else "blocked", "message": message, "bundle_type": bundle_type, "snapshot": _managed_runtime_status_snapshot(include_candidates=False)}), (202 if started else 409)
    if action == "retry-update":
        if bundle_type != _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE:
            return jsonify({"error": "retry-update is only supported for MusicBrainz"}), 400
        if str(bundle.get("mode") or "").strip().lower() != "managed":
            return jsonify({"error": "retry-update is only supported for managed MusicBrainz bundles"}), 400
        started, message, run_id = _scheduler_launch_job("managed_musicbrainz_update", "both", "managed_runtime", max_concurrency=1)
        return jsonify({"status": "started" if started else "blocked", "message": message, "run_id": run_id, "snapshot": _managed_runtime_status_snapshot(include_candidates=True)}), (202 if started else 409)
    if action == "pull-model":
        if bundle_type != _MANAGED_RUNTIME_OLLAMA_BUNDLE:
            return jsonify({"error": "pull-model is only supported for Ollama"}), 400
        url = str(bundle.get("effective_url") or _get_config_from_db("OLLAMA_URL", OLLAMA_URL) or "").strip()
        model_name = str(data.get("model") or "").strip()
        if not url or not model_name:
            return jsonify({"error": "Both an Ollama runtime URL and model name are required"}), 400
        current = _ollama_pull_status_snapshot()
        if bool(current.get("active")):
            return jsonify({"status": "blocked", "message": "Another Ollama pull is already running", "active": current}), 409
        worker = threading.Thread(target=_run_ollama_pull_async, args=(url, model_name), daemon=True, name="managed-ollama-model-pull")
        worker.start()
        _managed_runtime_bundle_upsert(bundle_type, state="pulling", phase="pulling", phase_message=f"Pulling {model_name}")
        return jsonify({"status": "started", "message": f"Pulling {model_name}", "snapshot": _managed_runtime_status_snapshot(include_candidates=False)}), 202
    if str(bundle.get("mode") or "") != "managed":
        return jsonify({"error": f"Action '{action}' is only available for managed runtimes"}), 400
    try:
        if bundle_type == _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE:
            if action == "start":
                result = _managed_runtime_start_musicbrainz_bundle(bundle)
            elif action == "stop":
                result = _managed_runtime_stop_musicbrainz_bundle(bundle)
            elif action == "restart":
                result = _managed_runtime_restart_musicbrainz_bundle(bundle)
            elif action == "reset":
                result = _managed_runtime_reset_musicbrainz_bundle(bundle)
            else:
                return jsonify({"error": f"Unsupported action for MusicBrainz: {action}"}), 400
        else:
            if action == "start":
                result = _managed_runtime_start_ollama_bundle(bundle)
            elif action == "stop":
                result = _managed_runtime_stop_ollama_bundle(bundle)
            elif action == "restart":
                result = _managed_runtime_restart_ollama_bundle(bundle)
            elif action == "reset":
                result = _managed_runtime_reset_ollama_bundle(bundle)
            else:
                return jsonify({"error": f"Unsupported action for Ollama: {action}"}), 400
    except Exception as exc:
        message = str(exc or f"Managed runtime action failed: {action}")
        _managed_runtime_log(bundle_type, message, level="error")
        _managed_runtime_bundle_upsert(bundle_type, last_error=message)
        return jsonify({"error": message, "snapshot": _managed_runtime_status_snapshot(include_candidates=True)}), 500
    return jsonify({"status": "ok", "bundle_type": bundle_type, "action": action, "result": result, "snapshot": _managed_runtime_status_snapshot(include_candidates=True)})

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}


def _managed_runtime_resolve_musicbrainz_install_root_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_resolve_musicbrainz_install_root(*args, **kwargs)

def _managed_runtime_container_path_to_host_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_container_path_to_host_path(*args, **kwargs)

def _managed_runtime_container_bind_alias_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_container_bind_alias_path(*args, **kwargs)

def _managed_runtime_json_dumps_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_json_dumps(*args, **kwargs)

def _managed_runtime_json_loads_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_json_loads(*args, **kwargs)

def _managed_runtime_bundle_defaults_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_bundle_defaults(*args, **kwargs)

def _managed_runtime_bundle_get_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_bundle_get(*args, **kwargs)

def _managed_runtime_bundle_upsert_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_bundle_upsert(*args, **kwargs)

def _managed_runtime_bundle_upsert_best_effort_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_bundle_upsert_best_effort(*args, **kwargs)

def _managed_runtime_log_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_log(*args, **kwargs)

def _managed_runtime_logs_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_logs(*args, **kwargs)

def _managed_runtime_action_update_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_action_update(*args, **kwargs)

def _managed_runtime_get_latest_action_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_get_latest_action(*args, **kwargs)

def _managed_runtime_docker_cli_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_docker_cli(*args, **kwargs)

def _managed_runtime_compose_cli_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_compose_cli(*args, **kwargs)

def _managed_runtime_git_cli_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_git_cli(*args, **kwargs)

def _managed_runtime_self_container_name_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_self_container_name(*args, **kwargs)

def _managed_runtime_sysfs_gpu_vendor_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_sysfs_gpu_vendor(*args, **kwargs)

def _managed_runtime_collect_dri_devices_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_collect_dri_devices(*args, **kwargs)

def _managed_runtime_gpu_probe_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_gpu_probe(*args, **kwargs)

def _managed_runtime_ollama_gpu_requested_mode_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_ollama_gpu_requested_mode(*args, **kwargs)

def _managed_runtime_ollama_gpu_profile_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_ollama_gpu_profile(*args, **kwargs)

def _managed_runtime_preflight_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_preflight(*args, **kwargs)

def _managed_runtime_docker_ps_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_docker_ps(*args, **kwargs)

def _managed_runtime_parse_ports_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_parse_ports(*args, **kwargs)

def _managed_runtime_docker_inspect_container_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_docker_inspect_container(*args, **kwargs)

def _managed_runtime_project_prefix_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_project_prefix(*args, **kwargs)

def _managed_runtime_health_check_musicbrainz_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_health_check_musicbrainz(*args, **kwargs)

def _managed_runtime_health_check_ollama_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_health_check_ollama(*args, **kwargs)

def _managed_runtime_container_labels_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_container_labels(*args, **kwargs)

def _managed_runtime_detect_musicbrainz_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_detect_musicbrainz_candidates(*args, **kwargs)

def _managed_runtime_detect_ollama_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_detect_ollama_candidates(*args, **kwargs)

def _managed_runtime_ensure_network_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_ensure_network(*args, **kwargs)

def _managed_runtime_connect_container_to_network_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_connect_container_to_network(*args, **kwargs)

def _managed_runtime_connect_self_to_network_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_connect_self_to_network(*args, **kwargs)

def _managed_runtime_try_connect_self_to_existing_network_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_try_connect_self_to_existing_network(*args, **kwargs)

def _managed_runtime_musicbrainz_install_root_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_musicbrainz_install_root(*args, **kwargs)

def _managed_runtime_musicbrainz_data_root_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_musicbrainz_data_root(*args, **kwargs)

def _managed_runtime_ollama_data_root_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_ollama_data_root(*args, **kwargs)

def _managed_runtime_musicbrainz_internal_url_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_musicbrainz_internal_url(*args, **kwargs)

def _managed_runtime_short_duration_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_short_duration(*args, **kwargs)

def _managed_runtime_capture_subprocess_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_capture_subprocess(*args, **kwargs)

def _managed_runtime_health_wait_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_health_wait(*args, **kwargs)

def _managed_runtime_register_mb_update_schedule_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_register_mb_update_schedule(*args, **kwargs)

def _managed_runtime_apply_musicbrainz_runtime_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_apply_musicbrainz_runtime(*args, **kwargs)

def _managed_runtime_apply_ollama_runtime_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_apply_ollama_runtime(*args, **kwargs)

def _managed_runtime_adopt_musicbrainz_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_adopt_musicbrainz(*args, **kwargs)

def _managed_runtime_adopt_ollama_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_adopt_ollama(*args, **kwargs)

def _managed_runtime_ollama_pull_blocking_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_ollama_pull_blocking(*args, **kwargs)

def _managed_runtime_ensure_ollama_models_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_ensure_ollama_models(*args, **kwargs)

def _managed_runtime_bootstrap_musicbrainz_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_bootstrap_musicbrainz(*args, **kwargs)

def _managed_runtime_bootstrap_ollama_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_bootstrap_ollama(*args, **kwargs)

def _managed_runtime_bootstrap_worker_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_bootstrap_worker(*args, **kwargs)

def _managed_runtime_launch_bootstrap_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_launch_bootstrap(*args, **kwargs)

def _managed_runtime_bundle_status_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_bundle_status(*args, **kwargs)

def _managed_runtime_status_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_status_snapshot(*args, **kwargs)

def _managed_runtime_musicbrainz_update_due_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_musicbrainz_update_due(*args, **kwargs)

def _managed_runtime_run_musicbrainz_update_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_run_musicbrainz_update(*args, **kwargs)

def _managed_runtime_musicbrainz_repair_worker_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_musicbrainz_repair_worker(*args, **kwargs)

def _managed_runtime_launch_musicbrainz_search_repair_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_launch_musicbrainz_search_repair(*args, **kwargs)

def _managed_runtime_maybe_enqueue_due_jobs_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_maybe_enqueue_due_jobs(*args, **kwargs)

def _managed_runtime_resolve_candidate_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_resolve_candidate(*args, **kwargs)

def _managed_runtime_mb_compose_cmd_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_mb_compose_cmd(*args, **kwargs)

def _managed_runtime_start_musicbrainz_bundle_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_start_musicbrainz_bundle(*args, **kwargs)

def _managed_runtime_stop_musicbrainz_bundle_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_stop_musicbrainz_bundle(*args, **kwargs)

def _managed_runtime_restart_musicbrainz_bundle_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_restart_musicbrainz_bundle(*args, **kwargs)

def _managed_runtime_reset_musicbrainz_bundle_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_reset_musicbrainz_bundle(*args, **kwargs)

def _managed_runtime_start_ollama_bundle_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_start_ollama_bundle(*args, **kwargs)

def _managed_runtime_stop_ollama_bundle_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_stop_ollama_bundle(*args, **kwargs)

def _managed_runtime_restart_ollama_bundle_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_restart_ollama_bundle(*args, **kwargs)

def _managed_runtime_reset_ollama_bundle_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _managed_runtime_reset_ollama_bundle(*args, **kwargs)

def _ollama_pull_status_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_pull_status_snapshot(*args, **kwargs)

def _ollama_pull_status_update_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_pull_status_update(*args, **kwargs)

def _run_ollama_pull_async_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_ollama_pull_async(*args, **kwargs)
def api_musicbrainz_test_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_musicbrainz_test(*args, **kwargs)

def api_ollama_models_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_ollama_models(*args, **kwargs)

def _normalize_ollama_probe_url_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _normalize_ollama_probe_url(*args, **kwargs)

def _ollama_probe_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_probe(*args, **kwargs)

def api_ollama_discover_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_ollama_discover(*args, **kwargs)

def _ollama_model_exists_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ollama_model_exists(*args, **kwargs)

def api_ollama_pull_status_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_ollama_pull_status(*args, **kwargs)

def api_ollama_pull_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_ollama_pull(*args, **kwargs)

def api_runtime_managed_status_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_runtime_managed_status(*args, **kwargs)

def api_runtime_managed_logs_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_runtime_managed_logs(*args, **kwargs)

def _api_runtime_managed_common_roots_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _api_runtime_managed_common_roots(*args, **kwargs)

def api_runtime_managed_bootstrap_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_runtime_managed_bootstrap(*args, **kwargs)

def api_runtime_managed_adopt_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_runtime_managed_adopt(*args, **kwargs)

def api_runtime_managed_action_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_runtime_managed_action(*args, **kwargs)
