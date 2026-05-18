"""Runtime auto-tuning helpers for provider and MusicBrainz queues."""
from __future__ import annotations

import logging
import sys
import time
from typing import Any

_RUNTIME: Any | None = None
_EXTRACTED_NAMES = {
    '_runtime_auto_tune_snapshot',
    '_discogs_effective_rpm',
    '_runtime_auto_tune_note_discogs_rate_limited',
    '_runtime_auto_tune_apply',
    '_runtime_auto_tune_worker',
    '_start_runtime_auto_tune_worker',
}


def _runtime_module() -> Any:
    return _RUNTIME if _RUNTIME is not None else sys.modules[__name__]


def _bind_runtime(runtime: Any) -> None:
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
        if name in {"_bind_runtime", "_runtime_module", "_sync_runtime_state"} or own_wrapper:
            continue
        globals()[name] = value


def _sync_runtime_state() -> None:
    runtime = _runtime_module()
    if runtime is sys.modules[__name__]:
        return
    for key in (
        "_RUNTIME_AUTO_TUNE_THREAD",
        "_RUNTIME_AUTO_TUNE_STATE",
        "PROVIDER_GATEWAY_MAX_INFLIGHT",
        "MB_MIRROR_QUEUE_RPS",
    ):
        if key in globals():
            try:
                setattr(runtime, key, globals()[key])
            except Exception:
                logging.debug("Failed to sync runtime tuning global %s", key, exc_info=True)

def _runtime_auto_tune_snapshot() -> dict[str, Any]:
    with _RUNTIME_AUTO_TUNE_LOCK:
        try:
            effective = float(_RUNTIME_AUTO_TUNE_STATE.get("discogs_effective_rpm") or 0.0)
        except Exception:
            effective = 0.0
        return {
            "enabled": bool(AUTO_TUNE_ENABLED),
            "interval_sec": int(max(15, AUTO_TUNE_INTERVAL_SEC or 60)),
            "mb_mirror_min_rps": float(max(1.0, AUTO_TUNE_MB_MIRROR_MIN_RPS or 1.0)),
            "mb_mirror_max_rps": float(max(1.0, AUTO_TUNE_MB_MIRROR_MAX_RPS or 1.0)),
            "provider_inflight_min": int(max(1, AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN or 1)),
            "provider_inflight_cap": int(max(1, AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP or 1)),
            "discogs_configured_rpm": int(max(1, PROVIDER_GATEWAY_DISCOGS_RPM or 1)),
            "discogs_effective_rpm": max(1.0, effective) if effective > 0.0 else float(max(1, PROVIDER_GATEWAY_DISCOGS_RPM or 1)),
            "last_run_at": float(_RUNTIME_AUTO_TUNE_STATE.get("last_run_at") or 0.0),
            "last_change_at": float(_RUNTIME_AUTO_TUNE_STATE.get("last_change_at") or 0.0),
            "last_reason": str(_RUNTIME_AUTO_TUNE_STATE.get("last_reason") or ""),
        }


def _discogs_effective_rpm() -> float:
    with _RUNTIME_AUTO_TUNE_LOCK:
        try:
            effective = float(_RUNTIME_AUTO_TUNE_STATE.get("discogs_effective_rpm") or 0.0)
        except Exception:
            effective = 0.0
    if effective > 0.0:
        return max(1.0, effective)
    return float(max(1, PROVIDER_GATEWAY_DISCOGS_RPM or 1))


def _runtime_auto_tune_note_discogs_rate_limited(context: str = "") -> None:
    base_rpm = float(max(1, PROVIDER_GATEWAY_DISCOGS_RPM or 1))
    min_rpm = max(12.0, min(base_rpm, max(12.0, round(base_rpm * 0.5, 1))))
    with _RUNTIME_AUTO_TUNE_LOCK:
        current_rpm = float(_RUNTIME_AUTO_TUNE_STATE.get("discogs_effective_rpm") or base_rpm)
        next_rpm = max(min_rpm, round(current_rpm * 0.75, 1))
        changed = abs(next_rpm - current_rpm) >= 0.5
        _RUNTIME_AUTO_TUNE_STATE["discogs_effective_rpm"] = float(next_rpm)
        _RUNTIME_AUTO_TUNE_STATE["last_run_at"] = time.time()
        if changed:
            _RUNTIME_AUTO_TUNE_STATE["last_change_at"] = time.time()
            _RUNTIME_AUTO_TUNE_STATE["last_reason"] = (
                f"Discogs RPM {current_rpm:.1f}->{next_rpm:.1f} after 429"
                + (f" ({context})" if str(context or "").strip() else "")
            )
    if changed:
        logging.info(
            "[AUTO TUNE] [V✅ ] Discogs RPM %.1f->%.1f after 429%s",
            current_rpm,
            next_rpm,
            f" context={context}" if str(context or "").strip() else "",
        )


def _runtime_auto_tune_apply() -> None:
    global PROVIDER_GATEWAY_MAX_INFLIGHT
    reason_parts: list[str] = []
    changed = False
    if not bool(AUTO_TUNE_ENABLED):
        with _RUNTIME_AUTO_TUNE_LOCK:
            _RUNTIME_AUTO_TUNE_STATE["last_run_at"] = time.time()
            _RUNTIME_AUTO_TUNE_STATE["last_reason"] = "disabled"
        return

    target = _musicbrainz_target_settings()
    if bool(target.get("enabled")) and bool(USE_MUSICBRAINZ) and bool(MB_QUEUE_ENABLED):
        try:
            queue_obj = get_mb_queue()
            if bool(getattr(queue_obj, "enabled", False)):
                stats = queue_obj.stats_snapshot()
                current_rps = float(stats.get("rate_limit_rps") or getattr(queue_obj, "rate_limit_rps", 0.0) or 0.0)
                min_rps = max(1.0, float(AUTO_TUNE_MB_MIRROR_MIN_RPS or 1.0))
                max_rps = max(min_rps, float(AUTO_TUNE_MB_MIRROR_MAX_RPS or min_rps))
                pending = int(stats.get("queue_pending") or 0)
                waiters = int(stats.get("queue_waiters") or 0)
                completed = int(stats.get("completed_count") or 0)
                errors = int(stats.get("error_count") or 0)
                avg_latency_ms = float(stats.get("avg_latency_ms") or 0.0)
                with _RUNTIME_AUTO_TUNE_LOCK:
                    prev_completed = int(_RUNTIME_AUTO_TUNE_STATE.get("mb_last_completed") or 0)
                    prev_errors = int(_RUNTIME_AUTO_TUNE_STATE.get("mb_last_errors") or 0)
                    _RUNTIME_AUTO_TUNE_STATE["mb_last_completed"] = completed
                    _RUNTIME_AUTO_TUNE_STATE["mb_last_errors"] = errors
                completed_delta = max(0, completed - prev_completed)
                error_delta = max(0, errors - prev_errors)
                next_rps = current_rps
                if current_rps < max_rps and (pending + waiters) >= max(4, int(current_rps)) and error_delta == 0 and avg_latency_ms <= 1800.0:
                    next_rps = min(max_rps, current_rps + 1.0)
                elif current_rps > min_rps and (error_delta > 0 or avg_latency_ms >= 3500.0 or ((pending + waiters) > 0 and completed_delta == 0)):
                    next_rps = max(min_rps, current_rps - 1.0)
                if abs(next_rps - current_rps) >= 0.5:
                    applied = queue_obj.reconfigure_rate_limit(next_rps)
                    try:
                        setattr(_runtime_module(), "MB_MIRROR_QUEUE_RPS", float(applied))
                        merged["MB_MIRROR_QUEUE_RPS"] = float(applied)
                    except Exception:
                        pass
                    changed = True
                    reason_parts.append(f"MB mirror RPS {current_rps:.1f}->{applied:.1f} backlog={pending + waiters} avg={avg_latency_ms:.0f}ms")
        except Exception:
            logging.debug("Runtime auto-tune MB iteration failed", exc_info=True)

    try:
        gateway_stats = _provider_gateway_stats_snapshot()
        providers_map = dict(gateway_stats.get("providers") or {})
        current_inflight = int(max(1, PROVIDER_GATEWAY_MAX_INFLIGHT or 1))
        min_inflight = max(1, int(AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN or 1))
        cap_inflight = max(min_inflight, int(AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP or min_inflight))
        total_timeouts = sum(int((stats or {}).get("timeout_count") or 0) for stats in providers_map.values())
        total_rate_limited = sum(int((stats or {}).get("rate_limited_count") or 0) for stats in providers_map.values())
        max_inflight_observed = int(gateway_stats.get("max_inflight_observed") or 0)
        with _RUNTIME_AUTO_TUNE_LOCK:
            prev_timeouts = int(_RUNTIME_AUTO_TUNE_STATE.get("provider_last_timeouts") or 0)
            prev_rate_limited = int(_RUNTIME_AUTO_TUNE_STATE.get("provider_last_rate_limited") or 0)
            _RUNTIME_AUTO_TUNE_STATE["provider_last_timeouts"] = total_timeouts
            _RUNTIME_AUTO_TUNE_STATE["provider_last_rate_limited"] = total_rate_limited
        timeout_delta = max(0, total_timeouts - prev_timeouts)
        rate_limited_delta = max(0, total_rate_limited - prev_rate_limited)
        next_inflight = current_inflight
        if current_inflight > min_inflight and (timeout_delta > 0 or rate_limited_delta > 0):
            next_inflight = max(min_inflight, current_inflight - 1)
        elif current_inflight < cap_inflight and timeout_delta == 0 and rate_limited_delta == 0 and max_inflight_observed >= current_inflight:
            next_inflight = min(cap_inflight, current_inflight + 1)
        if next_inflight != current_inflight:
            PROVIDER_GATEWAY_MAX_INFLIGHT = int(next_inflight)
            try:
                setattr(_runtime_module(), "PROVIDER_GATEWAY_MAX_INFLIGHT", PROVIDER_GATEWAY_MAX_INFLIGHT)
                merged["PROVIDER_GATEWAY_MAX_INFLIGHT"] = PROVIDER_GATEWAY_MAX_INFLIGHT
            except Exception:
                pass
            _provider_gateway_reconfigure()
            changed = True
            reason_parts.append(
                f"Gateway inflight {current_inflight}->{next_inflight} timeouts+={timeout_delta} 429s+={rate_limited_delta} peak={max_inflight_observed}"
            )

        discogs_stats = dict(providers_map.get("discogs") or {})
        base_discogs_rpm = float(max(1, PROVIDER_GATEWAY_DISCOGS_RPM or 1))
        min_discogs_rpm = max(12.0, min(base_discogs_rpm, max(12.0, round(base_discogs_rpm * 0.5, 1))))
        cap_discogs_rpm = max(min_discogs_rpm, base_discogs_rpm)
        with _RUNTIME_AUTO_TUNE_LOCK:
            current_discogs_rpm = float(_RUNTIME_AUTO_TUNE_STATE.get("discogs_effective_rpm") or base_discogs_rpm)
            prev_discogs_rate_limited = int(_RUNTIME_AUTO_TUNE_STATE.get("discogs_last_rate_limited") or 0)
            prev_discogs_network = int(_RUNTIME_AUTO_TUNE_STATE.get("discogs_last_network_requests") or 0)
            _RUNTIME_AUTO_TUNE_STATE["discogs_last_rate_limited"] = int(discogs_stats.get("rate_limited_count") or 0)
            _RUNTIME_AUTO_TUNE_STATE["discogs_last_network_requests"] = int(discogs_stats.get("network_request_count") or 0)
        discogs_rate_limited_delta = max(0, int(discogs_stats.get("rate_limited_count") or 0) - prev_discogs_rate_limited)
        discogs_network_delta = max(0, int(discogs_stats.get("network_request_count") or 0) - prev_discogs_network)
        discogs_avg_latency_ms = float(discogs_stats.get("avg_latency_ms") or 0.0)
        next_discogs_rpm = current_discogs_rpm
        if discogs_rate_limited_delta > 0 or discogs_avg_latency_ms >= 4500.0:
            next_discogs_rpm = max(
                min_discogs_rpm,
                round(max(current_discogs_rpm - 4.0, current_discogs_rpm * 0.85), 1),
            )
        elif (
            current_discogs_rpm < cap_discogs_rpm
            and discogs_rate_limited_delta == 0
            and discogs_network_delta >= max(4, int(max(1.0, current_discogs_rpm / 6.0)))
            and discogs_avg_latency_ms <= 1800.0
        ):
            next_discogs_rpm = min(cap_discogs_rpm, round(current_discogs_rpm + 2.0, 1))
        if abs(next_discogs_rpm - current_discogs_rpm) >= 0.5:
            with _RUNTIME_AUTO_TUNE_LOCK:
                _RUNTIME_AUTO_TUNE_STATE["discogs_effective_rpm"] = float(next_discogs_rpm)
            changed = True
            reason_parts.append(
                f"Discogs RPM {current_discogs_rpm:.1f}->{next_discogs_rpm:.1f} 429s+={discogs_rate_limited_delta} avg={discogs_avg_latency_ms:.0f}ms"
            )
    except Exception:
        logging.debug("Runtime auto-tune provider iteration failed", exc_info=True)

    now = time.time()
    with _RUNTIME_AUTO_TUNE_LOCK:
        _RUNTIME_AUTO_TUNE_STATE["last_run_at"] = now
        if changed:
            _RUNTIME_AUTO_TUNE_STATE["last_change_at"] = now
        _RUNTIME_AUTO_TUNE_STATE["last_reason"] = "; ".join(reason_parts) if reason_parts else "stable"
    if changed and reason_parts:
        logging.info("[AUTO TUNE] [V✅ ] %s", "; ".join(reason_parts))


def _runtime_auto_tune_worker() -> None:
    while True:
        try:
            _runtime_auto_tune_apply()
        except Exception:
            logging.debug("Runtime auto-tune iteration failed", exc_info=True)
        time.sleep(int(max(15, AUTO_TUNE_INTERVAL_SEC or 60)))


def _start_runtime_auto_tune_worker() -> None:
    global _RUNTIME_AUTO_TUNE_THREAD
    with _RUNTIME_AUTO_TUNE_LOCK:
        if _RUNTIME_AUTO_TUNE_THREAD and _RUNTIME_AUTO_TUNE_THREAD.is_alive():
            return
        _RUNTIME_AUTO_TUNE_THREAD = threading.Thread(
            target=_runtime_auto_tune_worker,
            daemon=True,
            name="runtime-auto-tune",
        )
        _RUNTIME_AUTO_TUNE_THREAD.start()


_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}

def _runtime_auto_tune_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    result = _runtime_auto_tune_snapshot(*args, **kwargs)
    _sync_runtime_state()
    return result

def _discogs_effective_rpm_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    result = _discogs_effective_rpm(*args, **kwargs)
    _sync_runtime_state()
    return result

def _runtime_auto_tune_note_discogs_rate_limited_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    result = _runtime_auto_tune_note_discogs_rate_limited(*args, **kwargs)
    _sync_runtime_state()
    return result

def _runtime_auto_tune_apply_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    result = _runtime_auto_tune_apply(*args, **kwargs)
    _sync_runtime_state()
    return result

def _runtime_auto_tune_worker_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    result = _runtime_auto_tune_worker(*args, **kwargs)
    _sync_runtime_state()
    return result

def _start_runtime_auto_tune_worker_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    result = _start_runtime_auto_tune_worker(*args, **kwargs)
    _sync_runtime_state()
    return result
