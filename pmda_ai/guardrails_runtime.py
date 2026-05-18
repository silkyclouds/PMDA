"""Runtime-bound AI guardrail and usage recording helpers."""
from __future__ import annotations

from typing import Any
import logging
import time
import uuid

_RUNTIME: Any | None = None
_EXTRACTED_NAMES = {
    "_ai_infer_runtime_context",
    "_ai_scan_budget_id_from_context",
    "_ai_guard_load_scan_snapshot",
    "_ai_guard_update_scan_state_live",
    "_ai_guardrail_global_non_scan_allows",
    "_ai_guardrail_precheck",
    "_ai_guard_reset_scan",
    "_ai_record_legacy_scan_call_counters",
    "record_ai_usage",
}
_ORIGINAL_EXTRACTED_FUNCTIONS: dict[str, Any] = {}


def _bind_runtime(runtime: Any) -> None:
    global _RUNTIME
    _RUNTIME = runtime
    for name, value in vars(runtime).items():
        if name in _EXTRACTED_NAMES:
            original = _ORIGINAL_EXTRACTED_FUNCTIONS.get(name)
            if original is not None:
                globals()[name] = original
            continue
        own_wrapper = name.endswith("_for_runtime") and name[: -len("_for_runtime")] in _EXTRACTED_NAMES
        if name == "_bind_runtime" or own_wrapper:
            continue
        globals()[name] = value

def _ai_infer_runtime_context() -> dict[str, Any]:
    ctx = _ai_usage_context_get()
    source = str(ctx.get("source") or "").strip().lower()
    scan_id_ctx = _int_or_none(ctx.get("scan_id"))
    origin_scan_id_ctx = _int_or_none(ctx.get("origin_scan_id"))
    run_id_ctx = str(ctx.get("run_id") or "").strip() or None
    job_type_ctx = _normalize_task_job_type(ctx.get("job_type"))
    scope_ctx = _normalize_task_scope(ctx.get("scope"), default="both")
    scheduler_job_id = str(ctx.get("scheduler_job_id") or "").strip() or run_id_ctx
    phase = "manual"
    scan_id = scan_id_ctx
    origin_scan_id = origin_scan_id_ctx
    if source == "post_scan_chain":
        phase = "post_scan"
    elif source in {"schedule", "manual"}:
        phase = "scheduled"
    if not scan_id:
        with lock:
            scanning_now = bool(state.get("scanning") or state.get("scan_finalizing"))
            scan_type_state = str(state.get("scan_type") or "full").strip().lower()
            scan_id_state = _int_or_none(state.get("scan_id"))
            run_id_state = str(state.get("scan_scheduler_run_id") or "").strip() or None
            source_state = str(state.get("scan_auto_trigger") or "").strip().lower()
        if scanning_now and scan_id_state:
            scan_id = scan_id_state
            if run_id_ctx is None and run_id_state:
                run_id_ctx = run_id_state
            if not job_type_ctx:
                job_type_ctx = "scan_changed" if scan_type_state == "changed_only" else "scan_full"
            if not scope_ctx or scope_ctx == "both":
                scope_ctx = "new" if scan_type_state == "changed_only" else "full"
            if phase == "manual":
                phase = "scan"
            if not source:
                source = source_state
    if phase == "manual" and scan_id:
        phase = "scan"
    if phase == "post_scan" and origin_scan_id is None:
        origin_scan_id = scan_id
        scan_id = None
    if phase == "scheduled" and source == "post_scan_chain":
        phase = "post_scan"
    if phase not in AI_USAGE_PHASES:
        phase = "manual"
    return {
        "phase": phase,
        "scan_id": scan_id,
        "origin_scan_id": origin_scan_id,
        "scheduler_job_id": scheduler_job_id,
        "run_id": run_id_ctx,
        "job_type": job_type_ctx or "",
        "scope": scope_ctx or "both",
        "source": source or "",
        "album_id": _int_or_none(ctx.get("album_id")),
        "album_artist": str(ctx.get("album_artist") or "").strip(),
        "album_title": str(ctx.get("album_title") or "").strip(),
    }


def _ai_scan_budget_id_from_context(ctx: dict[str, Any] | None) -> int:
    if not isinstance(ctx, dict):
        return 0
    sid = _int_or_none(ctx.get("scan_id"))
    if sid and sid > 0:
        return int(sid)
    origin = _int_or_none(ctx.get("origin_scan_id"))
    if origin and origin > 0:
        return int(origin)
    return 0


def _ai_guard_load_scan_snapshot(scan_id: int) -> tuple[int, float]:
    sid = int(scan_id or 0)
    if sid <= 0:
        return (0, 0.0)
    con = None
    try:
        con = _state_connect(timeout=5)
        cur = con.cursor()
        cur.execute(
            """
            SELECT COUNT(*) AS calls_total, MAX(created_at) AS last_ts
            FROM ai_call_usage
            WHERE scan_id = ? OR origin_scan_id = ?
            """,
            (sid, sid),
        )
        row = cur.fetchone() or {}
        calls_total = int(row["calls_total"] or 0)
        last_ts = float(row["last_ts"] or 0.0)
        return (calls_total, last_ts)
    except Exception:
        return (0, 0.0)
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _ai_guard_update_scan_state_live(
    *,
    scan_budget_id: int,
    used: int,
    blocked: int,
    reason: str = "",
    blocked_at: float | None = None,
) -> None:
    try:
        with lock:
            current_scan_id = int(state.get("scan_id") or 0)
            if int(scan_budget_id or 0) <= 0 or current_scan_id != int(scan_budget_id):
                return
            state["scan_ai_guard_calls_used"] = int(max(0, used))
            state["scan_ai_guard_calls_blocked"] = int(max(0, blocked))
            if reason:
                state["scan_ai_guard_last_reason"] = str(reason)
                state["scan_ai_guard_last_block_at"] = float(blocked_at or time.time())
    except Exception:
        pass


def _ai_guardrail_global_non_scan_allows() -> tuple[bool, str, dict[str, Any]]:
    return (
        True,
        "",
        {
            "global_max_calls_per_minute": 0,
            "global_max_calls_per_day": 0,
            "guardrail_policy": "disabled",
        },
    )


def _ai_guardrail_precheck(
    *,
    provider: str,
    model: str,
    endpoint_kind: str,
    analysis_type: str,
    requested_tokens: int,
) -> tuple[bool, str, dict[str, Any]]:
    ctx = _ai_infer_runtime_context()
    scan_budget_id = _ai_scan_budget_id_from_context(ctx)
    guard_meta = {
        "scan_budget_id": int(scan_budget_id or 0),
        "analysis_type": _normalize_ai_analysis_type(analysis_type),
        "endpoint_kind": _ai_usage_endpoint_kind(endpoint_kind),
        "requested_tokens": int(max(0, int(requested_tokens or 0))),
        "guardrail_policy": "disabled",
    }
    guard_meta.update(_ai_guardrail_global_non_scan_allows()[2] or {})
    if scan_budget_id > 0:
        now_ts = time.time()
        max_calls = int(max(0, int(globals().get("AI_MAX_CALLS_PER_SCAN", 0) or 0)))
        cooldown_sec = float(max(0.0, float(globals().get("AI_CALL_COOLDOWN_SEC", 0.0) or 0.0)))
        if max_calls > 0 or cooldown_sec > 0:
            guard_meta["guardrail_policy"] = "scan_budget"
            guard_meta["scan_max_calls"] = max_calls
            guard_meta["scan_call_cooldown_sec"] = cooldown_sec
        with _ai_guard_runtime_lock:
            rec = _ai_guard_runtime.get(int(scan_budget_id))
            if not isinstance(rec, dict):
                db_used, db_last_ts = _ai_guard_load_scan_snapshot(int(scan_budget_id))
                rec = {
                    "used": int(db_used or 0),
                    "blocked": 0,
                    "last_ts": float(db_last_ts or 0.0),
                    "last_reason": "",
                    "last_block_at": 0.0,
                }
                _ai_guard_runtime[int(scan_budget_id)] = rec
            used = int(rec.get("used") or 0)
            blocked = int(rec.get("blocked") or 0)
            last_ts = float(rec.get("last_ts") or 0.0)
            if max_calls > 0 and used >= max_calls:
                blocked += 1
                reason = f"cap_reached:{used}/{max_calls}"
                rec["blocked"] = blocked
                rec["last_reason"] = reason
                rec["last_block_at"] = now_ts
                _ai_guard_update_scan_state_live(
                    scan_budget_id=int(scan_budget_id),
                    used=used,
                    blocked=blocked,
                    reason=reason,
                    blocked_at=now_ts,
                )
                guard_meta["scan_calls_used"] = used
                guard_meta["scan_calls_blocked"] = blocked
                return (False, reason, guard_meta)
            if cooldown_sec > 0 and last_ts > 0 and (now_ts - last_ts) < cooldown_sec:
                blocked += 1
                retry_after = max(0.0, cooldown_sec - (now_ts - last_ts))
                reason = f"cooldown_active:{retry_after:.2f}s"
                rec["blocked"] = blocked
                rec["last_reason"] = reason
                rec["last_block_at"] = now_ts
                _ai_guard_update_scan_state_live(
                    scan_budget_id=int(scan_budget_id),
                    used=used,
                    blocked=blocked,
                    reason=reason,
                    blocked_at=now_ts,
                )
                guard_meta["scan_calls_used"] = used
                guard_meta["scan_calls_blocked"] = blocked
                guard_meta["retry_after_sec"] = retry_after
                return (False, reason, guard_meta)
            used += 1
            rec["used"] = used
            rec["last_ts"] = now_ts
            rec["last_reason"] = ""
            rec["last_block_at"] = 0.0
            _ai_guard_update_scan_state_live(
                scan_budget_id=int(scan_budget_id),
                used=used,
                blocked=blocked,
            )
            guard_meta["scan_calls_used"] = used
            guard_meta["scan_calls_blocked"] = blocked
    return (True, "", guard_meta)


def _ai_guard_reset_scan(scan_id: int) -> None:
    sid = int(scan_id or 0)
    if sid <= 0:
        return
    with _ai_guard_runtime_lock:
        _ai_guard_runtime.pop(sid, None)
    with _ai_web_search_run_seen_lock:
        _ai_web_search_run_seen.pop(f"scan:{sid}", None)


def _ai_record_legacy_scan_call_counters(analysis_type: str) -> None:
    with lock:
        if not bool(state.get("scanning") or state.get("scan_finalizing")):
            return
        state["scan_ai_calls_total"] = int(state.get("scan_ai_calls_total") or 0) + 1
        if analysis_type == "provider_identity_verify":
            state["scan_ai_calls_provider_identity"] = int(state.get("scan_ai_calls_provider_identity") or 0) + 1
        elif analysis_type in {
            "mb_match_verify",
            "acoustid_candidate_disambiguation",
            "mb_retry_disambiguation",
            "mb_candidate_tiebreak",
            "mb_artist_index_choice",
        }:
            state["scan_ai_calls_mb_verify"] = int(state.get("scan_ai_calls_mb_verify") or 0) + 1
        elif analysis_type in {"web_mbid_inference", "web_search"}:
            state["scan_ai_calls_web_mbid"] = int(state.get("scan_ai_calls_web_mbid") or 0) + 1
        elif analysis_type == "cover_vision_verify":
            state["scan_ai_calls_vision"] = int(state.get("scan_ai_calls_vision") or 0) + 1


def record_ai_usage(
    *,
    provider: str,
    model: str,
    endpoint_kind: str,
    analysis_type: str,
    started_at: float,
    status: str,
    response_obj: Any = None,
    image_inputs: int = 0,
    error: str = "",
    metadata: dict[str, Any] | None = None,
) -> None:
    try:
        normalized_analysis = _normalize_ai_analysis_type(analysis_type)
        metadata_payload = dict(metadata or {})
        token_data, usage_source, request_id = _ai_usage_extract_tokens(provider, response_obj)
        input_tokens = int(token_data.get("input_tokens") or 0)
        cached_input_tokens = int(token_data.get("cached_input_tokens") or 0)
        output_tokens = int(token_data.get("output_tokens") or 0)
        total_tokens = int(token_data.get("total_tokens") or 0)
        if total_tokens <= 0:
            total_tokens = max(0, input_tokens + output_tokens)
        ctx = _ai_infer_runtime_context()
        meta_album_id = _int_or_none(metadata_payload.get("album_id"))
        ctx_album_id = _int_or_none(ctx.get("album_id"))
        album_id = meta_album_id if meta_album_id and meta_album_id > 0 else (ctx_album_id if ctx_album_id and ctx_album_id > 0 else None)
        album_artist = str(metadata_payload.get("album_artist") or ctx.get("album_artist") or "").strip()
        album_title = str(metadata_payload.get("album_title") or ctx.get("album_title") or "").strip()
        ended = time.time()
        latency_ms = int(max(0.0, (ended - float(started_at or ended)) * 1000.0))
        row = {
            "call_id": str(uuid.uuid4()),
            "created_at": ended,
            "scan_id": ctx.get("scan_id"),
            "origin_scan_id": ctx.get("origin_scan_id"),
            "album_id": album_id,
            "album_artist": album_artist,
            "album_title": album_title,
            "scheduler_job_id": ctx.get("scheduler_job_id"),
            "run_id": ctx.get("run_id"),
            "phase": str(ctx.get("phase") or "manual"),
            "job_type": str(ctx.get("job_type") or ""),
            "scope": str(ctx.get("scope") or ""),
            "analysis_type": normalized_analysis,
            "provider": str(provider or "").strip().lower(),
            "model": str(model or "").strip(),
            "endpoint_kind": _ai_usage_endpoint_kind(endpoint_kind),
            "status": str(status or "failed").strip().lower() if str(status or "").strip().lower() in AI_USAGE_STATUSES else "failed",
            "latency_ms": latency_ms,
            "request_id": str(request_id or "") or None,
            "input_tokens": input_tokens,
            "cached_input_tokens": cached_input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "image_inputs": int(max(0, int(image_inputs or 0))),
            "usage_source": usage_source,
            "error_code": "provider_error" if str(error or "").strip() else "",
            "error_message": str(error or "").strip()[:600],
            "metadata": metadata_payload,
        }
        if not bool(metadata_payload.get("guardrail_blocked")):
            _ai_record_legacy_scan_call_counters(normalized_analysis)
        _start_ai_usage_worker_if_needed()
        _ai_usage_queue.put_nowait(row)
    except Exception:
        logging.debug("record_ai_usage failed", exc_info=True)


def _ai_infer_runtime_context_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_infer_runtime_context(*args, **kwargs)

def _ai_scan_budget_id_from_context_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_scan_budget_id_from_context(*args, **kwargs)

def _ai_guard_load_scan_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_guard_load_scan_snapshot(*args, **kwargs)

def _ai_guard_update_scan_state_live_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_guard_update_scan_state_live(*args, **kwargs)

def _ai_guardrail_global_non_scan_allows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_guardrail_global_non_scan_allows(*args, **kwargs)

def _ai_guardrail_precheck_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_guardrail_precheck(*args, **kwargs)

def _ai_guard_reset_scan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_guard_reset_scan(*args, **kwargs)

def _ai_record_legacy_scan_call_counters_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_record_legacy_scan_call_counters(*args, **kwargs)

def record_ai_usage_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return record_ai_usage(*args, **kwargs)


_ORIGINAL_EXTRACTED_FUNCTIONS.update({
    "_ai_infer_runtime_context": _ai_infer_runtime_context,
    "_ai_scan_budget_id_from_context": _ai_scan_budget_id_from_context,
    "_ai_guard_load_scan_snapshot": _ai_guard_load_scan_snapshot,
    "_ai_guard_update_scan_state_live": _ai_guard_update_scan_state_live,
    "_ai_guardrail_global_non_scan_allows": _ai_guardrail_global_non_scan_allows,
    "_ai_guardrail_precheck": _ai_guardrail_precheck,
    "_ai_guard_reset_scan": _ai_guard_reset_scan,
    "_ai_record_legacy_scan_call_counters": _ai_record_legacy_scan_call_counters,
    "record_ai_usage": record_ai_usage,
})
