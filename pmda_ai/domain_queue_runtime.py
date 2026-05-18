"""Domain AI queue runtime for matching, dedupe, incomplete, and review work."""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Any


def ai_queue_domain_requires_idle_scan(domain: str) -> bool:
    domain_norm = str(domain or "").strip().lower()
    return domain_norm in {"matching", "incomplete"}


def ai_queue_primary_analysis_type(domain: str) -> str:
    return {
        "matching": "mb_match_verify",
        "dedupe": "dedupe_choose_best",
        "incomplete": "incomplete_album_arbitration",
        "review": "album_review_validate",
    }.get(str(domain or "").strip().lower(), "other")


def ai_queue_status_snapshot_for_runtime(runtime: Any, domain: str | None = None) -> dict[str, Any]:
    if domain is None:
        snapshot: dict[str, Any] = {}
        for domain_name in runtime.AI_DOMAIN_NAMES:
            snapshot[domain_name] = ai_queue_status_snapshot_for_runtime(runtime, domain_name)
        return snapshot
    domain_norm = str(domain or "").strip().lower()
    if domain_norm == "incomplete":
        snap = dict(runtime._incomplete_ai_review_status_snapshot() or {})
        snap["domain"] = domain_norm
        return snap
    if domain_norm not in runtime._AI_QUEUE_DOMAINS:
        return {"domain": domain_norm, "error": "unsupported_domain"}
    with runtime._ai_domain_queue_lock:
        snap = dict(runtime._ai_domain_states.get(domain_norm) or {})
        snap["queued"] = int(len(runtime._ai_domain_queues.get(domain_norm) or []))
        snap["worker_started"] = bool(runtime._ai_domain_worker_started.get(domain_norm))
    snap["domain"] = domain_norm
    return snap


def ai_queue_update_metrics_for_runtime(
    runtime: Any,
    domain: str,
    *,
    status: str,
    latency_ms: int,
    result: dict[str, Any] | None = None,
    error: str = "",
) -> None:
    if domain == "incomplete":
        return
    with runtime._ai_domain_queue_lock:
        st = runtime._ai_domain_states.get(domain)
        if not isinstance(st, dict):
            return
        status_norm = str(status or "").strip().lower()
        if status_norm == "completed":
            st["completed_count"] = int(st.get("completed_count") or 0) + 1
        elif status_norm == "skipped":
            st["skipped_count"] = int(st.get("skipped_count") or 0) + 1
        else:
            st["failed_count"] = int(st.get("failed_count") or 0) + 1
        prev_done = int(st.get("completed_count") or 0) + int(st.get("failed_count") or 0) + int(st.get("skipped_count") or 0) - 1
        prev_avg = float(st.get("avg_latency_ms") or 0.0)
        latency_val = max(0, int(latency_ms or 0))
        st["avg_latency_ms"] = round(((prev_avg * max(0, prev_done)) + latency_val) / max(1, prev_done + 1), 2)
        st["last_latency_ms"] = latency_val
        st["last_status"] = status_norm
        st["last_error"] = str(error or "").strip()
        st["last_finished_at"] = float(time.time())
        st["last_result"] = dict(result or {})


def ai_queue_process_matching_for_runtime(runtime: Any, item: dict[str, Any]) -> dict[str, Any]:
    artist = str(item.get("artist") or "").strip()
    title_raw = str(item.get("title_raw") or "").strip() or None
    title_norm = str(item.get("title_norm") or "").strip()
    track_titles = [str(t or "").strip() for t in list(item.get("track_titles") or []) if str(t or "").strip()]
    track_count = max(0, int(runtime._parse_int_loose(item.get("track_count"), 0) or 0))
    has_cover = bool(runtime._parse_bool(item.get("has_cover"), False))
    extra_sources = [row for row in list(item.get("extra_sources") or []) if isinstance(row, dict)]
    raw_candidates = list(item.get("candidates") or [])
    candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for row in raw_candidates[:12]:
        if isinstance(row, dict) and isinstance(row.get("rg"), dict) and isinstance(row.get("result"), dict):
            candidates.append((dict(row["rg"]), dict(row["result"])))
    if not artist or not title_norm or not candidates:
        return {"status": "skipped", "reason": "invalid_payload"}
    runtime._ollama_prewarm_model(runtime._ollama_model_configured(), analysis_type="mb_match_verify", force=False)
    chosen, confidence = runtime.ai_verify_mb_match(
        artist,
        title_raw,
        title_norm,
        track_titles,
        track_count,
        candidates,
        has_cover=has_cover,
        extra_sources=extra_sources,
    )
    if chosen is None:
        return {"status": "completed", "selected_index": None, "confidence": confidence}
    selected_index = None
    for idx, cand in enumerate(candidates):
        if cand == chosen:
            selected_index = idx
            break
    return {
        "status": "completed",
        "selected_index": selected_index,
        "confidence": confidence,
        "selected_release_group_id": str((chosen[0] or {}).get("id") or "").strip(),
        "selected_release_id": str((chosen[1] or {}).get("id") or "").strip(),
    }


def ai_queue_process_dedupe_for_runtime(runtime: Any, item: dict[str, Any]) -> dict[str, Any]:
    artist = str(item.get("artist") or "").strip()
    raw_editions = list(item.get("editions") or [])
    editions = [dict(row or {}) for row in raw_editions if isinstance(row, dict)]
    if not editions and artist:
        album_id = runtime._parse_int_loose(item.get("album_id"), 0)
        group = runtime._find_duplicate_group_by_artist_album(artist, int(album_id or 0), allow_library_build=True)
        if group:
            editions = [runtime._normalize_edition_as_best(dict(group.get("best") or {}), artist)] + [
                dict(x or {}) for x in list(group.get("losers") or [])
            ]
    if not editions:
        return {"status": "skipped", "reason": "invalid_payload"}
    runtime._ollama_prewarm_model(runtime._ollama_model_configured(), analysis_type="dedupe_choose_best", force=False)
    best = runtime.choose_best(editions, defer_ai=False)
    if not isinstance(best, dict):
        return {"status": "failed", "reason": "no_best_candidate"}
    selected_index = None
    best_album_id = int(runtime._parse_int_loose(best.get("album_id"), 0) or 0)
    best_folder_key = runtime._dupe_folder_key_str(best.get("folder"))
    for idx, row in enumerate(editions):
        if best_album_id > 0 and int(runtime._parse_int_loose(row.get("album_id"), 0) or 0) == best_album_id:
            selected_index = idx
            break
        if best_folder_key and runtime._dupe_folder_key_str(row.get("folder")) == best_folder_key:
            selected_index = idx
            break
    return {
        "status": "completed",
        "selected_index": selected_index,
        "used_ai": bool(best.get("used_ai")),
        "confidence": runtime._parse_int_loose(best.get("ai_confidence"), 0),
        "rationale": str(best.get("rationale") or "").strip(),
        "merge_list": list(best.get("merge_list") or []),
        "selected_album_id": best_album_id or None,
    }


def ai_queue_process_review_for_runtime(runtime: Any, item: dict[str, Any]) -> dict[str, Any]:
    artist = str(item.get("artist") or "").strip()
    album = str(item.get("album") or "").strip()
    candidates = [row for row in list(item.get("candidates") or []) if isinstance(row, dict)]
    if not artist or not album or not candidates:
        return {"status": "skipped", "reason": "invalid_payload"}
    runtime._ollama_prewarm_model(runtime._ollama_model_configured(), analysis_type="album_review_validate", force=False)
    out = runtime._review_validate_candidates_with_ai(artist, album, candidates)
    if not out:
        return {"status": "failed", "reason": "no_validated_candidate"}
    return {
        "status": "completed",
        "confidence": int(runtime._parse_int_loose(out.get("confidence"), 0) or 0),
        "reason": str(out.get("reason") or "").strip(),
        "selected_index": int(runtime._parse_int_loose(out.get("selected_index"), 0) or 0),
        "provider_effective": str(out.get("provider_effective") or "").strip(),
        "model": str(out.get("model") or "").strip(),
    }


def run_ai_domain_worker_for_runtime(runtime: Any, domain: str) -> None:
    domain_norm = str(domain or "").strip().lower()
    try:
        while True:
            with runtime._ai_domain_queue_lock:
                queue_ref = runtime._ai_domain_queues.get(domain_norm)
                if not queue_ref:
                    runtime._ai_domain_states[domain_norm]["running"] = False
                    runtime._ai_domain_worker_started[domain_norm] = False
                    return
                item = dict(queue_ref[0] or {})
                runtime._ai_domain_states[domain_norm].update(
                    {
                        "running": True,
                        "waiting_for_idle_scan": False,
                        "queued": int(len(queue_ref)),
                        "current_label": str(item.get("label") or item.get("artist") or item.get("album") or item.get("title_norm") or "").strip(),
                        "current_model": str(runtime._ollama_model_configured() or ""),
                        "last_started_at": float(time.time()),
                    }
                )
            while ai_queue_domain_requires_idle_scan(domain_norm) and (
                runtime._scan_inline_matching_active() or runtime._ai_scan_lifecycle_phase_active()
            ):
                with runtime._ai_domain_queue_lock:
                    runtime._ai_domain_states[domain_norm]["waiting_for_idle_scan"] = True
                    runtime._ai_domain_states[domain_norm]["queued"] = int(len(runtime._ai_domain_queues.get(domain_norm) or []))
                time.sleep(3.0)
            with runtime._ai_domain_queue_lock:
                runtime._ai_domain_states[domain_norm]["waiting_for_idle_scan"] = False
            started = time.perf_counter()
            error = ""
            try:
                if domain_norm == "matching":
                    result = ai_queue_process_matching_for_runtime(runtime, item)
                elif domain_norm == "dedupe":
                    result = ai_queue_process_dedupe_for_runtime(runtime, item)
                elif domain_norm == "review":
                    result = ai_queue_process_review_for_runtime(runtime, item)
                else:
                    result = {"status": "failed", "reason": "unsupported_domain"}
            except Exception as exc:
                result = {"status": "failed", "reason": str(exc or "").strip() or "queue_processing_failed"}
                error = str(exc or "").strip()
                logging.warning("AI queue %s item failed: %s", domain_norm, exc)
            elapsed_ms = int(max(0.0, (time.perf_counter() - started) * 1000.0))
            status_norm = str(result.get("status") or "failed").strip().lower()
            ai_queue_update_metrics_for_runtime(
                runtime,
                domain_norm,
                status=status_norm,
                latency_ms=elapsed_ms,
                result=result,
                error=error or str(result.get("reason") or ""),
            )
            with runtime._ai_domain_queue_lock:
                queue_ref = runtime._ai_domain_queues.get(domain_norm) or deque()
                if queue_ref:
                    queue_ref.popleft()
                runtime._ai_domain_states[domain_norm]["queued"] = int(len(queue_ref))
                if not queue_ref:
                    runtime._ai_domain_states[domain_norm].update(
                        {
                            "running": False,
                            "waiting_for_idle_scan": False,
                            "current_label": "",
                            "current_model": "",
                        }
                    )
                    runtime._ai_domain_worker_started[domain_norm] = False
                    return
    finally:
        with runtime._ai_domain_queue_lock:
            if domain_norm in runtime._ai_domain_states:
                runtime._ai_domain_states[domain_norm]["running"] = False
                runtime._ai_domain_states[domain_norm]["waiting_for_idle_scan"] = False
            if domain_norm in runtime._ai_domain_worker_started:
                runtime._ai_domain_worker_started[domain_norm] = False


def trigger_ai_domain_queue_async_for_runtime(runtime: Any, domain: str, payload: dict[str, Any]) -> tuple[bool, str]:
    domain_norm = str(domain or "").strip().lower()
    if domain_norm == "incomplete":
        queued, status = runtime._trigger_incomplete_ai_review_async(
            str(payload.get("artist") or "").strip(),
            int(runtime._parse_int_loose(payload.get("album_id"), 0) or 0),
        )
        return queued, status
    if domain_norm not in runtime._AI_QUEUE_DOMAINS:
        return (False, "unsupported_domain")
    job = dict(payload or {})
    label = str(job.get("label") or job.get("artist") or job.get("album") or job.get("title_norm") or "").strip()
    dedupe_key = str(job.get("dedupe_key") or job.get("target_key") or label).strip()
    with runtime._ai_domain_queue_lock:
        queue_ref = runtime._ai_domain_queues.get(domain_norm)
        if queue_ref is None:
            return (False, "unsupported_domain")
        current_label = str((runtime._ai_domain_states.get(domain_norm) or {}).get("current_label") or "").strip()
        if label and label == current_label:
            return (False, "already_running")
        for queued in list(queue_ref):
            queued_key = str((queued or {}).get("dedupe_key") or (queued or {}).get("target_key") or (queued or {}).get("label") or "").strip()
            if dedupe_key and queued_key and dedupe_key == queued_key:
                return (False, "already_queued")
        queue_ref.append(job)
        runtime._ai_domain_states[domain_norm]["queued"] = int(len(queue_ref))
        if runtime._ai_domain_worker_started.get(domain_norm):
            return (True, "queued")
        runtime._ai_domain_worker_started[domain_norm] = True
    threading.Thread(
        target=run_ai_domain_worker_for_runtime,
        args=(runtime, domain_norm),
        daemon=True,
        name=f"ai-queue-{domain_norm}",
    ).start()
    return (True, "queued")
