"""Runtime-owned incomplete album AI assessment and review helpers."""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None

_EXTRACTED_NAMES = {
    '_build_incomplete_assessment',
    '_incomplete_ai_model_sequence',
    '_incomplete_ai_enabled',
    '_incomplete_ai_should_consider',
    '_incomplete_ai_track_rows',
    '_incomplete_ai_diff_summary',
    '_build_incomplete_ai_evidence',
    '_incomplete_ai_response_schema',
    '_incomplete_ai_conflict_schema',
    '_incomplete_ai_allowed_verdicts_for_conflict_type',
    '_build_incomplete_ai_conflict_payload',
    '_build_incomplete_ai_verdict_payload',
    '_normalize_incomplete_ai_conflict',
    '_normalize_incomplete_ai_verdict',
    '_incomplete_ai_should_retry_hard',
    '_incomplete_ai_should_retry_conflict_hard',
    '_run_incomplete_ai_stage',
    '_incomplete_ai_stage_fallback_verdict',
    '_run_incomplete_ai_shadow_verdict',
    '_incomplete_ai_prewarm_sequence',
    '_incomplete_ai_review_status_snapshot',
    '_run_incomplete_ai_review_worker',
    '_trigger_incomplete_ai_review_async',
    '_build_incomplete_assessment_from_payload',
}


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
        if name == "_bind_runtime" or own_wrapper:
            continue
        globals()[name] = value


def _runtime_module() -> Any:
    if _RUNTIME is None:
        raise RuntimeError("incomplete AI runtime is not bound")
    return _RUNTIME


def _build_incomplete_assessment(
    *,
    edition: dict | None,
    tags: dict | None,
    mb_hint: dict | None,
    is_broken_detected: bool,
    expected_track_count: int | None,
    actual_track_count: int,
    missing_indices: list[int] | None,
    strict_reject_reason: str = "",
) -> dict[str, Any]:
    e = edition if isinstance(edition, dict) else {}
    expected = max(0, int(expected_track_count or 0))
    actual = max(0, int(actual_track_count or 0))
    strict_code = _strict_reject_code(strict_reject_reason)
    missing_flat = sorted(
        {
            int(v)
            for v in (missing_indices or [])
            if _parse_int_loose(v, 0) > 0
        }
    )
    local_titles = _local_track_titles_for_strict(list(e.get("tracks") or []))
    expected_titles = _incomplete_expected_track_titles(e, mb_hint=mb_hint)
    deficit = max(0, expected - actual) if expected > 0 else 0

    flags: list[str] = []
    if missing_flat:
        flags.append("local_numbering_gap")
        flags.append(f"missing_run:{_incomplete_longest_missing_run(missing_flat)}")
    if strict_code:
        flags.append(f"strict:{strict_code}")
    if expected > 0 and deficit > 0:
        flags.append(f"provider_delta:{deficit}")

    verdict = _INCOMPLETE_VERDICT_STALE_MATCH
    confidence = 0.90
    source = "local_numbering_only"
    mark_broken = False
    quarantine_eligible = False
    needs_manual_review = False

    if actual <= 0:
        verdict = _INCOMPLETE_VERDICT_CONFIRMED
        confidence = 0.99
        mark_broken = True
        quarantine_eligible = True
        summary = "No readable audio tracks were detected in the local folder."
    elif missing_flat:
        verdict = _INCOMPLETE_VERDICT_CONFIRMED
        confidence = 0.97
        mark_broken = True
        quarantine_eligible = True
        missing_label = ", ".join(str(v) for v in missing_flat[:12])
        if len(missing_flat) > 12:
            missing_label += ", …"
        summary = (
            "The local folder has an obvious numbering hole. "
            f"Missing local track number(s): {missing_label}."
        )
    else:
        summary = (
            "PMDA no longer treats provider-only count differences as incomplete. "
            "This folder has no obvious local numbering holes."
        )

    return {
        "verdict": verdict,
        "confidence": float(max(0.0, min(1.0, confidence))),
        "source": source,
        "mark_broken": bool(mark_broken),
        "quarantine_eligible": bool(quarantine_eligible and verdict in _INCOMPLETE_QUARANTINE_ALLOWED_VERDICTS),
        "flags": list(flags),
        "summary": summary,
        "strict_reject_code": strict_code,
        "expected_track_count": expected,
        "actual_track_count": actual,
        "missing_indices": list(missing_flat),
        "local_track_titles": list(local_titles),
        "expected_track_titles": list(expected_titles),
        "track_overlap": 0.0,
        "track_overlap_exact": 0.0,
        "track_overlap_prefix": 0.0,
        "track_delta": int(deficit),
        "tail_only_missing": False,
        "bonus_tail_suspected": False,
        "edition_variant_cue": False,
        "needs_manual_review": bool(needs_manual_review),
    }
def _incomplete_ai_model_sequence() -> list[str]:
    available = {str(v or "").strip().lower() for v in _ollama_available_models_cached() if str(v or "").strip()}
    preferred: list[str] = []

    def _push(model_name: str | None) -> None:
        raw = str(model_name or "").strip()
        if not raw:
            return
        lowered = raw.lower()
        if available and lowered not in available:
            return
        if raw not in preferred:
            preferred.append(raw)

    configured_base = _ollama_model_configured()
    configured_hard = _ollama_complex_model_configured()
    _push(configured_base)
    if not preferred:
        for fallback in ("qwen2.5:3b-instruct", "qwen3:8b"):
            _push(fallback)
    if configured_hard and str(configured_hard).strip().lower() not in {"qwen3:14b", "qwen3:32b"}:
        _push(configured_hard)
    else:
        _push("qwen3:8b")
    if not preferred and available:
        for candidate in sorted(available):
            _push(candidate)
    return preferred
def _incomplete_ai_enabled() -> bool:
    if not bool(getattr(_runtime_module(), "PROVIDER_IDENTITY_USE_AI", True)):
        return False
    if not _ollama_service_configured():
        return False
    return bool(_incomplete_ai_model_sequence())
def _incomplete_ai_should_consider(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    if not _incomplete_ai_enabled():
        return False
    verdict = str(payload.get("classification") or "").strip()
    if verdict not in _INCOMPLETE_AI_SHADOW_VERDICTS:
        return False
    expected = max(0, int(payload.get("expected_track_count") or 0))
    actual = max(0, int(payload.get("actual_track_count") or 0))
    if verdict == _INCOMPLETE_VERDICT_NUMBERING and expected > 0 and actual == expected:
        return True
    evidence = payload.get("evidence")
    if isinstance(evidence, dict):
        if bool(evidence.get("bonus_tail_suspected")) or bool(evidence.get("edition_variant_cue")):
            return True
        if float(evidence.get("track_overlap") or 0.0) > 0:
            return True
    expected_tracks = payload.get("expected_tracks")
    if isinstance(expected_tracks, list) and expected_tracks:
        return True
    strict_reason = _strict_reject_code(str(payload.get("strict_reject_reason") or ""))
    return strict_reason in (_INCOMPLETE_IDENTITY_REJECT_CODES | _INCOMPLETE_REVIEW_ONLY_REJECT_CODES | _INCOMPLETE_CLASSICAL_REJECT_CODES)
def _incomplete_ai_track_rows(rows: list[Any] | None, *, limit: int | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    row_limit = max(1, int(limit or globals().get("_INCOMPLETE_AI_MAX_TRACK_ROWS", 30)))
    for raw in list(rows or [])[:row_limit]:
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title") or "").strip()
        try:
            index = int(raw.get("index") or raw.get("track_num") or 0)
        except Exception:
            index = 0
        try:
            disc_num = int(raw.get("disc_num") or 0)
        except Exception:
            disc_num = 0
        try:
            duration_sec = int(raw.get("duration_sec") or 0)
        except Exception:
            duration_sec = 0
        out.append(
            {
                "index": index if index > 0 else None,
                "disc_num": disc_num if disc_num > 0 else None,
                "title": title,
                "title_norm": _norm_track_title_strict(title),
                "duration_sec": duration_sec if duration_sec > 0 else None,
            }
        )
    return out
def _incomplete_ai_diff_summary(
    local_tracks: list[dict[str, Any]] | None,
    expected_tracks: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    local_rows = _incomplete_ai_track_rows(local_tracks)
    expected_rows = _incomplete_ai_track_rows(expected_tracks)
    local_titles = [str(row.get("title") or "").strip() for row in local_rows if str(row.get("title") or "").strip()]
    expected_titles = [str(row.get("title") or "").strip() for row in expected_rows if str(row.get("title") or "").strip()]
    local_norm = [str(row.get("title_norm") or "").strip() for row in local_rows if str(row.get("title_norm") or "").strip()]
    expected_norm = [str(row.get("title_norm") or "").strip() for row in expected_rows if str(row.get("title_norm") or "").strip()]
    local_set = set(local_norm)
    expected_set = set(expected_norm)
    shared_norm = expected_set & local_set
    expected_only_norm = [value for value in expected_norm if value and value not in local_set]
    local_only_norm = [value for value in local_norm if value and value not in expected_set]
    expected_title_by_norm = {
        _norm_track_title_strict(str(row.get("title") or "")): str(row.get("title") or "").strip()
        for row in expected_rows
        if str(row.get("title") or "").strip()
    }
    local_title_by_norm = {
        _norm_track_title_strict(str(row.get("title") or "")): str(row.get("title") or "").strip()
        for row in local_rows
        if str(row.get("title") or "").strip()
    }
    expected_only_titles = [expected_title_by_norm.get(value, value) for value in expected_only_norm[:12]]
    local_only_titles = [local_title_by_norm.get(value, value) for value in local_only_norm[:12]]
    expected_only_bonus_like_count = sum(1 for value in expected_only_titles if _incomplete_title_has_variant_cue(value))
    local_only_bonus_like_count = sum(1 for value in local_only_titles if _incomplete_title_has_variant_cue(value))
    return {
        "local_track_count": len(local_rows),
        "expected_track_count_from_tracklist": len(expected_rows),
        "count_match": bool(expected_rows and len(local_rows) == len(expected_rows)),
        "shared_track_count": len(shared_norm),
        "shared_track_ratio": (len(shared_norm) / len(expected_set)) if expected_set else 0.0,
        "local_only_titles": local_only_titles,
        "expected_only_titles": expected_only_titles,
        "local_only_bonus_like_count": int(local_only_bonus_like_count),
        "expected_only_bonus_like_count": int(expected_only_bonus_like_count),
        "local_title_preview": {
            "head": local_titles[:6],
            "tail": local_titles[-4:] if len(local_titles) > 6 else [],
        },
        "expected_title_preview": {
            "head": expected_titles[:6],
            "tail": expected_titles[-4:] if len(expected_titles) > 6 else [],
        },
    }
def _build_incomplete_ai_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    evidence = payload.get("evidence")
    evidence_map = dict(evidence) if isinstance(evidence, dict) else {}
    local_tracks = list(payload.get("local_tracks") or [])
    expected_tracks = list(payload.get("expected_tracks") or [])
    diff_summary = _incomplete_ai_diff_summary(local_tracks, expected_tracks)
    provider_refs = payload.get("provider_refs")
    provider_refs = dict(provider_refs) if isinstance(provider_refs, dict) else {}
    strict_provider = _normalize_identity_provider(str(payload.get("strict_match_provider") or "")) or None
    metadata_source = _normalize_identity_provider(str(payload.get("metadata_source") or "")) or None
    strict_reason = _strict_reject_code(str(payload.get("strict_reject_reason") or ""))
    track_delta = int(evidence_map.get("track_delta") or 0)
    derived_signals = {
        "provider_tracklist_available": bool(expected_tracks),
        "identity_conflict_hint": bool(strict_reason in _INCOMPLETE_IDENTITY_REJECT_CODES),
        "numbering_conflict_hint": bool(
            (diff_summary.get("count_match") and payload.get("missing_indices"))
            or strict_reason == "track_numbering_gap"
        ),
        "edition_conflict_hint": bool(
            evidence_map.get("bonus_tail_suspected")
            or evidence_map.get("edition_variant_cue")
            or int(diff_summary.get("expected_only_bonus_like_count") or 0) > 0
        ),
        "large_track_deficit": bool(track_delta >= 3),
        "small_track_deficit": bool(0 < track_delta < 3),
    }
    return {
        "task": "Classify whether this local album is truly incomplete or better explained by an alternate edition, provider mismatch, numbering issue, or insufficient evidence.",
        "allowed_verdicts": list(_INCOMPLETE_AI_ALLOWED_VERDICTS),
        "allowed_conflict_types": list(_INCOMPLETE_AI_ALLOWED_CONFLICT_TYPES),
        "local_album": {
            "artist": str(payload.get("artist") or "").strip(),
            "album_title": str(payload.get("album_title") or "").strip(),
            "folder_path": str(payload.get("folder_path") or "").strip(),
            "actual_track_count": max(0, int(payload.get("actual_track_count") or 0)),
            "expected_track_count": max(0, int(payload.get("expected_track_count") or 0)),
            "missing_indices": [int(v) for v in list(payload.get("missing_indices") or []) if _parse_int_loose(v, 0) > 0][:64],
            "missing_required_tags": [str(v or "").strip() for v in list(payload.get("missing_required_tags") or []) if str(v or "").strip()][:32],
        },
        "detector_summary": {
            "reason_summary": str(payload.get("reason_summary") or "").strip(),
            "deterministic_verdict": str(payload.get("classification") or "").strip(),
            "deterministic_confidence": float(payload.get("classification_confidence") or 0.0),
            "strict_reject_reason": str(payload.get("strict_reject_reason") or "").strip(),
            "quarantine_eligible": bool(payload.get("quarantine_eligible")),
            "pipeline_status": str(payload.get("pipeline_status") or "").strip(),
        },
        "provider_candidate": {
            "provider": strict_provider or metadata_source,
            "provider_refs": provider_refs,
            "musicbrainz_release_group_id": str(payload.get("musicbrainz_release_group_id") or "").strip(),
        },
        "comparison": diff_summary,
        "derived_signals": derived_signals,
        "evidence_flags": {
            "bonus_tail_suspected": bool(evidence_map.get("bonus_tail_suspected")),
            "edition_variant_cue": bool(evidence_map.get("edition_variant_cue")),
            "tail_only_missing": bool(evidence_map.get("tail_only_missing")),
            "track_overlap": float(evidence_map.get("track_overlap") or 0.0),
            "track_overlap_exact": float(evidence_map.get("track_overlap_exact") or 0.0),
            "track_overlap_prefix": float(evidence_map.get("track_overlap_prefix") or 0.0),
            "track_delta": int(evidence_map.get("track_delta") or 0),
            "flags": [str(v or "").strip() for v in list(evidence_map.get("flags") or []) if str(v or "").strip()][:24],
        },
    }
def _incomplete_ai_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "verdict": {
                "type": "string",
                "enum": list(_INCOMPLETE_AI_ALLOWED_VERDICTS),
            },
            "confidence": {"type": "number"},
            "reasoning_flags": {
                "type": "array",
                "items": {"type": "string"},
                "maxItems": 4,
            },
            "recommended_action": {
                "type": "string",
                "enum": ["keep_review", "do_not_quarantine", "quarantine_candidate"],
            },
            "needs_manual_review": {"type": "boolean"},
            "evidence_summary": {"type": "string"},
        },
        "required": [
            "verdict",
            "confidence",
            "recommended_action",
            "needs_manual_review",
        ],
    }
def _incomplete_ai_conflict_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "conflict_type": {
                "type": "string",
                "enum": list(_INCOMPLETE_AI_ALLOWED_CONFLICT_TYPES),
            },
            "confidence": {"type": "number"},
            "reasoning_flags": {
                "type": "array",
                "items": {"type": "string"},
                "maxItems": 4,
            },
            "needs_manual_review": {"type": "boolean"},
            "evidence_summary": {"type": "string"},
        },
        "required": ["conflict_type", "confidence", "needs_manual_review"],
    }
def _incomplete_ai_allowed_verdicts_for_conflict_type(conflict_type: str) -> tuple[str, ...]:
    mapping = {
        "tracklist_deficit": (
            _INCOMPLETE_VERDICT_CONFIRMED,
            _INCOMPLETE_VERDICT_REVIEW,
            _INCOMPLETE_VERDICT_MANUAL,
        ),
        "edition_conflict": (
            _INCOMPLETE_VERDICT_ALT_EDITION,
            _INCOMPLETE_VERDICT_REVIEW,
            _INCOMPLETE_VERDICT_MANUAL,
        ),
        "identity_conflict": (
            _INCOMPLETE_VERDICT_IDENTITY_MISMATCH,
            _INCOMPLETE_VERDICT_MANUAL,
        ),
        "numbering_conflict": (
            _INCOMPLETE_VERDICT_NUMBERING,
            _INCOMPLETE_VERDICT_MANUAL,
        ),
        "insufficient_evidence": (
            _INCOMPLETE_VERDICT_MANUAL,
            _INCOMPLETE_VERDICT_REVIEW,
        ),
    }
    return tuple(mapping.get(str(conflict_type or "").strip(), (_INCOMPLETE_VERDICT_MANUAL,)))
def _build_incomplete_ai_conflict_payload(evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "task": "Choose the dominant conflict type only. Do not produce a final product verdict yet.",
        "allowed_conflict_types": list(_INCOMPLETE_AI_ALLOWED_CONFLICT_TYPES),
        "local_album": dict(evidence.get("local_album") or {}),
        "detector_summary": dict(evidence.get("detector_summary") or {}),
        "provider_candidate": dict(evidence.get("provider_candidate") or {}),
        "comparison": dict(evidence.get("comparison") or {}),
        "derived_signals": dict(evidence.get("derived_signals") or {}),
        "evidence_flags": dict(evidence.get("evidence_flags") or {}),
    }
def _build_incomplete_ai_verdict_payload(evidence: dict[str, Any], *, conflict_type: str, conflict_confidence: float) -> dict[str, Any]:
    return {
        "task": "Choose the final product verdict using the supplied conflict type and stay inside the allowed verdict list.",
        "allowed_verdicts": list(_incomplete_ai_allowed_verdicts_for_conflict_type(conflict_type)),
        "conflict_type": str(conflict_type or "").strip(),
        "conflict_confidence": float(max(0.0, min(1.0, conflict_confidence or 0.0))),
        "local_album": dict(evidence.get("local_album") or {}),
        "detector_summary": dict(evidence.get("detector_summary") or {}),
        "provider_candidate": dict(evidence.get("provider_candidate") or {}),
        "comparison": dict(evidence.get("comparison") or {}),
        "derived_signals": dict(evidence.get("derived_signals") or {}),
        "evidence_flags": dict(evidence.get("evidence_flags") or {}),
    }
def _normalize_incomplete_ai_conflict(payload: dict[str, Any] | None, *, deterministic_verdict: str, deterministic_confidence: float) -> dict[str, Any]:
    out = dict(payload) if isinstance(payload, dict) else {}
    conflict_type = str(out.get("conflict_type") or "").strip()
    if conflict_type not in _INCOMPLETE_AI_ALLOWED_CONFLICT_TYPES:
        conflict_type = "insufficient_evidence"
    confidence = _safe_bounded_float(out.get("confidence"), minimum=0.0, maximum=1.0)
    if confidence is None:
        confidence = 0.0
    reasoning_flags = [str(v or "").strip() for v in list(out.get("reasoning_flags") or []) if str(v or "").strip()][:8]
    summary = str(out.get("evidence_summary") or "").strip() or "AI conflict classification produced no usable explanation."
    needs_manual_review = bool(out.get("needs_manual_review"))
    if conflict_type == "insufficient_evidence":
        needs_manual_review = True
    return {
        "status": "completed",
        "provider": "ollama",
        "shadow_mode": True,
        "prompt_version": _INCOMPLETE_AI_PROMPT_VERSION,
        "stage": "conflict_type",
        "conflict_type": conflict_type,
        "confidence": float(confidence),
        "reasoning_flags": reasoning_flags,
        "needs_manual_review": bool(needs_manual_review),
        "evidence_summary": summary,
        "deterministic_verdict": str(deterministic_verdict or "").strip(),
        "deterministic_confidence": float(max(0.0, min(1.0, deterministic_confidence or 0.0))),
        "created_at": float(time.time()),
    }
def _normalize_incomplete_ai_verdict(payload: dict[str, Any] | None, *, deterministic_verdict: str, deterministic_confidence: float) -> dict[str, Any]:
    out = dict(payload) if isinstance(payload, dict) else {}
    verdict = str(out.get("verdict") or "").strip()
    if verdict not in _INCOMPLETE_AI_ALLOWED_VERDICTS:
        verdict = _INCOMPLETE_VERDICT_MANUAL
    confidence = _safe_bounded_float(out.get("confidence"), minimum=0.0, maximum=1.0)
    if confidence is None:
        confidence = 0.0
    reasoning_flags = [str(v or "").strip() for v in list(out.get("reasoning_flags") or []) if str(v or "").strip()][:8]
    summary = str(out.get("evidence_summary") or "").strip()
    if not summary:
        summary = "AI shadow review did not provide a usable explanation."
    recommended_action = str(out.get("recommended_action") or "").strip().lower()
    if recommended_action not in {"keep_review", "do_not_quarantine", "quarantine_candidate"}:
        if verdict == _INCOMPLETE_VERDICT_CONFIRMED and confidence >= 0.85:
            recommended_action = "quarantine_candidate"
        elif verdict in {_INCOMPLETE_VERDICT_ALT_EDITION, _INCOMPLETE_VERDICT_IDENTITY_MISMATCH, _INCOMPLETE_VERDICT_NUMBERING}:
            recommended_action = "do_not_quarantine"
        else:
            recommended_action = "keep_review"
    needs_manual_review = bool(out.get("needs_manual_review"))
    if verdict in {_INCOMPLETE_VERDICT_REVIEW, _INCOMPLETE_VERDICT_MANUAL}:
        needs_manual_review = True
    elif verdict == _INCOMPLETE_VERDICT_CONFIRMED and confidence >= 0.85:
        needs_manual_review = False
    return {
        "status": "completed",
        "provider": "ollama",
        "shadow_mode": True,
        "prompt_version": _INCOMPLETE_AI_PROMPT_VERSION,
        "verdict": verdict,
        "confidence": float(confidence),
        "reasoning_flags": reasoning_flags,
        "recommended_action": recommended_action,
        "needs_manual_review": bool(needs_manual_review),
        "evidence_summary": summary,
        "deterministic_verdict": str(deterministic_verdict or "").strip(),
        "deterministic_confidence": float(max(0.0, min(1.0, deterministic_confidence or 0.0))),
        "ai_overrides_deterministic": bool(verdict and verdict != str(deterministic_verdict or "").strip()),
        "created_at": float(time.time()),
    }
def _incomplete_ai_should_retry_hard(result: dict[str, Any] | None, *, evidence: dict[str, Any]) -> bool:
    if not isinstance(result, dict):
        return True
    confidence = _safe_bounded_float(result.get("confidence"), minimum=0.0, maximum=1.0) or 0.0
    verdict = str(result.get("verdict") or "").strip()
    track_delta = int((((evidence or {}).get("evidence_flags") or {}).get("track_delta") or 0))
    if confidence < 0.55:
        return True
    if track_delta <= 2 and verdict in {_INCOMPLETE_VERDICT_REVIEW, _INCOMPLETE_VERDICT_MANUAL}:
        return False
    if verdict in {_INCOMPLETE_VERDICT_REVIEW, _INCOMPLETE_VERDICT_MANUAL} and confidence < 0.80:
        return True
    if track_delta >= 4 and verdict != _INCOMPLETE_VERDICT_CONFIRMED and confidence < 0.85:
        return True
    return False
def _incomplete_ai_should_retry_conflict_hard(result: dict[str, Any] | None, *, evidence: dict[str, Any]) -> bool:
    if not isinstance(result, dict):
        return True
    confidence = _safe_bounded_float(result.get("confidence"), minimum=0.0, maximum=1.0) or 0.0
    conflict_type = str(result.get("conflict_type") or "").strip()
    comparison = dict((evidence or {}).get("comparison") or {})
    derived = dict((evidence or {}).get("derived_signals") or {})
    shared_ratio = float(comparison.get("shared_track_ratio") or 0.0)
    track_delta = int((((evidence or {}).get("evidence_flags") or {}).get("track_delta") or 0))
    if confidence < 0.65:
        return True
    if conflict_type == "insufficient_evidence" and (track_delta >= 3 or shared_ratio >= 0.72):
        return True
    if conflict_type == "tracklist_deficit" and not bool(derived.get("provider_tracklist_available")):
        return True
    return False
def _run_incomplete_ai_stage(
    *,
    payload_json: str,
    system_msg: str,
    schema: dict[str, Any],
    deterministic_verdict: str,
    deterministic_confidence: float,
    stage_name: str,
    normalizer,
    should_retry,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    model_sequence = _incomplete_ai_model_sequence()
    last_error = ""
    evidence_hash = hashlib.sha1(payload_json.encode("utf-8", errors="ignore")).hexdigest()
    for idx, model_name in enumerate([m for m in model_sequence if str(m or "").strip()]):
        timeout_sec = _INCOMPLETE_AI_HARD_TIMEOUT_SEC if idx > 0 else _INCOMPLETE_AI_FAST_TIMEOUT_SEC
        started_at = time.time()
        status = "failed"
        response_obj: dict[str, Any] | None = None
        error_msg = ""
        try:
            response_obj = _ollama_chat_json(
                model_name=model_name,
                system_msg=system_msg,
                user_msg=payload_json,
                timeout_sec=timeout_sec,
                max_tokens=96,
                format_schema=schema,
                analysis_type="incomplete_album_arbitration",
            )
            raw_content = str(((response_obj.get("message") or {}) if isinstance(response_obj, dict) else {}).get("content") or "").strip()
            payload_obj = _assistant_extract_json_object(raw_content)
            if not payload_obj:
                raise RuntimeError(f"Ollama {stage_name} returned no JSON object")
            normalized = normalizer(
                payload_obj,
                deterministic_verdict=deterministic_verdict,
                deterministic_confidence=deterministic_confidence,
            )
            normalized["model"] = model_name
            normalized["evidence_hash"] = evidence_hash
            normalized["stage"] = stage_name
            status = "completed"
            if callable(globals().get("record_ai_usage")):
                globals()["record_ai_usage"](
                    provider="ollama",
                    model=model_name,
                    endpoint_kind="text",
                    analysis_type="incomplete_album_arbitration",
                    started_at=started_at,
                    status=status,
                    response_obj=payload_obj,
                    image_inputs=0,
                    error="",
                    metadata={
                        "stage": stage_name,
                        **dict(metadata or {}),
                    },
                )
            if idx < (len(model_sequence) - 1) and should_retry(normalized):
                continue
            return normalized
        except Exception as exc:
            error_msg = str(exc or "").strip()
            last_error = error_msg
            if callable(globals().get("record_ai_usage")):
                globals()["record_ai_usage"](
                    provider="ollama",
                    model=model_name,
                    endpoint_kind="text",
                    analysis_type="incomplete_album_arbitration",
                    started_at=started_at,
                    status=status,
                    response_obj=response_obj,
                    image_inputs=0,
                    error=error_msg,
                    metadata={
                        "stage": stage_name,
                        **dict(metadata or {}),
                    },
                )
            if idx < (len(model_sequence) - 1):
                continue
    return {
        "status": "failed",
        "provider": "ollama",
        "shadow_mode": True,
        "prompt_version": _INCOMPLETE_AI_PROMPT_VERSION,
        "stage": stage_name,
        "error": last_error or f"{stage_name}_failed",
        "deterministic_verdict": deterministic_verdict,
        "deterministic_confidence": deterministic_confidence,
        "created_at": float(time.time()),
    }
def _incomplete_ai_stage_fallback_verdict(
    conflict_result: dict[str, Any],
    *,
    deterministic_verdict: str,
    deterministic_confidence: float,
) -> dict[str, Any]:
    conflict_type = str(conflict_result.get("conflict_type") or "insufficient_evidence").strip()
    confidence = _safe_bounded_float(conflict_result.get("confidence"), minimum=0.0, maximum=1.0) or 0.0
    if conflict_type == "identity_conflict":
        verdict = _INCOMPLETE_VERDICT_IDENTITY_MISMATCH
        recommended_action = "do_not_quarantine"
        needs_manual_review = False
    elif conflict_type == "numbering_conflict":
        verdict = _INCOMPLETE_VERDICT_NUMBERING
        recommended_action = "do_not_quarantine"
        needs_manual_review = False
    elif conflict_type == "edition_conflict":
        verdict = _INCOMPLETE_VERDICT_ALT_EDITION
        recommended_action = "do_not_quarantine"
        needs_manual_review = False
    elif conflict_type == "tracklist_deficit":
        verdict = _INCOMPLETE_VERDICT_CONFIRMED if str(deterministic_verdict or "") == _INCOMPLETE_VERDICT_CONFIRMED else _INCOMPLETE_VERDICT_REVIEW
        recommended_action = "quarantine_candidate" if verdict == _INCOMPLETE_VERDICT_CONFIRMED and confidence >= 0.85 else "keep_review"
        needs_manual_review = verdict != _INCOMPLETE_VERDICT_CONFIRMED
    else:
        verdict = _INCOMPLETE_VERDICT_MANUAL
        recommended_action = "keep_review"
        needs_manual_review = True
    return {
        "status": "completed",
        "provider": "ollama",
        "shadow_mode": True,
        "prompt_version": _INCOMPLETE_AI_PROMPT_VERSION,
        "verdict": verdict,
        "confidence": float(confidence),
        "reasoning_flags": list(conflict_result.get("reasoning_flags") or []),
        "recommended_action": recommended_action,
        "needs_manual_review": bool(needs_manual_review),
        "evidence_summary": str(conflict_result.get("evidence_summary") or "").strip() or "Conflict stage fallback verdict.",
        "deterministic_verdict": str(deterministic_verdict or "").strip(),
        "deterministic_confidence": float(max(0.0, min(1.0, deterministic_confidence or 0.0))),
        "ai_overrides_deterministic": bool(verdict and verdict != str(deterministic_verdict or "").strip()),
        "created_at": float(time.time()),
        "conflict_type": conflict_type,
        "conflict_confidence": float(confidence),
        "fallback_from_conflict": True,
        "model": str(conflict_result.get("model") or "").strip(),
    }
def _run_incomplete_ai_shadow_verdict(payload: dict[str, Any], *, force: bool = False) -> dict[str, Any]:
    deterministic_verdict = str(payload.get("classification") or "").strip()
    deterministic_confidence = float(payload.get("classification_confidence") or 0.0)
    existing = payload.get("ai_verdict")
    if isinstance(existing, dict) and existing and not force:
        return dict(existing)
    if not _incomplete_ai_should_consider(payload):
        return {
            "status": "skipped",
            "provider": "ollama",
            "shadow_mode": True,
            "prompt_version": _INCOMPLETE_AI_PROMPT_VERSION,
            "reason": "not_eligible_for_ai_shadow",
            "deterministic_verdict": deterministic_verdict,
            "deterministic_confidence": deterministic_confidence,
            "created_at": float(time.time()),
        }
    evidence = _build_incomplete_ai_evidence(payload)
    conflict_payload = json.dumps(_build_incomplete_ai_conflict_payload(evidence), ensure_ascii=True, sort_keys=True)
    conflict_result = _run_incomplete_ai_stage(
        payload_json=conflict_payload,
        system_msg=(
            "You are a conservative self-hosted music library conflict classifier.\n"
            "Choose the dominant conflict type only: tracklist_deficit, edition_conflict, identity_conflict, numbering_conflict, or insufficient_evidence.\n"
            "Do not produce a final product verdict yet.\n"
            "Never call a provider mismatch a tracklist deficit.\n"
            "Never call numbering gaps alone a tracklist deficit when the effective file count matches.\n"
            "Use only the provided JSON. Return JSON only.\n"
        ),
        schema=_incomplete_ai_conflict_schema(),
        deterministic_verdict=deterministic_verdict,
        deterministic_confidence=deterministic_confidence,
        stage_name="conflict_type",
        normalizer=_normalize_incomplete_ai_conflict,
        should_retry=lambda normalized: _incomplete_ai_should_retry_conflict_hard(normalized, evidence=evidence),
        metadata={
            "deterministic_verdict": deterministic_verdict,
            "deterministic_confidence": float(deterministic_confidence or 0.0),
            "track_delta": int((((evidence or {}).get("evidence_flags") or {}).get("track_delta") or 0)),
        },
    )
    if str(conflict_result.get("status") or "") != "completed":
        return conflict_result
    conflict_type = str(conflict_result.get("conflict_type") or "insufficient_evidence").strip()
    conflict_confidence = float(conflict_result.get("confidence") or 0.0)
    verdict_payload = json.dumps(
        _build_incomplete_ai_verdict_payload(evidence, conflict_type=conflict_type, conflict_confidence=conflict_confidence),
        ensure_ascii=True,
        sort_keys=True,
    )
    verdict_result = _run_incomplete_ai_stage(
        payload_json=verdict_payload,
        system_msg=(
            "You are a conservative self-hosted music library arbitrator.\n"
            "Choose the final product verdict using the supplied conflict type and stay inside the allowed verdict list.\n"
            "Prefer not_incomplete outcomes when the evidence points to edition, identity, or numbering issues.\n"
            "If evidence is weak, choose insufficient_evidence_manual_review or likely_incomplete_review.\n"
            "Use only the provided JSON. Return JSON only.\n"
        ),
        schema=_incomplete_ai_response_schema(),
        deterministic_verdict=deterministic_verdict,
        deterministic_confidence=deterministic_confidence,
        stage_name="product_verdict",
        normalizer=_normalize_incomplete_ai_verdict,
        should_retry=lambda normalized: _incomplete_ai_should_retry_hard(normalized, evidence=evidence),
        metadata={
            "deterministic_verdict": deterministic_verdict,
            "deterministic_confidence": float(deterministic_confidence or 0.0),
            "conflict_type": conflict_type,
            "conflict_confidence": float(conflict_confidence),
            "track_delta": int((((evidence or {}).get("evidence_flags") or {}).get("track_delta") or 0)),
        },
    )
    if str(verdict_result.get("status") or "") != "completed":
        if conflict_confidence >= 0.84:
            verdict_result = _incomplete_ai_stage_fallback_verdict(
                conflict_result,
                deterministic_verdict=deterministic_verdict,
                deterministic_confidence=deterministic_confidence,
            )
        else:
            return verdict_result
    verdict_result["conflict_type"] = conflict_type
    verdict_result["conflict_confidence"] = float(conflict_confidence)
    verdict_result["conflict_reasoning_flags"] = list(conflict_result.get("reasoning_flags") or [])
    if not str(verdict_result.get("evidence_summary") or "").strip() and str(conflict_result.get("evidence_summary") or "").strip():
        verdict_result["evidence_summary"] = str(conflict_result.get("evidence_summary") or "").strip()
    return verdict_result
def _incomplete_ai_prewarm_sequence(*, force: bool = False) -> list[str]:
    warmed: list[str] = []
    for model_name in [m for m in _incomplete_ai_model_sequence() if str(m or "").strip()]:
        if _ollama_prewarm_model(
            str(model_name or "").strip(),
            analysis_type="incomplete_album_arbitration",
            force=force,
        ):
            warmed.append(str(model_name or "").strip())
    return warmed
def _incomplete_ai_review_status_snapshot() -> dict[str, Any]:
    with _incomplete_ai_review_queue_lock:
        snapshot = dict(_incomplete_ai_review_state or {})
        snapshot["queued"] = int(len(_incomplete_ai_review_queue))
        snapshot["worker_started"] = bool(_incomplete_ai_review_worker_started)
        return snapshot
def _run_incomplete_ai_review_worker() -> None:
    global _incomplete_ai_review_worker_started
    while True:
        with _incomplete_ai_review_queue_lock:
            if not _incomplete_ai_review_queue:
                _incomplete_ai_review_state.update(
                    {
                        "running": False,
                        "waiting_for_idle_scan": False,
                        "queued": 0,
                        "current_artist": "",
                        "current_album_id": 0,
                        "current_model": "",
                    }
                )
                _incomplete_ai_review_worker_started = False
                return
            item = dict(_incomplete_ai_review_queue[0] or {})
            artist = str(item.get("artist") or "").strip()
            album_id = _parse_int_loose(item.get("album_id"), 0)
            _incomplete_ai_review_state.update(
                {
                    "running": True,
                    "waiting_for_idle_scan": False,
                    "queued": int(len(_incomplete_ai_review_queue)),
                    "current_artist": artist,
                    "current_album_id": int(album_id),
                    "current_model": str((_incomplete_ai_model_sequence() or [""])[0] or ""),
                    "last_started_at": float(time.time()),
                }
            )
        while _scan_inline_matching_active() or _ai_scan_lifecycle_phase_active():
            with _incomplete_ai_review_queue_lock:
                _incomplete_ai_review_state["waiting_for_idle_scan"] = True
                _incomplete_ai_review_state["queued"] = int(len(_incomplete_ai_review_queue))
            time.sleep(5.0)
        with _incomplete_ai_review_queue_lock:
            _incomplete_ai_review_state["waiting_for_idle_scan"] = False
        _incomplete_ai_prewarm_sequence(force=False)
        last_status = "failed"
        last_error = ""
        started = time.perf_counter()
        verdict: dict[str, Any] = {}
        try:
            payload = _refresh_broken_album_row(
                artist,
                int(album_id),
                refresh_ai=True,
                allow_provider_lookup=False,
            )
            verdict = dict((payload or {}).get("ai_verdict") or {})
            last_status = str(verdict.get("status") or "completed").strip() or "completed"
            last_error = str(verdict.get("error") or verdict.get("reason") or "").strip()
        except Exception as exc:
            last_status = "failed"
            last_error = str(exc or "").strip() or "incomplete_ai_review_failed"
            logging.warning(
                "Incomplete AI review queue item failed artist=%s album_id=%s: %s",
                artist,
                album_id,
                exc,
            )
        finally:
            elapsed_ms = int(max(0.0, (time.perf_counter() - started) * 1000.0))
            with _incomplete_ai_review_queue_lock:
                if _incomplete_ai_review_queue:
                    _incomplete_ai_review_queue.popleft()
                status_norm = str(last_status or "failed").strip().lower()
                if status_norm == "completed":
                    _incomplete_ai_review_state["completed_count"] = int(_incomplete_ai_review_state.get("completed_count") or 0) + 1
                elif status_norm == "skipped":
                    _incomplete_ai_review_state["skipped_count"] = int(_incomplete_ai_review_state.get("skipped_count") or 0) + 1
                else:
                    _incomplete_ai_review_state["failed_count"] = int(_incomplete_ai_review_state.get("failed_count") or 0) + 1
                done_before = (
                    int(_incomplete_ai_review_state.get("completed_count") or 0)
                    + int(_incomplete_ai_review_state.get("failed_count") or 0)
                    + int(_incomplete_ai_review_state.get("skipped_count") or 0)
                    - 1
                )
                prev_avg = float(_incomplete_ai_review_state.get("avg_latency_ms") or 0.0)
                _incomplete_ai_review_state["avg_latency_ms"] = round(
                    ((prev_avg * max(0, done_before)) + elapsed_ms) / max(1, done_before + 1),
                    2,
                )
                _incomplete_ai_review_state.update(
                    {
                        "queued": int(len(_incomplete_ai_review_queue)),
                        "last_status": last_status,
                        "last_error": last_error,
                        "last_finished_at": float(time.time()),
                        "last_completed_artist": artist,
                        "last_completed_album_id": int(album_id),
                        "last_latency_ms": elapsed_ms,
                        "last_result": dict(verdict or {}),
                    }
                )
def _trigger_incomplete_ai_review_async(artist: str, album_id: int) -> tuple[bool, str]:
    global _incomplete_ai_review_worker_started
    artist_key = str(artist or "").strip()
    album_key = _parse_int_loose(album_id, 0)
    if not artist_key or album_key <= 0:
        return (False, "invalid_target")
    with _incomplete_ai_review_queue_lock:
        current_artist = str(_incomplete_ai_review_state.get("current_artist") or "").strip()
        current_album_id = _parse_int_loose(_incomplete_ai_review_state.get("current_album_id"), 0)
        if current_artist == artist_key and current_album_id == album_key:
            return (False, "already_running")
        for queued in list(_incomplete_ai_review_queue):
            if str((queued or {}).get("artist") or "").strip() == artist_key and _parse_int_loose((queued or {}).get("album_id"), 0) == album_key:
                return (False, "already_queued")
        _incomplete_ai_review_queue.append({"artist": artist_key, "album_id": int(album_key)})
        _incomplete_ai_review_state["queued"] = int(len(_incomplete_ai_review_queue))
        if _incomplete_ai_review_worker_started:
            return (True, "queued")
        _incomplete_ai_review_worker_started = True
    threading.Thread(
        target=_run_incomplete_ai_review_worker,
        daemon=True,
        name="incomplete-ai-review",
    ).start()
    return (True, "queued")
def _build_incomplete_assessment_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    local_tracks = list(payload.get("local_tracks") or [])
    local_tracks_verified = bool(payload.get("_local_tracks_verified"))

    def _track_index_value(track: dict[str, Any]) -> int:
        if not isinstance(track, dict):
            return 0
        return _parse_int_loose(track.get("track_num") or track.get("index"), 0)

    def _track_disc_value(track: dict[str, Any]) -> int:
        if not isinstance(track, dict):
            return 1
        return max(1, _parse_int_loose(track.get("disc_num") or track.get("disc"), 1) or 1)

    def _local_missing_indices(track_rows: list[dict[str, Any]]) -> list[int]:
        grouped: dict[int, set[int]] = defaultdict(set)
        for track in track_rows:
            idx = _track_index_value(track)
            if idx <= 0:
                continue
            grouped[_track_disc_value(track)].add(idx)
        if len(grouped) != 1:
            return []
        indices = sorted(next(iter(grouped.values())))
        if len(indices) < 2:
            return []
        missing: list[int] = []
        for prev_idx, next_idx in zip(indices, indices[1:]):
            if (next_idx - prev_idx) <= 1:
                continue
            missing.extend(list(range(prev_idx + 1, next_idx)))
            if len(missing) > 5000:
                return missing[:5000]
        return missing

    actual_track_count = int(len(local_tracks))
    missing_indices = _local_missing_indices(local_tracks)
    payload["actual_track_count"] = actual_track_count
    payload["missing_indices"] = list(missing_indices)

    if local_tracks and not missing_indices:
        payload["expected_track_count"] = int(actual_track_count or 0)
        payload["strict_reject_reason"] = ""
        payload["reason_summary"] = (
            "Current folder has no obvious local numbering holes. "
            "PMDA only keeps incomplete rows when the local folder has zero readable tracks or clear numbering gaps."
        )
        return {
            "verdict": _INCOMPLETE_VERDICT_STALE_MATCH,
            "confidence": 0.99,
            "source": "local_numbering_only",
            "mark_broken": False,
            "quarantine_eligible": False,
            "needs_manual_review": False,
            "summary": str(payload.get("reason_summary") or ""),
            "expected_track_count": int(actual_track_count or 0),
            "actual_track_count": int(actual_track_count or 0),
            "missing_indices": [],
            "missing_required_tags": [],
        }

    if local_tracks and missing_indices:
        payload["expected_track_count"] = int(actual_track_count + len(missing_indices))
        payload["strict_reject_reason"] = "Local Track Numbering Gap"
        payload["reason_summary"] = (
            "Local folder has obvious numbering holes; missing track number(s): "
            + ", ".join(str(v) for v in missing_indices)
            + "."
        )
        return {
            "verdict": _INCOMPLETE_VERDICT_CONFIRMED,
            "confidence": 0.98,
            "source": "local_numbering_only",
            "mark_broken": True,
            "quarantine_eligible": True,
            "needs_manual_review": False,
            "summary": str(payload.get("reason_summary") or ""),
            "expected_track_count": int(payload.get("expected_track_count") or 0),
            "actual_track_count": int(actual_track_count or 0),
            "missing_indices": list(missing_indices),
            "missing_required_tags": [],
        }

    if local_tracks_verified:
        payload["expected_track_count"] = 0
        payload["actual_track_count"] = 0
        payload["missing_indices"] = []
        payload["strict_reject_reason"] = "No Local Tracks Readable"
        payload["reason_summary"] = "PMDA could not read any local audio track from the folder."
        return {
            "verdict": _INCOMPLETE_VERDICT_CONFIRMED,
            "confidence": 0.99,
            "source": "local_numbering_only",
            "mark_broken": True,
            "quarantine_eligible": True,
            "needs_manual_review": False,
            "summary": str(payload.get("reason_summary") or ""),
            "expected_track_count": 0,
            "actual_track_count": 0,
            "missing_indices": [],
            "missing_required_tags": [],
        }

    payload["expected_track_count"] = 0
    payload["actual_track_count"] = 0
    payload["missing_indices"] = []
    payload["strict_reject_reason"] = ""
    payload["reason_summary"] = (
        "PMDA could not verify the local folder anymore. "
        "The stored incomplete snapshot was stale."
    )
    return {
        "verdict": _INCOMPLETE_VERDICT_STALE_MATCH,
        "confidence": 0.98,
        "source": "local_numbering_only",
        "mark_broken": False,
        "quarantine_eligible": False,
        "needs_manual_review": False,
        "summary": str(payload.get("reason_summary") or ""),
        "expected_track_count": 0,
        "actual_track_count": 0,
        "missing_indices": [],
        "missing_required_tags": [],
    }

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}

def _build_incomplete_assessment_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_incomplete_assessment(*args, **kwargs)

def _incomplete_ai_model_sequence_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_model_sequence(*args, **kwargs)

def _incomplete_ai_enabled_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_enabled(*args, **kwargs)

def _incomplete_ai_should_consider_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_should_consider(*args, **kwargs)

def _incomplete_ai_track_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_track_rows(*args, **kwargs)

def _incomplete_ai_diff_summary_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_diff_summary(*args, **kwargs)

def _build_incomplete_ai_evidence_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_incomplete_ai_evidence(*args, **kwargs)

def _incomplete_ai_response_schema_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_response_schema(*args, **kwargs)

def _incomplete_ai_conflict_schema_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_conflict_schema(*args, **kwargs)

def _incomplete_ai_allowed_verdicts_for_conflict_type_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_allowed_verdicts_for_conflict_type(*args, **kwargs)

def _build_incomplete_ai_conflict_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_incomplete_ai_conflict_payload(*args, **kwargs)

def _build_incomplete_ai_verdict_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_incomplete_ai_verdict_payload(*args, **kwargs)

def _normalize_incomplete_ai_conflict_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _normalize_incomplete_ai_conflict(*args, **kwargs)

def _normalize_incomplete_ai_verdict_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _normalize_incomplete_ai_verdict(*args, **kwargs)

def _incomplete_ai_should_retry_hard_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_should_retry_hard(*args, **kwargs)

def _incomplete_ai_should_retry_conflict_hard_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_should_retry_conflict_hard(*args, **kwargs)

def _run_incomplete_ai_stage_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_incomplete_ai_stage(*args, **kwargs)

def _incomplete_ai_stage_fallback_verdict_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_stage_fallback_verdict(*args, **kwargs)

def _run_incomplete_ai_shadow_verdict_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_incomplete_ai_shadow_verdict(*args, **kwargs)

def _incomplete_ai_prewarm_sequence_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_prewarm_sequence(*args, **kwargs)

def _incomplete_ai_review_status_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _incomplete_ai_review_status_snapshot(*args, **kwargs)

def _run_incomplete_ai_review_worker_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_incomplete_ai_review_worker(*args, **kwargs)

def _trigger_incomplete_ai_review_async_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _trigger_incomplete_ai_review_async(*args, **kwargs)

def _build_incomplete_assessment_from_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_incomplete_assessment_from_payload(*args, **kwargs)
