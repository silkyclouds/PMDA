"""Runtime-bound AI usage accounting and cost rollup helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_ai_usage_level_overrides',
    '_apply_ai_usage_level',
    '_ai_usage_extract_tokens',
    '_ai_price_lookup',
    '_ai_compute_costs',
    '_ai_breakdown_for_scan',
    '_ai_lifecycle_complete_for_scan',
    '_ai_refresh_rollup_for_scan',
    '_ai_usage_flush_batch',
    '_ai_scan_cost_summary',
    '_analysis_dir_path',
    '_latest_ai_benchmark_for_domain',
    '_ai_domain_usage_summary',
    '_ai_overview_snapshot',
}

_MUTABLE_GLOBAL_NAMES = {
    "AI_USAGE_LEVEL",
    "USE_AI_FOR_MB_MATCH",
    "USE_AI_FOR_MB_VERIFY",
    "USE_AI_FOR_DEDUPE",
    "USE_AI_VISION_FOR_COVER",
    "USE_AI_VISION_BEFORE_COVER_INJECT",
    "USE_WEB_SEARCH_FOR_MB",
    "USE_AI_WEB_SEARCH_FALLBACK",
    "PROVIDER_IDENTITY_USE_AI",
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


def _sync_runtime_globals(runtime: Any) -> None:
    for name in _MUTABLE_GLOBAL_NAMES:
        if name in globals():
            try:
                setattr(runtime, name, globals()[name])
            except Exception:
                pass


def _ai_usage_level_overrides(level: str) -> dict[str, bool]:
    normalized = _normalize_ai_usage_level(level)
    if normalized == "limited":
        return {
            "USE_AI_FOR_MB_MATCH": False,
            "USE_AI_FOR_MB_VERIFY": True,
            "USE_AI_FOR_DEDUPE": False,
            "USE_AI_VISION_FOR_COVER": False,
            "USE_AI_VISION_BEFORE_COVER_INJECT": False,
            "USE_WEB_SEARCH_FOR_MB": False,
            "USE_AI_WEB_SEARCH_FALLBACK": False,
            "PROVIDER_IDENTITY_USE_AI": False,
        }
    if normalized == "auto":
        return {
            "USE_AI_FOR_MB_MATCH": True,
            "USE_AI_FOR_MB_VERIFY": True,
            "USE_AI_FOR_DEDUPE": True,
            "USE_AI_VISION_FOR_COVER": False,
            "USE_AI_VISION_BEFORE_COVER_INJECT": False,
            "USE_WEB_SEARCH_FOR_MB": True,
            "USE_AI_WEB_SEARCH_FALLBACK": True,
            "PROVIDER_IDENTITY_USE_AI": True,
        }
    if normalized == "aggressive":
        return {
            "USE_AI_FOR_MB_MATCH": True,
            "USE_AI_FOR_MB_VERIFY": True,
            "USE_AI_FOR_DEDUPE": True,
            "USE_AI_VISION_FOR_COVER": True,
            "USE_AI_VISION_BEFORE_COVER_INJECT": True,
            "USE_WEB_SEARCH_FOR_MB": True,
            "USE_AI_WEB_SEARCH_FALLBACK": True,
            "PROVIDER_IDENTITY_USE_AI": True,
        }
    return {
        "USE_AI_FOR_MB_MATCH": False,
        "USE_AI_FOR_MB_VERIFY": True,
        "USE_AI_FOR_DEDUPE": True,
        "USE_AI_VISION_FOR_COVER": False,
        "USE_AI_VISION_BEFORE_COVER_INJECT": False,
        "USE_WEB_SEARCH_FOR_MB": False,
        "USE_AI_WEB_SEARCH_FALLBACK": True,
        "PROVIDER_IDENTITY_USE_AI": True,
    }


def _apply_ai_usage_level(level: str | None = None) -> str:
    """Apply the single UX AI usage level to all runtime AI feature flags."""
    global AI_USAGE_LEVEL
    global USE_AI_FOR_MB_MATCH, USE_AI_FOR_MB_VERIFY, USE_AI_FOR_DEDUPE
    global USE_AI_VISION_FOR_COVER, USE_AI_VISION_BEFORE_COVER_INJECT
    global USE_WEB_SEARCH_FOR_MB, USE_AI_WEB_SEARCH_FALLBACK, PROVIDER_IDENTITY_USE_AI

    normalized = _normalize_ai_usage_level(level if level is not None else AI_USAGE_LEVEL)
    AI_USAGE_LEVEL = normalized

    overrides = _ai_usage_level_overrides(normalized)
    USE_AI_FOR_MB_MATCH = bool(overrides["USE_AI_FOR_MB_MATCH"])
    USE_AI_FOR_MB_VERIFY = bool(overrides["USE_AI_FOR_MB_VERIFY"])
    USE_AI_FOR_DEDUPE = bool(overrides["USE_AI_FOR_DEDUPE"])
    USE_AI_VISION_FOR_COVER = bool(overrides["USE_AI_VISION_FOR_COVER"])
    USE_AI_VISION_BEFORE_COVER_INJECT = bool(overrides["USE_AI_VISION_BEFORE_COVER_INJECT"])
    USE_WEB_SEARCH_FOR_MB = bool(overrides["USE_WEB_SEARCH_FOR_MB"])
    USE_AI_WEB_SEARCH_FALLBACK = bool(overrides["USE_AI_WEB_SEARCH_FALLBACK"])
    PROVIDER_IDENTITY_USE_AI = bool(overrides["PROVIDER_IDENTITY_USE_AI"])

    merged["AI_USAGE_LEVEL"] = normalized
    merged["USE_AI_FOR_MB_MATCH"] = USE_AI_FOR_MB_MATCH
    merged["USE_AI_FOR_MB_VERIFY"] = USE_AI_FOR_MB_VERIFY
    merged["USE_AI_FOR_DEDUPE"] = USE_AI_FOR_DEDUPE
    merged["USE_AI_VISION_FOR_COVER"] = USE_AI_VISION_FOR_COVER
    merged["USE_AI_VISION_BEFORE_COVER_INJECT"] = USE_AI_VISION_BEFORE_COVER_INJECT
    merged["USE_WEB_SEARCH_FOR_MB"] = USE_WEB_SEARCH_FOR_MB
    merged["USE_AI_WEB_SEARCH_FALLBACK"] = USE_AI_WEB_SEARCH_FALLBACK
    merged["PROVIDER_IDENTITY_USE_AI"] = PROVIDER_IDENTITY_USE_AI
    return normalized


def _ai_usage_extract_tokens(provider: str, response_obj: Any) -> tuple[dict[str, int | None], str, str | None]:
    provider_lower = str(provider or "").strip().lower()
    usage_source = "missing"
    request_id = None
    out = {
        "input_tokens": None,
        "cached_input_tokens": None,
        "output_tokens": None,
        "total_tokens": None,
    }
    if response_obj is None:
        return out, usage_source, request_id
    try:
        request_id = getattr(response_obj, "id", None) or (
            response_obj.get("id") if isinstance(response_obj, dict) else None
        )
    except Exception:
        request_id = None
    try:
        if provider_lower in {"openai", "openai-api", "openai-codex"}:
            usage = getattr(response_obj, "usage", None)
            if usage is None and isinstance(response_obj, dict):
                usage = response_obj.get("usage")
            if usage is None:
                return out, usage_source, request_id
            prompt_tokens = getattr(usage, "prompt_tokens", None)
            if prompt_tokens is None and isinstance(usage, dict):
                prompt_tokens = usage.get("prompt_tokens")
            if prompt_tokens is None:
                prompt_tokens = getattr(usage, "input_tokens", None)
            if prompt_tokens is None and isinstance(usage, dict):
                prompt_tokens = usage.get("input_tokens")
            completion_tokens = getattr(usage, "completion_tokens", None)
            if completion_tokens is None and isinstance(usage, dict):
                completion_tokens = usage.get("completion_tokens")
            if completion_tokens is None:
                completion_tokens = getattr(usage, "output_tokens", None)
            if completion_tokens is None and isinstance(usage, dict):
                completion_tokens = usage.get("output_tokens")
            total_tokens = getattr(usage, "total_tokens", None)
            if total_tokens is None and isinstance(usage, dict):
                total_tokens = usage.get("total_tokens")
            prompt_details = getattr(usage, "prompt_tokens_details", None)
            if prompt_details is None and isinstance(usage, dict):
                prompt_details = usage.get("prompt_tokens_details")
            if prompt_details is None:
                prompt_details = getattr(usage, "input_tokens_details", None)
            if prompt_details is None and isinstance(usage, dict):
                prompt_details = usage.get("input_tokens_details")
            cached_tokens = None
            if prompt_details is not None:
                cached_tokens = getattr(prompt_details, "cached_tokens", None)
                if cached_tokens is None and isinstance(prompt_details, dict):
                    cached_tokens = prompt_details.get("cached_tokens")
            out["input_tokens"] = _int_or_none(prompt_tokens)
            out["cached_input_tokens"] = _int_or_none(cached_tokens)
            out["output_tokens"] = _int_or_none(completion_tokens)
            out["total_tokens"] = _int_or_none(total_tokens)
            usage_source = "provider"
            return out, usage_source, request_id
        if provider_lower == "anthropic":
            usage = getattr(response_obj, "usage", None)
            if usage is None and isinstance(response_obj, dict):
                usage = response_obj.get("usage")
            if usage is None:
                return out, usage_source, request_id
            input_tokens = getattr(usage, "input_tokens", None)
            if input_tokens is None and isinstance(usage, dict):
                input_tokens = usage.get("input_tokens")
            output_tokens = getattr(usage, "output_tokens", None)
            if output_tokens is None and isinstance(usage, dict):
                output_tokens = usage.get("output_tokens")
            cached_tokens = getattr(usage, "cache_read_input_tokens", None)
            if cached_tokens is None and isinstance(usage, dict):
                cached_tokens = usage.get("cache_read_input_tokens")
            total_tokens = getattr(usage, "total_tokens", None)
            if total_tokens is None and isinstance(usage, dict):
                total_tokens = usage.get("total_tokens")
            in_tok = _int_or_none(input_tokens)
            out_tok = _int_or_none(output_tokens)
            tot_tok = _int_or_none(total_tokens)
            out["input_tokens"] = in_tok
            out["cached_input_tokens"] = _int_or_none(cached_tokens)
            out["output_tokens"] = out_tok
            out["total_tokens"] = tot_tok if tot_tok is not None else (in_tok or 0) + (out_tok or 0)
            usage_source = "provider"
            return out, usage_source, request_id
        if provider_lower == "google":
            usage = getattr(response_obj, "usage_metadata", None)
            if usage is None:
                usage = getattr(response_obj, "usageMetadata", None)
            if usage is None and isinstance(response_obj, dict):
                usage = response_obj.get("usage_metadata") or response_obj.get("usageMetadata")
            if usage is None:
                return out, usage_source, request_id
            prompt_tokens = getattr(usage, "prompt_token_count", None)
            if prompt_tokens is None and isinstance(usage, dict):
                prompt_tokens = usage.get("prompt_token_count")
            output_tokens = getattr(usage, "candidates_token_count", None)
            if output_tokens is None and isinstance(usage, dict):
                output_tokens = usage.get("candidates_token_count")
            total_tokens = getattr(usage, "total_token_count", None)
            if total_tokens is None and isinstance(usage, dict):
                total_tokens = usage.get("total_token_count")
            cached_tokens = getattr(usage, "cached_content_token_count", None)
            if cached_tokens is None and isinstance(usage, dict):
                cached_tokens = usage.get("cached_content_token_count")
            in_tok = _int_or_none(prompt_tokens)
            out_tok = _int_or_none(output_tokens)
            tot_tok = _int_or_none(total_tokens)
            out["input_tokens"] = in_tok
            out["cached_input_tokens"] = _int_or_none(cached_tokens)
            out["output_tokens"] = out_tok
            out["total_tokens"] = tot_tok if tot_tok is not None else (in_tok or 0) + (out_tok or 0)
            usage_source = "provider"
            return out, usage_source, request_id
        if provider_lower == "ollama":
            usage = response_obj if isinstance(response_obj, dict) else {}
            in_tok = _int_or_none(usage.get("prompt_eval_count"))
            out_tok = _int_or_none(usage.get("eval_count"))
            out["input_tokens"] = in_tok
            out["cached_input_tokens"] = 0
            out["output_tokens"] = out_tok
            out["total_tokens"] = (in_tok or 0) + (out_tok or 0)
            usage_source = "provider"
            return out, usage_source, request_id
    except Exception:
        return out, usage_source, request_id
    return out, usage_source, request_id


def _ai_price_lookup(cur: sqlite3.Cursor, provider: str, model: str, endpoint_kind: str, created_at: float) -> dict[str, Any] | None:
    provider_key = str(provider or "").strip().lower()
    model_key = str(model or "").strip()
    endpoint_key = _ai_usage_endpoint_kind(endpoint_kind)
    now_ts = float(created_at or time.time())

    def _fetch_rate(endpoint_for_lookup: str) -> sqlite3.Row | None:
        cur.execute(
            """
            SELECT pricing_version,
                   rate_input_microusd_per_1m,
                   rate_cached_input_microusd_per_1m,
                   rate_output_microusd_per_1m,
                   rate_image_microusd_per_image
            FROM ai_pricing_catalog
            WHERE provider = ?
              AND model IN (?, '*')
              AND endpoint_kind IN (?, '*')
              AND effective_from <= ?
              AND (effective_to IS NULL OR effective_to > ?)
            ORDER BY
              CASE WHEN model = ? THEN 0 ELSE 1 END,
              CASE WHEN endpoint_kind = ? THEN 0 ELSE 1 END,
              effective_from DESC
            LIMIT 1
            """,
            (
                provider_key,
                model_key,
                endpoint_for_lookup,
                now_ts,
                now_ts,
                model_key,
                endpoint_for_lookup,
            ),
        )
        return cur.fetchone()

    row = _fetch_rate(endpoint_key)
    if row is None and endpoint_key == "web_search":
        # Keep pricing deterministic for the web-search endpoint:
        # if no dedicated rate is configured, fall back to longform/text for the same model.
        row = _fetch_rate("longform") or _fetch_rate("text")
    if not row:
        return None
    return {
        "pricing_version": str(row["pricing_version"] or ""),
        "rate_input_microusd_per_1m": _int_or_none(row["rate_input_microusd_per_1m"]),
        "rate_cached_input_microusd_per_1m": _int_or_none(row["rate_cached_input_microusd_per_1m"]),
        "rate_output_microusd_per_1m": _int_or_none(row["rate_output_microusd_per_1m"]),
        "rate_image_microusd_per_image": _int_or_none(row["rate_image_microusd_per_image"]),
    }


def _ai_compute_costs(
    *,
    usage_source: str,
    input_tokens: int,
    cached_input_tokens: int,
    output_tokens: int,
    image_inputs: int,
    rates: dict[str, Any] | None,
) -> dict[str, int | None]:
    qty_in = max(0, int(input_tokens or 0))
    qty_cached = max(0, int(cached_input_tokens or 0))
    qty_out = max(0, int(output_tokens or 0))
    qty_img = max(0, int(image_inputs or 0))
    if usage_source != "provider":
        if qty_in + qty_cached + qty_out + qty_img <= 0:
            return {
                "cost_input_microusd": 0,
                "cost_cached_input_microusd": 0,
                "cost_output_microusd": 0,
                "cost_image_microusd": 0,
                "cost_total_microusd": 0,
                "unpriced": 0,
            }
        return {
            "cost_input_microusd": None,
            "cost_cached_input_microusd": None,
            "cost_output_microusd": None,
            "cost_image_microusd": None,
            "cost_total_microusd": None,
            "unpriced": 1,
        }
    if rates is None:
        if qty_in + qty_cached + qty_out + qty_img <= 0:
            return {
                "cost_input_microusd": 0,
                "cost_cached_input_microusd": 0,
                "cost_output_microusd": 0,
                "cost_image_microusd": 0,
                "cost_total_microusd": 0,
                "unpriced": 0,
            }
        return {
            "cost_input_microusd": None,
            "cost_cached_input_microusd": None,
            "cost_output_microusd": None,
            "cost_image_microusd": None,
            "cost_total_microusd": None,
            "unpriced": 1,
        }
    rate_in = _int_or_none(rates.get("rate_input_microusd_per_1m")) or 0
    rate_cached = _int_or_none(rates.get("rate_cached_input_microusd_per_1m")) or 0
    rate_out = _int_or_none(rates.get("rate_output_microusd_per_1m")) or 0
    rate_img = _int_or_none(rates.get("rate_image_microusd_per_image")) or 0
    missing_rate = (
        (qty_in > 0 and rate_in <= 0)
        or (qty_cached > 0 and rate_cached <= 0)
        or (qty_out > 0 and rate_out <= 0)
        or (qty_img > 0 and rate_img <= 0)
    )
    if missing_rate:
        return {
            "cost_input_microusd": None,
            "cost_cached_input_microusd": None,
            "cost_output_microusd": None,
            "cost_image_microusd": None,
            "cost_total_microusd": None,
            "unpriced": 1,
        }
    cost_input = _microusd_half_up_from_rate(qty_in, rate_in)
    cost_cached = _microusd_half_up_from_rate(qty_cached, rate_cached)
    cost_output = _microusd_half_up_from_rate(qty_out, rate_out)
    cost_image = int(qty_img * rate_img)
    return {
        "cost_input_microusd": cost_input,
        "cost_cached_input_microusd": cost_cached,
        "cost_output_microusd": cost_output,
        "cost_image_microusd": cost_image,
        "cost_total_microusd": int(cost_input + cost_cached + cost_output + cost_image),
        "unpriced": 0,
    }


def _ai_breakdown_for_scan(cur: sqlite3.Cursor, scan_id: int, column: str) -> dict[str, dict[str, int]]:
    col = {"analysis_type": "analysis_type", "job_type": "job_type", "model": "model"}.get(column)
    if not col:
        return {}
    cur.execute(
        f"""
        SELECT COALESCE({col}, '') AS grp,
               COUNT(*) AS calls,
               COALESCE(SUM(COALESCE(input_tokens, 0)), 0) AS input_tokens,
               COALESCE(SUM(COALESCE(cached_input_tokens, 0)), 0) AS cached_input_tokens,
               COALESCE(SUM(COALESCE(output_tokens, 0)), 0) AS output_tokens,
               COALESCE(SUM(COALESCE(total_tokens, 0)), 0) AS total_tokens,
               COALESCE(SUM(COALESCE(cost_total_microusd, 0)), 0) AS cost_total_microusd,
               COALESCE(SUM(CASE WHEN cost_total_microusd IS NULL THEN 1 ELSE 0 END), 0) AS unpriced_calls
        FROM ai_call_usage
        WHERE scan_id = ? OR origin_scan_id = ?
        GROUP BY COALESCE({col}, '')
        ORDER BY calls DESC, grp ASC
        """,
        (int(scan_id), int(scan_id)),
    )
    out: dict[str, dict[str, int]] = {}
    for row in cur.fetchall():
        group_key = str(row["grp"] or "").strip() or "unknown"
        out[group_key] = {
            "calls": int(row["calls"] or 0),
            "input_tokens": int(row["input_tokens"] or 0),
            "cached_input_tokens": int(row["cached_input_tokens"] or 0),
            "output_tokens": int(row["output_tokens"] or 0),
            "total_tokens": int(row["total_tokens"] or 0),
            "cost_total_microusd": int(row["cost_total_microusd"] or 0),
            "unpriced_calls": int(row["unpriced_calls"] or 0),
        }
    return out


def _ai_lifecycle_complete_for_scan(cur: sqlite3.Cursor, scan_id: int) -> int:
    try:
        sid = int(scan_id or 0)
    except Exception:
        sid = 0
    if sid > 0:
        try:
            with lock:
                current_scan_id = int(state.get("scan_id") or 0)
                if current_scan_id == sid:
                    if bool(state.get("scanning")):
                        return 0
                    if bool(state.get("scan_finalizing")):
                        return 0
                    if bool(state.get("scan_post_processing")):
                        return 0
                    if bool(state.get("scan_profile_enrich_running")):
                        return 0
        except Exception:
            pass
    cur.execute(
        "SELECT status, end_time FROM scan_history WHERE scan_id = ? LIMIT 1",
        (sid,),
    )
    row = cur.fetchone()
    if not row:
        return 0
    status = str(row["status"] or "").strip().lower()
    scan_done = status in {"completed", "failed", "cancelled"} or row["end_time"] is not None
    cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM scheduler_jobs
        WHERE origin_scan_id = ?
          AND status IN ('queued', 'running')
        """,
        (int(scan_id),),
    )
    pending = int((cur.fetchone() or {"c": 0})["c"] or 0)
    return 1 if scan_done and pending <= 0 else 0


def _ai_refresh_rollup_for_scan(cur: sqlite3.Cursor, scan_id: int) -> None:
    cur.execute(
        """
        SELECT
            COUNT(*) AS calls_total,
            COALESCE(SUM(COALESCE(input_tokens, 0)), 0) AS input_tokens,
            COALESCE(SUM(COALESCE(cached_input_tokens, 0)), 0) AS cached_input_tokens,
            COALESCE(SUM(COALESCE(output_tokens, 0)), 0) AS output_tokens,
            COALESCE(SUM(COALESCE(total_tokens, 0)), 0) AS total_tokens,
            COALESCE(SUM(COALESCE(cost_total_microusd, 0)), 0) AS cost_total_microusd,
            COALESCE(SUM(CASE WHEN cost_total_microusd IS NULL THEN 1 ELSE 0 END), 0) AS unpriced_calls
        FROM ai_call_usage
        WHERE scan_id = ? OR origin_scan_id = ?
        """,
        (int(scan_id), int(scan_id)),
    )
    row = cur.fetchone() or {}
    calls_total = int(row["calls_total"] or 0)
    input_tokens = int(row["input_tokens"] or 0)
    cached_input_tokens = int(row["cached_input_tokens"] or 0)
    output_tokens = int(row["output_tokens"] or 0)
    total_tokens = int(row["total_tokens"] or 0)
    cost_total_microusd = int(row["cost_total_microusd"] or 0)
    unpriced_calls = int(row["unpriced_calls"] or 0)
    lifecycle_complete = _ai_lifecycle_complete_for_scan(cur, int(scan_id))
    breakdown_by_analysis = _ai_breakdown_for_scan(cur, int(scan_id), "analysis_type")
    breakdown_by_job = _ai_breakdown_for_scan(cur, int(scan_id), "job_type")
    breakdown_by_model = _ai_breakdown_for_scan(cur, int(scan_id), "model")
    now = time.time()
    cur.execute(
        """
        INSERT INTO ai_scan_cost_rollups
        (scan_id, calls_total, input_tokens, cached_input_tokens, output_tokens, total_tokens,
         cost_total_microusd, unpriced_calls, lifecycle_complete,
         breakdown_by_analysis_json, breakdown_by_job_json, breakdown_by_model_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scan_id) DO UPDATE SET
            calls_total = excluded.calls_total,
            input_tokens = excluded.input_tokens,
            cached_input_tokens = excluded.cached_input_tokens,
            output_tokens = excluded.output_tokens,
            total_tokens = excluded.total_tokens,
            cost_total_microusd = excluded.cost_total_microusd,
            unpriced_calls = excluded.unpriced_calls,
            lifecycle_complete = excluded.lifecycle_complete,
            breakdown_by_analysis_json = excluded.breakdown_by_analysis_json,
            breakdown_by_job_json = excluded.breakdown_by_job_json,
            breakdown_by_model_json = excluded.breakdown_by_model_json,
            updated_at = excluded.updated_at
        """,
        (
            int(scan_id),
            calls_total,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            total_tokens,
            cost_total_microusd,
            unpriced_calls,
            lifecycle_complete,
            _json_dumps_safe(breakdown_by_analysis),
            _json_dumps_safe(breakdown_by_job),
            _json_dumps_safe(breakdown_by_model),
            now,
        ),
    )
    cur.execute(
        """
        UPDATE scan_history
        SET ai_used_count = ?, ai_tokens_total = ?, ai_cost_usd_total = ?, ai_unpriced_calls = ?, ai_lifecycle_complete = ?
        WHERE scan_id = ?
        """,
        (
            int(calls_total),
            int(total_tokens),
            float(_microusd_to_usd(cost_total_microusd)),
            int(unpriced_calls),
            int(lifecycle_complete),
            int(scan_id),
        ),
    )


def _ai_usage_flush_batch(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return True
    max_tries = 5
    for attempt in range(1, max_tries + 1):
        con = None
        try:
            con = _state_connect(timeout=20)
            cur = con.cursor()
            affected_scan_ids: set[int] = set()
            for row in rows:
                created_at = float(row.get("created_at") or time.time())
                provider = str(row.get("provider") or "").strip().lower()
                model = str(row.get("model") or "").strip()
                endpoint_kind = _ai_usage_endpoint_kind(row.get("endpoint_kind"))
                rates = _ai_price_lookup(cur, provider, model, endpoint_kind, created_at)
                usage_source = str(row.get("usage_source") or "missing").strip().lower()
                if usage_source not in {"provider", "missing"}:
                    usage_source = "missing"
                input_tokens = int(row.get("input_tokens") or 0)
                cached_input_tokens = int(row.get("cached_input_tokens") or 0)
                output_tokens = int(row.get("output_tokens") or 0)
                total_tokens = int(row.get("total_tokens") or 0)
                if total_tokens <= 0:
                    total_tokens = max(0, input_tokens + output_tokens)
                image_inputs = int(row.get("image_inputs") or 0)
                costs = _ai_compute_costs(
                    usage_source=usage_source,
                    input_tokens=input_tokens,
                    cached_input_tokens=cached_input_tokens,
                    output_tokens=output_tokens,
                    image_inputs=image_inputs,
                    rates=rates,
                )
                cur.execute(
                    """
                    INSERT OR IGNORE INTO ai_call_usage (
                        call_id, created_at, scan_id, origin_scan_id, album_id, album_artist, album_title, scheduler_job_id, run_id,
                        phase, job_type, scope, analysis_type, provider, model, endpoint_kind,
                        status, latency_ms, request_id, input_tokens, cached_input_tokens, output_tokens,
                        total_tokens, image_inputs, pricing_version, rate_input_microusd_per_1m,
                        rate_cached_input_microusd_per_1m, rate_output_microusd_per_1m,
                        rate_image_microusd_per_image, cost_input_microusd, cost_cached_input_microusd,
                        cost_output_microusd, cost_image_microusd, cost_total_microusd, usage_source,
                        error_code, error_message, metadata_json
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        str(row.get("call_id") or str(uuid.uuid4())),
                        created_at,
                        _int_or_none(row.get("scan_id")),
                        _int_or_none(row.get("origin_scan_id")),
                        _int_or_none(row.get("album_id")),
                        str(row.get("album_artist") or "") or None,
                        str(row.get("album_title") or "") or None,
                        str(row.get("scheduler_job_id") or "") or None,
                        str(row.get("run_id") or "") or None,
                        str(row.get("phase") or "manual"),
                        str(row.get("job_type") or "") or None,
                        str(row.get("scope") or "") or None,
                        _normalize_ai_analysis_type(row.get("analysis_type")),
                        provider,
                        model,
                        endpoint_kind,
                        str(row.get("status") or "completed"),
                        _int_or_none(row.get("latency_ms")),
                        str(row.get("request_id") or "") or None,
                        input_tokens,
                        cached_input_tokens,
                        output_tokens,
                        total_tokens,
                        image_inputs,
                        str((rates or {}).get("pricing_version") or ""),
                        _int_or_none((rates or {}).get("rate_input_microusd_per_1m")),
                        _int_or_none((rates or {}).get("rate_cached_input_microusd_per_1m")),
                        _int_or_none((rates or {}).get("rate_output_microusd_per_1m")),
                        _int_or_none((rates or {}).get("rate_image_microusd_per_image")),
                        _int_or_none(costs.get("cost_input_microusd")),
                        _int_or_none(costs.get("cost_cached_input_microusd")),
                        _int_or_none(costs.get("cost_output_microusd")),
                        _int_or_none(costs.get("cost_image_microusd")),
                        _int_or_none(costs.get("cost_total_microusd")),
                        usage_source,
                        str(row.get("error_code") or "") or None,
                        str(row.get("error_message") or "") or None,
                        _json_dumps_safe(row.get("metadata") or {}),
                    ),
                )
                scan_id = _int_or_none(row.get("scan_id"))
                origin_scan_id = _int_or_none(row.get("origin_scan_id"))
                if scan_id:
                    affected_scan_ids.add(int(scan_id))
                if origin_scan_id:
                    affected_scan_ids.add(int(origin_scan_id))
            for scan_id in sorted(affected_scan_ids):
                _ai_refresh_rollup_for_scan(cur, scan_id)
            con.commit()
            con.close()
            return True
        except sqlite3.OperationalError as e:
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass
            if "locked" in str(e).lower() and attempt < max_tries:
                time.sleep(0.05 * attempt)
                continue
            logging.exception("AI usage flush failed (attempt %d/%d)", attempt, max_tries)
            return False
        except Exception:
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass
            logging.exception("AI usage flush failed unexpectedly")
            return False
    return False


def _ai_scan_cost_summary(
    scan_id: int,
    *,
    include_lifecycle: bool = True,
    group_by: str = "analysis_type",
    limit: int | None = None,
) -> dict[str, Any]:
    group_col = {
        "analysis_type": "analysis_type",
        "job_type": "job_type",
        "model": "model",
        "provider": "provider",
        "album": "album",
        "auth_mode": "json_extract(metadata_json, '$.auth_mode')",
    }.get(str(group_by or "").strip().lower(), "analysis_type")
    normalized_limit = max(0, int(limit or 0))
    if normalized_limit > 5000:
        normalized_limit = 5000
    con = _state_connect(timeout=15)
    cur = con.cursor()
    where_clause = "scan_id = ? OR origin_scan_id = ?" if include_lifecycle else "scan_id = ?"
    params: tuple[Any, ...] = (int(scan_id), int(scan_id)) if include_lifecycle else (int(scan_id),)
    cur.execute(
        f"""
        SELECT
            COUNT(*) AS calls_total,
            COALESCE(SUM(COALESCE(input_tokens, 0)), 0) AS input_tokens,
            COALESCE(SUM(COALESCE(cached_input_tokens, 0)), 0) AS cached_input_tokens,
            COALESCE(SUM(COALESCE(output_tokens, 0)), 0) AS output_tokens,
            COALESCE(SUM(COALESCE(total_tokens, 0)), 0) AS total_tokens,
            COALESCE(SUM(COALESCE(cost_total_microusd, 0)), 0) AS cost_total_microusd,
            COALESCE(SUM(CASE WHEN cost_total_microusd IS NULL THEN 1 ELSE 0 END), 0) AS unpriced_calls
        FROM ai_call_usage
        WHERE {where_clause}
        """,
        params,
    )
    totals_row = cur.fetchone() or {}
    if group_col == "album":
        query = f"""
            SELECT
                COALESCE(album_id, 0) AS album_id,
                COALESCE(album_artist, '') AS album_artist,
                COALESCE(album_title, '') AS album_title,
                COUNT(*) AS calls,
                COALESCE(SUM(COALESCE(input_tokens, 0)), 0) AS input_tokens,
                COALESCE(SUM(COALESCE(cached_input_tokens, 0)), 0) AS cached_input_tokens,
                COALESCE(SUM(COALESCE(output_tokens, 0)), 0) AS output_tokens,
                COALESCE(SUM(COALESCE(total_tokens, 0)), 0) AS total_tokens,
                COALESCE(SUM(COALESCE(cost_total_microusd, 0)), 0) AS cost_total_microusd,
                COALESCE(SUM(CASE WHEN cost_total_microusd IS NULL THEN 1 ELSE 0 END), 0) AS unpriced_calls
            FROM ai_call_usage
            WHERE {where_clause}
            GROUP BY COALESCE(album_id, 0), COALESCE(album_artist, ''), COALESCE(album_title, '')
            ORDER BY calls DESC, cost_total_microusd DESC, album_artist ASC, album_title ASC
        """
        params_group: tuple[Any, ...] = params
        if normalized_limit > 0:
            query += " LIMIT ?"
            params_group = params_group + (int(normalized_limit),)
        cur.execute(query, params_group)
        rows = cur.fetchall()
    else:
        query = f"""
            SELECT COALESCE({group_col}, '') AS grp,
                   COUNT(*) AS calls,
                   COALESCE(SUM(COALESCE(input_tokens, 0)), 0) AS input_tokens,
                   COALESCE(SUM(COALESCE(cached_input_tokens, 0)), 0) AS cached_input_tokens,
                   COALESCE(SUM(COALESCE(output_tokens, 0)), 0) AS output_tokens,
                   COALESCE(SUM(COALESCE(total_tokens, 0)), 0) AS total_tokens,
                   COALESCE(SUM(COALESCE(cost_total_microusd, 0)), 0) AS cost_total_microusd,
                   COALESCE(SUM(CASE WHEN cost_total_microusd IS NULL THEN 1 ELSE 0 END), 0) AS unpriced_calls
            FROM ai_call_usage
            WHERE {where_clause}
            GROUP BY COALESCE({group_col}, '')
            ORDER BY calls DESC, grp ASC
        """
        params_group = params
        if normalized_limit > 0:
            query += " LIMIT ?"
            params_group = params_group + (int(normalized_limit),)
        cur.execute(query, params_group)
        rows = cur.fetchall()
    cur.execute("SELECT updated_at FROM ai_scan_cost_rollups WHERE scan_id = ?", (int(scan_id),))
    rr = cur.fetchone()
    lifecycle_complete = _ai_lifecycle_complete_for_scan(cur, int(scan_id)) if include_lifecycle else 0
    con.close()
    breakdown: list[dict[str, Any]] = []
    for row in rows:
        entry = {
            "analysis_type": "",
            "job_type": "",
            "model": "",
            "provider": "",
            "auth_mode": "",
            "album_id": None,
            "album_artist": "",
            "album_title": "",
            "scope": "lifecycle" if include_lifecycle else "scan",
            "calls": int(row["calls"] or 0),
            "input_tokens": int(row["input_tokens"] or 0),
            "cached_input_tokens": int(row["cached_input_tokens"] or 0),
            "output_tokens": int(row["output_tokens"] or 0),
            "total_tokens": int(row["total_tokens"] or 0),
            "cost_usd": _microusd_to_usd(int(row["cost_total_microusd"] or 0)),
            "cost_microusd": int(row["cost_total_microusd"] or 0),
            "unpriced_calls": int(row["unpriced_calls"] or 0),
        }
        if group_col == "album":
            album_id = _int_or_none(row["album_id"])
            entry["album_id"] = int(album_id) if album_id and album_id > 0 else None
            entry["album_artist"] = str(row["album_artist"] or "").strip()
            entry["album_title"] = str(row["album_title"] or "").strip()
        else:
            grp = str(row["grp"] or "").strip()
            if group_col == "analysis_type":
                entry["analysis_type"] = grp
            elif group_col == "job_type":
                entry["job_type"] = grp
            elif group_col == "model":
                entry["model"] = grp
            elif group_col == "provider":
                entry["provider"] = grp
            elif group_col == "json_extract(metadata_json, '$.auth_mode')":
                entry["auth_mode"] = grp
        breakdown.append(entry)
    cost_total_microusd = int(totals_row["cost_total_microusd"] or 0)
    return {
        "scan_id": int(scan_id),
        "currency": "USD",
        "include_lifecycle": bool(include_lifecycle),
        "group_by": group_col,
        "limit": int(normalized_limit),
        "totals": {
            "calls": int(totals_row["calls_total"] or 0),
            "input_tokens": int(totals_row["input_tokens"] or 0),
            "cached_input_tokens": int(totals_row["cached_input_tokens"] or 0),
            "output_tokens": int(totals_row["output_tokens"] or 0),
            "total_tokens": int(totals_row["total_tokens"] or 0),
            "cost_usd": _microusd_to_usd(cost_total_microusd),
            "cost_microusd": cost_total_microusd,
            "unpriced_calls": int(totals_row["unpriced_calls"] or 0),
        },
        "breakdown": breakdown,
        "lifecycle_complete": bool(lifecycle_complete),
        "last_updated_at": float(rr["updated_at"] or 0.0) if rr and rr["updated_at"] is not None else None,
    }


def _analysis_dir_path() -> Any:
    try:
        return Path(str(PMDA_ANALYSIS_REPORTS_DIR or "")).expanduser()
    except Exception:
        return Path(__file__).resolve().parent / "analysis"


def _latest_ai_benchmark_for_domain(domain: str) -> dict[str, Any] | None:
    domain_norm = str(domain or "").strip().lower()
    patterns = list(AI_DOMAIN_BENCHMARK_GLOBS.get(domain_norm) or [])
    if not patterns:
        return None
    analysis_dir = _analysis_dir_path()
    if not analysis_dir.exists() or not analysis_dir.is_dir():
        return None
    files: list[Any] = []
    for pattern in patterns:
        try:
            files.extend([p for p in analysis_dir.glob(pattern) if p.is_file()])
        except Exception:
            continue
    if not files:
        return None
    files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
    for path in files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        baseline = payload.get("deterministic_accuracy")
        assisted = payload.get("assisted_accuracy")
        sample_size = payload.get("sample_size")
        if baseline is None and assisted is None:
            if domain_norm == "dedupe" and payload.get("decision"):
                return {
                    "file": path.name,
                    "generated_at": float(path.stat().st_mtime),
                    "available": False,
                    "note": str(payload.get("decision") or "").strip(),
                }
            continue
        baseline_val = _safe_bounded_float(baseline, minimum=0.0, maximum=1.0)
        assisted_val = _safe_bounded_float(assisted, minimum=0.0, maximum=1.0)
        sample_val = _parse_int_loose(sample_size, 0)
        delta = None
        if baseline_val is not None and assisted_val is not None:
            delta = round(float(assisted_val) - float(baseline_val), 4)
        return {
            "file": path.name,
            "generated_at": float(path.stat().st_mtime),
            "available": True,
            "sample_size": int(sample_val or 0),
            "baseline_score": float(baseline_val or 0.0) if baseline_val is not None else None,
            "assisted_score": float(assisted_val or 0.0) if assisted_val is not None else None,
            "delta": delta,
            "ai_completed": _parse_int_loose(payload.get("ai_completed"), 0) or 0,
            "ai_skipped": _parse_int_loose(payload.get("ai_skipped"), 0) or 0,
            "ai_failed": _parse_int_loose(payload.get("ai_failed"), 0) or 0,
            "avg_latency_sec": _safe_bounded_float(payload.get("ai_elapsed_avg_sec"), minimum=0.0, maximum=86400.0),
        }
    return None


def _ai_domain_usage_summary(domain: str) -> dict[str, Any]:
    domain_norm = str(domain or "").strip().lower()
    analysis_types = sorted(AI_DOMAIN_ANALYSIS_TYPES.get(domain_norm) or set())
    if not analysis_types:
        return {
            "domain": domain_norm,
            "analysis_types": [],
            "calls_total": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "avg_latency_ms": None,
            "cache_hits": 0,
            "cache_hit_rate": None,
            "last_call_at": None,
            "override_count": 0,
            "override_rate": None,
        }
    placeholders = ",".join("?" for _ in analysis_types)
    con = _state_connect(timeout=20)
    cur = con.cursor()
    cur.execute(
        f"""
        SELECT
            COUNT(*) AS calls_total,
            COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0) AS completed_count,
            COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed_count,
            COALESCE(SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END), 0) AS skipped_count,
            AVG(CASE WHEN latency_ms IS NOT NULL THEN latency_ms END) AS avg_latency_ms,
            COALESCE(SUM(
                CASE
                    WHEN COALESCE(cached_input_tokens, 0) > 0
                         OR COALESCE(json_extract(metadata_json, '$.cache_hit'), 0) = 1
                    THEN 1 ELSE 0
                END
            ), 0) AS cache_hits,
            MAX(created_at) AS last_call_at
        FROM ai_call_usage
        WHERE analysis_type IN ({placeholders})
        """,
        tuple(analysis_types),
    )
    usage_row = cur.fetchone() or {}
    cur.execute(
        """
        SELECT COUNT(*) AS override_count, MAX(created_at) AS last_override_at
        FROM ai_override_events
        WHERE domain = ?
        """,
        (domain_norm,),
    )
    override_row = cur.fetchone() or {}
    con.close()
    calls_total = int(usage_row["calls_total"] or 0)
    completed = int(usage_row["completed_count"] or 0)
    cache_hits = int(usage_row["cache_hits"] or 0)
    override_count = int(override_row["override_count"] or 0)
    return {
        "domain": domain_norm,
        "analysis_types": analysis_types,
        "calls_total": calls_total,
        "completed": completed,
        "failed": int(usage_row["failed_count"] or 0),
        "skipped": int(usage_row["skipped_count"] or 0),
        "avg_latency_ms": round(float(usage_row["avg_latency_ms"] or 0.0), 2) if usage_row["avg_latency_ms"] is not None else None,
        "cache_hits": cache_hits,
        "cache_hit_rate": round((cache_hits / calls_total) * 100.0, 1) if calls_total > 0 else None,
        "last_call_at": float(usage_row["last_call_at"] or 0.0) if usage_row["last_call_at"] is not None else None,
        "override_count": override_count,
        "override_rate": round((override_count / completed) * 100.0, 1) if completed > 0 else None,
        "last_override_at": float(override_row["last_override_at"] or 0.0) if override_row["last_override_at"] is not None else None,
    }


def _ai_overview_snapshot() -> dict[str, Any]:
    domains_out: list[dict[str, Any]] = []
    queue_snapshot = _ai_queue_status_snapshot()
    for domain in AI_DOMAIN_NAMES:
        usage = _ai_domain_usage_summary(domain)
        usage["queue"] = queue_snapshot.get(domain) if isinstance(queue_snapshot, dict) else None
        usage["benchmark"] = _latest_ai_benchmark_for_domain(domain)
        domains_out.append(usage)
    return {
        "generated_at": float(time.time()),
        "analysis_dir": str(_analysis_dir_path()),
        "queues": queue_snapshot,
        "domains": domains_out,
    }


_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}

def _ai_usage_level_overrides_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_usage_level_overrides(*args, **kwargs)

def _apply_ai_usage_level_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _apply_ai_usage_level(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _ai_usage_extract_tokens_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_usage_extract_tokens(*args, **kwargs)

def _ai_price_lookup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_price_lookup(*args, **kwargs)

def _ai_compute_costs_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_compute_costs(*args, **kwargs)

def _ai_breakdown_for_scan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_breakdown_for_scan(*args, **kwargs)

def _ai_lifecycle_complete_for_scan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_lifecycle_complete_for_scan(*args, **kwargs)

def _ai_refresh_rollup_for_scan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_refresh_rollup_for_scan(*args, **kwargs)

def _ai_usage_flush_batch_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_usage_flush_batch(*args, **kwargs)

def _ai_scan_cost_summary_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_scan_cost_summary(*args, **kwargs)

def _analysis_dir_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _analysis_dir_path(*args, **kwargs)

def _latest_ai_benchmark_for_domain_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _latest_ai_benchmark_for_domain(*args, **kwargs)

def _ai_domain_usage_summary_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_domain_usage_summary(*args, **kwargs)

def _ai_overview_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_overview_snapshot(*args, **kwargs)
