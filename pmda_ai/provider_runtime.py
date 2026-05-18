"""Runtime-backed AI provider calls.

This module holds the effectful text, vision, long-form, and bounded AI calls
that were historically embedded in ``pmda.py``. It still binds to the live PMDA
runtime for provider configuration, telemetry, guardrails, and compatibility
wrappers while the provider stack is migrated behind stable services.
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from typing import Any, List, Optional

import requests

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one AI provider call."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "call_ai_provider",
        "call_ai_provider_vision",
        "_call_ai_provider_bounded",
        "call_ai_provider_longform",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def _runtime_module() -> Any:
    if _RUNTIME is None:
        raise RuntimeError("AI provider runtime is not bound")
    return _RUNTIME


def call_ai_provider_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> str:
    _bind_runtime(runtime)
    return _call_ai_provider_impl(*args, **kwargs)


def call_ai_provider_vision_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> str:
    _bind_runtime(runtime)
    return _call_ai_provider_vision_impl(*args, **kwargs)


def call_ai_provider_bounded_for_runtime(runtime: Any, **kwargs: Any) -> str:
    _bind_runtime(runtime)
    return _call_ai_provider_bounded_impl(**kwargs)


def call_ai_provider_longform_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> str:
    _bind_runtime(runtime)
    return _call_ai_provider_longform_impl(*args, **kwargs)


def _call_ai_provider_impl(
    provider: str,
    model: str,
    system_msg: str,
    user_msg: str,
    max_tokens: int = 256,
    *,
    analysis_type: str,
    request_timeout_sec: float | None = None,
) -> str:
    """
    Call the configured provider (text endpoint) and persist token/cost usage.
    """
    started_at = time.time()
    response_obj: Any = None
    error_msg = ""
    status = "failed"
    result_text = ""
    guard_allowed = True
    guard_reason = ""
    guard_meta: dict[str, Any] = {}
    provider_effective = _resolve_provider_for_runtime(
        requested_provider=str(provider or ""),
        analysis_type=str(analysis_type or ""),
    )
    provider_for_usage = str(provider_effective or provider or "").strip()
    provider_lower = provider_for_usage.lower()
    model_for_usage = _resolve_model_for_runtime(
        provider_for_usage,
        str(model or ""),
        endpoint_kind="text",
        analysis_type=str(analysis_type or ""),
        system_msg=system_msg,
        user_msg=user_msg,
    )
    auth_mode_for_usage = _provider_auth_mode(provider_for_usage)
    ai_ctx_for_log = _ai_infer_runtime_context()
    logging.info(
        "[AI] Starting text call analysis=%s provider=%s model=%s context=%s prompt=%r",
        str(analysis_type or "other").strip() or "other",
        provider_for_usage,
        model_for_usage,
        _ai_trace_context_summary(ai_ctx_for_log),
        _log_preview_text(user_msg, 220),
    )
    try:
        guard_allowed, guard_reason, guard_meta = _ai_guardrail_precheck_safe(
            provider=provider_for_usage,
            model=model_for_usage,
            endpoint_kind="text",
            analysis_type=str(analysis_type or ""),
            requested_tokens=int(max_tokens or 0),
        )
        if not guard_allowed:
            raise RuntimeError(f"AI guardrail blocked call: {guard_reason}")
        if provider_lower == "openai-codex":
            out, response_obj = _run_openai_codex_exec(
                system_msg=system_msg,
                user_msg=user_msg,
                analysis_type=str(analysis_type or ""),
                request_timeout_sec=request_timeout_sec,
                web_search=False,
            )
            auth_mode_for_usage = "oauth"
            status = "completed"
            result_text = str(out or "")
            return result_text
        if provider_lower in {"openai", "openai-api", "openai-codex"}:
            try:
                client_to_use, auth_mode_for_usage, openai_runtime_reason = _resolve_openai_client_for_runtime(
                    provider_for_usage,
                    _current_user_id_or_zero(),
                )
                if not client_to_use:
                    raise ValueError(openai_runtime_reason or "OpenAI client not initialized")
                request_timeout = max(
                    5.0,
                    float(request_timeout_sec if request_timeout_sec is not None else _openai_request_timeout_seconds()),
                )
                try:
                    if (model_for_usage or "").strip().lower().startswith("gpt-5") and int(max_tokens or 0) < 128:
                        max_tokens = 128
                except Exception:
                    pass
                try:
                    is_gpt5 = (model_for_usage or "").strip().lower().startswith("gpt-5")
                except Exception:
                    is_gpt5 = False
                use_responses_api = bool(auth_mode_for_usage == "oauth" or provider_lower == "openai-codex")
                if use_responses_api:
                    try:
                        resp = client_to_use.responses.create(
                            model=model_for_usage,
                            input=[
                                {"role": ("developer" if is_gpt5 else "system"), "content": system_msg},
                                {"role": "user", "content": user_msg},
                            ],
                            max_output_tokens=max_tokens,
                            timeout=request_timeout,
                        )
                        response_obj = resp
                        out = _extract_text_from_openai_response(resp)
                        if out:
                            status = "completed"
                            result_text = str(out or "")
                            return result_text
                        raise RuntimeError("OpenAI responses call returned empty output")
                    except Exception as e:
                        if auth_mode_for_usage == "oauth":
                            raise
                        logging.debug("OpenAI responses fallback to chat.completions: %s", e)

                param_style = getattr(_runtime_module(), "RESOLVED_PARAM_STYLE", "mct")
                stop_ok = getattr(_runtime_module(), "RESOLVED_STOP_OK", True)
                _kwargs = {
                    "model": model_for_usage,
                    "messages": [
                        {"role": ("developer" if is_gpt5 else "system"), "content": system_msg},
                        {"role": "user", "content": user_msg},
                    ],
                    "timeout": request_timeout,
                }
                if is_gpt5:
                    _kwargs["reasoning_effort"] = "minimal"
                try:
                    if stop_ok and not is_gpt5:
                        _kwargs["stop"] = ["\n"]
                    if param_style == "mct":
                        _kwargs["max_completion_tokens"] = max_tokens
                    else:
                        _kwargs["max_tokens"] = max_tokens
                    resp = client_to_use.chat.completions.create(**_kwargs)
                    out = _openai_chat_text(resp)
                    response_obj = resp
                    if not out and is_gpt5 and int(max_tokens or 0) < 256:
                        _kwargs_retry = dict(_kwargs)
                        _kwargs_retry.pop("stop", None)
                        _kwargs_retry.pop("max_tokens", None)
                        _kwargs_retry["max_completion_tokens"] = 256
                        resp2 = client_to_use.chat.completions.create(**_kwargs_retry)
                        out = _openai_chat_text(resp2)
                        response_obj = resp2
                    status = "completed"
                    result_text = str(out or "")
                    return result_text
                except Exception as e:
                    err_msg = str(e).lower()
                    if "reasoning_effort" in err_msg and ("unsupported_parameter" in err_msg or "400" in err_msg):
                        _kwargs.pop("reasoning_effort", None)
                        resp = client_to_use.chat.completions.create(**_kwargs)
                        response_obj = resp
                        status = "completed"
                        result_text = _openai_chat_text(resp)
                        return result_text
                    if "unsupported_parameter" not in err_msg and "400" not in err_msg:
                        raise
                    if "max_tokens" in err_msg and "max_completion_tokens" in err_msg:
                        _kwargs.pop("max_tokens", None)
                        _kwargs["max_completion_tokens"] = max_tokens
                        resp = client_to_use.chat.completions.create(**_kwargs)
                        response_obj = resp
                        status = "completed"
                        result_text = _openai_chat_text(resp)
                        return result_text
                    if "max_completion_tokens" in err_msg and ("max_tokens" in err_msg or "use" in err_msg):
                        _kwargs.pop("max_completion_tokens", None)
                        _kwargs["max_tokens"] = max_tokens
                        resp = client_to_use.chat.completions.create(**_kwargs)
                        response_obj = resp
                        status = "completed"
                        result_text = _openai_chat_text(resp)
                        return result_text
                    if "stop" in err_msg or "unsupported" in err_msg:
                        _kwargs.pop("stop", None)
                        try:
                            _runtime_module().RESOLVED_STOP_OK = False
                        except Exception:
                            pass
                        resp = client_to_use.chat.completions.create(**_kwargs)
                        response_obj = resp
                        status = "completed"
                        result_text = _openai_chat_text(resp)
                        return result_text
                    raise
            except Exception as e:
                if provider_lower in {"openai", "openai-api"} and _openai_error_allows_codex_fallback(e):
                    uid = _current_user_id_or_zero()
                    if _openai_codex_runtime_available(uid, require_token=True):
                        logging.warning(
                            "[AI] OpenAI API failed for %s; retrying with Codex OAuth: %s",
                            str(analysis_type or ""),
                            e,
                        )
                        out, response_obj = _run_openai_codex_exec(
                            system_msg=system_msg,
                            user_msg=user_msg,
                            analysis_type=str(analysis_type or ""),
                            request_timeout_sec=request_timeout_sec,
                            web_search=False,
                        )
                        provider_for_usage = "openai-codex"
                        auth_mode_for_usage = "oauth"
                        status = "completed"
                        error_msg = ""
                        result_text = str(out or "")
                        return result_text
                raise

        if provider_lower == "anthropic":
            if not anthropic_client:
                raise ValueError("Anthropic client not initialized")
            resp = anthropic_client.messages.create(
                model=model_for_usage,
                max_tokens=max_tokens,
                system=system_msg,
                messages=[{"role": "user", "content": user_msg}],
            )
            response_obj = resp
            status = "completed"
            result_text = (resp.content[0].text or "").strip()
            return result_text

        if provider_lower == "google":
            if not google_client_configured or not google_client:
                raise ValueError("Google client not configured")
            response = google_client.models.generate_content(
                model=model_for_usage,
                contents=(user_msg or ""),
                config=genai.types.GenerateContentConfig(
                    systemInstruction=(system_msg or ""),
                    maxOutputTokens=max_tokens,
                    stopSequences=["\n"],
                ),
            )
            response_obj = response
            status = "completed"
            result_text = (getattr(response, "text", "") or "").strip()
            return result_text

        if provider_lower == "ollama":
            if not ollama_url:
                raise ValueError("Ollama URL not configured")
            ollama_timeout = (
                max(5.0, float(request_timeout_sec))
                if request_timeout_sec is not None
                else 60
            )
            try:
                if request_timeout_sec is None and str(model_for_usage or "").strip().lower() == _ollama_complex_model_configured().lower():
                    ollama_timeout = 180
            except Exception:
                ollama_timeout = max(5.0, float(request_timeout_sec)) if request_timeout_sec is not None else 60
            ollama_stop_sequences = ["\n"]
            if str(analysis_type or "").strip().lower() in {"identity_inference_no_tags"}:
                ollama_stop_sequences = []
            options_payload = {
                "num_predict": max_tokens,
                "temperature": 0,
            }
            if ollama_stop_sequences:
                options_payload["stop"] = ollama_stop_sequences
            _ollama_prewarm_model(
                model_for_usage,
                analysis_type=str(analysis_type or ""),
                force=False,
            )
            payload = {
                "model": model_for_usage,
                "messages": [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                "options": options_payload,
                "think": False,
                "stream": False,
                "keep_alive": _ollama_keep_alive_for_analysis(
                    analysis_type=str(analysis_type or ""),
                    model_name=str(model_for_usage or ""),
                ),
            }
            response = requests.post(f"{ollama_url}/api/chat", json=payload, timeout=ollama_timeout)
            if response.status_code != 200:
                raise Exception(f"Ollama API error: {response.status_code} - {response.text}")
            result = response.json()
            response_obj = result
            status = "completed"
            result_text = (result.get("message", {}) or {}).get("content", "").strip()
            return result_text

        raise ValueError(f"Unknown AI provider: {provider}")
    except Exception as e:
        error_msg = str(e)
        raise
    finally:
        elapsed = max(0.0, time.time() - started_at)
        analysis_norm = str(analysis_type or "other").strip().lower()
        if status == "completed" and analysis_norm == "identity_inference_no_tags":
            if not _assistant_extract_json_obj(result_text):
                status = "invalid_format"
                error_msg = "non_json_reply"
        if status == "completed":
            logging.info(
                "[AI] Finished text call analysis=%s provider=%s model=%s elapsed=%.2fs context=%s reply=%r",
                str(analysis_type or "other").strip() or "other",
                provider_for_usage,
                model_for_usage,
                elapsed,
                _ai_trace_context_summary(ai_ctx_for_log),
                _log_preview_text(result_text, 220),
            )
        else:
            logging.warning(
                "[AI] Text call failed analysis=%s provider=%s model=%s elapsed=%.2fs context=%s error=%s",
                str(analysis_type or "other").strip() or "other",
                provider_for_usage,
                model_for_usage,
                elapsed,
                _ai_trace_context_summary(ai_ctx_for_log),
                _log_preview_text(error_msg, 220),
            )
        recorder = globals().get("record_ai_usage")
        if callable(recorder):
            recorder(
                provider=provider_for_usage,
                model=model_for_usage,
                endpoint_kind="text",
                analysis_type=analysis_type,
                started_at=started_at,
                status=status,
                response_obj=response_obj,
                image_inputs=0,
                error=error_msg,
                metadata={
                    "max_tokens": int(max_tokens or 0),
                    "auth_mode": auth_mode_for_usage,
                    "guardrail_blocked": bool(not guard_allowed),
                    "guardrail_reason": guard_reason if not guard_allowed else "",
                    **(guard_meta or {}),
                },
            )


def _call_ai_provider_vision_impl(
    provider: str,
    model: str,
    system_msg: str,
    user_msg: str,
    image_urls: Optional[List[str]] = None,
    image_base64: Optional[List[dict]] = None,
    max_tokens: int = 32,
    *,
    analysis_type: str,
) -> str:
    """
    Call AI with optional images (vision). Used for cover comparison: "Do these two covers represent the same album? Yes/No."
    image_urls: list of image URLs (e.g. Cover Art Archive, or PMDA-served local cover).
    image_base64: list of {"type": "image_url", "image_url": {"url": "data:image/...;base64,..."}} or provider-specific.
    Returns the text response. Only OpenAI is supported for vision; other providers fall back to text-only.
    """
    provider_effective = _resolve_provider_for_runtime(
        requested_provider=str(provider or ""),
        analysis_type=str(analysis_type or ""),
    )
    provider_for_usage = str(provider_effective or provider or "").strip()
    provider_lower = provider_for_usage.lower()
    model_for_usage = _resolve_model_for_runtime(
        provider_for_usage,
        str(model or ""),
        endpoint_kind="vision",
        analysis_type=str(analysis_type or ""),
        system_msg=system_msg,
        user_msg=user_msg,
    )
    if provider_lower == "openai-codex":
        started_at = time.time()
        response_obj: Any = None
        status = "failed"
        error_msg = ""
        image_inputs = 0
        guard_allowed = True
        guard_reason = ""
        guard_meta: dict[str, Any] = {}
        try:
            guard_allowed, guard_reason, guard_meta = _ai_guardrail_precheck_safe(
                provider=provider_for_usage,
                model=model_for_usage,
                endpoint_kind="vision",
                analysis_type=str(analysis_type or ""),
                requested_tokens=int(max_tokens or 0),
            )
            if not guard_allowed:
                raise RuntimeError(f"AI guardrail blocked call: {guard_reason}")
            image_inputs = min(10, len(list(image_urls or [])) + len(list(image_base64 or [])))
            out, response_obj = _run_openai_codex_exec(
                system_msg=system_msg,
                user_msg=user_msg,
                analysis_type=str(analysis_type or ""),
                request_timeout_sec=None,
                web_search=False,
                image_urls=image_urls,
                image_base64=image_base64,
            )
            status = "completed"
            return out
        except Exception as e:
            error_msg = str(e)
            raise
        finally:
            recorder = globals().get("record_ai_usage")
            if callable(recorder):
                recorder(
                    provider=provider_for_usage,
                    model=model_for_usage,
                    endpoint_kind="vision",
                    analysis_type=analysis_type,
                    started_at=started_at,
                    status=status,
                    response_obj=response_obj,
                    image_inputs=image_inputs,
                    error=error_msg,
                    metadata={
                        "max_tokens": int(max_tokens or 0),
                        "auth_mode": "oauth",
                        "guardrail_blocked": bool(not guard_allowed),
                        "guardrail_reason": guard_reason if not guard_allowed else "",
                        **(guard_meta or {}),
                    },
                )
    if provider_lower not in {"openai", "openai-api", "openai-codex"}:
        return _runtime_module().call_ai_provider(
            provider_for_usage,
            model_for_usage,
            system_msg,
            user_msg,
            max_tokens=max_tokens,
            analysis_type=analysis_type,
        )
    client_to_use, auth_mode_for_usage, openai_runtime_reason = _resolve_openai_client_for_runtime(
        provider_for_usage,
        _current_user_id_or_zero(),
    )
    if not client_to_use:
        if openai_runtime_reason:
            logging.warning("OpenAI vision call downgraded to text provider due to runtime issue: %s", openai_runtime_reason)
        return _runtime_module().call_ai_provider(
            "openai-api",
            model_for_usage,
            system_msg,
            user_msg,
            max_tokens=max_tokens,
            analysis_type=analysis_type,
        )
    started_at = time.time()
    response_obj: Any = None
    status = "failed"
    error_msg = ""
    image_inputs = 0
    guard_allowed = True
    guard_reason = ""
    guard_meta: dict[str, Any] = {}
    content: List[dict] = [{"type": "text", "text": user_msg}]
    if image_urls:
        for url in image_urls[:10]:
            if url:
                content.append({"type": "image_url", "image_url": {"url": url}})
                image_inputs += 1
    if image_base64:
        for img in image_base64[:10]:
            if isinstance(img, dict) and img.get("type") == "image_url" and img.get("image_url", {}).get("url"):
                content.append(img)
                image_inputs += 1
    param_style = getattr(_runtime_module(), "RESOLVED_PARAM_STYLE", "mct")
    stop_ok = getattr(_runtime_module(), "RESOLVED_STOP_OK", True)
    try:
        is_gpt5 = (model_for_usage or "").strip().lower().startswith("gpt-5")
    except Exception:
        is_gpt5 = False
    # GPT-5 family models may spend very small budgets entirely on reasoning and return empty output.
    # Enforce a sane minimum for short YES/NO style outputs.
    try:
        if is_gpt5 and int(max_tokens or 0) < 128:
            max_tokens = 128
    except Exception:
        pass
    _kwargs = {
        "model": model_for_usage,
        "messages": [
            {"role": ("developer" if is_gpt5 else "system"), "content": system_msg},
            {"role": "user", "content": content},
        ],
    }
    if is_gpt5:
        _kwargs["reasoning_effort"] = "minimal"
    if stop_ok and not is_gpt5:
        _kwargs["stop"] = ["\n"]
    if param_style == "mct":
        _kwargs["max_completion_tokens"] = max_tokens
    else:
        _kwargs["max_tokens"] = max_tokens
    try:
        guard_allowed, guard_reason, guard_meta = _ai_guardrail_precheck_safe(
            provider=provider_for_usage,
            model=model_for_usage,
            endpoint_kind="vision",
            analysis_type=str(analysis_type or ""),
            requested_tokens=int(max_tokens or 0),
        )
        if not guard_allowed:
            raise RuntimeError(f"AI guardrail blocked call: {guard_reason}")
        resp = client_to_use.chat.completions.create(**_kwargs)
        out = _openai_chat_text(resp)
        response_obj = resp
        if not out and is_gpt5 and int(max_tokens or 0) < 256:
            # One retry with a larger budget for GPT-5 family models.
            _kwargs_retry = dict(_kwargs)
            _kwargs_retry.pop("stop", None)
            _kwargs_retry.pop("max_tokens", None)
            _kwargs_retry["max_completion_tokens"] = 256
            resp2 = client_to_use.chat.completions.create(**_kwargs_retry)
            out = _openai_chat_text(resp2)
            response_obj = resp2
        status = "completed"
        return out
    except Exception as e:
        logging.debug("[AI Vision] OpenAI vision call failed: %s", e)
        error_msg = str(e)
        if provider_lower in {"openai", "openai-api"} and _openai_error_allows_codex_fallback(e):
            uid = _current_user_id_or_zero()
            if _openai_codex_runtime_available(uid, require_token=True):
                logging.warning(
                    "[AI Vision] OpenAI API failed for %s; retrying with Codex OAuth: %s",
                    str(analysis_type or ""),
                    e,
                )
                out, response_obj = _run_openai_codex_exec(
                    system_msg=system_msg,
                    user_msg=user_msg,
                    analysis_type=str(analysis_type or ""),
                    request_timeout_sec=None,
                    web_search=False,
                    image_urls=image_urls,
                    image_base64=image_base64,
                )
                provider_for_usage = "openai-codex"
                auth_mode_for_usage = "oauth"
                status = "completed"
                error_msg = ""
                return out
        # Retry without reasoning_effort for models that don't support it.
        try:
            msg = str(e).lower()
        except Exception:
            msg = ""
        if "reasoning_effort" in msg and ("unsupported_parameter" in msg or "400" in msg):
            _kwargs.pop("reasoning_effort", None)
            try:
                resp = client_to_use.chat.completions.create(**_kwargs)
                response_obj = resp
                status = "completed"
                error_msg = ""
                return _openai_chat_text(resp)
            except Exception:
                raise e
        # Retry without stop if the model rejects it (some reasoning models reject stop).
        if "stop" in msg and ("unsupported" in msg or "unsupported_parameter" in msg or "400" in msg):
            _kwargs.pop("stop", None)
            try:
                _runtime_module().RESOLVED_STOP_OK = False
            except Exception:
                pass
            resp = client_to_use.chat.completions.create(**_kwargs)
            response_obj = resp
            status = "completed"
            error_msg = ""
            return _openai_chat_text(resp)
        raise
    finally:
        recorder = globals().get("record_ai_usage")
        if callable(recorder):
                recorder(
                    provider=provider_for_usage,
                    model=model_for_usage,
                endpoint_kind="vision",
                analysis_type=analysis_type,
                started_at=started_at,
                status=status,
                response_obj=response_obj,
                image_inputs=image_inputs,
                error=error_msg,
                metadata={
                    "max_tokens": int(max_tokens or 0),
                    "auth_mode": auth_mode_for_usage,
                    "guardrail_blocked": bool(not guard_allowed),
                    "guardrail_reason": guard_reason if not guard_allowed else "",
                    **(guard_meta or {}),
                },
            )


def parse_ai_confidence(reply: str) -> tuple[str, Optional[int]]:
    """
    Strip optional (confidence: N) from the end of an AI reply. N is clamped to 0-100.
    Returns (reply_without_confidence, confidence_or_None).
    """
    if not (reply or isinstance(reply, str)):
        return (reply or "", None)
    text = reply.strip()
    m = re.search(r"\s*\(confidence:\s*(\d+)\)\s*$", text, re.I)
    if m:
        conf = min(100, max(0, int(m.group(1))))
        clean = text[: m.start()].strip()
        return (clean, conf)
    return (text, None)


def _call_ai_provider_bounded_impl(
    *,
    provider: str,
    model: str,
    system_msg: str,
    user_msg: str,
    max_tokens: int,
    analysis_type: str,
    timeout_sec: float,
    log_prefix: str,
) -> str:
    """
    Run call_ai_provider with a hard timeout so album scan threads never hang indefinitely.
    """
    timeout_val = max(5.0, float(timeout_sec or 0.0))
    started = time.perf_counter()
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pmda-ai-bounded")
    fut = pool.submit(
        _runtime_module().call_ai_provider,
        provider,
        model,
        system_msg,
        user_msg,
        int(max_tokens or 0),
        analysis_type=analysis_type,
        request_timeout_sec=max(5.0, timeout_val - 2.0),
    )
    try:
        reply = fut.result(timeout=timeout_val)
        elapsed = time.perf_counter() - started
        analysis_norm = str(analysis_type or "").strip().lower()
        if analysis_norm == "identity_inference_no_tags" and not _assistant_extract_json_obj(str(reply or "")):
            logging.warning(
                "%s AI call returned non-JSON response in %.2fs; ignoring local-context hint",
                log_prefix,
                elapsed,
            )
            return ""
        logging.info("%s AI call completed in %.2fs", log_prefix, elapsed)
        return str(reply or "")
    except FutureTimeout:
        elapsed = time.perf_counter() - started
        logging.warning(
            "%s AI call timed out after %.1fs (elapsed %.2fs); continuing without AI result",
            log_prefix,
            timeout_val,
            elapsed,
        )
        raise TimeoutError(f"{log_prefix} timeout")
    finally:
        try:
            fut.cancel()
        except Exception:
            pass
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass


def _call_ai_provider_longform_impl(
    provider: str,
    model: str,
    system_msg: str,
    user_msg: str,
    max_tokens: int = 800,
    *,
    analysis_type: str,
    request_timeout_sec: float | None = None,
) -> str:
    """AI call that allows multi-line answers and persists token/cost telemetry."""
    started_at = time.time()
    response_obj: Any = None
    status = "failed"
    error_msg = ""
    result_text = ""
    guard_allowed = True
    guard_reason = ""
    guard_meta: dict[str, Any] = {}
    provider_effective = _resolve_provider_for_runtime(
        requested_provider=str(provider or ""),
        analysis_type=str(analysis_type or ""),
    )
    provider_for_usage = str(provider_effective or provider or "").strip()
    provider_lower = provider_for_usage.lower()
    model_for_usage = _resolve_model_for_runtime(
        provider_for_usage,
        str(model or ""),
        endpoint_kind="longform",
        analysis_type=str(analysis_type or ""),
        system_msg=system_msg,
        user_msg=user_msg,
    )
    auth_mode_for_usage = _provider_auth_mode(provider_for_usage)
    ai_ctx_for_log = _ai_infer_runtime_context()
    logging.info(
        "[AI] Starting longform call analysis=%s provider=%s model=%s context=%s prompt=%r",
        str(analysis_type or "other").strip() or "other",
        provider_for_usage,
        model_for_usage,
        _ai_trace_context_summary(ai_ctx_for_log),
        _log_preview_text(user_msg, 220),
    )
    try:
        guard_allowed, guard_reason, guard_meta = _ai_guardrail_precheck(
            provider=provider_for_usage,
            model=model_for_usage,
            endpoint_kind="longform",
            analysis_type=str(analysis_type or ""),
            requested_tokens=int(max_tokens or 0),
        )
        if not guard_allowed:
            raise RuntimeError(f"AI guardrail blocked call: {guard_reason}")
        if provider_lower == "openai-codex":
            out, response_obj = _run_openai_codex_exec(
                system_msg=system_msg,
                user_msg=user_msg,
                analysis_type=str(analysis_type or ""),
                request_timeout_sec=request_timeout_sec,
                web_search=False,
            )
            auth_mode_for_usage = "oauth"
            status = "completed"
            result_text = str(out or "")
            return result_text
        if provider_lower in {"openai", "openai-api", "openai-codex"}:
            try:
                client_to_use, auth_mode_for_usage, openai_runtime_reason = _resolve_openai_client_for_runtime(
                    provider_for_usage,
                    _current_user_id_or_zero(),
                )
                if not client_to_use:
                    raise ValueError(openai_runtime_reason or "OpenAI client not initialized")
                request_timeout = max(
                    5.0,
                    float(request_timeout_sec if request_timeout_sec is not None else _openai_request_timeout_seconds()),
                )
                try:
                    is_gpt5 = (model_for_usage or "").strip().lower().startswith("gpt-5")
                except Exception:
                    is_gpt5 = False
                try:
                    if is_gpt5 and int(max_tokens or 0) < 256:
                        max_tokens = 256
                except Exception:
                    pass
                use_responses_api = bool(auth_mode_for_usage == "oauth" or provider_lower == "openai-codex")
                if use_responses_api:
                    try:
                        resp = client_to_use.responses.create(
                            model=model_for_usage,
                            input=[
                                {"role": ("developer" if is_gpt5 else "system"), "content": system_msg},
                                {"role": "user", "content": user_msg},
                            ],
                            max_output_tokens=max_tokens,
                            timeout=request_timeout,
                        )
                        response_obj = resp
                        out = _extract_text_from_openai_response(resp)
                        if out:
                            status = "completed"
                            result_text = str(out or "")
                            return result_text
                        raise RuntimeError("OpenAI responses call returned empty output")
                    except Exception as e:
                        if auth_mode_for_usage == "oauth":
                            raise
                        logging.debug("OpenAI responses longform fallback to chat.completions: %s", e)

                param_style = getattr(_runtime_module(), "RESOLVED_PARAM_STYLE", "mct")
                _kwargs = {
                    "model": model_for_usage,
                    "messages": [
                        {"role": ("developer" if is_gpt5 else "system"), "content": system_msg},
                        {"role": "user", "content": user_msg},
                    ],
                    "timeout": request_timeout,
                }
                if is_gpt5:
                    _kwargs["reasoning_effort"] = "minimal"
                if param_style == "mct":
                    _kwargs["max_completion_tokens"] = max_tokens
                else:
                    _kwargs["max_tokens"] = max_tokens
                try:
                    resp = client_to_use.chat.completions.create(**_kwargs)
                    out = _openai_chat_text(resp)
                    response_obj = resp
                    if not out and is_gpt5:
                        _kwargs_retry = dict(_kwargs)
                        _kwargs_retry.pop("max_tokens", None)
                        _kwargs_retry["max_completion_tokens"] = max(512, int(max_tokens or 0) * 2)
                        resp2 = client_to_use.chat.completions.create(**_kwargs_retry)
                        out = _openai_chat_text(resp2)
                        response_obj = resp2
                    status = "completed"
                    result_text = str(out or "")
                    return result_text
                except Exception as e:
                    err_msg = str(e).lower()
                    if "reasoning_effort" in err_msg and ("unsupported_parameter" in err_msg or "400" in err_msg):
                        _kwargs.pop("reasoning_effort", None)
                        resp = client_to_use.chat.completions.create(**_kwargs)
                        response_obj = resp
                        status = "completed"
                        result_text = _openai_chat_text(resp)
                        return result_text
                    if "unsupported_parameter" in err_msg or "400" in err_msg:
                        if "max_completion_tokens" in err_msg and ("max_tokens" in err_msg or "use" in err_msg):
                            _kwargs.pop("max_completion_tokens", None)
                            _kwargs["max_tokens"] = max_tokens
                            resp = client_to_use.chat.completions.create(**_kwargs)
                            response_obj = resp
                            status = "completed"
                            result_text = _openai_chat_text(resp)
                            return result_text
                        if "max_tokens" in err_msg and "max_completion_tokens" in err_msg:
                            _kwargs.pop("max_tokens", None)
                            _kwargs["max_completion_tokens"] = max_tokens
                            resp = client_to_use.chat.completions.create(**_kwargs)
                            response_obj = resp
                            status = "completed"
                            return _openai_chat_text(resp)
                    raise
            except Exception as e:
                if provider_lower in {"openai", "openai-api"} and _openai_error_allows_codex_fallback(e):
                    uid = _current_user_id_or_zero()
                    if _openai_codex_runtime_available(uid, require_token=True):
                        logging.warning(
                            "[AI] OpenAI API longform failed for %s; retrying with Codex OAuth: %s",
                            str(analysis_type or ""),
                            e,
                        )
                        out, response_obj = _run_openai_codex_exec(
                            system_msg=system_msg,
                            user_msg=user_msg,
                            analysis_type=str(analysis_type or ""),
                            request_timeout_sec=request_timeout_sec,
                            web_search=False,
                        )
                        provider_for_usage = "openai-codex"
                        auth_mode_for_usage = "oauth"
                        status = "completed"
                        error_msg = ""
                        result_text = str(out or "")
                        return result_text
                raise

        if provider_lower == "anthropic":
            if not anthropic_client:
                raise ValueError("Anthropic client not initialized")
            resp = anthropic_client.messages.create(
                model=model_for_usage,
                max_tokens=max_tokens,
                system=system_msg,
                messages=[{"role": "user", "content": user_msg}],
            )
            response_obj = resp
            status = "completed"
            result_text = resp.content[0].text.strip()
            return result_text

        if provider_lower == "google":
            if not google_client_configured or not google_client:
                raise ValueError("Google client not configured")
            response = google_client.models.generate_content(
                model=model_for_usage,
                contents=(user_msg or ""),
                config=genai.types.GenerateContentConfig(
                    systemInstruction=(system_msg or ""),
                    maxOutputTokens=max_tokens,
                ),
            )
            response_obj = response
            status = "completed"
            result_text = (getattr(response, "text", "") or "").strip()
            return result_text

        if provider_lower == "ollama":
            if not ollama_url:
                raise ValueError("Ollama URL not configured")
            ollama_timeout = (
                max(5.0, float(request_timeout_sec))
                if request_timeout_sec is not None
                else 120
            )
            _ollama_prewarm_model(
                model_for_usage,
                analysis_type=str(analysis_type or ""),
                force=False,
            )
            payload = {
                "model": model_for_usage,
                "messages": [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                "options": {
                    "num_predict": max_tokens,
                    "temperature": 0,
                },
                "think": False,
                "stream": False,
                "keep_alive": _ollama_keep_alive_for_analysis(
                    analysis_type=str(analysis_type or ""),
                    model_name=str(model_for_usage or ""),
                ),
            }
            response = requests.post(f"{ollama_url}/api/chat", json=payload, timeout=ollama_timeout)
            if response.status_code != 200:
                raise Exception(f"Ollama API error: {response.status_code} - {response.text}")
            result = response.json()
            response_obj = result
            status = "completed"
            result_text = (result.get("message", {}) or {}).get("content", "").strip()
            return result_text

        raise ValueError(f"Unknown AI provider: {provider}")
    except Exception as e:
        error_msg = str(e)
        raise
    finally:
        elapsed = max(0.0, time.time() - started_at)
        if status == "completed":
            logging.info(
                "[AI] Finished longform call analysis=%s provider=%s model=%s elapsed=%.2fs context=%s reply=%r",
                str(analysis_type or "other").strip() or "other",
                provider_for_usage,
                model_for_usage,
                elapsed,
                _ai_trace_context_summary(ai_ctx_for_log),
                _log_preview_text(result_text, 220),
            )
        else:
            logging.warning(
                "[AI] Longform call failed analysis=%s provider=%s model=%s elapsed=%.2fs context=%s error=%s",
                str(analysis_type or "other").strip() or "other",
                provider_for_usage,
                model_for_usage,
                elapsed,
                _ai_trace_context_summary(ai_ctx_for_log),
                _log_preview_text(error_msg, 220),
            )
        recorder = globals().get("record_ai_usage")
        if callable(recorder):
            recorder(
                provider=provider_for_usage,
                model=model_for_usage,
                endpoint_kind="longform",
                analysis_type=analysis_type,
                started_at=started_at,
                status=status,
                response_obj=response_obj,
                image_inputs=0,
                error=error_msg,
                metadata={
                    "max_tokens": int(max_tokens or 0),
                    "auth_mode": auth_mode_for_usage,
                    "guardrail_blocked": bool(not guard_allowed),
                    "guardrail_reason": guard_reason if not guard_allowed else "",
                    **(guard_meta or {}),
                },
            )
